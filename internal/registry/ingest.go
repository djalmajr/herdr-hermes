package registry

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
)

// recordTypes are the accepted outbox tipos (docs/protocol.md, "Outbox
// record").
var recordTypes = map[string]bool{
	outbox.TipoJobEvent: true,
	outbox.TipoDispatch: true,
	outbox.TipoAmend:    true,
	outbox.TipoSession:  true,
	outbox.TipoDecision: true,
}

// errNoRow is the not-found signal of the jobRow reads.
var errNoRow = sql.ErrNoRows

// Ingest applies one record of the machine's outbox and reports whether
// the record changed the registry. The retention sweep and the record
// projection run in one transaction that a refused record rolls back
// whole: the refusal leaves no trace, the sweep included, and the next
// successful request sweeps the past-retention rows. A confirmed ingest
// runs the high-count check after the transaction, outside it.
func (s *Store) Ingest(rec outbox.Record) (bool, error) {
	ts, err := validateRecord(rec)
	if err != nil {
		return false, err
	}
	var ev jobapi.Event
	var sd sessionDados
	switch rec.Tipo {
	case outbox.TipoJobEvent:
		ev, err = jobapi.DecodeEvent(rec.Dados)
		if err != nil {
			return false, invalidErr("dados", "undecodable event")
		}
		if ev.Seq < 1 {
			return false, invalidErr("seq", "must be positive")
		}
		if err := validateEventType(ev.Tipo); err != nil {
			return false, err
		}
		if rec.IdempotencyKey != *rec.JobID+":"+strconv.FormatInt(ev.Seq, 10) {
			return false, invalidErr("idempotency_key", "must be <job_id>:<event seq>")
		}
	case outbox.TipoSession:
		if sd, err = decodeSession(rec); err != nil {
			return false, err
		}
	}
	var applied bool
	err = s.tx(func(tx *sql.Tx) error {
		now := s.now()
		// The sweep runs in the same transaction and rolls back with
		// the rest when the record is refused below.
		if err := sweepEnded(tx, s.endedRetention, now); err != nil {
			return err
		}
		applied, err = s.ingestRecord(tx, rec, ev, sd, ts, now)
		return err
	})
	if err != nil {
		return false, err
	}
	// The high-count check runs after the committed transaction,
	// outside it; it never blocks, undoes or skips the record.
	s.checkHighCount()
	return applied, nil
}

// ingestRecord applies one validated record inside the running
// transaction: the node contact and the record's projection.
func (s *Store) ingestRecord(tx *sql.Tx, rec outbox.Record, ev jobapi.Event, sd sessionDados, ts, now time.Time) (bool, error) {
	if err := touchNode(tx, rec.Maquina, now); err != nil {
		return false, err
	}
	switch rec.Tipo {
	case outbox.TipoJobEvent:
		return ingestJobEvent(tx, s.eventsPerJob, rec, ev, now)
	case outbox.TipoDispatch, outbox.TipoAmend:
		return ingestDispatch(tx, rec, ts)
	case outbox.TipoSession:
		return ingestSession(tx, rec, sd, ts, now)
	case outbox.TipoDecision:
		return ingestDecision(tx, rec, ts)
	}
	return false, invalidErr("tipo", "unknown tipo")
}

// validateRecord checks the record shape of every outbox type and returns
// the parsed record ts.
func validateRecord(rec outbox.Record) (time.Time, error) {
	if rec.Schema != 1 {
		return time.Time{}, invalidErr("schema", "must be 1")
	}
	if !recordTypes[rec.Tipo] {
		return time.Time{}, invalidErr("tipo", "unknown tipo")
	}
	if err := validateMachine(rec.Maquina); err != nil {
		return time.Time{}, err
	}
	if rec.Seq < 1 {
		return time.Time{}, invalidErr("seq", "must be positive")
	}
	if len(rec.TS) > maxTSLen {
		return time.Time{}, invalidErr("ts", "too long")
	}
	ts, err := time.Parse(time.RFC3339, rec.TS)
	if err != nil {
		return time.Time{}, invalidErr("ts", "must be RFC 3339 with an explicit offset")
	}
	if rec.IdempotencyKey == "" {
		return time.Time{}, invalidErr("idempotency_key", "required")
	}
	if len(rec.IdempotencyKey) > 200 {
		return time.Time{}, invalidErr("idempotency_key", "too long")
	}
	if rec.Projeto != "" {
		if _, err := normalizeProjeto(rec.Projeto); err != nil {
			return time.Time{}, err
		}
	}
	if rec.JobID != nil {
		if err := validateJobID(*rec.JobID); err != nil {
			return time.Time{}, err
		}
	}
	if !isJSONObject(rec.Dados) {
		return time.Time{}, invalidErr("dados", "must be a JSON object")
	}
	switch rec.Tipo {
	case outbox.TipoJobEvent, outbox.TipoDispatch, outbox.TipoAmend:
		if rec.JobID == nil {
			return time.Time{}, invalidErr("job_id", "required for "+rec.Tipo)
		}
	}
	return ts, nil
}

// isJSONObject reports whether dados is a JSON object: trimmed, it starts
// with '{' and is valid JSON.
func isJSONObject(d json.RawMessage) bool {
	d = bytes.TrimSpace(d)
	return len(d) > 0 && d[0] == '{' && json.Valid(d)
}

// sessionDados is the validated payload of a session record: the acao and
// the projeto are checked with an error; a bad card, branch or pr is
// dropped (nil), never a failing record.
type sessionDados struct {
	Acao    string
	Projeto string
	Branch  *string
	Card    *string
	PR      *string
	Job     string
}

// sessionRaw is the wire shape of a session dados: all strings, empty
// omitted, estado not stored (docs/protocol.md, "Sessions").
type sessionRaw struct {
	Acao    string `json:"acao"`
	Projeto string `json:"projeto"`
	Branch  string `json:"branch"`
	Card    string `json:"card"`
	Job     string `json:"job"`
	Estado  string `json:"estado"`
	PR      string `json:"pr"`
}

func decodeSession(rec outbox.Record) (sessionDados, error) {
	var raw sessionRaw
	if err := json.Unmarshal(rec.Dados, &raw); err != nil {
		return sessionDados{}, invalidErr("dados", "undecodable session")
	}
	switch raw.Acao {
	case "start", "update", "end":
	default:
		return sessionDados{}, invalidErr("acao", "must be start, update or end")
	}
	if raw.Projeto != "" {
		if _, err := normalizeProjeto(raw.Projeto); err != nil {
			return sessionDados{}, err
		}
	}
	if raw.Job != "" {
		if err := validateJobID(raw.Job); err != nil {
			return sessionDados{}, err
		}
	}
	var sd sessionDados
	sd.Acao, sd.Projeto, sd.Job = raw.Acao, raw.Projeto, raw.Job
	if raw.Branch != "" {
		if b, err := normalizeBranch(raw.Branch); err == nil {
			sd.Branch = &b
		}
	}
	if raw.Card != "" {
		if c, err := normalizeCard(raw.Card); err == nil {
			sd.Card = &c
		}
	}
	if raw.PR != "" {
		if p, err := normalizePR(raw.PR); err == nil {
			sd.PR = &p
		}
	}
	return sd, nil
}

// touchNode upserts the node; the last contact moves forward only, by
// the unix column, so a late record never regresses a newer contact.
func touchNode(tx *sql.Tx, maquina string, at time.Time) error {
	if err := admitNewNode(tx, maquina); err != nil {
		return err
	}
	_, err := tx.Exec(`INSERT INTO nodes(maquina, ultimo_contato, ultimo_contato_unix) VALUES(?,?,?)
		ON CONFLICT(maquina) DO UPDATE SET
			ultimo_contato = CASE WHEN ultimo_contato_unix IS NULL OR ultimo_contato_unix < excluded.ultimo_contato_unix THEN excluded.ultimo_contato ELSE ultimo_contato END,
			ultimo_contato_unix = CASE WHEN ultimo_contato_unix IS NULL OR ultimo_contato_unix < excluded.ultimo_contato_unix THEN excluded.ultimo_contato_unix ELSE ultimo_contato_unix END`,
		maquina, at.Format(outbox.TSLayout), at.Unix())
	return err
}

// sweepEnded deletes the ended rows past the retention, of every kind,
// with their events. A row inside the retention stays: a fence within the
// replay-fence window, and a future-dated row (the clock went backwards),
// are not past the retention, and a row ended exactly at it is not past
// it.
func sweepEnded(tx *sql.Tx, retention time.Duration, now time.Time) error {
	cutoff := now.Unix() - int64(retention/time.Second)
	if _, err := tx.Exec(`DELETE FROM events WHERE (maquina, chave) IN
		(SELECT maquina, chave FROM jobs WHERE encerrado = 1 AND encerrado_unix < ?)`, cutoff); err != nil {
		return err
	}
	_, err := tx.Exec(`DELETE FROM jobs WHERE encerrado = 1 AND encerrado_unix < ?`, cutoff)
	return err
}

// SweepEnded runs the ended-row sweep in one transaction of its own, the
// periodic sweep serve runs once at start and then every 24 h, whatever
// the requests: now is the caller's clock, and the predicate is the
// sweep's, so a row ended exactly at the retention stays. A failure
// leaves nothing half-deleted and the next sweep retries; a confirmed
// sweep runs the high-count check after the transaction, outside it.
func (s *Store) SweepEnded(now time.Time) error {
	err := s.tx(func(tx *sql.Tx) error {
		return sweepEnded(tx, s.endedRetention, now)
	})
	if err == nil {
		s.checkHighCount()
	}
	return err
}

// jobRow is the stored state of one job row, as far as the ingest and the
// poll apply need it.
type jobRow struct {
	maquina       string
	chave         string
	origem        string
	jobID         *string
	workspace     *string
	workspaceID   *string
	agente        *string
	agenteFonte   *string
	card          *string
	projeto       *string
	branch        *string
	pr            *string
	fase          *string
	herdrStatus   *string
	terminal      int
	saida         *int
	esperando     int
	esperaMotivo  *string
	ultimoSeq     int64
	ultima        *string
	ultimaUnix    int64 // valid when ultima is set
	obsoleto      int
	fontePush     int
	sessaoTs      *int64
	sessaoSeq     int64
	encerrado     int
	encerradoUnix int64
}

// jobRowOf reads one job row; nil when absent.
func jobRowOf(tx *sql.Tx, maquina, chave string) (*jobRow, error) {
	var r jobRow
	var encerrado, terminal, obsoleto, esperando, fontePush int
	var ultimoSeq, ultimaUnix int64
	var encerradoUnix sql.NullInt64
	var jobID, workspace, workspaceID, agente, agenteFonte, card, projeto, branch, pr, fase, herdrStatus, esperaMotivo, ultima, origem sql.NullString
	var saida sql.NullInt64
	var sessaoTs, sessaoSeq sql.NullInt64
	err := tx.QueryRow(`SELECT origem, job_id, workspace, workspace_id, agente, agente_fonte, card, projeto, branch, pr, fase, herdr_status, terminal, saida, esperando_humano, espera_motivo, ultimo_seq, ultima_atividade, ultima_atividade_unix, obsoleto, fonte_push, sessao_ts_unix, sessao_seq, encerrado, encerrado_unix
		FROM jobs WHERE maquina = ? AND chave = ?`, maquina, chave).
		Scan(&origem, &jobID, &workspace, &workspaceID, &agente, &agenteFonte, &card, &projeto, &branch, &pr, &fase, &herdrStatus, &terminal, &saida, &esperando, &esperaMotivo, &ultimoSeq, &ultima, &ultimaUnix, &obsoleto, &fontePush, &sessaoTs, &sessaoSeq, &encerrado, &encerradoUnix)
	if errors.Is(err, errNoRow) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.maquina, r.chave = maquina, chave
	r.origem = origem.String
	r.jobID = nullOf(jobID)
	r.workspace = nullOf(workspace)
	r.workspaceID = nullOf(workspaceID)
	r.agente = nullOf(agente)
	r.agenteFonte = nullOf(agenteFonte)
	r.card = nullOf(card)
	r.projeto = nullOf(projeto)
	r.branch = nullOf(branch)
	r.pr = nullOf(pr)
	r.fase = nullOf(fase)
	r.herdrStatus = nullOf(herdrStatus)
	r.terminal = terminal
	if saida.Valid {
		v := int(saida.Int64)
		r.saida = &v
	}
	r.esperando = esperando
	r.esperaMotivo = nullOf(esperaMotivo)
	r.ultimoSeq = ultimoSeq
	r.ultima = nullOf(ultima)
	r.ultimaUnix = ultimaUnix
	r.obsoleto = obsoleto
	r.fontePush = fontePush
	if sessaoTs.Valid {
		v := sessaoTs.Int64
		r.sessaoTs = &v
	}
	r.sessaoSeq = sessaoSeq.Int64
	r.encerrado = encerrado
	if encerradoUnix.Valid {
		r.encerradoUnix = encerradoUnix.Int64
	}
	return &r, nil
}

func nullOf(n sql.NullString) *string {
	if !n.Valid {
		return nil
	}
	v := n.String
	return &v
}

func strPtr(v string) *string { return &v }

// ringEvent is one stored event of the per-job ring.
type ringEvent struct {
	seq int64
	ts  string
}

// ringEvents lists the stored events of one job, oldest first.
func ringEvents(tx *sql.Tx, maquina, chave string) ([]ringEvent, error) {
	rows, err := tx.Query(`SELECT seq, ts FROM events WHERE maquina = ? AND chave = ? ORDER BY seq ASC, ts ASC`, maquina, chave)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ringEvent
	for rows.Next() {
		var e ringEvent
		if err := rows.Scan(&e.seq, &e.ts); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ingestJobEvent applies one job event: the job row, the per-job event
// ring and the event's state mapping. It reports applied = false for a
// tombstoned job, a duplicate and a stale replay below a full ring; the
// refusals are decided before the job row is touched, so a refused record
// changes no job column.
func ingestJobEvent(tx *sql.Tx, eventsPerJob int, rec outbox.Record, ev jobapi.Event, now time.Time) (bool, error) {
	jobID := *rec.JobID
	row, err := jobRowOf(tx, rec.Maquina, jobID)
	if err != nil {
		return false, err
	}
	if row != nil && row.encerrado == 1 {
		// A tombstoned job ignores every event.
		return false, nil
	}
	stored, err := ringEvents(tx, rec.Maquina, jobID)
	if err != nil {
		return false, err
	}
	for _, e := range stored {
		if e.seq == ev.Seq {
			// Duplicate: the same seq is already stored.
			return false, nil
		}
	}
	if len(stored) >= eventsPerJob && ev.Seq < stored[0].seq {
		// A stale replay below a full ring changes nothing.
		return false, nil
	}
	if len(ev.TS) > maxTSLen {
		return false, invalidErr("ts", "too long")
	}
	evTS, err := time.Parse(time.RFC3339, ev.TS)
	if err != nil {
		// The event ts came from the machine's outbox; a broken ts must
		// not corrupt the registry.
		return false, invalidErr("ts", "must be RFC 3339 with an explicit offset")
	}
	// The instant is kept, in the canonical numeric-offset layout: a `Z`
	// input is stored as `+00:00` and sub-second digits are dropped, as
	// in the record path.
	evTSOut := evTS.Format(outbox.TSLayout)
	if row == nil {
		// A cleanup of a job never seen before creates a fence row.
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, projeto, fonte_push, obsoleto)
			VALUES(?,?,?, ?,?, 1, 0)`,
			rec.Maquina, jobID, OrigemJob, jobID, nullStr(rec.Projeto)); err != nil {
			return false, err
		}
	} else {
		if _, err := tx.Exec(`UPDATE jobs SET fonte_push = 1, obsoleto = 0, projeto = COALESCE(projeto, ?)
			WHERE maquina = ? AND chave = ?`, nullStr(rec.Projeto), rec.Maquina, jobID); err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo, refs) VALUES(?,?,?,?,?,?,?)`,
		rec.Maquina, jobID, ev.Seq, evTSOut, ev.Tipo, cutResumo(ev.Resumo), reduceRefs(ev.Refs)); err != nil {
		return false, err
	}
	// Keep only the EventsPerJob highest seqs, dropping the oldest.
	full := append(append([]ringEvent{}, stored...), ringEvent{seq: ev.Seq, ts: evTSOut})
	sort.Slice(full, func(i, j int) bool {
		if full[i].seq != full[j].seq {
			return full[i].seq < full[j].seq
		}
		return full[i].ts < full[j].ts
	})
	if len(full) > eventsPerJob {
		for _, e := range full[:len(full)-eventsPerJob] {
			if _, err := tx.Exec(`DELETE FROM events WHERE maquina = ? AND chave = ? AND seq = ?`,
				rec.Maquina, jobID, e.seq); err != nil {
				return false, err
			}
		}
	}
	// The last activity moves forward only (compared by the unix
	// seconds, so the zones never matter).
	if _, err := tx.Exec(`UPDATE jobs SET ultima_atividade = ?, ultima_atividade_unix = ?
		WHERE maquina = ? AND chave = ? AND (ultima_atividade_unix IS NULL OR ultima_atividade_unix < ?)`,
		evTSOut, evTS.Unix(), rec.Maquina, jobID, evTS.Unix()); err != nil {
		return false, err
	}
	// A parseable refs.pr fills a null pr.
	if pr := parseRefPR(ev.Refs); pr != "" {
		if _, err := tx.Exec(`UPDATE jobs SET pr = ? WHERE maquina = ? AND chave = ? AND pr IS NULL`,
			pr, rec.Maquina, jobID); err != nil {
			return false, err
		}
	}
	// The state mapping runs only for a forward sequence.
	if ev.Seq <= ultimoSeqOf(row) {
		return true, nil
	}
	if _, err := tx.Exec(`UPDATE jobs SET ultimo_seq = ?, fase = ? WHERE maquina = ? AND chave = ?`,
		ev.Seq, ev.Tipo, rec.Maquina, jobID); err != nil {
		return false, err
	}
	switch ev.Tipo {
	case jobapi.TypeQuestion:
		err = setWait(tx, rec.Maquina, jobID, 1, "question")
	case jobapi.TypeBlocked:
		err = setWait(tx, rec.Maquina, jobID, 1, "blocked")
	case jobapi.TypeUnblocked:
		// The wait always clears; a blocked terminal (blockedSaida) moves
		// back to running, a done terminal never reopens.
		if row != nil && row.terminal == 1 && blockedSaida(row.saida) {
			if _, err = tx.Exec(`UPDATE jobs SET terminal = 0, saida = NULL WHERE maquina = ? AND chave = ?`,
				rec.Maquina, jobID); err != nil {
				return false, err
			}
		}
		err = setWait(tx, rec.Maquina, jobID, 0, "")
	case jobapi.TypeTerminal:
		saida := parseSaida(ev.Refs)
		if _, err = tx.Exec(`UPDATE jobs SET terminal = 1, saida = ? WHERE maquina = ? AND chave = ?`,
			saida, rec.Maquina, jobID); err != nil {
			return false, err
		}
		if blockedSaida(saida) {
			err = setWait(tx, rec.Maquina, jobID, 1, "blocked")
		} else {
			err = setWait(tx, rec.Maquina, jobID, 0, "")
		}
	case jobapi.TypeCleanup:
		// The tombstone: the job leaves the inventory, its events go
		// with it.
		if _, err = tx.Exec(`UPDATE jobs SET encerrado = 1, encerrado_unix = ? WHERE maquina = ? AND chave = ?`,
			now.Unix(), rec.Maquina, jobID); err != nil {
			return false, err
		}
		_, err = tx.Exec(`DELETE FROM events WHERE maquina = ? AND chave = ?`, rec.Maquina, jobID)
	default:
		// Every other type only moved the fase.
	}
	return true, err
}

// ultimoSeqOf is the stored ultimo_seq, 0 when the row was just created.
func ultimoSeqOf(row *jobRow) int64 {
	if row == nil {
		return 0
	}
	return row.ultimoSeq
}

// setWait sets the job's human wait; an empty motivo clears it.
func setWait(tx *sql.Tx, maquina, chave string, esperando int, motivo string) error {
	m := motivo
	var mp *string
	if m != "" {
		mp = &m
	}
	_, err := tx.Exec(`UPDATE jobs SET esperando_humano = ?, espera_motivo = ? WHERE maquina = ? AND chave = ?`,
		esperando, mp, maquina, chave)
	return err
}

// cutResumo caps the event resumo at MaxResumo code points.
func cutResumo(resumo string) string {
	if utf8.RuneCountInString(resumo) <= MaxResumo {
		return resumo
	}
	return string([]rune(resumo)[:MaxResumo])
}

// reduceRefs keeps only the allowlisted refs keys (registry.EventRefsKeys)
// whose value is a JSON string of at most MaxRefValueBytes bytes, a JSON
// number or a boolean; the local paths (report, brief) and the other
// values are dropped, never an error. nil when empty.
func reduceRefs(refs map[string]any) *string {
	if len(refs) == 0 {
		return nil
	}
	allowed := make(map[string]bool, len(EventRefsKeys))
	for _, k := range EventRefsKeys {
		allowed[k] = true
	}
	out := make(map[string]any, 0)
	for k, v := range refs {
		if allowed[k] && keptRefValue(v) {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	v := string(b)
	return &v
}

// maxTSLen bounds a record or event timestamp: an RFC 3339 time with
// nanoseconds and a numeric offset is 35 bytes, and the text is stored as
// received.
const maxTSLen = 35

// MaxRefValueBytes bounds a kept ref string. keptRefValue: a JSON string
// of at most 200 bytes, a JSON number or a boolean is kept; anything else
// (object, array, null, longer string) is dropped.
const MaxRefValueBytes = 200

func keptRefValue(v any) bool {
	switch t := v.(type) {
	case string:
		return len(t) <= MaxRefValueBytes
	case float64:
		return true
	case bool:
		return true
	}
	return false
}

// parseRefPR extracts a parseable refs.pr.
func parseRefPR(refs map[string]any) string {
	v, ok := refs["pr"].(string)
	if !ok {
		return ""
	}
	pr, err := normalizePR(v)
	if err != nil {
		return ""
	}
	return pr
}

// parseSaida extracts the integer exit of a terminal event: a JSON number
// with no fraction or a decimal string; anything else is null.
func parseSaida(refs map[string]any) *int {
	v, ok := refs["exit"]
	if !ok {
		return nil
	}
	switch n := v.(type) {
	case float64:
		if n == math.Trunc(n) && n >= math.MinInt32 && n <= math.MaxInt32 {
			i := int(n)
			return &i
		}
	case string:
		if i, err := strconv.ParseInt(n, 10, 32); err == nil {
			i := int(i)
			return &i
		}
	}
	return nil
}

// blockedSaida reports whether a terminal exit keeps the job alive and
// waiting: 7 needs a person and 11/14 are quota or provider errors, all
// `blocked` in the contract, waiting for `job amend`. The SQL that
// carries the same set (jobConflicts) is named after this helper.
func blockedSaida(saida *int) bool {
	return saida != nil && (*saida == 7 || *saida == 11 || *saida == 14)
}

// maxUltima sets the last activity forward only, by unix seconds, and
// stamps the push provenance of the job it touches.
func maxUltima(tx *sql.Tx, maquina, chave string, ts time.Time) error {
	_, err := tx.Exec(`UPDATE jobs SET
		fonte_push = 1,
		ultima_atividade = CASE WHEN ultima_atividade_unix IS NULL OR ultima_atividade_unix < ? THEN ? ELSE ultima_atividade END,
		ultima_atividade_unix = CASE WHEN ultima_atividade_unix IS NULL OR ultima_atividade_unix < ? THEN ? ELSE ultima_atividade_unix END
		WHERE maquina = ? AND chave = ?`,
		ts.Unix(), ts.Format(outbox.TSLayout), ts.Unix(), ts.Unix(), maquina, chave)
	return err
}

// ingestDispatch upserts the job of a dispatch or amend record without
// interpreting its dados.
func ingestDispatch(tx *sql.Tx, rec outbox.Record, ts time.Time) (bool, error) {
	jobID := *rec.JobID
	row, err := jobRowOf(tx, rec.Maquina, jobID)
	if err != nil {
		return false, err
	}
	if row != nil && row.encerrado == 1 {
		// A tombstoned job is left unchanged.
		return true, nil
	}
	if row == nil {
		_, err = tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, projeto, fonte_push, obsoleto, ultima_atividade, ultima_atividade_unix)
			VALUES(?,?, ?, ?, ?, 1, 0, ?, ?)`,
			rec.Maquina, jobID, OrigemJob, jobID, nullStr(rec.Projeto), ts.Format(outbox.TSLayout), ts.Unix())
		return true, err
	}
	if _, err = tx.Exec(`UPDATE jobs SET fonte_push = 1, obsoleto = 0, projeto = COALESCE(projeto, ?)
		WHERE maquina = ? AND chave = ?`, nullStr(rec.Projeto), rec.Maquina, jobID); err != nil {
		return false, err
	}
	return true, maxUltima(tx, rec.Maquina, jobID, ts)
}

// ingestSession applies a session record to its target: the named job or
// the sessao:<projeto>#<branch> row. An accepted record replaces the
// stored value of every validated non-empty field it carries; a field it
// omits (or carries invalid) keeps the stored value. A jobless end for a
// target that does not exist yet creates the row already tombstoned.
// Every job it creates or updates gets the push provenance.
func ingestSession(tx *sql.Tx, rec outbox.Record, sd sessionDados, ts, now time.Time) (bool, error) {
	projeto := sd.Projeto
	if projeto == "" {
		projeto = rec.Projeto
	}
	var chave, origem string
	switch {
	case sd.Job != "":
		chave, origem = sd.Job, OrigemJob
	case projeto != "" && sd.Branch != nil:
		chave, origem = "sessao:"+projeto+"#"+*sd.Branch, OrigemSessao
	}
	if chave == "" {
		// No target: only the node was touched.
		return true, nil
	}
	row, err := jobRowOf(tx, rec.Maquina, chave)
	if err != nil {
		return false, err
	}
	if row != nil && row.sessaoTs != nil && (*row.sessaoTs > ts.Unix() || (*row.sessaoTs == ts.Unix() && row.sessaoSeq >= rec.Seq)) {
		// An older record after a newer one, or a replay: ignored. The
		// outbox seq of the machine orders records of the same second.
		return false, nil
	}
	if row == nil {
		var jobID *string
		if origem == OrigemJob {
			jobID = strPtr(chave)
		}
		encerrado := 0
		var encerradoUnix *int64
		if sd.Acao == "end" && origem == OrigemSessao {
			// The end-first target is created already tombstoned, so it
			// never appears in the inventory or the check.
			encerrado = 1
			v := now.Unix()
			encerradoUnix = &v
		}
		_, err = tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, card, branch, pr, projeto, fonte_push, sessao_ts_unix, sessao_seq, encerrado, encerrado_unix, ultima_atividade, ultima_atividade_unix)
			VALUES(?,?,?,?,?,?,?,?,1,?,?,?,?,?,?)`,
			rec.Maquina, chave, origem, jobID, sd.Card, sd.Branch, sd.PR, nullStr(projeto), ts.Unix(), rec.Seq, encerrado, encerradoUnix, ts.Format(outbox.TSLayout), ts.Unix())
		return true, err
	}
	if row.encerrado == 1 {
		if row.origem != OrigemSessao || sd.Acao == "end" {
			// A closed job, or a closed session told again it ended,
			// stays closed; the ordering facts still move forward.
			_, err = tx.Exec(`UPDATE jobs SET sessao_ts_unix = ?, sessao_seq = ? WHERE maquina = ? AND chave = ?`,
				ts.Unix(), rec.Seq, rec.Maquina, chave)
			return true, err
		}
		// A newer start or update opens a new session on the same
		// project and branch; nothing of the closed one carries over. It
		// reuses the row, so it adds no row.
		if _, err = tx.Exec(`UPDATE jobs SET encerrado = 0, encerrado_unix = NULL, card = NULL, pr = NULL WHERE maquina = ? AND chave = ?`,
			rec.Maquina, chave); err != nil {
			return false, err
		}
	}
	// A validated non-empty field replaces the stored value; an omitted
	// field keeps it.
	proj := nullStr(projeto)
	if _, err = tx.Exec(`UPDATE jobs SET
		sessao_ts_unix = ?,
		sessao_seq = ?,
		fonte_push = 1,
		card = CASE WHEN ? IS NULL THEN card ELSE ? END,
		branch = CASE WHEN ? IS NULL THEN branch ELSE ? END,
		pr = CASE WHEN ? IS NULL THEN pr ELSE ? END,
		projeto = CASE WHEN ? IS NULL THEN projeto ELSE ? END
		WHERE maquina = ? AND chave = ?`,
		ts.Unix(), rec.Seq, sd.Card, sd.Card, sd.Branch, sd.Branch, sd.PR, sd.PR, proj, proj, rec.Maquina, chave); err != nil {
		return false, err
	}
	if err := maxUltima(tx, rec.Maquina, chave, ts); err != nil {
		return false, err
	}
	if sd.Acao == "end" && row.origem == OrigemSessao {
		// The end tombstones an open session row; on a job row it
		// changes nothing else.
		_, err = tx.Exec(`UPDATE jobs SET encerrado = 1, encerrado_unix = ? WHERE maquina = ? AND chave = ?`,
			now.Unix(), rec.Maquina, chave)
		return true, err
	}
	return true, nil
}

// ingestDecision touches the node and, for an existing non-tombstoned
// job, only its last activity.
func ingestDecision(tx *sql.Tx, rec outbox.Record, ts time.Time) (bool, error) {
	if rec.JobID == nil {
		return true, nil
	}
	row, err := jobRowOf(tx, rec.Maquina, *rec.JobID)
	if err != nil {
		return false, err
	}
	if row == nil || row.encerrado == 1 {
		return true, nil
	}
	return true, maxUltima(tx, rec.Maquina, *rec.JobID, ts)
}
