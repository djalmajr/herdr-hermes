package registry

// Tests of the total retention budget (S-F3): per-kind budgets over the
// TOTAL retained rows (active plus ended), the 7-day replay fence, the
// configurable ended retention, the oldest-ended eviction with its
// deterministic order, the atomic refusal, the poll skip and the alert
// hygiene. The stores use tiny budgets (Options{MaxJobs, MaxSessions})
// and the existing fixed clock; large populations are seeded with direct
// SQL in one transaction, and the boundary is crossed through the API.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

const (
	retDay       = 24 * 3600
	retSeed1Day  = retDay
	retSeed8Day  = 8 * retDay
	retSeed31Day = 31 * retDay
)

// retStore opens a fresh store with the given options and closes it on
// cleanup.
func retStore(t *testing.T, opts Options) *Store {
	t.Helper()
	if opts.Now == nil {
		opts.Now = fixedClock()
	}
	s, err := Open(pathFor(t), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// retEndedJob seeds one ended job row (with one event) at the given
// age, in one transaction; like every real row it carries its last
// activity.
func retEndedJob(t *testing.T, s *Store, maquina, chave string, age int64) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix, ultima_atividade, ultima_atividade_unix)
		VALUES(?,?, 'job', ?, 1, ?, ?, ?)`, maquina, chave, chave, fixedNow.Unix()-age, fixedNow.Format(outbox.TSLayout), fixedNow.Unix()-age); err != nil {
		t.Fatalf("seed the ended job %s: %v", chave, err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES(?, ?, 1, '2026-01-01T00:00:00-03:00', 'note', 'x')`, maquina, chave); err != nil {
		t.Fatalf("seed the event of %s: %v", chave, err)
	}
}

// retEndedSessao seeds one ended sessao row (with one event) at the
// given age; a negative age is a future-dated row. Like every real row
// it carries its last activity.
func retEndedSessao(t *testing.T, s *Store, maquina, chave string, age int64) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix, ultima_atividade, ultima_atividade_unix)
		VALUES(?,?, 'sessao', 1, ?, ?, ?)`, maquina, chave, fixedNow.Unix()-age, fixedNow.Format(outbox.TSLayout), fixedNow.Unix()-age); err != nil {
		t.Fatalf("seed the ended session %s: %v", chave, err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES(?, ?, 1, '2026-01-01T00:00:00-03:00', 'note', 'x')`, maquina, chave); err != nil {
		t.Fatalf("seed the event of %s: %v", chave, err)
	}
}

// retActiveJob seeds one active job row with its last activity set.
func retActiveJob(t *testing.T, s *Store, maquina, chave string) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, ultima_atividade, ultima_atividade_unix) VALUES(?,?, 'job', ?, ?, ?)`,
		maquina, chave, chave, fixedNow.Format(outbox.TSLayout), fixedNow.Unix()); err != nil {
		t.Fatalf("seed the active job %s: %v", chave, err)
	}
}

// retJobEventRec builds one job_event record for the named job.
func retJobEventRec(jobID string, seq int64, tipo string) outbox.Record {
	return recordBase(func(r *outbox.Record) {
		r.JobID = ptr(jobID)
		r.IdempotencyKey = fmt.Sprintf("%s:%d", jobID, seq)
		r.Dados = eventData(seq, tipo, "x", nil)
	})
}

// retWantRefusal checks the ErrInvalid refusal and its exact text.
func retWantRefusal(t *testing.T, err error, kind string) {
	t.Helper()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want the ErrInvalid refusal", err)
	}
	if got := strings.TrimPrefix(err.Error(), ErrInvalid.Error()+": "); got != kind+" limit reached" {
		t.Fatalf("refusal text = %q, want %q limit reached", got, kind)
	}
}

// retDump is the full state of every user table: one line per row with
// the table name and every column, in the primary-key order.
func retDump(t *testing.T, s *Store) map[string][]string {
	t.Helper()
	tables := map[string]string{
		"nodes":    `SELECT * FROM nodes ORDER BY maquina`,
		"jobs":     `SELECT * FROM jobs ORDER BY maquina, chave`,
		"events":   `SELECT * FROM events ORDER BY maquina, chave, seq`,
		"waits":    `SELECT * FROM waits ORDER BY maquina, pane`,
		"claims":   `SELECT * FROM claims ORDER BY id`,
		"policies": `SELECT * FROM policies ORDER BY escopo, valor`,
		"folders":  `SELECT * FROM folders ORDER BY maquina, caminho`,
	}
	out := make(map[string][]string, len(tables))
	for table, query := range tables {
		rows, err := s.db.Query(query)
		if err != nil {
			t.Fatalf("dump %s: %v", table, err)
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatalf("dump %s columns: %v", table, err)
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				t.Fatalf("dump %s scan: %v", table, err)
			}
			parts := make([]string, 0, len(cols)+1)
			parts = append(parts, table)
			for _, v := range vals {
				switch x := v.(type) {
				case nil:
					parts = append(parts, "\\N")
				case int64:
					parts = append(parts, fmt.Sprintf("%d", x))
				case []byte:
					parts = append(parts, string(x))
				default:
					parts = append(parts, fmt.Sprintf("%v", x))
				}
			}
			out[table] = append(out[table], strings.Join(parts, "\x1f"))
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatalf("dump %s: %v", table, err)
		}
		rows.Close()
	}
	return out
}

// TestRetentionAdmissionToBudget: with MaxJobs 2 the first two new job
// rows are admitted and the third is refused, when every retained row is
// active (an active-only full budget evicts nothing).
func TestRetentionAdmissionToBudget(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2, MaxSessions: 2})
	if !mustIngest(t, s, retJobEventRec("job-1", 1, jobapi.TypeAccepted)) {
		t.Fatal("job-1: applied = false")
	}
	if !mustIngest(t, s, retJobEventRec("job-2", 1, jobapi.TypeAccepted)) {
		t.Fatal("job-2: applied = false")
	}
	_, err := s.Ingest(retJobEventRec("job-3", 1, jobapi.TypeAccepted))
	retWantRefusal(t, err, "jobs")
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2 (the refusal created nothing)", n)
	}
	if f := jobFact(t, s, "node-a", "job-1"); f.encerrado != 0 {
		t.Errorf("active row after the refusal = %+v, want still live", f)
	}
}

// TestRetentionRefusalUnexpiredFences: a budget filled with ended rows
// inside the 7-day fence refuses the new end-first session and evicts
// nothing.
func TestRetentionRefusalUnexpiredFences(t *testing.T) {
	s := retStore(t, Options{MaxSessions: 2})
	retEndedSessao(t, s, "node-a", "sessao:org001/repo#feat/a", retSeed1Day)
	retEndedSessao(t, s, "node-a", "sessao:org002/repo#feat/b", retSeed1Day)
	_, err := s.Ingest(sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", ""))
	retWantRefusal(t, err, "sessions")
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2 (the fences were not evicted)", n)
	}
	if f := jobFact(t, s, "node-a", "sessao:org001/repo#feat/a"); !f.exists || f.encerrado != 1 {
		t.Errorf("fence after the refusal = %+v, want untouched", f)
	}
}

// TestRetentionRefusalFutureFence: an ended row with a future
// encerrado_unix (the clock went backwards) is treated as unexpired: it
// is not evicted and the new row is refused.
func TestRetentionRefusalFutureFence(t *testing.T) {
	s := retStore(t, Options{MaxSessions: 2})
	retEndedSessao(t, s, "node-a", "sessao:org001/repo#feat/a", -3600) // 1 h in the future
	retEndedSessao(t, s, "node-a", "sessao:org002/repo#feat/b", -3600)
	_, err := s.Ingest(sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", ""))
	retWantRefusal(t, err, "sessions")
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2 (the future fences were not evicted)", n)
	}
	// The future rows are also not swept by the retention: they are not
	// past it.
	if f := jobFact(t, s, "node-a", "sessao:org001/repo#feat/a"); !f.exists {
		t.Errorf("future fence after the Ingest = %+v, want still present", f)
	}
}

// TestRetentionEvictsExpiredFence: at a full jobs budget a new job evicts
// the oldest ended row past the fence (8 days: past the fence, inside
// the 30-day retention), with its event.
func TestRetentionEvictsExpiredFence(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2})
	retActiveJob(t, s, "node-a", "job-1")
	retEndedJob(t, s, "node-a", "job-old", retSeed8Day)
	if !mustIngest(t, s, retJobEventRec("job-2", 1, jobapi.TypeAccepted)) {
		t.Fatal("job-2: applied = false, want admitted by the eviction")
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Fatalf("jobs rows = %d, want 2 (one evicted, one inserted)", n)
	}
	if f := jobFact(t, s, "node-a", "job-old"); f.exists {
		t.Errorf("job-old = %+v, want evicted with its event", f)
	}
	if n := tableCount(t, s, "events"); n != 1 {
		t.Errorf("events = %d, want 1 (the new job's; the evicted row's event went with it)", n)
	}
	if f := jobFact(t, s, "node-a", "job-2"); !f.exists || f.encerrado != 0 {
		t.Errorf("job-2 = %+v, want the new live row", f)
	}
}

// TestRetentionEvictionOrderAndTies: the eviction takes the oldest
// encerrado_unix first and breaks ties by maquina, then by chave.
func TestRetentionEvictionOrderAndTies(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2})
	// Four ended rows: one 10 days old, three 9 days old. The 10-day
	// row and the two (node-a, j0/j3) rows of the 9-day tie go; the
	// (node-b, j2) row of the tie survives with the new row.
	retEndedJob(t, s, "node-a", "j1", 10*retDay)
	retEndedJob(t, s, "node-b", "j2", 9*retDay)
	retEndedJob(t, s, "node-a", "j3", 9*retDay)
	retEndedJob(t, s, "node-a", "j0", 9*retDay)
	if !mustIngest(t, s, retJobEventRec("j-new", 1, jobapi.TypeAccepted)) {
		t.Fatal("j-new: applied = false")
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Fatalf("jobs rows = %d, want 2", n)
	}
	if f := jobFact(t, s, "node-a", "j1"); f.exists {
		t.Errorf("j1 (the oldest) = %+v, want evicted first", f)
	}
	if f := jobFact(t, s, "node-a", "j0"); f.exists {
		t.Errorf("j0 (the tie by maquina then chave) = %+v, want evicted", f)
	}
	if f := jobFact(t, s, "node-a", "j3"); f.exists {
		t.Errorf("j3 = %+v, want evicted (only the needed count is evicted)", f)
	}
	if f := jobFact(t, s, "node-b", "j2"); !f.exists || f.encerrado != 1 {
		t.Errorf("j2 (the node-b row of the tie) = %+v, want the survivor", f)
	}
	if f := jobFact(t, s, "node-a", "j-new"); !f.exists || f.encerrado != 0 {
		t.Errorf("j-new = %+v, want the new live row", f)
	}
}

// TestRetentionEvictExactlyNeededOverBudget: a database already above
// the budget evicts exactly the number the new row needs.
func TestRetentionEvictExactlyNeededOverBudget(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2})
	retEndedJob(t, s, "node-a", "e1", 10*retDay)
	retEndedJob(t, s, "node-a", "e2", 9*retDay)
	retEndedJob(t, s, "node-a", "e3", 8*retDay)
	retEndedJob(t, s, "node-a", "e4", 7*retDay) // exactly at the fence: evictable
	if !mustIngest(t, s, retJobEventRec("j-new", 1, jobapi.TypeAccepted)) {
		t.Fatal("j-new: applied = false")
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Fatalf("jobs rows = %d, want 2 (three evicted, one inserted)", n)
	}
	for _, chave := range []string{"e1", "e2", "e3"} {
		if f := jobFact(t, s, "node-a", chave); f.exists {
			t.Errorf("%s = %+v, want evicted", chave, f)
		}
	}
	if f := jobFact(t, s, "node-a", "e4"); !f.exists || f.encerrado != 1 {
		t.Errorf("e4 = %+v, want the survivor (only the needed count was evicted)", f)
	}
}

// TestRetentionNeverEvictsOtherKind: a full jobs budget refuses the new
// job although the sessions kind holds an evictable ended row, and vice
// versa; the other kind's rows are never touched.
func TestRetentionNeverEvictsOtherKind(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 1, MaxSessions: 1})
	retActiveJob(t, s, "node-a", "job-1")
	retEndedSessao(t, s, "node-a", "sessao:org001/repo#feat/a", retSeed8Day)
	_, err := s.Ingest(retJobEventRec("job-2", 1, jobapi.TypeAccepted))
	retWantRefusal(t, err, "jobs")
	if f := jobFact(t, s, "node-a", "sessao:org001/repo#feat/a"); !f.exists || f.encerrado != 1 {
		t.Errorf("the evictable session row = %+v, want untouched by the jobs refusal", f)
	}
	// The mirror: a full sessions budget refuses the new session
	// although the jobs kind holds an evictable ended row.
	s2 := retStore(t, Options{MaxJobs: 1, MaxSessions: 1})
	retEndedSessao(t, s2, "node-a", "sessao:org001/repo#feat/a", retSeed1Day)
	retEndedJob(t, s2, "node-a", "job-old", retSeed8Day)
	_, err = s2.Ingest(sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", ""))
	retWantRefusal(t, err, "sessions")
	if f := jobFact(t, s2, "node-a", "job-old"); !f.exists || f.encerrado != 1 {
		t.Errorf("the evictable job row = %+v, want untouched by the sessions refusal", f)
	}
}

// TestRetentionNeverEvictsActiveOrOtherTables: the refusal evicts
// nothing: the active row of the kind, and the claims, policies and
// folders rows, stay.
func TestRetentionNeverEvictsActiveOrOtherTables(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 1})
	retActiveJob(t, s, "node-a", "job-1")
	if _, err := s.db.Exec(`INSERT INTO claims(id, agente, card, criado, criado_unix, expira, expira_unix)
		VALUES(?, 'dja', 'DJA-1', '2026-10-10T12:00:00-03:00', ?, '2026-10-10T13:00:00-03:00', ?)`,
		strings.Repeat("1", 32), fixedNow.Unix(), fixedNow.Unix()+3600); err != nil {
		t.Fatalf("seed the claim: %v", err)
	}
	if err := s.SetPolicy(PolicyInput{Escopo: EscopoCard, Valor: "DJA-2", Hold: true}); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	if err := s.SetFolder(FolderInput{Maquina: "node-a", Caminho: "data/report", Pedido: "x"}); err != nil {
		t.Fatalf("SetFolder: %v", err)
	}
	_, err := s.Ingest(retJobEventRec("job-2", 1, jobapi.TypeAccepted))
	retWantRefusal(t, err, "jobs")
	if f := jobFact(t, s, "node-a", "job-1"); !f.exists || f.encerrado != 0 {
		t.Errorf("the active row = %+v, want never evicted", f)
	}
	if n := tableCount(t, s, "claims"); n != 1 {
		t.Errorf("claims = %d, want 1", n)
	}
	if n := tableCount(t, s, "policies"); n != 1 {
		t.Errorf("policies = %d, want 1", n)
	}
	if n := tableCount(t, s, "folders"); n != 1 {
		t.Errorf("folders = %d, want 1", n)
	}
}

// TestRetentionPerKindBudgets: the budgets are per kind: a full jobs
// budget does not block a new session and vice versa.
func TestRetentionPerKindBudgets(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 1, MaxSessions: 1})
	if !mustIngest(t, s, retJobEventRec("job-1", 1, jobapi.TypeAccepted)) {
		t.Fatal("job-1: applied = false")
	}
	// The jobs budget is full, but the sessions budget is free.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:00:00-03:00", "start", "card-1", "feat/x", "", "")) {
		t.Fatal("session start with a full jobs budget: applied = false")
	}
	_, err := s.Ingest(sessaoRec("2026-10-10T11:30:00-03:00", "start", "card-2", "feat/y", "", ""))
	retWantRefusal(t, err, "sessions")
	_, err = s.Ingest(retJobEventRec("job-2", 1, jobapi.TypeAccepted))
	retWantRefusal(t, err, "jobs")
	// Known updates still apply at both full budgets.
	if !mustIngest(t, s, retJobEventRec("job-1", 2, jobapi.TypeNote)) {
		t.Fatal("known job event at the full budgets: applied = false")
	}
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:45:00-03:00", "update", "card-3", "feat/x", "", "")) {
		t.Fatal("known session update at the full budgets: applied = false")
	}
}

// TestRetentionKnownJobUpdatesAtFullBudget: at a full jobs budget the
// updates of a known job row apply: a later event, a terminal, the
// cleanup of the known job, a dispatch and an amend of the known job, a
// decision, a duplicate and a replayed event (applied = false, no
// error).
func TestRetentionKnownJobUpdatesAtFullBudget(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2})
	if !mustIngest(t, s, retJobEventRec("job-1", 1, jobapi.TypeAccepted)) {
		t.Fatal("job-1 seq 1: applied = false")
	}
	if !mustIngest(t, s, retJobEventRec("job-2", 1, jobapi.TypeAccepted)) {
		t.Fatal("job-2: applied = false (the budget is now full)")
	}
	if !mustIngest(t, s, retJobEventRec("job-1", 2, jobapi.TypeNote)) {
		t.Fatal("known job event: applied = false")
	}
	if !mustIngest(t, s, retJobEventRec("job-1", 3, jobapi.TypeTerminal)) {
		t.Fatal("known job terminal: applied = false")
	}
	if !mustIngest(t, s, retJobEventRec("job-1", 4, jobapi.TypeCleanup)) {
		t.Fatal("known job cleanup: applied = false")
	}
	for _, tipo := range []string{outbox.TipoDispatch, outbox.TipoAmend} {
		rec := recordBase(func(r *outbox.Record) {
			r.Tipo = tipo
			r.JobID = ptr("job-2")
			r.IdempotencyKey = "node-a:77"
			r.Dados = json.RawMessage(`{}`)
		})
		if !mustIngest(t, s, rec) {
			t.Fatalf("%s of a known job: applied = false", tipo)
		}
	}
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Tipo = outbox.TipoDecision
		r.JobID = ptr("job-2")
		r.IdempotencyKey = "node-a:78"
		r.Dados = json.RawMessage(`{"resumo":"r","motivo":"m","escopo":"global"}`)
	})) {
		t.Fatal("decision of a known job: applied = false")
	}
	// A duplicate seq is refused without an error, whatever its resumo.
	if mustIngest(t, s, retJobEventRec("job-2", 1, jobapi.TypeNote)) {
		t.Error("duplicate event: applied = true, want false")
	}
	if f := jobFact(t, s, "node-a", "job-1"); f.encerrado != 1 {
		t.Errorf("job-1 after its cleanup = %+v, want tombstoned", f)
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2 (no update created a row)", n)
	}
}

// TestRetentionKnownSessionUpdatesAtFullBudget: at a full sessions
// budget the updates of a known session row apply: an update, an end,
// a reopen by a newer start (no new row), and an older replay (applied
// = false, no error).
func TestRetentionKnownSessionUpdatesAtFullBudget(t *testing.T) {
	s := retStore(t, Options{MaxSessions: 2})
	if !mustIngest(t, s, sessaoRec("2026-10-10T10:00:00-03:00", "start", "card-1", "feat/a", "", "")) {
		t.Fatal("session a start: applied = false")
	}
	if !mustIngest(t, s, sessaoRec("2026-10-10T10:05:00-03:00", "start", "card-2", "feat/b", "", "")) {
		t.Fatal("session b start: applied = false (the budget is now full)")
	}
	if !mustIngest(t, s, sessaoRec("2026-10-10T10:30:00-03:00", "update", "card-3", "feat/a", "", "")) {
		t.Fatal("known session update: applied = false")
	}
	if !mustIngest(t, s, sessaoRec("2026-10-10T10:40:00-03:00", "end", "", "feat/b", "", "")) {
		t.Fatal("known session end: applied = false")
	}
	// A newer start reopens the closed session on the same row: it adds
	// no row and is never refused for capacity.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:00:00-03:00", "start", "card-4", "feat/b", "", "")) {
		t.Fatal("reopen by a newer start: applied = false")
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/b"); f.encerrado != 0 || f.card == nil || *f.card != "CARD-4" {
		t.Errorf("reopened session = %+v, want live with the new card", f)
	}
	// An older replay of the first start changes nothing.
	if mustIngest(t, s, sessaoRec("2026-10-10T09:00:00-03:00", "start", "card-0", "feat/a", "", "")) {
		t.Error("older replay: applied = true, want false")
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/a"); f.card == nil || *f.card != "CARD-3" {
		t.Errorf("session a after the replay = %+v, want the stored card kept", f)
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2 (no update created a row)", n)
	}
}

// TestRetentionKnownPollUpdateAtFullBudget: at a full jobs budget a poll
// that lists only the known workspace row updates it; the next poll with
// a new workspace is refused whole (covered by
// TestRetentionPollWholeRefusal).
func TestRetentionKnownPollUpdateAtFullBudget(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2})
	mustApplyPoll(t, s, okPoll("node-p", "",
		PolledWorkspace{ID: "w1", Label: "hm-p-impl-1", AgentStatus: "working"}))
	if !mustIngest(t, s, retJobEventRec("job-1", 1, jobapi.TypeAccepted)) {
		t.Fatal("job-1: applied = false")
	}
	// The budget is full (ws:w1 + job-1): a third new row is refused
	// with the jobs refusal...
	if applied, err := s.Ingest(retJobEventRec("job-2", 1, jobapi.TypeAccepted)); applied {
		t.Fatalf("job-2: applied = true, want refused (the budget is full)")
	} else {
		retWantRefusal(t, err, "jobs")
	}
	// ...but the poll update of the known workspace still applies.
	mustApplyPoll(t, s, okPoll("node-p", "",
		PolledWorkspace{ID: "w1", Label: "hm-p-impl-1", AgentStatus: "idle"}))
	var status sql.NullString
	if err := s.db.QueryRow(`SELECT herdr_status FROM jobs WHERE maquina = 'node-p' AND chave = 'ws:w1'`).Scan(&status); err != nil {
		t.Fatalf("herdr_status: %v", err)
	}
	if !status.Valid || status.String != "idle" {
		t.Errorf("poll update of the known row = %v, want the new status (no new row created)", status)
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2", n)
	}
}

// TestRetentionAtomicRefusalKnownMachine: a refused record from a known
// machine rolls back the whole transaction: the retention sweep
// included, every table byte-identical, and the known machine's contact
// unmoved. The 31-day row is swept by the next successful request.
func TestRetentionAtomicRefusalKnownMachine(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2})
	// The known machine with an older contact.
	if _, err := s.db.Exec(`INSERT INTO nodes(maquina, ultimo_contato, ultimo_contato_unix) VALUES('node-x','2026-10-10T00:00:00-03:00',?)`,
		fixedNow.Unix()-3600); err != nil {
		t.Fatalf("seed the known node: %v", err)
	}
	// Two 1-day fences fill the jobs budget; a 31-day row is past the
	// retention (the database is above the budget).
	retEndedJob(t, s, "node-a", "f1", retSeed1Day)
	retEndedJob(t, s, "node-a", "f2", retSeed1Day)
	retEndedJob(t, s, "node-a", "job-past", retSeed31Day)

	before := retDump(t, s)
	rec := recordBase(func(r *outbox.Record) {
		r.Maquina = "node-x"
		r.JobID = ptr("j-new")
		r.IdempotencyKey = "j-new:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	})
	_, err := s.Ingest(rec)
	retWantRefusal(t, err, "jobs")
	after := retDump(t, s)

	// The whole request rolled back: every table is byte-identical, the
	// 31-day row and its event included.
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the refused record:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
	// The known machine's contact did not move.
	var contact int64
	if err := s.db.QueryRow(`SELECT ultimo_contato_unix FROM nodes WHERE maquina = 'node-x'`).Scan(&contact); err != nil {
		t.Fatalf("contact: %v", err)
	}
	if contact != fixedNow.Unix()-3600 {
		t.Errorf("ultimo_contato_unix = %d, want unchanged (%d)", contact, fixedNow.Unix()-3600)
	}
	// The next successful request sweeps the 31-day row: an event of a
	// tombstoned job is ignored without an error, so the request
	// succeeds and its sweep commits.
	applied, err := s.Ingest(retJobEventRec("f1", 1, jobapi.TypeAccepted))
	if err != nil {
		t.Fatalf("Ingest of the tombstoned job event: %v", err)
	}
	if applied {
		t.Errorf("tombstoned job event: applied = true, want false")
	}
	if f := jobFact(t, s, "node-a", "job-past"); f.exists {
		t.Errorf("the 31-day row after the next successful request = %+v, want swept", f)
	}
}

// rowsEqual compares two row dumps exactly.
func rowsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRetentionAtomicRefusalNewMachine: a refused record from a new
// machine creates no nodes row.
func TestRetentionAtomicRefusalNewMachine(t *testing.T) {
	s := retStore(t, Options{MaxSessions: 2})
	retEndedSessao(t, s, "node-a", "sessao:org001/repo#feat/a", retSeed1Day)
	retEndedSessao(t, s, "node-a", "sessao:org002/repo#feat/b", retSeed1Day)
	rec := sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", "")
	rec.Maquina = "node-new"
	rec.IdempotencyKey = "node-new:s1"
	_, err := s.Ingest(rec)
	retWantRefusal(t, err, "sessions")
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE maquina = 'node-new'`).Scan(&n); err != nil {
		t.Fatalf("count nodes: %v", err)
	}
	if n != 0 {
		t.Errorf("nodes rows for the new machine = %d, want 0 (the refusal left no trace)", n)
	}
}

// TestRetentionOptions: 0 means the default 1000 budget; values outside
// 1..1000 and retentions outside whole days 7..30 make Open fail with an
// ErrInvalid wrap. Each opened store is closed by a cleanup before its
// TempDir is removed.
func TestRetentionOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{name: "defaults zero", opts: Options{}, wantErr: false},
		{name: "max jobs 1", opts: Options{MaxJobs: 1}, wantErr: false},
		{name: "max jobs 1000", opts: Options{MaxJobs: 1000}, wantErr: false},
		{name: "max sessions 1", opts: Options{MaxSessions: 1}, wantErr: false},
		{name: "max sessions 1000", opts: Options{MaxSessions: 1000}, wantErr: false},
		{name: "retention 7 days", opts: Options{EndedRetention: 7 * 24 * time.Hour}, wantErr: false},
		{name: "retention 30 days", opts: Options{EndedRetention: 30 * 24 * time.Hour}, wantErr: false},
		{name: "retention 30 days explicit", opts: Options{EndedRetention: 720 * time.Hour}, wantErr: false},
		{name: "max jobs -1", opts: Options{MaxJobs: -1}, wantErr: true},
		{name: "max jobs 1001", opts: Options{MaxJobs: 1001}, wantErr: true},
		{name: "max sessions -1", opts: Options{MaxSessions: -1}, wantErr: true},
		{name: "max sessions 1001", opts: Options{MaxSessions: 1001}, wantErr: true},
		{name: "retention 6 days", opts: Options{EndedRetention: 6 * 24 * time.Hour}, wantErr: true},
		{name: "retention 31 days", opts: Options{EndedRetention: 31 * 24 * time.Hour}, wantErr: true},
		{name: "retention negative", opts: Options{EndedRetention: -time.Hour}, wantErr: true},
		{name: "retention not whole days", opts: Options{EndedRetention: 8*24*time.Hour + time.Hour}, wantErr: true},
		{name: "retention not whole days small", opts: Options{EndedRetention: 7*24*time.Hour + time.Second}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Open(pathFor(t), tc.opts)
			if s != nil {
				t.Cleanup(func() {
					if err := s.Close(); err != nil {
						t.Errorf("Close: %v", err)
					}
				})
			}
			if tc.wantErr {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("Open = %v, want an ErrInvalid wrap", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Open = %v, want ok", err)
			}
		})
	}
}

// TestRetentionDefaultBudgets: with the zero budgets the defaults of 1000
// jobs and 1000 sessions apply.
func TestRetentionDefaultBudgets(t *testing.T) {
	s := retStore(t, Options{})
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for i := 0; i < MaxJobs; i++ {
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id) VALUES('node-a', ?, 'job', ?)`,
			fmt.Sprintf("d%04d", i), fmt.Sprintf("d%04d", i)); err != nil {
			tx.Rollback()
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	for i := 0; i < MaxSessions; i++ {
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix)
			VALUES('node-a', ?, 'sessao', 1, ?)`, fmt.Sprintf("sessao:org%04d/repo#feat/x", i),
			fixedNow.Unix()-retDay); err != nil {
			tx.Rollback()
			t.Fatalf("seed session %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	_, err = s.Ingest(retJobEventRec("j-new", 1, jobapi.TypeAccepted))
	retWantRefusal(t, err, "jobs")
	_, err = s.Ingest(sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/y", "", ""))
	retWantRefusal(t, err, "sessions")
	if n := tableCount(t, s, "jobs"); n != MaxJobs+MaxSessions {
		t.Errorf("jobs rows = %d, want %d (the refusals created nothing)", n, MaxJobs+MaxSessions)
	}
}

// TestRetentionPollWholeRefusal: at a full jobs budget a poll that lists
// new workspaces is refused whole (no partial apply: the known rows keep
// their stored values), with exactly one alert line that carries no
// workspace or machine label; with an evictable ended row present the
// poll admits the new workspace by evicting it.
func TestRetentionPollWholeRefusal(t *testing.T) {
	s := retStore(t, Options{MaxJobs: 2})
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w1", Label: "hm-first-impl-1", AgentStatus: "working"},
		PolledWorkspace{ID: "w2", Label: "hm-first-impl-2", AgentStatus: "working"}))
	lines := setLogf(t)
	before := retDump(t, s)
	p := okPoll("node-a", "")
	p.Workspaces = []PolledWorkspace{
		{ID: "w1", Label: "hm-first-impl-1", AgentStatus: "idle"},
		{ID: "w2", Label: "hm-first-impl-2", AgentStatus: "working"},
		{ID: "w3", Label: "hm-first-impl-3", AgentStatus: "working"},
		{ID: "w4", Label: "hm-first-impl-4", AgentStatus: "working"},
	}
	err := s.ApplyPoll(p)
	retWantRefusal(t, err, "jobs")
	after := retDump(t, s)
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the refused poll:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
	// Nothing of the poll applied: the known row keeps its stored
	// status and the new workspaces have no rows.
	var status sql.NullString
	if err := s.db.QueryRow(`SELECT herdr_status FROM jobs WHERE maquina = 'node-a' AND chave = 'ws:w1'`).Scan(&status); err != nil {
		t.Fatalf("herdr_status: %v", err)
	}
	if !status.Valid || status.String != "working" {
		t.Errorf("the known row = %v, want the stored value kept (the poll was refused whole)", status)
	}
	if f := jobFact(t, s, "node-a", "ws:w3"); f.exists {
		t.Errorf("ws:w3 = %+v, want no row (the new workspace was refused)", f)
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2 (nothing was admitted)", n)
	}
	// One alert line for the whole refusal, with the new workspaces
	// count and no label.
	if len(*lines) != 1 {
		t.Fatalf("alert lines = %d, want exactly one (%v)", len(*lines), *lines)
	}
	line := (*lines)[0]
	if !strings.Contains(line, "jobs limit reached") || !strings.Contains(line, "skipped 2") {
		t.Errorf("alert line = %q, want the jobs refusal with the new workspaces count", line)
	}
	for _, leak := range []string{"node-a", "hm-first-impl-1", "hm-first-impl-3", "hm-first-impl-4", "w1", "w3", "w4"} {
		if strings.Contains(line, leak) {
			t.Errorf("alert line = %q, must not carry %q", line, leak)
		}
	}

	// The mirror with an evictable ended row: the poll admits the new
	// workspace by evicting it, and writes no alert.
	s2 := retStore(t, Options{MaxJobs: 2})
	mustApplyPoll(t, s2, okPoll("node-a", "",
		PolledWorkspace{ID: "w1", Label: "hm-first-impl-1", AgentStatus: "working"}))
	retEndedJob(t, s2, "node-a", "job-old", retSeed8Day)
	lines2 := setLogf(t)
	mustApplyPoll(t, s2, okPoll("node-a", "",
		PolledWorkspace{ID: "w1", Label: "hm-first-impl-1", AgentStatus: "working"},
		PolledWorkspace{ID: "w3", Label: "hm-first-impl-3", AgentStatus: "working"}))
	if f := jobFact(t, s2, "node-a", "ws:w3"); !f.exists {
		t.Errorf("ws:w3 = %+v, want admitted by the eviction", f)
	}
	if f := jobFact(t, s2, "node-a", "job-old"); f.exists {
		t.Errorf("job-old = %+v, want evicted", f)
	}
	if n := tableCount(t, s2, "jobs"); n != 2 {
		t.Errorf("jobs rows = %d, want 2", n)
	}
	if len(*lines2) != 0 {
		t.Errorf("alert lines = %d, want none (the admission evicted)", len(*lines2))
	}
}

// TestRetentionHTTP: a refused record answers 400 with exactly the one
// JSON line, and a known-session end at the same full budget answers 200.
func TestRetentionHTTP(t *testing.T) {
	s := retStore(t, Options{MaxSessions: 2})
	retEndedSessao(t, s, "node-a", "sessao:example-org/example-repo#feat/a", retSeed1Day)
	retEndedSessao(t, s, "node-a", "sessao:example-org/example-repo#feat/b", retSeed1Day)
	h := NewHandler(s)
	body := mustMarshalRetRecord(t, sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", ""))
	w := doReq(t, h, "POST", "/v1/ingest", string(body), jsonHeaders(nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"status":"invalid","motivo":"sessions limit reached"}`+"\n" {
		t.Errorf("body = %q, want the one-line 400 refusal", got)
	}
	// A known-session end at the same full budget applies.
	body = mustMarshalRetRecord(t, sessaoRec("2026-10-10T12:00:00-03:00", "end", "", "feat/a", "", ""))
	w = doReq(t, h, "POST", "/v1/ingest", string(body), jsonHeaders(nil))
	if w.Code != http.StatusOK {
		t.Fatalf("known-session end code = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"status":"ok","aplicado":true}`+"\n" {
		t.Errorf("body = %q, want 200 ok/applied", got)
	}
}

// TestRetentionConcurrency: N goroutines ingesting distinct new sessions
// into a store with budget B < N end with exactly B rows and N-B "sessions
// limit reached" errors; two Stores opened on the same file behave the
// same.
func TestRetentionConcurrency(t *testing.T) {
	t.Run("one store", func(t *testing.T) {
		s := retStore(t, Options{MaxSessions: 3})
		const n = 8
		results := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rec := sessaoRec("2026-10-10T11:00:00-03:00", "end", "card-9", fmt.Sprintf("feat/c%d", i), "", "")
				_, err := s.Ingest(rec)
				results[i] = err
			}(i)
		}
		wg.Wait()
		refusals := 0
		for i, err := range results {
			if err == nil {
				continue
			}
			refusals++
			retWantRefusal(t, err, "sessions")
			_ = i
		}
		if refusals != n-3 {
			t.Errorf("refusals = %d, want %d", refusals, n-3)
		}
		if count := retCountKind(t, s, kindSessions); count != 3 {
			t.Errorf("session rows = %d, want exactly the budget 3", count)
		}
	})
	t.Run("two stores on the same file", func(t *testing.T) {
		path := pathFor(t)
		s1, err := Open(path, Options{Now: fixedClock(), MaxSessions: 3})
		if err != nil {
			t.Fatalf("Open store 1: %v", err)
		}
		s2, err := Open(path, Options{Now: fixedClock(), MaxSessions: 3})
		if err != nil {
			t.Fatalf("Open store 2: %v", err)
		}
		t.Cleanup(func() { s1.Close(); s2.Close() })
		stores := []*Store{s1, s2}
		const n = 8
		results := make([]error, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rec := sessaoRec("2026-10-10T11:00:00-03:00", "end", "card-9", fmt.Sprintf("feat/d%d", i), "", "")
				_, err := stores[i%2].Ingest(rec)
				results[i] = err
			}(i)
		}
		wg.Wait()
		refusals := 0
		for _, err := range results {
			if err != nil {
				refusals++
				retWantRefusal(t, err, "sessions")
			}
		}
		if refusals != n-3 {
			t.Errorf("refusals = %d, want %d", refusals, n-3)
		}
		if count := retCountKind(t, s1, kindSessions); count != 3 {
			t.Errorf("session rows = %d, want exactly the budget 3", count)
		}
	})
}

// retCountKind counts the retained rows of one kind.
func retCountKind(t *testing.T, s *Store, kind string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE ` + kindPred(kind)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", kind, err)
	}
	return n
}

// TestRetentionAlertHygiene: one line per refusal, carrying the kind and
// the counts (active, protected ended, evictable ended, which add up to
// the retained total) and nothing of the record: no machine label,
// project, branch, card, job id or Authorization value. The jobs refusal
// runs on a database above its budget with fewer evictable ended rows
// than the admission needs, so the evictable count is not the whole
// ended remainder.
func TestRetentionAlertHygiene(t *testing.T) {
	orig := logf
	var mu sync.Mutex
	var lines []string
	logf = func(format string, args ...any) {
		mu.Lock()
		lines = append(lines, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	t.Cleanup(func() { logf = orig })

	s := retStore(t, Options{MaxJobs: 2, MaxSessions: 2})
	// The sessions kind sits exactly at its budget, all protected fences.
	retEndedSessao(t, s, "node-a", "sessao:example-org/example-repo#feat/a", retSeed1Day)
	retEndedSessao(t, s, "node-a", "sessao:example-org/example-repo#feat/b", retSeed1Day)
	// The jobs kind is above its budget: one active row, one protected
	// fence and one evictable ended row.
	retActiveJob(t, s, "node-b", "job-1")
	retEndedJob(t, s, "node-b", "job-fence", retSeed1Day)
	retEndedJob(t, s, "node-b", "job-old", retSeed8Day)

	rec := sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", "")
	_, err := s.Ingest(rec)
	retWantRefusal(t, err, "sessions")
	rec2 := retJobEventRec("job-3", 1, jobapi.TypeAccepted)
	rec2.Maquina = "node-c"
	_, err = s.Ingest(rec2)
	retWantRefusal(t, err, "jobs")

	if len(lines) != 2 {
		t.Fatalf("alert lines = %d, want one per refusal (%v)", len(lines), lines)
	}
	wants := []struct {
		kind   string
		counts []string
	}{
		{"sessions", []string{"retained 2", "active 0", "protected ended 2", "evictable ended 0", "limit 2"}},
		{"jobs", []string{"retained 3", "active 1", "protected ended 1", "evictable ended 1", "limit 2"}},
	}
	for i, w := range wants {
		line := lines[i]
		if !strings.Contains(line, w.kind+" limit reached") {
			t.Errorf("line %d = %q, want the %s refusal", i, line, w.kind)
		}
		for _, want := range w.counts {
			if !strings.Contains(line, want) {
				t.Errorf("line %d = %q, want the count %q", i, line, want)
			}
		}
		for _, leak := range []string{"node-a", "node-b", "node-c", "example-org/example-repo", "feat/x", "card-9", "CARD-9", "job-3", "job-1", "job-fence", "job-old"} {
			if strings.Contains(line, leak) {
				t.Errorf("line %d = %q, must not carry %q", i, line, leak)
			}
		}
	}
}

// TestSessionKeysNeverHoldEvents: a sessao row cannot hold events rows
// through any admission or update path, which the inventory bound relies
// on to size the sessions rows without event rings. A job_event record's
// key is its job_id, which the contract id rule (no ':' or '#') cannot
// address with a sessao key; a session record that names a job validates
// that job with the same rule and targets the job row; a dispatch, amend
// or decision record names its job through the same rule; a poll
// workspace label reduces to a job id or a ws:<id> key, never a sessao
// key; and only a job event inserts an events row. The test tries every
// path with hostile inputs and asserts every session key keeps zero
// events rows.
func TestSessionKeysNeverHoldEvents(t *testing.T) {
	s := openStore(t)
	// A real session row, so a stray event could land on its key.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:00:00-03:00", "start", "card-9", "feat/x", "", "")) {
		t.Fatal("session start: applied = false")
	}
	// (1) A job_event whose job_id is shaped like a session key is
	// refused by the id rule: it cannot address a sessao row.
	if _, err := s.Ingest(recordBase(func(r *outbox.Record) {
		r.JobID = ptr("sessao:x#y")
		r.IdempotencyKey = "sessao:x#y:1"
	})); !errors.Is(err, ErrInvalid) {
		t.Fatalf("job_event with job_id sessao:x#y: err = %v, want the id-rule refusal", err)
	}
	// (2) A session record that names a job shaped like a session key is
	// refused the same way...
	if _, err := s.Ingest(sessaoRec("2026-10-10T11:00:30-03:00", "update", "", "feat/x", "", "sessao:org/repo#feat")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("session record with job sessao:org/repo#feat: err = %v, want the id-rule refusal", err)
	}
	// ...while a session record that names a real job lands on the job
	// row (the jobs kind), never on a session row.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:05:00-03:00", "update", "", "feat/y", "", "job-42")) {
		t.Fatal("session update of job-42: applied = false")
	}
	if f := jobFact(t, s, "node-a", "job-42"); !f.exists {
		t.Errorf("job-42 = %+v, want the named job row touched", f)
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/y"); f.exists {
		t.Errorf("session row for the named job = %+v, want none", f)
	}
	// (3) A dispatch, amend or decision with a session-shaped job id is
	// refused by the same rule, and none of them writes events.
	for _, tipo := range []string{outbox.TipoDispatch, outbox.TipoAmend, outbox.TipoDecision} {
		rec := recordBase(func(r *outbox.Record) {
			r.Tipo = tipo
			r.JobID = ptr("sessao:x#y")
			r.IdempotencyKey = "node-a:sess-shape"
		})
		if _, err := s.Ingest(rec); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s with job_id sessao:x#y: err = %v, want the id-rule refusal", tipo, err)
		}
	}
	// (4) A poll workspace that looks like a session key lands on a
	// ws:<id> row, never on a sessao row, and the poll writes no events.
	mustApplyPoll(t, s, okPoll("node-b", "",
		PolledWorkspace{ID: "sessao:org/repo#feat", Label: "hm-impl-1", AgentStatus: "working"}))
	if f := jobFact(t, s, "node-b", "sessao:org/repo#feat"); f.exists {
		t.Errorf("session-shaped row from the poll = %+v, want a ws: row only", f)
	}
	if f := jobFact(t, s, "node-b", "ws:sessao:org/repo#feat"); !f.exists {
		t.Errorf("ws:sessao:org/repo#feat = %+v, want the workspace row", f)
	}
	// Every session key keeps zero events rows.
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE chave LIKE 'sessao:%'`).Scan(&n); err != nil {
		t.Fatalf("count session events: %v", err)
	}
	if n != 0 {
		t.Fatalf("events rows for session keys = %d, want 0", n)
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/x"); !f.exists {
		t.Errorf("the session row = %+v, want untouched by the hostile paths", f)
	}
}
