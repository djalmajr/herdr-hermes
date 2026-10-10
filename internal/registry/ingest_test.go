package registry

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// recordBase is a valid job_event record; mut tweaks one field at a time.
func recordBase(mut func(*outbox.Record)) outbox.Record {
	r := outbox.Record{
		Schema:         1,
		Seq:            1,
		TS:             "2026-10-10T12:00:00-03:00",
		Tipo:           outbox.TipoJobEvent,
		Maquina:        "node-a",
		JobID:          ptr("job-1"),
		IdempotencyKey: "job-1:1",
		Dados:          eventData(1, jobapi.TypeAccepted, "started", nil),
	}
	if mut != nil {
		mut(&r)
	}
	return r
}

// eventData builds one job event JSON with the fixed record-time ts.
func eventData(seq int64, tipo, resumo string, refs map[string]any) json.RawMessage {
	e := map[string]any{
		"seq":    seq,
		"ts":     "2026-10-10T11:30:00-03:00",
		"tipo":   tipo,
		"resumo": resumo,
	}
	if refs != nil {
		e["refs"] = refs
	}
	b, _ := json.Marshal(e)
	return json.RawMessage(b)
}

// mustIngest ingests without an error and returns applied.
func mustIngest(t *testing.T, s *Store, rec outbox.Record) bool {
	t.Helper()
	applied, err := s.Ingest(rec)
	if err != nil {
		t.Fatalf("Ingest(%s): %v", rec.Tipo, err)
	}
	return applied
}

func tableCount(t *testing.T, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// jobFact is the readable state of one job row.
type jobFactRow struct {
	exists     bool
	encerrado  int
	terminal   int
	obsoleto   int
	fase       *string
	projeto    *string
	branch     *string
	pr         *string
	card       *string
	workspace  *string
	agente     *string
	fonte      *string
	trabalho   *string
	ref        *string
	esperando  int
	motivo     *string
	saida      *int
	ultimoSeq  int64
	ultima     *string
	ultimaUnix *int64
}

func jobFact(t *testing.T, s *Store, maquina, chave string) jobFactRow {
	t.Helper()
	var f jobFactRow
	var encerrado, terminal, obsoleto, esperando int
	var ultimoSeq int64
	var ultimaUnix sql.NullInt64
	var fase, projeto, branch, pr, card, workspace, agente, fonte, trabalho, ref, motivo, saida, ultima interface{}
	err := s.db.QueryRow(`SELECT encerrado, terminal, obsoleto, fase, projeto, branch, pr, card, workspace, agente, agente_fonte, trabalho, ref, esperando_humano, espera_motivo, saida, ultimo_seq, ultima_atividade, ultima_atividade_unix
		FROM jobs WHERE maquina = ? AND chave = ?`, maquina, chave).
		Scan(&encerrado, &terminal, &obsoleto, &fase, &projeto, &branch, &pr, &card, &workspace, &agente, &fonte, &trabalho, &ref, &esperando, &motivo, &saida, &ultimoSeq, &ultima, &ultimaUnix)
	if err != nil {
		return jobFactRow{} // no such row
	}
	f = jobFactRow{
		exists: true, encerrado: encerrado, terminal: terminal, obsoleto: obsoleto,
		fase: strOrNull(fase), projeto: strOrNull(projeto), branch: strOrNull(branch), pr: strOrNull(pr),
		card: strOrNull(card), workspace: strOrNull(workspace), agente: strOrNull(agente),
		fonte: strOrNull(fonte), trabalho: strOrNull(trabalho), ref: strOrNull(ref),
		esperando: esperando, motivo: strOrNull(motivo), ultimoSeq: ultimoSeq,
		ultima: strOrNull(ultima),
	}
	if v, ok := saida.(int64); ok {
		iv := int(v)
		f.saida = &iv
	}
	if ultimaUnix.Valid {
		v := ultimaUnix.Int64
		f.ultimaUnix = &v
	}
	return f
}

// strOrNull turns a scanned SQL value (string or nil) into a *string.
func strOrNull(v interface{}) *string {
	if v == nil {
		return nil
	}
	s, _ := v.(string)
	return &s
}

func eventSeqs(t *testing.T, s *Store, maquina, chave string) []int64 {
	t.Helper()
	rows, err := s.db.Query(`SELECT seq FROM events WHERE maquina = ? AND chave = ? ORDER BY seq`, maquina, chave)
	if err != nil {
		t.Fatalf("event seqs: %v", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("event seqs: %v", err)
		}
		out = append(out, seq)
	}
	return out
}

// TestIngestInvalid: every invalid record shape is refused with
// ErrInvalid and nothing is written.
// Mutation captured: a validation skipped here (or a write before the
// validation) fails one of the rows or the nothing-written assertion.
func TestIngestInvalid(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*outbox.Record)
	}{
		{name: "schema 2", mut: func(r *outbox.Record) { r.Schema = 2 }},
		{name: "unknown tipo", mut: func(r *outbox.Record) { r.Tipo = "amendment" }},
		{name: "bad maquina", mut: func(r *outbox.Record) { r.Maquina = "bad maquina" }},
		{name: "seq 0", mut: func(r *outbox.Record) { r.Seq = 0 }},
		{name: "ts without offset", mut: func(r *outbox.Record) { r.TS = "2026-10-10T12:00:00" }},
		{name: "empty key", mut: func(r *outbox.Record) { r.IdempotencyKey = "" }},
		{name: "key too long", mut: func(r *outbox.Record) { r.IdempotencyKey = strings.Repeat("k", 201) }},
		{name: "bad projeto", mut: func(r *outbox.Record) { r.Projeto = "acme" }},
		{name: "bad job id", mut: func(r *outbox.Record) { r.JobID = ptr(strings.Repeat("j", 65)) }},
		{name: "dados not an object", mut: func(r *outbox.Record) { r.Dados = json.RawMessage(`[1,2]`) }},
		{name: "dados null", mut: func(r *outbox.Record) { r.Dados = json.RawMessage(`null`) }},
		{name: "job_event key mismatch", mut: func(r *outbox.Record) { r.IdempotencyKey = "job-1:2" }},
		{name: "job_event undecodable dados", mut: func(r *outbox.Record) { r.Dados = json.RawMessage(`"nope"`) }},
		{name: "job_event event seq 0", mut: func(r *outbox.Record) {
			r.Dados = eventData(0, jobapi.TypeAccepted, "x", nil)
			r.IdempotencyKey = "job-1:0"
		}},
		{name: "dispatch without job_id", mut: func(r *outbox.Record) {
			r.Tipo = outbox.TipoDispatch
			r.JobID = nil
			r.IdempotencyKey = "node-a:1"
			r.Dados = json.RawMessage(`{}`)
		}},
		{name: "amend without job_id", mut: func(r *outbox.Record) {
			r.Tipo = outbox.TipoAmend
			r.JobID = nil
			r.IdempotencyKey = "node-a:1"
			r.Dados = json.RawMessage(`{}`)
		}},
		{name: "session bad acao", mut: func(r *outbox.Record) {
			r.Tipo = outbox.TipoSession
			r.IdempotencyKey = "node-a:1"
			r.Dados = json.RawMessage(`{"acao":"restart"}`)
		}},
		{name: "session invalid dados projeto", mut: func(r *outbox.Record) {
			r.Tipo = outbox.TipoSession
			r.IdempotencyKey = "node-a:1"
			r.Dados = json.RawMessage(`{"acao":"start","projeto":"oops"}`)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t)
			_, err := s.Ingest(recordBase(tc.mut))
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Ingest = %v, want ErrInvalid", err)
			}
			if n := tableCount(t, s, "nodes") + tableCount(t, s, "jobs") + tableCount(t, s, "events"); n != 0 {
				t.Fatalf("rows written = %d, want nothing", n)
			}
		})
	}
}

// TestEventMappingCoversContract: every contract event type is ingested
// and yields the documented state. The table is the mapping; a type
// missing from it fails the test, so a new contract type must fail here
// until a decision is made for it.
//
//	type                      esperando  motivo     terminal  tombstone
//	accepted..note (15)          0         -            0        no
//	question                     1      question        0        no
//	blocked                      1      blocked         0        no
//	unblocked                    0         -            0        no
//	terminal                     1*       -*           1        no  (*exit 7/11/14)
//	cleanup                      0         -            0       yes
//
// Mutation captured: a mapping row changed (or the seq > ultimo_seq gate
// widened) fails the matching type.
func TestEventMappingCoversContract(t *testing.T) {
	type want struct {
		esperando int
		motivo    *string
		terminal  bool
		encerrado bool
	}
	m := make(map[string]want, len(jobapi.EventTypes))
	for _, tipo := range []string{
		jobapi.TypeAccepted, jobapi.TypePreparing, jobapi.TypeWorkerSpawned,
		jobapi.TypeWorkerDone, jobapi.TypeCommit, jobapi.TypePush,
		jobapi.TypePrOpened, jobapi.TypeCheckpoint, jobapi.TypeReviewVerdict,
		jobapi.TypeAmendReceived, jobapi.TypeDecision, jobapi.TypeDecisionAcked,
		jobapi.TypeTimeoutWarning, jobapi.TypeFailure, jobapi.TypeNote,
	} {
		m[tipo] = want{}
	}
	q := "question"
	b := "blocked"
	m[jobapi.TypeQuestion] = want{esperando: 1, motivo: &q}
	m[jobapi.TypeBlocked] = want{esperando: 1, motivo: &b}
	m[jobapi.TypeUnblocked] = want{esperando: 0}
	m[jobapi.TypeTerminal] = want{esperando: 1, motivo: &b, terminal: true}
	m[jobapi.TypeCleanup] = want{encerrado: true}

	for _, tipo := range jobapi.EventTypes {
		t.Run(tipo, func(t *testing.T) {
			w, ok := m[tipo]
			if !ok {
				t.Fatalf("no mapping for contract type %q: extend the table with a decision", tipo)
			}
			s := openStore(t)
			refs := map[string]any{}
			if tipo == jobapi.TypeTerminal {
				refs["exit"] = 7
			}
			rec := recordBase(func(r *outbox.Record) {
				r.Dados = eventData(1, tipo, "one", refs)
			})
			if !mustIngest(t, s, rec) {
				t.Fatalf("applied = false, want the %s event applied", tipo)
			}
			f := jobFact(t, s, "node-a", "job-1")
			if !f.exists {
				t.Fatalf("job row missing")
			}
			if f.fase == nil || *f.fase != tipo {
				t.Errorf("fase = %v, want %q", f.fase, tipo)
			}
			if f.esperando != w.esperando {
				t.Errorf("esperando_humano = %d, want %d", f.esperando, w.esperando)
			}
			if w.motivo != nil {
				if f.motivo == nil || *f.motivo != *w.motivo {
					t.Errorf("espera_motivo = %v, want %q", f.motivo, *w.motivo)
				}
			} else if f.motivo != nil {
				t.Errorf("espera_motivo = %q, want null", *f.motivo)
			}
			if (f.terminal == 1) != w.terminal {
				t.Errorf("terminal = %d, want %v", f.terminal, w.terminal)
			}
			if (f.encerrado == 1) != w.encerrado {
				t.Errorf("encerrado = %d, want %v", f.encerrado, w.encerrado)
			}
		})
	}
}

// TestIngestJobEventQuestionUnblocked: a question starts the wait and an
// unblocked clears it.
// Mutation captured: the unblocked mapping missing (or applied to the
// wrong type) leaves the wait set.
func TestIngestJobEventQuestionUnblocked(t *testing.T) {
	s := openStore(t)
	rec := recordBase(func(r *outbox.Record) { r.Dados = eventData(1, jobapi.TypeQuestion, "which one?", nil) })
	if !mustIngest(t, s, rec) {
		t.Fatal("question: applied = false")
	}
	f := jobFact(t, s, "node-a", "job-1")
	if f.esperando != 1 || f.motivo == nil || *f.motivo != EsperaQuestion {
		t.Fatalf("after question = esperando %d motivo %v, want 1 question", f.esperando, f.motivo)
	}
	rec = recordBase(func(r *outbox.Record) {
		r.Seq = 2
		r.IdempotencyKey = "job-1:2"
		r.Dados = eventData(2, jobapi.TypeUnblocked, "done", nil)
	})
	if !mustIngest(t, s, rec) {
		t.Fatal("unblocked: applied = false")
	}
	f = jobFact(t, s, "node-a", "job-1")
	if f.esperando != 0 || f.motivo != nil {
		t.Errorf("after unblocked = esperando %d motivo %v, want 0 null", f.esperando, f.motivo)
	}
	if f.fase == nil || *f.fase != jobapi.TypeUnblocked {
		t.Errorf("fase = %v, want unblocked", f.fase)
	}
}

// TestIngestJobEventTerminal: exit 7/11/14 wait blocked, other exits do
// not; saida is the integer exit (JSON number without fraction or decimal
// string), null otherwise.
// Mutation captured: the 7/11/14 set or the integer-only saida rule
// changed fails a row.
func TestIngestJobEventTerminal(t *testing.T) {
	intPtr := func(v int) *int { return &v }
	cases := []struct {
		name     string
		exit     any
		wantS    *int
		wantWait int
	}{
		{name: "exit7", exit: 7, wantS: intPtr(7), wantWait: 1},
		{name: "exit11", exit: 11, wantS: intPtr(11), wantWait: 1},
		{name: "exit14", exit: 14, wantS: intPtr(14), wantWait: 1},
		{name: "exit0", exit: 0, wantS: intPtr(0), wantWait: 0},
		{name: "exit19", exit: 19, wantS: intPtr(19), wantWait: 0},
		{name: "exitStr7", exit: "7", wantS: intPtr(7), wantWait: 1},
		{name: "exitStr75", exit: "7.5", wantWait: 0},
		{name: "exitFloat75", exit: 7.5, wantWait: 0},
		{name: "exitBanana", exit: "banana", wantWait: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t)
			rec := recordBase(func(r *outbox.Record) {
				r.Dados = eventData(1, jobapi.TypeTerminal, "exited", map[string]any{"exit": tc.exit})
			})
			if !mustIngest(t, s, rec) {
				t.Fatal("terminal: applied = false")
			}
			f := jobFact(t, s, "node-a", "job-1")
			if f.terminal != 1 {
				t.Fatalf("terminal = %d, want 1", f.terminal)
			}
			if f.esperando != tc.wantWait {
				t.Errorf("esperando_humano = %d, want %d (exit %v)", f.esperando, tc.wantWait, tc.exit)
			}
			if tc.wantS != nil {
				if f.saida == nil || *f.saida != *tc.wantS {
					t.Errorf("saida = %v, want %d", f.saida, *tc.wantS)
				}
			} else if f.saida != nil {
				t.Errorf("saida = %d, want null (exit %v)", *f.saida, tc.exit)
			}
		})
	}
}

// TestIngestJobEventPR: a parseable refs.pr fills a null pr once; a
// stored pr is never overwritten.
// Mutation captured: a pr set without the null guard overwrites the first
// pr.
func TestIngestJobEventPR(t *testing.T) {
	s := openStore(t)
	rec := recordBase(func(r *outbox.Record) {
		r.Dados = eventData(1, jobapi.TypePrOpened, "opened", map[string]any{
			"pr": "https://example.org/example-org/example-repo/pull/67/files",
		})
	})
	if !mustIngest(t, s, rec) {
		t.Fatal("pr_opened: applied = false")
	}
	f := jobFact(t, s, "node-a", "job-1")
	if f.pr == nil || *f.pr != "67" {
		t.Fatalf("pr = %v, want 67", f.pr)
	}
	rec = recordBase(func(r *outbox.Record) {
		r.Seq = 2
		r.IdempotencyKey = "job-1:2"
		r.Dados = eventData(2, jobapi.TypePush, "pushed", map[string]any{"pr": "88"})
	})
	if !mustIngest(t, s, rec) {
		t.Fatal("push: applied = false")
	}
	f = jobFact(t, s, "node-a", "job-1")
	if f.pr == nil || *f.pr != "67" {
		t.Errorf("pr = %v, want 67 kept", f.pr)
	}
}

// TestIngestJobEventRefsReduced: only the allowlisted refs keys are
// stored; report and brief (local paths) are dropped.
// Mutation captured: a refs reduction that keeps every key leaves report
// behind.
func TestIngestJobEventRefsReduced(t *testing.T) {
	s := openStore(t)
	rec := recordBase(func(r *outbox.Record) {
		r.Dados = eventData(1, jobapi.TypeNote, "note", map[string]any{
			"pane":   "p9",
			"pr":     "67",
			"report": "/local/path/report.md",
			"brief":  "/local/path/brief.md",
			"agente": "a-1",
		})
	})
	if !mustIngest(t, s, rec) {
		t.Fatal("applied = false")
	}
	var refsText string
	if err := s.db.QueryRow(`SELECT refs FROM events WHERE maquina = 'node-a' AND chave = 'job-1' AND seq = 1`).Scan(&refsText); err != nil {
		t.Fatalf("stored refs: %v", err)
	}
	var refs map[string]any
	if err := json.Unmarshal([]byte(refsText), &refs); err != nil {
		t.Fatalf("stored refs: %v", err)
	}
	for _, k := range []string{"report", "brief"} {
		if _, ok := refs[k]; ok {
			t.Errorf("stored refs keep %q, want it dropped", k)
		}
	}
	for _, k := range []string{"pane", "pr", "agente"} {
		if _, ok := refs[k]; !ok {
			t.Errorf("stored refs drop %q, want it kept", k)
		}
	}
	// The inventory event carries the same reduced refs.
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 1 || len(inv.Jobs[0].Eventos) != 1 {
		t.Fatalf("inventory events = %d, want 1", len(inv.Jobs[0].Eventos))
	}
	if _, ok := inv.Jobs[0].Eventos[0].Refs["report"]; ok {
		t.Error("inventory event keeps report, want it dropped")
	}
}

// TestIngestJobEventTombstone: a cleanup tombstones the job; later and
// replayed events of the job are ignored with applied = false.
// Mutation captured: a missing tombstone (or a later-event check that
// skips the encerrado row) lets the job back into the inventory.
func TestIngestJobEventTombstone(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.Dados = eventData(1, jobapi.TypeCommit, "c", nil) }))
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Seq = 2
		r.IdempotencyKey = "job-1:2"
		r.Dados = eventData(2, jobapi.TypeCleanup, "clean", nil)
	}))
	f := jobFact(t, s, "node-a", "job-1")
	if f.encerrado != 1 {
		t.Fatalf("encerrado = %d, want 1", f.encerrado)
	}
	if n := eventCount(t, s, "node-a", "job-1"); n != 0 {
		t.Errorf("events left = %d, want 0: the tombstone deletes them", n)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 0 {
		t.Errorf("inventory jobs = %d, want 0: the job left the inventory", len(inv.Jobs))
	}
	// A later event is ignored.
	late := mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Seq = 3
		r.IdempotencyKey = "job-1:3"
		r.Dados = eventData(3, jobapi.TypeCommit, "late", nil)
	}))
	if late {
		t.Error("later event of a tombstoned job: applied = true, want false")
	}
	// A replayed event is ignored.
	replay := mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.IdempotencyKey = "job-1:1"
	}))
	if replay {
		t.Error("replayed event of a tombstoned job: applied = true, want false")
	}
}

func eventCount(t *testing.T, s *Store, maquina, chave string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE maquina = ? AND chave = ?`, maquina, chave).Scan(&n); err != nil {
		t.Fatalf("event count: %v", err)
	}
	return n
}

// TestIngestJobEventDuplicate: the same event twice applies once.
// Mutation captured: a dropped duplicate check applies it twice.
func TestIngestJobEventDuplicate(t *testing.T) {
	s := openStore(t)
	rec := recordBase(func(r *outbox.Record) { r.Dados = eventData(1, jobapi.TypeQuestion, "q", nil) })
	if !mustIngest(t, s, rec) {
		t.Fatal("first: applied = false")
	}
	if mustIngest(t, s, rec) {
		t.Fatal("replayed record: applied = true, want false")
	}
	if n := eventCount(t, s, "node-a", "job-1"); n != 1 {
		t.Errorf("events = %d, want 1", n)
	}
	f := jobFact(t, s, "node-a", "job-1")
	if f.fase == nil || *f.fase != jobapi.TypeQuestion {
		t.Errorf("fase = %v, want question (unchanged by the replay)", f.fase)
	}
}

// TestIngestJobEventOutOfOrder: seq 3 then seq 2 stores both events and
// keeps the fase of the higher seq; a replay below a full ring is
// refused.
// Mutation captured: a > swapped with >= on the seq gate moves the fase
// to seq 2; a dropped old-replay refusal stores the stale event.
func TestIngestJobEventOutOfOrder(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.IdempotencyKey = "job-1:3"
		r.Dados = eventData(3, jobapi.TypeCheckpoint, "cp", nil)
	}))
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.IdempotencyKey = "job-1:2"
		r.Dados = eventData(2, jobapi.TypeCommit, "commit", nil)
	})) {
		t.Fatal("seq 2 after seq 3: applied = false, want stored")
	}
	f := jobFact(t, s, "node-a", "job-1")
	if f.ultimoSeq != 3 {
		t.Errorf("ultimo_seq = %d, want 3", f.ultimoSeq)
	}
	if f.fase == nil || *f.fase != jobapi.TypeCheckpoint {
		t.Errorf("fase = %v, want the fase of seq 3", f.fase)
	}
	if got := eventSeqs(t, s, "node-a", "job-1"); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Errorf("event seqs = %v, want both stored", got)
	}
}

// TestIngestJobEventRing: only the newest EventsPerJob seqs are kept, and
// a stale replay below a full ring changes nothing.
// Mutation captured: a ring trim that evicts the newest (or never) keeps
// the wrong seqs.
func TestIngestJobEventRing(t *testing.T) {
	s, err := Open(pathFor(t), Options{Now: fixedClock(), EventsPerJob: 3})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	for seq := int64(1); seq <= 5; seq++ {
		mustIngest(t, s, recordBase(func(r *outbox.Record) {
			r.IdempotencyKey = "job-1:" + strconv.FormatInt(seq, 10)
			r.Dados = eventData(seq, jobapi.TypeCommit, "c", nil)
		}))
	}
	if got := eventSeqs(t, s, "node-a", "job-1"); len(got) != 3 || got[0] != 3 || got[1] != 4 || got[2] != 5 {
		t.Errorf("ring = %v, want the three newest seqs", got)
	}
	// A stale replay below the full ring is refused.
	if mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.IdempotencyKey = "job-1:1"
		r.Dados = eventData(1, jobapi.TypeCommit, "old", nil)
	})) {
		t.Error("stale replay below a full ring: applied = true, want false")
	}
	if got := eventSeqs(t, s, "node-a", "job-1"); len(got) != 3 || got[0] != 3 {
		t.Errorf("ring after the stale replay = %v, want unchanged", got)
	}
	// The inventory shows the ring in ascending seq.
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	var seqs []int64
	for _, e := range inv.Jobs[0].Eventos {
		seqs = append(seqs, e.Seq)
	}
	if len(seqs) != 3 || seqs[0] != 3 || seqs[2] != 5 {
		t.Errorf("inventory seqs = %v, want [3 4 5]", seqs)
	}
}

// TestIngestJobEventResumoCut: the resumo is cut at MaxResumo code points,
// by whole runes.
// Mutation captured: a byte-based cut (or a missing cut) leaves a
// truncated rune or the full text.
func TestIngestJobEventResumoCut(t *testing.T) {
	s := openStore(t)
	rec := recordBase(func(r *outbox.Record) {
		r.Dados = eventData(1, jobapi.TypeNote, strings.Repeat("é", 300), nil)
	})
	if !mustIngest(t, s, rec) {
		t.Fatal("applied = false")
	}
	var resumo string
	if err := s.db.QueryRow(`SELECT resumo FROM events WHERE maquina = 'node-a' AND chave = 'job-1'`).Scan(&resumo); err != nil {
		t.Fatalf("stored resumo: %v", err)
	}
	if len([]rune(resumo)) != MaxResumo {
		t.Errorf("stored resumo is %d code points, want %d", len([]rune(resumo)), MaxResumo)
	}
}

// TestIngestJobEventPerMachine: the same job id on two machines is two
// rows, keyed by (maquina, chave).
// Mutation captured: ignoring maquina in the job key merges the rows.
func TestIngestJobEventPerMachine(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(nil))
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.Maquina = "node-b" }))
	for _, m := range []string{"node-a", "node-b"} {
		f := jobFact(t, s, m, "job-1")
		if !f.exists {
			t.Fatalf("job row of %s missing", m)
		}
	}
	if n := tableCount(t, s, "jobs"); n != 2 {
		t.Errorf("job rows = %d, want one per machine", n)
	}
}

// TestIngestDispatch: a dispatch upserts the job without interpreting
// dados; a tombstoned job is left unchanged.
// Mutation captured: a dispatch that clears obsoleto (or interprets
// dados) fails the assertions.
func TestIngestDispatch(t *testing.T) {
	s := openStore(t)
	rec := outbox.Record{
		Schema: 1, Seq: 2, TS: "2026-10-10T12:05:00-03:00",
		Tipo: outbox.TipoDispatch, Maquina: "node-a", Projeto: "example-org/example-repo",
		JobID: ptr("job-9"), IdempotencyKey: "node-a:2",
		Dados: json.RawMessage(`{"whatever":"shape"}`),
	}
	if !mustIngest(t, s, rec) {
		t.Fatal("dispatch: applied = false")
	}
	f := jobFact(t, s, "node-a", "job-9")
	if !f.exists || f.fase != nil {
		t.Fatalf("job row = %+v, want created with no fase", f)
	}
	if f.projeto == nil || *f.projeto != "example-org/example-repo" {
		t.Errorf("projeto = %v, want the record projeto", f.projeto)
	}
	if f.ultima == nil || *f.ultima != "2026-10-10T12:05:00-03:00" {
		t.Errorf("ultima_atividade = %v, want the record ts", f.ultima)
	}
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("job-9")
		r.IdempotencyKey = "job-9:1"
		r.Dados = eventData(1, jobapi.TypeCleanup, "x", nil)
	}))
	// The cleanup tombstones the job and sets its fase to the event tipo.
	f = jobFact(t, s, "node-a", "job-9")
	if f.encerrado != 1 {
		t.Fatalf("after the cleanup: encerrado = %d, want 1", f.encerrado)
	}
	if f.fase == nil || *f.fase != jobapi.TypeCleanup {
		t.Errorf("fase = %v, want the cleanup tipo", f.fase)
	}
	// A dispatch on a tombstoned job is applied but leaves the row
	// unchanged.
	if !mustIngest(t, s, rec) {
		t.Fatal("dispatch on a tombstone: applied = false, want true")
	}
	f = jobFact(t, s, "node-a", "job-9")
	if f.encerrado != 1 || f.fase == nil || *f.fase != jobapi.TypeCleanup {
		t.Errorf("tombstoned job = %+v, want unchanged", f)
	}
}

// TestIngestSessionWithJob: a session named after a job carries its
// card, branch and pr to the job row.
// Mutation captured: a session that skips the field copy leaves the job
// card null.
func TestIngestSessionWithJob(t *testing.T) {
	s := openStore(t)
	rec := outbox.Record{
		Schema: 1, Seq: 3, TS: "2026-10-10T12:10:00-03:00",
		Tipo: outbox.TipoSession, Maquina: "node-a",
		JobID: ptr("job-5"), IdempotencyKey: "node-a:3",
		Dados: json.RawMessage(`{"acao":"start","job":"job-5","card":"ex-9","branch":"feat/x","pr":"7"}`),
	}
	if !mustIngest(t, s, rec) {
		t.Fatal("session: applied = false")
	}
	f := jobFact(t, s, "node-a", "job-5")
	if !f.exists {
		t.Fatal("the job row was not created")
	}
	if f.card == nil || *f.card != "EX-9" {
		t.Errorf("card = %v, want EX-9 (upper-cased)", f.card)
	}
	if f.branch == nil || *f.branch != "feat/x" {
		t.Errorf("branch = %v, want feat/x", f.branch)
	}
	if f.pr == nil || *f.pr != "7" {
		t.Errorf("pr = %v, want 7", f.pr)
	}
}

// TestIngestSessionWithoutJob: a session without a job keys its own
// sessao row; end tombstones it; a start older than the stored
// sessao_ts_unix arriving after the end is ignored.
// Mutation captured: a missing ordering check resurrects the tombstoned
// session row.
func TestIngestSessionWithoutJob(t *testing.T) {
	s := openStore(t)
	mk := func(acao string, ts string, seq int64) outbox.Record {
		return outbox.Record{
			Schema: 1, Seq: seq, TS: ts,
			Tipo: outbox.TipoSession, Maquina: "node-a",
			IdempotencyKey: "node-a:" + strconv.FormatInt(seq, 10),
			Dados:          json.RawMessage(`{"acao":"` + acao + `","projeto":"example-org/example-repo","branch":"feat/x","card":"ex-9"}`),
		}
	}
	if !mustIngest(t, s, mk("start", "2026-10-10T11:00:00-03:00", 4)) {
		t.Fatal("start: applied = false")
	}
	key := "sessao:example-org/example-repo#feat/x"
	f := jobFact(t, s, "node-a", key)
	if !f.exists || f.card == nil || *f.card != "EX-9" {
		t.Fatalf("session row = %+v, want the sessao key with the card", f)
	}
	mustIngest(t, s, mk("end", "2026-10-10T11:05:00-03:00", 5))
	f = jobFact(t, s, "node-a", key)
	if f.encerrado != 1 {
		t.Fatalf("after end: encerrado = %d, want 1", f.encerrado)
	}
	if mustIngest(t, s, mk("start", "2026-10-10T10:50:00-03:00", 6)) {
		t.Error("a start older than the stored sessao_ts_unix: applied = true, want false")
	}
	f = jobFact(t, s, "node-a", key)
	if f.encerrado != 1 {
		t.Errorf("after the ignored start: encerrado = %d, want 1 (still tombstoned)", f.encerrado)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 0 {
		t.Errorf("inventory jobs = %d, want 0", len(inv.Jobs))
	}
}

// TestIngestSessionFieldsIgnored: invalid card/pr/branch values in the
// session dados are ignored, not a failing record; a session with no
// target touches only the node.
// Mutation captured: a validation that fails the record on a bad card
// turns the first case red.
func TestIngestSessionFieldsIgnored(t *testing.T) {
	s := openStore(t)
	rec := outbox.Record{
		Schema: 1, Seq: 5, TS: "2026-10-10T12:10:00-03:00",
		Tipo: outbox.TipoSession, Maquina: "node-a",
		IdempotencyKey: "node-a:5",
		Dados:          json.RawMessage(`{"acao":"start","projeto":"example-org/example-repo","branch":"..bad","card":"BAD CARD","pr":"067"}`),
	}
	if !mustIngest(t, s, rec) {
		t.Fatal("session with invalid fields: applied = false, want the bad values ignored")
	}
	f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#..bad")
	// The invalid branch is ignored, so there is no sessao target at all:
	// only the node is touched.
	if f.exists {
		t.Errorf("session row created with an invalid branch, want it ignored")
	}
	if n := tableCount(t, s, "jobs"); n != 0 {
		t.Errorf("job rows = %d, want 0", n)
	}
	if n := tableCount(t, s, "nodes"); n != 1 {
		t.Errorf("nodes = %d, want the node touched", n)
	}
}

// TestIngestDecision: a decision touches the node and, for an existing
// non-tombstoned job, only its ultima_atividade.
// Mutation captured: a decision that moves the fase (or touches a
// tombstoned job) fails the assertions.
func TestIngestDecision(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(nil))
	rec := outbox.Record{
		Schema: 1, Seq: 6, TS: "2026-10-10T12:20:00-03:00",
		Tipo: outbox.TipoDecision, Maquina: "node-a",
		JobID: ptr("job-1"), IdempotencyKey: "node-a:6",
		Dados: json.RawMessage(`{"resumo":"decided","motivo":"because","escopo":"projeto"}`),
	}
	if !mustIngest(t, s, rec) {
		t.Fatal("decision: applied = false")
	}
	f := jobFact(t, s, "node-a", "job-1")
	if f.ultima == nil || *f.ultima != "2026-10-10T12:20:00-03:00" {
		t.Errorf("ultima_atividade = %v, want the record ts", f.ultima)
	}
	if f.fase == nil || *f.fase != jobapi.TypeAccepted {
		t.Errorf("fase = %v, want untouched (accepted)", f.fase)
	}
	// A decision naming an unknown job touches only the node.
	rec.JobID = ptr("job-unknown")
	rec.IdempotencyKey = "node-a:7"
	if !mustIngest(t, s, rec) {
		t.Fatal("decision for an unknown job: applied = false, want true")
	}
	if f2 := jobFact(t, s, "node-a", "job-unknown"); f2.exists {
		t.Errorf("a decision created the job row, want the node only")
	}
}

// TestIngestTombstoneSweep: a tombstone older than TombstoneTTL is
// deleted (with its events) at the start of an Ingest.
// Mutation captured: a missing sweep (or a >= swapped with >) leaves the
// stale row.
func TestIngestTombstoneSweep(t *testing.T) {
	s := openStore(t)
	_, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix) VALUES('node-a','job-old','job',1,?)`,
		fixedNow.Unix()-int64((TombstoneTTL/time.Second))-1)
	if err != nil {
		t.Fatalf("seed the stale tombstone: %v", err)
	}
	_, err = s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo) VALUES('node-a','job-old',1,'2026-01-01T00:00:00-03:00','note','x')`)
	if err != nil {
		t.Fatalf("seed the stale event: %v", err)
	}
	mustIngest(t, s, recordBase(nil))
	if n := tableCount(t, s, "jobs") - 1; n != 0 {
		t.Errorf("stale tombstones left = %d, want 0 (the ingested job is expected)", n)
	}
	if n := tableCount(t, s, "events"); n != 1 {
		t.Errorf("events left = %d, want 1 (the ingested one)", n)
	}
}

// pathFor is a fresh store path for tests that need non-default options.
func pathFor(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "registry.db")
}

// sessaoRec builds one session record on the fixed machine; job != ""
// targets the named job (the branch is a field there, not part of the key).
func sessaoRec(ts, acao, card, branch, pr, job string) outbox.Record {
	dados := map[string]any{"acao": acao, "projeto": "example-org/example-repo"}
	if card != "" {
		dados["card"] = card
	}
	if branch != "" {
		dados["branch"] = branch
	}
	if pr != "" {
		dados["pr"] = pr
	}
	if job != "" {
		dados["job"] = job
	}
	b, _ := json.Marshal(dados)
	return recordBase(func(r *outbox.Record) {
		r.Tipo = outbox.TipoSession
		r.TS = ts
		if job != "" {
			r.JobID = ptr(job)
		}
		r.IdempotencyKey = "node-a:s" + ts
		r.Projeto = "example-org/example-repo"
		r.Dados = b
	})
}

// TestIngestSessionFieldsReplace: after the ordering guard accepts a newer
// session record, every validated non-empty field it carries replaces the
// stored value; an omitted field keeps the stored value, and an older
// replay changes nothing (R1).
func TestIngestSessionFieldsReplace(t *testing.T) {
	s := openStore(t)
	const chave = "job-500"
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:00:00-03:00", "start", "card-1", "feat/old", "1", chave)) {
		t.Fatal("start: applied = false")
	}
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:30:00-03:00", "update", "card-2", "feat/current", "2", chave)) {
		t.Fatal("update: applied = false")
	}
	f := jobFact(t, s, "node-a", chave)
	if !f.exists || f.branch == nil || *f.branch != "feat/current" || f.card == nil || *f.card != "CARD-2" || f.pr == nil || *f.pr != "2" {
		t.Fatalf("after the update: row = %+v, want the replaced values", f)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	var shown *Job
	for i := range inv.Jobs {
		if inv.Jobs[i].Chave == chave {
			shown = &inv.Jobs[i]
		}
	}
	if shown == nil || shown.Branch == nil || *shown.Branch != "feat/current" {
		t.Fatalf("inventory shows %+v, want the new branch", shown)
	}
	// A record that omits a field keeps the stored value for it.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:45:00-03:00", "update", "card-3", "", "", chave)) {
		t.Fatal("partial update: applied = false")
	}
	f = jobFact(t, s, "node-a", chave)
	if f.card == nil || *f.card != "CARD-3" || f.branch == nil || *f.branch != "feat/current" || f.pr == nil || *f.pr != "2" {
		t.Errorf("after the partial update: row = %+v, want the card replaced and the omitted fields kept", f)
	}
	res, err := s.Check(CheckRequest{Agente: "dja", Card: "card-2", Projeto: "example-org/example-repo", Branch: "feat/current"})
	if err != nil {
		t.Fatalf("Check card-2: %v", err)
	}
	if res.Status != CheckDuplicate {
		t.Errorf("check for the new card = %q, want duplicate", res.Status)
	}
	res, err = s.Check(CheckRequest{Agente: "dja", Card: "card-1"})
	if err != nil {
		t.Fatalf("Check card-1: %v", err)
	}
	if res.Status != CheckOK {
		t.Errorf("check for the old card = %q, want ok", res.Status)
	}
	// An older replay of the start changes nothing.
	if mustIngest(t, s, sessaoRec("2026-10-10T10:00:00-03:00", "start", "card-1", "feat/old", "1", chave)) {
		t.Fatal("older replay: applied = true, want false")
	}
	f = jobFact(t, s, "node-a", chave)
	if f.branch == nil || *f.branch != "feat/current" || f.card == nil || *f.card != "CARD-3" {
		t.Errorf("after the older replay: row = %+v, want unchanged", f)
	}
}

// TestIngestSessionEndFirst: a jobless end for a target that does not
// exist yet creates the row already tombstoned, and an older start
// arriving later is ignored by the ordering guard (R2).
func TestIngestSessionEndFirst(t *testing.T) {
	s := openStore(t)
	const chave = "sessao:example-org/example-repo#feat/x"
	// The end arrives first for a target that does not exist yet.
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", "")) {
		t.Fatal("end-first: applied = false")
	}
	f := jobFact(t, s, "node-a", chave)
	if !f.exists || f.encerrado != 1 {
		t.Fatalf("row = %+v, want created and tombstoned", f)
	}
	var encerradoUnix sql.NullInt64
	if err := s.db.QueryRow(`SELECT encerrado_unix FROM jobs WHERE maquina = ? AND chave = ?`, "node-a", chave).Scan(&encerradoUnix); err != nil {
		t.Fatalf("encerrado_unix: %v", err)
	}
	if !encerradoUnix.Valid || encerradoUnix.Int64 != fixedNow.Unix() {
		t.Errorf("encerrado_unix = %v, want the store now", encerradoUnix)
	}
	// The tombstone never appears in the inventory.
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	for _, j := range inv.Jobs {
		if j.Chave == chave {
			t.Fatalf("inventory lists the tombstoned row %+v", j)
		}
	}
	// And the check for its branch is free.
	res, err := s.Check(CheckRequest{Agente: "dja", Projeto: "example-org/example-repo", Branch: "feat/x"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Status != CheckOK {
		t.Errorf("check for the tombstoned branch = %q, want ok", res.Status)
	}
	// An older start arriving later is ignored.
	if mustIngest(t, s, sessaoRec("2026-10-10T11:00:00-03:00", "start", "card-1", "feat/x", "", "")) {
		t.Fatal("older start after the end: applied = true, want false")
	}
	f = jobFact(t, s, "node-a", chave)
	if f.encerrado != 1 || f.card == nil || *f.card != "CARD-9" {
		t.Errorf("after the older start: row = %+v, want unchanged", f)
	}
}

// TestIngestReplayKeepsObsoleto: the replay refusals of a job_event are
// decided before any update of the job row, so an applied = false record
// changes no job column (R4).
func TestIngestReplayKeepsObsoleto(t *testing.T) {
	s := openStore(t)
	rec := recordBase(nil)
	if !mustIngest(t, s, rec) {
		t.Fatal("first ingest: applied = false")
	}
	mustApplyPoll(t, s, PollResult{Maquina: "node-a", At: fixedNow, OK: false, Motivo: "probe_timeout"})
	if f := jobFact(t, s, "node-a", "job-1"); f.obsoleto != 1 {
		t.Fatalf("after the failed poll: obsoleto = %d, want 1", f.obsoleto)
	}
	if mustIngest(t, s, rec) {
		t.Fatal("replay: applied = true, want false")
	}
	f := jobFact(t, s, "node-a", "job-1")
	if f.obsoleto != 1 {
		t.Errorf("after the replay: obsoleto = %d, want 1 (the replay changed no column)", f.obsoleto)
	}
}

// TestIngestSessionProvenanceSurvivesPoll: a job row created by a session
// record has the push provenance, so a poll that does not list its
// workspace only clears the workspace fields and never deletes it (R5).
func TestIngestSessionProvenanceSurvivesPoll(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Tipo = outbox.TipoSession
		r.TS = "2026-10-10T11:30:00-03:00"
		r.JobID = ptr("job-300")
		r.IdempotencyKey = "node-a:s300"
		r.Dados = json.RawMessage(`{"acao":"start","projeto":"example-org/example-repo","branch":"feat/survive","card":"card-7","job":"job-300"}`)
	}))
	mustApplyPoll(t, s, okPoll("node-a", "", PolledWorkspace{ID: "w9", Label: "hm-first-impl-ref9", AgentStatus: "working"}))
	f := jobFact(t, s, "node-a", "job-300")
	if !f.exists {
		t.Fatal("the pushed session job was deleted by the poll")
	}
	if f.workspace != nil {
		t.Errorf("job after the poll = %+v, want the workspace fields cleared", f)
	}
	if f.branch == nil || *f.branch != "feat/survive" {
		t.Errorf("job after the poll = %+v, want the branch kept", f)
	}
	res, err := s.Check(CheckRequest{Agente: "dja", Card: "card-7", Projeto: "example-org/example-repo", Branch: "feat/survive"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Status != CheckDuplicate {
		t.Errorf("check for the surviving branch = %q, want duplicate", res.Status)
	}
}

// TestIngestEventTipoPattern: the event tipo must match
// ^[a-z][a-z0-9_]{0,63}$ or the record is ErrInvalid (R7a).
func TestIngestEventTipoPattern(t *testing.T) {
	s := openStore(t)
	for _, bad := range []string{"Question", "bad-type", "9lives", strings.Repeat("a", 65), ""} {
		_, err := s.Ingest(recordBase(func(r *outbox.Record) {
			r.Dados = eventData(1, bad, "x", nil)
		}))
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("tipo %q: err = %v, want ErrInvalid", bad, err)
		}
	}
	if n := tableCount(t, s, "events"); n != 0 {
		t.Errorf("events = %d, want 0", n)
	}
	// A well-formed underscored tipo is admitted.
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.Dados = eventData(1, "my_new_type", "x", nil)
	})) {
		t.Error("my_new_type: applied = false, want true")
	}
}

// TestIngestEventRefValueBounds: a kept ref value must be a JSON string of
// at most 200 bytes, a JSON number or a boolean; anything else is dropped
// from the stored refs, not an error (R7b).
func TestIngestEventRefValueBounds(t *testing.T) {
	s := openStore(t)
	refs := map[string]any{
		"pane":    "w1:p1",
		"sha":     strings.Repeat("a", 200),
		"papel":   42,
		"motivo":  true,
		"agente":  strings.Repeat("b", 201),
		"seq_ref": map[string]any{"k": 1},
		"exit":    []any{7},
		"pr":      nil,
	}
	rec := recordBase(func(r *outbox.Record) {
		r.Dados = eventData(1, jobapi.TypeCommit, "x", refs)
	})
	if !mustIngest(t, s, rec) {
		t.Fatal("ingest: applied = false")
	}
	var refsJSON sql.NullString
	if err := s.db.QueryRow(`SELECT refs FROM events WHERE maquina = 'node-a' AND chave = 'job-1'`).Scan(&refsJSON); err != nil {
		t.Fatalf("refs: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(refsJSON.String), &m); err != nil {
		t.Fatalf("refs json: %v", err)
	}
	for _, kept := range []string{"pane", "sha", "papel", "motivo"} {
		if _, ok := m[kept]; !ok {
			t.Errorf("refs %q dropped, want kept", kept)
		}
	}
	for _, dropped := range []string{"agente", "seq_ref", "exit", "pr"} {
		if _, ok := m[dropped]; ok {
			t.Errorf("refs %q kept, want dropped", dropped)
		}
	}
	if v, _ := m["sha"].(string); len(v) != 200 {
		t.Errorf("sha = %d bytes, want the 200-byte boundary kept", len(v))
	}
}
