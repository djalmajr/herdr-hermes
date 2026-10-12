package registry

// Regression tests for the total retention budget of the registry
// (S-F3): the jobs and sessions budgets count the TOTAL retained rows of
// the kind (active plus ended), an end-first session flood and a cleanup
// flood of unknown jobs are refused at the default budget with "<kind>
// limit reached", an ended row past the fence but inside the 30-day
// retention is kept and still blocks a replayed event, and an ended row
// past the retention is swept. These tests compile on the pre-fix source
// and fail there: the old admission capped only the live rows and swept
// every tombstone after 7 days.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
)

// retSeedFences seeds n ended rows of one kind, all inside the 7-day
// fence (one day old), in one transaction.
func retSeedFences(t *testing.T, s *Store, n int, origem, chaveFmt string) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for i := 1; i <= n; i++ {
		chave := fmt.Sprintf(chaveFmt, i)
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, projeto, branch, encerrado, encerrado_unix)
			VALUES('node-a', ?, ?, ?, ?, 'feat/x', 1, ?)`,
			chave, origem, chave, fmt.Sprintf("org%03d/repo", i),
			fixedNow.Unix()-24*3600); err != nil {
			tx.Rollback()
			t.Fatalf("seed %s %d: %v", origem, i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
}

// retLimitMotivo is the text after the ErrInvalid prefix of a refusal.
func retLimitMotivo(err error) string {
	if err == nil {
		return "<nil>"
	}
	return strings.TrimPrefix(err.Error(), ErrInvalid.Error()+": ")
}

// mustMarshalRetRecord marshals the record as the ingest request body.
func mustMarshalRetRecord(t *testing.T, rec outbox.Record) []byte {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return b
}

// TestDJA229RetentionEndFirstSessionFlood: with the default budget, 1000
// retained session rows (fences inside the 7-day window) leave no room
// for the next end-first session end: it is refused with the text
// "sessions limit reached", the row count stays 1000, and the HTTP
// answer is the one-line 400 invalid. On the pre-fix source the admission
// capped only the live rows, so the record was admitted and a 1001-th
// row appeared: the error check and the count check fail there.
func TestDJA229RetentionEndFirstSessionFlood(t *testing.T) {
	s := openStore(t)
	retSeedFences(t, s, MaxJobs, "sessao", "sessao:org%03d/repo#feat/x")
	rec := sessaoRec("2026-10-10T11:30:00-03:00", "end", "card-9", "feat/x", "", "")
	_, err := s.Ingest(rec)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("end-first session at the default budget: err = %v, want ErrInvalid", err)
	}
	if got := retLimitMotivo(err); got != "sessions limit reached" {
		t.Fatalf("refusal text = %q, want %q", got, "sessions limit reached")
	}
	if n := tableCount(t, s, "jobs"); n != MaxJobs {
		t.Fatalf("jobs rows = %d, want %d (the refused record created nothing)", n, MaxJobs)
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/x"); f.exists {
		t.Errorf("the refused end-first row exists: %+v, want absent", f)
	}

	// The same refusal through the HTTP handler: one 400 invalid line.
	w := doReq(t, NewHandler(s), "POST", "/v1/ingest",
		string(mustMarshalRetRecord(t, rec)), jsonHeaders(nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("HTTP code = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	want := `{"status":"invalid","motivo":"sessions limit reached"}` + "\n"
	if w.Body.String() != want {
		t.Errorf("HTTP body = %q, want %q", w.Body.String(), want)
	}
}

// TestDJA229RetentionCleanupFlood: with the default budget, 1000 retained
// job rows leave no room for the cleanup of a job never seen: it is
// refused with the text "jobs limit reached" and the row count stays
// 1000. On the pre-fix source a cleanup of an unknown job was admitted
// without any check, so the record was applied and a 1001-th row
// appeared: the error check and the count check fail there.
func TestDJA229RetentionCleanupFlood(t *testing.T) {
	s := openStore(t)
	retSeedFences(t, s, MaxJobs, "job", "seed-j-%04d")
	rec := recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-new")
		r.IdempotencyKey = "j-new:5"
		r.Dados = eventData(5, jobapi.TypeCleanup, "x", nil)
	})
	_, err := s.Ingest(rec)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("cleanup of an unknown job at the default budget: err = %v, want ErrInvalid", err)
	}
	if got := retLimitMotivo(err); got != "jobs limit reached" {
		t.Fatalf("refusal text = %q, want %q", got, "jobs limit reached")
	}
	if n := tableCount(t, s, "jobs"); n != MaxJobs {
		t.Fatalf("jobs rows = %d, want %d (the refused cleanup created nothing)", n, MaxJobs)
	}
	if f := jobFact(t, s, "node-a", "j-new"); f.exists {
		t.Errorf("the refused fence row exists: %+v, want absent", f)
	}
}

// TestDJA229RetentionFenceExpiredKept: an ended job row 8 days old (the
// fence expired, the 30-day retention not reached) still exists after an
// Ingest and still blocks a replayed event of that job (applied = false,
// the job not back in the inventory). On the pre-fix source the 7-day
// sweep deleted the row, so the exists check and the applied = false
// check fail there.
func TestDJA229RetentionFenceExpiredKept(t *testing.T) {
	s := openStore(t)
	eightDays := fixedNow.Unix() - 8*24*3600
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix, ultima_atividade, ultima_atividade_unix)
		VALUES('node-a','job-8d','job','job-8d',1,?, ?, ?)`, eightDays, fixedNow.Format(outbox.TSLayout), eightDays); err != nil {
		t.Fatalf("seed the 8-day ended row: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES('node-a','job-8d',1,'2026-01-01T00:00:00-03:00','accepted','x')`); err != nil {
		t.Fatalf("seed the 8-day event: %v", err)
	}
	mustIngest(t, s, recordBase(nil))
	if f := jobFact(t, s, "node-a", "job-8d"); !f.exists || f.encerrado != 1 {
		t.Fatalf("8-day ended row after the Ingest = %+v, want still tombstoned", f)
	}
	applied := mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("job-8d")
		r.IdempotencyKey = "job-8d:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "replay", nil)
	}))
	if applied {
		t.Fatal("replayed event of the 8-day ended job: applied = true, want false (the tombstone blocks it)")
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	for _, j := range inv.Jobs {
		if j.Chave == "job-8d" {
			t.Errorf("inventory lists the ended job %+v, want excluded", j)
		}
	}
}

// TestDJA229RetentionPastRetentionSwept: an ended row 31 days old (past
// the 30-day retention) is gone, with its events, after an Ingest. The
// pre-fix source already passed this one (its 7-day sweep reached 31
// days); it guards that the longer retention still removes the row once
// the retention is past.
func TestDJA229RetentionPastRetentionSwept(t *testing.T) {
	s := openStore(t)
	thirtyOneDays := fixedNow.Unix() - 31*24*3600
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix)
		VALUES('node-a','job-31d','job','job-31d',1,?)`, thirtyOneDays); err != nil {
		t.Fatalf("seed the 31-day ended row: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES('node-a','job-31d',1,'2026-01-01T00:00:00-03:00','accepted','x')`); err != nil {
		t.Fatalf("seed the 31-day event: %v", err)
	}
	mustIngest(t, s, recordBase(nil))
	if f := jobFact(t, s, "node-a", "job-31d"); f.exists {
		t.Fatalf("31-day ended row after the Ingest = %+v, want swept", f)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE maquina = 'node-a' AND chave = 'job-31d'`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 0 {
		t.Errorf("31-day events left = %d, want 0", n)
	}
}
