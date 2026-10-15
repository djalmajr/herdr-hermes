package registry

// Regression tests for the ended-row retention of the registry: an
// ended row past the 7-day fence but inside the 30-day retention is kept
// and still blocks a replayed event, and an ended row past the retention
// is swept with its events. The kinds have no count cap (DJA-275), so
// the retention is the only thing that bounds the ended rows.

import (
	"encoding/json"
	"testing"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
)

// mustMarshalRetRecord marshals the record as the ingest request body.
func mustMarshalRetRecord(t *testing.T, rec outbox.Record) []byte {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return b
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
