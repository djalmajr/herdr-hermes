package registry

// DJA-275: the periodic sweep. SweepEnded runs the ended-row sweep in
// its own transaction with the caller's clock; serve runs it once at
// start and every 24 h. The predicate is unchanged: an ended row is
// swept strictly past the retention, so a row ended exactly at the
// retention stays; the fence window and future-dated rows stay; an
// active row never leaves. An interrupted sweep deletes nothing; a
// retry with the same now converges; a backward clock keeps what the
// previous sweep kept.

import (
	"fmt"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
)

// dja275SeedEnded seeds one ended row (with one event) of the given
// kind at the given age in seconds; a negative age is a future-dated
// row.
func dja275SeedEnded(t *testing.T, s *Store, maquina, chave, origem string, age int64) {
	t.Helper()
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix)
		VALUES(?, ?, ?, ?, 1, ?)`, maquina, chave, origem, chave, fixedNow.Unix()-age); err != nil {
		t.Fatalf("seed the ended %s row: %v", origem, err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
		VALUES(?, ?, 1, '2026-01-01T00:00:00-03:00', 'note', 'x')`, maquina, chave); err != nil {
		t.Fatalf("seed the event of %s: %v", chave, err)
	}
}

// TestDJA275SweepBoundary: the sweep deletes the row ended 30 days and
// 1 second ago, with its event, and keeps the row ended exactly 30 days
// ago, the row ended 6 days ago, the future-dated row and the active
// row.
func TestDJA275SweepBoundary(t *testing.T) {
	s := openStore(t)
	dja275SeedEnded(t, s, "node-a", "j-gone", "job", 30*24*3600+1)
	dja275SeedEnded(t, s, "node-a", "j-border", "job", 30*24*3600)
	dja275SeedEnded(t, s, "node-a", "j-recent", "job", 6*24*3600)
	dja275SeedEnded(t, s, "node-a", "j-future", "job", -3600)
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, ultima_atividade, ultima_atividade_unix)
		VALUES('node-a', 'j-active', 'job', 'j-active', ?, ?)`,
		fixedNow.Add(-40*24*time.Hour).Format(outbox.TSLayout), fixedNow.Unix()-40*24*3600); err != nil {
		t.Fatalf("seed the active row: %v", err)
	}
	// A sessao row at the same boundary, kept exactly at the retention:
	// the sweep is of every kind.
	dja275SeedEnded(t, s, "node-a", "sessao:org001/repo#feat/a", "sessao", 30*24*3600)

	if err := s.SweepEnded(fixedNow); err != nil {
		t.Fatalf("SweepEnded: %v", err)
	}
	if f := jobFact(t, s, "node-a", "j-gone"); f.exists {
		t.Errorf("j-gone = %+v, want swept with its event", f)
	}
	var ev int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE maquina = 'node-a' AND chave = 'j-gone'`).Scan(&ev); err != nil {
		t.Fatalf("count the swept events: %v", err)
	}
	if ev != 0 {
		t.Errorf("swept events = %d, want 0", ev)
	}
	for _, chave := range []string{"j-border", "j-recent", "j-future", "j-active", "sessao:org001/repo#feat/a"} {
		want := 1
		if chave == "j-active" {
			want = 0
		}
		f := jobFact(t, s, "node-a", chave)
		if !f.exists {
			t.Errorf("%s = gone, want kept by the sweep", chave)
			continue
		}
		if f.encerrado != want {
			t.Errorf("%s encerrado = %d, want %d", chave, f.encerrado, want)
		}
	}
	if n := tableCount(t, s, "jobs"); n != 5 {
		t.Errorf("jobs rows = %d, want 5 (only the past-retention job row went)", n)
	}
}

// TestDJA275SweepInterruptedAtomic: a failure injected inside the sweep
// transaction (a trigger aborting the jobs delete, after the events
// delete succeeded) rolls the whole sweep back: no row and no event are
// left half-deleted. Dropping the trigger, the retry converges.
func TestDJA275SweepInterruptedAtomic(t *testing.T) {
	s := openStore(t)
	dja275SeedEnded(t, s, "node-a", "j-gone", "job", 30*24*3600+1)
	dja275SeedEnded(t, s, "node-a", "j-keep", "job", 6*24*3600)
	before := retDump(t, s)

	// The fault: the events delete of the sweep succeeds, the jobs
	// delete aborts, and the whole transaction rolls back.
	if _, err := s.db.Exec(`CREATE TRIGGER dja275_fail BEFORE DELETE ON jobs
		BEGIN SELECT RAISE(ABORT, 'dja275 sweep fault'); END`); err != nil {
		t.Fatalf("create the fault trigger: %v", err)
	}
	t.Cleanup(func() {
		if _, err := s.db.Exec(`DROP TRIGGER IF EXISTS dja275_fail`); err != nil {
			t.Errorf("drop the fault trigger: %v", err)
		}
	})
	if err := s.SweepEnded(fixedNow); err == nil {
		t.Fatal("SweepEnded with the injected fault = nil, want the fault error")
	}
	after := retDump(t, s)
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the interrupted sweep:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
	if f := jobFact(t, s, "node-a", "j-gone"); !f.exists {
		t.Errorf("j-gone = gone, want kept by the rolled-back sweep (%+v)", f)
	}
	var ev int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM events WHERE maquina = 'node-a' AND chave = 'j-gone'`).Scan(&ev); err != nil {
		t.Fatalf("count the events: %v", err)
	}
	if ev != 1 {
		t.Errorf("j-gone events = %d, want 1 (the events delete rolled back)", ev)
	}

	// The retry converges: the fault is gone, the sweep deletes only
	// the past-retention row.
	if _, err := s.db.Exec(`DROP TRIGGER dja275_fail`); err != nil {
		t.Fatalf("drop the fault trigger: %v", err)
	}
	if err := s.SweepEnded(fixedNow); err != nil {
		t.Fatalf("SweepEnded retry: %v", err)
	}
	if f := jobFact(t, s, "node-a", "j-gone"); f.exists {
		t.Errorf("j-gone after the retry = %+v, want swept", f)
	}
	if f := jobFact(t, s, "node-a", "j-keep"); !f.exists {
		t.Errorf("j-keep after the retry = gone, want kept (%+v)", f)
	}
}

// TestDJA275SweepRetryConverges: two sweeps with the same now converge:
// the second deletes nothing and returns no error.
func TestDJA275SweepRetryConverges(t *testing.T) {
	s := openStore(t)
	dja275SeedEnded(t, s, "node-a", "j-gone", "job", 30*24*3600+1)
	dja275SeedEnded(t, s, "node-a", "j-keep", "job", 6*24*3600)
	if err := s.SweepEnded(fixedNow); err != nil {
		t.Fatalf("first SweepEnded: %v", err)
	}
	if f := jobFact(t, s, "node-a", "j-gone"); f.exists {
		t.Fatalf("j-gone after the first sweep = %+v, want swept", f)
	}
	before := retDump(t, s)
	if err := s.SweepEnded(fixedNow); err != nil {
		t.Fatalf("second SweepEnded: %v, want no error", err)
	}
	after := retDump(t, s)
	for table, rows := range before {
		if !rowsEqual(after[table], rows) {
			t.Errorf("table %s changed by the second sweep:\nbefore: %v\nafter: %v", table, rows, after[table])
		}
	}
}

// TestDJA275ReingestAfterSweep: re-running the ingest of the records of
// a swept job does not resurrect it as a live row and does not error:
// the push cursor may advance over the replayed records.
func TestDJA275ReingestAfterSweep(t *testing.T) {
	nowFn, setNow := dja275MovableClock(fixedNow)
	s, err := Open(pathFor(t), Options{Now: nowFn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	j1 := func(seq int64, tipo string) *outbox.Record {
		rec := recordBase(func(r *outbox.Record) {
			r.JobID = ptr("j1")
			r.Seq = seq
			r.IdempotencyKey = fmt.Sprintf("j1:%d", seq)
			r.Dados = eventData(seq, tipo, "x", nil)
		})
		return &rec
	}
	if !mustIngest(t, s, *j1(1, "accepted")) {
		t.Fatal("j1 seq 1: applied = false")
	}
	if !mustIngest(t, s, *j1(2, "cleanup")) {
		t.Fatal("j1 cleanup: applied = false")
	}
	// The clock moves past the retention and the sweep deletes the
	// tombstone.
	setNow(31 * 24 * 3600)
	if err := s.SweepEnded(nowFn()); err != nil {
		t.Fatalf("SweepEnded: %v", err)
	}
	if f := jobFact(t, s, "node-a", "j1"); f.exists {
		t.Fatalf("j1 after the sweep = %+v, want swept", f)
	}
	// The replay: no error on either record, and the job is not back
	// alive.
	if _, err := s.Ingest(*j1(1, "accepted")); err != nil {
		t.Fatalf("replayed seq 1: %v, want no error (the cursor advances)", err)
	}
	if _, err := s.Ingest(*j1(2, "cleanup")); err != nil {
		t.Fatalf("replayed cleanup: %v, want no error (the cursor advances)", err)
	}
	f := jobFact(t, s, "node-a", "j1")
	if f.exists && f.encerrado == 0 {
		t.Errorf("j1 after the replay = %+v, want not live (the tombstone, not the job, came back)", f)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	for _, j := range inv.Jobs {
		if j.Chave == "j1" {
			t.Errorf("inventory lists j1 after the replay: %+v, want excluded", j)
		}
	}
}

// TestDJA275SweepClockBackward: a clock that goes backwards between two
// sweeps does not delete a row the previous sweep kept, and a
// future-dated row is never deleted, whatever the clock.
func TestDJA275SweepClockBackward(t *testing.T) {
	nowFn, setNow := dja275MovableClock(fixedNow)
	s, err := Open(pathFor(t), Options{Now: nowFn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	// a: 20 days old at t0 (kept), b: 31 days old (swept at t0), c:
	// future-dated (never swept).
	dja275SeedEnded(t, s, "node-a", "j-a", "job", 20*24*3600)
	dja275SeedEnded(t, s, "node-a", "j-b", "job", 31*24*3600)
	dja275SeedEnded(t, s, "node-a", "j-c", "job", -3600)

	if err := s.SweepEnded(nowFn()); err != nil {
		t.Fatalf("SweepEnded at t0: %v", err)
	}
	if f := jobFact(t, s, "node-a", "j-b"); f.exists {
		t.Errorf("j-b after the first sweep = %+v, want swept", f)
	}
	if f := jobFact(t, s, "node-a", "j-a"); !f.exists {
		t.Fatalf("j-a after the first sweep = gone, want kept (%+v)", f)
	}

	// The clock goes back 15 days: the kept row stays kept.
	setNow(-15 * 24 * 3600)
	if err := s.SweepEnded(nowFn()); err != nil {
		t.Fatalf("SweepEnded with the backward clock: %v", err)
	}
	if f := jobFact(t, s, "node-a", "j-a"); !f.exists {
		t.Errorf("j-a after the backward sweep = gone, want kept by both sweeps (%+v)", f)
	}

	// Farther back: the future-dated row is still never deleted.
	setNow(-45 * 24 * 3600)
	if err := s.SweepEnded(nowFn()); err != nil {
		t.Fatalf("SweepEnded far backward: %v", err)
	}
	if f := jobFact(t, s, "node-a", "j-c"); !f.exists {
		t.Errorf("j-c (future-dated) = gone, want never deleted (%+v)", f)
	}
	if f := jobFact(t, s, "node-a", "j-a"); !f.exists {
		t.Errorf("j-a after the far-backward sweep = gone, want kept (%+v)", f)
	}
}
