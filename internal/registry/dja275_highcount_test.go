package registry

// DJA-275: the high-count warn. The retained total of the jobs and
// sessions kinds (active plus ended) over the threshold (HighCountWarn,
// tuned down to a small number through the store's unexported field)
// logs one line with the counts only, at most one per 24 h of the store
// clock while the total stays above, and again when the total crosses
// back over after dropping to or below the threshold. The check runs
// after every confirmed ingest, poll and sweep, outside the transaction,
// and never blocks, undoes or skips work.

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
)

// dja275HighCountStore opens a store with a movable clock and a high
// count threshold of 3, and returns the clock setter.
func dja275HighCountStore(t *testing.T) (*Store, func(int64)) {
	t.Helper()
	nowFn, setNow := dja275MovableClock(fixedNow)
	s, err := Open(pathFor(t), Options{Now: nowFn})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	s.highCountThreshold = 3
	return s, setNow
}

// dja275SeedMixed seeds the given rows of each kind: active jobs and
// sessions, plus ended rows at one day (inside the fence).
func dja275SeedMixed(t *testing.T, s *Store, jobsActive, sessActive int) {
	t.Helper()
	dja275SeedJobs(t, s, "node-a", jobsActive, "j")
	dja275SeedSessions(t, s, "node-a", sessActive, sessActive, nil)
}

// TestDJA275HighCountAlertRule: the five examples of the repetition rule
// with the injected clock, the threshold tuned to 3 so the examples run
// as 3 (no alert), 4 (alert), 4 one hour later (no alert), 4 twenty-five
// hours later (alert) and 3 then 4 again (alert), and the ingest keeps
// accepting after the alerts.
func TestDJA275HighCountAlertRule(t *testing.T) {
	s, setNow := dja275HighCountStore(t)
	dja275SeedMixed(t, s, 2, 1) // total 3: at the threshold, no alert
	lines := setLogf(t)

	// 3 -> no alert: the check after a no-op sweep (nothing to sweep)
	// sees the total at the threshold.
	if err := s.SweepEnded(s.now()); err != nil {
		t.Fatalf("SweepEnded: %v", err)
	}
	if n := len(dja275LinesWithHighCount(t, lines)); n != 0 {
		t.Fatalf("total at the threshold: %d high-count lines, want 0 (%v)", n, *lines)
	}

	// 4 -> alert: the new row crosses the threshold.
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-4")
		r.IdempotencyKey = "j-4:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	})) {
		t.Fatal("j-4: applied = false")
	}
	if n := len(dja275LinesWithHighCount(t, lines)); n != 1 {
		t.Fatalf("total one over the threshold: %d high-count lines, want 1 (%v)", n, *lines)
	}
	want := "registry: high count: retained 4 (jobs active 3, jobs ended 0, sessions active 1, sessions ended 0), warn above 3"
	if got := (*lines)[len(*lines)-1]; got != want {
		t.Errorf("alert line = %q, want %q", got, want)
	}

	// 4 one hour later -> no alert: the check runs on a confirmed
	// duplicate (applied = false, no error) and the 24 h period is not
	// over.
	setNow(3600)
	if mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-4")
		r.IdempotencyKey = "j-4:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	})) {
		t.Fatal("duplicate j-4: applied = true, want false")
	}
	if n := len(dja275LinesWithHighCount(t, lines)); n != 1 {
		t.Fatalf("one hour over the threshold: %d high-count lines, want 1 (%v)", n, *lines)
	}

	// 25 h later -> alert: the period is over.
	setNow(25 * 3600)
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-4")
		r.IdempotencyKey = "j-4:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	}))
	if n := len(dja275LinesWithHighCount(t, lines)); n != 2 {
		t.Fatalf("25 h over the threshold: %d high-count lines, want 2 (%v)", n, *lines)
	}

	// Drop to 3 and back to 4 -> alert: the threshold re-arms below.
	if _, err := s.db.Exec(`DELETE FROM jobs WHERE maquina = 'node-a' AND chave = 'j0001'`); err != nil {
		t.Fatalf("drop one row: %v", err)
	}
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-4")
		r.IdempotencyKey = "j-4:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	}))
	if n := len(dja275LinesWithHighCount(t, lines)); n != 2 {
		t.Fatalf("total back at the threshold: %d high-count lines, want 2 (re-armed, no alert) (%v)", n, *lines)
	}
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id)
		VALUES('node-a', 'j0002', 'job', 'j0002')`); err != nil {
		t.Fatalf("add one row: %v", err)
	}
	mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-4")
		r.IdempotencyKey = "j-4:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	}))
	if n := len(dja275LinesWithHighCount(t, lines)); n != 3 {
		t.Fatalf("total over the threshold again: %d high-count lines, want 3 (the re-armed alert) (%v)", n, *lines)
	}

	// The ingest keeps accepting after the alerts.
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-5")
		r.IdempotencyKey = "j-5:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	})) {
		t.Fatal("j-5 after the alerts: applied = false, want accepted")
	}
}

// TestDJA275HighCountClockBackward: a clock that goes backwards between
// checks neither re-alerts before 24 h of advancing clock nor panics,
// and the repetition works once the clock advances past the period.
func TestDJA275HighCountClockBackward(t *testing.T) {
	s, setNow := dja275HighCountStore(t)
	dja275SeedMixed(t, s, 4, 0) // total 4: over the threshold from the start
	lines := setLogf(t)

	// The first check over the threshold (a restart always alerts on
	// the first check) fires at t0.
	if err := s.SweepEnded(s.now()); err != nil {
		t.Fatalf("SweepEnded: %v", err)
	}
	if n := len(dja275LinesWithHighCount(t, lines)); n != 1 {
		t.Fatalf("first check over the threshold: %d high-count lines, want 1 (%v)", n, *lines)
	}

	// The clock goes back 48 h: no re-alert before 24 h of the clock
	// advancing, and no panic.
	setNow(-48 * 3600)
	if err := s.SweepEnded(s.now()); err != nil {
		t.Fatalf("SweepEnded with the backward clock: %v", err)
	}
	if n := len(dja275LinesWithHighCount(t, lines)); n != 1 {
		t.Fatalf("backward clock: %d high-count lines, want 1 (no re-alert) (%v)", n, *lines)
	}

	// From the backward instant the clock advances 98 h: 50 h past the
	// recorded alert, the repetition fires.
	setNow(98 * 3600)
	if err := s.SweepEnded(s.now()); err != nil {
		t.Fatalf("SweepEnded after the advance: %v", err)
	}
	if n := len(dja275LinesWithHighCount(t, lines)); n != 2 {
		t.Fatalf("50 h past the recorded alert: %d high-count lines, want 2 (%v)", n, *lines)
	}
}

// TestDJA275HighCountConcurrentRearm: a check that samples the total at
// or below the threshold and pauses before applying the sample must not
// disarm the alert of the later check that saw the crossing: the
// sampling of the counts, the clock read and the state update run under
// the one alert mutex, a sample is never applied after a sample that
// started after it, and a single 3 -> 4 transition logs exactly one
// alert line.
//
// Mutation captured: moving the count queries or the clock read outside
// the alert mutex lets the stale at-or-below sample disarm the fresh
// alert and the third check re-fire it.
func TestDJA275HighCountConcurrentRearm(t *testing.T) {
	s := openStore(t)
	s.highCountThreshold = 3
	dja275SeedJobs(t, s, "node-a", 3, "seed")
	lines := setLogf(t)

	// The first clock read pauses the check that takes it: the check A
	// that samples the total before the crossing. The pause lingers a bit
	// after the release: in the broken code, where the sampling and the
	// state update are not serialized, the fresh check B completes first
	// and the stale sample is applied after it; with the serialized code
	// the lock, not the linger, keeps the order, so the linger is
	// harmless there.
	sampled := make(chan struct{})
	released := make(chan struct{})
	var calls atomic.Int32
	s.now = func() time.Time {
		if calls.Add(1) == 1 {
			close(sampled)
			<-released
			time.Sleep(200 * time.Millisecond)
		}
		return fixedNow
	}
	// A samples the total and pauses on the clock read; B starts only
	// after the fourth row exists and sees the crossing; C runs last.
	startA, startB := make(chan struct{}), make(chan struct{})
	aDone, bDone := make(chan struct{}), make(chan struct{})
	go func() { <-startA; s.checkHighCount(); close(aDone) }()
	go func() { <-startB; s.checkHighCount(); close(bDone) }()
	close(startA)
	<-sampled
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id)
		VALUES('node-a', 'four', 'job', 'four')`); err != nil {
		t.Fatalf("cross the threshold: %v", err)
	}
	close(startB)
	close(released)
	<-aDone
	<-bDone
	cDone := make(chan struct{})
	go func() { s.checkHighCount(); close(cDone) }()
	<-cDone
	for _, l := range *lines {
		t.Log(l)
	}
	if n := len(*lines); n != 1 {
		t.Errorf("got %d alerts for a single 3 -> 4 transition without a later drop or clock advance; want 1 (%v)", n, *lines)
	}
}

// TestDJA275HighCountHygiene: the alert line carries the counts and the
// threshold only, never a machine label, a key, a card or a path.
func TestDJA275HighCountHygiene(t *testing.T) {
	s, _ := dja275HighCountStore(t)
	ended := fixedNow.Unix() - 24*3600
	for _, chave := range []string{"j-f1", "j-f2"} {
		if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix)
			VALUES('node-a', ?, 'job', ?, 1, ?)`, chave, chave, ended); err != nil {
			t.Fatalf("seed the ended job %s: %v", chave, err)
		}
	}
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix)
		VALUES('node-a', 'sessao:example-org/example-repo#feat/x', 'sessao', 1, ?)`, ended); err != nil {
		t.Fatalf("seed the ended session: %v", err)
	}
	lines := setLogf(t)
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:30:00-03:00", "start", "card-9", "feat/y", "", "")) {
		t.Fatal("session start: applied = false")
	}
	if n := len(dja275LinesWithHighCount(t, lines)); n != 1 {
		t.Fatalf("high-count lines = %d, want 1 (%v)", n, *lines)
	}
	line := (*lines)[len(*lines)-1]
	for _, want := range []string{
		"retained 4", "jobs active 0", "jobs ended 2",
		"sessions active 1", "sessions ended 1", "warn above 3",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("alert line = %q, want the count %q", line, want)
		}
	}
	for _, leak := range []string{"node-a", "j-f1", "j-f2", "sessao:example-org/example-repo#feat/x", "feat/y", "card-9", "CARD-9"} {
		if strings.Contains(line, leak) {
			t.Errorf("alert line = %q, must not carry %q", line, leak)
		}
	}
}
