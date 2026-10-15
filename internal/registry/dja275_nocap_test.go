package registry

// DJA-275: the jobs and sessions kinds have no count cap. Any new job or
// session row is admitted, active or ended, and no row is evicted to make
// room for another; the retained rows are deleted by age (the periodic
// sweep) and the high-count warn is the advance signal. These tests use
// the fixed clock and seed large populations with direct SQL in one
// transaction, crossing the former cap through the ingest and the poll
// API.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
)

// dja275SeedJobs seeds n active job rows of one machine in one
// transaction, like the existing retention seeds; the rows are pushed
// jobs (fonte_push set), so a poll of the machine never reconciles
// them away.
func dja275SeedJobs(t *testing.T, s *Store, maquina string, n int, prefix string) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for i := 0; i < n; i++ {
		chave := fmt.Sprintf("%s%04d", prefix, i)
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, fonte_push)
			VALUES(?, ?, 'job', ?, 1)`, maquina, chave, chave); err != nil {
			tx.Rollback()
			t.Fatalf("seed job %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
}

// dja275SeedSessions seeds n sessao rows of one kind state in one
// transaction: encerradoUnix nil for active rows, the given age in
// seconds for ended rows.
func dja275SeedSessions(t *testing.T, s *Store, maquina string, n, active int, encerradoUnix *int64) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for i := 0; i < n; i++ {
		end := 0
		var eu interface{}
		if i >= active && encerradoUnix != nil {
			end = 1
			eu = *encerradoUnix
		}
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix)
			VALUES(?, ?, 'sessao', ?, ?)`,
			maquina, fmt.Sprintf("sessao:org%04d/repo#feat/x", i), end, eu); err != nil {
			tx.Rollback()
			t.Fatalf("seed session %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
}

// TestDJA275IngestOverThousandJobs: more than 1000 active jobs are
// admitted and retained through the ingest; the 1001-st new row, refused
// at the old cap, is applied and the HTTP answer the push cursor
// advances on is the one-line 200 ok.
func TestDJA275IngestOverThousandJobs(t *testing.T) {
	s := openStore(t)
	dja275SeedJobs(t, s, "node-a", 1000, "j")
	rec := recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-1001")
		r.IdempotencyKey = "j-1001:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	})
	applied, err := s.Ingest(rec)
	if err != nil {
		t.Fatalf("Ingest of the 1001-st job: %v, want admitted", err)
	}
	if !applied {
		t.Fatal("Ingest of the 1001-st job: applied = false, want admitted")
	}
	// The push client advances its cursor on the one-line 200 ok answer.
	body := mustMarshalRetRecord(t, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-1002")
		r.IdempotencyKey = "j-1002:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	}))
	w := doReq(t, NewHandler(s), "POST", "/v1/ingest", string(body), jsonHeaders(nil))
	if w.Code != http.StatusOK || w.Body.String() != `{"status":"ok","aplicado":true}`+"\n" {
		t.Fatalf("HTTP ingest = %d %q, want the one-line 200 ok the cursor advances on", w.Code, w.Body.String())
	}
	if n := tableCount(t, s, "jobs"); n != 1002 {
		t.Errorf("jobs rows = %d, want 1002 (the new rows were retained)", n)
	}
	if f := jobFact(t, s, "node-a", "j-1001"); !f.exists || f.encerrado != 0 {
		t.Errorf("j-1001 = %+v, want the retained live row", f)
	}
}

// TestDJA275IngestOverThousandSessions: more than 1000 active sessions
// are admitted and retained through the ingest; the end-first form of a
// session past the old cap is admitted already tombstoned.
func TestDJA275IngestOverThousandSessions(t *testing.T) {
	s := openStore(t)
	dja275SeedSessions(t, s, "node-a", 1000, 1000, nil)
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:30:00-03:00", "start", "card-9", "feat/y", "", "")) {
		t.Fatal("the 1001-st session start: applied = false, want admitted")
	}
	if !mustIngest(t, s, sessaoRec("2026-10-10T11:31:00-03:00", "end", "", "feat/z", "", "")) {
		t.Fatal("the 1002-nd end-first session: applied = false, want admitted")
	}
	if n := tableCount(t, s, "jobs"); n != 1002 {
		t.Errorf("jobs rows = %d, want 1002 (the new sessions were retained)", n)
	}
	if f := jobFact(t, s, "node-a", "sessao:example-org/example-repo#feat/z"); !f.exists || f.encerrado != 1 {
		t.Errorf("the end-first session = %+v, want the retained tombstone", f)
	}
}

// TestDJA275PollOverThousandWorkspaces: a poll listing more than 1000
// workspaces is applied whole; every workspace row is retained.
func TestDJA275PollOverThousandWorkspaces(t *testing.T) {
	s := openStore(t)
	var ws []PolledWorkspace
	for i := 0; i < 1001; i++ {
		ws = append(ws, PolledWorkspace{
			ID:          fmt.Sprintf("w%04d", i),
			Label:       fmt.Sprintf("hm-agent-impl-x%04d", i),
			AgentStatus: "working",
		})
	}
	mustApplyPoll(t, s, okPoll("node-a", "", ws...))
	if n := tableCount(t, s, "jobs"); n != 1001 {
		t.Fatalf("jobs rows = %d, want 1001 (every workspace was retained)", n)
	}
	if f := jobFact(t, s, "node-a", "ws:w0000"); !f.exists || f.encerrado != 0 {
		t.Fatalf("workspace row 0 = %+v, want retained", f)
	}
	inv, err := s.Inventory(Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv.Jobs) != 1001 {
		t.Errorf("inventory jobs = %d, want 1001 (the poll rows are listed)", len(inv.Jobs))
	}
}

// TestDJA275MixedActiveEndedNoRefusal: a mix of active and ended rows of
// both kinds above the old cap neither refuses nor evicts: a new ingest
// and a new poll row are admitted on top of the 2200 retained rows.
func TestDJA275MixedActiveEndedNoRefusal(t *testing.T) {
	s := openStore(t)
	dja275SeedJobs(t, s, "node-a", 600, "a")
	ended := fixedNow.Unix() - 24*3600
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	for i := 0; i < 500; i++ {
		chave := fmt.Sprintf("e%04d", i)
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix)
			VALUES(?, ?, 'job', ?, 1, ?)`, "node-a", chave, chave, ended); err != nil {
			tx.Rollback()
			t.Fatalf("seed ended job %d: %v", i, err)
		}
	}
	for i := 0; i < 600; i++ {
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado)
			VALUES('node-b', ?, 'sessao', 0)`,
			fmt.Sprintf("sessao:org%04d/repo#feat/a", i)); err != nil {
			tx.Rollback()
			t.Fatalf("seed active session %d: %v", i, err)
		}
	}
	for i := 0; i < 500; i++ {
		if _, err := tx.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix)
			VALUES('node-b', ?, 'sessao', 1, ?)`,
			fmt.Sprintf("sessao:org%04d/repo#feat/b", i), ended); err != nil {
			tx.Rollback()
			t.Fatalf("seed ended session %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	// 2200 retained rows: the new ingest and the new poll row are
	// admitted, and nothing of the mix is evicted or refused.
	if !mustIngest(t, s, recordBase(func(r *outbox.Record) {
		r.JobID = ptr("j-new")
		r.IdempotencyKey = "j-new:1"
		r.Dados = eventData(1, jobapi.TypeAccepted, "x", nil)
	})) {
		t.Fatal("new job at 2200 retained rows: applied = false, want admitted")
	}
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "wn", Label: "hm-agent-impl-wn", AgentStatus: "working"}))
	if n := tableCount(t, s, "jobs"); n != 2202 {
		t.Errorf("jobs rows = %d, want 2202 (nothing was evicted)", n)
	}
	// The one-day ended rows survived the ingest and the poll sweeps.
	for _, chave := range []string{"e0000", "sessao:org0000/repo#feat/b"} {
		if f := jobFact(t, s, "node-a", chave); !f.exists {
			if f = jobFact(t, s, "node-b", chave); !f.exists {
				t.Errorf("the ended row %s = gone, want retained (only age deletes)", chave)
			}
		}
	}
}

// dja275MovableClock returns an injected clock fixed at start and a
// setter that moves it by whole seconds.
func dja275MovableClock(start time.Time) (func() time.Time, func(int64)) {
	cur := start
	return func() time.Time { return cur },
		func(delta int64) { cur = cur.Add(time.Duration(delta) * time.Second) }
}

// dja275LinesWithHighCount filters the captured log lines to the
// high-count warns.
func dja275LinesWithHighCount(t *testing.T, lines *[]string) []string {
	t.Helper()
	var out []string
	for _, l := range *lines {
		if strings.Contains(l, "high count") {
			out = append(out, l)
		}
	}
	return out
}
