package registry

import (
	"database/sql"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// okPoll builds an OK PollResult at the fixed clock.
func okPoll(maquina, herdrVer string, ws ...PolledWorkspace) PollResult {
	return PollResult{Maquina: maquina, At: fixedNow, OK: true, HerdrVersion: herdrVer, Workspaces: ws}
}

func mustApplyPoll(t *testing.T, s *Store, p PollResult) {
	t.Helper()
	if err := s.ApplyPoll(p); err != nil {
		t.Fatalf("ApplyPoll(%s): %v", p.Maquina, err)
	}
}

func waitFact(t *testing.T, s *Store, maquina, pane string) (existe bool, desde, agente string) {
	t.Helper()
	var desdeS string
	var agenteS sql.NullString
	err := s.db.QueryRow(`SELECT desde, agente FROM waits WHERE maquina = ? AND pane = ?`, maquina, pane).
		Scan(&desdeS, &agenteS)
	if err != nil {
		return false, "", ""
	}
	agente = ""
	if agenteS.Valid {
		agente = agenteS.String
	}
	return true, desdeS, agente
}

// movableClock returns a clock starting at the fixed instant and a setter
// that moves it by seconds.
func movableClock() (func() time.Time, func(int64)) {
	cur := fixedNow
	return func() time.Time { return cur },
		func(d int64) { cur = fixedNow.Add(time.Duration(d) * time.Second) }
}

func clockStore(t *testing.T, now func() time.Time) *Store {
	t.Helper()
	s, err := Open(pathFor(t), Options{Now: now})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// TestApplyPollHmWorkspace: an hm workspace becomes a job with the owner
// parsed from the name.
// Mutation captured: a missing owner parse (or a wrong chave) fails the
// row assertions.
func TestApplyPollHmWorkspace(t *testing.T) {
	s := openStore(t)
	mustApplyPoll(t, s, okPoll("node-a", "1.2.3",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"}))
	f := jobFact(t, s, "node-a", "ws:w1")
	if !f.exists {
		t.Fatal("the hm workspace did not create a job row")
	}
	if f.agente == nil || *f.agente != "example" || f.trabalho == nil || *f.trabalho != "impl" || f.ref == nil || *f.ref != "x1" {
		t.Errorf("owner = %v/%v/%v, want example/impl/x1", f.agente, f.trabalho, f.ref)
	}
	if f.fonte == nil || *f.fonte != AgenteFonteNome {
		t.Errorf("agente_fonte = %v, want %s", f.fonte, AgenteFonteNome)
	}
	if f.workspace == nil || *f.workspace != "hm-example-impl-x1" {
		t.Errorf("workspace = %v, want the label", f.workspace)
	}
	if f.ultima == nil || *f.ultima != "2026-10-10T12:00:00-03:00" {
		t.Errorf("ultima_atividade = %v, want the poll At", f.ultima)
	}
}

// TestApplyPollUnresolvedHm: an hm label that does not resolve keeps the
// owner null.
// Mutation captured: a guess in the owner parse (or a stored default) gives
// the row an agent.
func TestApplyPollUnresolvedHm(t *testing.T) {
	s := openStore(t)
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w2", Label: "hm-pinar", AgentStatus: "idle"}))
	f := jobFact(t, s, "node-a", "ws:w2")
	if !f.exists {
		t.Fatal("the unresolved hm workspace did not create a job row")
	}
	if f.agente != nil || f.trabalho != nil || f.ref != nil || f.fonte != nil {
		t.Errorf("owner = %v/%v/%v (fonte %v), want all null", f.agente, f.trabalho, f.ref, f.fonte)
	}
}

// TestApplyPollJobWorkspaceMerge: a job-<id> workspace merges into the
// pushed job row.
// Mutation captured: a poll that creates a second row (or skips the merge)
// leaves the pushed row without the workspace fields.
func TestApplyPollJobWorkspaceMerge(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.JobID = ptr("job-42"); r.IdempotencyKey = "job-42:1" }))
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w3", Label: "job-job-42", AgentStatus: "done"}))
	if n := tableCount(t, s, "jobs"); n != 1 {
		t.Fatalf("job rows = %d, want the pushed row merged", n)
	}
	f := jobFact(t, s, "node-a", "job-42")
	if f.workspace == nil || *f.workspace != "job-job-42" {
		t.Errorf("workspace = %v, want the polled label", f.workspace)
	}
	if f.ultima == nil || *f.ultima != "2026-10-10T12:00:00-03:00" {
		t.Errorf("ultima_atividade = %v, want the poll At", f.ultima)
	}
}

// TestApplyPollWorkspaceGone: a workspace absent from the next OK poll
// deletes the workspace row and nulls the workspace fields of a pushed
// job.
// Mutation captured: a disappearance step that skips the pushed-job
// nulling (or deletes the pushed row) fails the assertions.
func TestApplyPollWorkspaceGone(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(func(r *outbox.Record) { r.JobID = ptr("job-42"); r.IdempotencyKey = "job-42:1" }))
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"},
		PolledWorkspace{ID: "w3", Label: "job-job-42", AgentStatus: "done"}))
	mustApplyPoll(t, s, okPoll("node-a", ""))
	if f := jobFact(t, s, "node-a", "ws:w1"); f.exists {
		t.Error("the gone workspace row was not deleted")
	}
	f := jobFact(t, s, "node-a", "job-42")
	if !f.exists {
		t.Fatal("the pushed job row was deleted, want it kept until cleanup")
	}
	if f.workspace != nil {
		t.Errorf("workspace = %v, want null after the workspace left the poll", f.workspace)
	}
}

// TestApplyPollFailedPoll: a failed poll marks the machine's jobs obsolete
// and leaves the waits as they are.
// Mutation captured: a failed poll that clears obsoleto (or the waits)
// fails the assertions.
func TestApplyPollFailedPoll(t *testing.T) {
	s := openStore(t)
	mustIngest(t, s, recordBase(nil))
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"}))
	p := okPoll("node-a", "", PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "blocked"})
	p.Panes = []PolledPane{{PaneID: "w1:p1", WorkspaceID: "w1", Name: "agent-a", AgentStatus: "blocked"}}
	mustApplyPoll(t, s, p)
	mustApplyPoll(t, s, PollResult{Maquina: "node-a", At: fixedNow, OK: false, Motivo: "probe_timeout"})
	if f := jobFact(t, s, "node-a", "job-1"); f.obsoleto != 1 {
		t.Errorf("pushed job obsoleto = %d, want 1 after the failed poll", f.obsoleto)
	}
	if f := jobFact(t, s, "node-a", "ws:w1"); f.obsoleto != 1 {
		t.Errorf("workspace job obsoleto = %d, want 1", f.obsoleto)
	}
	existe, desde, _ := waitFact(t, s, "node-a", "w1:p1")
	if !existe || desde != "2026-10-10T12:00:00-03:00" {
		t.Errorf("wait after the failed poll = (%v, %q), want kept as it was", existe, desde)
	}
	var status string
	if err := s.db.QueryRow(`SELECT poll_status FROM nodes WHERE maquina = 'node-a'`).Scan(&status); err != nil {
		t.Fatalf("poll_status: %v", err)
	}
	if status != "probe_timeout" {
		t.Errorf("poll_status = %q, want probe_timeout", status)
	}
}

// TestApplyPollWaits: blocked panes become waits; desde is kept for a pane
// that was already waiting and a new pane gets the current At; a pane that
// unblocks drops its wait.
// Mutation captured: a wait replace that always rewrites desde (or keeps a
// pane that unblocked) fails the assertions.
func TestApplyPollWaits(t *testing.T) {
	nowFn, set := movableClock()
	s := clockStore(t, nowFn)
	p1 := okPoll("node-a", "")
	p1.Panes = []PolledPane{{PaneID: "w1:p1", WorkspaceID: "w1", Name: "agent-a", AgentStatus: "blocked"}}
	mustApplyPoll(t, s, p1)
	set(3600)
	p2 := okPoll("node-a", "")
	p2.At = nowFn()
	p2.Panes = []PolledPane{
		{PaneID: "w1:p1", WorkspaceID: "w1", Name: "agent-a", AgentStatus: "blocked"},
		{PaneID: "w9:p1", WorkspaceID: "w9", Name: "agent-b", AgentStatus: "blocked"},
	}
	mustApplyPoll(t, s, p2)
	existe, desde, agente := waitFact(t, s, "node-a", "w1:p1")
	if !existe || desde != "2026-10-10T12:00:00-03:00" {
		t.Errorf("old pane wait = (%v, %q), want the original desde kept", existe, desde)
	}
	if agente != "agent-a" {
		t.Errorf("wait agente = %q, want the pane name", agente)
	}
	existe, desde, _ = waitFact(t, s, "node-a", "w9:p1")
	if !existe || desde != "2026-10-10T13:00:00-03:00" {
		t.Errorf("new pane wait = (%v, %q), want the current At", existe, desde)
	}
	p3 := okPoll("node-a", "")
	p3.At = nowFn()
	p3.Panes = []PolledPane{
		{PaneID: "w1:p1", WorkspaceID: "w1", Name: "agent-a", AgentStatus: "blocked"},
		{PaneID: "w9:p1", WorkspaceID: "w9", Name: "agent-b", AgentStatus: "working"},
	}
	mustApplyPoll(t, s, p3)
	if existe, _, _ := waitFact(t, s, "node-a", "w9:p1"); existe {
		t.Error("the unblocked pane keeps its wait, want it dropped")
	}
	if existe, _, _ := waitFact(t, s, "node-a", "w1:p1"); !existe {
		t.Error("the still-blocked pane lost its wait, want it kept")
	}
}

// TestApplyPollClaimBinding: an unexpired claim binds its fields to a
// matching workspace row; the agent comes from the claim only when the row
// has no owner.
// Mutation captured: a binding that overwrites the parsed owner (or binds
// an expired claim) fails the assertions.
func TestApplyPollClaimBinding(t *testing.T) {
	s := openStore(t)
	res, err := s.Check(CheckRequest{Agente: "example", Maquina: "node-a", Card: "example-229", Workspace: "hm-example-impl-x1"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Status != CheckOK {
		t.Fatalf("check status = %q, want ok", res.Status)
	}
	// Resolved row: the fields bind, the parsed owner stays.
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w1", Label: "hm-example-impl-x1", AgentStatus: "working"}))
	f := jobFact(t, s, "node-a", "ws:w1")
	if f.card == nil || *f.card != "EXAMPLE-229" {
		t.Errorf("bound card = %v, want EXAMPLE-229", f.card)
	}
	if f.agente == nil || *f.agente != "example" || f.fonte == nil || *f.fonte != AgenteFonteNome {
		t.Errorf("owner = %v (fonte %v), want the parsed name kept", f.agente, f.fonte)
	}
	// Unresolved row: the agent comes from the claim.
	res2, err := s.Check(CheckRequest{Agente: "other", Workspace: "hm-pinar"})
	if err != nil || res2.Status != CheckOK {
		t.Fatalf("second Check: %v %v", res2, err)
	}
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w2", Label: "hm-pinar", AgentStatus: "working"}))
	f = jobFact(t, s, "node-a", "ws:w2")
	if f.agente == nil || *f.agente != "other" || f.fonte == nil || *f.fonte != AgenteFonteClaim {
		t.Errorf("owner = %v (fonte %v), want the claim agent", f.agente, f.fonte)
	}
}

// TestApplyPollHerdrVersion: the version is set and never cleared by an
// empty value.
// Mutation captured: a version write without the non-empty guard clears it.
func TestApplyPollHerdrVersion(t *testing.T) {
	s := openStore(t)
	mustApplyPoll(t, s, okPoll("node-a", "1.2.3"))
	var v string
	if err := s.db.QueryRow(`SELECT herdr_version FROM nodes WHERE maquina = 'node-a'`).Scan(&v); err != nil {
		t.Fatalf("herdr_version: %v", err)
	}
	if v != "1.2.3" {
		t.Fatalf("herdr_version = %q, want 1.2.3", v)
	}
	mustApplyPoll(t, s, okPoll("node-a", ""))
	if err := s.db.QueryRow(`SELECT herdr_version FROM nodes WHERE maquina = 'node-a'`).Scan(&v); err != nil {
		t.Fatalf("herdr_version: %v", err)
	}
	if v != "1.2.3" {
		t.Errorf("herdr_version = %q after the empty poll, want 1.2.3 kept", v)
	}
}

// TestApplyPollPersonalWorkspace: a workspace without the hm-/job- prefix
// never becomes a job.
// Mutation captured: a looser label rule creates a row for it.
func TestApplyPollPersonalWorkspace(t *testing.T) {
	s := openStore(t)
	mustApplyPoll(t, s, okPoll("node-a", "",
		PolledWorkspace{ID: "w7", Label: "my-personal-space", AgentStatus: "working"}))
	if n := tableCount(t, s, "jobs"); n != 0 {
		t.Errorf("job rows = %d, want 0: a personal workspace is not a job", n)
	}
	if n := tableCount(t, s, "nodes"); n != 1 {
		t.Errorf("nodes = %d, want the node touched", n)
	}
}

// TestApplyPollSweep: an ended row past the default retention on the
// polled machine is swept at the start of an ApplyPoll; an ended row past
// the fence but inside the retention is kept.
// Mutation captured: a missing ApplyPoll sweep leaves the stale row.
func TestApplyPollSweep(t *testing.T) {
	s := openStore(t)
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix) VALUES('node-a','job-old','job',1,?)`,
		fixedNow.Unix()-int64((DefaultEndedRetention/time.Second))-1); err != nil {
		t.Fatalf("seed the stale ended row: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo) VALUES('node-a','job-old',1,'2026-01-01T00:00:00-03:00','note','x')`); err != nil {
		t.Fatalf("seed the stale event: %v", err)
	}
	if _, err := s.db.Exec(`INSERT INTO jobs(maquina, chave, origem, encerrado, encerrado_unix) VALUES('node-a','job-kept','job',1,?)`,
		fixedNow.Unix()-int64((8*24*time.Hour)/time.Second)); err != nil {
		t.Fatalf("seed the fence-expired ended row: %v", err)
	}
	mustApplyPoll(t, s, okPoll("node-a", ""))
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE chave = 'job-old'`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("stale ended rows left = %d, want 0", n)
	}
	if n := tableCount(t, s, "events"); n != 0 {
		t.Errorf("stale events left = %d, want 0", n)
	}
	var kept int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE chave = 'job-kept' AND encerrado = 1`).Scan(&kept); err != nil {
		t.Fatalf("count kept: %v", err)
	}
	if kept != 1 {
		t.Errorf("8-day ended rows left = %d, want 1 (kept until the retention)", kept)
	}
}

// TestApplyPollOwnerFromCurrentLabel: on every OK poll an hm row re-
// derives its owner from the current label; a resolved name sets it, an
// unparsable label clears the name-derived owner and the ref, and then
// the claim binding may fill a null agente (R3).
func TestApplyPollOwnerFromCurrentLabel(t *testing.T) {
	s := openStore(t)
	const chave = "ws:w7"
	mustApplyPoll(t, s, okPoll("node-a", "", PolledWorkspace{ID: "w7", Label: "hm-first-impl-ref1", AgentStatus: "working"}))
	f := jobFact(t, s, "node-a", chave)
	if f.agente == nil || *f.agente != "first" || f.trabalho == nil || *f.trabalho != "impl" || f.ref == nil || *f.ref != "ref1" {
		t.Fatalf("after poll 1: owner = (%v, %v, %v), want first/impl/ref1", f.agente, f.trabalho, f.ref)
	}
	// The label changes: the owner is re-derived from it.
	mustApplyPoll(t, s, okPoll("node-a", "", PolledWorkspace{ID: "w7", Label: "hm-second-review-ref2", AgentStatus: "working"}))
	f = jobFact(t, s, "node-a", chave)
	if f.agente == nil || *f.agente != "second" || f.trabalho == nil || *f.trabalho != "review" || f.ref == nil || *f.ref != "ref2" || f.fonte == nil || *f.fonte != AgenteFonteNome {
		t.Fatalf("after poll 2: owner = (%v, %v, %v, %v), want second/review/ref2 from the name", f.agente, f.trabalho, f.ref, f.fonte)
	}
	// A check for the ref (any type token) is a duplicate on the ref.
	res, err := s.Check(CheckRequest{Agente: "second", Workspace: "hm-second-impl-ref2"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Status != CheckDuplicate {
		t.Fatalf("check for the ref2 workspace = %q, want duplicate", res.Status)
	}
	// An unparsable label clears the name-derived owner and the ref.
	mustApplyPoll(t, s, okPoll("node-a", "", PolledWorkspace{ID: "w7", Label: "hm-first-implement-ref1", AgentStatus: "working"}))
	f = jobFact(t, s, "node-a", chave)
	if f.agente != nil || f.fonte != nil || f.trabalho != nil || f.ref != nil {
		t.Fatalf("after the unparsable label: owner = (%v, %v, %v, %v), want all null", f.agente, f.fonte, f.trabalho, f.ref)
	}
	// And the check for that ref is free again.
	res, err = s.Check(CheckRequest{Agente: "first", Workspace: "hm-first-impl-ref1"})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if res.Status != CheckOK {
		t.Fatalf("check after the owner was cleared = %q, want ok", res.Status)
	}
}
