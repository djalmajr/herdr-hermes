package cli

// The DJA-351 reply-actor tests: inside Herdr (HERDR_PANE_ID set) the
// reply records the pane reference as the decision actor and refuses a
// pane that is not a recipient of the task; outside Herdr the behaviour
// is unchanged (actor "local", no recipient check). The tests run
// cmdA2A directly over a pre-seeded registry database with an injected
// Getenv; no real herdr-soho and no serve are involved.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/outbox"
)

// a2aReplyActorFixture seeds a working task with one planned owner
// delivery to ownerRef in the registry db of cfgDir and returns the
// task id and the db path. ownerRef "" leaves the task with no planned
// owner delivery.
func a2aReplyActorFixture(t *testing.T, cfgDir, ownerRef string) (string, string) {
	t.Helper()
	db := filepath.Join(outbox.StateDir(cfgDir), "registry.db")
	s, err := a2astore.OpenFile(db, a2astore.Options{
		Now:   func() time.Time { return serveFixedNow },
		Owner: func(context.Context) (string, error) { return "local", nil },
	})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = s.Close() }()
	const taskID = "task-actor"
	var plan []a2acore.PlannedDelivery
	if ownerRef != "" {
		plan = append(plan, a2acore.PlannedDelivery{Role: a2acore.RoleOwner, Ref: ownerRef, State: a2acore.DeliveryHeld})
	}
	res, err := s.Admit(context.Background(), a2acore.Admission{
		Envelope: a2acore.Envelope{
			Subject: "local", MessageID: "m-actor", TaskID: taskID, ContextID: "ctx-actor",
			Project: "org/example", Session: "s-actor", Epoch: 1, Seq: 1, TextBytes: 16,
		},
		TaskID: taskID, ContextID: "ctx-actor", NewTask: true,
		Deliveries: plan, ReceivedAt: serveFixedNow,
	})
	if err != nil || res.Refusal != nil {
		t.Fatalf("Admit = %+v, %v", res, err)
	}
	if _, err := s.Create(context.Background(), &a2a.Task{
		ID: a2a.TaskID(taskID), ContextID: "ctx-actor",
		Status: a2a.TaskStatus{State: a2a.TaskStateWorking},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	return taskID, db
}

// a2aReplyActorRegisterOwner registers the notify owner of project in the
// state dir of cfgDir; the reply's route source re-reads it on every call.
func a2aReplyActorRegisterOwner(t *testing.T, cfgDir, project, owner string) {
	t.Helper()
	store, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return serveFixedNow }})
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	nstore := notify.Open(store, func() time.Time { return serveFixedNow })
	if err := nstore.UpdateRegistry(func(reg *notify.Registry) error {
		reg.Owners[project] = owner
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
}

// a2aReplyActorRun invokes `a2a reply` with HERDR_PANE_ID set to paneID
// ("" for the operator outside Herdr) and returns stdout, stderr and the
// exit code. It delegates to a2aReplyActorRunNamed with the pane-identity
// resolver unset; the pane identity is never needed by the reference
// cases.
func a2aReplyActorRun(t *testing.T, cfgDir, paneID string, args ...string) (string, string, int) {
	t.Helper()
	return a2aReplyActorRunNamed(t, cfgDir, paneID, nil, args...)
}

// a2aReplyActorRunNamed is a2aReplyActorRun with the pane-identity
// resolver injected: paneAgentName answers the agent name of the pane
// (fake.name) or fails (fake.err) and counts its calls; nil leaves it
// unset.
func a2aReplyActorRunNamed(t *testing.T, cfgDir, paneID string, paneAgentName func(context.Context, string) (string, error), args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:         strings.NewReader(""),
		Stdout:        &out,
		Stderr:        &errb,
		ConfigDir:     cfgDir,
		Now:           func() time.Time { return serveFixedNow },
		Sleep:         sleepCtx,
		Environ:       func() []string { return fakeChildEnv },
		PaneAgentName: paneAgentName,
	}
	env.Getenv = func(k string) string {
		if k == "HERDR_PANE_ID" {
			return paneID
		}
		return ""
	}
	exit := cmdA2A(append([]string{"reply"}, args...), env)
	return out.String(), errb.String(), exit
}

// a2aReplyActorAgentFake is the injected Env.PaneAgentName: it counts
// the resolutions and answers fake.name or fails with fake.err, keeping
// the resolved pane of each call.
type a2aReplyActorAgentFake struct {
	name  string
	err   error
	calls int
	panes []string
}

func (f *a2aReplyActorAgentFake) call(ctx context.Context, pane string) (string, error) {
	f.calls++
	f.panes = append(f.panes, pane)
	return f.name, f.err
}

// a2aReplyActorDecisions counts the stored decisions of task in db.
func a2aReplyActorDecisions(t *testing.T, db, task string) int {
	t.Helper()
	s, err := a2astore.OpenFile(db, a2astore.Options{Now: func() time.Time { return serveFixedNow }})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = s.Close() }()
	_, _, decisions, err := s.Show(context.Background(), task)
	if err != nil {
		t.Fatalf("Show: %v", err)
	}
	return len(decisions)
}

// TestA2AReplyActor proves the pane-recipient gate of the reply: outside
// Herdr the actor is "local" and no check runs; inside Herdr the pane
// reference is recorded as the actor and a pane outside the task's
// recipients (the planned deliveries plus the current routing) is
// refused with not_a_recipient, writing nothing; a duplicate resend is
// answered before the gate. A recipient registered by the agent name is
// compared with the agent name of the pane that answers (the resolver is
// called only when the pane reference is not a recipient and a recipient
// carries the agent-name pattern): the actor stays the pane reference,
// a pane whose agent has another name is refused and a failed resolution
// exits 4 without writing anything.
func TestA2AReplyActor(t *testing.T) {
	// (a) No HERDR_PANE_ID: records, and the actor is "local".
	t.Run("no pane id records the local actor", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "local/w6Y:p1")
		stdout, _, exit := a2aReplyActorRun(t, cfgDir, "", "--task", task, "--answer", "--decision-id", "dec-actor-a", "--db", db, "text-a")
		if exit != 0 {
			t.Fatalf("reply exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		line := a2aOneJSONLine(t, "reply (a)", stdout)
		var body struct {
			Status   string            `json:"status"`
			Decision *a2acore.Decision `json:"decision"`
		}
		if err := json.Unmarshal([]byte(line), &body); err != nil || body.Status != "recorded" || body.Decision == nil {
			t.Fatalf("reply (a) = %q, want recorded with a decision", line)
		}
		if body.Decision.Actor != "local" {
			t.Fatalf("actor = %q, want local", body.Decision.Actor)
		}
	})

	// (b) HERDR_PANE_ID of the planned owner: records, and the actor is
	// "local/<pane>".
	t.Run("planned owner pane records its reference", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "local/w6Y:p1")
		stdout, _, exit := a2aReplyActorRun(t, cfgDir, "w6Y:p1", "--task", task, "--answer", "--decision-id", "dec-actor-b", "--db", db, "text-b")
		if exit != 0 {
			t.Fatalf("reply exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		line := a2aOneJSONLine(t, "reply (b)", stdout)
		var body struct {
			Status   string            `json:"status"`
			Decision *a2acore.Decision `json:"decision"`
		}
		if err := json.Unmarshal([]byte(line), &body); err != nil || body.Status != "recorded" || body.Decision == nil {
			t.Fatalf("reply (b) = %q, want recorded with a decision", line)
		}
		if body.Decision.Actor != "local/w6Y:p1" {
			t.Fatalf("actor = %q, want local/w6Y:p1", body.Decision.Actor)
		}
	})

	// (c) HERDR_PANE_ID of another pane: exit 2, not_a_recipient, and no
	// decision in the store.
	t.Run("a pane outside the recipients is refused", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "local/w6Y:p1")
		stdout, _, exit := a2aReplyActorRun(t, cfgDir, "w6Z:p2", "--task", task, "--answer", "--decision-id", "dec-actor-c", "--db", db, "text-c")
		if exit != 2 {
			t.Fatalf("reply exit = %d, want 2 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, `"status":"refused"`) || !strings.Contains(stdout, `"motivo":"not_a_recipient"`) {
			t.Fatalf("reply (c) = %q, want refused not_a_recipient", stdout)
		}
		if n := a2aReplyActorDecisions(t, db, task); n != 0 {
			t.Fatalf("decisions in the store = %d, want 0 (nothing recorded)", n)
		}
	})

	// (d) The registered owner changed after the admission and
	// HERDR_PANE_ID is the new owner: records, through the current
	// routing (set b).
	t.Run("the current routing owner may answer", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "local/old:owner")
		a2aReplyActorRegisterOwner(t, cfgDir, "org/example", "local/new:owner")
		stdout, _, exit := a2aReplyActorRun(t, cfgDir, "new:owner", "--task", task, "--answer", "--decision-id", "dec-actor-d", "--db", db, "text-d")
		if exit != 0 {
			t.Fatalf("reply exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		line := a2aOneJSONLine(t, "reply (d)", stdout)
		var body struct {
			Status   string            `json:"status"`
			Decision *a2acore.Decision `json:"decision"`
		}
		if err := json.Unmarshal([]byte(line), &body); err != nil || body.Status != "recorded" || body.Decision == nil {
			t.Fatalf("reply (d) = %q, want recorded with a decision", line)
		}
		if body.Decision.Actor != "local/new:owner" {
			t.Fatalf("actor = %q, want local/new:owner", body.Decision.Actor)
		}
	})

	// (e) A value that already carries the local/ prefix is accepted the
	// same.
	t.Run("a prefixed pane value is accepted", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "local/w6Y:p1")
		stdout, _, exit := a2aReplyActorRun(t, cfgDir, "local/w6Y:p1", "--task", task, "--answer", "--decision-id", "dec-actor-e", "--db", db, "text-e")
		if exit != 0 {
			t.Fatalf("reply exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		line := a2aOneJSONLine(t, "reply (e)", stdout)
		var body struct {
			Status   string            `json:"status"`
			Decision *a2acore.Decision `json:"decision"`
		}
		if err := json.Unmarshal([]byte(line), &body); err != nil || body.Status != "recorded" || body.Decision == nil {
			t.Fatalf("reply (e) = %q, want recorded with a decision", line)
		}
		if body.Decision.Actor != "local/w6Y:p1" {
			t.Fatalf("actor = %q, want local/w6Y:p1", body.Decision.Actor)
		}
	})

	// (f) A duplicate resend, the same id and the same content, from
	// another pane: still duplicate (answered before the gate).
	t.Run("a duplicate resend from another pane is duplicate", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "local/w6Y:p1")
		stdout, _, exit := a2aReplyActorRun(t, cfgDir, "w6Y:p1", "--task", task, "--answer", "--decision-id", "dec-actor-f", "--db", db, "text-f")
		if exit != 0 {
			t.Fatalf("first reply exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		stdout, _, exit = a2aReplyActorRun(t, cfgDir, "w6Z:p2", "--task", task, "--answer", "--decision-id", "dec-actor-f", "--db", db, "text-f")
		if exit != 0 {
			t.Fatalf("resend exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		line := a2aOneJSONLine(t, "reply (f) resend", stdout)
		if !strings.Contains(line, `"status":"duplicate"`) {
			t.Fatalf("reply (f) resend = %q, want duplicate", line)
		}
	})

	// (g) The owner is registered by the agent name (the planned
	// delivery stores the name) and the agent of the pane answers with
	// that name: records, and the actor is the pane reference, never the
	// name.
	t.Run("a pane whose agent has the registered name may answer", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "owner")
		fake := &a2aReplyActorAgentFake{name: "owner"}
		stdout, _, exit := a2aReplyActorRunNamed(t, cfgDir, "w6Y:p1", fake.call, "--task", task, "--answer", "--decision-id", "dec-actor-g", "--db", db, "text-g")
		if exit != 0 {
			t.Fatalf("reply exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		line := a2aOneJSONLine(t, "reply (g)", stdout)
		var body struct {
			Status   string            `json:"status"`
			Decision *a2acore.Decision `json:"decision"`
		}
		if err := json.Unmarshal([]byte(line), &body); err != nil || body.Status != "recorded" || body.Decision == nil {
			t.Fatalf("reply (g) = %q, want recorded with a decision", line)
		}
		if body.Decision.Actor != "local/w6Y:p1" {
			t.Fatalf("actor = %q, want local/w6Y:p1 (the reference, not the name)", body.Decision.Actor)
		}
		if fake.calls != 1 {
			t.Fatalf("pane identity resolutions = %d, want 1", fake.calls)
		}
		if len(fake.panes) != 1 || fake.panes[0] != "w6Y:p1" {
			t.Fatalf("resolved panes = %v, want [w6Y:p1]", fake.panes)
		}
	})

	// (h) The owner is registered by the agent name and the agent of the
	// pane has another name: refused with not_a_recipient, nothing
	// recorded.
	t.Run("a pane whose agent has another name is refused", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "owner")
		fake := &a2aReplyActorAgentFake{name: "someone-else"}
		stdout, _, exit := a2aReplyActorRunNamed(t, cfgDir, "w6Y:p1", fake.call, "--task", task, "--answer", "--decision-id", "dec-actor-h", "--db", db, "text-h")
		if exit != 2 {
			t.Fatalf("reply exit = %d, want 2 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, `"status":"refused"`) || !strings.Contains(stdout, `"motivo":"not_a_recipient"`) {
			t.Fatalf("reply (h) = %q, want refused not_a_recipient", stdout)
		}
		if n := a2aReplyActorDecisions(t, db, task); n != 0 {
			t.Fatalf("decisions in the store = %d, want 0 (nothing recorded)", n)
		}
	})

	// (i) The owner is registered by the agent name and the pane-identity
	// resolution fails: exit 4, pane identity unavailable, nothing
	// recorded.
	t.Run("a failed resolution is unavailable and writes nothing", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "owner")
		fake := &a2aReplyActorAgentFake{err: errors.New("herdr agent get: exited without the agent answer: exit status 2")}
		stdout, _, exit := a2aReplyActorRunNamed(t, cfgDir, "w6Y:p1", fake.call, "--task", task, "--answer", "--decision-id", "dec-actor-i", "--db", db, "text-i")
		if exit != 4 {
			t.Fatalf("reply exit = %d, want 4 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, `"status":"unavailable"`) || !strings.Contains(stdout, `"motivo":"a2a reply: pane identity unavailable"`) {
			t.Fatalf("reply (i) = %q, want unavailable pane identity unavailable", stdout)
		}
		if n := a2aReplyActorDecisions(t, db, task); n != 0 {
			t.Fatalf("decisions in the store = %d, want 0 (nothing recorded)", n)
		}
	})

	// (j) The owner is registered by the reference and the pane matches
	// by the reference: the pane identity is never resolved.
	t.Run("a pane that matches by the reference never resolves the name", func(t *testing.T) {
		cfgDir := t.TempDir()
		task, db := a2aReplyActorFixture(t, cfgDir, "local/w6Y:p1")
		fake := &a2aReplyActorAgentFake{name: "owner"}
		stdout, _, exit := a2aReplyActorRunNamed(t, cfgDir, "w6Y:p1", fake.call, "--task", task, "--answer", "--decision-id", "dec-actor-j", "--db", db, "text-j")
		if exit != 0 {
			t.Fatalf("reply exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		line := a2aOneJSONLine(t, "reply (j)", stdout)
		if !strings.Contains(line, `"status":"recorded"`) {
			t.Fatalf("reply (j) = %q, want recorded", line)
		}
		if fake.calls != 0 {
			t.Fatalf("pane identity resolutions = %d, want 0 (the reference matched)", fake.calls)
		}
	})
}
