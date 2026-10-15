package a2astore

import (
	"context"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// workingTask sets up a working task with one admitted delivery and
// returns the store's clock instant as the record time.
func workingTask(t *testing.T, s *Store, taskID, session string) {
	t.Helper()
	if _, err := s.Admit(context.Background(), admission("alice", taskID+"-m", taskID, "cx-"+taskID, "acme/widgets", session, 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit %s: %v", taskID, err)
	}
	if _, err := s.Create(context.Background(), mkTask(taskID, "cx-"+taskID, a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create %s: %v", taskID, err)
	}
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask(taskID, "cx-"+taskID, a2a.TaskStateWorking, 2), PrevVersion: 1}); err != nil {
		t.Fatalf("Update %s: %v", taskID, err)
	}
}

// TestRecordDecision proves the record rule: validation, idempotency by
// id, the conflict, the unknown task, the terminal task, and the record
// order.
func TestRecordDecision(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	workingTask(t, s, "t1", "s1")
	ctx := context.Background()
	base := a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "the answer", Actor: "local"}
	stored, refusal, err := s.RecordDecision(ctx, base, clk.Now())
	if err != nil || refusal != nil {
		t.Fatalf("RecordDecision: %v, %v", refusal, err)
	}
	if stored.State != a2acore.DecisionPending || stored.RecordedAt != clk.ts() {
		t.Fatalf("stored = %+v, want pending at %q", stored, clk.ts())
	}
	// Idempotent: the same id, task, kind and text returns the stored one.
	again, refusal, err := s.RecordDecision(ctx, base, clk.Now())
	if err != nil || refusal != nil || again != stored {
		t.Fatalf("repeat = %+v, %v, %v, want the stored decision", again, refusal, err)
	}
	// Conflict: the same id with other content.
	conflict := base
	conflict.Text = "other content"
	if _, refusal, err := s.RecordDecision(ctx, conflict, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseDecisionConflict {
		t.Fatalf("conflict: %v, %v, want decision_conflict", refusal, err)
	}
	// The original is untouched.
	kept, err := s.PendingDecisions(ctx, 10)
	if err != nil || len(kept) != 1 || kept[0].Text != "the answer" {
		t.Fatalf("after conflict = %+v, %v, want the original pending", kept, err)
	}
	// Invalid shapes are refused before any row.
	bad := []a2acore.Decision{
		{ID: "bad id!", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "x", Actor: "a"},
		{ID: "d-bad", TaskID: "t1", Kind: "explode", Text: "x", Actor: "a"},
		{ID: "d-bad", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "  ", Actor: "a"},
		{ID: "d-bad", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "x", Actor: ""},
		{ID: "d-bad", TaskID: "no/such", Kind: a2acore.DecisionAnswer, Text: "x", Actor: "a"},
	}
	for i, d := range bad {
		if _, refusal, err := s.RecordDecision(ctx, d, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseInvalidDecision {
			t.Fatalf("invalid %d: %v, %v, want invalid_decision", i, refusal, err)
		}
	}
	// Unknown task and terminal task.
	if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: "d-x", TaskID: "nope", Kind: a2acore.DecisionAnswer, Text: "x", Actor: "a"}, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseUnknownTask {
		t.Fatalf("unknown task: %v, %v, want unknown_task", refusal, err)
	}
	workingTask(t, s, "t9", "s9")
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t9", "cx-t9", a2a.TaskStateCompleted, 3), PrevVersion: 2}); err != nil {
		t.Fatalf("complete t9: %v", err)
	}
	if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: "d-t", TaskID: "t9", Kind: a2acore.DecisionAnswer, Text: "x", Actor: "a"}, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseTerminal {
		t.Fatalf("terminal task: %v, %v, want terminal", refusal, err)
	}
	// The record order.
	for _, id := range []string{"d-2", "d-3"} {
		clk.Add(time.Second)
		if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: id, TaskID: "t1", Kind: a2acore.DecisionAsk, Text: "a question", Actor: "local"}, clk.Now()); err != nil || refusal != nil {
			t.Fatalf("RecordDecision %s: %v, %v", id, refusal, err)
		}
	}
	list, err := s.PendingDecisions(ctx, 10)
	if err != nil {
		t.Fatalf("PendingDecisions: %v", err)
	}
	if len(list) != 3 || list[0].ID != "d-1" || list[1].ID != "d-2" || list[2].ID != "d-3" {
		t.Fatalf("PendingDecisions = %+v, want d-1, d-2, d-3 in record order", list)
	}
	// A limit caps the list.
	capped, err := s.PendingDecisions(ctx, 2)
	if err != nil || len(capped) != 2 || capped[0].ID != "d-1" || capped[1].ID != "d-2" {
		t.Fatalf("PendingDecisions(2) = %+v, %v", capped, err)
	}
	none, err := s.PendingDecisions(ctx, 0)
	if err != nil || len(none) != 0 {
		t.Fatalf("PendingDecisions(0) = %+v, %v, want none", none, err)
	}
}

// TestRefuseDecision proves RefuseDecision marks a pending decision
// refused with the code, and is a no-op when the decision is not
// pending.
func TestRefuseDecision(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	workingTask(t, s, "t1", "s1")
	ctx := context.Background()
	if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAsk, Text: "a question", Actor: "local"}, clk.Now()); err != nil || refusal != nil {
		t.Fatalf("RecordDecision: %v, %v", refusal, err)
	}
	if err := s.RefuseDecision(ctx, "d-1", a2acore.RefuseInvalidDecision, clk.Now()); err != nil {
		t.Fatalf("RefuseDecision: %v", err)
	}
	var state, refusal string
	if err := s.db.QueryRow(`SELECT state, COALESCE(refusal, '') FROM a2a_decisions WHERE decision_id = 'd-1'`).Scan(&state, &refusal); err != nil {
		t.Fatalf("row: %v", err)
	}
	if state != a2acore.DecisionRefused || refusal != a2acore.RefuseInvalidDecision {
		t.Fatalf("row = state %q refusal %q, want refused invalid_decision", state, refusal)
	}
	if list, err := s.PendingDecisions(ctx, 10); err != nil || len(list) != 0 {
		t.Fatalf("PendingDecisions = %+v, %v, want none", list, err)
	}
	// A refused decision stays refused.
	if err := s.RefuseDecision(ctx, "d-1", a2acore.RefuseTerminal, clk.Now()); err != nil {
		t.Fatalf("RefuseDecision twice: %v", err)
	}
	if err := s.db.QueryRow(`SELECT COALESCE(refusal, '') FROM a2a_decisions WHERE decision_id = 'd-1'`).Scan(&refusal); err != nil {
		t.Fatalf("row: %v", err)
	}
	if refusal != a2acore.RefuseInvalidDecision {
		t.Fatalf("refusal after a second refuse = %q, want the first code", refusal)
	}
	// A missing decision is a no-op.
	if err := s.RefuseDecision(ctx, "d-none", a2acore.RefuseTerminal, clk.Now()); err != nil {
		t.Fatalf("RefuseDecision missing: %v", err)
	}
}

// TestApplyDecisionEvents proves the out-of-band application: the
// answer events produce the artifact and the completed state, the
// decision is marked applied with the terminal side effects, a second
// call is idempotent, and the refused states (not working, terminal,
// unknown task, already refused) are committed and returned.
func TestApplyDecisionEvents(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	workingTask(t, s, "t1", "s1")
	ctx := context.Background()
	if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "the answer", Actor: "local"}, clk.Now()); err != nil || refusal != nil {
		t.Fatalf("RecordDecision: %v, %v", refusal, err)
	}
	events := []a2a.Event{
		&a2a.TaskArtifactUpdateEvent{
			TaskID:    "t1",
			ContextID: "cx-t1",
			LastChunk: true,
			Artifact: &a2a.Artifact{
				ID:    "answer-d-1",
				Name:  "answer",
				Parts: a2a.ContentParts{{Content: a2a.Text("the answer"), MediaType: "text/plain"}},
			},
		},
		&a2a.TaskStatusUpdateEvent{
			TaskID:    "t1",
			ContextID: "cx-t1",
			Status:    a2a.TaskStatus{State: a2a.TaskStateCompleted, Timestamp: &clk.t},
			Metadata:  map[string]any{a2acore.MetaDecision: "d-1"},
		},
	}
	task, refusal, err := s.ApplyDecisionEvents(ctx, "d-1", events, clk.Now())
	if err != nil || refusal != nil {
		t.Fatalf("ApplyDecisionEvents: %v, %v", refusal, err)
	}
	if task.Status.State != a2a.TaskStateCompleted || len(task.Artifacts) != 1 || string(task.Artifacts[0].ID) != "answer-d-1" {
		t.Fatalf("applied task = %+v, want completed with the answer artifact", task)
	}
	got, err := s.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != 3 || got.Task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("stored after apply = version %d state %q, want 3 completed (create 1, working 2, apply 3)", got.Version, got.Task.Status.State)
	}
	var state, applied string
	if err := s.db.QueryRow(`SELECT state, COALESCE(applied, '') FROM a2a_decisions WHERE decision_id = 'd-1'`).Scan(&state, &applied); err != nil {
		t.Fatalf("decision: %v", err)
	}
	if state != a2acore.DecisionApplied || applied != clk.ts() {
		t.Fatalf("decision = state %q applied %q, want applied at %q", state, applied, clk.ts())
	}
	if d := deliveryStates(t, s, "t1"); d["owner"] != a2acore.DeliveryObsolete {
		t.Fatalf("deliveries after the terminal apply = %v, want obsolete", d)
	}
	// A second call is idempotent: the task back, no refusal, no version bump.
	again, refusal, err := s.ApplyDecisionEvents(ctx, "d-1", events, clk.Now())
	if err != nil || refusal != nil {
		t.Fatalf("second ApplyDecisionEvents: %v, %v", refusal, err)
	}
	if again.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("second apply task = %+v, want the stored task", again)
	}
	got, err = s.Get(context.Background(), "t1")
	if err != nil || got.Version != 3 {
		t.Fatalf("version after the second apply = %d, %v, want 3 (idempotent: no bump)", got.Version, err)
	}
}

// TestApplyDecisionEventsRefusals proves the committed refusals: a
// non-working task is not_awaiting_owner, a terminal one terminal, a
// missing task unknown_task, and an already refused decision its code.
func TestApplyDecisionEventsRefusals(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	ctx := context.Background()
	// Not working: input required.
	workingTask(t, s, "t1", "s1")
	if _, err := s.Update(ctx, &taskstore.UpdateRequest{Task: mkTask("t1", "cx-t1", a2a.TaskStateInputRequired, 2), PrevVersion: 2}); err != nil {
		t.Fatalf("input required: %v", err)
	}
	record := func(id, taskID string) {
		t.Helper()
		if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: id, TaskID: taskID, Kind: a2acore.DecisionAsk, Text: "a question", Actor: "local"}, clk.Now()); err != nil || refusal != nil {
			t.Fatalf("RecordDecision %s: %v, %v", id, refusal, err)
		}
	}
	record("d-1", "t1")
	events := []a2a.Event{&a2a.TaskStatusUpdateEvent{TaskID: "t1", ContextID: "cx-t1", Status: a2a.TaskStatus{State: a2a.TaskStateWorking, Timestamp: &clk.t}}}
	if _, refusal, err := s.ApplyDecisionEvents(ctx, "d-1", events, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseNotAwaitingOwner {
		t.Fatalf("not working: %v, %v, want not_awaiting_owner", refusal, err)
	}
	decState := decisionState(t, s, "d-1")
	if decState != a2acore.DecisionRefused || decisionRefusal(t, s, "d-1") != a2acore.RefuseNotAwaitingOwner {
		t.Fatalf("d-1 = %s / %s, want refused not_awaiting_owner", decState, decisionRefusal(t, s, "d-1"))
	}
	// The task is unchanged.
	view, err := s.TaskView(ctx, "t1")
	if err != nil || view.State != a2acore.StateInputRequired {
		t.Fatalf("task after the refusal = %+v, %v, want still input_required", view, err)
	}
	// Already refused: the apply returns the stored code.
	if _, refusal, err := s.ApplyDecisionEvents(ctx, "d-1", events, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseNotAwaitingOwner {
		t.Fatalf("already refused: %v, %v, want the stored code", refusal, err)
	}
	// Terminal.
	workingTask(t, s, "t2", "s2")
	record("d-2", "t2")
	if _, err := s.Update(ctx, &taskstore.UpdateRequest{Task: mkTask("t2", "cx-t2", a2a.TaskStateCompleted, 3), PrevVersion: 2}); err != nil {
		t.Fatalf("complete t2: %v", err)
	}
	if _, refusal, err := s.ApplyDecisionEvents(ctx, "d-2", events, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseTerminal {
		t.Fatalf("terminal: %v, %v, want terminal", refusal, err)
	}
	if decState := decisionState(t, s, "d-2"); decState != a2acore.DecisionRefused {
		t.Fatalf("d-2 = %s, want refused", decState)
	}
	// Unknown task: the task row is gone.
	workingTask(t, s, "t3", "s3")
	record("d-3", "t3")
	if _, err := s.db.Exec(`DELETE FROM a2a_tasks WHERE task_id = 't3'`); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, refusal, err := s.ApplyDecisionEvents(ctx, "d-3", events, clk.Now()); err != nil || refuseCode(refusal) != a2acore.RefuseUnknownTask {
		t.Fatalf("unknown task: %v, %v, want unknown_task", refusal, err)
	}
	if decState := decisionState(t, s, "d-3"); decState != a2acore.DecisionRefused {
		t.Fatalf("d-3 = %s, want refused", decState)
	}
	// A missing decision is an error, not a refusal.
	if _, refusal, err := s.ApplyDecisionEvents(ctx, "d-none", events, clk.Now()); err == nil || refusal != nil {
		t.Fatalf("missing decision: %v, %v, want an error", refusal, err)
	}
}

// decisionState reads the stored state of one decision.
func decisionState(t *testing.T, s *Store, id string) string {
	t.Helper()
	var state string
	if err := s.db.QueryRow(`SELECT state FROM a2a_decisions WHERE decision_id = ?`, id).Scan(&state); err != nil {
		t.Fatalf("decision %s: %v", id, err)
	}
	return state
}

// decisionRefusal reads the stored refusal code of one decision.
func decisionRefusal(t *testing.T, s *Store, id string) string {
	t.Helper()
	var code string
	if err := s.db.QueryRow(`SELECT COALESCE(refusal, '') FROM a2a_decisions WHERE decision_id = ?`, id).Scan(&code); err != nil {
		t.Fatalf("decision %s: %v", id, err)
	}
	return code
}
