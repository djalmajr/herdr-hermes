package a2astore

import (
	"context"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/notify"
)

// TestCreateReleasesHeldDeliveries proves the held to pending release:
// an admission's held deliveries wait for the task, and Create flips
// them to pending (due at once) in the same transaction.
func TestCreateReleasesHeldDeliveries(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1"), {Role: a2acore.RoleGuardian, Ref: "", State: a2acore.DeliveryNoRoute}})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	states := deliveryStates(t, s, "t1")
	if states["owner"] != a2acore.DeliveryHeld || states["guardian"] != a2acore.DeliveryNoRoute {
		t.Fatalf("after Admit = %v, want owner held and guardian no_route", states)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	states = deliveryStates(t, s, "t1")
	if states["owner"] != a2acore.DeliveryPending || states["guardian"] != a2acore.DeliveryNoRoute {
		t.Fatalf("after Create = %v, want owner pending and guardian no_route", states)
	}
	var nextUnix int64
	if err := s.db.QueryRow(`SELECT next_attempt_unix FROM a2a_deliveries WHERE role = 'owner'`).Scan(&nextUnix); err != nil {
		t.Fatalf("next_attempt_unix: %v", err)
	}
	if nextUnix != clk.t.Unix() {
		t.Fatalf("next_attempt_unix = %d, want %d (due at once)", nextUnix, clk.t.Unix())
	}
}

// TestUpdateMarksDecisionApplied proves the decision marking: a status
// update event whose metadata names a pending decision of the task
// marks it applied in the same transaction as the task update.
func TestUpdateMarksDecisionApplied(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 2), PrevVersion: 1}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	decision := a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "the answer", Actor: "local"}
	if _, refusal, err := s.RecordDecision(context.Background(), decision, clk.Now()); err != nil || refusal != nil {
		t.Fatalf("RecordDecision: %v, %v", refusal, err)
	}
	// An event that names the decision, applied on the update.
	ev := &a2a.TaskStatusUpdateEvent{
		TaskID:    "t1",
		ContextID: "c1",
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking, Timestamp: &clk.t},
		Metadata:  map[string]any{a2acore.MetaDecision: "d-1"},
	}
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 2), PrevVersion: 2, Event: ev}); err != nil {
		t.Fatalf("Update with decision event: %v", err)
	}
	var state, applied string
	if err := s.db.QueryRow(`SELECT state, COALESCE(applied, '') FROM a2a_decisions WHERE decision_id = 'd-1'`).Scan(&state, &applied); err != nil {
		t.Fatalf("decision row: %v", err)
	}
	if state != a2acore.DecisionApplied || applied != clk.ts() {
		t.Fatalf("decision = state %q applied %q, want applied at %q", state, applied, clk.ts())
	}
	// An event that names a decision of another task changes nothing.
	other := &a2a.TaskStatusUpdateEvent{
		TaskID:    "t1",
		ContextID: "c1",
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking, Timestamp: &clk.t},
		Metadata:  map[string]any{a2acore.MetaDecision: "d-none"},
	}
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 3), PrevVersion: 3, Event: other}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var states int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_decisions WHERE decision_id = 'd-none'`).Scan(&states); err != nil {
		t.Fatalf("count: %v", err)
	}
	if states != 0 {
		t.Fatalf("unknown decision id created a row")
	}
}

// TestUpdateTerminalSideEffects proves the terminal update: the task's
// held or pending deliveries become obsolete and its other pending
// decisions are refused with terminal, in the same transaction.
func TestUpdateTerminalSideEffects(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 2), PrevVersion: 1}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	for _, id := range []string{"d-1", "d-2"} {
		decision := a2acore.Decision{ID: id, TaskID: "t1", Kind: a2acore.DecisionAsk, Text: "a question", Actor: "local"}
		if _, refusal, err := s.RecordDecision(context.Background(), decision, clk.Now()); err != nil || refusal != nil {
			t.Fatalf("RecordDecision %s: %v, %v", id, refusal, err)
		}
	}
	// d-1 is already applied by the update; d-2 stays pending.
	ev := &a2a.TaskStatusUpdateEvent{
		TaskID:    "t1",
		ContextID: "c1",
		Status:    a2a.TaskStatus{State: a2a.TaskStateCompleted, Timestamp: &clk.t},
		Metadata:  map[string]any{a2acore.MetaDecision: "d-1"},
	}
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateCompleted, 2), PrevVersion: 2, Event: ev}); err != nil {
		t.Fatalf("terminal Update: %v", err)
	}
	states := deliveryStates(t, s, "t1")
	if states["owner"] != a2acore.DeliveryObsolete {
		t.Fatalf("delivery after terminal = %v, want obsolete", states)
	}
	var d1, d2, d1Refusal, d2Refusal string
	var d1Applied, d2Applied string
	if err := s.db.QueryRow(`SELECT state, COALESCE(refusal, ''), COALESCE(applied, '') FROM a2a_decisions WHERE decision_id = 'd-1'`).Scan(&d1, &d1Refusal, &d1Applied); err != nil {
		t.Fatalf("d-1: %v", err)
	}
	if err := s.db.QueryRow(`SELECT state, COALESCE(refusal, ''), COALESCE(applied, '') FROM a2a_decisions WHERE decision_id = 'd-2'`).Scan(&d2, &d2Refusal, &d2Applied); err != nil {
		t.Fatalf("d-2: %v", err)
	}
	if d1 != a2acore.DecisionApplied || d1Applied == "" || d1Refusal != "" {
		t.Fatalf("d-1 = state %q refusal %q applied %q, want applied with no refusal", d1, d1Refusal, d1Applied)
	}
	if d2 != a2acore.DecisionRefused || d2Refusal != a2acore.RefuseTerminal || d2Applied != "" {
		t.Fatalf("d-2 = state %q refusal %q applied %q, want refused terminal", d2, d2Refusal, d2Applied)
	}
	// The task is terminal: a new decision is refused.
	decision := a2acore.Decision{ID: "d-3", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "late", Actor: "local"}
	if _, refusal, err := s.RecordDecision(context.Background(), decision, clk.Now()); err != nil || refusal == nil || refusal.Code != a2acore.RefuseTerminal {
		t.Fatalf("RecordDecision on a terminal task: %v, %v, want terminal", refusal, err)
	}
}

// deliveryStates reads the stored state of the deliveries of one task,
// keyed by role.
func deliveryStates(t *testing.T, s *Store, taskID string) map[string]string {
	t.Helper()
	rows, err := s.db.Query(`SELECT role, state FROM a2a_deliveries WHERE task_id = ?`, taskID)
	if err != nil {
		t.Fatalf("deliveries: %v", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var role, state string
		if err := rows.Scan(&role, &state); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[role] = state
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("no deliveries of %s", taskID)
	}
	return out
}

// TestCompleteAfterTerminalTask proves a delivery whose task became
// terminal while it was in flight still records its real outcome.
func TestCompleteAfterTerminalTask(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	if _, err := s.Admit(context.Background(), admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 2), PrevVersion: 1}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	claims, err := s.ClaimDue(context.Background(), clk.t, 10, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue: %v, %v", claims, err)
	}
	// The task goes terminal while the delivery is in flight.
	if _, err := s.Update(context.Background(), &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateCompleted, 2), PrevVersion: 2}); err != nil {
		t.Fatalf("terminal Update: %v", err)
	}
	clk.Add(time.Minute)
	if err := s.Complete(context.Background(), claims[0], notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}, clk.Now()); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	var state string
	if err := s.db.QueryRow(`SELECT state FROM a2a_deliveries WHERE delivery_id = ?`, claims[0].DeliveryID).Scan(&state); err != nil {
		t.Fatalf("delivery: %v", err)
	}
	if state != a2acore.DeliveryAccepted {
		t.Fatalf("in-flight delivery after a terminal task = %q, want accepted", state)
	}
}
