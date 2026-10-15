package a2astore

import (
	"context"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// TestInbox proves the local inbox: newest update first, the project and
// state filters, the limit bounds, the deliveries in delivery-id order
// and the pending decision count.
func TestInbox(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	ctx := context.Background()
	// Three tasks in three update instants; t1 carries two deliveries
	// and a pending decision.
	if _, err := s.Admit(ctx, admission("alice", "m-1", "t1", "c1", "acme/a", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1"), {Role: a2acore.RoleGuardian, Ref: "coordinator", State: a2acore.DeliveryHeld}})); err != nil {
		t.Fatalf("Admit t1: %v", err)
	}
	if _, err := s.Create(ctx, mkTask("t1", "c1", a2a.TaskStateSubmitted, 2)); err != nil {
		t.Fatalf("Create t1: %v", err)
	}
	if _, err := s.Update(ctx, &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateWorking, 3), PrevVersion: 1}); err != nil {
		t.Fatalf("Update t1: %v", err)
	}
	if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAsk, Text: "a question", Actor: "local"}, clk.Now()); err != nil || refusal != nil {
		t.Fatalf("RecordDecision: %v, %v", refusal, err)
	}
	clk.Add(time.Second)
	t2 := mkTask("t2", "c2", a2a.TaskStateSubmitted, 1)
	t2.Metadata[a2acore.MetaProject] = "acme/b"
	if _, err := s.Create(ctx, t2); err != nil {
		t.Fatalf("Create t2: %v", err)
	}
	t2w := mkTask("t2", "c2", a2a.TaskStateCompleted, 1)
	t2w.Metadata[a2acore.MetaProject] = "acme/b"
	if _, err := s.Update(ctx, &taskstore.UpdateRequest{Task: t2w, PrevVersion: 1}); err != nil {
		t.Fatalf("Update t2: %v", err)
	}
	clk.Add(time.Second)
	if _, err := s.Admit(ctx, admission("alice", "m-3", "t3", "c3", "acme/a", "s3", 1, 1, true, nil)); err != nil {
		t.Fatalf("Admit t3: %v", err)
	}
	t3 := mkTask("t3", "c3", a2a.TaskStateSubmitted, 1)
	t3.Metadata[a2acore.MetaProject] = "acme/a"
	if _, err := s.Create(ctx, t3); err != nil {
		t.Fatalf("Create t3: %v", err)
	}

	items, err := s.Inbox(ctx, a2acore.InboxFilter{})
	if err != nil {
		t.Fatalf("Inbox: %v", err)
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.TaskID)
	}
	if len(ids) != 3 || ids[0] != "t3" || ids[1] != "t2" || ids[2] != "t1" {
		t.Fatalf("Inbox order = %v, want t3, t2, t1 (newest update first)", ids)
	}
	// The item fields.
	t1 := items[2]
	if t1.Subject != "alice" || t1.Project != "acme/a" || t1.State != a2acore.StateWorking || t1.Messages != 3 ||
		t1.Created != clk.t.Add(-2*time.Second).Format(tsLayout) || t1.Updated != clk.t.Add(-2*time.Second).Format(tsLayout) ||
		t1.PendingDecisions != 1 || len(t1.Deliveries) != 2 {
		t.Fatalf("t1 item = %+v", t1)
	}
	if t1.Deliveries[0].Role != a2acore.RoleOwner || t1.Deliveries[1].Role != a2acore.RoleGuardian {
		t.Fatalf("deliveries = %+v, want owner then guardian (delivery-id order)", t1.Deliveries)
	}
	if t1.Deliveries[0].State != a2acore.DeliveryPending || t1.Deliveries[0].NextAttemptAt == "" {
		t.Fatalf("owner delivery = %+v, want pending with a next attempt", t1.Deliveries[0])
	}
	// Filters.
	items, err = s.Inbox(ctx, a2acore.InboxFilter{Project: "acme/a"})
	if err != nil || len(items) != 2 || items[0].TaskID != "t3" || items[1].TaskID != "t1" {
		t.Fatalf("Inbox project acme/a = %+v, %v, want t3 and t1", items, err)
	}
	items, err = s.Inbox(ctx, a2acore.InboxFilter{State: a2acore.StateWorking})
	if err != nil || len(items) != 1 || items[0].TaskID != "t1" {
		t.Fatalf("Inbox state working = %+v, %v, want only t1", items, err)
	}
	// The limit: 2 of 3, and the bounds (0 means 100, at most 500).
	items, err = s.Inbox(ctx, a2acore.InboxFilter{Limit: 2})
	if err != nil || len(items) != 2 || items[0].TaskID != "t3" {
		t.Fatalf("Inbox limit 2 = %+v, %v", items, err)
	}
	items, err = s.Inbox(ctx, a2acore.InboxFilter{Limit: 0})
	if err != nil || len(items) != 3 {
		t.Fatalf("Inbox limit 0 = %d, %v, want all 3 (the default)", len(items), err)
	}
	items, err = s.Inbox(ctx, a2acore.InboxFilter{Limit: 10000})
	if err != nil || len(items) != 3 {
		t.Fatalf("Inbox limit 10000 = %d, %v, want the capped 3", len(items), err)
	}
}

// TestShow proves the local read of one task: the full task, the inbox
// item and the decisions; a missing task is a2a.ErrTaskNotFound.
func TestShow(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	ctx := context.Background()
	if _, err := s.Admit(ctx, admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	task := mkTask("t1", "c1", a2a.TaskStateSubmitted, 3)
	if _, err := s.Create(ctx, task); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, refusal, err := s.RecordDecision(ctx, a2acore.Decision{ID: "d-1", TaskID: "t1", Kind: a2acore.DecisionAnswer, Text: "the answer", Actor: "local"}, clk.Now()); err != nil || refusal != nil {
		t.Fatalf("RecordDecision: %v, %v", refusal, err)
	}
	got, item, decisions, err := s.Show(ctx, "t1")
	if err != nil {
		t.Fatalf("Show: %v", err)
	}
	if got.ID != "t1" || len(got.History) != 3 || got.History[0].ID != "t1-m0" {
		t.Fatalf("Show task = %+v, want the full history", got)
	}
	if item == nil || item.TaskID != "t1" || item.State != a2acore.StateSubmitted || item.PendingDecisions != 1 || len(item.Deliveries) != 1 {
		t.Fatalf("Show item = %+v", item)
	}
	if len(decisions) != 1 || decisions[0].ID != "d-1" || decisions[0].State != a2acore.DecisionPending {
		t.Fatalf("Show decisions = %+v", decisions)
	}
	if _, _, _, err := s.Show(ctx, "nope"); err == nil || !isErr(err, a2a.ErrTaskNotFound) {
		t.Fatalf("Show missing = %v, want task not found", err)
	}
	// TaskView of a missing task is refused too.
	if _, err := s.TaskView(ctx, "nope"); !isErr(err, a2a.ErrTaskNotFound) {
		t.Fatalf("TaskView missing = %v, want task not found", err)
	}
	// The task's project and job come back through the view.
	view, err := s.TaskView(ctx, "t1")
	if err != nil || view.Project != "acme/widgets" || view.Owner != "alice" || view.Messages != 3 {
		t.Fatalf("TaskView = %+v, %v", view, err)
	}
}
