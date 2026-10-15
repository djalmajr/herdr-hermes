package a2astore

import (
	"context"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// TestAdmitContinuationNotAwaitingCaller proves the awaiting-caller gate
// of Admit, which runs after the duplicate check and before the fence:
// a continuation on a task that is not input_required is refused
// not_awaiting_caller without writing anything (the fence stands, and
// the same message is admitted once the task is input_required), a
// continuation on an input_required task is admitted, a resend of an
// admitted continuation keeps its duplicate answer when the task is
// working, and a continuation on a task without a row is refused
// unknown_task.
func TestAdmitContinuationNotAwaitingCaller(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openTest(t, clk, "alice")
	ctx := context.Background()

	// (a) A continuation of a working task is refused and writes
	// nothing; the same message with the same sequence is admitted
	// once the task awaits the caller (the fence never moved).
	if _, err := s.Admit(ctx, admission("alice", "m-1", "t1", "c1", "acme/widgets", "s1", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit t1: %v", err)
	}
	if _, err := s.Create(ctx, mkTask("t1", "c1", a2a.TaskStateWorking, 1)); err != nil {
		t.Fatalf("Create t1: %v", err)
	}
	before := queryCount(t, s.db)
	refused, err := s.Admit(ctx, admission("alice", "m-2", "t1", "c1", "acme/widgets", "s1", 1, 2, false,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || refused.Duplicate || refused.IngressID != 0 || refuseCode(refused.Refusal) != a2acore.RefuseNotAwaitingCaller {
		t.Fatalf("continuation of a working task = %+v, %v, want not_awaiting_caller", refused, err)
	}
	if refused.Refusal.Detail != "the task is not awaiting the caller" {
		t.Fatalf("refusal detail = %q, want the fixed text", refused.Refusal.Detail)
	}
	after := queryCount(t, s.db)
	for table := range before {
		if before[table] != after[table] {
			t.Fatalf("the refusal changed %s: %d -> %d", table, before[table], after[table])
		}
	}
	var lastSeq int64
	if err := s.db.QueryRow(`SELECT last_seq FROM a2a_fences WHERE subject = 'alice' AND session = 's1'`).Scan(&lastSeq); err != nil {
		t.Fatalf("fence: %v", err)
	}
	if lastSeq != 1 {
		t.Fatalf("fence last_seq = %d, want 1 (the refusal did not move it)", lastSeq)
	}
	// The owner asks: the task awaits the caller, and the very same
	// message with the same sequence is admitted.
	if _, err := s.Update(ctx, &taskstore.UpdateRequest{Task: mkTask("t1", "c1", a2a.TaskStateInputRequired, 2), PrevVersion: 1}); err != nil {
		t.Fatalf("input_required t1: %v", err)
	}
	admitted, err := s.Admit(ctx, admission("alice", "m-2", "t1", "c1", "acme/widgets", "s1", 1, 2, false,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || admitted.Refusal != nil || admitted.Duplicate || admitted.IngressID == 0 {
		t.Fatalf("the same message on the input_required task = %+v, %v, want admitted (the fence stands)", admitted, err)
	}

	// (b) A continuation of a task that already awaits the caller is
	// admitted.
	if _, err := s.Admit(ctx, admission("alice", "m-3", "t2", "c2", "acme/widgets", "s2", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit t2: %v", err)
	}
	if _, err := s.Create(ctx, mkTask("t2", "c2", a2a.TaskStateInputRequired, 1)); err != nil {
		t.Fatalf("Create t2: %v", err)
	}
	res, err := s.Admit(ctx, admission("alice", "m-4", "t2", "c2", "acme/widgets", "s2", 1, 2, false,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || res.Refusal != nil || res.Duplicate || res.IngressID == 0 {
		t.Fatalf("continuation of an input_required task = %+v, %v, want admitted", res, err)
	}

	// (c) A resend of an admitted continuation keeps its duplicate
	// answer when the task is working again (the duplicate check stands
	// before the gate).
	if _, err := s.Admit(ctx, admission("alice", "m-5", "t3", "c3", "acme/widgets", "s3", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit t3: %v", err)
	}
	if _, err := s.Create(ctx, mkTask("t3", "c3", a2a.TaskStateInputRequired, 1)); err != nil {
		t.Fatalf("Create t3: %v", err)
	}
	res, err = s.Admit(ctx, admission("alice", "m-6", "t3", "c3", "acme/widgets", "s3", 1, 2, false,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || res.Refusal != nil || res.IngressID == 0 {
		t.Fatalf("admit the continuation of t3 = %+v, %v", res, err)
	}
	// The task works again, as the executor's working status would
	// have stored it after the continuation.
	if _, err := s.Update(ctx, &taskstore.UpdateRequest{Task: mkTask("t3", "c3", a2a.TaskStateWorking, 2), PrevVersion: 1}); err != nil {
		t.Fatalf("working t3: %v", err)
	}
	dup, err := s.Admit(ctx, admission("alice", "m-6", "t3", "c3", "acme/widgets", "s3", 1, 2, false,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")}))
	if err != nil || dup.Refusal != nil || !dup.Duplicate || dup.DuplicateTaskID != "t3" {
		t.Fatalf("resend on a working task = %+v, %v, want the duplicate of t3 (not a refusal)", dup, err)
	}

	// (d) A continuation on a task without a row is refused
	// unknown_task and writes nothing.
	if _, err := s.Admit(ctx, admission("alice", "m-7", "t4", "c4", "acme/widgets", "s4", 1, 1, true, nil)); err != nil {
		t.Fatalf("Admit t4: %v", err)
	}
	// t4 is never created: no a2a_tasks row.
	before = queryCount(t, s.db)
	miss, err := s.Admit(ctx, admission("alice", "m-8", "t4", "c4", "acme/widgets", "s4", 1, 2, false, nil))
	if err != nil || miss.Duplicate || miss.IngressID != 0 || refuseCode(miss.Refusal) != a2acore.RefuseUnknownTask {
		t.Fatalf("continuation without a task row = %+v, %v, want unknown_task", miss, err)
	}
	if miss.Refusal.Detail != "no such task" {
		t.Fatalf("refusal detail = %q, want the fixed text", miss.Refusal.Detail)
	}
	after = queryCount(t, s.db)
	for table := range before {
		if before[table] != after[table] {
			t.Fatalf("the unknown-task refusal changed %s: %d -> %d", table, before[table], after[table])
		}
	}
}
