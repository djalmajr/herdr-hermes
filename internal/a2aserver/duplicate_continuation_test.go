package a2aserver

import (
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

// TestDuplicateContinuationResend: a continuation whose message id was
// already admitted is not admitted again (no second ingress row, no
// second delivery, no second history copy); the resend answers the
// task in the same state it had before the resend, and the durable
// state does not change.
func TestDuplicateContinuationResend(t *testing.T) {
	store, path := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}
	s := newTestServer(t, store, auth, routes)
	c := dialClient(t, serveServer(t, s).URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "dc-ask-1", "question 1")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask 1 = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	in := SendInput{TaskID: string(task.ID), ContextID: task.ContextID, Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "continuation"}
	res, err := c.Send(ctxT(t), in)
	if err != nil {
		t.Fatalf("admit the continuation: %v", err)
	}
	admitted, ok := res.(*a2a.Task)
	if !ok || admitted.ID != task.ID || admitted.Status.State != a2a.TaskStateWorking {
		t.Fatalf("continuation = %T %+v, want the same task working", res, res)
	}
	waitLive(t, s, 1)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "dc-ask-2", "question 2")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask 2 = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	tvBefore, err := store.TaskView(ctxT(t), string(task.ID))
	if err != nil || tvBefore == nil {
		t.Fatalf("task view before the resend: %v, %v", tvBefore, err)
	}
	ingressID := f1Count(t, path, `SELECT ingress_id FROM a2a_ingress WHERE message_id = ?`, "m-2")
	deliveriesBefore := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID)

	// The resend of the same message (same message id, session, epoch
	// and sequence) is not admitted again and answers the task in the
	// same state.
	res, err = c.Send(ctxT(t), in)
	if err != nil {
		t.Fatalf("resent the same continuation: %v", err)
	}
	resent, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("resent result = %T, want *a2a.Task", res)
	}
	if resent.ID != task.ID {
		t.Errorf("resent task id = %s, want %s", resent.ID, task.ID)
	}
	if resent.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("resent task state = %s, want %s (the state before the resend)", resent.Status.State, a2a.TaskStateInputRequired)
	}
	tvAfter, err := store.TaskView(ctxT(t), string(task.ID))
	if err != nil || tvAfter == nil {
		t.Fatalf("task view after the resend: %v, %v", tvAfter, err)
	}
	if tvAfter.State != tvBefore.State {
		t.Errorf("durable task state after the resend = %q, want %q (unchanged by the resend)", tvAfter.State, tvBefore.State)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress WHERE message_id = ?`, "m-2"); got != 1 {
		t.Errorf("ingress rows of m-2 after the resend = %d, want 1 (no second admission)", got)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID); got != deliveriesBefore {
		t.Errorf("deliveries of m-2 after the resend = %d, want %d (no second delivery)", got, deliveriesBefore)
	}
	copies := 0
	for _, m := range resent.History {
		if m != nil && m.ID == "m-2" {
			copies++
		}
	}
	if copies != 1 {
		t.Errorf("history copies of m-2 in the resent task = %d, want 1 (no second copy)", copies)
	}
}

// TestDuplicateFirstMessageResend: the resend of the first message of a
// task (same message id, no task id) keeps the message answer with the
// nau.duplicate_of metadata set to the first task id, and the task
// keeps its state.
func TestDuplicateFirstMessageResend(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}
	s := newTestServer(t, store, auth, routes)
	c := dialClient(t, serveServer(t, s).URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)

	res, err := c.Send(ctxT(t), SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	if err != nil {
		t.Fatalf("resent the first message: %v", err)
	}
	msg, ok := res.(*a2a.Message)
	if !ok {
		t.Fatalf("resent result = %T, want *a2a.Message", res)
	}
	if got, _ := msg.Metadata[a2acore.MetaDuplicateOf].(string); got != string(task.ID) {
		t.Errorf("nau.duplicate_of = %q, want %s", got, task.ID)
	}
	tv, err := store.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view after the resend: %v, %v", tv, err)
	}
	if tv.State != a2acore.StateWorking {
		t.Errorf("durable task state after the resend = %q, want %q (unchanged)", tv.State, a2acore.StateWorking)
	}
}
