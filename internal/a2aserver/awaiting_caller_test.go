package a2aserver

import (
	"net/http"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// TestContinuationRefusedNotAwaitingCaller proves the awaiting-caller
// gate through the A2A client: the second continuation of a working
// task is refused not_awaiting_caller by the interceptor's
// pre-evaluation — the task keeps its working state, nothing is
// admitted (no ingress row, no second delivery to the owner, the
// fence stands) and the message is not written to the history —
// whatever the first continuation's execution is still alive or not.
// The store-level effect of the refusal is what this test pins.
func TestContinuationRefusedNotAwaitingCaller(t *testing.T) {
	store, path := newTestStore(t)
	store2, err := a2astore.OpenFile(path, a2astore.Options{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}

	s := newTestServer(t, store, auth, routes)
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	// The task is created and parked in working.
	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	waitStoredState(t, store, string(task.ID), a2acore.StateWorking)

	// The owner asks: the task awaits the caller and the first
	// execution ends.
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "ac-ask-1", "question one")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	// The caller continues: admitted, working again, its execution
	// parked.
	first, err := c.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "the answer",
	})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if got, ok := first.(*a2a.Task); !ok || got.Status.State != a2a.TaskStateWorking {
		t.Fatalf("continuation = %T %+v, want a working task", first, first)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	waitLive(t, s, 1)
	waitStoredState(t, store, string(task.ID), a2acore.StateWorking)

	second := SendInput{TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 3, MessageID: "m-3", Text: "second continuation"}
	ingressBefore := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress`)
	deliveriesBefore := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries`)

	// Case 1: the first continuation's execution is still alive on the
	// first server. The interceptor's pre-evaluation refuses the second
	// continuation before any execution of the SDK: the answer is the
	// stored task with the refusal, and nothing is written or parked.
	rec := &pcRecorder{}
	cBusy, err := Dial(ctxT(t), ts.URL, staticCredential{value: auth.token("caller-1")}, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial the busy client: %v", err)
	}
	t.Cleanup(func() { cBusy.Close() })
	res1, err := cBusy.Send(ctxT(t), second)
	if err != nil {
		t.Fatalf("second continuation with a live execution = %v, want the stored task with the refusal", err)
	}
	got1, ok := res1.(*a2a.Task)
	if !ok {
		t.Fatalf("second continuation = %T, want *a2a.Task", res1)
	}
	if wire := rec.wire(); wire.status != http.StatusOK {
		t.Errorf("refusal case status = %d (%s), want 200 (the task answers, not an error)", wire.status, wire.body)
	}
	if got1.Metadata[a2acore.MetaRefusal] != a2acore.RefuseNotAwaitingCaller {
		t.Errorf("task metadata = %v, want the refusal code %s", got1.Metadata, a2acore.RefuseNotAwaitingCaller)
	}

	// Case 2: no live execution of the second continuation — a new
	// server on the same store runs the executor, and the refusal is
	// not_awaiting_caller: the task keeps its working state.
	s2 := newTestServer(t, store2, auth, routes)
	rec2 := &pcRecorder{}
	c2, err := Dial(ctxT(t), serveServer(t, s2).URL, staticCredential{value: auth.token("caller-1")}, &http.Client{Timeout: 10 * time.Second, Transport: rec2})
	if err != nil {
		t.Fatalf("dial the second server: %v", err)
	}
	t.Cleanup(func() { c2.Close() })
	res, err := c2.Send(ctxT(t), second)
	if err != nil {
		t.Fatalf("second continuation on the new server: %v", err)
	}
	got, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("second continuation = %T, want *a2a.Task", res)
	}
	if wire := rec2.wire(); wire.status != http.StatusOK {
		t.Errorf("refusal case status = %d (%s), want 200 (the task answers, not an error)", wire.status, wire.body)
	}
	if got.Status.State != a2a.TaskStateWorking {
		t.Errorf("state = %s, want working (the refusal keeps the task working)", got.Status.State)
	}
	if got.Metadata[a2acore.MetaRefusal] != a2acore.RefuseNotAwaitingCaller {
		t.Errorf("task metadata = %v, want the refusal code %s", got.Metadata, a2acore.RefuseNotAwaitingCaller)
	}
	if got.Status.Message == nil || got.Status.Message.Parts[0].Text() != "refused: not_awaiting_caller" {
		t.Errorf("status message = %+v, want the fixed refusal text", got.Status.Message)
	}
	// The refusal is answered in the interceptor and parks nothing.
	waitLive(t, s2, 0)

	// Nothing was written by the admission of either case: no ingress
	// row and no second delivery to the owner, and the fence did not
	// move (the same message with the same sequence can still be
	// admitted once the task awaits the caller).
	if n := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress`); n != ingressBefore {
		t.Errorf("ingress rows = %d, want %d (no admission of the refused message)", n, ingressBefore)
	}
	if n := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries`); n != deliveriesBefore {
		t.Errorf("deliveries = %d, want %d (no second delivery to the owner)", n, deliveriesBefore)
	}
	if n := f1Count(t, path, `SELECT last_seq FROM a2a_fences WHERE subject = ? AND session = ?`, "caller-1", "s-1"); n != 2 {
		t.Errorf("fence last_seq = %d, want 2 (the refusals did not move it)", n)
	}

	// The task keeps its working state.
	after, err := c2.Get(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("get after the refusals: %v", err)
	}
	if after.Status.State != a2a.TaskStateWorking {
		t.Errorf("durable state = %s, want working", after.Status.State)
	}
	// The owner received one delivery per admitted message (m-1 and
	// m-2), never one for the refused continuation.
	if n := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE task_id = ?`, string(task.ID)); n != 2 {
		t.Errorf("deliveries of the task = %d, want 2 (the owner got no second delivery for m-3)", n)
	}
}
