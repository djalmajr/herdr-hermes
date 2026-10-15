package a2aserver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/djalmajr/nau/internal/a2acore"
)

func TestNewTaskSubmittedThenWorking(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	// The client did not ask to return immediately: the server forced
	// it and the parked task comes back in the submitted state.
	task := sendTask(t, c, SendInput{
		Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1,
		MessageID: "m-1", Text: "first update",
	})
	// The client asked to return immediately: the handler interrupts on
	// the first task event and returns the stored task, whose state may
	// already be working (the executor yields both events). Both are
	// the admitted, pre-park states.
	if task.Status.State != a2a.TaskStateSubmitted && task.Status.State != a2a.TaskStateWorking {
		t.Fatalf("state = %s, want submitted or working", task.Status.State)
	}
	if task.Metadata[a2acore.MetaProject] != "org/repo" || task.Metadata[a2acore.MetaJob] != "job-1" {
		t.Errorf("task metadata = %v, want project and job", task.Metadata)
	}

	// Then the stored task is working and the execution is parked. The
	// submitted-to-working write is asynchronous, so wait for it first.
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	got, err := c.Get(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.State != a2a.TaskStateWorking {
		t.Errorf("stored state = %s, want working", got.Status.State)
	}
	waitLive(t, s, 1)

	// The delivery is pending after the task committed.
	items, err := store.Inbox(ctxT(t), a2acore.InboxFilter{Limit: 10})
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	if len(items) != 1 || items[0].TaskID != string(task.ID) {
		t.Fatalf("inbox = %+v, want one item for %s", items, task.ID)
	}
	if items[0].State != a2acore.StateWorking {
		t.Errorf("inbox state = %s, want working", items[0].State)
	}
	if len(items[0].Deliveries) != 1 {
		t.Fatalf("deliveries = %+v, want one", items[0].Deliveries)
	}
	d := items[0].Deliveries[0]
	if d.Role != a2acore.RoleOwner || d.State != a2acore.DeliveryPending {
		t.Errorf("delivery = %+v, want owner pending", d)
	}
}

func TestRefusedNewTaskCreatesNoRow(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(
		testSubject("caller-1", "org/a"),
		testSubject("caller-2"),
	)
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c1 := dialClient(t, ts.URL, auth, "caller-1")
	c2 := dialClient(t, ts.URL, auth, "caller-2")

	// Bad correlation: the session is missing.
	_, err := c2.Send(ctxT(t), SendInput{Project: "org/repo", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "hello"})
	if !errors.Is(err, a2a.ErrInvalidParams) {
		t.Errorf("bad correlation err = %v, want ErrInvalidParams", err)
	}
	if !strings.Contains(err.Error(), "refused: invalid_correlation") {
		t.Errorf("err = %v, want the refused code in the text", err)
	}

	// Unauthorized project: caller-1 may only address org/a.
	_, err = c1.Send(ctxT(t), SendInput{Project: "org/b", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-2", Text: "hello"})
	if !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("unauthorized err = %v, want ErrUnauthorized", err)
	}
	if !strings.Contains(err.Error(), "refused: unauthorized_project") {
		t.Errorf("err = %v, want the refused code in the text", err)
	}

	items, err := store.Inbox(ctxT(t), a2acore.InboxFilter{Limit: 10})
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("inbox = %+v, want no task rows", items)
	}
}

func TestDuplicateMessage(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first update"})

	// Resend the same first message (same id, no task id).
	res, err := c.Send(ctxT(t), SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first update"})
	if err != nil {
		t.Fatalf("duplicate send: %v", err)
	}
	msg, ok := res.(*a2a.Message)
	if !ok {
		t.Fatalf("duplicate result = %T, want *a2a.Message", res)
	}
	if msg.Metadata[a2acore.MetaDuplicateOf] != string(task.ID) {
		t.Errorf("duplicate_of = %v, want %s", msg.Metadata[a2acore.MetaDuplicateOf], task.ID)
	}
	if got := msg.Parts[0].Text(); got != "duplicate of task "+string(task.ID) {
		t.Errorf("duplicate text = %q, want the fixed text", got)
	}
	items, err := store.Inbox(ctxT(t), a2acore.InboxFilter{Limit: 10})
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("inbox = %d items, want 1 (no second task)", len(items))
	}
}

func TestSecondDecisionSkippedWhileExecutionLive(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s, 1)

	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "d-1", "one")
	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "d-2", "two")

	// Pass one: d-1 is handed to the live execution; d-2 must not be
	// applied out of band while the execution is live (it stays
	// pending, no refusal).
	applied, refused, err := s.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 1 || refused != 0 {
		t.Fatalf("pass 1 = %d/%d err=%v, want 1/0 nil (d-2 stays pending)", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateCompleted)
	waitLive(t, s, 0)

	// Pass two: the task is terminal, so the store already refused d-2
	// as terminal when the first decision completed it; nothing is
	// pending, and d-2 is never applied.
	applied, refused, err = s.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 0 || refused != 0 {
		t.Fatalf("pass 2 = %d/%d err=%v, want 0/0 nil (d-2 was already refused terminal)", applied, refused, err)
	}
	d2 := findDecision(t, store, string(task.ID), "d-2")
	if d2.State != a2acore.DecisionRefused || d2.Refusal != a2acore.RefuseTerminal {
		t.Errorf("d-2 = %+v, want refused terminal", d2)
	}
	d1 := findDecision(t, store, string(task.ID), "d-1")
	if d1.State != a2acore.DecisionApplied {
		t.Errorf("d-1 = %+v, want applied", d1)
	}
	got, err := c.Get(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got.Artifacts) != 1 || got.Artifacts[0].ID != "answer-d-1" {
		t.Errorf("artifacts = %+v, want only answer-d-1 (never both applied)", got.Artifacts)
	}
}

func TestParkedEntryExistsBeforeFirstEvent(t *testing.T) {
	// The executor is driven directly: the event sequence is consumed
	// synchronously in this goroutine, so no SDK scheduling races. The
	// parked entry must exist at the moment the first event of a fresh
	// admission is yielded.
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ec := &a2asrv.ExecutorContext{
		TaskID: "t-park-1",
		Message: &a2a.Message{
			ID:    "m-1",
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{a2a.NewTextPart("park probe")},
			Metadata: map[string]any{
				a2acore.MetaProject: "org/repo",
				a2acore.MetaJob:     "job-1",
				a2acore.MetaSession: "s-1",
				a2acore.MetaEpoch:   float64(1),
				a2acore.MetaSeq:     float64(1),
			},
		},
		User: a2asrv.NewAuthenticatedUser("caller-1", map[string]any{subjectAttr: testSubject("caller-1")}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		first := true
		for range s.executor.Execute(ctx, ec) {
			if first {
				first = false
				if got := s.LiveExecutions(); got != 1 {
					t.Errorf("live executions at the first event = %d, want 1 (the entry is registered before the first yield)", got)
				}
			}
		}
	}()
	waitLive(t, s, 1)
	cancel()
	<-done
}

func TestStaleEpochContinuationRefusal(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	// The task is created at epoch 2 and parked.
	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 2, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s, 1)

	// The owner asks a question: the task goes back to the caller.
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "d-1", "what happened?")
	applied, refused, err := s.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	// The ask execution has finished and been unregistered by the
	// SDK: only then is a follow-up send accepted.
	waitLive(t, s, 0)

	// The caller continues with a stale epoch: the task stays
	// input required and the refusal code is on the status event.
	res, err := c.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-2", Text: "still waiting",
	})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	got, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("continuation result = %T, want *a2a.Task", res)
	}
	if got.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("state = %s, want input required", got.Status.State)
	}
	if got.Status.Message == nil || got.Status.Message.Metadata[a2acore.MetaRefusal] != a2acore.RefuseStaleEpoch {
		t.Errorf("status message = %+v, want the refusal code", got.Status.Message)
	}
	if got.Status.Message != nil && got.Status.Message.Parts[0].Text() != "refused: stale_epoch" {
		t.Errorf("status text = %q, want the fixed text", got.Status.Message.Parts[0].Text())
	}
	// The refused continuation parked nothing and the ask applied.
	waitLive(t, s, 0)
	d := findDecision(t, store, string(task.ID), "d-1")
	if d.State != a2acore.DecisionApplied {
		t.Errorf("decision state = %s, want applied", d.State)
	}
}
