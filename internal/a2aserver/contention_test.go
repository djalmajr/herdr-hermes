package a2aserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// ctBusyBody is the wire body (timestamp blanked) of the stable
// contention answer: the body a permitted send on a visible task gets
// when it meets another execution or write of the same task.
const ctBusyBody = `{"error":{"code":500,"status":"INTERNAL","message":"a2a: task busy, retry later","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","domain":"a2a-protocol.org","metadata":{"timestamp":""},"reason":"INTERNAL_ERROR"}]}}`

// ctFlipAuthz is the Authorizer fixture of the cancel not-found text:
// disarmed it is the default policy, and armed it grants the first
// ActionCancel call with a non-empty project (the interceptor precheck
// scoped read) and denies the following ones (the SDK canceler setup
// scoped read), so the precheck passes and the SDK's own scoped read
// fails as task not found.
type ctFlipAuthz struct {
	mu    sync.Mutex
	armed bool
	seen  bool
}

// arm arms the flip for the next cancel (a fresh first grant).
func (a *ctFlipAuthz) arm() {
	a.mu.Lock()
	a.armed, a.seen = true, false
	a.mu.Unlock()
}

// Authorize implements a2acore.Authorizer.
func (a *ctFlipAuthz) Authorize(s a2acore.Subject, act a2acore.Action, project string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.armed && act == a2acore.ActionCancel && project != "" {
		if a.seen {
			return errors.New("ct: denied after the first cancel read")
		}
		a.seen = true
	}
	return (a2acore.SubjectPolicy{}).Authorize(s, act, project)
}

// ctEnv is one server with the local operator policy, the flipping
// Authorizer and the fixed routes, and one no-credential recording
// client.
type ctEnv struct {
	t     *testing.T
	store *a2astore.Store
	srv   *Server
	ts    *httptest.Server
	authz *ctFlipAuthz
	c     *Client
	rec   *pcRecorder
}

// ctNewEnv builds the fixture: New(Config{Store, Auth:
// LocalOrBearer{Local: LocalOperator{}}, Authz: ctFlipAuthz, Routes:
// fakeRoutes{owner: "owner-ref-1"}}) and a no-credential client over a
// pcRecorder.
func ctNewEnv(t *testing.T) *ctEnv {
	t.Helper()
	store, _ := newTestStore(t)
	e := &ctEnv{t: t, store: store, authz: &ctFlipAuthz{}}
	srv, err := New(Config{
		Store:   store,
		Auth:    LocalOrBearer{Local: LocalOperator{}},
		Authz:   e.authz,
		Routes:  fakeRoutes{owner: "owner-ref-1"},
		BaseURL: "http://127.0.0.1:7430",
		Version: "test-1",
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	e.srv = srv
	e.ts = serveServer(t, srv)
	rec := &pcRecorder{}
	c, err := Dial(ctxT(t), e.ts.URL, staticCredential{value: ""}, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	e.c, e.rec = c, rec
	return e
}

// ctPark sends one fresh task and waits for the durable task to reach
// working (the parked state a second reader of the file sees).
func ctPark(t *testing.T, e *ctEnv, project, session, msgID string) *a2a.Task {
	t.Helper()
	task := sendTask(t, e.c, SendInput{Project: project, Session: session, Epoch: 1, Seq: 1, MessageID: msgID, Text: "ct probe"})
	waitTaskState(t, e.c, string(task.ID), a2a.TaskStateWorking)
	return task
}

// ctAsk applies an ask decision to the task and waits for the durable
// input-required state.
func ctAsk(t *testing.T, e *ctEnv, taskID, decisionID string) {
	t.Helper()
	recordDecision(t, e.store, taskID, a2acore.DecisionAsk, decisionID, "question")
	if applied, refused, err := e.srv.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, e.c, taskID, a2a.TaskStateInputRequired)
}

// ctAssertCanceled cancels the task as the local operator and expects
// the normal 200 canceled answer.
func (e *ctEnv) ctAssertCanceled(taskID a2a.TaskID, what string) {
	e.t.Helper()
	task, err := e.c.Cancel(ctxT(e.t), string(taskID))
	if err != nil || task == nil || task.Status.State != a2a.TaskStateCanceled {
		e.t.Fatalf("%s cancel = %+v, %v, want the canceled task", what, task, err)
	}
}

// ctAssertArmedNotFound arms the flip, cancels the task as the local
// operator and expects the canonical 404 not-found body: the same body
// the SDK answers for a cancel of an id it does not hold, in the
// parked and the idle case alike.
func (e *ctEnv) ctAssertArmedNotFound(taskID a2a.TaskID) {
	e.t.Helper()
	e.authz.arm()
	_, err := e.c.Cancel(ctxT(e.t), string(taskID))
	wire := e.rec.wire()
	if err == nil || !errors.Is(err, a2a.ErrTaskNotFound) || wire.status != http.StatusNotFound {
		e.t.Fatalf("armed cancel err=%v wire=%d %s, want 404 task not found", err, wire.status, wire.body)
	}
	if got := pcNorm(wire.body); got != cgMaskedBody {
		e.t.Fatalf("armed cancel body = %s, want the canonical %s", got, cgMaskedBody)
	}
}

// TestCancelNotFoundTextParkedMatchesIdle: a cancel of a task the
// caller can no longer read is answered with the one canonical
// not-found body (the idle-path SDK text) whether the task's execution
// is parked or the task is idle, and a cancel the precheck and the SDK
// both allow keeps the normal canceled answer.
func TestCancelNotFoundTextParkedMatchesIdle(t *testing.T) {
	t.Run("unarmed cancels stay normal", func(t *testing.T) {
		e := ctNewEnv(t)
		parked := ctPark(t, e, "org/repo", "s-1", "m-1")
		idle := ctPark(t, e, "org/repo", "s-2", "m-2")
		waitLive(t, e.srv, 2)
		ctAsk(t, e, string(idle.ID), "ct-ask-unarmed")
		waitLive(t, e.srv, 1)
		e.ctAssertCanceled(parked.ID, "the parked task")
		waitLive(t, e.srv, 0)
		e.ctAssertCanceled(idle.ID, "the idle task")
	})

	t.Run("armed parked cancel is the canonical not found", func(t *testing.T) {
		e := ctNewEnv(t)
		parked := ctPark(t, e, "org/repo", "s-1", "m-1")
		waitLive(t, e.srv, 1)
		e.ctAssertArmedNotFound(parked.ID)
	})

	t.Run("armed idle cancel is the canonical not found", func(t *testing.T) {
		e := ctNewEnv(t)
		task := ctPark(t, e, "org/repo", "s-1", "m-1")
		ctAsk(t, e, string(task.ID), "ct-ask-idle")
		waitLive(t, e.srv, 0)
		e.ctAssertArmedNotFound(task.ID)
	})
}

// TestContentionVisibleParkedIsBusy: a permitted continuation on a
// visible parked task is answered with the exact stable busy body,
// writes nothing (the store snapshot and the live count are unchanged)
// and the same message id resent after the parked execution is gone is
// admitted as the same task: the busy send admitted nothing and fenced
// nothing.
func TestContentionVisibleParkedIsBusy(t *testing.T) {
	f := pcSetup(t)
	secretID := string(f.secret.ID)
	before := pcSnapshot(t, f.e, secretID)

	in := SendInput{
		TaskID: secretID, ContextID: f.secret.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "ct continuation",
	}
	res, err := f.cA.Send(ctxT(t), in)
	if err == nil || !errors.Is(err, a2a.ErrInternalError) || err.Error() != taskBusyText {
		t.Fatalf("visible parked continuation = %+v, %v, want the internal error %q", res, err, taskBusyText)
	}
	wire := f.recA.wire()
	if wire.status != http.StatusInternalServerError {
		t.Fatalf("busy status = %d (%s), want 500", wire.status, wire.body)
	}
	if got := strings.TrimSpace(pcNorm(wire.body)); got != ctBusyBody {
		t.Fatalf("busy body = %s, want %s", got, ctBusyBody)
	}
	for _, leak := range []string{secretID, f.secret.ContextID, "org/secret", "in progress", "concurrent modification", "exec ctx"} {
		if strings.Contains(wire.body, leak) {
			t.Errorf("busy body carries %q: %s", leak, wire.body)
		}
	}
	if after := pcSnapshot(t, f.e, secretID); after != before {
		t.Errorf("store changed by the busy send:\n before %+v\n after  %+v", before, after)
	}
	if n := f.e.srv.LiveExecutions(); n != 1 {
		t.Errorf("live executions = %d, want 1 (the parked one is untouched)", n)
	}

	// The same message id, resent after the parked execution is gone,
	// is admitted as the same task.
	idfxAsk(t, f.e, f.cA, secretID, "ct-ask-busy")
	res, err = f.cA.Send(ctxT(t), in)
	if err != nil {
		t.Fatalf("resent continuation: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok || task.ID != f.secret.ID {
		t.Fatalf("resent continuation = %T %+v, want the same task", res, res)
	}
}

// TestBusyAfterAdmission: a continuation whose admission commits before
// its first event loses a write race gets the same stable busy answer as
// the conflicts that are met before the admission, while the admission
// stands: the ingress row of the message and the session fence advance
// with it. A resend of the same message (same message id, session, epoch
// and sequence) is not admitted again (no second ingress row, no second
// delivery to the owner); the store answers it as a duplicate and the
// answer comes back as the task itself, which the SDK leaves failed
// because it does not accept a message answer on a task that is already
// stored. The order is forced with the store hook: on the working
// event's update a competing write of the same context and version lands
// first, so the update loses the SDK optimistic version check and the
// send's error is the contention the interceptor rewrites.
func TestBusyAfterAdmission(t *testing.T) {
	store, path := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}
	s1 := newTestServer(t, store, auth, routes)
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")
	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "ba-ask", "question")
	if applied, refused, err := s1.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s1, 0)

	var once sync.Once
	s2 := newHookedServer(t, store, auth, routes, &hookedTaskStore{
		reviewHook: func(ctx context.Context, req *taskstore.UpdateRequest) {
			if !isWorkingEvent(req) {
				return
			}
			once.Do(func() {
				competing := *req
				competing.Event = nil
				if _, err := store.Update(ctx, &competing); err != nil {
					t.Errorf("competing write: %v", err)
				}
			})
		},
	})
	rec := &pcRecorder{}
	c2, err := Dial(ctxT(t), serveServer(t, s2).URL, staticCredential{value: auth.token("caller-1")}, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c2.Close() })

	in := SendInput{TaskID: string(task.ID), ContextID: task.ContextID, Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "second"}
	res, err := c2.Send(ctxT(t), in)
	if err == nil || !errors.Is(err, a2a.ErrInternalError) || err.Error() != taskBusyText {
		t.Fatalf("send = %T, %v, want the internal error %q", res, err, taskBusyText)
	}
	wire := rec.wire()
	if wire.status != http.StatusInternalServerError {
		t.Fatalf("busy status = %d (%s), want 500", wire.status, wire.body)
	}
	if got := strings.TrimSpace(pcNorm(wire.body)); got != ctBusyBody {
		t.Fatalf("busy body = %s, want %s", got, ctBusyBody)
	}
	for _, leak := range []string{string(task.ID), task.ContextID, "org/repo", "caller-1", "in progress", "concurrent modification", "exec ctx"} {
		if strings.Contains(wire.body, leak) {
			t.Errorf("busy body carries %q: %s", leak, wire.body)
		}
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress WHERE message_id = ?`, "m-2"); got != 1 {
		t.Errorf("ingress rows of the busy message = %d, want 1 (the admission stands)", got)
	}
	if got := f1Count(t, path, `SELECT last_seq FROM a2a_fences WHERE subject = ? AND session = ?`, "caller-1", "s-1"); got != 2 {
		t.Errorf("fence last_seq = %d, want 2 (the fence advanced with the admission)", got)
	}
	waitLive(t, s2, 0)

	// What the documented check reads before a resend: the busy message
	// is already in the task history, and the task is not failed.
	before, err := c2.Get(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("get after the busy answer: %v", err)
	}
	inHistory := false
	for _, m := range before.History {
		if m.ID == "m-2" {
			inHistory = true
		}
	}
	if !inHistory {
		t.Errorf("history after the busy answer does not carry m-2: %+v", before.History)
	}
	if before.Status.State == a2a.TaskStateFailed {
		t.Errorf("task state after the busy answer = %s, want not failed", before.Status.State)
	}
	t.Logf("task state after the busy answer: %s", before.Status.State)

	// The resend of the same message (same message id, session, epoch
	// and sequence) is not admitted again: no second ingress row and no
	// second delivery to the owner.
	res, err = c2.Send(ctxT(t), in)
	if err != nil {
		t.Fatalf("resent the same message: %v", err)
	}
	resent, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("resent result = %T, want *a2a.Task", res)
	}
	if resent.ID != task.ID {
		t.Errorf("resent task id = %s, want %s", resent.ID, task.ID)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress WHERE message_id = ?`, "m-2"); got != 1 {
		t.Errorf("ingress rows after the resend = %d, want 1 (no second admission)", got)
	}
	ingressID := f1Count(t, path, `SELECT ingress_id FROM a2a_ingress WHERE message_id = ?`, "m-2")
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID); got != 1 {
		t.Errorf("deliveries of the message after the resend = %d, want 1 (no second notice to the owner)", got)
	}
	tv, err := store.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view after the resend: %v, %v", tv, err)
	}
	if resent.Status.State != a2a.TaskStateFailed {
		t.Errorf("resent task state = %s, want failed (the SDK refuses the duplicate message answer on a stored task)", resent.Status.State)
	}
	if tv.State != a2acore.StateFailed {
		t.Errorf("durable task state after the resend = %q, want %q (the SDK refuses the duplicate message answer on a stored task)", tv.State, a2acore.StateFailed)
	}
}

// TestCancelationInProgressVisibleIsBusy: a cancelation that runs
// without a live execution of the task (its store write is held by the
// store hook, so the SDK keeps the cancelation keyed by the task id)
// answers a permitted continuation of the visible task with the one
// stable busy body — the same answer, status and reason as every other
// contention, with no task id, owner, project or SDK internal text —
// and a resend of the same message after the cancelation commits gets
// the answer the canceled state asks for.
func TestCancelationInProgressVisibleIsBusy(t *testing.T) {
	store, path := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}

	var hold sync.Once
	reached := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseWrite)

	s := newHookedServer(t, store, auth, routes, &hookedTaskStore{
		beforeUpdate: func(req *taskstore.UpdateRequest) {
			ev, ok := req.Event.(*a2a.TaskStatusUpdateEvent)
			if !ok || ev.Status.State != a2a.TaskStateCanceled {
				return
			}
			hold.Do(func() { close(reached) })
			awaitSignal(t, "the test to release the cancelation write", release)
		},
	})
	rec := &pcRecorder{}
	c, err := Dial(ctxT(t), serveServer(t, s).URL, staticCredential{value: auth.token("caller-1")}, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	waitLive(t, s, 1)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "ci-ask", "question")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	// The cancelation runs in a background call and its store write is
	// held: while it runs the SDK keys the cancelation by the task id
	// and a send of the task meets it.
	cancelDone := make(chan struct{})
	var cancelTask *a2a.Task
	var cancelErr error
	go func() {
		defer close(cancelDone)
		cancelTask, cancelErr = c.Cancel(ctxT(t), string(task.ID))
	}()
	awaitSignal(t, "the cancelation to reach its store write", reached)

	in := SendInput{TaskID: string(task.ID), ContextID: task.ContextID, Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "second"}
	res, err := c.Send(ctxT(t), in)
	if err == nil || !errors.Is(err, a2a.ErrInternalError) || err.Error() != taskBusyText {
		t.Fatalf("continuation during the cancelation = %T, %v, want the internal error %q", res, err, taskBusyText)
	}
	wire := rec.wire()
	if wire.status != http.StatusInternalServerError {
		t.Fatalf("busy status = %d (%s), want 500", wire.status, wire.body)
	}
	if got := strings.TrimSpace(pcNorm(wire.body)); got != ctBusyBody {
		t.Fatalf("busy body = %s, want %s", got, ctBusyBody)
	}
	for _, leak := range []string{string(task.ID), task.ContextID, "org/repo", "caller-1", "in progress", "cancelation", "concurrent modification", "exec ctx"} {
		if strings.Contains(wire.body, leak) {
			t.Errorf("busy body carries %q: %s", leak, wire.body)
		}
	}
	// The conflict was met before the admission: nothing was admitted.
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress WHERE message_id = ?`, "m-2"); got != 0 {
		t.Errorf("ingress rows of the busy send = %d, want 0 (the conflict was met before the admission)", got)
	}
	if got := f1Count(t, path, `SELECT last_seq FROM a2a_fences WHERE subject = ? AND session = ?`, "caller-1", "s-1"); got != 1 {
		t.Errorf("fence last_seq = %d, want 1 (the fence stands)", got)
	}

	// The cancelation write releases: the cancelation commits the
	// canceled state and its call answers the canceled task.
	releaseWrite()
	<-cancelDone
	if cancelErr != nil || cancelTask == nil || cancelTask.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("cancel = %+v, %v, want the canceled task", cancelTask, cancelErr)
	}
	waitStoredState(t, store, string(task.ID), a2acore.StateCanceled)

	// After the cancelation, the same message resent gets the answer
	// the canceled state asks for: a continuation on a terminal task is
	// refused by the SDK, and nothing of the resends was admitted.
	res, err = c.Send(ctxT(t), in)
	if err == nil || !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Fatalf("resent continuation on the canceled task = %T, %v, want the unsupported operation refusal", res, err)
	}
	wire = rec.wire()
	if wire.status != http.StatusBadRequest || !strings.Contains(wire.body, `"reason":"UNSUPPORTED_OPERATION"`) {
		t.Fatalf("resent wire = %d %s, want 400 unsupported operation", wire.status, wire.body)
	}
	if !strings.Contains(wire.body, "terminal state") {
		t.Errorf("resent body lacks the terminal state cause: %s", wire.body)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress WHERE message_id = ?`, "m-2"); got != 0 {
		t.Errorf("ingress rows of the resends = %d, want 0 (nothing was admitted)", got)
	}
}
