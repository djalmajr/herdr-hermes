package a2aserver

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// dupPushSender is the push sender of the duplicate-resend tests: it
// only records the calls (the SDK never sends one for a resend that is
// answered before the execution).
type dupPushSender struct {
	mu     sync.Mutex
	events []a2a.Event
}

// SendPush implements push.Sender.
func (d *dupPushSender) SendPush(_ context.Context, _ *a2a.PushConfig, ev a2a.Event) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, ev)
	return nil
}

// count returns the number of recorded push calls.
func (d *dupPushSender) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.events)
}

// lastEvent returns the last recorded push call (nil when none was
// recorded).
func (d *dupPushSender) lastEvent() a2a.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.events) == 0 {
		return nil
	}
	return d.events[len(d.events)-1]
}

// dupNewServer builds one server with the given push sender injected in
// Config.PushSender, the way the push tests build theirs.
func dupNewServer(t *testing.T, store *a2astore.Store, auth a2acore.Authenticator, routes a2acore.RouteSource, sender push.Sender) *Server {
	t.Helper()
	s, err := New(Config{
		Store:      store,
		Auth:       auth,
		Routes:     routes,
		BaseURL:    "http://127.0.0.1:7430",
		Version:    "test-1",
		PushSender: sender,
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	return s
}

// dupTaskVersion reads the task's durable version through the same
// read-only connection the push tests read with (the probe of the
// review read it this way).
func dupTaskVersion(t *testing.T, path, taskID string) int {
	t.Helper()
	return f1Count(t, path, `SELECT version FROM a2a_tasks WHERE task_id = ?`, taskID)
}

// TestDuplicateContinuationResend: a continuation whose message id was
// already admitted is not admitted again (no second ingress row, no
// second delivery, no second history copy); the resend answers the
// task in the same state it had before the resend, and the durable
// state does not change.
func TestDuplicateContinuationResend(t *testing.T) {
	store, path := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}
	pusher := &dupPushSender{}
	s := dupNewServer(t, store, auth, routes, pusher)
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
	versionBefore := dupTaskVersion(t, path, string(task.ID))
	ingressID := f1Count(t, path, `SELECT ingress_id FROM a2a_ingress WHERE message_id = ?`, "m-2")
	deliveriesBefore := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID)

	// A push config on the task would be the target of the extra push
	// the resend used to send; the recording sender counts it.
	if _, err := c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: task.ID, URL: f1URL}); err != nil {
		t.Fatalf("create push config before the resend: %v", err)
	}

	// The resend of the same message (same message id, session, epoch
	// and sequence) is not admitted again and answers the task in the
	// same state, without running it: no push and no new version.
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
	if got := pusher.count(); got != 0 {
		t.Errorf("push calls caused by the resend = %d, want 0", got)
	}
	if got := dupTaskVersion(t, path, string(task.ID)); got != versionBefore {
		t.Errorf("task version after the resend = %d, want %d (no write by the resend)", got, versionBefore)
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

// TestDuplicateResendInvisibleTaskStaysNotFound: the early resend
// answer never reaches a task the caller cannot see — the visibility
// check runs before it — and another subject that resends the same
// message id on the same task gets the not-found of a send, the same
// body as a task that does not exist.
func TestDuplicateResendInvisibleTaskStaysNotFound(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"), testSubject("caller-2"))
	routes := fakeRoutes{owner: "owner-ref-1"}
	s := newTestServer(t, store, auth, routes)
	c1 := dialClient(t, serveServer(t, s).URL, auth, "caller-1")

	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "dr-ask-1", "question 1")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask 1 = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	in := SendInput{TaskID: string(task.ID), ContextID: task.ContextID, Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "continuation"}
	if _, err := c1.Send(ctxT(t), in); err != nil {
		t.Fatalf("admit the continuation: %v", err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "dr-ask-2", "question 2")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask 2 = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	// Another subject resends the same message id on the same task: the
	// task is outside its owner grant and the answer is the not-found of
	// a send, not the early response.
	rec := &pcRecorder{}
	c2, err := Dial(ctxT(t), serveServer(t, s).URL, staticCredential{value: auth.token("caller-2")}, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c2.Close() })
	res, err := c2.Send(ctxT(t), in)
	if err == nil {
		t.Fatalf("foreign resend = %T, nil, want the not-found of a send", res)
	}
	wire := rec.wire()
	if wire.status != http.StatusNotFound {
		t.Fatalf("foreign resend status = %d (%s), want 404", wire.status, wire.body)
	}
	if got := strings.TrimSpace(pcNorm(wire.body)); got != pcMissingBody {
		t.Fatalf("foreign resend body = %s, want the unchanged not-found body %s", got, pcMissingBody)
	}
	if strings.Contains(wire.body, string(task.ID)) {
		t.Errorf("foreign resend body carries the task id: %s", wire.body)
	}
}

// dupHoldBefore delegates to the real interceptor and, for the first
// send of the held message id, waits after the real Before has run
// (its admission lookup found nothing) until the test releases it, so
// the request reaches the SDK only after another request admitted the
// same message. A later send of the same message id (the resend,
// answered early by the interceptor) is not held.
type dupHoldBefore struct {
	*authInterceptor
	messageID string
	entered   chan struct{}
	release   chan struct{}
	heldOnce  sync.Once
}

// Before implements a2asrv.CallInterceptor.
func (g *dupHoldBefore) Before(ctx context.Context, callCtx *a2asrv.CallContext, req *a2asrv.Request) (context.Context, any, error) {
	ctx, result, err := g.authInterceptor.Before(ctx, callCtx, req)
	var held bool
	if p, ok := req.Payload.(*a2a.SendMessageRequest); ok && callCtx.Method() == "SendMessage" && p != nil && p.Message != nil && string(p.Message.ID) == g.messageID && err == nil {
		g.heldOnce.Do(func() { held = true })
	}
	if held {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx, nil, ctx.Err()
		}
	}
	return ctx, result, err
}

// TestDuplicateResendPastInterceptorWindow pins the one window the
// interceptor cannot close: a send whose admission lookup ran before
// the first admission of the same message committed, and that reaches
// the SDK only after that admission is stored (here, after the
// owner's second question ended the first execution). The executor's
// defense branch answers it with the stable busy refusal: the task
// keeps its state and is not failed, no second ingress row or
// delivery is made, nothing is written and no push goes out, and the
// second server keeps no live execution. The next resend of the same
// message is answered by the interceptor with the stored task, without
// running it.
func TestDuplicateResendPastInterceptorWindow(t *testing.T) {
	store, path := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}
	pusher := &dupPushSender{}
	s := dupNewServer(t, store, auth, routes, pusher)
	c := dialClient(t, serveServer(t, s).URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "dw-ask-1", "question 1")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask 1 = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	// A second server on the same store whose interceptor holds the m-2
	// send right after its admission lookup.
	s2 := dupNewServer(t, store, auth, routes, pusher)
	hold := &dupHoldBefore{
		authInterceptor: newAuthInterceptor(s2.cfg).withCapabilities(&s2.card.Capabilities),
		messageID:       "m-2",
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	released := false
	t.Cleanup(func() {
		if !released {
			close(hold.release)
		}
	})
	s2.handler = a2asrv.NewHandler(
		s2.executor,
		a2asrv.WithTaskStore(scopedTaskStore{store: store}),
		a2asrv.WithPushNotifications(store.PushConfigs(), s2.cfg.PushSender),
		a2asrv.WithCapabilityChecks(&s2.card.Capabilities),
		a2asrv.WithCallInterceptors(hold),
		a2asrv.WithLogger(s2.cfg.Logger),
	)
	rec := &pcRecorder{}
	c2, err := Dial(ctxT(t), serveServer(t, s2).URL, staticCredential{value: auth.token("caller-1")}, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c2.Close() })

	in := SendInput{TaskID: string(task.ID), ContextID: task.ContextID, Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "continuation"}
	type answer struct {
		res a2a.SendMessageResult
		err error
	}
	done := make(chan answer, 1)
	go func() {
		res, err := c2.Send(ctxT(t), in)
		done <- answer{res, err}
	}()
	awaitSignal(t, "held send after its admission lookup", hold.entered)

	// The same message is admitted through the first server, and the
	// owner's second question ends that execution.
	sendTask(t, c, in)
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "dw-ask-2", "question 2")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask 2 = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	if _, err := c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: task.ID, URL: f1URL}); err != nil {
		t.Fatalf("create push config before the release: %v", err)
	}
	versionBefore := dupTaskVersion(t, path, string(task.ID))
	pushesBefore := pusher.count()
	ingressID := f1Count(t, path, `SELECT ingress_id FROM a2a_ingress WHERE message_id = ?`, "m-2")
	deliveriesBefore := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID)

	close(hold.release)
	released = true
	var ans answer
	select {
	case ans = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the held send did not answer within 10s of its release")
	}
	// The held send gets the canonical busy answer: the stable text,
	// the 500 INTERNAL body, no new version, no push, the durable state
	// unchanged and no live execution on the second server.
	if ans.err == nil || !errors.Is(ans.err, a2a.ErrInternalError) || ans.err.Error() != taskBusyText {
		state := ""
		if task, ok := ans.res.(*a2a.Task); ok {
			state = string(task.Status.State)
		}
		t.Errorf("held send = %T (state %s), %v, want the internal error %q and no result", ans.res, state, ans.err, taskBusyText)
	}
	wire := rec.wire()
	if wire.status != http.StatusInternalServerError {
		t.Errorf("busy status = %d, want 500 (body: %.120s)", wire.status, wire.body)
	} else if got := strings.TrimSpace(pcNorm(wire.body)); got != ctBusyBody {
		t.Errorf("busy body = %s, want %s", got, ctBusyBody)
	}
	if got := dupTaskVersion(t, path, string(task.ID)); got != versionBefore {
		t.Errorf("task version after the held send = %d, want %d (no write by the held send)", got, versionBefore)
	}
	if got := pusher.count() - pushesBefore; got != 0 {
		t.Errorf("pushes caused by the held send = %d, want 0 (no push by the held send)", got)
	}
	tv, err := store.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil || tv.State != a2acore.StateInputRequired {
		t.Errorf("durable task after the held send = %+v, %v, want %q", tv, err, a2acore.StateInputRequired)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress WHERE message_id = ?`, "m-2"); got != 1 {
		t.Errorf("ingress rows of m-2 = %d, want 1 (no second admission)", got)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID); got != deliveriesBefore {
		t.Errorf("deliveries of m-2 = %d, want %d (no second delivery)", got, deliveriesBefore)
	}
	waitLive(t, s2, 0)

	// The next resend of the same message is answered by the
	// interceptor with the stored task, without running it: no new
	// version, no push and the durable state unchanged.
	res, err := c2.Send(ctxT(t), in)
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
	if resent.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("resent task state = %s, want %s (the state before the resend)", resent.Status.State, a2a.TaskStateInputRequired)
	}
	if got := dupTaskVersion(t, path, string(task.ID)); got != versionBefore {
		t.Errorf("task version after the resend = %d, want %d (no write by the resend)", got, versionBefore)
	}
	if got := pusher.count(); got != pushesBefore {
		t.Errorf("push calls after the resend = %d, want %d (no push by the resend)", got, pushesBefore)
	}
	tvAfter, err := store.TaskView(ctxT(t), string(task.ID))
	if err != nil || tvAfter == nil {
		t.Fatalf("task view after the resend: %v, %v", tvAfter, err)
	}
	if tvAfter.State != a2acore.StateInputRequired {
		t.Errorf("durable task state after the resend = %q, want %q (unchanged by the resend)", tvAfter.State, a2acore.StateInputRequired)
	}
}

// TestDuplicateResendWhileFirstExecutionAlive pins the cross-instance
// reach of the same window: the held send is released while the first
// admission's execution is still alive on the first server (working,
// before the decision). The defense branch answers it with the stable
// busy refusal: no push, no new version, the durable state still
// working and the first server's live execution the only one. The
// answer decision then completes the task through that live execution
// and both servers end with no live execution.
func TestDuplicateResendWhileFirstExecutionAlive(t *testing.T) {
	store, path := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}
	pusher := &dupPushSender{}
	s := dupNewServer(t, store, auth, routes, pusher)
	c := dialClient(t, serveServer(t, s).URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "dw-ask-1", "question 1")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask 1 = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	waitLive(t, s, 0)

	// A second server on the same store whose interceptor holds the m-2
	// send right after its admission lookup (which finds nothing, the
	// first admission not committed yet).
	s2 := dupNewServer(t, store, auth, routes, pusher)
	hold := &dupHoldBefore{
		authInterceptor: newAuthInterceptor(s2.cfg).withCapabilities(&s2.card.Capabilities),
		messageID:       "m-2",
		entered:         make(chan struct{}),
		release:         make(chan struct{}),
	}
	released := false
	t.Cleanup(func() {
		if !released {
			close(hold.release)
		}
	})
	s2.handler = a2asrv.NewHandler(
		s2.executor,
		a2asrv.WithTaskStore(scopedTaskStore{store: store}),
		a2asrv.WithPushNotifications(store.PushConfigs(), s2.cfg.PushSender),
		a2asrv.WithCapabilityChecks(&s2.card.Capabilities),
		a2asrv.WithCallInterceptors(hold),
		a2asrv.WithLogger(s2.cfg.Logger),
	)
	rec := &pcRecorder{}
	c2, err := Dial(ctxT(t), serveServer(t, s2).URL, staticCredential{value: auth.token("caller-1")}, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c2.Close() })

	in := SendInput{TaskID: string(task.ID), ContextID: task.ContextID, Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "continuation"}
	type answer struct {
		res a2a.SendMessageResult
		err error
	}
	done := make(chan answer, 1)
	go func() {
		res, err := c2.Send(ctxT(t), in)
		done <- answer{res, err}
	}()
	awaitSignal(t, "held send after its admission lookup", hold.entered)

	// The same message is admitted through the first server and stays
	// working: its execution is still alive, before the decision.
	sendTask(t, c, in)
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	waitLive(t, s, 1)

	if _, err := c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: task.ID, URL: f1URL}); err != nil {
		t.Fatalf("create push config before the release: %v", err)
	}
	versionBefore := dupTaskVersion(t, path, string(task.ID))
	pushesBefore := pusher.count()
	ingressID := f1Count(t, path, `SELECT ingress_id FROM a2a_ingress WHERE message_id = ?`, "m-2")
	deliveriesBefore := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID)

	close(hold.release)
	released = true
	var ans answer
	select {
	case ans = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the held send did not answer within 10s of its release")
	}
	// The held send gets the canonical busy answer while the first
	// execution is still alive: the stable text, the 500 INTERNAL body,
	// no new version, no push and the durable state still working.
	if ans.err == nil || !errors.Is(ans.err, a2a.ErrInternalError) || ans.err.Error() != taskBusyText {
		state := ""
		if task, ok := ans.res.(*a2a.Task); ok {
			state = string(task.Status.State)
		}
		t.Errorf("held send = %T (state %s), %v, want the internal error %q and no result", ans.res, state, ans.err, taskBusyText)
	}
	wire := rec.wire()
	if wire.status != http.StatusInternalServerError {
		t.Errorf("busy status = %d, want 500 (body: %.120s)", wire.status, wire.body)
	} else if got := strings.TrimSpace(pcNorm(wire.body)); got != ctBusyBody {
		t.Errorf("busy body = %s, want %s", got, ctBusyBody)
	}
	if got := dupTaskVersion(t, path, string(task.ID)); got != versionBefore {
		t.Errorf("task version after the held send = %d, want %d (no write by the held send)", got, versionBefore)
	}
	if got := pusher.count() - pushesBefore; got != 0 {
		t.Errorf("pushes caused by the held send = %d, want 0 (no push by the held send)", got)
	}
	tv, err := store.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil || tv.State != a2acore.StateWorking {
		t.Errorf("durable task after the held send = %+v, %v, want %q", tv, err, a2acore.StateWorking)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress WHERE message_id = ?`, "m-2"); got != 1 {
		t.Errorf("ingress rows of m-2 = %d, want 1 (no second admission)", got)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_deliveries WHERE ingress_id = ?`, ingressID); got != deliveriesBefore {
		t.Errorf("deliveries of m-2 = %d, want %d (no second delivery)", got, deliveriesBefore)
	}
	// The first server's live execution stays the only one.
	waitLive(t, s, 1)
	waitLive(t, s2, 0)

	// The answer decision completes the task through the first
	// server's live execution; both servers end with no live execution
	// and the final event is pushed.
	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "dw-answer", "the answer")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply answer = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitStoredState(t, store, string(task.ID), a2acore.StateCompleted)
	waitTaskState(t, c, string(task.ID), a2a.TaskStateCompleted)
	waitLive(t, s, 0)
	waitLive(t, s2, 0)
	if got := pusher.count() - pushesBefore; got != 2 {
		t.Errorf("pushes after the answer = %d, want 2 (the decision's artifact and final events)", got)
	}
	if ev, ok := pusher.lastEvent().(*a2a.TaskStatusUpdateEvent); !ok || ev.Status.State != a2a.TaskStateCompleted {
		t.Errorf("pushed final event = %T, want the completed status update", pusher.lastEvent())
	}
}
