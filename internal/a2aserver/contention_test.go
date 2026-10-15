package a2aserver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

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
