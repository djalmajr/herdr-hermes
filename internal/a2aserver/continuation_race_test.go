package a2aserver

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// hookedTaskStore wraps the task store an SDK handler uses and runs a
// hook around Get and Update, so a test pins the order in which two
// writers touch one task without any timing. The wrapped store is the
// real one; the hooks only block or signal.
type hookedTaskStore struct {
	taskstore.Store
	afterGet     func(id a2a.TaskID)
	beforeUpdate func(req *taskstore.UpdateRequest)
	afterUpdate  func(req *taskstore.UpdateRequest)
}

// Get implements taskstore.Store.
func (h *hookedTaskStore) Get(ctx context.Context, id a2a.TaskID) (*taskstore.StoredTask, error) {
	st, err := h.Store.Get(ctx, id)
	if h.afterGet != nil {
		h.afterGet(id)
	}
	return st, err
}

// Update implements taskstore.Store.
func (h *hookedTaskStore) Update(ctx context.Context, req *taskstore.UpdateRequest) (taskstore.TaskVersion, error) {
	if h.beforeUpdate != nil {
		h.beforeUpdate(req)
	}
	v, err := h.Store.Update(ctx, req)
	if h.afterUpdate != nil {
		h.afterUpdate(req)
	}
	return v, err
}

// isWorkingEvent reports that the update persists the WORKING status
// event the executor yields right after the Submitted task.
func isWorkingEvent(req *taskstore.UpdateRequest) bool {
	ev, ok := req.Event.(*a2a.TaskStatusUpdateEvent)
	return ok && ev.Status.State == a2a.TaskStateWorking
}

// isHistoryAppend reports that the update only appends the incoming
// message to the task history (the SDK does it when it loads the
// execution context of a continuation).
func isHistoryAppend(req *taskstore.UpdateRequest) bool {
	_, ok := req.Event.(*a2a.Message)
	return ok
}

// newHookedServer builds one server like New does, then rebuilds its SDK
// handler over the hooked task store: the only difference to New is the
// task store the handler sees. The server still answers over the real
// store for everything the executor reads itself.
func newHookedServer(t *testing.T, store *a2astore.Store, auth *tableAuth, routes fakeRoutes, hooks *hookedTaskStore) *Server {
	t.Helper()
	s := newTestServer(t, store, auth, routes)
	hooks.Store = store
	s.handler = a2asrv.NewHandler(
		s.executor,
		a2asrv.WithTaskStore(hooks),
		a2asrv.WithPushNotifications(store.PushConfigs(), push.NewHTTPPushSender(&push.HTTPSenderConfig{Timeout: 10 * time.Second})),
		a2asrv.WithCapabilityChecks(&s.card.Capabilities),
		a2asrv.WithCallInterceptors(newAuthInterceptor(s.cfg)),
		a2asrv.WithLogger(s.cfg.Logger),
	)
	return s
}

// awaitSignal waits for one channel; a missing signal is a test failure
// (never a hang) so the order under test cannot silently degrade.
func awaitSignal(t *testing.T, name string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Errorf("timed out waiting for %s", name)
	}
}

// TestContinuationOnSecondServerRacesFirstExecutionWrite pins the
// failure seen on a slow runner for the continuation of a parked task.
// The first server answers its ReturnImmediately send right after it
// stored the Submitted task; the WORKING status event is stored by the
// still running execution after that answer. A second server on the
// same store that loads the task in that window and appends the
// continuation message loses the SDK optimistic version check, and the
// caller gets the "concurrent modification" error of the executor
// setup. The order is forced with store hooks: the second server's Get
// returns first, then the first server's WORKING update lands, then the
// second server's history update runs.
func TestContinuationOnSecondServerRacesFirstExecutionWrite(t *testing.T) {
	store1, path := newTestStore(t)
	store2, err := a2astore.OpenFile(path, a2astore.Options{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}

	var loaded, durable sync.Once
	secondLoaded := make(chan struct{})
	firstDurable := make(chan struct{})

	s1 := newHookedServer(t, store1, auth, routes, &hookedTaskStore{
		beforeUpdate: func(req *taskstore.UpdateRequest) {
			if isWorkingEvent(req) {
				awaitSignal(t, "the second server to load the task", secondLoaded)
			}
		},
		afterUpdate: func(req *taskstore.UpdateRequest) {
			if isWorkingEvent(req) {
				durable.Do(func() { close(firstDurable) })
			}
		},
	})
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")
	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 2, Seq: 1, MessageID: "m-1", Text: "first"})
	if task.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("the answer carries %s, want the submitted task the answer was cut from", task.Status.State)
	}

	s2 := newHookedServer(t, store2, auth, routes, &hookedTaskStore{
		afterGet: func(id a2a.TaskID) {
			if id == task.ID {
				loaded.Do(func() { close(secondLoaded) })
			}
		},
		beforeUpdate: func(req *taskstore.UpdateRequest) {
			if isHistoryAppend(req) {
				awaitSignal(t, "the first server to store the working event", firstDurable)
			}
		},
	})
	c2 := dialClient(t, serveServer(t, s2).URL, auth, "caller-1")

	_, err = c2.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-2", Text: "stale",
	})
	if err == nil {
		t.Fatal("continuation succeeded, want the concurrent modification of the executor setup")
	}
	t.Logf("caller sees: %v", err)
	for _, want := range []string{"failed to load exec ctx", "task message history update failed", "concurrent modification"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not carry %q", err, want)
		}
	}
	// Nothing was parked or half applied by the lost continuation.
	if s2.LiveExecutions() != 0 {
		t.Errorf("second server live executions = %d, want 0", s2.LiveExecutions())
	}
	stored, err := store2.TaskView(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("task view: %v", err)
	}
	if stored.State != a2acore.StateWorking {
		t.Errorf("stored state = %q, want working (the first server's event is the one that landed)", stored.State)
	}
}

// TestContinuationOnSameServerWhileExecutionWritesIsRefused is the
// single-server counterpart: a continuation that arrives while the
// task's own execution is still storing its WORKING event never reaches
// the store on the same server. The SDK execution manager refuses it
// because one execution per task is live, so the caller gets a
// deterministic refusal and not a version conflict, and the execution
// that is mid-write is unaffected.
func TestContinuationOnSameServerWhileExecutionWritesIsRefused(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}

	var hold sync.Once
	workingReached := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseWrite)

	s := newHookedServer(t, store, auth, routes, &hookedTaskStore{
		beforeUpdate: func(req *taskstore.UpdateRequest) {
			if isWorkingEvent(req) {
				hold.Do(func() { close(workingReached) })
				awaitSignal(t, "the test to release the working write", release)
			}
		},
	})
	c := dialClient(t, serveServer(t, s).URL, auth, "caller-1")
	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 2, Seq: 1, MessageID: "m-1", Text: "first"})
	awaitSignal(t, "the execution to reach its working write", workingReached)

	_, err := c.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 2, Seq: 2, MessageID: "m-2", Text: "second",
	})
	if err == nil {
		t.Fatal("continuation of a task whose execution is mid-write succeeded, want a refusal")
	}
	t.Logf("caller sees: %v", err)
	if strings.Contains(err.Error(), "concurrent modification") || strings.Contains(err.Error(), "exec ctx") {
		t.Errorf("error %q is the store version conflict, want the execution-in-progress refusal", err)
	}
	if !strings.Contains(err.Error(), "already in progress") {
		t.Errorf("error %q does not say the task execution is already in progress", err)
	}

	// The execution that was mid-write is not disturbed: it stores its
	// event and parks.
	releaseWrite()
	waitTaskState(t, c, string(task.ID), a2a.TaskStateWorking)
	if s.LiveExecutions() != 1 {
		t.Errorf("live executions = %d, want 1", s.LiveExecutions())
	}
}

// TestParkedExecutionIsNotDurablyWorkingWhenSendReturns guards the
// premise of the park wait in the continuation tests: when the send
// answer is back and the execution is registered as live, the stored
// task can still be the Submitted one, because the WORKING event is
// stored after the answer. Only the stored state reaching working
// tells that a second reader of the file sees the parked task.
func TestParkedExecutionIsNotDurablyWorkingWhenSendReturns(t *testing.T) {
	store1, path := newTestStore(t)
	store2, err := a2astore.OpenFile(path, a2astore.Options{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}

	var hold sync.Once
	workingReached := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseWrite)

	s1 := newHookedServer(t, store1, auth, routes, &hookedTaskStore{
		beforeUpdate: func(req *taskstore.UpdateRequest) {
			if isWorkingEvent(req) {
				hold.Do(func() { close(workingReached) })
				awaitSignal(t, "the test to release the working write", release)
			}
		},
	})
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")
	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 2, Seq: 1, MessageID: "m-1", Text: "first"})
	awaitSignal(t, "the execution to reach its working write", workingReached)

	// The execution is live and the answer is back, yet the file still
	// holds the submitted task.
	waitLive(t, s1, 1)
	tv, err := store2.TaskView(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("task view: %v", err)
	}
	if tv.State == a2acore.StateWorking {
		t.Fatalf("stored state = %q before the working write, want the submitted task", tv.State)
	}

	releaseWrite()
	waitStoredState(t, store2, string(task.ID), a2acore.StateWorking)
}
