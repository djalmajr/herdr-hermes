package a2aserver

import (
	"errors"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// erReopen reopens the store file so a second server (the restart) has no
// parked execution, mirroring the restart tests.
func erReopen(t *testing.T, path string) *a2astore.Store {
	t.Helper()
	store2, err := a2astore.OpenFile(path, a2astore.Options{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { store2.Close() })
	return store2
}

// erVersion reads the durable task version through the shared file.
func erVersion(t *testing.T, path, taskID string) int {
	t.Helper()
	return f1Count(t, path, `SELECT version FROM a2a_tasks WHERE task_id = ?`, taskID)
}

// erFenceLastSeq reads the fence last_seq of one (subject, session).
func erFenceLastSeq(t *testing.T, path, subject, session string) int {
	t.Helper()
	return f1Count(t, path, `SELECT last_seq FROM a2a_fences WHERE subject = ? AND session = ?`, subject, session)
}

// TestEarlyRefusalWorkingTask: three continuations of a working task, with
// fresh message ids and the following sequences, are answered
// not_awaiting_caller before the SDK runs the task: the history size, the
// task version, the ingress rows and the fence do not change, no push goes
// out and no execution parks on the restarted server.
func TestEarlyRefusalWorkingTask(t *testing.T) {
	store, path := newTestStore(t)
	store2 := erReopen(t, path)
	auth := newTableAuth(dupRoutedSubject("caller-1"))
	routes := dupRoutes
	pusher := &dupPushSender{}
	s1 := dupNewServer(t, store, auth, routes, pusher)
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")

	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	waitStoredState(t, store, string(task.ID), a2acore.StateWorking)

	// The second server on the reopened store has no parked execution; a
	// push config is the target of the push an execution would send.
	s2 := dupNewServer(t, store2, auth, routes, pusher)
	c2 := dialClient(t, serveServer(t, s2).URL, auth, "caller-1")
	if _, err := c2.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: task.ID, URL: f1URL}); err != nil {
		t.Fatalf("create push config: %v", err)
	}

	tv, err := store2.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view: %v, %v", tv, err)
	}
	messagesBefore := tv.Messages
	versionBefore := erVersion(t, path, string(task.ID))
	ingressBefore := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress`)
	fenceBefore := erFenceLastSeq(t, path, "caller-1", "s-1")

	for i, msgID := range []string{"m-2", "m-3", "m-4"} {
		res, err := c2.Send(ctxT(t), SendInput{
			TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1",
			Session: "s-1", Epoch: 1, Seq: int64(2 + i), MessageID: msgID, Text: "continuation " + msgID,
		})
		if err != nil {
			t.Fatalf("continuation %s: %v", msgID, err)
		}
		got, ok := res.(*a2a.Task)
		if !ok {
			t.Fatalf("continuation %s = %T, want *a2a.Task", msgID, res)
		}
		if got.Status.State != a2a.TaskStateWorking {
			t.Errorf("continuation %s state = %s, want working", msgID, got.Status.State)
		}
		if refusalCode(res) != a2acore.RefuseNotAwaitingCaller {
			t.Errorf("continuation %s metadata = %v, want the refusal code %s", msgID, got.Metadata, a2acore.RefuseNotAwaitingCaller)
		}
		if got.Status.Message == nil || got.Status.Message.Parts[0].Text() != "refused: not_awaiting_caller" {
			t.Errorf("continuation %s status message = %+v, want the fixed refusal text", msgID, got.Status.Message)
		}
	}

	tv, err = store2.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view after: %v, %v", tv, err)
	}
	if tv.Messages != messagesBefore {
		t.Errorf("history size = %d, want %d (no refused message appended)", tv.Messages, messagesBefore)
	}
	if got := erVersion(t, path, string(task.ID)); got != versionBefore {
		t.Errorf("task version = %d, want %d (no write)", got, versionBefore)
	}
	if got := f1Count(t, path, `SELECT COUNT(*) FROM a2a_ingress`); got != ingressBefore {
		t.Errorf("ingress rows = %d, want %d (no admission)", got, ingressBefore)
	}
	if got := erFenceLastSeq(t, path, "caller-1", "s-1"); got != fenceBefore {
		t.Errorf("fence last_seq = %d, want %d (the refusals did not move it)", got, fenceBefore)
	}
	if got := pusher.count(); got != 0 {
		t.Errorf("push calls = %d, want 0 (no push by the refusals)", got)
	}
	if got := s2.LiveExecutions(); got != 0 {
		t.Errorf("live executions = %d, want 0 (the refusal is answered before the SDK runs)", got)
	}
}

// TestEarlyRefusalInputRequiredFence: on an input_required task a
// continuation with an epoch below the session's is answered stale_epoch and
// one that reuses the last sequence with a fresh id is answered replay, both
// before the SDK runs the task: the history, the version and the stored
// status message (the question) do not change and no push goes out.
func TestEarlyRefusalInputRequiredFence(t *testing.T) {
	store, path := newTestStore(t)
	store2 := erReopen(t, path)
	auth := newTableAuth(dupRoutedSubject("caller-1"))
	routes := dupRoutes
	pusher := &dupPushSender{}
	s1 := dupNewServer(t, store, auth, routes, pusher)
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")

	// The task is created at epoch 2 and parked in working; the owner then
	// asks, so the task awaits the caller and the fence is epoch 2, seq 1.
	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 2, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "er-ask-1", "question b")
	if applied, refused, err := s1.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateInputRequired)
	waitStoredState(t, store, string(task.ID), a2acore.StateInputRequired)

	s2 := dupNewServer(t, store2, auth, routes, pusher)
	c2 := dialClient(t, serveServer(t, s2).URL, auth, "caller-1")
	if _, err := c2.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: task.ID, URL: f1URL}); err != nil {
		t.Fatalf("create push config: %v", err)
	}

	tv, err := store2.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view: %v, %v", tv, err)
	}
	messagesBefore := tv.Messages
	versionBefore := erVersion(t, path, string(task.ID))

	// A continuation with an epoch below the session's is stale_epoch.
	res, err := c2.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1",
		Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-2", Text: "stale epoch",
	})
	if err != nil {
		t.Fatalf("stale continuation: %v", err)
	}
	if refusalCode(res) != a2acore.RefuseStaleEpoch {
		t.Fatalf("stale continuation = %T metadata %v, want the code %s", res, metaOf(res), a2acore.RefuseStaleEpoch)
	}
	// A continuation that reuses the last sequence with a fresh id is replay.
	res, err = c2.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1",
		Session: "s-1", Epoch: 2, Seq: 1, MessageID: "m-3", Text: "replay",
	})
	if err != nil {
		t.Fatalf("replay continuation: %v", err)
	}
	if refusalCode(res) != a2acore.RefuseReplay {
		t.Fatalf("replay continuation = %T metadata %v, want the code %s", res, metaOf(res), a2acore.RefuseReplay)
	}

	// Nothing was written: the history and the version are unchanged and the
	// stored status message is still the question.
	tv, err = store2.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view after: %v, %v", tv, err)
	}
	if tv.Messages != messagesBefore {
		t.Errorf("history size = %d, want %d (no refused message appended)", tv.Messages, messagesBefore)
	}
	if got := erVersion(t, path, string(task.ID)); got != versionBefore {
		t.Errorf("task version = %d, want %d (no write)", got, versionBefore)
	}
	after, err := c2.Get(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("get after the refusals: %v", err)
	}
	if after.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("durable state = %s, want input_required (unchanged)", after.Status.State)
	}
	if after.Status.Message == nil || after.Status.Message.Parts[0].Text() != "question b" {
		t.Errorf("stored status message = %+v, want the unchanged question", after.Status.Message)
	}
	if got := erFenceLastSeq(t, path, "caller-1", "s-1"); got != 1 {
		t.Errorf("fence last_seq = %d, want 1 (the refusals did not move it)", got)
	}
	if got := pusher.count(); got != 0 {
		t.Errorf("push calls = %d, want 0 (no push by the refusals)", got)
	}
	if got := s2.LiveExecutions(); got != 0 {
		t.Errorf("live executions = %d, want 0 (the refusal is answered before the SDK runs)", got)
	}
}

// metaOf returns the task metadata of a send result for the failure text.
func metaOf(res a2a.SendMessageResult) any {
	task, ok := res.(*a2a.Task)
	if !ok {
		return "<not a task>"
	}
	return task.Metadata
}

// refusalCode returns the refusal code of a send result's task metadata,
// or "" when the result is not a task or carries no refusal: a map[string]any
// lookup of an absent key is any(nil), which is not equal to "", so the
// presence of the code is checked through a typed assertion.
func refusalCode(res a2a.SendMessageResult) string {
	task, ok := res.(*a2a.Task)
	if !ok {
		return ""
	}
	code, _ := task.Metadata[a2acore.MetaRefusal].(string)
	return code
}

// TestEarlyRefusalDoesNotConsumeHistory is the regression of the DJA-350
// review: with a low MaxMessagesPerTask, several refused continuations on an
// input_required task do not consume the history bound, so a following
// legitimate continuation is admitted instead of refused loop_limit.
func TestEarlyRefusalDoesNotConsumeHistory(t *testing.T) {
	store, path := newTestStore(t)
	store2 := erReopen(t, path)
	auth := newTableAuth(dupRoutedSubject("caller-1"))
	routes := dupRoutes
	pusher := &dupPushSender{}
	lim := a2acore.Limits{MaxMessagesPerTask: 6}
	s1 := erServer(t, store, auth, routes, pusher, lim)
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")

	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "er-ask-2", "question c")
	if applied, refused, err := s1.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateInputRequired)

	s2 := erServer(t, store2, auth, routes, pusher, lim)
	c2 := dialClient(t, serveServer(t, s2).URL, auth, "caller-1")

	tv, err := store2.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view: %v, %v", tv, err)
	}
	messagesBefore := tv.Messages
	// The number of refused continuations that, if they were appended to the
	// history (the old behavior), would bring it to the bound of 6.
	numRefusals := 6 - messagesBefore
	if numRefusals < 1 {
		t.Fatalf("messages before the refusals = %d, want fewer than 6", messagesBefore)
	}
	for i := 0; i < numRefusals; i++ {
		// A replay: it reuses the last sequence (1) with a fresh id, so it is
		// refused by the fence and, before the change, would have been
		// appended to the history.
		res, err := c2.Send(ctxT(t), SendInput{
			TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1",
			Session: "s-1", Epoch: 1, Seq: 1, MessageID: "er-replay-" + string(rune('a'+i)), Text: "refused",
		})
		if err != nil {
			t.Fatalf("refused continuation %d: %v", i, err)
		}
		if refusalCode(res) != a2acore.RefuseReplay {
			t.Fatalf("refused continuation %d = %T metadata %v, want the code %s", i, res, metaOf(res), a2acore.RefuseReplay)
		}
	}
	// The refusals did not consume the history bound.
	tv, err = store2.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view after the refusals: %v, %v", tv, err)
	}
	if tv.Messages != messagesBefore {
		t.Fatalf("history size after the refusals = %d, want %d (the refusals did not consume the bound)", tv.Messages, messagesBefore)
	}
	// The legitimate continuation (the next sequence) is admitted, not
	// refused loop_limit.
	res, err := c2.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1",
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "er-legit", Text: "the answer",
	})
	if err != nil {
		t.Fatalf("legitimate continuation: %v", err)
	}
	if refusalCode(res) != "" {
		t.Fatalf("legitimate continuation = %T metadata %v, want it admitted (no loop_limit)", res, metaOf(res))
	}
	waitTaskState(t, c2, string(task.ID), a2a.TaskStateWorking)
}

// TestEarlyRefusalLetsLegitimateThrough: a legitimate continuation of an
// input_required task is not refused by the pre-evaluation; it follows the
// current path, is admitted and the task goes to working (its execution
// parks).
func TestEarlyRefusalLetsLegitimateThrough(t *testing.T) {
	store, path := newTestStore(t)
	store2 := erReopen(t, path)
	auth := newTableAuth(dupRoutedSubject("caller-1"))
	routes := dupRoutes
	pusher := &dupPushSender{}
	s1 := dupNewServer(t, store, auth, routes, pusher)
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")

	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "er-ask-3", "question d")
	if applied, refused, err := s1.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateInputRequired)

	s2 := dupNewServer(t, store2, auth, routes, pusher)
	c2 := dialClient(t, serveServer(t, s2).URL, auth, "caller-1")

	// The legitimate continuation (the next sequence) is admitted.
	res, err := c2.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1",
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "the answer",
	})
	if err != nil {
		t.Fatalf("legitimate continuation: %v", err)
	}
	if refusalCode(res) != "" {
		t.Fatalf("legitimate continuation = %T metadata %v, want it admitted (no refusal)", res, metaOf(res))
	}
	waitTaskState(t, c2, string(task.ID), a2a.TaskStateWorking)
	waitLive(t, s2, 1)
	// The message was appended to the history (the admission stands).
	tv, err := store2.TaskView(ctxT(t), string(task.ID))
	if err != nil || tv == nil {
		t.Fatalf("task view after: %v, %v", tv, err)
	}
	if tv.Messages == 0 {
		t.Errorf("history size = %d, want the admitted message appended", tv.Messages)
	}
}

// TestEarlyRefusalTerminalStillSDK: a terminal task is not pre-evaluated; the
// SDK still refuses a further continuation with unsupported operation, as
// today.
func TestEarlyRefusalTerminalStillSDK(t *testing.T) {
	store, path := newTestStore(t)
	store2 := erReopen(t, path)
	auth := newTableAuth(dupRoutedSubject("caller-1"))
	routes := dupRoutes
	pusher := &dupPushSender{}
	s1 := dupNewServer(t, store, auth, routes, pusher)
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")

	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "er-answer", "the answer")
	if applied, refused, err := s1.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply answer = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitStoredState(t, store, string(task.ID), a2acore.StateCompleted)

	s2 := dupNewServer(t, store2, auth, routes, pusher)
	c2 := dialClient(t, serveServer(t, s2).URL, auth, "caller-1")

	// A continuation on the terminal task is refused by the SDK, not by the
	// pre-evaluation: the caller gets the unsupported-operation answer.
	if _, err := c2.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1",
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "too late",
	}); err == nil || !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Fatalf("terminal continuation err = %v, want the SDK's unsupported operation", err)
	}
	if got := s2.LiveExecutions(); got != 0 {
		t.Errorf("live executions = %d, want 0 (the SDK refuses before it runs)", got)
	}
}

// TestEarlyRefusalDuplicateResendUnchanged: the resend of an already
// admitted continuation keeps the DJA-334 answer (the stored task, no
// refusal, no execution), which the pre-evaluation never reaches because the
// duplicate check runs first.
func TestEarlyRefusalDuplicateResendUnchanged(t *testing.T) {
	store, path := newTestStore(t)
	store2 := erReopen(t, path)
	auth := newTableAuth(dupRoutedSubject("caller-1"))
	routes := dupRoutes
	pusher := &dupPushSender{}
	s1 := dupNewServer(t, store, auth, routes, pusher)
	c1 := dialClient(t, serveServer(t, s1).URL, auth, "caller-1")

	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "er-ask-4", "question e")
	if applied, refused, err := s1.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateInputRequired)

	// The continuation m-2 is admitted on the first server.
	in := SendInput{TaskID: string(task.ID), ContextID: task.ContextID, Job: "job-1", Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "the answer"}
	if _, err := c1.Send(ctxT(t), in); err != nil {
		t.Fatalf("admit the continuation: %v", err)
	}
	waitTaskState(t, c1, string(task.ID), a2a.TaskStateWorking)
	waitStoredState(t, store, string(task.ID), a2acore.StateWorking)

	// The resend of the same message on the restarted server is answered by
	// the duplicate check (the stored task), not the pre-evaluation.
	s2 := dupNewServer(t, store2, auth, routes, pusher)
	c2 := dialClient(t, serveServer(t, s2).URL, auth, "caller-1")
	versionBefore := erVersion(t, path, string(task.ID))
	res, err := c2.Send(ctxT(t), in)
	if err != nil {
		t.Fatalf("resent the same continuation: %v", err)
	}
	resent, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("resent result = %T, want *a2a.Task", res)
	}
	if refusalCode(res) != "" {
		t.Errorf("resent metadata = %v, want no refusal (the DJA-334 stored-task answer)", resent.Metadata)
	}
	if got := erVersion(t, path, string(task.ID)); got != versionBefore {
		t.Errorf("task version after the resend = %d, want %d (no write)", got, versionBefore)
	}
	if got := s2.LiveExecutions(); got != 0 {
		t.Errorf("live executions = %d, want 0 (the resend is answered before the SDK runs)", got)
	}
}

// erServer builds one server over the store with the given push sender and
// limits, the way the early-refusal tests build theirs.
func erServer(t *testing.T, store *a2astore.Store, auth a2acore.Authenticator, routes a2acore.RouteSource, sender push.Sender, lim a2acore.Limits) *Server {
	t.Helper()
	s, err := New(Config{
		Store:      store,
		Auth:       auth,
		Routes:     routes,
		BaseURL:    "http://127.0.0.1:7430",
		Version:    "test-1",
		PushSender: sender,
		Limits:     lim,
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	return s
}
