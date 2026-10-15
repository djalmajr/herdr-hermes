package a2aserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

func TestLiveAnswerDecision(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s, 1)

	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "d-1", "the answer text")
	applied, refused, err := s.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}

	got := waitTaskState(t, c, string(task.ID), a2a.TaskStateCompleted)
	if len(got.Artifacts) != 1 {
		t.Fatalf("artifacts = %+v, want one", got.Artifacts)
	}
	art := got.Artifacts[0]
	if art.ID != "answer-d-1" || art.Name != "answer" {
		t.Errorf("artifact = id %s name %s, want answer-d-1 / answer", art.ID, art.Name)
	}
	if got := art.Parts[0].Text(); got != "the answer text" {
		t.Errorf("artifact text = %q, want the decision text", got)
	}
	d := findDecision(t, store, string(task.ID), "d-1")
	if d.State != a2acore.DecisionApplied {
		t.Errorf("decision state = %s, want applied", d.State)
	}
	waitLive(t, s, 0)
}

func TestLiveAskThenContinuation(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s, 1)

	// The owner asks back: the task goes to input required.
	recordDecision(t, store, string(task.ID), a2acore.DecisionAsk, "d-1", "question one")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	got := waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
	if got.Status.Message == nil || got.Status.Message.Parts[0].Text() != "question one" {
		t.Errorf("status message = %+v, want question one", got.Status.Message)
	}
	// The parked execution has finished and been cleaned up: the SDK
	// only accepts a follow-up send once its internal execution is
	// unregistered (the store state alone can be visible a moment
	// earlier).
	waitLive(t, s, 0)

	// The caller continues on the same task and context: working again.
	res, err := c.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "answer to the question",
	})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	got, ok := res.(*a2a.Task)
	if !ok || got.Status.State != a2a.TaskStateWorking {
		t.Fatalf("continuation = %T %+v, want a working task", res, got)
	}
	waitLive(t, s, 1)

	// Then the owner answers: completed with the second artifact.
	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "d-2", "final answer")
	if applied, refused, err := s.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply answer = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	done := waitTaskState(t, c, string(task.ID), a2a.TaskStateCompleted)
	if len(done.Artifacts) != 1 || done.Artifacts[0].ID != "answer-d-2" {
		t.Errorf("artifacts = %+v, want answer-d-2", done.Artifacts)
	}
	// The history keeps the two user messages and the agent question
	// verbatim as data (the SDK also keeps agent status messages).
	have := map[string]bool{}
	for _, m := range done.History {
		for _, p := range m.Parts {
			have[p.Text()] = true
		}
	}
	for _, want := range []string{"first", "answer to the question", "question one"} {
		if !have[want] {
			t.Errorf("history missing the verbatim text %q (history = %+v)", want, done.History)
		}
	}
	waitLive(t, s, 0)
}

func TestLiveDeclineDecision(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s, 1)

	recordDecision(t, store, string(task.ID), a2acore.DecisionDecline, "d-1", "not doing this")
	applied, refused, err := s.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	got := waitTaskState(t, c, string(task.ID), a2a.TaskStateRejected)
	if got.Status.Message == nil || got.Status.Message.Parts[0].Text() != "not doing this" {
		t.Errorf("status message = %+v, want the decision text", got.Status.Message)
	}
	d := findDecision(t, store, string(task.ID), "d-1")
	if d.State != a2acore.DecisionApplied {
		t.Errorf("decision state = %s, want applied", d.State)
	}
	waitLive(t, s, 0)
}

func TestMessageToTerminalTaskRefused(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s, 1)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "d-1", "the answer")
	if _, _, err := s.ApplyPendingDecisions(ctxT(t)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	before := waitTaskState(t, c, string(task.ID), a2a.TaskStateCompleted)
	waitLive(t, s, 0)

	// A message to a terminal task is an SDK refusal and changes nothing.
	_, err := c.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "too late",
	})
	if !errors.Is(err, a2a.ErrUnsupportedOperation) {
		t.Fatalf("err = %v, want ErrUnsupportedOperation", err)
	}
	after, err := c.Get(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	bb, _ := json.Marshal(before)
	aa, _ := json.Marshal(after)
	if !bytes.Equal(bb, aa) {
		t.Errorf("task changed by a refused message:\nbefore %s\nafter  %s", bb, aa)
	}
}

func TestOutOfBandDecisionPush(t *testing.T) {
	// The receiver records the push on a 127.0.0.1 httptest server.
	var mu sync.Mutex
	var gotBody []byte
	var gotToken, gotAuth string
	receiver := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBody = append([]byte{}, b...)
		gotToken = r.Header.Get("A2A-Notification-Token")
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()
		rw.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	// The second store handle is a separate connection to the same
	// file. Both handles share one SecretProvider so the second handle
	// can resolve the push-config secret handles (an in-memory provider
	// is per-instance).
	secrets := a2acore.NewMemorySecrets()
	store1, path := openTestStore(t, a2astore.Options{Secrets: secrets})
	store2, err := a2astore.OpenFile(path, a2astore.Options{Secrets: secrets})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()
	auth := newTableAuth(a2acore.Subject{ID: "caller-1", Ref: "guardian-ref-1", Projects: []string{a2acore.AnyProject}, Actions: append([]a2acore.Action{}, a2acore.AllActions...)})
	routes := fakeRoutes{owner: "owner-ref-1", guardian: "guardian-ref-1"}

	s1 := newTestServer(t, store1, auth, routes)
	ts1 := serveServer(t, s1)
	c := dialClient(t, ts1.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s1, 1)
	// The send answers at Submitted (ReturnImmediately) while the execution
	// may still be storing Working; the out-of-band apply below needs the
	// stored Working state, so wait for it as a second process would.
	waitStoredState(t, store1, string(task.ID), a2acore.StateWorking)

	// The push config carries the canary token and credentials.
	const tokenCanary = "canary-push-0004"
	const credsCanary = "canary-creds-0004"
	saved, err := c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{
		TaskID: task.ID,
		URL:    receiver.URL,
		Token:  tokenCanary,
		Auth:   &a2a.PushAuthInfo{Scheme: "Bearer", Credentials: credsCanary},
	})
	if err != nil {
		t.Fatalf("create push config: %v", err)
	}
	if saved.Token != tokenCanary || saved.Auth == nil || saved.Auth.Credentials != credsCanary {
		t.Errorf("saved config = %+v, want the original secret values", saved)
	}
	recordDecision(t, store1, string(task.ID), a2acore.DecisionAnswer, "d-1", "oob answer")
	// The push below is sent only because the routing still grants the
	// project to the config's recorded reference (the guardian
	// reference): the gate re-reads the routing on the send.

	// A second server on the reopened store has no parked execution:
	// the decision applies out-of-band and pushes the final event.
	s2, err := New(Config{
		Store:      store2,
		Auth:       auth,
		Routes:     routes,
		BaseURL:    "http://127.0.0.1:7430",
		Version:    "test-1",
		PushSender: push.NewHTTPPushSender(&push.HTTPSenderConfig{Timeout: 10 * time.Second, AllowPrivateNetworks: true}),
	})
	if err != nil {
		t.Fatalf("build second server: %v", err)
	}
	applied, refused, err := s2.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(gotBody) > 0
		mu.Unlock()
		if got {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(gotBody) == 0 {
		t.Fatal("the receiver got no push notification")
	}
	if !strings.Contains(string(gotBody), string(task.ID)) {
		t.Errorf("push body = %s, want the task id", gotBody)
	}
	if !strings.Contains(string(gotBody), string(a2a.TaskStateCompleted)) {
		t.Errorf("push body = %s, want the completed state", gotBody)
	}
	if gotToken != tokenCanary {
		t.Errorf("token header = %q, want the config token", gotToken)
	}
	if gotAuth != "Bearer "+credsCanary {
		t.Errorf("auth header = %q, want the config credentials", gotAuth)
	}

	// The decision applied through the store.
	d := findDecision(t, store2, string(task.ID), "d-1")
	if d.State != a2acore.DecisionApplied {
		t.Errorf("decision state = %s, want applied", d.State)
	}
	// The secrets live only in the memory provider: the database bytes
	// carry the handles, never the canaries.
	db := dbFileBytes(t, path)
	if bytes.Contains(db, []byte(tokenCanary)) || bytes.Contains(db, []byte(credsCanary)) {
		t.Error("database bytes carry the push secret canaries")
	}
}

func TestCancelParkedTaskRefusesPendingDecision(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s, 1)
	recordDecision(t, store, string(task.ID), a2acore.DecisionAnswer, "d-1", "too late")

	got, err := c.Cancel(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("state = %s, want canceled", got.Status.State)
	}
	waitLive(t, s, 0)

	// The cancel made the task terminal: the store's terminal side
	// effects already refused the pending decision terminal, so the
	// dispatcher pass has nothing left to apply.
	applied, refused, err := s.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 0 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 0/0 nil (already refused at cancel)", applied, refused, err)
	}
	d := findDecision(t, store, string(task.ID), "d-1")
	if d.State != a2acore.DecisionRefused || d.Refusal != a2acore.RefuseTerminal {
		t.Errorf("decision = %+v, want refused terminal", d)
	}
}

func TestPushConfigCRUDThroughClient(t *testing.T) {
	store, path := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")

	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	const token = "canary-push-0005"
	const creds = "canary-creds-0005"

	// Create (store-generated id) and get (secrets resolved).
	created, err := c.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{
		TaskID: task.ID,
		URL:    "http://127.0.0.1:8000/hook",
		Token:  token,
		Auth:   &a2a.PushAuthInfo{Scheme: "Bearer", Credentials: creds},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID == "" {
		t.Error("created config has no store-generated id")
	}
	got, err := c.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: task.ID, ID: created.ID})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Token != token || got.Auth == nil || got.Auth.Credentials != creds {
		t.Errorf("got config = %+v, want the resolved secrets", got)
	}

	// List returns the one config.
	list, err := c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].ID != created.ID {
		t.Errorf("list = %+v, want the one config", list)
	}

	// Delete: gone afterwards. The client maps the server's not-found
	// to its own error (the store sentinel is not preserved across the
	// wire), so the assertion is on the not-found text.
	if err := c.client.DeleteTaskPushConfig(ctxT(t), &a2a.DeleteTaskPushConfigRequest{TaskID: task.ID, ID: created.ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: task.ID, ID: created.ID}); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("get after delete err = %v, want a not-found error", err)
	}

	// After the calls the database bytes carry the handles, never the
	// secret values.
	db := dbFileBytes(t, path)
	if bytes.Contains(db, []byte(token)) || bytes.Contains(db, []byte(creds)) {
		t.Error("database bytes carry the push secret canaries")
	}
}

func TestRefusedContinuationOfWorkingTaskParks(t *testing.T) {
	// The task is parked on the first server; after a restart the same
	// store is reopened by a second server that has no parked execution.
	store1, path := newTestStore(t)
	store2, err := a2astore.OpenFile(path, a2astore.Options{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()
	auth := newTableAuth(testSubject("caller-1"))
	routes := fakeRoutes{owner: "owner-ref-1"}

	s1 := newTestServer(t, store1, auth, routes)
	ts1 := serveServer(t, s1)
	c1 := dialClient(t, ts1.URL, auth, "caller-1")

	// The task is created at epoch 2 and parked on s1.
	task := sendTask(t, c1, SendInput{Project: "org/repo", Session: "s-1", Epoch: 2, Seq: 1, MessageID: "m-1", Text: "first"})
	waitLive(t, s1, 1)
	// The answer of a ReturnImmediately send is cut from the Submitted
	// task: the WORKING status event is stored by the still running
	// execution after it. The task counts as parked only once that
	// event is durable on the shared file, as a restart would find it;
	// a continuation that loads the task earlier loses the SDK version
	// check against that write (see continuation_race_test.go).
	waitStoredState(t, store2, string(task.ID), a2acore.StateWorking)

	// The second server on the reopened store has no parked execution.
	s2, err := New(Config{
		Store:   store2,
		Auth:    auth,
		Routes:  routes,
		BaseURL: "http://127.0.0.1:7430",
		Version: "test-1",
	})
	if err != nil {
		t.Fatalf("build second server: %v", err)
	}
	ts2 := serveServer(t, s2)
	c2 := dialClient(t, ts2.URL, auth, "caller-1")

	// A stale-epoch continuation to the WORKING task: refused, but the
	// task stays working, the refusal code is in the status-event
	// metadata and the execution parks.
	res, err := c2.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-2", Text: "stale",
	})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	got, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("continuation result = %T, want *a2a.Task", res)
	}
	if got.Status.State != a2a.TaskStateWorking {
		t.Errorf("state = %s, want working", got.Status.State)
	}
	if got.Metadata[a2acore.MetaRefusal] != a2acore.RefuseStaleEpoch {
		t.Errorf("task metadata = %v, want the refusal code", got.Metadata)
	}
	if got.Status.Message == nil || got.Status.Message.Parts[0].Text() != "refused: stale_epoch" {
		t.Errorf("status message = %+v, want the refusal text", got.Status.Message)
	}
	if s2.LiveExecutions() != 1 {
		t.Errorf("live executions = %d, want 1 (the refused continuation parks)", s2.LiveExecutions())
	}

	// A later owner decision is applied live.
	recordDecision(t, store2, string(task.ID), a2acore.DecisionAnswer, "d-1", "the answer")
	applied, refused, err := s2.ApplyPendingDecisions(ctxT(t))
	if err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c2, string(task.ID), a2a.TaskStateCompleted)
	waitLive(t, s2, 0)
}
