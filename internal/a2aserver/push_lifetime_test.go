package a2aserver

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// The tests of this file prove the push lifetime policy: a push config
// records the routing reference of the subject that saved it, and the
// push gate re-evaluates that reference against the stored task row and
// the current routing before every send, live or out of band. A
// suppressed send returns nil, never calls the wrapped sender, counts
// itself and logs one record with the task id and the fixed reason and
// nothing else. Suppression is fail-closed for every config of a
// non-local subject without a recorded reference.

// plURL is the push config URL of the push lifetime tests: a discard
// port on loopback that is never dialed (the plSender records without
// network) and must never reach a log record or the database bytes.
const plURL = "http://127.0.0.1:9"

// plPushToken is the push config token canary of the push lifetime
// tests.
const plPushToken = "pl-push-token-canary"

// plLogCanaries are the fixed canaries of the push lifetime tests: the
// push token, the routing reference, the subject, the project and the
// webhook URL. The random bearer token is added by the caller.
var plLogCanaries = []string{plPushToken, "owner-ref-a", "svc-a", "org/secret", "127.0.0.1:9", "pc-1"}

// plSender is the recording push sender: it keeps one entry per call
// with the config id and the state of the event it was handed, and it
// never touches the network.
type plSender struct {
	mu    sync.Mutex
	calls []plCall
	// wake is closed and replaced on every call, so a waiter blocks on
	// a channel instead of polling.
	wake chan struct{}
}

type plCall struct {
	configID string
	state    a2a.TaskState
}

// SendPush implements push.Sender.
func (s *plSender) SendPush(_ context.Context, cfg *a2a.PushConfig, ev a2a.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, plCall{configID: cfg.ID, state: plEventState(ev)})
	if s.wake != nil {
		close(s.wake)
	}
	s.wake = make(chan struct{})
	return nil
}

// waitFor waits until cond holds over the recorded calls, bounded by a
// 10s deadline: every call closes the channel the waiter blocks on, so
// the wait needs no sleep.
func (s *plSender) waitFor(t *testing.T, what string, cond func([]plCall) bool) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		s.mu.Lock()
		if s.wake == nil {
			s.wake = make(chan struct{})
		}
		ok, n, wake := cond(s.calls), len(s.calls), s.wake
		s.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-wake:
		case <-timer.C:
			t.Fatalf("%s: not reached within 10s (%d push calls recorded)", what, n)
		}
	}
}

// callsFor returns the calls of one config id, in call order.
func (s *plSender) callsFor(configID string) []plCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []plCall
	for _, c := range s.calls {
		if c.configID == configID {
			out = append(out, c)
		}
	}
	return out
}

// count is the number of recorded calls.
func (s *plSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// plEventState is the task state carried by one event, unspecified for
// an event that carries no state.
func plEventState(ev a2a.Event) a2a.TaskState {
	switch e := ev.(type) {
	case *a2a.Task:
		return e.Status.State
	case *a2a.TaskStatusUpdateEvent:
		return e.Status.State
	}
	return a2a.TaskStateUnspecified
}

// plEnv is one server over a real store with the injected identity
// fixtures, a JSON log sink and the recording plSender: the server
// wraps the sender in the push gate like New does for every sender.
type plEnv struct {
	t      *testing.T
	store  *a2astore.Store
	obs    *a2astore.Store
	dbPath string
	srv    *Server
	ts     *httptest.Server
	ids    *idfxProvider
	routes *idfxRoutes
	logs   *idfxLog
	sender *plSender
}

// newPlEnv builds the env over the given store (dbPath must be the
// store's file, so the hygiene checks read the right bytes). The stored
// state waits read through a second handle (a separate connection, like
// a second process reading the same file): their queries must not
// interleave on the server's single pool connection between the working
// event's commit and the push List that follows it, or a config created
// in that window would be pushed for the working event.
func newPlEnv(t *testing.T, store *a2astore.Store, dbPath string, sender *plSender) *plEnv {
	t.Helper()
	e := &plEnv{t: t, store: store, dbPath: dbPath, ids: newIdfxProvider(), routes: newIdfxRoutes(), logs: &idfxLog{}, sender: sender}
	obs, err := a2astore.OpenFile(dbPath, a2astore.Options{})
	if err != nil {
		t.Fatalf("second store handle: %v", err)
	}
	t.Cleanup(func() { obs.Close() })
	e.obs = obs
	srv, err := New(Config{
		Store:        store,
		Auth:         LocalOrBearer{Local: LocalOperator{}, Bearer: idfxBearer(e.ids, e.routes)},
		Routes:       e.routes,
		BaseURL:      "http://127.0.0.1:7430",
		Version:      "test-1",
		LoopbackOnly: true,
		PushSender:   sender,
		Logger:       slog.New(slog.NewJSONHandler(e.logs, nil)),
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	e.srv = srv
	e.ts = serveServer(t, srv)
	return e
}

// client dials the SDK REST client with the bearer token ("" sends no
// header, the loopback local operator).
func (e *plEnv) client(token string) *Client {
	e.t.Helper()
	cred := idfxCred("")
	if token != "" {
		cred = idfxCred("Bearer " + token)
	}
	c, err := Dial(ctxT(e.t), e.ts.URL, cred, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		e.t.Fatalf("dial: %v", err)
	}
	e.t.Cleanup(func() { c.Close() })
	return c
}

// plCheckSuppressionRecords asserts that every JSON record with the
// suppression message in buf carries only the time, level, msg,
// task_id and reason keys (nothing else: never the config id, the
// reference, the subject, the project or the URL).
func plCheckSuppressionRecords(t *testing.T, buf string) {
	t.Helper()
	for _, line := range strings.Split(buf, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, `"msg":"a2a: push suppressed"`) {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Errorf("suppression record is not one JSON line: %s", line)
			continue
		}
		want := map[string]bool{"time": true, "level": true, "msg": true, "task_id": true, "reason": true}
		for k := range rec {
			if !want[k] {
				t.Errorf("suppression record carries the key %q: %s", k, line)
			}
		}
		if len(rec) != len(want) {
			missing := make([]string, 0, len(want))
			for k := range want {
				if _, ok := rec[k]; !ok {
					missing = append(missing, k)
				}
			}
			t.Errorf("suppression record lacks the keys %v: %s", missing, line)
		}
	}
}

// plCheckLog asserts the hygiene of the captured log: no canary
// appears anywhere in it and every JSON record with the suppression
// message carries only the time, level, msg, task_id and reason keys.
func (e *plEnv) plCheckLog(t *testing.T, canaries ...string) {
	t.Helper()
	buf := e.logs.String()
	for _, c := range canaries {
		if strings.Contains(buf, c) {
			t.Errorf("the log carries %q:\n%s", c, buf)
		}
	}
	plCheckSuppressionRecords(t, buf)
}

// plCheckDB asserts the database file and its WAL carry none of the
// canaries (typically the bearer and the push token).
func (e *plEnv) plCheckDB(t *testing.T, canaries ...string) {
	t.Helper()
	db := dbFileBytes(t, e.dbPath)
	for _, c := range canaries {
		if bytes.Contains(db, []byte(c)) {
			t.Errorf("the database bytes carry %q", c)
		}
	}
}

// plOwnerRef reads the owner_ref of one config row through a fresh
// connection ("" for NULL, Fatalf when the row is missing).
func plOwnerRef(t *testing.T, path, taskID, configID string) string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open for owner_ref: %v", err)
	}
	defer db.Close()
	var ref sql.NullString
	err = db.QueryRow(`SELECT owner_ref FROM a2a_push_configs WHERE task_id = ? AND config_id = ?`, taskID, configID).Scan(&ref)
	if err != nil {
		t.Fatalf("owner_ref: %v", err)
	}
	return ref.String
}

// plNullOwnerRef sets the owner_ref of one config row to NULL through a
// fresh writable connection (like f1Count, without mode=ro): the row of
// a config saved before the column existed, or by a call without a
// stamp.
func plNullOwnerRef(t *testing.T, path, taskID, configID string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open for the update: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE a2a_push_configs SET owner_ref = NULL WHERE task_id = ? AND config_id = ?`, taskID, configID); err != nil {
		t.Fatalf("set owner_ref NULL: %v", err)
	}
}

// plErrRoutes is the route source that fails: it proves the
// routing_unavailable path.
type plErrRoutes struct{}

// OwnerRef implements a2acore.RouteSource.
func (plErrRoutes) OwnerRef(string) (string, error) { return "", errors.New("routes unavailable") }

// GuardianRef implements a2acore.RouteSource.
func (plErrRoutes) GuardianRef() (string, error) { return "", errors.New("routes unavailable") }

// plBaseline is the push baseline of plNewTask: the sender's calls for
// pc-1 and the gate's suppressed count right after the working push of
// the first execution settled.
type plBaseline struct {
	calls      int
	suppressed int64
}

// plFirstSend is the first SendMessageRequest of one fresh task: the
// user message of the given input (the same metadata as Client.Send)
// and the push config pc-1 with the canary URL and token carried in
// the request, so the SDK stores the config before the execution
// starts (CreateExecutor) and the working push always reaches it.
func plFirstSend(in SendInput) *a2a.SendMessageRequest {
	if in.Job == "" {
		in.Job = "job-1"
	}
	meta := map[string]any{
		a2acore.MetaSession: in.Session,
		a2acore.MetaEpoch:   float64(in.Epoch),
		a2acore.MetaSeq:     float64(in.Seq),
	}
	if in.Project != "" {
		meta[a2acore.MetaProject] = in.Project
	}
	if in.Job != "" {
		meta[a2acore.MetaJob] = in.Job
	}
	return &a2a.SendMessageRequest{
		Message: &a2a.Message{
			ID:        in.MessageID,
			Role:      a2a.MessageRoleUser,
			Parts:     a2a.ContentParts{a2a.NewTextPart(in.Text)},
			TaskID:    a2a.TaskID(in.TaskID),
			ContextID: in.ContextID,
			Metadata:  meta,
		},
		Config: &a2a.SendMessageConfig{
			ReturnImmediately: true,
			PushConfig:        &a2a.PushConfig{ID: "pc-1", URL: plURL, Token: plPushToken},
		},
	}
}

// plWaitWorkingPush waits, with a deadline and no sleep (the sender
// wakes the wait on every call), until the sender records the working
// push of the given config: the first-send config is stored before the
// execution starts, so the working push always reaches it and this
// wait establishes the test's baseline.
func plWaitWorkingPush(t *testing.T, s *plSender, configID string) {
	t.Helper()
	s.waitFor(t, "the working push of "+configID+" (the first-send config must be stored before the execution)", func(calls []plCall) bool {
		for _, c := range calls {
			if c.configID == configID && c.state == a2a.TaskStateWorking {
				return true
			}
		}
		return false
	})
}

// plWaitCalls waits, with a deadline and no sleep (the sender wakes
// the wait on every call), until the sender has recorded at least want
// calls: the SDK sends the push right after it commits the event, so
// this bounds that window for a count assertion on a still-parked
// execution.
func plWaitCalls(t *testing.T, s *plSender, want int) {
	t.Helper()
	s.waitFor(t, fmt.Sprintf("at least %d push calls", want), func(calls []plCall) bool {
		return len(calls) >= want
	})
}

// plNewTask parks one task in the given project under the bearer
// subject svc-a / owner-ref-a, with the push config pc-1 (canary URL
// and token) carried by the first message, and waits until the working
// push settles (the baseline). It returns the task, the token, the
// client and the baseline: the caller's push assertions are relative
// to it, so none depends on the order of the working push. The config
// is stamped from the call's subject, so its stored owner_ref is
// owner-ref-a.
func plNewTask(t *testing.T, e *plEnv, project string) (*a2a.Task, string, *Client, plBaseline) {
	t.Helper()
	tok := e.ids.add(t, "svc-a", "owner-ref-a")
	c := e.client(tok)
	res, err := c.client.SendMessage(ctxT(t), plFirstSend(idfxSend(project, "s-1", "m-1")))
	if err != nil {
		t.Fatalf("send with the push config: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("send result = %T, want *a2a.Task", res)
	}
	plWaitWorkingPush(t, e.sender, "pc-1")
	return task, tok, c, plBaseline{calls: len(e.sender.callsFor("pc-1")), suppressed: e.srv.SuppressedPushes()}
}

// TestPushLifetimeLive proves the live path through the SDK sender:
// the project leaves the reference's routing before the decision and
// the parked execution's push is suppressed (the task stays
// input_required, never failed) with one counted and logged suppression
// and no call to the wrapped sender; without the revocation the same
// flow sends exactly one push.
func TestPushLifetimeLive(t *testing.T) {
	for name, revoke := range map[string]bool{"revoked": true, "control": false} {
		t.Run(name, func(t *testing.T) {
			sender := &plSender{}
			store, path := newTestStore(t)
			e := newPlEnv(t, store, path, sender)
			e.routes.set("owner-ref-a", "org/secret", "org/public")
			task, tok, c, base := plNewTask(t, e, "org/secret")
			if revoke {
				// The project leaves the reference's routing before
				// the decision.
				e.routes.set("owner-ref-a", "org/public")
			}
			recordDecision(t, e.store, string(task.ID), a2acore.DecisionAsk, "d-1", "question")
			if applied, refused, err := e.srv.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
				t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
			}
			waitStoredState(t, e.obs, string(task.ID), a2acore.StateInputRequired)
			waitLive(t, e.srv, 0)
			if revoke {
				if got := sender.callsFor("pc-1"); len(got) != base.calls {
					t.Fatalf("push calls after the baseline = %+v, want none (the reference no longer grants the project)", got[base.calls:])
				}
				if got := e.srv.SuppressedPushes() - base.suppressed; got != 1 {
					t.Fatalf("suppressed after the baseline = %d, want 1", got)
				}
				if !strings.Contains(e.logs.String(), `"reason":"not_granted"`) {
					t.Errorf("the log lacks the not_granted reason:\n%s", e.logs.String())
				}
			} else {
				got := sender.callsFor("pc-1")
				if len(got) != base.calls+1 || got[base.calls].state != a2a.TaskStateInputRequired {
					t.Fatalf("push calls after the baseline = %+v, want exactly one with the input-required state", got[base.calls:])
				}
				if got := e.srv.SuppressedPushes() - base.suppressed; got != 0 {
					t.Fatalf("suppressed after the baseline = %d, want 0", got)
				}
				// The task is still readable (the project is still
				// granted) and is input_required, not failed.
				waitTaskState(t, c, string(task.ID), a2a.TaskStateInputRequired)
			}
			// Hygiene: the suppressed or sent push never leaks a value
			// to the log, and the database carries the reference,
			// never a token.
			e.plCheckLog(t, append([]string{tok}, plLogCanaries...)...)
			e.plCheckDB(t, tok, plPushToken)
			if got := plOwnerRef(t, e.dbPath, string(task.ID), "pc-1"); got != "owner-ref-a" {
				t.Fatalf("owner_ref = %q, want owner-ref-a", got)
			}
		})
	}
}

// TestPushLifetimeOutOfBand proves the out-of-band path: the decision
// is applied by a second server on a second store handle (no parked
// execution) and the final event passes the same gate. Revoked, the
// second server's sender receives no call and the suppression is
// counted and logged; not revoked, it receives exactly one push with
// the completed state.
func TestPushLifetimeOutOfBand(t *testing.T) {
	for name, revoke := range map[string]bool{"revoked": true, "control": false} {
		t.Run(name, func(t *testing.T) {
			// The second store handle is a separate connection to the
			// same file. Both handles share one SecretProvider so the
			// second handle can resolve the push-config secret handles.
			secrets := a2acore.NewMemorySecrets()
			store1, path := openTestStore(t, a2astore.Options{Secrets: secrets})
			store2, err := a2astore.OpenFile(path, a2astore.Options{Secrets: secrets})
			if err != nil {
				t.Fatalf("reopen store: %v", err)
			}
			defer store2.Close()
			sender1 := &plSender{}
			e := newPlEnv(t, store1, path, sender1)
			e.routes.set("owner-ref-a", "org/secret", "org/public")
			task, tok, _, base := plNewTask(t, e, "org/secret")
			if revoke {
				e.routes.set("owner-ref-a", "org/public")
			}
			recordDecision(t, store1, string(task.ID), a2acore.DecisionAnswer, "d-1", "oob answer")
			sender2 := &plSender{}
			logs2 := &idfxLog{}
			// The second server has no parked execution: the decision
			// applies out of band and pushes the final event.
			s2, err := New(Config{
				Store:      store2,
				Auth:       LocalOrBearer{Local: LocalOperator{}, Bearer: idfxBearer(e.ids, e.routes)},
				Routes:     e.routes,
				BaseURL:    "http://127.0.0.1:7430",
				Version:    "test-1",
				PushSender: sender2,
				Logger:     slog.New(slog.NewJSONHandler(logs2, nil)),
			})
			if err != nil {
				t.Fatalf("build second server: %v", err)
			}
			applied, refused, err := s2.ApplyPendingDecisions(ctxT(t))
			if err != nil || applied != 1 || refused != 0 {
				t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
			}
			waitStoredState(t, store2, string(task.ID), a2acore.StateCompleted)
			if got := sender1.count(); got != base.calls {
				t.Fatalf("first server push calls after the baseline = %d, want 0 (the decision applied out of band)", got-base.calls)
			}
			if revoke {
				if got := sender2.callsFor("pc-1"); len(got) != 0 {
					t.Fatalf("second server push calls = %+v, want none (the reference no longer grants the project)", got)
				}
				if got := s2.SuppressedPushes(); got != 1 {
					t.Fatalf("suppressed = %d, want 1", got)
				}
				if !strings.Contains(logs2.String(), `"reason":"not_granted"`) {
					t.Errorf("the log lacks the not_granted reason:\n%s", logs2.String())
				}
			} else {
				got := sender2.callsFor("pc-1")
				if len(got) != 1 || got[0].state != a2a.TaskStateCompleted {
					t.Fatalf("second server push calls = %+v, want exactly one with the completed state", got)
				}
				if got := s2.SuppressedPushes(); got != 0 {
					t.Fatalf("suppressed = %d, want 0", got)
				}
			}
			// Hygiene.
			e.plCheckLog(t, append([]string{tok}, plLogCanaries...)...)
			{
				buf := logs2.String()
				for _, c := range append([]string{tok}, plLogCanaries...) {
					if strings.Contains(buf, c) {
						t.Errorf("the second server log carries %q:\n%s", c, buf)
					}
				}
				plCheckSuppressionRecords(t, buf)
			}
			e.plCheckDB(t, tok, plPushToken)
			if got := plOwnerRef(t, e.dbPath, string(task.ID), "pc-1"); got != "owner-ref-a" {
				t.Fatalf("owner_ref = %q, want owner-ref-a", got)
			}
		})
	}
}

// TestPushLifetimeToken proves the token policy: revoking the token
// alone refuses the next request made with it but does not stop the
// pushes of the subscriptions it created; a new token for the same
// subject and the same reference sees the task and the config (the
// rotation keeps the subscriptions); and the reference leaving the
// routing suppresses the next send.
func TestPushLifetimeToken(t *testing.T) {
	sender := &plSender{}
	store, path := newTestStore(t)
	e := newPlEnv(t, store, path, sender)
	e.routes.set("owner-ref-a", "org/secret")
	task, tokA, cA, base := plNewTask(t, e, "org/secret")

	// The token is revoked alone: the next request with it is refused.
	e.ids.revoke(tokA)
	if _, err := cA.Get(ctxT(t), string(task.ID)); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Fatalf("get with the revoked token = %v, want unauthenticated", err)
	}
	// ...but the subscription it created still pushes: the signature is
	// bound to the recorded reference, never to a token.
	recordDecision(t, e.store, string(task.ID), a2acore.DecisionAsk, "d-1", "question")
	if applied, refused, err := e.srv.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitStoredState(t, e.obs, string(task.ID), a2acore.StateInputRequired)
	waitLive(t, e.srv, 0)
	if got := sender.callsFor("pc-1"); len(got) != base.calls+1 || got[base.calls].state != a2a.TaskStateInputRequired {
		t.Fatalf("push calls after the baseline = %+v, want one input-required push despite the revoked token", got[base.calls:])
	}

	// A new token for the same subject and the same reference sees the
	// task and the config.
	tokB := e.ids.add(t, "svc-a", "owner-ref-a")
	cB := e.client(tokB)
	if _, err := cB.Get(ctxT(t), string(task.ID)); err != nil {
		t.Fatalf("get with the rotated token: %v", err)
	}
	list, err := cB.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	if err != nil || len(list) != 1 || list[0].ID != "pc-1" {
		t.Fatalf("list = %+v err=%v, want the one config pc-1", list, err)
	}

	// The caller continues while the routing still grants the project:
	// the working event is pushed.
	res, err := cB.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "answer to the question",
	})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	if got, ok := res.(*a2a.Task); !ok || got.Status.State != a2a.TaskStateWorking {
		t.Fatalf("continuation = %T %+v, want a working task", res, res)
	}
	waitLive(t, e.srv, 1)
	waitStoredState(t, e.obs, string(task.ID), a2acore.StateWorking)
	plWaitCalls(t, sender, base.calls+2)
	if got := sender.callsFor("pc-1"); len(got) != base.calls+2 || got[base.calls+1].state != a2a.TaskStateWorking {
		t.Fatalf("push calls after the baseline = %+v, want one more working push", got[base.calls:])
	}

	// The reference leaves the routing: the next send is suppressed.
	e.routes.set("owner-ref-a")
	recordDecision(t, e.store, string(task.ID), a2acore.DecisionAsk, "d-2", "question two")
	if applied, refused, err := e.srv.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitStoredState(t, e.obs, string(task.ID), a2acore.StateInputRequired)
	waitLive(t, e.srv, 0)
	if got := sender.callsFor("pc-1"); len(got) != base.calls+2 {
		t.Fatalf("push calls after the baseline = %+v, want none after the routing revocation", got[base.calls:])
	}
	if got := e.srv.SuppressedPushes() - base.suppressed; got != 1 {
		t.Fatalf("suppressed after the baseline = %d, want 1", got)
	}
	if !strings.Contains(e.logs.String(), `"reason":"not_granted"`) {
		t.Errorf("the log lacks the not_granted reason:\n%s", e.logs.String())
	}
	// Hygiene.
	e.plCheckLog(t, append([]string{tokA, tokB}, plLogCanaries...)...)
	e.plCheckDB(t, tokA, tokB, plPushToken)
}

// TestPushLifetimeLocalAndLegacy proves the two fail-closed edges of
// the gate: the local operator's config is always sent, with or
// without a recorded reference (it has none and the routing may be
// empty), and a config of any other subject whose owner_ref is NULL
// (saved before the column existed, or without a stamp) is never sent.
func TestPushLifetimeLocalAndLegacy(t *testing.T) {
	t.Run("local operator sends with empty routing", func(t *testing.T) {
		sender := &plSender{}
		store, path := newTestStore(t)
		e := newPlEnv(t, store, path, sender)
		// The routing is empty: the local operator's config is sent
		// anyway.
		c := e.client("")
		// The same first-send preparation as plNewTask, with the local
		// operator: the config carries no reference and the gate sends
		// it anyway.
		res, err := c.client.SendMessage(ctxT(t), plFirstSend(idfxSend("org/local", "s-1", "m-1")))
		if err != nil {
			t.Fatalf("send with the push config: %v", err)
		}
		task, ok := res.(*a2a.Task)
		if !ok {
			t.Fatalf("send result = %T, want *a2a.Task", res)
		}
		plWaitWorkingPush(t, sender, "pc-1")
		base := plBaseline{calls: len(sender.callsFor("pc-1")), suppressed: e.srv.SuppressedPushes()}
		recordDecision(t, e.store, string(task.ID), a2acore.DecisionAsk, "d-1", "question")
		if applied, refused, err := e.srv.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
			t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
		}
		waitStoredState(t, e.obs, string(task.ID), a2acore.StateInputRequired)
		waitLive(t, e.srv, 0)
		if got := sender.callsFor("pc-1"); len(got) != base.calls+1 || got[base.calls].state != a2a.TaskStateInputRequired {
			t.Fatalf("push calls after the baseline = %+v, want one input-required push", got[base.calls:])
		}
		if got := e.srv.SuppressedPushes() - base.suppressed; got != 0 {
			t.Fatalf("suppressed after the baseline = %d, want 0", got)
		}
		if got := plOwnerRef(t, e.dbPath, string(task.ID), "pc-1"); got != "" {
			t.Fatalf("owner_ref = %q, want NULL (the local operator has no reference)", got)
		}
		e.plCheckLog(t, plLogCanaries...)
		e.plCheckDB(t, plPushToken)
	})

	t.Run("legacy bearer config without reference is suppressed", func(t *testing.T) {
		sender := &plSender{}
		store, path := newTestStore(t)
		e := newPlEnv(t, store, path, sender)
		e.routes.set("owner-ref-a", "org/secret")
		task, tok, _, base := plNewTask(t, e, "org/secret")
		// The row is a legacy one: the reference is cleared like a
		// pre-migration row (the gate cannot re-evaluate it).
		plNullOwnerRef(t, e.dbPath, string(task.ID), "pc-1")
		recordDecision(t, e.store, string(task.ID), a2acore.DecisionAsk, "d-1", "question")
		if applied, refused, err := e.srv.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
			t.Fatalf("apply = %d/%d err=%v, want 1/0 nil", applied, refused, err)
		}
		waitStoredState(t, e.obs, string(task.ID), a2acore.StateInputRequired)
		waitLive(t, e.srv, 0)
		if got := sender.count(); got != base.calls {
			t.Fatalf("push calls after the baseline = %d, want 0 (the reference cannot be re-evaluated)", got-base.calls)
		}
		if got := e.srv.SuppressedPushes() - base.suppressed; got != 1 {
			t.Fatalf("suppressed after the baseline = %d, want 1", got)
		}
		if !strings.Contains(e.logs.String(), `"reason":"no_owner_reference"`) {
			t.Errorf("the log lacks the no_owner_reference reason:\n%s", e.logs.String())
		}
		e.plCheckLog(t, append([]string{tok}, plLogCanaries...)...)
		e.plCheckDB(t, tok, plPushToken)
	})
}

// TestPushLifetimeGate proves the gate failures in unit, with
// newPushGate directly: a route source error suppresses with
// routing_unavailable, a store that fails the read (a closed one)
// suppresses with lookup_failed, and a nil config suppresses with
// lookup_failed. In every case SendPush returns nil, the wrapped
// sender receives no call and the gate's counter rises.
func TestPushLifetimeGate(t *testing.T) {
	t.Run("routes error suppresses with routing_unavailable", func(t *testing.T) {
		sender := &plSender{}
		store, path := newTestStore(t)
		e := newPlEnv(t, store, path, sender)
		e.routes.set("owner-ref-a", "org/secret")
		task, _, _, base := plNewTask(t, e, "org/secret")
		cfg, err := e.store.PushConfigs().Get(ctxT(t), task.ID, "pc-1")
		if err != nil {
			t.Fatalf("Get config: %v", err)
		}
		logs := &idfxLog{}
		gate := newPushGate(sender, store.PushConfigs(), plErrRoutes{}, a2acore.SubjectPolicy{},
			slog.New(slog.NewJSONHandler(logs, nil)))
		if err := gate.SendPush(ctxT(t), cfg, nil); err != nil {
			t.Fatalf("SendPush = %v, want nil (a suppression is not an error)", err)
		}
		if got := sender.count(); got != base.calls {
			t.Fatalf("wrapped sender calls after the baseline = %d, want 0", got-base.calls)
		}
		if got := gate.suppressed.Load(); got != 1 {
			t.Fatalf("suppressed = %d, want 1", got)
		}
		if !strings.Contains(logs.String(), `"reason":"routing_unavailable"`) {
			t.Errorf("the log lacks the routing_unavailable reason:\n%s", logs.String())
		}
	})

	t.Run("closed store suppresses with lookup_failed", func(t *testing.T) {
		sender := &plSender{}
		store, path := newTestStore(t)
		e := newPlEnv(t, store, path, sender)
		e.routes.set("owner-ref-a", "org/secret")
		task, _, _, base := plNewTask(t, e, "org/secret")
		cfg, err := e.store.PushConfigs().Get(ctxT(t), task.ID, "pc-1")
		if err != nil {
			t.Fatalf("Get config: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		logs := &idfxLog{}
		gate := newPushGate(sender, store.PushConfigs(), e.routes, a2acore.SubjectPolicy{},
			slog.New(slog.NewJSONHandler(logs, nil)))
		if err := gate.SendPush(ctxT(t), cfg, nil); err != nil {
			t.Fatalf("SendPush = %v, want nil (a suppression is not an error)", err)
		}
		if got := sender.count(); got != base.calls {
			t.Fatalf("wrapped sender calls after the baseline = %d, want 0", got-base.calls)
		}
		if got := gate.suppressed.Load(); got != 1 {
			t.Fatalf("suppressed = %d, want 1", got)
		}
		if !strings.Contains(logs.String(), `"reason":"lookup_failed"`) {
			t.Errorf("the log lacks the lookup_failed reason:\n%s", logs.String())
		}
	})

	t.Run("nil config suppresses with lookup_failed", func(t *testing.T) {
		sender := &plSender{}
		logs := &idfxLog{}
		gate := newPushGate(sender, nil, plErrRoutes{}, a2acore.SubjectPolicy{},
			slog.New(slog.NewJSONHandler(logs, nil)))
		if err := gate.SendPush(ctxT(t), nil, nil); err != nil {
			t.Fatalf("SendPush = %v, want nil (a suppression is not an error)", err)
		}
		if got := sender.count(); got != 0 {
			t.Fatalf("wrapped sender calls = %d, want 0", got)
		}
		if got := gate.suppressed.Load(); got != 1 {
			t.Fatalf("suppressed = %d, want 1", got)
		}
		if !strings.Contains(logs.String(), `"reason":"lookup_failed"`) {
			t.Errorf("the log lacks the lookup_failed reason:\n%s", logs.String())
		}
	})
}
