package a2aserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// cgCancelText is the text the SDK answers a cancel of a task id the
// store does not hold (a wrapped a2a.ErrTaskNotFound, HTTP 404): the
// answer a cancel of a task the caller cannot see must be
// indistinguishable from.
const cgCancelText = "failed to cancel: cancelation failed: canceler setup failed: failed to load a task: task not found"

// cgMaskedBody is the wire body of that answer with the caller-supplied
// task id and the timestamp blanked (the SDK REST binding echoes the
// requested id in the error metadata of every cancel error), with the
// newline the REST binding ends it with.
const cgMaskedBody = `{"error":{"code":404,"status":"NOT_FOUND","message":"` + cgCancelText + `","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","domain":"a2a-protocol.org","metadata":{"taskId":"","timestamp":""},"reason":"TASK_NOT_FOUND"}]}}` + "\n"

// cgHold is the deterministic overlap seam. It blocks the first
// request that reaches the armed hold point until release: either an
// Authorize call of the injected Authorizer for the cancel action on a
// stored project that the match function selects (the scoped task read
// of one subject), or an error-level SDK log record whose message names
// a failed setup (the failing setup is still published in the SDK
// manager when it logs). The join signal is the SDK log record of a
// second cancel that waits for the cancelation in progress. Every wait
// ends with the test context.
type cgHold struct {
	done <-chan struct{}

	mu    sync.Mutex
	armed bool
	match func(a2acore.Subject, string) bool
	onLog bool

	entered chan struct{}
	joined  chan struct{}
	release chan struct{}

	joinOnce, releaseOnce sync.Once
}

// newCgHold builds a disarmed hold bound to a test context.
func newCgHold(done <-chan struct{}) *cgHold {
	return &cgHold{
		done:    done,
		entered: make(chan struct{}),
		joined:  make(chan struct{}),
		release: make(chan struct{}),
	}
}

// arm arms the hold: match selects an Authorize call (nil: none) and
// onLog also holds the first failed-setup error record.
func (h *cgHold) arm(match func(sub a2acore.Subject, project string) bool, onLog bool) {
	h.mu.Lock()
	h.armed, h.match, h.onLog = true, match, onLog
	h.mu.Unlock()
}

// takeAuthz reports whether this Authorize call takes the hold.
func (h *cgHold) takeAuthz(s a2acore.Subject, a a2acore.Action, project string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.armed || a != a2acore.ActionCancel || project == "" || h.match == nil || !h.match(s, project) {
		return false
	}
	h.armed = false
	return true
}

// takeLog reports whether this error record takes the hold.
func (h *cgHold) takeLog() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.armed || !h.onLog {
		return false
	}
	h.armed = false
	return true
}

// block announces that a request is held and waits for the release.
func (h *cgHold) block() {
	close(h.entered)
	select {
	case <-h.release:
	case <-h.done:
	}
}

// releaseNow releases the held request (idempotent).
func (h *cgHold) releaseNow() { h.releaseOnce.Do(func() { close(h.release) }) }

// markJoined records that a second cancel joined one in progress.
func (h *cgHold) markJoined() { h.joinOnce.Do(func() { close(h.joined) }) }

// cgAuthz is the Authorizer fixture: the default policy, counted, with
// the hold point.
type cgAuthz struct {
	hold  *cgHold
	calls atomic.Int64
}

// Authorize implements a2acore.Authorizer.
func (a *cgAuthz) Authorize(s a2acore.Subject, act a2acore.Action, project string) error {
	a.calls.Add(1)
	err := (a2acore.SubjectPolicy{}).Authorize(s, act, project)
	if a.hold.takeAuthz(s, act, project) {
		a.hold.block()
	}
	return err
}

// cgSink keeps every captured log record.
type cgSink struct {
	mu    sync.Mutex
	lines []string
}

// add keeps one formatted record.
func (s *cgSink) add(line string) {
	s.mu.Lock()
	s.lines = append(s.lines, line)
	s.mu.Unlock()
}

// mark returns the position after the records so far.
func (s *cgSink) mark() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lines)
}

// since returns the records after a mark, joined.
func (s *cgSink) since(mark int) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.lines[mark:], "\n")
}

// cgLogHandler is the capturing slog.Handler (every level enabled).
type cgLogHandler struct {
	sink  *cgSink
	hold  *cgHold
	attrs string
}

// Enabled implements slog.Handler.
func (cgLogHandler) Enabled(context.Context, slog.Level) bool { return true }

// Handle implements slog.Handler.
func (h cgLogHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Level.String() + " " + r.Message + h.attrs)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteString(" " + a.Key + "=" + a.Value.String())
		return true
	})
	h.sink.add(b.String())
	if strings.Contains(r.Message, "waiting for existing cancelation result") {
		h.hold.markJoined()
	}
	if r.Level >= slog.LevelError && strings.Contains(r.Message, "setup failed") && h.hold.takeLog() {
		h.hold.block()
	}
	return nil
}

// WithAttrs implements slog.Handler.
func (h cgLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	for _, a := range attrs {
		h.attrs += " " + a.Key + "=" + a.Value.String()
	}
	return h
}

// WithGroup implements slog.Handler.
func (h cgLogHandler) WithGroup(string) slog.Handler { return h }

// cgEnv is one server over a real store with the injected provider
// fixtures, the counted Authorizer with the hold point and the
// capturing logger.
type cgEnv struct {
	*idfxEnv
	ctx   context.Context
	hold  *cgHold
	authz *cgAuthz
	sink  *cgSink
}

// newCgEnv builds the server like newIdfxEnv does, with Config.Authz and
// Config.Logger injected.
func newCgEnv(t *testing.T, build func(*idfxProvider, *idfxRoutes) a2acore.Authenticator) *cgEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, path := newTestStore(t)
	base := &idfxEnv{t: t, store: store, dbPath: path, ids: newIdfxProvider(), routes: newIdfxRoutes(), logs: &idfxLog{}}
	e := &cgEnv{idfxEnv: base, ctx: ctx, hold: newCgHold(ctx.Done()), sink: &cgSink{}}
	e.authz = &cgAuthz{hold: e.hold}
	var auth a2acore.Authenticator
	if build == nil {
		auth = LocalOrBearer{Local: LocalOperator{}, Bearer: idfxBearer(base.ids, base.routes)}
	} else {
		auth = build(base.ids, base.routes)
	}
	srv, err := New(Config{
		Store:        store,
		Auth:         auth,
		Authz:        e.authz,
		Routes:       base.routes,
		BaseURL:      "http://127.0.0.1:7430",
		Version:      "test-1",
		LoopbackOnly: true,
		Logger:       slog.New(cgLogHandler{sink: e.sink, hold: e.hold}),
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	base.srv = srv
	base.ts = serveServer(t, srv)
	return e
}

// await waits for ch or fails the test when the test context ends.
func (e *cgEnv) await(ch <-chan struct{}, what string) {
	e.t.Helper()
	select {
	case <-ch:
	case <-e.ctx.Done():
		e.t.Fatalf("timed out waiting for %s", what)
	}
}

// cgActor is one caller: a recording client.
type cgActor struct {
	c   *Client
	rec *pcRecorder
}

// cgCall is one asynchronous cancel.
type cgCall struct {
	done chan struct{}
	task *a2a.Task
	err  error
	wire pcWire
}

// cancelAsync starts a cancel of id as the actor.
func (e *cgEnv) cancelAsync(a cgActor, id string) *cgCall {
	call := &cgCall{done: make(chan struct{})}
	go func() {
		defer close(call.done)
		call.task, call.err = a.c.Cancel(e.ctx, id)
		call.wire = a.rec.wire()
	}()
	return call
}

// cancel runs a cancel of id as the actor and waits for it.
func (e *cgEnv) cancel(a cgActor, id string) *cgCall {
	e.t.Helper()
	call := e.cancelAsync(a, id)
	e.await(call.done, "a cancel response")
	return call
}

// cgRandom is a fresh random hex token (a generated fixture value).
func cgRandom(t *testing.T) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random: %v", err)
	}
	return hex.EncodeToString(b)
}

// cgFix is the fixture: svc-a owns the parked org/secret task (with a
// canary body), svc-b is granted only org/other, svc-c is granted
// org/secret without being the owner and svc-a2 is the owner subject
// with a grant that no longer covers org/secret.
type cgFix struct {
	*cgEnv
	secret            *a2a.Task
	canary, job, sess string
	tokA, tokB, tokC  string
	tokR              string
	a, a2, b, c       cgActor
	leaks             []string
}

// cgSubjectOwner is the subject id of the task owner.
const cgSubjectOwner = "svc-a"

// newCgFix builds the fixture with the secret task parked and working.
func newCgFix(t *testing.T) *cgFix {
	t.Helper()
	e := newCgEnv(t, nil)
	e.routes.set("owner-ref-a", "org/secret", "org/public")
	e.routes.set("owner-ref-a-revoked", "org/public")
	e.routes.set("owner-ref-b", "org/other")
	f := &cgFix{cgEnv: e, canary: "canary-" + cgRandom(t), job: "job-" + cgRandom(t), sess: "sess-" + cgRandom(t)}
	f.tokA = e.ids.add(t, cgSubjectOwner, "owner-ref-a")
	f.tokR = e.ids.add(t, cgSubjectOwner, "owner-ref-a-revoked")
	f.tokB = e.ids.add(t, "svc-b", "owner-ref-b")
	f.a = f.dial(f.tokA)
	f.b = f.dial(f.tokB)
	f.secret = sendTask(t, f.a.c, SendInput{
		Project: "org/secret", Job: f.job, Session: f.sess, Epoch: 1, Seq: 1,
		MessageID: "m-" + cgRandom(t), Text: f.canary,
	})
	waitLive(t, e.srv, 1)
	waitTaskState(t, f.a.c, string(f.secret.ID), a2a.TaskStateWorking)
	// svc-c is granted the project of the task after it was routed to
	// its owner, so the owner reference of the project stays svc-a's.
	e.routes.set("owner-ref-c", "org/secret")
	f.tokC = e.ids.add(t, "svc-c", "owner-ref-c")
	f.c = f.dial(f.tokC)
	f.a2 = f.dial(f.tokR)
	f.leaks = []string{
		f.secret.ContextID, f.canary, f.job, f.sess,
		"org/secret", cgSubjectOwner, "history", "TASK_STATE_",
	}
	return f
}

// dial dials one recording client with the bearer token.
func (f *cgFix) dial(token string) cgActor {
	f.t.Helper()
	c, rec := pcDial(f.t, f.idfxEnv, token)
	return cgActor{c: c, rec: rec}
}

// holdOwner holds the owner: its first scoped read of the task for the
// cancel action (subject svc-a with the org/secret grant).
func (f *cgFix) holdOwner() {
	f.hold.arm(func(s a2acore.Subject, project string) bool {
		return s.ID == cgSubjectOwner && slices.Contains(s.Projects, "org/secret") && project == "org/secret"
	}, false)
}

// holdForeign holds a foreign request: a scoped read of the task by the
// owner subject whose grant no longer covers it, or the failed setup of
// a subject that is not the owner.
func (f *cgFix) holdForeign() {
	f.hold.arm(func(s a2acore.Subject, project string) bool {
		return s.ID == cgSubjectOwner && !slices.Contains(s.Projects, "org/secret") && project == "org/secret"
	}, true)
}

// assertMasked checks one refused cancel: TASK_NOT_FOUND with the exact
// cancel text, 404, the same wire body and nothing of the task.
func (f *cgFix) assertMasked(who string, call *cgCall) {
	f.t.Helper()
	if call.err == nil {
		f.t.Errorf("%s: cancel succeeded with %+v, want task not found", who, call.task)
		return
	}
	if !errors.Is(call.err, a2a.ErrTaskNotFound) {
		f.t.Errorf("%s: cancel error = %v, want a2a.ErrTaskNotFound", who, call.err)
	}
	if call.wire.status != http.StatusNotFound {
		f.t.Errorf("%s: status = %d, want 404 (body %q)", who, call.wire.status, call.wire.body)
	}
	if got := pcNorm(call.wire.body); got != cgMaskedBody {
		f.t.Errorf("%s: body = %q, want %q", who, got, cgMaskedBody)
	}
	// The caller-supplied id is echoed in the error metadata and is
	// blanked by pcNorm; nothing else of the task may appear.
	norm := pcNorm(call.wire.body)
	for _, leak := range f.leaks {
		if strings.Contains(norm, leak) {
			f.t.Errorf("%s: response body leaks %q: %s", who, leak, call.wire.body)
		}
	}
}

// assertCanceled checks one accepted owner cancel.
func (f *cgFix) assertCanceled(who string, call *cgCall) {
	f.t.Helper()
	if call.err != nil {
		f.t.Errorf("%s: cancel failed: %v (status %d, body %q)", who, call.err, call.wire.status, call.wire.body)
		return
	}
	if call.task == nil || call.task.Status.State != a2a.TaskStateCanceled || call.task.ID != f.secret.ID {
		f.t.Errorf("%s: cancel result = %+v, want the task canceled", who, call.task)
	}
}

// cgForeign is one foreign caller of the overlap scenarios.
type cgForeign struct {
	name string
	get  func(*cgFix) cgActor
}

var cgForeigns = []cgForeign{
	{"grant on another project", func(f *cgFix) cgActor { return f.b }},
	{"project grant without ownership", func(f *cgFix) cgActor { return f.c }},
	{"owner with the project grant revoked", func(f *cgFix) cgActor { return f.a2 }},
}

// TestCancelScopeForeignJoinsOwner: the owner cancel is held inside the
// window in which the SDK has published it; a foreign cancel arrives.
// The foreign caller must get the exact missing-task answer without
// anything of the task, and the owner must still get its canceled task.
func TestCancelScopeForeignJoinsOwner(t *testing.T) {
	for _, fc := range cgForeigns {
		t.Run(fc.name, func(t *testing.T) {
			f := newCgFix(t)
			id := string(f.secret.ID)
			f.holdOwner()
			owner := f.cancelAsync(f.a, id)
			f.await(f.hold.entered, "the owner cancel to reach the hold point")
			foreign := f.cancelAsync(fc.get(f), id)
			select {
			case <-f.hold.joined:
			case <-foreign.done:
			case <-f.ctx.Done():
				t.Fatal("timed out waiting for the foreign cancel to join or return")
			}
			f.hold.releaseNow()
			f.await(owner.done, "the owner response")
			f.await(foreign.done, "the foreign response")
			f.assertMasked(fc.name, foreign)
			f.assertCanceled("owner", owner)
			waitStoredState(t, f.store, id, a2acore.StateCanceled)
		})
	}
}

// TestCancelScopeForeignFirstOwnerArrives: the foreign cancel is held
// first; the owner arrives while it is held. The owner cancel must not
// share the foreign outcome: it still succeeds and the foreign caller is
// refused.
func TestCancelScopeForeignFirstOwnerArrives(t *testing.T) {
	for _, fc := range cgForeigns {
		t.Run(fc.name, func(t *testing.T) {
			f := newCgFix(t)
			id := string(f.secret.ID)
			f.holdForeign()
			foreign := f.cancelAsync(fc.get(f), id)
			select {
			case <-f.hold.entered:
			case <-foreign.done:
				// The foreign cancel never reached a hold point: it was
				// refused before the SDK (nothing to hold).
			case <-f.ctx.Done():
				t.Fatal("timed out waiting for the foreign cancel to reach a hold point or return")
			}
			owner := f.cancelAsync(f.a, id)
			select {
			case <-f.hold.joined:
			case <-owner.done:
			case <-f.ctx.Done():
				t.Fatal("timed out waiting for the owner cancel to join or return")
			}
			f.hold.releaseNow()
			f.await(foreign.done, "the foreign response")
			f.await(owner.done, "the owner response")
			f.assertMasked(fc.name, foreign)
			f.assertCanceled("owner", owner)
			waitStoredState(t, f.store, id, a2acore.StateCanceled)
		})
	}
}

// cgInbox is the JSON form of the inbox (tasks, deliveries).
func cgInbox(t *testing.T, e *cgEnv) string {
	t.Helper()
	items, err := e.store.Inbox(e.ctx, a2acore.InboxFilter{Limit: 100})
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	b, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("marshal inbox: %v", err)
	}
	return string(b)
}

// TestCancelScopeLoneCancelsAreIndistinguishable: a lone cancel of a
// task the caller cannot see (foreign grant, revoked grant, project
// grant without ownership) answers byte for byte what a cancel of an id
// that never existed answers, writes nothing and logs nothing of the
// task; the task stays parked and still cancels for its owner once the
// grant is back.
func TestCancelScopeLoneCancelsAreIndistinguishable(t *testing.T) {
	f := newCgFix(t)
	id := string(f.secret.ID)
	before := pcSnapshot(t, f.idfxEnv, id)
	inboxBefore := cgInbox(t, f.cgEnv)
	mark := f.sink.mark()

	f.assertMasked("missing id", f.cancel(f.c, pcMissingID))
	f.assertMasked("grant on another project", f.cancel(f.b, id))
	f.assertMasked("project grant without ownership", f.cancel(f.c, id))
	f.pcRevokeOwner()
	f.assertMasked("revoked grant", f.cancel(f.a, id))
	f.assertMasked("revoked grant, owner-subject client", f.cancel(f.a2, id))
	f.assertMasked("missing id as the revoked owner", f.cancel(f.a, pcMissingID))

	if after := pcSnapshot(t, f.idfxEnv, id); after != before {
		t.Errorf("a refused cancel changed the store: before %+v, after %+v", before, after)
	}
	if got := cgInbox(t, f.cgEnv); got != inboxBefore {
		t.Errorf("a refused cancel changed the inbox:\nbefore %s\nafter  %s", inboxBefore, got)
	}
	if f.srv.LiveExecutions() != 1 {
		t.Errorf("live executions = %d, want the task still parked (1)", f.srv.LiveExecutions())
	}
	// Log records name the requested id (the caller supplied it); they
	// must carry nothing else of the task.
	logged := f.sink.since(mark)
	for _, leak := range f.leaks {
		if strings.Contains(logged, leak) {
			t.Errorf("a log record of a refused cancel carries %q:\n%s", leak, logged)
		}
	}

	// The grant is back: the owner cancels its own task.
	f.pcRestoreOwner()
	owner := f.cancel(f.a, id)
	f.assertCanceled("owner after the grant was restored", owner)
	waitStoredState(t, f.store, id, a2acore.StateCanceled)
	waitLive(t, f.srv, 0)
}

// pcRevokeOwner drops org/secret from the owner's routing.
func (f *cgFix) pcRevokeOwner() { f.routes.set("owner-ref-a", "org/public") }

// pcRestoreOwner gives org/secret back to the owner.
func (f *cgFix) pcRestoreOwner() { f.routes.set("owner-ref-a", "org/secret", "org/public") }

// TestCancelScopeLoneOwnerCancel: a lone cancel of a visible task keeps
// the normal SDK result.
func TestCancelScopeLoneOwnerCancel(t *testing.T) {
	f := newCgFix(t)
	id := string(f.secret.ID)
	f.assertCanceled("owner", f.cancel(f.a, id))
	waitStoredState(t, f.store, id, a2acore.StateCanceled)
	waitLive(t, f.srv, 0)
}

// TestCancelScopeMissingIDTextMatchesSDK: the answer the interceptor
// gives for a task the caller cannot see is byte for byte (status and
// body) what the SDK, with no interceptor and its own store, answers for
// a cancel of an id it does not hold.
func TestCancelScopeMissingIDTextMatchesSDK(t *testing.T) {
	f := newCgFix(t)
	sdkStore := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return "sdk-user", nil },
	})
	h := a2asrv.NewHandler(&pcNoopExecutor{}, a2asrv.WithTaskStore(sdkStore))
	mux := http.NewServeMux()
	mux.Handle("GET "+a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(f.srv.Card()))
	mux.Handle("/", a2asrv.NewRESTHandler(h))
	plain := httptest.NewServer(mux)
	t.Cleanup(plain.Close)
	rec := &pcRecorder{}
	c, err := Dial(f.ctx, plain.URL, nil, &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial the plain SDK handler: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	if _, err := c.Cancel(f.ctx, pcMissingID); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("SDK cancel of a missing id = %v, want task not found", err)
	}
	sdk := rec.wire()
	if sdk.status != http.StatusNotFound {
		t.Fatalf("SDK missing-id status = %d, want 404", sdk.status)
	}
	if got := pcNorm(sdk.body); got != cgMaskedBody {
		t.Fatalf("SDK missing-id body = %q, want %q", got, cgMaskedBody)
	}
	ours := f.cancel(f.b, string(f.secret.ID)).wire
	if ours.status != sdk.status || pcNorm(ours.body) != pcNorm(sdk.body) {
		t.Errorf("invisible task answer differs from the SDK missing-id answer:\nours %d %s\nsdk  %d %s", ours.status, ours.body, sdk.status, sdk.body)
	}
}

// TestCancelScopeNilStoreFailsClosed: an interceptor without a task
// store answers a cancel as the same missing-task answer and never
// enters the SDK, even when the SDK's own store holds a cancelable task
// of that id.
func TestCancelScopeNilStoreFailsClosed(t *testing.T) {
	ex := &pcNoopExecutor{}
	sdkStore := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return a2acore.LocalSubjectID, nil },
	})
	if _, err := sdkStore.Create(ctxT(t), &a2a.Task{ID: "t-1", ContextID: "c-1", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}); err != nil {
		t.Fatalf("seed the SDK store: %v", err)
	}
	h := a2asrv.NewHandler(ex,
		a2asrv.WithTaskStore(sdkStore),
		a2asrv.WithCallInterceptors(newAuthInterceptor(Config{Auth: LocalOrBearer{Local: LocalOperator{}}, Authz: a2acore.SubjectPolicy{}})),
	)
	ctx, _ := a2asrv.NewCallContext(context.WithValue(ctxT(t), credentialKeyType{}, a2acore.Credential{RemoteAddr: "127.0.0.1:1"}), nil)
	_, err := h.CancelTask(ctx, &a2a.CancelTaskRequest{ID: "t-1"})
	if !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Fatalf("cancel without a store err = %v, want task not found", err)
	}
	if err.Error() != cgCancelText {
		t.Errorf("cancel without a store text = %q, want %q", err.Error(), cgCancelText)
	}
	if ex.ran {
		t.Error("the executor ran for a cancel without a store")
	}
}

// TestCancelScopeEmptyIDIsNotPrechecked: an empty id is not looked up;
// the SDK refuses it as invalid params exactly as it does without the
// precheck (the interceptor here has no store, which would answer task
// not found for any id it looked up).
func TestCancelScopeEmptyIDIsNotPrechecked(t *testing.T) {
	ex := &pcNoopExecutor{}
	h := a2asrv.NewHandler(ex,
		a2asrv.WithTaskStore(taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
			Authenticator: func(context.Context) (string, error) { return a2acore.LocalSubjectID, nil },
		})),
		a2asrv.WithCallInterceptors(newAuthInterceptor(Config{Auth: LocalOrBearer{Local: LocalOperator{}}, Authz: a2acore.SubjectPolicy{}})),
	)
	ctx, _ := a2asrv.NewCallContext(context.WithValue(ctxT(t), credentialKeyType{}, a2acore.Credential{RemoteAddr: "127.0.0.1:1"}), nil)
	if _, err := h.CancelTask(ctx, &a2a.CancelTaskRequest{ID: ""}); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Errorf("cancel with an empty id err = %v, want a2a.ErrInvalidParams from the SDK", err)
	}
	if ex.ran {
		t.Error("the executor ran for a cancel with an empty id")
	}
}

// TestCancelScopeStoreFailureIsFixedText: a store driver failure on the
// lookup answers the fixed server error and nothing of the driver.
func TestCancelScopeStoreFailureIsFixedText(t *testing.T) {
	f := newCgFix(t)
	id := string(f.secret.ID)
	if err := f.store.Close(); err != nil {
		t.Fatalf("close the store: %v", err)
	}
	for name, a := range map[string]cgActor{"owner": f.a, "foreign": f.b} {
		call := f.cancel(a, id)
		if !errors.Is(call.err, a2a.ErrServerError) || call.wire.status != http.StatusInternalServerError {
			t.Errorf("%s: err = %v status = %d body %q, want a server error (500)", name, call.err, call.wire.status, call.wire.body)
		}
		if !strings.Contains(call.wire.body, `"message":"a2a: task lookup failed"`) {
			t.Errorf("%s: body = %q, want the fixed task lookup text", name, call.wire.body)
		}
		for _, bad := range []string{"sql", "closed", "database", "sqlite", f.secret.ContextID, f.canary} {
			if strings.Contains(pcNorm(call.wire.body), bad) {
				t.Errorf("%s: body carries %q: %s", name, bad, call.wire.body)
			}
		}
	}
}

// TestCancelScopeIdentityFailureBeforeAnyLookup: an identity provider
// that failed or is absent is honest unavailability before any lookup:
// the Authorizer and the store are never called.
func TestCancelScopeIdentityFailureBeforeAnyLookup(t *testing.T) {
	t.Run("provider failure", func(t *testing.T) {
		f := newCgFix(t)
		id := string(f.secret.ID)
		f.ids.failWith(f.tokA, errors.New("backend down: "+f.canary))
		calls := f.authz.calls.Load()
		mark := f.sink.mark()
		call := f.cancel(f.a, id)
		if !errors.Is(call.err, a2a.ErrServerError) || !strings.Contains(call.wire.body, "a2a: identity provider unavailable") {
			t.Errorf("err = %v body %q, want the identity provider unavailable answer", call.err, call.wire.body)
		}
		if strings.Contains(call.wire.body, f.canary) || strings.Contains(f.sink.since(mark), f.canary) {
			t.Error("the provider error text reached a response or a log record")
		}
		if got := f.authz.calls.Load(); got != calls {
			t.Errorf("Authorizer called %d times, want 0 for a failed identity", got-calls)
		}
		if tv, err := f.store.TaskView(f.ctx, id); err != nil || tv == nil || tv.State != a2acore.StateWorking {
			t.Errorf("task view = %+v, %v, want the task untouched and working", tv, err)
		}
	})
	t.Run("no provider configured", func(t *testing.T) {
		e := newCgEnv(t, func(*idfxProvider, *idfxRoutes) a2acore.Authenticator {
			return LocalOrBearer{Local: LocalOperator{}}
		})
		// The store is closed: a lookup would answer the store failure,
		// not the identity answer.
		if err := e.store.Close(); err != nil {
			t.Fatalf("close the store: %v", err)
		}
		c, rec := pcDial(t, e.idfxEnv, "unverifiable")
		_, err := c.Cancel(e.ctx, pcMissingID)
		w := rec.wire()
		if !errors.Is(err, a2a.ErrServerError) || !strings.Contains(w.body, "a2a: identity provider unavailable") {
			t.Errorf("err = %v body %q, want the identity provider unavailable answer", err, w.body)
		}
		if got := e.authz.calls.Load(); got != 0 {
			t.Errorf("Authorizer called %d times, want 0", got)
		}
	})
}
