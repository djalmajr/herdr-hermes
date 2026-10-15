package a2aserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// pcMissingID is a well-formed task id that is never created.
const pcMissingID = "01a00000-0000-7000-8000-000000000000"

// pcMissingBody is the wire body (timestamp blanked) the idle
// missing-task path rendered before the pre-check: the invisible and the
// missing id must keep rendering exactly this.
const pcMissingBody = `{"error":{"code":404,"status":"NOT_FOUND","message":"executor setup failed: failed to load exec ctx: task loading failed: task not found","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","domain":"a2a-protocol.org","metadata":{"timestamp":""},"reason":"TASK_NOT_FOUND"}]}}`

// pcInProgress is the SDK refusal text of a continuation on a task
// whose execution is still parked; the visible parked task now gets the
// stable busy answer instead, so it is a forbidden substring of every
// answer the busy rewrite covers.
const pcInProgress = "task execution is already in progress"

// pcWire is what one response looked like on the wire.
type pcWire struct {
	status int
	body   string
}

// pcRecorder is a RoundTripper that keeps the status and body of the
// last response, so a test sees the wire form the SDK client decoded.
type pcRecorder struct {
	mu   sync.Mutex
	last pcWire
}

// RoundTrip implements http.RoundTripper.
func (r *pcRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return res, err
	}
	b, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		return nil, err
	}
	res.Body = io.NopCloser(bytes.NewReader(b))
	r.mu.Lock()
	r.last = pcWire{status: res.StatusCode, body: string(b)}
	r.mu.Unlock()
	return res, nil
}

// wire returns the last recorded response.
func (r *pcRecorder) wire() pcWire {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// pcDial dials one recording SDK client with the bearer token.
func pcDial(t *testing.T, e *idfxEnv, token string) (*Client, *pcRecorder) {
	t.Helper()
	rec := &pcRecorder{}
	c, err := Dial(ctxT(t), e.ts.URL, idfxCred("Bearer "+token), &http.Client{Timeout: 10 * time.Second, Transport: rec})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, rec
}

// pcBlank blanks the values that legitimately differ between two
// refusals: the second-resolution error timestamp, a task id value in
// the error metadata and the per-event id line of an event stream.
var pcBlank = []*regexp.Regexp{
	regexp.MustCompile(`"timestamp":"[^"]*"`),
	regexp.MustCompile(`"taskId":"[^"]*"`),
	regexp.MustCompile(`(?m)^id: .*$`),
}

// pcNorm returns body with the legitimately differing values blanked.
func pcNorm(body string) string {
	for _, re := range pcBlank {
		body = re.ReplaceAllStringFunc(body, func(m string) string {
			if strings.HasPrefix(m, "id: ") {
				return "id: "
			}
			return m[:strings.LastIndex(m, `:"`)] + `:""`
		})
	}
	return body
}

// pcFix is one parked-continuation fixture: subject A owns org/secret
// and org/public and has one parked task in org/secret; subject B owns
// org/other.
type pcFix struct {
	e      *idfxEnv
	tokA   string
	tokB   string
	cA, cB *Client
	recA   *pcRecorder
	recB   *pcRecorder
	secret *a2a.Task
}

// pcSetup builds the fixture with the secret task parked.
func pcSetup(t *testing.T) *pcFix {
	t.Helper()
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-a", "org/secret", "org/public")
	e.routes.set("owner-ref-b", "org/other")
	f := &pcFix{e: e}
	f.tokA = e.ids.add(t, "svc-a", "owner-ref-a")
	f.tokB = e.ids.add(t, "svc-b", "owner-ref-b")
	f.cA, f.recA = pcDial(t, e, f.tokA)
	f.cB, f.recB = pcDial(t, e, f.tokB)
	f.secret = sendTask(t, f.cA, idfxSend("org/secret", "s-1", "m-1"))
	waitLive(t, e.srv, 1)
	// The executor stores the working status after the send returns; a
	// store snapshot is only stable once the parked task reached it.
	waitTaskState(t, f.cA, string(f.secret.ID), a2a.TaskStateWorking)
	return f
}

// pcRevoke drops org/secret from A's routing; A keeps org/public.
func (f *pcFix) pcRevoke() { f.e.routes.set("owner-ref-a", "org/public") }

// pcRestore gives org/secret back to A.
func (f *pcFix) pcRestore() { f.e.routes.set("owner-ref-a", "org/secret", "org/public") }

// pcContinue sends one continuation on taskID as the recorded client.
func pcContinue(t *testing.T, c *Client, rec *pcRecorder, taskID, contextID, msgID string) (pcWire, error) {
	t.Helper()
	_, err := c.Send(ctxT(t), SendInput{
		TaskID: taskID, ContextID: contextID,
		Session: "s-pc", Epoch: 1, Seq: 2, MessageID: msgID, Text: "continuation probe",
	})
	return rec.wire(), err
}

// pcStore is the durable state a refused continuation must not touch.
type pcStore struct {
	version, messages                      int
	state                                  string
	ingress, fences, deliveries, decisions int
	pushConfigs, tasks                     int
}

// pcSnapshot reads the durable state of one task and the counts of the
// tables a continuation would write.
func pcSnapshot(t *testing.T, e *idfxEnv, taskID string) pcStore {
	t.Helper()
	tv, err := e.store.TaskView(ctxT(t), taskID)
	if err != nil || tv == nil {
		t.Fatalf("task view %s = %v, %v", taskID, tv, err)
	}
	return pcStore{
		version:     f1Count(t, e.dbPath, `SELECT version FROM a2a_tasks WHERE task_id = ?`, taskID),
		messages:    tv.Messages,
		state:       tv.State,
		ingress:     f1Count(t, e.dbPath, `SELECT COUNT(*) FROM a2a_ingress`),
		fences:      f1Count(t, e.dbPath, `SELECT COUNT(*) FROM a2a_fences`),
		deliveries:  f1Count(t, e.dbPath, `SELECT COUNT(*) FROM a2a_deliveries`),
		decisions:   f1Count(t, e.dbPath, `SELECT COUNT(*) FROM a2a_decisions`),
		pushConfigs: f1Count(t, e.dbPath, `SELECT COUNT(*) FROM a2a_push_configs`),
		tasks:       f1Count(t, e.dbPath, `SELECT COUNT(*) FROM a2a_tasks`),
	}
}

// TestParkedContinuationInvisibleIsTaskNotFound: a continuation on a
// task the caller cannot see is answered exactly like a continuation on
// a missing id while the task's execution is still parked in the SDK:
// the revoked owner and a foreign subject learn nothing about the
// task, the SDK execution manager is never entered and nothing is
// written.
func TestParkedContinuationInvisibleIsTaskNotFound(t *testing.T) {
	f := pcSetup(t)
	f.pcRevoke()
	secretID := string(f.secret.ID)
	before := pcSnapshot(t, f.e, secretID)

	for _, withContext := range []bool{true, false} {
		name := "with the real context id"
		contextID := f.secret.ContextID
		if !withContext {
			name = "without a context id"
			contextID = ""
		}
		t.Run(name, func(t *testing.T) {
			missing, missingErr := pcContinue(t, f.cA, f.recA, pcMissingID, contextID, "pc-missing-"+name)
			if !errors.Is(missingErr, a2a.ErrTaskNotFound) || missing.status != http.StatusNotFound {
				t.Fatalf("missing id = %d %q err=%v, want 404 task not found (setup)", missing.status, missing.body, missingErr)
			}
			if got := strings.TrimSpace(pcNorm(missing.body)); got != pcMissingBody {
				t.Fatalf("missing id body = %s, want the unchanged idle body %s", got, pcMissingBody)
			}
			for _, tc := range []struct {
				name string
				c    *Client
				rec  *pcRecorder
			}{
				{"revoked owner", f.cA, f.recA},
				{"foreign subject", f.cB, f.recB},
			} {
				got, err := pcContinue(t, tc.c, tc.rec, secretID, contextID, "pc-"+tc.name+"-"+name)
				if !errors.Is(err, a2a.ErrTaskNotFound) {
					t.Errorf("%s: err = %v, want task not found", tc.name, err)
				}
				if got.status != http.StatusNotFound {
					t.Errorf("%s: status = %d (%s), want 404", tc.name, got.status, got.body)
				}
				for _, want := range []string{`"status":"NOT_FOUND"`, `"reason":"TASK_NOT_FOUND"`, `task loading failed: task not found`} {
					if !strings.Contains(got.body, want) {
						t.Errorf("%s: body %s lacks %s", tc.name, got.body, want)
					}
				}
				if pcNorm(got.body) != pcNorm(missing.body) {
					t.Errorf("%s: body differs from the missing id's:\n got %s\nwant %s", tc.name, got.body, missing.body)
				}
				for _, leak := range []string{secretID, f.secret.ContextID, "org/secret", "identity probe", "continuation probe", f2PushToken} {
					if strings.Contains(got.body, leak) {
						t.Errorf("%s: body carries %q: %s", tc.name, leak, got.body)
					}
				}
			}
		})
	}

	// A subject that holds the project grant but is not the owner of the
	// task is masked by the owner, not only by the project.
	t.Run("foreign subject holding the project grant", func(t *testing.T) {
		f.e.routes.set("owner-ref-b", "org/other", "org/secret")
		defer f.e.routes.set("owner-ref-b", "org/other")
		missing, _ := pcContinue(t, f.cB, f.recB, pcMissingID, f.secret.ContextID, "pc-b-grant-missing")
		got, err := pcContinue(t, f.cB, f.recB, secretID, f.secret.ContextID, "pc-b-grant")
		if !errors.Is(err, a2a.ErrTaskNotFound) || got.status != http.StatusNotFound || pcNorm(got.body) != pcNorm(missing.body) {
			t.Errorf("granted foreign subject = %d %s err=%v, want the missing id's %d %s", got.status, got.body, err, missing.status, missing.body)
		}
	})

	// The refusals left the parked task and the store untouched.
	waitLive(t, f.e.srv, 1)
	if after := pcSnapshot(t, f.e, secretID); after != before {
		t.Errorf("store changed by refused continuations:\n before %+v\n after  %+v", before, after)
	}

	// Restoring the grant makes the original task visible again; the
	// owner then gets the stored task with the refusal for the working
	// parked task (the pre-evaluation answers it before any execution).
	f.pcRestore()
	if got, err := f.cA.Get(ctxT(t), secretID); err != nil || got.ID != f.secret.ID {
		t.Fatalf("get after restore = %+v, %v", got, err)
	}
	res, err := pcContinue(t, f.cA, f.recA, secretID, f.secret.ContextID, "pc-restored")
	if err != nil {
		t.Errorf("continuation after restore err = %v, want the stored task with the refusal", err)
	}
	if res.status != http.StatusOK {
		t.Errorf("continuation after restore status = %d (%s), want 200", res.status, res.body)
	}
	if !strings.Contains(res.body, a2acore.RefuseNotAwaitingCaller) {
		t.Errorf("continuation after restore body %s lacks the refusal code", res.body)
	}
	waitLive(t, f.e.srv, 1)
}

// TestParkedContinuationStoreFailureIsFixedServerError: a lookup that
// fails for a reason other than not-found is a fixed-text server error,
// never the store error, and the SDK is not entered.
func TestParkedContinuationStoreFailureIsFixedServerError(t *testing.T) {
	f := pcSetup(t)
	if err := f.e.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	got, err := pcContinue(t, f.cA, f.recA, string(f.secret.ID), f.secret.ContextID, "pc-store-down")
	if !errors.Is(err, a2a.ErrServerError) || errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("err = %v, want a server error", err)
	}
	if got.status != http.StatusInternalServerError || !strings.Contains(got.body, "a2a: task lookup failed") {
		t.Errorf("wire = %d %s, want 500 with the fixed text", got.status, got.body)
	}
	for _, leak := range []string{"closed", "sql", string(f.secret.ID), pcInProgress} {
		if strings.Contains(got.body, leak) {
			t.Errorf("body carries %q: %s", leak, got.body)
		}
	}
}

// TestParkedContinuationVisibleIsBusy: the controls on the same kind
// of fixture: a still-granted working parked task is answered by the
// pre-evaluation with the stored task and the refusal (the task is not
// awaiting the caller) and a still-granted task is readable.
func TestParkedContinuationVisibleIsBusy(t *testing.T) {
	f := pcSetup(t)
	public := sendTask(t, f.cA, idfxSend("org/public", "s-2", "m-2"))
	waitLive(t, f.e.srv, 2)
	f.pcRevoke()

	if got, err := f.cA.Get(ctxT(t), string(public.ID)); err != nil || got.ID != public.ID {
		t.Fatalf("get of the still-granted task = %+v, %v", got, err)
	}
	got, err := pcContinue(t, f.cA, f.recA, string(public.ID), public.ContextID, "pc-public")
	if err != nil {
		t.Fatalf("visible parked continuation err = %v, want the stored task with the refusal", err)
	}
	if got.status != http.StatusOK {
		t.Errorf("visible parked continuation status = %d (%s), want 200", got.status, got.body)
	}
	if !strings.Contains(got.body, a2acore.RefuseNotAwaitingCaller) {
		t.Errorf("visible parked continuation body %s lacks the refusal code", got.body)
	}
	waitLive(t, f.e.srv, 2)
}

// TestParkedContinuationNewTaskSendIsNotLookedUp: a send without a task
// id does no lookup and keeps the first-message push config flow.
func TestParkedContinuationNewTaskSendIsNotLookedUp(t *testing.T) {
	e := newF1Env(t, testSubject("caller-1"))
	c := f1Dial(t, e.ts, e.auth, "caller-1", nil)
	res, err := c.client.SendMessage(ctxT(t), f1Request("org/repo", "s-1", "m-1"))
	if err != nil {
		t.Fatalf("new-task send with push config: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("send result = %T, want *a2a.Task", res)
	}
	if got := f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = ?`, string(task.ID)); got != 1 {
		t.Errorf("push configs of the new task = %d, want 1 (first-message push config kept)", got)
	}
}

// TestParkedContinuationIdentityUnavailable: an absent identity
// provider on a task-id-bearing send is the identity-unavailable server
// error, whether the id exists or not, so the lookup (which would
// answer task-not-found for the missing one) is never reached.
func TestParkedContinuationIdentityUnavailable(t *testing.T) {
	e := newIdfxEnv(t, func(*idfxProvider, *idfxRoutes) a2acore.Authenticator {
		return LocalOrBearer{Local: LocalOperator{}, Bearer: nil}
	})
	e.routes.set("owner-ref-a", "org/repo")
	tok := e.ids.add(t, "svc-a", "owner-ref-a")
	c, rec := pcDial(t, e, tok)
	lc := e.client("")
	task := sendTask(t, lc, idfxSend("org/repo", "s-1", "m-1"))
	waitLive(t, e.srv, 1)
	for name, id := range map[string]string{"existing id": string(task.ID), "missing id": pcMissingID} {
		got, err := pcContinue(t, c, rec, id, task.ContextID, "pc-"+name)
		if !errors.Is(err, a2a.ErrServerError) || errors.Is(err, a2a.ErrTaskNotFound) {
			t.Errorf("%s: err = %v, want the identity-unavailable server error", name, err)
		}
		if got.status != http.StatusInternalServerError || !strings.Contains(got.body, "identity provider unavailable") {
			t.Errorf("%s: wire = %d %s, want 500 identity provider unavailable", name, got.status, got.body)
		}
	}
}

// TestParkedContinuationStreamingOffIsUnchanged: while streaming is off
// in the card the SDK refuses every streaming send with the same
// unsupported-operation answer before it reads the store or enters the
// execution manager, so there is nothing to mask: the revoked owner, a
// foreign subject, a missing id and a visible task all get that one
// unchanged answer, and nothing is written. The client does not issue a
// streaming call while the card says streaming is off, so the test posts
// the same JSON body to the REST streaming route (/message:stream) on
// the loopback server.
func TestParkedContinuationStreamingOffIsUnchanged(t *testing.T) {
	f := pcSetup(t)
	if f.e.srv.Card().Capabilities.Streaming {
		t.Fatal("the card advertises streaming, want it off (setup)")
	}
	f.pcRevoke()
	secretID := string(f.secret.ID)
	before := pcSnapshot(t, f.e, secretID)
	post := func(token, taskID string) pcWire {
		t.Helper()
		body, err := json.Marshal(&a2a.SendMessageRequest{
			Message: &a2a.Message{
				ID: "pc-stream-" + taskID[len(taskID)-4:], Role: a2a.MessageRoleUser, TaskID: a2a.TaskID(taskID), ContextID: f.secret.ContextID,
				Parts: a2a.ContentParts{a2a.NewTextPart("continuation probe")},
				Metadata: map[string]any{
					a2acore.MetaSession: "s-pc", a2acore.MetaEpoch: float64(1), a2acore.MetaSeq: float64(2),
				},
			},
			Config: &a2a.SendMessageConfig{ReturnImmediately: true},
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		req, err := http.NewRequestWithContext(ctxT(t), http.MethodPost, f.e.ts.URL+"/message:stream", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return pcWire{status: resp.StatusCode, body: string(b)}
	}
	missing := post(f.tokA, pcMissingID)
	if !strings.Contains(missing.body, `"reason":"UNSUPPORTED_OPERATION"`) || strings.Contains(missing.body, "TASK_NOT_FOUND") {
		t.Fatalf("streaming on a missing id = %d %s, want the unchanged unsupported operation answer", missing.status, missing.body)
	}
	answers := map[string]pcWire{
		"revoked owner":   post(f.tokA, secretID),
		"foreign subject": post(f.tokB, secretID),
	}
	f.pcRestore()
	answers["visible task"] = post(f.tokA, secretID)
	for name, got := range answers {
		if got.status != missing.status || pcNorm(got.body) != pcNorm(missing.body) {
			t.Errorf("%s: streaming = %d %s, want the missing id's %d %s", name, got.status, got.body, missing.status, missing.body)
		}
		if strings.Contains(got.body, secretID) || strings.Contains(got.body, f.secret.ContextID) {
			t.Errorf("%s: streaming body carries the real task or context id: %s", name, got.body)
		}
	}
	waitLive(t, f.e.srv, 1)
	if after := pcSnapshot(t, f.e, secretID); after != before {
		t.Errorf("store changed by the streaming sends:\n before %+v\n after  %+v", before, after)
	}
}

// errPcStop ends a call after the interceptor accepted it, so the SDK
// never enters its execution manager.
var errPcStop = errors.New("pc: stop after the interceptor")

// pcProbeInterceptor wraps the auth interceptor: it records what Before
// answered and turns an acceptance into errPcStop.
type pcProbeInterceptor struct {
	inner *authInterceptor
	err   error
}

// Before implements a2asrv.CallInterceptor.
func (p *pcProbeInterceptor) Before(ctx context.Context, callCtx *a2asrv.CallContext, req *a2asrv.Request) (context.Context, any, error) {
	ctx, res, err := p.inner.Before(ctx, callCtx, req)
	p.err = err
	if err == nil && res == nil {
		err = errPcStop
	}
	return ctx, res, err
}

// After implements a2asrv.CallInterceptor.
func (p *pcProbeInterceptor) After(ctx context.Context, callCtx *a2asrv.CallContext, resp *a2asrv.Response) error {
	return p.inner.After(ctx, callCtx, resp)
}

// TestParkedContinuationStreamingInterceptor: with streaming enabled in
// the capabilities the interceptor answers an invisible or missing task
// id of a SendStreamingMessage with errTaskNotVisible and accepts a
// visible one; with streaming off (the SDK refuses it first) it does
// no lookup. The card is never changed: the call goes in process through
// an SDK handler that carries the test capabilities.
func TestParkedContinuationStreamingInterceptor(t *testing.T) {
	f := pcSetup(t)
	secretID := f.secret.ID
	f.pcRevoke()
	// call runs one in-process streaming send as the token and returns
	// what the interceptor's Before answered.
	call := func(t *testing.T, caps *a2a.AgentCapabilities, method, token string, taskID a2a.TaskID) error {
		t.Helper()
		probe := &pcProbeInterceptor{inner: newAuthInterceptor(f.e.srv.cfg).withCapabilities(caps)}
		h := a2asrv.NewHandler(&pcNoopExecutor{},
			a2asrv.WithTaskStore(scopedTaskStore{store: f.e.store}),
			a2asrv.WithCapabilityChecks(caps),
			a2asrv.WithCallInterceptors(probe),
		)
		ctx := context.WithValue(ctxT(t), credentialKeyType{}, a2acore.Credential{Authorization: "Bearer " + token, RemoteAddr: "127.0.0.1:1"})
		req := &a2a.SendMessageRequest{Message: &a2a.Message{
			ID: "pc-unit", Role: a2a.MessageRoleUser, TaskID: taskID, ContextID: f.secret.ContextID,
			Parts: a2a.ContentParts{a2a.NewTextPart("continuation probe")},
		}}
		switch method {
		case "SendStreamingMessage":
			for _, err := range h.SendStreamingMessage(ctx, req) {
				if err != nil && !errors.Is(err, errPcStop) && !errors.Is(err, a2a.ErrUnsupportedOperation) && !errors.Is(err, a2a.ErrTaskNotFound) {
					t.Fatalf("unexpected stream error: %v", err)
				}
			}
		default:
			if _, err := h.SendMessage(ctx, req); err != nil && !errors.Is(err, errPcStop) && !errors.Is(err, a2a.ErrTaskNotFound) {
				t.Fatalf("unexpected send error: %v", err)
			}
		}
		return probe.err
	}
	on := &a2a.AgentCapabilities{Streaming: true}
	off := &a2a.AgentCapabilities{Streaming: false}

	t.Run("streaming enabled", func(t *testing.T) {
		for name, tc := range map[string]struct {
			token string
			id    a2a.TaskID
		}{
			"revoked owner":   {f.tokA, secretID},
			"foreign subject": {f.tokB, secretID},
			"missing id":      {f.tokA, pcMissingID},
		} {
			if err := call(t, on, "SendStreamingMessage", tc.token, tc.id); err != errTaskNotVisible {
				t.Errorf("%s: Before = %v, want errTaskNotVisible", name, err)
			}
		}
		// A visible task (the still-granted public one) is accepted.
		public := sendTask(t, f.cA, idfxSend("org/public", "s-2", "m-2"))
		if err := call(t, on, "SendStreamingMessage", f.tokA, public.ID); err != nil {
			t.Errorf("visible task: Before = %v, want nil", err)
		}
		f.pcRestore()
		if err := call(t, on, "SendStreamingMessage", f.tokA, secretID); err != nil {
			t.Errorf("restored owner: Before = %v, want nil", err)
		}
	})
	t.Run("streaming off", func(t *testing.T) {
		for name, tc := range map[string]struct {
			token string
			id    a2a.TaskID
		}{
			"revoked owner":   {f.tokA, secretID},
			"foreign subject": {f.tokB, secretID},
			"missing id":      {f.tokA, pcMissingID},
		} {
			if err := call(t, off, "SendStreamingMessage", tc.token, tc.id); err != nil {
				t.Errorf("%s: Before = %v, want nil (no lookup while the SDK refuses streaming)", name, err)
			}
		}
	})
	t.Run("no capabilities", func(t *testing.T) {
		// The SDK refuses a streaming send only when capabilities are set
		// and streaming is off, so without capabilities it reaches the
		// manager and must be looked up.
		if err := call(t, nil, "SendStreamingMessage", f.tokB, secretID); err != errTaskNotVisible {
			t.Errorf("Before = %v, want errTaskNotVisible", err)
		}
	})
	t.Run("message send is looked up whatever the capabilities say", func(t *testing.T) {
		f.pcRevoke()
		for _, caps := range []*a2a.AgentCapabilities{on, off, nil} {
			if err := call(t, caps, "SendMessage", f.tokA, secretID); err != errTaskNotVisible {
				t.Errorf("caps %+v: SendMessage Before = %v, want errTaskNotVisible", caps, err)
			}
		}
	})
}

// pcNoopExecutor is an executor the SDK is never expected to run.
type pcNoopExecutor struct{ ran bool }

// Execute implements a2asrv.AgentExecutor.
func (x *pcNoopExecutor) Execute(context.Context, *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	x.ran = true
	return func(func(a2a.Event, error) bool) {}
}

// Cancel implements a2asrv.AgentExecutor.
func (x *pcNoopExecutor) Cancel(context.Context, *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	x.ran = true
	return func(func(a2a.Event, error) bool) {}
}

var _ a2asrv.AgentExecutor = (*pcNoopExecutor)(nil)

// TestParkedContinuationInterceptorFailsClosedWithoutStore: an
// interceptor built without a task store answers a task-id-bearing send
// as task-not-found and never enters the SDK, even when the SDK's own
// store holds a continuable task of that id; a send with no task id is
// not looked up.
func TestParkedContinuationInterceptorFailsClosedWithoutStore(t *testing.T) {
	ex := &pcNoopExecutor{}
	auth := LocalOrBearer{Local: LocalOperator{}}
	sdkStore := taskstore.NewInMemory(&taskstore.InMemoryStoreConfig{
		Authenticator: func(context.Context) (string, error) { return a2acore.LocalSubjectID, nil },
	})
	if _, err := sdkStore.Create(ctxT(t), &a2a.Task{ID: "t-1", ContextID: "c-1", Status: a2a.TaskStatus{State: a2a.TaskStateInputRequired}}); err != nil {
		t.Fatalf("seed the SDK store: %v", err)
	}
	h := a2asrv.NewHandler(ex,
		a2asrv.WithTaskStore(sdkStore),
		a2asrv.WithCallInterceptors(newAuthInterceptor(Config{Auth: auth, Authz: a2acore.SubjectPolicy{}})),
	)
	ctx, _ := a2asrv.NewCallContext(context.WithValue(ctxT(t), credentialKeyType{}, a2acore.Credential{RemoteAddr: "127.0.0.1:1"}), nil)
	_, err := h.SendMessage(ctx, &a2a.SendMessageRequest{Message: &a2a.Message{
		ID: "m-1", Role: a2a.MessageRoleUser, TaskID: "t-1", ContextID: "c-1", Parts: a2a.ContentParts{a2a.NewTextPart("x")},
	}})
	if !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("task-id-bearing send without a store err = %v, want task not found", err)
	}
	if ex.ran {
		t.Error("the executor ran for a task-id-bearing send without a store")
	}
}
