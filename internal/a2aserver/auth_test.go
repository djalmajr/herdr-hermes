package a2aserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/djalmajr/nau/internal/a2acore"
)

// testHTTPClientTimeout bounds every test client call (no test may hang).
const testHTTPClientTimeout = 10 * time.Second

func TestLocalOperator(t *testing.T) {
	op := LocalOperator{}
	cases := []struct {
		name    string
		cred    a2acore.Credential
		wantErr bool
	}{
		{name: "loopback without credential", cred: a2acore.Credential{RemoteAddr: "127.0.0.1:4711"}},
		{name: "ipv6 loopback", cred: a2acore.Credential{RemoteAddr: "[::1]:4711"}},
		{name: "non loopback", cred: a2acore.Credential{RemoteAddr: "192.0.2.1:4711"}, wantErr: true},
		{name: "garbage peer", cred: a2acore.Credential{RemoteAddr: "not-an-address"}, wantErr: true},
		{name: "loopback with credential", cred: a2acore.Credential{RemoteAddr: "127.0.0.1:4711", Authorization: "Bearer canary-local-0001"}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := op.Authenticate(context.Background(), tc.cred)
			if tc.wantErr {
				if !errors.Is(err, a2acore.ErrUnauthenticated) {
					t.Fatalf("err = %v, want ErrUnauthenticated", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if s.ID != "local" {
				t.Errorf("subject id = %q, want local", s.ID)
			}
			if !slices.Contains(s.Projects, a2acore.AnyProject) {
				t.Errorf("projects = %v, want AnyProject", s.Projects)
			}
			for _, a := range a2acore.AllActions {
				if !slices.Contains(s.Actions, a) {
					t.Errorf("actions = %v, want %s in them", s.Actions, a)
				}
			}
		})
	}
}

func TestCredentialMiddlewareStripsHeader(t *testing.T) {
	var mu sync.Mutex
	var seen http.Header
	var seenCred a2acore.Credential
	inner := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = r.Header.Clone()
		seenCred = credentialFrom(r.Context())
		mu.Unlock()
		rw.WriteHeader(http.StatusNoContent)
	})
	const canary = "canary-middleware-0001"
	ts := newProbeServer(t, credentialMiddleware(inner))
	req, err := http.NewRequestWithContext(ctxT(t), http.MethodPost, ts.URL+"/message:send", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+canary)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()

	mu.Lock()
	defer mu.Unlock()
	if got := seen.Get("Authorization"); got != "" {
		t.Errorf("inner saw Authorization = %q, want empty (deleted before the SDK)", got)
	}
	if seenCred.Authorization != "Bearer "+canary {
		t.Errorf("context credential = %q, want the captured value", seenCred.Authorization)
	}
	if seenCred.RemoteAddr == "" {
		t.Error("context credential has no remote address")
	}
}

func TestChunkedBodyCappedAtOneMiB(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)

	// A 2 MiB body of unknown length: the explicit ContentLength = -1
	// makes the client send it with Transfer-Encoding: chunked and no
	// Content-Length (the server sees ContentLength = -1).
	body := largeBodyReader{size: 2 << 20}
	req, err := http.NewRequestWithContext(ctxT(t), http.MethodPost, ts.URL+"/message:send", &body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.ContentLength = -1
	req.Header.Set("Authorization", auth.token("caller-1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	all, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		t.Errorf("oversized chunked body accepted: %s", all)
	}
	items, err := store.Inbox(ctxT(t), a2acore.InboxFilter{Limit: 10})
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("inbox = %+v, want no task (the body was capped)", items)
	}
}

// largeBodyReader is a body of unknown length (chunked on the wire) of
// size bytes.
type largeBodyReader struct {
	size int
	n    int
}

// Read implements io.Reader.
func (r *largeBodyReader) Read(p []byte) (int, error) {
	if r.n >= r.size {
		return 0, io.EOF
	}
	m := len(p)
	if m > r.size-r.n {
		m = r.size - r.n
	}
	for i := 0; i < m; i++ {
		p[i] = 'x'
	}
	r.n += m
	return m, nil
}

func TestLoopbackOnlyGuard(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s, err := New(Config{
		Store:        store,
		Auth:         auth,
		Routes:       fakeRoutes{owner: "owner-ref-1"},
		BaseURL:      "http://127.0.0.1:7430",
		Version:      "test-1",
		LoopbackOnly: true,
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	ts := serveServer(t, s)
	client := &http.Client{Timeout: testHTTPClientTimeout}
	card := ts.URL + "/.well-known/agent-card.json"

	// A non-loopback Host is refused before routing, the agent card
	// route included, with the fixed JSON error body.
	req, err := http.NewRequestWithContext(ctxT(t), http.MethodGet, card, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = "evil.example"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("host evil.example status = %d, want 403 (body %s)", resp.StatusCode, body)
	}
	if string(body) != `{"status":"forbidden","motivo":"a2a: non-loopback host or origin"}` {
		t.Errorf("body = %s, want the fixed JSON error", body)
	}

	// A non-loopback Origin is refused too, even with a loopback Host.
	req, err = http.NewRequestWithContext(ctxT(t), http.MethodGet, card, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", "https://evil.example")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("origin https://evil.example status = %d, want 403", resp.StatusCode)
	}

	// A loopback Origin is allowed.
	req, err = http.NewRequestWithContext(ctxT(t), http.MethodGet, card, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Origin", "http://127.0.0.1:1234")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("origin http://127.0.0.1:1234 status = %d, want 200", resp.StatusCode)
	}

	// No Origin and a loopback Host is the normal path.
	if resp, err := client.Get(card); err != nil {
		t.Fatalf("get card: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("plain loopback status = %d, want 200", resp.StatusCode)
		}
	}
}

func TestLoopbackOnlyDisabledByDefault(t *testing.T) {
	// LoopbackOnly false (the default) means no guard: a non-loopback
	// Host is routed.
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	client := &http.Client{Timeout: testHTTPClientTimeout}
	req, err := http.NewRequestWithContext(ctxT(t), http.MethodGet, ts.URL+"/.well-known/agent-card.json", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = "evil.example"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (no guard when LoopbackOnly is false)", resp.StatusCode)
	}
}

func TestAuthInterceptorProbes(t *testing.T) {
	const canary = "canary-svcparams-0002"
	// canaryAuth accepts only the canary credential: it proves the
	// interceptor authenticated the captured header while the service
	// param assertions prove the value never reached the SDK.
	auth := canaryAuth{cred: "Bearer " + canary, subject: testSubject("caller-1")}

	var mu sync.Mutex
	var handlerParams map[string][]string
	var execParams map[string][]string
	var execUser *a2asrv.User
	probeI := &probeCallInterceptor{fn: func(callCtx *a2asrv.CallContext) {
		handlerParams = listServiceParams(callCtx.ServiceParams())
	}}
	probeE := &probeExecutor{fn: func(ec *a2asrv.ExecutorContext) {
		execParams = listServiceParams(ec.ServiceParams)
		execUser = ec.User
	}}

	// The probe server uses the SDK's default in-memory task store:
	// the durable store creates tasks only through admissions (ingress
	// rows), which the probe executor does not run.
	handler := a2asrv.NewHandler(probeE,
		a2asrv.WithCallInterceptors(probeI, newAuthInterceptor(Config{Auth: auth, Authz: a2acore.SubjectPolicy{}})),
	)
	ts := newProbeServer(t, credentialMiddleware(a2asrv.NewRESTHandler(handler)))

	// No card route is mounted on this probe server: the raw client is
	// built from a local card that points at the served URL.
	raw, err := a2aclient.NewFromCard(ctxT(t), &a2a.AgentCard{
		Name:                cardName,
		Version:             "test-1",
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(ts.URL, a2a.TransportProtocolHTTPJSON)},
	}, a2aclient.WithConfig(a2aclient.Config{PreferredTransports: []a2a.TransportProtocol{a2a.TransportProtocolHTTPJSON}}))
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer raw.Destroy()
	rctx := a2aclient.AttachServiceParams(ctxT(t), a2aclient.ServiceParams{
		"authorization": []string{"Bearer " + canary},
	})
	if _, err := raw.SendMessage(rctx, &a2a.SendMessageRequest{
		Message: &a2a.Message{
			ID:    "m-1",
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{a2a.NewTextPart("probe")},
			Metadata: map[string]any{
				a2acore.MetaProject: "org/repo",
				a2acore.MetaSession: "s-1",
				a2acore.MetaEpoch:   float64(1),
				a2acore.MetaSeq:     float64(1),
			},
		},
		Config: &a2a.SendMessageConfig{ReturnImmediately: true},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if handlerParams == nil || execParams == nil {
		t.Fatalf("probe not reached: handlerParams=%v execParams=%v", handlerParams, execParams)
	}
	if _, ok := handlerParams["a2a-version"]; !ok {
		t.Errorf("handler params = %v, want the a2a-version key present", handlerParams)
	}
	if _, ok := handlerParams["authorization"]; ok {
		t.Errorf("handler params = %v, want no authorization key", handlerParams)
	}
	if _, ok := execParams["authorization"]; ok {
		t.Errorf("executor params = %v, want no authorization key", execParams)
	}
	for _, vals := range handlerParams {
		if slices.Contains(vals, canary) {
			t.Errorf("handler params carry the canary: %v", handlerParams)
		}
	}
	for _, vals := range execParams {
		if slices.Contains(vals, canary) {
			t.Errorf("executor params carry the canary: %v", execParams)
		}
	}
	if execUser == nil || !execUser.Authenticated || execUser.Name != "caller-1" {
		t.Fatalf("executor user = %+v, want authenticated caller-1", execUser)
	}
	if _, ok := execUser.Attributes[subjectAttr].(a2acore.Subject); !ok {
		t.Error("executor user has no subject attribute")
	}
}

func TestMissingCredentialIsUnauthenticated(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)

	// No credential at all: the client sends no header.
	c, err := Dial(ctxT(t), ts.URL, nil, &http.Client{Timeout: testHTTPClientTimeout})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, err := c.Get(ctxT(t), "t-1"); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("get err = %v, want ErrUnauthenticated", err)
	}
	if _, err := c.Send(ctxT(t), SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, Text: "hi"}); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("send err = %v, want ErrUnauthenticated", err)
	}

	// A credential the authenticator does not know.
	uc := dialClient(t, ts.URL, auth, "someone-else")
	if _, err := uc.Get(ctxT(t), "t-1"); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("unknown token get err = %v, want ErrUnauthenticated", err)
	}
}

func TestMethodAuthorization(t *testing.T) {
	store, _ := newTestStore(t)
	sender := a2acore.Subject{ID: "sender-1", Projects: []string{a2acore.AnyProject}, Actions: []a2acore.Action{a2acore.ActionSend}}
	reader := a2acore.Subject{ID: "reader-1", Projects: []string{a2acore.AnyProject}, Actions: []a2acore.Action{a2acore.ActionRead, a2acore.ActionCancel, a2acore.ActionPush}}
	auth := newTableAuth(sender, reader)
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	sc := dialClient(t, ts.URL, auth, "sender-1")
	rc := dialClient(t, ts.URL, auth, "reader-1")

	// The sender cannot read, cancel or manage push configs.
	if _, err := sc.Get(ctxT(t), "t-1"); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("sender get err = %v, want ErrUnauthorized", err)
	}
	if _, err := sc.Cancel(ctxT(t), "t-1"); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("sender cancel err = %v, want ErrUnauthorized", err)
	}
	if _, err := sc.client.ListTasks(ctxT(t), &a2a.ListTasksRequest{}); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("sender list err = %v, want ErrUnauthorized", err)
	}
	if _, err := sc.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: "t-1", URL: "http://127.0.0.1:8000/hook"}); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("sender push create err = %v, want ErrUnauthorized", err)
	}

	// The reader cannot send.
	if _, err := rc.Send(ctxT(t), SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, Text: "hi"}); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("reader send err = %v, want ErrUnauthorized", err)
	}
}

func TestCredentialCanaryNeverLogged(t *testing.T) {
	var buf bytes.Buffer
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	const canary = "canary-log-0003"
	s, err := New(Config{
		Store:   store,
		Auth:    auth,
		Routes:  fakeRoutes{owner: "owner-ref-1"},
		BaseURL: "http://127.0.0.1:7430",
		Version: "test-1",
		Logger:  slog.New(slog.NewJSONHandler(&buf, nil)),
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	ts := serveServer(t, s)
	c := dialClient(t, ts.URL, auth, "caller-1")
	// A send with the canary credential, a get, a recorded decision,
	// a cancel of the parked task and a decision pass exercise the
	// logged paths.
	task := sendTask(t, c, SendInput{Project: "org/repo", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "log probe"})
	if _, err := c.Get(ctxT(t), string(task.ID)); err != nil {
		t.Fatalf("get: %v", err)
	}
	recordDecision(t, store, string(task.ID), a2acore.DecisionDecline, "d-1", "no thanks")
	if _, err := c.Cancel(ctxT(t), string(task.ID)); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, _, err := s.ApplyPendingDecisions(ctxT(t)); err != nil {
		t.Fatalf("apply decisions: %v", err)
	}
	if bytes.Contains(buf.Bytes(), []byte(canary)) {
		t.Errorf("captured logs carry the credential canary:\n%s", buf.String())
	}
}

func TestBodyCappedAtOneMiB(t *testing.T) {
	store, _ := newTestStore(t)
	auth := newTableAuth(testSubject("caller-1"))
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	ts := serveServer(t, s)
	// A body above 1 MiB is refused: the SDK decode fails.
	body := bytes.Repeat([]byte(`{"message":{"parts":[{"kind":"text","text":"x`), 200_000)
	req, err := http.NewRequestWithContext(ctxT(t), http.MethodPost, ts.URL+"/message:send", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", auth.token("caller-1"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	all, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		t.Errorf("oversized body accepted: %s", all)
	}
}

// listServiceParams flattens one SDK service param set.
func listServiceParams(sp *a2asrv.ServiceParams) map[string][]string {
	out := map[string][]string{}
	if sp == nil {
		return out
	}
	for k, v := range sp.List() {
		out[k] = v
	}
	return out
}

// probeCallInterceptor is the test a2asrv.CallInterceptor that records
// the call context the interceptor chain sees.
type probeCallInterceptor struct {
	fn func(*a2asrv.CallContext)
}

// Before implements a2asrv.CallInterceptor.
func (p *probeCallInterceptor) Before(ctx context.Context, callCtx *a2asrv.CallContext, _ *a2asrv.Request) (context.Context, any, error) {
	p.fn(callCtx)
	return ctx, nil, nil
}

// After implements a2asrv.CallInterceptor.
func (p *probeCallInterceptor) After(_ context.Context, _ *a2asrv.CallContext, _ *a2asrv.Response) error {
	return nil
}

// probeExecutor is the test a2asrv.AgentExecutor that records the
// executor context and ends with a minimal valid event sequence.
type probeExecutor struct {
	fn func(*a2asrv.ExecutorContext)
}

// Execute implements a2asrv.AgentExecutor.
func (p *probeExecutor) Execute(_ context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		p.fn(ec)
		if !yield(a2a.NewSubmittedTask(ec, ec.Message), nil) {
			return
		}
		yield(a2a.NewStatusUpdateEvent(ec, a2a.TaskStateCompleted, nil), nil)
	}
}

// Cancel implements a2asrv.AgentExecutor.
func (p *probeExecutor) Cancel(_ context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewStatusUpdateEvent(ec, a2a.TaskStateCanceled, nil), nil)
	}
}

// newProbeServer serves one handler on a 127.0.0.1 httptest server.
func newProbeServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

// canaryAuth is the probe authenticator: it accepts only one fixed
// credential and returns the fixture subject.
type canaryAuth struct {
	cred    string
	subject a2acore.Subject
}

// Authenticate implements a2acore.Authenticator.
func (c canaryAuth) Authenticate(_ context.Context, cr a2acore.Credential) (a2acore.Subject, error) {
	if cr.Authorization == c.cred {
		return c.subject, nil
	}
	return a2acore.Subject{}, fmt.Errorf("%w: unknown credential", a2acore.ErrUnauthenticated)
}
