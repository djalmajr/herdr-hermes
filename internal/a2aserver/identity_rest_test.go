package a2aserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// idfxCanaryProvider is a provider error text that must never reach a
// response, a log record or the database.
const idfxCanaryProvider = "provider-secret-text-5e08b2d4"

// idfxProvider is the injected identity provider fixture: a
// mutex-guarded in-memory table of generated random tokens that supports
// revocation, per-token failures and counts every Verify call.
type idfxProvider struct {
	mu     sync.Mutex
	tokens map[string]a2acore.Identity
	fail   map[string]error
	verify int
}

func newIdfxProvider() *idfxProvider {
	return &idfxProvider{tokens: map[string]a2acore.Identity{}, fail: map[string]error{}}
}

// add registers one identity under a fresh random token (crypto/rand
// hex, not a person credential) and returns the token.
func (p *idfxProvider) add(t *testing.T, subject, ref string) string {
	t.Helper()
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random token: %v", err)
	}
	tok := hex.EncodeToString(b)
	p.mu.Lock()
	p.tokens[tok] = a2acore.Identity{Subject: subject, Ref: ref}
	p.mu.Unlock()
	return tok
}

// revoke forgets one token.
func (p *idfxProvider) revoke(tok string) {
	p.mu.Lock()
	delete(p.tokens, tok)
	p.mu.Unlock()
}

// failWith makes Verify of one token return err.
func (p *idfxProvider) failWith(tok string, err error) {
	p.mu.Lock()
	p.fail[tok] = err
	p.mu.Unlock()
}

// verifies reports how many times Verify was called.
func (p *idfxProvider) verifies() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.verify
}

// Verify implements a2acore.IdentityProvider.
func (p *idfxProvider) Verify(_ context.Context, token string) (a2acore.Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.verify++
	if err, ok := p.fail[token]; ok {
		return a2acore.Identity{}, err
	}
	id, ok := p.tokens[token]
	if !ok {
		return a2acore.Identity{}, fmt.Errorf("%w: unknown token", a2acore.ErrUnauthenticated)
	}
	return id, nil
}

// idfxRoutes is the mutable route fixture: the projects each reference
// owns and the guardian reference. It is both the a2acore.ProjectRoutes
// of the grants and the a2acore.RouteSource of the server.
type idfxRoutes struct {
	mu       sync.Mutex
	owned    map[string][]string
	guardian string
}

func newIdfxRoutes() *idfxRoutes { return &idfxRoutes{owned: map[string][]string{}} }

// set replaces the projects a reference owns.
func (r *idfxRoutes) set(ref string, projects ...string) {
	r.mu.Lock()
	r.owned[ref] = projects
	r.mu.Unlock()
}

// OwnedProjects implements a2acore.ProjectRoutes.
func (r *idfxRoutes) OwnedProjects(ref string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.owned[ref]), nil
}

// GuardianRef implements a2acore.ProjectRoutes and a2acore.RouteSource.
func (r *idfxRoutes) GuardianRef() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.guardian, nil
}

// OwnerRef implements a2acore.RouteSource.
func (r *idfxRoutes) OwnerRef(project string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for ref, ps := range r.owned {
		if slices.Contains(ps, project) {
			return ref, nil
		}
	}
	return "", nil
}

// idfxLog is a goroutine-safe log sink.
type idfxLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write implements io.Writer.
func (l *idfxLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// String returns what was logged so far.
func (l *idfxLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// idfxCred is the test CredentialSource: the Authorization value as is
// ("" sends no header).
type idfxCred string

// Authorization implements CredentialSource.
func (c idfxCred) Authorization(context.Context) (string, error) { return string(c), nil }

// idfxEnv is one server over a real store with the injected provider
// fixtures.
type idfxEnv struct {
	t      *testing.T
	store  *a2astore.Store
	dbPath string
	srv    *Server
	ts     *httptest.Server
	ids    *idfxProvider
	routes *idfxRoutes
	logs   *idfxLog
}

// newIdfxEnv builds the server with the auth the build function returns
// from the fixtures (nil build: the full LocalOrBearer wiring over the
// fixtures).
func newIdfxEnv(t *testing.T, build func(*idfxProvider, *idfxRoutes) a2acore.Authenticator) *idfxEnv {
	t.Helper()
	store, path := newTestStore(t)
	e := &idfxEnv{t: t, store: store, dbPath: path, ids: newIdfxProvider(), routes: newIdfxRoutes(), logs: &idfxLog{}}
	var auth a2acore.Authenticator
	if build == nil {
		auth = LocalOrBearer{Local: LocalOperator{}, Bearer: idfxBearer(e.ids, e.routes)}
	} else {
		auth = build(e.ids, e.routes)
	}
	srv, err := New(Config{
		Store:        store,
		Auth:         auth,
		Routes:       e.routes,
		BaseURL:      "http://127.0.0.1:7430",
		Version:      "test-1",
		LoopbackOnly: true,
		Logger:       slog.New(slog.NewJSONHandler(e.logs, nil)),
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	e.srv = srv
	e.ts = serveServer(t, srv)
	return e
}

// idfxBearer is the production bearer wiring over the fixtures.
func idfxBearer(ids *idfxProvider, routes *idfxRoutes) a2acore.Authenticator {
	return a2acore.BearerAuthenticator{Identity: ids, Grants: a2acore.RoutingGrants{Routes: routes}}
}

// client dials the SDK REST client with the bearer token ("" sends no
// header).
func (e *idfxEnv) client(token string) *Client {
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

// rawGet sends one GET with the given Authorization header value ("" and
// withHeader false sends none) and returns the status and body.
func (e *idfxEnv) rawGet(path string, authorization *string) (int, string) {
	e.t.Helper()
	req, err := http.NewRequestWithContext(ctxT(e.t), http.MethodGet, e.ts.URL+path, nil)
	if err != nil {
		e.t.Fatalf("new request: %v", err)
	}
	if authorization != nil {
		req.Header.Set("Authorization", *authorization)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		e.t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// inboxLen counts the task rows.
func (e *idfxEnv) inboxLen() int {
	e.t.Helper()
	items, err := e.store.Inbox(ctxT(e.t), a2acore.InboxFilter{Limit: 100})
	if err != nil {
		e.t.Fatalf("inbox: %v", err)
	}
	return len(items)
}

// taskOwner reads the stored owner of one task.
func (e *idfxEnv) taskOwner(taskID string) string {
	e.t.Helper()
	tv, err := e.store.TaskView(ctxT(e.t), taskID)
	if err != nil || tv == nil {
		e.t.Fatalf("task view %s = %v, %v", taskID, tv, err)
	}
	return tv.Owner
}

// idfxSend is the send input of a fresh task in a project.
func idfxSend(project, session, msgID string) SendInput {
	return SendInput{Project: project, Session: session, Epoch: 1, Seq: 1, MessageID: msgID, Text: "identity probe"}
}

// idfxAsk moves one parked task to input-required through a recorded ask
// decision, so a continuation is accepted (the SDK takes a follow-up
// send only once the parked execution is gone).
func idfxAsk(t *testing.T, e *idfxEnv, c *Client, taskID, decisionID string) {
	t.Helper()
	recordDecision(t, e.store, taskID, a2acore.DecisionAsk, decisionID, "question")
	if applied, refused, err := e.srv.ApplyPendingDecisions(ctxT(t)); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply ask = %d/%d err=%v, want 1/0 nil", applied, refused, err)
	}
	waitTaskState(t, c, taskID, a2a.TaskStateInputRequired)
	waitLive(t, e.srv, 0)
}

func TestIdentityInjectedSuccess(t *testing.T) {
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-a", "org/repo")
	tok := e.ids.add(t, "svc-a", "owner-ref-a")
	c := e.client(tok)

	task := sendTask(t, c, idfxSend("org/repo", "s-1", "m-1"))
	if got := e.taskOwner(string(task.ID)); got != "svc-a" {
		t.Errorf("stored owner = %q, want the bearer subject svc-a", got)
	}
	waitLive(t, e.srv, 1)
	got, err := c.Get(ctxT(t), string(task.ID))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != task.ID || got.ContextID != task.ContextID {
		t.Errorf("get = %s/%s, want %s/%s", got.ID, got.ContextID, task.ID, task.ContextID)
	}

	idfxAsk(t, e, c, string(task.ID), "d-1")
	res, err := c.Send(ctxT(t), SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "continuation",
	})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	cont, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("continuation result = %T, want *a2a.Task", res)
	}
	if cont.ID != task.ID || cont.ContextID != task.ContextID {
		t.Errorf("continuation = %s/%s, want the same task and context %s/%s", cont.ID, cont.ContextID, task.ID, task.ContextID)
	}
	if cont.Status.State != a2a.TaskStateWorking {
		t.Errorf("continuation state = %s, want working", cont.Status.State)
	}
	if n := e.inboxLen(); n != 1 {
		t.Errorf("task rows = %d, want 1", n)
	}
}

func TestIdentityNoProviderConfigured(t *testing.T) {
	builds := map[string]func(*idfxProvider, *idfxRoutes) a2acore.Authenticator{
		"nil bearer": func(*idfxProvider, *idfxRoutes) a2acore.Authenticator {
			return LocalOrBearer{Local: LocalOperator{}, Bearer: nil}
		},
		"empty bearer authenticator": func(*idfxProvider, *idfxRoutes) a2acore.Authenticator {
			return LocalOrBearer{Local: LocalOperator{}, Bearer: a2acore.BearerAuthenticator{}}
		},
		"bearer without grants": func(ids *idfxProvider, _ *idfxRoutes) a2acore.Authenticator {
			return LocalOrBearer{Local: LocalOperator{}, Bearer: a2acore.BearerAuthenticator{Identity: ids}}
		},
	}
	for name, build := range builds {
		t.Run(name, func(t *testing.T) {
			e := newIdfxEnv(t, build)
			e.routes.set("owner-ref-a", "org/repo")
			// A well-formed token: the provider is not configured, so it
			// is honest unavailability, not an authentication verdict.
			tok := e.ids.add(t, "svc-a", "owner-ref-a")
			c := e.client(tok)
			if _, err := c.Send(ctxT(t), idfxSend("org/repo", "s-1", "m-1")); !errors.Is(err, a2a.ErrServerError) {
				t.Errorf("send err = %v, want a server error", err)
			}
			if _, err := c.Get(ctxT(t), "t-1"); !errors.Is(err, a2a.ErrServerError) {
				t.Errorf("get err = %v, want a server error", err)
			}
			if status, body := e.rawGet("/tasks/t-1", ptr("Bearer "+tok)); status != http.StatusInternalServerError {
				t.Errorf("raw status = %d (%s), want 500", status, body)
			}
			if n := e.inboxLen(); n != 0 {
				t.Errorf("task rows = %d, want 0", n)
			}
			if v := e.ids.verifies(); v != 0 {
				t.Errorf("Verify calls = %d, want 0 (no provider configured)", v)
			}

			// The loopback local operator is a distinct policy and still
			// works without a header.
			lc := e.client("")
			task := sendTask(t, lc, idfxSend("org/repo", "s-2", "m-2"))
			if got := e.taskOwner(string(task.ID)); got != a2acore.LocalSubjectID {
				t.Errorf("local task owner = %q, want %q", got, a2acore.LocalSubjectID)
			}
		})
	}
}

func ptr(s string) *string { return &s }

func TestIdentityMissingMalformedUnknownCredential(t *testing.T) {
	// A server whose local policy is absent: a request with no header is
	// unauthenticated instead of the loopback operator.
	t.Run("missing credential without a local policy", func(t *testing.T) {
		e := newIdfxEnv(t, func(ids *idfxProvider, routes *idfxRoutes) a2acore.Authenticator {
			return LocalOrBearer{Local: nil, Bearer: idfxBearer(ids, routes)}
		})
		e.routes.set("owner-ref-a", "org/repo")
		c := e.client("")
		if _, err := c.Send(ctxT(t), idfxSend("org/repo", "s-1", "m-1")); !errors.Is(err, a2a.ErrUnauthenticated) {
			t.Errorf("send err = %v, want unauthenticated", err)
		}
		if status, _ := e.rawGet("/tasks/t-1", nil); status != http.StatusUnauthorized {
			t.Errorf("raw status = %d, want 401", status)
		}
		if n := e.inboxLen(); n != 0 {
			t.Errorf("task rows = %d, want 0", n)
		}
		if v := e.ids.verifies(); v != 0 {
			t.Errorf("Verify calls = %d, want 0", v)
		}
	})

	// The bearer authenticator alone, with no header at all.
	t.Run("missing credential on the bearer authenticator", func(t *testing.T) {
		e := newIdfxEnv(t, func(ids *idfxProvider, routes *idfxRoutes) a2acore.Authenticator {
			return idfxBearer(ids, routes)
		})
		c := e.client("")
		if _, err := c.Send(ctxT(t), idfxSend("org/repo", "s-1", "m-1")); !errors.Is(err, a2a.ErrUnauthenticated) {
			t.Errorf("send err = %v, want unauthenticated", err)
		}
		if n := e.inboxLen(); n != 0 {
			t.Errorf("task rows = %d, want 0", n)
		}
	})

	t.Run("malformed and unknown tokens", func(t *testing.T) {
		e := newIdfxEnv(t, nil)
		e.routes.set("owner-ref-a", "org/repo")
		valid := e.ids.add(t, "svc-a", "owner-ref-a")
		unknown := strings.Repeat("ab", 24)
		headers := map[string]string{
			"basic scheme":       "Basic " + valid,
			"scheme only":        "Bearer",
			"two spaces":         "Bearer  " + valid,
			"token with space":   "Bearer abc def",
			"token over 4096":    "Bearer " + strings.Repeat("a", 4097),
			"token with non-b64": "Bearer ab\"cd",
			"unknown token":      "Bearer " + unknown,
			"revoked-looking":    "Bearer " + valid + "x",
		}
		for name, h := range headers {
			if status, body := e.rawGet("/tasks/t-1", ptr(h)); status != http.StatusUnauthorized {
				t.Errorf("%s: status = %d (%s), want 401", name, status, body)
			}
		}
		// Through the official client: an unknown token on a send.
		c := e.client(unknown)
		if _, err := c.Send(ctxT(t), idfxSend("org/repo", "s-1", "m-1")); !errors.Is(err, a2a.ErrUnauthenticated) {
			t.Errorf("unknown token send err = %v, want unauthenticated", err)
		}
		if _, err := c.Get(ctxT(t), "t-1"); !errors.Is(err, a2a.ErrUnauthenticated) {
			t.Errorf("unknown token get err = %v, want unauthenticated", err)
		}
		if n := e.inboxLen(); n != 0 {
			t.Errorf("task rows = %d, want 0", n)
		}
	})
}

func TestIdentityRevocationIsNotCached(t *testing.T) {
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-a", "org/repo")
	tok := e.ids.add(t, "svc-a", "owner-ref-a")
	c := e.client(tok)

	task := sendTask(t, c, idfxSend("org/repo", "s-1", "m-1"))
	before := e.ids.verifies()
	if _, err := c.Get(ctxT(t), string(task.ID)); err != nil {
		t.Fatalf("first get: %v", err)
	}
	if got := e.ids.verifies(); got != before+1 {
		t.Fatalf("Verify calls after a get = %d, want %d (one per request, no cache)", got, before+1)
	}

	e.ids.revoke(tok)
	if _, err := c.Get(ctxT(t), string(task.ID)); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("get after revocation err = %v, want unauthenticated", err)
	}
	if got := e.ids.verifies(); got != before+2 {
		t.Errorf("Verify calls after the revoked get = %d, want %d", got, before+2)
	}
	if _, err := c.Send(ctxT(t), idfxSend("org/repo", "s-2", "m-2")); !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("send after revocation err = %v, want unauthenticated", err)
	}
	if n := e.inboxLen(); n != 1 {
		t.Errorf("task rows = %d, want 1 (the revoked token created none)", n)
	}
}

func TestIdentityAuthorizationChangesBetweenRequests(t *testing.T) {
	t.Run("one project removed", func(t *testing.T) {
		e := newIdfxEnv(t, nil)
		e.routes.set("owner-ref-a", "org/repo-a", "org/repo-b")
		tok := e.ids.add(t, "svc-a", "owner-ref-a")
		c := e.client(tok)

		task := sendTask(t, c, idfxSend("org/repo-a", "s-1", "m-1"))
		idfxAsk(t, e, c, string(task.ID), "d-1")

		// The project leaves the subject's routing: the task is task not
		// found for the continuation (the SDK loads it first), a new task
		// in it is refused, the other project still works.
		e.routes.set("owner-ref-a", "org/repo-b")
		_, err := c.Send(ctxT(t), SendInput{
			TaskID: string(task.ID), ContextID: task.ContextID,
			Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "continuation",
		})
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Fatalf("refused continuation err = %v, want task not found", err)
		}
		if _, err := c.Send(ctxT(t), idfxSend("org/repo-a", "s-2", "m-3")); !errors.Is(err, a2a.ErrUnauthorized) {
			t.Errorf("new task in the removed project err = %v, want unauthorized", err)
		}
		if n := e.inboxLen(); n != 1 {
			t.Errorf("task rows = %d, want 1", n)
		}
		other := sendTask(t, c, idfxSend("org/repo-b", "s-3", "m-4"))
		if e.taskOwner(string(other.ID)) != "svc-a" {
			t.Errorf("the still-granted project was refused")
		}

		// The project returns: the continuation with the same sequence
		// number is accepted again, which also shows that the refused one
		// did not consume the fence.
		e.routes.set("owner-ref-a", "org/repo-a", "org/repo-b")
		res, err := c.Send(ctxT(t), SendInput{
			TaskID: string(task.ID), ContextID: task.ContextID,
			Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-5", Text: "continuation",
		})
		if err != nil {
			t.Fatalf("accepted continuation: %v", err)
		}
		got, ok := res.(*a2a.Task)
		if !ok || got.ID != task.ID || got.Status.State != a2a.TaskStateWorking {
			t.Errorf("accepted continuation = %T %+v, want the same task working", res, got)
		}
	})

	t.Run("every project removed", func(t *testing.T) {
		e := newIdfxEnv(t, nil)
		e.routes.set("owner-ref-a", "org/repo")
		tok := e.ids.add(t, "svc-a", "owner-ref-a")
		c := e.client(tok)
		task := sendTask(t, c, idfxSend("org/repo", "s-1", "m-1"))

		// No project left: the grant is empty (no actions), so even the
		// read of the subject's own task is refused.
		e.routes.set("owner-ref-a")
		if _, err := c.Get(ctxT(t), string(task.ID)); !errors.Is(err, a2a.ErrUnauthorized) {
			t.Errorf("get with an empty grant err = %v, want unauthorized", err)
		}
		if _, err := c.Send(ctxT(t), idfxSend("org/repo", "s-2", "m-2")); !errors.Is(err, a2a.ErrUnauthorized) {
			t.Errorf("send with an empty grant err = %v, want unauthorized", err)
		}
		e.routes.set("owner-ref-a", "org/repo")
		if _, err := c.Get(ctxT(t), string(task.ID)); err != nil {
			t.Errorf("get after the grant returned: %v", err)
		}
		if n := e.inboxLen(); n != 1 {
			t.Errorf("task rows = %d, want 1", n)
		}
	})
}

func TestIdentityWrongSubjectAndScope(t *testing.T) {
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-x", "org/x")
	e.routes.set("owner-ref-y", "org/y")
	tokA := e.ids.add(t, "svc-a", "owner-ref-x")
	tokB := e.ids.add(t, "svc-b", "owner-ref-y")
	ca := e.client(tokA)
	cb := e.client(tokB)

	task := sendTask(t, ca, idfxSend("org/x", "s-1", "m-1"))

	// Subject B cannot read A's task: it does not exist for B.
	if _, err := cb.Get(ctxT(t), string(task.ID)); !errors.Is(err, a2a.ErrTaskNotFound) {
		t.Errorf("B get of A's task err = %v, want task not found", err)
	}
	listed, err := cb.client.ListTasks(ctxT(t), &a2a.ListTasksRequest{})
	if err != nil {
		t.Fatalf("B list: %v", err)
	}
	for _, lt := range listed.Tasks {
		if lt.ID == task.ID {
			t.Errorf("B list carries A's task %s", lt.ID)
		}
	}
	// Subject B owns project y only: it cannot create in x.
	if _, err := cb.Send(ctxT(t), idfxSend("org/x", "s-2", "m-2")); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("B send into org/x err = %v, want unauthorized", err)
	}
	if _, err := ca.Send(ctxT(t), idfxSend("org/y", "s-2", "m-3")); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("A send into org/y err = %v, want unauthorized", err)
	}
	if n := e.inboxLen(); n != 1 {
		t.Errorf("task rows = %d, want 1 (only A's task)", n)
	}
	// A still reads its own task.
	if _, err := ca.Get(ctxT(t), string(task.ID)); err != nil {
		t.Errorf("A get of its own task: %v", err)
	}
}

func TestIdentityForgedActorMetadataIgnored(t *testing.T) {
	e := newIdfxEnv(t, nil)
	e.routes.guardian = "guardian-ref"
	e.routes.set("owner-ref-a", "org/own")
	e.routes.set("owner-ref-z", "org/other")
	tok := e.ids.add(t, "svc-a", "owner-ref-a")
	c := e.client(tok)

	forged := func(project, session, msgID string) *a2a.SendMessageRequest {
		return &a2a.SendMessageRequest{
			Message: &a2a.Message{
				ID:    msgID,
				Role:  a2a.MessageRoleUser,
				Parts: a2a.ContentParts{a2a.NewTextPart("forged actor probe")},
				Metadata: map[string]any{
					a2acore.MetaProject: project,
					a2acore.MetaSession: session,
					a2acore.MetaEpoch:   float64(1),
					a2acore.MetaSeq:     float64(1),
					a2acore.MetaJob:     "job-1",
					"nau.subject":       "svc-forged",
					"nau.role":          "guardian",
					"nau.ref":           "guardian-ref",
					"actor":             "svc-forged",
					"role":              "guardian",
					"ref":               "guardian-ref",
					"subject":           "local",
				},
			},
			Config: &a2a.SendMessageConfig{ReturnImmediately: true},
			Metadata: map[string]any{
				"nau.subject": "svc-forged",
				"role":        "guardian",
				"ref":         "guardian-ref",
				"actor":       "svc-forged",
			},
		}
	}

	// A project the subject does not own: refused despite the claims.
	if _, err := c.client.SendMessage(ctxT(t), forged("org/other", "s-1", "m-1")); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("forged send into org/other err = %v, want unauthorized", err)
	}
	if n := e.inboxLen(); n != 0 {
		t.Fatalf("task rows = %d, want 0 after the refused forged send", n)
	}

	// An owned project: accepted, and the owner is the bearer subject,
	// never a metadata value.
	res, err := c.client.SendMessage(ctxT(t), forged("org/own", "s-2", "m-2"))
	if err != nil {
		t.Fatalf("forged send into the owned project: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("result = %T, want *a2a.Task", res)
	}
	if got := e.taskOwner(string(task.ID)); got != "svc-a" {
		t.Errorf("stored owner = %q, want the bearer subject svc-a", got)
	}
	items, err := e.store.Inbox(ctxT(t), a2acore.InboxFilter{Limit: 10})
	if err != nil || len(items) != 1 {
		t.Fatalf("inbox = %+v err=%v, want 1 task", items, err)
	}
	if items[0].Project != "org/own" {
		t.Errorf("stored project = %q, want org/own", items[0].Project)
	}
}

func TestIdentityLocalSubjectFromProviderRefused(t *testing.T) {
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-a", "org/repo")
	e.routes.guardian = "guardian-ref"
	spoof := e.ids.add(t, a2acore.LocalSubjectID, "guardian-ref")
	cs := e.client(spoof)

	// The local operator (no header, loopback) creates a task.
	lc := e.client("")
	localTask := sendTask(t, lc, idfxSend("org/repo", "s-1", "m-1"))
	if got := e.taskOwner(string(localTask.ID)); got != a2acore.LocalSubjectID {
		t.Fatalf("local task owner = %q, want %q", got, a2acore.LocalSubjectID)
	}

	// The provider identity "local" is refused as an unavailable
	// provider and reads nothing of the local operator.
	if task, err := cs.Get(ctxT(t), string(localTask.ID)); !errors.Is(err, a2a.ErrServerError) || task != nil {
		t.Errorf("get as provider local = %+v err=%v, want a server error and no task", task, err)
	}
	if _, err := cs.Send(ctxT(t), idfxSend("org/repo", "s-2", "m-2")); !errors.Is(err, a2a.ErrServerError) {
		t.Errorf("send as provider local err = %v, want a server error", err)
	}
	if status, _ := e.rawGet("/tasks/"+string(localTask.ID), ptr("Bearer "+spoof)); status != http.StatusInternalServerError {
		t.Errorf("raw status = %d, want 500", status)
	}
	if n := e.inboxLen(); n != 1 {
		t.Errorf("task rows = %d, want 1 (only the local operator's)", n)
	}
	// The local operator still works and still reads its own task.
	if _, err := lc.Get(ctxT(t), string(localTask.ID)); err != nil {
		t.Errorf("local get: %v", err)
	}
}

func TestIdentityProviderErrorCanaryNeverLeaks(t *testing.T) {
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-a", "org/repo")
	good := e.ids.add(t, "svc-a", "owner-ref-a")
	bad := e.ids.add(t, "svc-b", "owner-ref-a")
	e.ids.failWith(bad, errors.New("backend down for token "+bad+": "+idfxCanaryProvider))
	revoked := e.ids.add(t, "svc-c", "owner-ref-a")
	e.ids.failWith(revoked, fmt.Errorf("%w: %s", a2acore.ErrUnauthenticated, idfxCanaryProvider))

	// A valid flow puts real rows in the database.
	gc := e.client(good)
	task := sendTask(t, gc, idfxSend("org/repo", "s-1", "m-1"))
	if _, err := gc.Get(ctxT(t), string(task.ID)); err != nil {
		t.Fatalf("good get: %v", err)
	}

	// The failing and the rejected tokens: the SDK error text and the
	// raw bodies.
	var texts []string
	for name, tok := range map[string]string{"failing provider": bad, "rejected token": revoked} {
		c := e.client(tok)
		_, serr := c.Send(ctxT(t), idfxSend("org/repo", "s-9", "m-9"))
		_, gerr := c.Get(ctxT(t), string(task.ID))
		want := a2a.ErrServerError
		if tok == revoked {
			want = a2a.ErrUnauthenticated
		}
		if !errors.Is(serr, want) || !errors.Is(gerr, want) {
			t.Errorf("%s: send err = %v, get err = %v, want %v", name, serr, gerr, want)
		}
		texts = append(texts, serr.Error(), gerr.Error())
		status, body := e.rawGet("/tasks/"+string(task.ID), ptr("Bearer "+tok))
		if tok == bad && status != http.StatusInternalServerError || tok == revoked && status != http.StatusUnauthorized {
			t.Errorf("%s: raw status = %d, want 500 / 401", name, status)
		}
		texts = append(texts, body)
	}

	secrets := []string{idfxCanaryProvider, bad, revoked}
	for _, s := range secrets {
		for _, txt := range texts {
			if strings.Contains(txt, s) {
				t.Errorf("a response carries %q: %s", s, txt)
			}
		}
		if strings.Contains(e.logs.String(), s) {
			t.Errorf("the server log carries %q:\n%s", s, e.logs.String())
		}
		if bytes.Contains(dbFileBytes(t, e.dbPath), []byte(s)) {
			t.Errorf("the database carries %q", s)
		}
	}
	// The good token is a credential too: it never reaches the log or the
	// database.
	if strings.Contains(e.logs.String(), good) {
		t.Errorf("the server log carries the valid token:\n%s", e.logs.String())
	}
	if bytes.Contains(dbFileBytes(t, e.dbPath), []byte(good)) {
		t.Errorf("the database carries the valid token")
	}
	if n := e.inboxLen(); n != 1 {
		t.Errorf("task rows = %d, want 1 (the failing tokens created none)", n)
	}
}

// idfxCountingAuth is an Authenticator fixture that counts its calls and
// returns a fixed result.
type idfxCountingAuth struct {
	calls   int
	subject a2acore.Subject
	err     error
}

// Authenticate implements a2acore.Authenticator.
func (a *idfxCountingAuth) Authenticate(context.Context, a2acore.Credential) (a2acore.Subject, error) {
	a.calls++
	return a.subject, a.err
}

func TestLocalOrBearerKeepsPoliciesApart(t *testing.T) {
	cred := func(h string) a2acore.Credential {
		return a2acore.Credential{Authorization: h, RemoteAddr: "127.0.0.1:4711"}
	}
	t.Run("no header goes to local only", func(t *testing.T) {
		local := &idfxCountingAuth{subject: a2acore.Subject{ID: "local"}}
		bearer := &idfxCountingAuth{subject: a2acore.Subject{ID: "svc-a"}}
		s, err := LocalOrBearer{Local: local, Bearer: bearer}.Authenticate(context.Background(), cred(""))
		if err != nil || s.ID != "local" || local.calls != 1 || bearer.calls != 0 {
			t.Errorf("subject=%q err=%v local=%d bearer=%d, want local once and bearer never", s.ID, err, local.calls, bearer.calls)
		}
	})
	t.Run("header goes to bearer only", func(t *testing.T) {
		local := &idfxCountingAuth{subject: a2acore.Subject{ID: "local"}}
		bearer := &idfxCountingAuth{subject: a2acore.Subject{ID: "svc-a"}}
		s, err := LocalOrBearer{Local: local, Bearer: bearer}.Authenticate(context.Background(), cred("Bearer x"))
		if err != nil || s.ID != "svc-a" || local.calls != 0 || bearer.calls != 1 {
			t.Errorf("subject=%q err=%v local=%d bearer=%d, want bearer once and local never", s.ID, err, local.calls, bearer.calls)
		}
	})
	t.Run("a refused bearer never falls through to local", func(t *testing.T) {
		local := &idfxCountingAuth{subject: a2acore.Subject{ID: "local"}}
		bearer := &idfxCountingAuth{err: a2acore.ErrUnauthenticated}
		s, err := LocalOrBearer{Local: local, Bearer: bearer}.Authenticate(context.Background(), cred("Bearer x"))
		if !errors.Is(err, a2acore.ErrUnauthenticated) || s.ID != "" || local.calls != 0 {
			t.Errorf("subject=%q err=%v local=%d, want unauthenticated and local never", s.ID, err, local.calls)
		}
	})
	t.Run("a refused local never falls through to bearer", func(t *testing.T) {
		local := &idfxCountingAuth{err: a2acore.ErrUnauthenticated}
		bearer := &idfxCountingAuth{subject: a2acore.Subject{ID: "svc-a"}}
		s, err := LocalOrBearer{Local: local, Bearer: bearer}.Authenticate(context.Background(), cred(""))
		if !errors.Is(err, a2acore.ErrUnauthenticated) || s.ID != "" || bearer.calls != 0 {
			t.Errorf("subject=%q err=%v bearer=%d, want unauthenticated and bearer never", s.ID, err, bearer.calls)
		}
	})
	t.Run("nil bearer is unavailable and never local", func(t *testing.T) {
		local := &idfxCountingAuth{subject: a2acore.Subject{ID: "local"}}
		s, err := LocalOrBearer{Local: local}.Authenticate(context.Background(), cred("Bearer x"))
		if !errors.Is(err, a2acore.ErrIdentityUnavailable) || errors.Is(err, a2acore.ErrUnauthenticated) || s.ID != "" || local.calls != 0 {
			t.Errorf("subject=%q err=%v local=%d, want identity unavailable and local never", s.ID, err, local.calls)
		}
	})
	t.Run("nil local is unauthenticated and never bearer", func(t *testing.T) {
		bearer := &idfxCountingAuth{subject: a2acore.Subject{ID: "svc-a"}}
		s, err := LocalOrBearer{Bearer: bearer}.Authenticate(context.Background(), cred(""))
		if !errors.Is(err, a2acore.ErrUnauthenticated) || s.ID != "" || bearer.calls != 0 {
			t.Errorf("subject=%q err=%v bearer=%d, want unauthenticated and bearer never", s.ID, err, bearer.calls)
		}
	})
}
