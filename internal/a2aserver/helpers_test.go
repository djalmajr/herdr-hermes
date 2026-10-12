package a2aserver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// tableAuth is the test authenticator: a bearer table of generated
// non-person fixtures. Its error text never carries the credential.
type tableAuth struct {
	mu       sync.Mutex
	subjects map[string]a2acore.Subject
}

// newTableAuth builds the table from the fixture subjects (the token of
// a subject is "Bearer tok-<id>").
func newTableAuth(subjects ...a2acore.Subject) *tableAuth {
	m := make(map[string]a2acore.Subject, len(subjects))
	for _, s := range subjects {
		m["tok-"+s.ID] = s
	}
	return &tableAuth{subjects: m}
}

// Authenticate implements a2acore.Authenticator.
func (t *tableAuth) Authenticate(_ context.Context, c a2acore.Credential) (a2acore.Subject, error) {
	t.mu.Lock()
	s, ok := t.subjects[strings.TrimPrefix(c.Authorization, "Bearer ")]
	t.mu.Unlock()
	if !ok {
		return a2acore.Subject{}, fmt.Errorf("%w: unknown credential", a2acore.ErrUnauthenticated)
	}
	return s, nil
}

// token is the Authorization value of one fixture subject.
func (t *tableAuth) token(id string) string { return "Bearer tok-" + id }

// staticCredential is the test CredentialSource.
type staticCredential struct {
	value string
	err   error
}

// Authorization implements a2aserver.CredentialSource.
func (s staticCredential) Authorization(_ context.Context) (string, error) {
	return s.value, s.err
}

// fakeRoutes is the test route source.
type fakeRoutes struct {
	owner    string
	guardian string
	err      error
}

// OwnerRef implements a2acore.RouteSource.
func (f fakeRoutes) OwnerRef(string) (string, error) { return f.owner, f.err }

// GuardianRef implements a2acore.RouteSource.
func (f fakeRoutes) GuardianRef() (string, error) { return f.guardian, f.err }

// testSubject is one fixture subject (every action by default).
func testSubject(id string, projects ...string) a2acore.Subject {
	if len(projects) == 0 {
		projects = []string{a2acore.AnyProject}
	}
	return a2acore.Subject{ID: id, Projects: projects, Actions: append([]a2acore.Action{}, a2acore.AllActions...)}
}

// openTestStore opens one real store in a temp dir with the given
// options and returns it with its path (so a test can reopen it).
func openTestStore(t *testing.T, opts a2astore.Options) (*a2astore.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "a2a.db")
	s, err := a2astore.OpenFile(path, opts)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

// newTestStore opens one real store in a temp dir and returns it with
// its path (so a test can reopen it).
func newTestStore(t *testing.T) (*a2astore.Store, string) {
	return openTestStore(t, a2astore.Options{})
}

// newTestServer builds one server over the store with the given
// authenticator and routes.
func newTestServer(t *testing.T, store *a2astore.Store, auth a2acore.Authenticator, routes a2acore.RouteSource) *Server {
	t.Helper()
	s, err := New(Config{
		Store:   store,
		Auth:    auth,
		Routes:  routes,
		BaseURL: "http://127.0.0.1:7430",
		Version: "test-1",
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	return s
}

// serveServer starts the server handler on a 127.0.0.1 httptest server.
func serveServer(t *testing.T, s *Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts
}

// dialClient dials the test client with the bearer token of id.
func dialClient(t *testing.T, baseURL string, auth *tableAuth, id string) *Client {
	t.Helper()
	c, err := Dial(ctxT(t), baseURL, staticCredential{value: auth.token(id)}, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// ctxT is one call context with a 10 s deadline (tests must never hang).
func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// waitLive polls LiveExecutions until it equals want (10 s deadline).
func waitLive(t *testing.T, s *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if s.LiveExecutions() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("live executions = %d, want %d", s.LiveExecutions(), want)
}

// waitTaskState polls one task through the client until its state is
// want (10 s deadline).
func waitTaskState(t *testing.T, c *Client, taskID string, want a2a.TaskState) *a2a.Task {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last *a2a.Task
	for time.Now().Before(deadline) {
		task, err := c.Get(ctxT(t), taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		last = task
		if task.Status.State == want {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s state = %s, want %s", taskID, last.Status.State, want)
	return nil
}

// recordDecision records one pending decision on the store.
func recordDecision(t *testing.T, store *a2astore.Store, taskID string, kind a2acore.DecisionKind, id, text string) {
	t.Helper()
	_, r, err := store.RecordDecision(ctxT(t), a2acore.Decision{
		ID: id, TaskID: taskID, Kind: kind, Text: text, Actor: "owner-1",
	}, time.Now())
	if err != nil {
		t.Fatalf("record decision: %v", err)
	}
	if r != nil {
		t.Fatalf("record decision refused: %v", r)
	}
}

// findDecision looks up one stored decision of a task by id.
func findDecision(t *testing.T, store *a2astore.Store, taskID, decisionID string) a2acore.Decision {
	t.Helper()
	_, _, decisions, err := store.Show(ctxT(t), taskID)
	if err != nil {
		t.Fatalf("show task: %v", err)
	}
	for _, d := range decisions {
		if d.ID == decisionID {
			return d
		}
	}
	t.Fatalf("decision %s not found on task %s", decisionID, taskID)
	return a2acore.Decision{}
}

// sendTask sends one fresh task through the client and returns it. It
// always sends a job (the CLI always has one): the store's Show and
// Inbox read the optional job column into a plain string, so a NULL job
// is not readable there in this dependency version.
func sendTask(t *testing.T, c *Client, in SendInput) *a2a.Task {
	t.Helper()
	if in.Job == "" {
		in.Job = "job-1"
	}
	res, err := c.Send(ctxT(t), in)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("send result = %T, want *a2a.Task", res)
	}
	return task
}

// dbFileBytes reads the database file and its -wal sidecar (bytes for
// the secret-absence assertions).
func dbFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	out := []byte{}
	for _, p := range []string{path, path + "-wal"} {
		b, err := os.ReadFile(p)
		if err != nil {
			continue // sidecar may be absent or rolled away
		}
		out = append(out, b...)
	}
	return out
}
