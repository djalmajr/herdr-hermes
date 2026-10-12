package a2aserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

const (
	f1Token = "fixture-push-token"
	f1URL   = "http://127.0.0.1:9/push"
)

// f1Secrets is a recording SecretProvider: it keeps the live values and
// counts the Put calls, so a test sees what the store handed it.
type f1Secrets struct {
	mu     sync.Mutex
	values map[string]string
	next   int
	puts   int
}

func newF1Secrets() *f1Secrets { return &f1Secrets{values: map[string]string{}} }

// Put implements a2acore.SecretProvider.
func (f *f1Secrets) Put(_ context.Context, value string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	f.puts++
	h := fmt.Sprintf("f1h_%02d", f.next)
	f.values[h] = value
	return h, nil
}

// Get implements a2acore.SecretProvider.
func (f *f1Secrets) Get(_ context.Context, handle string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.values[handle]
	if !ok {
		return "", a2acore.ErrSecretUnavailable
	}
	return v, nil
}

// Delete implements a2acore.SecretProvider.
func (f *f1Secrets) Delete(_ context.Context, handle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.values, handle)
	return nil
}

// stats returns the number of Put calls and of live values.
func (f *f1Secrets) stats() (puts, live int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts, len(f.values)
}

// f1PeekTransport runs peek once the response is on the wire, before the
// caller decodes it, so a test sees what SQLite already committed.
type f1PeekTransport struct {
	peek func(*http.Request, *http.Response)
}

// RoundTrip implements http.RoundTripper.
func (p f1PeekTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil || p.peek == nil {
		return resp, err
	}
	p.peek(r, resp)
	return resp, nil
}

// f1Count counts rows through a read-only second connection.
func f1Count(t *testing.T, path, q string, args ...any) int {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("query %s: %v", q, err)
	}
	return n
}

// f1WaitFor polls cond until it holds or the test context ends: the
// discard runs in the execution cleanup, which the SDK runs after the
// response may already be on the wire.
func f1WaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	ctx := ctxT(t)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-tick.C:
		}
	}
}

// f1Dial dials the SDK client with an optional peeking transport.
func f1Dial(t *testing.T, baseURL string, auth *tableAuth, id string, peek func(*http.Request, *http.Response)) *Client {
	t.Helper()
	hc := &http.Client{Timeout: 10 * time.Second}
	if peek != nil {
		hc.Transport = f1PeekTransport{peek: peek}
	}
	c, err := Dial(ctxT(t), baseURL, staticCredential{value: auth.token(id)}, hc)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// f1Request is a new-task SendMessageRequest carrying a push config;
// meta keys listed in drop are removed from the correlation metadata.
func f1Request(project, session, msgID string, drop ...string) *a2a.SendMessageRequest {
	meta := map[string]any{
		a2acore.MetaProject: project,
		a2acore.MetaJob:     "job-1",
		a2acore.MetaSession: session,
		a2acore.MetaEpoch:   float64(1),
		a2acore.MetaSeq:     float64(1),
	}
	for _, k := range drop {
		delete(meta, k)
	}
	return &a2a.SendMessageRequest{
		Message: &a2a.Message{
			ID:       msgID,
			Role:     a2a.MessageRoleUser,
			Parts:    a2a.ContentParts{a2a.NewTextPart("register push with the first message")},
			Metadata: meta,
		},
		Config: &a2a.SendMessageConfig{
			ReturnImmediately: true,
			PushConfig:        &a2a.PushConfig{URL: f1URL, Token: f1Token},
		},
	}
}

// f1Env is one server over a real store with a recording provider.
type f1Env struct {
	store *a2astore.Store
	path  string
	srv   *Server
	ts    string
	auth  *tableAuth
	sec   *f1Secrets
}

func newF1Env(t *testing.T, subjects ...a2acore.Subject) *f1Env {
	t.Helper()
	sec := newF1Secrets()
	store, path := openTestStore(t, a2astore.Options{Secrets: sec})
	auth := newTableAuth(subjects...)
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	return &f1Env{store: store, path: path, srv: s, ts: serveServer(t, s).URL, auth: auth, sec: sec}
}

// rows counts the rows of one table through the read-only connection.
func (e *f1Env) rows(t *testing.T, table string) int {
	t.Helper()
	return f1Count(t, e.path, `SELECT COUNT(*) FROM `+table)
}

// f1RequireDiscarded waits for the cleanup discard and then asserts that
// nothing of the refused or duplicate send remains: wantPuts is the number
// of Put calls the store must have made (the config really reached Save)
// and wantConfigs the push config rows and live secret values left.
func (e *f1Env) f1RequireDiscarded(t *testing.T, wantPuts, wantConfigs int) {
	t.Helper()
	f1WaitFor(t, "the uncommitted push config to be discarded", func() bool {
		puts, live := e.sec.stats()
		return puts >= wantPuts && e.rows(t, "a2a_push_configs") == wantConfigs && live == wantConfigs
	})
	if puts, live := e.sec.stats(); puts != wantPuts || live != wantConfigs {
		t.Fatalf("secret provider puts=%d live=%d, want puts=%d live=%d", puts, live, wantPuts, wantConfigs)
	}
}

func TestFirstSendPushConfigSuccessIsCommittedWithTheTask(t *testing.T) {
	e := newF1Env(t, testSubject("caller-1"))
	var status, tasks, ingress, configs, tokenRefs int
	var refEqualsToken bool
	var colsHoldToken bool
	c := f1Dial(t, e.ts, e.auth, "caller-1", func(r *http.Request, resp *http.Response) {
		if r.Method != http.MethodPost || !strings.Contains(r.URL.Path, "message:send") {
			return
		}
		status = resp.StatusCode
		tasks = f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_tasks`)
		ingress = f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_ingress`)
		configs = f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs`)
		tokenRefs = f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs WHERE COALESCE(token_ref, '') <> ''`)
		refEqualsToken = f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs WHERE token_ref = ?`, f1Token) > 0
		colsHoldToken = f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs WHERE instr(task_id || '|' || config_id || '|' || url || '|' || COALESCE(auth_scheme, '') || '|' || COALESCE(token_ref, '') || '|' || COALESCE(credentials_ref, '') || '|' || created, ?) > 0`, f1Token) > 0
	})
	res, err := c.client.SendMessage(ctxT(t), f1Request("org/repo", "s-1", "m-1"))
	if err != nil {
		t.Fatalf("send with push config: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("send result = %T, want *a2a.Task", res)
	}
	if status != http.StatusOK {
		t.Errorf("http status = %d, want 200", status)
	}
	if tasks != 1 || ingress != 1 || configs != 1 {
		t.Errorf("committed at response: tasks=%d ingress=%d push configs=%d, want 1/1/1", tasks, ingress, configs)
	}
	if got := f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = ?`, string(task.ID)); got != 1 {
		t.Errorf("push configs of the new task = %d, want 1", got)
	}
	if tokenRefs != 1 || refEqualsToken {
		t.Errorf("token_ref rows = %d, equals the token = %v, want a non-empty handle that is not the token", tokenRefs, refEqualsToken)
	}
	if colsHoldToken {
		t.Error("a column of a2a_push_configs contains the token value")
	}
	if strings.Contains(string(dbFileBytes(t, e.path)), f1Token) {
		t.Error("the token value is present in the database file or its WAL")
	}
	// The committed config is not discarded by the cleanup that follows.
	waitLive(t, e.srv, 1)
	if n := e.rows(t, "a2a_push_configs"); n != 1 {
		t.Errorf("push configs after the execution started = %d, want 1", n)
	}
}

func TestFirstSendPushConfigRetrievalAndDelete(t *testing.T) {
	e := newF1Env(t, testSubject("caller-1"))
	c := f1Dial(t, e.ts, e.auth, "caller-1", nil)
	res, err := c.client.SendMessage(ctxT(t), f1Request("org/repo", "s-1", "m-1"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	task := res.(*a2a.Task)
	list, err := c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].Token != f1Token || list[0].URL != f1URL || list[0].TaskID != task.ID {
		t.Fatalf("list = %+v, want one config with the original token", list)
	}
	got, err := c.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: task.ID, ID: list[0].ID})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Token != f1Token || got.URL != f1URL || got.ID != list[0].ID {
		t.Fatalf("get = %+v, want the original token", got)
	}
	if err := c.client.DeleteTaskPushConfig(ctxT(t), &a2a.DeleteTaskPushConfigRequest{TaskID: task.ID, ID: list[0].ID}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	after, err := c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	if err != nil || len(after) != 0 {
		t.Fatalf("list after delete = %+v, %v, want none", after, err)
	}
	if puts, live := e.sec.stats(); puts != 1 || live != 0 {
		t.Errorf("secret provider puts=%d live=%d after delete, want 1/0", puts, live)
	}
}

func TestFirstSendPushConfigForeignSubjectIsMasked(t *testing.T) {
	e := newF1Env(t, testSubject("caller-1"), testSubject("caller-2"))
	owner := f1Dial(t, e.ts, e.auth, "caller-1", nil)
	res, err := owner.client.SendMessage(ctxT(t), f1Request("org/repo", "s-1", "m-1"))
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	task := res.(*a2a.Task)
	mine, err := owner.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	if err != nil || len(mine) != 1 {
		t.Fatalf("owner list = %+v, %v", mine, err)
	}
	other := f1Dial(t, e.ts, e.auth, "caller-2", nil)
	checks := map[string]error{}
	_, checks["get"] = other.client.GetTaskPushConfig(ctxT(t), &a2a.GetTaskPushConfigRequest{TaskID: task.ID, ID: mine[0].ID})
	_, checks["list"] = other.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	_, checks["create"] = other.client.CreateTaskPushConfig(ctxT(t), &a2a.PushConfig{TaskID: task.ID, URL: f1URL, Token: "fixture-foreign-token"})
	checks["delete"] = other.client.DeleteTaskPushConfig(ctxT(t), &a2a.DeleteTaskPushConfigRequest{TaskID: task.ID, ID: mine[0].ID})
	for name, err := range checks {
		if !errors.Is(err, a2a.ErrTaskNotFound) {
			t.Errorf("foreign %s error = %v, want a2a.ErrTaskNotFound", name, err)
			continue
		}
		if strings.Contains(err.Error(), f1Token) {
			t.Errorf("foreign %s error carries the token", name)
		}
	}
	// The owner's config is untouched by the foreign attempts.
	still, err := owner.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	if err != nil || len(still) != 1 || still[0].Token != f1Token {
		t.Errorf("owner list after the foreign attempts = %+v, %v, want the one original config", still, err)
	}
}

func TestFirstSendPushConfigRefusedProjectLeavesNothing(t *testing.T) {
	e := newF1Env(t, testSubject("caller-1", "org/other"))
	c := f1Dial(t, e.ts, e.auth, "caller-1", nil)
	if _, err := c.client.SendMessage(ctxT(t), f1Request("org/repo", "s-1", "m-1")); err == nil {
		t.Fatal("send to a project the subject is not granted succeeded, want a refusal")
	}
	e.f1RequireDiscarded(t, 1, 0)
	for _, table := range []string{"a2a_tasks", "a2a_ingress", "a2a_push_configs"} {
		if n := e.rows(t, table); n != 0 {
			t.Errorf("%s rows after the refused send = %d, want 0", table, n)
		}
	}
}

func TestFirstSendPushConfigInvalidCorrelationLeavesNothing(t *testing.T) {
	e := newF1Env(t, testSubject("caller-1"))
	c := f1Dial(t, e.ts, e.auth, "caller-1", nil)
	if _, err := c.client.SendMessage(ctxT(t), f1Request("org/repo", "s-1", "m-1", a2acore.MetaSession)); err == nil {
		t.Fatal("send without nau.session succeeded, want a refusal")
	}
	e.f1RequireDiscarded(t, 1, 0)
	for _, table := range []string{"a2a_tasks", "a2a_ingress", "a2a_push_configs"} {
		if n := e.rows(t, table); n != 0 {
			t.Errorf("%s rows after the refused send = %d, want 0", table, n)
		}
	}
}

func TestFirstSendPushConfigDuplicateLeavesNoSecondConfig(t *testing.T) {
	e := newF1Env(t, testSubject("caller-1"))
	c := f1Dial(t, e.ts, e.auth, "caller-1", nil)
	res, err := c.client.SendMessage(ctxT(t), f1Request("org/repo", "s-1", "m-1"))
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	first := res.(*a2a.Task)
	dup := f1Request("org/repo", "s-1", "m-1")
	dup.Config.PushConfig = &a2a.PushConfig{URL: f1URL, Token: "fixture-duplicate-token"}
	res2, err := c.client.SendMessage(ctxT(t), dup)
	if err != nil {
		t.Fatalf("duplicate send: %v", err)
	}
	reply, ok := res2.(*a2a.Message)
	if !ok || reply.Metadata[a2acore.MetaDuplicateOf] != string(first.ID) {
		t.Fatalf("duplicate answer = %T %+v, want a message that is a duplicate of task %s", res2, res2, first.ID)
	}
	if reply.TaskID == first.ID || reply.TaskID == "" {
		t.Fatalf("duplicate answer task id = %q, want the never-created id of the second send", reply.TaskID)
	}
	e.f1RequireDiscarded(t, 2, 1)
	if n := f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = ?`, string(reply.TaskID)); n != 0 {
		t.Errorf("push configs of the never-created task = %d, want 0", n)
	}
	if n := f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_tasks WHERE task_id = ?`, string(reply.TaskID)); n != 0 {
		t.Errorf("task rows of the never-created task = %d, want 0", n)
	}
	if n := f1Count(t, e.path, `SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = ?`, string(first.ID)); n != 1 {
		t.Errorf("push configs of the first task = %d, want 1", n)
	}
	list, err := c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: first.ID})
	if err != nil || len(list) != 1 || list[0].Token != f1Token {
		t.Errorf("list = %+v, %v, want only the first config", list, err)
	}
}

func TestFirstSendPushConfigContinuationStoresOnTheExistingTask(t *testing.T) {
	e := newIdfxEnv(t, nil)
	e.routes.set("owner-ref-a", "org/repo")
	tok := e.ids.add(t, "svc-a", "owner-ref-a")
	c := e.client(tok)
	task := sendTask(t, c, idfxSend("org/repo", "s-1", "m-1"))
	idfxAsk(t, e, c, string(task.ID), "d-1")

	res, err := c.client.SendMessage(ctxT(t), &a2a.SendMessageRequest{
		Message: &a2a.Message{
			ID:        "m-2",
			Role:      a2a.MessageRoleUser,
			TaskID:    task.ID,
			ContextID: task.ContextID,
			Parts:     a2a.ContentParts{a2a.NewTextPart("continuation")},
			Metadata: map[string]any{
				a2acore.MetaSession: "s-1",
				a2acore.MetaEpoch:   float64(1),
				a2acore.MetaSeq:     float64(2),
			},
		},
		Config: &a2a.SendMessageConfig{
			ReturnImmediately: true,
			PushConfig:        &a2a.PushConfig{URL: f1URL, Token: f1Token},
		},
	})
	if err != nil {
		t.Fatalf("continuation with push config: %v", err)
	}
	cont, ok := res.(*a2a.Task)
	if !ok || cont.ID != task.ID {
		t.Fatalf("continuation result = %T %+v, want the same task %s", res, res, task.ID)
	}
	list, err := c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID})
	if err != nil || len(list) != 1 || list[0].Token != f1Token {
		t.Fatalf("list = %+v, %v, want the config stored on the existing task", list, err)
	}
	waitLive(t, e.srv, 1)
	// The cleanup of a continuation never discards a committed task's config.
	if again, err := c.client.ListTaskPushConfigs(ctxT(t), &a2a.ListTaskPushConfigRequest{TaskID: task.ID}); err != nil || len(again) != 1 {
		t.Fatalf("list after the execution started = %+v, %v, want 1", again, err)
	}
}
