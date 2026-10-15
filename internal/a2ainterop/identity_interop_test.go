package a2ainterop

// The identity adapter over the official SDK (DJA-283, acceptance item
// 4): the bearer credential of an injected identity provider is verified
// on every request, its grants come from the routing and are never
// cached, and none of the credential, push or provider-error canaries
// reaches a durable row, the database bytes, the logs, an HTTP response
// body or a notice.

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	_ "modernc.org/sqlite"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2aserver"
	"github.com/djalmajr/nau/internal/a2astore"
)

// idxProvider is the counting identity provider fixture: a table of
// tokens to identities, one token that makes the provider fail with a
// given error, and a revocation.
type idxProvider struct {
	mu     sync.Mutex
	tokens map[string]a2acore.Identity
	fail   map[string]error
	calls  int
}

// Verify implements a2acore.IdentityProvider.
func (p *idxProvider) Verify(_ context.Context, token string) (a2acore.Identity, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if err, ok := p.fail[token]; ok {
		return a2acore.Identity{}, err
	}
	id, ok := p.tokens[token]
	if !ok {
		return a2acore.Identity{}, fmt.Errorf("%w: unknown token", a2acore.ErrUnauthenticated)
	}
	return id, nil
}

// revoke forgets one token in the source.
func (p *idxProvider) revoke(token string) {
	p.mu.Lock()
	delete(p.tokens, token)
	p.mu.Unlock()
}

// count is the number of Verify calls so far.
func (p *idxProvider) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// idxRoutes is the route fixture: both the RouteSource of the server and
// the ProjectRoutes of the grants.
type idxRoutes struct {
	owners   map[string]string
	guardian string
}

// OwnerRef implements a2acore.RouteSource.
func (r *idxRoutes) OwnerRef(project string) (string, error) { return r.owners[project], nil }

// GuardianRef implements a2acore.RouteSource and a2acore.ProjectRoutes.
func (r *idxRoutes) GuardianRef() (string, error) { return r.guardian, nil }

// OwnedProjects implements a2acore.ProjectRoutes.
func (r *idxRoutes) OwnedProjects(ref string) ([]string, error) {
	var out []string
	for p, o := range r.owners {
		if ref != "" && o == ref {
			out = append(out, p)
		}
	}
	return out, nil
}

// idxGrants counts the grant lookups of the real RoutingGrants.
type idxGrants struct {
	inner a2acore.GrantSource
	mu    sync.Mutex
	calls int
}

// Grant implements a2acore.GrantSource.
func (g *idxGrants) Grant(ctx context.Context, id a2acore.Identity) (a2acore.Grant, error) {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	return g.inner.Grant(ctx, id)
}

// count is the number of Grant calls so far.
func (g *idxGrants) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// recordedResponse is one HTTP response the client saw.
type recordedResponse struct {
	status int
	body   []byte
}

// recordingTransport records every response body the clients receive.
type recordingTransport struct {
	inner http.RoundTripper
	mu    sync.Mutex
	resps []recordedResponse
}

// RoundTrip implements http.RoundTripper.
func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := r.inner.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	body, rerr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if rerr != nil {
		return nil, rerr
	}
	r.mu.Lock()
	r.resps = append(r.resps, recordedResponse{status: resp.StatusCode, body: body})
	r.mu.Unlock()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return resp, nil
}

// last returns the most recent recorded response.
func (r *recordingTransport) last(t *testing.T) recordedResponse {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.resps) == 0 {
		t.Fatal("no HTTP response was recorded")
	}
	return r.resps[len(r.resps)-1]
}

// idxAuthInterceptor sets the Authorization header of the raw SDK
// client.
type idxAuthInterceptor struct{ value string }

// Before implements a2aclient.CallInterceptor.
func (i idxAuthInterceptor) Before(ctx context.Context, req *a2aclient.Request) (context.Context, any, error) {
	req.ServiceParams["authorization"] = []string{i.value}
	return ctx, nil, nil
}

// After implements a2aclient.CallInterceptor.
func (idxAuthInterceptor) After(context.Context, *a2aclient.Response) error { return nil }

// scanAllCells reads every cell of every table of the database file
// (tables from sqlite_master) and returns, per table, the column names
// and the rendered text of every cell.
func scanAllCells(t *testing.T, path string) (columns map[string][]string, cells []string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open the database for the row scan: %v", err)
	}
	defer func() { _ = db.Close() }()
	tables, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var names []string
	for tables.Next() {
		var n string
		if err := tables.Scan(&n); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, n)
	}
	if err := tables.Err(); err != nil {
		t.Fatalf("list tables: %v", err)
	}
	_ = tables.Close()
	columns = map[string][]string{}
	for _, name := range names {
		rows, err := db.Query(`SELECT * FROM "` + strings.ReplaceAll(name, `"`, `""`) + `"`)
		if err != nil {
			t.Fatalf("select from %s: %v", name, err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("columns of %s: %v", name, err)
		}
		columns[name] = cols
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatalf("scan row of %s: %v", name, err)
			}
			for _, v := range vals {
				switch x := v.(type) {
				case nil:
				case []byte:
					cells = append(cells, string(x))
				default:
					cells = append(cells, fmt.Sprint(x))
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows of %s: %v", name, err)
		}
		_ = rows.Close()
	}
	return columns, cells
}

// TestA2AIdentityInterop drives the official SDK client against a server
// wired with the identity adapter over the real store file.
func TestA2AIdentityInterop(t *testing.T) {
	clock := newTestClock()
	dbPath := filepath.Join(t.TempDir(), "a2a.db")
	store, err := a2astore.OpenFile(dbPath, a2astore.Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	storeOpen := true
	defer func() {
		if storeOpen {
			_ = store.Close()
		}
	}()

	const subject = "subject-identity-1"
	const guardianRef = "guardian-ref-a"
	tokOK := "canary-bearer-" + canaryRandom(t)
	tokErr := "canary-providerfail-" + canaryRandom(t)
	providerCanary := "canary-providererr-" + canaryRandom(t)
	pushToken := "canary-pushtoken-" + canaryRandom(t)
	pushCred := "canary-pushcred-" + canaryRandom(t)
	forgedSubject := "forged-subject-" + canaryRandom(t)
	provider := &idxProvider{
		// The subject speaks for the guardian, so the owner's notice is
		// not a delivery to itself.
		tokens: map[string]a2acore.Identity{tokOK: {Subject: subject, Ref: guardianRef}},
		fail:   map[string]error{tokErr: errors.New("backend down: " + providerCanary)},
	}
	routes := &idxRoutes{owners: map[string]string{projectP1: ownerRefA}, guardian: guardianRef}
	grants := &idxGrants{inner: a2acore.RoutingGrants{Routes: routes}}
	logb := &bytes.Buffer{}
	pusher := &fakePushSender{}
	srv, err := a2aserver.New(a2aserver.Config{
		Store: store,
		Auth: a2aserver.LocalOrBearer{
			Local:  a2aserver.LocalOperator{},
			Bearer: a2acore.BearerAuthenticator{Identity: provider, Grants: grants},
		},
		Routes:     routes,
		PushSender: pusher,
		BaseURL:    "http://127.0.0.1:7430",
		Version:    "test-1",
		Limits:     a2acore.DefaultLimits,
		Now:        clock.Now,
		Logger:     slog.New(slog.NewJSONHandler(logb, nil)),
	})
	if err != nil {
		t.Fatalf("build server: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	tsOpen := true
	defer func() {
		if tsOpen {
			ts.Close()
		}
	}()
	rec := &recordingTransport{inner: http.DefaultTransport}
	hc := &http.Client{Transport: rec, Timeout: 30 * time.Second}

	// expectCalls asserts the provider and grant source were each
	// called exactly the given number of times since the last check.
	lastProvider, lastGrants := 0, 0
	expectCalls := func(step string, wantProvider, wantGrants int) {
		t.Helper()
		p, g := provider.count(), grants.count()
		if p-lastProvider != wantProvider || g-lastGrants != wantGrants {
			t.Fatalf("%s: provider calls = %d, grant calls = %d, want %d and %d", step, p-lastProvider, g-lastGrants, wantProvider, wantGrants)
		}
		lastProvider, lastGrants = p, g
	}

	c, err := a2aserver.Dial(context.Background(), ts.URL, bearerSource{"Bearer " + tokOK}, hc)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	expectCalls("dial (the agent card is public)", 0, 0)

	// A task through the official wrapper client.
	ctx := func() context.Context {
		cx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(cancel)
		return cx
	}
	res, err := c.Send(ctx(), a2aserver.SendInput{Project: projectP1, Job: "job-1", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("send result = %T, want *a2a.Task", res)
	}
	expectCalls("send", 1, 1)
	item := (&agentEnv{store: store}).taskOf(t, string(task.ID))
	if item.Subject != subject {
		t.Fatalf("stored owner = %q, want the provider subject %q", item.Subject, subject)
	}

	// Forged identity metadata on the message and on the request is
	// ignored: the owner stays the provider subject.
	raw, err := a2aclient.NewFromCard(ctx(), c.Card(),
		a2aclient.WithConfig(a2aclient.Config{PreferredTransports: []a2a.TransportProtocol{a2a.TransportProtocolHTTPJSON}}),
		a2aclient.WithRESTTransport(hc),
		a2aclient.WithCallInterceptors(idxAuthInterceptor{value: "Bearer " + tokOK}))
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer func() { _ = raw.Destroy() }()
	forged := map[string]any{"nau.subject": forgedSubject, "nau.role": "guardian", "actor": forgedSubject, "role": "owner"}
	msg := &a2a.Message{
		ID: "m-forged", Role: a2a.MessageRoleUser, Parts: a2a.ContentParts{a2a.NewTextPart("forged metadata")},
		Metadata: map[string]any{
			a2acore.MetaSession: "s-forged", a2acore.MetaEpoch: float64(1), a2acore.MetaSeq: float64(1),
			a2acore.MetaProject: projectP1, a2acore.MetaJob: "job-1",
			"nau.subject": forgedSubject, "nau.role": "guardian", "actor": forgedSubject, "role": "owner",
		},
	}
	fres, err := raw.SendMessage(ctx(), &a2a.SendMessageRequest{
		Message: msg, Metadata: forged, Config: &a2a.SendMessageConfig{ReturnImmediately: true},
	})
	if err != nil {
		t.Fatalf("send with forged metadata: %v", err)
	}
	ftask, ok := fres.(*a2a.Task)
	if !ok {
		t.Fatalf("forged send result = %T, want *a2a.Task", fres)
	}
	expectCalls("forged send", 1, 1)
	if got := (&agentEnv{store: store}).taskOf(t, string(ftask.ID)).Subject; got != subject {
		t.Fatalf("owner of the forged-metadata task = %q, want the provider subject %q", got, subject)
	}
	(&agentEnv{store: store}).pollTask(t, string(task.ID), a2acore.StateWorking)

	// A push config with a token and credentials canary.
	if _, err = raw.CreateTaskPushConfig(ctx(), &a2a.PushConfig{
		TaskID: task.ID, URL: "http://127.0.0.1:9", Token: pushToken,
		Auth: &a2a.PushAuthInfo{Scheme: "Bearer", Credentials: pushCred},
	}); err != nil {
		t.Fatalf("create the push config: %v", err)
	}
	expectCalls("push config", 1, 1)
	pushResp := rec.last(t)

	// The owner notices are delivered through the fake sender while the
	// tasks are working (a terminal task obsoletes its deliveries).
	env := &agentEnv{store: store, clock: clock, sender: newFakeSender()}
	(&agentEnv{store: store}).pollTask(t, string(ftask.ID), a2acore.StateWorking)
	env.deliver(t)
	if env.sender.count() == 0 {
		t.Fatal("no notice was delivered: the notice scan would be vacuous")
	}

	// The owner answers: the push fires in memory and the task
	// completes.
	env.recordDecision(t, a2acore.Decision{ID: "dec-id-1", TaskID: string(task.ID), Kind: a2acore.DecisionAnswer, Text: "answered", Actor: "owner-a"})
	if applied, refused, err := srv.ApplyPendingDecisions(context.Background()); err != nil || applied != 1 || refused != 0 {
		t.Fatalf("apply = applied %d refused %d err %v, want applied 1", applied, refused, err)
	}
	env.pollTask(t, string(task.ID), a2acore.StateCompleted)

	// A provider failure: HTTP 500, a fixed error, the provider's text
	// never surfaces; the grant source is never reached.
	cErr, err := a2aserver.Dial(context.Background(), ts.URL, bearerSource{"Bearer " + tokErr}, hc)
	if err != nil {
		t.Fatalf("dial for the provider failure: %v", err)
	}
	defer func() { _ = cErr.Close() }()
	expectCalls("dial for the provider failure", 0, 0)
	_, err = cErr.Get(ctx(), string(task.ID))
	if err == nil {
		t.Fatal("a request whose provider failed succeeded")
	}
	if !errors.Is(err, a2a.ErrServerError) {
		t.Errorf("provider failure error = %v, want a server error", err)
	}
	for _, canary := range []string{providerCanary, tokErr} {
		if strings.Contains(err.Error(), canary) {
			t.Errorf("provider failure error text carries %q", canary[:20])
		}
	}
	if r := rec.last(t); r.status != http.StatusInternalServerError {
		t.Errorf("provider failure status = %d, want 500", r.status)
	}
	expectCalls("provider failure", 1, 0)

	// Revocation lives in the source: nothing is cached.
	provider.revoke(tokOK)
	_, err = c.Get(ctx(), string(task.ID))
	if err == nil {
		t.Fatal("a revoked token was accepted")
	}
	if !errors.Is(err, a2a.ErrUnauthenticated) {
		t.Errorf("revoked token error = %v, want unauthenticated", err)
	}
	expectCalls("revoked token", 1, 0)

	// Close everything, then scan every durable artifact and output.
	ts.Close()
	tsOpen = false
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	storeOpen = false

	canaries := map[string]string{
		"bearer token": tokOK, "bearer header": "Bearer " + tokOK, "provider-failure token": tokErr,
		"provider error text": providerCanary, "push token": pushToken, "push credentials": pushCred,
	}
	type artifact struct {
		name string
		data []byte
	}
	var artifacts []artifact
	for _, suffix := range []string{"", "-wal", "-shm"} {
		data, err := readIfExists(dbPath + suffix)
		if err != nil {
			t.Fatalf("read database file%s: %v", suffix, err)
		}
		if data != nil {
			artifacts = append(artifacts, artifact{"database file" + suffix, data})
		}
	}
	columns, cells := scanAllCells(t, dbPath)
	for _, cell := range cells {
		artifacts = append(artifacts, artifact{"a table cell", []byte(cell)})
	}
	artifacts = append(artifacts, artifact{"log buffer", logb.Bytes()})
	rec.mu.Lock()
	for _, r := range rec.resps {
		if r.status == pushResp.status && bytes.Equal(r.body, pushResp.body) {
			continue // the push config echo is asserted separately
		}
		artifacts = append(artifacts, artifact{"an HTTP response body", r.body})
	}
	rec.mu.Unlock()
	for _, call := range env.sender.callsCopy() {
		if call.Ref != ownerRefA {
			t.Errorf("a notice went to %q, want the owner %q", call.Ref, ownerRefA)
		}
		artifacts = append(artifacts, artifact{"a delivered notice", []byte(call.Message)})
	}
	for name, canary := range canaries {
		for _, art := range artifacts {
			if bytes.Contains(art.data, []byte(canary)) {
				t.Errorf("the %s canary appears in %s", name, art.name)
			}
		}
	}
	// The push response may only echo the push config back to its
	// owner; it never carries the bearer or provider canaries.
	for _, name := range []string{"bearer token", "bearer header", "provider-failure token", "provider error text"} {
		if bytes.Contains(pushResp.body, []byte(canaries[name])) {
			t.Errorf("the %s canary appears in the push config response", name)
		}
	}
	if len(cells) == 0 || len(columns) == 0 {
		t.Fatal("the row scan read nothing")
	}

	// The grant is derived per request and never stored: no table or
	// column is named for it and no cell holds a serialized grant.
	for table, cols := range columns {
		for _, name := range append([]string{table}, cols...) {
			low := strings.ToLower(name)
			for _, word := range []string{"grant", "scope", "permission", "authz"} {
				if strings.Contains(low, word) {
					t.Errorf("table or column %q looks like a stored authorization", name)
				}
			}
		}
	}
	for _, cell := range cells {
		if strings.Contains(cell, `"actions"`) && strings.Contains(cell, `"projects"`) {
			t.Errorf("a cell holds a serialized grant: %.80s", cell)
		}
	}
	if subjectStored := bytes.Contains(artifacts[0].data, []byte(subject)); !subjectStored {
		t.Errorf("the provider subject %q is not stored as the owner", subject)
	}
}

// readIfExists reads one file; a missing file is nil data, not an error.
func readIfExists(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return data, err
}
