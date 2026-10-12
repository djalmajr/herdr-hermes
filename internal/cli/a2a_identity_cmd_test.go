package cli

// The identity wiring of a2a serve (DJA-283, acceptance item 4): the
// runtime has two distinct policies. A request without an Authorization
// header is the loopback local operator; a request with one goes to the
// injected identity provider, whose grants come from the notify routing
// and are re-read on every request. No identity source is configured in
// this release, so the unconfigured runtime fails closed for a
// credential; these tests inject a generated fixture provider through
// the package seam. They run the real serve in-process and never run in
// parallel with each other (they replace the seam).

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
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
	"github.com/djalmajr/nau/internal/a2aserver"
	"github.com/djalmajr/nau/internal/a2astore"
	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/outbox"
)

// fixtureIdentity is the injected identity provider fixture: a table of
// generated tokens to identities, with a call counter. An unknown token
// is an error wrapping ErrUnauthenticated.
type fixtureIdentity struct {
	mu     sync.Mutex
	tokens map[string]a2acore.Identity
	calls  int
}

// Verify implements a2acore.IdentityProvider.
func (f *fixtureIdentity) Verify(_ context.Context, token string) (a2acore.Identity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	id, ok := f.tokens[token]
	if !ok {
		return a2acore.Identity{}, a2acore.ErrUnauthenticated
	}
	return id, nil
}

// bearerCredential is the test CredentialSource of one Authorization
// value.
type bearerCredential string

// Authorization implements a2aserver.CredentialSource.
func (b bearerCredential) Authorization(context.Context) (string, error) { return string(b), nil }

// a2aRandomToken is one generated random fixture token.
func a2aRandomToken(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("random token: %v", err)
	}
	return "tok-" + hex.EncodeToString(b[:])
}

// setIdentitySeam installs the identity provider seam for one test and
// restores it on cleanup.
func setIdentitySeam(t *testing.T, p a2acore.IdentityProvider) {
	t.Helper()
	orig := a2aIdentityProvider
	a2aIdentityProvider = p
	t.Cleanup(func() { a2aIdentityProvider = orig })
}

// a2aStartServeCleanup starts serve and stops it on cleanup.
func a2aStartServeCleanup(t *testing.T, cfgDir string) (base string) {
	t.Helper()
	base, exitCh, cancel, _, _ := startA2AServe(t, cfgDir, "--listen", "127.0.0.1:0")
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	return base
}

// a2aDialBearer dials the official SDK client with one Authorization
// value.
func a2aDialBearer(t *testing.T, base, authorization string) *a2aserver.Client {
	t.Helper()
	c, err := a2aserver.Dial(context.Background(), base, bearerCredential(authorization), &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// a2aSendCtx is one bounded call context.
func a2aSendCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// a2aInboxOf lists the inbox through a second store handle on the serve
// database file.
func a2aInboxOf(t *testing.T, cfgDir string) []a2acore.InboxItem {
	t.Helper()
	store, err := a2astore.OpenFile(filepath.Join(outbox.StateDir(cfgDir), "registry.db"), a2astore.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()
	items, err := store.Inbox(context.Background(), a2acore.InboxFilter{})
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	return items
}

// a2aSetOwner sets or (empty ref) removes the notify owner of a project.
func a2aSetOwner(t *testing.T, cfgDir, project, ref string) {
	t.Helper()
	store, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return serveFixedNow }})
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	if err := notify.Open(store, func() time.Time { return serveFixedNow }).UpdateRegistry(func(reg *notify.Registry) error {
		if ref == "" {
			delete(reg.Owners, project)
		} else {
			reg.Owners[project] = ref
		}
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
}

// a2aRawStatus does one raw GET with an optional Authorization header
// and Host override and returns the HTTP status.
func a2aRawStatus(t *testing.T, url, authorization, host string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	if host != "" {
		req.Host = host
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// TestA2AIdentityUnconfigured: with no identity provider (the state of
// this release) a request that carries a credential fails closed with a
// server error and creates nothing, the local operator without a header
// still works, and the loopback boundaries are unchanged.
func TestA2AIdentityUnconfigured(t *testing.T) {
	setIdentitySeam(t, nil)
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	base := a2aStartServeCleanup(t, cfgDir)

	// A bearer request fails closed: HTTP 500, never a task.
	bearer := "Bearer " + a2aRandomToken(t)
	if got := a2aRawStatus(t, base+"/tasks/none", bearer, ""); got != http.StatusInternalServerError {
		t.Fatalf("raw bearer request status = %d, want 500", got)
	}
	c := a2aDialBearer(t, base, bearer)
	_, err := c.Send(a2aSendCtx(t), a2aserver.SendInput{Project: "org/example", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-bearer", Text: "via bearer"})
	if err == nil {
		t.Fatal("bearer send succeeded without an identity provider")
	}
	if !errors.Is(err, a2a.ErrServerError) {
		t.Errorf("bearer send error = %v, want a server error", err)
	}
	if strings.Contains(err.Error(), strings.TrimPrefix(bearer, "Bearer ")) {
		t.Errorf("error text carries the credential: %v", err)
	}
	if items := a2aInboxOf(t, cfgDir); len(items) != 0 {
		t.Fatalf("inbox after the refused bearer send = %+v, want no task row", items)
	}

	// The local operator (no header) still works and is the reserved
	// local subject.
	stdout, _, exit := runA2A(t, cfgDir, "send", "--server", base,
		"--session", "s-local", "--epoch", "1", "--seq", "1",
		"--project", "org/example", "--message-id", "m-local", "via local operator")
	if exit != 0 {
		t.Fatalf("local send exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	items := a2aInboxOf(t, cfgDir)
	if len(items) != 1 || items[0].Subject != a2acore.LocalSubjectID {
		t.Fatalf("inbox = %+v, want exactly the local operator's task", items)
	}

	// Host guard unchanged.
	if got := a2aRawStatus(t, base+"/.well-known/agent-card.json", "", "rebind.example:7430"); got != http.StatusForbidden {
		t.Fatalf("non-loopback Host status = %d, want 403", got)
	}
}

// TestA2AIdentityLoopbackDefault: the non-loopback --listen refusal and
// its motivo are unchanged whether or not an identity provider is
// injected.
func TestA2AIdentityLoopbackDefault(t *testing.T) {
	for name, provider := range map[string]a2acore.IdentityProvider{
		"unconfigured": nil,
		"configured":   &fixtureIdentity{tokens: map[string]a2acore.Identity{}},
	} {
		t.Run(name, func(t *testing.T) {
			setIdentitySeam(t, provider)
			dir := t.TempDir()
			stdout, _, exit := runA2A(t, dir, "serve", "--listen", "0.0.0.0:7430")
			if exit != 2 {
				t.Fatalf("exit = %d, want 2 (stdout %q)", exit, stdout)
			}
			want := fmt.Sprintf(`{"status":"2","motivo":%q}`+"\n", a2aLoopbackMotivo)
			if stdout != want {
				t.Fatalf("stdout = %q, want %q", stdout, want)
			}
			assertDirEmpty(t, dir)
		})
	}
}

// TestA2AIdentityConfigured: a fixture provider injected through the
// seam serves a bearer with grants derived from the notify routing,
// re-read on every request.
func TestA2AIdentityConfigured(t *testing.T) {
	const subject = "subject-fixture-1"
	token := a2aRandomToken(t)
	provider := &fixtureIdentity{tokens: map[string]a2acore.Identity{
		token: {Subject: subject, Ref: "owner-a"},
	}}
	setIdentitySeam(t, provider)
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir) // org/example is owned by owner-a
	a2aSetOwner(t, cfgDir, "org/other", "owner-b")
	base := a2aStartServeCleanup(t, cfgDir)
	c := a2aDialBearer(t, base, "Bearer "+token)

	// A send to the owned project is accepted and owned by the
	// provider subject.
	res, err := c.Send(a2aSendCtx(t), a2aserver.SendInput{Project: "org/example", Session: "s-1", Epoch: 1, Seq: 1, MessageID: "m-1", Text: "first"})
	if err != nil {
		t.Fatalf("bearer send to the owned project: %v", err)
	}
	task, ok := res.(*a2a.Task)
	if !ok {
		t.Fatalf("send result = %T, want *a2a.Task", res)
	}
	items := a2aInboxOf(t, cfgDir)
	if len(items) != 1 || items[0].TaskID != string(task.ID) || items[0].Subject != subject {
		t.Fatalf("inbox = %+v, want one task owned by %s", items, subject)
	}

	// A project the reference does not own is refused, and nothing is
	// created.
	_, err = c.Send(a2aSendCtx(t), a2aserver.SendInput{Project: "org/other", Session: "s-2", Epoch: 1, Seq: 1, MessageID: "m-other", Text: "not mine"})
	if err == nil {
		t.Fatal("send to a project the reference does not own succeeded")
	}
	if !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("refused send error = %v, want unauthorized", err)
	}
	if n := len(a2aInboxOf(t, cfgDir)); n != 1 {
		t.Fatalf("inbox rows = %d after the refused send, want 1", n)
	}

	// The owner asks a question (the execution ends, the task awaits
	// the caller); a continuation keeps the same task and context ids.
	waitInbox := func(want string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if its := a2aInboxOf(t, cfgDir); len(its) == 1 && its[0].State == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("task never reached %s", want)
			}
			select {
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	waitInbox(a2acore.StateWorking)
	_, stderrOut, exit := runA2A(t, cfgDir, "reply", "--task", string(task.ID), "--ask", "--decision-id", "d-ask-1", "--db", filepath.Join(outbox.StateDir(cfgDir), "registry.db"), "which branch?")
	if exit != 0 {
		t.Fatalf("reply --ask exit = %d (stderr %q)", exit, stderrOut)
	}
	waitInbox(a2acore.StateInputRequired)

	res, err = c.Send(a2aSendCtx(t), a2aserver.SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 2, MessageID: "m-2", Text: "the main branch",
	})
	if err != nil {
		t.Fatalf("continuation: %v", err)
	}
	cont, ok := res.(*a2a.Task)
	if !ok || cont.ID != task.ID || cont.ContextID != task.ContextID {
		t.Fatalf("continuation = %+v, want the same task %s and context %s", res, task.ID, task.ContextID)
	}
	waitInbox(a2acore.StateWorking)

	// Authorization is re-read on every request: after the owner is
	// unregistered the same credential is refused on the next request.
	// (Park the task again so a continuation is admissible.)
	_, stderrOut, exit = runA2A(t, cfgDir, "reply", "--task", string(task.ID), "--ask", "--decision-id", "d-ask-2", "--db", filepath.Join(outbox.StateDir(cfgDir), "registry.db"), "and the tag?")
	if exit != 0 {
		t.Fatalf("second reply --ask exit = %d (stderr %q)", exit, stderrOut)
	}
	waitInbox(a2acore.StateInputRequired)
	a2aSetOwner(t, cfgDir, "org/example", "")
	provider.mu.Lock()
	before := provider.calls
	provider.mu.Unlock()
	_, err = c.Send(a2aSendCtx(t), a2aserver.SendInput{
		TaskID: string(task.ID), ContextID: task.ContextID,
		Session: "s-1", Epoch: 1, Seq: 3, MessageID: "m-3", Text: "after the unregistration",
	})
	if err == nil {
		t.Fatal("continuation after the owner was unregistered succeeded")
	}
	if !errors.Is(err, a2a.ErrUnauthorized) {
		t.Errorf("continuation error = %v, want unauthorized", err)
	}
	provider.mu.Lock()
	after := provider.calls
	provider.mu.Unlock()
	if after != before+1 {
		t.Errorf("provider calls for the refused continuation = %d, want 1 (verified on every request)", after-before)
	}
}

// TestA2AIdentityNowrite: the NOWRITE refusal of serve is unchanged by
// the identity wiring: same JSON, exit 2, nothing written.
func TestA2AIdentityNowrite(t *testing.T) {
	setIdentitySeam(t, &fixtureIdentity{tokens: map[string]a2acore.Identity{}})
	dir := t.TempDir()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(true),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: dir,
		Now:       func() time.Time { return serveFixedNow },
		Sleep:     sleepCtx,
	}
	if exit := cmdA2A([]string{"serve", "--listen", "127.0.0.1:0"}, env); exit != 2 {
		t.Fatalf("exit = %d, want 2 (stdout %q)", exit, out.String())
	}
	if out.String() != nowriteErrorJSON+"\n" {
		t.Errorf("stdout = %q, want the nowrite line", out.String())
	}
	assertDirEmpty(t, dir)
}
