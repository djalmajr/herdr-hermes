package a2astore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
)

var (
	errInjectedPut    = errors.New("injected put failure")
	errInjectedDelete = errors.New("injected delete failure")
)

// faultSecrets wraps a real SecretProvider, records every call in order
// and injects failures: the failAt-th Put since armPut fails, and a
// Delete of a handle in failDelete fails without deleting. onDelete runs
// before every Delete. live and isLive answer through the wrapped
// provider, so a leaked handle is found by what the provider still
// resolves, never by the wrapper's bookkeeping.
type faultSecrets struct {
	mu         sync.Mutex
	inner      a2acore.SecretProvider
	ops        []string
	handles    []string
	putCalls   int
	failAt     int
	failDelete map[string]bool
	failUnless map[string]bool // when set, every Delete of a handle outside it fails
	onDelete   func(handle string)
}

func newFaultSecrets(inner a2acore.SecretProvider) *faultSecrets {
	return &faultSecrets{inner: inner, failDelete: map[string]bool{}}
}

// Put implements a2acore.SecretProvider.
func (f *faultSecrets) Put(ctx context.Context, value string) (string, error) {
	f.mu.Lock()
	f.putCalls++
	fail := f.failAt != 0 && f.putCalls == f.failAt
	f.ops = append(f.ops, "put")
	f.mu.Unlock()
	if fail {
		return "", errInjectedPut
	}
	h, err := f.inner.Put(ctx, value)
	if err != nil {
		return "", err
	}
	f.mu.Lock()
	f.handles = append(f.handles, h)
	f.mu.Unlock()
	return h, nil
}

// Get implements a2acore.SecretProvider.
func (f *faultSecrets) Get(ctx context.Context, handle string) (string, error) {
	return f.inner.Get(ctx, handle)
}

// Delete implements a2acore.SecretProvider.
func (f *faultSecrets) Delete(ctx context.Context, handle string) error {
	f.mu.Lock()
	f.ops = append(f.ops, "delete")
	hook := f.onDelete
	fail := f.failDelete[handle] || (f.failUnless != nil && !f.failUnless[handle])
	f.mu.Unlock()
	if hook != nil {
		hook(handle)
	}
	if fail {
		return errInjectedDelete
	}
	return f.inner.Delete(ctx, handle)
}

// armPut makes the n-th Put from now on fail (1 is the next one).
func (f *faultSecrets) armPut(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putCalls, f.failAt = 0, n
}

// disarm clears every injected failure and hook.
func (f *faultSecrets) disarm() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAt, f.onDelete, f.failUnless = 0, nil, nil
	f.failDelete = map[string]bool{}
}

// failDeleteOf makes the Delete of handle fail.
func (f *faultSecrets) failDeleteOf(handle string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failDelete[handle] = true
}

// resetOps forgets the recorded call order.
func (f *faultSecrets) resetOps() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = nil
}

// callOrder is the recorded calls since resetOps.
func (f *faultSecrets) callOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ops...)
}

// isLive reports whether the provider still resolves handle.
func (f *faultSecrets) isLive(handle string) bool {
	_, err := f.inner.Get(context.Background(), handle)
	return err == nil
}

// live counts the handles ever put that the provider still resolves.
func (f *faultSecrets) live() int {
	f.mu.Lock()
	handles := append([]string(nil), f.handles...)
	f.mu.Unlock()
	n := 0
	for _, h := range handles {
		if f.isLive(h) {
			n++
		}
	}
	return n
}

// secretValue is a fixture secret generated at run time.
func secretValue(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return "cv-" + hex.EncodeToString(b[:])
}

// pushEnv is one store with a committed task t1 whose config pc1 holds an
// old token and old credentials.
type pushEnv struct {
	s           *Store
	path        string
	sec         *faultSecrets // nil when the provider is the bare native one
	provider    a2acore.SecretProvider
	oldToken    string
	oldCreds    string
	oldTokenRef string
	oldCredsRef string
}

func pushConfigOf(token, creds string) *a2a.PushConfig {
	return &a2a.PushConfig{
		ID:    "pc1",
		URL:   "http://127.0.0.1:9999/push",
		Token: token,
		Auth:  &a2a.PushAuthInfo{Scheme: "Bearer", Credentials: creds},
	}
}

// newPushEnv opens a store over provider (a *faultSecrets is also kept
// as env.sec), creates t1 and saves pc1 with the old secret values.
func newPushEnv(t *testing.T, provider a2acore.SecretProvider) *pushEnv {
	t.Helper()
	clk := &clock{t: testNow}
	s, path := openStore(t, clk, "alice", provider)
	env := &pushEnv{s: s, path: path, provider: provider, oldToken: secretValue(t), oldCreds: secretValue(t)}
	env.sec, _ = provider.(*faultSecrets)
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(env.oldToken, env.oldCreds)); err != nil {
		t.Fatalf("Save the old config: %v", err)
	}
	env.oldTokenRef, env.oldCredsRef = env.refs(t)
	return env
}

// refs reads the stored handles of pc1 (handles are not secret).
func (e *pushEnv) refs(t *testing.T) (string, string) {
	t.Helper()
	var tok, creds sql.NullString
	if err := e.s.db.QueryRow(`SELECT token_ref, credentials_ref FROM a2a_push_configs WHERE task_id = 't1' AND config_id = 'pc1'`).Scan(&tok, &creds); err != nil {
		t.Fatalf("read the stored handles: %v", err)
	}
	return tok.String, creds.String
}

// rowCount is the number of stored rows of pc1.
func (e *pushEnv) rowCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.s.db.QueryRow(`SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = 't1' AND config_id = 'pc1'`).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

// requireUsable fails unless Get and List return the config with exactly
// the given secret values, the row still carries wantTokenRef and
// wantCredsRef, and the database file carries no canary.
func (e *pushEnv) requireUsable(t *testing.T, what, token, creds, wantTokenRef, wantCredsRef string, canaries ...string) {
	t.Helper()
	got, err := e.s.PushConfigs().Get(context.Background(), "t1", "pc1")
	if err != nil {
		t.Errorf("%s: Get = %v, want the config", what, err)
	} else if got.Token != token || got.Auth == nil || got.Auth.Credentials != creds {
		t.Errorf("%s: Get returned other secret values than %s", what, "expected")
	}
	list, err := e.s.PushConfigs().List(context.Background(), "t1")
	if err != nil || len(list) != 1 || list[0].Token != token {
		t.Errorf("%s: List length = %d, err = %v, want the one config", what, len(list), err)
	}
	if n, err := e.s.PushConfigs().UnavailableCount(context.Background(), "t1"); err != nil || n != 0 {
		t.Errorf("%s: UnavailableCount = %d, %v, want 0", what, n, err)
	}
	if gotTok, gotCreds := e.refs(t); gotTok != wantTokenRef || gotCreds != wantCredsRef {
		t.Errorf("%s: the row refs changed (token same %v, credentials same %v)", what, gotTok == wantTokenRef, gotCreds == wantCredsRef)
	}
	if err := assertSecretsAbsent(t, e.path, canaries); err != nil {
		t.Errorf("%s: a secret value reached the database file", what)
	}
}

// requireNoCanary fails when the error text carries a secret value.
func requireNoCanary(t *testing.T, what string, err error, canaries ...string) {
	t.Helper()
	if err == nil {
		return
	}
	for _, c := range canaries {
		if strings.Contains(err.Error(), c) {
			t.Errorf("%s: the error text carries a secret value", what)
		}
	}
}

// execTest runs setup SQL on the store's own connection.
func execTest(t *testing.T, s *Store, query string) {
	t.Helper()
	if _, err := s.db.Exec(query); err != nil {
		t.Fatalf("test SQL: %v", err)
	}
}

// providerCases are the providers the replace failures run over: the bare
// native in-memory provider (usability only) and the native provider
// behind the fault wrapper (usability and leak counts).
func providerCases() []struct {
	name string
	make func() a2acore.SecretProvider
} {
	return []struct {
		name string
		make func() a2acore.SecretProvider
	}{
		{"native", func() a2acore.SecretProvider { return a2acore.NewMemorySecrets() }},
		{"native behind the fault wrapper", func() a2acore.SecretProvider { return newFaultSecrets(a2acore.NewMemorySecrets()) }},
	}
}

// TestPushReplaceSecondPutFailsKeepsOldConfig proves a replace whose
// Put fails (the token's or the credentials') leaves the committed old
// config usable with its old handles, leaks no new handle and returns
// the provider error without a secret value.
func TestPushReplaceSecondPutFailsKeepsOldConfig(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		name := "token put fails"
		if failAt == 2 {
			name = "credentials put fails"
		}
		t.Run(name, func(t *testing.T) {
			env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
			newToken, newCreds := secretValue(t), secretValue(t)
			env.sec.armPut(failAt)
			_, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(newToken, newCreds))
			if !errors.Is(err, errInjectedPut) {
				t.Fatalf("Save error is the injected put failure: %v, want true", errors.Is(err, errInjectedPut))
			}
			requireNoCanary(t, "Save", err, env.oldToken, env.oldCreds, newToken, newCreds)
			env.requireUsable(t, "after the failed replace", env.oldToken, env.oldCreds, env.oldTokenRef, env.oldCredsRef,
				env.oldToken, env.oldCreds, newToken, newCreds)
			if n := env.sec.live(); n != 2 {
				t.Errorf("live handles = %d, want the 2 old ones (no new handle leaked)", n)
			}
			// The old config still replaces cleanly afterwards.
			env.sec.disarm()
			if _, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(newToken, newCreds)); err != nil {
				t.Fatalf("Save after the failure: %v", err)
			}
			tok, creds := env.refs(t)
			env.requireUsable(t, "after the retry", newToken, newCreds, tok, creds, env.oldToken, env.oldCreds, newToken, newCreds)
			if n := env.sec.live(); n != 2 {
				t.Errorf("live handles after the retry = %d, want 2 (the old ones are gone)", n)
			}
		})
	}
}

// TestPushReplaceSQLAbortKeepsOldConfig proves a deterministic abort of
// the upsert (a test trigger) leaves the old config usable and leaks no
// new handle, with the native in-memory provider.
func TestPushReplaceSQLAbortKeepsOldConfig(t *testing.T) {
	for _, pc := range providerCases() {
		t.Run(pc.name, func(t *testing.T) {
			env := newPushEnv(t, pc.make())
			execTest(t, env.s, `CREATE TRIGGER test_abort_upsert BEFORE UPDATE ON a2a_push_configs BEGIN SELECT RAISE(ABORT, 'test abort'); END`)
			newToken, newCreds := secretValue(t), secretValue(t)
			_, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(newToken, newCreds))
			if err == nil {
				t.Fatal("Save succeeded, want the aborted upsert")
			}
			requireNoCanary(t, "Save", err, env.oldToken, env.oldCreds, newToken, newCreds)
			env.requireUsable(t, "after the aborted replace", env.oldToken, env.oldCreds, env.oldTokenRef, env.oldCredsRef,
				env.oldToken, env.oldCreds, newToken, newCreds)
			if env.sec != nil {
				if n := env.sec.live(); n != 2 {
					t.Errorf("live handles = %d, want the 2 old ones", n)
				}
			}
		})
	}
}

// TestPushFirstSaveSQLAbortLeaksNothing proves a first save whose insert
// aborts leaves no row and no live handle.
func TestPushFirstSaveSQLAbortLeaksNothing(t *testing.T) {
	sec := newFaultSecrets(a2acore.NewMemorySecrets())
	clk := &clock{t: testNow}
	s, path := openStore(t, clk, "alice", sec)
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	execTest(t, s, `CREATE TRIGGER test_abort_insert BEFORE INSERT ON a2a_push_configs BEGIN SELECT RAISE(ABORT, 'test abort'); END`)
	token, creds := secretValue(t), secretValue(t)
	_, err := s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(token, creds))
	if err == nil {
		t.Fatal("Save succeeded, want the aborted insert")
	}
	requireNoCanary(t, "Save", err, token, creds)
	if n := sec.live(); n != 0 {
		t.Errorf("live handles = %d, want 0 (the new handles are compensated)", n)
	}
	if _, err := s.PushConfigs().Get(context.Background(), "t1", "pc1"); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Errorf("Get after the aborted first save: %v, want not found", err)
	}
	if err := assertSecretsAbsent(t, path, []string{token, creds}); err != nil {
		t.Errorf("a secret value reached the database file")
	}
}

// TestPushReplaceCommitFailureKeepsOldConfig proves a replace whose
// COMMIT fails (a deferred foreign key violated by a test trigger)
// leaves the old config usable and leaks no new handle.
func TestPushReplaceCommitFailureKeepsOldConfig(t *testing.T) {
	for _, pc := range providerCases() {
		t.Run(pc.name, func(t *testing.T) {
			env := newPushEnv(t, pc.make())
			execTest(t, env.s, `CREATE TABLE test_parent(id INTEGER PRIMARY KEY)`)
			execTest(t, env.s, `CREATE TABLE test_child(pid INTEGER REFERENCES test_parent(id) DEFERRABLE INITIALLY DEFERRED)`)
			execTest(t, env.s, `CREATE TRIGGER test_commit_fail AFTER UPDATE ON a2a_push_configs BEGIN INSERT INTO test_child(pid) VALUES(999); END`)
			newToken, newCreds := secretValue(t), secretValue(t)
			_, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(newToken, newCreds))
			if err == nil {
				t.Fatal("Save succeeded, want the failed commit")
			}
			if !strings.Contains(strings.ToUpper(err.Error()), "FOREIGN KEY") {
				t.Fatalf("Save failed for another reason than the deferred foreign key (the commit): the failure is not the foreign key")
			}
			requireNoCanary(t, "Save", err, env.oldToken, env.oldCreds, newToken, newCreds)
			env.requireUsable(t, "after the failed commit", env.oldToken, env.oldCreds, env.oldTokenRef, env.oldCredsRef,
				env.oldToken, env.oldCreds, newToken, newCreds)
			if env.sec != nil {
				if n := env.sec.live(); n != 2 {
					t.Errorf("live handles = %d, want the 2 old ones", n)
				}
			}
		})
	}
}

// TestPushReplaceCleanupFailureAfterCommit is the declared boundary of
// the replace: a provider Delete that fails after the commit leaves the
// obsolete handle in the provider. The failure is a true error (the text
// says the config was stored and the cleanup of an obsolete handle
// failed, the cause is kept), the new config is usable, and exactly the
// one handle whose delete failed leaks. A durable ledger of obsolete
// handles is out of scope here.
func TestPushReplaceCleanupFailureAfterCommit(t *testing.T) {
	env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
	env.sec.failDeleteOf(env.oldTokenRef)
	newToken, newCreds := secretValue(t), secretValue(t)
	cfg, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(newToken, newCreds))
	if err == nil {
		t.Fatal("Save returned success although the obsolete handle cleanup failed")
	}
	if cfg != nil {
		t.Errorf("Save returned a config together with the error")
	}
	if !errors.Is(err, errInjectedDelete) {
		t.Errorf("the cause of the cleanup failure is lost: errors.Is = false")
	}
	msg := err.Error()
	if !strings.Contains(msg, "stored") || !strings.Contains(msg, "obsolete") {
		t.Errorf("the error does not say that the config was stored and the obsolete handle cleanup failed")
	}
	requireNoCanary(t, "Save", err, env.oldToken, env.oldCreds, newToken, newCreds)
	tok, creds := env.refs(t)
	env.requireUsable(t, "after the failed cleanup", newToken, newCreds, tok, creds, env.oldToken, env.oldCreds, newToken, newCreds)
	if tok == env.oldTokenRef || creds == env.oldCredsRef {
		t.Errorf("the row still points at an old handle")
	}
	// Two new handles live, plus the one obsolete handle whose delete
	// failed; the other obsolete handle was still deleted.
	if n := env.sec.live(); n != 3 {
		t.Errorf("live handles = %d, want 3 (2 new + 1 leaked obsolete)", n)
	}
	if !env.sec.isLive(env.oldTokenRef) || env.sec.isLive(env.oldCredsRef) {
		t.Errorf("leaked handle is not exactly the one whose delete failed")
	}
}

// TestPushReplaceOrder pins the order of a successful replace: both Puts
// before any Delete, and the obsolete handles are deleted only after the
// commit (the delete hook reads the committed row through a second
// read-only connection and sees the new handles already stored).
func TestPushReplaceOrder(t *testing.T) {
	env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
	ro, err := sql.Open("sqlite", "file:"+filepath.ToSlash(env.path)+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer ro.Close()
	var seenNewRefs []bool
	env.sec.mu.Lock()
	env.sec.onDelete = func(string) {
		var tok, creds sql.NullString
		if err := ro.QueryRow(`SELECT token_ref, credentials_ref FROM a2a_push_configs WHERE task_id = 't1' AND config_id = 'pc1'`).Scan(&tok, &creds); err != nil {
			seenNewRefs = append(seenNewRefs, false)
			return
		}
		seenNewRefs = append(seenNewRefs, tok.String != env.oldTokenRef && creds.String != env.oldCredsRef)
	}
	env.sec.mu.Unlock()
	env.sec.resetOps()
	newToken, newCreds := secretValue(t), secretValue(t)
	if _, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(newToken, newCreds)); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got, want := env.sec.callOrder(), []string{"put", "put", "delete", "delete"}; !reflect.DeepEqual(got, want) {
		t.Errorf("provider call order = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(seenNewRefs, []bool{true, true}) {
		t.Errorf("the committed row held the new handles at each obsolete delete = %v, want [true true]", seenNewRefs)
	}
	tok, creds := env.refs(t)
	env.requireUsable(t, "after the replace", newToken, newCreds, tok, creds, env.oldToken, env.oldCreds, newToken, newCreds)
	if n := env.sec.live(); n != 2 {
		t.Errorf("live handles = %d, want 2 (the obsolete ones are deleted)", n)
	}
}

// TestPushReplaceCompensationFailureJoined proves a failed compensation
// delete of a freshly put handle is joined to the returned error (both
// causes stay visible), and that only the handle whose delete failed leaks.
func TestPushReplaceCompensationFailureJoined(t *testing.T) {
	env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
	env.sec.armPut(2)
	// Only the two old handles may be deleted: the compensation delete of
	// the freshly put token handle fails.
	env.sec.mu.Lock()
	env.sec.failUnless = map[string]bool{env.oldTokenRef: true, env.oldCredsRef: true}
	env.sec.mu.Unlock()
	newToken, newCreds := secretValue(t), secretValue(t)
	_, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(newToken, newCreds))
	if !errors.Is(err, errInjectedPut) || !errors.Is(err, errInjectedDelete) {
		t.Fatalf("Save error carries the put cause = %v and the compensation cause = %v, want both",
			errors.Is(err, errInjectedPut), errors.Is(err, errInjectedDelete))
	}
	requireNoCanary(t, "Save", err, env.oldToken, env.oldCreds, newToken, newCreds)
	env.requireUsable(t, "after the failed replace", env.oldToken, env.oldCreds, env.oldTokenRef, env.oldCredsRef,
		env.oldToken, env.oldCreds, newToken, newCreds)
	if n := env.sec.live(); n != 3 {
		t.Errorf("live handles = %d, want the 2 old ones and the 1 whose compensation failed", n)
	}
}

// TestPushReplaceWithoutSecretsDeletesOldHandles is the control: a
// replace by a config without secrets deletes both old handles after the
// commit and leaves the new row without refs.
func TestPushReplaceWithoutSecretsDeletesOldHandles(t *testing.T) {
	env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
	if _, err := env.s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{ID: "pc1", URL: "http://127.0.0.1:9999/other"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if tok, creds := env.refs(t); tok != "" || creds != "" {
		t.Errorf("the row still carries refs")
	}
	if n := env.sec.live(); n != 0 {
		t.Errorf("live handles = %d, want 0", n)
	}
	got, err := env.s.PushConfigs().Get(context.Background(), "t1", "pc1")
	if err != nil || got.Token != "" || got.Auth != nil {
		t.Errorf("Get after the replace: error = %v, config missing = %v, carries a token = %v, carries credentials = %v, want the config without secrets",
			err, got == nil, got != nil && got.Token != "", got != nil && got.Auth != nil)
	}
}

// TestPushDeletePartialFailureThenRetry proves the order of Delete and
// DeleteAll converges: a provider failure after the token handle was
// deleted returns an error without a secret value and keeps the row (the
// transaction rolled back); the retry removes the row and every handle.
func TestPushDeletePartialFailureThenRetry(t *testing.T) {
	for _, op := range []struct {
		name string
		run  func(*pushEnv) error
	}{
		{"Delete", func(e *pushEnv) error { return e.s.PushConfigs().Delete(context.Background(), "t1", "pc1") }},
		{"DeleteAll", func(e *pushEnv) error { return e.s.PushConfigs().DeleteAll(context.Background(), "t1") }},
	} {
		t.Run(op.name, func(t *testing.T) {
			env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
			env.sec.failDeleteOf(env.oldCredsRef)
			err := op.run(env)
			if !errors.Is(err, errInjectedDelete) {
				t.Fatalf("%s error is the injected delete failure: %v, want true", op.name, errors.Is(err, errInjectedDelete))
			}
			requireNoCanary(t, op.name, err, env.oldToken, env.oldCreds)
			if n := env.rowCount(t); n != 1 {
				t.Errorf("rows after the partial failure = %d, want 1 (rolled back)", n)
			}
			if env.sec.isLive(env.oldTokenRef) || !env.sec.isLive(env.oldCredsRef) {
				t.Errorf("the token handle is deleted and the credentials handle is live before the retry: false")
			}
			env.sec.disarm()
			if err := op.run(env); err != nil {
				t.Fatalf("%s retry: %v", op.name, err)
			}
			if n := env.rowCount(t); n != 0 {
				t.Errorf("rows after the retry = %d, want 0", n)
			}
			if n := env.sec.live(); n != 0 {
				t.Errorf("live handles after the retry = %d, want 0", n)
			}
			if err := assertSecretsAbsent(t, env.path, []string{env.oldToken, env.oldCreds}); err != nil {
				t.Errorf("a secret value reached the database file")
			}
		})
	}
}
