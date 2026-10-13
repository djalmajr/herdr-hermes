package a2astore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"
	"modernc.org/sqlite"

	"github.com/djalmajr/nau/internal/a2acore"
)

// The tests of this file prove the order of the push config deletes: the
// rows are deleted inside the transaction and the provider handles only
// after the commit succeeded, so a failed commit, a failed statement or a
// canceled request never leaves a row whose handles are already gone.

// cancelHooks maps a per-test key to the cancel function the test-only SQL
// function test_cancel_ctx calls. It is registered once, before any store
// opens a connection, so every connection of every test has it.
var cancelHooks sync.Map

func init() {
	sqlite.MustRegisterScalarFunction("test_cancel_ctx", 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		if key, ok := args[0].(string); ok {
			if cancel, found := cancelHooks.Load(key); found {
				cancel.(context.CancelFunc)()
			}
		}
		return int64(0), nil
	})
}

// armCancelBeforeCommit returns a context (bounded by a deadline) that an
// AFTER DELETE trigger on a2a_push_configs cancels from inside the
// transaction, before the commit and with no provider involved. The old
// fault wrapper onDelete cancel can no longer prove a pre-commit
// cancellation: the provider is now called only after the commit. The
// trigger is named test_cancel_commit; the key is random per call, so
// parallel or repeated tests cannot cross.
func armCancelBeforeCommit(t *testing.T, s *Store) context.Context {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	key := hex.EncodeToString(b[:])
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cancelHooks.Store(key, context.CancelFunc(cancel))
	t.Cleanup(func() {
		cancelHooks.Delete(key)
		cancel()
	})
	execTest(t, s, `CREATE TRIGGER test_cancel_commit AFTER DELETE ON a2a_push_configs BEGIN SELECT test_cancel_ctx('`+key+`'); END`)
	return ctx
}

// installCommitFailure makes every delete of a push config row fail at the
// COMMIT (a deferred foreign key violated by a trigger).
func installCommitFailure(t *testing.T, s *Store) {
	t.Helper()
	execTest(t, s, `CREATE TABLE test_parent(id INTEGER PRIMARY KEY)`)
	execTest(t, s, `CREATE TABLE test_child(pid INTEGER REFERENCES test_parent(id) DEFERRABLE INITIALLY DEFERRED)`)
	execTest(t, s, `CREATE TRIGGER test_commit_fail AFTER DELETE ON a2a_push_configs BEGIN INSERT INTO test_child(pid) VALUES(999); END`)
}

// requireCanceledClass fails unless err is context.Canceled or
// sql.ErrTxDone. Both are what a request canceled inside a transaction
// returns, and which of the two is not deterministic: database/sql ends
// the transaction in a goroutine when the context is canceled, so the
// Commit that follows reports context.Canceled when it runs first and
// sql.ErrTxDone when the rollback ran first (always the latter under the
// race detector). Only that class is asserted; the exact one is not.
func requireCanceledClass(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, context.Canceled) && !errors.Is(err, sql.ErrTxDone) {
		t.Errorf("%s error is context.Canceled or sql.ErrTxDone = false", what)
	}
}

// requireForeignKeyFailure fails unless err is the deferred foreign key
// failure of the commit.
func requireForeignKeyFailure(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s succeeded, want the failed commit", what)
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "FOREIGN KEY") {
		t.Fatalf("%s failed for another reason than the deferred foreign key (the commit)", what)
	}
}

// hasDelete reports whether the recorded provider calls include a delete.
func hasDelete(order []string) bool {
	for _, op := range order {
		if op == "delete" {
			return true
		}
	}
	return false
}

// installRowGoneHook makes every provider Delete record whether the row
// of taskID is already gone for a second read-only connection, which only
// sees committed data. The slice is read after the operation returned.
func installRowGoneHook(t *testing.T, sec *faultSecrets, path, taskID string) *[]bool {
	t.Helper()
	ro, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	t.Cleanup(func() { ro.Close() })
	seen := &[]bool{}
	sec.mu.Lock()
	defer sec.mu.Unlock()
	sec.onDelete = func(string) {
		var n int
		if err := ro.QueryRow(`SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = ?`, taskID).Scan(&n); err != nil {
			*seen = append(*seen, false)
			return
		}
		*seen = append(*seen, n == 0)
	}
	return seen
}

// requireGone fails unless the config pc1 of t1 is committed-deleted: no
// row, Get not found, List empty, nothing unavailable and no canary in the
// database file.
func (e *pushEnv) requireGone(t *testing.T, what string, canaries ...string) {
	t.Helper()
	if n := e.rowCount(t); n != 0 {
		t.Errorf("%s: rows = %d, want 0", what, n)
	}
	if _, err := e.s.PushConfigs().Get(context.Background(), "t1", "pc1"); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Errorf("%s: Get is not found = false", what)
	}
	if list, err := e.s.PushConfigs().List(context.Background(), "t1"); err != nil || len(list) != 0 {
		t.Errorf("%s: List length = %d, err = %v, want 0", what, len(list), err)
	}
	if n, err := e.s.PushConfigs().UnavailableCount(context.Background(), "t1"); err != nil || n != 0 {
		t.Errorf("%s: UnavailableCount = %d, %v, want 0", what, n, err)
	}
	if err := assertSecretsAbsent(t, e.path, canaries); err != nil {
		t.Errorf("%s: a secret value reached the database file", what)
	}
}

// deleteOps are the two public deletes of the config pc1 of t1 and the
// fixed text of their post-commit failure.
var deleteOps = []struct {
	name string
	text string
	run  func(context.Context, *pushEnv) error
}{
	{"Delete", "push config deleted; deleting a secret handle failed", func(ctx context.Context, e *pushEnv) error {
		return e.s.PushConfigs().Delete(ctx, "t1", "pc1")
	}},
	{"DeleteAll", "push configs deleted; deleting a secret handle failed", func(ctx context.Context, e *pushEnv) error {
		return e.s.PushConfigs().DeleteAll(ctx, "t1")
	}},
}

// requireIntact fails unless the old config is committed, usable with its
// old refs and values; with the fault wrapper both handles are live and
// the provider was never asked to delete.
func (e *pushEnv) requireIntact(t *testing.T, what string) {
	t.Helper()
	e.requireUsable(t, what, e.oldToken, e.oldCreds, e.oldTokenRef, e.oldCredsRef, e.oldToken, e.oldCreds)
	if e.rowCount(t) != 1 {
		t.Errorf("%s: rows = %d, want 1", what, e.rowCount(t))
	}
	if e.sec == nil {
		return
	}
	if n := e.sec.live(); n != 2 || !e.sec.isLive(e.oldTokenRef) || !e.sec.isLive(e.oldCredsRef) {
		t.Errorf("%s: live handles = %d, want both old handles live", what, n)
	}
	if hasDelete(e.sec.callOrder()) {
		t.Errorf("%s: the provider was asked to delete a handle before a commit", what)
	}
}

// TestPushDeleteCommitSQLAbortBeforeProvider proves a delete aborted by
// the database leaves the config usable and never reaches the provider.
func TestPushDeleteCommitSQLAbortBeforeProvider(t *testing.T) {
	for _, op := range deleteOps {
		t.Run(op.name, func(t *testing.T) {
			env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
			execTest(t, env.s, `CREATE TRIGGER test_abort_delete BEFORE DELETE ON a2a_push_configs BEGIN SELECT RAISE(ABORT, 'test abort'); END`)
			env.sec.resetOps()
			err := op.run(context.Background(), env)
			if err == nil {
				t.Fatalf("%s succeeded, want the aborted delete", op.name)
			}
			requireNoCanary(t, op.name, err, env.oldToken, env.oldCreds)
			env.requireIntact(t, "after the aborted delete")
		})
	}
}

// TestPushDeleteCommitFailureKeepsConfig proves a delete whose COMMIT
// fails (a deferred foreign key violated by a test trigger) rolls the row
// back and leaves its handles live: the config stays usable.
func TestPushDeleteCommitFailureKeepsConfig(t *testing.T) {
	for _, pc := range providerCases() {
		for _, op := range deleteOps {
			t.Run(pc.name+"/"+op.name, func(t *testing.T) {
				env := newPushEnv(t, pc.make())
				installCommitFailure(t, env.s)
				if env.sec != nil {
					env.sec.resetOps()
				}
				err := op.run(context.Background(), env)
				requireForeignKeyFailure(t, op.name, err)
				requireNoCanary(t, op.name, err, env.oldToken, env.oldCreds)
				env.requireIntact(t, "after the failed commit")
			})
		}
	}
}

// TestPushDeleteCancelBeforeCommitKeepsConfig proves a request canceled
// inside the transaction, before the commit and with no provider call,
// leaves the config usable with both handles live. The error is
// context.Canceled or sql.ErrTxDone (see requireCanceledClass).
func TestPushDeleteCancelBeforeCommitKeepsConfig(t *testing.T) {
	for _, pc := range providerCases() {
		for _, op := range deleteOps {
			t.Run(pc.name+"/"+op.name, func(t *testing.T) {
				env := newPushEnv(t, pc.make())
				ctx := armCancelBeforeCommit(t, env.s)
				if env.sec != nil {
					env.sec.resetOps()
				}
				err := op.run(ctx, env)
				if err == nil {
					t.Fatalf("%s succeeded, want the canceled operation", op.name)
				}
				requireCanceledClass(t, op.name, err)
				requireNoCanary(t, op.name, err, env.oldToken, env.oldCreds)
				env.requireIntact(t, "after the canceled delete")
			})
		}
	}
}

// TestPushDeleteCancelAfterCommitStillCleansHandles proves a request
// canceled after the commit (by the first provider delete) does not stop
// the cleanup: the operation succeeds and no handle is left.
func TestPushDeleteCancelAfterCommitStillCleansHandles(t *testing.T) {
	for _, op := range deleteOps {
		t.Run(op.name, func(t *testing.T) {
			env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			env.sec.mu.Lock()
			env.sec.onDelete = func(string) { cancel() }
			env.sec.mu.Unlock()
			if err := op.run(ctx, env); err != nil {
				t.Fatalf("%s: %v, want success after the commit", op.name, err)
			}
			if ctx.Err() == nil {
				t.Errorf("the context was not canceled by the first provider delete")
			}
			env.requireGone(t, "after the delete", env.oldToken, env.oldCreds)
			if n := env.sec.live(); n != 0 {
				t.Errorf("live handles = %d, want 0 (the cleanup ignored the canceled context)", n)
			}
		})
	}
}

// TestPushDeleteCommitOrder pins the order of a successful delete: the
// provider is asked to delete each handle only when the row is already
// gone for a second read-only connection (committed data only).
func TestPushDeleteCommitOrder(t *testing.T) {
	for _, op := range deleteOps {
		t.Run(op.name, func(t *testing.T) {
			env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
			seen := installRowGoneHook(t, env.sec, env.path, "t1")
			env.sec.resetOps()
			if err := op.run(context.Background(), env); err != nil {
				t.Fatalf("%s: %v", op.name, err)
			}
			if got, want := env.sec.callOrder(), []string{"delete", "delete"}; !reflect.DeepEqual(got, want) {
				t.Errorf("provider call order = %v, want %v", got, want)
			}
			if !reflect.DeepEqual(*seen, []bool{true, true}) {
				t.Errorf("the row was gone at each provider delete = %v, want [true true]", *seen)
			}
			env.requireGone(t, "after the delete", env.oldToken, env.oldCreds)
			if n := env.sec.live(); n != 0 {
				t.Errorf("live handles = %d, want 0", n)
			}
		})
	}
}

// TestPushDeleteCleanupFailureAfterCommit is the declared boundary of the
// delete, the same class as the one of Save: a provider Delete that fails
// after the commit leaves exactly that handle in the provider. The error
// says the config was deleted and keeps the cause, the row is gone, and a
// second call is a no-op: it does not clean the handle (the row and its
// refs are gone and the store keeps no durable ledger of handles), so
// there is no retry convergence.
func TestPushDeleteCleanupFailureAfterCommit(t *testing.T) {
	for _, op := range deleteOps {
		t.Run(op.name, func(t *testing.T) {
			env := newPushEnv(t, newFaultSecrets(a2acore.NewMemorySecrets()))
			env.sec.failDeleteOf(env.oldCredsRef)
			err := op.run(context.Background(), env)
			if !errors.Is(err, errInjectedDelete) {
				t.Fatalf("%s error is the injected delete failure = %v, want true", op.name, errors.Is(err, errInjectedDelete))
			}
			if !strings.Contains(err.Error(), op.text) {
				t.Errorf("the error does not carry the fixed committed-outcome text")
			}
			requireNoCanary(t, op.name, err, env.oldToken, env.oldCreds)
			env.requireGone(t, "after the failed cleanup", env.oldToken, env.oldCreds)
			if env.sec.isLive(env.oldTokenRef) || !env.sec.isLive(env.oldCredsRef) {
				t.Errorf("the live handle is not exactly the one whose delete failed")
			}
			if n := env.sec.live(); n != 1 {
				t.Errorf("live handles = %d, want exactly 1 (the bounded residual)", n)
			}
			env.sec.disarm()
			if err := op.run(context.Background(), env); err != nil {
				t.Fatalf("%s again: %v, want a no-op", op.name, err)
			}
			env.requireGone(t, "after the second call", env.oldToken, env.oldCreds)
			// No retry convergence without a durable ledger: the row is gone, so
			// the second call has no ref to delete and the handle stays.
			if n := env.sec.live(); n != 1 {
				t.Errorf("live handles after the second call = %d, want still 1", n)
			}
		})
	}
}

// pendEnv is one store with a pending push config pc1 (its task t-new has
// no a2a_tasks row) behind the fault wrapper, a held delivery of another
// task when held is true, and a clock already past the grace.
type pendEnv struct {
	s                  *Store
	clk                *clock
	path               string
	sec                *faultSecrets
	token, creds       string
	tokenRef, credsRef string
}

const pendGrace = 5 * time.Minute

func newPendEnv(t *testing.T, held bool) *pendEnv {
	t.Helper()
	clk := &clock{t: testNow}
	sec := newFaultSecrets(a2acore.NewMemorySecrets())
	s, path := openStore(t, clk, "alice", sec)
	e := &pendEnv{s: s, clk: clk, path: path, sec: sec, token: secretValue(t), creds: secretValue(t)}
	if _, err := s.PushConfigs().Save(context.Background(), "t-new", pushConfigOf(e.token, e.creds)); err != nil {
		t.Fatalf("Save the pending config: %v", err)
	}
	var tok, creds sql.NullString
	if err := s.db.QueryRow(`SELECT token_ref, credentials_ref FROM a2a_push_configs WHERE task_id = 't-new'`).Scan(&tok, &creds); err != nil {
		t.Fatalf("read the pending handles: %v", err)
	}
	e.tokenRef, e.credsRef = tok.String, creds.String
	if held {
		if _, err := s.Admit(context.Background(), admission("alice", "m-held", "t-held", "c-held", "acme/widgets", "s1", 1, 1, true,
			[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
			t.Fatalf("Admit: %v", err)
		}
	}
	clk.Add(2 * pendGrace)
	sec.resetOps()
	return e
}

// requireInvisible fails unless the pending config is not visible.
func (e *pendEnv) requireInvisible(t *testing.T, what string) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.s.PushConfigs().Get(ctx, "t-new", "pc1"); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Errorf("%s: Get pending is not found = false", what)
	}
	if list, err := e.s.PushConfigs().List(ctx, "t-new"); err != nil || len(list) != 0 {
		t.Errorf("%s: List length = %d, err = %v, want 0", what, len(list), err)
	}
	if n, err := e.s.PushConfigs().UnavailableCount(ctx, "t-new"); err != nil || n != 0 {
		t.Errorf("%s: UnavailableCount = %d, %v, want 0", what, n, err)
	}
}

// requireCommittedUsable creates the task and fails unless the pending
// config became the active one with its exact values and a live handle.
func (e *pendEnv) requireCommittedUsable(t *testing.T, what string) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.s.Create(ctx, mkTask("t-new", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("%s: Create: %v", what, err)
	}
	got, err := e.s.PushConfigs().Get(ctx, "t-new", "pc1")
	if err != nil {
		t.Fatalf("%s: Get = %v, want the config", what, err)
	}
	if got.Token != e.token || got.Auth == nil || got.Auth.Credentials != e.creds || got.URL != pushConfigOf("", "").URL {
		t.Errorf("%s: Get returned other values than saved", what)
	}
	if list, err := e.s.PushConfigs().List(ctx, "t-new"); err != nil || len(list) != 1 || list[0].Token != e.token {
		t.Errorf("%s: List length = %d, err = %v, want the one config", what, len(list), err)
	}
	if n, err := e.s.PushConfigs().UnavailableCount(ctx, "t-new"); err != nil || n != 0 {
		t.Errorf("%s: UnavailableCount = %d, %v, want 0", what, n, err)
	}
}

// requireKept fails unless the pending row and both handles survived and
// the provider was never asked to delete.
func (e *pendEnv) requireKept(t *testing.T, what string) {
	t.Helper()
	if n := ppRows(t, e.s, "t-new"); n != 1 {
		t.Errorf("%s: pending rows = %d, want 1", what, n)
	}
	if n := e.sec.live(); n != 2 || !e.sec.isLive(e.tokenRef) || !e.sec.isLive(e.credsRef) {
		t.Errorf("%s: live handles = %d, want both pending handles live", what, n)
	}
	if hasDelete(e.sec.callOrder()) {
		t.Errorf("%s: the provider was asked to delete a handle before a commit", what)
	}
	e.requireInvisible(t, what)
}

// pendingOps are the two sweeps of a pending config: the count they
// return is the committed one (rows discarded, deliveries marked) and the
// text is the one of their post-commit failure.
var pendingOps = []struct {
	name string
	held bool
	text string
	run  func(context.Context, *pendEnv) (int, error)
}{
	{"DiscardUncommitted", false, "pending push configs discarded; deleting a secret handle failed", func(ctx context.Context, e *pendEnv) (int, error) {
		return e.s.PushConfigs().DiscardUncommitted(ctx, "t-new")
	}},
	{"RecoverHeld", true, "held deliveries recovered; deleting a secret handle of a discarded pending push config failed", func(ctx context.Context, e *pendEnv) (int, error) {
		return e.s.RecoverHeld(ctx, e.clk.Now(), pendGrace)
	}},
}

// TestPushPendingSweepCommitFailureThenCreate proves a sweep whose COMMIT
// fails keeps the pending row and its live handles, the config stays
// invisible, and the task that commits later gets a usable config (not an
// unreadable one with dead handles).
func TestPushPendingSweepCommitFailureThenCreate(t *testing.T) {
	for _, op := range pendingOps {
		t.Run(op.name, func(t *testing.T) {
			e := newPendEnv(t, op.held)
			installCommitFailure(t, e.s)
			n, err := op.run(context.Background(), e)
			requireForeignKeyFailure(t, op.name, err)
			requireNoCanary(t, op.name, err, e.token, e.creds)
			if n != 0 {
				t.Errorf("%s returned %d with the error, want 0 (nothing committed)", op.name, n)
			}
			e.requireKept(t, "after the failed commit")
			execTest(t, e.s, `DROP TRIGGER test_commit_fail`)
			e.requireCommittedUsable(t, "after Create")
			if err := assertSecretsAbsent(t, e.path, []string{e.token, e.creds}); err != nil {
				t.Errorf("a secret value reached the database file")
			}
		})
	}
}

// TestPushPendingSweepCancelBeforeCommitThenCreate is the same sequence
// with a request canceled inside the transaction, before the commit.
func TestPushPendingSweepCancelBeforeCommitThenCreate(t *testing.T) {
	for _, op := range pendingOps {
		t.Run(op.name, func(t *testing.T) {
			e := newPendEnv(t, op.held)
			ctx := armCancelBeforeCommit(t, e.s)
			n, err := op.run(ctx, e)
			if err == nil {
				t.Fatalf("%s succeeded, want the canceled operation", op.name)
			}
			requireCanceledClass(t, op.name, err)
			requireNoCanary(t, op.name, err, e.token, e.creds)
			if n != 0 {
				t.Errorf("%s returned %d with the error, want 0", op.name, n)
			}
			e.requireKept(t, "after the canceled sweep")
			execTest(t, e.s, `DROP TRIGGER test_cancel_commit`)
			e.requireCommittedUsable(t, "after Create")
		})
	}
}

// TestPushPendingSweepSuccessOrder proves a successful sweep removes the
// row and both handles, and asks the provider only once the row is gone
// for a second read-only connection.
func TestPushPendingSweepSuccessOrder(t *testing.T) {
	for _, op := range pendingOps {
		t.Run(op.name, func(t *testing.T) {
			e := newPendEnv(t, op.held)
			seen := installRowGoneHook(t, e.sec, e.path, "t-new")
			n, err := op.run(context.Background(), e)
			if err != nil || n != 1 {
				t.Fatalf("%s = %d, %v, want 1 and no error", op.name, n, err)
			}
			if got, want := e.sec.callOrder(), []string{"delete", "delete"}; !reflect.DeepEqual(got, want) {
				t.Errorf("provider call order = %v, want %v", got, want)
			}
			if !reflect.DeepEqual(*seen, []bool{true, true}) {
				t.Errorf("the row was gone at each provider delete = %v, want [true true]", *seen)
			}
			if ppRows(t, e.s, "t-new") != 0 || e.sec.live() != 0 {
				t.Errorf("rows = %d, live handles = %d, want 0/0", ppRows(t, e.s, "t-new"), e.sec.live())
			}
			if err := assertSecretsAbsent(t, e.path, []string{e.token, e.creds}); err != nil {
				t.Errorf("a secret value reached the database file")
			}
		})
	}
}

// TestPushPendingSweepCleanupFailureAfterCommit proves a provider failure
// after the commit returns the committed count together with an error that
// keeps the cause and says what was committed, and leaves exactly the one
// handle whose delete failed.
func TestPushPendingSweepCleanupFailureAfterCommit(t *testing.T) {
	for _, op := range pendingOps {
		t.Run(op.name, func(t *testing.T) {
			e := newPendEnv(t, op.held)
			e.sec.failDeleteOf(e.credsRef)
			n, err := op.run(context.Background(), e)
			if !errors.Is(err, errInjectedDelete) {
				t.Fatalf("%s error is the injected delete failure = %v, want true", op.name, errors.Is(err, errInjectedDelete))
			}
			if !strings.Contains(err.Error(), op.text) {
				t.Errorf("the error does not carry the fixed committed-outcome text")
			}
			requireNoCanary(t, op.name, err, e.token, e.creds)
			if n != 1 {
				t.Errorf("%s returned %d with the error, want the committed count 1", op.name, n)
			}
			if rows := ppRows(t, e.s, "t-new"); rows != 0 {
				t.Errorf("rows = %d, want 0 (committed)", rows)
			}
			if e.sec.isLive(e.tokenRef) || !e.sec.isLive(e.credsRef) || e.sec.live() != 1 {
				t.Errorf("live handles = %d, want exactly the one whose delete failed", e.sec.live())
			}
			e.requireInvisible(t, "after the failed cleanup")
			if err := assertSecretsAbsent(t, e.path, []string{e.token, e.creds}); err != nil {
				t.Errorf("a secret value reached the database file")
			}
		})
	}
}
