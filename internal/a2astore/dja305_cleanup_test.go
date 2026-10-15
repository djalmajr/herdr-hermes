package a2astore

// The DJA-305 tests: the durable cleanup ledger of secret handles that
// stop being referenced. Every path that removes or replaces an
// a2a_push_configs row schedules its handles in the same transaction;
// after the commit the drain deletes them from the provider, a failed
// delete increments attempts and is retried by the next drain, and a
// cleanup failure never loses the committed outcome.

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

var (
	// errDja305Put is the injected failure of the DJA-305 test
	// provider's Put.
	errDja305Put = errors.New("dja305: injected secret put failure")
	// errDja305Delete is the injected failure of the DJA-305 test
	// provider's Delete.
	errDja305Delete = errors.New("dja305: injected secret delete failure")
)

// dja305Canary is the DJA-305 test secret value: a distinctive canary
// that must never reach any a2a_% table.
const dja305Canary = "dja305-canary-secret-VALUE"

// dja305Secrets is the DJA-305 test SecretProvider that fails Delete on
// command: it fails the n-th Put since armPutFailure and the Delete of
// the handles marked by failDeleteOf (or every Delete under
// failAllDeletes) without side effects, and records the successful
// deletes in order.
type dja305Secrets struct {
	mu         sync.Mutex
	seq        int
	putCalls   int
	failPutAt  int
	values     map[string]string
	handles    []string
	failDelete map[string]bool
	failAll    bool
	deleted    []string
}

func newDja305Secrets() *dja305Secrets {
	return &dja305Secrets{values: map[string]string{}, failDelete: map[string]bool{}}
}

// Put implements a2acore.SecretProvider.
func (d *dja305Secrets) Put(_ context.Context, value string) (string, error) {
	d.mu.Lock()
	d.putCalls++
	fail := d.failPutAt != 0 && d.putCalls == d.failPutAt
	d.seq++
	h := "dja305-h" + strconv.Itoa(d.seq)
	d.mu.Unlock()
	if fail {
		return "", errDja305Put
	}
	d.mu.Lock()
	d.values[h] = value
	d.handles = append(d.handles, h)
	d.mu.Unlock()
	return h, nil
}

// Get implements a2acore.SecretProvider.
func (d *dja305Secrets) Get(_ context.Context, handle string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.values[handle]
	if !ok {
		return "", a2acore.ErrSecretUnavailable
	}
	return v, nil
}

// Delete implements a2acore.SecretProvider.
func (d *dja305Secrets) Delete(_ context.Context, handle string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failAll || d.failDelete[handle] {
		return errDja305Delete
	}
	d.deleted = append(d.deleted, handle)
	delete(d.values, handle)
	return nil
}

// armPutFailure makes the n-th Put from now on fail (the counter resets).
func (d *dja305Secrets) armPutFailure(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.putCalls, d.failPutAt = 0, n
}

// failDeleteOf makes the Delete of handle fail.
func (d *dja305Secrets) failDeleteOf(handle string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failDelete[handle] = true
}

// allowDeleteOf clears the injected failure of handle.
func (d *dja305Secrets) allowDeleteOf(handle string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.failDelete, handle)
}

// failAllDeletes makes every Delete fail.
func (d *dja305Secrets) failAllDeletes() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failAll = true
}

// allowAllDeletes clears failAllDeletes.
func (d *dja305Secrets) allowAllDeletes() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failAll = false
}

// insert is a test setup of a handle the provider already holds.
func (d *dja305Secrets) insert(handle, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.values[handle] = value
	d.handles = append(d.handles, handle)
}

// putHandle is the i-th handle ever recorded (1 is the first).
func (d *dja305Secrets) putHandle(i int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i < 1 || i > len(d.handles) {
		panic("dja305: putHandle out of range")
	}
	return d.handles[i-1]
}

// lastPutHandle is the most recently recorded handle.
func (d *dja305Secrets) lastPutHandle() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.handles) == 0 {
		panic("dja305: no handle recorded yet")
	}
	return d.handles[len(d.handles)-1]
}

// isLive reports whether the provider still resolves handle.
func (d *dja305Secrets) isLive(handle string) bool {
	_, err := d.Get(context.Background(), handle)
	return err == nil
}

// deletedOf counts the successful deletes of handle.
func (d *dja305Secrets) deletedOf(handle string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, h := range d.deleted {
		if h == handle {
			n++
		}
	}
	return n
}

// deletedOrder is the order of the successful deletes.
func (d *dja305Secrets) deletedOrder() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.deleted...)
}

// dja305LedgerRow is one a2a_secret_cleanup row as the test reads it.
type dja305LedgerRowData struct {
	found       bool
	scheduled   string
	attempts    int
	lastAttempt string // empty when NULL
}

func dja305LedgerRow(t *testing.T, s *Store, handle string) dja305LedgerRowData {
	t.Helper()
	var row dja305LedgerRowData
	var last sql.NullString
	err := s.db.QueryRow(`SELECT scheduled, attempts, last_attempt FROM a2a_secret_cleanup WHERE handle = ?`, handle).
		Scan(&row.scheduled, &row.attempts, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return row
	}
	if err != nil {
		t.Fatalf("read the cleanup ledger row: %v", err)
	}
	row.found = true
	row.lastAttempt = last.String
	return row
}

func dja305LedgerCount(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_secret_cleanup`).Scan(&n); err != nil {
		t.Fatalf("count the cleanup ledger: %v", err)
	}
	return n
}

// dja305Exec runs one setup SQL statement on the store's own connection
// with the given arguments.
func dja305Exec(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.Exec(query, args...); err != nil {
		t.Fatalf("test SQL: %v", err)
	}
}

// requireDja305NoSecretInA2ATables sweeps every TEXT column of every
// a2a_% table after the canary values and fails when one of them
// appears in any row.
func requireDja305NoSecretInA2ATables(t *testing.T, s *Store, what string, canaries ...string) {
	t.Helper()
	tables, err := s.db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'a2a_%' ORDER BY name`)
	if err != nil {
		t.Fatalf("a2a tables: %v", err)
	}
	var names []string
	for tables.Next() {
		var name string
		if err := tables.Scan(&name); err != nil {
			tables.Close()
			t.Fatalf("a2a table name: %v", err)
		}
		names = append(names, name)
	}
	tables.Close()
	if err := tables.Err(); err != nil {
		t.Fatalf("a2a tables: %v", err)
	}
	for _, name := range names {
		cols, err := s.db.Query(`PRAGMA table_info(` + name + `)`)
		if err != nil {
			t.Fatalf("columns of %s: %v", name, err)
		}
		var textCols []string
		for cols.Next() {
			var (
				cid, notNull, pk int
				nameCol, cType   string
				dflt             any
			)
			if err := cols.Scan(&cid, &nameCol, &cType, &notNull, &dflt, &pk); err != nil {
				cols.Close()
				t.Fatalf("column of %s: %v", name, err)
			}
			if strings.EqualFold(cType, "TEXT") {
				textCols = append(textCols, nameCol)
			}
		}
		cols.Close()
		if err := cols.Err(); err != nil {
			t.Fatalf("columns of %s: %v", name, err)
		}
		for _, col := range textCols {
			rows, err := s.db.Query(`SELECT "` + col + `" FROM ` + name)
			if err != nil {
				t.Fatalf("sweep %s.%s: %v", name, col, err)
			}
			for rows.Next() {
				var v sql.NullString
				if err := rows.Scan(&v); err != nil {
					rows.Close()
					t.Fatalf("sweep %s.%s: %v", name, col, err)
				}
				for _, c := range canaries {
					if v.Valid && strings.Contains(v.String, c) {
						rows.Close()
						t.Fatalf("%s: %s.%s carries the secret value %q", what, name, col, c)
					}
				}
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				t.Fatalf("sweep %s.%s: %v", name, col, err)
			}
		}
	}
}

// dja305RecoverEnv is one store with a pending push config (token only)
// of a task that never commits and a held delivery of another
// never-committed task, the clock already past the recovery grace, and
// the pending config's handle read back.
func dja305RecoverEnv(t *testing.T) (*Store, *clock, *dja305Secrets, string, string) {
	t.Helper()
	clk := &clock{t: testNow}
	sec := newDja305Secrets()
	s, path := openStore(t, clk, "alice", sec)
	if _, err := s.PushConfigs().Save(context.Background(), "t-305-new",
		&a2a.PushConfig{ID: "pc1", URL: "http://127.0.0.1:9999/push", Token: dja305Canary}); err != nil {
		t.Fatalf("Save the pending config: %v", err)
	}
	if _, err := s.Admit(context.Background(), admission("alice", "m-305", "t-305-held", "c-305", "acme/widgets", "s-305", 1, 1, true,
		[]a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	clk.Add(10 * time.Minute)
	var handle string
	if err := s.db.QueryRow(`SELECT token_ref FROM a2a_push_configs WHERE task_id = 't-305-new'`).Scan(&handle); err != nil {
		t.Fatalf("read the pending handle: %v", err)
	}
	return s, clk, sec, path, handle
}

// TestDJA305RecoverHeldFailureSchedulesAndKeepsCount proves acceptance
// 1: a Delete failure after the commit of RecoverHeld leaves the
// committed outcome (the marked deliveries and the gone pending row),
// the handle in the ledger with attempts 1, and the committed count
// plus an error carrying the cleanup sentinel and the provider cause.
func TestDJA305RecoverHeldFailureSchedulesAndKeepsCount(t *testing.T) {
	s, clk, sec, path, handle := dja305RecoverEnv(t)
	sec.failDeleteOf(handle)
	marked, err := s.RecoverHeld(context.Background(), clk.Now(), 5*time.Minute)
	if marked != 1 {
		t.Fatalf("RecoverHeld returned %d with the error, want the committed count 1", marked)
	}
	if !errors.Is(err, ErrSecretCleanupPending) {
		t.Fatalf("the error does not carry the cleanup sentinel: %v", err)
	}
	if !errors.Is(err, errDja305Delete) {
		t.Fatalf("the error lost the provider cause: %v", err)
	}
	if !strings.Contains(err.Error(), "held deliveries recovered") {
		t.Fatalf("the error does not say the recovery committed: %v", err)
	}
	// the committed outcome: the held delivery is obsolete, the
	// pending config row is gone.
	var state string
	if err := s.db.QueryRow(`SELECT state FROM a2a_deliveries WHERE task_id = 't-305-held'`).Scan(&state); err != nil {
		t.Fatalf("delivery state: %v", err)
	}
	if state != a2acore.DeliveryObsolete {
		t.Fatalf("delivery state = %q, want obsolete (committed)", state)
	}
	var rows int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_push_configs`).Scan(&rows); err != nil {
		t.Fatalf("config rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("push config rows = %d, want 0 (committed)", rows)
	}
	// the handle is in the ledger with attempts 2 (the restricted and
	// the old drain both tried it and failed) and both stamps at now.
	row := dja305LedgerRow(t, s, handle)
	if !row.found {
		t.Fatalf("the handle is not in the cleanup ledger")
	}
	if row.attempts != 2 {
		t.Fatalf("attempts = %d, want 2", row.attempts)
	}
	if row.scheduled != clk.ts() || row.lastAttempt != clk.ts() {
		t.Fatalf("stamps = %q/%q, want %q for both", row.scheduled, row.lastAttempt, clk.ts())
	}
	// the provider still holds the handle (its delete failed).
	if !sec.isLive(handle) {
		t.Fatalf("the failed handle is gone from the provider, want still live")
	}
	requireDja305NoSecretInA2ATables(t, s, "after the failed recovery", dja305Canary)
	if err := assertSecretsAbsent(t, path, []string{dja305Canary}); err != nil {
		t.Fatalf("a secret value reached the database file: %v", err)
	}
}

// TestDJA305OldPendingDrainedAfterFreshFailure proves the fix of the
// first security finding: when the delete of a fresh handle fails, the
// recovery still drains the old pendings of earlier recoveries; the
// healthy old handle is gone from the provider and from the table, and
// the returned error is the fresh one, keeping the sentinel and the
// provider cause.
func TestDJA305OldPendingDrainedAfterFreshFailure(t *testing.T) {
	s, clk, sec, _, fresh := dja305RecoverEnv(t)
	old := "dja305-old-pending"
	sec.insert(old, "dja305-old-value")
	dja305Exec(t, s, `INSERT INTO a2a_secret_cleanup(handle, scheduled) VALUES(?, ?)`, old, testNow.Add(-time.Hour).Format(tsLayout))
	sec.failDeleteOf(fresh)
	marked, err := s.RecoverHeld(context.Background(), clk.Now(), 5*time.Minute)
	if marked != 1 {
		t.Fatalf("RecoverHeld returned %d with the error, want the committed count 1", marked)
	}
	if !errors.Is(err, ErrSecretCleanupPending) {
		t.Fatalf("the error does not carry the cleanup sentinel: %v", err)
	}
	if !errors.Is(err, errDja305Delete) {
		t.Fatalf("the error lost the provider cause: %v", err)
	}
	// the healthy old pending was drained from the provider and the
	// table even though the fresh delete failed.
	if sec.isLive(old) {
		t.Fatalf("the old pending is still live in the provider, want drained")
	}
	if row := dja305LedgerRow(t, s, old); row.found {
		t.Fatalf("the old pending row is still in the table: %+v", row)
	}
	// the fresh handle keeps its row (the old drain tried it again and
	// failed, so attempts is 2) and is still live in the provider.
	if row := dja305LedgerRow(t, s, fresh); !row.found || row.attempts != 2 {
		t.Fatalf("the fresh handle row = %+v, want the row with attempts 2", row)
	}
	if !sec.isLive(fresh) {
		t.Fatalf("the fresh handle is gone from the provider, want still live")
	}
	requireDja305NoSecretInA2ATables(t, s, "after the mixed recovery", dja305Canary)
}

// TestDJA305DrainRetryConverges proves acceptance 2 and the [retry]
// case: a failed recovery (the restricted and the old drain both fail)
// brings attempts to 2, one more failed drain brings it to 3, the cured
// drain deletes the handle and empties the table, and a repeated drain
// does nothing, does not delete twice and does not error.
func TestDJA305DrainRetryConverges(t *testing.T) {
	s, clk, sec, _, handle := dja305RecoverEnv(t)
	sec.failDeleteOf(handle)
	if _, err := s.RecoverHeld(context.Background(), clk.Now(), 5*time.Minute); !errors.Is(err, ErrSecretCleanupPending) {
		t.Fatalf("first recovery = %v, want the cleanup sentinel", err)
	}
	if row := dja305LedgerRow(t, s, handle); !row.found || row.attempts != 2 {
		t.Fatalf("after the first failure: %+v, want the row with attempts 2 (the restricted and the old drain both tried it)", row)
	}
	// a second failure: attempts goes to 3.
	deleted, failed, derr := s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 0 || failed != 1 || !errors.Is(derr, ErrSecretCleanupPending) || !errors.Is(derr, errDja305Delete) {
		t.Fatalf("second drain = %d/%d, %v, want 0/1 with the sentinel and the cause", deleted, failed, derr)
	}
	if row := dja305LedgerRow(t, s, handle); !row.found || row.attempts != 3 {
		t.Fatalf("after the second failure: %+v, want the row with attempts 3", row)
	}
	// cured: the drain deletes the handle and empties the table.
	sec.allowDeleteOf(handle)
	deleted, failed, derr = s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 1 || failed != 0 || derr != nil {
		t.Fatalf("cured drain = %d/%d, %v, want 1/0 and no error", deleted, failed, derr)
	}
	if n := dja305LedgerCount(t, s); n != 0 {
		t.Fatalf("ledger rows = %d, want 0", n)
	}
	if sec.isLive(handle) {
		t.Fatalf("the handle is still live after the cured drain")
	}
	// a repeated drain is a no-op and does not delete twice.
	deleted, failed, derr = s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 0 || failed != 0 || derr != nil {
		t.Fatalf("repeat drain = %d/%d, %v, want a no-op", deleted, failed, derr)
	}
	if n := sec.deletedOf(handle); n != 1 {
		t.Fatalf("provider deletes of the handle = %d, want exactly 1", n)
	}
	requireDja305NoSecretInA2ATables(t, s, "after the convergence", dja305Canary)
}

// TestDJA305ClockRewindSkipsNothing proves the [clock] case: a clock
// that goes back between two drains skips nothing and deletes nothing
// without a successful Delete; the scheduled column only orders the
// rows, it never discards them.
func TestDJA305ClockRewindSkipsNothing(t *testing.T) {
	clk := &clock{t: testNow}
	sec := newDja305Secrets()
	s, _ := openStore(t, clk, "alice", sec)
	// two pendings with different scheduled instants: the later-named
	// handle is scheduled first.
	first, second := "zzz-late-name", "aaa-early-name"
	sec.insert(first, "first value")
	sec.insert(second, "second value")
	t0 := testNow.Add(-2 * time.Hour)
	t1 := testNow.Add(-1 * time.Hour)
	dja305Exec(t, s, `INSERT INTO a2a_secret_cleanup(handle, scheduled, attempts) VALUES(?, ?, 0)`, first, t0.Format(tsLayout))
	dja305Exec(t, s, `INSERT INTO a2a_secret_cleanup(handle, scheduled, attempts) VALUES(?, ?, 0)`, second, t1.Format(tsLayout))
	sec.failDeleteOf(first)
	sec.failDeleteOf(second)
	// both fail: nothing is deleted, both are counted.
	deleted, failed, derr := s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 0 || failed != 2 || !errors.Is(derr, ErrSecretCleanupPending) {
		t.Fatalf("first drain = %d/%d, %v, want 0/2 with the sentinel", deleted, failed, derr)
	}
	// the clock goes back before both scheduled instants: nothing is
	// skipped and nothing is deleted without a successful Delete.
	clk.t = t0.Add(-time.Hour)
	deleted, failed, derr = s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 0 || failed != 2 || !errors.Is(derr, ErrSecretCleanupPending) {
		t.Fatalf("rewound drain = %d/%d, %v, want 0/2 (nothing skipped)", deleted, failed, derr)
	}
	for _, h := range []string{first, second} {
		if row := dja305LedgerRow(t, s, h); !row.found || row.attempts != 2 {
			t.Fatalf("after the rewound drain: %+v, want the row with attempts 2", row)
		}
	}
	// cured: the delete order is scheduled ASC (first, second), never
	// the name order.
	sec.allowDeleteOf(first)
	sec.allowDeleteOf(second)
	clk.t = testNow
	deleted, failed, derr = s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 2 || failed != 0 || derr != nil {
		t.Fatalf("cured drain = %d/%d, %v, want 2/0 and no error", deleted, failed, derr)
	}
	if got, want := sec.deletedOrder(), []string{first, second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete order = %v, want %v (scheduled ASC, handle ASC)", got, want)
	}
	if n := dja305LedgerCount(t, s); n != 0 {
		t.Fatalf("ledger rows = %d, want 0", n)
	}
}

// TestDJA305CrashBetweenCommitAndDrain proves the [crash] case: the
// crash happens between the commit that scheduled the handle and the
// Delete; the handle stays in the table and the recovery of the next
// process cleans it (a Delete of a handle the new process never knew
// is not an error, so the drain converges).
func TestDJA305CrashBetweenCommitAndDrain(t *testing.T) {
	clk := &clock{t: testNow}
	sec := newDja305Secrets()
	path := filepath.Join(t.TempDir(), "registry.db")
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice"), Secrets: sec})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	// the committed recovery state a crash would leave behind: the
	// pending config's handle is in the ledger with attempts 0 (the
	// crash happened between the commit and the drain) and the
	// provider still holds it.
	const handle = "dja305-crash-handle"
	sec.insert(handle, "dja305-crash-value")
	dja305Exec(t, s, `INSERT INTO a2a_secret_cleanup(handle, scheduled, attempts) VALUES(?, ?, 0)`, handle, clk.ts())
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// a new process reopens the store and recovers: the next recovery
	// drains the pending handle.
	s2, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice"), Secrets: sec})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	marked, err := s2.RecoverHeld(context.Background(), clk.Now(), 5*time.Minute)
	if marked != 0 || err != nil {
		t.Fatalf("RecoverHeld after the crash = %d, %v, want 0 and no error", marked, err)
	}
	if n := dja305LedgerCount(t, s2); n != 0 {
		t.Fatalf("ledger rows after the recovery = %d, want 0", n)
	}
	if sec.isLive(handle) {
		t.Fatalf("the crashed handle is still live after the recovery")
	}
}

// TestDJA305SaveCompensationFailureSchedulesOrphans proves acceptance 3
// and decision 4: when the creation fails after the Puts (the
// credentials Put or the transaction), the compensation delete runs as
// today and, when it fails, the orphaned handles go to the ledger in a
// small transaction of their own; the original error is returned, and
// the next drain cleans the orphans.
func TestDJA305SaveCompensationFailureSchedulesOrphans(t *testing.T) {
	t.Run("credentials put fails", func(t *testing.T) {
		clk := &clock{t: testNow}
		sec := newDja305Secrets()
		s, _ := openStore(t, clk, "alice", sec)
		if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := s.PushConfigs().Save(context.Background(), "t1",
			&a2a.PushConfig{ID: "pc1", URL: "http://127.0.0.1:9999/push", Token: "old token value"}); err != nil {
			t.Fatalf("Save the old config: %v", err)
		}
		// the replace fails at the credentials Put and the
		// compensation delete of the fresh token handle fails too.
		sec.armPutFailure(2)
		sec.failAllDeletes()
		_, err := s.PushConfigs().Save(context.Background(), "t1", pushConfigOf("new token value", "new creds value"))
		if !errors.Is(err, errDja305Put) {
			t.Fatalf("the original put error is not returned: %v", err)
		}
		if !errors.Is(err, errDja305Delete) {
			t.Fatalf("the compensation failure is not joined: %v", err)
		}
		// the fresh token handle is orphaned in the ledger (attempts 0)
		// and still live; the still-referenced old handle was never
		// scheduled.
		var oldTok string
		if err := s.db.QueryRow(`SELECT token_ref FROM a2a_push_configs WHERE task_id = 't1' AND config_id = 'pc1'`).Scan(&oldTok); err != nil {
			t.Fatalf("read the old handle: %v", err)
		}
		if row := dja305LedgerRow(t, s, oldTok); row.found {
			t.Fatalf("the still-referenced old handle is in the ledger: %+v", row)
		}
		fresh := sec.lastPutHandle()
		if fresh == oldTok {
			t.Fatalf("the fresh handle equals the old one: %q", fresh)
		}
		if row := dja305LedgerRow(t, s, fresh); !row.found || row.attempts != 0 {
			t.Fatalf("the orphaned handle is not in the ledger: %+v", row)
		}
		if !sec.isLive(fresh) {
			t.Fatalf("the orphaned handle is not live in the provider")
		}
		// the next drain (the provider cured) cleans it.
		sec.allowAllDeletes()
		deleted, failed, derr := s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
		if deleted != 1 || failed != 0 || derr != nil {
			t.Fatalf("drain = %d/%d, %v, want 1/0 and no error", deleted, failed, derr)
		}
		if n := dja305LedgerCount(t, s); n != 0 {
			t.Fatalf("ledger rows = %d, want 0", n)
		}
		requireDja305NoSecretInA2ATables(t, s, "after the compensation", "new token value", "new creds value")
	})
	t.Run("transaction fails", func(t *testing.T) {
		clk := &clock{t: testNow}
		sec := newDja305Secrets()
		s, _ := openStore(t, clk, "alice", sec)
		if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
			t.Fatalf("Create: %v", err)
		}
		// the upsert aborts: both fresh handles are orphaned and both
		// compensation deletes fail.
		execTest(t, s, `CREATE TRIGGER test_dja305_abort BEFORE INSERT ON a2a_push_configs BEGIN SELECT RAISE(ABORT, 'test dja305 abort'); END`)
		sec.failAllDeletes()
		_, err := s.PushConfigs().Save(context.Background(), "t1", pushConfigOf("tok305", "creds305"))
		if err == nil || !strings.Contains(err.Error(), "test dja305 abort") {
			t.Fatalf("the original abort error is not returned: %v", err)
		}
		if !errors.Is(err, errDja305Delete) {
			t.Fatalf("the compensation failure is not joined: %v", err)
		}
		tokRef, credsRef := sec.putHandle(1), sec.putHandle(2)
		if tokRef == credsRef {
			t.Fatalf("the two fresh handles are not distinct: %q", tokRef)
		}
		for _, h := range []string{tokRef, credsRef} {
			if row := dja305LedgerRow(t, s, h); !row.found || row.attempts != 0 {
				t.Fatalf("the orphaned handle is not in the ledger: %+v", row)
			}
			if !sec.isLive(h) {
				t.Fatalf("the orphaned handle is not live in the provider")
			}
		}
		var rows int
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_push_configs`).Scan(&rows); err != nil {
			t.Fatalf("count: %v", err)
		}
		if rows != 0 {
			t.Fatalf("push config rows = %d, want 0 (nothing committed)", rows)
		}
		sec.allowAllDeletes()
		deleted, failed, derr := s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
		if deleted != 2 || failed != 0 || derr != nil {
			t.Fatalf("drain = %d/%d, %v, want 2/0 and no error", deleted, failed, derr)
		}
		if n := dja305LedgerCount(t, s); n != 0 {
			t.Fatalf("ledger rows = %d, want 0", n)
		}
		requireDja305NoSecretInA2ATables(t, s, "after the aborted save", "tok305", "creds305")
	})
}

// TestDJA305RemovalPathsScheduleAndDrain proves decision 2 on every
// path that removes or replaces an a2a_push_configs row with handles:
// the failed post-commit delete leaves exactly the failed handle in
// the ledger (the error carries the cleanup sentinel and the provider
// cause), and the cured drain empties it.
func TestDJA305RemovalPathsScheduleAndDrain(t *testing.T) {
	ctx := context.Background()
	t.Run("Save replace", func(t *testing.T) {
		clk := &clock{t: testNow}
		sec := newDja305Secrets()
		env := newPushEnv(t, sec)
		sec.failDeleteOf(env.oldTokenRef)
		_, err := env.s.PushConfigs().Save(ctx, "t1", pushConfigOf(secretValue(t), secretValue(t)))
		if !errors.Is(err, ErrSecretCleanupPending) || !errors.Is(err, errDja305Delete) {
			t.Fatalf("Save = %v, want the sentinel and the provider cause", err)
		}
		if !strings.Contains(err.Error(), "push config stored") {
			t.Fatalf("the error does not say the config was stored: %v", err)
		}
		if row := dja305LedgerRow(t, env.s, env.oldTokenRef); !row.found || row.attempts != 1 {
			t.Fatalf("the obsolete handle is not in the ledger: %+v", row)
		}
		if row := dja305LedgerRow(t, env.s, env.oldCredsRef); row.found {
			t.Fatalf("the deleted old credentials handle is in the ledger: %+v", row)
		}
		sec.allowDeleteOf(env.oldTokenRef)
		deleted, failed, derr := env.s.DrainSecretCleanup(ctx, clk.Now(), 100)
		if deleted != 1 || failed != 0 || derr != nil {
			t.Fatalf("drain = %d/%d, %v, want 1/0 and no error", deleted, failed, derr)
		}
		if n := dja305LedgerCount(t, env.s); n != 0 {
			t.Fatalf("ledger rows = %d, want 0", n)
		}
	})
	t.Run("Delete", func(t *testing.T) {
		clk := &clock{t: testNow}
		sec := newDja305Secrets()
		env := newPushEnv(t, sec)
		sec.failDeleteOf(env.oldCredsRef)
		err := env.s.PushConfigs().Delete(ctx, "t1", "pc1")
		if !errors.Is(err, ErrSecretCleanupPending) || !errors.Is(err, errDja305Delete) {
			t.Fatalf("Delete = %v, want the sentinel and the provider cause", err)
		}
		if !strings.Contains(err.Error(), "push config deleted") {
			t.Fatalf("the error does not say the config was deleted: %v", err)
		}
		if row := dja305LedgerRow(t, env.s, env.oldCredsRef); !row.found || row.attempts != 1 {
			t.Fatalf("the failed handle is not in the ledger: %+v", row)
		}
		if row := dja305LedgerRow(t, env.s, env.oldTokenRef); row.found {
			t.Fatalf("the deleted token handle is in the ledger: %+v", row)
		}
		sec.allowDeleteOf(env.oldCredsRef)
		deleted, failed, derr := env.s.DrainSecretCleanup(ctx, clk.Now(), 100)
		if deleted != 1 || failed != 0 || derr != nil {
			t.Fatalf("drain = %d/%d, %v, want 1/0 and no error", deleted, failed, derr)
		}
	})
	t.Run("DeleteAll", func(t *testing.T) {
		clk := &clock{t: testNow}
		sec := newDja305Secrets()
		env := newPushEnv(t, sec)
		sec.failDeleteOf(env.oldCredsRef)
		err := env.s.PushConfigs().DeleteAll(ctx, "t1")
		if !errors.Is(err, ErrSecretCleanupPending) || !errors.Is(err, errDja305Delete) {
			t.Fatalf("DeleteAll = %v, want the sentinel and the provider cause", err)
		}
		if !strings.Contains(err.Error(), "push configs deleted") {
			t.Fatalf("the error does not say the configs were deleted: %v", err)
		}
		if row := dja305LedgerRow(t, env.s, env.oldCredsRef); !row.found || row.attempts != 1 {
			t.Fatalf("the failed handle is not in the ledger: %+v", row)
		}
		sec.allowDeleteOf(env.oldCredsRef)
		deleted, failed, derr := env.s.DrainSecretCleanup(ctx, clk.Now(), 100)
		if deleted != 1 || failed != 0 || derr != nil {
			t.Fatalf("drain = %d/%d, %v, want 1/0 and no error", deleted, failed, derr)
		}
	})
	t.Run("DiscardUncommitted", func(t *testing.T) {
		clk := &clock{t: testNow}
		sec := newDja305Secrets()
		s, _ := openStore(t, clk, "alice", sec)
		if _, err := s.PushConfigs().Save(context.Background(), "t-305-pend", pushConfigOf(secretValue(t), secretValue(t))); err != nil {
			t.Fatalf("Save the pending config: %v", err)
		}
		var tokRef, credsRef string
		if err := s.db.QueryRow(`SELECT COALESCE(token_ref, ''), COALESCE(credentials_ref, '') FROM a2a_push_configs WHERE task_id = 't-305-pend'`).
			Scan(&tokRef, &credsRef); err != nil {
			t.Fatalf("read the pending handles: %v", err)
		}
		clk.Add(2 * time.Hour)
		sec.failDeleteOf(credsRef)
		deleted, err := s.PushConfigs().DiscardUncommitted(context.Background(), "t-305-pend")
		if deleted != 1 {
			t.Fatalf("DiscardUncommitted returned %d with the error, want the committed count 1", deleted)
		}
		if !errors.Is(err, ErrSecretCleanupPending) || !errors.Is(err, errDja305Delete) {
			t.Fatalf("DiscardUncommitted = %v, want the sentinel and the provider cause", err)
		}
		if !strings.Contains(err.Error(), "pending push configs discarded") {
			t.Fatalf("the error does not say the configs were discarded: %v", err)
		}
		if row := dja305LedgerRow(t, s, credsRef); !row.found || row.attempts != 1 {
			t.Fatalf("the failed handle is not in the ledger: %+v", row)
		}
		if !sec.isLive(credsRef) || sec.isLive(tokRef) {
			t.Fatalf("the live handles are not exactly the one whose delete failed")
		}
		sec.allowDeleteOf(credsRef)
		d, f, derr := s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
		if d != 1 || f != 0 || derr != nil {
			t.Fatalf("drain = %d/%d, %v, want 1/0 and no error", d, f, derr)
		}
	})
}

// TestDJA305DrainLimitOrderAndIdempotent proves the drain contract: the
// limit bounds one round, the order is scheduled ASC then handle ASC, a
// Delete of a handle the provider no longer knows is not an error (the
// repeat is idempotent), and an empty table is a silent no-op.
func TestDJA305DrainLimitOrderAndIdempotent(t *testing.T) {
	clk := &clock{t: testNow}
	sec := newDja305Secrets()
	s, _ := openStore(t, clk, "alice", sec)
	// three pendings with distinct scheduled instants; the provider lost
	// the middle one (a restart), which is not a delete failure.
	sec.insert("h1", "v1")
	sec.insert("h3", "v3")
	dja305Exec(t, s, `INSERT INTO a2a_secret_cleanup(handle, scheduled, attempts) VALUES('h1', ?, 0)`, testNow.Add(-3*time.Hour).Format(tsLayout))
	dja305Exec(t, s, `INSERT INTO a2a_secret_cleanup(handle, scheduled, attempts) VALUES('h2', ?, 0)`, testNow.Add(-2*time.Hour).Format(tsLayout))
	dja305Exec(t, s, `INSERT INTO a2a_secret_cleanup(handle, scheduled, attempts) VALUES('h3', ?, 0)`, testNow.Add(-1*time.Hour).Format(tsLayout))
	if n, err := s.PendingSecretCleanup(context.Background()); n != 3 || err != nil {
		t.Fatalf("PendingSecretCleanup = %d, %v, want 3", n, err)
	}
	// the limit bounds one round; the unknown handle deletes fine.
	deleted, failed, derr := s.DrainSecretCleanup(context.Background(), clk.Now(), 2)
	if deleted != 2 || failed != 0 || derr != nil {
		t.Fatalf("limited drain = %d/%d, %v, want 2/0 and no error", deleted, failed, derr)
	}
	if n, err := s.PendingSecretCleanup(context.Background()); n != 1 || err != nil {
		t.Fatalf("pending = %d, %v, want 1 (the round was bounded)", n, err)
	}
	deleted, failed, derr = s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 1 || failed != 0 || derr != nil {
		t.Fatalf("second drain = %d/%d, %v, want 1/0 and no error", deleted, failed, derr)
	}
	if got, want := sec.deletedOrder(), []string{"h1", "h2", "h3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete order = %v, want %v", got, want)
	}
	// an empty table is a no-op: nothing deleted, no error.
	deleted, failed, derr = s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 0 || failed != 0 || derr != nil {
		t.Fatalf("empty drain = %d/%d, %v, want a no-op", deleted, failed, derr)
	}
	if n, err := s.PendingSecretCleanup(context.Background()); n != 0 || err != nil {
		t.Fatalf("pending = %d, %v, want 0", n, err)
	}
}

// TestDJA305DrainKeepsReferencedHandle proves the depth defense: a
// ledger handle that still appears in a2a_push_configs (token_ref or
// credentials_ref) is not deleted from the provider, its ledger row is
// removed, and it counts as resolved; an unreferenced handle of the
// same round is still deleted from the provider.
func TestDJA305DrainKeepsReferencedHandle(t *testing.T) {
	clk := &clock{t: testNow}
	sec := newDja305Secrets()
	env := newPushEnv(t, sec)
	// the overlap: the still-referenced old token handle is also in the
	// cleanup ledger, beside an unreferenced handle the provider holds.
	unref := "dja305-unreferenced"
	sec.insert(unref, "dja305-unref-value")
	dja305Exec(t, env.s, `INSERT INTO a2a_secret_cleanup(handle, scheduled) VALUES(?, ?)`, env.oldTokenRef, clk.ts())
	dja305Exec(t, env.s, `INSERT INTO a2a_secret_cleanup(handle, scheduled) VALUES(?, ?)`, unref, clk.ts())
	deleted, failed, derr := env.s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 2 || failed != 0 || derr != nil {
		t.Fatalf("drain = %d/%d, %v, want 2/0 and no error (the referenced handle counts as resolved)", deleted, failed, derr)
	}
	if !sec.isLive(env.oldTokenRef) {
		t.Fatalf("the referenced handle is gone from the provider, want still live")
	}
	if sec.isLive(unref) {
		t.Fatalf("the unreferenced handle is still live in the provider, want deleted")
	}
	for _, h := range []string{env.oldTokenRef, unref} {
		if row := dja305LedgerRow(t, env.s, h); row.found {
			t.Fatalf("the handle %s row is still in the table: %+v", h, row)
		}
	}
	if n := sec.deletedOf(env.oldTokenRef); n != 0 {
		t.Fatalf("provider deletes of the referenced handle = %d, want 0", n)
	}
	// the credentials_ref column is protected the same way.
	dja305Exec(t, env.s, `INSERT INTO a2a_secret_cleanup(handle, scheduled) VALUES(?, ?)`, env.oldCredsRef, clk.ts())
	deleted, failed, derr = env.s.DrainSecretCleanup(context.Background(), clk.Now(), 100)
	if deleted != 1 || failed != 0 || derr != nil {
		t.Fatalf("second drain = %d/%d, %v, want 1/0 and no error", deleted, failed, derr)
	}
	if !sec.isLive(env.oldCredsRef) {
		t.Fatalf("the referenced credentials handle is gone from the provider, want still live")
	}
	if row := dja305LedgerRow(t, env.s, env.oldCredsRef); row.found {
		t.Fatalf("the credentials handle row is still in the table: %+v", row)
	}
	if n := sec.deletedOf(env.oldCredsRef); n != 0 {
		t.Fatalf("provider deletes of the credentials handle = %d, want 0", n)
	}
}

// TestDJA305HygieneNoSecretInAnyA2ATable proves acceptance 4: after a
// replace whose old handles' deletes fail, the secret value appears in
// no TEXT column of any a2a_% table (the ledger holds only the opaque
// handle), in no error text and not even in the database file bytes.
func TestDJA305HygieneNoSecretInAnyA2ATable(t *testing.T) {
	sec := newDja305Secrets()
	env := newPushEnv(t, sec)
	const (
		tokenValue = "dja305-token-VALUE-305"
		credsValue = "dja305-creds-VALUE-305"
	)
	sec.failDeleteOf(env.oldTokenRef)
	sec.failDeleteOf(env.oldCredsRef)
	_, err := env.s.PushConfigs().Save(context.Background(), "t1", pushConfigOf(tokenValue, credsValue))
	if !errors.Is(err, ErrSecretCleanupPending) {
		t.Fatalf("Save = %v, want the cleanup sentinel", err)
	}
	requireNoCanary(t, "Save", err, env.oldToken, env.oldCreds, tokenValue, credsValue)
	requireDja305NoSecretInA2ATables(t, env.s, "after the failed replace", env.oldToken, env.oldCreds, tokenValue, credsValue)
	if cerr := assertSecretsAbsent(t, env.path, []string{env.oldToken, env.oldCreds, tokenValue, credsValue}); cerr != nil {
		t.Fatalf("a secret value reached the database file: %v", cerr)
	}
}
