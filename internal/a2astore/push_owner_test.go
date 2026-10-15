package a2astore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"
)

// The tests of this file prove the owner_ref column of
// a2a_push_configs: the additive migration of a database created before
// the column (and the repeated and the concurrent open over one), the
// Save stamp (the reference the call carries, NULL when it carries
// none), and the Owner read the push gate re-evaluates on every send.

// poURL is the push config URL of the store tests: loopback only.
const poURL = "http://127.0.0.1:8000/hook"

// poOwnerRef reads the owner_ref of one config row ("" for NULL,
// Fatalf when the row is missing or the query fails).
func poOwnerRef(t *testing.T, db *sql.DB, taskID, configID string) string {
	t.Helper()
	var ref sql.NullString
	err := db.QueryRow(`SELECT owner_ref FROM a2a_push_configs WHERE task_id = ? AND config_id = ?`, taskID, configID).Scan(&ref)
	if err != nil {
		t.Fatalf("owner_ref: %v", err)
	}
	return ref.String
}

// poDropOwnerRef removes the owner_ref column (the database of a build
// before the column) on the given handle: only the first drop may run
// on a pool of an open store, because its pooled connection then keeps
// a stale in-memory schema and cannot see a column another connection
// re-added.
func poDropOwnerRef(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`ALTER TABLE a2a_push_configs DROP COLUMN owner_ref`); err != nil {
		t.Fatalf("drop owner_ref: %v", err)
	}
}

// poDropOwnerRefFresh removes the owner_ref column through a fresh
// connection on the file: a fresh connection parses the file schema on
// first use, unlike the pooled connections of an open store, which
// keep a stale in-memory schema across a change made by another
// connection.
func poDropOwnerRefFresh(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open for the drop: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE a2a_push_configs DROP COLUMN owner_ref`); err != nil {
		t.Fatalf("drop owner_ref: %v", err)
	}
}

// poColumnCount returns whether the fresh connection on the file sees
// the owner_ref column (1 when present, 0 when absent).
func poColumnCount(t *testing.T, path string) (int, error) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var n int
	err = db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('a2a_push_configs') WHERE name = 'owner_ref'`).Scan(&n)
	return n, err
}

// poExecFresh runs one statement through a fresh connection on the
// file (the pooled connections of an open store may keep a stale
// in-memory schema).
func poExecFresh(t *testing.T, path, stmt string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open for the statement: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(stmt, args...); err != nil {
		t.Fatalf("statement: %v", err)
	}
}

// poOwnerRefFresh reads the owner_ref of one config row through a fresh
// connection ("" for NULL, Fatalf when the row is missing).
func poOwnerRefFresh(t *testing.T, path, taskID, configID string) string {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open for owner_ref: %v", err)
	}
	defer db.Close()
	var ref sql.NullString
	err = db.QueryRow(`SELECT owner_ref FROM a2a_push_configs WHERE task_id = ? AND config_id = ?`, taskID, configID).Scan(&ref)
	if err != nil {
		t.Fatalf("owner_ref: %v", err)
	}
	return ref.String
}

// TestPushOwnerMigration proves the additive migration: a database
// created before the column (simulated by dropping it) gains the column
// when the store opens, its old rows stay NULL, the a2a_meta schema
// value stays 1, a third open never alters the table again, and two
// concurrent opens of a pre-column database both succeed.
func TestPushOwnerMigration(t *testing.T) {
	clk := &clock{t: testNow}
	s, path := openStore(t, clk, "svc-a", newFakeSecrets())
	workingTask(t, s, "t1", "s1")
	ctx := context.Background()
	if _, err := s.PushConfigs().Save(WithOwnerRef(ctx, "owner-ref-a"), "t1", &a2a.PushConfig{ID: "pc-1", URL: poURL}); err != nil {
		t.Fatalf("Save stamped: %v", err)
	}
	if _, err := s.PushConfigs().Save(ctx, "t1", &a2a.PushConfig{ID: "pc-2", URL: poURL}); err != nil {
		t.Fatalf("Save unstamped: %v", err)
	}
	if got := poOwnerRef(t, s.db, "t1", "pc-1"); got != "owner-ref-a" {
		t.Fatalf("owner_ref = %q, want owner-ref-a", got)
	}
	if got := poOwnerRef(t, s.db, "t1", "pc-2"); got != "" {
		t.Fatalf("owner_ref = %q, want NULL", got)
	}

	// A second open of the same database never alters the table again
	// (no duplicate column name) and keeps the stored values.
	s2, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("svc-a")})
	if err != nil {
		t.Fatalf("second OpenFile: %v", err)
	}
	defer s2.Close()
	if got := poOwnerRef(t, s2.db, "t1", "pc-1"); got != "owner-ref-a" {
		t.Fatalf("owner_ref after the second open = %q, want owner-ref-a", got)
	}

	// The database of a build before the column: drop the column.
	poDropOwnerRef(t, s.db)

	// The next open migrates in place: the column is back, the old rows
	// are NULL and the schema value is unchanged.
	s3, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("svc-a")})
	if err != nil {
		t.Fatalf("OpenFile after the drop: %v", err)
	}
	defer s3.Close()
	if got := poOwnerRef(t, s3.db, "t1", "pc-1"); got != "" {
		t.Fatalf("migrated row = %q, want NULL", got)
	}
	if got := poOwnerRef(t, s3.db, "t1", "pc-2"); got != "" {
		t.Fatalf("migrated row = %q, want NULL", got)
	}
	var schema string
	if err := s3.db.QueryRow(`SELECT value FROM a2a_meta WHERE key = 'schema'`).Scan(&schema); err != nil || schema != "1" {
		t.Fatalf("a2a_meta schema = %q err=%v, want 1", schema, err)
	}

	// A third open on the migrated database never alters it again.
	s4, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("svc-a")})
	if err != nil {
		t.Fatalf("third OpenFile: %v", err)
	}
	s4.Close()

	// Two concurrent opens of a pre-column database: the immediate
	// BEGIN serializes the migrations and neither errors. The second
	// drop runs on a fresh connection, because the pool of s keeps a
	// stale in-memory schema it cannot re-see through.
	poDropOwnerRefFresh(t, path)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("svc-a")})
			if err != nil {
				errs <- err
				return
			}
			errs <- st.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent OpenFile: %v", err)
		}
	}
	// One of the two opens re-added the column and the other did not
	// run the ALTER again: a fresh connection sees the column exactly
	// once.
	n, err := poColumnCount(t, path)
	if err != nil || n != 1 {
		t.Fatalf("post-concurrent column = %d err=%v, want 1", n, err)
	}

	// Downgrade fixture: the write of a build before this slice (the
	// old column list, no owner_ref) lands on the migrated table with
	// the column NULL; the new build reading it back suppresses the
	// config for a non-local owner (TestPushLifetimeLocalAndLegacy) and
	// keeps sending for the local owner. Opening the migrated database
	// with an actual old binary is operational proof.
	poExecFresh(t, path, `INSERT INTO a2a_push_configs(task_id, config_id, url, auth_scheme, token_ref, credentials_ref, created)
		VALUES('t-old', 'pc-old', 'http://127.0.0.1:8000/hook', NULL, NULL, NULL, '2026-01-01T00:00:00-03:00')`)
	if got := poOwnerRefFresh(t, path, "t-old", "pc-old"); got != "" {
		t.Fatalf("owner_ref of the old-list row = %q, want NULL", got)
	}
}

// TestPushOwnerSaveStamp proves the Save stamp: a Save with the stamp
// records the reference, a Save without it records NULL, and the
// upsert of the same config id records the current reference without a
// second row (an unstamped upsert clears it to NULL).
func TestPushOwnerSaveStamp(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore(t, clk, "svc-a", newFakeSecrets())
	workingTask(t, s, "t1", "s1")
	ctx := context.Background()
	if _, err := s.PushConfigs().Save(WithOwnerRef(ctx, "owner-ref-a"), "t1", &a2a.PushConfig{ID: "pc-1", URL: poURL}); err != nil {
		t.Fatalf("Save stamped: %v", err)
	}
	if got := poOwnerRef(t, s.db, "t1", "pc-1"); got != "owner-ref-a" {
		t.Fatalf("owner_ref = %q, want owner-ref-a", got)
	}
	if _, err := s.PushConfigs().Save(ctx, "t1", &a2a.PushConfig{ID: "pc-2", URL: poURL}); err != nil {
		t.Fatalf("Save unstamped: %v", err)
	}
	if got := poOwnerRef(t, s.db, "t1", "pc-2"); got != "" {
		t.Fatalf("owner_ref = %q, want NULL", got)
	}
	// The upsert of the same config id records the current reference
	// and leaves exactly one row.
	if _, err := s.PushConfigs().Save(WithOwnerRef(ctx, "owner-ref-b"), "t1", &a2a.PushConfig{ID: "pc-1", URL: poURL}); err != nil {
		t.Fatalf("Save upsert: %v", err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = 't1' AND config_id = 'pc-1'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows of pc-1 = %d err=%v, want 1", n, err)
	}
	if got := poOwnerRef(t, s.db, "t1", "pc-1"); got != "owner-ref-b" {
		t.Fatalf("owner_ref after the upsert = %q, want owner-ref-b", got)
	}
	// An unstamped upsert clears the reference to NULL.
	if _, err := s.PushConfigs().Save(ctx, "t1", &a2a.PushConfig{ID: "pc-1", URL: poURL}); err != nil {
		t.Fatalf("Save unstamped upsert: %v", err)
	}
	if got := poOwnerRef(t, s.db, "t1", "pc-1"); got != "" {
		t.Fatalf("owner_ref after the unstamped upsert = %q, want NULL", got)
	}
}

// TestPushOwnerLookup proves the Owner read: the owner, the stored
// project and the recorded reference of a committed task, and
// push.ErrPushConfigNotFound for a missing config and for a pending
// one (its task has no a2a_tasks row).
func TestPushOwnerLookup(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore(t, clk, "svc-a", newFakeSecrets())
	workingTask(t, s, "t1", "s1")
	ctx := context.Background()
	if _, err := s.PushConfigs().Save(WithOwnerRef(ctx, "owner-ref-a"), "t1", &a2a.PushConfig{ID: "pc-1", URL: poURL}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	o, err := s.PushConfigs().Owner(ctx, "t1", "pc-1")
	if err != nil {
		t.Fatalf("Owner: %v", err)
	}
	if o.Subject != "svc-a" || o.Project != "acme/widgets" || o.Ref != "owner-ref-a" {
		t.Fatalf("Owner = %+v, want svc-a / acme/widgets / owner-ref-a", o)
	}
	// A missing config is push.ErrPushConfigNotFound.
	if _, err := s.PushConfigs().Owner(ctx, "t1", "missing"); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Fatalf("Owner missing = %v, want ErrPushConfigNotFound", err)
	}
	// A pending config (no task row) is push.ErrPushConfigNotFound.
	if _, err := s.PushConfigs().Save(ctx, "tp", &a2a.PushConfig{ID: "pc-9", URL: poURL}); err != nil {
		t.Fatalf("Save pending: %v", err)
	}
	if _, err := s.PushConfigs().Owner(ctx, "tp", "pc-9"); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Fatalf("Owner pending = %v, want ErrPushConfigNotFound", err)
	}
}
