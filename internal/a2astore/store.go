// Package a2astore is the durable SQLite persistence of the A2A ingress
// (DJA-283): the additive A2A tables beside the canonical registry tables,
// the official SDK task and push-config stores with owner masking and
// optimistic versioning, and the durable queue of admissions, per-
// recipient deliveries and owner decisions.
//
// The database is opened through registry.Open, which enforces the file-
// safety rules and owns the registry schema version; this package never
// touches that version. Every mutation is one immediate transaction on
// the single pooled connection, so nothing committed is ever lost between
// the caller's return and a crash: a close and a reopen from the same
// path see every committed row.
//
// An incoming task is data: the task history is stored and returned as
// JSON, never executed, rendered or logged, and the push-config secrets
// live in the SecretProvider, never in a row, a log or an error.
package a2astore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/registry"
)

// tsLayout is the local time with an explicit offset (outbox.TSLayout
// style): every stored timestamp carries a numeric offset, never Z.
const tsLayout = "2006-01-02T15:04:05-07:00"

// a2aSchema is the only supported value of a2a_meta("schema"): the tables
// are additive and idempotent, a stored value greater than one is a newer
// schema this build must refuse.
const a2aSchema = 1

// secretCleanupBatch is the default (and the startup recovery's) limit
// of one DrainSecretCleanup round.
const secretCleanupBatch = 100

// ErrSecretCleanupPending is the sentinel of a secret handle deletion
// that is still pending in the durable cleanup ledger: the operation
// that removed the reference committed, and the provider deletion is
// retried by the next drain (the startup recovery repeats it). It is
// wrapped in the cleanup error and tested with errors.Is, so a caller
// can separate a cleanup-only failure (which does not stop the serve)
// from a transaction failure (which still refuses it).
var ErrSecretCleanupPending = errors.New("a2astore: secret handle deletion pending")

// a2aDDL is the additive schema, created with CREATE TABLE IF NOT EXISTS
// in one transaction on every open. It never touches the registry tables
// or the registry PRAGMA user_version.
const a2aDDL = `
CREATE TABLE IF NOT EXISTS a2a_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS a2a_tasks(task_id TEXT PRIMARY KEY, context_id TEXT NOT NULL, owner TEXT NOT NULL, project TEXT NOT NULL, job TEXT, state TEXT NOT NULL, version INTEGER NOT NULL, messages INTEGER NOT NULL, task_json TEXT NOT NULL, created TEXT NOT NULL, created_unix INTEGER NOT NULL, updated TEXT NOT NULL, updated_nano INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS a2a_tasks_owner ON a2a_tasks(owner, updated_nano DESC, task_id DESC);
CREATE INDEX IF NOT EXISTS a2a_tasks_context ON a2a_tasks(context_id);
CREATE TABLE IF NOT EXISTS a2a_ingress(ingress_id INTEGER PRIMARY KEY AUTOINCREMENT, subject TEXT NOT NULL, message_id TEXT NOT NULL, task_id TEXT NOT NULL, context_id TEXT NOT NULL, project TEXT NOT NULL, job TEXT, session TEXT NOT NULL, epoch INTEGER NOT NULL, seq INTEGER NOT NULL, new_task INTEGER NOT NULL, text_bytes INTEGER NOT NULL, received TEXT NOT NULL, received_unix INTEGER NOT NULL, UNIQUE(subject, message_id));
CREATE TABLE IF NOT EXISTS a2a_fences(subject TEXT NOT NULL, session TEXT NOT NULL, epoch INTEGER NOT NULL, last_seq INTEGER NOT NULL, updated TEXT NOT NULL, PRIMARY KEY(subject, session));
CREATE TABLE IF NOT EXISTS a2a_deliveries(delivery_id INTEGER PRIMARY KEY AUTOINCREMENT, ingress_id INTEGER NOT NULL, task_id TEXT NOT NULL, role TEXT NOT NULL, ref TEXT, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_unix INTEGER, lease_until_unix INTEGER, first_attempt TEXT, last_attempt TEXT, accepted TEXT, accept_status TEXT, last_exit INTEGER, last_error TEXT, created TEXT NOT NULL, created_unix INTEGER NOT NULL, UNIQUE(ingress_id, role));
CREATE INDEX IF NOT EXISTS a2a_deliveries_due ON a2a_deliveries(state, next_attempt_unix);
CREATE TABLE IF NOT EXISTS a2a_decisions(decision_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, kind TEXT NOT NULL, text TEXT NOT NULL, actor TEXT NOT NULL, state TEXT NOT NULL, refusal TEXT, recorded TEXT NOT NULL, recorded_seq INTEGER NOT NULL, applied TEXT);
CREATE TABLE IF NOT EXISTS a2a_push_configs(task_id TEXT NOT NULL, config_id TEXT NOT NULL, url TEXT NOT NULL, auth_scheme TEXT, token_ref TEXT, credentials_ref TEXT, created TEXT NOT NULL, PRIMARY KEY(task_id, config_id));
CREATE TABLE IF NOT EXISTS a2a_secret_cleanup(handle TEXT PRIMARY KEY, scheduled TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, last_attempt TEXT);
`

// Options configures OpenFile.
type Options struct {
	// Now is the clock; time.Now when nil.
	Now func() time.Time
	// Limits bounds admissions and decisions; DefaultLimits per zero
	// field.
	Limits a2acore.Limits
	// Secrets keeps the push-config secret values outside the durable
	// rows; a2acore.NewMemorySecrets when nil.
	Secrets a2acore.SecretProvider
	// Owner returns the authenticated subject of an SDK call; nil uses
	// the SDK call context: an authenticated user with a non-empty name
	// is that name, anything else is a2a.ErrUnauthenticated.
	Owner func(ctx context.Context) (string, error)
}

// Store is one open A2A store: the additive A2A tables of one canonical
// registry database. All access goes through the single pooled
// connection the registry handle keeps, so a goroutine never runs a
// query on the pool while one of its own transactions is open.
type Store struct {
	reg     *registry.Store
	db      *sql.DB
	now     func() time.Time
	limits  a2acore.Limits
	secrets a2acore.SecretProvider
	owner   func(ctx context.Context) (string, error)
}

// OpenFile opens the canonical registry database at path (registry.Open
// creates the parent directory, the file and the registry schema when
// missing) and creates the additive A2A schema in one immediate
// transaction. A database whose a2a_meta("schema") value is greater than
// one is refused. The registry schema version is never changed.
func OpenFile(path string, opts Options) (*Store, error) {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	reg, err := registry.Open(path, registry.Options{Now: now})
	if err != nil {
		return nil, err
	}
	s := &Store{
		reg:     reg,
		db:      reg.DB(),
		now:     now,
		limits:  resolveLimits(opts.Limits),
		secrets: opts.Secrets,
		owner:   opts.Owner,
	}
	if s.secrets == nil {
		s.secrets = a2acore.NewMemorySecrets()
	}
	if s.owner == nil {
		s.owner = defaultOwner
	}
	if err := s.installSchema(); err != nil {
		reg.Close()
		return nil, err
	}
	return s, nil
}

// installSchema creates the additive tables in one immediate transaction
// and refuses a stored A2A schema newer than this build.
func (s *Store) installSchema() error {
	txn, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()
	var haveMeta int
	if err := txn.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'a2a_meta'`).Scan(&haveMeta); err != nil {
		return err
	}
	if haveMeta > 0 {
		var value string
		err := txn.QueryRow(`SELECT value FROM a2a_meta WHERE key = 'schema'`).Scan(&value)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			stored, perr := strconv.Atoi(value)
			if perr != nil {
				return fmt.Errorf("a2astore: a2a_meta schema %q is not a number", value)
			}
			if stored > a2aSchema {
				return fmt.Errorf("a2astore: a2a schema %d is newer than this build supports (%d)", stored, a2aSchema)
			}
		}
	}
	if _, err := txn.Exec(a2aDDL); err != nil {
		return err
	}
	_, err = txn.Exec(`INSERT OR IGNORE INTO a2a_meta(key, value) VALUES('schema', '1')`)
	if err != nil {
		return err
	}
	return txn.Commit()
}

// Close closes the database.
func (s *Store) Close() error {
	return s.reg.Close()
}

// resolveLimits fills the zero fields of lim with the defaults.
func resolveLimits(lim a2acore.Limits) a2acore.Limits {
	if lim.MaxParts <= 0 {
		lim.MaxParts = a2acore.DefaultLimits.MaxParts
	}
	if lim.MaxTextBytes <= 0 {
		lim.MaxTextBytes = a2acore.DefaultLimits.MaxTextBytes
	}
	if lim.MaxMessagesPerTask <= 0 {
		lim.MaxMessagesPerTask = a2acore.DefaultLimits.MaxMessagesPerTask
	}
	if lim.MaxTasksPerContext <= 0 {
		lim.MaxTasksPerContext = a2acore.DefaultLimits.MaxTasksPerContext
	}
	if lim.MaxOpenTasks <= 0 {
		lim.MaxOpenTasks = a2acore.DefaultLimits.MaxOpenTasks
	}
	if lim.MaxDecisionBytes <= 0 {
		lim.MaxDecisionBytes = a2acore.DefaultLimits.MaxDecisionBytes
	}
	return lim
}

// defaultOwner is the Options.Owner default: the authenticated user of
// the SDK call context, or a2a.ErrUnauthenticated.
func defaultOwner(ctx context.Context) (string, error) {
	callCtx, ok := a2asrv.CallContextFrom(ctx)
	if !ok || callCtx == nil || callCtx.User == nil {
		return "", a2a.ErrUnauthenticated
	}
	if callCtx.User.Authenticated && callCtx.User.Name != "" {
		return callCtx.User.Name, nil
	}
	return "", a2a.ErrUnauthenticated
}

// tx runs fn in one immediate transaction (the DSN's _txlock=immediate),
// committing when fn returns nil and rolling back otherwise. fn must use
// only the given *sql.Tx: a query on the pool while the transaction is
// open would block on the single connection.
func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	err = fn(txn)
	if err != nil {
		txn.Rollback()
		return err
	}
	return txn.Commit()
}

// stamp is the local text form and the Unix seconds of one instant: the
// text column and its *_unix companion.
func stamp(t time.Time) (string, int64) {
	return t.Format(tsLayout), t.Unix()
}

// scheduleHandleCleanup inserts every non-empty handle into the durable
// secret cleanup ledger inside the caller's transaction (INSERT OR
// IGNORE: a handle already pending keeps its first scheduled instant).
// It never stores a secret value or a provider error message: the
// ledger holds only the opaque handle, which was referenced in
// a2a_push_configs until the same transaction removed it.
func scheduleHandleCleanup(tx *sql.Tx, now time.Time, handles ...string) error {
	scheduled, _ := stamp(now)
	for _, h := range handles {
		if h == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO a2a_secret_cleanup(handle, scheduled, attempts, last_attempt)
			VALUES(?, ?, 0, NULL)`, h, scheduled); err != nil {
			return err
		}
	}
	return nil
}

// DrainSecretCleanup drains the durable secret cleanup ledger: it reads
// up to limit rows (a limit at or below zero uses secretCleanupBatch)
// ordered by scheduled then handle and asks the SecretProvider to
// delete each handle with a context that cannot be canceled, like the
// other post-commit cleanups. Before the delete, a handle that still
// appears in a2a_push_configs (token_ref or credentials_ref) came back
// into use: it is not deleted from the provider and its row is removed
// from the ledger, counted as resolved. A successful delete removes the
// row; a failed one increments attempts and stores last_attempt. err is
// the SQL failure when the SQL itself failed, or the provider failures
// wrapped in ErrSecretCleanupPending when any delete failed; nil when
// the round left nothing pending. A Delete of a handle the provider no
// longer knows is not an error per the provider contract, so a repeated
// drain is idempotent.
func (s *Store) DrainSecretCleanup(ctx context.Context, now time.Time, limit int) (deleted, failed int, err error) {
	return s.drainSecretCleanup(ctx, now, limit, nil)
}

// drainRestricted drains only the handles given, in the same order and
// with the same contract as DrainSecretCleanup; it is a no-op when no
// handle is given, so a path that scheduled nothing never drains the
// old pendings of other operations.
func (s *Store) drainRestricted(ctx context.Context, now time.Time, limit int, handles ...string) (deleted, failed int, err error) {
	names := []string{}
	for _, h := range handles {
		if h != "" {
			names = append(names, h)
		}
	}
	if len(names) == 0 {
		return 0, 0, nil
	}
	return s.drainSecretCleanup(ctx, now, limit, names)
}

// drainSecretCleanup is the drain core: when handles is empty it drains
// the whole ledger, otherwise only the given handles.
func (s *Store) drainSecretCleanup(ctx context.Context, now time.Time, limit int, handles []string) (deleted, failed int, err error) {
	if limit <= 0 {
		limit = secretCleanupBatch
	}
	query := `SELECT handle FROM a2a_secret_cleanup ORDER BY scheduled ASC, handle ASC LIMIT ?`
	args := []any{}
	for _, h := range handles {
		if h != "" {
			args = append(args, h)
		}
	}
	if len(args) > 0 {
		in := strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")
		query = `SELECT handle FROM a2a_secret_cleanup WHERE handle IN (` + in + `) ORDER BY scheduled ASC, handle ASC LIMIT ?`
	}
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return 0, 0, err
	}
	var pend []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			rows.Close()
			return 0, 0, err
		}
		pend = append(pend, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	if len(pend) == 0 {
		return 0, 0, nil
	}
	// Defense in depth against imported or corrupted state: a handle
	// that still appears in a2a_push_configs (token_ref or
	// credentials_ref) came back into use, so it is not deleted from the
	// provider and its ledger row is removed, counted as resolved. The
	// two IN blocks each bind the full handle list (the arguments are
	// the list followed by the list again), so every handle of the
	// batch is checked against both columns no matter its position.
	refIn := strings.TrimSuffix(strings.Repeat("?,", len(pend)), ",")
	refArgs := make([]any, 0, 2*len(pend))
	for _, h := range pend {
		refArgs = append(refArgs, h)
	}
	for _, h := range pend {
		refArgs = append(refArgs, h)
	}
	referenced := map[string]bool{}
	refRows, rerr := s.db.Query(`SELECT token_ref, credentials_ref FROM a2a_push_configs WHERE token_ref IN (`+refIn+`) OR credentials_ref IN (`+refIn+`)`, refArgs...)
	if rerr != nil {
		return 0, 0, rerr
	}
	for refRows.Next() {
		var tok, creds sql.NullString
		if err := refRows.Scan(&tok, &creds); err != nil {
			refRows.Close()
			return 0, 0, err
		}
		if tok.Valid {
			referenced[tok.String] = true
		}
		if creds.Valid {
			referenced[creds.String] = true
		}
	}
	refRows.Close()
	if err := refRows.Err(); err != nil {
		return 0, 0, err
	}
	// The provider calls run outside any SQL transaction, with a
	// context that cannot be canceled; the results are applied in one
	// small transaction, so a crash between the two leaves the ledger
	// intact and the next drain converges (a delete of an already-gone
	// handle is not an error).
	delCtx := context.WithoutCancel(ctx)
	var (
		gone    []string
		kept    []string
		delErrs []error
	)
	for _, h := range pend {
		if referenced[h] {
			gone = append(gone, h)
			continue
		}
		if err := s.secrets.Delete(delCtx, h); err == nil {
			gone = append(gone, h)
		} else {
			kept = append(kept, h)
			delErrs = append(delErrs, err)
		}
	}
	aerr := s.tx(delCtx, func(tx *sql.Tx) error {
		nowTS, _ := stamp(now)
		for _, h := range gone {
			if _, err := tx.Exec(`DELETE FROM a2a_secret_cleanup WHERE handle = ?`, h); err != nil {
				return err
			}
		}
		for _, h := range kept {
			if _, err := tx.Exec(`UPDATE a2a_secret_cleanup SET attempts = attempts + 1, last_attempt = ? WHERE handle = ?`,
				nowTS, h); err != nil {
				return err
			}
		}
		return nil
	})
	if aerr != nil {
		return 0, 0, aerr
	}
	if len(kept) > 0 {
		return len(gone), len(kept), fmt.Errorf("%w: %w", ErrSecretCleanupPending, errors.Join(delErrs...))
	}
	return len(gone), 0, nil
}

// PendingSecretCleanup counts the handles whose provider deletion is
// still pending in the durable cleanup ledger: only the count, never a
// handle, a secret or a provider message.
func (s *Store) PendingSecretCleanup(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM a2a_secret_cleanup`).Scan(&n)
	return n, err
}

// nullStr is a nullable column value: "" is stored as NULL.
func nullStr(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// boolInt is a boolean stored as 0 or 1.
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// NeutralState maps an SDK task state to the neutral state kept in the
// database: TASK_STATE_AUTH_REQUIRED and the unspecified state have no
// neutral form and map to "".
func NeutralState(st a2a.TaskState) string {
	switch st {
	case a2a.TaskStateSubmitted:
		return a2acore.StateSubmitted
	case a2a.TaskStateWorking:
		return a2acore.StateWorking
	case a2a.TaskStateInputRequired:
		return a2acore.StateInputRequired
	case a2a.TaskStateCompleted:
		return a2acore.StateCompleted
	case a2a.TaskStateCanceled:
		return a2acore.StateCanceled
	case a2a.TaskStateFailed:
		return a2acore.StateFailed
	case a2a.TaskStateRejected:
		return a2acore.StateRejected
	}
	return ""
}

// SDKState maps a neutral state back to the SDK: an unknown or empty
// state maps to the unspecified state.
func SDKState(state string) a2a.TaskState {
	switch state {
	case a2acore.StateSubmitted:
		return a2a.TaskStateSubmitted
	case a2acore.StateWorking:
		return a2a.TaskStateWorking
	case a2acore.StateInputRequired:
		return a2a.TaskStateInputRequired
	case a2acore.StateCompleted:
		return a2a.TaskStateCompleted
	case a2acore.StateCanceled:
		return a2a.TaskStateCanceled
	case a2acore.StateFailed:
		return a2a.TaskStateFailed
	case a2acore.StateRejected:
		return a2a.TaskStateRejected
	}
	return a2a.TaskStateUnspecified
}

// unmarshalTask is the deep copy of a stored task: the JSON round trip
// returns a fresh value the caller may mutate without touching the store.
func unmarshalTask(b []byte) (*a2a.Task, error) {
	var task a2a.Task
	if err := json.Unmarshal(b, &task); err != nil {
		return nil, fmt.Errorf("a2astore: task: %w", err)
	}
	return &task, nil
}
