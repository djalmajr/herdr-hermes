package a2astore

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
)

// PushConfigStore is the store's implementation of the SDK push.ConfigStore:
// the durable rows hold only the URL and the secret handles; the secret
// values live in the SecretProvider and never in a row, a log or an error.
// A config whose handle is unavailable (for example after a restart of an
// in-memory provider) is omitted from List, is push.ErrPushConfigNotFound
// from Get, and is counted by UnavailableCount.
type PushConfigStore struct {
	s *Store
}

// PushConfigs returns the push config store of this store.
func (s *Store) PushConfigs() *PushConfigStore {
	return &PushConfigStore{s: s}
}

// Save stores one push config of a task. The task does not need a row
// yet: the SDK saves the push config of a first message before the
// executor admits it, so a config whose task has no a2a_tasks row is
// "pending". A pending config is invisible to Get, List and
// UnavailableCount until the task commits, and is discarded by
// DiscardUncommitted (the execution cleanup) or by RecoverHeld (the
// startup recovery) when the task never commits. A config without an id
// gets a generated "pc_<16 hex>" one; the URL is required
// (a2a.ErrInvalidParams when missing). The Token and the
// Auth.Credentials are put into the Secret Provider and only their
// handles are stored. The returned config is a copy carrying the
// original secret values. Save does no owner check: the SDK masks every
// existing task before it saves and a new task id is server-generated.
//
// The order protects the committed config: the new handles are put
// first, outside the transaction (when the credentials Put fails the
// token handle just put is deleted); then one transaction reads the
// previous row's handles and upserts the row; when the transaction or
// its commit fails, only the handles this call just put are deleted
// (a failed compensation delete is joined to the returned error), so the
// previous row and its handles stay intact and usable. Only after the
// commit are the previous row's handles deleted from the provider. A
// failure of that last cleanup is an error, not a success (the config
// is stored, the error says so and keeps the cause, and the obsolete
// handle stays in the provider): the provider has no transactional
// delete and the store keeps no durable list of obsolete handles, so
// that window is the declared limit of a provider whose Delete can fail
// (the native in-memory provider never fails it).
func (p *PushConfigStore) Save(ctx context.Context, taskID a2a.TaskID, cfg *a2a.PushConfig) (*a2a.PushConfig, error) {
	if cfg == nil || cfg.URL == "" {
		return nil, a2a.ErrInvalidParams
	}
	id := cfg.ID
	if id == "" {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, fmt.Errorf("a2astore: push config id: %w", err)
		}
		id = "pc_" + hex.EncodeToString(b[:])
	}
	// 1. The new handles, outside the transaction.
	tokenRef, credsRef, scheme := "", "", ""
	if cfg.Token != "" {
		handle, err := p.s.secrets.Put(ctx, cfg.Token)
		if err != nil {
			return nil, err
		}
		tokenRef = handle
	}
	if cfg.Auth != nil && cfg.Auth.Credentials != "" {
		handle, err := p.s.secrets.Put(ctx, cfg.Auth.Credentials)
		if err != nil {
			return nil, joinCleanup(err, p.deleteHandles(ctx, tokenRef))
		}
		credsRef = handle
		scheme = cfg.Auth.Scheme
	}
	// 2. The previous handles and the upsert, in one transaction.
	var oldToken, oldCreds sql.NullString
	err := p.s.tx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRow(`SELECT token_ref, credentials_ref FROM a2a_push_configs WHERE task_id = ? AND config_id = ?`,
			string(taskID), id).Scan(&oldToken, &oldCreds)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		nowTS, _ := stamp(p.s.now())
		_, err = tx.Exec(`INSERT INTO a2a_push_configs(task_id, config_id, url, auth_scheme, token_ref, credentials_ref, created)
			VALUES(?,?,?,?,?,?,?)
			ON CONFLICT(task_id, config_id) DO UPDATE SET url = excluded.url, auth_scheme = excluded.auth_scheme,
			token_ref = excluded.token_ref, credentials_ref = excluded.credentials_ref`,
			string(taskID), id, cfg.URL, nullStr(scheme), nullStr(tokenRef), nullStr(credsRef), nowTS)
		return err
	})
	if err != nil {
		// 3. Nothing committed: only the handles of this call go away.
		return nil, joinCleanup(err, p.deleteHandles(ctx, tokenRef, credsRef))
	}
	// 4. Committed: the previous handles are obsolete.
	var obsolete []string
	for _, old := range []sql.NullString{oldToken, oldCreds} {
		if old.Valid && old.String != tokenRef && old.String != credsRef {
			obsolete = append(obsolete, old.String)
		}
	}
	if derr := p.deleteHandles(ctx, obsolete...); derr != nil {
		return nil, fmt.Errorf("a2astore: push config stored; deleting an obsolete secret handle failed: %w", derr)
	}
	out := &a2a.PushConfig{Tenant: cfg.Tenant, TaskID: taskID, ID: id, URL: cfg.URL, Token: cfg.Token}
	if cfg.Auth != nil {
		out.Auth = &a2a.PushAuthInfo{Scheme: cfg.Auth.Scheme, Credentials: cfg.Auth.Credentials}
	}
	return out, nil
}

// deleteHandles deletes every non-empty handle from the SecretProvider,
// trying all of them even when one fails, and returns the failures
// joined (nil when every delete succeeded). It runs after the caller's
// outcome is decided, so a canceled context does not stop it.
func (p *PushConfigStore) deleteHandles(ctx context.Context, handles ...string) error {
	ctx = context.WithoutCancel(ctx)
	var errs []error
	for _, h := range handles {
		if h == "" {
			continue
		}
		if err := p.s.secrets.Delete(ctx, h); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// joinCleanup is err, joined with the failure of its compensation when
// there is one.
func joinCleanup(err, cleanup error) error {
	if cleanup == nil {
		return err
	}
	return errors.Join(err, cleanup)
}

// Get retrieves one push config of a committed task, resolving its
// handles through the SecretProvider: a missing row, a pending config
// (its task has no a2a_tasks row) or an unavailable handle is
// push.ErrPushConfigNotFound.
func (p *PushConfigStore) Get(ctx context.Context, taskID a2a.TaskID, configID string) (*a2a.PushConfig, error) {
	row, err := p.s.pushRow(ctx, string(taskID), configID)
	if err != nil {
		return nil, err
	}
	return p.s.pushConfig(ctx, string(taskID), row)
}

// List retrieves the push configs of one committed task, in config-id
// order, omitting the ones whose handles are unavailable; the pending
// configs of a task with no a2a_tasks row are not listed.
func (p *PushConfigStore) List(ctx context.Context, taskID a2a.TaskID) ([]*a2a.PushConfig, error) {
	rows, err := p.s.db.Query(`SELECT config_id, url, COALESCE(auth_scheme, ''), COALESCE(token_ref, ''), COALESCE(credentials_ref, '')
		FROM a2a_push_configs WHERE task_id = ? AND task_id IN (SELECT task_id FROM a2a_tasks) ORDER BY config_id ASC`, string(taskID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	configs := []*a2a.PushConfig{}
	for rows.Next() {
		var row pushRow
		if err := rows.Scan(&row.configID, &row.url, &row.authScheme, &row.tokenRef, &row.credsRef); err != nil {
			return nil, err
		}
		cfg, err := p.s.pushConfig(ctx, string(taskID), row)
		if errors.Is(err, push.ErrPushConfigNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return configs, nil
}

// UnavailableCount counts the push configs of one committed task whose
// secret handles no longer resolve (for example after a restart of an
// in-memory provider); pending configs are not counted.
func (p *PushConfigStore) UnavailableCount(ctx context.Context, taskID a2a.TaskID) (int, error) {
	rows, err := p.s.db.Query(`SELECT COALESCE(token_ref, ''), COALESCE(credentials_ref, '')
		FROM a2a_push_configs WHERE task_id = ? AND task_id IN (SELECT task_id FROM a2a_tasks)`, string(taskID))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var tokenRef, credsRef string
		if err := rows.Scan(&tokenRef, &credsRef); err != nil {
			return 0, err
		}
		if unavailable(ctx, p.s.secrets, tokenRef, credsRef) {
			n++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return n, nil
}

// Delete removes one push config and deletes its secret handles.
func (p *PushConfigStore) Delete(ctx context.Context, taskID a2a.TaskID, configID string) error {
	return p.s.tx(ctx, func(tx *sql.Tx) error {
		var tokenRef, credsRef sql.NullString
		err := tx.QueryRow(`SELECT token_ref, credentials_ref FROM a2a_push_configs WHERE task_id = ? AND config_id = ?`,
			string(taskID), configID).Scan(&tokenRef, &credsRef)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM a2a_push_configs WHERE task_id = ? AND config_id = ?`, string(taskID), configID); err != nil {
			return err
		}
		if tokenRef.Valid {
			if err := p.s.secrets.Delete(ctx, tokenRef.String); err != nil {
				return err
			}
		}
		if credsRef.Valid {
			if err := p.s.secrets.Delete(ctx, credsRef.String); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteAll removes every push config of one task and deletes their
// secret handles.
func (p *PushConfigStore) DeleteAll(ctx context.Context, taskID a2a.TaskID) error {
	return p.s.tx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT token_ref, credentials_ref FROM a2a_push_configs WHERE task_id = ?`, string(taskID))
		if err != nil {
			return err
		}
		type handles struct{ token, creds sql.NullString }
		var stored []handles
		for rows.Next() {
			var h handles
			if err := rows.Scan(&h.token, &h.creds); err != nil {
				rows.Close()
				return err
			}
			stored = append(stored, h)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM a2a_push_configs WHERE task_id = ?`, string(taskID)); err != nil {
			return err
		}
		for _, h := range stored {
			if h.token.Valid {
				if err := p.s.secrets.Delete(ctx, h.token.String); err != nil {
					return err
				}
			}
			if h.creds.Valid {
				if err := p.s.secrets.Delete(ctx, h.creds.String); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// DiscardUncommitted deletes the pending push configs of taskID (rows and
// their secret handles) in one transaction and returns how many rows it
// deleted. When the task has an a2a_tasks row it changes nothing and
// returns 0, so a committed task's configs are never touched.
func (p *PushConfigStore) DiscardUncommitted(ctx context.Context, taskID a2a.TaskID) (int, error) {
	var deleted int
	err := p.s.tx(ctx, func(tx *sql.Tx) error {
		var n int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM a2a_tasks WHERE task_id = ?`, string(taskID)).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
		rows, err := pendingPushRows(tx, `WHERE task_id = ?`, string(taskID))
		if err != nil {
			return err
		}
		deleted, err = p.s.deletePushRows(ctx, tx, rows)
		return err
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// pendingPush is the key, the handles and the creation stamp of one push
// config row.
type pendingPush struct {
	taskID, configID string
	token, creds     sql.NullString
	created          string
}

// pendingPushRows loads the push config rows selected by where (a clause
// over a2a_push_configs); the caller restricts it to rows whose task has
// no a2a_tasks row.
func pendingPushRows(tx *sql.Tx, where string, args ...any) ([]pendingPush, error) {
	rows, err := tx.Query(`SELECT task_id, config_id, token_ref, credentials_ref, created FROM a2a_push_configs `+where, args...)
	if err != nil {
		return nil, err
	}
	var out []pendingPush
	for rows.Next() {
		var r pendingPush
		if err := rows.Scan(&r.taskID, &r.configID, &r.token, &r.creds, &r.created); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// deletePushRows deletes the rows, then their token and credentials
// handles from the SecretProvider, and returns how many rows it deleted.
func (s *Store) deletePushRows(ctx context.Context, tx *sql.Tx, rows []pendingPush) (int, error) {
	for _, r := range rows {
		if _, err := tx.Exec(`DELETE FROM a2a_push_configs WHERE task_id = ? AND config_id = ?`, r.taskID, r.configID); err != nil {
			return 0, err
		}
	}
	for _, r := range rows {
		if r.token.Valid {
			if err := s.secrets.Delete(ctx, r.token.String); err != nil {
				return 0, err
			}
		}
		if r.creds.Valid {
			if err := s.secrets.Delete(ctx, r.creds.String); err != nil {
				return 0, err
			}
		}
	}
	return len(rows), nil
}

// pushRow is one a2a_push_configs row with its nullable columns resolved.
type pushRow struct {
	configID   string
	url        string
	authScheme string
	tokenRef   string
	credsRef   string
}

// pushRow loads one push config row: a missing row is
// push.ErrPushConfigNotFound.
func (s *Store) pushRow(ctx context.Context, taskID, configID string) (pushRow, error) {
	var row pushRow
	err := s.db.QueryRow(`SELECT config_id, url, COALESCE(auth_scheme, ''), COALESCE(token_ref, ''), COALESCE(credentials_ref, '')
		FROM a2a_push_configs WHERE task_id = ? AND config_id = ?
		AND task_id IN (SELECT task_id FROM a2a_tasks)`, taskID, configID).
		Scan(&row.configID, &row.url, &row.authScheme, &row.tokenRef, &row.credsRef)
	if errors.Is(err, sql.ErrNoRows) {
		return pushRow{}, push.ErrPushConfigNotFound
	}
	if err != nil {
		return pushRow{}, err
	}
	return row, nil
}

// pushConfig resolves the handles of one row: an unavailable handle
// (the provider lost them, for example after a restart) makes the
// config push.ErrPushConfigNotFound; the returned value carries the
// original secret values.
func (s *Store) pushConfig(ctx context.Context, taskID string, row pushRow) (*a2a.PushConfig, error) {
	out := &a2a.PushConfig{TaskID: a2a.TaskID(taskID), ID: row.configID, URL: row.url}
	if row.tokenRef != "" {
		value, err := s.secrets.Get(ctx, row.tokenRef)
		if err != nil {
			return nil, push.ErrPushConfigNotFound
		}
		out.Token = value
	}
	if row.authScheme != "" || row.credsRef != "" {
		var creds string
		if row.credsRef != "" {
			var err error
			creds, err = s.secrets.Get(ctx, row.credsRef)
			if err != nil {
				return nil, push.ErrPushConfigNotFound
			}
		}
		out.Auth = &a2a.PushAuthInfo{Scheme: row.authScheme, Credentials: creds}
	}
	return out, nil
}

// unavailable reports whether any of the row's handles no longer
// resolves; an empty handle never counts.
func unavailable(ctx context.Context, secrets a2acore.SecretProvider, handles ...string) bool {
	for _, h := range handles {
		if h == "" {
			continue
		}
		if _, err := secrets.Get(ctx, h); err != nil {
			return true
		}
	}
	return false
}
