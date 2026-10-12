package a2astore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
)

// fakeSecrets is a SecretProvider with deterministic handles and a log
// of the handles it is asked to delete.
type fakeSecrets struct {
	mu      sync.Mutex
	values  map[string]string
	next    int
	deleted []string
}

func newFakeSecrets() *fakeSecrets {
	return &fakeSecrets{values: map[string]string{}}
}

// Put implements a2acore.SecretProvider.
func (f *fakeSecrets) Put(_ context.Context, value string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	h := fmt.Sprintf("sec_%02d", f.next)
	f.values[h] = value
	return h, nil
}

// Get implements a2acore.SecretProvider.
func (f *fakeSecrets) Get(_ context.Context, handle string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.values[handle]
	if !ok {
		return "", a2acore.ErrSecretUnavailable
	}
	return v, nil
}

// Delete implements a2acore.SecretProvider and records the handle.
func (f *fakeSecrets) Delete(_ context.Context, handle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, handle)
	return nil
}

// TestPushSecretsNeverStored proves the canary rule: after a Save, the
// token and credential values are absent from the database file and its
// WAL (even after a checkpoint moves the WAL into the main file); only
// the handles are stored, and Get returns the original values.
func TestPushSecretsNeverStored(t *testing.T) {
	clk := &clock{t: testNow}
	sec := newFakeSecrets()
	s, path := openStore(t, clk, "alice", sec)
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	token, creds := "canary-token-VALUE-123", "canary-cred-VALUE-456"
	cfg, err := s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{
		URL:   "http://127.0.0.1:9999/push",
		Token: token,
		Auth:  &a2a.PushAuthInfo{Scheme: "Basic", Credentials: creds},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if cfg.Token != token || cfg.Auth == nil || cfg.Auth.Credentials != creds {
		t.Fatalf("Save returned %+v, want the original secret values", cfg)
	}
	// The database bytes (main file and WAL) never carry the values.
	if err := assertSecretsAbsent(t, path, []string{token, creds}); err != nil {
		t.Fatalf("secrets in the database bytes: %v", err)
	}
	// Get resolves the handles back to the original values.
	got, err := s.PushConfigs().Get(context.Background(), "t1", cfg.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Token != token || got.Auth == nil || got.Auth.Credentials != creds || got.Auth.Scheme != "Basic" || got.URL != "http://127.0.0.1:9999/push" {
		t.Fatalf("Get = %+v", got)
	}
	// A checkpoint moves the WAL into the main file: the values are
	// still absent from the main file.
	if _, err := s.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := assertSecretsAbsent(t, path, []string{token, creds}); err != nil {
		t.Fatalf("secrets in the checkpointed database: %v", err)
	}
	// The handles, in contrast, are stored.
	var tokenRef, credsRef string
	if err := s.db.QueryRow(`SELECT COALESCE(token_ref, ''), COALESCE(credentials_ref, '') FROM a2a_push_configs WHERE task_id = 't1'`).Scan(&tokenRef, &credsRef); err != nil {
		t.Fatalf("handles: %v", err)
	}
	if !strings.HasPrefix(tokenRef, "sec_") || !strings.HasPrefix(credsRef, "sec_") {
		t.Fatalf("stored refs = %q, %q, want secret handles", tokenRef, credsRef)
	}
	// List sees the config.
	listed, err := s.PushConfigs().List(context.Background(), "t1")
	if err != nil || len(listed) != 1 || listed[0].Token != token {
		t.Fatalf("List = %+v, %v", listed, err)
	}
}

// assertSecretsAbsent reads the database file and its WAL (when present)
// and fails when any of the canary values appears in the bytes.
func assertSecretsAbsent(t *testing.T, path string, canaries []string) error {
	t.Helper()
	files := []string{path}
	if _, err := os.Stat(path + "-wal"); err == nil {
		files = append(files, path+"-wal")
	}
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		for _, c := range canaries {
			if strings.Contains(string(b), c) {
				return fmt.Errorf("%s carries the canary %q", file, c)
			}
		}
	}
	return nil
}

// TestPushSaveRules proves the Save shape rules: a generated id for a
// config without one, the required URL, and the refusal of a missing
// task.
func TestPushSaveRules(t *testing.T) {
	clk := &clock{t: testNow}
	s, _ := openStore(t, clk, "alice", newFakeSecrets())
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	cfg, err := s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{URL: "http://127.0.0.1:9999/push"})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !strings.HasPrefix(cfg.ID, "pc_") || len(cfg.ID) != len("pc_")+16 {
		t.Fatalf("generated id = %q, want pc_ plus 16 hex", cfg.ID)
	}
	if !isHex16(cfg.ID[len("pc_"):]) {
		t.Fatalf("generated id %q is not 16 hex", cfg.ID)
	}
	if cfg.TaskID != "t1" {
		t.Fatalf("TaskID = %q, want t1", cfg.TaskID)
	}
	if _, err := s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{Token: "t"}); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("Save without URL: %v, want invalid params", err)
	}
	// A task without a row yet (the SDK saves the push config of a first
	// message before the admission) is a pending config: Save succeeds,
	// and Get does not see it until the task commits.
	pending, err := s.PushConfigs().Save(context.Background(), "nope", &a2a.PushConfig{URL: "http://127.0.0.1:9999/push"})
	if err != nil {
		t.Fatalf("Save on a task without a row: %v, want a pending config", err)
	}
	if _, err := s.PushConfigs().Get(context.Background(), "nope", pending.ID); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Fatalf("Get of a pending config: %v, want push config not found", err)
	}
}

// TestPushReplaceDeletesOldHandles proves a replace stores the new
// handles, deletes the previous row's handles from the provider, and
// returns the new values.
func TestPushReplaceDeletesOldHandles(t *testing.T) {
	clk := &clock{t: testNow}
	sec := newFakeSecrets()
	s, _ := openStore(t, clk, "alice", sec)
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	first, err := s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{
		ID: "pc_fixed", URL: "http://127.0.0.1:9999/push", Token: "token-one", Auth: &a2a.PushAuthInfo{Scheme: "Basic", Credentials: "creds-one"},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	_, err = s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{
		ID: "pc_fixed", URL: "http://127.0.0.1:9999/push", Token: "token-two", Auth: &a2a.PushAuthInfo{Scheme: "Bearer", Credentials: "creds-two"},
	})
	if err != nil {
		t.Fatalf("replace Save: %v", err)
	}
	sec.mu.Lock()
	deleted := append([]string(nil), sec.deleted...)
	sec.mu.Unlock()
	if len(deleted) != 2 {
		t.Fatalf("deleted handles = %v, want the two previous handles", deleted)
	}
	got, err := s.PushConfigs().Get(context.Background(), "t1", "pc_fixed")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Token != "token-two" || got.Auth == nil || got.Auth.Credentials != "creds-two" || got.Auth.Scheme != "Bearer" {
		t.Fatalf("Get after replace = %+v", got)
	}
	if first.TaskID != "t1" || first.Token != "token-one" {
		t.Fatalf("first Save returned %+v, want the original values", first)
	}
}

// TestPushLostSecrets proves the provider that lost its handles (a new
// in-memory provider after a reopen) makes the config omitted from
// List, push.ErrPushConfigNotFound from Get, and counted by
// UnavailableCount.
func TestPushLostSecrets(t *testing.T) {
	clk := &clock{t: testNow}
	path := t.TempDir() + "/registry.db"
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice")})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	cfg, err := s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{URL: "http://127.0.0.1:9999/push", Token: "secret-token"})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	s.Close()
	// The default in-memory provider is fresh: the handles are lost.
	s2, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner("alice")})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	listed, err := s2.PushConfigs().List(context.Background(), "t1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("List with lost handles = %+v, want omitted", listed)
	}
	if _, err := s2.PushConfigs().Get(context.Background(), "t1", cfg.ID); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Fatalf("Get with lost handles: %v, want not found", err)
	}
	n, err := s2.PushConfigs().UnavailableCount(context.Background(), "t1")
	if err != nil {
		t.Fatalf("UnavailableCount: %v", err)
	}
	if n != 1 {
		t.Fatalf("UnavailableCount = %d, want 1", n)
	}
	// The row itself is still there (the durable record survives).
	var rows int
	if err := s2.db.QueryRow(`SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = 't1'`).Scan(&rows); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
}

// TestPushDelete proves Delete and DeleteAll remove the rows and their
// secret handles, and are idempotent.
func TestPushDelete(t *testing.T) {
	clk := &clock{t: testNow}
	sec := newFakeSecrets()
	s, _ := openStore(t, clk, "alice", sec)
	if _, err := s.Create(context.Background(), mkTask("t1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	a, err := s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{ID: "a", URL: "http://127.0.0.1:9999/a", Token: "ta"})
	if err != nil {
		t.Fatalf("Save a: %v", err)
	}
	b, err := s.PushConfigs().Save(context.Background(), "t1", &a2a.PushConfig{ID: "b", URL: "http://127.0.0.1:9999/b", Token: "tb", Auth: &a2a.PushAuthInfo{Scheme: "Basic", Credentials: "cb"}})
	if err != nil {
		t.Fatalf("Save b: %v", err)
	}
	if err := s.PushConfigs().Delete(context.Background(), "t1", "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.PushConfigs().Get(context.Background(), "t1", "a"); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Fatalf("Get deleted: %v, want not found", err)
	}
	sec.mu.Lock()
	deleted := append([]string(nil), sec.deleted...)
	sec.mu.Unlock()
	if len(deleted) != 1 {
		t.Fatalf("deleted after Delete = %v, want the token handle of a", deleted)
	}
	if a.URL == "" || b.URL == "" {
		t.Fatalf("Save copies = %+v, %+v", a, b)
	}
	// Deleting again is a no-op.
	if err := s.PushConfigs().Delete(context.Background(), "t1", "a"); err != nil {
		t.Fatalf("Delete twice: %v", err)
	}
	if err := s.PushConfigs().DeleteAll(context.Background(), "t1"); err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	listed, err := s.PushConfigs().List(context.Background(), "t1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("List after DeleteAll = %+v, want none", listed)
	}
	sec.mu.Lock()
	total := len(sec.deleted)
	sec.mu.Unlock()
	// b had a token and a credential: DeleteAll deleted both.
	if total != 3 {
		t.Fatalf("deleted after DeleteAll = %d, want 3 (a token, b token, b credentials)", total)
	}
}

// isHex16 reports whether s is 16 lowercase hex characters.
func isHex16(s string) bool {
	if len(s) != 16 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// openStore is openTest with a caller-chosen secret provider.
func openStore(t *testing.T, clk *clock, owner string, sec a2acore.SecretProvider) (*Store, string) {
	t.Helper()
	path := t.TempDir() + "/registry.db"
	s, err := OpenFile(path, Options{Now: clk.Now, Owner: fixedOwner(owner), Secrets: sec})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}
