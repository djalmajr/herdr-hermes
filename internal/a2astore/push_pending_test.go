package a2astore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
)

const ppToken = "pp-fixture-token"
const ppCreds = "pp-fixture-credentials"

// ppOpen opens a store on a test clock with a recording provider.
func ppOpen(t *testing.T) (*Store, *clock, *fakeSecrets) {
	t.Helper()
	clk := &clock{t: testNow}
	sec := newFakeSecrets()
	s, _ := openStore(t, clk, "alice", sec)
	return s, clk, sec
}

// ppSave saves one pending-or-committed config with a token and
// credentials and returns it.
func ppSave(t *testing.T, s *Store, taskID string) *a2a.PushConfig {
	t.Helper()
	cfg, err := s.PushConfigs().Save(context.Background(), a2a.TaskID(taskID), &a2a.PushConfig{
		URL:   "http://127.0.0.1:9/push",
		Token: ppToken,
		Auth:  &a2a.PushAuthInfo{Scheme: "Bearer", Credentials: ppCreds},
	})
	if err != nil {
		t.Fatalf("Save %s: %v", taskID, err)
	}
	return cfg
}

// ppRows counts the raw push config rows of one task.
func ppRows(t *testing.T, s *Store, taskID string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM a2a_push_configs WHERE task_id = ?`, taskID).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

// ppLive counts the live secret values of the provider.
func ppLive(sec *fakeSecrets) int {
	sec.mu.Lock()
	defer sec.mu.Unlock()
	return len(sec.values)
}

// ppDeletedCount counts the handles the provider was asked to delete.
func ppDeletedCount(sec *fakeSecrets) int {
	sec.mu.Lock()
	defer sec.mu.Unlock()
	return len(sec.deleted)
}

// ppForgetting is a provider whose Delete also forgets the value, so the
// live map shrinks like a real provider does (fakeSecrets only logs).
type ppForgetting struct {
	*fakeSecrets
}

// Delete implements a2acore.SecretProvider and forgets the value.
func (f ppForgetting) Delete(ctx context.Context, handle string) error {
	if err := f.fakeSecrets.Delete(ctx, handle); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.values, handle)
	return nil
}

// ppOpenForgetting opens a store whose provider forgets deleted values.
func ppOpenForgetting(t *testing.T) (*Store, *clock, *fakeSecrets) {
	t.Helper()
	clk := &clock{t: testNow}
	sec := newFakeSecrets()
	s, _ := openStore(t, clk, "alice", ppForgetting{sec})
	return s, clk, sec
}

func TestPushPendingSaveWithoutTaskRowStoresOnlyHandles(t *testing.T) {
	s, _, sec := ppOpen(t)
	cfg := ppSave(t, s, "t-new")
	if cfg.ID == "" || cfg.Token != ppToken || cfg.Auth == nil || cfg.Auth.Credentials != ppCreds || cfg.TaskID != "t-new" {
		t.Fatalf("Save returned %+v, want the original values on the new task", cfg)
	}
	var url, scheme, tokenRef, credsRef, created string
	if err := s.db.QueryRow(`SELECT url, auth_scheme, token_ref, credentials_ref, created FROM a2a_push_configs WHERE task_id = 't-new'`).
		Scan(&url, &scheme, &tokenRef, &credsRef, &created); err != nil {
		t.Fatalf("row: %v", err)
	}
	if tokenRef == "" || tokenRef == ppToken || credsRef == "" || credsRef == ppCreds || created != testNow.Format(tsLayout) {
		t.Fatalf("row = token_ref %q credentials_ref %q created %q, want handles and the store timestamp", tokenRef, credsRef, created)
	}
	if ppLive(sec) != 2 {
		t.Fatalf("live secret values = %d, want 2", ppLive(sec))
	}
	if _, err := s.PushConfigs().Save(context.Background(), "t-new", &a2a.PushConfig{}); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("Save without URL = %v, want a2a.ErrInvalidParams", err)
	}
}

func TestPushPendingIsInvisibleUntilTheTaskCommits(t *testing.T) {
	s, _, _ := ppOpen(t)
	ctx := context.Background()
	cfg := ppSave(t, s, "t-new")
	if _, err := s.PushConfigs().Get(ctx, "t-new", cfg.ID); !errors.Is(err, push.ErrPushConfigNotFound) {
		t.Fatalf("Get pending = %v, want push.ErrPushConfigNotFound", err)
	}
	list, err := s.PushConfigs().List(ctx, "t-new")
	if err != nil || list == nil || len(list) != 0 {
		t.Fatalf("List pending = %#v, %v, want an empty non-nil slice", list, err)
	}
	if n, err := s.PushConfigs().UnavailableCount(ctx, "t-new"); err != nil || n != 0 {
		t.Fatalf("UnavailableCount pending = %d, %v, want 0", n, err)
	}
	// The task commits: the same config becomes visible with its values.
	if _, err := s.Create(ctx, mkTask("t-new", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := s.PushConfigs().Get(ctx, "t-new", cfg.ID)
	if err != nil || got.Token != ppToken || got.Auth == nil || got.Auth.Credentials != ppCreds {
		t.Fatalf("Get committed = %+v, %v, want the original values", got, err)
	}
	list, err = s.PushConfigs().List(ctx, "t-new")
	if err != nil || len(list) != 1 || list[0].ID != cfg.ID || list[0].Token != ppToken {
		t.Fatalf("List committed = %+v, %v, want the one config", list, err)
	}
}

func TestPushPendingUnavailableCountStillCountsCommittedConfigs(t *testing.T) {
	s, _, sec := ppOpen(t)
	ctx := context.Background()
	if _, err := s.Create(ctx, mkTask("t-1", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	ppSave(t, s, "t-1")
	sec.mu.Lock()
	sec.values = map[string]string{} // the provider lost its values
	sec.mu.Unlock()
	if n, err := s.PushConfigs().UnavailableCount(ctx, "t-1"); err != nil || n != 1 {
		t.Fatalf("UnavailableCount committed = %d, %v, want 1", n, err)
	}
}

func TestPushPendingDiscardUncommitted(t *testing.T) {
	s, _, sec := ppOpenForgetting(t)
	ctx := context.Background()
	ppSave(t, s, "t-pending")
	ppSave(t, s, "t-pending-2")
	if _, err := s.Create(ctx, mkTask("t-done", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	ppSave(t, s, "t-done")
	if ppLive(sec) != 6 {
		t.Fatalf("live secret values = %d, want 6", ppLive(sec))
	}
	// A committed task's configs are never touched.
	if n, err := s.PushConfigs().DiscardUncommitted(ctx, "t-done"); err != nil || n != 0 {
		t.Fatalf("DiscardUncommitted committed = %d, %v, want 0", n, err)
	}
	if ppRows(t, s, "t-done") != 1 || ppLive(sec) != 6 || ppDeletedCount(sec) != 0 {
		t.Fatalf("committed config changed: rows=%d live=%d deleted=%d", ppRows(t, s, "t-done"), ppLive(sec), ppDeletedCount(sec))
	}
	// A task id that never had anything discards nothing.
	if n, err := s.PushConfigs().DiscardUncommitted(ctx, "t-unknown"); err != nil || n != 0 {
		t.Fatalf("DiscardUncommitted unknown = %d, %v, want 0", n, err)
	}
	// The pending task's rows and handles go; another pending task stays.
	if n, err := s.PushConfigs().DiscardUncommitted(ctx, "t-pending"); err != nil || n != 1 {
		t.Fatalf("DiscardUncommitted pending = %d, %v, want 1", n, err)
	}
	if ppRows(t, s, "t-pending") != 0 || ppRows(t, s, "t-pending-2") != 1 || ppRows(t, s, "t-done") != 1 {
		t.Fatalf("rows after discard: pending=%d pending-2=%d done=%d, want 0/1/1", ppRows(t, s, "t-pending"), ppRows(t, s, "t-pending-2"), ppRows(t, s, "t-done"))
	}
	if ppDeletedCount(sec) != 2 || ppLive(sec) != 4 {
		t.Fatalf("handles deleted=%d live=%d, want the token and credentials handles of one row gone (2/4)", ppDeletedCount(sec), ppLive(sec))
	}
	if n, err := s.PushConfigs().DiscardUncommitted(ctx, "t-pending"); err != nil || n != 0 {
		t.Fatalf("second DiscardUncommitted = %d, %v, want 0", n, err)
	}
}

func TestPushPendingRecoverHeldSweepsOnlyOldPendingConfigs(t *testing.T) {
	s, clk, sec := ppOpenForgetting(t)
	ctx := context.Background()
	// Old pending configs (two tasks, one with a stamp that does not parse),
	// an old config of a committed task, and one held delivery past the grace.
	ppSave(t, s, "t-old")
	ppSave(t, s, "t-garbled")
	if _, err := s.db.Exec(`UPDATE a2a_push_configs SET created = 'not-a-time' WHERE task_id = 't-garbled'`); err != nil {
		t.Fatalf("garble created: %v", err)
	}
	if _, err := s.Create(ctx, mkTask("t-done", "c1", a2a.TaskStateSubmitted, 1)); err != nil {
		t.Fatalf("Create: %v", err)
	}
	ppSave(t, s, "t-done")
	if _, err := s.Admit(ctx, admission("alice", "m-held", "t-held", "c-held", "acme/widgets", "s1", 1, 1, true, []a2acore.PlannedDelivery{ownerDelivery("m1/ws1:p1")})); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	clk.Add(6 * time.Minute)
	ppSave(t, s, "t-young") // created at the recover instant: inside the grace
	if ppLive(sec) != 8 {
		t.Fatalf("live secret values = %d, want 8", ppLive(sec))
	}

	marked, err := s.RecoverHeld(ctx, clk.Now(), 5*time.Minute)
	if err != nil {
		t.Fatalf("RecoverHeld: %v", err)
	}
	if marked != 1 {
		t.Fatalf("RecoverHeld returned %d, want 1 (only the held delivery; the swept configs are not counted)", marked)
	}
	for task, want := range map[string]int{"t-old": 0, "t-garbled": 0, "t-young": 1, "t-done": 1} {
		if got := ppRows(t, s, task); got != want {
			t.Errorf("push config rows of %s = %d, want %d", task, got, want)
		}
	}
	if ppDeletedCount(sec) != 4 || ppLive(sec) != 4 {
		t.Errorf("handles deleted=%d live=%d, want the 4 handles of the two swept rows gone (4/4)", ppDeletedCount(sec), ppLive(sec))
	}
	// The kept pending config is still discardable and the committed one
	// still resolves.
	list, err := s.PushConfigs().List(ctx, "t-done")
	if err != nil || len(list) != 1 || list[0].Token != ppToken {
		t.Errorf("committed List = %+v, %v, want the kept config", list, err)
	}
	if marked, err := s.RecoverHeld(ctx, clk.Now(), 5*time.Minute); err != nil || marked != 0 {
		t.Errorf("second RecoverHeld = %d, %v, want 0", marked, err)
	}
	clk.Add(6 * time.Minute)
	if _, err := s.RecoverHeld(ctx, clk.Now(), 5*time.Minute); err != nil {
		t.Fatalf("third RecoverHeld: %v", err)
	}
	if ppRows(t, s, "t-young") != 0 || ppRows(t, s, "t-done") != 1 {
		t.Errorf("after the grace: young=%d done=%d, want 0/1", ppRows(t, s, "t-young"), ppRows(t, s, "t-done"))
	}
}
