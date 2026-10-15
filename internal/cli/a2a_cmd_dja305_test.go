package cli

// The DJA-305 serve tests: a cleanup failure of the secret handles at
// startup (the recovery itself committed) does not stop the serve — it
// prints one count-only line on stderr and keeps serving — while a
// transaction failure of the recovery still exits 4.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/outbox"
)

// errDja305ServeDelete is the fixed failure of the DJA-305 serve test
// provider's Delete.
var errDja305ServeDelete = errors.New("dja305: injected serve delete failure")

// dja305ServeSecrets is the DJA-305 test SecretProvider injected into
// the a2a serve through a2aServeSecrets: it fails the Delete of the
// handles marked by failDeleteOf and records the successful deletes.
type dja305ServeSecrets struct {
	mu         sync.Mutex
	seq        int
	values     map[string]string
	handles    []string
	failDelete map[string]bool
	deleted    []string
}

func newDja305ServeSecrets() *dja305ServeSecrets {
	return &dja305ServeSecrets{values: map[string]string{}, failDelete: map[string]bool{}}
}

// Put implements a2acore.SecretProvider.
func (d *dja305ServeSecrets) Put(_ context.Context, value string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	h := "dja305-serve-h" + strconv.Itoa(d.seq)
	d.values[h] = value
	d.handles = append(d.handles, h)
	return h, nil
}

// Get implements a2acore.SecretProvider.
func (d *dja305ServeSecrets) Get(_ context.Context, handle string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.values[handle]
	if !ok {
		return "", a2acore.ErrSecretUnavailable
	}
	return v, nil
}

// Delete implements a2acore.SecretProvider.
func (d *dja305ServeSecrets) Delete(_ context.Context, handle string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failDelete[handle] {
		return errDja305ServeDelete
	}
	d.deleted = append(d.deleted, handle)
	delete(d.values, handle)
	return nil
}

// failDeleteOf makes the Delete of handle fail.
func (d *dja305ServeSecrets) failDeleteOf(handle string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failDelete[handle] = true
}

// handle is the i-th handle put through Put (1 is the first).
func (d *dja305ServeSecrets) handle(i int) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if i < 1 || i > len(d.handles) {
		panic("dja305: handle out of range")
	}
	return d.handles[i-1]
}

// deletedCount counts the successful deletes.
func (d *dja305ServeSecrets) deletedCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.deleted)
}

// a2aDja305Fixture seeds the serve database with a stale pending push
// config (token and credentials) of a task that never commits and a
// held delivery of another never-committed task, both older than the
// serve recovery grace, and returns the two stored handles.
func a2aDja305Fixture(t *testing.T, cfgDir string, sec *dja305ServeSecrets) (string, string) {
	t.Helper()
	db := filepath.Join(outbox.StateDir(cfgDir), "registry.db")
	old := func() time.Time { return serveFixedNow.Add(-10 * time.Minute) }
	s, err := a2astore.OpenFile(db, a2astore.Options{
		Now:     old,
		Owner:   func(context.Context) (string, error) { return "local", nil },
		Secrets: sec,
	})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := s.PushConfigs().Save(context.Background(), "t-305", &a2a.PushConfig{
		ID:    "pc1",
		URL:   "http://127.0.0.1:9999/push",
		Token: "dja305-serve-token-VALUE",
		Auth:  &a2a.PushAuthInfo{Scheme: "Bearer", Credentials: "dja305-serve-creds-VALUE"},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := s.Admit(context.Background(), a2acore.Admission{
		Envelope: a2acore.Envelope{
			Subject: "local", MessageID: "m-305", TaskID: "t-305-held",
			ContextID: "c-305", Project: "org/example", Session: "s-305",
			Epoch: 1, Seq: 1, TextBytes: 16,
		},
		TaskID:     "t-305-held",
		ContextID:  "c-305",
		NewTask:    true,
		Deliveries: []a2acore.PlannedDelivery{{Role: a2acore.RoleOwner, Ref: "owner-a", State: a2acore.DeliveryHeld}},
	}); err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return sec.handle(1), sec.handle(2)
}

// startA2ADja305Serve launches a2a serve in a goroutine with the
// test-owned context, the fake sender, the injected secret provider and
// the fast loop intervals, and returns the base url once the serving
// line is printed.
func startA2ADja305Serve(t *testing.T, cfgDir string, sec a2acore.SecretProvider) (base string, exitCh chan int, cancel func(), out, errb *testLockedBuffer) {
	t.Helper()
	out = &testLockedBuffer{}
	errb = &testLockedBuffer{}
	sender := &fakeA2ASender{script: map[string][]notify.SendResult{}}
	ctx, c := context.WithCancel(context.Background())
	origCtx := a2aServeContext
	a2aServeContext = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	origSender := newA2aNoticeSender
	newA2aNoticeSender = func(string, []string) notify.Sender { return sender }
	origDeliver := a2aDefaultDeliverInterval
	a2aDefaultDeliverInterval = 50 * time.Millisecond
	origDecision := a2aDecisionInterval
	a2aDecisionInterval = 50 * time.Millisecond
	origSecrets := a2aServeSecrets
	a2aServeSecrets = sec
	t.Cleanup(func() {
		a2aServeContext = origCtx
		newA2aNoticeSender = origSender
		a2aDefaultDeliverInterval = origDeliver
		a2aDecisionInterval = origDecision
		a2aServeSecrets = origSecrets
		c()
	})
	exitCh = make(chan int, 1)
	env := a2aTestEnv(t, cfgDir, out, errb)
	go func() {
		exitCh <- cmdA2A([]string{"serve", "--listen", "127.0.0.1:0"}, env)
	}()
	line := out.nextLine(t, 10*time.Second)
	var start struct {
		Status string `json:"status"`
		Listen string `json:"listen"`
		DB     string `json:"db"`
	}
	if err := json.Unmarshal([]byte(line), &start); err != nil {
		t.Fatalf("serving line %q is not JSON: %v", line, err)
	}
	if start.Status != "serving" || start.Listen == "" || start.DB == "" {
		t.Fatalf("serving line = %q, want status serving with a listen address and a db path", line)
	}
	return "http://" + start.Listen, exitCh, c, out, errb
}

// TestDJA305ServeSurvivesCleanupFailure proves acceptance 5: with the
// test provider failing the handle deletions at startup, the serve
// starts (the recovery committed), writes exactly one count-only line
// on stderr and still answers the card.
func TestDJA305ServeSurvivesCleanupFailure(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	sec := newDja305ServeSecrets()
	tokenRef, credsRef := a2aDja305Fixture(t, cfgDir, sec)
	sec.failDeleteOf(tokenRef)
	sec.failDeleteOf(credsRef)
	base, exitCh, cancel, out, errb := startA2ADja305Serve(t, cfgDir, sec)
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	// exactly one count-only line on stderr: no handle, no secret, no
	// provider message.
	const want = "a2a serve: 2 secret handle deletions are pending; they are retried on the next recovery"
	if got := errb.snapshot(); got != want+"\n" {
		t.Fatalf("stderr = %q, want exactly the count line %q", got, want)
	}
	// stdout carries exactly the serving line.
	if n := strings.Count(out.snapshot(), "\n"); n != 1 {
		t.Fatalf("serve printed %d stdout lines, want exactly one (%q)", n, out.snapshot())
	}
	// and the serve still answers the card.
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(base + "/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("card: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("card status = %d, want 200", resp.StatusCode)
	}
}

// TestDJA305ServeReportsOldPendingAlone proves the fix of the second
// security finding: with only an old pending whose delete fails (no
// fresh config to discard at the startup recovery), the serve still
// starts, writes exactly one count-only line on stderr and answers the
// card.
func TestDJA305ServeReportsOldPendingAlone(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	sec := newDja305ServeSecrets()
	h, err := sec.Put(context.Background(), "dja305-serve-old-VALUE")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	sec.failDeleteOf(h)
	// the durable state of an earlier failed cleanup: one old pending
	// row in the cleanup ledger, scheduled an hour before now.
	db := filepath.Join(outbox.StateDir(cfgDir), "registry.db")
	s, err := a2astore.OpenFile(db, a2astore.Options{
		Now:     func() time.Time { return serveFixedNow },
		Owner:   func(context.Context) (string, error) { return "local", nil },
		Secrets: sec,
	})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	raw, rerr := sql.Open("sqlite", "file:"+filepath.ToSlash(db))
	if rerr != nil {
		t.Fatalf("open raw: %v", rerr)
	}
	if _, rerr := raw.Exec(`INSERT INTO a2a_secret_cleanup(handle, scheduled) VALUES(?, ?)`,
		h, serveFixedNow.Add(-time.Hour).Format("2006-01-02T15:04:05-07:00")); rerr != nil {
		_ = raw.Close()
		t.Fatalf("insert the old pending: %v", rerr)
	}
	if rerr := raw.Close(); rerr != nil {
		t.Fatalf("close raw: %v", rerr)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	base, exitCh, cancel, _, errb := startA2ADja305Serve(t, cfgDir, sec)
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	// exactly one count-only line on stderr, even though no fresh
	// cleanup failed at this startup.
	const want = "a2a serve: 1 secret handle deletions are pending; they are retried on the next recovery"
	if got := errb.snapshot(); got != want+"\n" {
		t.Fatalf("stderr = %q, want exactly the count line %q", got, want)
	}
	// and the serve still answers the card.
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(base + "/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("card: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("card status = %d, want 200", resp.StatusCode)
	}
}

// TestDJA305ServeTransactionFailureStillExits4 proves acceptance 5:
// with the recovery transaction failing, the serve still exits 4
// (fail-closed), the stdout line is the unavailable one, and nothing is
// committed: no delivery is marked, no handle is scheduled and no
// handle is touched.
func TestDJA305ServeTransactionFailureStillExits4(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	sec := newDja305ServeSecrets()
	_, _ = a2aDja305Fixture(t, cfgDir, sec)
	// make the recovery transaction fail: an abort trigger on the
	// deliveries update (the fixture holds one eligible delivery).
	dbPath := filepath.Join(outbox.StateDir(cfgDir), "registry.db")
	raw, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TRIGGER test_dja305_recover_fail BEFORE UPDATE ON a2a_deliveries BEGIN SELECT RAISE(ABORT, 'test dja305 abort'); END`); err != nil {
		_ = raw.Close()
		t.Fatalf("install the trigger: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}
	stdout, stderr, exit := runA2A(t, cfgDir, "serve", "--listen", "127.0.0.1:0")
	if exit != 4 {
		t.Fatalf("serve exit = %d, want 4 (stdout %q stderr %q)", exit, stdout, stderr)
	}
	if !strings.Contains(stdout, `"status":"unavailable"`) {
		t.Fatalf("stdout = %q, want the unavailable line", stdout)
	}
	if !strings.Contains(stderr, "test dja305 abort") {
		t.Fatalf("stderr = %q, want the transaction failure", stderr)
	}
	// fail-closed: the rolled-back recovery changed nothing and never
	// touched a handle.
	ro, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?mode=ro")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer ro.Close()
	var state string
	if err := ro.QueryRow(`SELECT state FROM a2a_deliveries`).Scan(&state); err != nil {
		t.Fatalf("delivery state: %v", err)
	}
	if state != a2acore.DeliveryHeld {
		t.Fatalf("delivery state = %q, want held (the transaction rolled back)", state)
	}
	var pending int
	if err := ro.QueryRow(`SELECT COUNT(*) FROM a2a_secret_cleanup`).Scan(&pending); err != nil {
		t.Fatalf("cleanup count: %v", err)
	}
	if pending != 0 {
		t.Fatalf("cleanup rows = %d, want 0 (nothing was scheduled)", pending)
	}
	if n := sec.deletedCount(); n != 0 {
		t.Fatalf("provider deletes = %d, want 0 (no handle was touched)", n)
	}
}

// TestDJA305ServeMixedSQLExits4 proves the mixed failure end to end:
// with the fresh deletions failing at the provider and the old drain
// failing with a SQL error, the serve refuses the start with exit 4
// and the unavailable line, like a transaction failure, and neither a
// handle nor the provider's error text reaches stdout or stderr.
func TestDJA305ServeMixedSQLExits4(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	sec := newDja305ServeSecrets()
	tok, creds := a2aDja305Fixture(t, cfgDir, sec)
	sec.failDeleteOf(tok)
	sec.failDeleteOf(creds)
	// a deterministic local SQL failure of the old drain at startup:
	// the trigger aborts the update that brings a fresh handle to
	// attempts 2 (the restricted drain already brought it to 1).
	raw, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(outbox.StateDir(cfgDir), "registry.db")))
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TRIGGER dja305_abort_second BEFORE UPDATE OF attempts ON a2a_secret_cleanup WHEN NEW.attempts=2 BEGIN SELECT RAISE(ABORT, 'dja305 mixed SQL failure'); END`); err != nil {
		_ = raw.Close()
		t.Fatalf("install the trigger: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}
	// run the serve with the test-owned context and the fake sender so
	// a started serve (the defect) is stopped and never spawns a
	// subprocess; the fixed refusal exits before serving.
	out := &testLockedBuffer{}
	errbuf := &testLockedBuffer{}
	sender := &fakeA2ASender{script: map[string][]notify.SendResult{}}
	ctx, cancel := context.WithCancel(context.Background())
	origCtx := a2aServeContext
	a2aServeContext = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	origSender := newA2aNoticeSender
	newA2aNoticeSender = func(string, []string) notify.Sender { return sender }
	origDeliver := a2aDefaultDeliverInterval
	a2aDefaultDeliverInterval = 50 * time.Millisecond
	origDecision := a2aDecisionInterval
	a2aDecisionInterval = 50 * time.Millisecond
	origSecrets := a2aServeSecrets
	a2aServeSecrets = sec
	t.Cleanup(func() {
		a2aServeContext = origCtx
		newA2aNoticeSender = origSender
		a2aDefaultDeliverInterval = origDeliver
		a2aDecisionInterval = origDecision
		a2aServeSecrets = origSecrets
		cancel()
	})
	env := a2aTestEnv(t, cfgDir, out, errbuf)
	exitCh := make(chan int, 1)
	go func() {
		exitCh <- cmdA2A([]string{"serve", "--listen", "127.0.0.1:0"}, env)
	}()
	select {
	case exit := <-exitCh:
		if exit != 4 {
			t.Fatalf("serve exit = %d, want 4 (stdout %q stderr %q)", exit, out.snapshot(), errbuf.snapshot())
		}
	case <-time.After(10 * time.Second):
		cancel()
		<-exitCh
		t.Fatalf("serve started despite the SQL failure in the old drain; want startup exit 4")
	}
	const want = `{"status":"unavailable","motivo":"a2a serve: registry database unavailable"}`
	if got := out.snapshot(); got != want+"\n" {
		t.Fatalf("stdout = %q, want exactly the unavailable line %q", got, want)
	}
	// no handle and no provider error text in the output.
	for _, banned := range []string{tok, creds, errDja305ServeDelete.Error()} {
		if strings.Contains(out.snapshot(), banned) || strings.Contains(errbuf.snapshot(), banned) {
			t.Fatalf("a handle or the provider error text reached the output (stdout %q stderr %q)", out.snapshot(), errbuf.snapshot())
		}
	}
}
