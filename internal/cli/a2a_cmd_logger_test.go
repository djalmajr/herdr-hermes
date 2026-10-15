package cli

// The DJA-337 serve logger test: the serve passes the agent a JSON
// logger on stderr at the WARN level, so the agent's warning records —
// the push gate's suppression among them — reach stderr as JSON while
// stdout keeps the JSON status lines. The fixture seeds the database
// with a working task of a non-local owner, the task's push config
// without a recorded owner reference (saved without a stamp, like a
// legacy row) and a pending answer decision: the serve's decision loop
// applies the decision out of band and the gate suppresses the final
// push with no_owner_reference.

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/outbox"
)

// The fixture values of the DJA-337 logger test: every one of them is
// checked as absent from the serve output.
const (
	a2a337Task     = "t-337"
	a2a337Context  = "c-337"
	a2a337Config   = "pc-337"
	a2a337Decision = "d-337"
	a2a337Owner    = "owner-337"
	a2a337Project  = "org/dja337"
	a2a337URL      = "http://127.0.0.1:9996/push"
	a2a337Token    = "dja337-canary-VALUE"
	a2a337Answer   = "the dja337 recorded answer"
)

// The fixture values of the DJA-337 round-2 failing-push case: a local
// operator task whose push config is always sent, to a loopback hook
// the SSRF guard refuses; the canary of the hook path must not reach
// the output.
const (
	a2a337bTask     = "t-337b"
	a2a337bContext  = "c-337b"
	a2a337bConfig   = "pc-337b"
	a2a337bDecision = "d-337b"
	a2a337bURL      = "http://127.0.0.1:9/hook/secret-canary-337"
	a2a337bAnswer   = "the dja337b recorded answer"
)

// a2a337Fixture seeds the serve database with the DJA-337 suppression
// fixture and returns the database path: a working task of the
// non-local owner, the task's push config without a recorded owner
// reference (the Save runs without a stamp, like a legacy row) and a
// pending answer decision. The shared secret provider keeps the token
// so the serve's store resolves the handle and lists the config.
func a2a337Fixture(t *testing.T, cfgDir string, sec a2acore.SecretProvider) string {
	t.Helper()
	db := filepath.Join(outbox.StateDir(cfgDir), "registry.db")
	s, err := a2astore.OpenFile(db, a2astore.Options{
		Now:     func() time.Time { return serveFixedNow },
		Owner:   func(context.Context) (string, error) { return a2a337Owner, nil },
		Secrets: sec,
	})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := s.Create(context.Background(), &a2a.Task{
		ID:        a2a.TaskID(a2a337Task),
		ContextID: a2a337Context,
		Metadata:  map[string]any{a2acore.MetaProject: a2a337Project},
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.PushConfigs().Save(context.Background(), a2a337Task, &a2a.PushConfig{
		ID:    a2a337Config,
		URL:   a2a337URL,
		Token: a2a337Token,
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, r, err := s.RecordDecision(context.Background(), a2acore.Decision{
		ID: a2a337Decision, TaskID: a2a337Task, Kind: a2acore.DecisionAnswer,
		Text: a2a337Answer, Actor: a2a337Owner,
	}, serveFixedNow); err != nil {
		t.Fatalf("RecordDecision: %v", err)
	} else if r != nil {
		t.Fatalf("RecordDecision refused: %s %s", r.Code, r.Detail)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return db
}

// startA2A337Serve launches a2a serve in a goroutine with the
// test-owned context, the fake sender, the shared secret provider and
// the fast loop intervals, and returns the output buffers once the
// serving line is printed.
func startA2A337Serve(t *testing.T, cfgDir string, sec a2acore.SecretProvider) (exitCh chan int, cancel func(), out, errb *testLockedBuffer) {
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
	return exitCh, c, out, errb
}

// a2a337bFixture seeds the serve database with the DJA-337 round-2
// failing-push fixture and returns the database path: a working task
// of the local operator (its push configs are always sent), its push
// config to a loopback hook the SSRF guard refuses and a pending
// answer decision. The serve's decision loop applies the decision out
// of band, the final push fails and the SDK sender logs it.
func a2a337bFixture(t *testing.T, cfgDir string) string {
	t.Helper()
	db := filepath.Join(outbox.StateDir(cfgDir), "registry.db")
	s, err := a2astore.OpenFile(db, a2astore.Options{
		Now:   func() time.Time { return serveFixedNow },
		Owner: func(context.Context) (string, error) { return a2acore.LocalSubjectID, nil },
	})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := s.Create(context.Background(), &a2a.Task{
		ID:        a2a.TaskID(a2a337bTask),
		ContextID: a2a337bContext,
		Metadata:  map[string]any{a2acore.MetaProject: "org/example"},
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := s.PushConfigs().Save(context.Background(), a2a337bTask, &a2a.PushConfig{
		ID:  a2a337bConfig,
		URL: a2a337bURL,
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, r, err := s.RecordDecision(context.Background(), a2acore.Decision{
		ID: a2a337bDecision, TaskID: a2a337bTask, Kind: a2acore.DecisionAnswer,
		Text: a2a337bAnswer, Actor: a2acore.LocalSubjectID,
	}, serveFixedNow); err != nil {
		t.Fatalf("RecordDecision: %v", err)
	} else if r != nil {
		t.Fatalf("RecordDecision refused: %s %s", r.Code, r.Detail)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return db
}

// TestA2AServeMasksPushURLInStderr proves the DJA-337 round-2 wiring
// end to end: the serve applies the pending decision of a local task
// out of band, the final push fails at the SSRF guard and the SDK
// sender's "push sending failed" record reaches stderr as JSON through
// the serve logger, with the URL masked as <url> — the canary of the
// hook path never appears — while stdout keeps the serving line.
func TestA2AServeMasksPushURLInStderr(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	a2a337bFixture(t, cfgDir)
	exitCh, cancel, out, errb := startA2A337Serve(t, cfgDir, nil)
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	// The first stderr line is the sender's record: the decision loop
	// applies the decision out of band every 50 ms, the gate passes
	// the local config through and the send fails at the SSRF guard.
	line := errb.nextLine(t, 10*time.Second)
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("first stderr line %q is not JSON: %v", line, err)
	}
	if rec["msg"] != "push sending failed" {
		t.Fatalf("record = %v, want msg %q", rec, "push sending failed")
	}
	// The URL is masked and the canary of the hook path never appears.
	stderr := errb.snapshot()
	if !strings.Contains(stderr, "<url>") {
		t.Fatalf("stderr %q lacks the masked <url>", stderr)
	}
	for _, banned := range []string{a2a337bURL, "secret-canary-337", a2a337bAnswer} {
		if strings.Contains(stderr, banned) {
			t.Fatalf("stderr carries %q (%q)", banned, stderr)
		}
	}
	// Exactly one stderr line (the JSON record) and stdout keeps the
	// serving line and nothing else.
	if n := strings.Count(stderr, "\n"); n != 1 {
		t.Fatalf("stderr has %d line(s), want exactly the one JSON record (%q)", n, stderr)
	}
	stdout := out.snapshot()
	if n := strings.Count(stdout, "\n"); n != 1 {
		t.Fatalf("stdout has %d line(s), want exactly the serving line (%q)", n, stdout)
	}
	if !strings.Contains(stdout, `"status":"serving"`) {
		t.Fatalf("stdout = %q, want the serving line", stdout)
	}
}

// TestA2AServeLogsSuppressedPushOnStderr proves the DJA-337 wiring:
// the decision loop of the serve applies the pending decision out of
// band and the push gate suppresses the final push with
// no_owner_reference, logged on stderr as exactly one JSON record with
// only the time, the level, the fixed msg, the task id and the fixed
// reason — never the config's token, the config's URL, the owner id,
// the project or the decision text — while stdout keeps the serving
// line.
func TestA2AServeLogsSuppressedPushOnStderr(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	sec := a2acore.NewMemorySecrets()
	db := a2a337Fixture(t, cfgDir, sec)
	exitCh, cancel, out, errb := startA2A337Serve(t, cfgDir, sec)
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	// The first stderr line is the suppression record: the decision
	// loop applies the decision out of band every 50 ms and the gate
	// suppresses the final push with no_owner_reference. Poll with a
	// bounded deadline, no fixed sleep.
	line := errb.nextLine(t, 10*time.Second)
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("first stderr line %q is not JSON: %v", line, err)
	}
	if len(rec) != 5 {
		t.Fatalf("record has %d keys (%v), want exactly time, level, msg, task_id and reason", len(rec), rec)
	}
	for _, k := range []string{"time", "level", "msg", "task_id", "reason"} {
		if _, ok := rec[k]; !ok {
			t.Fatalf("record is missing the key %q (%v)", k, rec)
		}
	}
	if rec["level"] != "WARN" || rec["msg"] != "a2a: push suppressed" ||
		rec["reason"] != "no_owner_reference" || rec["task_id"] != a2a337Task {
		t.Fatalf("record = %v, want the WARN record msg %q reason %q task_id %q",
			rec, "a2a: push suppressed", "no_owner_reference", a2a337Task)
	}
	// The decision was applied out of band by the serve: the decision
	// is applied and the task is completed (a second process reading
	// the same database sees the committed states).
	ro, err := sql.Open("sqlite", "file:"+filepath.ToSlash(db)+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer ro.Close()
	var dstate, tstate string
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := ro.QueryRow(`SELECT state FROM a2a_decisions WHERE decision_id = ?`, a2a337Decision).Scan(&dstate); err != nil {
			t.Fatalf("decision state: %v", err)
		}
		if err := ro.QueryRow(`SELECT state FROM a2a_tasks WHERE task_id = ?`, a2a337Task).Scan(&tstate); err != nil {
			t.Fatalf("task state: %v", err)
		}
		if dstate == a2acore.DecisionApplied && tstate == a2acore.StateCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("decision state = %q and task state = %q, want applied and completed", dstate, tstate)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Exactly one stderr line (the JSON record, no text lines) and
	// none of the fixture values in it.
	stderr := errb.snapshot()
	if n := strings.Count(stderr, "\n"); n != 1 {
		t.Fatalf("stderr has %d line(s), want exactly the one JSON record (%q)", n, stderr)
	}
	for _, banned := range []string{a2a337Token, a2a337URL, a2a337Owner, a2a337Project, a2a337Answer} {
		if strings.Contains(stderr, banned) {
			t.Fatalf("stderr carries %q (%q)", banned, stderr)
		}
	}
	// Stdout keeps the serving line and nothing else.
	stdout := out.snapshot()
	if n := strings.Count(stdout, "\n"); n != 1 {
		t.Fatalf("stdout has %d line(s), want exactly the serving line (%q)", n, stdout)
	}
	if !strings.Contains(stdout, `"status":"serving"`) {
		t.Fatalf("stdout = %q, want the serving line", stdout)
	}
}
