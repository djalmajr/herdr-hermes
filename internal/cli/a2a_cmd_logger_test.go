package cli

// The DJA-337 serve logger test: the serve passes the agent a JSON
// logger on stderr at the WARN level, so the agent's warning records —
// the push gate's suppression among them — reach stderr as JSON while
// stdout keeps the JSON status lines. The fixture seeds the database
// with a working task of a non-local owner, the task's push config
// without a recorded owner reference (saved without a stamp, like a
// legacy row) and a pending answer decision: the serve's decision loop
// applies the decision out of band and the gate suppresses the final
// push with no_owner_reference. The round-3 cases: a failed push send
// is logged on stderr with only the fixed classification of the
// error — never the error text, which carries the webhook URL or
// parts of it — for the SSRF guard's refusal (out of band) and for
// the invalid URL forms (out of band and live).

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"path/filepath"
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

// The DJA-337 round-3 invalid-URL cases: the store accepts any
// non-empty URL, and the SDK sender's error of the failed send carries
// the URL or parts of it — the canary after the quote in the malformed
// form, the whole path in the schemeless form. Neither may reach the
// output; the failure is logged with the fixed classification only.
const (
	a2a337cMalformedURL = `http://127.0.0.1:9/"/secret-canary-337%zz`
	a2a337cSchemeless   = "127.0.0.1:9/hook/secret-canary-337"
	a2a337cCanary       = "secret-canary-337"
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
// a2a337WakeErr is the test-owned stderr writer of the DJA-337 serve
// tests: it forwards every write to the shared locked buffer and
// wakes the wait on every write (the plSender.waitFor pattern of
// internal/a2aserver), so a wait on a serve-written result needs no
// sleep. The write count and the wake channel move under one lock, so
// a write before or after the waiter's grab is never lost: a write
// that finished before the grab shows up as a fresh count, and a
// write after the grab closes the channel the waiter blocks on.
type a2a337WakeErr struct {
	inner *testLockedBuffer
	mu    sync.Mutex
	wake  chan struct{}
	// writes is the write count so far; lastWoken is the count the
	// wait last consumed.
	writes, lastWoken int
}

// Write implements io.Writer.
func (b *a2a337WakeErr) Write(p []byte) (int, error) {
	n, err := b.inner.Write(p)
	b.mu.Lock()
	b.writes++
	if b.wake != nil {
		close(b.wake)
	}
	b.wake = make(chan struct{})
	b.mu.Unlock()
	return n, err
}

// waitWoken blocks until a write the wait has not consumed yet
// happened or the deadline passes: the wait rechecks its condition
// after the wake, and the deadline bounds it.
func (b *a2a337WakeErr) waitWoken(t *testing.T, deadline time.Time) {
	t.Helper()
	timeout := time.Until(deadline)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		b.mu.Lock()
		if b.wake == nil {
			b.wake = make(chan struct{})
		}
		fresh, wake := b.writes > b.lastWoken, b.wake
		b.mu.Unlock()
		if fresh {
			b.mu.Lock()
			b.lastWoken = b.writes
			b.mu.Unlock()
			return
		}
		select {
		case <-wake:
			b.mu.Lock()
			b.lastWoken = b.writes
			b.mu.Unlock()
			return
		case <-timer.C:
			t.Fatalf("no stderr write within %v (buffer %q)", timeout, b.inner.snapshot())
		}
	}
}

func startA2A337Serve(t *testing.T, cfgDir string, sec a2acore.SecretProvider) (exitCh chan int, cancel func(), out, errb *testLockedBuffer, wake *a2a337WakeErr) {
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
	// The serve's stderr is the test-owned writer that wakes the wait
	// on every write (no sleep to synchronize the test with the
	// serve's logs); it forwards to the shared locked buffer.
	wake = &a2a337WakeErr{inner: errb}
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    out,
		Stderr:    wake,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return serveFixedNow },
		Sleep:     sleepCtx,
	}
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
	return exitCh, c, out, errb, wake
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
	exitCh, cancel, out, errb, _ := startA2A337Serve(t, cfgDir, nil)
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
	// The gate's push logger classifies the SSRF guard's refusal (a
	// transport error): the fixed class, never the error text, which
	// carries the URL — the canary of the hook path never appears.
	if rec["error"] != "push endpoint unreachable" {
		t.Fatalf("record error = %v, want the fixed classification %q", rec["error"], "push endpoint unreachable")
	}
	stderr := errb.snapshot()
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
	exitCh, cancel, out, errb, wake := startA2A337Serve(t, cfgDir, sec)
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
		// The serve writes the suppression record to stderr after the
		// apply commit, so the stderr writer wakes this wait on the
		// write that carries the committed state: no sleep, the
		// deadline bounds the wait.
		wake.waitWoken(t, deadline)
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

// a2a337CheckFailedPushRecord asserts the stderr of one failed push
// send: exactly one JSON record — the sender's message at ERROR level
// with the fixed classification of the failure — and none of the
// banned values (the URL or the canary) anywhere in the stderr.
func a2a337CheckFailedPushRecord(t *testing.T, errb *testLockedBuffer, wantClass string, banned ...string) {
	t.Helper()
	line := errb.nextLine(t, 10*time.Second)
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("first stderr line %q is not JSON: %v", line, err)
	}
	if rec["msg"] != "push sending failed" || rec["level"] != "ERROR" {
		t.Fatalf("record = %v, want the ERROR record msg %q", rec, "push sending failed")
	}
	if rec["error"] != wantClass {
		t.Fatalf("record error = %v, want the fixed classification %q", rec["error"], wantClass)
	}
	stderr := errb.snapshot()
	for _, banned := range banned {
		if strings.Contains(stderr, banned) {
			t.Fatalf("stderr carries %q (the error text must not reach the log): %s", banned, stderr)
		}
	}
	if n := strings.Count(stderr, "\n"); n != 1 {
		t.Fatalf("stderr has %d line(s), want exactly the one JSON record (%q)", n, stderr)
	}
}

// a2a337LiveSend posts one live send with the push config URL to the
// served message:send endpoint and returns the HTTP status: the SDK
// saves the config before the run and the working push passes the
// gate (the local operator's configs are always sent).
func a2a337LiveSend(t *testing.T, listen, webhook string) int {
	t.Helper()
	req := &a2a.SendMessageRequest{
		Message: &a2a.Message{
			ID:    "m-337c",
			Role:  a2a.MessageRoleUser,
			Parts: a2a.ContentParts{a2a.NewTextPart(a2a337bAnswer)},
			Metadata: map[string]any{
				a2acore.MetaProject: "org/example",
				a2acore.MetaJob:     "job-337c",
				a2acore.MetaSession: "s-337c",
				a2acore.MetaEpoch:   float64(1),
				a2acore.MetaSeq:     float64(1),
			},
		},
		Config: &a2a.SendMessageConfig{
			ReturnImmediately: true,
			PushConfig:        &a2a.PushConfig{URL: webhook},
		},
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal send request: %v", err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post("http://"+listen+"/message:send", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("message:send: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// TestA2AServeInvalidPushURLOutOfBand proves the round-3 out-of-band
// path: the push config URL is invalid (the malformed form with the
// canary after the quote, and the schemeless form with the canary in
// the path), so the SDK sender fails to create the request; the serve
// applies the pending decision out of band and the failure is logged
// on stderr with only the fixed classification invalid push URL —
// never the URL or the canary.
func TestA2AServeInvalidPushURLOutOfBand(t *testing.T) {
	for name, webhook := range map[string]string{
		"malformed":  a2a337cMalformedURL,
		"schemeless": a2a337cSchemeless,
	} {
		t.Run(name, func(t *testing.T) {
			cfgDir := t.TempDir()
			a2aRegisterNotify(t, cfgDir)
			db := a2a337bFixture(t, cfgDir)
			// Re-save the same config with the invalid URL: the store
			// accepts any non-empty URL.
			s, err := a2astore.OpenFile(db, a2astore.Options{})
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			if _, err := s.PushConfigs().Save(context.Background(), a2a337bTask, &a2a.PushConfig{
				ID:  a2a337bConfig,
				URL: webhook,
			}); err != nil {
				t.Fatalf("Save: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			exitCh, cancel, out, errb, _ := startA2A337Serve(t, cfgDir, nil)
			t.Cleanup(func() {
				cancel()
				select {
				case <-exitCh:
				case <-time.After(15 * time.Second):
				}
			})
			// The decision loop applies the decision out of band and
			// the final push fails at the request creation.
			a2a337CheckFailedPushRecord(t, errb, "invalid push URL", webhook, a2a337cCanary)
			stdout := out.snapshot()
			if n := strings.Count(stdout, "\n"); n != 1 {
				t.Fatalf("stdout has %d line(s), want exactly the serving line (%q)", n, stdout)
			}
			if !strings.Contains(stdout, `"status":"serving"`) {
				t.Fatalf("stdout = %q, want the serving line", stdout)
			}
		})
	}
}

// TestA2AServeInvalidPushURLLive proves the round-3 live path: a live
// send carries a push config with the invalid URL (the malformed form
// with the canary after the quote, and the schemeless form with the
// canary in the path); the SDK saves the config before the run, the
// working push fails at the request creation and is logged on stderr
// with only the fixed classification invalid push URL — never the URL
// or the canary — while the send still answers HTTP 200 (a push
// failure does not fail the send).
func TestA2AServeInvalidPushURLLive(t *testing.T) {
	for name, webhook := range map[string]string{
		"malformed":  a2a337cMalformedURL,
		"schemeless": a2a337cSchemeless,
	} {
		t.Run(name, func(t *testing.T) {
			cfgDir := t.TempDir()
			a2aRegisterNotify(t, cfgDir)
			exitCh, cancel, out, errb, _ := startA2A337Serve(t, cfgDir, nil)
			t.Cleanup(func() {
				cancel()
				select {
				case <-exitCh:
				case <-time.After(15 * time.Second):
				}
			})
			var serving struct {
				Listen string `json:"listen"`
			}
			if err := json.Unmarshal([]byte(strings.TrimSpace(out.snapshot())), &serving); err != nil {
				t.Fatalf("serving line %q is not JSON: %v", out.snapshot(), err)
			}
			// The live send with the invalid push config URL: the
			// working push fails at the request creation.
			if status := a2a337LiveSend(t, serving.Listen, webhook); status != http.StatusOK {
				t.Fatalf("message:send answered HTTP %d, want 200 (a push failure does not fail the send)", status)
			}
			a2a337CheckFailedPushRecord(t, errb, "invalid push URL", webhook, a2a337cCanary)
			stdout := out.snapshot()
			if n := strings.Count(stdout, "\n"); n != 1 {
				t.Fatalf("stdout has %d line(s), want exactly the serving line (%q)", n, stdout)
			}
			if !strings.Contains(stdout, `"status":"serving"`) {
				t.Fatalf("stdout = %q, want the serving line", stdout)
			}
		})
	}
}
