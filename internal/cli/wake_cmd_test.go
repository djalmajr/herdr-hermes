package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// runCLIStdin runs Run over a hermetic env with a custom stdin and an
// env-var map, using the fixed clock of the slice-1 tests.
func runCLIStdin(t *testing.T, dir, stdin string, vars map[string]string, args ...string) (string, string, int) {
	t.Helper()
	return runCLIStdinSleep(t, dir, stdin, vars, sleepCtx, args...)
}

// runCLIStdinSleep is runCLIStdin with an injected sleep, so the push
// backoff never uses real time.
func runCLIStdinSleep(t *testing.T, dir, stdin string, vars map[string]string, sleep func(context.Context, time.Duration) error, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:  strings.NewReader(stdin),
		Stdout: &out,
		Stderr: &errb,
		Getenv: func(k string) string {
			return vars[k]
		},
		ConfigDir: dir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleep,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// setConfig writes the given configuration keys.
func setConfig(t *testing.T, dir string, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		if err := config.Set(dir, k, v); err != nil {
			t.Fatalf("config.Set(%s): %v", k, err)
		}
	}
}

func wakeEvent(seq int) string {
	return fmt.Sprintf(`{"seq":%d,"ts":"2026-01-01T12:00:00-03:00","tipo":"question","resumo":"ship it?"}`, seq)
}

// outboxLines returns the complete lines of the outbox file.
func outboxLines(t *testing.T, dir string) []string {
	t.Helper()
	data, err := os.ReadFile(dir + "/state/outbox.jsonl")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read outbox: %v", err)
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

// cliPushServer is an httptest dispatcher that logs every request.
type cliPushServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []http.Request
	bodies   [][]byte
	status   int
}

func newCLIPushServer(t *testing.T, status int) *cliPushServer {
	t.Helper()
	s := &cliPushServer{status: status}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, *r)
		s.bodies = append(s.bodies, buf.Bytes())
		s.mu.Unlock()
		w.WriteHeader(s.status)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *cliPushServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

// TestWake covers criterion 5: an event on stdin plus the HERDR_SOHO_JOB_*
// env vars appends one job_event record; a repeat converges to one record;
// malformed input exits 2; a push failure or a missing key still exits 0
// with the record pending; an unknown job is tracked.
func TestWake(t *testing.T) {
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	vars := map[string]string{
		jobapi.EnvJobID:             "job-1",
		jobapi.EnvJobSeq:            "1",
		jobapi.EnvJobEvent:          `{"seq":1}`,
		jobapi.EnvJobIdempotencyKey: "job-1:1",
	}
	stdout, stderr, exit := runCLIStdin(t, dir, wakeEvent(1), vars, "wake")
	if exit != 0 {
		t.Fatalf("wake exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"seq":1,"duplicado":false,"enviados":0,"pendentes":1}
` {
		t.Fatalf("wake stdout = %q (no dispatcher_url: push disabled, record pending)", stdout)
	}
	lines := outboxLines(t, dir)
	if len(lines) != 1 || !strings.Contains(lines[0], `"tipo":"job_event"`) || !strings.Contains(lines[0], `"idempotency_key":"job-1:1"`) {
		t.Fatalf("outbox lines = %v", lines)
	}
	if !strings.Contains(lines[0], `"job_id":"job-1"`) {
		t.Fatalf("record = %q, want job_id job-1", lines[0])
	}
	// The same event again is deduplicated: no second record, seq 0.
	stdout, stderr, exit = runCLIStdin(t, dir, wakeEvent(1), vars, "wake")
	if exit != 0 {
		t.Fatalf("wake retry exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"seq":0,"duplicado":true,"enviados":0,"pendentes":1}
` {
		t.Fatalf("wake retry stdout = %q, want deduplicated with seq 0", stdout)
	}
	if n := len(outboxLines(t, dir)); n != 1 {
		t.Fatalf("outbox has %d lines after retry, want 1", n)
	}
	// An unknown job is tracked on the fly (empty projeto).
	vars2 := map[string]string{jobapi.EnvJobID: "job-9"}
	stdout, stderr, exit = runCLIStdin(t, dir, wakeEvent(3), vars2, "wake")
	if exit != 0 {
		t.Fatalf("wake unknown job exit = %d, stderr %q", exit, stderr)
	}
	if !strings.Contains(stdout, `"duplicado":false`) {
		t.Fatalf("wake unknown job stdout = %q", stdout)
	}
	data, err := os.ReadFile(dir + "/state/jobs.json")
	if err != nil {
		t.Fatal(err)
	}
	var jobs outbox.Jobs
	if err := json.Unmarshal(data, &jobs); err != nil {
		t.Fatal(err)
	}
	if j, ok := jobs.Jobs["job-9"]; !ok || j.LastEventSeq != 3 || j.Projeto != "" {
		t.Fatalf("jobs.json job-9 = %+v, want tracked with empty projeto", j)
	}
	if j, ok := jobs.Jobs["job-1"]; !ok || j.LastEventSeq != 1 {
		t.Fatalf("jobs.json job-1 = %+v, want last_event_seq 1", j)
	}

	// The projeto comes from the tracked job when there is one.
	store, err := outbox.Open(outbox.StateDir(dir), outbox.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateJobs(func(j *outbox.Jobs) error {
		j.Jobs["job-7"] = &outbox.Job{ID: "job-7", Projeto: "org/repo"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	vars7 := map[string]string{jobapi.EnvJobID: "job-7"}
	stdout, stderr, exit = runCLIStdin(t, dir, wakeEvent(2), vars7, "wake")
	if exit != 0 {
		t.Fatalf("wake tracked job exit = %d, stderr %q", exit, stderr)
	}
	last := outboxLines(t, dir)
	if !strings.Contains(last[len(last)-1], `"projeto":"org/repo"`) {
		t.Fatalf("record for tracked job = %q, want the projeto from jobs.json", last[len(last)-1])
	}

	// Malformed input exits 2 and writes nothing new.
	before := len(outboxLines(t, dir))
	cases := []struct {
		name  string
		stdin string
		vars  map[string]string
	}{
		{"not json", `{nope`, map[string]string{jobapi.EnvJobID: "job-1"}},
		{"empty stdin", ``, map[string]string{jobapi.EnvJobID: "job-1"}},
		{"missing id", wakeEvent(1), map[string]string{}},
		{"invalid id", wakeEvent(1), map[string]string{jobapi.EnvJobID: "bad id!"}},
		{"seq missing", `{"tipo":"question"}`, map[string]string{jobapi.EnvJobID: "job-1"}},
		{"seq zero", `{"seq":0}`, map[string]string{jobapi.EnvJobID: "job-1"}},
		{"seq negative", `{"seq":-2}`, map[string]string{jobapi.EnvJobID: "job-1"}},
		{"seq string", `{"seq":"1"}`, map[string]string{jobapi.EnvJobID: "job-1"}},
		{"env seq mismatch", wakeEvent(1), map[string]string{jobapi.EnvJobID: "job-1", jobapi.EnvJobSeq: "2"}},
		{"env seq not a number", wakeEvent(1), map[string]string{jobapi.EnvJobID: "job-1", jobapi.EnvJobSeq: "x"}},
		{"idempotency mismatch", wakeEvent(1), map[string]string{jobapi.EnvJobID: "job-1", jobapi.EnvJobIdempotencyKey: "job-1:9"}},
	}
	for _, tc := range cases {
		stdout, stderr, exit := runCLIStdin(t, dir, tc.stdin, tc.vars, "wake")
		if exit != 2 {
			t.Errorf("%s: exit = %d, want 2 (stdout %q)", tc.name, exit, stdout)
		}
		if !strings.Contains(stderr, "herdr-hermes") || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("%s: stderr %q stdout %q", tc.name, stderr, stdout)
		}
		if n := len(outboxLines(t, dir)); n != before {
			t.Errorf("%s: outbox grew to %d lines, want %d", tc.name, n, before)
		}
	}
	// Stdin over the 64 KiB cap exits 2; exactly at the cap is accepted.
	over := `{"seq":1,"pad":"` + strings.Repeat("a", jobapi.StdinCapAmend) + `"}`
	if _, _, exit := runCLIStdin(t, dir, over, map[string]string{jobapi.EnvJobID: "job-1"}, "wake"); exit != 2 {
		t.Error("wake with stdin over 64 KiB must exit 2")
	}
	exact := eventAtCap(jobapi.StdinCapAmend)
	stdout, stderr, exit = runCLIStdin(t, dir, exact, map[string]string{jobapi.EnvJobID: "job-1"}, "wake")
	if exit != 0 || !strings.Contains(stdout, `"duplicado":false`) {
		t.Errorf("wake at exactly 64 KiB: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	last = outboxLines(t, dir)
	if !strings.Contains(last[len(last)-1], `"seq":8`) {
		t.Errorf("event at the cap was not recorded: %q", last[len(last)-1])
	}
	// machine_label empty exits 2 with a clear message.
	dirEmpty := t.TempDir()
	stdout, stderr, exit = runCLIStdin(t, dirEmpty, wakeEvent(1), map[string]string{jobapi.EnvJobID: "job-1"}, "wake")
	if exit != 2 || !strings.Contains(stderr, "machine_label is empty") {
		t.Errorf("wake with empty machine_label: exit %d stderr %q", exit, stderr)
	}
}

// eventAtCap builds a valid event whose serialized form is exactly cap
// bytes.
func eventAtCap(cap int) string {
	const seq = 8
	base := fmt.Sprintf(`{"seq":%d,"pad":""}`, seq)
	pad := strings.Repeat("a", cap-len(base))
	return fmt.Sprintf(`{"seq":%d,"pad":"%s"}`, seq, pad)
}

// TestWakePushOutcome pins the exit-0-on-push-failure and missing-key
// rules, and that wake makes exactly one push attempt (no retries).
func TestWakePushOutcome(t *testing.T) {
	vars := map[string]string{jobapi.EnvJobID: "job-1"}
	// Missing key: still exit 0, record pending.
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1", "dispatcher_url": "http://127.0.0.1:1"})
	stdout, stderr, exit := runCLIStdin(t, dir, wakeEvent(1), vars, "wake")
	if exit != 0 {
		t.Fatalf("wake without key exit = %d, stderr %q, want 0", exit, stderr)
	}
	if !strings.Contains(stdout, `"duplicado":false`) || !strings.Contains(stdout, `"pendentes":1`) {
		t.Fatalf("wake without key stdout = %q, want the record pending", stdout)
	}
	// A push failure (500): still exit 0, record pending.
	srv := newCLIPushServer(t, 500)
	dir2 := t.TempDir()
	setConfig(t, dir2, map[string]string{"machine_label": "m1", "dispatcher_url": srv.URL})
	loginAssert(t, dir2)
	stdout, stderr, exit = runCLIStdin(t, dir2, wakeEvent(1), vars, "wake")
	if exit != 0 {
		t.Fatalf("wake with a failing push exit = %d, stderr %q, want 0", exit, stderr)
	}
	var got struct {
		Seq       int64 `json:"seq"`
		Duplicado bool  `json:"duplicado"`
		Enviados  int   `json:"enviados"`
		Pendentes int   `json:"pendentes"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil || got.Seq != 1 || got.Duplicado || got.Enviados != 0 || got.Pendentes != 1 {
		t.Fatalf("wake push failure stdout = %q (parsed %+v), want seq 1, 0 sent, 1 pending", stdout, got)
	}
	if n := srv.count(); n != 1 {
		t.Fatalf("wake made %d push attempts, want exactly 1 (no retries)", n)
	}
	// A second event for the same job keeps the seq consecutive.
	stdout, stderr, exit = runCLIStdin(t, dir2, wakeEvent(2), vars, "wake")
	if exit != 0 || !strings.Contains(stdout, `"seq":2`) {
		t.Fatalf("wake second event: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
}

func TestWakeNowrite(t *testing.T) {
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	var outb, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(wakeEvent(1)),
		Stdout:    &outb,
		Stderr:    &errb,
		Getenv:    func(k string) string { return "1" },
		ConfigDir: dir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	exit := Run([]string{"wake"}, env)
	if exit != 2 || outb.String() != "{\"status\":\"nowrite\",\"motivo\":\"HERDR_HERMES_NOWRITE=1\"}\n" {
		t.Fatalf("wake under NOWRITE: exit %d stdout %q", exit, outb.String())
	}
	if _, err := os.Stat(dir + "/state"); !os.IsNotExist(err) {
		t.Fatalf("wake under NOWRITE created the state dir")
	}
}

func TestWakePushTimeoutBound(t *testing.T) {
	// The wake push runs under push_timeout_s + 5 s.
	for in, want := range map[int]time.Duration{15: 20 * time.Second, 1: 6 * time.Second, 300: 305 * time.Second} {
		cfg := config.Config{PushTimeoutS: in}
		if got := wakePushTimeout(cfg); got != want {
			t.Errorf("wakePushTimeout(%d) = %v, want %v", in, got, want)
		}
	}
}
