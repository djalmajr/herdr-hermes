package cli

// The a2a command group tests (DJA-283): the usage errors per
// subcommand, the NOWRITE refusals of the writing subcommands, the
// loopback and --server refusals, the local subcommands over a missing
// or empty database, and the end-to-end flow: the serve prints one
// serving line, the client subcommands drive it over 127.0.0.1, the
// inbox and show read the database, the reply records a decision the
// running serve applies, and the delivery loop sends one
// identifiers-only notice. The tests run cmdA2A directly: the command
// table line is added by the orchestrator at integration. No real
// herdr-soho is involved: the notice sender is an in-memory fake.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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

// a2aTestEnv builds the hermetic Env for an a2a invocation over the
// locked writers (the serve writes from its loop goroutines).
func a2aTestEnv(t *testing.T, cfgDir string, out, errb *testLockedBuffer) Env {
	t.Helper()
	return Env{
		Stdin:     strings.NewReader(""),
		Stdout:    out,
		Stderr:    errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return serveFixedNow },
		Sleep:     sleepCtx,
	}
}

// runA2A invokes cmdA2A hermetically and returns stdout, stderr and the
// exit code.
func runA2A(t *testing.T, cfgDir string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return serveFixedNow },
		Sleep:     sleepCtx,
	}
	exit := cmdA2A(args, env)
	return out.String(), errb.String(), exit
}

// a2aRegisterNotify registers the notify owner of the fixture project
// (and the coordinator) in the state dir of cfgDir: the serve route
// source re-reads it on every call.
func a2aRegisterNotify(t *testing.T, cfgDir string) {
	t.Helper()
	store, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{Now: func() time.Time { return serveFixedNow }})
	if err != nil {
		t.Fatalf("outbox.Open: %v", err)
	}
	nstore := notify.Open(store, func() time.Time { return serveFixedNow })
	if err := nstore.UpdateRegistry(func(reg *notify.Registry) error {
		reg.Owners["org/example"] = "owner-a"
		reg.Coordinator = "coord-a"
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
}

// fakeA2ASender is the in-memory notify.Sender of the delivery loop
// tests: it records every (ref, message) call and answers the scripted
// outcomes of a ref in order (the last one repeats); the default
// outcome is accepted.
type fakeA2ASender struct {
	mu     sync.Mutex
	calls  []a2aSenderCall
	script map[string][]notify.SendResult
}

// a2aSenderCall is one recorded send attempt.
type a2aSenderCall struct {
	Ref     string
	Message string
}

// Send implements notify.Sender.
func (f *fakeA2ASender) Send(_ context.Context, ref, message string) notify.SendResult {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, a2aSenderCall{Ref: ref, Message: message})
	var out notify.SendResult
	if s := f.script[ref]; len(s) > 0 {
		if len(s) > 1 {
			f.script[ref] = s[1:]
		}
		out = s[0]
	} else {
		out = notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}
	}
	return out
}

// count reports the number of recorded send attempts.
func (f *fakeA2ASender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// calls returns a copy of the recorded send attempts.
func (f *fakeA2ASender) callsCopy() []a2aSenderCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]a2aSenderCall(nil), f.calls...)
}

// startA2AServe launches a2a serve in a goroutine with the test-owned
// context, the fake sender and fast loop intervals, and returns the
// base url once the serving line is printed.
func startA2AServe(t *testing.T, cfgDir string, args ...string) (base string, exitCh chan int, cancel func(), out *testLockedBuffer, sender *fakeA2ASender) {
	t.Helper()
	out = &testLockedBuffer{}
	errb := &testLockedBuffer{}
	sender = &fakeA2ASender{script: map[string][]notify.SendResult{}}
	ctx, c := context.WithCancel(context.Background())
	origCtx := a2aServeContext
	a2aServeContext = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	origSender := newA2aNoticeSender
	newA2aNoticeSender = func(string, []string) notify.Sender { return sender }
	origDeliver := a2aDefaultDeliverInterval
	a2aDefaultDeliverInterval = 50 * time.Millisecond
	origDecision := a2aDecisionInterval
	a2aDecisionInterval = 50 * time.Millisecond
	t.Cleanup(func() {
		a2aServeContext = origCtx
		newA2aNoticeSender = origSender
		a2aDefaultDeliverInterval = origDeliver
		a2aDecisionInterval = origDecision
		c()
	})
	exitCh = make(chan int, 1)
	env := a2aTestEnv(t, cfgDir, out, errb)
	go func() {
		exitCh <- cmdA2A(append([]string{"serve"}, args...), env)
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
	return "http://" + start.Listen, exitCh, c, out, sender
}

// a2aOneJSONLine asserts the output is exactly one line of valid JSON
// and returns the line.
func a2aOneJSONLine(t *testing.T, label, stdout string) string {
	t.Helper()
	if n := strings.Count(stdout, "\n"); n != 1 || !strings.HasSuffix(stdout, "\n") {
		t.Fatalf("%s printed %d line(s), want exactly one (%q)", label, n, truncateForLog(stdout))
	}
	line := stdout[:len(stdout)-1]
	if !json.Valid([]byte(line)) {
		t.Fatalf("%s line is not JSON: %q", label, truncateForLog(line))
	}
	return line
}

// a2aPollGet polls `a2a get` with a bounded deadline until the task
// reaches the wanted state and returns the task.
func a2aPollGet(t *testing.T, cfgDir, base, taskID string, wantState a2a.TaskState) *a2a.Task {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var stdout, stderr string
	var exit int
	for {
		stdout, stderr, exit = runA2A(t, cfgDir, "get", "--server", base, "--task", taskID)
		if exit == 0 {
			var body struct {
				Status string    `json:"status"`
				Task   *a2a.Task `json:"task"`
			}
			if json.Unmarshal([]byte(stdout), &body) == nil && body.Task != nil && body.Task.Status.State == wantState {
				return body.Task
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("task %s never reached %s (exit %d stdout %q stderr %q)", taskID, wantState, exit, truncateForLog(stdout), truncateForLog(stderr))
		}
		select {
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestA2AUsage: every malformed a2a form exits 2 before any side effect
// (no listener, no store, no dial) and prints the usage text on stderr.
func TestA2AUsage(t *testing.T) {
	const goodServer = "http://127.0.0.1:7430"
	cases := []struct {
		name   string
		args   []string
		motivo string
	}{
		{"no subcommand", []string{}, "usage: a2a serve|card|send|get|cancel|inbox|show|reply"},
		{"unknown subcommand", []string{"bogus"}, "unknown a2a subcommand"},
		// serve
		{"serve equals form", []string{"serve", "--listen=127.0.0.1:0"}, "usage: a2a serve"},
		{"serve missing value", []string{"serve", "--listen"}, "usage: a2a serve"},
		{"serve repeated listen", []string{"serve", "--listen", "127.0.0.1:1", "--listen", "127.0.0.1:2"}, "usage: a2a serve"},
		{"serve repeated db", []string{"serve", "--db", "a", "--db", "b"}, "usage: a2a serve"},
		{"serve unknown flag", []string{"serve", "--bogus"}, "usage: a2a serve"},
		{"serve positional", []string{"serve", "extra"}, "usage: a2a serve"},
		{"serve trailing positional", []string{"serve", "--listen", "127.0.0.1:0", "extra"}, "usage: a2a serve"},
		{"serve non-loopback listen", []string{"serve", "--listen", "0.0.0.0:7430"}, a2aLoopbackMotivo},
		{"serve hostname listen", []string{"serve", "--listen", "example.com:80"}, a2aLoopbackMotivo},
		{"serve empty listen", []string{"serve", "--listen", ":7430"}, a2aLoopbackMotivo},
		{"serve interval below range", []string{"serve", "--deliver-interval", "4"}, "a2a serve: --deliver-interval"},
		{"serve interval above range", []string{"serve", "--deliver-interval", "3601"}, "a2a serve: --deliver-interval"},
		{"serve interval not integer", []string{"serve", "--deliver-interval", "x"}, "a2a serve: --deliver-interval"},
		// card
		{"card no server", []string{"card"}, "a2a card: " + a2aServerMotivo},
		{"card non-loopback server", []string{"card", "--server", "http://example.com:80"}, a2aServerMotivo},
		{"card wrong scheme server", []string{"card", "--server", "ftp://127.0.0.1"}, a2aServerMotivo},
		{"card repeated server", []string{"card", "--server", goodServer, "--server", goodServer}, "usage: a2a card"},
		{"card equals form", []string{"card", "--server=" + goodServer}, "usage: a2a card"},
		{"card positional", []string{"card", "--server", goodServer, "extra"}, "usage: a2a card"},
		// send
		{"send no text", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1"}, "usage: a2a send"},
		{"send two positionals", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1", "a", "b"}, "usage: a2a send"},
		{"send missing session", []string{"send", "--server", goodServer, "--epoch", "1", "--seq", "1", "x"}, "a2a send: --session"},
		{"send missing epoch", []string{"send", "--server", goodServer, "--session", "s1", "--seq", "1", "x"}, "a2a send: --epoch"},
		{"send bad session", []string{"send", "--server", goodServer, "--session", "bad session", "--epoch", "1", "--seq", "1", "x"}, "a2a send: --session"},
		{"send epoch zero", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "0", "--seq", "1", "x"}, "a2a send: --epoch"},
		{"send seq not integer", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "x", "text"}, "a2a send: --seq"},
		{"send seq too large", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "99999999999999999999", "x"}, "a2a send: --seq"},
		{"send bad project", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1", "--project", "bad", "x"}, "a2a send: --project"},
		{"send bad job", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1", "--job", "bad job", "x"}, "a2a send: --job"},
		{"send task without context", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1", "--task", "t1", "x"}, "a2a send: --task and --context are given together"},
		{"send context without task", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1", "--context", "c1", "x"}, "a2a send: --task and --context are given together"},
		{"send bad task", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1", "--task", "bad task", "--context", "c1", "x"}, "a2a send: --task"},
		{"send bad message id", []string{"send", "--server", goodServer, "--session", "s1", "--epoch", "1", "--seq", "1", "--message-id", "bad id", "x"}, "a2a send: --message-id"},
		{"send non-loopback server", []string{"send", "--server", "http://example.com:80", "--session", "s1", "--epoch", "1", "--seq", "1", "x"}, a2aServerMotivo},
		{"send repeated session", []string{"send", "--server", goodServer, "--session", "s1", "--session", "s2", "--epoch", "1", "--seq", "1", "x"}, "usage: a2a send"},
		// get
		{"get no task", []string{"get", "--server", goodServer}, "a2a get: --task"},
		{"get bad task", []string{"get", "--server", goodServer, "--task", "bad task"}, "a2a get: --task"},
		{"get non-loopback server", []string{"get", "--server", "http://example.com:80", "--task", "t1"}, a2aServerMotivo},
		// cancel
		{"cancel no server", []string{"cancel", "--task", "t1"}, "a2a cancel: " + a2aServerMotivo},
		{"cancel no task", []string{"cancel", "--server", goodServer}, "a2a cancel: --task"},
		{"cancel wrong scheme server", []string{"cancel", "--server", "gopher://127.0.0.1", "--task", "t1"}, a2aServerMotivo},
		// inbox
		{"inbox unknown flag", []string{"inbox", "--bogus"}, "usage: a2a inbox"},
		{"inbox repeated state", []string{"inbox", "--state", "working", "--state", "working"}, "usage: a2a inbox"},
		{"inbox bad state", []string{"inbox", "--state", "bogus"}, "a2a inbox: --state"},
		{"inbox bad project", []string{"inbox", "--project", "bad"}, "a2a inbox: --project"},
		{"inbox positional", []string{"inbox", "extra"}, "usage: a2a inbox"},
		// show
		{"show no task", []string{"show"}, "a2a show: --task"},
		{"show bad task", []string{"show", "--task", "bad task"}, "a2a show: --task"},
		// reply
		{"reply no kind", []string{"reply", "--task", "t1", "x"}, "a2a reply: exactly one"},
		{"reply two kinds", []string{"reply", "--task", "t1", "--answer", "--ask", "x"}, "a2a reply: exactly one"},
		{"reply repeated kind", []string{"reply", "--task", "t1", "--answer", "--answer", "x"}, "usage: a2a reply"},
		{"reply no task", []string{"reply", "--answer", "x"}, "a2a reply: --task"},
		{"reply bad task", []string{"reply", "--task", "bad task", "--answer", "x"}, "a2a reply: --task"},
		{"reply bad decision id", []string{"reply", "--task", "t1", "--answer", "--decision-id", "bad id", "x"}, "a2a reply: --decision-id"},
		{"reply two positionals", []string{"reply", "--task", "t1", "--answer", "a", "b"}, "usage: a2a reply"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stdout, stderr, exit := runA2A(t, dir, tc.args...)
			if exit != 2 {
				t.Fatalf("a2a %v exit = %d, want 2 (stdout %q)", tc.args, exit, stdout)
			}
			if !strings.Contains(stdout, tc.motivo) {
				t.Errorf("stdout = %q, want %q", stdout, tc.motivo)
			}
			if strings.TrimSpace(stderr) == "" {
				t.Errorf("no diagnostic on stderr (got empty)")
			}
			// No side effect: the state dir was never created.
			assertDirEmpty(t, dir)
		})
	}
}

// TestA2ANowrite: serve and reply are writing subcommands and are
// refused under HERDR_HERMES_NOWRITE=1 before any side effect.
func TestA2ANowrite(t *testing.T) {
	for _, args := range [][]string{
		{"serve"},
		{"serve", "--listen", "127.0.0.1:7430"},
		{"reply"},
		{"reply", "--task", "t1", "--answer", "x"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			dir := t.TempDir()
			var out, errb bytes.Buffer
			env := Env{
				Stdin:     strings.NewReader(""),
				Stdout:    &out,
				Stderr:    &errb,
				Getenv:    getenvFor(true),
				Environ:   func() []string { return fakeChildEnv },
				ConfigDir: dir,
				Now:       func() time.Time { return serveFixedNow },
				Sleep:     sleepCtx,
			}
			exit := cmdA2A(args, env)
			if exit != 2 {
				t.Fatalf("a2a %v under NOWRITE exit = %d, want 2 (stdout %q)", args, exit, out.String())
			}
			if out.String() != nowriteErrorJSON+"\n" {
				t.Errorf("stdout = %q, want the nowrite line", out.String())
			}
			assertDirEmpty(t, dir)
		})
	}
}

// TestA2AServerURL: the accepted --server urls are the http:// loopback
// ones and the https:// ones.
func TestA2AServerURL(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"http://127.0.0.1:7430", true},
		{"http://localhost:7430", true},
		{"http://[::1]:7430", true},
		{"http://127.0.0.1", true},
		{"http://192.0.2.1:80", false},
		{"http://example.com:80", false},
		{"https://example.com:8443", true},
		{"https://127.0.0.1:8443", true},
		{"ftp://127.0.0.1", false},
		{"http://", false},
		{"not a url", false},
		{"", false},
	}
	for _, c := range cases {
		if got := a2aServerURLOK(c.raw); got != c.want {
			t.Errorf("a2aServerURLOK(%q) = %v, want %v", c.raw, got, c.want)
		}
	}
}

// TestA2ALocalWithoutDatabase: inbox, show and reply over a missing
// database print the empty or not-found result without creating the
// file; over an empty database they report the empty inbox and the
// unknown task.
func TestA2ALocalWithoutDatabase(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "missing", "registry.db")

	// inbox: the empty list, nothing created.
	stdout, _, exit := runA2A(t, dir, "inbox", "--db", db)
	if exit != 0 {
		t.Fatalf("inbox over missing db exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	if stdout != `{"status":"ok","items":[]}`+"\n" {
		t.Fatalf("inbox over missing db = %q, want the empty list", stdout)
	}
	if _, err := os.Stat(db); !os.IsNotExist(err) {
		t.Fatalf("inbox created the database file %s", db)
	}

	// show: not found, nothing created.
	stdout, _, exit = runA2A(t, dir, "show", "--task", "t1", "--db", db)
	if exit != 3 {
		t.Fatalf("show over missing db exit = %d, want 3 (stdout %q)", exit, stdout)
	}
	if !strings.Contains(stdout, `"status":"not_found"`) {
		t.Errorf("show over missing db = %q, want not_found", stdout)
	}
	if _, err := os.Stat(db); !os.IsNotExist(err) {
		t.Fatalf("show created the database file %s", db)
	}

	// reply: not found, nothing created.
	stdout, _, exit = runA2A(t, dir, "reply", "--task", "t1", "--answer", "x", "--db", db)
	if exit != 3 {
		t.Fatalf("reply over missing db exit = %d, want 3 (stdout %q)", exit, stdout)
	}
	if !strings.Contains(stdout, `"status":"not_found"`) {
		t.Errorf("reply over missing db = %q, want not_found", stdout)
	}
	if _, err := os.Stat(db); !os.IsNotExist(err) {
		t.Fatalf("reply created the database file %s", db)
	}

	// An empty database: the empty inbox, the unknown task and the
	// unknown-task reply refusal.
	store, err := a2astore.OpenFile(db, a2astore.Options{Now: func() time.Time { return serveFixedNow }})
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	_ = store.Close()
	stdout, _, exit = runA2A(t, dir, "inbox", "--db", db)
	if exit != 0 || stdout != `{"status":"ok","items":[]}`+"\n" {
		t.Fatalf("inbox over empty db = %d %q, want 0 and the empty list", exit, stdout)
	}
	stdout, _, exit = runA2A(t, dir, "show", "--task", "t1", "--db", db)
	if exit != 3 || !strings.Contains(stdout, `"status":"not_found"`) {
		t.Fatalf("show over empty db = %d %q, want 3 and not_found", exit, stdout)
	}
	stdout, _, exit = runA2A(t, dir, "reply", "--task", "t1", "--answer", "x", "--db", db)
	if exit != 3 || !strings.Contains(stdout, `"status":"not_found"`) {
		t.Fatalf("reply over empty db = %d %q, want 3 and not_found", exit, stdout)
	}
}

// TestA2AEndToEnd: serve prints one serving line; the client subcommands
// drive it over 127.0.0.1 (card, send, get, cancel); the inbox lists the
// tasks and the show prints the text as JSON data; the reply records a
// decision the running serve applies (the task completes); the duplicate
// reply and the unknown-task reply answer as specified; the delivery
// loop sends one identifiers-only notice through the fake sender; every
// stdout is exactly one JSON line.
func TestA2AEndToEnd(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	base, exitCh, cancel, out, sender := startA2AServe(t, cfgDir, "--listen", "127.0.0.1:0")
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	db := filepath.Join(outbox.StateDir(cfgDir), "registry.db")

	// card: the agent card over the served base url.
	stdout, _, exit := runA2A(t, cfgDir, "card", "--server", base)
	if exit != 0 {
		t.Fatalf("card exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line := a2aOneJSONLine(t, "card", stdout)
	var cardBody struct {
		Status string `json:"status"`
		Card   struct {
			Name                string `json:"name"`
			ProtocolVersion     string `json:"protocolVersion"`
			SupportedInterfaces []struct {
				URL             string `json:"url"`
				ProtocolBinding string `json:"protocolBinding"`
				ProtocolVersion string `json:"protocolVersion"`
			} `json:"supportedInterfaces"`
			Capabilities struct {
				Streaming         bool `json:"streaming"`
				PushNotifications bool `json:"pushNotifications"`
			} `json:"capabilities"`
		} `json:"card"`
	}
	if err := json.Unmarshal([]byte(line), &cardBody); err != nil {
		t.Fatalf("card body %q: %v", line, err)
	}
	if cardBody.Status != "ok" || cardBody.Card.Name != "Nau" {
		t.Fatalf("card = %q, want status ok and name Nau", line)
	}
	if len(cardBody.Card.SupportedInterfaces) != 1 {
		t.Fatalf("card supportedInterfaces = %v, want exactly one", cardBody.Card.SupportedInterfaces)
	}
	iface := cardBody.Card.SupportedInterfaces[0]
	if iface.ProtocolBinding != "HTTP+JSON" || iface.ProtocolVersion != string(a2a.Version) || iface.URL != base {
		t.Fatalf("card interface = %+v, want HTTP+JSON %s at %s", iface, a2a.Version, base)
	}
	if cardBody.Card.Capabilities.Streaming || !cardBody.Card.Capabilities.PushNotifications {
		t.Fatalf("card capabilities = %+v, want streaming off and push on", cardBody.Card.Capabilities)
	}

	// send: a new task for the project.
	stdout, _, exit = runA2A(t, cfgDir, "send", "--server", base,
		"--session", "s-e2e", "--epoch", "1", "--seq", "1",
		"--project", "org/example", "--message-id", "m-e2e-1",
		"status update: feature done")
	if exit != 0 {
		t.Fatalf("send exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "send", stdout)
	var sendBody struct {
		Status string    `json:"status"`
		Task   *a2a.Task `json:"task"`
	}
	if err := json.Unmarshal([]byte(line), &sendBody); err != nil || sendBody.Status != "ok" || sendBody.Task == nil || sendBody.Task.ID == "" {
		t.Fatalf("send body %q: %v, want status ok with a task", line, err)
	}
	task1 := string(sendBody.Task.ID)

	// get: the task reaches working (the executor parks awaiting the
	// owner) — polled with a bounded deadline.
	a2aPollGet(t, cfgDir, base, task1, a2a.TaskStateWorking)

	// inbox: the task is listed with its project and its delivery.
	stdout, _, exit = runA2A(t, cfgDir, "inbox", "--db", db)
	if exit != 0 {
		t.Fatalf("inbox exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "inbox", stdout)
	var inboxBody struct {
		Status string              `json:"status"`
		Items  []a2acore.InboxItem `json:"items"`
	}
	if err := json.Unmarshal([]byte(line), &inboxBody); err != nil || inboxBody.Status != "ok" {
		t.Fatalf("inbox body %q: %v, want status ok", line, err)
	}
	var found a2acore.InboxItem
	for _, it := range inboxBody.Items {
		if it.TaskID == task1 {
			found = it
		}
	}
	if found.TaskID == "" {
		t.Fatalf("inbox items = %+v, want the task %s", inboxBody.Items, task1)
	}
	if found.Project != "org/example" || found.Subject != "local" {
		t.Fatalf("inbox item = %+v, want project org/example and subject local", found)
	}
	if len(found.Deliveries) != 1 || found.Deliveries[0].Role != "owner" || found.Deliveries[0].Ref != "owner-a" {
		t.Fatalf("inbox deliveries = %+v, want one owner delivery to owner-a", found.Deliveries)
	}

	// show: the text is JSON data of the task history, plus the inbox
	// item and the (empty) decisions.
	const sentText = "status update: feature done"
	stdout, _, exit = runA2A(t, cfgDir, "show", "--task", task1, "--db", db)
	if exit != 0 {
		t.Fatalf("show exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "show", stdout)
	if !strings.Contains(line, sentText) {
		t.Fatalf("show output %q does not carry the message text as JSON data", truncateForLog(line))
	}
	var showBody struct {
		Status    string             `json:"status"`
		Task      *a2a.Task          `json:"task"`
		Item      *a2acore.InboxItem `json:"item"`
		Decisions []a2acore.Decision `json:"decisions"`
	}
	if err := json.Unmarshal([]byte(line), &showBody); err != nil || showBody.Status != "ok" || showBody.Task == nil {
		t.Fatalf("show body %q: %v, want status ok with the task", truncateForLog(line), err)
	}
	if len(showBody.Task.History) == 0 {
		t.Fatalf("show task has no history: %+v", showBody.Task)
	}
	last := showBody.Task.History[len(showBody.Task.History)-1]
	if last.Role != a2a.MessageRoleUser || len(last.Parts) != 1 || last.Parts[0].Text() != sentText {
		t.Fatalf("show history last = %+v, want the user message with the exact text", last)
	}
	if showBody.Item == nil || showBody.Item.TaskID != task1 {
		t.Fatalf("show item = %+v, want the inbox item of %s", showBody.Item, task1)
	}
	if showBody.Decisions == nil {
		t.Fatalf("show decisions is nil, want an empty list")
	}

	// reply: the decision is recorded (the task is NOT reported changed:
	// the line carries the decision only) and the running serve applies
	// it: the task reaches completed, the answer as an artifact.
	stdout, _, exit = runA2A(t, cfgDir, "reply", "--task", task1, "--answer",
		"--decision-id", "dec-e2e-1", "--db", db, "answer text e2e")
	if exit != 0 {
		t.Fatalf("reply exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "reply", stdout)
	var replyBody struct {
		Status   string            `json:"status"`
		Decision *a2acore.Decision `json:"decision"`
		Task     json.RawMessage   `json:"task"`
	}
	if err := json.Unmarshal([]byte(line), &replyBody); err != nil || replyBody.Status != "recorded" {
		t.Fatalf("reply body %q: %v, want status recorded", line, err)
	}
	if replyBody.Task != nil {
		t.Fatalf("reply line reports the task as changed: %q", line)
	}
	if replyBody.Decision == nil || replyBody.Decision.ID != "dec-e2e-1" || replyBody.Decision.State != a2acore.DecisionPending {
		t.Fatalf("reply decision = %+v, want dec-e2e-1 pending", replyBody.Decision)
	}

	done := a2aPollGet(t, cfgDir, base, task1, a2a.TaskStateCompleted)
	if len(done.Artifacts) == 0 {
		t.Fatalf("completed task has no artifact: %+v", done)
	}

	// duplicate reply with the same decision id and content.
	stdout, _, exit = runA2A(t, cfgDir, "reply", "--task", task1, "--answer",
		"--decision-id", "dec-e2e-1", "--db", db, "answer text e2e")
	if exit != 0 {
		t.Fatalf("duplicate reply exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "duplicate reply", stdout)
	if !strings.Contains(line, `"status":"duplicate"`) {
		t.Fatalf("duplicate reply = %q, want status duplicate", line)
	}

	// reply on an unknown task: exit 3.
	stdout, _, exit = runA2A(t, cfgDir, "reply", "--task", "no-such-task", "--answer", "x", "--db", db)
	if exit != 3 {
		t.Fatalf("reply unknown task exit = %d, want 3 (stdout %q)", exit, stdout)
	}
	if !strings.Contains(stdout, `"status":"not_found"`) {
		t.Errorf("reply unknown task = %q, want not_found", stdout)
	}

	// cancel: a second task is canceled by the caller.
	stdout, _, exit = runA2A(t, cfgDir, "send", "--server", base,
		"--session", "s-e2e-2", "--epoch", "1", "--seq", "1",
		"--project", "org/example", "--message-id", "m-e2e-2", "cancel me")
	if exit != 0 {
		t.Fatalf("second send exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "second send", stdout)
	if err := json.Unmarshal([]byte(line), &sendBody); err != nil || sendBody.Task == nil || sendBody.Task.ID == "" {
		t.Fatalf("second send body %q: %v, want a task", line, err)
	}
	task2 := string(sendBody.Task.ID)
	if task2 == task1 {
		t.Fatalf("second send returned the same task id %s", task1)
	}
	a2aPollGet(t, cfgDir, base, task2, a2a.TaskStateWorking)
	stdout, _, exit = runA2A(t, cfgDir, "cancel", "--server", base, "--task", task2)
	if exit != 0 {
		t.Fatalf("cancel exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "cancel", stdout)
	if err := json.Unmarshal([]byte(line), &sendBody); err != nil || sendBody.Task == nil {
		t.Fatalf("cancel body %q: %v, want the task", line, err)
	}
	if sendBody.Task.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("cancel task state = %s, want canceled", sendBody.Task.Status.State)
	}

	// inbox lists both tasks.
	stdout, _, exit = runA2A(t, cfgDir, "inbox", "--db", db)
	if exit != 0 {
		t.Fatalf("final inbox exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "final inbox", stdout)
	if err := json.Unmarshal([]byte(line), &inboxBody); err != nil {
		t.Fatalf("final inbox body %q: %v", line, err)
	}
	seen1, seen2 := false, false
	for _, it := range inboxBody.Items {
		if it.TaskID == task1 {
			seen1 = it.State == "completed"
		}
		if it.TaskID == task2 {
			seen2 = it.State == "canceled"
		}
	}
	if !seen1 || !seen2 {
		t.Fatalf("final inbox = %+v, want %s completed and %s canceled", inboxBody.Items, task1, task2)
	}

	// the delivery loop sent one identifiers-only notice: no message
	// text, no decision text, only validated identifiers, and it
	// names the task and the project.
	// the delivery loop (50 ms ticks in this test) sends one
	// identifiers-only notice for the first task; poll with a bounded
	// deadline so the test never races the loop's next round.
	noticeDeadline := time.Now().Add(5 * time.Second)
	for {
		if sender.count() > 0 {
			break
		}
		if time.Now().After(noticeDeadline) {
			t.Fatal("the delivery loop sent no notice within the deadline")
		}
		select {
		case <-time.After(25 * time.Millisecond):
		}
	}
	for i, call := range sender.callsCopy() {
		if call.Ref != "owner-a" {
			t.Fatalf("notice %d sent to ref %q, want owner-a", i, call.Ref)
		}
		if len(call.Message) > 2048 {
			t.Fatalf("notice %d is %d bytes, want at most 2048", i, len(call.Message))
		}
		for _, leak := range []string{sentText, "answer text e2e", "cancel me"} {
			if strings.Contains(call.Message, leak) {
				t.Fatalf("notice %d carries message text %q: %q", i, leak, truncateForLog(call.Message))
			}
		}
	}
	first := sender.callsCopy()[0]
	if !strings.Contains(first.Message, "task="+task1) || !strings.Contains(first.Message, "project=org/example") ||
		!strings.Contains(first.Message, "class=a2a_request") || !strings.Contains(first.Message, "role=owner") {
		t.Fatalf("first notice = %q, want the identifiers-only notice of %s", first.Message, task1)
	}

	// the running serve printed exactly one stdout line (the serving
	// line) over the whole run.
	if n := strings.Count(out.snapshot(), "\n"); n != 1 {
		t.Fatalf("serve printed %d stdout lines, want exactly one (%q)", n, out.snapshot())
	}
}

// TestA2AClientErrors: the client subcommands map the server errors to
// the fixed exit codes and lines: the SDK sentinels through
// a2aClientError and the transport failures through the dial of an
// unreachable loopback server.
func TestA2AClientErrors(t *testing.T) {
	refusedSentinels := []error{
		a2a.ErrUnauthenticated,
		a2a.ErrUnauthorized,
		a2a.ErrInvalidParams,
		a2a.ErrInvalidRequest,
		a2a.ErrParseError,
		a2a.ErrUnsupportedOperation,
		a2a.ErrTaskNotCancelable,
		a2a.ErrMethodNotFound,
	}
	for _, sentinel := range refusedSentinels {
		t.Run(sentinel.Error(), func(t *testing.T) {
			var out, errb bytes.Buffer
			env := Env{
				Stdin:     strings.NewReader(""),
				Stdout:    &out,
				Stderr:    &errb,
				Getenv:    getenvFor(false),
				Environ:   func() []string { return fakeChildEnv },
				ConfigDir: t.TempDir(),
				Now:       func() time.Time { return serveFixedNow },
				Sleep:     sleepCtx,
			}
			exit := a2aClientError(env, "get", fmt.Errorf("call: %w", sentinel))
			if exit != 2 {
				t.Fatalf("a2aClientError(%v) exit = %d, want 2 (stdout %q)", sentinel, exit, out.String())
			}
			if !strings.Contains(out.String(), `"status":"refused"`) {
				t.Errorf("a2aClientError(%v) = %q, want refused", sentinel, out.String())
			}
		})
	}
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: t.TempDir(),
		Now:       func() time.Time { return serveFixedNow },
		Sleep:     sleepCtx,
	}
	if exit := a2aClientError(env, "get", a2a.ErrTaskNotFound); exit != 3 || !strings.Contains(out.String(), `"status":"not_found"`) {
		t.Fatalf("a2aClientError(task not found) = %d %q, want 3 and not_found", exit, out.String())
	}
	out.Reset()
	var err error = fmt.Errorf("dial: %w", context.DeadlineExceeded)
	if exit := a2aClientError(env, "get", err); exit != 4 || !strings.Contains(out.String(), `"status":"unavailable"`) {
		t.Fatalf("a2aClientError(deadline) = %d %q, want 4 and unavailable", exit, out.String())
	}

	// The transport refusal: the base url is unreachable (no listener
	// on 127.0.0.1:1): the dial fails and the exit is unavailable.
	for _, args := range [][]string{
		{"get", "--server", "http://127.0.0.1:1", "--task", "t1"},
		{"card", "--server", "http://127.0.0.1:1"},
		{"send", "--server", "http://127.0.0.1:1", "--session", "s1", "--epoch", "1", "--seq", "1", "x"},
		{"cancel", "--server", "http://127.0.0.1:1", "--task", "t1"},
	} {
		stdout, _, exit := runA2A(t, t.TempDir(), args...)
		if exit != 4 {
			t.Fatalf("a2a %v unreachable exit = %d, want 4 (stdout %q)", args, exit, stdout)
		}
		if !strings.Contains(stdout, `"status":"unavailable"`) {
			t.Errorf("a2a %v unreachable = %q, want unavailable", args, stdout)
		}
	}
}

// TestA2AServeLoopbackGuard: the running serve refuses, with 403 and
// before any routing, a request whose Host or Origin is not loopback (a
// browser page or DNS rebinding must not reach the loopback local
// operator), and still serves a loopback request.
func TestA2AServeLoopbackGuard(t *testing.T) {
	cfgDir := t.TempDir()
	base, exitCh, cancel, _, _ := startA2AServe(t, cfgDir, "--listen", "127.0.0.1:0")
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	client := &http.Client{Timeout: 10 * time.Second}
	get := func(host, origin string) int {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, base+"/.well-known/agent-card.json", nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if host != "" {
			req.Host = host
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := get("", ""); code != http.StatusOK {
		t.Fatalf("loopback card = %d, want 200", code)
	}
	if code := get("rebind.example:7430", ""); code != http.StatusForbidden {
		t.Fatalf("non-loopback Host = %d, want 403", code)
	}
	if code := get("", "https://site.example"); code != http.StatusForbidden {
		t.Fatalf("cross-site Origin = %d, want 403", code)
	}
}
