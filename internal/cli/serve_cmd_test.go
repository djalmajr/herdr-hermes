package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/registry"
	"github.com/djalmajr/herdr-hermes/internal/router"
)

var serveFixedNow = time.Date(2026, 10, 10, 12, 0, 0, 0, time.FixedZone("x", -3*3600))

// testLockedBuffer is a mutex-protected byte buffer: serve writes to the
// test's env.Stderr from the poll goroutine and to the handler logger, so
// the test side of the stream must not race.
type testLockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
	off int
}

func (b *testLockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// snapshot returns the buffer content so far.
func (b *testLockedBuffer) snapshot() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// nextLine returns the next complete line, polling with a bounded
// deadline instead of sleeping on a fixed schedule.
func (b *testLockedBuffer) nextLine(t *testing.T, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		b.mu.Lock()
		data := b.buf.String()
		if i := strings.IndexByte(data[b.off:], '\n'); i >= 0 {
			line := data[b.off : b.off+i]
			b.off += i + 1
			b.mu.Unlock()
			return line
		}
		b.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("no complete line within %v (buffer %q)", timeout, data)
		}
		select {
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// fakeHerdr is an in-memory router.Exec for the serve poll loop: it
// records every argv and answers the poll fixtures per machine. Exec
// receives the argument list without the binary path.
type fakeHerdr struct {
	mu    sync.Mutex
	calls [][]string
}

func (f *fakeHerdr) Run(ctx context.Context, argv []string) ([]byte, int, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]string(nil), argv...))
	f.mu.Unlock()
	rest := argv
	label := "local"
	if len(argv) >= 2 && argv[0] == "--machine" {
		label, rest = argv[1], argv[2:]
	}
	switch {
	case len(rest) == 2 && rest[0] == "workspace" && rest[1] == "list":
		id, l := "w1", "hm-example-impl-x1"
		if label != "local" {
			id, l = "w2", "hm-example-impl-x2"
		}
		raw := fmt.Sprintf(`{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[{"active_tab_id":"%s:t1","agent_status":"working","focused":false,"label":%q,"number":1,"pane_count":2,"tab_count":1,"workspace_id":%q}]}}`, id, l, id)
		return []byte(raw), 0, nil
	case len(rest) == 2 && rest[0] == "agent" && rest[1] == "list":
		return []byte(`{"id":"cli:agent:list","result":{"agents":[]}}`), 0, nil
	case len(rest) == 2 && rest[0] == "status" && rest[1] == "server":
		return []byte("status: running\nversion: 0.9.1\nendpoint_compatible: yes\n"), 0, nil
	}
	return nil, 3, nil
}

// serveTestEnv builds the hermetic Env for a serve invocation.
func serveTestEnv(t *testing.T, cfgDir string, out, errb *testLockedBuffer) Env {
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

// runJobs invokes the jobs command hermetically.
func runJobs(t *testing.T, cfgDir string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(false),
		ConfigDir: cfgDir,
		Now:       func() time.Time { return serveFixedNow },
		Sleep:     sleepCtx,
	}
	exit := Run(append([]string{"jobs"}, args...), env)
	return out.String(), errb.String(), exit
}

// startServe launches serve in a goroutine with the test-owned context and
// fake herdr exec, and returns the startup line once printed.
func startServe(t *testing.T, cfgDir, db string, args ...string) (base string, exitCh chan int, cancel func(), out *testLockedBuffer, errb *testLockedBuffer) {
	t.Helper()
	out = &testLockedBuffer{}
	errb = &testLockedBuffer{}
	fake := &fakeHerdr{}
	origCtx := serveContext
	ctx, c := context.WithCancel(context.Background())
	serveContext = func() (context.Context, context.CancelFunc) { return ctx, func() {} }
	origExec := newServeExec
	newServeExec = func(bin string, environ []string) router.Exec { return fake.Run }
	t.Cleanup(func() {
		serveContext = origCtx
		newServeExec = origExec
		c()
	})
	exitCh = make(chan int, 1)
	env := serveTestEnv(t, cfgDir, out, errb)
	go func() {
		exitCh <- Run(append([]string{"serve", "--listen", "127.0.0.1:0", "--db", db}, args...), env)
	}()
	line := out.nextLine(t, 5*time.Second)
	var start struct {
		Status   string   `json:"status"`
		Listen   string   `json:"listen"`
		Maquinas []string `json:"maquinas"`
		PollS    int      `json:"poll_s"`
	}
	if err := json.Unmarshal([]byte(line), &start); err != nil {
		t.Fatalf("startup line %q is not JSON: %v", line, err)
	}
	if start.Status != "serving" || start.Listen == "" {
		t.Fatalf("startup line = %q, want status serving with a listen address", line)
	}
	return "http://" + start.Listen, exitCh, c, out, errb
}

// TestServeUsage: every malformed serve form exits 2 before any listener
// is opened and before any machine or config is contacted.
//
// Mutation captured: skipping the CheckLoopback gate lets the
// 0.0.0.0 listen case proceed to the store open and exit 4.
func TestServeUsage(t *testing.T) {
	var many []string
	for i := 1; i <= 33; i++ {
		many = append(many, fmt.Sprintf("m%d", i))
	}
	manyList := strings.Join(many, ",")
	cases := []struct {
		name   string
		args   []string
		motivo string
	}{
		{"equals form", []string{"--listen=127.0.0.1:7429"}, "usage: serve"},
		{"missing value", []string{"--listen"}, "usage: serve"},
		{"repeated listen", []string{"--listen", "127.0.0.1:1", "--listen", "127.0.0.1:2"}, "usage: serve"},
		{"repeated db", []string{"--db", "a", "--db", "b"}, "usage: serve"},
		{"repeated machines", []string{"--machines", "a", "--machines", "b"}, "usage: serve"},
		{"repeated poll", []string{"--poll-interval", "60", "--poll-interval", "60"}, "usage: serve"},
		{"unknown flag", []string{"--bogus"}, "usage: serve"},
		{"positional", []string{"extra"}, "usage: serve"},
		{"trailing positional", []string{"--listen", "127.0.0.1:1", "extra"}, "usage: serve"},
		{"non-loopback listen", []string{"--listen", "0.0.0.0:7429"}, "serve: --listen must be a loopback address"},
		{"host name listen", []string{"--listen", "example.com:80"}, "serve: --listen must be a loopback address"},
		{"empty listen", []string{"--listen", ":7429"}, "serve: --listen must be a loopback address"},
		{"poll below range", []string{"--poll-interval", "9"}, "serve: --poll-interval"},
		{"poll above range", []string{"--poll-interval", "3601"}, "serve: --poll-interval"},
		{"poll not integer", []string{"--poll-interval", "x"}, "serve: --poll-interval"},
		{"machines duplicate", []string{"--machines", "a,a"}, "serve: --machines"},
		{"machines bad label", []string{"--machines", "-x"}, "serve: --machines"},
		{"machines too many", []string{"--machines", manyList}, "serve: --machines"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stdout, stderr, exit := runCLI(t, dir, false, append([]string{"serve"}, tc.args...)...)
			if exit != 2 {
				t.Fatalf("serve %v exit = %d, want 2 (stdout %q)", tc.args, exit, stdout)
			}
			if !strings.Contains(stdout, tc.motivo) {
				t.Errorf("stdout = %q, want %q", stdout, tc.motivo)
			}
			if strings.TrimSpace(stderr) == "" {
				t.Errorf("no diagnostic on stderr (got empty)")
			}
		})
	}
}

// TestServeNowrite: serve is a writing command and is refused under
// HERDR_HERMES_NOWRITE before any side effect.
func TestServeNowrite(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, true, "serve", "--listen", "127.0.0.1:7429")
	if exit != 2 {
		t.Fatalf("serve under NOWRITE exit = %d, want 2 (stdout %q)", exit, stdout)
	}
	if stdout != `{"status":"nowrite","motivo":"HERDR_HERMES_NOWRITE=1"}`+"\n" {
		t.Errorf("stdout = %q", stdout)
	}
	assertDirEmpty(t, dir)
}

// TestListenerIsLoopback: the post-bind check is true only for loopback
// TCP listener addresses; a non-TCP address and a nil address fail it.
//
// Mutation captured: never checking the bound address (the post-Listen
// gate in cmdServe) lets a non-loopback bind through; this test fails
// to build when the helper is reverted with the fix.
func TestListenerIsLoopback(t *testing.T) {
	cases := []struct {
		addr net.Addr
		want bool
	}{
		{&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5}, true},
		{&net.TCPAddr{IP: net.ParseIP("::1"), Port: 5}, true},
		{&net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 5}, false},
		{&net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 5}, false},
		{&net.UnixAddr{Name: "/tmp/x"}, false},
		{nil, false},
	}
	for _, c := range cases {
		if got := listenerIsLoopback(c.addr); got != c.want {
			t.Errorf("listenerIsLoopback(%v) = %v, want %v", c.addr, got, c.want)
		}
	}
}

// TestServeEndToEnd: serve runs the inventory push, the poll and the HTTP
// API; the jobs client drives the full flow.
//
// Mutation captured: not starting the poll loop (len(machines) > 0 to
// false) leaves the inventory empty and the bounded deadline fails with
// "inventory never listed both workspaces".
func TestServeEndToEnd(t *testing.T) {
	dbDir := t.TempDir()
	db := filepath.Join(dbDir, "registry.db")
	cfgDir := t.TempDir()
	base, exitCh, cancel, out, _ := startServe(t, cfgDir, db, "--machines", "local,node-b", "--poll-interval", "10")

	// The poll applies before the inventory is asserted: poll the
	// inventory with a bounded deadline until both workspaces show up.
	deadline := time.Now().Add(5 * time.Second)
	var stdout, stderr string
	var exit int
	for {
		stdout, stderr, exit = runJobs(t, cfgDir, "--server", base)
		if exit == 0 {
			var inv registry.Inventory
			if json.Unmarshal([]byte(stdout), &inv) == nil &&
				invHasWorkspace(inv, "local", "hm-example-impl-x1") &&
				invHasWorkspace(inv, "node-b", "hm-example-impl-x2") {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("inventory never listed both workspaces (exit %d stdout %q stderr %q)", exit, stdout, stderr)
		}
		select {
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Check for an hm workspace that exists on node-b: 44, duplicate.
	stdout, stderr, exit = runJobs(t, cfgDir, "check", "--server", base, "--agente", "example", "--workspace", "hm-example-impl-x2")
	if exit != 44 {
		t.Fatalf("jobs check existing workspace exit = %d, want 44 (stdout %q)", exit, stdout)
	}
	var dup registry.CheckResult
	if err := json.Unmarshal([]byte(stdout), &dup); err != nil || dup.Status != registry.CheckDuplicate {
		t.Fatalf("jobs check duplicate body = %q (err %v)", stdout, err)
	}
	if !strings.Contains(stderr, "herdr-hermes: jobs check: refused (duplicate)") {
		t.Errorf("jobs check stderr = %q, want the refused (duplicate) line", stderr)
	}

	// A free check exits 0 with a claim; the same check again exits 44
	// with a claim conflict.
	stdout, _, exit = runJobs(t, cfgDir, "check", "--server", base, "--agente", "review", "--card", "DJA-9")
	if exit != 0 {
		t.Fatalf("jobs free check exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	var ok registry.CheckResult
	if err := json.Unmarshal([]byte(stdout), &ok); err != nil || ok.Status != registry.CheckOK || ok.Claim == nil || ok.Claim.ID == "" {
		t.Fatalf("jobs free check body = %q (err %v), want ok with a claim", stdout, err)
	}
	claimID := ok.Claim.ID
	stdout, _, exit = runJobs(t, cfgDir, "check", "--server", base, "--agente", "review", "--card", "DJA-9")
	if exit != 44 {
		t.Fatalf("jobs repeated check exit = %d, want 44 (stdout %q)", exit, stdout)
	}
	var claimDup registry.CheckResult
	if err := json.Unmarshal([]byte(stdout), &claimDup); err != nil || claimDup.Status != registry.CheckDuplicate {
		t.Fatalf("jobs repeated check body = %q (err %v), want duplicate", stdout, err)
	}
	foundClaim := false
	for _, c := range claimDup.Conflitos {
		if c.Tipo == "claim" {
			foundClaim = true
		}
	}
	if !foundClaim {
		t.Errorf("repeated check conflicts = %+v, want one with tipo claim", claimDup.Conflitos)
	}

	// Release exits 0; the second release exits 3.
	stdout, _, exit = runJobs(t, cfgDir, "release", "--server", base, "--claim", claimID)
	if exit != 0 || stdout != `{"status":"ok"}`+"\n" {
		t.Fatalf("jobs release exit = %d stdout = %q, want 0 and ok", exit, stdout)
	}
	stdout, _, exit = runJobs(t, cfgDir, "release", "--server", base, "--claim", claimID)
	if exit != 3 || stdout != `{"status":"not_found","motivo":"registry: not found"}`+"\n" {
		t.Fatalf("jobs second release exit = %d stdout = %q, want 3 and not_found", exit, stdout)
	}

	// Cancel the context: serve returns 0 and printed exactly one stdout
	// line (the startup line).
	cancel()
	select {
	case code := <-exitCh:
		if code != 0 {
			t.Fatalf("serve exit = %d, want 0 after cancel", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not return after the context cancel")
	}
	lines := strings.Count(out.snapshot(), "\n")
	if lines != 1 {
		t.Errorf("serve printed %d stdout lines, want exactly 1 (%q)", lines, out.snapshot())
	}
}

// TestServePushIngest: an outbox session record reaches serve's ingest
// endpoint through push, appears in the inventory, and the API key never
// leaves the credential store and the Authorization header.
//
// Mutation captured: making the empty --machines list a usage error makes
// serve refuse to start; the startup line is the usage JSON.
func TestServePushIngest(t *testing.T) {
	dbDir := t.TempDir()
	db := filepath.Join(dbDir, "registry.db")
	cfgDir := t.TempDir()
	const canary = "canary-key-9d2f"
	base, exitCh, cancel, out, errb := startServe(t, cfgDir, db, "--machines", "", "--poll-interval", "10")

	// Bridge configuration: machine label and the ingest endpoint.
	if err := config.Set(cfgDir, "machine_label", "node-a"); err != nil {
		t.Fatalf("config.Set machine_label: %v", err)
	}
	if err := config.Set(cfgDir, "dispatcher_url", base+"/v1/ingest"); err != nil {
		t.Fatalf("config.Set dispatcher_url: %v", err)
	}
	// The API key comes in through stdin only, with an explicit non-UTC
	// offset in the record timestamp.
	stdout, stderr, exit := runStdinFixedNow(t, cfgDir, canary+"\n", "auth", "login", "--key", "-")
	if exit != 0 {
		t.Fatalf("auth login exit = %d (stdout %q stderr %q)", exit, stdout, stderr)
	}
	stdout, stderr, exit = runStdinFixedNow(t, cfgDir, "", "session", "start", "--projeto", "example-org/example-repo", "--branch", "feat/x", "--card", "DJA-1")
	if exit != 0 || stdout != `{"seq":1}`+"\n" {
		t.Fatalf("session start exit = %d stdout = %q (stderr %q)", exit, stdout, stderr)
	}
	stdout, stderr, exit = runStdinFixedNow(t, cfgDir, "", "push")
	if exit != 0 || stdout != `{"enviados":1,"pendentes":0}`+"\n" {
		t.Fatalf("push exit = %d stdout = %q (stderr %q), want 0 with the record sent", exit, stdout, stderr)
	}
	stdout, stderr, exit = runJobs(t, cfgDir, "--server", base)
	if exit != 0 {
		t.Fatalf("jobs after push exit = %d (stdout %q stderr %q)", exit, stdout, stderr)
	}
	var inv registry.Inventory
	if err := json.Unmarshal([]byte(stdout), &inv); err != nil {
		t.Fatalf("inventory body %q: %v", stdout, err)
	}
	var foundSession bool
	for _, j := range inv.Jobs {
		if j.Chave == "sessao:example-org/example-repo#feat/x" && j.Maquina == "node-a" && j.Card != nil && *j.Card == "DJA-1" {
			foundSession = true
		}
	}
	if !foundSession {
		t.Fatalf("inventory = %+v, want the sessao: job with card DJA-1 from node-a", inv.Jobs)
	}

	cancel()
	select {
	case code := <-exitCh:
		if code != 0 {
			t.Fatalf("serve exit = %d, want 0 after cancel", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not return after the context cancel")
	}

	// The canary key never appears in the test output or in the database
	// files (it belongs to the credential store and the Authorization
	// header only).
	for _, data := range []string{out.snapshot(), errb.snapshot(), stdout, stderr} {
		if strings.Contains(data, canary) {
			t.Fatalf("the canary API key appeared in the test output: %q", truncateForLog(data))
		}
	}
	filepath.Walk(dbDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		if strings.Contains(string(data), canary) {
			t.Fatalf("the canary API key appeared in the database file %s", path)
		}
		return nil
	})
}

// runStdinFixedNow runs the CLI with the given stdin and a fixed
// non-UTC clock (the record timestamp carries an explicit offset).
func runStdinFixedNow(t *testing.T, dir, stdin string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(stdin),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: dir,
		Now:       func() time.Time { return serveFixedNow },
		Sleep:     sleepCtx,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

func invHasWorkspace(inv registry.Inventory, maquina, label string) bool {
	for _, j := range inv.Jobs {
		if j.Maquina == maquina && j.Workspace != nil && *j.Workspace == label {
			return true
		}
	}
	return false
}

func truncateForLog(s string) string {
	if len(s) > 120 {
		return s[:120] + "…"
	}
	return s
}

var _ io.Writer = (*testLockedBuffer)(nil)
