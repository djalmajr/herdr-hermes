package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/config"
	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/registry"
	"github.com/djalmajr/nau/internal/router"
	_ "modernc.org/sqlite"
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

func (f *fakeHerdr) Run(ctx context.Context, argv []string) ([]byte, []byte, int, error) {
	stdout, code, err := f.answer(argv)
	return stdout, nil, code, err
}

func (f *fakeHerdr) answer(argv []string) ([]byte, int, error) {
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
		{"max-jobs not positive", []string{"--max-jobs", "0"}, "serve: --max-jobs must be a positive integer"},
		{"max-jobs not integer", []string{"--max-jobs", "x"}, "serve: --max-jobs must be a positive integer"},
		{"repeated max-jobs", []string{"--max-jobs", "5", "--max-jobs", "6"}, "usage: serve"},
		{"max-jobs missing value", []string{"--max-jobs"}, "usage: serve"},
		{"max-sessions not positive", []string{"--max-sessions", "0"}, "serve: --max-sessions must be a positive integer"},
		{"max-sessions not integer", []string{"--max-sessions", "x"}, "serve: --max-sessions must be a positive integer"},
		{"repeated max-sessions", []string{"--max-sessions", "5", "--max-sessions", "6"}, "usage: serve"},
		{"max-sessions missing value", []string{"--max-sessions"}, "usage: serve"},
		{"retention below range", []string{"--ended-retention-days", "6"}, "serve: --ended-retention-days must be an integer between 7 and 30"},
		{"retention above range", []string{"--ended-retention-days", "31"}, "serve: --ended-retention-days must be an integer between 7 and 30"},
		{"retention not integer", []string{"--ended-retention-days", "x"}, "serve: --ended-retention-days must be an integer between 7 and 30"},
		{"repeated retention", []string{"--ended-retention-days", "7", "--ended-retention-days", "8"}, "usage: serve"},
		{"retention missing value", []string{"--ended-retention-days"}, "usage: serve"},
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

// TestServeDeprecatedCountFlags: the deprecated --max-jobs and
// --max-sessions flags stay accepted for who already passes them: any
// positive integer is ignored with one deprecation line each on stderr,
// and the registry has no jobs or sessions count limit, so the
// end-first sessions that the old budget of one refused are all
// applied.
func TestServeDeprecatedCountFlags(t *testing.T) {
	dbDir := t.TempDir()
	db := filepath.Join(dbDir, "registry.db")
	cfgDir := t.TempDir()
	base, exitCh, cancel, _, errb := startServe(t, cfgDir, db, "--machines", "", "--max-jobs", "5000", "--max-sessions", "5000")
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	// One deprecation line per flag, on stderr, with the flag name and
	// the kind, and the stdout keeps its single serving line.
	snapshot := func() string {
		return errb.snapshot()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		snap := snapshot()
		if strings.Contains(snap, "serve: --max-jobs is deprecated and ignored: the registry has no job count limit") &&
			strings.Contains(snap, "serve: --max-sessions is deprecated and ignored: the registry has no session count limit") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deprecation lines not printed (stderr %q)", snap)
		}
		select {
		case <-time.After(10 * time.Millisecond):
		}
	}
	if n := strings.Count(snapshot(), "--max-jobs is deprecated and ignored"); n != 1 {
		t.Errorf("max-jobs deprecation lines = %d, want exactly one (%q)", n, snapshot())
	}
	if n := strings.Count(snapshot(), "--max-sessions is deprecated and ignored"); n != 1 {
		t.Errorf("max-sessions deprecation lines = %d, want exactly one (%q)", n, snapshot())
	}

	// No count cap: the second end-first session, refused at the old
	// budget of one, is applied like the first.
	rec := func(seq int, branch string) string {
		return fmt.Sprintf(`{"schema":1,"seq":%d,"ts":"2026-10-10T11:%02d:00-03:00","tipo":"session","maquina":"node-a","projeto":"org/x","idempotency_key":"node-a:%d","dados":{"acao":"end","projeto":"org/x","branch":"%s"}}`, seq, seq, seq, branch)
	}
	post := func(body string) (int, string) {
		t.Helper()
		resp, err := http.Post(base+"/v1/ingest", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST /v1/ingest: %v", err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read the answer: %v", err)
		}
		return resp.StatusCode, string(data)
	}
	for _, branch := range []string{"feat/a", "feat/b"} {
		if code, body := post(rec(1, branch)); code != 200 || body != `{"status":"ok","aplicado":true}`+"\n" {
			t.Fatalf("end-first session %s = %d %q, want 200 ok/applied (no count cap)", branch, code, body)
		}
	}
}

// TestServePollRefusalKeepsOtherMachines: one machine's poll is refused
// whole (1001 blocked panes past the stored waits bound) while the other
// machine's snapshot is valid: the valid machine is applied, the refused
// machine keeps its previous state (its seed workspace row and its old
// wait, its last poll unchanged), the log line for the refused machine
// carries the waits refusal, and the loop keeps running (the second
// round refuses it again) until its context ends.
//
// Mutation captured: applying the refused machine's snapshot partially
// (or stopping the loop on a refusal) drops the seed workspace row or
// never logs the second round; both fail here.
func TestServePollRefusalKeepsOtherMachines(t *testing.T) {
	db := filepath.Join(t.TempDir(), "registry.db")
	store, err := registry.Open(db, registry.Options{Now: func() time.Time { return serveFixedNow }})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	const refused, valid = "node-r", "node-v"
	seedTS := serveFixedNow.Format("2006-01-02T15:04:05-07:00")
	// The previous state of the refused machine: one valid poll with its
	// workspace row and one blocked pane.
	if err := store.ApplyPoll(registry.PollResult{Maquina: refused, At: serveFixedNow, OK: true,
		Workspaces: []registry.PolledWorkspace{{ID: "w0", Label: "hm-example-impl-x0", AgentStatus: "working"}},
		Panes:      []registry.PolledPane{{PaneID: "p0", WorkspaceID: "w0", Name: "example", AgentStatus: "blocked"}},
	}); err != nil {
		t.Fatalf("seed ApplyPoll: %v", err)
	}

	// The fake herdr: the refused machine answers the agent list with
	// 1001 blocked panes every round; the valid machine answers one
	// workspace and no panes.
	fake := func(ctx context.Context, argv []string) ([]byte, []byte, int, error) {
		label, rest := "local", argv
		if len(argv) >= 2 && argv[0] == "--machine" {
			label, rest = argv[1], argv[2:]
		}
		switch {
		case len(rest) == 2 && rest[0] == "workspace" && rest[1] == "list":
			if label == valid {
				return []byte(`{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[{"agent_status":"working","label":"hm-example-impl-x1","number":1,"workspace_id":"w1"}]}}`), nil, 0, nil
			}
			return []byte(`{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[]}}`), nil, 0, nil
		case len(rest) == 2 && rest[0] == "agent" && rest[1] == "list":
			if label == refused {
				var b strings.Builder
				b.WriteString(`{"id":"cli:agent:list","result":{"agents":[`)
				for i := 0; i < 1001; i++ {
					if i > 0 {
						b.WriteByte(',')
					}
					fmt.Fprintf(&b, `{"pane_id":"p%04d","agent_status":"blocked","name":"x","workspace_id":"w0"}`, i)
				}
				b.WriteString(`]}}`)
				return []byte(b.String()), nil, 0, nil
			}
			return []byte(`{"id":"cli:agent:list","result":{"agents":[]}}`), nil, 0, nil
		case len(rest) == 2 && rest[0] == "status" && rest[1] == "server":
			return []byte("status: running\nversion: 0.9.1\nendpoint_compatible: yes\n"), nil, 0, nil
		}
		return nil, nil, 3, nil
	}

	logb := &testLockedBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	poller := registry.Poller{Exec: fake, Timeout: 5 * time.Second, Now: func() time.Time { return serveFixedNow }}
	done := make(chan struct{})
	go func() {
		servePollLoop(ctx, logb, ProgramNau, store, []string{refused, valid}, time.Second, poller)
		close(done)
	}()

	// Two rounds at the one-second interval: every round the refused
	// machine logs the waits refusal, so two lines prove the loop kept
	// running past a refusal.
	for i := 0; i < 2; i++ {
		line := logb.nextLine(t, 15*time.Second)
		if !strings.Contains(line, "poll "+refused+": registry: invalid input: waits limit reached") {
			t.Fatalf("round %d log line = %q, want the waits refusal for %s", i+1, line, refused)
		}
	}

	inv, err := store.Inventory(registry.Filter{})
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	has := func(maquina, label string) bool {
		for i := range inv.Jobs {
			if inv.Jobs[i].Maquina == maquina && inv.Jobs[i].Workspace != nil && *inv.Jobs[i].Workspace == label {
				return true
			}
		}
		return false
	}
	if !has(valid, "hm-example-impl-x1") {
		t.Errorf("the valid machine was not applied: jobs = %+v", inv.Jobs)
	}
	if !has(refused, "hm-example-impl-x0") {
		t.Errorf("the refused machine lost its seed workspace row: jobs = %+v", inv.Jobs)
	}
	if len(inv.Esperas) != 1 || inv.Esperas[0].Maquina != refused || inv.Esperas[0].Pane == nil || *inv.Esperas[0].Pane != "p0" {
		t.Errorf("esperas = %+v, want the seed wait of the refused machine only", inv.Esperas)
	}
	for i := range inv.Nodes {
		if inv.Nodes[i].Maquina == refused {
			if inv.Nodes[i].UltimoPoll == nil || *inv.Nodes[i].UltimoPoll != seedTS {
				t.Errorf("refused node ultimo_poll = %v, want the seed poll time %q", inv.Nodes[i].UltimoPoll, seedTS)
			}
			if inv.Nodes[i].PollStatus == nil || *inv.Nodes[i].PollStatus != "ok" {
				t.Errorf("refused node poll_status = %v, want the seed status ok", inv.Nodes[i].PollStatus)
			}
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the poll loop did not stop after the context cancel")
	}
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

// dja275ServeDBConn opens the registry database with a plain connection,
// for the seed and the fault of the sweep tests (serve has not opened
// it yet, and the tests are in a package that cannot reach the store's
// connection).
func dja275ServeDBConn(t *testing.T, db string) *sql.DB {
	t.Helper()
	segments := strings.Split(filepath.ToSlash(db), "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	conn, err := sql.Open("sqlite", "file:"+strings.Join(segments, "/")+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("open the database: %v", err)
	}
	return conn
}

// dja275ServeSeedDB seeds the sweep fixture rows in the serve database
// before serve opens it: one ended row past the 30-day retention (with
// its event), one ended exactly at it, one ended six days ago and one
// active row.
func dja275ServeSeedDB(t *testing.T, db string) {
	t.Helper()
	// The registry open creates the schema; the rows go in through a
	// plain connection, as the tests are in a package that cannot
	// reach the store's connection.
	seed, err := registry.Open(db, registry.Options{Now: func() time.Time { return serveFixedNow }})
	if err != nil {
		t.Fatalf("seed Open: %v", err)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed Close: %v", err)
	}
	conn := dja275ServeDBConn(t, db)
	defer conn.Close()
	now := serveFixedNow.Unix()
	r := func(chave string, encerrado int, eu int64) {
		t.Helper()
		var euv interface{}
		if encerrado == 1 {
			euv = eu
		}
		if _, err := conn.Exec(`INSERT INTO jobs(maquina, chave, origem, job_id, encerrado, encerrado_unix, ultima_atividade, ultima_atividade_unix)
			VALUES('node-a', ?, 'job', ?, ?, ?, ?, ?)`, chave, chave, encerrado, euv, serveFixedNow.Format(outbox.TSLayout), now); err != nil {
			t.Fatalf("seed %s: %v", chave, err)
		}
		if _, err := conn.Exec(`INSERT INTO events(maquina, chave, seq, ts, tipo, resumo)
			VALUES('node-a', ?, 1, '2026-01-01T00:00:00-03:00', 'note', 'x')`, chave); err != nil {
			t.Fatalf("seed the event of %s: %v", chave, err)
		}
	}
	r("j-gone", 1, now-30*24*3600-1)
	r("j-border", 1, now-30*24*3600)
	r("j-recent", 1, now-6*24*3600)
	r("j-active", 0, 0)
}

// dja275ServeHealthCounts reads the health counts of the served registry.
func dja275ServeHealthCounts(t *testing.T, base string) (jobs, sessions map[string]int) {
	t.Helper()
	resp, err := http.Get(base + "/v1/health")
	if err != nil {
		t.Fatalf("GET /v1/health: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the health answer: %v", err)
	}
	var body struct {
		Status string `json:"status"`
		Counts struct {
			Jobs     map[string]int `json:"jobs"`
			Sessions map[string]int `json:"sessions"`
		} `json:"counts"`
	}
	if err := json.Unmarshal(data, &body); err != nil || body.Status != "ok" {
		t.Fatalf("health body %q: %v", data, err)
	}
	return body.Counts.Jobs, body.Counts.Sessions
}

// TestServeSweepLoop: with the injected interval, the sweep runs with no
// new request: it deletes the row ended 30 days and 1 second ago (with
// its event) and keeps the row ended exactly 30 days ago, the row ended
// six days ago and the active row; the loop stops with the serve
// context, and serve returns 0.
func TestServeSweepLoop(t *testing.T) {
	dbDir := t.TempDir()
	db := filepath.Join(dbDir, "registry.db")
	cfgDir := t.TempDir()
	dja275ServeSeedDB(t, db)
	serveSweepInterval = 500 * time.Millisecond
	var exited bool
	base, exitCh, cancel, _, _ := startServe(t, cfgDir, db, "--machines", "")
	t.Cleanup(func() {
		serveSweepInterval = 24 * time.Hour
		cancel()
		if !exited {
			select {
			case <-exitCh:
			case <-time.After(15 * time.Second):
			}
		}
	})
	// No ingest or poll: the sweep alone moves the state. Poll the
	// health counts with a bounded deadline until the sweep has run.
	deadline := time.Now().Add(10 * time.Second)
	var jobs, sessions map[string]int
	for {
		jobs, sessions = dja275ServeHealthCounts(t, base)
		if jobs["total"] == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sweep never ran: jobs counts = %v (want total 3)", jobs)
		}
		select {
		case <-time.After(50 * time.Millisecond):
		}
	}
	// j-gone is gone; j-border, j-recent and j-active are kept: total 3,
	// active 1, ended 2, protected ended 1 (the 6-day row inside the
	// 7-day fence; the 30-day row is outside it).
	for k, want := range map[string]int{"total": 3, "active": 1, "ended": 2, "protected_ended": 1} {
		if jobs[k] != want {
			t.Errorf("jobs counts[%s] = %d, want %d (full: %v)", k, jobs[k], want, jobs)
		}
	}
	for k, want := range map[string]int{"total": 0, "active": 0, "ended": 0, "protected_ended": 0} {
		if sessions[k] != want {
			t.Errorf("sessions counts[%s] = %d, want %d (full: %v)", k, sessions[k], want, sessions)
		}
	}
	// The serve context cancels: the sweep loop ends with it, and serve
	// returns 0 (it waits for the loop before closing the store).
	cancel()
	select {
	case code := <-exitCh:
		exited = true
		if code != 0 {
			t.Fatalf("serve exit = %d, want 0 after cancel", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not return after the context cancel (the sweep loop is still running)")
	}
}

// dja275ServeCreateFaultTrigger opens the registry database with a plain
// connection (before serve opens it) and creates the fault trigger:
// every jobs delete aborts, so every sweep fails inside its transaction.
func dja275ServeCreateFaultTrigger(t *testing.T, db string) {
	t.Helper()
	conn := dja275ServeDBConn(t, db)
	defer conn.Close()
	if _, err := conn.Exec(`CREATE TRIGGER dja275_fail BEFORE DELETE ON jobs
		BEGIN SELECT RAISE(ABORT, 'dja275 sweep fault'); END`); err != nil {
		t.Fatalf("create the fault trigger: %v", err)
	}
}

// TestServeSweepFailureKeepsServing: a failed sweep writes the one line
// on stderr, without path or key, and the serve keeps serving.
func TestServeSweepFailureKeepsServing(t *testing.T) {
	dbDir := t.TempDir()
	db := filepath.Join(dbDir, "registry.db")
	cfgDir := t.TempDir()
	dja275ServeSeedDB(t, db)
	dja275ServeCreateFaultTrigger(t, db)
	serveSweepInterval = 500 * time.Millisecond
	var exited bool
	base, exitCh, cancel, _, errb := startServe(t, cfgDir, db, "--machines", "")
	t.Cleanup(func() {
		serveSweepInterval = 24 * time.Hour
		cancel()
		if !exited {
			select {
			case <-exitCh:
			case <-time.After(15 * time.Second):
			}
		}
	})
	line := errb.nextLine(t, 10*time.Second)
	if !strings.Contains(line, "serve: sweep failed") {
		t.Fatalf("first stderr line = %q, want the sweep failure line", line)
	}
	for _, leak := range []string{db, "j-gone", "registry.db"} {
		if strings.Contains(line, leak) {
			t.Errorf("sweep failure line = %q, must not carry %q", line, leak)
		}
	}
	// The serve keeps serving: the health endpoint still answers.
	resp, err := http.Get(base + "/v1/health")
	if err != nil {
		t.Fatalf("GET /v1/health after the sweep failure: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health after the sweep failure = %d, want 200 (the serve keeps serving)", resp.StatusCode)
	}
	cancel()
	select {
	case code := <-exitCh:
		exited = true
		if code != 0 {
			t.Fatalf("serve exit = %d, want 0 after cancel", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not return after the context cancel")
	}
}

// TestServeListenerFailureStopsSweep: a listener failure after the serve
// is up exits with the failure code without an external cancellation: the
// serve cancels the sweep context before waiting for the sweep to end, so
// the store is closed on the serve's own exit path.
func TestServeListenerFailureStopsSweep(t *testing.T) {
	old := serveListen
	listeners := make(chan net.Listener, 1)
	serveListen = func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			select {
			case listeners <- ln:
			default:
			}
		}
		return ln, err
	}
	t.Cleanup(func() { serveListen = old })
	_, exitCh, _, _, errb := startServe(t, t.TempDir(), filepath.Join(t.TempDir(), "registry.db"), "--machines", "")
	select {
	case ln := <-listeners:
		ln.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("no listener captured after the startup line")
	}
	select {
	case code := <-exitCh:
		if code != 4 {
			t.Fatalf("serve exit = %d, want 4 (the listener failure)", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("serve did not return within 5 s of the listener failure; stderr=%q", errb.snapshot())
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
	if !strings.Contains(stderr, "nau: jobs check: refused (duplicate)") {
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
