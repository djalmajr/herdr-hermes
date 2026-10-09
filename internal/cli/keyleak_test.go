package cli

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/plugin"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// TestKeyNeverLeaks proves the key-leak contract end to end: the sentinel
// key is stored through `auth login --key -` and then used by every command
// that pushes (and refused by every command that does not). Every command
// path owned by this slice and the slice-1 commands are run on their
// success and failure paths; the key must appear nowhere except the
// Authorization header of the push requests — not on stdout, stderr, any
// state file, the config file, or the request bodies.
//
// The command cases live in tables (one row per case) so the slice-2
// job/sync/doctor paths can be appended at integration; every output of
// every case is swept at the end, so adding a case adds no new assertion.
func TestKeyNeverLeaks(t *testing.T) {
	srv200 := newCLIPushServer(t, 200)
	srv401 := newCLIPushServer(t, 401)
	srv500 := newCLIPushServer(t, 500)
	// A server that closes the connection without answering.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	t.Cleanup(func() { ln.Close() })
	deadURL := "http://" + ln.Addr().String()

	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1", "dispatcher_url": srv200.URL})

	// Every command output (stdout and stderr) lands in sinks; the final
	// sweep scans them all for the key.
	var sinks []string
	run := func(stdin string, vars map[string]string, args ...string) (string, string, int) {
		t.Helper()
		out, errb, exit := runCLIStdin(t, dir, stdin, vars, args...)
		sinks = append(sinks, out, errb)
		return out, errb, exit
	}
	runSleep := func(stdin string, vars map[string]string, args ...string) (string, string, int) {
		t.Helper()
		sleep := func(context.Context, time.Duration) error { return nil }
		out, errb, exit := runCLIStdinSleep(t, dir, stdin, vars, sleep, args...)
		sinks = append(sinks, out, errb)
		return out, errb, exit
	}
	jobVars := map[string]string{
		jobapi.EnvJobID:             "job-1",
		jobapi.EnvJobSeq:            "1",
		jobapi.EnvJobIdempotencyKey: "job-1:1",
	}

	// A command case of the table.
	type leakCase struct {
		name  string
		stdin string
		vars  map[string]string
		sleep bool // use the injected (instant) sleep
		args  []string
		want  int
	}
	runTable := func(cases []leakCase) {
		for _, tc := range cases {
			t.Helper()
			var exit int
			if tc.sleep {
				_, _, exit = runSleep(tc.stdin, tc.vars, tc.args...)
			} else {
				_, _, exit = run(tc.stdin, tc.vars, tc.args...)
			}
			if exit != tc.want {
				t.Fatalf("%s: exit %d, want %d", tc.name, exit, tc.want)
			}
		}
	}

	// 1. The key is stored through the only accepted path.
	if out, errb, exit := run(sentinelKey+"\n", nil, "auth", "login", "--key", "-"); exit != 0 || out != `{"configured":true,"store":"file"}
` {
		t.Fatalf("auth login: exit %d stdout %q stderr %q", exit, out, errb)
	}

	// 2. The credentials file is 0600 on POSIX.
	if runtime.GOOS != "windows" {
		st, err := os.Stat(dir + "/credentials")
		if err != nil || st.Mode().Perm() != 0600 {
			t.Fatalf("credentials mode = %v (%v); want 0600", st.Mode(), err)
		}
	}

	// 3. Every owned command on the success path (dispatcher 200), and the
	// slice-1 commands.
	//
	// The fake herdr-soho backs the slice-2 subprocess paths (job
	// forwarding, sync, doctor) and the slice-4b plugin entry points added
	// below; every child's argv, stdin and environment are captured in the
	// fake's call log and swept for the key at the end.
	fakeExe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{Argv: []string{"config"}, Code: 0},
		fakesoho.Rule{Argv: []string{"job", "start", "--id", "job-2", "--repo", "org/repo"}, Stdout: `{"id":"job-2","status":"accepted"}` + "\n", Code: 0},
		fakesoho.Rule{Argv: []string{"job", "status", "--id", "job-2"}, Stdout: `{"id":"job-2","status":"running"}` + "\n", Code: 0},
		fakesoho.Rule{Argv: []string{"job", "status", "--id", "job-9"}, Stdout: `{"status":"not_found"}` + "\n", Code: 3},
		fakesoho.Rule{Argv: []string{"job", "amend", "--id", "job-2"}, Stdout: `{"id":"job-2","status":"running"}` + "\n", Code: 0},
		fakesoho.Rule{Argv: []string{"job", "send", "--id", "job-2"}, Stdout: `{"id":"job-2","status":"running"}` + "\n", Code: 0},
		fakesoho.Rule{Argv: []string{"job", "close", "--id", "job-2"}, Stdout: `{"id":"job-2","status":"closed"}` + "\n", Code: 0},
		fakesoho.Rule{Argv: []string{"job", "events", "--id", "job-1"}, ArgvPrefix: true, Stdout: trailer(3, "running") + "\n", Code: 0},
		fakesoho.Rule{Argv: []string{"job", "events", "--id", "job-2"}, ArgvPrefix: true, Stdout: eventLine(1, "status", "working") + "\n" + trailer(1, "running") + "\n", Code: 0},
		fakesoho.Rule{Argv: []string{"job", "events", "--id", "J1"}, ArgvPrefix: true, Stdout: eventLine(1, "status", "working") + "\n" + trailer(1, "running") + "\n", Code: 0},
	)
	setSohoConfig(t, dir, fakeExe, "")
	runTable([]leakCase{
		{"wake first event", wakeEvent(1), jobVars, false, []string{"wake"}, 0},
		{"wake second event", wakeEvent(2), map[string]string{jobapi.EnvJobID: "job-1"}, false, []string{"wake"}, 0},
		{"session start", "", nil, false, []string{"session", "start", "--projeto", "org/repo", "--branch", "b1"}, 0},
		{"session update", "", nil, false, []string{"session", "update", "--projeto", "org/repo", "--branch", "b1", "--estado", "working"}, 0},
		{"session end", "", nil, false, []string{"session", "end", "--projeto", "org/repo", "--estado", "done"}, 0},
		{"decision from stdin", "ship it", nil, false, []string{"decision", "--projeto", "org/repo", "--motivo", "m", "--escopo", "projeto", "-"}, 0},
		// The two wake records were already delivered by wake's own bounded
		// push; push delivers the remaining session and decision records.
		{"push", "", nil, false, []string{"push"}, 0},
		{"outbox", "", nil, false, []string{"outbox"}, 0},
		{"config list", "", nil, false, []string{"config", "list"}, 0},
		{"config get", "", nil, false, []string{"config", "get", "machine_label"}, 0},
		{"capabilities", "", nil, false, []string{"capabilities", "--json"}, 0},
		{"version", "", nil, false, []string{"version"}, 0},
		{"help", "", nil, false, []string{"help"}, 0},
	})

	// 4. Failure paths with the key stored. Each failing server first gets
	// a new pending record (wake appends and still exits 0).
	setConfig(t, dir, map[string]string{"dispatcher_url": srv401.URL})
	runTable([]leakCase{
		{"wake against the 401 dispatcher", wakeEvent(3), map[string]string{jobapi.EnvJobID: "job-1"}, false, []string{"wake"}, 0},
		{"push 401 auth_rejected", "", nil, false, []string{"push"}, 41},
	})
	setConfig(t, dir, map[string]string{"dispatcher_url": srv500.URL})
	runTable([]leakCase{
		{"push 500 unreachable after retries", "", nil, true, []string{"push"}, 42},
	})
	setConfig(t, dir, map[string]string{"dispatcher_url": deadURL})
	runTable([]leakCase{
		{"push closed connection unreachable", "", nil, true, []string{"push"}, 42},
	})
	// Input and usage failures.
	runTable([]leakCase{
		{"wake bad json", "not json", jobVars, false, []string{"wake"}, 2},
		{"wake missing job id", wakeEvent(9), nil, false, []string{"wake"}, 2},
		{"session bad projeto", "", nil, false, []string{"session", "start", "--projeto", "bad repo"}, 2},
		{"decision extra flag", "resumo", nil, false, []string{"decision", "--projeto", "org/repo", "--extra"}, 2},
		{"push extra args", "", nil, false, []string{"push", "extra"}, 2},
	})

	// 4b. The slice-2 subprocess paths (job forwarding, sync, doctor) and
	// the slice-4b plugin entry points, against the fake herdr-soho; the
	// call-log sweep below proves no child saw the key in its environment
	// or stdin.
	setConfig(t, dir, map[string]string{"dispatcher_url": srv200.URL})
	runTable([]leakCase{
		{"job start", "", nil, false, []string{"job", "start", "--id", "job-2", "--repo", "org/repo"}, 0},
		{"job status", "", nil, false, []string{"job", "status", "--id", "job-2"}, 0},
		{"job status forward failure", "", nil, false, []string{"job", "status", "--id", "job-9"}, 3},
		{"job amend", "", nil, false, []string{"job", "amend", "--id", "job-2"}, 0},
		{"job send", "note for the job", nil, false, []string{"job", "send", "--id", "job-2"}, 0},
		{"job close", "", nil, false, []string{"job", "close", "--id", "job-2"}, 0},
		{"sync against the 200 dispatcher", "", nil, false, []string{"sync"}, 0},
		{"doctor", "", nil, false, []string{"doctor"}, 0},
		{"plugin startup", "", nil, false, []string{"plugin", "startup"}, 0},
		{"plugin event workspace.created", "", map[string]string{plugin.EnvEvent: "workspace.created", plugin.EnvEventJSON: `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`}, false, []string{"plugin", "event"}, 0},
		{"plugin event workspace.closed", "", map[string]string{plugin.EnvEvent: "workspace.closed", plugin.EnvEventJSON: `{"workspace":{"label":"job-J1","workspace_id":"ws-1"}}`}, false, []string{"plugin", "event"}, 0},
		{"plugin bridge status", "", nil, false, []string{"plugin", "bridge", "status"}, 0},
		{"plugin bridge sync", "", nil, false, []string{"plugin", "bridge", "sync"}, 0},
	})

	// 5. auth status prints the exact configured line.
	if out, _, exit := run("", nil, "auth", "status"); exit != 0 || out != `{"configured":true,"store":"file"}
` {
		t.Fatalf("auth status: exit %d stdout %q", exit, out)
	}

	// 6. The key is never accepted as a flag value and no environment
	// variable is ever read for it (the allowlist is HERDR_HERMES_NOWRITE).
	var queried []string
	var outB, errB bytes.Buffer
	customEnv := Env{
		Stdin:  strings.NewReader(sentinelKey + "\n"),
		Stdout: &outB,
		Stderr: &errB,
		Getenv: func(k string) string {
			queried = append(queried, k)
			return ""
		},
		ConfigDir: dir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	// Flag value refused; the constant message never echoes the value.
	exit := Run([]string{"auth", "login", "--key", sentinelKey}, customEnv)
	if exit != 2 {
		t.Fatalf("auth login --key <value>: exit %d, want 2", exit)
	}
	if strings.Contains(outB.String()+errB.String(), sentinelKey) {
		t.Fatalf("flag-value refusal echoed the key: %q", outB.String()+errB.String())
	}
	// Even when the environment could carry the key, it is never queried:
	// the same command succeeds purely from stdin.
	outB.Reset()
	errB.Reset()
	customEnv.Stdout = &outB
	customEnv.Stderr = &errB
	exit = Run([]string{"auth", "login", "--key", "-"}, customEnv)
	if exit != 0 {
		t.Fatalf("auth login from stdin: exit %d stderr %q", exit, errB.String())
	}
	allowed := map[string]bool{nowriteVar: true}
	for _, k := range queried {
		if !allowed[k] {
			t.Fatalf("auth login queried the environment variable %q; the allowlist is %v", k, allowed)
		}
	}

	// 7. The key appears nowhere.
	for i, s := range sinks {
		if strings.Contains(s, sentinelKey) {
			t.Fatalf("command output %d leaked the key: %.300q", i, s)
		}
	}
	for _, f := range []string{"config", "state/outbox.jsonl", "state/cursor.json", "state/friction.log", "state/jobs.json", "state/sessions.json"} {
		data, err := os.ReadFile(dir + "/" + f)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read %s: %v", f, err)
		}
		if strings.Contains(string(data), sentinelKey) {
			t.Fatalf("%s leaked the key: %.300q", f, data)
		}
	}
	// The subprocess audit: the fake call log captures every herdr-soho
	// child's argv, stdin and full environment; the key must be in none of
	// them.
	if calls := readFakeCalls(t, fakeDir); len(calls) == 0 {
		t.Fatal("the fake herdr-soho was never called; the subprocess sweep is vacuous")
	}
	callLog, err := os.ReadFile(filepath.Join(fakeDir, "fake-herdr-soho.calls.jsonl"))
	if err != nil {
		t.Fatalf("read fake call log: %v", err)
	}
	if strings.Contains(string(callLog), sentinelKey) {
		t.Fatalf("a herdr-soho child environment or stdin carried the key: %.300q", callLog)
	}
	// On the wire the key exists only as the Authorization header.
	for name, s := range map[string]*cliPushServer{"200": srv200, "401": srv401, "500": srv500} {
		s.mu.Lock()
		for i, r := range s.requests {
			ah := r.Header.Get("Authorization")
			if ah != "" && ah != "Bearer "+sentinelKey {
				t.Fatalf("%s server request %d Authorization = %q, want the bearer key or empty", name, i, ah)
			}
			if len(s.bodies[i]) > 0 && strings.Contains(string(s.bodies[i]), sentinelKey) {
				t.Fatalf("%s server request %d body carried the key", name, i)
			}
		}
		s.mu.Unlock()
	}
	if srv200.count() == 0 {
		t.Fatal("no push request reached the 200 server; the Authorization assertion is vacuous")
	}

	// 8. auth logout removes the key and leaves no file.
	if out, _, exit := run("", nil, "auth", "logout"); exit != 0 || out != `{"configured":false,"store":"file"}
` {
		t.Fatalf("auth logout: exit %d stdout %q", exit, out)
	}
	if _, err := os.Stat(dir + "/credentials"); !os.IsNotExist(err) {
		t.Fatalf("credentials still present after logout (err %v)", err)
	}
	if out, _, exit := run("", nil, "auth", "status"); exit != 0 || out != `{"configured":false,"store":"file"}
` {
		t.Fatalf("auth status after logout: exit %d stdout %q", exit, out)
	}
}
