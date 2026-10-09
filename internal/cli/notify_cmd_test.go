package cli

// The notify command unit tests: every subcommand's success line, the
// validation failures (exit 2, nothing written), the per-subcommand
// NOWRITE gating and the read-only list/status behavior. The end-to-end
// flows across wake/sync/plugin live in notify_flow_test.go.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/notify"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

const (
	notifyTestRepo   = "example-org/example-repo"
	notifyTestJob    = "job-1"
	notifyTestOwner  = "local/w1:p1"
	notifyTestOrch   = "agent-1"
	notifyTestCoord  = "local/w1:p9"
	notifyTestPane   = "w1:p2"
	notifyNowriteOut = "{\"status\":\"nowrite\",\"motivo\":\"HERDR_HERMES_NOWRITE=1\"}\n"
)

// testClock is the injected clock of the notify tests, advanced by the
// tests themselves (never by real time).
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock {
	return &testClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// newNotifyFlowEnv builds the hermetic env of the notify flow tests: the
// test clock, the controlled child environment and a CLI-visible
// env-var map.
func newNotifyFlowEnv(dir string, clk *testClock, vars map[string]string, stdin string) (Env, *bytes.Buffer, *bytes.Buffer) {
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(stdin),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    func(k string) string { return vars[k] },
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: dir,
		Now:       clk.now,
		Sleep:     sleepCtx,
	}
	return env, &out, &errb
}

// runNotifyFlow runs Run with the test clock, the controlled child
// environment and a CLI-visible env-var map.
func runNotifyFlow(t *testing.T, dir string, clk *testClock, vars map[string]string, stdin string, args ...string) (string, string, int) {
	t.Helper()
	env, out, errb := newNotifyFlowEnv(dir, clk, vars, stdin)
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// runNotifyFlowNowrite runs Run with the test clock under
// HERDR_HERMES_NOWRITE=1.
func runNotifyFlowNowrite(t *testing.T, dir string, clk *testClock, vars map[string]string, stdin string, args ...string) (string, string, int) {
	t.Helper()
	env, out, errb := newNotifyFlowEnv(dir, clk, vars, stdin)
	env.Getenv = func(k string) string {
		if k == nowriteVar {
			return "1"
		}
		return vars[k]
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// sendRule is the fake herdr-soho rule for the `send` command: Argv
// prefix `send`, optional Nth-call match (call == 0 matches every call).
func sendRule(code, call int) fakesoho.Rule {
	return fakesoho.Rule{Argv: []string{"send"}, ArgvPrefix: true, Call: call, Code: code, Stdout: "sent to ref\n"}
}

// flowSendCalls returns the argv of every `send` call logged by the fake.
func flowSendCalls(t *testing.T, fakeDir string) [][]string {
	t.Helper()
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read fake calls: %v", err)
	}
	var sends [][]string
	for _, c := range calls {
		if len(c.Argv) > 0 && c.Argv[0] == "send" {
			sends = append(sends, c.Argv)
		}
	}
	return sends
}

// readLedger loads the notify ledger read-only.
func readLedger(t *testing.T, dir string) *notify.Ledger {
	t.Helper()
	store, err := outbox.Open(outbox.StateDir(dir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	led, err := notify.Open(store, func() time.Time { return time.Time{} }).LoadLedger()
	if err != nil {
		t.Fatalf("load ledger: %v", err)
	}
	return led
}

// TestNotifyRegister covers the four register kinds: the success line,
// the validation failures (exit 2, nothing written) and the NOWRITE
// refusal before any side effect.
func TestNotifyRegister(t *testing.T) {
	clk := newTestClock()
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})

	cases := []struct {
		args   []string
		stdout string
	}{
		{[]string{"notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner},
			`{"registered":"owner","projeto":"example-org/example-repo","to":"local/w1:p1"}` + "\n"},
		{[]string{"notify", "register", "orchestrator", "--job", notifyTestJob, "--to", notifyTestOrch},
			`{"registered":"orchestrator","job":"job-1","to":"agent-1"}` + "\n"},
		{[]string{"notify", "register", "coordinator", "--to", notifyTestCoord},
			`{"registered":"coordinator","to":"local/w1:p9"}` + "\n"},
		{[]string{"notify", "register", "watch", "--pane", notifyTestPane, "--job", notifyTestJob, "--projeto", notifyTestRepo},
			`{"registered":"watch","pane":"w1:p2","job":"job-1","projeto":"example-org/example-repo"}` + "\n"},
		// The --name=value form and the omitted-empty watch fields.
		{[]string{"notify", "register", "watch", "--pane=w1:p3"},
			`{"registered":"watch","pane":"w1:p3"}` + "\n"},
	}
	for _, tc := range cases {
		stdout, stderr, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", tc.args...)
		if exit != 0 {
			t.Fatalf("%v: exit = %d, stderr %q", tc.args, exit, stderr)
		}
		if stdout != tc.stdout {
			t.Fatalf("%v: stdout = %q, want %q", tc.args, stdout, tc.stdout)
		}
	}

	// Validation failures: exit 2, nothing written.
	before := dirEntries(t, dir)
	bad := [][]string{
		{"notify", "register"}, // no kind
		{"notify", "register", "boss", "--to", notifyTestOwner},
		{"notify", "register", "owner", "--projeto", notifyTestRepo}, // missing --to
		{"notify", "register", "owner", "--to", notifyTestOwner},     // missing --projeto
		{"notify", "register", "owner", "--projeto", "bad repo", "--to", notifyTestOwner},
		{"notify", "register", "owner", "--projeto", notifyTestRepo, "--to", "bad ref!"},
		{"notify", "register", "orchestrator", "--job", "bad id!", "--to", notifyTestOrch},
		{"notify", "register", "orchestrator", "--job", notifyTestJob}, // missing --to
		{"notify", "register", "coordinator"},                          // missing --to
		{"notify", "register", "watch", "--job", notifyTestJob},        // missing --pane
		{"notify", "register", "watch", "--pane", "w 1:p2", "--job", notifyTestJob},
		{"notify", "register", "watch", "--pane", notifyTestPane, "--job", "bad id!"},
		{"notify", "register", "watch", "--pane", notifyTestPane, "--projeto", "bad repo"},
		{"notify", "register", "coordinator", "--to", notifyTestCoord, "--to", notifyTestCoord}, // twice
		{"notify", "register", "coordinator", "--to", notifyTestCoord, "--extra"},               // unknown
		{"notify", "register", "coordinator", "extra", "--to", notifyTestCoord},                 // positional
		{"notify", "register", "coordinator", "--to"},                                           // bare trailing flag
	}
	for _, args := range bad {
		stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 {
			t.Fatalf("%v: exit = %d, want 2 (stdout %q)", args, exit, stdout)
		}
		if !strings.Contains(stdout, `"status":"2"`) {
			t.Fatalf("%v: stdout = %q, want the exit-2 error line", args, stdout)
		}
	}
	if after := dirEntries(t, dir); after != before {
		t.Fatalf("the validation failures wrote files: before %q after %q", before, after)
	}

	// The registry file holds the registrations.
	if got := readRegistry(t, dir); got.Owners[notifyTestRepo] != notifyTestOwner || got.Orchestrators[notifyTestJob] != notifyTestOrch ||
		got.Coordinator != notifyTestCoord || len(got.Watches) != 2 || got.Watches[notifyTestPane].Job != notifyTestJob {
		t.Fatalf("registry = %+v", got)
	}

	// NOWRITE: the writing subcommand refuses before any side effect.
	dirNR := t.TempDir()
	stdout, _, exit := runNotifyFlowNowrite(t, dirNR, clk, map[string]string{}, "",
		"notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	if exit != 2 || stdout != notifyNowriteOut {
		t.Fatalf("register under NOWRITE: exit %d stdout %q", exit, stdout)
	}
	if stateDirExists(t, dirNR) {
		t.Fatal("register under NOWRITE created the state dir")
	}
}

// TestNotifyUnregister covers the four unregister kinds: the removed
// true/false outcome and the validation failures.
func TestNotifyUnregister(t *testing.T) {
	clk := newTestClock()
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	mustRunNotify(t, dir, clk,
		"notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	mustRunNotify(t, dir, clk,
		"notify", "register", "orchestrator", "--job", notifyTestJob, "--to", notifyTestOrch)
	mustRunNotify(t, dir, clk,
		"notify", "register", "coordinator", "--to", notifyTestCoord)
	mustRunNotify(t, dir, clk,
		"notify", "register", "watch", "--pane", notifyTestPane)

	cases := []struct {
		args   []string
		stdout string
	}{
		{[]string{"notify", "unregister", "owner", "--projeto", notifyTestRepo},
			`{"unregistered":"owner","projeto":"example-org/example-repo","removed":true}` + "\n"},
		{[]string{"notify", "unregister", "owner", "--projeto", notifyTestRepo},
			`{"unregistered":"owner","projeto":"example-org/example-repo","removed":false}` + "\n"},
		{[]string{"notify", "unregister", "orchestrator", "--job", notifyTestJob},
			`{"unregistered":"orchestrator","job":"job-1","removed":true}` + "\n"},
		{[]string{"notify", "unregister", "coordinator"},
			`{"unregistered":"coordinator","removed":true}` + "\n"},
		{[]string{"notify", "unregister", "coordinator"},
			`{"unregistered":"coordinator","removed":false}` + "\n"},
		{[]string{"notify", "unregister", "watch", "--pane", notifyTestPane},
			`{"unregistered":"watch","pane":"w1:p2","removed":true}` + "\n"},
		{[]string{"notify", "unregister", "watch", "--pane", notifyTestPane},
			`{"unregistered":"watch","pane":"w1:p2","removed":false}` + "\n"},
	}
	for _, tc := range cases {
		stdout, stderr, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", tc.args...)
		if exit != 0 {
			t.Fatalf("%v: exit = %d, stderr %q", tc.args, exit, stderr)
		}
		if stdout != tc.stdout {
			t.Fatalf("%v: stdout = %q, want %q", tc.args, stdout, tc.stdout)
		}
	}

	// Validation failures: exit 2.
	bad := [][]string{
		{"notify", "unregister"},
		{"notify", "unregister", "boss", "--projeto", notifyTestRepo},
		{"notify", "unregister", "owner"},
		{"notify", "unregister", "owner", "--projeto", "bad repo"},
		{"notify", "unregister", "orchestrator", "--job", "bad id!"},
		{"notify", "unregister", "watch", "--pane", "bad pane"},
		{"notify", "unregister", "watch", "--pane", notifyTestPane, "--projeto", notifyTestRepo},
		{"notify", "unregister", "coordinator", "extra"},
	}
	for _, args := range bad {
		stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Fatalf("%v: exit = %d stdout %q, want 2", args, exit, stdout)
		}
	}
	// The coordinator unregister keeps no flag: even a valid one is
	// rejected.
	if _, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "",
		"notify", "unregister", "coordinator", "--to", notifyTestCoord); exit != 2 {
		t.Fatal("unregister coordinator with a flag must exit 2")
	}
}

// mustRunNotify runs one notify command and fails the test unless it
// exits 0.
func mustRunNotify(t *testing.T, dir string, clk *testClock, args ...string) string {
	t.Helper()
	stdout, stderr, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
	if exit != 0 {
		t.Fatalf("%v: exit = %d, stderr %q", args, exit, stderr)
	}
	return stdout
}

// readRegistry loads the notify registry read-only.
func readRegistry(t *testing.T, dir string) notify.Registry {
	t.Helper()
	store, err := outbox.Open(outbox.StateDir(dir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	reg, err := notify.Open(store, func() time.Time { return time.Time{} }).LoadRegistry()
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	return reg
}

// TestNotifyList covers the read-only list line, the disabled state on a
// fresh dir (nothing created) and the counts after registrations and a
// projected event.
func TestNotifyList(t *testing.T) {
	clk := newTestClock()
	// Fresh dir: disabled, nothing created (not even the state dir).
	dir := t.TempDir()
	stdout, stderr, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "list")
	if exit != 0 {
		t.Fatalf("list exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"enabled":false,"owners":{},"orchestrators":{},"coordinator":"","watches":{},"projected_seq":0,"notifications":0,"pending":0}`+"\n" {
		t.Fatalf("list stdout = %q", stdout)
	}
	if stateDirExists(t, dir) {
		t.Fatal("list created the state dir")
	}

	// After registrations.
	setConfig(t, dir, map[string]string{"machine_label": "m1"})
	mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	mustRunNotify(t, dir, clk, "notify", "register", "orchestrator", "--job", notifyTestJob, "--to", notifyTestOrch)
	mustRunNotify(t, dir, clk, "notify", "register", "coordinator", "--to", notifyTestCoord)
	mustRunNotify(t, dir, clk, "notify", "register", "watch", "--pane", notifyTestPane)
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "list")
	if exit != 0 {
		t.Fatalf("list exit = %d", exit)
	}
	var l struct {
		Enabled       bool                    `json:"enabled"`
		Owners        map[string]string       `json:"owners"`
		Orchestrators map[string]string       `json:"orchestrators"`
		Coordinator   string                  `json:"coordinator"`
		Watches       map[string]notify.Watch `json:"watches"`
		ProjectedSeq  int64                   `json:"projected_seq"`
		Notifications int                     `json:"notifications"`
		Pending       int                     `json:"pending"`
	}
	if err := json.Unmarshal([]byte(stdout), &l); err != nil {
		t.Fatalf("list stdout %q is not JSON: %v", stdout, err)
	}
	if !l.Enabled || l.Owners[notifyTestRepo] != notifyTestOwner || l.Orchestrators[notifyTestJob] != notifyTestOrch ||
		l.Coordinator != notifyTestCoord || l.Watches[notifyTestPane].Pane != notifyTestPane {
		t.Fatalf("list = %+v", l)
	}

	// A projected accepted event: one notification, zero pending.
	vars := map[string]string{jobapi.EnvJobID: notifyTestJob}
	stdout, _, exit = runNotifyFlow(t, dir, clk, vars, wakeEvent(1), "wake")
	if exit != 0 {
		t.Fatalf("wake exit = %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "list")
	if exit != 0 {
		t.Fatalf("list exit = %d", exit)
	}
	var l2 struct {
		ProjectedSeq  int64 `json:"projected_seq"`
		Notifications int   `json:"notifications"`
		Pending       int   `json:"pending"`
	}
	if err := json.Unmarshal([]byte(stdout), &l2); err != nil {
		t.Fatalf("list stdout %q is not JSON: %v", stdout, err)
	}
	if l2.ProjectedSeq != 1 || l2.Notifications != 1 || l2.Pending != 0 {
		t.Fatalf("list = %+v, want projected_seq 1, 1 notification, 0 pending", l2)
	}
}

// TestNotifyListPending counts a pending delivery in the list.
func TestNotifyListPending(t *testing.T) {
	clk := newTestClock()
	exe, fakeDir := installFakeSoho(t, sendRule(17, 0))
	dir := t.TempDir()
	setSohoConfig(t, dir, exe, "machine-a")
	mustRunNotify(t, dir, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	seedJob(t, dir, notifyTestJob, notifyTestRepo, 0, "accepted")
	vars := map[string]string{jobapi.EnvJobID: notifyTestJob}
	if _, _, exit := runNotifyFlow(t, dir, clk, vars, wakeEvent(1), "wake"); exit != 0 {
		t.Fatal("wake exit != 0")
	}
	stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "list")
	if exit != 0 {
		t.Fatalf("list exit = %d", exit)
	}
	if !strings.Contains(stdout, `"pending":1`) || !strings.Contains(stdout, `"notifications":1`) {
		t.Fatalf("list = %q, want 1 pending notification", stdout)
	}
	_ = fakeDir
}

// TestNotifyStatus covers the read-only status line: the last 50 in
// ledger order, the exact-id lookup, the unknown-id exit 3 and the
// validation failures.
func TestNotifyStatus(t *testing.T) {
	clk := newTestClock()
	dir := t.TempDir()
	// Fresh dir: an empty list, nothing created.
	stdout, stderr, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "status")
	if exit != 0 {
		t.Fatalf("status exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"notifications":[]}`+"\n" {
		t.Fatalf("status stdout = %q", stdout)
	}
	if stateDirExists(t, dir) {
		t.Fatal("status created the state dir")
	}
	// Unknown id: exit 3.
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, "",
		"notify", "status", "--id", "n"+strings.Repeat("0", 20))
	if exit != 3 || stdout != `{"status":"not_found"}`+"\n" {
		t.Fatalf("status unknown id: exit %d stdout %q, want 3 not_found", exit, stdout)
	}
	// Validation failures: exit 2.
	for _, args := range [][]string{
		{"notify", "status", "--id", "n1", "--id", "n2"},
		{"notify", "status", "--extra"},
		{"notify", "status", "n1"},
	} {
		stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Fatalf("%v: exit = %d stdout %q, want 2", args, exit, stdout)
		}
	}

	// Fifty-two notifications: the status shows the last fifty.
	store, err := outbox.Open(outbox.StateDir(dir), outbox.Options{})
	if err != nil {
		t.Fatal(err)
	}
	nstore := notify.Open(store, clk.now)
	if err := nstore.UpdateRegistry(func(reg *notify.Registry) error {
		reg.Owners[notifyTestRepo] = notifyTestOwner
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	const nEvents = 52
	for i := 1; i <= nEvents; i++ {
		if _, _, err := store.AppendJobEvent("m1", notifyTestRepo, notifyTestJob,
			[]byte(fmt.Sprintf(`{"seq":%d,"ts":"2026-01-01T12:00:00-03:00","tipo":"question"}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := nstore.ProjectOutbox(notify.ProjectJobEvent); err != nil {
		t.Fatal(err)
	}
	led := readLedger(t, dir)
	if len(led.Notifications) != nEvents {
		t.Fatalf("ledger has %d notifications, want %d", len(led.Notifications), nEvents)
	}
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "status")
	if exit != 0 {
		t.Fatalf("status exit = %d", exit)
	}
	var st struct {
		Notifications []struct {
			ID string `json:"id"`
		} `json:"notifications"`
	}
	if err := json.Unmarshal([]byte(stdout), &st); err != nil {
		t.Fatalf("status stdout %q is not JSON: %v", stdout, err)
	}
	if len(st.Notifications) != 50 {
		t.Fatalf("status shows %d notifications, want the last 50", len(st.Notifications))
	}
	if st.Notifications[0].ID != led.Notifications[2].ID {
		t.Fatalf("status first = %q, want the third notification %q", st.Notifications[0].ID, led.Notifications[2].ID)
	}
	if st.Notifications[49].ID != led.Notifications[nEvents-1].ID {
		t.Fatalf("status last = %q, want the last notification", st.Notifications[49].ID)
	}
	// The exact id lookup shows exactly that notification.
	want := led.Notifications[nEvents-1].ID
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "status", "--id", want)
	if exit != 0 {
		t.Fatalf("status --id exit = %d", exit)
	}
	var st2 struct {
		Notifications []struct {
			ID string `json:"id"`
		} `json:"notifications"`
	}
	if err := json.Unmarshal([]byte(stdout), &st2); err != nil || len(st2.Notifications) != 1 || st2.Notifications[0].ID != want {
		t.Fatalf("status --id = %q (parsed %+v), want exactly %q", stdout, st2, want)
	}
}

// TestNotifyDeliver covers the deliver line, the disabled state, the
// timeout validation, the unopenable state dir (exit 2) and the NOWRITE
// refusal.
func TestNotifyDeliver(t *testing.T) {
	clk := newTestClock()
	// Fresh dir: disabled, zero counts, nothing created.
	dir := t.TempDir()
	stdout, stderr, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", "notify", "deliver")
	if exit != 0 {
		t.Fatalf("deliver exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"enabled":false,"created":0,"duplicates":0,"malformed":0,"claimed":0,"accepted":0,"uncertain":0,"rejected":0,"retrying":0,"exhausted":0}`+"\n" {
		t.Fatalf("deliver stdout = %q", stdout)
	}
	if stateDirExists(t, dir) {
		t.Fatal("deliver created the state dir")
	}

	// Timeout validation: exit 2.
	for _, args := range [][]string{
		{"notify", "deliver", "--timeout", "0"},
		{"notify", "deliver", "--timeout", "600001"},
		{"notify", "deliver", "--timeout", "abc"},
		{"notify", "deliver", "--timeout", "-5"},
		{"notify", "deliver", "--timeout"},
		{"notify", "deliver", "--extra"},
		{"notify", "deliver", "extra"},
	} {
		stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Fatalf("%v: exit = %d stdout %q, want 2", args, exit, stdout)
		}
	}

	// A pending delivery is claimed and retried (transient -> retrying).
	clk2 := newTestClock()
	exe, _ := installFakeSoho(t, sendRule(17, 0))
	dir2 := t.TempDir()
	setSohoConfig(t, dir2, exe, "machine-a")
	mustRunNotify(t, dir2, clk2, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	seedJob(t, dir2, notifyTestJob, notifyTestRepo, 0, "accepted")
	vars := map[string]string{jobapi.EnvJobID: notifyTestJob}
	if _, _, exit := runNotifyFlow(t, dir2, clk2, vars, wakeEvent(1), "wake"); exit != 0 {
		t.Fatal("wake exit != 0")
	}
	// After the injected clock passes the 10 s retry delay, deliver
	// claims and retries the pending delivery.
	clk2.add(11 * time.Second)
	stdout, _, exit = runNotifyFlow(t, dir2, clk2, map[string]string{}, "", "notify", "deliver", "--timeout", "1000")
	if exit != 0 {
		t.Fatalf("deliver exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"enabled":true,"created":0,"duplicates":0,"malformed":0,"claimed":1,"accepted":0,"uncertain":0,"rejected":0,"retrying":1,"exhausted":0}`+"\n" {
		t.Fatalf("deliver stdout = %q", stdout)
	}

	// Unopenable state dir: exit 2.
	dir3 := t.TempDir()
	setConfig(t, dir3, map[string]string{"machine_label": "m1"})
	if err := os.WriteFile(outbox.StateDir(dir3), []byte("not a dir"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, _, exit = runNotifyFlow(t, dir3, clk, map[string]string{}, "", "notify", "deliver")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Fatalf("deliver with an unopenable state dir: exit = %d stdout %q, want 2", exit, stdout)
	}

	// NOWRITE: the writing subcommand refuses before any side effect.
	dirNR := t.TempDir()
	stdout, _, exit = runNotifyFlowNowrite(t, dirNR, clk, map[string]string{}, "", "notify", "deliver")
	if exit != 2 || stdout != notifyNowriteOut {
		t.Fatalf("deliver under NOWRITE: exit %d stdout %q", exit, stdout)
	}
	if stateDirExists(t, dirNR) {
		t.Fatal("deliver under NOWRITE created the state dir")
	}
}

// TestNotifyIngest covers the ingest validation, the not-an-agent-status
// and the malformed outcomes, the disabled no-write path and the NOWRITE
// refusal.
func TestNotifyIngest(t *testing.T) {
	clk := newTestClock()
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})

	// Usage failures: exit 2.
	for _, args := range [][]string{
		{"notify", "ingest"},
		{"notify", "ingest", "bogus"},
		{"notify", "ingest", "agent-status", "extra"},
	} {
		stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Fatalf("%v: exit = %d stdout %q, want 2", args, exit, stdout)
		}
	}
	// Stdin over the 64 KiB cap: exit 2.
	over := `{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"blocked","pad":"` + strings.Repeat("a", jobapi.StdinCapAmend) + `"}`
	stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, over, "notify", "ingest", "agent-status")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Fatalf("ingest over the cap: exit = %d stdout %q, want 2", exit, stdout)
	}
	if stateDirExists(t, dir) {
		t.Fatal("the failing ingests created the state dir")
	}
	// Not an agent status event: exit 0 with the ignored line.
	notStatus := `{"event":"workspace.created","data":{"label":"job-1"}}`
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, notStatus, "notify", "ingest", "agent-status")
	if exit != 0 || stdout != `{"ignored":true,"reason":"not_agent_status"}`+"\n" {
		t.Fatalf("ingest not-agent-status: exit %d stdout %q", exit, stdout)
	}
	// Malformed input: exit 2.
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, `{nope`, "notify", "ingest", "agent-status")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Fatalf("ingest malformed: exit = %d stdout %q, want 2", exit, stdout)
	}
	// Disabled (no registry): the valid event is ignored, nothing created.
	valid := `{"event":"pane.agent_status_changed","data":{"pane_id":"w1:p2","workspace_id":"w1","agent_status":"blocked"}}`
	stdout, _, exit = runNotifyFlow(t, dir, clk, map[string]string{}, valid, "notify", "ingest", "agent-status")
	if exit != 0 || stdout != `{"ignored":true,"duplicate":false,"routine":false,"created":false,"notification":"","n":0,"accepted":0}`+"\n" {
		t.Fatalf("ingest disabled: exit %d stdout %q", exit, stdout)
	}
	if stateDirExists(t, dir) {
		t.Fatal("ingest while disabled created the state dir")
	}
	// NOWRITE: the writing subcommand refuses.
	stdout, _, exit = runNotifyFlowNowrite(t, dir, clk, map[string]string{}, valid, "notify", "ingest", "agent-status")
	if exit != 2 || stdout != notifyNowriteOut {
		t.Fatalf("ingest under NOWRITE: exit %d stdout %q", exit, stdout)
	}
}

// TestNotifyRaise covers the raise validation, the not-enabled exit 2
// (the ErrNotEnabled text), the success line and the idempotent replay.
func TestNotifyRaise(t *testing.T) {
	clk := newTestClock()
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})

	// Not enabled: exit 2 with the ErrNotEnabled text.
	stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "",
		"notify", "raise", "--class", "stuck")
	if exit != 2 {
		t.Fatalf("raise not enabled: exit = %d, want 2 (stdout %q)", exit, stdout)
	}
	if !strings.Contains(stdout, notify.ErrNotEnabled.Error()) {
		t.Fatalf("raise not enabled stdout = %q, want the ErrNotEnabled text", stdout)
	}
	if stateDirExists(t, dir) {
		t.Fatal("raise while not enabled created the state dir")
	}

	// Validation failures: exit 2.
	for _, args := range [][]string{
		{"notify", "raise"},
		{"notify", "raise", "--class", "bogus"},
		{"notify", "raise", "--class", "stuck", "--class", "stuck"},
		{"notify", "raise", "--class", "stuck", "--job", "bad id!"},
		{"notify", "raise", "--class", "stuck", "--projeto", "bad repo"},
		{"notify", "raise", "--class", "stuck", "--id", "bad id!"},
		{"notify", "raise", "--class", "stuck", "extra"},
		{"notify", "raise", "--class", "stuck", "--extra"},
	} {
		stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Fatalf("%v: exit = %d stdout %q, want 2", args, exit, stdout)
		}
	}

	// Enabled: a stuck raise is created and delivered; the replay with
	// the same id returns the stored notification, created=false, and
	// sends nothing new.
	exe, fakeDir := installFakeSoho(t, sendRule(0, 0))
	dir2 := t.TempDir()
	setSohoConfig(t, dir2, exe, "machine-a")
	mustRunNotify(t, dir2, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	mustRunNotify(t, dir2, clk, "notify", "register", "coordinator", "--to", notifyTestCoord)
	stdout, _, exit = runNotifyFlow(t, dir2, clk, map[string]string{}, "",
		"notify", "raise", "--class", "stuck", "--job", notifyTestJob, "--projeto", notifyTestRepo, "--id", "raise-1")
	if exit != 0 {
		t.Fatalf("raise exit = %d, stdout %q", exit, stdout)
	}
	var r struct {
		Notification string `json:"notification"`
		RaiseID      string `json:"raise_id"`
		Created      bool   `json:"created"`
		Accepted     int    `json:"accepted"`
	}
	if err := json.Unmarshal([]byte(stdout), &r); err != nil || r.RaiseID != "raise-1" || !r.Created || r.Accepted != 2 ||
		!strings.HasPrefix(r.Notification, "n") || len(r.Notification) != 21 {
		t.Fatalf("raise = %+v (stdout %q), want created with raise-1 and 2 accepted sends", r, stdout)
	}
	if n := len(flowSendCalls(t, fakeDir)); n != 2 {
		t.Fatalf("raise made %d sends, want 2 (owner + coordinator)", n)
	}
	stdout, _, exit = runNotifyFlow(t, dir2, clk, map[string]string{}, "",
		"notify", "raise", "--class", "stuck", "--job", notifyTestJob, "--projeto", notifyTestRepo, "--id", "raise-1")
	if exit != 0 {
		t.Fatalf("raise replay exit = %d, stdout %q", exit, stdout)
	}
	var r2 struct {
		Notification string `json:"notification"`
		Created      bool   `json:"created"`
		Accepted     int    `json:"accepted"`
	}
	if err := json.Unmarshal([]byte(stdout), &r2); err != nil || r2.Notification != r.Notification || r2.Created || r2.Accepted != 0 {
		t.Fatalf("raise replay = %+v, want the stored notification with created=false and no new sends", r2)
	}
	if n := len(flowSendCalls(t, fakeDir)); n != 2 {
		t.Fatalf("the replay made %d sends total, want still 2", n)
	}
	// A generated raise id is 16 hex digits and accepted.
	stdout, _, exit = runNotifyFlow(t, dir2, clk, map[string]string{}, "",
		"notify", "raise", "--class", "cross_project", "--projeto", notifyTestRepo)
	if exit != 0 {
		t.Fatalf("raise cross_project exit = %d, stdout %q", exit, stdout)
	}
	var r3 struct {
		RaiseID  string `json:"raise_id"`
		Created  bool   `json:"created"`
		Accepted int    `json:"accepted"`
	}
	if err := json.Unmarshal([]byte(stdout), &r3); err != nil || !r3.Created || len(r3.RaiseID) != 16 || r3.Accepted != 2 {
		t.Fatalf("raise cross_project = %+v, want a 16-hex id, created, 2 accepted sends", r3)
	}
	// NOWRITE: the writing subcommand refuses.
	stdout, _, exit = runNotifyFlowNowrite(t, dir, clk, map[string]string{}, "", "notify", "raise", "--class", "stuck")
	if exit != 2 || stdout != notifyNowriteOut {
		t.Fatalf("raise under NOWRITE: exit %d stdout %q", exit, stdout)
	}
}

// TestNotifyAck covers the ack validation, the not-found exit 3 and the
// success line, including the second ack keeping the first timestamp.
func TestNotifyAck(t *testing.T) {
	clk := newTestClock()
	dir := t.TempDir()
	setConfig(t, dir, map[string]string{"machine_label": "m1"})

	// Validation failures: exit 2.
	for _, args := range [][]string{
		{"notify", "ack"},
		{"notify", "ack", "n1"},
		{"notify", "ack", "n1", "--role", "boss"},
		{"notify", "ack", "n1", "n2", "--role", "owner"},
		{"notify", "ack", "n1", "--role", "owner", "--extra"},
		{"notify", "ack", "n1"},
	} {
		stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Fatalf("%v: exit = %d stdout %q, want 2", args, exit, stdout)
		}
	}
	// Unknown id on a fresh state: exit 3.
	stdout, _, exit := runNotifyFlow(t, dir, clk, map[string]string{}, "",
		"notify", "ack", "n"+strings.Repeat("0", 20), "--role", "owner")
	if exit != 3 || stdout != `{"status":"not_found"}`+"\n" {
		t.Fatalf("ack unknown id: exit %d stdout %q, want 3 not_found", exit, stdout)
	}
	// Notifications disabled: the ack writes nothing, not even the state
	// dir (a recipient acking on a machine without registrations).
	if _, err := os.Stat(filepath.Join(dir, "state")); !os.IsNotExist(err) {
		t.Fatalf("ack with notifications disabled created the state dir (stat err %v)", err)
	}

	// A delivered notification is acked; a non-final one is not found.
	ev1, ev2 := wakeEvent(1), wakeEvent(2)
	dir2 := t.TempDir()
	setConfig(t, dir2, map[string]string{"machine_label": "m1"})
	mustRunNotify(t, dir2, clk, "notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner)
	seedJob(t, dir2, notifyTestJob, notifyTestRepo, 0, "accepted")
	msg1 := expectedSendMessage(t, dir2, clk, 1, ev1)
	msg2 := expectedSendMessage(t, dir2, clk, 2, ev2)
	exe, _ := installFakeSoho(t,
		fakesoho.Rule{Argv: sendArgv(notifyTestOwner, msg1), Code: 0, Stdout: "sent to ref\n"},
		fakesoho.Rule{Argv: sendArgv(notifyTestOwner, msg2), Code: 17},
	)
	setSohoConfig(t, dir2, exe, "machine-a")
	vars := map[string]string{jobapi.EnvJobID: notifyTestJob}
	if _, _, exit := runNotifyFlow(t, dir2, clk, vars, ev1, "wake"); exit != 0 {
		t.Fatal("wake exit != 0")
	}
	if _, _, exit := runNotifyFlow(t, dir2, clk, vars, ev2, "wake"); exit != 0 {
		t.Fatal("wake exit != 0")
	}
	led := readLedger(t, dir2)
	if len(led.Notifications) != 2 {
		t.Fatalf("ledger has %d notifications, want 2", len(led.Notifications))
	}
	acceptedID, pendingID := "", ""
	for _, n := range led.Notifications {
		// Route order: the owner first, the unregistered orchestrator as a
		// no_route delivery second.
		if len(n.Deliveries) != 2 {
			t.Fatalf("notification %s has %d deliveries, want 2", n.ID, len(n.Deliveries))
		}
		if n.Deliveries[1].State != notify.StateNoRoute {
			t.Fatalf("notification %s orchestrator delivery = %+v, want no_route", n.ID, n.Deliveries[1])
		}
		if n.Deliveries[0].State == notify.StateAccepted {
			acceptedID = n.ID
		}
		if n.Deliveries[0].State == notify.StatePending {
			pendingID = n.ID
		}
	}
	if acceptedID == "" || pendingID == "" {
		t.Fatalf("ledger states = %v %v, want one accepted and one pending", acceptedID, pendingID)
	}
	// The pending one is not ack-able: not found.
	stdout, _, exit = runNotifyFlow(t, dir2, clk, map[string]string{}, "", "notify", "ack", pendingID, "--role", "owner")
	if exit != 3 || stdout != `{"status":"not_found"}`+"\n" {
		t.Fatalf("ack pending: exit %d stdout %q, want 3 not_found", exit, stdout)
	}
	// The accepted one is acked twice; the line is the same both times.
	for i := 0; i < 2; i++ {
		stdout, _, exit = runNotifyFlow(t, dir2, clk, map[string]string{}, "", "notify", "ack", acceptedID, "--role", "owner")
		if exit != 0 || stdout != `{"acked":true}`+"\n" {
			t.Fatalf("ack %d: exit %d stdout %q, want {\"acked\":true}", i+1, exit, stdout)
		}
	}
	// The ack timestamp is recorded and kept (the second ack changed
	// nothing).
	led2 := readLedger(t, dir2)
	for _, n := range led2.Notifications {
		if n.ID != acceptedID {
			continue
		}
		if n.Deliveries[0].AckAt == "" {
			t.Fatal("the ack timestamp is missing")
		}
	}
	// A role the recipient does not hold is not found.
	stdout, _, exit = runNotifyFlow(t, dir2, clk, map[string]string{}, "", "notify", "ack", acceptedID, "--role", "orchestrator")
	if exit != 3 || stdout != `{"status":"not_found"}`+"\n" {
		t.Fatalf("ack wrong role: exit %d stdout %q, want 3 not_found", exit, stdout)
	}
	// NOWRITE: the writing subcommand refuses.
	stdout, _, exit = runNotifyFlowNowrite(t, dir, clk, map[string]string{}, "",
		"notify", "ack", acceptedID, "--role", "owner")
	if exit != 2 || stdout != notifyNowriteOut {
		t.Fatalf("ack under NOWRITE: exit %d stdout %q", exit, stdout)
	}
}

// TestNotifyNowriteReadWrite splits the gating: list and status run
// under HERDR_HERMES_NOWRITE=1 and write nothing; every other
// subcommand is refused before any side effect.
func TestNotifyNowriteReadWrite(t *testing.T) {
	clk := newTestClock()
	// list and status under NOWRITE on a fresh dir: exit 0, nothing
	// created.
	for _, args := range [][]string{
		{"notify", "list"},
		{"notify", "status"},
	} {
		dir := t.TempDir()
		stdout, _, exit := runNotifyFlowNowrite(t, dir, clk, map[string]string{}, "", args...)
		if exit != 0 {
			t.Fatalf("%v under NOWRITE: exit %d stdout %q, want 0", args, exit, stdout)
		}
		if stateDirExists(t, dir) {
			t.Fatalf("%v under NOWRITE created the state dir", args)
		}
	}
	// status --id on an unknown id still reports not found (read-only).
	dir := t.TempDir()
	stdout, _, exit := runNotifyFlowNowrite(t, dir, clk, map[string]string{}, "",
		"notify", "status", "--id", "n"+strings.Repeat("0", 20))
	if exit != 3 || stdout != `{"status":"not_found"}`+"\n" {
		t.Fatalf("status --id under NOWRITE: exit %d stdout %q, want 3 not_found", exit, stdout)
	}
	if stateDirExists(t, dir) {
		t.Fatal("status --id under NOWRITE created the state dir")
	}
	// Every writing subcommand is refused before any side effect.
	for _, args := range [][]string{
		{"notify", "register", "owner", "--projeto", notifyTestRepo, "--to", notifyTestOwner},
		{"notify", "unregister", "coordinator"},
		{"notify", "deliver"},
		{"notify", "ingest", "agent-status"},
		{"notify", "raise", "--class", "stuck"},
		{"notify", "ack", "n" + strings.Repeat("0", 20), "--role", "owner"},
	} {
		dir := t.TempDir()
		stdout, _, exit := runNotifyFlowNowrite(t, dir, clk, map[string]string{}, "", args...)
		if exit != 2 || stdout != notifyNowriteOut {
			t.Fatalf("%v under NOWRITE: exit %d stdout %q, want the nowrite refusal", args, exit, stdout)
		}
		if stateDirExists(t, dir) {
			t.Fatalf("%v under NOWRITE created the state dir", args)
		}
	}
	// A bare `notify` under NOWRITE still prints the usage error.
	stdout, _, exit = runNotifyFlowNowrite(t, t.TempDir(), clk, map[string]string{}, "", "notify")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Fatalf("bare notify under NOWRITE: exit %d stdout %q", exit, stdout)
	}
}
