package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// fakeChildEnv is the controlled environment every test passes to the
// subprocesses (also asserted on in the fake call log).
var fakeChildEnv = []string{"PATH=/bin", "HERMES_CHILD=1"}

// installFakeSoho installs the fake herdr-soho and returns (exePath, dir).
func installFakeSoho(t *testing.T, rules ...fakesoho.Rule) (string, string) {
	t.Helper()
	dir := t.TempDir()
	return fakesoho.Install(t, dir, rules...), dir
}

// setSohoConfig writes the herdr-hermes config pointing herdr_soho_bin at
// the fake, with the given machine label ("" for none).
func setSohoConfig(t *testing.T, cfgDir, exe, label string) {
	t.Helper()
	if err := config.Set(cfgDir, "herdr_soho_bin", exe); err != nil {
		t.Fatalf("config.Set herdr_soho_bin: %v", err)
	}
	if label != "" {
		if err := config.Set(cfgDir, "machine_label", label); err != nil {
			t.Fatalf("config.Set machine_label: %v", err)
		}
	}
}

// runHermes runs Run with the test env (controlled child environment) and
// returns stdout, stderr and the exit code.
func runHermes(t *testing.T, cfgDir string, nowrite bool, stdin string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(stdin),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(nowrite),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// readOutboxRecords parses the outbox records of a state dir in seq order.
func readOutboxRecords(t *testing.T, cfgDir string) []outbox.Record {
	t.Helper()
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open outbox: %v", err)
	}
	lines, _, err := s.Read(0)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	var recs []outbox.Record
	for _, l := range lines {
		var r outbox.Record
		if err := json.Unmarshal(l, &r); err != nil {
			t.Fatalf("outbox line %q: %v", l, err)
		}
		recs = append(recs, r)
	}
	return recs
}

// jobTrackedAndRecorded reports whether the state dir tracks jobID and
// holds exactly one dispatch record for it: the coarse "bookkeeping
// finished" signal a bounded poll checks while the delivery is still in
// flight.
func jobTrackedAndRecorded(t *testing.T, cfgDir, jobID string) bool {
	t.Helper()
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		return false
	}
	jobs, err := s.LoadJobs()
	if err != nil || jobs.Jobs[jobID] == nil {
		return false
	}
	lines, _, err := s.Read(0)
	if err != nil {
		return false
	}
	count := 0
	for _, l := range lines {
		var r outbox.Record
		if json.Unmarshal(l, &r) != nil {
			continue
		}
		if r.Tipo == outbox.TipoDispatch && r.JobID != nil && *r.JobID == jobID {
			count++
		}
	}
	return count == 1
}

// seedJob adds one job to jobs.json.
func seedJob(t *testing.T, cfgDir string, id, projeto string, lastSeq int64, estado string) {
	t.Helper()
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	err = s.UpdateJobs(func(js *outbox.Jobs) error {
		js.Jobs[id] = &outbox.Job{ID: id, Projeto: projeto, LastEventSeq: lastSeq, Estado: estado}
		return nil
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
}

// jobTestSubs pairs every public sub with an exit code from the contract
// table (covering 0, 3, 7, 20, 22, 24) and distinct stdio payloads.
var jobTestSubs = []struct {
	sub    string
	args   []string
	code   int
	stdout string
	stderr string
}{
	{"start", []string{"--id", "J1", "--repo", "org/repo"}, 0, `{"id":"J1","status":"running"}` + "\n", "start-err"},
	{"status", []string{"--id", "J1"}, 3, `{"status":"not_found"}` + "\n", "status-err"},
	{"wait", []string{"--id", "J1"}, 7, `{"id":"J1","status":"blocked"}` + "\n", "wait-err"},
	{"events", []string{"--id", "J1"}, 20, `{"id":"J1","status":"conflict"}` + "\n", "events-err"},
	{"collect", []string{"--id", "J1"}, 22, `{"id":"J1","status":"preparation_failed"}` + "\n", "collect-err"},
	{"amend", []string{"--id", "J1", "-"}, 24, `{"id":"J1","status":"pending_ack"}` + "\n", "amend-err"},
	{"send", []string{"--id", "J1", "-"}, 0, `{"id":"J1","status":"delivered"}` + "\n", "send-err"},
	{"ack", []string{"--id", "J1", "--upto", "3"}, 0, `{"id":"J1","acked":3}` + "\n", "ack-err"},
	{"cancel", []string{"--id", "J1"}, 21, `{"id":"J1","status":"canceled"}` + "\n", "cancel-err"},
	{"close", []string{"--id", "J1"}, 0, `{"id":"J1","status":"closed"}` + "\n", "close-err"},
	{"list", nil, 0, `{"jobs":{}}` + "\n", "list-err"},
}

// TestJobForwardPassthrough: argv, stdin, stdout, stderr and the exit code
// pass through unchanged for every public sub.
func TestJobForwardPassthrough(t *testing.T) {
	var rules []fakesoho.Rule
	for _, tc := range jobTestSubs {
		rules = append(rules, fakesoho.Rule{
			Argv:       append([]string{"job", tc.sub}, tc.args...),
			ArgvPrefix: true,
			Stdout:     tc.stdout,
			Stderr:     tc.stderr,
			Code:       tc.code,
		})
	}
	exe, fakeDir := installFakeSoho(t, rules...)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	for _, tc := range jobTestSubs {
		args := append([]string{"job", tc.sub}, tc.args...)
		stdout, stderr, exit := runHermes(t, cfgDir, false, "body-σ", args...)
		if exit != tc.code {
			t.Errorf("%s: exit = %d, want %d (stdout %q)", tc.sub, exit, tc.code, stdout)
		}
		if stdout != tc.stdout {
			t.Errorf("%s: stdout = %q, want %q", tc.sub, stdout, tc.stdout)
		}
		if stderr != tc.stderr {
			t.Errorf("%s: stderr = %q, want %q", tc.sub, stderr, tc.stderr)
		}
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != len(jobTestSubs) {
		t.Fatalf("fake calls = %d, want %d", len(calls), len(jobTestSubs))
	}
	for i, tc := range jobTestSubs {
		want := append([]string{"job", tc.sub}, tc.args...)
		if len(calls[i].Argv) != len(want) {
			t.Errorf("%s: child argv = %v, want %v", tc.sub, calls[i].Argv, want)
			continue
		}
		for j := range want {
			if calls[i].Argv[j] != want[j] {
				t.Errorf("%s: child argv = %v, want %v", tc.sub, calls[i].Argv, want)
				break
			}
		}
		if calls[i].Stdin != "body-σ" {
			t.Errorf("%s: child stdin = %q, want %q", tc.sub, calls[i].Stdin, "body-σ")
		}
		// No shell: the child is the fake itself, argv[0] is the first
		// forwarded argument.
		if calls[i].Argv[0] != "job" || (len(calls[i].Argv) > 1 && calls[i].Argv[1] != tc.sub) {
			t.Errorf("%s: child argv not direct: %v", tc.sub, calls[i].Argv)
		}
	}
}

// TestJobForwardStdinCaps: over-cap stdin for start/amend/send exits 2
// without running the subprocess; at-cap stdin is forwarded.
func TestJobForwardStdinCaps(t *testing.T) {
	caps := []struct {
		sub string
		cap int
	}{
		{"start", 256 * 1024},
		{"amend", 64 * 1024},
		{"send", 16 * 1024},
	}
	for _, tc := range caps {
		// At the cap: forwarded, the child sees the full body.
		exe, fakeDir := installFakeSoho(t, fakesoho.Rule{
			Argv: []string{"job", tc.sub, "--id", "J1"}, ArgvPrefix: true,
			Stdout: `{"id":"J1"}` + "\n", Code: 0,
		})
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		stdout, _, exit := runHermes(t, cfgDir, false, strings.Repeat("x", tc.cap), "job", tc.sub, "--id", "J1")
		if exit != 0 {
			t.Errorf("%s at cap: exit = %d, stdout %q", tc.sub, exit, stdout)
		}
		calls, err := fakesoho.ReadCalls(fakeDir)
		if err != nil {
			t.Fatalf("ReadCalls: %v", err)
		}
		if len(calls) != 1 || len(calls[0].Stdin) != tc.cap {
			t.Errorf("%s at cap: calls = %d, stdin len = %d, want %d", tc.sub, len(calls), len(calls[0].Stdin), tc.cap)
		}
		// One byte over: exit 2 and no subprocess at all.
		exe, fakeDir = installFakeSoho(t, fakesoho.Rule{
			Argv: []string{"job", tc.sub, "--id", "J1"}, ArgvPrefix: true,
			Stdout: `{"id":"J1"}` + "\n", Code: 0,
		})
		cfgDir = t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		stdout, _, exit = runHermes(t, cfgDir, false, strings.Repeat("x", tc.cap+1), "job", tc.sub, "--id", "J1")
		if exit != 2 {
			t.Errorf("%s over cap: exit = %d, want 2", tc.sub, exit)
		}
		if strings.TrimSpace(stdout) == "" || strings.Contains(stdout, `"id"`) {
			t.Errorf("%s over cap: stdout = %q, want the usage error only", tc.sub, stdout)
		}
		if _, err := os.Stat(fakesoho.LogPath(fakeDir)); !os.IsNotExist(err) {
			t.Errorf("%s over cap: the subprocess ran (log exists)", tc.sub)
		}
	}
	// Uncapped subs pass stdin through at any size (here: just over the
	// send cap, which means nothing for them).
	exe, fakeDir := installFakeSoho(t, fakesoho.Rule{
		Argv: []string{"job", "status", "--id", "J1"}, Stdout: `{"id":"J1"}` + "\n", Code: 0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	_, _, exit := runHermes(t, cfgDir, false, strings.Repeat("x", 20*1024), "job", "status", "--id", "J1")
	if exit != 0 {
		t.Errorf("status with 20KiB stdin: exit = %d", exit)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil || len(calls) != 1 || len(calls[0].Stdin) != 20*1024 {
		t.Errorf("status: stdin not passed through (calls %d, len %d)", len(calls), len(calls[0].Stdin))
	}
}

// TestJobForwardInternalSubs: supervise, checkpoint and note are refused
// with exit 2 without running anything.
func TestJobForwardInternalSubs(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, fakesoho.Rule{AnyArgs: true, Stdout: "should not run\n"})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	for _, sub := range []string{"supervise", "checkpoint", "note"} {
		stdout, _, exit := runHermes(t, cfgDir, false, "", "job", sub, "--id", "J1")
		if exit != 2 {
			t.Errorf("%s: exit = %d, want 2", sub, exit)
		}
		if strings.TrimSpace(stdout) != `{"status":"2","motivo":"job `+sub+` is an internal subcommand, not a dispatcher command"}` {
			t.Errorf("%s: stdout = %q", sub, stdout)
		}
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("fake was called %d times, want 0", len(calls))
	}
}

// TestJobForwardUnknownSub: an unknown sub and a bare `job` exit 2.
func TestJobForwardUnknownSub(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, fakesoho.Rule{AnyArgs: true})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	_, _, exit := runHermes(t, cfgDir, false, "", "job", "frobnicate")
	if exit != 2 {
		t.Errorf("unknown sub: exit = %d, want 2", exit)
	}
	_, _, exit = runHermes(t, cfgDir, false, "", "job")
	if exit != 2 {
		t.Errorf("bare job: exit = %d, want 2", exit)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("fake was called %d times, want 0", len(calls))
	}
}

// TestJobForwardValidation: bad --id/--repo/--base (both flag forms) and
// over-limit waits exit 2 without running the subprocess; valid values are
// forwarded.
func TestJobForwardValidation(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, fakesoho.Rule{
		AnyArgs: true, Stdout: `{"id":"J1"}` + "\n", Code: 0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	bad := [][]string{
		{"job", "start", "--id", "bad id!"},
		{"job", "start", "--id=bad id"},
		{"job", "start", "--id", strings.Repeat("a", 65)},
		{"job", "start", "--repo", "org"},
		{"job", "start", "--repo", "a/b/c"},
		{"job", "start", "--repo=org/bad_repo!"},
		{"job", "start", "--base", "../x"},
		{"job", "start", "--base=.."},
		{"job", "start", "--base", strings.Repeat("a/", 51)[:100] + "x"},
		{"job", "wait", "--id", "J1", "--timeout", "600001"},
		{"job", "wait", "--id", "J1", "--timeout=abc"},
		{"job", "wait", "--id", "J1", "--timeout", "-1"},
		{"job", "events", "--id", "J1", "--wait", "600001"},
		{"job", "events", "--id", "J1", "--wait=-1"},
	}
	for _, args := range bad {
		_, _, exit := runHermes(t, cfgDir, false, "", args...)
		if exit != 2 {
			t.Errorf("%v: exit = %d, want 2", args, exit)
		}
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("fake was called %d times on bad input, want 0", len(calls))
	}
	// Valid values in both flag forms are forwarded.
	_, _, exit := runHermes(t, cfgDir, false, "", "job", "start", "--id", "ok-id.1_-", "--repo=org/repo_1", "--base=feat/x.y")
	if exit != 0 {
		t.Errorf("valid start: exit = %d", exit)
	}
	_, _, exit = runHermes(t, cfgDir, false, "", "job", "wait", "--id", "J1", "--timeout=600000")
	if exit != 0 {
		t.Errorf("wait --timeout=600000: exit = %d", exit)
	}
	_, _, exit = runHermes(t, cfgDir, false, "", "job", "events", "--id", "J1", "--wait", "0")
	if exit != 0 {
		t.Errorf("events --wait 0: exit = %d", exit)
	}
	calls, err = fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 3 {
		t.Errorf("valid calls = %d, want 3", len(calls))
	}
}

// TestJobForwardMissingBinary: a missing herdr-soho exits 4 with the exact
// unavailable line.
func TestJobForwardMissingBinary(t *testing.T) {
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, "no-such-herdr-soho-binary-xyz", "machine-a")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "job", "status", "--id", "J1")
	if exit != 4 {
		t.Fatalf("exit = %d, want 4", exit)
	}
	if stdout != `{"status":"unavailable","motivo":"herdr-soho not found"}`+"\n" {
		t.Errorf("stdout = %q", stdout)
	}
}

// TestJobForwardBookkeepingStart: a successful start tracks the job and
// appends one dispatch record; a duplicate_of response for the tracked job
// adds no second job and no second record.
func TestJobForwardBookkeepingStart(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		fakesoho.Rule{
			Argv:   []string{"job", "start", "--id", "J1", "--repo", "org/repo"},
			Call:   1,
			Stdout: `{"id":"J1","status":"running","motivo":null}` + "\n",
			Code:   0,
		},
		fakesoho.Rule{
			Argv:   []string{"job", "start", "--id", "J1", "--repo", "org/repo"},
			Call:   2,
			Stdout: `{"id":"J1","duplicate_of":"J1","status":"running"}` + "\n",
			Code:   0,
		},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	stdout, _, exit := runHermes(t, cfgDir, false, "brief", "job", "start", "--id", "J1", "--repo", "org/repo")
	if exit != 0 {
		t.Fatalf("start: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"id":"J1","status":"running","motivo":null}`+"\n" {
		t.Fatalf("start stdout = %q", stdout)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	j := jobs.Jobs["J1"]
	if j == nil {
		t.Fatalf("job J1 not tracked: %+v", jobs)
	}
	if j.Projeto != "org/repo" || j.Estado != "running" || j.Closed {
		t.Errorf("job J1 = %+v", j)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	r := recs[0]
	if r.Tipo != outbox.TipoDispatch || r.Maquina != "machine-a" || r.Projeto != "org/repo" {
		t.Errorf("dispatch record = %+v", r)
	}
	if r.JobID == nil || *r.JobID != "J1" {
		t.Errorf("dispatch job_id = %v, want J1", r.JobID)
	}
	if string(r.Dados) != `{"id":"J1","status":"running","motivo":null}` {
		t.Errorf("dispatch dados = %s", r.Dados)
	}
	// The duplicate start: no second job, no second record, stdout and
	// exit unchanged.
	stdout, _, exit = runHermes(t, cfgDir, false, "brief", "job", "start", "--id", "J1", "--repo", "org/repo")
	if exit != 0 {
		t.Fatalf("duplicate start: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"id":"J1","duplicate_of":"J1","status":"running"}`+"\n" {
		t.Fatalf("duplicate stdout = %q", stdout)
	}
	jobs, err = s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if len(jobs.Jobs) != 1 {
		t.Errorf("jobs after duplicate = %d, want 1", len(jobs.Jobs))
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 1 {
		t.Errorf("records after duplicate = %d, want 1", len(recs))
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 2 {
		t.Errorf("fake calls = %d, want 2", len(calls))
	}
}

// TestJobForwardBookkeepingStartDuplicateUntracked: a duplicate_of
// response for a job not yet tracked (a crash window) tracks it and
// appends the dispatch record.
func TestJobForwardBookkeepingStartDuplicateUntracked(t *testing.T) {
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "start", "--id", "J2", "--repo", "org/repo"},
		Stdout: `{"id":"J2","duplicate_of":"J2","status":"blocked"}` + "\n",
		Code:   7,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	_, _, exit := runHermes(t, cfgDir, false, "brief", "job", "start", "--id", "J2", "--repo", "org/repo")
	if exit != 7 {
		t.Fatalf("exit = %d, want 7", exit)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J2"]; j == nil || j.Estado != "blocked" {
		t.Errorf("job J2 = %+v, want tracked with estado blocked", j)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 1 || recs[0].Tipo != outbox.TipoDispatch {
		t.Errorf("records = %+v, want one dispatch", recs)
	}
}

// TestJobForwardBookkeepingAmendSend: successful amend and send append one
// amend record each with the right refs.tipo; a failed amend appends none.
func TestJobForwardBookkeepingAmendSend(t *testing.T) {
	exe, _ := installFakeSoho(t,
		fakesoho.Rule{
			Argv:   []string{"job", "amend", "--id", "J1", "-"},
			Stdout: `{"id":"J1","delivered":true}` + "\n",
			Code:   0,
		},
		fakesoho.Rule{
			Argv:   []string{"job", "send", "--id", "J1", "-"},
			Stdout: `{"id":"J1","delivered":true,"note":true}` + "\n",
			Code:   0,
		},
		fakesoho.Rule{
			Argv:   []string{"job", "amend", "--id", "J404", "-"},
			Stdout: `{"status":"not_found"}` + "\n",
			Code:   3,
		},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 2, "blocked")
	if _, _, exit := runHermes(t, cfgDir, false, "fix the thing", "job", "amend", "--id", "J1", "-"); exit != 0 {
		t.Fatalf("amend: exit = %d", exit)
	}
	if _, _, exit := runHermes(t, cfgDir, false, "just a note", "job", "send", "--id", "J1", "-"); exit != 0 {
		t.Fatalf("send: exit = %d", exit)
	}
	if _, _, exit := runHermes(t, cfgDir, false, "fix again", "job", "amend", "--id", "J404", "-"); exit != 3 {
		t.Fatalf("amend J404: exit = %d, want 3", exit)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2: %+v", len(recs), recs)
	}
	for i, wantTipo := range []string{"amend", "nota"} {
		r := recs[i]
		if r.Tipo != outbox.TipoAmend || r.Maquina != "machine-a" || r.Projeto != "org/repo" {
			t.Errorf("record %d = %+v", i, r)
		}
		if r.JobID == nil || *r.JobID != "J1" {
			t.Errorf("record %d job_id = %v, want J1", i, r.JobID)
		}
		var dados struct {
			Refs struct {
				Tipo string `json:"tipo"`
			} `json:"refs"`
			Resposta json.RawMessage `json:"resposta"`
		}
		if err := json.Unmarshal(r.Dados, &dados); err != nil {
			t.Fatalf("record %d dados: %v", i, err)
		}
		if dados.Refs.Tipo != wantTipo {
			t.Errorf("record %d refs.tipo = %q, want %q", i, dados.Refs.Tipo, wantTipo)
		}
	}
	if string(recs[0].Dados) == "" || !strings.Contains(string(recs[0].Dados), `"resposta":{"id":"J1","delivered":true}`) {
		t.Errorf("record 0 resposta = %s", recs[0].Dados)
	}
}

// TestJobForwardBookkeepingClose: a successful close marks the job
// closed in jobs.json.
func TestJobForwardBookkeepingClose(t *testing.T) {
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "close", "--id", "J1"},
		Stdout: `{"id":"J1","status":"closed"}` + "\n",
		Code:   0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 3, "done")
	if _, _, exit := runHermes(t, cfgDir, false, "", "job", "close", "--id", "J1"); exit != 0 {
		t.Fatalf("close: exit = %d", exit)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || !j.Closed {
		t.Errorf("job J1 = %+v, want closed", j)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 0 {
		t.Errorf("close wrote %d records, want 0", len(recs))
	}
}

// TestJobForwardBookkeepingFailure: an uncreatable state dir and an empty
// machine_label leave the forwarded exit code and stdout unchanged; the
// failure goes to friction, not to the output.
func TestJobForwardBookkeepingFailure(t *testing.T) {
	// Uncreatable state: a regular file named "state" blocks the state
	// directory on every OS (a read-only config dir would only work where
	// directory mode changes are honored). The forward still succeeds.
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "start", "--id", "J1", "--repo", "org/repo"},
		Stdout: `{"id":"J1","status":"running"}` + "\n",
		Code:   0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	if err := os.WriteFile(filepath.Join(cfgDir, "state"), []byte("blocker"), 0o600); err != nil {
		t.Fatalf("write state blocker file: %v", err)
	}
	stdout, _, exit := runHermes(t, cfgDir, false, "brief", "job", "start", "--id", "J1", "--repo", "org/repo")
	if exit != 0 {
		t.Fatalf("start with unwritable state: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"id":"J1","status":"running"}`+"\n" {
		t.Errorf("stdout = %q, want the forwarded line unchanged", stdout)
	}
	if st, err := os.Stat(filepath.Join(cfgDir, "state")); err != nil || !st.Mode().IsRegular() {
		t.Fatalf("state = %v, %v; want the plain blocker file untouched", st, err)
	}
	// Empty machine_label: friction instead of the record, the forward is
	// unchanged.
	exe, _ = installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "amend", "--id", "J9", "-"},
		Stdout: `{"id":"J9","delivered":true}` + "\n",
		Code:   0,
	})
	cfgDir = t.TempDir()
	setSohoConfig(t, cfgDir, exe, "")
	seedJob(t, cfgDir, "J9", "org/repo", 1, "blocked")
	stdout, _, exit = runHermes(t, cfgDir, false, "fix", "job", "amend", "--id", "J9", "-")
	if exit != 0 {
		t.Fatalf("amend with empty label: exit = %d", exit)
	}
	if stdout != `{"id":"J9","delivered":true}`+"\n" {
		t.Errorf("stdout = %q, want the forwarded line unchanged", stdout)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 0 {
		t.Errorf("records with empty label = %d, want 0", len(recs))
	}
	logData, err := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
	if err != nil || !strings.Contains(string(logData), "machine_label") {
		t.Errorf("friction.log = %q, want the machine_label line", logData)
	}
}

// TestJobForwardBookkeepingStartDispatchRetry: a start whose dispatch
// record was lost (the job is tracked but the outbox holds no dispatch
// record for it) appends exactly one record on the next duplicate_of;
// a second duplicate_of appends none.
func TestJobForwardBookkeepingStartDispatchRetry(t *testing.T) {
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "start", "--id", "J1", "--repo", "org/repo"},
		Stdout: `{"id":"J1","duplicate_of":"J1","status":"running"}` + "\n",
		Code:   0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// Simulate the earlier append failure: the job is tracked in
	// jobs.json, the outbox has no dispatch record.
	seedJob(t, cfgDir, "J1", "org/repo", 0, "running")
	_, _, exit := runHermes(t, cfgDir, false, "brief", "job", "start", "--id", "J1", "--repo", "org/repo")
	if exit != 0 {
		t.Fatalf("first duplicate start: exit = %d", exit)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 1 {
		t.Fatalf("records after first duplicate = %d, want 1 (the lost record is appended): %+v", len(recs), recs)
	}
	if r := recs[0]; r.Tipo != outbox.TipoDispatch || r.JobID == nil || *r.JobID != "J1" {
		t.Errorf("record = %+v, want a dispatch record for J1", r)
	}
	// The second duplicate appends nothing.
	_, _, exit = runHermes(t, cfgDir, false, "brief", "job", "start", "--id", "J1", "--repo", "org/repo")
	if exit != 0 {
		t.Fatalf("second duplicate start: exit = %d", exit)
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 1 {
		t.Errorf("records after second duplicate = %d, want 1", len(recs))
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if len(jobs.Jobs) != 1 {
		t.Errorf("jobs = %d, want 1", len(jobs.Jobs))
	}
}

// TestJobForwardNotRunnable: an existing but not runnable herdr-soho
// prints the same unavailable line as a missing binary (POSIX; on
// Windows a non-executable file is still runnable, so skip there).
func TestJobForwardNotRunnable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("a non-executable file is only unrunnable on POSIX")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "herdr-soho")
	if err := os.WriteFile(bin, []byte("not executable\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, bin, "machine-a")
	stdout, _, exit := runHermes(t, cfgDir, false, "", "job", "status", "--id", "J1")
	if exit != 4 {
		t.Fatalf("exit = %d, want 4", exit)
	}
	if stdout != `{"status":"unavailable","motivo":"herdr-soho not found"}`+"\n" {
		t.Errorf("stdout = %q, want the single unavailable line", stdout)
	}
}

// TestJobForwardBookkeepingStartOmittedStatus: a start line that omits
// status and estado must not clear the tracked estado.
func TestJobForwardBookkeepingStartOmittedStatus(t *testing.T) {
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "start", "--id", "J1", "--repo", "org/repo"},
		Stdout: `{"id":"J1","duplicate_of":"J1"}` + "\n",
		Code:   0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	seedJob(t, cfgDir, "J1", "org/repo", 4, "done")
	_, _, exit := runHermes(t, cfgDir, false, "brief", "job", "start", "--id", "J1", "--repo", "org/repo")
	if exit != 0 {
		t.Fatalf("start: exit = %d", exit)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.Estado != "done" || j.LastEventSeq != 4 {
		t.Fatalf("job J1 = %+v, want estado done (absent status/estado cleared it), last 4", j)
	}
}

// TestJobForwardDeadlineTimeout: a child killed at the bound exits 4 and,
// when the child wrote nothing to stdout, prints exactly one timeout
// line; when the child already wrote, stdout keeps only the child's
// bytes (no timeout line) and the stderr diagnostic stays. Both rules
// use ExitOnTerm 1: with a plain delay the fake ignores SIGTERM, so
// each sub would take bound + the runner's WaitDelay (~5.5s) here;
// exiting 1 on SIGTERM keeps the test fast and mirrors the Windows
// failure mode. The WaitDelay grace period itself is locked in the
// soho package (TestSohoRunDeadline).
func TestJobForwardDeadlineTimeout(t *testing.T) {
	oldDefault, oldMargin := jobDefaultBound, jobBoundMargin
	jobDefaultBound, jobBoundMargin = 500*time.Millisecond, 500*time.Millisecond
	defer func() { jobDefaultBound, jobBoundMargin = oldDefault, oldMargin }()
	exe, _ := installFakeSoho(t,
		fakesoho.Rule{Argv: []string{"job", "status", "--id", "J1"}, Delay: 5000, Code: 0, ExitOnTerm: 1},
		fakesoho.Rule{Argv: []string{"job", "status", "--id", "J2"}, Stdout: `{"id":"J2"}` + "\n", StdoutFirst: true, Delay: 5000, Code: 0, ExitOnTerm: 1},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// No output before the bound: exit 4 and the exact single line.
	start := time.Now()
	stdout, stderr, exit := runHermes(t, cfgDir, false, "", "job", "status", "--id", "J1")
	elapsed := time.Since(start)
	if exit != 4 {
		t.Fatalf("no-output deadline: exit = %d, stdout %q, stderr %q", exit, stdout, stderr)
	}
	if want := `{"status":"timeout","motivo":"herdr-soho job status did not finish within 500ms"}` + "\n"; stdout != want {
		t.Errorf("no-output deadline stdout = %q, want %q", stdout, want)
	}
	if stderr == "" {
		t.Errorf("no-output deadline: stderr empty, want the diagnostic kept")
	}
	if elapsed >= 2*time.Second {
		t.Errorf("no-output deadline took %s, want well under 2s", elapsed)
	}
	// The child wrote a line before the bound: exit 4 and stdout
	// holds only that line (no timeout line). The rule's StdoutFirst
	// makes the fake print the line and only then sleep.
	stdout, stderr, exit = runHermes(t, cfgDir, false, "", "job", "status", "--id", "J2")
	if exit != 4 {
		t.Fatalf("output deadline: exit = %d, stdout %q, stderr %q", exit, stdout, stderr)
	}
	if stdout != `{"id":"J2"}`+"\n" {
		t.Errorf("output deadline stdout = %q, want only the child's line", stdout)
	}
	if stderr == "" {
		t.Errorf("output deadline: stderr empty, want the diagnostic kept")
	}
}

// holdFirstWrite blocks its first Write until the test closes release,
// then forwards every byte to w and closes done; it simulates a stdout
// consumer that stalls longer than the runner's WaitDelay grace period.
type holdFirstWrite struct {
	first   atomic.Bool
	release chan struct{}
	done    chan struct{}
	w       io.Writer
}

func (h *holdFirstWrite) Write(p []byte) (int, error) {
	if h.first.Swap(true) {
		return h.w.Write(p)
	}
	<-h.release
	n, err := h.w.Write(p)
	close(h.done)
	return n, err
}

// TestJobForwardStartSlowStdoutBookkeeping: a start whose stdout
// delivery stalls longer than the runner's WaitDelay grace period still
// exits 0 with exactly the child's stdout bytes; while the stdout
// writer is still held the job is tracked and exactly one dispatch
// record exists (the runner captured the start line into its own buffer
// before the slow consumer) and Run has NOT returned: the delivery of
// what the child wrote follows the consumer, so no exit code comes back
// before it. One friction line and one stderr diagnostic record the
// truncated output; the stderr line is written synchronously after the
// delivery, so it is there when Run returns (no waitFor).
func TestJobForwardStartSlowStdoutBookkeeping(t *testing.T) {
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:       []string{"job", "start", "--id", "J1", "--repo", "org/repo"},
		Stdout:     `{"id":"J1","status":"running"}` + "\n",
		Code:       0,
		HoldPipeMs: 8000,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	// The stdout writer blocks its first Write until the test releases
	// it, past the runner's 5s WaitDelay (the fake also holds the pipe
	// open past the grace period, so the copy is still draining when it
	// fires): the child's line is still in flight when Run returns, so
	// bookkeeping can only see it from the runner's own buffer (written
	// before the stalled consumer).
	var out bytes.Buffer
	errb := newLineBuffer()
	slowStdout := &holdFirstWrite{release: make(chan struct{}), done: make(chan struct{}), w: &out}
	env := Env{
		Stdin:     strings.NewReader("brief"),
		Stdout:    slowStdout,
		Stderr:    errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	done := make(chan int, 1)
	go func() { done <- Run([]string{"job", "start", "--id", "J1", "--repo", "org/repo"}, env) }()
	// The bookkeeping runs while the consumer is still stalled, so the
	// record and the job state can only have come from the buffer the
	// runner wrote first. Poll the state with a bounded deadline until
	// both exist; Run cannot have returned before the stalled writer is
	// released, so every poll also fails the test if it has.
	deadline := time.Now().Add(30 * time.Second)
	bookkept := false
	for !bookkept {
		select {
		case exit := <-done:
			t.Fatalf("Run returned %d while the stdout consumer was still stalled", exit)
		default:
		}
		bookkept = jobTrackedAndRecorded(t, cfgDir, "J1")
		if time.Now().After(deadline) {
			t.Fatalf("the job was not tracked and no dispatch record appeared within 30s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Give a forwarder that returned right after the bookkeeping (while
	// the consumer was still stalled) time to show up; the new policy
	// cannot return before the release.
	time.Sleep(500 * time.Millisecond)
	select {
	case exit := <-done:
		t.Fatalf("Run returned %d while the stdout consumer was still stalled", exit)
	default:
	}
	// Release the stalled consumer only now, and bounded-wait for the
	// in-flight write to finish into out and for Run to return.
	close(slowStdout.release)
	select {
	case <-slowStdout.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the stalled stdout writer did not finish after release")
	}
	select {
	case exit := <-done:
		if exit != 0 {
			t.Fatalf("start: exit = %d, want 0 (stdout %q, stderr %q)", exit, out.String(), errb.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return after the release")
	}
	if out.String() != `{"id":"J1","status":"running"}`+"\n" {
		t.Fatalf("stdout = %q, want exactly the child's line", out.String())
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.Projeto != "org/repo" || j.Estado != "running" || j.Closed {
		t.Errorf("job J1 not tracked correctly: %+v", jobs.Jobs)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	if recs[0].Tipo != outbox.TipoDispatch || recs[0].JobID == nil || *recs[0].JobID != "J1" {
		t.Errorf("dispatch record = %+v", recs[0])
	}
	if string(recs[0].Dados) != `{"id":"J1","status":"running"}` {
		t.Errorf("dispatch dados = %s", recs[0].Dados)
	}
	wantDiag := "herdr-soho job start exited 0 but its output did not drain within 5s; output may be truncated"
	fb, err := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
	if err != nil {
		t.Fatalf("friction.log: %v", err)
	}
	if n := strings.Count(string(fb), wantDiag); n != 1 {
		t.Errorf("friction.log = %q, want the WaitDelay diagnostic exactly once", fb)
	}
	// The stderr line is written synchronously after the delivery, so
	// it is here when Run returned: no waitFor.
	if n := strings.Count(errb.String(), "herdr-hermes: "+wantDiag); n != 1 {
		t.Errorf("stderr = %q, want the WaitDelay diagnostic exactly once", errb.String())
	}
}

// TestJobForwardNowrite: the writing subs refuse under NOWRITE without
// running anything; the read-only subs work and skip bookkeeping.
func TestJobForwardNowrite(t *testing.T) {
	exe, fakeDir := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "status", "--id", "J1"},
		Stdout: `{"id":"J1","status":"running"}` + "\n",
		Code:   0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	for _, sub := range []string{"start", "amend", "send", "ack", "cancel", "close"} {
		stdout, _, exit := runHermes(t, cfgDir, true, "", "job", sub, "--id", "J1")
		if exit != 2 {
			t.Errorf("%s under NOWRITE: exit = %d, want 2", sub, exit)
		}
		if stdout != nowriteErrorJSON+"\n" {
			t.Errorf("%s under NOWRITE: stdout = %q", sub, stdout)
		}
	}
	// status runs, the fake is called, and nothing is written.
	stdout, _, exit := runHermes(t, cfgDir, true, "", "job", "status", "--id", "J1")
	if exit != 0 {
		t.Fatalf("status under NOWRITE: exit = %d, stdout %q", exit, stdout)
	}
	if stdout != `{"id":"J1","status":"running"}`+"\n" {
		t.Errorf("status stdout = %q", stdout)
	}
	calls, err := fakesoho.ReadCalls(fakeDir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Errorf("fake calls = %d, want 1 (the status only)", len(calls))
	}
	if _, err := os.Stat(filepath.Join(cfgDir, "state")); !os.IsNotExist(err) {
		t.Errorf("state dir created under NOWRITE")
	}
}

// TestJobForwardStalledDeliveryResume: a stalled consumer holds the
// command for the whole stall: Run has not returned after a stall longer
// than 1 s and still not after an 11 s stall (longer than the 10 s
// delivery wait the old policy had); after the release it returns the
// child's exit with the exact stdout and the bookkeeping record, and
// the old "did not finish delivering" diagnostic is nowhere (stderr or
// friction.log) — the delivery simply followed the consumer.
func TestJobForwardStalledDeliveryResume(t *testing.T) {
	// Parallel: it holds a stalled consumer for seconds and changes no
	// package variable.
	t.Parallel()
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "start", "--id", "J1", "--repo", "org/repo"},
		Stdout: `{"id":"J1","status":"running"}` + "\n",
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	var out bytes.Buffer
	errb := newLineBuffer()
	slowStdout := &holdFirstWrite{release: make(chan struct{}), done: make(chan struct{}), w: &out}
	env := Env{
		Stdin:     strings.NewReader("brief"),
		Stdout:    slowStdout,
		Stderr:    errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	done := make(chan int, 1)
	go func() { done <- Run([]string{"job", "start", "--id", "J1", "--repo", "org/repo"}, env) }()
	notReturned := func(what string) {
		select {
		case exit := <-done:
			t.Fatalf("%s: Run returned %d while the stdout consumer was still stalled", what, exit)
		default:
		}
	}
	time.Sleep(1500 * time.Millisecond)
	notReturned("after a stall longer than 1 s")
	// Hold past the 10 s delivery wait the old policy had: under it Run
	// would have returned at the wait with a truncation diagnostic.
	time.Sleep(9500 * time.Millisecond)
	notReturned("after an 11 s stall")
	close(slowStdout.release)
	select {
	case <-slowStdout.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the stalled stdout writer did not finish after release")
	}
	select {
	case exit := <-done:
		if exit != 0 {
			t.Fatalf("start: exit = %d, want 0 (stderr %q)", exit, errb.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return after the release")
	}
	if out.String() != `{"id":"J1","status":"running"}`+"\n" {
		t.Fatalf("stdout = %q, want exactly the child's line", out.String())
	}
	if recs := readOutboxRecords(t, cfgDir); len(recs) != 1 || recs[0].Tipo != outbox.TipoDispatch {
		t.Fatalf("records = %+v, want one dispatch record", recs)
	}
	logData, rerr := os.ReadFile(filepath.Join(outbox.StateDir(cfgDir), "friction.log"))
	if rerr == nil && strings.Count(string(logData), "did not finish delivering") != 0 {
		t.Errorf("friction.log = %q, want no delivery diagnostic", logData)
	}
	if strings.Contains(errb.String(), "did not finish delivering") {
		t.Errorf("stderr = %q, want no delivery diagnostic", errb.String())
	}
}

// stallWriter blocks every Write until release is closed, standing for a
// consumer that stopped reading (a shared stdout and stderr pipe, for
// instance).
type stallWriter struct {
	release chan struct{}
	mu      sync.Mutex
	buf     bytes.Buffer
}

func (s *stallWriter) Write(p []byte) (int, error) {
	<-s.release
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *stallWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// lineBuffer is a race-free writer for the stderr of a forward whose
// stdout delivery runs on another goroutine.
type lineBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newLineBuffer() *lineBuffer { return &lineBuffer{} }

func (b *lineBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lineBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestJobForwardStalledSharedStreamReturns: when stdout and stderr are
// one stalled consumer, Run does not return while the consumer is
// stalled (the delivery of what the child wrote follows the consumer);
// after the release it returns the child's exit and the shared writer
// holds exactly the child's stdout and stderr bytes, no forwarder line.
func TestJobForwardStalledSharedStreamReturns(t *testing.T) {
	// Parallel: it holds a stalled consumer for seconds and changes no
	// package variable.
	t.Parallel()
	// Distinct byte classes: the two drain goroutines may interleave
	// chunks on the shared writer, so the test filters by class and by
	// total length.
	const childOut = "ssssssssss\n"
	const childErr = "eeeeee\n"
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:   []string{"job", "status", "--id", "J1"},
		Stdout: childOut,
		Stderr: childErr,
		Code:   7,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	stall := &stallWriter{release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-stall.release:
		default:
			close(stall.release)
		}
	})
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    stall,
		Stderr:    stall,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	done := make(chan int, 1)
	go func() { done <- Run([]string{"job", "status", "--id", "J1"}, env) }()
	time.Sleep(1500 * time.Millisecond)
	select {
	case exit := <-done:
		t.Fatalf("after a stall longer than 1 s: Run returned %d while the shared consumer was still stalled", exit)
	default:
	}
	// Hold past the 10 s delivery wait the old policy had, then release.
	time.Sleep(9500 * time.Millisecond)
	close(stall.release)
	select {
	case exit := <-done:
		if exit != 7 {
			t.Fatalf("status: exit = %d, want 7 (the child's own exit)", exit)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return after the release")
	}
	// The old policy wrote its diagnostic from a goroutine that races
	// the release; settle so a red run cannot read the writer between
	// the child's chunks and that line. Under the new policy the
	// delivery finished before Run returned, so nothing is still in
	// flight and the writer holds only the child's bytes.
	time.Sleep(300 * time.Millisecond)
	data := stall.String()
	if len(data) != len(childOut)+len(childErr) {
		t.Fatalf("shared writer = %q (%d bytes), want exactly the child's %d bytes", data, len(data), len(childOut)+len(childErr))
	}
	if n := strings.Count(data, "s"); n != 10 {
		t.Errorf("shared writer has %d 's' bytes, want 10 (the child's stdout)", n)
	}
	if n := strings.Count(data, "e"); n != 6 {
		t.Errorf("shared writer has %d 'e' bytes, want 6 (the child's stderr)", n)
	}
}

// TestJobForwardNowriteGraceDiagAfterDelivery: under NOWRITE a
// read-only forward whose grace period fired on a zero exit returns
// only after the stalled stdout consumer is released: the delivery of
// what the child wrote follows the consumer, the grace-period
// diagnostic is on stderr exactly once when Run returns (written
// synchronously after the delivery, so no waitFor), and the config dir
// holds nothing but the config file (no friction, no state dir).
func TestJobForwardNowriteGraceDiagAfterDelivery(t *testing.T) {
	// Parallel: it holds a stalled consumer for seconds and changes no
	// package variable.
	t.Parallel()
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:       []string{"job", "status", "--id", "J1"},
		Stdout:     `{"id":"J1","estado":"running"}` + "\n",
		HoldPipeMs: 8000,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	var out bytes.Buffer
	errb := newLineBuffer()
	slowStdout := &holdFirstWrite{release: make(chan struct{}), done: make(chan struct{}), w: &out}
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    slowStdout,
		Stderr:    errb,
		Getenv:    getenvFor(true),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	done := make(chan int, 1)
	go func() { done <- Run([]string{"job", "status", "--id", "J1"}, env) }()
	// Past the runner's 5 s grace period (the fake holds the pipe open
	// for 8 s, so the copy is still draining when the grace fires), but
	// the delivery is still stalled: Run must not be back yet.
	time.Sleep(6500 * time.Millisecond)
	select {
	case exit := <-done:
		t.Fatalf("Run returned %d while the stdout consumer was still stalled", exit)
	default:
	}
	close(slowStdout.release)
	select {
	case <-slowStdout.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the stalled stdout writer did not finish after release")
	}
	select {
	case exit := <-done:
		if exit != 0 {
			t.Fatalf("status: exit = %d, want 0 (stderr %q)", exit, errb.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return after the release")
	}
	if out.String() != `{"id":"J1","estado":"running"}`+"\n" {
		t.Fatalf("stdout = %q, want the complete child line", out.String())
	}
	wantDiag := "herdr-soho job status exited 0 but its output did not drain within 5s; output may be truncated"
	// The stderr line is written synchronously after the delivery, so
	// it is here when Run returned: no waitFor.
	if n := strings.Count(errb.String(), "did not drain within"); n != 1 {
		t.Fatalf("stderr = %q, want the grace-period diagnostic exactly once", errb.String())
	}
	if !strings.Contains(errb.String(), "herdr-hermes: "+wantDiag) {
		t.Fatalf("stderr = %q, want the diagnostic kept", errb.String())
	}
	// Under NOWRITE nothing is written to the config dir but the config
	// file itself: no state dir, no friction.
	if _, err := os.Stat(filepath.Join(cfgDir, "state")); !os.IsNotExist(err) {
		t.Fatalf("state dir under NOWRITE: stat err = %v, want not exist", err)
	}
	var files []string
	_ = filepath.Walk(cfgDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			files = append(files, path)
		}
		return nil
	})
	if len(files) != 1 || filepath.Base(files[0]) != "config" {
		t.Fatalf("config dir files = %v, want only the config file", files)
	}
}

// TestJobForwardFreshNodeGraceFriction: a fresh-node forward (no
// pre-created state dir, not NOWRITE) whose grace period fired on a
// zero exit leaves the state dir (mode 0700 on POSIX) and its
// friction.log (mode 0600 on POSIX) with the WaitDelay diagnostic line
// and one stderr line; the friction line carries the diagnostic only,
// never the child's output.
func TestJobForwardFreshNodeGraceFriction(t *testing.T) {
	const childOut = "child-stdout-marker-7f3a\n"
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv:       []string{"job", "status", "--id", "J1"},
		Stdout:     childOut,
		Code:       0,
		HoldPipeMs: 8000,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	stdout, stderr, exit := runHermes(t, cfgDir, false, "", "job", "status", "--id", "J1")
	if exit != 0 {
		t.Fatalf("status: exit = %d, want 0 (stdout %q, stderr %q)", exit, stdout, stderr)
	}
	if stdout != childOut {
		t.Fatalf("stdout = %q, want the child's line", stdout)
	}
	stateDir := outbox.StateDir(cfgDir)
	st, err := os.Stat(stateDir)
	if err != nil {
		t.Fatalf("state dir: %v, want it created", err)
	}
	if runtime.GOOS != "windows" && st.Mode().Perm() != 0o700 {
		t.Errorf("state dir mode = %o, want 700", st.Mode().Perm())
	}
	logPath := filepath.Join(stateDir, "friction.log")
	fst, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("friction.log: %v, want it created", err)
	}
	if runtime.GOOS != "windows" && fst.Mode().Perm() != 0o600 {
		t.Errorf("friction.log mode = %o, want 600", fst.Mode().Perm())
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read friction.log: %v", err)
	}
	wantDiag := "herdr-soho job status exited 0 but its output did not drain within 5s; output may be truncated"
	if n := strings.Count(string(data), wantDiag); n != 1 {
		t.Errorf("friction.log = %q, want the WaitDelay diagnostic exactly once", data)
	}
	if strings.Contains(string(data), "child-stdout-marker-7f3a") {
		t.Errorf("friction.log = %q, must not carry the child's output", data)
	}
	if n := strings.Count(stderr, "herdr-hermes: "+wantDiag); n != 1 {
		t.Errorf("stderr = %q, want the diagnostic line exactly once", stderr)
	}
}
