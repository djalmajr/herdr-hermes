package soho_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/soho"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// installFake installs the fake herdr-soho in a temp dir and returns the
// executable path and the fake dir (for the call log).
func installFake(t *testing.T, rules ...fakesoho.Rule) (exe, dir string) {
	t.Helper()
	dir = t.TempDir()
	return fakesoho.Install(t, dir, rules...), dir
}

var childEnv = []string{"PATH=/bin", "SOHO_TEST=1"}

// waitDrain waits for r's most recent Run to finish delivering the
// child's output to the writers it was given before the test reads a
// buffered writer; the writers used here are plain buffers and never
// stall, so the delivery always completes.
func waitDrain(t *testing.T, r soho.Runner) {
	t.Helper()
	select {
	case <-r.DrainDone():
	case <-time.After(10 * time.Second):
		t.Fatalf("output delivery did not finish after Run")
	}
}

// TestSohoRunExitCodes: the runner returns the child exit code unchanged.
func TestSohoRunExitCodes(t *testing.T) {
	var rules []fakesoho.Rule
	for _, code := range []int{0, 3, 7, 20, 22, 24} {
		id := "J" + strconv.Itoa(code)
		rules = append(rules, fakesoho.Rule{
			Argv:   []string{"job", "status", "--id", id},
			Code:   code,
			Stdout: strconv.Itoa(code) + "\n",
		})
	}
	exe, dir := installFake(t, rules...)
	r := soho.Runner{Bin: exe, Environ: childEnv}
	for _, code := range []int{0, 3, 7, 20, 22, 24} {
		var out bytes.Buffer
		id := "J" + strconv.Itoa(code)
		exit, err := r.Run(context.Background(), []string{"job", "status", "--id", id}, nil, &out, io.Discard, 10*time.Second)
		if err != nil {
			t.Fatalf("code %d: Run: %v", code, err)
		}
		waitDrain(t, r)
		if exit != code {
			t.Errorf("code %d: exit = %d", code, exit)
		}
		if out.String() != strconv.Itoa(code)+"\n" {
			t.Errorf("code %d: stdout = %q", code, out.String())
		}
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 6 {
		t.Fatalf("fake calls = %d, want 6", len(calls))
	}
}

// TestSohoRunStdioPassThrough: stdin, stdout and stderr are carried
// unchanged between the caller and the child.
func TestSohoRunStdioPassThrough(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "start", "--id", "J1", "--repo", "o/r"},
		Stdout: "child-out-σ",
		Stderr: "child-err-σ",
		Code:   5,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	var out, errb bytes.Buffer
	exit, err := r.Run(context.Background(),
		[]string{"job", "start", "--id", "J1", "--repo", "o/r"},
		bytes.NewBufferString("brief-body"), &out, &errb, 10*time.Second)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitDrain(t, r)
	if exit != 5 {
		t.Errorf("exit = %d, want 5", exit)
	}
	if out.String() != "child-out-σ" {
		t.Errorf("stdout = %q", out.String())
	}
	if errb.String() != "child-err-σ" {
		t.Errorf("stderr = %q", errb.String())
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if calls[0].Stdin != "brief-body" {
		t.Errorf("child stdin = %q, want %q", calls[0].Stdin, "brief-body")
	}
}

// TestSohoRunChildEnv: the child sees exactly the environment the runner
// was given, nothing more, nothing less.
func TestSohoRunChildEnv(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{
		Argv: []string{"capabilities", "--json"}, Code: 0,
	})
	r := soho.Runner{Bin: exe, Environ: []string{"A=1", "B=two words"}}
	var out bytes.Buffer
	if _, err := r.Run(context.Background(), []string{"capabilities", "--json"}, nil, &out, io.Discard, 10*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	env := calls[0].Env
	if runtime.GOOS == "windows" {
		// The Go runtime adds SYSTEMROOT to a child environment on
		// Windows when it is missing; that is the standard library's
		// behavior, not a leak, so it is ignored before the exact
		// comparison (which stays exact on every other OS).
		var kept []string
		for _, kv := range env {
			if !strings.HasPrefix(kv, "SYSTEMROOT=") {
				kept = append(kept, kv)
			}
		}
		env = kept
	}
	if len(env) != 2 || env[0] != "A=1" || env[1] != "B=two words" {
		t.Errorf("child env = %v, want [A=1 B=two words]", env)
	}
}

// TestSohoRunDeadline: a child that runs longer than the bound and
// ignores SIGTERM (the fake's default for a plain delay, installed
// before its first byte is written, so a slow start can never lose the
// race to the cancel) is ended by the runner's WaitDelay grace period
// (a kill on unix) or by the context watcher's kill on Windows; the
// runner reports ErrDeadline. The parent context is cancelled as soon
// as the fake's ready line reaches the stdout writer, and the elapsed
// time is measured from that cancel, so the assertion is independent of
// the fake's start time.
func TestSohoRunDeadline(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:        []string{"job", "wait", "--id", "J1"},
		StdoutFirst: true,
		Stdout:      "ready\n",
		Delay:       30000,
		Code:        0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	ready := make(chan struct{})
	w := &readyWriter{ready: ready}
	ctx, cancel := context.WithCancel(context.Background())
	// The watcher cancels the parent context as soon as the ready line
	// reaches the writer and hands the cancel time over through a
	// channel (the race-free handoff for the measurement below); a
	// fake that never prints fails the test cleanly instead of
	// hanging.
	cancelAt := make(chan time.Time, 1)
	go func() {
		select {
		case <-ready:
		case <-time.After(15 * time.Second):
			t.Errorf("the fake never printed the ready line within 15s")
		}
		cancelAt <- time.Now()
		cancel()
	}()
	_, err := r.Run(ctx, []string{"job", "wait", "--id", "J1"}, nil, w, io.Discard, 60*time.Second)
	elapsed := time.Since(<-cancelAt)
	var de *soho.ErrDeadline
	if !errors.As(err, &de) {
		t.Fatalf("Run: err = %v, want ErrDeadline", err)
	}
	if elapsed >= 10*time.Second {
		t.Errorf("ended %s after the cancel, beyond the WaitDelay grace period (5s) + margin", elapsed)
	}
	if runtime.GOOS != "windows" {
		// On unix the SIGTERM at the cancel is ignored by the fake
		// (installed before the ready line was written), so only the
		// 5s WaitDelay can end the run: the elapsed time must sit
		// close to the grace period, proving the kill came from the
		// grace period, not from the signal. Windows kills at the
		// cancel, so the lower bound is skipped there.
		if elapsed < 4*time.Second {
			t.Errorf("ended %s after the cancel, before the WaitDelay grace period (~5s): the SIGTERM, not the grace period, ended the run", elapsed)
		}
	}
}

// readyWriter closes ready on its first Write; the guarded writer
// delivers writes in order through one goroutine, so the first Write is
// the fake's ready line and the close runs at most once. Closing a
// channel (never sending a value) keeps the handoff to the cancelling
// goroutine race-free.
type readyWriter struct {
	ready  chan struct{}
	closed atomic.Bool
}

func (w *readyWriter) Write(p []byte) (int, error) {
	if w.closed.CompareAndSwap(false, true) {
		close(w.ready)
	}
	return len(p), nil
}

// holdWriter blocks its first Write until the test closes release, then
// records the bytes and closes done; later Writes just record. Blocking
// on a channel instead of sleeping keeps the stall deterministic: it
// outlives the 5s WaitDelay without racing any sleep against the
// timer.
type holdWriter struct {
	first   atomic.Bool
	release chan struct{}
	done    chan struct{}
	buf     bytes.Buffer
}

func newHoldWriter() *holdWriter {
	return &holdWriter{release: make(chan struct{}), done: make(chan struct{})}
}

func (w *holdWriter) Write(p []byte) (int, error) {
	if w.first.Swap(true) {
		return w.buf.Write(p)
	}
	<-w.release
	n, err := w.buf.Write(p)
	close(w.done)
	return n, err
}

// unblock releases the first Write; waitDone bounds the wait for it to
// finish into the writer. unblock must be called exactly once (from
// whichever point the test decides: after Run returned, or from a
// goroutine on its own schedule).
func (w *holdWriter) unblock() {
	close(w.release)
}

func (w *holdWriter) waitDone(t *testing.T) {
	t.Helper()
	select {
	case <-w.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the writer did not finish after release")
	}
}

// TestSohoRunNonZeroExitStdoutPastBound: the child exits 7 on its own,
// but its stdout is still being delivered (a stalled consumer blocks
// the first Write) when the bound expires. The runner must report the
// child's own exit code (7), not a deadline: the bound firing is not
// the child's cause of death. The writer is released from a goroutine
// after the bound has passed but before the 5s grace period expires, so
// the copy finishes first and the exit-7 path is decided from the
// child's own ExitError; if a loaded runner lets the grace period win
// instead, the same (7, nil) must hold from the process state.
func TestSohoRunNonZeroExitStdoutPastBound(t *testing.T) {
	exe, _ := installFake(t,
		fakesoho.Rule{Argv: []string{"job", "status", "--id", "J7-cal"}, Stdout: "done\n", Code: 7},
		fakesoho.Rule{Argv: []string{"job", "status", "--id", "J7"}, Stdout: "done\n", Code: 7, HoldPipeMs: 4000},
	)
	r := soho.Runner{Bin: exe, Environ: childEnv}
	argv := []string{"job", "status", "--id", "J7"}
	// Calibrate a plain exit (no pipe hold) on this machine so the
	// bound below sits after the child has already exited on its own.
	var cal bytes.Buffer
	calStart := time.Now()
	code, err := r.Run(context.Background(), []string{"job", "status", "--id", "J7-cal"}, nil, &cal, io.Discard, 30*time.Second)
	base := time.Since(calStart)
	waitDrain(t, r)
	if err != nil || code != 7 || cal.String() != "done\n" {
		t.Fatalf("calibrate: exit = %d, err = %v, out = %q, want (7, nil, \"done\\n\")", code, err, cal.String())
	}
	t.Logf("calibrate elapsed=%s", base)
	// bound = measured exit + 2s: after the child's own exit and before
	// the pipe holder gives up (4s), so the bound fires while the
	// copy is still draining. The release at bound + 1s stays before
	// the holder exits for any calibration under 1s.
	bound := base + 2*time.Second
	w := newHoldWriter()
	go func() {
		time.Sleep(bound + time.Second)
		close(w.release)
	}()
	start := time.Now()
	code, err = r.Run(context.Background(), argv, nil, w, io.Discard, bound)
	elapsed := time.Since(start)
	t.Logf("past bound: exit=%d err=%v elapsed=%s bound=%s", code, err, elapsed, bound)
	if err != nil || code != 7 {
		t.Fatalf("Run: exit = %d, err = %v, want (7, nil): the child exited 7 on its own", code, err)
	}
	if elapsed >= 10*time.Second {
		t.Fatalf("elapsed = %s, want well under the 10s ceiling", elapsed)
	}
	// The release already happened from the goroutine above; wait for
	// the in-flight write to finish into the writer.
	w.waitDone(t)
	if w.buf.String() != "done\n" {
		t.Errorf("stdout = %q, want \"done\\n\"", w.buf.String())
	}
}

// TestSohoRunZeroExitPastWaitDelay: the child exits 0 on its own, but
// its stdout stays blocked in the runner's stdout writer past the
// runner's WaitDelay grace period (the fake also holds the pipe open
// past the grace period, so the copy goroutine is still draining when
// it fires), and Wait reports the grace period expiring while the bound
// (60s) is far away. The runner must report the child's exit (0) and
// emit exactly one friction diagnostic, not a deadline — and must
// return while the writer is still blocked, because close never waits
// for the in-flight Write.
func TestSohoRunZeroExitPastWaitDelay(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:       []string{"job", "status", "--id", "J0"},
		Stdout:     "done\n",
		Code:       0,
		HoldPipeMs: 8000,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	var diags []string
	r.Diag = func(msg string) { diags = append(diags, msg) }
	argv := []string{"job", "status", "--id", "J0"}
	w := newHoldWriter()
	start := time.Now()
	code, err := r.Run(context.Background(), argv, nil, w, io.Discard, 60*time.Second)
	elapsed := time.Since(start)
	t.Logf("past WaitDelay: exit=%d err=%v diags=%d elapsed=%s", code, err, len(diags), elapsed)
	if err != nil || code != 0 {
		t.Fatalf("Run: exit = %d, err = %v, want (0, nil): the child exited 0 on its own", code, err)
	}
	// Run returned about one grace period after the child's fast exit,
	// while the writer was still blocked: the deadline is decided by
	// the grace period, not by the stalled consumer. The pipe holder
	// (8s) keeps the copy in flight past the 5s grace period, so the
	// grace period is what fired.
	if elapsed < 4*time.Second || elapsed >= 15*time.Second {
		t.Fatalf("elapsed = %s, want >= 4s and < 15s (about the 5s grace period)", elapsed)
	}
	if len(diags) != 1 {
		t.Fatalf("diagnostics = %d, want exactly 1: %v", len(diags), diags)
	}
	want := "herdr-soho job status exited 0 but its output did not drain within 5s; output may be truncated"
	if diags[0] != want {
		t.Errorf("diagnostic = %q, want %q", diags[0], want)
	}
	w.unblock()
	w.waitDone(t)
	if w.buf.String() != "done\n" {
		t.Errorf("stdout = %q, want \"done\\n\"", w.buf.String())
	}
}

// TestSohoRunDeadlineExitCode: a child killed at the bound is reported as
// ErrDeadline even when the kill ends in an ordinary exit code, exactly
// the Windows failure mode (TerminateProcess exits with code 1): the
// deadline is decided from the context, not from how the child died.
// The late child uses ExitOnTerm 1 so that on unix it exits 1 on
// SIGTERM, the same way a Windows child exits 1 on TerminateProcess;
// its bound (2s) is long enough that the fake installs its signal
// handler before the kill (a cold fake start can take a while). The
// control child that exits 1 on its own before the bound still returns
// 1 unchanged.
func TestSohoRunDeadlineExitCode(t *testing.T) {
	exe, _ := installFake(t,
		fakesoho.Rule{Argv: []string{"job", "wait", "--id", "late"}, Delay: 30000, Code: 1, ExitOnTerm: 1},
		fakesoho.Rule{Argv: []string{"job", "wait", "--id", "early"}, Code: 1},
	)
	r := soho.Runner{Bin: exe, Environ: childEnv}
	_, err := r.Run(context.Background(), []string{"job", "wait", "--id", "late"}, nil, io.Discard, io.Discard, 2*time.Second)
	var de *soho.ErrDeadline
	if !errors.As(err, &de) {
		t.Fatalf("deadline child: err = %v, want ErrDeadline", err)
	}
	code, err := r.Run(context.Background(), []string{"job", "wait", "--id", "early"}, nil, io.Discard, io.Discard, 10*time.Second)
	if err != nil || code != 1 {
		t.Fatalf("early child: exit = %d, err = %v, want (1, nil)", code, err)
	}
}

// TestSohoRunNotFound: a missing executable is a typed ErrUnavailable that
// wraps the LookPath error.
func TestSohoRunNotFound(t *testing.T) {
	r := soho.Runner{Bin: "no-such-herdr-soho-binary", Environ: childEnv}
	_, err := r.Run(context.Background(), []string{"job", "status", "--id", "J1"}, nil, io.Discard, io.Discard, 10*time.Second)
	var ue *soho.ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("Run: err = %v, want ErrUnavailable", err)
	}
	if !errors.Is(ue.Err, exec.ErrNotFound) {
		t.Errorf("cause = %v, want ErrNotFound", ue.Err)
	}
}

// TestSohoRunNotRunnable: an absolute path that exists but is not
// executable is reported as ErrUnavailable.
func TestSohoRunNotRunnable(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "herdr-soho")
	if err := os.WriteFile(bin, []byte("not executable\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	r := soho.Runner{Bin: bin, Environ: childEnv}
	_, err := r.Run(context.Background(), []string{"job", "status", "--id", "J1"}, nil, io.Discard, io.Discard, 10*time.Second)
	var ue *soho.ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("Run: err = %v, want ErrUnavailable", err)
	}
}

// TestSohoCapabilities: the capabilities client runs
// `capabilities --json` and parses the line leniently.
func TestSohoCapabilities(t *testing.T) {
	exe, dir := installFake(t, fakesoho.Rule{
		Argv:   []string{"capabilities", "--json"},
		Stdout: "{\"schema\":1,\"worker_collaboration\":1,\"ephemeral_job\":1,\"job_events\":1}\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	caps, err := r.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	want := "{\"schema\":1,\"worker_collaboration\":1,\"ephemeral_job\":1,\"job_events\":1}"
	if caps.Raw != want {
		t.Errorf("Raw = %q, want %q", caps.Raw, want)
	}
	if !caps.Has("ephemeral_job") || !caps.Has("job_events") {
		t.Errorf("caps %v: ephemeral_job and job_events must be present", caps.Raw)
	}
	if caps.Has("unknown_cap") {
		t.Errorf("caps %v: unknown capability reported present", caps.Raw)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 || len(calls[0].Argv) != 2 || calls[0].Argv[0] != "capabilities" || calls[0].Argv[1] != "--json" {
		t.Errorf("argv = %v, want [capabilities --json]", calls)
	}
}

// TestSohoCapabilitiesNonJSON: a non-JSON line is an error, not a silent
// capability set.
func TestSohoCapabilitiesNonJSON(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"capabilities", "--json"},
		Stdout: "not json\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Capabilities(context.Background()); err == nil {
		t.Error("Capabilities with non-JSON output: err = nil, want error")
	}
}

// TestSohoCapabilitiesNonZeroExit: a non-zero child exit is an error.
func TestSohoCapabilitiesNonZeroExit(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"capabilities", "--json"},
		Stdout: "{}\n",
		Code:   1,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Capabilities(context.Background()); err == nil {
		t.Error("Capabilities exit 1: err = nil, want error")
	}
}

// TestSohoEvents: the events reader runs `job events --id <id> --since
// <n>` without --wait and returns the raw lines plus the parsed trailer.
func TestSohoEvents(t *testing.T) {
	const stdout = `{"seq":4,"ts":"2026-01-01T00:00:04-03:00","tipo":"commit","resumo":"c4"}
{"seq":5,"ts":"2026-01-01T00:00:05-03:00","tipo":"push","resumo":"p5"}
{"eventos":"fim","ultimo_seq":5,"estado":"running"}
`
	exe, dir := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "J1", "--since", "3"},
		Stdout: stdout,
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	res, err := r.Events(context.Background(), "J1", 3)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(res.Lines) != 2 {
		t.Fatalf("lines = %d, want 2: %v", len(res.Lines), res.Lines)
	}
	if res.Lines[0] != `{"seq":4,"ts":"2026-01-01T00:00:04-03:00","tipo":"commit","resumo":"c4"}` {
		t.Errorf("line 1 = %q", res.Lines[0])
	}
	if res.Trailer.Eventos != "fim" || res.Trailer.UltimoSeq != 5 || res.Trailer.Estado != "running" {
		t.Errorf("trailer = %+v", res.Trailer)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	want := []string{"job", "events", "--id", "J1", "--since", "3"}
	if len(calls[0].Argv) != len(want) {
		t.Fatalf("argv = %v, want %v", calls[0].Argv, want)
	}
	for i := range want {
		if calls[0].Argv[i] != want[i] {
			t.Fatalf("argv = %v, want %v", calls[0].Argv, want)
		}
	}
	for _, a := range calls[0].Argv {
		if a == "--wait" {
			t.Errorf("argv carries --wait: %v", calls[0].Argv)
		}
	}
}

// TestSohoEventsNoNew: a --since beyond the last event yields only the
// trailer, no lines.
func TestSohoEventsNoNew(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "J1", "--since", "5"},
		Stdout: `{"eventos":"fim","ultimo_seq":5,"estado":"done"}` + "\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	res, err := r.Events(context.Background(), "J1", 5)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(res.Lines) != 0 || res.Trailer.UltimoSeq != 5 || res.Trailer.Estado != "done" {
		t.Errorf("res = %+v", res)
	}
}

// TestSohoEventsNoTrailer: output without the fim trailer line is an
// error.
func TestSohoEventsNoTrailer(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "J1", "--since", "0"},
		Stdout: `{"seq":1,"ts":"2026-01-01T00:00:01-03:00","tipo":"accepted","resumo":"a1"}` + "\n",
		Code:   0,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Events(context.Background(), "J1", 0); err == nil {
		t.Error("Events without trailer: err = nil, want error")
	}
}

// TestSohoEventsNonZeroExit: a non-zero child exit is an error.
func TestSohoEventsNonZeroExit(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   []string{"job", "events", "--id", "X", "--since", "0"},
		Stdout: `{"eventos":"fim","ultimo_seq":1,"estado":"done"}` + "\n",
		Code:   3,
	})
	r := soho.Runner{Bin: exe, Environ: childEnv}
	if _, err := r.Events(context.Background(), "X", 0); err == nil {
		t.Error("Events exit 3: err = nil, want error")
	}
}
