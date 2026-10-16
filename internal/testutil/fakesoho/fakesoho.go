// Package fakesoho provides a re-executed Go test binary as a deterministic
// fake of the herdr-soho executable. It is configured without any
// environment variable: Install copies the test binary next to a JSON
// script, and the fake finds the script next to os.Executable(). Call
// Install from a test and Main first from the package TestMain.
package fakesoho

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// exeBase is the executable base name the fake is installed as.
	exeBase = "herdr-soho"
	// scriptName is the script file installed next to the executable.
	scriptName = "fake-herdr-soho.json"
	// logName is the call log installed next to the executable.
	logName = "fake-herdr-soho.calls.jsonl"
	// pipeHolderMarker is the argv marker that routes the re-exec'd copy
	// into the pipe-holder role: it keeps the inherited stdout and
	// stderr open for the millisecond count in the next argument.
	pipeHolderMarker = "-fakesoho-pipe-holder"
	// warmMarker is the argv marker for Install's one-off warm run of the
	// installed copy: it makes the copy exit 0 at once, before any other
	// role is detected, so the slow first run of a fresh executable
	// happens outside the tests' measurements.
	warmMarker = "-fakesoho-warm"
	// lockHolderMarker is the argv marker that routes a re-exec'd copy of
	// the test binary into the lock-holder role: it takes the lock on the
	// next argument and holds it until it is killed, so a lock test can
	// exercise a process that dies while holding the lock.
	lockHolderMarker = "-fakesoho-lock-holder"
)

// lockWait bounds how long a contended lock waits for the lock to be
// free; a lock test may shorten it and restore it.
var lockWait = 5 * time.Second

// lockHolderUnlock keeps the lock-holder role's unlock closure (and the
// file behind it) reachable for the life of the re-exec'd process, so
// the file's finalizer cannot close the descriptor and drop the lock;
// the system releases the lock with the process.
var lockHolderUnlock func()

// Rule is one scripted response. Argv matches the process argv (the
// arguments after the executable), exactly or as a prefix; Call, when
// non-zero, matches only the rule's Nth call with that same argv.
type Rule struct {
	Argv        []string `json:"argv"`
	ArgvPrefix  bool     `json:"argv_prefix,omitempty"`
	AnyArgs     bool     `json:"any_args,omitempty"`
	Call        int      `json:"call,omitempty"`
	Stdout      string   `json:"stdout,omitempty"`
	StdoutBytes []byte   `json:"stdout_bytes,omitempty"`
	Stderr      string   `json:"stderr,omitempty"`
	StderrBytes []byte   `json:"stderr_bytes,omitempty"`
	Code        int      `json:"code,omitempty"`
	Delay       int      `json:"delay_ms,omitempty"`
	// StartDelay, when non-zero, is that many ms applied before anything is written: it models a slow process start.
	StartDelay int `json:"start_delay_ms,omitempty"`
	// StdoutFirst writes the stdout first, then applies the delay,
	// then the stderr and the exit; the default is delay, stdout,
	// stderr, exit.
	StdoutFirst bool `json:"stdout_first,omitempty"`
	// ExitOnTerm, when non-zero, makes the fake install a signal
	// handler (os.Interrupt and SIGTERM where it exists) before its
	// delay and exit with that code on a signal — a child killed at the
	// bound that still ends with an ordinary exit code, as a Windows
	// child does under TerminateProcess. Default behavior is unchanged.
	ExitOnTerm int `json:"exit_on_term,omitempty"`
	// WroteMarker, when non-empty, is a file path where the fake writes
	// the instant (time.Now().UnixNano() as text) at which it finished
	// writing its stdout, right after that write: a test that needs to
	// tell "the child wrote before the bound" from "the child was too
	// slow to start" reads the marker instead of guessing. With an empty
	// path nothing is written and the rule's order of effects is
	// unchanged.
	WroteMarker string `json:"wrote_marker,omitempty"`
	// HoldPipeMs, when non-zero, starts a copy of the fake before the
	// rule runs that keeps the inherited stdout and stderr open for that
	// many milliseconds: the pipes stay open (the pipe has no writer
	// left only when the holder exits) after the fake itself has
	// exited, so a runner's copy goroutine stays draining past the
	// child's own exit.
	HoldPipeMs int `json:"hold_pipe_ms,omitempty"`
}

// Call is one logged invocation: the argv, the full stdin and the full
// environment as it was seen by the child process.
type Call struct {
	Argv  []string `json:"argv"`
	Stdin string   `json:"stdin,omitempty"`
	Env   []string `json:"env,omitempty"`
}

type script struct {
	Log   string `json:"log"`
	Rules []Rule `json:"rules"`
}

// Install copies the test binary as <dir>/herdr-soho (herdr-soho.exe on
// Windows) and writes the rule script to <dir>/fake-herdr-soho.json. It
// returns the absolute executable path; tests point the herdr_soho_bin
// config key at it.
func Install(t testing.TB, dir string, rules ...Rule) (exePath string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("fakesoho: mkdir %s: %v", dir, err)
	}
	bin, err := os.Executable()
	if err != nil {
		t.Fatalf("fakesoho: executable: %v", err)
	}
	data, err := os.ReadFile(bin)
	if err != nil {
		t.Fatalf("fakesoho: read test binary: %v", err)
	}
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	exePath = filepath.Join(dir, exeBase+ext)
	// A copy, not a link: os.Executable must resolve to the fake on every
	// platform (on Linux a link would resolve back to the test binary).
	if err := os.WriteFile(exePath, data, 0o755); err != nil {
		t.Fatalf("fakesoho: write %s: %v", exePath, err)
	}
	b, err := json.Marshal(script{Log: filepath.Join(dir, logName), Rules: rules})
	if err != nil {
		t.Fatalf("fakesoho: marshal script: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, scriptName), b, 0o600); err != nil {
		t.Fatalf("fakesoho: write script: %v", err)
	}
	// Warm the installed copy once before returning its path: the first
	// run of a fresh executable can be slow (on a Windows node the
	// system scan of a new executable takes tens of seconds), and that
	// cost must not land inside a test's deadline measurement. The warm
	// run exits 0 without reading the script, stdin or the call log, so
	// it leaves no trace in the log.
	warmRun(t, exePath)
	if hold := maxHoldPipeMs(rules); hold > 0 {
		// A pipe holder runs a copy of the installed executable for up to
		// hold ms after the fake exits, and Windows refuses to delete a
		// running executable. Registered after the caller's TempDir, this
		// cleanup runs first and retries the removal until the holder has
		// exited, so the TempDir cleanup finds nothing left to fail on.
		t.Cleanup(func() { removeAllWithin(t, dir, time.Duration(hold)*time.Millisecond+10*time.Second) })
	}
	return exePath
}

// maxHoldPipeMs returns the longest HoldPipeMs among the rules.
func maxHoldPipeMs(rules []Rule) int {
	hold := 0
	for _, r := range rules {
		if r.HoldPipeMs > hold {
			hold = r.HoldPipeMs
		}
	}
	return hold
}

// warmRunOnce runs the installed copy once with the warm marker as its
// only argument, bounded at 120s with a 1s kill grace; it reports the
// exit code and the combined output in the error.
func warmRunOnce(exePath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	var out bytes.Buffer
	cmd := exec.CommandContext(ctx, exePath, warmMarker)
	cmd.WaitDelay = time.Second
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	code := -1
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	}
	if err != nil {
		return fmt.Errorf("warm run %s: %v, exit %d, output %q", exePath, err, code, out.String())
	}
	return nil
}

// busyExec reports whether the error is the busy-executable failure
// (syscall.ETXTBSY, "text file busy"): on Linux a parallel fork can
// inherit the write descriptor of the executable that was just copied
// (golang/go#22315), and the first exec of the fresh binary fails. The
// platform files install the check; on the operating systems where the
// condition does not occur it never matches and the retry never runs.
var busyExec func(error) bool = func(error) bool { return false }

// retryWarm runs run until it reports success, retrying a
// busy-executable failure (per busyExec) at most warmRetries attempts
// with a warmRetryPause pause between them; the pause is injected so
// tests keep real time out of the loop. Any other failure, or a busy
// failure after the last attempt, is returned.
const (
	// warmRetries is the number of warm-run attempts when the exec
	// fails with the busy-executable error.
	warmRetries = 5
	// warmRetryPause is the pause between warm-run attempts.
	warmRetryPause = 100 * time.Millisecond
)

func retryWarm(run func() error, pause func(context.Context, time.Duration) error) error {
	var err error
	for attempt := 1; attempt <= warmRetries; attempt++ {
		err = run()
		if err == nil || !busyExec(err) || attempt == warmRetries {
			return err
		}
		if perr := pause(context.Background(), warmRetryPause); perr != nil {
			return perr
		}
	}
	return err
}

// systemPause is the context-aware pause between warm-run retries.
func systemPause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// warmRun warms the installed copy once before Install returns its
// path: the first run of a fresh executable can be slow (on a Windows
// node the system scan of a new executable takes tens of seconds), and
// that cost must not land inside a test's deadline measurement. The
// warm run exits 0 without reading the script, stdin or the call log,
// so it leaves no trace in the log. The first exec of a just-copied
// binary can fail with the busy-executable error, so the run goes
// through retryWarm: at most warmRetries attempts, warmRetryPause
// between them, an ordinary failure at once.
func warmRun(t testing.TB, exePath string) {
	t.Helper()
	err := retryWarm(func() error { return warmRunOnce(exePath) }, systemPause)
	if err != nil {
		t.Fatalf("fakesoho: %v", err)
	}
}

// removeAllWithin removes dir, retrying while a file in it is still in use,
// for at most d.
func removeAllWithin(t testing.TB, dir string, d time.Duration) {
	deadline := time.Now().Add(d)
	for {
		err := os.RemoveAll(dir)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("fakesoho: remove %s: %v", dir, err)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ScriptPath returns the script file installed by Install in dir.
func ScriptPath(dir string) string {
	return filepath.Join(dir, scriptName)
}

// LogPath returns the call log file installed by Install in dir.
func LogPath(dir string) string {
	return filepath.Join(dir, logName)
}

// ReadCalls decodes the ordered JSONL call log installed in dir. A missing
// log file means no call happened.
func ReadCalls(dir string) ([]Call, error) {
	b, err := os.ReadFile(LogPath(dir))
	if err != nil {
		return nil, err
	}
	var calls []Call
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var c Call
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			return nil, err
		}
		calls = append(calls, c)
	}
	return calls, nil
}

// Main assumes the fake herdr-soho role when the process executable base
// name is herdr-soho and the script is installed next to it; otherwise it
// returns at once. Call it first from the package TestMain; it exits the
// re-executed process after dispatching the rule.
func Main() {
	// Warm run: Install runs the installed copy once with this marker so
	// the slow first run of a fresh executable happens outside the tests'
	// measurements. Checked first, before the pipe-holder marker and the
	// fake-role detection: the warm run exits 0 without reading the
	// script, stdin or the call log.
	if len(os.Args) == 2 && os.Args[1] == warmMarker {
		os.Exit(0)
	}
	// Lock-holder role: a copy of this test binary re-executed by a lock
	// test; it takes the lock on the path in argv, prints "locked" and
	// holds it until it is killed, so a test can exercise the lock a dead
	// process leaves behind (or does not). Checked before the pipe-holder
	// marker.
	if len(os.Args) == 3 && os.Args[1] == lockHolderMarker {
		// The closure is parked in the package variable so the file (and
		// its lock) stays open until the process is killed; a discarded
		// closure would let the file's finalizer close the descriptor and
		// drop the lock at once.
		var err error
		if lockHolderUnlock, err = lock(os.Args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "fakesoho: %v\n", err)
			os.Exit(126)
		}
		fmt.Fprintln(os.Stdout, "locked")
		// Hold the lock until the test kills this process; the system
		// releases it with the process. The sleeps keep a timer pending,
		// so the runtime's deadlock check (a main goroutine blocked
		// without a timer is a fatal "all goroutines are asleep" error
		// and would kill the holder, dropping the lock) does not fire.
		for {
			time.Sleep(time.Hour)
		}
	}
	// Pipe-holder role: a copy of this binary started by a rule's
	// HoldPipeMs; it keeps the inherited stdout and stderr open for the
	// millisecond count in argv and exits. Checked before the fake-role
	// detection.
	if len(os.Args) == 3 && os.Args[1] == pipeHolderMarker {
		if ms, err := strconv.Atoi(os.Args[2]); err == nil && ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
			os.Exit(0)
		}
	}
	exe, err := os.Executable()
	if err != nil {
		return
	}
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	if filepath.Base(exe) != exeBase && filepath.Base(exe) != exeBase+ext {
		return
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(exe), scriptName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakesoho: %v\n", err)
		os.Exit(126)
	}
	var cfg script
	if err := json.Unmarshal(b, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "fakesoho: %v\n", err)
		os.Exit(126)
	}
	// Always drain stdin: the parent waits for the copy to finish, and the
	// bridge caps or passes through a finite reader.
	stdin, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakesoho: read stdin: %v\n", err)
		os.Exit(126)
	}
	argv := os.Args[1:]
	call := Call{Argv: argv, Stdin: string(stdin), Env: os.Environ()}
	if err := logCall(cfg.Log, call); err != nil {
		fmt.Fprintf(os.Stderr, "fakesoho: %v\n", err)
		os.Exit(126)
	}
	callNumber := 0
	if calls, readErr := ReadCallsFromPath(cfg.Log); readErr == nil {
		for _, c := range calls {
			if same(argv, c.Argv) {
				callNumber++
			}
		}
	}
	for _, rule := range cfg.Rules {
		if !matches(argv, rule) || (rule.Call != 0 && rule.Call != callNumber) {
			continue
		}
		stdoutData := append([]byte(rule.Stdout), rule.StdoutBytes...)
		stderrData := append([]byte(rule.Stderr), rule.StderrBytes...)
		if rule.HoldPipeMs > 0 {
			spawnPipeHolder(rule.HoldPipeMs)
		}
		// The rule's signal behavior is installed before anything is
		// written, so a cancel that races the first byte already sees
		// it in place; the plain path's order of effects is unchanged
		// (nothing ran between the setup and the wait before either).
		sig := installRuleSignal(rule)
		// StartDelay runs under the same signal wait as the rule's
		// delay, so a bound that expires inside it ends the child the
		// same way one that expires inside the delay does; its wait is
		// never the last one, so it leaves the signal channel registered
		// for the rule's remaining wait.
		if rule.StartDelay > 0 {
			waitRuleSignal(rule, rule.StartDelay, sig, false)
		}
		if rule.StdoutFirst {
			// The stdout write goes straight to the pipe (os.File has
			// no user-space buffering), so the parent sees it before
			// the delay; then stderr and the exit.
			_, _ = os.Stdout.Write(stdoutData)
			writeWroteMarker(rule)
			waitRuleSignal(rule, rule.Delay, sig, true)
			_, _ = os.Stderr.Write(stderrData)
			os.Exit(rule.Code)
		}
		waitRuleSignal(rule, rule.Delay, sig, true)
		_, _ = os.Stdout.Write(stdoutData)
		writeWroteMarker(rule)
		_, _ = os.Stderr.Write(stderrData)
		os.Exit(rule.Code)
	}
	fmt.Fprintf(os.Stderr, "fakesoho: no rule for %v\n", argv)
	os.Exit(127)
}

// logCall appends one call line under a short-lived exclusive lock, so
// concurrent fake invocations never interleave half lines.
func logCall(logPath string, call Call) error {
	unlock, err := lock(logPath + ".lock")
	if err != nil {
		return err
	}
	defer unlock()
	line, err := json.Marshal(call)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(line, '\n'))
	return err
}

// ReadCallsFromPath decodes the ordered JSONL call log at file.
func ReadCallsFromPath(file string) ([]Call, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var calls []Call
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var c Call
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			return nil, err
		}
		calls = append(calls, c)
	}
	return calls, nil
}

// spawnPipeHolder starts a copy of this binary that keeps the inherited
// stdout and stderr open for ms milliseconds, so the pipes stay open
// after the fake itself exits. Start failures are ignored: the rule
// still runs, it just does not hold the pipes.
func spawnPipeHolder(ms int) {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	// The holder bounds itself (it sleeps exactly ms and exits); the fake
	// never blocks on it.
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ms)*time.Millisecond+5*time.Second)
	cmd := exec.CommandContext(ctx, exe, pipeHolderMarker, strconv.Itoa(ms))
	cmd.WaitDelay = time.Second
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		return
	}
	// Reap the holder and release the deadline; the fake's own exit is
	// not blocked by it.
	go func() {
		_ = cmd.Wait()
		cancel()
	}()
}

// installRuleSignal is the setup step of a rule's wait: with ExitOnTerm
// zero and a non-zero delay it makes the process ignore SIGTERM, so a
// runner deadline is decided by its WaitDelay grace period (unix) or its
// Kill (Windows), not by the signal's default action; with ExitOnTerm
// non-zero it installs the exit-on-signal handler and returns its
// channel. It is called before anything is written, so the behavior is
// in place before the first byte reaches the parent.
func installRuleSignal(rule Rule) chan os.Signal {
	if rule.ExitOnTerm == 0 {
		if rule.Delay > 0 {
			ignoreTermForSleep()
		}
		return nil
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, interruptSignals()...)
	return ch
}

// waitRuleSignal is the wait step of a rule's wait: it sleeps for ms
// milliseconds, or (when ExitOnTerm is non-zero and the handler
// installed by installRuleSignal fires) exits with that code on a
// terminal signal (os.Interrupt, plus SIGTERM where it exists). The
// channel is stopped only when last is true and the wait completes:
// the registration made once before the rule's first wait must stay in
// place across an earlier wait, so a finished wait cannot leave the
// channel dead for the rule's remaining ones.
func waitRuleSignal(rule Rule, ms int, ch chan os.Signal, last bool) {
	if rule.ExitOnTerm == 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return
	}
	if last {
		defer signal.Stop(ch)
	}
	select {
	case <-ch:
		os.Exit(rule.ExitOnTerm)
	case <-time.After(time.Duration(ms) * time.Millisecond):
	}
}

// writeWroteMarker records, at the rule's WroteMarker path, the instant
// the rule finished writing its stdout: time.Now().UnixNano() as text,
// written right after the stdout write. With an empty path it does
// nothing, so a rule that does not ask for the marker keeps its
// previous order of effects.
func writeWroteMarker(rule Rule) {
	if rule.WroteMarker == "" {
		return
	}
	_ = os.WriteFile(rule.WroteMarker, []byte(strconv.FormatInt(time.Now().UnixNano(), 10)), 0o600)
}

func matches(argv []string, rule Rule) bool {
	if rule.AnyArgs {
		return true
	}
	if rule.ArgvPrefix {
		return len(argv) >= len(rule.Argv) && same(argv[:len(rule.Argv)], rule.Argv)
	}
	return same(argv, rule.Argv)
}

func same(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
