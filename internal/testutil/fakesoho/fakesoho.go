// Package fakesoho provides a re-executed Go test binary as a deterministic
// fake of the herdr-soho executable. It is configured without any
// environment variable: Install copies the test binary next to a JSON
// script, and the fake finds the script next to os.Executable(). Call
// Install from a test and Main first from the package TestMain.
package fakesoho

import (
	"context"
	"encoding/json"
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
)

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
	return exePath
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
	// Pipe-holder role: a copy of this binary started by a rule's
	// HoldPipeMs; it keeps the inherited stdout and stderr open for the
	// millisecond count in argv and exits. Checked first, before the
	// fake-role detection.
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
		if rule.StdoutFirst {
			// The stdout write goes straight to the pipe (os.File has
			// no user-space buffering), so the parent sees it before
			// the delay; then stderr and the exit.
			_, _ = os.Stdout.Write(stdoutData)
			sleepOrExit(rule, rule.Delay)
			_, _ = os.Stderr.Write(stderrData)
			os.Exit(rule.Code)
		}
		sleepOrExit(rule, rule.Delay)
		_, _ = os.Stdout.Write(stdoutData)
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

// sleepOrExit sleeps for ms milliseconds; if the rule requests an exit
// code on a terminal signal (ExitOnTerm non-zero) and the process is
// interrupted (os.Interrupt, plus SIGTERM where it exists) during the
// sleep, it exits with that code immediately. With ExitOnTerm zero the
// behavior is the plain sleep, which on unix ignores SIGTERM for the
// duration of the sleep: a runner deadline is then decided by its
// WaitDelay grace period (unix) or its Kill (Windows), not by the
// signal's default action.
func sleepOrExit(rule Rule, ms int) {
	if rule.ExitOnTerm == 0 {
		if ms > 0 {
			ignoreTermForSleep()
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, interruptSignals()...)
	defer signal.Stop(ch)
	select {
	case <-ch:
		os.Exit(rule.ExitOnTerm)
	case <-time.After(time.Duration(ms) * time.Millisecond):
	}
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
