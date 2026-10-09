//go:build darwin || linux

package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// These tests run the real production code on a real terminal. The pty is
// allocated by the system "script" command (which this environment allows
// to create ptys even when ad-hoc binaries cannot), so the re-exec'd test
// binary gets a pty slave as its stdin and exercises the actual
// termios/console-mode toggle.
//
// The test binary re-executes itself under `script` with
// HERDR_HERMES_PTY_MODE set; TestMain routes that process into ptyHelper
// instead of the test run.

// ptyHelper runs inside the re-exec'd test binary (stdin is a pty slave).
// It returns the process exit code.
func ptyHelper(mode string) int {
	in := os.Stdin
	line := func(format string, args ...any) {
		fmt.Fprintf(os.Stdout, format+"\n", args...)
	}

	if mode == "toggle" {
		on, err := termEchoOn(in)
		line("initial-echo=%v err=%v", on, err)
		if err := setTermEcho(in, false); err != nil {
			line("off-err=%v", err)
			return 3
		}
		on, err = termEchoOn(in)
		line("after-off=%v err=%v", on, err)
		if err := setTermEcho(in, true); err != nil {
			line("restore-err=%v", err)
			return 4
		}
		on, err = termEchoOn(in)
		line("restored=%v err=%v", on, err)
		return 0
	}

	if mode == "login" {
		dir := os.Getenv("HERDR_HERMES_PTY_DIR")
		on, err := termEchoOn(in)
		line("initial-echo=%v err=%v", on, err)
		// Pre-disable echo (the same production call auth login makes)
		// before the key is typed, so nothing is echoed.
		if err := setTermEcho(in, false); err != nil {
			line("off-err=%v", err)
			return 3
		}
		line("ready")
		var outB, errB strings.Builder
		env := Env{
			Stdin:     in,
			Stdout:    &outB,
			Stderr:    &errB,
			Getenv:    os.Getenv,
			ConfigDir: dir,
		}
		exit := Run([]string{"auth", "login", "--key", "-"}, env)
		line("exit=%d out=%q err=%q", exit, outB.String(), errB.String())
		on, err = termEchoOn(in)
		line("restored=%v err=%v", on, err)
		return 0
	}

	line("unknown-mode=%v", mode)
	return 2
}

func TestMain(m *testing.M) {
	if mode := os.Getenv("HERDR_HERMES_PTY_MODE"); mode != "" {
		os.Exit(ptyHelper(mode))
	}
	os.Exit(m.Run())
}

// runUnderScript re-executes the test binary under the system `script` so
// its stdin is a real pty. It captures the child output, and once the child
// prints a line containing readyMarker it feeds stdinInput into the pty.
func runUnderScript(t *testing.T, mode, readyMarker, stdinInput string, extraEnv map[string]string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.Command("script", "-q", "/dev/null", exe)
	} else {
		cmd = exec.Command("script", "-q", "-c", `exec "$HERDR_HERMES_PTY_EXE"`, "/dev/null")
	}
	env := append(os.Environ(), "HERDR_HERMES_PTY_MODE="+mode, "HERDR_HERMES_PTY_EXE="+exe)
	for k, v := range extraEnv {
		env = append(env, k+"="+v)
	}
	cmd.Env = env
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			t.Skipf("the system `script` command is not available in this environment: %v", err)
		}
		t.Fatalf("start script: %v", err)
	}
	type done struct {
		out  string
		errs string
		werr error
	}
	ch := make(chan done, 1)
	go func() {
		var out strings.Builder
		feeded := false
		sc := bufio.NewScanner(outPipe)
		for sc.Scan() {
			l := sc.Text()
			out.WriteString(l + "\n")
			t.Logf("pty child: %s", l)
			if !feeded && readyMarker != "" && strings.Contains(l, readyMarker) && stdinInput != "" {
				if _, werr := stdinPipe.Write([]byte(stdinInput)); werr != nil {
					t.Errorf("feed pty stdin: %v", werr)
				}
				feeded = true
			}
		}
		var errB strings.Builder
		io.Copy(&errB, errPipe)
		werr := cmd.Wait()
		ch <- done{out.String(), errB.String(), werr}
	}()
	select {
	case d := <-ch:
		if d.werr != nil {
			t.Fatalf("pty child exit: %v (stderr %q, output %q)", d.werr, d.errs, d.out)
		}
		return d.out
	case <-time.After(20 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("timed out waiting for the pty child")
	}
	return ""
}

// TestTermEchoToggle proves the real termios/console echo toggle on a real
// terminal: echo starts on, setTermEcho(off) clears it, and the restore
// puts it back.
func TestTermEchoToggle(t *testing.T) {
	out := runUnderScript(t, "toggle", "", "", nil)
	if !strings.Contains(out, "initial-echo=true err=<nil>") {
		t.Fatalf("pty output %q, want echo on initially", out)
	}
	if !strings.Contains(out, "after-off=false err=<nil>") {
		t.Fatalf("pty output %q, want echo off after setTermEcho(false)", out)
	}
	if !strings.Contains(out, "restored=true err=<nil>") {
		t.Fatalf("pty output %q, want echo on after the restore", out)
	}
}

// TestAuthLoginTerminal runs the production `auth login --key -` on a real
// terminal: the prompt goes to stderr, the key is read with echo disabled
// (nothing is echoed back), the credentials file is written, and the echo
// flag is restored afterwards.
func TestAuthLoginTerminal(t *testing.T) {
	dir := t.TempDir()
	out := runUnderScript(t, "login", "ready", sentinelKey+"\n", map[string]string{"HERDR_HERMES_PTY_DIR": dir})
	if !strings.Contains(out, "initial-echo=true err=<nil>") {
		t.Fatalf("pty output %q, want echo on initially", out)
	}
	if !strings.Contains(out, "exit=0 out=\"{\\\"configured\\\":true,\\\"store\\\":\\\"file\\\"}\\n\"") {
		t.Fatalf("pty output %q, want the production login success line", out)
	}
	if strings.Contains(out, sentinelKey) {
		t.Fatalf("the pty echoed the key back (leak): %q", out)
	}
	if !strings.Contains(out, "restored=true err=<nil>") {
		t.Fatalf("pty output %q, want echo restored after the read", out)
	}
	data, err := os.ReadFile(dir + "/credentials")
	if err != nil || string(data) != sentinelKey+"\n" {
		t.Fatalf("credentials = %q, %v; want the typed key", data, err)
	}
}
