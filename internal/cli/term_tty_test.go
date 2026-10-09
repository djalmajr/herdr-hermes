//go:build darwin || linux

package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// ptyHelperMarker is the exact argv marker that routes the re-exec'd test
// binary into ptyHelper instead of the test run.
const ptyHelperMarker = "-herdr-hermes-pty-helper"

func init() {
	testMainHook = func() {
		// argv: <test binary> -herdr-hermes-pty-helper <mode> <config dir or ->
		if len(os.Args) > 1 && os.Args[1] == ptyHelperMarker {
			code := 2
			if len(os.Args) >= 4 {
				code = ptyHelper(os.Args[2], os.Args[3])
			}
			os.Exit(code)
		}
	}
}

// These tests run the real production code on a real terminal. The pty is
// allocated in-process by openPty, and the test binary re-executes itself
// with the slave as stdin and controlling terminal; the argv marker
// routes the child into ptyHelper instead of the test run. No shell and
// no environment variable carry anything to the child.

// ptyHelper runs inside the re-exec'd test binary (stdin is a pty slave,
// stdout and stderr are pipes). It returns the process exit code.
func ptyHelper(mode, dir string) int {
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

// ptyRunResult carries everything observable from one bounded pty child:
// the child's pipe output, everything the pty line discipline returned on
// the master (where an echo leak would appear), and the Wait result.
type ptyRunResult struct {
	stdout  string
	stderr  string
	master  string
	waitErr error
}

// runPtyChild re-executes the test binary as ptyHelper with a pty slave as
// stdin and controlling terminal. When readyLine is non-empty it waits for
// a child stdout line containing it and then writes feed to the master.
// The 20 s context deadline, the 2 s WaitDelay and the cleanup (cancel and
// close the pty ends) bound every step, so a failure never leaves the
// child running.
func runPtyChild(t *testing.T, mode, dir, readyLine, feed string) ptyRunResult {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("test executable: %v", err)
	}
	master, slave, err := openPty()
	if err != nil {
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skipf("open pty: %v (this environment does not grant pty allocation, so the real-terminal coverage cannot run here)", err)
		}
		t.Fatalf("open pty: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(func() {
		cancel()
		master.Close()
		slave.Close()
	})
	cmd := exec.CommandContext(ctx, exe, ptyHelperMarker, mode, dir)
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = slave
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	// The slave is fd 0 in the child, so Ctty: 0 makes it the
	// controlling terminal.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start pty child: %v", err)
	}
	// The child holds its own copy of the slave; close the parent's so the
	// master sees EOF as soon as the child exits.
	slave.Close()

	var (
		mu      sync.Mutex
		stdoutB strings.Builder
		stderrB []byte
		masterB []byte
		readers sync.WaitGroup
	)
	ready := make(chan struct{}, 1)
	readers.Add(3)
	go func() {
		defer readers.Done()
		fed := false
		sc := bufio.NewScanner(stdoutPipe)
		for sc.Scan() {
			l := sc.Text()
			mu.Lock()
			stdoutB.WriteString(l + "\n")
			mu.Unlock()
			t.Logf("pty child: %s", l)
			if !fed && readyLine != "" && strings.Contains(l, readyLine) {
				fed = true
				ready <- struct{}{}
			}
		}
	}()
	go func() {
		defer readers.Done()
		b, _ := io.ReadAll(stderrPipe)
		mu.Lock()
		stderrB = b
		mu.Unlock()
	}()
	go func() {
		defer readers.Done()
		b, _ := io.ReadAll(master)
		mu.Lock()
		masterB = b
		mu.Unlock()
	}()
	if feed != "" {
		select {
		case <-ready:
			if _, err := master.Write([]byte(feed)); err != nil {
				t.Fatalf("write to the pty master: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for the pty child's ready line: %v", ctx.Err())
		}
	}
	waitErr := cmd.Wait()
	master.Close()
	readers.Wait()
	mu.Lock()
	defer mu.Unlock()
	return ptyRunResult{
		stdout:  stdoutB.String(),
		stderr:  string(stderrB),
		master:  string(masterB),
		waitErr: waitErr,
	}
}

// TestTermEchoToggle proves the real termios echo toggle on a real
// terminal: echo starts on, setTermEcho(false) clears it, and the restore
// puts it back.
func TestTermEchoToggle(t *testing.T) {
	res := runPtyChild(t, "toggle", "-", "", "")
	if res.waitErr != nil {
		t.Fatalf("pty child exit: %v (stdout %q stderr %q)", res.waitErr, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stdout, "initial-echo=true err=<nil>") {
		t.Fatalf("pty output %q, want echo on initially", res.stdout)
	}
	if !strings.Contains(res.stdout, "after-off=false err=<nil>") {
		t.Fatalf("pty output %q, want echo off after setTermEcho(false)", res.stdout)
	}
	if !strings.Contains(res.stdout, "restored=true err=<nil>") {
		t.Fatalf("pty output %q, want echo on after the restore", res.stdout)
	}
}

// TestAuthLoginTerminal runs the production `auth login --key -` on a real
// terminal: the prompt goes to stderr, the key is read with echo disabled
// (nothing is echoed back on the master), the credentials file is written,
// and the echo flag is restored afterwards.
func TestAuthLoginTerminal(t *testing.T) {
	dir := t.TempDir()
	res := runPtyChild(t, "login", dir, "ready", sentinelKey+"\n")
	if res.waitErr != nil {
		t.Fatalf("pty child exit: %v (stdout %q stderr %q)", res.waitErr, res.stdout, res.stderr)
	}
	if !strings.Contains(res.stdout, "initial-echo=true err=<nil>") {
		t.Fatalf("pty output %q, want echo on initially", res.stdout)
	}
	if !strings.Contains(res.stdout, "exit=0 out=\"{\\\"configured\\\":true,\\\"store\\\":\\\"file\\\"}\\n\"") {
		t.Fatalf("pty output %q, want the production login success line", res.stdout)
	}
	if strings.Contains(res.master, sentinelKey) {
		t.Fatalf("the pty master echoed the sentinel key back (leak): %d bytes from the master", len(res.master))
	}
	if strings.Contains(res.stdout, sentinelKey) || strings.Contains(res.stderr, sentinelKey) {
		t.Fatalf("a child pipe carried the sentinel key (leak)")
	}
	if !strings.Contains(res.stdout, "restored=true err=<nil>") {
		t.Fatalf("pty output %q, want echo restored after the read", res.stdout)
	}
	data, err := os.ReadFile(dir + "/credentials")
	if err != nil || string(data) != sentinelKey+"\n" {
		t.Fatalf("credentials = %q, %v; want the typed key", data, err)
	}
}
