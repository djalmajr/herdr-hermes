//go:build !windows

package fakesoho

import (
	"bufio"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

// TestSignalAfterStartDelay: a SIGTERM that arrives after the rule's
// StartDelay wait has completed — the child left its first wait and is
// inside its second one — is still caught by the handler the rule
// installed before its first wait, so the fake exits with the ExitOnTerm
// code instead of the signal's default action (exit -1, "signal:
// terminated"). The probe that found the lost handler ran this same shape
// with StartDelay 100ms. The StartDelay 0 subtest is the control: its
// single wait is the first and the last, the behavior the fix must not
// change. Signals cannot be delivered across processes on Windows, so
// this test builds only on unix.
func TestSignalAfterStartDelay(t *testing.T) {
	// readyBound only bounds the precondition (the child started and
	// printed its line); the signal property is checked once the line is
	// out. A child that never prints within readyBound under load leaves
	// the property unexercised and the test skips with that evidence; a
	// child that exits before printing is a real failure.
	const readyBound = 30 * time.Second
	for _, delay := range []int{0, 100} {
		t.Run(time.Duration(delay).String(), func(t *testing.T) {
			// Delay is long so the SIGTERM always lands inside the
			// second wait, however slow the test is between reading the
			// line and signalling; a kept handler exits 7 at once and a
			// lost one dies at once from the default action.
			exe := Install(t, t.TempDir(), Rule{AnyArgs: true, StartDelay: delay, StdoutFirst: true, Stdout: "ready\n", Delay: 20000, ExitOnTerm: 7})
			cmd := exec.Command(exe)
			cmd.WaitDelay = time.Second
			pipe, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			type readResult struct {
				line string
				err  error
			}
			ready := make(chan readResult, 1)
			go func() {
				line, err := bufio.NewReader(pipe).ReadString('\n')
				ready <- readResult{line, err}
			}()
			select {
			case r := <-ready:
				if r.err != nil || r.line != "ready\n" {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
					t.Fatalf("ready: line %q, err %v after %s", r.line, r.err, time.Since(start))
				}
			case <-time.After(readyBound):
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Skipf("precondition not met: the child did not print its line within %s of its start (load); the signal property was not exercised", readyBound)
			}
			// The line is out: with StartDelay > 0 the child has just left
			// its first wait and is inside its second one.
			if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatal(err)
			}
			exited := make(chan struct{})
			go func() {
				_ = cmd.Wait()
				close(exited)
			}()
			select {
			case <-exited:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-exited
				t.Fatalf("the child did not exit within 10s of SIGTERM")
			}
			t.Logf("StartDelay=%d exit=%d", delay, cmd.ProcessState.ExitCode())
			if got := cmd.ProcessState.ExitCode(); got != 7 {
				t.Errorf("ExitOnTerm handler lost after StartDelay: want exit 7, got %d", got)
			}
		})
	}
}
