//go:build !windows

package soho_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/proctree"
)

const (
	// sigPoll is the tick of the bounded polls the signal tests wait
	// on with: a deadline-bounded wait, the only readiness channel
	// across the process boundary.
	sigPoll = 20 * time.Millisecond
	// sigPollBound bounds the wait for the pid files and the ready
	// marker the fixture roles write.
	sigPollBound = 10 * time.Second
	// sigGoneBound bounds the wait for a fixture process to be
	// confirmed dead after the test ended it.
	sigGoneBound = 5 * time.Second
	// sigGrace mirrors the shutdown grace the relay gives the child's
	// group after the first forwarded signal: the same 5s the runner's
	// WaitDelay gives the cancel (the proctree grace). The test's
	// bounds are stated against it.
	sigGrace = 5 * time.Second
)

// sigProcessAlive reports whether pid is running: a kill with signal 0
// probes without touching the process, and ESRCH is the "gone" answer
// the regression asserts. Any other error is a probe failure,
// reported as such (never folded into "gone").
func sigProcessAlive(pid int) (bool, error) {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true, nil
	}
	if err == syscall.ESRCH {
		return false, nil
	}
	return false, err
}

// sigProcessGone waits with a bound (sigGoneBound) until pid is
// confirmed dead: the one race the assertion must not lose to is a
// just-killed process still visible as a zombie (its new parent reaps
// it moments later), and the bounded poll rides out that window
// instead of flaking on it.
func sigProcessGone(pid int) error {
	deadline := time.Now().Add(sigGoneBound)
	for {
		alive, err := sigProcessAlive(pid)
		if err != nil {
			return fmt.Errorf("probe pid %d: %w", pid, err)
		}
		if !alive {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pid %d is still alive after %s", pid, sigGoneBound)
		}
		time.Sleep(sigPoll)
	}
}

// sigWaitForPidFile returns the pid the fixture role wrote to pidfile,
// waiting with a bound (sigPollBound) and a bounded poll (sigPoll
// tick). It fails the test when the file does not hold a pid by the
// bound, with the state of the file for the next run.
func sigWaitForPidFile(t *testing.T, pidfile, what string) int {
	t.Helper()
	var last []byte
	var lastErr error
	deadline := time.Now().Add(sigPollBound)
	for {
		b, err := os.ReadFile(pidfile)
		if err == nil {
			last, lastErr = b, nil
			if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil && pid > 0 {
				return pid
			}
		} else {
			last, lastErr = nil, err
		}
		if time.Now().After(deadline) {
			t.Fatalf("the %s pid file %s did not hold a pid within %s (last: %q, %v): the fixture role was too slow", what, pidfile, sigPollBound, string(last), lastErr)
		}
		time.Sleep(sigPoll)
	}
}

// sigWaitForFile waits with a bound (sigPollBound) for path to exist:
// the ready marker the fixture role writes when its run ended.
func sigWaitForFile(t *testing.T, path string) error {
	t.Helper()
	deadline := time.Now().Add(sigPollBound)
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the file %s did not appear within %s", path, sigPollBound)
		}
		time.Sleep(sigPoll)
	}
}

// sigEndFixture ends one fixture process the test built, on success
// or on failure, if it is still alive: a SIGKILL to the pid, nothing
// broader (never by name, never a group the test did not build), and
// the bounded proof that it is gone. A process that is already gone
// is the success (the test's own assertions or the mechanism under
// test ended it).
func sigEndFixture(t *testing.T, pid int, what string) {
	t.Helper()
	alive, err := sigProcessAlive(pid)
	if err != nil {
		t.Errorf("cleanup: probe the %s %d: %v", what, pid, err)
		return
	}
	if !alive {
		return
	}
	syscall.Kill(pid, syscall.SIGKILL)
	if err := sigProcessGone(pid); err != nil {
		t.Errorf("cleanup: the %s %d the fixture ended is not gone: %v", what, pid, err)
	}
}

// sigEndFixturePidFile is the pid-file form of sigEndFixture: it ends
// the process the fixture role wrote to pidfile, if the file holds a
// pid and the process is still alive.
func sigEndFixturePidFile(t *testing.T, pidfile, what string) {
	t.Helper()
	b, err := os.ReadFile(pidfile)
	if err != nil {
		return
	}
	pid, perr := strconv.Atoi(strings.TrimSpace(string(b)))
	if perr != nil || pid <= 0 {
		return
	}
	sigEndFixture(t, pid, what)
}

// TestInterruptedRunWithResistantBackendEndsOnTheSignal is the Ctrl-C
// regression with a signal-resistant backend: the relay that only
// forwarded the signal and waited let a backend that ignores it hold
// the interrupted run for the whole bound. The first forwarded signal
// must start the shutdown grace instead: the child's group that
// survives it is forced down so the run's wait returns, and the run
// ends on the very signal. The fixture builds a helper in its own
// process group (the test's Prepare); the helper runs the production
// runner with a long bound on a backend that ignores SIGINT in its
// own process (inherited by the grandchild across the exec, the
// package's pattern), and the test sends SIGINT to the helper's group
// only — a group the test created.
func TestInterruptedRunWithResistantBackendEndsOnTheSignal(t *testing.T) {
	t.Run("single signal", func(t *testing.T) {
		runResistantInterrupt(t, false)
	})
	t.Run("second signal during the grace", func(t *testing.T) {
		runResistantInterrupt(t, true)
	})
}

// runResistantInterrupt builds the resistant fixture and interrupts
// the helper's group. With secondSignal a second SIGINT lands inside
// the first signal's grace: it is forwarded as well and does not
// restart the deadline, so the run still ends at the first signal's
// grace — the bound between the first signal's grace and a restarted
// one is what separates the two.
func runResistantInterrupt(t *testing.T, secondSignal bool) {
	t.Helper()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	backendPidFile := filepath.Join(dir, "backend.pid")
	grandPidFile := filepath.Join(dir, "grandchild.pid")
	var out, errOut bytes.Buffer
	cmd := exec.Command(exe, resistantRunnerMarker, dir)
	// The helper leads its own process group: the SIGINT the test
	// sends goes to this group only, and this group is the one the
	// test created — the way the terminal's Ctrl-C reaches only the
	// foreground process after the child left the group.
	proctree.Prepare(cmd)
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	// End the fixture processes the test built, on success or on
	// failure (a broken mechanism leaves them running as orphans):
	// only the pids the fixture created, never anything else.
	t.Cleanup(func() {
		sigEndFixture(t, cmd.Process.Pid, "helper")
		sigEndFixturePidFile(t, backendPidFile, "backend")
		sigEndFixturePidFile(t, grandPidFile, "grandchild")
	})
	backendPid := sigWaitForPidFile(t, backendPidFile, "backend")
	grandPid := sigWaitForPidFile(t, grandPidFile, "grandchild")
	// The control that the run really had a resistant tree to end:
	// the backend and the grandchild are alive before the signal.
	for _, fixture := range []struct {
		pid  int
		what string
	}{
		{backendPid, "backend"},
		{grandPid, "grandchild"},
	} {
		alive, aerr := sigProcessAlive(fixture.pid)
		if aerr != nil {
			t.Fatalf("probe the %s before the signal: %v", fixture.what, aerr)
		}
		if !alive {
			t.Fatalf("the %s %d was not alive before the signal: the run had no tree to end", fixture.what, fixture.pid)
		}
	}
	// The signal goes to the helper's group only (its id is the
	// helper's pid under Prepare's Setpgid): the test never signals a
	// group it did not create.
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGINT); err != nil {
		t.Fatalf("signal the helper's group: %v", err)
	}
	interruptAt := time.Now()
	if secondSignal {
		// The second SIGINT lands inside the first signal's grace:
		// it is forwarded as well and does not restart the deadline.
		time.Sleep(4 * time.Second)
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGINT); err != nil && err != syscall.ESRCH {
			t.Fatalf("signal the helper's group the second time: %v", err)
		}
	} else {
		// Inside the grace the resistant tree is still up: the
		// forwarded signal was ignored by the backend and, across the
		// exec, by the grandchild — the fixture really ignores the
		// terminal's signal, so only the grace can end the run.
		time.Sleep(time.Second)
		for _, fixture := range []struct {
			pid  int
			what string
		}{
			{backendPid, "backend"},
			{grandPid, "grandchild"},
		} {
			alive, aerr := sigProcessAlive(fixture.pid)
			if aerr != nil {
				t.Fatalf("probe the %s inside the grace: %v", fixture.what, aerr)
			}
			if !alive {
				t.Fatalf("the %s %d died inside the grace, before the shutdown grace expired: the fixture did not ignore the forwarded signal", fixture.what, fixture.pid)
			}
		}
	}
	// The bound: the single-signal run ends within the grace plus 3s;
	// the second-signal run ends within the bound between the first
	// signal's grace (sigGrace) and a grace restarted at the second
	// signal (4s + sigGrace).
	bound := sigGrace + 3*time.Second
	if secondSignal {
		bound = 6500 * time.Millisecond
	}
	var runErr error
	select {
	case runErr = <-waitErr:
	case <-time.After(bound):
		t.Fatalf("the helper did not end within %s of the first SIGINT to its group (helper stderr: %q): the resistant backend held the interrupted run past the shutdown grace", bound, errOut.String())
	}
	elapsed := time.Since(interruptAt)
	if runErr == nil {
		t.Fatalf("the helper exited 0 %s after the SIGINT, want the death on the signal (helper stderr: %q)", elapsed, errOut.String())
	}
	// The helper ended on the very signal the test sent to its group:
	// the relay's stop reported it and the production wiring re-raised
	// it on the helper itself.
	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("process state sys = %T, want syscall.WaitStatus", cmd.ProcessState.Sys())
	}
	if !ws.Signaled() || ws.Signal() != syscall.SIGINT {
		t.Fatalf("the helper died %v, want the death on SIGINT (helper stderr: %q)", ws, errOut.String())
	}
	// The lower bound is the control that the grace, not the
	// forwarded signal, ended the resistant tree: a tree that died on
	// the forwarded signal would let the run end well before the
	// grace.
	if elapsed < 4*time.Second {
		t.Fatalf("the helper ended %s after the SIGINT, want at least the shutdown grace minus margin: the resistant tree died on the forwarded signal, not the grace kill", elapsed)
	}
	// The relay reached the child's group and the grace ended it: the
	// backend and the grandchild (in the group, holding the pipe the
	// runner pointed at the backend) are both gone, with a bounded
	// wait.
	if err := sigProcessGone(backendPid); err != nil {
		t.Fatalf("the backend %d survived the interrupted run: %v", backendPid, err)
	}
	if err := sigProcessGone(grandPid); err != nil {
		t.Fatalf("the grandchild %d survived the interrupted run: %v", grandPid, err)
	}
}

// TestNoSignalRunStopsTheSignalReception is the guard of the no-signal
// path: after a run that received no signal, the relay's signal
// reception is stopped (the process is back to its normal signal
// behaviour), so nothing of the relay keeps consuming the terminal's
// signals after the run. The fixture builds a helper that runs the
// production runner on a child that exits 0 at once (no signal
// anywhere); after Run returned the helper holds, and the test sends
// SIGINT to the helper only — a process the test created. The helper
// must end on the very signal (the default action); a leftover signal
// reception would deliver the SIGINT to a channel nobody reads and
// keep the helper alive.
func TestNoSignalRunStopsTheSignalReception(t *testing.T) {
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	var out, errOut bytes.Buffer
	cmd := exec.Command(exe, quietRunnerMarker, dir)
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	t.Cleanup(func() {
		sigEndFixture(t, cmd.Process.Pid, "helper")
	})
	// The ready marker: the no-signal Run returned.
	if err := sigWaitForFile(t, filepath.Join(dir, "ready")); err != nil {
		t.Fatalf("the helper did not finish the no-signal run within %s (helper stderr: %q): %v", sigPollBound, errOut.String(), err)
	}
	// No signal reached the run: the SIGINT the test sends must end
	// the helper on the very signal (the default action restored once
	// the reception stopped), never survive it.
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGINT); err != nil {
		t.Fatalf("signal the helper: %v", err)
	}
	var runErr error
	select {
	case runErr = <-waitErr:
	case <-time.After(sigGoneBound):
		sigEndFixture(t, cmd.Process.Pid, "helper")
		<-waitErr
		t.Fatalf("the helper survived its SIGINT after the no-signal run (helper stderr: %q): the signal reception is still registered", errOut.String())
	}
	if runErr == nil {
		t.Fatalf("the helper exited 0 after its SIGINT, want the death on the signal (helper stderr: %q)", errOut.String())
	}
	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("process state sys = %T, want syscall.WaitStatus", cmd.ProcessState.Sys())
	}
	if !ws.Signaled() || ws.Signal() != syscall.SIGINT {
		t.Fatalf("the helper died %v, want the death on SIGINT (helper stderr: %q): a leftover reception would have consumed the signal", ws, errOut.String())
	}
}
