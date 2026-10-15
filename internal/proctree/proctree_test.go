package proctree_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/proctree"
)

const (
	// pidPoll is the tick of the bounded polls that wait on the pid
	// file the child role writes: a deadline-bounded wait on a file
	// marker, the only readiness channel across the process boundary
	// (the same idiom fakesoho's removal retry and the job forwarder's
	// bookkeeping poll use).
	pidPoll = 20 * time.Millisecond
	// pidFileBound bounds the wait for the pid file: a role that is not
	// up by the bound fails the test with the reason instead of
	// hanging.
	pidFileBound = 10 * time.Second
	// goneBound bounds the wait for a process to be confirmed dead
	// after the test ended it: an orphaned process is reaped by the
	// system soon after its killer, and a bound that is not met is a
	// failure with the pid, not a hang.
	goneBound = 5 * time.Second
	// waitBound bounds Wait after the cancel in the cancelled runs: a
	// run whose tree is not ended would still come back at its
	// WaitDelay, so the bound only guards against a hang, with a
	// failure that names what is still alive.
	waitBound = 10 * time.Second
)

// waitForPidFile returns the pid the child role wrote to pidfile,
// waiting with a bound (pidFileBound) and a bounded poll (pidPoll
// tick). It fails the test when the file does not hold a pid by the
// bound, with the state of the file for the next run.
func waitForPidFile(t *testing.T, pidfile string) int {
	t.Helper()
	var last []byte
	var lastErr error
	deadline := time.Now().Add(pidFileBound)
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
			t.Fatalf("the pid file %s did not hold a pid within %s (last: %q, %v): the child role was too slow to start the grandchild", pidfile, pidFileBound, string(last), lastErr)
		}
		time.Sleep(pidPoll)
	}
}

// cleanupTree registers the fixture cleanup: when the test ends, on
// success or on failure, it ends the child and the grandchild the test
// built if they are still alive, and records the outcome. A test that
// fails before its own assertions ended the fixture (a cancel test that
// red on the mechanism itself, the normal-exit case that red before its
// explicit kill) must not leave the fixture processes running; the
// cleanup only ends the pids the fixture created (the child from cmd
// and the grandchild from the pid file), never anything else.
func cleanupTree(t *testing.T, cmd *exec.Cmd, pidfile string) {
	t.Helper()
	t.Cleanup(func() {
		if cmd.Process != nil {
			endFixtureProcess(t, cmd.Process.Pid, "child")
		}
		if b, err := os.ReadFile(pidfile); err == nil {
			if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil && pid > 0 {
				endFixtureProcess(t, pid, "grandchild")
			}
		}
	})
}

// endFixtureProcess ends one fixture process if it is still alive and
// records the outcome: a process that is already gone is the success
// (the test's own assertions or the mechanism under test ended it), a
// process that survives the SIGKILL is a failure, named with its pid.
func endFixtureProcess(t *testing.T, pid int, what string) {
	t.Helper()
	alive, err := processAlive(pid)
	if err != nil {
		t.Errorf("cleanup: probe the %s %d: %v", what, pid, err)
		return
	}
	if !alive {
		return
	}
	killTree(pid)
	if err := processGone(pid); err != nil {
		t.Errorf("cleanup: the %s %d the fixture ended is not gone: %v", what, pid, err)
	}
}

// runCancelledTree runs the child role under a cancellable context,
// wired exactly the way the production callers wire it: Prepare before
// Start, the context cancel through proctree.Terminate into an
// interrupted flag, Attach right after Start, WaitDelay as given. It
// waits with a bound for the grandchild to be up (the pid file),
// asserts the grandchild is alive before the cancel (the control that
// the run really had a tree), cancels, waits for Wait with a bound, and
// applies the production post-Wait step (Kill when the cancel fired).
// It reports the grandchild pid, the elapsed time from the cancel to
// Wait's return, Wait's error, whether the cancel reached a live child
// and the cmd (for the assertions on how the child died).
func runCancelledTree(t *testing.T, dir string, roleArgs []string, waitDelay time.Duration) (pid int, elapsed time.Duration, err error, cancelled bool, cmd *exec.Cmd) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	pidfile := filepath.Join(dir, "grandchild.pid")
	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	argv := append([]string{roleArgs[0], pidfile}, roleArgs[1:]...)
	cmd = exec.CommandContext(ctx, exe, argv...)
	proctree.Prepare(cmd)
	cmd.Stdout = &out
	cmd.WaitDelay = waitDelay
	var interrupted atomic.Bool
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := proctree.Terminate(cmd); err != nil {
			return err
		}
		interrupted.Store(true)
		return nil
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cleanupTree(t, cmd, pidfile)
	release, err := proctree.Attach(cmd)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer release()
	pid = waitForPidFile(t, pidfile)
	alive, aerr := processAlive(pid)
	if aerr != nil {
		t.Fatalf("probe the grandchild before the cancel: %v", aerr)
	}
	if !alive {
		t.Fatalf("the grandchild %d was not alive before the cancel: the run had no tree to end", pid)
	}
	cancelAt := time.Now()
	cancel()
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case err = <-waitErr:
	case <-time.After(waitBound):
		t.Fatalf("Wait did not return within %s after the cancel (grandchild %d, waitDelay %s): the tree was not ended", waitBound, pid, waitDelay)
	}
	elapsed = time.Since(cancelAt)
	if interrupted.Load() {
		proctree.Kill(cmd)
	}
	return pid, elapsed, err, interrupted.Load(), cmd
}

// TestCancelTerminatesTheTree is the regression: a run cancelled while
// the child's tree is alive ends the whole tree, not only the direct
// child. The test binary plays the child and the grandchild (the
// TestMain markers): the child starts a grandchild that inherits the
// child's stdout (the pipe the test pointed the child at) and holds it
// open, and writes the grandchild's pid to the file the test polls.
// Before this change the cancel reached only the child: the call came
// back after the WaitDelay grace and the grandchild was still running.
func TestCancelTerminatesTheTree(t *testing.T) {
	dir := t.TempDir()
	pid, elapsed, err, cancelled, cmd := runCancelledTree(t, dir, []string{childHoldMarker}, 5*time.Second)
	if !cancelled {
		t.Fatalf("the cancel did not reach a live child (wait: %v): the run was not cancelled", err)
	}
	if err == nil {
		t.Fatalf("Wait = nil, want the child's death after the cancel")
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("the cancelled run ended %s after the cancel, want under 5s: the tree was not ended by the cancel", elapsed)
	}
	assertChildDiedOnTheCancel(t, cmd)
	if err := processGone(pid); err != nil {
		t.Fatalf("the grandchild %d is not gone after the cancelled run: %v", pid, err)
	}
}

// TestNormalExitLeavesDescendants is the control of the regression: a
// child that exits on its own, without any cancel, must leave the
// descendants it left on purpose running (a bounded command may leave
// legitimate work behind), and the test is the one that ends them.
func TestNormalExitLeavesDescendants(t *testing.T) {
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	pidfile := filepath.Join(dir, "grandchild.pid")
	var out bytes.Buffer
	cmd := exec.Command(exe, childExitMarker, pidfile)
	proctree.Prepare(cmd)
	cmd.Stdout = &out
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	cleanupTree(t, cmd, pidfile)
	release, err := proctree.Attach(cmd)
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer release()
	start := time.Now()
	err = cmd.Wait()
	elapsed := time.Since(start)
	// The grandchild held the pipe the child was pointed at, so the
	// WaitDelay grace period is what ended the wait: a path that
	// touched the tree on the normal exit would have let the wait end
	// right after the child's own exit.
	if !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("Wait = %v, want ErrWaitDelay (the grandchild held the child's stdout)", err)
	}
	if elapsed < 500*time.Millisecond {
		t.Fatalf("Wait ended %s after the start, want at least the 1s grace period minus the start: the pipe was released before the grace period", elapsed)
	}
	if st := cmd.ProcessState; st == nil || st.ExitCode() != 0 {
		t.Fatalf("exit = %v, want 0: the child exited on its own", st)
	}
	b, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatalf("read the pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		t.Fatalf("the pid file holds %q, want a pid", string(b))
	}
	// The grandchild the child left on purpose is still running...
	alive, aerr := processAlive(pid)
	if aerr != nil {
		t.Fatalf("probe: %v", aerr)
	}
	if !alive {
		t.Fatalf("the grandchild %d is not alive after the child's normal exit: the normal exit touched the tree", pid)
	}
	// ...and the test is the one that ends it, with the cleanup
	// proved (the probe says the process is gone, not just signalled).
	killTree(pid)
	if err := processGone(pid); err != nil {
		t.Fatalf("cleanup: the grandchild %d the test ended is not gone: %v", pid, err)
	}
}
