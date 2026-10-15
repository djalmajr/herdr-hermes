//go:build !windows

package proctree_test

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/proctree"
)

// processAlive reports whether pid is running: kill with signal 0
// probes without touching the process, and ESRCH is the "gone" answer
// the regression asserts. Any other error is a probe failure, reported
// as such (never folded into "gone").
func processAlive(pid int) (bool, error) {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true, nil
	}
	if err == syscall.ESRCH {
		return false, nil
	}
	return false, err
}

// processGone waits with a bound (goneBound) until pid is confirmed
// dead: the one race the assertion must not lose to is a just-killed
// process still visible as a zombie (its new parent reaps it moments
// later), and the bounded poll rides out that window instead of
// flaking on it.
func processGone(pid int) error {
	deadline := time.Now().Add(goneBound)
	for {
		alive, err := processAlive(pid)
		if err != nil {
			return fmt.Errorf("probe pid %d: %w", pid, err)
		}
		if !alive {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pid %d is still alive after %s", pid, goneBound)
		}
		time.Sleep(pidPoll)
	}
}

// killTree ends the one process the test's own fixture created: a
// SIGKILL to the pid, nothing broader (never by name, never a group
// the test did not build).
func killTree(pid int) {
	syscall.Kill(pid, syscall.SIGKILL)
}

// armInheritedTermIgnore is the unix side of the hard fixture: it arms
// the SIGTERM ignore on the child before it execs the grandchild, and a
// SIG_IGN disposition is inherited across the exec and survives the
// grandchild's runtime start (verified by probe), so the grandchild
// ignores the cancel's SIGTERM from its first instruction, with no
// window. The child keeps the ignore: the standard library cannot
// restore the default action after an Ignore (signal.Reset only stops
// the relaying of the Notify), so the grace period, not the cancel's
// signal, ends the child in that case. The Notify installs the runtime
// handler first: without a prior Notify, Ignore leaves the signal's
// default action in place — the same setup fakesoho uses for its
// SIGTERM-ignoring rules.
func armInheritedTermIgnore() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	signal.Ignore(syscall.SIGTERM)
}

// assertChildDiedOnTheCancel asserts how the child died in a cancelled
// run: on the cancel's SIGTERM (the graceful step), not on the WaitDelay
// kill (SIGKILL) — which is what separates "the group signal reached
// the child" from "the grace period had to do it".
func assertChildDiedOnTheCancel(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	st := cmd.ProcessState
	if st == nil {
		t.Fatal("no process state: the child was not reaped")
	}
	ws, ok := st.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("process state sys = %T, want syscall.WaitStatus", st.Sys())
	}
	if sig := ws.Signal(); sig != syscall.SIGTERM {
		t.Fatalf("the child died on %v, want SIGTERM from the cancel's graceful step", sig)
	}
}

// assertChildDiedOnTheGrace is the straggler-fixture side of the
// assertion: there the child also ignores SIGTERM (the inherited
// ignore the fixture arms before the exec), so the child must die on
// the WaitDelay kill (SIGKILL at the grace period), which is also what
// separates "the grace period ended the run" from "the pipe released
// early".
func assertChildDiedOnTheGrace(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	st := cmd.ProcessState
	if st == nil {
		t.Fatal("no process state: the child was not reaped")
	}
	ws, ok := st.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("process state sys = %T, want syscall.WaitStatus", st.Sys())
	}
	if sig := ws.Signal(); sig != syscall.SIGKILL {
		t.Fatalf("the child died on %v, want SIGKILL from the WaitDelay grace period", sig)
	}
}

// TestPrepareKeepsTheExistingSysProcAttr proves the Prepare contract on
// unix: it only sets Setpgid and preserves a SysProcAttr the caller
// already configured (a clobbering Prepare would drop the caller's
// attributes), and it allocates an attribute when there is none.
func TestPrepareKeepsTheExistingSysProcAttr(t *testing.T) {
	cmd := exec.Command("true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setctty: true, Setpgid: false}
	proctree.Prepare(cmd)
	if !cmd.SysProcAttr.Setpgid {
		t.Fatal("Prepare did not set Setpgid")
	}
	if !cmd.SysProcAttr.Setctty {
		t.Fatal("Prepare clobbered an existing SysProcAttr field (Setctty)")
	}
	fresh := exec.Command("true")
	proctree.Prepare(fresh)
	if !fresh.SysProcAttr.Setpgid || fresh.SysProcAttr.Setctty {
		t.Fatalf("Prepare on a fresh cmd left the wrong attribute: %+v", fresh.SysProcAttr)
	}
}

// TestStragglerIsKilledAfterTheGrace is the force step of the cancel: a
// descendant that ignores the cancel's SIGTERM is ended by the SIGKILL
// the production callers apply to the group after Wait returned from a
// cancelled run (the step the WaitDelay grace and the post-Wait return
// both funnel into). The hard fixture arms the SIGTERM ignore on the
// child before the exec (inherited by the grandchild, see runChild), so
// both the child and the straggler ignore the cancel's graceful signal:
// the grace period (the WaitDelay kill) ends the child, the straggler's
// pipe hold pushes the wait out to the grace period, and only the
// group's SIGKILL after Wait can end the straggler. The cancel's
// graceful step on a child that honors it is asserted in the plain
// cancel test.
func TestStragglerIsKilledAfterTheGrace(t *testing.T) {
	dir := t.TempDir()
	pid, elapsed, err, cancelled, cmd := runCancelledTree(t, dir, []string{childHoldMarker, "hard"}, time.Second)
	if !cancelled {
		t.Fatalf("the cancel did not reach a live child (wait: %v): the run was not cancelled", err)
	}
	if err == nil {
		t.Fatalf("Wait = nil, want the child's death after the cancel")
	}
	assertChildDiedOnTheGrace(t, cmd)
	if elapsed < 500*time.Millisecond {
		t.Fatalf("Wait ended %s after the cancel, want at least the 1s grace period minus the start: the straggler did not hold the pipe", elapsed)
	}
	if elapsed >= 4*time.Second {
		t.Fatalf("the cancelled run ended %s after the cancel, want under 4s (the 1s grace period plus margin)", elapsed)
	}
	// The straggler that ignored the SIGTERM is gone: the group's
	// SIGKILL after Wait ended it (nothing else can: it ignores the
	// cancel's signal and outlived the child's own death).
	if err := processGone(pid); err != nil {
		t.Fatalf("the straggler %d that ignored SIGTERM is not gone after the cancelled run: %v", pid, err)
	}
}
