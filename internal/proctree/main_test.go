package proctree_test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/proctree"
)

// The argv markers that route a re-executed copy of this test binary
// into the roles of the tree the tests build. The pattern is the one
// internal/testutil/fakesoho already uses: the test binary plays the
// role when it is re-executed with a marker, and just runs the tests
// otherwise.
const (
	grandchildMarker = "-proctree-grandchild"
	childHoldMarker  = "-proctree-child-hold"
	childExitMarker  = "-proctree-child-exit"
	forwarderMarker  = "-proctree-forwarder"
)

// TestMain lets the re-executed copy of this test binary play the child
// and the grandchild the tests build: started with one of the markers it
// runs the role and exits, and run as the test binary it just runs the
// tests.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case grandchildMarker:
			if len(os.Args) != 3 {
				roleDie(fmt.Errorf("usage: %s <liveness>", grandchildMarker))
			}
			runGrandchild(os.Args[2])
		case forwarderMarker:
			if len(os.Args) != 4 {
				roleDie(fmt.Errorf("usage: %s <childpidfile> <grandchildpidfile>", forwarderMarker))
			}
			runForwarder(os.Args[2], os.Args[3])
		case childHoldMarker:
			if len(os.Args) < 3 || len(os.Args) > 4 {
				roleDie(fmt.Errorf("usage: %s <pidfile> [hard]", childHoldMarker))
			}
			runChild(os.Args[2], true, len(os.Args) == 4 && os.Args[3] == "hard")
		case childExitMarker:
			if len(os.Args) != 3 {
				roleDie(fmt.Errorf("usage: %s <pidfile>", childExitMarker))
			}
			runChild(os.Args[2], false, false)
		}
	}
	os.Exit(m.Run())
}

// roleDie ends a role with a distinguishable code and the reason on
// stderr, so a test that re-executes a role sees a start failure, not a
// silent hang.
func roleDie(err error) {
	fmt.Fprintln(os.Stderr, "proctree role:", err)
	os.Exit(126)
}

// runGrandchild is the grandchild role: it holds the stdout and stderr
// the child gave it (the pipe the test pointed the child at stays open
// through it), until it is killed, and answers the liveness requests
// the test writes to liveness+".req". The sleep loop keeps a timer
// pending, so the runtime's deadlock check (all goroutines asleep with
// no timer) does not end the holder.
func runGrandchild(liveness string) {
	if liveness != "" {
		go serveLiveness(liveness)
	}
	for {
		time.Sleep(time.Hour)
	}
}

// serveLiveness polls the liveness request file and answers it: the
// request's content is echoed on the response file and the request is
// removed, so one request yields exactly one fresh answer — an answer
// only exists after a request, so a request written after the run
// ended is answered only if the holder is still able to run. It is
// file polling with the package's tick, the same idiom the tests use
// for the pid file.
func serveLiveness(liveness string) {
	for {
		req, err := os.ReadFile(liveness + ".req")
		if err == nil {
			_ = os.WriteFile(liveness+".resp", req, 0o600)
			_ = os.Remove(liveness + ".req")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// runChild is the child role: it starts the grandchild with the child's
// own stdout and stderr (the pipe the test pointed the child at, or the
// test's own output), writes the grandchild's pid to the pid file, and
// then holds (stay) or exits 0 leaving the grandchild behind. With hard
// it also makes the child and the grandchild ignore SIGTERM (on unix):
// the ignore is armed on the child before the exec, because a SIG_IGN
// disposition is inherited across the exec and survives the
// grandchild's runtime start, so the grandchild ignores the cancel's
// SIGTERM from its first instruction — armed inside the grandchild
// instead, there would be a window in which the cancel could reach it
// before its own setup runs. The child keeps the ignore for its whole
// life (the standard library cannot restore the default action after
// it), so in that case the grace period, not the cancel's signal, ends
// the child; the cancel's graceful step is still asserted on a child
// that honors it, in the plain cancel test.
func runChild(pidfile string, stay, hard bool) {
	exe, err := os.Executable()
	if err != nil {
		roleDie(err)
	}
	if hard {
		armInheritedTermIgnore()
	}
	cmd := exec.Command(exe, grandchildMarker, pidfile+".liveness")
	// os/exec connects a nil Stdout/Stderr to the null device, it does
	// not inherit: the grandchild must take the child's own descriptors
	// explicitly (the same way fakesoho's pipe holder takes them), so
	// the pipe the test pointed the child at stays held through the
	// grandchild's lifetime. The liveness file derives from the pid
	// file, so the test knows its path without another argument.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		roleDie(err)
	}
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		roleDie(err)
	}
	if !stay {
		os.Exit(0)
	}
	for {
		time.Sleep(time.Hour)
	}
}

// runForwarder is the helper role of the relay test: it runs the
// production wiring of a run — Prepare before Start, Attach right
// after Start, the ForwardSignals relay around Wait and the ReRaise
// when the stop reports a signal — on a child that starts a grandchild
// and holds the helper's stdout (the pipe the test pointed the helper
// at, held through the grandchild). The test starts the helper in its
// own process group, so a signal the test sends to the helper's group
// reaches only the helper, the way the terminal's Ctrl-C reaches only
// the foreground process after the child left the group.
func runForwarder(childpidfile, grandchildpidfile string) {
	exe, err := os.Executable()
	if err != nil {
		roleDie(err)
	}
	cmd := exec.Command(exe, childHoldMarker, grandchildpidfile)
	proctree.Prepare(cmd)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		roleDie(err)
	}
	if err := os.WriteFile(childpidfile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		roleDie(err)
	}
	release, err := proctree.Attach(cmd)
	if err != nil {
		roleDie(err)
	}
	defer release()
	stop := proctree.ForwardSignals(cmd)
	cmd.Wait()
	sig, had := stop()
	if had {
		// The relay forwarded the terminal's signal to the child's
		// group and the run ended on it: the helper must die on the
		// very signal it would have died on without the group
		// isolation, so the re-raise ends the process and the exit
		// below is the fallback only when it did not.
		proctree.ReRaise(sig)
	}
	os.Exit(0)
}
