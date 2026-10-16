//go:build !windows

package proctree_test

import (
	"bytes"
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

// TestForwardSignalsRelaysTheTerminalSignalToTheGroup is the Ctrl-C
// regression of the group isolation: Prepare puts the child in its own
// process group, so a terminal signal sent to the foreground group
// reaches only the process that runs the relay — without the relay the
// child's whole tree would outlive the interrupted run as orphans. The
// fixture builds a helper in its own process group (the test's
// Prepare), and the helper runs the production wiring on a child that
// starts a grandchild (the forwarder role). The test signals the
// helper's group only — a group the test created — and asserts that
// the helper ended on the very signal, and that the child and the
// grandchild are gone after the bounded wait (the relay reached the
// child's group; nothing else could have signalled it).
func TestForwardSignalsRelaysTheTerminalSignalToTheGroup(t *testing.T) {
	for _, tc := range []struct {
		name string
		sig  syscall.Signal
	}{
		{"SIGINT", syscall.SIGINT},
		{"SIGTERM", syscall.SIGTERM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			exe, err := os.Executable()
			if err != nil {
				t.Fatalf("executable: %v", err)
			}
			childpidfile := filepath.Join(dir, "child.pid")
			grandpidfile := filepath.Join(dir, "grandchild.pid")
			var out, errOut bytes.Buffer
			cmd := exec.Command(exe, forwarderMarker, childpidfile, grandpidfile)
			// The helper leads its own process group: the signal the
			// test sends goes to this group only, and this group is
			// the one the test created.
			proctree.Prepare(cmd)
			cmd.Stdout = &out
			cmd.Stderr = &errOut
			if err := cmd.Start(); err != nil {
				t.Fatalf("Start: %v", err)
			}
			// End the fixture processes the test built, on success or
			// on failure (the no-relay case leaves the child and the
			// grandchild running as orphans): only the pids the
			// fixture created, never anything else.
			t.Cleanup(func() {
				endFixtureProcess(t, cmd.Process.Pid, "helper")
				for name, file := range map[string]string{"child": childpidfile, "grandchild": grandpidfile} {
					if b, err := os.ReadFile(file); err == nil {
						if pid, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil && pid > 0 {
							endFixtureProcess(t, pid, name)
						}
					}
				}
			})
			childpid := waitForPidFile(t, childpidfile)
			grandpid := waitForPidFile(t, grandpidfile)
			// The control that the run really had a tree to relay to:
			// the grandchild is alive before the signal.
			alive, aerr := processAlive(grandpid)
			if aerr != nil {
				t.Fatalf("probe the grandchild before the signal: %v", aerr)
			}
			if !alive {
				t.Fatalf("the grandchild %d was not alive before the signal: the run had no tree to end", grandpid)
			}
			// The signal goes to the helper's group only (its id is
			// the helper's pid under Prepare's Setpgid): the test
			// never signals a group it did not create.
			if err := syscall.Kill(-cmd.Process.Pid, tc.sig); err != nil {
				t.Fatalf("signal the helper's group: %v", err)
			}
			waitErr := make(chan error, 1)
			go func() { waitErr <- cmd.Wait() }()
			var runErr error
			select {
			case runErr = <-waitErr:
			case <-time.After(waitBound):
				t.Fatalf("the helper did not end within %s after the %v to its group (helper stderr: %q): the relay did not end the run", waitBound, tc.sig, errOut.String())
			}
			if runErr == nil {
				t.Fatalf("the helper exited 0, want the death on %v (helper stderr: %q)", tc.sig, errOut.String())
			}
			// The helper ended on the very signal the test sent to
			// its group: the relay's stop reported it and the
			// production wiring re-raised it on the helper itself.
			ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
			if !ok {
				t.Fatalf("process state sys = %T, want syscall.WaitStatus", cmd.ProcessState.Sys())
			}
			if !ws.Signaled() || ws.Signal() != tc.sig {
				t.Fatalf("the helper died %v, want the death on %v (helper stderr: %q)", ws, tc.sig, errOut.String())
			}
			// The relay reached the child's group: the grandchild (in
			// the group, holding the helper's stdout) and the child
			// are both gone, with a bounded wait.
			if err := processGone(grandpid); err != nil {
				t.Fatalf("the grandchild %d survived the helper's death on %v: the relay did not reach the child's group", grandpid, tc.sig)
			}
			if err := processGone(childpid); err != nil {
				t.Fatalf("the child %d survived the helper's death on %v: the relay did not reach the child's group", childpid, tc.sig)
			}
		})
	}
}
