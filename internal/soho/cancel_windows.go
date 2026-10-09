//go:build windows

package soho

import (
	"os"
	"os/exec"
	"sync/atomic"
)

// boundedCancel cancels the child on the context deadline by killing the
// process; the WaitDelay grace period bounds the cleanup. interrupted is
// set only when the kill reached a live process: an already-finished
// child is reported as os.ErrProcessDone (killed only through
// cmd.Process, never by raw pid) and leaves the flag unset, so its own
// exit code stays unchanged.
func boundedCancel(cmd *exec.Cmd, interrupted *atomic.Bool) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := cmd.Process.Kill(); err != nil {
			return err
		}
		interrupted.Store(true)
		return nil
	}
}
