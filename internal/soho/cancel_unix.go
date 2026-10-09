//go:build !windows

package soho

import (
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
)

// boundedCancel cancels the child on the context deadline with SIGTERM;
// the WaitDelay grace period turns into an OS kill if the child ignores
// the signal. interrupted is set only when the signal reached a live
// process: a child that had already finished is reported as
// os.ErrProcessDone (signalled only through cmd.Process, never by raw
// pid) and leaves the flag unset, so its own exit code stays unchanged.
func boundedCancel(cmd *exec.Cmd, interrupted *atomic.Bool) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return err
		}
		interrupted.Store(true)
		return nil
	}
}
