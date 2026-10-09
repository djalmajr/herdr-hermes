//go:build !windows

package soho

import (
	"os"
	"os/exec"
	"syscall"
)

// boundedCancel cancels the child on the context deadline with SIGTERM;
// the WaitDelay grace period turns into an OS kill if the child ignores
// the signal.
func boundedCancel(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		return cmd.Process.Signal(syscall.SIGTERM)
	}
}
