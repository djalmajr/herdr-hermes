//go:build windows

package soho

import (
	"os"
	"os/exec"
)

// boundedCancel cancels the child on the context deadline by killing the
// process; the WaitDelay grace period bounds the cleanup.
func boundedCancel(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		return cmd.Process.Kill()
	}
}
