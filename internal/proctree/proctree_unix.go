//go:build !windows

package proctree

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// Prepare puts the child in its own process group (Setpgid), so the
// whole tree the child builds stays in one group Terminate and Kill
// reach on one call (a descendant inherits the group unless it leaves
// it). It preserves a SysProcAttr that already exists: every field is
// left as it is and only Setpgid is set, so a caller that configured
// the attribute first is not clobbered. It must be called before
// cmd.Start.
func Prepare(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// Attach has nothing to attach on unix: with Prepare's Setpgid the
// group id is the child's own pid, and the group needs no handle. It
// returns a release that does nothing and a nil error, so a caller runs
// the same sequence on every platform.
func Attach(cmd *exec.Cmd) (func(), error) {
	return func() {}, nil
}

// Terminate ends the tree gracefully on the cancel: SIGTERM to the
// whole group (the child and every descendant in it, the descendant
// included only as long as it stayed in the group — the default). It
// reports os.ErrProcessDone when there is no process at all, so a
// caller keeps the interrupted-flag semantics of an already-finished
// child; a signal that reaches a live group reports nil, whatever the
// group then does. A descendant that survives the signal is left to
// Kill after Wait. Terminate requires Prepare to have run, because the
// group id it signals is the child's pid only under Setpgid.
func Terminate(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	return killGroup(cmd.Process.Pid, syscall.SIGTERM)
}

// Kill forces the tree down after Wait has returned from a cancelled
// run: SIGKILL to whatever the group still holds. It runs exactly once
// per run, after Wait, and only when the cancel fired: a run that
// finished without a cancel never calls it, so a child that exited on
// its own keeps the descendants it left on purpose. When the group is
// already empty (the usual case: the cancel's SIGTERM took the whole
// tree) the ESRCH is discarded — an empty group is the success.
func Kill(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	err := killGroup(cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

// killGroup signals the process group whose id is pid: under Prepare's
// Setpgid that group is the child's own tree, and a negative pid is the
// group form of the signal.
func killGroup(pid int, sig syscall.Signal) error {
	return syscall.Kill(-pid, sig)
}
