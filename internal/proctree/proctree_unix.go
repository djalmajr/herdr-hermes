//go:build !windows

package proctree

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

// reRaiseBound bounds the wait, inside ReRaise, for the re-raised
// signal to end the process: the delivery ends the process on the
// very signal, so the bound is only the fallback for the case where
// it never arrives.
const reRaiseBound = 5 * time.Second

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

// ForwardSignals relays the terminal's interrupt signals from the
// calling process to the child's process group for as long as the
// child runs. Prepare puts the child in its own group (Setpgid), so a
// Ctrl-C (SIGINT to the foreground group) reaches only the calling
// process: without the relay the child's whole tree would outlive an
// interrupted run as orphans. It receives SIGINT, SIGTERM and SIGHUP
// on the calling process (signal.Notify) and sends each one to the
// group the child leads — the child's own pid under Prepare's
// Setpgid, the negative-pid group form; a group the child has already
// emptied (it exited first) answers ESRCH and is the no-op. The first
// signal also starts a fixed grace (Grace): if the group has not
// ended within it, the relay forces the group down (SIGKILL, the same
// call Kill runs) so the caller's Wait returns — a backend that
// ignores the terminal's signal cannot hold the interrupted run for
// the caller's whole bound. A second signal during the grace is
// forwarded as well and does not restart it. It returns one function:
// stop ends the relay, stops the signal reception (signal.Stop, so the
// process goes back to its normal signal behaviour) and reports the
// last signal received, if any; the caller re-raises the reported
// signal on itself (ReRaise) when one was received, so the run ends on
// the very signal. A run that received no signal never re-raises; its
// reception is already stopped, so nothing of the relay is left
// behind and the exit, the interrupted flag and the deadline are all
// untouched. It must be called after Start (the group id is the child's
// pid) and stopped exactly once, after Wait.
func ForwardSignals(cmd *exec.Cmd) func() (os.Signal, bool) {
	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	ch := make(chan os.Signal, 8)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	var (
		last   os.Signal
		stopCh = make(chan struct{})
		done   = make(chan struct{})
	)
	// The relay loop is the only reader and writer of last and grace:
	// stop reads last only after done closed (the loop exited), so no
	// lock is needed.
	go func() {
		defer close(done)
		var grace <-chan time.Time
		for {
			select {
			case <-stopCh:
				return
			case sig := <-ch:
				// The first signal arms the grace once; a second signal
				// during it is forwarded as well and does not restart
				// the deadline.
				if grace == nil && pid > 0 {
					grace = time.NewTimer(Grace).C
				}
				last = sig
				// The child's group: under Prepare's Setpgid it is the
				// child's own tree. A group the child has already emptied
				// answers ESRCH and is the no-op the relay needs; the
				// caller's re-raise is independent of whether the
				// forwarding reached it. The relayed signals are the
				// terminal's, which are syscall signals on unix.
				if pid > 0 {
					if s, ok := sig.(syscall.Signal); ok {
						_ = killGroup(pid, s)
					}
				}
			case <-grace:
				// The first signal's grace expired with the relay still
				// active: the group survived the forwarded signal. Force
				// it (the very step Kill runs, after Wait) so the
				// caller's Wait returns.
				grace = nil
				_ = Kill(cmd)
			}
		}
	}()
	return func() (os.Signal, bool) {
		close(stopCh)
		<-done
		// Stop the reception as well: with the relay gone the process
		// goes back to its normal signal behaviour, and a run that
		// received no signal leaves nothing of the relay behind; the
		// re-raise (ReRaise) only has to reset and re-send the reported
		// signal.
		signal.Stop(ch)
		return last, last != nil
	}
}

// ReRaise ends the calling process on the very signal the relay
// forwarded to the child's group — the same death the process would
// have had without the group isolation. It runs the standard route:
// signal.Reset restores the default handling of the signal (as if the
// relay's Notify had never been called), and the process sends the
// signal to itself, which the default handling turns into the death
// on the very signal. It is the caller's step after a stop that
// reports a signal; the child's exit code is not translated. The
// delivery ends the process, so the bounded wait and the exit below
// are the fallback only: if the process is still running after
// reRaiseBound, it ends with the shell convention (128 plus the
// signal number) instead of hanging.
func ReRaise(sig os.Signal) {
	s, ok := sig.(syscall.Signal)
	if !ok {
		return
	}
	signal.Reset(s)
	syscall.Kill(os.Getpid(), s)
	time.Sleep(reRaiseBound)
	os.Exit(128 + int(s))
}
