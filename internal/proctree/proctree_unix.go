//go:build !windows

package proctree

import (
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"sync"
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

// ForwardSignals relays the terminal's interrupt signals from the
// calling process to the child's process group for as long as the
// child runs. Prepare puts the child in its own group (Setpgid), so a
// Ctrl-C (SIGINT to the foreground group) reaches only the calling
// process: without the relay the child's whole tree would outlive an
// interrupted run as orphans. It receives SIGINT, SIGTERM and SIGHUP
// on the calling process (signal.Notify) and sends each one to the
// group the child leads — the child's own pid under Prepare's
// Setpgid, the negative-pid group form; a group the child has already
// emptied (it exited first) answers ESRCH and is the no-op. It returns
// two functions. stop ends the relay's forwarding and reports the last
// signal received, if any, but leaves the signal reception in place —
// the runtime still has the relayed signals unblocked on the thread it
// dedicates to signal delivery — so the caller can re-raise the
// reported signal on itself (ReRaise) and end on the very signal; a run
// that received no signal never re-raises and must run cleanup, which
// stops the reception (signal.Stop) so the process goes back to its
// normal signal behaviour. The exit, the interrupted flag and the
// deadline are all untouched by a run that received no signal. It must
// be called after Start (the group id is the child's pid) and stopped
// exactly once, after Wait.
func ForwardSignals(cmd *exec.Cmd) (stop func() (os.Signal, bool), cleanup func()) {
	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	ch := make(chan os.Signal, 8)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	var (
		mu     sync.Mutex
		last   os.Signal
		done   = make(chan struct{})
		stopCh = make(chan struct{})
	)
	go func() {
		defer close(done)
		for {
			select {
			case <-stopCh:
				return
			case sig := <-ch:
				// The child's group: under Prepare's Setpgid it is the
				// child's own tree. A group the child has already
				// emptied answers ESRCH and is the no-op the relay
				// needs; the caller's re-raise is independent of
				// whether the forwarding reached it. The relayed
				// signals are the terminal's, which are syscall signals
				// on unix.
				mu.Lock()
				last = sig
				mu.Unlock()
				if pid > 0 {
					if s, ok := sig.(syscall.Signal); ok {
						_ = killGroup(pid, s)
					}
				}
			}
		}
	}()
	return func() (os.Signal, bool) {
			close(stopCh)
			<-done
			mu.Lock()
			defer mu.Unlock()
			return last, last != nil
		}, func() {
			// Stop the reception so the process goes back to its normal
			// signal behaviour. Only the no-signal path runs it: the
			// re-raise ends the process, so a run that received a signal
			// never gets to clean up, and its leaked reception is moot.
			signal.Stop(ch)
		}
}

// ReRaise restores the default action of sig and re-raises it on the
// calling process, so the process ends on the very signal the relay
// forwarded to the child's group — the same death it would have had
// without the group isolation. It is the caller's step after a stop
// that reports a signal; the child's exit code is not translated. The
// default action of a relayed signal ends the process, so the line
// after a ReRaise that ran is a fallback only.
func ReRaise(sig os.Signal) {
	s, ok := sig.(syscall.Signal)
	if !ok {
		return
	}
	// Install SIG_DFL first: os/signal cannot restore the default action
	// for the signals the runtime installs its own handler for (SIGINT,
	// SIGTERM, SIGHUP, ...), so a signal the process sends itself would
	// otherwise be swallowed by the runtime instead of ending it. The raw
	// sigaction is the only way to set SIG_DFL from Go.
	setDefaultAction(s)
	// The relay's stop left the signal reception in place — the runtime
	// still has the signal unblocked on the thread it dedicates to signal
	// delivery — and the unblock clears it from this thread too, so at
	// least one thread can take the re-raise. With the disposition at
	// SIG_DFL, whichever thread receives it ends the process on the very
	// signal.
	unblockSignal(s)
	syscall.Kill(os.Getpid(), s)
}

// setDefaultAction and unblockSignal are the raw sigaction and
// sigprocmask the re-raise needs, implemented per OS: the kernel
// numbers and the argument shape of the two calls differ (sigaction
// and rt_sigaction, SIGPROCMASK and RT_SIGPROCMASK), so each target's
// definition lives in its own build-tagged file (proctree_sigaction_*.go).
