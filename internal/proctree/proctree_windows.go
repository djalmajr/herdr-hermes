//go:build windows

package proctree

import (
	"os"
	"os/exec"
	"sync"

	"golang.org/x/sys/windows"
)

// attach is the job object that holds a started child's tree, the
// process handle the child was assigned with, and a mutex that guards
// the handles against the one race the sequence leaves open: a cancel
// that reads the job handle while release is closing it after Wait
// returned.
type attach struct {
	mu     sync.Mutex
	job    windows.Handle
	proc   windows.Handle
	closed bool
}

var (
	regMu sync.Mutex
	reg   = map[*exec.Cmd]*attach{}
)

// Prepare does nothing on Windows: the tree is bounded by the job
// object Attach creates right after Start, and there is no per-process
// attribute that would bind it before the start. A SysProcAttr the
// caller configured is left exactly as it is.
func Prepare(cmd *exec.Cmd) {}

// Attach creates the job object that holds the child's whole tree and
// assigns the child to it. It must be called right after Start.
//
// The job carries no kill-on-close limit. The cancel terminates the job
// explicitly (TerminateJobObject), and a normal exit closes the job
// handle with no limit armed, so the close cannot kill a descendant the
// child left on purpose. The alternative — arming
// JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE and disarming it before the close
// of a normal exit — keeps a kill path on the very line the normal exit
// must take, and a failed disarm would kill the tree on the close; with
// no limit, the close is harmless by construction and the only kill is
// the explicit TerminateJobObject on the cancel.
//
// There is a window between Start and the assignment: a descendant the
// child starts inside it is outside the job, and Terminate cannot reach
// it. The window is the span of the two system calls the assignment
// costs (a process-handle open and the job assignment); it is
// documented here because a tree kill is only as strong as the moment
// its membership is decided.
//
// On failure (the job object, the process handle or the assignment
// could not be created) the run continues and the cancel falls back to
// the child alone — the behavior before this package existed — because
// a run that started is not refused over a tracking step. Every path
// returns a non-nil release (inert on the error paths), so a caller's
// `defer release()` is safe without checking the error.
func Attach(cmd *exec.Cmd) (func(), error) {
	inert := func() {}
	if cmd.Process == nil {
		return inert, os.ErrProcessDone
	}
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return inert, err
	}
	proc, err := windows.OpenProcess(windows.PROCESS_ALL_ACCESS, false, uint32(cmd.Process.Pid))
	if err != nil {
		windows.CloseHandle(job)
		return inert, err
	}
	if err := windows.AssignProcessToJobObject(job, proc); err != nil {
		windows.CloseHandle(job)
		windows.CloseHandle(proc)
		return inert, err
	}
	a := &attach{job: job, proc: proc}
	regMu.Lock()
	reg[cmd] = a
	regMu.Unlock()
	return func() { release(cmd) }, nil
}

// Terminate ends the tree on the cancel: TerminateJobObject ends the
// child and every process in its job (descendants join the job by
// default, so the whole tree is in it). It reports os.ErrProcessDone
// when there is no process at all, so a caller keeps the
// interrupted-flag semantics of an already-finished child. When Attach
// never happened (it failed), the child alone is killed, the behavior
// before this package existed.
func Terminate(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	regMu.Lock()
	a := reg[cmd]
	regMu.Unlock()
	if a == nil {
		return cmd.Process.Kill()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return os.ErrProcessDone
	}
	return windows.TerminateJobObject(a.job, 1)
}

// Kill is a no-op on Windows: TerminateJobObject already ended the
// whole tree on the cancel (there is no graceful/force split to
// finish), and a job whose assignment failed cannot be reached at all.
// It exists so a caller runs the same sequence on every platform, and
// it reports os.ErrProcessDone when there is no process at all, like
// the other steps.
func Kill(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	return nil
}

// release closes the job's handles. It is the release Attach returned,
// to be called once when the run has ended (deferred), and it is safe
// to call it from the run's goroutine while a late cancel still reads
// the handles: the mutex and the closed flag make a use-after-close
// impossible. Without the kill-on-close limit, closing the job handle
// never kills: a normal exit closes it with the descendants still
// running, and a cancelled run closes it after TerminateJobObject has
// already ended the tree.
func release(cmd *exec.Cmd) {
	regMu.Lock()
	a := reg[cmd]
	if a != nil {
		delete(reg, cmd)
	}
	regMu.Unlock()
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	a.mu.Unlock()
	windows.CloseHandle(a.proc)
	windows.CloseHandle(a.job)
}

// ForwardSignals is a no-op on Windows: Prepare changes nothing before
// the start, and the job object Attach creates does not move the child
// out of the console group, so the console's Ctrl-C keeps reaching the
// child the way it did before the group isolation on unix; there is no
// signal for the runner process to relay. The returned stop is inert:
// it always reports no signal, so the caller's post-Wait re-raise never
// runs.
func ForwardSignals(cmd *exec.Cmd) func() (os.Signal, bool) {
	return func() (os.Signal, bool) { return nil, false }
}

// ReRaise is a no-op on Windows: the relay never reports a signal (see
// ForwardSignals), so there is no signal to re-raise; the console's
// Ctrl-C ends the process the normal way.
func ReRaise(sig os.Signal) {}
