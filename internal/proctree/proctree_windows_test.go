//go:build windows

package proctree_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"golang.org/x/sys/windows"

	"github.com/djalmajr/nau/internal/proctree"
)

// stillActive is the SDK's STILL_ACTIVE (winbase.h, 259): the value
// GetExitCodeProcess reports for a process that is still running. The
// x/sys windows package does not export the constant, so the test
// names the value here, with the SDK name, instead of a bare literal.
const stillActive = 259

// processAlive reports whether pid is running, the way the regression
// asserts on Windows: OpenProcess plus GetExitCodeProcess, with
// anything but STILL_ACTIVE as the "gone" answer. A process that no
// longer exists is a "gone" answer, not a probe failure: OpenProcess
// reports ERROR_INVALID_PARAMETER for a pid with no process (and
// ERROR_FILE_NOT_FOUND is accepted as well); any other OpenProcess
// error is a probe failure, reported as such (never folded into "gone").
func processAlive(pid int) (bool, error) {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		if err == windows.ERROR_INVALID_PARAMETER || err == windows.ERROR_FILE_NOT_FOUND {
			return false, nil
		}
		return false, err
	}
	defer windows.CloseHandle(h)
	var code uint32
	if err := windows.GetExitCodeProcess(h, &code); err != nil {
		return false, err
	}
	return code == stillActive, nil
}

// processGone waits with a bound (goneBound) until pid is confirmed
// dead; on Windows there is no zombie window to ride out, so one probe
// usually answers, and the bounded retry only guards the moments
// between TerminateJobObject and the state settling.
func processGone(pid int) error {
	deadline := time.Now().Add(goneBound)
	for {
		alive, err := processAlive(pid)
		if err != nil {
			return fmt.Errorf("probe pid %d: %w", pid, err)
		}
		if !alive {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("pid %d is still alive after %s", pid, goneBound)
		}
		time.Sleep(pidPoll)
	}
}

// killTree ends the one process the test's own fixture created:
// TerminateProcess on the pid, nothing broader (never by name, never a
// group the test did not build).
func killTree(pid int) {
	h, err := windows.OpenProcess(windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return
	}
	defer windows.CloseHandle(h)
	windows.TerminateProcess(h, 1)
}

// armInheritedTermIgnore is the Windows side of the hard fixture: SIGTERM
// does not exist on Windows, and the straggler test that needs the hard
// role does not build there, so the role is a plain hold.
func armInheritedTermIgnore() {}

// assertChildDiedOnTheCancel asserts how the child died in a cancelled
// run on Windows: with the exit code TerminateJobObject gives the job's
// processes (1), which is the cancel's doing, not the child's own exit.
func assertChildDiedOnTheCancel(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	st := cmd.ProcessState
	if st == nil {
		t.Fatal("no process state: the child was not reaped")
	}
	if code := st.ExitCode(); code != 1 {
		t.Fatalf("the child exited %d, want the cancel's exit code 1 (TerminateJobObject)", code)
	}
}

// TestPrepareLeavesSysProcAttrAlone proves the Prepare contract on
// Windows: the job object is created after Start, so there is nothing
// to set before it, and a SysProcAttr the caller configured is left
// exactly as it is.
func TestPrepareLeavesSysProcAttrAlone(t *testing.T) {
	cmd := exec.Command("true")
	cmd.SysProcAttr = &windows.SysProcAttr{CreationFlags: 0x1}
	proctree.Prepare(cmd)
	if cmd.SysProcAttr == nil {
		t.Fatal("Prepare replaced the SysProcAttr with nil")
	}
	if cmd.SysProcAttr.CreationFlags != 0x1 {
		t.Fatalf("Prepare changed the SysProcAttr (CreationFlags = %x, want 1)", cmd.SysProcAttr.CreationFlags)
	}
}

// TestAttachErrorReturnsAnInertRelease proves the release contract of
// the Attach error path on Windows: a cmd whose process was never
// started takes the os.ErrProcessDone path, and the release it
// returns is not nil — the callers run `defer release()` without
// checking the error, so a nil release there is a panic at the end of
// the run, not the documented degraded fallback. Calling the release
// must not panic and must not change anything.
func TestAttachErrorReturnsAnInertRelease(t *testing.T) {
	cmd := exec.Command("true")
	release, err := proctree.Attach(cmd)
	if err == nil {
		t.Fatal("Attach = nil error, want the no-process error")
	}
	if !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Attach error = %v, want os.ErrProcessDone", err)
	}
	if release == nil {
		t.Fatal("the Attach error path returned a nil release: the caller's defer release() would panic")
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("calling the error-path release panicked: %v", r)
			}
		}()
		release()
	}()
}
