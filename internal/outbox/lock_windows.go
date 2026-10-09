//go:build windows

package outbox

import (
	"fmt"
	"syscall"
	"time"
	"unsafe"
)

// LockFileEx and UnlockFileEx from kernel32, called through the standard
// library. Like the Go toolchain's own file locker, the whole file is
// locked (byte counts ^uint32(0)) with a real (zero-offset) OVERLAPPED, and
// success is decided by the nonzero BOOL returned in r1, never by the error
// value (Proc.Call reports it as a non-nil Errno even on success).
var (
	lockFileExProc   = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")
	unlockFileExProc = syscall.NewLazyDLL("kernel32.dll").NewProc("UnlockFileEx")
)

const (
	// LOCKFILE_FAIL_IMMEDIATELY (0x1) | LOCKFILE_EXCLUSIVE_LOCK (0x2):
	// a non-blocking exclusive lock.
	lockFlags = 0x1 | 0x2
	allBytes  = ^uint32(0)

	// Win32 error codes.
	errLockViolation = 33  // ERROR_LOCK_VIOLATION
	errIOPending     = 997 // ERROR_IO_PENDING
)

// lockAcquire takes an exclusive non-blocking whole-file lock on handle h,
// retrying while the lock is held elsewhere until the deadline. It succeeds
// only when LockFileEx returns a nonzero BOOL; any errno other than
// ERROR_LOCK_VIOLATION or ERROR_IO_PENDING is returned at once.
func lockAcquire(h uintptr, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ol := syscall.Overlapped{}
		r1, _, e1 := lockFileExProc.Call(
			h,
			uintptr(lockFlags),
			0,
			uintptr(allBytes),
			uintptr(allBytes),
			uintptr(unsafe.Pointer(&ol)),
		)
		if r1 != 0 {
			return nil
		}
		errno, _ := e1.(syscall.Errno)
		if errno != errLockViolation && errno != errIOPending {
			return fmt.Errorf("outbox: LockFileEx failed: %w", errno)
		}
		if time.Now().After(deadline) {
			return &LockError{Timeout: timeout}
		}
		time.Sleep(lockPollInterval)
	}
}

// lockRelease drops the whole-file lock on handle h with UnlockFileEx; it
// succeeds only when UnlockFileEx returns a nonzero BOOL.
func lockRelease(h uintptr) error {
	ol := syscall.Overlapped{}
	r1, _, e1 := unlockFileExProc.Call(
		h,
		0,
		uintptr(allBytes),
		uintptr(allBytes),
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 != 0 {
		return nil
	}
	return fmt.Errorf("outbox: UnlockFileEx failed: %v", e1)
}
