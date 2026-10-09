//go:build unix

package outbox

import (
	"syscall"
	"time"
)

// lockAcquire takes an exclusive non-blocking flock on fd h, retrying on
// contention until the deadline.
func lockAcquire(h uintptr, timeout time.Duration) error {
	fd := int(h)
	deadline := time.Now().Add(timeout)
	for {
		err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			return &LockError{Timeout: timeout}
		}
		if time.Now().After(deadline) {
			return &LockError{Timeout: timeout}
		}
		time.Sleep(lockPollInterval)
	}
}

// lockRelease drops the flock on fd h.
func lockRelease(h uintptr) error {
	return syscall.Flock(int(h), syscall.LOCK_UN)
}
