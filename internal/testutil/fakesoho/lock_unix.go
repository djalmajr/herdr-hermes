//go:build !windows

package fakesoho

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

// lock takes an exclusive flock on the file at path for the duration of
// one call-log append. The lock is taken on the open file itself and the
// system releases it when the owning process exits, because the process
// exit closes its descriptor: a child killed at a deadline leaves no
// stale lock behind. The file is created once and never removed, so
// there is no stale-file state to wait out.
func lock(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(lockWait)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
				_ = f.Close()
			}, nil
		}
		// Anything but contention is a real failure: return it at once.
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = f.Close()
			return nil, err
		}
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, fmt.Errorf("timed out waiting for %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}
