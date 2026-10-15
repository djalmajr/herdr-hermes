//go:build windows

package fakesoho

import (
	"fmt"
	"syscall"
	"time"
)

// errorSharingViolation is the Windows error 32 (ERROR_SHARING_VIOLATION),
// which the standard library does not export; CreateFile with no sharing
// reports it while another process holds the file open.
const errorSharingViolation = syscall.Errno(32)

// lock takes an exclusive open of the file at path for the duration of
// one call-log append: a CreateFile with no sharing acts as the lock,
// and the system releases it when the owning process exits and its
// handle is closed, so a child killed at a deadline leaves no stale lock
// behind. The file is created once and never removed, so there is no
// stale-file state to wait out.
func lock(path string) (func(), error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(lockWait)
	for {
		h, cerr := syscall.CreateFile(p, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
		if cerr == nil {
			return func() { _ = syscall.CloseHandle(h) }, nil
		}
		// Anything but a sharing violation is a real failure: return it
		// at once.
		if cerr != errorSharingViolation {
			return nil, cerr
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}
