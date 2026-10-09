//go:build windows

package fakesoho

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

// lock takes a short-lived exclusive lock on a lock directory for the
// duration of one call-log append.
func lock(path string) (func(), error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := os.Mkdir(path, 0o700)
		if err == nil {
			return func() { _ = os.Remove(path) }, nil
		}
		if !os.IsExist(err) {
			// Windows can report ACCESS_DENIED when another process removes
			// the lock directory between this mkdir and the check; treat it
			// as contention and retry.
			if !errors.Is(err, syscall.ERROR_ACCESS_DENIED) {
				return nil, err
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}
