//go:build !windows

package fakesoho

import (
	"fmt"
	"os"
	"time"
)

// lock takes a short-lived exclusive lock on path for the duration of one
// call-log append. The lock lives on the file itself; a crashed process
// leaves a stale file, which the bounded wait below times out on.
func lock(path string) (func(), error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_ = f.Close()
			return func() { _ = os.Remove(path) }, nil
		}
		if !os.IsExist(err) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for %s", path)
		}
		time.Sleep(time.Millisecond)
	}
}
