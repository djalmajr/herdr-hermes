//go:build linux || darwin

package fakesoho

import (
	"errors"
	"syscall"
)

// On the unix operating systems the busy-executable condition exists,
// so the busy check matches the platform ETXTBSY error (including a
// start error that wraps it).
func init() {
	busyExec = func(err error) bool {
		return errors.Is(err, syscall.ETXTBSY)
	}
}
