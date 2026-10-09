//go:build !linux && !windows && !darwin && !freebsd && !netbsd && !openbsd

package cli

import (
	"errors"
	"os"
)

// errTermNotSupported reports that this platform has no supported way to
// toggle terminal echo. Reading a secret with echo on is refused.
var errTermNotSupported = errors.New("terminal echo control not supported on this platform")

// setTermEcho is not supported on this platform; the caller refuses to read
// the key rather than risk echoing it.
func setTermEcho(f *os.File, on bool) error {
	return errTermNotSupported
}

// termEchoOn is not supported on this platform.
func termEchoOn(f *os.File) (bool, error) {
	return false, errTermNotSupported
}
