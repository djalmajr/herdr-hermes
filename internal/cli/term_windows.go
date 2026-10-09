//go:build windows

package cli

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

// kernel32 console-mode entry points; no third-party module is available
// for this.
var (
	kernel32       = syscall.NewLazyDLL("kernel32.dll")
	getConsoleMode = kernel32.NewProc("GetConsoleMode")
	setConsoleMode = kernel32.NewProc("SetConsoleMode")
)

// enableEcho is the ENABLE_ECHO console mode flag.
const enableEcho = 0x0004

// setTermEcho toggles the ENABLE_ECHO console mode of the console
// attached to f. It fails when f is not a console, which the caller turns
// into a refusal to read the key with echo on.
func setTermEcho(f *os.File, on bool) error {
	var mode uint32
	r, _, err := getConsoleMode.Call(uintptr(f.Fd()), uintptr(unsafe.Pointer(&mode)))
	if r == 0 {
		return consoleModeError(err)
	}
	if on {
		mode |= enableEcho
	} else {
		mode &^= enableEcho
	}
	r, _, err = setConsoleMode.Call(uintptr(f.Fd()), uintptr(mode))
	if r == 0 {
		return consoleModeError(err)
	}
	return nil
}

// termEchoOn reports whether ENABLE_ECHO is set on the console attached to
// f.
func termEchoOn(f *os.File) (bool, error) {
	var mode uint32
	r, _, err := getConsoleMode.Call(uintptr(f.Fd()), uintptr(unsafe.Pointer(&mode)))
	if r == 0 {
		return false, consoleModeError(err)
	}
	return mode&enableEcho != 0, nil
}

func consoleModeError(err error) error {
	if err == nil {
		return errors.New("console mode call failed")
	}
	return err
}
