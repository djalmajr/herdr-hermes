//go:build darwin || freebsd || netbsd || openbsd

package cli

import (
	"os"
	"syscall"
	"unsafe"
)

// setTermEcho toggles the ECHO flag of the terminal attached to f with the
// BSD TIOCGETA/TIOCSETA ioctls. It fails (ENOTTY) when f is not a terminal,
// which the caller turns into a refusal to read the key with echo on.
func setTermEcho(f *os.File, on bool) error {
	var t syscall.Termios
	if _, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, f.Fd(),
		uintptr(syscall.TIOCGETA), uintptr(unsafe.Pointer(&t)), 0, 0, 0); errno != 0 {
		return errno
	}
	if on {
		t.Lflag |= syscall.ECHO
	} else {
		t.Lflag &^= syscall.ECHO
	}
	if _, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, f.Fd(),
		uintptr(syscall.TIOCSETA), uintptr(unsafe.Pointer(&t)), 0, 0, 0); errno != 0 {
		return errno
	}
	return nil
}

// termEchoOn reports the current ECHO flag of the terminal attached to f.
func termEchoOn(f *os.File) (bool, error) {
	var t syscall.Termios
	if _, _, errno := syscall.Syscall6(syscall.SYS_IOCTL, f.Fd(),
		uintptr(syscall.TIOCGETA), uintptr(unsafe.Pointer(&t)), 0, 0, 0); errno != 0 {
		return false, errno
	}
	return t.Lflag&syscall.ECHO != 0, nil
}
