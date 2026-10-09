//go:build darwin

package cli

import (
	"bytes"
	"os"
	"syscall"
	"unsafe"
)

// openPty allocates a pty pair through /dev/ptmx: grant and unlock the
// slave, read its name with TIOCPTYGNAME, and open both ends. Test-only.
func openPty() (master, slave *os.File, err error) {
	master, err = os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, err
	}
	fail := func(e error) (*os.File, *os.File, error) {
		master.Close()
		return nil, nil, e
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), uintptr(syscall.TIOCPTYGRANT), 0); errno != 0 {
		return fail(errno)
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), uintptr(syscall.TIOCPTYUNLK), 0); errno != 0 {
		return fail(errno)
	}
	var name [128]byte
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), uintptr(syscall.TIOCPTYGNAME), uintptr(unsafe.Pointer(&name[0]))); errno != 0 {
		return fail(errno)
	}
	n := bytes.IndexByte(name[:], 0)
	if n <= 0 {
		return fail(os.ErrInvalid)
	}
	slave, err = os.OpenFile(string(name[:n]), os.O_RDWR, 0)
	if err != nil {
		return fail(err)
	}
	return master, slave, nil
}
