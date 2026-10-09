//go:build linux

package cli

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// openPty allocates a pty pair through /dev/ptmx: unlock the slave with
// TIOCSPTLCK, read its number with TIOCGPTN, and open both ends. Test-only.
func openPty() (master, slave *os.File, err error) {
	master, err = os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, err
	}
	fail := func(e error) (*os.File, *os.File, error) {
		master.Close()
		return nil, nil, e
	}
	// TIOCSPTLCK takes a pointer to a C int: zero unlocks the slave.
	var lock int32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), uintptr(syscall.TIOCSPTLCK), uintptr(unsafe.Pointer(&lock))); errno != 0 {
		return fail(errno)
	}
	// TIOCGPTN writes the slave number as a C unsigned int; the slave is
	// /dev/pts/<n>.
	var num uint32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(), uintptr(syscall.TIOCGPTN), uintptr(unsafe.Pointer(&num))); errno != 0 {
		return fail(errno)
	}
	slave, err = os.OpenFile(fmt.Sprintf("/dev/pts/%d", num), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return fail(err)
	}
	return master, slave, nil
}
