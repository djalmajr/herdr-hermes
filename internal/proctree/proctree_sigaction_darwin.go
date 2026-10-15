//go:build darwin

package proctree

import (
	"syscall"
	"unsafe"
)

// setDefaultAction installs SIG_DFL as the disposition of sig through a
// raw sigaction. A zeroed buffer is a safe sigaction: sa_handler is the
// first field of struct sigaction on darwin and zero is SIG_DFL, and
// sa_flags and the mask words are zero. The buffer is sized past the
// kernel's struct sigaction (40 bytes on darwin); the kernel copies only
// its own size, so the extra words are harmless.
func setDefaultAction(sig syscall.Signal) {
	var act [64]byte
	_, _, _ = syscall.Syscall6(syscall.SYS_SIGACTION, uintptr(sig),
		uintptr(unsafe.Pointer(&act)), 0, 0, 0, 0)
}

// unblockSignal clears sig from the calling thread's blocked set through a
// raw sigprocmask, so the kernel can deliver the re-raise the caller sends
// itself. On darwin how 2 is SIG_UNBLOCK and sigprocmask takes
// (how, set, oset); the set is sized past the kernel's sigset_t (128
// bits on darwin).
func unblockSignal(sig syscall.Signal) {
	var set [16]byte
	set[int(sig)/8] |= 1 << (uint(int(sig)) % 8)
	_, _, _ = syscall.Syscall6(syscall.SYS_SIGPROCMASK, 2, /* SIG_UNBLOCK */
		uintptr(unsafe.Pointer(&set)), 0, 0, 0, 0)
}
