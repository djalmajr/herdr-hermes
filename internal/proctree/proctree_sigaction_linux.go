//go:build linux

package proctree

import (
	"syscall"
	"unsafe"
)

// sigsetSize is the sigset size the kernel's rt_sigaction and
// rt_sigprocmask take on linux: rt_sigaction passes the size of the
// sigaction's sa_mask field (one word, 8 bytes) and rt_sigprocmask
// passes _NSIG/8 (65/8 = 8). The Go runtime's own signal setup uses the
// same 8-byte size for both calls, so the re-raise mirrors it.
const sigsetSize = 8

// setDefaultAction installs SIG_DFL as the disposition of sig through a
// raw rt_sigaction. On linux the call takes (sig, act, oact, setsize); a
// zeroed buffer is a safe sigaction because sa_handler is the first field
// of struct sigaction and zero is SIG_DFL. The buffer is sized past the
// kernel's struct sigaction (32 bytes on linux); the kernel copies only
// its own size, so the extra words are harmless.
func setDefaultAction(sig syscall.Signal) {
	var act [64]byte
	_, _, _ = syscall.Syscall6(syscall.SYS_RT_SIGACTION, uintptr(sig),
		uintptr(unsafe.Pointer(&act)), 0, sigsetSize, 0, 0)
}

// unblockSignal clears sig from the calling thread's blocked set through a
// raw rt_sigprocmask, so the kernel can deliver the re-raise the caller
// sends itself. On linux the call takes (how, set, oset, setsize) and how
// 1 is SIG_UNBLOCK; the kernel reads sigsetSize bytes of the set, which
// covers the relayed signals.
func unblockSignal(sig syscall.Signal) {
	var set [16]byte
	set[int(sig)/8] |= 1 << (uint(int(sig)) % 8)
	_, _, _ = syscall.Syscall6(syscall.SYS_RT_SIGPROCMASK, 1, /* SIG_UNBLOCK */
		uintptr(unsafe.Pointer(&set)), 0, sigsetSize, 0, 0)
}
