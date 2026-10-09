//go:build !windows

package fakesoho

import (
	"os"
	"syscall"
)

// interruptSignals returns the signals that count as a terminal
// interruption on this platform: os.Interrupt and SIGTERM.
func interruptSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
