//go:build windows

package fakesoho

import "os"

// interruptSignals returns the signals that count as a terminal
// interruption on this platform; SIGTERM does not exist on Windows, so
// only os.Interrupt.
func interruptSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
