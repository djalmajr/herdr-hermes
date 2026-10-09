//go:build !windows

package fakesoho

import (
	"os"
	"os/signal"
	"syscall"
)

// ignoreTermForSleep makes a plain delay ignore SIGTERM for the duration
// of the sleep, so a runner deadline is decided by its WaitDelay grace
// period (unix) or its Kill (Windows), not by the signal's default
// action. The Notify is only there to install the runtime handler:
// without a prior Notify, Ignore leaves the signal's default action
// (kill) in place.
func ignoreTermForSleep() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM)
	signal.Ignore(syscall.SIGTERM)
}
