//go:build windows

package fakesoho

// ignoreTermForSleep is a no-op on Windows: there is no SIGTERM, and the
// runner's cancel is a Kill.
func ignoreTermForSleep() {}
