package cli

import (
	"os"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// TestMain lets the re-executed test binary act as the fake herdr-soho
// or as the pty helper of the terminal echo tests.
func TestMain(m *testing.M) {
	fakesoho.Main()
	if testMainHook != nil {
		testMainHook()
	}
	os.Exit(m.Run())
}

// testMainHook is set by platform-specific test files that re-execute the
// test binary as a helper (the pty helper of the terminal echo tests).
var testMainHook func()
