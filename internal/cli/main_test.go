package cli

import (
	"fmt"
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
	// Fail closed against a real herdr-soho: after the re-executed roles
	// above (which exit before this point and are unaffected), the test
	// process PATH points at an empty directory, so the default
	// herdr-soho name never resolves to a binary on the machine — every
	// test that needs herdr-soho passes an absolute fake path — and
	// local runs behave like CI. The directory is removed after the run.
	emptyPath, err := os.MkdirTemp("", "herdr-hermes-test-path-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cli test: create empty PATH dir: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("PATH", emptyPath); err != nil {
		fmt.Fprintf(os.Stderr, "cli test: set PATH: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(emptyPath)
	os.Exit(code)
}

// testMainHook is set by platform-specific test files that re-execute the
// test binary as a helper (the pty helper of the terminal echo tests).
var testMainHook func()
