package fakesoho

import (
	"os"
	"testing"
)

// TestMain lets the re-executed copy of this test binary play the fake
// role when it is installed as herdr-soho; running as the test binary it
// just runs the tests. It lives in a file without a build tag so the
// lock and warm tests, which also re-execute the test binary, run on
// every platform.
func TestMain(m *testing.M) {
	Main()
	os.Exit(m.Run())
}
