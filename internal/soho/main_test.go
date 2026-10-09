package soho_test

import (
	"os"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// TestMain lets the re-executed test binary act as the fake herdr-soho.
func TestMain(m *testing.M) {
	fakesoho.Main()
	os.Exit(m.Run())
}
