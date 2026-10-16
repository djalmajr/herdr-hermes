package soho_test

import (
	"os"
	"testing"

	"github.com/djalmajr/nau/internal/testutil/fakesoho"
)

// TestMain lets the re-executed test binary act as the fake herdr-soho
// and as the fixture roles of the signal tests: a re-executed copy
// started with a role marker runs the role and exits (runSoHoRole),
// and run as the test binary it just runs the tests.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && runSoHoRole(os.Args) {
		return
	}
	fakesoho.Main()
	os.Exit(m.Run())
}
