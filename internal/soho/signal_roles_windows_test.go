//go:build windows

package soho_test

import (
	"fmt"
	"os"
)

// The fixture roles of the signal tests exist on unix only: the tests
// that drive them are build-tagged !windows, and a re-executed copy
// started with one of the markers never happens on Windows. The
// markers are named here so the dispatch of the package's TestMain
// compiles on every platform.
const (
	resistantRunnerMarker     = "-soho-resistant-runner"
	resistantBackendMarker    = "-soho-resistant-backend"
	resistantGrandchildMarker = "-soho-resistant-grandchild"
	quietRunnerMarker         = "-soho-quiet-runner"
	quickChildMarker          = "-soho-quick-child"
)

// runSoHoRole never handles the fixture roles on Windows: the signal
// tests that build them do not run there. A re-executed copy started
// with one of the markers dies with the role's failure code, so a
// stray invocation sees a start failure, not a silent hang.
func runSoHoRole(argv []string) bool {
	switch argv[1] {
	case resistantRunnerMarker, resistantBackendMarker, resistantGrandchildMarker, quietRunnerMarker, quickChildMarker:
		fmt.Fprintln(os.Stderr, "soho role: the signal fixture roles are unix only")
		os.Exit(126)
	}
	return false
}
