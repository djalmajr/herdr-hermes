//go:build !windows

package soho_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/djalmajr/nau/internal/soho"
)

// The argv markers that route a re-executed copy of this test binary
// into the fixture roles of the signal tests. The pattern is the one
// internal/testutil/fakesoho and the proctree tests already use: the
// test binary plays the role when it is re-executed with a marker, and
// just runs the tests otherwise.
const (
	// resistantRunnerMarker is the helper role: it runs the
	// production runner (soho.Runner.Run) with a long bound on the
	// resistant backend, the way the CLI runs a bounded job.
	resistantRunnerMarker = "-soho-resistant-runner"
	// resistantBackendMarker is the fake backend role: it ignores
	// SIGINT in its own process and starts a grandchild that holds
	// the runner's stdout.
	resistantBackendMarker = "-soho-resistant-backend"
	// resistantGrandchildMarker is the grandchild role: it holds the
	// stdout and stderr the backend gave it until it is killed.
	resistantGrandchildMarker = "-soho-resistant-grandchild"
	// quietRunnerMarker is the no-signal helper role: it runs the
	// production runner on the quick child, then holds after Run
	// returned so the test can probe the process's signal behaviour.
	quietRunnerMarker = "-soho-quiet-runner"
	// quickChildMarker is the quick child role: it exits 0 at once.
	quickChildMarker = "-soho-quick-child"
)

// roleDie ends a role with a distinguishable code and the reason on
// stderr, so a test that re-executes a role sees a start failure, not
// a silent hang.
func roleDie(err error) {
	fmt.Fprintln(os.Stderr, "soho role:", err)
	os.Exit(126)
}

// runSoHoRole runs the fixture role named by argv[1], if any, and
// reports whether it handled the argv. A role ends the process; an
// unknown marker leaves the argv for the caller (the fakesoho fake).
func runSoHoRole(argv []string) bool {
	switch argv[1] {
	case resistantRunnerMarker:
		if len(argv) != 3 {
			roleDie(fmt.Errorf("usage: %s <dir>", resistantRunnerMarker))
		}
		runResistantRunner(argv[2])
	case resistantBackendMarker:
		if len(argv) != 3 {
			roleDie(fmt.Errorf("usage: %s <dir>", resistantBackendMarker))
		}
		runResistantBackend(argv[2])
	case resistantGrandchildMarker:
		if len(argv) != 2 {
			roleDie(fmt.Errorf("usage: %s", resistantGrandchildMarker))
		}
		runResistantGrandchild()
	case quietRunnerMarker:
		if len(argv) != 3 {
			roleDie(fmt.Errorf("usage: %s <dir>", quietRunnerMarker))
		}
		runQuietRunner(argv[2])
	case quickChildMarker:
		if len(argv) != 2 {
			roleDie(fmt.Errorf("usage: %s", quickChildMarker))
		}
		runQuickChild()
	default:
		return false
	}
	return true
}

// runResistantRunner is the helper role of the resistant-backend test:
// it runs the production runner (the same Run the CLI runs a bounded
// job with) with a long bound on the resistant backend, so the only
// way the interrupted run ends is the relay's handling of the signal
// the test sends to the helper's group. The stderr line marks the
// fallback path: the re-raise ends the process on the signal, so a
// return of Run at all is itself the anomaly the test reports.
func runResistantRunner(dir string) {
	exe, err := os.Executable()
	if err != nil {
		roleDie(err)
	}
	r := soho.Runner{Bin: exe, Environ: os.Environ()}
	code, runErr := r.Run(context.Background(), []string{resistantBackendMarker, dir}, nil, os.Stdout, os.Stderr, 120*time.Second)
	fmt.Fprintf(os.Stderr, "soho resistant runner: Run returned code=%d err=%v\n", code, runErr)
	os.Exit(0)
}

// runResistantBackend is the fake backend role: it ignores SIGINT in
// its own process with the package's pattern (Notify first, then
// Ignore — the kernel-level ignore the grandchild it starts next
// inherits across the exec, so the whole backend tree survives the
// forwarded signal from its first instruction), and it starts the
// grandchild on the backend's own stdout and stderr, so the pipe the
// runner pointed at the backend stays held through the grandchild's
// lifetime. It writes its own and the grandchild's pid to the dir's
// pid files, then holds until it is killed.
func runResistantBackend(dir string) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT)
	signal.Ignore(syscall.SIGINT)
	exe, err := os.Executable()
	if err != nil {
		roleDie(err)
	}
	grand := exec.Command(exe, resistantGrandchildMarker)
	// os/exec connects a nil Stdout/Stderr to the null device, it does
	// not inherit: the grandchild must take the backend's own
	// descriptors explicitly, so the pipe the runner pointed at the
	// backend stays held through the grandchild's lifetime.
	grand.Stdout = os.Stdout
	grand.Stderr = os.Stderr
	if err := grand.Start(); err != nil {
		roleDie(err)
	}
	pid := os.Getpid()
	if err := os.WriteFile(filepath.Join(dir, "backend.pid"), []byte(strconv.Itoa(pid)), 0o600); err != nil {
		roleDie(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "grandchild.pid"), []byte(strconv.Itoa(grand.Process.Pid)), 0o600); err != nil {
		roleDie(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

// runResistantGrandchild is the grandchild role: it holds the stdout
// and stderr the backend gave it (the pipe the runner pointed at the
// backend) until it is killed. The sleep loop keeps a timer pending,
// so the runtime's deadlock check (all goroutines asleep with no
// timer) does not end the holder.
func runResistantGrandchild() {
	for {
		time.Sleep(time.Hour)
	}
}

// runQuietRunner is the helper role of the no-signal reception test:
// it runs the production runner on the quick child, which exits 0
// with no signal anywhere; after Run returned it writes the ready
// marker and holds until the test ends the process, so the test can
// probe the process's signal behaviour after the run.
func runQuietRunner(dir string) {
	exe, err := os.Executable()
	if err != nil {
		roleDie(err)
	}
	r := soho.Runner{Bin: exe, Environ: os.Environ()}
	code, runErr := r.Run(context.Background(), []string{quickChildMarker}, nil, os.Stdout, os.Stderr, 30*time.Second)
	if runErr != nil {
		roleDie(fmt.Errorf("Run returned an error on the no-signal run: code=%d err=%v", code, runErr))
	}
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("1"), 0o600); err != nil {
		roleDie(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

// runQuickChild is the quick child role: it exits 0 at once, so the
// runner's run ends on its own without any signal.
func runQuickChild() {
	os.Exit(0)
}
