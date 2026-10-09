// Process-level regression for the job forwarder: the real herdr-hermes
// binary is built once (TestMain) and run against the fake herdr-soho
// while the test's stdout reader pauses 15s before draining, so the CLI's
// stdout pipe stays full while the command is still running. The child's
// full output must still reach the pipe and the forwarder's friction
// diagnostic must not be lost: a stalled consumer must never make the CLI
// exit with only the pipe buffer's worth of bytes.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

const (
	// buildBound bounds the one-shot go build of the CLI in TestMain.
	buildBound = 180 * time.Second
	// cliCaseBound is the strict outer deadline of one CLI case.
	cliCaseBound = 90 * time.Second
	// graceWaitDelay is the WaitDelay of the build and of every CLI run.
	graceWaitDelay = 5 * time.Second
	// readerPause is the exact pause before the test drains the CLI's
	// stdout. It is longer than the 10s delivery bound an earlier
	// forwarder had, which exited while the pipe was still full.
	readerPause = 15 * time.Second
)

var (
	// bigOut is 1 MiB of the bytes a..p plus one final newline: exactly
	// one newline, no byte outside a..p, deterministic.
	bigOut = cyclePayload(1<<20, 'a')
	// bigErr is 256 KiB of the bytes A..P plus one final newline.
	bigErr = cyclePayload(256<<10, 'A')
	// startLine is the small job start/status JSON line the fake prints
	// first.
	startLine = `{"id":"J1","status":"running"}` + "\n"
	// frictionDiag is the runner's stalled-consumer diagnostic for
	// `job status` (the runner's grace period is 5s).
	frictionDiag = "herdr-soho job status exited 0 but its output did not drain within 5s; output may be truncated"
)

// cyclePayload returns n bytes cycling lo..lo+15 and one final newline.
func cyclePayload(n int, lo byte) []byte {
	b := make([]byte, n+1)
	for i := 0; i < n; i++ {
		b[i] = lo + byte(i%16)
	}
	b[n] = '\n'
	return b
}

// TestMain lets the re-executed test binary act as the fake herdr-soho,
// then builds the real CLI once (into a temp dir removed after m.Run) and
// runs the package.
func TestMain(m *testing.M) {
	fakesoho.Main()
	bin, err := buildCLI()
	if err != nil {
		fmt.Fprintf(os.Stderr, "herdr-hermes test: %v\n", err)
		os.Exit(1)
	}
	cliPath = bin
	code := m.Run()
	_ = os.RemoveAll(filepath.Dir(bin))
	os.Exit(code)
}

// cliPath is the real CLI binary built by TestMain.
var cliPath string

// buildCLI builds the real CLI binary out of tree with the go tool found
// on the test's PATH (the go tool puts its own bin dir there), under a
// context deadline and a WaitDelay, with no shell.
func buildCLI() (string, error) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		return "", fmt.Errorf("look up go on PATH: %w", err)
	}
	dir, err := os.MkdirTemp("", "herdr-hermes-cli-build-")
	if err != nil {
		return "", fmt.Errorf("create build dir: %w", err)
	}
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	out := filepath.Join(dir, "herdr-hermes"+ext)
	ctx, cancel := context.WithTimeout(context.Background(), buildBound)
	defer cancel()
	cmd := exec.CommandContext(ctx, goBin, "build", "-o", out, "github.com/djalmajr/herdr-hermes/cmd/herdr-hermes")
	cmd.WaitDelay = graceWaitDelay
	if o, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("go build: %v\n%s", err, o)
	}
	return out, nil
}

// runSpec configures one process-level case.
type runSpec struct {
	name     string
	argv     []string
	stdin    string
	rules    []fakesoho.Rule
	label    bool          // machine_label=machine-a in the config (bookkeeping)
	nowrite  bool          // HERDR_HERMES_NOWRITE=1 in the child environment
	shared   bool          // the CLI's stdout and stderr share one pipe
	outPause time.Duration // pause before draining the CLI's stdout
	errPause time.Duration // pause before draining the CLI's stderr
	wantOut  int           // expected stdout bytes (0: no byte assertion)
	wantErr  int           // expected stderr bytes (0: no byte assertion)
}

// runResult is the observed outcome of one case.
type runResult struct {
	stdout  []byte
	stderr  []byte
	exit    int
	waitErr error
	elapsed time.Duration
	cfgDir  string
}

// cfgDirFor returns the herdr-hermes config dir the child resolves from
// its explicit environment; it matches os.UserConfigDir for the standard
// per-OS variables the test sets (HOME, XDG_CONFIG_HOME, APPDATA).
func cfgDirFor(root string) string {
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(root, "home", "Library", "Application Support", "herdr-hermes")
	case "windows":
		return filepath.Join(root, "appdata", "herdr-hermes")
	default:
		return filepath.Join(root, "xdg", "herdr-hermes")
	}
}

// runCase runs one case: it installs the fake, writes the config, runs
// the real CLI with an explicit environment, drains the pipes after the
// case pauses, reaps the CLI, and logs the one-line summary. It never
// hangs: the 90s context kills the CLI and the cleanup reaps it.
func runCase(t *testing.T, spec runSpec) runResult {
	t.Helper()
	t.Parallel()
	root := t.TempDir()
	exe := fakesoho.Install(t, filepath.Join(root, "fake"), spec.rules...)
	cfgDir := cfgDirFor(root)
	if err := os.MkdirAll(filepath.Dir(cfgDir), 0o700); err != nil {
		t.Fatalf("case %s: config dir parent: %v", spec.name, err)
	}
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatalf("case %s: config dir: %v", spec.name, err)
	}
	cfg := "herdr_soho_bin=" + exe + "\n"
	if spec.label {
		cfg += "machine_label=machine-a\n"
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "config"), []byte(cfg), 0o600); err != nil {
		t.Fatalf("case %s: write config: %v", spec.name, err)
	}
	env := []string{
		"HOME=" + filepath.Join(root, "home"),
		"XDG_CONFIG_HOME=" + filepath.Join(root, "xdg"),
		"APPDATA=" + filepath.Join(root, "appdata"),
	}
	if spec.nowrite {
		env = append(env, "HERDR_HERMES_NOWRITE=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cliCaseBound)
	defer cancel()
	cmd := exec.CommandContext(ctx, cliPath, spec.argv...)
	cmd.Env = env
	cmd.WaitDelay = graceWaitDelay
	if spec.stdin != "" {
		cmd.Stdin = strings.NewReader(spec.stdin)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("case %s: stdout pipe: %v", spec.name, err)
	}
	cmd.Stdout = outW
	var errR, errW *os.File
	if spec.shared {
		cmd.Stderr = outW
	} else {
		errR, errW, err = os.Pipe()
		if err != nil {
			t.Fatalf("case %s: stderr pipe: %v", spec.name, err)
		}
		cmd.Stderr = errW
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("case %s: start CLI: %v", spec.name, err)
	}
	// Close the write ends now: the child holds its own copy since Start,
	// and keeping them open here would hold the pipe past the child's
	// exit, so the readers would never see EOF and this case would hang.
	_ = outW.Close()
	if errW != nil {
		_ = errW.Close()
	}
	var (
		waitOnce sync.Once
		waitErr  error
	)
	reap := func() error {
		waitOnce.Do(func() { waitErr = cmd.Wait() })
		return waitErr
	}
	t.Cleanup(func() {
		_ = reap()
	})
	start := time.Now()
	type stream struct {
		b   []byte
		err error
	}
	drain := func(r *os.File, pause time.Duration) stream {
		if pause > 0 {
			time.Sleep(pause)
		}
		b, err := io.ReadAll(r)
		return stream{b, err}
	}
	outCh := make(chan stream, 1)
	go func() { outCh <- drain(outR, spec.outPause) }()
	var errCh chan stream
	if !spec.shared {
		errCh = make(chan stream, 1)
		go func() { errCh <- drain(errR, spec.errPause) }()
	}
	out := <-outCh
	if out.err != nil {
		t.Fatalf("case %s: read stdout: %v", spec.name, out.err)
	}
	res := runResult{stdout: out.b, cfgDir: cfgDir}
	if errCh != nil {
		errS := <-errCh
		if errS.err != nil {
			t.Fatalf("case %s: read stderr: %v", spec.name, errS.err)
		}
		res.stderr = errS.b
	}
	res.waitErr = reap()
	res.exit = -1
	if cmd.ProcessState != nil {
		res.exit = cmd.ProcessState.ExitCode()
	}
	res.elapsed = time.Since(start)
	stderrView := res.stderr
	if spec.shared {
		stderrView = filterAlphabet(res.stdout, 'A')
	}
	t.Logf("case=%s exit=%d elapsed=%s stdout_bytes=%d/%d stderr_bytes=%d/%d",
		spec.name, res.exit, res.elapsed, len(res.stdout), spec.wantOut, len(stderrView), spec.wantErr)
	return res
}

// filterAlphabet keeps the bytes lo..lo+15. The payloads use disjoint
// alphabets and exactly one final newline per stream, so the letters of
// each stream are recovered exactly from a shared pipe no matter how
// the two streams' chunks interleave.
func filterAlphabet(b []byte, lo byte) []byte {
	out := make([]byte, 0, len(b))
	for _, c := range b {
		if c >= lo && c < lo+16 {
			out = append(out, c)
		}
	}
	return out
}

// listDir returns the slash paths of every entry under root, relative to
// root, in walk order.
func listDir(t *testing.T, root string) []string {
	t.Helper()
	var names []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel != "." {
			names = append(names, filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return names
}

// modeOf returns the file mode of p, failing the case on error.
func modeOf(t *testing.T, p string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat %s: %v", p, err)
	}
	return fi.Mode()
}

// TestProcessStalledReader runs the real CLI against the fake herdr-soho
// with a stdout reader that pauses 15s before draining. On a stalled
// consumer the CLI must deliver every byte the child wrote and keep the
// forwarder's diagnostic, not exit with the pipe buffer's worth.
func TestProcessStalledReader(t *testing.T) {
	statusArgs := []string{"job", "status", "--id", "J1"}
	bigStartOut := append(append([]byte(nil), startLine...), bigOut...)

	t.Run("separate-exit0", func(t *testing.T) {
		res := runCase(t, runSpec{
			name:     "separate-exit0",
			argv:     statusArgs,
			rules:    []fakesoho.Rule{{Argv: statusArgs, StdoutBytes: bigOut, StderrBytes: bigErr}},
			outPause: readerPause,
			wantOut:  len(bigOut),
			wantErr:  len(bigErr),
		})
		if res.exit != 0 {
			t.Errorf("exit = %d, want 0", res.exit)
		}
		if !bytes.Equal(res.stdout, bigOut) {
			t.Errorf("stdout = %d bytes, want exactly %d", len(res.stdout), len(bigOut))
		}
		if !bytes.Equal(res.stderr, bigErr) {
			t.Errorf("stderr = %d bytes, want exactly %d", len(res.stderr), len(bigErr))
		}
	})

	t.Run("separate-exit7", func(t *testing.T) {
		res := runCase(t, runSpec{
			name:     "separate-exit7",
			argv:     statusArgs,
			rules:    []fakesoho.Rule{{Argv: statusArgs, StdoutBytes: bigOut, StderrBytes: bigErr, Code: 7}},
			outPause: readerPause,
			errPause: readerPause,
			wantOut:  len(bigOut),
			wantErr:  len(bigErr),
		})
		if res.exit != 7 {
			t.Errorf("exit = %d, want 7", res.exit)
		}
		if !bytes.Equal(res.stdout, bigOut) {
			t.Errorf("stdout = %d bytes, want exactly %d", len(res.stdout), len(bigOut))
		}
		if !bytes.Equal(res.stderr, bigErr) {
			t.Errorf("stderr = %d bytes, want exactly %d", len(res.stderr), len(bigErr))
		}
	})

	t.Run("shared-stream", func(t *testing.T) {
		res := runCase(t, runSpec{
			name:     "shared-stream",
			argv:     statusArgs,
			rules:    []fakesoho.Rule{{Argv: statusArgs, StdoutBytes: bigOut, StderrBytes: bigErr}},
			shared:   true,
			outPause: readerPause,
			wantOut:  len(bigOut) + len(bigErr),
			wantErr:  len(bigErr) - 1, // the logged stderr view is its letters

		})
		if res.exit != 0 {
			t.Errorf("exit = %d, want 0", res.exit)
		}
		if n := len(res.stdout); n != len(bigOut)+len(bigErr) {
			t.Errorf("shared total = %d bytes, want %d", n, len(bigOut)+len(bigErr))
		}
		// Each stream's final newline is shared ground: compare the
		// letters of each stream exactly and count the newlines apart.
		if got, want := filterAlphabet(res.stdout, 'a'), bigOut[:len(bigOut)-1]; !bytes.Equal(got, want) {
			t.Errorf("stdout letters = %d, want exactly %d", len(got), len(want))
		}
		if got, want := filterAlphabet(res.stdout, 'A'), bigErr[:len(bigErr)-1]; !bytes.Equal(got, want) {
			t.Errorf("stderr letters = %d, want exactly %d", len(got), len(want))
		}
		if n := strings.Count(string(res.stdout), "\n"); n != 2 {
			t.Errorf("newline count = %d, want 2", n)
		}
	})

	t.Run("normal", func(t *testing.T) {
		res := runCase(t, runSpec{
			name:    "normal",
			argv:    statusArgs,
			rules:   []fakesoho.Rule{{Argv: statusArgs, Stdout: startLine}},
			wantOut: len(startLine),
		})
		if res.exit != 0 {
			t.Errorf("exit = %d, want 0", res.exit)
		}
		if !bytes.Equal(res.stdout, []byte(startLine)) {
			t.Errorf("stdout = %q, want %q", res.stdout, startLine)
		}
		if len(res.stderr) != 0 {
			t.Errorf("stderr = %q, want empty", res.stderr)
		}
	})

	t.Run("start-bookkeeping", func(t *testing.T) {
		startArgs := []string{"job", "start", "--id", "J1", "--repo", "org/repo"}
		res := runCase(t, runSpec{
			name:     "start-bookkeeping",
			argv:     startArgs,
			stdin:    "brief",
			rules:    []fakesoho.Rule{{Argv: startArgs, Stdout: startLine, StdoutBytes: bigOut}},
			label:    true,
			outPause: readerPause,
			wantOut:  len(bigStartOut),
		})
		if res.exit != 0 {
			t.Errorf("exit = %d, want 0", res.exit)
		}
		if !bytes.Equal(res.stdout, bigStartOut) {
			t.Errorf("stdout = %d bytes, want exactly %d", len(res.stdout), len(bigStartOut))
		}
		stateDir := filepath.Join(res.cfgDir, "state")
		jobsData, err := os.ReadFile(filepath.Join(stateDir, "jobs.json"))
		if err != nil {
			t.Fatalf("jobs.json: %v", err)
		}
		var doc struct {
			Jobs map[string]struct {
				ID      string `json:"id"`
				Projeto string `json:"projeto"`
				Estado  string `json:"estado"`
			} `json:"jobs"`
		}
		if err := json.Unmarshal(jobsData, &doc); err != nil {
			t.Fatalf("jobs.json: %v", err)
		}
		j, ok := doc.Jobs["J1"]
		if !ok || j.ID != "J1" || j.Projeto != "org/repo" || j.Estado != "running" {
			t.Errorf("jobs.json: J1 = %+v (present %v), want id=J1 projeto=org/repo estado=running", j, ok)
		}
		outboxData, err := os.ReadFile(filepath.Join(stateDir, "outbox.jsonl"))
		if err != nil {
			t.Fatalf("outbox.jsonl: %v", err)
		}
		dispatch := 0
		for _, line := range strings.Split(strings.TrimSpace(string(outboxData)), "\n") {
			if line == "" {
				continue
			}
			var rec struct {
				Tipo string `json:"tipo"`
			}
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("outbox.jsonl line %q: %v", line, err)
			}
			if rec.Tipo == "dispatch" {
				dispatch++
			}
		}
		if dispatch != 1 {
			t.Errorf("outbox.jsonl dispatch records = %d, want 1", dispatch)
		}
		if runtime.GOOS != "windows" {
			if m := modeOf(t, stateDir); m&0o777 != 0o700 {
				t.Errorf("state dir mode = %o, want 700", m&0o777)
			}
			for _, f := range []string{"jobs.json", "outbox.jsonl"} {
				if m := modeOf(t, filepath.Join(stateDir, f)); m&0o777 != 0o600 {
					t.Errorf("%s mode = %o, want 600", f, m&0o777)
				}
			}
		}
	})

	t.Run("nowrite", func(t *testing.T) {
		res := runCase(t, runSpec{
			name:     "nowrite",
			argv:     statusArgs,
			rules:    []fakesoho.Rule{{Argv: statusArgs, StdoutBytes: bigOut}},
			nowrite:  true,
			outPause: readerPause,
			wantOut:  len(bigOut),
		})
		if res.exit != 0 {
			t.Errorf("exit = %d, want 0", res.exit)
		}
		if !bytes.Equal(res.stdout, bigOut) {
			t.Errorf("stdout = %d bytes, want exactly %d", len(res.stdout), len(bigOut))
		}
		if got := listDir(t, res.cfgDir); !reflect.DeepEqual(got, []string{"config"}) {
			t.Errorf("config dir = %v, want exactly [config]", got)
		}
	})

	t.Run("fresh-friction", func(t *testing.T) {
		res := runCase(t, runSpec{
			name:    "fresh-friction",
			argv:    statusArgs,
			rules:   []fakesoho.Rule{{Argv: statusArgs, Stdout: startLine, HoldPipeMs: 8000}},
			wantOut: len(startLine),
		})
		if res.exit != 0 {
			t.Errorf("exit = %d, want 0", res.exit)
		}
		if !bytes.Equal(res.stdout, []byte(startLine)) {
			t.Errorf("stdout = %q, want %q", res.stdout, startLine)
		}
		if n := strings.Count(string(res.stderr), frictionDiag); n != 1 {
			t.Errorf("stderr has %d copies of the diagnostic, want 1 (stderr %q)", n, res.stderr)
		}
		stateDir := filepath.Join(res.cfgDir, "state")
		logData, err := os.ReadFile(filepath.Join(stateDir, "friction.log"))
		if err != nil {
			t.Fatalf("friction.log: %v", err)
		}
		if n := strings.Count(string(logData), frictionDiag); n != 1 {
			t.Errorf("friction.log has %d copies of the diagnostic, want 1 (log %q)", n, logData)
		}
		if strings.Contains(string(logData), `{"id":"J1","status":"running"}`) {
			t.Errorf("friction.log contains the child's stdout: %q", logData)
		}
		if runtime.GOOS != "windows" {
			if m := modeOf(t, stateDir); m&0o777 != 0o700 {
				t.Errorf("state dir mode = %o, want 700", m&0o777)
			}
			if m := modeOf(t, filepath.Join(stateDir, "friction.log")); m&0o777 != 0o600 {
				t.Errorf("friction.log mode = %o, want 600", m&0o777)
			}
		}
	})
}
