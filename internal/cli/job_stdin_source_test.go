package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/testutil/fakesoho"
)

// poisonStdin fails every Read and records that it was called.
type poisonStdin struct {
	reads atomic.Int64
}

func (p *poisonStdin) Read([]byte) (int, error) {
	p.reads.Add(1)
	return 0, errors.New("poison stdin was read")
}

// runWithStdin runs the CLI with the given stdin reader and the controlled
// child environment and returns stdout, stderr and the exit code.
func runWithStdin(cfgDir string, stdin io.Reader, args ...string) (string, string, int) {
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     stdin,
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    getenvFor(false),
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: cfgDir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// stdinSourceFile writes a body file under a temp dir and returns its path.
func stdinSourceFile(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte("file body"), 0o600); err != nil {
		t.Fatalf("write body file: %v", err)
	}
	return p
}

// TestJobStdinSourceFileBodyNeverReadsStdin: with an explicit file body the
// CLI never reads stdin and the child gets an empty stdin.
func TestJobStdinSourceFileBodyNeverReadsStdin(t *testing.T) {
	file := stdinSourceFile(t, "body.md")
	cases := []struct {
		name string
		args []string
	}{
		{"start --brief file", []string{"job", "start", "--id", "J1", "--repo", "org/repo", "--brief", file}},
		{"start --brief=file", []string{"job", "start", "--id", "J1", "--repo", "org/repo", "--brief=" + file}},
		{"amend file", []string{"job", "amend", "--id", "J1", file}},
		{"amend --id=J1 file", []string{"job", "amend", "--id=J1", file}},
		{"send file", []string{"job", "send", "--id", "J1", file}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wantOut := `{"id":"J1","status":"running"}` + "\n"
			exe, fakeDir := installFakeSoho(t, fakesoho.Rule{
				Argv: tc.args[:2], ArgvPrefix: true,
				Stdout: wantOut, Stderr: "scripted-err", Code: 0,
			})
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			poison := &poisonStdin{}
			stdout, stderr, exit := runWithStdin(cfgDir, poison, tc.args...)
			if exit != 0 || stdout != wantOut || stderr != "scripted-err" {
				t.Errorf("exit = %d, stdout = %q, stderr = %q", exit, stdout, stderr)
			}
			if n := poison.reads.Load(); n != 0 {
				t.Errorf("stdin was read %d times, want 0", n)
			}
			calls, err := fakesoho.ReadCalls(fakeDir)
			if err != nil || len(calls) != 1 {
				t.Fatalf("calls = %d, err = %v, want 1 call", len(calls), err)
			}
			if got := strings.Join(calls[0].Argv, "\x00"); got != strings.Join(tc.args, "\x00") {
				t.Errorf("child argv = %q, want %q", calls[0].Argv, tc.args)
			}
			if calls[0].Stdin != "" {
				t.Errorf("child stdin = %q, want empty", calls[0].Stdin)
			}
		})
	}
}

// TestJobStdinSourceNeverClosingStdin: a stdin that never delivers a byte
// nor EOF does not hold a start with a file body.
func TestJobStdinSourceNeverClosingStdin(t *testing.T) {
	file := stdinSourceFile(t, "brief.json")
	exe, _ := installFakeSoho(t, fakesoho.Rule{
		Argv: []string{"job", "start"}, ArgvPrefix: true,
		Stdout: `{"id":"J1","status":"running"}` + "\n", Code: 0,
	})
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	pr, pw := io.Pipe()
	// Always unblock a read stuck on the pipe, so nothing leaks.
	t.Cleanup(func() { _ = pw.CloseWithError(errors.New("test cleanup")) })
	type result struct {
		stdout string
		exit   int
	}
	done := make(chan result, 1)
	go func() {
		stdout, _, exit := runWithStdin(cfgDir, pr, "job", "start", "--id", "J1", "--repo", "org/repo", "--brief", file)
		done <- result{stdout, exit}
	}()
	select {
	case r := <-done:
		if r.exit != 0 || r.stdout != `{"id":"J1","status":"running"}`+"\n" {
			t.Errorf("exit = %d, stdout = %q", r.exit, r.stdout)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("job start --brief <file> did not return within 10s: it waits on stdin that never closes")
	}
}

// TestJobStdinSourceControlsRealStdin: when the body is stdin (explicit "-"
// or absent) the bytes are read capped and forwarded exactly as before.
func TestJobStdinSourceControlsRealStdin(t *testing.T) {
	cases := []struct {
		name string
		args []string
		cap  int
	}{
		{"start --brief -", []string{"job", "start", "--id", "J1", "--repo", "org/repo", "--brief", "-"}, 256 * 1024},
		{"start --brief=-", []string{"job", "start", "--id", "J1", "--repo", "org/repo", "--brief=-"}, 256 * 1024},
		{"start absent", []string{"job", "start", "--id", "J1", "--repo", "org/repo"}, 256 * 1024},
		{"amend -", []string{"job", "amend", "--id", "J1", "-"}, 64 * 1024},
		{"amend absent", []string{"job", "amend", "--id", "J1"}, 64 * 1024},
		{"send -", []string{"job", "send", "--id", "J1", "-"}, 16 * 1024},
		{"send absent", []string{"job", "send", "--id", "J1"}, 16 * 1024},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rule := fakesoho.Rule{Argv: tc.args[:2], ArgvPrefix: true, Stdout: `{"id":"J1"}` + "\n", Code: 0}
			// Small body: forwarded exactly.
			exe, fakeDir := installFakeSoho(t, rule)
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			stdout, _, exit := runWithStdin(cfgDir, strings.NewReader("body-σ"), tc.args...)
			if exit != 0 || stdout != `{"id":"J1"}`+"\n" {
				t.Errorf("small: exit = %d, stdout = %q", exit, stdout)
			}
			calls, err := fakesoho.ReadCalls(fakeDir)
			if err != nil || len(calls) != 1 || calls[0].Stdin != "body-σ" {
				t.Fatalf("small: calls = %+v, err = %v", calls, err)
			}
			// At the cap: forwarded in full.
			exe, fakeDir = installFakeSoho(t, rule)
			cfgDir = t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			_, _, exit = runWithStdin(cfgDir, strings.NewReader(strings.Repeat("x", tc.cap)), tc.args...)
			if exit != 0 {
				t.Errorf("at cap: exit = %d", exit)
			}
			calls, err = fakesoho.ReadCalls(fakeDir)
			if err != nil || len(calls) != 1 || len(calls[0].Stdin) != tc.cap {
				t.Fatalf("at cap: calls = %d, err = %v", len(calls), err)
			}
			// One byte over: exit 2, no subprocess.
			exe, fakeDir = installFakeSoho(t, rule)
			cfgDir = t.TempDir()
			setSohoConfig(t, cfgDir, exe, "machine-a")
			stdout, _, exit = runWithStdin(cfgDir, strings.NewReader(strings.Repeat("x", tc.cap+1)), tc.args...)
			if exit != 2 || !strings.Contains(stdout, "stdin exceeds the contract limit") {
				t.Errorf("over cap: exit = %d, stdout = %q", exit, stdout)
			}
			if _, err := os.Stat(fakesoho.LogPath(fakeDir)); !os.IsNotExist(err) {
				t.Errorf("over cap: the subprocess ran (log exists)")
			}
		})
	}
}

// TestJobStdinSourceErrorMappings: refusals before the subprocess are
// unchanged and keep their relation to the stdin read.
func TestJobStdinSourceErrorMappings(t *testing.T) {
	file := stdinSourceFile(t, "body.md")
	t.Run("invalid id with a file body", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, fakesoho.Rule{AnyArgs: true, Stdout: "should not run\n"})
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		poison := &poisonStdin{}
		stdout, _, exit := runWithStdin(cfgDir, poison, "job", "amend", "--id", "bad id", file)
		if exit != 2 || !strings.Contains(stdout, "invalid --id") {
			t.Errorf("exit = %d, stdout = %q", exit, stdout)
		}
		if n := poison.reads.Load(); n != 0 {
			t.Errorf("stdin was read %d times, want 0", n)
		}
		if _, err := os.Stat(fakesoho.LogPath(fakeDir)); !os.IsNotExist(err) {
			t.Errorf("the subprocess ran (log exists)")
		}
	})
	t.Run("unreadable stdin with --brief -", func(t *testing.T) {
		exe, fakeDir := installFakeSoho(t, fakesoho.Rule{AnyArgs: true, Stdout: "should not run\n"})
		cfgDir := t.TempDir()
		setSohoConfig(t, cfgDir, exe, "machine-a")
		poison := &poisonStdin{}
		stdout, _, exit := runWithStdin(cfgDir, poison, "job", "start", "--id", "J1", "--repo", "org/repo", "--brief", "-")
		if exit != 2 || !strings.Contains(stdout, "job start: cannot read stdin: poison stdin was read") {
			t.Errorf("exit = %d, stdout = %q", exit, stdout)
		}
		if poison.reads.Load() == 0 {
			t.Errorf("stdin was not read for --brief -")
		}
		if _, err := os.Stat(fakesoho.LogPath(fakeDir)); !os.IsNotExist(err) {
			t.Errorf("the subprocess ran (log exists)")
		}
	})
}

// TestJobStdinSourceBookkeepingFileBody: a successful start or amend with a
// file body is tracked and recorded as with a stdin body.
func TestJobStdinSourceBookkeepingFileBody(t *testing.T) {
	file := stdinSourceFile(t, "body.md")
	startOut := `{"id":"J1","status":"running","motivo":null}` + "\n"
	exe, _ := installFakeSoho(t,
		fakesoho.Rule{Argv: []string{"job", "start"}, ArgvPrefix: true, Stdout: startOut, Code: 0},
		fakesoho.Rule{Argv: []string{"job", "amend"}, ArgvPrefix: true, Stdout: `{"id":"J1","delivered":true}` + "\n", Code: 0},
	)
	cfgDir := t.TempDir()
	setSohoConfig(t, cfgDir, exe, "machine-a")
	poison := &poisonStdin{}
	stdout, _, exit := runWithStdin(cfgDir, poison, "job", "start", "--id", "J1", "--repo", "org/repo", "--brief", file)
	if exit != 0 || stdout != startOut {
		t.Fatalf("start: exit = %d, stdout = %q", exit, stdout)
	}
	s, err := outbox.Open(outbox.StateDir(cfgDir), outbox.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	jobs, err := s.LoadJobs()
	if err != nil {
		t.Fatalf("LoadJobs: %v", err)
	}
	if j := jobs.Jobs["J1"]; j == nil || j.Projeto != "org/repo" || j.Estado != "running" || j.Closed {
		t.Errorf("job J1 = %+v", j)
	}
	recs := readOutboxRecords(t, cfgDir)
	if len(recs) != 1 {
		t.Fatalf("records after start = %d, want 1", len(recs))
	}
	if r := recs[0]; r.Tipo != outbox.TipoDispatch || r.Maquina != "machine-a" || r.Projeto != "org/repo" ||
		r.JobID == nil || *r.JobID != "J1" || string(r.Dados) != strings.TrimSpace(startOut) {
		t.Errorf("dispatch record = %+v dados %s", r, r.Dados)
	}
	if _, _, exit := runWithStdin(cfgDir, poison, "job", "amend", "--id", "J1", file); exit != 0 {
		t.Fatalf("amend: exit = %d", exit)
	}
	recs = readOutboxRecords(t, cfgDir)
	if len(recs) != 2 {
		t.Fatalf("records after amend = %d, want 2", len(recs))
	}
	r := recs[1]
	if r.Tipo != outbox.TipoAmend || r.Maquina != "machine-a" || r.Projeto != "org/repo" || r.JobID == nil || *r.JobID != "J1" {
		t.Errorf("amend record = %+v", r)
	}
	var dados struct {
		Refs struct {
			Tipo string `json:"tipo"`
		} `json:"refs"`
		Resposta json.RawMessage `json:"resposta"`
	}
	if err := json.Unmarshal(r.Dados, &dados); err != nil {
		t.Fatalf("amend dados: %v", err)
	}
	if dados.Refs.Tipo != "amend" || string(dados.Resposta) != `{"id":"J1","delivered":true}` {
		t.Errorf("amend dados = %s", r.Dados)
	}
	if n := poison.reads.Load(); n != 0 {
		t.Errorf("stdin was read %d times, want 0", n)
	}
}
