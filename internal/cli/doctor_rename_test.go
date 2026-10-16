package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/config"
	"github.com/djalmajr/nau/internal/testutil/fakesoho"
)

// countEnv reports how many entries of env carry the given key (exact,
// unix semantics), and whether every one of them equals want.
func countEnv(env []string, key, want string) (n int, allWant bool) {
	allWant = true
	for _, kv := range env {
		if k, _, ok := strings.Cut(kv, "="); ok && k == key {
			n++
			if kv != want {
				allWant = false
			}
		}
	}
	return
}

// TestDoctorConfigChildNowrite: the doctor's backend `config` call
// receives exactly one SOHO_NOWRITE=1 and one HERDR_SOHO_NOWRITE=1,
// with the inherited values of the two keys removed, whether the
// parent carried SOHO_NOWRITE, HERDR_SOHO_NOWRITE, both or none; the
// wake hook classification is decided from the backend answer and does
// not depend on which NOWRITE variables the parent had.
func TestDoctorConfigChildNowrite(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable(wakeRow), Code: 0},
	)
	cases := []struct {
		name   string
		parent []string
	}{
		{"parent without NOWRITE", fakeChildEnv},
		{"parent with SOHO_NOWRITE", append(append([]string{}, fakeChildEnv...), "SOHO_NOWRITE=0")},
		{"parent with HERDR_SOHO_NOWRITE", append(append([]string{}, fakeChildEnv...), "HERDR_SOHO_NOWRITE=0")},
		{"parent with both backend NOWRITE variables", append(append([]string{}, fakeChildEnv...), "SOHO_NOWRITE=0", "HERDR_SOHO_NOWRITE=0")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, exe, "")
			var out, errb bytes.Buffer
			env := Env{
				Stdin:     strings.NewReader(""),
				Stdout:    &out,
				Stderr:    &errb,
				Getenv:    func(string) string { return "" },
				Environ:   func() []string { return tc.parent },
				ConfigDir: cfgDir,
				Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
				Sleep:     sleepCtx,
			}
			exit := Run([]string{"doctor"}, env)
			if exit != 0 {
				t.Fatalf("doctor exit = %d, stderr %q", exit, errb.String())
			}
			if !strings.Contains(out.String(), `"wake_hook":"true"`) {
				t.Fatalf("the wake hook classification must not depend on the parent NOWRITE variables, stdout %q", out.String())
			}
			calls, err := fakesoho.ReadCalls(fakeDir)
			if err != nil {
				t.Fatalf("read the fake call log: %v", err)
			}
			var configCall *fakesoho.Call
			for i := range calls {
				if len(calls[i].Argv) == 1 && calls[i].Argv[0] == "config" {
					configCall = &calls[i]
					break
				}
			}
			if configCall == nil {
				t.Fatalf("no config call in the fake log: %v", calls)
			}
			nSoho, okSoho := countEnv(configCall.Env, "SOHO_NOWRITE", "SOHO_NOWRITE=1")
			if nSoho != 1 || !okSoho {
				t.Errorf("config child environment has %d SOHO_NOWRITE entries (all =1: %t), want exactly SOHO_NOWRITE=1: %v", nSoho, okSoho, configCall.Env)
			}
			nLegacy, okLegacy := countEnv(configCall.Env, "HERDR_SOHO_NOWRITE", "HERDR_SOHO_NOWRITE=1")
			if nLegacy != 1 || !okLegacy {
				t.Errorf("config child environment has %d HERDR_SOHO_NOWRITE entries (all =1: %t), want exactly HERDR_SOHO_NOWRITE=1: %v", nLegacy, okLegacy, configCall.Env)
			}
		})
	}
}

// TestDoctorLegacyNameWarning: with soho on the PATH and an explicit
// legacy selection, the doctor keeps its JSON line and exit code and
// adds the exact documented warning to stderr; without soho on the
// PATH the legacy selection is the machine's reality and no warning is
// printed; an auto selection never warns.
func TestDoctorLegacyNameWarning(t *testing.T) {
	exe, fakeDir := installFakeSoho(t,
		capsRule(capsJSON, 0),
		fakesoho.Rule{Argv: []string{"config"}, Stdout: configTable(wakeRow), Code: 0},
	)
	// A PATH entry that additionally carries a soho executable (a dummy
	// file is enough: the legacy selection does not run it; the .exe
	// twin covers the extension lookup on Windows).
	withSoho := t.TempDir()
	for _, n := range []string{"soho", "soho.exe"} {
		if err := os.WriteFile(withSoho+"/"+n, []byte("x"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	wantWarn := "nau: doctor: herdr_soho_bin points to the legacy herdr-soho binary while soho is on the PATH; 'nau config set herdr_soho_bin soho' switches to soho"
	cases := []struct {
		name     string
		bin      string
		path     string
		wantWarn bool
	}{
		{"legacy selection and soho on PATH warn", exe, withSoho + string(os.PathListSeparator) + fakeDir, true},
		{"legacy selection without soho on PATH does not warn", exe, fakeDir, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PATH", tc.path)
			cfgDir := t.TempDir()
			setSohoConfig(t, cfgDir, tc.bin, "")
			var out, errb bytes.Buffer
			env := Env{
				Stdin:     strings.NewReader(""),
				Stdout:    &out,
				Stderr:    &errb,
				Getenv:    func(string) string { return "" },
				Environ:   func() []string { return fakeChildEnv },
				ConfigDir: cfgDir,
				Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
				Sleep:     sleepCtx,
			}
			exit := Run([]string{"doctor"}, env)
			if exit != 0 {
				t.Fatalf("doctor exit = %d, want 0 (stderr %q)", exit, errb.String())
			}
			var line map[string]any
			if err := json.Unmarshal([]byte(out.String()), &line); err != nil {
				t.Fatalf("doctor stdout %q is not one JSON line: %v", out.String(), err)
			}
			if n := strings.Count(errb.String(), wantWarn); tc.wantWarn && n != 1 {
				t.Errorf("stderr %q must carry exactly one warning line %q, found %d", errb.String(), wantWarn, n)
			}
			if n := strings.Count(errb.String(), wantWarn); !tc.wantWarn && n != 0 {
				t.Errorf("stderr %q must carry no warning", errb.String())
			}
		})
	}
	// An auto selection (the empty stored value) never warns, even with
	// soho on the PATH: it resolved to soho itself.
	t.Setenv("PATH", withSoho)
	cfgDir := t.TempDir()
	if err := config.Set(cfgDir, "herdr_soho_bin", ""); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, exit := runHermes(t, cfgDir, false, "", "doctor")
	if exit != 43 {
		t.Fatalf("doctor with a non-runnable resolved soho exit = %d, want 43 (stdout %q)", exit, stdout)
	}
	if strings.Contains(stderr, "points to the legacy herdr-soho binary") {
		t.Errorf("an auto selection must not warn, stderr %q", stderr)
	}
}

// TestLegacyBackendName: the doctor's legacy-name check strips the .exe
// extension and compares the name without case on Windows, and is exact
// on the unix operating systems.
func TestLegacyBackendName(t *testing.T) {
	cases := []struct {
		bin     string
		windows bool
		want    bool
	}{
		{"herdr-soho", false, true},
		{"/opt/bin/herdr-soho", false, true},
		{"herdr-soho.exe", false, true},
		{"HERDR-SOHO", false, false},
		{"herdr-soho.EXE", false, false},
		{"soho", false, false},
		{"herdr-soho-dev", false, false},
		{"herdr-soho", true, true},
		{"herdr-soho.exe", true, true},
		{"HERDR-SOHO.EXE", true, true},
		{"herdr-soho.ExE", true, true},
		{"Herdr-Soho", true, true},
		{"soho.exe", true, false},
		{".exe", true, false},
		{"herdr-soho.exe.bak", true, false},
	}
	for _, c := range cases {
		if got := isLegacyBackendName(c.bin, c.windows); got != c.want {
			t.Errorf("isLegacyBackendName(%q, windows=%t) = %t, want %t", c.bin, c.windows, got, c.want)
		}
	}
}
