package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/djalmajr/nau/internal/config"
)

// binPathDir builds a PATH directory that holds exactly the named
// executable files (a non-runnable placeholder body is enough:
// resolution only looks the names up).
func binPathDir(t *testing.T, names ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, n := range names {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// TestResolveBin is the resolution matrix: the auto value (empty or
// exactly herdr-soho) resolves to the first of soho then herdr-soho on
// PATH, else to the bare name soho; an explicit value (a path or
// another name) passes through unchanged.
func TestResolveBin(t *testing.T) {
	empty := t.TempDir()
	onlySoho := binPathDir(t, "soho")
	both := binPathDir(t, "soho", "herdr-soho")
	onlyLegacy := binPathDir(t, "herdr-soho")
	cases := []struct {
		name   string
		stored string
		path   string
		want   string
	}{
		{"auto: soho found", "", both, filepath.Join(both, "soho")},
		{"auto: only the legacy is found", "", onlyLegacy, filepath.Join(onlyLegacy, "herdr-soho")},
		{"auto: nothing found", "", empty, "soho"},
		{"auto: only soho is found", "", onlySoho, filepath.Join(onlySoho, "soho")},
		{"stored herdr-soho behaves as auto", "herdr-soho", both, filepath.Join(both, "soho")},
		{"stored herdr-soho falls back to the legacy path", "herdr-soho", onlyLegacy, filepath.Join(onlyLegacy, "herdr-soho")},
		{"stored herdr-soho with nothing on PATH resolves to the new name", "herdr-soho", empty, "soho"},
		{"explicit bare name passes through", "soho", onlyLegacy, "soho"},
		{"explicit relative path passes through", "./custom", empty, "./custom"},
		{"explicit absolute path passes through", "/opt/bin/backend", empty, "/opt/bin/backend"},
		{"explicit other name passes through", "backend", both, "backend"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PATH", tc.path)
			if got := config.ResolveBin(tc.stored); got != tc.want {
				t.Errorf("ResolveBin(%q) with PATH=%s = %q, want %q", tc.stored, tc.path, got, tc.want)
			}
		})
	}
}

// TestLoadResolvesSohoBin: Load applies the resolution to the stored
// value, so a fresh configuration (nothing stored) resolves to soho on
// PATH, and a legacy file written by an older release resolves to soho
// when it is present.
func TestLoadResolvesSohoBin(t *testing.T) {
	both := binPathDir(t, "soho", "herdr-soho")
	t.Setenv("PATH", both)

	// Fresh: nothing stored, soho resolves.
	fresh := t.TempDir()
	cfg, err := config.Load(fresh)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HerdrSohoBin != filepath.Join(both, "soho") {
		t.Errorf("fresh config: HerdrSohoBin = %q, want %q", cfg.HerdrSohoBin, filepath.Join(both, "soho"))
	}

	// A legacy file with the old default written by an older release.
	legacy := t.TempDir()
	legacyFile := legacy + "/config"
	data := "# legacy config\nherdr_soho_bin=herdr-soho\nmachine_label=m1\n"
	if err := os.WriteFile(legacyFile, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = config.Load(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HerdrSohoBin != filepath.Join(both, "soho") {
		t.Errorf("legacy stored value: HerdrSohoBin = %q, want the resolved soho %q", cfg.HerdrSohoBin, filepath.Join(both, "soho"))
	}
	// The legacy fallback: no soho on PATH, the legacy binary is used.
	onlyLegacy := binPathDir(t, "herdr-soho")
	t.Setenv("PATH", onlyLegacy)
	cfg, err = config.Load(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HerdrSohoBin != filepath.Join(onlyLegacy, "herdr-soho") {
		t.Errorf("legacy fallback: HerdrSohoBin = %q, want %q", cfg.HerdrSohoBin, filepath.Join(onlyLegacy, "herdr-soho"))
	}
}

// TestSetStoresAutoEmpty: setting herdr_soho_bin to the auto value
// (empty or a legacy name) stores the empty auto form in the file; an
// explicit value is stored unchanged. Setting the auto value again is
// idempotent and keeps the auto form.
func TestSetStoresAutoEmpty(t *testing.T) {
	both := binPathDir(t, "soho", "herdr-soho")
	t.Setenv("PATH", both)

	autoValues := []string{"", "herdr-soho"}
	for _, v := range autoValues {
		dir := t.TempDir()
		if err := config.Set(dir, "herdr_soho_bin", v); err != nil {
			t.Fatalf("Set(herdr_soho_bin=%q): %v", v, err)
		}
		data, err := os.ReadFile(dir + "/config")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "herdr_soho_bin=\n") {
			t.Errorf("Set(%q): the file does not store the empty auto form: %q", v, data)
		}
	}

	// Idempotent: auto set, then the legacy name again — the stored form
	// stays the empty auto form and the effective value is unchanged.
	dir := t.TempDir()
	if err := config.Set(dir, "herdr_soho_bin", ""); err != nil {
		t.Fatal(err)
	}
	if err := config.Set(dir, "herdr_soho_bin", "herdr-soho"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dir + "/config")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "herdr_soho_bin=\n") || strings.Contains(string(data), "herdr_soho_bin=herdr") {
		t.Errorf("idempotent auto: the file must keep the empty auto form: %q", data)
	}
	cfg, err := config.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HerdrSohoBin != filepath.Join(both, "soho") {
		t.Errorf("effective after the idempotent set = %q, want the resolved soho", cfg.HerdrSohoBin)
	}

	// Explicit values are stored unchanged.
	for v, want := range map[string]string{"soho": "herdr_soho_bin=soho", "/opt/bin/backend": "herdr_soho_bin=/opt/bin/backend"} {
		dir := t.TempDir()
		if err := config.Set(dir, "herdr_soho_bin", v); err != nil {
			t.Fatalf("Set(herdr_soho_bin=%q): %v", v, err)
		}
		data, err := os.ReadFile(dir + "/config")
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), want+"\n") {
			t.Errorf("Set(%q): the file must store %q: %q", v, want, data)
		}
	}
}

// TestSetOtherKeysKeepStoredForm: setting another key rewrites the file
// without persisting the resolved absolute path of the backend binary —
// the stored (auto) form is what the file keeps.
func TestSetOtherKeysKeepStoredForm(t *testing.T) {
	both := binPathDir(t, "soho", "herdr-soho")
	t.Setenv("PATH", both)
	dir := t.TempDir()
	if err := config.Set(dir, "machine_label", "m1"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dir + "/config")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "herdr_soho_bin=\n") {
		t.Errorf("a set of another key must keep the empty auto form in the file, got %q", data)
	}
	if strings.Contains(string(data), "herdr_soho_bin=soho") {
		t.Errorf("the resolved path must not be persisted: %q", data)
	}
}
