package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRouteLists covers the two list helpers: nil on the empty value and the
// items of the normalized value otherwise.
func TestRouteLists(t *testing.T) {
	var empty Config
	if got := empty.RouteMachineList(); got != nil {
		t.Errorf("RouteMachineList on empty config = %v, want nil", got)
	}
	if got := empty.RouteDisabledList(); got != nil {
		t.Errorf("RouteDisabledList on empty config = %v, want nil", got)
	}
	c := Config{RouteMachines: "mac-a,win-a", RouteDisabled: "win-a"}
	if got, want := c.RouteMachineList(), []string{"mac-a", "win-a"}; !sameList(got, want) {
		t.Errorf("RouteMachineList = %v, want %v", got, want)
	}
	if got, want := c.RouteDisabledList(), []string{"win-a"}; !sameList(got, want) {
		t.Errorf("RouteDisabledList = %v, want %v", got, want)
	}
}

func sameList(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestSetRouteNormalization: the stored and effective form is the normalized
// value (trimmed herdr_bin, comma-joined route lists).
func TestSetRouteNormalization(t *testing.T) {
	dir := t.TempDir()
	if err := Set(dir, "route_machines", " mac-a , win-a "); err != nil {
		t.Fatalf("Set route_machines: %v", err)
	}
	if err := Set(dir, "herdr_bin", "  herdr-x  "); err != nil {
		t.Fatalf("Set herdr_bin: %v", err)
	}
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RouteMachines != "mac-a,win-a" {
		t.Errorf("RouteMachines = %q, want %q", cfg.RouteMachines, "mac-a,win-a")
	}
	if cfg.HerdrBin != "herdr-x" {
		t.Errorf("HerdrBin = %q, want %q", cfg.HerdrBin, "herdr-x")
	}
	if v, err := cfg.Value("route_probe_timeout_s"); err != nil || v != "20" {
		t.Errorf("Value(route_probe_timeout_s) = %q (err %v), want %q", v, err, "20")
	}
}

// TestSetRouteRefusals: an invalid value is refused with an error that names
// the key and writes nothing, even when the file already exists.
func TestSetRouteRefusals(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		key string
		val string
	}{
		{"herdr_bin", ""},
		{"herdr_bin", "   "},
		{"route_machines", "a,,b"},
		{"route_machines", "a,"},
		{"route_machines", "-x"},
		{"route_machines", "mac-a,mac-a"},
		{"route_disabled", "win-a,win-a"},
		{"route_orchestrator_name", ""},
		{"route_orchestrator_name", "Orchestrator"},
		{"route_orchestrator_name", "9name"},
		{"route_probe_timeout_s", "0"},
		{"route_probe_timeout_s", "121"},
		{"route_probe_timeout_s", "x"},
	}
	for _, tc := range cases {
		if err := Set(dir, tc.key, tc.val); err == nil {
			t.Errorf("Set %s %q: want error, got nil", tc.key, tc.val)
		} else if !strings.Contains(err.Error(), tc.key) {
			t.Errorf("Set %s %q: error does not name the key: %v", tc.key, tc.val, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "config")); !os.IsNotExist(err) {
		t.Errorf("refused sets created the config file: %v", err)
	}
	// The refusal happens before any read or write of the file.
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("machine_label=lab-1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Set(dir, "route_machines", "-x"); err == nil {
		t.Error("Set route_machines -x over an existing file: want error, got nil")
	}
	after, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Errorf("refused Set changed the file:\n%s", after)
	}
	// A valid set is also refused when the file holds an invalid value.
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("route_machines=-x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Set(dir, "machine_label", "lab"); err == nil {
		t.Error("Set machine_label over an invalid file: want error, got nil")
	}
}

// TestLoadRouteErrorsNameTheKey: an invalid value in the file is refused at
// load time with an error that names the key.
func TestLoadRouteErrorsNameTheKey(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{"route_machines=mac-a,mac-a\n", "route_machines"},
		{"route_machines=-x\n", `invalid machine label "-x"`},
		{"route_machines=a,,b\n", "route_machines"},
		{"route_disabled=win-a,win-a\n", "route_disabled"},
		{"route_disabled=a,\n", "route_disabled"},
		{"route_orchestrator_name=Orchestrator\n", "route_orchestrator_name"},
		{"route_orchestrator_name=9name\n", "route_orchestrator_name"},
		{"route_probe_timeout_s=0\n", "route_probe_timeout_s must be an integer from 1 to 120"},
		{"route_probe_timeout_s=121\n", "route_probe_timeout_s must be an integer from 1 to 120"},
		{"route_probe_timeout_s=abc\n", "route_probe_timeout_s must be an integer from 1 to 120"},
		{"herdr_bin=\n", "herdr_bin"},
	}
	for _, tc := range cases {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "config"), []byte(tc.line), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(dir)
		if err == nil {
			t.Errorf("Load with %q: want error, got nil", tc.line)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("Load with %q: error = %v, want it to contain %q", tc.line, err, tc.want)
		}
	}
}

// TestRouteValidationMessages pins the exact error texts of the validation
// examples.
func TestRouteValidationMessages(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("route_machines=-x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("Load: want error, got nil")
	} else if err.Error() != `config: route_machines: invalid machine label "-x"` {
		t.Errorf("Load error = %q", err.Error())
	}
	dir = t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("route_probe_timeout_s=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("Load: want error, got nil")
	} else if err.Error() != "config: route_probe_timeout_s must be an integer from 1 to 120" {
		t.Errorf("Load error = %q", err.Error())
	}
	// Set reports the same texts without the "config: " prefix, which the
	// command layer prepends.
	if err := Set(dir, "route_machines", "-x"); err == nil {
		t.Fatal("Set route_machines: want error, got nil")
	} else if err.Error() != `route_machines: invalid machine label "-x"` {
		t.Errorf("Set error = %q", err.Error())
	}
	if err := Set(dir, "route_probe_timeout_s", "121"); err == nil {
		t.Fatal("Set route_probe_timeout_s: want error, got nil")
	} else if err.Error() != "route_probe_timeout_s must be an integer from 1 to 120" {
		t.Errorf("Set error = %q", err.Error())
	}
}

// TestLoadNormalizesRouteValues: hand-edited values are validated and the
// effective configuration holds the normalized form.
func TestLoadNormalizesRouteValues(t *testing.T) {
	dir := t.TempDir()
	content := "route_machines= mac-a , win-a \nroute_disabled=win-a\nherdr_bin=  herdr-x  \n"
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RouteMachines != "mac-a,win-a" {
		t.Errorf("RouteMachines = %q, want %q", cfg.RouteMachines, "mac-a,win-a")
	}
	if cfg.RouteDisabled != "win-a" {
		t.Errorf("RouteDisabled = %q, want %q", cfg.RouteDisabled, "win-a")
	}
	if cfg.HerdrBin != "herdr-x" {
		t.Errorf("HerdrBin = %q, want %q", cfg.HerdrBin, "herdr-x")
	}
}

// TestRouteLabelFormatsValid: the documented valid label examples load and
// store cleanly (the router reserved label, a dashed label list and a label
// with a dot and underscore).
func TestRouteLabelFormatsValid(t *testing.T) {
	dir := t.TempDir()
	for _, want := range []string{"local", "mac-a,win-a", "win.a_1"} {
		if err := Set(dir, "route_machines", want); err != nil {
			t.Errorf("Set route_machines %q: %v", want, err)
			continue
		}
		cfg, err := Load(dir)
		if err != nil {
			t.Errorf("Load after Set %q: %v", want, err)
			continue
		}
		if cfg.RouteMachines != want {
			t.Errorf("RouteMachines = %q, want %q", cfg.RouteMachines, want)
		}
	}
}

// TestRouteListLimits: 32 labels are the limit and valid, 33 are refused.
func TestRouteListLimits(t *testing.T) {
	labels := make([]string, 0, 33)
	for i := 0; i < 33; i++ {
		labels = append(labels, fmt.Sprintf("m%02d", i))
	}
	dir := t.TempDir()
	if err := Set(dir, "route_machines", strings.Join(labels[:32], ",")); err != nil {
		t.Fatalf("Set with 32 labels: %v", err)
	}
	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := len(cfg.RouteMachineList()); got != 32 {
		t.Errorf("RouteMachineList has %d items, want 32", got)
	}
	if err := Set(dir, "route_machines", strings.Join(labels, ",")); err == nil {
		t.Fatal("Set with 33 labels: want error, got nil")
	} else if !strings.Contains(err.Error(), "32") {
		t.Errorf("Set with 33 labels: error = %v, want the 32-item limit named", err)
	}
}
