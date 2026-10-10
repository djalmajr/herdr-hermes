package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigGetDefaults(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 0 {
		t.Fatalf("config get exit = %d, stdout %q", exit, stdout)
	}
	if stdout != "{\"key\":\"machine_label\",\"value\":\"\"}\n" {
		t.Errorf("config get = %q", stdout)
	}
	// Read-only: a fresh config dir is not created.
	assertDirEmpty(t, dir)
}

func TestConfigSetGet(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "machine_label", "lab-1")
	if exit != 0 {
		t.Fatalf("config set exit = %d, stdout %q", exit, stdout)
	}
	if stdout != "{\"key\":\"machine_label\",\"value\":\"lab-1\"}\n" {
		t.Errorf("config set = %q", stdout)
	}
	if _, err := os.Stat(filepath.Join(dir, "config")); err != nil {
		t.Fatalf("config file missing: %v", err)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 0 || stdout != "{\"key\":\"machine_label\",\"value\":\"lab-1\"}\n" {
		t.Errorf("config get after set = %q (exit %d)", stdout, exit)
	}
	// Other keys keep their defaults; the file holds the nine keys as
	// key=value lines in canonical order.
	data, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	want := "machine_label=lab-1\n" +
		"dispatcher_url=\n" +
		"herdr_soho_bin=herdr-soho\n" +
		"push_timeout_s=15\n" +
		"herdr_bin=herdr\n" +
		"route_machines=\n" +
		"route_disabled=\n" +
		"route_orchestrator_name=orchestrator\n" +
		"route_probe_timeout_s=20\n"
	if string(data) != want {
		t.Errorf("config file = %q, want %q", data, want)
	}
}

func TestConfigList(t *testing.T) {
	dir := t.TempDir()
	if _, _, exit := runCLI(t, dir, false, "config", "set", "dispatcher_url", "set-value"); exit != 0 {
		t.Fatal("setup: config set dispatcher_url")
	}
	stdout, _, exit := runCLI(t, dir, false, "config", "list")
	if exit != 0 {
		t.Fatalf("config list exit = %d", exit)
	}
	want := `{"config":{"machine_label":"","dispatcher_url":"set-value","herdr_soho_bin":"herdr-soho","push_timeout_s":15,"herdr_bin":"herdr","route_machines":"","route_disabled":"","route_orchestrator_name":"orchestrator","route_probe_timeout_s":20}}
`
	if stdout != want {
		t.Errorf("config list = %q, want %q", stdout, want)
	}
}

func TestConfigPushTimeout(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "push_timeout_s", "30")
	if exit != 0 {
		t.Fatalf("set 30: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "push_timeout_s")
	if exit != 0 || stdout != "{\"key\":\"push_timeout_s\",\"value\":\"30\"}\n" {
		t.Errorf("get push_timeout_s = %q (exit %d)", stdout, exit)
	}
	for _, bad := range []string{"0", "-5", "abc", "1.5", ""} {
		stdout, _, exit := runCLI(t, dir, false, "config", "set", "push_timeout_s", bad)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("set %q: exit %d, stdout %q; want exit 2", bad, exit, stdout)
		}
	}
	// The valid value survives the rejected writes.
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "push_timeout_s")
	if exit != 0 || stdout != "{\"key\":\"push_timeout_s\",\"value\":\"30\"}\n" {
		t.Errorf("get after bad sets = %q (exit %d)", stdout, exit)
	}
}

func TestConfigUnknownKey(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "not_a_key", "x")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) || !strings.Contains(stdout, "not_a_key") {
		t.Errorf("set unknown: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "not_a_key")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Errorf("get unknown: exit %d, stdout %q", exit, stdout)
	}
	// An unknown key already in the file is also refused at read time.
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte("machine_label=lab\nbogus=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Errorf("get with unknown key in file: exit %d, stdout %q", exit, stdout)
	}
}

func TestConfigCommentsAndBlanks(t *testing.T) {
	dir := t.TempDir()
	content := "# machine notes\n\nmachine_label=lab-9\n   # indented comment\n"
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, _, exit := runCLI(t, dir, false, "config", "get", "machine_label")
	if exit != 0 || stdout != "{\"key\":\"machine_label\",\"value\":\"lab-9\"}\n" {
		t.Errorf("config get with comments = %q (exit %d)", stdout, exit)
	}
	// set rewrites the file canonically (no comments kept, all keys present)
	if _, _, exit := runCLI(t, dir, false, "config", "set", "machine_label", "lab-10"); exit != 0 {
		t.Fatal("config set")
	}
	data, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if strings.Contains(s, "#") || strings.Contains(s, "lab-9") {
		t.Errorf("rewritten file keeps old content:\n%s", s)
	}
	for _, want := range []string{"machine_label=lab-10", "dispatcher_url=", "herdr_soho_bin=herdr-soho", "push_timeout_s=15", "herdr_bin=herdr", "route_machines=", "route_disabled=", "route_orchestrator_name=orchestrator", "route_probe_timeout_s=20"} {
		if !strings.Contains(s, want) {
			t.Errorf("rewritten file missing %q:\n%s", want, s)
		}
	}
}

func TestConfigRouteGetDefaults(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		key  string
		want string
	}{
		{"herdr_bin", `{"key":"herdr_bin","value":"herdr"}`},
		{"route_machines", `{"key":"route_machines","value":""}`},
		{"route_disabled", `{"key":"route_disabled","value":""}`},
		{"route_orchestrator_name", `{"key":"route_orchestrator_name","value":"orchestrator"}`},
		{"route_probe_timeout_s", `{"key":"route_probe_timeout_s","value":"20"}`},
	} {
		stdout, _, exit := runCLI(t, dir, false, "config", "get", tc.key)
		if exit != 0 {
			t.Fatalf("config get %s exit = %d, stdout %q", tc.key, exit, stdout)
		}
		if stdout != tc.want+"\n" {
			t.Errorf("config get %s = %q, want %q", tc.key, stdout, tc.want)
		}
	}
}

func TestConfigRouteSetGetNormalization(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "route_machines", " mac-a , win-a ")
	if exit != 0 {
		t.Fatalf("config set exit = %d, stdout %q", exit, stdout)
	}
	// set prints the stored (normalized) value.
	if stdout != "{\"key\":\"route_machines\",\"value\":\"mac-a,win-a\"}\n" {
		t.Errorf("config set route_machines = %q", stdout)
	}
	data, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "route_machines=mac-a,win-a\n") {
		t.Errorf("config file holds the unnormalized value:\n%s", data)
	}
	// get prints the effective (normalized) value.
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "route_machines")
	if exit != 0 || stdout != "{\"key\":\"route_machines\",\"value\":\"mac-a,win-a\"}\n" {
		t.Errorf("config get route_machines = %q (exit %d)", stdout, exit)
	}
	// route_disabled normalizes the same way.
	stdout, _, exit = runCLI(t, dir, false, "config", "set", "route_disabled", " win-a , mac-a ")
	if exit != 0 {
		t.Fatalf("set route_disabled: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "route_disabled")
	if exit != 0 || stdout != "{\"key\":\"route_disabled\",\"value\":\"win-a,mac-a\"}\n" {
		t.Errorf("config get route_disabled = %q (exit %d)", stdout, exit)
	}
	// herdr_bin is stored trimmed.
	stdout, _, exit = runCLI(t, dir, false, "config", "set", "herdr_bin", "  herdr-x  ")
	if exit != 0 {
		t.Fatalf("set herdr_bin: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "herdr_bin")
	if exit != 0 || stdout != "{\"key\":\"herdr_bin\",\"value\":\"herdr-x\"}\n" {
		t.Errorf("config get herdr_bin = %q (exit %d)", stdout, exit)
	}
	// The empty value is valid and stored empty.
	stdout, _, exit = runCLI(t, dir, false, "config", "set", "route_machines", "")
	if exit != 0 {
		t.Fatalf("set empty route_machines: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "route_machines")
	if exit != 0 || stdout != "{\"key\":\"route_machines\",\"value\":\"\"}\n" {
		t.Errorf("config get empty route_machines = %q (exit %d)", stdout, exit)
	}
	// route_orchestrator_name round trip.
	stdout, _, exit = runCLI(t, dir, false, "config", "set", "route_orchestrator_name", "orch-2")
	if exit != 0 {
		t.Fatalf("set route_orchestrator_name: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "route_orchestrator_name")
	if exit != 0 || stdout != "{\"key\":\"route_orchestrator_name\",\"value\":\"orch-2\"}\n" {
		t.Errorf("config get route_orchestrator_name = %q (exit %d)", stdout, exit)
	}
}

func TestConfigRouteSetInvalid(t *testing.T) {
	dir := t.TempDir()
	// A refused set in a fresh dir creates no file.
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "route_machines", "a,,b")
	if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
		t.Fatalf("set a,,b: exit %d, stdout %q; want exit 2", exit, stdout)
	}
	if _, err := os.Stat(filepath.Join(dir, "config")); !os.IsNotExist(err) {
		t.Fatalf("refused set created the config file: %v", err)
	}
	// A valid set first, so each refusal must leave the file unchanged.
	if _, _, exit := runCLI(t, dir, false, "config", "set", "route_machines", "mac-a"); exit != 0 {
		t.Fatal("setup: config set route_machines mac-a")
	}
	before, err := os.ReadFile(filepath.Join(dir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	var many []string
	for i := 0; i < 33; i++ {
		many = append(many, fmt.Sprintf("m%02d", i))
	}
	invalid := []struct {
		key string
		val string
	}{
		{"herdr_bin", ""},
		{"herdr_bin", "   "},
		{"route_machines", "-x"},
		{"route_machines", "a b"},
		{"route_machines", "a,,b"},
		{"route_machines", "a,"},
		{"route_machines", ",a"},
		{"route_machines", "mac-a,mac-a"},
		{"route_machines", "a/b"},
		{"route_machines", strings.Join(many, ",")}, // 33 items
		{"route_disabled", "-x"},
		{"route_disabled", "win-a,win-a"},
		{"route_orchestrator_name", ""},
		{"route_orchestrator_name", "Orchestrator"},
		{"route_orchestrator_name", "-x"},
		{"route_orchestrator_name", "9name"},
		{"route_orchestrator_name", strings.Repeat("a", 33)},
		{"route_probe_timeout_s", "0"},
		{"route_probe_timeout_s", "121"},
		{"route_probe_timeout_s", "x"},
	}
	for _, tc := range invalid {
		stdout, _, exit := runCLI(t, dir, false, "config", "set", tc.key, tc.val)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) || !strings.Contains(stdout, tc.key) {
			t.Errorf("set %s %q: exit %d, stdout %q; want exit 2 naming the key", tc.key, tc.val, exit, stdout)
		}
		after, err := os.ReadFile(filepath.Join(dir, "config"))
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != string(before) {
			t.Errorf("set %s %q changed the file:\n%s", tc.key, tc.val, after)
		}
	}
}

func TestConfigRouteProbeTimeoutBounds(t *testing.T) {
	dir := t.TempDir()
	for _, bad := range []string{"0", "121", "x"} {
		stdout, _, exit := runCLI(t, dir, false, "config", "set", "route_probe_timeout_s", bad)
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("set %q: exit %d, stdout %q; want exit 2", bad, exit, stdout)
		}
	}
	// 1 is the lower bound and valid.
	stdout, _, exit := runCLI(t, dir, false, "config", "set", "route_probe_timeout_s", "1")
	if exit != 0 {
		t.Fatalf("set 1: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "route_probe_timeout_s")
	if exit != 0 || stdout != "{\"key\":\"route_probe_timeout_s\",\"value\":\"1\"}\n" {
		t.Errorf("get after set 1 = %q (exit %d)", stdout, exit)
	}
	// 120 is the upper bound and valid.
	stdout, _, exit = runCLI(t, dir, false, "config", "set", "route_probe_timeout_s", "120")
	if exit != 0 {
		t.Fatalf("set 120: exit %d, stdout %q", exit, stdout)
	}
	stdout, _, exit = runCLI(t, dir, false, "config", "get", "route_probe_timeout_s")
	if exit != 0 || stdout != "{\"key\":\"route_probe_timeout_s\",\"value\":\"120\"}\n" {
		t.Errorf("get after set 120 = %q (exit %d)", stdout, exit)
	}
}

func TestConfigListInvalidRouterValue(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		line string
		key  string
	}{
		{"route_machines=mac-a,mac-a\n", "route_machines"},
		{"route_machines=-x\n", "route_machines"},
		{"route_machines=a,,b\n", "route_machines"},
		{"route_disabled=win-a,win-a\n", "route_disabled"},
		{"route_disabled=a,\n", "route_disabled"},
		{"route_orchestrator_name=Orchestrator\n", "route_orchestrator_name"},
		{"route_orchestrator_name=9name\n", "route_orchestrator_name"},
		{"route_probe_timeout_s=0\n", "route_probe_timeout_s"},
		{"route_probe_timeout_s=121\n", "route_probe_timeout_s"},
		{"route_probe_timeout_s=abc\n", "route_probe_timeout_s"},
		{"herdr_bin=\n", "herdr_bin"},
	} {
		if err := os.WriteFile(filepath.Join(dir, "config"), []byte(tc.line), 0o600); err != nil {
			t.Fatal(err)
		}
		stdout, _, exit := runCLI(t, dir, false, "config", "list")
		if exit != 2 || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("config list with %q: exit %d, stdout %q; want exit 2", tc.line, exit, stdout)
		}
		if !strings.Contains(stdout, tc.key) {
			t.Errorf("config list with %q: the error does not name the key: %q", tc.line, stdout)
		}
	}
}

func TestConfigUsage(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		{},
		{"frobnicate"},
		{"get"},
		{"get", "a", "b"},
		{"set", "machine_label"},
		{"set", "machine_label", "x", "y"},
		{"list", "extra"},
	} {
		stdout, stderr, exit := runCLI(t, dir, false, append([]string{"config"}, args...)...)
		if exit != 2 || !strings.Contains(stderr, "commands:") || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("config %v: exit %d, stderr %q, stdout %q", args, exit, stderr, stdout)
		}
	}
}
