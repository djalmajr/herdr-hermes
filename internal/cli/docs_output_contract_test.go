package cli

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// docReadmePath is the repository README relative to this package's working
// directory.
const docReadmePath = "../../README.md"

// docLoadReadme reads the repository README and normalizes CRLF to LF.
func docLoadReadme(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(docReadmePath)
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	return strings.ReplaceAll(string(raw), "\r\n", "\n")
}

// docFirstLine returns the first line of s without its newline.
func docFirstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// TestDocsHelpIsPlainText keeps the README output contract for the
// plain-text exceptions: `help` and running herdr-hermes with no arguments
// print the usage on stdout and exit 0 instead of a JSON line.
func TestDocsHelpIsPlainText(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "help", args: []string{"help"}},
		{name: "noargs", args: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, exit := runCLI(t, t.TempDir(), false, tc.args...)
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if stdout != usage {
				t.Fatalf("stdout does not equal the usage text byte for byte (len %d, want %d)", len(stdout), len(usage))
			}
			if strings.Count(stdout, "\n") < 2 {
				t.Fatalf("stdout has %d newlines, want more than one line", strings.Count(stdout, "\n"))
			}
			if json.Valid([]byte(docFirstLine(stdout))) {
				t.Fatalf("first line %q is valid JSON, want plain text", docFirstLine(stdout))
			}
			if stderr != "" {
				t.Fatalf("stderr = %q, want empty", stderr)
			}
		})
	}
}

// TestDocsHelpBadUsageKeepsJSON keeps the README output contract for bad
// usage: the usage text goes to stderr and stdout still carries exactly one
// JSON error line with status 2.
func TestDocsHelpBadUsageKeepsJSON(t *testing.T) {
	stdout, stderr, exit := runCLI(t, t.TempDir(), false, "help", "extra")
	if exit != 2 {
		t.Fatalf("exit = %d, want 2", exit)
	}
	if !strings.HasSuffix(stdout, "\n") {
		t.Fatalf("stdout does not end with a newline: %q", stdout)
	}
	body := strings.TrimSuffix(stdout, "\n")
	if strings.Count(body, "\n") != 0 {
		t.Fatalf("stdout has more than one line: %q", stdout)
	}
	var line map[string]string
	if err := json.Unmarshal([]byte(body), &line); err != nil {
		t.Fatalf("stdout is not valid JSON: %v", err)
	}
	if line["status"] != "2" {
		t.Fatalf("status = %q, want %q", line["status"], "2")
	}
	if !strings.Contains(stderr, usage) {
		t.Fatalf("stderr does not contain the usage text")
	}
}

// TestDocsSingleJSONLineCommands keeps the README output contract for the
// default case: version and capabilities --json print exactly one valid
// JSON line on stdout.
func TestDocsSingleJSONLineCommands(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "version", args: []string{"version"}},
		{name: "capabilities", args: []string{"capabilities", "--json"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout, _, exit := runCLI(t, t.TempDir(), false, tc.args...)
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if !strings.HasSuffix(stdout, "\n") {
				t.Fatalf("stdout does not end with exactly one newline: %q", stdout)
			}
			body := strings.TrimSuffix(stdout, "\n")
			if strings.Count(body, "\n") != 0 {
				t.Fatalf("stdout has more than one line: %q", stdout)
			}
			if !json.Valid([]byte(body)) {
				t.Fatalf("stdout is not valid JSON: %q", body)
			}
		})
	}
}

// TestDocsReadmeOutputContract keeps the README aligned with the real output
// contract: the single-JSON-line promise is qualified with the plain-text,
// outbox and forwarding exceptions, the routing onboarding pointer is
// present, and no active orchestrator wording remains.
func TestDocsReadmeOutputContract(t *testing.T) {
	readme := docLoadReadme(t)
	if strings.Contains(readme, "Every command prints exactly one JSON line on stdout (diagnostics go to stderr)") {
		t.Errorf("README still carries the unqualified single-JSON-line promise")
	}
	for _, want := range []string{
		"with these exceptions: `help` and `herdr-hermes` run with no arguments print the plain-text usage on stdout and exit 0",
		"`outbox` prints one JSON line per record and then the trailer line",
		"for `job events` is JSON lines and a trailer line",
		"docs/routing.md#preflight-read-only",
		"orchestrator-<n>",
	} {
		if !strings.Contains(readme, want) {
			t.Errorf("README does not contain %q", want)
		}
	}
	if strings.Contains(strings.ToLower(readme), "active orchestrator") {
		t.Errorf("README mentions an active orchestrator (case-insensitive)")
	}
}
