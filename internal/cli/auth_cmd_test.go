package cli

import (
	"os"
	"runtime"
	"strings"
	"testing"
)

const sentinelKey = "sentinel-key-7f3a"

func TestAuthLoginPipe(t *testing.T) {
	dir := t.TempDir()
	stdout, stderr, exit := runCLIStdin(t, dir, sentinelKey+"\n", nil, "auth", "login", "--key", "-")
	if exit != 0 {
		t.Fatalf("auth login exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"configured":true,"store":"file"}
` {
		t.Fatalf("auth login stdout = %q", stdout)
	}
	data, err := os.ReadFile(dir + "/credentials")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != sentinelKey+"\n" {
		t.Fatalf("credentials file = %q, want the key plus one newline", data)
	}
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(dir + "/credentials")
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Fatalf("credentials mode = %v, want 0600", fi.Mode().Perm())
		}
	}
	// status reports exactly the two fields and nothing else.
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "auth", "status")
	if exit != 0 {
		t.Fatalf("auth status exit = %d, stderr %q", exit, stderr)
	}
	if stdout != `{"configured":true,"store":"file"}
` {
		t.Fatalf("auth status stdout = %q, want exactly the two fields", stdout)
	}
	// CRLF on the typed key line is trimmed.
	dir2 := t.TempDir()
	if _, _, exit := runCLIStdin(t, dir2, "krlf\r\n", nil, "auth", "login", "--key", "-"); exit != 0 {
		t.Fatalf("auth login CRLF exit = %d", exit)
	}
	if data, _ := os.ReadFile(dir2 + "/credentials"); string(data) != "krlf\n" {
		t.Fatalf("credentials after CRLF input = %q, want \"krlf\" plus newline", data)
	}
	// logout removes the key.
	stdout, stderr, exit = runCLIStdin(t, dir, "", nil, "auth", "logout")
	if exit != 0 || stdout != `{"configured":false,"store":"file"}
` {
		t.Fatalf("auth logout: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
	if _, err := os.Stat(dir + "/credentials"); !os.IsNotExist(err) {
		t.Fatalf("credentials still present after logout (err %v)", err)
	}
	stdout, _, exit = runCLIStdin(t, dir, "", nil, "auth", "status")
	if exit != 0 || stdout != `{"configured":false,"store":"file"}
` {
		t.Fatalf("auth status after logout: exit %d stdout %q", exit, stdout)
	}
	// logout with no key is still a success (nothing to remove).
	stdout, stderr, exit = runCLIStdin(t, dir2, "", nil, "auth", "logout")
	if exit != 0 || stdout != `{"configured":false,"store":"file"}
` {
		t.Fatalf("auth logout without key: exit %d stdout %q stderr %q", exit, stdout, stderr)
	}
}

// TestAuthLoginRefusals: only `auth login --key -` is accepted. A flag
// value, a positional argument or a missing --key exits 2, and the message
// never echoes the value. The key is never read from the environment.
func TestAuthLoginRefusals(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		stdin string
	}{
		{"no subcommand args", []string{"auth", "login"}, sentinelKey + "\n"},
		{"flag value", []string{"auth", "login", "--key", "flagvalue-9"}, ""},
		{"flag missing value", []string{"auth", "login", "--key"}, ""},
		{"extra positional", []string{"auth", "login", "--key", "-", "extra"}, ""},
		{"equals form", []string{"auth", "login", "--key=-"}, ""},
		{"unknown flag", []string{"auth", "login", "--password", "x"}, ""},
		{"empty stdin", []string{"auth", "login", "--key", "-"}, ""},
		{"newline only", []string{"auth", "login", "--key", "-"}, "\n"},
		{"space in key", []string{"auth", "login", "--key", "-"}, "a b\n"},
		{"tab in key", []string{"auth", "login", "--key", "-"}, "a\tb\n"},
		{"over 4 KiB", []string{"auth", "login", "--key", "-"}, strings.Repeat("k", 4*1024+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stdout, stderr, exit := runCLIStdin(t, dir, tc.stdin, nil, tc.args...)
			if exit != 2 {
				t.Fatalf("exit = %d, want 2 (stdout %q)", exit, stdout)
			}
			// The refusal message never echoes a value.
			if strings.Contains(stdout+stderr, sentinelKey) || strings.Contains(stdout+stderr, "flagvalue-9") || strings.Contains(stdout+stderr, "a b") || strings.Contains(stdout+stderr, "a\tb") {
				t.Fatalf("refusal echoed a value: stdout %q stderr %q", stdout, stderr)
			}
			if _, err := os.Stat(dir + "/credentials"); !os.IsNotExist(err) {
				t.Fatalf("credentials created on a refusal (err %v)", err)
			}
		})
	}
	// Exactly 4 KiB of key is accepted (the cap is the maximum).
	dir := t.TempDir()
	if _, _, exit := runCLIStdin(t, dir, strings.Repeat("k", 4*1024), nil, "auth", "login", "--key", "-"); exit != 0 {
		t.Fatalf("a 4 KiB key must be accepted")
	}
}

// TestAuthLoginReadsNoEnvironment: the login path queries only the NOWRITE
// variable; no environment variable is ever read for the key (the map
// answers the sentinel for every other name, so a read would leak it).
func TestAuthLoginReadsNoEnvironment(t *testing.T) {
	dir := t.TempDir()
	var queried []string
	var out, errB strings.Builder
	env := Env{
		Stdin:  strings.NewReader("pipekey-1\n"),
		Stdout: &out,
		Stderr: &errB,
		Getenv: func(k string) string {
			queried = append(queried, k)
			if k == nowriteVar {
				return ""
			}
			return sentinelKey
		},
		ConfigDir: dir,
	}
	exit := Run([]string{"auth", "login", "--key", "-"}, env)
	if exit != 0 {
		t.Fatalf("auth login exit = %d, stderr %q", exit, errB.String())
	}
	for _, k := range queried {
		if k != nowriteVar {
			t.Fatalf("auth login queried the environment for %q; only %s may be read", k, nowriteVar)
		}
	}
	// The key came from stdin, not from any environment answer.
	if data, err := os.ReadFile(dir + "/credentials"); err != nil || string(data) != "pipekey-1\n" {
		t.Fatalf("credentials = %q, err %v; want the stdin key", data, err)
	}
}

func TestAuthNowrite(t *testing.T) {
	dir := t.TempDir()
	// login and logout are writing commands and refuse under NOWRITE.
	var outb, errb strings.Builder
	env := Env{
		Stdin:     strings.NewReader("x\n"),
		Stdout:    &outb,
		Stderr:    &errb,
		Getenv:    func(k string) string { return "1" },
		ConfigDir: dir,
	}
	if exit := Run([]string{"auth", "login", "--key", "-"}, env); exit != 2 ||
		outb.String() != "{\"status\":\"nowrite\",\"motivo\":\"HERDR_HERMES_NOWRITE=1\"}\n" {
		t.Fatalf("auth login under NOWRITE: exit %d stdout %q", exit, outb.String())
	}
	if _, err := os.Stat(dir + "/credentials"); !os.IsNotExist(err) {
		t.Fatalf("auth login under NOWRITE wrote the credentials")
	}
	outb.Reset()
	if exit := Run([]string{"auth", "logout"}, env); exit != 2 ||
		outb.String() != "{\"status\":\"nowrite\",\"motivo\":\"HERDR_HERMES_NOWRITE=1\"}\n" {
		t.Fatalf("auth logout under NOWRITE: exit %d stdout %q", exit, outb.String())
	}
	// status works under NOWRITE and creates nothing.
	outb.Reset()
	if exit := Run([]string{"auth", "status"}, env); exit != 0 ||
		outb.String() != "{\"configured\":false,\"store\":\"file\"}\n" {
		t.Fatalf("auth status under NOWRITE: exit %d stdout %q", exit, outb.String())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("auth status under NOWRITE created %v", entries)
	}
}

func TestAuthBadUsage(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		{"auth"},
		{"auth", "bogus"},
		{"auth", "status", "extra"},
		{"auth", "login", "--key", "-", "extra"},
	} {
		stdout, stderr, exit := runCLIStdin(t, dir, "", nil, args...)
		if exit != 2 || !strings.Contains(stderr, "commands:") || !strings.Contains(stdout, `"status":"2"`) {
			t.Errorf("%v: exit %d stderr %q stdout %q", args, exit, stderr, stdout)
		}
	}
}
