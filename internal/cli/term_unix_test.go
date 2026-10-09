//go:build darwin || linux

package cli

import (
	"os"
	"strings"
	"testing"
)

// TestAuthLoginTerminalEchoRefusal: a character device that is not a
// terminal (/dev/zero) fails the echo toggle, so the command must refuse to
// read the key with echo on, with exit 2 and no credentials file.
func TestAuthLoginTerminalEchoRefusal(t *testing.T) {
	f, err := os.OpenFile("/dev/zero", os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open /dev/zero: %v", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Mode()&os.ModeCharDevice == 0 {
		t.Fatalf("/dev/zero stat = %v, %v; want a character device", st, err)
	}
	dir := t.TempDir()
	var outB, errB strings.Builder
	env := Env{
		Stdin:     f,
		Stdout:    &outB,
		Stderr:    &errB,
		Getenv:    func(string) string { return "" },
		ConfigDir: dir,
	}
	exit := Run([]string{"auth", "login", "--key", "-"}, env)
	if exit != 2 {
		t.Fatalf("auth login on a non-terminal char device exit = %d, want 2", exit)
	}
	if !strings.Contains(outB.String()+errB.String(), "echo") {
		t.Fatalf("refusal message = stdout %q stderr %q, want the echo reason", outB.String(), errB.String())
	}
	if _, err := os.Stat(dir + "/credentials"); !os.IsNotExist(err) {
		t.Fatalf("credentials created despite the echo refusal (err %v)", err)
	}
}
