package fakesoho

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestWarmRun: the warm marker alone makes the installed copy exit 0
// without matching a rule, reading stdin or writing a call to the log,
// and Install has already spent that one run on the fresh executable,
// so the call log is empty right after Install returns.
func TestWarmRun(t *testing.T) {
	dir := t.TempDir()
	exe := Install(t, dir)
	if err := callLogEmpty(t, LogPath(dir), "right after Install"); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cmd := exec.Command(exe, warmMarker)
	cmd.WaitDelay = time.Second
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("warm run: %v, output %q", err, out.String())
	}
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("warm run: exit %d, want 0, output %q", code, out.String())
	}
	if err := callLogEmpty(t, LogPath(dir), "after the warm run"); err != nil {
		t.Fatal(err)
	}
}

// callLogEmpty checks that the call log at file holds no call: either
// the file does not exist (no call ever happened) or it holds zero
// calls.
func callLogEmpty(t *testing.T, file, when string) error {
	t.Helper()
	calls, err := ReadCallsFromPath(file)
	if err == nil {
		if len(calls) != 0 {
			return fmt.Errorf("call log %s: %d calls, want 0", when, len(calls))
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("call log %s: read: %v", when, err)
	}
	return nil
}
