package fakesoho

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// lockReadyBound bounds the precondition (the lock-holder child started
// and printed its line); a child that never prints within it under load
// fails the test, because the lock property under test requires the
// child to have taken the lock.
const lockReadyBound = 30 * time.Second

// startLockHolder re-executes this test binary with the lock-holder
// marker and waits, bounded by lockReadyBound, for its "locked" line:
// the child holds the lock on lockPath until it is killed.
func startLockHolder(t *testing.T, lockPath string) *exec.Cmd {
	t.Helper()
	bin, err := os.Executable()
	if err != nil {
		t.Fatalf("lock-holder: executable: %v", err)
	}
	cmd := exec.Command(bin, lockHolderMarker, lockPath)
	cmd.WaitDelay = time.Second
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("lock-holder: stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("lock-holder: start: %v", err)
	}
	type lineResult struct {
		line string
		err  error
	}
	line := make(chan lineResult, 1)
	go func() {
		l, rerr := bufio.NewReader(pipe).ReadString('\n')
		line <- lineResult{l, rerr}
	}()
	var got lineResult
	select {
	case got = <-line:
	case <-time.After(lockReadyBound):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("lock-holder: no line within %s", lockReadyBound)
	}
	if got.err != nil || got.line != "locked\n" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("lock-holder: line %q (err %v), want \"locked\\n\"", got.line, got.err)
	}
	return cmd
}

// TestLockAfterDeadProcess: a holder killed after it took the lock
// (the shape of a child killed at a runner deadline while it holds the
// call-log lock) leaves no stale lock behind: the system releases the
// lock with the process, so the next lock call acquires at once.
// Against the old create-and-delete lock the file would be left behind
// and the acquisition would wait the full five seconds and time out.
func TestLockAfterDeadProcess(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "lock")
	cmd := startLockHolder(t, lockPath)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill the holder: %v", err)
	}
	// The child was killed on purpose; its exit state is not the
	// property under test.
	_ = cmd.Wait()
	start := time.Now()
	unlock, err := lock(lockPath)
	if err != nil {
		t.Fatalf("lock after the holder died: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("lock after the holder died took %s, want under 2s", elapsed)
	}
	unlock()
}

// TestLockWhileHolderAlive: while a live process holds the lock, a
// contended lock in this process must not get it: it retries until the
// wait bound (shortened for the test and restored by the defers) and
// returns the timeout error; after the holder dies, the lock comes at
// once.
func TestLockWhileHolderAlive(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "lock")
	cmd := startLockHolder(t, lockPath)
	oldLockWait := lockWait
	lockWait = 500 * time.Millisecond
	defer func() {
		lockWait = oldLockWait
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	start := time.Now()
	_, err := lock(lockPath)
	if err == nil {
		t.Fatalf("lock while the holder is alive: got the lock in %s, want the timeout", time.Since(start))
	}
	if !strings.Contains(err.Error(), "timed out waiting for") {
		t.Fatalf("lock while the holder is alive: err %q, want the timeout error", err)
	}
	if elapsed := time.Since(start); elapsed < 400*time.Millisecond || elapsed >= 2*time.Second {
		t.Fatalf("lock while the holder is alive: waited %s, want about the shortened bound", elapsed)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill the holder: %v", err)
	}
	_ = cmd.Wait()
	start = time.Now()
	unlock, err := lock(lockPath)
	if err != nil {
		t.Fatalf("lock after the holder died: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("lock after the holder died took %s, want under 2s", elapsed)
	}
	unlock()
}
