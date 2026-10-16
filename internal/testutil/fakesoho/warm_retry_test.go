package fakesoho

import (
	"context"
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// busySimulated is a stand-in for a busy-executable start failure: the
// retry loop under test takes the recognition through busyExec, which
// the platform file installs and the test may replace.
var busySimulated = errors.New("exec: text file busy (simulated)")

// TestWarmRetry: a warm run that fails with a failure the busy check
// recognizes is retried after a pause, until it succeeds; the pause is
// injected so no real time is used.
func TestWarmRetry(t *testing.T) {
	old := busyExec
	t.Cleanup(func() { busyExec = old })
	busyExec = func(error) bool { return true }
	calls := 0
	paused := 0
	err := retryWarm(func() error {
		calls++
		if calls <= 2 {
			return busySimulated
		}
		return nil
	}, func(context.Context, time.Duration) error { paused++; return nil })
	if err != nil {
		t.Fatalf("retryWarm = %v, want success after the retries", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want exactly 3 (2 busy starts, 1 success)", calls)
	}
	if paused != 2 {
		t.Errorf("pauses = %d, want exactly 2 (one between each attempt)", paused)
	}
}

// TestWarmRetryExhausted: a warm run that is busy on every attempt is
// tried at most 5 times and then reported, with a pause between each
// attempt.
func TestWarmRetryExhausted(t *testing.T) {
	old := busyExec
	t.Cleanup(func() { busyExec = old })
	busyExec = func(error) bool { return true }
	calls := 0
	paused := 0
	err := retryWarm(func() error {
		calls++
		return busySimulated
	}, func(context.Context, time.Duration) error { paused++; return nil })
	if err == nil {
		t.Fatal("retryWarm = nil, want the busy failure after the attempts")
	}
	if calls != 5 {
		t.Errorf("calls = %d, want exactly 5 attempts", calls)
	}
	if paused != 4 {
		t.Errorf("pauses = %d, want exactly 4 (one between each attempt)", paused)
	}
}

// TestWarmRetryOrdinaryFailure: a warm-run failure the busy check does
// not recognize is returned at once, without a pause.
func TestWarmRetryOrdinaryFailure(t *testing.T) {
	old := busyExec
	t.Cleanup(func() { busyExec = old })
	busyExec = func(error) bool { return false }
	calls := 0
	paused := 0
	err := retryWarm(func() error {
		calls++
		return errors.New("warm run: exit status 127")
	}, func(context.Context, time.Duration) error { paused++; return nil })
	if err == nil {
		t.Fatal("retryWarm = nil, want the ordinary failure")
	}
	if calls != 1 || paused != 0 {
		t.Errorf("calls = %d, pauses = %d, want no retry of an ordinary failure", calls, paused)
	}
}

// TestWarmRetryPauseFailure: a pause that fails (the context is done
// between attempts) stops the retry and reports the pause failure.
func TestWarmRetryPauseFailure(t *testing.T) {
	old := busyExec
	t.Cleanup(func() { busyExec = old })
	busyExec = func(error) bool { return true }
	calls := 0
	err := retryWarm(func() error {
		calls++
		return busySimulated
	}, func(context.Context, time.Duration) error { return context.DeadlineExceeded })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retryWarm = %v, want the pause failure", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (the retry stops when the pause fails)", calls)
	}
}

// TestBusyExecMatchesPlatformError: on the operating systems where the
// busy-executable condition occurs, the default check recognizes
// exactly the platform error, including a start error that wraps it,
// and nothing else.
func TestBusyExecMatchesPlatformError(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("the busy-executable error is a unix condition")
	}
	if !busyExec(syscall.ETXTBSY) {
		t.Error("busyExec must match the platform ETXTBSY error")
	}
	if busyExec(errors.New("other")) {
		t.Error("busyExec must not match ordinary errors")
	}
	wrapped := &os.PathError{Op: "fork/exec", Path: "/x", Err: syscall.ETXTBSY}
	if !busyExec(wrapped) {
		t.Error("busyExec must match a start error wrapping ETXTBSY")
	}
}
