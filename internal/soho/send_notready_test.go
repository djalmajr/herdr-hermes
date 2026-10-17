package soho

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/testutil/fakesoho"
)

// The not-ready tests run in the internal package on purpose: they
// replace notReadySleep (used only by tests) so the in-Send retries
// never sleep for real, while notReadyPauses stays at its production
// values 1s, 2s and 4s.

// notReadyTestTimeout is the fixed timeout the not-ready tests use, so
// the exact argv the fake rules must match stays the same.
const notReadyTestTimeout = 20 * time.Second

// notReadyStderr is the stderr soho prints when the recipient exists
// but is not active yet, its start still running (exit 4).
const notReadyStderr = "soho: send: x unavailable: agent_not_ready: agent w1:p2 is not an active named agent\n"

func notReadyArgv(ref, msg string) []string {
	return []string{"send", ref, msg, "--now", "--timeout", "20000"}
}

var notReadyChildEnv = []string{"PATH=/bin", "SOHO_TEST=1"}

func installNotReadyFake(t *testing.T, rules ...fakesoho.Rule) (exe, dir string) {
	t.Helper()
	dir = t.TempDir()
	return fakesoho.Install(t, dir, rules...), dir
}

func readNotReadyCalls(t *testing.T, dir string) []fakesoho.Call {
	t.Helper()
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	return calls
}

// withNotReadySleep replaces notReadySleep with a recorder that never
// sleeps and returns the pauses it received, in order; on, when
// non-nil, runs before each pause returns.
func withNotReadySleep(t *testing.T, on func(d time.Duration)) *[]time.Duration {
	t.Helper()
	got := new([]time.Duration)
	old := notReadySleep
	notReadySleep = func(ctx context.Context, d time.Duration) error {
		*got = append(*got, d)
		if on != nil {
			on(d)
		}
		return nil
	}
	t.Cleanup(func() { notReadySleep = old })
	return got
}

func wantPauses(t *testing.T, got *[]time.Duration, want ...time.Duration) {
	t.Helper()
	if len(*got) != len(want) {
		t.Fatalf("pauses = %v, want %v", *got, want)
	}
	for i := range want {
		if (*got)[i] != want[i] {
			t.Fatalf("pauses = %v, want %v", *got, want)
		}
	}
}

// TestNotifySendNotReadyRetrySucceeds: the first run is refused with
// agent_not_ready and the second is accepted: the result is the
// second run's own mapping and the fake sees exactly two calls.
func TestNotifySendNotReadyRetrySucceeds(t *testing.T) {
	const ref = "local/w1:p2"
	argv := notReadyArgv(ref, "hi")
	exe, dir := installNotReadyFake(t,
		fakesoho.Rule{Argv: argv, Call: 1, Stderr: notReadyStderr, Code: 4},
		fakesoho.Rule{Argv: argv, Call: 2, Stdout: "sent\n", Code: 0},
	)
	got := withNotReadySleep(t, nil)
	s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
	res := s.Send(context.Background(), ref, "hi")
	want := notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	wantPauses(t, got, time.Second)
	calls := readNotReadyCalls(t, dir)
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	for i, c := range calls {
		if fmt.Sprint(c.Argv) != fmt.Sprint(argv) {
			t.Errorf("call %d argv = %v, want %v", i, c.Argv, argv)
		}
	}
}

// TestNotifySendNotReadyExhausted: four not-ready refusals end the call
// with the not-ready result after the injected pauses received 1s, 2s
// and 4s, in order.
func TestNotifySendNotReadyExhausted(t *testing.T) {
	const ref = "local/w1:p3"
	exe, dir := installNotReadyFake(t,
		fakesoho.Rule{Argv: notReadyArgv(ref, "hi"), Stderr: notReadyStderr, Code: 4},
	)
	got := withNotReadySleep(t, nil)
	s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
	res := s.Send(context.Background(), ref, "hi")
	want := notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "not_ready"}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	wantPauses(t, got, time.Second, 2*time.Second, 4*time.Second)
	if calls := readNotReadyCalls(t, dir); len(calls) != 4 {
		t.Fatalf("calls = %d, want 4", len(calls))
	}
}

// TestNotifySendNotReadyOtherExit4: an exit 4 that is not a not-ready
// refusal (no such agent) is not retried and keeps the ordinary exit
// mapping.
func TestNotifySendNotReadyOtherExit4(t *testing.T) {
	const ref = "local/w1:p4"
	exe, dir := installNotReadyFake(t,
		fakesoho.Rule{Argv: notReadyArgv(ref, "hi"), Stderr: "soho: send: x: no agent in x\n", Code: 4},
	)
	got := withNotReadySleep(t, nil)
	s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
	res := s.Send(context.Background(), ref, "hi")
	want := notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "exit"}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	if len(*got) != 0 {
		t.Fatalf("a pause was taken for a non-not-ready exit 4: %v", *got)
	}
	if calls := readNotReadyCalls(t, dir); len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
}

// TestNotifySendNotReadyExit4NoStderr: an exit 4 with empty stderr is
// not a not-ready refusal: no retry and the ordinary exit mapping.
func TestNotifySendNotReadyExit4NoStderr(t *testing.T) {
	const ref = "local/w1:p5"
	exe, dir := installNotReadyFake(t,
		fakesoho.Rule{Argv: notReadyArgv(ref, "hi"), Code: 4},
	)
	got := withNotReadySleep(t, nil)
	s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
	res := s.Send(context.Background(), ref, "hi")
	want := notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "exit"}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	if len(*got) != 0 {
		t.Fatalf("a pause was taken for an exit 4 without the marker: %v", *got)
	}
	if calls := readNotReadyCalls(t, dir); len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
}

// TestNotifySendNotReadyDeadlineTooShort: a caller deadline that leaves
// less than the first pause (1s) plus 2s after the first not-ready run
// skips the retry: one call and the not-ready result.
func TestNotifySendNotReadyDeadlineTooShort(t *testing.T) {
	const ref = "local/w1:p6"
	exe, dir := installNotReadyFake(t,
		fakesoho.Rule{Argv: notReadyArgv(ref, "hi"), Stderr: notReadyStderr, Code: 4},
	)
	got := withNotReadySleep(t, nil)
	// The deadline (2.9s from now) leaves less than 3s — the first
	// pause plus 2s — after the first run: the retry must not run. If
	// the first run itself outruns it, the result is a deadline, which
	// the want check below rejects.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(2900*time.Millisecond))
	defer cancel()
	s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
	res := s.Send(ctx, ref, "hi")
	want := notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "not_ready"}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	if len(*got) != 0 {
		t.Fatalf("a pause was taken although the deadline was too short: %v", *got)
	}
	if calls := readNotReadyCalls(t, dir); len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
}

// TestNotifySendNotReadyCancelledDuringPause: a context that ends
// during the pause (by the injected pause) returns the not-ready
// result without another run.
func TestNotifySendNotReadyCancelledDuringPause(t *testing.T) {
	const ref = "local/w1:p7"
	exe, dir := installNotReadyFake(t,
		fakesoho.Rule{Argv: notReadyArgv(ref, "hi"), Stderr: notReadyStderr, Code: 4},
	)
	ctx, cancel := context.WithCancel(context.Background())
	got := new([]time.Duration)
	old := notReadySleep
	notReadySleep = func(ctx context.Context, d time.Duration) error {
		*got = append(*got, d)
		cancel()
		return ctx.Err()
	}
	t.Cleanup(func() { notReadySleep = old })
	s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
	res := s.Send(ctx, ref, "hi")
	want := notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "not_ready"}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	wantPauses(t, got, time.Second)
	if calls := readNotReadyCalls(t, dir); len(calls) != 1 {
		t.Fatalf("calls = %d, want 1 (no new run after the cancelled pause)", len(calls))
	}
}

// TestNotifySendNotReadyNoRetryUncertainAndDeadline: an exit 15 and a
// run killed in flight at the bound are never retried: one call each.
func TestNotifySendNotReadyNoRetryUncertainAndDeadline(t *testing.T) {
	t.Run("exit 15", func(t *testing.T) {
		const ref = "local/w1:p8"
		exe, dir := installNotReadyFake(t,
			fakesoho.Rule{Argv: notReadyArgv(ref, "hi"), Code: 15},
		)
		got := withNotReadySleep(t, nil)
		s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
		res := s.Send(context.Background(), ref, "hi")
		want := notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: 15, Category: "uncertain"}
		if res != want {
			t.Fatalf("res = %+v, want %+v", res, want)
		}
		if len(*got) != 0 {
			t.Fatalf("a pause was taken after exit 15: %v", *got)
		}
		if calls := readNotReadyCalls(t, dir); len(calls) != 1 {
			t.Fatalf("calls = %d, want 1", len(calls))
		}
	})
	t.Run("deadline", func(t *testing.T) {
		// The child holds past the bound on purpose (the 750ms timeout
		// plus the 3s send grace); ExitOnTerm 1 makes it exit at once
		// on the cancel signal — the Windows TerminateProcess shape —
		// so the run ends at the bound and the adapter reports a
		// deadline.
		const ref = "local/wd:p9"
		argv := []string{"send", ref, "hi", "--now", "--timeout", "750"}
		exe, dir := installNotReadyFake(t, fakesoho.Rule{
			Argv:       argv,
			Delay:      30000,
			ExitOnTerm: 1,
			Code:       0,
		})
		got := withNotReadySleep(t, nil)
		s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: 750 * time.Millisecond}
		res := s.Send(context.Background(), ref, "hi")
		want := notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: -1, Category: "deadline"}
		if res != want {
			t.Fatalf("res = %+v, want %+v", res, want)
		}
		if len(*got) != 0 {
			t.Fatalf("a pause was taken after a deadline: %v", *got)
		}
		if calls := readNotReadyCalls(t, dir); len(calls) != 1 {
			t.Fatalf("calls = %d, want 1", len(calls))
		}
	})
}

// TestNotifySendNotReadyCanaryStderr: a canary in the child's stderr
// never appears in any field of the returned SendResult.
func TestNotifySendNotReadyCanaryStderr(t *testing.T) {
	const canary = "CANARY-DJA376"
	const ref = "local/secret:p1"
	exe, _ := installNotReadyFake(t,
		fakesoho.Rule{Argv: notReadyArgv(ref, "hi"), Stderr: "soho: send: x unavailable: agent_not_ready: " + canary + "\n", Code: 4},
	)
	got := withNotReadySleep(t, nil)
	s := NotifySender{Bin: exe, Environ: notReadyChildEnv, Timeout: notReadyTestTimeout}
	res := s.Send(context.Background(), ref, "hi")
	want := notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "not_ready"}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	if rendered := fmt.Sprintf("%+v", res); strings.Contains(rendered, canary) {
		t.Fatalf("the rendered SendResult %q contains the canary", rendered)
	}
	wantPauses(t, got, time.Second, 2*time.Second, 4*time.Second)
}

// TestNotifySendNotReadyProductionPauses: the production retry pauses
// are exactly 1s, 2s and 4s, in order.
func TestNotifySendNotReadyProductionPauses(t *testing.T) {
	pauses := notReadyPauses
	wantPauses(t, &pauses, time.Second, 2*time.Second, 4*time.Second)
}
