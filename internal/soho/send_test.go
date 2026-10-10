package soho_test

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/notify"
	"github.com/djalmajr/herdr-hermes/internal/soho"
	"github.com/djalmajr/herdr-hermes/internal/testutil/fakesoho"
)

// sendTestTimeout is the fixed timeout the send tests use, so the argv
// (and thus the fake rules) stays exact. It is generous so a slow start
// of the fake (a loaded machine, -race) never turns an ordinary exit into
// a deadline; only the deadline test uses sendShortTimeout.
const sendTestTimeout = 20 * time.Second

// sendShortTimeout is the timeout of the deadline test, whose fake holds
// past the bound on purpose.
const sendShortTimeout = 750 * time.Millisecond

// sendArgv builds the exact argv the adapter must run for the given ref
// and message.
func sendArgv(ref, message string, timeout time.Duration) []string {
	return []string{"send", ref, message, "--now", "--timeout", strconv.FormatInt(timeout.Milliseconds(), 10)}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// childEnvExact compares the child environment the fake logged with want
// exactly. On Windows the Go runtime adds SYSTEMROOT to a child
// environment when it is missing (standard library behavior, not a
// leak), so that entry is filtered out there before the comparison; on
// every other OS the comparison stays exact.
func childEnvExact(t *testing.T, env, want []string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		kept := env[:0]
		for _, kv := range env {
			if !strings.HasPrefix(kv, "SYSTEMROOT=") {
				kept = append(kept, kv)
			}
		}
		env = kept
	}
	if !sameStrings(env, want) {
		t.Errorf("child env = %v, want %v", env, want)
	}
}

// TestNotifySendExitMapping covers every row of the outcome mapping and
// checks the exact argv and the exact child environment the fake
// logged.
func TestNotifySendExitMapping(t *testing.T) {
	rows := []struct {
		ref    string
		msg    string
		stdout string
		code   int
		want   notify.SendResult
	}{
		{"local/w1:p1", "hi one", "sent to x\n", 0, notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}},
		{"local/w1:p2", "hi two", "queued for x\n", 0, notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "queued", Exit: 0}},
		{"local/w1:p3", "hi three", "some other wording\n", 0, notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "accepted", Exit: 0}},
		{"local/w1:p4", "hi four", "", 0, notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "accepted", Exit: 0}},
		{"local/w1:p5", "hi five", "", 15, notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: 15, Category: "uncertain"}},
		{"local/w1:p6", "hi six", "", 2, notify.SendResult{Outcome: notify.OutcomeRejected, Exit: 2, Category: "usage"}},
		{"local/w1:p7", "hi seven", "", 3, notify.SendResult{Outcome: notify.OutcomeRejected, Exit: 3, Category: "unknown_agent"}},
		{"local/w1:p8", "hi eight", "", 18, notify.SendResult{Outcome: notify.OutcomeRejected, Exit: 18, Category: "refused"}},
		{"local/w1:p9", "hi nine", "", 17, notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 17, Category: "busy"}},
		{"local/w1:p10", "hi ten", "", 7, notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 7, Category: "exit"}},
		{"local/w1:p11", "hi eleven", "", 42, notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 42, Category: "exit"}},
		{"owner-agent", "hi owner", "SENT to owner\n", 0, notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}},
	}
	var rules []fakesoho.Rule
	for _, row := range rows {
		rules = append(rules, fakesoho.Rule{Argv: sendArgv(row.ref, row.msg, sendTestTimeout), Stdout: row.stdout, Code: row.code})
	}
	exe, dir := installFake(t, rules...)
	s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendTestTimeout}
	ctx := context.Background()
	for i, row := range rows {
		res := s.Send(ctx, row.ref, row.msg)
		if res != row.want {
			t.Errorf("row %d (%s exit %d): got %+v, want %+v", i, row.ref, row.code, res, row.want)
		}
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != len(rows) {
		t.Fatalf("calls = %d, want %d", len(calls), len(rows))
	}
	for i, c := range calls {
		want := sendArgv(rows[i].ref, rows[i].msg, sendTestTimeout)
		if !sameStrings(c.Argv, want) {
			t.Errorf("call %d argv = %v, want %v", i, c.Argv, want)
		}
		childEnvExact(t, c.Env, childEnv)
	}
}

// TestNotifySendValidation: every invalid ref or message is refused with
// a usage rejection and no fake call at all.
func TestNotifySendValidation(t *testing.T) {
	const msg = "hello"
	cases := []struct {
		name, ref, m string
	}{
		{"empty ref", "", msg},
		{"space in ref", "not a ref", msg},
		{"ref without pane", "local/w1", msg},
		{"ref with two colons", "local/w1:p1:extra", msg},
		{"machine over 64", strings.Repeat("a", 65) + "/w1:p1", msg},
		{"agent name starts with digit", "9agent", msg},
		{"agent name uppercase", "Agent-name", msg},
		{"empty message", "local/w1:p3", ""},
		{"message over 2048", "local/w1:p3", strings.Repeat("a", 2049)},
		{"leading dash", "local/w1:p3", "-leading"},
		{"newline", "local/w1:p3", "a\nb"},
		{"carriage return", "local/w1:p3", "a\rb"},
		{"control byte", "local/w1:p3", "a\x01b"},
		{"bare newline", "local/w1:p3", "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A rule that would match if the invalid input were run:
			// any call shows up in the log.
			exe, dir := installFake(t, fakesoho.Rule{Argv: sendArgv(tc.ref, tc.m, sendTestTimeout), Stdout: "sent to x\n", Code: 0})
			s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendTestTimeout}
			res := s.Send(context.Background(), tc.ref, tc.m)
			want := notify.SendResult{Outcome: notify.OutcomeRejected, Exit: -1, Category: "usage"}
			if res != want {
				t.Errorf("res = %+v, want %+v", res, want)
			}
			calls, err := fakesoho.ReadCalls(dir)
			if err == nil && len(calls) != 0 {
				t.Errorf("invalid input ran the fake: %d calls", len(calls))
			}
		})
	}
}

// TestNotifySendBoundaries: exactly 2048 bytes and a message with an
// ordinary space are valid inputs and run the fake.
func TestNotifySendBoundaries(t *testing.T) {
	msg := strings.Repeat("a", 2048)
	exe, dir := installFake(t,
		fakesoho.Rule{Argv: sendArgv("local/w1:p3", msg, sendTestTimeout), Stdout: "sent to x\n", Code: 0},
		fakesoho.Rule{Argv: sendArgv("local/w1:p4", "a b", sendTestTimeout), Stdout: "queued for x\n", Code: 0},
	)
	s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendTestTimeout}
	if res := s.Send(context.Background(), "local/w1:p3", msg); res != (notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}) {
		t.Errorf("2048-byte message: res = %+v", res)
	}
	if res := s.Send(context.Background(), "local/w1:p4", "a b"); res != (notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "queued", Exit: 0}) {
		t.Errorf("message with a space: res = %+v", res)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 2 {
		t.Errorf("calls = %d, want 2", len(calls))
	}
}

// TestNotifySendDefaultTimeout: a zero Timeout passes --timeout 5000.
func TestNotifySendDefaultTimeout(t *testing.T) {
	wantArgv := []string{"send", "local/w1:p1", "hi", "--now", "--timeout", "5000"}
	exe, dir := installFake(t, fakesoho.Rule{Argv: wantArgv, Stdout: "sent to x\n", Code: 0})
	s := soho.NotifySender{Bin: exe, Environ: childEnv}
	res := s.Send(context.Background(), "local/w1:p1", "hi")
	if res != (notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}) {
		t.Fatalf("res = %+v, want accepted/sent", res)
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != 1 || !sameStrings(calls[0].Argv, wantArgv) {
		t.Errorf("argv = %v, want %v", calls, wantArgv)
	}
}

// TestNotifySendDeadline: a fake that outlives the process bound
// (Timeout + 3s) is reported as an uncertain deadline (killed in flight: receipt not
// proved, never resent) within the bound, the runner's WaitDelay grace and
// a small margin; a caller context cancelled while the child runs is the
// same.
func TestNotifySendDeadline(t *testing.T) {
	// bound = Timeout + 3s; the runner adds its own 5s WaitDelay grace
	// on top; the margin absorbs a slow fake start.
	limit := sendShortTimeout + 3*time.Second + 5*time.Second + 2*time.Second
	t.Run("past bound", func(t *testing.T) {
		// ExitOnTerm 1: the child exits at once on the cancel signal —
		// the Windows TerminateProcess shape — so the run ends at the
		// bound with an ordinary exit code, and the adapter must still
		// report a deadline.
		exe, _ := installFake(t, fakesoho.Rule{
			Argv:       sendArgv("local/wd:p1", "hold", sendShortTimeout),
			Delay:      30000,
			ExitOnTerm: 1,
			Code:       0,
		})
		s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendShortTimeout}
		start := time.Now()
		res := s.Send(context.Background(), "local/wd:p1", "hold")
		elapsed := time.Since(start)
		t.Logf("past bound: elapsed=%s bound=%s", elapsed, sendShortTimeout+3*time.Second)
		want := notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: -1, Category: "deadline"}
		if res != want {
			t.Fatalf("res = %+v, want %+v", res, want)
		}
		if elapsed >= limit {
			t.Errorf("elapsed = %s, want < %s (bound + WaitDelay grace + margin)", elapsed, limit)
		}
	})
	t.Run("caller ctx cancelled mid-run", func(t *testing.T) {
		// The child would exit 0 at 100ms; the caller context is
		// cancelled at 50ms, before the child exits: the cancel must
		// reach the child and the run must be reported as a deadline,
		// not accepted.
		exe, _ := installFake(t, fakesoho.Rule{
			Argv:  sendArgv("local/wd:p2", "hold", sendShortTimeout),
			Delay: 100,
			Code:  0,
		})
		s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendShortTimeout}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		res := s.Send(ctx, "local/wd:p2", "hold")
		elapsed := time.Since(start)
		t.Logf("ctx cancelled mid-run: elapsed=%s", elapsed)
		want := notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: -1, Category: "deadline"}
		if res != want {
			t.Fatalf("res = %+v, want %+v", res, want)
		}
		// Worst case: the cancel signal is ignored by the fake and the
		// runner's 5s WaitDelay grace (counted from the cancel) ends
		// the run.
		if elapsed >= 10*time.Second {
			t.Errorf("elapsed = %s, want < 10s (cancel + WaitDelay grace + margin)", elapsed)
		}
	})
}

// TestNotifySendUnavailable: a missing binary is a transient
// unavailable, without a subprocess.
func TestNotifySendUnavailable(t *testing.T) {
	s := soho.NotifySender{Bin: "no-such-herdr-soho-binary", Environ: childEnv, Timeout: sendTestTimeout}
	res := s.Send(context.Background(), "local/w1:p1", "hi")
	want := notify.SendResult{Outcome: notify.OutcomeTransient, Exit: -1, Category: "unavailable"}
	if res != want {
		t.Errorf("res = %+v, want %+v", res, want)
	}
}

// TestNotifySendLargeStdout: 1 MiB of stdout on exit 0 is still
// accepted and classified from the first line; the adapter keeps at
// most 4096 bytes of the output internally (the rest is dropped), so
// the run stays fast and memory-flat regardless of volume.
func TestNotifySendLargeStdout(t *testing.T) {
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   sendArgv("local/big:p1", "big", sendTestTimeout),
		Stdout: "sent to x\n" + strings.Repeat("y", 1024*1024),
		Code:   0,
	})
	s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendTestTimeout}
	start := time.Now()
	res := s.Send(context.Background(), "local/big:p1", "big")
	elapsed := time.Since(start)
	t.Logf("1 MiB stdout: elapsed=%s", elapsed)
	want := notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}
	if res != want {
		t.Errorf("res = %+v, want %+v", res, want)
	}
	if elapsed >= 10*time.Second {
		t.Errorf("elapsed = %s, want well under 10s", elapsed)
	}
}

// TestNotifySendConcurrent: concurrent Sends against the fake stay
// correct under -race; each Send must use its own runner, because the
// runner is not safe for concurrent use.
func TestNotifySendConcurrent(t *testing.T) {
	const n = 8
	exe, dir := installFake(t, fakesoho.Rule{
		Argv:   sendArgv("local/c:p1", "hello all", sendTestTimeout),
		Stdout: "sent to all\n",
		Code:   0,
	})
	s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendTestTimeout}
	var wg sync.WaitGroup
	results := make([]notify.SendResult, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = s.Send(context.Background(), "local/c:p1", "hello all")
		}(i)
	}
	wg.Wait()
	want := notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}
	for i, res := range results {
		if res != want {
			t.Errorf("goroutine %d: res = %+v, want %+v", i, res, want)
		}
	}
	calls, err := fakesoho.ReadCalls(dir)
	if err != nil {
		t.Fatalf("ReadCalls: %v", err)
	}
	if len(calls) != n {
		t.Errorf("calls = %d, want %d", len(calls), n)
	}
}

// TestNotifySendCanary: a secret in the message and in the child's
// stdout never reaches the returned SendResult.
func TestNotifySendCanary(t *testing.T) {
	const canary = "CANARY-SECRET-1"
	msg := "note " + canary + " ok"
	exe, _ := installFake(t, fakesoho.Rule{
		Argv:   sendArgv("local/secret:p1", msg, sendTestTimeout),
		Stdout: "sent " + canary + "\n",
		Code:   0,
	})
	s := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendTestTimeout}
	res := s.Send(context.Background(), "local/secret:p1", msg)
	want := notify.SendResult{Outcome: notify.OutcomeAccepted, Status: "sent", Exit: 0}
	if res != want {
		t.Fatalf("res = %+v, want %+v", res, want)
	}
	rendered := fmt.Sprintf("%+v", res)
	if strings.Contains(rendered, canary) {
		t.Errorf("the rendered SendResult %q contains the canary", rendered)
	}
}

// TestNotifySendBound: a send slower than Timeout + 3s is killed in flight
// (uncertain) under the default bound, and accepted when the caller passes
// a larger process Bound.
func TestNotifySendBound(t *testing.T) {
	rule := fakesoho.Rule{Argv: sendArgv("local/wb:p1", "slow", sendShortTimeout), Stdout: "sent to x\n", Code: 0, Delay: 4500}
	exe, _ := installFake(t, rule)
	short := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendShortTimeout}
	if res := short.Send(context.Background(), "local/wb:p1", "slow"); res.Outcome != notify.OutcomeUncertain || res.Category != "deadline" {
		t.Fatalf("default bound: %+v, want uncertain deadline", res)
	}
	long := soho.NotifySender{Bin: exe, Environ: childEnv, Timeout: sendShortTimeout, Bound: 20 * time.Second}
	if res := long.Send(context.Background(), "local/wb:p1", "slow"); res.Outcome != notify.OutcomeAccepted || res.Status != "sent" {
		t.Fatalf("larger bound: %+v, want accepted sent", res)
	}
}
