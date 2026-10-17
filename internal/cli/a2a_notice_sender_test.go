package cli

import (
	"context"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/soho"
)

// recordingSender records the deadline of the context it is sent with.
type recordingSender struct {
	deadline time.Time
	ok       bool
}

func (r *recordingSender) Send(ctx context.Context, ref, message string) notify.SendResult {
	r.deadline, r.ok = ctx.Deadline()
	return notify.SendResult{Outcome: notify.OutcomeAccepted}
}

// TestA2aNoticeSenderBounds: the serve's notice sender gives soho its
// own timeout and process bound, and bounds the whole attempt under the
// claim lease, so a claim never expires while its send runs.
func TestA2aNoticeSenderBounds(t *testing.T) {
	s, ok := newA2aNoticeSender("soho", nil).(a2aNoticeSender)
	if !ok {
		t.Fatalf("newA2aNoticeSender returned %T, want a2aNoticeSender", newA2aNoticeSender("soho", nil))
	}
	inner, ok := s.inner.(soho.NotifySender)
	if !ok {
		t.Fatalf("inner sender is %T, want soho.NotifySender", s.inner)
	}
	if inner.Timeout != a2aNoticeSendTimeout || inner.Bound != a2aNoticeSendBound || s.total != a2aNoticeSendTotal {
		t.Errorf("bounds = timeout %v, bound %v, total %v; want %v, %v, %v",
			inner.Timeout, inner.Bound, s.total, a2aNoticeSendTimeout, a2aNoticeSendBound, a2aNoticeSendTotal)
	}
	if inner.Bin != "soho" {
		t.Errorf("Bin = %q, want soho", inner.Bin)
	}
	if !(a2aNoticeSendTimeout < a2aNoticeSendBound && a2aNoticeSendBound < a2aNoticeSendTotal && a2aNoticeSendTotal < notify.LeaseDuration) {
		t.Errorf("want timeout < bound < total < lease, got %v, %v, %v, %v",
			a2aNoticeSendTimeout, a2aNoticeSendBound, a2aNoticeSendTotal, notify.LeaseDuration)
	}
	// The bound must leave room past the 8 s that a slow recipient was
	// measured to need, the default that left notices uncertain.
	if a2aNoticeSendBound < 20*time.Second {
		t.Errorf("a2aNoticeSendBound = %v, want at least 20s", a2aNoticeSendBound)
	}
}

// TestA2aNoticeSenderDeadline: every send runs with a deadline of at
// most total, and an earlier deadline of the caller is kept.
func TestA2aNoticeSenderDeadline(t *testing.T) {
	rec := &recordingSender{}
	s := a2aNoticeSender{inner: rec, total: 3 * time.Second}
	start := time.Now()
	if got := s.Send(context.Background(), "local/w1:p1", "m"); got.Outcome != notify.OutcomeAccepted {
		t.Fatalf("outcome = %v, want accepted", got.Outcome)
	}
	if !rec.ok {
		t.Fatal("inner send had no deadline")
	}
	if d := rec.deadline.Sub(start); d > 3*time.Second+100*time.Millisecond || d < 2*time.Second {
		t.Errorf("deadline in %v, want about 3s", d)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	want, _ := ctx.Deadline()
	s.Send(ctx, "local/w1:p1", "m")
	if !rec.ok || !rec.deadline.Equal(want) {
		t.Errorf("deadline = %v, want the caller's earlier %v", rec.deadline, want)
	}
}
