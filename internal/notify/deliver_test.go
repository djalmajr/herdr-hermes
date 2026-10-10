package notify

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

// recordingSender is a fake Sender: it records the calls, counts the
// concurrent sends, can block the first send on a channel and can delay
// every send.
type recordingSender struct {
	mu       sync.Mutex
	refs     []string
	messages []string
	active   int
	max      int
	block    chan struct{}
	entered  chan struct{}
	delay    time.Duration
	result   SendResult
	perRef   map[string]SendResult
}

func (f *recordingSender) Send(ctx context.Context, ref, message string) SendResult {
	var block, entered chan struct{}
	var delay time.Duration
	f.mu.Lock()
	f.refs = append(f.refs, ref)
	f.messages = append(f.messages, message)
	f.active++
	if f.active > f.max {
		f.max = f.active
	}
	if f.block != nil {
		block = f.block
		f.block = nil
	}
	entered = f.entered
	f.entered = nil
	delay = f.delay
	f.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	if block != nil {
		<-block
	}
	f.mu.Lock()
	f.active--
	defer f.mu.Unlock()
	if r, ok := f.perRef[ref]; ok {
		return r
	}
	return f.result
}

func (f *recordingSender) calls() (refs, messages []string, max int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	refs = append([]string(nil), f.refs...)
	messages = append([]string(nil), f.messages...)
	max = f.max
	return refs, messages, max
}

// fakeRender renders a stable, checkable message for one delivery.
func fakeRender(n *Notification, d Delivery) string {
	return "class=" + n.Class + " ref=" + d.Ref + " pane=" + n.Pane
}

// deliverSetup enables notify and raises n notifications, each with one
// pending delivery.
func deliverSetup(t *testing.T, n int) (*Store, *ledgerTestClock) {
	t.Helper()
	ob := openOutbox(t, t.TempDir())
	clk := newLedgerTestClock()
	s := Open(ob, clk.Now)
	if err := s.UpdateRegistry(func(reg *Registry) error {
		reg.Owners[testProjetoID] = testOwnerRef
		return nil
	}); err != nil {
		t.Fatalf("UpdateRegistry: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, created, err := s.Raise(RaiseInput{ID: "ra-" + strconv.Itoa(i+1), Class: ClassStuck, Projeto: testProjetoID}, fakeProjectRaise); err != nil || !created {
			t.Fatalf("Raise %d: created=%v err=%v", i+1, created, err)
		}
	}
	return s, clk
}

// TestDeliverConcurrency: at most Concurrency sends run at once and
// every claimed delivery is sent and rendered.
func TestDeliverConcurrency(t *testing.T) {
	s, _ := deliverSetup(t, 8)
	f := &recordingSender{result: SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0}, delay: 15 * time.Millisecond}
	summary, err := Deliver(context.Background(), s, f, fakeRender, DeliverOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	want := DeliverSummary{Claimed: 8, Accepted: 8}
	if summary != want {
		t.Fatalf("summary = %+v, want %+v", summary, want)
	}
	refs, messages, max := f.calls()
	if len(refs) != 8 {
		t.Fatalf("sends = %d, want 8", len(refs))
	}
	if max != 2 {
		t.Fatalf("max concurrent sends = %d, want 2 (bound respected and reached)", max)
	}
	const wantMsg = "class=stuck ref=" + testOwnerRef + " pane="
	for _, m := range messages {
		if m != wantMsg {
			t.Fatalf("message = %q, want %q", m, wantMsg)
		}
	}
	// everything is final: a second run claims nothing.
	summary, err = Deliver(context.Background(), s, f, fakeRender, DeliverOptions{Concurrency: 2})
	if err != nil || summary != (DeliverSummary{}) {
		t.Fatalf("second Deliver = %+v, %v; want zero summary", summary, err)
	}
}

// TestDeliverDefaults: zero options use the defaults (limit 32,
// concurrency 4).
func TestDeliverDefaults(t *testing.T) {
	s, _ := deliverSetup(t, 8)
	f := &recordingSender{result: SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0}, delay: 15 * time.Millisecond}
	summary, err := Deliver(context.Background(), s, f, fakeRender, DeliverOptions{})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	want := DeliverSummary{Claimed: 8, Accepted: 8}
	if summary != want {
		t.Fatalf("summary = %+v, want %+v", summary, want)
	}
	if _, _, max := f.calls(); max > 4 {
		t.Fatalf("max concurrent sends = %d, want at most 4", max)
	}
}

// TestDeliverNotLocked: the store lock is free while a send runs: a
// concurrent ClaimDue from another goroutine completes while the sender
// is blocked, and it sees the in-flight claim (no double claim).
func TestDeliverNotLocked(t *testing.T) {
	s, _ := deliverSetup(t, 1)
	other := Open(s.ob, newLedgerTestClock().Now)
	releaseCh := make(chan struct{})
	entered := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	t.Cleanup(release)
	f := &recordingSender{
		block:   releaseCh,
		entered: entered,
		result:  SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0},
	}
	res := make(chan struct {
		summary DeliverSummary
		err     error
	}, 1)
	go func() {
		summary, err := Deliver(context.Background(), s, f, fakeRender, DeliverOptions{Concurrency: 4})
		res <- struct {
			summary DeliverSummary
			err     error
		}{summary, err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the sender was not entered within 10s (the store may be locked during the send)")
	}
	claimsCh := make(chan []Claim, 1)
	errCh := make(chan error, 1)
	go func() {
		claims, err := other.ClaimDue(0)
		claimsCh <- claims
		errCh <- err
	}()
	select {
	case claims := <-claimsCh:
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent ClaimDue while the sender blocks: %v", err)
		}
		if len(claims) != 0 {
			t.Fatalf("concurrent ClaimDue claimed %d delivery while the send is in flight, want 0", len(claims))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent ClaimDue did not complete while the sender blocks: the store is locked during the send")
	}
	release()
	var r struct {
		summary DeliverSummary
		err     error
	}
	select {
	case r = <-res:
	case <-time.After(10 * time.Second):
		t.Fatal("Deliver did not return within 10s of the send finishing")
	}
	if r.err != nil {
		t.Fatalf("Deliver: %v", r.err)
	}
	want := DeliverSummary{Claimed: 1, Accepted: 1}
	if r.summary != want {
		t.Fatalf("summary = %+v, want %+v", r.summary, want)
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	d := led.Notifications[0].Deliveries[0]
	if d.State != StateAccepted || d.AcceptStatus != "sent" {
		t.Fatalf("delivery = %+v, want accepted/sent", d)
	}
}

// TestLeaseForRun: the run lease is the time until the context deadline
// (store clock) plus the crash margin with a deadline, LeaseDuration
// without one or with one that already passed.
func TestLeaseForRun(t *testing.T) {
	now := newLedgerTestClock().Now()
	if got := leaseForRun(context.Background(), now); got != LeaseDuration {
		t.Fatalf("lease = %v, want %v (no deadline)", got, LeaseDuration)
	}
	budget := 10 * time.Minute
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(budget))
	defer cancel()
	if got := leaseForRun(ctx, now); got != budget+LeaseDuration {
		t.Fatalf("lease = %v, want %v (the budget plus the crash margin)", got, budget+LeaseDuration)
	}
	past, cancel := context.WithDeadline(context.Background(), now.Add(-time.Minute))
	defer cancel()
	if got := leaseForRun(past, now); got != LeaseDuration {
		t.Fatalf("lease = %v, want %v (an already-passed deadline)", got, LeaseDuration)
	}
}

// TestDeliverLeaseCoversRunBudget: a Deliver whose context deadline is
// 10 minutes away claims with a lease that covers the run's remaining
// budget plus the crash margin: while its sender is blocked, advancing
// the store clock by LeaseDuration + 1s does not make the delivery
// claimable by another ClaimDue, and after the blocked send returns
// accepted the first completion is recorded (accepted on attempt 1).
func TestDeliverLeaseCoversRunBudget(t *testing.T) {
	s, clk := deliverSetup(t, 1)
	budget := 10 * time.Minute
	// the deadline is always in the real future (the store clock is an
	// injected fixture that may sit behind or ahead of the wall clock),
	// so the run's context is live for the whole test.
	base := time.Now()
	if clk.Now().After(base) {
		base = clk.Now()
	}
	ctx, cancel := context.WithDeadline(context.Background(), base.Add(budget))
	defer cancel()
	releaseCh := make(chan struct{})
	entered := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	t.Cleanup(release)
	f := &recordingSender{
		block:   releaseCh,
		entered: entered,
		result:  SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0},
	}
	res := make(chan struct {
		summary DeliverSummary
		err     error
	}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		summary, err := Deliver(ctx, s, f, fakeRender, DeliverOptions{})
		res <- struct {
			summary DeliverSummary
			err     error
		}{summary, err}
	}()
	// the cleanup runs also on assertion failure: release the sender and
	// wait for the run to finish before the store's temp dir is removed.
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the sender was not entered within 10s")
	}
	// the lease covers the run's budget: LeaseUntil is at least now +
	// the budget + the crash margin.
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	d := led.Notifications[0].Deliveries[0]
	if d.State != StateInFlight {
		t.Fatalf("delivery state = %s, want in_flight", d.State)
	}
	leaseUntil, err := time.Parse(TSLayout, d.LeaseUntil)
	if err != nil {
		t.Fatalf("LeaseUntil %q: %v", d.LeaseUntil, err)
	}
	want := clk.Now().Add(budget).Add(LeaseDuration)
	if leaseUntil.Before(want) {
		t.Fatalf("LeaseUntil = %s, want at least %s (the run's budget plus the crash margin)", d.LeaseUntil, want.Format(TSLayout))
	}
	// while the send is blocked, advancing the store clock by
	// LeaseDuration + 1s does not make the delivery claimable.
	clk.Add(LeaseDuration + time.Second)
	claims, err := Open(s.ob, clk.Now).ClaimDue(0)
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims = %+v, want 0 (a live claim must not become due while its run can still send it)", claims)
	}
	// after the blocked send returns accepted, the first completion is
	// recorded: accepted on attempt 1.
	release()
	var r struct {
		summary DeliverSummary
		err     error
	}
	select {
	case r = <-res:
	case <-time.After(10 * time.Second):
		t.Fatal("Deliver did not return within 10s of the send finishing")
	}
	if r.err != nil {
		t.Fatalf("Deliver: %v", r.err)
	}
	wantSummary := DeliverSummary{Claimed: 1, Accepted: 1}
	if r.summary != wantSummary {
		t.Fatalf("summary = %+v, want %+v", r.summary, wantSummary)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	d = led.Notifications[0].Deliveries[0]
	if d.State != StateAccepted || d.Attempts != 1 || d.AcceptStatus != "sent" {
		t.Fatalf("delivery = %+v, want accepted on attempt 1 (the first completion is recorded)", d)
	}
}

// TestDeliverCtxDone: a context already done before the sends start is
// not sent: the claim is released unsent, without consuming the attempt,
// and stays due at once (Deferred), so a backlog larger than one bounded
// run never exhausts deliveries that were never tried.
func TestDeliverCtxDone(t *testing.T) {
	s, _ := deliverSetup(t, 1)
	f := &recordingSender{result: SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for run := 0; run < MaxAttempts+2; run++ {
		summary, err := Deliver(ctx, s, f, fakeRender, DeliverOptions{})
		if err != nil {
			t.Fatalf("Deliver: %v", err)
		}
		want := DeliverSummary{Claimed: 1, Deferred: 1}
		if summary != want {
			t.Fatalf("run %d: summary = %+v, want %+v", run, summary, want)
		}
	}
	if refs, _, _ := f.calls(); len(refs) != 0 {
		t.Fatalf("sender called %d times, want 0 (context done before the send)", len(refs))
	}
	led, err := s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	d := led.Notifications[0].Deliveries[0]
	if d.State != StatePending || d.Attempts != 0 || d.LeaseUntil != "" {
		t.Fatalf("delivery = %+v, want pending, 0 attempts, no lease", d)
	}
	if d.FirstAttemptAt != "" || d.LastAttemptAt != "" || d.LastError != "" || d.LastExit != nil {
		t.Fatalf("delivery = %+v, want no attempt recorded", d)
	}
	// A live context then delivers it on its first real attempt.
	summary, err := Deliver(context.Background(), s, f, fakeRender, DeliverOptions{})
	if err != nil || summary != (DeliverSummary{Claimed: 1, Accepted: 1}) {
		t.Fatalf("live Deliver = %+v, %v; want one accepted", summary, err)
	}
	led, err = s.LoadLedger()
	if err != nil {
		t.Fatalf("LoadLedger: %v", err)
	}
	if d := led.Notifications[0].Deliveries[0]; d.State != StateAccepted || d.Attempts != 1 {
		t.Fatalf("delivery = %+v, want accepted on attempt 1", d)
	}
}

// TestReleaseRestoresAttemptTimes: releasing a retry claim restores the
// attempt count and the timestamps of the earlier real attempt, and a
// stale release is ignored.
func TestReleaseRestoresAttemptTimes(t *testing.T) {
	s, clk := deliverSetup(t, 1)
	claims, err := s.ClaimDue(0)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue = %d, %v", len(claims), err)
	}
	if err := s.Complete(claims[0], SendResult{Outcome: OutcomeTransient, Exit: 17, Category: "busy"}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	led, _ := s.LoadLedger()
	first := led.Notifications[0].Deliveries[0]
	clk.Add(RetrySchedule[0])
	retry, err := s.ClaimDue(0)
	if err != nil || len(retry) != 1 || retry[0].Attempt != 2 {
		t.Fatalf("retry ClaimDue = %+v, %v", retry, err)
	}
	if err := s.Release(retry[0]); err != nil {
		t.Fatalf("Release: %v", err)
	}
	led, _ = s.LoadLedger()
	d := led.Notifications[0].Deliveries[0]
	if d.State != StatePending || d.Attempts != 1 || d.FirstAttemptAt != first.FirstAttemptAt || d.LastAttemptAt != first.LastAttemptAt || d.LastError != "busy" {
		t.Fatalf("released = %+v, want pending with the first attempt's record (%+v)", d, first)
	}
	if err := s.Release(retry[0]); err != nil {
		t.Fatalf("stale Release: %v", err)
	}
	led2, _ := s.LoadLedger()
	if got := led2.Notifications[0].Deliveries[0]; got.Attempts != 1 || got.State != StatePending {
		t.Fatalf("stale Release changed the delivery: %+v", got)
	}
}

// TestDeliverDisabled: a disabled store delivers nothing and calls no
// sender.
func TestDeliverDisabled(t *testing.T) {
	ob := openOutbox(t, t.TempDir())
	s := Open(ob, newLedgerTestClock().Now)
	f := &recordingSender{}
	summary, err := Deliver(context.Background(), s, f, fakeRender, DeliverOptions{})
	if err != nil || summary != (DeliverSummary{}) {
		t.Fatalf("Deliver = %+v, %v; want zero summary", summary, err)
	}
	if refs, _, _ := f.calls(); len(refs) != 0 {
		t.Fatalf("sender called %d times on a disabled store, want 0", len(refs))
	}
}

// TestRetryRequeuesUncertainAndExhausted: an uncertain delivery is never
// claimed again until an explicit Retry; Retry re-queues it once, keeps the
// attempts, and finds nothing for an accepted or unknown delivery.
func TestRetryRequeuesUncertainAndExhausted(t *testing.T) {
	s, clk := deliverSetup(t, 1)
	claims, err := s.ClaimDue(0)
	if err != nil || len(claims) != 1 {
		t.Fatalf("ClaimDue = %d, %v", len(claims), err)
	}
	if err := s.Complete(claims[0], SendResult{Outcome: OutcomeUncertain, Exit: -1, Category: "deadline"}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	clk.Add(24 * time.Hour)
	if again, _ := s.ClaimDue(0); len(again) != 0 {
		t.Fatalf("an uncertain delivery was claimed again without Retry (%d)", len(again))
	}
	id, role := claims[0].NotificationID, claims[0].Roles[0]
	if found, err := s.Retry(id, role); err != nil || !found {
		t.Fatalf("Retry = %v, %v; want found", found, err)
	}
	retry, err := s.ClaimDue(0)
	if err != nil || len(retry) != 1 || retry[0].Attempt != 2 {
		t.Fatalf("after Retry ClaimDue = %+v, %v; want attempt 2", retry, err)
	}
	if err := s.Complete(retry[0], SendResult{Outcome: OutcomeAccepted, Status: "sent", Exit: 0}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if found, _ := s.Retry(id, role); found {
		t.Fatal("Retry found an accepted delivery")
	}
	if found, _ := s.Retry("n00000000000000000000", role); found {
		t.Fatal("Retry found an unknown notification")
	}

	// Exhausted: one more attempt only.
	s2, clk2 := deliverSetup(t, 1)
	for i := 0; i < MaxAttempts; i++ {
		c, err := s2.ClaimDue(0)
		if err != nil || len(c) != 1 {
			t.Fatalf("attempt %d: ClaimDue = %d, %v", i+1, len(c), err)
		}
		if err := s2.Complete(c[0], SendResult{Outcome: OutcomeTransient, Exit: 17, Category: "busy"}); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		clk2.Add(time.Hour)
	}
	led, _ := s2.LoadLedger()
	d := led.Notifications[0].Deliveries[0]
	if d.State != StateExhausted {
		t.Fatalf("state = %s, want exhausted", d.State)
	}
	if found, err := s2.Retry(led.Notifications[0].ID, d.Roles[0]); err != nil || !found {
		t.Fatalf("Retry exhausted = %v, %v", found, err)
	}
	c, _ := s2.ClaimDue(0)
	if len(c) != 1 {
		t.Fatalf("the re-queued exhausted delivery was not claimed (%d)", len(c))
	}
	if err := s2.Complete(c[0], SendResult{Outcome: OutcomeTransient, Exit: 17, Category: "busy"}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	led, _ = s2.LoadLedger()
	if got := led.Notifications[0].Deliveries[0]; got.State != StateExhausted || got.Attempts != MaxAttempts+1 {
		t.Fatalf("after one more failure = %+v, want exhausted after %d attempts", got, MaxAttempts+1)
	}
}
