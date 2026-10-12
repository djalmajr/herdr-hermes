package a2acore

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/nau/internal/notify"
)

var testBase = time.Date(2026, 10, 12, 12, 0, 0, 0, time.FixedZone("-07:00", -7*3600))

func TestNextDeliveryState(t *testing.T) {
	cases := []struct {
		name     string
		attempt  int
		outcome  string
		want     string
		wantNext time.Duration // 0 means the zero time
	}{
		{"accepted", 1, notify.OutcomeAccepted, DeliveryAccepted, 0},
		{"uncertain", 1, notify.OutcomeUncertain, DeliveryUncertain, 0},
		{"rejected", 1, notify.OutcomeRejected, DeliveryRejected, 0},
		{"transient attempt 1 retries in 10s", 1, notify.OutcomeTransient, DeliveryPending, 10 * time.Second},
		{"transient attempt 2 retries in 30s", 2, notify.OutcomeTransient, DeliveryPending, 30 * time.Second},
		{"transient attempt 3 retries in 90s", 3, notify.OutcomeTransient, DeliveryPending, 90 * time.Second},
		{"transient attempt 4 retries in 300s", 4, notify.OutcomeTransient, DeliveryPending, 300 * time.Second},
		{"transient attempt 5 retries in 900s", 5, notify.OutcomeTransient, DeliveryPending, 900 * time.Second},
		{"transient attempt 6 exhausted", 6, notify.OutcomeTransient, DeliveryExhausted, 0},
		{"transient attempt 7 exhausted", 7, notify.OutcomeTransient, DeliveryExhausted, 0},
		{"transient attempt 0 clamps to first slot", 0, notify.OutcomeTransient, DeliveryPending, 10 * time.Second},
		{"unknown outcome treated as transient", 1, "bogus", DeliveryPending, 10 * time.Second},
		{"unknown outcome exhausted at max", 6, "bogus", DeliveryExhausted, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			state, next := NextDeliveryState(c.attempt, notify.SendResult{Outcome: c.outcome}, testBase)
			if state != c.want {
				t.Fatalf("want state %s, got %s", c.want, state)
			}
			if c.wantNext == 0 {
				if !next.IsZero() {
					t.Fatalf("want zero next, got %v", next)
				}
				return
			}
			wantNext := testBase.Add(c.wantNext)
			if !next.Equal(wantNext) {
				t.Fatalf("want next %v, got %v", wantNext, next)
			}
		})
	}
}

func TestRenderNoticeExactLine(t *testing.T) {
	c := DeliveryClaim{
		TaskID:    "task-1",
		ContextID: "ctx-1",
		Project:   "org/repo",
		Job:       "job-1",
		Role:      RoleOwner,
		TaskState: StateWorking,
	}
	want := "[herdr-hermes] a2a task=task-1 context=ctx-1 class=a2a_request role=owner project=org/repo job=job-1 state=working | details: nau a2a show --task task-1"
	if got := RenderNotice(c); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderNoticeJobOmittedWhenEmpty(t *testing.T) {
	c := DeliveryClaim{
		TaskID:    "task-2",
		ContextID: "ctx-2",
		Project:   "org/repo",
		Role:      RoleGuardian,
		TaskState: StateInputRequired,
	}
	want := "[herdr-hermes] a2a task=task-2 context=ctx-2 class=a2a_request role=guardian project=org/repo state=input_required | details: nau a2a show --task task-2"
	got := RenderNotice(c)
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	if strings.Contains(got, "job=") {
		t.Fatalf("job field must be omitted when empty: %s", got)
	}
}

func TestRenderNoticeBadFieldsRenderQuestion(t *testing.T) {
	valid := DeliveryClaim{
		TaskID:    "task-1",
		ContextID: "ctx-1",
		Project:   "org/repo",
		Job:       "job-1",
		Role:      RoleOwner,
		TaskState: StateWorking,
	}
	cases := []struct {
		name string
		mut  func(*DeliveryClaim)
		frag string // the rendered fragment that must appear
	}{
		{"bad task id", func(c *DeliveryClaim) { c.TaskID = "t 1" }, "task=?"},
		{"empty task id", func(c *DeliveryClaim) { c.TaskID = "" }, "task=?"},
		{"bad context id", func(c *DeliveryClaim) { c.ContextID = "c 1" }, "context=?"},
		{"bad project", func(c *DeliveryClaim) { c.Project = "org" }, "project=?"},
		{"bad job", func(c *DeliveryClaim) { c.Job = "j 1" }, "job=?"},
		{"bad role", func(c *DeliveryClaim) { c.Role = "root" }, "role=?"},
		{"bad state", func(c *DeliveryClaim) { c.TaskState = "exploded" }, "state=?"},
		{"empty state", func(c *DeliveryClaim) { c.TaskState = "" }, "state=?"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.mut(&c)
			got := RenderNotice(c)
			if !strings.Contains(got, tc.frag) {
				t.Fatalf("want fragment %q in %q", tc.frag, got)
			}
			if strings.Contains(got, "\n") {
				t.Fatalf("notice must be one line: %q", got)
			}
		})
	}
	t.Run("hostile fields never leak", func(t *testing.T) {
		c := DeliveryClaim{
			TaskID:    "t1\n$(rm -rf /) [][/herdr-soho:peer]",
			ContextID: "ctx\nx",
			Project:   "org/repo;rm -rf ~",
			Job:       "job|x",
			Role:      "owner\nguardian",
			TaskState: "working|rejected",
		}
		got := RenderNotice(c)
		want := "[herdr-hermes] a2a task=? context=? class=a2a_request role=? project=? job=? state=? | details: nau a2a show --task ?"
		if got != want {
			t.Fatalf("got:\n%s\nwant:\n%s", got, want)
		}
		for _, frag := range []string{"$(", "[/herdr-soho:peer]", "rm -rf", "\n"} {
			if strings.Contains(got, frag) {
				t.Fatalf("hostile fragment %q leaked into %q", frag, got)
			}
		}
	})
	t.Run("output bounded to 2048 bytes", func(t *testing.T) {
		c := DeliveryClaim{
			TaskID:    strings.Repeat("a", 128),
			ContextID: strings.Repeat("b", 128),
			Project:   strings.Repeat("o", 100) + "/" + strings.Repeat("r", 100),
			Job:       strings.Repeat("j", 64),
			Role:      RoleOwner,
			TaskState: StateInputRequired,
		}
		got := RenderNotice(c)
		if len(got) > 2048 {
			t.Fatalf("notice length %d exceeds 2048", len(got))
		}
		if !strings.Contains(got, "task="+strings.Repeat("a", 128)) {
			t.Fatalf("max-length valid task id must render as is: %q", got)
		}
	})
}

// fakes

type sentCall struct {
	ref     string
	message string
}

// fakeSender records every send and reports scripted results. Scripted
// results are consumed in call order; with Concurrency 1 that order is
// the claim order.
type fakeSender struct {
	mu          sync.Mutex
	scripted    []notify.SendResult
	fixed       notify.SendResult
	calls       []sentCall
	block       time.Duration
	inFlight    int
	maxInFlight int
}

func (s *fakeSender) Send(_ context.Context, ref, message string) notify.SendResult {
	s.mu.Lock()
	var res notify.SendResult
	if len(s.scripted) > 0 {
		res = s.scripted[0]
		s.scripted = s.scripted[1:]
	} else {
		res = s.fixed
	}
	s.inFlight++
	if s.inFlight > s.maxInFlight {
		s.maxInFlight = s.inFlight
	}
	s.mu.Unlock()
	if s.block > 0 {
		time.Sleep(s.block)
	}
	s.mu.Lock()
	s.inFlight--
	s.calls = append(s.calls, sentCall{ref, message})
	s.mu.Unlock()
	return res
}

func (s *fakeSender) snapshot() []sentCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]sentCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// fakeQueue is a recording Queue; only the delivery methods are used by
// Deliver.
type fakeQueue struct {
	mu           sync.Mutex
	claims       []DeliveryClaim
	claimErr     error
	completeErr  error
	releaseErr   error
	gotLimit     int
	gotLease     time.Duration
	completed    []DeliveryClaim
	completedRes []notify.SendResult
	released     []DeliveryClaim
}

func (q *fakeQueue) Admit(context.Context, Admission) (AdmitResult, error) {
	return AdmitResult{}, nil
}

func (q *fakeQueue) ClaimDue(_ context.Context, _ time.Time, limit int, lease time.Duration) ([]DeliveryClaim, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.gotLimit, q.gotLease = limit, lease
	if q.claimErr != nil {
		return nil, q.claimErr
	}
	return q.claims, nil
}

func (q *fakeQueue) Complete(_ context.Context, c DeliveryClaim, r notify.SendResult, _ time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.completed = append(q.completed, c)
	q.completedRes = append(q.completedRes, r)
	return q.completeErr
}

func (q *fakeQueue) Release(_ context.Context, c DeliveryClaim) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.released = append(q.released, c)
	return q.releaseErr
}

func (q *fakeQueue) RecordDecision(context.Context, Decision, time.Time) (Decision, *Refusal, error) {
	return Decision{}, nil, nil
}

func (q *fakeQueue) PendingDecisions(context.Context, int) ([]Decision, error) { return nil, nil }

func (q *fakeQueue) RefuseDecision(context.Context, string, string, time.Time) error {
	return nil
}

func (q *fakeQueue) Inbox(context.Context, InboxFilter) ([]InboxItem, error) { return nil, nil }

func (q *fakeQueue) RecoverHeld(context.Context, time.Time, time.Duration) (int, error) {
	return 0, nil
}

func (q *fakeQueue) completedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.completed)
}

func (q *fakeQueue) releasedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.released)
}

func deliverClaim(id int64, attempt int) DeliveryClaim {
	return DeliveryClaim{
		DeliveryID: id,
		IngressID:  id,
		TaskID:     fmt.Sprintf("task-%d", id),
		ContextID:  fmt.Sprintf("ctx-%d", id),
		Project:    "org/repo",
		Role:       RoleOwner,
		Ref:        fmt.Sprintf("o%d/w1:p1", id),
		Attempt:    attempt,
		TaskState:  StateWorking,
	}
}

func TestDeliverSendsRenderedNoticeToEachRef(t *testing.T) {
	claims := []DeliveryClaim{deliverClaim(1, 1), deliverClaim(2, 1), deliverClaim(3, 1)}
	q := &fakeQueue{claims: claims}
	s := &fakeSender{fixed: notify.SendResult{Outcome: notify.OutcomeAccepted}}
	summary, err := Deliver(context.Background(), q, s, func() time.Time { return testBase }, DeliverOptions{Concurrency: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary != (DeliverSummary{Claimed: 3, Accepted: 3}) {
		t.Fatalf("unexpected summary %+v", summary)
	}
	calls := s.snapshot()
	if len(calls) != 3 {
		t.Fatalf("want 3 sends, got %d", len(calls))
	}
	for _, c := range claims {
		found := false
		for _, call := range calls {
			if call.ref == c.Ref && call.message == RenderNotice(c) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("claim %d not sent with its rendered notice to %s (calls: %+v)", c.DeliveryID, c.Ref, calls)
		}
	}
	if q.completedCount() != 3 {
		t.Fatalf("want 3 completes, got %d", q.completedCount())
	}
}

func TestDeliverSummaryPerOutcome(t *testing.T) {
	claims := []DeliveryClaim{
		deliverClaim(1, 1),
		deliverClaim(2, 1),
		deliverClaim(3, 1),
		deliverClaim(4, 1),
		deliverClaim(5, 6),
	}
	q := &fakeQueue{claims: claims}
	s := &fakeSender{scripted: []notify.SendResult{
		{Outcome: notify.OutcomeAccepted},
		{Outcome: notify.OutcomeUncertain},
		{Outcome: notify.OutcomeRejected},
		{Outcome: notify.OutcomeTransient},
		{Outcome: notify.OutcomeTransient},
	}}
	summary, err := Deliver(context.Background(), q, s, func() time.Time { return testBase }, DeliverOptions{Concurrency: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// claim 5 is at the last attempt: its transient result is exhausted.
	want := DeliverSummary{Claimed: 5, Accepted: 1, Uncertain: 1, Rejected: 1, Retrying: 1, Exhausted: 1}
	if summary != want {
		t.Fatalf("summary mismatch: got %+v, want %+v", summary, want)
	}
	if q.completedCount() != 5 {
		t.Fatalf("want 5 completes, got %d", q.completedCount())
	}
}

func TestDeliverConcurrencyBound(t *testing.T) {
	var claims []DeliveryClaim
	for i := int64(1); i <= 8; i++ {
		claims = append(claims, deliverClaim(i, 1))
	}
	q := &fakeQueue{claims: claims}
	s := &fakeSender{fixed: notify.SendResult{Outcome: notify.OutcomeAccepted}, block: 15 * time.Millisecond}
	summary, err := Deliver(context.Background(), q, s, func() time.Time { return testBase }, DeliverOptions{Concurrency: 2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary != (DeliverSummary{Claimed: 8, Accepted: 8}) {
		t.Fatalf("unexpected summary %+v", summary)
	}
	s.mu.Lock()
	max := s.maxInFlight
	s.mu.Unlock()
	if max > 2 {
		t.Fatalf("concurrency bound 2 exceeded: max in flight %d", max)
	}
	if max < 2 {
		t.Fatalf("want the pool to run 2 sends at once, max in flight %d", max)
	}
}

func TestDeliverDefaultOptions(t *testing.T) {
	var claims []DeliveryClaim
	for i := int64(1); i <= 8; i++ {
		claims = append(claims, deliverClaim(i, 1))
	}
	q := &fakeQueue{claims: claims}
	s := &fakeSender{fixed: notify.SendResult{Outcome: notify.OutcomeAccepted}, block: 2 * time.Millisecond}
	if _, err := Deliver(context.Background(), q, s, func() time.Time { return testBase }, DeliverOptions{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	q.mu.Lock()
	limit, lease := q.gotLimit, q.gotLease
	q.mu.Unlock()
	if limit != 32 {
		t.Fatalf("default limit = %d, want 32", limit)
	}
	if lease != notify.LeaseDuration {
		t.Fatalf("default lease = %v, want %v", lease, notify.LeaseDuration)
	}
	s.mu.Lock()
	max := s.maxInFlight
	s.mu.Unlock()
	if max > 4 {
		t.Fatalf("default concurrency 4 exceeded: max in flight %d", max)
	}
}

func TestDeliverContextDoneReleasesWithoutSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	claims := []DeliveryClaim{deliverClaim(1, 1), deliverClaim(2, 2)}
	q := &fakeQueue{claims: claims}
	s := &fakeSender{}
	summary, err := Deliver(ctx, q, s, func() time.Time { return testBase }, DeliverOptions{Concurrency: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if summary != (DeliverSummary{Claimed: 2, Deferred: 2}) {
		t.Fatalf("unexpected summary %+v", summary)
	}
	if got := s.snapshot(); len(got) != 0 {
		t.Fatalf("sender must not be called when the context is done, got %+v", got)
	}
	if q.completedCount() != 0 {
		t.Fatalf("no complete expected, got %d", q.completedCount())
	}
	if q.releasedCount() != 2 {
		t.Fatalf("want 2 releases, got %d", q.releasedCount())
	}
}

func TestDeliverReturnsFirstCompleteErrorAfterAllSends(t *testing.T) {
	errBoom := errors.New("boom")
	claims := []DeliveryClaim{deliverClaim(1, 1), deliverClaim(2, 1), deliverClaim(3, 1)}
	q := &fakeQueue{claims: claims, completeErr: errBoom}
	s := &fakeSender{fixed: notify.SendResult{Outcome: notify.OutcomeAccepted}}
	summary, err := Deliver(context.Background(), q, s, func() time.Time { return testBase }, DeliverOptions{Concurrency: 1})
	if !errors.Is(err, errBoom) {
		t.Fatalf("want the complete error, got %v", err)
	}
	// All sends still happen: the error is returned only after they end.
	if got := s.snapshot(); len(got) != 3 {
		t.Fatalf("want 3 sends before the error is returned, got %d", len(got))
	}
	if summary != (DeliverSummary{Claimed: 3, Accepted: 3}) {
		t.Fatalf("unexpected summary %+v", summary)
	}
}

func TestDeliverClaimDueErrorPropagates(t *testing.T) {
	errClaim := errors.New("claim failed")
	q := &fakeQueue{claims: []DeliveryClaim{deliverClaim(1, 1)}, claimErr: errClaim}
	s := &fakeSender{}
	summary, err := Deliver(context.Background(), q, s, func() time.Time { return testBase }, DeliverOptions{})
	if !errors.Is(err, errClaim) {
		t.Fatalf("want the claim error, got %v", err)
	}
	if summary != (DeliverSummary{}) {
		t.Fatalf("want zero summary, got %+v", summary)
	}
	if got := s.snapshot(); len(got) != 0 {
		t.Fatalf("no sends expected after a claim error, got %+v", got)
	}
}

var handlePattern = regexp.MustCompile(`^sec_[0-9a-f]{32}$`)

func TestMemorySecretsRoundTrip(t *testing.T) {
	ctx := context.Background()
	m := NewMemorySecrets()
	h, err := m.Put(ctx, "value-1")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if !handlePattern.MatchString(h) {
		t.Fatalf("handle %q does not match sec_<32 hex>", h)
	}
	got, err := m.Get(ctx, h)
	if err != nil || got != "value-1" {
		t.Fatalf("get = %q, %v; want value-1, nil", got, err)
	}
	if err := m.Delete(ctx, h); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := m.Delete(ctx, h); err != nil {
		t.Fatalf("delete of unknown handle must be nil, got %v", err)
	}
}

func TestMemorySecretsUnknownHandle(t *testing.T) {
	m := NewMemorySecrets()
	_, err := m.Get(context.Background(), "sec_"+strings.Repeat("0", 32))
	if !errors.Is(err, ErrSecretUnavailable) {
		t.Fatalf("want ErrSecretUnavailable, got %v", err)
	}
}

func TestMemorySecretsValueNeverInErrors(t *testing.T) {
	ctx := context.Background()
	value := "super-secret-token-xyz-42"
	m := NewMemorySecrets()
	h, err := m.Put(ctx, value)
	if err != nil {
		if strings.Contains(err.Error(), value) {
			t.Fatalf("put error carries the value: %v", err)
		}
		t.Fatalf("put: %v", err)
	}
	m.Delete(ctx, h)
	_, err = m.Get(ctx, h)
	if err == nil {
		t.Fatal("want error for deleted handle")
	}
	if strings.Contains(err.Error(), value) {
		t.Fatalf("get error carries the value: %v", err)
	}
	_, err = m.Get(ctx, "sec_"+strings.Repeat("0", 32))
	if err == nil {
		t.Fatal("want error for unknown handle")
	}
	if strings.Contains(err.Error(), value) {
		t.Fatalf("unknown-handle error carries the value: %v", err)
	}
}

func TestMemorySecretsConcurrent(t *testing.T) {
	ctx := context.Background()
	const workers = 16
	m := NewMemorySecrets()
	handles := make([]string, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h, err := m.Put(ctx, fmt.Sprintf("value-%d", i))
			if err != nil {
				t.Errorf("put: %v", err)
				return
			}
			handles[i] = h
		}(i)
	}
	wg.Wait()
	var wg2 sync.WaitGroup
	for i := 0; i < workers*4; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			h := handles[i%workers]
			v, err := m.Get(ctx, h)
			if err != nil && !errors.Is(err, ErrSecretUnavailable) {
				t.Errorf("get: %v", err)
				return
			}
			if err == nil && v == "" {
				t.Errorf("get returned an empty value without error")
			}
			if err := m.Delete(ctx, h); err != nil {
				t.Errorf("delete: %v", err)
			}
		}(i)
	}
	wg2.Wait()
}
