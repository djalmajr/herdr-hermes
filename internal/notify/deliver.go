package notify

import (
	"context"
	"errors"
	"sync"
	"time"
)

// RenderFunc renders the message of one delivery.
type RenderFunc func(n *Notification, d Delivery) string

// DeliverOptions bounds one Deliver run: Limit is passed to ClaimDue (0
// means the default) and Concurrency caps the parallel sends (0 means 4).
type DeliverOptions struct{ Limit, Concurrency int }

// DeliverSummary counts the outcomes of one Deliver run: Retrying is a
// transient outcome that went back to pending, Exhausted a transient
// outcome that became exhausted, and Deferred a claim released unsent
// because the run's context was done before its send started (no attempt
// consumed).
type DeliverSummary struct{ Claimed, Accepted, Uncertain, Rejected, Retrying, Exhausted, Deferred int }

// defaultConcurrency caps the parallel sends when the options do not set
// one.
const defaultConcurrency = 4

// Deliver claims the due deliveries and sends them with at most
// Concurrency goroutines. The claim lease covers the run's remaining
// context budget plus the crash margin (LeaseDuration when the context
// has no deadline), so a live claim never becomes due while the run can
// still send it. Each send sits between two locked steps (the claim and
// its Complete), so the store is never locked while a sender
// runs. When the context is already done before a send starts, the
// sender is not called and the attempt is completed as a transient
// "deadline". It returns the first Complete error, after all sends have
// finished. A disabled store returns a zero summary without claiming.
func Deliver(ctx context.Context, s *Store, sender Sender, render RenderFunc, opts DeliverOptions) (DeliverSummary, error) {
	if !s.Enabled() {
		return DeliverSummary{}, nil
	}
	if sender == nil || render == nil {
		return DeliverSummary{}, errors.New("notify: Deliver requires a Sender and a RenderFunc")
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	claims, err := s.ClaimDueLease(opts.Limit, leaseForRun(ctx, s.now()))
	if err != nil {
		return DeliverSummary{}, err
	}
	summary := DeliverSummary{Claimed: len(claims)}
	if len(claims) == 0 {
		return summary, nil
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, c := range claims {
		wg.Add(1)
		go func(c Claim) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if ctx.Err() != nil {
				// Never sent: release the claim without consuming the
				// attempt, so a backlog larger than one bounded run
				// never exhausts deliveries that were not tried.
				err := s.Release(c)
				mu.Lock()
				if err != nil && firstErr == nil {
					firstErr = err
				}
				summary.Deferred++
				mu.Unlock()
				return
			}
			r := sendOne(ctx, c, sender, render)
			if err := s.Complete(c, r); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
			mu.Lock()
			switch r.Outcome {
			case OutcomeAccepted:
				summary.Accepted++
			case OutcomeUncertain:
				summary.Uncertain++
			case OutcomeRejected:
				summary.Rejected++
			default: // transient: retried or exhausted by Complete
				if c.Attempt >= MaxAttempts {
					summary.Exhausted++
				} else {
					summary.Retrying++
				}
			}
			mu.Unlock()
		}(c)
	}
	wg.Wait()
	return summary, firstErr
}

// leaseForRun derives the claim lease of one Deliver run from its
// context: when the context has a deadline, the lease covers the time
// until the deadline (measured from the store clock) plus the crash
// margin (LeaseDuration), so a live claim never becomes due while the
// run can still send it; without a deadline — or with one that already
// passed — the lease is LeaseDuration.
func leaseForRun(ctx context.Context, now time.Time) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		rem := d.Sub(now)
		if rem < 0 {
			rem = 0
		}
		return rem + LeaseDuration
	}
	return LeaseDuration
}

// sendOne sends one claim and reports its result: when the context is
// already done before the send starts, the sender is not called and the
// result is a transient deadline.
func sendOne(ctx context.Context, c Claim, sender Sender, render RenderFunc) SendResult {
	if err := ctx.Err(); err != nil {
		return SendResult{Outcome: OutcomeTransient, Exit: -1, Category: "deadline"}
	}
	return sender.Send(ctx, c.Ref, render(&c.Notification, c.Notification.Deliveries[c.Index]))
}
