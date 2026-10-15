package a2acore

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/notify"
)

// noticeLimit bounds the rendered notice in bytes.
const noticeLimit = 2048

// renderedID renders v when it matches pattern, otherwise "?".
func renderedID(pattern *regexp.Regexp, v string) string {
	if pattern.MatchString(v) {
		return v
	}
	return "?"
}

// renderedRepo renders v when it is a valid org/repo, otherwise "?".
func renderedRepo(v string) string {
	if jobapi.ValidRepo(v) {
		return v
	}
	return "?"
}

// renderedJob renders v when it is a valid job id, otherwise "?".
func renderedJob(v string) string {
	if jobapi.ValidID(v) {
		return v
	}
	return "?"
}

// renderedRole renders v when it is a delivery role, otherwise "?".
func renderedRole(v string) string {
	if v == RoleOwner || v == RoleGuardian {
		return v
	}
	return "?"
}

// renderedState renders v when it is a neutral task state, otherwise "?".
func renderedState(v string) string {
	switch v {
	case StateSubmitted, StateWorking, StateInputRequired,
		StateCompleted, StateCanceled, StateFailed, StateRejected:
		return v
	}
	return "?"
}

// RenderNotice renders the identifiers-only one-line notice of one
// delivery claim. It is built only from the claim's allowlisted
// identifiers, each checked against its pattern; a value that fails its
// pattern renders as "?". It never carries message text, the subject or
// the reference, and it is at most 2048 bytes.
func RenderNotice(c DeliveryClaim) string {
	var b strings.Builder
	b.WriteString("[herdr-hermes] a2a task=")
	b.WriteString(renderedID(TaskIDPattern, c.TaskID))
	b.WriteString(" context=")
	b.WriteString(renderedID(TaskIDPattern, c.ContextID))
	b.WriteString(" class=a2a_request role=")
	b.WriteString(renderedRole(c.Role))
	b.WriteString(" project=")
	b.WriteString(renderedRepo(c.Project))
	if c.Job != "" {
		b.WriteString(" job=")
		b.WriteString(renderedJob(c.Job))
	}
	b.WriteString(" state=")
	b.WriteString(renderedState(c.TaskState))
	b.WriteString(" | details: nau a2a show --task ")
	b.WriteString(renderedID(TaskIDPattern, c.TaskID))
	out := b.String()
	if len(out) > noticeLimit {
		out = out[:noticeLimit]
	}
	return out
}

// DeliverOptions tunes one delivery pass over the queue. Zero fields
// use the defaults: limit 32, concurrency 4, lease notify.LeaseDuration.
type DeliverOptions struct {
	// Limit is the maximum number of claims per pass.
	Limit int
	// Concurrency is the maximum number of concurrent sends.
	Concurrency int
	// Lease is the in-flight lease of a claimed delivery.
	Lease time.Duration
}

// DeliverSummary counts the outcomes of one delivery pass.
type DeliverSummary struct {
	// Claimed is the number of claims taken from the queue.
	Claimed int
	// Accepted, Uncertain, Rejected and Exhausted are the final states
	// the attempts reached (uncertain is never resent automatically).
	Accepted  int
	Uncertain int
	Rejected  int
	Exhausted int
	// Retrying counts transient failures scheduled for a later attempt.
	Retrying int
	// Deferred counts claims released back to the queue because the
	// context was done before the send started.
	Deferred int
}

// Deliver runs one delivery pass over q: it claims the due deliveries
// and sends each one to its reference through sender with its rendered
// notice, with at most opts.Concurrency sends in flight. A claim whose
// context is already done before the send starts is released back to
// the queue without consuming the attempt and counted Deferred. Each
// finished attempt is recorded with NextDeliveryState, and the summary
// counts the resulting states. It returns the first Complete or Release
// error after all sends have ended. It never resends an uncertain
// delivery: such rows are not claimed by the queue.
func Deliver(ctx context.Context, q Queue, sender notify.Sender, now func() time.Time, opts DeliverOptions) (DeliverSummary, error) {
	if now == nil {
		now = time.Now
	}
	limit := opts.Limit
	if limit == 0 {
		limit = 32
	}
	lease := opts.Lease
	if lease == 0 {
		lease = notify.LeaseDuration
	}
	concurrency := opts.Concurrency
	if concurrency == 0 {
		concurrency = 4
	}

	claims, err := q.ClaimDue(ctx, now(), limit, lease)
	if err != nil {
		return DeliverSummary{}, err
	}
	summary := DeliverSummary{Claimed: len(claims)}

	claimsCh := make(chan DeliveryClaim, len(claims))
	for _, c := range claims {
		claimsCh <- c
	}
	close(claimsCh)

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	work := func() {
		defer wg.Done()
		for c := range claimsCh {
			var state string
			var err error
			if ctx.Err() != nil {
				// The pass was cancelled before this send could
				// start: give the claim back without consuming the
				// attempt.
				err = q.Release(context.Background(), c)
				mu.Lock()
				summary.Deferred++
				if first == nil {
					first = err
				}
				mu.Unlock()
				continue
			}
			res := sender.Send(ctx, c.Ref, RenderNotice(c))
			// The retry schedule counts from the end of the attempt.
			ts := now()
			state, _ = NextDeliveryState(c.Attempt, res, ts)
			err = q.Complete(context.Background(), c, res, ts)
			mu.Lock()
			switch state {
			case DeliveryAccepted:
				summary.Accepted++
			case DeliveryUncertain:
				summary.Uncertain++
			case DeliveryRejected:
				summary.Rejected++
			case DeliveryExhausted:
				summary.Exhausted++
			default:
				summary.Retrying++
			}
			if first == nil {
				first = err
			}
			mu.Unlock()
		}
	}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go work()
	}
	wg.Wait()
	return summary, first
}
