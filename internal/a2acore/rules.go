package a2acore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/djalmajr/nau/internal/notify"
)

// SubjectPolicy is the default Authorizer: a subject may perform the
// actions it lists, on the projects it lists (AnyProject for all). An
// action that is not project-scoped is checked with project "".
type SubjectPolicy struct{}

// Authorize implements Authorizer.
func (SubjectPolicy) Authorize(s Subject, a Action, project string) error {
	if !slices.Contains(s.Actions, a) {
		return fmt.Errorf("%w: action %s not granted", ErrForbidden, a)
	}
	if project == "" {
		return nil
	}
	if slices.Contains(s.Projects, AnyProject) || slices.Contains(s.Projects, project) {
		return nil
	}
	return fmt.Errorf("%w: project not granted", ErrForbidden)
}

// CheckFence applies the per (subject, session) fence to one message:
// an epoch below the stored one is stale, a new (higher) epoch must
// start at sequence 1, and within the same epoch the sequence must be
// exactly the next one; a lower or equal sequence is a replay (the same
// message id never reaches here: the store answers duplicates first).
// A refusal returns prev unchanged.
func CheckFence(prev FenceState, env Envelope) (FenceState, *Refusal) {
	if !prev.Found || env.Epoch > prev.Epoch {
		if env.Seq != 1 {
			return prev, &Refusal{Code: RefuseOutOfOrder, Detail: "a new session or epoch starts at seq 1"}
		}
		return FenceState{Found: true, Epoch: env.Epoch, LastSeq: 1}, nil
	}
	if env.Epoch < prev.Epoch {
		return prev, &Refusal{Code: RefuseStaleEpoch, Detail: "epoch below the session's current epoch"}
	}
	switch {
	case env.Seq == prev.LastSeq+1:
		return FenceState{Found: true, Epoch: prev.Epoch, LastSeq: env.Seq}, nil
	case env.Seq <= prev.LastSeq:
		return prev, &Refusal{Code: RefuseReplay, Detail: "seq already used by another message"}
	default:
		return prev, &Refusal{Code: RefuseOutOfOrder, Detail: "seq is not the next one"}
	}
}

// NextDeliveryState maps the outcome of one send attempt to the stored
// delivery state, with the notify semantics: accepted, uncertain and
// rejected are final (uncertain is never resent automatically); a
// transient failure, or an unknown outcome, retries on the notify
// schedule until notify.MaxAttempts attempts, then is exhausted. next is
// the time of the next attempt, zero unless the state is pending.
func NextDeliveryState(attempt int, r notify.SendResult, now time.Time) (state string, next time.Time) {
	switch r.Outcome {
	case notify.OutcomeAccepted:
		return DeliveryAccepted, time.Time{}
	case notify.OutcomeUncertain:
		return DeliveryUncertain, time.Time{}
	case notify.OutcomeRejected:
		return DeliveryRejected, time.Time{}
	}
	if attempt >= notify.MaxAttempts {
		return DeliveryExhausted, time.Time{}
	}
	i := attempt - 1
	if i < 0 {
		i = 0
	}
	if i >= len(notify.RetrySchedule) {
		i = len(notify.RetrySchedule) - 1
	}
	return DeliveryPending, now.Add(notify.RetrySchedule[i])
}

// ValidateDecision checks the shape of a decision before it is recorded
// or applied: a valid id, kind and task id, a non-blank UTF-8 text within
// MaxDecisionBytes (DefaultLimits when lim leaves it zero) and an actor.
func ValidateDecision(d Decision, lim Limits) *Refusal {
	maxBytes := lim.MaxDecisionBytes
	if maxBytes <= 0 {
		maxBytes = DefaultLimits.MaxDecisionBytes
	}
	bad := func(detail string) *Refusal { return &Refusal{Code: RefuseInvalidDecision, Detail: detail} }
	switch {
	case !DecisionIDPattern.MatchString(d.ID):
		return bad("invalid decision id")
	case d.Kind != DecisionAnswer && d.Kind != DecisionAsk && d.Kind != DecisionDecline:
		return bad("invalid decision kind")
	case !TaskIDPattern.MatchString(d.TaskID):
		return bad("invalid task id")
	case !utf8.ValidString(d.Text) || strings.TrimSpace(d.Text) == "":
		return bad("decision text must be non-blank UTF-8")
	case len(d.Text) > maxBytes:
		return bad("decision text too long")
	case d.Actor == "" || len(d.Actor) > 128:
		return bad("invalid actor")
	}
	return nil
}

// MemorySecrets is a process-lifetime SecretProvider: the values are
// kept in memory only, so they do not survive a restart (a Get after a
// restart is ErrSecretUnavailable). It is safe for concurrent use.
type MemorySecrets struct {
	mu     sync.Mutex
	values map[string]string
}

// NewMemorySecrets returns an empty in-memory secret provider.
func NewMemorySecrets() *MemorySecrets {
	return &MemorySecrets{values: make(map[string]string)}
}

// Put stores value under a new random handle "sec_<32 hex>".
func (m *MemorySecrets) Put(_ context.Context, value string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("a2a: secret handle: %w", err)
	}
	h := "sec_" + hex.EncodeToString(b[:])
	m.mu.Lock()
	m.values[h] = value
	m.mu.Unlock()
	return h, nil
}

// Get returns the value of handle, or ErrSecretUnavailable.
func (m *MemorySecrets) Get(_ context.Context, handle string) (string, error) {
	m.mu.Lock()
	v, ok := m.values[handle]
	m.mu.Unlock()
	if !ok {
		return "", ErrSecretUnavailable
	}
	return v, nil
}

// Delete forgets handle; an unknown handle is not an error.
func (m *MemorySecrets) Delete(_ context.Context, handle string) error {
	m.mu.Lock()
	delete(m.values, handle)
	m.mu.Unlock()
	return nil
}
