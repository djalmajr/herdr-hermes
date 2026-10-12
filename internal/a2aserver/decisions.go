package a2aserver

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

// pendingBatch is how many pending decisions one pass of
// ApplyPendingDecisions looks at.
const pendingBatch = 100

// ApplyPendingDecisions runs one pass of the decision dispatcher: each
// pending decision is validated (a refusal is committed through the
// store), handed to the parked execution of its task when one exists
// and was not handed a decision yet, skipped while its task's live
// execution is holding another decision (it stays pending, no refusal,
// and a later pass retries it), or applied out-of-band through the
// store, after which the final event is pushed to every push config of
// the task. It reports how many decisions were applied and how many
// were refused.
func (s *Server) ApplyPendingDecisions(ctx context.Context) (int, int, error) {
	decisions, err := s.cfg.Store.PendingDecisions(ctx, pendingBatch)
	if err != nil {
		return 0, 0, err
	}
	now := s.now()
	applied, refused := 0, 0
	for _, d := range decisions {
		if r := a2acore.ValidateDecision(d, s.cfg.Limits); r != nil {
			if err := s.cfg.Store.RefuseDecision(ctx, d.ID, r.Code, now); err != nil {
				return applied, refused, err
			}
			refused++
			continue
		}
		taken, live := s.executor.hand(d.TaskID, d)
		switch {
		case taken:
			applied++
		case live:
			// The live execution was already handed a decision: this
			// one stays pending (no refusal) and a later pass retries
			// it; it must not be applied out of band.
		default:
			events, err := s.decisionEventsFor(ctx, d)
			if err != nil {
				return applied, refused, err
			}
			_, r, err := s.cfg.Store.ApplyDecisionEvents(ctx, d.ID, events, now)
			if err != nil {
				return applied, refused, err
			}
			if r != nil {
				refused++
				continue
			}
			applied++
			s.pushFinalEvent(ctx, d.TaskID, events)
		}
	}
	return applied, refused, nil
}

// decisionEventsFor builds the out-of-band event sequence of one
// decision from the stored task. A missing task yields no events: the
// store refuses the decision as unknown_task before applying events.
func (s *Server) decisionEventsFor(ctx context.Context, d a2acore.Decision) ([]a2a.Event, error) {
	tv, err := s.cfg.Store.TaskView(ctx, d.TaskID)
	if err != nil {
		if errors.Is(err, a2a.ErrTaskNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return decisionEvents(taskRef{taskID: a2a.TaskID(d.TaskID), contextID: tv.ContextID}, d), nil
}

// taskRef is a minimal a2a.TaskInfoProvider over the stored identifiers
// of one task.
type taskRef struct {
	taskID    a2a.TaskID
	contextID string
}

// TaskInfo implements a2a.TaskInfoProvider.
func (t taskRef) TaskInfo() a2a.TaskInfo {
	return a2a.TaskInfo{TaskID: t.taskID, ContextID: t.contextID}
}

// pushFinalEvent sends the final event of one out-of-band decision to
// every push config of the task. Errors are counted and logged without
// values; a push failure never fails the pass.
func (s *Server) pushFinalEvent(ctx context.Context, taskID string, events []a2a.Event) {
	if len(events) == 0 {
		return
	}
	configs, err := s.cfg.Store.PushConfigs().List(ctx, a2a.TaskID(taskID))
	if err != nil {
		s.cfg.Logger.ErrorContext(ctx, "a2a: listing push configs failed",
			slog.String("task_id", taskID))
		return
	}
	failed := 0
	for _, c := range configs {
		if err := s.cfg.PushSender.SendPush(ctx, c, events[len(events)-1]); err != nil {
			failed++
		}
	}
	if failed > 0 {
		s.cfg.Logger.ErrorContext(ctx, "a2a: push notifications failed",
			slog.Int("failed", failed), slog.String("task_id", taskID))
	}
}

// RunDecisions loops ApplyPendingDecisions every interval until the
// context ends; a non-positive interval becomes one second.
func (s *Server) RunDecisions(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			if _, _, err := s.ApplyPendingDecisions(ctx); err != nil && ctx.Err() == nil {
				s.cfg.Logger.ErrorContext(ctx, "a2a: decision pass failed",
					slog.String("cause", err.Error()))
			}
		}
	}
}
