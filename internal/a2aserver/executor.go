package a2aserver

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// acceptedMessage is the fixed agent status message of one admitted
// message: the task waits for the owner or guardian.
const acceptedMessage = "accepted for owner evaluation"

// parkedEntry is one execution parked awaiting the decision of its
// task. The channel holds one decision at most and the handed flag keeps
// the dispatcher from handing a second one before the first is applied.
type parkedEntry struct {
	decisions chan a2acore.Decision
	handed    bool
}

// executor is the SDK agent executor: it evaluates the incoming message
// as data, routes it, admits it durably before the first event, yields
// the task events and then parks until the recorded decision arrives (or
// the execution context ends). It implements a2asrv.AgentExecutor and
// a2asrv.AgentExecutionCleaner.
type executor struct {
	store   *a2astore.Store
	routes  a2acore.RouteSource
	limits  a2acore.Limits
	now     func() time.Time
	parkMu  sync.Mutex
	parkeds map[string]*parkedEntry
}

// newExecutor builds the executor of one server.
func newExecutor(cfg Config) *executor {
	return &executor{
		store:   cfg.Store,
		routes:  cfg.Routes,
		limits:  cfg.Limits,
		now:     cfg.Now,
		parkeds: make(map[string]*parkedEntry),
	}
}

// liveExecutions reports the number of parked executions.
func (e *executor) liveExecutions() int {
	e.parkMu.Lock()
	defer e.parkMu.Unlock()
	return len(e.parkeds)
}

// register parks a new entry for one task (the SDK runs at most one live
// execution per task).
func (e *executor) register(taskID string) *parkedEntry {
	e.parkMu.Lock()
	defer e.parkMu.Unlock()
	p := &parkedEntry{decisions: make(chan a2acore.Decision, 1)}
	e.parkeds[taskID] = p
	return p
}

// unregister removes the parked entry of one finished execution.
func (e *executor) unregister(taskID string) {
	e.parkMu.Lock()
	defer e.parkMu.Unlock()
	delete(e.parkeds, taskID)
}

// hand hands one decision to the parked execution of the task with a
// non-blocking send. taken reports that the decision was taken by the
// live execution, live reports that a live execution exists for the
// task (taken or already handed); a decision for a live task must
// never be applied out of band.
func (e *executor) hand(taskID string, d a2acore.Decision) (taken bool, live bool) {
	e.parkMu.Lock()
	defer e.parkMu.Unlock()
	p, ok := e.parkeds[taskID]
	if !ok {
		return false, false
	}
	if p.handed {
		return false, true
	}
	select {
	case p.decisions <- d:
		p.handed = true
		return true, true
	default:
		return false, true
	}
}

// subjectOf reads the authenticated subject the interceptor stamped on
// the executor context.
func subjectOf(execCtx *a2asrv.ExecutorContext) (a2acore.Subject, error) {
	if execCtx.User == nil {
		return a2acore.Subject{}, fmt.Errorf("%w: no authenticated user", a2a.ErrUnauthenticated)
	}
	s, ok := execCtx.User.Attributes[subjectAttr].(a2acore.Subject)
	if !ok {
		return a2acore.Subject{}, fmt.Errorf("%w: no subject attribute", a2a.ErrUnauthenticated)
	}
	return s, nil
}

// newTaskRefusalError is the client error of a refused new task:
// unauthorized_project maps to a2a.ErrUnauthorized and every other code
// to a2a.ErrInvalidParams, with the fixed text "refused: <code>".
func newTaskRefusalError(r *a2acore.Refusal) error {
	if r.Code == a2acore.RefuseUnauthorizedProject {
		return a2a.NewError(a2a.ErrUnauthorized, "refused: "+r.Code)
	}
	return a2a.NewError(a2a.ErrInvalidParams, "refused: "+r.Code)
}

// Execute implements a2asrv.AgentExecutor. One live message evaluates
// to a durable admission before the first event is yielded, and the
// parked entry exists at that point, so the dispatcher never sees an
// admitted task without its live entry. The execution parks until the
// recorded decision is handed to it; the decision is applied through
// the same event pipe (so the SDK, the store and the push notification
// observe it identically), and a cancelation ends the execution while
// the decision stays pending for the dispatcher.
func (e *executor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		msg := execCtx.Message
		if msg == nil {
			return
		}
		subject, err := subjectOf(execCtx)
		if err != nil {
			yield(nil, err)
			return
		}
		view := messageView(msg)
		var existing *a2acore.TaskView
		if msg.TaskID != "" {
			tv, err := e.store.TaskView(ctx, string(msg.TaskID))
			if err != nil {
				yield(nil, a2a.NewError(a2a.ErrInternalError, "a2a: task view unavailable"))
				return
			}
			existing = tv
		}
		env, refusal := a2acore.Evaluate(subject, view, existing, e.limits)
		if refusal != nil {
			e.yieldRefusal(ctx, yield, execCtx, existing, refusal)
			return
		}
		deliveries, err := a2acore.Route(e.routes, env.Project, env.SubjectRef)
		if err != nil {
			yield(nil, a2a.NewError(a2a.ErrInternalError, "a2a: route unavailable"))
			return
		}
		contextID := env.ContextID
		if contextID == "" {
			contextID = execCtx.ContextID
		}
		result, err := e.store.Admit(ctx, a2acore.Admission{
			Envelope:   env,
			TaskID:     string(execCtx.TaskID),
			ContextID:  contextID,
			NewTask:    existing == nil,
			Deliveries: deliveries,
			ReceivedAt: e.now(),
		})
		if err != nil {
			yield(nil, a2a.NewError(a2a.ErrInternalError, "a2a: admission unavailable"))
			return
		}
		if result.Refusal != nil {
			e.yieldRefusal(ctx, yield, execCtx, existing, result.Refusal)
			return
		}
		if result.Duplicate {
			if existing == nil {
				// The first message of a task was already admitted
				// elsewhere: answer it with the message that carries
				// the task id.
				m := a2a.NewMessageForTask(a2a.MessageRoleAgent, execCtx,
					a2a.NewTextPart("duplicate of task "+result.DuplicateTaskID))
				m.SetMeta(a2acore.MetaDuplicateOf, result.DuplicateTaskID)
				yield(m, nil)
				return
			}
			// A continuation whose message id was already admitted: the
			// admission stands (no second ingress row, no second
			// delivery) and the stored task — the version the SDK loaded
			// for this continuation, the same one its update is checked
			// against — is the first event, so the resend never changes
			// the task state. In a state that is final for the SDK
			// (input_required or terminal, the rule of
			// SDK/internal/taskupdate/final.go; a terminal task is already
			// refused before the executor) the execution ends with the
			// task event; in working it parks like an admitted
			// continuation, so the owner's decision reaches it through
			// the live entry and ends the execution with the decision's
			// final event. A yield failure ends the execution the same
			// way the admitted path does: the entry is cleaned by
			// Cleanup when the execution ends.
			if state := execCtx.StoredTask.Status.State; !state.Terminal() && state != a2a.TaskStateInputRequired {
				p := e.register(string(execCtx.TaskID))
				if !yield(execCtx.StoredTask, nil) {
					return
				}
				e.park(ctx, p, yield, execCtx)
				return
			}
			yield(execCtx.StoredTask, nil)
			return
		}
		// Admitted: the parked entry exists before the first event is
		// yielded, so a decision recorded right after the request
		// returns is never applied out of band.
		p := e.register(string(execCtx.TaskID))
		if existing == nil {
			task := a2a.NewSubmittedTask(execCtx, msg)
			meta := map[string]any{a2acore.MetaProject: env.Project}
			if env.Job != "" {
				meta[a2acore.MetaJob] = env.Job
			}
			task.Metadata = meta
			if !yield(task, nil) {
				return
			}
		}
		if !yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateWorking,
			a2a.NewMessageForTask(a2a.MessageRoleAgent, execCtx, a2a.NewTextPart(acceptedMessage))), nil) {
			return
		}
		// Park until the recorded decision arrives or the execution
		// context ends. The SDK runs executions in a detached context;
		// a cancelation ends the execution while the decision stays
		// pending for the dispatcher.
		e.park(ctx, p, yield, execCtx)
	}
}

// park waits for the recorded decision of the task or the end of the
// execution context and yields the decision events through the same
// event pipe.
func (e *executor) park(ctx context.Context, p *parkedEntry, yield func(a2a.Event, error) bool, execCtx *a2asrv.ExecutorContext) {
	select {
	case d := <-p.decisions:
		for _, ev := range decisionEvents(execCtx, d) {
			if !yield(ev, nil) {
				return
			}
		}
	case <-ctx.Done():
	}
}

// yieldRefusal yields the refusal of one message: a refused new task is
// a client error (no task is created); a refused continuation of a
// working task keeps the task working and parks, because after a
// restart the execution that parked it no longer exists; a refused
// continuation of any other task is one input-required status update
// that keeps the task awaiting the caller. Message text never reaches
// the yielded events; only the fixed refusal text and code do.
func (e *executor) yieldRefusal(ctx context.Context, yield func(a2a.Event, error) bool, execCtx *a2asrv.ExecutorContext, existing *a2acore.TaskView, r *a2acore.Refusal) {
	if existing == nil {
		yield(nil, newTaskRefusalError(r))
		return
	}
	if existing.State == a2acore.StateWorking {
		if !yield(refusalWorkingEvent(execCtx, r), nil) {
			return
		}
		p := e.register(string(execCtx.TaskID))
		e.park(ctx, p, yield, execCtx)
		return
	}
	yield(refusalStatusEvent(execCtx, r), nil)
}

// Cancel implements a2asrv.AgentExecutor.
func (e *executor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

// Cleanup implements a2asrv.AgentExecutionCleaner. For a message
// execution (not a cancelation) the SDK calls it after the final event
// was processed and stored, and the parked entry of the task goes away
// with it.
func (e *executor) Cleanup(ctx context.Context, execCtx *a2asrv.ExecutorContext, _ a2a.SendMessageResult, _ error) {
	if execCtx.Message != nil {
		e.unregister(string(execCtx.TaskID))
		if execCtx.StoredTask == nil {
			// A new task: the SDK saved its push config before the
			// admission, so one whose task never committed is discarded
			// here. The error is ignored: the startup recovery pass
			// (RecoverHeld) removes what a failed discard leaves.
			_, _ = e.store.PushConfigs().DiscardUncommitted(ctx, execCtx.TaskID)
		}
	}
}
