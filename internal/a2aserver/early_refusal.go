package a2aserver

import (
	"context"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
)

// preEvaluate runs the read-only pre-evaluation of one continuation, in
// Before, before any SDK execution: the same rules in the same order as the
// executor's path — Evaluate with the store's TaskView and the server's
// limits, the awaiting-caller gate, then the fence (the same query as Admit,
// without writing). It returns a refusal when the continuation will be
// refused, so the interceptor answers it before the SDK runs the task
// (nothing is written and no push goes out); nil when no refusal is
// predicted, so the request proceeds to the SDK unchanged (the executor keeps
// its checks as the defense for the race between this read and the
// admission). A terminal task is not pre-evaluated (the SDK refuses it with
// unsupported operation, unchanged). A store error in any read is the
// fixed-text server error.
func (i *authInterceptor) preEvaluate(ctx context.Context, subject a2acore.Subject, stored *taskstore.StoredTask, msg *a2a.Message) (*a2acore.Refusal, error) {
	// A terminal task keeps the SDK's own answer (unsupported operation);
	// it is not pre-evaluated, so no refusal is predicted.
	if stored.Task.Status.State.Terminal() {
		return nil, nil
	}
	// The neutral view of the task, the same read the executor's path uses
	// for Evaluate and the gate.
	tv, err := i.tasks.store.TaskView(ctx, string(stored.Task.ID))
	if err != nil {
		return nil, a2a.NewError(a2a.ErrServerError, "a2a: task lookup failed")
	}
	// 1. Evaluate: the same rules and limits as the executor's path.
	env, refusal := a2acore.Evaluate(subject, messageView(msg), tv, i.limits)
	if refusal != nil {
		return refusal, nil
	}
	// 2. The awaiting-caller gate, the same as Admit's: a continuation is
	// admitted only while the task awaits the caller.
	if tv.State != a2acore.StateInputRequired {
		return &a2acore.Refusal{Code: a2acore.RefuseNotAwaitingCaller, Detail: "the task is not awaiting the caller"}, nil
	}
	// 3. The fence, read-only: the same query as Admit's, without writing.
	prev, err := i.tasks.store.Fence(ctx, env.Subject, env.Session)
	if err != nil {
		return nil, a2a.NewError(a2a.ErrServerError, "a2a: task lookup failed")
	}
	if _, refusal := a2acore.CheckFence(prev, env); refusal != nil {
		return refusal, nil
	}
	// No refusal is predicted: the request proceeds to the SDK unchanged.
	return nil, nil
}

// earlyRefusalResponse is the answer of one refused continuation: the stored
// task (already a copy of the durable task) with its status message set to
// the fixed refusal agent message (the text "refused: <code>" and the code
// in its nau.refusal metadata) and the code in its metadata, in a copied map
// (the stored task's map is not touched). The status state stays as the
// stored task has it. Both formats the clients read are covered: the task
// metadata (the working-task form) and the status message metadata (the
// input_required form). The refused message's text never reaches the answer,
// the logs or the metadata; only the fixed text and code do.
func earlyRefusalResponse(stored *taskstore.StoredTask, code string) *a2a.Task {
	task := stored.Task
	meta := make(map[string]any, len(task.Metadata)+1)
	for k, v := range task.Metadata {
		meta[k] = v
	}
	meta[a2acore.MetaRefusal] = code
	task.Metadata = meta
	msg := a2a.NewMessageForTask(a2a.MessageRoleAgent, task, a2a.NewTextPart("refused: "+code))
	msg.SetMeta(a2acore.MetaRefusal, code)
	task.Status.Message = msg
	return task
}
