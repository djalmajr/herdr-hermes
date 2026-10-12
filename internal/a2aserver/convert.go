package a2aserver

import (
	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

// messageView builds the neutral view of one incoming SDK message: the
// kind and text of every part, the role and the metadata as data.
func messageView(m *a2a.Message) a2acore.MessageView {
	parts := make([]a2acore.PartView, 0, len(m.Parts))
	for _, p := range m.Parts {
		parts = append(parts, partView(p))
	}
	role := ""
	switch m.Role {
	case a2a.MessageRoleUser:
		role = a2acore.RoleUser
	case a2a.MessageRoleAgent:
		role = a2acore.RoleAgent
	}
	return a2acore.MessageView{
		ID:        m.ID,
		TaskID:    string(m.TaskID),
		ContextID: m.ContextID,
		Role:      role,
		Parts:     parts,
		Metadata:  m.Metadata,
	}
}

// partView classifies one SDK part: text stays text, a file URL is a
// file part, structured data a data part and anything else (raw bytes,
// an empty or null content) is other.
func partView(p *a2a.Part) a2acore.PartView {
	switch c := p.Content.(type) {
	case a2a.Text:
		return a2acore.PartView{Kind: a2acore.PartText, Text: string(c)}
	case a2a.URL:
		if c != "" {
			return a2acore.PartView{Kind: a2acore.PartFile}
		}
	case a2a.Data:
		if c.Value != nil {
			return a2acore.PartView{Kind: a2acore.PartData}
		}
	}
	return a2acore.PartView{Kind: a2acore.PartOther}
}

// decisionEvents builds the event sequence of one recorded decision: an
// answer completes the task with the text as the artifact
// "answer-<decision id>", a question puts it back to the caller and a
// decline rejects it. The final status event carries the decision id in
// its metadata so the store marks the decision applied in the same
// transaction as the task update.
func decisionEvents(p a2a.TaskInfoProvider, d a2acore.Decision) []a2a.Event {
	info := p.TaskInfo()
	var events []a2a.Event
	switch d.Kind {
	case a2acore.DecisionAnswer:
		events = append(events,
			&a2a.TaskArtifactUpdateEvent{
				TaskID:    info.TaskID,
				ContextID: info.ContextID,
				Artifact: &a2a.Artifact{
					ID:    a2a.ArtifactID("answer-" + d.ID),
					Name:  "answer",
					Parts: a2a.ContentParts{a2a.NewTextPart(d.Text)},
				},
				LastChunk: true,
			},
			finalStatusEvent(p, a2a.TaskStateCompleted, nil, d.ID))
	case a2acore.DecisionAsk:
		events = append(events, finalStatusEvent(p, a2a.TaskStateInputRequired,
			a2a.NewMessageForTask(a2a.MessageRoleAgent, p, a2a.NewTextPart(d.Text)), d.ID))
	case a2acore.DecisionDecline:
		events = append(events, finalStatusEvent(p, a2a.TaskStateRejected,
			a2a.NewMessageForTask(a2a.MessageRoleAgent, p, a2a.NewTextPart(d.Text)), d.ID))
	}
	return events
}

// finalStatusEvent is the decision's completed, input-required or
// rejected status update with the decision id in its metadata.
func finalStatusEvent(p a2a.TaskInfoProvider, state a2a.TaskState, msg *a2a.Message, decisionID string) *a2a.TaskStatusUpdateEvent {
	ev := a2a.NewStatusUpdateEvent(p, state, msg)
	ev.SetMeta(a2acore.MetaDecision, decisionID)
	return ev
}

// refusalStatusEvent is the refusal of one continuation message: one
// input-required status update whose agent message carries the fixed
// refusal text and the code in its metadata.
func refusalStatusEvent(p a2a.TaskInfoProvider, r *a2acore.Refusal) *a2a.TaskStatusUpdateEvent {
	msg := a2a.NewMessageForTask(a2a.MessageRoleAgent, p, a2a.NewTextPart("refused: "+r.Code))
	msg.SetMeta(a2acore.MetaRefusal, r.Code)
	return a2a.NewStatusUpdateEvent(p, a2a.TaskStateInputRequired, msg)
}

// refusalWorkingEvent is the refusal of one continuation of a working
// task: one working status update whose agent message carries the fixed
// refusal text and the code in the event metadata. The task keeps its
// working state and waits for the owner or guardian, and the code is
// merged into the task metadata by the SDK.
func refusalWorkingEvent(p a2a.TaskInfoProvider, r *a2acore.Refusal) *a2a.TaskStatusUpdateEvent {
	ev := a2a.NewStatusUpdateEvent(p, a2a.TaskStateWorking,
		a2a.NewMessageForTask(a2a.MessageRoleAgent, p, a2a.NewTextPart("refused: "+r.Code)))
	ev.SetMeta(a2acore.MetaRefusal, r.Code)
	return ev
}
