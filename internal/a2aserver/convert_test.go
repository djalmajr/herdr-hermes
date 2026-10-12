package a2aserver

import (
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/a2acore"
)

func TestMessageView(t *testing.T) {
	msg := &a2a.Message{
		ID:        "m-1",
		TaskID:    "t-1",
		ContextID: "c-1",
		Role:      a2a.MessageRoleUser,
		Parts: a2a.ContentParts{
			a2a.NewTextPart("hello"),
			a2a.NewDataPart(map[string]any{"k": "v"}),
			a2a.NewFileURLPart("http://127.0.0.1:8000/f.txt", "text/plain"),
			a2a.NewRawPart([]byte{0x01, 0x02}),
			a2a.NewTextPart(""),
		},
		Metadata: map[string]any{a2acore.MetaSession: "s-1"},
	}
	v := messageView(msg)
	if v.ID != "m-1" || v.TaskID != "t-1" || v.ContextID != "c-1" {
		t.Errorf("view ids = %+v, want m-1/t-1/c-1", v)
	}
	if v.Role != a2acore.RoleUser {
		t.Errorf("role = %q, want %q", v.Role, a2acore.RoleUser)
	}
	want := []a2acore.PartView{
		{Kind: a2acore.PartText, Text: "hello"},
		{Kind: a2acore.PartData},
		{Kind: a2acore.PartFile},
		{Kind: a2acore.PartOther},
		{Kind: a2acore.PartText, Text: ""},
	}
	if len(v.Parts) != len(want) {
		t.Fatalf("parts = %d, want %d", len(v.Parts), len(want))
	}
	for i := range want {
		if v.Parts[i] != want[i] {
			t.Errorf("part[%d] = %+v, want %+v", i, v.Parts[i], want[i])
		}
	}
	if v.Metadata[a2acore.MetaSession] != "s-1" {
		t.Errorf("metadata = %v, want session s-1", v.Metadata)
	}

	agent := messageView(&a2a.Message{Role: a2a.MessageRoleAgent})
	if agent.Role != a2acore.RoleAgent {
		t.Errorf("agent role = %q, want %q", agent.Role, a2acore.RoleAgent)
	}
	if messageView(&a2a.Message{}).Role != "" {
		t.Errorf("unknown role = %q, want empty", messageView(&a2a.Message{}).Role)
	}
}

func TestDecisionEvents(t *testing.T) {
	const decisionID = "d-1"
	provider := taskRef{taskID: "t-1", contextID: "c-1"}

	answer := decisionEvents(provider, a2acore.Decision{ID: decisionID, Kind: a2acore.DecisionAnswer, Text: "the answer"})
	if len(answer) != 2 {
		t.Fatalf("answer events = %d, want 2", len(answer))
	}
	art, ok := answer[0].(*a2a.TaskArtifactUpdateEvent)
	if !ok {
		t.Fatalf("first answer event = %T, want artifact update", answer[0])
	}
	if art.Artifact.ID != "answer-"+decisionID || art.Artifact.Name != "answer" || !art.LastChunk {
		t.Errorf("artifact = %+v, want id answer-%s, name answer, last chunk", art.Artifact, decisionID)
	}
	if got := art.Artifact.Parts[0].Text(); got != "the answer" {
		t.Errorf("artifact text = %q, want the answer", got)
	}
	if art.TaskID != "t-1" || art.ContextID != "c-1" {
		t.Errorf("artifact task = %s/%s, want t-1/c-1", art.TaskID, art.ContextID)
	}
	if ev := finalEvent(t, answer[1]); ev.Status.State != a2a.TaskStateCompleted {
		t.Errorf("final state = %s, want completed", ev.Status.State)
	}
	if ev := finalEvent(t, answer[1]); ev.Metadata[a2acore.MetaDecision] != decisionID {
		t.Errorf("final metadata = %v, want decision %s", ev.Metadata, decisionID)
	}

	ask := decisionEvents(provider, a2acore.Decision{ID: decisionID, Kind: a2acore.DecisionAsk, Text: "question one"})
	if len(ask) != 1 {
		t.Fatalf("ask events = %d, want 1", len(ask))
	}
	if ev := finalEvent(t, ask[0]); ev.Status.State != a2a.TaskStateInputRequired {
		t.Errorf("ask state = %s, want input required", ev.Status.State)
	}
	if ev := finalEvent(t, ask[0]); ev.Status.Message == nil || ev.Status.Message.Parts[0].Text() != "question one" {
		t.Errorf("ask message = %+v, want question one", ev.Status.Message)
	}
	if ev := finalEvent(t, ask[0]); ev.Metadata[a2acore.MetaDecision] != decisionID {
		t.Errorf("ask metadata = %v, want decision %s", ev.Metadata, decisionID)
	}

	decline := decisionEvents(provider, a2acore.Decision{ID: decisionID, Kind: a2acore.DecisionDecline, Text: "no"})
	if len(decline) != 1 {
		t.Fatalf("decline events = %d, want 1", len(decline))
	}
	if ev := finalEvent(t, decline[0]); ev.Status.State != a2a.TaskStateRejected {
		t.Errorf("decline state = %s, want rejected", ev.Status.State)
	}
	if ev := finalEvent(t, decline[0]); ev.Status.Message == nil || ev.Status.Message.Parts[0].Text() != "no" {
		t.Errorf("decline message = %+v, want no", ev.Status.Message)
	}
}

// finalEvent asserts that one event is a status update and returns it.
func finalEvent(t *testing.T, ev a2a.Event) *a2a.TaskStatusUpdateEvent {
	t.Helper()
	evv, ok := ev.(*a2a.TaskStatusUpdateEvent)
	if !ok {
		t.Fatalf("event = %T, want status update", ev)
	}
	return evv
}
