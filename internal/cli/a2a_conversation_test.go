package cli

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/djalmajr/nau/internal/outbox"
)

// TestA2AConversationFromSource proves the source-side conversation
// cycle over the CLI, the way docs/a2a.md describes it: send a new
// task, read it to working, reply --ask for the question back
// (input_required, the question as the status message), send the
// continuation, meet the refusal or the busy answer of a second
// continuation while the task works, and reply --answer to complete it
// with the answer artifact. Every command prints exactly one JSON line,
// and every wait is a bounded poll, never a sleep.
func TestA2AConversationFromSource(t *testing.T) {
	cfgDir := t.TempDir()
	a2aRegisterNotify(t, cfgDir)
	base, exitCh, cancel, out, sender := startA2AServe(t, cfgDir, "--listen", "127.0.0.1:0")
	t.Cleanup(func() {
		cancel()
		select {
		case <-exitCh:
		case <-time.After(15 * time.Second):
		}
	})
	_ = out
	_ = sender
	db := filepath.Join(outbox.StateDir(cfgDir), "registry.db")

	// (1) The source sends the question: a new task, working while the
	// owner has not decided.
	stdout, _, exit := runA2A(t, cfgDir, "send", "--server", base,
		"--session", "s-conv", "--epoch", "1", "--seq", "1",
		"--project", "org/example", "--message-id", "m-conv-1",
		"what is the status of the build?")
	if exit != 0 {
		t.Fatalf("send exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line := a2aOneJSONLine(t, "send", stdout)
	var sendBody struct {
		Status string    `json:"status"`
		Task   *a2a.Task `json:"task"`
	}
	if err := json.Unmarshal([]byte(line), &sendBody); err != nil || sendBody.Status != "ok" || sendBody.Task == nil || sendBody.Task.ID == "" {
		t.Fatalf("send body %q: %v, want status ok with a task", line, err)
	}
	taskID := string(sendBody.Task.ID)
	contextID := sendBody.Task.ContextID
	a2aPollGet(t, cfgDir, base, taskID, a2a.TaskStateWorking)

	// (2) The owner asks a question back: the task reaches
	// input_required and the status message carries the question.
	const askID = "dec-conv-ask"
	const question = "is the build blocked?"
	stdout, _, exit = runA2A(t, cfgDir, "reply", "--task", taskID, "--ask",
		"--decision-id", askID, "--db", db, question)
	if exit != 0 {
		t.Fatalf("reply ask exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "reply ask", stdout)
	if !strings.Contains(line, `"status":"recorded"`) {
		t.Fatalf("reply ask = %q, want status recorded", line)
	}
	asked := a2aPollGet(t, cfgDir, base, taskID, a2a.TaskStateInputRequired)
	if asked.Status.Message == nil || asked.Status.Message.Parts[0].Text() != question {
		t.Fatalf("status message = %+v, want the question back %q", asked.Status.Message, question)
	}

	// (3) The source answers with the continuation, the next sequence of
	// its session on the same task and context: the task works again.
	stdout, _, exit = runA2A(t, cfgDir, "send", "--server", base,
		"--task", taskID, "--context", contextID,
		"--session", "s-conv", "--epoch", "1", "--seq", "2",
		"--message-id", "m-conv-2",
		"no, the build is green")
	if exit != 0 {
		t.Fatalf("continuation exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "continuation", stdout)
	var contBody struct {
		Status string    `json:"status"`
		Task   *a2a.Task `json:"task"`
	}
	if err := json.Unmarshal([]byte(line), &contBody); err != nil || contBody.Status != "ok" || contBody.Task == nil || string(contBody.Task.ID) != taskID {
		t.Fatalf("continuation body %q: %v, want status ok with the task", line, err)
	}
	a2aPollGet(t, cfgDir, base, taskID, a2a.TaskStateWorking)

	// (4) A second continuation while the task works: either the SDK's
	// stable busy answer (the first execution is still alive) reported
	// unavailable with exit 4, or the not_awaiting_caller refusal in
	// the answered task — both are the expected answers, and the task
	// stays working either way.
	stdout, _, exit = runA2A(t, cfgDir, "send", "--server", base,
		"--task", taskID, "--context", contextID,
		"--session", "s-conv", "--epoch", "1", "--seq", "3",
		"--message-id", "m-conv-3",
		"second continuation")
	line = a2aOneJSONLine(t, "second continuation", stdout)
	switch {
	case exit == 4 && strings.Contains(line, `"status":"unavailable"`) && strings.Contains(line, "task busy"):
		// The first continuation's execution is alive: the stable busy
		// answer, nothing admitted.
	case exit == 0 && strings.Contains(line, `"status":"ok"`):
		var body struct {
			Status string    `json:"status"`
			Task   *a2a.Task `json:"task"`
		}
		if err := json.Unmarshal([]byte(line), &body); err != nil || body.Task == nil ||
			body.Task.Status.Message == nil || body.Task.Status.Message.Parts[0].Text() != "refused: not_awaiting_caller" {
			t.Fatalf("second continuation = %s, want the not_awaiting_caller refusal in the answered task", line)
		}
	default:
		t.Fatalf("second continuation exit = %d line = %s, want the busy unavailable answer or the not_awaiting_caller refusal", exit, line)
	}
	stdout, _, exit = runA2A(t, cfgDir, "get", "--server", base, "--task", taskID)
	if exit != 0 {
		t.Fatalf("get after the second continuation exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "get after the second continuation", stdout)
	var getBody struct {
		Status string    `json:"status"`
		Task   *a2a.Task `json:"task"`
	}
	if err := json.Unmarshal([]byte(line), &getBody); err != nil || getBody.Status != "ok" || getBody.Task == nil || getBody.Task.Status.State != a2a.TaskStateWorking {
		t.Fatalf("get after the second continuation = %q, want the task still working", line)
	}

	// (5) The owner answers: the task completes with exactly one
	// artifact, the answer-<decision id> with the decision text.
	const answerID = "dec-conv-answer"
	const answer = "the build is green, ship it"
	stdout, _, exit = runA2A(t, cfgDir, "reply", "--task", taskID, "--answer",
		"--decision-id", answerID, "--db", db, answer)
	if exit != 0 {
		t.Fatalf("reply answer exit = %d, want 0 (stdout %q)", exit, stdout)
	}
	line = a2aOneJSONLine(t, "reply answer", stdout)
	if !strings.Contains(line, `"status":"recorded"`) {
		t.Fatalf("reply answer = %q, want status recorded", line)
	}
	done := a2aPollGet(t, cfgDir, base, taskID, a2a.TaskStateCompleted)
	if len(done.Artifacts) != 1 {
		t.Fatalf("completed task artifacts = %+v, want exactly one", done.Artifacts)
	}
	artifact := done.Artifacts[0]
	if string(artifact.ID) != "answer-"+answerID {
		t.Fatalf("artifact id = %s, want answer-%s", artifact.ID, answerID)
	}
	if len(artifact.Parts) != 1 || artifact.Parts[0].Text() != answer {
		t.Fatalf("artifact = %+v, want the answer text %q", artifact, answer)
	}
}
