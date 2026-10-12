package a2acore

import (
	"encoding/json"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/djalmajr/nau/internal/jobapi"
)

// completeLimits returns lim with every zero field replaced by its
// DefaultLimits counterpart, so a caller may set only some of the bounds.
func completeLimits(lim Limits) Limits {
	if lim.MaxParts == 0 {
		lim.MaxParts = DefaultLimits.MaxParts
	}
	if lim.MaxTextBytes == 0 {
		lim.MaxTextBytes = DefaultLimits.MaxTextBytes
	}
	if lim.MaxMessagesPerTask == 0 {
		lim.MaxMessagesPerTask = DefaultLimits.MaxMessagesPerTask
	}
	if lim.MaxTasksPerContext == 0 {
		lim.MaxTasksPerContext = DefaultLimits.MaxTasksPerContext
	}
	if lim.MaxOpenTasks == 0 {
		lim.MaxOpenTasks = DefaultLimits.MaxOpenTasks
	}
	if lim.MaxDecisionBytes == 0 {
		lim.MaxDecisionBytes = DefaultLimits.MaxDecisionBytes
	}
	return lim
}

// correlationInt reports whether v is an integral JSON number in
// 1..MaxSeq in one of the accepted decoded types (float64, int, int64 or
// json.Number, as decoded from a JSON object).
func correlationInt(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		if n >= 1 && n <= float64(MaxSeq) && n == math.Trunc(n) {
			return int64(n), true
		}
	case int:
		if n >= 1 && n <= MaxSeq {
			return int64(n), true
		}
	case int64:
		if n >= 1 && n <= MaxSeq {
			return n, true
		}
	case json.Number:
		if i, err := n.Int64(); err == nil {
			if i >= 1 && i <= MaxSeq {
				return i, true
			}
			return 0, false
		}
		f, err := n.Float64()
		if err == nil && f >= 1 && f <= float64(MaxSeq) && f == math.Trunc(f) {
			return int64(f), true
		}
	}
	return 0, false
}

// ParseCorrelation reads the nau.* correlation keys from the incoming
// message metadata: a session matching SessionPattern, integral epoch
// and sequence numbers in 1..MaxSeq, and an optional project (valid by
// jobapi.ValidRepo) and job (valid by jobapi.ValidID). A missing or
// malformed key is an invalid_correlation refusal; the detail is fixed
// text and never quotes a value.
func ParseCorrelation(meta map[string]any) (project, job, session string, epoch, seq int64, r *Refusal) {
	bad := func(detail string) (string, string, string, int64, int64, *Refusal) {
		return "", "", "", 0, 0, &Refusal{Code: RefuseInvalidCorrelation, Detail: detail}
	}
	s, ok := meta[MetaSession].(string)
	if !ok || !SessionPattern.MatchString(s) {
		return bad("session must be a string matching the session pattern")
	}
	e, ok := correlationInt(meta[MetaEpoch])
	if !ok {
		return "", "", s, 0, 0, &Refusal{Code: RefuseInvalidCorrelation, Detail: "epoch must be an integral number in 1..MaxSeq"}
	}
	q, ok := correlationInt(meta[MetaSeq])
	if !ok {
		return "", "", s, e, 0, &Refusal{Code: RefuseInvalidCorrelation, Detail: "seq must be an integral number in 1..MaxSeq"}
	}
	p := ""
	if v, present := meta[MetaProject]; present {
		p, ok = v.(string)
		if !ok || !jobapi.ValidRepo(p) {
			return "", "", s, e, 0, &Refusal{Code: RefuseInvalidCorrelation, Detail: "project must be a valid org/repo"}
		}
	}
	j := ""
	if v, present := meta[MetaJob]; present {
		j, ok = v.(string)
		if !ok || !jobapi.ValidID(j) {
			return "", "", s, e, q, &Refusal{Code: RefuseInvalidCorrelation, Detail: "job must be a valid job id"}
		}
	}
	return p, j, s, e, q, nil
}

// Evaluate checks an incoming message as data against the subject, the
// optional existing task and the limits, and returns its correlation
// envelope or a refusal. It never runs, renders or forwards the message
// text: the envelope holds identifiers only. Zero limit fields use
// DefaultLimits for that field.
func Evaluate(s Subject, m MessageView, existing *TaskView, lim Limits) (Envelope, *Refusal) {
	lim = completeLimits(lim)
	bad := func(code, detail string) (Envelope, *Refusal) {
		return Envelope{}, &Refusal{Code: code, Detail: detail}
	}
	if m.Role != RoleUser {
		return bad(RefuseInvalidMessage, "only user messages are evaluated")
	}
	if len(m.Parts) < 1 || len(m.Parts) > lim.MaxParts {
		return bad(RefuseInvalidMessage, "message must have 1 to MaxParts parts")
	}
	textBytes := 0
	nonBlank := false
	for _, p := range m.Parts {
		if p.Kind != PartText {
			return bad(RefuseInvalidMessage, "only text parts are evaluated")
		}
		if !utf8.ValidString(p.Text) {
			return bad(RefuseInvalidMessage, "text must be valid UTF-8")
		}
		textBytes += len(p.Text)
		if strings.TrimSpace(p.Text) != "" {
			nonBlank = true
		}
	}
	if textBytes > lim.MaxTextBytes {
		return bad(RefuseInvalidMessage, "text exceeds MaxTextBytes")
	}
	if !nonBlank {
		return bad(RefuseInvalidMessage, "text must contain a non-whitespace byte")
	}
	if !MessageIDPattern.MatchString(m.ID) {
		return bad(RefuseInvalidMessage, "message id does not match the message pattern")
	}
	project, job, session, epoch, seq, r := ParseCorrelation(m.Metadata)
	if r != nil {
		return Envelope{}, r
	}
	var taskID, contextID, envProject, envJob string
	if existing == nil {
		if m.TaskID != "" {
			return bad(RefuseInvalidCorrelation, "task id cannot be set on a new task")
		}
		if project == "" {
			return bad(RefuseInvalidCorrelation, "project is required for a new task")
		}
		if m.ContextID != "" && !TaskIDPattern.MatchString(m.ContextID) {
			return bad(RefuseInvalidCorrelation, "context id does not match the task pattern")
		}
		contextID = m.ContextID
		envProject, envJob = project, job
	} else {
		if existing.Owner != s.ID {
			return bad(RefuseUnknownTask, "task does not belong to the caller")
		}
		if Terminal(existing.State) {
			return bad(RefuseTerminal, "task is terminal")
		}
		if project != "" && project != existing.Project {
			return bad(RefuseCorrelationMismatch, "project does not match the task")
		}
		if job != "" && job != existing.Job {
			return bad(RefuseCorrelationMismatch, "job does not match the task")
		}
		if m.ContextID != "" && m.ContextID != existing.ContextID {
			return bad(RefuseCorrelationMismatch, "context id does not match the task")
		}
		if existing.Messages >= lim.MaxMessagesPerTask {
			return bad(RefuseLoopLimit, "task history reached MaxMessagesPerTask")
		}
		taskID = existing.ID
		contextID = existing.ContextID
		envProject, envJob = existing.Project, existing.Job
	}
	if err := (SubjectPolicy{}).Authorize(s, ActionSend, envProject); err != nil {
		return bad(RefuseUnauthorizedProject, "project is not granted to the subject")
	}
	return Envelope{
		Subject:    s.ID,
		SubjectRef: s.Ref,
		MessageID:  m.ID,
		TaskID:     taskID,
		ContextID:  contextID,
		Project:    envProject,
		Job:        envJob,
		Session:    session,
		Epoch:      epoch,
		Seq:        seq,
		TextBytes:  textBytes,
	}, nil
}
