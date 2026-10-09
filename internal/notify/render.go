package notify

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
)

// notificationIDPattern is the render-side check of a notification id
// (the projection always produces one; a corrupt stored one renders ?).
var notificationIDPattern = regexp.MustCompile(`^n[0-9a-f]{20}$`)

// renderable classes, roles, escalations and the contract event tipos.
var (
	renderClasses     = []string{ClassQuestion, ClassBlocked, ClassFailed, ClassTerminal, ClassTimeoutWarning, ClassReviewVerdict, ClassPROpened, ClassDecision, ClassAgentBlocked, ClassAgentDone, ClassCrossProject, ClassStuck, ClassWorkspaceOpened, ClassWorkspaceClosed}
	renderRoles       = []string{RoleOwner, RoleOrchestrator, RoleCoordinator}
	renderEscalations = []string{EscBlocked, EscFailed, EscMissingOwner, EscCrossProject, EscStuck}
)

func inList(v string, list []string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// sanitize returns "" for an empty value and "?" for a value that fails
// its check; every other value passes through unchanged.
func sanitize(v string, ok func(string) bool) string {
	if v == "" {
		return ""
	}
	if !ok(v) {
		return "?"
	}
	return v
}

// sanitizeList joins each item with "," after sanitizing it; an empty list
// gives "".
func sanitizeList(items []string, ok func(string) bool) string {
	if len(items) == 0 {
		return ""
	}
	out := make([]string, len(items))
	for i, v := range items {
		out[i] = sanitize(v, ok)
	}
	return strings.Join(out, ",")
}

// Render returns the one-line peer message for one delivery of n. It is a
// single line with no newline; only allowlisted identifiers are rendered,
// every value is validated against its pattern and a value that fails is
// rendered as "?". A token whose value is empty or zero is omitted, as is
// the details or ack segment whose value is missing. resumo, refs other
// than exit, paths, titles, agent kinds and free text are never rendered.
func Render(n *Notification, d Delivery) string {
	var tokens []string
	add := func(key, value string) {
		if value != "" {
			tokens = append(tokens, key+"="+value)
		}
	}
	add("notification", sanitize(n.ID, notificationIDPattern.MatchString))
	add("class", sanitize(n.Class, func(v string) bool { return inList(v, renderClasses) }))
	add("role", sanitizeList(d.Roles, func(v string) bool { return inList(v, renderRoles) }))
	add("job", sanitize(n.JobID, jobapi.ValidID))
	add("project", sanitize(n.Projeto, jobapi.ValidRepo))
	add("event", sanitize(n.EventTipo, func(v string) bool { return inList(v, jobapi.EventTypes) }))
	if n.EventSeq != 0 {
		add("seq", strconv.FormatInt(n.EventSeq, 10))
	}
	if n.Exit != nil {
		add("exit", strconv.Itoa(*n.Exit))
	}
	add("pane", sanitize(n.Pane, PanePattern.MatchString))
	add("workspace", sanitize(n.Workspace, WorkspacePattern.MatchString))
	add("status", sanitize(n.Status, isAgentStatus))
	add("escalation", sanitizeList(n.Escalations, func(v string) bool { return inList(v, renderEscalations) }))

	line := "[herdr-hermes] " + strings.Join(tokens, " ")
	if seg := detailsSegment(n); seg != "" {
		line += " | " + seg
	}
	if notificationIDPattern.MatchString(n.ID) && len(d.Roles) > 0 && inList(d.Roles[0], renderRoles) {
		line += " | optional ack: herdr-hermes notify ack " + n.ID + " --role " + d.Roles[0]
	}
	return line
}

// detailsSegment is the per-source-kind details command hint: job events
// point at the job events command (from seq-1), agent status at the
// pane, workspace events at the job status command (only with a valid
// job); the job commands carry the outbox hint because the local
// read-only pull channel always works; raises carry no details segment.
func detailsSegment(n *Notification) string {
	switch n.SourceKind {
	case SourceJobEvent:
		if jobapi.ValidID(n.JobID) && n.EventSeq > 0 {
			return "details: herdr-hermes job events --id " + n.JobID + " --since " + strconv.FormatInt(n.EventSeq-1, 10) + " (or the local outbox: herdr-hermes outbox)"
		}
	case SourceAgentStatus:
		if PanePattern.MatchString(n.Pane) {
			return "details: inspect pane " + n.Pane
		}
	case SourceWorkspace:
		if jobapi.ValidID(n.JobID) {
			return "details: herdr-hermes job status --id " + n.JobID + " (or the local outbox: herdr-hermes outbox)"
		}
	}
	return ""
}
