// Package notify projects native sources (job contract events, Herdr agent
// status changes and explicit local raises) into sanitized notifications,
// routes them to explicitly registered recipients (the project owner, the
// job orchestrator and the machine coordinator), keeps a durable
// per-source/per-recipient ledger and delivers them through the existing
// herdr-soho peer message command. The outbox schema is not changed: the
// ledger lives in its own files next to the outbox, and a notification
// carries only allowlisted identifiers, never a raw event, output, prompt,
// path or environment value.
package notify

import (
	"context"
	"regexp"
	"time"
)

// LedgerSchema is the schema version of the registry and ledger files.
const LedgerSchema = 1

// TSLayout is the ledger timestamp layout: local time with an explicit
// offset and milliseconds, never "Z".
const TSLayout = "2006-01-02T15:04:05.000-07:00"

// Source kinds.
const (
	// SourceJobEvent is a job contract event stored in the outbox (from
	// the wake hook or from the sync reconciliation).
	SourceJobEvent = "job_event"
	// SourceAgentStatus is a Herdr pane.agent_status_changed event of a
	// registered (watched) pane.
	SourceAgentStatus = "agent_status"
	// SourceRaise is an explicit local raise (`notify raise`).
	SourceRaise = "raise"
	// SourceWorkspace is a Herdr workspace.created or workspace.closed
	// event of a job workspace (label "job-<id>").
	SourceWorkspace = "workspace"
)

// Notification classes.
const (
	ClassQuestion       = "question"
	ClassBlocked        = "blocked"
	ClassFailed         = "failed"
	ClassTerminal       = "terminal"
	ClassTimeoutWarning = "timeout_warning"
	ClassReviewVerdict  = "review_verdict"
	ClassPROpened       = "pr_opened"
	ClassDecision       = "decision"
	ClassAgentBlocked   = "agent_blocked"
	ClassCrossProject   = "cross_project"
	ClassStuck          = "stuck"
	// ClassAgentDone is a watched pane whose agent reported done (it
	// finished a turn). It is never implementation done or acceptance.
	ClassAgentDone       = "agent_done"
	ClassWorkspaceOpened = "workspace_opened"
	ClassWorkspaceClosed = "workspace_closed"
)

// Recipient roles.
const (
	RoleOwner        = "owner"
	RoleOrchestrator = "orchestrator"
	RoleCoordinator  = "coordinator"
)

// Escalation reasons (why the coordinator is a recipient).
const (
	EscBlocked      = "blocked"
	EscFailed       = "failed"
	EscMissingOwner = "missing_owner"
	EscCrossProject = "cross_project"
	EscStuck        = "stuck"
)

// Delivery states.
const (
	// StatePending: not accepted yet; due at NextAttemptAt.
	StatePending = "pending"
	// StateInFlight: claimed by a delivery run until LeaseUntil; an
	// expired lease is due again (the run crashed or was killed).
	StateInFlight = "in_flight"
	// StateAccepted: the endpoint accepted the message (herdr-soho send
	// exit 0). Final.
	StateAccepted = "accepted"
	// StateUncertain: herdr-soho send exit 15 or a send killed in flight
	// at its deadline, receipt not proved. Final:
	// never resent automatically, so a message is never injected twice.
	StateUncertain = "uncertain"
	// StateRejected: herdr-soho send refused for good (exit 2, 3 or 18).
	// Final.
	StateRejected = "rejected"
	// StateExhausted: MaxAttempts transient failures. Final.
	StateExhausted = "exhausted"
	// StateNoRoute: the role has no registered recipient. Final.
	StateNoRoute = "no_route"
	// StateSelf: the recipient is the source itself (loop guard). Final.
	StateSelf = "self"
)

// Send outcomes reported by a Sender.
const (
	// OutcomeAccepted: exit 0.
	OutcomeAccepted = "accepted"
	// OutcomeUncertain: exit 15, or a send killed in flight at its
	// deadline: receipt not proved, never resent automatically.
	OutcomeUncertain = "uncertain"
	// OutcomeRejected: exit 2, 3 or 18.
	OutcomeRejected = "rejected"
	// OutcomeTransient: any other exit, or herdr-soho unavailable
	// (nothing was sent); retried with backoff.
	OutcomeTransient = "transient"
)

// RetrySchedule is the delay before attempt 2, 3, … after a transient
// failure; the last value repeats. MaxAttempts bounds the attempts.
var RetrySchedule = []time.Duration{
	10 * time.Second, 30 * time.Second, 90 * time.Second,
	300 * time.Second, 900 * time.Second,
}

// MaxAttempts is the number of attempts after which a transiently failing
// delivery becomes StateExhausted.
const MaxAttempts = 6

// LeaseDuration is how long a claimed delivery stays in flight before
// another run may claim it again.
const LeaseDuration = 60 * time.Second

// Identifier patterns. Every value rendered into a message must match its
// pattern; a value that does not is rendered as "?".
var (
	// RefPattern is a herdr-soho send reference "<machine>/<ws>:<pane>".
	RefPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}/[A-Za-z0-9._-]{1,64}:[A-Za-z0-9._-]{1,64}$`)
	// AgentNamePattern is a herdr-soho agent name on the local server.
	AgentNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	// PanePattern is a Herdr pane id "<ws>:<pane>".
	PanePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}:[A-Za-z0-9._-]{1,64}$`)
	// WorkspacePattern is a Herdr workspace id.
	WorkspacePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	// RaiseIDPattern is the caller-chosen idempotency id of a raise.
	RaiseIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// ValidRef reports whether ref is a herdr-soho send reference or a local
// agent name.
func ValidRef(ref string) bool {
	return RefPattern.MatchString(ref) || AgentNamePattern.MatchString(ref)
}

// Registry holds the explicit registrations. Nothing is ever guessed: a
// role with no entry has no recipient.
type Registry struct {
	Schema int `json:"schema"`
	// Owners maps a project "<org>/<repo>" to its owner reference.
	Owners map[string]string `json:"owners"`
	// Orchestrators maps a job id to its orchestrator reference.
	Orchestrators map[string]string `json:"orchestrators"`
	// Coordinator is the machine escalation reference ("" when none).
	Coordinator string `json:"coordinator"`
	// Watches maps a Herdr pane id to the watch registration; only
	// watched panes produce agent status notifications.
	Watches map[string]Watch `json:"watches"`
	// WorkspaceWatches maps a Herdr workspace id to its registration:
	// a registered workspace produces workspace opened/closed
	// notifications whatever its label, routed by the registration.
	WorkspaceWatches map[string]WorkspaceWatch `json:"workspace_watches,omitempty"`
}

// WorkspaceWatch registers one Herdr workspace as a workspace source.
type WorkspaceWatch struct {
	Workspace string `json:"workspace"`
	Job       string `json:"job,omitempty"`
	Projeto   string `json:"projeto,omitempty"`
}

// Watch registers one Herdr pane as an agent status source.
type Watch struct {
	Pane    string `json:"pane"`
	Job     string `json:"job,omitempty"`
	Projeto string `json:"projeto,omitempty"`
}

// JobEventInput is the projection input for one stored job_event outbox
// record: the record fields plus the decoded contract event.
type JobEventInput struct {
	OutboxSeq int64
	Projeto   string
	JobID     string
	// Event is the original event JSON (the outbox record's dados).
	Event []byte
}

// AgentStatusEvent is a parsed Herdr pane.agent_status_changed event.
// Only allowlisted fields are kept; the title, labels and agent kind are
// dropped at parse time.
type AgentStatusEvent struct {
	Pane      string
	Workspace string
	Status    string // idle, working, blocked, done, unknown
}

// Transition is one true agent status change of a watched pane, numbered
// per pane by the ledger (N starts at 1). A repeated status is not a
// transition.
type Transition struct {
	Pane      string
	Workspace string
	From      string // "" for the first observation
	To        string
	N         int64
	Watch     Watch
}

// Workspace event names, as parsed from the Herdr event.
const (
	WorkspaceCreated = "created"
	WorkspaceClosed  = "closed"
)

// WorkspaceEvent is a parsed Herdr workspace.created or workspace.closed
// event. Only allowlisted fields are kept. Label is empty when the event
// carries no workspace info (a closed workspace may carry none).
type WorkspaceEvent struct {
	Event     string // WorkspaceCreated or WorkspaceClosed
	Workspace string
	Label     string
}

// WorkspaceInput is the projection input for one true workspace
// transition (opened or closed), numbered per workspace by the ledger (N
// starts at 1), so a later reopen of the same workspace id is a distinct
// notification. JobID and Projeto come from the registration, else from
// a job-<id> label, else from the job recorded when it opened.
type WorkspaceInput struct {
	Event     string // WorkspaceCreated or WorkspaceClosed
	Workspace string
	N         int64
	JobID     string
	Projeto   string
}

// RaiseInput is one explicit local raise.
type RaiseInput struct {
	// ID is the idempotency id (caller-chosen or generated once).
	ID      string
	Class   string // ClassCrossProject or ClassStuck
	Projeto string
	JobID   string
}

// Notification is one projected source, with its deliveries.
type Notification struct {
	// ID is "n" + the first 20 hex digits of sha256(SourceID): the same
	// source always maps to the same notification.
	ID         string `json:"id"`
	SourceKind string `json:"source_kind"`
	// SourceID: "job:<job>:<seq>", "agent:<pane>:<n>:<status>",
	// "workspace:<ws>:<n>:<opened|closed>" or
	// "raise:<id>".
	SourceID    string   `json:"source_id"`
	Class       string   `json:"class"`
	JobID       string   `json:"job_id,omitempty"`
	Projeto     string   `json:"projeto,omitempty"`
	EventTipo   string   `json:"event_tipo,omitempty"`
	EventSeq    int64    `json:"event_seq,omitempty"`
	Exit        *int     `json:"exit,omitempty"`
	Pane        string   `json:"pane,omitempty"`
	Workspace   string   `json:"workspace,omitempty"`
	Status      string   `json:"status,omitempty"`
	Escalations []string `json:"escalations,omitempty"`
	// SourceTS is the source's own timestamp when the source provides
	// one (job events), else "" — never invented.
	SourceTS    string     `json:"source_ts,omitempty"`
	ObservedAt  string     `json:"observed_at"`
	PersistedAt string     `json:"persisted_at"`
	Deliveries  []Delivery `json:"deliveries"`
}

// Delivery is one recipient of a notification.
type Delivery struct {
	// Roles lists every role this recipient holds for the notification
	// (one delivery per distinct reference).
	Roles          []string `json:"roles"`
	Ref            string   `json:"ref,omitempty"`
	State          string   `json:"state"`
	Attempts       int      `json:"attempts"`
	NextAttemptAt  string   `json:"next_attempt_at,omitempty"`
	LeaseUntil     string   `json:"lease_until,omitempty"`
	FirstAttemptAt string   `json:"first_attempt_at,omitempty"`
	LastAttemptAt  string   `json:"last_attempt_at,omitempty"`
	AcceptedAt     string   `json:"accepted_at,omitempty"`
	// AcceptStatus is the herdr-soho send result word: sent, queued or
	// accepted (exit 0 with another wording).
	AcceptStatus string `json:"accept_status,omitempty"`
	LastExit     *int   `json:"last_exit,omitempty"`
	// LastError is a fixed category, never child output.
	LastError string `json:"last_error,omitempty"`
	// AckAt is the optional recipient processing acknowledgement
	// (`notify ack`); delivery never waits for it.
	AckAt string `json:"ack_at,omitempty"`
}

// SendResult is what a Sender reports for one attempt.
type SendResult struct {
	Outcome string // Outcome* constant
	// Status is sent, queued or accepted on OutcomeAccepted.
	Status string
	// Exit is the herdr-soho exit code, or -1 when it did not run to an
	// exit (unavailable, deadline).
	Exit int
	// Category is a fixed error category on failure: "unavailable",
	// "deadline", "busy", "uncertain", "refused", "usage", "unknown_agent",
	// "exit".
	Category string
}

// Sender delivers one rendered message to one reference.
type Sender interface {
	Send(ctx context.Context, ref, message string) SendResult
}
