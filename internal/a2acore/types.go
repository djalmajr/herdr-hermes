// Package a2acore is the neutral core of the A2A ingress (DJA-283): the
// authenticated subject and its authorization, the evaluation of an
// incoming message as data, the per-session fence and sequence rules, the
// owner/guardian routing, the per-recipient delivery state machine and the
// delivery runner. It depends only on the standard library and on the
// neutral notify and jobapi packages: it never imports the A2A SDK, an
// HTTP stack, a database driver or a provider-specific transport. The
// SDK binding lives in internal/a2aserver, the SQLite persistence in
// internal/a2astore, and the Herdr transport is any notify.Sender.
//
// An incoming message is a request to evaluate, never an instruction:
// nothing in this package runs, renders or forwards message text, and a
// delivery notice carries only allowlisted identifiers.
//
// This file is the frozen contract shared by the A2A slices; change it
// only through the orchestrator.
package a2acore

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/djalmajr/nau/internal/notify"
)

// ---------------------------------------------------------------------
// Authentication and authorization
// ---------------------------------------------------------------------

// Action is one operation a subject may be authorized for.
type Action string

// Actions.
const (
	ActionSend   Action = "message:send"
	ActionRead   Action = "task:read"
	ActionCancel Action = "task:cancel"
	ActionPush   Action = "push:manage"
)

// AllActions lists every action, in a fixed order.
var AllActions = []Action{ActionSend, ActionRead, ActionCancel, ActionPush}

// AnyProject in Subject.Projects authorizes every project.
const AnyProject = "*"

// Subject is the authenticated caller of one request. Only an
// Authenticator creates it, from transport credentials; it is never read
// from message content or metadata.
type Subject struct {
	// ID is the stable subject identifier (SubjectPattern). It owns the
	// tasks it creates: another subject never sees them.
	ID string
	// Ref is the herdr-soho reference this subject speaks for, or "".
	// A delivery to the same reference is never sent (loop guard).
	Ref string
	// Projects are the "<org>/<repo>" projects the subject may address;
	// AnyProject authorizes all of them.
	Projects []string
	// Actions are the actions the subject may perform.
	Actions []Action
}

// SubjectPattern bounds a subject identifier.
var SubjectPattern = regexp.MustCompile(`^[A-Za-z0-9._:@-]{1,128}$`)

// Credential is the transport credential of one request. The
// Authorization value is opaque: it is never logged, stored, rendered,
// put in an error or handed to a subprocess.
type Credential struct {
	// Authorization is the raw Authorization header value ("" when the
	// request carried none).
	Authorization string
	// RemoteAddr is the peer address as the HTTP server saw it.
	RemoteAddr string
}

// Authenticator maps the transport credential of a request to a subject.
// It returns an error wrapping ErrUnauthenticated when the credential is
// missing, malformed or unknown; the error text never carries the
// credential.
type Authenticator interface {
	Authenticate(ctx context.Context, c Credential) (Subject, error)
}

// Authorizer decides whether a subject may perform an action on a
// project ("" when the action is not project-scoped). It returns nil or
// an error wrapping ErrForbidden.
type Authorizer interface {
	Authorize(s Subject, a Action, project string) error
}

// SecretProvider keeps secret values (push notification tokens and
// credentials) outside the durable records: the records hold only the
// opaque handle. Get returns an error wrapping ErrSecretUnavailable when
// the handle is unknown (for example after a restart of an in-memory
// provider).
type SecretProvider interface {
	Put(ctx context.Context, value string) (handle string, err error)
	Get(ctx context.Context, handle string) (string, error)
	Delete(ctx context.Context, handle string) error
}

// Errors.
var (
	ErrUnauthenticated   = errors.New("a2a: unauthenticated")
	ErrForbidden         = errors.New("a2a: forbidden")
	ErrSecretUnavailable = errors.New("a2a: secret unavailable")
)

// ---------------------------------------------------------------------
// Evaluation: the data-only view of a message
// ---------------------------------------------------------------------

// Message metadata keys. The correlation keys are read from the incoming
// message metadata; the others are written by Nau on its own events.
const (
	MetaProject     = "nau.project"      // "<org>/<repo>", required on a new task
	MetaJob         = "nau.job"          // contract job id, optional
	MetaSession     = "nau.session"      // client session id, required
	MetaEpoch       = "nau.epoch"        // client epoch, integer >= 1, required
	MetaSeq         = "nau.seq"          // per (session, epoch) sequence, integer >= 1, required
	MetaRefusal     = "nau.refusal"      // refusal code on a status message
	MetaDecision    = "nau.decision"     // decision id on the event that applies it
	MetaDuplicateOf = "nau.duplicate_of" // task id on a duplicate-message reply
)

// SessionPattern bounds a client session id.
var SessionPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

// MessageIDPattern bounds a client message id.
var MessageIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// TaskIDPattern bounds a task or context id rendered or accepted by Nau.
var TaskIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// DecisionIDPattern bounds a decision id.
var DecisionIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// MaxSeq bounds epochs and sequence numbers (the largest integer a JSON
// number carries exactly).
const MaxSeq = 1<<53 - 1

// Part kinds of a PartView.
const (
	PartText  = "text"
	PartData  = "data"
	PartFile  = "file"
	PartOther = "other"
)

// Roles of a MessageView.
const (
	RoleUser  = "user"
	RoleAgent = "agent"
)

// PartView is one message part as the adapter saw it.
type PartView struct {
	Kind string // PartText, PartData, PartFile or PartOther
	// Text is the text of a PartText part ("" otherwise).
	Text string
}

// MessageView is the SDK-independent view of one incoming message,
// filled by the adapter. Metadata values keep their JSON-decoded Go
// types (string, float64, bool, nil, map, slice).
type MessageView struct {
	ID        string
	TaskID    string // "" for the first message of a new task
	ContextID string // "" when the client did not choose one
	Role      string // RoleUser or RoleAgent ("" when unknown)
	Parts     []PartView
	Metadata  map[string]any
}

// Neutral task states (the adapter maps them to and from the SDK).
const (
	StateSubmitted     = "submitted"
	StateWorking       = "working" // accepted, awaiting the owner or guardian
	StateInputRequired = "input_required"
	StateCompleted     = "completed"
	StateCanceled      = "canceled"
	StateFailed        = "failed"
	StateRejected      = "rejected"
)

// Terminal reports whether a neutral task state is terminal.
func Terminal(state string) bool {
	switch state {
	case StateCompleted, StateCanceled, StateFailed, StateRejected:
		return true
	}
	return false
}

// TaskView is the SDK-independent view of a stored task, used to
// evaluate a continuation message.
type TaskView struct {
	ID        string
	ContextID string
	State     string // neutral state
	Owner     string // subject id that created the task
	Project   string
	Job       string
	Messages  int // messages in the task history
}

// Limits bounds what Evaluate and the store admit.
type Limits struct {
	MaxParts           int // parts per message
	MaxTextBytes       int // total text bytes per message
	MaxMessagesPerTask int // messages in one task history
	MaxTasksPerContext int // tasks in one context (loop breaker)
	MaxOpenTasks       int // non-terminal tasks in the store (capacity)
	MaxDecisionBytes   int // decision text bytes
}

// DefaultLimits are the limits used when the caller sets none.
var DefaultLimits = Limits{
	MaxParts:           16,
	MaxTextBytes:       64 << 10,
	MaxMessagesPerTask: 64,
	MaxTasksPerContext: 32,
	MaxOpenTasks:       1000,
	MaxDecisionBytes:   16 << 10,
}

// Envelope is the evaluated correlation of one admitted message. It
// holds identifiers only: the message text stays in the task history as
// data.
type Envelope struct {
	Subject    string // authenticated subject id
	SubjectRef string // Subject.Ref (loop guard), "" when none
	MessageID  string
	TaskID     string // "" until the task id is known
	ContextID  string
	Project    string
	Job        string
	Session    string
	Epoch      int64
	Seq        int64
	TextBytes  int
}

// Refusal codes.
const (
	RefuseUnauthorizedProject = "unauthorized_project" // project outside the subject's scope
	RefuseInvalidMessage      = "invalid_message"      // role, parts, size or content kind refused
	RefuseInvalidCorrelation  = "invalid_correlation"  // missing or malformed nau.* metadata
	RefuseCorrelationMismatch = "correlation_mismatch" // continuation names another project, job or context
	RefuseStaleEpoch          = "stale_epoch"          // epoch below the session's current epoch
	RefuseReplay              = "replay"               // seq already used by another message id
	RefuseOutOfOrder          = "out_of_order"         // seq is not the next one
	RefuseLoopLimit           = "loop_limit"           // context or task history at its bound
	RefuseCapacity            = "capacity"             // open tasks at their bound
	RefuseTerminal            = "terminal"             // the task is terminal
	RefuseNotAwaitingOwner    = "not_awaiting_owner"   // a decision on a task not in StateWorking
	RefuseUnknownTask         = "unknown_task"         // no such task (or not the caller's)
	RefuseDecisionConflict    = "decision_conflict"    // decision id reused with other content
	RefuseInvalidDecision     = "invalid_decision"     // decision kind, id or text refused
)

// Refusal is a deterministic refusal with a fixed code. Detail is a
// short fixed English text; it never quotes message text or credentials.
type Refusal struct {
	Code   string
	Detail string
}

// Error implements error.
func (r *Refusal) Error() string { return "a2a: refused: " + r.Code + ": " + r.Detail }

// ---------------------------------------------------------------------
// Fence and sequence
// ---------------------------------------------------------------------

// FenceState is the stored fence of one (subject, session): the current
// epoch and the last admitted sequence number in it. Found is false when
// the session was never seen.
type FenceState struct {
	Found   bool
	Epoch   int64
	LastSeq int64
}

// ---------------------------------------------------------------------
// Routing and delivery
// ---------------------------------------------------------------------

// Recipient roles.
const (
	RoleOwner    = "owner"    // the project owner (notify owner registration)
	RoleGuardian = "guardian" // the machine coordinator, when the owner is missing
)

// Delivery states. The shared ones keep the notify semantics: accepted
// means the endpoint accepted the notice, never that the recipient
// processed it; uncertain is final and never resent automatically.
const (
	DeliveryHeld      = "held" // admitted, waiting for its task to commit
	DeliveryPending   = notify.StatePending
	DeliveryInFlight  = notify.StateInFlight
	DeliveryAccepted  = notify.StateAccepted
	DeliveryUncertain = notify.StateUncertain
	DeliveryRejected  = notify.StateRejected
	DeliveryExhausted = notify.StateExhausted
	DeliveryNoRoute   = notify.StateNoRoute
	DeliverySelf      = notify.StateSelf
	DeliveryObsolete  = "obsolete" // the task ended (or never committed) before the send
)

// RouteSource answers the explicit registrations: the owner reference of
// a project and the guardian (coordinator) reference. "" means none; an
// error means the registrations could not be read.
type RouteSource interface {
	OwnerRef(project string) (string, error)
	GuardianRef() (string, error)
}

// PlannedDelivery is one recipient of an admitted message.
type PlannedDelivery struct {
	Role  string // RoleOwner or RoleGuardian
	Ref   string // "" for DeliveryNoRoute
	State string // DeliveryHeld, DeliveryNoRoute or DeliverySelf
}

// DeliveryClaim is one delivery claimed for a send attempt.
type DeliveryClaim struct {
	DeliveryID int64
	IngressID  int64
	TaskID     string
	ContextID  string
	Project    string
	Job        string
	Role       string
	Ref        string
	Attempt    int // the attempt number of this claim (1 for the first)
	TaskState  string
}

// Delivery is one stored delivery, as listed by the inbox.
type Delivery struct {
	DeliveryID    int64  `json:"delivery_id"`
	IngressID     int64  `json:"ingress_id"`
	Role          string `json:"role"`
	Ref           string `json:"ref,omitempty"`
	State         string `json:"state"`
	Attempts      int    `json:"attempts"`
	NextAttemptAt string `json:"next_attempt_at,omitempty"`
	AcceptedAt    string `json:"accepted_at,omitempty"`
	AcceptStatus  string `json:"accept_status,omitempty"`
	LastExit      *int   `json:"last_exit,omitempty"`
	LastError     string `json:"last_error,omitempty"`
}

// ---------------------------------------------------------------------
// Owner and guardian decisions
// ---------------------------------------------------------------------

// DecisionKind is what the owner or guardian decided on a task awaiting
// them.
type DecisionKind string

// Decision kinds.
const (
	DecisionAnswer  DecisionKind = "answer"  // completes the task with the text as an artifact
	DecisionAsk     DecisionKind = "ask"     // input required: the text is a question to the caller
	DecisionDecline DecisionKind = "decline" // rejects the task with the text as the reason
)

// Decision states.
const (
	DecisionPending = "pending" // recorded, not applied yet
	DecisionApplied = "applied" // applied to the task in the same transaction as the task update
	DecisionRefused = "refused" // never applied (Refusal holds the code)
)

// Decision is one recorded owner or guardian decision.
type Decision struct {
	ID         string       `json:"id"`
	TaskID     string       `json:"task_id"`
	Kind       DecisionKind `json:"kind"`
	Text       string       `json:"text"`
	Actor      string       `json:"actor"`
	State      string       `json:"state"`
	Refusal    string       `json:"refusal,omitempty"`
	RecordedAt string       `json:"recorded_at"`
	AppliedAt  string       `json:"applied_at,omitempty"`
}

// ---------------------------------------------------------------------
// The durable queue (implemented by internal/a2astore)
// ---------------------------------------------------------------------

// Admission is one evaluated message to admit, with its planned
// deliveries.
type Admission struct {
	Envelope   Envelope
	TaskID     string // the task the message belongs to (the server-assigned id for a new task)
	ContextID  string
	NewTask    bool
	Deliveries []PlannedDelivery
	ReceivedAt time.Time
}

// AdmitResult is the outcome of one Admit call. Exactly one of a fresh
// admission (IngressID > 0, Duplicate false), a duplicate (Duplicate
// true, DuplicateTaskID set) or a refusal (Refusal non-nil) is reported
// with a nil error.
type AdmitResult struct {
	IngressID       int64
	Duplicate       bool
	DuplicateTaskID string
	Refusal         *Refusal
}

// InboxFilter selects inbox items; empty fields select all.
type InboxFilter struct {
	Project string
	State   string // neutral task state
	Limit   int    // 0 means 100
}

// InboxItem is one task as the owner sees it locally. It carries the
// correlation and delivery state, never the message text (Show does).
type InboxItem struct {
	TaskID           string     `json:"task_id"`
	ContextID        string     `json:"context_id"`
	State            string     `json:"state"`
	Subject          string     `json:"subject"`
	Project          string     `json:"project"`
	Job              string     `json:"job,omitempty"`
	Messages         int        `json:"messages"`
	Created          string     `json:"created"`
	Updated          string     `json:"updated"`
	Deliveries       []Delivery `json:"deliveries"`
	PendingDecisions int        `json:"pending_decisions"`
}

// Queue is the durable ingress, delivery and decision queue. Every
// mutation is one immediate SQLite transaction.
type Queue interface {
	// Admit runs, in one transaction: the duplicate check on (subject,
	// message id) — a duplicate returns the first admission's task —,
	// CheckFence on the (subject, session) fence, the loop limit
	// (MaxTasksPerContext for a new task, MaxMessagesPerTask for a
	// continuation), the capacity limit (MaxOpenTasks for a new task),
	// then inserts the ingress row (its id is the global admission
	// order), advances the fence and inserts the planned deliveries
	// (DeliveryHeld rows wait for their task to commit). A refusal
	// changes nothing.
	Admit(ctx context.Context, a Admission) (AdmitResult, error)

	// ClaimDue claims at most limit due deliveries (DeliveryPending with
	// next attempt at or before now, or DeliveryInFlight whose lease
	// expired), ordered by ingress id then delivery id, marks them
	// DeliveryInFlight until now+lease and counts the attempt.
	ClaimDue(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]DeliveryClaim, error)

	// Complete records the outcome of one claimed attempt with
	// NextDeliveryState. A claim whose delivery is no longer in flight
	// for that attempt changes nothing.
	Complete(ctx context.Context, c DeliveryClaim, r notify.SendResult, now time.Time) error

	// Release returns an unsent claim to DeliveryPending without
	// consuming its attempt.
	Release(ctx context.Context, c DeliveryClaim) error

	// RecordDecision records a pending decision: idempotent by id (the
	// same id and content returns the stored decision, other content is
	// RefuseDecisionConflict); an unknown task is RefuseUnknownTask and a
	// terminal one RefuseTerminal.
	RecordDecision(ctx context.Context, d Decision, now time.Time) (Decision, *Refusal, error)

	// PendingDecisions lists at most limit pending decisions in record
	// order.
	PendingDecisions(ctx context.Context, limit int) ([]Decision, error)

	// RefuseDecision marks a pending decision refused with a code.
	RefuseDecision(ctx context.Context, id, code string, now time.Time) error

	// Inbox lists tasks with their deliveries, newest update first.
	Inbox(ctx context.Context, f InboxFilter) ([]InboxItem, error)

	// RecoverHeld marks DeliveryObsolete the held deliveries whose task
	// did not commit within grace of their admission (a crash between
	// the admission and the task write). In the same transaction it
	// deletes the pending push configs (rows and secret handles) whose
	// task has no row and that are older than grace. It returns how many
	// deliveries it marked.
	RecoverHeld(ctx context.Context, now time.Time, grace time.Duration) (int, error)
}
