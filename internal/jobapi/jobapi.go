// Package jobapi vendors the public surface of the herdr-soho ephemeral job
// contract (docs/upstream/job-contract.md). Phase 1 duplicates the types
// instead of importing herdr-soho; contract_test.go fails when the vendored
// constants drift from the vendored document.
package jobapi

import (
	"encoding/json"
	"regexp"
)

// Event is one line of a job event log (jobs/<id>/events.jsonl). Decoding is
// lenient: unknown fields are ignored and unknown types are kept, so events
// gained by herdr-soho later still travel through the bridge.
type Event struct {
	Seq    int64          `json:"seq"`
	TS     string         `json:"ts"`
	Tipo   string         `json:"tipo"`
	Resumo string         `json:"resumo"`
	Refs   map[string]any `json:"refs,omitempty"`
	// Escopo and Motivo are present on decision events.
	Escopo string `json:"escopo,omitempty"`
	Motivo string `json:"motivo,omitempty"`
}

// DecodeEvent parses one event line. It never rejects an unknown tipo and
// keeps unknown fields out of the struct without error.
func DecodeEvent(data []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, err
	}
	return e, nil
}

// Event types, in the order the contract document lists them.
const (
	TypeAccepted       = "accepted"
	TypePreparing      = "preparing"
	TypeWorkerSpawned  = "worker_spawned"
	TypeWorkerDone     = "worker_done"
	TypeCommit         = "commit"
	TypePush           = "push"
	TypePrOpened       = "pr_opened"
	TypeCheckpoint     = "checkpoint"
	TypeReviewVerdict  = "review_verdict"
	TypeQuestion       = "question"
	TypeBlocked        = "blocked"
	TypeUnblocked      = "unblocked"
	TypeAmendReceived  = "amend_received"
	TypeDecision       = "decision"
	TypeDecisionAcked  = "decision_acked"
	TypeTimeoutWarning = "timeout_warning"
	TypeFailure        = "failure"
	TypeNote           = "note"
	TypeTerminal       = "terminal"
	TypeCleanup        = "cleanup"
)

// EventTypes lists the 20 contract event types in document order.
var EventTypes = []string{
	TypeAccepted, TypePreparing, TypeWorkerSpawned, TypeWorkerDone, TypeCommit,
	TypePush, TypePrOpened, TypeCheckpoint, TypeReviewVerdict, TypeQuestion,
	TypeBlocked, TypeUnblocked, TypeAmendReceived, TypeDecision,
	TypeDecisionAcked, TypeTimeoutWarning, TypeFailure, TypeNote, TypeTerminal,
	TypeCleanup,
}

// WakeMode says whether an event wakes the dispatcher.
type WakeMode int

const (
	// WakeNo: the event never wakes the dispatcher.
	WakeNo WakeMode = iota
	// WakeYes: the event wakes the dispatcher.
	WakeYes
	// WakeOnFailure: the event wakes the dispatcher only on failure.
	WakeOnFailure
)

// WakesDispatcher returns the wake mode of an event type. Unknown types never
// wake the dispatcher (WakeNo).
func WakesDispatcher(tipo string) WakeMode {
	switch tipo {
	case TypePush:
		return WakeOnFailure
	case TypePrOpened, TypeReviewVerdict, TypeQuestion, TypeBlocked,
		TypeDecision, TypeTimeoutWarning, TypeFailure, TypeTerminal:
		return WakeYes
	default:
		return WakeNo
	}
}

// EventsTrailer is the last line of `job events`:
// {"eventos":"fim","ultimo_seq":N,"estado":"<state>"}.
type EventsTrailer struct {
	Eventos   string `json:"eventos"`
	UltimoSeq int64  `json:"ultimo_seq"`
	Estado    string `json:"estado"`
}

// Lifecycle state names.
const (
	StateAccepted  = "accepted"
	StatePreparing = "preparing"
	StateRunning   = "running"
	StateBlocked   = "blocked"
	StateFinishing = "finishing"
	StateDone      = "done"
	StateFailed    = "failed"
	StateTimeout   = "timeout"
	StateCanceled  = "canceled"
	StateCollected = "collected"
	StateClosed    = "closed"
)

// States lists every lifecycle state name.
var States = []string{
	StateAccepted, StatePreparing, StateRunning, StateBlocked, StateFinishing,
	StateDone, StateFailed, StateTimeout, StateCanceled, StateCollected,
	StateClosed,
}

// TerminalStates are the states a job cannot leave by amendment.
var TerminalStates = []string{StateDone, StateFailed, StateTimeout, StateCanceled}

// IsTerminalState reports whether the state is terminal.
func IsTerminalState(state string) bool {
	for _, s := range TerminalStates {
		if s == state {
			return true
		}
	}
	return false
}

// RefsKeys are the known keys of an event refs object.
var RefsKeys = []string{
	"agente", "papel", "pane", "sha", "pr", "report", "brief", "motivo",
	"exit", "seq_ref",
}

// Exit codes of the herdr-soho job contract (docs/upstream/job-contract.md,
// "### Exit codes"), in document order.
const (
	ExitSuccess      = 0
	ExitUsage        = 2
	ExitNotFound     = 3
	ExitUnavailable  = 4
	ExitBlocked      = 7
	ExitJobTimeout   = 9
	ExitBlockedQuota = 11
	ExitBlockedAPI   = 14
	ExitFailed       = 19
	ExitIDConflict   = 20
	ExitCanceled     = 21
	ExitPreparation  = 22
	ExitReserved     = 23
	ExitPendingAck   = 24
)

// ExitCodes lists the contract exit codes in document order. The "11 / 14"
// row is two codes.
var ExitCodes = []int{
	ExitSuccess, ExitUsage, ExitNotFound, ExitUnavailable, ExitBlocked,
	ExitJobTimeout, ExitBlockedQuota, ExitBlockedAPI, ExitFailed,
	ExitIDConflict, ExitCanceled, ExitPreparation, ExitReserved,
	ExitPendingAck,
}

// Input limits copied from the contract "### Flags" section and "Input
// limits".
const (
	// StdinCapStart is the maximum brief size for `job start`.
	StdinCapStart = 256 * 1024
	// StdinCapAmend is the maximum body size for `job amend`.
	StdinCapAmend = 64 * 1024
	// StdinCapSend is the maximum body size for `job send`.
	StdinCapSend = 16 * 1024
	// MaxWaitMS bounds `wait --timeout` and `events --wait` (and this
	// bridge's outbox --wait) in milliseconds.
	MaxWaitMS = 600000
)

// Flag patterns from the contract "### Flags" section.
var (
	// IDPattern is the --id shape: ^[A-Za-z0-9._-]{1,64}$.
	IDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	// RepoPartPattern is the repo part of --repo <org>/<repo>:
	// ^[A-Za-z0-9._-]{1,100}$; the org itself is validated against the
	// machine's job_orgs list, which is not a regex.
	RepoPartPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)
	// BasePattern is the --base shape: ^[A-Za-z0-9._/-]{1,100}$, without
	// "..".
	BasePattern = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,100}$`)
)

// ValidID reports whether id matches the --id pattern.
func ValidID(id string) bool { return IDPattern.MatchString(id) }

// ValidRepo reports whether repo has the <org>/<repo> shape with each part
// matching RepoPartPattern. The org membership in job_orgs is herdr-soho's
// check, not a regex, and is not enforced here.
func ValidRepo(repo string) bool {
	org, name, ok := splitRepo(repo)
	if !ok {
		return false
	}
	return RepoPartPattern.MatchString(org) && RepoPartPattern.MatchString(name)
}

// ValidBase reports whether base matches the --base pattern without "..".
func ValidBase(base string) bool {
	return BasePattern.MatchString(base) && !containsDotDot(base)
}

func splitRepo(repo string) (org, name string, ok bool) {
	for i := 0; i < len(repo); i++ {
		if repo[i] == '/' {
			if i == 0 || i == len(repo)-1 {
				return "", "", false
			}
			for j := i + 1; j < len(repo); j++ {
				if repo[j] == '/' {
					return "", "", false
				}
			}
			return repo[:i], repo[i+1:], true
		}
	}
	return "", "", false
}

func containsDotDot(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '.' && s[i+1] == '.' {
			return true
		}
	}
	return false
}

// Wake hook environment variable names (the contract "Waking" section).
const (
	EnvJobID             = "HERDR_SOHO_JOB_ID"
	EnvJobSeq            = "HERDR_SOHO_JOB_SEQ"
	EnvJobEvent          = "HERDR_SOHO_JOB_EVENT"
	EnvJobIdempotencyKey = "HERDR_SOHO_JOB_IDEMPOTENCY_KEY"
)

// Capability keys of `herdr-soho capabilities --json`.
const (
	CapEphemeralJob = "ephemeral_job"
	CapJobEvents    = "job_events"
)
