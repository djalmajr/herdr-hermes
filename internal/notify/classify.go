package notify

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
)

// ProjectJobEvent projects one stored job_event. ok=false (and a nil
// notification) when the event tipo is not notifiable; err != nil when the
// event is not a JSON object, has no positive integer seq or an empty tipo,
// or the record job id is invalid. Invalid identifiers never leave this
// function: the projeto is kept only when valid (dropped otherwise and
// routed as missing), and resumo, the refs contents (except the parsed
// exit) and unknown fields are never copied into the notification.
func ProjectJobEvent(in JobEventInput, reg Registry, now time.Time) (*Notification, bool, error) {
	if !jobapi.ValidID(in.JobID) {
		return nil, false, errors.New("notify: job event job id is missing or invalid")
	}
	if !isJSONObject(in.Event) {
		return nil, false, errors.New("notify: job event is not a JSON object")
	}
	var ev jobapi.Event
	if err := json.Unmarshal(in.Event, &ev); err != nil {
		return nil, false, fmt.Errorf("notify: decode job event: %w", err)
	}
	if ev.Seq <= 0 {
		return nil, false, errors.New("notify: job event seq is not a positive integer")
	}
	if ev.Tipo == "" {
		return nil, false, errors.New("notify: job event tipo is empty")
	}
	class, escs, exit, ok := classifyJobEvent(ev)
	if !ok {
		return nil, false, nil
	}
	// An invalid projeto is dropped, not stored: routing then treats the
	// notification as having no owner (missing_owner + no_route).
	projeto := in.Projeto
	if !jobapi.ValidRepo(projeto) {
		projeto = ""
	}
	n := &Notification{
		SourceKind: SourceJobEvent,
		SourceID:   "job:" + in.JobID + ":" + strconv.FormatInt(ev.Seq, 10),
		Class:      class,
		JobID:      in.JobID,
		Projeto:    projeto,
		EventTipo:  ev.Tipo,
		EventSeq:   ev.Seq,
		Exit:       exit,
		SourceTS:   validSourceTS(ev.TS),
		ObservedAt: now.Format(TSLayout),
	}
	n.ID = NotificationID(n.SourceID)
	// The refs only feed the self guard: agente and pane as strings.
	agente, _ := ev.Refs["agente"].(string)
	pane, _ := ev.Refs["pane"].(string)
	n.Escalations, n.Deliveries = route(reg, projeto, in.JobID, escs, now, func(ref string) bool {
		if ref == "" {
			return false
		}
		if agente != "" && ref == agente {
			return true
		}
		return pane != "" && (ref == pane || strings.HasSuffix(ref, "/"+pane))
	})
	return n, true, nil
}

// classifyJobEvent maps one contract event tipo to its class, class
// escalations and parsed refs.exit. ok=false for tipos that are not
// projected (routine and unknown tipos).
func classifyJobEvent(ev jobapi.Event) (class string, escs []string, exit *int, ok bool) {
	switch ev.Tipo {
	case jobapi.TypeQuestion:
		return ClassQuestion, nil, nil, true
	case jobapi.TypeBlocked:
		return ClassBlocked, []string{EscBlocked}, nil, true
	case jobapi.TypeFailure:
		return ClassFailed, []string{EscFailed}, nil, true
	case jobapi.TypeTerminal:
		e := parseExit(ev.Refs)
		switch {
		case e != nil && (*e == jobapi.ExitBlocked || *e == jobapi.ExitBlockedQuota || *e == jobapi.ExitBlockedAPI):
			return ClassBlocked, []string{EscBlocked}, e, true
		case e != nil && (*e == jobapi.ExitJobTimeout || *e == jobapi.ExitFailed || *e == jobapi.ExitPreparation):
			return ClassFailed, []string{EscFailed}, e, true
		default:
			// Exit 0, 21, missing, unparseable or any other value.
			return ClassTerminal, nil, e, true
		}
	case jobapi.TypeTimeoutWarning:
		return ClassTimeoutWarning, nil, nil, true
	case jobapi.TypeReviewVerdict:
		return ClassReviewVerdict, nil, nil, true
	case jobapi.TypePrOpened:
		return ClassPROpened, nil, nil, true
	case jobapi.TypeDecision:
		if ev.Escopo == "global" {
			return ClassDecision, []string{EscCrossProject}, nil, true
		}
		return ClassDecision, nil, nil, true
	default:
		return "", nil, nil, false
	}
}

// parseExit extracts refs.exit: a JSON integer or a string of decimal
// digits. It returns nil when the key is absent, unparseable or out of
// integer range — the caller treats nil as a missing exit.
func parseExit(refs map[string]any) *int {
	if refs == nil {
		return nil
	}
	v, ok := refs["exit"]
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case float64:
		if t < 0 || t > math.MaxInt32 || t != float64(int64(t)) {
			return nil
		}
		i := int(t)
		return &i
	case string:
		if t == "" {
			return nil
		}
		for i := 0; i < len(t); i++ {
			if t[i] < '0' || t[i] > '9' {
				return nil
			}
		}
		n, err := strconv.Atoi(t)
		if err != nil {
			return nil
		}
		return &n
	default:
		return nil
	}
}

// ProjectAgentStatus projects one agent status transition. "blocked" and
// "done" are notifiable; idle, working and unknown are routine, coalesced
// by the ledger and never fail or escalate anything. A done transition of
// a pane that is itself the destination of any registered recipient is
// coalesced (loop guard); blocked keeps notifying whatever the registry
// says (a dialog needs a person). Invalid watch job/projeto values are
// dropped at projection (routed as missing), never stored on the
// notification.
func ProjectAgentStatus(tr Transition, reg Registry, now time.Time) (*Notification, bool) {
	var (
		class     string
		escs      []string
		selfGuard bool
	)
	switch tr.To {
	case "blocked":
		class, escs, selfGuard = ClassAgentBlocked, []string{EscBlocked}, true
	case "done":
		if doneLoopGuarded(tr, reg) {
			return nil, false
		}
		class = ClassAgentDone
	default:
		return nil, false
	}
	jobID := tr.Watch.Job
	if !jobapi.ValidID(jobID) {
		jobID = ""
	}
	projeto := tr.Watch.Projeto
	if !jobapi.ValidRepo(projeto) {
		projeto = ""
	}
	n := &Notification{
		SourceKind: SourceAgentStatus,
		SourceID:   "agent:" + tr.Pane + ":" + strconv.FormatInt(tr.N, 10) + ":" + tr.To,
		Class:      class,
		Pane:       tr.Pane,
		Workspace:  tr.Workspace,
		Status:     tr.To,
		JobID:      jobID,
		Projeto:    projeto,
		ObservedAt: now.Format(TSLayout),
	}
	n.ID = NotificationID(n.SourceID)
	var isSelf func(ref string) bool
	if selfGuard {
		isSelf = func(ref string) bool {
			if ref == "" || tr.Pane == "" {
				return false
			}
			return ref == tr.Pane || strings.HasSuffix(ref, "/"+tr.Pane)
		}
	}
	n.Escalations, n.Deliveries = route(reg, projeto, jobID, escs, now, isSelf)
	return n, true
}

// doneLoopGuarded reports whether the watched pane is itself the
// destination of any registered recipient: any value of the registry's
// Owners, any value of its Orchestrators, or its Coordinator, equal to
// the pane or ending in "/"+pane. A delivered done notification makes the
// recipient work and then report done, which would notify the other
// recipient and ping-pong; the transition is coalesced instead.
func doneLoopGuarded(tr Transition, reg Registry) bool {
	if tr.Pane == "" {
		return false
	}
	isPane := func(ref string) bool {
		return ref != "" && (ref == tr.Pane || strings.HasSuffix(ref, "/"+tr.Pane))
	}
	for _, ref := range reg.Owners {
		if isPane(ref) {
			return true
		}
	}
	for _, ref := range reg.Orchestrators {
		if isPane(ref) {
			return true
		}
	}
	return isPane(reg.Coordinator)
}

// ProjectRaise projects one explicit local raise. It errors when the class
// is not cross_project/stuck, the id does not match RaiseIDPattern, the
// projeto is neither empty nor a valid repo, or the job id is neither empty
// nor a valid id. Raises have no self guard.
func ProjectRaise(in RaiseInput, reg Registry, now time.Time) (*Notification, error) {
	switch in.Class {
	case ClassCrossProject, ClassStuck:
	default:
		return nil, fmt.Errorf("notify: raise class %q is not %s or %s", in.Class, ClassCrossProject, ClassStuck)
	}
	if !RaiseIDPattern.MatchString(in.ID) {
		return nil, errors.New("notify: raise id does not match the raise id pattern")
	}
	if in.Projeto != "" && !jobapi.ValidRepo(in.Projeto) {
		return nil, errors.New("notify: raise projeto is neither empty nor a valid repo")
	}
	if in.JobID != "" && !jobapi.ValidID(in.JobID) {
		return nil, errors.New("notify: raise job id is neither empty nor a valid id")
	}
	escs := []string{EscCrossProject}
	if in.Class == ClassStuck {
		escs = []string{EscStuck}
	}
	n := &Notification{
		SourceKind: SourceRaise,
		SourceID:   "raise:" + in.ID,
		Class:      in.Class,
		JobID:      in.JobID,
		Projeto:    in.Projeto,
		ObservedAt: now.Format(TSLayout),
	}
	n.ID = NotificationID(n.SourceID)
	n.Escalations, n.Deliveries = route(reg, in.Projeto, in.JobID, escs, now, nil)
	return n, nil
}

// NotificationID returns "n" + the first 20 lowercase hex digits of
// sha256(sourceID): the same source always maps to the same notification.
func NotificationID(sourceID string) string {
	sum := sha256.Sum256([]byte(sourceID))
	return "n" + hex.EncodeToString(sum[:])[:20]
}

// NewRaiseID returns 16 lowercase hex digits read from r (8 bytes); it
// errors when r fails or ends before 8 bytes.
func NewRaiseID(r io.Reader) (string, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return "", fmt.Errorf("notify: read raise id bytes: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// route builds the delivery list of one notification: owner first, then
// orchestrator, then coordinator. A missing owner (empty projeto or no
// registration) appends the missing_owner escalation and a no_route owner
// delivery; a missing orchestrator adds a no_route orchestrator delivery;
// the coordinator is a recipient only when the escalations are non-empty
// after the owner step. A ref already used by an earlier delivery gets the
// role appended to that delivery instead of a new one. isSelf marks a
// delivery whose ref is the source itself (loop guard); nil disables the
// guard. A delivery that has a ref and is not the source is pending, due at
// now.
func route(reg Registry, projeto, jobID string, escs []string, now time.Time, isSelf func(ref string) bool) ([]string, []Delivery) {
	out := make([]string, len(escs))
	copy(out, escs)
	var deliveries []Delivery
	add := func(role, ref string) {
		if ref == "" {
			deliveries = append(deliveries, Delivery{Roles: []string{role}, State: StateNoRoute})
			return
		}
		for i := range deliveries {
			if deliveries[i].Ref == ref {
				deliveries[i].Roles = append(deliveries[i].Roles, role)
				return
			}
		}
		state := StatePending
		if isSelf != nil && isSelf(ref) {
			state = StateSelf
		}
		d := Delivery{Roles: []string{role}, Ref: ref, State: state}
		if state == StatePending {
			d.NextAttemptAt = now.Format(TSLayout)
		}
		deliveries = append(deliveries, d)
	}
	ownerRef := reg.Owners[projeto]
	if projeto == "" || ownerRef == "" {
		out = append(out, EscMissingOwner)
		add(RoleOwner, "")
	} else {
		add(RoleOwner, ownerRef)
	}
	orchestratorRef := reg.Orchestrators[jobID]
	if jobID == "" || orchestratorRef == "" {
		add(RoleOrchestrator, "")
	} else {
		add(RoleOrchestrator, orchestratorRef)
	}
	if len(out) > 0 {
		add(RoleCoordinator, reg.Coordinator)
	}
	return out, deliveries
}

// isJSONObject reports whether b is a JSON object.
func isJSONObject(b []byte) bool {
	t := bytes.TrimSpace(b)
	return len(t) > 0 && t[0] == '{' && json.Valid(t)
}

// validSourceTS returns the source ts verbatim when it parses as RFC3339,
// else "" — a timestamp is never invented.
func validSourceTS(ts string) string {
	if ts == "" {
		return ""
	}
	if _, err := time.Parse(time.RFC3339, ts); err != nil {
		return ""
	}
	return ts
}
