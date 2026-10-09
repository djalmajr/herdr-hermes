package notify

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
)

// ErrNotWorkspace is returned by ParseWorkspaceEvent when the envelope
// (event, data type or hook) names another event.
var ErrNotWorkspace = errors.New("notify: not a workspace created/closed event")

// The four event names of the Herdr workspace events: the socket
// subscription names (dotted) and the event-stream schema names
// (underscored).
const (
	workspaceCreatedDotted      = "workspace.created"
	workspaceCreatedUnderscored = "workspace_created"
	workspaceClosedDotted       = "workspace.closed"
	workspaceClosedUnderscored  = "workspace_closed"
)

// ParseWorkspaceEvent parses a Herdr workspace event. It accepts the
// socket subscription line {"event":"workspace.created"|"workspace_created"
// |"workspace.closed"|"workspace_closed","data":{…}} where data is the
// event-stream form, the event-stream form
// {"type":"workspace_created","workspace":{…}} and
// {"type":"workspace_closed","workspace_id":…,"workspace":null|{…}} (the
// bundled API schema), and the existing plugin payload shapes — nested
// {"workspace":{"label":…,"workspace_id":…}} or flat
// {"label":…,"workspace_id":…} — whose event name comes from hook
// ("workspace.created" or "workspace.closed") when the JSON carries no
// name of its own. The event name is the envelope event, else the data
// type, else hook; any other name returns ErrNotWorkspace and a missing
// name a plain error. The workspace id is workspace.workspace_id, else
// the top-level (or data-level) workspace_id, and must match
// WorkspacePattern. The label is workspace.label, else the top-level
// label; it may be empty only for a closed event (a created event without
// a label is an error). Unknown fields (agent kinds, titles, tokens,
// worktree, counts) are discarded at parse time.
func ParseWorkspaceEvent(raw []byte, hook string) (WorkspaceEvent, error) {
	if !isJSONObject(raw) {
		return WorkspaceEvent{}, errors.New("notify: workspace event is not a JSON object")
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return WorkspaceEvent{}, fmt.Errorf("notify: decode workspace event: %w", err)
	}
	var data map[string]any
	var envelope string
	if ev, present := m["event"]; present {
		name, isStr := ev.(string)
		if !isStr {
			return WorkspaceEvent{}, errors.New("notify: workspace event name is not a string")
		}
		envelope = name
		d, ok := m["data"].(map[string]any)
		if !ok {
			return WorkspaceEvent{}, errors.New("notify: workspace event has no data object")
		}
		data = d
	} else {
		// No envelope: the event-stream form or a plugin payload; the
		// data fields sit at the top level.
		data = m
	}
	name := envelope
	if name == "" {
		switch t, present := data["type"]; {
		case present:
			ts, isStr := t.(string)
			if !isStr {
				return WorkspaceEvent{}, errors.New("notify: workspace event type is not a string")
			}
			name = ts
		case hook != "":
			name = hook
		default:
			return WorkspaceEvent{}, errors.New("notify: workspace event name is missing")
		}
	}
	var kind string
	switch name {
	case workspaceCreatedDotted, workspaceCreatedUnderscored:
		kind = WorkspaceCreated
	case workspaceClosedDotted, workspaceClosedUnderscored:
		kind = WorkspaceClosed
	default:
		return WorkspaceEvent{}, ErrNotWorkspace
	}
	var ws map[string]any
	if w, present := data["workspace"]; present {
		ws, _ = w.(map[string]any) // null or non-object: no workspace object
	}
	wsID, _, err := workspaceField([]map[string]any{ws, data, m}, "workspace_id")
	if err != nil {
		return WorkspaceEvent{}, err
	}
	if !WorkspacePattern.MatchString(wsID) {
		return WorkspaceEvent{}, errors.New("notify: workspace id is missing or invalid")
	}
	label, _, err := workspaceField([]map[string]any{ws, data, m}, "label")
	if err != nil {
		return WorkspaceEvent{}, err
	}
	if kind == WorkspaceCreated && label == "" {
		return WorkspaceEvent{}, errors.New("notify: workspace created event has no label")
	}
	return WorkspaceEvent{Event: kind, Workspace: wsID, Label: label}, nil
}

// workspaceField returns the value of key from the first map (in order)
// that carries the key. It errors when the first present value is not a
// string; "" with no error when no map carries the key.
func workspaceField(maps []map[string]any, key string) (string, bool, error) {
	for _, m := range maps {
		if m == nil {
			continue
		}
		v, present := m[key]
		if !present {
			continue
		}
		s, isStr := v.(string)
		if !isStr {
			return "", false, fmt.Errorf("notify: workspace field %q is not a string", key)
		}
		return s, true, nil
	}
	return "", false, nil
}

// ProjectWorkspace projects one true workspace transition. It errors when
// the event is not WorkspaceCreated/WorkspaceClosed, in.Workspace fails
// WorkspacePattern, in.N is below 1, or in.JobID is non-empty and fails
// jobapi.ValidID. JobID may be empty (a registered workspace without a
// job): the orchestrator delivery is then no_route as usual. An invalid
// projeto is dropped (routed as missing), and routing has no class
// escalations and no self guard: a missing owner still adds missing_owner
// and the coordinator, as everywhere. The source id carries the workspace
// id and the per-workspace transition number, so a later reopen of the
// same workspace id is a distinct notification.
func ProjectWorkspace(in WorkspaceInput, reg Registry, now time.Time) (*Notification, error) {
	var (
		class  string
		suffix string
	)
	switch in.Event {
	case WorkspaceCreated:
		class, suffix = ClassWorkspaceOpened, "opened"
	case WorkspaceClosed:
		class, suffix = ClassWorkspaceClosed, "closed"
	default:
		return nil, fmt.Errorf("notify: workspace event %q is not created or closed", in.Event)
	}
	if !WorkspacePattern.MatchString(in.Workspace) {
		return nil, errors.New("notify: workspace id is missing or invalid")
	}
	if in.N < 1 {
		return nil, errors.New("notify: workspace transition number must be positive")
	}
	if in.JobID != "" && !jobapi.ValidID(in.JobID) {
		return nil, errors.New("notify: workspace job id is invalid")
	}
	projeto := in.Projeto
	if !jobapi.ValidRepo(projeto) {
		projeto = ""
	}
	n := &Notification{
		SourceKind: SourceWorkspace,
		SourceID:   "workspace:" + in.Workspace + ":" + strconv.FormatInt(in.N, 10) + ":" + suffix,
		Class:      class,
		JobID:      in.JobID,
		Projeto:    projeto,
		Workspace:  in.Workspace,
		ObservedAt: now.Format(TSLayout),
	}
	n.ID = NotificationID(n.SourceID)
	n.Escalations, n.Deliveries = route(reg, projeto, in.JobID, nil, now, nil)
	return n, nil
}
