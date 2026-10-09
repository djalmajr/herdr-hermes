package notify

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrNotAgentStatus is returned by ParseAgentStatusEvent when the envelope
// names another event (its event or type field carries another name).
var ErrNotAgentStatus = errors.New("notify: not an agent status event")

// Agent status names Herdr reports for a pane.
var agentStatuses = []string{"idle", "working", "blocked", "done", "unknown"}

// The two event names of the Herdr agent status change event: the socket
// subscription name (dotted) and the event-stream schema name (underscored).
const (
	agentStatusEventDotted      = "pane.agent_status_changed"
	agentStatusEventUnderscored = "pane_agent_status_changed"
)

// ParseAgentStatusEvent parses a Herdr agent status event. It accepts the
// socket subscription envelope {"event":"pane.agent_status_changed","data":{…}},
// the event-stream schema form {"event":"pane_agent_status_changed","data":{…}}
// and the flat data object {pane_id, workspace_id, agent_status, optional
// type}. Unknown fields (agent kind, title, display name, state labels) are
// discarded at parse time. An envelope that names another event returns
// ErrNotAgentStatus; malformed JSON or invalid fields return another error.
func ParseAgentStatusEvent(raw []byte) (AgentStatusEvent, error) {
	if !isJSONObject(raw) {
		return AgentStatusEvent{}, errors.New("notify: agent status event is not a JSON object")
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return AgentStatusEvent{}, fmt.Errorf("notify: decode agent status event: %w", err)
	}
	var data map[string]any
	if ev, present := m["event"]; present {
		name, isStr := ev.(string)
		if !isStr {
			return AgentStatusEvent{}, errors.New("notify: agent status event name is not a string")
		}
		if name != agentStatusEventDotted && name != agentStatusEventUnderscored {
			return AgentStatusEvent{}, ErrNotAgentStatus
		}
		d, ok := m["data"].(map[string]any)
		if !ok {
			return AgentStatusEvent{}, errors.New("notify: agent status event has no data object")
		}
		data = d
	} else {
		// Flat payload: the data fields sit at the top level.
		data = m
	}
	if t, present := data["type"]; present {
		if name, isStr := t.(string); isStr {
			if name != agentStatusEventUnderscored {
				return AgentStatusEvent{}, ErrNotAgentStatus
			}
		} else {
			return AgentStatusEvent{}, errors.New("notify: agent status type is not a string")
		}
	}
	pane, ok := data["pane_id"].(string)
	if !ok || !PanePattern.MatchString(pane) {
		return AgentStatusEvent{}, errors.New("notify: agent status pane_id is missing or invalid")
	}
	ws, ok := data["workspace_id"].(string)
	if !ok || !WorkspacePattern.MatchString(ws) {
		return AgentStatusEvent{}, errors.New("notify: agent status workspace_id is missing or invalid")
	}
	status, ok := data["agent_status"].(string)
	if !ok {
		return AgentStatusEvent{}, errors.New("notify: agent status agent_status is missing")
	}
	if !isAgentStatus(status) {
		return AgentStatusEvent{}, errors.New("notify: agent status agent_status is invalid")
	}
	return AgentStatusEvent{Pane: pane, Workspace: ws, Status: status}, nil
}

// isAgentStatus reports whether s is one of the five agent status names.
func isAgentStatus(s string) bool {
	for _, v := range agentStatuses {
		if v == s {
			return true
		}
	}
	return false
}
