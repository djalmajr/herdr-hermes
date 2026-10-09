// Package plugin holds the pure logic behind the Herdr plugin entry points
// (slice 4b): the workspace event JSON shape and the job-<id> workspace
// label rule. The CLI glue lives in internal/cli (plugin_cmd.go); this
// package reads no environment, opens no file and prints nothing.
package plugin

import (
	"encoding/json"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
)

// The two Herdr-provided event variables the plugin event entry point
// reads. No other environment variable names exist in the bridge.
const (
	EnvEvent     = "HERDR_PLUGIN_EVENT"
	EnvEventJSON = "HERDR_PLUGIN_EVENT_JSON"
)

// ParseEvent extracts the workspace label and workspace id from the
// HERDR_PLUGIN_EVENT_JSON value. It accepts the nested shape
// {"workspace":{"label":…,"workspace_id":…}} and the flat top-level shape
// {"label":…,"workspace_id":…}; when both are present the nested one wins.
// ok is false when raw is not a JSON object or carries no non-empty string
// label.
//
// TODO(herdr-hermes): verify the HERDR_PLUGIN_EVENT_JSON shape for
// workspace.created and workspace.closed against a real Herdr.
func ParseEvent(raw string) (label, workspaceID string, ok bool) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err != nil || obj == nil {
		return "", "", false
	}
	if wsRaw, has := obj["workspace"]; has {
		var ws map[string]json.RawMessage
		if err := json.Unmarshal(wsRaw, &ws); err == nil && ws != nil {
			if l, okL := stringField(ws["label"]); okL {
				w, _ := stringField(ws["workspace_id"])
				return l, w, true
			}
		}
	}
	l, okL := stringField(obj["label"])
	if !okL {
		return "", "", false
	}
	w, _ := stringField(obj["workspace_id"])
	return l, w, true
}

// stringField unmarshals a non-empty string field.
func stringField(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil || s == "" {
		return "", false
	}
	return s, true
}

// JobIDFromLabel reports whether the workspace label is "job-<id>" with an
// id that passes the contract id rule, and returns the id.
func JobIDFromLabel(label string) (string, bool) {
	id, hasPrefix := strings.CutPrefix(label, "job-")
	if !hasPrefix || !jobapi.ValidID(id) {
		return "", false
	}
	return id, true
}
