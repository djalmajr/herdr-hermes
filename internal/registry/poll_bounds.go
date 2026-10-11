package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode/utf8"
)

// truncationMarkerPrefix opens the ASCII marker that ends a bounded
// polled string: "[truncated:" + the 64 lower-case hex digits of the
// SHA-256 of the original bytes + "]".
const truncationMarkerPrefix = "[truncated:"

// truncationMarkerLen is the length of the whole marker in bytes.
const truncationMarkerLen = len(truncationMarkerPrefix) + 64 + 1

// boundPolledString returns s unchanged when it is valid UTF-8 of at most
// limit bytes; otherwise the longest valid UTF-8 prefix that leaves room
// for the marker, followed by the marker of the original bytes. The
// result is always valid UTF-8 and at most limit bytes: an invalid input
// carries its invalid bytes as U+FFFD inside the prefix, and the digest
// is of the original bytes, so two values that share a prefix still get
// different markers. Bounding a bounded result returns it unchanged.
func boundPolledString(s string, limit int) string {
	if utf8.ValidString(s) && len(s) <= limit {
		return s
	}
	body := strings.ToValidUTF8(s, "\ufffd")
	maxPrefix := limit - truncationMarkerLen
	if maxPrefix < 0 {
		maxPrefix = 0
	}
	prefix := body
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
		for len(prefix) > 0 && !utf8.ValidString(prefix) {
			// The cut fell inside a rune: drop the partial tail.
			prefix = prefix[:len(prefix)-1]
		}
	}
	sum := sha256.Sum256([]byte(s))
	return prefix + truncationMarkerPrefix + hex.EncodeToString(sum[:]) + "]"
}

// normalizePollResult returns a bounded deep copy of p: every stored
// string is bounded (Maquina stays untouched, validateMachine ran
// before), HerdrVersion is kept only when it matches the parser's
// pattern, and a workspace or pane with an empty id or an unknown
// status is an ErrInvalid wrap. A failed poll drops its lists without
// reading them: whatever they carry, the failure is recorded and the
// node is aged. The caller's PollResult, slices and strings are never
// written, on success or on failure.
func normalizePollResult(p PollResult) (PollResult, error) {
	out := p
	out.Motivo = boundPolledString(p.Motivo, MaxPolledStringBytes)
	if !pollVersionPattern.MatchString(p.HerdrVersion) {
		// Stored as null: the stored version is kept.
		out.HerdrVersion = ""
	}
	if !p.OK {
		// The lists of a failed poll are dropped without being
		// validated or applied.
		out.Workspaces = nil
		out.Panes = nil
		return out, nil
	}
	out.Workspaces = append([]PolledWorkspace(nil), p.Workspaces...)
	for i, ws := range out.Workspaces {
		id := boundPolledString(ws.ID, MaxPolledStringBytes)
		if id == "" {
			return out, invalidErr("workspaces", "workspace_id is empty")
		}
		if !agentStatuses[ws.AgentStatus] {
			return out, invalidErr("workspaces", "agent_status is not recognized")
		}
		out.Workspaces[i] = PolledWorkspace{ID: id, Label: boundPolledString(ws.Label, MaxPolledStringBytes), AgentStatus: ws.AgentStatus}
	}
	out.Panes = append([]PolledPane(nil), p.Panes...)
	for i, pane := range out.Panes {
		paneID := boundPolledString(pane.PaneID, MaxPolledPaneBytes)
		if paneID == "" {
			return out, invalidErr("panes", "pane_id is empty")
		}
		if !agentStatuses[pane.AgentStatus] {
			return out, invalidErr("panes", "agent_status is not recognized")
		}
		out.Panes[i] = PolledPane{PaneID: paneID, WorkspaceID: boundPolledString(pane.WorkspaceID, MaxPolledStringBytes), Name: boundPolledString(pane.Name, MaxPolledPaneBytes), AgentStatus: pane.AgentStatus}
	}
	return out, nil
}
