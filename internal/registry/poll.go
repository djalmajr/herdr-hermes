package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/router"
)

// defaultPollTimeout bounds each herdr call when Poller.Timeout is zero
// or negative.
const defaultPollTimeout = 20 * time.Second

// pollVersionPattern bounds the value of a `version: <v>` line of
// `herdr status server`.
var pollVersionPattern = regexp.MustCompile(`^[0-9A-Za-z.+-]{1,40}$`)

// Poller is the read-only Herdr inventory poll: it runs, per machine,
// `herdr [--machine <label>] workspace list`, `agent list` and
// `status server` through the router Exec and reduces the answers to
// the allowlisted fields. It never starts, replays or moves anything.
type Poller struct {
	// Exec runs herdr; router.NewExec in production.
	Exec router.Exec
	// Timeout bounds each herdr call; 20 s when <= 0.
	Timeout time.Duration
	// Now is the clock; time.Now when nil.
	Now func() time.Time
}

// Poll polls one machine. A label that fails router.LabelPattern is
// refused with OK=false and Motivo=unknown_machine before any Exec
// call. The workspace list and the agent list must both succeed for
// OK; when the workspace list fails the agent list is not run. The
// status server failing in any way only leaves HerdrVersion empty. At
// is taken when the poll of this machine starts.
func (p Poller) Poll(ctx context.Context, maquina string) PollResult {
	now := p.Now
	if now == nil {
		now = time.Now
	}
	res := PollResult{Maquina: maquina, At: now()}
	if !router.LabelPattern.MatchString(maquina) {
		res.Motivo = router.ReasonUnknownMachine
		return res
	}
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = defaultPollTimeout
	}
	prefix := []string{}
	if maquina != router.Local {
		prefix = []string{"--machine", maquina}
	}

	var wsList []PolledWorkspace
	var paneList []PolledPane
	wsRaw, reason := p.run(ctx, timeout, append(prefix, "workspace", "list"))
	if reason == "" {
		ws, err := ParseWorkspaceList(wsRaw)
		if err != nil {
			reason = router.ReasonProbeMalformed
		} else {
			wsList = ws
		}
	}
	if reason == "" {
		agRaw, agReason := p.run(ctx, timeout, append(prefix, "agent", "list"))
		if agReason == "" {
			panes, err := ParseAgentList(agRaw)
			if err != nil {
				agReason = router.ReasonProbeMalformed
			} else {
				paneList = panes
			}
		}
		reason = agReason
	}

	// The status server is independent of the lists: a failure in any
	// way only leaves the version empty.
	stRaw, stReason := p.run(ctx, timeout, append(prefix, "status", "server"))
	if stReason == "" {
		res.HerdrVersion = ParseServerVersion(stRaw)
	}

	res.OK = reason == ""
	res.Motivo = reason
	// A failed poll carries no lists: the stored state is never
	// inferred from a partial answer.
	if res.OK {
		res.Workspaces = wsList
		res.Panes = paneList
	}
	return res
}

// PollAll polls every machine concurrently, one goroutine per machine,
// and returns the results in input order.
func (p Poller) PollAll(ctx context.Context, machines []string) []PollResult {
	out := make([]PollResult, len(machines))
	if len(machines) == 0 {
		return out
	}
	var wg sync.WaitGroup
	for i, m := range machines {
		wg.Add(1)
		go func(i int, m string) {
			defer wg.Done()
			out[i] = p.Poll(ctx, m)
		}(i, m)
	}
	wg.Wait()
	return out
}

// run runs one herdr call under its own deadline and maps the outcome
// like the router: it returns the stdout on success and a reason code
// otherwise.
func (p Poller) run(ctx context.Context, timeout time.Duration, argv []string) ([]byte, string) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	stdout, code, err := p.Exec(cctx, argv)
	cancel()
	switch {
	case err != nil && errors.Is(err, router.ErrProbeTimeout):
		return nil, router.ReasonProbeTimeout
	case err != nil && errors.Is(err, router.ErrHerdrUnavailable):
		return nil, router.ReasonHerdrUnavailable
	case err != nil || code != 0:
		return nil, router.ReasonProbeFailed
	case len(stdout) > router.MaxProbeOutput:
		return nil, router.ReasonProbeMalformed
	}
	return stdout, ""
}

// agentStatuses are the recognized Herdr agent statuses; anything else
// is a malformed answer.
var agentStatuses = map[string]bool{"idle": true, "working": true, "blocked": true, "done": true, "unknown": true}

// ParseWorkspaceList parses the stdout of `herdr workspace list`: the
// Herdr response {"id":...,"result":{"type":"workspace_list",
// "workspaces":[...]}}. A top-level error key, a missing or non-object
// result, a missing or non-array workspaces, an element that is not an
// object, a missing, empty or non-string workspace_id, a non-string
// label (missing label is an empty string), a missing or non-string
// agent_status outside idle|working|blocked|done|unknown, or trailing
// non-whitespace data is an error. Unknown fields are ignored; only
// the id, label and agent_status of each workspace are kept.
func ParseWorkspaceList(raw []byte) ([]PolledWorkspace, error) {
	topRaw, err := firstJSONValue(raw, "workspace list")
	if err != nil {
		return nil, err
	}
	top, err := objectValue(topRaw)
	if err != nil {
		return nil, errors.New("registry: workspace list top level is not an object")
	}
	if _, ok := top["error"]; ok {
		return nil, errors.New("registry: workspace list is a herdr error response")
	}
	resRaw, ok := top["result"]
	if !ok {
		return nil, errors.New("registry: workspace list result is missing")
	}
	res, err := objectValue(resRaw)
	if err != nil {
		return nil, errors.New("registry: workspace list result is not an object")
	}
	wsRaw, ok := res["workspaces"]
	if !ok {
		return nil, errors.New("registry: workspace list workspaces is missing")
	}
	if !isArrayValue(wsRaw) {
		return nil, errors.New("registry: workspace list workspaces is not an array")
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(wsRaw, &elements); err != nil {
		return nil, fmt.Errorf("registry: workspace list workspaces is malformed: %w", err)
	}
	out := make([]PolledWorkspace, 0, len(elements))
	for i, elemRaw := range elements {
		elem, err := objectValue(elemRaw)
		if err != nil {
			return nil, fmt.Errorf("registry: workspace %d is not an object", i)
		}
		id, err := stringField(elem, "workspace_id")
		if err != nil {
			return nil, fmt.Errorf("registry: workspace %d: %w", i, err)
		}
		if id == "" {
			return nil, fmt.Errorf("registry: workspace %d: workspace_id is empty", i)
		}
		label := ""
		if rawLabel, present := elem["label"]; present {
			trimmed := bytes.TrimSpace(rawLabel)
			if !bytes.HasPrefix(trimmed, []byte{'"'}) {
				return nil, fmt.Errorf("registry: workspace %d: label must be a string", i)
			}
			if err := json.Unmarshal(rawLabel, &label); err != nil {
				return nil, fmt.Errorf("registry: workspace %d: label must be a string", i)
			}
		}
		status, err := stringField(elem, "agent_status")
		if err != nil {
			return nil, fmt.Errorf("registry: workspace %d: %w", i, err)
		}
		if !agentStatuses[status] {
			return nil, fmt.Errorf("registry: workspace %d: agent_status %q is not recognized", i, status)
		}
		out = append(out, PolledWorkspace{ID: id, Label: label, AgentStatus: status})
	}
	return out, nil
}

// ParseAgentList parses the stdout of `herdr agent list` with the same
// shape rules as the router agent list: the Herdr response
// {"id":...,"result":{"agents":[...]}}, a non-empty string pane_id, an
// agent_status inside idle|working|blocked|done|unknown, a name that is
// a string or null (missing is an empty string) and a workspace_id that
// is a string (missing is an empty string). Only the pane id, the
// workspace id, the name and the status are kept: never the cwd, the
// titles, the terminal ids or the sessions.
func ParseAgentList(raw []byte) ([]PolledPane, error) {
	topRaw, err := firstJSONValue(raw, "agent list")
	if err != nil {
		return nil, err
	}
	top, err := objectValue(topRaw)
	if err != nil {
		return nil, errors.New("registry: agent list top level is not an object")
	}
	if _, ok := top["error"]; ok {
		return nil, errors.New("registry: agent list is a herdr error response")
	}
	resRaw, ok := top["result"]
	if !ok {
		return nil, errors.New("registry: agent list result is missing")
	}
	res, err := objectValue(resRaw)
	if err != nil {
		return nil, errors.New("registry: agent list result is not an object")
	}
	agentsRaw, ok := res["agents"]
	if !ok {
		return nil, errors.New("registry: agent list agents is missing")
	}
	if !isArrayValue(agentsRaw) {
		return nil, errors.New("registry: agent list agents is not an array")
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(agentsRaw, &elements); err != nil {
		return nil, fmt.Errorf("registry: agent list agents is malformed: %w", err)
	}
	out := make([]PolledPane, 0, len(elements))
	for i, elemRaw := range elements {
		elem, err := objectValue(elemRaw)
		if err != nil {
			return nil, fmt.Errorf("registry: agent %d is not an object", i)
		}
		paneID, err := stringField(elem, "pane_id")
		if err != nil {
			return nil, fmt.Errorf("registry: agent %d: %w", i, err)
		}
		if paneID == "" {
			return nil, fmt.Errorf("registry: agent %d: pane_id is empty", i)
		}
		status, err := stringField(elem, "agent_status")
		if err != nil {
			return nil, fmt.Errorf("registry: agent %d: %w", i, err)
		}
		if !agentStatuses[status] {
			return nil, fmt.Errorf("registry: agent %d: agent_status %q is not recognized", i, status)
		}
		name := ""
		if rawName, present := elem["name"]; present {
			trimmed := bytes.TrimSpace(rawName)
			if !bytes.Equal(trimmed, []byte("null")) && !bytes.HasPrefix(trimmed, []byte{'"'}) {
				return nil, fmt.Errorf("registry: agent %d: name must be a string or null", i)
			}
			if !bytes.Equal(trimmed, []byte("null")) {
				if err := json.Unmarshal(rawName, &name); err != nil {
					return nil, fmt.Errorf("registry: agent %d: name must be a string or null", i)
				}
			}
		}
		wsID := ""
		if rawWS, present := elem["workspace_id"]; present {
			trimmed := bytes.TrimSpace(rawWS)
			if !bytes.HasPrefix(trimmed, []byte{'"'}) {
				return nil, fmt.Errorf("registry: agent %d: workspace_id must be a string", i)
			}
			if err := json.Unmarshal(rawWS, &wsID); err != nil {
				return nil, fmt.Errorf("registry: agent %d: workspace_id must be a string", i)
			}
		}
		out = append(out, PolledPane{PaneID: paneID, WorkspaceID: wsID, Name: name, AgentStatus: status})
	}
	return out, nil
}

// ParseServerVersion extracts the version from the text output of
// `herdr status server`: the first line of the form `version: <v>`
// yields v trimmed, when it matches the version pattern; otherwise the
// result is empty.
func ParseServerVersion(raw []byte) string {
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "version:") {
			continue
		}
		v := strings.TrimSpace(line[len("version:"):])
		if !pollVersionPattern.MatchString(v) {
			return ""
		}
		return v
	}
	return ""
}

// firstJSONValue decodes raw as a single JSON value and fails when
// non-whitespace data follows it.
func firstJSONValue(raw []byte, what string) (json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	var head json.RawMessage
	if err := dec.Decode(&head); err != nil {
		return nil, fmt.Errorf("registry: %s is not valid JSON: %w", what, err)
	}
	// A second Decode must see a clean end of input: dec.More alone
	// accepts a stray closing bracket or brace after the value.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("registry: %s has trailing data", what)
	}
	return head, nil
}

// isArrayValue reports whether raw is a JSON array.
func isArrayValue(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '['
}

// objectValue decodes a raw JSON value that must be an object.
func objectValue(raw json.RawMessage) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("not an object")
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

// stringField returns the value of key as a string. The key must be
// present and a string: absent keys, null and other types are errors.
func stringField(obj map[string]json.RawMessage, key string) (string, error) {
	raw, ok := obj[key]
	if !ok {
		return "", fmt.Errorf("missing %q", key)
	}
	trimmed := bytes.TrimSpace(raw)
	if !bytes.HasPrefix(trimmed, []byte{'"'}) {
		return "", fmt.Errorf("%q must be a string", key)
	}
	var v string
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", fmt.Errorf("%q must be a string", key)
	}
	return v, nil
}
