package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"time"
)

// defaultProbeTimeout bounds each probe when Fleet.Timeout is zero or
// negative.
const defaultProbeTimeout = 20 * time.Second

// CatalogEntry is one saved Herdr machine from the stdout of
// `herdr machine list --json`.
type CatalogEntry struct {
	ID      string
	Label   string
	Enabled bool
}

// ParseCatalog parses the stdout of `herdr machine list --json`: a JSON
// array of objects, every one with a string id, a string label and a bool
// enabled. Unknown fields (target, session, selected, ...) are ignored.
// Anything else is an error: not an array, an element that is not an
// object, a missing or wrongly typed field, invalid JSON, or trailing
// non-whitespace data after the array. The empty array is valid.
func ParseCatalog(raw []byte) ([]CatalogEntry, error) {
	elements, err := firstJSONValue(raw, "catalog")
	if err != nil {
		return nil, err
	}
	if !isArrayValue(elements) {
		return nil, errors.New("router: catalog is not a JSON array")
	}
	var rawElements []json.RawMessage
	if err := json.Unmarshal(elements, &rawElements); err != nil {
		return nil, fmt.Errorf("router: catalog array is malformed: %w", err)
	}
	entries := make([]CatalogEntry, 0, len(rawElements))
	for i, elem := range rawElements {
		obj, err := objectValue(elem)
		if err != nil {
			return nil, fmt.Errorf("router: catalog element %d is not an object", i)
		}
		id, err := stringField(obj, "id")
		if err != nil {
			return nil, fmt.Errorf("router: catalog element %d: %w", i, err)
		}
		label, err := stringField(obj, "label")
		if err != nil {
			return nil, fmt.Errorf("router: catalog element %d: %w", i, err)
		}
		enabled, err := boolField(obj, "enabled")
		if err != nil {
			return nil, fmt.Errorf("router: catalog element %d: %w", i, err)
		}
		entries = append(entries, CatalogEntry{ID: id, Label: label, Enabled: enabled})
	}
	return entries, nil
}

// CountOrchestrators counts, in the stdout of `herdr agent list`, the
// agents whose name is base or base followed by -<n> (n >= 1), whatever
// their status. The shape is the Herdr API response
// {"id":...,"result":{"agents":[...]}}: a top-level error object, a
// missing or malformed result.agents array, an agent that is not an
// object, an agent whose pane_id is missing, not a string or empty, an
// agent_status that is missing, not a string or outside
// idle|working|blocked|done|unknown, a name that is neither a string nor
// null, or trailing non-whitespace data is an error. The empty agents
// array is valid and returns 0.
func CountOrchestrators(agentList []byte, base string) (int, error) {
	topRaw, err := firstJSONValue(agentList, "agent list")
	if err != nil {
		return 0, err
	}
	top, err := objectValue(topRaw)
	if err != nil {
		return 0, errors.New("router: agent list top level is not an object")
	}
	if _, ok := top["error"]; ok {
		return 0, errors.New("router: agent list is a herdr error response")
	}
	resRaw, ok := top["result"]
	if !ok {
		return 0, errors.New("router: agent list result is missing")
	}
	res, err := objectValue(resRaw)
	if err != nil {
		return 0, errors.New("router: agent list result is not an object")
	}
	agentsRaw, ok := res["agents"]
	if !ok {
		return 0, errors.New("router: agent list agents is missing")
	}
	if !isArrayValue(agentsRaw) {
		return 0, errors.New("router: agent list agents is not an array")
	}
	var agents []json.RawMessage
	if err := json.Unmarshal(agentsRaw, &agents); err != nil {
		return 0, fmt.Errorf("router: agent list agents is malformed: %w", err)
	}
	re := regexp.MustCompile(`^` + regexp.QuoteMeta(base) + `(-[1-9][0-9]*)?$`)
	count := 0
	for i, agentRaw := range agents {
		agent, err := objectValue(agentRaw)
		if err != nil {
			return 0, fmt.Errorf("router: agent %d is not an object", i)
		}
		paneID, err := stringField(agent, "pane_id")
		if err != nil {
			return 0, fmt.Errorf("router: agent %d: %w", i, err)
		}
		if paneID == "" {
			return 0, fmt.Errorf("router: agent %d: pane_id is empty", i)
		}
		status, err := stringField(agent, "agent_status")
		if err != nil {
			return 0, fmt.Errorf("router: agent %d: %w", i, err)
		}
		switch status {
		case "idle", "working", "blocked", "done", "unknown":
		default:
			return 0, fmt.Errorf("router: agent %d: agent_status %q is not recognized", i, status)
		}
		name := ""
		if raw, present := agent["name"]; present {
			trimmed := bytes.TrimSpace(raw)
			if !bytes.Equal(trimmed, []byte("null")) && !bytes.HasPrefix(trimmed, []byte{'"'}) {
				return 0, fmt.Errorf("router: agent %d: name must be a string or null", i)
			}
			if !bytes.Equal(trimmed, []byte("null")) {
				if err := json.Unmarshal(raw, &name); err != nil {
					return 0, fmt.Errorf("router: agent %d: name must be a string or null", i)
				}
			}
		}
		if re.MatchString(name) {
			count++
		}
	}
	return count, nil
}

// Probe probes every configured machine read-only (only f.Exec runs) and
// returns one candidate per machine, in configured order.
//
// A label that does not match LabelPattern is unsupported and a label in
// Disabled is disabled; neither is ever passed to the Exec. When at least
// one remaining label is not Local the catalog
// (machine list --json) runs exactly once: a catalog failure marks every
// remaining non-local label unavailable with the mapped reason, a label
// absent from the catalog is unsupported, one listed more than once is
// ambiguous, and one saved with enabled false is disabled. Local needs
// no catalog. Every remaining label then gets one agent-list probe, all
// of them concurrently. A label whose availability or load cannot be
// established is unavailable with the mapped reason; nothing is ever
// inferred from a failed or malformed probe.
func (f Fleet) Probe(ctx context.Context) []Candidate {
	timeout := f.Timeout
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}
	base := f.OrchestratorName
	if base == "" {
		base = DefaultOrchestratorName
	}
	out := make([]Candidate, len(f.Machines))

	var pending []pendingProbe
	needCatalog := false
	for i, label := range f.Machines {
		switch {
		case !LabelPattern.MatchString(label):
			out[i] = Candidate{Machine: label, State: StateUnsupported, Reason: ReasonUnknownMachine}
		case containsLabel(f.Disabled, label):
			out[i] = Candidate{Machine: label, State: StateDisabled, Reason: ReasonDisabledByConfig}
		default:
			argv := []string{"agent", "list"}
			if label != Local {
				needCatalog = true
				argv = []string{"--machine", label, "agent", "list"}
			}
			pending = append(pending, pendingProbe{index: i, label: label, argv: argv})
		}
	}

	if needCatalog {
		catalogCtx, cancel := context.WithTimeout(ctx, timeout)
		stdout, stderr, code, err := f.Exec(catalogCtx, []string{"machine", "list", "--json"})
		cancel()
		reason := catalogReason(err, code, stdout)
		if reason != "" {
			// A failed catalog marks every remaining non-local label
			// unavailable; local needs no catalog and keeps its own
			// probe. When herdr exited on its own, the shared cause and
			// exit code come from its capped output.
			cause, exit := "", 0
			if reason == ReasonCatalogUnavailable && err == nil {
				cause = ClassifyFailure(stdout, stderr)
				exit = code
			}
			var keep []pendingProbe
			for _, p := range pending {
				if p.label == Local {
					keep = append(keep, p)
					continue
				}
				out[p.index] = Candidate{Machine: p.label, State: StateUnavailable, Reason: reason, Cause: cause, ExitCode: exit}
			}
			pending = keep
		} else {
			entries, err := ParseCatalog(stdout)
			if err != nil {
				var keep []pendingProbe
				for _, p := range pending {
					if p.label == Local {
						keep = append(keep, p)
						continue
					}
					out[p.index] = Candidate{Machine: p.label, State: StateUnavailable, Reason: ReasonCatalogMalformed}
				}
				pending = keep
			} else {
				var keep []pendingProbe
				for _, p := range pending {
					if p.label == Local {
						keep = append(keep, p)
						continue
					}
					if state, reason, ok := resolveCatalog(entries, p.label); !ok {
						out[p.index] = Candidate{Machine: p.label, State: state, Reason: reason}
						continue
					}
					keep = append(keep, p)
				}
				pending = keep
			}
		}
	}

	if len(pending) == 0 {
		return out
	}
	type probeResult struct {
		index int
		cand  Candidate
	}
	results := make(chan probeResult, len(pending))
	var wg sync.WaitGroup
	for _, p := range pending {
		wg.Add(1)
		go func(p pendingProbe) {
			defer wg.Done()
			results <- probeResult{index: p.index, cand: f.probeLoad(ctx, timeout, base, p)}
		}(p)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for r := range results {
		out[r.index] = r.cand
	}
	return out
}

// pendingProbe is one configured label still to be probed for load: its
// index in the fleet, its label and the exact agent-list argv.
type pendingProbe struct {
	index int
	label string
	argv  []string
}

// catalogReason maps a catalog probe outcome to the reason code of the
// failed state, or "" on success.
func catalogReason(err error, code int, stdout []byte) string {
	switch {
	case err != nil && errors.Is(err, ErrHerdrUnavailable):
		return ReasonHerdrUnavailable
	case err != nil || code != 0:
		return ReasonCatalogUnavailable
	case len(stdout) > MaxProbeOutput:
		return ReasonCatalogMalformed
	}
	return ""
}

// resolveCatalog maps one non-local label against the catalog: exactly
// one enabled entry is resolvable (ok), everything else returns the
// mapped state and reason.
func resolveCatalog(entries []CatalogEntry, label string) (State, string, bool) {
	matches := 0
	enabled := false
	for _, e := range entries {
		if e.Label == label {
			matches++
			enabled = e.Enabled
		}
	}
	switch {
	case matches == 0:
		return StateUnsupported, ReasonUnknownMachine, false
	case matches > 1:
		return StateUnsupported, ReasonAmbiguousMachine, false
	case !enabled:
		return StateDisabled, ReasonMachineDisabled, false
	}
	return "", "", true
}

// probeLoad runs one agent-list probe for p and maps the outcome: a
// well-formed agent list is available with its orchestrator count;
// anything else is unavailable with the mapped reason.
func (f Fleet) probeLoad(ctx context.Context, timeout time.Duration, base string, p pendingProbe) Candidate {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	stdout, stderr, code, err := f.Exec(cctx, p.argv)
	cancel()
	switch {
	case err != nil && errors.Is(err, ErrProbeTimeout):
		return Candidate{Machine: p.label, State: StateUnavailable, Reason: ReasonProbeTimeout}
	case err != nil && errors.Is(err, ErrHerdrUnavailable):
		return Candidate{Machine: p.label, State: StateUnavailable, Reason: ReasonHerdrUnavailable}
	case err != nil || code != 0:
		cand := Candidate{Machine: p.label, State: StateUnavailable, Reason: ReasonProbeFailed}
		if err == nil {
			// Herdr exited on its own: classify the capped output and
			// keep the non-zero exit code.
			cand.Cause = ClassifyFailure(stdout, stderr)
			cand.ExitCode = code
		}
		return cand
	case len(stdout) > MaxProbeOutput:
		return Candidate{Machine: p.label, State: StateUnavailable, Reason: ReasonProbeMalformed}
	}
	count, err := CountOrchestrators(stdout, base)
	if err != nil {
		return Candidate{Machine: p.label, State: StateUnavailable, Reason: ReasonProbeMalformed}
	}
	n := count
	return Candidate{Machine: p.label, State: StateAvailable, Orchestrators: &n}
}

// containsLabel reports whether labels holds label (exact match).
func containsLabel(labels []string, label string) bool {
	for _, l := range labels {
		if l == label {
			return true
		}
	}
	return false
}

// firstJSONValue decodes raw as a single JSON value and fails when
// non-whitespace data follows it.
func firstJSONValue(raw []byte, what string) (json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	var head json.RawMessage
	if err := dec.Decode(&head); err != nil {
		return nil, fmt.Errorf("router: %s is not valid JSON: %w", what, err)
	}
	// A second Decode must see a clean end of input: dec.More alone
	// accepts a stray closing bracket or brace after the value.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("router: %s has trailing data", what)
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

// boolField returns the value of key as a bool. The key must be present
// and a JSON true/false: absent keys, null and other types are errors.
func boolField(obj map[string]json.RawMessage, key string) (bool, error) {
	raw, ok := obj[key]
	if !ok {
		return false, fmt.Errorf("missing %q", key)
	}
	trimmed := bytes.TrimSpace(raw)
	if string(trimmed) != "true" && string(trimmed) != "false" {
		return false, fmt.Errorf("%q must be a bool", key)
	}
	var v bool
	if err := json.Unmarshal(raw, &v); err != nil {
		return false, fmt.Errorf("%q must be a bool", key)
	}
	return v, nil
}
