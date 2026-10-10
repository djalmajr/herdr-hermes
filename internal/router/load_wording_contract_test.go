package router_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/djalmajr/herdr-hermes/internal/router"
)

// lwAgent is one agent entry for lwAgentsJSON. A nil name with nullName
// set renders "name": null; a nil name without it is an absent field.
type lwAgent struct {
	pane     string
	status   string
	name     *string
	nullName bool
}

// lwName returns a pointer to s.
func lwName(s string) *string { return &s }

// lwAgentsJSON builds the stdout of `herdr agent list`: the Herdr API
// response with the given agents, each with a distinct non-empty pane_id.
func lwAgentsJSON(agents ...lwAgent) []byte {
	var b strings.Builder
	b.WriteString(`{"id":"cli:agent:list","result":{"agents":[`)
	for i, a := range agents {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"pane_id":%q,"agent_status":%q`, a.pane, a.status)
		if a.name != nil {
			fmt.Fprintf(&b, `,"name":%q`, *a.name)
		} else if a.nullName {
			b.WriteString(`,"name":null`)
		}
		b.WriteByte('}')
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

// lwCount fails the test unless CountOrchestrators returns want for raw.
func lwCount(t *testing.T, raw []byte, base string, want int) {
	t.Helper()
	got, err := router.CountOrchestrators(raw, base)
	if err != nil || got != want {
		t.Errorf("CountOrchestrators(base %q) = %d, %v; want %d", base, got, err, want)
	}
}

// TestLoadCountsEveryStatus: a single agent named orchestrator counts
// under every one of the five statuses, and a list of five recognized
// names, one per status, counts five.
func TestLoadCountsEveryStatus(t *testing.T) {
	statuses := []string{"idle", "working", "blocked", "done", "unknown"}
	for _, s := range statuses {
		raw := lwAgentsJSON(lwAgent{pane: "w1:p1", status: s, name: lwName("orchestrator")})
		if got, err := router.CountOrchestrators(raw, "orchestrator"); err != nil || got != 1 {
			t.Errorf("status %s: CountOrchestrators = %d, %v; want 1", s, got, err)
		}
	}
	var agents []lwAgent
	for i, s := range statuses {
		n := "orchestrator"
		if i > 0 {
			n = fmt.Sprintf("orchestrator-%d", i+1)
		}
		agents = append(agents, lwAgent{pane: fmt.Sprintf("w1:p%d", i+1), status: s, name: lwName(n)})
	}
	lwCount(t, lwAgentsJSON(agents...), "orchestrator", 5)
}

// TestLoadRecognizedNamesCountOnce: the recognized names each count
// exactly once, every other agent does not count, null and absent names
// are not counted, and a different base switches the set.
func TestLoadRecognizedNamesCountOnce(t *testing.T) {
	raw := lwAgentsJSON(
		lwAgent{pane: "w1:p1", status: "working", name: lwName("orchestrator")},
		lwAgent{pane: "w1:p2", status: "working", name: lwName("orchestrator-2")},
		lwAgent{pane: "w1:p3", status: "working", name: lwName("orchestrator-12")},
		lwAgent{pane: "w1:p4", status: "working", name: lwName("orchestrator-0")},
		lwAgent{pane: "w1:p5", status: "working", name: lwName("orchestrator-02")},
		lwAgent{pane: "w1:p6", status: "working", name: lwName("orchestrator-x")},
		lwAgent{pane: "w1:p7", status: "working", name: lwName("Orchestrator")},
		lwAgent{pane: "w1:p8", status: "working", name: lwName("my-orchestrator")},
		lwAgent{pane: "w1:p9", status: "working", name: lwName("orchestrators")},
		lwAgent{pane: "w1:p10", status: "working", name: lwName("build")},
		lwAgent{pane: "w1:p11", status: "working", name: lwName("build-2")},
		lwAgent{pane: "w1:p12", status: "working", name: lwName("review-2")},
		lwAgent{pane: "w1:p13", status: "working", nullName: true},
		lwAgent{pane: "w1:p14", status: "working"},
	)
	lwCount(t, raw, "orchestrator", 3)

	base := lwAgentsJSON(
		lwAgent{pane: "w1:p1", status: "working", name: lwName("planner")},
		lwAgent{pane: "w1:p2", status: "working", name: lwName("planner-3")},
		lwAgent{pane: "w1:p3", status: "working", name: lwName("orchestrator")},
	)
	lwCount(t, base, "planner", 2)
}

// TestLoadWordingDocs: the routing documentation keeps the any-status
// load wording, the new section structure and the read-only checks, and
// the phrase "active orchestrator" does not come back in any owned file.
func TestLoadWordingDocs(t *testing.T) {
	files := []struct {
		path  string
		label string
	}{
		{"../../docs/routing.md", "routing.md"},
		{"../../skills/herdr-hermes/SKILL.md", "SKILL.md"},
		{"../../skills/herdr-hermes/references/protocol.md", "protocol.md"},
		{"../../skills/herdr-hermes/references/setup.md", "setup.md"},
	}
	texts := map[string]string{}
	for _, f := range files {
		raw, err := os.ReadFile(f.path)
		if err != nil {
			t.Fatalf("read %s: %v", f.label, err)
		}
		texts[f.label] = strings.ReplaceAll(string(raw), "\r\n", "\n")
	}
	for label, text := range texts {
		if strings.Contains(strings.ToLower(text), "active orchestrator") {
			t.Errorf("%s contains the phrase \"active orchestrator\" (case-insensitive)", label)
		}
	}
	for _, want := range []string{
		"## Orchestrator load",
		"## Orchestrator naming",
		"## Dispatcher environment",
		"### Preflight (read-only)",
		"### Labels and reason codes",
		"whatever their `agent_status`",
		"`idle`, `working`, `blocked`, `done` and `unknown` all count the same",
		"Each recognized agent counts exactly once",
		"`herdr-soho init`",
		"`herdr agent rename <target> <name>`",
		"`herdr machine list --json`",
		"`unknown_machine`",
		"`HERDR_HERMES_NOWRITE=1`",
	} {
		if !strings.Contains(texts["routing.md"], want) {
			t.Errorf("routing.md missing %s", want)
		}
	}
	if !strings.Contains(texts["protocol.md"], "`idle`, `working`, `blocked`, `done` and `unknown` all count the same") {
		t.Errorf("protocol.md missing the any-status load phrase")
	}
	for _, label := range []string{"SKILL.md", "setup.md"} {
		for _, want := range []string{"`herdr machine list --json`", "`orchestrator-<n>`"} {
			if !strings.Contains(texts[label], want) {
				t.Errorf("%s missing %s", label, want)
			}
		}
	}
}
