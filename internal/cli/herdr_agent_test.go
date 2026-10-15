package cli

// The parser tests of the default pane-identity resolution of the a2a
// reply: the one JSON line of `herdr agent get <pane>`, read without a
// subprocess.

import "testing"

// herdrAgentGetSample is the agent line of `herdr agent get w6Y:p1` in
// the shape the binary prints (id, result.agent and result.type), with
// neutral values: no path, no session, no credential.
const herdrAgentGetSample = `{"id":"cli:agent:get","result":{"agent":{"agent_status":"done","name":"orchestrator-4","pane_id":"w6Y:p1","revision":13,"tab_id":"w6Y:t1","workspace_id":"w6Y"},"type":"agent_info"}}`

// TestParseHerdrAgentGet proves the parser of the `herdr agent get`
// output: the agent line answers the name of the requested pane, a pane
// without an agent (the binary's agent_not_found answer) has no name and
// is not a failure, and every other body is an error (fail closed): an
// invalid JSON, a result without an agent, an agent without a name, an
// agent of another pane and an error answer that is not agent_not_found.
func TestParseHerdrAgentGet(t *testing.T) {
	// (a) The agent line: the name of the requested pane.
	t.Run("the agent line returns the name", func(t *testing.T) {
		name, err := parseHerdrAgentGet(herdrAgentGetSample, "w6Y:p1")
		if err != nil {
			t.Fatalf("parse = %v, want nil", err)
		}
		if name != "orchestrator-4" {
			t.Fatalf("name = %q, want orchestrator-4", name)
		}
	})

	// (b) A pane without an agent: the agent_not_found answer is the
	// absence of a name, not a failure.
	t.Run("a pane without an agent has no name", func(t *testing.T) {
		name, err := parseHerdrAgentGet(`{"error":{"code":"agent_not_found","message":"agent target w6Z:p2 not found"},"id":"cli:agent:get"}`, "w6Z:p2")
		if err != nil {
			t.Fatalf("parse = %v, want nil", err)
		}
		if name != "" {
			t.Fatalf("name = %q, want empty (no agent)", name)
		}
	})

	// (c) A body that is not the JSON line is an error.
	t.Run("an invalid JSON body is an error", func(t *testing.T) {
		if _, err := parseHerdrAgentGet("not a JSON line", "w6Y:p1"); err == nil {
			t.Fatal("parse = nil error, want an error for an invalid JSON")
		}
	})

	// (d) A result without an agent is an error.
	t.Run("a result without an agent is an error", func(t *testing.T) {
		if _, err := parseHerdrAgentGet(`{"id":"cli:agent:get","result":{"type":"agent_info"}}`, "w6Y:p1"); err == nil {
			t.Fatal("parse = nil error, want an error for a missing agent")
		}
	})

	// (e) An agent without a name is an error.
	t.Run("an agent without a name is an error", func(t *testing.T) {
		if _, err := parseHerdrAgentGet(`{"id":"cli:agent:get","result":{"agent":{"pane_id":"w6Y:p1"},"type":"agent_info"}}`, "w6Y:p1"); err == nil {
			t.Fatal("parse = nil error, want an error for a missing name")
		}
	})

	// (f) An agent of another pane is an error.
	t.Run("an agent of another pane is an error", func(t *testing.T) {
		if _, err := parseHerdrAgentGet(`{"id":"cli:agent:get","result":{"agent":{"name":"orchestrator-4","pane_id":"w6Z:p2"},"type":"agent_info"}}`, "w6Y:p1"); err == nil {
			t.Fatal("parse = nil error, want an error for a different pane_id")
		}
	})

	// (g) An error answer that is not agent_not_found is an error: a miss
	// and a failure must not be confused (fail closed).
	t.Run("another error answer is an error", func(t *testing.T) {
		if _, err := parseHerdrAgentGet(`{"error":{"code":"invalid_target","message":"bad target"},"id":"cli:agent:get"}`, "w6Y:p1"); err == nil {
			t.Fatal("parse = nil error, want an error for another error code")
		}
	})

	// (h) An error next to a result is an error, whatever the result
	// says: the error member decides before the result is read.
	t.Run("an error next to a matching result is an error", func(t *testing.T) {
		for _, code := range []string{"invalid_target", "agent_not_found"} {
			body := `{"error":{"code":"` + code + `"},"id":"cli:agent:get","result":{"agent":{"name":"orchestrator-4","pane_id":"w6Y:p1"},"type":"agent_info"}}`
			if name, err := parseHerdrAgentGet(body, "w6Y:p1"); err == nil {
				t.Fatalf("parse with error %s = %q, nil, want an error", code, name)
			}
		}
	})
}
