package cli

// The default pane-identity resolution of the a2a reply: the agent name
// of a pane on the local Herdr, read from the herdr binary of the PATH
// with `herdr agent get <pane>`. The query is read-only and the
// subprocess is an argv array with the exact environment of the nau
// process, never a shell; on the bound the direct child is canceled and,
// after the wait delay, killed. Like the herdr-soho runner, it does not
// track descendants the child may start. The parser of the one JSON line
// the binary prints is pure and is tested without a subprocess.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"
)

const (
	// herdrAgentGetBin is the herdr binary of the PATH the default of
	// Env.PaneAgentName runs.
	herdrAgentGetBin = "herdr"
	// herdrAgentGetBound bounds the `herdr agent get` subprocess.
	herdrAgentGetBound = 10 * time.Second
	// herdrAgentGetWaitDelay gives a child that ignores the cancel
	// signal a bounded grace period before the process is killed, so
	// the bound never leaves the direct child behind.
	herdrAgentGetWaitDelay = 5 * time.Second
)

// herdrAgentGet is the one JSON line `herdr agent get <pane>` prints: on
// a hit result holds the agent info (result.agent), on a miss error
// holds the binary's error (agent_not_found for a pane without an
// agent).
type herdrAgentGet struct {
	ID     string `json:"id"`
	Result *struct {
		Type  string `json:"type"`
		Agent *struct {
			Name   string `json:"name"`
			PaneID string `json:"pane_id"`
		} `json:"agent"`
	} `json:"result"`
	Error *struct {
		Code string `json:"code"`
	} `json:"error"`
}

// parseHerdrAgentGet reads the output of `herdr agent get <pane>` and
// returns the agent name of the pane. It returns "" and a nil error when
// the pane has no agent (the binary's agent_not_found answer, which
// exits 0, with no result); every other answer that is not the agent of
// the requested pane — an invalid JSON, any other error answer, an error
// next to a result, a result without an agent, an empty name or a
// different pane_id — is an error (fail closed). An error member decides
// the answer before the result is read.
func parseHerdrAgentGet(out, pane string) (string, error) {
	var body herdrAgentGet
	if err := json.Unmarshal([]byte(out), &body); err != nil {
		return "", fmt.Errorf("herdr agent get: invalid JSON output: %v", err)
	}
	if body.Error != nil {
		if body.Error.Code == "agent_not_found" && body.Result == nil {
			return "", nil
		}
		return "", errors.New("herdr agent get: the answer carries an error")
	}
	if body.Result == nil {
		return "", errors.New("herdr agent get: no agent in the answer")
	}
	agent := body.Result.Agent
	if agent == nil {
		return "", errors.New("herdr agent get: no agent in the result")
	}
	if agent.Name == "" {
		return "", errors.New("herdr agent get: empty agent name")
	}
	if agent.PaneID != pane {
		return "", fmt.Errorf("herdr agent get: pane_id %q is not the requested pane", agent.PaneID)
	}
	return agent.Name, nil
}

// paneAgentName is the default of Env.PaneAgentName: it runs the herdr
// binary of the PATH with `agent get <pane>` as an argv subprocess (no
// shell) with environ as the exact child environment, under a bound of
// herdrAgentGetBound, and returns the agent name of the pane on the
// local Herdr, or "" when the pane has no agent. A missing binary, a
// failed start, a bound that expired or any other exit is an error.
func paneAgentName(ctx context.Context, environ []string, pane string) (string, error) {
	bin, err := exec.LookPath(herdrAgentGetBin)
	if err != nil {
		return "", fmt.Errorf("herdr agent get: binary unavailable: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, herdrAgentGetBound)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, "agent", "get", pane)
	cmd.Env = environ
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	cmd.WaitDelay = herdrAgentGetWaitDelay
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("herdr agent get: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		if cctx.Err() != nil {
			return "", fmt.Errorf("herdr agent get: killed after %s", herdrAgentGetBound)
		}
		return "", fmt.Errorf("herdr agent get: exited without the agent answer: %v", err)
	}
	return parseHerdrAgentGet(out.String(), pane)
}
