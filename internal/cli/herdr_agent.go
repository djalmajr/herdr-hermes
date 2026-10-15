package cli

// The default pane-identity resolution of the a2a reply: the agent name
// of a pane on the local Herdr, read from the herdr binary of the PATH
// with `herdr agent get <pane>`. The query is read-only and the
// subprocess is an argv array with the exact environment of the nau
// process, never a shell; on the bound the child's whole process tree is
// terminated (the graceful step on the cancel and the force after the
// wait), and a child that exits on its own leaves its descendants
// untouched — the same proctree wiring as the herdr-soho runner. The
// parser of the one JSON line the binary prints is pure and is tested
// without a subprocess.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/djalmajr/nau/internal/proctree"
)

const (
	// herdrAgentGetBin is the herdr binary of the PATH the default of
	// Env.PaneAgentName runs.
	herdrAgentGetBin = "herdr"
	// herdrAgentGetBound bounds the `herdr agent get` subprocess.
	herdrAgentGetBound = 10 * time.Second
	// herdrAgentGetWaitDelay gives a child that ignores the cancel
	// signal a bounded grace period before the process is killed, so
	// the bound never leaves the child's tree behind; it is the process
	// tree's one grace, the same the first forwarded terminal signal
	// gives the group.
	herdrAgentGetWaitDelay = proctree.Grace
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
// local Herdr, or "" when the pane has no agent. When the bound expires
// while the child is alive, the child's whole process tree is
// terminated (the graceful step on the cancel and the force after the
// wait); a child that exits on its own keeps its exit and the
// descendants it left on purpose. A missing binary, a failed start, a
// bound that expired or any other exit is an error.
func paneAgentName(ctx context.Context, environ []string, pane string) (string, error) {
	bin, err := exec.LookPath(herdrAgentGetBin)
	if err != nil {
		return "", fmt.Errorf("herdr agent get: binary unavailable: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, herdrAgentGetBound)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, "agent", "get", pane)
	// Put the child in its own tree scope before Start, so the cancel
	// below can end the whole tree, not only the direct child.
	proctree.Prepare(cmd)
	cmd.Env = environ
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	cmd.WaitDelay = herdrAgentGetWaitDelay
	// The cancel ends the child's whole process tree (internal/proctree);
	// the flag marks the runs the cancel reached a live child, so the
	// force after the wait runs exactly on those.
	var interrupted atomic.Bool
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return os.ErrProcessDone
		}
		if err := proctree.Terminate(cmd); err != nil {
			return err
		}
		interrupted.Store(true)
		return nil
	}
	if err := cmd.Start(); err != nil {
		return "", fmt.Errorf("herdr agent get: %w", err)
	}
	// Attach right after Start binds the tree to the cancel (the job
	// object on Windows). It fails only where the platform cannot track
	// the tree; then the run continues and the cancel falls back to the
	// child alone.
	release, _ := proctree.Attach(cmd)
	defer release()
	// The relay forwards the terminal's signals (SIGINT, SIGTERM and
	// SIGHUP on unix) to the child's group while the child runs, so an
	// interrupted run does not leave the tree behind as orphans; on
	// Windows the relay is inert and the console's Ctrl-C keeps
	// reaching the child. The first forwarded signal starts the relay's
	// fixed grace: the child's group that survives it is forced down
	// (SIGKILL) so the wait returns. The stop runs after the wait: it
	// ends the relay, stops the signal reception and reports the last
	// signal received, if any.
	stopForward := proctree.ForwardSignals(cmd)
	waitErr := cmd.Wait()
	if sig, hadSignal := stopForward(); hadSignal {
		// The relay forwarded the terminal's signal to the child's
		// group and the run ended on it: the process must die on the
		// very signal it would have died on without the group
		// isolation. The re-raise ends the process, so the error below
		// is the fallback only when the re-raise did not.
		proctree.ReRaise(sig)
		return "", fmt.Errorf("herdr agent get: ended by %v", sig)
	}
	if waitErr != nil {
		if cctx.Err() != nil {
			// The bound expired while the child (or one of its
			// descendants) was still alive: end whatever the cancel
			// left behind in the tree before reporting.
			if interrupted.Load() {
				proctree.Kill(cmd)
			}
			return "", fmt.Errorf("herdr agent get: killed after %s", herdrAgentGetBound)
		}
		return "", fmt.Errorf("herdr agent get: exited without the agent answer: %v", waitErr)
	}
	return parseHerdrAgentGet(out.String(), pane)
}
