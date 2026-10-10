package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/router"
)

// cmdRoute implements the read-only dispatcher-side machine router:
// `herdr-hermes route [--machine <label>]`. It loads the configured fleet,
// probes each machine read-only through the public Herdr CLI (argv
// subprocesses under the configured timeout, never a shell) and prints the
// routing decision as one JSON line. It writes and creates nothing, not
// even the config dir, so it works under HERDR_HERMES_NOWRITE=1. The
// decision is advisory: the command never starts, moves or replays a job.
func cmdRoute(args []string, env Env) int {
	requested, ok := parseRouteArgs(args)
	if !ok {
		badUsage(env, "usage: route [--machine <label>]")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	machines := cfg.RouteMachineList()
	if len(machines) == 0 {
		fail(env, 2, "route: route_machines is not configured")
		return 2
	}
	if requested != "" && !containsLabel(machines, requested) {
		fail(env, 2, "route: requested machine is not configured")
		return 2
	}
	fleet := router.Fleet{
		Machines:         machines,
		Disabled:         cfg.RouteDisabledList(),
		OrchestratorName: cfg.RouteOrchestratorName,
		Timeout:          time.Duration(cfg.RouteProbeTimeoutS) * time.Second,
		Exec:             router.NewExec(cfg.HerdrBin, childEnviron(env)),
	}
	res, err := router.Select(fleet.Probe(context.Background()), requested)
	switch {
	case err == nil:
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(res))
		return 0
	case errors.Is(err, router.ErrUnknownRequested):
		// Unreachable after the configured check above; the contract
		// stays total.
		fail(env, 2, "route: requested machine is not configured")
		return 2
	case errors.Is(err, router.ErrNoneAvailable):
		_, _ = io.WriteString(env.Stderr, "herdr-hermes: route: no eligible machine is available\n")
		line := struct {
			Status     string             `json:"status"`
			Motivo     string             `json:"motivo"`
			Candidates []router.Candidate `json:"candidatos"`
		}{Status: "unavailable", Motivo: "no eligible machine is available", Candidates: res.Candidates}
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(line))
		return 4
	default:
		fail(env, 2, "route: "+err.Error())
		return 2
	}
}

// parseRouteArgs validates the route arguments: none, or exactly
// --machine <label> once, in the space form only. A label that does not
// match router.LabelPattern (a flag-like value such as -x, the empty
// string, a too-long label) is a usage error and is never passed on; the
// --machine=<label> form, a missing value, a repeated flag and any other
// argument are usage errors too.
func parseRouteArgs(args []string) (requested string, ok bool) {
	for i := 0; i < len(args); i++ {
		if args[i] != "--machine" {
			return "", false
		}
		if i+1 >= len(args) {
			return "", false
		}
		value := args[i+1]
		if requested != "" || !router.LabelPattern.MatchString(value) {
			return "", false
		}
		requested = value
		i++
	}
	return requested, true
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
