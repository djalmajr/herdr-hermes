package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/router"
)

// cmdRoute implements the read-only dispatcher-side machine router:
// `herdr-hermes route [--machine <label>]`. It validates its arguments
// first: a malformed invocation exits 2 with its own fixed motivo and the
// route-scoped diagnostic on stderr (the route help text for -h/--help,
// the one-line diagnostic plus the route usage otherwise), before the
// config is loaded and before any herdr call. It then loads the configured
// fleet, probes each machine read-only through the public Herdr CLI (argv
// subprocesses under the configured timeout, never a shell) and prints the
// routing decision as one JSON line. Every machine that is unavailable or
// unsupported gets one diagnostic line on stderr built only from the
// closed router vocabulary (router.Candidate.Diagnostic), and a
// route_disabled label that matches no configured machine gets a warning
// line; neither changes stdout or the exit code. It writes and creates nothing, not
// even the config dir, so it works under HERDR_HERMES_NOWRITE=1. The
// decision is advisory: the command never starts, moves or replays a job.
func cmdRoute(args []string, env Env) int {
	requested, problem := parseRouteArgs(args)
	if problem != "" {
		return routeProblem(env, problem)
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
	// A route_disabled label outside the fleet excludes nothing: say so
	// on stderr (the labels are validated configuration values), never
	// as an error, so an exclusion staged ahead of a fleet change stays
	// valid.
	if unmatched := cfg.UnmatchedDisabled(); len(unmatched) > 0 {
		_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: route: warning: route_disabled labels not in route_machines exclude nothing: %s\n", strings.Join(unmatched, ","))
	}
	fleet := router.Fleet{
		Machines:         machines,
		Disabled:         cfg.RouteDisabledList(),
		OrchestratorName: cfg.RouteOrchestratorName,
		Timeout:          time.Duration(cfg.RouteProbeTimeoutS) * time.Second,
		Exec:             router.NewExec(cfg.HerdrBin, childEnviron(env)),
	}
	cands := fleet.Probe(context.Background())
	// One fixed-vocabulary line per unavailable or unsupported machine,
	// on success and on exit 4 alike; the raw herdr output never reaches
	// stderr or stdout.
	for _, c := range cands {
		if d := c.Diagnostic(); d != "" {
			_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: route: %s\n", d)
		}
	}
	res, err := router.Select(cands, requested)
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

// routeUsage is the route-scoped usage printed on stderr for every route
// usage problem except a help request.
const routeUsage = "usage: herdr-hermes route [--machine <label>]\nrun 'herdr-hermes route --help' for the route options\n"

// routeHelp is the route-scoped help printed on stderr for -h/--help.
const routeHelp = `usage: herdr-hermes route [--machine <label>]

Choose the machine for a new job (dispatcher side, read-only). Prints one
JSON line on stdout; diagnostics go to stderr.

options:
  --machine <label>   prefer this configured machine; the label is a separate
                      argument (the --machine=<label> form is not accepted)

configuration (herdr-hermes config set <key> <value>):
  route_machines, route_disabled, route_orchestrator_name,
  route_probe_timeout_s, herdr_bin

exit codes:
  0   a machine was chosen
  2   bad usage, invalid configuration, route_machines not configured or
      the requested machine is not configured
  4   no eligible machine is available
`

// The route problem details: fixed text that never carries the offending
// argument's value.
const (
	routeProblemHelp       = "help requested"
	routeProblemEqualsForm = "the --machine=<label> form is not accepted; pass the label as a separate argument: --machine <label>"
	routeProblemNeedsLabel = "--machine needs a machine label"
	routeProblemRepeated   = "--machine is given more than once"
	routeProblemEmptyLabel = "the machine label is empty"
	routeProblemFlagValue  = "the --machine value looks like a flag; a machine label starts with a letter or digit"
	routeProblemTooLong    = "the machine label is longer than 64 characters"
	routeProblemBadLabel   = "invalid machine label: use a letter or digit, then up to 63 letters, digits, '.', '_' or '-'"
	routeProblemUnknown    = "unknown flag"
	routeProblemExtra      = "unexpected argument"
)

// routeProblem prints the route-scoped diagnostic to stderr (routeHelp for
// a help request, the one-line diagnostic followed by routeUsage
// otherwise) and the exit-2 error JSON with the route usage suffix to
// stdout, and returns 2. It is called before config.Load and before any
// herdr call, and the detail is fixed text, so the offending argument's
// value is never echoed.
func routeProblem(env Env, detail string) int {
	if detail == routeProblemHelp {
		_, _ = io.WriteString(env.Stderr, routeHelp)
	} else {
		_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: route: %s\n", detail)
		_, _ = io.WriteString(env.Stderr, routeUsage)
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "2", Motivo: "route: " + detail + "; usage: route [--machine <label>]"}))
	return 2
}

// parseRouteArgs validates the route arguments: none, or exactly
// --machine <label> once, in the space form only. On success it returns
// the requested label ("" when no machine is requested) and an empty
// problem. Otherwise it returns "" and the fixed detail of the first
// problem, scanning left to right: a -h or --help argument anywhere except
// the value position directly after a --machine token wins before the
// scan (help), then the --machine=<label> equals form, a missing value,
// a repeated --machine (detected at the second token, before its value is
// examined), an empty, flag-like, over-long or invalid label, an unknown
// flag and an unexpected argument. The offending argument's value never
// appears in the returned detail.
func parseRouteArgs(args []string) (requested string, problem string) {
	// Help first: -h or --help anywhere except the value position directly
	// after a --machine token.
	for i := 0; i < len(args); i++ {
		if (args[i] == "-h" || args[i] == "--help") && (i == 0 || args[i-1] != "--machine") {
			return "", routeProblemHelp
		}
	}
	seen := false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--machine":
			if seen {
				return "", routeProblemRepeated
			}
			if i+1 >= len(args) {
				return "", routeProblemNeedsLabel
			}
			value := args[i+1]
			switch {
			case value == "":
				return "", routeProblemEmptyLabel
			case strings.HasPrefix(value, "-"):
				return "", routeProblemFlagValue
			case len(value) > 64:
				return "", routeProblemTooLong
			case !router.LabelPattern.MatchString(value):
				return "", routeProblemBadLabel
			}
			requested = value
			seen = true
			i++
		case strings.HasPrefix(args[i], "--machine="):
			return "", routeProblemEqualsForm
		case strings.HasPrefix(args[i], "-"):
			return "", routeProblemUnknown
		default:
			return "", routeProblemExtra
		}
	}
	return requested, ""
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
