// Package router chooses the machine a dispatcher sends a new job to. It is
// the dispatcher-side half of DJA-195: herdr-soho never routes a job, so the
// choice happens here, before the node-local job contract is invoked on the
// chosen machine through the dispatcher's own remote execution channel.
//
// The fleet is the ordered list of configured machine labels. Each label is
// probed read-only through the public Herdr CLI: `herdr machine list --json`
// tells whether a saved machine exists and is enabled, and
// `herdr [--machine <label>] agent list` returns the live agents of that
// machine, from which the active orchestrators are counted. Select then
// picks the requested machine when it is available, else the available
// machine with the fewest active orchestrators, ties broken by the
// configured order. A machine whose availability or load cannot be
// established is unavailable; nothing is ever inferred from a failed or
// malformed probe.
//
// The result is advisory: the router never starts, replays or moves a job.
package router

import (
	"context"
	"errors"
	"regexp"
	"time"
)

// Local is the reserved label of the local Herdr server: it is probed with
// `herdr agent list` (no --machine) and needs no saved machine entry.
const Local = "local"

// LabelPattern is the accepted form of a configured or requested machine
// label. The first character is alphanumeric, so a label can never be read
// as a flag by the herdr CLI.
var LabelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// OrchestratorNamePattern is the accepted form of the orchestrator agent
// base name (the herdr-soho orchestrator_name).
var OrchestratorNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// DefaultOrchestratorName is the herdr-soho default orchestrator agent name.
const DefaultOrchestratorName = "orchestrator"

// MaxProbeOutput caps the stdout read from one herdr probe; a longer output
// is a malformed probe.
const MaxProbeOutput = 8 << 20

// MaxProbeDiag caps the child stderr kept per probe. The bytes are used
// only to classify a failed probe into the fixed Cause vocabulary; they
// are never printed, returned to callers or embedded in any diagnostic.
const MaxProbeDiag = 4096

// State is the availability of one configured machine after the probe.
type State string

const (
	// StateAvailable: the machine answered a well-formed agent list.
	StateAvailable State = "available"
	// StateUnavailable: the availability or the load could not be
	// established (catalog, transport, timeout or malformed output).
	StateUnavailable State = "unavailable"
	// StateDisabled: excluded by route_disabled or by a disabled saved
	// Herdr machine; never probed for load.
	StateDisabled State = "disabled"
	// StateUnsupported: the label is not a saved Herdr machine (and is not
	// the reserved local label), or the catalog lists it more than once.
	StateUnsupported State = "unsupported"
)

// Reason codes carried by a non-available candidate.
const (
	ReasonDisabledByConfig   = "disabled_by_config"
	ReasonMachineDisabled    = "machine_disabled"
	ReasonUnknownMachine     = "unknown_machine"
	ReasonAmbiguousMachine   = "ambiguous_machine"
	ReasonCatalogUnavailable = "catalog_unavailable"
	ReasonCatalogMalformed   = "catalog_malformed"
	ReasonProbeTimeout       = "probe_timeout"
	ReasonProbeFailed        = "probe_failed"
	ReasonProbeMalformed     = "probe_malformed"
	ReasonHerdrUnavailable   = "herdr_unavailable"
)

// Selection reasons carried by a Result.
const (
	MotivoRequested = "requested"
	MotivoLeastLoad = "least_load"
	MotivoFallback  = "fallback"
)

// Candidate is one configured machine after the probe, in configured order.
// Orchestrators is set only when State is StateAvailable; Reason only when
// it is not. Cause and ExitCode are operator diagnostics excluded from the
// JSON: Cause holds one of the Cause constants when a failure was
// classified from the capped probe output, and ExitCode the non-zero exit
// code of a herdr child that exited on its own.
type Candidate struct {
	Machine       string `json:"maquina"`
	State         State  `json:"estado"`
	Orchestrators *int   `json:"orquestradores,omitempty"`
	Reason        string `json:"motivo,omitempty"`
	Cause         string `json:"-"`
	ExitCode      int    `json:"-"`
}

// Result is the routing decision. Requested is the requested label or empty;
// Candidates always holds every configured machine in configured order, also
// when Select returns an error.
type Result struct {
	Machine       string      `json:"maquina"`
	Motivo        string      `json:"motivo"`
	Requested     string      `json:"solicitada,omitempty"`
	Orchestrators int         `json:"orquestradores"`
	Candidates    []Candidate `json:"candidatos"`
}

// ErrNoneAvailable: no configured machine is available.
var ErrNoneAvailable = errors.New("router: no eligible machine is available")

// ErrUnknownRequested: the requested label is not a configured machine.
var ErrUnknownRequested = errors.New("router: requested machine is not configured")

// ErrProbeTimeout is wrapped by an Exec error when the probe hit its
// deadline.
var ErrProbeTimeout = errors.New("router: probe timed out")

// ErrHerdrUnavailable is wrapped by an Exec error when the herdr executable
// is missing or cannot be started.
var ErrHerdrUnavailable = errors.New("router: herdr unavailable")

// Exec runs the herdr CLI with argv (the arguments after the executable,
// never a shell) and empty stdin. It returns the child's stdout (at most
// MaxProbeOutput+1 bytes), its stderr (at most MaxProbeDiag bytes, used
// only to classify a failed probe) and its exit code. err is non-nil only
// when the child did not exit on its own: it wraps ErrProbeTimeout on the
// deadline and ErrHerdrUnavailable when the executable is missing or not
// runnable.
type Exec func(ctx context.Context, argv []string) (stdout, stderr []byte, code int, err error)

// Fleet is the configured fleet and how to probe it.
type Fleet struct {
	// Machines is the configured order of the eligible labels.
	Machines []string
	// Disabled lists the labels excluded by configuration.
	Disabled []string
	// OrchestratorName is the orchestrator agent base name.
	OrchestratorName string
	// Timeout bounds each probe (the catalog and every agent list).
	Timeout time.Duration
	// Exec runs herdr.
	Exec Exec
}
