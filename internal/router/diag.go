package router

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// The Cause vocabulary of a failed probe. The set is closed:
// ClassifyFailure returns one of these constants or "", and Diagnostic
// only prints the constant, the fixed hint text, a label validated by
// LabelPattern and an integer exit code. No part of the child's stdout or
// stderr is ever returned, printed or embedded in a diagnostic.
const (
	// CauseServerNotRunning: no herdr server is running.
	CauseServerNotRunning = "server_not_running"
	// CauseServerUnavailable: the server did not accept the request.
	CauseServerUnavailable = "server_unavailable"
	// CauseProtocolMismatch: the client and server versions differ.
	CauseProtocolMismatch = "protocol_mismatch"
	// CauseEndpointTimeout: the endpoint did not answer in time.
	CauseEndpointTimeout = "endpoint_timeout"
	// CauseSocketPathTooLong: the socket path exceeds the OS limit.
	CauseSocketPathTooLong = "socket_path_too_long"
	// CauseUnknownMachine: herdr does not know the label.
	CauseUnknownMachine = "unknown_machine"
	// CauseConnectionRefused: the remote machine refused the connection.
	CauseConnectionRefused = "connection_refused"
	// CauseHostUnresolved: the remote host name could not be resolved.
	CauseHostUnresolved = "host_unresolved"
	// CausePermissionDenied: the remote machine refused the credentials.
	CausePermissionDenied = "permission_denied"
	// CauseConnectionTimedOut: the connection to the remote machine timed out.
	CauseConnectionTimedOut = "connection_timed_out"
	// CauseHostUnreachable: the remote machine is unreachable on the network.
	CauseHostUnreachable = "host_unreachable"
)

// sunPathMarker is the case-sensitive marker of a socket path longer than
// the operating system allows.
var sunPathMarker = []byte("sun_path")

// ClassifyFailure maps the capped stdout and stderr of a failed herdr
// probe to one of the Cause constants, or "" when no cause is recognized.
// It is pure and first-match-wins: a herdr JSON error object whose code is
// exactly one of the mapped codes (scanned line by line on the first
// MaxProbeDiag bytes of stderr, then of stdout), then a case-sensitive
// sun_path marker in stderr, then the fixed case-insensitive substrings of
// stderr in order. It never returns any part of the input.
func ClassifyFailure(stdout, stderr []byte) string {
	if cause := herdrJSONCause(stderr); cause != "" {
		return cause
	}
	if cause := herdrJSONCause(stdout); cause != "" {
		return cause
	}
	if bytes.Contains(stderr, sunPathMarker) {
		return CauseSocketPathTooLong
	}
	lower := strings.ToLower(string(stderr))
	switch {
	case strings.Contains(lower, "unknown machine"):
		return CauseUnknownMachine
	case strings.Contains(lower, "connection refused"):
		return CauseConnectionRefused
	case strings.Contains(lower, "could not resolve hostname") ||
		strings.Contains(lower, "name or service not known") ||
		strings.Contains(lower, "nodename nor servname"):
		return CauseHostUnresolved
	case strings.Contains(lower, "permission denied"):
		return CausePermissionDenied
	case strings.Contains(lower, "connection timed out") ||
		strings.Contains(lower, "operation timed out"):
		return CauseConnectionTimedOut
	case strings.Contains(lower, "no route to host") ||
		strings.Contains(lower, "network is unreachable"):
		return CauseHostUnreachable
	}
	return ""
}

// herdrJSONCause scans each line of raw (at most the first MaxProbeDiag
// bytes), trimmed of spaces and carriage returns, and returns the mapped
// cause of the first line that starts with { and decodes to a herdr JSON
// error object whose code is exactly one of the mapped codes. Every other
// line — a decode failure, a missing or non-object error field, a code
// outside the closed allowlist — is ignored and the scan continues.
func herdrJSONCause(raw []byte) string {
	if len(raw) > MaxProbeDiag {
		raw = raw[:MaxProbeDiag]
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.Trim(line, " \r")
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var obj struct {
			Error *struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		if obj.Error == nil {
			continue
		}
		switch obj.Error.Code {
		case CauseServerNotRunning, CauseServerUnavailable,
			CauseProtocolMismatch, CauseEndpointTimeout:
			return obj.Error.Code
		}
	}
	return ""
}

// CauseHint returns the fixed operator-facing hint for one cause
// constant. An empty or unrecognized cause gets a neutral hint. The text
// is closed and fixed: it never names a machine, a path or any part of
// the child output.
func CauseHint(cause string) string {
	switch cause {
	case CauseServerNotRunning:
		return "no Herdr server is running at the configured socket; start herdr or check the socket path"
	case CauseServerUnavailable:
		return "the Herdr server did not accept the request"
	case CauseProtocolMismatch:
		return "the herdr client and server versions are not compatible"
	case CauseEndpointTimeout:
		return "the Herdr endpoint did not answer in time"
	case CauseSocketPathTooLong:
		return "the Herdr socket path is longer than the operating system allows"
	case CauseUnknownMachine:
		return "herdr does not know this machine; check herdr machine list"
	case CauseConnectionRefused:
		return "the remote machine refused the connection"
	case CauseHostUnresolved:
		return "the remote host name could not be resolved"
	case CausePermissionDenied:
		return "the remote machine refused the credentials"
	case CauseConnectionTimedOut:
		return "the connection to the remote machine timed out"
	case CauseHostUnreachable:
		return "the remote machine is not reachable on the network"
	default:
		return "herdr reported no recognized cause"
	}
}

// Diagnostic returns the one-line, non-secret operator diagnostic of a
// non-available candidate: the label (printed as-is when it matches
// LabelPattern, else the literal <invalid label>), the state, the reason
// code, the classified cause when present, the child exit code when
// present and the fixed hint. It returns "" for an available or disabled
// candidate. It never contains any part of the child's stdout or stderr:
// only the fixed cause vocabulary, a validated label and an integer exit
// code appear in the line.
func (c Candidate) Diagnostic() string {
	if c.State == StateAvailable || c.State == StateDisabled {
		return ""
	}
	label := c.Machine
	if !LabelPattern.MatchString(label) {
		label = "<invalid label>"
	}
	var b strings.Builder
	b.WriteString(label)
	b.WriteByte(' ')
	b.WriteString(string(c.State))
	b.WriteString(": ")
	b.WriteString(c.Reason)
	if c.Cause != "" {
		b.WriteString(": ")
		b.WriteString(c.Cause)
	}
	if c.ExitCode != 0 {
		b.WriteString(" (herdr exit ")
		b.WriteString(strconv.Itoa(c.ExitCode))
		b.WriteByte(')')
	}
	b.WriteString(": ")
	b.WriteString(diagnosticHint(c))
	return b.String()
}

// diagnosticHint returns the fixed hint text for a non-available
// candidate: the CauseHint of the classified cause for a probe_failed or
// catalog_unavailable reason, the fixed reason text otherwise.
func diagnosticHint(c Candidate) string {
	switch c.Reason {
	case ReasonProbeFailed, ReasonCatalogUnavailable:
		return CauseHint(c.Cause)
	case ReasonProbeTimeout:
		return "the probe did not finish within route_probe_timeout_s"
	case ReasonHerdrUnavailable:
		return "the herdr executable (herdr_bin) is missing or not runnable"
	case ReasonProbeMalformed:
		return "herdr answered with output that is not a valid agent list"
	case ReasonCatalogMalformed:
		return "herdr machine list --json returned an unexpected shape"
	case ReasonUnknownMachine:
		return "not a saved Herdr machine (herdr machine list) and not local"
	case ReasonAmbiguousMachine:
		return "herdr machine list lists this label more than once"
	default:
		return "no further detail"
	}
}
