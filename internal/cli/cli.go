// Package cli implements the herdr-hermes command line: the command table
// with the HERDR_HERMES_NOWRITE read-only gating, the shared output
// conventions (one JSON line on stdout, diagnostics on stderr, errors as
// {"status":"<code>","motivo":"<text>"}), and the slice-1 commands
// (outbox, config, capabilities, version, help). Later slices register
// their commands in the marked blocks of the table.
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime/debug"
	"strconv"
	"time"
)

// version is set for release builds with -ldflags. Dev builds keep "dev"
// and get the VCS revision recorded by the Go toolchain appended.
var version = "dev"

// nowriteVar is the read-only gate.
const nowriteVar = "HERDR_HERMES_NOWRITE"

// command is one entry of the command table.
type command struct {
	name           string
	writes         bool
	needsConfigDir bool
	run            func(args []string, env Env) int
}

// Command table. The later slices register their commands in the marked
// blocks so parallel slices merge cleanly.
var commands = []command{
	// slice 2: job, sync, doctor
	{"outbox", false, true, cmdOutbox},
	// slice 3: wake, session, decision, push, auth
	{"wake", true, true, cmdWake},
	{"session", true, true, cmdSession},
	{"decision", true, true, cmdDecision},
	{"push", true, true, cmdPush},
	{"auth", false, true, cmdAuth},
	{"config", false, true, cmdConfig},
	{"capabilities", false, false, cmdCapabilities},
	{"version", false, false, cmdVersion},
	{"help", false, false, cmdHelp},
	// slice 4: plugin
}

// Run executes args[0] as a command and returns its exit code. An unknown
// command exits 2. Under HERDR_HERMES_NOWRITE=1 a command with writes=true
// exits 2 with the nowrite error before any side effect.
func Run(args []string, env Env) int {
	if env.Stdout == nil || env.Stderr == nil {
		env.Stdout = io.Discard
		env.Stderr = io.Discard
	}
	if env.Getenv == nil {
		env.Getenv = func(string) string { return "" }
	}
	if env.Now == nil {
		env.Now = time.Now
	}
	if env.Sleep == nil {
		env.Sleep = sleepCtx
	}
	if len(args) == 0 {
		_, _ = io.WriteString(env.Stdout, usage)
		return 0
	}
	var cmd *command
	for i := range commands {
		if commands[i].name == args[0] {
			cmd = &commands[i]
			break
		}
	}
	if cmd == nil {
		badUsage(env, "unknown command "+quote(args[0]))
		return 2
	}
	// A command that reads or writes the config dir cannot run when the user
	// config directory is unavailable; help, version and capabilities still
	// work. There is no silent temp-dir fallback.
	if cmd.needsConfigDir && env.ConfigDir == "" {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", noConfigDirErrorJSON)
		return 2
	}
	if env.Getenv(nowriteVar) == "1" && cmd.writes {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	return cmd.run(args[1:], env)
}

// errorLine is the error JSON on stdout: {"status":"<code>","motivo":"…"}.
type errorLine struct {
	Status string `json:"status"`
	Motivo string `json:"motivo"`
}

const nowriteErrorJSON = `{"status":"nowrite","motivo":"HERDR_HERMES_NOWRITE=1"}`

const noConfigDirErrorJSON = `{"status":"no_config_dir","motivo":"user config directory unavailable"}`

// badUsage prints the usage text to stderr and the exit-2 error JSON to
// stdout.
func badUsage(env Env, motivo string) {
	_, _ = io.WriteString(env.Stderr, usage)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "2", Motivo: motivo}))
}

// fail prints an error JSON with the given numeric code to stdout and a
// diagnostic to stderr.
func fail(env Env, code int, motivo string) {
	_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: %s\n", motivo)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: itoa(code), Motivo: motivo}))
}

func itoa(n int) string {
	return strconv.Itoa(n)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"status":"2","motivo":"internal error"}`
	}
	return string(b)
}

func quote(s string) string { return `"` + s + `"` }

func cmdCapabilities(args []string, env Env) int {
	if len(args) != 1 || args[0] != "--json" {
		badUsage(env, "usage: capabilities --json")
		return 2
	}
	_, _ = io.WriteString(env.Stdout, "{\"schema\":1,\"bridge\":1,\"outbox\":1,\"push\":1}\n")
	return 0
}

func cmdVersion(args []string, env Env) int {
	if len(args) != 0 {
		badUsage(env, "usage: version")
		return 2
	}
	v := version
	if v == "" {
		v = "dev"
	}
	if v == "dev" {
		if rev := vcsRevision(); rev != "" {
			v += "+" + rev
		}
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Version string `json:"version"`
	}{Version: v}))
	return 0
}

// vcsRevision returns the VCS revision recorded by the Go toolchain for
// dev builds, like the reference implementation.
func vcsRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			return setting.Value
		}
	}
	return ""
}

func cmdHelp(args []string, env Env) int {
	if len(args) != 0 {
		badUsage(env, "usage: help")
		return 2
	}
	_, _ = io.WriteString(env.Stdout, usage)
	return 0
}
