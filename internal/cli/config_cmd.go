package cli

import (
	"fmt"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/config"
)

// cmdConfig implements `config get <key>`, `config set <key> <value>` and
// `config list`. get and list are read-only; set is a writing command and is
// refused under HERDR_HERMES_NOWRITE=1 (the subcommand carries the write, so
// the gate lives here, not in the table entry).
func cmdConfig(args []string, env Env) int {
	if len(args) == 0 || (len(args) > 1 && (args[0] == "--help" || args[0] == "-h")) {
		badUsage(env, "usage: config get <key> | config set <key> <value> | config list")
		return 2
	}
	switch args[0] {
	case "get":
		if len(args) != 2 {
			badUsage(env, "usage: config get <key>")
			return 2
		}
		cfg, err := config.Load(env.ConfigDir)
		if err != nil {
			fail(env, 2, "config: "+err.Error())
			return 2
		}
		value, err := cfg.Value(args[1])
		if err != nil {
			fail(env, 2, "config: "+err.Error())
			return 2
		}
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", keyValuePairJSON(args[1], value))
		return 0
	case "list":
		if len(args) != 1 {
			badUsage(env, "usage: config list")
			return 2
		}
		cfg, err := config.Load(env.ConfigDir)
		if err != nil {
			fail(env, 2, "config: "+err.Error())
			return 2
		}
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Config config.Config `json:"config"`
		}{Config: cfg}))
		return 0
	case "set":
		if env.Getenv(nowriteVar) == "1" {
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
			return 2
		}
		if len(args) != 3 {
			badUsage(env, "usage: config set <key> <value>")
			return 2
		}
		if err := config.Set(env.ConfigDir, args[1], args[2]); err != nil {
			fail(env, 2, "config: "+err.Error())
			return 2
		}
		// Print the stored value: the router lists are normalized on set.
		value := args[2]
		var warning string
		if cfg, err := config.Load(env.ConfigDir); err == nil {
			if stored, err := cfg.Value(args[1]); err == nil {
				value = stored
			}
			if args[1] == "route_disabled" || args[1] == "route_machines" {
				warning = unmatchedDisabledWarning(cfg)
			}
		}
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", keyValuePairJSON(args[1], value))
		// The warning, if any, goes to stderr after the stdout line and never
		// changes the exit code: staging an exclusion before the fleet stays
		// valid.
		if warning != "" {
			_, _ = fmt.Fprint(env.Stderr, warning)
		}
		return 0
	default:
		badUsage(env, "unknown config subcommand "+quote(args[0]))
		return 2
	}
}

// keyValuePairJSON is {"key":"…","value":"…"}.
func keyValuePairJSON(key, value string) string {
	return mustJSON(struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}{Key: key, Value: value})
}

// unmatchedDisabledWarning returns the one stderr warning line (with its
// trailing newline) for a stored configuration whose route_disabled labels
// are not all in route_machines, or "" when there is nothing to warn about.
// It exists so the route command can print the same line.
func unmatchedDisabledWarning(cfg config.Config) string {
	unmatched := cfg.UnmatchedDisabled()
	if len(unmatched) == 0 {
		return ""
	}
	return "herdr-hermes: config: warning: route_disabled labels not in route_machines exclude nothing: " +
		strings.Join(unmatched, ",") + "\n"
}
