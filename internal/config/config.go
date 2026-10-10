// Package config manages the herdr-hermes user configuration file: a
// key=value file under the user configuration directory, with defaults for
// every known key. The configuration also carries the five keys of the
// dispatcher-side machine router (herdr_bin, route_machines, route_disabled,
// route_orchestrator_name and route_probe_timeout_s), which are validated on
// load and on set. The dispatcher API key never lives in this file.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/router"
)

// Config is the machine configuration with effective values (defaults
// applied for absent keys).
type Config struct {
	MachineLabel          string `json:"machine_label"`
	DispatcherURL         string `json:"dispatcher_url"`
	HerdrSohoBin          string `json:"herdr_soho_bin"`
	PushTimeoutS          int    `json:"push_timeout_s"`
	HerdrBin              string `json:"herdr_bin"`
	RouteMachines         string `json:"route_machines"`
	RouteDisabled         string `json:"route_disabled"`
	RouteOrchestratorName string `json:"route_orchestrator_name"`
	RouteProbeTimeoutS    int    `json:"route_probe_timeout_s"`
}

// ErrUnknownKey marks an attempt to set or read a key the configuration does
// not define.
var ErrUnknownKey = fmt.Errorf("unknown configuration key")

// Limits of the router keys: at most maxRouteListItems labels in one route
// list and a route probe timeout of routeProbeTimeoutMin to
// routeProbeTimeoutMax seconds.
const (
	maxRouteListItems    = 32
	routeProbeTimeoutMin = 1
	routeProbeTimeoutMax = 120
)

// defaultConfig returns the defaults: machine_label and dispatcher_url
// empty, herdr_soho_bin "herdr-soho", push_timeout_s 15, herdr_bin "herdr",
// route_machines and route_disabled empty, route_orchestrator_name
// "orchestrator", route_probe_timeout_s 20.
func defaultConfig() Config {
	return Config{
		HerdrSohoBin:          "herdr-soho",
		PushTimeoutS:          15,
		HerdrBin:              "herdr",
		RouteOrchestratorName: router.DefaultOrchestratorName,
		RouteProbeTimeoutS:    20,
	}
}

// Keys lists every known configuration key in canonical order.
func Keys() []string {
	return []string{"machine_label", "dispatcher_url", "herdr_soho_bin", "push_timeout_s", "herdr_bin", "route_machines", "route_disabled", "route_orchestrator_name", "route_probe_timeout_s"}
}

// Load reads <dir>/config and returns the effective configuration. A missing
// file or directory yields the defaults without error and without creating
// anything, so read-only commands never create the config directory.
func Load(dir string) (Config, error) {
	cfg := defaultConfig()
	data, err := os.ReadFile(filepath.Join(dir, "config"))
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return cfg, fmt.Errorf("config line %d: missing '='", i+1)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		switch key {
		case "machine_label":
			cfg.MachineLabel = value
		case "dispatcher_url":
			cfg.DispatcherURL = value
		case "herdr_soho_bin":
			cfg.HerdrSohoBin = value
		case "push_timeout_s":
			n, err := strconv.Atoi(value)
			if err != nil || n <= 0 {
				return cfg, fmt.Errorf("config: push_timeout_s must be a positive integer")
			}
			cfg.PushTimeoutS = n
		case "herdr_bin":
			if strings.TrimSpace(value) == "" {
				return cfg, fmt.Errorf("config: herdr_bin must be a non-empty string")
			}
			cfg.HerdrBin = value
		case "route_machines", "route_disabled":
			list, err := parseLabelList(key, value)
			if err != nil {
				return cfg, fmt.Errorf("config: %w", err)
			}
			if key == "route_machines" {
				cfg.RouteMachines = strings.Join(list, ",")
			} else {
				cfg.RouteDisabled = strings.Join(list, ",")
			}
		case "route_orchestrator_name":
			if !router.OrchestratorNamePattern.MatchString(value) {
				return cfg, fmt.Errorf("config: route_orchestrator_name: invalid orchestrator name %q", value)
			}
			cfg.RouteOrchestratorName = value
		case "route_probe_timeout_s":
			n, err := parseRouteProbeTimeout(value)
			if err != nil {
				return cfg, fmt.Errorf("config: %w", err)
			}
			cfg.RouteProbeTimeoutS = n
		default:
			return cfg, fmt.Errorf("%w: %q", ErrUnknownKey, key)
		}
	}
	return cfg, nil
}

// Set writes one key of the configuration file atomically. Unknown keys and
// invalid values are refused with an error before anything is read or
// written. The stored form is normalized: herdr_bin is trimmed and the
// route lists are joined with "," without spaces.
func Set(dir, key, value string) error {
	stored, err := storedValue(key, value)
	if err != nil {
		return err
	}
	cfg, err := Load(dir)
	if err != nil {
		return err
	}
	switch key {
	case "machine_label":
		cfg.MachineLabel = stored
	case "dispatcher_url":
		cfg.DispatcherURL = stored
	case "herdr_soho_bin":
		cfg.HerdrSohoBin = stored
	case "push_timeout_s":
		cfg.PushTimeoutS = atoi(stored)
	case "herdr_bin":
		cfg.HerdrBin = stored
	case "route_machines":
		cfg.RouteMachines = stored
	case "route_disabled":
		cfg.RouteDisabled = stored
	case "route_orchestrator_name":
		cfg.RouteOrchestratorName = stored
	case "route_probe_timeout_s":
		cfg.RouteProbeTimeoutS = atoi(stored)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	var b strings.Builder
	for _, k := range Keys() {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(valueOf(cfg, k))
		b.WriteString("\n")
	}
	return outbox.WriteFileAtomic(filepath.Join(dir, "config"), []byte(b.String()))
}

// Value returns the effective value of a known key as a string, or
// ErrUnknownKey for an unknown key.
func (c Config) Value(key string) (string, error) {
	switch key {
	case "machine_label":
		return c.MachineLabel, nil
	case "dispatcher_url":
		return c.DispatcherURL, nil
	case "herdr_soho_bin":
		return c.HerdrSohoBin, nil
	case "push_timeout_s":
		return strconv.Itoa(c.PushTimeoutS), nil
	case "herdr_bin":
		return c.HerdrBin, nil
	case "route_machines":
		return c.RouteMachines, nil
	case "route_disabled":
		return c.RouteDisabled, nil
	case "route_orchestrator_name":
		return c.RouteOrchestratorName, nil
	case "route_probe_timeout_s":
		return strconv.Itoa(c.RouteProbeTimeoutS), nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownKey, key)
}

// storedValue validates the value of key and returns the form that is stored
// in the configuration file (the trimmed herdr_bin, the normalized route
// lists).
func storedValue(key, value string) (string, error) {
	switch key {
	case "machine_label", "dispatcher_url", "herdr_soho_bin":
		return value, nil
	case "push_timeout_s":
		if n, err := strconv.Atoi(value); err != nil || n <= 0 {
			return "", fmt.Errorf("push_timeout_s must be a positive integer")
		}
		return value, nil
	case "herdr_bin":
		if strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("herdr_bin must be a non-empty string")
		}
		return strings.TrimSpace(value), nil
	case "route_machines", "route_disabled":
		list, err := parseLabelList(key, value)
		if err != nil {
			return "", err
		}
		return strings.Join(list, ","), nil
	case "route_orchestrator_name":
		if !router.OrchestratorNamePattern.MatchString(value) {
			return "", fmt.Errorf("route_orchestrator_name: invalid orchestrator name %q", value)
		}
		return value, nil
	case "route_probe_timeout_s":
		if _, err := parseRouteProbeTimeout(value); err != nil {
			return "", err
		}
		return value, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownKey, key)
	}
}

func valueOf(cfg Config, key string) string {
	v, err := cfg.Value(key)
	if err != nil {
		return ""
	}
	return v
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// RouteMachineList returns route_machines as a list (nil when empty).
func (c Config) RouteMachineList() []string {
	return routeList(c.RouteMachines)
}

// RouteDisabledList returns route_disabled as a list (nil when empty).
func (c Config) RouteDisabledList() []string {
	return routeList(c.RouteDisabled)
}

// routeList splits a normalized route list (nil when empty).
func routeList(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

// parseLabelList validates a route_machines or route_disabled value: a
// comma-separated list of machine labels. Each item is trimmed of surrounding
// spaces and must match router.LabelPattern; the empty value is valid, while
// an empty item, a non-matching label, a duplicate and more than
// maxRouteListItems items are not.
func parseLabelList(key, value string) ([]string, error) {
	if value == "" {
		return nil, nil
	}
	seen := make(map[string]struct{})
	var list []string
	for _, item := range strings.Split(value, ",") {
		label := strings.TrimSpace(item)
		if label == "" {
			return nil, fmt.Errorf("%s: empty machine label", key)
		}
		if !router.LabelPattern.MatchString(label) {
			return nil, fmt.Errorf("%s: invalid machine label %q", key, label)
		}
		if _, dup := seen[label]; dup {
			return nil, fmt.Errorf("%s: duplicate machine label %q", key, label)
		}
		seen[label] = struct{}{}
		list = append(list, label)
		if len(list) > maxRouteListItems {
			return nil, fmt.Errorf("%s: at most %d machine labels", key, maxRouteListItems)
		}
	}
	return list, nil
}

// parseRouteProbeTimeout validates a route_probe_timeout_s value: an integer
// from 1 to 120.
func parseRouteProbeTimeout(value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n < routeProbeTimeoutMin || n > routeProbeTimeoutMax {
		return 0, fmt.Errorf("route_probe_timeout_s must be an integer from 1 to 120")
	}
	return n, nil
}
