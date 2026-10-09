// Package config manages the herdr-hermes user configuration file: a
// key=value file under the user configuration directory, with defaults for
// every known key. The dispatcher API key never lives in this file.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// Config is the machine configuration with effective values (defaults
// applied for absent keys).
type Config struct {
	MachineLabel  string `json:"machine_label"`
	DispatcherURL string `json:"dispatcher_url"`
	HerdrSohoBin  string `json:"herdr_soho_bin"`
	PushTimeoutS  int    `json:"push_timeout_s"`
}

// ErrUnknownKey marks an attempt to set or read a key the configuration does
// not define.
var ErrUnknownKey = fmt.Errorf("unknown configuration key")

// defaultConfig returns the defaults: machine_label and dispatcher_url
// empty, herdr_soho_bin "herdr-soho", push_timeout_s 15.
func defaultConfig() Config {
	return Config{HerdrSohoBin: "herdr-soho", PushTimeoutS: 15}
}

// Keys lists every known configuration key in canonical order.
func Keys() []string {
	return []string{"machine_label", "dispatcher_url", "herdr_soho_bin", "push_timeout_s"}
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
		default:
			return cfg, fmt.Errorf("%w: %q", ErrUnknownKey, key)
		}
	}
	return cfg, nil
}

// Set writes one key of the configuration file atomically. Unknown keys and
// invalid push_timeout_s values are refused.
func Set(dir, key, value string) error {
	switch key {
	case "machine_label", "dispatcher_url", "herdr_soho_bin":
	case "push_timeout_s":
		if n, err := strconv.Atoi(value); err != nil || n <= 0 {
			return fmt.Errorf("push_timeout_s must be a positive integer")
		}
	default:
		return fmt.Errorf("%w: %q", ErrUnknownKey, key)
	}
	cfg, err := Load(dir)
	if err != nil {
		return err
	}
	switch key {
	case "machine_label":
		cfg.MachineLabel = value
	case "dispatcher_url":
		cfg.DispatcherURL = value
	case "herdr_soho_bin":
		cfg.HerdrSohoBin = value
	case "push_timeout_s":
		cfg.PushTimeoutS = atoi(value)
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
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownKey, key)
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
