package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Env carries every dependency a command needs from the process. Tests
// replace each field; ConfigDir is the herdr-hermes directory itself (in
// production <user config dir>/herdr-hermes), never its parent.
type Env struct {
	Stdin          io.Reader
	Stdout, Stderr io.Writer
	Getenv         func(string) string
	// Environ returns the exact environment of the herdr-hermes process;
	// forwarded subprocesses get this environment and nothing else. The
	// API key is never in it because it is never put there.
	Environ   func() []string
	ConfigDir string
	Now       func() time.Time
	Sleep     func(context.Context, time.Duration) error
}

// EnvFromOS builds the production Env. When the user config directory
// cannot be determined, ConfigDir stays empty: Run then refuses every
// command that needs the config dir with exit 2 instead of silently writing
// state to a shared temp dir.
func EnvFromOS() Env {
	env := Env{
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Getenv:  os.Getenv,
		Environ: os.Environ,
		Now:     time.Now,
		Sleep:   sleepCtx,
	}
	if cfg, err := os.UserConfigDir(); err == nil && cfg != "" {
		env.ConfigDir = filepath.Join(cfg, "herdr-hermes")
	}
	return env
}

// sleepCtx sleeps for d or until the context is done, returning the context
// error when it is.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
