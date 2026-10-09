// Package soho runs the herdr-soho CLI as an argv subprocess: the bounded
// runner, the capabilities client and the job events reader. No shell is
// ever involved; every subprocess gets a context deadline and a WaitDelay.
package soho

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/jobapi"
)

// waitDelay gives a child that ignores the cancel signal a bounded grace
// period before the process is killed.
const waitDelay = 5 * time.Second

// defaultBound bounds the subprocesses without a caller-specific bound
// (capabilities, events and the job forwarder subs without a wait).
const defaultBound = 120 * time.Second

// ErrUnavailable reports that the herdr-soho executable could not be
// found or started.
type ErrUnavailable struct {
	Bin string
	Err error
}

func (e *ErrUnavailable) Error() string {
	return "soho: herdr-soho unavailable: " + e.Err.Error()
}

// ErrDeadline reports that the bound expired and the child was killed
// before it produced an exit code.
type ErrDeadline struct {
	Bound time.Duration
}

func (e *ErrDeadline) Error() string {
	return "soho: child killed after " + e.Bound.String()
}

// Runner executes the herdr-soho binary. Bin is an argv0 (a bare name on
// PATH or an absolute path); Environ is the exact child environment, and
// nothing else is inherited by the child.
type Runner struct {
	Bin     string
	Environ []string
	Now     func() time.Time
}

// Run executes Bin with args (an argv array, never a shell) under a
// context deadline of bound plus the WaitDelay grace period. stdin is
// streamed to the child; the child's stdout and stderr are copied
// unchanged to the given writers. It returns the child exit code
// unchanged when the child exits; ErrUnavailable when the executable is
// missing or not runnable; ErrDeadline when the bound expired and the
// child was killed without an exit code.
func (r *Runner) Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, bound time.Duration) (int, error) {
	if bound <= 0 {
		bound = defaultBound
	}
	bin, err := exec.LookPath(r.Bin)
	if err != nil {
		return -1, &ErrUnavailable{Bin: r.Bin, Err: err}
	}
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = r.Environ
	if stdin != nil {
		cmd.Stdin = stdin
	}
	if stdout != nil {
		cmd.Stdout = stdout
	}
	if stderr != nil {
		cmd.Stderr = stderr
	}
	cmd.WaitDelay = waitDelay
	boundedCancel(cmd)
	if err := cmd.Start(); err != nil {
		return -1, &ErrUnavailable{Bin: r.Bin, Err: err}
	}
	err = cmd.Wait()
	if err == nil {
		return 0, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if code := ee.ExitCode(); code >= 0 {
			return code, nil
		}
	}
	return -1, &ErrDeadline{Bound: bound}
}

// Caps is the parsed `herdr-soho capabilities --json` output. Raw keeps
// the original line so doctor can report it.
type Caps struct {
	Raw  string
	vals map[string]float64
}

// Has reports whether the capability key is present with a value of at
// least 1 (a boolean true counts as 1).
func (c Caps) Has(key string) bool {
	v, ok := c.vals[key]
	return ok && v >= 1
}

// Capabilities runs `herdr-soho capabilities --json` (bounded) and parses
// the last line leniently: unknown fields are ignored, numeric values are
// recorded and a boolean true counts as 1. A non-JSON line or a non-zero
// exit is an error.
func (r *Runner) Capabilities(ctx context.Context) (Caps, error) {
	var out bytes.Buffer
	exit, err := r.Run(ctx, []string{"capabilities", "--json"}, nil, &out, io.Discard, defaultBound)
	if err != nil {
		return Caps{}, err
	}
	if exit != 0 {
		return Caps{}, fmt.Errorf("soho: capabilities exited %d", exit)
	}
	raw := lastLine(out.String())
	if raw == "" {
		return Caps{}, errors.New("soho: capabilities printed no line")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return Caps{}, fmt.Errorf("soho: capabilities output is not a JSON line: %w", err)
	}
	c := Caps{Raw: raw, vals: map[string]float64{}}
	for k, v := range m {
		switch n := v.(type) {
		case float64:
			c.vals[k] = n
		case bool:
			if n {
				c.vals[k] = 1
			}
		}
	}
	return c, nil
}

// EventsResult is the output of `herdr-soho job events --id <id> --since
// <n>`: the raw event lines (without the trailer) and the parsed trailer.
type EventsResult struct {
	Lines   []string
	Trailer jobapi.EventsTrailer
}

// Events runs `herdr-soho job events --id <id> --since <since>` without
// --wait (bounded) and returns the event lines plus the trailer. The
// trailer must be the last line with eventos "fim"; anything else is an
// error.
func (r *Runner) Events(ctx context.Context, id string, since int64) (EventsResult, error) {
	var out bytes.Buffer
	args := []string{"job", "events", "--id", id, "--since", strconv.FormatInt(since, 10)}
	exit, err := r.Run(ctx, args, nil, &out, io.Discard, defaultBound)
	if err != nil {
		return EventsResult{}, err
	}
	if exit != 0 {
		return EventsResult{}, fmt.Errorf("soho: job events exited %d", exit)
	}
	lines := splitLines(out.String())
	if len(lines) == 0 {
		return EventsResult{}, errors.New("soho: job events printed no trailer")
	}
	tailer := lines[len(lines)-1]
	var tr jobapi.EventsTrailer
	if err := json.Unmarshal([]byte(tailer), &tr); err != nil || tr.Eventos != "fim" {
		return EventsResult{}, fmt.Errorf("soho: job events trailer is missing or malformed")
	}
	return EventsResult{Lines: lines[:len(lines)-1], Trailer: tr}, nil
}

// lastLine returns the last non-empty line of s, trimmed.
func lastLine(s string) string {
	lines := strings.Split(s, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if line := strings.TrimSpace(lines[i]); line != "" {
			return line
		}
	}
	return ""
}

// splitLines returns the non-empty lines of s, trimmed, in order.
func splitLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}
