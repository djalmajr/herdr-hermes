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
	"sync"
	"sync/atomic"
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

// ErrDeadline reports that the bound expired and the cancel reached a
// live child, whatever exit code that child then ended with.
type ErrDeadline struct {
	Bound time.Duration
}

func (e *ErrDeadline) Error() string {
	return "soho: child killed after " + e.Bound.String()
}

// Runner executes the herdr-soho binary. Bin is an argv0 (a bare name on
// PATH or an absolute path); Environ is the exact child environment, and
// nothing else is inherited by the child. Run is not safe for
// concurrent use on one Runner.
type Runner struct {
	Bin     string
	Environ []string
	Now     func() time.Time
	// Diag, when non-nil, receives exactly one friction diagnostic when
	// a child exits 0 on its own but its output does not drain within
	// the grace period. The message names the subcommand only: never
	// child output, the child environment or the credential key.
	Diag func(msg string)
	// Capture, when non-nil, receives the child's stdout on the copy
	// path, before any consumer delivery: whatever the child wrote is
	// in Capture by the time Run returns, even while the consumer
	// delivery is still in flight — the job forwarder's bookkeeping
	// buffer.
	Capture *bytes.Buffer

	drainDone chan struct{}
}

// DrainDone returns a channel that closes when the most recent Run's
// delivery of the child's output to the given writers has finished. It
// is never nil: until Run replaces it, it is an already-closed channel,
// so a caller that waits on it after any Run (including one that
// returned early) blocks for nothing and a reused Runner never reports
// a previous run's channel. A caller that must not lose output waits on
// it after Run returned; the Capture buffer needs no such wait, because
// it is written on the copy path that Run has already waited for.
func (r *Runner) DrainDone() <-chan struct{} {
	return r.drainDone
}

// awaitDrainDone waits for the most recent Run's delivery to the given
// writers to finish. The client methods pass internal buffers that
// never stall, so the delivery always completes; the bound only trips
// if the delivery goroutine is lost.
func (r *Runner) awaitDrainDone() error {
	select {
	case <-r.DrainDone():
		return nil
	case <-time.After(10 * time.Second):
		return errors.New("soho: output delivery did not finish")
	}
}

// joinDone returns a channel that closes when both a and b have closed.
func joinDone(a, b chan struct{}) chan struct{} {
	c := make(chan struct{})
	go func() {
		<-a
		<-b
		close(c)
	}()
	return c
}

// Run executes Bin with args (an argv array, never a shell) under a
// context deadline of bound plus the WaitDelay grace period. stdin is
// streamed to the child; the child's stdout and stderr are copied
// unchanged to the given writers. The writers are guarded: a consumer
// that stalls cannot hold the run past the deadline, the deadline is
// decided by the bound and the grace period, never by the writer. It
// returns the child exit code unchanged when the child exited on its
// own — including while its stdout was still draining past the bound
// or past the grace period — and ErrDeadline when the cancel reached a
// live child; the deadline is decided from the cancel, because on
// Windows the kill surfaces as an ordinary exit code (TerminateProcess).
// ErrUnavailable is returned when the executable is missing or not
// runnable. Every byte Run read from the child is handed to the given
// writers, but that delivery may still be in flight when Run returns,
// so a caller that must not lose output waits on DrainDone after Run
// returned.
func (r *Runner) Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, bound time.Duration) (int, error) {
	// DrainDone is an already-closed channel from the first line, so it
	// is never nil and never refers to a previous run after any early
	// return (a missing binary, a failed start, or a reused Runner).
	drained := make(chan struct{})
	close(drained)
	r.drainDone = drained
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
	var interrupted atomic.Bool
	if stdin != nil {
		cmd.Stdin = stdin
	}
	out := newGuardedWriter(stdout, r.Capture)
	defer out.close()
	if stdout != nil {
		cmd.Stdout = out
	}
	errOut := newGuardedWriter(stderr, nil)
	defer errOut.close()
	if stderr != nil {
		cmd.Stderr = errOut
	}
	cmd.WaitDelay = waitDelay
	boundedCancel(cmd, &interrupted)
	r.drainDone = joinDone(out.done, errOut.done)
	if err := cmd.Start(); err != nil {
		return -1, &ErrUnavailable{Bin: r.Bin, Err: err}
	}
	waitErr := cmd.Wait()
	// The writers are closed (deferred, not awaited) once the wait
	// machinery has decided the run: a delivery that is still in flight
	// may finish into the wrapped writer after Run returned.
	// The deadline is decided from the cancel, not from the context or
	// from how the child died: the flag is set only when the signal or
	// kill reached a live child, so a child we cancelled keeps its
	// deadline whatever exit code it ended with — the Windows
	// TerminateProcess code 1 and a unix child that exits on SIGTERM
	// both stay deadlines.
	if interrupted.Load() {
		return -1, &ErrDeadline{Bound: bound}
	}
	// Whatever Wait returned (nil, the child's own ExitError, the
	// expired context after the child had already gone, or the expired
	// grace period while its output was still draining), a child that
	// exited on its own keeps its real code: the bound or the grace
	// period firing is not the child's cause of death.
	if st := cmd.ProcessState; st != nil {
		if code := st.ExitCode(); code >= 0 {
			// One friction diagnostic when the grace period expired on a
			// successful exit: the output may be truncated.
			if code == 0 && errors.Is(waitErr, exec.ErrWaitDelay) {
				r.diagWaitDelay(args)
			}
			return code, nil
		}
	}
	// No exit code: the child was killed by a signal we did not send, or
	// no state at all; the deadline stays the fallback.
	return -1, &ErrDeadline{Bound: bound}
}

// guardedWriter delivers Writes to the wrapped writer through a single
// dedicated goroutine, in order. The public Write copies p into a queue
// and returns immediately: it never blocks on the wrapped writer, so
// the copying goroutine that os/exec waits for cannot be held by a
// consumer that stalls, and the deadline stays decided by the wait
// machinery (the bound and the WaitDelay grace period), not by the
// writer. When a capture buffer is set, Write also appends p to it
// here, on the copy path: Run waits for the copy before it returns, so
// the capture holds everything the child wrote whenever Run does, no
// matter how the consumer delivery lags. close never waits either — it
// flips a flag: bytes already queued are still delivered, a delivery in
// flight may finish into the wrapped writer after Run returned (a
// caller that must not lose output waits on the runner's DrainDone
// after Run returned), and every Write that starts after close is
// dropped.
type guardedWriter struct {
	w      io.Writer
	cap    *bytes.Buffer
	closed atomic.Bool
	mu     sync.Mutex
	queue  [][]byte
	wake   chan struct{}
	done   chan struct{}
}

func newGuardedWriter(w io.Writer, cap *bytes.Buffer) *guardedWriter {
	g := &guardedWriter{w: w, cap: cap, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go g.drainLoop()
	return g
}

// Write queues p for consumer delivery and reports success immediately
// — whether the delivery happens later or is dropped after close — so
// the copying goroutine keeps going. When a capture is set, it is
// written first, synchronously, on the copy path.
func (g *guardedWriter) Write(p []byte) (int, error) {
	if g.w == nil || g.closed.Load() {
		return len(p), nil
	}
	if g.cap != nil {
		_, _ = g.cap.Write(p)
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	g.mu.Lock()
	g.queue = append(g.queue, cp)
	g.mu.Unlock()
	select {
	case g.wake <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (g *guardedWriter) drainLoop() {
	defer close(g.done)
	for {
		if g.drainOne() {
			continue
		}
		if g.closed.Load() && g.queueEmpty() {
			return
		}
		<-g.wake
	}
}

// drainOne delivers the next queued byte slice to the wrapped writer,
// which may block for an arbitrary time, or reports the queue empty. It
// is the only place that touches the wrapped writer.
func (g *guardedWriter) drainOne() bool {
	g.mu.Lock()
	var chunk []byte
	if len(g.queue) > 0 {
		chunk = g.queue[0]
		g.queue = g.queue[1:]
	}
	g.mu.Unlock()
	if chunk == nil {
		return false
	}
	_, _ = g.w.Write(chunk)
	return true
}

func (g *guardedWriter) queueEmpty() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.queue) == 0
}

func (g *guardedWriter) close() {
	g.closed.Store(true)
	// Wake the drain in case it is sleeping: it re-checks the flag and
	// exits once the queue is empty.
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

// diagWaitDelay reports a successful exit whose output did not drain
// within the grace period. The message names the subcommand only —
// never child output, the child environment or the credential key.
func (r *Runner) diagWaitDelay(args []string) {
	if r.Diag == nil {
		return
	}
	sub := ""
	if len(args) > 0 {
		sub = args[0]
	}
	if len(args) > 1 {
		sub += " " + args[1]
	}
	r.Diag("herdr-soho " + sub + " exited 0 but its output did not drain within " +
		waitDelay.String() + "; output may be truncated")
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
	if err := r.awaitDrainDone(); err != nil {
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
	if err := r.awaitDrainDone(); err != nil {
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
