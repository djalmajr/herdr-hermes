package router

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/soho"
)

// defaultProbeBound bounds a probe whose context carries no deadline.
const defaultProbeBound = 20 * time.Second

// minProbeBound is the smallest bound handed to the runner: a deadline
// that is already at or past still gets a runnable child and a fast,
// mapped timeout.
const minProbeBound = time.Millisecond

// drainBound bounds the wait for the probe output delivery to finish
// after the runner has returned.
const drainBound = 10 * time.Second

// NewExec returns an Exec that runs bin (the herdr executable) as an
// argv subprocess: never a shell, exactly environ as the child
// environment (nothing added, nothing removed), an empty stdin and a
// discarded stderr. Stdout is kept at most MaxProbeOutput+1 bytes; a
// longer output is discarded beyond the cap while every write still
// reports the full length written, so the reader keeps going.
//
// The bound handed to the runner is the time left until the context's
// deadline: defaultProbeBound when the context has none, at least
// minProbeBound. A child the deadline cancels is an error wrapping
// ErrProbeTimeout; a missing or unstartable executable is an error
// wrapping ErrHerdrUnavailable; any other runner error is returned as
// is. A run that did not hit the deadline returns the (capped) stdout
// and the child exit code with err nil, even when the exit code is
// non-zero. Every call builds a fresh runner, which is not safe for
// concurrent use.
func NewExec(bin string, environ []string) Exec {
	return func(ctx context.Context, argv []string) ([]byte, int, error) {
		bound := defaultProbeBound
		if deadline, ok := ctx.Deadline(); ok {
			bound = time.Until(deadline)
		}
		if bound < minProbeBound {
			bound = minProbeBound
		}
		var stdout cappedWriter
		stdout.limit = MaxProbeOutput + 1
		runner := soho.Runner{Bin: bin, Environ: environ, Now: time.Now}
		code, err := runner.Run(ctx, argv, bytes.NewReader(nil), &stdout, io.Discard, bound)
		if waitErr := awaitDrain(runner); err == nil {
			err = waitErr
		}
		var unavailable *soho.ErrUnavailable
		var deadlineErr *soho.ErrDeadline
		switch {
		case err != nil && errors.As(err, &unavailable):
			return nil, -1, fmt.Errorf("%w: %s", ErrHerdrUnavailable, unavailable.Error())
		case err != nil && errors.As(err, &deadlineErr):
			return nil, -1, fmt.Errorf("%w: %s", ErrProbeTimeout, deadlineErr.Error())
		}
		return stdout.data, code, err
	}
}

// awaitDrain waits for the runner's most recent Run to finish delivering
// the child's output to the probe's writers. The probe's writers are
// in-memory and never stall, so the wait completes at once in practice;
// the bound only trips if the delivery goroutine is lost.
func awaitDrain(r soho.Runner) error {
	select {
	case <-r.DrainDone():
		return nil
	case <-time.After(drainBound):
		return fmt.Errorf("router: probe output delivery did not finish within %s", drainBound)
	}
}

// cappedWriter keeps at most limit bytes of what is written to it and
// silently discards the rest, while every Write still reports the full
// length of the slice it was given.
type cappedWriter struct {
	data  []byte
	limit int
}

// Write implements io.Writer.
func (w *cappedWriter) Write(p []byte) (int, error) {
	if len(w.data) < w.limit {
		room := w.limit - len(w.data)
		if len(p) > room {
			p = p[:room]
		}
		w.data = append(w.data, p...)
	}
	return len(p), nil
}
