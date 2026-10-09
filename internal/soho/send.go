package soho

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/notify"
)

// sendGrace is added to the caller's timeout to form the process bound
// handed to Runner.Run; the runner adds its own WaitDelay grace on top.
const sendGrace = 3 * time.Second

// sendDefaultTimeout is used when NotifySender.Timeout is 0.
const sendDefaultTimeout = 5 * time.Second

// sendMaxMessage is the largest message accepted, in bytes.
const sendMaxMessage = 2048

// sendCaptureLimit bounds how many bytes of the child's stdout are kept
// for classification; the rest is dropped.
const sendCaptureLimit = 4096

// NotifySender delivers notification messages with `herdr-soho send`.
type NotifySender struct {
	Bin     string        // argv0, as soho.Runner.Bin
	Environ []string      // exact child environment, as soho.Runner.Environ
	Timeout time.Duration // passed as --timeout <ms>; 0 means 5s
	// Bound is the process bound handed to Runner.Run; 0 means Timeout
	// plus sendGrace. A send to a busy target can take longer than its
	// --timeout while herdr-soho observes the receipt, so a caller with
	// a larger budget passes it here instead of killing the send in
	// flight.
	Bound time.Duration
}

// Send implements notify.Sender. It refuses to run the command for an
// invalid ref or message; otherwise it runs `herdr-soho send <ref>
// <message> --now --timeout <ms>` as an argv subprocess on a fresh
// Runner (the runner is not safe for concurrent use, and Send is),
// bounds the process at Timeout plus sendGrace, and maps the child
// outcome to a fixed SendResult. The child's stdout is kept bounded
// only to classify the status word: it is never returned, logged or
// stored.
func (s NotifySender) Send(ctx context.Context, ref, message string) notify.SendResult {
	if !notify.ValidRef(ref) || !validMessage(message) {
		return notify.SendResult{Outcome: notify.OutcomeRejected, Exit: -1, Category: "usage"}
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = sendDefaultTimeout
	}
	r := Runner{Bin: s.Bin, Environ: s.Environ}
	var out captureWriter
	argv := []string{"send", ref, message, "--now", "--timeout", strconv.FormatInt(timeout.Milliseconds(), 10)}
	bound := s.Bound
	if bound <= 0 {
		bound = timeout + sendGrace
	}
	exit, err := r.Run(ctx, argv, nil, &out, io.Discard, bound)
	if err != nil {
		var ue *ErrUnavailable
		if errors.As(err, &ue) {
			return notify.SendResult{Outcome: notify.OutcomeTransient, Exit: -1, Category: "unavailable"}
		}
		// Every other error from Run is a deadline (ErrDeadline,
		// including a done caller context): the child was started and
		// killed in flight, so the message may already have reached the
		// target. Receipt is not proved, exactly like exit 15: the
		// delivery is uncertain and never resent automatically, so a
		// message is never injected twice.
		return notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: -1, Category: "deadline"}
	}
	// Wait for the copy to finish delivering the child's stdout to the
	// capture before classifying: the runner's guarded writer delivers
	// asynchronously and Run may return while delivery is in flight.
	_ = r.awaitDrainDone()
	switch exit {
	case 0:
		return notify.SendResult{Outcome: notify.OutcomeAccepted, Status: statusWord(out.data), Exit: 0, Category: ""}
	case 15:
		return notify.SendResult{Outcome: notify.OutcomeUncertain, Exit: 15, Category: "uncertain"}
	case 2:
		return notify.SendResult{Outcome: notify.OutcomeRejected, Exit: 2, Category: "usage"}
	case 3:
		return notify.SendResult{Outcome: notify.OutcomeRejected, Exit: 3, Category: "unknown_agent"}
	case 18:
		return notify.SendResult{Outcome: notify.OutcomeRejected, Exit: 18, Category: "refused"}
	case 17:
		return notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 17, Category: "busy"}
	default:
		return notify.SendResult{Outcome: notify.OutcomeTransient, Exit: exit, Category: "exit"}
	}
}

// validMessage reports whether the message is non-empty, at most
// sendMaxMessage bytes, does not start with '-' and carries no newline,
// carriage return or other control byte (< 0x20).
func validMessage(m string) bool {
	if m == "" || len(m) > sendMaxMessage {
		return false
	}
	if m[0] == '-' {
		return false
	}
	for i := 0; i < len(m); i++ {
		if m[i] < 0x20 {
			return false
		}
	}
	return true
}

// statusWord classifies the first line of the child's stdout, trimmed
// and lower-cased: a line starting with "sent" or "queued" reports that
// word; anything else (including empty output) reports "accepted".
func statusWord(data []byte) string {
	line := string(data)
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	line = strings.ToLower(strings.TrimSpace(line))
	switch {
	case strings.HasPrefix(line, "sent"):
		return "sent"
	case strings.HasPrefix(line, "queued"):
		return "queued"
	default:
		return "accepted"
	}
}

// captureWriter keeps at most the first sendCaptureLimit bytes of the
// stream and drops everything after the bound. It never blocks and
// never reports an error. The runner delivers to it in order through a
// single goroutine, so it needs no lock of its own.
type captureWriter struct {
	data []byte
	full bool
}

func (w *captureWriter) Write(p []byte) (int, error) {
	if !w.full {
		room := sendCaptureLimit - len(w.data)
		if len(p) < room {
			w.data = append(w.data, p...)
		} else {
			w.data = append(w.data, p[:room]...)
			w.full = true
		}
	}
	return len(p), nil
}
