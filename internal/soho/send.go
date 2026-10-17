package soho

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/djalmajr/nau/internal/notify"
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

// notReadyMarker is the exact stderr substring of a send refused
// because the recipient exists but is not active yet, its `herdr
// agent start` still running: soho prints it and exits 4. Any other
// exit 4 (a missing agent, Herdr down, a permission error) is not a
// not-ready refusal and is never retried.
const notReadyMarker = " unavailable: agent_not_ready:"

// notReadyPauses are the pauses before the in-Send retries of a
// not-ready refusal: 1s, 2s and 4s — at most three more runs after the
// first, four runs in total. The tests replace the sleep that applies
// them, so the production values are exactly these.
var notReadyPauses = []time.Duration{
	time.Second,
	2 * time.Second,
	4 * time.Second,
}

// notReadySkipBound is added to a pause in the deadline check: when the
// caller's deadline leaves less than the pause plus this bound, the
// retry does not run and the not-ready result is returned at once.
const notReadySkipBound = 2 * time.Second

// notReadySleep pauses for d or until ctx ends, whichever comes first.
// The tests replace it, so the in-Send retries never sleep for real.
var notReadySleep = ctxSleep

// ctxSleep pauses for d or until ctx ends, whichever comes first.
func ctxSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// NotifySender delivers notification messages with `soho send`.
// The child never receives the process's HERDR_PANE_ID: the
// notification is sent by Nau itself, not by a pane, so soho
// must not attribute the message to the pane that fired the hook or
// that runs the serve, and must not refuse it as a worker report when
// that pane is a worker. Every other variable of Environ is forwarded
// as is. A send refused with agent_not_ready (the recipient still
// starting) is retried inside the call, after pauses of 1s, 2s and
// 4s, within the caller's deadline; no other exit is retried.
type NotifySender struct {
	Bin     string        // argv0, as soho.Runner.Bin
	Environ []string      // exact child environment, as soho.Runner.Environ
	Timeout time.Duration // passed as --timeout <ms>; 0 means 5s
	// Bound is the process bound handed to Runner.Run; 0 means Timeout
	// plus sendGrace. A send to a busy target can take longer than its
	// --timeout while soho observes the receipt, so a caller with
	// a larger budget passes it here instead of killing the send in
	// flight.
	Bound time.Duration
}

// Send implements notify.Sender. It refuses to run the command for an
// invalid ref or message; otherwise it runs `soho send <ref>
// <message> --now --timeout <ms>` as an argv subprocess on a fresh
// Runner (the runner is not safe for concurrent use, and Send is)
// with a copy of Environ without the HERDR_PANE_ID entry — the child
// does not inherit the pane identity of the calling process, because
// the notification is sent by Nau, not by a pane — bounds the process
// at Timeout plus sendGrace, and maps the child
// outcome to a fixed SendResult. The child's stdout is kept bounded
// only to classify the status word: it is never returned, logged or
// stored. A send refused with agent_not_ready (the recipient exists
// but is not active yet: exit 4 with notReadyMarker in the captured
// stderr) is run up to three more times within the same call, after
// pauses of 1s, 2s and 4s, skipping the rest when the caller's
// deadline leaves less than the pause plus 2s; every other outcome
// (accepted, uncertain, deadline, unavailable, any other exit) is
// returned at once, never retried.
func (s NotifySender) Send(ctx context.Context, ref, message string) notify.SendResult {
	if !notify.ValidRef(ref) || !validMessage(message) {
		return notify.SendResult{Outcome: notify.OutcomeRejected, Exit: -1, Category: "usage"}
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = sendDefaultTimeout
	}
	argv := []string{"send", ref, message, "--now", "--timeout", strconv.FormatInt(timeout.Milliseconds(), 10)}
	bound := s.Bound
	if bound <= 0 {
		bound = timeout + sendGrace
	}
	exit, err, out, errOut := s.runSend(ctx, argv, bound)
	// A not-ready refusal means the recipient exists but its start has
	// not finished: run one more attempt after each pause of
	// notReadyPauses, within the same call. Before each pause, a
	// caller deadline that leaves less than the pause plus
	// notReadySkipBound skips the rest, and a context that ends during
	// a pause returns the not-ready result without another run. A run
	// that failed to exit (deadline, unavailable) is returned at once:
	// the message may already have reached the target, so it is never
	// retried.
	for i := 0; err == nil && isNotReady(exit, errOut) && i < len(notReadyPauses); i++ {
		pause := notReadyPauses[i]
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < pause+notReadySkipBound {
			return notReadyResult()
		}
		if perr := notReadySleep(ctx, pause); perr != nil {
			return notReadyResult()
		}
		exit, err, out, errOut = s.runSend(ctx, argv, bound)
	}
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
	case 4:
		// Exit 4 after its in-Send retries: only a not-ready refusal is
		// reported as such; the other exit 4s keep the ordinary mapping.
		if isNotReady(exit, errOut) {
			return notReadyResult()
		}
		return notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "exit"}
	default:
		return notify.SendResult{Outcome: notify.OutcomeTransient, Exit: exit, Category: "exit"}
	}
}

// runSend runs one `soho send` attempt on a fresh runner (the runner
// is not safe for concurrent use) with a copy of Environ without the
// HERDR_PANE_ID entry and the process bound, and waits for the
// delivery of the child's output before returning: the runner's
// guarded writers deliver asynchronously and Run may return while
// delivery is in flight. The stdout is kept only to classify the
// status word, and the stderr only to classify an agent_not_ready
// refusal: neither is returned, logged or stored.
func (s NotifySender) runSend(ctx context.Context, argv []string, bound time.Duration) (int, error, *captureWriter, *captureWriter) {
	r := Runner{Bin: s.Bin, Environ: withoutPaneID(s.Environ)}
	var out, errOut captureWriter
	exit, err := r.Run(ctx, argv, nil, &out, &errOut, bound)
	// Wait for the copy to finish delivering the child's output to the
	// captures before classifying: the runner's guarded writers
	// deliver asynchronously and Run may return while delivery is in
	// flight.
	_ = r.awaitDrainDone()
	return exit, err, &out, &errOut
}

// notReadyResult is the SendResult of a send whose attempts were all
// refused as not-ready, or whose retry was skipped for lack of time.
func notReadyResult() notify.SendResult {
	return notify.SendResult{Outcome: notify.OutcomeTransient, Exit: 4, Category: "not_ready"}
}

// isNotReady reports whether the run was refused as not-ready: exit 4
// with the exact marker in the captured stderr, which must be
// delivered first (see runSend).
func isNotReady(exit int, errOut *captureWriter) bool {
	return exit == 4 && errOut != nil && strings.Contains(string(errOut.data), notReadyMarker)
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

// paneIDKey is the environment variable the backend uses to identify
// the sending pane. A notification is sent by Nau, not by a pane, so
// the send child never receives it: the backend must not attribute the
// message to the pane that fired the hook or that runs the serve, and
// must not refuse it as a worker report when that pane is a worker.
const paneIDKey = "HERDR_PANE_ID"

// withoutPaneID returns env without the entries whose key is exactly
// HERDR_PANE_ID (the key is compared before the first '='; on Windows
// the comparison is case-insensitive, because environment keys there
// are case-insensitive). Longer keys such as HERDR_PANE_ID_EXTRA are
// kept. It never mutates env, keeps the remaining entries in order,
// and returns env unchanged (nil stays nil) when nothing is removed.
func withoutPaneID(env []string) []string {
	kept := make([]string, 0, len(env))
	for _, kv := range env {
		if !keyIsPaneID(kv) {
			kept = append(kept, kv)
		}
	}
	if len(kept) == len(env) {
		return env
	}
	return kept
}

// keyIsPaneID reports whether the environment entry kv has the key
// HERDR_PANE_ID exactly.
func keyIsPaneID(kv string) bool {
	key := kv
	if i := strings.IndexByte(kv, '='); i >= 0 {
		key = kv[:i]
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(key, paneIDKey)
	}
	return key == paneIDKey
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
