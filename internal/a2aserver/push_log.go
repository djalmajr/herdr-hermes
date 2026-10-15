package a2aserver

// The push log logger of the gate: the SDK sender logs the error of a
// failed send, and that error carries the webhook URL, or parts of it
// — a parse error keeps the path of the URL, and the URL may carry a
// secret — so the logger the gate attaches to the context before
// delegating to the inner sender replaces the value of every
// attribute whose value is an error by one fixed classification of
// the error, with none of its text. The serve's URL mask is the
// second layer for the other paths.

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"strings"
)

// The fixed classifications of one failed push send, in the order the
// checks run: the first match wins.
const (
	pushClassInvalidURL  = "invalid push URL"
	pushClassTimeout     = "push request timed out"
	pushClassUnreachable = "push endpoint unreachable"
	pushClassNonSuccess  = "push endpoint answered a non-success status"
	pushClassSendFailed  = "push send failed"
)

// pushErrorClass returns the one fixed classification of a failed
// push send error: never the error text, which carries the webhook
// URL or parts of it, and the URL may carry a secret.
//
// The SDK's SSRF guard has no exported sentinel (its blocked-target
// error is unexported), so a guard refusal is a transport error — a
// *net.OpError under the *url.Error of the dial — and lands in the
// unreachable class.
func pushErrorClass(err error) string {
	var uerr *url.Error
	// A parse failure of the webhook URL: the error of
	// http.NewRequestWithContext, a *url.Error whose Op is parse and
	// whose text keeps the path of the URL.
	if errors.As(err, &uerr) && uerr.Op == "parse" {
		return pushClassInvalidURL
	}
	// The deadline of the send context, or a transport timeout.
	if errors.Is(err, context.DeadlineExceeded) {
		return pushClassTimeout
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return pushClassTimeout
	}
	// A transport error of the send (the SSRF guard's refusal among
	// them): a *url.Error of the request or a *net.OpError.
	if errors.As(err, &uerr) {
		return pushClassUnreachable
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return pushClassUnreachable
	}
	// The non-success status error of the SDK sender: its text has the
	// status only, but the fixed class is logged instead of it.
	if strings.Contains(err.Error(), "push notification endpoint returned non-success status") {
		return pushClassNonSuccess
	}
	return pushClassSendFailed
}

// pushSafeHandler wraps one slog.Handler: every record that reaches it
// is forwarded with the value of every attribute whose value is an
// error replaced by the fixed classification of the error
// (pushErrorClass), so the sender's error text — which carries the
// webhook URL, or parts of it, and the URL may carry a secret — never
// reaches the log. The message and the other attributes are
// untouched.
type pushSafeHandler struct{ next slog.Handler }

// Enabled implements slog.Handler.
func (h pushSafeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle implements slog.Handler: it rebuilds the record with the
// error attributes replaced by their fixed classification and
// forwards it to the wrapped handler.
func (h pushSafeHandler) Handle(ctx context.Context, r slog.Record) error {
	nr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		if err, ok := a.Value.Any().(error); ok {
			nr.AddAttrs(slog.String(a.Key, pushErrorClass(err)))
			return true
		}
		nr.AddAttrs(a)
		return true
	})
	return h.next.Handle(ctx, nr)
}

// WithAttrs implements slog.Handler.
func (h pushSafeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return pushSafeHandler{next: h.next.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler.
func (h pushSafeHandler) WithGroup(group string) slog.Handler {
	return pushSafeHandler{next: h.next.WithGroup(group)}
}

// newPushSafeLogger wraps the handler of one logger with the
// classification handler: the logger the gate attaches to the send
// context.
func newPushSafeLogger(logger *slog.Logger) *slog.Logger {
	return slog.New(pushSafeHandler{next: logger.Handler()})
}
