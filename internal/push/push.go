// Package push delivers outbox records to the dispatcher: one POST per
// record, in seq order, with the per-user API key carried only in the
// Authorization header. Records are pushed one at a time; a 2xx advances
// the delivered_seq cursor, and stops (auth rejection, unreachable,
// rejected) leave the remaining records pending for the next push or the
// dispatcher pull. Errors from this package never contain the key or the
// Authorization header.
//
// TODO(herdr-hermes): verify the dispatcher receiver contract (path,
// response codes) once it exists.
package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// defaultTimeout bounds every request when the caller passes timeout <= 0.
const defaultTimeout = 15 * time.Second

// retrySchedule is the backoff between push retries: exactly 3 retries.
var retrySchedule = []time.Duration{10 * time.Second, 30 * time.Second, 90 * time.Second}

// maxResponseDrain bounds how much of a response body is read before the
// connection is released.
const maxResponseDrain = 8192

// Pusher delivers one outbox record and reports the dispatcher's HTTP
// status. A transport failure (no HTTP response) is reported through err;
// the status is 0 then.
type Pusher interface {
	Push(ctx context.Context, body []byte, idempotencyKey string) (status int, err error)
}

// TransportError reports a push request that got no HTTP response. The
// message is a fixed text plus the failure kind; it never carries the
// request (whose Authorization header holds the key), the URL or the body.
type TransportError struct {
	Kind string // "timeout", "canceled" or "connection"
}

func (e *TransportError) Error() string {
	return "push: transport " + e.Kind
}

// HTTP is a Pusher over one configured dispatcher URL.
type HTTP struct {
	rawURL  string
	key     string
	version string
	timeout time.Duration
	client  *http.Client
}

// NewHTTP builds a push client for rawURL with the given per-user key and
// bridge version. A URL that is not https:// is refused unless its host is
// a loopback address (localhost, 127.0.0.0/8 or ::1); an empty key is
// refused. timeout bounds each request; a non-positive timeout gets the
// 15 s default.
func NewHTTP(rawURL, key, version string, timeout time.Duration) (*HTTP, error) {
	if key == "" {
		return nil, errors.New("push: empty API key")
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, errors.New("push: invalid dispatcher URL")
	}
	// https is accepted for any host; http only on a loopback host (the
	// httptest case); any other scheme is refused.
	if u.Scheme != "https" && (u.Scheme != "http" || !isLoopback(u.Hostname())) {
		return nil, errors.New("push: dispatcher URL must be https unless it is http on a loopback host")
	}
	return &HTTP{
		rawURL:  rawURL,
		key:     key,
		version: version,
		timeout: timeout,
		client: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// isLoopback reports whether host is localhost, 127.0.0.0/8 or ::1.
func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Push POSTs one record line to the dispatcher. The request carries exactly
// the headers Authorization, Idempotency-Key, Content-Type and User-Agent
// (plus the protocol headers the transport adds). Redirects are not
// followed: a 3xx is returned as the status. The returned error, when
// present, is a *TransportError with a fixed message plus the failure kind.
func (h *HTTP) Push(ctx context.Context, body []byte, idempotencyKey string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.rawURL, bytes.NewReader(body))
	if err != nil {
		return 0, &TransportError{Kind: "connection"}
	}
	req.Header.Set("Authorization", "Bearer "+h.key)
	req.Header.Set("Idempotency-Key", idempotencyKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "herdr-hermes/"+h.version)
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, &TransportError{Kind: transportKind(err)}
	}
	defer resp.Body.Close()
	// Drain a bounded amount so the connection can be reused; the body is
	// never read further and never reaches an error message.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseDrain))
	return resp.StatusCode, nil
}

// transportKind classifies a failed request as a timeout, a cancellation
// or a connection problem.
func transportKind(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "connection"
	}
}

// Result is the outcome of one push batch.
type Result struct {
	Sent    int
	Pending int
	Code    int    // 0, 41 or 42
	Status  string // "", "auth_rejected", "unreachable" or "rejected"
}

// Pending pushes the records with seq > delivered_seq, one at a time, in
// seq order. A 2xx advances delivered_seq and continues. 401/403 stop the
// batch with outcome auth_rejected (code 41). 408/429/5xx, a transport
// error or a timeout are retried for the same record after 10 s, 30 s and
// 90 s when retry is true (exactly 3 retries, through the injected sleep);
// still failing stops with outcome unreachable (code 42). 3xx (redirects
// are not followed) and any other 4xx stop without retry with outcome
// rejected (code 42). Each stop writes one friction line (status code and
// outcome only, never the key or a body), and the batch records its result
// in cursor.json via SetLastPush. An error from sleep (a canceled context)
// stops the batch as unreachable.
func Pending(ctx context.Context, store *outbox.Store, p Pusher, sleep func(context.Context, time.Duration) error, retry bool) Result {
	delivered, err := store.DeliveredSeq()
	if err != nil {
		return Result{Code: 42, Status: "unreachable"}
	}
	lines, _, err := store.Read(delivered)
	if err != nil {
		return Result{Code: 42, Status: "unreachable"}
	}
	if len(lines) == 0 {
		return Result{}
	}
	sent := 0
	lastHTTP := 0
	for i, line := range lines {
		var rec outbox.Record
		if err := json.Unmarshal(line, &rec); err != nil || rec.Seq <= delivered {
			// A torn or unreadable line: nothing further in this batch
			// can be trusted; stop.
			outbox.Friction(store.Dir(), "push", "unreadable record outcome unreachable")
			return finish(store, sent, len(lines)-i, 42, "unreachable", 0)
		}
		status, tErr := pushWithRetries(ctx, p, line, rec.IdempotencyKey, sleep, retry)
		if tErr != nil {
			outbox.Friction(store.Dir(), "push", "transport "+kindOf(tErr)+" outcome unreachable")
			return finish(store, sent, len(lines)-i, 42, "unreachable", 0)
		}
		switch {
		case status >= 200 && status < 300:
			if err := store.SetDeliveredSeq(rec.Seq); err != nil {
				// Accepted but the cursor could not move: the next push
				// re-sends the record with the same Idempotency-Key.
				outbox.Friction(store.Dir(), "push", fmt.Sprintf("status %d outcome unreachable", status))
				return finish(store, sent, len(lines)-i, 42, "unreachable", status)
			}
			sent++
			lastHTTP = status
		case status == 401 || status == 403:
			outbox.Friction(store.Dir(), "push", fmt.Sprintf("status %d outcome auth_rejected", status))
			return finish(store, sent, len(lines)-i, 41, "auth_rejected", status)
		case status == 408 || status == 429 || status >= 500:
			// Retries are exhausted (or retry is false).
			outbox.Friction(store.Dir(), "push", fmt.Sprintf("status %d outcome unreachable", status))
			return finish(store, sent, len(lines)-i, 42, "unreachable", status)
		default:
			// 3xx (redirect not followed) and any other 4xx: stop without
			// retry.
			outbox.Friction(store.Dir(), "push", fmt.Sprintf("status %d outcome rejected", status))
			return finish(store, sent, len(lines)-i, 42, "rejected", status)
		}
	}
	return finish(store, sent, 0, 0, "", lastHTTP)
}

// pushWithRetries attempts the record once and, when retry is true and the
// outcome is retryable (408/429/5xx or a transport failure that is not a
// cancellation), up to 3 more times through the 10/30/90 s backoff. It
// returns as soon as an attempt yields a non-retryable HTTP status, when
// the backoff sleep gives up, or when the retries are exhausted: then the
// last status (0 with the last transport error when no attempt got an HTTP
// response) is returned.
func pushWithRetries(ctx context.Context, p Pusher, line []byte, key string, sleep func(context.Context, time.Duration) error, retry bool) (int, error) {
	status, err := p.Push(ctx, line, key)
	if !retry || !retryable(status, err) {
		return status, err
	}
	for _, d := range retrySchedule {
		if err := sleep(ctx, d); err != nil {
			return status, err
		}
		status, err = p.Push(ctx, line, key)
		if !retryable(status, err) {
			return status, err
		}
	}
	return status, err
}

// retryable reports whether one more attempt after a backoff is due. A
// canceled context is never retryable: the caller gave up.
func retryable(status int, err error) bool {
	if err != nil {
		var te *TransportError
		return errors.As(err, &te) && te.Kind != "canceled"
	}
	return status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
}

// kindOf names a transport error for the friction line.
func kindOf(err error) string {
	var te *TransportError
	if errors.As(err, &te) {
		return te.Kind
	}
	return "connection"
}

// finish records the batch result in cursor.json and returns the Result.
// lastHTTP is the last HTTP status seen (0 when the batch ended on a
// transport error).
func finish(store *outbox.Store, sent, pending, code int, status string, lastHTTP int) Result {
	_ = store.SetLastPush(outbox.PushResult{
		Status: statusOrOK(status),
		Code:   lastHTTP,
	})
	return Result{Sent: sent, Pending: pending, Code: code, Status: status}
}

func statusOrOK(s string) string {
	if s == "" {
		return "ok"
	}
	return s
}
