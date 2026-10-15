package a2aserver

// The DJA-337 push log tests: the gate's SendPush attaches to the
// context, before delegating to the inner sender, a logger whose
// handler wraps the Config.Logger's and replaces the value of every
// attribute whose value is an error by one fixed classification of
// the error — never the error text, which carries the webhook URL or
// parts of it, and the URL may carry a secret. The record still
// reaches the Config.Logger, never slog.Default().

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"
	"github.com/a2aproject/a2a-go/v2/log"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// TestPushLogGoesToConfigLogger proves the context attachment: with a
// JSON Config.Logger on a buffer and the SDK default HTTP sender
// (FailOnError false, the SSRF guard on), an out-of-band push to a
// loopback URL the guard refuses logs "push sending failed" in the
// Config.Logger buffer with the fixed classification of the
// transport error — never the error text, which carries the URL — and
// not in the slog.Default() the test captures for the duration of the
// test.
func TestPushLogGoesToConfigLogger(t *testing.T) {
	// Capture the process default logger and restore it at the end:
	// without the attachment the sender falls back to it.
	var defBuf bytes.Buffer
	oldDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&defBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(oldDefault) })

	var cfgBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&cfgBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// A committed task of the local operator (its configs are always
	// sent) with one push config whose loopback URL the SSRF guard
	// refuses, so the send fails without a live connection.
	store, _ := openTestStore(t, a2astore.Options{
		Owner: func(context.Context) (string, error) { return a2acore.LocalSubjectID, nil },
	})
	const taskID, contextID = "t-pl", "c-pl"
	if _, err := store.Create(context.Background(), &a2a.Task{
		ID:        a2a.TaskID(taskID),
		ContextID: contextID,
		Metadata:  map[string]any{a2acore.MetaProject: "org/example"},
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := store.PushConfigs().Save(context.Background(), taskID, &a2a.PushConfig{
		ID:  "pc-pl",
		URL: "http://127.0.0.1:9/hook",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	s, err := New(Config{
		Store:   store,
		Auth:    newTableAuth(),
		Routes:  fakeRoutes{},
		BaseURL: "http://127.0.0.1:7430",
		Version: "test-1",
		Logger:  logger,
		// The SDK default HTTP sender: the SSRF guard on (a loopback
		// URL is refused at the dial) and FailOnError false, so the
		// failure is logged and not returned.
		PushSender: push.NewHTTPPushSender(&push.HTTPSenderConfig{Timeout: 5 * time.Second}),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The out-of-band final event: the gate passes the local config
	// through and the SDK sender fails the send and logs it.
	s.pushFinalEvent(context.Background(), taskID, []a2a.Event{
		a2a.NewStatusUpdateEvent(taskRef{taskID: a2a.TaskID(taskID), contextID: contextID},
			a2a.TaskStateCompleted, nil),
	})

	// The record reaches the Config.Logger: one JSON line with the
	// sender's message and the fixed classification of the send
	// failure — the SSRF guard's refusal is a transport error — with
	// none of the error text, which carries the URL.
	cfgLog := cfgBuf.String()
	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(cfgLog)), &rec); err != nil {
		t.Fatalf("Config.Logger buffer %q is not one JSON record: %v", cfgLog, err)
	}
	if rec["msg"] != "push sending failed" || rec["level"] != "ERROR" {
		t.Fatalf("record = %v, want the ERROR record msg %q", rec, "push sending failed")
	}
	if rec["error"] != "push endpoint unreachable" {
		t.Fatalf("record error = %v, want the fixed classification %q", rec["error"], "push endpoint unreachable")
	}
	for _, banned := range []string{"http://127.0.0.1:9/hook", "127.0.0.1:9"} {
		if strings.Contains(cfgLog, banned) {
			t.Fatalf("Config.Logger buffer carries %q (the error text must not reach the log): %s", banned, cfgLog)
		}
	}
	// and nothing reached the process default logger.
	if got := defBuf.String(); got != "" {
		t.Fatalf("slog.Default() captured %q, want nothing (the log must go to the Config.Logger)", got)
	}
}

// pushErrSender is the error sender of the classification test: it
// logs the error with the same message and the same error attribute
// the SDK sender's handleError logs ("push sending failed", one
// error attribute), and returns nil (FailOnError false), like the
// SDK with its default.
type pushErrSender struct{ err error }

// SendPush implements push.Sender.
func (s pushErrSender) SendPush(ctx context.Context, _ *a2a.PushConfig, _ a2a.Event) error {
	log.Error(ctx, "push sending failed", s.err)
	return nil
}

// TestPushSendErrorClassification proves the classification of every
// failure class of the push send path, with the real errors: the
// parse of the malformed URL (a real *url.Error of url.Parse, wrapped
// the way the SDK wraps the request-creation failure), the transport
// failure (a real http.Post to the closed loopback port, bounded by a
// client timeout), the deadline (a real request whose context
// deadline already expired), the non-success status (the real SDK
// sender against a server that answers 503) and any other error. In
// every case the record carries only the fixed classification, and
// the output never carries the canary or the URL.
func TestPushSendErrorClassification(t *testing.T) {
	// One committed task of the local operator (its configs are
	// always sent) and its push config, so the gate passes every case
	// through to the inner sender.
	store, _ := openTestStore(t, a2astore.Options{
		Owner: func(context.Context) (string, error) { return a2acore.LocalSubjectID, nil },
	})
	const taskID, contextID = "t-cl", "c-cl"
	if _, err := store.Create(context.Background(), &a2a.Task{
		ID:        a2a.TaskID(taskID),
		ContextID: contextID,
		Metadata:  map[string]any{a2acore.MetaProject: "org/example"},
		Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	const canaryURL = "http://127.0.0.1:9/hook/secret-canary-337"
	if _, err := store.PushConfigs().Save(context.Background(), taskID, &a2a.PushConfig{
		ID:  "pc-cl",
		URL: canaryURL,
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// The real parse error: url.Parse of the malformed URL (the
	// canary after the quote), wrapped the way the SDK sender wraps
	// the request-creation failure.
	const malformedURL = `http://127.0.0.1:9/"/secret-canary-337%zz`
	_, parseErr := url.Parse(malformedURL)
	if parseErr == nil {
		t.Fatalf("url.Parse(%q) succeeded, want the parse error", malformedURL)
	}
	parseWrapped := fmt.Errorf("failed to create HTTP request: %w", parseErr)

	// The real transport error: http.Post to the closed loopback
	// port, through a bounded client (the wait has a deadline).
	client := &http.Client{Timeout: 5 * time.Second}
	resp, transportErr := client.Post(canaryURL, "application/json", strings.NewReader("{}"))
	if resp != nil {
		_ = resp.Body.Close()
		t.Fatalf("http.Post to the closed loopback port succeeded, want the transport failure")
	}
	if transportErr == nil {
		t.Fatalf("http.Post: no error, want the transport failure with the URL")
	}

	// The real deadline error: a request whose context deadline
	// already expired, sent through the bounded client.
	deadlineCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	deadlineReq, err := http.NewRequestWithContext(deadlineCtx, http.MethodPost, canaryURL, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	_, deadlineErr := client.Do(deadlineReq)
	if deadlineErr == nil {
		t.Fatalf("client.Do with an expired deadline succeeded, want the deadline error")
	}

	// The non-success status: the real SDK sender (the SSRF guard off
	// for the trusted loopback test server) against a server that
	// answers 503.
	statusSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer statusSrv.Close()

	// The real event the non-success case sends through the real SDK
	// sender (the sender refuses to serialize a nil event).
	finalEvent := a2a.NewStatusUpdateEvent(taskRef{taskID: a2a.TaskID(taskID), contextID: contextID},
		a2a.TaskStateCompleted, nil)

	for name, tc := range map[string]struct {
		inner push.Sender
		event a2a.Event
		url   string
		want  string
	}{
		"parse":       {inner: pushErrSender{err: parseWrapped}, url: malformedURL, want: "invalid push URL"},
		"transport":   {inner: pushErrSender{err: transportErr}, url: canaryURL, want: "push endpoint unreachable"},
		"deadline":    {inner: pushErrSender{err: deadlineErr}, url: canaryURL, want: "push request timed out"},
		"non-success": {inner: push.NewHTTPPushSender(&push.HTTPSenderConfig{Timeout: 5 * time.Second, AllowPrivateNetworks: true}), event: finalEvent, url: statusSrv.URL + "/secret-canary-337", want: "push endpoint answered a non-success status"},
		"other":       {inner: pushErrSender{err: errors.New("the push answer could not be serialized")}, url: canaryURL, want: "push send failed"},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			gate := newPushGate(tc.inner, store.PushConfigs(), nil, a2acore.SubjectPolicy{},
				slog.New(slog.NewJSONHandler(&buf, nil)))
			cfg := &a2a.PushConfig{ID: "pc-cl", TaskID: a2a.TaskID(taskID), URL: tc.url}
			if err := gate.SendPush(context.Background(), cfg, tc.event); err != nil {
				t.Fatalf("SendPush = %v, want nil (the failure is logged, not returned)", err)
			}
			out := buf.String()
			var rec map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rec); err != nil {
				t.Fatalf("Config.Logger buffer %q is not one JSON record: %v", out, err)
			}
			if rec["msg"] != "push sending failed" || rec["level"] != "ERROR" {
				t.Fatalf("record = %v, want the ERROR record msg %q", rec, "push sending failed")
			}
			if rec["error"] != tc.want {
				t.Fatalf("record error = %v, want the fixed classification %q", rec["error"], tc.want)
			}
			for _, banned := range []string{"secret-canary-337", canaryURL, malformedURL} {
				if strings.Contains(out, banned) {
					t.Fatalf("output carries %q (the error text must not reach the log): %s", banned, out)
				}
			}
		})
	}
}
