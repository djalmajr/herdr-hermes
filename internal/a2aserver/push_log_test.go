package a2aserver

// The DJA-337 round-2 push log test: the gate's SendPush attaches the
// server logger to the context before delegating to the inner SDK
// sender, so the sender's log of the live and the out-of-band sends
// always reaches the Config.Logger — never slog.Default(), which in
// the serve would write the push error (carrying the URL of the
// webhook) to stderr in plain text.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// TestPushLogGoesToConfigLogger proves the context attachment: with a
// JSON Config.Logger on a buffer and the SDK default HTTP sender
// (FailOnError false, the SSRF guard on), an out-of-band push to a
// loopback URL the guard refuses logs "push sending failed" in the
// Config.Logger buffer and not in the slog.Default() the test
// captures for the duration of the test.
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
	// sender's message and the error (which carries the URL at this
	// layer: the masking is the serve's logger).
	cfgLog := cfgBuf.String()
	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(cfgLog)), &rec); err != nil {
		t.Fatalf("Config.Logger buffer %q is not one JSON record: %v", cfgLog, err)
	}
	if rec["msg"] != "push sending failed" || rec["level"] != "ERROR" {
		t.Fatalf("record = %v, want the ERROR record msg %q", rec, "push sending failed")
	}
	if !strings.Contains(cfgLog, "http://127.0.0.1:9/hook") {
		t.Fatalf("Config.Logger buffer %q lacks the send error with the URL", cfgLog)
	}
	// and nothing reached the process default logger.
	if got := defBuf.String(); got != "" {
		t.Fatalf("slog.Default() captured %q, want nothing (the log must go to the Config.Logger)", got)
	}
}
