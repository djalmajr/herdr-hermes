package cli

import (
	"context"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/push"
)

// pushOutcome is the result of the push step shared by the commands that
// talk to the dispatcher (slice 2's sync and plugin path use the same
// variable).
type pushOutcome struct {
	Sent    int
	Pending int
	Code    int    // 0, 40, 41 or 42
	Status  string // "", "auth_missing", "auth_rejected", "unreachable", "rejected", "push_disabled"
}

// pushPending pushes the pending outbox records and reports the outcome.
//
// With no dispatcher_url the push is disabled (code 0, the outbox stays the
// pull channel). With no stored key it is code 40 auth_missing. A URL the
// push client refuses (not https, non-loopback host) is code 42 rejected
// with a friction line. Otherwise push.Pending runs with retry =
// interactive: wake pushes each record once, sync and push retry.
//
// A store that cannot be read (a broken cursor or outbox) is reported as
// unreachable: the records cannot be counted, and nothing is pushed.
var pushPending = func(ctx context.Context, env Env, cfg config.Config, store *outbox.Store, interactive bool) pushOutcome {
	delivered, err := store.DeliveredSeq()
	if err != nil {
		outbox.Friction(store.Dir(), "push", "unreadable cursor outcome unreachable")
		return pushOutcome{Code: 42, Status: "unreachable"}
	}
	lines, _, err := store.Read(delivered)
	if err != nil {
		outbox.Friction(store.Dir(), "push", "unreadable outbox outcome unreachable")
		return pushOutcome{Code: 42, Status: "unreachable"}
	}
	pending := len(lines)
	if cfg.DispatcherURL == "" {
		return pushOutcome{Pending: pending, Code: 0, Status: "push_disabled"}
	}
	key, err := newCredStore(env).Get()
	if err != nil {
		return pushOutcome{Pending: pending, Code: 40, Status: "auth_missing"}
	}
	h, err := push.NewHTTP(cfg.DispatcherURL, key, version, time.Duration(cfg.PushTimeoutS)*time.Second)
	if err != nil {
		outbox.Friction(store.Dir(), "push", "invalid dispatcher url outcome rejected")
		return pushOutcome{Pending: pending, Code: 42, Status: "rejected"}
	}
	res := push.Pending(ctx, store, h, env.Sleep, interactive)
	return pushOutcome{Sent: res.Sent, Pending: res.Pending, Code: res.Code, Status: res.Status}
}
