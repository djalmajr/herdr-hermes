package a2aserver

import (
	"context"
	"log/slog"
	"slices"
	"sync/atomic"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"
	"github.com/a2aproject/a2a-go/v2/log"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// pushSuppressedMsg is the single log message of one suppressed push
// send.
const pushSuppressedMsg = "a2a: push suppressed"

// The fixed reasons of a suppressed push send.
const (
	pushReasonNotGranted         = "not_granted"
	pushReasonNoOwnerReference   = "no_owner_reference"
	pushReasonRoutingUnavailable = "routing_unavailable"
	pushReasonLookupFailed       = "lookup_failed"
)

// ownerRefConfigs is the push config store the SDK saves through: it
// reads the authenticated subject the interceptor stamped on the call
// context, stamps its routing reference (never a token, the subject id
// or any provider value) on the context and delegates. A call without a
// subject, or the local operator (whose reference is empty), delegates
// without a stamp, so the store records NULL.
type ownerRefConfigs struct {
	*a2astore.PushConfigStore
}

// Save implements push.ConfigStore: it stamps the subject's routing
// reference when the call context carries one and delegates to the
// store.
func (o ownerRefConfigs) Save(ctx context.Context, taskID a2a.TaskID, cfg *a2a.PushConfig) (*a2a.PushConfig, error) {
	if callCtx, ok := a2asrv.CallContextFrom(ctx); ok && callCtx != nil && callCtx.User != nil {
		if subject, ok := callCtx.User.Attributes[subjectAttr].(a2acore.Subject); ok {
			ctx = a2astore.WithOwnerRef(ctx, subject.Ref)
		}
	}
	return o.PushConfigStore.Save(ctx, taskID, cfg)
}

// pushGate is the push sender of one server, live and out of band:
// before every send it re-reads the task's stored owner and project,
// the config's recorded routing reference and the current routing, and
// suppresses the send when the reference no longer grants the project.
// A suppression returns nil (a push error would fail the task), never
// calls the wrapped sender, counts itself and logs one record with the
// task id and the fixed reason and nothing else. Suppression changes
// nothing durable: the config stays, the task is not failed, and a
// later event is sent again once the routing grants the project again.
type pushGate struct {
	inner      push.Sender
	configs    *a2astore.PushConfigStore
	routes     a2acore.RouteSource
	authz      a2acore.Authorizer
	logger     *slog.Logger
	suppressed atomic.Int64
}

// newPushGate wraps one push sender in the gate of one server: the
// store's push config reader, the routes and the authorizer are the
// server's, so every send is re-evaluated at the moment of the send
// with the stored task row and the current routing.
func newPushGate(inner push.Sender, configs *a2astore.PushConfigStore, routes a2acore.RouteSource, authz a2acore.Authorizer, logger *slog.Logger) *pushGate {
	if logger == nil {
		logger = slog.New(discardHandler{})
	}
	return &pushGate{inner: inner, configs: configs, routes: routes, authz: authz, logger: logger}
}

// SendPush implements push.Sender: it passes the send to the wrapped
// sender only when the gate allows it (the local operator's configs are
// always sent and any other config without a recorded reference is
// never sent), and a suppression returns nil.
func (g *pushGate) SendPush(ctx context.Context, config *a2a.PushConfig, event a2a.Event) error {
	if config == nil {
		return g.suppress(ctx, "", pushReasonLookupFailed)
	}
	owner, err := g.configs.Owner(ctx, config.TaskID, config.ID)
	if err != nil {
		return g.suppress(ctx, string(config.TaskID), pushReasonLookupFailed)
	}
	var subject a2acore.Subject
	switch {
	case owner.Subject == a2acore.LocalSubjectID:
		// The local operator is evaluated the same way LocalOperator
		// authenticates it: every action on every project, with or
		// without a recorded reference.
		subject = a2acore.Subject{
			ID:       a2acore.LocalSubjectID,
			Projects: []string{a2acore.AnyProject},
			Actions:  slices.Clone(a2acore.AllActions),
		}
	case owner.Ref == "":
		// A config of any other subject that has no recorded reference
		// (saved before the column existed, or saved without a stamp)
		// is never sent: its grant cannot be re-evaluated.
		return g.suppress(ctx, string(config.TaskID), pushReasonNoOwnerReference)
	default:
		granted, err := routeGranted(g.routes, owner.Ref, owner.Project)
		if err != nil {
			return g.suppress(ctx, string(config.TaskID), pushReasonRoutingUnavailable)
		}
		if !granted {
			return g.suppress(ctx, string(config.TaskID), pushReasonNotGranted)
		}
		subject = a2acore.Subject{
			ID:       owner.Subject,
			Ref:      owner.Ref,
			Projects: []string{owner.Project},
			Actions:  slices.Clone(a2acore.AllActions),
		}
	}
	if g.authz.Authorize(subject, a2acore.ActionRead, owner.Project) != nil ||
		g.authz.Authorize(subject, a2acore.ActionPush, owner.Project) != nil {
		return g.suppress(ctx, string(config.TaskID), pushReasonNotGranted)
	}
	// The SDK sender logs through the logger attached to the context
	// (slog.Default() without one): attach the server logger so the
	// push log of the live and the out-of-band sends both reach the
	// Config.Logger and never the process default (which would write
	// the send error, with the URL of the webhook, to stderr in plain
	// text).
	ctx = log.AttachLogger(ctx, g.logger)
	return g.inner.SendPush(ctx, config, event)
}

// routeGranted is the RoutingGrants rule for one project, read on the
// call: the reference grants the project when the reference is not
// empty and is either the guardian reference (when that is not empty)
// or the registered owner of the project (when that is not empty). A
// route source error is returned as is.
func routeGranted(routes a2acore.RouteSource, ref, project string) (bool, error) {
	if ref == "" {
		return false, nil
	}
	guardian, err := routes.GuardianRef()
	if err != nil {
		return false, err
	}
	if guardian != "" && guardian == ref {
		return true, nil
	}
	owner, err := routes.OwnerRef(project)
	if err != nil {
		return false, err
	}
	return owner != "" && owner == ref, nil
}

// suppress counts one suppressed send, logs the one fixed record with
// the task id and the reason (never the config id, the reference, the
// subject, the project or the URL) and returns nil: a push error would
// fail the task and a suppression is not an error.
func (g *pushGate) suppress(ctx context.Context, taskID, reason string) error {
	g.suppressed.Add(1)
	g.logger.WarnContext(ctx, pushSuppressedMsg, slog.String("task_id", taskID), slog.String("reason", reason))
	return nil
}
