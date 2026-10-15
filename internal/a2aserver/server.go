// Package a2aserver binds the Nau A2A agent to the official A2A SDK
// HTTP+JSON (REST) binding (DJA-283): the agent card, the credential
// middleware and the authentication/authorization call interceptor, the
// parked agent executor (evaluation, routing and durable admission
// before the first event), the decision dispatcher that applies recorded
// owner decisions either to a parked execution or out-of-band through
// the store, the loopback local operator authenticator and the REST-only
// SDK client wrapper.
//
// An incoming A2A message is data to evaluate, never an instruction:
// message text is stored in the task history and returned as JSON, never
// executed, rendered into a terminal or logged, and the request
// credential is consumed by the interceptor and never reaches the SDK
// service params, the executor or the store.
package a2aserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/push"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// maxBodyBytes caps every request body at 1 MiB (http.MaxBytesReader).
const maxBodyBytes = 1 << 20

// Config is the input of New. Store, Auth and Routes are required;
// Authz defaults to a2acore.SubjectPolicy{}, PushSender to the SDK HTTP
// sender with a 10 s timeout and the default SSRF guard on, Now to
// time.Now and Logger to a discard handler. New wraps the sender,
// configured or default, in the push gate, which suppresses a send when
// the config's recorded reference no longer grants the task's project;
// a suppressed send never reaches the wrapped sender and never fails
// the task. The logger never logs credentials, message text or
// secrets. LoopbackOnly makes Handler refuse, before routing, every
// request whose Host or Origin is not a loopback one; set it by the
// command that binds the agent to loopback only.
type Config struct {
	Store        *a2astore.Store
	Auth         a2acore.Authenticator
	Authz        a2acore.Authorizer
	Routes       a2acore.RouteSource
	PushSender   push.Sender
	BaseURL      string
	Version      string
	LoopbackOnly bool
	Limits       a2acore.Limits
	Now          func() time.Time
	Logger       *slog.Logger
}

// Server is one running Nau A2A agent: the SDK request handler wired to
// the store, the executor with its parked-execution registry and the
// decision dispatcher.
type Server struct {
	cfg      Config
	card     *a2a.AgentCard
	handler  a2asrv.RequestHandler
	executor *executor
	gate     *pushGate
}

// discardHandler is the default Logger target: every record is dropped.
type discardHandler struct{}

// Enabled implements slog.Handler.
func (discardHandler) Enabled(context.Context, slog.Level) bool { return false }

// Handle implements slog.Handler.
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }

// WithAttrs implements slog.Handler.
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler { return d }

// WithGroup implements slog.Handler.
func (d discardHandler) WithGroup(string) slog.Handler { return d }

// New builds the agent: the card, the SDK request handler (task store,
// push notifications, capability checks, the auth interceptor and the
// logger) and the parked executor.
func New(cfg Config) (*Server, error) {
	if cfg.Store == nil {
		return nil, errors.New("a2aserver: Store is required")
	}
	if cfg.Auth == nil {
		return nil, errors.New("a2aserver: Auth is required")
	}
	if cfg.Routes == nil {
		return nil, errors.New("a2aserver: Routes is required")
	}
	if cfg.Authz == nil {
		cfg.Authz = a2acore.SubjectPolicy{}
	}
	if cfg.PushSender == nil {
		cfg.PushSender = push.NewHTTPPushSender(&push.HTTPSenderConfig{Timeout: 10 * time.Second})
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(discardHandler{})
	}
	// The push gate wraps the sender, configured or default, so the SDK
	// live sends and the out-of-band final event both pass through it:
	// every send is re-evaluated against the stored task row and the
	// current routing at the moment of the send. The SDK saves through
	// ownerRefConfigs, which stamps the subject's routing reference on
	// the context so the Save records it (never a token).
	gate := newPushGate(cfg.PushSender, cfg.Store.PushConfigs(), cfg.Routes, cfg.Authz, cfg.Logger)
	cfg.PushSender = gate
	s := &Server{cfg: cfg, gate: gate}
	s.card = buildCard(cfg)
	s.executor = newExecutor(cfg)
	s.handler = a2asrv.NewHandler(
		s.executor,
		a2asrv.WithTaskStore(scopedTaskStore{store: cfg.Store}),
		a2asrv.WithPushNotifications(ownerRefConfigs{cfg.Store.PushConfigs()}, cfg.PushSender),
		a2asrv.WithCapabilityChecks(&s.card.Capabilities),
		a2asrv.WithCallInterceptors(newAuthInterceptor(cfg).withCapabilities(&s.card.Capabilities)),
		a2asrv.WithLogger(cfg.Logger),
	)
	return s, nil
}

// Handler returns the public HTTP handler: GET
// /.well-known/agent-card.json is served without authentication and
// everything else goes through the credential middleware and the SDK
// REST handler. When Config.LoopbackOnly is set, the whole handler is
// wrapped in the loopback guard, which refuses a non-loopback Host or
// Origin before routing, the agent card route included.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET "+a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(s.card))
	mux.Handle("/", credentialMiddleware(a2asrv.NewRESTHandler(s.handler)))
	if s.cfg.LoopbackOnly {
		return loopbackGuard(mux)
	}
	return mux
}

// Card returns the agent card served at /.well-known/agent-card.json.
func (s *Server) Card() *a2a.AgentCard { return s.card }

// LiveExecutions reports how many executions are parked awaiting the
// decision of the owner or guardian (a test observation point).
func (s *Server) LiveExecutions() int { return s.executor.liveExecutions() }

// SuppressedPushes reports how many push sends the gate suppressed
// since the server started (a test observation point).
func (s *Server) SuppressedPushes() int64 { return s.gate.suppressed.Load() }

// now is the server clock (Config.Now, time.Now when unset).
func (s *Server) now() time.Time { return s.cfg.Now() }
