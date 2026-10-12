package a2aserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/djalmajr/nau/internal/a2acore"
)

// subjectAttr is the authenticated user attribute key that carries the
// a2acore.Subject (the executor reads it back).
const subjectAttr = "nau.subject"

// credentialKeyType is the context key of the request credential the
// middleware captures and the interceptor consumes.
type credentialKeyType struct{}

// credentialFrom returns the captured request credential (empty when
// nothing was captured on the context).
func credentialFrom(ctx context.Context) a2acore.Credential {
	c, _ := ctx.Value(credentialKeyType{}).(a2acore.Credential)
	return c
}

// credentialMiddleware captures the Authorization header and the peer
// address into an unexported context value and deletes the Authorization
// header before the SDK sees the request, so the credential never
// reaches the SDK service params, the logs, the executor or the store.
// Request bodies are capped at 1 MiB with http.MaxBytesReader,
// unconditionally: a chunked request reports ContentLength -1 and an
// unknown length must be capped too.
func credentialMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		ctx := context.WithValue(req.Context(), credentialKeyType{}, a2acore.Credential{
			Authorization: req.Header.Get("Authorization"),
			RemoteAddr:    req.RemoteAddr,
		})
		req.Header.Del("Authorization")
		req.Body = http.MaxBytesReader(rw, req.Body, maxBodyBytes)
		next.ServeHTTP(rw, req.WithContext(ctx))
	})
}

// loopbackGuard refuses, before routing, every request that is not
// addressed to a loopback authority (Config.LoopbackOnly). The agent is
// bound to loopback by the command that exposes it, and the guard
// applies to every route, the agent card included.
func loopbackGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if !loopbackRequest(r) {
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusForbidden)
			_, _ = rw.Write([]byte(`{"status":"forbidden","motivo":"a2a: non-loopback host or origin"}`))
			return
		}
		next.ServeHTTP(rw, r)
	})
}

// loopbackRequest reports whether r is addressed to a loopback
// authority: its Host must be one, and an Origin header, when present,
// must be an http or https origin of one; the CLI and the bridge push
// send no Origin, so an absent header is fine.
func loopbackRequest(r *http.Request) bool {
	if !loopbackAuthority(r.Host) {
		return false
	}
	if o := r.Header.Get("Origin"); o != "" && !loopbackOrigin(o) {
		return false
	}
	return true
}

// loopbackAuthority reports whether host is a loopback authority: the
// host part (split from a port when one is present; a bare host is
// allowed) is localhost, case-insensitive, or an IP literal with
// IsLoopback (127.0.0.0/8, ::1; an IPv4-mapped address counts only if
// IsLoopback says so).
func loopbackAuthority(host string) bool {
	h := host
	if p, _, err := net.SplitHostPort(host); err == nil {
		h = p
	}
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// loopbackOrigin reports whether o is an http or https URL to a
// loopback authority and nothing else: no user info, path or query.
func loopbackOrigin(o string) bool {
	u, err := url.Parse(o)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	if u.User != nil || u.Host == "" || u.Path != "" || u.RawQuery != "" {
		return false
	}
	return loopbackAuthority(u.Host)
}

// authInterceptor authenticates the captured request credential, stamps
// the authenticated user (with the subject attribute) onto the call
// context, replaces the credential value with an empty one for the rest
// of the call, authorizes the method and forces ReturnImmediately on
// message sends (a task waits for a person, a blocking HTTP call must
// not hold for days). A message send that carries a task id is first
// checked against the scoped task store: the SDK execution manager
// answers a task whose execution is parked before it reads the store,
// so without this check the answer for a task the caller cannot see
// would differ from the answer for a task that does not exist. The
// streaming send is checked only when the SDK would let it reach the
// execution manager: while streaming is off the SDK refuses it with
// the same unsupported-operation answer for every task id before any
// store or manager access, so there is nothing to mask and the answer
// stays as it is.
type authInterceptor struct {
	auth  a2acore.Authenticator
	authz a2acore.Authorizer
	tasks scopedTaskStore
	caps  *a2a.AgentCapabilities
}

// newAuthInterceptor builds the interceptor of one server.
func newAuthInterceptor(cfg Config) *authInterceptor {
	return &authInterceptor{auth: cfg.Auth, authz: cfg.Authz, tasks: scopedTaskStore{store: cfg.Store}}
}

// withCapabilities sets the capabilities the server advertises, the
// same pointer given to a2asrv.WithCapabilityChecks, and returns the
// interceptor. Without them (nil) the interceptor assumes the SDK does
// not refuse a streaming send, which is the SDK's own reading of nil
// capabilities.
func (i *authInterceptor) withCapabilities(caps *a2a.AgentCapabilities) *authInterceptor {
	i.caps = caps
	return i
}

// streamingRefusedBySDK mirrors the SDK check that answers a streaming
// send with a2a.ErrUnsupportedOperation before it reads the task store
// or enters the execution manager: capabilities are set and streaming
// is off.
func (i *authInterceptor) streamingRefusedBySDK() bool {
	return i.caps != nil && !i.caps.Streaming
}

// errTaskNotVisible is what a continuation on a task id the caller
// cannot see gets: the SDK's own text for a continuation on a task that
// does not exist (a wrapped a2a.ErrTaskNotFound, HTTP 404), the same
// value for a missing id and for a task outside the caller's owner or
// project grant. It carries no task id, owner, project or store text.
var errTaskNotVisible = fmt.Errorf("executor setup failed: failed to load exec ctx: task loading failed: %w", a2a.ErrTaskNotFound)

// requireVisibleTask loads id through the scoped task store on the
// request context (owner mask and the stored-project predicate of the
// freshly authenticated subject): an invisible or missing task is
// errTaskNotVisible and a store failure is a fixed-text server error.
// A nil store fails closed as not found.
func (i *authInterceptor) requireVisibleTask(ctx context.Context, id a2a.TaskID) error {
	if i.tasks.store == nil {
		return errTaskNotVisible
	}
	_, err := i.tasks.Get(ctx, id)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, a2a.ErrTaskNotFound):
		return errTaskNotVisible
	}
	return a2a.NewError(a2a.ErrServerError, "a2a: task lookup failed")
}

// actionForMethod maps one SDK method to the action it authorizes. The
// project is always "" here: the project is checked per task through
// the request scope the interceptor stamps (scopedTaskStore), and the
// executor checks the project of a send.
func actionForMethod(method string) (a2acore.Action, bool) {
	switch method {
	case "SendMessage", "SendStreamingMessage":
		return a2acore.ActionSend, true
	case "GetTask", "ListTasks", "SubscribeToTask", "GetExtendedAgentCard":
		return a2acore.ActionRead, true
	case "CancelTask":
		return a2acore.ActionCancel, true
	case "GetTaskPushConfig", "ListTaskPushConfigs", "CreateTaskPushConfig", "DeleteTaskPushConfig":
		return a2acore.ActionPush, true
	}
	return "", false
}

// Before implements a2asrv.CallInterceptor.
func (i *authInterceptor) Before(ctx context.Context, callCtx *a2asrv.CallContext, req *a2asrv.Request) (context.Context, any, error) {
	subject, err := i.auth.Authenticate(ctx, credentialFrom(ctx))
	if err != nil {
		// An identity provider that is not configured, failed or answered
		// something invalid is honest unavailability (HTTP 500), not a
		// credential problem; the error text is never forwarded.
		if errors.Is(err, a2acore.ErrIdentityUnavailable) {
			return ctx, nil, a2a.NewError(a2a.ErrServerError, "a2a: identity provider unavailable")
		}
		return ctx, nil, a2a.NewError(a2a.ErrUnauthenticated, "a2a: unauthenticated")
	}
	callCtx.User = a2asrv.NewAuthenticatedUser(subject.ID, map[string]any{subjectAttr: subject})
	ctx = context.WithValue(ctx, credentialKeyType{}, a2acore.Credential{})
	action, ok := actionForMethod(callCtx.Method())
	if !ok || i.authz.Authorize(subject, action, "") != nil {
		return ctx, nil, a2a.NewError(a2a.ErrUnauthorized, "a2a: unauthorized")
	}
	// The project scope is built from this request's freshly
	// authenticated subject and is never cached: a task whose stored
	// project the subject's grant does not cover is task-not-found.
	ctx = withProjectScope(ctx, func(project string) bool {
		return project != "" && i.authz.Authorize(subject, action, project) == nil
	})
	if p, ok := req.Payload.(*a2a.SendMessageRequest); ok {
		// A continuation is looked up before the SDK sees it: the
		// execution manager refuses a task with a parked execution
		// before any store read. A send without a task id is a new task
		// and is not looked up. A streaming send is looked up only when
		// the SDK does not refuse it for the streaming capability first
		// (it then never reaches the manager and the answer is the same
		// for every id).
		lookup := callCtx.Method() == "SendMessage" ||
			(callCtx.Method() == "SendStreamingMessage" && !i.streamingRefusedBySDK())
		if lookup && p.Message != nil && p.Message.TaskID != "" {
			if err := i.requireVisibleTask(ctx, p.Message.TaskID); err != nil {
				return ctx, nil, err
			}
		}
		if p.Config == nil {
			p.Config = &a2a.SendMessageConfig{}
		}
		p.Config.ReturnImmediately = true
	}
	return ctx, nil, nil
}

// After implements a2asrv.CallInterceptor.
func (i *authInterceptor) After(_ context.Context, _ *a2asrv.CallContext, _ *a2asrv.Response) error {
	return nil
}

// LocalOperator is the existing-policy adapter of the loopback local
// operator: it accepts only a request whose peer is a loopback IP and
// that carries no Authorization header, and authenticates it as the
// subject a2acore.LocalSubjectID with every action and every project.
// Anything else, including a loopback request that carries an
// unverified credential, is refused: an unverified credential is never
// silently ignored.
type LocalOperator struct{}

// Authenticate implements a2acore.Authenticator.
func (LocalOperator) Authenticate(_ context.Context, c a2acore.Credential) (a2acore.Subject, error) {
	host, _, err := net.SplitHostPort(c.RemoteAddr)
	if err != nil {
		host = c.RemoteAddr
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return a2acore.Subject{}, fmt.Errorf("%w: peer is not a loopback address", a2acore.ErrUnauthenticated)
	}
	if c.Authorization != "" {
		return a2acore.Subject{}, fmt.Errorf("%w: a credential is not accepted on the loopback", a2acore.ErrUnauthenticated)
	}
	return a2acore.Subject{
		ID:       a2acore.LocalSubjectID,
		Projects: []string{a2acore.AnyProject},
		Actions:  slices.Clone(a2acore.AllActions),
	}, nil
}

// LocalOrBearer keeps the two policies apart: a request with no
// Authorization header goes to Local (the loopback local operator), a
// request with one goes to Bearer (the injected provider). Neither falls
// through to the other: a refused credential is never retried as the
// local operator and a missing one never reaches the provider. A nil
// Bearer is a2acore.ErrIdentityUnavailable (fail closed, no provider is
// configured) and a nil Local is a2acore.ErrUnauthenticated.
type LocalOrBearer struct {
	Local  a2acore.Authenticator
	Bearer a2acore.Authenticator
}

// Authenticate implements a2acore.Authenticator.
func (a LocalOrBearer) Authenticate(ctx context.Context, c a2acore.Credential) (a2acore.Subject, error) {
	if c.Authorization == "" {
		if a.Local == nil {
			return a2acore.Subject{}, fmt.Errorf("%w: no local operator policy", a2acore.ErrUnauthenticated)
		}
		return a.Local.Authenticate(ctx, c)
	}
	if a.Bearer == nil {
		return a2acore.Subject{}, fmt.Errorf("%w: identity provider not configured", a2acore.ErrIdentityUnavailable)
	}
	return a.Bearer.Authenticate(ctx, c)
}
