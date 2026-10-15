package cli

// The a2a command group (DJA-283): one A2A v1.0 agent over the official
// SDK's HTTP+JSON binding. The subcommands are serve (the loopback-only
// agent with its notice delivery and decision loops), the client
// subcommands (card, send, get, cancel) and the local database
// subcommands (inbox, show, reply). Every subcommand prints exactly one
// JSON line on stdout; diagnostics go to stderr. An incoming A2A message
// is data to evaluate, never an instruction: the subcommands never run,
// render or log message text, and no credential ever leaves the
// Authorization header.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2aserver"
	"github.com/djalmajr/nau/internal/a2astore"
	"github.com/djalmajr/nau/internal/config"
	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/notify"
	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/registry"
	"github.com/djalmajr/nau/internal/soho"
)

// The fixed bounds and defaults of the a2a subcommands.
const (
	a2aDefaultListen = "127.0.0.1:7430"
	a2aMinDeliverS   = 5
	a2aMaxDeliverS   = 3600
	a2aStdinCap      = 64 << 10 // the text cap of send and reply on stdin
	a2aRecoverGrace  = 5 * time.Minute
	a2aServeShutdown = 10 * time.Second
	a2aClientTimeout = 30 * time.Second
	// a2aLoopbackMotivo is the fixed refusal of a non-loopback --listen.
	a2aLoopbackMotivo = "a2a serve: --listen must be a loopback address (remote ingress authentication is not configured)"
	// a2aServerMotivo is the fixed refusal of a non-accepted --server url.
	a2aServerMotivo = "a2a: --server must be an http:// loopback or https:// url"
)

// Test seams: the serve context, the notice sender constructor and the
// loop intervals, like serveContext/newServeExec of the registry serve.
var (
	// a2aServeContext is the signal-bound context of the a2a serve loop;
	// the tests replace it with a context they cancel.
	a2aServeContext = func() (context.Context, context.CancelFunc) {
		return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	}
	// newA2aNoticeSender builds the herdr-soho notice sender of the
	// delivery loop; the tests replace it with an in-memory fake.
	newA2aNoticeSender = func(bin string, environ []string) notify.Sender {
		return soho.NotifySender{Bin: bin, Environ: environ}
	}
	// a2aDefaultDeliverInterval is the delivery interval of the serve
	// when --deliver-interval is absent; the tests replace it to run the
	// loop quickly.
	a2aDefaultDeliverInterval = 15 * time.Second
	// a2aDecisionInterval is the decision loop interval of the serve;
	// the tests replace it.
	a2aDecisionInterval = time.Second
	// a2aIdentityProvider is the injected existing identity source that
	// verifies a bearer credential. No identity source is configured in
	// this release and no flag, environment variable or file configures
	// one, so it is nil and a request that carries a credential fails
	// closed; the tests inject a fixture.
	a2aIdentityProvider a2acore.IdentityProvider
	// a2aServeSecrets is the injected SecretProvider of the a2a serve
	// store; nil uses the store default (the process-lifetime in-memory
	// provider). No flag, environment variable or file configures a
	// provider in this release; the tests inject a fixture whose Delete
	// fails to prove the serve survives a cleanup failure.
	a2aServeSecrets a2acore.SecretProvider
)

// a2aNeutralStates are the neutral task states a filter may name.
var a2aNeutralStates = map[string]bool{
	a2acore.StateSubmitted:     true,
	a2acore.StateWorking:       true,
	a2acore.StateInputRequired: true,
	a2acore.StateCompleted:     true,
	a2acore.StateCanceled:      true,
	a2acore.StateFailed:        true,
	a2acore.StateRejected:      true,
}

// a2aParseFlags accepts only the strict space form: every flag named in
// valued expects exactly one following value, every flag named in
// booleans takes none, each flag appears at most once; an unknown flag,
// a repeated flag, an equals form, a missing value or a bare "--" is
// malformed (ok=false). Positional arguments are returned in order;
// present reports every flag seen (the boolean ones have no value).
func a2aParseFlags(args []string, valued, booleans map[string]bool) (values map[string]string, present map[string]bool, positional []string, ok bool) {
	values = map[string]string{}
	present = map[string]bool{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			positional = append(positional, a)
			continue
		}
		if strings.IndexByte(a, '=') >= 0 {
			return nil, nil, nil, false
		}
		if booleans[a] {
			if present[a] {
				return nil, nil, nil, false
			}
			present[a] = true
			continue
		}
		if !valued[a] || present[a] || i+1 >= len(args) {
			return nil, nil, nil, false
		}
		i++
		present[a] = true
		values[a] = args[i]
	}
	return values, present, positional, true
}

// a2aServerURLOK reports whether raw is an accepted client --server url:
// an http:// url with a loopback host (localhost or a loopback IP
// literal) or an https:// url.
func a2aServerURLOK(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return false
	}
	switch u.Scheme {
	case "https":
		return true
	case "http":
		host := u.Hostname()
		if host == "localhost" {
			return true
		}
		ip := net.ParseIP(host)
		return ip != nil && ip.IsLoopback()
	default:
		return false
	}
}

// a2aDBMissing reports whether the database file (or its directory) does
// not exist yet: inbox answers an empty list and show and reply answer
// not found without creating anything.
func a2aDBMissing(path string) bool {
	_, err := os.Stat(path)
	return errors.Is(err, os.ErrNotExist)
}

// a2aDB resolves the --db flag of the local subcommands: the registry
// database of the state dir when absent, the absolute path when given.
func a2aDB(values map[string]string, env Env) (string, bool) {
	db := values["--db"]
	if db == "" {
		return filepath.Join(outbox.StateDir(env.ConfigDir), "registry.db"), true
	}
	abs, err := filepath.Abs(db)
	if err != nil {
		return "", false
	}
	return abs, true
}

// a2aTaskNotFound is the fixed not-found motivo of show and reply.
const a2aTaskNotFound = "no such task"

// a2aText resolves the one positional argument of send and reply: the
// text itself, or "-" for stdin read with the 64 KiB cap (an overrun is
// the fixed over-cap refusal).
func a2aText(positional []string, env Env) (string, string) {
	if positional[0] != "-" {
		return positional[0], ""
	}
	data, err := readCapped(env.Stdin, a2aStdinCap)
	if errors.Is(err, errInputOverCap) {
		return "", "stdin is over the 64 KiB cap"
	}
	if err != nil {
		return "", "reading stdin: " + err.Error()
	}
	return string(data), ""
}

// a2aInt64 parses a strict integer in [1, a2acore.MaxSeq]; the motivo
// is the fixed text the caller prefixes with its command name.
func a2aInt64(name, raw string) (int64, string) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 1 || n > a2acore.MaxSeq {
		return 0, name + " must be an integer between 1 and " + strconv.FormatInt(a2acore.MaxSeq, 10)
	}
	return n, ""
}

// a2aNewDecisionID generates one local decision id (the pattern allows
// alphanumerics, dots, underscores and dashes): "d_" plus 16 lower-case
// hex digits from crypto/rand.
func a2aNewDecisionID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "d_" + hex.EncodeToString(b[:]), nil
}

// cmdA2A dispatches the a2a subcommands; the writing subcommands (serve
// and reply) gate themselves under HERDR_HERMES_NOWRITE=1.
func cmdA2A(args []string, env Env) int {
	if len(args) == 0 {
		badUsage(env, "usage: a2a serve|card|send|get|cancel|inbox|show|reply")
		return 2
	}
	switch sub := args[0]; sub {
	case "serve":
		return cmdA2AServe(args[1:], env)
	case "card":
		return cmdA2ACard(args[1:], env)
	case "send":
		return cmdA2ASend(args[1:], env)
	case "get":
		return cmdA2AGet(args[1:], env)
	case "cancel":
		return cmdA2ACancel(args[1:], env)
	case "inbox":
		return cmdA2AInbox(args[1:], env)
	case "show":
		return cmdA2AShow(args[1:], env)
	case "reply":
		return cmdA2AReply(args[1:], env)
	default:
		badUsage(env, "unknown a2a subcommand "+quote(sub))
		return 2
	}
}

// ---------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------

// a2aNotifyRoutes is the RouteSource over the notify registrations: the
// owner reference of a project and the machine coordinator, re-read on
// every call so a registration change is visible without a restart. A
// missing registry file yields "" (no_route): nothing is ever invented.
type a2aNotifyRoutes struct {
	ob  *outbox.Store
	now func() time.Time
}

// OwnerRef implements a2acore.RouteSource.
func (r a2aNotifyRoutes) OwnerRef(project string) (string, error) {
	reg, err := notify.Open(r.ob, r.now).LoadRegistry()
	if err != nil {
		return "", err
	}
	return reg.Owners[project], nil
}

// GuardianRef implements a2acore.RouteSource.
func (r a2aNotifyRoutes) GuardianRef() (string, error) {
	reg, err := notify.Open(r.ob, r.now).LoadRegistry()
	if err != nil {
		return "", err
	}
	return reg.Coordinator, nil
}

// OwnedProjects implements a2acore.ProjectRoutes: the projects whose
// registered owner is ref, in no particular order, re-read on every
// call. An empty ref owns nothing.
func (r a2aNotifyRoutes) OwnedProjects(ref string) ([]string, error) {
	if ref == "" {
		return nil, nil
	}
	reg, err := notify.Open(r.ob, r.now).LoadRegistry()
	if err != nil {
		return nil, err
	}
	var projects []string
	for project, owner := range reg.Owners {
		if owner == ref {
			projects = append(projects, project)
		}
	}
	return projects, nil
}

// a2aDeliverLoop runs one immediate notice delivery round, then a round
// every interval, until the context ends; rounds never overlap. A round
// aborted by the shutdown releases its unsent claims, so the closing
// signal never loses or duplicates a notice.
func a2aDeliverLoop(ctx context.Context, q a2acore.Queue, sender notify.Sender, interval time.Duration, now func() time.Time) {
	for {
		_, _ = a2acore.Deliver(ctx, q, sender, now, a2acore.DeliverOptions{})
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// cmdA2AServe runs the A2A agent on a loopback address: the HTTP+JSON
// handler, the notice delivery loop and the decision loop. It prints one
// line once it listens and serves until its context ends; the loops are
// stopped before the HTTP shutdown and the store is closed last.
func cmdA2AServe(args []string, env Env) int {
	if env.Getenv(nowriteVar) == "1" {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	values, _, positional, ok := a2aParseFlags(args, map[string]bool{
		"--listen": true, "--db": true, "--deliver-interval": true,
	}, nil)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: a2a serve [--listen <host:port>] [--db <path>] [--deliver-interval <seconds>]")
		return 2
	}
	listen := values["--listen"]
	if listen == "" {
		listen = a2aDefaultListen
	}
	if err := registry.CheckLoopback(listen); err != nil {
		fail(env, 2, a2aLoopbackMotivo)
		return 2
	}
	db := values["--db"]
	if db == "" {
		db = filepath.Join(outbox.StateDir(env.ConfigDir), "registry.db")
	} else {
		abs, err := filepath.Abs(db)
		if err != nil {
			fail(env, 2, "a2a serve: --db is not a valid path")
			return 2
		}
		db = abs
	}
	deliver := a2aDefaultDeliverInterval
	if raw := values["--deliver-interval"]; raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < a2aMinDeliverS || n > a2aMaxDeliverS {
			fail(env, 2, "a2a serve: --deliver-interval must be an integer between 5 and 3600")
			return 2
		}
		deliver = time.Duration(n) * time.Second
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	store, err := a2astore.OpenFile(db, a2astore.Options{Now: env.Now, Secrets: a2aServeSecrets})
	if err != nil {
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a serve:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a serve: registry database unavailable"}))
		return 4
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		_ = store.Close()
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a serve:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a serve: cannot listen on " + listen}))
		return 4
	}
	// The flag check sees the address as typed; the listener sees the
	// resolved one, and localhost may resolve elsewhere. Re-check the
	// bound address and refuse like the flag check.
	if !listenerIsLoopback(ln.Addr()) {
		_ = ln.Close()
		_ = store.Close()
		fail(env, 2, a2aLoopbackMotivo)
		return 2
	}
	// The route source re-reads the notify registry on every call; the
	// read-only outbox store creates nothing.
	ro, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		_ = ln.Close()
		_ = store.Close()
		fail(env, 2, "a2a serve: "+err.Error())
		return 2
	}
	// Two distinct policies: a request without an Authorization header
	// is the loopback local operator; a request with one goes to the
	// injected identity provider (nil here, so it fails closed), whose
	// grants are derived from the same routes, re-read on every call.
	routes := a2aNotifyRoutes{ob: ro, now: env.Now}
	var bearer a2acore.Authenticator
	if a2aIdentityProvider != nil {
		bearer = a2acore.BearerAuthenticator{
			Identity: a2aIdentityProvider,
			Grants:   a2acore.RoutingGrants{Routes: routes},
		}
	}
	srv, err := a2aserver.New(a2aserver.Config{
		Store: store,
		Auth:  a2aserver.LocalOrBearer{Local: a2aserver.LocalOperator{}, Bearer: bearer},
		// The local operator trusts a loopback peer: a request whose
		// Host or Origin is not loopback (a browser page, DNS
		// rebinding) is refused before it reaches it.
		LoopbackOnly: true,
		Routes:       routes,
		BaseURL:      "http://" + ln.Addr().String(),
		Version:      version,
		Now:          env.Now,
		// The warnings of the agent (the push gate's suppression among
		// them) go to stderr as JSON records at the WARN level and
		// above; stdout keeps the JSON status lines. Every URL of a
		// record is masked as <url> before it is written: a webhook URL
		// can carry a secret in the path or the query.
		Logger: a2aServeLogger(env.Stderr),
	})
	if err != nil {
		_ = ln.Close()
		_ = store.Close()
		fail(env, 2, "a2a serve: "+err.Error())
		return 2
	}
	// The held deliveries of a task that never committed (a crash between
	// the admission and the task write) become obsolete before serving.
	// A transaction or SQL failure of the recovery still refuses the
	// start; a failure that came only from the cleanup of the secret
	// handles (the recovery itself committed) does not: the handles are
	// pending in the durable cleanup ledger and are retried on the next
	// recovery.
	if _, err := store.RecoverHeld(context.Background(), env.Now(), a2aRecoverGrace); err != nil {
		if !errors.Is(err, a2astore.ErrSecretCleanupPending) {
			_ = ln.Close()
			_ = store.Close()
			_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a serve:", err)
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a serve: registry database unavailable"}))
			return 4
		}
	}
	// The pending count is consulted whether or not a fresh cleanup
	// failed: an old failure alone must not start with an empty stderr.
	// At most one count-only line: no handle, no secret and no provider
	// message, only the number.
	pending, cerr := store.PendingSecretCleanup(context.Background())
	if cerr != nil {
		_ = ln.Close()
		_ = store.Close()
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a serve:", cerr)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a serve: registry database unavailable"}))
		return 4
	}
	if pending > 0 {
		_, _ = fmt.Fprintf(env.Stderr, "a2a serve: %d secret handle deletions are pending; they are retried on the next recovery\n", pending)
	}

	ctx, cancel := a2aServeContext()
	defer cancel()
	deliverCtx, stopDeliver := context.WithCancel(ctx)
	decisionCtx, stopDecision := context.WithCancel(ctx)
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		a2aDeliverLoop(deliverCtx, store, newA2aNoticeSender(cfg.HerdrSohoBin, childEnviron(env)), deliver, env.Now)
	}()
	go func() {
		defer loops.Done()
		srv.RunDecisions(decisionCtx, a2aDecisionInterval)
	}()

	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status string `json:"status"`
		Listen string `json:"listen"`
		DB     string `json:"db"`
	}{"serving", ln.Addr().String(), db}))

	srvHTTP := &http.Server{
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srvHTTP.Serve(ln)
	}()
	code := 0
	select {
	case <-ctx.Done():
	case err := <-errCh:
		// Serve returns only on a listener failure before any shutdown;
		// stdout already carries the serving line, so only stderr says why.
		_, _ = fmt.Fprintf(env.Stderr, "%s: a2a serve: %v\n", env.program(), err)
		code = 4
	}
	stopDeliver()
	stopDecision()
	loops.Wait()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), a2aServeShutdown)
	if err := srvHTTP.Shutdown(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(env.Stderr, "%s: a2a serve: shutdown: %v\n", env.program(), err)
	}
	shutdownCancel()
	_ = store.Close()
	return code
}

// ---------------------------------------------------------------------
// client subcommands (card, send, get, cancel)
// ---------------------------------------------------------------------

// a2aClientError maps one client call error to the client exit codes and
// prints the error JSON line: refused is 2, not found is 3, and anything
// else (transport, unexpected) is 4. The motivo is the error text: a
// refusal carries the fixed "refused: <code>" server text and a transport
// error the standard dial text — never the message text or a credential.
func a2aClientError(env Env, op string, err error) int {
	status, code := "unavailable", 4
	switch {
	case errors.Is(err, a2a.ErrUnauthenticated), errors.Is(err, a2a.ErrUnauthorized),
		errors.Is(err, a2a.ErrInvalidParams), errors.Is(err, a2a.ErrInvalidRequest),
		errors.Is(err, a2a.ErrParseError), errors.Is(err, a2a.ErrUnsupportedOperation),
		errors.Is(err, a2a.ErrTaskNotCancelable), errors.Is(err, a2a.ErrMethodNotFound):
		status, code = "refused", 2
	case errors.Is(err, a2a.ErrTaskNotFound):
		status, code = "not_found", 3
	}
	_, _ = fmt.Fprintf(env.Stderr, "%s: a2a %s: %v\n", env.program(), op, err)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: status, Motivo: err.Error()}))
	return code
}

// a2aClientDial dials one --server url with a bounded context and no
// credential (remote credentials are an open owner decision): the agent
// card is fetched and the REST client is bound to it.
func a2aClientDial(ctx context.Context, serverURL string) (*a2aserver.Client, error) {
	return a2aserver.Dial(ctx, serverURL, nil, &http.Client{Timeout: a2aClientTimeout})
}

// a2aServerFlag validates the client --server flag (present and an
// accepted url) and returns its value or the badUsage refusal.
func a2aServerFlag(values map[string]string, env Env, op string) (string, bool) {
	server := values["--server"]
	if server == "" || !a2aServerURLOK(server) {
		badUsage(env, "a2a "+op+": "+a2aServerMotivo)
		return "", false
	}
	return server, true
}

// cmdA2ACard fetches the agent card and prints {"status":"ok","card":…}.
func cmdA2ACard(args []string, env Env) int {
	values, _, positional, ok := a2aParseFlags(args, map[string]bool{"--server": true}, nil)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: a2a card --server <url>")
		return 2
	}
	server, ok := a2aServerFlag(values, env, "card")
	if !ok {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), a2aClientTimeout)
	defer cancel()
	client, err := a2aClientDial(ctx, server)
	if err != nil {
		return a2aClientError(env, "card", err)
	}
	defer func() { _ = client.Close() }()
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status string         `json:"status"`
		Card   *a2a.AgentCard `json:"card"`
	}{"ok", client.Card()}))
	return 0
}

// cmdA2ASend sends one message: a new task (with --project) or a
// continuation (with --task and --context together). It prints
// {"status":"ok","task":…} or, for a replayed message id,
// {"status":"duplicate","message":…}.
func cmdA2ASend(args []string, env Env) int {
	values, _, positional, ok := a2aParseFlags(args, map[string]bool{
		"--server": true, "--session": true, "--epoch": true, "--seq": true,
		"--project": true, "--job": true, "--task": true, "--context": true,
		"--message-id": true,
	}, nil)
	if !ok || len(positional) != 1 {
		badUsage(env, "usage: a2a send --server <url> --session <id> --epoch <n> --seq <n> [--project <org/repo>] [--job <id>] [--task <id> --context <id>] [--message-id <id>] (<text>|-)")
		return 2
	}
	server, ok := a2aServerFlag(values, env, "send")
	if !ok {
		return 2
	}
	if values["--session"] == "" || !a2acore.SessionPattern.MatchString(values["--session"]) {
		badUsage(env, "a2a send: --session does not match the session pattern")
		return 2
	}
	epoch, motivo := a2aInt64("--epoch", values["--epoch"])
	if motivo != "" {
		badUsage(env, "a2a send: "+motivo)
		return 2
	}
	seq, motivo := a2aInt64("--seq", values["--seq"])
	if motivo != "" {
		badUsage(env, "a2a send: "+motivo)
		return 2
	}
	if p := values["--project"]; p != "" && !jobapi.ValidRepo(p) {
		badUsage(env, "a2a send: --project does not match the repo pattern")
		return 2
	}
	if j := values["--job"]; j != "" && !jobapi.ValidID(j) {
		badUsage(env, "a2a send: --job does not match the job id pattern")
		return 2
	}
	// --task and --context name a continuation: they are given together.
	if (values["--task"] == "") != (values["--context"] == "") {
		badUsage(env, "a2a send: --task and --context are given together")
		return 2
	}
	if t := values["--task"]; t != "" && !a2acore.TaskIDPattern.MatchString(t) {
		badUsage(env, "a2a send: --task does not match the task id pattern")
		return 2
	}
	if c := values["--context"]; c != "" && !a2acore.TaskIDPattern.MatchString(c) {
		badUsage(env, "a2a send: --context does not match the task id pattern")
		return 2
	}
	if m := values["--message-id"]; m != "" && !a2acore.MessageIDPattern.MatchString(m) {
		badUsage(env, "a2a send: --message-id does not match the message id pattern")
		return 2
	}
	text, motivo := a2aText(positional, env)
	if motivo != "" {
		fail(env, 2, "a2a send: "+motivo)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), a2aClientTimeout)
	defer cancel()
	client, err := a2aClientDial(ctx, server)
	if err != nil {
		return a2aClientError(env, "send", err)
	}
	defer func() { _ = client.Close() }()
	in := a2aserver.SendInput{
		Project:   values["--project"],
		Job:       values["--job"],
		TaskID:    values["--task"],
		ContextID: values["--context"],
		Session:   values["--session"],
		MessageID: values["--message-id"],
		Text:      text,
		Epoch:     epoch,
		Seq:       seq,
	}
	res, serr := client.Send(ctx, in)
	if serr != nil {
		return a2aClientError(env, "send", serr)
	}
	switch r := res.(type) {
	case *a2a.Task:
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Status string    `json:"status"`
			Task   *a2a.Task `json:"task"`
		}{"ok", r}))
	case *a2a.Message:
		// The server answers a replayed message id with one agent message
		// whose metadata carries the first task id.
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Status  string       `json:"status"`
			Message *a2a.Message `json:"message"`
		}{a2aSendResultStatus(r), r}))
	default:
		fail(env, 4, "a2a send: unexpected empty result")
	}
	return 0
}

// a2aSendResultStatus is the status of a message result of send: the
// server answers a replayed message id with the duplicate_of metadata,
// and any other bare message answer (none expected from this server) is
// reported ok.
func a2aSendResultStatus(m *a2a.Message) string {
	if _, dup := m.Metadata[a2acore.MetaDuplicateOf]; dup {
		return "duplicate"
	}
	return "ok"
}

// a2aTaskFlag validates the --task flag (present and a valid task id)
// and returns its value or the badUsage refusal.
func a2aTaskFlag(values map[string]string, env Env, op string) (string, bool) {
	task := values["--task"]
	if task == "" || !a2acore.TaskIDPattern.MatchString(task) {
		badUsage(env, "a2a "+op+": --task does not match the task id pattern")
		return "", false
	}
	return task, true
}

// cmdA2AGet prints {"status":"ok","task":…} for the --task task.
func cmdA2AGet(args []string, env Env) int {
	values, _, positional, ok := a2aParseFlags(args, map[string]bool{"--server": true, "--task": true}, nil)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: a2a get --server <url> --task <id>")
		return 2
	}
	server, ok := a2aServerFlag(values, env, "get")
	if !ok {
		return 2
	}
	task, ok := a2aTaskFlag(values, env, "get")
	if !ok {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), a2aClientTimeout)
	defer cancel()
	client, err := a2aClientDial(ctx, server)
	if err != nil {
		return a2aClientError(env, "get", err)
	}
	defer func() { _ = client.Close() }()
	t, err := client.Get(ctx, task)
	if err != nil {
		return a2aClientError(env, "get", err)
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status string    `json:"status"`
		Task   *a2a.Task `json:"task"`
	}{"ok", t}))
	return 0
}

// cmdA2ACancel cancels the non-terminal --task and prints
// {"status":"ok","task":…}.
func cmdA2ACancel(args []string, env Env) int {
	values, _, positional, ok := a2aParseFlags(args, map[string]bool{"--server": true, "--task": true}, nil)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: a2a cancel --server <url> --task <id>")
		return 2
	}
	server, ok := a2aServerFlag(values, env, "cancel")
	if !ok {
		return 2
	}
	task, ok := a2aTaskFlag(values, env, "cancel")
	if !ok {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), a2aClientTimeout)
	defer cancel()
	client, err := a2aClientDial(ctx, server)
	if err != nil {
		return a2aClientError(env, "cancel", err)
	}
	defer func() { _ = client.Close() }()
	t, err := client.Cancel(ctx, task)
	if err != nil {
		return a2aClientError(env, "cancel", err)
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status string    `json:"status"`
		Task   *a2a.Task `json:"task"`
	}{"ok", t}))
	return 0
}

// ---------------------------------------------------------------------
// local database subcommands (inbox, show, reply)
// ---------------------------------------------------------------------

// cmdA2AInbox lists the tasks of this machine with their delivery states
// and pending decisions (newest update first); it never shows message
// text and never creates a missing database (it prints an empty list).
func cmdA2AInbox(args []string, env Env) int {
	values, _, positional, ok := a2aParseFlags(args, map[string]bool{"--db": true, "--project": true, "--state": true}, nil)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: a2a inbox [--db <path>] [--project <org/repo>] [--state <state>]")
		return 2
	}
	if p := values["--project"]; p != "" && !jobapi.ValidRepo(p) {
		badUsage(env, "a2a inbox: --project does not match the repo pattern")
		return 2
	}
	if s := values["--state"]; s != "" && !a2aNeutralStates[s] {
		badUsage(env, "a2a inbox: --state must be a task state")
		return 2
	}
	db, ok := a2aDB(values, env)
	if !ok {
		badUsage(env, "a2a inbox: --db is not a valid path")
		return 2
	}
	empty := []a2acore.InboxItem{}
	if a2aDBMissing(db) {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Status string              `json:"status"`
			Items  []a2acore.InboxItem `json:"items"`
		}{"ok", empty}))
		return 0
	}
	store, err := a2astore.OpenFile(db, a2astore.Options{Now: env.Now})
	if err != nil {
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a inbox:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a inbox: registry database unavailable"}))
		return 4
	}
	defer func() { _ = store.Close() }()
	items, err := store.Inbox(context.Background(), a2acore.InboxFilter{
		Project: values["--project"],
		State:   values["--state"],
	})
	if err != nil {
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a inbox:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a inbox: registry database unavailable"}))
		return 4
	}
	if items == nil {
		items = []a2acore.InboxItem{}
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status string              `json:"status"`
		Items  []a2acore.InboxItem `json:"items"`
	}{"ok", items}))
	return 0
}

// cmdA2AShow prints {"status":"ok","task":…,"item":…,"decisions":[…]}:
// the task with its full history — the message text appears only as JSON
// data here — plus the inbox item and the recorded decisions. An unknown
// task, or a missing database, exits 3 without creating the file.
func cmdA2AShow(args []string, env Env) int {
	values, _, positional, ok := a2aParseFlags(args, map[string]bool{"--task": true, "--db": true}, nil)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: a2a show --task <id> [--db <path>]")
		return 2
	}
	task, ok := a2aTaskFlag(values, env, "show")
	if !ok {
		return 2
	}
	db, ok := a2aDB(values, env)
	if !ok {
		badUsage(env, "a2a show: --db is not a valid path")
		return 2
	}
	notFound := func() int {
		_, _ = fmt.Fprintf(env.Stderr, "%s: a2a show: %s\n", env.program(), a2aTaskNotFound)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "not_found", Motivo: a2aTaskNotFound}))
		return 3
	}
	if a2aDBMissing(db) {
		return notFound()
	}
	store, err := a2astore.OpenFile(db, a2astore.Options{Now: env.Now})
	if err != nil {
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a show:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a show: registry database unavailable"}))
		return 4
	}
	defer func() { _ = store.Close() }()
	t, item, decisions, err := store.Show(context.Background(), task)
	if err != nil {
		if errors.Is(err, a2a.ErrTaskNotFound) {
			return notFound()
		}
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a show:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a show: registry database unavailable"}))
		return 4
	}
	if decisions == nil {
		decisions = []a2acore.Decision{}
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status    string             `json:"status"`
		Task      *a2a.Task          `json:"task"`
		Item      *a2acore.InboxItem `json:"item"`
		Decisions []a2acore.Decision `json:"decisions"`
	}{"ok", t, item, decisions}))
	return 0
}

// cmdA2AReply records the owner or guardian decision on a working task:
// --answer completes it (the text as the artifact), --ask moves it to
// input required (the text as the question back) and --decline rejects
// it (the text as the reason). It prints {"status":"recorded","decision":…}
// (or "duplicate" for the same decision id and content), exit 2 with
// {"status":"refused","motivo":"<code>"} on a refusal and exit 3 for an
// unknown task. It never applies the decision itself: a running serve
// does, and reply never claims the task changed.
func cmdA2AReply(args []string, env Env) int {
	if env.Getenv(nowriteVar) == "1" {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	values, present, positional, ok := a2aParseFlags(args, map[string]bool{
		"--task": true, "--decision-id": true, "--db": true,
	}, map[string]bool{"--answer": true, "--ask": true, "--decline": true})
	if !ok || len(positional) != 1 {
		badUsage(env, "usage: a2a reply --task <id> (--answer | --ask | --decline) [--decision-id <id>] [--db <path>] (<text>|-)")
		return 2
	}
	task, ok := a2aTaskFlag(values, env, "reply")
	if !ok {
		return 2
	}
	// Exactly one kind flag.
	kinds := 0
	var kind a2acore.DecisionKind
	if present["--answer"] {
		kind, kinds = a2acore.DecisionAnswer, kinds+1
	}
	if present["--ask"] {
		kind, kinds = a2acore.DecisionAsk, kinds+1
	}
	if present["--decline"] {
		kind, kinds = a2acore.DecisionDecline, kinds+1
	}
	if kinds != 1 {
		badUsage(env, "a2a reply: exactly one of --answer, --ask or --decline is required")
		return 2
	}
	decisionID := values["--decision-id"]
	if decisionID == "" {
		generated, err := a2aNewDecisionID()
		if err != nil {
			fail(env, 2, "a2a reply: "+err.Error())
			return 2
		}
		decisionID = generated
	} else if !a2acore.DecisionIDPattern.MatchString(decisionID) {
		badUsage(env, "a2a reply: --decision-id does not match the decision id pattern")
		return 2
	}
	text, motivo := a2aText(positional, env)
	if motivo != "" {
		fail(env, 2, "a2a reply: "+motivo)
		return 2
	}
	db, ok := a2aDB(values, env)
	if !ok {
		badUsage(env, "a2a reply: --db is not a valid path")
		return 2
	}
	notFound := func() int {
		_, _ = fmt.Fprintf(env.Stderr, "%s: a2a reply: %s\n", env.program(), a2aTaskNotFound)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "not_found", Motivo: a2aTaskNotFound}))
		return 3
	}
	if a2aDBMissing(db) {
		return notFound()
	}
	store, err := a2astore.OpenFile(db, a2astore.Options{Now: env.Now})
	if err != nil {
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a reply:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a reply: registry database unavailable"}))
		return 4
	}
	defer func() { _ = store.Close() }()
	// The task must exist: reply never creates anything, and the stored
	// decisions answer the duplicate before the write.
	_, item, decisions, err := store.Show(context.Background(), task)
	if err != nil {
		if errors.Is(err, a2a.ErrTaskNotFound) {
			return notFound()
		}
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a reply:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a reply: registry database unavailable"}))
		return 4
	}
	// An existing decision id with the same content is a duplicate: the
	// stored decision is returned and nothing is written again.
	for i := range decisions {
		if decisions[i].ID == decisionID && decisions[i].Kind == kind && decisions[i].Text == text {
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
				Status   string            `json:"status"`
				Decision *a2acore.Decision `json:"decision"`
			}{"duplicate", &decisions[i]}))
			return 0
		}
	}
	// Inside Herdr (HERDR_PANE_ID set) the reply records the pane
	// reference as the actor and is refused with not_a_recipient when the
	// pane is not a recipient of the task: one of the planned deliveries
	// or, when not planned, the current routing, so a re-registered owner
	// may answer. Outside Herdr the actor is "local" and no check runs.
	actor := "local"
	if paneID := env.Getenv("HERDR_PANE_ID"); paneID != "" {
		ref := paneID
		if !strings.HasPrefix(ref, "local/") {
			ref = "local/" + ref
		}
		actor = ref
		planned, err := store.TaskRecipientRefs(context.Background(), task)
		if err != nil {
			_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a reply:", err)
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a reply: registry database unavailable"}))
			return 4
		}
		recipient := false
		for _, r := range planned {
			if r == ref {
				recipient = true
				break
			}
		}
		if !recipient {
			// The planned deliveries do not include the pane: the current
			// routing is read, only then (a read-only open, like the serve
			// opens it), so a routing change after the admission is what
			// lets a new owner answer.
			ro, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
			if err != nil {
				_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a reply:", err)
				_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a reply: routing unavailable"}))
				return 4
			}
			plan, err := a2acore.Route(a2aNotifyRoutes{ob: ro, now: env.Now}, item.Project, "")
			if err != nil {
				_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a reply:", err)
				_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a reply: routing unavailable"}))
				return 4
			}
			for _, p := range plan {
				if p.Ref == ref {
					recipient = true
					break
				}
			}
		}
		if !recipient {
			_, _ = fmt.Fprintf(env.Stderr, "%s: a2a reply: refused: %s\n", env.program(), a2acore.RefuseNotARecipient)
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "refused", Motivo: a2acore.RefuseNotARecipient}))
			return 2
		}
	}
	d := a2acore.Decision{
		ID:     decisionID,
		TaskID: task,
		Kind:   kind,
		Text:   text,
		Actor:  actor,
	}
	stored, refusal, err := store.RecordDecision(context.Background(), d, env.Now())
	if err != nil {
		if errors.Is(err, a2a.ErrTaskNotFound) {
			return notFound()
		}
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": a2a reply:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "a2a reply: registry database unavailable"}))
		return 4
	}
	if refusal != nil {
		// The store reports a vanished task as the unknown_task refusal:
		// the unknown-task answer is exit 3, every other refusal exit 2.
		if refusal.Code == a2acore.RefuseUnknownTask {
			return notFound()
		}
		_, _ = fmt.Fprintf(env.Stderr, "%s: a2a reply: refused: %s\n", env.program(), refusal.Code)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "refused", Motivo: refusal.Code}))
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status   string            `json:"status"`
		Decision *a2acore.Decision `json:"decision"`
	}{"recorded", &stored}))
	return 0
}
