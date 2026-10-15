package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/djalmajr/nau/internal/config"
	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/registry"
	"github.com/djalmajr/nau/internal/router"
)

const (
	serveDefaultListen = "127.0.0.1:7429"
	serveDefaultPollS  = 60
	serveMinPollS      = 10
	serveMaxPollS      = 3600
	serveMaxMachines   = 32
	servePollTimeout   = 20 * time.Second
	serveServeTimeout  = 10 * time.Second
)

// serveSweepInterval is the period of the ended-rows sweep: the serve
// runs one sweep at the start and then one every period, whatever the
// requests; the tests replace it with a short interval.
var serveSweepInterval = 24 * time.Hour

// serveListen opens the listener socket; the tests replace it to capture
// the listener so they can close it and fail the serve on it.
var serveListen = net.Listen

// serveContext is the signal-bound context of the serve loop; the tests
// replace it with a context they cancel.
var serveContext = func() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// newServeExec builds the herdr runner of the poll loop; the tests replace
// it with an in-memory fake.
var newServeExec = router.NewExec

// lockedWriter serializes the poll-loop diagnostics and the registry
// logger (HTTP handler goroutines) on the same stderr stream.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// serveFlags is the parsed form of the serve arguments: each flag in the
// space form, at most once.
type serveFlags struct {
	listen         string
	listenSet      bool
	db             string
	dbSet          bool
	machines       string
	machinesSet    bool
	pollS          string
	pollSSet       bool
	maxJobs        string
	maxJobsSet     bool
	maxSessions    string
	maxSessionsSet bool
	retentionDays  string
	retentionSet   bool
}

// parseServeFlags accepts only the flags, in the space form, each at most
// once, followed by a value; anything else is malformed.
func parseServeFlags(args []string) (serveFlags, bool) {
	var f serveFlags
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--listen":
			if f.listenSet || i+1 >= len(args) {
				return f, false
			}
			f.listen, f.listenSet = args[i+1], true
			i++
		case "--db":
			if f.dbSet || i+1 >= len(args) {
				return f, false
			}
			f.db, f.dbSet = args[i+1], true
			i++
		case "--machines":
			if f.machinesSet || i+1 >= len(args) {
				return f, false
			}
			f.machines, f.machinesSet = args[i+1], true
			i++
		case "--poll-interval":
			if f.pollSSet || i+1 >= len(args) {
				return f, false
			}
			f.pollS, f.pollSSet = args[i+1], true
			i++
		case "--max-jobs":
			if f.maxJobsSet || i+1 >= len(args) {
				return f, false
			}
			f.maxJobs, f.maxJobsSet = args[i+1], true
			i++
		case "--max-sessions":
			if f.maxSessionsSet || i+1 >= len(args) {
				return f, false
			}
			f.maxSessions, f.maxSessionsSet = args[i+1], true
			i++
		case "--ended-retention-days":
			if f.retentionSet || i+1 >= len(args) {
				return f, false
			}
			f.retentionDays, f.retentionSet = args[i+1], true
			i++
		default:
			return f, false
		}
	}
	return f, true
}

// parseMachineList splits the comma-separated label list: it trims the
// whitespace around each label, skips empty parts (an empty value means
// no machines, so push only), and requires unique labels at most in
// number.
func parseMachineList(value string) ([]string, bool) {
	var out []string
	seen := make(map[string]bool)
	for _, part := range strings.Split(value, ",") {
		label := strings.TrimSpace(part)
		if label == "" {
			continue
		}
		if !router.LabelPattern.MatchString(label) || seen[label] {
			return nil, false
		}
		seen[label] = true
		out = append(out, label)
	}
	if len(out) > serveMaxMachines {
		return nil, false
	}
	return out, true
}

// cmdServe runs the registry service on a loopback address: the push
// ingest endpoint, the read-only Herdr inventory poll and the HTTP API. It
// prints one line once it listens and serves until its context ends; the
// poll loop is stopped before the store is closed.
func cmdServe(args []string, env Env) int {
	f, ok := parseServeFlags(args)
	if !ok {
		badUsage(env, "usage: serve [--listen <host:port>] [--db <path>] [--machines <label,...>] [--poll-interval <seconds>] [--max-jobs <n>] [--max-sessions <n>] [--ended-retention-days <n>]")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	listen := f.listen
	if !f.listenSet {
		listen = serveDefaultListen
	}
	if err := registry.CheckLoopback(listen); err != nil {
		fail(env, 2, "serve: --listen must be a loopback address")
		return 2
	}
	db := f.db
	if !f.dbSet {
		db = filepath.Join(outbox.StateDir(env.ConfigDir), "registry.db")
	} else if db, err = filepath.Abs(db); err != nil {
		fail(env, 2, "serve: --db is not a valid path")
		return 2
	}
	var machines []string
	if f.machinesSet {
		machines, ok = parseMachineList(f.machines)
		if !ok {
			fail(env, 2, "serve: --machines must be a comma-separated list of unique labels (at most 32)")
			return 2
		}
	} else {
		machines = cfg.RouteMachineList()
	}
	pollS := serveDefaultPollS
	if f.pollSSet {
		n, err := strconv.Atoi(f.pollS)
		if err != nil || n < serveMinPollS || n > serveMaxPollS {
			fail(env, 2, "serve: --poll-interval must be an integer between 10 and 3600")
			return 2
		}
		pollS = n
	}
	opts := registry.Options{Now: env.Now}
	// The deprecated count flags stay accepted for who already passes
	// them: any positive integer is ignored, and the registry has no
	// job or session count limit. A missing value is refused by the
	// flag parse above; a non-integer or a value below 1 exits 2.
	if f.maxJobsSet {
		n, err := strconv.Atoi(f.maxJobs)
		if err != nil || n < 1 {
			fail(env, 2, "serve: --max-jobs must be a positive integer")
			return 2
		}
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": serve: --max-jobs is deprecated and ignored: the registry has no job count limit")
	}
	if f.maxSessionsSet {
		n, err := strconv.Atoi(f.maxSessions)
		if err != nil || n < 1 {
			fail(env, 2, "serve: --max-sessions must be a positive integer")
			return 2
		}
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": serve: --max-sessions is deprecated and ignored: the registry has no session count limit")
	}
	if f.retentionSet {
		n, err := strconv.Atoi(f.retentionDays)
		if err != nil || n < 7 || n > 30 {
			fail(env, 2, "serve: --ended-retention-days must be an integer between 7 and 30")
			return 2
		}
		opts.EndedRetention = time.Duration(n) * 24 * time.Hour
	}

	store, err := registry.Open(db, opts)
	if err != nil {
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": serve:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "serve: registry database unavailable"}))
		return 4
	}
	ln, err := serveListen("tcp", listen)
	if err != nil {
		_ = store.Close()
		_, _ = fmt.Fprintln(env.Stderr, env.program()+": serve:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "serve: cannot listen on " + listen}))
		return 4
	}
	// The flag check sees the address as typed; the listener sees the
	// resolved one, and localhost may resolve elsewhere. Re-check the
	// bound address and refuse like the flag check.
	if !listenerIsLoopback(ln.Addr()) {
		_ = ln.Close()
		_ = store.Close()
		fail(env, 2, "serve: --listen must be a loopback address")
		return 2
	}

	// The poll loop and the HTTP handler write to stderr concurrently:
	// one protected writer.
	stderrW := &lockedWriter{w: env.Stderr}
	registry.SetLogger(func(format string, args ...any) {
		_, _ = fmt.Fprintf(stderrW, env.program()+": serve: "+format+"\n", args...)
	})

	ctx, cancel := serveContext()
	defer cancel()
	// The poll loop gets its own context so a failed listener also stops
	// it, and the store is closed only after the loop has returned.
	pollCtx, stopPoll := context.WithCancel(ctx)
	// The sweep gets its own context too: every exit of the serve
	// cancels it before the join below, so a failed listener closes the
	// store without an external cancellation.
	sweepCtx, stopSweep := context.WithCancel(ctx)
	var polling sync.WaitGroup
	if len(machines) > 0 {
		poller := registry.Poller{
			Exec:    newServeExec(cfg.HerdrBin, childEnviron(env)),
			Timeout: servePollTimeout,
			Now:     env.Now,
		}
		polling.Add(1)
		go func() {
			defer polling.Done()
			servePollLoop(pollCtx, stderrW, env.program(), store, machines, time.Duration(pollS)*time.Second, poller)
		}()
	}

	// The periodic sweep of the ended rows: one round at the start and
	// then one every serveSweepInterval, whatever the requests. A failed
	// round writes one line, without path or key, and the serve keeps
	// serving; the loop ends with its own context, which every exit of
	// the serve cancels before waiting for it, and serve waits for it
	// before closing the store, so no goroutine is left behind.
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		for {
			if err := store.SweepEnded(env.Now()); err != nil {
				_, _ = fmt.Fprintf(stderrW, "%s: serve: sweep failed\n", env.program())
			}
			select {
			case <-sweepCtx.Done():
				return
			case <-time.After(serveSweepInterval):
			}
		}
	}()

	maquinas := machines
	if maquinas == nil {
		maquinas = []string{}
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status   string   `json:"status"`
		Listen   string   `json:"listen"`
		Maquinas []string `json:"maquinas"`
		PollS    int      `json:"poll_s"`
	}{"serving", ln.Addr().String(), maquinas, pollS}))

	srv := &http.Server{
		Handler:           registry.NewHandler(store),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()
	code := 0
	select {
	case <-ctx.Done():
	case err := <-errCh:
		// Serve returns only on a listener failure before any shutdown;
		// stdout already carries the serving line, so only stderr says why.
		_, _ = fmt.Fprintf(stderrW, "%s: serve: %v\n", env.program(), err)
		code = 4
	}
	stopPoll()
	// This is the single exit of the serve, whatever made it leave the
	// loop (signal, listener failure or a logged shutdown error): cancel
	// the sweep before waiting for it, and close the store only after it
	// has returned.
	stopSweep()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), serveServeTimeout)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(stderrW, "%s: serve: shutdown: %v\n", env.program(), err)
	}
	shutdownCancel()
	polling.Wait()
	<-sweepDone
	_ = store.Close()
	return code
}

// listenerIsLoopback reports whether the bound address of a listener is
// a loopback IP: the check of the resolved address, not only of the
// flag, because localhost may resolve elsewhere.
func listenerIsLoopback(addr net.Addr) bool {
	ta, ok := addr.(*net.TCPAddr)
	if !ok {
		return false
	}
	return ta.IP.IsLoopback()
}

// servePollLoop runs one immediate round, then a round every interval,
// until the context ends; rounds never overlap. A round aborted by the
// shutdown is not applied, so the closing signal never marks the
// inventory obsolete.
func servePollLoop(ctx context.Context, logW io.Writer, prog string, store *registry.Store, machines []string, interval time.Duration, poller registry.Poller) {
	for {
		results := poller.PollAll(ctx, machines)
		if ctx.Err() != nil {
			return
		}
		for _, r := range results {
			if err := store.ApplyPoll(r); err != nil {
				_, _ = fmt.Fprintf(logW, "%s: serve: poll %s: %v\n", prog, r.Maquina, err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
