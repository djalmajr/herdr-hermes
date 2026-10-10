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

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/registry"
	"github.com/djalmajr/herdr-hermes/internal/router"
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
	listen      string
	listenSet   bool
	db          string
	dbSet       bool
	machines    string
	machinesSet bool
	pollS       string
	pollSSet    bool
}

// parseServeFlags accepts only the four flags, in the space form, each at
// most once, followed by a value; anything else is malformed.
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
		badUsage(env, "usage: serve [--listen <host:port>] [--db <path>] [--machines <label,...>] [--poll-interval <seconds>]")
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

	store, err := registry.Open(db, registry.Options{Now: env.Now})
	if err != nil {
		_, _ = fmt.Fprintln(env.Stderr, "herdr-hermes: serve:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "serve: registry database unavailable"}))
		return 4
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		_ = store.Close()
		_, _ = fmt.Fprintln(env.Stderr, "herdr-hermes: serve:", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "serve: cannot listen on " + listen}))
		return 4
	}

	// The poll loop and the HTTP handler write to stderr concurrently:
	// one protected writer.
	stderrW := &lockedWriter{w: env.Stderr}
	registry.SetLogger(func(format string, args ...any) {
		_, _ = fmt.Fprintf(stderrW, "herdr-hermes: serve: "+format+"\n", args...)
	})

	ctx, cancel := serveContext()
	defer cancel()
	// The poll loop gets its own context so a failed listener also stops
	// it, and the store is closed only after the loop has returned.
	pollCtx, stopPoll := context.WithCancel(ctx)
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
			servePollLoop(pollCtx, stderrW, store, machines, time.Duration(pollS)*time.Second, poller)
		}()
	}

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
		_, _ = fmt.Fprintf(stderrW, "herdr-hermes: serve: %v\n", err)
		code = 4
	}
	stopPoll()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), serveServeTimeout)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_, _ = fmt.Fprintf(stderrW, "herdr-hermes: serve: shutdown: %v\n", err)
	}
	shutdownCancel()
	polling.Wait()
	_ = store.Close()
	return code
}

// servePollLoop runs one immediate round, then a round every interval,
// until the context ends; rounds never overlap. A round aborted by the
// shutdown is not applied, so the closing signal never marks the
// inventory obsolete.
func servePollLoop(ctx context.Context, logW io.Writer, store *registry.Store, machines []string, interval time.Duration, poller registry.Poller) {
	for {
		results := poller.PollAll(ctx, machines)
		if ctx.Err() != nil {
			return
		}
		for _, r := range results {
			if err := store.ApplyPoll(r); err != nil {
				_, _ = fmt.Fprintf(logW, "herdr-hermes: serve: poll %s: %v\n", r.Maquina, err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
