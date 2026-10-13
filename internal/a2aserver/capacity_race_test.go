package a2aserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"

	"github.com/djalmajr/nau/internal/a2acore"
	"github.com/djalmajr/nau/internal/a2astore"
)

// capacityClock is the injected clock shared by the store and the test,
// safe for the concurrent handlers.
type capacityClock struct{ ns atomic.Int64 }

func newCapacityClock() *capacityClock {
	c := &capacityClock{}
	c.ns.Store(time.Date(2026, 3, 4, 5, 6, 7, 0, time.FixedZone("TST", 2*3600)).UnixNano())
	return c
}

// Now is the Options.Now clock.
func (c *capacityClock) Now() time.Time {
	return time.Unix(0, c.ns.Load()).In(time.FixedZone("TST", 2*3600))
}

// Add advances the clock.
func (c *capacityClock) Add(d time.Duration) { c.ns.Add(int64(d)) }

// createGate holds the task create of the messages it was told to hold,
// so a test pins the window between a request's admission and the
// creation of its task without any timing: the create announces that it
// arrived, then waits for its release (or ends after a deadline, so a
// broken test fails instead of hanging).
type createGate struct {
	mu      sync.Mutex
	arrived map[string]chan struct{}
	release map[string]chan struct{}
	opened  map[string]bool
}

func newCreateGate(t *testing.T, messageIDs ...string) *createGate {
	g := &createGate{
		arrived: map[string]chan struct{}{},
		release: map[string]chan struct{}{},
		opened:  map[string]bool{},
	}
	for _, id := range messageIDs {
		g.arrived[id] = make(chan struct{})
		g.release[id] = make(chan struct{})
	}
	t.Cleanup(func() {
		for _, id := range messageIDs {
			g.open(id)
		}
	})
	return g
}

// open releases the held create of the message (idempotent).
func (g *createGate) open(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ch, ok := g.release[id]; ok && !g.opened[id] {
		g.opened[id] = true
		close(ch)
	}
}

// gatedTaskStore is the task store of the SDK handler with the gate in
// front of Create; the wrapped store is the production scoped one.
type gatedTaskStore struct {
	taskstore.Store
	gate *createGate
	mu   sync.Mutex
	seen map[string]bool
}

// Create implements taskstore.Store.
func (g *gatedTaskStore) Create(ctx context.Context, task *a2a.Task) (taskstore.TaskVersion, error) {
	if len(task.History) > 0 {
		id := string(task.History[0].ID)
		g.gate.mu.Lock()
		arrived, held := g.gate.arrived[id]
		release := g.gate.release[id]
		g.gate.mu.Unlock()
		if held {
			g.mu.Lock()
			first := !g.seen[id]
			if g.seen == nil {
				g.seen = map[string]bool{}
			}
			g.seen[id] = true
			g.mu.Unlock()
			if first {
				close(arrived)
			}
			select {
			case <-release:
			case <-ctx.Done():
			case <-time.After(8 * time.Second):
			}
		}
	}
	return g.Store.Create(ctx, task)
}

// newGatedServer is New with the gate in front of the task create: the
// only difference to the production handler is the wrapper.
func newGatedServer(t *testing.T, store *a2astore.Store, auth *tableAuth, gate *createGate) *Server {
	t.Helper()
	s := newTestServer(t, store, auth, fakeRoutes{owner: "owner-ref-1"})
	s.handler = a2asrv.NewHandler(
		s.executor,
		a2asrv.WithTaskStore(&gatedTaskStore{Store: scopedTaskStore{store: store}, gate: gate}),
		a2asrv.WithPushNotifications(store.PushConfigs(), s.cfg.PushSender),
		a2asrv.WithCapabilityChecks(&s.card.Capabilities),
		a2asrv.WithCallInterceptors(newAuthInterceptor(s.cfg).withCapabilities(&s.card.Capabilities)),
		a2asrv.WithLogger(s.cfg.Logger),
	)
	return s
}

// sendOutcome is the result of one asynchronous send.
type sendOutcome struct {
	res a2a.SendMessageResult
	err error
}

// sendAsync sends one new task in its own goroutine.
func sendAsync(t *testing.T, c *Client, in SendInput) <-chan sendOutcome {
	t.Helper()
	out := make(chan sendOutcome, 1)
	ctx := ctxT(t)
	go func() {
		res, err := c.Send(ctx, in)
		out <- sendOutcome{res: res, err: err}
	}()
	return out
}

// awaitOutcome waits for one send; a missing answer is a test failure.
func awaitOutcome(t *testing.T, what string, ch <-chan sendOutcome) sendOutcome {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for the answer of %s", what)
		return sendOutcome{}
	}
}

// openTasks counts the durable non-terminal tasks through a read-only
// connection (what a second process would find).
func openTasks(t *testing.T, path string) int {
	t.Helper()
	return f1Count(t, path, `SELECT COUNT(*) FROM a2a_tasks WHERE state NOT IN (?, ?, ?, ?)`,
		a2acore.StateCompleted, a2acore.StateCanceled, a2acore.StateFailed, a2acore.StateRejected)
}

func capacitySend(id string) SendInput {
	return SendInput{Project: "org/repo", Job: "job-1", Session: "s-" + id, Epoch: 1, Seq: 1, MessageID: "m-" + id, Text: "task " + id}
}

// wireRecord is one REST exchange as it crossed the wire.
type wireRecord struct {
	request string
	status  int
	body    string
}

// wireCapture records every exchange of the test client, so a test sees
// the status and the body of the error answer next to what the client
// decoded from it. The recorded bodies carry only the fixed fixture text.
type wireCapture struct {
	mu      sync.Mutex
	records []wireRecord
}

// RoundTrip implements http.RoundTripper.
func (w *wireCapture) RoundTrip(r *http.Request) (*http.Response, error) {
	var req []byte
	if r.Body != nil {
		req, _ = io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(req))
	}
	resp, err := http.DefaultTransport.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	w.mu.Lock()
	w.records = append(w.records, wireRecord{request: string(req), status: resp.StatusCode, body: string(body)})
	w.mu.Unlock()
	return resp, nil
}

// answerTo is the recorded exchange of the send of the message.
func (w *wireCapture) answerTo(t *testing.T, messageID string) wireRecord {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range w.records {
		if strings.Contains(r.request, `"`+messageID+`"`) && strings.Contains(r.request, "task "+messageID[len("m-"):]) {
			return r
		}
	}
	t.Fatalf("no recorded exchange for %s", messageID)
	return wireRecord{}
}

// capacityEnv is a limit-of-one store behind a gated server.
type capacityEnv struct {
	store *a2astore.Store
	path  string
	clk   *capacityClock
	gate  *createGate
	c     *Client
	wire  *wireCapture
}

func newCapacityEnv(t *testing.T, messageIDs ...string) *capacityEnv {
	t.Helper()
	clk := newCapacityClock()
	store, path := openTestStore(t, a2astore.Options{Now: clk.Now, Limits: a2acore.Limits{MaxOpenTasks: 1}})
	auth := newTableAuth(testSubject("caller-1"))
	gate := newCreateGate(t, messageIDs...)
	srv := newGatedServer(t, store, auth, gate)
	wire := &wireCapture{}
	c, err := Dial(ctxT(t), serveServer(t, srv).URL, staticCredential{value: auth.token("caller-1")}, &http.Client{Timeout: 10 * time.Second, Transport: wire})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return &capacityEnv{store: store, path: path, clk: clk, gate: gate, c: c, wire: wire}
}

// awaitArrived waits until the create of the message reached the gate.
func (e *capacityEnv) awaitArrived(t *testing.T, id string) {
	t.Helper()
	select {
	case <-e.gate.arrived[id]:
	case <-time.After(10 * time.Second):
		t.Fatalf("the create of %s never reached the gate", id)
	}
}

// TestConcurrentNewTasksNeverExceedOpenLimit pins, through the official
// REST path, the window between the admission of a new task and the
// creation of its task: the create of the first request is held at the
// gate while the second request is sent. The second request either is
// refused at its admission (the reservation of the first counts) or
// reaches its own create; the gate is then opened and the number of
// accepted requests and of durable non-terminal tasks must not pass the
// limit of one. No sleeps: the only waits are on channels with deadlines.
func TestConcurrentNewTasksNeverExceedOpenLimit(t *testing.T) {
	env := newCapacityEnv(t, "m-x", "m-y")
	xDone := sendAsync(t, env.c, capacitySend("x"))
	env.awaitArrived(t, "m-x")
	yDone := sendAsync(t, env.c, capacitySend("y"))
	var y sendOutcome
	yEarly := false
	select {
	case y = <-yDone:
		yEarly = true
	case <-env.gate.arrived["m-y"]:
	case <-time.After(10 * time.Second):
		t.Fatal("the second request neither answered nor reached its create")
	}
	env.gate.open("m-x")
	env.gate.open("m-y")
	x := awaitOutcome(t, "the first request", xDone)
	if !yEarly {
		y = awaitOutcome(t, "the second request", yDone)
	}
	accepted := 0
	for _, o := range []sendOutcome{x, y} {
		if o.err == nil {
			accepted++
		}
	}
	if n := openTasks(t, env.path); n > 1 {
		t.Errorf("durable non-terminal tasks = %d, want at most the limit of 1", n)
	}
	if accepted > 1 {
		t.Errorf("accepted requests = %d, want at most the limit of 1", accepted)
	}
	if x.err != nil {
		t.Errorf("the first request was refused: %v", x.err)
	}
	if y.err == nil {
		t.Errorf("the second request was accepted, want the capacity refusal")
	} else {
		if !errors.Is(y.err, a2a.ErrInvalidParams) || !strings.Contains(y.err.Error(), "refused: "+a2acore.RefuseCapacity) {
			t.Errorf("the second request error = %v, want invalid params with the capacity refusal", y.err)
		}
		t.Logf("admission refusal as the client sees it: %q (invalid params: %v)", y.err.Error(), errors.Is(y.err, a2a.ErrInvalidParams))
	}
}

// TestExpiredReservationIsRefusedByTheCreateBackstop pins the case the
// admission cannot see: the reservation of the first request expired
// (the injected clock passes repointGrace) while its create is still
// held, the second request is admitted and committed, and the late
// create of the first is refused by the definitive check of the store.
// It records the exact error the client receives for that refusal next
// to the one of a refusal at admission.
func TestExpiredReservationIsRefusedByTheCreateBackstop(t *testing.T) {
	env := newCapacityEnv(t, "m-x", "m-y")
	xDone := sendAsync(t, env.c, capacitySend("x"))
	env.awaitArrived(t, "m-x")
	env.clk.Add(60 * time.Second)
	yDone := sendAsync(t, env.c, capacitySend("y"))
	env.awaitArrived(t, "m-y")
	env.gate.open("m-y")
	y := awaitOutcome(t, "the second request", yDone)
	if y.err != nil {
		t.Fatalf("the second request was refused although the reservation of the first expired: %v", y.err)
	}
	if n := openTasks(t, env.path); n != 1 {
		t.Fatalf("durable non-terminal tasks before the late create = %d, want 1", n)
	}
	env.gate.open("m-x")
	x := awaitOutcome(t, "the first request", xDone)
	if x.err == nil {
		t.Errorf("the late create of the first request succeeded, want the capacity refusal")
	} else {
		t.Logf("create backstop refusal as the client sees it: %q (invalid params: %v)", x.err.Error(), errors.Is(x.err, a2a.ErrInvalidParams))
		if !errors.Is(x.err, a2a.ErrInvalidParams) || !strings.Contains(x.err.Error(), "refused: "+a2acore.RefuseCapacity) {
			t.Errorf("the backstop refusal error = %v, want invalid params with the capacity refusal text", x.err)
		}
	}
	if n := openTasks(t, env.path); n != 1 {
		t.Errorf("durable non-terminal tasks = %d, want 1", n)
	}
	// The refused create left its deliveries held (it never committed).
	if n := f1Count(t, env.path, `SELECT COUNT(*) FROM a2a_deliveries WHERE state = 'held'`); n != 1 {
		t.Errorf("held deliveries = %d, want 1 (the refused task's)", n)
	}
	// The same request refused at its admission, for the mapping: a new
	// task while the second one holds the only slot.
	zDone := sendAsync(t, env.c, capacitySend("z"))
	z := awaitOutcome(t, "the third request", zDone)
	if z.err == nil {
		t.Fatalf("the third request was accepted at the limit")
	}
	t.Logf("admission refusal as the client sees it: %q (invalid params: %v)", z.err.Error(), errors.Is(z.err, a2a.ErrInvalidParams))
	if x.err != nil && !errors.Is(z.err, a2a.ErrInvalidParams) {
		t.Errorf("the admission refusal is not invalid params: %v", z.err)
	}
	// The wire: the status and the body of both refusals.
	wx, wz := env.wire.answerTo(t, "m-x"), env.wire.answerTo(t, "m-z")
	t.Logf("create backstop refusal on the wire: HTTP %d %s", wx.status, wx.body)
	t.Logf("admission refusal on the wire: HTTP %d %s", wz.status, wz.body)
	if wx.status != http.StatusBadRequest || !strings.Contains(wx.body, "refused: "+a2acore.RefuseCapacity) {
		t.Errorf("the backstop refusal answered HTTP %d, want 400 with the capacity refusal text", wx.status)
	}
	if wx.status != wz.status {
		t.Errorf("the backstop refusal answered HTTP %d, the admission refusal HTTP %d, want the same status", wx.status, wz.status)
	}
}
