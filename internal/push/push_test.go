package push

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// seedStore writes n decision-like records into a fresh state dir and
// returns the store.
func seedStore(t *testing.T, n int) *outbox.Store {
	t.Helper()
	s, err := outbox.Open(t.TempDir(), outbox.Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 1; i <= n; i++ {
		if _, err := s.Append(outbox.Record{
			Tipo: outbox.TipoDecision, Maquina: "machine-a", Projeto: "org/repo",
			Dados: []byte(`{"resumo":"r` + itoa(i) + `","motivo":"m","escopo":"global"}`),
		}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return s
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for n := i; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

// fakeSleep records the requested durations and returns immediately.
type fakeSleep struct {
	durs   []time.Duration
	failAt int // fail (context error) on this 1-based call, 0 = never
	calls  int
}

func (f *fakeSleep) Sleep(ctx context.Context, d time.Duration) error {
	f.calls++
	f.durs = append(f.durs, d)
	if f.failAt == f.calls {
		return context.Canceled
	}
	return ctx.Err()
}

// request is one captured dispatcher request.
type request struct {
	method string
	path   string
	header http.Header
	body   []byte
}

// recordingServer runs an httptest server that logs every request.
type recordingServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []request
}

func newRecordingServer(t *testing.T, status int, delay time.Duration) *recordingServer {
	t.Helper()
	r := &recordingServer{}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.requests = append(r.requests, request{method: req.Method, path: req.URL.Path, header: req.Header.Clone(), body: body})
		r.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *recordingServer) all() []request {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]request, len(r.requests))
	copy(out, r.requests)
	return out
}

func TestPushNewHTTPValidation(t *testing.T) {
	const key = "the-key"
	cases := []struct {
		raw  string
		ok   bool
		name string
	}{
		{"https://example.invalid/x", true, "https anywhere (unidentifiable placeholder, never contacted)"},
		{"http://localhost:8080/x", true, "http loopback localhost"},
		{"http://127.0.0.1:8080/x", true, "http loopback 127.0.0.1"},
		{"http://127.0.7.9:8080/x", true, "http loopback 127/8"},
		{"http://[::1]:8080/x", true, "http loopback v6"},
		{"http://10.0.0.1:8080/x", false, "http non-loopback ip"},
		{"http://example.invalid/x", false, "http non-loopback host"},
		{"ftp://127.0.0.1/x", false, "other scheme refused"},
		{"http://127.0.0.1.example/x", false, "http lookalike 127.0.0.1 prefix"},
		{"http://127.0.0.1.example.com/x", false, "http lookalike 127.0.0.1 subdomain"},
		{"http://localhost.example/x", false, "http lookalike localhost prefix"},
		{"http://localhost.example.com/x", false, "http lookalike localhost subdomain"},
		{"", false, "empty url"},
		{"https://", false, "url without host"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewHTTP(tc.raw, key, "1.2.3", 5*time.Second)
			if tc.ok && err != nil {
				t.Fatalf("NewHTTP(%q) = %v, want ok", tc.raw, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("NewHTTP(%q) = ok, want refusal", tc.raw)
			}
			if err != nil && (strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "Authorization")) {
				t.Fatalf("refusal error leaks the key: %v", err)
			}
		})
	}
	if _, err := NewHTTP("https://h", "", "1", time.Second); err == nil {
		t.Fatal("NewHTTP with an empty key must be refused")
	}
}

func TestPushHTTPRequestShape(t *testing.T) {
	const key = "bearer-me-42"
	srv := newRecordingServer(t, 200, 0)
	h, err := NewHTTP(srv.URL, key, "9.9.9", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"schema":1,"seq":1,"idempotency_key":"m:1"}`)
	status, err := h.Push(context.Background(), body, "m:1")
	if err != nil || status != 200 {
		t.Fatalf("Push = %d, %v", status, err)
	}
	reqs := srv.all()
	if len(reqs) != 1 {
		t.Fatalf("server saw %d requests, want 1", len(reqs))
	}
	q := reqs[0]
	if q.method != http.MethodPost {
		t.Errorf("method = %q, want POST", q.method)
	}
	if q.path != "/" {
		t.Errorf("path = %q, want the URL path as configured", q.path)
	}
	if string(q.body) != string(body) {
		t.Errorf("body = %q, want the outbox record line unchanged", q.body)
	}
	want := map[string]string{
		"Authorization":   "Bearer " + key,
		"Idempotency-Key": "m:1",
		"Content-Type":    "application/json",
		"User-Agent":      "herdr-hermes/9.9.9",
	}
	for hname, val := range want {
		if got := q.header.Get(hname); got != val {
			t.Errorf("header %s = %q, want %q", hname, got, val)
		}
	}
	// The key must appear in exactly one place: the Authorization header.
	occurrences := 0
	for name, vals := range q.header {
		for _, v := range vals {
			if strings.Contains(v, key) {
				occurrences++
			}
		}
		_ = name
	}
	if occurrences != 1 {
		t.Errorf("key occurrences in headers = %d, want exactly 1 (Authorization)", occurrences)
	}
}

func TestPushNoRedirectFollowed(t *testing.T) {
	var target atomic.Int32
	targetSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target.Add(1)
	}))
	defer targetSrv.Close()
	redirectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, targetSrv.URL, http.StatusFound)
	}))
	defer redirectSrv.Close()
	h, err := NewHTTP(redirectSrv.URL, "k", "1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	status, err := h.Push(context.Background(), []byte(`{}`), "m:1")
	if err != nil {
		t.Fatalf("Push = %v; with ErrUseLastResponse the 3xx itself is the result", err)
	}
	if status != http.StatusFound {
		t.Fatalf("status = %d, want the 302 itself (no redirect followed)", status)
	}
	if n := target.Load(); n != 0 {
		t.Fatalf("redirect target received %d requests, want 0", n)
	}
}

func TestPushTransportErrorNeverLeaksKey(t *testing.T) {
	const key = "leak-check-xyz"
	// Find a loopback port that refuses connections, so Do fails
	// client-side with no dispatcher to echo anything back.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	h, err := NewHTTP("http://"+addr+"/x", key, "1", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var pe *TransportError
	status, err := h.Push(context.Background(), []byte(`{}`), "m:1")
	if err == nil {
		t.Fatalf("Push = %d, want a transport error", status)
	}
	if !errors.As(err, &pe) {
		t.Fatalf("error type = %T, want *TransportError", err)
	}
	msg := err.Error()
	if strings.Contains(msg, key) || strings.Contains(msg, "Authorization") || strings.Contains(msg, "Bearer") {
		t.Fatalf("transport error leaks the key or its header: %q", msg)
	}
	// The message is a fixed text plus the kind, nothing else.
	if msg != "push: transport connection" {
		t.Fatalf("transport error message = %q, want the fixed text plus the kind", msg)
	}
	// A deadline on the bounded context classifies as a timeout.
	slow := newRecordingServer(t, 200, 500*time.Millisecond)
	h2, _ := NewHTTP(slow.URL, key, "1", 30*time.Millisecond)
	_, err = h2.Push(context.Background(), []byte(`{}`), "m:1")
	if err == nil {
		t.Fatal("Push with an expired deadline must fail")
	}
	if !errors.As(err, &pe) || pe.Kind != "timeout" {
		t.Fatalf("deadline error = %v, want *TransportError{Kind:timeout}", err)
	}
	if strings.Contains(err.Error(), key) {
		t.Fatalf("timeout error leaks the key: %v", err)
	}
	// A canceled context classifies as canceled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h3, _ := NewHTTP(slow.URL, key, "1", time.Second)
	_, err = h3.Push(ctx, []byte(`{}`), "m:1")
	if err == nil {
		t.Fatal("Push on a canceled context must fail")
	}
	if !errors.As(err, &pe) || pe.Kind != "canceled" {
		t.Fatalf("canceled error = %v, want *TransportError{Kind:canceled}", err)
	}
}

func TestPushPendingDeliversInOrder(t *testing.T) {
	s := seedStore(t, 3)
	srv := newRecordingServer(t, 200, 0)
	h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
	var sleeps []time.Duration
	res := Pending(context.Background(), s, h, func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}, true)
	if res.Sent != 3 || res.Pending != 0 || res.Code != 0 || res.Status != "" {
		t.Fatalf("Result = %+v, want all delivered", res)
	}
	if len(sleeps) != 0 {
		t.Fatalf("success path slept %v, want no sleeps", sleeps)
	}
	reqs := srv.all()
	if len(reqs) != 3 {
		t.Fatalf("server saw %d requests, want 3", len(reqs))
	}
	for i, q := range reqs {
		if !strings.Contains(string(q.body), `"seq":`+itoa(i+1)) {
			t.Errorf("request %d body has seq other than %d:\n%s", i, i+1, q.body)
		}
		wantKey := "machine-a:" + itoa(i+1)
		if q.header.Get("Idempotency-Key") != wantKey {
			t.Errorf("request %d Idempotency-Key = %q, want %q", i, q.header.Get("Idempotency-Key"), wantKey)
		}
	}
	got, err := s.DeliveredSeq()
	if err != nil || got != 3 {
		t.Fatalf("delivered_seq = %d, %v; want 3", got, err)
	}
	lp, err := s.LastPush()
	if err != nil || lp == nil || lp.Status != "ok" || lp.Code != 200 || lp.TS == "" {
		t.Fatalf("last_push = %+v, %v; want ok/200 with ts", lp, err)
	}
}

func TestPushPendingAuthRejectedStops(t *testing.T) {
	s := seedStore(t, 3)
	srv := newRecordingServer(t, 401, 0)
	h, _ := NewHTTP(srv.URL, "bad-key", "1", 5*time.Second)
	res := Pending(context.Background(), s, h, func(context.Context, time.Duration) error { return nil }, true)
	if res.Code != 41 || res.Status != "auth_rejected" {
		t.Fatalf("Result = %+v, want 41 auth_rejected", res)
	}
	if res.Sent != 0 || res.Pending != 3 {
		t.Fatalf("Sent/Pending = %d/%d, want 0/3", res.Sent, res.Pending)
	}
	if n := len(srv.all()); n != 1 {
		t.Fatalf("server saw %d requests, want exactly 1 (stop, no retry)", n)
	}
	if got, _ := s.DeliveredSeq(); got != 0 {
		t.Fatalf("delivered_seq = %d, want 0", got)
	}
	lp, _ := s.LastPush()
	if lp == nil || lp.Status != "auth_rejected" || lp.Code != 401 {
		t.Fatalf("last_push = %+v, want auth_rejected/401", lp)
	}
	// One friction line on the stop, without the key.
	friction := readFriction(t, s.Dir())
	if len(friction) != 1 {
		t.Fatalf("friction lines = %d, want 1", len(friction))
	}
	if strings.Contains(friction[0], "bad-key") || !strings.Contains(friction[0], "auth_rejected") {
		t.Fatalf("friction line = %q", friction[0])
	}
	// 403 behaves the same.
	s2 := seedStore(t, 1)
	srv2 := newRecordingServer(t, 403, 0)
	h2, _ := NewHTTP(srv2.URL, "bad-key", "1", 5*time.Second)
	if res := Pending(context.Background(), s2, h2, func(context.Context, time.Duration) error { return nil }, true); res.Code != 41 || res.Status != "auth_rejected" {
		t.Fatalf("403 Result = %+v, want 41 auth_rejected", res)
	}
}

func TestPushPendingRetrySchedule(t *testing.T) {
	cases := []struct {
		status int
		name   string
	}{
		{http.StatusRequestTimeout, "408"},
		{http.StatusTooManyRequests, "429"},
		{http.StatusInternalServerError, "500"},
		{http.StatusServiceUnavailable, "503"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := seedStore(t, 2)
			srv := newRecordingServer(t, tc.status, 0)
			h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
			fs := &fakeSleep{}
			res := Pending(context.Background(), s, h, fs.Sleep, true)
			if res.Code != 42 || res.Status != "unreachable" {
				t.Fatalf("Result = %+v, want 42 unreachable", res)
			}
			if res.Sent != 0 || res.Pending != 2 {
				t.Fatalf("Sent/Pending = %d/%d, want 0/2", res.Sent, res.Pending)
			}
			// Exactly 1 attempt + 3 retries = 4 requests on the first
			// record only; the batch stops there.
			if n := len(srv.all()); n != 4 {
				t.Fatalf("server saw %d requests, want 4 (1 + 3 retries)", n)
			}
			// The 10s/30s/90s schedule, in order, via the injected sleep.
			want := []time.Duration{10 * time.Second, 30 * time.Second, 90 * time.Second}
			if len(fs.durs) != 3 {
				t.Fatalf("sleeps = %v, want exactly %v", fs.durs, want)
			}
			for i := range want {
				if fs.durs[i] != want[i] {
					t.Fatalf("sleep %d = %v, want %v (fake clock must never shorten or extend the schedule)", i, fs.durs[i], want[i])
				}
			}
			lp, _ := s.LastPush()
			if lp == nil || lp.Status != "unreachable" || lp.Code != tc.status {
				t.Fatalf("last_push = %+v, want unreachable/%d", lp, tc.status)
			}
		})
	}
	// A backwards or stuck clock changes nothing: the schedule is the
	// fixed 10/30/90s list, recorded by the fake sleep.
	t.Run("BackwardsClock", func(t *testing.T) {
		s := seedStore(t, 1)
		srv := newRecordingServer(t, 500, 0)
		h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
		fs := &fakeSleep{}
		res := Pending(context.Background(), s, h, fs.Sleep, true)
		if res.Code != 42 || len(srv.all()) != 4 {
			t.Fatalf("Result = %+v, %d requests; want 42 after 4 attempts", res, len(srv.all()))
		}
		if d := fs.durs; len(d) != 3 || d[0] != 10*time.Second || d[1] != 30*time.Second || d[2] != 90*time.Second {
			t.Fatalf("schedule under a fake clock = %v, want 10s/30s/90s", d)
		}
	})
}

func TestPushPendingNoRetrySingleAttempt(t *testing.T) {
	s := seedStore(t, 2)
	srv := newRecordingServer(t, 500, 0)
	h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
	fs := &fakeSleep{}
	res := Pending(context.Background(), s, h, fs.Sleep, false)
	if res.Code != 42 || res.Status != "unreachable" {
		t.Fatalf("Result = %+v, want 42 unreachable", res)
	}
	if n := len(srv.all()); n != 1 {
		t.Fatalf("server saw %d requests, want exactly 1 (no retries when interactive=false)", n)
	}
	if len(fs.durs) != 0 {
		t.Fatalf("sleeps = %v, want none", fs.durs)
	}
}

func TestPushPendingRejectedStopsWithoutRetry(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusBadRequest} {
		s := seedStore(t, 2)
		srv := newRecordingServer(t, status, 0)
		h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
		fs := &fakeSleep{}
		res := Pending(context.Background(), s, h, fs.Sleep, true)
		if res.Code != 42 || res.Status != "rejected" {
			t.Fatalf("%d: Result = %+v, want 42 rejected", status, res)
		}
		if n := len(srv.all()); n != 1 {
			t.Fatalf("%d: server saw %d requests, want 1 (no retry on other 4xx)", status, n)
		}
		lp, _ := s.LastPush()
		if lp == nil || lp.Status != "rejected" || lp.Code != status {
			t.Fatalf("%d: last_push = %+v, want rejected/%d", status, lp, status)
		}
	}
}

func TestPushPendingTransportErrorRetried(t *testing.T) {
	s := seedStore(t, 1)
	// A listener that accepts and immediately closes: a transport error,
	// not an HTTP status.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	h, err := NewHTTP("http://"+ln.Addr().String(), "transportkey-77", "1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeSleep{}
	res := Pending(context.Background(), s, h, fs.Sleep, true)
	ln.Close()
	<-done
	if res.Code != 42 || res.Status != "unreachable" {
		t.Fatalf("Result = %+v, want 42 unreachable", res)
	}
	if n := len(fs.durs); n != 3 {
		t.Fatalf("sleeps = %v, want the 3-retry schedule", fs.durs)
	}
	if res.Sent != 0 || res.Pending != 1 {
		t.Fatalf("Sent/Pending = %d/%d, want 0/1", res.Sent, res.Pending)
	}
	lp, _ := s.LastPush()
	if lp == nil || lp.Status != "unreachable" || lp.Code != 0 {
		t.Fatalf("last_push = %+v, want unreachable/0 (no HTTP status)", lp)
	}
	friction := readFriction(t, s.Dir())
	if len(friction) != 1 {
		t.Fatalf("friction lines = %d, want 1", len(friction))
	}
	joined := strings.Join(friction, "\n")
	if strings.Contains(joined, "transportkey-77") || !strings.Contains(joined, "unreachable") {
		t.Fatalf("friction line = %q", joined)
	}
}

func TestPushPendingSleepErrorStops(t *testing.T) {
	s := seedStore(t, 1)
	srv := newRecordingServer(t, 500, 0)
	h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
	fs := &fakeSleep{failAt: 1}
	res := Pending(context.Background(), s, h, fs.Sleep, true)
	if res.Code != 42 || res.Status != "unreachable" {
		t.Fatalf("Result = %+v, want 42 unreachable when the backoff sleep fails", res)
	}
	if n := len(srv.all()); n != 1 {
		t.Fatalf("server saw %d requests, want 1 (stop before the retry)", n)
	}
}

func TestPushPendingTimeoutRetried(t *testing.T) {
	s := seedStore(t, 1)
	srv := newRecordingServer(t, 200, 300*time.Millisecond) // slower than the 30ms request deadline
	h, err := NewHTTP(srv.URL, "k", "1", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeSleep{}
	res := Pending(context.Background(), s, h, fs.Sleep, true)
	if res.Code != 42 || res.Status != "unreachable" {
		t.Fatalf("Result = %+v, want 42 unreachable on request timeouts", res)
	}
	if n := len(srv.all()); n != 4 {
		t.Fatalf("server saw %d requests, want 4 (1 + 3 retries)", n)
	}
	if len(fs.durs) != 3 {
		t.Fatalf("sleeps = %v, want 3", fs.durs)
	}
}

func TestPushPendingRecoveryAfterOutage(t *testing.T) {
	// [offline] a later successful push after an outage delivers
	// everything, in order.
	s := seedStore(t, 3)
	var fail atomic.Bool
	fail.Store(true)
	srv := &recordingServer{}
	srv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		srv.mu.Lock()
		srv.requests = append(srv.requests, request{method: r.Method, path: r.URL.Path, header: r.Header.Clone(), body: body})
		srv.mu.Unlock()
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
	noSleep := func(context.Context, time.Duration) error { return nil }

	res := Pending(context.Background(), s, h, noSleep, true)
	if res.Code != 42 || res.Status != "unreachable" || res.Pending != 3 {
		t.Fatalf("outage Result = %+v, want 42 unreachable with 3 pending", res)
	}
	if got, _ := s.DeliveredSeq(); got != 0 {
		t.Fatalf("delivered_seq after outage = %d, want 0", got)
	}
	fail.Store(false)
	res = Pending(context.Background(), s, h, noSleep, true)
	if res.Code != 0 || res.Sent != 3 || res.Pending != 0 {
		t.Fatalf("recovery Result = %+v, want all 3 delivered", res)
	}
	reqs := srv.all()
	if len(reqs) != 4+3 {
		t.Fatalf("total requests = %d, want 4 (outage) + 3 (recovery)", len(reqs))
	}
	for i := 4; i < len(reqs); i++ {
		if !strings.Contains(string(reqs[i].body), `"seq":`+itoa(i-3)) {
			t.Errorf("recovery request %d does not carry seq %d in order: %s", i-4, i-3, reqs[i].body)
		}
	}
	if got, _ := s.DeliveredSeq(); got != 3 {
		t.Fatalf("delivered_seq after recovery = %d, want 3", got)
	}
}

func TestPushPendingCrashResendsSameKey(t *testing.T) {
	// [crash] a crash after a successful POST and before SetDeliveredSeq
	// re-sends the same record with the same Idempotency-Key.
	s := seedStore(t, 2)
	srv := newRecordingServer(t, 200, 0)
	h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
	noSleep := func(context.Context, time.Duration) error { return nil }
	// First push: the dispatcher accepts both records, but the process
	// dies before the cursor update (simulate by never calling
	// SetDeliveredSeq).
	res := Pending(context.Background(), s, h, noSleep, true)
	if res.Code != 0 || res.Sent != 2 {
		t.Fatalf("first push Result = %+v, want 2 delivered", res)
	}
	// The process dies after the POSTs but before the cursor update:
	// rewind the cursor file to where the crash left it (0).
	cursorAt(s, t, 0)
	res2 := Pending(context.Background(), s, h, noSleep, true)
	if res2.Code != 0 || res2.Sent != 2 || res2.Pending != 0 {
		t.Fatalf("recovery Result = %+v, want 2 delivered", res2)
	}
	reqs := srv.all()
	if len(reqs) != 4 {
		t.Fatalf("server saw %d requests, want 2 + 2 resends", len(reqs))
	}
	if reqs[0].header.Get("Idempotency-Key") != reqs[2].header.Get("Idempotency-Key") {
		t.Fatalf("resend changed the Idempotency-Key: %q vs %q", reqs[0].header.Get("Idempotency-Key"), reqs[2].header.Get("Idempotency-Key"))
	}
	if string(reqs[0].body) != string(reqs[2].body) {
		t.Fatalf("resend changed the record body:\n%s\nvs\n%s", reqs[0].body, reqs[2].body)
	}
	if reqs[1].header.Get("Idempotency-Key") != reqs[3].header.Get("Idempotency-Key") {
		t.Fatalf("second record resent with a different key")
	}
}

func cursorAt(s *outbox.Store, t *testing.T, n int64) {
	t.Helper()
	// Simulate the crash window: the records are durable in outbox.jsonl
	// but the cursor file never moved. SetDeliveredSeq refuses to move
	// backwards, so the test rewrites cursor.json with the package's own
	// atomic writer.
	err := outbox.WriteFileAtomic(s.Dir()+"/cursor.json", []byte(`{"delivered_seq":`+itoa(int(n))+`}`))
	if err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
}

func TestPushPendingEmptyNoLastPush(t *testing.T) {
	s := seedStore(t, 1)
	if err := s.SetDeliveredSeq(1); err != nil {
		t.Fatal(err)
	}
	srv := newRecordingServer(t, 200, 0)
	h, _ := NewHTTP(srv.URL, "k", "1", 5*time.Second)
	res := Pending(context.Background(), s, h, func(context.Context, time.Duration) error { return nil }, true)
	if res.Code != 0 || res.Sent != 0 || res.Pending != 0 || res.Status != "" {
		t.Fatalf("Result = %+v, want the zero result", res)
	}
	if n := len(srv.all()); n != 0 {
		t.Fatalf("server saw %d requests, want 0 (nothing pending)", n)
	}
	if lp, _ := s.LastPush(); lp != nil {
		t.Fatalf("last_push = %+v, want untouched when nothing was pushed", lp)
	}
}

func TestPushPendingURLRefusedByNewHTTPIsNotHere(t *testing.T) {
	// NewHTTP refuses non-loopback http; Pending never sees such a client.
	// This test pins the loopback http acceptance used by httptest.
	srv := newRecordingServer(t, 200, 0)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if u.Scheme != "http" {
		t.Fatalf("httptest server scheme = %q, want http (loopback, allowed)", u.Scheme)
	}
	h, err := NewHTTP(srv.URL, "k", "1", 5*time.Second)
	if err != nil {
		t.Fatalf("NewHTTP on the httptest (loopback http) URL = %v, want accepted", err)
	}
	s := seedStore(t, 1)
	res := Pending(context.Background(), s, h, func(context.Context, time.Duration) error { return nil }, true)
	if res.Sent != 1 {
		t.Fatalf("Result = %+v, want 1 delivered", res)
	}
}

func readFriction(t *testing.T, dir string) []string {
	t.Helper()
	data, err := readFile(dir + "/friction.log")
	if err != nil {
		return nil
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimSuffix(string(data), "\n"), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
