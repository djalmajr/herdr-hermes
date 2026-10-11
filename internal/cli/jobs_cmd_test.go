package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/registry"
)

// runCLIVars is runCLI with an explicit environment map (for the NOWRITE
// cases that must not use getenvFor).
func runCLIVars(t *testing.T, dir string, vars map[string]string, args ...string) (string, string, int) {
	t.Helper()
	var out, errb bytes.Buffer
	env := Env{
		Stdin:     strings.NewReader(""),
		Stdout:    &out,
		Stderr:    &errb,
		Getenv:    func(k string) string { return vars[k] },
		Environ:   func() []string { return fakeChildEnv },
		ConfigDir: dir,
		Now:       func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) },
		Sleep:     sleepCtx,
	}
	exit := Run(args, env)
	return out.String(), errb.String(), exit
}

// captureServer serves a fixed response (or a redirect) and reports every
// request through the channel; the counter proves whether anything was
// contacted at all.
func captureServer(t *testing.T, code int, resp []byte, redirect bool) (*httptest.Server, chan wireCapture, *atomic.Int32) {
	t.Helper()
	ch := make(chan wireCapture, 8)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		c := wireCapture{
			method: r.Method,
			path:   r.URL.Path,
			raw:    r.URL.RawQuery,
			ua:     r.Header.Get("User-Agent"),
			ct:     r.Header.Get("Content-Type"),
			auth:   r.Header.Get("Authorization"),
		}
		if r.Body != nil {
			b, _ := io.ReadAll(r.Body)
			c.body = string(b)
		}
		ch <- c
		if redirect {
			w.Header().Set("Location", "/elsewhere")
			w.WriteHeader(http.StatusFound)
			return
		}
		w.WriteHeader(code)
		w.Write(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, ch, &hits
}

type wireCapture struct {
	method string
	path   string
	raw    string
	body   string
	ua     string
	ct     string
	auth   string
}

// requireCapture waits for the captured request with a bounded deadline,
// so a client that never sends one fails instead of hanging.
func requireCapture(t *testing.T, ch <-chan wireCapture) wireCapture {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(10 * time.Second):
		t.Fatal("the server never received the request")
	}
	return wireCapture{}
}

// TestJobsUsage: every malformed jobs form exits 2 before any request is
// sent; the invalid --server values are refused with the dedicated
// motivo.
func TestJobsUsage(t *testing.T) {
	cases := []struct {
		name   string
		args   []string
		motivo string
	}{
		{"unknown flag", []string{"--bogus"}, "usage: jobs"},
		{"positional", []string{"frobnicate"}, "usage: jobs"},
		{"trailing positional", []string{"--maquina", "m", "extra"}, "usage: jobs"},
		{"check missing agente", []string{"check"}, "usage: jobs"},
		{"check agente repeated", []string{"check", "--agente", "a", "--agente", "b"}, "usage: jobs"},
		{"check equals form", []string{"check", "--agente=a"}, "usage: jobs"},
		{"check unknown flag", []string{"check", "--agente", "a", "--bogus"}, "usage: jobs"},
		{"check missing ttl value", []string{"check", "--agente", "a", "--ttl"}, "usage: jobs"},
		{"release missing claim", []string{"release"}, "usage: jobs"},
		{"release unknown flag", []string{"release", "--claim", "x", "--bogus"}, "usage: jobs"},
		{"policy missing verb", []string{"policy"}, "usage: jobs"},
		{"policy bad verb", []string{"policy", "frobnicate"}, "usage: jobs"},
		{"policy verb as flag", []string{"policy", "--set"}, "usage: jobs"},
		{"policy set missing escopo", []string{"policy", "set", "--valor", "v"}, "usage: jobs"},
		{"policy set missing valor", []string{"policy", "set", "--escopo", "card"}, "usage: jobs"},
		{"policy clear extra flag", []string{"policy", "clear", "--escopo", "card", "--valor", "v", "--hold"}, "usage: jobs"},
		{"folder add missing pedido", []string{"folder", "add", "--maquina", "m", "--caminho", "/x"}, "usage: jobs"},
		{"folder bad verb", []string{"folder", "frobnicate"}, "usage: jobs"},
		{"node set missing maquina", []string{"node", "set"}, "usage: jobs"},
		{"node set no optional", []string{"node", "set", "--maquina", "m"}, "usage: jobs"},
		{"node bad manutencao", []string{"node", "set", "--maquina", "m", "--manutencao", "maybe"}, "usage: jobs"},
		{"node empty runners", []string{"node", "set", "--maquina", "m", "--runners", ""}, "usage: jobs"},
		{"node unknown flag", []string{"node", "set", "--maquina", "m", "--soho-version", "0.9.1", "--bogus"}, "usage: jobs"},
		{"server plain http non-loopback", []string{"--server", "http://192.0.2.10:80"}, "jobs: invalid --server"},
		{"server http userinfo", []string{"--server", "http://user@127.0.0.1:7429"}, "jobs: invalid --server"},
		{"server http query", []string{"--server", "http://127.0.0.1:7429?x=1"}, "jobs: invalid --server"},
		{"server http fragment", []string{"--server", "http://127.0.0.1:7429#f"}, "jobs: invalid --server"},
		{"server http path", []string{"--server", "http://127.0.0.1:7429/api"}, "jobs: invalid --server"},
		{"server bad scheme", []string{"--server", "ftp://127.0.0.1:7429"}, "jobs: invalid --server"},
		{"server empty", []string{"--server", ""}, "jobs: invalid --server"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stdout, stderr, exit := runCLI(t, dir, false, append([]string{"jobs"}, tc.args...)...)
			if exit != 2 {
				t.Fatalf("jobs %v exit = %d, want 2 (stdout %q)", tc.args, exit, stdout)
			}
			if !strings.Contains(stdout, tc.motivo) {
				t.Errorf("stdout = %q, want %q", stdout, tc.motivo)
			}
			if strings.TrimSpace(stderr) == "" {
				t.Errorf("no diagnostic on stderr")
			}
		})
	}
}

// TestJobsNowrite: the plain inventory runs under HERDR_HERMES_NOWRITE=1;
// every other form is refused before any request.
//
// Mutation captured: disabling the NOWRITE gate lets the subcommands send
// their request and exit 0 instead of 2.
func TestJobsNowrite(t *testing.T) {
	t.Run("inventory allowed", func(t *testing.T) {
		srv, _, hits := captureServer(t, http.StatusOK, []byte(`{"jobs":[],"manutencao":[],"folders":[],"nodes":[],"poll":[]}`), false)
		dir := t.TempDir()
		stdout, _, exit := runCLIVars(t, dir, map[string]string{"HERDR_HERMES_NOWRITE": "1"}, "jobs", "--server", srv.URL)
		if exit != 0 {
			t.Fatalf("jobs inventory under NOWRITE exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		if hits.Load() != 1 {
			t.Errorf("inventory hit the server %d times, want 1", hits.Load())
		}
	})
	for name, args := range map[string][]string{
		"check":   {"check", "--agente", "a"},
		"release": {"release", "--claim", "x"},
		"policy":  {"policy", "set", "--escopo", "card", "--valor", "v"},
		"folder":  {"folder", "add", "--maquina", "m", "--caminho", "/x", "--pedido", "p"},
		"node":    {"node", "set", "--maquina", "m", "--motivo", "x"},
	} {
		t.Run(name+" refused", func(t *testing.T) {
			srv, _, hits := captureServer(t, http.StatusOK, []byte(`{"status":"ok"}`), false)
			dir := t.TempDir()
			stdout, _, exit := runCLIVars(t, dir, map[string]string{"HERDR_HERMES_NOWRITE": "1"}, append([]string{"jobs", "--server", srv.URL}, args...)...)
			if exit != 2 {
				t.Fatalf("jobs %s under NOWRITE exit = %d, want 2 (stdout %q)", name, exit, stdout)
			}
			if stdout != `{"status":"nowrite","motivo":"HERDR_HERMES_NOWRITE=1"}`+"\n" {
				t.Errorf("stdout = %q", stdout)
			}
			if hits.Load() != 0 {
				t.Errorf("the request was sent %d times, want 0 (refused before any request)", hits.Load())
			}
		})
	}
}

// TestJobsErrors: the status mapping — 400 → 2 with the server motivo,
// 404 → 3 for release/policy clear/folder clear, 409 from check → 44,
// anything else → 4, redirects never followed, bodies over the cap and
// non-JSON bodies → 4 malformed, transport errors → 4 unreachable.
//
// Mutation captured: breaking the 409-to-44 mapping (check conflicts)
// makes the check case exit 4 instead of 44.
func TestJobsErrors(t *testing.T) {
	t.Run("400 maps to 2 with the server motivo", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusBadRequest, []byte(`{"status":"invalid","motivo":"agente is required"}`), false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "check", "--agente", "a", "--card", "C-1")
		if exit != 2 {
			t.Fatalf("jobs check 400 exit = %d, want 2 (stdout %q)", exit, stdout)
		}
		if stdout != `{"status":"2","motivo":"registry: agente is required"}`+"\n" {
			t.Errorf("stdout = %q", stdout)
		}
		requireCapture(t, ch)
	})
	t.Run("404 maps to 3 for the three clear forms", func(t *testing.T) {
		for name, args := range map[string][]string{
			"release":      {"release", "--claim", "x"},
			"policy clear": {"policy", "clear", "--escopo", "card", "--valor", "v"},
			"folder clear": {"folder", "clear", "--maquina", "m", "--caminho", "/x"},
		} {
			srv, ch, _ := captureServer(t, http.StatusNotFound, []byte(`{"status":"not_found","motivo":"not found"}`), false)
			dir := t.TempDir()
			stdout, _, exit := runCLI(t, dir, false, append([]string{"jobs", "--server", srv.URL}, args...)...)
			if exit != 3 {
				t.Fatalf("jobs %s 404 exit = %d, want 3 (stdout %q)", name, exit, stdout)
			}
			if stdout != `{"status":"not_found","motivo":"registry: not found"}`+"\n" {
				t.Errorf("%s stdout = %q", name, stdout)
			}
			requireCapture(t, ch)
		}
	})
	t.Run("409 from check maps to 44 with the refused line", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusConflict, []byte(`{"status":"duplicate","conflitos":[{"chave":"ws:2","tipo":"job","campo":"workspace"}]}`), false)
		dir := t.TempDir()
		stdout, stderr, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "check", "--agente", "a", "--workspace", "w")
		if exit != 44 {
			t.Fatalf("jobs check 409 exit = %d, want 44 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, `"status":"duplicate"`) || !strings.Contains(stdout, `"tipo":"job"`) {
			t.Errorf("stdout = %q, want the 409 body as one line", stdout)
		}
		if !strings.Contains(stderr, "herdr-hermes: jobs check: refused (duplicate)") {
			t.Errorf("stderr = %q, want the refused (duplicate) line", stderr)
		}
		requireCapture(t, ch)
	})
	t.Run("409 from a non-check form maps to 4", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusConflict, []byte(`{"status":"conflict"}`), false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "release", "--claim", "x")
		if exit != 4 {
			t.Fatalf("jobs release 409 exit = %d, want 4 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, "registry answered 409") {
			t.Errorf("stdout = %q, want registry answered 409", stdout)
		}
		requireCapture(t, ch)
	})
	t.Run("500 maps to 4 with the code", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusInternalServerError, []byte(`{"status":"500","motivo":"internal error"}`), false)
		dir := t.TempDir()
		stdout, stderr, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL)
		if exit != 4 {
			t.Fatalf("jobs 500 exit = %d, want 4 (stdout %q)", exit, stdout)
		}
		if stdout != `{"status":"unavailable","motivo":"registry answered 500"}`+"\n" {
			t.Errorf("stdout = %q", stdout)
		}
		if strings.TrimSpace(stderr) == "" {
			t.Errorf("no diagnostic on stderr")
		}
		requireCapture(t, ch)
	})
	t.Run("redirects are not followed", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusFound, nil, true)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL)
		if exit != 4 {
			t.Fatalf("jobs redirect exit = %d, want 4 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, "registry answered 302") {
			t.Errorf("stdout = %q, want registry answered 302", stdout)
		}
		requireCapture(t, ch)
	})
	t.Run("non-json 200 maps to 4 malformed", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, []byte("hello"), false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL)
		if exit != 4 {
			t.Fatalf("jobs non-json 200 exit = %d, want 4 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, "registry answered malformed") {
			t.Errorf("stdout = %q, want registry answered malformed", stdout)
		}
		requireCapture(t, ch)
	})
	// The cap is lowered to 1 MiB here so the boundary stays cheap; its
	// real value is pinned below to the registry's inventory bound.
	lowerCap := func(t *testing.T) {
		old := jobsRespCap
		jobsRespCap = 1 << 20
		t.Cleanup(func() { jobsRespCap = old })
	}
	t.Run("body over the cap maps to 4 malformed", func(t *testing.T) {
		lowerCap(t)
		big := strings.Repeat("a", (1<<20)+1)
		srv, ch, _ := captureServer(t, http.StatusOK, []byte(`{"pad":"`+big+`"}`), false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL)
		if exit != 4 {
			t.Fatalf("jobs over-cap body exit = %d, want 4 (stdout %q)", exit, stdout)
		}
		if !strings.Contains(stdout, "registry answered malformed") {
			t.Errorf("stdout = %q, want registry answered malformed", stdout)
		}
		requireCapture(t, ch)
	})
	// Boundary: a full response of exactly the cap is accepted and printed.
	t.Run("body of exactly the cap passes", func(t *testing.T) {
		lowerCap(t)
		big := strings.Repeat("a", 1<<20-10) // with the JSON padding, exactly 1 MiB
		srv, ch, _ := captureServer(t, http.StatusOK, []byte(`{"pad":"`+big+`"}`), false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL)
		if exit != 0 {
			t.Fatalf("jobs at-cap body exit = %d, want 0 (stdout %q)", exit, stdout)
		}
		requireCapture(t, ch)
	})
	t.Run("cap is the registry inventory bound", func(t *testing.T) {
		if jobsRespCap != registry.MaxInventoryBytes {
			t.Fatalf("jobsRespCap = %d, want registry.MaxInventoryBytes (%d)", jobsRespCap, registry.MaxInventoryBytes)
		}
	})
	t.Run("transport error maps to 4 unreachable", func(t *testing.T) {
		dir := t.TempDir()
		stdout, stderr, exit := runCLI(t, dir, false, "jobs", "--server", "http://127.0.0.1:1")
		if exit != 4 {
			t.Fatalf("jobs transport error exit = %d, want 4 (stdout %q)", exit, stdout)
		}
		if stdout != `{"status":"unavailable","motivo":"registry unreachable"}`+"\n" {
			t.Errorf("stdout = %q", stdout)
		}
		if strings.TrimSpace(stderr) == "" {
			t.Errorf("no diagnostic on stderr")
		}
	})
}

// TestJobsRequests: the wire format of every jobs form — method, path,
// query, body and headers — and the 200 path (compact single line, exit
// 0). No request ever carries an Authorization header.
//
// Mutation captured: an off-by-one or a dropped body cap makes the at-cap
// body exit 4 or the over-cap body exit 0, and a cap other than the
// registry's inventory bound fails its pin.
func TestJobsRequests(t *testing.T) {
	ok := []byte(`{"status":"ok"}`)
	t.Run("inventory", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "--maquina", "m1", "--agente", "a1")
		if exit != 0 {
			t.Fatalf("jobs inventory exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "GET" || c.path != "/v1/jobs" || c.raw != "agente=a1&maquina=m1" {
			t.Errorf("wire = %s %s?%s, want GET /v1/jobs?agente=a1&maquina=m1", c.method, c.path, c.raw)
		}
		if c.ct != "" || c.body != "" {
			t.Errorf("inventory request has content-type %q body %q, want none", c.ct, c.body)
		}
		assertWireHeaders(t, c, false)
		if stdout != `{"status":"ok"}`+"\n" {
			t.Errorf("stdout = %q, want the compact body as one line", stdout)
		}
	})
	t.Run("check", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "check", "--agente", "a", "--card", "C-1", "--projeto", "org/repo", "--branch", "feat/x", "--pr", "7", "--workspace", "ws", "--maquina", "m", "--ttl", "120")
		if exit != 0 {
			t.Fatalf("jobs check exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "POST" || c.path != "/v1/check" {
			t.Errorf("wire = %s %s, want POST /v1/check", c.method, c.path)
		}
		var req registryCheckRequest
		if err := json.Unmarshal([]byte(c.body), &req); err != nil {
			t.Fatalf("check body %q: %v", c.body, err)
		}
		if req.Agente != "a" || req.Card != "C-1" || req.Projeto != "org/repo" || req.Branch != "feat/x" || req.PR != "7" || req.Workspace != "ws" || req.Maquina != "m" || req.TTLs != 120 {
			t.Errorf("check body = %q", c.body)
		}
		assertWireHeaders(t, c, true)
	})
	t.Run("release", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "release", "--claim", "cl-1")
		if exit != 0 {
			t.Fatalf("jobs release exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "DELETE" || c.path != "/v1/claims/cl-1" || c.body != "" {
			t.Errorf("wire = %s %s body %q, want DELETE /v1/claims/cl-1 without a body", c.method, c.path, c.body)
		}
		assertWireHeaders(t, c, false)
	})
	t.Run("policy set with every flag", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "policy", "set", "--escopo", "card", "--valor", "DJA-229", "--hold", "--prioridade", "5", "--motivo", "holding the card")
		if exit != 0 {
			t.Fatalf("jobs policy set exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "PUT" || c.path != "/v1/policies" {
			t.Errorf("wire = %s %s, want PUT /v1/policies", c.method, c.path)
		}
		if c.body != `{"escopo":"card","valor":"DJA-229","hold":true,"prioridade":5,"motivo":"holding the card"}` {
			t.Errorf("policy set body = %q", c.body)
		}
		assertWireHeaders(t, c, true)
	})
	t.Run("policy set without optionals", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "policy", "set", "--escopo", "card", "--valor", "DJA-229")
		if exit != 0 {
			t.Fatalf("jobs policy set exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.body != `{"escopo":"card","valor":"DJA-229","hold":false}` {
			t.Errorf("policy set body = %q, want only escopo, valor and hold false", c.body)
		}
		assertWireHeaders(t, c, true)
	})
	t.Run("policy clear", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "policy", "clear", "--escopo", "card", "--valor", "DJA-229")
		if exit != 0 {
			t.Fatalf("jobs policy clear exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "DELETE" || c.path != "/v1/policies" || c.raw != "escopo=card&valor=DJA-229" {
			t.Errorf("wire = %s %s?%s, want DELETE /v1/policies?escopo=card&valor=DJA-229", c.method, c.path, c.raw)
		}
		assertWireHeaders(t, c, false)
	})
	t.Run("folder add", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "folder", "add", "--maquina", "m1", "--caminho", "/tmp/f", "--pedido", "reserve this path", "--tamanho", "123", "--job", "j-1")
		if exit != 0 {
			t.Fatalf("jobs folder add exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "PUT" || c.path != "/v1/folders" {
			t.Errorf("wire = %s %s, want PUT /v1/folders", c.method, c.path)
		}
		if c.body != `{"maquina":"m1","caminho":"/tmp/f","tamanho_bytes":123,"pedido":"reserve this path","job":"j-1"}` {
			t.Errorf("folder add body = %q", c.body)
		}
		assertWireHeaders(t, c, true)
	})
	t.Run("folder clear", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "folder", "clear", "--maquina", "m1", "--caminho", "/tmp/f")
		if exit != 0 {
			t.Fatalf("jobs folder clear exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "DELETE" || c.path != "/v1/folders" || c.raw != "caminho=%2Ftmp%2Ff&maquina=m1" {
			t.Errorf("wire = %s %s?%s, want DELETE /v1/folders?caminho=%%2Ftmp%%2Ff&maquina=m1", c.method, c.path, c.raw)
		}
		assertWireHeaders(t, c, false)
	})
	t.Run("node set with every flag", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "node", "set", "--maquina", "m1", "--manutencao", "on", "--motivo", "maintenance", "--soho-version", "0.9.1", "--runners", "a,b")
		if exit != 0 {
			t.Fatalf("jobs node set exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.method != "PUT" || c.path != "/v1/nodes/m1" {
			t.Errorf("wire = %s %s, want PUT /v1/nodes/m1", c.method, c.path)
		}
		if c.body != `{"manutencao":true,"manutencao_motivo":"maintenance","soho_version":"0.9.1","runners":["a","b"]}` {
			t.Errorf("node set body = %q", c.body)
		}
		assertWireHeaders(t, c, true)
	})
	// Regression: --manutencao off sent nothing, so a node that left
	// maintenance never left it. on and off are both sent; absent sends
	// nothing.
	// Mutation captured: sending off as nil (the pre-fix boolPtr
	// behavior) drops the field and the off body is {}.
	t.Run("node set manutencao off sends false", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "node", "set", "--maquina", "m1", "--manutencao", "off")
		if exit != 0 {
			t.Fatalf("jobs node set off exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.body != `{"manutencao":false}` {
			t.Errorf("node set off body = %q, want {\"manutencao\":false}", c.body)
		}
		assertWireHeaders(t, c, true)
	})
	// Regression: node set without --runners was refused as bad usage.
	// Mutation captured: requiring a non-empty runners value even when
	// --runners is absent makes this form exit 2.
	t.Run("node set without runners", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "node", "set", "--maquina", "m1", "--soho-version", "0.2.0")
		if exit != 0 {
			t.Fatalf("jobs node set exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.body != `{"soho_version":"0.2.0"}` {
			t.Errorf("node set body = %q", c.body)
		}
	})
	t.Run("node set with only runners", func(t *testing.T) {
		srv, ch, _ := captureServer(t, http.StatusOK, ok, false)
		dir := t.TempDir()
		stdout, _, exit := runCLI(t, dir, false, "jobs", "--server", srv.URL, "node", "set", "--maquina", "m1", "--runners", "a")
		if exit != 0 {
			t.Fatalf("jobs node set exit = %d (stdout %q)", exit, stdout)
		}
		c := requireCapture(t, ch)
		if c.body != `{"runners":["a"]}` {
			t.Errorf("node set body = %q, want only runners", c.body)
		}
		assertWireHeaders(t, c, true)
	})
}

// registryCheckRequest mirrors the registry CheckRequest wire shape for
// the wire-format assertions.
type registryCheckRequest struct {
	Agente    string `json:"agente"`
	Maquina   string `json:"maquina"`
	Card      string `json:"card"`
	Projeto   string `json:"projeto"`
	Branch    string `json:"branch"`
	PR        string `json:"pr"`
	Workspace string `json:"workspace"`
	TTLs      int    `json:"ttl_s"`
}

// assertWireHeaders checks the common client headers: User-Agent with the
// herdr-hermes prefix, content-type only on bodies, no Authorization.
func assertWireHeaders(t *testing.T, c wireCapture, withBody bool) {
	t.Helper()
	if !strings.HasPrefix(c.ua, "herdr-hermes/") {
		t.Errorf("User-Agent = %q, want the herdr-hermes prefix", c.ua)
	}
	if c.auth != "" {
		t.Errorf("Authorization = %q, want none (the client never sends credentials)", c.auth)
	}
	if withBody && c.ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", c.ct)
	}
	if !withBody && c.ct != "" {
		t.Errorf("Content-Type = %q, want none on a bodyless request", c.ct)
	}
}

// TestUsageListsRegistry: help lists serve and jobs, and the 44 exit code
// line.
func TestUsageListsRegistry(t *testing.T) {
	dir := t.TempDir()
	stdout, _, exit := runCLI(t, dir, false, "help")
	if exit != 0 {
		t.Fatalf("help exit = %d (stdout %q)", exit, stdout)
	}
	if !strings.Contains(stdout, "serve [--listen") {
		t.Errorf("help = %q, want the serve line", stdout)
	}
	if !strings.Contains(stdout, "jobs check --agente <a>") {
		t.Errorf("help = %q, want the jobs line", stdout)
	}
	if !strings.Contains(stdout, "44  the pre-dispatch check refused") {
		t.Errorf("help = %q, want the 44 exit code line", stdout)
	}
}
