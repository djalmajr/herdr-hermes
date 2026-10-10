package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// validRecordBody is one well-formed outbox record (docs/protocol.md
// "Outbox record"), with an unknown top-level field the ingest must
// ignore.
const validRecordBody = `{"schema":1,"seq":7,"ts":"2026-01-02T03:04:05-03:00","tipo":"session","maquina":"node-a","projeto":"org/repo","job_id":null,"idempotency_key":"node-a:7","dados":{"acao":"start","card":"dja-229"},"extra-unknown":"kept"}`

// fakeBackend is an in-memory Backend: every method records its input
// and answers from the configured fn, or succeeds with an ok result
// when none is set.
type fakeBackend struct {
	mu sync.Mutex

	ingestFn        func(rec outbox.Record) (bool, error)
	lastRec         outbox.Record
	gotIngest       bool
	invFn           func(f Filter) (Inventory, error)
	lastFilter      Filter
	gotInv          bool
	checkFn         func(req CheckRequest) (CheckResult, error)
	lastCheck       CheckRequest
	gotCheck        bool
	releaseFn       func(id string) error
	released        []string
	setPolicyFn     func(in PolicyInput) error
	lastPolicy      PolicyInput
	gotPolicy       bool
	clearPolicyFn   func(escopo, valor string) error
	lastClearPolicy [2]string
	gotClearPolicy  bool
	setFolderFn     func(in FolderInput) error
	lastFolder      FolderInput
	gotFolder       bool
	clearFolderFn   func(maquina, caminho string) error
	lastClearFolder [2]string
	gotClearFolder  bool
	setNodeFn       func(u NodeUpdate) error
	lastNode        NodeUpdate
	gotNode         bool
}

func (b *fakeBackend) Ingest(rec outbox.Record) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastRec = rec
	b.gotIngest = true
	if b.ingestFn != nil {
		return b.ingestFn(rec)
	}
	return true, nil
}

func (b *fakeBackend) Inventory(f Filter) (Inventory, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastFilter = f
	b.gotInv = true
	if b.invFn != nil {
		return b.invFn(f)
	}
	return Inventory{
		Gerado:    "2026-01-02T03:04:05-03:00",
		Nodes:     []Node{{Maquina: "node-a", Online: true}},
		Jobs:      []Job{},
		Esperas:   []Wait{},
		Claims:    []Claim{},
		Politicas: []Policy{},
		Pastas:    []Folder{},
	}, nil
}

func (b *fakeBackend) Check(req CheckRequest) (CheckResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastCheck = req
	b.gotCheck = true
	if b.checkFn != nil {
		return b.checkFn(req)
	}
	id := "0123456789abcdef0123456789abcdef"
	return CheckResult{Status: CheckOK, Claim: &Claim{ID: id, Agente: req.Agente}}, nil
}

func (b *fakeBackend) Release(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.released = append(b.released, id)
	if b.releaseFn != nil {
		return b.releaseFn(id)
	}
	return nil
}

func (b *fakeBackend) SetPolicy(in PolicyInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastPolicy = in
	b.gotPolicy = true
	if b.setPolicyFn != nil {
		return b.setPolicyFn(in)
	}
	return nil
}

func (b *fakeBackend) ClearPolicy(escopo, valor string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastClearPolicy = [2]string{escopo, valor}
	b.gotClearPolicy = true
	if b.clearPolicyFn != nil {
		return b.clearPolicyFn(escopo, valor)
	}
	return nil
}

func (b *fakeBackend) SetFolder(in FolderInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastFolder = in
	b.gotFolder = true
	if b.setFolderFn != nil {
		return b.setFolderFn(in)
	}
	return nil
}

func (b *fakeBackend) ClearFolder(maquina, caminho string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastClearFolder = [2]string{maquina, caminho}
	b.gotClearFolder = true
	if b.clearFolderFn != nil {
		return b.clearFolderFn(maquina, caminho)
	}
	return nil
}

func (b *fakeBackend) SetNode(u NodeUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastNode = u
	b.gotNode = true
	if b.setNodeFn != nil {
		return b.setNodeFn(u)
	}
	return nil
}

// setLogf replaces the package logger for the test duration and returns
// the lines it recorded.
func setLogf(t *testing.T) *[]string {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	orig := logf
	logf = func(format string, args ...any) {
		mu.Lock()
		lines = append(lines, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	t.Cleanup(func() { logf = orig })
	return &lines
}

// doReq runs one request against the handler and returns the recorder.
func doReq(t *testing.T, h http.Handler, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rdr)
	// The handler refuses a non-loopback authority; httptest's default
	// Host is example.com, so set the loopback one explicitly.
	req.Host = "127.0.0.1:7429"
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// jsonHeaders is the Content-Type header of a JSON request.
func jsonHeaders(extra map[string]string) map[string]string {
	h := map[string]string{"Content-Type": "application/json"}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

// wantJSONLine checks that body is exactly one JSON line (trailing
// newline) whose status and motivo match.
func wantJSONLine(t *testing.T, body, status, motivo string) {
	t.Helper()
	if !strings.HasSuffix(body, "\n") {
		t.Errorf("response %q does not end with a newline", body)
	}
	var e struct {
		Status string `json:"status"`
		Motivo string `json:"motivo"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(body, "\n")), &e); err != nil {
		t.Fatalf("response %q is not one JSON object: %v", body, err)
	}
	if e.Status != status {
		t.Errorf("response status = %q, want %q (body %s)", e.Status, status, body)
	}
	if e.Motivo != motivo {
		t.Errorf("response motivo = %q, want %q (body %s)", e.Motivo, motivo, body)
	}
}

// TestHandlerAuthority: before any routing, the handler refuses with a
// one-line 403 a request whose Host is not a loopback authority or that
// carries an Origin that is not an http(s) loopback origin; an absent
// Origin is fine; the backend receives no call on a refusal.
//
// Mutation captured: removing the authority gate lets the foreign Host
// and Origin cases reach the backend: they answer 200 and the backend's
// gotInv flag is set.
func TestHandlerAuthority(t *testing.T) {
	const forbidden = `{"status":"forbidden","motivo":"request authority is not loopback"}` + "\n"
	for _, tc := range []struct {
		name   string
		host   string
		origin string
		want   int
	}{
		{"loopback v4", "127.0.0.1:7429", "", http.StatusOK},
		{"loopback name", "localhost:7429", "", http.StatusOK},
		{"loopback v6", "[::1]:7429", "", http.StatusOK},
		{"uppercase name bare", "LOCALHOST", "", http.StatusOK},
		{"foreign name", "example.com", "", http.StatusForbidden},
		{"foreign name with port", "example.com:7429", "", http.StatusForbidden},
		{"foreign ip", "192.0.2.1:7429", "", http.StatusForbidden},
		{"non-loopback ip", "0.0.0.0:7429", "", http.StatusForbidden},
		{"empty host", "", "", http.StatusForbidden},
		{"loopback origin", "127.0.0.1:7429", "http://127.0.0.1:7429", http.StatusOK},
		{"loopback https origin", "localhost:7429", "https://localhost", http.StatusOK},
		{"absent origin", "127.0.0.1:7429", "", http.StatusOK},
		{"foreign origin", "127.0.0.1:7429", "http://example.com", http.StatusForbidden},
		{"null origin", "127.0.0.1:7429", "null", http.StatusForbidden},
		{"file origin", "127.0.0.1:7429", "file://", http.StatusForbidden},
		{"origin with path", "127.0.0.1:7429", "http://127.0.0.1:7429/x", http.StatusForbidden},
		{"origin with userinfo", "127.0.0.1:7429", "http://user@127.0.0.1:7429", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &fakeBackend{}
			h := NewHandler(b)
			req := httptest.NewRequest("GET", "/v1/jobs", nil)
			req.Host = tc.host
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("Host %q origin %q status = %d, want %d (body %q)", tc.host, tc.origin, w.Code, tc.want, w.Body.String())
			}
			if tc.want == http.StatusForbidden {
				if w.Body.String() != forbidden {
					t.Errorf("Host %q origin %q body = %q, want %q", tc.host, tc.origin, w.Body.String(), forbidden)
				}
				if b.gotInv {
					t.Errorf("Host %q origin %q: the backend was called, want no call", tc.host, tc.origin)
				}
				return
			}
			if !b.gotInv {
				t.Errorf("Host %q origin %q: the backend was not called", tc.host, tc.origin)
			}
		})
	}
}

// TestHandler: every route with its success code and body; 404 JSON for
// an unknown path; 405 JSON with Allow for a wrong method; 413 for an
// oversize body and 415 for a missing or wrong content type; 400 for
// unknown JSON fields, trailing data, a mismatched Idempotency-Key and
// an unknown query parameter; 409 for duplicate and hold check results;
// and the ErrInvalid, ErrNotFound and other-error mapping, the 500 body
// never carrying the Go error text.
// Mutation captured: answering 200 for a hold check result makes the
// hold case fail.
func TestHandler(t *testing.T) {
	lines := setLogf(t)
	b := &fakeBackend{}
	h := NewHandler(b)

	t.Run("health", func(t *testing.T) {
		w := doReq(t, h, "GET", "/v1/health", "", nil)
		if w.Code != 200 {
			t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
		}
		if w.Body.String() != `{"status":"ok","schema":1}`+"\n" {
			t.Errorf("body = %q", w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		w = doReq(t, h, "POST", "/v1/health", "", nil)
		if w.Code != 405 {
			t.Fatalf("POST health code = %d, want 405", w.Code)
		}
		if allow := w.Header().Get("Allow"); allow != "GET" {
			t.Errorf("Allow = %q, want GET", allow)
		}
		wantJSONLine(t, w.Body.String(), "method_not_allowed", "method not allowed")
	})

	t.Run("jobs", func(t *testing.T) {
		w := doReq(t, h, "GET", "/v1/jobs", "", nil)
		if w.Code != 200 {
			t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
		}
		var inv Inventory
		if err := json.Unmarshal(w.Body.Bytes(), &inv); err != nil {
			t.Fatalf("body is not the inventory: %v", err)
		}
		if inv.Gerado == "" {
			t.Errorf("gerado = empty, want the backend value")
		}
		if !b.gotInv || b.lastFilter != (Filter{}) {
			t.Errorf("filter = %+v, want zero", b.lastFilter)
		}

		b.invFn = func(f Filter) (Inventory, error) {
			return Inventory{}, fmt.Errorf("%w: maquina", ErrInvalid)
		}
		w = doReq(t, h, "GET", "/v1/jobs?maquina=node-a&agente=build", "", nil)
		b.invFn = nil
		if w.Code != 400 {
			t.Errorf("filtered code = %d, want 400 (the backend validates the filter)", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "maquina")
		if b.lastFilter != (Filter{Maquina: "node-a", Agente: "build"}) {
			t.Errorf("filter = %+v, want node-a/build", b.lastFilter)
		}

		w = doReq(t, h, "GET", "/v1/jobs?foo=1", "", nil)
		if w.Code != 400 {
			t.Fatalf("unknown query parameter code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "unknown query parameter")

		w = doReq(t, h, "POST", "/v1/jobs", "", nil)
		if w.Code != 405 || w.Header().Get("Allow") != "GET" {
			t.Errorf("POST jobs = %d, Allow %q; want 405 GET", w.Code, w.Header().Get("Allow"))
		}
	})

	t.Run("check", func(t *testing.T) {
		body := `{"agente":"build","card":"dja-229","ttl_s":3600}`
		w := doReq(t, h, "POST", "/v1/check", body, jsonHeaders(nil))
		if w.Code != 200 {
			t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
		}
		var res CheckResult
		if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
			t.Fatalf("body is not a check result: %v", err)
		}
		if res.Status != CheckOK || res.Claim == nil || res.Claim.ID == "" {
			t.Errorf("result = %+v, want ok with a claim", res)
		}
		if !b.gotCheck || b.lastCheck.Agente != "build" || b.lastCheck.Card != "dja-229" || b.lastCheck.TTLS != 3600 {
			t.Errorf("request = %+v, want the decoded body", b.lastCheck)
		}

		for _, tc := range []struct {
			name   string
			status string
		}{{"duplicate", CheckDuplicate}, {"hold", CheckHold}} {
			t.Run(tc.name, func(t *testing.T) {
				b.checkFn = func(req CheckRequest) (CheckResult, error) {
					return CheckResult{Status: tc.status, Conflitos: []Conflict{{Tipo: ConflitoJob, Campo: CampoCard, Valor: "dja-229"}}}, nil
				}
				w := doReq(t, h, "POST", "/v1/check", body, jsonHeaders(nil))
				b.checkFn = nil
				if w.Code != 409 {
					t.Fatalf("%s code = %d, want 409", tc.name, w.Code)
				}
				var res2 CheckResult
				if err := json.Unmarshal(w.Body.Bytes(), &res2); err != nil {
					t.Fatalf("body is not a check result: %v", err)
				}
				if res2.Status != tc.status || len(res2.Conflitos) != 1 {
					t.Errorf("result = %+v, want %s with the conflicts", res2, tc.status)
				}
			})
		}

		// Error mapping: the 500 body never carries the Go error text.
		b.checkFn = func(req CheckRequest) (CheckResult, error) {
			return CheckResult{}, errors.New("boom sqlite detail")
		}
		w = doReq(t, h, "POST", "/v1/check", `{"agente":"build-500"}`, jsonHeaders(nil))
		b.checkFn = nil
		if w.Code != 500 {
			t.Fatalf("other error code = %d, want 500", w.Code)
		}
		if w.Body.String() != `{"status":"error","motivo":"internal error"}`+"\n" {
			t.Errorf("body = %q", w.Body.String())
		}
		if strings.Contains(w.Body.String(), "boom") {
			t.Errorf("500 body leaks the Go error text")
		}
		if len(*lines) == 0 {
			t.Errorf("no log line for a 500")
		}
		for _, l := range *lines {
			if strings.Contains(l, "build-500") {
				t.Errorf("log line leaks the request body: %q", l)
			}
			if !strings.Contains(l, "boom sqlite detail") {
				t.Errorf("log line misses the error: %q", l)
			}
		}

		b.checkFn = func(req CheckRequest) (CheckResult, error) {
			return CheckResult{}, fmt.Errorf("%w: agente is required", ErrInvalid)
		}
		w = doReq(t, h, "POST", "/v1/check", body, jsonHeaders(nil))
		b.checkFn = nil
		if w.Code != 400 {
			t.Fatalf("ErrInvalid code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "agente is required")

		b.checkFn = func(req CheckRequest) (CheckResult, error) {
			return CheckResult{}, ErrNotFound
		}
		w = doReq(t, h, "POST", "/v1/check", body, jsonHeaders(nil))
		b.checkFn = nil
		if w.Code != 404 {
			t.Fatalf("ErrNotFound code = %d, want 404", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "not_found", "not found")
	})

	t.Run("claims", func(t *testing.T) {
		id := "0123456789abcdef0123456789abcdef"
		w := doReq(t, h, "DELETE", "/v1/claims/"+id, "", nil)
		if w.Code != 200 || w.Body.String() != `{"status":"ok"}`+"\n" {
			t.Fatalf("DELETE claims = %d %q, want 200 ok", w.Code, w.Body.String())
		}
		if len(b.released) != 1 || b.released[0] != id {
			t.Errorf("released = %v, want %q", b.released, id)
		}

		b.releaseFn = func(id string) error { return ErrNotFound }
		w = doReq(t, h, "DELETE", "/v1/claims/"+id, "", nil)
		b.releaseFn = nil
		if w.Code != 404 {
			t.Fatalf("unknown claim code = %d, want 404", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "not_found", "not found")

		b.releaseFn = func(id string) error {
			return fmt.Errorf("%w: claim id is not 32 hex", ErrInvalid)
		}
		w = doReq(t, h, "DELETE", "/v1/claims/"+id, "", nil)
		b.releaseFn = nil
		if w.Code != 400 {
			t.Fatalf("invalid id code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "claim id is not 32 hex")

		w = doReq(t, h, "GET", "/v1/claims/"+id, "", nil)
		if w.Code != 405 || w.Header().Get("Allow") != "DELETE" {
			t.Errorf("GET claims = %d, Allow %q; want 405 DELETE", w.Code, w.Header().Get("Allow"))
		}
		for _, target := range []string{"/v1/claims", "/v1/claims/", "/v1/claims/a/b"} {
			w := doReq(t, h, "DELETE", target, "", nil)
			if w.Code != 404 {
				t.Errorf("DELETE %s = %d, want 404", target, w.Code)
			}
		}
	})

	t.Run("policies", func(t *testing.T) {
		body := `{"escopo":"card","valor":"dja-229","hold":true,"motivo":"manual"}`
		w := doReq(t, h, "PUT", "/v1/policies", body, jsonHeaders(nil))
		if w.Code != 200 || w.Body.String() != `{"status":"ok"}`+"\n" {
			t.Fatalf("PUT policies = %d %q, want 200 ok", w.Code, w.Body.String())
		}
		if !b.gotPolicy || b.lastPolicy.Escopo != "card" || b.lastPolicy.Valor != "dja-229" || !b.lastPolicy.Hold {
			t.Errorf("policy = %+v, want the decoded body", b.lastPolicy)
		}

		w = doReq(t, h, "PUT", "/v1/policies", `{"escopo":"card","valor":"dja-229","bogus":1}`, jsonHeaders(nil))
		if w.Code != 400 {
			t.Fatalf("unknown field code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "invalid json")

		b.setPolicyFn = func(in PolicyInput) error {
			return fmt.Errorf("%w: valor", ErrInvalid)
		}
		w = doReq(t, h, "PUT", "/v1/policies", body, jsonHeaders(nil))
		b.setPolicyFn = nil
		if w.Code != 400 {
			t.Fatalf("ErrInvalid code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "valor")

		w = doReq(t, h, "DELETE", "/v1/policies?escopo=card&valor=DJA-229", "", nil)
		if w.Code != 200 {
			t.Fatalf("DELETE policies = %d, want 200", w.Code)
		}
		if b.lastClearPolicy != [2]string{"card", "DJA-229"} {
			t.Errorf("cleared = %v, want card/DJA-229", b.lastClearPolicy)
		}

		b.clearPolicyFn = func(escopo, valor string) error { return ErrNotFound }
		w = doReq(t, h, "DELETE", "/v1/policies?escopo=card&valor=dja-229", "", nil)
		b.clearPolicyFn = nil
		if w.Code != 404 {
			t.Fatalf("unknown policy code = %d, want 404", w.Code)
		}

		w = doReq(t, h, "POST", "/v1/policies", "", nil)
		if w.Code != 405 || w.Header().Get("Allow") != "PUT, DELETE" {
			t.Errorf("POST policies = %d, Allow %q; want 405 PUT, DELETE", w.Code, w.Header().Get("Allow"))
		}
	})

	t.Run("folders", func(t *testing.T) {
		body := `{"maquina":"node-a","caminho":"/tmp/x","pedido":"delete me"}`
		w := doReq(t, h, "PUT", "/v1/folders", body, jsonHeaders(nil))
		if w.Code != 200 || w.Body.String() != `{"status":"ok"}`+"\n" {
			t.Fatalf("PUT folders = %d %q, want 200 ok", w.Code, w.Body.String())
		}
		if !b.gotFolder || b.lastFolder.Maquina != "node-a" || b.lastFolder.Caminho != "/tmp/x" {
			t.Errorf("folder = %+v, want the decoded body", b.lastFolder)
		}

		w = doReq(t, h, "PUT", "/v1/folders", `{"maquina":"node-a","caminho":"/tmp/x","pedido":"x","bogus":1}`, jsonHeaders(nil))
		if w.Code != 400 {
			t.Fatalf("unknown field code = %d, want 400", w.Code)
		}

		b.setFolderFn = func(in FolderInput) error {
			return fmt.Errorf("%w: caminho", ErrInvalid)
		}
		w = doReq(t, h, "PUT", "/v1/folders", body, jsonHeaders(nil))
		b.setFolderFn = nil
		if w.Code != 400 {
			t.Fatalf("ErrInvalid code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "caminho")

		w = doReq(t, h, "DELETE", "/v1/folders?maquina=node-a&caminho=%2Ftmp%2Fx", "", nil)
		if w.Code != 200 {
			t.Fatalf("DELETE folders = %d, want 200", w.Code)
		}
		if b.lastClearFolder != [2]string{"node-a", "/tmp/x"} {
			t.Errorf("cleared = %v, want node-a//tmp/x", b.lastClearFolder)
		}

		b.clearFolderFn = func(maquina, caminho string) error { return ErrNotFound }
		w = doReq(t, h, "DELETE", "/v1/folders?maquina=node-a&caminho=%2Ftmp%2Fx", "", nil)
		b.clearFolderFn = nil
		if w.Code != 404 {
			t.Fatalf("unknown folder code = %d, want 404", w.Code)
		}

		w = doReq(t, h, "GET", "/v1/folders", "", nil)
		if w.Code != 405 || w.Header().Get("Allow") != "PUT, DELETE" {
			t.Errorf("GET folders = %d, Allow %q; want 405 PUT, DELETE", w.Code, w.Header().Get("Allow"))
		}
	})

	t.Run("nodes", func(t *testing.T) {
		body := `{"manutencao":true,"soho_version":"0.1.0"}`
		w := doReq(t, h, "PUT", "/v1/nodes/node-a", body, jsonHeaders(nil))
		if w.Code != 200 || w.Body.String() != `{"status":"ok"}`+"\n" {
			t.Fatalf("PUT nodes = %d %q, want 200 ok", w.Code, w.Body.String())
		}
		if !b.gotNode || b.lastNode.Maquina != "node-a" {
			t.Errorf("node = %+v, want maquina from the path", b.lastNode)
		}
		if b.lastNode.Manutencao == nil || !*b.lastNode.Manutencao {
			t.Errorf("manutencao = %v, want true", b.lastNode.Manutencao)
		}
		if b.lastNode.SohoVersion == nil || *b.lastNode.SohoVersion != "0.1.0" {
			t.Errorf("soho_version = %v, want 0.1.0", b.lastNode.SohoVersion)
		}

		// The maquina comes from the path; in the body it is unknown.
		w = doReq(t, h, "PUT", "/v1/nodes/node-a", `{"maquina":"node-b"}`, jsonHeaders(nil))
		if w.Code != 400 {
			t.Fatalf("body maquina code = %d, want 400", w.Code)
		}

		w = doReq(t, h, "PUT", "/v1/nodes/node-a", `{"bogus":1}`, jsonHeaders(nil))
		if w.Code != 400 {
			t.Fatalf("unknown field code = %d, want 400", w.Code)
		}

		b.setNodeFn = func(u NodeUpdate) error {
			return fmt.Errorf("%w: runners", ErrInvalid)
		}
		w = doReq(t, h, "PUT", "/v1/nodes/node-a", body, jsonHeaders(nil))
		b.setNodeFn = nil
		if w.Code != 400 {
			t.Fatalf("ErrInvalid code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "runners")

		w = doReq(t, h, "DELETE", "/v1/nodes/node-a", "", nil)
		if w.Code != 405 || w.Header().Get("Allow") != "PUT" {
			t.Errorf("DELETE nodes = %d, Allow %q; want 405 PUT", w.Code, w.Header().Get("Allow"))
		}
		for _, target := range []string{"/v1/nodes", "/v1/nodes/", "/v1/nodes/a/b"} {
			w := doReq(t, h, "PUT", target, body, jsonHeaders(nil))
			if w.Code != 404 {
				t.Errorf("PUT %s = %d, want 404", target, w.Code)
			}
		}
	})

	t.Run("unknown paths", func(t *testing.T) {
		for _, tc := range []struct{ method, target string }{
			{"GET", "/"},
			{"GET", "/nope"},
			{"GET", "/v1/whatever"},
			{"POST", "/v1/ingestx"},
			{"DELETE", "/v1/claims/"},
		} {
			w := doReq(t, h, tc.method, tc.target, "", nil)
			if w.Code != 404 {
				t.Errorf("%s %s = %d, want 404", tc.method, tc.target, w.Code)
			}
			wantJSONLine(t, w.Body.String(), "not_found", "not found")
		}
	})

	t.Run("ingest bodies", func(t *testing.T) {
		// Unknown top-level fields are ignored.
		w := doReq(t, h, "POST", "/v1/ingest", validRecordBody, jsonHeaders(map[string]string{"Idempotency-Key": "node-a:7"}))
		if w.Code != 200 {
			t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
		}
		if w.Body.String() != `{"status":"ok","aplicado":true}`+"\n" {
			t.Errorf("body = %q", w.Body.String())
		}

		// Trailing data and invalid JSON are 400.
		w = doReq(t, h, "POST", "/v1/ingest", validRecordBody+" x", jsonHeaders(nil))
		if w.Code != 400 {
			t.Fatalf("trailing data code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "invalid json")
		w = doReq(t, h, "POST", "/v1/ingest", `{`, jsonHeaders(nil))
		if w.Code != 400 {
			t.Fatalf("invalid json code = %d, want 400", w.Code)
		}

		// A mismatched Idempotency-Key is 400.
		w = doReq(t, h, "POST", "/v1/ingest", validRecordBody, jsonHeaders(map[string]string{"Idempotency-Key": "node-a:8"}))
		if w.Code != 400 {
			t.Fatalf("mismatched key code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "idempotency key mismatch")

		// Content type: missing or wrong is 415; parameters are ok.
		w = doReq(t, h, "POST", "/v1/ingest", validRecordBody, nil)
		if w.Code != 415 {
			t.Fatalf("missing content type code = %d, want 415", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "unsupported_media_type", "content type must be application/json")
		w = doReq(t, h, "POST", "/v1/ingest", validRecordBody, map[string]string{"Content-Type": "text/plain"})
		if w.Code != 415 {
			t.Fatalf("wrong content type code = %d, want 415", w.Code)
		}
		w = doReq(t, h, "POST", "/v1/ingest", validRecordBody, map[string]string{"Content-Type": "application/json; charset=utf-8"})
		if w.Code != 200 {
			t.Fatalf("content type with parameters code = %d, want 200", w.Code)
		}

		// Oversize is 413, before the content-type check.
		oversize := strings.Repeat("a", 1<<20+1)
		w = doReq(t, h, "POST", "/v1/ingest", oversize, map[string]string{"Content-Type": "text/plain"})
		if w.Code != 413 {
			t.Fatalf("oversize ingest code = %d, want 413", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "too_large", "body too large")
		w = doReq(t, h, "POST", "/v1/ingest", strings.Repeat("a", 1<<20), jsonHeaders(nil))
		if w.Code == 413 {
			t.Errorf("a 1 MiB body is at the cap, not over it (got 413)")
		}

		// The other JSON bodies cap at 64 KiB.
		w = doReq(t, h, "POST", "/v1/check", strings.Repeat("a", 64<<10+1), jsonHeaders(nil))
		if w.Code != 413 {
			t.Fatalf("oversize check code = %d, want 413", w.Code)
		}
		w = doReq(t, h, "POST", "/v1/check", strings.Repeat("a", 64<<10), jsonHeaders(nil))
		if w.Code != 400 {
			t.Fatalf("a 64 KiB body is at the cap (invalid JSON), not 413 (got %d)", w.Code)
		}
	})

	t.Run("ingest error mapping", func(t *testing.T) {
		b.ingestFn = func(rec outbox.Record) (bool, error) {
			return false, fmt.Errorf("%w: tipo", ErrInvalid)
		}
		w := doReq(t, h, "POST", "/v1/ingest", validRecordBody, jsonHeaders(nil))
		b.ingestFn = nil
		if w.Code != 400 {
			t.Fatalf("ErrInvalid code = %d, want 400", w.Code)
		}
		wantJSONLine(t, w.Body.String(), "invalid", "tipo")
	})
}

// TestHandlerIngest: the ingest decodes the record unchanged to the
// backend, never exposes the Authorization header in the response or
// the logger, and echoes the applied flag.
// Mutation captured: echoing the Authorization header into the response
// body or the log line makes the canary assertion fail.
func TestHandlerIngest(t *testing.T) {
	lines := setLogf(t)
	b := &fakeBackend{}
	h := NewHandler(b)

	w := doReq(t, h, "POST", "/v1/ingest", validRecordBody, jsonHeaders(map[string]string{
		"Authorization":   "Bearer canary-…",
		"Idempotency-Key": "node-a:7",
	}))
	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	out := w.Body.String()
	if out != `{"status":"ok","aplicado":true}`+"\n" {
		t.Errorf("body = %q", out)
	}
	if strings.Contains(out, "canary-") {
		t.Errorf("response leaks the Authorization header: %s", out)
	}
	for _, l := range *lines {
		if strings.Contains(l, "canary-") {
			t.Errorf("logger received the Authorization header: %q", l)
		}
	}

	// The backend got the decoded record unchanged.
	if !b.gotIngest {
		t.Fatal("the backend never saw the ingest")
	}
	rec := b.lastRec
	if rec.Schema != 1 || rec.Seq != 7 || rec.TS != "2026-01-02T03:04:05-03:00" ||
		rec.Tipo != "session" || rec.Maquina != "node-a" || rec.Projeto != "org/repo" ||
		rec.IdempotencyKey != "node-a:7" {
		t.Errorf("record = %+v", rec)
	}
	if rec.JobID != nil {
		t.Errorf("job_id = %q, want null", *rec.JobID)
	}
	if string(rec.Dados) != `{"acao":"start","card":"dja-229"}` {
		t.Errorf("dados = %s, want the raw object unchanged", rec.Dados)
	}

	// applied=false is echoed.
	b.ingestFn = func(rec outbox.Record) (bool, error) { return false, nil }
	w = doReq(t, h, "POST", "/v1/ingest", validRecordBody, jsonHeaders(nil))
	b.ingestFn = nil
	if w.Code != 200 || w.Body.String() != `{"status":"ok","aplicado":false}`+"\n" {
		t.Errorf("applied=false = %d %q, want 200 ok/false", w.Code, w.Body.String())
	}
}

// TestCheckLoopback: loopback hosts (localhost or a loopback IP
// literal) with a port in 0..65535 are accepted; everything else is an
// ErrInvalid wrap.
// Mutation captured: accepting any host (dropping the loopback check)
// makes the 192.168.1.10 case pass.
func TestCheckLoopback(t *testing.T) {
	ok := []string{
		"127.0.0.1:7429",
		"[::1]:7429",
		"localhost:0",
		"127.0.0.1:0",
		"localhost:65535",
		"127.0.0.1:65535",
		"[::1]:1",
	}
	for _, addr := range ok {
		t.Run("ok "+addr, func(t *testing.T) {
			if err := CheckLoopback(addr); err != nil {
				t.Errorf("CheckLoopback(%q) = %v, want nil", addr, err)
			}
		})
	}
	bad := []string{
		":7429",
		"0.0.0.0:7429",
		"[::]:1",
		"192.168.1.10:80",
		"example.com:80",
		"127.0.0.1",
		"127.0.0.1:99999",
		"localhost:notaport",
		"[::1]:-1",
	}
	for _, addr := range bad {
		t.Run("bad "+addr, func(t *testing.T) {
			err := CheckLoopback(addr)
			if err == nil {
				t.Fatalf("CheckLoopback(%q) = nil, want error", addr)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Errorf("CheckLoopback(%q) = %v, want an ErrInvalid wrap", addr, err)
			}
		})
	}
}

// TestHandlerEndToEnd: a real loopback HTTP server answers a POST
// ingest and a GET jobs through a plain http.Client with a timeout.
// Mutation captured: answering every request 200 ok (ignoring the
// route) makes the jobs assertion fail.
func TestHandlerEndToEnd(t *testing.T) {
	setLogf(t)
	b := &fakeBackend{}
	srv := httptest.NewServer(NewHandler(b))
	t.Cleanup(srv.Close)
	client := &http.Client{Timeout: 2 * time.Second}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/v1/ingest", strings.NewReader(validRecordBody))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "node-a:7")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read ingest: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("ingest status = %d, body = %s", resp.StatusCode, data)
	}
	if string(data) != `{"status":"ok","aplicado":true}`+"\n" {
		t.Errorf("ingest body = %q", data)
	}
	if !b.gotIngest || b.lastRec.Seq != 7 {
		t.Errorf("backend record = %+v, want the pushed record", b.lastRec)
	}

	resp2, err := client.Get(srv.URL + "/v1/jobs?maquina=node-a")
	if err != nil {
		t.Fatalf("jobs: %v", err)
	}
	data2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if err != nil {
		t.Fatalf("read jobs: %v", err)
	}
	if resp2.StatusCode != 200 {
		t.Fatalf("jobs status = %d, body = %s", resp2.StatusCode, data2)
	}
	var inv Inventory
	if err := json.Unmarshal(data2, &inv); err != nil {
		t.Fatalf("jobs body is not the inventory: %v", err)
	}
	if inv.Gerado == "" || len(inv.Nodes) != 1 || inv.Nodes[0].Maquina != "node-a" {
		t.Errorf("inventory = %+v, want the filtered backend state", inv)
	}
}
