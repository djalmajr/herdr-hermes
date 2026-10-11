package registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// logf is the handler's logger: one line per internal error, never a
// request body or header. serve wires a real logger; the default
// discards.
var logf = func(string, ...any) {}

// SetLogger replaces the handler's logger. Call it before the handler
// serves its first request.
func SetLogger(f func(format string, args ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	logf = f
}

// Request body caps.
const (
	// ingestBodyCap is the maximum ingest body: one outbox record.
	ingestBodyCap = 1 << 20 // 1 MiB
	// apiBodyCap is the maximum body of the other JSON routes.
	apiBodyCap = 64 << 10 // 64 KiB
)

// Backend is the store surface the HTTP API calls: everything the
// registry store does, nothing the store does not.
type Backend interface {
	Ingest(rec outbox.Record) (bool, error)
	Inventory(f Filter) (Inventory, error)
	Check(req CheckRequest) (CheckResult, error)
	Release(id string) error
	SetPolicy(in PolicyInput) error
	ClearPolicy(escopo, valor string) error
	SetFolder(in FolderInput) error
	ClearFolder(maquina, caminho string) error
	SetNode(u NodeUpdate) error
}

// errorBody is the one-line error answer of every route.
type errorBody struct {
	Status string `json:"status"`
	Motivo string `json:"motivo"`
}

// okBody is the one-line success answer of the writing routes.
type okBody struct {
	Status string `json:"status"`
}

// NewHandler returns the read/write API over b. Every answer is one
// JSON line; errors are {"status":"<code>","motivo":"<text>"}, with
// forbidden (403, a request whose Host or Origin authority is not
// loopback, refused before routing), invalid (400), not_found (404,
// also an unknown path), method_not_allowed (405, with an Allow
// header), too_large (413), unsupported_media_type (415) and error
// (500, never the Go error text).
func NewHandler(b Backend) http.Handler {
	return &apiHandler{b: b}
}

// apiHandler routes by path and checks the method, so the 404 and 405
// answers stay JSON.
type apiHandler struct {
	b Backend
}

// ServeHTTP implements http.Handler. It refuses the request before any
// routing when its authority is not loopback, and routes by path with
// the method check, so the 404 and 405 answers stay JSON.
func (h *apiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !loopbackRequest(r) {
		writeError(w, http.StatusForbidden, "forbidden", "request authority is not loopback")
		return
	}
	path := r.URL.Path
	switch {
	case path == "/v1/health":
		h.route(w, r, "GET", h.handleHealth)
	case path == "/v1/ingest":
		h.route(w, r, "POST", h.handleIngest)
	case path == "/v1/jobs":
		h.route(w, r, "GET", h.handleJobs)
	case path == "/v1/check":
		h.route(w, r, "POST", h.handleCheck)
	case path == "/v1/policies":
		h.route(w, r, "PUT, DELETE", h.handlePolicies)
	case path == "/v1/folders":
		h.route(w, r, "PUT, DELETE", h.handleFolders)
	case strings.HasPrefix(path, "/v1/claims/"):
		id := strings.TrimPrefix(path, "/v1/claims/")
		if id == "" || strings.Contains(id, "/") {
			writeError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		h.route(w, r, "DELETE", func(w http.ResponseWriter, r *http.Request) {
			h.handleClaim(w, r, id)
		})
	case strings.HasPrefix(path, "/v1/nodes/"):
		maquina := strings.TrimPrefix(path, "/v1/nodes/")
		if maquina == "" || strings.Contains(maquina, "/") {
			writeError(w, http.StatusNotFound, "not_found", "not found")
			return
		}
		h.route(w, r, "PUT", func(w http.ResponseWriter, r *http.Request) {
			h.handleNode(w, r, maquina)
		})
	default:
		writeError(w, http.StatusNotFound, "not_found", "not found")
	}
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

// route runs fn when r.Method is among the allowed methods and
// answers 405 with the Allow header otherwise.
func (h *apiHandler) route(w http.ResponseWriter, r *http.Request, allow string, fn func(http.ResponseWriter, *http.Request)) {
	if !methodAllowed(r.Method, allow) {
		methodNotAllowed(w, allow)
		return
	}
	fn(w, r)
}

// methodAllowed reports whether method is one of the allow list.
func methodAllowed(method, allow string) bool {
	for _, m := range strings.Split(allow, ",") {
		if method == strings.TrimSpace(m) {
			return true
		}
	}
	return false
}

// handleHealth answers GET /v1/health.
func (h *apiHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, struct {
		Status string `json:"status"`
		Schema int    `json:"schema"`
	}{"ok", SchemaVersion})
}

// handleIngest answers POST /v1/ingest: one outbox record, at most
// ingestBodyCap bytes, content type application/json. The
// Authorization header is not checked, stored, logged or echoed; the
// Idempotency-Key header, when present, must equal the record's
// idempotency key.
func (h *apiHandler) handleIngest(w http.ResponseWriter, r *http.Request) {
	body, ok := readJSONBody(w, r, ingestBodyCap)
	if !ok {
		return
	}
	var rec outbox.Record
	if err := decodeJSON(body, &rec, false); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json")
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" && key != rec.IdempotencyKey {
		writeError(w, http.StatusBadRequest, "invalid", "idempotency key mismatch")
		return
	}
	applied, err := h.b.Ingest(rec)
	if err != nil {
		h.fail(w, r.URL.Path, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Status   string `json:"status"`
		Aplicado bool   `json:"aplicado"`
	}{"ok", applied})
}

// handleJobs answers GET /v1/jobs with the inventory, filtered by the
// maquina and agente query parameters only.
func (h *apiHandler) handleJobs(w http.ResponseWriter, r *http.Request) {
	var f Filter
	for key := range r.URL.Query() {
		switch key {
		case "maquina":
			f.Maquina = r.URL.Query().Get("maquina")
		case "agente":
			f.Agente = r.URL.Query().Get("agente")
		default:
			writeError(w, http.StatusBadRequest, "invalid", "unknown query parameter")
			return
		}
	}
	inv, err := h.b.Inventory(f)
	if err != nil {
		h.fail(w, r.URL.Path, err)
		return
	}
	writeJSON(w, http.StatusOK, inv)
}

// handleCheck answers POST /v1/check: 200 when the check is ok, 409
// when it is a duplicate or a hold, with the same result body.
func (h *apiHandler) handleCheck(w http.ResponseWriter, r *http.Request) {
	body, ok := readJSONBody(w, r, apiBodyCap)
	if !ok {
		return
	}
	var req CheckRequest
	if err := decodeJSON(body, &req, true); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json")
		return
	}
	res, err := h.b.Check(req)
	if err != nil {
		h.fail(w, r.URL.Path, err)
		return
	}
	switch res.Status {
	case CheckOK:
		writeJSON(w, http.StatusOK, res)
	case CheckDuplicate, CheckHold:
		writeJSON(w, http.StatusConflict, res)
	default:
		logf("registry: %s: check status %q", r.URL.Path, res.Status)
		writeError(w, http.StatusInternalServerError, "error", "internal error")
	}
}

// handleClaim answers DELETE /v1/claims/{id}.
func (h *apiHandler) handleClaim(w http.ResponseWriter, r *http.Request, id string) {
	if err := h.b.Release(id); err != nil {
		h.fail(w, r.URL.Path, err)
		return
	}
	writeJSON(w, http.StatusOK, okBody{"ok"})
}

// handlePolicies answers PUT /v1/policies (a policy body) and DELETE
// /v1/policies (the escopo and valor query parameters).
func (h *apiHandler) handlePolicies(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut {
		body, ok := readJSONBody(w, r, apiBodyCap)
		if !ok {
			return
		}
		var in PolicyInput
		if err := decodeJSON(body, &in, true); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", "invalid json")
			return
		}
		if err := h.b.SetPolicy(in); err != nil {
			h.fail(w, r.URL.Path, err)
			return
		}
		writeJSON(w, http.StatusOK, okBody{"ok"})
		return
	}
	q := r.URL.Query()
	if err := h.b.ClearPolicy(q.Get("escopo"), q.Get("valor")); err != nil {
		h.fail(w, r.URL.Path, err)
		return
	}
	writeJSON(w, http.StatusOK, okBody{"ok"})
}

// handleFolders answers PUT /v1/folders (a folder body) and DELETE
// /v1/folders (the maquina and caminho query parameters).
func (h *apiHandler) handleFolders(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPut {
		body, ok := readJSONBody(w, r, apiBodyCap)
		if !ok {
			return
		}
		var in FolderInput
		if err := decodeJSON(body, &in, true); err != nil {
			writeError(w, http.StatusBadRequest, "invalid", "invalid json")
			return
		}
		if err := h.b.SetFolder(in); err != nil {
			h.fail(w, r.URL.Path, err)
			return
		}
		writeJSON(w, http.StatusOK, okBody{"ok"})
		return
	}
	q := r.URL.Query()
	if err := h.b.ClearFolder(q.Get("maquina"), q.Get("caminho")); err != nil {
		h.fail(w, r.URL.Path, err)
		return
	}
	writeJSON(w, http.StatusOK, okBody{"ok"})
}

// handleNode answers PUT /v1/nodes/{maquina}: the maquina comes from
// the path, so the body must not carry one (it is an unknown field).
func (h *apiHandler) handleNode(w http.ResponseWriter, r *http.Request, maquina string) {
	body, ok := readJSONBody(w, r, apiBodyCap)
	if !ok {
		return
	}
	var u NodeUpdate
	if err := decodeJSON(body, &u, true); err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json")
		return
	}
	u.Maquina = maquina
	if err := h.b.SetNode(u); err != nil {
		h.fail(w, r.URL.Path, err)
		return
	}
	writeJSON(w, http.StatusOK, okBody{"ok"})
}

// fail maps a backend error to its answer: ErrInvalid is 400 with the
// text after the prefix, ErrNotFound is 404, anything else is 500
// with a generic motivo and one line on the logger.
func (h *apiHandler) fail(w http.ResponseWriter, path string, err error) {
	switch {
	case errors.Is(err, ErrInvalid):
		writeError(w, http.StatusBadRequest, "invalid", strings.TrimPrefix(err.Error(), ErrInvalid.Error()+": "))
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "not found")
	default:
		logf("registry: %s: %v", path, err)
		writeError(w, http.StatusInternalServerError, "error", "internal error")
	}
}

// CheckLoopback reports whether addr may be bound by the API server:
// the host must be localhost or a loopback IP literal and the port
// 0..65535. Everything else is an ErrInvalid wrap.
func CheckLoopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%w: malformed address", ErrInvalid)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 0 || p > 65535 {
		return fmt.Errorf("%w: port out of range", ErrInvalid)
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("%w: host is not loopback", ErrInvalid)
		}
	}
	return nil
}

// readJSONBody reads the request body up to cap+1 bytes and reports it,
// or a written 413 (oversize) / 415 (content type) and false. The size
// is checked before the content type, like the design orders them.
func readJSONBody(w http.ResponseWriter, r *http.Request, cap int) ([]byte, bool) {
	data, err := io.ReadAll(io.LimitReader(r.Body, int64(cap)+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid", "invalid json")
		return nil, false
	}
	if len(data) > cap {
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", "body too large")
		return nil, false
	}
	if mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json")
		return nil, false
	}
	return data, true
}

// decodeJSON decodes data as exactly one JSON value into v; with
// strict, unknown object fields are refused.
func decodeJSON(data []byte, v any, strict bool) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if strict {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(v); err != nil {
		return err
	}
	// A second Decode must see a clean end of input: a stray closing
	// bracket or brace after the value is trailing data.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

// methodNotAllowed answers 405 with the Allow header.
func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

// writeError answers one errorBody line.
func writeError(w http.ResponseWriter, status int, code, motivo string) {
	writeJSON(w, status, errorBody{Status: code, Motivo: motivo})
}

// writeJSON answers v as one JSON line with the content type set.
func writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		logf("registry: response marshal: %v", err)
		data = []byte(`{"status":"error","motivo":"internal error"}`)
		status = http.StatusInternalServerError
	}
	data = append(data, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(data)
}
