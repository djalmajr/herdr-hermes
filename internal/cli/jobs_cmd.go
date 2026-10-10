package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	jobsDefaultServer = "http://127.0.0.1:7429"
	jobsUsageText     = "usage: jobs [check|release|policy|folder|node] …"
	jobsRespCap       = 16 << 20
	jobsTimeout       = 30 * time.Second
)

// jobsForm is the parsed form of a jobs invocation: the subcommand, the
// verb for policy/folder/node and the space-form flags.
type jobsForm struct {
	sub          string
	verb         string
	server       string
	serverSet    bool
	maquina      string
	agente       string
	card         string
	projeto      string
	branch       string
	pr           string
	workspace    string
	ttl          string
	claim        string
	escopo       string
	valor        string
	hold         bool
	prioridade   string
	motivo       string
	caminho      string
	pedido       string
	tamanho      string
	job          string
	manutencao   string
	sohoVersion  string
	runners      string
	runnersSet   bool
	nodeOptional bool
}

// jobsAllowedFlags is the flag set per form; anything outside it is bad
// usage.
func jobsAllowedFlags(f jobsForm) map[string]bool {
	switch f.sub {
	case "":
		return map[string]bool{"--server": true, "--maquina": true, "--agente": true}
	case "check":
		return map[string]bool{"--server": true, "--agente": true, "--card": true, "--projeto": true, "--branch": true, "--pr": true, "--workspace": true, "--maquina": true, "--ttl": true}
	case "release":
		return map[string]bool{"--server": true, "--claim": true}
	case "policy":
		if f.verb == "set" {
			return map[string]bool{"--server": true, "--escopo": true, "--valor": true, "--hold": true, "--prioridade": true, "--motivo": true}
		}
		return map[string]bool{"--server": true, "--escopo": true, "--valor": true}
	case "folder":
		if f.verb == "add" {
			return map[string]bool{"--server": true, "--maquina": true, "--caminho": true, "--pedido": true, "--tamanho": true, "--job": true}
		}
		return map[string]bool{"--server": true, "--maquina": true, "--caminho": true}
	case "node":
		return map[string]bool{"--server": true, "--maquina": true, "--manutencao": true, "--motivo": true, "--soho-version": true, "--runners": true}
	}
	return map[string]bool{}
}

// parseJobsArgs accepts the forms documented in the usage: the subcommand
// (and the verb for policy, folder and node) as positionals, anywhere
// among the flags, and the flags in the space form, each at most once.
// --hold is bare. The allowed flags are checked per form.
func parseJobsArgs(args []string) (jobsForm, bool) {
	var f jobsForm
	type jobsFlag struct {
		name  string
		value string
	}
	var flags []jobsFlag
	var positionals []string
	seen := make(map[string]bool)
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			positionals = append(positionals, a)
			continue
		}
		if len(a) <= 2 || seen[a] {
			return f, false
		}
		seen[a] = true
		if a == "--hold" {
			flags = append(flags, jobsFlag{name: a})
			continue
		}
		if i+1 >= len(args) {
			return f, false
		}
		flags = append(flags, jobsFlag{name: a, value: args[i+1]})
		i++
	}
	switch len(positionals) {
	case 0:
	case 1:
		switch positionals[0] {
		case "check", "release":
			f.sub = positionals[0]
		default:
			return f, false
		}
	case 2:
		switch positionals[0] + " " + positionals[1] {
		case "policy set", "policy clear", "folder add", "folder clear", "node set":
			f.sub, f.verb = positionals[0], positionals[1]
		default:
			return f, false
		}
	default:
		return f, false
	}
	allowed := jobsAllowedFlags(f)
	for _, fl := range flags {
		if !allowed[fl.name] {
			return f, false
		}
		switch fl.name {
		case "--server":
			f.server, f.serverSet = fl.value, true
		case "--maquina":
			f.maquina = fl.value
		case "--agente":
			f.agente = fl.value
		case "--card":
			f.card = fl.value
		case "--projeto":
			f.projeto = fl.value
		case "--branch":
			f.branch = fl.value
		case "--pr":
			f.pr = fl.value
		case "--workspace":
			f.workspace = fl.value
		case "--ttl":
			f.ttl = fl.value
		case "--claim":
			f.claim = fl.value
		case "--escopo":
			f.escopo = fl.value
		case "--valor":
			f.valor = fl.value
		case "--prioridade":
			f.prioridade = fl.value
		case "--motivo":
			f.motivo = fl.value
		case "--caminho":
			f.caminho = fl.value
		case "--pedido":
			f.pedido = fl.value
		case "--tamanho":
			f.tamanho = fl.value
		case "--job":
			f.job = fl.value
		case "--manutencao":
			f.manutencao = fl.value
		case "--soho-version":
			f.sohoVersion = fl.value
		case "--runners":
			f.runners, f.runnersSet = fl.value, true
		case "--hold":
			f.hold = true
		}
		if f.sub == "node" && (fl.name == "--manutencao" || fl.name == "--motivo" || fl.name == "--soho-version" || fl.name == "--runners") {
			f.nodeOptional = true
		}
	}
	return f, true
}

// validValues applies the client-side value rules; the field validation
// itself (escopo, ranges) stays on the server and maps to 400.
func (f jobsForm) validValues() bool {
	if f.sub == "check" && f.agente == "" {
		return false
	}
	if f.sub == "release" && f.claim == "" {
		return false
	}
	if f.sub == "policy" && (f.escopo == "" || f.valor == "") {
		return false
	}
	if f.sub == "folder" {
		if f.maquina == "" || f.caminho == "" {
			return false
		}
		if f.verb == "add" && f.pedido == "" {
			return false
		}
	}
	if f.sub == "node" {
		if f.maquina == "" || !f.nodeOptional {
			return false
		}
		if f.manutencao != "" && f.manutencao != "on" && f.manutencao != "off" {
			return false
		}
		if f.runnersSet && f.runners == "" {
			return false
		}
	}
	if f.ttl != "" {
		n, err := strconv.Atoi(f.ttl)
		if err != nil || n < 0 {
			return false
		}
	}
	if f.prioridade != "" {
		if _, err := strconv.Atoi(f.prioridade); err != nil {
			return false
		}
	}
	if f.tamanho != "" {
		if _, err := strconv.ParseInt(f.tamanho, 10, 64); err != nil {
			return false
		}
	}
	return true
}

// validJobsServer accepts an http or https URL with no userinfo, query or
// fragment and an empty or "/" path; plain http only on localhost or a
// loopback IP literal.
func validJobsServer(u *url.URL) bool {
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	if u.Path != "" && u.Path != "/" {
		return false
	}
	host := u.Hostname()
	if host == "" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// buildRequest assembles the request of the form: method, target and body
// (nil for the bodyless forms).
func (f jobsForm) buildRequest(base string) (method, target string, body []byte) {
	q := url.Values{}
	switch f.sub {
	case "":
		target = base + "/v1/jobs"
		if f.maquina != "" {
			q.Set("maquina", f.maquina)
		}
		if f.agente != "" {
			q.Set("agente", f.agente)
		}
		method = "GET"
	case "check":
		method = "POST"
		target = base + "/v1/check"
		var ttl int
		if f.ttl != "" {
			ttl, _ = strconv.Atoi(f.ttl)
		}
		body, _ = json.Marshal(struct {
			Agente    string `json:"agente"`
			Maquina   string `json:"maquina"`
			Card      string `json:"card"`
			Projeto   string `json:"projeto"`
			Branch    string `json:"branch"`
			PR        string `json:"pr"`
			Workspace string `json:"workspace"`
			TTLs      int    `json:"ttl_s"`
		}{Agente: f.agente, Maquina: f.maquina, Card: f.card, Projeto: f.projeto, Branch: f.branch, PR: f.pr, Workspace: f.workspace, TTLs: ttl})
	case "release":
		method = "DELETE"
		target = base + "/v1/claims/" + url.PathEscape(f.claim)
	case "policy":
		if f.verb == "set" {
			method = "PUT"
			target = base + "/v1/policies"
			body, _ = json.Marshal(struct {
				Escopo     string `json:"escopo"`
				Valor      string `json:"valor"`
				Hold       bool   `json:"hold"`
				Prioridade *int   `json:"prioridade,omitempty"`
				Motivo     string `json:"motivo,omitempty"`
			}{Escopo: f.escopo, Valor: f.valor, Hold: f.hold, Prioridade: intPtr(f.prioridade), Motivo: f.motivo})
		} else {
			method = "DELETE"
			target = base + "/v1/policies"
			q.Set("escopo", f.escopo)
			q.Set("valor", f.valor)
		}
	case "folder":
		if f.verb == "add" {
			method = "PUT"
			target = base + "/v1/folders"
			body, _ = json.Marshal(struct {
				Maquina      string `json:"maquina"`
				Caminho      string `json:"caminho"`
				TamanhoBytes *int64 `json:"tamanho_bytes,omitempty"`
				Pedido       string `json:"pedido"`
				Job          string `json:"job,omitempty"`
			}{Maquina: f.maquina, Caminho: f.caminho, TamanhoBytes: int64Ptr(f.tamanho), Pedido: f.pedido, Job: f.job})
		} else {
			method = "DELETE"
			target = base + "/v1/folders"
			q.Set("maquina", f.maquina)
			q.Set("caminho", f.caminho)
		}
	case "node":
		method = "PUT"
		target = base + "/v1/nodes/" + url.PathEscape(f.maquina)
		body, _ = json.Marshal(struct {
			Manutencao  *bool     `json:"manutencao,omitempty"`
			Motivo      *string   `json:"manutencao_motivo,omitempty"`
			SohoVersion *string   `json:"soho_version,omitempty"`
			Runners     *[]string `json:"runners,omitempty"`
		}{Manutencao: boolPtr(f.manutencao == "on"), Motivo: strPtr(f.motivo), SohoVersion: strPtr(f.sohoVersion), Runners: runnerList(f.runners)})
	}
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	return method, target, body
}

func intPtr(s string) *int {
	if s == "" {
		return nil
	}
	n, _ := strconv.Atoi(s)
	return &n
}

func int64Ptr(s string) *int64 {
	if s == "" {
		return nil
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return &n
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func boolPtr(b bool) *bool {
	if !b {
		return nil
	}
	v := true
	return &v
}

// runnerList splits the comma-separated --runners value.
func runnerList(s string) *[]string {
	if s == "" {
		return nil
	}
	v := strings.Split(s, ",")
	return &v
}

// cmdJobs is the jobs client: one HTTP request per form against the
// registry service, with the documented status mapping. The client never
// sends credentials.
//
// Mutation captured: breaking the 409-to-44 mapping makes a conflicting
// check exit 4 (registry answered 409) instead of 44.
func cmdJobs(args []string, env Env) int {
	f, ok := parseJobsArgs(args)
	if !ok {
		badUsage(env, jobsUsageText)
		return 2
	}
	// NOWRITE refuses every subcommand except the plain inventory, before
	// any request.
	if env.Getenv(nowriteVar) == "1" && f.sub != "" {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	if !f.validValues() {
		badUsage(env, jobsUsageText)
		return 2
	}
	server := f.server
	if !f.serverSet {
		server = jobsDefaultServer
	}
	u, err := url.Parse(server)
	if err != nil || !validJobsServer(u) {
		fail(env, 2, "jobs: invalid --server")
		return 2
	}
	base := strings.TrimSuffix(server, "/")
	method, target, body := f.buildRequest(base)
	req, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: jobs: %v\n", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "registry unreachable"}))
		return 4
	}
	req.Header.Set("User-Agent", bridgeUserAgent())
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{
		Timeout: jobsTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: jobs: %v\n", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "registry unreachable"}))
		return 4
	}
	defer resp.Body.Close()
	raw, over, err := readCappedOver(resp.Body, jobsRespCap)
	if err != nil {
		_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: jobs: %v\n", err)
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "registry unreachable"}))
		return 4
	}
	if over {
		_, _ = fmt.Fprintln(env.Stderr, "herdr-hermes: jobs: response body exceeds 16 MiB")
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "registry answered malformed"}))
		return 4
	}
	if resp.StatusCode == http.StatusOK {
		if err := exactlyOneJSON(raw); err != nil {
			return jobsMalformed(env)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, raw); err != nil {
			return jobsMalformed(env)
		}
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", compact.String())
		return 0
	}
	if resp.StatusCode == http.StatusConflict && f.sub == "check" {
		_, _ = printJSONLine(env, raw)
		status := jobsBodyStatus(raw)
		if status == "" {
			status = "unknown"
		}
		_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: jobs check: refused (%s)\n", status)
		return 44
	}
	if resp.StatusCode == http.StatusBadRequest {
		motivo := jobsBodyMotivo(raw)
		if motivo == "" {
			motivo = "registry error"
		}
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "2", Motivo: "registry: " + motivo}))
		return 2
	}
	if resp.StatusCode == http.StatusNotFound &&
		(f.sub == "release" || (f.sub == "policy" && f.verb == "clear") || (f.sub == "folder" && f.verb == "clear")) {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "not_found", Motivo: "registry: not found"}))
		return 3
	}
	_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: jobs: registry answered %d\n", resp.StatusCode)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "registry answered " + strconv.Itoa(resp.StatusCode)}))
	return 4
}

// jobsMalformed prints the 4 line for a body that is not exactly one JSON
// value.
func jobsMalformed(env Env) int {
	_, _ = fmt.Fprintln(env.Stderr, "herdr-hermes: jobs: response body is not one JSON value")
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(errorLine{Status: "unavailable", Motivo: "registry answered malformed"}))
	return 4
}

// printJSONLine prints the response body as exactly one line.
func printJSONLine(env Env, raw []byte) (int, error) {
	if !bytes.HasSuffix(raw, []byte("\n")) {
		raw = append(append([]byte(nil), raw...), '\n')
	}
	return env.Stdout.Write(raw)
}

// jobsBodyStatus extracts the status field of the response body; empty
// when the body is not a JSON object.
func jobsBodyStatus(raw []byte) string {
	var v struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return ""
	}
	return v.Status
}

// jobsBodyMotivo extracts the motivo field of the response body; empty
// when the body is not a JSON object.
func jobsBodyMotivo(raw []byte) string {
	var v struct {
		Motivo string `json:"motivo"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return ""
	}
	return v.Motivo
}

// exactlyOneJSON reports whether raw is exactly one JSON value.
func exactlyOneJSON(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if !dec.More() {
		return errors.New("no JSON value")
	}
	var v json.RawMessage
	if err := dec.Decode(&v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("more than one JSON value")
	}
	return nil
}

// bridgeUserAgent is the User-Agent of the outbound requests.
func bridgeUserAgent() string {
	v := version
	if v == "" {
		v = "dev"
	}
	if v == "dev" {
		if rev := vcsRevision(); rev != "" {
			v += "+" + rev
		}
	}
	return "herdr-hermes/" + v
}

var _ io.Reader = (*bytes.Reader)(nil)
