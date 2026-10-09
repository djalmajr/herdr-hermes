package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// stateCut is the contract cap on resumo, motivo and estado: 280 code
// points.
const stateCut = 280

// sessionDados is the dados payload of a session record; empty fields are
// omitted. No person field is ever carried: the receiver derives the person
// from the authenticated key.
type sessionDados struct {
	Acao    string `json:"acao"`
	Projeto string `json:"projeto,omitempty"`
	Branch  string `json:"branch,omitempty"`
	Card    string `json:"card,omitempty"`
	Job     string `json:"job,omitempty"`
	Estado  string `json:"estado,omitempty"`
	PR      string `json:"pr,omitempty"`
}

// sessionFlags carries the parsed flags of one session subcommand.
type sessionFlags struct {
	projeto string
	branch  string
	card    string
	job     string
	estado  string
	pr      string
	have    map[string]bool
}

// sessionFlagAllow lists the flags each subcommand accepts besides
// --projeto.
var sessionFlagAllow = map[string]map[string]bool{
	"start":  {"--card": true, "--branch": true, "--job": true},
	"update": {"--branch": true, "--card": true, "--estado": true, "--pr": true},
	"end":    {"--estado": true, "--pr": true},
}

// cutRunes truncates s to at most n code points, cutting on rune
// boundaries so a UTF-8 sequence is never split.
func cutRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// cmdSession implements `session start|update|end --projeto <org>/<repo>
// [flags]`. Every call appends one session record; start and update create
// or update the sessions.json entry, end closes it (open sessions only
// live in sessions.json, so closing removes the entry).
func cmdSession(args []string, env Env) int {
	if len(args) < 2 {
		badUsage(env, "usage: session start|update|end --projeto <org>/<repo> [flags]")
		return 2
	}
	sub := args[0]
	allow, ok := sessionFlagAllow[sub]
	if !ok {
		badUsage(env, "unknown session subcommand "+quote(sub))
		return 2
	}
	flags, ok := parseSessionFlags(args[1:])
	if !ok {
		badUsage(env, "usage: session start|update|end --projeto <org>/<repo> [flags]")
		return 2
	}
	for f := range flags.have {
		if f != "--projeto" && !allow[f] {
			badUsage(env, "session "+sub+": "+f+" is not accepted by this subcommand")
			return 2
		}
	}
	if !flags.have["--projeto"] {
		badUsage(env, "session "+sub+": --projeto is required")
		return 2
	}
	if !jobapi.ValidRepo(flags.projeto) {
		badUsage(env, "session "+sub+": --projeto must be <org>/<repo> with each part matching the contract charset")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	if cfg.MachineLabel == "" {
		fail(env, 2, "machine_label is empty: set it with `herdr-hermes config set machine_label <label>`")
		return 2
	}
	flags.estado = cutRunes(flags.estado, stateCut)
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		fail(env, 2, "session "+sub+": "+err.Error())
		return 2
	}
	// The session change and the record append are one atomic step under
	// one lock, with the append first: when the append fails the open
	// session is still there, so a retry writes the end record with the
	// branch, card and job.
	var dados sessionDados
	rec, err := store.UpdateSessionsAndAppend(func(ss *outbox.Sessions) (outbox.Record, error) {
		key := sessionTarget(ss, sub, flags)
		e := ss.Sessions[key]
		now := env.Now().Format(outbox.TSLayout)
		if e == nil {
			e = &outbox.Session{Projeto: flags.projeto, StartedAt: now}
			ss.Sessions[key] = e
		}
		if flags.branch != "" {
			e.Branch = flags.branch
		}
		if flags.card != "" {
			e.Card = flags.card
		}
		if flags.job != "" {
			e.Job = flags.job
		}
		if flags.estado != "" {
			e.Estado = flags.estado
		}
		if flags.pr != "" {
			e.PR = flags.pr
		}
		e.UpdatedAt = now
		dados = sessionDados{
			Acao:    sub,
			Projeto: flags.projeto,
			Branch:  e.Branch,
			Card:    e.Card,
			Job:     e.Job,
			Estado:  e.Estado,
			PR:      e.PR,
		}
		if sub == "end" {
			// Open sessions only: closing the session removes the entry.
			delete(ss.Sessions, key)
		}
		return outbox.Record{
			Tipo:    outbox.TipoSession,
			Maquina: cfg.MachineLabel,
			Projeto: flags.projeto,
			JobID:   jobIDPtr(dados.Job),
			Dados:   mustMarshal(dados),
		}, nil
	})
	if err != nil {
		fail(env, 2, "session "+sub+": "+err.Error())
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "{\"seq\":%d}\n", rec.Seq)
	return 0
}

// sessionTarget resolves the sessions.json key from the already loaded
// sessions: <projeto>@<branch or job or "">. start and update with --branch
// use the branch; start with --job (no --branch) uses the job; otherwise
// it is the most recently updated open session of that projeto (ties: the
// lexicographically smallest key), and when none exists the key with the
// empty part. It runs inside the UpdateSessionsAndAppend lock, so it takes
// the loaded sessions instead of the store (no second lock).
func sessionTarget(ss *outbox.Sessions, sub string, flags sessionFlags) string {
	if flags.branch != "" {
		return flags.projeto + "@" + flags.branch
	}
	if sub == "start" && flags.job != "" {
		return flags.projeto + "@" + flags.job
	}
	prefix := flags.projeto + "@"
	var best *outbox.Session
	var bestKey string
	for k, s := range ss.Sessions {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if best == nil || s.UpdatedAt > best.UpdatedAt || (s.UpdatedAt == best.UpdatedAt && k < bestKey) {
			best = s
			bestKey = k
		}
	}
	if best == nil {
		return prefix
	}
	return bestKey
}

// parseSessionFlags parses the --flag value pairs of a session subcommand.
// Any argument that is not a known flag with a value is bad usage.
func parseSessionFlags(args []string) (sessionFlags, bool) {
	var f sessionFlags
	f.have = map[string]bool{}
	if len(args)%2 != 0 {
		return f, false
	}
	for i := 0; i < len(args); i += 2 {
		switch args[i] {
		case "--projeto":
			f.projeto = args[i+1]
		case "--branch":
			f.branch = args[i+1]
		case "--card":
			f.card = args[i+1]
		case "--job":
			f.job = args[i+1]
		case "--estado":
			f.estado = args[i+1]
		case "--pr":
			f.pr = args[i+1]
		default:
			return f, false
		}
		f.have[args[i]] = true
	}
	return f, true
}

func jobIDPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte(`{}`)
	}
	return b
}
