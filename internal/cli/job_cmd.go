package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/soho"
)

// jobPublicSubs are the dispatcher-facing subcommands forwarded to
// herdr-soho job with the same arguments.
var jobPublicSubs = map[string]bool{
	"start": true, "status": true, "wait": true, "events": true,
	"collect": true, "amend": true, "send": true, "ack": true,
	"cancel": true, "close": true, "list": true,
}

// jobInternalSubs are refused without running anything (exit 2).
var jobInternalSubs = map[string]bool{
	"supervise": true, "checkpoint": true, "note": true,
}

// jobWritingSubs refuse to run under HERDR_HERMES_NOWRITE=1 (exit 2); the
// read-only subs work under it and skip bookkeeping.
var jobWritingSubs = map[string]bool{
	"start": true, "amend": true, "send": true, "ack": true,
	"cancel": true, "close": true,
}

// jobStdinCaps are the contract stdin caps (bytes) per sub.
var jobStdinCaps = map[string]int{
	"start": jobapi.StdinCapStart,
	"amend": jobapi.StdinCapAmend,
	"send":  jobapi.StdinCapSend,
}

// jobBoundMargin and jobDefaultBound are the subprocess bounds of the
// forwarded job command (wait/events --wait plus the margin, everything
// else the default). Package variables so tests can run a deadline in
// milliseconds instead of minutes.
var (
	jobBoundMargin  = 30 * time.Second
	jobDefaultBound = 120 * time.Second
)

// cmdJob implements `job <sub> [args]`: it validates the contract input
// limits, forwards the subcommand to herdr-soho as an argv subprocess and
// does the local bookkeeping after a successful forward. The forwarded
// exit code and stdout are never changed by the bookkeeping.
//
// TODO(herdr-hermes): verify that the remote execution channel that runs
// `herdr-hermes job` for the dispatcher forwards stdin and the exit code
// unchanged; the forwarder itself is transparent.
func cmdJob(args []string, env Env) int {
	if len(args) == 0 {
		badUsage(env, "usage: job <start|status|wait|events|collect|amend|send|ack|cancel|close|list> [args]")
		return 2
	}
	sub := args[0]
	if jobInternalSubs[sub] {
		badUsage(env, "job "+sub+" is an internal subcommand, not a dispatcher command")
		return 2
	}
	if !jobPublicSubs[sub] {
		badUsage(env, "unknown job subcommand "+quote(sub))
		return 2
	}
	rest := args[1:]
	nowrite := env.Getenv(nowriteVar) == "1"
	if jobWritingSubs[sub] && nowrite {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	// The contract input limits, applied before forwarding so an invalid
	// input never reaches the subprocess.
	for _, name := range []string{"--id", "--repo", "--base"} {
		value, present := flagValue(rest, name)
		if !present {
			continue
		}
		var valid bool
		switch name {
		case "--id":
			valid = jobapi.ValidID(value)
		case "--repo":
			valid = jobapi.ValidRepo(value)
		case "--base":
			valid = jobapi.ValidBase(value)
		}
		if !valid {
			badUsage(env, "job "+sub+": invalid "+name)
			return 2
		}
	}
	// wait --timeout and events --wait are bounded by the contract (ms),
	// and wait --timeout also sets the subprocess bound. The bounds are
	// package variables so tests can run a deadline in milliseconds.
	var bound time.Duration
	switch sub {
	case "wait":
		bound = time.Duration(jobapi.MaxWaitMS)*time.Millisecond + jobBoundMargin
		if value, present := flagValue(rest, "--timeout"); present {
			ms, err := parseWaitMS(value)
			if err != nil {
				badUsage(env, "job wait: invalid --timeout")
				return 2
			}
			bound = time.Duration(ms)*time.Millisecond + jobBoundMargin
		}
	case "events":
		bound = jobDefaultBound
		if value, present := flagValue(rest, "--wait"); present {
			ms, err := parseWaitMS(value)
			if err != nil {
				badUsage(env, "job events: invalid --wait")
				return 2
			}
			bound = time.Duration(ms)*time.Millisecond + jobBoundMargin
		}
	default:
		bound = jobDefaultBound
	}
	// stdin caps: read at most cap+1 bytes; over the cap refuses without
	// running the subprocess. Other subs pass stdin through.
	var stdin io.Reader = env.Stdin
	if cap, ok := jobStdinCaps[sub]; ok {
		data, over, err := readCappedOver(env.Stdin, cap)
		if err != nil {
			badUsage(env, "job "+sub+": cannot read stdin: "+err.Error())
			return 2
		}
		if over {
			badUsage(env, "job "+sub+": stdin exceeds the contract limit")
			return 2
		}
		stdin = bytes.NewReader(data)
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	runner := soho.Runner{Bin: cfg.HerdrSohoBin, Environ: childEnviron(env)}
	// pendingDiag remembers the one diagnostic the runner reports
	// inside Run (a zero exit whose output did not drain within the
	// runner's grace period): the friction line is written immediately
	// when not under NOWRITE (a file write, never the CLI's stdout or
	// stderr), and the stderr line is written once, synchronously,
	// after the delivery has completed and before the command returns.
	var pendingDiag string
	runner.Diag = func(msg string) {
		if !nowrite {
			outbox.Friction(outbox.StateDir(env.ConfigDir), "job", msg)
		}
		pendingDiag = msg
	}
	// awaitDelivery waits for the delivery of the child's output to
	// env.Stdout and env.Stderr to finish, so the bytes on the writers
	// are complete before the command returns. The delivery to the
	// capture buffer needs no wait (it is written on the copy path);
	// this only orders the consumer delivery. The child is bounded (the
	// context deadline plus the runner's WaitDelay), so what it wrote is
	// finite, and the delivery of it follows the consumer: a consumer
	// that never reads holds the CLI exactly as it would hold
	// herdr-soho run directly. No byte the child wrote is dropped and
	// no exit code is returned before the delivery ends.
	awaitDelivery := func() {
		<-runner.DrainDone()
	}
	var stdoutBuf bytes.Buffer
	// The capture is written on the copy path, so the bookkeeping
	// below sees the start line even when the stdout delivery stalls
	// past the runner's grace period; the bytes on env.Stdout stay
	// exactly the child's bytes (delivered in flight when the consumer
	// stalls).
	runner.Capture = &stdoutBuf
	exit, rerr := runner.Run(context.Background(),
		append([]string{"job", sub}, rest...),
		stdin, env.Stdout, env.Stderr, bound)
	if rerr != nil {
		var ue *soho.ErrUnavailable
		if errors.As(rerr, &ue) {
			// Every ErrUnavailable (not found, or exists but not
			// runnable) prints the same unavailable line; the cause
			// stays in the stderr diagnostic. The child never ran, so
			// there is no delivery to await.
			_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: %v\n", rerr)
			_, _ = fmt.Fprintf(env.Stdout, `{"status":"unavailable","motivo":"herdr-soho not found"}`+"\n")
			return jobapi.ExitUnavailable
		}
		var de *soho.ErrDeadline
		if errors.As(rerr, &de) {
			// The bound killed the child: exit 4, with one timeout line
			// on stdout only when the child wrote nothing there (the
			// child's bytes stay as they are); the cause stays in the
			// stderr diagnostic. No bookkeeping on this path. Await the
			// delivery first so the forwarder's own lines do not
			// interleave with the child's bytes on the env's writers;
			// they are written synchronously on this goroutine once the
			// delivery is done.
			awaitDelivery()
			_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: herdr-soho job %s killed after %s\n", sub, de.Bound)
			if stdoutBuf.Len() == 0 {
				_, _ = fmt.Fprintf(env.Stdout, `{"status":"timeout","motivo":"herdr-soho job %s did not finish within %s"}`+"\n", sub, de.Bound)
			}
			return jobapi.ExitUnavailable
		}
		awaitDelivery()
		motivo := "herdr-soho job " + sub + ": " + rerr.Error()
		_, _ = io.WriteString(env.Stderr, "herdr-hermes: "+motivo+"\n")
		_, _ = io.WriteString(env.Stdout, mustJSON(errorLine{Status: itoa(2), Motivo: motivo})+"\n")
		return 2
	}
	// Bookkeeping after a successful forward; it never changes the exit
	// code or the stdout above, and it is skipped under NOWRITE. It runs
	// from the Capture buffer, before the delivery wait, so a successful
	// start is tracked while the consumer delivery is still in flight.
	if !nowrite {
		bookkeepJob(sub, rest, cfg, env, &stdoutBuf, exit)
	}
	awaitDelivery()
	// The pending diagnostic (if any) is written after the delivery and
	// before the return: one stderr line, synchronously, under NOWRITE
	// too (the friction line, when not under NOWRITE, was already
	// written immediately inside Run).
	if pendingDiag != "" {
		_, _ = io.WriteString(env.Stderr, "herdr-hermes: "+pendingDiag+"\n")
	}
	return exit
}

// childEnviron returns the exact environment forwarded to subprocesses.
func childEnviron(env Env) []string {
	if env.Environ == nil {
		return os.Environ()
	}
	return env.Environ()
}

// flagValue returns the value of the --name flag in args, in both the
// "--name value" and "--name=value" forms. present is false when the flag
// is absent; a bare trailing "--name" is present with an empty value.
func flagValue(args []string, name string) (value string, present bool) {
	prefix := name + "="
	for i := 0; i < len(args); i++ {
		if args[i] == name {
			if i+1 < len(args) {
				return args[i+1], true
			}
			return "", true
		}
		if strings.HasPrefix(args[i], prefix) {
			return args[i][len(prefix):], true
		}
	}
	return "", false
}

// parseWaitMS parses a contract wait value in milliseconds: a
// non-negative integer of at most jobapi.MaxWaitMS.
func parseWaitMS(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("empty")
	}
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("not a non-negative integer")
		}
		n = n*10 + int64(r-'0')
		if n > jobapi.MaxWaitMS {
			return 0, errors.New("exceeds the contract limit")
		}
	}
	return n, nil
}

// readCappedOver reads at most cap+1 bytes from r: over is true when more
// than cap bytes were available.
func readCappedOver(r io.Reader, cap int) (data []byte, over bool, err error) {
	buf := make([]byte, cap+1)
	n, err := io.ReadFull(r, buf)
	data = buf[:n]
	if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
		err = nil
	}
	if err != nil {
		return data, false, err
	}
	return data, n > cap, nil
}

// bookkeepJob runs the post-forward bookkeeping for one sub. Every
// failure is a friction line; none of it touches the exit code or stdout.
func bookkeepJob(sub string, rest []string, cfg config.Config, env Env, stdout *bytes.Buffer, exit int) {
	stateDir := outbox.StateDir(env.ConfigDir)
	friction := func(msg string) { outbox.Friction(stateDir, "job "+sub, msg) }
	id, _ := flagValue(rest, "--id")
	repo, _ := flagValue(rest, "--repo")
	switch sub {
	case "start":
		bookkeepStart(stateDir, rest, cfg, env, stdout, exit, friction)
	case "amend", "send":
		bookkeepAmend(stateDir, sub, rest, cfg, env, stdout, exit, id, repo, friction)
	case "close":
		bookkeepClose(stateDir, rest, env, exit, id, friction)
	}
}

// bookkeepStart tracks a started job and appends one dispatch record,
// except for a duplicate_of response to an already tracked job.
func bookkeepStart(stateDir string, rest []string, cfg config.Config, env Env, stdout *bytes.Buffer, exit int, friction func(string)) {
	line, ok := firstJSONLine(stdout.String())
	if !ok {
		if exit == 0 {
			friction("start: no JSON line on stdout, bookkeeping skipped")
		}
		return
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		friction("start: stdout JSON line is malformed: " + err.Error())
		return
	}
	hasDup := hasKey(obj, "duplicate_of")
	if exit != 0 && !hasDup {
		return
	}
	id, _ := flagValue(rest, "--id")
	repo, _ := flagValue(rest, "--repo")
	jobID := stringField(obj, "id")
	if jobID == "" {
		jobID = id
	}
	if jobID == "" {
		friction("start: no job id in the response, bookkeeping skipped")
		return
	}
	estado := stringField(obj, "status")
	if estado == "" {
		estado = stringField(obj, "estado")
	}
	store, err := outbox.Open(stateDir, outbox.Options{Now: env.Now})
	if err != nil {
		friction("start: open state: " + err.Error())
		return
	}
	projeto := repo
	// The job is tracked independently: created when missing and
	// estado updated when present, whatever happens with the
	// dispatch record below.
	if err := store.UpdateJobs(func(js *outbox.Jobs) error {
		j := js.Jobs[jobID]
		if j == nil {
			ts := env.Now().Format(outbox.TSLayout)
			j = &outbox.Job{ID: jobID, Projeto: projeto, CreatedAt: ts, UpdatedAt: ts}
			js.Jobs[jobID] = j
		}
		if projeto != "" {
			j.Projeto = projeto
		}
		// An absent status/estado never clears the tracked estado.
		if estado != "" {
			j.Estado = estado
		}
		j.UpdatedAt = env.Now().Format(outbox.TSLayout)
		return nil
	}); err != nil {
		friction("start: track job: " + err.Error())
		return
	}
	if cfg.MachineLabel == "" {
		friction("start: machine_label empty, dispatch record not written")
		return
	}
	// The dispatch record is decided by what is actually in the
	// outbox, not by whether the job is tracked: the first start
	// appends one, a duplicate_of appends one when the earlier
	// append failed (the job tracked without a record), and a
	// second one is never appended.
	if exists, err := hasDispatchRecord(store, jobID); err != nil {
		friction("start: scan dispatch records: " + err.Error())
		return
	} else if exists {
		return
	}
	rec := outbox.Record{
		Tipo:    outbox.TipoDispatch,
		Maquina: cfg.MachineLabel,
		Projeto: repo,
		JobID:   &jobID,
		Dados:   json.RawMessage(line),
	}
	if _, err := store.Append(rec); err != nil {
		friction("start: append dispatch record: " + err.Error())
	}
}

// hasDispatchRecord reports whether the outbox already holds a
// dispatch record for jobID. There is no exported helper for it, so
// it scans the raw lines with a lenient decode.
func hasDispatchRecord(store *outbox.Store, jobID string) (bool, error) {
	lines, _, err := store.Read(0)
	if err != nil {
		return false, err
	}
	for _, line := range lines {
		var r outbox.Record
		if err := json.Unmarshal(line, &r); err != nil {
			continue
		}
		if r.Tipo == outbox.TipoDispatch && r.JobID != nil && *r.JobID == jobID {
			return true, nil
		}
	}
	return false, nil
}

// bookkeepAmend appends one amend record after a successful amend (type
// amend) or send (type nota).
func bookkeepAmend(stateDir, sub string, rest []string, cfg config.Config, env Env, stdout *bytes.Buffer, exit int, id, repo string, friction func(string)) {
	if exit != 0 {
		return
	}
	line, ok := firstJSONLine(stdout.String())
	if !ok {
		friction(sub + ": no JSON line on stdout, record not written")
		return
	}
	if cfg.MachineLabel == "" {
		friction(sub + ": machine_label empty, amend record not written")
		return
	}
	store, err := outbox.Open(stateDir, outbox.Options{Now: env.Now})
	if err != nil {
		friction(sub + ": open state: " + err.Error())
		return
	}
	projeto := repo
	if projeto == "" {
		projeto, _ = trackedProjeto(store, id)
	}
	tipo := "amend"
	if sub == "send" {
		tipo = "nota"
	}
	dados := struct {
		Refs struct {
			Tipo string `json:"tipo"`
		} `json:"refs"`
		Resposta json.RawMessage `json:"resposta"`
	}{Resposta: json.RawMessage(line)}
	dados.Refs.Tipo = tipo
	dadosJSON, err := json.Marshal(dados)
	if err != nil {
		friction(sub + ": encode record: " + err.Error())
		return
	}
	rec := outbox.Record{
		Tipo:    outbox.TipoAmend,
		Maquina: cfg.MachineLabel,
		Projeto: projeto,
		Dados:   dadosJSON,
	}
	if id != "" {
		rec.JobID = &id
	}
	if _, err := store.Append(rec); err != nil {
		friction(sub + ": append amend record: " + err.Error())
	}
}

// bookkeepClose marks a closed job as closed in jobs.json.
func bookkeepClose(stateDir string, rest []string, env Env, exit int, id string, friction func(string)) {
	if exit != 0 {
		return
	}
	if id == "" {
		friction("close: no --id, bookkeeping skipped")
		return
	}
	store, err := outbox.Open(stateDir, outbox.Options{Now: env.Now})
	if err != nil {
		friction("close: open state: " + err.Error())
		return
	}
	err = store.UpdateJobs(func(js *outbox.Jobs) error {
		j := js.Jobs[id]
		if j == nil {
			return fmt.Errorf("job %s is not tracked", id)
		}
		j.Closed = true
		j.UpdatedAt = env.Now().Format(outbox.TSLayout)
		return nil
	})
	if err != nil {
		friction("close: " + err.Error())
	}
}

// firstJSONLine returns the first non-empty line of s when it is a JSON
// object.
func firstJSONLine(s string) (string, bool) {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if json.Valid([]byte(line)) && strings.HasPrefix(line, "{") {
			return line, true
		}
		return "", false
	}
	return "", false
}

func hasKey(obj map[string]json.RawMessage, key string) bool {
	_, ok := obj[key]
	return ok
}

func stringField(obj map[string]json.RawMessage, key string) string {
	var s string
	_ = json.Unmarshal(obj[key], &s)
	return s
}

func trackedProjeto(store *outbox.Store, id string) (string, error) {
	jobs, err := store.LoadJobs()
	if err != nil {
		return "", err
	}
	if j, ok := jobs.Jobs[id]; ok {
		return j.Projeto, nil
	}
	return "", nil
}
