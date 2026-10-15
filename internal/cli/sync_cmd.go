package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/djalmajr/nau/internal/config"
	"github.com/djalmajr/nau/internal/jobapi"
	"github.com/djalmajr/nau/internal/outbox"
	"github.com/djalmajr/nau/internal/soho"
)

// syncTerminalStates are the trailer states that make a job terminal for
// sync purposes: a terminal job is skipped by later syncs only while a
// workspace_closed record of the job is durable, the job is closed, or
// the post-terminal grace is over.
var syncTerminalStates = []string{
	jobapi.StateDone, jobapi.StateFailed, jobapi.StateTimeout,
	jobapi.StateCanceled, jobapi.StateCollected, jobapi.StateClosed,
}

// syncPostTerminalGrace bounds how long a gapless terminal job without a
// recorded workspace_closed keeps being queried. With the automatic close
// switched off the workspace_closed never comes, and the grace is what
// ends the job; that is accepted. Without the grace such a terminal job
// would be queried forever, and the loss is permanent because the
// workspace_closed does not wake the dispatcher.
const syncPostTerminalGrace = time.Hour

func isSyncTerminal(state string) bool {
	for _, s := range syncTerminalStates {
		if s == state {
			return true
		}
	}
	return false
}

// syncSkipPostTerminal reports whether the gapless terminal job may be
// skipped: a workspace_closed record of the job is durable in the outbox,
// or the post-terminal grace is over. A read failure skips the job for
// this run, like a cursor read failure.
func syncSkipPostTerminal(store *outbox.Store, env Env, j *outbox.Job, id string) bool {
	hasTail, terminalTS, err := store.JobEventTail(id, jobapi.TypeTerminal, []string{jobapi.TypeWorkspaceClosed})
	if err != nil {
		outbox.Friction(outbox.StateDir(env.ConfigDir), "sync", "job "+id+": "+err.Error())
		return true
	}
	if hasTail {
		return true
	}
	return syncPostTerminalExpired(env, j, terminalTS)
}

// syncPostTerminalExpired reports whether the post-terminal grace of a
// gapless terminal job is over: the age of the job's stored terminal
// record (or, when the state came only from the trailer and no terminal
// record is stored, of the job's terminal_seen_at) reaches
// syncPostTerminalGrace. The instant comes from the outbox record's ts or
// the job's terminal_seen_at, never from the backend clock or the job's
// updated_at; an unparseable or absent instant is not expired, so the job
// keeps being queried.
func syncPostTerminalExpired(env Env, j *outbox.Job, terminalTS string) bool {
	instant := terminalTS
	if instant == "" {
		instant = j.TerminalSeenAt
	}
	if instant == "" {
		return false
	}
	stored, err := time.Parse(outbox.TSLayout, instant)
	if err != nil {
		return false
	}
	return env.Now().Sub(stored) >= syncPostTerminalGrace
}

// validTerminalAnchor reports whether s parses as an outbox.TSLayout
// instant: an empty or unparseable terminal_seen_at is treated as absent,
// so the next terminal update rewrites it and a bad format can never keep
// the job being queried forever.
func validTerminalAnchor(s string) bool {
	_, err := time.Parse(outbox.TSLayout, s)
	return err == nil
}

// syncResult is the outcome of one sync run: the counts and the push step
// result, plus the one-line reason (ErrMotivo) for the non-push failure
// exits (2, 3 and 43).
type syncResult struct {
	Jobs      int
	Novos     int
	Enviados  int
	Pendentes int
	Status    string
	ErrMotivo string
}

// runSync runs the sync: the capability check (skipped with pushOnly), the
// per-job event sync and the push of pending records. It performs the
// writes; the caller prints the CLI line. Exit codes: 2 (config or state
// error, machine label), 3 (unknown --job), 43 (capabilities) or the push
// code (0, 40, 41, 42).
func runSync(ctx context.Context, env Env, jobID string, pushOnly bool) (syncResult, int) {
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		return syncResult{ErrMotivo: "config: " + err.Error()}, 2
	}
	runner := soho.Runner{Bin: cfg.HerdrSohoBin, Environ: childEnviron(env)}
	// The capability check comes first and is skipped with --push-only.
	if !pushOnly {
		caps, cerr := runner.Capabilities(ctx)
		if cerr != nil || !caps.Has(jobapi.CapEphemeralJob) || !caps.Has(jobapi.CapJobEvents) {
			motivo := "herdr-soho capabilities missing or unreadable"
			if cerr != nil {
				motivo = cerr.Error()
			}
			return syncResult{ErrMotivo: motivo}, 43
		}
	}
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		return syncResult{ErrMotivo: "sync: " + err.Error()}, 2
	}
	// The sync writes job_event records, so it needs the machine label;
	// a --push-only run never writes a record and needs nothing.
	if !pushOnly && cfg.MachineLabel == "" {
		return syncResult{ErrMotivo: "machine_label not set"}, 2
	}
	jobs, err := store.LoadJobs()
	if err != nil {
		return syncResult{ErrMotivo: "sync: " + err.Error()}, 2
	}
	var ids []string
	if jobID != "" {
		if _, known := jobs.Jobs[jobID]; !known {
			return syncResult{ErrMotivo: "job not found"}, 3
		}
		ids = []string{jobID}
	} else {
		for id := range jobs.Jobs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	}
	novos := 0
	jobCount := 0
	for _, id := range ids {
		j := jobs.Jobs[id]
		// The --since value is always the recomputed contiguous stored
		// prefix, never the recorded cursor: a legacy or stale cursor
		// above the stored records would make the sync skip the gap.
		prefix, maxStored, cerr := store.JobEventCursor(id)
		if cerr != nil {
			outbox.Friction(outbox.StateDir(env.ConfigDir), "sync", "job "+id+": "+cerr.Error())
			continue
		}
		// Gap evidence: a stored record above a hole, or a recorded
		// cursor above what is stored. A closed job is skipped only when
		// it has no gap; a terminal job is skipped only when it has no
		// gap and a workspace_closed record is durable or the
		// post-terminal grace is over (the workspace_closed is the last
		// automatic-close event and must reach the outbox); with gap
		// evidence either is synced like an open job (and keeps Closed as
		// it is), so the missing records below the gap are never lost.
		gap := maxStored > prefix || j.LastEventSeq > prefix
		if !gap && j.Closed {
			continue
		}
		if !gap && isSyncTerminal(j.Estado) && syncSkipPostTerminal(store, env, j, id) {
			continue
		}
		jobCount++
		res, err := runner.Events(ctx, id, prefix)
		if err != nil {
			outbox.Friction(outbox.StateDir(env.ConfigDir), "sync", "job "+id+": "+err.Error())
			continue
		}
		stopped := false // set when an event line fails to append
		for _, line := range res.Lines {
			// A line that is not valid JSON or has no positive integer
			// seq fails in AppendJobEvent; the first failure stops the
			// loop and leaves the trailer unapplied, so the next sync
			// retries from the unchanged cursor.
			rec, appended, aerr := store.AppendJobEvent(cfg.MachineLabel, j.Projeto, id, json.RawMessage(line))
			_ = rec
			if aerr != nil {
				outbox.Friction(outbox.StateDir(env.ConfigDir), "sync", "job "+id+": "+aerr.Error())
				stopped = true
				break
			}
			if appended {
				novos++
			}
		}
		if !stopped {
			estado := res.Trailer.Estado
			if err := store.UpdateJobs(func(js *outbox.Jobs) error {
				if jj := js.Jobs[id]; jj != nil {
					// terminal_seen_at anchors the post-terminal grace when
					// the state came only from the trailer. It is rewritten
					// with the env clock when the update makes the job
					// terminal (the stored state before the update was not
					// terminal, so a stale value never starts the grace
					// already expired) and when the stored value does not
					// parse (an invalid anchor is absent, so the first valid
					// rewrite bounds the recurrence); it is cleared when the
					// job stops being terminal and preserved while the job
					// stays terminal with a valid anchor.
					terminal := isSyncTerminal(estado)
					if terminal {
						if !isSyncTerminal(jj.Estado) || !validTerminalAnchor(jj.TerminalSeenAt) {
							jj.TerminalSeenAt = env.Now().Format(outbox.TSLayout)
						}
					} else {
						jj.TerminalSeenAt = ""
					}
					jj.Estado = estado
					jj.UpdatedAt = env.Now().Format(outbox.TSLayout)
				}
				return nil
			}); err != nil {
				outbox.Friction(outbox.StateDir(env.ConfigDir), "sync", "job "+id+": "+err.Error())
			}
		}
	}
	// Project and deliver the pending notifications after the per-job
	// event loop and before the push (skipped with --push-only); the
	// output line and exit code are unchanged.
	if !pushOnly {
		runNotify(ctx, env, cfg, 30*time.Second)
	}
	res := pushPending(ctx, env, cfg, store, true)
	status := ""
	if res.Code != 0 {
		// Only a failed push (40, 41, 42) carries a status in the line.
		status = res.Status
	}
	return syncResult{Jobs: jobCount, Novos: novos, Enviados: res.Sent, Pendentes: res.Pending, Status: status}, res.Code
}

// cmdSync implements `sync [--job <id>] [--push-only]`.
func cmdSync(args []string, env Env) int {
	jobID, pushOnly, ok := parseSyncArgs(args)
	if !ok {
		badUsage(env, "usage: sync [--job <id>] [--push-only]")
		return 2
	}
	res, code := runSync(context.Background(), env, jobID, pushOnly)
	printSyncOutcome(env, res, code)
	return code
}

// printSyncOutcome prints the sync output line according to the exit code.
func printSyncOutcome(env Env, res syncResult, code int) {
	switch code {
	case 3:
		_, _ = fmt.Fprintf(env.Stdout, `{"status":"not_found"}`+"\n")
	case 43:
		_, _ = fmt.Fprintf(env.Stderr, "%s: %s\n", env.program(), res.ErrMotivo)
		_, _ = fmt.Fprintf(env.Stdout, `{"status":"capabilities_missing","motivo":"%s"}`+"\n", sanitizeJSON(res.ErrMotivo))
	case 2:
		fail(env, 2, res.ErrMotivo)
	default:
		printSyncLine(env, res)
	}
}

// printSyncLine prints the sync counts line (with status on a non-zero
// push outcome).
func printSyncLine(env Env, res syncResult) {
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Jobs      int    `json:"jobs"`
		Novos     int    `json:"novos"`
		Enviados  int    `json:"enviados"`
		Pendentes int    `json:"pendentes"`
		Status    string `json:"status,omitempty"`
	}{res.Jobs, res.Novos, res.Enviados, res.Pendentes, res.Status}))
}

// parseSyncArgs accepts --job <id> and --push-only in any order, once each.
func parseSyncArgs(args []string) (jobID string, pushOnly bool, ok bool) {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--job":
			if i+1 >= len(args) || jobID != "" {
				return "", false, false
			}
			jobID = args[i+1]
			i++
		case "--push-only":
			if pushOnly {
				return "", false, false
			}
			pushOnly = true
		default:
			return "", false, false
		}
	}
	return jobID, pushOnly, true
}

// sanitizeJSON keeps the error motivo a valid JSON string.
func sanitizeJSON(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\"", "'")
}
