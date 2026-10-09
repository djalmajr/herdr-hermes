package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/soho"
)

// syncTerminalStates are the trailer states that make a job terminal for
// sync purposes: a terminal job is skipped by later syncs.
var syncTerminalStates = []string{
	jobapi.StateDone, jobapi.StateFailed, jobapi.StateTimeout,
	jobapi.StateCanceled, jobapi.StateCollected, jobapi.StateClosed,
}

func isSyncTerminal(state string) bool {
	for _, s := range syncTerminalStates {
		if s == state {
			return true
		}
	}
	return false
}

// cmdSync implements `sync [--job <id>] [--push-only]`: the capability
// check, the per-job event sync and the push of pending records.
func cmdSync(args []string, env Env) int {
	jobID, pushOnly, ok := parseSyncArgs(args)
	if !ok {
		badUsage(env, "usage: sync [--job <id>] [--push-only]")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	runner := soho.Runner{Bin: cfg.HerdrSohoBin, Environ: childEnviron(env)}
	ctx := context.Background()
	// The capability check comes first and is skipped with --push-only.
	if !pushOnly {
		caps, cerr := runner.Capabilities(ctx)
		if cerr != nil || !caps.Has(jobapi.CapEphemeralJob) || !caps.Has(jobapi.CapJobEvents) {
			motivo := "herdr-soho capabilities missing or unreadable"
			if cerr != nil {
				motivo = cerr.Error()
			}
			_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: %s\n", motivo)
			_, _ = fmt.Fprintf(env.Stdout, `{"status":"capabilities_missing","motivo":"%s"}`+"\n", sanitizeJSON(motivo))
			return 43
		}
	}
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		fail(env, 2, "sync: "+err.Error())
		return 2
	}
	// The sync writes job_event records, so it needs the machine label;
	// a --push-only run never writes a record and needs nothing.
	if !pushOnly && cfg.MachineLabel == "" {
		fail(env, 2, "machine_label not set")
		return 2
	}
	jobs, err := store.LoadJobs()
	if err != nil {
		fail(env, 2, "sync: "+err.Error())
		return 2
	}
	var ids []string
	if jobID != "" {
		if _, known := jobs.Jobs[jobID]; !known {
			_, _ = fmt.Fprintf(env.Stdout, `{"status":"not_found"}`+"\n")
			return 3
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
		if j.Closed || isSyncTerminal(j.Estado) {
			continue
		}
		jobCount++
		res, err := runner.Events(ctx, id, j.LastEventSeq)
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
					jj.Estado = estado
					jj.UpdatedAt = env.Now().Format(outbox.TSLayout)
				}
				return nil
			}); err != nil {
				outbox.Friction(outbox.StateDir(env.ConfigDir), "sync", "job "+id+": "+err.Error())
			}
		}
	}
	res := pushPending(ctx, env, cfg, store, true)
	status := ""
	if res.Code != 0 {
		// Only a failed push (40, 41, 42) carries a status in the line.
		status = res.Status
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Jobs      int    `json:"jobs"`
		Novos     int    `json:"novos"`
		Enviados  int    `json:"enviados"`
		Pendentes int    `json:"pendentes"`
		Status    string `json:"status,omitempty"`
	}{jobCount, novos, res.Sent, res.Pending, status}))
	return res.Code
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
