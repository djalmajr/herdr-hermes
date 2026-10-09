package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
)

// wakeStdinCap is the wake event stdin cap: 64 KiB, the same bound the
// contract gives the `job amend` body.
const wakeStdinCap = jobapi.StdinCapAmend

// cmdWake records one job event on stdin as a job_event outbox record and
// attempts one bounded push. It is the `herdr-soho` wake hook (machine key
// job_wake_cmd). It exits 0 as soon as the record is durable (or deduplicated),
// whatever the push result; it exits 2 only for malformed input.
func cmdWake(args []string, env Env) int {
	if len(args) != 0 {
		badUsage(env, "usage: wake")
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
	event, err := readCapped(env.Stdin, wakeStdinCap)
	if errors.Is(err, errInputOverCap) {
		fail(env, 2, "wake: stdin is over the 64 KiB cap")
		return 2
	}
	if err != nil {
		fail(env, 2, "wake: reading stdin: "+err.Error())
		return 2
	}
	id := env.Getenv(jobapi.EnvJobID)
	if !jobapi.ValidID(id) {
		fail(env, 2, "wake: "+jobapi.EnvJobID+" is missing or does not match the job id pattern")
		return 2
	}
	var probe struct {
		Seq int64 `json:"seq"`
	}
	if err := json.Unmarshal(event, &probe); err != nil {
		fail(env, 2, "wake: stdin is not a valid JSON event")
		return 2
	}
	if probe.Seq <= 0 {
		fail(env, 2, "wake: the event seq is missing or not a positive integer")
		return 2
	}
	if s := env.Getenv(jobapi.EnvJobSeq); s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err != nil || n != probe.Seq {
			fail(env, 2, "wake: "+jobapi.EnvJobSeq+" does not match the event seq")
			return 2
		}
	}
	if k := env.Getenv(jobapi.EnvJobIdempotencyKey); k != "" {
		if k != id+":"+strconv.FormatInt(probe.Seq, 10) {
			fail(env, 2, "wake: "+jobapi.EnvJobIdempotencyKey+" is not <id>:<seq> for this event")
			return 2
		}
	}
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		fail(env, 2, "wake: "+err.Error())
		return 2
	}
	// The projeto comes from the tracked job when there is one; an unknown
	// job is tracked on the fly with an empty projeto.
	projeto := ""
	jobs, err := store.LoadJobs()
	if err != nil {
		fail(env, 2, "wake: "+err.Error())
		return 2
	}
	if j, ok := jobs.Jobs[id]; ok {
		projeto = j.Projeto
	}
	rec, appended, err := store.AppendJobEvent(cfg.MachineLabel, projeto, id, json.RawMessage(event))
	if err != nil {
		// A failure here (lock timeout, disk error) is not malformed input,
		// but the exit-2 convention still applies: the wake hook retries
		// and the dedupe converges.
		fail(env, 2, "wake: "+err.Error())
		return 2
	}
	seq := int64(0)
	if appended {
		seq = rec.Seq
	}
	// The record is durable: exit 0 whatever the push result.
	ctx, cancel := context.WithTimeout(context.Background(), wakePushTimeout(cfg))
	defer cancel()
	o := pushPending(ctx, env, cfg, store, false)
	_, _ = fmt.Fprintf(env.Stdout, "{\"seq\":%d,\"duplicado\":%t,\"enviados\":%d,\"pendentes\":%d}\n",
		seq, !appended, o.Sent, o.Pending)
	return 0
}

// wakePushTimeout bounds the wake push step: push_timeout_s plus 5 s.
func wakePushTimeout(cfg config.Config) time.Duration {
	return time.Duration(cfg.PushTimeoutS+5) * time.Second
}
