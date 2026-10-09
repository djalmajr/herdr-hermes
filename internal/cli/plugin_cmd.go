package cli

// The Herdr plugin entry points (slice 4b): the one-shot startup and event
// hooks plus the two bridge popup actions. The one-shot hooks always exit 0
// (failures go to friction), so the plugin can never fail the Herdr server;
// the bridge actions behave like the regular commands. No second copy of
// the outbox or push logic lives here — startup and bridge sync reuse the
// sync run, and bridge status reads the state read-only.

import (
	"context"
	"fmt"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/notify"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/plugin"
)

// pluginEventAgentStatus is the Herdr event name of the pane agent
// status change hook.
const pluginEventAgentStatus = "pane.agent_status_changed"

// The Herdr event names of the workspace hooks.
const (
	pluginEventWorkspaceCreated = "workspace.created"
	pluginEventWorkspaceClosed  = "workspace.closed"
)

// cmdPlugin dispatches the plugin subcommands.
func cmdPlugin(args []string, env Env) int {
	if len(args) == 0 {
		badUsage(env, "usage: plugin startup|event|bridge status|bridge sync")
		return 2
	}
	switch args[0] {
	case "startup":
		if len(args) != 1 {
			badUsage(env, "usage: plugin startup")
			return 2
		}
		return pluginStartup(env)
	case "event":
		if len(args) != 1 {
			badUsage(env, "usage: plugin event")
			return 2
		}
		return pluginEvent(env)
	case "bridge":
		return cmdPluginBridge(args[1:], env)
	default:
		badUsage(env, "unknown plugin subcommand "+quote(args[0]))
		return 2
	}
}

func cmdPluginBridge(args []string, env Env) int {
	if len(args) != 1 {
		badUsage(env, "usage: plugin bridge status|sync")
		return 2
	}
	switch args[0] {
	case "status":
		return pluginBridgeStatus(env)
	case "sync":
		// The bridge sync runs the full sync (writes plus push), so it is
		// refused under NOWRITE like every writing command.
		if env.Getenv(nowriteVar) == "1" {
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
			return 2
		}
		return pluginBridgeSync(env)
	default:
		badUsage(env, "unknown plugin bridge subcommand "+quote(args[0]))
		return 2
	}
}

// pluginStartup runs the sync logic for every open tracked job plus the
// push step and always exits 0: a config/capability/machine-label failure
// writes one friction line and prints the skipped line; a push failure is a
// normal sync outcome (the counts line carries its status).
func pluginStartup(env Env) int {
	if env.Getenv(nowriteVar) == "1" {
		printPluginSkipped(env, nowriteMotivo)
		return 0
	}
	res, code := runSync(context.Background(), env, "", false)
	if code == 2 || code == 43 {
		pluginFriction(env, "startup: "+res.ErrMotivo)
		// runSync did not reach the notify step (capability or state
		// failure): deliver the pending notifications anyway (30 s
		// bound); the output line and exit code are unchanged.
		if cfg, cerr := config.Load(env.ConfigDir); cerr == nil {
			runNotify(context.Background(), env, cfg, 30*time.Second)
		}
		printPluginSkipped(env, res.ErrMotivo)
		return 0
	}
	printSyncLine(env, res)
	return 0
}

// pluginFriction records one friction line in the plugin's own write area,
// creating the state dir when needed; best effort (the state dir may be
// unwritable, in which case the line is lost, as with every friction line).
func pluginFriction(env Env, msg string) {
	dir := outbox.StateDir(env.ConfigDir)
	if _, err := outbox.Open(dir, outbox.Options{Now: env.Now}); err == nil {
		outbox.Friction(dir, "plugin", msg)
	}
}

// pluginEvent handles the workspace.created and workspace.closed Herdr
// events for workspaces labeled job-<id> and always exits 0: created
// tracks the job (empty projeto) when unknown and runs the sync --job
// logic; closed runs a final sync for a tracked job. Other labels, other
// event names and unknown or empty JSON shapes are ignored (an unknown or
// empty shape writes one friction line).
func pluginEvent(env Env) int {
	if env.Getenv(nowriteVar) == "1" {
		printPluginSkipped(env, nowriteMotivo)
		return 0
	}
	eventName := env.Getenv(plugin.EnvEvent)
	switch eventName {
	case pluginEventAgentStatus:
		pluginAgentStatusEvent(env)
	case pluginEventWorkspaceCreated, pluginEventWorkspaceClosed:
		pluginWorkspaceEvent(env, eventName)
	default:
		// Other event names: nothing written, but an unreadable payload
		// still leaves one friction line, as before.
		if _, _, ok := plugin.ParseEvent(env.Getenv(plugin.EnvEventJSON)); !ok {
			pluginFriction(env, "event: unknown event JSON shape")
		}
	}
	return 0
}

// pluginWorkspaceEvent handles workspace.created and workspace.closed. A
// job workspace (label job-<id>, the job contract's label) keeps today's
// bookkeeping: created tracks the job (empty projeto) when unknown, and a
// tracked job is synced (on close, a final sync). When notifications are
// enabled, the event is also recorded as a workspace notification when
// the workspace is registered (notify register workspace), is a job
// workspace, or, for a closed event without workspace info, was seen
// opening; it is delivered by the sync's notify step, or within the hook bound when
// the sync does not reach it. Any other workspace writes nothing; an
// unreadable payload writes one friction line.
func pluginWorkspaceEvent(env Env, eventName string) {
	ev, err := notify.ParseWorkspaceEvent([]byte(env.Getenv(plugin.EnvEventJSON)), eventName)
	if err != nil {
		pluginFriction(env, "event: unknown event JSON shape")
		return
	}
	labelJob := workspaceLabelJob(ev)
	syncJob := labelJob
	if syncJob == "" && ev.Event == notify.WorkspaceClosed {
		syncJob = openedWorkspaceJob(env, ev.Workspace)
	}
	tracked := syncJob != "" && isJobTracked(env, syncJob)
	if ev.Event == notify.WorkspaceCreated && labelJob != "" && !tracked {
		if err := trackPluginJob(env, labelJob); err != nil {
			pluginFriction(env, "event: track "+labelJob+": "+err.Error())
			return
		}
		tracked = true
	}
	recorded := recordWorkspaceNotification(env, ev, labelJob)
	if tracked {
		res, code := runSync(context.Background(), env, syncJob, false)
		if code != 0 {
			pluginFriction(env, "event: sync "+syncJob+": "+res.ErrMotivo)
		}
		if code != 2 && code != 43 {
			// The sync reached its notify step and delivered.
			return
		}
	}
	if recorded {
		if cfg, err := config.Load(env.ConfigDir); err == nil {
			runNotify(context.Background(), env, cfg, notifyHookBound)
		}
	}
}

// openedWorkspaceJob returns the job recorded in the notification ledger
// when the workspace opened, or "" (read-only).
func openedWorkspaceJob(env Env, workspace string) string {
	ro, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		return ""
	}
	job, _ := notify.Open(ro, env.Now).WorkspaceJob(workspace)
	return job
}

// recordWorkspaceNotification records the workspace event as a
// notification when notifications are enabled (nothing is written
// otherwise) and reports whether a new notification was created.
func recordWorkspaceNotification(env Env, ev notify.WorkspaceEvent, labelJob string) bool {
	ro, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil || !notify.Open(ro, env.Now).Enabled() {
		return false
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		pluginFriction(env, "event: workspace notification state dir unavailable")
		return false
	}
	sum, err := nstore.IngestWorkspace(ev, labelJob, jobProjeto(env, labelJob), notify.ProjectWorkspace)
	if err != nil {
		pluginFriction(env, "event: workspace notification failed")
		return false
	}
	return sum.Created
}

// jobProjeto returns the projeto of a tracked job, or "" (read-only).
func jobProjeto(env Env, jobID string) string {
	if jobID == "" {
		return ""
	}
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		return ""
	}
	jobs, err := store.LoadJobs()
	if err != nil {
		return ""
	}
	if j, ok := jobs.Jobs[jobID]; ok {
		return j.Projeto
	}
	return ""
}

// pluginAgentStatusEvent ingests the Herdr pane.agent_status_changed
// event and delivers the pending notifications within the hook bound. It
// always exits 0, prints nothing on stdout, and writes nothing (not even
// the state dir) when the pane is not watched or notifications are
// disabled: an over-cap, malformed or non-agent-status JSON is one
// friction line with a fixed text.
func pluginAgentStatusEvent(env Env) {
	raw := env.Getenv(plugin.EnvEventJSON)
	if len(raw) > notifyStdinCap {
		pluginFriction(env, "event: agent status JSON is over the 64 KiB cap")
		return
	}
	ev, err := notify.ParseAgentStatusEvent([]byte(raw))
	if err != nil {
		pluginFriction(env, "event: agent status JSON is not a valid agent status event")
		return
	}
	// Disabled (no registry): nothing is written at all (not even the
	// state dir), so the enabled check runs against the read-only store.
	dir := outbox.StateDir(env.ConfigDir)
	ro, err := outbox.Open(dir, outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		pluginFriction(env, "event: agent status state dir unavailable")
		return
	}
	if !notify.Open(ro, env.Now).Enabled() {
		return
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		pluginFriction(env, "event: agent status config unreadable")
		return
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		pluginFriction(env, "event: agent status state dir unavailable")
		return
	}
	if _, err := nstore.IngestAgentStatus(ev, notify.ProjectAgentStatus); err != nil {
		pluginFriction(env, "event: agent status ingest failed")
		return
	}
	runNotify(context.Background(), env, cfg, notifyHookBound)
}

// pluginBridgeStatus is the read-only popup action: exactly one JSON line
// (and nothing else), no writes — it works under HERDR_HERMES_NOWRITE=1
// and creates nothing.
func pluginBridgeStatus(env Env) int {
	jobsAbertos, pendentes := pluginBridgeCounts(env)
	var lastPush *outbox.PushResult
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err == nil {
		if lp, lerr := store.LastPush(); lerr == nil {
			lastPush = lp
		}
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		JobsAbertos   int                `json:"jobs_abertos"`
		Pendentes     int                `json:"pendentes"`
		LastPush      *outbox.PushResult `json:"ultimo_push"`
		KeyConfigured bool               `json:"key_configured"`
	}{jobsAbertos, pendentes, lastPush, keyConfigured(env)}))
	return 0
}

// pluginBridgeSync says what it will do (open tracked jobs and pending
// records) on stderr, then runs the sync and returns the sync exit code.
func pluginBridgeSync(env Env) int {
	jobs, pending := pluginBridgeCounts(env)
	_, _ = fmt.Fprintf(env.Stderr, "herdr-hermes: plugin bridge sync: %d open job(s), %d pending record(s)\n", jobs, pending)
	res, code := runSync(context.Background(), env, "", false)
	printSyncOutcome(env, res, code)
	return code
}

// pluginBridgeCounts reads the open-job and pending-record counts
// read-only (zero on unreadable state), for the bridge lines.
func pluginBridgeCounts(env Env) (jobs, pending int) {
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		return 0, 0
	}
	if js, err := store.LoadJobs(); err == nil {
		for _, j := range js.Jobs {
			if !j.Closed && !isSyncTerminal(j.Estado) {
				jobs++
			}
		}
	}
	if _, last, err := store.Read(0); err == nil {
		delivered, derr := store.DeliveredSeq()
		if derr != nil {
			delivered = 0
		}
		pending = int(last - delivered)
		if pending < 0 {
			pending = 0
		}
	}
	return jobs, pending
}

// trackPluginJob adds the job to the local bookkeeping when it is not
// there yet (empty projeto) and is a no-op when it is.
func trackPluginJob(env Env, jobID string) error {
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		return err
	}
	return store.UpdateJobs(func(js *outbox.Jobs) error {
		if _, ok := js.Jobs[jobID]; !ok {
			ts := env.Now().Format(outbox.TSLayout)
			js.Jobs[jobID] = &outbox.Job{ID: jobID, CreatedAt: ts, UpdatedAt: ts}
		}
		return nil
	})
}

// isJobTracked reports whether the job is in the local bookkeeping
// (read-only).
func isJobTracked(env Env, jobID string) bool {
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		return false
	}
	jobs, err := store.LoadJobs()
	if err != nil {
		return false
	}
	_, ok := jobs.Jobs[jobID]
	return ok
}

// printPluginSkipped prints the plugin skipped line.
func printPluginSkipped(env Env, motivo string) {
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Status string `json:"status"`
		Motivo string `json:"motivo"`
	}{"skipped", motivo}))
}
