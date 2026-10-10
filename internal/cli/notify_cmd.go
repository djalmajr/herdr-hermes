package cli

// The notify command (slice 5): the registration subcommands, the
// read-only list and status, the deliver/ingest/raise/ack writing
// subcommands and runNotify — the one place that composes the notify
// package with the herdr-soho send sender. The one-shot wake, sync and
// plugin hooks call runNotify between their existing steps; their
// output lines and exit codes are unchanged.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/djalmajr/herdr-hermes/internal/config"
	"github.com/djalmajr/herdr-hermes/internal/jobapi"
	"github.com/djalmajr/herdr-hermes/internal/notify"
	"github.com/djalmajr/herdr-hermes/internal/outbox"
	"github.com/djalmajr/herdr-hermes/internal/plugin"
	"github.com/djalmajr/herdr-hermes/internal/soho"
)

// notifyStdinCap is the agent-status ingest stdin cap: 64 KiB, the same
// bound the contract gives the `job amend` body.
const notifyStdinCap = jobapi.StdinCapAmend

// notifySendTimeout is the per-send herdr-soho timeout of runNotify.
const notifySendTimeout = 5 * time.Second

// notifySendMin is the floor of the per-send timeout clipped to the
// remaining bound.
const notifySendMin = time.Second

// notifyHookBound bounds the delivery run of the one-shot paths (wake, the
// plugin event hooks, ingest, raise, retry). The notification is durable
// before any send; the bound only limits how long the hook waits for the
// endpoints. herdr-soho send to a busy recipient answers "queued" only
// after observing the receipt, measured at about 15 s, so a shorter bound
// would kill such sends in flight and leave them uncertain.
const notifyHookBound = 20 * time.Second

// notifyDeliverTimeoutMS are the `notify deliver --timeout` bounds.
const (
	notifyDeliverDefaultTimeoutMS = 30000
	notifyDeliverMinTimeoutMS     = 1
	notifyDeliverMaxTimeoutMS     = 600000
)

// The fixed friction texts of runNotify: never event content.
const (
	frictionNotifyOpen      = "state dir unavailable"
	frictionNotifyProject   = "job event projection failed"
	frictionNotifyMalformed = "job event records could not be projected"
	frictionNotifyDeliver   = "notification delivery failed"
)

// notifyRunResult is the outcome of one runNotify step. OpenErr is set
// when the state dir could not be opened for writing: the wake, sync
// and plugin callers ignore it (the failure is already a friction line)
// and `notify deliver` exits 2.
type notifyRunResult struct {
	Enabled    bool
	OpenErr    error
	Created    int
	Duplicates int
	Malformed  int
	Claimed    int
	Accepted   int
	Uncertain  int
	Rejected   int
	Retrying   int
	Exhausted  int
}

// runNotify projects the pending outbox job events into notifications and
// delivers the due ones within bound. Disabled (no registry) → zero
// result, nothing written (not even the state dir). Errors and malformed
// counts go to friction (outbox.Friction(dir, "notify", <fixed text>),
// never event content); it never changes the caller's exit code. A
// per-send timeout larger than the remaining bound is clipped to the
// remaining bound (minimum 1 s).
func runNotify(ctx context.Context, env Env, cfg config.Config, bound time.Duration) notifyRunResult {
	var res notifyRunResult
	start := time.Now()
	dir := outbox.StateDir(env.ConfigDir)
	// The enabled check runs against the read-only store so a disabled
	// notify (no registry) creates nothing at all.
	ro, err := outbox.Open(dir, outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		res.OpenErr = err
		outbox.Friction(dir, "notify", frictionNotifyOpen)
		return res
	}
	if !notify.Open(ro, env.Now).Enabled() {
		return res
	}
	res.Enabled = true
	store, err := outbox.Open(dir, outbox.Options{Now: env.Now})
	if err != nil {
		res.OpenErr = err
		outbox.Friction(dir, "notify", frictionNotifyOpen)
		return res
	}
	nstore := notify.Open(store, env.Now)
	psum, err := nstore.ProjectOutbox(notify.ProjectJobEvent)
	if err != nil {
		outbox.Friction(dir, "notify", frictionNotifyProject)
		return res
	}
	res.Created, res.Duplicates, res.Malformed = psum.Created, psum.Duplicates, psum.Malformed
	if psum.Malformed > 0 {
		outbox.Friction(dir, "notify", frictionNotifyMalformed)
	}
	senderTimeout := notifySendTimeout
	if remaining := bound - time.Since(start); remaining < senderTimeout {
		senderTimeout = remaining
	}
	if senderTimeout < notifySendMin {
		senderTimeout = notifySendMin
	}
	dctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	// The send process may use the whole remaining budget: herdr-soho can
	// take longer than its --timeout to observe the receipt on a busy
	// target, and a send killed in flight is uncertain (never resent).
	sendBound := bound - time.Since(start)
	if sendBound < senderTimeout+3*time.Second {
		sendBound = senderTimeout + 3*time.Second
	}
	sender := soho.NotifySender{Bin: cfg.HerdrSohoBin, Environ: childEnviron(env), Timeout: senderTimeout, Bound: sendBound}
	dsum, err := notify.Deliver(dctx, nstore, sender, notify.Render, notify.DeliverOptions{})
	if err != nil {
		outbox.Friction(dir, "notify", frictionNotifyDeliver)
	}
	res.Claimed, res.Accepted = dsum.Claimed, dsum.Accepted
	res.Uncertain, res.Rejected = dsum.Uncertain, dsum.Rejected
	res.Retrying, res.Exhausted = dsum.Retrying, dsum.Exhausted
	return res
}

// notifyFlags parses the args of one notify subcommand as exactly-once
// `--name value` or `--name=value` flags restricted to allowed (dashed
// names); a repeated flag, an unknown name or a bare `--` fails
// (ok=false). Positional arguments are returned in order. A bare
// trailing `--name` is present with an empty value, like flagValue.
func notifyFlags(args []string, allowed map[string]bool) (map[string]string, []string, bool) {
	values := map[string]string{}
	present := map[string]bool{}
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			positional = append(positional, a)
			continue
		}
		name, value := a, ""
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			name, value = name[:eq], name[eq+1:]
		} else if i+1 < len(args) {
			i++
			value = args[i]
		}
		if !allowed[name] || present[name] {
			return nil, nil, false
		}
		present[name] = true
		values[name] = value
	}
	return values, positional, true
}

// cmdNotify dispatches the notify subcommands. NOWRITE gates per
// subcommand: only `list` and `status` run under HERDR_HERMES_NOWRITE=1
// (read-only); every other subcommand prints the nowrite error and
// exits 2 before any side effect.
func cmdNotify(args []string, env Env) int {
	if len(args) == 0 {
		badUsage(env, "usage: notify register|unregister|list|status|deliver|ingest|raise|ack|retry")
		return 2
	}
	sub := args[0]
	if sub != "list" && sub != "status" && env.Getenv(nowriteVar) == "1" {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", nowriteErrorJSON)
		return 2
	}
	switch sub {
	case "register":
		return cmdNotifyRegister(args[1:], env)
	case "unregister":
		return cmdNotifyUnregister(args[1:], env)
	case "list":
		return cmdNotifyList(env)
	case "status":
		return cmdNotifyStatus(args[1:], env)
	case "deliver":
		return cmdNotifyDeliver(args[1:], env)
	case "ingest":
		return cmdNotifyIngest(args[1:], env)
	case "raise":
		return cmdNotifyRaise(args[1:], env)
	case "ack":
		return cmdNotifyAck(args[1:], env)
	case "retry":
		return cmdNotifyRetry(args[1:], env)
	default:
		badUsage(env, "unknown notify subcommand "+quote(sub))
		return 2
	}
}

// openNotifyWrite opens the notify state for writing; an error means the
// state dir could not be opened.
func openNotifyWrite(env Env) (*notify.Store, error) {
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now})
	if err != nil {
		return nil, err
	}
	return notify.Open(store, env.Now), nil
}

func cmdNotifyRegister(args []string, env Env) int {
	if len(args) == 0 {
		badUsage(env, "usage: notify register owner|orchestrator|coordinator|watch|workspace [flags]")
		return 2
	}
	kind := args[0]
	switch kind {
	case "owner", "orchestrator", "coordinator", "watch", "workspace":
	default:
		badUsage(env, "unknown notify register kind "+quote(kind))
		return 2
	}
	allowed := map[string]bool{}
	switch kind {
	case "owner":
		allowed["--projeto"], allowed["--to"] = true, true
	case "orchestrator":
		allowed["--job"], allowed["--to"] = true, true
	case "coordinator":
		allowed["--to"] = true
	case "watch":
		allowed["--pane"], allowed["--job"], allowed["--projeto"] = true, true, true
	case "workspace":
		allowed["--workspace"], allowed["--job"], allowed["--projeto"] = true, true, true
	}
	values, positional, ok := notifyFlags(args[1:], allowed)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: notify register "+kind+" [flags]")
		return 2
	}
	if motivo := validateNotifyRegister(kind, values); motivo != "" {
		badUsage(env, "notify register "+kind+": "+motivo)
		return 2
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if err := nstore.UpdateRegistry(func(reg *notify.Registry) error {
		switch kind {
		case "owner":
			reg.Owners[values["--projeto"]] = values["--to"]
		case "orchestrator":
			reg.Orchestrators[values["--job"]] = values["--to"]
		case "coordinator":
			reg.Coordinator = values["--to"]
		case "watch":
			w := notify.Watch{Pane: values["--pane"]}
			if v := values["--job"]; v != "" {
				w.Job = v
			}
			if v := values["--projeto"]; v != "" {
				w.Projeto = v
			}
			reg.Watches[values["--pane"]] = w
		case "workspace":
			if reg.WorkspaceWatches == nil {
				reg.WorkspaceWatches = map[string]notify.WorkspaceWatch{}
			}
			reg.WorkspaceWatches[values["--workspace"]] = notify.WorkspaceWatch{
				Workspace: values["--workspace"], Job: values["--job"], Projeto: values["--projeto"],
			}
		}
		return nil
	}); err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	switch kind {
	case "owner":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Registered string `json:"registered"`
			Projeto    string `json:"projeto"`
			To         string `json:"to"`
		}{kind, values["--projeto"], values["--to"]}))
	case "orchestrator":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Registered string `json:"registered"`
			Job        string `json:"job"`
			To         string `json:"to"`
		}{kind, values["--job"], values["--to"]}))
	case "coordinator":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Registered string `json:"registered"`
			To         string `json:"to"`
		}{kind, values["--to"]}))
	case "watch":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Registered string `json:"registered"`
			Pane       string `json:"pane"`
			Job        string `json:"job,omitempty"`
			Projeto    string `json:"projeto,omitempty"`
		}{kind, values["--pane"], values["--job"], values["--projeto"]}))
	case "workspace":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Registered string `json:"registered"`
			Workspace  string `json:"workspace"`
			Job        string `json:"job,omitempty"`
			Projeto    string `json:"projeto,omitempty"`
		}{kind, values["--workspace"], values["--job"], values["--projeto"]}))
	}
	return 0
}

// validateNotifyRegister checks the flag values of one register call;
// it returns the motivo when a value is missing or invalid.
func validateNotifyRegister(kind string, v map[string]string) string {
	need := func(name string) string {
		if v[name] == "" {
			return "missing " + name
		}
		return ""
	}
	switch kind {
	case "owner":
		if m := need("--projeto"); m != "" {
			return m
		}
		if m := need("--to"); m != "" {
			return m
		}
		if !jobapi.ValidRepo(v["--projeto"]) {
			return "--projeto does not match the repo pattern"
		}
		if !notify.ValidRef(v["--to"]) {
			return "--to does not match a send reference or an agent name"
		}
	case "orchestrator":
		if m := need("--job"); m != "" {
			return m
		}
		if m := need("--to"); m != "" {
			return m
		}
		if !jobapi.ValidID(v["--job"]) {
			return "--job does not match the job id pattern"
		}
		if !notify.ValidRef(v["--to"]) {
			return "--to does not match a send reference or an agent name"
		}
	case "coordinator":
		if m := need("--to"); m != "" {
			return m
		}
		if !notify.ValidRef(v["--to"]) {
			return "--to does not match a send reference or an agent name"
		}
	case "watch":
		if m := need("--pane"); m != "" {
			return m
		}
		if !notify.PanePattern.MatchString(v["--pane"]) {
			return "--pane does not match the pane pattern"
		}
		if v["--job"] != "" && !jobapi.ValidID(v["--job"]) {
			return "--job does not match the job id pattern"
		}
		if v["--projeto"] != "" && !jobapi.ValidRepo(v["--projeto"]) {
			return "--projeto does not match the repo pattern"
		}
	case "workspace":
		if m := need("--workspace"); m != "" {
			return m
		}
		if !notify.WorkspacePattern.MatchString(v["--workspace"]) {
			return "--workspace does not match the workspace pattern"
		}
		if v["--job"] != "" && !jobapi.ValidID(v["--job"]) {
			return "--job does not match the job id pattern"
		}
		if v["--projeto"] != "" && !jobapi.ValidRepo(v["--projeto"]) {
			return "--projeto does not match the repo pattern"
		}
	}
	return ""
}

func cmdNotifyUnregister(args []string, env Env) int {
	if len(args) == 0 {
		badUsage(env, "usage: notify unregister owner|orchestrator|coordinator|watch|workspace [flags]")
		return 2
	}
	kind := args[0]
	allowed := map[string]bool{}
	switch kind {
	case "owner":
		allowed["--projeto"] = true
	case "orchestrator":
		allowed["--job"] = true
	case "coordinator":
		// No flags.
	case "watch":
		allowed["--pane"] = true
	case "workspace":
		allowed["--workspace"] = true
	default:
		badUsage(env, "unknown notify unregister kind "+quote(kind))
		return 2
	}
	values, positional, ok := notifyFlags(args[1:], allowed)
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: notify unregister "+kind+" [flags]")
		return 2
	}
	var motivo string
	var key string
	switch kind {
	case "owner":
		key = values["--projeto"]
		motivo = validateNotifyKey("--projeto", key, jobapi.ValidRepo)
	case "orchestrator":
		key = values["--job"]
		motivo = validateNotifyKey("--job", key, jobapi.ValidID)
	case "watch":
		key = values["--pane"]
		motivo = validateNotifyKey("--pane", key, func(v string) bool { return notify.PanePattern.MatchString(v) })
	case "workspace":
		key = values["--workspace"]
		motivo = validateNotifyKey("--workspace", key, func(v string) bool { return notify.WorkspacePattern.MatchString(v) })
	}
	if motivo != "" {
		badUsage(env, "notify unregister "+kind+": "+motivo)
		return 2
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	var removed bool
	if err := nstore.UpdateRegistry(func(reg *notify.Registry) error {
		switch kind {
		case "owner":
			_, removed = reg.Owners[key]
			delete(reg.Owners, key)
		case "orchestrator":
			_, removed = reg.Orchestrators[key]
			delete(reg.Orchestrators, key)
		case "coordinator":
			removed = reg.Coordinator != ""
			reg.Coordinator = ""
		case "watch":
			_, removed = reg.Watches[key]
			delete(reg.Watches, key)
		case "workspace":
			_, removed = reg.WorkspaceWatches[key]
			delete(reg.WorkspaceWatches, key)
		}
		return nil
	}); err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	switch kind {
	case "owner":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Unregistered string `json:"unregistered"`
			Projeto      string `json:"projeto"`
			Removed      bool   `json:"removed"`
		}{kind, key, removed}))
	case "orchestrator":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Unregistered string `json:"unregistered"`
			Job          string `json:"job"`
			Removed      bool   `json:"removed"`
		}{kind, key, removed}))
	case "coordinator":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Unregistered string `json:"unregistered"`
			Removed      bool   `json:"removed"`
		}{kind, removed}))
	case "watch":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Unregistered string `json:"unregistered"`
			Pane         string `json:"pane"`
			Removed      bool   `json:"removed"`
		}{kind, key, removed}))
	case "workspace":
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Unregistered string `json:"unregistered"`
			Workspace    string `json:"workspace"`
			Removed      bool   `json:"removed"`
		}{kind, key, removed}))
	}
	return 0
}

// validateNotifyKey checks one unregister key; an absent key is a
// missing-flag error.
func validateNotifyKey(name, key string, valid func(string) bool) string {
	if key == "" {
		return "missing " + name
	}
	if !valid(key) {
		return name + " does not match the expected pattern"
	}
	return ""
}

// cmdNotifyList prints the registrations and the notification and
// delivery counts read-only: no file is created.
func cmdNotifyList(env Env) int {
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	nstore := notify.Open(store, env.Now)
	reg, err := nstore.LoadRegistry()
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	led, err := nstore.LoadLedger()
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	pending := 0
	for i := range led.Notifications {
		for j := range led.Notifications[i].Deliveries {
			switch led.Notifications[i].Deliveries[j].State {
			case notify.StatePending, notify.StateInFlight:
				pending++
			}
		}
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Enabled       bool                             `json:"enabled"`
		Owners        map[string]string                `json:"owners"`
		Orchestrators map[string]string                `json:"orchestrators"`
		Coordinator   string                           `json:"coordinator"`
		Watches       map[string]notify.Watch          `json:"watches"`
		Workspaces    map[string]notify.WorkspaceWatch `json:"workspace_watches,omitempty"`
		ProjectedSeq  int64                            `json:"projected_seq"`
		Notifications int                              `json:"notifications"`
		Pending       int                              `json:"pending"`
	}{nstore.Enabled(), reg.Owners, reg.Orchestrators, reg.Coordinator, reg.Watches, reg.WorkspaceWatches,
		led.ProjectedSeq, len(led.Notifications), pending}))
	return 0
}

// statusDelivery is one delivery of the status output: the stored fields
// plus the latency_ms keys whose two timestamps exist.
type statusDelivery struct {
	Roles          []string         `json:"roles"`
	Ref            string           `json:"ref,omitempty"`
	State          string           `json:"state"`
	Attempts       int              `json:"attempts"`
	NextAttemptAt  string           `json:"next_attempt_at,omitempty"`
	LeaseUntil     string           `json:"lease_until,omitempty"`
	FirstAttemptAt string           `json:"first_attempt_at,omitempty"`
	LastAttemptAt  string           `json:"last_attempt_at,omitempty"`
	AcceptedAt     string           `json:"accepted_at,omitempty"`
	AcceptStatus   string           `json:"accept_status,omitempty"`
	LastExit       *int             `json:"last_exit,omitempty"`
	LastError      string           `json:"last_error,omitempty"`
	AckAt          string           `json:"ack_at,omitempty"`
	LatencyMS      map[string]int64 `json:"latency_ms,omitempty"`
}

// statusNotification mirrors the stored notification form.
type statusNotification struct {
	ID          string           `json:"id"`
	SourceKind  string           `json:"source_kind"`
	SourceID    string           `json:"source_id"`
	Class       string           `json:"class"`
	JobID       string           `json:"job_id,omitempty"`
	Projeto     string           `json:"projeto,omitempty"`
	EventTipo   string           `json:"event_tipo,omitempty"`
	EventSeq    int64            `json:"event_seq,omitempty"`
	Exit        *int             `json:"exit,omitempty"`
	Pane        string           `json:"pane,omitempty"`
	Workspace   string           `json:"workspace,omitempty"`
	Status      string           `json:"status,omitempty"`
	Escalations []string         `json:"escalations,omitempty"`
	SourceTS    string           `json:"source_ts,omitempty"`
	ObservedAt  string           `json:"observed_at"`
	PersistedAt string           `json:"persisted_at"`
	Deliveries  []statusDelivery `json:"deliveries"`
}

// cmdNotifyStatus prints the last 50 notifications in ledger order (or
// exactly the one --id) read-only; an unknown id exits 3.
func cmdNotifyStatus(args []string, env Env) int {
	values, positional, ok := notifyFlags(args, map[string]bool{"--id": true})
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: notify status [--id <nid>]")
		return 2
	}
	store, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	led, err := notify.Open(store, env.Now).LoadLedger()
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	var nlist []*notify.Notification
	if id, present := values["--id"]; present {
		var found *notify.Notification
		for i := range led.Notifications {
			if led.Notifications[i].ID == id {
				found = led.Notifications[i]
				break
			}
		}
		if found == nil {
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", `{"status":"not_found"}`)
			return 3
		}
		nlist = []*notify.Notification{found}
	} else if len(led.Notifications) > 50 {
		nlist = led.Notifications[len(led.Notifications)-50:]
	} else {
		nlist = led.Notifications
	}
	out := make([]statusNotification, 0, len(nlist))
	for _, n := range nlist {
		var sn statusNotification
		sn.ID, sn.SourceKind, sn.SourceID, sn.Class = n.ID, n.SourceKind, n.SourceID, n.Class
		sn.JobID, sn.Projeto, sn.EventTipo, sn.EventSeq = n.JobID, n.Projeto, n.EventTipo, n.EventSeq
		sn.Exit, sn.Pane, sn.Workspace, sn.Status = n.Exit, n.Pane, n.Workspace, n.Status
		sn.Escalations, sn.SourceTS = n.Escalations, n.SourceTS
		sn.ObservedAt, sn.PersistedAt = n.ObservedAt, n.PersistedAt
		sn.Deliveries = make([]statusDelivery, 0, len(n.Deliveries))
		for j := range n.Deliveries {
			d := n.Deliveries[j]
			var sd statusDelivery
			sd.Roles, sd.Ref, sd.State, sd.Attempts = d.Roles, d.Ref, d.State, d.Attempts
			sd.NextAttemptAt, sd.LeaseUntil = d.NextAttemptAt, d.LeaseUntil
			sd.FirstAttemptAt, sd.LastAttemptAt = d.FirstAttemptAt, d.LastAttemptAt
			sd.AcceptedAt, sd.AcceptStatus, sd.LastExit = d.AcceptedAt, d.AcceptStatus, d.LastExit
			sd.LastError, sd.AckAt = d.LastError, d.AckAt
			sd.LatencyMS = deliveryLatencyMS(n, d)
			sn.Deliveries = append(sn.Deliveries, sd)
		}
		out = append(out, sn)
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Notifications []statusNotification `json:"notifications"`
	}{out}))
	return 0
}

// deliveryLatencyMS computes the delivery latency keys: only the keys
// whose two timestamps exist are present. The source timestamp is the
// event's own RFC3339 value; the ledger timestamps use the ledger
// layout.
func deliveryLatencyMS(n *notify.Notification, d notify.Delivery) map[string]int64 {
	parseSource := func(ts string) (time.Time, error) { return time.Parse(time.RFC3339, ts) }
	parseLedger := func(ts string) (time.Time, error) { return time.Parse(notify.TSLayout, ts) }
	ms := func(from string, fp, tp func(string) (time.Time, error), to string) (int64, bool) {
		a, errA := fp(from)
		b, errB := tp(to)
		if errA != nil || errB != nil {
			return 0, false
		}
		return int64(b.Sub(a) / time.Millisecond), true
	}
	var out map[string]int64
	add := func(key string, v int64, ok bool) {
		if !ok {
			return
		}
		if out == nil {
			out = map[string]int64{}
		}
		out[key] = v
	}
	v, ok := ms(n.SourceTS, parseSource, parseLedger, n.PersistedAt)
	add("source_to_persist", v, ok)
	v, ok = ms(n.ObservedAt, parseLedger, parseLedger, n.PersistedAt)
	add("observed_to_persist", v, ok)
	v, ok = ms(n.PersistedAt, parseLedger, parseLedger, d.FirstAttemptAt)
	add("persist_to_first_attempt", v, ok)
	v, ok = ms(n.PersistedAt, parseLedger, parseLedger, d.AcceptedAt)
	add("persist_to_accept", v, ok)
	v, ok = ms(n.SourceTS, parseSource, parseLedger, d.AcceptedAt)
	add("source_to_accept", v, ok)
	return out
}

// parseNotifyTimeoutMS parses a deliver timeout in milliseconds: an
// integer of 1..600000.
func parseNotifyTimeoutMS(s string) (int64, error) {
	if s == "" {
		return 0, errors.New("empty")
	}
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errors.New("not a non-negative integer")
		}
		n = n*10 + int64(r-'0')
		if n > notifyDeliverMaxTimeoutMS {
			return 0, errors.New("exceeds the limit")
		}
	}
	if n < notifyDeliverMinTimeoutMS {
		return 0, errors.New("below the minimum")
	}
	return n, nil
}

// cmdNotifyDeliver runs one projection and delivery within the bound.
// Exit 2 only for bad usage or an unopenable state dir.
func cmdNotifyDeliver(args []string, env Env) int {
	values, positional, ok := notifyFlags(args, map[string]bool{"--timeout": true})
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: notify deliver [--timeout <ms>]")
		return 2
	}
	timeout := int64(notifyDeliverDefaultTimeoutMS)
	if v, present := values["--timeout"]; present {
		n, err := parseNotifyTimeoutMS(v)
		if err != nil {
			badUsage(env, "notify deliver: --timeout must be an integer of 1..600000 milliseconds")
			return 2
		}
		timeout = n
	}
	// An unopenable state dir (it exists but is not a directory) is a
	// usage-environment error; a missing one is fine and is created only
	// when notify is enabled, by the writing step below.
	if info, err := os.Lstat(outbox.StateDir(env.ConfigDir)); err == nil && !info.IsDir() {
		fail(env, 2, "notify: state dir is not a directory")
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	res := runNotify(context.Background(), env, cfg, time.Duration(timeout)*time.Millisecond)
	if res.OpenErr != nil {
		fail(env, 2, "notify: "+res.OpenErr.Error())
		return 2
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Enabled    bool `json:"enabled"`
		Created    int  `json:"created"`
		Duplicates int  `json:"duplicates"`
		Malformed  int  `json:"malformed"`
		Claimed    int  `json:"claimed"`
		Accepted   int  `json:"accepted"`
		Uncertain  int  `json:"uncertain"`
		Rejected   int  `json:"rejected"`
		Retrying   int  `json:"retrying"`
		Exhausted  int  `json:"exhausted"`
	}{res.Enabled, res.Created, res.Duplicates, res.Malformed, res.Claimed,
		res.Accepted, res.Uncertain, res.Rejected, res.Retrying, res.Exhausted}))
	return 0
}

// cmdNotifyIngest ingests one Herdr agent status event from stdin (the
// 64 KiB cap) and delivers within the hook bound.
func cmdNotifyIngest(args []string, env Env) int {
	if len(args) == 1 && args[0] == "workspace" {
		return cmdNotifyIngestWorkspace(env)
	}
	if len(args) != 1 || args[0] != "agent-status" {
		badUsage(env, "usage: notify ingest agent-status|workspace")
		return 2
	}
	data, err := readCapped(env.Stdin, notifyStdinCap)
	if errors.Is(err, errInputOverCap) {
		fail(env, 2, "ingest: stdin is over the 64 KiB cap")
		return 2
	}
	if err != nil {
		fail(env, 2, "ingest: reading stdin: "+err.Error())
		return 2
	}
	ev, perr := notify.ParseAgentStatusEvent(data)
	if perr != nil {
		if errors.Is(perr, notify.ErrNotAgentStatus) {
			_, _ = fmt.Fprintf(env.Stdout, "%s\n", `{"ignored":true,"reason":"not_agent_status"}`)
			return 0
		}
		fail(env, 2, "ingest: "+perr.Error())
		return 2
	}
	// Disabled (no registry): nothing is written (not even the state
	// dir), so the enabled check runs against the read-only store.
	dir := outbox.StateDir(env.ConfigDir)
	ro, err := outbox.Open(dir, outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if !notify.Open(ro, env.Now).Enabled() {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n",
			`{"ignored":true,"duplicate":false,"routine":false,"created":false,"notification":"","n":0,"accepted":0}`)
		return 0
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	sum, err := nstore.IngestAgentStatus(ev, notify.ProjectAgentStatus)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	res := runNotify(context.Background(), env, cfg, notifyHookBound)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Ignored      bool   `json:"ignored"`
		Duplicate    bool   `json:"duplicate"`
		Routine      bool   `json:"routine"`
		Created      bool   `json:"created"`
		Notification string `json:"notification"`
		N            int64  `json:"n"`
		Accepted     int    `json:"accepted"`
	}{sum.Ignored, sum.Duplicate, sum.Routine, sum.Created, sum.NotificationID, sum.N, res.Accepted}))
	return 0
}

// cmdNotifyRaise raises an explicit cross_project or stuck
// notification and delivers within the hook bound; the same --id replayed
// returns the stored notification with created=false.
func cmdNotifyRaise(args []string, env Env) int {
	values, positional, ok := notifyFlags(args, map[string]bool{"--class": true, "--projeto": true, "--job": true, "--id": true})
	if !ok || len(positional) != 0 {
		badUsage(env, "usage: notify raise --class cross_project|stuck [--projeto <org/repo>] [--job <id>] [--id <raise-id>]")
		return 2
	}
	class := values["--class"]
	if class != notify.ClassCrossProject && class != notify.ClassStuck {
		badUsage(env, "notify raise: --class must be cross_project or stuck")
		return 2
	}
	if v := values["--projeto"]; v != "" && !jobapi.ValidRepo(v) {
		badUsage(env, "notify raise: --projeto does not match the repo pattern")
		return 2
	}
	if v := values["--job"]; v != "" && !jobapi.ValidID(v) {
		badUsage(env, "notify raise: --job does not match the job id pattern")
		return 2
	}
	rid := values["--id"]
	if rid == "" {
		g, err := notify.NewRaiseID(rand.Reader)
		if err != nil {
			fail(env, 2, "notify: "+err.Error())
			return 2
		}
		rid = g
	} else if !notify.RaiseIDPattern.MatchString(rid) {
		badUsage(env, "notify raise: --id does not match the raise id pattern")
		return 2
	}
	// Not enabled: exit 2 with the ErrNotEnabled text, nothing written.
	dir := outbox.StateDir(env.ConfigDir)
	ro, err := outbox.Open(dir, outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if !notify.Open(ro, env.Now).Enabled() {
		fail(env, 2, notify.ErrNotEnabled.Error())
		return 2
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	in := notify.RaiseInput{ID: rid, Class: class}
	if v := values["--projeto"]; v != "" {
		in.Projeto = v
	}
	if v := values["--job"]; v != "" {
		in.JobID = v
	}
	n, created, err := nstore.Raise(in, notify.ProjectRaise)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	res := runNotify(context.Background(), env, cfg, notifyHookBound)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Notification string `json:"notification"`
		RaiseID      string `json:"raise_id"`
		Created      bool   `json:"created"`
		Accepted     int    `json:"accepted"`
	}{n.ID, rid, created, res.Accepted}))
	return 0
}

// cmdNotifyAck records the optional recipient processing acknowledgement
// on the delivery of <nid> that holds the role; not found exits 3.
func cmdNotifyAck(args []string, env Env) int {
	values, positional, ok := notifyFlags(args, map[string]bool{"--role": true})
	if !ok || len(positional) != 1 {
		badUsage(env, "usage: notify ack <nid> --role owner|orchestrator|coordinator")
		return 2
	}
	role := values["--role"]
	if role != notify.RoleOwner && role != notify.RoleOrchestrator && role != notify.RoleCoordinator {
		badUsage(env, "notify ack: --role must be owner, orchestrator or coordinator")
		return 2
	}
	// Disabled (no registry): nothing to acknowledge and nothing is
	// written (not even the state dir), so the enabled check runs
	// against the read-only store.
	ro, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if !notify.Open(ro, env.Now).Enabled() {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", `{"status":"not_found"}`)
		return 3
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	found, err := nstore.Ack(positional[0], role)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if !found {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", `{"status":"not_found"}`)
		return 3
	}
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", `{"acked":true}`)
	return 0
}

// cmdNotifyIngestWorkspace ingests one Herdr workspace.created or
// workspace.closed event from stdin and delivers within the hook bound. The
// workspace is a source when it is registered (notify register
// workspace), when its label is job-<id> (the job contract's workspace
// label), or, for a closed event without workspace info, when it was seen
// opening. Another event, any other workspace and disabled notifications
// are ignored with nothing written.
func cmdNotifyIngestWorkspace(env Env) int {
	data, err := readCapped(env.Stdin, notifyStdinCap)
	if errors.Is(err, errInputOverCap) {
		fail(env, 2, "ingest: stdin is over the 64 KiB cap")
		return 2
	}
	if err != nil {
		fail(env, 2, "ingest: reading stdin: "+err.Error())
		return 2
	}
	ignored := func(reason string) int {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
			Ignored bool   `json:"ignored"`
			Reason  string `json:"reason"`
		}{true, reason}))
		return 0
	}
	ev, perr := notify.ParseWorkspaceEvent(data, "")
	if perr != nil {
		if errors.Is(perr, notify.ErrNotWorkspace) {
			return ignored("not_workspace")
		}
		fail(env, 2, "ingest: workspace event is malformed")
		return 2
	}
	ro, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if !notify.Open(ro, env.Now).Enabled() {
		return ignored("disabled")
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	labelJob := workspaceLabelJob(ev)
	sum, err := nstore.IngestWorkspace(ev, labelJob, jobProjeto(env, labelJob), notify.ProjectWorkspace)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if sum.Ignored {
		return ignored("unregistered_workspace")
	}
	res := runNotify(context.Background(), env, cfg, notifyHookBound)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Ignored      bool   `json:"ignored"`
		Duplicate    bool   `json:"duplicate"`
		Created      bool   `json:"created"`
		Notification string `json:"notification"`
		N            int64  `json:"n"`
		Accepted     int    `json:"accepted"`
	}{false, sum.Duplicate, sum.Created, sum.NotificationID, sum.N, res.Accepted}))
	return 0
}

// workspaceLabelJob returns the job of a job-<id> workspace label, or "".
func workspaceLabelJob(ev notify.WorkspaceEvent) string {
	if job, ok := plugin.JobIDFromLabel(ev.Label); ok {
		return job
	}
	return ""
}

// cmdNotifyRetry re-queues once the uncertain or exhausted delivery of
// <nid> that holds the role — the explicit decision of someone who checked
// that the recipient did not get the message — and delivers within the
// hook bound; not found exits 3. Notifications disabled: not found, nothing
// written.
func cmdNotifyRetry(args []string, env Env) int {
	values, positional, ok := notifyFlags(args, map[string]bool{"--role": true})
	if !ok || len(positional) != 1 {
		badUsage(env, "usage: notify retry <nid> --role owner|orchestrator|coordinator")
		return 2
	}
	role := values["--role"]
	if role != notify.RoleOwner && role != notify.RoleOrchestrator && role != notify.RoleCoordinator {
		badUsage(env, "notify retry: --role must be owner, orchestrator or coordinator")
		return 2
	}
	ro, err := outbox.Open(outbox.StateDir(env.ConfigDir), outbox.Options{Now: env.Now, ReadOnly: true})
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if !notify.Open(ro, env.Now).Enabled() {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", `{"status":"not_found"}`)
		return 3
	}
	cfg, err := config.Load(env.ConfigDir)
	if err != nil {
		fail(env, 2, "config: "+err.Error())
		return 2
	}
	nstore, err := openNotifyWrite(env)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	found, err := nstore.Retry(positional[0], role)
	if err != nil {
		fail(env, 2, "notify: "+err.Error())
		return 2
	}
	if !found {
		_, _ = fmt.Fprintf(env.Stdout, "%s\n", `{"status":"not_found"}`)
		return 3
	}
	res := runNotify(context.Background(), env, cfg, notifyHookBound)
	_, _ = fmt.Fprintf(env.Stdout, "%s\n", mustJSON(struct {
		Retried  bool `json:"retried"`
		Accepted int  `json:"accepted"`
	}{true, res.Accepted}))
	return 0
}
