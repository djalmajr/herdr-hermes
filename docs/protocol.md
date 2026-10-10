# Protocol

`herdr-hermes` is the bridge between a remote dispatcher and one machine ("node"). This document covers the wire and file formats the node exposes: the outbox record, the pull channel, the push request, the wake hook, the forwarded job commands, the native notification ledger and message, and the security rules for the per-user API key. The job semantics themselves (event types, lifecycle, input limits) are defined by the vendored job contract in `docs/upstream/job-contract.md`; this document never changes them.

## Outbox record

The outbox is an append-only file of one JSON object per line (schema 1), in the state directory of the machine:

```json
{"schema":1,"seq":12,"ts":"2026-01-02T03:04:05-03:00","tipo":"job_event","maquina":"<label>","projeto":"<org>/<repo>","job_id":"<id>","idempotency_key":"<key>","dados":{...}}
```

- `schema` is `1`.
- `seq` is consecutive from 1 with no gaps for the machine. It is assigned under a process-wide advisory lock on a persistent lock file, and the line is written with one `O_APPEND` write followed by `fsync` before the append returns. `seq` is derived from the file itself (last complete line + 1), never from a separate counter, so a crash cannot duplicate or skip a seq; a torn final line is truncated to the last newline and `fsync`ed before the next append.
- `ts` is local time with an explicit offset (for example `2026-01-02T03:04:05-03:00`), never `Z`.
- `tipo` is one of `job_event`, `dispatch`, `amend`, `session`, `decision`.
- `maquina` is the configured machine label; `projeto` is `<org>/<repo>`, or an empty string (the key is always present) when the project is unknown, for example a `job_event` from a job that was not started through `herdr-hermes`.
- `job_id` is the job id or `null`.
- `idempotency_key` is `<job_id>:<event seq>` for `job_event` (the same key the job contract uses) and `<maquina>:<outbox seq>` for every other record.
- `dados` carries the original job event unchanged for `job_event` (unknown fields included, so fields added later by `herdr-soho` are never dropped), or the command payload for the other types.

A `job_event` whose `(job_id, event seq)` is already in the outbox is not appended again: the wake hook, `sync` and push retries converge on the same content. Job events are deduplicated by their idempotency key, and `last_event_seq` is the highest event seq up to which every event of the job is stored, so `sync` refetches any gap.

## Pull channel

`herdr-hermes outbox [--since <seq>] [--wait <ms>]` is read-only and needs no key and no network endpoint. It prints the records with `seq > since` as JSON lines and always ends with the trailer:

```json
{"outbox":"fim","ultimo_seq":N,"entregue_seq":M}
```

When nothing is new, it long-polls until a record with `seq > since` appears or `--wait` elapses (at most 600000 ms). A `--since` beyond the last seq prints only the trailer. It works under `HERDR_HERMES_NOWRITE=1` and writes nothing.

## Push

Push happens only when `dispatcher_url` is configured; otherwise it is disabled (`push_disabled`) and the records stay in the outbox for a pull. Each pending record (those with `seq` greater than the delivery cursor) is sent as one request, in `seq` order, one at a time:

- Method and body: `POST <dispatcher_url>`, the outbox record line as the JSON body.
- Headers, exactly: `Authorization: Bearer <key>`, `Idempotency-Key: <idempotency_key>`, `Content-Type: application/json`, `User-Agent: herdr-hermes/<version>`.
- The key appears only in the `Authorization` header; errors and friction lines never contain it or the request body.

Response handling:

- 2xx — the record is delivered: the delivery cursor (`delivered_seq` in `cursor.json`) advances to its `seq`, and the next record is sent.
- 401/403 — the batch stops, the outcome `auth_rejected` is recorded in friction, and `sync`/`push` exit 41.
- 408, 429, 5xx, network errors and timeouts — the same record is retried at most 3 times (after 10 s, 30 s and 90 s, on the injected clock) for the retrying commands `push` and `sync`; if it still fails the batch stops with the outcome `unreachable`, the record stays pending and the command exits 42. `wake` makes a single attempt with no retries and exits 0 either way.
- 3xx (redirects are never followed) and any other 4xx — the batch stops without retry, the outcome `rejected` is recorded and the command exits 42.

The URL must be HTTPS unless the host is a loopback address (tests); redirects are not followed. Sending is best effort and never affects a job: a failed push leaves the records in the outbox for a later push or pull, and a crash between a successful POST and the cursor update re-sends the same record with the same `Idempotency-Key`, which the receiver deduplicates.

## last_push

`cursor.json` holds the delivery cursor and the last push result:

```json
{"delivered_seq":N,"last_push":{"ts":"2026-01-02T03:04:05-03:00","status":"ok","code":200}}
```

`status` is one of `ok`, `auth_rejected`, `unreachable`, `rejected`. Recording a new push result preserves `delivered_seq`; advancing the cursor preserves the last result. The `plugin bridge status` action and `doctor` read it.

## Wake hook

`herdr-hermes wake` is meant to be configured as the `herdr-soho` machine key `job_wake_cmd=herdr-hermes wake`. When `herdr-soho` wakes the dispatcher for a job event, it runs `wake` with the event JSON on stdin (cap 64 KiB) and the four environment variables `HERDR_SOHO_JOB_ID`, `HERDR_SOHO_JOB_SEQ`, `HERDR_SOHO_JOB_EVENT`, `HERDR_SOHO_JOB_IDEMPOTENCY_KEY`. `wake` appends one `job_event` record (deduplicated by `(job_id, event seq)`), then attempts exactly one push when `dispatcher_url` is set. It exits 0 as soon as the record is durable, whatever the push result, and prints `{"seq":N,"duplicado":bool,"enviados":N,"pendentes":N}` (`seq` is the outbox seq, 0 when the event was already recorded). It exits 2 for malformed input (missing or invalid job id, stdin over the cap, invalid JSON, a missing or mismatched event seq, a mismatched idempotency key, or an empty machine label), and also exits 2 when the record could not be made durable (the state directory or the outbox cannot be opened or written). A job unknown to the local bookkeeping is tracked on the fly, so interactive jobs not started through `herdr-hermes` are covered too. Job events are deduplicated by their idempotency key, and `last_event_seq` is the highest event seq up to which every event of the job is stored, so `sync` refetches any gap.

## Job forwarding

`herdr-hermes job <sub> [args]` forwards the dispatcher-facing job subcommands to `herdr-soho job <sub> [args]` as an argv subprocess (never a shell): `start`, `status`, `wait`, `events`, `collect`, `amend`, `send`, `ack`, `cancel`, `close`, `list`. The internal subcommands `supervise`, `checkpoint` and `note` are refused with exit 2 without running anything.

Forwarding is transparent: stdin is streamed through, stdout and stderr are copied unchanged, and the exit code of `herdr-soho` is returned unchanged. Because the child's lifetime is bounded by the deadline above, the forwarder waits for the delivery of everything the child wrote to reach the consumer's stdout and stderr before it returns: a consumer that stops reading holds the command until it reads again (exactly as it would hold `herdr-soho` run directly), so no byte the child wrote is dropped and no exit code is returned before the delivery ends. Before forwarding, `herdr-hermes` applies the contract input limits so an oversize or malformed input never reaches the subprocess: `--id` must match `^[A-Za-z0-9._-]{1,64}$`, `--repo` is `<org>/<repo>` with each part `^[A-Za-z0-9._-]{1,100}$`, `--base` is `^[A-Za-z0-9._/-]{1,100}$` with no `..`, `wait --timeout` and `events --wait` accept at most 600000 ms, and the stdin caps are 256 KiB for `start`, 64 KiB for `amend` and 16 KiB for `send`; a violation exits 2 without running the subprocess. Every forwarded subprocess runs with a context deadline (the command's own bound plus a margin: for `wait`, the `--timeout` value plus 30 s, or 600000 ms plus 30 s when `--timeout` is absent; for `events`, the `--wait` value plus 30 s, or 120 s when `--wait` is absent; 120 s for the others); a missing or not runnable `herdr-soho` exits 4 with `{"status":"unavailable","motivo":"herdr-soho not found"}`. When the deadline kills the subprocess, `job` exits 4 and, if the subprocess wrote nothing to stdout, prints `{"status":"timeout","motivo":"herdr-soho job <sub> did not finish within <bound>"}`; no bookkeeping happens on that path.

Bookkeeping happens only after a successful forward, never changes the forwarded exit code or stdout, and its failures go to friction: a `start` that exits 0 (or answers with `duplicate_of`) adds the job to the local bookkeeping and appends one `dispatch` record — but a `duplicate_of` response for an already tracked job adds no second job and no second record; `amend` and `send` that exit 0 append one `amend` record each (`dados.refs.tipo` is `amend` or `nota`); a `close` that exits 0 marks the job closed. When the machine label is empty, bookkeeping writes a friction line instead of a record and the forward still succeeds.

## Sync

`herdr-hermes sync [--job <id>] [--push-only]` first checks that `herdr-soho` has the `ephemeral_job` and `job_events` capabilities (skipped with `--push-only`); a missing capability exits 43 with `{"status":"capabilities_missing",…}`, and a `--job` unknown to the local bookkeeping exits 3 with `{"status":"not_found"}`. For each tracked job (or the given one) it runs `herdr-soho job events --id <id> --since <prefix>` without waiting, where `<prefix>` is the contiguous stored prefix recomputed from the outbox (the recorded `last_event_seq` is never the `--since` value), appends the new events in order, updates the job's `estado` from the events trailer and marks the job terminal when the trailer state is a terminal one (`done`, `failed`, `timeout`, `canceled`, `collected`, `closed`); a closed or terminal job is skipped only while it has no gap, and it is still synced while the outbox shows a stored event above the contiguous stored prefix or the recorded `last_event_seq` above the stored records. A per-job events failure goes to friction and the loop continues. Job events are deduplicated by their idempotency key, and `last_event_seq` is the highest event seq up to which every event of the job is stored, so `sync` refetches any gap. It then pushes the pending records and prints:

```json
{"jobs":N,"novos":N,"enviados":N,"pendentes":N}
```

with exit 0, 40, 41 or 42 according to the push outcome; on a non-zero outcome the line carries the counts plus a `status` field.

## Sessions

`herdr-hermes session start|update|end` manages the node's open session records. `--projeto <org>/<repo>` is required (each part `^[A-Za-z0-9._-]{1,100}$`) as is the machine label. `start` takes `--card`, `--branch` and `--job`; a repeated `start` for the same project and branch (or job) updates the stored session and still appends a record (the receiver deduplicates by content). `update` takes `--branch`, `--card`, `--estado` and `--pr`; `end` takes `--estado` and `--pr` and marks the session closed. Every call appends one `session` record whose `dados` is `{"acao":"start|update|end","projeto","branch","card","job","estado","pr"}` (empty fields omitted), with the record's `job_id` set to the session's job when there is one and `--estado` cut at 280 code points. The record never carries a person field; the receiver derives the person from the authenticated key.

## Decisions

`herdr-hermes decision --projeto <org/repo> --escopo global|projeto --motivo <text> [--job <id>] [<resumo>|-]` records a decision taken outside a job (job decisions already travel as `job_event` records). `--escopo` other than `global` or `projeto` exits 2, `--motivo` is required, and the `resumo` comes from the positional argument or from stdin when it is `-` (cap 16 KiB, trailing newline trimmed). `resumo` and `motivo` are cut at 280 code points, by whole code points. It appends one `decision` record whose `dados` is `{"resumo","motivo","escopo"}` and prints `{"seq":N}`.

## Doctor

`herdr-hermes doctor` is read-only (it works under `HERDR_HERMES_NOWRITE=1` and creates no files) and prints one JSON line:

```json
{"herdr_soho":{"found":true,"capabilities":"{...}","ephemeral_job":true,"job_events":true},"machine_label":true,"dispatcher_url":true,"key_configured":false,"wake_hook":"true","outbox":{"ultimo_seq":N,"entregue_seq":M,"pendentes":K}}
```

`capabilities` is the raw `herdr-soho capabilities --json` line or null. `wake_hook` is `true` when the `herdr-soho` configuration carries a `job_wake_cmd` that starts with `herdr-hermes wake`, `false` when a parseable configuration has a different value or none, and `unknown` when the configuration cannot be read or parsed. `doctor` exits 43 when `herdr-soho` is missing or lacks either capability, else 0.

## Plugin

The optional plugin (`plugin/herdr-plugin.toml`) runs the CLI as one-shot entry points; it keeps no second copy of the outbox or push logic, and the CLI works fully without the plugin. Every `command` in the manifest is an argv array whose first element is `herdr-hermes`.

- `herdr-hermes plugin startup` (the `[[startup]]` hook) runs the `sync` logic for every tracked job and then the push step. It always exits 0 and never fails the Herdr server: a config, capability or machine-label failure writes one friction line and prints `{"status":"skipped","motivo":…}`; otherwise it prints the sync counts line (a push failure is a normal sync outcome, with its `status` field).
- `herdr-hermes plugin event` (the `workspace.created`, `workspace.closed` and `pane.agent_status_changed` hooks) reads `HERDR_PLUGIN_EVENT` and `HERDR_PLUGIN_EVENT_JSON` from the environment. A `workspace.created` for a workspace labeled `job-<id>` (id matching the contract id rule) tracks the job in the local bookkeeping when it is unknown (empty `projeto`) and then runs the `sync --job <id>` logic; a `workspace.closed` runs a final `sync --job <id>` for a tracked `job-<id>`, so terminal and cleanup events reach the outbox; a closed event that carries no workspace info (the event-stream form allows `workspace: null`) is resolved to the job recorded when the workspace opened. When notifications are enabled, both hooks also record a workspace notification for a registered workspace or a job workspace (see Notifications). A `pane.agent_status_changed` event is ingested as an agent status source (an unwatched pane writes nothing) and then a delivery run is made with a 20 s bound. The plugin hook payload shape is not documented by Herdr and has not been verified live for any of the three hooks (no plugin was installed to observe it), so the parsers accept the socket subscription line (`{"event":"pane.agent_status_changed","data":{…}}`, `{"event":"workspace_created","data":{…}}`), the event-stream forms of the bundled API schema and the flat or nested payloads; only the socket subscription line of `pane.agent_status_changed` was captured from a live Herdr. Other event names are ignored with nothing written; an unknown or empty `HERDR_PLUGIN_EVENT_JSON` shape writes one friction line. It always exits 0; failures go to friction.
- `herdr-hermes plugin bridge status` (read-only action, exit 0) prints exactly one JSON line and nothing else: `{"jobs_abertos":N,"pendentes":N,"ultimo_push":…,"key_configured":bool}`, where `ultimo_push` is the `last_push` entry of `cursor.json` or `null`. It is read-only: it works under `HERDR_HERMES_NOWRITE=1` and creates no files.
- `herdr-hermes plugin bridge sync` (action) prints to stderr what it will do — the number of open tracked jobs and of pending outbox records — then runs the `sync` logic and prints its JSON line; its exit code is the sync exit code (0, 2, 40, 41, 42, 43). It is a writing command and is refused under `HERDR_HERMES_NOWRITE=1` with exit 2.

## Notifications

Native notifications project this machine's own sources into sanitized one-line messages and deliver them to explicitly registered recipients through `herdr-soho send`. The outbox record schema is not changed and notifications are never added to the outbox: the ledger lives in its own files, and a notification carries only allowlisted identifiers, never a raw event, output, prompt, path or environment value.

**Sources.** Exactly four sources project: the job contract events stored in the outbox (from the `wake` hook or from the `sync` reconciliation), the Herdr `pane.agent_status_changed` events of registered (watched) panes (via `notify ingest agent-status` or the plugin `event` hook), the Herdr `workspace.created` and `workspace.closed` events of a registered workspace or of a job workspace (via `notify ingest workspace` or the plugin `event` hooks), and explicit local raises (`notify raise`). Nothing else projects. [`native-sources.md`](native-sources.md) lists every source with its verification status.

**Mapping.** Job contract events map to a notification class with the escalations that make the machine coordinator a recipient:

| event tipo | class | escalations |
|---|---|---|
| `question` | `question` | — |
| `blocked` | `blocked` | `blocked` |
| `failure` | `failed` | `failed` |
| `terminal` with exit 0, 21, missing or other | `terminal` | — |
| `terminal` with exit 7, 11, 14 | `blocked` | `blocked` |
| `terminal` with exit 9, 19, 22 | `failed` | `failed` |
| `timeout_warning` | `timeout_warning` | — |
| `review_verdict` | `review_verdict` (the reported verdict, never acceptance) | — |
| `pr_opened` | `pr_opened` | — |
| `decision` | `decision` | `cross_project` when the event's top-level `escopo` is `global` |
| `accepted`, `preparing`, `worker_spawned`, `worker_done`, `commit`, `push`, `checkpoint`, `unblocked`, `amend_received`, `decision_acked`, `note`, `cleanup`, unknown | not projected | — |

State plainly what is not derived: execution start is never inferred from `accepted` or `preparing`; implementation done is never inferred from `worker_done`, `checkpoint` or a report; review acceptance is never inferred from `review_verdict` or a report arriving; `push` is not projected (a rejected push arrives as `blocked`); and stuck and cross-project dependency are not detected automatically and are only raised explicitly with `notify raise`. These need producers in `herdr-soho` (upstream) and are out of scope.

**Agent status.** For a watched pane, `blocked` notifies (class `agent_blocked`, escalation `blocked`) and `done` notifies (class `agent_done`, no escalation: the agent reported that it finished a turn, which is never implementation done or acceptance); the routine statuses `idle`, `working` and `unknown` are coalesced in the ledger and never notify, escalate or fail anything. Loop guard: a `done` of a watched pane that is itself the destination of a registered recipient (a reference equal to the pane or ending with `"/"` + the pane) is coalesced, because a delivered notification makes that recipient work and then report done; register recipients by pane reference for the guard to recognize them. `blocked` always notifies. A repeated status is a duplicate and not a transition; each true status transition gets the next per-pane number and projects a distinct notification.

**Workspaces.** A Herdr workspace is a source when it is registered (`notify register workspace --workspace <ws> [--job <id>] [--projeto <org/repo>]`, whatever its label; the registration's job and project route it), when its label is `job-<id>` (the workspace label the job contract defines; the tracked job's project routes it), or, for a closed event that carries no workspace info, when it was seen opening. `workspace.created` is class `workspace_opened` and `workspace.closed` is class `workspace_closed`, with no escalation; any other workspace writes nothing. A repeated event is a duplicate; each true open or close gets the next per-workspace number, so a later reopen of the same workspace id is a distinct notification.

**Raises.** An explicit raise is class `cross_project` (escalation `cross_project`) or `stuck` (escalation `stuck`).

**IDs.** Source IDs are `job:<job>:<seq>`, `agent:<pane>:<n>:<status>`, `workspace:<ws>:<n>:<opened|closed>` and `raise:<id>`; the notification ID is `n` plus the first 20 hex digits of the sha256 of the source ID, so the same source always maps to the same notification: a replayed wake, a reconciliation sync and a repeated raise reuse the stored notification and never create a new one, while a later true status or workspace transition gets the next per-pane or per-workspace number and is a distinct notification.

**Registry and ledger.** The registrations and the ledger live in `<state>/notify/registry.json` and `<state>/notify/ledger.json`: the directory is mode 0700, the files are mode 0600 and written atomically (temp file + rename), and every mutation happens under the outbox lock. Nothing is written until the first `notify register`; registering starts projection at the current last outbox seq, so there is no backfill. Nothing is ever guessed: a role with no registered recipient has no recipient.

**Routing.** The owner is `registry.owners[<projeto>]` (for an agent status: the watch's `projeto`; for a workspace: the registration's project, else the tracked job's), the orchestrator is `registry.orchestrators[<job>]` (for an agent status: the watch's `job`; for a workspace: the registration's job, else the `job-<id>` label's). A missing owner (the projeto is empty or not registered) adds the escalation `missing_owner` and a `no_route` owner delivery; a missing orchestrator gets a `no_route` orchestrator delivery. The coordinator is a recipient if and only if the escalations are non-empty; none registered is a `no_route` coordinator delivery. The same reference holding several roles gets one delivery with all roles, listed in the order owner, orchestrator, coordinator. A delivery is marked `self` and never sent when the recipient reference is the source itself: for a job event, when `refs.agente` equals the reference, or `refs.pane` equals the reference, or the reference ends with `"/"` + `refs.pane`; for an agent status, when the reference equals the pane id or ends with `"/"` + the pane id. The deliveries are ordered owner, orchestrator, coordinator.

**Delivery.** A notification is persisted before any send. Each delivery runs `herdr-soho send <ref> <message> --now --timeout <ms>` as an argv subprocess with a context deadline (never a shell); the bounded output is classified and never stored. Per recipient the ledger separates the attempts (`attempts`, `first_attempt_at`, `last_attempt_at`), the endpoint acceptance (`accepted_at` and `accept_status`, one of `sent`, `queued`, `accepted`) and the optional processing acknowledgement (`ack_at`, set by `notify ack`; the delivery never waits for it). Exit 0 is `accepted`; exit 15, or a send killed in flight at its deadline, is `uncertain` and is never resent automatically, so a message is never injected twice; exit 2, 3 or 18 is `rejected`; anything else is transient and is retried after 10 s, 30 s, 90 s, 300 s and 900 s (the last repeats) up to 6 attempts, then `exhausted`. A crashed delivery is retried after a 60 s lease. A due delivery that a bounded run did not reach before its deadline is released unsent, without consuming an attempt, and stays due for the next run. The send process may use the run's whole remaining budget, because `herdr-soho send` can take longer than its `--timeout` to observe the receipt on a busy recipient. An `uncertain` or `exhausted` delivery is re-queued once only by an explicit `notify retry <nid> --role <role>`, the decision of someone who checked that the recipient did not get the message. The inbound policy of the receiving side is never changed and there is no opt-out in this bridge.

**Message.** The message is one line with only validated identifiers — the notification id, the class, the roles, the job id, the project, the event tipo and seq, the terminal exit, the pane, the workspace, the status and the escalations — plus a `details` hint and the optional `ack` command. It never carries a `resumo`, a ref other than the exit, paths, titles, agent kinds, terminal output, prompts or environment values; an identifier that fails its pattern is rendered as `?`. At projection, a project or job id that fails its pattern is dropped from the notification (an invalid project routes as a missing owner) and a job event whose job id is invalid is counted as malformed and not projected.

**Timing.** The `wake` hook projects and delivers with a 20 s bound before its push; `sync` and the plugin `startup` hook project and deliver with a 30 s bound; the plugin `event` hook for `pane.agent_status_changed` ingests the event and then delivers with a 20 s bound, and the `workspace.created`/`workspace.closed` hooks record the workspace notification before their sync delivers it (20 s bound when the sync does not reach its notify step). The record is durable before any send; the 20 s bound only limits how long a one-shot hook waits for the endpoints: `herdr-soho send --now` to an idle recipient was measured at under 1 s, but to a busy one it answers `queued` only after observing the receipt, measured at about 15 s, and a shorter bound would kill such sends in flight. `wake` and `sync` print the same output line as before notifications existed. Each notification is stamped with `source_ts` (only when the source provides one — job events do, Herdr status events carry none and none is invented), `observed_at` and `persisted_at`, and each delivery carries its attempt, acceptance and acknowledgement times, so source-to-persistence, sending and recovery are measured separately.

**Commands.** Every `notify` subcommand prints one JSON line:

- `notify register owner --projeto <org/repo> --to <ref>`
- `notify register orchestrator --job <id> --to <ref>`
- `notify register coordinator --to <ref>`
- `notify register watch --pane <ws:pane> [--job <id>] [--projeto <org/repo>]`
- `notify register workspace --workspace <ws> [--job <id>] [--projeto <org/repo>]`
- `notify unregister owner --projeto <org/repo> | orchestrator --job <id> | coordinator | watch --pane <ws:pane> | workspace --workspace <ws>`
- `notify list` (read-only)
- `notify status [--id <nid>]` (read-only)
- `notify deliver [--timeout <ms>]`
- `notify ingest agent-status|workspace` (Herdr event JSON on stdin, 64 KiB cap)
- `notify raise --class cross_project|stuck [--projeto <org/repo>] [--job <id>] [--id <raise-id>]`
- `notify ack <nid> --role owner|orchestrator|coordinator`
- `notify retry <nid> --role owner|orchestrator|coordinator`

A `<ref>` is a `herdr-soho send` reference `<machine>/<ws>:<pane>` or a local agent name; it is validated, never guessed or derived from an event. `list` and `status` work under `HERDR_HERMES_NOWRITE=1`; every other `notify` subcommand is refused under NOWRITE with exit 2, like the other writing commands.

## Exit codes

| code | meaning |
|------|---------|
| 0 | success (forwarded commands: whatever `herdr-soho` returned) |
| 2 | bad usage, input limit exceeded, unknown config key, internal job subcommand refused, a writing command under `HERDR_HERMES_NOWRITE=1`, no user config directory (`no_config_dir`), or a `wake` event that could not be made durable |
| 3 | unknown job id in `herdr-hermes` bookkeeping (`sync --job`) |
| 4 | `herdr-soho` not found, not runnable, or killed by the forwarding deadline |
| 40 | no API key configured (push required) |
| 41 | API key rejected by the dispatcher (401/403) |
| 42 | dispatcher unreachable or retries exhausted; records stay pending |
| 43 | `herdr-soho` lacks the `ephemeral_job` or `job_events` capability (`doctor`, `sync`) |

Forwarded `job` subcommands return the `herdr-soho` exit code unchanged; codes 40–43 are never produced by a forwarded command.

## NOWRITE

`HERDR_HERMES_NOWRITE=1` makes the CLI read-only. The read-only commands work normally and write nothing: `outbox`, `doctor`, `auth status`, `config get` and `config list`, `capabilities`, `version`, `help`, `plugin bridge status`, `notify list` and `notify status`, and the read-only `job` subcommands (`status`, `wait`, `events`, `collect`, `list`, which also skip bookkeeping). Every writing command (`job start|amend|send|ack|cancel|close`, `wake`, `sync`, `push`, `session`, `decision`, `auth login`, `auth logout`, `config set`, `plugin bridge sync`, `notify register`, `notify unregister`, `notify deliver`, `notify ingest`, `notify raise`, `notify ack` and `notify retry`) refuses with exit 2 and `{"status":"nowrite","motivo":"HERDR_HERMES_NOWRITE=1"}` before any side effect. Exception: the plugin entrypoints `plugin startup` and `plugin event` exit 0 under NOWRITE, print `{"status":"skipped","motivo":"HERDR_HERMES_NOWRITE=1"}` and write nothing.

## Key security

The per-user API key is issued to the person by the dispatcher operator; phase 1 only stores and sends it.

- Storage: one file in the user configuration directory (`credentials`), written atomically with mode 0600, behind a credential-store interface so an OS keychain backend can replace it later without changing the commands.
- The key is never printed, logged, written to the outbox, passed to any subprocess environment, typed into a pane, or included in an error message. It appears only in the `Authorization` header of push requests.
- `herdr-hermes auth login --key -` is the only way to set the key: it reads from stdin (or from an interactive prompt with echo off when stdin is a terminal), and refuses an empty key, a key containing whitespace, and a key over 4 KiB. No flag value or environment variable is ever accepted as the key.
- `auth status` prints only `{"configured":true|false,"store":"file"}`; `auth logout` removes the key.
- Commands that need the key with none configured: `wake` records the event and exits 0; `sync`/`push` exit 40 with `{"status":"auth_missing"}`. Pulling through `outbox` never needs a key.
