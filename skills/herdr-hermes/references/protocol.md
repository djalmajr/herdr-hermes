# Protocol (condensed)

The full protocol is `docs/protocol.md` in the `herdr-hermes` repository. This reference condenses the outbox record, the push headers and the exit codes.

## Outbox record

One JSON line per record, schema 1:

```json
{"schema":1,"seq":12,"ts":"2026-01-02T03:04:05-03:00","tipo":"job_event","maquina":"<label>","projeto":"<org>/<repo>","job_id":"<id>","idempotency_key":"<key>","dados":{...}}
```

- `seq` is consecutive from 1 with no gaps for the machine, assigned under a process-wide lock.
- `tipo` is one of `job_event`, `dispatch`, `amend`, `session`, `decision`.
- `job_id` is the job id or null.
- `idempotency_key` is `<job_id>:<event seq>` for `job_event` (the same key the job contract uses) and `<maquina>:<outbox seq>` for every other record.
- `dados` carries the original job event for `job_event`, or the command payload otherwise.

Job events are deduplicated by their idempotency key, and `last_event_seq` is the highest event seq up to which every event of the job is stored, so `sync` refetches any gap.

## Pull channel

`herdr-hermes outbox [--since <seq>] [--wait <ms>]` prints the records with `seq > since` as JSON lines and always ends with the trailer `{"outbox":"fim","ultimo_seq":N,"entregue_seq":M}`. It is read-only, needs no key and no network, and works under `HERDR_HERMES_NOWRITE=1`.

## Push

`POST <dispatcher_url>` with one record as the JSON body. Headers, exactly:

- `Authorization: Bearer <key>`
- `Idempotency-Key: <idempotency_key>`
- `Content-Type: application/json`
- `User-Agent: herdr-hermes/<version>`

Records are pushed in `seq` order, one at a time. A 2xx advances the delivery cursor. 401/403 stops the batch (exit 41 for `sync`/`push`). 408/429/5xx and network errors are retried at most three times after 10 s, 30 s and 90 s, then the records stay pending (exit 42 for `sync`/`push`; `wake` makes a single attempt and exits 0 either way). 3xx and other 4xx stop without retry (exit 42). HTTPS is required unless the host is a loopback address; redirects are not followed. When `dispatcher_url` is empty, push is disabled and the records stay in the outbox for a pull.

## Notifications

Native notifications project this machine's own sources into sanitized one-line messages delivered to explicitly registered recipients through `herdr-soho send`. The sources are the job contract events stored in the outbox (from `wake` or `sync`), the Herdr `pane.agent_status_changed` events of registered (watched) panes, the Herdr `workspace.created`/`workspace.closed` events of a registered workspace or a `job-<id>` workspace, and explicit local raises. Notifications are never added to the outbox; the ledger lives in its own files under the state directory, and a notification carries only validated identifiers, never a resumo, a ref other than the exit, paths, titles, agent kinds, terminal output, prompts or environment values.

Mapping: `question` → `question`; `blocked` → `blocked` (escalation `blocked`); `failure` → `failed` (escalation `failed`); `terminal` → `terminal` for exit 0, 21, missing or other, `blocked` for exit 7, 11, 14 (escalation `blocked`) and `failed` for exit 9, 19, 22 (escalation `failed`); `timeout_warning` → `timeout_warning`; `review_verdict` → `review_verdict` (the reported verdict, never acceptance); `pr_opened` → `pr_opened`; `decision` → `decision` (escalation `cross_project` when the event's top-level `escopo` is `global`). `accepted`, `preparing`, `worker_spawned`, `worker_done`, `commit`, `push`, `checkpoint`, `unblocked`, `amend_received`, `decision_acked`, `note` and `cleanup` are not projected. Nothing is inferred: execution start never from `accepted` or `preparing`, implementation done never from `worker_done`, `checkpoint` or a report, review acceptance never from `review_verdict` or a report, and `push` is not projected (a rejected push arrives as `blocked`).

Agent status of watched panes: `blocked` notifies (class `agent_blocked`, escalation `blocked`) and `done` notifies (class `agent_done`: the agent finished a turn, never implementation done or acceptance; coalesced when the pane is itself a registered recipient, the loop guard); `idle`, `working` and `unknown` are coalesced in the ledger and never notify, escalate or fail anything. Workspaces: `workspace.created` is `workspace_opened` and `workspace.closed` is `workspace_closed`, for a workspace registered with `notify register workspace` or labeled `job-<id>`; a later reopen is a distinct notification. Stuck and cross-project dependency are never detected automatically: raise them with `notify raise --class stuck|cross_project` only after you have confirmed them.

Recipients are registered explicitly (`notify register owner|orchestrator|coordinator|watch|workspace`); a role without a registration has no recipient, a missing owner escalates with `missing_owner`, and a recipient that is the source itself is never delivered to (`self`). Delivery exits: 0 is `accepted`, 15 or a send killed in flight at its deadline is `uncertain` (never resent, so a message is never injected twice), 2, 3 or 18 is `rejected`, and anything else retries after 10 s, 30 s, 90 s, 300 s and 900 s up to 6 attempts, then `exhausted`; `notify retry <nid> --role <role>` re-queues an `uncertain` or `exhausted` delivery once, after you checked that the recipient did not get it. `notify list` and `notify status` are read-only; every other `notify` subcommand is refused under `HERDR_HERMES_NOWRITE=1` with exit 2. The full spec is the "Notifications" section of `docs/protocol.md`.

## Routing

`herdr-hermes route [--machine <label>]` is the dispatcher-side machine choice: it runs on the host the dispatcher uses to reach the fleet, probes the configured fleet read-only, picks one machine and prints it as one JSON line. It is advisory — it never starts, moves or replays a job — and the dispatcher then runs `job start` on the chosen machine through its own remote execution channel, never re-sending the dispatch to another machine after a transport loss or timeout. It is read-only (it works under `HERDR_HERMES_NOWRITE=1` and writes nothing) and exits 2 for bad usage, an invalid configuration, `route_machines` not configured, or a requested machine that is not configured, and 4 when no machine is available.

Configuration keys (set with `herdr-hermes config set <key> <value>`): `route_machines` (the fleet: comma-separated machine labels in priority order; a label is a saved Herdr machine label or the reserved `local` for the host that runs `route`), `route_disabled` (labels to exclude without editing the Herdr machine profiles), `route_orchestrator_name` (the orchestrator agent base name to count, default `orchestrator`), `route_probe_timeout_s` (the bound of each probe, 1..120 seconds, default 20), and `herdr_bin` (the `herdr` executable, default `herdr`).

The probe is read-only and uses only the public Herdr CLI (`herdr machine list --json`, then `herdr agent list` or `herdr --machine <label> agent list` under the bound), never a shell and never the state files. A machine is `available` when it answers a valid agent list; its load is the number of agents whose name is the orchestrator base name or the base name followed by `-<n>`, whatever their status — `idle`, `working`, `blocked`, `done` and `unknown` all count the same, so a finished orchestrator whose pane is still open keeps counting until its pane or workspace closes (workers, unnamed agents and other agents are not counted). A machine is `unavailable` when the probe times out, fails or is malformed, or the catalog fails; `disabled` when excluded by `route_disabled` or saved with `enabled: false`; and `unsupported` when the label is unknown or ambiguous. The available machine with the lowest orchestrator load wins, a tie going to the first in `route_machines` order (`motivo: least_load`); a requested `--machine` that is available always wins (`motivo: requested`), and a requested machine that is not available falls back with `motivo: fallback` and `solicitada` set. The full specification, with the reason codes, the outputs and the safety rules, is `docs/routing.md` and the "Routing" section of `docs/protocol.md` in the `herdr-hermes` repository.

## Exit codes

| code | meaning |
|------|---------|
| 0 | success (forwarded commands: whatever `herdr-soho` returned) |
| 2 | bad usage, input limit exceeded, unknown config key, internal job subcommand refused, a writing command under `HERDR_HERMES_NOWRITE=1`, no user config directory (`no_config_dir`), or a `wake` event that could not be made durable |
| 3 | unknown job id in `herdr-hermes` bookkeeping (`sync --job`) |
| 4 | `herdr-soho` not found, not runnable, or killed by the forwarding deadline, or no eligible machine available (`route`) |
| 40 | no API key configured (push required) |
| 41 | API key rejected by the dispatcher (401/403) |
| 42 | dispatcher unreachable or retries exhausted; records stay pending |
| 43 | `herdr-soho` lacks the `ephemeral_job` or `job_events` capability (`doctor`, `sync`) |

Forwarded `job` subcommands return the `herdr-soho` exit code unchanged; codes 40–43 are never produced by a forwarded command.

## Key

The per-user API key is stored on the machine (`auth login --key -`), is sent only in the `Authorization` header, and never appears in any output, file, log or subprocess environment. `auth status` prints only `{"configured":true|false,"store":"file"}`.
