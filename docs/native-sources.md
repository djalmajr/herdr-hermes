# Native sources

This is the public inventory of the native notification feature: the sources it projects, the supported mapping, what is deliberately not derived from any source, how each source was verified, and the recipient routing and escalation rules. The wire details — the ledger files, the message format, the delivery retries and the `notify` commands — are in [`protocol.md`](protocol.md) (section "Notifications").

## Sources

Four sources reach the bridge and no others:

- Job contract events stored in the outbox. The `wake` hook appends a `job_event` record as each job event wakes the dispatcher and `sync` reconciles any gap, and projection reads those stored records. A notification is always a projection of a stored outbox record, never of a live event stream, so a replayed wake or a reconciliation sync reuses the stored notification instead of creating a new one.
- Herdr `pane.agent_status_changed` events of registered (watched) panes only. Herdr emits the event when the agent status of a pane changes; it reaches the bridge through `herdr-hermes notify ingest agent-status` (the event JSON on stdin, 64 KiB cap) or through the optional plugin's `pane.agent_status_changed` hook. Only a pane registered with `notify register watch` produces notifications; an event for an unwatched pane writes nothing. The event carries no source timestamp and no seq, and none is invented.
- Herdr `workspace.created` and `workspace.closed` events of a registered workspace or of a job workspace. They reach the bridge through `herdr-hermes notify ingest workspace` or through the optional plugin's workspace hooks. A workspace registered with `notify register workspace --workspace <ws> [--job <id>] [--projeto <org/repo>]` is a source whatever its label, routed by its registration; a workspace labeled `job-<id>` (the label the job contract gives the workspace of a job) is a source routed by that job; a closed event that carries no workspace info is resolved to the workspace's record from when it opened. Any other workspace writes nothing. The events carry no source timestamp and none is invented.
- Explicit local raises. `herdr-hermes notify raise --class stuck|cross_project` records a condition that no source can detect (see "Not derived"); a repeated raise with the same id reuses the stored notification.

## Supported mapping

### Job contract events

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

### Agent status (watched panes)

| status | result |
|---|---|
| `blocked` | an `agent_blocked` notification, escalation `blocked` |
| `done` | an `agent_done` notification, no escalation: the agent reported that it finished a turn; it is never implementation done or acceptance |
| `idle`, `working`, `unknown` | routine: coalesced in the ledger, no notification, no escalation, no failure |

A repeated status is a duplicate, not a transition. Each true transition gets the next per-pane number and projects a distinct notification, so `done` after `working` after `done` is a second `agent_done`. Loop guard: a `done` of a watched pane that is itself the destination of a registered recipient (a reference equal to the pane or ending with `"/"` + the pane) is coalesced, because a delivered notification makes that recipient work and then report done, which would notify the other recipients back and forth; a recipient registered by agent name cannot be matched to a pane, so register recipients by pane reference when their panes are also watched.

### Workspaces

| event | class | escalations |
|---|---|---|
| `workspace.created` | `workspace_opened` | — |
| `workspace.closed` | `workspace_closed` | — |

A repeated event is a duplicate. Each true open or close gets the next per-workspace number, so a later reopen of the same workspace id is a distinct notification.

### Raises

| class | escalations |
|---|---|
| `cross_project` | `cross_project` |
| `stuck` | `stuck` |

## Not derived

The following are never inferred from any source. Each would need a new producer in `herdr-soho` (upstream of this bridge) and is out of scope:

- Execution start — never inferred from `accepted` or `preparing`: they mean the job was validated and preparation began, not that the work started.
- Implementation done — never inferred from `worker_done`, `checkpoint`, a report landing or an agent `done`: a worker finishing a turn or a report arriving is not the job's acceptance.
- Review acceptance — never inferred from `review_verdict` (which carries the reported verdict) or from a report arriving.
- Push success or failure — the `push` event is not projected at all; a rejected push arrives as `blocked`.
- Stuck detection — there is no timeout or silence heuristic; `stuck` is raised only explicitly with `notify raise`.
- Cross-project dependency — never inferred from event content; `cross_project` is raised only explicitly with `notify raise`.

Other Herdr events (`workspace.updated`, `workspace.renamed`, `workspace.focused`, `tab.*`, `pane.created`, `pane.closed`, `pane.exited`, `pane.agent_detected`, `pane.output_matched`, `layout.updated`, …) are not projected.

## Verification status

Each source is listed with the strongest evidence it has; fixture evidence and live evidence are kept apart.

| source | how it was verified |
|---|---|
| Job contract events | Adapter tested with fixture events that follow the vendored job contract. The reference node's `herdr-soho` has no `job` command (no `ephemeral_job` or `job_events` capability), so no live job event exists there; fixture events were also fed to `wake` on a live node and delivered through the real `herdr-soho send` transport to stand-in recipient agents — a fixture source with a real transport, not a live producer. |
| Herdr agent status, socket subscription | Captured live: the socket subscription line `{"event":"pane.agent_status_changed","data":{"agent_status":…,"pane_id":…,"workspace_id":…,"agent":…}}` of a real agent, including real `blocked` (approval and trust dialogs), `working`, `done` and `idle` transitions, was ingested unchanged by `notify ingest agent-status` and delivered through `herdr-soho send` to stand-in recipient agents in the same workspace. The recipients were stand-ins, not real project owners or coordinators. |
| Herdr agent status, plugin hook | Unverified: the `HERDR_PLUGIN_EVENT_JSON` payload of plugin hooks is not documented by Herdr and no plugin was installed to observe it. The parser accepts the socket subscription line, the event-stream form `pane_agent_status_changed` and the flat data object. |
| Herdr workspace events, socket and plugin hook | Unverified live: capturing them needs a workspace to be opened and closed, outside the single workspace the verification ran in. The shapes come from the bundled Herdr API schema (`workspace_created` carries the workspace info; `workspace_closed` carries the workspace id and possibly `workspace: null`); the adapter is tested with fixtures of those shapes and of the existing plugin payload shapes. |
| Raises | Local command, tested end to end. |

## Recipient routing and escalation

Recipients are registered explicitly and never guessed; a role with no registration has no recipient:

- The owner is the reference registered for the project (`notify register owner --projeto <org/repo>`; for an agent status, the watch's `projeto`; for a workspace, the registration's project, else the tracked job's). A missing owner (the project is empty or not registered) adds the escalation `missing_owner` and a `no_route` owner delivery.
- The orchestrator is the reference registered for the job (`notify register orchestrator --job <id>`; for an agent status, the watch's `job`; for a workspace, the registration's job, else the `job-<id>` label's). A missing orchestrator gets a `no_route` orchestrator delivery.
- The coordinator (registered with `notify register coordinator`) is a recipient if and only if the escalations are non-empty; none registered is a `no_route` coordinator delivery.
- The same reference holding several roles gets one delivery with all roles, listed in the order owner, orchestrator, coordinator.
- A recipient that is the source itself is marked `self` and never delivered to: a job event whose `refs.agente` or `refs.pane` equals the reference, or whose `refs.pane` is the tail of the reference after a `"/"`, and an agent status whose pane is the reference itself or the tail of the reference after a `"/"`.
- The delivery order is owner, orchestrator, coordinator.

Delivery is final on the wire: exit 0 is `accepted`, exit 15, or a send killed in flight at its deadline, is `uncertain` (never resent automatically, so a message is never injected twice), exit 2, 3 or 18 is `rejected`, and anything else is retried after 10 s, 30 s, 90 s, 300 s and 900 s (the last repeats) up to 6 attempts, then `exhausted`. A due delivery that a bounded run did not reach before its deadline is released unsent without consuming an attempt; an `uncertain` or `exhausted` delivery is re-queued once only by an explicit `notify retry`. The inbound policy of the receiving side is never changed and there is no opt-out in this bridge.
