# Ephemeral job contract

An ephemeral job is one self-contained orchestration run: a dispatcher hands `herdr-soho` a brief, the machine prepares an isolated checkout, a job orchestrator runs a team in Herdr, the work is kept in sync with a draft pull request, a structured report and an event log come back, and every resource the job opened is released. This document is the contract between `herdr-soho job` and any dispatcher that drives it.

JSON field names and enumerated values (`status`, `tipo`, `memoria`, ...) are part of the shared schema with the dispatcher and are kept as specified, including the ones in Portuguese; prose in this document is English.

The dispatcher chooses the machine and runs these subcommands on it through its own remote execution channel; the transport is outside this contract, and `herdr-soho` never routes a job to another machine. The dispatcher talks only to the job (through the commands below), never to the job's workers. People give feedback to the dispatcher, which relays it to the job; nobody drives the job's panes by hand.

## Commands

Every subcommand prints exactly one JSON line on stdout, except `events`, which prints JSON lines. Diagnostics go to stderr.

```text
herdr-soho job start   --id <id> --repo <org/repo> [--base <branch>] [--mode worktree|workspace]
                       [--brief <file|->] [--timeout <minutes>] [--dry-run]
herdr-soho job status  --id <id>                      # read-only; state, event count, last seq, pending decisions, partial collaborations
herdr-soho job wait    --id <id> [--timeout MS]       # blocks until a terminal state
herdr-soho job events  --id <id> [--since <seq>] [--wait MS]   # JSON lines; long-poll
herdr-soho job collect --id <id> [--md] [--verify]    # final report (JSON, or report.md with --md)
herdr-soho job amend   --id <id> [<file>|-]           # contract change or answer to a question
herdr-soho job send    --id <id> [<file>|-]           # non-blocking note to the job orchestrator
herdr-soho job ack     --id <id> --upto <seq>         # dispatcher routed every decision up to <seq>
herdr-soho job cancel  --id <id> [--grace <seconds>]
herdr-soho job close   --id <id> [--force]
herdr-soho job list    [--state <state>]              # job counts by state on this machine

# internal, used by the supervisor and the job orchestrator, never by the dispatcher:
herdr-soho job supervise  --id <id>                   # runs inside the supervisor pane (HERDR_ENV=1)
herdr-soho job checkpoint --id <id>                   # job orchestrator asks for an immediate push
herdr-soho job note    --id <id> --tipo <type> [--escopo global|projeto] [--motivo <text>] [--refs k=v,...] [<summary>|-]
```

`job start` runs outside Herdr: it validates, prepares the repository and worktree, resolves the team, creates the Herdr workspace `job-<id>` with `herdr workspace create --cwd <worktree> --label job-<id> --no-focus`, and starts `herdr-soho job supervise --id <id>` in its first pane. The first pane alone also receives, privately, the location of the starter's own user configuration (and, for a home-relative `job_repos_root`, the starter's home), so the supervisor and the team commands it runs (lane, spawn, dispatch, send, status, release, roster, gc) read the same user configuration file and repository root as `job start`, whatever the Herdr server's environment holds; no other variable, value or configuration content travels, worker panes do not receive it, and git, gh, herdr and the wake command keep the server's own environment. It is not a configuration interface: a command typed later in that pane, or any command not run by the supervisor, ignores it, and a supervisor started by hand without it resolves everything from its own environment as before. `job start` then waits a bounded time for the supervisor to confirm that it found this job and is about to run it, and returns as soon as the job is `running`; without that confirmation the job fails with exit 4 and its workspace is left open. On Windows, `job start` never starts a Herdr server; when no running server is reachable it exits 4. `status`, `wait`, `collect`, `list` and `events` read files only. `amend`, `send`, `cancel` and `ack` write a control file under the job directory that the supervisor picks up; none of them needs to run inside Herdr.

Job lookup by `--id`: `status`, `wait`, `events`, `collect`, `ack`, `close`, `note`, `amend`, `send`, `cancel` and `checkpoint` look for the job in every checkout under the machine's `job_repos_root` and, on every call, also in the job whose own directory the command runs from: the job worktree `<checkout>/.worktrees/job-<id>` or, for a `workspace` job, its checkout, including any directory inside it. That directory must be a Git worktree registered with that checkout (or the checkout itself), and the job's recorded `state.json` must name the same id, mode, checkout and directory; the job's state directory and files must not be links. Paths are compared as file-system entries, not as strings, so an alias or a case variant of a path on a case-insensitive file system names the same directory. So the job orchestrator and its workers find their job from inside it even when their own user configuration names another checkout root. When both places hold the id in two different state directories, the command exits 2 before it changes anything (the lookup itself only reads); when they are the same directory it is one job. A command run outside the job's directory keeps the checkout-root lookup only (unknown id: exit 3). Inside the job's directory the job's state directory is the one the caller's own configuration resolves there: the default one, or, for a job started with another `state_dir`, the one the job workspace's session file names (see Team configuration per machine), which a later command finds only with that workspace's id; any other caller whose configuration does not name that state directory gets exit 3. `start`, `supervise` and `list` keep the checkout-root lookup. The check proves that the directory and the records agree, not who runs the command: any process of the same user can already read and write the job's files.

Requirements: Herdr 0.9.1 or newer (0.9.3 or newer recommended on Windows). `capabilities --json` prints `{"schema":1,"worker_collaboration":1,"ephemeral_job":1,"job_events":1}`.

### Flags

- `--id` (required): `^[A-Za-z0-9._-]{1,64}$`. It is the idempotency key.
- `--repo` (required): `<org>/<repo>`, with `<org>` in the machine's `job_orgs` list and `<repo>` matching `^[A-Za-z0-9._-]{1,100}$`.
- `--base`: `^[A-Za-z0-9._/-]{1,100}$` without `..`; defaults to the brief's `base`, then to the remote default branch.
- `--mode worktree` (default): a worktree at `<checkout>/.worktrees/job-<id>` on branch `job/<id>` from `origin/<base>` after a fetch. `--mode workspace`: the checkout itself, only for jobs that change no code; a dirty or locked checkout is refused.
- `--brief`: a file or `-` (stdin). A remote dispatcher sends the brief on stdin.
- `--timeout`: the job's active budget in minutes, 1..1440 (default `job_timeout=120`). Time the job spends `blocked` does not count against it (see Budgets). It is separate from the per-role timeouts; a worker's wait timeout never ends the job.

### Exit codes

| code | meaning |
|---|---|
| 0 | `done`, or a read/control command succeeded |
| 2 | bad usage, organization out of scope, invalid brief, no team configuration, an `--id` found in two state directories, an `amend`, `send` or `checkpoint` under a pending cancel (`job: cancel_pending: the job is being canceled`) |
| 3 | unknown `--id` (`{"status":"not_found"}`) |
| 4 | Herdr failure or unavailable (including no reachable Herdr server on Windows) |
| 7 | `blocked`: a question, approval or dialog needs a person |
| 9 | job `timeout`: the active budget or the blocked ceiling was exhausted (`motivo` tells which) |
| 11 / 14 | `blocked` by quota / provider error (detail in `motivo`) |
| 19 | `failed`: partial items with no pending decision, failed gates, or the job orchestrator ended without a report |
| 20 | `--id` reused with a different brief (hash mismatch) |
| 21 | `canceled` |
| 22 | preparation failed: checkout or clone, GitHub authentication, fetch, unknown base, worktree |
| 23 | reserved, not used |
| 24 | `job close`: a `decision` event has not been acknowledged; `--force` closes anyway |

Codes 2 through 18 keep the meanings they have in the rest of the CLI.

## Brief

JSON on stdin (or a file). Required fields: `schema`, `id`, `origem`, `repo`, `objetivo`, `aceite`.

```json
{
  "schema": 1,
  "id": "TASK-123-7f3a",
  "origem": { "tipo": "card", "ref": "TASK-123" },
  "repo": "example-org/example-repo",
  "base": "main",
  "modo": "worktree",
  "maquina": "machine-a",
  "objetivo": "One sentence: what must be true at the end.",
  "contexto": "Optional facts already decided.",
  "aceite": [{ "criterio": "focused tests pass", "prova": "go test ./internal/example -run TestExample" }],
  "decisoes": ["names, formats and texts the job must not choose"],
  "restricoes": ["do not touch internal/plugin/**"],
  "nao_objetivos": ["..."],
  "equipe": { "lane.review.effort": "high" },
  "prazo": { "timeout_min": 90 },
  "budget": { "max_workers": 2 },
  "memoria": { "escopos": ["example-workspace/example-project"] },
  "idioma": "en"
}
```

- `origem.tipo`: `conversa`, `card` or `cron`; `ref` identifies the origin.
- `aceite[]`: each item becomes an acceptance criterion with the command that proves it.
- `maquina`: informative. When present it must equal the local `machine_label`, otherwise the job exits 2.
- `equipe`: optional override, applied on top of the machine's team file. Only known configuration keys are accepted (the `session set` parser); an unknown key exits 2.
- `memoria.escopos`: optional memory scopes the job orchestrator reads before it plans.
- There is no delivery option: every job with at least one commit gets a draft pull request (see Git).

The job converts the brief into the standard Markdown brief (`skills/herdr-soho/templates/brief.md`) and lints it before dispatch; a `strict` lint failure exits 2 before any pane opens.

## Team configuration per machine

The team is configured per machine, not in the repository, because the same project can run a larger team on a stronger machine. Files use the `session.conf` `key=value` format and live in the user configuration directory of `herdr-soho` (`$XDG_CONFIG_HOME/herdr-soho`, `%APPDATA%\herdr-soho` on Windows, otherwise `~/.config/herdr-soho`):

- `teams/<org>/<repo>.conf`: the team for one repository on this machine.
- `teams/default.conf`: the machine fallback.

Resolution at `job start`: the repository file; else the machine default; else the repository's `.agents/herdr-soho.conf` when it sets at least one `lane.*.roles`; else exit 2. A job never falls back to the built-in 2/3/4-pane presets. The resolved file, plus the brief's `equipe` overrides, is written into the job workspace's `session.conf`, and `report.json` records the source. Before it starts the job orchestrator, the supervisor completes that `session.conf` with the team settings the starting user's configuration resolves and the team file leaves open: for each role of the team's lanes, its `kind`, `model` and `effort`, on the lane key when a lane decided it and on the role key otherwise, plus that lane's kind when the job's agents' own configuration sets a kind for a team lane whose roles all resolve the same kind for the starting user (a lane key outranks a role key, so the session's lane kind keeps the per-role values in force), and `max_workers`. Approvals are never copied: a job's agents get the team file's approvals, and otherwise their own configuration's. The agents of the job workspace read that `session.conf`; when their own environment resolves its session file under another state directory (a `state_dir` that differs between the starting user's configuration and the Herdr server's), the supervisor writes that session file too, with the same team and a `state_dir` naming the job's state directory, so the job orchestrator's own team and `job` commands use the job's team, roster and state. When that session file lies inside the checkout, its directory is added to the checkout's local `.git/info/exclude` (never to a tracked file). It never copies other user settings, machine keys or credentials. The supervisor then resolves the team and state directory as the starting user and as the job's agents, and the job fails before the job orchestrator starts when they differ, or when the session file is needed but cannot be written safely: a state directory path with `#` or a line break, which a configuration line cannot hold; a location inside a job worktree or tracked by Git; a link or another kind of file in its path; an unreadable or oversized session or exclude file. Settings outside the team (for example approvals, arguments or other roles) still come from each agent's own user configuration: put a job team's settings in its team file.

Example team shapes: implementer only (`panes=2`, `lane.build.roles=implementer`); implementer and reviewer (`panes=3`, adds `lane.review.roles=reviewer`); implementer, reviewer and security reviewer (`panes=4`, adds `lane.sec.roles=security-reviewer`, `lane.sec.panes=1`).

## Machine configuration

Keys in the user configuration file:

- `machine_label`: the canonical name of this machine, reported in `report.json` and checked against `brief.maquina` (examples: `machine-a`, `machine-b`).
- `job_orgs`: comma-separated organizations a job may check out.
- `job_repos_root`: checkout root (default `~/repo.git`, `%USERPROFILE%\repo.git` on Windows); checkouts live at `<root>/<org>/<repo>`.
- `job_timeout`: default active budget in minutes (default 120).
- `job_blocked_timeout`: the blocked ceiling in minutes, 1..43200 (default 1440, 24 hours): the most time a job may spend `blocked` in total. An invalid value makes `job start` exit 2. The value in effect at `job start` is recorded with the job.
- `job_finishing_timeout`: the budget of the terminal procedure in minutes, 1..1440 (default 10): the most time the final push, the draft pull request step and the collaboration wait may take in total once a job is `finishing`. An invalid value makes `job start` exit 2. The value in effect at `job start` is recorded with the job (`finishing_timeout_min` in `state.json`, in the same write as `blocked_timeout_min`), and a supervisor, including a restarted one, uses only the recorded value; a job recorded before the field existed uses 10 minutes and its state is not rewritten.
- `job_auto_cleanup`: the job's automatic close at the end (see Release and automatic close), `on` (default) or `off`. `job_auto_cleanup` and `job_human_input_minutes` are resolved through the layered configuration with the job's checkout as the project (defaults, the user file, the repository's `.agents/herdr-soho.conf`, and the `HERDR_SOHO_*` environment over them), so the repository's file beats the user file and a repository can opt the job out. The value in effect at `job start` is recorded with the job once (`auto_cleanup` in `state.json`, in the same write as `blocked_timeout_min`), and a supervisor, including a restarted one, uses only the recorded value; a job recorded before the field existed reads `on`. An invalid value makes `job start` exit 2.
- `job_human_input_minutes`: the minutes the automatic close waits for the panes that are not the job's to stop changing their screen before it closes the job's workspace (see Release and automatic close), 1..1440 (default 5). Resolved and recorded with the job like `job_auto_cleanup` (`human_input_min` in `state.json`); a job recorded before the field existed uses 5. An invalid value makes `job start` exit 2.
- `job_wake_cmd`: an optional local wake hook command (see Events).

When the checkout is missing, the job clones it into `<job_repos_root>/<org>/<repo>` only when the organization is in `job_orgs` and the machine already has working GitHub authentication; otherwise it exits 22. Every job also checks push access to the remote and `gh` authentication before any pane opens, because pushing is mandatory.

## Git: always in sync, always a draft pull request

- The job orchestrator commits once per integrated slice; workers never commit or push.
- Every new commit on `job/<id>` is pushed by the supervisor (it watches `HEAD` every 30 seconds; `job checkpoint` pushes immediately). Pushes never use `--force`; a rejected push becomes a `blocked` event.
- The first push opens a draft pull request against `base`. The final push happens in `finishing`. The job never merges and never marks the pull request ready for review.
- A job that ends with no commit (research, review) opens no pull request: `report.json` has `"pr": null` and `"motivo": "sem commits"`.
- On cancel or timeout, uncommitted changes in the job worktree become a `wip(job): checkpoint` commit, which is pushed; the pull request stays a draft.
- Pull request bodies and commit messages use only the report's `resumo` and `publico` block (verdict, generic blockers, identifiers). They never carry hosts, paths, machine labels, models, costs, memory entries or event text.

## Lifecycle

```text
accepted -> preparing -> running <-> blocked
                         running -> finishing
                         blocked -> finishing
finishing -> {done | failed | timeout | canceled} -> closed
finishing -> blocked
```

The graph shows every edge the job store allows, except the legacy `collected` edges described below; a start or preparation failure reaches `failed` along these same edges.

The `closed` edge is the owner's `job close`, accepted only from a terminal outcome, from `collected` or from `closed` itself (a no-op retry): a live `blocked` job is never closed, and the blocked ceiling ends the job `timeout` through the stop path (see Blocked jobs below). `collected` is a legacy state no current command records: `job collect` is a read that leaves the status at the terminal outcome, and `job close` moves a terminal job straight to `closed`, so `collected` can only appear in a `state.json` written by an older binary. The read commands treat it as a report-carrying state, and `job close` from it finishes the close as from a terminal outcome.

State lives in `<checkout>/.herdr-soho/jobs/<id>/`: `state.json` (atomic write: temp file next to the target, then rename; mode 0600), `events.jsonl`, `brief.json`, `brief.md`, `report.md`, `report.json`, `lock` and `control/`. The directory sits at the state root, outside any workspace id, so it survives the workspace.

- Two optional fields of `state.json` carry the cancel's anchor: `cancel_requested_at` and `cancel_deadline`, the wall times (RFC 3339) of the acceptance of the `job cancel` and of the acceptance plus the requested grace. They are set once, at the acceptance, and are not cleared when the cancel completes; they are absent from every state that never accepted a cancel and from states written by a binary before the fields existed, and a reader that sees them changes nothing.
- Idempotency: a lock per id. `start` again with the same brief hash returns the current state with `duplicate_of`; a different hash exits 20. Repeated `close`, `cancel`, `ack` and `checkpoint` calls with nothing to do exit 0.
- Error reports: once `--id` is valid, every terminal state writes `report.json`, including preparation failures, timeouts and cancellations.
- Cancel and timeout: the job orchestrator receives a "stop and report what you have" amendment, the job waits `--grace` seconds (default 120; 120 for a timeout), commits and pushes leftovers, finishes the job's collaborations (see Collaborations at the end of a job), releases every worker with `--close --force`, and ends `canceled` (21) or `timeout` (9). The state's `motivo` names the reason: `cancelado`, `tempo esgotado` (active budget) or `tempo de bloqueio esgotado` (blocked ceiling).
- The grace starts at the acceptance: the acceptance of a `job cancel` records the anchor (`cancel_requested_at`, the wall time of the acceptance) and the deadline (`cancel_deadline`, the acceptance plus the requested grace) in `state.json` in the same locked write as the request file, and the supervisor ends the stop on the persisted deadline; a restarted supervisor re-anchors the grace on the persisted deadline and never restarts it, and a job accepted before the fields existed, or with an unreadable deadline, keeps the full grace measured from its own process, with one friction line.
- While a cancel is pending, `job amend`, `job send` and `job checkpoint` are refused with exit 2 (`job: cancel_pending: the job is being canceled`), before anything enters `control/`, and a second `job cancel` stays the no-op that changes neither the anchor nor the request file.
- The supervisor's deliveries are interrupted by the arrival of the cancel, and every delivery of the stop (the stop amendment and the deliveries of the pending queue) is limited to the grace remaining at its start, with no extra time and no retry; a delivery cut by the cancel records a `delivery_uncertain` event (`refs.seq_ref` the request's counter), is never re-sent automatically, and the request stays in `control/` for the dispatcher.
- From the acceptance of the `job cancel` to the durable terminal record (`state.json` in `canceled`, written by the terminal transition of the stop), the time is at most `--grace` plus one supervisor poll interval (5 s) plus the finishing budget (`job_finishing_timeout`): the stop ends on the first supervisor tick at or past the persisted deadline, at most one poll after it, and the final `wip(job): checkpoint` commit and the final push run inside the finishing budget after the grace. Measured in real use with a 20 s grace, the terminal event came 22.6 s after the acceptance on Windows and 28.8 s after it on macOS: a margin of about 3 to 9 s over the grace, the wait for the next supervisor tick plus the final commit and push.
- Blocked jobs: a `blocked` job stays alive, with its job orchestrator in its pane, waiting for `job amend`. It does not consume the active budget while it waits, but it consumes the blocked ceiling; when the ceiling is reached the job ends through the same stop path as a timeout. The owner may still cancel it at any time.
- Held requests: while the job orchestrator is in a dialog or a question, the supervisor does not attempt to deliver a pending `amend` or `send`: it holds the request, records one `control_held` per request, and lists the request in the status line's `held_controls`. The dispatcher answers by hand in the job orchestrator's pane, and the held request is delivered on the first tick on which the orchestrator is out of the block. An automatic answer to a blocked agent's question is not available yet, because it depends on a Herdr capability that does not exist today; the human action in the pane is not attested by herdr-soho.
- Unblocking: `job amend` on a `blocked` job sends a formal amendment and moves it back to `running` with the same workspace, worktree and pull request, when the job orchestrator accepts the prompt; while the orchestrator is in a dialog or a question the amendment is held (see Held requests) until the manual answer, and it is delivered on the first tick after the block ends.

### Budgets: active time and the blocked ceiling

- A job has two separate clocks. The active clock counts every moment the job is not `blocked` (`accepted`, `preparing`, `running`, `finishing`) against `--timeout` (`job_timeout`). The blocked clock counts only the time spent `blocked`, against `job_blocked_timeout`. Each transition into or out of `blocked` settles the elapsed interval into the matching clock and starts the next interval in the same atomic state write, so a crash leaves either the state before the transition or the state after it, never a mix.
- Blocking pauses the accrual of active time, not its enforcement: a job whose active time already reached 80% or 100% of `--timeout` before it was blocked still gets the warning or the timeout while it is blocked.
- `timeout_warning` fires once per job at 80% of the active budget; blocked time never brings a job closer to it. At 100% the stop path ends the job `timeout` with `motivo` `tempo esgotado`.
- `blocked_warning` fires once per job at 80% of the blocked ceiling. At 100% the stop path ends the job `timeout` (exit 9) with `motivo` `tempo de bloqueio esgotado`. When both budgets are exhausted in the same check, the active timeout wins.
- Both clocks and the warnings already emitted are persisted with the job, so a restarted supervisor continues them: it neither resets nor double-counts an interval, and it emits a missing warning once. The clocks are measured on the monotonic clock; a wall clock that moves backwards between supervisor runs never shortens or extends a budget.
- The terminal procedure in `finishing` has its own budget, `job_finishing_timeout` minutes recorded at `job start` (10 by default), persisted with the job and kept across a supervisor restart, so a restart neither resets nor extends it.
- Inside that budget the final push, the draft pull request step and the collaboration wait each have their own bound (5 minutes, 2 minutes and 2 minutes), and every command of those steps is also limited to what remains of its step.
- When the final push or the draft pull request step is cut by that budget, a job that would end `done` ends `failed` (exit 19) with `motivo` `tempo de finalização esgotado`; a `canceled` or `timeout` job keeps its own outcome, and a collaboration wait cut by the budget keeps the partial collaboration facts.

### Collaborations at the end of a job

- On every terminal path (`done`, `failed`, `timeout`, `canceled`, including a supervisor restarted on that path), before the terminal status becomes visible, the supervisor asks each active collaboration the job owns to finish: the same stop amendment as a cancel goes to its participants that belong to the job's team. A collaboration is owned by the job only when its record names the job's Herdr workspace and the job's repository root; any other record is never asked, stopped or reported.
- It then waits for those collaborations within the existing grace: the cancel's `--grace` or the timeout's grace on the stop path, 120 seconds after the orchestrator's report on `done` and `failed`. The deadline is never restarted.
- When the wait ends, it records one entry per owned collaboration under `colaboracoes` in `state.json` and `report.json`, with the evidence it can read safely (phase, round, revision, the last event, its report path and recorded hash, a fresh hash of report files inside the job's report directories and worktree, at most 8 MiB; the trusted base a report directory hangs from (the job's state directory, the system temporary directory, or the worktree itself) is resolved as given, its own links included, while every directory appended under that base must be a real directory, checked on the directory actually opened, so replacing one with a link, or with something that is not a directory such as a named pipe, directly or behind a link, is refused without waiting; the report file is opened through that opened directory, where a relative link that stays inside it is followed and a link that leaves it or any absolute link is refused, and a file that is not a regular file, such as a named pipe, is refused without waiting), and the outcome: `finished` (the job orchestrator ended it, with its verdict), `partial` (still active when the grace ended) or `unavailable` (its record could not be read or enumerated). An unreadable record counts as `unavailable` only when its id is already known to the job or its raw record still shows the job's own workspace and root, and is otherwise reported as friction only. A `partial` or `unavailable` entry is never presented as accepted work.
- Only after those facts are persisted does the job reach its terminal status. The supervisor then interrupts the collaborations it recorded as `partial` (`collaborate stop`, then `finalize --verdict reject`; never `accept`) and records `finalized_by_supervisor`, releases the workers and closes the workspace when the automatic close runs (see Release and automatic close), so no pane or process of the job is left behind. A later interruption never rewrites a recorded outcome.

### Release and automatic close

- Closing a job releases processes. On a terminal state (`done`, `failed`, `timeout`, `canceled`), right after the final push, the supervisor finishes the job's collaborations (above), releases workers and the job orchestrator, and runs `gc --yes` for the job's registered copies and processes. When the job's collaborations cannot be enumerated by the end of the grace, every collaboration already known to the job keeps its recorded outcome or is recorded `unavailable`, and the job still reaches its outcome, but the interruption, the release, `gc` and the automatic close are refused and a friction line names the failure; the workspace stays open until a later supervisor pass can enumerate the collaborations and finish them.
- With `job_auto_cleanup` on (the default), the supervisor runs the automatic close (the worktree removal only after a successful release; the workspace close as before, also after a failed release):
  - the job's own worktree (`<checkout>/.worktrees/job-<id>`, branch `job/<id>`) is removed only when it is clean (no change and no untracked file) and pushed (`HEAD` equals `origin/job/<id>`), read again right before the removal so a commit made while the agents were being released keeps the worktree (`not_pushed`), with `git worktree remove` without `--force`; the local branch stays, and in `workspace` mode there is no job worktree, so nothing is removed. When git removes the files and the registration but cannot delete the directory itself (on Windows the job's root pane still has it as its current directory), the empty directory that git no longer lists counts as removed and may stay until it is deleted by hand;
  - a worktree that is not removed fires a `needs_attention` event with `refs.motivo` one of `dirty`, `not_pushed`, `check_failed`, `remove_refused` or `not_owned` (at most one per motivo), and the worktree stays;
  - `folder_retained` fires once, with `refs.path` (the job's folder) and `refs.bytes` (its size);
  - the workspace close waits while a pane that is not the job's is still changing: with no pane outside the job the Herdr workspace (`herdr workspace close`) closes at once, and the close otherwise waits until the panes that are not the job's have not changed their screen for `job_human_input_minutes` — Herdr does not expose human input, so the wait is on the panes' screen changes; if the wait outlives six waits of `job_human_input_minutes`, a `needs_attention` event fires with `refs.motivo` `human_input` and the workspace is not closed;
  - `workspace_closed` fires once, right before the close: the supervisor runs in the workspace's root pane and ends with it. A close that then fails fires `needs_attention` with `refs.motivo` `close_failed` (once) and the workspace stays open; a later supervisor pass that finds `workspace_closed` already logged closes a workspace still listed by Herdr without firing it again.
- With `job_auto_cleanup` off, nothing is removed and the workspace stays open; only `folder_retained` fires.
- The job's folder (`jobs/<id>/`) is never removed. The three events are additive: `needs_attention` wakes the dispatcher; `workspace_closed` and `folder_retained` do not. `report.json` records `limpeza.worktree: "removed"` or `"kept"` and sets `limpeza.removivel` to true only when the tree is clean and `HEAD` equals `origin/job/<id>` (informational only). The remote branch and the pull request are never touched.
- Removing a worktree the automatic close kept is the user's decision, done by hand (`git worktree remove`, `gc`, `clean --older-than`). `clean --older-than` never removes `jobs/<id>/` while a decision is still unacknowledged.

### Decisions and acknowledgement

- The job orchestrator never writes to the shared memory store. A relevant decision is published as a `decision` event, and also aggregated into `report.json` under `memoria.global` or `memoria.projeto` with its event `seq`.
- The dispatcher is the single maintainer of the shared memory: it classifies each decision, stores it, then calls `job ack --id <id> --upto <seq>` (monotonic; a lower value is a no-op).
- A decision is pending while its `seq` is greater than `decisions_acked_seq`. `status` and `collect` report `decisions_pendentes`.
- `job close` with a pending decision exits 24 and lists the pending `seq` values; `--force` closes anyway and records a `cleanup` event with `motivo=force_sem_ack`.

## Status

`job status` and `job wait` print one line:

```json
{"id":"TASK-123-7f3a","status":"done","motivo":null,"eventos":{"total":42,"ultimo_seq":42},"decisions_acked_seq":0,"decisions_pendentes":[],"partial":true,"partial_collaborations":["c-0123456789abcdef"],"held_controls":[]}
```

- `partial` is always present: `true` when at least one of the job's collaborations ended `partial` or `unavailable`, otherwise `false`.
- `partial_collaborations` is always present: the IDs of those collaborations, sorted and without duplicates; `[]` when there are none. A job whose state predates these fields reads as `false` and `[]`, and the read changes nothing.
- The two fields are published before the terminal status, so a reader that sees `done` (or any terminal status) already sees the final partial facts, and they are never removed later. `report.json` carries the same two values and is written before the terminal status is saved in `state.json`; a crash between the two writes leaves the job `finishing` with its recorded outcome, and the restarted supervisor writes both again.
- `report_missing` is `true` only when the status is one that carries a stored report (`done`, `failed`, `timeout`, `canceled`, `collected` or `closed`) and no report is stored; in every other case the field is absent from the line. A legacy terminal job without a report reads as missing, never as complete; the exit code is unchanged, and the read writes nothing.
- `held_controls` is always present, at the end of the line: every `amend` or `send` request still pending in `control/` that carries its `control_held` mark in the event log — the request the supervisor is holding while the job orchestrator is in a dialog or a question — as `{"kind","seq"}` pairs in `seq` order; `[]` when there are none. A request without the mark, and one already retired, are out; the read writes nothing.

## Report

`report.md` is the job orchestrator's report (per item `[done]`, `[partial]` or `[skipped]`, plus a `## Memória` section with `Global` and `Projeto` subsections). `report.json` is generated by the supervisor:

```json
{
  "schema": 1,
  "id": "TASK-123-7f3a",
  "status": "done",
  "motivo": null,
  "resumo": "3 to 6 lines: what changed and what was proved.",
  "maquina": "machine-a",
  "repo": "example-org/example-repo", "base": "main",
  "branch": "job/TASK-123-7f3a", "head": "<sha>", "head_remoto": "<sha>", "sincronizado": true,
  "commits": [{ "sha": "<sha>", "titulo": "feat: ...", "push": "ok" }],
  "pr": { "numero": 12, "url": "<pull request url>", "estado": "draft" },
  "itens": [{ "n": 1, "item": "...", "estado": "done" }],
  "parciais": 0,
  "partial": false,
  "partial_collaborations": [],
  "testes": [{ "cmd": "go test ./...", "resultado": "pass" }],
  "revisao": { "verdict": "pass", "findings": 1, "severity": "P0 0, P1 0, P2 1, P3 0" },
  "artefatos": [{ "path": "report.md", "sha256": "<hex>" }],
  "blockers": [],
  "perguntas": [],
  "memoria": {
    "lida": [{ "escopo": "example-workspace/example-project", "paginas": ["..."] }],
    "global": [{ "tipo": "decision", "seq": 17, "resumo": "...", "motivo": "...", "refs": { "sha": "<sha>" } }],
    "projeto": [{ "tipo": "decision", "seq": 9, "resumo": "...", "motivo": "...", "refs": {} }],
    "decisions_total": 2, "decisions_acked_seq": 0
  },
  "equipe": { "fonte": "teams/example-org/example-repo.conf", "override_brief": ["lane.review.effort"] },
  "eventos": { "total": 42, "ultimo_seq": 42, "arquivo": "<abs>/events.jsonl" },
  "custo": { "inicio": "<ISO 8601 with offset>", "fim": "<ISO 8601 with offset>", "duracao_s": 2710, "workers": 3 },
  "logs": { "report_md": "<abs>", "state_dir": "<abs>", "friction": "<abs>" },
  "limpeza": { "workspace": "closed", "worktree": "kept", "removivel": true },
  "publico": { "verdict": "pass", "blockers": [], "ids": ["TASK-123", "PR#12"] }
}
```

- `done`: every item `[done]` or `[skipped]` with a reason, the acceptance checks pass, the review (from another model family) passes when code changed, and `sincronizado` is true. When the job orchestrator completed but a collaboration was still partial after the graceful closure, the job is still `done` (exit 0) and says so explicitly: `partial: true` and the collaboration IDs in `partial_collaborations`, with the detail in `colaboracoes`. A dispatcher must read those fields; `done` alone does not mean every collaboration finished.
- `failed`: partial items with no pending decision, a failed check or review, or a job orchestrator that settled without a report or disappeared.
- `blocked`: an unresolved question, dialog, quota or provider error; the question goes in `perguntas`.
- `memoria.global` holds suggestions that may apply beyond this repository; `memoria.projeto` holds notes that apply only to it. The job never writes them anywhere else; the dispatcher routes them.
- `limpeza`: `worktree` (`removed` or `kept`) and `removivel` are always present; `workspace` is present when the supervisor records the workspace outcome — `closed` when the workspace closed, `open` when it did not — and is absent from a report without a recorded outcome. The example shows the fields the report produces.
- `limpeza.removivel` is informational: true when the job worktree is clean and in sync with `origin/job/<id>`, the same facts the automatic close checks before removing it (see Release and automatic close); it never causes a removal.
- `partial` and `partial_collaborations`: as in Status; `colaboracoes` (present only when the job had collaborations) holds the per-collaboration evidence and outcome. `parciais` keeps counting the `[partial]` items of the orchestrator's report; partial items with no pending decision still end the job `failed`.
- `publico` is built from an allowlist of fields. It is the only part of a report that may appear on a public forge.
- When `report.json` is absent, `job collect` prints the missing line with `"report":null`, and that line carries `"report_missing":true` only when the status is one that carries a stored report (as in Status); the exit code stays 0, and `--md` without a `report.md` still exits 4 as before.

## Events

`jobs/<id>/events.jsonl` is append-only (one `O_APPEND` write per line under the job lock, synced before the wake hook runs); it is never rewritten or truncated.

```json
{"seq":7,"ts":"2026-01-01T12:00:00-03:00","tipo":"question","resumo":"short text","refs":{"agente":"orchestrator","report":"<abs>"}}
```

- `seq`: consecutive integers from 1 per job, with no gaps.
- `ts`: local time with an explicit UTC offset.
- `resumo`: at most 280 characters; no secrets, file contents or command output.
- `refs`: known keys only: `agente`, `papel`, `pane`, `sha`, `pr`, `report`, `brief`, `motivo`, `exit`, `seq_ref`, `path`, `bytes`.
- `decision` events also carry top-level `escopo` (`global` or `projeto`, a suggestion) and `motivo`.

| type | writer | wakes the dispatcher | when |
|---|---|---|---|
| `accepted` | supervisor | no | `start` validated |
| `preparing` | supervisor | no | clone, fetch, worktree, team |
| `worker_spawned` | supervisor | no | a worker was spawned (role, kind, lane) |
| `worker_done` | supervisor | no | a worker report landed (`done` or partial count) |
| `commit` | supervisor | no | a new commit on the job branch |
| `push` | supervisor | only on failure | push succeeded or was rejected |
| `pr_opened` | supervisor | yes | draft pull request opened |
| `checkpoint` | supervisor, orchestrator | no | `job checkpoint` or a declared milestone |
| `review_verdict` | supervisor | yes | a review report header (`findings`, `verdict`) |
| `question` | supervisor, orchestrator | yes, immediately | a question needs a decision |
| `blocked` | supervisor | yes, immediately | dialog, approval, quota, provider error, rejected push |
| `unblocked` | supervisor | no | back to `running` after an amendment |
| `amend_received` | supervisor | no | an `amend` or `send` was delivered (`refs.seq_ref`) |
| `delivery_uncertain` | supervisor | no | a delivery of the pending queue was interrupted by the cancel's stop (`refs.seq_ref` the request's counter; the request stays in `control/`) |
| `control_held` | supervisor | no | a pending `amend` or `send` is held while the job orchestrator is in a dialog or a question (`refs.seq_ref` the request's counter; at most one per request; it does not wake the dispatcher, because the `blocked` or `question` of the same block already does) |
| `decision` | orchestrator | yes | a relevant decision (`job note --tipo decision`) |
| `decision_acked` | supervisor | no | `job ack` received |
| `timeout_warning` | supervisor | yes | 80% of the active budget used (blocked time excluded) |
| `blocked_warning` | supervisor | yes | 80% of the blocked ceiling used |
| `failure` | supervisor | yes | preparation failure, worker gone, failed acceptance check, no report |
| `note` | orchestrator | no | an observation that is not a decision |
| `terminal` | supervisor | yes | final state (`refs.motivo`, `refs.exit`) |
| `cleanup` | supervisor | no | release and workspace close, emitted only after a successful release, with an informational `removivel` |
| `needs_attention` | supervisor | yes | the automatic close stopped: the job's worktree was not removed (`refs.motivo` `dirty`, `not_pushed`, `check_failed`, `remove_refused` or `not_owned`, at most one per motivo) or the close outlived six waits of `job_human_input_minutes` (`refs.motivo` `human_input`) or failed (`refs.motivo` `close_failed`) |
| `workspace_closed` | supervisor | no | the supervisor closes the job's Herdr workspace (once, right before the close, since the supervisor ends with it) |
| `folder_retained` | supervisor | no | the job's folder stays on the machine (`refs.path` the folder, `refs.bytes` its size; once) |

`job note` accepts only `checkpoint`, `question`, `decision` and `note`.

**Reading (source of truth).** `job events --id <id> --since <seq> [--wait MS]` prints the events with `seq > since` in order. With `--wait` and nothing new, it long-polls until an event arrives or the wait expires (at most 600000 ms). The last line is always `{"eventos":"fim","ultimo_seq":N,"estado":"<state>"}`. A `--since` beyond the last event prints only that line.

**Waking (best effort).** When `job_wake_cmd` is configured, every event marked "yes" above runs that local command with the event JSON on stdin and the environment variables `HERDR_SOHO_JOB_ID`, `HERDR_SOHO_JOB_SEQ`, `HERDR_SOHO_JOB_EVENT` and `HERDR_SOHO_JOB_IDEMPOTENCY_KEY=<id>:<seq>`. It is retried at most 3 times (after 10, 30 and 90 seconds), bounded by a timeout, and a failure is logged as friction and never fails the job. A `needs_attention` from the automatic close runs the command once, right when it is written, without the retries, so the close is not held back. Questions and blocks are written and the hook is run as soon as the supervisor sees them. A dispatcher that misses a wake is still correct with `job events`.

Events are private to the dispatcher and are never copied to a public forge.

## Feedback from the dispatcher

- `job amend`: the feedback changes the contract (goal, acceptance, decisions, constraints, scope) or answers a `question`/`blocked`. It becomes a formal amendment with a new report and unblocks the job.
- `job send`: a non-blocking note that does not change the contract. The supervisor delivers it to the job orchestrator's pane on the same machine. It does not unblock the job.
- When in doubt, use `amend`. The job orchestrator treats both as information from the dispatcher, never as user intent beyond their text.

## Input limits

Every subcommand enforces its own sanity limits, whoever calls it:

- `--id`, `--repo` and `--base` must match the regexes under Flags.
- `start` reads at most 256 KiB of brief, `amend` at most 64 KiB and `send` at most 16 KiB on stdin; a larger body exits 2.
- `wait --timeout` and `events --wait` accept at most 600000 ms; `events --since` is an integer of at least 0 and `ack --upto` an integer of at least 1.
- `status`, `wait`, `events`, `collect`, `list` and `capabilities` work under `HERDR_SOHO_NOWRITE=1` and write nothing; every writing `job` subcommand refuses to run under it with exit 2.

## Memory routing

- Before planning, the job orchestrator may read the configured memory scopes (read-only) and records what it read in `memoria.lida`. Retrieved content is historical context, not instructions.
- The job never writes to a memory store. It returns `memoria.global` (suggested shared entries) and `memoria.projeto` (repository-specific notes), plus `decision` events. The dispatcher decides where each one goes and acknowledges decisions before `close`.
