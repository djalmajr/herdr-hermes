# Ephemeral job contract

An ephemeral job is one self-contained orchestration run: a dispatcher hands `herdr-soho` a brief, the machine prepares an isolated checkout, a job orchestrator runs a team in Herdr, the work is kept in sync with a draft pull request, a structured report and an event log come back, and every resource the job opened is released. This document is the contract between `herdr-soho job` and any dispatcher that drives it.

JSON field names and enumerated values (`status`, `tipo`, `memoria`, ...) are part of the shared schema with the dispatcher and are kept as specified, including the ones in Portuguese; prose in this document is English.

The dispatcher chooses the machine and runs these subcommands on it through its own remote execution channel; the transport is outside this contract, and `herdr-soho` never routes a job to another machine. The dispatcher talks only to the job (through the commands below), never to the job's workers. People give feedback to the dispatcher, which relays it to the job; nobody drives the job's panes by hand.

## Commands

Every subcommand prints exactly one JSON line on stdout, except `events`, which prints JSON lines. Diagnostics go to stderr.

```text
herdr-soho job start   --id <id> --repo <org/repo> [--base <branch>] [--mode worktree|workspace]
                       [--brief <file|->] [--timeout <minutes>] [--dry-run]
herdr-soho job status  --id <id>                      # read-only; state, event count, last seq, pending decisions
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

`job start` runs outside Herdr: it validates, prepares the repository and worktree, resolves the team, creates the Herdr workspace `job-<id>` with `herdr workspace create --cwd <worktree> --label job-<id> --no-focus`, and starts `herdr-soho job supervise --id <id>` in its first pane. It returns as soon as the job is `running`. On Windows, `job start` never starts a Herdr server; when no running server is reachable it exits 4. `status`, `wait`, `collect`, `list` and `events` read files only. `amend`, `send`, `cancel` and `ack` write a control file under the job directory that the supervisor picks up; none of them needs to run inside Herdr.

Requirements: Herdr 0.9.1 or newer (0.9.3 or newer recommended on Windows). `capabilities --json` prints `{"schema":1,"worker_collaboration":1,"ephemeral_job":1,"job_events":1}`.

### Flags

- `--id` (required): `^[A-Za-z0-9._-]{1,64}$`. It is the idempotency key.
- `--repo` (required): `<org>/<repo>`, with `<org>` in the machine's `job_orgs` list and `<repo>` matching `^[A-Za-z0-9._-]{1,100}$`.
- `--base`: `^[A-Za-z0-9._/-]{1,100}$` without `..`; defaults to the brief's `base`, then to the remote default branch.
- `--mode worktree` (default): a worktree at `<checkout>/.worktrees/job-<id>` on branch `job/<id>` from `origin/<base>` after a fetch. `--mode workspace`: the checkout itself, only for jobs that change no code; a dirty or locked checkout is refused.
- `--brief`: a file or `-` (stdin). A remote dispatcher sends the brief on stdin.
- `--timeout`: wall-clock budget of the whole job in minutes, 1..1440 (default `job_timeout=120`). It is separate from the per-role timeouts; a worker's wait timeout never ends the job.

### Exit codes

| code | meaning |
|---|---|
| 0 | `done`, or a read/control command succeeded |
| 2 | bad usage, organization out of scope, invalid brief, no team configuration |
| 3 | unknown `--id` (`{"status":"not_found"}`) |
| 4 | Herdr failure or unavailable (including no reachable Herdr server on Windows) |
| 7 | `blocked`: a question, approval or dialog needs a person |
| 9 | job `timeout` |
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

Resolution at `job start`: the repository file; else the machine default; else the repository's `.agents/herdr-soho.conf` when it sets at least one `lane.*.roles`; else exit 2. A job never falls back to the built-in 2/3/4-pane presets. The resolved file, plus the brief's `equipe` overrides, is written into the job workspace's `session.conf`, and `report.json` records the source.

Example team shapes: implementer only (`panes=2`, `lane.build.roles=implementer`); implementer and reviewer (`panes=3`, adds `lane.review.roles=reviewer`); implementer, reviewer and security reviewer (`panes=4`, adds `lane.sec.roles=security-reviewer`, `lane.sec.panes=1`).

## Machine configuration

Keys in the user configuration file:

- `machine_label`: the canonical name of this machine, reported in `report.json` and checked against `brief.maquina` (examples: `machine-a`, `machine-b`).
- `job_orgs`: comma-separated organizations a job may check out.
- `job_repos_root`: checkout root (default `~/repo.git`, `%USERPROFILE%\repo.git` on Windows); checkouts live at `<root>/<org>/<repo>`.
- `job_timeout`: default job timeout in minutes.
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
accepted -> preparing -> running <-> blocked -> finishing -> {done | failed | blocked | timeout | canceled}
                                                                     -> collected -> closed
```

State lives in `<checkout>/.herdr-soho/jobs/<id>/`: `state.json` (atomic write: temp file next to the target, then rename; mode 0600), `events.jsonl`, `brief.json`, `brief.md`, `report.md`, `report.json`, `lock` and `control/`. The directory sits at the state root, outside any workspace id, so it survives the workspace.

- Idempotency: a lock per id. `start` again with the same brief hash returns the current state with `duplicate_of`; a different hash exits 20. Repeated `close`, `cancel`, `ack` and `checkpoint` calls with nothing to do exit 0.
- Error reports: once `--id` is valid, every terminal state writes `report.json`, including preparation failures, timeouts and cancellations.
- Cancel and timeout: the job orchestrator receives a "stop and report what you have" amendment, the job waits `--grace` seconds (default 120), commits and pushes leftovers, stops active collaborations, releases every worker with `--close --force`, and ends `canceled` (21) or `timeout` (9). A `timeout_warning` event fires at 80% of the budget.
- Blocked jobs: a `blocked` job stays alive, with its job orchestrator in its pane, waiting for `job amend`; there is no automatic deadline, and the owner decides when to cancel it.
- Unblocking: `job amend` on a `blocked` job sends a formal amendment and moves it back to `running` with the same workspace, worktree and pull request.

### Release (no automatic cleanup)

- Closing a job releases processes only. On a terminal state (`done`, `failed`, `canceled`, `timeout`), right after the final push, the supervisor stops active collaborations, releases workers and the job orchestrator, runs `gc --yes` for the job's registered copies and processes, and closes the Herdr workspace (`herdr workspace close`).
- Job folders and worktrees are never removed automatically. The worktree, its local branch and `jobs/<id>/` stay on the machine; `report.json` records `limpeza.worktree: "kept"` and sets `limpeza.removivel` to true only when the tree is clean and `HEAD` equals `origin/job/<id>` (informational only). The remote branch and the pull request are never touched.
- Removing a worktree or a job folder is the user's decision, done by hand (`git worktree remove`, `gc`, `clean --older-than`). `clean --older-than` never removes `jobs/<id>/` while a decision is still unacknowledged.

### Decisions and acknowledgement

- The job orchestrator never writes to the shared memory store. A relevant decision is published as a `decision` event, and also aggregated into `report.json` under `memoria.global` or `memoria.projeto` with its event `seq`.
- The dispatcher is the single maintainer of the shared memory: it classifies each decision, stores it, then calls `job ack --id <id> --upto <seq>` (monotonic; a lower value is a no-op).
- A decision is pending while its `seq` is greater than `decisions_acked_seq`. `status` and `collect` report `decisions_pendentes`.
- `job close` with a pending decision exits 24 and lists the pending `seq` values; `--force` closes anyway and records a `cleanup` event with `motivo=force_sem_ack`.

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
  "limpeza": { "workspace": "closed", "worktree": "kept", "panes_fechados": 4, "removivel": true },
  "publico": { "verdict": "pass", "blockers": [], "ids": ["TASK-123", "PR#12"] }
}
```

- `done`: every item `[done]` or `[skipped]` with a reason, the acceptance checks pass, the review (from another model family) passes when code changed, and `sincronizado` is true.
- `failed`: partial items with no pending decision, a failed check or review, or a job orchestrator that settled without a report or disappeared.
- `blocked`: an unresolved question, dialog, quota or provider error; the question goes in `perguntas`.
- `memoria.global` holds suggestions that may apply beyond this repository; `memoria.projeto` holds notes that apply only to it. The job never writes them anywhere else; the dispatcher routes them.
- `limpeza.removivel` is informational: true when the job worktree is clean and in sync with `origin/job/<id>`, so the user can remove it safely. Nothing is removed automatically.
- `publico` is built from an allowlist of fields. It is the only part of a report that may appear on a public forge.

## Events

`jobs/<id>/events.jsonl` is append-only (one `O_APPEND` write per line under the job lock, synced before the wake hook runs); it is never rewritten or truncated.

```json
{"seq":7,"ts":"2026-01-01T12:00:00-03:00","tipo":"question","resumo":"short text","refs":{"agente":"orchestrator","report":"<abs>"}}
```

- `seq`: consecutive integers from 1 per job, with no gaps.
- `ts`: local time with an explicit UTC offset.
- `resumo`: at most 280 characters; no secrets, file contents or command output.
- `refs`: known keys only: `agente`, `papel`, `pane`, `sha`, `pr`, `report`, `brief`, `motivo`, `exit`, `seq_ref`.
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
| `decision` | orchestrator | yes | a relevant decision (`job note --tipo decision`) |
| `decision_acked` | supervisor | no | `job ack` received |
| `timeout_warning` | supervisor | yes | 80% of the job budget used |
| `failure` | supervisor | yes | preparation failure, worker gone, failed acceptance check, no report |
| `note` | orchestrator | no | an observation that is not a decision |
| `terminal` | supervisor | yes | final state (`refs.motivo`, `refs.exit`) |
| `cleanup` | supervisor | no | release and workspace close; the worktree is always kept, with an informational `removivel` |

`job note` accepts only `checkpoint`, `question`, `decision` and `note`.

**Reading (source of truth).** `job events --id <id> --since <seq> [--wait MS]` prints the events with `seq > since` in order. With `--wait` and nothing new, it long-polls until an event arrives or the wait expires (at most 600000 ms). The last line is always `{"eventos":"fim","ultimo_seq":N,"estado":"<state>"}`. A `--since` beyond the last event prints only that line.

**Waking (best effort).** When `job_wake_cmd` is configured, every event marked "yes" above runs that local command with the event JSON on stdin and the environment variables `HERDR_SOHO_JOB_ID`, `HERDR_SOHO_JOB_SEQ`, `HERDR_SOHO_JOB_EVENT` and `HERDR_SOHO_JOB_IDEMPOTENCY_KEY=<id>:<seq>`. It is retried at most 3 times (after 10, 30 and 90 seconds), bounded by a timeout, and a failure is logged as friction and never fails the job. Questions and blocks are written and the hook is run as soon as the supervisor sees them. A dispatcher that misses a wake is still correct with `job events`.

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
