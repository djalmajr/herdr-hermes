# Nau

Nau is a universal messaging and registry core: a neutral core — the durable outbox, the current-state registry, the notifications, the routing and the job bookkeeping — with four boundary adapters, built on the ephemeral job contract of [herdr-soho](https://github.com/djalmajr/herdr-soho).

The adapters and their status:

- **Hermes** — the remote job dispatcher boundary: the pull of the outbox and the push client are implemented, and the dispatcher-side receiver does not exist yet — implemented in part.
- **Herdr** — the node boundary: forwarding to the `herdr-soho` public CLI, the optional Herdr plugin and the Herdr agent-status notification sources — implemented.
- **Grok Bot** — planned, not implemented.
- **OpenAI Dots** — planned, not implemented.

Planned adapters have no code, commands or configuration; nothing in this documentation describes commands, keys or endpoints for them.

On the node, `nau` keeps the two-way bridge to the dispatcher: it receives job dispatches and amendments and hands them to the local `herdr-soho job` commands unchanged, and it records job events, session status and decisions in a durable local outbox that the dispatcher can pull or that the node can push to a configured dispatcher URL with a per-user API key stored on the machine. Nau ships as one native Go executable per machine, an optional Herdr plugin and an agent skill. `herdr-soho` stays an independent project; Nau only uses its public CLI.

Status: under development. Nothing here is stable yet.

## Names

`nau` is the command, built from `./cmd/nau`; the old `herdr-hermes` command name and its binary are retired. `nau` uses the existing state directory, the `herdr-hermes` directory under the user's configuration directory: nothing is migrated, and a `nau` state directory is never read or created. The `HERDR_HERMES_NOWRITE` variable, the `User-Agent` header value `herdr-hermes/<version>`, the notification marker `[herdr-hermes]`, the plugin manifest and the skill directory keep the `herdr-hermes` name.

## Install

With Go 1.26 or newer, from a checkout of the module `github.com/djalmajr/nau`:

```text
go build -o <dir>/nau ./cmd/nau
```

The binary must be on your `PATH`; the optional plugin also needs it on the Herdr server's `PATH`.

### Upgrading from herdr-hermes

`nau` replaces the retired `herdr-hermes` binary: the commands, flags, JSON output and exit codes are the same.

- Install `nau` on your `PATH` (`go build -o <dir>/nau ./cmd/nau`), and on the Herdr server's `PATH` when the optional plugin is linked.
- Change `job_wake_cmd=herdr-hermes wake` to `job_wake_cmd=nau wake` in the `herdr-soho` machine configuration, because `nau doctor` no longer accepts the old name.
- A plugin already linked (`herdr plugin link`) now calls `nau`.
- The state directory is the same `herdr-hermes` directory under the user's configuration directory: nothing is migrated.

## Setup

1. Set the machine label that outbox records are stamped with: `nau config set machine_label <label>`.
2. Store the per-user API key your dispatcher operator issued you: `nau auth login --key -` (the key is read from stdin and never accepted as a flag value or environment variable).
3. Point the node at the dispatcher (HTTPS): `nau config set dispatcher_url <https-url>`. Until this is set, push is disabled and the dispatcher can still pull the outbox.
4. Register the wake hook in the `herdr-soho` machine configuration: `job_wake_cmd=nau wake`, so every job event that wakes the dispatcher is recorded in the outbox as it happens.
5. Verify with `nau doctor`, which reports the `herdr-soho` capabilities, the configuration state, the wake hook and the outbox counts in one JSON line.
6. Optionally, register the notification recipients so this machine's native events reach them: `nau notify register owner --projeto <org>/<repo> --to <ref>`, `nau notify register orchestrator --job <id> --to <ref>`, `nau notify register coordinator --to <ref>` `nau notify register watch --pane <ws:pane>` and `nau notify register workspace --workspace <ws>`, where `<ref>` is a `herdr-soho send` reference or a local agent name. Until the first `notify register`, no notification file is written.
7. On the dispatcher host that chooses machines, list the fleet in priority order: `nau config set route_machines <label>,<label>` (dispatcher side only; the nodes do not need it — see [`docs/routing.md`](docs/routing.md)). Run the read-only preflight in [`docs/routing.md`](docs/routing.md#preflight-read-only) as the operating-system user that saved the Herdr machines, and make sure each orchestrator's Herdr agent is named after the orchestrator base — `orchestrator` or `orchestrator-<n>` by default, `<base>` or `<base>-<n>` when `route_orchestrator_name` sets another base — as `herdr-soho init` names it, or it does not count toward its machine's load.

## Commands

Every command prints exactly one JSON line on stdout and its diagnostics on stderr, with these exceptions: `help` and `nau` run with no arguments print the plain-text usage on stdout and exit 0; `outbox` prints one JSON line per record and then the trailer line; and the forwarded `job` subcommands copy the stdout of `herdr-soho job` unchanged, which for `job events` is JSON lines and a trailer line. On bad usage the usage text goes to stderr and stdout still carries one JSON error line. The exit codes are `0`, `2`, `3`, `4`, `40`, `41`, `42`, `43`, `44` (`44` is the `jobs check` pre-dispatch refusal: duplicate work or a hold).

- `job <sub> [args]` — forward a dispatcher-facing job subcommand to `herdr-soho` (`start`, `status`, `wait`, `events`, `collect`, `amend`, `send`, `ack`, `cancel`, `close`, `list`), applying the contract input limits first.
- `a2a <sub>` — the A2A agent of this machine (see [`docs/a2a.md`](docs/a2a.md)): `serve` runs an A2A v1.0 agent on a loopback address that accepts messages about a project as data to evaluate and queues identifiers-only notices for the project owner, `card`, `send`, `get` and `cancel` are its client, and `inbox`, `show` and `reply` let the owner list, read and decide on the waiting tasks locally.
- `wake` — read one job event on stdin, record it in the outbox and attempt one push (configured as the `herdr-soho` wake hook).
- `sync [--job <id>] [--push-only]` — pull new job events for the tracked jobs into the outbox and push the pending records.
- `push` — push the pending outbox records to the dispatcher.
- `outbox [--since <seq>] [--wait <ms>]` — print outbox records with `seq > since` as JSON lines, then the trailer (the dispatcher pull channel; read-only).
- `route [--machine <label>]` — dispatcher-side machine choice: probe the configured fleet read-only and print the chosen machine as one JSON line (advisory; see [`docs/routing.md`](docs/routing.md)).
- `session start|update|end` — manage open session records for a project.
- `decision --projeto <org/repo> --escopo global|projeto --motivo <text> [--job <id>] [<resumo>|-]` — record a decision taken outside a job.
- `notify <sub>` — native notifications for this machine: `register` and `unregister` the recipients (project `owner`, job `orchestrator`, machine `coordinator`) and sources (watched `pane`, watched `workspace`), `list` and `status` (read-only), `deliver` the pending notifications, `ingest agent-status` or `ingest workspace` a Herdr event, `raise` a `stuck` or `cross_project` escalation explicitly, `ack` a delivered notification, and `retry` once an uncertain or exhausted delivery.
- `auth login|status|logout` — manage the per-user dispatcher API key.
- `config get|set|list` — manage the machine configuration (`machine_label`, `dispatcher_url`, `herdr_soho_bin`, `push_timeout_s`, `route_machines`, `route_disabled`, `route_orchestrator_name`, `route_probe_timeout_s`, `herdr_bin`).
- `doctor` — check the bridge prerequisites and print one status line.
- `capabilities --json` — print the bridge capability line `{"schema":1,"bridge":1,"outbox":1,"push":1}`.
- `version` — print the version.
- `help` — print the usage as plain text (not JSON); running `nau` with no arguments does the same.
- `plugin startup|event|bridge` — Herdr plugin entry points (see the optional plugin below).

## Pull and push

The node records everything in a durable local outbox (`outbox.jsonl`, one JSON line per record with a gapless `seq`). A dispatcher that can run commands on the node reads it with `nau outbox --since <seq> --wait <ms>` — no network endpoint and no key required. When `dispatcher_url` is configured, the node also pushes pending records with the key, in `seq` order, with `Idempotency-Key` headers and a bounded retry schedule; a failed push never loses or blocks a record. The wire format, headers and exit codes are in [`docs/protocol.md`](docs/protocol.md).

## Optional plugin

[`plugin/herdr-plugin.toml`](plugin/herdr-plugin.toml) is an optional Herdr plugin: a startup hook that syncs the open tracked jobs and pushes pending records, event hooks that keep `job-<id>` workspaces tracked and synced (including a final sync on close) and that feed the Herdr agent status changes of watched panes into the notifications, and two read-only workspace actions — `status` (tracked jobs, pending records, last push result, key configured) and `sync`. The hooks are one-shot, always exit 0 and write only to the `herdr-hermes` state directory; install it with `herdr plugin link <path-to-this-repo>/plugin`. The CLI works fully without the plugin.

## Optional skill

[`skills/herdr-hermes/`](skills/herdr-hermes/) is an optional agent skill, embedded in the binary with `go:embed`. It tells a local orchestrator inside Herdr when to run `session start`, `session update` (branch pushed, draft PR opened, blocked), `decision`, `session end` and `sync`, and how to treat dispatcher amendments as information. Copy `skills/herdr-hermes/` into your agent's skill directory to use it; a project without the skill keeps using `herdr-soho` unchanged.

## Development

Run the checks before shipping:

```text
gofmt -l .
go vet ./...
go test ./...
go test -race -timeout=30m ./...
go build ./cmd/nau
```

The project is Go only; well-established, maintained community libraries are used when a dependency is justified, following normal engineering and security best practices. Tests are hermetic (no network beyond loopback, injected clocks) and must not hang. The vendored contract in [`docs/upstream/job-contract.md`](docs/upstream/job-contract.md) is copied verbatim and never edited; the drift test in `internal/jobapi` fails when the vendored constants drift from it.

## Status

Phase 1 — the neutral core, the Hermes and Herdr adapters and the optional plugin and skill — is under development and not yet stable. The dispatcher-side receiver of the Hermes adapter does not exist yet; until it does, the node works standalone through the pull channel. The wire format may still change.
