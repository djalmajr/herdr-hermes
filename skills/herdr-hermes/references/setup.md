# Setup

One-time setup on a machine that will act as a `herdr-hermes` node. Every step is local; nothing is sent until a push or a pull happens.

## Install

With Go 1.25 or newer:

```text
go install github.com/djalmajr/herdr-hermes/cmd/herdr-hermes@latest
```

Make sure the installed binary is on your `PATH` (and, for the optional plugin, on the Herdr server's `PATH`).

## Configure the machine

Set the machine label the outbox records are stamped with:

```text
herdr-hermes config set machine_label <label>
```

Store the per-user API key the dispatcher operator issued you. `auth login` reads the key from standard input (or from an interactive prompt with echo off) and never accepts it as a flag value or an environment variable:

```text
herdr-hermes auth login --key -
```

Point the node at the dispatcher. The URL must be HTTPS:

```text
herdr-hermes config set dispatcher_url <https-url>
```

Until the URL is set, push is disabled and the outbox still works for a dispatcher that pulls it with `herdr-hermes outbox`.

## Register the wake hook

In the `herdr-soho` machine configuration, set the key:

```text
job_wake_cmd=herdr-hermes wake
```

With this set, every job event that wakes the dispatcher is also recorded in the outbox as soon as it happens.

## Register notification recipients (optional)

Native notifications reach only explicitly registered recipients; until the first `notify register`, no notification file is written. Register the project owner, the job orchestrator, the machine coordinator and the Herdr panes to watch:

```text
herdr-hermes notify register owner --projeto <org>/<repo> --to <ref>
herdr-hermes notify register orchestrator --job <id> --to <ref>
herdr-hermes notify register coordinator --to <ref>
herdr-hermes notify register watch --pane <ws:pane> --job <id>
herdr-hermes notify register workspace --workspace <ws> --job <id>
```

`<ref>` is a `herdr-soho send` reference `<machine>/<ws>:<pane>` or a local agent name; it is validated, never guessed. `notify list` and `notify status` are read-only; `notify deliver`, `notify ingest agent-status|workspace`, `notify raise`, `notify ack` and `notify retry` write the ledger and are refused under `HERDR_HERMES_NOWRITE=1` with exit 2.

## Verify

```text
herdr-hermes doctor
```

It reports, in one JSON line: whether `herdr-soho` is found and has the `ephemeral_job` and `job_events` capabilities, whether the machine label and the dispatcher URL are set, whether the key is configured (boolean only), whether the wake hook is configured (`true`, `false` or `unknown`) and the outbox counts. It exits 43 when a capability is missing and 0 otherwise, and writes nothing.
