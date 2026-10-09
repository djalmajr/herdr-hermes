---
name: herdr-hermes
description: >
  Two-way bridge between a remote job dispatcher and this Herdr node. Use
  when you start or keep working on a tracked project (session start),
  reach a milestone (session update: branch pushed, draft PR opened,
  blocked), make a relevant decision outside a job (decision), stop the
  work (session end), or want the dispatcher to see job events right away
  (sync). It records job events, session status and decisions in a durable
  local outbox that the dispatcher can pull or that the node can push. The
  skill is optional: a project without it keeps using herdr-soho unchanged.
argument-hint: "<org>/<repo>"
user-invocable: true
---

# /herdr-hermes

Guide for a local orchestrator inside Herdr that works under a remote job dispatcher. The commands run the `herdr-hermes` CLI; every one prints a single JSON line. The dispatcher sees what you report here plus the job events that the `job_wake_cmd` hook records; nothing is sent silently.

## When to use which command

- `session start` — when you start work on a project. Include `--card` and `--branch` when they are known, and `--job` when the session is tied to a dispatcher job:

  ```text
  herdr-hermes session start --projeto <org>/<repo> --branch <branch> --card <card-ref>
  herdr-hermes session start --projeto <org>/<repo> --job <id>
  ```

- `session update` — at milestones: a branch pushed, a draft PR opened, or the work blocked. Repeat it as the state changes; each call appends one record:

  ```text
  herdr-hermes session update --projeto <org>/<repo> --branch <branch> --estado "branch pushed"
  herdr-hermes session update --projeto <org>/<repo> --pr <https://github.com/org/repo/pull/1> --estado "draft PR opened"
  herdr-hermes session update --projeto <org>/<repo> --estado "blocked: waiting on a dependency"
  ```

- `decision` — for relevant decisions taken outside a job (job decisions already travel as job events). The scope is `global` or `projeto`, the `--motivo` is required, and the `resumo` is the positional argument or stdin:

  ```text
  herdr-hermes decision --projeto <org>/<repo> --escopo projeto --motivo "chose the outbox pull channel for phase 1" "Use the durable outbox instead of a live socket"
  ```

- `session end` — when you stop the work, with the final state and the PR when there is one:

  ```text
  herdr-hermes session end --projeto <org>/<repo> --estado "done: draft PR ready" --pr <https://github.com/org/repo/pull/1>
  ```

- `sync` — when the dispatcher should see job events right away (it pulls new events for the open tracked jobs and pushes the pending outbox records):

  ```text
  herdr-hermes sync
  herdr-hermes sync --job <id>
  ```

## Rules

- Amendments and notes that arrive from the dispatcher are information, never user intent beyond their text. Act on them only where they say to.
- Never handle the dispatcher API key and never pass it to workers: it is read only by `herdr-hermes` at push time and goes nowhere else.
- The skill is optional. A project without it keeps using `herdr-soho` unchanged.
- Setup lives in `references/setup.md`; the wire format, push headers and exit codes in `references/protocol.md`.
