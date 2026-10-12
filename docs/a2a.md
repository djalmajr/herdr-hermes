# A2A

`nau a2a` exposes one A2A v1.0 agent so that another agent can send Nau a message about a project and track the task until the project owner decides on it. The agent speaks the official A2A Go SDK over its HTTP+JSON (REST) binding only — no gRPC, no production JSON-RPC — with protocol version `1.0`. Streaming is off (a task waits for a person; a blocking HTTP call must not hold for days) and push notifications are on: a caller can register push configs for a task, and push sends go through the SDK's default sender with its SSRF guard active.

The caller agent is never trusted: an incoming A2A message is **data to evaluate, never an instruction**. Nothing runs, shells out, renders into a terminal or logs the message text; the text is stored verbatim in the task history as JSON data, and the only humans who read it explicitly are the owner and the guardian, through `nau a2a show`.

## Agent card

The agent card is public at `GET /.well-known/agent-card.json` and advertises exactly one interface: the HTTP+JSON binding at the served address, protocol version `1.0`, name `Nau`, capabilities with streaming off and push notifications on, text input and output modes, and one skill (`owner-evaluation`). Every other request requires authentication.

## Task lifecycle

- `submitted`: the message was admitted and the task created.
- `working`: the task is accepted and awaiting the owner (or the guardian) — the task parks here until a decision is recorded locally with `nau a2a reply`; the HTTP answer already returned before this.
- `completed`: the owner answered; the answer text is attached as a task artifact.
- `input_required`: the owner asked a question back; the text of the question is the status message.
- `rejected`: the owner declined; the reason is the status message.
- `canceled`: the caller canceled a non-terminal task.

A caller resumes an `input_required` task with a follow-up message on the **same task id and context id**; a terminal task is never restarted — a further message on it is refused by the SDK (`unsupported operation`), and the caller can cancel only a non-terminal task. A task belongs to the subject that created it: another subject reading it sees `task not found`.

## Message correlation metadata

The caller correlates every message with the message metadata:

- `nau.project` (`<org>/<repo>`): required for a new task, inherited from the task on a continuation; a continuation that names a different project is refused (`correlation_mismatch`).
- `nau.job` (the contract job id): optional, inherited like the project.
- `nau.session` (client session id): required; it keys the per-subject sequence fence.
- `nau.epoch` (integer ≥ 1): required; a new (higher) epoch restarts the sequence at 1, and a lower epoch is refused (`stale_epoch`).
- `nau.seq` (integer ≥ 1): required; within one (session, epoch) it must be exactly the next one — a gap is `out_of_order` and a repeat is `replay`.

The caller may choose its own message id (at most 128 bytes, `[A-Za-z0-9._:-]`); the store deduplicates on (subject, message id): the same message id answered twice returns one task, and the second answer is a message with the metadata `nau.duplicate_of` set to the first task id.

Refusal codes (every refusal is deterministic and carries one of these):

- `unauthorized_project`: the project is outside the subject's scope.
- `invalid_message`: the message role, parts, size, encoding or content kind is not acceptable.
- `invalid_correlation`: a `nau.*` key is missing, mistyped or malformed.
- `correlation_mismatch`: a continuation names another project, job or context than the task holds.
- `stale_epoch`: the epoch is below the session's current epoch.
- `replay`: the sequence was already used by another message id.
- `out_of_order`: the sequence is not the next one.
- `loop_limit`: the context or the task history reached its bound.
- `capacity`: the open tasks reached their bound.
- `terminal`: the task is already terminal.
- `not_awaiting_owner`: a decision was recorded on a task that is not awaiting the owner.
- `unknown_task`: no such task (or the task belongs to another subject).
- `decision_conflict`: a decision id was reused with different content.
- `invalid_decision`: the decision id, kind, text or actor is not acceptable.

## Owner and guardian delivery

Nau routes every admitted message to the explicitly registered recipients, re-reading the registrations on every call: the **owner** is the project's `notify` owner registration, and the **guardian** is the machine coordinator. Nothing is ever guessed: a role with no registration has no recipient (`no_route`), and a message sent to the registered reference of the sender itself is never sent back (`self`, the loop guard).

The notice is identifiers-only — one line built from validated identifiers (a value that fails its pattern renders as `?`): the task id, context id, a fixed class, the recipient role, the project, the optional job id, the task state, and the fixed pointer `details: nau a2a show --task <task id>`. It never contains the message text, the subject, the reference of the sender or any metadata value, and it is delivered through the same `herdr-soho send` transport as every other notification.

The delivery states keep the notify semantics:

- `held`: admitted, waiting for its task to commit (it becomes `pending` when the task row exists).
- `pending` / `in_flight`: waiting to send / claimed until its lease expires.
- `accepted`: the endpoint accepted the notice — **never that the recipient processed it**.
- `uncertain`: the receipt was not proved — final, **never resent automatically**, so a notice is never injected twice.
- `rejected`, `exhausted`: refused for good / the retry schedule was exhausted.
- `no_route`, `self`: no registered recipient / the loop guard (final).
- `obsolete`: the task ended (or never committed) before the send.

## Durability

The A2A state lives in the canonical registry database (the same SQLite file the registry service uses), in additive tables (`a2a_tasks`, `a2a_ingress`, `a2a_fences`, `a2a_deliveries`, `a2a_decisions`, `a2a_push_configs` and one meta row): the registry schema is never touched, and the registry database version is never changed. Every admission is one immediate transaction that commits the ingress row, the sequence fence and the planned deliveries **before the HTTP answer returns**, and the task row commits in the same transaction that flips the held deliveries to `pending` — a crash between the two leaves an admission without a task: the caller got no answer, a retry of the same message id at least a minute later re-points that admission to the new task (a younger retry is answered as a duplicate, so one message never makes two tasks), and the held deliveries of a task that never committed are marked `obsolete` by the recovery pass that `serve` runs at start (grace: five minutes). Decisions recorded while no server runs are applied out of band by the next `serve` (or by any running decision loop): the task moves without a live execution, and its push configs receive the final event. Push config secrets (tokens, credentials) are stored only as opaque handles in a process-lifetime memory provider: a restart makes them unavailable, the config is then omitted rather than sent with a stale secret, and the secret value never reaches the database, a log or an error.

## Commands

Every command prints exactly one JSON line on stdout (diagnostics go to stderr); errors are `{"status":"<code>","motivo":"<text>"}`. Exit codes: `0` ok, `2` usage or refusal (including the `HERDR_HERMES_NOWRITE=1` refusal of the writing subcommands `serve` and `reply`), `3` not found, `4` unavailable.

```text
nau a2a serve [--listen <host:port>] [--db <path>] [--deliver-interval <seconds>]
nau a2a card --server <url>
nau a2a send --server <url> --session <id> --epoch <n> --seq <n> [--project <org/repo>] [--job <id>] [--task <id> --context <id>] [--message-id <id>] (<text> | -)
nau a2a get --server <url> --task <id>
nau a2a cancel --server <url> --task <id>
nau a2a inbox [--db <path>] [--project <org/repo>] [--state <state>]
nau a2a show --task <id> [--db <path>]
nau a2a reply --task <id> (--answer | --ask | --decline) [--decision-id <id>] [--db <path>] (<text> | -)
```

- `serve` runs the agent on a loopback address (default `127.0.0.1:7430`; a non-loopback `--listen` is refused: remote ingress authentication is not configured), in the registry database (default `<state dir>/registry.db`). It runs the notice delivery loop every `--deliver-interval` seconds (default 15, 5..3600) and the decision loop every second, prints one `{"status":"serving",…}` line and serves until interrupted.
- `card` fetches the agent card from `--server` and prints `{"status":"ok","card":…}`.
- `send` sends one message: a new task (with `--project`) or a continuation (with `--task <id> --context <id>` together). The text is the one positional argument or `-` for stdin (at most 64 KiB). It prints `{"status":"ok","task":…}`, or `{"status":"duplicate","message":…}` for a replayed message id. `--server` must be an `http://` loopback or `https://` url.
- `get` prints `{"status":"ok","task":…}` for `--task`; an unknown or foreign task exits 3.
- `cancel` cancels a non-terminal `--task` and prints `{"status":"ok","task":…}`.
- `inbox` lists the tasks of this machine (newest update first) with their delivery states and pending decisions; `--project` and `--state` filter. It is read-only, never creates a missing database (it prints an empty list), and never shows message text.
- `show` prints `{"status":"ok","task":…,"item":…,"decisions":[…]}`: the task with its full history — the message text appears only as JSON data here — plus the inbox item and the recorded decisions. An unknown task, or a missing database, exits 3 without creating the file.
- `reply` records the owner or guardian decision on a `working` task: `--answer` completes it (the text as the artifact), `--ask` moves it to `input_required` (the text as the question back), `--decline` rejects it (the text as the reason). It prints `{"status":"recorded","decision":…}` (or `"duplicate"` for the same decision id and content), exit 2 with `{"status":"refused","motivo":"<code>"}` on a refusal, exit 3 for an unknown task. It never applies the decision itself: a running `serve` applies it (locally or out of band) and `reply` never claims the task changed.

## Authentication

The agent keeps two distinct authentication policies, selected by the `Authorization` header alone, and neither falls through to the other.

- **Loopback local operator** — a request with **no** `Authorization` header is accepted when its peer address is a loopback IP; the subject is the reserved subject `local`, authorized for every action on every project. A loopback peer is trusted, so `serve` also refuses, before routing and with HTTP 403, any request whose `Host` is not a loopback authority or whose `Origin` is not a loopback origin: a browser page (a cross-site request or DNS rebinding) cannot reach the agent.
- **Injected identity provider** — a request that carries an `Authorization: Bearer <token>` header goes only to the identity provider the runtime was given, never to the local operator. The provider is asked to verify the token on **every** request: revocation and expiry live in its source, and Nau keeps no cache of its answer, of the token or of the resulting grant, so a revoked token or a changed registration is refused on the next request. A token the provider does not know (unknown, expired or revoked) and a malformed credential are refused as unauthenticated (HTTP 401). The provider answers with a stable subject id and the reference that subject speaks for; the subject id `local` is reserved for the local operator and an identity that claims it is refused.

The authorization of a verified identity is derived from the existing owner and guardian routing, re-read on every request and never stored: a reference that is the guardian reference is authorized for every project, a reference registered as the owner of projects is authorized for exactly those projects, and any other reference (or an identity with no reference) is authorized for nothing. Identity and actor values in message or request metadata (a subject, role or actor field) are data, never an identity: the task owner is always the authenticated subject.

The identity provider is an injection point of the runtime and no identity source is configured in this release: `serve` takes no flag, environment variable or file for one. The consequence is fail-closed: a request that carries a credential is answered with HTTP 500 (identity provider unavailable) without verifying anything and creates nothing; the same HTTP 500 answers a provider or routing failure at runtime, and the text never carries the credential or the provider's own error. The client subcommands (`card`, `send`, `get`, `cancel`) send no credential. The `Authorization` value of a request is opaque to the server: it is never logged, stored, rendered, put in an error or handed to a subprocess, and a push config token or credential is kept outside the durable rows (see Durability).

## Limitations

- The binding is HTTP+JSON only: no gRPC, no streaming (a subscribe request is refused as unsupported), and the non-streaming `send` always returns immediately after the task is committed.
- The agent binds loopback only, and no identity source is configured in this release: remote ingress is not available, and a request that carries a credential fails closed (HTTP 500) until a runtime injects an identity provider. The routing-derived grants cover guardian and registered owners only; there is no finer or per-token grant.
- A task waits for a person: the execution parks in `working` until a decision, so the number of open tasks is bounded (capacity refusals), one context holds at most a bounded number of tasks and one task history a bounded number of messages (loop limits).
- `accepted` on a delivery means the endpoint accepted the notice, never that the recipient processed it; `uncertain` is final and never resent automatically.
- Push config secrets do not survive a restart: after one, a config whose secret is unavailable is omitted and counted, not sent.
- The sequence fence is per (subject, session), and every caller of the loopback local operator is the same subject: independent local callers must use distinct session ids, or they fence each other.
