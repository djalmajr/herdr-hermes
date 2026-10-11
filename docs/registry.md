# Registry

`herdr-hermes serve` is the central mode of the same binary: a live registry of the **current state** of the fleet, kept in one SQLite database and served over HTTP. `herdr-hermes jobs` is its client: one inventory across every node and the mandatory pre-dispatch check. History and decisions are not kept here; the registry only answers what is true now.

## What it records

- Active jobs per node: the owner agent, the card, the workspace, the phase (the type of the latest job event), the idle time, and whether the job waits for a person.
- Nodes: online, the Herdr version, the `herdr-soho` version and the runners that work there (when reported), and a maintenance flag.
- Panes waiting for a person: the panes Herdr reports as `blocked` (trust, permission and question dialogs) and the jobs whose latest event is a `question` or a `blocked`.
- Branches and pull requests in use per job, so a new job never overlaps them.
- Folders retained on a node, awaiting a person's decision: path, size and request. A record is never a permission to delete; the registry removes only the ended rows past the retention.
- Holds and priorities for a project, an agent or a card.
- The last events of each job (20 per job).

## Producers

The registry infers nothing that no producer reports; an unknown value is `null`.

- **Bridge push.** `serve` accepts the push request of [`protocol.md`](protocol.md) ("Push") unchanged: `POST /v1/ingest` with one outbox record as the body. A node bridge pushes to it when its `dispatcher_url` is set to `http://<serve address>/v1/ingest`. Job events, dispatch, amend, session and decision records are projected into the current state (see "Projection"). The `Authorization` header is neither checked nor stored: authentication of the push is outside this release, which is why `serve` binds a loopback address only.
- **Herdr inventory poll.** Every `--poll-interval` seconds `serve` runs, for each machine of `--machines` (default: the `route_machines` configuration), the read-only public Herdr CLI: `herdr [--machine <label>] workspace list`, `agent list` and `status server`, as argv subprocesses with a 20-second deadline each. It keeps only the workspace ids, labels and agent statuses, and the pane ids, workspace ids, agent names and statuses of the agent list: never working directories, titles or terminal content. The stored strings are bounded: a workspace id or label and a pane's workspace id at most 1024 bytes, a pane id or agent name at most 256 bytes; a longer value, or one that is not valid UTF-8, is cut to the bound with as much of its prefix as fits plus the marker `[truncated:<digest>]`, the 64 lower-case hex digits of the SHA-256 of the original bytes, so two values that share a prefix still differ. The classification runs on the bounded values: a truncated name never resolves to an owner, a type or a ref, a truncated `job-<id>` label never names a job id, and a truncated `hm-` label is still an `hm-*` workspace keyed by its bounded id with a `null` owner; nothing is ever forged. A `workspace list` or `agent list` answer that is not valid UTF-8 anywhere in its raw bytes is a malformed answer: the whole poll of that machine fails with `probe_malformed` and nothing of that answer is stored; an escaped unpaired surrogate such as `\ud800` is valid JSON and is decoded to U+FFFD, not refused.
- **Operators and agents** report what no other producer can: holds and priorities (`jobs policy`), retained folders (`jobs folder`), and the node maintenance flag, `herdr-soho` version and runners (`jobs node set`).

## Ownership from the workspace name

A workspace named `hm-<agent>-<type>-<ref>` (type `impl`, `review`, `fix` or `plan`) is a job owned by `<agent>`, keyed by its Herdr workspace id. The name is split on `-`; it resolves only when exactly one token is one of the four types with at least one token before and after it, for example `hm-example-agent-impl-card12` (agent `example-agent`, type `impl`, ref `card12`). A name with no such token or with more than one (`hm-a-review-impl-x`) is not guessed: the job is listed with a `null` owner. The owner is derived again from the label on every successful poll, so a renamed workspace changes its owner and ref, and a label that no longer resolves clears them. A workspace named `job-<id>` is the workspace of the contract job `<id>`. Any other workspace is not a job and is ignored. A claim made with `--workspace <name>` gives its card, project, branch and pull request to the workspace job when it appears, and its agent when the name did not resolve.

## Projection

- `job_event`: the job keyed by its id. `question` and `blocked` mark it as waiting for a person and `unblocked` clears that; `terminal` marks it terminal with its `refs.exit`, waiting for a person when the exit is 7, 11 or 14 (the contract's blocked exits); a job ended with one of those blocked exits stays alive for the pre-dispatch check, and a later `unblocked` returns it to running (no longer terminal, its exit cleared), while a terminal with any other exit is never reopened; `cleanup` removes the job (the fence row of a job never seen consumes the jobs budget, and the ended row is kept until the ended retention, default 30 days, so a replayed event cannot bring it back); `refs.pr` fills the pull request. Every other type, including types added later to the contract, only moves the phase. Events are deduplicated by job and seq; an event older than the latest one is stored but does not move the state. Only the contract refs `agente`, `papel`, `pane`, `sha`, `pr`, `motivo`, `exit` and `seq_ref` are kept: local paths (`report`, `brief`) are dropped. A refused record (a closed job, a seq already stored, or a replay older than every stored event) changes nothing. An event `tipo` must match `^[a-z][a-z0-9_]{0,63}$`, a record or event `ts` is at most 35 bytes (an RFC 3339 time with nanoseconds and a numeric offset), and a kept ref value must be a string of at most 200 bytes, a number or a boolean; other values are dropped. An event `ts` is stored and returned as the same instant to the second with a numeric offset, so a `Z` input reads `+00:00`.
- `dispatch` and `amend`: the job exists from then on (a new job consumes the jobs budget like every new row).
- `session`: a session with a job sets the card, branch and pull request of that job; a session without a job is listed as active work keyed by its project and branch until its `end`. A newer record replaces every valid field it carries and keeps the ones it omits; records older than the latest applied one, or of the same second with an outbox `seq` not above it, are ignored, and an `end` that arrives before anything else of its session closes it at once (the end-first row consumes the sessions budget). A newer `start` or `update` on the project and branch of an ended session opens a new session that carries nothing of the old one.
- `decision`: only the job's last activity; decisions are not kept here.
- A job created or updated by a pushed record keeps that provenance: a poll that stops listing its workspace only clears the workspace fields, and the job leaves only with its `cleanup` event (or its session `end`).
- Idle time is the time since the latest event or record of the job, or since the poll last saw its workspace working or change status; it is never more precise than the poll interval. The last activity of a job and the last contact of a node move forward only, compared by instant: a poll that started before a newer push, or a record older than the stored contact, never moves them back, while the poll's own time and status are always recorded.
- A node is online when a poll succeeded or a record arrived in the last 3 minutes. When a poll fails, the jobs of that node stay listed with `obsoleto: true`: unknown is not free.

## Pre-dispatch check

Before an agent opens a job, a workspace or a branch, it runs `herdr-hermes jobs check` and opens nothing unless the check exits 0. The check is atomic: in one SQLite immediate transaction it compares the request with the active jobs (not terminal, or terminal with a blocked exit 7, 11 or 14), the unexpired claims and the holds, and either refuses or records a claim, so two concurrent checks for the same work never both pass. A request names its agent and at least one of:

- `--workspace`: the same workspace name, and for an `hm-` name the same agent and ref (an `impl` and a `review` workspace of the same ref overlap, and a ref followed by a collision suffix `-2` to `-9`, as in `hm-example-agent-fix-pr64-2`, is the same ref as `pr64`; a longer last part such as `card238-241` stays a distinct ref);
- `--card`: the same card (case-insensitive);
- `--branch` with `--projeto`: the same branch of the same project;
- `--pr` with `--projeto`: the same pull request (`67`, `#67` and a pull request URL are the same).

A hold on the agent, the project or the card refuses the request with `status: hold`; any overlap refuses it with `status: duplicate`; both list every conflict. A claim lasts `--ttl` seconds (default 3600, 60..86400) or until `jobs release --claim <id>`. Jobs of a node whose poll failed still count.

## Commands

```text
herdr-hermes serve [--listen <host:port>] [--db <path>] [--machines <label,...>] [--poll-interval <seconds>] [--max-jobs <n>] [--max-sessions <n>] [--ended-retention-days <n>]
herdr-hermes jobs [--maquina <label>] [--agente <agent>] [--server <url>]
herdr-hermes jobs check --agente <agent> [--card <card>] [--projeto <org/repo>] [--branch <branch>] [--pr <n>] [--workspace <name>] [--maquina <label>] [--ttl <seconds>] [--server <url>]
herdr-hermes jobs release --claim <id> [--server <url>]
herdr-hermes jobs policy set --escopo projeto|agente|card --valor <value> [--hold] [--prioridade <n>] [--motivo <text>] [--server <url>]
herdr-hermes jobs policy clear --escopo projeto|agente|card --valor <value> [--server <url>]
herdr-hermes jobs folder add --maquina <label> --caminho <path> --pedido <text> [--tamanho <bytes>] [--job <id>] [--server <url>]
herdr-hermes jobs folder clear --maquina <label> --caminho <path> [--server <url>]
herdr-hermes jobs node set --maquina <label> [--manutencao on|off] [--motivo <text>] [--soho-version <version>] [--runners <a,b>] [--server <url>]
```

- `serve` listens on `--listen` (default `127.0.0.1:7429`), which must be a loopback address; a non-loopback address, or a name that does not bind to a loopback address, exits 2. The database defaults to `registry.db` in the state directory. `--max-jobs <n>` and `--max-sessions <n>` (each 1 to 1000, default 1000) bound the total retained rows of the jobs and sessions kinds, and `--ended-retention-days <n>` (7 to 30 whole days, default 30) how long an ended row is kept; a value out of range exits 2. It prints one JSON line when it is listening, `{"status":"serving","listen":"127.0.0.1:7429","maquinas":["local","node-b"],"poll_s":60}`, and serves until it is interrupted. It is a writing command, refused under `HERDR_HERMES_NOWRITE=1`.
- `jobs` prints the server's answer as one JSON line. `--server` defaults to `http://127.0.0.1:7429`; plain HTTP is accepted only for a loopback host. A refused check (exit 44) prints the answer only when it is one JSON object whose `status` is `duplicate` or `hold`; any other 409 answer is malformed (exit 4) and its text is never printed. Under `HERDR_HERMES_NOWRITE=1` only the plain inventory runs.

Exit codes: 0 success; 2 bad usage or a request the registry rejected as invalid; 3 unknown claim, policy or folder; 4 the registry is unreachable or answered unexpectedly, or `serve` could not open its database or listen; 44 the pre-dispatch check refused the request (duplicate work or a hold).

## HTTP API

Every answer is one JSON line. A request whose `Host` is not a loopback authority (`localhost` or a loopback IP, with or without a port), or that carries an `Origin` other than an `http` or `https` loopback origin, is refused before routing. Errors are `{"status":"<code>","motivo":"<text>"}` with `forbidden` (403, the authority rule), `invalid` (400), `not_found` (404), `method_not_allowed` (405), `too_large` (413), `unsupported_media_type` (415) and `error` (500).

| method and path | body | answer |
|---|---|---|
| `GET /v1/health` | — | `{"status":"ok","schema":2}`; `schema` is the database schema version |
| `POST /v1/ingest` | one outbox record (at most 1 MiB) | `{"status":"ok","aplicado":true}`; `aplicado` is false for a duplicate or a refused replay |
| `GET /v1/jobs[?maquina=&agente=]` | — | the inventory |
| `POST /v1/check` | `{"agente","card","projeto","branch","pr","workspace","maquina","ttl_s"}` | 200 `{"status":"ok","claim":{…}}`, or 409 `{"status":"duplicate"|"hold","conflitos":[…]}` |
| `DELETE /v1/claims/{id}` | — | `{"status":"ok"}` or 404 |
| `PUT /v1/policies` | `{"escopo","valor","hold","prioridade","motivo"}` | `{"status":"ok"}` |
| `DELETE /v1/policies?escopo=&valor=` | — | `{"status":"ok"}` or 404 |
| `PUT /v1/folders` | `{"maquina","caminho","tamanho_bytes","pedido","job"}` | `{"status":"ok"}` |
| `DELETE /v1/folders?maquina=&caminho=` | — | `{"status":"ok"}` or 404 |
| `PUT /v1/nodes/{maquina}` | `{"manutencao","manutencao_motivo","soho_version","runners"}` | `{"status":"ok"}` |

Request bodies other than ingest are at most 64 KiB, must be `application/json`, and unknown fields are rejected. A `push` `Idempotency-Key` header must match the record's `idempotency_key`.

The inventory:

```json
{"gerado":"2026-01-02T03:04:05-03:00","nodes":[{"maquina":"node-a","online":true,"ultimo_contato":"…","ultimo_poll":"…","poll_status":"ok","herdr_version":"0.9.1","soho_version":null,"runners":null,"manutencao":false,"manutencao_motivo":null}],"jobs":[{"maquina":"node-a","chave":"ws:w3","origem":"workspace","job_id":null,"workspace":"hm-example-agent-impl-card12","workspace_id":"w3","agente":"example-agent","agente_fonte":"hm_name","trabalho":"impl","ref":"card12","card":"CARD-12","projeto":"example-org/example-repo","branch":"feat/x","pr":null,"fase":null,"herdr_status":"working","terminal":false,"saida":null,"esperando_humano":false,"espera_motivo":null,"ultimo_seq":0,"ultima_atividade":"…","ocioso_s":0,"obsoleto":false,"eventos":[]}],"esperas":[],"claims":[],"politicas":[],"pastas":[]}
```

With `--agente` (`?agente=`), the jobs are that agent's and so are the waits: a waiting pane counts for the agent that owns its workspace job, not for the pane's own name, and a waiting pane whose workspace is no job is left out.

## Storage

The database is SQLite in WAL mode (a pure Go driver, no C toolchain), created with mode 0600 in a 0700 directory; a database of an older schema version is upgraded in place, in one transaction, when `serve` opens it (a session stored before the upgrade then takes only records of a later second), and one of a newer version is refused. `serve` refuses to open a database or a `-wal`, `-shm` or `-journal` file that is a symlink or not a regular file and, outside Windows, one with group or other permission bits or owned by another user; there, every directory on the way to the database must also be owned by the current user or root and not writable by group or other, except a sticky shared directory such as `/tmp`, under which the next entry must be a real directory of the current user or root, never a symlink. When the way to the database goes through a symlink, the directory it resolves to and every directory above it must pass the same rules (for the existing part of a path still to be created as well), and the database file is checked again once it is created. Every timestamp it writes or returns carries a numeric offset, never `Z`: the local zone for the times it takes itself, the producer's offset for pushed ones. Admission is bounded: at most 64 nodes, 1000 jobs and 1000 sessions, 1000 claims, 1000 policies and 1000 folder records, 1000 stored pane waits across all nodes, and a folder's optional `job` must be a contract job id (1 to 64 characters of `A-Z`, `a-z`, `0-9`, `.`, `_` and `-`). `--max-jobs` and `--max-sessions` accept only 1 to 1000: 1000 is a hard ceiling because the 256 MiB inventory bound below is computed with 1000 job rows carrying full event rings, 1000 session rows and the 1000 stored pane waits, and session rows never hold events. Each budget counts every retained row of its kind, active and ended: the jobs kind is the pushed jobs, the `job-<id>` rows and the `hm-*` workspace rows (the poll's workspaces are inside it), and the sessions kind is the session rows. A new row of a kind over its budget evicts the oldest ended rows of that kind past the 7-day replay fence, ordered by the ended instant and then the machine and key, their events with them; an ended row within the last 7 days, and one dated in the future, is never deleted, swept or evicted, and an active row is never removed by age or capacity. A request is one push record or one node's poll snapshot, and it is admitted whole or refused whole: a new row the budget cannot admit, a new node past the 64, a stored-waits total past 1000, or any other invalid input refuses the request and rolls back everything it did, the retention sweep included, so a refused record or poll changes nothing and serve writes one alert line to its stderr carrying the kind and the counts and nothing else (never a machine label, key, pane id, name or workspace id). The refusal texts are `jobs limit reached`, `sessions limit reached`, `nodes limit reached` and `waits limit reached` (400 for the push, whose cursor does not advance): a poll whose new workspace cannot be admitted is refused whole and tried again on the next poll, a known node is still updated at the 64-node cap while a 65th node refuses the poll, and an update of an existing row is never refused. The pane waits of a snapshot are admitted by their final total, the waits stored for the other nodes plus the snapshot's blocked panes with the node's own old waits removed first: past 1000 the poll is refused, a database already over the cap may shrink or stay equal but never grow, and a refusal never deletes its stored waits. Ended rows are deleted, with their events, once they are older than the ended retention, default 30 days, `--ended-retention-days` setting it in whole days from 7 to 30. With every table at its bound and every kept value at its length limit, a full inventory stays under 256 MiB, which is what `jobs` reads at most. It holds only current state: a removed job is kept, without its events, until the ended retention, the events are capped per job, and an expired claim is deleted at the next check.

## Limitations

- This release binds loopback only and does not authenticate the push or the API; a central deployment on a private network needs that first and is not part of it.
- The poll sees only what the public Herdr CLI reports: a worker that is stuck without being `blocked` is not detected, and the idle time is bounded by the poll interval.
- The `herdr-soho` version and the runners of a node are known only when reported with `jobs node set`.
- The check prevents overlaps among agents that run it; it cannot stop work started without it.
