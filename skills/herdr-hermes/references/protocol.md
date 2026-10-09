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

## Pull channel

`herdr-hermes outbox [--since <seq>] [--wait <ms>]` prints the records with `seq > since` as JSON lines and always ends with the trailer `{"outbox":"fim","ultimo_seq":N,"entregue_seq":M}`. It is read-only, needs no key and no network, and works under `HERDR_HERMES_NOWRITE=1`.

## Push

`POST <dispatcher_url>` with one record as the JSON body. Headers, exactly:

- `Authorization: Bearer <key>`
- `Idempotency-Key: <idempotency_key>`
- `Content-Type: application/json`
- `User-Agent: herdr-hermes/<version>`

Records are pushed in `seq` order, one at a time. A 2xx advances the delivery cursor. 401/403 stops the batch (exit 41 for `sync`/`push`). 408/429/5xx and network errors are retried at most three times after 10 s, 30 s and 90 s, then the records stay pending (exit 42 for `sync`/`push`; `wake` makes a single attempt and exits 0 either way). 3xx and other 4xx stop without retry (exit 42). HTTPS is required unless the host is a loopback address; redirects are not followed. When `dispatcher_url` is empty, push is disabled and the records stay in the outbox for a pull.

## Exit codes

| code | meaning |
|------|---------|
| 0 | success (forwarded commands: whatever `herdr-soho` returned) |
| 2 | bad usage, input limit exceeded, unknown config key, internal job subcommand refused |
| 3 | unknown job id in `herdr-hermes` bookkeeping (`sync --job`) |
| 4 | `herdr-soho` not found, not runnable, or killed by the forwarding deadline |
| 40 | no API key configured (push required) |
| 41 | API key rejected by the dispatcher (401/403) |
| 42 | dispatcher unreachable or retries exhausted; records stay pending |
| 43 | `herdr-soho` lacks the `ephemeral_job` or `job_events` capability (`doctor`, `sync`) |

Forwarded `job` subcommands return the `herdr-soho` exit code unchanged; codes 40–43 are never produced by a forwarded command.

## Key

The per-user API key is stored on the machine (`auth login --key -`), is sent only in the `Authorization` header, and never appears in any output, file, log or subprocess environment. `auth status` prints only `{"configured":true|false,"store":"file"}`.
