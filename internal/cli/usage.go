package cli

// Usage text printed by `help` and on bad usage (to stderr). It lists every
// command and every exit code of the bridge, including the commands the
// later slices register.
const usage = `herdr-hermes - two-way bridge between a remote dispatcher and a Herdr node

usage: herdr-hermes <command> [args]

commands:
  job <sub> [args]                  forward a dispatcher-facing job subcommand to herdr-soho
                                    (start, status, wait, events, collect, amend, send, ack,
                                    cancel, close, list)
  route [--machine <label>]         choose the machine for a new job (dispatcher side, read-only)
  serve [--listen <host:port>] [--db <path>] [--machines <label,...>]
                        [--poll-interval <seconds>]
                                    run the registry service on a loopback address:
                                    inventory push, poll and read/write HTTP API
  jobs [--maquina <label>] [--agente <a>] [--server <url>]
                                    print the registry inventory of every node (read-only)
  jobs check --agente <a> [--card <c>] [--projeto <org/repo>] [--branch <b>] [--pr <n>]
                        [--workspace <name>] [--maquina <label>] [--ttl <s>]
                                    mandatory pre-dispatch check: refuse duplicate work or
                                    a hold, else record a claim
  jobs release --claim <id>         release a claim
  jobs policy set|clear, jobs folder add|clear, jobs node set
                                    holds and priorities, retained folders (a record, never
                                    a permission to delete) and node facts
                                    (every jobs form takes [--server <url>])
  wake                              read one job event on stdin, record it in the outbox and
                                    attempt one push
  sync [--job <id>] [--push-only]   sync tracked job events and push pending records
  push                              push pending outbox records to the dispatcher
  outbox [--since <seq>] [--wait <ms>]
                                    print outbox records with seq > since as JSON lines, then
                                    the trailer (the dispatcher pull channel; read-only)
  session start|update|end          manage open session records for a project
  decision --projeto <org/repo> --escopo global|projeto --motivo <text> [--job <id>]
                                    [<resumo>|-]
  auth login|status|logout          manage the per-user dispatcher API key
  config get|set|list               manage the machine configuration
  doctor                            check the bridge prerequisites
  capabilities --json               print the bridge capability line
  version                           print the version
  help                              print this usage
  plugin startup|event|bridge       Herdr plugin entry points
  notify register owner --projeto <org/repo> --to <ref> | orchestrator --job <id>
                        --to <ref> | coordinator --to <ref> | watch --pane <ws:pane>
                        [--job <id>] [--projeto <org/repo>] | workspace
                        --workspace <ws> [--job <id>] [--projeto <org/repo>]
                                    register notification recipients and sources
  notify unregister owner --projeto <org/repo> | orchestrator --job <id> |
                        coordinator | watch --pane <ws:pane> | workspace --workspace <ws>
                                    unregister notification recipients and sources
  notify list                   show the registrations and the notification,
                                    delivery and pending counts (read-only)
  notify status [--id <nid>]    show the last 50 notifications, or exactly one by
                                    id (read-only)
  notify deliver [--timeout <ms>]
                                project new job events and deliver the due
                                    notifications
  notify ingest agent-status|workspace
                                ingest a Herdr agent status or workspace event from stdin
                                    (64 KiB cap)
  notify raise --class cross_project|stuck [--projeto <org/repo>] [--job <id>]
                                [--id <raise-id>]
                                    raise an explicit cross-project or stuck
                                    notification
  notify ack <nid> --role owner|orchestrator|coordinator
                                    acknowledge a delivered notification
  notify retry <nid> --role owner|orchestrator|coordinator
                                    re-queue once an uncertain or exhausted delivery

exit codes:
  0   success (forwarded commands: whatever herdr-soho returned)
  2   bad usage, input limit exceeded, unknown config key, internal job
      subcommand refused, registry usage or field rejected (serve, jobs)
  3   unknown job id in herdr-hermes bookkeeping (sync --job) and unknown
      notification id (notify status --id, notify ack, notify retry), unknown
      claim, policy or folder (jobs)
  4   herdr-soho not found or not runnable; no eligible machine available
      (route); registry unreachable or unavailable (serve, jobs)
  40  no API key configured (push required)
  41  API key rejected by the dispatcher (401/403)
  42  dispatcher unreachable or retries exhausted; records stay pending
  43  herdr-soho lacks the ephemeral_job or job_events capability
      (doctor, sync)
  44  the pre-dispatch check refused: duplicate work or a hold (jobs check)
`
