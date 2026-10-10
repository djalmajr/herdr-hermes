# Routing

`herdr-hermes route` is the dispatcher-side machine choice. The job contract ([`docs/upstream/job-contract.md`](upstream/job-contract.md)) says that the dispatcher chooses the machine and runs the job subcommands on it through its own remote execution channel, and that `herdr-soho` never routes a job to another machine; `route` is the helper for that choice. It runs on the host the dispatcher uses to reach the fleet, probes the configured machines read-only, picks one and prints it. The choice is advisory: `route` never starts, moves or replays a job, and the dispatcher then runs `herdr-hermes job start …` on the chosen machine through its own remote execution channel (the job forwarding is unchanged). A dispatch-only host that must never run jobs is simply not listed in the fleet.

## Command

`herdr-hermes route [--machine <label>]` is read-only: it works under `HERDR_HERMES_NOWRITE=1`, writes nothing, and prints exactly one JSON line on stdout.

## Configuration

The keys are set with `herdr-hermes config set <key> <value>`:

- `route_machines` — the fleet: comma-separated machine labels in priority order (empty by default; `route` exits 2 until it is set). A label is a saved Herdr machine label (as listed by `herdr machine list`) or the reserved label `local` for the Herdr server of the host that runs `route`; the label form is `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`, with no duplicates and at most 32 labels.
- `route_disabled` — labels to exclude without editing the Herdr machine profiles (same form, empty by default).
- `route_orchestrator_name` — the orchestrator agent base name to count (default `orchestrator`, the `herdr-soho` default `orchestrator_name`; form `^[a-z][a-z0-9_-]{0,31}$`).
- `route_probe_timeout_s` — the bound of each probe, 1..120 seconds (default 20).
- `herdr_bin` — the `herdr` executable (default `herdr`).

## Probe

The probe is read-only and uses only the public Herdr CLI, as argv subprocesses with a context deadline — never a shell, and never the Herdr or `herdr-soho` state files.

1. A label in `route_disabled` is `disabled` (`disabled_by_config`) and is not probed. A label that does not match the label form is `unsupported` (`unknown_machine`).
2. When at least one remaining label is not `local`, `herdr machine list --json` runs once. If it fails, times out, or its output is not the expected array of `{id,label,enabled}` objects, every non-local label is `unavailable` (`catalog_unavailable`, `catalog_malformed`, or `herdr_unavailable` when the executable is missing). A label absent from the list is `unsupported` (`unknown_machine`), a label listed more than once is `unsupported` (`ambiguous_machine`), and a saved machine with `enabled: false` is `disabled` (`machine_disabled`).
3. Each remaining machine is probed concurrently with `herdr agent list` (`local`) or `herdr --machine <label> agent list`, each under `route_probe_timeout_s`. A timeout is `probe_timeout`, a non-zero exit is `probe_failed` (even when it printed JSON), a missing executable is `herdr_unavailable`, and output that is not a valid agent list (a Herdr `error` response, a missing `result.agents` array, an agent without a `pane_id`, an agent with an unknown `agent_status`, or oversize output over 8 MiB) is `probe_malformed`; all of them make the machine `unavailable`. Load is never inferred from a failed or malformed probe.
4. A machine that answered a valid agent list is `available`; its load is the number of active orchestrators: the agents whose `name` is the base name or the base name followed by `-<n>` (for example `orchestrator`, `orchestrator-2`), whatever their status. Workers (for example `build`, `review-2`), unnamed panes and other agents are not counted. There is no resource cap.

## Selection

- `--machine <label>` names a requested machine. A label not in `route_machines` exits 2 without probing. A requested machine that is `available` wins even when another machine has fewer orchestrators (`motivo: requested`).
- Otherwise the available machine with the fewest active orchestrators wins; a tie goes to the first in `route_machines` order (`motivo: least_load`). When a requested machine is not available, the same rule picks another one and `motivo` is `fallback`, with `solicitada` set.
- When no machine is available, `route` exits 4.

## Output

On success (exit 0), `route` prints exactly one JSON line:

```json
{"maquina":"mac-a","motivo":"least_load","orquestradores":1,"candidatos":[{"maquina":"mac-a","estado":"available","orquestradores":1},{"maquina":"win-a","estado":"unavailable","motivo":"probe_timeout"}]}
```

`solicitada` appears only when `--machine` was given. `candidatos` lists every configured machine in configured order with `estado` (`available`, `unavailable`, `disabled`, `unsupported`), `orquestradores` only when available, and the reason code in `motivo` otherwise.

## Exit codes

- Exit 2 with `{"status":"2","motivo":"…"}` for bad usage (unknown flag, missing or repeated `--machine`, invalid label), an invalid configuration, `route_machines` not configured, or a requested machine that is not configured.
- Exit 4 when no machine is available:

```json
{"status":"unavailable","motivo":"no eligible machine is available","candidatos":[…]}
```

## Safety rules

- Fallback is a read-only preflight decision only. Once the dispatcher sent `job start` (or any other mutating job command) to a machine, it never re-sends it to another machine after a transport loss or timeout: a connection failure does not prove the mutation was not applied; it inspects that machine (`job status --id <id>`) when it is reachable again. The same `--id` is the job's idempotency key.
- The route is advisory and point-in-time; two dispatches at the same moment may see the same load.
- `route` never reads the API key and passes the process environment unchanged to `herdr`; it adds nothing to it.
- A machine reported `unavailable` is one whose probe failed or timed out; `route` cannot tell a powered-off machine from an unreachable or unresponsive one.

## Dispatcher flow

1. `herdr-hermes route [--machine <label>]` on the dispatcher host.
2. `herdr-hermes job start …` on the chosen machine, through the dispatcher's own remote execution channel.
3. Never re-send the dispatch to another machine after a transport loss or timeout; inspect the chosen machine with `job status --id <id>` instead.

## Limitations

- The route is advisory and point-in-time; dispatches at the same moment may see the same load.
- Herdr remote forwarding needs saved SSH machines with a running compatible remote Herdr server.
- A machine that is offline cannot be told apart from an unreachable or unresponsive one.
