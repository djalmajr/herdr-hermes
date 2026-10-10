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
4. A machine that answered a valid agent list is `available`; its load is its number of recognized orchestrators, whatever their status (see [Orchestrator load](#orchestrator-load)). There is no resource cap.

## Orchestrator load

- A recognized orchestrator is a Herdr agent whose `name` is exactly the base name (`route_orchestrator_name`, default `orchestrator`) or the base name followed by `-<n>`, where `<n>` is a positive integer without a leading zero (for example `orchestrator`, `orchestrator-2`, `orchestrator-12`). The match is exact and case-sensitive: `orchestrator-0`, `orchestrator-02`, `orchestrator-x`, `Orchestrator` and `my-orchestrator` are not recognized.
- A machine's load is the number of recognized orchestrators in its agent list, whatever their `agent_status`: `idle`, `working`, `blocked`, `done` and `unknown` all count the same. Each recognized agent counts exactly once.
- Load is therefore the number of orchestrator agents open on the machine, not the number working right now: a finished orchestrator whose pane is still open (usually `done` or `idle`) keeps counting until its agent leaves the Herdr agent list, that is until its pane or its workspace is closed. Close the workspaces of finished jobs to release their load. The set of counted statuses is not configurable.
- A machine with four `done`, one `idle` and two `working` orchestrators has load 7; a machine with two `done` and two `working` orchestrators has load 4; `route` picks the second machine (`least_load`) although both have two working orchestrators.
- An agent whose name does not match that rule is not counted, whatever it is doing: with the default base this covers the workers that `herdr-soho` spawns (for example `build`, `build-2`, `review-2`), unnamed agents (no `name`, or `name: null`) and every other agent. `route` recognizes orchestrators by name only; it never infers a role from a pane, its title, its command or its working directory.

## Orchestrator naming

- An orchestrator is visible to `route` only through its Herdr agent name. Name it with the existing public interfaces: `herdr-soho init`, run from the orchestrator's own pane, names the calling agent after the `herdr-soho` `orchestrator_name` (`orchestrator` by default), or `<base>-<n>` such as `orchestrator-2` when that name is already taken on the same Herdr server; `herdr agent rename <target> <name>` names an agent by hand. An orchestrator started without either stays unnamed and does not count, even while it is working.
- Workers that `herdr-soho` spawns are named after their lane or role (`build`, `review-2`, `implementer`), so they do not count as long as the base name is not one of those names; do not choose a lane or role name as the base.
- Custom base name: when the fleet's orchestrators use another `herdr-soho` `orchestrator_name`, set `route_orchestrator_name` on the dispatcher host to the same value (`herdr-hermes config set route_orchestrator_name <name>`). One base name applies to every machine in the fleet, so keep `orchestrator_name` the same on every node. With `route_orchestrator_name=<base>`, the recognized names are `<base>` and `<base>-<n>`: with `route_orchestrator_name=planner`, `planner`, `planner-1` and `planner-3` count and `orchestrator` does not.
- Read-only verification:

  ```text
  herdr agent list
  herdr --machine <label> agent list
  herdr-hermes route
  ```

  In each agent list, count the agents whose `name` matches the rule in [Orchestrator load](#orchestrator-load), whatever their `agent_status`; that number is the `orquestradores` value `route` reports for that machine in `candidatos`. An orchestrator you expect to count but that is missing from the number is unnamed or misnamed: name it as above.

## Dispatcher environment

- `route` sees the fleet only through the `herdr` it runs (`herdr_bin`), in the dispatcher's own environment: it passes its environment to `herdr` unchanged and adds nothing. Two pieces of per-user state decide the result:
- The saved-machine catalog belongs to Herdr and to the operating-system user that saved the machines (`herdr machine add`). `route` reads it only through `herdr machine list --json`, never from a file.
- The `herdr-hermes` configuration (`route_machines` and the other keys) lives in the configuration directory of the user that runs `herdr-hermes`: `~/Library/Application Support/herdr-hermes/config` on macOS, `%AppData%\herdr-hermes\config` on Windows, and `$XDG_CONFIG_HOME/herdr-hermes/config` (by default `~/.config/herdr-hermes/config`) on Linux.
- Run the dispatcher as the user that saved the machines, with that user's normal environment: on macOS and Linux the same `HOME`; on Windows the same user profile (`USERPROFILE`, `APPDATA` and `LOCALAPPDATA`). A service account, a scheduler, a sandbox or a wrapper that changes these variables sees another user's catalog and configuration, usually empty ones: an empty configuration makes `route` exit 2 (`route_machines is not configured`) before any probe, and an empty catalog makes every saved label in a configured fleet `unsupported` (`unknown_machine`).
- The `local` label is probed through the host's own Herdr server, which `herdr` reaches on its own or through `HERDR_SOCKET_PATH`; a dispatcher in the wrong environment can therefore still see `local` as `available` while every saved label is `unsupported` (`unknown_machine`).
- Do not copy or edit the Herdr catalog or the machine profiles to work around this: save the machines as the dispatcher user with `herdr machine add`, or run the dispatcher as the user that already has them.

### Preflight (read-only)

```text
herdr machine list --json
herdr-hermes config get route_machines
herdr-hermes route
```

Run it as the dispatcher user, in the dispatcher's environment (the same service, scheduler or shell). `herdr-hermes config get route_machines` must print the intended fleet; an empty value means this environment reads another configuration, and `route` then exits 2 before any probe. Every saved label in `route_machines` that should be eligible (not listed in `route_disabled`) must appear exactly once in `herdr machine list --json` with `enabled: true`: a label that is missing is `unknown_machine`, a label listed more than once is `ambiguous_machine` and an entry with `enabled: false` is `machine_disabled`. `local` needs no catalog entry and is never resolved through the catalog. An empty list `[]` where you expect saved machines is the sign to check which user and environment the dispatcher runs with. All three commands are read-only and work under `HERDR_HERMES_NOWRITE=1`.

### Labels and reason codes

- `local` is reserved: it always means the Herdr server of the host that runs `route`, it is probed with `herdr agent list` and it is never looked up in the catalog. Every other label is a saved Herdr machine label resolved through the catalog. A catalog entry labelled `local` is never used by `route`: that label always means the host's own server.
- `unsupported` with `unknown_machine`: the catalog that `herdr machine list --json` returned in this environment has no machine with that label (a typo, a machine this user never saved, or a dispatcher running in another user's environment), or the label does not match the label form. It is a configuration problem; retrying does not help.
- `unavailable`: `route` could not establish the machine's availability or load in this run; a later `route` may see it again. The reason code says where to look: `probe_failed` (`herdr` exited non-zero, for example because the Herdr server of that machine is not running or the machine cannot be reached) and `probe_timeout` (no answer within `route_probe_timeout_s`) point at the machine or the path to it, which may be powered off, unreachable or busy; `herdr_unavailable` means the `herdr` executable (`herdr_bin`) is missing or cannot start on the dispatcher host, so fix the installation or the configuration; `probe_malformed` and `catalog_malformed` mean `herdr` answered with output `route` does not accept (an `error` response, an unexpected shape or oversize output), so check that the Herdr versions are compatible; `catalog_unavailable` means `herdr machine list --json` itself failed or timed out, which makes every saved label unavailable while `local` keeps its own probe.

## Selection

- `--machine <label>` names a requested machine. A label not in `route_machines` exits 2 without probing. A requested machine that is `available` wins even when another machine has fewer orchestrators (`motivo: requested`).
- Otherwise the available machine with the lowest orchestrator load wins; a tie goes to the first in `route_machines` order (`motivo: least_load`). When a requested machine is not available, the same rule picks another one and `motivo` is `fallback`, with `solicitada` set.
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
