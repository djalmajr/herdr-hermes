# Pre-merge validation

This document is the pre-merge validation checklist and evidence contract for Nau. Before a change is merged, someone must run the real `nau` CLI end to end against a real `herdr-soho` backend job on a real development node, for the exact reviewed commit, and paste the actual evidence on the linked tracking card. This checklist holds no results: it only says what to run and what to record.

## 1. Purpose and scope

- This is an actual end-to-end CLI job validation on a real development node, for the exact reviewed commit SHA. It is in addition to code review and CI, never a replacement for either: green unit tests, a green CI run or a clean review do not satisfy it, and it does not satisfy them.
- In scope: the candidate `nau` binary driving a real backend job through `nau job …`, and the durable outbox of that same job through `nau wake`, `nau sync` and `nau outbox`.
- Out of scope: production deployment or install, creating, rotating, reading or logging secrets or API keys, and any production service.
- The public repository holds only this checklist. The real evidence (command outputs, job, workspace and pane ids, screenshots) is posted on the linked tracking card before merge. A path, a link, a count or a report assertion alone is not evidence.
- The validation fails closed: anything that is missing, unreadable or not proved is `FAIL` or `NOT RUN` with its reason, never `PASS`.

## 2. Preconditions

Check every precondition first and record the exact command, version and output of each. If any prerequisite is missing the validation is `FAILED` or `BLOCKED` and the evidence records why. A missing prerequisite is never replaced by a mock, a fake backend, a fixture `go test` run or `--dry-run`. Do not update the backend broadly and do not edit the job contract to make a precondition pass.

- Native build of the candidate at the reviewed SHA, built on the development node itself with `go build -o <dir>/nau ./cmd/nau` into a scratch directory. Record `git rev-parse HEAD` and the tree hash, then run `<dir>/nau version` and `<dir>/nau help`. The version output must carry the reviewed SHA. Do not install the binary globally and do not change `PATH` for the whole machine.
- A compatible `herdr-soho` backend on the same node. Run `herdr-soho capabilities --json` and record its output and the backend version. The public contract says a backend that supports jobs prints a line containing `"ephemeral_job":1` and `"job_events":1`. A backend whose capability line lacks either, or that answers `unknown command 'job'`, does not satisfy the precondition: the validation is `BLOCKED`, and the evidence records the exact command, version and output.
- The bridge view of the same backend: `<dir>/nau doctor` (read-only; it exits 43 when the backend is missing or lacks either capability) and `<dir>/nau capabilities --json` for the bridge capability line.
- An isolated test state. The `nau` state directory lives under the user configuration directory (see the README), so run the validation as a user or in an environment whose configuration directory is a fresh, empty, disposable location, and record how it was isolated. Inside that isolated state only, set the machine label that bookkeeping needs with `<dir>/nau config set machine_label <label>`, and read back `<dir>/nau config list`. Never point the validation at a state directory that holds real records.
- A uniquely named test job id matching `^[A-Za-z0-9._-]{1,64}$`, for example a short prefix plus the reviewed SHA prefix and a date. It is the idempotency key of the job, so never reuse an id.
- A disposable test repository under the owner's control, with its organization in the backend's `job_orgs`, and a team configuration resolvable by the backend for it. The contract opens a draft pull request for every job that has a commit, and pushing is mandatory, so never use a production repository.
- No global installs, no real key operations (`nau auth login`, `nau auth logout` and the credential store are not part of this validation), and no production service. The validation needs no API key: the pull channel `nau outbox` needs none, and a push without a key exits 40, which is an honest outcome to record, not something to fix here.

## 3. Required: real backend job through `nau job`

The job runs through the candidate binary: `nau job <sub>` forwards to `herdr-soho job <sub>` and returns the backend's stdout and exit code unchanged. Run every command below for real, copy its full stdout, stderr and exit status into the evidence, and do not summarize.

1. Write a benign, self-contained brief with the fields the contract requires (`schema`, `id`, `origem`, `repo`, `objetivo`, `aceite`; see the Brief section of the job contract). Ask for one small deterministic artifact with exactly specified content, for example a single new text file whose full expected content is written in the brief, and give an `aceite` item with a command that proves it. Record the expected content before the job starts.
2. Start the job: `nau job start --id <job-id> --repo <org/repo> --brief <brief-file>` (or `--brief -` with the brief on stdin). Optional flags are `--base <branch>`, `--mode worktree|workspace` and `--timeout <minutes>`. Record the output and exit code. Never pass `--dry-run`: it is not an execution.
3. Observe that the job really runs. Record the backend job state, the Herdr workspace created for it (labeled `job-<job-id>`), and the worker panes actually executing, with their real ids. Look at the live runtime itself, not only at the report.
4. Follow it with `nau job status --id <job-id>` (state, event count, last seq, pending decisions) while it runs.
5. Block for the end with `nau job wait --id <job-id> [--timeout <ms>]`. Record the exit code: the contract defines 0 for `done`, 7 for `blocked`, 9 for `timeout`, 19 for `failed`, 21 for `canceled`, 22 for a failed preparation, and 2, 3, 4 and 20 for usage, unknown id, Herdr failure and a reused id. The exit code `nau` returns must equal the exit code `herdr-soho` returns, so run the same read-only command once through `herdr-soho job …` directly and compare both outputs and codes.
6. Read the whole event log with `nau job events --id <job-id> --since 0`, and tail it with `--wait <ms>` while the job runs. Check that `seq` is consecutive from 1 with no gap, that the last line is the `{"eventos":"fim",…}` trailer, and that a terminal event exists with its `refs.exit` and `refs.motivo`. Run it again with `--since <last-seq>` and confirm that only the trailer is printed.
7. Collect the report with `nau job collect --id <job-id>` (JSON), with `--md` for the Markdown report, and with `--verify` when artifact hashes matter. Record `status`, `pr`, `itens`, `artefatos`, `eventos.ultimo_seq` and `limpeza`.
8. Compare the produced artifact with the expected content recorded in step 1, byte for byte, in the job worktree or draft pull request branch, and record both the expected and the observed content. A job that reports `done` with a different artifact is `FAIL`.
9. When the job reaches `blocked` or raises a `question`, answer with `nau job amend --id <job-id> [<file>|-]` (a contract change or an answer). Exercise `nau job send --id <job-id> [<file>|-]` (a non-blocking note) and `nau job ack --id <job-id> --upto <seq>` (the dispatcher routed every decision up to `<seq>`) when the backend supports them and the job produces a case for them (an `amend_received` event, a `decision` event). Items that the job did not make relevant are `NOT RUN` with the reason, not `PASS`.
10. Check the correlation. The job id, the workspace label, the pane ids, the event `refs`, the `report.json` fields (`id`, `branch`, `pr`, `eventos`) and the pull request must all point at one and the same job. A mismatch is `FAIL`.
11. Check that errors and terminal exit codes are preserved. At least run `nau job status --id <unknown-id>` (the contract says exit 3 with `{"status":"not_found"}`) and compare with the direct `herdr-soho job status --id <unknown-id>`. Run an input `nau` refuses before forwarding, such as an `--id` that does not match the pattern, and record that it exits 2 without a backend call.
12. Finish with `nau job list` for the counts by state.

A `--job` label on a queued request, a bookkeeping record, a task from another protocol or an outbox record is not proof that a backend job ran. The proof is the backend job itself: its state, its live Herdr workspace and workers, its event log, its report and its artifact, all correlated by the same id.

## 4. Required: durable outbox on the same job

Use the same job as in section 3, through the real commands. Read-only inspection never changes state; `wake`, `sync` and `push` do.

1. Reconcile: `nau sync --job <job-id>`. It checks the backend capabilities (exit 43 when missing), fetches the job events without waiting, appends the new ones and prints one counts line `{"jobs":…,"novos":…,"enviados":…,"pendentes":…}`. With no `dispatcher_url` configured the records stay in the outbox for a pull; with one configured but no key the exit is 40. Record the line and the exit code as they are; do not configure a key to change them. `nau sync --push-only` and `nau push` only push pending records.
2. Pull: `nau outbox --since 0` prints the records as JSON lines, then the trailer `{"outbox":"fim","ultimo_seq":…,"entregue_seq":…}`. Check that the outbox `seq` values are consecutive from 1 with no gap and no duplicate, that each `job_event` record of the job carries the event `seq` and the `idempotency_key` `<job-id>:<event seq>` of the backend event log from section 3, and that the set of events matches `nau job events` one for one. Use `nau outbox --since <seq> --wait <ms>` to check the long poll returns the new record or the trailer.
3. Restart and offline recovery: stop and restart only the validation's own processes, re-run `nau sync --job <job-id>` and `nau outbox --since 0`, and check that no record is lost or duplicated, that `seq` stays monotonic and that the counts equal the ones before. Where the node can be taken offline (the backend or the dispatcher unreachable), do it only for the validation's own processes, run `nau sync`, and record that the pending records stay pending and are delivered later without duplicates. Whatever cannot be exercised on this node is `NOT RUN` with the reason.
4. Live wake: the backend runs the machine's `job_wake_cmd` for the events that wake the dispatcher, and `nau wake` is the intended command. Check with `nau doctor` whether the `wake_hook` field is `true` on this node, and say for every outbox record of the job whether it arrived through the live wake hook (read `nau outbox --since 0` before the first `nau sync`) or through the `sync` reconciliation. Do not edit the backend's global configuration to enable the hook; if it is not configured, record the live wake as `NOT RUN` with the `nau doctor` output. Piping a hand-written event into `nau wake` proves only that `wake` accepts an event and records it once (`duplicado` on a repeat); it is not proof of a live upstream event source and must be labeled as such.
5. Accepted versus processed: the evidence states plainly what each output proves. A recorded outbox record means the event is durable on the node. A push with a 2xx answer means the receiver accepted the record. Neither says the receiver processed or acted on it. A delivery that is `uncertain` or `exhausted` is reported as such, never as delivered.

## 5. Conditional: A2A (only when the candidate includes it)

This section applies only when the candidate includes the A2A feature (`nau a2a …`). First run the candidate's own `nau help` and record whether `a2a` is listed. If it is not listed, record `NOT RUN: the candidate has no A2A feature` and skip the rest of this section; never assume that an older binary has the commands, and never run another binary in its place. Read the candidate's `docs/a2a.md`, in particular its Authentication and Limitations sections, and check the behavior against that document, not against this one.

- `nau a2a serve` on its default loopback address (a non-loopback `--listen` is refused), with `--db` pointing at the isolated state. Record the first output line.
- `nau a2a card --server <url>`: the public agent card is returned.
- `nau a2a send --server <url> --session <id> --epoch <n> --seq <n> --project <org/repo> [--job <job-id>] (<text> | -)`: creates a task. Record the task id and the context id. Use distinct session ids for independent callers, and the next `--seq` each time.
- `nau a2a inbox [--project <org/repo>] [--state <state>]` and `nau a2a show --task <task-id>`: the task, its delivery states and its pending decisions are visible.
- `nau a2a reply --task <task-id> (--answer | --ask | --decline) [--decision-id <id>] (<text> | -)`: exercise a question back with `--ask`, a continuation with `nau a2a send … --task <task-id> --context <context-id>` and a terminal decision with `--answer` or `--decline`. Check that the same task id and context id hold across the input-required, continuation and terminal steps. `reply` only records the decision; a running `serve` applies it.
- `nau a2a get --server <url> --task <task-id>` and `nau a2a cancel --server <url> --task <task-id>` (cancel only a non-terminal task; a terminal task refuses).
- Real owner and notice handoff: the owner registration comes from the `nau notify register …` commands in the isolated state, and the notice goes through the real `herdr-soho send` transport. Record the delivery states exactly (`accepted` means the endpoint accepted the notice, never that the recipient processed it). With no registration the honest state is `no_route`.
- Persistence: stop `serve`, restart it on the same `--db`, and check that the tasks, their ids and their decisions survive and that `get` returns the same task.
- Fail closed: the `nau a2a` client subcommands send no credential, so this check uses a plain HTTP client against the loopback agent with an `Authorization: Bearer` header whose value is a throwaway, non-secret placeholder (never a real, reused or generated credential). Check the behavior the candidate's `docs/a2a.md` describes for a runtime with no identity provider configured, and confirm with `nau a2a inbox` that nothing was created. Do not claim an identity provider is configured, and do not create, request or paste any real key or token: record only the observed status and the unchanged state.

The A2A lifecycle proves transport and decisions only. It does not replace the required backend job of section 3: a task that carries a `--job` label does not show that a backend job ran.

## 6. Evidence template

Post the filled-in template, with the real pasted outputs, on the linked tracking card before merge. Leave a slot empty or write `NOT RUN` with the reason; never fill a slot with an expectation. Redact secrets without hiding failures: a redaction names what was removed and keeps the rest of the output intact.

```text
Reviewed commit SHA:
Tree hash:
Native OS and architecture:
How the state was isolated:
nau version (command, stdout, exit):
nau help (first lines and the full output attached):
Backend version:
Backend capability line (herdr-soho capabilities --json, stdout, exit):
nau doctor (stdout, exit):
Test repository (org/repo) and test job id:

Command log (one entry per command, in order):
  command:
  stdout:
  stderr:
  exit code:

Backend job:
  job id:
  Herdr workspace id and label:
  worker pane ids:
  state sequence observed:
  terminal exit code (nau) and (herdr-soho direct):
  last event seq and terminal event:
  report fields (status, pr, itens, eventos):

Artifact:
  expected content:
  observed content:
  comparison:

Correlation (job id, workspace, events, report, pull request):

Outbox:
  outbox records seq range and trailer:
  event-to-record comparison:
  restart / offline recovery:
  live wake (wake_hook value, source of records):
  push outcome and exit code:

A2A (only when the candidate includes it):
  task id and context id across each step:
  delivery states:
  persistence across restart:
  fail-closed behavior observed:

Result per item (PASS | FAIL | NOT RUN, with the reason):
  1 build and version:
  2 backend capability precondition:
  3 isolated state and disposable repository:
  4 job start and live execution:
  5 status, wait, events, collect:
  6 artifact comparison:
  7 amend / send / ack:
  8 correlation:
  9 exit codes and error forwarding:
  10 outbox monotonic and complete:
  11 restart / offline recovery:
  12 live wake:
  13 A2A:
Overall result:
Screenshots (optional):
```

- CLI output pasted as text is enough. When screenshots are used, they must show the real runtime of the job (its live workspace and worker panes) and identify it by the job id, and they never replace the pasted outputs.
- The overall result is `PASS` only when every required item is `PASS`. Any `FAIL`, or any required item `NOT RUN` without a recorded blocker, means the change is not validated.

## 7. Cleanup

- Stop only the processes the validation started itself (for example its own `nau a2a serve`), identified by the process ids it recorded when it started them. Never stop, signal or kill anything else.
- Release the validation's own finished worker panes with the backend's `--close` release option, and close the validation's own job workspace after the evidence is saved. A job that reached a terminal state already releases its processes and closes its workspace; check that this happened and record it. `nau job close --id <job-id>` closes a collected job and exits 24 while a `decision` event is not acknowledged; use `nau job ack` first, and use `--force` only when the owner decides so and records it.
- Keep every unmerged job folder, worktree, copy and report. Do not delete anything, do not remove worktrees, do not clean the test repository's branch or pull request, and make no production change. Removing a worktree or a job folder is a decision of the owner, taken later by hand.
- An infrastructure finding (a backend that lacks the capability, a Herdr failure, a node that cannot be isolated) goes on the linked issue through its owner, with the exact command, version and output. It is not fixed as part of this validation.
