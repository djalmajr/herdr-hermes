# herdr-hermes

Two-way bridge between a remote job dispatcher and one machine: the Go CLI in `cmd/herdr-hermes`, the vendored job contract in `docs/upstream/`, the protocol in `docs/protocol.md`, the optional Herdr plugin in `plugin/` and the embedded agent skill in `skills/herdr-hermes/`.

- Go is the sole project runtime and test implementation. Standard library only, no third-party modules, no Node.js, Bun, Bash or interpreter fallback.
- `herdr-soho` is used only through its public CLI (`herdr-soho job …`, `herdr-soho capabilities --json`, `herdr-soho config`), executed as an argv subprocess with a context deadline and `WaitDelay` — never a shell, and never by reading or writing `herdr-soho` state files.
- The plugin entrypoints (`plugin startup|event|bridge`) call the same CLI code as the commands; they keep no second copy of the outbox or push logic, and the CLI works fully without the plugin.
- The per-user API key never leaves the credential store and the `Authorization` header: it is never printed, logged, written to the outbox, passed to a subprocess environment, included in an error message, or accepted as a flag value or environment variable. `auth login --key -` is the only way to set it.
- Every command prints exactly one JSON line on stdout (`outbox` prints JSON lines plus a trailer); diagnostics go to stderr; errors are `{"status":"<code>","motivo":"<text>"}` on stdout with the exit codes `0`, `2`, `3`, `4`, `40`, `41`, `42`, `43`.
- Timestamps are local time with an explicit offset, never `Z`; state files are written atomically (temp file + rename, mode 0600) and the state directory is mode 0700.
- `docs/upstream/job-contract.md` is vendored verbatim and never edited; `internal/jobapi/contract_test.go` fails when the vendored constants drift from the document, and the vendored types are the only dependency on the contract.
- Keep the code, the skill, the plugin and the documentation portable: no machine paths, hostnames, IPs, private service names, provider, model or tool names, tracker URLs, credentials or person names in code, tests, docs or commit messages; no AI attribution.
- Write all documentation in English. Keep each prose paragraph and list item on one physical line.
- Failing test before implementation; tests are hermetic (no network beyond loopback, injected clocks, no real `herdr-soho` or dispatcher), finish in seconds and never hang.
- Run the checks before shipping: `gofmt -l .`, `go vet ./...`, `go test ./...` and `go test -race -timeout=30m ./...`; cross-compile for linux, macos and windows — cross-compilation is not execution proof.
