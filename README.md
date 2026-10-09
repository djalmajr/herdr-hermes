# herdr-hermes

A two-way bridge between a remote job dispatcher and a Herdr node, built on the ephemeral job contract of [herdr-soho](https://github.com/djalmajr/herdr-soho).

- **Dispatcher to node:** receives job dispatches and amendments and hands them to the local `herdr-soho job` commands unchanged.
- **Node to dispatcher:** records job events, session status and decisions in a durable local outbox that the dispatcher can pull, and can optionally push them to a configured dispatcher URL with a per-user API key stored on the machine.

It ships as one native Go executable per machine, an optional Herdr plugin and an agent skill. `herdr-soho` stays standalone; this project only uses its public CLI contract.

Status: under development. Nothing here is stable yet.
