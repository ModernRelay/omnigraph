# Control-plane seams: observation, readiness and bounded shutdown

**Decides:** planning and observation read the ledger without writer admission and label their output as observed; `/readyz` reports which applied revision a replica booted and how many graphs it serves, without naming them; graceful shutdown has one absolute deadline that a thread enforces.
**Contract:** keeps E5, E6
**Code:** `crates/omnigraph-cluster/src/lib.rs`, `crates/omnigraph-cluster/src/types.rs`, `crates/omnigraph-server/src/handlers.rs`, `crates/omnigraph-server/src/lib.rs`, `crates/omnigraph-server/src/main.rs`
**Cited as:** RFC 0049

## Context

A control plane that manages many clusters needs a drift signal while a server owns the cluster; taking writer admission just to look would block deployments. An orchestrator replacing replicas cannot tell, from liveness alone, which revision a replica serves, which graphs it could not open, or that it is draining. Without a bound, one stalled connection keeps a replica alive past any orchestration grace, and the orchestrator's kill looks like a crash.

## Decision

- `cluster plan` and `cluster observe` take no writer admission and write nothing. Their output carries `authority: observed` with the `state_cas` and `state_revision` they read. An existing lock is reported, not refused. An observation is never authority for an effect: apply prepares again under admission.
- `GET /readyz` needs no authentication and reports status (`loading`, `serving`, `degraded`, `blocked`, `draining`), the applied `config_digest` the process booted as `booted_serving_digest`, the ledger revision and CAS, and the registry, ready, loading and blocked graph counts.
- Graph ids and per-graph availability stay behind authenticated `GET /graphs` under `graph_list`, as one registry-derived list with `state`, `read_available`, `write_available`, a sanitized `failure` and an `action`.
- Readiness and inventory derive from the serving registry, never from the boot witness, so no phantom entry appears. Status reads registry snapshots with no graph or storage I/O.
- Readiness is 503 while any startup open is loading, even if a sibling serves. After startup, a nonempty inventory is ready while any graph is ready (`degraded` if some are blocked). A valid empty inventory is ready.
- A known unavailable graph answers 503 only to a caller allowed `read` on `main` or `graph_list`; to anyone else it is 404. MCP uses the same resolution.
- `/healthz` stays liveness only and answers 200 while draining.
- `--shutdown-grace-seconds` (environment `OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`, default 25, zero cuts off at once) bounds shutdown from the signal. The signal listener is installed before any graph opens. At the deadline an operating-system thread exits the process with status 2, whatever the async runtime is doing. A cutoff never claims success and repairs nothing.

## Rejected

- **Observe through `state.lock: false`:** a bundle setting changes every command and cannot label a result as an observation.
- **List graph ids on `/readyz`:** it would bypass the authentication and `graph_list` gate on `GET /graphs`.
- **Report the boot digest on `/healthz`:** liveness and readiness have different consumers; a draining replica is alive and not ready.
- **A Tokio task as the watchdog:** it cannot fire while the executor is blocked or tearing down, which is when a deadline is needed.
- **Restore a ledger from a file:** its real use is a restore point where ledger and graphs return together, so it waits to be designed once against that.
