# Engine settlement and resource bounds

**Decides:** a query returns only after the graph workers it registered have finished, a multi-table write refuses before any table effect when it passes an operation-wide byte allowance, and the engine is never disposed or reused while native work it cannot prove settled may remain.
**Contract:** keeps E5
**Code:** `crates/omnigraph/src/engine/context.rs`, `crates/omnigraph/src/engine/operators/memory.rs`, `crates/omnigraph/src/storage_layer.rs`, `crates/omnigraph-server/src/operations.rs`, `crates/omnigraph/tests/lance_surface_guards.rs`

## Context

A server that owns admitted work after its caller disconnects needs to know when that work is over. An awaited task, a zero request count or an exclusive gate does not show it: a graph producer, a blocking filesystem job or a multipart cleanup can still run below them and hold memory. Per-table write limits also let a write touching many tables retain far more than any one limit. The pinned Lance gives no public hook that carries an operation's ownership through its native I/O and CPU work, so full settlement cannot be proven yet.

## Decision

- `QueryContext::run_owned` runs the whole query, drops its execution future, closes registration and waits for every registered producer and blocking worker before it returns a result, an error or a panic. A panic resumes only after the workers settle.
- A child registers before dispatch, and its future and captured resources drop before its registration does. A descendant registers only through its parent's lease (`QueryWorkLease::child`); a new root registration after closure is refused.
- The server keeps read execution alive after the caller goes, so these joins finish. An embedded caller that drops a query only closes registration.
- Named mutations and keyed loads share a 32 MiB retained-batch allowance across their tables, beside the per-table limits. Keyed parsing and removed-id collection each have their own 32 MiB operation-wide allowance. A removed id is charged its UTF-8 length plus one `String` slot.
- Passing an allowance is `ResourceLimitExceeded`, before the operation's table effects and publication. A branch a load created implicitly may already exist.
- No reuse capability exists. Draining a serving epoch proves only that registered server work finished, never native settlement, and grants no engine disposal or reuse.
- Effect outcome and settlement are separate facts: a known commit can have unfinished cleanup, and settled work can have an unknown outcome.

## Rejected

- **A task counter or a quiet interval as proof of settlement:** neither proves that native work has finished.
- **Per-graph engine subprocesses:** they change the in-process server architecture and still cannot resolve a remotely accepted write.
- **A second memory pool that mirrors DataFusion and Lance usage:** parallel ownership of resources the substrate already owns.
- **A patched or vendored Lance to get the hooks:** the integration must use public APIs of an unmodified release.

## Not built

- One ownership tree through native I/O: local persistence, multipart parts, completion and abort, commit handlers and Lance CPU workers do not register with the operation, so a dropped native writer or reader can outlive it.
- The resource hierarchy with protected completion and status reserves, and process totals for caches, scratch, accepted I/O and output.
- The sealed drain that issues a single-use capability for engine reuse.
