# Remote optimize through the serving owner

**Decides:** a served graph's `optimize` of main runs in the process that holds the cluster lock, as a bounded run under a caller-chosen operation id whose acceptance is durable in `__cluster/operations.json` before any effect, and whose in-doubt publication is settled by main's history, never by running it again.
**Contract:** keeps maintain:1, maintain:2, E4, E5, E7
**Code:** `crates/omnigraph/src/db/omnigraph/optimize.rs`, `crates/omnigraph/src/db/omnigraph/table_ops.rs`, `crates/omnigraph/src/db/omnigraph/resolution.rs`, `crates/omnigraph-cluster/src/operations.rs`, `crates/omnigraph-server/src/recorded_operations.rs`, `crates/omnigraph-cli/src/commands/maintenance/served.rs`

## Context

A direct `optimize` takes the cluster lock, so while a server runs nothing removes the fragments small commits add, and the only remedy is to stop the server. Running it inside the server needs three things a synchronous call lacks: a caller that loses the response must learn what happened without resubmitting, a run must not hold every writer of main for the minutes a large catalog takes, and a restart must neither repeat the work nor block serving on an operator.

## Decision

- `optimize_as` is the one engine entry point and enforces the `maintain` grant on main; `admin` does not grant it.
- A run keeps today's gates and publishes once. Before each table's rewrite it checks the gate-hold time and the run byte budget; once either is reached, that table and every later one are `deferred` and the run publishes what it finished. A table over a per-table ceiling is `deferred(resource_limit)` and nothing is written for it. A size the run cannot read is that table's failure.
- Every table gets one outcome: `optimized`, `unchanged`, `deferred(reason)` or `failed(code)`. A table-local failure fails only that table. An index link that fails after an earlier link of its table committed keeps the committed links, reported as `index_failed`.
- A full-catalog run works in type-key order from a resume position and wraps. The server stores the first table a bound deferred as the graph's position; a direct run prints it as `resume_at` for `--start-at`.
- The publication helper takes an optional `before_publish` callback, called once before the first manifest request with the intent nonce and the input head. The server writes them into the record there. Other writers pass none.
- The configuration clear stays under the gates. The adjacency artifact is built after every writer gate is released, bounded by `artifact_write_limit`, and an issued write is awaited, never dropped. Neither fails the operation.
- The record is generic, `optimize` its first kind. It is written by conditional put at acceptance, at `running`, in `before_publish` and at the end. One operation id names one exact request. Terminal records are capped by the deployment receipts' bounds, and an eviction mark refuses an id at or before the newest evicted one.
- A `publishing` record stays in doubt while main's graph head is the input head. Once the head moves, a paged first-parent walk finds the input head's child: it answers the nonce and the run `succeeded`, or it does not and the run `failed`. At most four unresolved records are kept; a fifth refuses admission.
- One operation is active per cluster in the maintenance scope. The routes are off until an operator enables them, and a request names only `datasets` and a `max_gate_hold_ms` at or below the server's ceiling.
- A served CLI run mints the id, waits, and after a lost response looks it up; it never resubmits.

## Rejected

- **A synchronous call with no record:** a lost response stays unknown for good, and the control plane needs an id it can look up.
- **Recovery that blocks admission until an administrator reconciles:** main's history already answers whether the commit exists.
- **A section in `__cluster/state.json`:** older binaries refuse the ledger, which denies unknown fields.
- **Server-assigned ids:** a lost acceptance response would leave nothing to look up.
- **A general job registry with JSON specs and pause:** it carries semantics as strings and its job knows nothing of the graph commit it produced.

