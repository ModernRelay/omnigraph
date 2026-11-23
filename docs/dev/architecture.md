# Architecture

**Audience:** contributors and maintainers
**Authority:** system boundaries and crate ownership; detailed protocols live
in the linked area guides

Omnigraph is a typed property-graph engine that coordinates many Lance
datasets. Lance supplies columnar storage, per-dataset versions, branches,
transactions, and indexes. Omnigraph adds one accepted graph schema, one
graph-wide visibility boundary, graph query and mutation semantics, policy,
cluster deployment state, and public CLI/HTTP surfaces.

## System shape

The crate dependency graph is each crate's `Cargo.toml`
(`cargo tree -p <crate> --depth 1`).

These are separate libraries and binaries, not one monolithic process. An
embedded caller may use the engine directly; the CLI may use an embedded store
or the HTTP API; the server hosts multiple graphs from one applied cluster
revision.

## Authority model

Three kinds of state must not be conflated:

1. **Accepted graph state.** Accepted SchemaIR owns rename-stable type,
   property, and table-incarnation identities. `__manifest` owns the graph
   branch heads, visible table versions, and graph lineage. One manifest
   publication makes a graph change visible.
2. **Lance physical state.** Each node and edge table is a Lance dataset with
   its own versions, branches, fragments, and indexes. Graph writes stage detached
   versions; a registered table's linear HEAD remains at its creation version.
3. **Derived runtime state.** Topology indexes, physical index coverage,
   caches, fragment layout, and serving projections may be rebuilt. They never
   become a second authority for graph contents.

Names and paths are diagnostic aliases. A supported rename preserves stable
identity and table history; drop/re-add creates a new lifetime even if the
alias is reused. See [invariants.md](invariants.md) and
[versioning.md](versioning.md).

## Layers and ownership

| Layer | Owns |
|---|---|
| `omnigraph-compiler` | `.pg` and `.gq` parsing, catalog, type checking, lint, migration planning, and typed IR lowering. It has no Lance dependency. |
| `omnigraph-planner` | Logical and physical plans, optimizer passes, cost estimates and explain documents; metadata enters through `PlanSource`, with no Lance dependency. |
| `omnigraph` (`omnigraph-engine`) | The public facade (`Omnigraph`, `Snapshot`), the graph coordinator, query and mutation execution, the table store, graph topology, validation, and recovery. |
| `omnigraph-catalog` | `__manifest` publication (multi-dataset publication), graph branch registrations and lineage, retention, and `commit_graph`. |
| `omnigraph-core` | `OmniError`, Lance dataset access and instrumentation, native Lance branch ref control (`branch_control`), and dataset addressing. |
| Lance | Dataset files, transactions, versions, native refs, secondary indexes, compaction, and version cleanup. |
| `omnigraph-policy` | Cedar compilation and the engine-facing action/scope/actor gate. |
| `omnigraph-skill` | The `skills/omnigraph` agent skill, embedded byte for byte by its build script; the package root is `skills/`, so the skill tree stays its one copy. `omnigraph skill export` writes it. |
| `omnigraph-gqt-core` | GQT format, embedded and served execution, and expectation checks shared by the GQT runner and benchmark harness. It has no build script or test-only engine dependency. |
| `omnigraph-reference-engine` | Engine v1, frozen (`publish = false`, hash-pinned by its `tests/frozen.rs`): the reference executor a GQT step's `--- expect same as v1` compares engine v2 against. Its input is the pinned `omnigraph_compiler::ir::untyped` read IR, produced by one-way erasure only at the test reference door. It depends only on `omnigraph-compiler`, `omnigraph-core`, `omnigraph-catalog` and third-party crates; `omnigraph-gqt` is the only crate that may depend on it (`forbidden_apis.rs` guards both), and no production door reaches it. |
| `omnigraph-seams` | The one seam type for test-time behavior substitution: a named slot a test fills and the engine, core, catalog and cluster consult. |
| `omnigraph-bench` | Declarative benchmark planning and execution. |

The table lists the crates whose `lib.rs` has no crate-level doc for these
facts. The CLI, server, cluster, storage, API-types, MCP and Azure admission
crates document their ownership in their `lib.rs`; the CLI's command flow is
in the module docs under `crates/omnigraph-cli/src/`, from `lib.rs` and
`app.rs` down. How the server mounts `omnigraph-mcp` at `/mcp` is in
[control plane](control-plane.md#oidc-resource-identity-and-mcp).

## Principal flows

- A read resolves one branch or snapshot and keeps that immutable view for the
  full query. The compiler produces typed IR; the executor combines Lance
  scans/search with the scoped in-memory topology index. See
  [execution.md](execution.md).
- A content write captures one accepted authority view, stages exact Lance
  transactions as unreachable detached table versions, and publishes every table
  pin, accepted schema and lineage change in one manifest CAS. See [writes.md](writes.md) and
  [recovery.md](recovery.md).
- A merge performs a graph-level three-way comparison, validates the selected
  result, stages its table effects, and uses the same publication boundary. A fast forward whose candidates are all proven pure inserts into node
  types with no `@unique`, `@range`, `@check` or enum property, and no edge type
  with a non-zero `@card` minimum from them, skips the validation. See [merge.md](merge.md).
- Blob cells use an engine-owned, snapshot-bound facade; callers never receive
  a raw Lance `BlobFile` or physical placement. See [blob.md](blob.md).
- Cluster apply converges definitions and graph topology into a CAS-protected
  ledger. Servers boot from an applied serving snapshot and execute live
  deployments under their existing exclusive cluster admission. See [control-plane.md](control-plane.md).

## Trust and policy boundaries

Who resolves the actor and where policy is checked is `E6` of
[the contract](contract.md). No-policy embedded development remains an explicit
engine configuration.

Admission and authorization are different controls. `WorkloadController`
bounds served work, and the write queue orders in-process effects; neither is a
durable cross-process lock. Azure has the additional deployment admission
wrapper because mutation-capable Azure replicas must not rely on process-local
gates for single-writer ownership.

## Concurrency and support boundary

- A read sees one commit for its whole run (contract `E1`). A read-view capture takes a shared schema
  permit (so it cannot observe a contract mid-swap) and no branch or table
  gate, so it does not wait for writers on other handles. It does wait for
  an exclusive pass on any handle of the process (schema apply, the
  system-column upgrade, open, refresh, settle, reload, `sync_branch`,
  branch create), and, because the gate is write-preferring, from the
  moment one is queued. It takes no lock on its handle's coordinator: the
  installed state is an immutable value a read loads with one atomic
  pointer read, and a publish or refresh on that handle's bound branch does
  its storage round trips on a clone and installs the result with one
  pointer swap, refused when the installed state already supersedes it
  (`Omnigraph::install_coordinator`). A read and a write sharing one handle
  and branch (for example, the server's requests on `main`) do not wait for each other; a read that starts while a publish is in flight binds the
  installed state and sees the publish once it is installed. While the
  process holds the branch's [writer epoch](writes.md#writer-epoch) with its
  last publication there on record, a read of the handle's bound branch takes
  the installed state at or above that publication with no probe of the
  branch's head.
- Write preparation may overlap. Durable effects are ordered by the shared
  schema, branch, and sorted-table gates, then fenced again by persisted
  authority and Lance transaction identity.
- The gates prevent same-process races and deadlocks; manifest preconditions,
  Lance transactions, and accepted manifest identities provide durable correctness.
- Branch create and delete, schema apply, dropping a proposal and `cleanup`
  intentionally retain a one-mutation-process support boundary. Do not infer
  distributed writer safety from an in-process mutex. Gap `G2` of
  [the contract](contract.md#gaps) names them.
- Missing physical indexes or stale cache entries never change an answer
  (`E7`). Explicit reconciliation restores physical coverage.

## Storage failure boundary

The storage failure kinds and what they do not authorize are the doc comment
of `StorageFailureKind` in `crates/omnigraph-storage/src/lib.rs`;
[RFC 0038](../decisions/0038-typed-storage-failures.md) records the rationale.

## Storage backends

Graph datasets and cluster control objects support local paths, S3-compatible
URIs, and native Azure Blob `az://` roots. Azure writes are a preview (`G4`).
Any mutation-capable
Azure server must run through `omnigraph-azure-admission`; direct server boot is
for read-only qualification and tests, not writer ownership.

## Index facts in read planning

The engine gathers effective index metadata from each query table at its pinned
version, including correlated inner reads. `PlanSource` exposes one fact per
index name and keyed column. Coverage unions same-name segments and intersects
current fragments; missing bitmaps remain unknown. The planner prices Expand
as fully indexed only when every required endpoint has complete usable BTREE
coverage across its candidates. See the [index-facts decision](../decisions/2026-10-08-index-facts-as-planner-input.md).

Saved Expand inputs carry coverage provenance. Fresh plans use pinned facts;
accepted older plans with no provenance default to the legacy runtime probe.
Execution still adjusts for frontier size and warm CSR state. Explain statistics
are diagnostics and do not select the replay path. Root scans finalize their access through the same Lance read builder used by
execution. Static scans record a Boolean index query and residual or disable
indexing. A filtered scan whose table has no fact that could narrow it (no
index, or only indexes covering no current fragment, such as an untrained
full-text segment) records sequential access without building that read. Dynamic and dependent reads keep their explicit runtime paths.
Single String key equality can gain a canonical identity predicate before
finalization; historical IDs prevent broader key narrowing.
