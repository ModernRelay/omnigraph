---
rfc: "0050"
title: "Engine crate topology and the sealed substrate boundary"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-09-03
updated: 2026-09-03
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0050: Engine crate topology and the sealed substrate boundary

## Summary

The engine package (`omnigraph-engine`, library crate `omnigraph`) becomes six
crates in a strict dependency order. `omnigraph-lance` is the only production
crate that links Lance and DataFusion execution, and its public API names no
Lance type: callers receive pinned table references, read handles, typed scan
requests, staged-write handles, and a manifest row gateway. `omnigraph-graph`
owns accepted graph state: schema authority, manifest state, snapshots,
captured write authority, validation, lineage, and the change feed.
`omnigraph-exec` executes typed IR over read handles and owns the derived
topology index. `omnigraph-write` holds every operation that can move a
dataset HEAD or publish a manifest, behind one `publish_once`. `omnigraph`
keeps the `Omnigraph` handle, the policy gates, and today's public API by
re-export. Two leaves, `omnigraph-embedding` and `omnigraph-failpoints`, hold
the provider client and the failpoint registry that other crates already reach
through the engine.

The boundary that does not change: no storage layout, manifest schema,
recovery sidecar, wire format, CLI, HTTP, or query-language behavior changes.
The public Rust surface of `omnigraph` is preserved by re-export. The closed
inventory of public async `Omnigraph` methods that the write-surface guard
keys on stays on one handle. The sealed `TableStorage` trait stays the
deterministic-simulation injection point. Arrow remains the interchange type
across every boundary; only Lance and DataFusion execution are sealed.

## Motivation

The engine is one crate of 57 source files and about 84k lines. Its design
rules already describe a layered system: a sealed storage boundary, a logical
authority model, a typed executor, and one publication door. The crate shape
cannot express those layers, so each is held by review and by source scanning
instead of by the compiler. The evidence, from the current `main` on the Lance
11.0.0 pin:

- **The seal is lexical.** The forbidden-API guard's own header states that
  crate visibility is the primary boundary and the scanner is defense in
  depth. Inside one crate, visibility cannot separate the executor from the
  storage layer, so the seal is a deny-list of type names plus a `syn`-based
  registry over source text, with a `// forbidden-api-allow:` sentinel for
  exemptions. Thirty of the 57 files import a Lance or DataFusion type
  directly, including the logical modules: validation, the change feed, the
  Blob facade, branch control, the loader, the executor's import hub, and the
  runtime cache.
- **The read path holds writable handles.** `Snapshot::open_dataset` returns
  the read-only `SnapshotDataset` facade, but no engine module uses it; it
  exists for external callers. The executor, the graph index, validation, the
  change feed, and merge open raw `lance::Dataset` values through the
  crate-private opener, seven sites in `exec/query.rs` alone, and every
  `TableStore` read helper takes `&Dataset`. A `Dataset` is Lance's mutating
  handle. Only the guard keeps a read from calling a write method.
- **Read execution and write protocols share one module.** `exec` holds the
  read executor (`query.rs`, `projection.rs`) beside merge, mutation, and
  staging, joined by a glob-import hub. Measured usage shows the read files
  reference none of the write-side imports, so the coupling is namespace,
  not need, yet nothing enforces that it stays so.
- **Execution has side inputs.** The executor reads seven environment
  variables and one task-local override during execution, and performs an
  outbound embedding request for a String `nearest` argument inside the
  pipeline. Invariant 3 asks for one coherent view per operation; a knob read
  mid-traversal is not that.
- **Substrate changes are not localized.** The Lance 11 upgrade commit
  modified 16 engine source files across the manifest, change-feed, executor,
  loader, and storage modules, plus ten engine test files, before any
  user-visible work.
- **The wire-DTO crate depends on the engine.** `omnigraph-api-types` imports
  `Snapshot`, `GraphCommit`, `MergeOutcome`, `SchemaApplyResult`, and the load
  receipt types from the engine, so it is not the additive leaf that
  [the architecture guide](../dev/architecture.md) describes.

The five-changes lens: adaptive traversal, RRF gating, ANN probe bounds, the
retrieval IR of RFC 0047, the index status surface of
[RFC 0046](0046-index-status.md), and each Lance bump all land in the same
compile unit and the same guard scope. Five more changes of that kind leave
the executor, the write protocol, and the substrate as one crate whose real
boundaries live in a test file.

An issue or a local refactor can tidy imports, but it cannot make the seal a
compiler fact, and it cannot change what the guards are able to enforce. Crate
names, published packages, and public types are a lasting contract, which is
why this is an RFC.

## User and operational behavior

- No user-visible change. CLI, HTTP routes, OpenAPI, `.pg` and `.gq`
  behavior, storage layout, internal manifest schema, and recovery sidecar
  schema are unchanged.
- Embedded Rust users keep `omnigraph` (package `omnigraph-engine`) as their
  dependency. Every public type and method they use today stays reachable at
  its current path through re-exports. A `cargo-public-api` diff of the facade
  before and after each phase shows only additive re-exports.
- The new crates are published in lockstep with the workspace version. The
  publish workflow's crate order gains the new crates below the facade.
- Environment knobs keep their names and defaults. They are captured once per
  query at the facade instead of being read mid-pipeline, so a value changed
  during a traversal no longer changes that traversal's mode.
  [The execution guide](../dev/execution.md) records the capture point.
- Failure posture is unchanged. `OmniError` and `StorageFailure` from
  [RFC 0038](0038-typed-storage-failures.md) keep their variants and paths.
- Build commands are unchanged in shape. `-p omnigraph-engine` builds the
  facade and its dependencies; `--workspace` reaches every crate. The
  `failpoints` and `dst` features are forwarded by the facade to the crates
  that own them, so the canonical CI graph is the same invocation.

## Design

### Topology

```text
   omnigraph-server     omnigraph-cli     omnigraph-cluster     omnigraph-gqt
          \                  |                  /                    /
           v                 v                 v                    v
                    omnigraph   (facade: the Omnigraph handle, _as policy gates,
                                 read-view capture, re-exports, dst seams)
                     /                          \
                    v                            v
           omnigraph-exec  <---------------  omnigraph-write
           (IR execution over read handles;  (mutation, load, merge, schema apply,
            derived topology index)           indexes, optimize, repair, branch refs,
                    \                         recovery protocol; publish_once)
                     v                          /
                              omnigraph-graph  <-----  omnigraph-api-types
                              (accepted schema, manifest state, Snapshot,
                               WriteTxn, validation, lineage, change feed,
                               logical Blob facade, error types)
                                      |
                                      v
                              omnigraph-lance
                              (sealed substrate: staged primitives, exact commit,
                               read handles, manifest row gateway, recovery
                               primitives, sessions, physical maintenance)
                                      |
     omnigraph-compiler   omnigraph-storage   omnigraph-policy
     omnigraph-embedding  omnigraph-failpoints              (leaves, no Lance)
```

Arrows point from dependent to dependency. `omnigraph-write` depends on
`omnigraph-exec` for predicate lowering and read-your-writes evaluation; the
executor never depends on the write crate. The benchmark and simulation
harnesses keep their direct Lance dependencies for instrumentation and fault
injection; they are test tooling, not production crates.

### Crates

| Crate | Owns | Must not contain | Today's modules |
|---|---|---|---|
| `omnigraph-lance` | Staged write primitives and exact commit, read handles and typed scan requests, index coverage probes, the manifest row gateway with CAS, recovery primitives, cleanup and compaction, index build, sessions and the process-wide store registry, the held-handle cache, physical Blob access, I/O probes, the Lance surface guards | Any graph semantics; it does not know what a node, edge, branch, or schema is | `table_store.rs` and its submodules, `storage_layer.rs`, `lance_access.rs`, `db/manifest/{namespace,publisher,graph,layout,metadata}.rs`, `db/recovery_audit.rs`, the Lance-facing half of `db/manifest/recovery.rs`, the opener and probes in `instrumentation.rs`, the handle cache in `runtime_cache.rs`, the physical half of `blob.rs` and `db/omnigraph/optimize.rs` |
| `omnigraph-graph` | Accepted SchemaIR state and its catalog projection, manifest state model and migrations, `Snapshot`, `ReadTarget` resolution, commit graph and lineage, `WriteTxn` capture, branch naming, catalog-derived validation, change feed, export cut, logical Blob facade, public receipt and outcome types, `OmniError` | Any Lance or DataFusion execution type in its public API | `db/manifest.rs`, `db/manifest/{state,migrations}.rs`, `db/{graph_coordinator,commit_graph,schema_state}.rs`, `branch_names.rs`, `validate.rs`, `changes/`, `db/omnigraph/export.rs`, the logical half of `blob.rs`, `error.rs`, `storage.rs` |
| `omnigraph-exec` | IR execution: search mode from the lowered retrieval, expand with its cost model and adaptive switch, node scan, anti-join, projection, aggregation, ordering, IR-to-predicate lowering, batch utilities, the CSR/CSC topology index with its persisted-artifact codec and version-keyed cache | Network calls, environment reads, task-locals, `_as` entry points, anything that can move a HEAD | `exec/query.rs`, `exec/projection.rs`, `graph_index/`, the index cache in `runtime_cache.rs` |
| `omnigraph-write` | Mutation, staging, load publication, three-way merge, schema apply, `ensure_indices`, `optimize`, `repair`, cleanup floors, branch ref control, the recovery protocol over substrate primitives, the write queue gates, and `publish_once` | A manifest publication from any function except `publish_once` | `exec/{mutation,staging,merge}.rs`, the publication half of `loader/mod.rs`, `db/omnigraph/{table_ops,schema_apply,repair}.rs`, the protocol half of `db/omnigraph/optimize.rs`, `branch_control.rs`, `db/write_queue.rs`, the protocol half of `db/manifest/recovery.rs`, the write orchestration in `db/omnigraph.rs` |
| `omnigraph` | The `Omnigraph` handle: open, init, refresh, read-view capture, every `query*` and `_as` entry, policy gate application, query-vector resolution, handle-level caches, re-exports, and the `dst_*` seams | Logic beyond capture, gate, delegate, and map | The remainder of `db/omnigraph.rs`, the orchestration in `exec/query.rs`, `dst_{clock,gate,ids}.rs`, `lib.rs` |
| `omnigraph-embedding` | The provider-independent embedding client and its configuration | Engine types | `embedding.rs` |
| `omnigraph-failpoints` | The registry-as-value failpoint machinery and the name namespace | Engine types | `failpoints.rs` |

Two existing crates change shape without changing role. `omnigraph-compiler`
gains the loader's pure NDJSON and JSONL parsing and the date-literal parser,
because both are catalog-driven input validation with no storage dependency.
`omnigraph-api-types` depends on `omnigraph-graph` and `omnigraph-compiler`
only; the receipt and outcome types it converts are defined in the graph crate
and produced by the write crate.

Approximate sizes from the current tree, so reviewers can weigh each cut:

| Crate | Lines |
|---|---:|
| `omnigraph-write` | 28k |
| `omnigraph-lance` | 21k |
| `omnigraph-graph` | 16k |
| `omnigraph-exec` | 8k |
| `omnigraph` facade | 3k |
| `omnigraph-embedding` | 1k |
| `omnigraph-failpoints` | under 1k |

### Seams

Each crate boundary is a type. Each type is chosen so that an invariant that
is reviewed today becomes a compile error tomorrow.

**Table references and read handles.** A `Snapshot` maps each table key to a
`TableRef`: stable table identity, table incarnation, dataset path, and pinned
dataset version. It never holds a Lance handle. Opening a `TableRef` through
the substrate crate yields a `ReadHandle` that exposes scan, count, Arrow
schema, published version, index coverage and index metadata probes, and Blob
reads. `SnapshotDataset` is the seed of this type. A `ReadHandle` has no
method that can move a HEAD, and the underlying `Dataset` is private to the
substrate crate, so the deny-list item on public writable dataset handles is
enforced by the compiler rather than by the scanner.

**Typed scan requests.** `ReadHandle::scan` takes a `ScanRequest` carrying
projection, predicate, prefilter, a full-text request, a nearest request,
limit and offset, batch sizing, row-address and row-id selection, Blob
handling, ordering, and the single-partition fence for late payload
hydration. The substrate crate owns `FtsQuery` and `NearestQuery` so that no
`lance-index` type crosses the seal. The predicate is the DataFusion
expression AST from `datafusion-expr`, which is a pure expression crate with
no execution engine; the executor and the graph crate may depend on it, and
only `omnigraph-lance` may depend on `datafusion`, `lance-datafusion`, or any
`lance*` crate.

**One predicate lowering.** `omnigraph-exec` owns the IR-to-predicate
lowering, and the mutation path uses it for update and delete predicates
through the substrate's typed delete staging. The string-building
`predicate_to_sql` seam that [the execution guide](../dev/execution.md)
lists as a retained compatibility boundary is retired with it. The retrieval
shape comes from the lowered plan as RFC 0047 proposes; the executor's search
module translates that plan into the substrate's typed requests and never
re-infers a mode from ordering.

**Captured write authority.** `omnigraph-graph` captures a `WriteTxn` from one
attempt: accepted schema hash, branch, native ref identity, graph head, and
per-table baselines as `TableRef` values. `omnigraph-write` revalidates the
complete token under the shared gates and never re-reads authority mid-attempt.
The capture-then-use split across two crates is invariant 3 as a data-flow
constraint.

**Authorized action tokens.** Every public function of `omnigraph-write` takes
an `Authorized<Action>` value alongside the `WriteTxn`. Only the policy gate in
the facade can construct one, from the installed `PolicyChecker` and the
resolved actor, or from the explicit no-policy embedded configuration. An
embedded caller that reaches the write crate without the gate has no way to
name the token type's constructor. Invariant 10 stops being a checklist item
for every new entry point.

**One publication call.** `publish_once` in `omnigraph-write` takes every
staged participant plus the lineage update and performs the single manifest
CAS through the substrate's row gateway. It is the only caller of that
gateway's publish method, and the structural guard pins that fact.

**Execution configuration captured once.** The facade reads the traversal
override, the expand caps, the RRF gate settings, and the ANN probe bound into
an `ExecConfig` at query start and passes it in. Query vectors for String
`nearest` arguments are resolved by the facade through `omnigraph-embedding`
before execution begins. `execute_query` is then a function of the IR, the
parameters, the snapshot, the catalog, the configuration, and the lazily
built topology index; its only I/O is Lance reads through read handles.

**Recovery in two halves.** Operations that touch Lance to witness a
transaction identity, restore a dataset to an exact version, or drop staged
files are primitives in `omnigraph-lance`. Sidecar encoding, classification
into completed, partial, ambiguous, and foreign outcomes, and roll-forward or
compensation decisions are the protocol in `omnigraph-write`, beside the
writers they serve. [The recovery guide](../dev/recovery.md) keeps one
protocol owner; the cut adds no second decision maker.

**Derived state stays derived.** The topology index, its persisted artifact
under the graph root, and the version-keyed cache live in `omnigraph-exec`.
`optimize` in the write crate calls the executor's codec to save the artifact;
loads verify per-table identity stamps and fall open to a scan build, exactly
as today. No logical decision anywhere reads the artifact.

### Inherited Lance behavior

Lance still owns dataset files, transactions, versions, native refs, secondary
indexes, compaction, and cleanup. This RFC changes which OmniGraph crate is
allowed to speak to Lance, not what Lance does. Every fence in
[the Lance guide](../dev/lance.md) keeps its test owner; those owners move
with the substrate crate.

### Guards

- **Dependency guard.** A workspace test reads `cargo metadata` and fails if
  any production crate other than `omnigraph-lance` lists a `lance*` crate,
  `datafusion`, or `lance-datafusion` under `[dependencies]`. The benchmark
  and simulation harnesses are the named exceptions. This replaces the
  lexical deny-list.
- **Public API snapshot.** The public surfaces of `omnigraph-lance` and
  `omnigraph-write` are captured with `cargo-public-api`, the tool the
  vocabulary guard already drives, and compared in CI. A `Dataset`, `Scanner`,
  or `Transaction` in a signature is a failing diff; a new export is a
  reviewed diff.
- **Structural registry.** The existing write-surface registry keeps pinning
  every public async inherent `Omnigraph` method to a protocol disposition,
  and additionally pins `publish_once` as the sole caller of the manifest
  gateway's publish method.
- **Facade snapshot.** A `cargo-public-api` capture of `omnigraph` before
  phase 1 is the compatibility baseline every later phase is diffed against.

### Features

`failpoints` is owned by `omnigraph-failpoints` and enabled by the substrate,
write, and cluster crates; `dst` is owned by the substrate crate for the
injected store registry and by the facade for the seeded clock, id, and gate
seams. The facade forwards both so existing feature flags keep working.

## Invariants

1. **Respect the substrate.** Strengthened. One crate reads
   [the Lance guide](../dev/lance.md) and owns every fence; a Lance bump is a
   change to one crate plus its public API snapshot.
2. **One publication door.** Strengthened. `publish_once` is the only manifest
   publication, and the guard pins it structurally rather than by inventory of
   call sites.
3. **One coherent accepted view.** Strengthened. `Snapshot` and `WriteTxn` are
   captured in the graph crate and consumed above it; `ExecConfig` is captured
   once per query. A mid-operation re-read has no API to call.
4. **A mutation publishes once.** Unchanged. Staging still accumulates every
   participant, and the D2 constructive-versus-destructive split stays in the
   write crate.
5. **Recovery is part of the commit protocol.** Unchanged in behavior. The
   protocol has one owner in the write crate; primitives in the substrate
   crate carry no decisions.
6. **Stable identity survives renames.** Strengthened. `TableRef` carries
   stable table identity and incarnation; a path or native ref name never
   crosses a crate boundary as identity.
7. **Physical acceleration is derived state.** Unchanged. Coverage probes,
   the topology index, and the persisted artifact remain derived, and the
   dependency direction makes it impossible for the graph crate to consult
   them.
8. **Integrity failures are loud.** Unchanged. Typed outcomes move crate and
   keep their variants.
9. **Query semantics are typed structures.** Strengthened. `ScanRequest`,
   `FtsQuery`, `NearestQuery`, and the predicate AST replace the last
   string-built predicate seam.
10. **Trust at the boundary, enforced at the engine.** Strengthened.
    `Authorized<Action>` makes the engine-side gate unskippable by type.
11. **Bounded, observable failures and resources.** Unchanged. Retry bounds,
    budgets, and probes move with their owners; the I/O probes live where I/O
    happens.
12. **One source of truth, cheaply derived.** Unchanged. No new state is
    introduced; the handle cache and topology cache stay version-keyed hints.
13. **Evidence matches the boundary.** Strengthened. Each crate has test
    owners at its own contract, and executor tests can build fixtures through
    staged primitives without the mutation protocol.

Deny-list review: raw Lance writers and public writable `Dataset` handles
become unrepresentable above the seal; ad-hoc SQL predicate generation is
retired; no job queue, WAL, buffer pool, shadow truth, or cloud-only path is
added; the process-local write queue stays process-local and documented as
such. No known gap changes.

## Compatibility and reversibility

- **Storage and wire.** No dataset, manifest, sidecar, artifact, HTTP, or
  OpenAPI change. A graph written before this RFC is read by a binary built
  after it, and the reverse, with no migration or refusal.
- **Rust API.** The facade's public surface is preserved by re-export and
  proven by the facade snapshot. Types that move crates keep their paths
  through `omnigraph`. Downstream workspace crates may keep depending on the
  facade or switch to the crate that owns the types they use; `omnigraph-cluster`
  is expected to switch its failpoint and embedding-config imports to the leaf
  crates.
- **Publishing.** New crates ship on crates.io with the workspace version.
  Their public API is a contract from the first publish; the API snapshot
  guard exists for that reason.
- **Support boundaries.** The one-mutation-process boundaries listed in
  [the invariants](../dev/invariants.md) are unchanged; the write queue and
  the merge mutex remain in-process gates.
- **Reverting.** Each phase is a module move plus a type narrowing, and each
  can be reverted alone by moving code back into the facade crate. The cost
  of reverting the whole topology is the reverse of the moves; no persisted
  state is touched. Reverting after publishing removes crates from crates.io
  consumers, which is the one irreversible cost, so the publish of each new
  crate happens at the end of its phase, not the start.

## Alternatives

- **Keep one crate and tidy modules.** Explicit imports, moving the write
  protocols out of `exec`, and pointing read helpers at the read facade
  capture most of the dependency hygiene, and they are phase 0 of this RFC.
  They cannot make the seal a compiler fact or stop the next Lance bump from
  touching sixteen files. Rejected as the end state, adopted as the first
  phase.
- **Extract the executor above the current engine.** A naive split puts
  `omnigraph-exec` on top of today's crate and forces the crate-private
  opener and the `&Dataset` read helpers public, trading a compiler fence for
  a doc-hidden convention. Rejected; the seal must move down first.
- **A storage backend trait object.** One implementation exists and is
  sealed; the crate boundary is the seam. A trait object would invite a mock
  store that tests what does not matter, when the simulation harness of
  [RFC 0037](0037-deterministic-simulation-harness.md) already injects faults
  below the executor at the Lance I/O seam. Rejected.
- **A DataFusion-plan executor.** Replacing the hand-rolled Arrow operators
  with DataFusion plans is a separate decision. This topology makes it a
  change local to `omnigraph-exec` if it is ever taken.
- **Split the `Omnigraph` handle.** The closed inventory of public async
  methods is what lets the write-surface guard reject an unregistered
  `transact` or `vacuum`. Rejected.
- **One `omnigraph-core` holding graph and write.** Simpler to cut, but the
  capture-then-use split of `WriteTxn` is the invariant-3 seam, and the two
  halves change at different rates: the authority model is stable while the
  write protocols follow the RFC 0022 family. Rejected; they start separate.
- **Do nothing.** The guards hold today. They hold by scanning source, and
  every planner, search, and substrate change widens the scope they scan.
  Rejected on the five-changes lens.

## Evidence and tests

- **Existing owners move with their code.** `lance_surface_guards.rs`,
  `lance_version_columns.rs`, the physical half of `maintenance.rs`, and the
  Blob compaction fences go to `omnigraph-lance`. `traversal.rs`,
  `traversal_indexed.rs`, `aggregation.rs`, `ordering.rs`,
  `literal_filters.rs`, `search.rs`, `scalar_indexes.rs`, and
  `proptest_equivalence.rs` go to `omnigraph-exec` and build fixtures through
  staged primitives and a test manifest, so executor evidence no longer
  routes through the mutation protocol. `writes.rs`, `validators.rs`,
  `schema_apply.rs`, `recovery.rs`, `failpoints.rs`, `failpoint_names_guard.rs`,
  and the `merge_*` suites go to `omnigraph-write`. `branching.rs`,
  `point_in_time.rs`, `changes.rs`, `changes_cost.rs`, `export.rs`, and
  `lineage_projection.rs` go to `omnigraph-graph`. `lifecycle.rs`,
  `end_to_end.rs`, `composite_flow.rs`, `consistency.rs`, and
  `policy_engine_chassis.rs` stay on the facade. The map in
  [the testing guide](../dev/testing.md) is updated in the same phase as
  each move.
- **New guards.** The dependency guard, the two public API snapshots, the
  facade snapshot, and the `publish_once` pin from the Design section. The
  structural registry in `forbidden_apis.rs` is retargeted to the write crate;
  its lexical deny-list is deleted when the dependency guard lands.
- **Behavior proof.** The `.gqt` corpus of [RFC 0045](0045-gq-logic-tests.md),
  the server and CLI suites, the parity matrix, and the simulation scenarios
  run unchanged at every phase. A phase that needs a test body to change has
  changed behavior and is rejected.
- **Cost proof.** Cross-crate boundaries can change inlining in the hot Arrow
  paths. `warm_read_cost.rs`, `write_cost.rs`, and `merge_cost.rs` prove the
  deterministic operation counts are unchanged, and the local benchmark suite
  is run before and after phases 2 and 4 under the release profile, which
  already uses thin LTO. A wall-time regression above the suite's noise band
  blocks the phase until the boundary is adjusted.
- **Upstream surfaces.** No Lance surface is added or removed; the phases
  reuse the current `lance` 11.0.0 and `datafusion` 54 pins and touch no
  upstream page that [the Lance guide](../dev/lance.md) does not already
  list.

## Rollout

Every phase is a green canonical test graph with unchanged public behavior.
Nothing user-facing is unavailable at any stop.

0. **In-crate preparation.** Replace the `exec` glob-import hub with explicit
   imports and move mutation, staging, and merge out of `exec` so the module
   means read execution. Point the read helpers at `SnapshotDataset` and ban
   the crate-private opener outside the storage, manifest, and graph-index
   modules. Hoist query-vector resolution and knob capture into the facade
   behind `ExecConfig`. Move the date and NDJSON parsers to the compiler.
   Extend `forbidden_apis.rs` with an import allow-list for the read
   executor. `implementation` advances to `in-progress`.
1. **Leaves.** Extract `omnigraph-failpoints` and `omnigraph-embedding`; the
   facade re-exports both; `omnigraph-cluster` switches its imports.
2. **The substrate crate.** Extract `omnigraph-lance` with `TableRef`,
   `ReadHandle`, `ScanRequest`, `FtsQuery`, `NearestQuery`, the staged-write
   handles, the manifest row gateway, and the recovery primitives. Land the
   dependency guard and the substrate API snapshot, move the surface guards,
   and delete the lexical deny-list. At this stop the seal is a compiler
   fact and everything else is still one crate. `implementation` advances to
   `partial`.
3. **The graph crate.** Extract `omnigraph-graph` with `Snapshot`, the
   manifest state model, `WriteTxn`, validation, the change feed, the logical
   Blob facade, the receipt types, and `OmniError`. `omnigraph-api-types`
   switches to it.
4. **The executor crate.** Extract `omnigraph-exec`, splitting `query.rs` by
   concern: search, expand, scan, anti-join, lowering, projection, batch
   utilities, and the topology index. Executor suites move and gain
   primitive-built fixtures.
5. **The write crate.** Extract `omnigraph-write` with `Authorized<Action>`,
   `publish_once`, and the recovery protocol. Retarget the structural
   registry and land the write API snapshot.
6. **Facade and documentation.** Thin `db/omnigraph.rs` to capture, gate,
   delegate, and map. Update the layers table in
   [the architecture guide](../dev/architecture.md), the ownership map in
   [the testing guide](../dev/testing.md), and the fence owners in
   [the Lance guide](../dev/lance.md). `implementation` advances to
   `complete`.

Phases 3, 4, and 5 may land in either order after phase 2, with the
constraint that the write crate cannot be cut before the graph crate holds
`WriteTxn`.

## Unresolved questions

- Whether the predicate crossing the seal is the `datafusion-expr` AST, as
  designed above, or an owned `Predicate` type wrapped by `omnigraph-lance`.
  The AST keeps lowering in one place and adds a pure crate above the seal;
  the wrapper keeps DataFusion entirely below it at the cost of a second
  expression vocabulary. The design recommends the AST.
- Whether `omnigraph-graph` and `omnigraph-write` are published as separate
  crates from their first release or kept `publish = false` behind the
  facade until their APIs settle. Cargo requires path dependencies of a
  published crate to be published, so the second option constrains the
  facade's own publish; the design recommends publishing in lockstep.

## Decision log

- 2026-09-03: Draft opened.
