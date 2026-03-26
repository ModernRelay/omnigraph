# Architecture Review

Review of the current implementation on `main` against the current Omnigraph target architecture.

- lake-native graph database
- Lance-native storage, versioning, and branching primitives
- explicit branch/snapshot targeting
- Arrow-native runtime with derived graph indices
- server-first API model

Some sections of the checked-in docs on `main` still describe the current prototype shape.
This review is intentionally comparing the code to the target architecture, not to stale descriptive sections.

## Executive Summary

The current implementation is not architecturally wrong from scratch.
The storage and execution substrate is already moving in the right direction:

- `Omnigraph` exists as the top-level handle
- graph state is coordinated through a Lance manifest table
- data is stored in per-type Lance datasets
- snapshots pin reads to manifest versions
- traversal uses a derived in-memory graph index
- persisted identity is already string-based

The drift is mostly in the coordination and product layer:

- the API still relies on ambient current state instead of explicit targets
- graph-level branch and merge semantics are mostly missing
- the commit protocol is still prototype-grade
- the cache model is too implicit for multi-branch, multi-writer use
- remote and service-first semantics are not real yet

This means the project does not need a rewrite.
It does need a structural refactor before more product features are added.

## What Is Already Aligned

### 1. Top-Level Handle

Current implementation already uses `Omnigraph` as the main entry point instead of reusing Nanograph's old `Database` shape.

That is the right direction.

### 2. Lance Manifest + Per-Type Tables

Current implementation already stores:

- one Lance manifest table
- one Lance dataset per node type
- one Lance dataset per edge type

This is very close to the target storage model.

### 3. Pinned Snapshots

`Snapshot` already means "read this graph at these pinned sub-table versions."

That is one of the most important target semantics, and it is already present.

### 4. Hybrid Runtime Shape

The current system already separates:

- persisted string IDs on disk
- derived dense runtime IDs in the graph index

That is also aligned with the target direction.

## Main Conflicts

### 1. Ambient Current State vs Explicit Target API

### Current Implementation

Most operations act on one mutable `Omnigraph` handle and implicitly use whatever graph state that handle currently knows about.

Simple example:

```rust
db.refresh().await?;
db.run_query(source, "friends_of", &params).await?;
```

The query does not say:

- which branch it is reading
- which snapshot it is reading
- whether the caller expects moving branch-head semantics or pinned snapshot semantics

The answer is just: "whatever `db` currently points at."

### Target Architecture

The target model is explicit targeting:

```rust
db.query(Target::Branch("main"), "friends_of", &params).await?;
db.query(Target::Snapshot(snapshot_id), "friends_of", &params).await?;
db.load(Target::Branch("experiment"), data, LoadMode::Merge).await?;
```

### Why This Matters

This is not just API style.
It changes the whole architecture:

- request routing becomes explicit
- auth and audit become explicit
- retries become safe
- cache keys become explicit
- server APIs map cleanly to the core model

With the current shape, branch/snapshot identity lives in mutable object state instead of in the call contract.

### 2. One Known Graph State vs Real Graph-Level Branch Semantics

### Current Implementation

The code has:

- one manifest coordinator
- one known manifest version
- one `snapshot()` call over that known version

There is no real graph-level branch/ref model yet.
There is a `table_branch` field in manifest rows, but the system does not expose a real branch-aware API or branch-aware snapshots.

Simple example:

```rust
let snap = db.snapshot();
let ds = snap.open("node:Person").await?;
```

This is a pinned read, but it is not "snapshot of branch `main`" or "snapshot of branch `feature-x`."

### Target Architecture

The target system needs graph-level semantics like:

- branch heads
- pinned snapshots
- later merge and remote sync

That requires a real coordinator-level notion of:

- branch name
- snapshot identity
- graph commit point

not just one mutable handle with one latest-known manifest.

### Why This Matters

Without this layer, Omnigraph is still effectively:

"a single graph database with versioned reads"

not:

"a graph database with git-like collaboration semantics."

### 3. Single Cache Slot vs Cache Keyed by Target

### Current Implementation

`Omnigraph` has one cached graph index:

```rust
cached_graph_index: Option<Arc<GraphIndex>>
```

That means the cache answers the question:

"do we currently have some topology cache?"

instead of:

"do we have the topology cache for branch `main` at snapshot `42`?"

Simple example:

```rust
let idx1 = db.graph_index().await?;
db.refresh().await?;
let idx2 = db.graph_index().await?;
```

Today, `idx2` can still be the old cache even after `refresh()`.

### Target Architecture

The target system needs caches keyed by graph identity:

```rust
HashMap<GraphIndexKey, Arc<GraphIndex>>
```

Where the key is derived from:

- branch
- snapshot
- relevant edge-table versions

### Why This Matters

In a branchable graph database, topology is not global.
It is specific to a graph state.

A single implicit cache slot is workable for a local prototype, but it is the wrong model for:

- multiple branches
- multi-writer refresh
- service requests
- pinned snapshots

### 4. Commit Protocol Is Still "Write First, Validate Later"

### Current Implementation

The loader currently:

1. writes node and edge sub-tables
2. advances the manifest
3. validates edge cardinality

Simple example:

```text
load bad edges
-> data becomes visible
-> cardinality check fails
-> API returns error
```

So the caller hears "the write failed" after the graph was already mutated.

### Target Architecture

The target architecture treats the manifest advance as the graph-level commit point.

The desired behavior is:

```text
prepare writes
validate constraints
advance manifest
readers now see new graph state
```

If validation fails, old graph state should remain visible.

### Why This Matters

This is the difference between:

- prototype ingestion
- real graph commit semantics

If a failed write can still mutate visible graph state, branch/merge reasoning becomes unreliable very quickly.

### 5. Edge Rows Store Raw Strings Instead of Enforced Graph References

### Current Implementation

The loader currently takes edge `from` and `to` strings and writes them directly into `src` and `dst`.

Simple example:

```json
{"edge":"WorksAt","from":"alice","to":"acme"}
```

Today this can be stored even if:

- there is no `Person` with id `alice`
- there is no `Company` with id `acme`
- `alice` actually belongs to the wrong node type

### Target Architecture

The target identity model still uses string `src` and `dst`, but those strings must be valid graph references.

That means:

- endpoint IDs must exist
- endpoint types must match the schema
- orphan edges are not allowed in v1

### Why This Matters

The problem is not "string IDs are bad."
The problem is:

"the system currently treats graph references as unchecked text."

That breaks:

- traversal correctness
- merge correctness
- constraint enforcement
- data trustworthiness

### 6. URI-Shaped API vs Local Filesystem Reality

### Current Implementation

The surface API looks URI-friendly:

```rust
Omnigraph::open(uri)
```

But core file operations still use local filesystem calls like `std::fs::write` and `std::fs::read_to_string`.

Simple example:

```rust
let db = Omnigraph::open("s3://bucket/my-graph").await?;
```

This reads like supported behavior, but the schema file path still resolves through local path handling.

### Target Architecture

The target model is:

- same API for local and remote URIs
- object-store-aware open/init behavior
- service-first compatibility

### Why This Matters

Right now the API surface over-promises.
It suggests "lake-native URI" semantics before the implementation really has them.

### 7. Server-First Product vs Library-First Shape

### Current Implementation

The current core model still feels like an embedded database handle:

- open one handle
- mutate it
- refresh it
- run reads against its known state

Simple example:

```rust
let mut db = Omnigraph::open("./graph").await?;
db.refresh().await?;
db.run_query(source, "q", &params).await?;
```

### Target Architecture

The target product is server-first:

- each request states its target
- state is explicit, not ambient
- auth/routing/audit can sit cleanly above the core API

### Why This Matters

This is not about HTTP specifically.
It is about whether the core database model is designed to be safely wrapped by a service.

Right now it is only partially there.

### 8. CLI Surface Exists, Product Behavior Does Not

### Current Implementation

The CLI advertises commands like:

- `init`
- `load`
- `branch create`
- `snapshot`

but the command handlers are still stubs.

### Target Architecture

The target CLI is just one frontend over the real product semantics:

- init a graph
- load into a target branch
- inspect snapshots
- later branch and merge

### Why This Matters

This conflict is less about architecture purity and more about surface honesty.
The product vocabulary exists before the underlying behavior does.

## Drift Assessment

The drift is not uniform.

### Low Drift

These parts are already close enough that they should be preserved:

- compiler pipeline
- per-type Lance storage
- manifest table as coordinator substrate
- snapshot reads
- graph index concept
- string identity direction
- Arrow/Lance execution direction

### Medium Drift

These parts need real refactoring but not replacement:

- loader correctness
- commit protocol
- cache invalidation model
- URI handling
- service-friendly API shape

### High Drift

These parts are mostly not implemented yet:

- explicit branch/snapshot target API
- graph-level branch/ref model
- merge semantics
- remote collaboration model
- service and CLI product layer

## Recommendation

Do not rewrite the engine.
Do not keep layering product features on the current shape either.

The right move is:

1. stabilize correctness in the current single-graph core
2. refactor the public API around explicit targets
3. turn the manifest coordinator into the real graph coordinator
4. add branch, merge, remote, and service layers on top of that

In short:

- the foundation is good enough to keep
- the coordination layer needs to be redesigned
- the current drift is large enough that feature work should pause until that refactor starts

## Higher-Level Architecture

If the goal is maximum clarity and composability, the current implementation is not quite there yet.

The lower half is already promising:

- compiler frontend is a meaningful reusable boundary
- Lance-backed table storage is a good substrate
- manifest coordination is the right direction
- snapshots and graph indices are the right core runtime concepts

The higher half is still too collapsed into one handle.

Today, `Omnigraph` still acts like too many things at once:

- public API
- state holder
- manifest owner
- cache owner
- query entry point
- mutation entry point
- loader host

That makes the prototype convenient, but it reduces long-term clarity.

### What A Clearer Architecture Should Look Like

For long-term evolution, the system should move toward a layered shape like this:

```text
Omnigraph / Service API
  -> Session / TargetResolver
  -> CompilerFrontend
  -> GraphCoordinator
  -> QueryExecutor
  -> MutationExecutor / Loader
  -> RuntimeCache / GraphIndex
  -> TableStore (Lance)
  -> ObjectStore / Local FS
```

ASCII view with responsibilities:

```text
                         ┌───────────────────────┐
                         │   Omnigraph API       │
                         │   service / SDK / CLI │
                         │   user-facing facade  │
                         └───────────┬───────────┘
                                     │
                         ┌───────────▼───────────┐
                         │ Session / Target      │
                         │ resolves Branch or    │
                         │ Snapshot explicitly   │
                         └───────────┬───────────┘
                                     │
                  ┌──────────────────┼──────────────────┐
                  │                  │                  │
       ┌──────────▼─────────┐ ┌──────▼──────────┐ ┌─────▼────────────┐
       │ CompilerFrontend   │ │ GraphCoordinator │ │ RuntimeCache     │
       │ parse / typecheck  │ │ refs, snapshots, │ │ graph index,     │
       │ / lower to IR      │ │ commit publish   │ │ hydration caches │
       └──────────┬─────────┘ └──────┬──────────┘ └─────┬────────────┘
                  │                  │                  │
                  └──────────┬───────┴───────┬──────────┘
                             │               │
                  ┌──────────▼───────┐ ┌─────▼────────────┐
                  │ QueryExecutor    │ │ MutationExecutor │
                  │ read-only over   │ │ stage, validate, │
                  │ pinned snapshots │ │ then publish     │
                  └──────────┬───────┘ └─────┬────────────┘
                             │               │
                             └───────┬───────┘
                                     │
                         ┌───────────▼───────────┐
                         │ TableStore (Lance)    │
                         │ datasets, versions,   │
                         │ merge_insert, indices │
                         └───────────┬───────────┘
                                     │
                         ┌───────────▼───────────┐
                         │ ObjectStore / FS      │
                         │ local paths or        │
                         │ object-store URIs     │
                         └───────────────────────┘
```

Rough view of the current shape:

```text
                         ┌───────────────────────┐
                         │      Omnigraph        │
                         │ facade + state +      │
                         │ cache + coordinator   │
                         └───────┬───────┬───────┘
                                 │       │
                  ┌──────────────▼──┐ ┌──▼────────────────┐
                  │ exec/mod.rs     │ │ loader/mod.rs     │
                  │ query + mutate  │ │ ingest + validate │
                  │ + storage ops   │ │ + storage ops     │
                  └──────────────┬──┘ └──┬────────────────┘
                                 │       │
                                 └──┬────┘
                                    │
                         ┌──────────▼───────────┐
                         │ ManifestCoordinator  │
                         │ version table +      │
                         │ pinned snapshots     │
                         └──────────┬───────────┘
                                    │
                         ┌──────────▼───────────┐
                         │ Lance datasets       │
                         │ per-type tables      │
                         └──────────────────────┘
```

The main difference is simple:

- current shape: too many layers reach directly into graph state and storage
- target shape: each layer has one job and lower layers do not leak upward

### Layer Responsibilities

#### Omnigraph / Service API

This should be a thin facade.

Its job is to expose user-facing operations like:

- open
- init
- query
- mutate
- load
- branch
- merge
- snapshot

It should not directly own low-level commit logic.

#### Session / TargetResolver

This layer should resolve:

- `Target::Branch("main")`
- `Target::Snapshot(id)`

into a concrete read or write target.

This is where explicit targeting belongs.
It replaces ambient "whatever this handle currently points at" behavior.

#### CompilerFrontend

This layer owns:

- parsing
- typechecking
- IR lowering

It should stay storage-agnostic.

This is already one of the cleanest parts of the codebase.

#### GraphCoordinator

This should become the real heart of Omnigraph.

It should own:

- branch heads
- snapshot resolution
- manifest version coordination
- graph-level commit protocol
- later merge and ref behavior

This is the layer that turns "many Lance tables" into "one graph database."

#### QueryExecutor

This layer should be read-only.

It should take:

- a resolved read target
- compiled query IR
- runtime caches

and execute against pinned graph state.

It should not mutate coordinator state.

#### MutationExecutor / Loader

This layer should stage writes and validate them.

Its flow should be:

```text
resolve target
-> stage sub-table writes
-> validate graph constraints
-> ask coordinator to publish
```

It should not directly define graph visibility by itself.

#### RuntimeCache / GraphIndex

This layer should own:

- topology caches
- dense ID mappings
- hydration caches

All of these should be keyed by graph identity, not kept in one global slot.

#### TableStore

This layer should hide Lance-specific mechanics like:

- open dataset
- write batch
- merge insert
- create indices
- count rows
- checkout version

This is important for composability.
Today too much code reaches directly into dataset operations.

#### ObjectStore / Local FS

This layer should abstract:

- local paths
- object-store URIs
- schema file reads
- manifest path resolution

That is how the code becomes truly lake-native instead of only looking URI-shaped.

### What Is Not Clean Enough Today

The current implementation has several architectural smells that will make future work harder if left alone:

- `Omnigraph` is still a god object
- execution and mutation logic are mixed too closely
- loader logic knows too much about manifest and table details
- the graph cache is owned as one implicit slot instead of a target-keyed cache
- internal coordinator access leaks upward through `manifest_mut()`
- storage operations are not hidden behind a dedicated table layer

None of these mean the design is bad.
They mean the prototype boundaries are still too porous.

### Concrete Improvements

If we want the architecture to be easier to build on top of, these are the highest-value improvements:

1. Make `Omnigraph` a thin facade, not the place where everything happens.
2. Remove direct coordinator escape hatches like `manifest_mut()` from the public working surface.
3. Introduce explicit `Target` resolution for query, mutation, and load operations.
4. Turn the manifest layer into a real `GraphCoordinator`, not just a version table wrapper.
5. Add a `TableStore` layer so executors and loaders stop opening and mutating Lance datasets directly.
6. Move commit flow to a staged model: prepare, validate, publish.
7. Replace the single graph-index cache slot with target/version-keyed runtime caches.
8. Split execution into clearer read, mutation, and hydration/runtime components.

### Practical Conclusion

The current implementation is good enough as a substrate.
It is not yet the clean high-level architecture you would want to scale for years.

So the right move is not:

- rewrite everything

And also not:

- keep adding features to the current top-level shape

The right move is:

- keep the storage and compiler foundations
- refactor the coordination and API layers into cleaner boundaries
- then build branch, merge, remote, and service behavior on top

## Short Version

The current implementation is best understood as:

"a Lance-backed graph database prototype with pinned snapshots"

The target product is:

"a lake-native graph database with explicit graph targets and git-like collaboration semantics"

Those are related systems, but they are not the same architecture.
