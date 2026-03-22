# Omnigraph Specs

Distilled architecture spec for the repo-native successor product.

This document is intentionally narrower than the archived long-form design. It focuses on the product boundary, the runtime model, and the implementation constraints that matter given the current Nanograph codebase.

---

## Product Position

Omnigraph is not "Nanograph with branching."

It is a new product with:

- a **repo-native API**: branch, merge, tag, clone, push, pull
- a **repo-level storage model** over Lance
- a **different identity model** for persisted entities
- a **different runtime shape** for traversal hydration, change tracking, and remote access

Nanograph remains the embedded local-first product with a single-database API.

What carries forward unchanged or near-unchanged:

- schema DSL (`.pg`)
- query DSL (`.gq`)
- parser/typechecker/lowering pipeline
- catalog/schema IR concepts
- Arrow/Lance/DataFusion as the execution substrate

What does **not** carry forward unchanged:

- the current `Database` API as the top-level abstraction
- the current numeric storage identity assumptions
- the current manifest/WAL coordination model
- the current mutation/runtime coupling
- the current SDK surface as the canonical product API

---

## Core Outcomes

1. **Git-style graph repos**: branch, merge, tag, clone, push, pull for typed property graphs
2. **Local-first, remote-capable**: usable on a laptop, but pushable to shared object storage
3. **Fast traversal without full-row residency**: in-memory topology, on-demand property hydration
4. **Typed columnar execution**: pushdown scans over typed Lance tables
5. **Unified search**: vector, full-text, fuzzy, and hybrid search in one query language
6. **Schema-as-code**: `.pg` remains readable and versioned with data
7. **Two products, two repos**: Nanograph and Omnigraph share frontend/compiler crates but own different engines

---

## Product Boundary

### Shared Across Nanograph And Omnigraph

- schema AST/parser
- query AST/parser
- typechecker
- catalog + schema IR
- IR lowering
- common diagnostics and result transport helpers

These should live in shared crates with stable APIs.

### Nanograph-Owned

- embedded `Database` API
- current single-db manifest + WAL coordination
- current loader/mutation flows
- current SDK wrappers around `Database`
- embedded-product CLI semantics

### Omnigraph-Owned

- `Repo` API and branch/ref model
- repo metadata and manifest-table coordination
- repo-level clone/push/pull semantics
- persisted identity model for nodes/edges
- branch-aware traversal/index caches
- repo-aware CLI and SDK semantics

---

## Repository Model

Omnigraph introduces a new top-level abstraction:

```text
Repo
  -> Branch
  -> Snapshot
  -> Engine
```

Meaning:

- **Repo** owns refs, branches, tags, remotes, and manifest coordination
- **Branch** resolves a mutable line of work
- **Snapshot** resolves a read-consistent manifest version
- **Engine** executes typed queries and mutations against that snapshot

Nanograph's current `Database` object is closer to `Engine` than to `Repo`.

---

## First Implementation Slice

Omnigraph should start with a **branch-aware vertical slice**, not with a frontend rewrite.

The first prototype should cover:

- `Repo -> Branch -> Snapshot`
- branch-scoped schema and catalog loading
- pinned-snapshot read execution
- branch-local writes
- basic diff and merge behavior

Why this comes first:

- branching is the highest-leverage new architectural boundary
- it is likely to influence schema visibility rules and catalog loading
- it will define the real cache keys for runtime state
- it may force engine API changes around snapshots and write isolation
- it will tell us which frontend/compiler seams are truly shared and which are not

Expected downstream impact:

- **schema validation** may need branch-aware schema visibility, but the validation model itself should not be rewritten until the prototype proves what changes
- **catalog/schema IR** will likely need branch- and snapshot-aware loading semantics
- **execution/runtime state** will need branch/snapshot-aware cache keys
- **DataFusion planning** may need snapshot-aware table providers or execution context changes, but this should be driven by the prototype rather than assumed upfront

Assumption going in:

- `.pg` and `.gq` syntax stay stable
- parser ASTs stay stable
- most typechecking rules stay stable
- most IR lowering stays stable

Until the branching prototype proves otherwise, Omnigraph should treat those layers as shared frontend assets rather than immediate rewrite targets.

---

## Storage Architecture

### Per-Type Lance Tables Plus Manifest Table

Omnigraph stores:

```text
graph-repo/
  _schema.pg
  _manifest.lance/
  nodes/{Type}.lance/
  edges/{Type}.lance/
  _refs/branches/{name}.json
  _refs/tags/{name}.json
```

Rules:

- one Lance table per node type
- one Lance table per edge type
- one manifest Lance table records the consistent repo snapshot
- a repo version is one manifest-table version
- only changed sub-tables advance on write

This is a repo-level design, not a thin rename of Nanograph's current db directory.

### Why Per-Type Tables

- type-local schemas stay narrow
- scalar/vector/FTS indexes stay type-local
- compaction stays type-local
- hydration can stay selective
- branch/merge work scales with changed types, not total schema width

---

## Identity Model

Omnigraph uses two identities:

### Persisted Identity

- every node/edge row has a String `id`
- `@key` types use the key as `id`
- keyless entities use generated ULIDs
- edge `src` / `dst` persist String IDs

### Traversal Identity

- per-type transient dense `u32` indices
- built lazily for graph traversal
- cached per `(branch, manifest_version, edge_type)`

This is intentionally different from Nanograph's current pervasive `u64` storage identity.

---

## Query Execution

Pipeline stays:

```text
.gq -> parse -> typecheck -> lower -> execute
```

Backend changes:

- **tabular scans** go through Lance/DataFusion with pushdown
- **graph traversal** uses lazy in-memory topology indices
- **hydration** is tiered:
  - cached full-type batches for small hot types
  - Lance `WHERE id IN (...)` for larger or cold types

Execution goal:

- topology in memory
- properties on demand
- no full-row residency requirement for opening a repo

---

## Memory Model

### Always In Memory

- schema catalog
- branch/snapshot metadata
- topology indices needed for active traversals

### Lazily Cached

- small hot node types
- projected read fragments from Lance
- search/vector index state the underlying engine keeps warm

### Never Required At Open

- every node batch
- every edge property batch
- full graph hydration

Omnigraph should inherit the "Path 2" direction from Nanograph's current storage work: on-demand data, lazy topology, direct Lance writes.

---

## Change Tracking And Hooks

Change tracking should be based on:

- Lance version tracking columns
- manifest version advancement
- snapshot diff at the repo level when needed

Hooks remain application-facing orchestration, not a special storage primitive.

Trigger families:

- change
- schedule
- manual

Executor families:

- shell
- webhook

The hook system should remain agnostic about whether a shell command is an agent, a script, or a worker binary.

---

## Search

Omnigraph replaces Nanograph's remaining brute-force text search paths with Lance-native indexed search where available:

- `search()`
- `fuzzy()`
- `match_text()`
- `bm25()`
- `nearest()`
- `rrf()` remains app-level fusion over indexed retrievals

This is an execution-backend upgrade, not a DSL change.

---

## Concurrency Model

Common-case concurrency model:

- branches provide write isolation
- snapshots provide read isolation
- shared object storage provides multi-machine access

Progression:

1. optimistic branch-local writes
2. merge-based collaboration across branches
3. optional higher-throughput same-branch write mechanisms later

Omnigraph does not require a coordinator for the common local-first/shared-storage case, but the design should not preclude a future coordinator or service mode.

---

## Non-Goals For Phase 1

- preserving Nanograph's current public API unchanged
- reusing Nanograph's current storage/runtime crates as a shared engine
- rewriting parser/typechecker behavior before the branching prototype shows a need
- rewriting DataFusion planning before the branching prototype shows a need
- implementing every remote/distributed feature before the repo boundary exists
- splitting code into many crates without first defining stable architectural seams

---

## Implementation Constraints

The build order matters.

Before Omnigraph can ship cleanly:

1. a branching prototype must prove the `Repo -> Branch -> Snapshot` boundary
2. shared frontend/compiler crates should be extracted after the prototype reveals the real shared seams
3. Nanograph's on-demand storage path should be finished enough to avoid reifying old full-residency assumptions into shared crates
4. Omnigraph must introduce a first-class `Repo` API instead of extending `Database`
5. the persisted identity model must be isolated from Nanograph's current `u64` runtime assumptions

---

## Success Criteria

Omnigraph is successful when:

- the branching prototype has validated the repo/snapshot boundary
- the frontend/compiler crates are shared across products
- Nanograph and Omnigraph can evolve their engines independently
- Omnigraph exposes repo-native operations without leaking Nanograph's current `Database` semantics
- the product split is clear enough to justify two separate repositories
