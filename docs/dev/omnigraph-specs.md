# Omnigraph Specs

Architecture specification for the Lance-native graph database.

---

## Product Position

Omnigraph is not "Nanograph with branching."

It is a new product with:

- a **Lance-native storage model**: manifest as a Lance table, per-type Lance datasets, stable row IDs, shallow clone branching — no custom WAL, no custom JSON manifest, no custom CDC
- a **git-style API**: branch, merge, tag, clone, push, pull
- a **different identity model**: String IDs (`@key` values or ULIDs), no `u64` counters, no cross-branch ID coordination
- a **different runtime shape**: lazy topology indices, tiered hydration, on-demand property access, URI-based open for local and remote repos

Nanograph remains the embedded local-first product with a single-database API.

What carries forward unchanged:

- schema DSL (`.pg`)
- query DSL (`.gq`)
- parser/typechecker/lowering pipeline
- catalog/schema IR concepts
- Arrow type system

What does **not** carry forward:

- the `Database` API and `GraphStorage` in-memory model
- the `u64` numeric storage identity
- the `graph.manifest.json` / `_wal.jsonl` coordination model
- the custom DataFusion `ExecutionPlan` operators
- the brute-force search implementations
- the full-graph-in-memory assumption

---

## Core Outcomes

1. **Git-style graph database**: branch, merge, tag, clone, push, pull for typed property graphs
2. **Local-first, remote-native**: works on a laptop, works on S3 — same API, same URI
3. **Fast traversal without full-row residency**: in-memory topology, on-demand property hydration
4. **Typed columnar execution**: DataFusion + LanceTableProvider with pushdown scans over per-type Lance tables
5. **Indexed search**: Lance FTS, n-gram, and vector indexes replace brute-force in-memory search
6. **Schema-as-code**: `.pg` remains readable and versioned with data
7. **Two products, shared compiler**: Nanograph and Omnigraph share the `omnigraph-compiler` crate but own different storage/execution layers

---

## Product Boundary

### Shared: `omnigraph-compiler`

- schema AST/parser (`.pg`)
- query AST/parser (`.gq`)
- typechecker
- catalog + schema IR
- IR lowering
- type system (ScalarType, PropType, Direction)
- error types + diagnostics
- result transport (QueryResult, MutationResult, Arrow → JSON)
- embedding client
- query input helpers

**Zero Lance dependency.** This crate compiles fast, tests in milliseconds, and is the future shared boundary with Nanograph. When the time comes to extract `graph-schema`/`graph-query`/`graph-catalog`/`graph-ir`, they split out of this one crate.

### Nanograph-Owned

- embedded `Database` API
- single-db manifest + WAL coordination
- `GraphStorage` in-memory model
- current loader/mutation flows
- SDK wrappers around `Database`

### Omnigraph-Owned: `omnigraph`

- `Omnigraph` top-level API (not "Repo" — it's a database)
- `ManifestCoordinator` — Lance table for cross-dataset coordination
- Lance-native branching, tagging, shallow clone
- `QueryExecutor` — DataFusion SessionContext + LanceTableProvider
- `GraphIndexManager` — lazy CSR/CSC with dense u32 indices
- JSONL loader targeting per-type Lance tables
- CLI and SDK semantics

---

## Top-Level Abstraction

```text
Omnigraph (handle — one per connection)
  ├── Catalog              (immutable, parsed from _schema.pg)
  ├── ManifestCoordinator  (manages writes, holds Lance dataset handle)
  │
  ├── snapshot() → Snapshot   (immutable read view at a manifest version)
  └── commit(writes) → u64   (atomic write, advances manifest version)

Snapshot (immutable read view)
  ├── version: u64                          (manifest version)
  ├── entries: {key → (path, version)}      (pinned sub-table versions)
  └── open("node:Person") → Dataset        (at pinned version)
```

- **`Omnigraph`** is the single entry point: init, open, load, query, branch, merge, tag, warm
- **`Snapshot`** is an immutable point-in-time view — the unit of read consistency. All reads within a query go through one Snapshot.
- **`ManifestCoordinator`** owns the manifest Lance table, sub-table lifecycle, and commit protocol
- **`Catalog`** is built from `_schema.pg` by the compiler — no storage dependency

```rust
let db = Omnigraph::open("s3://bucket/my-graph").await?;   // remote
let db = Omnigraph::open("/local/my-graph").await?;         // local
let db = Omnigraph::init(uri, schema_source).await?;
db.load(data_path, LoadMode::Merge).await?;

let snapshot = db.snapshot().await?;         // pinned read view
db.run_query(query_source, "friends_of", &params).await?;  // takes snapshot internally

db.branch_create("experiment").await?;
db.refresh().await?;                         // see other writers' commits
db.warm("main").await?;
```

All methods accept `&str` URIs, not filesystem paths. Lance handles local and remote storage transparently.

---

## Crate Structure

```
crates/
├── omnigraph-compiler/        # No Lance dependency. Fast to build and test.
│   └── src/
│       ├── schema/            # .pg parser, AST
│       ├── query/             # .gq parser, AST, typechecker
│       ├── catalog/           # Catalog, schema IR
│       ├── ir/                # IR types, lowering
│       ├── types.rs           # ScalarType, PropType, Direction
│       ├── error.rs           # NanoError, ParseDiagnostic
│       ├── embedding.rs       # Embedding client
│       ├── json_output.rs     # Arrow → JSON
│       ├── result.rs          # QueryResult, MutationResult
│       └── query_input.rs     # Named query lookup, param parsing
│
├── omnigraph/                 # The database. Lance-dependent.
│   └── src/
│       ├── db/                # Omnigraph, ManifestCoordinator, branching, tagging
│       ├── exec/              # QueryExecutor, scan, traverse, search, mutate, filter
│       ├── graph_index/       # CSR/CSC, TypeIndex, tiered node cache
│       └── loader/            # JSONL → per-type Lance tables, constraints
│
└── omnigraph-cli/             # Binary
    └── src/
        └── main.rs
```

The boundary is the **Lance dependency line**. Everything above it (compiler) has zero Lance dependency and compiles in seconds. Everything below it (database) depends on Lance, DataFusion, and Arrow. No circular dependencies. No artificial engine/repo split.

---

## Storage Architecture

### Layout

```text
graph-db/                               # repo root (local path or s3:// URI)
├── _schema.pg                          # Schema source of truth
├── _manifest.lance/                    # Lance table: one row per sub-table
│   ├── _versions/                      # MVCC — repo version = manifest version
│   ├── _refs/branches/                 # Lance-native branch metadata
│   ├── _refs/tags/                     # Lance-native tag metadata
│   └── tree/{branch}/                  # Branch-specific manifest versions
├── nodes/
│   ├── a1b2c3d4/                       # Per-type dataset. Hash of type name at creation.
│   │   ├── _versions/
│   │   ├── data/
│   │   ├── _indices/                   # BTree on id, FTS on @index properties
│   │   └── tree/{branch}/             # Lazily created on first branch write
│   └── e5f6a7b8/
└── edges/
    ├── f7012952/
    └── 3c4d5e6f/
```

Sub-table directory names are stable hashes of the type name at creation time. On type rename, only the manifest row's `table_key` changes — the directory and all data files are untouched. This avoids directory renames (impossible on S3) and keeps paths stable across schema evolution.

### Manifest Table

The manifest is a Lance table. One row per sub-table. A repo version is one manifest version.

| Column | Type | Description |
|---|---|---|
| `table_key` | Utf8 | `"node:Person"` or `"edge:Knows"`. Unenforced primary key. |
| `table_path` | Utf8 | `"nodes/a1b2c3d4"` — relative to repo root, stable hash |
| `table_version` | UInt64 | Pinned Lance version for this snapshot |
| `table_branch` | Utf8 (nullable) | Lance branch name on sub-table (null = main) |
| `row_count` | UInt64 | Rows in sub-table at this version |

Branching = branching the manifest table. Tagging = tagging the manifest table. Time travel = checking out a prior manifest version.

### Sub-Table Schemas

**Nodes:**

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | `@key` value or ULID. Unenforced primary key. BTree index. |
| `{property}` | typed | One column per `.pg` property |

**Edges:**

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | ULID. Unenforced primary key. |
| `src` | Utf8 | Source node `id`. BTree index. |
| `dst` | Utf8 | Destination node `id`. BTree index. |
| `{property}` | typed | Edge properties |

All datasets created with `enable_stable_row_ids = true` and `LanceFileVersion::V2_2`.

### Commit Protocol

Writes to multiple sub-tables are coordinated through the manifest:

```
1. (parallel) write_fragments to each changed sub-table
2. Commit each sub-table (LanceOperation.Append / MergeInsert / Delete)
3. merge_insert on _manifest.lance with updated versions
   → new manifest version = new repo version (atomic commit point)
```

If any step before 3 fails, the manifest still points to old versions. Orphaned sub-table versions are invisible to readers and cleaned up by compaction.

### Why Per-Type Tables

- type-local schemas stay narrow (~5-15 columns, no null waste)
- scalar/vector/FTS indexes cover only relevant data
- compaction is type-local
- hydration is selective (only load types touched by query)
- branch/merge scales with changed types, not total schema width
- each sub-table is an independent Lance dataset with full MVCC

---

## Identity Model

### Persisted Identity

- every node/edge row has a String `id` column
- `@key` types use the key property value as `id`
- keyless types use generated ULIDs (no coordinator, branch-safe)
- edge `src` / `dst` persist String IDs of referenced nodes
- `id` has `unenforced_primary_key` for efficient `merge_insert`

### Traversal Identity

- per-type transient dense `u32` indices (not `u64`)
- `TypeIndex`: `HashMap<String, u32>` + `Vec<String>` per node type
- built lazily alongside the graph index on first traversal query
- cached per `(branch, manifest_version)`
- offset arrays sized to actual node count per type — no waste

---

## Query Execution

Pipeline stays:

```text
.gq → parse → typecheck → lower → execute
```

All reads within a query go through a single `Snapshot` (see Consistency Model).

Backend changes from custom DataFusion operators to Lance-native scans:

- **tabular scans**: IR `NodeScan` → Lance `Scanner` with SQL filter pushdown. Filters are converted from IR to Lance SQL strings (`name = 'Alice' AND age > 30`). Type casting is automatic (e.g., Int64 literal against Int32 column).
- **graph traversal**: lazy in-memory CSR/CSC topology indices with dense u32. BFS/DFS is sub-millisecond.
- **hydration** is tiered:
  - `NodeCache::Full` — cached RecordBatch for small types (< 100K rows). Sub-millisecond take.
  - `NodeCache::OnDemand` — Lance `WHERE id IN (...)` for large types. Single-digit ms via BTree.
- **projection**: IR `PropAccess` expressions map to Arrow column extraction. Aliases applied from return clause.
- **ordering**: Arrow `lexsort_to_indices` with `SortColumn` per ordering expression.
- **limit**: batch slicing after ordering.
- **search predicates**: Lance FTS (`contains_tokens`, `phrase`, `match` with BM25), n-gram for fuzzy, IVF-HNSW for vector. All indexed.
- **mutations**: Lance native `merge_insert`, `append`, `delete` (deletion vectors). Schema-driven edge cascade.

---

## Memory Model

### Always In Memory

- schema catalog (from `_schema.pg`, small)
- manifest state (current sub-table versions)
- graph index: CSR/CSC offset arrays + TypeIndex mappings for active branch

### Lazily Cached

- `NodeCache::Full` for small hot node types (< 100K rows)
- Lance fragment cache on local disk (transparent, configurable)
- Lance metadata/index cache (shared Session)

### Never Required At Open

- every node batch
- every edge property batch
- full graph hydration
- graph index for branches not yet queried

### Tiered Storage Model

| Tier | Latency | What lives here | Owner |
|---|---|---|---|
| Memory | < 1 us | CSR/CSC, TypeIndex, NodeCache::Full, manifest state, catalog | `Omnigraph` struct |
| Local disk | < 1 ms | Lance fragment cache, Lance metadata cache | Lance SDK |
| Object storage | 10-100 ms | Lance data fragments, manifests, indices | Lance SDK |

The `warm(branch)` method pre-populates all tiers for a branch: manifest state → edge topology → small node caches.

---

## Branching Model

### Lance-Native Branching

Branches are Lance branches on the manifest table. Sub-tables are branched lazily.

**Create branch:**
1. Create Lance branch on `_manifest.lance` (zero-copy — inherits all rows)
2. No sub-tables branched yet

**First write on branch:**
1. Sub-table has no branch → create Lance branch (shallow clone via `base_paths`)
2. Write to the branched sub-table → new fragments at the branch's dataset root
3. Update manifest row: set `table_branch`, commit manifest on the branch

**Read from branch:**
1. Open manifest at the branch
2. For each sub-table: if `table_branch` is null, open at `table_version` on main. If set, open at that branch.

Sub-tables not written to on a branch are never branched. Storage overhead is proportional to what changed.

### Shallow Clone via `base_paths`

Lance's `base_paths` system handles cross-location data resolution at the fragment level:

- Inherited fragments: `DataFile { path: "fragment-0.lance", base_id: 1 }` → resolves to source
- New fragments: `DataFile { path: "fragment-new.lance", base_id: 0 }` → lives at clone root
- Reads see both seamlessly. Writes always go to the clone's own root.
- Source dataset is never modified.

This applies to both branching (within a repo) and cloning (across locations).

### Merge

1. Read source and target manifest states
2. For each sub-table branched on source:
   - Diff using `_row_created_at_version` / `_row_last_updated_at_version`
   - Apply changes to target via `merge_insert` keyed by `id`
3. Update target manifest with new sub-table versions

---

## Local/Remote Model

Omnigraph works identically on local disk and remote storage. `Omnigraph::open(uri)` accepts local paths and `s3://`/`gs://` URIs — Lance handles both transparently.

### Single Location (Simplest)

Repo lives at one location (local or S3). All clients open it directly. Branches provide write isolation. No sync needed.

### Clone + Push/Pull (Hub Model)

**Clone:** Create a local repo that references the remote via `base_paths`. Metadata is local. Data stays remote, fetched on demand.

**Work locally:** Writes create new fragments at the local root. Inherited remote fragments are read transparently via `base_paths`.

**Push:** Copy local-only fragments to hub. Commit new hub manifest version including them.

**Pull:** Fetch remote manifest. Update local manifest to reference new remote versions.

Per-type tables mean only changed types need to sync — O(changed types), not O(all types).

### Design Constraints for Remote

- All methods accept `&str` URIs, not `&Path`
- Sub-table paths in the manifest are relative to repo root
- `base_paths` handles cross-location fragment resolution (Lance-native, not application code)
- The graph index is always local/ephemeral — rebuilt from whatever sub-tables are accessible
- Manifest metadata stores remote origin: `"omnigraph:remote:origin" → "s3://..."`

---

## Change Tracking

Based entirely on Lance-native capabilities:

- `_row_created_at_version` — which manifest version a row was inserted
- `_row_last_updated_at_version` — which manifest version a row was last modified
- Deletion vectors — track deleted rows per fragment

Change queries are standard filters:

```sql
-- Inserts since version N
WHERE _row_created_at_version > N AND _row_created_at_version <= current

-- Updates since version N (not newly created)
WHERE _row_created_at_version <= N
  AND _row_last_updated_at_version > N
  AND _row_last_updated_at_version <= current
```

Cross-dataset change detection: compare manifest versions to find which sub-tables changed, then query each.

No custom WAL. No custom CDC log. No inline JSON payloads.

### Hooks

Trigger families: change, schedule, manual.
Executor families: shell, webhook.

Hooks observe manifest version changes and dispatch. They are a thin layer on top of Lance's version tracking — not a separate change infrastructure.

---

## Search

Lance-native indexed search replaces Nanograph's brute-force implementations:

| Predicate | Nanograph (brute-force) | Omnigraph (Lance-indexed) |
|---|---|---|
| `search()` | O(rows × tokens) tokenize + set intersection | FTS `contains_tokens` on indexed column |
| `fuzzy()` | O(rows × tokens²) Levenshtein per pair | N-gram FTS index |
| `match_text()` | O(rows × tokens) sliding window | FTS `phrase` query |
| `bm25()` | O(rows × tokens) BM25 per row | FTS `match` with BM25 scoring |
| `nearest()` | Lance ANN (already indexed) | Lance IVF-HNSW (keep) |
| `rrf()` | App-level fusion | App-level fusion (keep) |

This is an execution-backend upgrade, not a DSL change.

---

## Consistency Model

### Core Principle

**A manifest version IS a database version.** One manifest version pins a consistent set of sub-table versions. That pinned set is an immutable `Snapshot` — the unit of read consistency.

### Snapshot Isolation

A `Snapshot` is produced by reading the manifest table at a specific version. It contains the `table_version` for every sub-table at that point in time. All reads through one Snapshot see a consistent cross-type view, regardless of concurrent writes.

```rust
pub struct Snapshot {
    version: u64,
    entries: HashMap<String, SubTableEntry>,
}

impl Snapshot {
    pub async fn open(&self, table_key: &str) -> Result<Dataset> {
        // Opens sub-table at the version pinned by this snapshot.
        // Lance MVCC guarantees the read is isolated from concurrent writes.
    }
}
```

**Within a query:** A single Snapshot is taken at query start. All NodeScan, Expand, and AntiJoin operations read through it. No matter what writers do during the query, the reads are consistent.

**Across queries:** Each query takes its own Snapshot (the latest manifest version). Successive queries see the latest committed state.

### Multi-Reader

Multiple readers call `snapshot()` concurrently — each gets an immutable view. Snapshots are `Clone + Send + Sync`. Readers never block writers. This is Lance MVCC for free.

### Multi-Writer

Multiple writers each open their own `Omnigraph::open(uri)` handle, getting their own `ManifestCoordinator` with their own Lance `Dataset` handle. When two writers commit:

- **Different sub-tables** (e.g., one writes Person, other writes Company): Lance merges the manifest rows automatically. The `merge_insert` by `table_key` is non-conflicting for different keys.
- **Same sub-table**: Lance's per-dataset optimistic concurrency handles it. Append+Append is rebasable (auto-merged). Same-row Update+Update is retryable.
- **No application-level locking required.**

### Read-After-Write

After `commit()`, the coordinator's manifest dataset handle is updated. The next `snapshot()` sees the new version immediately.

### Cross-Writer Visibility

Writer A commits. Writer B calls `refresh()` (re-opens the manifest at the latest version). Writer B's next `snapshot()` sees A's changes. Without `refresh()`, Writer B continues reading from its last known version — stale but consistent.

### What Lance Provides Natively

| Capability | Lance mechanism |
|---|---|
| Snapshot isolation | Immutable versioned manifests in `_versions/` |
| Atomic commits | `put-if-not-exists` or `rename-if-not-exists` on manifest file |
| Conflict detection | Transaction files in `_transactions/` with compatibility matrix |
| Conflict resolution | Rebasable (auto-merge), retryable (re-execute), or incompatible (fail) |
| Time travel | `checkout_version(N)` reads any historical version |
| Deletion without rewrite | Deletion vectors (soft delete) |
| Row-level change tracking | `_row_created_at_version`, `_row_last_updated_at_version` (with stable row IDs) |

### What Omnigraph Adds

The manifest table is the single coordination layer that extends Lance's per-dataset MVCC to cross-dataset consistency. Without it, each sub-table has independent versioning with no guarantee that Person version 5 and Knows version 3 form a consistent graph. The manifest pins them together: manifest version N says "Person is at version 5 and Knows is at version 3" — that's the Snapshot.

---

## Concurrency Model

Branches are the primary concurrency primitive. Progression:

| Phase | Workload | Mechanism |
|---|---|---|
| Local/agent | Single writer, batch loads | Lance `merge_insert` per sub-table + manifest commit |
| Multi-agent | Independent work streams | Branch-per-writer + merge (zero-copy fork, isolated writes) |
| Collaboration | Same branch, low contention | Lance optimistic concurrency (Append+Append = rebasable, same-row Update = retryable) |
| Streaming | High throughput on one branch | Lance MemWAL per sub-table (region-based, epoch fencing) |

Omnigraph does not require a coordinator for the common case. The manifest table's MVCC handles concurrent access. MemWAL is only needed when write throughput exceeds what single-writer commits can sustain AND writes must land on the same branch.

---

## Non-Goals For Phase 1

- preserving Nanograph's public API
- reusing Nanograph's storage/runtime crates
- implementing push/pull before local branching works
- MemWAL or high-throughput concurrency
- schema evolution / migration (deferred — Lance `add_columns`/`alter_columns` when needed)
- DuckDB fallback for very large graphs
- SDK wrappers (TS, Swift, Python)

---

## Implementation Constraints

Build order:

1. Foundation types (String IDs, Utf8 schemas) — unblocks everything
2. Manifest coordinator (Lance table) — everything depends on it
3. `Omnigraph` init/open — the entry point
4. Data loading — JSONL → per-type Lance tables via manifest
5. Query execution — DataFusion SessionContext + LanceTableProvider
6. Graph index — lazy CSR/CSC with dense u32, tiered node cache
7. Mutations — insert/update/delete with cascade via manifest
8. Search — Lance FTS/n-gram/ANN indexes
9. Branching — Lance-native on manifest table, lazy sub-table branching
10. Change tracking + CLI polish

Steps 4, 5, 7 can parallel after step 3. Step 9 can parallel with 4-8.

Critical path: 1 → 2 → 3 → 5 → 6 (types → manifest → open → query → traversal).

---

## Success Criteria

Omnigraph is successful when:

- `omnigraph init` creates a repo with per-type Lance tables, manifest, and stable row IDs
- `omnigraph load` writes JSONL to per-type tables and commits the manifest atomically
- `omnigraph run` executes queries with filter pushdown, graph traversal, and indexed search
- `omnigraph branch create` is O(1) regardless of data size (manifest branch only)
- `omnigraph branch merge` applies net changes proportional to what changed
- the same URI works for local and S3 repos
- no custom WAL, no custom JSON manifest, no custom CDC — Lance-native throughout
- the compiler crate has zero Lance dependency and can be shared with Nanograph
