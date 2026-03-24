# Omnigraph Specs

Architecture specification for the Lance-native graph database.

---

## Product Position

Omnigraph is not "Nanograph with branching."

It is a new product with:

- a **Lance-native storage model**: manifest as a Lance table, per-type Lance datasets, stable row IDs, shallow clone branching — no custom WAL, no custom JSON manifest, no custom CDC
- a **git-style API**: branch, merge, tag, clone, push, pull
- a **different identity model**: String IDs (`key()` values or ULIDs), no `u64` counters, no cross-branch ID coordination
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
4. **Typed columnar execution**: Lance Scanner with SQL filter pushdown over per-type Lance tables. BTree/Inverted/Vector indices from schema constraints.
5. **Indexed search**: Lance FTS, n-gram, and vector indexes replace brute-force in-memory search
6. **Schema-as-code**: `.pg` remains readable and versioned with data
7. **Two products, shared compiler**: Nanograph and Omnigraph share the `omnigraph-compiler` crate but own different storage/execution layers

---

## Product Boundary

### Shared: `omnigraph-compiler`

- schema AST/parser (`.pg`) with first-class constraint system (`key`, `unique`, `index`, `range`, `check`)
- edge cardinality model (`@card(min..max)`)
- property derivations (`= embed(source)`)
- query AST/parser (`.gq`)
- typechecker (read queries + mutations)
- catalog builder
- IR lowering (QueryIR + MutationIR)
- type system (ScalarType, PropType, Direction)
- error types + diagnostics
- result transport (QueryResult, MutationResult, RunResult, Arrow → JSON)
- embedding client
- query input helpers + `params!` macro

**Zero Lance dependency.** This crate compiles fast, tests in milliseconds, and is the future shared boundary with Nanograph.

### Omnigraph-Owned: `omnigraph`

- `Omnigraph` top-level handle
- `Snapshot` — immutable point-in-time view
- `ManifestCoordinator` — Lance table for cross-dataset coordination
- `GraphIndex` — lazy CSR/CSC with dense u32 indices
- `execute_query()` — pure function executor
- JSONL loader targeting per-type Lance tables with constraint validation
- `ensure_indices()` — BTree, Inverted, Vector index creation from schema constraints
- Lance-native branching, tagging, shallow clone (planned)
- CLI (planned)

---

## Schema Language

The `.pg` schema defines typed nodes, edges, constraints, and metadata. Constraints are first-class grammar elements — not annotations. Annotations are reserved for metadata only.

### Type Declarations

```
node TypeName @description("...") {
    prop: Type                               // required
    prop: Type?                              // nullable

    key(prop)                                // identity constraint
    unique(p1, p2)                           // uniqueness (composite ok)
    index(p1, p2)                            // index hint (composite ok)
    range(prop, 0..100)                      // value bound
    check(prop, "[A-Z]{3}-[0-9]+")           // regex pattern
}

edge TypeName: From -> To @card(0..1) {
    prop: Type?

    unique(src, dst)                         // prevent duplicate edges
    index(prop)                              // edge property index
}
```

### Constraints

First-class grammar elements inside type bodies. NOT annotations.

| Constraint | Syntax | Targets | Composites | Effect |
|---|---|---|---|---|
| `key` | `key(prop)` | nodes | no (single) | Value becomes `id` column, implies index, at most 1 per type |
| `unique` | `unique(p1, ...)` | nodes, edges | yes | Enforce uniqueness on column tuple |
| `index` | `index(p1, ...)` | nodes, edges | yes | BTree (scalar), Inverted (String), IVF-HNSW (Vector) |
| `range` | `range(prop, min..max)` | nodes | no | Validate value within bounds at load/mutation |
| `check` | `check(prop, "regex")` | nodes | no | Validate value matches pattern at load/mutation |

Enforcement is strict: violations are hard errors at load and mutation time.

### Edge Cardinality

```
edge WorksAt: Person -> Company @card(0..1)      // at most one employer
edge Knows: Person -> Person                      // default: @card(0..)
edge Authored: Person -> Paper @card(1..)         // at least one author required
```

Enforced at: loader (post-write validation), mutation insert (max bound check), mutation delete (min bound check).

### Derivations

Property derivation replaces the former `@embed` annotation:

```
node Document {
    body: String
    embedding: Vector(1536) = embed(body)
}
```

The `= embed(source_property)` syntax makes the derivation relationship explicit. Derivations are validated at parse time: target must be `Vector`, source must be `String`, source property must exist.

### Annotations (Metadata Only)

After the constraint restructuring, annotations are exclusively for metadata that does not affect storage or runtime behavior:

- `@description("...")` — human-readable documentation on types and properties
- `@instruction("...")` — agent/LLM guidance on types
- `@rename_from("...")` — schema evolution hint

### Example Schema

```
node Person @description("A person in the graph") {
    name: String
    email: String
    age: I32?
    bio: String?
    embedding: Vector(384) = embed(bio)

    key(name)
    unique(email)
    index(name, age)
    range(age, 0..200)
}

node Company @description("An organization") {
    name: String

    key(name)
}

edge WorksAt: Person -> Company @card(0..1) {
    since: Date?

    index(since)
}

edge Knows: Person -> Person {
    unique(src, dst)
}
```

---

## Top-Level Abstraction

Three types with separate lifecycles:

```text
Omnigraph (handle — one per connection, long-lived)
  ├── catalog: Catalog                              (immutable, from _schema.pg)
  ├── manifest: ManifestCoordinator                 (manages writes, known version)
  ├── cached_graph_index: Option<Arc<GraphIndex>>   (cached topology, single slot)
  │
  ├── snapshot() → Snapshot        (sync, no I/O — reads from known state)
  ├── refresh()                    (async — re-reads manifest from storage)
  ├── graph_index()                (async — cache check, maybe rebuild)
  ├── run_query()                  (async — snapshot + compile + execute)
  └── ensure_indices()             (async — create BTree indices)

Snapshot (immutable read view, per-query, cheap)
  ├── version: u64
  ├── entries: HashMap<String, SubTableEntry>
  └── open(key) → Dataset at pinned version

GraphIndex (topology only, expensive to build, shared via Arc)
  ├── type_indices: HashMap<String, TypeIndex>   (String → u32 dense mapping)
  ├── csr: HashMap<String, CsrIndex>             (outgoing adjacency)
  └── csc: HashMap<String, CsrIndex>             (incoming adjacency)
```

**Actual struct** (`crates/omnigraph/src/db/omnigraph.rs`):
```rust
pub struct Omnigraph {
    root_uri: String,
    manifest: ManifestCoordinator,
    catalog: Catalog,
    schema_source: String,
    cached_graph_index: Option<Arc<GraphIndex>>,
}
```

**Actual API** (`crates/omnigraph/src/db/omnigraph.rs` + `crates/omnigraph/src/exec/mod.rs`):
```rust
impl Omnigraph {
    pub async fn init(uri: &str, schema_source: &str) -> Result<Self>;
    pub async fn open(uri: &str) -> Result<Self>;
    pub fn snapshot(&self) -> Snapshot;                      // sync, no I/O
    pub async fn refresh(&mut self) -> Result<()>;           // re-read from storage
    pub async fn graph_index(&mut self) -> Result<Arc<GraphIndex>>;
    pub async fn run_query(&mut self, source: &str, name: &str, params: &ParamMap) -> Result<QueryResult>;
    pub async fn ensure_indices(&self) -> Result<()>;        // BTree/Inverted/Vector from schema constraints
    pub fn catalog(&self) -> &Catalog;
    pub fn uri(&self) -> &str;
    pub fn version(&self) -> u64;
    pub fn manifest_mut(&mut self) -> &mut ManifestCoordinator;
}
```

### Query Path (Actual)

```rust
// crates/omnigraph/src/exec/mod.rs
impl Omnigraph {
    pub async fn run_query(&mut self, query_source: &str, query_name: &str, params: &ParamMap) -> Result<QueryResult> {
        let snapshot = self.snapshot();                           // 1. sync, no I/O
        let query_decl = find_named_query(query_source, query_name)?;
        let type_ctx = typecheck_query(self.catalog(), &query_decl)?;
        let ir = lower_query(self.catalog(), &query_decl, &type_ctx)?;

        let needs_graph = ir.pipeline.iter().any(|op| matches!(op, IROp::Expand { .. } | IROp::AntiJoin { .. }));
        let graph_index = if needs_graph { Some(self.graph_index().await?) } else { None };

        execute_query(&ir, params, &snapshot, graph_index.as_deref(), self.catalog()).await
    }
}
```

The executor `execute_query()` is a **public pure function** — no state, no caches, no interior mutability.

### Version Advancement

- **`open()`** — reads manifest from storage, sets known version
- **`commit()`** — advances manifest, updates known version (read-your-own-writes)
- **`refresh()`** — re-reads manifest from storage, updates known version (see other writers)
- **`snapshot()`** — returns Snapshot at the known version (sync, no storage I/O)

### Graph Index Invalidation

The graph index is a single cache slot: `Option<Arc<GraphIndex>>`. It is:
- **Built lazily** on first traversal query via `graph_index()`
- **Shared** across queries via `Arc`
- **Invalidated** by setting to `None` after edge mutations (insert edge, delete node with cascade, delete edge)

Phase 1 uses simple invalidation. Future: key by edge sub-table versions (`BTreeMap<String, u64>`) so node-only loads don't trigger rebuilds.

### Extension Points

Every future optimization is additive — a new field on `Omnigraph` or a new parameter to the executor, not a restructuring:

- Multi-branch → `HashMap<GraphIndexKey, Arc<GraphIndex>>` replaces `Option`
- Node cache → new field on Omnigraph, new parameter to executor
- DuckDB fallback → new field, new parameter
- Persistent index → serialize/deserialize `GraphIndex` (it's just data)

---

## Crate Structure

```
crates/
├── omnigraph-compiler/        # No Lance dependency. 149+ tests.
│   └── src/
│       ├── schema/            # .pg parser, AST, pest grammar, Constraint/Derivation/Cardinality types
│       ├── query/             # .gq parser, AST, typechecker, pest grammar
│       ├── catalog/           # Catalog, build_catalog (key, unique, index, range, check, cardinality)
│       ├── ir/                # QueryIR, MutationIR, lowering
│       ├── types.rs           # ScalarType, PropType, Direction
│       ├── error.rs           # NanoError, ParseDiagnostic
│       ├── embedding.rs       # OpenAI embedding client
│       ├── json_output.rs     # Arrow → JSON (JS-safe integers)
│       ├── result.rs          # QueryResult, MutationResult, RunResult
│       └── query_input.rs     # Named query lookup, param parsing, params! macro
│
├── omnigraph/                 # The database. Lance-dependent. 60+ tests.
│   └── src/
│       ├── db/
│       │   ├── manifest.rs    # Snapshot, ManifestCoordinator (known_state, snapshot, refresh, commit)
│       │   └── omnigraph.rs   # Omnigraph handle (init, open, run_query, run_mutation, graph_index, ensure_indices)
│       ├── exec/
│       │   └── mod.rs         # execute_query, execute_mutation, execute_expand, execute_anti_join, hydrate_nodes
│       ├── graph_index/
│       │   └── mod.rs         # GraphIndex, TypeIndex (String↔u32), CsrIndex (CSR adjacency)
│       └── loader/
│           └── mod.rs         # load_jsonl, load_jsonl_file, build_node_batch, validate_constraints
│
└── omnigraph-cli/             # Binary (stubbed, wired in Step 10)
    └── src/
        └── main.rs
```

The boundary is the **Lance dependency line**. Everything above it (compiler) has zero Lance dependency and compiles in seconds. Everything below it (database) depends on Lance and Arrow.

---

## Storage Architecture

### Layout

```text
graph-db/                               # repo root (local path or s3:// URI)
├── _schema.pg                          # Schema source of truth
├── _manifest.lance/                    # Lance table: one row per sub-table
│   ├── _versions/                      # MVCC — repo version = manifest version
│   ├── _refs/branches/                 # Lance-native branch metadata
│   └── data/
├── nodes/
│   ├── {hash}/                         # Per-type dataset (FNV-1a hash of type name)
│   │   ├── _versions/
│   │   ├── data/
│   │   └── _indices/                   # BTree on id (created by ensure_indices)
│   └── ...
└── edges/
    ├── {hash}/                         # Per-edge-type dataset
    │   ├── _versions/
    │   ├── data/
    │   └── _indices/                   # BTree on src, BTree on dst
    └── ...
```

Sub-table directory names are FNV-1a hashes of the type name (`manifest.rs:type_name_hash`). On type rename, only the manifest row's `table_key` changes — directories are stable.

### Manifest Table

The manifest is a Lance table. One row per sub-table. A repo version is one manifest version.

| Column | Type | Description |
|---|---|---|
| `table_key` | Utf8 | `"node:Person"` or `"edge:Knows"` |
| `table_path` | Utf8 | `"nodes/a1b2c3d4"` — relative to repo root |
| `table_version` | UInt64 | Pinned Lance version for this snapshot |
| `table_branch` | Utf8 (nullable) | Lance branch name on sub-table (null = main) |
| `row_count` | UInt64 | Rows in sub-table at this version |

**Commit protocol** (`ManifestCoordinator::commit`):
1. Read current state to resolve `table_path` for each key
2. Build update batch
3. `merge_insert` on `_manifest.lance` keyed by `table_key` (atomic commit point)
4. Update `known_state` for read-your-own-writes

### Sub-Table Schemas

**Nodes** (from `Catalog::node_types[name].arrow_schema`):

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | `key()` value or ULID. BTree index. |
| `{property}` | typed | One column per `.pg` property |

**Edges** (from `Catalog::edge_types[name].arrow_schema`):

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | ULID. |
| `src` | Utf8 | Source node `id`. BTree index. |
| `dst` | Utf8 | Destination node `id`. BTree index. |
| `{property}` | typed | Edge properties |

All datasets created with `enable_stable_row_ids = true` and `LanceFileVersion::V2_2`.

### Indices

`Omnigraph::ensure_indices()` creates indices after data load.

**Always created (structural):**
- Node tables: BTree on `id`
- Edge tables: BTree on `src`, BTree on `dst`

**From schema constraints (`index(...)`):**
- Scalar properties → `IndexType::BTree`
- String properties → `IndexType::Inverted` (for FTS)
- Vector properties → `IndexType::IvfHnswPq` (for ANN)
- Composite indices: `index(p1, p2)` → multi-column BTree
- Edge property indices: `index(since)` inside edge body → BTree on edge property columns

Uses `lance_index::DatasetIndexExt::create_index_builder`. Idempotent (`replace=true`).

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

- every node/edge row has a String `id` column (Utf8)
- `key()` types use the key property value as `id` (e.g., `key(name)` → `id = "Alice"`)
- keyless types use generated ULIDs (`ulid::Ulid::new().to_string()`)
- edge `src` / `dst` persist String IDs of referenced nodes
- `id` has BTree scalar index for efficient lookups and `merge_insert`

### Traversal Identity

- per-type transient dense `u32` indices
- `TypeIndex`: `HashMap<String, u32>` (id → dense) + `Vec<String>` (dense → id)
- built lazily during `GraphIndex::build` from edge `src`/`dst` columns
- two-phase build: scan all edge types first (grows TypeIndices), then build CSR/CSC with final sizes
- invalidated when `cached_graph_index` is set to `None`

### Future: Binary ULIDs

The current design stores all IDs as Utf8 strings — 26-byte Crockford Base32 for ULIDs, variable-length for `key()` values. This prioritizes simplicity and debuggability at v0.1.0 but has known performance costs at scale:

- **Space:** Each edge row carries 78+ bytes of string IDs (`id` + `src` + `dst`). Binary ULIDs (`FixedSizeBinary(16)`) would cut this to 48 bytes — a 38% reduction on the ID columns.
- **Speed:** String hashing/comparison for `TypeIndex` build, `merge_insert` keyed by `id`, and edge-to-node joins is ~3-5x slower than fixed-width binary comparison.
- **GraphIndex build:** `HashMap<String, u32>` requires heap-allocated keys. `HashMap<u128, u32>` would be inline and faster to hash.

**Recommended migration path** (not before Step 6 profiling justifies it):

1. Separate external identity (`key()` — user-facing, indexed, human-readable) from internal identity (`id` — binary ULID, `FixedSizeBinary(16)`).
2. Edge `src`/`dst` store binary ULIDs, not `key()` strings.
3. `key()` property becomes a unique indexed column, not the `id` value.
4. `TypeIndex` becomes `HashMap<u128, u32>` — faster build, less memory.
5. Lookup by `key()` goes through a Lance scalar index on that column.

**Trigger:** Profile the `TypeIndex` build in Step 6. If `HashMap<String, u32>` construction is measurable (>10ms for the target graph size), that's the signal to switch.

---

## Query Execution

### Pipeline

```text
.gq → parse → typecheck → lower → execute_query(ir, params, snapshot, graph_index, catalog)
```

The executor is a pure function (`crates/omnigraph/src/exec/mod.rs`). All reads go through a single `Snapshot`. Graph index is `Option<&GraphIndex>` — only built/passed when the pipeline contains `Expand` or `AntiJoin`.

### IR Operations Supported

| IR Op | Implementation | Lance API |
|---|---|---|
| `NodeScan` | `execute_node_scan()` — opens dataset via `snapshot.open()`, pushes SQL filter | `Dataset::scan().filter(sql)` |
| `Filter` | `apply_filter()` — Arrow comparison kernels with auto type casting | `arrow_ord::cmp::eq/gt/lt/...` |
| `Expand` | `execute_expand()` — BFS on CSR/CSC, then `hydrate_nodes()` | `Dataset::scan().filter("id IN (...)")` |
| `AntiJoin` | `try_bulk_anti_join()` fast path (CSR offset check, O(N), zero I/O) or per-row fallback | `CsrIndex::has_neighbors()` |

### Expand Details

1. Extract source String IDs from binding batch
2. Map to dense u32 via `TypeIndex::to_dense()`
3. Select adjacency: CSR for `Direction::Out`, CSC for `Direction::In`
4. BFS with hop bounds (`min_hops..=max_hops`), cross-type aware visited set
5. Map result u32 back to String IDs via `TypeIndex::to_id()`
6. Hydrate destination nodes: `snapshot.open("node:{type}")` with `id IN (...)` filter (BTree indexed)

### AntiJoin Optimization

**Fast path** (`try_bulk_anti_join`): If the inner pipeline is exactly one `Expand` whose `src_var == outer_var`, resolve entirely from CSR — `CsrIndex::has_neighbors(dense)` is O(1) per node, zero Lance I/O. This handles the common `not { $p worksAt $_ }` pattern.

**Slow path**: Per-row inner pipeline execution for complex negations (e.g., `not { $p worksAt $c, $c.name = "Acme" }`).

### Projection and Ordering

- **Projection**: `PropAccess { variable, property }` → `bindings[variable].column_by_name(property)`
- **Ordering**: `lexsort_to_indices` with `SortColumn` per ordering expression
- **Limit**: `batch.slice(0, limit)`

### Known Limitation: Row Correlation

Each variable binding is an independent `RecordBatch`. Multi-variable returns across traversal hops work only when one side has exactly 1 row (filters down to a single source). General N×M cross-products require row-correlated bindings (deferred to v0.2.0).

---

## Memory Model

### Always In Memory

- schema catalog (from `_schema.pg`, small)
- known manifest state (version + entries — refreshed explicitly)

### Lazily Cached On Omnigraph Handle

- `GraphIndex`: CSR/CSC offset arrays + TypeIndex mappings. Built on first traversal query. Single slot, invalidated by edge mutations.

### Never Cached (Phase 1)

- node data (hydrated on demand from Snapshot per query)
- edge property data (read on demand)
- Lance fragment data (managed by Lance's own cache)

### Tiered Storage Model

| Tier | Latency | What lives here | Owner |
|---|---|---|---|
| Memory | < 1 us | CSR/CSC, TypeIndex, catalog, manifest metadata | `Omnigraph` handle |
| Local disk | < 1 ms | Lance fragment cache, Lance metadata cache | Lance SDK |
| Object storage | 10-100 ms | Lance data fragments, manifests, indices | Lance SDK |

---

## Branching Model (Planned — Step 9)

### Lance-Native Branching

Branches are Lance branches on the manifest table. Sub-tables are branched lazily.

**Lance APIs available** (verified in lance 3.0):
- `Dataset::create_branch(name)` — zero-copy branch
- `Dataset::checkout_branch(name)` — open at branch
- `Dataset::list_branches()` — enumerate branches
- `Dataset::delete_branch(name)` — remove branch
- `Dataset::shallow_clone(dest, base_paths)` — cross-location clone

**Create branch:**
1. `_manifest.lance` dataset: `ds.create_branch("experiment")` (zero-copy, inherits all rows)
2. No sub-tables branched yet — `table_branch` remains null

**First write on branch:**
1. Sub-table has no branch → create Lance branch on the sub-table
2. Write to the branched sub-table
3. Update manifest row: set `table_branch`, commit manifest on the branch

**Read from branch:**
1. Open manifest at the branch: `ds.checkout_branch("experiment")`
2. For each sub-table: if `table_branch` is null, open at `table_version` on main. If set, checkout that branch.

**Merge:**
1. Read source and target manifest states
2. For each sub-table that differs: `merge_insert` keyed by `id` to apply changes
3. Commit target manifest

Sub-tables not written to on a branch are never branched. Storage overhead is proportional to what changed.

---

## Mutation Model

### Pipeline

`Omnigraph::run_mutation()` → parse → typecheck → lower → `execute_mutation()`.

**Grammar**: `insert Type { prop: value }`, `update Type set { prop: value } where pred`, `delete Type where pred`

**IR**: `MutationIR { name, params, op: MutationOpIR }` with `Insert/Update/Delete` variants. `IRAssignment { property, value: IRExpr }`, `IRMutationPredicate { property, op, value: IRExpr }`.

**Typechecker**:
- Insert validates all non-nullable properties provided, validates edge `from`/`to` endpoints
- Update validates property types, rejects edge updates (T16)
- Delete validates predicate for both nodes and edges

### Runtime

**Insert**: Build single-row RecordBatch → `key()` types use `merge_insert` (upsert), keyless use `append` → commit manifest. Edge inserts invalidate graph index.

**Update**: Scan with predicate filter → apply assignments → `merge_insert` keyed by `id` → commit manifest. Rejects `key()` property changes.

**Delete with edge cascade**:
1. Scan for matching IDs
2. `Dataset::delete(predicate_sql)` — Lance deletion vectors (soft delete)
3. For each edge type referencing the deleted node type: `edge_ds.delete("src IN (...) OR dst IN (...)")` (BTree indexed)
4. Commit manifest for all changed sub-tables
5. Invalidate graph index: `self.cached_graph_index = None`

### Constraint Enforcement

Mutations enforce schema constraints at runtime:

- **Value constraints**: `range()` and `check()` validated before write in `execute_insert` and `execute_update`. Violations are hard errors.
- **Edge cardinality**: `@card(min..max)` validated in `execute_insert` (max bound check) and `execute_delete` (min bound check for min > 0). Violations are hard errors.
- **Unique constraints**: enforced via `merge_insert` keyed by `id` for `key()`, post-write duplicate scan for `unique(...)`.
- **Key immutability**: updates to `key()` properties are rejected by the typechecker (T16-style check).

---

## Search Model

Lance-native indexed search replaces Nanograph's brute-force implementations:

| Predicate | Omnigraph (Lance-indexed) | Lance Index Type |
|---|---|---|
| `search()` | FTS `match_tokens` | `IndexType::Inverted` |
| `fuzzy()` | N-gram FTS index | `IndexType::Inverted` (n-gram config) |
| `match_text()` | FTS `match_phrase` | `IndexType::Inverted` |
| `bm25()` | FTS `match` with BM25 scoring | `IndexType::Inverted` |
| `nearest()` | Lance ANN | `IndexType::IvfHnswPq` |
| `rrf()` | Application-level score fusion | N/A |

Index creation is driven by `index()` constraints in the schema:
- `index(title)` on String properties → `IndexType::Inverted` (FTS)
- `index(embedding)` on Vector properties → `IndexType::IvfHnswPq` (ANN)
- `ensure_indices()` already creates Inverted indices on `index()` String properties

The compiler IR already supports all search expressions (`IRExpr::Search/Fuzzy/MatchText/Bm25/Nearest/Rrf`). Only the runtime execution is missing.

---

## Consistency Model

### Core Principle

**A manifest version IS a database version.** One manifest version pins a consistent set of sub-table versions. That pinned set is an immutable `Snapshot` — the unit of read consistency.

### Snapshot Isolation

`Snapshot` is created from `ManifestCoordinator::known_state` — sync, no storage I/O. It holds `version: u64` + `entries: HashMap<String, SubTableEntry>`. It can open any sub-table on demand via `open(table_key)`, which resolves the path and checks out the pinned version.

**Within a query:** The executor takes one Snapshot at query start. All `NodeScan`, `Expand`, and `AntiJoin` operations read through it. Concurrent writes are invisible.

**Across queries:** Each query takes its own Snapshot from the handle's known version. Successive queries see the latest committed state (if the handle committed) or the last known state (if another writer committed and `refresh()` hasn't been called).

### Multi-Reader

Multiple readers call `snapshot()` concurrently — each gets an immutable value. Snapshots are `Clone + Send + Sync`. Readers never block writers. This is Lance MVCC for free.

### Multi-Writer

Multiple writers each open their own `Omnigraph::open(uri)` handle. When two writers commit:

- **Different sub-tables**: Lance merges manifest rows automatically (`merge_insert` by `table_key` is non-conflicting for different keys)
- **Same sub-table**: Lance's optimistic concurrency handles it (Append+Append is rebasable, same-row Update is retryable)
- **No application-level locking required**

### Read-After-Write

After `commit()`, the coordinator's `known_state` is updated. The next `snapshot()` sees the new version immediately. No `refresh()` needed for own writes.

### What Lance Provides Natively

| Capability | Lance mechanism |
|---|---|
| Snapshot isolation | Immutable versioned manifests in `_versions/` |
| Atomic commits | `put-if-not-exists` or `rename-if-not-exists` on manifest file |
| Conflict detection | Transaction files in `_transactions/` with compatibility matrix |
| Conflict resolution | Rebasable (auto-merge), retryable (re-execute), or incompatible (fail) |
| Time travel | `checkout_version(N)` reads any historical version |
| Deletion without rewrite | Deletion vectors (soft delete) |

### What Omnigraph Adds

The manifest table extends Lance's per-dataset MVCC to **cross-dataset consistency**. Without it, each sub-table has independent versioning with no guarantee that Person version 5 and Knows version 3 form a consistent graph. The manifest pins them together.

### Streaming Consistency (MemWAL — Planned)

When MemWAL is enabled on sub-table datasets, consistency operates at two levels:

**Sub-table level (Lance MemWAL):** Each sub-table has its own LSM-tree. Readers must merge results from the base table + flushed MemTables + (optionally) the in-memory MemTable. Lance handles this transparently through its scanner — `Snapshot::open()` returns a `Dataset` whose scan already includes flushed MemTable data via LSM-tree deduplication by primary key.

**Cross-table level (Omnigraph manifest):** The manifest pins sub-table versions into a consistent graph snapshot. With MemWAL, the manifest pins base table versions. Flushed-but-unmerged MemTable data is visible through the dataset's LSM-tree read but not explicitly tracked in the manifest — it rides along with the dataset version.

**Read consistency spectrum:**

| Mode | Mechanism | Freshness | Use case |
|---|---|---|---|
| Strong | `Snapshot` + in-memory MemTable access for all regions | All committed writes visible | Single-process, co-located reader-writer |
| Checkpoint | `Snapshot` at last manifest commit | Writes visible up to last checkpoint | Multi-process readers, typical query workload |
| Eventual | Stale `Snapshot` (no `refresh()`) | Writes visible up to last known version | Long-running analytics, time-travel queries |

**Correctness guarantees preserved:**

- Within a query: all reads go through one Snapshot — same as without MemWAL
- Cross-type consistency: manifest still pins sub-table versions together
- Stale MemWAL index is safe: reading already-merged MemTables from both flushed storage and base table produces correct results via LSM-tree deduplication by primary key. No data loss, just minor read amplification
- Garbage-collected MemTables still in the MemWAL index: reader fails to open them and skips — safe because the data is already in the base table
- Newly flushed MemTables not yet in the MemWAL index: not queried — result is eventually consistent but correct for the snapshot's point in time

**Graph index interaction with MemWAL:**

- `GraphIndex` is built from edge sub-tables via `Snapshot::open()` → `ds.scan()`
- With MemWAL, the scan includes flushed MemTable data automatically (Lance LSM-tree merging read)
- Unflushed in-memory MemTable edges are NOT in the graph index — they become visible after MemTable flush + next graph index rebuild
- Graph index invalidation triggers on manifest checkpoint that includes edge sub-table version changes, not on every WAL write
- The graph index cache key could be extended to include MemWAL generation numbers for finer-grained invalidation

---

## Concurrency Model

Branches are the primary concurrency primitive. Progression:

| Phase | Workload | Mechanism |
|---|---|---|
| Local/agent | Single writer, batch loads | Lance `merge_insert` per sub-table + manifest commit |
| Multi-agent | Independent work streams | Branch-per-writer + merge (zero-copy fork, isolated writes) |
| Collaboration | Same branch, low contention | Lance optimistic concurrency (Append+Append = rebasable, same-row Update = retryable) |
| Streaming | High throughput on one branch | Lance MemWAL per sub-table (see below) |

### Streaming: Lance MemWAL (Planned)

Lance MemWAL is a Lance-native LSM-tree architecture for high-throughput streaming writes. It enables multiple concurrent writers on a single branch without custom WAL or coordination logic in Omnigraph.

**Architecture:**

Each sub-table dataset (the "base table" in MemWAL terms) can independently have MemWAL enabled. Writers write to an in-memory MemTable backed by a durable WAL per region. MemTables flush to storage periodically; flushed MemTables merge into the base table asynchronously via `merge_insert`. The same Lance API surface (`append`, `merge_insert`, `delete`) works transparently — MemWAL routes writes through the WAL internally. Omnigraph does not build a custom WAL.

**Region model and primary keys:**

- Each MemWAL region has exactly one active writer at a time, enforced by epoch-based fencing
- **Correctness invariant:** rows with the same primary key must go to the same region — otherwise merge order between regions can corrupt "last write wins" semantics
- Omnigraph's `id` column (String, `key()` value or ULID) serves as the unenforced primary key required by MemWAL
- Region assignment strategy: for node types, partition by `id`; for edge types, partition by `src` (ensures all edges from the same source node go to the same region, which aligns with CSR build patterns)
- Region specs use Lance's `bucket(id, N)` transform to distribute rows across N regions
- Multiple writers claim different regions → horizontal write scale-out on a single branch, no coordination between writers

**Manifest coordination challenge:**

The current model (Step 7) does one manifest commit per mutation. With streaming writes, hundreds of per-row commits per second would thrash `_manifest.lance`. The solution is **manifest commit batching**:

- `ManifestCoordinator` accumulates `SubTableUpdate` entries in memory
- Commits periodically (every N writes or every T seconds) via an explicit `checkpoint()` call
- The manifest version represents a "checkpoint" rather than every individual write
- Between checkpoints, readers using `Snapshot` see the last checkpoint — this matches MemWAL's own eventual consistency model (unflushed MemTable data is invisible to readers without in-memory access)

**What changes vs what stays:**

| Aspect | Current (Step 7) | Streaming (MemWAL) |
|---|---|---|
| Write path | `ds.append()` / `ds.delete()` | Same API, routed through WAL internally |
| Commit granularity | Per mutation | Batched (periodic manifest checkpoint) |
| Writer concurrency | `&mut Omnigraph` (exclusive) | Region-per-writer (epoch fencing) |
| Read freshness | Snapshot at last commit | Snapshot at last checkpoint (eventual) |
| Graph index | Invalidate on edge mutation | Invalidate on checkpoint that includes edge changes |
| `run_mutation()` API | Unchanged | Unchanged (buffering is internal) |

**Omnigraph integration steps** (when triggered):

1. **Enable MemWAL on sub-table datasets** — configuration when creating datasets in `ManifestCoordinator::init()` or via post-creation setup
2. **Region assignment** — partition by `id` column. Region specs use Lance's `bucket(id, N)` transform
3. **Manifest commit batching** — replace per-mutation manifest commit with periodic checkpoint. `ManifestCoordinator` accumulates `SubTableUpdate` entries, flushes on interval or explicit `checkpoint()` call
4. **Writer handle pool** — `Omnigraph` handle exposes region-aware write API. Each writer claims a region via epoch-based fencing. Multiple handles write concurrently to different regions on the same branch
5. **Read path unchanged** — `Snapshot::open()` returns a `Dataset` that includes flushed MemTable data via Lance's LSM-tree merging read. Graph index builds from merged scan results

**Trigger:** Profile write throughput on agent workloads. If per-mutation commit latency exceeds 10ms or throughput drops below 100 writes/sec, add manifest batching first, then MemWAL. **Prerequisite:** Step 7 (complete) and Step 9 (branching).

---

## Implementation Status

See `implementation-plan.md` for detailed step-by-step status, compiler types to reuse, runtime code to write, Lance APIs, and test cases for each remaining step.

```
Steps 0–8 ✅ (216 tests)
     └→ Step 7a: Constraint system restructuring
          ├→ Step 9: Branching (Lance branches on manifest + sub-tables)
          │    └→ Step 10: Change tracking + CLI
          └→ Step 10: CLI core wiring
```

---

## Success Criteria

Omnigraph is successful when:

- `omnigraph init` creates a repo with per-type Lance tables, manifest, stable row IDs, and BTree indices
- `omnigraph load` writes JSONL to per-type tables, commits the manifest atomically, and creates scalar indices
- `omnigraph run` executes queries with filter pushdown, graph traversal (CSR/CSC), bulk anti-join, and indexed search
- `omnigraph branch create` is O(1) regardless of data size (manifest branch only)
- `omnigraph branch merge` applies net changes proportional to what changed
- the same URI works for local and S3 repos
- no custom WAL, no custom JSON manifest, no custom CDC — Lance-native throughout
- the compiler crate has zero Lance dependency and can be shared with Nanograph

---

## Deferred

1. **Remote sync (clone/push/pull).** URI-based open + `base_paths` make this architecturally possible. Deferred to after local branching works.
2. **Schema evolution / migration.** Lance `add_columns`/`alter_columns`/`drop_columns` when needed.
3. **Hook system.** Thin dispatch layer on Lance version tracking. Requires Step 10.
4. **Compaction.** Lance handles natively. Wire up as CLI command.
5. **SDKs (TS, Swift, Python).** Thin wrappers after API stabilizes.
6. **DuckDB fallback.** For graphs > ~5M edges. Defer until scale demands.
7. **MemWAL — Streaming concurrent writes on a single branch.** Lance MemWAL is an LSM-tree architecture enabling high-throughput streaming writes. Each sub-table dataset gets its own MemWAL with regions (one active writer per region, epoch-fenced). The same `append`/`merge_insert`/`delete` API surface works transparently — MemWAL routes writes through a durable WAL internally. Omnigraph integration requires: (1) enable MemWAL on sub-table datasets, (2) region assignment by `id` column using `bucket(id, N)` transform, (3) manifest commit batching — replace per-mutation commit with periodic checkpoint, (4) writer handle pool with region-aware API. The read path is unchanged — `Snapshot::open()` returns a Dataset that includes flushed MemTable data via Lance's LSM-tree merging read. See Concurrency Model and Consistency Model sections for full architecture. **Trigger:** per-mutation commit latency >10ms or throughput <100 writes/sec. **Prerequisite:** Step 7 (complete) + Step 9.
8. **Service layer / HTTP API.** `Omnigraph` struct IS the cache — a service just keeps it alive.
9. **Binary ULIDs.** Switch `id` from Utf8 to `FixedSizeBinary(16)` for performance. See Identity Model section.
10. **Row-correlated bindings.** Tuple-based binding model for general N×M cross-variable returns. See Query Execution section.
11. **take_rows() hydration.** Use Lance stable row ID addresses for O(1) node hydration. Scalar indices already cover the IN filter path.
12. **Composite key enforcement at Lance level.** Step 7a adds composite key syntax (`key(tenant, slug)`) to the schema language, but the runtime still uses a single `id` column. Full composite key support (multi-column unique constraint at the Lance level) is deferred.
13. **Cross-branch cardinality validation.** Edge cardinality is validated per-branch. Cross-branch cardinality semantics after merge are deferred.
