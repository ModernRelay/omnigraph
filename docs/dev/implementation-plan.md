# Omnigraph Implementation Plan

Optimal build order from first principles. No backwards compatibility. No intermediate scaffolding.

---

## Principles

1. **Build each layer correctly once.** No temporary implementations that get replaced. Every line of code targets the final architecture.
2. **Lance-native from the start.** Manifest is a Lance table. Stable row IDs on every dataset. Shallow clone branching. No custom WAL, no custom JSON manifest, no custom CDC.
3. **Carry forward the compiler.** The schema parser, query parser, typechecker, and IR lowering are proven and unchanged. Everything below the IR is new.
4. **String IDs everywhere.** `@key` values or ULIDs. No `u64` counters, no ID coordination across branches.
5. **URIs not paths.** Every `open`/`init` accepts `&str` (local path or `s3://`). Lance handles both.
6. **Dependency order, not difficulty order.** Build what other things depend on first.

---

## Target Architecture

```
graph-db/                                 # repo root (local path or s3:// URI)
├── _schema.pg                            # Schema source of truth
├── _manifest.lance/                      # Lance table: one row per sub-table
│   ├── _versions/                        # MVCC versioning (repo version = manifest version)
│   ├── _refs/branches/                   # Lance-native branch metadata
│   ├── _refs/tags/                       # Lance-native tag metadata
│   └── tree/{branch}/                    # Branch-specific manifest versions
├── nodes/
│   ├── a1b2c3d4/                         # Per-type dataset (hash of type name at creation)
│   │   ├── _versions/
│   │   ├── data/
│   │   ├── _indices/
│   │   └── tree/{branch}/               # Lazily created on first branch write
│   └── e5f6a7b8/
└── edges/
    ├── f7012952/
    └── 3c4d5e6f/
```

Sub-table directory names are stable hashes of the type name at creation time. On type rename, only the manifest row's `table_key` changes — the directory is untouched. No S3 directory renames needed.

**Manifest table schema:**

| Column | Type | Description |
|---|---|---|
| `table_key` | Utf8 (PK) | `"node:Person"` or `"edge:Knows"` |
| `table_path` | Utf8 | `"nodes/a1b2c3d4"` — relative to repo root, stable hash |
| `table_version` | UInt64 | Pinned Lance version |
| `table_branch` | Utf8 (nullable) | Lance branch on sub-table (null = main) |
| `row_count` | UInt64 | Rows in sub-table at this version |

Repo version = manifest table version. Branch = manifest table branch. Tag = manifest table tag.

**Sub-table schemas (nodes):**

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | `@key` value or ULID. `unenforced_primary_key`. BTree index. |
| `{property}` | typed | One column per property, matching `.pg` type |

**Sub-table schemas (edges):**

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | ULID. `unenforced_primary_key`. |
| `src` | Utf8 | Source node `id`. BTree index. |
| `dst` | Utf8 | Destination node `id`. BTree index. |
| `{property}` | typed | Edge properties |

All datasets created with `enable_stable_row_ids = true` and `LanceFileVersion::V2_2`.

---

## Crate Structure

```
crates/
├── omnigraph-compiler/        # No Lance dependency. Fast to build and test.
│   └── src/
│       ├── schema/            # .pg parser, AST (pest)
│       ├── query/             # .gq parser, AST, typechecker (pest)
│       ├── catalog/           # Catalog, schema IR (arrow-schema)
│       ├── ir/                # IR types, lowering
│       ├── types.rs           # ScalarType, PropType, Direction
│       ├── error.rs           # NanoError, ParseDiagnostic (ariadne)
│       ├── embedding.rs       # Embedding client (reqwest)
│       ├── json_output.rs     # Arrow → JSON
│       ├── result.rs          # QueryResult, MutationResult
│       └── query_input.rs     # Named query lookup, param parsing
│
├── omnigraph/                 # The database. Lance-dependent. Repo + engine unified.
│   └── src/
│       ├── db/                # Omnigraph, ManifestCoordinator, branching, tagging
│       │   ├── mod.rs         # Omnigraph struct — the single entry point
│       │   ├── manifest.rs    # ManifestCoordinator — Lance table CRUD, commit protocol
│       │   └── branch.rs      # Lazy sub-table branching, shallow clone, merge
│       ├── exec/              # QueryExecutor, scan, traverse, search, mutate, filter
│       │   ├── mod.rs         # QueryExecutor — SessionContext setup, orchestration
│       │   ├── scan.rs        # IR NodeScan → DataFusion SQL via LanceTableProvider
│       │   ├── traverse.rs    # IR Expand → CSR/CSC graph index
│       │   ├── search.rs      # IR Search/Fuzzy/BM25 → Lance FTS
│       │   ├── mutate.rs      # IR Insert/Update/Delete → Lance write APIs
│       │   └── filter.rs      # IR Filter/AntiJoin → DataFusion + Arrow
│       ├── graph_index/       # Lazy CSR/CSC with String→u32 dense mapping
│       │   ├── mod.rs         # GraphIndexManager — lazy build + cache
│       │   ├── csr.rs         # CSR with u32 dense indices
│       │   └── cache.rs       # Tiered node cache (Full vs OnDemand)
│       └── loader/            # JSONL → per-type Lance tables
│           ├── mod.rs         # Orchestrator: parse → write per-type → commit manifest
│           ├── jsonl.rs       # JSONL parsing → per-type RecordBatches
│           ├── constraints.rs # @key/@unique validation against Lance tables
│           └── embeddings.rs  # Embedding pipeline (independent of storage)
│
└── omnigraph-cli/             # Binary
    └── src/
        └── main.rs
```

**Dependency chain:**

```
omnigraph-cli → omnigraph → omnigraph-compiler
                    ↓
               lance, datafusion, arrow, tokio
```

The boundary is the **Lance dependency line**. `omnigraph-compiler` has zero Lance dependency.

**What's deleted entirely from current codebase:** All of `store/` (database.rs, graph.rs, lance_io.rs, manifest.rs, txlog.rs, runtime.rs, metadata.rs, indexing.rs, migration.rs, export.rs, and all database/ submodules). All of `plan/` (physical.rs, node_scan.rs, planner.rs, bindings.rs). Replaced by `db/`, `exec/`, `graph_index/`.

---

## Build Order

### Step 1: Foundation Types

Change the identity model at the type level. This touches almost nothing but unblocks everything.

**Changes in `omnigraph-compiler`:**
- `types.rs`: Remove `NodeId = u64` / `EdgeId = u64` aliases (use String directly)
- `catalog/mod.rs`: `id` field becomes `DataType::Utf8` instead of `DataType::UInt64` in both node and edge Arrow schemas. Edge `src`/`dst` become `Utf8`.
- Add `@key` awareness to `NodeType`: track which property (if any) is the `@key` so the `id` column value can be derived from it.

**What doesn't change:** Schema parser, query parser, typechecker, IR lowering, IR types, error types, embedding client, JSON output, result types, query input. These are all type-agnostic or already handle String IDs.

**Test:** `build_catalog()` produces Arrow schemas with `Utf8` id/src/dst columns. All existing parser and typechecker tests pass unchanged.

**~50 lines changed, 0 new files.**

### Step 2: Manifest Coordinator

The single most important new component. Everything depends on it.

**New: `omnigraph/src/db/manifest.rs`**

```rust
pub struct ManifestCoordinator {
    root_uri: String,              // local path or s3:// — not PathBuf
    dataset: Dataset,              // Lance dataset for _manifest.lance
}

impl ManifestCoordinator {
    pub async fn init(root_uri: &str, catalog: &Catalog) -> Result<Self>;
    pub async fn open(root_uri: &str) -> Result<Self>;
    pub async fn state(&self) -> Result<ManifestState>;
    pub async fn commit(&self, updates: &[SubTableUpdate]) -> Result<u64>;
    pub async fn open_sub_table(&self, entry: &SubTableEntry) -> Result<Dataset>;
    pub fn version(&self) -> u64;
    pub fn root_uri(&self) -> &str;
}
```

`init` creates:
1. Per-type Lance datasets from the catalog's Arrow schemas, all with `enable_stable_row_ids = true` and `LanceFileVersion::V2_2`
2. The `_manifest.lance` table with one row per sub-table (all at version 0)
3. Copies `_schema.pg` into the repo root

`open_sub_table` resolves `entry.table_path` relative to `root_uri`. For shallow clones, the sub-table's own Lance manifest handles remote fragment resolution via `base_paths` — no application-level fallback needed.

`commit` does `merge_insert` on `_manifest.lance` keyed by `table_key`. This is the atomic commit point for the repo.

**Test:** Init a repo (local), read manifest state, commit an update, read again. Version advances. Sub-table entries are correct. Also test with a URI to verify string-based paths work.

**~300 lines new.**

### Step 3: Omnigraph (Init + Open)

The top-level API. Wraps ManifestCoordinator with schema handling.

**New: `omnigraph/src/db/mod.rs`**

```rust
pub struct Omnigraph {
    root_uri: String,
    manifest: ManifestCoordinator,
    catalog: Catalog,
    schema_source: String,
}

impl Omnigraph {
    pub async fn init(uri: &str, schema_source: &str) -> Result<Self>;
    pub async fn open(uri: &str) -> Result<Self>;
    pub fn catalog(&self) -> &Catalog;
    pub fn uri(&self) -> &str;
    pub async fn version(&self) -> u64;
}
```

`init`:
1. Parse schema → build catalog (uses Step 1 changes)
2. Write `_schema.pg` to repo root
3. Call `ManifestCoordinator::init()` with the catalog

`open`:
1. Read `_schema.pg` from repo root → parse → build catalog
2. Open `ManifestCoordinator`
3. Validate manifest state against catalog

**Test:** `init` creates the repo with `_schema.pg`, `_manifest.lance/`, and per-type Lance datasets. `open` reads it back. Catalog matches schema.

**CLI: `omnigraph init --schema ./schema.pg s3://bucket/my-graph`** (positional URI, not `--repo`)

**~200 lines new (db/mod.rs), ~50 lines adapted (CLI).**

### Step 4: Data Loading

Load JSONL into per-type Lance tables via the manifest.

The JSONL parser from `store/loader/jsonl.rs` carries forward with the output target changed: instead of building `GraphStorage`, it produces per-type `RecordBatch` vectors that are written directly to Lance datasets.

**Adapt: `loader/jsonl.rs`**
- Parse JSONL, spool by type (this logic is unchanged)
- Build Arrow batches per type (unchanged)
- Change: `id` column is `Utf8` (ULID or `@key` value instead of auto-increment u64)
- Change: edge `src`/`dst` are `Utf8` (resolved from `@key` of source/target)

**New: `loader/mod.rs` orchestrator**

```rust
pub async fn load_jsonl(db: &Omnigraph, data_path: &str, mode: LoadMode) -> Result<LoadResult>;
```

1. Parse JSONL → per-type RecordBatches
2. For each type with data:
   - Open sub-table via manifest
   - Write batch (Overwrite / Append / `merge_insert` by `id`)
   - Collect new version number
3. Commit manifest with all updated versions (single atomic commit)

**Adapt: `loader/constraints.rs`** — validate `@key` uniqueness and `@unique` by scanning the Lance table with a filter, not by scanning in-memory batches.

**Adapt: `loader/embeddings.rs`** — unchanged (independent of storage).

**Test:** Load `test.jsonl` into a fresh repo. Read back via Lance scan. All types populated. IDs are Utf8. Edge src/dst reference node IDs correctly. Test all three modes (overwrite, append, merge).

**CLI: `omnigraph load --data ./data.jsonl /local/my-graph`**

**~500 lines adapted, ~200 lines new orchestrator.**

### Step 5: Query Execution — Tabular Operations

Replace the custom DataFusion plan tree with SQL generation via `SessionContext` + `LanceTableProvider`.

**New: `exec/mod.rs`**

```rust
pub struct QueryExecutor {
    ctx: SessionContext,
    graph_index: GraphIndexManager,
}

impl QueryExecutor {
    pub async fn new(db: &Omnigraph) -> Result<Self>;
    pub async fn execute_query(&self, ir: &QueryIR, params: &ParamMap) -> Result<QueryResult>;
    pub async fn execute_mutation(&self, ir: &MutationIR, params: &ParamMap) -> Result<MutationResult>;
}
```

`new`:
1. Create shared `Session` for pooling Lance cache resources across sub-tables
2. Create `SessionContext`
3. Open each sub-table via manifest with the shared `Session`, register as `LanceTableProvider`
4. Register Lance UDFs (`contains_tokens`, etc.)
5. Create `GraphIndexManager` (empty — built lazily)

**New: `exec/scan.rs`** — IR `NodeScan { type, filters }` → generate SQL:
```sql
SELECT * FROM "nodes/Person" WHERE age > 30 AND name = 'Alice'
```
Execute via `ctx.sql()`. Return `RecordBatch`.

**New: `exec/filter.rs`** — IR `Filter` → SQL WHERE clause generation. IR `AntiJoin` → execute inner pipeline, compute set difference on `id`. Carry forward `apply_ir_filters` for struct-column operations DataFusion can't express.

**Carry forward:** `plan/literal_utils.rs` (literal → Arrow conversion), portions of `apply_projection`, `apply_aggregation`, `apply_order_and_limit` from `plan/planner.rs` for graph-specific struct-column operations.

**Test:** Execute `get_person`, `adults`, `top_by_age` from `test.gq` against a loaded repo. Results match expected output.

**CLI: `omnigraph run get_person --param name=Alice /local/my-graph`**

**~400 lines new, ~300 lines carried forward from planner.rs.**

### Step 6: Graph Index + Traversal

The part that makes it a graph database. No Lance equivalent exists — this is custom.

**New: `graph_index/mod.rs`**

```rust
pub struct GraphIndexManager {
    cache: HashMap<(String, u64), Arc<GraphIndex>>,  // (branch, manifest_version)
}

pub struct GraphIndex {
    type_indices: HashMap<String, TypeIndex>,    // per node type
    csr: HashMap<String, CsrIndex>,              // per edge type (outgoing)
    csc: HashMap<String, CsrIndex>,              // per edge type (incoming)
    node_caches: HashMap<String, NodeCache>,
}

pub struct TypeIndex {
    id_to_dense: HashMap<String, u32>,
    dense_to_id: Vec<String>,
}

pub enum NodeCache {
    Full(RecordBatch),
    OnDemand,
}
```

**Adapt: `graph_index/csr.rs`** — same algorithm as `store/csr.rs` but with `u32` indices instead of `u64`. Offset arrays sized to actual node count per type.

**Build process** (lazy, on first traversal query):
1. For each edge type in the catalog:
   - Open sub-table via manifest (using shared Session from Step 5)
   - Scan `(src, dst)` columns only (2 columns, no properties)
   - Build `TypeIndex` for src and dst node types (String → dense u32)
   - Build CSR (outgoing) and CSC (incoming)
2. For each destination node type:
   - If row_count < 100K: scan full table → `NodeCache::Full`
   - Otherwise: `NodeCache::OnDemand`
3. Cache keyed by `(branch, manifest_version)`

**New: `exec/traverse.rs`** — IR `Expand { src_var, edge_type, direction }`:
1. Extract source String IDs from input batch
2. Map to dense u32 via `type_indices[src_type]`
3. BFS/DFS on CSR/CSC (sub-millisecond, in-memory)
4. Map result u32 back to String IDs
5. Hydrate:
   - `NodeCache::Full` → `take_rows()` from cached batch (microseconds)
   - `NodeCache::OnDemand` → `SELECT * FROM "nodes/Person" WHERE id IN (...)` (milliseconds)

**Public: `Omnigraph::warm(branch)`** — pre-builds the graph index + node caches for a branch. Returns immediately if already cached. Useful for service deployments to avoid cold-start latency on first traversal.

**Test:** Execute `friends_of`, `friends_of_friends`, `employees_of`, `unemployed` from `test.gq`. Multi-hop traversal works. Negation (`not { $p worksAt $_ }`) works via AntiJoin.

**~400 lines new (graph index), ~200 lines new (traverse executor).**

### Step 7: Mutations

Insert, update, delete with schema-driven edge cascade.

**New: `exec/mutate.rs`**

**Insert:**
1. Validate against catalog (type exists, properties valid)
2. Generate `id`: use `@key` property value, or generate ULID
3. Build RecordBatch with id + properties
4. Append to `nodes/{Type}.lance` or `edges/{Type}.lance`
5. Commit manifest

**Update (keyed):**
1. `merge_insert` on `nodes/{Type}.lance` keyed by `id`
2. Commit manifest

**Delete with cascade:**
1. Query `nodes/{Type}.lance WHERE predicate` → collect IDs
2. Lance native `delete()` on node table (deletion vectors, no rewrite)
3. For each edge type referencing this node type (from catalog):
   - `delete()` on edge table `WHERE src IN (...) OR dst IN (...)`
4. Commit manifest
5. Invalidate graph index cache for current branch

**Test:** Insert a Person, query it back. Update a property, verify change. Delete a Person, verify cascade removes connected edges. Verify graph index sees updated topology after invalidation.

**~300 lines new.**

### Step 8: Search Predicates

Replace brute-force search with Lance FTS.

**New: `exec/search.rs`**

- `search()` → Lance FTS `contains_tokens` via DataFusion UDF
- `fuzzy()` → Lance n-gram FTS index
- `match_text()` → Lance FTS `phrase` query
- `bm25()` → Lance FTS `match` with BM25 scoring
- `nearest()` → Lance ANN via `dataset.scanner().nearest()`
- `rrf()` → application-level score fusion over two result sets

**Index creation:** During `init` (Step 3), create FTS indexes on `@index`-annotated String properties and vector indexes on `@index`-annotated Vector properties. During `load` (Step 4), indexes are updated automatically by Lance on write.

**Test:** Load the signals fixture. Execute search queries. Verify indexed results match expected output.

**~300 lines new.**

### Step 9: Branching

Lance-native branching on the manifest table + lazy sub-table branching.

**New: `omnigraph/src/db/branch.rs`**

**Create branch:**
1. Create Lance branch on `_manifest.lance` (zero-copy — inherits all rows from parent)
2. No sub-tables are branched yet

**First write on branch:**
1. Detect that sub-table has no branch for this branch name
2. Create Lance branch on the sub-table (shallow clone via `base_paths`)
3. Write to the branched sub-table — new fragments at the branch's dataset root
4. Update manifest row: set `table_branch` to branch name
5. Commit manifest on the branch

**Snapshot (reading from a branch):**
1. Open manifest at the branch
2. For each sub-table the query needs:
   - If `table_branch` is null: open sub-table at `table_version` on main
   - If `table_branch` is set: open sub-table at that branch
   - Lance handles fragment resolution via `base_paths` — inherited fragments read from source, new fragments read from branch root

**Merge:**
1. Read source and target manifest states
2. For each sub-table that was branched on source:
   - Diff using `_row_created_at_version` / `_row_last_updated_at_version`
   - Apply changes to target via `merge_insert` keyed by `id`
   - Record new target version
3. Update target manifest with new sub-table versions
4. Optionally delete source branch

**Tag:** Create Lance tag on `_manifest.lance`. Immutable pointer to a manifest version.

**Test:** Create branch, load data on branch, verify main doesn't see it. Merge back, verify main sees it. Tag a version, verify time travel works. Verify graph index per branch — branch traversal sees branch-specific edges.

**CLI: `omnigraph branch create experiment /local/my-graph`**

**~500 lines new.**

### Step 10: Change Tracking + CLI Polish

**Changes command:**
```
omnigraph changes --since 5 /local/my-graph
```
Query manifest to find which sub-tables changed between version 5 and current. For each, query `_row_created_at_version` and `_row_last_updated_at_version`. Report inserts, updates, deletes.

**Diff command:**
```
omnigraph diff 3 7 /local/my-graph
```
Same mechanism, specific version range.

**Remaining CLI commands:**
- `describe` — read manifest state + sub-table schemas
- `snapshot --json` — dump manifest state as JSON
- `compact` — run Lance compaction on manifest + all sub-tables
- `export` — scan per-type tables via manifest, emit JSONL

**~400 lines adapted/new.**

---

## Dependency Graph

```
Step 1 (types)
  └→ Step 2 (manifest coordinator)
       ├→ Step 3 (Omnigraph init/open)
       │    ├→ Step 4 (loader)
       │    │    └→ Step 8 (search indexes created during init/load)
       │    ├→ Step 5 (query execution — tabular)
       │    │    └→ Step 6 (graph index + traversal)
       │    └→ Step 7 (mutations)
       └→ Step 9 (branching)
            └→ Step 10 (changes, diff, CLI polish)
```

Steps 4, 5, 7 can proceed in parallel after Step 3.
Step 6 depends on Step 5 (uses the same SessionContext + shared Session).
Step 8 can proceed in parallel with 5/6/7 (independent search path).
Step 9 depends on Step 2 (manifest) but not on 4-8.
Step 10 is last.

**Critical path:** 1 → 2 → 3 → 5 → 6 (this gives you `init` + `load` + full query execution including traversal)

---

## What Carries Forward Unchanged

All in `omnigraph-compiler`:

| Module | Lines | Why |
|---|---|---|
| `schema/parser.rs` + `schema.pest` | ~1,000 | Grammar and parser are proven |
| `schema/ast.rs` | 53 | Clean AST types |
| `query/parser.rs` + `query.pest` | ~1,500 | Grammar and parser are proven |
| `query/ast.rs` | 219 | Clean AST types |
| `query/typecheck.rs` | 2,456 | Compile-time validation, type rules T1-T21 |
| `ir/lower.rs` | 637 | AST → IR transformation |
| `ir/mod.rs` | 143 | IR types (IROp, IRFilter, IRExpr, etc.) |
| `error.rs` | 85 | Error types with source spans |
| `embedding.rs` | 377 | Provider-agnostic embedding client |
| `json_output.rs` | 305 | Arrow → JSON with JS-safe integers |
| `result.rs` | 281 | QueryResult, MutationResult, IPC serialization |
| `query_input.rs` | 809 | Named query lookup, param parsing |
| **Total** | **~7,865** | |

## What's New

All in `omnigraph`:

| Module | Est. Lines | Purpose |
|---|---|---|
| `db/mod.rs` | ~200 | Omnigraph struct — init, open, warm, version |
| `db/manifest.rs` | ~300 | ManifestCoordinator — Lance table CRUD, commit protocol |
| `db/branch.rs` | ~500 | Branching, tagging, merge, lazy sub-table branching |
| `exec/mod.rs` | ~150 | QueryExecutor — SessionContext + shared Session setup |
| `exec/scan.rs` | ~200 | IR NodeScan → DataFusion SQL |
| `exec/traverse.rs` | ~200 | IR Expand → CSR/CSC traversal + hydration |
| `exec/search.rs` | ~300 | IR Search/Fuzzy/BM25/Nearest → Lance FTS/ANN |
| `exec/mutate.rs` | ~300 | IR Insert/Update/Delete → Lance write APIs |
| `exec/filter.rs` | ~200 | IR Filter/AntiJoin + struct-column ops |
| `graph_index/mod.rs` | ~200 | Lazy graph index build + cache + warm() |
| `graph_index/csr.rs` | ~80 | CSR with u32 dense indices |
| `graph_index/cache.rs` | ~100 | Tiered node cache (Full vs OnDemand) |
| `loader/mod.rs` | ~200 | Load orchestrator: parse → write per-type → commit |
| **Total** | **~2,930** | |

## What's Adapted

| Module | Est. Lines | Change |
|---|---|---|
| `types.rs` | ~50 changed | Remove u64 aliases, id column Utf8 |
| `catalog/mod.rs` | ~30 changed | Arrow schema uses Utf8 for id/src/dst, @key tracking |
| `loader/jsonl.rs` | ~200 changed | Target Lance tables, String IDs |
| `loader/constraints.rs` | ~100 changed | Validate against Lance tables |
| `loader/embeddings.rs` | 0 changed | Carries forward unchanged |
| `omnigraph-cli/main.rs` | ~200 changed | New backend calls, URI-based, branch/tag commands |
| **Total** | **~580** | |

## What's Deleted

Everything in `store/` and `plan/` from the current `omnigraph-engine`:

| Module | Lines | Replaced by |
|---|---|---|
| `store/database.rs` + submodules | ~5,000 | `db/` + `exec/` |
| `store/graph.rs` | 543 | Direct Lance table access |
| `store/lance_io.rs` | 274 | Direct Lance SDK |
| `store/manifest.rs` | 146 | `db/manifest.rs` |
| `store/txlog.rs` | 941 | Lance versioning (no WAL) |
| `store/runtime.rs` | varies | `graph_index/` |
| `store/metadata.rs` | varies | Manifest table metadata |
| `store/indexing.rs` | 200 | Lance index API |
| `store/migration.rs` | 2,648 | Deferred |
| `store/export.rs` | varies | Thin scan over manifest |
| `plan/physical.rs` | varies | `exec/traverse.rs` + `exec/scan.rs` |
| `plan/node_scan.rs` | varies | `exec/scan.rs` |
| `plan/planner.rs` | varies | `exec/mod.rs` (SQL generation) |
| `plan/bindings.rs` | varies | `exec/filter.rs` |
| **Total** | **~12,000+** | |

---

## What This Doesn't Include (Deferred)

These are real requirements but not part of the initial build. The architecture supports all of them without retrofitting.

1. **Remote sync (clone/push/pull).** The URI-based open and `base_paths` fragment resolution make this architecturally possible now. Implementation (copying fragments, advancing remote manifests) is deferred to after local branching works. Per-type tables give O(changed types) sync.

2. **Schema evolution / migration.** The current `store/migration.rs` (2,648 lines) handles schema diffs and safety classifications. Conceptually carries forward but execution backend needs rewriting for Lance `add_columns`/`alter_columns`/`drop_columns`. Defer to after the core works.

3. **Hook system.** Reactive hooks on graph changes via Lance version tracking columns. Requires change tracking (Step 10) to be solid. A thin dispatch layer, not a storage concern.

4. **Compaction and cleanup.** Lance handles this natively. Wire up as a CLI command after the core works.

5. **TypeScript/Swift/Python SDKs.** Thin wrappers around the Rust core. Build after the API stabilizes.

6. **DuckDB fallback for large graphs.** The in-memory graph index works up to ~5M edges. Beyond that, a DuckDB recursive CTE fallback may be needed. Defer until we hit that scale.

7. **MemWAL for high-throughput ingest.** Only needed for SaaS streaming workloads with many concurrent writers on one branch. The branch-per-writer model handles multi-agent concurrency without MemWAL. Per-type tables are natural MemWAL boundaries when the time comes.

8. **Service layer / HTTP API.** The `Omnigraph` struct IS the cache — a service just keeps it alive across requests. The tiered caching model (memory / local disk / S3) works at the library level. A service adds request routing, auth, and connection pooling.

---

## Verification Gates

Each step has a concrete "it works when..." gate:

| Step | Gate |
|---|---|
| 1 | `build_catalog()` produces Utf8 id/src/dst schemas. Existing parser/typecheck tests pass. |
| 2 | `ManifestCoordinator::init()` creates repo at a URI. `state()` reads back correct entries. `commit()` advances version. |
| 3 | `omnigraph init --schema ./test.pg /local/test` creates a valid repo. `open` reads it back. |
| 4 | `omnigraph load --data ./test.jsonl /local/test` populates per-type tables. Data round-trips. |
| 5 | `omnigraph run get_person --param name=Alice /local/test` returns correct result from Lance via DataFusion. |
| 6 | `omnigraph run friends_of --param name=Alice /local/test` does multi-hop traversal via CSR. `warm()` pre-populates cache. |
| 7 | Mutation queries insert/update/delete entities. Delete cascades edges. Graph index invalidated. |
| 8 | `search()`, `fuzzy()`, `bm25()` return indexed results from Lance FTS. |
| 9 | Branch, write on branch, merge back. Main sees merged data. Graph index per branch works. |
| 10 | `omnigraph changes --since 0` shows all mutations. `omnigraph diff 1 3` shows changes between versions. |

**End-to-end gate:** Both test fixtures (`test.pg`/`test.jsonl`/`test.gq` and `signals.pg`/`signals.jsonl`) load, query, traverse, and mutate correctly through the CLI — against both local and S3 URIs.
