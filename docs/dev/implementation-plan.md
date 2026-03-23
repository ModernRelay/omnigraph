# Omnigraph Implementation Plan

Living document tracking the build of the Lance-native graph database.

---

## Principles

1. **Build each layer correctly once.** No temporary implementations that get replaced.
2. **Lance-native from the start.** Manifest is a Lance table. Stable row IDs on every dataset. Shallow clone branching. No custom WAL, no custom JSON manifest, no custom CDC.
3. **Carry forward the compiler.** The schema parser, query parser, typechecker, and IR lowering are proven and unchanged. Everything below the IR is new.
4. **String IDs everywhere.** `@key` values or ULIDs. No `u64` counters, no ID coordination across branches.
5. **Snapshot consistency.** A manifest version IS a database version. All reads within a query go through one immutable Snapshot.
6. **URIs not paths.** Every `open`/`init` accepts `&str` (local path or `s3://`). Lance handles both.

---

## Current Status

**178 tests passing.** Steps 0–5 complete. Next: Snapshot refactor → Step 6 (graph traversal).

```
Step 0  ✅  Crate restructuring
Step 1  ✅  Foundation types (String IDs)
Step 2  ✅  ManifestCoordinator
Step 3  ✅  Omnigraph init/open
Step 4  ✅  Data loading
Step 5  ✅  Query execution (tabular)
Step 5a ▶  Snapshot refactor (read consistency)
Step 6     Graph index + traversal
Step 7     Mutations
Step 8     Search predicates
Step 9     Branching
Step 10    Change tracking + CLI
```

---

## Target Architecture

```
graph-db/                                 # repo root (local path or s3:// URI)
├── _schema.pg                            # Schema source of truth
├── _manifest.lance/                      # Lance table: one row per sub-table
│   ├── _versions/                        # MVCC — repo version = manifest version
│   ├── _refs/branches/                   # Lance-native branch metadata
│   ├── _refs/tags/                       # Lance-native tag metadata
│   └── tree/{branch}/                    # Branch-specific manifest versions
├── nodes/
│   ├── {hash}/                           # Per-type dataset (FNV-1a hash of type name)
│   │   ├── _versions/
│   │   ├── data/
│   │   ├── _indices/
│   │   └── tree/{branch}/               # Lazily created on first branch write
│   └── ...
└── edges/
    ├── {hash}/
    └── ...
```

**Manifest table schema:**

| Column | Type | Description |
|---|---|---|
| `table_key` | Utf8 | `"node:Person"` or `"edge:Knows"`. Unenforced primary key. |
| `table_path` | Utf8 | `"nodes/a1b2c3d4..."` — relative to repo root, stable hash |
| `table_version` | UInt64 | Pinned Lance version for this snapshot |
| `table_branch` | Utf8 (nullable) | Lance branch name on sub-table (null = main) |
| `row_count` | UInt64 | Rows in sub-table at this version |

All datasets created with `enable_stable_row_ids = true` and `LanceFileVersion::V2_2`.

---

## Crate Structure

```
crates/
├── omnigraph-compiler/        # No Lance dependency. 149 tests. Compiles in seconds.
│   └── src/
│       ├── schema/            # .pg parser, AST, pest grammar
│       ├── query/             # .gq parser, AST, typechecker, pest grammar
│       ├── catalog/           # Catalog, build_catalog (no schema_ir — dropped)
│       ├── ir/                # IR types, lowering
│       ├── types.rs           # ScalarType, PropType, Direction
│       ├── error.rs           # NanoError, ParseDiagnostic
│       ├── embedding.rs       # OpenAI embedding client
│       ├── json_output.rs     # Arrow → JSON
│       ├── result.rs          # QueryResult, MutationResult, MutationExecResult
│       └── query_input.rs     # Named query lookup, param parsing
│
├── omnigraph/                 # The database. Lance-dependent. 29 tests.
│   └── src/
│       ├── db/                # Omnigraph, ManifestCoordinator
│       │   ├── mod.rs
│       │   ├── manifest.rs    # ManifestCoordinator — Lance table CRUD, commit protocol
│       │   └── omnigraph.rs   # Omnigraph struct — init, open, snapshot, run_query
│       ├── exec/              # Query executor
│       │   └── mod.rs         # IR pipeline execution, Lance scan, Arrow filter/project/sort
│       └── loader/            # JSONL → per-type Lance tables
│           └── mod.rs         # Parse JSONL, build RecordBatches, write to Lance, commit
│
└── omnigraph-cli/             # Binary (stubbed, wired in Step 10)
    └── src/
        └── main.rs
```

---

## Completed Steps

### Step 0: Crate Restructuring ✅

Moved compiler frontend from `omnigraph-engine` into `omnigraph-compiler`. Created `omnigraph` crate with `OmniError`. Deleted `omnigraph-engine` (~14,000 lines) and `omnigraph-repo` (~430 lines). CLI stubbed.

**Deviations from plan:**
- `DataFusion` error variant removed from compiler's `NanoError` (no datafusion dep in compiler)
- `MutationExecResult` moved into `result.rs` (was in deleted `plan/physical.rs`)
- `embedding.rs` test counted as compiler test (147 total, not 146)

**Tests:** 147 compiler tests pass.

### Step 1: Foundation Types ✅

Removed `NodeId = u64` / `EdgeId = u64` aliases. Changed Arrow schemas: node `id` from `UInt64` → `Utf8`, edge `id`/`src`/`dst` from `UInt64` → `Utf8`. Added `key_property: Option<String>` to `NodeType`.

**Tests:** +2 new (`test_id_fields_are_utf8`, `test_key_property_tracking`) → 149 compiler tests.

### Step 2: ManifestCoordinator ✅

Built `db/manifest.rs` (~300 lines). `ManifestCoordinator` manages a Lance table at `_manifest.lance/`. `init()` creates per-type empty Lance datasets + manifest table. `state()` reads manifest. `commit()` does `merge_insert` keyed by `table_key`. `open_sub_table()` opens a sub-table at its pinned version.

**Deviations from plan:**
- Used `MergeInsertBuilder::try_new()` + `try_build()` + `execute()` pattern (not the fluent `ds.merge_insert()` which doesn't exist on `Arc<Dataset>`)
- Added `lance-datafusion` as dependency for `reader_to_stream` in merge_insert

**Tests:** +5 new → 154 total.

### Step 3: Omnigraph Init/Open ✅

Built `db/omnigraph.rs` (~120 lines). `Omnigraph::init()` parses schema, writes `_schema.pg`, creates ManifestCoordinator. `Omnigraph::open()` reads schema, rebuilds catalog, opens manifest.

**Deviations from plan:**
- No `Session` or `QueryExecutor` yet — deferred to when they're needed
- File I/O uses `std::fs` directly (S3 support deferred to when Lance object_store is wired)

**Tests:** +3 new → 157 total.

### Step 4: Data Loading ✅

Built `loader/mod.rs` (~400 lines). Parses JSONL, builds per-type `RecordBatch` vectors with String IDs, writes to Lance datasets, commits manifest atomically. Supports Overwrite/Append/Merge modes. Node `id` = `@key` value or ULID. Edge `id` = ULID, `src`/`dst` = node key values. Uses `ulid` crate for robust ID generation.

**Deviations from plan:**
- Wrote loader from scratch rather than adapting old `jsonl.rs` (old code was coupled to `u64` IDs and `GraphStorage`)
- Old loader files (`jsonl.rs`, `constraints.rs`, `embeddings.rs`) remain as dormant files in `src/loader/` for reference
- Updated `test.pg` fixture to add `@key` annotations (correct by design — no implicit name-based lookups)

**Tests:** +6 unit + 11 integration (end_to_end.rs) → 178 total.

### Step 5: Query Execution (Tabular) ✅

Built `exec/mod.rs` (~400 lines). Executes IR pipeline: `NodeScan` → Lance scan with SQL filter pushdown, `Filter` → Arrow comparison with auto type casting, projection via `PropAccess`, ordering via `lexsort_to_indices`, limit via batch slicing. Added `Omnigraph::run_query()`.

**Deviations from plan:**
- Used Lance `Scanner` API directly with SQL filter strings instead of DataFusion `SessionContext` + `LanceTableProvider`. Simpler, fewer dependencies, sufficient for current query patterns.
- No separate `exec/scan.rs` / `exec/filter.rs` files — all in `exec/mod.rs` for now
- Discovered param names are stored without `$` prefix (parser strips it)

**Tests:** +4 query tests in end_to_end.rs (`get_person`, `not_found`, `adults`, `top_by_age`) → 178 total.

---

## Next Steps

### Step 5a: Snapshot Refactor

**Why:** The current executor calls `db.state()` per NodeScan. For multi-type queries (graph traversal), this breaks cross-type consistency — concurrent writes between scans would produce inconsistent reads.

**What:** Extract `Snapshot` as an immutable read view from `ManifestState`. A Snapshot is taken once at query start and threaded through all read operations.

```rust
pub struct Snapshot {
    root_uri: String,
    version: u64,
    entries: HashMap<String, SubTableEntry>,
}

impl Snapshot {
    pub async fn open(&self, table_key: &str) -> Result<Dataset>;
}

impl Omnigraph {
    pub async fn snapshot(&self) -> Result<Snapshot>;
    pub async fn refresh(&mut self) -> Result<()>;  // see other writers' commits
}
```

**Changes:**
- `ManifestState` → `Snapshot` with `open()` method (combines state + open_sub_table)
- `execute_query_ir()` takes `&Snapshot` instead of `&Omnigraph`
- `execute_node_scan()` takes `&Snapshot` instead of calling `db.state()` each time
- Loader continues using `ManifestCoordinator` directly for writes

**Tests:** Existing tests pass unchanged. Add test verifying snapshot version is pinned across multiple reads.

**~50 lines changed, 0 new files.**

### Step 6: Graph Index + Traversal

The part that makes it a graph database. No Lance equivalent exists — this is custom.

**New modules:**
- `graph_index/mod.rs` — `GraphIndex` with `TypeIndex` (String→u32 mapping), CSR/CSC indices
- `graph_index/csr.rs` — CSR with u32 dense indices (~40 lines, same algorithm as deleted `store/csr.rs`)

**`GraphIndex` build process** (lazy, on first traversal query):
1. For each edge type: scan `(src, dst)` columns from Snapshot
2. Build `TypeIndex` for source and destination node types (String → dense u32)
3. Build CSR (outgoing) and CSC (incoming)
4. Cache keyed by `(branch, manifest_version)`

**`Expand` execution:**
1. Extract source String IDs from input batch
2. Map to dense u32 via `TypeIndex`
3. BFS/DFS on CSR/CSC (sub-millisecond, in-memory)
4. Map result u32 back to String IDs
5. Hydrate destination nodes from Snapshot

**`AntiJoin` execution:**
1. Execute inner pipeline (produces a set of IDs)
2. Filter outer binding: keep rows where ID is NOT in the inner set

**Test:** Execute `friends_of`, `friends_of_friends`, `employees_of`, `unemployed` from `test.gq`.

**~400 lines new (graph index), ~200 lines new (traverse/anti-join in exec).**

### Step 7: Mutations

Insert, update, delete with schema-driven edge cascade.

**Insert:** Generate `id` (`@key` or ULID), build RecordBatch, append to sub-table, commit manifest.

**Update:** `merge_insert` on sub-table keyed by `id`, commit manifest.

**Delete with cascade:** Query sub-table for matching IDs, Lance `delete()` (deletion vectors), cascade to edge sub-tables where `src` or `dst` matches, commit manifest, invalidate graph index cache.

**Test:** Insert a Person, query it back. Update a property, verify. Delete a Person, verify cascade.

**~300 lines new.**

### Step 8: Search Predicates

Replace brute-force search with Lance FTS.

- `search()` → Lance FTS `contains_tokens`
- `fuzzy()` → Lance n-gram FTS index
- `match_text()` → Lance FTS `phrase` query
- `bm25()` → Lance FTS `match` with BM25 scoring
- `nearest()` → Lance ANN via `scanner.nearest()`
- `rrf()` → application-level score fusion

**~300 lines new.**

### Step 9: Branching

Lance-native branching on the manifest table + lazy sub-table branching.

**Create branch:** Create Lance branch on `_manifest.lance` (zero-copy). No sub-tables branched.

**First write on branch:** Create Lance branch on the sub-table (shallow clone via `base_paths`). Write to the branched sub-table. Update manifest row with `table_branch`.

**Read from branch:** Open manifest at the branch → Snapshot → open sub-tables at pinned versions.

**Merge:** Diff using `_row_created_at_version` / `_row_last_updated_at_version`. Apply via `merge_insert` keyed by `id`.

**~500 lines new.**

### Step 10: Change Tracking + CLI

- `omnigraph changes --since N` — query `_row_created_at_version` / `_row_last_updated_at_version`
- `omnigraph diff V1 V2` — version range changes
- CLI commands: `init`, `load`, `run`, `branch create/list`, `snapshot`, `describe`, `compact`, `export`

**~400 lines new.**

---

## Dependency Graph

```
Step 0  ✅ (crate restructuring)
  └→ Step 1 ✅ (types)
       └→ Step 2 ✅ (manifest coordinator)
            ├→ Step 3 ✅ (Omnigraph init/open)
            │    ├→ Step 4 ✅ (loader)
            │    │    └→ Step 8 (search indexes created during init/load)
            │    ├→ Step 5 ✅ (query execution — tabular)
            │    │    └→ Step 5a ▶ (snapshot refactor)
            │    │         └→ Step 6 (graph index + traversal)
            │    └→ Step 7 (mutations)
            └→ Step 9 (branching)
                 └→ Step 10 (changes, diff, CLI polish)
```

**Critical path:** 5a → 6 → 7 (snapshot → traversal → mutations)

Steps 7, 8, 9 can proceed in parallel after 5a.

---

## Test Summary

| Step | Tests | Running Total |
|---|---|---|
| 0 | 147 carried forward | 147 |
| 1 | +2 (Utf8 schemas, @key tracking) | 149 |
| 2 | +5 (manifest CRUD) | 154 |
| 3 | +3 (init/open) | 157 |
| 4 | +6 unit, +11 integration | 178 |
| 5 | +4 query tests (in end_to_end.rs) | 178 (included above) |
| 5a | ~1 new | ~179 |
| 6 | ~6 new | ~185 |
| 7 | ~4 new | ~189 |
| 8 | ~4 new | ~193 |
| 9 | ~6 new | ~199 |
| 10 | ~3 new | ~202 |

**Current: 178 tests passing.** Target: ~202 at completion.

---

## Deferred

1. **Remote sync (clone/push/pull).** URI-based open + `base_paths` make this architecturally possible. Deferred to after local branching works.
2. **Schema evolution / migration.** Lance `add_columns`/`alter_columns`/`drop_columns` when needed.
3. **Hook system.** Thin dispatch layer on Lance version tracking. Requires Step 10.
4. **Compaction.** Lance handles natively. Wire up as CLI command.
5. **SDKs (TS, Swift, Python).** Thin wrappers after API stabilizes.
6. **DuckDB fallback.** For graphs > ~5M edges. Defer until scale demands.
7. **MemWAL.** For high-throughput streaming ingest. Branch-per-writer handles multi-agent concurrency without it.
8. **Service layer / HTTP API.** `Omnigraph` struct IS the cache — a service just keeps it alive.
