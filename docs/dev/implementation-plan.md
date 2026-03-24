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

**262 tests passing.** Steps 0–8 + 7a complete.

```
Step 0  ✅  Crate restructuring
Step 1  ✅  Foundation types (String IDs)
Step 2  ✅  ManifestCoordinator
Step 3  ✅  Omnigraph init/open
Step 4  ✅  Data loading
Step 5  ✅  Query execution (tabular)
Step 5a ✅  Snapshot refactor (read consistency)
Step 6  ✅  Graph index + traversal + Lance-native optimizations
Step 7  ✅  Mutations (insert/update/delete + edge cascade)
Step 8  ✅  Search predicates (FTS inverted indices)
Step 7a ✅  Constraint system restructuring (interfaces, body constraints, cardinality, value constraints)
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
├── omnigraph-compiler/        # No Lance dependency. 149 tests.
│   └── src/
│       ├── schema/            # .pg parser, AST, pest grammar
│       ├── query/             # .gq parser, AST, typechecker, pest grammar
│       ├── catalog/           # Catalog, build_catalog
│       ├── ir/                # IR types (QueryIR, MutationIR), lowering
│       ├── types.rs           # ScalarType, PropType, Direction
│       ├── error.rs           # NanoError, ParseDiagnostic
│       ├── embedding.rs       # OpenAI embedding client
│       ├── json_output.rs     # Arrow → JSON
│       ├── result.rs          # QueryResult, MutationResult, RunResult
│       └── query_input.rs     # Named query lookup, param parsing
│
├── omnigraph/                 # The database. Lance-dependent. 35 tests.
│   └── src/
│       ├── db/                # Omnigraph, ManifestCoordinator, Snapshot
│       │   ├── mod.rs
│       │   ├── manifest.rs    # Snapshot, ManifestCoordinator
│       │   └── omnigraph.rs   # Omnigraph handle — init, open, snapshot, run_query, graph_index, ensure_indices
│       ├── exec/              # Pure function executor
│       │   └── mod.rs         # execute_query, execute_pipeline, execute_expand, execute_anti_join, hydrate_nodes
│       ├── graph_index/       # Topology-only graph index
│       │   └── mod.rs         # GraphIndex, TypeIndex, CsrIndex
│       └── loader/            # JSONL → per-type Lance tables
│           └── mod.rs         # load_jsonl, load_jsonl_file, build_node_batch, write_batch_to_dataset
│
└── omnigraph-cli/             # Binary (stubbed, wired in Step 10)
    └── src/
        └── main.rs
```

---

## Completed Steps

### Step 0: Crate Restructuring ✅

Moved compiler frontend into `omnigraph-compiler`. Created `omnigraph` crate. Deleted `omnigraph-engine` + `omnigraph-repo`. Tests: 147.

### Step 1: Foundation Types ✅

String IDs everywhere: node `id` Utf8, edge `id`/`src`/`dst` Utf8. `key_property: Option<String>` on `NodeType`. Tests: 149.

### Step 2: ManifestCoordinator ✅

`_manifest.lance` table: init, state, commit (merge_insert by table_key), open_sub_table. Tests: 154.

### Step 3: Omnigraph Init/Open ✅

`Omnigraph::init()` writes `_schema.pg` + creates manifest. `Omnigraph::open()` reads schema + opens manifest. Tests: 157.

### Step 4: Data Loading ✅

JSONL loader: per-type RecordBatch building, Overwrite/Append/Merge modes, `@key` → id or ULID, atomic manifest commit. Tests: 178.

### Step 5: Query Execution (Tabular) ✅

Lance Scanner with SQL filter pushdown, Arrow filter/project/sort, limit. Tests: 178.

### Step 5a: Snapshot Refactor ✅

`Snapshot` struct (immutable, no I/O). `ManifestCoordinator` stores `known_state`, `snapshot()` sync, `refresh()` async. Executor is pure function: `execute_query(ir, params, snapshot, graph_index, catalog)`. Removed `state()`, `open_sub_table()`, `manifest()` from public API. Tests: 179.

### Step 6: Graph Index + Traversal ✅

`GraphIndex` with `TypeIndex` (String→u32), `CsrIndex` (CSR adjacency). Two-phase build: scan all edges → grow TypeIndices → build CSR/CSC with final sizes. `Expand` (BFS with hop bounds, cross-type aware visited). `AntiJoin` (per-row fallback + bulk CSR fast path). Node hydration via Lance `IN` filter. BTree scalar indices on `id`/`src`/`dst` via `ensure_indices()` after load. Graph index cached on `Omnigraph` handle. Tests: 184.

---

## Next Steps

### Step 7: Mutations ✅

Insert, update, delete nodes and edges with schema-driven edge cascade. **Complete.**

- `Omnigraph::run_mutation()` in `exec/mod.rs` — parse, typecheck, lower, dispatch
- Insert: single-row RecordBatch, upsert for `@key` types via `merge_insert`, append for keyless. Edge inserts invalidate graph index.
- Update: scan with predicate filter → apply assignments → `merge_insert` keyed by `id`. Rejects `@key` property updates.
- Delete: node deletes cascade to all referencing edge types via `src IN (...) OR dst IN (...)` filters. Edge deletes have no cascade. Graph index invalidated.
- 7 mutation tests + supporting tests in end_to_end.rs, consistency.rs, traversal.rs
- Tests: +25. Running total: 209.

---

### Step 7a: Constraint System Restructuring ✅

Interfaces, body-level constraints, edge cardinality, value constraints. **Complete.**

**Grammar** (`schema.pest`): `interface_decl`, `implements_clause`, `body_constraint` (`@key(name)`, `@unique(a, b)`, `@index(a, b)`, `@range(prop, min..max)`, `@check(prop, "regex")`), `cardinality` (`@card(0..1)`). Negative lookahead on `annotation` rule to prevent constraint keywords from being consumed as annotations. Inheritance syntax replaced with `implements`.

**AST** (`ast.rs`): `InterfaceDecl`, `Constraint` enum (Key/Unique/Index/Range/Check), `ConstraintBound` (Integer/Float), `Cardinality`. `SchemaDecl::Interface` variant. `NodeDecl`: `implements`, `constraints`, `parent` removed. `EdgeDecl`: `constraints`, `cardinality`.

**Parser** (`parser.rs`): `parse_interface_decl`, `parse_body_constraint`, `parse_cardinality`, `resolve_interfaces` (verify/inject interface properties, detect type conflicts), `desugar_property_constraints` (property-level `@key` → `Constraint::Key`), `validate_constraints` (typed constraint validation), `validate_property_annotations` (extracted, simplified). `@unique`/`@index` now allowed on edge properties.

**Catalog** (`catalog/mod.rs`): `NodeType` — `key: Option<Vec<String>>`, `unique_constraints`, `indices: Vec<Vec<String>>`, `range_constraints`, `check_constraints`, `implements`. `key_property()` convenience method. `EdgeType` — `cardinality`, `unique_constraints`, `indices`. `Catalog` — `interfaces: HashMap<String, InterfaceType>`. Three-pass `build_catalog` (interfaces → nodes → edges). `@key` implies index.

**Runtime**: `validate_value_constraints()` in loader (range + check, strict errors). `validate_edge_cardinality()` after edge writes. `ensure_indices()` reads from `node_type.indices`. `key_property()` method calls in loader + executor.

**Tests**: +32. Running total: 262.

---

### Step 8: Search Predicates ✅

Lance-native indexed search: FTS, fuzzy, phrase match, BM25 scoring, vector ANN, RRF score fusion. **Complete.**

- `SearchMode` extraction in `exec/mod.rs` — dispatches `nearest()`, `bm25()`, `rrf()` from query IR
- `search()`/`fuzzy()`/`match_text()` → `lance_index::scalar::FullTextSearchQuery` via `scanner.full_text_search()`
- `nearest()` → `scanner.nearest(column, vector, k)` for Lance ANN
- `bm25()` → FTS with BM25 scoring via `FullTextSearchQuery`
- `rrf()` → app-level reciprocal rank fusion of two sub-queries (nearest + bm25)
- `ensure_indices()` extended: Inverted indices on `@index` String properties
- Tests: +7. Running total: 216.

---

### Step 9: Branching

Lance-native branching on the manifest table + lazy sub-table branching.

#### Lance Branch APIs Available

From `lance::Dataset` (verified in docs):

```rust
// Create a new branch (zero-copy — inherits all data)
ds.create_branch(branch_name: &str).await?;

// Open dataset at a specific branch
let ds = Dataset::open(uri).await?;
let ds = ds.checkout_branch(branch_name).await?;

// List branches
let branches: Vec<String> = ds.list_branches().await?;

// Delete a branch
ds.delete_branch(branch_name).await?;

// Shallow clone (cross-location, uses base_paths)
ds.shallow_clone(dest_uri, base_paths).await?;
```

#### Design

**Create branch** (`Omnigraph::branch_create`):
1. Open `_manifest.lance` dataset
2. `ds.create_branch("experiment")` — zero-copy, inherits all manifest rows
3. No sub-tables branched yet — `table_branch` remains null in all manifest rows

**Open at branch** (`Omnigraph::open_branch` or modify `open`):
1. Open `_manifest.lance`
2. `ds.checkout_branch("experiment")`
3. Read manifest state → build Snapshot
4. For each sub-table: if `table_branch` is null, open at `table_version` on main. If set, `ds.checkout_branch(table_branch)`.

**First write on branch** (in mutation/load path):
1. Check if sub-table has been branched (entry's `table_branch` is null)
2. If not: open sub-table, `ds.create_branch(branch_name)`, `ds.checkout_branch(branch_name)`
3. Write data to the branched sub-table
4. Update manifest row: set `table_branch = branch_name`, commit manifest on the branch

**Merge** (`Omnigraph::branch_merge`):
1. Read source branch manifest and target manifest
2. For each sub-table that differs:
   - Open both versions
   - Use `merge_insert` keyed by `id` to apply source changes to target
3. Re-validate `unique()` constraints after merge (post-merge scan)
4. Commit target manifest with new sub-table versions

**Note:** Edge cardinality is validated per-branch only. Cross-branch cardinality semantics after merge are deferred.

#### Files to Modify

**`crates/omnigraph/src/db/omnigraph.rs`** (~100 lines):
- `Omnigraph::branch_create(&mut self, name: &str) -> Result<()>`
- `Omnigraph::open_branch(uri: &str, branch: &str) -> Result<Self>` (or add `branch` param to `open`)
- `Omnigraph::branch_list(&self) -> Result<Vec<String>>`

**`crates/omnigraph/src/db/manifest.rs`** (~80 lines):
- `ManifestCoordinator::create_branch(&mut self, name: &str) -> Result<()>`
- `ManifestCoordinator::open_at_branch(uri: &str, branch: &str) -> Result<Self>`
- `Snapshot::open()` must handle `entry.table_branch` — checkout branch on sub-table if set

**`crates/omnigraph/src/loader/mod.rs`** (~30 lines):
- On first write to a sub-table on a branch: create sub-table branch, set `table_branch` in manifest

#### Test Cases

```rust
// 1. Create branch, verify it exists
db.branch_create("experiment").await?;
let branches = db.branch_list().await?;
assert!(branches.contains(&"experiment".to_string()));

// 2. Write to branch, verify main is unaffected
let mut branch_db = Omnigraph::open_branch(uri, "experiment").await?;
// insert a Person on the branch
// verify it's visible on the branch
// reopen main — verify the person is NOT there

// 3. Merge branch back
db.branch_merge("experiment").await?;
// verify the person is now visible on main

// 4. Branch read isolation
// load data on main after branching — branch shouldn't see it until refresh
```

**Expected: ~210 lines across manifest/omnigraph/loader, ~80 lines in tests. Total: ~249 tests.**

---

### Step 10: Change Tracking + CLI

#### Change Tracking

Lance stable row IDs enable `_rowid` tracking. Version metadata on fragments enables change detection:

**Changes since version N:**
1. Compare manifest at version N vs current → find which sub-tables changed
2. For each changed sub-table: scan with version-based filters

**Diff between versions:**
1. Checkout manifest at V1 and V2
2. For each sub-table that differs: scan both, compute row-level diff by `id`

#### CLI Commands

**File: `crates/omnigraph-cli/src/main.rs`** — currently stubbed, all commands print "not yet implemented".

Wire each command to the `Omnigraph` API:

| Command | Implementation |
|---|---|
| `omnigraph init --schema <file> <uri>` | `Omnigraph::init(uri, &fs::read_to_string(schema)?)` |
| `omnigraph load --data <file> <uri>` | `Omnigraph::open(uri)` → `load_jsonl_file(&mut db, data, LoadMode::Merge)` |
| `omnigraph run --query <file> --name <name> [--param key=val] <uri>` | `db.run_query(source, name, &params)` → print JSON |
| `omnigraph branch create <name> <uri>` | `db.branch_create(name)` |
| `omnigraph branch list <uri>` | `db.branch_list()` → print |
| `omnigraph snapshot [--branch <b>] [--json] <uri>` | `db.snapshot()` → print entries |
| `omnigraph describe <uri>` | Print schema + catalog + constraints + cardinality + manifest summary |
| `omnigraph compact <uri>` | `Dataset::compact_files()` on each sub-table |

**Tokio runtime:** The CLI main function needs `#[tokio::main]` since all Omnigraph methods are async. Currently uses sync `fn main()`.

**Param parsing for `run`:** Parse `--param name=value` pairs into `ParamMap`. Handle `$` prefix stripping (params stored without `$`). Detect type from value format (quoted → String, numeric → Integer, etc.) or accept `--param-json` for typed params.

#### Files to Modify

**`crates/omnigraph-cli/src/main.rs`** (~200 lines):
- Add `#[tokio::main]` to main
- Wire each subcommand to the Omnigraph API
- Add `Run` subcommand with `--query`, `--name`, `--param` args
- Add `Describe` subcommand
- JSON output formatting for query results

**`crates/omnigraph/src/db/omnigraph.rs`** (~30 lines):
- `Omnigraph::describe(&self) -> String` — human-readable summary

#### Test Cases

CLI tests can be integration tests using `assert_cmd` crate, or just verify the API layer works:

```rust
// 1. Init via CLI args → open → verify
// 2. Load via CLI → query → verify results
// 3. Branch create via CLI → list → verify
```

**Expected: ~230 lines in CLI, ~30 lines in omnigraph.rs. Total: ~254 tests.**

---

## Dependency Graph

```
Steps 0–8 + 7a ✅ (262 tests)
     ├→ Step 9 (branching)
     │    └→ Step 10 (CLI — needs branching)
     └→ Step 10 (CLI core wiring)
```

**Critical path:** Step 9 → Step 10.

---

## Test Summary

| Step | Tests | Running Total |
|---|---|---|
| 0 | 147 carried forward | 147 |
| 1 | +2 (Utf8 schemas, key tracking) | 149 |
| 2 | +5 (manifest CRUD) | 154 |
| 3 | +3 (init/open) | 157 |
| 4 | +6 unit, +11 integration | 178 |
| 5 | +4 query tests (in end_to_end.rs) | 178 |
| 5a | +1 (snapshot pinning) | 179 |
| 6 | +5 (traversal + anti-join + optimizations) | 184 |
| 7 | +25 (insert/update/delete/cascade/mutations) | 209 |
| 8 | +21 (search/fuzzy/nearest/bm25/rrf/indices/blobs) | 230 |
| 7a | +32 (interfaces, body constraints, cardinality, value constraints) | 262 |
| 9 | ~8 (branch create/read/merge) | ~270 |
| 10 | ~5 (CLI) | ~275 |

**Current: 262 tests passing.** Target: ~275 at completion.

---

## Key Patterns to Follow

**Writing to Lance** — follow `loader/mod.rs:345-413` (`write_batch_to_dataset`):
- Append: `ds.append(reader, None)`
- Merge: `MergeInsertBuilder::try_new(Arc::new(ds), vec!["id".to_string()])`
- Overwrite: `Dataset::write(reader, uri, Some(WriteParams { mode: WriteMode::Overwrite, ... }))`

**Building RecordBatches** — follow `loader/mod.rs:188-251` (`build_node_batch`, `build_edge_batch`):
- Schema from `catalog.node_types[name].arrow_schema`
- `id` column first, then properties

**Lance SQL filters** — follow `exec/mod.rs:399-443` (`build_lance_filter`, `literal_to_sql`):
- String values: `format!("'{}'", s.replace('\'', "''"))`
- IN filters: `format!("id IN ({})", escaped.join(", "))`

**Manifest commit** — follow `loader/mod.rs:182-184`:
- Collect `SubTableUpdate` per changed table
- `db.manifest_mut().commit(&updates).await?`

**Graph index invalidation** — set `self.cached_graph_index = None` after any edge mutation (field on `Omnigraph` struct in `db/omnigraph.rs:26`).

**Error handling** — `OmniError::Lance(e.to_string())` for Lance errors, `OmniError::Manifest(msg)` for logic errors, `OmniError::Compiler(e)` for compiler errors (auto via `From`).

**Value constraint validation** (after Step 7a) — follow `loader/mod.rs:validate_value_constraints()`:
- Before writing to Lance, iterate `node_type.range_constraints` and `node_type.check_constraints`
- For each constraint, validate the corresponding column in the RecordBatch
- Return `OmniError::Manifest("constraint violation: ...")` on failure

---

## Deferred

1. **Remote sync (clone/push/pull).** URI-based open + `base_paths` make this architecturally possible. Deferred to after local branching works.
2. **Schema evolution / migration.** Lance `add_columns`/`alter_columns`/`drop_columns` when needed.
3. **Hook system.** Thin dispatch layer on Lance version tracking. Requires Step 10.
4. **Compaction.** Lance handles natively. Wire up as CLI command.
5. **SDKs (TS, Swift, Python).** Thin wrappers after API stabilizes.
6. **DuckDB fallback.** For graphs > ~5M edges. Defer until scale demands.
7. **MemWAL — Streaming concurrent writes on a single branch.** Lance MemWAL is an LSM-tree architecture enabling high-throughput streaming writes. Each sub-table dataset gets its own MemWAL with regions (one active writer per region, epoch-fenced). The same `append`/`merge_insert`/`delete` API surface works transparently — MemWAL routes writes through a durable WAL internally. Omnigraph integration requires: (1) enable MemWAL on sub-table datasets, (2) region assignment by `id` column using `bucket(id, N)` transform, (3) manifest commit batching — replace per-mutation commit with periodic checkpoint via `ManifestCoordinator`, (4) writer handle pool with region-aware API and epoch-based fencing. Read path unchanged — `Snapshot::open()` includes flushed MemTable data via Lance's LSM-tree merging read. Graph index builds from merged scan results. See `omnigraph-specs.md` Concurrency Model and Consistency Model sections for full architecture and read consistency spectrum. **Trigger:** per-mutation commit latency >10ms or throughput <100 writes/sec. **Prerequisite:** Step 7 (complete) + Step 9 (branching). The `run_mutation()` API surface does not change — buffering and MemWAL are internal to the write path.
8. **Service layer / HTTP API.** `Omnigraph` struct IS the cache — a service just keeps it alive.
9. **Binary ULIDs.** Switch `id` from Utf8 to `FixedSizeBinary(16)` for performance. See `omnigraph-specs.md` Identity Model section. Trigger: profile TypeIndex build in production-scale graphs.
10. **Row-correlated bindings.** The executor uses flat per-variable RecordBatches. Multi-variable returns across traversal hops break when row counts differ. Needs tuple-based binding model for v0.2.0.
11. **take_rows() hydration.** Use Lance stable row ID addresses for O(1) node hydration instead of `IN (...)` filter. Deferred — scalar indices already make the IN filter fast.
12. **Composite key enforcement at Lance level.** Step 7a adds composite key syntax (`@key(tenant, slug)`) to the schema language, but the runtime still uses a single `id` column. Full composite key support (multi-column unique constraint at the Lance level) is deferred.
13. **Cross-branch cardinality validation.** Edge cardinality is validated per-branch. Cross-branch cardinality semantics after merge are deferred.
14. **Polymorphic interface queries.** Phase 1 (Step 7a) treats interfaces as compile-time property verification. Phase 2 enables `$e: InterfaceName` in queries (multi-dataset scan + union) and interface types as edge targets. Requires executor and graph index changes.
