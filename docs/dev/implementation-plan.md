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

**283 tests passing.** Steps 0–9 complete. Step 10 is next.

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
Step 8a ✅  Runtime alignment before branching
Step 8b ✅  Graph write correctness before branching
Step 9  ✅  Branching
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

Lance-native search runtime: FTS, fuzzy search, `match_text()` fallback on the FTS path, BM25 scoring, vector nearest-neighbor ordering, and RRF score fusion. **Complete for the Step 8 runtime scope.**

- `SearchMode` extraction in `exec/mod.rs` — dispatches `nearest()`, `bm25()`, `rrf()` from query IR
- `search()`/`fuzzy()`/`match_text()` → `lance_index::scalar::FullTextSearchQuery` via `scanner.full_text_search()`
- `nearest()` → `scanner.nearest(column, vector, k)` for Lance ANN
- `bm25()` → FTS with BM25 scoring via `FullTextSearchQuery`
- `rrf()` → app-level reciprocal rank fusion of two sub-queries (nearest + bm25)
- `ensure_indices()` extended: Inverted indices on `@index` String properties
- Tests: +7. Running total: 216.

---

### Step 8a: Runtime Alignment Before Branching ✅

Lock down correctness and narrow the docs/runtime gap before Step 9 expands the read and write surface with branches. **Complete.**

#### Implemented

**Snapshot + cache correctness:**
- `cached_graph_index` remains a single cache slot, but it is now keyed by edge sub-table state instead of raw handle lifetime
- `refresh()` drops the cached graph index when edge table versions change
- Regression test covers another writer changing edge topology and a refreshed reader rebuilding traversal state

**Search parity cleanup:**
- `ensure_indices()` now creates schema-driven ANN indices for single-column `Vector @index`
- `ensure_indices()` now commits updated sub-table versions back into the manifest so snapshot-pinned reads actually target the indexed dataset version
- `match_text()` remains an explicit FTS fallback because the Lance Rust API still does not expose clean phrase semantics
- Regression tests cover both the documented `match_text()` fallback and vector ANN index creation

**Constraint/materialization parity:**
- Mutation insert/update paths now reuse the loader’s `@range(...)` / `@check(...)` validation before writing
- `@unique(...)` and automatic `@embed` materialization remain narrowed in the public contract/docs; they are not part of the active compiled runtime yet

#### Tests

- +7 tests:
  - refresh-driven graph-index rebuild
  - `match_text()` fallback contract
  - vector ANN index creation
  - mutation insert/update parity for `@range(...)`
  - mutation insert/update parity for `@check(...)`

**Running total: 283 registered tests. Step 8b and Step 9 are complete; Step 10 is next.**

---

### Step 8b: Graph Write Correctness Before Branching ✅

Close the remaining single-graph correctness gaps before adding branch and merge semantics.

#### Scope

- Validate edge endpoint existence and endpoint type before publish
- Change the load path from "write, publish, then validate" to "stage, validate, then publish"
- Add regression tests proving failed graph writes are not made visible

#### Implement

**Endpoint validation:**
- JSONL edge loads must verify `from` / `to` resolve to existing node IDs of the schema-declared endpoint types
- Edge mutation inserts must enforce the same endpoint existence/type rules before commit
- Missing node, wrong node type, or orphaned endpoint is a hard error

**Publish-after-validation load flow:**
- Stage node and edge writes against sub-table datasets first
- Validate endpoint existence and edge cardinality against the staged candidate graph state
- Advance `_manifest.lance` only after validation succeeds
- If staging succeeds but validation fails, old graph state remains visible because the manifest is unchanged

**Non-goals for Step 8b:**
- no branch-aware refactor yet
- no public explicit-target API redesign yet
- no MemWAL / streaming write changes yet

#### Tests

- edge load fails when `from` endpoint is missing
- edge load fails when `to` endpoint is missing
- edge load fails when endpoint ID exists but belongs to the wrong node type
- failed edge load does not advance manifest-visible graph state
- failed cardinality check does not advance manifest-visible graph state
- edge mutation insert rejects invalid endpoints

**Result:** endpoint validation and publish-after-validation are in the active runtime, including edge mutation insert validation and failed-load visibility tests.

---

### Step 9: Branching ✅

Lance-native branching on the manifest table + lazy sub-table branching, with a graph-level three-way merge model. **Complete.**

#### Lance Branch APIs Available

From `lance::Dataset` (verified in lance 3.0.1 Rust API):

```rust
// Create a new branch (zero-copy — inherits all data)
ds.create_branch(branch_name: &str, version, None).await?;

// Open dataset at a specific branch
let ds = Dataset::open(uri).await?;
let ds = ds.checkout_branch(branch_name).await?;

// List branches
let branches: HashMap<String, BranchContents> = ds.list_branches().await?;

// Delete a branch
ds.delete_branch(branch_name).await?;

// Shallow clone (cross-location, uses base_paths)
ds.shallow_clone(dest_uri, base_paths).await?;
```

#### Design

This step replaces the earlier naive two-way `"source rows overwrite target rows via merge_insert"` idea with a graph-level three-way merge. That is the smallest design that is both correct and extensible.

**System metadata: graph commit DAG**

Add a tiny system dataset, `_graph_commits.lance`, to track graph ancestry independently of per-table Lance history.

Suggested columns:

| Column | Type | Purpose |
|---|---|---|
| `graph_commit_id` | Utf8 | Stable graph commit ID (ULID) |
| `manifest_branch` | Utf8 nullable | Manifest branch name (`null` = main) |
| `manifest_version` | UInt64 | Manifest version for this graph commit |
| `parent_commit_id` | Utf8 nullable | First parent |
| `merged_parent_commit_id` | Utf8 nullable | Second parent for merge commits |
| `created_at` | Timestamp | Audit / ordering |

Rules:
- Every manifest-advancing graph write appends one row to `_graph_commits.lance`
- `_graph_commits.lance` is branched eagerly alongside `_manifest.lance` because it is tiny and is required for merge-base resolution
- When opening an older repo that predates Step 9, bootstrap a genesis commit pointing at the current main manifest version

This avoids relying on Lance branch creation metadata alone. Lance's `parent_version` is enough to describe branch creation, but not enough to serve as a complete graph merge-base model after repeated merges.

**Create branch** (`Omnigraph::branch_create`):
1. Open `_manifest.lance` and `_graph_commits.lance`
2. `ds.create_branch("experiment")` on both datasets
3. No sub-tables branched yet — `table_branch` remains null in all manifest rows
4. Branch head in `_graph_commits.lance` initially matches the parent's current head

**Open at branch** (`Omnigraph::open_branch`):
1. Open `_manifest.lance` at the requested branch
2. Open `_graph_commits.lance` at the same branch
3. Read manifest state → build Snapshot
4. For each sub-table: if `table_branch` is null, open at `table_version` on main. If set, checkout that sub-table branch first, then the pinned version

**First write on branch** (in mutation/load path):
1. Check whether the sub-table has already been branched (`table_branch` is null)
2. If not: open sub-table, `ds.create_branch(branch_name)`, `ds.checkout_branch(branch_name)`
3. Write data to the branched sub-table
4. Update manifest row: set `table_branch = branch_name`, commit manifest on the branch
5. Append a new graph commit row to `_graph_commits.lance`

**Merge** (`Omnigraph::branch_merge(source, target)`):
1. Resolve `source_head`, `target_head`, and `merge_base` from `_graph_commits.lance`
2. Open base/source/target manifest states
3. For each sub-table:
   - unchanged on source and target → keep target
   - changed on source only → take source change
   - changed on target only → keep target change
   - changed on both → run row-level merge keyed by persisted `id`
4. Validate the merged candidate graph
5. If there are no conflicts:
   - materialize a full validated candidate batch for each changed sub-table
   - rewrite only the changed target sub-tables from those candidate batches
   - commit target manifest
   - append a merge commit row to `_graph_commits.lance`

**Row merge model**

- Row identity is persisted `id` for both nodes and edges
- Edge topology (`src`, `dst`) is data, not identity
- Endpoint changes are treated as delete + insert, not as in-place semantic updates

**V1 conflict policy (conservative by design)**

Treat the following as conflicts:
- same `id` inserted differently on both source and target
- same `id` updated differently on both source and target
- delete vs update on the same `id`
- any merged edge row whose `src` or `dst` no longer exists in the merged node state
- post-merge `@unique(...)` or `@card(...)` violations

Treat the following as auto-merge:
- source-only insert/update/delete
- target-only insert/update/delete
- identical updates on both sides
- identical deletes on both sides

This is intentionally more conservative than the long-term design. It is easier to trust, and it preserves the same architecture needed for later property-wise auto-merge.

**Validation after row merge**

Validate the merged candidate before publishing it:
- no orphan edges
- `@range(...)` / `@check(...)` on candidate node tables
- `@unique(...)` on candidate tables
- edge cardinality on candidate edge tables

If validation fails, return structured merge conflicts instead of committing partial state.

#### What We Intentionally Address Later

The design above is the durable foundation. These are improvements, not architectural rewrites:

- property-wise auto-merge for disjoint updates on the same `id`
- fast-forward optimization when target is an ancestor of source
- CDC-based / change-set-based diffing instead of full-table scan on doubly changed tables
- incremental `@unique(...)` / `@card(...)` validation instead of rescanning affected tables
- richer conflict reporting and agent-assisted resolution workflows
- broader target selection (arbitrary branch-to-branch merge) if v1 starts with merge-into-current-branch only
- merge-time performance optimizations for very large tables

#### Files to Modify

**`crates/omnigraph/src/db/omnigraph.rs`**:
- `Omnigraph::branch_create(&mut self, name: &str) -> Result<()>`
- `Omnigraph::open_branch(uri: &str, branch: &str) -> Result<Self>`
- `Omnigraph::branch_list(&self) -> Result<Vec<String>>`
- `Omnigraph::branch_merge(&mut self, source: &str, target: &str) -> Result<()>`

**`crates/omnigraph/src/db/manifest.rs`**:
- `ManifestCoordinator::create_branch(&mut self, name: &str) -> Result<()>`
- `ManifestCoordinator::open_at_branch(uri: &str, branch: &str) -> Result<Self>`
- `Snapshot::open()` must handle `entry.table_branch` — checkout branch on sub-table if set

**`crates/omnigraph/src/db/commit_graph.rs`** (new):
- graph commit DAG dataset wrapper
- merge-base lookup
- append normal commit / append merge commit
- bootstrap old repos that do not yet have `_graph_commits.lance`

**`crates/omnigraph/src/loader/mod.rs`**:
- On first write to a sub-table on a branch: create sub-table branch, set `table_branch` in manifest
- Append graph commit rows after successful graph-state commits

**`crates/omnigraph/src/exec/mod.rs`**:
- Append graph commit rows after mutation commits
- Build validated candidate batches for changed tables, then publish them to the target branch

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
db.branch_merge("experiment", "main").await?;
// verify the person is now visible on main

// 4. Conservative conflict: both branches update same id differently
let conflicts = db.branch_merge("experiment", "main").await.unwrap_err();
assert!(conflicts.to_string().contains("same id changed on both branches"));

// 5. Delete vs update conflict
// delete node on source, update same node on target -> conflict

// 6. Orphan edge conflict
// source deletes node, target adds edge referencing it -> conflict

// 7. Bootstrap old repo without _graph_commits.lance
// open old repo -> commit graph is created with a genesis row
```

**Result:** `_graph_commits.lance`, branch-aware `Snapshot::open()`, lazy sub-table branching with retry-on-existing-branch, explicit `branch_merge(source, target)`, and merge-conflict reporting are now in the runtime with integration coverage.

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
Steps 0–9 ✅ (283 tests)
     └→ Step 10 (CLI — needs branching + change tracking)
```

**Critical path:** Step 10.

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
| 8a | +7 (snapshot/cache/search/constraint parity) | 270 |
| 8b | ~5-7 (endpoint validation, staged publish, visibility regressions) | ~275-277 |
| 9 | ~8 (branch create/read/merge) | ~283-285 |
| 10 | ~5 (CLI) | ~288-290 |

**Current: 283 registered tests passing.** Milestone running totals are approximate and may drift as coverage is added to existing files. Target: ~290 tests at completion.

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
7. **MemWAL — Streaming concurrent writes on a single branch.** Lance MemWAL is an LSM-tree architecture enabling high-throughput streaming writes. Each sub-table dataset gets its own MemWAL with regions (one active writer per region, epoch-fenced). The same `append`/`merge_insert`/`delete` API surface works transparently — MemWAL routes writes through a durable WAL internally. Omnigraph integration requires: (1) enable MemWAL on sub-table datasets, (2) region assignment by `id` column using `bucket(id, N)` transform, (3) manifest commit batching — replace per-mutation commit with periodic checkpoint via `ManifestCoordinator`, (4) writer handle pool with region-aware API and epoch-based fencing. Read path unchanged — `Snapshot::open()` includes flushed MemTable data via Lance's LSM-tree merging read. Graph index builds from merged scan results. See `omnigraph-specs.md` Concurrency Model and Consistency Model sections for full architecture and read consistency spectrum. **Scope note:** this is a later table/write-path optimization after branching, not part of the Step 9 graph-coordination redesign. **Trigger:** per-mutation commit latency >10ms or throughput <100 writes/sec. **Prerequisite:** Step 7 (complete) + Step 9 (branching). The `run_mutation()` API surface does not change — buffering and MemWAL are internal to the write path.
8. **Service layer / HTTP API.** `Omnigraph` struct IS the cache — a service just keeps it alive.
9. **Binary ULIDs.** Switch `id` from Utf8 to `FixedSizeBinary(16)` for performance. See `omnigraph-specs.md` Identity Model section. Trigger: profile TypeIndex build in production-scale graphs.
10. **Row-correlated bindings.** The executor uses flat per-variable RecordBatches. Multi-variable returns across traversal hops break when row counts differ. Needs tuple-based binding model for v0.2.0.
11. **take_rows() hydration.** Use Lance stable row ID addresses for O(1) node hydration instead of `IN (...)` filter. Deferred — scalar indices already make the IN filter fast.
12. **Composite key enforcement at Lance level.** Step 7a adds composite key syntax (`@key(tenant, slug)`) to the schema language, but the runtime still uses a single `id` column. Full composite key support (multi-column unique constraint at the Lance level) is deferred.
13. **Cross-branch cardinality validation.** Edge cardinality is validated per-branch. Cross-branch cardinality semantics after merge are deferred.
14. **Polymorphic interface queries.** Phase 1 (Step 7a) treats interfaces as compile-time property verification. Phase 2 enables `$e: InterfaceName` in queries (multi-dataset scan + union) and interface types as edge targets. Requires executor and graph index changes.
