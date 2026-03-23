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

**184 tests passing.** Steps 0–6 complete including Lance-native optimizations.

```
Step 0  ✅  Crate restructuring
Step 1  ✅  Foundation types (String IDs)
Step 2  ✅  ManifestCoordinator
Step 3  ✅  Omnigraph init/open
Step 4  ✅  Data loading
Step 5  ✅  Query execution (tabular)
Step 5a ✅  Snapshot refactor (read consistency)
Step 6  ✅  Graph index + traversal + Lance-native optimizations
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

### Step 7: Mutations

Insert, update, delete nodes and edges with schema-driven edge cascade.

#### Compiler Types Already Available

The full mutation pipeline exists in the compiler — parsing, typechecking, and IR lowering are done. Only the runtime execution is missing.

**Query grammar** (`crates/omnigraph-compiler/src/query/query.pest:27-32`):
```
insert Person { name: $name, age: $age }
update Person set { age: $age } where name = $name
delete Person where name = $name
```

**AST** (`crates/omnigraph-compiler/src/query/ast.rs:184-221`):
- `Mutation::Insert(InsertMutation)` — type_name + assignments
- `Mutation::Update(UpdateMutation)` — type_name + assignments + predicate
- `Mutation::Delete(DeleteMutation)` — type_name + predicate
- `MutationAssignment { property, value: MatchValue }`
- `MutationPredicate { property, op: CompOp, value: MatchValue }`

**Typechecker** (`crates/omnigraph-compiler/src/query/typecheck.rs:180-393`):
- `typecheck_query_decl()` → `CheckedQuery::Mutation(MutationTypeContext { target_type })`
- Insert: validates all non-nullable properties provided, validates edge `from`/`to` endpoints
- Update: validates property existence and types, validates predicate, **rejects edge updates** (T16)
- Delete: validates predicate for both nodes and edges

**IR** (`crates/omnigraph-compiler/src/ir/mod.rs:18-53`):
- `MutationIR { name, params, op: MutationOpIR }`
- `MutationOpIR::Insert { type_name, assignments: Vec<IRAssignment> }`
- `MutationOpIR::Update { type_name, assignments, predicate: IRMutationPredicate }`
- `MutationOpIR::Delete { type_name, predicate: IRMutationPredicate }`
- `IRAssignment { property: String, value: IRExpr }`
- `IRMutationPredicate { property: String, op: CompOp, value: IRExpr }`

**Lowering** (`crates/omnigraph-compiler/src/ir/lower.rs:63-112`):
- `lower_mutation_query(query: &QueryDecl) -> Result<MutationIR>`
- Already handles params → `IRExpr::Param`, literals → `IRExpr::Literal`, `now()` → reserved param

**Result types** (`crates/omnigraph-compiler/src/result.rs`):
- `MutationResult { affected_nodes: usize, affected_edges: usize }`
- `RunResult::Mutation(MutationResult)` — the return type for mutation queries

**Public re-exports** (`crates/omnigraph-compiler/src/lib.rs`):
- `omnigraph_compiler::lower_mutation_query`
- `omnigraph_compiler::find_named_query`
- `omnigraph_compiler::MutationResult`
- `omnigraph_compiler::RunResult`
- `omnigraph_compiler::query::typecheck::{typecheck_query_decl, CheckedQuery}`

#### Runtime to Build

**File: `crates/omnigraph/src/exec/mod.rs`** (~150 lines)

Add `Omnigraph::run_mutation()`:

```rust
impl Omnigraph {
    pub async fn run_mutation(
        &mut self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let query_decl = find_named_query(query_source, query_name)...;
        let checked = typecheck_query_decl(self.catalog(), &query_decl)?;
        // Must be CheckedQuery::Mutation
        let ir = lower_mutation_query(&query_decl)?;
        execute_mutation(&ir, params, &mut self).await
    }
}
```

Add `execute_mutation()` pure function:

```rust
async fn execute_mutation(
    ir: &MutationIR,
    params: &ParamMap,
    db: &mut Omnigraph,
) -> Result<MutationResult> {
    match &ir.op {
        MutationOpIR::Insert { type_name, assignments } => { ... }
        MutationOpIR::Update { type_name, assignments, predicate } => { ... }
        MutationOpIR::Delete { type_name, predicate } => { ... }
    }
}
```

Note: `execute_mutation` takes `&mut Omnigraph` (not pure) because it writes to Lance and commits the manifest. This is intentional — mutations are side-effectful.

#### Insert Implementation

For **node insert**:
1. Resolve param values from `IRExpr::Param` / `IRExpr::Literal` using the existing `literal_to_sql` pattern
2. Build a single-row `RecordBatch` with the node schema:
   - `id` = `@key` property value (from assignments) or `ulid::Ulid::new().to_string()`
   - Property columns from assignments, nulls for unassigned nullable props
   - Follow pattern: `loader/mod.rs:build_node_batch()` but for a single row from IR assignments
3. Open the sub-table: `snapshot.entry("node:{type_name}").table_path` → `Dataset::open(full_path)`
4. Append: `ds.append(reader, None).await` (follow `loader/mod.rs:375-387` pattern)
5. Commit manifest with new version: `db.manifest_mut().commit(&[SubTableUpdate { ... }])`
6. Return `MutationResult { affected_nodes: 1, affected_edges: 0 }`

For **edge insert**:
1. `from` and `to` assignments map to `src` and `dst` columns
2. `id` = `ulid::Ulid::new().to_string()` (edges never have `@key`)
3. Same append + commit pattern
4. **Invalidate graph index**: `db.cached_graph_index = None`
5. Return `MutationResult { affected_nodes: 0, affected_edges: 1 }`

#### Update Implementation

1. Resolve predicate: `IRMutationPredicate { property, op, value }` → Lance SQL filter string (reuse `ir_filter_to_sql` / `literal_to_sql` from exec)
2. Build a RecordBatch of the rows to update:
   - Scan with predicate filter to get existing rows
   - For each matched row, apply assignments (overwrite column values)
3. `merge_insert` keyed by `id` (follow `loader/mod.rs:389-411` pattern):
   ```rust
   MergeInsertBuilder::try_new(ds, vec!["id".to_string()])
       .when_matched(WhenMatched::UpdateAll)
       .when_not_matched(WhenNotMatched::DoNothing)
   ```
4. Commit manifest
5. Return `MutationResult { affected_nodes: count, affected_edges: 0 }`

Note: typechecker rejects edge updates (T16), so we only handle node updates.

#### Delete Implementation

1. Resolve predicate → Lance SQL filter string
2. Scan to find matching IDs: `ds.scan().project(&["id"]).filter(sql).try_into_stream()`
3. Delete: `ds.delete(&predicate_sql).await` — Lance uses deletion vectors (soft delete, no rewrite)
4. **Edge cascade** (node deletes only):
   - For each edge type where `from_type == deleted_type` or `to_type == deleted_type`:
     - Build cascade filter: `src IN ({deleted_ids})` or `dst IN ({deleted_ids})`
     - `edge_ds.delete(&cascade_filter).await`
     - BTree indices on `src`/`dst` (from Step 6 `ensure_indices`) make this efficient
5. Commit manifest with updated versions for all changed sub-tables
6. **Invalidate graph index**: `db.cached_graph_index = None` (if any edges deleted)
7. Return `MutationResult { affected_nodes: N, affected_edges: M }`

Edge deletes: same as step 3 but no cascade. Set `affected_edges` only.

#### Lance APIs Used

- `Dataset::open(uri)` — open sub-table for writing
- `ds.append(reader, None)` — insert rows (append mode)
- `MergeInsertBuilder::try_new(ds, keys).when_matched(UpdateAll)` — upsert for updates
- `ds.delete(predicate)` — soft delete with deletion vectors
- `ds.count_rows(Some(filter))` — count affected rows before delete
- All existing in the codebase from loader/manifest code

#### Graph Index Invalidation

After any mutation that changes edges (insert edge, delete node with cascade, delete edge):
```rust
self.cached_graph_index = None;
```
Next traversal query will rebuild. This is the single-slot cache pattern from Step 5a.

#### Test Cases (`crates/omnigraph/tests/end_to_end.rs`)

Use test schema from `test.pg` (Person @key name, Company @key name, Knows, WorksAt).

```rust
// 1. Insert node, query it back
query insert_person($name: String, $age: I32) {
    insert Person { name: $name, age: $age }
}
// Run with params name="Eve", age=22
// Then run get_person("Eve") → should return 1 row

// 2. Insert edge, verify traversal
query add_friend($from: String, $to: String) {
    insert Knows { from: $from, to: $to }
}
// Run add_friend("Eve", "Alice")
// Then run friends_of("Eve") → should return Alice

// 3. Update node property
query set_age($name: String, $age: I32) {
    update Person set { age: $age } where name = $name
}
// Run set_age("Alice", 31)
// Then run get_person("Alice") → age should be 31

// 4. Delete node with edge cascade
query remove_person($name: String) {
    delete Person where name = $name
}
// Run remove_person("Alice")
// get_person("Alice") → 0 rows
// Knows edges from/to Alice should be gone
// WorksAt edges from Alice should be gone

// 5. Delete edge
query remove_friendship($from: String) {
    delete Knows where src = $from
}
```

**Expected: ~5 new tests, ~150 lines in exec/mod.rs, ~80 lines in end_to_end.rs. Total: ~190 tests.**

---

### Step 8: Search Predicates

Replace brute-force search with Lance FTS and vector indexes.

#### Compiler Types Already Available

The query AST and IR already support all search expressions. Typechecking validates field types and parameter types. Only the runtime execution is missing.

**IR expressions** (`crates/omnigraph-compiler/src/ir/mod.rs:91-131`):
- `IRExpr::Nearest { variable, property, query }` — vector similarity
- `IRExpr::Search { field, query }` — full-text search
- `IRExpr::Fuzzy { field, query, max_edits }` — fuzzy text match
- `IRExpr::MatchText { field, query }` — exact phrase match
- `IRExpr::Bm25 { field, query }` — BM25 scoring
- `IRExpr::Rrf { primary, secondary, k }` — reciprocal rank fusion

**Query grammar examples** (`crates/omnigraph-compiler/src/query/query.pest`):
```
search($p.title, "machine learning")
fuzzy($p.name, "alice", 2)
match_text($p.bio, "data scientist")
nearest($p.embedding, $query_vector)
bm25($p.title, "AI models")
rrf(nearest($p.embedding, $vec), bm25($p.title, $text))
```

#### Lance FTS APIs to Use

Lance 3.0 provides FTS through scalar indices:

**Index creation** (extend `ensure_indices` in `db/omnigraph.rs`):
```rust
// For @index String properties — create FTS inverted index
ds.create_index_builder(&["title"], IndexType::Inverted, &params).await;

// For Vector properties — create IVF-PQ or IVF-HNSW index
// (requires lance::index::vector::VectorIndexParams)
ds.create_index_builder(&["embedding"], IndexType::IvfHnswPq, &vector_params).await;
```

**Query execution** (Lance scanner filter syntax):
- `search()` → `scanner.filter("match_tokens(title, 'machine learning')")`
- `fuzzy()` → `scanner.filter("match_tokens(name, 'alice', fuzzy_max_edits=2)")`
- `match_text()` → `scanner.filter("match_phrase(bio, 'data scientist')")`
- `bm25()` → `scanner.full_text_search(FullTextSearchQuery { query: "AI models", columns: &["title"] })`
- `nearest()` → `scanner.nearest("embedding", &query_vector, k)` — Lance native ANN

**Note:** The exact Lance FTS filter syntax and `FullTextSearchQuery` API should be verified against Lance 3.0 docs at implementation time. The scanner `.nearest()` method is well-established.

#### Runtime to Build

**File: `crates/omnigraph/src/exec/mod.rs`** (~150 lines)

1. In `execute_node_scan`: extend filter pushdown to handle search expressions in IRFilter
2. Add `build_search_filter()` that converts `IRExpr::Search/Fuzzy/MatchText` to Lance filter strings
3. For `nearest()` ordering: use `scanner.nearest(column, vector, k)` instead of post-scan sort
4. For `bm25()`: use Lance full-text search API with BM25 scoring
5. For `rrf()`: execute two sub-queries (nearest + bm25), fuse scores at application level

**File: `crates/omnigraph/src/db/omnigraph.rs`** (~30 lines)

Extend `ensure_indices()`:
- For node properties with `@index` annotation and `ScalarType::String` → create `IndexType::Inverted`
- For node properties with `ScalarType::Vector(dim)` → create `IndexType::IvfHnswPq`
- Access `catalog.node_types[name].indexed_properties` and `catalog.node_types[name].properties[prop].scalar`

#### Index Creation (Extend `ensure_indices`)

The catalog already tracks which properties need indices:
- `NodeType::indexed_properties: HashSet<String>` — properties with `@key` or `@index`
- `PropType::scalar` — check if `ScalarType::String` (FTS) or `ScalarType::Vector(_)` (ANN)

#### Test Cases

Add to signals fixture (`test/fixtures/signals.pg` already has Signal with title/source/strength) or create a search-specific fixture:

```rust
// 1. Text search
query search_signals($query: String) {
    match { $s: Signal }
    return { $s.title }
    order { search($s.title, $query) }
    limit 5
}

// 2. Nearest vector (needs a schema with Vector type)
query similar($vec: Vector(3)) {
    match { $d: Doc }
    return { $d.title }
    order { nearest($d.embedding, $vec) }
    limit 5
}
```

**Expected: ~150 lines in exec/mod.rs, ~30 lines in omnigraph.rs, ~50 lines in tests. Total: ~195 tests.**

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
3. Commit target manifest with new sub-table versions

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

**Expected: ~210 lines across manifest/omnigraph/loader, ~80 lines in tests. Total: ~201 tests.**

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
| `omnigraph describe <uri>` | Print schema + catalog + manifest summary |
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

**Expected: ~230 lines in CLI, ~30 lines in omnigraph.rs. Total: ~205 tests.**

---

## Dependency Graph

```
Steps 0–6 ✅
     ├→ Step 7 (mutations — writes to Lance, edge cascade, index invalidation)
     ├→ Step 8 (search — Lance FTS/ANN, index creation)
     └→ Step 9 (branching — Lance branches on manifest + sub-tables)
          └→ Step 10 (change tracking + CLI)
```

Steps 7, 8, 9 can proceed in parallel. Step 10 depends on 9 (branching CLI) but the core CLI wiring can start anytime.

**Critical path:** Step 7 (mutations make the database writable beyond initial load).

---

## Test Summary

| Step | Tests | Running Total |
|---|---|---|
| 0 | 147 carried forward | 147 |
| 1 | +2 (Utf8 schemas, @key tracking) | 149 |
| 2 | +5 (manifest CRUD) | 154 |
| 3 | +3 (init/open) | 157 |
| 4 | +6 unit, +11 integration | 178 |
| 5 | +4 query tests (in end_to_end.rs) | 178 |
| 5a | +1 (snapshot pinning) | 179 |
| 6 | +5 (traversal + anti-join + optimizations) | 184 |
| 7 | ~5 new (insert/update/delete/cascade) | ~189 |
| 8 | ~4 new (search/fuzzy/nearest) | ~193 |
| 9 | ~6 new (branch create/read/merge) | ~199 |
| 10 | ~3 new (CLI) | ~202 |

**Current: 184 tests passing.** Target: ~202 at completion.

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
9. **Binary ULIDs.** Switch `id` from Utf8 to `FixedSizeBinary(16)` for performance. See `omnigraph-specs.md` Identity Model section. Trigger: profile TypeIndex build in production-scale graphs.
10. **Row-correlated bindings.** The executor uses flat per-variable RecordBatches. Multi-variable returns across traversal hops break when row counts differ. Needs tuple-based binding model for v0.2.0.
11. **take_rows() hydration.** Use Lance stable row ID addresses for O(1) node hydration instead of `IN (...)` filter. Deferred — scalar indices already make the IN filter fast.
