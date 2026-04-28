# OmniGraph — Agent Guide

> **Maintenance contract for agents:** This file is the canonical, in-repo orientation for any AI coding agent working in this repository. **You MUST keep it up to date.** Whenever you change schema, query language, IR, storage layout, server endpoints, CLI surface, policy actions, defaults, constants, deployment, or release history, update the relevant section of this file in the same change. If a section becomes stale, fix it — do not leave drift. When unsure whether a change is user-visible enough to document here, err on the side of updating.

**Version surveyed:** 0.3.1 (branch `ragnorc/omnigraph-spec`)
**Workspace crates:** `omnigraph-compiler`, `omnigraph` (engine), `omnigraph-cli`, `omnigraph-server`
**Storage substrate:** Lance 4.x (columnar, versioned, branchable)
**License:** MIT
**Toolchain:** Rust stable, edition 2024

> Convention: each feature is split into **L1 — Inherited from Lance** (capabilities OmniGraph gets "for free" by sitting on top of the Lance dataset format) and **L2 — Added by OmniGraph** (typing, graph semantics, coordination across many datasets, server, CLI, policy, etc.). When a section only exists at one layer, only that layer is shown.

---

## 0. Architecture at a Glance

```
┌──────────────────────────────────────────────────────────────────┐
│  CLI (omnigraph)        HTTP Server (omnigraph-server, Axum)     │
│  - 13 cmd families      - REST + OpenAPI                         │
│  - Aliases, configs     - Bearer auth + Cedar policy             │
└──────────────────────────────┬───────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│  omnigraph-compiler                                              │
│  - Pest grammars: schema.pest, query.pest                        │
│  - Catalog (Node/Edge/Interface types)                           │
│  - IR + lowering (NodeScan / Expand / Filter / AntiJoin)         │
│  - Schema migration planner                                      │
│  - Embedding client (OpenAI-style for query-time normalization)  │
└──────────────────────────────┬───────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│  omnigraph (engine)                                              │
│  - GraphCoordinator + ManifestRepo (__manifest)                  │
│  - CommitGraph (_graph_commits.lance)                            │
│  - RunRegistry  (_graph_runs.lance, __run__ branches)            │
│  - GraphIndex (CSR/CSC) + RuntimeCache (LRU 8)                   │
│  - exec::query / mutation / merge                                │
│  - Embedding client (Gemini for runtime ingest)                  │
└──────────────────────────────┬───────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│  Lance 4.x  (per-table dataset)                                  │
│  - Columnar (Arrow) storage, fragments                           │
│  - Manifest versions per dataset                                 │
│  - Per-dataset branches (copy-on-write)                          │
│  - Indexes: BTREE, Inverted (FTS/BM25), IVF/HNSW vector          │
│  - merge_insert (upsert), append, delete                         │
│  - compact_files, cleanup_old_versions                           │
└──────────────────────────────┬───────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│  Object store: local FS, S3, RustFS, MinIO, S3-compatible        │
└──────────────────────────────────────────────────────────────────┘
```

**One-liner:** OmniGraph is a typed property-graph engine built as a coordination layer over many Lance datasets, with Git-style branches and commits across the whole graph, multi-modal querying (vector + FTS + BM25 + RRF + graph traversal) in one runtime, an HTTP server with Cedar policy, and a CLI driven by a single `omnigraph.yaml`.

---

## 1. Storage Architecture

### 1.1 L1 — Lance dataset (per node/edge type)
Every node type and every edge type is its own Lance dataset:
- **Columnar Arrow storage**: each property is a column; nullable per Arrow schema.
- **Fragments**: data is partitioned into fragments; new writes create new fragments.
- **Manifest versioning**: every commit produces a new dataset version; old versions remain readable.
- **Stable row IDs**: enabled by OmniGraph for the commit-graph and run-registry datasets so durable references survive compaction.
- **Append / delete / `merge_insert`**: native Lance write modes.
- **Per-dataset branches** (Lance native): copy-on-write at the dataset level.
- **Object-store agnostic**: file://, s3://, gs://, az://, http (read-only via Lance) — OmniGraph wires file:// and s3:// (`storage.rs`).

### 1.2 L2 — Multi-dataset coordination via `__manifest`
OmniGraph is **not** a single Lance dataset; it is a *graph* of datasets coordinated through one append-only manifest table.

- **Manifest table**: `__manifest/` Lance dataset.
- **Layout** (`db/manifest/layout.rs`, `db/manifest/state.rs`):
  - `nodes/{fnv1a64-hex(type_name)}` — one Lance dataset per node type
  - `edges/{fnv1a64-hex(edge_type_name)}` — one Lance dataset per edge type
  - `__manifest/` — the catalog of all sub-tables and their published versions
  - `_graph_commits.lance` / `_graph_commit_actors.lance` — the commit graph and its actor map
  - `_graph_runs.lance` / `_graph_run_actors.lance` — the run registry and its actor map
- **Manifest row schema** (`object_id, object_type, location, metadata, base_objects, table_key, table_version, table_branch, row_count`):
  - `object_type` ∈ `table | table_version | table_tombstone`
  - `table_key` ∈ `node:<TypeName> | edge:<EdgeName>`
  - `table_branch` is `null` for the main lineage and the branch name otherwise
- **Snapshot reconstruction**: latest visible `table_version` per `(table_key, table_branch)` minus tombstones whose `tombstone_version >= table_version`.
- **Atomic publish**: multi-dataset commits publish via a `ManifestBatchPublisher` so a single write to `__manifest` flips all the new sub-table versions visible at once.

### 1.3 URI scheme support (`storage.rs`)
| Scheme | Backend | Notes |
|---|---|---|
| local path / `file://` | `LocalStorageAdapter` (tokio) | Normalized to absolute paths |
| `s3://bucket/prefix` | `S3StorageAdapter` (object_store) | Honors `AWS_ENDPOINT_URL_S3`, `AWS_ALLOW_HTTP`, `AWS_S3_FORCE_PATH_STYLE` |
| `http(s)://host:port` | HTTP client to `omnigraph-server` | Used by CLI as a target, not a storage backend |

### 1.4 Object-store env vars (S3-compatible)
- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` — for MinIO / RustFS / GCS-via-XML
- `AWS_S3_FORCE_PATH_STYLE=true` — path-style URLs
- `AWS_ALLOW_HTTP=true` — allow plain HTTP (local dev)

---

## 2. Schema Language (`.pg`)

Pest grammar at `crates/omnigraph-compiler/src/schema/schema.pest`. AST at `schema/ast.rs`. Catalog at `catalog/mod.rs`.

### 2.1 Top-level declarations
- `interface <Name> { property* }` — reusable property contracts.
- `node <Name> [implements <Iface>, ...] { property* | constraint* }`
- `edge <Name>: <FromType> -> <ToType> [@card(min..max)] { property* | constraint* }`
- Comments: line `//` and block `/* … */`.

### 2.2 Property declarations
`<ident>: <TypeRef> [annotation*]`

### 2.3 Built-in scalar types
| Scalar | Arrow type |
|---|---|
| `String` | Utf8 |
| `Blob` | LargeBinary |
| `Bool` | Boolean |
| `I32` / `I64` | Int32 / Int64 |
| `U32` / `U64` | UInt32 / UInt64 |
| `F32` / `F64` | Float32 / Float64 |
| `Date` | Date32 |
| `DateTime` | Date64 |
| `Vector(<dim>)` | FixedSizeList(Float32, dim), `1 ≤ dim ≤ i32::MAX` |
| `[<scalar>]` | List(scalar) |
| `enum(v1, v2, …)` | Utf8 with sorted/dedup'd set of allowed string values |
| `<scalar>?` | Same as scalar but `nullable: true` |

### 2.4 Constraints (body level)
| Constraint | On | Effect |
|---|---|---|
| `@key(p, …)` | node | Primary key; implies index on key columns; `key_property()` returns the first key |
| `@unique(p, …)` | node, edge | Uniqueness across listed columns |
| `@index(p, …)` | node, edge | Build a scalar (BTREE) index on the columns |
| `@range(p, min..max)` | node | Numeric range validation (open ranges allowed) |
| `@check(p, "regex")` | node | Regex pattern validation |
| `@card(min..max?)` | edge | Edge multiplicity — default `0..*`; `0..1`, `1..1`, `1..*`, etc. |

Edge bodies only allow `@unique` and `@index`.

### 2.5 Annotations
- `@<ident>` or `@<ident>(<literal>)` on any declaration or property.
- Known annotations:
  - `@embed` on a Vector property — names the *source* property whose text gets embedded into this vector at ingest (`embed_sources` map in NodeType).
  - `@description("…")`, `@instruction("…")` on query declarations (carried through to clients).
- Custom annotations are accepted by the parser and surfaced in catalog metadata; unrecognized annotations don't fail compilation.

### 2.6 Catalog construction
- Pass 0: collect interfaces.
- Pass 1: collect nodes, expand `implements`, build constraint and `@embed` mappings, build the Arrow schema for each node table (`id: Utf8` plus all properties; blob columns get `LargeBinary`).
- Pass 2: collect edges, validate that `from_type` / `to_type` exist, normalize edge names case-insensitively for lookup, validate constraints for edges. Edge Arrow schema: `id: Utf8, src: Utf8, dst: Utf8` plus edge properties.

### 2.7 Schema IR & stable type IDs
- `IR_VERSION = 1` (`catalog/schema_ir.rs`).
- Each interface/node/edge gets a `stable_type_id` (kind+name hashed) so renames can be tracked.
- Serialized as JSON for diff/migration plans.

### 2.8 Schema migration planning
`plan_schema_migration(accepted, desired) -> SchemaMigrationPlan { supported, steps[] }` with step types:
- `AddType { type_kind, name }`
- `RenameType { type_kind, from, to }`
- `AddProperty { type_kind, type_name, property_name, property_type }`
- `RenameProperty { type_kind, type_name, from, to }`
- `AddConstraint { type_kind, type_name, constraint }`
- `UpdateTypeMetadata { … annotations }`
- `UpdatePropertyMetadata { … annotations }`
- `UnsupportedChange { entity, reason }` (forces `supported=false`)

`apply_schema()` returns `SchemaApplyResult { supported, applied, manifest_version, steps }` and is gated by an internal `__schema_apply_lock__` system branch so concurrent schema applies serialize.

---

## 3. Query Language (`.gq`)

Pest grammar at `crates/omnigraph-compiler/src/query/query.pest`. AST in `query/ast.rs`. Type checker in `query/typecheck.rs`. Lowering in `ir/lower.rs`.

### 3.1 Query declarations
```
query <name>($p1: T1, $p2: T2?, …)
  @description("…") @instruction("…") {
  …
}
```
Two body shapes:
- **Read**: `match { … } return { … } [order { … }] [limit N]`
- **Mutation**: one or more of `insert | update | delete` statements

Param types reuse all schema scalars; trailing `?` makes a param optional. The compiler reserves `$__nanograph_now` for `now()`.

### 3.2 MATCH clauses
- **Binding**: `$x: NodeType { prop: <literal | $param | now()>, … }`
- **Traversal**: `$src EDGE_NAME { min, max? } $dst` — variable-length paths via hop bounds; default 1..1 if bounds omitted.
- **Filter**: `<expr> <op> <expr>` with operators `>=`, `<=`, `!=`, `>`, `<`, `=`, and string `contains`.
- **Negation**: `not { clause+ }` — desugars to anti-join over the inner pipeline.

### 3.3 Search clauses (multi-modal)
Used inside MATCH or as expressions inside RETURN/ORDER:
| Function | Purpose | Underlying Lance facility |
|---|---|---|
| `nearest($x.vec, $q)` | k-NN vector search (cosine) | Lance vector index (IVF / HNSW) |
| `search(field, q)` | Generic FTS | Inverted index |
| `fuzzy(field, q [, max_edits])` | Levenshtein-tolerant text search | Inverted index |
| `match_text(field, q)` | Pattern match | Inverted index |
| `bm25(field, q)` | BM25 scoring | Inverted index |
| `rrf(rank_a, rank_b [, k])` | Reciprocal Rank Fusion of two rankings (default k=60) | OmniGraph fuses scored rankings |

`nearest()` requires a `LIMIT`; the compiler resolves the query vector via the param map (or via the runtime embedding client when bound to a text input).

### 3.4 RETURN clause
`return { <expr> [as <alias>], … }` with expressions:
- Variable / property access: `$x`, `$x.prop`
- Literals: string, int, float, bool, list
- `now()`
- Aggregates: `count`, `sum`, `avg`, `min`, `max`
- All search functions above (so you can return a score column)
- `AliasRef` — re-use a previous projection alias

### 3.5 ORDER & LIMIT
- `order { <expr> [asc|desc], … }` — supports plain expressions and `nearest(...)`.
- `limit <integer>` — required when there is a `nearest(...)` ordering.

### 3.6 Mutation statements
- `insert <Type> { prop: <value>, … }`
- `update <Type> set { prop: <value>, … } where <prop> <op> <value>`
- `delete <Type> where <prop> <op> <value>`

`<value>` is a literal, `$param`, or `now()`. Multi-statement mutations execute atomically (added in v0.2.0).

### 3.7 IR (Intermediate Representation)
`QueryIR { name, params, pipeline: Vec<IROp>, return_exprs, order_by, limit }`

Pipeline operations:
- `NodeScan { variable, type_name, filters }`
- `Expand { src_var, dst_var, edge_type, direction (Out|In), dst_type, min_hops, max_hops, dst_filters }` — destination filters are pushed *into* the expand so Lance scalar pushdown can prune.
- `Filter { left, op, right }`
- `AntiJoin { outer_var, inner: Vec<IROp> }` — for `not { … }`

Lowering:
1. Partition MATCH clauses (bindings, traversals, filters, negations).
2. Identify "deferred" bindings (a destination of a traversal that has filters) so the Expand can carry the filter as a pushdown.
3. Emit NodeScan for the first binding, then Expand operations, then remaining Filter operations, then AntiJoins for negations.
4. Translate RETURN / ORDER expressions; preserve LIMIT.

### 3.8 Linting & validation (`query/lint.rs`)
Codes seen so far:
- **Q000** (Error): parse error
- **L201** (Warning): nullable property never set by any UPDATE — "{type}.{prop} exists in schema but no update query sets it"
- (Warning): mutation declares no params — hardcoded mutations are easy to miss
- Plus all type errors from `typecheck_query_decl()` (undefined types, mismatched operators, undefined edges, etc.)

Output:
```
QueryLintOutput { status, schema_source, query_path,
  queries_processed, errors, warnings, infos,
  results: [{ name, kind, status, error?, warnings[] }],
  findings: [{ severity, code, message, type_name?, property?, query_names[] }] }
```
CLI exits non-zero only on `status = Error`.

---

## 4. Indexes (multi-modal lens)

### 4.1 L1 — Lance index types OmniGraph exposes
| Index | Use | Notes |
|---|---|---|
| **BTREE scalar** | range / equality on any scalar | created on `@key`, `@index(...)`, and on key columns by `ensure_indices()` |
| **Inverted (FTS)** | `search`, `fuzzy`, `match_text`, `bm25` | created on text columns referenced by FTS queries |
| **Vector** | `nearest()` k-NN | Lance picks IVF_PQ vs HNSW family by configuration; OmniGraph stores as FixedSizeList(Float32, dim) |

### 4.2 L2 — OmniGraph orchestration
- `ensure_indices()` / `ensure_indices_on(branch)` — idempotent build of BTREE + inverted indexes for the current head; safe to re-run.
- Indexes are built on the *branch head* (not on a snapshot), so reads always see the current index state.
- **Lazy branch forking for indexes**: a branch that hasn't mutated a sub-table doesn't need its own index — the main lineage's index is reused until the first write triggers a copy-on-write fork.
- Vector index parameters (metric, nlist, nprobe, etc.) are not exposed in the schema; they default at the Lance layer and are picked up automatically when an index is asked for on a Vector column.

### 4.3 L2 — Graph topology index (`graph_index/mod.rs`)
This is OmniGraph-specific (not Lance):
- `TypeIndex`: dense `u32 ↔ String id` mapping per node type.
- `CsrIndex`: Compressed Sparse Row representation of edges per edge type — `offsets[i]..offsets[i+1]` slices into `targets`.
- `GraphIndex { type_indices, csr (out), csc (in) }` — built on demand from a snapshot's edge tables.
- Cached in `RuntimeCache::graph_indices` (LRU, max 8 entries, keyed by snapshot id + edge table versions).
- Built only when an `Expand` or `AntiJoin` IR op is present in the lowered query, so pure scans skip it.

---

## 5. Embeddings

OmniGraph has **two** embedding clients with different defaults and purposes:

### 5.1 Compiler-side client (`omnigraph-compiler/src/embedding.rs`) — query-time normalization
- Default model: `text-embedding-3-small` (OpenAI-style schema)
- Env: `NANOGRAPH_EMBED_MODEL`, `OPENAI_API_KEY`, `OPENAI_BASE_URL` (default `https://api.openai.com/v1`), `NANOGRAPH_EMBEDDINGS_MOCK`, `NANOGRAPH_EMBED_TIMEOUT_MS=30000`, `NANOGRAPH_EMBED_RETRY_ATTEMPTS=4`, `NANOGRAPH_EMBED_RETRY_BACKOFF_MS=200`
- Methods: `embed_text(input, expected_dim)`, `embed_texts(inputs, expected_dim)`
- Mock mode: deterministic FNV-1a + xorshift64 → L2-normalized vectors

### 5.2 Engine-side client (`omnigraph/src/embedding.rs`) — runtime ingest
- Model: `gemini-embedding-2-preview`
- Env: `GEMINI_API_KEY`, `OMNIGRAPH_GEMINI_BASE_URL` (default Google generativelanguage v1beta), `OMNIGRAPH_EMBED_TIMEOUT_MS=30000`, `OMNIGRAPH_EMBED_RETRY_ATTEMPTS=4`, `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS=200`, `OMNIGRAPH_EMBEDDINGS_MOCK`
- Two task types: `embed_query_text` (RETRIEVAL_QUERY) and `embed_document_text` (RETRIEVAL_DOCUMENT)
- Exponential backoff with retryable detection (timeouts, 429, 5xx)

### 5.3 Schema integration
Mark a Vector property with `@embed("source_text_property")`. At ingest, the engine pulls the source text and writes the embedding into the vector column. Stored as L2-normalized FixedSizeList(Float32, dim).

### 5.4 CLI `omnigraph embed` (offline file pipeline)
Operates on **JSONL files** (not on a repo). Three modes (mutually exclusive):
- (default) `fill_missing` — only embed rows whose target field is empty
- `--reembed-all` — overwrite all
- `--clean` — strip embeddings

Inputs are either a single seed manifest YAML or `--input/--output/--spec`. Selectors `--type T`, `--select T:field=value` filter rows. Streams JSONL → JSONL.

---

## 6. Branching, Commits, Snapshots

### 6.1 L1 — Lance per-dataset branches
Lance supports branching at the dataset level: a branch is a named lineage of versions, and `fork_branch_from_state(source_branch, target_branch, source_version)` creates a copy-on-write fork.

### 6.2 L2 — Graph-level branches
OmniGraph builds *graph branches* on top by branching every sub-table coherently:
- `branch_create(name)` / `branch_create_from(target, name)` — disallowed name `main`; fails if branch exists; ensures the schema-apply lock is idle.
- `branch_list()` — returns public branches, **filters internal** `__run__…` and `__schema_apply_lock__` prefixes.
- `branch_delete(name)` — refuses if there are descendants or active runs on the branch; cleans up owned per-branch fragments.
- **Lazy forking**: a branch only forks a sub-table when that sub-table is first mutated on it. Pure-read branches share fragments with their source.
- `sync_branch(branch)` — re-binds the in-memory handle to the latest head of the branch.

### 6.3 L2 — Commit graph (`db/commit_graph.rs`)
Stored as a Lance dataset `_graph_commits.lance` (with stable row IDs):
```
GraphCommit {
  graph_commit_id: ULID,
  manifest_branch: Option<String>,
  manifest_version: u64,
  parent_commit_id: Option<String>,
  merged_parent_commit_id: Option<String>,   // populated for merge commits
  actor_id: Option<String>,
  created_at: i64 (microseconds since epoch),
}
```
- Every successful publish (load / change / merge / schema_apply / publish_run) appends one commit.
- Merge commits have two parents; linear commits have one.
- `_graph_commit_actors.lance` — optional separate actor map (created on demand).
- API: `list_commits(branch)`, `get_commit(id)`, `head_commit_id_for_branch(branch)`.

### 6.4 L2 — Snapshots & time travel
- `snapshot()` — current snapshot for the bound branch; cached.
- `snapshot_of(target)` — snapshot at a `ReadTarget` (branch | snapshot id).
- `snapshot_at_version(v: u64)` — historical snapshot from any manifest version.
- `entity_at(table_key, id, version)` — single-entity time travel without building a full snapshot.
- A `Snapshot` is a `(version, HashMap<table_key, SubTableEntry>)` — cheap to build, snapshot-isolated cross-table reads.

### 6.5 L2 — Internal system branches
Filtered from `branch_list()` but visible to internals:
- `__run__<run-id>` — ephemeral isolation branch for a transactional run.
- `__schema_apply_lock__` — serializes schema migrations.

---

## 7. Runs (transactional graph mutations)

`db/run_registry.rs` + run lifecycle in `db/omnigraph.rs`. Stored in `_graph_runs.lance` and `_graph_run_actors.lance`.

### 7.1 RunRecord
```
RunRecord {
  run_id: RunId (ULID),
  target_branch: String,           // where the run will publish
  run_branch: "__run__<id>",       // ephemeral isolation branch
  base_snapshot_id: String,
  base_manifest_version: u64,
  operation_hash: Option<String>,  // idempotency key
  actor_id: Option<String>,
  status: Running | Published | Failed | Aborted,
  published_snapshot_id: Option<String>,
  created_at, updated_at: i64 (microseconds),
}
```

### 7.2 Lifecycle
1. `begin_run(target_branch, op_hash)` / `begin_run_as(target_branch, op_hash, actor_id)` — forks `__run__<id>` from the target's current head, appends a `RunRecord`.
2. Mutations on `run_branch` (via the normal write APIs) — isolated from concurrent activity on the target.
3. `publish_run(id)` / `publish_run_as(id, actor)`:
   - **Fast path**: if the target hasn't moved since `base_snapshot_id`, promote the run snapshot directly.
   - **Merge path**: if it has moved, perform a three-way merge (see §8) into the target.
   - On success: `status = Published`, `published_snapshot_id` set, run branch cleaned up asynchronously.
4. `abort_run(id)` / `fail_run(id)` — terminal; cleans up run branch best-effort.

### 7.3 Idempotency
`operation_hash` is an optional field clients can use to detect a duplicate `begin_run` retry.

### 7.4 Cleanup
`cleanup_terminal_run_branches_for_target(branch)` is called as branches change; failures are swallowed (lazy cleanup on next branch op).

---

## 8. Merging (three-way) and Conflicts

`exec/merge.rs`.

### 8.1 Strategy
Ordered, row-by-row cursor merge:
- `OrderedTableCursor` scans each table sorted by `id` and supports peek/pop matching.
- `StagedTableWriter` buffers `MERGE_STAGE_BATCH_ROWS = 8192` rows into a temp Lance dataset (`OMNIGRAPH_MERGE_STAGING_DIR`).
- The merge runs per sub-table; results are published as one atomic manifest update.

### 8.2 Outcome enum
`MergeOutcome { AlreadyUpToDate | FastForward | Merged }`

### 8.3 Conflict types (`error.rs`)
```
MergeConflictKind:
  DivergentInsert        // same id inserted on both branches
  DivergentUpdate        // updated differently on both branches
  DeleteVsUpdate         // one side deletes, other updates
  OrphanEdge             // edge references a node deleted by the other side
  UniqueViolation
  CardinalityViolation
  ValueConstraintViolation
```
Returned as `OmniError::MergeConflicts(Vec<MergeConflict { table_key, row_id?, kind, message }>)`. The HTTP server surfaces this as a 409 with structured `merge_conflicts[]` (top 3 + "+N more").

---

## 9. Change Detection / Diff

`changes/mod.rs`. Three-level algorithm:
1. **Manifest diff**: skip sub-tables whose `(table_version, table_branch)` is unchanged.
2. **Lineage check**:
   - Same branch lineage → fast path: use the per-row `_row_last_updated_at_version` column to classify Insert/Update/Delete.
   - Different lineages → ID-based streaming comparison.
3. **Row-level diff**: streaming, no full materialization.

Public API:
- `diff_between(from: ReadTarget, to: ReadTarget, filter: Option<ChangeFilter>) -> ChangeSet`
- `diff_commits(from_commit_id, to_commit_id, filter)` — cross-branch safe.

Types:
```
ChangeOp: Insert | Update | Delete
EntityKind: Node | Edge
EntityChange { table_key, kind, type_name, id, op, manifest_version, endpoints?: {src, dst} }
ChangeFilter { kinds?, type_names?, ops? }
ChangeSet { from_version, to_version, branch?, changes[], stats }
```

---

## 10. Query Execution

`exec/query.rs`. Pipeline:
1. Parse + typecheck via `omnigraph-compiler`.
2. Lower to IR.
3. If `Expand` or `AntiJoin` is present, build (or fetch from `RuntimeCache`) a `GraphIndex`.
4. Run `execute_query` against the snapshot.

### 10.1 Multi-modal search modes (`SearchMode`)
The executor recognizes three modes that may be combined in a single query:
- **`nearest`** — vector ANN (uses Lance vector index; `LIMIT` required).
- **`bm25`** — BM25 over an inverted index.
- **`rrf`** — Reciprocal Rank Fusion of two rankings, with k (default 60).

Hybrid example: `order { rrf(nearest($d.embedding, $q), bm25($d.body, $q_text)) desc } limit 20`.

### 10.2 Joins / set operations
- Joins are implicit: MATCH bindings + traversals are implemented as scans + CSR/CSC lookups.
- `not { … }` lowers to an `AntiJoin` over the inner pipeline.

### 10.3 Scoped reads
- `query(target, source, name, params)` — at any branch or snapshot.
- `run_query_at(version, …)` — direct historical query at a manifest version.

### 10.4 Concurrency
- Snapshot isolation per query: all reads inside a query use the same `Snapshot`.
- Readers and writers on different branches don't block each other.

---

## 11. Mutations & Loading

### 11.1 Mutation execution (`exec/mutation.rs`)
Resolves expression values to literals, converts to typed Arrow arrays (`literal_to_typed_array(lit, DataType, num_rows)`), then writes:
- `insert` → Lance `WriteMode::Append`
- `update` → Lance `merge_insert(WhenMatched::Update)`
- `delete` → Lance `merge_insert(WhenMatched::Delete)` (logical) or filtered overwrite.

Multi-statement mutations are atomic at the manifest commit boundary.

### 11.2 Bulk loader (`loader/mod.rs`)
- **JSONL only** in v1, with two record shapes:
  - Node: `{"type":"NodeType", "data":{…}}`
  - Edge: `{"edge":"EdgeType", "from":"src_id", "to":"dst_id", "data":{…}}`
- Lines starting with `//` are treated as comments.
- Schema validation on every row (typecheck, required props, blob base64 decoding).
- Edge endpoint resolution by node `@key`.

### 11.3 Load modes (`LoadMode`)
| Mode | Semantics |
|---|---|
| `Overwrite` | Replace all data in the target tables on the branch |
| `Append` | Strict insert; duplicates error |
| `Merge` | Upsert by id (`merge_insert`) |

### 11.4 `load` vs `ingest`
- `load(branch, data, mode)` — direct load to a branch.
- `ingest(branch, from, data, mode)` — branch-creating, transactional load:
  1. If target advanced since the run started, fork a fresh run branch from `from`.
  2. Load into the run branch (Append).
  3. If target hasn't moved, fast-publish; otherwise abort.
- Returns `IngestResult { branch, base_branch, branch_created, mode, tables[] }`.
- `ingest_as(actor_id)` records the actor on the resulting commit.

### 11.5 Embeddings during load
If a node type has `@embed` properties, the loader calls the engine embedding client (Gemini, RETRIEVAL_DOCUMENT) per row to populate the vector column.

---

## 12. Maintenance: Optimize & Cleanup

`db/omnigraph/optimize.rs`.

### 12.1 `optimize_all_tables(db)` — non-destructive
- Lance `compact_files()` on every node + edge table on `main`.
- Rewrites small fragments into fewer large ones; old fragments remain reachable via older manifests.
- Bounded by `OMNIGRAPH_MAINTENANCE_CONCURRENCY` (default 8).
- Returns `[TableOptimizeStats { table_key, fragments_removed, fragments_added, committed }]`.

### 12.2 `cleanup_all_tables(db, options)` — destructive
- Lance `cleanup_old_versions()` per table.
- Removes manifests (and their unique fragments) older than the retention policy.
- `CleanupPolicyOptions { keep_versions: Option<u32>, older_than: Option<Duration> }` — at least one is required.
- Returns `[TableCleanupStats { table_key, bytes_removed, old_versions_removed }]`.
- CLI guards with `--confirm`; without it, prints a preview line.

### 12.3 Tombstones
Logical sub-table delete markers in `__manifest`; `tombstone_object_id(table_key, version)` excludes a sub-table version from snapshot reconstruction.

---

## 13. Authorization (Cedar policy)

OmniGraph integrates AWS Cedar (`cedar-policy = 4.9`) for ABAC.

### 13.1 Policy actions
1. `read` — query / snapshot / list branches & commits
2. `export` — NDJSON export
3. `change` — mutations
4. `schema_apply` — apply schema migrations
5. `branch_create`
6. `branch_delete`
7. `branch_merge`
8. `run_publish`
9. `run_abort`
10. `admin` — reserved

### 13.2 Scope kinds
- `branch_scope` — applied to source branch (`read`, `export`, `change`)
- `target_branch_scope` — applied to destination (`schema_apply`, branch ops, run ops)
- `protected_branches` — named list with special rules; rule scopes are `any | protected | unprotected`

### 13.3 Configuration
`omnigraph.yaml`:
```yaml
policy:
  file: ./policy.yaml          # Cedar rules + groups
  tests: ./policy.tests.yaml   # declarative test cases
```
Each rule must use exactly one of `branch_scope` or `target_branch_scope`.

### 13.4 CLI
- `omnigraph policy validate` — parse + count actors, exit 1 on parse error.
- `omnigraph policy test` — run cases in `policy.tests.yaml`, exit 1 on any expectation mismatch.
- `omnigraph policy explain --actor … --action … [--branch …] [--target-branch …]` — show decision and matched rule.

### 13.5 Server enforcement
Every mutating endpoint calls `authorize_request()` *before* the handler runs; decisions are logged with actor / action / branch / outcome / matched rule.

---

## 14. HTTP Server (`omnigraph-server`)

Axum 0.8 + tokio + utoipa-generated OpenAPI. Single repo per process; deploy multiple processes for multi-tenant.

### 14.1 Endpoint inventory

| Method | Path | Auth | Action | Handler |
|---|---|---|---|---|
| GET | `/healthz` | none | — | `server_health` |
| GET | `/openapi.json` | none | — | `server_openapi` (strips security if auth disabled) |
| GET | `/snapshot?branch=` | bearer + `read` | snapshot of branch | `server_snapshot` |
| POST | `/read` | bearer + `read` | run named query | `server_read` |
| POST | `/export` | bearer + `export` | NDJSON stream | `server_export` |
| POST | `/change` | bearer + `change` | mutation | `server_change` |
| GET | `/schema` | bearer + `read` | get current `.pg` source | `server_schema_get` |
| POST | `/schema/apply` | bearer + `schema_apply` (target=`main`) | migrate | `server_schema_apply` |
| POST | `/ingest` | bearer + `branch_create` (if new) + `change` | bulk load | `server_ingest` (32 MB body limit) |
| GET | `/branches` | bearer + `read` | list branches | `server_branch_list` |
| POST | `/branches` | bearer + `branch_create` | create | `server_branch_create` |
| DELETE | `/branches/{branch}` | bearer + `branch_delete` | delete | `server_branch_delete` |
| POST | `/branches/merge` | bearer + `branch_merge` | merge `source → target` | `server_branch_merge` |
| GET | `/runs` | bearer + `read` | list | `server_run_list` |
| GET | `/runs/{run_id}` | bearer + `read` | show | `server_run_show` |
| POST | `/runs/{run_id}/publish` | bearer + `run_publish` | publish | `server_run_publish` |
| POST | `/runs/{run_id}/abort` | bearer + `run_abort` | abort | `server_run_abort` |
| GET | `/commits?branch=` | bearer + `read` | list | `server_commit_list` |
| GET | `/commits/{commit_id}` | bearer + `read` | show | `server_commit_show` |

### 14.2 Streaming
Only `/export` streams (`application/x-ndjson`, MPSC channel + `Body::from_stream`). Everything else is buffered JSON.

### 14.3 Error model
Uniform `ErrorOutput { error, code?, merge_conflicts[] }` with `code ∈ unauthorized | forbidden | bad_request | not_found | conflict | internal`. Merge conflicts attach structured `MergeConflictOutput { table_key, row_id?, kind, message }`.

HTTP status codes used: 200, 400, 401, 403, 404, 409, 500.

### 14.4 Body limits
- Default: 1 MB
- `/ingest`: 32 MB

### 14.5 Auth model (`bearer + SHA-256`)
- Tokens are SHA-256 hashed on startup; plaintext is never persisted in memory.
- Constant-time comparison via `subtle::ConstantTimeEq`.
- Three sources, in precedence:
  1. `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` — AWS Secrets Manager (build with `--features aws`)
  2. `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` or `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` — JSON `{actor_id: token, …}`
  3. `OMNIGRAPH_SERVER_BEARER_TOKEN` — single legacy token, actor `default`
- If no tokens configured, server runs unauthenticated (local dev) and `/openapi.json` strips the security scheme.

### 14.6 Tracing & observability
- `tower_http::TraceLayer::new_for_http()`
- Policy decisions logged at INFO level with actor, action, branch, decision, matched rule
- Startup logs: token source name, repo URI, bind address
- Graceful SIGINT shutdown

### 14.7 Not implemented (by design or "TBD")
- CORS — not configured; add `tower_http::cors` if needed.
- Rate limiting — none.
- Pagination — none (commits/branches/runs return everything; export streams).
- Multi-tenant routing — one repo per process.

---

## 15. CLI (`omnigraph`)

13 top-level command families, 40+ subcommands. All commands accept either a positional `URI`, `--uri`, or a `--target <name>` resolved against `omnigraph.yaml`.

### 15.1 Top-level commands
| Command | Purpose |
|---|---|
| `init` | `--schema <pg>` → initialize a repo (also scaffolds `omnigraph.yaml` if missing) |
| `load` | bulk load a branch (`--mode overwrite|append|merge`) |
| `ingest` | branch-creating transactional load (`--from <base>`) |
| `read` | run named query (params via `--params`, `--params-file`, or alias args) |
| `change` | run mutation query |
| `snapshot` | print current snapshot (per-table version + row count) |
| `export` | dump to JSONL on stdout (`--type T`, `--table K` filters) |
| `branch create | list | delete | merge` | branching ops |
| `commit list | show` | inspect commit graph |
| `run list | show | publish | abort` | transactional run ops |
| `schema plan | apply | show (alias: get)` | migrations |
| `query lint | check` | offline / repo-backed validation |
| `optimize` | non-destructive Lance compaction |
| `cleanup --keep N --older-than 7d --confirm` | destructive version GC |
| `embed` | offline JSONL embedding pipeline |
| `policy validate | test | explain` | Cedar tooling |
| `version` / `-v` | print `omnigraph 0.3.x` |

### 15.2 `omnigraph.yaml` schema
```yaml
project: { name }
graphs:
  <name>:
    uri: <local|s3://|http(s)://>
    bearer_token_env: <ENV_NAME>
server:
  graph: <name>
  bind: <ip:port>
cli:
  graph: <name>
  branch: <name>
  output_format: json|jsonl|csv|kv|table
  table_max_column_width: 80
  table_cell_layout: truncate|wrap
query:
  roots: [<dir>, …]   # search path for .gq files
auth:
  env_file: ./.env.omni
aliases:
  <alias>:
    command: read|change
    query: <path-to-.gq>
    name: <query-name>
    args: [<positional-name>, …]
    graph: <name>
    branch: <name>
    format: <output-format>
policy:
  file: ./policy.yaml
```

### 15.3 Output formats (read command)
- `json` — pretty-printed object with metadata + rows
- `jsonl` — one metadata line then one JSON object per row
- `csv` — RFC 4180-ish quoting
- `table` — fitted text table, honors `table_max_column_width` + `table_cell_layout`
- `kv` — grouped per-row key/value blocks

### 15.4 Param resolution
Precedence (high to low): explicit `--params` / `--params-file`, alias positional args, omnigraph.yaml defaults. JS-safe-integer handling is built in (`is_js_safe_integer_i64`, `JS_MAX_SAFE_INTEGER_U64`) so 64-bit ids round-trip safely through JSON clients.

### 15.5 Bearer token resolution (CLI)
1. `graphs.<name>.bearer_token_env`
2. `OMNIGRAPH_BEARER_TOKEN` global env
3. `auth.env_file` referenced `.env`

### 15.6 Duration parsing (cleanup)
`s | m | h | d | w` units, e.g. `--older-than 7d`.

---

## 16. Audit / Actor tracking

- `Omnigraph::audit_actor_id: Option<String>` is the actor in effect.
- `_as` variants of every write API let callers override the actor: `begin_run_as`, `publish_run_as`, `ingest_as`, `mutate_as`, `branch_merge_as`, etc.
- Actor IDs are persisted both on `RunRecord.actor_id` and on `GraphCommit.actor_id`, with optional split storage in `_graph_commit_actors.lance` and `_graph_run_actors.lance`.
- HTTP server uses the bearer-token actor automatically; CLI uses the local user / explicit env (no implicit actor).

---

## 17. Concurrency Model

- **MVCC**: every Lance write bumps a per-dataset version; the OmniGraph manifest version coordinates which sub-table versions are visible together.
- **Snapshot isolation**: a query holds one `Snapshot` for its lifetime; concurrent writes don't leak in.
- **Cross-branch isolation**: copy-on-write means readers and writers on different branches don't block each other.
- **Run isolation**: each transactional run lives on its own `__run__<id>` branch.
- **Schema-apply lock**: `__schema_apply_lock__` system branch serializes schema migrations.
- **Fail-points** (`failpoints` cargo feature): `failpoints::maybe_fail("operation.step")?` in `branch_create`, publish, etc., for deterministic failure injection in tests.

---

## 18. Error Taxonomy

`omnigraph::error::OmniError`:
- `Compiler(...)` — schema/query parse/typecheck errors
- `Lance(String)` — storage layer
- `DataFusion(String)` — execution layer
- `Io(io::Error)`
- `Manifest(ManifestError { kind: BadRequest|NotFound|Conflict|Internal, … })`
- `MergeConflicts(Vec<MergeConflict>)`

Compiler-side `NanoError` covers parse / catalog / type / plan / execution / arrow / lance / IO / manifest / unique-constraint, each with structured spans (`SourceSpan { start, end }`) for ariadne-style diagnostics.

---

## 19. Result Serialization

`omnigraph_compiler::result::QueryResult`:
- `to_arrow_ipc()` — efficient binary
- `to_sdk_json()` — JS-safe JSON (large i64 wrapped in metadata)
- `to_rust_json()` — Rust-friendly JSON
- `batches()` — direct Arrow `RecordBatch` access

Mutation results: `{ affectedNodes: usize, affectedEdges: usize }` (also exposed as a tiny Arrow batch).

---

## 20. Deployment

### 20.1 Binary install
- `curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/install.sh | bash`
  - `RELEASE_CHANNEL=stable|edge`, `VERSION=v0.x.y`, `INSTALL_DIR=~/.local/bin`
  - SHA-256 checksum verification
  - Auto-fallback stable → edge if a tagged release isn't published yet
- Homebrew: `brew tap ModernRelay/tap && brew install ModernRelay/tap/omnigraph`
- Source: `scripts/install-source.sh` (clones + cargo build)
- Manual: `cargo build --release --locked -p omnigraph-cli -p omnigraph-server`

### 20.2 Release artifacts (per release)
- `omnigraph-linux-x86_64.tar.gz`
- `omnigraph-macos-x86_64.tar.gz`
- `omnigraph-macos-arm64.tar.gz`
- Each archive contains both `omnigraph` and `omnigraph-server`, plus a `.sha256` sidecar.

### 20.3 Container
- `docker build -t omnigraph-server:local .`
- Base: `public.ecr.aws/debian/debian:bookworm-slim` (avoids Docker Hub rate limits — switched in v0.3.0)
- Entrypoint env: `OMNIGRAPH_TARGET_URI` or `OMNIGRAPH_CONFIG` (+ optional `OMNIGRAPH_TARGET`), `OMNIGRAPH_BIND` (default `0.0.0.0:8080`)
- Healthcheck: `curl http://localhost:8080/healthz`, 30 s interval / 5 s timeout / 10 s start-period / 3 retries.

### 20.4 Local RustFS one-shot bootstrap
`scripts/local-rustfs-bootstrap.sh` (~422 lines):
- Starts RustFS in Docker on `127.0.0.1:9000`
- Creates a bucket
- Initializes an S3-backed repo (`s3://…`)
- Loads the checked-in fixture
- Launches `omnigraph-server` on `127.0.0.1:8080`
- Tunables: `WORKDIR`, `RUSTFS_CONTAINER_NAME`, `BUCKET`, `PREFIX`, `RESET_REPO`, `BIND`, `FORCE_BUILD`, AWS creds.
- Prefers rolling `edge` binaries; falls back to source build.

### 20.5 Build variants
| Variant | Build | Backend extras |
|---|---|---|
| default | `cargo build --release` | local FS, S3, JSON-file/env tokens |
| aws | `cargo build --release --features aws` | + AWS Secrets Manager bearer-token backend |

The aws variant pulls ~150 transitive dependencies (~30–60 s compile time).

---

## 21. CI Matrix

`.github/workflows/`:
- **ci.yml**: text-only changes skip; otherwise `cargo test --workspace --locked` on ubuntu-latest with protobuf compiler. OpenAPI-drift check that auto-commits the regenerated `openapi.json` for same-repo PRs.
- **AWS feature build job**: `cargo build/test -p omnigraph-server --features aws` on ubuntu-latest.
- **RustFS S3 integration**: spins up RustFS in Docker, runs `s3_storage`, `server_opens_s3_repo_directly_and_serves_snapshot_and_read`, and `local_cli_s3_end_to_end_init_load_read_flow`.
- **release-edge.yml**: on every push to main, retags `edge`, builds Linux/macOS-Intel/macOS-arm64 archives + sha256, publishes a rolling prerelease.
- **release.yml**: on `v*` tags, builds the 3-platform matrix and updates the Homebrew tap (`scripts/update-homebrew-formula.sh`) by force-pushing the regenerated formula to `ModernRelay/homebrew-tap`.
- **package.yml**: manual ECR image build; emits two image tags per commit (`<sha>`, `<sha>-aws`) via CodeBuild.

---

## 22. Release History (highlights)

### v0.2.0 — operability
- Schema planning + apply (CLI + `POST /schema/apply`)
- Concurrent-write blocking on schema apply, index preservation, source-head verification
- Multi-statement atomic mutations
- `/openapi.json` endpoint
- Streamed `/export`, load summaries

### v0.2.1 — query fixes
- `omnigraph query lint` / `query check`
- Aggregate execution
- Nullable-param omission and explicit null
- Traversal planning + join alignment robustness
- Config terminology: `targets:` → `graphs:`

### v0.2.2 — packaging
- Crate rename: `omnigraph` → `omnigraph-engine` on crates.io (CLI surface unchanged)

### v0.3.0 — AWS + security hardening
- `--features aws` build + AWS Secrets Manager bearer-token backend
- SHA-256 token hashing, constant-time comparison, server-authoritative actor IDs
- `GET /schema` + `omnigraph schema get`
- Static OpenAPI spec auto-synced by CI
- Stricter run-branch hygiene (`__run__…` filtered + auto-deleted)
- Dockerfile base → ECR Public (`public.ecr.aws/debian/debian`)
- LANCE memory pool default raised to 1 GB
- Homebrew tap update automation
- **Breaking**: pre-0.3 repos must be reinitialized (schema state changes)

### v0.3.1 — current
- Parallel per-type load writes
- `omnigraph optimize` / `cleanup` CLI commands and runtime APIs
- Dst-id deduplication during edge expand hydration

---

## 23. Capability Matrix — "Lens by default vs. added by OmniGraph"

| Capability | L1 (Lance default) | L2 (OmniGraph adds) |
|---|---|---|
| Columnar storage on object store | ✅ Arrow/Lance | URI normalization, S3 env-var plumbing |
| Per-dataset versioning + time travel | ✅ | `snapshot_at_version`, `entity_at`, snapshot-pinned reads across many tables |
| Per-dataset branches | ✅ | **Graph-level** branches (atomic across all sub-tables), lazy fork, system branch filtering |
| Atomic single-dataset commits | ✅ | **Atomic multi-dataset publish** via `__manifest` + `ManifestBatchPublisher` |
| Compaction (`compact_files`) | ✅ | `omnigraph optimize` orchestrates over all node/edge tables, bounded concurrency |
| Cleanup (`cleanup_old_versions`) | ✅ | `omnigraph cleanup` with `--keep` / `--older-than` policy |
| BTREE / inverted (FTS) / vector indexes | ✅ | `ensure_indices` builds them on every relevant column; idempotent; lazy across branches |
| `merge_insert` upsert | ✅ | `LoadMode::Merge`, mutation `update`/`insert`/`delete` lowering |
| Vector search | ✅ | `nearest()` query op; embedding pipeline (Gemini / OpenAI clients); `@embed` in schema |
| Full-text search | ✅ | `search/fuzzy/match_text/bm25` query ops |
| Hybrid ranking | — | `rrf(...)` Reciprocal Rank Fusion in one runtime |
| Graph traversal | — | CSR/CSC topology index, `Expand` IR op, variable-length hops, `not { }` anti-join |
| Schema language | — | `.pg` + Pest grammar + catalog + interfaces + constraints + annotations |
| Query language | — | `.gq` + Pest grammar + IR + lowering + linter |
| Schema migration planning | — | `plan_schema_migration` + `apply_schema` step types + `__schema_apply_lock__` |
| Commit graph (DAG) across whole repo | — | `_graph_commits.lance` with linear + merge parents, ULID ids, actor map |
| Transactional runs | — | `_graph_runs.lance`, `__run__<id>` ephemeral branches, fast-path & merge-path publish |
| Three-way row-level merge | — | `OrderedTableCursor` + `StagedTableWriter`, structured `MergeConflictKind` |
| Change feeds | — | `diff_between` / `diff_commits` with manifest fast path + ID streaming |
| Cedar policy | — | 10 actions, branch / target_branch / protected scopes, validate/test/explain CLI |
| HTTP server | — | Axum, OpenAPI via utoipa, bearer auth (SHA-256, AWS Secrets Manager option), policy gating, NDJSON streaming export |
| CLI with config | — | `omnigraph.yaml`, aliases, multi-format output (json/jsonl/csv/kv/table) |
| Audit / actor tracking | — | `_as` write APIs + actor maps in commit & run datasets |
| Local RustFS bootstrap | — | `scripts/local-rustfs-bootstrap.sh` one-shot S3-backed dev environment |

---

## 24. Constants & Tunables (cheat sheet)

| Name | Value | Where |
|---|---|---|
| `MANIFEST_DIR` | `__manifest` | `db/manifest/layout.rs` |
| Commit graph dir | `_graph_commits.lance` | `db/commit_graph.rs` |
| Run registry dir | `_graph_runs.lance` | `db/run_registry.rs` |
| Run branch prefix | `__run__` | `db/run_registry.rs` |
| Schema apply lock | `__schema_apply_lock__` | `db/mod.rs` |
| Merge stage batch | `MERGE_STAGE_BATCH_ROWS = 8192` | `exec/merge.rs` |
| Maintenance concurrency | `OMNIGRAPH_MAINTENANCE_CONCURRENCY=8` | `db/omnigraph/optimize.rs` |
| Graph index cache size | `8` (LRU) | `runtime_cache.rs` |
| Default body limit | `1 MB` | `omnigraph-server/lib.rs` |
| Ingest body limit | `32 MB` | `omnigraph-server/lib.rs` |
| Engine embed model | `gemini-embedding-2-preview` | `omnigraph/embedding.rs` |
| Compiler embed model | `text-embedding-3-small` | `omnigraph-compiler/embedding.rs` |
| Embed timeout | `30 000 ms` | both clients |
| Embed retries | `4` | both clients |
| Embed retry backoff | `200 ms` | both clients |
| LANCE memory pool default | `1 GB` (raised in v0.3.0) | runtime |

---

## 25. Quick-reference: typical flows

```bash
# Initialize an S3-backed repo
omnigraph init --schema ./schema.pg s3://my-bucket/repo.omni

# Bulk load
omnigraph load --data ./seed.jsonl --mode overwrite s3://my-bucket/repo.omni

# Branch + ingest a review batch
omnigraph branch create --from main review/2026-04-25 s3://my-bucket/repo.omni
omnigraph ingest --branch review/2026-04-25 --data ./batch.jsonl s3://my-bucket/repo.omni

# Run a hybrid (vector + BM25) query
omnigraph read --query ./queries.gq --name find_similar \
  --params '{"q":"trends in AI safety"}' --format table s3://my-bucket/repo.omni

# Plan + apply schema migration
omnigraph schema plan  --schema ./next.pg s3://my-bucket/repo.omni
omnigraph schema apply --schema ./next.pg s3://my-bucket/repo.omni --json

# Merge review branch back
omnigraph branch merge review/2026-04-25 --into main s3://my-bucket/repo.omni

# Compact + GC (preview, then confirm)
omnigraph optimize s3://my-bucket/repo.omni
omnigraph cleanup  --keep 10 --older-than 7d s3://my-bucket/repo.omni
omnigraph cleanup  --keep 10 --older-than 7d --confirm s3://my-bucket/repo.omni

# Stand up the HTTP server (token from env)
OMNIGRAPH_SERVER_BEARER_TOKEN=xxxx \
  omnigraph-server s3://my-bucket/repo.omni --bind 0.0.0.0:8080

# Cedar policy explain
omnigraph policy explain --actor act-alice --action change --branch main
```

---

## 26. Keeping this file current (agent responsibilities)

When you make a change in this repo, also update AGENTS.md if the change touches any of:

- **Schema language** (`.pg` grammar, scalar types, constraints, annotations) → §2
- **Query language** (`.gq` grammar, MATCH/RETURN/ORDER, search funcs, mutations, IR ops, lint codes) → §3
- **Indexes** (BTREE / inverted / vector / graph topology) → §4
- **Embeddings** (clients, models, env vars, `@embed`) → §5
- **Branches / commits / snapshots / runs / merge / diff** → §6–§9
- **Storage layout, manifest schema, URI schemes, env vars** → §1
- **Mutations / loader / load modes / ingest** → §11
- **Optimize / cleanup / tombstones** → §12
- **Cedar policy actions, scopes, CLI** → §13
- **HTTP server** (endpoint inventory, body limits, auth, error model) → §14
- **CLI** (commands, `omnigraph.yaml`, output formats, param/token resolution) → §15
- **Audit / actor tracking** → §16
- **Concurrency, error taxonomy, result serialization** → §17–§19
- **Deployment / CI / release** → §20–§22
- **Constants and tunables** → §24
- **Capability matrix and example flows** → §23, §25

Rules of engagement:

1. **Update in the same change.** If a PR adds a new endpoint, query function, CLI flag, env var, constant, or schema construct, the same PR must update the corresponding section here. Never split documentation drift into a follow-up.
2. **Bump the version surveyed line** at the top when a change crosses a release boundary (e.g. v0.3.1 → v0.3.2). Add a new bullet under §22 Release History describing the user-visible delta.
3. **Don't lie.** If a section is now wrong but you don't have time to rewrite it fully, replace the wrong line with `*(stale — needs update after <change>)*` rather than leaving silently incorrect text. Then fix it ASAP.
4. **Match section numbering.** Don't renumber sections; new content goes into the closest existing section, or — if it doesn't fit — as a new sub-heading inside one. Renumbering breaks every cross-reference in this file and in linked CLAUDE.md.
5. **Tables before prose.** Endpoint lists, env vars, constants, and step types are easier to keep current as tables. Prefer tables when adding new entries.
6. **Cross-check on schema/query/IR changes.** Any edit to `schema.pest`, `query.pest`, `ir/lower.rs`, `query/typecheck.rs`, or `query/lint.rs` should trigger a re-read of §2, §3, and §10 to confirm they still describe reality.
7. **Re-verify before recommending.** If you cite a flag, env var, or endpoint to the user, grep for it in the source first. This file is a living guide, not a frozen contract.

> The CLAUDE.md file in this repo is a symlink to AGENTS.md — there is exactly one source of truth. Do not edit CLAUDE.md directly; edit AGENTS.md and the link will reflect the change.

---

*End of guide — sections 0–25 reflect code at branch `ragnorc/omnigraph-spec`, tag-equivalent v0.3.1. Section 26 is the maintenance contract for future agents.*
