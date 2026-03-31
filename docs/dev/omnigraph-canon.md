# Omnigraph Canon

## 1. What Omnigraph Is

Omnigraph is a Lance-native typed property graph database.

Today it is split into four layers:

| Layer | Current role |
| --- | --- |
| `omnigraph-compiler` | Parses schema and query source, builds the catalog, typechecks queries, lowers them to IR, normalizes JSON params, and formats result JSON. |
| `omnigraph` | Owns storage, snapshots, loading, execution, traversal, search, branching, merge, runs, blobs, and change detection. |
| `omnigraph-server` | Wraps `Omnigraph` in an Axum HTTP API with config loading and error mapping. |
| `omnigraph-cli` | Local and remote operator surface over the same core APIs. |

The core design choice is that Omnigraph does not store one physical “graph”.
It stores one Lance dataset per node type and one per edge type, then uses a manifest as the consistency boundary.

## 2. Runtime Spine

The live runtime model is:

`Omnigraph -> GraphCoordinator -> ManifestCoordinator -> Snapshot -> per-type Lance datasets`

Supporting pieces:

- `Catalog`: typed graph model compiled from `_schema.pg`
- `TableStore`: all Lance dataset open/scan/append/merge/index operations
- `RuntimeCache`: async interior cache for derived read-side structures
- `GraphIndex`: topology-only traversal index built from edge tables
- `CommitGraph`: graph commit DAG for branch history and merge-base resolution
- `RunRegistry`: hidden transactional run log

This is the main mental model:

- the `Catalog` defines types and semantics
- the manifest defines one pinned version of every sub-table
- a `Snapshot` is the immutable read view created from that manifest state
- traversal uses a derived in-memory graph index, not a persistent adjacency store
- all writes ultimately become new dataset versions plus a new manifest version

## 3. Storage Layout

A repo currently contains:

- `_schema.pg`: source schema
- `_manifest.lance`: one row per node/edge table with `table_path`, `table_version`, `table_branch`, and `row_count`
- `nodes/<hash>`: one Lance dataset per node type
- `edges/<hash>`: one Lance dataset per edge type
- `_graph_commits.lance`: commit DAG used for branch history and merge-base resolution
- `_graph_runs.lance`: transactional run log

Important consequence:
the manifest version is the database version.
A read is consistent because all tables are opened through one pinned manifest snapshot.

## 4. Compiler Layer

`omnigraph-compiler` is shared frontend logic. It does not depend on Lance.

Current responsibilities:

- schema parsing
- query parsing
- catalog construction
- semantic typechecking
- IR lowering for reads and mutations
- JSON param decoding
- JSON result normalization
- embedding client code

The compiler is authoritative for:

- schema validity
- query validity
- edge/type/property name resolution
- parameter type checking
- IR shape used by the runtime executor

The runtime does not re-interpret the query language from scratch. It executes compiler-lowered IR.

## 5. Read Path

Current read execution is:

1. resolve a branch or snapshot target
2. open a `Snapshot` from the manifest state for that target
3. parse and typecheck the named `.gq` query
4. lower the query to IR
5. build or reuse a `GraphIndex` if traversal or anti-join is needed
6. execute IR against snapshot-pinned Lance datasets

Current implementation details:

- read APIs are `&self`, not `&mut self`
- the graph index cache is an async LRU with capacity `8`
- the cache key is snapshot identity plus edge-table state, not just branch name
- `run_query_at(version, ...)` bypasses the cache and builds a one-off historical index

## 6. Write Path

Public writes are transactional by default.

For public `load()` and `mutate()` the runtime now does:

1. capture the target branch head
2. create a hidden run branch
3. stage the write on that run branch
4. verify the target branch head did not advance
5. publish the staged result into the target branch
6. mark the run `published` or `failed`

Important current behavior:

- public writes no longer replay the operation onto the target branch after staging
- the staged run state is what gets published
- if drift or publish failure occurs, the run is marked failed and the target branch stays unchanged

## 7. Branching Model

### What exists

Omnigraph supports git-style branching over graph state.

Current operations:

- create branch from current state
- create branch from another branch
- read from any branch
- write to any public branch
- merge source branch into target branch

### How it works now

Branching is built on Lance branches plus Omnigraph metadata:

- `_manifest.lance` is branched
- sub-table datasets are branched lazily
- `_graph_commits.lance` tracks branch history and merge commits

Lazy branching is important:

- if a branch inherits a table unchanged, it can keep pointing at the parent branch’s table state
- a table is only forked into the child branch when that table actually needs branch-local mutation or index changes

### Merge implementation

Branch merge is not a blunt dataset replace.
It is a three-way merge using:

- source head
- target head
- merge base from `CommitGraph`

The merge logic is row-oriented and tries to preserve row-version metadata for unchanged rows.
It detects divergent updates/inserts and orphan-edge conflicts.

`publish_run()` uses the same internal merge machinery, but only for hidden transactional run branches.

## 8. Transactional Runs

### What runs are

Runs are Omnigraph’s hidden transactional staging mechanism.

Each run has:

- `run_id`
- `target_branch`
- hidden `run_branch`
- `base_snapshot_id`
- `base_manifest_version`
- `status`: `running`, `published`, `failed`, `aborted`

### How they work now

`begin_run()` creates:

- a `RunRecord` in `_graph_runs.lance`
- a hidden internal branch prefixed with `__run__`

Public `load()` and `mutate()` use this automatically.
Manual run operations also exist through the API/CLI:

- begin indirectly through runtime API
- show/list runs
- publish run
- abort run

### Current guarantees

- target branch is unchanged until publish succeeds
- failed public writes become failed runs
- published runs are merged, not replayed
- run metadata is cached in-memory in `RunRegistry` for fast lookup/listing

## 9. Snapshots And Time Travel

### What exists

Omnigraph supports point-in-time reads by manifest version and by graph commit.

Current capabilities:

- `snapshot_of(branch or snapshot-id)`
- `snapshot_at_version(version)`
- `run_query_at(version, ...)`
- `entity_at(table_key, id, version)`
- `diff_commits(from_commit, to_commit, ...)`

### How it works now

A `Snapshot` is just:

- repo root URI
- manifest version
- map of table keys to pinned dataset state

Opening a table from a snapshot means:

- open the dataset
- checkout the correct Lance branch if needed
- checkout the exact pinned version

This makes time-travel reads explicit and stable.

## 10. Change Detection

### What exists

Omnigraph can diff two snapshots or two graph commits and classify changes as:

- insert
- update
- delete

for both nodes and edges.

### How it works now

The diff pipeline is:

1. manifest-level diff to skip unchanged tables
2. lineage check
3. row-level diff

There are two row-level strategies:

- same-lineage diff: use row version columns
- cross-lineage diff: do streaming ID-based comparison

Current change APIs:

- `diff_between(from_target, to_target, filter)`
- `diff_commits(from_commit_id, to_commit_id, filter)`

Important current detail:

- Lance `_row_*` system columns are excluded from cross-lineage row signatures, so metadata-only drift does not show up as graph changes

## 11. Graph Traversal

### What exists

The query engine supports:

- directed traversals
- multi-hop traversals
- bounded and variable-hop traversal
- reverse traversal
- anti-join / negation patterns

### How it works now

Traversal does not scan edges on every hop.
Omnigraph builds a derived `GraphIndex` from edge tables:

- `TypeIndex`: string node ID <-> dense integer ID per node type
- `CsrIndex`: outgoing adjacency per edge type
- `CscIndex`: incoming adjacency per edge type

This index is:

- built lazily
- topology-only
- snapshot-specific
- cached for hot read paths

Important current detail:

- traversal destination sets are deduplicated across cross-type expansion paths

## 12. Search And Ranking

### What exists

Current search capabilities:

- text search
- fuzzy text search
- BM25 ranking
- vector nearest search
- RRF hybrid ranking

### How it works now

Search is implemented by combining compiler support with Lance scanner features:

- text and fuzzy search are pushed down via Lance full-text search
- nearest search is pushed down via Lance vector search
- BM25 ordering is supported in the executor
- RRF fuses two ranked result sets in Omnigraph after sub-search execution

Search requires indices to be created on the relevant properties.
`ensure_indices()` is the entry point for making those Lance indices exist.

### Current limits

- phrase search is constrained by the current Lance Rust API and behaves as documented FTS fallback rather than a dedicated phrase engine
- search quality behavior is real, but still thinner and less battle-tested than the core graph read/write paths

## 13. Schema Capabilities

### What exists

The schema layer currently supports:

- nodes
- edges
- interfaces
- `implements`
- scalar properties
- nullable properties
- list properties
- vector properties
- blob properties
- property descriptions/instructions
- node keys
- uniqueness constraints
- index declarations
- numeric range constraints
- regex check constraints
- edge cardinality
- `@embed` annotations on vector properties

### How it is represented

The compiler builds a `Catalog` containing:

- `NodeType`
- `EdgeType`
- `InterfaceType`
- normalized edge-name index

Each node/edge type carries:

- property map
- Arrow schema
- constraints
- blob property set
- embed-source mapping where present

Important current detail:

- edge-name lookup uses full lowercase normalization, not first-character folding
- exact-name lookup still works
- normalized-name collisions are rejected during catalog build

## 14. Query Language Capabilities

### Read queries

Current read language coverage includes:

- `match`
- typed variable binding
- property predicates
- comparisons
- negation
- traversal
- return projection
- aliasing
- ordering
- limit
- aggregates in the compiler
- search expressions: `search`, `fuzzy`, `bm25`, `nearest`, `rrf`

### Mutation queries

Current mutation coverage includes:

- `insert` node
- `insert` edge
- `update` node
- `delete` node
- `delete` edge

### Current limits

- aggregate support typechecks cleanly, but execution is not as complete as the language surface
- list `contains` is parsed and typechecked, but executor support is still incomplete
- non-scalar filter comparisons are now explicitly rejected by the compiler

## 15. Loading And Validation

### What exists

The active loader ingests JSONL into node and edge datasets.

Supported load modes:

- `overwrite`
- `append`
- `merge`

### How it works now

The live loader is `crates/omnigraph/src/loader/mod.rs`.
It:

- parses JSONL rows
- groups rows by type
- converts typed values to Arrow/Lance columns
- validates references and constraints
- writes per-type datasets
- commits the new table states to the manifest

### Validation currently enforced

- unknown types rejected
- edge endpoints must resolve
- `@range` enforced
- `@check` enforced
- edge cardinality validated after load

### Current note on stale code

There are older loader files under `crates/omnigraph/src/loader/` that are not the live path.
The exported loader is `loader/mod.rs`.

## 16. Mutations

### What exists

The mutation executor supports:

- node insert
- edge insert
- node update
- edge delete
- node delete with edge cascade
- blob writes in mutations

### How it works now

Mutations are compiled to IR, then executed table-by-table against the snapshot-pinned target state.
The executor uses `TableStore` for guarded dataset reopen and write operations.

Important current details:

- public mutations run transactionally through hidden runs
- node delete cascades to connected edges
- blob assignments are handled separately from scalar column updates
- nullable vectors now preserve nulls instead of zero-filling

## 17. Blob Support

### What exists

Blob support is live in the runtime.

Current capabilities:

- blob properties in schema
- load blob values from URI strings or `data:...;base64,...`
- insert/update blob values through mutations
- read blob handles through `read_blob(type, id, property)`

### How it works now

The compiler represents `Blob` as a scalar type.
At runtime, `Omnigraph` rewrites those placeholder fields into Lance blob-v2 fields.

Query behavior is intentionally limited:

- blob columns appear as `null` in normal query projections
- actual blob bytes are accessed through the dedicated blob read API

Reason:

- the current Lance Rust path has projection/filter limitations around blob descriptions, so Omnigraph excludes blob columns from normal filtered scans and rehydrates blobs only when explicitly needed

## 18. Embeddings

### What exists in the compiler/runtime

The codebase has two separate embedding-related pieces:

- live compiler-side embedding client code in `omnigraph-compiler`
- older loader-side embedding materialization utilities in `crates/omnigraph/src/loader/embeddings.rs`

### What is actually live today

Schema support for `@embed(source_prop)` exists.
The compiler records embed-source metadata in the catalog.

The compiler also ships an embedding client with:

- OpenAI embeddings endpoint support
- configurable base URL
- configurable model
- retries and timeout
- deterministic mock transport for tests

Current environment knobs:

- `OPENAI_API_KEY`
- `OPENAI_BASE_URL`
- `NANOGRAPH_EMBED_MODEL`
- `NANOGRAPH_EMBED_TIMEOUT_MS`
- retry settings

Default model:

- `text-embedding-3-small`

### What is not wired into the live load path

Automatic `@embed` materialization is not part of the active compiled loader flow used by `Omnigraph::load()`.
The loader-side embedding utilities exist, but they are not the canonical live ingestion path today.

### Current status summary

- schema can describe embeddings
- vector properties can be queried with `nearest`
- explicit vectors work today
- automatic text-to-vector materialization is not currently part of the main runtime load path
- query-time string embedding is not a finished live feature

## 19. Indexing

### What exists

Omnigraph can ensure storage-level indices on graph tables.

Current uses:

- key-column scalar indices
- full-text search support
- vector search support

### How it works now

`ensure_indices()` opens the latest table heads, not a pinned snapshot, because indices must be created on the current dataset head.
Updated index metadata is then committed back through the manifest.

Important branching behavior:

- on child branches, inherited tables are only forked when needed
- index creation tries to preserve lazy branch ownership instead of eagerly copying everything

## 20. Server

### What exists

`omnigraph-server` exposes:

- `GET /healthz`
- `GET /snapshot`
- `POST /read`
- `POST /change`
- `GET /runs`
- `GET /runs/{id}`
- `POST /runs/{id}/publish`
- `POST /runs/{id}/abort`

### How it works now

The server holds one shared `Omnigraph` in:

- `Arc<RwLock<Omnigraph>>`

Locking model:

- read lock for snapshot/read/run-list/run-show
- write lock for change/publish/abort

Important current details:

- no `block_in_place` or nested sync wrappers remain
- request body limit is `1 MiB`
- manifest errors are typed and mapped to HTTP status by `ManifestErrorKind`

## 21. CLI

### What exists

The CLI is the main operator surface for:

- init
- load
- branch create/list/merge
- snapshot
- read
- change
- run list/show/publish/abort

It supports both:

- direct local repo access
- remote HTTP access via `omnigraph-server`

### How it works now

The CLI resolves:

- repo URI directly
- or target/config from `omnigraph.yaml`

Current output formats for reads:

- table
- kv
- csv
- jsonl
- json

Important current details:

- remote operations reuse one `reqwest::Client` per process
- read output now preserves query projection order end-to-end via `ReadOutput.columns`
- relative query paths resolve against config `base_dir`, not ambient cwd

## 22. Storage Abstraction

### What exists

There is a small async `StorageAdapter` abstraction used for repo metadata.

Current methods:

- `read_text`
- `write_text`
- `exists`

### Current status

Only the local filesystem adapter is implemented in the live path.
The repo shape and some CLI/config language mention broader URI support, but local filesystem is the real supported storage backend today.

## 23. Performance-Critical Caches

Current in-memory caches:

- graph index cache in `RuntimeCache`
- commit cache in `CommitGraph`
- run cache in `RunRegistry`

What each cache does:

- `RuntimeCache`: avoids rebuilding traversal topology for hot snapshots
- `CommitGraph`: avoids rescanning `_graph_commits.lance` for head/commit lookup
- `RunRegistry`: avoids rescanning `_graph_runs.lance` for run lookup/listing

These caches are refreshed or invalidated on branch sync/refresh and write-side state changes.

## 24. Current Capability Summary

### Solid today

- typed schema parsing
- typed query parsing
- semantic typechecking
- Lance-backed snapshots
- per-type node/edge storage
- public transactional load and mutate
- branch create and merge
- graph traversal
- time-travel reads
- diff/change APIs
- text/vector/hybrid search
- blob read/write support
- server and CLI surfaces

### Present but partial

- aggregate execution
- some list-operator runtime support
- CLI maturity as a full operator tool

### Present in code but not canonical/live

- automatic embedding materialization in the main loader path
- non-local storage backends as first-class runtime support

## 25. Best Entry Points

Read these first if you want the live implementation, not historical leftovers:

- `crates/omnigraph-compiler/src/lib.rs`
- `crates/omnigraph-compiler/src/schema/parser.rs`
- `crates/omnigraph-compiler/src/query/parser.rs`
- `crates/omnigraph-compiler/src/query/typecheck.rs`
- `crates/omnigraph-compiler/src/catalog/mod.rs`
- `crates/omnigraph-compiler/src/ir/lower.rs`
- `crates/omnigraph/src/db/omnigraph.rs`
- `crates/omnigraph/src/db/graph_coordinator.rs`
- `crates/omnigraph/src/db/manifest.rs`
- `crates/omnigraph/src/exec/mod.rs`
- `crates/omnigraph/src/loader/mod.rs`
- `crates/omnigraph/src/graph_index/mod.rs`
- `crates/omnigraph/src/changes/mod.rs`
- `crates/omnigraph-server/src/lib.rs`
- `crates/omnigraph-cli/src/main.rs`

Treat these as non-canonical unless you are explicitly reviving them:

- `crates/omnigraph/src/loader/jsonl.rs`
- `crates/omnigraph/src/loader/constraints.rs`
- `crates/omnigraph/src/loader/embeddings.rs`

## 26. Bottom Line

Omnigraph today is a typed graph engine built on:

- compiler-validated graph/query semantics
- manifest-pinned multi-dataset consistency
- Lance-native storage, branching, and indexing
- derived in-memory graph topology for traversal
- transactional public writes via hidden runs

The most important thing to understand is this:

Omnigraph is not “a graph store with one graph file”.
It is a coordinated set of typed Lance datasets, where the manifest is the database version, the compiler is the semantic front door, and everything else is derived from those two facts.
