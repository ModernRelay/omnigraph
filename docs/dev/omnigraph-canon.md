# Omnigraph Canon

## 1. What Omnigraph Is

Omnigraph is a Lance-native typed property graph database.

Today it is split into four layers:

| Layer | Current role |
| --- | --- |
| `omnigraph-compiler` | Parses schema and query source, builds the catalog, typechecks queries, lowers them to IR, decodes JSON params, and formats result JSON. |
| `omnigraph` | Owns storage, snapshots, loading, execution, traversal, search, branching, merge, runs, blobs, embeddings-at-query-time, and change detection. |
| `omnigraph-server` | Wraps `Omnigraph` in an Axum HTTP API with config loading, bearer auth, and error mapping. |
| `omnigraph-cli` | Local and remote operator surface over the same core APIs. |

The central design choice is unchanged:
Omnigraph does not store one physical “graph”.
It stores one Lance dataset per node type and one per edge type, then uses a manifest as the consistency boundary.

## 2. Runtime Spine

The live runtime model is:

`Omnigraph -> GraphCoordinator -> ManifestCoordinator -> Snapshot -> per-type Lance datasets`

Supporting pieces:

- `Catalog`: typed graph model compiled from the accepted persisted schema IR
- `TableStore`: Lance dataset open, scan, append, merge, and index operations
- `RuntimeCache`: async interior cache for derived read-side structures
- `GraphIndex`: topology-only traversal index built from edge tables
- `CommitGraph`: graph commit DAG for branch history and merge-base resolution
- `RunRegistry`: hidden transactional run log

The core mental model is:

- the compiler defines graph semantics
- the manifest defines one pinned version of every sub-table
- a `Snapshot` is the immutable read view created from that manifest state
- traversal uses a derived in-memory graph index, not a persistent adjacency store
- all writes ultimately become new dataset versions plus a new manifest version

## 3. Storage Model And Repo Layout

A repo currently contains:

- `_schema.pg`: source schema
- `_schema.ir.json`: accepted compiled schema contract used by runtime open and future migration work
- `__schema_state.json`: recorded schema IR hash plus schema identity version
- `__manifest`: namespace-style manifest table with one stable `table` row per
  logical graph table and one append-only `table_version` row per published
  table version
- `nodes/<hash>`: one Lance dataset per node type
- `edges/<hash>`: one Lance dataset per edge type
- `_graph_commits.lance`: commit DAG used for branch history and merge-base resolution
- `_graph_runs.lance`: transactional run log

Important consequence:
the `__manifest` dataset version is the database version.
A read is consistent because all tables are opened through one pinned manifest snapshot.

Repo roots are now URI-based, not local-path-only.
Current supported repo root forms are:

- local filesystem path
- `file://...`
- `s3://...`

That support is real in the runtime now, not just aspirational.
Repo metadata like `_schema.pg`, `_schema.ir.json`, `__schema_state.json`, `_graph_commits.lance`, and `_graph_runs.lance` go through the storage adapter.

Schema evolution is now intentionally fail-closed in phase 1:

- `_schema.ir.json` is the accepted machine contract for an existing repo
- `_schema.pg` is still the human-authored source, but editing it does not change the accepted schema
- if `_schema.pg`, `_schema.ir.json`, or `__schema_state.json` drift out of sync, Omnigraph rejects reads and writes with a conflict
- legacy repos can auto-bootstrap schema state only when they are effectively single-branch (`main` only)
- `omnigraph schema plan` is the first migration-facing surface, but it is local-only and plan-only; no schema apply path exists yet

## 4. Compiler Layer

`omnigraph-compiler` is the shared frontend. It does not depend on Lance.

Current responsibilities:

- schema parsing
- query parsing
- catalog construction
- semantic typechecking
- IR lowering for reads and mutations
- JSON param decoding
- JSON result normalization

The compiler is authoritative for:

- schema validity
- query validity
- edge, type, and property name resolution
- parameter type checking
- IR shape used by the runtime executor

The runtime executes compiler-lowered IR. It does not re-interpret the query language from scratch.

## 5. Read Path

Current read execution is:

1. resolve a branch, snapshot, or exact repo target
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

For public `load()` and `mutate()` the runtime does:

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

## 7. Branching And Merge

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

- `__manifest` is branched
- sub-table datasets are branched lazily
- `_graph_commits.lance` is branched eagerly and tracks branch history and merge commits

Lazy branching matters:

- if a branch inherits a table unchanged, it can keep pointing at the parent branch’s table state
- a table is only forked into the child branch when it actually needs branch-local mutation or index changes

### Merge implementation

Branch merge is a three-way merge using:

- source head
- target head
- merge base from `CommitGraph`

The current v1 merge contract is conservative:

- source-only changes are adopted into the target
- target-only changes remain in the target
- doubly changed rows auto-merge only when both branches produce the same final row state
- otherwise the merge stops with typed conflicts instead of guessing

The merge logic is row-oriented and tries to preserve row-version metadata for unchanged rows.
Current typed conflicts include divergent insert, divergent update, delete-vs-update, orphan-edge, unique, cardinality, and value-constraint violations.

Merged candidate graphs are validated before publish, and HTTP callers receive structured conflict entries when a merge is rejected.

Deliberately not in v1:

- property-wise auto-merge for disjoint field edits on the same row
- manual or interactive conflict resolution

`publish_run()` uses the same internal merge machinery, but only for hidden transactional run branches.

## 8. Transactional Runs

Runs are Omnigraph’s hidden transactional staging mechanism.

Each run has:

- `run_id`
- `target_branch`
- hidden `run_branch`
- `base_snapshot_id`
- `base_manifest_version`
- `status`: `running`, `published`, `failed`, `aborted`

Current guarantees:

- target branch is unchanged until publish succeeds
- failed public writes become failed runs
- published runs are merged, not replayed
- run metadata is cached in-memory in `RunRegistry` for fast lookup and listing

## 9. Snapshots And Time Travel

Omnigraph supports point-in-time reads by manifest version and by graph commit.

Current capabilities:

- `snapshot_of(branch or snapshot-id)`
- `snapshot_at_version(version)`
- `run_query_at(version, ...)`
- `entity_at(table_key, id, version)`
- `diff_commits(from_commit, to_commit, ...)`

A `Snapshot` is just:

- repo root URI
- manifest version
- map of table keys to pinned dataset state

Opening a table from a snapshot means:

- resolve the table through the branch-aware namespace layer
- open the dataset at the pinned `table_path`
- checkout the correct Lance branch when `table_branch` is set
- checkout the exact pinned version

## 10. Change Detection

Omnigraph can diff two snapshots or two graph commits and classify changes as:

- insert
- update
- delete

for both nodes and edges.

The diff pipeline is:

1. manifest-level diff to skip unchanged tables
2. lineage check
3. row-level diff

There are two row-level strategies:

- same-lineage diff: use row version columns
- cross-lineage diff: do streaming ID-based comparison

Important current detail:

- Lance `_row_*` system columns are excluded from cross-lineage row signatures, so metadata-only drift does not show up as graph changes

## 11. Graph Traversal

The query engine supports:

- directed traversals
- multi-hop traversals
- bounded and variable-hop traversal
- reverse traversal
- anti-join and negation patterns

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

Current search capabilities:

- text search
- fuzzy text search
- BM25 ranking
- vector nearest search
- RRF hybrid ranking

How it works now:

- text and fuzzy search are pushed down via Lance full-text search
- nearest search is pushed down via Lance vector search
- BM25 ordering is supported in the executor
- RRF fuses two ranked result sets in Omnigraph after sub-search execution

Search requires indices on the relevant properties.
`ensure_indices()` is the runtime entry point for making those Lance indices exist.

## 13. Schema Capabilities

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
- property descriptions and instructions
- node keys
- uniqueness metadata
- index declarations
- numeric range constraints
- regex check constraints
- edge cardinality
- `@embed(source_prop)` annotations on vector properties

The compiler builds a `Catalog` containing:

- `NodeType`
- `EdgeType`
- `InterfaceType`
- normalized edge-name index

Important current details:

- edge-name lookup uses full lowercase normalization, not first-character folding
- exact-name lookup still works
- normalized-name collisions are rejected during catalog build

## 14. Query And Mutation Language

### Read queries

Current read language coverage includes:

- `match`
- typed variable binding
- property predicates
- scalar comparisons
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

### Datatype coverage

Current live datatype coverage includes:

- scalars
- `Date`
- `DateTime`
- lists of supported scalar/date/datetime types
- `Vector(N)`
- `Blob`

Declared query params now support `DateTime` and list types end to end.

### Current limits

- aggregate support typechecks cleanly, but execution is still narrower than the language surface
- list `contains` is parsed and typechecked, but runtime support is still incomplete
- non-scalar filter comparisons are explicitly rejected by the compiler

## 15. Loading And Validation

The active loader ingests JSONL into node and edge datasets.

Supported load modes:

- `overwrite`
- `append`
- `merge`

The live loader is:

- [mod.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/loader/mod.rs)

It:

- parses JSONL rows
- groups rows by type
- converts typed values to Arrow and Lance columns
- validates references and constraints
- writes per-type datasets
- commits the new table states to the manifest

Validation currently enforced:

- unknown types rejected
- edge endpoints must resolve
- `@range` enforced
- `@check` enforced
- edge cardinality validated after load

Important current note:
older loader files under `crates/omnigraph/src/loader/` are not the live path.
The exported loader is `loader/mod.rs`.

## 16. Mutations

The mutation executor supports:

- node insert
- edge insert
- node update
- edge delete
- node delete with edge cascade
- blob writes in mutations

Important current details:

- public mutations run transactionally through hidden runs
- node delete cascades to connected edges
- blob assignments are handled separately from scalar column updates
- nullable vectors preserve nulls instead of zero-filling

## 17. Blob Support

Blob support is live in the runtime.

Current capabilities:

- blob properties in schema
- load blob values from URI strings or `data:...;base64,...`
- insert and update blob values through mutations
- read blob handles through `read_blob(type, id, property)`

Query behavior is intentionally limited:

- blob columns appear as `null` in normal query projections
- actual blob bytes are accessed through the dedicated blob read API

Reason:
the current Lance Rust path has projection and filter limitations around blob descriptions, so Omnigraph excludes blob columns from normal filtered scans and rehydrates blobs only when explicitly needed.

## 18. Embeddings

### What is live now

Query-time string embedding is live in the runtime.

The canonical path is:

- store vectors in `Vector(N)` properties
- query with `nearest($node.embedding, $q)`
- if `$q` is a `String`, embed it at runtime

The runtime embedding client lives in:

- [embedding.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/embedding.rs)

Current provider support in the canonical path:

- Gemini only
- model hardcoded to `gemini-embedding-2-preview`
- mock transport for tests

Current runtime env surface:

- `GEMINI_API_KEY`
- `OMNIGRAPH_GEMINI_BASE_URL`
- `OMNIGRAPH_EMBED_TIMEOUT_MS`
- `OMNIGRAPH_EMBED_RETRY_ATTEMPTS`
- `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS`
- `OMNIGRAPH_EMBEDDINGS_MOCK`

Important behavior:

- explicit vector nearest queries still work without credentials
- string nearest queries fail only when a real embedding call is needed and no Gemini key is available
- query-time embeddings are normalized before search so they behave consistently with the current L2 index path

### What is not canonical yet

- automatic `@embed` materialization during load
- mutation-time re-embedding
- media embedding
- embedding cache in the live runtime path

The old loader-side embedding utilities still exist, but they are not the canonical live ingestion path.

## 19. Indexing

Omnigraph can ensure storage-level indices on graph tables.

Current uses:

- key-column scalar indices
- full-text search support
- vector search support

How it works now:

- `ensure_indices()` opens current table heads, not a pinned snapshot, because indices must be created on the live dataset head
- updated index metadata is then committed back through the manifest

Important branching behavior:

- on child branches, inherited tables are only forked when needed
- index creation tries to preserve lazy branch ownership instead of eagerly copying everything

## 20. Server

`omnigraph-server` exposes:

- `GET /healthz`
- `GET /snapshot`
- `POST /read`
- `POST /change`
- `POST /ingest`
- `GET /runs`
- `GET /runs/{id}`
- `POST /runs/{id}/publish`
- `POST /runs/{id}/abort`

The server holds one shared `Omnigraph` in:

- `Arc<RwLock<Omnigraph>>`

Locking model:

- read lock for snapshot, read, run-list, and run-show
- write lock for change, publish, and abort

Important current details:

- bearer auth is supported via `OMNIGRAPH_SERVER_BEARER_TOKEN`
- `/healthz` stays open even when bearer auth is enabled
- request body limit is `1 MiB`, except `/ingest` which allows `32 MiB`
- manifest errors are typed and mapped to HTTP status by `ManifestErrorKind`

## 21. CLI And Config

The CLI is now a real operator surface for:

- `init`
- `load`
- `ingest`
- `schema plan`
- `branch create/list/delete/merge`
- `snapshot`
- `read`
- `change`
- `run list/show/publish/abort`
- `version`

It supports both:

- direct local repo access
- remote HTTP access via `omnigraph-server`

Current output formats for reads:

- table
- kv
- csv
- jsonl
- json

Important current details:

- remote operations reuse one `reqwest::Client` per process
- `load` is still local-only; branch-first bulk ingest goes through `ingest`
- v1 remote `ingest` sends inline JSONL to the server and inherits the `32 MiB` request cap
- read output preserves query projection order end to end via `ReadOutput.columns`
- relative query paths resolve against config `base_dir`, not ambient cwd
- target-scoped bearer token env names are supported in config
- `auth.env_file` can be loaded from `omnigraph.yaml`
- local commands now autoload the env file into the process environment, which matters for local `s3://` workflows

## 22. Storage Abstraction

There is a small async `StorageAdapter` abstraction used for repo metadata.

Current methods:

- `read_text`
- `write_text`
- `exists`

Current live backends:

- local filesystem
- S3-compatible object storage

The runtime now selects the backend from the repo URI scheme:

- local path or `file://` -> local adapter
- `s3://` -> S3 adapter

The S3 adapter is backend-neutral:

- AWS S3 is the cloud target
- RustFS is the canonical on-prem compatibility target

Current env surface for S3-compatible repos:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- optional `AWS_ENDPOINT_URL_S3`
- optional `AWS_ENDPOINT_URL`
- optional `AWS_S3_FORCE_PATH_STYLE=true`
- optional `AWS_ALLOW_HTTP=true` for local or test endpoints

Important operating rule:

- single writer per repo is still the intended deployment model
- S3 support does not imply multi-writer coordination

## 23. Deployment Profiles

Current supported and target deploy profiles are:

- local dev:
  - local filesystem repo
  - direct CLI or local server process
- on-prem compatible:
  - `s3://` repo against RustFS
  - same runtime code as AWS
- current AWS runtime:
  - EC2 + ALB + CloudFront
  - bearer-protected server
  - packaging moving toward Amazon Linux 2023 compatibility
- target AWS runtime:
  - S3-backed repo first
  - ECS/Fargate later, after S3-backed runtime is proven on AWS

The current deploy story is therefore:

- S3-compatible storage is live in the product
- RustFS validation is live
- real AWS S3 validation is the next step
- Fargate is not the current runtime yet

## 24. Performance-Critical Caches

Current in-memory caches:

- graph index cache in `RuntimeCache`
- commit cache in `CommitGraph`
- run cache in `RunRegistry`

What each cache does:

- `RuntimeCache`: avoids rebuilding traversal topology for hot snapshots
- `CommitGraph`: avoids rescanning `_graph_commits.lance` for head and commit lookup
- `RunRegistry`: avoids rescanning `_graph_runs.lance` for run lookup and listing

These caches are refreshed or invalidated on branch sync, refresh, and write-side state changes.

## 25. Current Capability Summary

### Solid today

- typed schema parsing
- typed query parsing
- semantic typechecking
- Lance-backed snapshots
- per-type node and edge storage
- public transactional load and mutate
- branch create, delete, and merge
- graph traversal
- time-travel reads
- diff and change APIs
- text, vector, and hybrid search
- query-time Gemini string embeddings
- `Date`, `DateTime`, and list param support
- blob read and write support
- server and CLI surfaces
- local and S3-compatible repo roots

### Present but partial

- aggregate execution
- some list-operator runtime support
- CLI maturity as a full operator tool
- production deployment automation for S3-backed AWS runtime

### Present in code but not canonical

- automatic embedding materialization in the main loader path
- multi-writer coordination for one repo
- Fargate runtime as the primary deployed compute shape

## 26. Best Entry Points

Read these first if you want the live implementation, not historical leftovers:

- [lib.rs](/Users/andrew/code/omnigraph/crates/omnigraph-compiler/src/lib.rs)
- [parser.rs](/Users/andrew/code/omnigraph/crates/omnigraph-compiler/src/schema/parser.rs)
- [parser.rs](/Users/andrew/code/omnigraph/crates/omnigraph-compiler/src/query/parser.rs)
- [typecheck.rs](/Users/andrew/code/omnigraph/crates/omnigraph-compiler/src/query/typecheck.rs)
- [mod.rs](/Users/andrew/code/omnigraph/crates/omnigraph-compiler/src/catalog/mod.rs)
- [lower.rs](/Users/andrew/code/omnigraph/crates/omnigraph-compiler/src/ir/lower.rs)
- [omnigraph.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/db/omnigraph.rs)
- [graph_coordinator.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/db/graph_coordinator.rs)
- [manifest.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/db/manifest.rs)
- [mod.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/exec/mod.rs)
- [mod.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/loader/mod.rs)
- [mod.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/graph_index/mod.rs)
- [mod.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/changes/mod.rs)
- [storage.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/storage.rs)
- [embedding.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/embedding.rs)
- [lib.rs](/Users/andrew/code/omnigraph/crates/omnigraph-server/src/lib.rs)
- [config.rs](/Users/andrew/code/omnigraph/crates/omnigraph-server/src/config.rs)
- [main.rs](/Users/andrew/code/omnigraph/crates/omnigraph-cli/src/main.rs)

Treat these as non-canonical unless you are explicitly reviving them:

- [jsonl.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/loader/jsonl.rs)
- [constraints.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/loader/constraints.rs)
- [embeddings.rs](/Users/andrew/code/omnigraph/crates/omnigraph/src/loader/embeddings.rs)

## 27. Bottom Line

Omnigraph today is a typed graph engine built on:

- compiler-validated graph and query semantics
- manifest-pinned multi-dataset consistency
- Lance-native storage, branching, and indexing
- derived in-memory graph topology for traversal
- transactional public writes via hidden runs
- S3-compatible repo storage

The most important thing to understand is still this:

Omnigraph is not “a graph store with one graph file”.
It is a coordinated set of typed Lance datasets, where the manifest is the database version, the compiler is the semantic front door, and everything else is derived from those two facts.
