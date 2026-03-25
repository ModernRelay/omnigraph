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
- Arrow type system (including Lance blob v2)

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
4. **Typed columnar execution**: Lance Scanner with SQL filter pushdown over per-type Lance tables. BTree, Inverted, and schema-driven Vector ANN indices are active today.
5. **Indexed search**: Lance FTS, n-gram, and vector indexes replace brute-force in-memory search
6. **Schema-as-code**: `.pg` remains readable and versioned with data
7. **Two products, shared compiler**: Nanograph and Omnigraph share the `omnigraph-compiler` crate but own different storage/execution layers

---

## Product Boundary

### Shared: `omnigraph-compiler`

- schema AST/parser (`.pg`) with constraint system, interfaces, edge cardinality
- interface declarations with compile-time property verification
- edge cardinality model (`@card(min..max)`)
- query AST/parser (`.gq`)
- typechecker (read queries + mutations)
- catalog builder
- IR lowering (QueryIR + MutationIR)
- type system (ScalarType incl. Blob, PropType, Direction)
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
- `ensure_indices()` — BTree, Inverted, and Vector ANN index creation from active schema constraints
- Lance-native branching, tagging, shallow clone (planned)
- CLI (planned)

---

## Schema Language

The `.pg` schema defines typed nodes, edges, constraints, and metadata. Constraints use `@` syntax and can appear in two positions:

- **Property-level** (shorthand): `name: String @key` — applies to that property
- **Body-level** (composites, value constraints): `@unique(first_name, last_name)` — references properties by name

Both positions produce the same AST representation. Property-level is sugar for body-level with one argument.

### Type Declarations

```
node TypeName @description("...") {
    prop: Type                               // required
    prop: Type?                              // nullable
    prop: Type @key                          // property-level constraint (shorthand)
    prop: Type @unique @index                // multiple constraints on one property

    @unique(p1, p2)                          // body-level: composite uniqueness
    @index(p1, p2)                           // body-level: composite index
    @range(prop, 0..100)                     // body-level: value bound
    @check(prop, "[A-Z]{3}-[0-9]+")          // body-level: regex pattern
}

edge TypeName: From -> To @card(0..1) {
    prop: Type? @index                       // property-level works on edges

    @unique(src, dst)                        // body-level: prevent duplicate edges
}
```

### Property Types

| Type | Syntax | Arrow Mapping | Notes |
|---|---|---|---|
| String | `String` | `Utf8` | |
| Bool | `Bool` | `Boolean` | |
| I32 | `I32` | `Int32` | |
| I64 | `I64` | `Int64` | |
| U32 | `U32` | `UInt32` | |
| U64 | `U64` | `UInt64` | |
| F32 | `F32` | `Float32` | |
| F64 | `F64` | `Float64` | |
| Date | `Date` | `Date32` | Days since epoch. JSONL: `"2024-01-15"` |
| DateTime | `DateTime` | `Date64` | Milliseconds since epoch. JSONL: `"2024-01-15T12:00:00Z"` |
| Vector | `Vector(N)` | `FixedSizeList(Float32, N)` | N-dimensional float vector for embeddings |
| Blob | `Blob` | Lance blob v2 struct | Large binary objects. See Blob Type section |

**Modifiers:**
- `Type?` — nullable (column allows null values)
- `[Type]` — list (Arrow `List<Type>`). Cannot contain Blob or Vector.
- `enum(a, b, c)` — string enum. Stored as `Utf8`, validated at load/mutation time. Values sorted and deduplicated.

All properties without `?` are required — the loader rejects null values for non-nullable properties.

### Interfaces

Interfaces define shared property contracts. A node type declares `implements` to compose one or more interfaces:

```
interface Slugged {
    slug: String @key
}

interface Described {
    title: String
    description: String?
}

node Signal implements Slugged, Described {
    strength: F64
    source: String @index
}

node Pattern implements Slugged, Described {
    category: String
}
```

**Phase 1 (Step 7a, complete): compile-time property verification.** The parser verifies that Signal has all properties declared in Slugged and Described (or injects them if missing). The catalog sees a flat `NodeType` — no runtime awareness of interfaces. This is mixin behavior with interface syntax.

**Phase 2 (deferred): polymorphic queries and edge targets.** Same schema, no syntax changes. The catalog stores interface metadata. The typechecker allows `$e: Slugged` in queries (multi-dataset scan + union, projecting only interface properties). Edges accept interface types: `edge Mentions: Signal -> Slugged`.

**Rules:**
- Interfaces can declare properties with annotations (`@key`, `@unique`, `@index`, `@embed`)
- Interfaces cannot declare constraints, edges, or cardinality
- A node can implement multiple interfaces (composition, not hierarchy)
- Property name conflicts between interfaces: same name + same type = ok (deduplicated), same name + different type = parse error
- `implements` is optional — nodes without it work exactly as before

### Constraints

| Constraint | Property-level | Body-level | Targets | Effect |
|---|---|---|---|---|
| `@key` | `name: String @key` | `@key(name)` | nodes | Value becomes `id` column, implies index, at most 1 per type |
| `@unique` | `email: String @unique` | `@unique(a, b)` | nodes, edges | Enforce uniqueness on column(s) |
| `@index` | `name: String @index` | `@index(a, b)` | nodes, edges | BTree (scalar), Inverted (String), IVF-HNSW (Vector) |
| `@range` | — | `@range(age, 0..200)` | nodes | Validate value within bounds at load/mutation |
| `@check` | — | `@check(code, "[A-Z]+")` | nodes | Validate value matches pattern at load/mutation |
| `@embed` | `v: Vector(N) @embed(src)` | — | nodes | Auto-generate embedding from source property |

`@range` and `@check` are body-level only (they require arguments). `@embed` is property-level only (it applies to one vector property).

**Enforcement:**
- `@key`: validated at parse time (non-nullable, non-list, non-vector, non-blob, at most 1 per type). At runtime, `@key` value becomes the `id` column; `@key` types use `merge_insert` (upsert) on insert.
- `@unique`: validated at parse time (properties exist). `@key` uniqueness is enforced indirectly through the `id`-keyed write path; composite and non-key `@unique(...)` enforcement is not part of the active compiled runtime yet.
- `@index`: validated at parse time (properties exist, not blob). `ensure_indices()` creates BTree, Inverted, and Vector ANN indices in the active runtime.
- `@range`: validated at parse time (numeric property). In the active runtime, both the loader and mutation insert/update paths check every row.
- `@check`: validated at parse time (String property, valid regex). In the active runtime, both the loader and mutation insert/update paths check every row.
- `@embed`: validated at parse time (target is Vector, source is String, source exists). Automatic embedding materialization is not part of the active compiled loader path yet.

### Blob Type

The `Blob` scalar type stores large binary objects (images, videos, documents, model artifacts) via Lance blob v2. Omnigraph treats blobs as first-class graph properties — they participate in Lance's lifecycle governance, are auto-cleaned on delete cascade, and support mixed storage strategies within a single column.

#### Storage Semantics

Lance blob v2 auto-selects storage strategy by size. All four semantics can coexist within one column:

| Blob Size | Storage Semantic | Physical Layout |
|---|---|---|
| ≤ 64 KB | Inline | In the main data file (locality, zero overhead) |
| 64 KB – 4 MB | Packed | Shared `.blob` sidecar files up to 1 GiB (batch throughput) |
| > 4 MB | Dedicated | Individual `.blob` file per blob (operability, range reads) |
| External URI | External | URI reference only, no data copied (interop with existing assets) |

Thresholds are Lance defaults, configurable via schema metadata.

**On-disk descriptor** (Lance blob v2 struct, `ARROW:extension:name = "lance.blob.v2"`):

| Field | Type | Description |
|---|---|---|
| `kind` | UInt8 | 0=Inline, 1=Packed, 2=Dedicated, 3=External |
| `position` | UInt64 | Byte offset within file |
| `size` | UInt64 | Blob size in bytes |
| `blob_id` | UInt32 | Sidecar file reference (Packed/Dedicated) or base ID (External) |
| `blob_uri` | Utf8 | URI or relative path (External only; empty for managed blobs) |

#### Schema Declaration

```
node Document {
    title: String @key
    content: Blob?              // nullable blob
    thumbnail: Blob             // required blob
}
```

**Restrictions:** `@key`, `@unique`, `@index`, `@embed` are not supported on Blob properties. `[Blob]` (list of Blob) is not supported. Blob properties cannot appear in match patterns or filter predicates.

#### Compiler Boundary

The `omnigraph-compiler` crate has zero Lance dependency. `ScalarType::Blob` maps to `DataType::LargeBinary` as a placeholder in `to_arrow()`. The `omnigraph` crate's `fixup_blob_schemas()` replaces these with the real Lance blob v2 struct type via `lance::blob::blob_field(name, nullable)` during `init()` and `open()`. The catalog tracks blob columns in `NodeType::blob_properties: HashSet<String>` and `EdgeType::blob_properties: HashSet<String>`.

#### JSONL Loading

Blob values in JSONL are strings — either base64-encoded data or URI references:

```jsonl
{"type": "Document", "data": {"title": "readme", "content": "base64:SGVsbG8gV29ybGQ="}}
{"type": "Document", "data": {"title": "photo", "content": "s3://bucket/photo.jpg"}}
{"type": "Document", "data": {"title": "local", "content": "file:///path/to/doc.pdf"}}
{"type": "Document", "data": {"title": "empty"}}
```

- `base64:` prefix → decoded to bytes, stored via `BlobArrayBuilder::push_bytes()` (Lance selects Inline/Packed/Dedicated by size)
- Any other string → treated as URI, stored via `BlobArrayBuilder::push_uri()` (External semantic)
- `null` or missing → `push_null()` (only for nullable Blob properties)

All write paths set `allow_external_blob_outside_bases: true` to permit arbitrary external URIs.

#### Query Projection

Blob columns are excluded from Lance scan projections. This is a Lance constraint: `BlobHandling::BlobsDescriptions` works for unfiltered scans but triggers an assertion when combined with `scanner.filter()` (Lance 3.0.1 bug). Query results return `null` for blob columns. Blob data is accessed separately via `read_blob()`.

#### Blob Read API

```rust
impl Omnigraph {
    /// Read a blob by node type, string ID, and property name.
    /// Returns a BlobFile handle — no bytes copied until read().
    pub async fn read_blob(
        &self,
        type_name: &str,   // e.g. "Document"
        id: &str,          // e.g. "readme" (@key value or ULID)
        property: &str,    // e.g. "content"
    ) -> Result<BlobFile>;
}
```

**BlobFile** is Lance's async file-like handle:

| Method | Description |
|---|---|
| `size() -> u64` | Blob size in bytes (metadata, no I/O) |
| `kind() -> BlobKind` | Storage semantic: Inline, Packed, Dedicated, External |
| `uri() -> Option<&str>` | URI for External blobs |
| `read() -> Result<Bytes>` | Read entire blob from cursor to end |
| `read_up_to(len) -> Result<Bytes>` | Read up to N bytes |
| `seek(pos) -> Result<()>` | Seek to byte position |
| `tell() -> Result<u64>` | Current cursor position |
| `close() -> Result<()>` | Release resources |

**Implementation path**: scan for node by `id` with `with_row_id()`, extract the stable row ID, call `Dataset::take_blobs(&[row_id], property)`.

#### Mutation Support

**INSERT**: Blob values accepted as `String` parameters (URI or `base64:` prefixed). The typechecker allows `String` → `Blob` assignment in mutation contexts. `build_insert_batch()` detects blob columns via `blob_properties` and uses `BlobArrayBuilder` instead of `literal_to_typed_array()`.

**UPDATE**: Two-phase execution:
1. Scan non-blob columns (blob columns excluded from projection) → apply scalar assignments → `merge_insert` keyed by `id`
2. If blob assignments exist: extract matched IDs, build a separate batch with `id` + blob columns using `BlobArrayBuilder`, execute a second `merge_insert` keyed by `id`

This avoids the fundamental constraint that blob columns can't be read through filtered scans and written back through merge_insert.

**DELETE**: Node deletion cascades to edges normally. Lance handles blob sidecar cleanup via version-aware garbage collection — no Omnigraph-level blob cleanup needed.

**Blob properties cannot be used in WHERE predicates** (rejected by typechecker).

#### Lifecycle Governance

Lance blob v2 provides automatic lifecycle governance:
- **Delete cascade**: when a node is deleted, its blob sidecars are tracked via fragment metadata. Lance's version cleanup GCs orphaned `.blob` files.
- **Version travel**: `Snapshot::open()` pins blob columns to their version — no stale blob references.
- **No manual cleanup**: blob sidecars follow the same lifecycle as rows. No separate manifest, no reconciliation scripts.

Enforcement is strict: violations are hard errors at load and mutation time.

### Edge Cardinality

```
edge WorksAt: Person -> Company @card(0..1)      // at most one employer
edge Knows: Person -> Person                      // default: @card(0..)
edge Authored: Person -> Paper @card(1..)         // at least one author required
```

Enforced at: loader (post-write validation), mutation insert (max bound check), mutation delete (min bound check).

### Annotations (Metadata)

Annotations provide metadata that does not affect storage or constraint enforcement:

| Annotation | Targets | Purpose |
|---|---|---|
| `@description("...")` | nodes, edges, properties | Human-readable documentation |
| `@instruction("...")` | nodes, edges | Agent/LLM guidance for query generation |
| `@rename_from("...")` | nodes, edges, properties | Schema evolution — maps old name to new |

`@description` can appear on both types and individual properties. `@instruction` is type-level only (not on properties). Each can appear at most once per target.

### Example Schema

```
interface Named {
    name: String @key
}

interface Described {
    title: String
    description: String?
}

node Person implements Named @description("A person in the graph") {
    email: String @unique
    age: I32? @index
    bio: String?
    avatar: Blob?
    embedding: Vector(384) @embed(bio)

    @unique(name, email)
    @index(name, age)
    @range(age, 0..200)
}

node Company implements Named @description("An organization") {
    industry: String?
}

node Signal implements Described {
    slug: String @key
    strength: F64
    source: String @index
}

edge WorksAt: Person -> Company @card(0..1) {
    since: Date? @index
}

edge Knows: Person -> Person {
    @unique(src, dst)
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
  └── ensure_indices()             (async — create schema-driven Lance indices + commit manifest updates)

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
    cached_graph_index: Option<CachedGraphIndex>,
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
    pub async fn run_mutation(&mut self, source: &str, name: &str, params: &ParamMap) -> Result<MutationResult>;
    pub async fn read_blob(&self, type_name: &str, id: &str, property: &str) -> Result<BlobFile>;
    pub async fn ensure_indices(&mut self) -> Result<()>;    // BTree + Inverted + Vector ANN; commits manifest updates
    pub fn catalog(&self) -> &Catalog;
    pub fn uri(&self) -> &str;
    pub fn version(&self) -> u64;
    pub fn manifest_mut(&mut self) -> &mut ManifestCoordinator;
}
```

**Current gaps / TODOs:**
- The public API is still ambient-handle based. Explicit branch/snapshot targets are a later API step, not the current runtime shape.
- `init(uri)` / `open(uri)` use URI-shaped signatures, but `_schema.pg` I/O is still local-path-only today.
- `manifest_mut()` is a prototype escape hatch and not a stable long-term coordinator boundary.

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

The graph index is a handle-local cache keyed by active branch plus current edge sub-table state. It is:
- **Built lazily** on first traversal query via `graph_index()`
- **Shared** across queries via `Arc`
- **Invalidated** by setting to `None` after edge mutations and edge-affecting merges
- **Dropped on refresh** when the manifest shows different edge table branch/version state than the cached key

This keeps the single-slot cache semantically correct for the current one-handle / one-target runtime. A multi-target cache map remains a future optimization, not a correctness gap.

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
├── omnigraph-compiler/        # No Lance dependency. 149+ tests. Blob maps to LargeBinary placeholder.
│   └── src/
│       ├── schema/            # .pg parser, AST, pest grammar, Constraint/Derivation/Cardinality types
│       ├── query/             # .gq parser, AST, typechecker, pest grammar
│       ├── catalog/           # Catalog, build_catalog (key, unique, index, range, check, cardinality, blob_properties)
│       ├── ir/                # QueryIR, MutationIR, lowering
│       ├── types.rs           # ScalarType (incl. Blob), PropType, Direction
│       ├── error.rs           # NanoError, ParseDiagnostic
│       ├── embedding.rs       # OpenAI embedding client
│       ├── json_output.rs     # Arrow → JSON (JS-safe integers)
│       ├── result.rs          # QueryResult, MutationResult, RunResult
│       └── query_input.rs     # Named query lookup, param parsing, params! macro
│
├── omnigraph/                 # The database. Lance-dependent. 85+ tests. Blob v2 via fixup_blob_schemas().
│   └── src/
│       ├── db/
│       │   ├── manifest.rs    # Snapshot, ManifestCoordinator (known_state, snapshot, refresh, commit)
│       │   └── omnigraph.rs   # Omnigraph handle (init, open, run_query, run_mutation, read_blob, graph_index, ensure_indices, fixup_blob_schemas)
│       ├── exec/
│       │   └── mod.rs         # execute_query, execute_mutation, execute_expand, execute_anti_join, hydrate_nodes
│       ├── graph_index/
│       │   └── mod.rs         # GraphIndex, TypeIndex (String↔u32), CsrIndex (CSR adjacency)
│       └── loader/
│           └── mod.rs         # load_jsonl, load_jsonl_file, build_node_batch, build_blob_column, validate_constraints
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
├── _graph_commits.lance/               # Tiny graph commit DAG for merge-base resolution
│   ├── _versions/
│   ├── _refs/branches/
│   └── data/
├── nodes/
│   ├── {hash}/                         # Per-type dataset (FNV-1a hash of type name)
│   │   ├── _versions/
│   │   ├── data/
│   │   │   └── {data_file_key}/
│   │   │       └── *.blob              # Blob v2 sidecar files (packed/dedicated)
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

**Target contract:** the manifest advance is the graph-level publish point. Sub-table writes and graph-level validation must complete before a new manifest version becomes visible.

The active JSONL load path stages writes, validates edge endpoints and edge cardinality, and advances the manifest only after validation passes.

### Sub-Table Schemas

**Nodes** (from `Catalog::node_types[name].arrow_schema`):

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | `@key` value or ULID. BTree index. |
| `{property}` | typed | One column per `.pg` property |
| `{blob_property}` | Struct (lance.blob.v2) | Blob v2 descriptor: kind, position, size, blob_id, blob_uri |

**Edges** (from `Catalog::edge_types[name].arrow_schema`):

| Column | Type | Notes |
|---|---|---|
| `id` | Utf8 | ULID. |
| `src` | Utf8 | Source node `id`. BTree index. |
| `dst` | Utf8 | Destination node `id`. BTree index. |
| `{property}` | typed | Edge properties |

All datasets created with `enable_stable_row_ids = true`, `LanceFileVersion::V2_2`, and `allow_external_blob_outside_bases = true` (permits external blob URI references without registered base paths).

### Indices

`Omnigraph::ensure_indices()` creates indices after data load.

**Always created (structural):**
- Node tables: BTree on `id`
- Edge tables: BTree on `src`, BTree on `dst`

**From schema constraints (`@index`):**
- Scalar properties → `IndexType::BTree`
- String properties → `IndexType::Inverted` (for FTS)
- Vector properties → `IndexType::IvfHnswPq` (for ANN)
- Composite indices: `@index(p1, p2)` → multi-column BTree
- Edge property indices: `since: Date? @index` inside edge body → BTree on edge property columns

Uses `lance_index::DatasetIndexExt::create_index_builder`. Idempotent (`replace=true`).

**Current gap / TODO:** the active runtime currently materializes:
- structural BTree indices on `id`, `src`, and `dst`
- single-column String `@index` as Inverted / FTS
- single-column Vector `@index` as ANN

Non-String scalar `@index`, composite `@index(p1, p2)`, and schema-driven edge-property `@index` materialization remain TODO.

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
- `@key` types use the key property value as `id` (e.g., `name: String @key` → `id = "Alice"`)
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

The current design stores all IDs as Utf8 strings — 26-byte Crockford Base32 for ULIDs, variable-length for `@key` values. This prioritizes simplicity and debuggability at v0.1.0 but has known performance costs at scale:

- **Space:** Each edge row carries 78+ bytes of string IDs (`id` + `src` + `dst`). Binary ULIDs (`FixedSizeBinary(16)`) would cut this to 48 bytes — a 38% reduction on the ID columns.
- **Speed:** String hashing/comparison for `TypeIndex` build, `merge_insert` keyed by `id`, and edge-to-node joins is ~3-5x slower than fixed-width binary comparison.
- **GraphIndex build:** `HashMap<String, u32>` requires heap-allocated keys. `HashMap<u128, u32>` would be inline and faster to hash.

**Recommended migration path** (not before Step 6 profiling justifies it):

1. Separate external identity (`@key` — user-facing, indexed, human-readable) from internal identity (`id` — binary ULID, `FixedSizeBinary(16)`).
2. Edge `src`/`dst` store binary ULIDs, not `@key` strings.
3. `@key` property becomes a unique indexed column, not the `id` value.
4. `TypeIndex` becomes `HashMap<u128, u32>` — faster build, less memory.
5. Lookup by `@key` goes through a Lance scalar index on that column.

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
| `NodeScan` | `execute_node_scan()` — opens dataset via `snapshot.open()`, pushes SQL filter. Blob columns excluded from projection (returns null placeholder). | `Dataset::scan().project(non_blob_cols).filter(sql)` |
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

## Branching Model

### Lance-Native Branching

Branches are Lance branches on the manifest table. Sub-tables are branched lazily.

**Lance APIs available** (verified in lance 3.0.1 Rust API):
- `Dataset::create_branch(name, version, None)` — zero-copy branch
- `Dataset::checkout_branch(name)` — open at branch
- `Dataset::list_branches()` — enumerate branches as `HashMap<String, BranchContents>`
- `Dataset::delete_branch(name)` — remove branch
- `Dataset::shallow_clone(dest, base_paths)` — cross-location clone

**Graph ancestry:**
- Omnigraph keeps a tiny `_graph_commits.lance` dataset to track graph commit ancestry
- This is separate from per-table Lance history and exists to answer one question correctly: "what is the merge base?"
- Each row records `graph_commit_id`, `manifest_branch`, `manifest_version`, `parent_commit_id`, optional `merged_parent_commit_id`, and `created_at`
- `_graph_commits.lance` is branched eagerly with `_manifest.lance` because it is small and is required for branch merge

**Create branch:**
1. `_manifest.lance` dataset: `ds.create_branch("experiment", version, None)` (zero-copy, inherits all rows)
2. `_graph_commits.lance` dataset: `ds.create_branch("experiment", version, None)`
3. No sub-tables branched yet — `table_branch` remains null

**First write on branch:**
1. Sub-table has no branch → create Lance branch on the sub-table
2. Write to the branched sub-table
3. Update manifest row: set `table_branch`, commit manifest on the branch
4. Append a graph commit row to `_graph_commits.lance`

**Read from branch:**
1. Open manifest at the branch: `ds.checkout_branch("experiment")`
2. For each sub-table: if `table_branch` is null, open at `table_version` on main. If set, checkout that branch.

**Merge:**
1. Resolve `source_head`, `target_head`, and nearest common ancestor from `_graph_commits.lance`
2. Preflight outcome:
   - `source_head == target_head` or `merge_base == source_head` → `MergeOutcome::AlreadyUpToDate`
   - `merge_base == target_head` → `MergeOutcome::FastForward`
   - otherwise → `MergeOutcome::Merged`
3. Open base/source/target manifest states
4. For each sub-table:
   - source-only change → adopt source state into the target
   - target-only change → keep target
   - both changed → ordered streaming three-way diff keyed by persisted `id`
5. Validate the candidate merged graph
6. Publish:
   - named targets shallow-clone adopted branch-owned sub-tables into target-owned branch state
   - `main` rewrites adopted branch-owned sub-table state into the main dataset head
   - doubly changed tables stage merged rows in temp Lance datasets with bounded chunking, then rewrite only those target tables
   - rewritten tables rebuild search/scalar indices before the manifest commit
   - commit target manifest once, append one merge commit row

**V1 conflict policy:**
- same `id` changed differently on both sides → conflict
- delete vs update on the same `id` → conflict
- same `id` inserted differently on both sides → conflict
- orphan edge introduced by merge → conflict
- post-merge `@unique(...)` / `@card(...)` violations → conflict

This is intentionally conservative. The architecture supports finer-grained property-wise auto-merge later without changing the storage model.

**Current implementation detail:** ordered merge scans use `ORDER BY id`. If a table’s `id` ordering is not index-backed at read time, Lance may pay a full scan + sort cost. This is a performance concern, not a semantic one.

Sub-tables not written to on a branch are never branched. Storage overhead is proportional to what changed.

---

## Mutation Model

### Pipeline

`Omnigraph::run_mutation()` → parse → typecheck → lower → `execute_mutation()`.

**Grammar**: `insert Type { prop: value }`, `update Type set { prop: value } where pred`, `delete Type where pred`

**IR**: `MutationIR { name, params, op: MutationOpIR }` with `Insert/Update/Delete` variants. `IRAssignment { property, value: IRExpr }`, `IRMutationPredicate { property, op, value: IRExpr }`.

**Typechecker**:
- Insert validates all non-nullable properties provided
- Update validates property types, rejects edge updates (T16)
- Delete validates predicate for both nodes and edges

**Target contract:** edge writes must validate `from` / `to` endpoint existence and schema-declared endpoint type before publish.

The active runtime validates endpoint existence/type on JSONL edge loads and edge mutation inserts before publish.

### Runtime

**Insert**: Build single-row RecordBatch → `@key` types use `merge_insert` (upsert), keyless use `append` → commit manifest. Edge inserts invalidate graph index. Blob properties use `BlobArrayBuilder` (accepting URI or base64 string values).

Edge inserts and edge loads validate endpoint IDs before publish, and branch merge re-validates no-orphan-edge visibility against the merged candidate graph.

**Update**: Two-phase for types with blob properties:
1. Scan non-blob columns (blob columns excluded via `.project()`) → apply scalar assignments → `merge_insert` keyed by `id`
2. If blob assignments exist: extract matched IDs, build blob-only batch with `BlobArrayBuilder`, second `merge_insert` keyed by `id`
For types without blobs: single-phase scan → apply → merge_insert (unchanged). Rejects `@key` property changes.

**Delete with edge cascade**:
1. Scan for matching IDs
2. `Dataset::delete(predicate_sql)` — Lance deletion vectors (soft delete)
3. For each edge type referencing the deleted node type: `edge_ds.delete("src IN (...) OR dst IN (...)")` (BTree indexed)
4. Commit manifest for all changed sub-tables
5. Invalidate graph index: `self.cached_graph_index = None`

### Constraint Enforcement

Current runtime enforcement is split by path:

- **Load path**: `@range` and `@check` are validated in the active JSONL loader, and edge cardinality is validated after edge writes. Violations are hard errors.
- **Mutation path**: `@range` and `@check` are validated before write, `@key` immutability is enforced, edge-delete cascade is enforced, and `@key` node inserts/upserts use `merge_insert` keyed by `id`.
- **Unique constraints**: the active compiled runtime does not yet perform the post-write duplicate scans described for `@unique(...)`; that remains deferred work beyond the Step 8a cleanup.
- **Embedding materialization**: automatic `@embed` generation is not part of the active compiled loader path.
- **Key immutability**: updates to `@key` properties are rejected by the typechecker (T16-style check).

---

## Search Model

Steps 8, 8a, 8b, and 9 are complete. Search/runtime parity, publish-safe graph writes, and branch-aware runtime semantics are in the active implementation.

| Predicate | Current runtime | Notes |
|---|---|---|
| `search()` | FTS query via `FullTextSearchQuery` | Uses Inverted indices when available |
| `fuzzy()` | Fuzzy FTS query via `FullTextSearchQuery::new_fuzzy` | Uses the same Lance FTS path |
| `match_text()` | Currently the same FTS path as `search()` | True phrase semantics are deferred until the Lance Rust API exposes them cleanly |
| `bm25()` | FTS with BM25 scoring | Uses Lance ranking support |
| `nearest()` | `scanner.nearest(column, vector, k)` | Query-time ANN path works and `ensure_indices()` now builds ANN indices for `Vector @index` |
| `rrf()` | Application-level score fusion | Fuses two ranked result sets in Omnigraph |

Index creation in the active runtime matches the current schema contract for active index types:
- `ensure_indices()` creates BTree indices on `id` / `src` / `dst`
- `ensure_indices()` creates Inverted indices on single-column String `@index` properties
- `ensure_indices()` creates Vector ANN indices on single-column `Vector @index` properties
- Index creation commits the new sub-table versions back into the manifest so snapshot-pinned reads target the indexed version

The compiler IR and the runtime executor support `search` / `fuzzy` / `match_text` / `bm25` / `nearest` / `rrf`. `match_text()` remains a documented FTS fallback until Lance exposes a clean phrase-search API in Rust.

---

## Consistency Model

### Core Principle

**A manifest version IS a database version.** One manifest version pins a consistent set of sub-table versions. That pinned set is an immutable `Snapshot` — the unit of read consistency.

### Snapshot Isolation

`Snapshot` is created from `ManifestCoordinator::known_state` — sync, no storage I/O. It holds `version: u64` + `entries: HashMap<String, SubTableEntry>`. `open(table_key)` resolves the path and checks out the pinned `(table_branch, table_version)` state for that sub-table.

**Within a query:** The executor takes one Snapshot at query start. All `NodeScan`, `Expand`, and `AntiJoin` operations read through it. Concurrent writes are invisible.

**Across queries:** Each query takes its own Snapshot from the handle's known version. Successive queries see the latest committed state (if the handle committed) or the last known state (if another writer committed and `refresh()` hasn't been called).

**Current limitation:** the cached `GraphIndex` is still a single slot per handle. It is branch-aware through the cache key, but the runtime does not yet keep a multi-target cache pool.

### Multi-Reader

Multiple readers call `snapshot()` concurrently — each gets an immutable value. Snapshots are `Clone + Send + Sync`. Readers never block writers. This is Lance MVCC for free.

### Multi-Writer

Multiple writers each open their own `Omnigraph::open(uri)` handle. When two writers commit:

- **Different sub-tables**: Lance merges manifest rows automatically (`merge_insert` by `table_key` is non-conflicting for different keys)
- **Same sub-table**: Lance's optimistic concurrency handles it (Append+Append is rebasable, same-row Update is retryable)
- **Branch merge conflicts**: handled at the Omnigraph layer by graph-level three-way merge keyed by persisted `id`
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

**Scope note:** this is a later table/write-path optimization after branching. It does not change the Step 9 graph-coordination design.

**Architecture:**

Each sub-table dataset (the "base table" in MemWAL terms) can independently have MemWAL enabled. Writers write to an in-memory MemTable backed by a durable WAL per region. MemTables flush to storage periodically; flushed MemTables merge into the base table asynchronously via `merge_insert`. The same Lance API surface (`append`, `merge_insert`, `delete`) works transparently — MemWAL routes writes through the WAL internally. Omnigraph does not build a custom WAL.

**Region model and primary keys:**

- Each MemWAL region has exactly one active writer at a time, enforced by epoch-based fencing
- **Correctness invariant:** rows with the same primary key must go to the same region — otherwise merge order between regions can corrupt "last write wins" semantics
- Omnigraph's `id` column (String, `@key` value or ULID) serves as the unenforced primary key required by MemWAL
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
Steps 0–9 ✅ (283 tests)
     └→ Step 10: Change tracking + CLI
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
12. **Blob metadata in query projections.** Query projections currently return `null` for blob columns. Lance 3.0.1 has a bug where `BlobHandling::BlobsDescriptions` + `scanner.filter()` triggers a projection assertion (`field.rs:277`). Without a filter, blob descriptors scan correctly as `Struct<kind, position, size, blob_id, blob_uri>`. When Lance fixes this, remove the `.project()` exclusion in `execute_node_scan` and `hydrate_nodes`, replace with `scanner.blob_handling(BlobHandling::BlobsDescriptions)`, and the `DataType::Struct` handler in `json_output.rs` will serialize blob metadata automatically.
12. **Composite key enforcement at Lance level.** Step 7a adds composite key syntax (`@key(tenant, slug)`) to the schema language, but the runtime still uses a single `id` column. Full composite key support (multi-column unique constraint at the Lance level) is deferred.
13. **Cross-branch cardinality validation.** Edge cardinality is validated per-branch. Cross-branch cardinality semantics after merge are deferred.
14. **Polymorphic interface queries.** Phase 1 treats interfaces as compile-time property verification (mixin behavior). Phase 2 enables `$e: InterfaceName` in queries (multi-dataset scan + union) and interface types as edge targets (`edge E: A -> InterfaceName`). Requires executor changes for multi-type scan and graph index cross-type awareness.
