# Omnigraph Lance v4 Adoption

What changes in Lance SDK v4.0.0, what matters for Omnigraph, and how to adopt it.

---

## Version Disambiguation

Lance has three independent version tracks. "v4" is the SDK version, not a new file format.

| Track | Current (us) | Latest | What it versions |
|---|---|---|---|
| **SDK (lance crate)** | `3.0.1` | `4.0.0` (2026-03-30) | Rust/Python/Java API surface |
| **File format** | `2.2` | `2.2` stable, `2.3` introduced | On-disk columnar encoding |
| **Table format** | feature-flag based | same | Manifest, transactions, MVCC |

SDK v4 is a semver major bump for breaking API changes. The on-disk format is backwards compatible — existing data works without migration.

---

## v4 Breaking Changes

### DataFusion upgrade to 52.1.0

The biggest API break. We already pin `datafusion-* = "52"` and resolve to `52.4.0` in our lockfile. **Already compatible** — the upgrade should be a version bump with no code changes on the DataFusion side.

### `create_empty_table` removed

We don't use it. Our `create_empty_dataset()` in both `table_store.rs` and `manifest.rs` writes an empty `RecordBatch` via `Dataset::write()`. No impact.

### FTS indexing rework

`skip_merge` flag removed (merge phase eliminated — 2.5x faster, 60% less memory). Default workers changed to half available cores. We don't currently use Lance FTS indices (our search is brute-force), but this matters when we adopt Lance-indexed search per the alignment doc.

### IVF_RQ index version bump

Newly-created delta indices declare a minimum required Lance version. Older Lance versions may not read them. One-directional — fine if we commit to v4, but newly-built ANN indices won't be readable by lance 3.x.

### Index segment model / distributed indexing API

`staging_index_uuid` removed, `partial_indices` renamed to `segments`. Segments written directly to final locations. We don't use distributed indexing today.

### Java file format version access refactored

`LanceFileVersion` enum replaced with string constants on the Java side. No Rust impact.

---

## v4 Non-Breaking Features

- **File format 2.3 introduced** (2.2 marked stable)
- **Atomic multi-table transactions via namespace manifest** (the big one — see below)
- **JSONB literal support** in Lance SQL planner
- **Blob v2 improvements**: distributed blob sidecar keys, multi-base-aware blob reads
- **HNSW-accelerated partition assignment** for fp16 vectors
- **Index prewarm** for IVF-based ANN indices
- **abfss:// Azure ADLS Gen2 support**
- **Compaction options in manifest config**
- **~50% faster BM25/WAND queries**
- **~300% faster 2.2 scans** via spawning structural decode batch tasks

---

## Feature 1: Namespaces (Atomic Multi-Table Transactions)

This is the single most important v4 feature for Omnigraph.

### The problem it solves

Omnigraph has one Lance dataset per graph type (Person, Company, Knows, etc.). Today we coordinate them through a custom `_manifest.lance` table with manual version pinning. There is no atomic guarantee that writing Person version 7 AND Knows version 4 AND advancing the manifest all succeed or all fail together.

### What Lance Namespace V2 provides

A `__manifest` Lance table that tracks all tables in a namespace. With `table_version_management=true`, the `BatchCreateTableVersions` operation provides the atomic cross-table commit:

```
1. Write fragments to Person.lance and Knows.lance in parallel
2. Commit each sub-table individually → Person v7, Knows v4
3. Atomically commit both version entries to __manifest via single merge_insert
   → this is the consistency boundary
```

This replaces our hand-built `ManifestCoordinator` + `_manifest.lance` with a spec-compliant system that has:

- MVCC on the manifest itself (snapshot isolation for readers)
- Time travel across the entire graph state (checkout manifest at any version)
- Conflict detection for concurrent writers
- Hash-based directory naming for S3 throughput
- Compatibility mode for incremental migration from current layout

### Namespace spec layers

1. **Client Spec** — unified abstraction (namespace = catalog/schema/metastore/database)
2. **Directory Namespace** — storage-only, no external service. V1 is flat directory listing; V2 uses `__manifest` Lance table
3. **REST Namespace** — REST-based catalog for enterprise/cloud
4. **Implementation Specs** — integrations with Polaris, Unity, Hive, Glue, Iceberg REST, etc.
5. **Partitioning Spec** — physically separated units sharing a common schema

### Mapping to Omnigraph

| Omnigraph concept | Namespace mapping |
|---|---|
| A repo | A root namespace directory |
| Node type (Person) | A table (`Person.lance`) |
| Edge type (Knows) | A table (`Knows.lance`) |
| A graph snapshot | A `__manifest` version |
| A branch | A Lance branch on `__manifest`, or a child namespace |
| `GraphCoordinator` | Thin wrapper over `DirectoryNamespace` |
| `Snapshot` | Query namespace at a pinned version, resolve table locations |

### V2 manifest table schema

Column | Type | Description
---|---|---
`object_id` | String (PK) | `"Person"` or `"ns1$table_name"` for nested
`object_type` | String | `"namespace"`, `"table"`, or `"table_version"`
`location` | String (nullable) | Relative path to the table directory
`metadata` | String (nullable) | JSON-encoded metadata
`base_objects` | List (nullable) | Reserved (view dependencies)

Table version entries use `object_id = "<table_id>$<version>"` with `object_type = "table_version"`. The `metadata` column stores manifest path, size, e_tag, and naming scheme.

### V2 directory layout

```
my-repo/
  __manifest/                       # Namespace manifest table
    data/...
    _versions/...
  Person.lance/                     # Root namespace table (V1 compat naming)
    data/...
    _versions/...
  a1b2c3d4_Knows/                   # Root namespace table (V2 hash naming)
    data/...
    _versions/...
```

Hash-based directory prefixes maximize S3 throughput by distributing across partitions.

### BatchCreateTableVersions — the atomic commit

With `table_version_management=true`:

1. **Stage manifests**: write new manifest to staging path for each sub-table
2. **Atomic manifest commit**: merge-insert all version rows into `__manifest` in a single commit. PK deduplication on `object_id` prevents duplicates. If any version already exists, the entire batch fails.
3. **Finalize**: copy staged manifests to standard locations
4. **Update pointers**: update `__manifest` metadata to point to finalized paths

Step 2 is the atomic boundary. The manifest commit is the graph's consistency point.

### What this replaces in our codebase

| Current component | Replacement |
|---|---|
| `ManifestCoordinator` | Namespace client |
| `SubTableEntry` / custom manifest schema | `__manifest` rows |
| `GraphCoordinator` manifest arm | Namespace wrapper |
| Custom `_manifest.lance` creation/write | Namespace owns it |
| `Snapshot` construction from manifest | Query namespace at version |

What survives unchanged: `RuntimeCache`, `GraphIndex`, compiler pipeline, executor, `TableStore` (individual dataset I/O), `RunRegistry` (Omnigraph concept above storage), `CommitGraph`.

### Branching via namespace

Two options:

**Option 1 — Lance branches on `__manifest` only**: Each Omnigraph branch = a Lance branch on the manifest table. Sub-tables are shared; only the manifest version pointer diverges. Sub-tables branched lazily on first write (shallow clone). Cheapest to implement.

```
__manifest/
  _versions/*.manifest          # main branch versions
  _refs/branches/feature-x.json # Lance branch metadata
  tree/feature-x/
    _versions/*.manifest        # feature branch versions
```

**Option 2 — Child namespaces per branch**: Each Omnigraph branch = a child namespace with its own `__manifest` and table directories. More isolated but more storage overhead.

Option 1 is better for Omnigraph because it preserves the shared-data model. Branches share sub-table data; only the manifest version pointer diverges. This aligns with the lazy sub-table branching design from `lance-alignment.md`.

---

## Feature 2: MemWAL (WAL Authority)

An experimental LSM-tree architecture for high-throughput streaming writes. Documented at the table format level but implementation is still stabilizing.

### Core concept: regions

A **region** is the horizontal write scale unit. Each region:

- Has a single active writer at any time (epoch-fenced)
- Contains an in-memory MemTable, a WAL, and flushed MemTable files
- Has its own manifest that is authoritative for that region's state
- Invariant: rows with the same primary key MUST go to the same region

### Multi-tier write model

Instead of committing every write directly as a new table version:

```
1. WAL write      — Arrow IPC file in _mem_wal/{region}/wal/. Durability checkpoint.
2. MemTable       — in-memory Arrow batches. Loss-safe because WAL exists.
3. Flushed MemTable — periodic flush to storage as a Lance table (a "generation")
4. Base table     — async background merge via merge-insert
```

### WAL authority

The WAL is the source of truth for durability, not the base table version. A write is committed when it's in the WAL, even before it's merged into the base table. The region manifest tracks which WAL entries have been flushed and which flushed MemTables have been merged.

### Writer fencing (epoch-based)

Writers claim regions by incrementing `writer_epoch` and atomically writing a new region manifest. If `local_writer_epoch < stored_writer_epoch`, the writer is fenced and must abort. This provides single-writer-per-region semantics without external coordination.

### Reader model

Readers merge results across three tiers by primary key:

- Base table (generation 0)
- Flushed MemTables (generations 1+)
- In-memory MemTable (if accessible)

Higher generation wins. Within same generation, higher row address wins.

### Region partitioning

Regions can be partitioned by field values using transforms: `identity`, `year/month/day/hour`, `bucket(N)`, `multi_bucket(N)`, `truncate(W)`, or arbitrary DataFusion SQL expressions.

### Storage layout

```
{table_path}/
  _indices/
    {index_uuid}/
      index.lance              # Region snapshots (if >100 regions)
  _mem_wal/
    {region_uuid}/
      manifest/
        {bit_reversed_version}.binpb
        version_hint.json
      wal/
        {bit_reversed_entry_id}.arrow
      {random_hash}_gen_{i}/   # Flushed MemTable (a Lance table)
        _versions/{version}.manifest
        _indices/{index_name}/
        bloom_filter.bin
```

### Relevance to Omnigraph

For our current workload (batch JSONL loads, run-based mutations via server API), we don't need MemWAL. Our commit path is:

```
load JSONL → write fragments → commit sub-tables → commit manifest
```

Each step is a full MVCC commit. This works at batch scale.

MemWAL becomes relevant when:

- Streaming ingestion (continuous row-level writes, not batch loads)
- Multiple concurrent writers to the same type on the same branch
- Write latencies where a full Lance commit per mutation is too slow

The progression from `lance-alignment.md`:

| Phase | Workload | Mechanism |
|---|---|---|
| Local/agent | Single writer, batch loads | Lance merge_insert + manifest commit |
| Branching | Multiple agents, independent work | Branch-per-writer + merge |
| Collaboration | Multiple writers, same branch, low contention | Lance optimistic concurrency with rebase |
| **SaaS streaming** | **High-throughput ingestion** | **MemWAL per sub-table** |

We are in Phase 1-2. MemWAL is Phase 4.

### How MemWAL would map to Omnigraph if adopted later

- Each graph type (Person, Knows) would have its own MemWAL regions
- Region partitioning could use `@key` values to route rows deterministically
- The graph index would need to merge results from base + flushed MemTables (more complex reads)
- The manifest commit path changes: instead of pinning sub-table versions, we'd track region generation cursors
- Indices maintained across the LSM tree keep search consistent

### Status

Explicitly labeled **experimental** in the spec. The Rust SDK implementation is stabilizing. Not recommended for adoption now.

---

## Other v4 Changes Worth Noting

### Multi-base layout

Lance manifests support a `base_paths` array enabling a single dataset to span multiple storage locations. Developed with Uber for multi-bucket S3 distribution.

```protobuf
message BasePath {
  uint32 id = 1;
  optional string name = 2;
  bool is_dataset_root = 3;
  string path = 4;
}
```

Relocating a 10-million-file dataset requires changing only the few base paths. Enables hot/cold tiering, multi-region distribution, shallow clones, and selective failover.

This is the mechanism that makes shallow clone branching work — branch datasets reference unchanged data through `base_id` back to the source.

### Transaction system enhancements

- Manifest V2 naming: `{u64::MAX - version:020}.manifest` for efficient latest-version discovery via lexicographic listing
- External manifest store protocol for S3/DynamoDB concurrent writes (stage → commit → finalize → synchronize)
- `UpdateMemWalState` transaction type for MemWAL state management
- `IncompatibleTransaction` error type for clearer conflict reporting

### File format 2.2 (stable) / 2.3 (new)

2.2 brings:

- Blob V2: adaptive storage (inline for small, shared blocks for medium, dedicated regions for large, external URI references)
- Nested schema evolution: add fields to nested structs without rewriting parent data
- Native Map type: first-class support
- Enhanced compression: auto LZ4, block-level RLE, dictionary value compression

We already use 2.2. Format 2.3 is introduced in v4 but not yet required.

### Index segments

Indices now composed of independent segments covering disjoint fragment subsets. Each segment has a UUID and can be built/committed independently. Enables distributed index building: workers produce segments, committed as a single logical index via `commit_existing_index_segments`.

---

## Upgrade Impact Assessment

### What breaks on `lance = "3.0"` → `"4.0"`

| Change | Risk | Our exposure |
|---|---|---|
| DataFusion 52.1.0 | **Low** | Already on 52.4.0 |
| `create_empty_table` removed | **None** | We use `Dataset::write()` |
| FTS `skip_merge` removed | **None** | We don't use Lance FTS yet |
| IVF_RQ version bump | **Low** | Forward-only — new indices unreadable by v3 |
| Distributed indexing API renames | **None** | We don't use distributed indexing |
| Java `LanceFileVersion` refactor | **None** | Rust-side unchanged |

### What we already have right

- `enable_stable_row_ids: true` on all datasets (required for CDC, merge, MemWAL)
- `LanceFileVersion::V2_2` for new datasets
- Per-type Lance datasets (exactly how namespaces are designed)
- Version pinning via `checkout_version()` (correct MVCC usage)
- `merge_insert` for `@key` types (intended pattern)

---

## Adoption Plan

### Phase 1: SDK Upgrade

Bump `lance = "4.0"` and all `lance-*` crates. Fix any compile errors. Run full test suite. This is a prerequisite for everything else.

**Scope**: `Cargo.toml` version bumps, potential minor type signature changes.

**Risk**: Low. DataFusion version already aligned. No format changes.

### Phase 2: Adopt Directory Namespace V2

Replace `ManifestCoordinator` with the Lance namespace client. This is the highest-value change.

**Steps**:

1. Verify `lance-namespace` Rust crate availability and API surface (`DirectoryNamespace`, `BatchCreateTableVersions`, branch operations). If not available in Rust, implement against the format spec directly.
2. Create new repos using namespace V2 layout (`__manifest` table with `table_version_management=true`)
3. Replace `ManifestCoordinator` with namespace client wrapper
4. Replace custom `SubTableEntry` tracking with `__manifest` queries
5. Replace `Snapshot` construction to resolve from namespace version
6. Write migration tool for existing repos (read current `_manifest.lance` → populate `__manifest`)

**What changes**:

```
Before:
  write sub-tables → merge_insert our _manifest.lance → done

After:
  write sub-tables → BatchCreateTableVersions on __manifest → done
```

The commit path becomes simpler and gets true atomicity.

**Dependencies**: Phase 1.

### Phase 3: Namespace-Based Branching

Replace directory-copy branching with namespace branches on `__manifest`.

**Steps**:

1. Branch creation: create Lance branch on `__manifest` (zero-copy)
2. Sub-table branching: lazy on first write (shallow clone via `base_paths`)
3. Branch reads: resolve sub-table versions from the branch's manifest version
4. Branch merge: compare manifest versions, three-way diff on divergent sub-tables

**What changes**: Branch creation goes from O(data) to O(1). Storage per branch proportional to what changed.

**Dependencies**: Phase 2.

### Phase 4: MemWAL (Deferred)

Monitor spec stabilization. Only relevant for streaming ingestion workloads.

**Trigger**: When write throughput exceeds what single-writer Lance commits can sustain AND writes must land on the same branch without merge delay.

**Dependencies**: Phase 2. Stable row IDs (already enabled).

---

## Watch Items

### Rust SDK coverage for namespace operations

The `lance-namespace` crate is at v0.6.1 on PyPI. Java and Python SDKs are confirmed. **Verify that the Rust crate exposes `DirectoryNamespace`, `BatchCreateTableVersions`, and branch operations before starting Phase 2.** If not available, we can implement against the format spec directly — it's well-specified — but that's ~500 extra lines.

### MemWAL stability

Labeled experimental. The spec is complete but the SDK implementation is still being refined. v5.0.0-beta.2 (2026-03-27) adds further distributed indexing refinements but MemWAL status is unchanged.

### Forward-only index compatibility

After upgrading, newly-built IVF_RQ indices won't be readable by lance 3.x. If any downstream consumers read our Lance datasets with older lance versions, coordinate the upgrade. Within Omnigraph (single codebase), this is a non-issue.

### Manifest version accumulation

Every mutation advances the `__manifest` version. At 100 mutations/day, manifest versions accumulate (3,000/month). Each is an immutable file (~KB). Mitigation: include `__manifest` in compaction scope. Default retention: 100 versions. Tags exempt from cleanup.
