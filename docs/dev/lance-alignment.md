# Omnigraph–Lance Alignment

Historical note: this document predates the Lance 4 / `__manifest` cutover.
References here to `graph.manifest.json`, `_manifest.lance`, full-directory
branch copies, and “each branch is a namespace” describe older architecture or
earlier design options, not the live runtime. For the current implementation,
see:

- `docs/dev/omnigraph-lance4.md`
- `docs/dev/snapshots.md`
- `docs/dev/soc-lance-manifest.md`
- `docs/dev/namespace-publisher-gap.md`

What must change in Omnigraph's storage, branching, WAL, and manifest layers to align with the Lance format specification — and why.

---

## Background

Omnigraph is a repo-native graph database built on top of Lance. It stores typed property graphs as per-type Lance datasets (one dataset per node type, one per edge type), coordinates them through a custom JSON manifest, records transaction history in a custom JSONL WAL, and implements branching by copying entire database directories.

This document evaluates the current implementation against the Lance format specification (file format, table format, catalog spec) as of Lance v3.0 / format v2.2, identifies gaps, and proposes changes organized by priority and dependency.

---

## Current Architecture

### Storage Layout

```
my-repo/
  _repo.json                     # Omnigraph repo metadata (format version, default branch)
  _schema.pg                     # Graph schema
  _refs/branches/main.json       # Branch head (snapshot ID, parent, dataset refs, schema hash)
  _db/                           # Main branch engine database
    graph.manifest.json           # Omnigraph's JSON manifest (cross-dataset coordination)
    _wal.jsonl                    # Transaction log + inline CDC
    schema.pg                     # Copy of schema
    schema_ir.json                # Compiled schema IR
    nodes/00000064/               # Lance dataset for a node type
      data/*.lance
      _versions/*.manifest
    edges/0000012c/               # Lance dataset for an edge type
      data/*.lance
      _versions/*.manifest
  _db_branches/
    feature-x/                    # Full recursive copy of _db/ for this branch
      graph.manifest.json
      _wal.jsonl
      nodes/00000064/
      edges/0000012c/
```

### Three Coordination Layers

1. **Lance's own manifests** — per-dataset MVCC with protobuf manifests in `_versions/`, transaction files in `_transactions/`, deletion vectors in `_deletions/`. Each Lance dataset is independently versioned with full ACID semantics.
2. **Omnigraph engine manifest** (`graph.manifest.json`) — a JSON file tracking which Lance datasets exist and their current `dataset_version`, plus next-ID counters and schema hash. Written atomically via `write_tmp → fsync → rename`.
3. **Omnigraph repo branch heads** (`_refs/branches/*.json`) — JSON files tracking branch name, parent branch, snapshot ID, schema hash, and a list of `DatasetRef` entries pointing to Lance datasets with their versions.

### WAL (`_wal.jsonl`)

A single JSONL append log inside each database directory. Each line is a `WalEntry`:

```json
{
  "tx_id": "manifest-3",
  "db_version": 3,
  "dataset_versions": { "nodes/00000064": 7, "edges/0000012c": 4 },
  "committed_at": "1700000003",
  "op_summary": "load:merge",
  "changes": [
    {
      "tx_id": "manifest-3", "db_version": 3, "seq_in_tx": 0,
      "op": "insert", "entity_kind": "node", "type_name": "Person",
      "entity_key": "alice", "payload": { "name": "Alice" },
      "committed_at": "1700000003"
    }
  ]
}
```

The commit path appends a WAL entry (with `fsync`), then atomically writes the manifest. On recovery, WAL entries ahead of the manifest's `db_version` are truncated. CDC is read by filtering WAL entries within a version window.

#### Known bugs in the current WAL (from design review)

These are not theoretical — they are concrete issues identified during S3 compatibility analysis:

- `**prune_logs_for_replay_window` non-atomic dual-file rewrite.** The WAL originally had a separate `_tx_catalog.jsonl` and `_cdc_log.jsonl` (now unified into `_wal.jsonl`, but the pruning logic still rewrites entries with updated internal offsets). A crash during rewrite can leave entries pointing to invalid positions — `reconcile_logs_to_manifest` may truncate valid entries or fail to open.
- **No writer fencing.** No mechanism prevents two processes from appending to the same JSONL log concurrently. Interleaved appends corrupt the log structure. Not a problem for single-process embedded use, but a blocker for any multi-process or S3 deployment.
- **Reconcile-on-read side effect.** `read_visible_cdc_entries()` calls `reconcile_logs_to_manifest()`, which truncates the WAL file. Reads mutate the log — incompatible with concurrent readers and impossible on S3 (no truncate operation).
- **S3 incompatible by design.** The WAL relies on file append, file truncation, and byte-offset seeking — none of which exist on S3. The entire approach would need to be replaced for object storage support.

### Branching

Branch creation (`GraphRepo::create_branch`) does a full recursive filesystem copy:

```rust
fn copy_branch_database(&self, from: &str, to: &str) -> Result<()> {
    copy_dir_all(&source_db, &target_db)?
}
```

Main branch data lives at `_db/`, other branches at `_db_branches/{name}/`. Each branch gets a completely independent copy of every Lance dataset, manifest, WAL, and schema file.

---

## What Lance Already Provides

The Lance format specification defines capabilities at three levels that overlap heavily with what Omnigraph implements manually.

### Table-Level MVCC (Lance Table Format)

Every Lance dataset already has:

- **Immutable versioned manifests** in `_versions/` — each commit produces a new protobuf manifest with monotonically increasing version number
- **Transaction files** in `_transactions/` — serialized protobuf `Transaction` messages describing the operation (Append, Delete, Rewrite, Merge, Clone, etc.) with conflict metadata
- **Atomic commits** via `put-if-not-exists` or `rename-if-not-exists` on the manifest file
- **Conflict detection and resolution** — rebasable conflicts (merge deletion vectors), retryable conflicts (re-execute operation), and incompatible conflicts (fail with error)
- **Deletion vectors** — soft-delete rows without rewriting data files, stored as Arrow IPC or Roaring bitmaps
- **Time travel** — any historical version is readable via `checkout_version()`

### Native Branching and Tagging (Lance Table Format)

Lance has a built-in branch and tag system:

- **Branch metadata** at `_refs/branches/{name}.json` within a dataset
- **Branch datasets** at `tree/{branch_name}/` implemented as **shallow clones**
- **Shallow clones** reference original data files through `base_paths` without copying
- **The manifest protobuf** has an `optional string branch` field
- **Tags** at `_refs/tags/{name}.json` pointing to specific versions
- **The `Clone` transaction type** creates shallow or deep copies atomically

The `base_paths` system in the manifest enables multi-location storage:

```protobuf
message BasePath {
  uint32 id = 1;
  optional string name = 2;
  bool is_dataset_root = 3;
  string path = 4;
}
```

Data files reference a `base_id` to resolve their actual location. Branch datasets store new/changed data locally and reference unchanged data back to the source through `base_id`.

### Row-Level Version Tracking (Native CDC)

When stable row IDs are enabled, Lance tracks:

- `_row_created_at_version` — which dataset version a row first appeared in
- `_row_last_updated_at_version` — which dataset version a row was last modified in

CDC queries become standard predicates:

```sql
-- Rows inserted between versions 5 and 10
SELECT * FROM dataset
WHERE _row_created_at_version > 5 AND _row_created_at_version <= 10

-- Rows updated (but not inserted) between versions 5 and 10
SELECT * FROM dataset
WHERE _row_created_at_version <= 5
  AND _row_last_updated_at_version > 5
  AND _row_last_updated_at_version <= 10
```

### MemWAL (Experimental, Lance Table Format)

Lance defines an LSM-tree architecture for high-throughput streaming writes:

- In-memory MemTables with WAL entries as Arrow IPC files
- Region-based write partitioning for horizontal write scale-out
- Epoch-based writer fencing for single-writer-per-region semantics
- Flushed MemTables merged into the base table asynchronously
- Maintained indices (vector, scalar, FTS) kept consistent across the LSM tree

### Index System (Lance Table Format)

Lance supports scalar indices (BTree, bitmap, bloom filter, zone map, label list, N-gram, FTS, R-tree) and vector indices (IVF_PQ, IVF_HNSW_SQ, IVF_RQ, IVF_FLAT). Indices are:

- Stored as Lance files in `_indices/{UUID}/`
- Tracked in the manifest's `IndexSection`
- Loaded on demand (not required to open a dataset)
- Immutable once written; updated by creating new segments
- Fragment-bitmap-aware for handling deleted/updated rows

### Catalog / Namespace (Lance Namespace Spec)

Lance defines a Directory Namespace spec for organizing collections of tables:

- V1: flat directory with `{name}.lance/` convention
- V2: manifest table (`__manifest`) tracking all tables and namespaces with MVCC
- Support for nested namespaces, table version management, shallow cloning across namespaces

---

## Gap Analysis

### 1. Branching: Full Copy vs. Shallow Clone

**Current:** `copy_dir_all()` recursively copies every file — Lance data files, manifests, versions, WAL, everything. Branch creation is O(data size).

**Lance provides:** Shallow clones via the `Clone` transaction type and `base_paths`. A branch dataset at `tree/{branch_name}/` stores only new manifests and changed data. Unchanged fragments resolve back to the source through `base_id` references. Branch creation is O(1) in data size.

**Impact:** For a repo with 10 types and millions of rows, branching currently copies gigabytes. With shallow clones, it would copy only metadata (a few KB). This is the single largest efficiency gap.

**Complication:** Omnigraph has one Lance dataset per type, not one dataset for the whole graph. Lance's native branching operates within a single dataset. Branching the graph means branching every dataset, or restructuring to use fewer datasets.

### 2. Manifest: Three Redundant Coordination Layers

**Current:** Three layers of metadata coordination run in parallel:


| Layer                   | Format   | Scope         | Provides                                                     |
| ----------------------- | -------- | ------------- | ------------------------------------------------------------ |
| Lance dataset manifests | Protobuf | Per-dataset   | MVCC, version history, fragment tracking, schema             |
| `graph.manifest.json`   | JSON     | Cross-dataset | Dataset inventory, pinned versions, ID counters, schema hash |
| Branch head JSON        | JSON     | Cross-branch  | Branch parentage, snapshot ID, dataset refs                  |


**Why this is a problem:**

- The graph manifest re-tracks `dataset_version` numbers that Lance already stores in each dataset's own manifests
- Cross-dataset atomicity is not guaranteed — a crash between writing `nodes/Person` (Lance commit) and writing `graph.manifest.json` (rename) leaves the graph inconsistent
- The branch head re-tracks information that Lance's native branch metadata would handle
- Three separate formats (protobuf, JSON manifest, JSON branch refs) with three separate consistency models
- The custom manifest also carries Nanograph-specific `next_node_id` / `next_edge_id` counters that are tightly coupled to the `u64` identity model — these break when branches diverge and merge (both branches advance the same counter independently)

**What should change:** A single Lance table could serve as the repo-level manifest, providing MVCC and atomic commits for cross-dataset coordination. This is essentially the pattern from the Directory Namespace V2 spec, where a `__manifest` Lance table tracks all datasets.

**Concrete commit protocol** (from the design document): Lance's distributed write API enables parallel sub-table writes with coordinated commit:

```
1. (parallel) write_fragments to nodes/Person.lance  → Person FragmentMetadata
2. (parallel) write_fragments to edges/Knows.lance   → Knows FragmentMetadata
3. Commit each sub-table's fragments (LanceOperation.Append)
   → Person version 7, Knows version 4
4. merge_insert on _manifest.lance:
     ("node:Person", 7, null)
     ("edge:Knows", 4, null)
   → manifest version 12 (this is the repo version)
```

Steps 1-2 run in parallel — `write_fragments` produces fragment metadata without committing. Step 3 commits each sub-table. Step 4 (manifest write) is the atomic commit point. If any step before 4 fails, the manifest still points to old versions. Orphaned sub-table versions are invisible to readers (the manifest never references them) and cleaned up by compaction.

### 3. WAL: Custom JSONL vs. Lance Transactions + Row Versioning

**Current:** A single `_wal.jsonl` file serves two purposes:

1. **Transaction catalog** — which datasets were at which versions for each logical commit
2. **CDC log** — inline JSON payloads of every entity change (insert, update, delete)

The commit path is: append WAL entry (fsync) → write manifest (tmp → fsync → rename).

**What Lance already provides:**

- **Transaction history:** Every Lance dataset commit produces an immutable manifest in `_versions/` and optionally a transaction file in `_transactions/`. The full history of every dataset is already durable and versioned.
- **CDC:** With stable row IDs enabled, row-level `_row_created_at_version` and `_row_last_updated_at_version` columns provide native change tracking queryable through standard predicates.

**What the custom WAL provides that Lance doesn't:** Cross-dataset atomic coordination — a single logical transaction ID grouping writes to multiple Lance datasets. This is the WAL's real value.

**What's redundant:** The inline CDC payloads (full JSON of every changed entity) duplicate what Lance can provide natively through row version columns. This is also the part that makes the WAL grow fastest and requires pruning.

**What's missing:** The WAL has no concurrency primitives. It relies on a process-level `Mutex<()>`. Lance's transaction spec defines conflict detection, rebase mechanisms, and `put-if-not-exists` atomic commits designed for concurrent writers on shared object storage.

**S3 incompatibility:** The WAL's reliance on file append, truncation, and byte-offset seeking makes it fundamentally incompatible with object storage. Lance's storage primitives (`PutObject` for fragments, atomic manifest commits via conditional writes) are S3-native by construction. Replacing the WAL with Lance-native transaction history eliminates an entire category of object storage incompatibility.

### 4. Identity Model: `u64` Internal IDs vs. String Persisted IDs

**Current:** The engine uses numeric `u64` IDs internally (`next_node_id`, `next_edge_id` counters in the manifest), stored as `__ng_id` in Lance datasets. The spec calls for string persisted IDs (`@key` values or ULIDs).

**Why this matters for Lance alignment:** Lance's unenforced primary key system works with any leaf field type. The merge-insert builder already uses string key columns for `@key` types. But the `u64` identity model inherited from Nanograph means:

- ID counters must be coordinated across branches (currently solved by copying the manifest, but broken if branches diverge and merge)
- The identity model is tightly coupled to the single-writer assumption

This is already identified in the Omnigraph spec as a deliberate divergence from Nanograph.

### 5. Concurrency: Mutex vs. MVCC

**Current:** All writes go through a `Mutex<()>` in the `Database` struct. Each write is sequential: load data → write Lance datasets one at a time → update manifest → append WAL. Additionally, the engine explicitly disables Lance's conflict resolution — `run_lance_merge_insert_with_key()` sets `conflict_retries(0)` because the custom manifest is the coordination layer. This means the engine cannot support concurrent writers at all — it's single-writer by design.

**Lance provides:** Full MVCC with optimistic concurrency control per-dataset. Multiple writers can commit to the same dataset concurrently; conflicts are detected and resolved (rebased, retried, or rejected) at commit time. The transaction spec defines detailed compatibility matrices for every operation pair.

The conflict resolution model is more capable than simple optimistic retry:

- **Rebasable** (auto-merged): Append + Append, Delete + Delete on different rows, Append + Delete — Lance merges deletion vectors automatically
- **Retryable** (re-execute): Update + Update on the same row, operations on compacted fragments
- **Incompatible** (fail): Delete targeting rows from a simultaneously-Restored version

For most graph repo workloads (users adding different entities or editing different entities), Lance handles concurrency automatically via rebasing. Only same-row Update + Update requires application-level retry.

**Impact:** The single-writer model works for local-first single-user usage. It breaks for:

- Concurrent branch writes (two users loading data into different branches)
- Remote/shared storage (the spec's vision of pushable repos)
- Any multi-process access

**Progression from the design document:**


| Phase          | Workload                                           | Mechanism                                                                   |
| -------------- | -------------------------------------------------- | --------------------------------------------------------------------------- |
| Local/agent    | Single writer, batch loads                         | Lance `merge_insert` per sub-table + manifest commit                        |
| Branching      | Multiple agents, independent work                  | Branch-per-writer + merge (git model — each writer gets an isolated branch) |
| Collaboration  | Multiple writers, same branch, low contention      | Lance optimistic concurrency with rebase/retry                              |
| SaaS streaming | High-throughput ingestion, many concurrent writers | Lance MemWAL per sub-table (region-based write partitioning, epoch fencing) |


Branches are the primary concurrency primitive for graph repos. MemWAL is only needed when write throughput exceeds what single-writer Lance commits can sustain AND the writes must land on the same branch without merge delay.

### 6. Storage Versioning and Format

**Correct:** New datasets use `LanceFileVersion::V2_2`, the latest file format. Version pinning via `checkout_version()` is used correctly for all reads. Merge-insert uses Lance's native `MergeInsertBuilder`.

**Minor gap:** The engine doesn't use Lance's stable row IDs feature, which would enable native CDC and simplify the identity model for future merge operations.

### 7. Indexing and Search

**Current:** The engine builds CSR (Compressed Sparse Row) topology indices in-memory at runtime for graph traversal. Lance indexing capabilities (BTree, vector, FTS) are available but not explicitly managed at the repo level.

**Search predicates are brute-force.** The engine's `search()`, `fuzzy()`, `match_text()`, and `bm25()` predicates scan every row in memory with no index support. Only `nearest()` uses a Lance ANN index. The implementations in `plan/planner.rs`:

- `search()` — tokenize field + query, check token set intersection. O(rows × tokens).
- `fuzzy()` — tokenize + Levenshtein distance per token pair. O(rows × tokens²).
- `match_text()` — sliding window token match. O(rows × tokens).
- `bm25()` — BM25 scoring computed per row. O(rows × tokens).

**Lance FTS is more capable than assumed.** The Lance FTS spec provides:

- Multiple tokenizers: simple, whitespace, raw, n-gram, Jieba (Chinese), Lindera (Japanese)
- Token filter pipeline: lowercase, stemming (17+ languages), stop words, ASCII folding
- Query types: contains_tokens, match (AND/OR), phrase, boolean (must/should/must_not), multi_match, boost
- BM25 scoring with configurable parameters
- Partitioned indexes for large-scale FTS

With per-type tables, FTS indexes are created directly on property columns within each type's table. The mapping:


| Predicate                   | Current (brute-force)               | Target (Lance-indexed)                        |
| --------------------------- | ----------------------------------- | --------------------------------------------- |
| `search($c.note, $q)`       | Tokenize + set intersection per row | Lance FTS `contains_tokens` on indexed column |
| `fuzzy($c.name, $q)`        | Levenshtein per token pair          | Lance n-gram FTS index                        |
| `match_text($c.note, $q)`   | Sliding window per row              | Lance FTS `phrase` query                      |
| `bm25($c.note, $q)`         | BM25 per row                        | Lance FTS `match` with BM25 scoring           |
| `nearest($c.embedding, $q)` | Lance ANN (already indexed)         | Lance IVF-HNSW (keep)                         |


**Opportunity:** Lance's index system is branch-aware through fragment bitmaps. Index segments track which fragments they cover, and indices can be incrementally updated when fragments change. A repo-native index strategy could leverage this for branch-aware search without rebuilding indices from scratch on each branch.

### 8. Query Execution Model

**Current:** The engine uses custom `ExecutionPlan` implementations (`ExpandExec`, `NodeScanExec` in `plan/physical.rs` and `plan/node_scan.rs`) and a manual plan tree builder in `plan/planner.rs`. This is ~3,200 lines of custom DataFusion operator code.

**What Lance + DataFusion provide natively:** Lance ships a `LanceTableProvider` that registers each Lance dataset as a DataFusion table. With per-type datasets, each graph type becomes a queryable table in a DataFusion `SessionContext`:

```rust
let ctx = SessionContext::new();
lance_datafusion::udf::register_functions(&ctx);  // contains_tokens, BM25, JSON UDFs

for entry in manifest.datasets() {
    let dataset = Dataset::open(&entry.path).await?;
    ctx.register_table(&entry.table_name, Arc::new(LanceTableProvider::new(Arc::new(dataset), true, false)))?;
}
```

This gives full SQL query capability over Lance tables — filter pushdown, column projection, streaming execution, aggregation — all handled by DataFusion with Lance's predicate pushdown optimization. The ~3,200 lines of custom operator code can be replaced by SQL generation from the IR.

**What remains custom:** Graph traversal (multi-hop BFS/DFS on CSR/CSC adjacency indices) has no SQL equivalent. DataFusion does not support recursive graph traversal. The graph index (CSR/CSC + TypeIndex) remains custom in-memory code. The executor orchestrates between DataFusion (tabular operations) and the graph index (topology).

The existing `apply_ir_filters`, `apply_projection`, `apply_aggregation`, `apply_order_and_limit` (~1,700 lines of pure Arrow code) handle graph-specific struct column operations that DataFusion SQL can't express. These are portable and carry forward.

---

## Proposed Changes

### Phase A: Replace Branch Copy with Shallow Clone

**Goal:** Branch creation becomes O(1) in data size.

**Approach:**

For each Lance dataset in the source branch, create a shallow clone using Lance's `Clone` transaction:

1. The cloned dataset references source data files through `base_paths`
2. New writes to the branch create new fragments in the branch's own storage
3. Reads resolve unchanged data back to the source through `base_id`

**Storage layout change:**

```
my-repo/
  _db/                           # Main branch (unchanged)
    nodes/person/
    edges/knows/
  _db_branches/
    feature-x/
      nodes/person/              # Shallow clone — _versions/ and new data only
        _versions/*.manifest     # Branch-specific manifests
        data/new-data.lance      # Only data written on this branch
      edges/knows/               # Shallow clone
```

Each branch dataset's manifest would include a `base_paths` entry pointing back to the source:

```
base_paths: [
  { id: 0, is_dataset_root: true, path: "../../_db/nodes/person" }
]
```

**Lazy sub-table branching** (from the design document): Rather than cloning every dataset upfront, branch only the manifest table. Sub-tables are branched lazily — only when first written to on that branch:

```
1. User writes Drug entities on branch "experiment"
2. System checks: does Drug.lance have branch "experiment"? No.
3. Create Lance branch "experiment" on Drug.lance (zero-copy via multi-base)
4. Write to Drug.lance branch "experiment" → Drug/experiment version 1
5. Update experiment manifest: merge_insert ("nodes/Drug", 1, "experiment")
```

Sub-tables that aren't written to on a branch are never branched. They continue reading from the version recorded in the manifest. This means:

- Branch creation is instant (only the manifest is branched)
- Most sub-tables are shared (only touched types get their own branch)
- Storage overhead per branch is proportional to what changed, not total graph size

**Complications:**

- The `base_paths` system requires the source to remain accessible. Deleting a branch that other branches depend on requires either deep-copying or reference counting.

**Dependencies:** None. Can be done independently of other changes.

### Phase B: Replace Custom Manifest with a Lance Manifest Table

**Goal:** Eliminate `graph.manifest.json` and get atomic cross-dataset coordination through Lance's own MVCC.

**Approach:**

Create a Lance dataset at `_db/_manifest/` (or `_db/__manifest/`) that serves as the repo-level manifest. Each row represents a dataset in the graph:


| Column            | Type        | Description                            |
| ----------------- | ----------- | -------------------------------------- |
| `dataset_key`     | String (PK) | `"node:Person"` or `"edge:Knows"`      |
| `dataset_path`    | String      | Relative path to the Lance dataset     |
| `dataset_version` | UInt64      | Pinned Lance version for this snapshot |
| `type_id`         | UInt32      | Internal type ID                       |
| `row_count`       | UInt64      | Number of rows                         |


Additional metadata (next IDs, schema hash) stored in the Lance table's metadata map.

**Why this works:**

- Lance table commits are atomic — a single manifest write advances the entire graph state
- MVCC provides snapshot isolation for reads (any reader sees a consistent cross-dataset state)
- Version history is automatic (every commit produces a new manifest version)
- Time travel is free (checkout any historical version of the manifest table)
- Conflict detection works if multiple processes try to advance the manifest concurrently

**What this replaces:**

- `graph.manifest.json` — entirely replaced
- The branch head's `datasets` field — the manifest table version *is* the snapshot

**Branch heads simplify to:**

```json
{
  "name": "feature-x",
  "parent": "main",
  "manifest_version": 42,
  "schema_hash": "a1b2c3d4..."
}
```

Where `manifest_version` is the Lance version of the `_manifest/` dataset.

**Dependencies:** Can be done independently, but the value compounds with Phase A (shallow clones reference data through the manifest).

### Phase C: Replace Custom WAL with Lance-Native Transaction History + CDC

**Goal:** Eliminate `_wal.jsonl` entirely.

**Approach — Transaction History:**

With the manifest table from Phase B, every commit to the manifest table is already a durable, versioned transaction. The Lance `_transactions/` directory stores the operation metadata. Walking manifest versions gives you the full transaction catalog:

```
manifest version 1 → initial state
manifest version 2 → loaded Person nodes (dataset_version: Person → 3)
manifest version 3 → loaded Knows edges (dataset_version: Knows → 2)
```

The `op_summary` can be stored in the manifest table's commit metadata or in the transaction file.

**Approach — CDC:**

Enable stable row IDs on all Lance datasets. This gives you:

- `_row_created_at_version` and `_row_last_updated_at_version` per row
- CDC queries via standard predicates against the dataset

For cross-dataset CDC (e.g., "all changes between graph version 5 and 10"), query the manifest table to find which datasets changed between those versions, then query each changed dataset's row version columns.

**Change subscription pattern** (from the design document): Agents and services subscribe to graph changes by polling Lance directly using version-tracking columns. A subscriber maintains a cursor (last-seen manifest version) and queries:

```sql
-- New entities since last check
WHERE _row_created_at_version > :cursor AND _row_created_at_version <= :current_version

-- Updated entities (not newly created) since last check
WHERE _row_created_at_version <= :cursor
  AND _row_last_updated_at_version > :cursor
  AND _row_last_updated_at_version <= :current_version

-- Deleted entities: detected via Lance deletion vectors
-- (rows present at cursor version but absent at current version)
```

For before/after comparison on updates, the subscriber uses Lance time travel — checkout the previous version and compare with current. No write-path coupling, no event bus, no change records beyond what Lance stores natively. Different agents can subscribe to different slices (specific types, specific properties, specific branches).

This enables a reactive hook system where hooks fire on graph changes, on a schedule, or manually. Hook cursors are persisted locally. The hook system is a thin dispatch layer on top of Lance's version tracking — not a separate change tracking infrastructure.

**What this eliminates:**

- `_wal.jsonl` — entirely
- `WalEntry`, `CdcLogEntry` structs — no longer needed
- WAL repair, reconciliation, pruning logic — handled by Lance's version management
- Inline CDC payload storage — replaced by Lance's columnar row versioning (more efficient, queryable, indexed)

**What changes in the commit path:**

Before:

```
append WAL entry (fsync) → write graph.manifest.json (tmp → fsync → rename)
```

After:

```
write Lance datasets → commit manifest table (atomic Lance commit)
```

Single atomic operation instead of two sequential ones.

**Dependencies:** Requires Phase B (manifest table). Stable row IDs must be enabled at dataset creation time and cannot be retroactively added.

### Phase D: Leverage Lance Concurrency

**Goal:** Support concurrent writers across branches and eventually within branches.

**Approach:**

1. Remove the `Mutex<()>` writer lock from `Database`
2. Use Lance's per-dataset MVCC for concurrent writes to different datasets
3. Use the manifest table's MVCC for atomic graph-level state advancement
4. Handle conflicts at the manifest table level:
  - Two writers loading into different types → no conflict (different rows in manifest table)
  - Two writers loading into the same type → Lance dataset-level conflict resolution (merge-insert handles this)
  - Branch writes are fully isolated (different manifest tables)

**For cross-branch merge (future):**

Lance's conflict resolution model provides the primitives:

- Read both branch manifest tables to find divergent datasets
- For each divergent dataset, compare row-level changes using stable row IDs
- Apply three-way merge using Lance's merge-insert with conflict detection

**Dependencies:** Requires Phase B (manifest table) and Phase C (stable row IDs for merge semantics).

### Phase E: Align Branch Metadata with Lance Native Branching

**Goal:** Use Lance's own branch/tag system instead of custom JSON files.

**Approach:**

Two options, depending on how far to take Lance-native branching:

**Option 1 — Lance branches on the manifest table only:**

Use Lance's native branching on the `_manifest/` dataset. Each Omnigraph branch becomes a Lance branch on the manifest table. The manifest table's branch tracks which dataset versions are current for that branch.

```
_db/_manifest/
  _versions/*.manifest          # Main branch versions
  _refs/branches/feature-x.json # Lance branch metadata
  tree/feature-x/
    _versions/*.manifest        # Feature branch versions
```

Per-type datasets still use the shallow clone approach from Phase A. The manifest table branch is the single source of truth for branch state.

**Option 2 — Full Lance namespace:**

Use the Lance Directory Namespace V2 spec with a `__manifest` Lance table. Each Omnigraph branch is a namespace, and graph types are tables within the namespace. This fully aligns with the Lance catalog spec but is a larger architectural change.

**Dependencies:** Requires Phase B. Option 2 also requires evaluating whether the namespace spec's table-per-type model fits the graph use case.

---

## What Is Already Correct

These aspects of the current implementation align well with the Lance spec and should be preserved:

1. **Per-type Lance datasets.** One dataset per node type and one per edge type gives narrow schemas, type-local indices, type-local compaction, and selective hydration. This is exactly how Lance is designed to be used.
2. **Version pinning.** All reads use `checkout_version()` for point-in-time snapshot isolation. This correctly leverages Lance's MVCC.
3. **File format version.** New datasets use `LanceFileVersion::V2_2`, the latest format with the best encoding and compression support.
4. **Merge-insert for `@key` types.** The engine uses Lance's native `MergeInsertBuilder` with `WhenMatched::UpdateAll` and `WhenNotMatched::InsertAll` for keyed upserts. This is the intended usage pattern.
5. **Field renaming for internal columns.** The `__ng_id`, `__ng_src`, `__ng_dst` convention avoids collisions with user-defined property names while keeping Lance datasets self-describing.
6. **Atomic manifest writes.** The `write_tmp → fsync → rename` pattern for `graph.manifest.json` is crash-safe for single-writer local usage, even if it will eventually be replaced.

---

## Migration Considerations

### Backwards Compatibility

Existing repos created with the current format cannot be transparently upgraded because:

- Enabling stable row IDs requires creating datasets with the feature enabled from the start
- Switching from `graph.manifest.json` to a Lance manifest table changes the database layout
- Switching from directory copy to shallow clones changes branch storage semantics

A migration tool should:

1. Read the existing `graph.manifest.json` to discover datasets and versions
2. Create the new Lance manifest table with the current state
3. Optionally re-create datasets with stable row IDs enabled (requires rewriting data)
4. Convert existing branch copies to shallow clones (requires creating `base_paths` references)

### Ordering

The phases have a natural dependency chain:

```
Phase A (shallow clone branching)     — independent, highest immediate impact
Phase B (Lance manifest table)        — independent, enables C/D/E
Phase C (eliminate WAL)               — requires B, benefits from stable row IDs
Phase D (concurrency)                 — requires B+C
Phase E (Lance-native branch metadata)— requires B
```

Phase A and Phase B can proceed in parallel. Phase A gives the biggest immediate win (branch creation goes from O(data) to O(1)). Phase B is the architectural foundation that enables everything else.

### Risk: Sharing Too Much with Lance Internals

The Omnigraph spec explicitly states that Omnigraph and Nanograph should share only the compiler/frontend layer, not the runtime. Aligning deeply with Lance's table-format-level features (branching, transactions, namespace) is different from sharing a runtime — it's using the storage format as intended. But the coupling to Lance's specific API surface increases:

- Lance's Rust SDK is the only way to create shallow clones and manage `base_paths`
- Stable row IDs are a Lance-specific feature
- The MemWAL spec is experimental and may change

Mitigation: wrap Lance-specific operations in a thin storage abstraction within `omnigraph-engine` so the repo layer interacts with graph-level concepts, not Lance API calls directly.

### Risk: Lance Rust SDK Branch/Tag Method Availability

The Lance format spec fully documents branching and tagging — branch metadata at `_refs/branches/{name}.json`, branch datasets at `tree/{branch_name}/`, tag metadata at `_refs/tags/{name}.json`, naming rules, and the `Clone` transaction type. The format-level spec is stable and complete.

The risk is narrower than it appears: the **format exists**, but high-level SDK convenience methods (`create_branch()`, `merge_branch()` as single Rust calls) may not yet be fully wrapped in the Lance Rust crate (v3.0). The Python SDK has confirmed high-level branch/tag methods.

**Mitigation:** Even if high-level SDK wrappers aren't ready, branch operations can be implemented at the format level — writing `_refs/branches/` JSON files and creating `tree/{name}/` directory structures using the existing Lance Rust SDK's lower-level APIs. This is more work (~200 extra lines) but not a blocker. Phase 1 does not depend on branching. Phase 2 can proceed with format-level implementation if SDK wrappers aren't ready.

**Action required:** Verify which branch/tag operations are exposed in the `lance` v3.0 Rust crate before starting Phase A or Phase E.

### Risk: Manifest Version Accumulation

Every mutation advances the manifest version. At 100 mutations/day, manifest versions accumulate (3,000/month). Each version is an immutable Lance manifest file (~KBs) but the count affects listing operations.

**Mitigation:** The `compact` command includes the manifest table in its scope — running Lance `compact` + `cleanup_old_versions` on `_manifest.lance` alongside all sub-tables. Default retention: 100 versions (configurable). Tags reference specific versions and are exempt from cleanup.

---

## Summary


| Component             | Current                                  | Target                                             | Why                                                             |
| --------------------- | ---------------------------------------- | -------------------------------------------------- | --------------------------------------------------------------- |
| **Branching**         | Full directory copy                      | Lance shallow clone via `base_paths`               | O(1) vs O(data) branch creation                                 |
| **Manifest**          | Custom JSON (`graph.manifest.json`)      | Lance table with MVCC                              | Atomic cross-dataset coordination, version history, time travel |
| **WAL**               | Custom JSONL (`_wal.jsonl`)              | Eliminated — Lance transactions + row versioning   | Remove redundancy, get native CDC, simplify commit path         |
| **CDC**               | Inline JSON payloads in WAL              | Lance row version columns                          | Columnar, queryable, indexed, no separate log to maintain       |
| **Concurrency**       | Process-level Mutex                      | Lance MVCC per-dataset + manifest table            | Concurrent writers, shared storage support                      |
| **Branch metadata**   | Custom JSON in `_refs/`                  | Lance native branches on manifest table            | Single source of truth, MVCC-protected                          |
| **Identity**          | `u64` counters                           | String IDs with stable row IDs                     | Branch-safe identity, native CDC, merge support                 |
| **Search**            | Brute-force O(N) per query               | Lance FTS / n-gram / ANN indexes                   | Indexed search at scale, not memory-bound                       |
| **Query execution**   | Custom DataFusion operators (~3,200 LOC) | DataFusion `SessionContext` + `LanceTableProvider` | Push filters/projections to Lance, eliminate custom plan code   |
| **Per-type datasets** | Keep                                     | Keep                                               | Correct alignment with Lance                                    |
| **Version pinning**   | Keep                                     | Keep                                               | Correct alignment with Lance                                    |
| **File format**       | V2.2                                     | V2.2                                               | Correct alignment with Lance                                    |

