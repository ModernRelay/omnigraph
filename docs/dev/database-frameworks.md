# Database Frameworks Applied to Omnigraph

How standard database evaluation frameworks map onto Omnigraph's architecture, with emphasis on the branching product model.

Reference: Almog Gavra, *Frameworks for Understanding Databases* (Dec 2025). The post presents three evaluation dimensions — performance (RWS amplification, RUM conjecture, data orientation), availability (PACELC), and durability (LCD) — applied across nine database types.

---

## Performance

### Read / Write / Space Amplification

The blog's RWS framework treats amplification as a single system property. In Omnigraph it splits into two profiles because branches create asymmetric workloads.

**Branch-local operations (the common path):**

| Dimension | Cost | Why |
|---|---|---|
| Write amp | Moderate | Sub-table write + manifest `merge_insert` = 2 Lance writes per mutation. Edge cascade fans out to N edge types. |
| Read amp | Low | Snapshot pins exact versions. Column projection narrows to requested properties. BTree on `id`/`src`/`dst`. |
| Space amp | Minimal | Lazy branching: unmodified sub-tables share storage with parent. A branch touching 3 of 50 tables forks only 3 datasets. |

**Cross-branch operations (merge):**

| Dimension | Cost | Why |
|---|---|---|
| Write amp | High | 3 full table scans per changed table (base + source + target), delta staging into temp Lance datasets, surgical `merge_insert` + `delete` publish, index rebuild. |
| Read amp | High | Ordered cursor iteration on all three versions streams every row to compute `row_signature()` hashes. |
| Space amp | Optimized | Delta staging writes only changed rows. Unchanged rows keep `_row_created_at_version`, so post-merge change detection stays fast. |

The tradeoff is intentional: merges are expensive but infrequent. Branch-local writes are cheap and frequent.

**Specific amplification sources:**

- **Manifest double-write.** Every graph write touches both the sub-table and `_manifest.lance`. The spec acknowledges this becomes a bottleneck above ~100 writes/sec; MemWAL's manifest commit batching is the planned mitigation.
- **Surgical merge publish (Step 9b).** Accepts higher write amplification (`merge_insert` + `delete` instead of `truncate` + `append`) to preserve Lance version columns (`_row_created_at_version`, `_row_last_updated_at_version`). Without this, change detection after merge requires full table scans (read amplification) instead of version-column queries.
- **Two-phase blob updates.** Non-blob columns first, blob-only batch second. Required because Lance 3.0.1 can't filter + read blobs simultaneously.
- **String IDs.** 26-byte Crockford Base32 ULIDs vs 16-byte binary. Each edge row carries 78+ bytes of string IDs (`id` + `src` + `dst`). The spec quantifies a 38% reduction with binary ULIDs (deferred).

### Indexes and the RUM Conjecture

The blog's RUM conjecture — a single index can't optimize Read, Update, and Memory simultaneously — applies to each of Omnigraph's index structures:

| Index | Read | Update | Memory | Notes |
|---|---|---|---|---|
| **GraphIndex (CSR/CSC)** | O(1) neighbor lookup | Full rebuild on any edge mutation | Entire topology in memory | Biggest RUM liability |
| **BTree on `id`** | Binary search | Lance maintains on write | On-disk (Lance managed) | Good all-around |
| **Inverted (FTS)** | Fast text search | Full rebuild (`replace=true`) | On-disk | Expensive rebuild after merge |
| **IVF-HNSW (ANN)** | Fast vector search | Full rebuild | On-disk | Most expensive rebuild |

**Branch-specific RUM implications:**

The GraphIndex is the biggest tension point. With branching, each branch can have different edge topology. The cache key includes `(snapshot_id, edge_table_states)`. Switching between branches triggers a full CSR rebuild. If 5 active branches each modify edges and queries alternate between them, rebuild cost multiplies up to 5x. The spec's planned migration to `HashMap<GraphIndexKey, Arc<GraphIndex>>` (multi-slot cache) addresses this.

Inverted and vector indices are the worst case for merge: after a merge rewrites a table, `ensure_indices()` rebuilds them with `replace=true`. Every merge that touches a text-indexed or vector-indexed table pays the full index rebuild cost.

**Omnigraph's index categories mapped to the blog's taxonomy:**

| Blog Category | Omnigraph Implementation |
|---|---|
| Clustering (primary) | Per-type tables act as a clustering index on type. No global sort key within a table (Lance fragment order = write order). |
| Secondary | BTree on `src`/`dst` for edges, Inverted for FTS, IVF-HNSW for ANN — all return IDs for hydration. |
| Filter | Lance fragment-level min/max statistics enable fragment skipping during scans. Manifest-level diff (Level 1 change detection) skips unchanged tables in O(1). |

### Data Orientation

The blog says: choose columnar for aggregation, row for entity reconstruction. Omnigraph is a graph database (entity-oriented) built on columnar storage (Lance/Arrow). This isn't contradictory — it's a deliberate tradeoff where columnar provides three non-obvious benefits:

**1. Narrow per-type tables mitigate entity reconstruction cost.** A `Person` table with 5 properties means a full-entity scan reads 5 columns, not 50. The blog's 32-column read amplification example doesn't apply when types are decomposed into separate datasets.

**2. Columnar enables efficient merge streaming.** Three-way merge scans rows ordered by `id`. In columnar format: the `id` column can be scanned independently to build the merge plan; `row_signature()` hashes across contiguous single-type column arrays are cache-friendly; delta staging benefits from dictionary encoding and run-length compression on unchanged columns.

**3. Search and change detection benefit directly.** BM25 scoring scans one text column across all rows. Version-column change detection (`_row_last_updated_at_version > Vf`) scans one system column. Vector ANN operates on contiguous `FixedSizeList(Float32, N)` arrays. These are exactly the "aggregate values across rows" workload that columnar excels at.

### Compaction

The blog describes compaction as resolving overlapping data into non-overlapping files. In Omnigraph, **branch merge is the compaction event:**

- Multiple branches create overlapping data (same `id` modified on different branches)
- Merge resolves overlaps via three-way diff (equivalent to merge-sort in LSM compaction)
- The merged result has no overlaps (single consistent state per `id`)
- Delta staging writes only changed rows (incremental compaction, not full rewrite)

The blog's separation of compaction and GC also applies:

- **Compaction** = merge (resolves overlapping state across branches)
- **GC** = Lance version cleanup (removes orphaned fragments from old versions)

Omnigraph delegates GC to Lance. Post-merge, old branch versions accumulate until explicitly cleaned up — a space amplification cost that grows with merge frequency.

---

## Availability

### System Architecture

The blog classifies databases on two axes: leader/leaderless and disaggregated/distributed. Omnigraph maps to **single writer, multi-reader (disaggregated)** today, with a planned evolution:

| Blog Quadrant | Omnigraph Phase | Mechanism |
|---|---|---|
| Single Writer, Multi-Reader | Current (Steps 0–10a) | `&mut Omnigraph` for writes, `Snapshot` for reads, Lance on local/S3 |
| Multi-Writer, Multi-Reader | MemWAL (deferred) | Region-per-writer, epoch fencing, manifest commit batching |

### Branches Replace Replicas

The blog's availability frameworks assume replicas as the unit of concurrency. Omnigraph uses **branches instead:**

| Blog Concept | Omnigraph Equivalent |
|---|---|
| Replica | Branch |
| Leader election | N/A (single writer per branch, optimistic) |
| Replication lag | Branch divergence (resolved at merge time) |
| Quorum write | Manifest commit (single coordination point) |
| Partition tolerance | Branch isolation (partitions are a feature, not a failure) |

In a traditional distributed database, partitions are failures to be tolerated. In Omnigraph, partitions are features — you intentionally fork state, work independently, and reconcile via merge.

### PACELC Applied to Branching

**PA (partition + availability):** When branches diverge, both remain fully available for reads and writes. No partition event, no availability loss.

**EL (else, latency vs consistency):** In normal operation on a single branch, every mutation commits to the manifest synchronously — consistency over latency. The MemWAL plan trades consistency for latency by batching manifest commits (checkpoint-based freshness).

**EC (else, consistency):** Cross-branch consistency is deferred to merge time. While branches are diverged, there is no consistency guarantee between them. This is the product model, not a limitation.

The spec's concurrency progression:

| Phase | Blog Equivalent | Mechanism |
|---|---|---|
| Single writer | Single-node database | `&mut Omnigraph` exclusive access |
| Branch-per-writer | Leaderless with intentional partitions | Zero-copy fork, isolated writes, merge to reconcile |
| Same branch, low contention | Leader with optimistic locking | Lance OCC (version drift detection, retry on conflict) |
| MemWAL streaming | Multi-writer with WAL regions | Region-per-writer, epoch fencing, periodic checkpoint |

---

## Durability

### LCD Framework

The blog's LCD triangle (pick two of Latency, Cost, Durability) maps to Omnigraph's write model:

**Current (no WAL):** High durability + acceptable cost, but higher latency per write. Every mutation is a full Lance dataset write — durable immediately on the storage backend, but expensive per-op. This works for batch-oriented workflows (JSONL loads) and moderate mutation rates.

**Planned (MemWAL):** Trades durability guarantees for latency. Writers write to an in-memory MemTable backed by a durable WAL per region. The spec's trigger is quantified: per-mutation commit latency >10ms or throughput <100 writes/sec.

### Storage Tiers

The blog's storage options chart maps to Omnigraph's tiered model:

| Tier | Latency | What Lives Here | Owner |
|---|---|---|---|
| Memory (< 1 us) | CSR/CSC, TypeIndex, catalog, manifest metadata | `Omnigraph` handle |
| Local disk (< 1 ms) | Lance fragment cache, Lance metadata cache | Lance SDK |
| Object storage (10–100 ms) | Lance data fragments, manifests, indices | Lance SDK |

No tiered write system exists today — writes go directly to the configured storage backend. MemWAL would add a memory tier for recent writes that flush to storage periodically.

### WAL: Intentional Absence

The blog emphasizes WALs as the core durability primitive — an "UM data structure" (Update & Memory optimized) that sacrifices read efficiency for fast durable writes. Omnigraph intentionally has **no custom WAL:**

- **Pro:** Simpler recovery. No WAL replay needed; manifest version = truth. No write-path coupling for change detection.
- **Con:** Every mutation pays full Lance write cost. No batching of small writes into a sequential log. A narrow crash window exists between manifest commit and graph commit DAG append where the DAG can lag the manifest.
- **Mitigation:** MemWAL (deferred) is Lance's own LSM-tree WAL — it provides the same fast-write buffering without a custom implementation.

### Manifest Atomicity and the Crash Window

The manifest commit is the single coordination point. Sub-table writes must complete before the manifest advances. The atomicity guarantee:

1. Sub-table writes complete (Lance atomic dataset writes)
2. Manifest `merge_insert` publishes new versions (atomic commit point)
3. Graph commit DAG appended

A crash between steps 2 and 3 leaves the manifest advanced but the commit DAG stale. Merge-base resolution could be incorrect until the DAG is repaired. The blog's WAL pattern would solve this (single WAL entry covering both), but the spec chose no custom WAL. Failpoint tests (`graph_publish.after_manifest_commit`, `graph_publish.before_commit_append`) verify the system handles this window correctly.

---

## What the Frameworks Reveal About Omnigraph's Roadmap

The blog's frameworks help prioritize deferred work:

1. **GraphIndex multi-slot cache** — highest-priority RUM improvement for branching workloads. Branch switching currently triggers full CSR rebuilds.

2. **MemWAL + manifest batching** — the LCD framework's answer to high-frequency mutation workloads. Trigger: >10ms per-mutation latency or <100 writes/sec.

3. **Binary ULIDs** — space amplification reduction (38% on ID columns). Should be prioritized based on measured space ratio, not just speed.

4. **Persistent GraphIndex** — shifts from "Read-optimized, Update-expensive" to "Read-optimized, Update-moderate" by serializing/deserializing the topology instead of rebuilding from edge scans.

5. **Lance compaction as CLI command** — space amplification grows with branch/merge frequency. Version GC is the counterpart to merge-as-compaction.

---

## What's Not Relevant

- **Leader/follower and leaderless replication** — Omnigraph is not distributed. Branches replace replicas.
- **Network partition handling** — No multi-node deployment. Branch isolation is intentional, not a failure mode.
- **Row-oriented storage** — Committed to columnar via Lance. Per-type narrow tables mitigate entity-reconstruction cost.
- **Bloom filters** — Lance doesn't expose them. Manifest-level skipping and BTree indices serve equivalent purposes.
- **fsync() specifics** — Lance owns fsync semantics via the storage adapter.

---

## References

- Gavra, A. (2025). *Frameworks for Understanding Databases.* bits & pages.
- Kleppmann, M. (2017). *Designing Data-Intensive Applications.* O'Reilly.
- Athanassoulis, M. et al. (2016). *Designing Access Methods: The RUM Conjecture.* EDBT.
- Abadi, D. (2012). *Consistency Tradeoffs in Modern Distributed Database System Design.* IEEE Computer.
