# Indexes

## L1 — Lance index types OmniGraph exposes

| Index | Use | Notes |
|---|---|---|
| **BTREE scalar** | `=` / range / `IN` / `IS NULL` on a scalar | always on the node `id` and edge `src`/`dst`; and on each one-column `@index`/`@key` property that is an **enum** or an **orderable scalar** (`DateTime`/`Date`/`I32`/`I64`/`U32`/`U64`/`F32`/`F64`/`Bool`) |
| **Inverted (FTS)** | `search`, `fuzzy`, `match_text`, `bm25` | created on **free-text** (non-enum) `String` `@index`/`@key` columns |
| **Vector** | `nearest()` k-NN | Lance picks IVF_PQ vs HNSW family by configuration; OmniGraph stores as FixedSizeList(Float32, dim) |

The per-property index a column gets is decided by `node_prop_index_kind` (shared
by the builder and the sidecar-pinning coverage check so they cannot drift):
enums and orderable scalars → BTREE, free-text Strings → FTS, `Vector` → vector,
list/`Blob` columns → none.

> **Free-text Strings are not equality-indexed.** A non-enum `String` column
> (including a `String @key` slug) gets an FTS inverted index, which Lance does
> **not** consult for `=`/range — only for `search`/`match_text`/`bm25`. So an
> equality filter on a free-text String falls back to a full scan. If you filter
> a String identifier by equality on a large table, model it so the value is the
> node id, or track it as a follow-up to also build a BTREE on such columns.

> **Coverage and cost.** Each indexed column adds index files and build time, and
> an index only covers the fragments it was built over. Rows appended after the
> index was built (e.g. by `load --mode merge`) are scanned unindexed until a
> reindex extends coverage; see [maintenance](../operations/maintenance.md) → `optimize`.

## L2 — OmniGraph orchestration

- **`@index`/`@key` declares intent; the physical index is derived state.** A migration records the declaration in the catalog/IR and never fails on it — `schema apply` builds **no** indexes. Mutation/load likewise publish only their exact data effects; they do not widen the recovery plan with index commits. Reads stay correct while an index is missing or partially covered by falling back to scans (vector search to brute-force). A later `ensure_indices`/`optimize` materializes every buildable declaration; an untrainable Vector column remains pending rather than failing the logical write.
- `ensure_indices()` / `ensure_indices_on(branch)` — idempotent build of BTREE + inverted + vector indexes for the current manifest head; safe to re-run; returns the columns it had to defer as pending. Existing Lance HEAD drift is refused with `omnigraph repair` guidance rather than silently adopted. On a lazy child branch, an ancestor-owned table stays inherited when there is no index work; real index work first verifies the target ref is absent, then creates it only after recovery intent is durable. `optimize` runs the reconciler after compaction, so the maintenance cron is the convergence path for deferred indexes.
- Indexes are built on the *branch head* (not on a snapshot), so reads always see the current index state.
- **Lazy branch forking for indexes**: a branch that hasn't mutated a sub-table doesn't need its own index — the main lineage's index is reused until the first write triggers a copy-on-write fork.
- Vector index parameters (metric, nlist, nprobe, etc.) are not exposed in the schema; they default at the Lance layer and are picked up automatically when an index is asked for on a Vector column.

## L2 — Graph topology index

This is OmniGraph-specific (not Lance):

- A Compressed Sparse Row (CSR) adjacency representation of edges, with both out- (CSR) and in- (CSC) directions, plus a dense per-node-type id mapping.
- Built on demand from a snapshot's edge tables, **lazily**: only when an `Expand` the planner routes to the CSR path (dense / large frontier) or an `AntiJoin` actually needs it.
- Cached per snapshot (LRU, keyed by snapshot id + edge table versions), so repeat traversals over the same snapshot reuse it.
- Selective `Expand`s resolve neighbors from the persisted `src`/`dst` BTREE instead (one indexed scan per hop) and never trigger the CSR build; see [query-language](../queries/index.md) → Traversal execution. Pure scans, and queries served entirely by the indexed traversal path, skip it.
