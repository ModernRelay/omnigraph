# Indexes

## L1 — Lance index types OmniGraph exposes

| Index | Use | Notes |
|---|---|---|
| **BTREE scalar** | range / equality on any scalar | created on `@key`, `@index(...)`, and on key columns by `ensure_indices()` |
| **Inverted (FTS)** | `search`, `fuzzy`, `match_text`, `bm25` | created on text columns referenced by FTS queries |
| **Vector** | `nearest()` k-NN | Lance picks IVF_PQ vs HNSW family by configuration; OmniGraph stores as FixedSizeList(Float32, dim) |

## L2 — OmniGraph orchestration

- `ensure_indices()` / `ensure_indices_on(branch)` — idempotent build of BTREE + inverted indexes for the current head; safe to re-run.
- Indexes are built on the *branch head* (not on a snapshot), so reads always see the current index state.
- **Lazy branch forking for indexes**: a branch that hasn't mutated a sub-table doesn't need its own index — the main lineage's index is reused until the first write triggers a copy-on-write fork.
- Vector index parameters (metric, nlist, nprobe, etc.) are not exposed in the schema; they default at the Lance layer and are picked up automatically when an index is asked for on a Vector column.

## L2 — Graph topology index (`graph_index/mod.rs`)

This is OmniGraph-specific (not Lance):

- `TypeIndex`: dense `u32 ↔ String id` mapping per node type.
- `CsrIndex`: Compressed Sparse Row representation of edges per edge type — `offsets[i]..offsets[i+1]` slices into `targets`.
- `GraphIndex { type_indices, csr (out), csc (in) }` — built on demand from a snapshot's edge tables.
- Cached in `RuntimeCache::graph_indices` (LRU, max 8 entries, keyed by snapshot id + edge table versions).
- Built only when an `Expand` or `AntiJoin` IR op is present in the lowered query, so pure scans skip it.
