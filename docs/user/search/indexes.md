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

## L2 — Graph topology index

This is OmniGraph-specific (not Lance):

- A Compressed Sparse Row (CSR) adjacency representation of edges, with both out- (CSR) and in- (CSC) directions, plus a dense per-node-type id mapping.
- Built on demand from a snapshot's edge tables, **lazily**: only when an `Expand` the planner routes to the CSR path (dense / large frontier) or an `AntiJoin` actually needs it.
- Cached per snapshot (LRU, keyed by snapshot id + edge table versions), so repeat traversals over the same snapshot reuse it.
- Selective `Expand`s resolve neighbors from the persisted `src`/`dst` BTREE instead (one indexed scan per hop) and never trigger the CSR build; see [query-language](../queries/index.md) → Traversal execution. Pure scans, and queries served entirely by the indexed traversal path, skip it.
