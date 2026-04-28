# Architecture

OmniGraph is a typed property-graph engine built as a coordination layer over many Lance datasets, with Git-style branches and commits across the whole graph, multi-modal querying (vector + FTS + BM25 + RRF + graph traversal) in one runtime, an HTTP server with Cedar policy, and a CLI driven by a single `omnigraph.yaml`.

## Stack

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

## L1 / L2 framing

Throughout the docs, capabilities are split into:

- **L1 — Inherited from Lance**: what OmniGraph gets "for free" by sitting on top of the Lance dataset format (columnar Arrow storage, per-dataset versions and branches, index types, `merge_insert`, `compact_files` / `cleanup_old_versions`).
- **L2 — Added by OmniGraph**: typing (schema language), graph semantics, multi-dataset coordination via `__manifest`, graph-level branches and commits, the `.gq` query language and IR, the topology index, the HTTP server, Cedar policy, the CLI.

## Concurrency model

- **MVCC**: every Lance write bumps a per-dataset version; the OmniGraph manifest version coordinates which sub-table versions are visible together.
- **Snapshot isolation**: a query holds one `Snapshot` for its lifetime; concurrent writes don't leak in.
- **Cross-branch isolation**: copy-on-write means readers and writers on different branches don't block each other.
- **Run isolation**: each transactional run lives on its own `__run__<id>` branch.
- **Schema-apply lock**: `__schema_apply_lock__` system branch serializes schema migrations.
- **Fail-points** (`failpoints` cargo feature): `failpoints::maybe_fail("operation.step")?` in `branch_create`, publish, etc., for deterministic failure injection in tests.

## Workspace crates

- `omnigraph-compiler` — schema and query grammars, catalog, IR, lowering, type checker, lint, migration planner, OpenAI-style embedding client.
- `omnigraph` (engine, published as `omnigraph-engine` on crates.io since v0.2.2) — the Lance-backed runtime: manifest, commit graph, run registry, snapshot, exec, merge, loader, Gemini embedding client.
- `omnigraph-cli` — the `omnigraph` binary.
- `omnigraph-server` — the `omnigraph-server` binary (Axum HTTP server).
