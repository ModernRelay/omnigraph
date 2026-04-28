# Constants & Tunables (cheat sheet)

| Name | Value | Where |
|---|---|---|
| `MANIFEST_DIR` | `__manifest` | `db/manifest/layout.rs` |
| Commit graph dir | `_graph_commits.lance` | `db/commit_graph.rs` |
| Run registry dir | `_graph_runs.lance` | `db/run_registry.rs` |
| Run branch prefix | `__run__` | `db/run_registry.rs` |
| Schema apply lock | `__schema_apply_lock__` | `db/mod.rs` |
| Merge stage batch | `MERGE_STAGE_BATCH_ROWS = 8192` | `exec/merge.rs` |
| Maintenance concurrency | `OMNIGRAPH_MAINTENANCE_CONCURRENCY=8` | `db/omnigraph/optimize.rs` |
| Graph index cache size | `8` (LRU) | `runtime_cache.rs` |
| Default body limit | `1 MB` | `omnigraph-server/lib.rs` |
| Ingest body limit | `32 MB` | `omnigraph-server/lib.rs` |
| Engine embed model | `gemini-embedding-2-preview` | `omnigraph/embedding.rs` |
| Compiler embed model | `text-embedding-3-small` | `omnigraph-compiler/embedding.rs` |
| Embed timeout | `30 000 ms` | both clients |
| Embed retries | `4` | both clients |
| Embed retry backoff | `200 ms` | both clients |
| LANCE memory pool default | `1 GB` (raised in v0.3.0) | runtime |
