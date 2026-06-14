# Constants & Tunables (cheat sheet)

| Name | Value | Where |
|---|---|---|
| `MANIFEST_DIR` | `__manifest` | `db/manifest/layout.rs` |
| Commit graph dir | `_graph_commits.lance` | `db/commit_graph.rs` |
| Run registry dir (legacy, removed MR-771) | `_graph_runs.lance` | inert post-v0.4.0; bytes remain until a `delete_prefix` primitive lands |
| Run branch prefix (legacy, removed MR-771/MR-770) | `__run__` | swept off `__manifest` by the v2→v3 migration; no longer a reserved name |
| Schema apply lock | `__schema_apply_lock__` | `db/mod.rs` |
| Manifest publisher retry budget | `PUBLISHER_RETRY_BUDGET = 5` | `db/manifest/publisher.rs` |
| Internal manifest schema version | `INTERNAL_MANIFEST_SCHEMA_VERSION = 3` | `db/manifest/migrations.rs` |
| Merge stage batch | `MERGE_STAGE_BATCH_ROWS = 8192` | `exec/merge.rs` |
| Maintenance concurrency | `OMNIGRAPH_MAINTENANCE_CONCURRENCY=8` | `db/omnigraph/optimize.rs` |
| Lance blob compaction support | `LANCE_SUPPORTS_BLOB_COMPACTION = false` | `db/omnigraph/optimize.rs` |
| Graph index cache size | `8` (LRU) | `runtime_cache.rs` |
| Expand indexed-path frontier ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER=1024` | `exec/query.rs` |
| Expand indexed-path hop ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS=6` | `exec/query.rs` |
| Expand CSR-build cost factor | `CSR_BUILD_FACTOR = 1.5` | `exec/query.rs` |
| Expand mode override | `OMNIGRAPH_TRAVERSAL_MODE` (`indexed`\|`csr`; unset = cost-based auto) | `exec/query.rs` |
| Default body limit | `1 MB` | `omnigraph-server/lib.rs` |
| Ingest body limit | `32 MB` | `omnigraph-server/lib.rs` |
| Engine embed model | `gemini-embedding-2-preview` | `omnigraph/embedding.rs` |
| Compiler embed model | `text-embedding-3-small` | `omnigraph-compiler/embedding.rs` |
| Embed timeout | `30 000 ms` | both clients |
| Embed retries | `4` | both clients |
| Embed retry backoff | `200 ms` | both clients |
| LANCE memory pool default | `1 GB` (raised in v0.3.0) | runtime |

**Expand traversal dispatch.** With `OMNIGRAPH_TRAVERSAL_MODE` unset, the engine
chooses the indexed (per-hop BTREE) vs CSR (whole-graph in-memory) path with a
cost model over cheap manifest counts (frontier size, |E|, source-vertex count,
hops) plus the index-coverage signal: the indexed path is preferred when its
frontier-relative work beats building the CSR (≈ when `hops × frontier` is a
small fraction of the source-vertex set), and CSR is preferred for dense/deep
traversals or when the BTREE coverage is degraded and a full scan would be paid
per hop. The two ceilings bound the **initial dispatch** frontier/hops (beyond
them CSR is always used); they are not a hard per-hop bound — the cost model
*estimates* total indexed work as ~`hops × frontier × fanout`, so dense fan-out is
priced toward CSR rather than capped mid-traversal. The override flag forces a path (the `auto` result is identical either way;
only the path differs).
