# Constants & Tunables (cheat sheet)

| Name | Value | Area |
|---|---|---|
| `MANIFEST_DIR` | `__manifest` | manifest layout |
| Commit graph dir | `_graph_commits.lance` | commit graph |
| Run registry dir (legacy, removed) | `_graph_runs.lance` | inert post-v0.4.0; bytes remain until a prefix-delete primitive lands |
| Run branch prefix (legacy, removed) | `__run__` | swept off `__manifest` by the internal schema migration; no longer a reserved name |
| Schema apply lock | `__schema_apply_lock__` | schema apply |
| Manifest publisher retry budget | `PUBLISHER_RETRY_BUDGET = 5` | manifest publish |
| Internal manifest schema version | `INTERNAL_MANIFEST_SCHEMA_VERSION = 3` | manifest migrations |
| Merge stage batch | `MERGE_STAGE_BATCH_ROWS = 8192` | merge execution |
| Maintenance concurrency | `OMNIGRAPH_MAINTENANCE_CONCURRENCY=8` | optimize/cleanup |
| Lance blob compaction support | `LANCE_SUPPORTS_BLOB_COMPACTION = false` | optimize |
| Graph index cache size | `8` (LRU) | runtime cache |
| Expand indexed-path frontier ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER=1024` | traversal |
| Expand indexed-path hop ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS=6` | traversal |
| Expand CSR-build cost factor | `CSR_BUILD_FACTOR = 1.5` | traversal |
| Expand mode override | `OMNIGRAPH_TRAVERSAL_MODE` (`indexed`\|`csr`; unset = cost-based auto) | traversal |
| Default body limit | `1 MB` | HTTP server |
| Ingest body limit | `32 MB` | HTTP server |
| Engine embed model | `gemini-embedding-2-preview` | engine embedding |
| Compiler embed model | `text-embedding-3-small` | compiler embedding |
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
