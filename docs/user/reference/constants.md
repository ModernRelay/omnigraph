# Constants & Tunables (cheat sheet)

| Name | Value | Area |
|---|---|---|
| `MANIFEST_DIR` | `__manifest` | manifest layout |
| Commit graph dirs (retired) | `_graph_commits.lance` / `_graph_commit_actors.lance` | retired in Phase B; lineage lives in `__manifest` (`graph_commit` / `graph_head` rows) since RFC-013 Phase 7. A graph this binary creates has neither. |
| Recovery audit dir | `_graph_commit_recoveries.lance` | internal exact record of completed crash-recovery actions; no public CLI query yet |
| Exact recovery history-scan ceiling | `MAX_EFFECT_IDENTITY_SCAN_VERSIONS = 1024` | v3/v4 recovery fails closed as unverifiable rather than scanning an unbounded Lance transaction history; a v4 logical merge chain is at most three commits today, with only derived `CreateIndex` commits allowed after it |
| Run branch prefix (legacy, removed) | `__run__` | pre-v0.4.0 Run state machine; no longer a reserved name. A graph still carrying `__run__*` branches is sub-v4 and refused on open (rebuild via export/import). |
| Schema apply lock | `__schema_apply_lock__` | schema apply |
| Manifest publisher retry budget | `PUBLISHER_RETRY_BUDGET = 5` | manifest publish |
| Internal manifest schema version | `INTERNAL_MANIFEST_SCHEMA_VERSION = 4` | manifest migrations (v4 = graph lineage in `__manifest`, RFC-013 Phase 7) |
| Merge stage batch | `MERGE_STAGE_BATCH_ROWS = 8192` | merge execution |
| Maintenance concurrency | `OMNIGRAPH_MAINTENANCE_CONCURRENCY=8` | optimize/cleanup |
| Graph index cache size | `8` (LRU) | runtime cache |
| Expand indexed-path frontier ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER=1024` | traversal |
| Expand indexed-path hop ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS=6` | traversal |
| Expand CSR-build cost factor | `CSR_BUILD_FACTOR = 1.5` | traversal |
| Expand mode override | `OMNIGRAPH_TRAVERSAL_MODE` (`indexed`\|`csr`; unset = cost-based auto) | traversal |
| Default body limit | `1 MB` | HTTP server |
| Load (bulk-write) body limit | `32 MB` | HTTP server (`/load`; shared by the deprecated `/ingest` alias) |
| Default embed provider/model | `openai-compatible` / `openai/text-embedding-3-large` | engine embedding |
| OpenAI-direct embed model | `text-embedding-3-large` | engine embedding |
| Gemini-direct embed model | `gemini-embedding-2` | engine embedding |
| Embed deadline | `OMNIGRAPH_EMBED_DEADLINE_MS=60000` | engine embedding |
| Embed timeout | `OMNIGRAPH_EMBED_TIMEOUT_MS=30000` | engine embedding |
| Embed retries | `OMNIGRAPH_EMBED_RETRY_ATTEMPTS=4` | engine embedding |
| Embed retry backoff | `OMNIGRAPH_EMBED_RETRY_BACKOFF_MS=200` | engine embedding |
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
