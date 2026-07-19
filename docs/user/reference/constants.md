# Constants & Tunables (cheat sheet)

| Name | Value | Area |
|---|---|---|
| `MANIFEST_DIR` | `__manifest` | manifest layout |
| Commit graph dirs (retired) | `_graph_commits.lance` / `_graph_commit_actors.lance` | retired in Phase B; lineage lives in `__manifest` (`graph_commit` / `graph_head` rows) since RFC-013 Phase 7. A graph this binary creates has neither. |
| Recovery audit dir | `_graph_commit_recoveries.lance` | internal exact record of completed crash-recovery actions; no public CLI query yet |
| BranchMerge logical data-transaction ceiling | `MAX_BRANCH_MERGE_DATA_TRANSACTIONS = 1024` | maximum strict-insert/upsert/delete transactions one table may arm in a `protocol_v4` chain; a larger plan fails before sidecar arm |
| Exact recovery history-scan ceiling | `MAX_EFFECT_IDENTITY_SCAN_VERSIONS = 1026` | bounded schema-v9 transaction-history classification: 1,024 logical BranchMerge data transactions plus headroom for one allowed derived `CreateIndex` tail and one compensating `Restore`. Recovery can crash after the restore but before manifest publication, so both extra versions must remain classifiable; a longer history fails closed as unverifiable rather than causing an unbounded scan |
| Run branch prefix (legacy, removed) | `__run__` | pre-v0.4.0 Run state machine; no longer a reserved name. A graph still carrying `__run__*` branches is sub-v4 and refused on open (rebuild via export/import). |
| Schema apply lock | `__schema_apply_lock__` | schema apply |
| Manifest publisher retry budget | `PUBLISHER_RETRY_BUDGET = 5` | manifest publish |
| Internal manifest schema version | `INTERNAL_MANIFEST_SCHEMA_VERSION = 8` | strict RFC-026 B1 strand; preserves v5 identity, v6 keyed fencing, and v7 stream-lifecycle authority while activating stream-config v2 plus recovery-v11 for the private data-bearing core. V7 and older graphs require export/init/load rebuild |
| Keyed-write row ceiling | `KEYED_WRITE_MAX_ROWS = 8192` | one Mutation/Load keyed table or one BranchMerge chunk; inclusive |
| Keyed-write Arrow-memory ceiling | `KEYED_WRITE_MAX_BYTES = 33,554,432` (32 MiB) | accumulated Mutation/Load keyed input (including pending state plus a streamed mutation-update match set) or one BranchMerge row/upsert/delete-filter chunk; a single larger row is refused before sidecar arm. Stored update Blobs and keyed external-URI ranges/object sizes are charged before payload reads. The complete retained BranchMerge delete plan and the operation-wide projected scalar validation delta are separately capped at the same value; ordered merge and validation scans explicitly apply it as Lance's per-batch decoded-byte ceiling. Overwrite retains external-reference semantics |
| Private RFC-026 B1 generation ceiling | `8,192 rows`, `8,192 batches`, `33,554,432` Arrow bytes | one no-roll MemWAL generation; the private seam refuses the next put before exceeding any bound |
| Private RFC-026 B1 resident-writer ceiling | `1` per graph root and table | evidence-qualified process-local worker admission; not a public throughput contract. B2 must requalify any higher multi-resident limit with an RSS cell |
| Private RFC-026 B1 aggregate Arrow reservation | `33,554,432` bytes (32 MiB) per graph root | cheap raw caller row/byte bounds reject obviously over-cap input before recovery I/O; raw-fit input then receives exact post-tombstone validation at that same pre-recovery boundary. After any recovery/authority prelude, the exact charge is recomputed and reserved against the aggregate before any same-key queue wait, shared admission, detached ownership, or cold claim; the permit transfers into the resident generation and remains charged through fold publication. Distinct from whole-process RSS |
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
