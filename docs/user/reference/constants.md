# Constants & Tunables (cheat sheet)

| Name | Value | Area |
|---|---|---|
| `MANIFEST_DIR` | `__manifest` | manifest layout |
| Init ownership object | `__init_claim.json` | transient create-if-absent ownership for strict/force init; absent after normal success, retained on indeterminate physical creation or cleanup |
| Commit graph dirs (retired) | `_graph_commits.lance` / `_graph_commit_actors.lance` | retired in Phase B; lineage lives in `__manifest` (`graph_commit` / `graph_head` rows) since RFC-013 Phase 7. A graph this binary creates has neither. |
| Recovery audit dir | `_graph_commit_recoveries.lance` | internal exact record of completed crash-recovery actions; no public CLI query yet |
| BranchMerge logical data-transaction ceiling | `MAX_BRANCH_MERGE_DATA_TRANSACTIONS = 1024` | maximum strict-insert/upsert/delete transactions one dataset may arm in a `protocol_v4` chain; a larger plan fails before sidecar arm |
| Exact recovery history-scan ceiling | `MAX_EFFECT_IDENTITY_SCAN_VERSIONS = 1026` | bounded schema-v9 transaction-history classification: 1,024 logical BranchMerge data transactions plus headroom for one allowed derived `CreateIndex` tail and one compensating `Restore`. Recovery can crash after the restore but before graph-manifest publication, so both extra versions must remain classifiable; a longer history fails closed as unverifiable rather than causing an unbounded scan |
| Run branch prefix (legacy, removed) | `__run__` | pre-v0.4.0 Run state machine; no longer a reserved name. A graph still carrying `__run__*` branches is sub-v4 and refused on open (rebuild via export/import). |
| Schema apply lock | `__schema_apply_lock__` | schema apply |
| Manifest publisher retry budget | `PUBLISHER_RETRY_BUDGET = 5` | manifest publish |
| Internal manifest schema version | `INTERNAL_MANIFEST_SCHEMA_VERSION = 6` | strict RFC-023 fencing strand; preserves v5's SchemaIR-v2 identity-bearing manifest/recovery ownership |
| Keyed-write input-entity ceiling | `KEYED_WRITE_MAX_ROWS = 8192` | one Mutation/Load keyed dataset (`mutate`, `load --mode append`/`merge`) or one BranchMerge chunk; inclusive. `load --mode overwrite` stages a whole-dataset replacement transaction and is not subject to the keyed ceiling |
| Keyed-write Arrow-memory ceiling | `KEYED_WRITE_MAX_BYTES = 33,554,432` (32 MiB) | accumulated Mutation/Load keyed input (including pending state plus a streamed mutation-update match set) or one BranchMerge entity/upsert/delete-filter chunk; a single larger entity is refused before sidecar arm. Stored update Blobs and keyed external-URI ranges/object sizes are charged before payload reads. Entity-writing BranchMerge additionally shares one operation-wide 32 MiB carried-Blob budget across exact external ranges and managed values, and caps retained external-URI metadata at 32 MiB before HEAD. The complete retained BranchMerge delete plan and the operation-wide projected scalar validation delta are separately capped at the same value; ordered merge and validation scans explicitly apply it as Lance's per-batch decoded-byte ceiling. Overwrite retains external-reference semantics |
| External Blob raw-URI ceiling | `EXTERNAL_BLOB_URI_MAX_BYTES = 65,536` (64 KiB) | one configured base or input URI, inclusive; checked before trimming, URL parsing, percent decoding, or filesystem resolution. A one-over input returns typed `resource_limit` for `external Blob URI bytes` without source I/O |
| External Blob URI-cell admission ceiling | `KEYED_WRITE_MAX_ROWS = 8,192` | operation-wide maximum across all new logical Mutation/Load input, including Overwrite and multiple types; independently, the same maximum bounds selected persisted external descriptors in an entity-writing BranchMerge. This is not Overwrite's keyed-entity ceiling. Refusal precedes external HEAD/payload I/O, recovery arm, target HEAD/ref movement, and graph visibility |
| External Blob probe concurrency | `EXTERNAL_BLOB_PROBE_CONCURRENCY = 8` | maximum simultaneous metadata requests for distinct normalized external sources in one admission plan; aliases deduplicate before entering the bounded probe stream |
| Managed Blob read-range ceiling | `BLOB_READ_RANGE_MAX_BYTES = 4,194,304` (4 MiB) | maximum bytes returned by one engine `BlobReader::read_range` call. Ranges are half-open and require `start <= end <= length`; empty-at-end is valid. A wider otherwise-valid range returns `ResourceLimitExceeded` for `Blob read range bytes` before payload I/O; callers read larger values through consecutive bounded ranges |
| Served Blob delivery envelope | `2` retained chunks; `8,388,608` bytes (8 MiB) maximum retained payload; each chunk at most `BLOB_READ_RANGE_MAX_BYTES` (4 MiB) | `GET /graphs/{id}/blob` pulls consecutive ranges under HTTP backpressure. Disconnect cancels the snapshot-pinned reader promptly. This is a per-response transport bound, not a whole-process RSS or Blob-size limit; HEAD performs no payload read |
| Served export scan targets and chunk ceiling | initial `8,192`-row estimate and approximate `33,554,432` decoded-Arrow-byte target; emitted chunks: hard `65,536`-byte maximum | `POST /graphs/{id}/export` incrementally scans exact pinned Lance versions without whole-dataset collection. Lance's byte target overrides the row setting; neither scanner setting is a hard limit. Blob descriptor batches are sliced to one logical entity before that entity's complete Blob-property set is materialized. One entity remains indivisible scratch before its encoded JSONL is split into bounded transport chunks |
| Served export transport budget | `2` queued chunks; `262,144` bytes reserved per response queue envelope; `2,097,152` bytes process-wide; `250 ms` reservation deadline; `1` nonwaiting immutable cut per graph root | each reservation covers two queued chunks, one producer chunk awaiting admission, and one consumer-current chunk. At most eight reservations coexist. Saturation or an occupied graph cut returns typed HTTP 413 before success headers; response and producer ownership retain the permit and cut until completion or disconnect unwinds both |
| Change page packing | default `1,000` changes / `4,194,304` bytes (4 MiB); maximum requested `8,192` changes / `33,554,432` bytes (32 MiB) | commit diff and feed pages. Bytes are a packing target, not a wall for one legal change: one oversized change is emitted alone, and the exception is page-wide rather than reset at each commit |
| Change-feed commit walk | default `128`; maximum `512` commits per poll | first-parent commits examined after the durable cursor, plus one bounded sentinel when needed to determine `caught_up` |
| Change continuation token | `4,096` encoded bytes maximum; exact logical ids through `256` bytes; longer ids use an at-most-`64`-byte UTF-8 prefix plus SHA-256 position | page tokens and feed cursors stay query-transport-safe even for legal long ids/branch names. A long-id resume scans the bounded prefix range to the unique digest witness; malformed, missing, or ambiguous positions fail typed |
| Ordered-scan spill envelope | `157,286,400` bytes (150 MiB) resident pool; `107,374,182,400` bytes (100 GiB) scratch quota; `39,321,600` bytes (37.5 MiB) hard sorter-input batch cap | one fresh execution context per ordered diff/feed, branch merge, or export scan. Concurrent scans each own an envelope, so these are not process-global admission limits. In-flight spill writes can overshoot the scratch quota before failure; one row remains indivisible. `LANCE_BYPASS_SPILLING` makes the scan refuse instead of weakening the bound |
| Maintenance concurrency | `OMNIGRAPH_MAINTENANCE_CONCURRENCY=8` | optimize/cleanup |
| Branch-delete fork-reclaim watchdog | `FORK_RECLAIM_ABANDON_AFTER = 600 s` | bounds how long the background fork reclaim after a branch delete (and the control gates it carries) can be pinned by a wedged object store. On expiry the reclaim is abandoned; leftovers converge via `cleanup` (ref still listed) or the next same-name branch create (ref already removed, tree residue only) |
| Graph index cache size | `8` (LRU) | runtime cache |
| Expand indexed-path frontier ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER=1024` | traversal |
| Expand indexed-path hop ceiling | `OMNIGRAPH_EXPAND_INDEXED_MAX_HOPS=6` | traversal |
| Expand CSR-build cost factor | `CSR_BUILD_FACTOR = 1.5` | traversal |
| Expand mode override | `OMNIGRAPH_TRAVERSAL_MODE` (`indexed`\|`csr`; unset = cost-based auto) | traversal |
| Default body limit | `1 MB` | HTTP server |
| Load (bulk-write) body limit | `32 MiB` | HTTP server (`/load`, `/load/ndjson`, and the deprecated `/ingest` alias) |
| Strict-input Arrow preflight | `strict_input_arrow_bytes` ceiling = `KEYED_WRITE_MAX_BYTES` (32 MiB) | a strict load's projected Arrow allocation per declaration group, charged before materialization. Applies to **every** load mode — Overwrite escapes the keyed entity/byte ceilings above but not this preflight; a larger bulk replacement is one overwrite chunk followed by merge chunks |
| Strict graph-batch line limit | `GRAPH_BATCH_MAX_LINE_BYTES = 33,554,432` (32 MiB) | each nonblank logical node/edge envelope read by `load_graph_batch{,_as}`; an oversized tail is discarded through its newline without retaining it |
| Strict graph-batch structural limit | `GRAPH_BATCH_JSON_MAX_STRUCTURAL_SLOTS = 131,072` (64 MiB modeled DOM budget at 512 bytes/slot) | pre-DOM guard over JSON object/array delimiters, commas, and colons; excess fails as typed `graph_batch_json_structural_slots` |
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
cost model over cheap graph-manifest counts (frontier size, |E|, source-vertex count,
hops) plus the index-coverage signal: the indexed path is preferred when its
frontier-relative work beats building the CSR (≈ when `hops × frontier` is a
small fraction of the source-vertex set), and CSR is preferred for dense/deep
traversals or when the BTREE coverage is degraded and a full scan would be paid
per hop. The two ceilings bound the **initial dispatch** frontier/hops (beyond
them CSR is always used); they are not a hard per-hop bound — the cost model
*estimates* total indexed work as ~`hops × frontier × fanout`, so dense fan-out is
priced toward CSR rather than capped mid-traversal. The override flag forces a path (the `auto` result is identical either way;
only the path differs).
