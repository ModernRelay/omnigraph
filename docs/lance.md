# Lance Docs Index (for OmniGraph agents)

OmniGraph sits on top of Lance. Many problems — index lifecycle, branching, transactions, fragments, compaction, vector/FTS internals — are answered upstream in Lance's docs, not in this repo.

This file is the curated entry point. **When you hit a Lance-shaped problem, find the matching topic below and fetch the listed URL(s) before guessing.** Don't grep our codebase for behavior that is documented authoritatively in Lance.

Base URL: `https://lance.org`. **Fetch the FULL page content, not summaries** — use `npx mdrip <url>` (or `npx mdrip --max-chars 200000 <url>` for very long pages). Tools that summarize pages (like Claude's `WebFetch`) routinely drop load-bearing details — defaults, `pub(crate)` blockers, sub-specs hidden behind navigation hubs. If `npx mdrip` is unavailable, fall back to `curl <url> | pandoc -f html -t markdown` or paste the rendered page text manually; **never act on a summarized fetch alone**. Keep this index curated to relevant material — the upstream sitemap has hundreds of URLs (notably the Namespace REST API model surface, Spark/Trino/Databricks integrations) that we don't use.

> **Substrate boundary check.** Before fetching, recall [docs/invariants.md §I](invariants.md): if Lance already does the thing, we don't reimplement it. The most common reason to read these docs is to confirm a substrate behavior, not to learn what to clone.

## Quick-start (read these once per project)

| Read when | URL |
|---|---|
| Onboarding to Lance — concepts in 10 min | https://lance.org/quickstart/ |
| Onboarding to vector search | https://lance.org/quickstart/vector-search/ |
| Onboarding to full-text search | https://lance.org/quickstart/full-text-search/ |
| Onboarding to versioning / time travel | https://lance.org/quickstart/versioning/ |
| Lance's own AGENTS.md (its agent guide) | https://lance.org/format/AGENTS/ |

## By problem domain

### Storage format & file layout

Touching `db/manifest`, fragment lifecycle, dataset reconstruction, or anything that reads/writes raw Lance state.

| Topic | URL |
|---|---|
| Lance file format overview | https://lance.org/format/ |
| File-level format spec | https://lance.org/format/file/ |
| File encoding | https://lance.org/format/file/encoding/ |
| File-level versioning | https://lance.org/format/file/versioning/ |
| Table layout (fragments, manifest) | https://lance.org/format/table/layout/ |
| Table schema metadata | https://lance.org/format/table/schema/ |
| Table-level versioning | https://lance.org/format/table/versioning/ |
| Transactions (commit semantics, conflict types) | https://lance.org/format/table/transaction/ |
| MemWAL (durability story) | https://lance.org/format/table/mem_wal/ |
| Row-ID lineage (stable row IDs) | https://lance.org/format/table/row_id_lineage/ |
| Branches & tags (Lance native) | https://lance.org/format/table/branch_tag/ |

### Branching / tags / time travel

Touching graph-level branches, snapshots, run isolation, the commit graph.

| Topic | URL |
|---|---|
| Branch & tag format | https://lance.org/format/table/branch_tag/ |
| Tags & branches operational guide | https://lance.org/guide/tags_and_branches/ |
| Versioning quick-start | https://lance.org/quickstart/versioning/ |
| Table-level versioning spec | https://lance.org/format/table/versioning/ |

### Indexes

Adding/changing index types, fixing coverage, debugging FTS or vector recall, designing the reconciler.

| Topic | URL |
|---|---|
| Index spec overview | https://lance.org/format/table/index/ |
| BTREE scalar index | https://lance.org/format/table/index/scalar/btree/ |
| Bitmap scalar index | https://lance.org/format/table/index/scalar/bitmap/ |
| Bloom-filter scalar index | https://lance.org/format/table/index/scalar/bloom_filter/ |
| Label-list scalar index | https://lance.org/format/table/index/scalar/label_list/ |
| Zone-map scalar index | https://lance.org/format/table/index/scalar/zonemap/ |
| R-Tree scalar index (spatial) | https://lance.org/format/table/index/scalar/rtree/ |
| Full-text search (FTS) index | https://lance.org/format/table/index/scalar/fts/ |
| N-gram scalar index | https://lance.org/format/table/index/scalar/ngram/ |
| Vector index | https://lance.org/format/table/index/vector/ |
| Fragment-reuse system index | https://lance.org/format/table/index/system/frag_reuse/ |
| MemWAL system index | https://lance.org/format/table/index/system/mem_wal/ |
| HNSW Rust example | https://lance.org/examples/rust/hnsw/ |
| Distributed indexing | https://lance.org/guide/distributed_indexing/ |
| Tokenizer (FTS, n-gram) | https://lance.org/guide/tokenizer/ |

### Reads & writes

Touching the bulk loader, mutation execution, `merge_insert`, `WriteMode` selection.

| Topic | URL |
|---|---|
| Read-and-write guide | https://lance.org/guide/read_and_write/ |
| Distributed write | https://lance.org/guide/distributed_write/ |
| Rust example: write & read a dataset | https://lance.org/examples/rust/write_read_dataset/ |

### Schema evolution

Touching `apply_schema`, the migration planner, additive evolution.

| Topic | URL |
|---|---|
| Data-evolution guide | https://lance.org/guide/data_evolution/ |
| Migration guide | https://lance.org/guide/migration/ |

### Object store / S3

Touching `storage.rs`, S3-compatible backends (RustFS, MinIO), env vars.

| Topic | URL |
|---|---|
| Object-store guide | https://lance.org/guide/object_store/ |

### Data types

Touching schema-language scalar mappings, blob columns, JSON, list columns.

| Topic | URL |
|---|---|
| Data types overview | https://lance.org/guide/data_types/ |
| Arrays / list types | https://lance.org/guide/arrays/ |
| Blobs (LargeBinary) | https://lance.org/guide/blob/ |
| JSON | https://lance.org/guide/json/ |

### Performance & tuning

Optimizing scans, fragment counts, cache behavior, memory pool sizing.

| Topic | URL |
|---|---|
| Performance guide | https://lance.org/guide/performance/ |

### Compaction & cleanup

Touching `omnigraph optimize` / `cleanup`, the underlying `compact_files` / `cleanup_old_versions`.

| Topic | URL |
|---|---|
| Read-and-write guide (covers `compact_files`, `cleanup_old_versions`) | https://lance.org/guide/read_and_write/ |
| Performance (compaction tradeoffs) | https://lance.org/guide/performance/ |
| Fragment-reuse index | https://lance.org/format/table/index/system/frag_reuse/ |

### DataFusion integration

The runtime substrate that may carry our query execution. See [docs/invariants.md §I.4](invariants.md): we don't rebuild relational machinery.

| Topic | URL |
|---|---|
| DataFusion integration | https://lance.org/integrations/datafusion/ |

### SDK reference

Looking up a specific Rust API (signature, return type, error variant).

| Topic | URL |
|---|---|
| SDK docs landing | https://lance.org/sdk_docs/ |

## What's not in this index (and why)

- **Namespace REST API model surface** (`/format/namespace/client/operations/models/...`) — hundreds of REST schema docs for the Lance Namespace catalog API. Omnigraph does not run a Lance Namespace server, so these are not reachable from our problem space.
- **Spark / Trino / Databricks / Dataproc / Hive / Glue / Polaris / Iceberg / Unity / OneLake / Gravitino integrations** — not part of OmniGraph's deployment surface.
- **Python / TF / PyTorch / Hugging Face / Ray integrations** — OmniGraph is Rust-only; Python notebooks aren't relevant.
- **Community / governance / release / voting / PMC pages** — meta, not technical.

If a future need pulls one of these into scope, add a row to the matching domain section above and link it from `AGENTS.md`'s topic index.

## Maintenance

When Lance ships a major release that changes any of the above (file format bump, new index type, transaction semantics change, new branching primitive), refresh this index in the same change as the omnigraph upgrade. Stale Lance pointers are worse than no pointers.

### Last alignment audit: 2026-05-02 (Lance 4.0.1 upstream; omnigraph pinned at 4.0.0)

A full read-through of every index page above was performed in the MR-793 cycle. Findings (no code changes required for PR #70):

- The MemWAL system index has three deeper sub-pages that this index does not yet list — they're load-bearing for understanding crash-recovery semantics and are needed before MR-847 (recovery reconciler) implementation. Add when located: `MemWAL Index Overview`, `MemWAL Index Details`, `MemWAL Implementation` (linked from the parent MemWAL page but at sub-URLs not currently in `lance.md`).
- The distributed-indexing guide names Python APIs (`commit_existing_index_segments`, `merge_existing_index_segments`); the Rust analogues exist via `CreateIndexBuilder::execute_uncommitted` for scalar indices but **`build_index_metadata_from_segments` is `pub(crate)`** and blocks vector-index two-phase commits from outside the lance crate. Filed [lance-format/lance#6666](https://github.com/lance-format/lance/issues/6666) as a companion to [#6658](https://github.com/lance-format/lance/issues/6658).
- "Stable Row ID for Index" is documented as **experimental** in lance-4.0.x. Our datasets enable stable row IDs at the dataset level (`WriteParams::enable_stable_row_ids = true`); confirming whether our created indices opt into stable-row-id mode is a follow-up worth doing before MR-848 (index reconciler) lands.
- Fragment Reuse Index (FRI) is documented as one of three compaction strategies. omnigraph currently uses option 2 (immediate index rewrite at compaction time, via `omnigraph optimize`'s post-compaction rebuild). Adopting FRI is the explicit option for compaction-friendly index updates; relevant to MR-848.

Bump this date stanza on the next alignment pass.
