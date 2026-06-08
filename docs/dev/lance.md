# Lance Docs Index (for OmniGraph agents)

OmniGraph sits on top of Lance. Many problems — index lifecycle, branching, transactions, fragments, compaction, vector/FTS internals — are answered upstream in Lance's docs, not in this codebase.

This file is the curated entry point. **When you hit a Lance-shaped problem, find the matching topic below and fetch the listed URL(s) before guessing.** Don't grep our codebase for behavior that is documented authoritatively in Lance.

Base URL: `https://lance.org`. **Fetch the FULL page content, not summaries** — use `curl -sL <url> | pandoc -f html -t markdown` or paste the rendered page text manually. Tools that summarize pages (like Claude's `WebFetch`) routinely drop load-bearing details — defaults, `pub(crate)` blockers, sub-specs hidden behind navigation hubs. **Never act on a summarized fetch alone.** Keep this index curated to relevant material — the upstream sitemap has hundreds of URLs (notably the Namespace REST API model surface, Spark/Trino/Databricks integrations) that we don't use.

> **Substrate boundary check.** Before fetching, recall [docs/dev/invariants.md](invariants.md): if Lance already does the thing, we don't reimplement it. The most common reason to read these docs is to confirm a substrate behavior, not to learn what to clone.

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
| Index spec overview | https://lance.org/format/index/ |
| BTREE scalar index | https://lance.org/format/index/scalar/btree/ |
| Bitmap scalar index | https://lance.org/format/index/scalar/bitmap/ |
| Bloom-filter scalar index | https://lance.org/format/index/scalar/bloom_filter/ |
| Label-list scalar index | https://lance.org/format/index/scalar/label_list/ |
| Zone-map scalar index | https://lance.org/format/index/scalar/zonemap/ |
| R-Tree scalar index (spatial) | https://lance.org/format/index/scalar/rtree/ |
| Full-text search (FTS) index | https://lance.org/format/index/scalar/fts/ |
| N-gram scalar index | https://lance.org/format/index/scalar/ngram/ |
| Vector index | https://lance.org/format/index/vector/ |
| Fragment-reuse system index | https://lance.org/format/index/system/frag_reuse/ |
| MemWAL system index | https://lance.org/format/index/system/mem_wal/ |
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
| Fragment-reuse index | https://lance.org/format/index/system/frag_reuse/ |

### DataFusion integration

The runtime substrate that may carry our query execution. See [docs/dev/invariants.md](invariants.md): we don't rebuild relational machinery.

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

### Last alignment audit: 2026-05-22 (Lance 6.0.1 upstream; omnigraph pinned at 6.0.1)

Migration from Lance 4.0.0 → 6.0.1 landed in this cycle (DataFusion 52 → 53, Arrow 57 → 58, lance-tokenizer 6.0.1 added, tantivy* removed). Direct 4 → 6 jump; v5.x was not used as an intermediate (rationale in `~/.claude/plans/shimmering-percolating-duckling.md`). Behavior-affecting findings:

- **DatasetIndexExt moved** from `lance-index` to `lance::index` (Lance PR #6280, v5.0). Six import sites updated. `lance-index::IndexType` and `lance-index::is_system_index` stayed in `lance-index`. `omnigraph-cli` and `omnigraph-server` gained `lance = { workspace = true }` in their dev-dependencies.
- **`DescribeTableResponse` gained `is_only_declared: Option<bool>`** (lance-namespace 6.0+, v5.0 PR #6186). Set to `Some(false)` in both `BranchManifestNamespace::describe_table` and `StagedTableNamespace::describe_table` — every table we return is physically materialized via `Dataset::open`, never "declared-only."
- **`MergeInsertBuilder` execute_reader return shape preserved** `(Arc<Dataset>, MergeStats)`; the publisher CAS chain at `db/manifest/publisher.rs:370-391` works unchanged. Pinned by `tests/lance_surface_guards.rs::_compile_merge_insert_builder_method_chain`.
- **`LanceError::TooMuchWriteContention` variant retained** in v6.0.1 (no rename). The typed publisher translation at `db/manifest/publisher.rs:417-430` continues to apply. Pinned by `lance_surface_guards.rs::lance_error_too_much_write_contention_variant_exists`.
- **`ManifestLocation` field shape stable**: `.path: object_store::path::Path`, `.size: Option<u64>`, `.e_tag: Option<String>`, `.naming_scheme: ManifestNamingScheme`. Pinned by `lance_surface_guards.rs::manifest_location_field_shape`.
- **`LanceFileVersion::default()` flipped V2_0 → V2_1** (v5.0). No effect — every `data_storage_version` callsite explicitly pins `Some(LanceFileVersion::V2_2)` (load-bearing for blob v2: `Blob v2 requires file version >= 2.2` enforced in `lance/src/dataset/write.rs:748`).
- **`Dataset::checkout_version(N).await?.restore().await?`**: `restore()` takes `&mut self` and returns `Result<()>` (mutates in place, does not consume + return a new dataset). The recovery rollback hammer at `db/manifest/recovery.rs:505-522` continues to work. Pinned by `lance_surface_guards.rs::_compile_checkout_version_then_restore_signature`.
- **`DatasetBuilder::from_namespace(...).with_branch(...).with_version(...).load()`** surface preserved (the namespace builder chain at `db/manifest/namespace.rs:162-174`). Pinned by `lance_surface_guards.rs::_compile_dataset_builder_from_namespace_signature`.
- **`compact_files(&mut ds, CompactionOptions::default(), None)`** signature stable. `CompactionOptions` still does not expose `data_storage_version`; `compact_files` builds its own `WriteParams { ..Default::default() }`. Note: `LanceFileVersion::default()` is now V2_1 in v6, so optimize-rewritten fragments come out at V2_1 by default (was V2_0 in v4). Existing explicit V2_2 pins on creates/appends still apply.
- **`Dataset::delete(predicate)` returns `DeleteResult { new_dataset: Arc<Dataset>, num_deleted_rows: u64 }`** — unchanged shape. Pinned by `lance_surface_guards.rs::_compile_delete_result_field_shape`. MR-A will repurpose this guard to the staged two-phase variant once `DeleteBuilder::execute_uncommitted` migration lands.
- **File reader read methods now async** (Lance PR #6710, v6.0). No effect — omnigraph reaches Lance exclusively through `Dataset::scan` and the staged-write API.
- **Tokenizer vendored as `lance-tokenizer`** (Lance PR #6512, v6.0). No effect — no direct tokenizer imports.
- **Lance #6658 closed** (2026-05-14) but `DeleteBuilder::execute_uncommitted` did **not** ship in v6.0.1 — binary search across the release stream shows it first appears in `v7.0.0-beta.10` (the closing commits landed on main but didn't backport to the 6.x line). Tracked as MR-A: migrate `delete_where` to staged, retire the parse-time D2 mutation rule, extend recovery sidecar coverage. **Gated on the Lance v7.x bump**, not this PR. v7.0.0-rc.1 dropped 2026-05-21.
- **Lance #6666 still open** (`build_index_metadata_from_segments` public): vector-index two-phase blocked; inline `create_vector_index` residual retained.
- **Lance #6877 still open** (`MergeInsertBuilder` dup-rowid): PR #109's `SourceDedupeBehavior::FirstSeen` + `check_batch_unique_by_keys` precondition stay load-bearing.
- **`Dataset::force_delete_branch`** (`branches().delete(name, force=true)`, dataset.rs:524) tolerates a missing branch-*contents* ref (vs plain `delete_branch`'s `RefNotFound`), but on the local store still errors `NotFound` if the branch `tree/` directory is fully absent (`remove_dir_all`'s NotFound is not caught for Lance's native error variant, refs.rs:526-549). Both variants still refuse a branch with referencing descendants (`RefConflict`). `TableStore::force_delete_branch` wraps this to be fully idempotent (tolerates already-absent). The single-authority branch-delete redesign uses it for orphan reclamation (eager best-effort reclaim + cleanup reconciler). Pinned by `lance_surface_guards.rs::force_delete_branch_semantics`. Branch delete is "flip the ref atomically, then `remove_dir_all(tree/{branch})`"; branch-exclusive data lives under `tree/{branch}/` so a drop reclaims it immediately without touching `main`.
- **Lance blob-v2 `compact_files` bug** (no public issue found as of 2026-06): `compact_files` disables binary-copy for blob datasets and forces `BlobHandling::AllBinary` on the read side; the v2.1+ structural decoder then mis-counts column infos for the blob-v2 struct and fails with `Invalid user input: there were more fields in the schema than provided column indices / infos` (`lance-encoding/src/decoder.rs::ColumnInfoIter::expect_next`). This fails even a pristine uniform-V2_2 multi-fragment blob table; vector/list/scalar/ragged columns and mixed file versions all compact fine. Reads/queries use descriptor handling (`BlobHandling::default()`) and are unaffected. `optimize` skips blob-bearing tables behind `LANCE_SUPPORTS_BLOB_COMPACTION = false` (`db/omnigraph/optimize.rs`), reporting `SkipReason::BlobColumnsUnsupportedByLance`. Pinned by `lance_surface_guards.rs::compact_files_still_fails_on_blob_columns`, which turns red when the bug is fixed → flip the gate, remove the skip branch + the `maintenance.rs::optimize_skips_blob_table_and_reports_skip` skip assertions.

Surface guards added: `crates/omnigraph/tests/lance_surface_guards.rs` (10 named guards; 5 runtime + 5 compile-only). Future Lance bumps re-run this file first as the smoke check. Two additional guards from the original plan deferred to follow-up (`manifest_cas_returns_row_level_contention_variant` needs full publisher-race harness; `table_version_metadata_byte_compatible_with_v4` needs `pub(crate)` reach extension).

Bump this date stanza on the next alignment pass.
