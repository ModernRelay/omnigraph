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

### Last alignment audit: 2026-07-12 (Lance 9.0.0-beta.21 upstream; omnigraph pinned at 9.0.0-beta.21 via git rev)

The pin advanced from `v9.0.0-beta.15` (`f24e42c1`) to
`v9.0.0-beta.21` (`1aec1465`) after reviewing all 77 intervening commits and
the beta.16–beta.21 release notes. Every Lance workspace dependency, including
the engine's `lance-io` test dependency, uses that same rev. Arrow remains 58,
DataFusion remains 53, and `object_store` remains 0.13.2; the only new
third-party lockfile entry is `bytemuck_derive`, enabled by Lance's encoding
work. No upstream migration note or Lance file-version bump accompanies this
beta-to-beta move; OmniGraph continues to write explicit V2_2 datasets and
requires one pinned Lance version across a deployment.

Behavior-affecting findings in this audit:

- **The RFC-022 full-table vector-index stage is no longer substrate-blocked.**
  `CreateIndexBuilder::execute_uncommitted` builds the physical vector artifact
  and returns complete `IndexMetadata`; Lance's own `execute` wraps that value
  directly in public `Operation::CreateIndex`. That source is byte-for-byte
  unchanged between beta.15 and beta.21 and has had this shape since upstream
  #7129. OmniGraph can therefore mirror the scalar staging path, pre-mint the
  transaction identity, and commit through `commit_staged_exact`. The exact
  EnsureIndices v8 adapter now does so: all missing BTREE, FTS, and full-table
  vector artifacts for a table are combined into one `Operation::CreateIndex`,
  and `InlineCommitResidual` has been removed. This closes the OmniGraph rollout
  gap for the one-segment full-table IVF-Flat build.
- **The generic multi-segment API is still not an exact-commit primitive.**
  `commit_existing_index_segments` constructs and inline-commits its own
  transaction with default retry behavior, while
  `build_index_metadata_from_segments` remains `pub(crate)`. Lance #6666 is
  therefore still relevant to external generic multi-segment publication, but
  it does not block OmniGraph's current full-table vector shape.
- **Maintenance still has no stable public caller-controlled transaction
  boundary.** In beta.21, `compact_files` and `optimize_indices` remain
  high-level committing operations; their internal transaction construction is
  not a public contract that OmniGraph can pre-mint and later prove as one
  complete compact/reindex effect. RFC-022 therefore keeps Optimize's bounded
  schema-v2 adapter instead of binding an "exact-v9" protocol to beta internals.
  Revisit exact Optimize provenance only after Lance exposes a stable public
  maintenance-transaction API and OmniGraph has distributed recovery fencing;
  the latter is independently required before destructive recovery is safe
  against a live foreign process.
- **Index construction gained correctness and bounded-resource fixes:** beta.17
  prevents an FTS builder thread-pool deadlock and bounds tail-partition merge
  memory; beta.18 fixes a streaming IVF training hang; beta.19 caps nullable
  IVF training prefetch memory and translates address-domain scalar-index
  results under stable row ids; beta.21 packs prewarmed FTS posting groups.
  Beta.19 also completes the ICU English stop-word list (#7621), which changes
  BM25 document-length normalization and therefore legitimately changes some
  score/rank ties. `search::rrf_fuses_two_fts_fields` pins the new deterministic
  fused order rather than treating output ordering as an incidental detail.
- **MemWAL durability tightened:** beta.17 fences the writer after WAL
  persistence failure and makes the memtable flush threshold slice-aware.
  These are upstream correctness fixes for RFC-026's chosen substrate, not a
  change to its OmniGraph enrollment or acknowledgement protocol.
- **Blob and runtime fixes are additive:** beta.20 fixes late-materialized blob
  columns being read as binary; beta.21 fixes empty-blob structural round trips
  and removes an aliased mutable Tokio runtime reference.
- **Native `DirectoryNamespace` reads now open an existing manifest without
  migrating it** (beta.19 #7687). With directory listing disabled, native
  `list_table_versions` / `describe_table_version` can therefore resolve
  OmniGraph's manifest rows and enumerate the physical Lance history, including
  a data-table HEAD that OmniGraph has not graph-published. This is visibility,
  not graph authority: those per-table APIs cannot atomically advance
  OmniGraph's graph-wide table pointers, and production does not route through
  the native namespace. The manifest guard now pins both sides explicitly: the
  native namespace sees the unpublished physical version while
  `ManifestCoordinator::refresh` remains on the unchanged logical snapshot.
  This supersedes the beta.15 `TableNotFound` behavior recorded below and also
  proves that read-only namespace construction does not rewrite OmniGraph's
  legacy PK annotation.
- **No RFC-022 control-protocol change was found.** Native branch create/delete
  remains the same two-phase shape. Beta.18 does make force-deleting a fully
  absent branch tree idempotent by mapping object-store absence to Lance
  `NotFound` and accepting that during cleanup; the positive behavior is pinned
  by Guard 9. OmniGraph retains its outer `RefNotFound` / `NotFound`
  normalization because Lance still performs the branch-contents existence
  check and delete separately, leaving a concurrent-delete TOCTOU window around
  the otherwise-idempotent tree cleanup.
  The index-create source is unchanged; compaction changes are mechanical
  iterator cleanup; the transaction change only centralizes calculation of the
  next version. Branch checkout now reuses the session-cached manifest,
  improving the warm-access shape without changing branch identity or commit
  semantics.

The existing Lance surface guards plus the canonical workspace and failpoint
suites are the compatibility gate for this pin. Keep the beta.15 audit below as
historical provenance for the larger 7.0 → 9.0 migration.

### Prior alignment audit: 2026-07-05 (Lance 9.0.0-beta.15 upstream; omnigraph pinned at 9.0.0-beta.15 via git rev)

Migration from Lance 7.0.0 → 9.0.0-beta.15 landed in this cycle. The 9.x betas
are **git tags only** (crates.io carries ≤ 8.0.0), so every lance crate is a git
dependency rev-pinned to the `v9.0.0-beta.15` tag commit (`f24e42c1`); switch to
the crates.io release at 9.0.0 stable. 382 upstream commits reviewed across two
audit legs (243 in 7→8, 139 in 8→9-beta.15). **Arrow stayed 58, DataFusion
stayed 53, object_store stayed 0.13.2** — zero ecosystem churn. **No table/file
format or minimum-reader-version movement in either leg**: data written by this
binary under our settings (explicit V2_2 pins) round-trips to a 7.0.0 reader;
the one soft door is FTS v2 index files (default for NEW inverted indexes since
9.0.0-beta.11 — readable by 7.0.0+, rebuildable derived state). Behavior-affecting
findings:

- **lance#7480 shipped (9.0.0-beta.11)** → the `vendor/lance-table` pin and its
  `[patch.crates-io]` entry are **retired** per their documented removal
  condition; `filtered_scan_tolerates_merge_update_row_id_overlap` now passes on
  stock lance-table and stays as the regression tripwire.
- **lance#7320 shipped (9.0.0-beta.1)** → the sequential BTREE segment-merge
  corruption (lance#7230: update-preserved row ids duplicated by
  `build_stable_row_id_filter` when compaction skips the superseded fragment;
  empirically reproduced on 7.0.0 via load → keyed update → optimize → broken
  filtered reads AND broken keyed writes) is fixed upstream. No engine-side
  mitigation needed.
- **Blob-v2 compaction fixed** (8.0.0 PR #7017, hardened by #7618 in beta.15
  after a beta.13 regression) → executed the documented removal plan:
  `LANCE_SUPPORTS_BLOB_COMPACTION`, the optimize skip branch, and
  `SkipReason::BlobColumnsUnsupportedByLance` deleted; the guard inverted to a
  positive pin (`compact_files_succeeds_on_blob_columns`) and
  `maintenance.rs::optimize_compacts_blob_table_alongside_plain_table` asserts
  blob tables compact, publish, and keep every row.
- **Filter-literal coercion improved** (v8 #6935 et al.): a width-mismatched
  literal (`n32 = 5i64`) is now coerced to the column type BEFORE pushdown and
  USES the BTREE (7.0.0 planned an index-defeating column cast).
  `scalar_index_use_requires_matched_literal_type` re-pinned to the new truth;
  `query.rs::literal_to_typed_expr` stays load-bearing.
- **Branch-consistency enforcement on open** (9.x): `DatasetBuilder` now errors
  loudly when the resolved manifest belongs to a different branch than
  requested ("open of branch X resolved a manifest belonging to branch Y") —
  the substrate enforcing what omnigraph's entry-owner resolution always did.
  Production opens by owner-resolved location (unaffected); the lazy-fork
  namespace test pins both the error and the owner-branch open.
- **Native branch control remains two-phase at beta.15.**
  `Dataset::create_branch` commits the shallow-cloned `tree/{branch}` dataset
  before validating/writing authoritative `BranchContents`; its random
  `BranchIdentifier` element is minted only in that second phase and cannot be
  pre-minted through the public API. OmniGraph therefore validates first and
  classifies an ambiguous result from exact parent metadata rather than
  inventing an identifier. Delete removes `BranchContents` before tree cleanup
  and exposes no compare-and-delete primitive. Slash-separated names share
  nested physical directories; `force_delete_branch` deliberately leaves an
  ancestor tree while a live path-child exists, so live graph names are
  path-prefix-disjoint. The public format page does not currently spell out the
  identifier field, so the pinned Rust shape remains load-bearing. Guard 9 in
  `lance_surface_guards.rs` pins clone-only raw-create failure plus force reclaim;
  `src/branch_control.rs` pins delete classification and JSON identity fencing.
- **Native DirectoryNamespace churn at beta.15** (#7222 removed
  `table_version_storage_enabled` + the `__manifest` version-storage
  experiment; #7176/#7191/#7234 rewrote manifest handling): the decoupling
  guard then observed manifest-tracked tables as `TableNotFound` to native
  tooling with dir-listing disabled and was realigned. Beta.19 #7687 later
  replaced that accidental read-time decoupling with a non-mutating manifest
  open; the beta.21 audit above records the current behavior.
- **merge_insert substantially rewritten** (+1321 lines across #6878 composite
  indexed join keys, #7484 WhenNotMatchedBySource::Delete/Fail fixed on the
  indexed-scan path, #7410/#7359 stale scalar-index entries after
  stable-row-id update): every edge-table merge (src+dst BTREE keys) now takes
  the AND-folded index-probe path. Full suite green; the surfaces omnigraph
  pins (`execute_reader` return shape, WhenMatched/WhenNotMatched,
  `SourceDedupeBehavior`) are unchanged. **#6877 was fixed in 8.0.0 (#6965)**
  — re-evaluate retiring `SourceDedupeBehavior::FirstSeen` +
  `check_batch_unique_by_keys` in a follow-up.
- **optimize_indices steady-state is a true no-op** (v8 #6905: no version bump
  when there is no index work) and **FTS AND queries enforce required terms**
  (#7385 — `match_text`/`bm25` result sets can legitimately change). Both
  absorbed by the existing suites (68/68 green, incl. cost gates within slack).
- **New failure surfaces**: every commit path has a default 30-minute timeout
  (v8 #6773); `alter_columns` with a cast on an indexed column now hard-errors
  (v8 #7158 — omnigraph's planner never emits casts today, OG-MF-106 refuses
  type changes first).
- **Audit correction:** beta.15 already exposed a usable uncommitted shape for
  OmniGraph's one-segment full-table vector build; Lance's own `execute` wraps
  that returned `IndexMetadata` directly in `Operation::CreateIndex`. The
  original audit incorrectly treated #6666's generic multi-segment helper as a
  blocker. The `create_vector_index` inline residual and its recovery coverage
  were retained because the OmniGraph adapter migration had not landed, not
  because this full-table shape was impossible. #6914 per-row version-metadata
  refresh remained unshipped, and upstream
  **#7508 is open** (FTS "record batch length" errors after frequent
  `optimize_indices` — omnigraph's reconciler call pattern; watch it).
- **RowAddrTreeMap moved** from `lance_core::utils::mask` to the new
  `lance-select` crate — the single compile break in the whole migration.
- Beta-line caveat: the blob read path regressed inside the line (beta.13,
  fixed beta.15 by #7618) — pre-release churn is real; re-audit before any
  beta→stable or beta→beta advance.

Bump this date stanza on the next alignment pass.

### Patch pin: 2026-07-02 (vendored lance-table 7.0.0 + lance#7480) — RETIRED at the 9.0.0-beta.15 bump (fix upstream since 9.0.0-beta.11)

Not a version bump — a single-fix vendored pin. `[patch.crates-io] lance-table = { path = "vendor/lance-table" }` points at the pristine published 7.0.0 source carrying ONLY the lance#7480 `rowids/index.rs` hunk (merged upstream 2026-07-01, a few hours AFTER v8.0.0 was cut, so it ships in no release ≤ 8.0.0):

- **Why:** an update-style `merge_insert` over a merge-written fragment legally reuses the updated rows' stable row ids (row-id-lineage spec: updates preserve `_rowid`) while the superseded fragment keeps its full sequence + a deletion vector. A later delete leaves the overlapping id range sparsely tiled, and unpatched `RowIdIndex::new` asserted dense tiling — every filtered read that builds the id→address map then fails ("Wrong range" debug assert; "all columns in a record batch must have the same length" or a silently-wrong batch in release). Upstream bug lance#7444; tracked as `iss-merge-rowid-overlap-corrupts-filtered-reads` / `blk-lance-7444` on the dev graph. The fix is read-side only: the on-disk overlap is spec-legal, so already-written graphs become readable as-is — no data repair.
- **Pinned by** `lance_surface_guards.rs::filtered_scan_tolerates_merge_update_row_id_overlap` (a faithful transcription of lance#7444's minimal repro — merge-seed → merge-update → delete → filter + `with_row_id`; the merge-on-merge seed and the filtered-with-row-id read are both load-bearing) and the engine-level `writes.rs::filtered_read_after_merge_update_and_delete_keeps_row_ids_consistent` (+ its green append-only control).
- **Removal condition:** drop `vendor/lance-table` + the `[patch.crates-io]` entry at the first Lance bump whose `lance-table` ships lance#7480 (9.0.0, or a backported 8.0.1). The surface guard keeps the removal honest in both directions. Verify-the-delta instructions live in `vendor/lance-table/README.omnigraph.md`.
- **Related, found during the same investigation, NOT consumed by this pin:** Lance v8.0.0 (released 2026-07-01) fixes merge_insert's legacy-Merger silent match-dropping under a scalar-indexed join key with a partial-schema / all-null-leading-column source (PR #7251) — the path any iss-986 field-level-merge implementation would use, since omnigraph BTREE-indexes every merge join key. omnigraph's *current* full-schema batches dodge #7251 by construction (the compiler puts `id` / `src`+`dst` at the exact leading positions the buggy check inspects — catalog/mod.rs:220,275). Gate iss-986 on the 7→8 bump.

### Last alignment audit: 2026-06-15 (Lance 7.0.0 upstream; omnigraph pinned at 7.0.0)

Migration from Lance 6.0.1 → 7.0.0 landed in this cycle. **Arrow stayed 58, DataFusion stayed 53** (no change) — the only transitive bump is `object_store` 0.12.5 → 0.13.2. 141 upstream commits reviewed (6.0.1 → 7.0.0); no fixes lost (the 6.0.x release-branch backports are all forward-ported into 7.0.0). Behavior-affecting findings:

- **object_store 0.13 moved convenience methods behind a new `ObjectStoreExt` trait** (`get`/`put`/`head`/`rename`/`delete`; `list`/`list_with_delimiter`/`put_opts` stay on the core `ObjectStore` trait). Fix = add `use object_store::ObjectStoreExt;` to `storage.rs` and `db/manifest/namespace.rs`; no call-site changes. Mirrors Lance's own migration in PR #6672. The local-FS `PutMode::Update` gap is unchanged (still unimplemented upstream), so `storage.rs::write_text_if_match`'s local content-token emulation stays.
- **`roaring` must be pinned to 0.11.4** (`cargo update -p roaring --precise 0.11.4`). Lance 7.0.0's `UpdatedFragmentOffsets` newtype (PR #6650) derives `Eq` over `HashMap<u64, RoaringBitmap>`, which needs `RoaringBitmap: Eq` — added only in roaring 0.11.4 (roaring-rs PR #341). Lance's loose `roaring = "0.11"` constraint otherwise resolves the broken 0.11.3 and **lance itself fails to compile** (`RoaringBitmap: Eq is not satisfied`). roaring is transitive (no direct workspace dep); the pin lives only in `Cargo.lock`.
- **`_row_created_at_version` for merge-insert INSERT rows now = the commit version** (PR #6774; was a fallback of 1 / dataset-creation version). Flipped `lance_version_columns.rs::lance_merge_insert_new_row_stamps_created_at_version` to assert `== v2`. Production change-detection keys on `_row_last_updated_at_version` + ID-set membership, so classification logic is unaffected (the `changes/mod.rs` rationale comment was corrected).
- **BTREE range-query bound inclusiveness fixed** (PR #6796, issue #6792): `x <= hi AND x > lo` returned the wrong boundary row on 6.0.1. omnigraph today builds BTREE only on string `@key` columns (`id`/`src`/`dst`) and queries them by equality/IN, not range, so its *current* query patterns almost certainly never hit this bug — but the corrected boundary semantics are a contract we rely on the moment a BTREE-range path appears (BTREE-on-properties via the index-type tickets, or a range-on-key query). Pinned by `lance_surface_guards.rs::btree_range_query_boundary_is_correct` (reproduces #6792's 5-row + BTREE shape).
- **`WriteParams::auto_cleanup` default flipped from on (every-20-commits) to `None`** (PR #6755). On 6.0.1 the on-by-default hook could GC versions the `__manifest` pins for snapshots/time-travel. omnigraph owns cleanup explicitly (`optimize.rs::cleanup_all_tables`). Two parts to the fix, because `auto_cleanup` is **create-time config only and has no effect on existing datasets** (Lance `write.rs` docs): (1) `auto_cleanup: None` at all 11 `WriteParams` sites so *new* datasets store no cleanup config; (2) — the load-bearing half — `skip_auto_cleanup: true` on every commit path, because graphs created **before** the bump still carry the on-config in their datasets, and Lance's hook fires off the *dataset's stored* config at commit time (`io/commit.rs`: `if !commit_config.skip_auto_cleanup`). So the staged commit path (`commit_staged` → `CommitBuilder::with_skip_auto_cleanup(true)`), the `__manifest` publisher (`MergeInsertBuilder::skip_auto_cleanup(true)`), and the direct `WriteParams` paths all skip the hook. Without this, an upgraded graph would still auto-cleanup and delete `__manifest`-pinned versions. Pinned by `lance_surface_guards.rs::skip_auto_cleanup_suppresses_version_gc` (negative control + with-skip survival).
- **Lance #6658 SHIPPED in 7.0.0** (`DeleteBuilder::execute_uncommitted`, exposed via PR #6781) → MR-A (migrate `delete` to the staged two-phase API) **has since landed** (dev-graph `iss-950`): `delete_where` is retired, deletes stage via `TableStorage::stage_delete`, and the guard was flipped to `_compile_uncommitted_delete_field_shape` (pins `execute_uncommitted` / `UncommittedDelete`). `StagedWrite` must carry `UncommittedDelete.affected_rows` through `commit_staged` so Lance's row-level rebase metadata is preserved. The parse-time D2 rule is retained as a deliberate boundary (constructive XOR destructive per query), not as scaffolding awaiting further work.
- **The unenforced primary key is now immutable once set** (`lance::dataset::transaction`, ~L2472–2480: `if !primary_key_before.is_empty() && (writes_primary_key || primary_key_after != primary_key_before) → "the unenforced primary key is a reserved key and cannot be changed once set"`). omnigraph marks `__manifest.object_id` as the unenforced PK (`lance-schema:unenforced-primary-key`) for merge-insert row-level CAS — baked into the manifest schema at init (`db/manifest/state.rs`). With the strand model there is no in-place migration, so the PK is only ever set at init: a graph that predates the annotation is refused on open (`refuse_if_stamp_unsupported`) and rebuilt via export/import, never re-keyed — which is also what Lance's immutability rule would require, since the wrong PK could not be changed once set. Pinned by `lance_surface_guards.rs::unenforced_primary_key_is_immutable_once_set` (red if Lance relaxes immutability).
- **Native `DirectoryNamespace` no longer recognizes omnigraph's manifest-tracked tables** (`lance-namespace-impls` dir.rs ~L1310): `list/describe/create_table_version` route through `check_table_status`, which reports an omnigraph table absent → `TableNotFound`. The decoupling is *contingent on omnigraph's legacy boolean PK key*, not an unconditional v7 property: v7's namespace eagerly adds the new `lance-schema:unenforced-primary-key:position` key to any `__manifest` lacking it; that write hits the immutable-PK rule above (the boolean key already set the PK), so `ensure_manifest_table_up_to_date` errors and the namespace silently falls back to directory listing. omnigraph keeps the boolean key deliberately — Lance honors it permanently (maps to PK position 0), and one uniform on-disk format beats a new-vs-old split (existing graphs can't be re-keyed to the position key under that same immutability rule). omnigraph production never uses Lance's native namespace (its publisher writes `__manifest` directly via merge_insert; its own `namespace.rs` impls are custom), so this was test-only. This paragraph records the v7 behavior; beta.19 #7687 later made manifest opening non-mutating and restored native physical-version reads, as documented in the beta.21 audit above.
- **Still NOT fixed in 7.0.0:** vector-index two-phase (Lance #6666 open) — `create_vector_index` inline residual retained; blob-column compaction — `compact_files_still_fails_on_blob_columns` guard still red on a fix, `optimize` still skips blob tables behind `LANCE_SUPPORTS_BLOB_COMPACTION`.
- **No Lance API surface omnigraph uses changed at *compile* time** (the only compile break was object_store) — but **two runtime behaviors did** (the unenforced-PK immutability and the native-namespace `TableNotFound`, above), each caught by the full engine test suite rather than the build. `CleanupPolicy`, `WriteParams` (apart from the `auto_cleanup` default), `CompactionOptions`, the namespace models (resolved via `lance-namespace-reqwest-client` 0.7.7, unchanged across the bump), `Operation`, `ManifestLocation`, and `MergeInsertBuilder` shapes are all stable. Lesson: a clean build is not a clean alignment — run `cargo test --workspace` before declaring a Lance bump done.
- **The v3→v4 migration-robustness surface guards were removed with the strand.**
  An earlier cycle added `dataset_open_missing_returns_not_found_variant` and
  `lance_error_incompatible_transaction_variant_exists` to pin Lance error surfaces
  the `migrate_v3_to_v4` backfill classified on. The strand retirement deleted that
  migration (storage is now strict-single-version — see [invariants.md](invariants.md)),
  so those guards and the legacy-read/stamp-bump code they pinned are gone. No
  current omnigraph code path classifies on those Lance variants.

Bump this date stanza on the next alignment pass.

### Prior alignment audit: 2026-05-22 (Lance 6.0.1 upstream; omnigraph pinned at 6.0.1)

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
- **`Dataset::optimize_indices(&mut self, &lance_index::optimize::OptimizeOptions)`** (via `DatasetIndexExt`) is a depended-on surface as of the index-coverage work: `db/omnigraph/optimize.rs` calls it after `compact_files` to fold appended/rewritten fragments into existing indexes (incremental merge, not retrain). It is a **committing** call (mutates in place, advances HEAD; no uncommitted variant in v6.0.1), so optimize treats it as an inline-commit residual under the `SidecarKind::Optimize` recovery sidecar. Signature pinned by `lance_surface_guards.rs::_compile_optimize_indices_signature`; the incremental-coverage behavior pinned by `optimize_indices_extends_fragment_coverage` (appended fragment uncovered before, covered after).
- **`Dataset::delete(predicate)` returns `DeleteResult { new_dataset: Arc<Dataset>, num_deleted_rows: u64 }`** — unchanged shape. Pinned by `lance_surface_guards.rs::_compile_delete_result_field_shape`. MR-A will repurpose this guard to the staged two-phase variant once `DeleteBuilder::execute_uncommitted` migration lands.
- **File reader read methods now async** (Lance PR #6710, v6.0). No effect — omnigraph reaches Lance exclusively through `Dataset::scan` and the staged-write API.
- **Tokenizer vendored as `lance-tokenizer`** (Lance PR #6512, v6.0). No effect — no direct tokenizer imports.
- **Lance #6658 closed** (2026-05-14) but `DeleteBuilder::execute_uncommitted` did **not** ship in v6.0.1 — binary search across the release stream shows it first appears in `v7.0.0-beta.10` (the closing commits landed on main but didn't backport to the 6.x line). Tracked as MR-A: migrate `delete_where` to staged, retire the parse-time D2 mutation rule, extend recovery sidecar coverage. **Gated on the Lance v7.x bump**, not this PR. v7.0.0-rc.1 dropped 2026-05-21.
- **Lance #6666 still open** (`build_index_metadata_from_segments` public): vector-index two-phase blocked; inline `create_vector_index` residual retained.
- **Lance #6877 still open** (`MergeInsertBuilder` dup-rowid): PR #109's `SourceDedupeBehavior::FirstSeen` + `check_batch_unique_by_keys` precondition stay load-bearing.
- **`Dataset::force_delete_branch`** (`branches().delete(name, force=true)`) tolerates both a missing branch-*contents* ref and, since beta.18, a fully absent `tree/{branch}/` directory. Plain `delete_branch` still returns `RefNotFound`, and both delete variants still refuse a branch with referencing descendants (`RefConflict`). OmniGraph retains its path-descendant precheck because Lance deliberately preserves an ancestor tree while a live path-child exists, and defensively normalizes `RefNotFound` / `NotFound` around the full force-delete call because the native branch-contents existence check and delete are not atomic.
- **Lance blob-v2 `compact_files` bug** (no public issue found as of 2026-06): `compact_files` disables binary-copy for blob datasets and forces `BlobHandling::AllBinary` on the read side; the v2.1+ structural decoder then mis-counts column infos for the blob-v2 struct and fails with `Invalid user input: there were more fields in the schema than provided column indices / infos` (`lance-encoding/src/decoder.rs::ColumnInfoIter::expect_next`). This fails even a pristine uniform-V2_2 multi-fragment blob table; vector/list/scalar/ragged columns and mixed file versions all compact fine. Reads/queries use descriptor handling (`BlobHandling::default()`) and are unaffected. `optimize` skips blob-bearing tables behind `LANCE_SUPPORTS_BLOB_COMPACTION = false` (`db/omnigraph/optimize.rs`), reporting `SkipReason::BlobColumnsUnsupportedByLance`. Pinned by `lance_surface_guards.rs::compact_files_still_fails_on_blob_columns`, which turns red when the bug is fixed → flip the gate, remove the skip branch + the `maintenance.rs::optimize_skips_blob_table_and_reports_skip` skip assertions.

Surface guards added: `crates/omnigraph/tests/lance_surface_guards.rs` (10 named guards; 5 runtime + 5 compile-only; plus the index-coverage work's `_compile_optimize_indices_signature` and `optimize_indices_extends_fragment_coverage`). Future Lance bumps re-run this file first as the smoke check. Two additional guards from the original plan deferred to follow-up (`manifest_cas_returns_row_level_contention_variant` needs full publisher-race harness; `table_version_metadata_byte_compatible_with_v4` needs `pub(crate)` reach extension).
