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

### Last alignment audit: 2026-07-17 (Lance 9.0.0-rc.1; git rev `cec0b7df`)

The pin advanced from `v9.0.0-beta.21` (`1aec1465`) to
`v9.0.0-rc.1` (`cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d`) after reviewing
all 40 intervening commits, the cumulative RC release notes, and the complete
matching format/guide pages listed above. Every Lance workspace dependency,
including the engine's `lance-io` test dependency, uses the same exact rev.
DataFusion advances 53→54; Arrow remains 58 and `object_store` remains 0.13.2.
The lockfile adds Lance's new transitive `lance-index-core` crate. OmniGraph
still writes explicit stable V2_2 files, so this dependency bump does not
activate a new storage format or require a general OmniGraph format migration
for schemas that avoid the newly reserved names documented below. RC.1 requires
Rust 1.91 or newer; OmniGraph continues to track stable Rust and this audit ran
on Rust 1.95.

#### Post-audit implementation note: 2026-07-18

RFC-026 Gate E0 subsequently passed and Phase A activated OmniGraph internal
schema v7. V7 adds only the bounded streaming foundation: exact recovery-v10
enrollment, identity-keyed lifecycle authority, process-local admission and
writer exclusion, current-HEAD witness validation, partial-format refusal, and
strict v6↔v7 rebuild. The private adapter can establish one empty unsharded
epoch-1 enrollment on main; no production caller can put or acknowledge a row,
fold a generation, or operate drain/resume.

The Phase-B1 source audit also narrows the RC.1 row contract. `put_no_wait`
returns a `WriteResult` plus an optional `BatchDurableWatcher`; the watcher's
successful `Result<()>` is the only per-put durable-completion signal.
`WriteResult.batch_positions` are active-MemTable/`BatchStore` positions that
restart after `freeze_memtable`, while the durability watermark is shared for
the writer lifetime. A generation-`N + 1` watcher can therefore resolve from
generation `N`'s old watermark before `N + 1` reaches the WAL. In addition,
`put_no_wait` inserts into the MemTable before a later flush-scheduling error,
so its `Err` is not generally an effect-free result. Finally,
`wal_stats.next_wal_entry_position` is mutable status, so neither is a durable
row address or public receipt. Durable mode returning no watcher is treated as
post-invocation `AckUnknown`, not effect-free configuration rejection.

RC.1 replay has a second independent bookkeeping hazard. It inserts durable
WAL batches into a fresh `BatchStore` and rebuilds indexes, but does not advance
that store's per-MemTable WAL-flush watermark. A later put or plain seal starts
its WAL range at batch zero, re-appending and re-indexing the replayed prefix.
Repeated crashes after that duplicate WAL PUT but before the shard-manifest
commit can multiply the replay tail until BatchStore capacity is exhausted.
The public `BatchStore::set_max_flushed_batch_position` surface is therefore a
pinned compatibility bridge: under the exclusive fold lease, B1 verifies that
the active contiguous prefix came only from authoritative replay, marks it
already WAL-durable, and then seals without another WAL/index write. Non-empty
replay is fold-only; it never accepts a new put.

Two flush details also constrain the proof. `wait_for_flush_drain` can return
`Ok(())` after a fast failed handler has removed its watcher, so B1 separately
requires empty frozen refs and the exact generation/cursor in the latest shard
manifest. And RC.1 writes the randomized generation dataset and sidecars before
that manifest CAS, so a crash may leave a complete or partial unreferenced
`{hash}_gen_N/` subtree. B1 treats only that recognized subtree shape as
retained derived orphan output—never adopted, scanned, or deleted—and leaves
reclamation to B2's proved GC contract. Seal/drain/abort are background-owned
with deadline-bounded caller waits: an error or stalled handler keeps the
original task and abort completion retained, the registry retired, and
admission closed. The caller never cancels `shutdown_all`, retries abort, or
claims quiescence while the taken handler join may still be active.

The safe B1 profile is deliberately bounded: one root-scoped serialized
worker owns one generation whose complete input is capped at 8,192 rows/32 MiB;
its explicit writer configuration prevents automatic rollover; it owns
`put_no_wait` and the watcher, seals/drains, retires without writing into the
replacement MemTable, folds one keyed transaction through recovery schema v11,
then reopens at a higher epoch. Cold claim/replay and warm final-check/put hold
the shared admission lease from before epoch claim until durability or
quiesced retirement, preventing a claimant from crossing a drain's captured
floor. Replay preserves possible residue but cannot resolve which caller
attempt produced it. Configuration identity binds only
correctness/topology/no-rollover fields; explicit runtime policy and injected
Session/store capabilities remain separate. Planned B1 activates graph schema
v8 and stream-config v2 rather than adopting v7/config-v1 in place. Phase B2 is
the later public strict activation and additionally waits for durable
contributor attribution, bounded reclamation, strict correction, a same-key
`AckUnknown` sequencing/idempotency contract, and persistent
quiesce/resume/abort-drain/rebuild. The missing combined enrollment receipt and
cross-process admission seal still gate broader topology, not B1 inside the
existing single-live-writer-process boundary.

The no-roll profile uses fixed portable capacities (`8,193` rows/batches and
1-GiB byte/unflushed thresholds), not architecture-dependent `usize::MAX`
metadata. After reopen, the 32-MiB contract is validated from public
`in_memory_memtable_refs().active.batch_store`: sum physical rows and exact
stored post-tombstone Arrow batch memory, including replayed duplicate batches.
An empty valid reopen may admit; a non-empty valid replay is routed fold-only.
`MemTableStats::estimated_size` has different accounting and does not replace
that contract. Retirement first stops the serialized worker and then uses
public `ShardWriter::abort`; `close` ignores final completion errors and is not
durability evidence.

Behavior-affecting findings in this audit:

- **The RFC-022 write/recovery architecture remains aligned.** Native
  branch/tag/cleanup behavior, shared `Session` construction, merge-insert
  filter emission, the directional filtered/unfiltered conflict resolver, and
  full-table `CreateIndexBuilder::execute_uncommitted` retain the shapes
  OmniGraph consumes. All 23 Lance surface guards pass on RC.1, including the
  exact tag/branch/ABA, blob-compaction, key-filter, shared-session, and vector
  staging cells. Search, writes, schema apply, branching, maintenance,
  row-version, ordinary recovery, and all 129 runnable failpoint tests also
  pass. A graph initialized and populated by the cached beta.21 CLI was opened,
  queried, merge-written, and queried again by the RC.1 CLI, proving the
  supported ordinary-schema V2_2 forward-open/write path rather than relying
  only on fresh RC fixtures. This proof intentionally does not cover the two
  newly reserved row-version names; their explicit upgrade path is documented
  below.
- **Data Overlay is not an OmniGraph primitive.** RC.1 adds the experimental,
  opt-in `Operation::DataOverlay` and table feature flag 64. OmniGraph neither
  enables that flag nor emits the operation; its recovery classifiers keep
  unknown foreign effects on the fail-closed path. The explicit V2_2 pin does
  not silently opt a dataset into overlays.
- **At RC.1 audit time, MemWAL's direction improved and RFC-026 owned a bounded
  decision gate.**
  Derived MemWAL datasets now inherit the base dataset's store parameters and
  `Session`, which is compatible with OmniGraph's shared-session design and
  remote credentials. The public initializer still commits internally and
  shard claiming remains a separate effect: there is still no caller-owned
  combined enrollment receipt plus reversible cross-process shard-admission
  seal. Source inspection confirms that `execute()` builds one
  `Operation::CreateIndex` from the current manifest, commits it through
  `CommitBuilder`, mutates the handle, and returns `Result<()>`; the public
  builder persists arbitrary namespaced writer defaults, and the public writer
  path later accepts a caller-selected shard UUID and claims an observable
  epoch. Gate E0 uses a pre-minted enrollment/config-version default as
  read-back evidence independent of the replaceable index UUID. The public
  branch/version/current-transaction/e_tag tuple
  is a mutable current-HEAD witness—ordinary commits change it—not a stable
  enrollment incarnation.

  RC.1 persists writer defaults in the MemWAL index but does not apply them to
  a caller-supplied `ShardWriterConfig`; any bounded adapter must reconstruct
  the exact durable-write and buffer configuration rather than infer compatible
  defaults.

  MemWAL stays the strategic substrate and RFC-026 stays draft. Its
  production-neutral Gate E0 passed for the bounded profile. The post-audit
  note above records the subsequently implemented Phase A boundary. The first
  history-cost result was rejected: local
  `checkout_latest` may discover the tip through filesystem `read_dir`, which
  bypasses `IOTracker`, so the observed tracked GET count omitted part of the
  lookup. RC.1's public but guide-hidden `Dataset::has_successor_version`
  provides the accepted replacement: from a freshly ABA-verified exact `N`, it
  tests only `N + 1` through `CommitHandler::version_exists`; the exact `N + 1`
  handle can then reject a buried `N + 2` without latest resolution or listing.
  `AttemptTracker` records before forwarding—including failed/`NotFound`
  HEADs—and observes the identical complete shape at baseline versions 8 and
  80: four successful manifest HEADs, one `NotFound` manifest HEAD, one
  successful manifest GET, and zero lists. A Unix permissions tripwire proves
  the exact probe works while latest enumeration fails and makes an unreadable
  exact HEAD error.

  Fourteen substantive local cells cover exact initializer readback,
  lost-result reopening, the pre-minted empty shard, buried-effect refusal, and
  broad fail-closed classification. The configured RustFS exact cell passes
  non-vacuously with the same six-attempt/zero-list shape and covers the
  positive sequence plus foreign shard, malformed/loose root, durable WAL,
  persisted cursor, and corrupt-manifest negatives. Surface guards pin
  `has_successor_version`, flush/drain, merged-generation state, and S3 ABA; CI
  rejects skipped E0/ABA cells. Exclusive HEAD and cleanup/version-GC exclusion
  remain load-bearing; only `Ok(false)` means absence, while errors, overflow,
  or detached boundaries fail closed. The RC.1 audit itself introduced no
  private API, raw object emulation, production schema, or stream
  acknowledgement; the post-audit note records the later Phase A format work.
  The exact
  upstream receipt/seal remains the preferred simplification and the gate for
  broader topology.
- **Maintenance and index defaults preserve current behavior.** Compaction
  avoids a useless fragment-reuse-index optimization in a deferred-remap case;
  OmniGraph uses `defer_index_remap=false`, and Lance still exposes no stable
  caller-minted maintenance transaction. FTS posting blocks are now
  configurable, but the default and legacy fallback remain 128; OmniGraph does
  not opt into the experimental 256/v3 layout. The one-segment full-table
  vector staging API remains public with the same relevant shape.
- **Lance virtual system-column names now fail early in OmniGraph.** RC.1
  rejects writes whose physical schema declares `_rowid`, `_rowaddr`,
  `_rowoffset`, `_row_created_at_version`, or
  `_row_last_updated_at_version`. The schema parser and accepted-SchemaIR
  validator now reserve those five exact, case-sensitive property names before
  init or SchemaApply can create durable state or arm recovery. A surface guard
  pins the compiler-owned list to the five surveyed Lance public constants;
  every Lance bump still audits upstream source for additions. At pin-audit
  time internal schema v6 had not been released, so this was a validation
  tightening rather than a format bump; Phase A's later v7 activation does not
  change that conclusion. A development graph already using either newly
  reserved row-version name must be exported with the beta.21 binary, renamed, and
  rebuilt before opening on RC.1. The audit minted a genuine beta.21 v6 graph
  with `_row_created_at_version`: beta.21 opened it successfully, while RC.1
  refused it before any write with that exact recovery instruction.
- **The research-blocked cost conclusions survive, with RC-specific numbers.**
  RFC-025's local 10→1,000 compacted reconciled list/cleanup scan bytes are now
  17,012→38,000 cold and 12,336→15,064 warm; exact show is 29,348→53,064
  and 24,672→30,128. Cold scan operations still cross 24→25 (show 34→35).
  These are modest improvements over beta.21, not a change in asymptotic
  disposition: the proposed in-manifest checkpoint registry remains rejected.
  RFC-024's ignored 10/100/1,000 instrument likewise keeps exact heads and
  indexed row/range work flat, but RC.1 adds a bounded one-operation boundary
  at the 1,000-commit endpoint on top of the already-rejected byte curves. The
  local default and decision-scale instruments pass with these current no-go
  facts asserted; S3/RustFS remains bucket-gated and was not available for this
  audit.

### Prior alignment audit: 2026-07-12 (Lance 9.0.0-beta.21 upstream; omnigraph pinned at 9.0.0-beta.21 via git rev)

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

- **Session sharing has two separable layers.** Lance `Session::new` accepts an
  `Arc<ObjectStoreRegistry>` independently of its metadata/index cache
  capacities. OmniGraph therefore shares one process-wide registry to reuse
  graph-dataset object-store clients, but does not use one process-global cached
  Session. Data tables use a graph-handle-scoped cached Session; `__manifest`
  and other mutable-tip control datasets use a zero-cache control Session.
  Coordinator open/full-refresh reads decode manifest state and lineage from
  one row scan.
  This access shape reduces duplicate opens and cold client construction while
  avoiding a mutable-tip cache authority. It does not make the append-only
  manifest fold history-flat.
- **The RFC-022 full-table vector-index stage is no longer substrate-blocked.**
  `CreateIndexBuilder::execute_uncommitted` builds the physical vector artifact
  and returns complete `IndexMetadata`; Lance's own `execute` wraps that value
  directly in public `Operation::CreateIndex`. That source is byte-for-byte
  unchanged between beta.15 and beta.21 and has had this shape since upstream
  #7129. OmniGraph can therefore mirror the scalar staging path, pre-mint the
  transaction identity, and commit through `commit_staged_exact`. The exact
  EnsureIndices adapter now does so inside the RFC-028 identity-bearing v9
  envelope: all missing BTREE, FTS, and full-table
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
  maintenance payload inside the v9 identity envelope instead of claiming
  exact maintenance provenance from beta internals.
  Revisit exact Optimize provenance only after Lance exposes a stable public
  maintenance-transaction API and OmniGraph has distributed recovery fencing;
  the latter is independently required before destructive recovery is safe
  against a live foreign process.
- **RFC-024 Gate A found a usable current-HEAD witness with ABA sensitivity but
  rejected the proposed physical lookup shape.** The backend-portable candidate
  combines the public table version, `Dataset::branch_identifier()`, the
  current public `Dataset::read_transaction()` UUID, and
  `Dataset::manifest_location().e_tag`.
  Capture brackets transaction/e_tag collection with two branch-identifier
  reads and fails if the ref moves; a missing transaction or empty UUID also
  fails. Beta.21's local `current_manifest_path` synthesizes an
  inode-mtime-size e_tag, while S3/RustFS returns the object e_tag. Guards prove
  stability across unchanged reopens and distinguish main and named-ref
  delete/recreate at the same numeric version on local and S3/RustFS. An
  ordinary commit changes the witness; it is not a stable dataset/enrollment
  incarnation. The local guard reuses the
  original shared `Session`; the S3 guard also pins unchanged-incarnation
  reopen stability. Main's canonical empty `BranchIdentifier` makes the UUID
  and e_tag load-bearing; named refs additionally mint a new branch identifier.
  Raw byte-identical restoration or forged UUID/ref metadata is outside the
  supported OmniGraph writer topology: this composite fences ABA, but is not
  authentication. The proof closes the substrate-source question, not
  publisher-level stale-writer rejection, because no heads-format production
  path exists.

  The separate test-only `durable_head_lookup_cost.rs` fixture measures a
  structured exact `object_id IN (...)` scan over a BTREE at catalog width 10.
  Reconciled rows and ranges are flat at 10→10; result fragments are 10→10
  uncompacted and 1→1 compacted; cold/warm BTREE pages are 1→1 / 0→0. An
  absent-index negative control grows, and one/eight uncovered fragments are
  correct and observable. After `optimize_indices`, coverage returns to zero
  uncovered and the representative tail work returns from 27→10 in the
  beta.21 `rows_scanned` proxy
  and 17→10 ranges. Nevertheless, representative RustFS 20→80 curves grow:
  uncompacted cold object reads 34→94 and bytes 61,947→121,592; compacted cold
  object bytes 30,545→61,642 and plan bytes 39,085→61,894; compacted warm object
  bytes 22,934→45,413. Flat indexed scan work therefore does not make the full
  latest-manifest/object-byte cost flat, so RFC-024 is research-blocked and no
  heads format ships.

  The instrument keeps counter scopes separate: object reads/bytes come from an
  `IOTracker` installed before `DatasetBuilder::load`, while plan counters come
  from `ExecutionSummaryCounts`; they are not additive. The public summary
  fields are `iops`, `requests`, `bytes_read`, and `parts_loaded`.
  `fragments_scanned`, `ranges_scanned`, and `rows_scanned` are beta.21
  `all_counts` debug names explicitly subject to change, so every Lance bump
  must re-audit them rather than silently treating them as stable API.
- **RFC-025 Gate 0 validates Lance tags but rejects the current checkpoint-
  registry access shape.** The pinned public tag surface targets an exact main
  or named-branch version, creates/deletes auxiliary `_refs/tags/*.json`
  metadata without advancing the dataset version, and exempts the tagged
  version from cleanup. Deleting the tag makes the version reclaimable. A tag
  on a named branch does **not** retain `tree/<branch>` after branch deletion,
  so OmniGraph's proposed checkpoint-aware branch-delete refusal remains
  load-bearing. `Tags::create` is an existence check followed by an
  unconditional put on this revision, not a conditional create; RFC-025's
  retention claim plus exact post-create verification therefore cannot be
  replaced by the tag call itself. All 22 `lance_surface_guards` passed,
  including `native_tags_pin_exact_main_and_named_branch_versions_through_cleanup`
  and the extended `force_delete_branch_semantics` cell.

  The separate production-neutral `checkpoint_retention_cost.rs` fixture holds
  three checkpoints and catalog width ten constant while unrelated journal
  history grows. At the local 10→1,000 decision endpoints, reconciled
  uncompacted list stays at 3 rows / 3 ranges / 1 fragment / 1 page and 24 scan
  operations / 13,752 bytes; exact show stays at 12 / 2 / 2 / 3 and 34 /
  22,952; cleanup returns 44 rows with list-like cost. The eight-fragment tail
  is exact and history-flat. Compaction rejects the candidate: list/cleanup
  cold scan bytes grow 17,012→39,668 and warm bytes 12,336→16,736; exact-show
  cold bytes grow 29,348→56,404 and warm bytes 24,672→33,472. At 1,000 commits
  scan operations also cross 24→25 for the one-scan paths and 34→35 for show.
  The default local 20/80 matrix passes its no-go-preservation assertions. The
  bucket-gated S3/RustFS cost cell exists but was not run for this decision and
  is not claimed.

  This result blocks the in-manifest BTREE access shape, not checkpoint rows as
  logical authority or Lance tags as physical pins. RFC-025 is
  research-blocked and no retention format ships. Schema v6 was production
  truth when the gate ran; current schema v7 likewise carries no retention
  state. A successor needs a history-flat current-authority lookup
  or revised evidence-backed operational contract without adding a second
  authority dataset.
- **RFC-023 key-filter behavior remains route-dependent and directional, and
  v6 closes production routing around that fact with two distinct adapters.**
  A 2026-07-14 probe on this beta.21 pin shows that an explicitly selected v2
  MergeInsert plan (`use_index(false)`) over the exact unenforced PK emits
  `Some(KeyExistenceFilter)`: a fresh insert and fresh-key
  `WhenMatched::Fail` populate the Bloom filter, while a matched-only
  partial-schema UpdateAll + DoNothing emits a semantically empty filter rather
  than `None`. A mismatched ON set emits `None`; when all ON columns have scalar
  indexes and `use_index` remains enabled, Lance selects legacy v1, which also
  emits `None`. Conflict resolution is directional: filtered-current conflicts
  with committed unfiltered Update or Append, but current unfiltered Update or
  Append can rebase after a committed filtered Update. The surface guards pin
  both orders so a future symmetry or route change forces an alignment audit.

  Internal schema v6 creates every graph node/edge table with exact non-null
  physical `id` as Lance's unenforced PK. General production StrictInsert and
  Upsert use a sealed MergeInsert adapter that forces beta.21's v2 route and
  verifies the resulting filter covers exactly the physical `id` field.
  StrictInsert first performs one exact probe against its pinned parent; after
  a pure insertion effect is staged it mints the durable transaction property
  `omnigraph.insert_absence=v1`. Upsert never changes semantics to obtain the
  optimization, but an all-new result may mint the same property when Lance's
  completed `MergeStats` prove one attempt inserted all source rows with zero
  updates, deletes, or skipped duplicates. Certification is optional: an
  Upsert or historical transaction without it remains correct and simply
  cannot use the shortcut.
  The mint additionally proves that the filter encodes exactly every source
  `id` and that the fragments account for the same number of physical rows.

  BranchMerge admits the proven route only for a complete, contiguous
  base-to-source v1 chain. Every persisted transaction must name the exact
  parent and carry the full pure-insert `Operation::Update` shape: no removed or
  updated fragments, nonempty new fragments with `physical_rows`, no modified
  fields or merged generations, `RewriteRows`, no updated offsets, the exact
  `id` filter, and `fields_for_preserving_frag_bitmap` equal to the complete
  nested schema preorder. The chain's physical-row sum must equal the observed
  inserted delta. Final under-gate checks fence both source and target native
  branch identifiers, including same-version delete/recreate ABA. Missing,
  cleaned, unknown-version, malformed, or otherwise incomplete proof falls
  back to the general ordered row diff; it is never partially trusted.

  The proven publisher does **not** run MergeInsert. Its opaque,
  batch-owning `ProvenInsertChunk` binds the classifier's rows to one target
  version, complete schema, stable-row-id mode, and chunk index; a structural
  guard keeps its only production mint site in the complete-history classifier.
  `InsertBuilder::execute_uncommitted` stages immutable data fragments, after
  which OmniGraph replaces the temporary uncommitted Append operation with the
  exact-`id` filter-bearing `Update` above and re-mints v1. No target exact-ID
  preflight, target merge join, or Append transaction is committed. Because
  the output is itself a valid certificate link, the proof composes across a
  second merge generation. Generic data-table `stage_append` and
  `stage_merge_insert` remain test-only primitives.

  Both routes preserve the fixed resource/recovery boundary: Mutation/Load
  keeps one keyed transaction per table and refuses more than 8,192 rows / 32
  MiB before arm, while BranchMerge records row/byte-bounded chunks in an
  ordered recovery chain capped at 1,024 logical data transactions per table.
  The source-interval scanner deliberately does not enable beta.21
  `strict_batch_size`: Lance's `StrictBatchSizeStream` concatenates solely to a
  row count (which `LANCE_DEFAULT_BATCH_SIZE` may override) and ignores the byte
  target while accumulating. OmniGraph's lazy normalizer hard-bounds each
  emitted/writer chunk and retains only one raw emission plus bounded working
  pieces; Lance's approximate raw `batch_size_bytes` emission remains covered
  by the full-process RSS gate. The proven adapter avoids beta.21's full
  DataFusion merge join and its unbounded pool; the general adapter still uses
  that join for rows requiring target comparison. Exact recovery's separate
  1,026-version scan reserves one derived-index tail and one restore.
  `MergeInsertBuilder` exposes no `WriteParams` hook, so the general keyed
  adapter cannot enable `allow_external_blob_outside_bases`: it pre-sums
  external URI ranges/object sizes under the 32 MiB ceiling and materializes
  accepted payloads. Overwrite does accept `WriteParams` and retains external
  references. Because Lance's retryable class is broader than exact key
  overlap, an effect-free general StrictInsert conflict becomes `KeyConflict`
  only after a fresh manifest-visible probe finds an attempted ID. This is an
  intentional beta.21-compatible closure, not an assumption that the indexed
  path is filtered.

  The certificate is an internal, non-cryptographic capability. A raw Lance
  writer can omit or forge the transaction property and is outside OmniGraph's
  supported graph-writer topology; the certificate does not authenticate
  foreign history. The genuine v5↔v6 binary rebuild/refusal run passed on
  2026-07-15. The corrected five-pair full-lifecycle production series now also
  passes both fixed gates: at 10K × 256, the median operation ratio was
  **3.875×** and maximum paired RSS overhead was **24,297,472 bytes**; at
  100K × 256, the ratio was **136/35 ≈ 3.886×** and maximum overhead was
  **32,604,160 bytes**. Every production trial reported zero target strict-
  insert preflights, zero MergeInsert calls, zero ordered-diff scans, and exact
  content. The earlier 30.0×/108,625,920-byte production failure remains the
  evidence that motivated this certificate/InsertBuilder split. These
  measurements validate the present pin; a later Lance route or transaction-
  serialization change must rerun them.
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
  OmniGraph's access-shape follow-up performs one operation-local post-gate
  control capture instead of refreshing the handle-local coordinator around
  table-gate acquisition, then invalidates derived caches after a successful
  ref transition. This changes neither `BranchContents` authority nor the
  single-writer-process support boundary.
  The index-create source is unchanged; compaction changes are mechanical
  iterator cleanup; the transaction change only centralizes calculation of the
  next version. Branch checkout now reuses the session-cached manifest,
  improving the warm-access shape without changing branch identity or commit
  semantics.

The Lance surface guards, including the RFC-023 route and conflict-order probes,
plus the canonical workspace and failpoint suites are the compatibility gate
for this pin. The engine-level source guard, PK-lifecycle tests, concurrency
tests, and recovery failpoints separately prove how v6 consumes that substrate.
Keep the beta.15 audit below as historical provenance for the larger 7.0 → 9.0
migration. A substrate guard records current Lance truth; it does not replace
format, cross-version, or measured-cost evidence.

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
