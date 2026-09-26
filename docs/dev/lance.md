# Lance documentation index

**Pinned dependency:** unmodified crates.io Lance 11.0.0, complete package
family. OmniGraph does not vendor or patch Lance.
**Purpose:** required upstream reading and current OmniGraph compatibility
fences

**Upgrade boundary:** existing full-text indexes require an explicit rebuild
before search. See [full-text compatibility](#full-text-compatibility).

Lance is OmniGraph's storage substrate. Before changing a Lance-shaped
behavior, read **every full page in the matching domain below, plus every page
that is even slightly relevant**. The domains overlap: transactions affect
indexes, compaction affects row IDs, and cleanup follows branch/tag references.

Fetch full pages, never summaries:

```bash
curl -sL <url> | pandoc -f html -t markdown
```

Summaries routinely omit default flags, visibility restrictions, nested specs,
and compatibility details. The index is a router, not a substitute for the
pages.

## Quick start

| Topic | URL |
|---|---|
| Lance overview | https://lance.org/quickstart/ |
| Vector search | https://lance.org/quickstart/vector-search/ |
| Full-text search | https://lance.org/quickstart/full-text-search/ |
| Versioning and time travel | https://lance.org/quickstart/versioning/ |
| Lance's agent guide | https://lance.org/format/AGENTS/ |

## Storage format and transactions

Read the complete section for manifest/table work, staged writes, recovery,
schema metadata, fragment lifecycle, or raw file assumptions.

| Topic | URL |
|---|---|
| Format overview | https://lance.org/format/ |
| File format | https://lance.org/format/file/ |
| File encoding | https://lance.org/format/file/encoding/ |
| File versioning | https://lance.org/format/file/versioning/ |
| Table layout | https://lance.org/format/table/layout/ |
| Table schema | https://lance.org/format/table/schema/ |
| Table versioning | https://lance.org/format/table/versioning/ |
| Transactions and conflicts | https://lance.org/format/table/transaction/ |
| Branch/tag format | https://lance.org/format/table/branch_tag/ |
| Row-ID lineage | https://lance.org/format/table/row_id_lineage/ |
| MemWAL format (upstream only; no OmniGraph consumer) | https://lance.org/format/table/mem_wal/ |

## Branches, tags, and cleanup

Read all four for graph branches, snapshots, merge ancestry, retention, or GC.

| Topic | URL |
|---|---|
| Branch/tag format | https://lance.org/format/table/branch_tag/ |
| Operational guide | https://lance.org/guide/tags_and_branches/ |
| Versioning quick start | https://lance.org/quickstart/versioning/ |
| Table versioning | https://lance.org/format/table/versioning/ |

Lance refs protect one dataset's files. OmniGraph's graph branch and manifest
remain the authority that coordinates the corresponding refs across datasets.

## Indexes and search

Read the overview plus the concrete index and lifecycle pages involved.

| Topic | URL |
|---|---|
| Index overview | https://lance.org/format/index/ |
| BTREE | https://lance.org/format/index/scalar/btree/ |
| Bitmap | https://lance.org/format/index/scalar/bitmap/ |
| Bloom filter | https://lance.org/format/index/scalar/bloom_filter/ |
| Label list | https://lance.org/format/index/scalar/label_list/ |
| Zone map | https://lance.org/format/index/scalar/zonemap/ |
| R-Tree | https://lance.org/format/index/scalar/rtree/ |
| Full-text search | https://lance.org/format/index/scalar/fts/ |
| N-gram | https://lance.org/format/index/scalar/ngram/ |
| Vector indexes | https://lance.org/format/index/vector/ |
| Fragment-reuse system index | https://lance.org/format/index/system/frag_reuse/ |
| MemWAL system index (no OmniGraph consumer) | https://lance.org/format/index/system/mem_wal/ |
| HNSW Rust example | https://lance.org/examples/rust/hnsw/ |
| Distributed indexing | https://lance.org/guide/distributed_indexing/ |
| FTS/n-gram tokenizer | https://lance.org/guide/tokenizer/ |
| Vector quick start | https://lance.org/quickstart/vector-search/ |
| FTS quick start | https://lance.org/quickstart/full-text-search/ |

## Reads, writes, and schema evolution

| Topic | URL |
|---|---|
| Read/write guide | https://lance.org/guide/read_and_write/ |
| Distributed write | https://lance.org/guide/distributed_write/ |
| Rust write/read example | https://lance.org/examples/rust/write_read_dataset/ |
| Data evolution | https://lance.org/guide/data_evolution/ |
| Migration | https://lance.org/guide/migration/ |

## Object stores and observability

Read both for local/S3/Azure behavior, retries, sessions, request accounting,
credentials, or storage fault tests.

| Topic | URL |
|---|---|
| Object stores | https://lance.org/guide/object_store/ |
| Observability | https://lance.org/guide/observability/ |

## Data types and Blob

| Topic | URL |
|---|---|
| Data types | https://lance.org/guide/data_types/ |
| Arrays/lists | https://lance.org/guide/arrays/ |
| Blob v2 | https://lance.org/guide/blob/ |
| JSON | https://lance.org/guide/json/ |

## Performance, compaction, and DataFusion

| Topic | URL |
|---|---|
| Performance and caches | https://lance.org/guide/performance/ |
| Read/write maintenance | https://lance.org/guide/read_and_write/ |
| Fragment reuse | https://lance.org/format/index/system/frag_reuse/ |
| Distributed indexing | https://lance.org/guide/distributed_indexing/ |
| DataFusion integration | https://lance.org/integrations/datafusion/ |

## Full-text compatibility

Lance 11's stemmer is not query-compatible with existing Lance 10 postings.
Every full-text segment used by a query must carry OmniGraph's artifact-scoped
analyzer certificate. The certificate is bound to the immutable UUID, index
details, and file inventory, not the current dataset writer or graph head.
Missing or unrecognized proof produces `FullTextIndexRebuildRequired` before
execution. Successful immutable proof uses Lance's bounded session metadata
cache; failures are not cached. Ordinary reads do not inspect these certificates.

The full builder writes proof before the existing CreateIndex publication;
public object-store/base-path resolution and native garbage collection own the
bounded JSON file. Directory placement mirrors Lance's private helper and is
guarded against independently written native index files. Stable-ID
compaction preserves proof. Ordinary optimize excludes incremental full-text
folding, which lacks an uncommitted provenance hook; uncovered rows remain
searchable until an explicit rebuild. Planning and execution share the same
foldable-index predicate; pending full-text coverage alone is not recovery work.
Other index maintenance is unchanged.

The audited analyzer dependency set has exact pins and a resolved-lockfile guard.
Any change needs a compatibility audit before keeping or changing the certificate
generation. See
[RFC 0043](../rfcs/0043-full-text-index-compatibility.md) for the decision and
[the operator procedure](../user/operations/upgrade.md#full-text-index-upgrade).

## Current compatibility fences

These are the current OmniGraph deltas over stock Lance 11. They are not a
history of dependency bumps.

| Surface | Current fence | Test owner |
|---|---|---|
| File format | Every production write explicitly selects stable V2_2; experimental V2_3 is not part of the graph contract. | `lifecycle.rs`, write-site source guards |
| Graph keys | Schema-v9 node/edge tables carry the exact non-null `__id` unenforced primary key (edges also `__src`/`__dst`); v8 tables keep the `id`/`src`/`dst` spellings introduced in v6, and the explicit system-column upgrade (RFC 0040) renames them with one rename-only `Operation::Project` commit per table (`TableStore::renamed_schema`), staged detached and published as a pin (RFC 0067, detached-only), which must keep every fragment, field id, the primary-key marker and index attachments. Strict insert/upsert uses the sealed filter-bearing adapter; raw keyed Append is forbidden. | `lance_surface_guards.rs`, staged-table tests, `forbidden_apis.rs`, `system_column_upgrade.rs` |
| Stable row IDs | Graph tables use stable row IDs; delete/update/index maintenance must retain their mapping. Overwrite allocates fresh IDs and restore retains the allocation high-water marks. Staged-view IDs are provisional, not committed identity. | `lance_surface_guards.rs`, staged-table tests, `writes.rs` |
| KNN result order | A late payload-hydration plan can lose global ordering metadata, so nearest requests one final output partition. Internal reads remain parallel. | `lance_surface_guards.rs`, `search.rs` |
| KNN probe budget | A `nearest` scan sets `maximum_nprobes` per index delta and widens it from Lance's execution summary (`partitions_searched` / `partitions_ranked`, read through `scan_stats_callback`); a prefilter admitting fewer rows than `k` makes Lance emit the unreached rows at `_distance = +inf`, which the engine resolves with a flat exact rescan (`use_index(false)`); `count_rows(None)` is the live row count (deletions excluded), read for the ladder's exhaustion stop and as the overfetch loop's exact-pass `k`. A renamed counter or marker turns the ladder fail-closed. | `lance_surface_guards.rs`, `search.rs` |
| Full-text analyzer | Every selected FTS segment needs artifact-scoped compatibility proof. Explicit branch rebuilds replace all segments from rows; old snapshots may refuse FTS. | staged-table tests, `search.rs`, `maintenance.rs` |
| Blob v2 | Null, valid empty, non-empty, selector cardinality, neighboring bytes, and 3→1 compaction are pinned on Lance 11. | `lance_surface_guards.rs`, `maintenance.rs` |
| Index coverage | Indexes are derived. Rewrites and compaction may leave an uncovered tail; reads must combine indexed and scan paths until explicit reconciliation. | `scalar_indexes.rs`, `search.rs`, `maintenance.rs` |
| Inherited index files | An engine `CommitHandler` adapter repairs stock Lance's nested-clone index origins within the existing clone commit. Exact source metadata preserves inherited `Some(base_id)` values and assigns the immediate-source base only to source-local indexes. External fragment-reuse details are read from their original base and converted to validated inline details. Invalid details fail closed before native creation. | `storage_layer::lance_clone` tests; `lance_surface_guards.rs::stock_lance_nested_clone_overwrites_inherited_index_base_issue_7840` |
| Branches/tags | Each graph-branch lifetime is `{logical}.{ULID}`; bare names identify legacy lifetimes. Unretired native `__manifest/_refs/branches/` entries define logical branches. Deletion writes identity-bound retirement metadata, archives the exact `BranchContents` inside its native tree, then unlinks the active ref. Needed native ancestry and historical merge-base providers remain readable through the archive. User `__manifest` tags block logical deletion; valid engine merge-input tags instead protect their exact snapshots until the target publication witness expires. | `branching.rs`, `branch_control.rs` tests, `merge_cleanup.rs` |
| Branch ref reads | Stock Lance 11 reads a ref as `head` then `get_range(0..size)`, so a `replace_metadata` landing between the two calls serves a prefix of the rewritten object and the read fails to parse instead of returning either version. `branch_control::read_branch_refs` rereads a torn read (`Arrow`, `CorruptFile`) or a raced delete (`NotFound`), at most three reads in all, around the engine's listed and exact authority reads (`list_branches`, `branches().get`, `get_identifier`). Resolving a logical branch name (`branch_control::resolve_live_native_branch`) lists `_refs/branches/` once and reads only that branch's own incarnations, so another branch's retirement rewrite, or its malformed retirement record, no longer fails a branch open or a held-ref re-resolve; fresh creation narrows content reads to its source, target-related names and schema sentinel. Operations that require a complete inventory, including logical enumeration and cleanup, still read every active ref; maintenance and historical resolution also read retirement archives. Delete the retry arm and un-ignore the guard at the bump whose `from_path` reads a ref in one `get`. | `branch_control.rs` stale-head tests (they go red at that bump), `lance_surface_guards.rs::branch_ref_read_survives_concurrent_metadata_rewrite` (run with `--ignored`) |
| Cleanup | Stock `cleanup_old_versions` is never called on a graph table: Lance 11 does not trace detached manifests. The engine collector discovers canonical registrations, including tombstoned table lifetimes. Roots include retained graph snapshots, selected merge bases, exact tagged graph snapshots and exact tagged table versions. It marks base and overlay data files, deletion files, index directories, transaction files and Blob sidecars, preserving borrowed physical origins. Native tree candidates are frozen before graph capture; after table inventories, unchanged branches, identities, heads and tag inventories must validate before deletion. Valid staging witnesses are judged against captured heads and a complete post-list live-identity cut; ambiguous ownership stays. Unneeded retired trees and their archives are collectible after dependency closure. Foreign linear versions remain untouched; orphan objects require their own seven-day age. A failed trace keeps its table, and incomplete origin tracing prevents physical-file deletion across the run. | `maintenance.rs`, `collector.rs` tests, the DST collector oracle, `merge_cleanup.rs`, `branching.rs` |
| Table forks | No table fork is created for a graph branch: a branch write stages detached on the dataset and native ref its inherited registration names. Lance's `create_branch` accepts a detached id as the parent but leaves the branch headless (a linear commit on it is refused), which is why the engine forks nothing. Forks created before v11 (`fork.{incarnation}.m{base_version}.{commit_ULID}`, `legacy` in the incarnation position) stay valid: a registration naming one opens there, `table_fork_owner` records ownership, and `cleanup` may reclaim an unreferenced one after its parsed owner incarnation is gone and tag, ancestry and age protections allow it. Canonical ULIDs and version numbers are required for that name-based liveness check; generated forks with malformed or `legacy` owner components stay retained. Names never establish table ownership. | `lance_surface_guards.rs`, `branching.rs`, metadata tests |
| Detached commits | Every graph-table write is a Lance detached commit (`_versions/d{version}.manifest`), which never moves HEAD and needs no conflict pass, and it is the table's version for its whole life: nothing replays it linearly, no `Restore` is used, a graph table's linear HEAD stays at its `v1` create, and the registration's `omnigraph.last_linear_version` records where the linear history stopped. Every manifest names its transaction file `{read_version}-{uuid}.txn`, which is how the engine matches a pin's `transaction_uuid` and follows a chain of detached commits by `read_version` links (the merge proofs, the collector) without a second request. A detached commit never triggers Lance auto-cleanup: `auto_cleanup_hook` runs only from `commit_transaction`, and every engine commit passes `skip_auto_cleanup: true`. | `lance_surface_guards.rs` (`rfc_0067_*`), `collector.rs` tests |
| MemWAL | Upstream support exists, but OmniGraph's RFC 0018 and RFC 0026 experiments were removed. No stream profile, token ledger, hidden stream column, or `_mem_wal` path is current. | `lifecycle.rs`, cluster removed-field diagnostics |

The clone adapter is installed during normal dataset opens and initial dataset
creation, preserving configured commit handlers without an additional reopen.
Exact, version-pinned source context scopes native branch creation. The adapter
validates the clone target, source bases and index metadata, then delegates the
existing manifest writer and transaction to the original handler. Ordinary
commits pass through without capturing index metadata; indexed fork preparation
reads the source's immutable index section, while an absent section skips that
read. The adapter adds no separate index-object write or manifest publication.

External fragment-reuse details are read through the index's original
`base_id`, using checked offset/size arithmetic and the actual file length.
Public typed decoding validates the payload before embedding it in the same
clone manifest. Missing, malformed or out-of-bounds details refuse native
creation. Reads, memory and the resulting inline manifest bytes scale with
that payload; this is not constant-cost metadata work. Current stable-row-ID
compaction does not produce external fragment-reuse details, but the adapter
preserves this stock Lance representation. The existing adapter tests cover
successive mixed-base clones, cold full-text/vector queries, and local and
inherited external fragment-reuse origins.

Schema v8 defines native-ref retirement metadata; schema v9 adds the
system-column namespace (RFC 0040); schema v10 lets a registration name a
detached table commit (RFC 0067); schema v11 makes that commit the table's
version for life and records `omnigraph.last_linear_version` on every
registration (RFC: Detached-only tables). Normal open serves v11 only;
qualified v6/v7/v8/v9/v10 graphs have explicit offline routes to v11, and the
system-column respelling is a separate step on a served graph. The v7 → v8 handler
changes only manifest configuration metadata and does not infer fork ownership
or retire branches. Source v6/v7 graphs with reserved retirement metadata refuse;
v8 no-op admission validates markers and counts only live logical refs
while retaining physical ancestors. Older binaries must not expose retired refs
as live branches.
See [versioning](versioning.md).

Stock `Branches::get` and `list` include every physical ref. OmniGraph's logical
manifest helpers validate and filter `omnigraph.retired_manifest_branch`, a
version-1 JSON value binding the exact native name and identifier. An absent
marker means live; malformed, unsupported, or mismatched metadata fails closed.
The metadata update publishes retirement and preserves unrelated metadata.
Before unlinking the active ref, retirement archives the exact `BranchContents`
inside its native tree; retries validate the same identity in the ref or archive.
Native branch controls retain OmniGraph's existing process-local serialization.
Cold logical enumeration reads active refs; cached named-write admission checks
the existing ref lookup without an added request. Cleanup and historical
resolution read archives. Cleanup retains required native ancestors and exact
merge-base providers, then reclaims unneeded trees with their archives. Delayed
staging is judged from the complete post-list identity cut and final capture
validation, so its retirement history need not remain forever.

`--keep N` retains the newest N `__manifest` versions per live branch, which
is N graph commits, and every table version they pin; it never counts Lance
versions of a dataset. `--older-than` retains every `__manifest` version
newer than the cutoff and also gates pre-v11 fork collection using tree and
native ref object timestamps before closing over dependencies. Any recent
object retains such a fork, including a fresh retirement metadata update over
an old tree. Ref metadata listings for this age check occur only during
cleanup with an explicit age policy.

## Dependency bump checklist

1. Fetch every full page in every affected domain.
2. Inspect the complete upstream release/source and dependency delta. Re-audit
   the public API assumptions behind every engine compatibility adapter;
   preserve unmodified crates.io dependencies.
3. Run `lance_surface_guards` first; a red guard is a required design review,
   not a test to weaken.
4. Run focused write, merge, search, maintenance, Blob, branch, and recovery
   owners as applicable.
5. Run local cost guards and configured RustFS/Azure tests for the affected
   backend.
6. Run the canonical workspace test and both Clippy graphs from
   [testing.md](testing.md).
7. Update the pinned version and only the active compatibility table above.
8. Record bump evidence in the release note or owning RFC. Git history is the
   archive; do not append a permanent audit diary to this live guide.

Namespace REST models and Spark/Trino/Databricks/Python integrations are
deliberately absent because OmniGraph does not expose those surfaces. Add a
domain only when code makes it reachable.
