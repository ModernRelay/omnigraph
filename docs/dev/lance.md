# Lance documentation index

**Pinned dependency:** Lance 10.0.0, complete package family
**Purpose:** required upstream reading and current OmniGraph compatibility
fences

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

## Current compatibility fences

These are the current OmniGraph deltas over stock Lance 10. They are not a
history of dependency bumps.

| Surface | Current fence | Test owner |
|---|---|---|
| File format | Every production write explicitly selects stable V2_2; experimental V2_3 is not part of the graph contract. | `lifecycle.rs`, write-site source guards |
| Graph keys | Every v6 node/edge table has exact non-null `id` as its unenforced primary key. Strict insert/upsert uses the sealed filter-bearing adapter; raw keyed Append is forbidden. | `lance_surface_guards.rs`, staged-table tests, `forbidden_apis.rs` |
| Stable row IDs | Graph tables use stable row IDs; delete/update/index maintenance must retain their mapping. | `lance_surface_guards.rs`, `writes.rs` |
| KNN result order | A late payload-hydration plan can lose global ordering metadata, so nearest requests one final output partition. Internal reads remain parallel. | `lance_surface_guards.rs`, `search.rs` |
| Blob v2 | Null, valid empty, non-empty, selector cardinality, neighboring bytes, and 3→1 compaction are pinned on Lance 10. | `lance_surface_guards.rs`, `maintenance.rs` |
| Index coverage | Indexes are derived. Rewrites and compaction may leave an uncovered tail; reads must combine indexed and scan paths until explicit reconciliation. | `scalar_indexes.rs`, `search.rs`, `maintenance.rs` |
| Branches/tags | Native refs are per dataset. OmniGraph validates ref incarnation and coordinates graph-level authority through `__manifest`. | `branching.rs`, `lance_surface_guards.rs` |
| Cleanup | Lance protects native refs/tags; OmniGraph additionally computes graph-wide lazy-branch and recovery floors before invoking cleanup. | `maintenance.rs` |
| MemWAL | Upstream support exists, but OmniGraph's RFC 0018 and RFC 0026 experiments were removed. No stream profile, token ledger, hidden stream column, or `_mem_wal` path is current. | `lifecycle.rs`, cluster removed-field diagnostics |

## Dependency bump checklist

1. Fetch every full page in every affected domain.
2. Inspect the complete upstream tag/source and dependency delta.
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
