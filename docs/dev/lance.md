# Lance alignment

**Pinned dependency:** unmodified crates.io Lance 13.0.0, complete package
family. Omnigraph does not vendor or patch Lance.
**Purpose:** required upstream reading and the route to Omnigraph's
compatibility fences

Lance is Omnigraph's storage substrate. Before changing a Lance-shaped
behavior:

1. Read **every full page in the matching domain below, plus every page that
   is even slightly relevant**. The domains overlap: transactions affect
   indexes, compaction affects row IDs, and cleanup follows branch/tag
   references.
2. Read the matching entries in [Lance compatibility fences](lance-fences.md):
   every Lance behavior Omnigraph depends on, works around or forbids, with
   the tests that pin it.
3. Check each assumption about Lance internals in the upstream source at the
   pinned version.

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

## Branches, tags, and cleanup

Read all four for graph branches, snapshots, merge ancestry, retention, or GC.

| Topic | URL |
|---|---|
| Branch/tag format | https://lance.org/format/table/branch_tag/ |
| Operational guide | https://lance.org/guide/tags_and_branches/ |
| Versioning quick start | https://lance.org/quickstart/versioning/ |
| Table versioning | https://lance.org/format/table/versioning/ |

Lance refs protect one dataset's files. Omnigraph's graph branch and manifest
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

Omnigraph's deltas over stock Lance 13 are in
[Lance compatibility fences](lance-fences.md), one section per surface with
its test owners. The rules an unrelated change breaks most easily:

- Every production write selects the stable V2_2 file format. Experimental
  V2_3, spilled row lineage and stable-row-ID fragment-reuse indexes are not
  graph capabilities ([file format](lance-fences.md#file-format)).
- Every graph-table write is a detached commit and stays the table's version
  for life; nothing replays it linearly
  ([detached commits](lance-fences.md#detached-commits)).
- Stock `cleanup_old_versions` is never called: it does not trace detached
  manifests, so it can delete files a retained snapshot needs
  ([cleanup](lance-fences.md#cleanup)).
- Every `__manifest` publish is a strict overwrite with zero retries, the
  version CAS ([catalog publication](lance-fences.md#catalog-publication)).
- Strict insert and upsert go through the sealed filter-bearing adapter; raw
  keyed Append is forbidden ([graph keys](lance-fences.md#graph-keys)).
- Indexes are derived, so reads combine indexed and scan paths
  ([index coverage](lance-fences.md#index-coverage)), and a lagging index is
  rebuilt whole, never extended with a delta segment
  ([index segments](lance-fences.md#index-segments)).
- No query reads a full-text index; the exact lexical scan answers every
  lexical call. A segment qualifies only through a certificate whose measured
  analyzer fingerprint this binary reproduces, so a Lance or analyzer
  dependency change shows as a refusal, never as different terms
  ([full-text compatibility](lance-fences.md#full-text-compatibility)).
- A red `lance_surface_guards` test is a required design review, not a test
  to weaken.

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
6. Run the canonical workspace test, GQT and both Clippy graphs, the genuine
   predecessor journey, and the release-profile guards described in
   [test ownership](test-ownership.md#lance-compatibility).
7. Update the pinned version and only the affected entries in
   [Lance compatibility fences](lance-fences.md).
8. Record bump evidence in the release note or owning RFC. Git history is the
   archive; do not append a permanent audit diary to this live guide.

Namespace REST models and Spark/Trino/Databricks/Python integrations are
deliberately absent because Omnigraph does not expose those surfaces. Add a
domain only when code makes it reachable.
