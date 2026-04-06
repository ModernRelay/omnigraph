# Snapshots

## Current Model

Omnigraph snapshots are now defined by the namespace `__manifest` dataset, not by the older `_manifest.lance` table.

A snapshot is an immutable, point-in-time view of the whole graph:

- one `__manifest` version = one graph-visible version
- each table entry pins one Lance table version
- reads never bypass the snapshot

```text
Snapshot
  root_uri:  "s3://omnigraph-repo/context"
  version:   9                                  # __manifest dataset version
  entries:
    node:Person   -> nodes/a3f8c...  v4  branch=null     rows=120
    node:Company  -> nodes/7b21e...  v3  branch=null     rows=45
    edge:Knows    -> edges/c92d1...  v5  branch=feature  rows=200
```

The snapshot contains only coordination metadata. Row data is read lazily from the pinned Lance tables.

## What Lives In `__manifest`

`__manifest` is a Lance dataset with:

- one stable `table` row per logical Omnigraph table
- one append-only `table_version` row per published table version

The persisted schema is:

| Column | Type | Meaning |
|---|---|---|
| `object_id` | Utf8 | `table_key` or `<table_key>$<version>` |
| `object_type` | Utf8 | `table` or `table_version` |
| `location` | Utf8? | Relative table path for `table` rows |
| `metadata` | Utf8? | JSON-encoded table-version metadata |
| `base_objects` | List<Utf8>? | Reserved |
| `table_key` | Utf8 | Logical key like `node:Person` |
| `table_version` | UInt64? | Pinned Lance version for `table_version` rows |
| `table_branch` | Utf8? | Owning Lance branch for lazy branch forks |
| `row_count` | UInt64? | Row count for the published table version |

Omnigraph reconstructs graph state by reading all visible `table_version` rows in `__manifest` and selecting the latest version row per `table_key`.

## Two Version Spaces

There are still two independent version spaces:

- `__manifest` version: the graph version
- sub-table version: the native Lance version of an individual node or edge table

Example:

```text
__manifest v7:  Person@v2, Company@v1, Knows@v3
__manifest v8:  Person@v2, Company@v1, Knows@v4
__manifest v9:  Person@v4, Company@v1, Knows@v4
```

Only changed tables advance. Unchanged tables stay pinned to their previous native Lance version.

## How `Snapshot::open()` Works

`Snapshot::open("node:Person")` does not open a table by path directly anymore. It resolves through the namespace layer:

1. Look up the pinned `SubTableEntry` from the snapshot map.
2. Build a branch-aware namespace view for the active graph branch.
3. Use `DatasetBuilder::from_namespace(...)` to resolve the table.
4. Load the pinned table version, including `table_branch` when the table was lazily forked on a branch.

That keeps table discovery and version lookup on the Lance namespace path instead of hardcoding dataset opens all over the runtime.

## How Snapshots Are Built

### Hot path

`ManifestCoordinator` keeps the current `ManifestState` in memory. `snapshot()` is just a cheap clone of that state into a `Snapshot`.

```text
ManifestCoordinator
  known_state: { version: 9, entries: [...] }
      |
      +--> Snapshot { version: 9, entries: HashMap<table_key, SubTableEntry> }
```

No storage I/O happens on this path.

### Historical path

`ManifestCoordinator::snapshot_at(root_uri, branch, version)` reopens `__manifest`, checks out the requested branch and manifest version, and reconstructs the historical `ManifestState`.

This is the path used by:

- point-in-time reads
- `entity_at()`
- diff and change-detection APIs

## Branch Semantics

Omnigraph branches are still Lance branches, but now the graph branch head is the branch head of `__manifest`.

```text
__manifest
  main        v1 -> v2 -> v3 -> v4
                     |
  feature           +-> v3 -> v4
```

Sub-tables are still branched lazily:

- if a branch never writes to `node:Person`, the snapshot keeps `table_branch = null`
- on first branch-local write, the table gets its own Lance branch
- published `table_version` rows in `__manifest` then record `table_branch = feature`

That keeps branch storage proportional to changed tables instead of copying the whole graph.

## Write And Publish Flow

There are now two separate control-plane layers:

### Table-local write path

- `StagedTableNamespace` owns table-local namespace operations
- writes still commit native Lance table versions
- staged table history is visible through the namespace adapter before graph publish

### Graph publish path

- `GraphNamespacePublisher` is the graph-visible publish boundary
- it validates batch publish invariants against `__manifest`
- it atomically inserts immutable `table_version` rows into `__manifest`

That final `__manifest` merge-insert is the graph commit point.

## Why Omnigraph Still Has A Publisher Layer

Lance now owns:

- table storage
- table-local versioning
- namespace lookup
- native table history

Omnigraph still owns one thing Lance Rust does not expose directly today:

- branch-aware atomic batch publication of multiple table versions into `__manifest`

That is why `publisher.rs` still exists. It is the last custom graph-specific coordination layer.

## CLI / API Shape

`omnigraph snapshot` and `GET /snapshot` still expose:

- `manifest_version`
- per-table `table_key`
- `table_path`
- `table_version`
- `table_branch`
- `row_count`

The meaning of `manifest_version` is now specifically the `__manifest` dataset version.

## Source Files

- `crates/omnigraph/src/db/manifest.rs`
- `crates/omnigraph/src/db/manifest/repo.rs`
- `crates/omnigraph/src/db/manifest/state.rs`
- `crates/omnigraph/src/db/manifest/namespace.rs`
- `crates/omnigraph/src/db/manifest/publisher.rs`
