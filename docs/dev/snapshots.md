# Snapshots

## What a Snapshot Is

A snapshot is a frozen, consistent read view of the entire graph at a point in time. It records which version of each sub-table is current, not the data itself. All reads within a query go through one snapshot to guarantee cross-type consistency.

A snapshot is cheap to create (no storage I/O) and immutable once built.

```
Snapshot
  root_uri:  "s3://omnigraph-repo/context"
  version:   7                                  ← manifest version
  entries:
    node:Person   → nodes/a3f8c...  v4  120 rows
    node:Company  → nodes/7b21e...  v3   45 rows
    edge:Knows    → edges/c92d1...  v5  200 rows
    edge:WorksAt  → edges/e1f0a...  v2   90 rows
```

Two snapshots pointing to the same sub-table version share the same physical data on disk. A snapshot is a set of pinned pointers, not a copy of data.

## Two-Level Versioning

Omnigraph has two independent version spaces:

- **Manifest version** — the database version. Monotonic, one per commit.
- **Sub-table version** — each Lance dataset (Person, Company, Knows, ...) has its own version timeline.

The manifest records which sub-table versions are current at each database version:

```
Manifest v5:  Person@v2, Company@v1, Knows@v3
Manifest v6:  Person@v2, Company@v1, Knows@v4    ← only Knows changed
Manifest v7:  Person@v4, Company@v1, Knows@v4    ← only Person changed
```

Tables that did not change keep their old pinned version. The manifest is the single coordination point. A write commits new sub-table versions, then atomically advances the manifest. That manifest version becomes the new database version.

## The Manifest

The manifest is itself a Lance dataset (`_manifest.lance`) with one row per sub-table:

| Column | Type | Description |
|---|---|---|
| `table_key` | String | `"node:Person"`, `"edge:Knows"`, etc. |
| `table_path` | String | Relative path to the Lance dataset directory |
| `table_version` | UInt64 | Pinned dataset version |
| `table_branch` | String? | Lance branch name (null = main) |
| `row_count` | UInt64 | Row count at this version |

The `table_path` uses a deterministic FNV-1a hash of the type name:

```
Person  → nodes/a3f8c0e1b2d34567
Knows   → edges/c92d10f4a5b67890
```

The `table_branch` field is null when the sub-table is on the main Lance branch. When Omnigraph branching forks a sub-table onto a child branch, this field records which Lance branch to check out before pinning the version.

## How Snapshot.open() Works

When a query needs data from a specific type, it calls `snapshot.open("node:Person")`. This does three things:

```
1. Look up the entry from the in-memory map
   → { table_path: "nodes/a3f8c...", table_version: 4, table_branch: None }

2. Open the Lance dataset at {root_uri}/{table_path}

3. If table_branch is set, checkout that Lance branch
   Then checkout the exact pinned version
   → Dataset pinned at version 4
```

This is the only path to data. Every read goes through a snapshot, and every snapshot pins every table. There is no way to accidentally read a table at the wrong version.

## How Snapshots Are Created

### Hot path — from in-memory state

`ManifestCoordinator` holds the current manifest state in memory. Calling `.snapshot()` clones this state into a `Snapshot` struct. No storage I/O.

```
ManifestCoordinator
  known_state: { version: 7, entries: [...] }
        |
        +---> Snapshot { version: 7, entries: HashMap<key, entry> }
```

This is the normal read path. Every query that targets the current branch head uses this.

### Time-travel path — from historical state

`ManifestCoordinator::snapshot_at(root_uri, branch, version)` reopens the manifest dataset, checks out the requested branch and version, and reads the entries from that historical state:

```
snapshot_at(root, Some("feature-x"), version=5)
  |
  +-- open _manifest.lance
  +-- checkout_branch("feature-x")
  +-- checkout_version(5)
  +-- read entries from that manifest state
       +---> Snapshot { version: 5, entries: { ... } }
```

This path is used by `run_query_at()`, `entity_at()`, and the change detection APIs.

## Snapshots and Branches

Each Omnigraph branch has its own manifest timeline. A branch is a Lance branch on `_manifest.lance`. Opening a branch means checking out that branch on the manifest dataset:

```
_manifest.lance
  main          v1 → v2 → v3 → v4 → v5 → v6 → v7
                           |
  feature-x                +→ v4 → v5 → v6
```

A snapshot from `main` at v7 and a snapshot from `feature-x` at v6 may point to the same sub-table version for types that did not diverge. The data is shared; only the pointers differ.

Sub-tables are branched lazily. If a branch never modifies a type, that type's entry still points at the parent branch's dataset and version. The sub-table is only forked when the branch actually writes to it.

## Snapshots and Writes

Writes never modify a snapshot. The write path is:

```
1. Capture target branch head (a snapshot)
2. Create a hidden run branch
3. Write data to sub-tables on the run branch
4. Verify target branch head did not advance
5. Publish: merge run state into target branch
6. Advance manifest → new snapshot
```

The old snapshot remains valid and unchanged. Readers holding the old snapshot continue to see the pre-write state. The new snapshot, with updated sub-table versions, becomes visible to subsequent reads.

## The snapshot CLI Command

`omnigraph snapshot` is a diagnostic tool that dumps the manifest state for a branch. It reads only the manifest entries, not the actual table data.

```
$ omnigraph snapshot ./my-repo --branch main

branch: main
manifest_version: 7
node:Company   v3  branch=main  rows=45
node:Person    v4  branch=main  rows=120
edge:Knows     v5  branch=main  rows=200
edge:WorksAt   v2  branch=main  rows=90
```

With `--json`, the output is structured:

```json
{
  "branch": "main",
  "manifest_version": 7,
  "tables": [
    {
      "table_key": "node:Person",
      "table_path": "nodes/a3f8c...",
      "table_version": 4,
      "table_branch": null,
      "row_count": 120
    }
  ]
}
```

This works against both local repos and remote servers (`--target http://...` calls `GET /snapshot?branch=...`).

Useful for answering:

- What manifest version is this branch at?
- Which sub-tables changed between branches?
- Is a sub-table on a forked Lance branch or still on main?
- How many rows does each type have?

## What Is Not in a Snapshot

A snapshot contains only coordination metadata. It does not hold or cache:

- Row data (node properties, edge properties)
- Blob content
- Graph topology (the GraphIndex is a separate derived structure)
- Query results

All row data is read from Lance datasets on demand when a query opens a sub-table through the snapshot. Nothing is retained after the query completes.

## Key Source Files

- `crates/omnigraph/src/db/manifest.rs` — `Snapshot`, `ManifestCoordinator`, `SubTableEntry`
- `crates/omnigraph/src/db/graph_coordinator.rs` — coordinates manifest with commit graph and run registry
- `crates/omnigraph/src/db/omnigraph.rs` — `snapshot_of()`, `snapshot_at_version()`
- `crates/omnigraph-server/src/api.rs` — `snapshot_payload()`, `SnapshotOutput`
- `crates/omnigraph-cli/src/main.rs` — `Command::Snapshot` handler
