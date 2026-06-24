# Storage

## L1 — Lance dataset (per node/edge type)

Every node type and every edge type is its own Lance dataset:

- **Columnar Arrow storage**: each property is a column; nullable per Arrow schema.
- **Fragments**: data is partitioned into fragments; new writes create new fragments.
- **Manifest versioning**: every commit produces a new dataset version; old versions remain readable.
- **Stable row IDs**: stable row IDs are enabled on every Lance dataset OmniGraph creates — node and edge data tables, `__manifest`, the commit-graph datasets, and any future system tables. This is an architectural invariant: the flag is one-way at dataset create, so a future change that introduces a Lance dataset must preserve it. Consequences: `_row_created_at_version` and `_row_last_updated_at_version` are available on every dataset (load-bearing for change-feed validators); indices survive `omnigraph optimize`. Pre-0.4.x graphs created before this code path settled may have datasets without the flag and cannot be retrofitted in place — the supported path is dump-and-reload. The rewrite path used by `schema_apply` preserves the flag.
- **Append / delete / `merge_insert`**: native Lance write modes.
- **Per-dataset branches** (Lance native): copy-on-write at the dataset level.
- **Object-store agnostic**: file://, s3://, gs://, az://, http (read-only via Lance) — OmniGraph wires file:// and s3://.

## L2 — Multi-dataset coordination via `__manifest`

OmniGraph is **not** a single Lance dataset; it is a *graph* of datasets coordinated through one append-only manifest table.

- **Manifest table**: `__manifest/` Lance dataset.
- **Layout**:
  - `nodes/{fnv1a64-hex(type_name)}` — one Lance dataset per node type
  - `edges/{fnv1a64-hex(edge_type_name)}` — one Lance dataset per edge type
  - `__manifest/` — the catalog of all sub-tables and their published versions, **and** the graph commit lineage (RFC-013 Phase 7)
  - `_graph_commits.lance` / `_graph_commit_actors.lance` — legacy / branch-ref carriers. Since RFC-013 Phase 7 the graph lineage lives in `__manifest` (`graph_commit` / `graph_head` rows, written in the publish CAS); `_graph_commits.lance` no longer receives commit rows, but is retained to carry the Lance branch refs that `create_branch` / `list_branches` / the `cleanup` orphan reconciler operate on. A graph created before Phase 7 (internal schema v3) keeps its lineage here until its first read-write open, which migrates it into `__manifest` via `migrate_v3_to_v4`.
  - (legacy `_graph_runs.lance` / `_graph_run_actors.lance` from pre-v0.4.0 graphs are inert; the run state machine was removed. The internal schema migration sweeps stale `__run__*` branches on first write-open; the inert dataset bytes themselves remain until a prefix-delete storage primitive lands)
- **Manifest row schema** (`object_id, object_type, location, metadata, base_objects, table_key, table_version, table_branch, row_count`):
  - `object_type` ∈ `table | table_version | table_tombstone | graph_commit | graph_head`
  - `table_key` ∈ `node:<TypeName> | edge:<EdgeName>` (empty for `graph_commit` / `graph_head` lineage rows)
  - `table_branch` is `null` for the main lineage and the branch name otherwise
  - **Graph lineage rows** (RFC-013 Phase 7): one immutable `graph_commit` row per commit (`object_id` = the commit ULID; `metadata` JSON carries parent / merged-parent / actor / timestamp) plus one mutable `graph_head:<branch>` pointer per branch (`graph_head:main` for main). The in-memory commit DAG is a projection of these rows.
- **Snapshot reconstruction**: latest visible `table_version` per `(table_key, table_branch)` minus tombstones — rows where `object_type = table_tombstone`, whose own `table_version` (acting as the tombstone version) is `>= the entry's table_version`.
- **Atomic publish**: multi-dataset commits publish so that a single write to `__manifest` flips all the new sub-table versions visible at once.
- **Row-level CAS on the merge-insert join key**: `object_id` carries an unenforced-primary-key annotation so Lance's bloom-filter conflict resolver rejects two concurrent commits that land the same `object_id` row. Without this annotation, Lance's transparent rebase would admit silent duplicates from racing publishers.
- **Optimistic concurrency control on publish**: a publish asserts the manifest's current latest non-tombstoned version for each touched table is exactly what the caller observed; mismatches surface as an `ExpectedVersionMismatch` manifest conflict naming the table and the expected/actual versions. Concurrent advances surface as a conflict rather than being silently rebased through.

### Internal schema versioning

The on-disk shape of `__manifest` is reconciled with the binary via a single version stamp held in the manifest dataset's schema-level metadata.

- **Graph creation** stamps the current version, so newly initialized graphs never need migration.
- **The open-for-write path** migrates the on-disk stamp before reading state. When the stamp matches the binary, this is a single metadata read with no writes; otherwise the migration walks steps forward (1→2, 2→3, …) until the stamp matches, then proceeds with the publish. Reads stay side-effect-free.
- **Forward-version protection**: a stamp *higher* than the binary's known version triggers a clear "upgrade omnigraph first" error. An old binary cannot clobber a newer schema by silently treating "unknown stamp" as "missing stamp".
- **Idempotency**: each migration step is safe to re-run. A crash between two metadata updates inside a single step leaves the partial state; the next open re-runs the step and the second update lands.

| Stamp | Shape change |
|---|---|
| v1 (implicit, pre-stamp) | `__manifest.object_id` had no PK annotation; no row-level CAS protection. |
| v2 | `__manifest.object_id` carries an unenforced-primary-key annotation; row-level CAS engaged. |
| v3 | One-time sweep of legacy `__run__*` staging branches (pre-v0.4.0 Run state machine, removed) off `__manifest`. Runs at read-write open and on publish. |

## On-disk layout

A graph on disk is a directory tree of Lance datasets. Each dataset follows the standard Lance layout (`_versions/`, `data/`, `_indices/`, `_refs/`); OmniGraph adds the multi-dataset coordination by keeping `__manifest/` alongside the per-type datasets.

```mermaid
flowchart TB
    classDef l1 fill:#fef3e8,stroke:#c46900,color:#000
    classDef l2 fill:#e8f4fd,stroke:#1e6aa8,color:#000

    graph["graph URI<br/>file:// or s3://bucket/prefix"]:::l2

    manifest["__manifest/<br/>L2 catalog of sub-tables"]:::l2
    nodes["nodes/{fnv1a64-hex}/<br/>one dataset per node type"]:::l2
    edges["edges/{fnv1a64-hex}/<br/>one dataset per edge type"]:::l2
    cgraph["_graph_commits.lance/<br/>_graph_commit_actors.lance/<br/>_graph_commit_recoveries.lance/"]:::l2
    recovery["__recovery/{ulid}.json<br/>recovery sidecars (transient)"]:::l2
    refs["_refs/branches/{name}.json<br/>graph-level branches"]:::l2

    graph --> manifest
    graph --> nodes
    graph --> edges
    graph --> cgraph
    graph --> recovery
    graph --> refs

    subgraph dataset[Inside each Lance dataset — L1]
        ds_v["_versions/{n}.manifest<br/>per-dataset versions"]:::l1
        ds_data["data/<br/>fragment files (Arrow IPC)"]:::l1
        ds_idx["_indices/{uuid}/<br/>BTREE · Inverted FTS · IVF/HNSW"]:::l1
        ds_refs["_refs/<br/>per-dataset Lance branches/tags"]:::l1
        ds_tx["_transactions/<br/>commit transaction logs"]:::l1
    end

    nodes -.-> dataset
    edges -.-> dataset
    manifest -.-> dataset
```

**What's where:**

- **Graph root** is one directory (or S3 prefix). Everything below is part of one OmniGraph graph.
- **`__manifest/`** is a Lance dataset whose rows describe which sub-table version is published at which graph-branch. Reading a snapshot starts here.
- **`nodes/`** and **`edges/`** are sibling directories holding one Lance dataset per declared type. Names are `fnv1a64-hex` of the type name to keep paths fixed-length and case-safe.
- **`_graph_commits.lance`** is an L2 dataset retained only as a branch-ref carrier (and, on a pre-Phase-7 graph, the migration source). Since RFC-013 Phase 7 the graph commit DAG lives in `__manifest` as `graph_commit` / `graph_head` rows written in the publish CAS — `_graph_commits.lance` and its paired `_graph_commit_actors.lance` no longer receive commit rows. A graph created before Phase 7 (internal schema v3) backfills its lineage into `__manifest` on its first read-write open (`migrate_v3_to_v4`). (Pre-v0.4.0 graphs also have inert `_graph_runs.lance` / `_graph_run_actors.lance` from the removed Run state machine; the internal schema migration sweeps their stale `__run__*` branches, and the dataset bytes are reclaimed once a prefix-delete primitive lands.)
- **`_graph_commit_recoveries.lance`** — one row per crash-recovery action. Joined by `graph_commit_id` to the graph commit lineage (the `graph_commit` rows in `__manifest` since RFC-013 Phase 7); the linked commit carries `actor_id=omnigraph:recovery`. Operators correlate recoveries with the original mutations they rolled forward / back via this join.
- **`__recovery/{ulid}.json`** — transient sidecar files written by a writer before it advances the underlying dataset, deleted once the matching manifest publish succeeds. A sidecar persisting after process exit means the writer crashed mid-commit; the next read-write open processes it. Steady-state directory is empty.
- **`_refs/branches/{name}.json`** is graph-level branch metadata — pointers from a branch name to the manifest version it heads.
- **Inside each Lance dataset** (orange): the standard Lance directory layout. `_versions/{n}.manifest` records every commit; `data/` holds the actual Arrow fragments; `_indices/{uuid}/` holds index segments with their own `fragment_bitmap` for partial coverage; `_refs/` holds Lance-native per-dataset branches and tags.

The split — L2 owns the cross-dataset catalog; L1 owns the per-dataset internals — means that schema work (which adds or removes datasets) updates `__manifest`, while data work (which adds fragments) updates `_versions/` inside the affected dataset and then bumps `__manifest`.

## URI scheme support

| Scheme | Backend | Notes |
|---|---|---|
| local path / `file://` | local filesystem | Normalized to absolute paths; relative and dot-segment paths are lexically absolutized |
| `s3://bucket/prefix` | S3 object store | Honors `AWS_ENDPOINT_URL_S3`, `AWS_ALLOW_HTTP`, `AWS_S3_FORCE_PATH_STYLE` |
| `http(s)://host:port` | HTTP client to `omnigraph-server` | Used by CLI as a target, not a storage backend |

## Object-store env vars (S3-compatible)

- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` — for MinIO / RustFS / GCS-via-XML
- `AWS_S3_FORCE_PATH_STYLE=true` — path-style URLs
- `AWS_ALLOW_HTTP=true` — allow plain HTTP (local dev)
