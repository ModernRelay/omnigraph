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
  - `__manifest/` — the catalog of all sub-tables and their published versions, **and** the graph commit lineage (RFC-013 Phase 7: `graph_commit` / `graph_head` rows). Graph-level branches are Lance branches on these datasets.
  - `_graph_commit_recoveries.lance` — the crash-recovery audit log (one row per recovery action; see below). The former `_graph_commits.lance` / `_graph_commit_actors.lance` lineage tables are **retired**: lineage lives in `__manifest`, so a graph this binary creates has neither.
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

The on-disk shape of `__manifest` is reconciled with the binary via a single version stamp (`omnigraph:internal_schema_version`) held in the manifest dataset's schema-level metadata. Storage is **strict-single-version** (the strand model): this binary reads exactly ONE internal-schema version, and there is no in-place migration.

- **Graph creation** stamps the current version, so newly initialized graphs always open.
- **Both open paths** (read-write and read-only) read main's stamp before reading any data and refuse a graph the binary cannot serve:
  - a stamp *below* CURRENT — a graph from an older release whose storage format this binary does not read — is refused with a **rebuild-via-export/import** message (there is no in-place upgrade; see the [upgrade guide](../operations/upgrade.md)).
  - a stamp *above* CURRENT — a graph written by a newer release — is refused with an **"upgrade omnigraph first"** message, so an old binary cannot misread a newer format.
- The stamp is read with no object-store writes, so the check is safe under a read-only open. Operators can see a graph's stamp with `omnigraph snapshot` and the binary's served version with `omnigraph version` (the `internal-schema` line).

The stamp values below are historical; this binary serves only the current one (`v4`). An earlier-stamped graph is rebuilt via export/import, not migrated in place.

| Stamp | Shape |
|---|---|
| v1 (implicit, pre-stamp) | `__manifest.object_id` had no PK annotation; no row-level CAS protection. |
| v2 | `__manifest.object_id` carries an unenforced-primary-key annotation; row-level CAS engaged. |
| v3 | Legacy `__run__*` staging branches (pre-v0.4.0 Run state machine) swept off `__manifest`. |
| v4 | Graph lineage folded into `__manifest` as `graph_commit` / `graph_head` rows (RFC-013 Phase 7); the `_graph_commits.lance` / `_graph_commit_actors.lance` tables retired. **The only version this binary serves.** |

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
    cgraph["_graph_commit_recoveries.lance/<br/>crash-recovery audit log"]:::l2
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
- The graph commit DAG lives in **`__manifest`** as `graph_commit` / `graph_head` rows written in the publish CAS (RFC-013 Phase 7). The former `_graph_commits.lance` / `_graph_commit_actors.lance` lineage tables are retired — a graph this binary creates has neither.
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
