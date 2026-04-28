# Storage

## L1 — Lance dataset (per node/edge type)

Every node type and every edge type is its own Lance dataset:

- **Columnar Arrow storage**: each property is a column; nullable per Arrow schema.
- **Fragments**: data is partitioned into fragments; new writes create new fragments.
- **Manifest versioning**: every commit produces a new dataset version; old versions remain readable.
- **Stable row IDs**: enabled by OmniGraph for the commit-graph and run-registry datasets so durable references survive compaction.
- **Append / delete / `merge_insert`**: native Lance write modes.
- **Per-dataset branches** (Lance native): copy-on-write at the dataset level.
- **Object-store agnostic**: file://, s3://, gs://, az://, http (read-only via Lance) — OmniGraph wires file:// and s3:// (`storage.rs`).

## L2 — Multi-dataset coordination via `__manifest`

OmniGraph is **not** a single Lance dataset; it is a *graph* of datasets coordinated through one append-only manifest table.

- **Manifest table**: `__manifest/` Lance dataset.
- **Layout** (`db/manifest/layout.rs`, `db/manifest/state.rs`):
  - `nodes/{fnv1a64-hex(type_name)}` — one Lance dataset per node type
  - `edges/{fnv1a64-hex(edge_type_name)}` — one Lance dataset per edge type
  - `__manifest/` — the catalog of all sub-tables and their published versions
  - `_graph_commits.lance` / `_graph_commit_actors.lance` — the commit graph and its actor map
  - `_graph_runs.lance` / `_graph_run_actors.lance` — the run registry and its actor map
- **Manifest row schema** (`object_id, object_type, location, metadata, base_objects, table_key, table_version, table_branch, row_count`):
  - `object_type` ∈ `table | table_version | table_tombstone`
  - `table_key` ∈ `node:<TypeName> | edge:<EdgeName>`
  - `table_branch` is `null` for the main lineage and the branch name otherwise
- **Snapshot reconstruction**: latest visible `table_version` per `(table_key, table_branch)` minus tombstones whose `tombstone_version >= table_version`.
- **Atomic publish**: multi-dataset commits publish via a `ManifestBatchPublisher` so a single write to `__manifest` flips all the new sub-table versions visible at once.

## URI scheme support (`storage.rs`)

| Scheme | Backend | Notes |
|---|---|---|
| local path / `file://` | `LocalStorageAdapter` (tokio) | Normalized to absolute paths |
| `s3://bucket/prefix` | `S3StorageAdapter` (object_store) | Honors `AWS_ENDPOINT_URL_S3`, `AWS_ALLOW_HTTP`, `AWS_S3_FORCE_PATH_STYLE` |
| `http(s)://host:port` | HTTP client to `omnigraph-server` | Used by CLI as a target, not a storage backend |

## Object-store env vars (S3-compatible)

- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` — for MinIO / RustFS / GCS-via-XML
- `AWS_S3_FORCE_PATH_STYLE=true` — path-style URLs
- `AWS_ALLOW_HTTP=true` — allow plain HTTP (local dev)
