# Storage

## L1 — Lance dataset (per node/edge type)

Every node type and every edge type is its own Lance dataset:

- **Columnar Arrow storage**: each property is a column; nullable per Arrow schema.
- **Fragments**: data is partitioned into fragments; new writes create new fragments.
- **Dataset versioning**: each successful Lance dataset commit produces a new
  dataset version; old versions remain readable.
- **Stable row IDs**: stable row IDs are enabled on every Lance dataset OmniGraph creates — node and edge datasets, `__manifest`, `_graph_commit_recoveries.lance`, and any future system datasets. This is an architectural invariant: the flag is one-way at dataset create, so a future change that introduces a Lance dataset must preserve it. Consequences: `_row_created_at_version` and `_row_last_updated_at_version` are available on every dataset (load-bearing for change-feed validators); indices survive `omnigraph optimize`. Pre-0.4.x graphs created before this code path settled may have datasets without the flag and cannot be retrofitted in place — the supported path is dump-and-reload. The rewrite path used by `schema_apply` preserves the flag.
- **Append / delete / `merge_insert`**: native Lance write modes.
- **Per-dataset branches** (Lance native): copy-on-write at the dataset level.
- **Object-store agnostic**: file://, s3://, gs://, az://, http (read-only via Lance) — OmniGraph wires file:// and s3://.

## L2 — Multi-dataset coordination via `__manifest`

OmniGraph is **not** a single Lance dataset; it is a *graph* of datasets
coordinated through one versioned graph-manifest dataset.

- **Graph manifest**: the `__manifest/` Lance dataset.
- **Layout**:
  - `nodes/{stable_table_id:016x}-{table_incarnation_id:016x}` — one Lance dataset per node-type incarnation
  - `edges/{stable_table_id:016x}-{table_incarnation_id:016x}` — one Lance dataset per edge-type incarnation
  - `__manifest/` — the catalog of all backing datasets and their published versions, **and** the graph commit lineage (RFC-013 Phase 7: `graph_commit` / `graph_head` rows). A graph-level branch is authoritative through a Lance branch on `__manifest`; backing datasets fork lazily when written.
  - `_graph_commit_recoveries.lance` — the crash-recovery audit log (one row per recovery action; see below). The former `_graph_commits.lance` / `_graph_commit_actors.lance` lineage datasets are **retired**: lineage lives in `__manifest`, so a graph this binary creates has neither.
  - `__graph_index/csr-current.bin` — the persisted traversal-adjacency artifact (CSR/CSC + id dictionaries, binary), written only by `optimize` and read by traversal builds. **Derived, regenerable, never authoritative**: every load verifies embedded per-edge-dataset identity stamps against the live snapshot plus a payload digest, and rejects into the in-memory scan build. Not manifest-tracked and not covered by `cleanup`; one object per graph root, overwritten in place. Absent on stores never optimized by a ≥ this-version binary.
- **Manifest row schema** (`object_id, object_type, location, metadata, base_objects, table_key, stable_table_id, table_incarnation_id, table_version, table_branch, row_count`):
  - `object_type` ∈ `table | table_version | table_tombstone | graph_commit | graph_head`
  - `table_key` ∈ `node:<TypeName> | edge:<EdgeName>` (empty for `graph_commit` / `graph_head` lineage rows)
  - `(stable_table_id, table_incarnation_id)` is the immutable dataset coordinate; both legacy-named fields are nonzero on dataset registration, version, and tombstone rows and null on lineage rows. Persisted `table_key` is the current internal human-readable alias and may change on rename; public contracts use graph type names/keys instead.
  - `table_branch` is `null` for the main lineage and the branch name otherwise
  - **Graph lineage rows** (RFC-013 Phase 7): one immutable `graph_commit` row per commit (`object_id` = the commit ULID; `metadata` JSON carries parent / merged-parent / actor / timestamp) plus one mutable `graph_head:<branch>` pointer per branch (`graph_head:main` for main). The in-memory commit DAG is a projection of these rows.
- **Snapshot reconstruction**: latest visible `table_version` per stable table ID + incarnation minus tombstones scoped to that same pair, joined to the pair's current registration for its alias and path. Two live pairs cannot expose the same alias. A drop/re-add therefore starts an independent dataset-version sequence and cannot be hidden by the old lifetime's tombstone.
- **Atomic publish**: multi-dataset commits publish so that a single write to `__manifest` flips all the new dataset versions visible at once.
- **Row-level CAS on the merge-insert join key**: `object_id` carries an unenforced-primary-key annotation so Lance's bloom-filter conflict resolver rejects two concurrent commits that land the same `object_id` row. Without this annotation, Lance's transparent rebase would admit silent duplicates from racing publishers.
- **Optimistic concurrency control on publish**: legacy writers assert the graph manifest's current latest non-tombstoned version for each touched dataset; a mismatch surfaces as `PublishedDatasetVersionMismatch`. RFC-022-enrolled mutation/load attempts use a stronger, branch-wide contract: preparation captures the Lance-native branch identity, the exact `graph_head` (including absence), the accepted schema identity/catalog, and one base dataset snapshot. Under root-shared schema → branch → sorted-dataset gates, the engine revalidates that complete authority before any physical effect, then the publisher rechecks the exact native branch identity/head plus the touched-dataset versions. An insert-only mutation or Append/Merge load whose authority changed before effects discards and fully reprepares the bounded attempt; Update/Delete/Overwrite returns `ReadSetChanged`. Once any Lance dataset effect is durable, any later failure leaves the recovery sidecar authoritative and returns `RecoveryRequired` instead of silently rebasing or replaying the prepared plan.

### Internal schema versioning

The on-disk shape of `__manifest` is reconciled with the binary via a single version stamp (`omnigraph:internal_schema_version`) held in the manifest dataset's schema-level metadata. Storage is **strict-single-version** (the strand model): this binary reads exactly ONE internal-schema version, and there is no in-place migration.

- **Graph creation** stamps the current version, so newly initialized graphs always open.
- **Both open paths** (read-write and read-only) read main's stamp before reading any data and refuse a graph the binary cannot serve:
  - a stamp *below* CURRENT — a graph from an older release whose storage format this binary does not read — is refused with a **rebuild-via-export/import** message (there is no in-place upgrade; see the [upgrade guide](../operations/upgrade.md)).
  - a stamp *above* CURRENT — a graph written by a newer release — is refused with an **"upgrade omnigraph first"** message, so an old binary cannot misread a newer format.
- The stamp is read with no object-store writes, so the check is safe under a read-only open. Operators can see a graph's stamp with `omnigraph snapshot` and the binary's served version with `omnigraph version` (the `internal-schema` line).

The stamp values below are historical; this binary serves only the current one
(`v6`). An earlier-stamped graph is rebuilt via export/import, not migrated in
place.

| Stamp | Shape |
|---|---|
| v1 (implicit, pre-stamp) | `__manifest.object_id` had no PK annotation; no row-level CAS protection. |
| v2 | `__manifest.object_id` carries an unenforced-primary-key annotation; row-level CAS engaged. |
| v3 | Legacy `__run__*` staging branches (pre-v0.4.0 Run state machine) swept off `__manifest`. |
| v4 | Graph lineage folded into `__manifest` as `graph_commit` / `graph_head` rows (RFC-013 Phase 7); the `_graph_commits.lance` / `_graph_commit_actors.lance` datasets retired. |
| v5 | RFC-028 SchemaIR v2 plus graph-domain stable schema IDs; graph-manifest rows, OCC, recovery ownership, and physical paths keyed by stable table ID + incarnation. |
| v6 | Preserves v5 identity and activates RFC-023: every graph node/edge dataset has exact non-null physical `id` as Lance's unenforced PK, and every production strict insert/upsert uses the exact-`id` filter-bearing adapter. **The only version this binary serves.** |

## On-disk layout

A graph on disk is a directory tree of Lance datasets. Each dataset follows the standard Lance layout (`_versions/`, `data/`, `_indices/`, `_refs/`); OmniGraph adds the multi-dataset coordination by keeping `__manifest/` alongside the per-type datasets.

```mermaid
flowchart TB
    classDef l1 fill:#fef3e8,stroke:#c46900,color:#000
    classDef l2 fill:#e8f4fd,stroke:#1e6aa8,color:#000

    graph["graph URI<br/>file:// or s3://bucket/prefix"]:::l2

    manifest["__manifest/<br/>L2 catalog of datasets"]:::l2
    nodes["nodes/{stable-id}-{incarnation}/<br/>one dataset per node-type incarnation"]:::l2
    edges["edges/{stable-id}-{incarnation}/<br/>one dataset per edge-type incarnation"]:::l2
    cgraph["_graph_commit_recoveries.lance/<br/>crash-recovery audit log"]:::l2
    recovery["__recovery/{ulid}.json<br/>recovery sidecars (transient)"]:::l2
    initclaim["__init_claim.json<br/>init ownership (transient)"]:::l2
    refs["_refs/branches/{name}.json<br/>graph-level branches"]:::l2

    graph --> manifest
    graph --> nodes
    graph --> edges
    graph --> cgraph
    graph --> recovery
    graph --> initclaim
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
- **`__manifest/`** is a Lance dataset whose rows describe which dataset version is published on which graph branch. Reading a snapshot starts here.
- **`nodes/`** and **`edges/`** are sibling directories holding one Lance dataset per live node/edge type incarnation. Names encode the stable table ID and incarnation, so a public type rename keeps its path while a drop/re-add receives a fresh one.
- The graph commit DAG lives in **`__manifest`** as `graph_commit` / `graph_head` rows written in the publish CAS (RFC-013 Phase 7). The former `_graph_commits.lance` / `_graph_commit_actors.lance` lineage datasets are retired — a graph this binary creates has neither.
- **`_graph_commit_recoveries.lance`** — one internal row per completed crash-recovery action, including its exact per-dataset outcomes and the original actor. It joins by `graph_commit_id` to the graph commit lineage in `__manifest`. An exact v9 writer roll-forward keeps the interrupted writer's original actor; rollback and legacy recovery commits use `omnigraph:recovery`. The CLI does not currently expose this internal dataset.
- **`__recovery/{ulid}.json`** — transient sidecar files written by a writer before it advances the underlying dataset, deleted once the matching graph-manifest publish succeeds. A sidecar persisting after process exit means the writer crashed mid-commit; the next read-write open processes it. Steady-state directory is empty.
- **`__init_claim.json`** — transient create-if-absent ownership for one graph initialization attempt. It is absent after a normal init. An indeterminate physical Create or interrupted cleanup retains it so another strict or force initializer cannot overwrite uncertain state; remove stale residue only after every initializer for the root is quiesced and the root has been inspected.
- **`_refs/branches/{name}.json`** is graph-level branch metadata — pointers from a branch name to the graph manifest version it heads.
- **Inside each Lance dataset** (orange): the standard Lance directory layout. `_versions/{n}.manifest` records every commit; `data/` holds the actual Arrow fragments; `_indices/{uuid}/` holds index segments with their own `fragment_bitmap` for partial coverage; `_refs/` holds Lance-native per-dataset branches and tags.

The split — L2 owns the cross-dataset catalog; L1 owns the per-dataset internals — means that schema work (which adds or removes datasets) updates `__manifest`, while data work (which adds fragments) updates `_versions/` inside the affected dataset and then bumps `__manifest`.

## URI scheme support

| Scheme | Backend | Notes |
|---|---|---|
| local path / `file://` | local filesystem | Normalized to absolute paths; relative and dot-segment paths are lexically absolutized. Requires hard-link support (below) |
| `s3://bucket/prefix` | S3 object store | Honors `AWS_ENDPOINT_URL_S3`, `AWS_ALLOW_HTTP`, `AWS_S3_FORCE_PATH_STYLE` |
| `http(s)://host:port` | HTTP client to `omnigraph-server` | Used by CLI as a target, not a storage backend |

### Local filesystem requirement: hard links

The local backend publishes every atomic create-if-absent write — the root
`__init_claim.json`, strict init's additional `_schema.pg` defense, and each
Lance manifest commit, i.e. every graph write — via `hard_link(2)`. Filesystems that refuse hard links
(Android app-private storage, FAT/exFAT, some network and FUSE mounts) cannot
hold a writable local graph. `init` and read-write opens probe the graph
root's filesystem and fail up front with an error naming this requirement,
before any partial state is created; probing briefly creates and deletes
internal objects in the graph root (names starting with `__`, a prefix
reserved for OmniGraph internals, e.g. `__create_if_absent_probe_<unique-id>`).
Each bind removes only the probe object it successfully created; a colliding
foreign or crash-residue name is left untouched and retried with a fresh name.
Read-only opens (export, `commit list`) perform no writes and work on such
filesystems. S3-compatible backends are unaffected — the store implements
the conditional put server-side. The limitation comes from the upstream
local object-store implementation, which performs conditional put via
`hard_link(2)`; [apache/arrow-rs-object-store#826](https://github.com/apache/arrow-rs-object-store/pull/826)
tracks a `renameat2(RENAME_NOREPLACE)` fallback.

## Object-store env vars (S3-compatible)

- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` — for MinIO / RustFS / GCS-via-XML
- `AWS_S3_FORCE_PATH_STYLE=true` — path-style URLs
- `AWS_ALLOW_HTTP=true` — allow plain HTTP (local dev)
