# Storage

## L1 — Lance dataset (per node/edge type)

Every node type and every edge type is its own Lance dataset:

- **Columnar Arrow storage**: each property is a column; nullable per Arrow schema.
- **Fragments**: data is partitioned into fragments; new writes create new fragments.
- **Manifest versioning**: every commit produces a new dataset version; old versions remain readable.
- **Stable row IDs**: stable row IDs are enabled on every Lance dataset OmniGraph creates — node and edge data tables, `__manifest`, `_stream_tokens.lance`, `_graph_commit_recoveries.lance`, and any future system tables. This is an architectural invariant: the flag is one-way at dataset create, so a future change that introduces a Lance dataset must preserve it. Consequences: `_row_created_at_version` and `_row_last_updated_at_version` are available on every dataset (load-bearing for change-feed validators); indices survive `omnigraph optimize`. Pre-0.4.x graphs created before this code path settled may have datasets without the flag and cannot be retrofitted in place — the supported path is dump-and-reload. The rewrite path used by `schema_apply` preserves the flag.
- **Private v9 row attribution**: every node/edge table physically carries one nullable `__omnigraph_stream_v1$` struct. The trailing `$` is outside the `.pg` identifier grammar. SDK catalog/schema reflection, scans, filters, index reflection, and export omit or reject this storage-protocol field; it is not a user property.
- **Append / delete / `merge_insert`**: native Lance write modes.
- **Per-dataset branches** (Lance native): copy-on-write at the dataset level.
- **Object-store agnostic**: file://, s3://, gs://, az://, http (read-only via Lance) — OmniGraph wires file:// and s3://.

## L2 — Multi-dataset coordination via `__manifest`

OmniGraph is **not** a single Lance dataset; it is a *graph* of datasets coordinated through one append-only manifest table.

- **Manifest table**: `__manifest/` Lance dataset.
- **Layout**:
  - `nodes/{stable_table_id:016x}-{table_incarnation_id:016x}` — one Lance dataset per node-table lifetime
  - `edges/{stable_table_id:016x}-{table_incarnation_id:016x}` — one Lance dataset per edge-table lifetime
  - `__manifest/` — the catalog of all sub-tables and their published versions, **and** the graph commit lineage (RFC-013 Phase 7: `graph_commit` / `graph_head` rows). Graph-level branches are Lance branches on these datasets.
  - `_stream_tokens.lance` — graph-global RFC-026 current-token state. Its raw Lance HEAD is not authority; `__manifest` selects one exact witness.
  - `_graph_commit_recoveries.lance` — the graph-lineage crash-recovery audit log (see below). Recovery-v19 authority retirement is lineage-neutral and instead uses its selected immutable receipt/profile chain, so it appends no row here. The former `_graph_commits.lance` / `_graph_commit_actors.lance` lineage tables are **retired**: lineage lives in `__manifest`, so a graph this binary creates has neither.
- **Manifest row schema** (`object_id, object_type, location, metadata, base_objects, table_key, stable_table_id, table_incarnation_id, table_version, table_branch, row_count`):
  - `object_type` ∈ `table | table_version | table_tombstone | graph_commit | graph_head | stream_state | stream_token_authority | stream_profile`
  - `table_key` ∈ `node:<TypeName> | edge:<EdgeName>` (empty for lineage, `stream_token_authority`, and `stream_profile` rows). On `stream_state` it is diagnostic only; identity and binding come from the stable pair and metadata payload.
  - `(stable_table_id, table_incarnation_id)` is the immutable table coordinate; both fields are nonzero on table and `stream_state` rows and null on lineage and the graph-global `stream_token_authority` / `stream_profile` rows. `table_key` is the current human-readable alias and may change on rename.
  - **`stream_profile`** was introduced by v10 as the required graph-global RFC-026 §4.7 enablement singleton. V11 writes protocol v2: `profile_revision`, a bounded receipt-chain reference, and one strict state — `DISABLED`, `ENABLED` with its active fold delegation, `DISABLING` with a drain-only continuation plan, or receipt/cut-bound `RETIRED`. The row is pure metadata (null location/version/branch), cluster-owned, and movable only through the shared publisher's exact-entry CAS with a strict revision advance. Offline, state-lock-held `cluster apply --confirm-stream-offline` owns normal profile changes; v17's separate `cluster stream retire-for-rebuild plan|confirm` handshake owns the sole irreversible `DISABLED → RETIRED` transition. An enabled profile admits ordinary content writes only from the checked cluster serving runtime.
  - `table_branch` is `null` for the main lineage and the branch name otherwise
  - **Graph lineage rows** (RFC-013 Phase 7): one immutable `graph_commit` row per commit (`object_id` = the commit ULID; `metadata` JSON carries parent / merged-parent / actor / timestamp) plus one mutable `graph_head:<branch>` pointer per branch (`graph_head:main` for main). The in-memory commit DAG is a projection of these rows.
- **Snapshot reconstruction**: latest visible `table_version` per stable table ID + incarnation minus tombstones scoped to that same pair, joined to the pair's current registration for its alias and path. Two live pairs cannot expose the same alias. A drop/re-add therefore starts an independent version sequence and cannot be hidden by the old lifetime's tombstone.
  - **Atomic publish**: multi-dataset commits publish so that a single write to `__manifest` flips all the new sub-table versions visible at once. A private stream fold includes both its base-table pointer and the exact `_stream_tokens.lance` witness in that same visibility decision. Recovery-v13 `StreamProfileChange` similarly selects its exact token-ledger profile receipt with the next profile. Recovery-v14 selects lifecycle-v3 enrollment, claim, fold, or terminal-management ledger authority with the exact lifecycle successor; data-bearing folds also select graph lineage in that same CAS. Recovery-v15 selects the terminal claim and management receipts with the sole `OPEN` successor for a private resume or guarded drain abort. Recovery-v16 selects exact EnsureIndices table pointers with every affected `SEALED` lifecycle proof refresh in one CAS. Recovery-v17 separately selects Optimize's complete confirmed output set and exact achieved table HEADs with every affected `SEALED` proof refresh. Neither checked maintenance path writes a token row or maintenance receipt. Recovery-v18 selects a fresh physical binding and exact next `SEALED` proof. Recovery-v19 selects one immutable authority-retirement receipt/token witness with the terminal `RETIRED` profile; that CAS moves no graph head.
- **Row-level CAS on the merge-insert join key**: `object_id` carries an unenforced-primary-key annotation so Lance's bloom-filter conflict resolver rejects two concurrent commits that land the same `object_id` row. Without this annotation, Lance's transparent rebase would admit silent duplicates from racing publishers.
- **Optimistic concurrency control on publish**: legacy writers assert the manifest's current latest non-tombstoned version for each touched table; a mismatch surfaces as `ExpectedVersionMismatch`. RFC-022-enrolled mutation/load attempts use a stronger, branch-wide contract: preparation captures the Lance-native branch identity, the exact `graph_head` (including absence), the accepted schema identity/catalog, and one base table snapshot. Under root-shared schema → branch → stream-token → sorted-table gates, the engine revalidates that complete authority before any physical effect, then the publisher rechecks the exact native branch identity/head plus the touched-table versions. An insert-only mutation or Append/Merge load whose authority changed before effects discards and fully reprepares the bounded attempt; Update/Delete/Overwrite returns `ReadSetChanged`. Once any Lance effect is durable, any later failure leaves the recovery sidecar authoritative and returns `RecoveryRequired` instead of silently rebasing or replaying the prepared plan.

### Internal schema versioning

The on-disk shape of `__manifest` is reconciled with the binary via a single version stamp (`omnigraph:internal_schema_version`) held in the manifest dataset's schema-level metadata. Storage is **strict-single-version** (the strand model): this binary reads exactly ONE internal-schema version, and there is no in-place migration.

- **Graph creation** stamps the current version, so newly initialized graphs always open.
- **Both open paths** (read-write and read-only) read main's stamp before reading any data and refuse a graph the binary cannot serve:
  - a stamp *below* CURRENT — a graph from an older release whose storage format this binary does not read — is refused with a **rebuild-via-export/import** message (there is no in-place upgrade; see the [upgrade guide](../operations/upgrade.md)).
  - a stamp *above* CURRENT — a graph written by a newer release — is refused with an **"upgrade omnigraph first"** message, so an old binary cannot misread a newer format.
- The stamp is read with no object-store writes, so the check is safe under a read-only open. Operators can see a graph's stamp with `omnigraph snapshot` and the binary's served version with `omnigraph version` (the `internal-schema` line).

The stamp values below are historical; this binary serves only the current one
(`v17`). An earlier-stamped graph is rebuilt via export/import, not migrated in
place.

| Stamp | Shape |
|---|---|
| v1 (implicit, pre-stamp) | `__manifest.object_id` had no PK annotation; no row-level CAS protection. |
| v2 | `__manifest.object_id` carries an unenforced-primary-key annotation; row-level CAS engaged. |
| v3 | Legacy `__run__*` staging branches (pre-v0.4.0 Run state machine) swept off `__manifest`. |
| v4 | Graph lineage folded into `__manifest` as `graph_commit` / `graph_head` rows (RFC-013 Phase 7); the `_graph_commits.lance` / `_graph_commit_actors.lance` tables retired. |
| v5 | RFC-028 SchemaIR v2 plus graph-domain stable schema IDs; manifest table rows, OCC, recovery ownership, and physical paths keyed by stable table ID + incarnation. |
| v6 | Preserves v5 identity and activates RFC-023: every graph node/edge dataset has exact non-null physical `id` as Lance's unenforced PK, and every production strict insert/upsert uses the exact-`id` filter-bearing adapter. |
| v7 | Preserves v5/v6 identity and keyed-write contracts, and adds RFC-026 identity-keyed `stream_state` authority: physical enrollment binding, mutable current-HEAD witness, lifecycle, and per-shard epoch floor. Phase A can recover one exact empty private enrollment; no production row-streaming API is active. |
| v8 | Preserves the v5/v6/v7 contracts and activates RFC-026 stream-config v2 plus recovery-v11 for the private B1 row/fold core: one no-roll generation, watcher success plus a same-writer post-durability epoch check before clean acknowledgement, and one exact fold whose manifest publish advances the table pointer, lifecycle witness, and graph lineage together. The legal near-cap closure cell is green after logical-slice accounting and dense scanner-batch copies. |
| v9 | Preserves the bounded B1 worker mechanics and activates RFC-026 stream-config v3, lifecycle state v2, the grammar-impossible trusted-attribution field, manifest-selected `_stream_tokens.lance` authority, compare-and-chain tokens, and recovery-v12's exact base-plus-token fold. The selected B2a profile retains all canonical MemWAL objects and imposes no retained-byte/object/file/history quota. |
| v10 | Preserves v9 and adds RFC-026 §4.7's required graph-global boolean `stream_profile` enablement singleton plus the now-frozen explicit-null fold-attribution dead-letter compatibility placeholder. The graph-scoped `stream_ingest` / `stream_manage` Cedar vocabulary is registered, and embedded manifest-only status is read-only. Recovery-v12 is the exact ordinary private fold envelope. |
| v11 | Replaces the boolean with protocol-v2 `DISABLED | ENABLED | DISABLING | RETIRED` state, a bounded receipt-chain reference, and checked delegation/continuation/retirement authority. Profile mutation becomes an offline checked cluster-control operation. Enabled-profile Mutation/Load/delete require the exact served runtime, while BranchMerge is refused under both `ENABLED` and `DISABLING`. Recovery-v13 owns the exact profile-management receipt/profile transition. |
| v12 | Replaces lifecycle state-v2's inline receipt vectors with lifecycle-v3 fixed-size ledger-chain/current pointers and an authenticated WAL-tail commitment. Recovery-v14 activates exact hidden `StreamEnrollmentV2`, `StreamClaim`, `StreamFoldV2`, `StreamDrainFold`, and `StreamLifecycleReceipt` owners. The private path can recovery-cover cold epoch claims and quiesce never-written or non-empty lanes through `OPEN → DRAINING → SEALED`, including restart after a claim or an already-flushed cut. Historical recovery-v10 enrollment and recovery-v12 lifecycle-v2 fold payloads retain their old meaning and are refused under this format. Its three-field v14 resume scaffold remains frozen and is not reinterpreted. |
| v13 | Preserves lifecycle-v3 and raises the sidecar ceiling to recovery-v15. The hidden `StreamResume` owner recovery-covers revision-fenced `SEALED → OPEN` resume and guarded `DRAINING → OPEN` abort, including the higher-epoch claim and terminal claim/management receipts. |
| v14 | Raises the sidecar ceiling to recovery-v16 for the private, checked-runtime, canonical-main `SEALED` EnsureIndices bridge. V16 reuses the exact recovery-v8 CreateIndex plan and atomically refreshes each affected lifecycle HEAD witness, verified-empty proof, and revision without a token effect, management receipt, or caller operation ID. Ambient EnsureIndices remains refused for enrolled tables. |
| v15 | Raises the sidecar ceiling to recovery-v17 for the distinct private, checked-runtime, canonical-main `SEALED` Optimize bridge. V17 binds Optimize's complete confirmed output set and exact achieved table HEADs, then atomically refreshes productive pointers and lifecycle proofs without a token effect, management receipt, or caller operation ID. Ambient Optimize remains refused for enrolled tables. |
| v16 | Raises the sidecar ceiling to recovery-v18 for private physical rebind. V18 binds the complete prior `SEALED` authority, exact fresh enrollment and empty shard, immutable binding and fence-only claim receipts, and the next `SEALED` proof. Rebind does not open admission; a separate resume claims within the new scope. |
| v17 | Raises the sidecar ceiling to recovery-v19 for root-wide authority retirement. A stopped/offline, state-lock-held plan proves an exact `DISABLED`, all-`SEALED`, recovery-clear cut with base/token parity and at least one current `WITHDRAWN` token. Confirmation appends one immutable receipt and selects it with `RETIRED` without moving graph lineage. The source becomes permanently query/status/export-only; retired export emits the root receipt plus a closed witness for the selected frozen branch member as provenance. Correction and public row/lifecycle transports remain inactive. **The only version this binary serves.** |

## On-disk layout

A graph on disk is a directory tree of Lance datasets. Each dataset follows the standard Lance layout (`_versions/`, `data/`, `_indices/`, `_refs/`); OmniGraph adds the multi-dataset coordination by keeping `__manifest/` alongside the per-type datasets.

```mermaid
flowchart TB
    classDef l1 fill:#fef3e8,stroke:#c46900,color:#000
    classDef l2 fill:#e8f4fd,stroke:#1e6aa8,color:#000

    graph["graph URI<br/>file:// or s3://bucket/prefix"]:::l2

    manifest["__manifest/<br/>L2 catalog of sub-tables"]:::l2
    nodes["nodes/{stable-id}-{incarnation}/<br/>one dataset per node-table lifetime"]:::l2
    edges["edges/{stable-id}-{incarnation}/<br/>one dataset per edge-table lifetime"]:::l2
    tokens["_stream_tokens.lance/<br/>manifest-selected current-token authority"]:::l2
    cgraph["_graph_commit_recoveries.lance/<br/>crash-recovery audit log"]:::l2
    recovery["__recovery/{ulid}.json<br/>recovery sidecars (transient)"]:::l2
    refs["_refs/branches/{name}.json<br/>graph-level branches"]:::l2

    graph --> manifest
    graph --> nodes
    graph --> edges
    graph --> tokens
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
    tokens -.-> dataset
    manifest -.-> dataset
```

**What's where:**

- **Graph root** is one directory (or S3 prefix). Everything below is part of one OmniGraph graph.
- **`__manifest/`** is a Lance dataset whose rows describe which sub-table version is published at which graph-branch. Reading a snapshot starts here.
- **`nodes/`** and **`edges/`** are sibling directories holding one Lance dataset per live table lifetime. Names encode the stable table ID and incarnation, so a public type rename keeps its path while a drop/re-add receives a fresh one.
- **`_stream_tokens.lance`** stores at most one current RFC-026 authority row per logical graph key. Only the exact version selected by the matching `stream_token_authority` manifest row is authoritative; operators must not advance, rewrite, clean, or delete it directly.
- The graph commit DAG lives in **`__manifest`** as `graph_commit` / `graph_head` rows written in the publish CAS (RFC-013 Phase 7). The former `_graph_commits.lance` / `_graph_commit_actors.lance` lineage tables are retired — a graph this binary creates has neither.
- **`_graph_commit_recoveries.lance`** — one internal row per completed crash-recovery action, including its exact per-table outcomes and the original actor. It joins by `graph_commit_id` to the graph commit lineage in `__manifest`. An exact v9 writer roll-forward keeps the interrupted writer's original actor; rollback and legacy recovery commits use `omnigraph:recovery`. The CLI does not currently expose this internal table.
- **`__recovery/{ulid}.json`** — transient sidecar files written by a writer before it advances the underlying dataset, deleted once the matching manifest publish succeeds. A sidecar persisting after process exit means the writer crashed mid-commit; the next read-write open processes it. Steady-state directory is empty.
- **`_refs/branches/{name}.json`** is graph-level branch metadata — pointers from a branch name to the manifest version it heads.
- **Inside each Lance dataset** (orange): the standard Lance directory layout. `_versions/{n}.manifest` records every commit; `data/` holds the actual Arrow fragments; `_indices/{uuid}/` holds index segments with their own `fragment_bitmap` for partial coverage; `_refs/` holds Lance-native per-dataset branches and tags.

The split — L2 owns cross-dataset authority; L1 owns each dataset's internals — means that schema work (which adds or removes datasets) updates `__manifest`, while data work (which adds fragments) updates `_versions/` inside the affected dataset and then bumps `__manifest`. A private stream fold stages both base and token effects first, then publishes their exact witnesses together.

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
