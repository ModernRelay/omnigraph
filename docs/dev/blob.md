# Blob internals

OmniGraph treats a Blob as one property cell on an existing node or edge. Lance owns the Blob-v2 physical placement; OmniGraph owns logical identity, snapshot selection, authorization, external-source admission, bounded delivery, and graph-level publication.

This page describes the implemented read and ingestion behavior. Upload and clear commands proposed by [RFC 0033](../rfcs/0033-blob-management.md) are not implemented yet.

## Logical states

A Blob cell is exactly one of:

- **null** — no value;
- **managed** — bytes owned by the selected Lance dataset version;
- **external** — a persisted descriptor for caller-owned bytes at an absolute URI.

A valid zero-byte managed value is not null. Null detection uses Arrow validity, never descriptor-field heuristics. Managed placement—inline, packed, or dedicated—is derived Lance state and is not exposed as a product contract.

Blob properties cannot be keys, unique values, query projections, filters, ordering keys, or aggregate inputs. `.gq` rejects projection rather than substituting a plausible null.

## Engine read facade

The public engine entry point is:

```rust
read_blob_at(ReadTarget, BlobCell)
```

`BlobCell` carries entity kind, current type name, entity ID, and current property name. It supports nodes and edges symmetrically. The engine resolves the selector through one coherently captured catalog and snapshot, then carries stable table/incarnation and property identity internally.

A returned managed reader is pinned to the selected immutable dataset version. It never follows the branch after opening and remains `Send + Sync`. Each `read_range(start..end)` is half-open and limited to 4 MiB; callers stream larger values with repeated bounded reads. There is no public unbounded `read_all` or Lance `BlobFile` escape.

Managed metadata includes a strong ETag bound to the exact logical cell and immutable opened version. Null has no payload or ETag. External classification returns the stored descriptor without opening the target object.

### Historical identity fences

The current accepted catalog resolves selector aliases even when the target is historical. A type rename can therefore reach older table history through stable table identity. The old type alias is not retained.

Property aliases are not bridged across historical physical field names. A target before a property rename fails rather than reading a similarly placed field. Upgraded tables whose older schema lacks the persisted stable-property marker also fail closed for those older versions.

Named-branch reads require evidence for the selected native branch incarnation. The exact current branch head is readable; an older named-branch snapshot without a persisted incarnation witness is rejected to prevent delete/recreate ABA retargeting.

These are compatibility limits, not lookup fallbacks. A Blob reader must never silently select a different lifetime.

## Write-path admission

Bulk load and mutation inputs use these values:

- `null` for a null cell;
- `base64:<payload>` for managed bytes;
- another string for an external URI request.

New external references are denied by default. A configured policy permits only normalized URIs beneath declared bases. Credentials must not be persisted in URIs. Cluster serving projects only server-safe bases; `file://` may be useful to a deliberate embedded process but is never admitted by the HTTP server.

A base may not overlap OmniGraph storage. Ingress reads with the process's storage principal, so a base over a graph or cluster root would let a writer copy manifest, table, or ledger bytes into a managed cell that Cedar then serves as graph data. Three checks hold the rule, each against the roots it knows: cluster validation compares every base with the cluster storage root (`external_blob_base_overlaps_storage_root`), serve boot quarantines a graph whose applied server-safe base overlaps that root, and `Omnigraph::with_external_blob_policy` refuses a base overlapping the handle's own graph root. The comparison is by URI component in either direction, using the lexical and canonical forms of both sides (`ExternalBlobBase::ensure_disjoint_from_storage_root`). The root is classified by the storage layer's rule (`omnigraph_storage::storage_kind_for_uri`), so an absolute local path containing `://` is a local root, and a local root is made absolute by the storage adapter's own lexical fold (`omnigraph_storage::absolutize_lexically`), so `<dir>/link/../cluster` is compared at `<dir>/cluster`, where storage writes. A base whose scheme cannot name the root's storage kind (any base against an `az://` or in-memory root, an `s3://` base against a local root, a `file://` base against an S3 root) is disjoint without the root being rendered. A same-kind root that has no base-shaped form (an empty or percent-sign path component, an ancestor that fails to resolve for a reason other than absence) and a root in an unrecognized scheme fail closed as `StorageRootConflict::UncomparableRoot`, published as `external_blob_storage_root_uncomparable`; `StorageRootConflict::Overlap` is `external_blob_base_overlaps_storage_root`, and a base or policy that fails its own validation is `StorageRootConflict::InvalidPolicy`, never either code. Serve boot applies the quarantine only to a graph entry whose composite digest binds its policy; any other entry fails boot with `external_blob_policy_digest_mismatch`.

Admission is done after last-write-wins coalescing, so superseded input cannot trigger target I/O. Equivalent normalized references are probed and read once per bounded operation where materialization is required. URI metadata, selected reference count, and carried payload bytes all participate in pre-effect limits.

Storage mode determines ownership:

- full-table overwrite may retain an allowed external descriptor;
- keyed insert/upsert and row-writing merge materialize selected external bytes into managed values because the keyed Lance writer has no safe external-reference option;
- a pointer-only branch adoption keeps the existing dataset version and performs no source-object I/O.

An existing stored external reference remains classifiable even if current ingress policy would reject creating it, and exportable when it names its whole object (below). OmniGraph never deletes the referenced object.

A null assigned to a nullable Blob clears the cell; `resolve_assignments` refuses a null on a non-nullable one before the update opens its table. An update omits the Blob columns it assigns from its materializing scan (`scan_with_pending_materialized_blobs`'s `omit_blob_columns`): their old cells are never taken, authorized, probed, read, or charged to the byte budget. Every other Blob cell of a matched row is carried, which means reading it and rewriting it as managed bytes, because the keyed writer has no external-reference option. Carrying a stored external reference therefore passes the graph's policy; a refusal on that path is `OmniError::StoredExternalBlobDenied`, naming the type, id and property, distinct from `ExternalBlobPolicy`, which refuses caller input. Merge and new-input admission keep `ExternalBlobPolicy`.

A materializing rewrite (a carried update or merge cell, the bounded rewrite stream, a schema rewrite) reads the managed cells of one batch through `TableStore::managed_blob_payloads`, one `Dataset::read_blobs` call, never one `BlobFile::read` per row. It passes managed row ids only, because given an external row Lance resolves and reads the referenced object itself; external cells go through the preflight (keyed writes) or are carried as descriptors (schema rewrite). The read is streamed (`try_into_stream`, never `execute`) with an explicit `BLOB_REBUILD_IO_BUFFER_BYTES` (8 MiB) buffer, since Lance's default is 32 MiB times the store's I/O parallelism, and each value is checked against its descriptor length as it is consumed. Two reads keep `take_blobs`: the single-cell read facade (one stable row id) and export's `export_blob_column_values` (the managed rows of one descriptor batch).

A whole-object surface cannot carry a ranged external descriptor: `ExternalBlobRef::whole_object_uri` returns a `RangedExternalBlob` (whose display never includes the URI) for it, and the HTTP redirect, CLI delivery, schema rewrite and export all refuse it rather than widen it. Surfaces that are not reloaded describe it instead: `export_blob_values` takes `RangedExternalBlobs::{Refuse, Describe}` and returns `LogicalBlobValue::RangedExternal` under `Describe`, which `describe_ranged_blob_cells` writes as `{"uri", "offset", "length"}` with a positive length (the decoder below guarantees one; a missing length there is an internal error) into the row images of `logical_row_image` (change-feed images, entity reads) and the snapshot rows of the change-feed baseline, and change-row comparison compares exactly, so a ranged row never wedges a feed cursor or refuses a baseline. The baseline shares export's table walk and line format and passes `Describe` where export passes `Refuse`: it is the exact state its consumer starts from, and a refusal there would leave the graph with no baseline. The descriptor decoder refuses an external descriptor with an offset but size 0 as a Blob integrity error: Lance reads size 0 as the object's size but keeps the position, which would read past the object's end.

## HTTP and CLI delivery

`GET` and `HEAD /graphs/{graph_id}/blob` select one logical cell. Managed delivery supports one bounded range, strong conditional requests, and constant-memory backpressure. `HEAD` does not read payload bytes.

An external value produces a `302` redirect with the stored URI; the server never proxies or probes the target. Null, missing entity/property, unsatisfiable range, failed precondition, and integrity failure remain distinct typed outcomes.

The CLI exposes `blob get` and `blob stat` for embedded and remote graphs. `get` streams managed bytes to stdout or `--out`; `stat` returns kind, resolved-view metadata, size/ETag for managed data, or the descriptor for external data. The CLI refuses to follow external references.

There is currently no HTTP or CLI Blob put/clear surface.

## Maintenance and export

Export emits managed values as base64 and whole-object external values as URI descriptors; a ranged external descriptor is refused, because a bare URI reloads as the whole object; change-feed images, the change-feed baseline and entity reads describe it as `{uri, offset, length}`. Its scratch space is bounded at the row level.

`optimize` compacts Blob-bearing tables. The pinned Lance release must pass the substrate guard proving null, empty, non-empty, neighboring payloads, stable row IDs, and range reads survive fragment compaction. Lance materializes every managed payload of a compaction scanner batch, so `stage_compaction` executes the plan's tasks itself and derives each task's batch size from a descriptor-only scan of that task's fragments: the largest row's managed Blob bytes, summed over its Blob columns (external descriptors are carried unread and count nothing), so one batch materializes at most 32 MiB of managed payload. A row over 16 MiB puts its whole task on one-row batches, and a row over the budget is materialized whole. Every task of a Blob table gets the derived size explicitly (1 to 8192 rows, or a caller's smaller one), so it takes precedence over `LANCE_DEFAULT_BATCH_SIZE`, including when the variable is smaller. All tasks are sized before any executes, so when a fragment being compacted holds a descriptor the decoder refuses, sizing can refuse the compaction with a Blob integrity error before that table is rewritten, and nothing is committed; a table with nothing to compact is not scanned. This bounds payload per batch, not heap: Lance's writer copies inline payloads into its prepared arrays while the batch is held; see the compaction fence in [lance.md](lance.md#current-compatibility-fences). `cleanup` can reclaim old managed bytes with their dataset versions; callers must quiesce long-lived readers before destructive GC.

The storage upgrade's source validation (`db/upgrade.rs::validate_blobs`) scans only `_rowid` and the Blob descriptor columns and classifies each descriptor with the central decoder before any payload read. An external reference's URI is recorded in `work.external_blob_exclusions` as the descriptor stores it, without its byte range, with no store access (§4.1 of RFC 0033); a managed value is read back through `read_blob_ranges` in 1 MiB windows, streamed with the explicit rewrite I/O buffer, and a read Lance refuses or returns short fails the preflight (on a truncated file, Lance's own range error is what surfaces). The windows are planned lazily and read at most 64 per call, so a descriptor's recorded length never sizes an allocation: a corrupt one claiming `u64::MAX` bytes costs one chunk before that chunk's read fails. A non-v2 Blob column, a nested Blob field (checked even when the table has no top-level Blob column), or a descriptor the decoder refuses fails closed.

## Test owners

- Engine logical reads and ingestion: `crates/omnigraph/tests/end_to_end.rs`, `branching.rs`, and in-source Blob/table-store tests.
- Base and storage-root disjointness: in-source `blob.rs` tests (root forms and S3/local overlap), `end_to_end.rs::external_blob_policy_refuses_base_overlapping_graph_root`, and the `omnigraph-cluster` tests `external_blob_config_rejects_bases_overlapping_storage_root`, `external_blob_base_overlapping_storage_root_refuses_apply_over_existing_state`, `external_blob_config_reports_uncomparable_storage_root_under_its_own_code`, `serving_quarantines_applied_policies_overlapping_storage_root` (the classifier), `serving_snapshot_quarantines_graph_whose_applied_base_overlaps_storage_root` (the snapshot reader: quarantine with a healthy sibling, refusal when none is left, ledger unchanged), and `serving_snapshot_refuses_overlapping_policy_its_digest_does_not_bind`.
- Lance compatibility: `crates/omnigraph/tests/lance_surface_guards.rs`.
- Cluster policy persistence and serving projection: `omnigraph-cluster` in-source tests.
- HTTP transport: `crates/omnigraph-server/tests/data_routes.rs`, `auth_policy.rs`, and `openapi.rs`.
- CLI and embedded/remote parity: `crates/omnigraph-cli/tests/cli_data.rs` and `parity_matrix.rs`.

The user contract and examples live in [Blob values](../user/blobs.md).
