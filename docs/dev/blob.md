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

Admission is done after last-write-wins coalescing, so superseded input cannot trigger target I/O. Equivalent normalized references are probed and read once per bounded operation where materialization is required. URI metadata, selected reference count, and carried payload bytes all participate in pre-effect limits.

Storage mode determines ownership:

- full-table overwrite may retain an allowed external descriptor;
- keyed insert/upsert and row-writing merge materialize selected external bytes into managed values because the keyed Lance writer has no safe external-reference option;
- a pointer-only branch adoption keeps the existing dataset version and performs no source-object I/O.

An existing stored external reference remains classifiable and exportable even if current ingress policy would reject creating it. OmniGraph never deletes the referenced object.

## HTTP and CLI delivery

`GET` and `HEAD /graphs/{graph_id}/blob` select one logical cell. Managed delivery supports one bounded range, strong conditional requests, and constant-memory backpressure. `HEAD` does not read payload bytes.

An external value produces a `302` redirect with the stored URI; the server never proxies or probes the target. Null, missing entity/property, unsatisfiable range, failed precondition, and integrity failure remain distinct typed outcomes.

The CLI exposes `blob get` and `blob stat` for embedded and remote graphs. `get` streams managed bytes to stdout or `--out`; `stat` returns kind, resolved-view metadata, size/ETag for managed data, or the descriptor for external data. The CLI refuses to follow external references.

There is currently no HTTP or CLI Blob put/clear surface.

## Maintenance and export

Export emits managed values as base64 and external values as URI descriptors. Its scratch space is bounded at the row level.

`optimize` compacts Blob-bearing tables. The pinned Lance release must pass the substrate guard proving null, empty, non-empty, neighboring payloads, stable row IDs, and range reads survive fragment compaction. `cleanup` can reclaim old managed bytes with their dataset versions; callers must quiesce long-lived readers before destructive GC.

## Test owners

- Engine logical reads and ingestion: `crates/omnigraph/tests/end_to_end.rs`, `branching.rs`, and in-source Blob/table-store tests.
- Lance compatibility: `crates/omnigraph/tests/lance_surface_guards.rs`.
- Cluster policy persistence and serving projection: `omnigraph-cluster` in-source tests.
- HTTP transport: `crates/omnigraph-server/tests/data_routes.rs`, `auth_policy.rs`, and `openapi.rs`.
- CLI and embedded/remote parity: `crates/omnigraph-cli/tests/cli_data.rs` and `parity_matrix.rs`.

The user contract and examples live in [Blob values](../user/blobs.md).
