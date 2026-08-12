---
type: spec
title: "RFC-033 — Blob management"
description: A safe, snapshot-aware, engine-owned blob surface for node and edge cells, with bounded streaming reads, raw-byte replacement and clear operations, explicit external-reference policy, and correctness gates for empty blobs and maintenance.
status: draft
tags: [eng, rfc, blob, storage, api, security, streaming, maintenance, omnigraph]
timestamp: 2026-08-09
owner: OmniGraph maintainers
---

# RFC-033: Blob management

**Status:** Draft
**Date:** 2026-08-09
**Author track:** Maintainer design series
**Depends on:** RFC-022 unified writes, RFC-023 exact `id` fencing,
RFC-028 stable schema identity, internal manifest schema v6, and Lance 10.0.0
blob-v2 (`lance.blob.v2`) on file format V2_2.
**Surveyed:** OmniGraph `8d12b66703ac`; Lance 9.0.0 as the defect baseline;
and Lance 10.0.0 as the required implementation substrate.
**Audience:** engine, compiler, server, CLI, security, storage, maintenance,
and documentation maintainers.

This RFC is the required successor to an earlier reverted blob-delivery
experiment. The relevant evidence and decisions are restated here; no untracked
internal note is part of the contract, and nothing from the experiment is
restored merely by accepting this RFC.

---

## 0. Decision summary

OmniGraph will manage a Blob as one property cell on an existing node or edge.
The cell has three logical states: null, a managed byte sequence, or an external
reference. A managed byte sequence may be empty; null and zero bytes are never
interchangeable. Lance owns whether managed bytes are inline, packed, or in a
dedicated sidecar. OmniGraph owns graph identity, authorization, snapshot
selection, transport, boundedness, and atomic publication.

This RFC makes the following decisions:

1. Add one engine-owned, descriptor-first Blob facade. Public APIs do not expose
   `lance::dataset::BlobFile`, prepared blob structs, physical paths, or placement
   kinds.
2. Address both node and edge Blob cells by an explicit typed selector. Reads
   accept the existing branch-or-snapshot `ReadTarget`; writes target one public
   branch.
3. Add `GET` and `HEAD` delivery, bounded single-range reads, and strong
   validators for managed bytes. Add raw-byte `PUT` replacement and nullable-cell
   `DELETE` clear. Both writes are update-only and compose the existing Mutation
   writer and recovery/publication tail.
4. Add CLI parity through `blob get`, `blob stat`, `blob put`, and `blob clear`.
5. Make external-reference ingress an explicit trust decision. It is denied by
   default, allowed only beneath normalized configured bases, and never enabled
   merely by allowing a URI scheme. Server mode never accepts `file://` external
   inputs. Existing stored references remain readable.
6. Never proxy an external object through the HTTP server. A Blob read classifies
   the stored descriptor without opening the referenced store; HTTP returns a
   redirect and CLI reports the reference.
7. Reject Blob projection in `.gq` until the query result model has a real Blob
   descriptor value. The current behavior that substitutes null is silent data
   corruption at the API boundary and is removed.
8. Fix all OmniGraph empty-vs-null classification to use Arrow validity. The
   Lance 10.0.0 migration must land with an exact positive compaction guard for
   null, empty, non-empty, and neighboring payloads before this RFC is
   implemented. No production compaction skip ships. A future Lance bump whose
   guard is red is blocked unless that same change carries a fix or a tested,
   typed per-table skip. Correctness takes precedence over compaction throughput.
9. Do not add schema annotations for inline, packed, or dedicated thresholds.
   Those are physical derived state and the current Lance defaults are not an
   OmniGraph compatibility promise.

The design is deliberately a facade over Lance Blob v2, not a new blob store,
object catalog, lifecycle database, content-addressed layer, or garbage
collector.

## 1. Motivation

Blob is already an accepted schema type, but it is not currently a coherent
product capability. The write paths can ingest bytes and URIs and the export
path can reproduce them, while ordinary queries, embedded reads, server APIs,
CLI commands, maintenance, and trust boundaries disagree about what a Blob is.
The result is more dangerous than a missing feature because several existing
surfaces return plausible but false answers.

### 1.1 Current behavior

At the surveyed commit:

| Area | Current behavior |
|---|---|
| Logical schema | The compiler represents `Blob` as a `LargeBinary` placeholder. The engine replaces it with a Blob-v2 physical field when it builds a table schema. |
| Input | A `base64:` string becomes managed bytes. Every other string is treated as an external URI. |
| Overwrite | Lance `WriteParams` preserves external references with `allow_external_blob_outside_bases = true`. |
| Keyed insert/upsert and merge | Because `MergeInsertBuilder` has no `WriteParams` hook, OmniGraph sizes and materializes external payloads under the 32 MiB keyed-write ceiling. |
| Mutation update | Matching rows are rebuilt and all of their Blob payloads may be read and rewritten, including Blob properties the statement did not change. |
| Embedded read | `read_blob(type, id, property)` is node-only, current-branch-only, builds a SQL string for the `id` filter, and returns Lance's `BlobFile` directly. |
| `.gq` node projection | Blob columns are omitted from the Lance scan and reintroduced as nullable Utf8 columns containing null for every row. |
| `.gq` edge projection | Blob properties are excluded and typechecking rejects their use. |
| Export | Internal bytes are materialized and emitted as base64; external references are emitted as URIs. One row's full Blob set is indivisible scratch. |
| HTTP / CLI | There is no Blob delivery, stat, upload, or clear surface. |
| Maintenance | On the pre-migration Lance 9.0.0 baseline, `optimize` compacted Blob-v2 tables through the reachable empty-Blob defect described in §8.5. The OmniGraph 0.10 development line now pins Lance 10.0.0 and the exact fixed behavior. |

The public schema documentation also calls Blob storage `LargeBinary`. That is
only the compiler's logical placeholder. OmniGraph-created data tables use the
Blob-v2 extension struct on Lance V2_2; the documentation must distinguish
logical type from physical encoding.

### 1.2 Correctness gaps

Four defects require a design decision, not isolated patches:

1. **False null query results.** A valid non-null node Blob projected by `.gq`
   is currently returned as null. This violates the loud-integrity rule.
2. **Empty is classified as null.** Two OmniGraph descriptor helpers infer null
   from inline `position = 0`, `size = 0`, and empty URI. That is also the valid
   descriptor of a zero-length managed Blob. The mistake reaches export,
   updates, merges, and schema apply.
3. **Pinned-Lance compaction exposure.** Lance issue #7965 records an empty Blob
   being classified as null during compaction, with an additional Blob-v1 case
   that can damage neighboring payloads. `omnigraph optimize` reaches the Blob-v2
   path. Existing compaction tests contain only non-empty payloads and therefore
   do not close this case.
4. **Constraint validation is split.** Property-level `@unique` rejects Blob,
   but body-level `@unique(blob_property)` is accepted and fails only when the
   writer attempts to canonicalize the value.

### 1.3 Security and operability gaps

Treating every non-`base64:` string as an unrestricted URI lets a remote writer
ask a server to access `file://` paths or any object-store location available to
the server's credentials. Keyed modes actually read that object. This is a
server-side file/object read primitive, even if the bytes are later stored only
inside the caller's authorized graph.

Base64 is also a poor large-payload transport. It adds about one third to the
payload before JSON overhead, so a raw file that fits the 32 MiB write envelope
may not fit the served JSON body limit. There is no range delivery, no stat
operation, and no optimistic concurrency token for replacing one cell.

The long-run liability is the absence of one owner. Each new caller currently
has to rediscover descriptor validity, snapshot selection, external-reference
behavior, size accounting, and publication. This RFC centralizes those decisions
at the engine boundary and makes HTTP, CLI, export, schema apply, merge, and
maintenance consume the same meanings.

## 2. Goals and non-goals

### 2.1 Goals

- Correctly distinguish null, empty managed bytes, non-empty managed bytes, and
  external references across every engine path.
- Provide constant-memory delivery and byte-range access without exposing Lance
  types.
- Support current, branch, and historical snapshot reads with one coherent
  pinned view.
- Support node and edge Blob cells symmetrically.
- Replace or clear one existing cell through the ordinary graph commit,
  authorization, recovery, and audit mechanisms.
- Bound upload memory, download buffering, external probes, and any materialized
  row rewrite before a durable effect.
- Make the external-object trust boundary explicit and deny unsafe defaults.
- Preserve existing graph format and stored Blob-v2 data.

### 2.2 Non-goals

- A second object store, custom WAL, reference-count table, or Blob garbage
  collector.
- Upload sessions, multipart-resume tokens, or acknowledgement before graph
  publication.
- Automatic deletion, retention, replication, checksumming, or immutability of
  caller-owned external objects.
- MIME sniffing or persisted content-type metadata. V1 serves managed bytes as
  `application/octet-stream`.
- Blob comparison, search, indexing, keys, uniqueness, ordering, or ordinary
  query projection.
- User control over Lance's inline, packed-sidecar, dedicated-file, or pack-file
  thresholds.
- A public batch-Blob endpoint. Engine internals may use Lance's batched read
  APIs where doing so preserves the facade.
- Creating a missing node or edge as a side effect of uploading bytes.

## 3. Logical contract

### 3.1 Blob states

For one accepted-schema Blob property and one existing entity row:

```text
BlobCellState = Null
              | Managed { length: u64 }
              | External { uri: AbsoluteUri, offset: u64, length: Option<u64> }
```

The following rules are normative:

- `Managed { length: 0 }` is valid and is not `Null`.
- A null cell has no payload, size, range, redirect, or entity tag.
- Managed bytes are owned by the Lance dataset version. Lance may represent them
  inline, in a packed `.blob` sidecar, or in a dedicated `.blob` object without
  changing the logical state.
- An external object is owned by the URI's operator. OmniGraph stores a
  reference, does not include the object in graph cleanup, and cannot promise
  that its bytes remain stable.
- The public surface never reveals Lance's managed placement kind or physical
  path.
- V1 ingress creates whole-object external references only (`offset = 0`,
  unknown persisted length). The descriptor model carries a range so existing
  or future Lance values can be represented without changing the facade, but no
  public V1 command accepts an external offset/length.

Null detection uses only the parent struct's Arrow validity. A non-null struct
with a zero length is a non-null empty value. Field-value heuristics are
forbidden.

### 3.2 Cell selector

All Blob operations use one selector:

```rust
pub enum EntityKind {
    Node,
    Edge,
}

pub struct BlobCell {
    pub entity: EntityKind,
    pub type_name: String,
    pub id: String,
    pub property: String,
}
```

`type_name` and `property` are resolved through the handle's current accepted
catalog, captured coherently with the read view. The engine then carries stable
table identity, incarnation identity, and stable property identity internally.
A rename keeps those identities; drop/re-add mints a new lifetime. `table_key`,
alias, physical path, field position, and Lance version are not identity
substitutes.

Phase 1 resolves selector aliases against the handle's **current accepted
catalog**, even when the requested target is historical. After a pure type
rename, the current type alias can still select pre-rename table history because
the engine binds that alias to the selected snapshot by stable
table/incarnation identity. The previous type alias is not retained as a
compatibility alias and returns typed `BadRequest`. This is the table-identity
resolution rule; the property-name, property-lifetime, and native-branch
incarnation fences below still apply independently.

Phase 1 does **not** bridge the current property alias to a differently named
physical field in a pre-rename table version. The supplied property must be a
Blob in the current catalog and must exist under that spelling in the selected
physical schema. A target from before that property rename therefore returns
typed `BadRequest`; supplying the previous property alias is also rejected by
the current catalog. A future slice may add explicit stable-property-ID
crossing, but Phase 1 never guesses from field position or silently substitutes
the other spelling.

Physical user fields newly initialized, added, or schema-rebuilt by 0.10
persist their authoritative graph property lifetime as decimal metadata under
`omnigraph.stable_property_id`. A Blob read compares that value with the stable
property ID from the current accepted catalog. A mismatch is typed `BadRequest`:
an identically spelled field from a soft-drop/same-name-re-add is a different
property lifetime. A malformed marker is `BlobIntegrity`. Lance field IDs,
positions, and names are never graph identity.

Pre-0.10 v6 fields have no marker and are not rewritten in place. A
schema-preserving `LoadMode::Append`, `LoadMode::Merge`, or mutation write after
upgrade retains that unmarked schema. A full-table `LoadMode::Overwrite`
instead carries the 0.10 catalog schema and adopts the marker on its replacement
physical fields; it does not rewrite older versions. The compatibility exception
is deliberately narrow: a missing-marker read is allowed only when the selected
snapshot points at the exact current physical table entry. An older snapshot
fails `BadRequest` with `no persisted property-lifetime witness`, even when the
spelling matches, because it cannot prove that the property did not cross a
drop/re-add lifetime. The refusal also applies when no rename occurred. This
restriction needs no manifest-format bump or graph rebuild; a later 0.10 field
initialization, addition, schema rebuild, or full-table Overwrite carries the
marker for that new physical version.

All OmniGraph physical entity IDs are exact non-null Utf8. Lookup uses a typed
`col("id").eq(lit(id))` expression and the stable row ID selected from that
snapshot. Caller text is never flattened into SQL.

### 3.3 Read target and snapshot isolation

`read_blob_at` takes the same `ReadTarget` as query/export: exactly one branch or
snapshot, with the normal default to `main` at transport boundaries. It resolves
one manifest view and one exact table version. Catalog validation, row lookup,
descriptor classification, and payload access all use that immutable selected
view. Compatibility admission may read current branch authority only as a
comparison witness for the two fail-closed cases below; it never sources data
from that live view or retargets the selected table.

The exact physical manifest incarnation must also be provable. Resolving an
explicit snapshot reopens its persisted `(manifest branch, version)`; because a
deleted branch can reuse both, the reopened manifest's exact graph-head row must
still name the resolved graph commit. This first fence applies independently of
where the selected table is stored, so a named graph snapshot cannot retarget
through an inherited-main table. Genuine inherited-main history remains
eligible when that graph-snapshot proof succeeds.

A persisted object-store table-manifest e-tag is also compared at open, but it
is a coherence check, not a native table-branch-incarnation witness. V6
historical entries do not carry Lance's native `BranchIdentifier`. Phase 1
therefore bypasses the held table-handle cache for a named-native-branch table,
then cold-proves that the selected graph ref's effective current head still
equals the captured graph commit. The effective head is exact after the branch
owns a commit and inherited on a fresh fork. The comparison uses the zero-cache
control session rather than trusting the handle's warm read coordinator. A
concurrent ordinary advance may therefore make a branch-owned read fail loudly
rather than retarget, and an older explicit snapshot of a branch-owned table
fails typed `BadRequest` (`no persisted native-branch incarnation witness`).
Main table versions remain eligible after the independent graph-snapshot proof;
the property and schema fences still apply.

A returned reader keeps the snapshot/table handle needed to finish the read.
Advancing a branch after the reader is opened does not switch the payload under
that reader. Branch deletion and its physical tree reclamation are destructive
boundaries, just like `cleanup`: Phase 1 adds no durable reader lease or
cross-process live-reader registry. Callers that require an opened reader to
finish must quiesce it before deleting that branch or running version GC. A
reader never retargets, but if reclamation removes an immutable object it has not
yet cached, a later range read may fail loudly with a storage/integrity error. It
never switches to branch HEAD, another table version, or plausible partial
bytes.

### 3.4 Managed validator

Managed content receives a strong quoted ETag derived in the engine from:

```text
stable_table_id: u64_be
table_incarnation_id: u64_be
stable_property_id: u64_be
exact_table_version: u64_be
stable_row_id: u64_be
manifest_transaction_file_utf8_byte_length: u64_be
manifest_transaction_file_utf8: [u8; manifest_transaction_file_utf8_byte_length]
```

The hash input is exactly the ASCII domain separator
`omnigraph/blob-etag/v1\0`, followed without delimiters by the five identity and
version values above in that order, each encoded as one unsigned 64-bit
**big-endian** integer. Those five fixed-width values are followed by one more
big-endian `u64`: the UTF-8 byte length of the exact non-empty
`transaction_file` identity stored in the immutable opened Lance manifest. The
identity's exact UTF-8 bytes follow with no normalization, terminator, or
delimiter. The token is the lowercase hex of the first 16 bytes of SHA-256 over
that complete byte sequence, wrapped in double quotes. A single engine function
owns the encoding and a literal golden vector pins it; later server and CLI
phases delegate to that function rather than reconstructing it.

The normative golden vector is the five-`u64` tuple `(1, 2, 3, 4, 5)` plus the
exact transaction-file string `6-00000000000000000000000000000007.txn`. Its
UTF-8 byte length is 38 and its token is
`"f0e89bc86388accc9a7877df658a1f1c"`.

This validator is deliberately table-version-granular. An unrelated write to
the same table may invalidate a client's token even when the selected bytes did
not change. It is still a strong validator: equal tokens identify the same
immutable cell representation at an exact table version and immutable manifest
incarnation. The exact numeric table version plus the manifest's
transaction-file identity prevents a token from surviving native-branch
delete/recreate ABA without widening it to graph-snapshot granularity. A missing
or empty identity is `BlobIntegrity { reason }`; the engine never emits a weaker
token. The coarser table-granular behavior is documented rather than hidden.

External references receive no ETag. A stored URI descriptor does not prove the
current bytes of a mutable external object, and calling it a strong payload
validator would be false. HTTP redirects include `Cache-Control: no-store`.

## 4. Engine facade

The exact Rust names may move during implementation, but this capability split
and ownership boundary are normative:

```rust
pub const BLOB_READ_RANGE_MAX_BYTES: u64 = 4 * 1024 * 1024;

pub struct BlobRead {
    pub cell: BlobCell,
    pub resolved_target: ResolvedTarget,
    pub content: BlobContent,
}

pub enum BlobContent {
    Managed {
        length: u64,
        etag: BlobEtag,
        reader: BlobReader,
    },
    External(ExternalBlobRef),
}

pub struct ExternalBlobRef {
    pub uri: String,
    pub offset: u64,
    pub length: Option<u64>,
}

impl Omnigraph {
    pub async fn read_blob_at(
        &self,
        target: impl Into<ReadTarget>,
        cell: BlobCell,
    ) -> Result<BlobRead>;

    pub async fn put_blob_at_as(
        &self,
        branch: &str,
        cell: BlobCell,
        bytes: bytes::Bytes,
        precondition: Option<BlobPrecondition>,
        actor: &Actor,
    ) -> Result<BlobWriteOutcome>;

    pub async fn clear_blob_at_as(
        &self,
        branch: &str,
        cell: BlobCell,
        precondition: Option<BlobPrecondition>,
        actor: &Actor,
    ) -> Result<BlobWriteOutcome>;
}
```

Phase 1 implements the read types and `read_blob_at` only. The PUT/clear types
and methods shown above remain the normative Phase 3 shape; they are not exposed
early merely to reserve names.

`BlobReader` is an engine-owned `Send + Sync` abstraction with `len()` and
bounded `read_range(Range<u64>)`. Ranges are half-open and valid exactly when
`start <= end <= len`. Empty ranges are valid at every in-bounds position,
including `len..len`, and return empty bytes without payload I/O. A reversed or
out-of-bounds range returns typed
`BlobRangeNotSatisfiable { start, end, length }`. One successful call returns at
most `BLOB_READ_RANGE_MAX_BYTES` (4 MiB); a wider otherwise-valid range returns
`ResourceLimitExceeded` for resource `Blob read range bytes`, limit
`BLOB_READ_RANGE_MAX_BYTES`, before payload I/O. Callers read a larger managed
value through consecutive bounded ranges; there is no unbounded `read_all`
escape hatch.

An implementation may wrap Lance `read_blob_ranges`, `read_blobs`, or
`take_blobs`, but Lance types do not appear in public signatures.
Complete-payload internal work uses Lance's batched `read_blobs` API. It and
`read_blob_ranges` already shipped in Lance 9.0.0 with row-id, row-index, and
row-address selectors; Lance 10.0.0 is required for their null-preserving,
request-cardinality behavior. The hard rule is the anti-pattern: it must not
build a thread pool around one `BlobFile::read()` per row. Range work prefers
`read_blob_ranges` when it supports the needed selector.

### 4.1 Descriptor-first classification

The read path fetches the persisted descriptor and classifies it before opening
any payload reader. An external reference must be returned with zero I/O to the
referenced object store. In particular, the engine must not call a Lance helper
that creates an external store client or issues `HEAD` merely to determine
whether the value is external.

Descriptor decoding is one internal module used by read, export, mutation
rewrite, merge, schema apply, and maintenance tests. Duplicate
`blob_description_is_null` functions are removed. Unknown descriptor versions,
invalid child types, illegal base-relative references, arithmetic overflow, and
out-of-bounds descriptor ranges fail as typed `BlobIntegrity { reason }`, never
as `NotFound`, null, or an opaque Lance display string.

### 4.2 Read errors

- Unknown type/property or known non-Blob property: typed `BadRequest`.
- Unknown entity ID or null cell: typed `NotFound`.
- A persisted stable-property marker that names another lifetime, or a
  pre-0.10 historical field with no authoritative marker: typed `BadRequest`.
- An explicit snapshot whose reopened named manifest no longer carries the
  resolved graph commit: typed `BadRequest`, even when its selected table is
  inherited from main.
- A named-native-branch table whose selected graph ref no longer has the
  captured effective head when the post-open proof runs: typed `BadRequest`,
  even when a manifest e-tag exists. Genuine inherited-main table history
  remains eligible after the graph-snapshot proof; other property/schema checks
  may still refuse the read.
- A malformed persisted stable-property marker: `BlobIntegrity { reason }`.
- A Blob property whose descriptor is malformed:
  `BlobIntegrity { reason }`, never `NotFound` and never a null substitute.
- A managed table version whose immutable manifest has no non-empty
  `transaction_file` identity: `BlobIntegrity { reason }`; no validator or
  managed reader is returned.
- A range that violates `start <= end <= length`:
  `BlobRangeNotSatisfiable { start, end, length }`.
- An otherwise-valid range wider than 4 MiB: `ResourceLimitExceeded` for
  `Blob read range bytes`, before payload I/O.
- An empty in-bounds range, including `length..length`, returns empty bytes
  without payload I/O. Reading the full zero-length Blob as `0..0` succeeds.

### 4.3 Write semantics

`put_blob_at_as` replaces exactly one cell with managed bytes. `clear_blob_at_as`
sets exactly one nullable cell to null. Both require an existing entity row; they
never insert a row. Clear on a non-nullable property is `BadRequest`.

The raw managed payload limit is 32 MiB inclusive. The engine rejects a larger
value before any staged fragment, transaction, sidecar, manifest update, or
lineage row. The existing writer may need to materialize other Blob cells on the
same row in order to carry the row through Lance. Those bytes remain charged to
the same operation-wide 32 MiB pre-effect budget. Therefore a near-limit target
can be refused when the row has other large Blob cells. This is an explicit V1
limitation, not hidden behavior.

The target cell's old payload is not read or charged before replacement. A
replacement is declarative and retryable: after an ordinary pre-effect conflict,
the writer captures a fresh base, re-locates the row, re-evaluates the
precondition, and rebuilds the attempt. It does not replay a stale read-modify-
write plan.

`BlobPrecondition` is transport-neutral:

```rust
pub enum BlobPrecondition {
    AnyExisting,
    Tags(Vec<BlobEtag>),
}
```

`AnyExisting` means that a non-null Blob representation exists at the pinned
write base; either managed or external satisfies it. An existing entity row with
a null Blob cell does not. `Tags` uses strong comparison and matches only the
managed token at the pinned write base. A null or external cell has no token and
cannot match a tag. `PreconditionFailed { current_etag: Option<BlobEtag> }` is a
typed successful decision, mapped to HTTP 412, rather than a substrate failure.

After publication, a successful managed write derives the validator from the
published table version, its exact immutable-manifest `transaction_file`
identity, and the resulting row ID. The result is the same ETag a subsequent
GET at that head returns.

### 4.4 Publication and recovery

Blob replacement and clear are not new writer kinds. They enter the current
Mutation preparation path and use its existing validation, exact per-table
transaction, recovery-v9 sidecar, graph lineage, and one manifest CAS. The
shared publish tail remains one call site. The writer:

1. normalizes and rejects internal branch names;
2. enforces Cedar `change` for the actor and branch;
3. crosses the ordinary recovery barrier and captures one write base;
4. resolves stable schema/table/property identity and exact row ID;
5. evaluates the precondition and prepares one-row replacement state under the
   normal row/byte ceilings;
6. commits through `SidecarKind::Mutation` and publishes once; and
7. maps any post-arm uncertainty to `RecoveryRequired`.

No per-table publish, direct Lance commit, server-only writer, or Blob-specific
recovery record is permitted. `forbidden_apis.rs` classifies the public Blob
write methods as the existing Mutation protocol and keeps durable calls in the
shared helper.

## 5. HTTP surface

The server adds one multi-graph route:

```text
/graphs/{graph_id}/blob
```

All methods require query parameters:

```text
entity=node|edge&type=<type>&id=<id>&property=<property>
```

### 5.1 `GET` and `HEAD`

Reads additionally accept `branch=<name>` or `snapshot=<commit>`, never both;
the transport default is `branch=main`. Authorization uses the existing `read`
action and the same snapshot-to-policy-branch resolution as `/read`.

For managed content:

- `200 OK` with `Content-Type: application/octet-stream`, exact
  `Content-Length`, `Accept-Ranges: bytes`, and the strong `ETag`;
- one RFC 9110 byte range (`start-end`, `start-`, or `-suffix`) returns `206`
  with `Content-Range`;
- an unsatisfiable range returns `416` with `Content-Range: bytes */N`;
- multiple ranges are not implemented in V1 and cause Range to be ignored, so
  the complete representation is returned;
- `If-None-Match` uses weak comparison over an entity-tag list and supports `*`;
- `If-Range` accepts one strong validator; mismatch ignores Range and serves the
  complete representation; and
- HEAD produces the same status and headers without reading payload bytes.

Payload chunks are at most 4 MiB and are pulled under transport backpressure.
At most two chunks are retained for one response. Disconnect drops the reader
and its snapshot pin promptly. No implementation may call `read_all()` before
starting a response.

For external content, GET and HEAD return `302 Found`, the stored absolute URI
in `Location`, and `Cache-Control: no-store`. The server performs no `HEAD`, GET,
credential lookup, signing, proxying, MIME sniffing, or range translation
against that URI. There is no ETag or asserted length. A client that needs a
managed copy uploads the bytes through `PUT`; V1 does not add a server-side
copy-from-URI endpoint.

Unknown entity/null maps to 404; non-Blob property or invalid target maps to
400; authorization retains the existing 401/403 behavior; range failure maps to
416; recovery/integrity failures retain their existing typed mappings.

### 5.2 `PUT`

PUT accepts a branch only (default `main`); `snapshot` is invalid. The body is
raw `application/octet-stream`, not JSON or base64. Its route-specific body
limit is 32 MiB inclusive. The server authorizes `change`, acquires the ordinary
per-actor write admission sized by the retained body, then calls the engine.
Payloads over the transport or engine limit return 413 before a graph effect.

`If-Match` is optional. The boundary parses `*` as `AnyExisting` and a comma-
separated entity-tag list as `Tags`; weak tags do not participate in strong
comparison. Header syntax stays in API-types/server code, not in the engine.

Success returns `200`, the new ETag header, and a typed JSON result containing
the selector, branch, size, ETag, commit ID, and server-resolved actor ID. Missing
row returns 404, invalid property/target returns 400, precondition failure returns
412 with the current managed ETag when one exists, and admission saturation
returns 429 with the ordinary retry metadata.

### 5.3 `DELETE`

DELETE has the same selector, branch, authorization, admission, and optional
`If-Match` semantics as PUT. It clears a nullable cell and returns `204 No
Content`. Clearing an already-null cell is idempotent and produces no graph
commit; a matching entity row is still required. A non-nullable Blob returns
400. A successful clear of managed content makes the previous ETag stale.

The server OpenAPI describes the binary PUT body, all query parameters,
redirect, range, conditional, 412, 413, and 416 responses. The checked-in
`openapi.json` is regenerated in the implementing change.

## 6. CLI surface

The CLI adds:

```text
omnigraph blob get   ENTITY TYPE ID PROPERTY [--branch | --snapshot] [--out PATH]
omnigraph blob stat  ENTITY TYPE ID PROPERTY [--branch | --snapshot] [--json]
omnigraph blob put   ENTITY TYPE ID PROPERTY [--branch] [--file PATH] [--if-match TAG] [--json]
omnigraph blob clear ENTITY TYPE ID PROPERTY [--branch] [--if-match TAG]
```

`ENTITY` is `node` or `edge`. Graph selection uses the ordinary `--store`,
`--server` + `--graph`, `--cluster`, `--profile`, alias, and operator-default
rules.

- `get` streams to stdout unless `--out` is supplied. `--offset` and `--length`
  map to one range; length zero is rejected because it is not representable as
  an HTTP byte range. The embedded arm uses repeated bounded `read_range` calls.
- `stat` performs descriptor work only. Managed output includes kind, size,
  ETag, and resolved target. External output includes kind and URI but no size or
  ETag. Null is a typed not-found result.
- `put` reads one file or stdin, never both. Input is retained only within the
  32 MiB envelope and passed as raw bytes. It never base64-encodes the payload.
- `clear` asks for confirmation only according to the CLI's existing
  destructive-operation rules; it does not require the graph-cleanup `--confirm`
  flag because this is an ordinary audited mutation, not physical GC.

The remote client disables automatic redirects for Blob calls. A 302 becomes a
typed external-reference result carrying the URI; the CLI must not unexpectedly
download a caller-owned object or fail on `s3://`/`file://` redirect schemes.

Embedded and remote arms share API output types, If-Match parsing, validator
formatting, range rules, and error text. The parity matrix covers managed full
and range reads, stat, snapshot reads, PUT, clear, stale preconditions, missing
rows, external references, zero bytes, and oversize refusal. No Blob-specific
known divergence is accepted.

## 7. External-reference policy

External references are useful for large existing object collections, but they
cross a credential boundary. The following policy applies to every input path:
load, ingest, mutation parameters, export/rebuild import, embedded SDK, CLI, and
HTTP.

### 7.1 Default and configuration

New external-reference ingress is denied unless the graph is opened with an
explicit policy:

```rust
pub enum ExternalBlobPolicy {
    Deny,
    Allow { bases: Vec<ExternalBlobBase> },
}
```

Cluster configuration exposes the same policy per graph. The default is
`deny`. An allowed base contains an absolute URI prefix and an execution scope:
server-safe or embedded-only. Configuration validation normalizes it once,
rejects URI user-info and query/fragment credentials, and rejects overlapping or
ambiguous encodings.

Every raw configured base and input URI is capped at 64 KiB inclusive before
trimming, URL parsing, percent decoding, or filesystem resolution. This bounds
normalizer scratch independently of the operation-wide retained-metadata
budget; a one-over value returns the typed `external Blob URI bytes` resource
limit without source I/O.

Containment compares normalized scheme, authority, and path components; it is
not string `starts_with`. A scheme allowlist without a base is insufficient.
Server-safe bases may use only storage schemes supported by the shared object-
store registry and may never use `file://`. Embedded-only bases may opt into an
exact local directory because the process principal and caller are the same
trust domain.

The engine receives the policy at graph open, so correctness and security do not
depend on HTTP code. Server, CLI, and embedded callers cannot set a per-request
escape hatch. Cedar still decides who may change the graph; external-base policy
decides which server resources any authorized writer may reference. Both gates
must pass.

Phase 0B exposes allow-policy authority through applied cluster state and the
embedded builder, not through a direct-store CLI flag. A direct CLI graph open
therefore remains default-Deny. CLI ingestion of an external reference targets a
cluster-served graph with configured bases; rebuild tooling with direct storage
can import managed `base64:` values, while a rebuild containing external URIs
must use that served route or an embedded handle with the graph policy installed.
This is a deliberate trust-boundary choice, not a temporary per-request bypass.

### 7.2 Ingest behavior

The current accepted JSON spelling remains compatible:

- `base64:<payload>` means managed bytes;
- any other string requests an external URI and is rejected unless the external
  policy allows its normalized base.

This RFC does not add a second persisted logical struct. The canonical logical
input into Lance remains exactly `{ data: LargeBinary, uri: Utf8 }`. Prepared
`{ kind, position, size, ... }` descriptors are storage-internal and rejected at
OmniGraph input boundaries. Persisting an alternate child shape previously
poisoned later keyed writes; the boundary assertion remains mandatory.

New logical Mutation/Load input and every row-writing BranchMerge build one
operation-wide source-admission plan before the first durable effect. Probes use
the graph's shared object-store registry, deduplicate identical normalized URIs,
and run with at most eight concurrent metadata requests. Scalar table
preparation may precede this plan; admission still completes before any external
payload read, recovery arm, target HEAD or branch-ref movement, or graph-visible
effect. A predicate mutation that carries an existing persisted external
descriptor discovers it through bounded materialization scans: equivalent
spellings deduplicate within each scan batch, but a source may be probed again
in a later batch. A malformed, disallowed, missing, or unreadable object fails
loudly before its batch can stage a payload-bearing effect. Cost tests include
external references so the implementation cannot regress to a new registry and
sequential HEAD per row.

All new logical Mutation/Load modes, including Overwrite, refuse more than
8,192 external-reference cells across the complete multi-table graph operation.
This is an external-source admission bound, not the keyed-write row ceiling:
Overwrite may contain more than 8,192 logical rows when no more than 8,192 of
their Blob cells are external. The admission plan also refuses more than 32 MiB
of retained raw/normalized URI metadata before issuing a HEAD.

BranchMerge's descriptor-only first pass refuses more than 8,192 selected
external-reference cells or more than 32 MiB of retained raw/normalized URI
metadata before issuing a HEAD. It preserves each descriptor's offset and
length, then charges exact selected ranges together with managed Blob lengths
against one 32 MiB carried-payload budget. It never retains an operation-wide
payload cache.

Storage semantics remain explicit while Lance lacks a keyed-write `WriteParams`
hook:

- overwrite preserves an allowed external reference under the exact normalized
  URI proven by preflight; embedded `file://` symlink spellings are replaced by
  their admitted canonical regular-file target before persistence, so
  retargeting the discarded alias cannot change the cell. The retained URI is
  still a pathname, not an inode lease: embedded callers opt into a trusted,
  stable local namespace, and replacing the canonical leaf or an ancestor can
  redirect a later read under that same process principal;
- strict insert, upsert, load append/merge, mutation update, and a
  HEAD-advancing branch merge that writes selected rows pre-size and copy the
  allowed bytes into managed Blob storage under the operation's 32 MiB
  aggregate ceiling;
- a pointer-only branch fast-forward writes no row and preserves the existing
  descriptor without policy approval or source I/O.

The URI therefore names a source; write mode determines whether the resulting
cell remains a reference. This is existing observable behavior and remains
documented. If Lance later lets keyed writes preserve external references, a
separate RFC must decide whether changing this contract is worth the migration
and parity cost.

### 7.3 Existing references and disclosure

The new ingress policy does not make existing graphs unreadable. Stored external
references can be opened, exported, inspected, and redirected even when new
ingress is denied. This separation permits an operator to freeze reference
creation without losing access to historical data.

Read authorization permits observing the selected Blob cell, including its
stored URI. Operators who consider bucket/key names or paths sensitive must
apply policy at the graph/branch boundary accordingly. The server never dereferences
the URI on behalf of a reader.

OmniGraph never deletes an external target during update, clear, entity delete,
branch delete, optimize, cleanup, or graph removal. It owns only the stored
descriptor.

## 8. Query, schema, export, and maintenance behavior

### 8.1 Query language

V1 `.gq` cannot return or aggregate a Blob-valued expression. Typechecking
rejects direct projection, ordering, every aggregate whose argument is Blob
(including `count`, because nullness is part of its answer), and edge-property
projection with the stable `T24` diagnostic that points to the Blob management
surface. Existing comparison, match, and mutation-predicate diagnostics also
reject Blob operands. No rejected shape may execute and substitute null.

Blob assignment from a string remains available only through existing mutation
semantics and the external-reference policy. Raw bytes use `blob put`; there is
no base64 requirement in `.gq` added by this RFC.

A future query-level descriptor such as `blob_meta($n.payload)` would be a new IR
concept and requires its own design. It must not arrive as another physical
struct or Utf8 side channel.

### 8.2 Schema constraints

Blob remains ineligible for `@key`, `@unique`, `@index`, and `@embed`, whether
the annotation is property-level or body-level and whether the property belongs
to a node or edge. New-schema validation rejects the constraint before catalog
acceptance. A narrow accepted-contract parser still recognizes a body-level
Blob `@unique` already persisted by an older v6 binary so the graph can open for
inspection and export; it does not admit that shape through init/schema apply or
make the constraint executable. No schema-format bump is required; this closes
a parser/validator hole without stranding a historical root.

### 8.3 Export and rebuild

Export uses the central decoder. Null emits JSON null, managed zero bytes emits
`base64:` with an empty payload, non-empty managed content emits base64, and an
external reference emits its URI. Export's current one-row indivisible Blob
scratch and chunked transport limits remain documented; the Blob GET endpoint is
the preferred way to move a single large payload without base64 expansion.

Export/rebuild continues to preserve external references because the documented
rebuild loads with overwrite. The target's external policy must allow those
references. A denied reference fails the new target before effect rather than
silently materializing or dropping it.

### 8.4 Mutation, merge, and schema apply

Every path that carries an existing Blob cell uses the central descriptor
decoder and accounts `BlobReader::len()` before payload allocation. Zero-length
managed values participate as values. A nullable cell alone is skipped.

Schema apply carries a whole-object external descriptor without probing or
reading its target. Lance's current logical Blob input cannot express an
existing external offset/length range, so schema apply refuses such a valid
ranged descriptor before recovery arm or table movement; it never silently
widens the cell to the whole object. Supporting descriptor-preserving ranged
schema rewrites remains part of the future ownership-proof optimization below.

The current materializing rewrite is accepted as a bounded V1 implementation,
not as an ideal physical plan. A future optimization may carry immutable
prepared descriptors for unchanged cells only after a Lance surface guard proves
that references cannot escape their source dataset/incarnation and recovery can
account for every file. Correctness and ownership proof come before avoiding the
copy. Lance 10's `merge_insert` support for sources that omit Blob columns
(upstream #7615) removes a schema-level blocker for that shape but does not by
itself avoid the rewrite: Lance's merge path still materializes and re-writes
target Blob values when rows move, so the unchanged-cell optimization still
requires a descriptor-preserving or column-patching proof.

### 8.5 Optimize and cleanup

The Lance 9.0.0 line contains a reachable empty-Blob compaction gap
(lance#7965): `is_inline_null_blob` classifies a prepared inline descriptor
with `position = 0, size = 0` as null, which is also the valid descriptor of a
zero-length managed Blob that leads its fragment. The exact reproducer —
non-empty bytes and null in one fragment, then a valid empty Blob *leading* a
second fragment followed by non-empty bytes; compact; verify value, nullness
(via Arrow validity, since the 9.0.0 selection APIs omit null selections rather
than returning one result per request), and neighbor payloads — was run as
scratch decision evidence on 2026-08-10: **red on Lance 9.0.0** (the empty Blob
is nullified; non-null count drops 3 → 2) and **green on Lance 10.0.0**. This
dated scratch result is not acceptance evidence; the checked-in guard required
below is authoritative. The nullifying shape is reachable through
`omnigraph optimize` when a Blob-v2 table contains that valid-empty fragment
layout and those fragments are compacted.

The OmniGraph 0.10 development line implements the Lance 10.0.0 prerequisite.
The migration promotes the exact reproducer into
`lance_surface_guards.rs::compact_files_succeeds_on_blob_columns` as the
positive pin for empty/non-empty/null/neighbor preservation through
`compact_files`; no production compaction skip ships. A future Lance bump that
turns the guard red must not land until that same change either carries a
proven fix or introduces a tested per-table skip that leaves the Blob table
HEAD unchanged and reports a typed reason. The v10 fix covers Lance's
compaction and zero-length read implementation, not OmniGraph's independently
duplicated descriptor heuristics. Those still misclassify inline
`0/0/empty-uri` as null, so the §4.1 central Arrow-validity decoder work remains
required in full.

Cleanup remains Lance-owned version GC under OmniGraph's manifest/ref/recovery
floors. It can remove managed Blob sidecars only when Lance proves they are no
longer reachable from retained dataset versions. It never interprets or deletes
external URIs.

## 9. Physical layout and Lance alignment

OmniGraph uses Blob-v2 fields only on V2_2 datasets. The canonical logical input
and prepared physical descriptor are distinct contracts. Append/schema evolution
must retain the exact extension metadata Lance assigned at table creation.

In the surveyed Lance 9.0.0 and 10.0.0 Rust implementations, the defaults are
64 KiB inline, 4 MiB dedicated, and 1 GiB per packed sidecar. Later versions may
use different defaults. OmniGraph does not expose or restate these as user
promises. A graph stores the chosen field metadata, and Lance rejects
incompatible metadata on later appends.

No `.pg` annotation is added for these thresholds. Exposing them now would turn
physical tuning into persisted accepted-schema behavior and would require
schema-planner, rename, migration, export/import, and compatibility rules without
evidence that users need the control. If production metrics later show placement
as a material cost term, a follow-up RFC can propose an annotation with migration
semantics and a comparative benchmark.

Batch complete reads use `Dataset::read_blobs`; batch range reads use
`Dataset::read_blob_ranges`; lazy single-cell reads may use `take_blobs` behind
the engine facade. Logical row IDs are preferred within an exact snapshot.
Physical row addresses never become public stable identity.

## 10. Security and resource model

| Risk | Required control |
|---|---|
| Server-side file/object read through URI input | Default-deny engine policy, exact normalized bases, no server `file://`, no per-request override |
| URI credential disclosure | Reject user-info/query/fragment credentials at config and input; return URI only to an authorized Blob reader |
| URI parser amplification | Reject a raw configured or input URI above 64 KiB before trimming, parsing, decoding, or filesystem resolution |
| External SSRF during read | Descriptor-first classification; redirect only; no proxy or validation on GET/HEAD |
| Oversize upload | Route and engine 32 MiB inclusive limits; refusal before effect |
| Rewrite amplification | New logical input and row-writing branch merge pre-size all carried Blob payloads under one 32 MiB operation budget before read; predicate mutation carry applies the same cumulative byte ceiling while materializing bounded scan batches |
| External-source planning | Row-writing branch merge admits at most 8,192 external-reference cells and 32 MiB of retained URI metadata before HEAD; probes are bounded and normalized aliases deduplicate within the applicable operation or scan-batch envelope |
| Engine read memory | `BlobReader::read_range` returns at most `BLOB_READ_RANGE_MAX_BYTES` (4 MiB); larger values require consecutive calls and there is no unbounded full-read method |
| Delivery memory | Phase 2 adds a two-chunk queue, backpressure, and prompt cancellation without weakening the engine's 4 MiB per-call bound |
| Range arithmetic | Half-open `start <= end <= length`; empty-at-end is valid; reversed/out-of-bounds ranges are typed and carry the logical length |
| Stale overwrite | Optional strong `If-Match`, evaluated at each freshly pinned attempt |
| Actor spoofing | Existing server-resolved actor and engine-wide Cedar enforcement |
| Partial graph visibility | Existing Mutation sidecar and one manifest publication door |
| External-object deletion | Never performed by OmniGraph |

The workload-admission system treats PUT and DELETE as writes. GET/HEAD are
read-only, but their transport buffering is separately bounded. If measurements
show that long-lived Blob streams can starve ordinary reads, adding a read-stream
permit is an operational change that may be made without changing byte or
snapshot semantics; it still needs a cost/latency test before becoming a
default.

## 11. Error and observability contract

New errors use existing structured families where possible. The public contract
depends on typed code and fields, not an opaque Lance string.

| Condition | Engine class | HTTP |
|---|---|---|
| Unknown type/entity or null cell | `not_found` | 404 |
| Non-Blob property, invalid selector, non-nullable clear | `bad_request` | 400 |
| Branch and snapshot together / snapshot on write | `bad_request` | 400 |
| Disallowed or malformed external URI | `bad_request` with policy reason | 400 |
| External source missing/unreadable | typed external source error | 424 Failed Dependency; never generic 500 |
| Upload/rewrite budget exceeded | `resource_limit` with limit/observed | 413 |
| Managed read range exceeds 4 MiB | `resource_limit` for `Blob read range bytes` | existing server mapping is 413; no HTTP Blob route in Phase 1 |
| Reversed or out-of-bounds range | `BlobRangeNotSatisfiable { start, end, length }` | exhaustive server mapping is 416; no HTTP Blob route in Phase 1 |
| If-Match failed | `PreconditionFailed` outcome | 412 |
| Recovery intent armed but completion uncertain | `recovery_required` | existing mapping |
| Persisted table/Blob integrity contradiction | `BlobIntegrity { reason }` | exhaustive server mapping is 5xx; no HTTP Blob route in Phase 1 |

Instrumentation records operation, entity kind, managed/external/null
classification, requested and served byte count, range/full mode, precondition
result, admitted external cells, actual metadata-probe attempts, successful
external payload reads, and time-to-first-byte. It never logs payload bytes,
bearer tokens, URI credentials, or complete sensitive URIs.
URI metrics use scheme plus a keyed/irreversible base identifier.

## 12. Test and acceptance plan

The implementation extends existing owners before creating new fixtures, per
`docs/dev/testing.md`.

### 12.1 Compiler

- Extend parser/typecheck tests to reject body-level `@unique` containing a Blob
  on nodes and edges.
- Extend query typecheck tests to reject direct and aggregate Blob projection
  instead of producing a result schema.
- Keep existing key/index/embed/comparison/match refusals green.

### 12.2 Engine

- Phase 1 extends `end_to_end.rs`'s existing Blob fixture rather than creating a
  second graph: managed node and edge reads; null versus valid-empty versus
  non-empty; exact-ID metacharacters; full, sub-, empty, reversed, out-of-bounds,
  exact-4-MiB, and one-over range requests; typed missing/non-Blob,
  `BlobRangeNotSatisfiable`, and exact-resource limit errors; current-branch
  freshness; and external descriptor classification after the target object is
  unavailable.
- `blob_read_on_upgraded_unmarked_v6_table_fails_closed_for_old_snapshots`
  pins the compatibility transition: schema-preserving Append retains the
  unmarked schema and makes the prior entry ineligible, while full-table
  Overwrite adopts the 0.10 catalog marker on its replacement fields.
- The `src/blob.rs` owner pins the ETag byte grammar with a literal golden vector,
  `BlobReader: Send + Sync`, typed `BlobIntegrity` for a missing/empty immutable
  manifest transaction-file witness, and the existing malformed-descriptor
  matrix.
  Integration coverage proves repeating the same exact table-manifest/cell read
  means the same token, an unrelated write to that table changes it, and a
  historical exact manifest admitted by the independent fences keeps its
  original token; range coverage owns the inclusive 4 MiB boundary.
- Phase 1 extends `branching.rs` for explicit named-branch/snapshot reads and a
  reader that stays on its captured bytes after branch advance or logical row
  deletion. Branch deletion/reclamation remains a destructive boundary: no test
  or API promise requires an uncached later range to survive it. The same owner
  pins the v6 boundary: a warm-bound fresh child is admitted through its
  inherited effective head; a recreated named-native table cannot reuse a held
  local handle; an older branch-owned snapshot fails `BadRequest`; and genuine
  inherited-main history survives ordinary advance while a same-version graph
  ref recreation is refused during snapshot authentication. A manifest e-tag is
  not an exception. The failpoint suite parks a live branch read after graph
  capture, replaces the branch, and proves the same refusal.
  `schema_apply.rs` proves a current renamed type/property selector on the
  current target and the Phase 1 `BadRequest` boundary at its pre-property-
  rename snapshot. Its existing soft-drop fixture additionally drops and
  re-adds the same Blob property name, then proves the old snapshot is refused
  as a different stable-property lifetime.
- Phase 1 migrates `export.rs`'s four-way null/empty/non-empty/external fixture to
  the facade. External classification stays descriptor-first while the target is
  unavailable; bulk export itself retains its batched reader and must not loop
  over the single-cell API.
- `forbidden_apis.rs` removes the old `read_blob -> BlobFile` surface, classifies
  `read_blob_at` as read-only, and proves no durable call site was added.
- Destructive-reclamation acceptance is explicit: branch/ref retention remains
  covered by `maintenance.rs`, but Phase 1 introduces no cross-process
  live-reader lease. The reader never retargets; operators quiesce readers before
  cleanup or deletion of their branch, and an uncached read raced with physical
  reclamation may only fail loudly.
- Phase 3 extends the same `end_to_end.rs` fixture with update-only PUT, nullable
  clear, returned-ETag equality, and fresh/stale/`*` preconditions, including
  `If-Match: *` failing for a null cell.
- `writes.rs`: inclusive and +1-byte PUT bounds; aggregate carried-row bound;
  target old payload is not read; every refusal proves table HEAD, manifest,
  lineage, and sidecar state unchanged.
- Add a deterministic retry race using the existing Mutation rendezvous: pause a
  conditional PUT after its first precondition evaluation but before effect,
  publish a competing replacement, then resume. The first request must
  re-evaluate against the fresh base and return 412 with the winner's ETag,
  without a lost update or an extra table, manifest, lineage, or sidecar effect.
- Later merge/mutation work in `branching.rs` retains merge preservation of
  empty bytes, operation-wide managed-plus-exact-range accounting, the 8,192
  external-cell and 32 MiB URI-metadata admission bounds, normalized probe
  deduplication, chunk-bounded payload reuse, and pointer-only no-I/O adoption.
- Existing schema-apply coverage gains an empty Blob and a neighboring non-empty
  Blob rather than adding a duplicate initialization fixture.
- `failpoints.rs`: extend the existing Mutation recovery matrix with Blob PUT and
  clear cells that stop after the table effect but before manifest publication;
  reopen and prove graph visibility and the expected completed-recovery audit
  record with no unresolved sidecar. PUT proves exact bytes and an ETag equal to
  a fresh read. Clear proves null/NotFound with no ETag and proves the old ETag is
  stale; the injected call itself returns no successful write outcome.
- Phase 3 registers new writes under Mutation in `forbidden_apis.rs`; no new
  durable call site appears.

### 12.3 Lance and maintenance guards

- The Lance 10.0.0 migration owns an exact `lance_surface_guards.rs` reproducer:
  non-empty and null in one fragment; valid empty leading the next fragment and
  followed by a non-empty neighbor; `compact_files`; then exact Arrow validity,
  bytes, cardinality, and neighbor integrity. The dependency bump may not land
  without this green guard.
- For every Lance selection API the implementation actually uses (`take_blobs`,
  `read_blobs`, or `read_blob_ranges`), surface guards pin request order and
  duplicates, one logical result per request, null as `None`, valid empty as a
  non-null empty result, and typed failure for an unknown or deleted stable row
  ID. An unused API need not become part of OmniGraph's contract.
- Extend the existing `maintenance.rs` Blob optimize test with the same
  empty-leading-fragment layout. Assert exact payloads and Arrow validity plus
  one atomic graph publication for the Blob and plain tables; row count alone is
  insufficient.
- `cleanup` retains a managed sidecar reachable from a snapshot/ref and never
  attempts deletion of an external URI. A process-local `BlobReader` is not a
  durable ref and does not widen cleanup's cross-process contract: readers must
  be quiesced before destructive GC; a raced read may return its captured bytes
  or fail loudly, but never switch versions.

### 12.4 Server, CLI, parity, and cost

- `data_routes.rs`: GET/HEAD headers; empty 200; ranges and conditionals; external
  302 with zero external I/O; branch/snapshot validation; PUT/DELETE; 412/413;
  node and edge selectors; authorization and actor attribution. A payload-read
  probe must remain at zero for managed HEAD on both empty and near-limit values.
- `openapi.rs`: regenerate and compare the binary request/response surface.
- `cli_data.rs`: stdin/file PUT, stdout/file GET, stat, clear, ranges, snapshots,
  external no-follow behavior, JSON output, and stable failure text.
- `parity_matrix.rs`: byte and structured-output equality for every CLI verb and
  the failures listed in §6.
- `write_cost.rs` and its S3 owner: fixed external-reference counts prove shared
  registry reuse, URI deduplication, bounded probes, and no per-row cold setup.
  The live S3 cell uses 64 normalized-equivalent references and requires exactly
  64 admitted cells, one metadata request, and one payload read.
- Extend the bounded transport/backpressure owner with a paused-client test that
  records at most two retained chunks and at most 8 MiB of retained payload for a
  near-limit managed Blob. Disconnect must drop the reader and snapshot pin.
  These deterministic counters, rather than a platform-noisy RSS number, are the
  acceptance gate.

Every phase requires the canonical workspace test graph, its relevant focused
suites from a clean baseline, and `scripts/check-agents-md.sh`. Phase 1 adds no
HTTP route, CLI verb, HTTP wire DTO, or OpenAPI schema: the existing drift test proves
that product-surface absence. Mechanical integration changes are still required:
the server's exhaustive `OmniError` mapping handles the new variants, and
server/CLI fixtures migrate from the removed embedded method to `read_blob_at`.
The live raw PUT → ranged GET → stale If-Match exercise against the cluster
server becomes an acceptance gate only when Phases 2 and 3 expose those
transports.
The object-store cost evidence is explicitly on-demand and is not implied by the
canonical local test graph. Follow
[`deployment.md` → Testing against S3 locally](../user/deployment.md#testing-against-s3-locally),
run `cargo test -p omnigraph-engine --test write_cost_s3` with the documented
bucket, endpoint, and credential environment, and attach the untruncated test log
plus the non-secret endpoint configuration to the implementation PR.

## 13. Rollout

Implementation lands in ordered phases; a later phase may not weaken an earlier
correctness gate.

### Phase 0 — substrate, correctness, and containment

- Land the Lance 10.0.0 migration with the exact positive compaction surface
  guard from §12.3. This is a prerequisite for every following phase; no Blob
  compaction skip is introduced.
- Centralize descriptor decoding and fix empty/null behavior everywhere.
- Reject Blob query projection and body-level unique constraints.
- Add the external-base policy, default deny, and route all existing URI ingress
  through it.
- Correct public docs that describe the physical Blob field as LargeBinary.

### Phase 1 — engine read facade

- Add typed node/edge selector, snapshot-aware descriptor-first read, managed
  reader, external reference, and validator. Range calls are half-open,
  `start <= end <= length`, accept empty-at-end, and return at most
  `BLOB_READ_RANGE_MAX_BYTES` (4 MiB).
- Pin the validator as SHA-256 over the domain, five ordered big-endian `u64`s,
  one big-endian byte length, and the exact non-empty UTF-8 bytes of the opened
  immutable Lance manifest's `transaction_file` identity. Missing identity is
  `BlobIntegrity`; invalid ranges are `BlobRangeNotSatisfiable`.
- Remove the public Lance-returning `read_blob`; internal callers migrate in the
  same release. Because the crate is pre-1.0, no permanent compatibility wrapper
  may continue leaking Lance.
- Resolve selectors through the current accepted catalog. The current type alias
  binds across pure type-rename history by stable table/incarnation identity,
  subject to the independent property and branch fences; the old alias is not
  retained. Defer crossing a historical property rename to its old physical
  field; both a pre-rename target under the current property alias and the
  retired alias fail loudly rather than falling back to field position.
- Persist `omnigraph.stable_property_id` on every physical user field newly
  initialized, added, or schema-rebuilt by 0.10 and compare it on Blob reads.
  Schema-preserving Append, Merge, and mutation writes retain an upgraded
  table's unmarked schema; a full-table Overwrite adopts the current 0.10
  catalog schema and marker on the replacement fields without rewriting older
  versions. Never substitute Lance field ID. For a pre-0.10 v6 field without
  the marker, admit only a snapshot whose table entry is exactly the current
  physical entry; refuse older snapshots as lacking a property-lifetime
  witness. Soft-drop/same-name re-add must refuse the retired lifetime.
- Authenticate every explicit snapshot by requiring its reopened manifest's
  exact graph-head row to name the resolved commit; this closes named graph-ref
  ABA even when the selected table is inherited from main. For a table stored
  on a named native branch, bypass the held-handle cache and cold-recheck the
  selected graph ref's effective head after opening it because v6 persists no
  native `BranchIdentifier`; a table-manifest e-tag is not a substitute.
  Genuine inherited-main history remains eligible after the first proof, while
  older branch-owned snapshots and raced live captures fail loudly. Independent
  property/schema checks still apply. Never infer branch incarnation from name
  and numeric version.
- Preserve the exact captured version across branch advance. Do not add a
  durable live-reader lease: branch deletion/reclamation and destructive cleanup
  require quiesced readers when completion is required. An uncached later range
  may fail loudly after reclamation, but it never returns different bytes.
- Keep the product capability engine-only. Mechanical server error-mapping and
  integration-fixture migrations are allowed, but no HTTP route, CLI verb, HTTP
  wire DTO, or OpenAPI schema is added.

### Phase 2 — delivery

- Add HTTP GET/HEAD, ranges and conditionals.
- Add CLI get/stat and parity coverage.

### Phase 3 — mutation

- Add engine PUT/clear through the shared Mutation writer.
- Add HTTP PUT/DELETE, CLI put/clear, admission, preconditions, OpenAPI, and
  recovery/failpoint coverage.

### Phase 4 — measured optimization

- Benchmark and tune the already-required batched complete/range reads in export
  and materializing rewrites. Tuning is optional and may not weaken the batched
  contract or change logical behavior.
- Retain the exact empty/null/neighbor compaction guard across every future Lance
  dependency bump.
- Consider descriptor-preserving unchanged-cell updates only with an ownership
  proof and recovery guard.

Existing stored external references need no migration. Operators who require
new URI ingress must add explicit bases before upgrading a workload that writes
them. This intentional secure-default change is called out in release notes and
as a graph-attributed effective-Deny warning in `cluster validate` output.

## 14. Invariants and deny-list check

No architectural invariant is weakened.

- **Respect the substrate:** Lance remains the Blob-v2 store and batched reader.
  OmniGraph adds coordination and transport, not a competing object layer.
- **One publication door / mutation once:** PUT and clear enter the existing
  Mutation transaction and one manifest CAS.
- **One coherent accepted view:** every read resolves catalog, table version,
  exact immutable-manifest identity, row, descriptor, and bytes from one
  `ReadTarget`.
- **Ordinary writes are recoverable:** no Blob-specific direct commit or recovery
  protocol is introduced.
- **Strong consistency:** validators and preconditions are evaluated against the
  captured base; success is returned only after graph publication.
- **Physical state is derived:** placement kind and thresholds remain Lance-owned;
  the positive guard protects current optimize. Only a future dependency bump
  may introduce a tested typed skip for unsafe physical work, without failing
  logical reads or writes.
- **Stable schema identity:** ETags and internal selectors use stable table,
  incarnation, and property IDs rather than aliases or paths. ETags additionally
  bind the exact opened Lance manifest's non-empty `transaction_file` identity
  so same-version branch deletion/recreation ABA cannot reuse a token.
- **Loud integrity:** false-null projection is removed, malformed descriptors
  error, and external failures are typed.
- **Typed IR and pushdown:** Blob stays a first-class type, and exact ID lookup is
  a structured expression rather than SQL text.
- **Authorization at the boundary:** engine-wide Cedar applies to embedded,
  server, and CLI writes; external-base policy is an additional resource gate.
- **Bounded failure:** upload, rewrite, range, probe concurrency, stream buffering,
  and cancellation all have explicit ceilings.
- **One source of truth:** Lance and the manifest remain authoritative; Blob
  metadata is derived descriptor state, not a parallel catalog.

The proposal explicitly avoids the relevant deny-list items: no custom WAL,
job queue, alternate writer, maintained side table, string-flattened filter,
silent fallback, cloud-only fix, or public substrate type. The permanent
positive compaction guard prevents a known physical rewrite defect from becoming
logical byte corruption. If a later dependency bump needs a temporary skip, the
bump itself must add and test that typed behavior before it can land.

## 15. Compatibility and reversibility

### 15.1 Storage format

No manifest, accepted SchemaIR, table path, Blob descriptor, or file-format
change is required. Current V2_2 Blob values need no rewrite; the new
single-cell historical facade applies the conservative incarnation and property-
lifetime witness boundaries in §15.2. This part is highly reversible because
the facade derives from existing state.

### 15.2 Public behavior

GET/HEAD/PUT/DELETE and CLI verbs are additive, but once released their routes,
range rules, validator format, redirect behavior, error codes, and table-version
granularity become observable contracts. The `v1` ETag domain separator permits
a future token format without ambiguous comparison.

The Phase 1 Rust facade is observable too. It removes the pre-1.0
`Omnigraph::read_blob` method that returned Lance's `BlobFile` and replaces it
with engine-owned `read_blob_at`, `BlobRead`, `BlobContent`, and `BlobReader`.
There is no compatibility wrapper: retaining one would keep Lance placement and
reader behavior in OmniGraph's public contract. This is a source-incompatible
break for embedded callers but requires no graph rebuild. The exact ETag byte
grammar, half-open range rules, 4 MiB per-call ceiling, and typed error variants
become public contracts in the same release.

Phase 1 accepts selector aliases from the handle's current accepted catalog. A
current type alias remains able to read pre-rename table history because stable
table/incarnation identity crosses the rename; the retired type alias is not a
compatibility spelling. This structural binding remains subject to the
independent property-lifetime, physical-name, and branch-incarnation checks.
Automatic property-field crossing through a historical rename is deferred: a
pre-rename target lacks the current physical field, while
the retired property alias is absent from the current catalog, so both return
`BadRequest` rather than inferring a column. That limitation is reversible and
changes no stored state.

The property-lifetime marker is additive field metadata on physical user fields
newly initialized, added, or schema-rebuilt by 0.10, not a manifest-schema bump.
Schema-preserving Append, Merge, and mutation writes retain an upgraded table's
unmarked schema. A full-table Overwrite adopts the current 0.10 catalog schema
and marker on its replacement physical fields without rewriting older versions.
Pre-0.10 v6 fields remain readable at their exact current physical table entry.
An older selected entry without the marker returns `BadRequest` (`no persisted
property-lifetime witness`) even when no rename occurred, and a marker from a
soft-dropped/re-added same-name property returns `BadRequest` as a different
lifetime. This fail-closed compatibility boundary never treats Lance field IDs
as graph identity.

Named-branch read eligibility has a separate conservative compatibility
boundary. V6 historical entries do not persist Lance's native
`BranchIdentifier`, and a manifest e-tag is not a sufficient substitute. An
explicit snapshot first requires its reopened manifest's exact graph-head row
to name the resolved commit, closing named graph-ref ABA even for an
inherited-main table. A table stored on a named native branch then bypasses the
held-handle cache and is cold-rechecked against the selected graph ref's
effective current head after the open. A live capture whose branch moved before
that proof and an older branch-owned snapshot are refused with `BadRequest`
(`no persisted native-branch incarnation witness`) rather than trusting a
branch name and numeric version that deletion/recreation can reuse. Genuine
main/inherited-main table history remains eligible after the graph-snapshot
proof; the independent property/schema checks still apply. This narrows a read
case without a manifest-format change.

Three deliberate behavior tightenings are not backward-compatible in the loose
sense:

1. a query that returned false null for a Blob now fails typechecking;
2. a body-level Blob `@unique` schema that failed only during writes is rejected
   by new init/schema-apply admission; a historical accepted v6 contract remains
   openable through the compatibility parser for inspection/export; and
3. new external URI ingress is denied without explicit bases.

All three replace unsafe or non-executable behavior. They require release-note
callouts but not a storage migration. Existing external references remain
readable, which keeps rollback and staged policy rollout possible.

The applied allow-list is nevertheless a control-plane compatibility boundary:
v0.10 stores its normalized form in an optional graph-resource field that the
strict v0.9 state parser does not know. A rollback therefore uses v0.10 to
remove `external_blobs` from desired configuration and apply default deny to
convergence before starting v0.9. That transition omits the optional field and
restores the historical graph-resource digest without a manifest or data-table
write. It restores only a v0.9-readable state shape: v0.9 itself has no
external-base policy and admits arbitrary supported external URIs, including
`file://`. Writers must be quiesced and the whole serving/control-plane fleet
cut over together; mixed v0.9/v0.10 writers are not a supported policy rollout.
If the external-ingress boundary is required, rollback to v0.9 is prohibited.
Editing desired YAML alone leaves the applied allow authority in place;
hand-editing the ledger severs the digest binding. Serving and every
state-mutating command refuse that ledger before a recovery sweep can reuse or
re-sign its metadata; restore the ledger from a trusted copy before retrying.

### 15.3 Substrate dependency

The exact positive compaction surface guard is a permanent dependency-bump gate;
the initial implementation contains no skip. No upstream fix is assumed by
version number alone. A future Lance bump must run `lance_surface_guards` and the
full workspace graph and cannot land while the guard is red unless that same
change carries a proven fix or a tested, typed per-table skip.

## 16. Drawbacks and rejected alternatives

### Keep returning `BlobFile`

Rejected. It leaks the substrate, cannot express historical target or external
no-I/O classification safely, and makes every caller own range/error semantics.

### Continue using base64 JSON only

Rejected. It wastes bandwidth and memory, reduces the usable payload beneath
the body envelope, has no range delivery, and encourages callers to materialize
whole values.

### Proxy or sign external objects in the server

Rejected for V1. Proxying turns every graph reader into a consumer of server
credentials and foreign-store availability. Signing is provider-specific and
would falsely imply that OmniGraph owns external-object delivery. The stored URI
is returned to the authorized caller without server-side I/O.

### Permit external URIs by scheme

Rejected. `s3://` or `file://` alone grants far too much authority. Exact
normalized bases are the smallest reviewable unit.

### Copy every external object into managed storage

Rejected as a universal rule. It removes the reason external references exist,
can make overwrite unbounded, and would silently alter export/rebuild behavior.
Keyed modes retain their current bounded copy because the current Lance merge
path cannot preserve the reference.

### Add a graph-level content-addressed Blob store

Rejected. It would create a second authority, reference counting, GC, recovery,
branch/snapshot ownership, encryption, and migration surface for behavior Lance
already supplies. Five more changes would expand this into a database beside the
database.

### Expose placement thresholds in `.pg`

Rejected pending evidence. The annotation would be persisted and migration-
relevant while solving only speculative tuning. Lance defaults and physical
optimization remain the lower-liability choice.

### Compact blob-bearing tables and merely document the empty bug

Rejected. A known reachable corruption case cannot be converted into an
operator caveat. This RFC requires Lance 10.0.0 plus the positive guard before
implementation. A future regression blocks its dependency bump unless that same
change supplies a proven fix or a tested skip; logical bytes are never put at
risk merely to retain compaction throughput.

## 17. Resolved implementation choices

Phase 0 fixed the two spellings that were open when this RFC was drafted:

1. **Configuration spelling.** `graphs.<id>.external_blobs.allow[]` carries
   exact `{ base, scope }` entries. Scope is `server_safe` or `embedded_only`;
   there is no operator- or request-level override.
2. **External dependency error mapping.** A malformed or disallowed URI is a
   typed policy failure and HTTP 400. A configured source that is missing or
   unreadable is a typed external-source failure and HTTP 424 Failed
   Dependency, never an opaque 500 or an unchecked retained reference.

These choices do not change storage semantics, the trust boundary, or the one-
publisher architecture.
