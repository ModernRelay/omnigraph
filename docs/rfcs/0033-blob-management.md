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
RFC-028 stable schema identity, internal manifest schema v6, and Lance 9.0.0
blob-v2 (`lance.blob.v2`) on file format V2_2.
**Surveyed:** OmniGraph `e53f3c9509d0` and Lance 9.0.0.
**Audience:** engine, compiler, server, CLI, security, storage, maintenance,
and documentation maintainers.

This RFC is the required successor to the reverted blob-delivery work recorded
in [`docs/internal/blob-api.md`](../internal/blob-api.md). It takes the useful
parts of that experiment as evidence, not as an accepted contract. Nothing in
that internal note is restored merely by accepting this RFC.

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
8. Fix all OmniGraph empty-vs-null classification to use Arrow validity, and
   temporarily re-gate compaction of blob-bearing tables until the pinned Lance
   empty-blob compaction reproducer is green. Correctness takes precedence over
   compaction throughput.
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
| Maintenance | `optimize` compacts blob-bearing tables on the pinned Lance version. |

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
- A public batch-Blob endpoint. Engine internals may use Lance's planned batch
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

`type_name` and `property` are resolved through the accepted catalog captured
with the target snapshot. The engine then carries stable table identity,
incarnation identity, and stable property identity internally. A rename keeps
those identities; drop/re-add mints a new lifetime. `table_key`, alias, physical
path, field position, and Lance version are not identity substitutes.

All OmniGraph physical entity IDs are exact non-null Utf8. Lookup uses a typed
`col("id").eq(lit(id))` expression and the stable row ID selected from that
snapshot. Caller text is never flattened into SQL.

### 3.3 Read target and snapshot isolation

`read_blob_at` takes the same `ReadTarget` as query/export: exactly one branch or
snapshot, with the normal default to `main` at transport boundaries. It resolves
one manifest view and one exact table version. Catalog validation, row lookup,
descriptor classification, and payload access all use that view. The call does
not re-read branch head after resolution.

A returned reader keeps the snapshot/table handle needed to finish the read.
Advancing or deleting a branch after the reader is opened does not switch the
payload under that reader. Normal cleanup floors must keep live readers safe in
the same way as other snapshot-pinned reads.

### 3.4 Managed validator

Managed content receives a strong quoted ETag derived in the engine from:

```text
stable_table_id
table_incarnation_id
stable_property_id
exact_table_version
stable_row_id
```

The encoded token is the lowercase hex of the first 16 bytes of SHA-256 over a
domain-separated, fixed-width encoding of that tuple, wrapped in quotes. The
domain separator is `omnigraph/blob-etag/v1\0`. A single engine function owns
the encoding; server and CLI delegate to it.

This validator is deliberately table-version-granular. An unrelated write to
the same table may invalidate a client's token even when the selected bytes did
not change. It is still a strong validator: equal tokens identify the same
immutable cell representation at an exact table version. The coarser behavior
is documented rather than hidden.

External references receive no ETag. A stored URI descriptor does not prove the
current bytes of a mutable external object, and calling it a strong payload
validator would be false. HTTP redirects include `Cache-Control: no-store`.

## 4. Engine facade

The exact Rust names may move during implementation, but this capability split
and ownership boundary are normative:

```rust
pub struct BlobRead {
    pub cell: BlobCell,
    pub resolved_target: ResolvedReadTarget,
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
        target: ReadTarget,
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

`BlobReader` is an engine-owned `Send + Sync` abstraction with `len()` and
bounded `read_range(Range<u64>)`. An implementation may wrap Lance
`read_blob_ranges`, `read_blobs`, or `take_blobs`, but Lance types do not appear
in public signatures. Complete-payload internal work uses Lance's planned
`read_blobs` API; it must not build a thread pool around one `BlobFile::read()`
per row. Range work prefers `read_blob_ranges` when the pinned Rust API supports
the needed selector.

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
out-of-bounds ranges fail loudly as integrity/substrate errors.

### 4.2 Read errors

- Unknown type, entity ID, or null cell: typed `NotFound`.
- Known non-Blob property: typed `BadRequest`.
- A Blob property whose descriptor is malformed: typed integrity error, never
  `NotFound` and never a null substitute.
- A range whose start is at or beyond a non-empty managed payload, or whose
  arithmetic overflows: typed range error carrying the logical length.
- An empty range on a non-null managed Blob returns empty bytes without payload
  I/O. Reading the full zero-length Blob succeeds with an empty body.

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

`AnyExisting` means that the target entity row exists, including when the Blob
cell is null. `Tags` uses strong comparison and matches only the managed token at
the pinned write base. A null or external cell has no token and cannot match a
tag. `PreconditionFailed { current_etag: Option<BlobEtag> }` is a typed successful
decision, mapped to HTTP 412, rather than a substrate failure.

After publication, a successful managed write derives the validator from the
published table version and resulting row ID. The result is the same ETag a
subsequent GET at that head returns.

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

An allowed external URI is probed once before the first durable effect of the
write that consumes it. Probes use the graph's shared object-store registry,
deduplicate identical normalized URIs, and run with bounded parallelism. A
malformed, disallowed, missing, or unreadable object fails loudly. Cost tests
include external references so the implementation cannot regress to a new
registry and sequential HEAD per row.

Storage semantics remain explicit while Lance lacks a keyed-write `WriteParams`
hook:

- overwrite preserves an allowed external reference;
- strict insert, upsert, load append/merge, mutation update, and branch merge
  pre-size and copy the allowed bytes into managed Blob storage under the
  operation's 32 MiB aggregate ceiling.

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
(including `count`, because nullness is part of its answer), comparison, match,
and edge-property projection with a stable typed diagnostic that points to the
Blob management surface. It must not execute and substitute null.

Blob assignment from a string remains available only through existing mutation
semantics and the external-reference policy. Raw bytes use `blob put`; there is
no base64 requirement in `.gq` added by this RFC.

A future query-level descriptor such as `blob_meta($n.payload)` would be a new IR
concept and requires its own design. It must not arrive as another physical
struct or Utf8 side channel.

### 8.2 Schema constraints

Blob remains ineligible for `@key`, `@unique`, `@index`, and `@embed`, whether
the annotation is property-level or body-level and whether the property belongs
to a node or edge. Compiler validation rejects the constraint before catalog
acceptance. No schema-format bump is required; this closes a parser/validator
hole for a shape that never had executable semantics.

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

The current materializing rewrite is accepted as a bounded V1 implementation,
not as an ideal physical plan. A future optimization may carry immutable
prepared descriptors for unchanged cells only after a Lance surface guard proves
that references cannot escape their source dataset/incarnation and recovery can
account for every file. Correctness and ownership proof come before avoiding the
copy.

### 8.5 Optimize and cleanup

The pinned Lance 9.0.0 survey contains a reachable empty-Blob compaction gap.
Implementation of this RFC restores a Blob compaction capability gate and a
reported skip reason for every blob-bearing table. It also adds an upstream
surface reproducer containing, in order, non-empty bytes, a valid empty Blob,
another non-empty Blob, and null. The guard must verify value, nullness, and
neighbor payloads before and after compaction.

Until that reproducer is green on the pinned Lance release, `optimize` performs
no `compact_files` call for blob-bearing tables and reports
`PinnedLanceEmptyBlobCompactionUnsafe`. Index reconciliation that does not route
through the unsafe rewrite may still run only if its transaction/publication
envelope can remain coherent without compaction. Once an upstream fix is pinned,
the gate flips, the positive maintenance test is restored for empty and
non-empty values together, and the skip branch may be removed.

Cleanup remains Lance-owned version GC under OmniGraph's manifest/ref/recovery
floors. It can remove managed Blob sidecars only when Lance proves they are no
longer reachable from retained dataset versions. It never interprets or deletes
external URIs.

## 9. Physical layout and Lance alignment

OmniGraph uses Blob-v2 fields only on V2_2 datasets. The canonical logical input
and prepared physical descriptor are distinct contracts. Append/schema evolution
must retain the exact extension metadata Lance assigned at table creation.

At Lance 9.0.0, the Rust implementation's defaults are 64 KiB inline, 4 MiB
dedicated, and 1 GiB per packed sidecar. Upstream documentation and later
versions may use different defaults. OmniGraph does not expose or restate these
as user promises. A graph stores the chosen field metadata, and Lance rejects
incompatible metadata on later appends.

No `.pg` annotation is added for these thresholds. Exposing them now would turn
physical tuning into persisted accepted-schema behavior and would require
schema-planner, rename, migration, export/import, and compatibility rules without
evidence that users need the control. If production metrics later show placement
as a material cost term, a follow-up RFC can propose an annotation with migration
semantics and a comparative benchmark.

Planned complete reads use `Dataset::read_blobs`; planned range reads use
`Dataset::read_blob_ranges`; lazy single-cell reads may use `take_blobs` behind
the engine facade. Logical row IDs are preferred within an exact snapshot.
Physical row addresses never become public stable identity.

## 10. Security and resource model

| Risk | Required control |
|---|---|
| Server-side file/object read through URI input | Default-deny engine policy, exact normalized bases, no server `file://`, no per-request override |
| URI credential disclosure | Reject user-info/query/fragment credentials at config and input; return URI only to an authorized Blob reader |
| External SSRF during read | Descriptor-first classification; redirect only; no proxy or validation on GET/HEAD |
| Oversize upload | Route and engine 32 MiB inclusive limits; refusal before effect |
| Rewrite amplification | Pre-size all carried Blob payloads under one 32 MiB operation budget before read |
| Download memory | ≤4 MiB chunks, two-chunk queue, backpressure, prompt cancellation |
| Range arithmetic | Checked `u64` addition and exact logical-size bounds |
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
| External source missing/unreadable | typed external source error | 400 or mapped dependency failure; never generic 500 |
| Upload/rewrite budget exceeded | `resource_limit` with limit/observed | 413 |
| Unsatisfiable range | `range_not_satisfiable` with length | 416 |
| If-Match failed | `PreconditionFailed` outcome | 412 |
| Recovery intent armed but completion uncertain | `recovery_required` | existing mapping |
| Malformed persisted descriptor | integrity/substrate error | existing 5xx integrity mapping |

Instrumentation records operation, entity kind, managed/external/null
classification, requested and served byte count, range/full mode, precondition
result, external probe count/dedup count, and time-to-first-byte. It never logs
payload bytes, bearer tokens, URI credentials, or complete sensitive URIs.
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

- `end_to_end.rs`: managed node and edge full/range reads; branch and snapshot
  pinning; null versus empty versus non-empty; update-only PUT; nullable clear;
  returned ETag equals a follow-up read; missing/non-Blob errors; fresh/stale/`*`
  preconditions; external descriptor classification after the target object is
  made unavailable.
- `writes.rs`: inclusive and +1-byte PUT bounds; aggregate carried-row bound;
  target old payload is not read; every refusal proves table HEAD, manifest,
  lineage, and sidecar state unchanged.
- `branching.rs`: edge and node Blob reads on branches, merge preservation of
  empty bytes, and external-source accounting.
- `export.rs`: four-way null/empty/non-empty/external round trip.
- Existing schema-apply coverage gains an empty Blob and a neighboring non-empty
  Blob rather than adding a duplicate initialization fixture.
- `forbidden_apis.rs`: the old public `read_blob -> BlobFile` surface is removed;
  new writes are registered under Mutation; no new durable call site appears.

### 12.3 Lance and maintenance guards

- `lance_surface_guards.rs`: descriptor validity/cardinality and planned range
  reads preserve null versus empty; the compaction reproducer covers neighboring
  payload integrity.
- `maintenance.rs`: the pinned unsafe version reports the explicit skip with no
  Blob-table HEAD movement while plain tables still optimize in the same graph;
  when the pin is fixed, flip the expected cell to atomic compaction of both.
- `cleanup` retains a managed sidecar reachable from a snapshot/ref and never
  attempts deletion of an external URI.

### 12.4 Server, CLI, parity, and cost

- `data_routes.rs`: GET/HEAD headers; empty 200; ranges and conditionals; external
  302 with zero external I/O; branch/snapshot validation; PUT/DELETE; 412/413;
  node and edge selectors; authorization and actor attribution.
- `openapi.rs`: regenerate and compare the binary request/response surface.
- `cli_data.rs`: stdin/file PUT, stdout/file GET, stat, clear, ranges, snapshots,
  external no-follow behavior, JSON output, and stable failure text.
- `parity_matrix.rs`: byte and structured-output equality for every CLI verb and
  the failures listed in §6.
- `write_cost.rs` and its S3 owner: fixed external-reference counts prove shared
  registry reuse, URI deduplication, bounded probes, and no per-row cold setup.
- Add a served streaming memory/RSS scenario for a near-limit managed payload;
  integration tests own bounds and cancellation, not wall-clock thresholds.

Acceptance requires the canonical workspace test graph, the relevant focused
suites from a clean baseline, `scripts/check-agents-md.sh`, OpenAPI drift, and a
live raw PUT → ranged GET → stale If-Match exercise against the cluster server.

## 13. Rollout

Implementation lands in ordered phases; a later phase may not weaken an earlier
correctness gate.

### Phase 0 — correctness and containment

- Centralize descriptor decoding and fix empty/null behavior everywhere.
- Reject Blob query projection and body-level unique constraints.
- Restore the compaction safety gate and surface the skip.
- Add the external-base policy, default deny, and route all existing URI ingress
  through it.
- Correct public docs that describe the physical Blob field as LargeBinary.

### Phase 1 — engine read facade

- Add typed node/edge selector, snapshot-aware descriptor-first read, managed
  reader, external reference, and validator.
- Deprecate then remove the public Lance-returning `read_blob`; internal callers
  migrate in the same release. Because the crate is pre-1.0, no permanent
  compatibility wrapper may continue leaking Lance.

### Phase 2 — delivery

- Add HTTP GET/HEAD, ranges and conditionals.
- Add CLI get/stat and parity coverage.

### Phase 3 — mutation

- Add engine PUT/clear through the shared Mutation writer.
- Add HTTP PUT/DELETE, CLI put/clear, admission, preconditions, OpenAPI, and
  recovery/failpoint coverage.

### Phase 4 — measured optimization

- Use Lance planned batch/range reads in export and materializing rewrites where
  measurements prove fewer requests/bytes without changing contracts.
- Remove the compaction gate only after the exact empty/neighbor guard is green
  on the pinned Lance version.
- Consider descriptor-preserving unchanged-cell updates only with an ownership
  proof and recovery guard.

Existing stored external references need no migration. Operators who require
new URI ingress must add explicit bases before upgrading a workload that writes
them. This intentional secure-default change is called out in release notes and
`cluster validate` output.

## 14. Invariants and deny-list check

No architectural invariant is weakened.

- **Respect the substrate:** Lance remains the Blob-v2 store and planned reader.
  OmniGraph adds coordination and transport, not a competing object layer.
- **One publication door / mutation once:** PUT and clear enter the existing
  Mutation transaction and one manifest CAS.
- **One coherent accepted view:** every read resolves catalog, table version,
  row, descriptor, and bytes from one `ReadTarget`.
- **Ordinary writes are recoverable:** no Blob-specific direct commit or recovery
  protocol is introduced.
- **Strong consistency:** validators and preconditions are evaluated against the
  captured base; success is returned only after graph publication.
- **Physical state is derived:** placement kind and thresholds remain Lance-owned;
  optimize may skip unsafe physical work without failing logical reads/writes.
- **Stable schema identity:** ETags and internal selectors use stable table,
  incarnation, and property IDs rather than aliases or paths.
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
silent fallback, cloud-only fix, or public substrate type. Temporarily skipping
unsafe Blob compaction is an application of logical-contract-over-physical-state,
not a loss of logical Blob capability.

## 15. Compatibility and reversibility

### 15.1 Storage format

No manifest, accepted SchemaIR, table path, Blob descriptor, or file-format
change is required. Current V2_2 Blob values remain readable. This part is highly
reversible because the new facade derives from existing state.

### 15.2 Public behavior

GET/HEAD/PUT/DELETE and CLI verbs are additive, but once released their routes,
range rules, validator format, redirect behavior, error codes, and table-version
granularity become observable contracts. The `v1` ETag domain separator permits
a future token format without ambiguous comparison.

Three deliberate behavior tightenings are not backward-compatible in the loose
sense:

1. a query that returned false null for a Blob now fails typechecking;
2. a body-level Blob `@unique` schema that failed only during writes is rejected
   at schema validation; and
3. new external URI ingress is denied without explicit bases.

All three replace unsafe or non-executable behavior. They require release-note
callouts but not a storage migration. Existing external references remain
readable, which keeps rollback and staged policy rollout possible.

### 15.3 Substrate dependency

The compaction skip is reversible through one capability gate backed by an exact
surface guard. No upstream fix is assumed by version number alone. A future
Lance bump must run `lance_surface_guards` and the full workspace graph before
the gate changes.

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
Keyed modes retain their current bounded copy only because the pinned Lance
builder cannot preserve the reference.

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
operator caveat. Skipping derived maintenance work is cheaper than risking
logical bytes.

## 17. Unresolved questions

The product contract is intentionally closed enough to implement. Review still
needs to settle two narrow implementation details:

1. **Configuration spelling.** The engine policy and semantics in §7 are fixed;
   the exact `cluster.yaml` field names and operator-config override shape should
   follow the cluster schema's existing naming conventions during implementation.
   There is no per-request override regardless of spelling.
2. **External dependency error mapping.** A malformed/disallowed URI is 400 and
   a missing entity is 404. Review must choose the existing structured error
   family for a configured external source that is temporarily unreadable so it
   is stable across server and embedded arms; it must not collapse to an opaque
   500 or silently retain an unchecked reference.

Neither question changes storage semantics, the trust boundary, or the one-
publisher architecture.
