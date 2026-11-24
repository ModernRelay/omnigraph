# Blob management

**Decides:** a Blob is one property cell in three logical states (null, managed bytes, external reference), read and written through one engine facade over Lance Blob v2, with writes through the ordinary mutation publication and external references denied unless a configured base admits them.
**Contract:** keeps E1, E2, E5, E6, write:3, read:11, reclaim:4
**Code:** `crates/omnigraph/src/blob.rs`, `crates/omnigraph/src/exec/blob_write.rs`, `crates/omnigraph/src/table_store.rs`, `crates/omnigraph-server/src/blob_transport.rs`, `crates/omnigraph-cli/src/blob_cli.rs`
**Cited as:** RFC 0033

## Context

Blob was a schema type with no coherent owner. Queries returned null for
non-null Blobs, empty bytes were classified as null, the embedded read leaked
Lance's `BlobFile`, and any non-`base64:` string was taken as a URI the server
would read with its own credentials. Base64 JSON was the only transport. Each
new caller had to rediscover validity, snapshot selection, size accounting and
publication.

## Decision

- Null is decided only by the struct's Arrow validity. `Managed { length: 0 }`
  is a value, not null. One descriptor decoder serves read, export, rewrite,
  merge and the change feed; a malformed descriptor is `BlobIntegrity`.
- Lance owns placement (inline, packed, dedicated). No public surface reveals
  placement, paths, versions or row ids.
- A cell is selected by entity kind, type, id and property, resolved through the
  accepted catalog and stable identity. Lookup is a typed `id` expression.
- A read takes a `ReadTarget`, pins one manifest view and table version, and
  classifies the descriptor before any payload I/O. An external reference is
  returned without touching its store.
- `BlobReader::read_range` returns at most 4 MiB per call; there is no
  unbounded read. HTTP streams chunks under backpressure.
- A managed cell has a strong ETag, hashed from stable table, incarnation and
  property ids, exact table version, stable row id and the manifest's
  transaction-file name. It is table-version granular. External cells have none.
- `put` and `clear` are update-only on an existing row, go through mutation
  staging and one manifest publication, and return the exact commit and ETag.
  An optional `If-Match` (`*` or strong tags) is checked on each attempt;
  failure is `BlobWritePreconditionFailed` (412). Clearing a null cell
  publishes nothing.
- The row is rewritten whole. Untouched managed siblings are carried
  byte-identical; an external sibling is read under the current policy and
  stored managed, or the write fails with `StoredExternalBlobDenied`.
- Payload bytes count against `write_max_bytes` (at most 32 MiB) apart from the
  batch's framing, before any effect.
- HTTP GET and HEAD serve managed bytes with one byte range and conditionals;
  an external cell is a 302 with `Cache-Control: no-store`, never proxied. PUT
  takes raw `application/octet-stream`; DELETE clears.
- The CLI has `blob get`, `stat`, `put`, `clear`, sharing output types and
  `If-Match` parsing with the server. Redirects are not followed.
- External ingress is denied by default. `ExternalBlobPolicy::Allow` lists
  normalized bases, each `server_safe` or `embedded_only`; a server base cannot
  be `file://`, and no base may overlap Omnigraph storage. Probes are bounded
  and deduplicated. Stored references stay readable under any policy.
- Keyed writes copy an admitted external object into managed storage;
  overwrite keeps the reference. Omnigraph never deletes an external object.
- `.gq` refuses projecting, ordering or aggregating a Blob (`T24`). Blob is
  ineligible for `@key`, `@unique`, `@index` and `@embed`.
- Complete payload reads use batched `read_blobs`; `take_blobs` serves only the
  single-cell range facade. Schema apply never rewrites a Blob cell.

## Rejected

- **Returning Lance's `BlobFile`:** it leaks the substrate and makes every
  caller own historical targets, ranges and errors.
- **Base64 JSON only:** it inflates payloads past the body limit, has no ranges,
  and invites whole-value materialization.
- **Proxying or signing external objects:** every reader would consume server
  credentials and foreign-store availability.
- **Allowing external URIs by scheme:** `s3://` or `file://` alone grants far
  too much authority; a normalized base is the smallest reviewable unit.
- **A content-addressed Blob store:** a second authority with reference
  counting, GC and recovery, for what Lance already supplies.

## Not built

- Keeping an external reference through a keyed write or a sibling carry,
  which needs a Lance merge-insert that accepts write parameters.
- Managed values above 32 MiB.
- Delivery telemetry by classification, range and time to first byte.
