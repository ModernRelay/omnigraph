# Errors and Result Serialization

## Error taxonomy (`omnigraph::error::OmniError`)

- `Compiler(...)` — schema/query parse/typecheck errors
- `Lance(String)` — storage layer
- `DataFusion(String)` — execution layer
- `Io(io::Error)`
- `Manifest(ManifestError { kind: BadRequest|NotFound|Conflict|Internal, details: Option<ManifestConflictDetails>, … })`
  - `ManifestConflictDetails::ExpectedVersionMismatch { table_key, expected, actual }` — caller's `expected_table_versions` did not match the manifest's current latest non-tombstoned version (set by `OmniError::manifest_expected_version_mismatch`).
  - `ManifestConflictDetails::ReadSetChanged { member, expected, actual }` — an RFC-022 prepared write's branch/head/table authority changed before physical effects. HTTP returns **409** with `read_set_conflict`. A retry must start from preparation; strict writes leave that choice to the caller.
  - `ManifestConflictDetails::RowLevelCasContention` — Lance row-level CAS rejected the publish because a concurrent writer landed the same `object_id`. Retried internally by the publisher; only surfaces if the retry budget exhausts.
  - **Missing schema files on open**: after opening a readable `__manifest`,
    `open` returns `NotFound` when `_schema.pg` is absent. This proves that the
    manifest is readable, not that every referenced data table is present or
    valid. Restore the matching `_schema.pg`, `_schema.ir.json`, and
    `__schema_state.json` contract from a backup, or rebuild a fresh graph from
    an existing export or backup. The damaged graph cannot be exported through
    the normal API because it cannot open. A graph missing only
    `_schema.ir.json` or `__schema_state.json` is refused by the schema-contract
    validation that runs at open.
  - **D₂ parse-time rejection**: a single mutation query that mixes inserts/updates with deletes errors out *before any I/O* with kind `BadRequest`. Message: `mutation '<name>' on the same query mixes inserts/updates and deletes; split into separate mutations: (1) inserts and updates, then (2) deletes`. See [query-language.md](../queries/index.md) for the rule.
  - **Blob property-lifetime refusal**: `read_blob_at` returns `BadRequest` when
    the selected field's persisted `omnigraph.stable_property_id` belongs to a
    different property lifetime, or when an older pre-0.10 v6 snapshot has no
    persisted property-lifetime witness. OmniGraph never substitutes Lance field
    ID, field position, or same-name spelling as graph identity.
    Schema-preserving Append, Merge, and mutation writes retain an upgraded
    table's unmarked schema; full-table Overwrite adopts the 0.10 catalog marker
    on its replacement fields. Every older snapshot of an unmarked field is
    refused even when no rename occurred; only its exact current physical entry
    is admitted.
  - **Blob native-branch-incarnation refusal**: `read_blob_at` returns
    `BadRequest` when an explicit snapshot's reopened named manifest no longer
    carries its resolved graph commit, for an older snapshot of a
    named-branch-owned table, or for a live branch-owned read whose captured
    effective head changed before the post-open proof. V6 did not persist the
    native `BranchIdentifier`; a manifest e-tag is not a sufficient substitute.
    Genuine main/inherited-main table history remains eligible after the graph
    snapshot authenticates; independent property/schema checks may still refuse
    the read.
- `MergeConflicts(Vec<MergeConflict>)`
- `KeyConflict { table_key, key }` — a strict insert found an existing `id` in
  its pinned table image or lost an effect-free concurrent same-key race. HTTP
  returns **409** with `key_conflict.table_key`. V6 emits
  `key_conflict.key` only after an observed preflight or fresh exact-ID probe;
  the field stays optional in the additive wire schema. Retrying the same
  strict operation does not turn it into an upsert.
- `RetryableCommitConflict(String)` — the typed internal signal that Lance
  rejected a stale filtered transaction. Upsert writers consume an effect-free
  instance by discarding and fully repreparing the logical operation; a strict
  writer does the same when its fresh attempted-ID probe finds no match. No code
  parses Lance error text. If this signal escapes an enrolled writer, HTTP maps
  it to a generic **409** conflict.
- `ResourceLimitExceeded { resource, limit, actual }` — a keyed Mutation/Load
  table (`mutate`, `load --mode append`/`merge`; Overwrite stages a whole-table
  replacement transaction and is not subject to the keyed ceiling) exceeded its
  single-transaction ceiling of 8,192 rows or 32 MiB of
  staged Arrow memory (with an earlier conservative parsed-value/base64 guard
  to bound the load spool, and a streamed remaining-budget guard on mutation
  update matches); keyed external-URI or stored-update blob payloads exceeded
  the remaining 32 MiB operation budget before their bytes were read; a
  BranchMerge materialized row, aggregate managed-plus-external carried payload,
  retained external-URI metadata, escaped delete filter, complete retained
  delete plan, or operation-wide projected scalar validation delta exceeded
  32 MiB; a row-writing BranchMerge selected more than 8,192 external-reference
  cells; or its logical data chain would exceed 1,024 transactions. This is
  detected before recovery arm and has no durable effect.
  HTTP returns **413** with `resource_limit.{resource,limit,actual}`.
  Reshape the input; it is not partial success. Served streaming export also
  uses this typed response before `200`: `stream_export_slots` means another
  response owns the graph's immutable export cut, while
  `stream_export_transport_bytes` means the process-wide bounded response
  budget did not become available within 250 ms. Those export limits are
  transient; finish or disconnect the earlier response and retry rather than
  changing graph data.
  Embedded `BlobReader::read_range` also returns this variant with resource
  exactly `Blob read range bytes` when an otherwise-valid half-open range is
  wider than `BLOB_READ_RANGE_MAX_BYTES` (4 MiB). The check happens before
  payload I/O. HTTP Blob delivery splits a full or single-range representation
  into consecutive bounded engine reads, so an HTTP range wider than 4 MiB is
  served under backpressure rather than mapped to 413.
  The full set of `resource_limit.resource` names a client can receive is:
  `strict_input_arrow_bytes` (a strict load's projected Arrow allocation
  exceeded 32 MiB — this preflight applies to **every** load mode, Overwrite
  included), `graph_batch_request_bytes`, `graph_batch_line_bytes`,
  `graph_batch_json_structural_slots`, `stream_export_slots`,
  `stream_export_transport_bytes`, `Blob read range bytes`, `external Blob URI
  bytes`, `external Blob reference cells`, `external Blob URI metadata bytes`,
  `external Blob object bytes`, `decoded blob input bytes`, `materialized blob
  payload bytes`,
  `materialized external blob payload bytes`, `branch-merge delete filter
  bytes`, `branch-merge retained delete plan bytes`, `branch-merge fenced row
  bytes`, `branch-merge recovery transaction chain`, and `branch-merge retained
  validation delta bytes`. Table-specific instances use these enumerable
  patterns: `keyed rows for {table_key}`, `keyed parsed value bytes for
  {table_key}`, `decoded blob input bytes for {table_key}`, `keyed write rows
  for {table_key}`, `keyed write bytes for {table_key}`, `keyed bytes for
  {table_key}`, `branch-merge pure-insert validation batch rows for
  {table_key}`, `branch-merge pure-insert validation batch bytes for
  {table_key}`, `branch-merge recovery transactions for {table_key}`, `proven
  insert delta rows for {table_key}`, and `proven insert delta bytes for
  {table_key}`. Treat `resource` as a typed discriminator, including its
  documented table-key suffix; do not infer one ceiling from another.
- `ExternalBlobPolicy { uri, reason }` — new external-URI ingress is malformed,
  uses a disallowed execution scope, or falls outside the graph's normalized
  allow bases. The URI field is normalized and credential-free (or redacted);
  raw credentials are never echoed. HTTP returns **400**. This failure happens
  before external payload reads, recovery arm, target HEAD/ref movement, or a
  graph-visible effect. Scalar-only input preparation may already have created
  reclaimable temporary staging.
- `ExternalBlobSource { uri, reason }` — an allowed external source could not
  be probed or read. HTTP returns **424 Failed Dependency**; it is not collapsed
  into a generic storage 500. The response carries
  `external_blob_source: { uri, reason }`; `uri` is normalized and
  credential-free (or redacted), while `reason` is human-readable and must not
  be parsed. Its optional top-level `code` is omitted: the structured field is
  the rolling-safe discriminator because extending the closed `ErrorCode` enum
  would break older clients. Source metadata or payload I/O may already have
  begun—that is what this error reports—but the operation fails before recovery
  is armed, a target HEAD/ref moves, or graph-visible state changes. Scalar-only
  input preparation may already have created reclaimable temporary staging.
- `BlobIntegrity { reason }` — persisted table or Blob state contradicts the
  exact selected identity or logical Blob contract. Examples include a
  malformed Blob-v2 descriptor, a malformed
  `omnigraph.stable_property_id` marker, the wrong table incarnation/version or
  manifest e-tag, a missing or empty immutable-manifest `transaction_file`
  identity required for a strong ETag, and a physical non-Blob field behind a
  catalog Blob property.
  Embedded callers match this variant rather than interpreting malformed state
  as null, absence, or plausible bytes or accepting a weaker validator. The
  server's exhaustive conversion maps it through the existing 5xx integrity
  class; the Blob delivery route returns a generic pre-header 500 and logs only
  a non-sensitive error class, never substrate paths or persisted identities.
- `BlobRangeNotSatisfiable { start, end, length }` — an embedded managed-Blob
  range violates `start <= end <= length`. Half-open empty ranges are valid at
  every in-bounds position, including `length..length`. The server's exhaustive
  conversion and the HTTP Blob delivery range parser map it to 416 with
  structured `blob_range { start, end, length }` details.
- `Policy(String)` — a Cedar policy denied the action for the resolved actor.
  HTTP returns **403**.
- `AlreadyInitialized { uri }` — strict `init` targeted a root that already
  holds a graph or orphan schema metadata. `--force` may replace only orphan
  schema files after proving no `__manifest` exists; it never overwrites an
  initialized graph. HTTP returns **409**.
- `InitializationCommitted { uri, source }` — the authoritative `__manifest`
  Create commit completed, but a later read-back or validation step failed.
  The schema artifacts are preserved and the original typed error is retained
  in `source`. This outcome does **not** promise that an ordinary `open` will
  succeed: inspect or try to open the graph, diagnose `source`, and do not
  delete its schema files.
- `InitializationIndeterminate { uri, source, probe }` — physical graph
  initialization returned `source`, then the exact genesis probe failed with
  `probe`. OmniGraph therefore cannot prove which table or manifest Creates
  landed.
  It preserves both the schema artifacts and `__init_claim.json`, so another
  initializer cannot enter the uncertain root. Do not retry `init`, remove the
  claim, or delete the root until the storage failure is resolved, every
  initializer is quiesced, and the root is inspected.
- `InitializationClaimed { uri }` — `__init_claim.json` already reserves this
  root for another initialization attempt. That attempt may still be running,
  or the claim may be residue from a stopped process. Quiesce every initializer
  for the root before manually removing a stale claim and retrying `init`; use
  `--force` only when orphan schema files remain. Never remove the claim while
  another initializer may be live.
- Init cleanup is best-effort and is attempted only for failures returned
  before any physical graph initialization. It removes the schema-contract
  artifacts owned by the attempt, not partially created Lance table
  directories. If any schema delete has an indeterminate outcome, the init
  claim is retained so a delayed delete cannot race a later initializer. A
  confirmed manifest or indeterminate physical outcome is never cleaned
  backward.
- `RecoveryRequired { operation_id, reason }` — an overlapping durable recovery intent remains unresolved. Its physical effects may already have landed, or it may still be armed before the first effect. HTTP returns **503** with `recovery_required.operation_id`. Resolve the sidecar through a read-write reopen/server restart before retrying; this is intentionally not an ordinary OCC retry.

For RFC-023 Mutation/Load keyed writes, `KeyConflict` is returned only after
the writer proves that none of its planned table effects landed, finalizes the
empty `protocol_v3` recovery intent, and finds an attempted ID in fresh
manifest-visible state. A generic retryable substrate conflict without that
match becomes an internal read-set conflict consumed by bounded full strict-
mode reprepare, not a false duplicate. If another table already advanced, or
effect ownership is ambiguous, the result is
`RecoveryRequired` instead; the engine never retries around that sidecar.
BranchMerge uses strict chunks only as an internal physical mechanism: after
its `protocol_v4` sidecar is armed, any chunk conflict remains
`RecoveryRequired`, including a conflict on the first chunk before a
merge-owned table effect lands.

Compiler-side `CompilerError` covers parse / catalog / type / storage / plan / execution / arrow / lance / IO / manifest / unique-constraint, each with structured spans (`SourceSpan { start, end }`) for ariadne-style diagnostics. The legacy `NanoError` name remains as a deprecated compatibility alias.

## Result serialization (`omnigraph_compiler::result::QueryResult`)

- `to_arrow_ipc()` — efficient binary
- `to_sdk_json()` — JS-safe JSON (large i64 wrapped in metadata)
- `to_rust_json()` — Rust-friendly JSON
- `batches()` — direct Arrow `RecordBatch` access

Mutation results: `{ affectedNodes: usize, affectedEdges: usize }` (also exposed as a tiny Arrow batch).
