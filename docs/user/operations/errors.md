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
  - **D₂ parse-time rejection**: a single mutation query that mixes inserts/updates with deletes errors out *before any I/O* with kind `BadRequest`. Message: `mutation '<name>' on the same query mixes inserts/updates and deletes; split into separate mutations: (1) inserts and updates, then (2) deletes`. See [query-language.md](../queries/index.md) for the rule.
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
  table exceeded its single-transaction ceiling of 8,192 rows or 32 MiB of
  staged Arrow memory (with an earlier conservative parsed-value/base64 guard
  to bound the load spool, and a streamed remaining-budget guard on mutation
  update matches); keyed external-URI or stored-update blob payloads exceeded
  the remaining 32 MiB table budget before their bytes were read; a BranchMerge
  materialized row, escaped delete filter, complete retained delete plan, or
  operation-wide projected scalar validation delta exceeded 32 MiB; or its
  logical data chain would exceed 1,024 transactions. This is detected before
  recovery arm and has no durable effect.
  HTTP returns **413** with `resource_limit.{resource,limit,actual}`.
  Reshape the input; it is not partial success.
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
