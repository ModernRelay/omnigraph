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
- `RecoveryRequired { operation_id, reason }` — an overlapping durable recovery intent remains unresolved. Its physical effects may already have landed, or it may still be armed before the first effect. HTTP returns **503** with `recovery_required.operation_id`. Resolve the sidecar through a read-write reopen/server restart before retrying; this is intentionally not an ordinary OCC retry.

Compiler-side `CompilerError` covers parse / catalog / type / storage / plan / execution / arrow / lance / IO / manifest / unique-constraint, each with structured spans (`SourceSpan { start, end }`) for ariadne-style diagnostics. The legacy `NanoError` name remains as a deprecated compatibility alias.

## Result serialization (`omnigraph_compiler::result::QueryResult`)

- `to_arrow_ipc()` — efficient binary
- `to_sdk_json()` — JS-safe JSON (large i64 wrapped in metadata)
- `to_rust_json()` — Rust-friendly JSON
- `batches()` — direct Arrow `RecordBatch` access

Mutation results: `{ affectedNodes: usize, affectedEdges: usize }` (also exposed as a tiny Arrow batch).
