# Errors and Result Serialization

## Error taxonomy (`omnigraph::error::OmniError`)

- `Compiler(...)` — schema/query parse/typecheck errors
- `Lance(String)` — storage layer
- `DataFusion(String)` — execution layer
- `Io(io::Error)`
- `Manifest(ManifestError { kind: BadRequest|NotFound|Conflict|Internal, details: Option<ManifestConflictDetails>, … })`
  - `ManifestConflictDetails::ExpectedVersionMismatch { table_key, expected, actual }` — caller's `expected_table_versions` did not match the manifest's current latest non-tombstoned version (set by `OmniError::manifest_expected_version_mismatch`).
  - `ManifestConflictDetails::RowLevelCasContention` — Lance row-level CAS rejected the publish because a concurrent writer landed the same `object_id`. Retried internally by the publisher; only surfaces if the retry budget exhausts.
  - **D₂ parse-time rejection** (MR-794): a single mutation query that mixes inserts/updates with deletes errors out *before any I/O* with kind `BadRequest`. Message: `mutation '<name>' on the same query mixes inserts/updates and deletes; split into separate mutations: (1) inserts and updates, then (2) deletes`. See [docs/query-language.md](query-language.md) for the rule and [docs/runs.md](runs.md) for the underlying staged-write rationale.
- `MergeConflicts(Vec<MergeConflict>)`

Compiler-side `NanoError` covers parse / catalog / type / storage / plan / execution / arrow / lance / IO / manifest / unique-constraint, each with structured spans (`SourceSpan { start, end }`) for ariadne-style diagnostics.

## Result serialization (`omnigraph_compiler::result::QueryResult`)

- `to_arrow_ipc()` — efficient binary
- `to_sdk_json()` — JS-safe JSON (large i64 wrapped in metadata)
- `to_rust_json()` — Rust-friendly JSON
- `batches()` — direct Arrow `RecordBatch` access

Mutation results: `{ affectedNodes: usize, affectedEdges: usize }` (also exposed as a tiny Arrow batch).
