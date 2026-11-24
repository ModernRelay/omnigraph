# JSON output through the Arrow writer

**Decides:** every JSON body Omnigraph writes for graph data is the `arrow-json` writer's rendering, with default options, of the record batches the engine produced, and no Omnigraph code decides how a cell type is spelled.
**Contract:** keeps E5
**Code:** `crates/omnigraph-compiler/src/json_output.rs`, `crates/omnigraph-compiler/src/result.rs`, `crates/omnigraph/src/db/omnigraph/export.rs`, `crates/omnigraph-api-types/src/lib.rs`, `crates/omnigraph-cli/src/read_format.rs`
**Cited as:** RFC 0051

## Context

A hand-written match over Arrow types per surface (query results; entity
fetch and export) that builds a `serde_json::Value` per cell drifts: the
copies spell types differently, and a fix lands in one and misses the other. A
`serde_json::Value` holds only 64-bit numbers, so a 32-bit float is widened
before printing and `0.99` reads back as `0.9900000095367432`, and every
narrower type meets the same defect. The Arrow project's writer formats each
cell at its own width and writes bytes directly.

## Decision

- Query results (`QueryResult::to_json_bytes`), export lines and change images
  (`to_json_lines`) all go through `arrow_json::WriterBuilder::new()` with no
  options set.
- The spelling of each property type is a contract pinned by
  `every_catalog_type_renders_to_the_documented_spelling` against checked-in
  bytes. An `arrow-json` upgrade that changes a spelling turns that test red
  and ships as a wire change with a release note, never through a builder
  option.
- Omnigraph owns four pieces only: the `base64:` substitution of `Blob` cells
  on export and entity fetch, done before the writer; a range check on `Date`
  and `DateTime` columns before the writer, because the writer prints an
  `ERROR:` string for a count it cannot format; the export line envelope with
  `id` first; and re-inserting explicit nulls in change images, where an absent
  key means the property was outside that commit's schema.
- A null cell's key is omitted elsewhere. A non-finite float renders as
  `null`; stored data cannot hold one.
- A cell the writer cannot render is a typed error, never an empty string or a
  silent `null`. The loader refuses a date count the writer cannot format.
- `ReadOutput.rows` is `Box<RawValue>`: JSON text validated once and passed
  through by the server without parsing.
- Duplicate return-column names are refused by the typechecker, since the
  writer would emit both keys.
- The CLI's `--format arrow` writes the result batches of a query its own
  engine runs on a direct target as an Arrow IPC stream; a served or managed
  target, a terminal stdout and `alias` refuse it.

## Rejected

- **Share more helpers between the two encoders:** keeps two copies of
  everything the helpers miss, the failure that motivated the change.
- **An Omnigraph byte writer:** keeps a per-type match in Omnigraph code, the
  place spelling defects are written.
- **`arrow-json` with per-type overrides to keep every old spelling:** each
  override is Omnigraph deciding a spelling again, for behavior no caller
  depends on.
- **`serde_json` with `arbitrary_precision`:** a workspace-wide feature that
  changes `Number` for every crate and leaves both encoders in place.
- **A separate route, or the Arrow file format, for IPC:** a second route
  duplicates the request contract and its policy path; the file format needs
  the whole result before its footer.

## Not built

- `Accept: application/vnd.apache.arrow.stream` on `POST /query` and stored
  query invocation, with envelope fields as response headers and 406 for an
  unsupported `Accept`. The query routes answer JSON only.
