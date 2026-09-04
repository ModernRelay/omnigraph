---
rfc: "0051"
title: "JSON output via Arrow"
track: maintainer
status: draft
implementation: not-started
authors:
  - azimafroozeh
created: 2026-09-04
updated: 2026-09-04
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0051: JSON output via Arrow

## Summary

Every JSON body OmniGraph writes for graph data, whether a query result, an entity fetch, or an export line, will be the `arrow-json` rendering of the Arrow record batches the engine already produces, with the crate's default options. OmniGraph will keep no per-type code that decides how a cell is spelled in JSON. A client that needs exact types will request Arrow IPC, the Arrow columnar interchange stream, from the query routes instead of JSON.

Today every value is first copied into a `serde_json::Value` and then serialized; that copy is where the code, the latency, and the narrow-float defects live, and it exists twice, once for query results and once for export and entity fetch. Both paths move to the one writer. OmniGraph keeps three small pieces of its own: the `base64:` blob substitution on export and entity fetch, a range check on date columns, and the export line envelope.

What Lance stores is untouched. Two inputs that succeeded silently become typed errors: a query whose return names collide, and a date count the formatter cannot render. What else changes is the JSON spelling of a small, listed set of cell types and the deletion of two hand-written JSON encoders.

## Motivation

OmniGraph writes JSON for graph cells in two places. `omnigraph_compiler::json_output::array_value_to_json_with_mode` renders query results, and `omnigraph::db::omnigraph::json_value_from_array` renders entity fetches and export lines. Each is a match over Arrow types that builds a `serde_json::Value` per cell, and the two disagree on how several types are spelled:

| Arrow type | Query results | Entity fetch and export |
|---|---|---|
| `Date32` | `"2024-01-01"` | `19723`, the day count |
| `Date64` | `"2024-01-01T00:00:00.000Z"` | `1704067200000`, the millisecond count |
| `Blob` (logical `LargeBinary`; stored as a descriptor struct) | not projected; the typechecker rejects the access | export: `base64:`-prefixed base64, substituted before the encoder; entity fetch: the stored blob descriptor rendered as an object, no substitution (exact shape confirmed in phase 2) |
| `Float64` NaN or infinity | `"NaN"`, `"Infinity"`, `"-Infinity"` as strings | an error |
| `Int64` beyond 2^53 | a string in `to_sdk_json` (the JavaScript mode), a number in `to_rust_json` (the native mode) | a number |
| `LargeList` | a quoted display string | a dedicated arm |
| downcast failure | silently `null` | a typed error |

Two consequences follow. First, a rendering fix lands in one encoder and misses the other: issue #618 found that an `F32` cell stored from `0.99` came back as `0.9900000095367432`, and closing it requires the same change in both encoders. Second, a `serde_json::Value` holds only `f64`, `i64`, and `u64`, so every narrower numeric type must be widened before it is printed, and the widening is where the `F32` defect lived. Any future narrower type, such as a 16-bit float, would meet the same defect.

`arrow-json`, the Arrow project's own JSON writer, already sits in `Cargo.lock` as a transitive dependency of DataFusion and Lance. It formats each cell at the cell's own Arrow width and writes bytes directly, with no intermediate `serde_json::Value`. Polars and the Arrow reference writer take the same shape. Adopting it removes both encoders and the class of defect they carry, at the cost of a small, enumerable set of spelling changes.

An issue or local refactor is not enough because the change moves a wire contract, the JSON spelling of dates in export lines, adds an HTTP content type, and sets a rule that later types must follow. Those are decisions the RFC registry exists to record.

## User and operational behavior

**Three rules define every JSON body.**

1. Every JSON body OmniGraph writes for graph data is the `arrow-json` rendering of the result batches with the crate's default options; the spellings that result are in the table below.
2. A `Blob` cell is written as `base64:` followed by base64 in the RFC 4648 standard alphabet with `=` padding of its managed bytes, or as the stored URI string for an external reference, on export and entity fetch, the two surfaces that carry a blob column; the column is substituted before the writer and is the only cell the writer does not spell. Query results never carry a blob column.
3. A cell the writer cannot render is a typed error, never an empty string. The writer itself never fails on a date: for a day or millisecond count outside the range it can format (the range where `arrow_array::temporal_conversions::date32_to_datetime` and `date64_to_datetime` return `None`, chrono's representable years) it writes an `ERROR:` string, so OmniGraph checks every `Date` and `DateTime` column against that range before the writer and raises the typed error itself. The loader applies the same range check to a raw date count on import. Two spellings that follow from the defaults are stated here so no consumer discovers them: a null cell's key is omitted from the row object, at row level and inside a struct, and a non-finite float is written as `null`; stored data cannot hold a non-finite float, so only a computed column can produce one.

**The spelling of every property type is fixed by this table, not by the crate's source.**

| Property type | JSON spelling |
|---|---|
| `String` | JSON string |
| `Bool` | `true`/`false` |
| `I32`, `I64`, `U32`, `U64` | bare JSON number at any width (integers beyond 2^53 are bare numbers on every route, today and after this RFC; `JSON.parse` rounds them; a client needing exact integers requests Arrow IPC) |
| `F32` | shortest decimal that reads back to the same 32-bit value (`0.99`); integral values carry `.0` (`1.0`); `-0.0` is kept; exponent form at the `F64` thresholds |
| `F64` | shortest decimal that reads back to the same 64-bit value; integral values carry `.0`; exponent form from 1e10 (`1.0e10`) and below 1e-5 (`1.0e-7`) |
| `Date` | `"2024-01-01"` |
| `DateTime` | `"2024-01-01T12:34:56.789"`: three fractional digits when the millisecond part is non-zero, none when it is zero, no `Z` (UTC wall-clock) |
| `Vector(N)` | array of N `F32` numbers |
| `[T]` | array; a null element is `null` |
| `T?` | the key is omitted when the value is null, at row level and inside a struct |
| `enum(...)` | the variant as a JSON string |
| `Blob` | rule 2 |

The differential test in Evidence and tests enforces this table. No property type reaches the writer as a struct; the differential test covers structs only as writer input.

**Query results keep their envelope; only the listed spellings change.** The response body keeps its shape: a `ReadOutput` object whose `rows` field is an array of objects keyed by column name in schema order; from phase 1 null cells are omitted. The bytes change only for the phase 1 rows in the table below. `POST /read` keeps its envelope byte-stable as documented; the cell spelling inside its `rows` follows this RFC like every query route, and `Accept` is ignored on `POST /read`.

**Export lines keep their grammar and change four spellings.** An export line is `{"type":<node type>,"data":<row>}` or `{"edge":<edge type>,"from":<source id>,"to":<destination id>,"data":<row>}`, where `<row>` is the writer's rendering of the row in schema column order with `id` first, `src` and `dst` removed, and `Blob` columns substituted per rule 2. That grammar is unchanged. `<node type>`, `<edge type>`, and every id are JSON strings. Dates and datetimes become the strings in the spelling table instead of raw counts. Null cells drop their key; the loader reads a missing key as null. Floats take the spelling table's exponent form. No version field is added: a `Date` or `DateTime` cell is a number in a line written before phase 2 and a string after; a file may mix both and loads either way.

**Arrow IPC is selected by the `Accept` header on the query routes.** Content negotiation on `Accept` is new to the server. It applies to `POST /query` and to stored-query invocation; the deprecated `POST /read` always answers JSON. An `Accept` that is absent, `*/*`, or `application/json` receives JSON. An `Accept` whose first supported token is exactly `application/vnd.apache.arrow.stream` (parameters ignored) receives the result as an Arrow IPC stream with that response `Content-Type`; an `Accept` list naming neither form receives 406 with the `ErrorOutput` body; any other media range, `application/*` included, counts as naming neither form. The IPC body carries the same batches the JSON body would serialize, after policy and projection have been applied. The envelope fields travel as response headers: `Omnigraph-Graph-Commit-Id`, `Omnigraph-Query-Name`, `Omnigraph-Branch`, and the existing `Omnigraph-Snapshot-Id`; `row_count` is the sum of batch lengths and `columns` is the IPC schema. The CLI gains `--format arrow`, writing that stream to stdout.

**Spelling changes a JSON consumer can observe.** Each line is the complete list for its phase; nothing outside it changes.

| Phase | Surface | Before | After |
|---|---|---|---|
| 1 | query results | `"NaN"`, `"Infinity"`, `"-Infinity"` strings for non-finite floats | `null`, the writer's spelling; only a computed column can produce one |
| 1 | query results | `LargeList` through the display fallback (a quoted string) | an array like `[T]` |
| 1 | query results | `F32` scalars and `Vector(N)` cells as the widened 64-bit digits (`0.9900000095367432`, while #618 is open) | shortest digits at 32-bit width (`0.99`), the #618 defect closing |
| 1 | query results | floats in exponent form as `1e+20` or `1e-7`, and floats from 1e10 to below 1e16 as plain digits (`10000000000.0`) | `1.0e20`, `1.0e-7`, and `1.0e10`; the parsed value is unchanged |
| 1 | query results | `DateTime` as `"2024-01-01T12:34:56.789Z"` | `"2024-01-01T12:34:56.789"`: no `Z`, and no fractional part when it is zero |
| 1 | query results | a null cell as `"note":null` | the key is omitted from the row |
| 1 | query results | duplicate return-column names collapse to the last value | the planner refuses a query whose return names collide (a typed error) |
| 1 | query results | a cell the encoder cannot render is `""` or a silent `null` | status 500 with the `ErrorOutput` body |
| 1 | import | a `Date` or `DateTime` count outside the formatter's range loads | the loader refuses the line with a typed error |
| 2 | entity fetch | a `Blob` as the descriptor object | `base64:`-prefixed base64, the export spelling |
| 2 | entity fetch, export | dates and datetimes as raw counts | the strings in the spelling table; the loader already accepts both forms |
| 2 | entity fetch, export | floats in exponent form as `1e+20` or `1e-7`, and floats from 1e10 to below 1e16 as plain digits | `1.0e20`, `1.0e-7`, and `1.0e10`; the parsed value is unchanged |
| 2 | entity fetch, export | a null cell as `"note":null` | the key is omitted, the phase 1 spelling; the loader reads a missing key as null |
| 3 | query routes | JSON only | JSON, or Arrow IPC on request |

**A render error is loud on every surface.** On `POST /query` a render error is status 500 with the `ErrorOutput` body and no `rows`. On export the stream ends after the last complete line and the export call returns the typed error; the file carries no in-band truncation signal. Entity fetch, an engine call with no HTTP route, returns the `OmniError`. Today the query-result encoder swallows display errors into an empty string; that behavior ends.

## Design

**One writer replaces two.** `QueryResult` gains `to_json_bytes()`, built on `arrow_json::WriterBuilder` with its default options, producing the row array. The engine's export and entity paths call the same writer on their one-row batches. No omnigraph code matches on Arrow types to produce JSON. No schema type maps to an Arrow `Timestamp`. Duplicate return-column names are refused by the planner: the old encoder collapsed them to the last value, and the writer would emit both keys.

**Rows travel as text.** `ReadOutput.rows` and `LegacyReadOutput.rows` become `Box<serde_json::value::RawValue>`, JSON text carried without parsing; the OpenAPI schema for `rows` is unchanged. The three server handlers pass the text through unchanged. The CLI parses it for the `table`, `csv`, and `kv` formats; `--format json` keeps its pretty-printed envelope with the writer's compact bytes inside `rows`, and `--format jsonl` re-splits the row array. `QueryResult::to_rust_json()` remains for in-tree callers as a parse of the bytes; the server uses that native mode today, and `to_sdk_json`, the JavaScript mode, has only in-tree test callers.

**OmniGraph owns the blob substitution, the date-range check, and the export envelope.** The `base64:` substitution for `Blob` cells on export and entity fetch, applied before the writer. The range check on `Date` and `DateTime` columns before the writer. The export line envelope and its key order. Nothing else that spells a cell.

**The IPC route serializes the batches the JSON route serializes.** `QueryResult::to_arrow_ipc()` exists in `omnigraph-compiler` and is unused by the server. The query handlers select it on the `Accept` header after the same policy and projection steps the JSON path runs. The read policy runs before the result is built, so both bodies serialize identical batches.

**`arrow-json` decides every other spelling.** `arrow-json` decides the digits of every number, dates, datetimes, nulls, the escaping of strings, and the shape of lists and structs. OmniGraph decides the row array shape, the pieces listed above, and which content type a request receives.

## Invariants

- **Integrity failures are loud (invariant 8).** The query-result encoder's swallowed display errors and silent `null` on downcast failure end; the writer's errors propagate as typed outcomes. The deny-list entry "swallowed errors" is the shape being removed, not introduced. A non-finite float is written as `null`; the rule that states it is in User and operational behavior, so the spelling is documented rather than silent, and stored data can never contain one.
- **Query semantics are typed structures (invariant 9).** Rendering moves from a hand-written match to a typed writer over the Arrow schema; no semantics move into strings or transport flags. The `Accept` header selects a serialization, not a query meaning.
- **Trust is established at the boundary and enforced at the engine (invariant 10).** The Arrow IPC route serializes batches after the same policy and projection the JSON route applies. Nothing bypasses the read policy.
- **One source of truth, cheaply derived (invariant 12).** One writer replaces two; the JSON spelling of a type is derived from its Arrow type in one place.
- **Evidence matches the boundary (invariant 13).** The changed contract is the JSON spelling, so the evidence is a differential test over every Arrow type OmniGraph can produce, and the `.gqt` corpus that compares result rows. Both are named below.

No invariant is weakened. No deny-list item is invoked.

## Compatibility and reversibility

**Query-result bytes and parsed values change only for the listed phase 1 rows.** The bytes are unchanged except for the phase 1 rows in User and operational behavior; of those only the exponent-form row keeps its parsed value. The exponent-form, `DateTime`, and omitted-null rows are observable on any stored graph; the `F32` row only while #618 is open. Export lines change the spelling of dates, datetimes, exponent-form floats, and null cells in phase 2; the loader accepts both the raw count and the string today, so lines from before and after the change load identically. Export blob lines are unchanged.

**Storage is untouched.** The writer reads batches; it writes nothing to Lance or the manifest.

**An older server reads new export lines.** The loader's date parsing accepted strings, and a missing key read as null, before this RFC. An older client parsing new query results sees the phase 1 spellings and nothing else.

**The spelling table is the contract; `arrow-json` is pinned by the lock file.** The JSON spelling of every property type is the table in User and operational behavior. `arrow-json` is pinned by `Cargo.lock` (58.3.0 today); phase 1 adds it under the workspace `arrow-*` requirement `"58"`, and a lock bump is the review point. An upgrade that changes any spelling in that table is a wire change and is treated as one: the differential test turns red, and the change ships with a release note and an updated spelling table, never silently and never through a builder option.

**Rust consumers of `omnigraph-api-types` see `rows` change type.** `ReadOutput.rows` and `LegacyReadOutput.rows` change type in phase 1; `omnigraph-api-types` enables the `serde_json/raw_value` feature.

**Each phase reverts alone.** Phase 1 and phase 2 each delete one encoder; reverting a phase restores its encoder from history. The Arrow IPC route is additive and can be removed without touching JSON.

## Alternatives

- **Do nothing beyond the #618 fix.** The fix shares one helper between the two encoders. The two matches, their per-type disagreements, and the widening through `serde_json::Value` all remain. The next narrow type repeats the defect.
- **Share more helpers between the two encoders.** Removes the widening for the types the helpers cover, keeps two copies of everything else. This is the minus-one-mechanism design: no new writer, no new route. It fails the case that motivated the RFC, a fix landing in one encoder and not the other, because the encoders still exist.
- **A byte writer of OmniGraph's own.** Removes the duplication and the widening but keeps a per-type match in OmniGraph code, which is the surface where spelling defects are written. `arrow-json` is that writer, already compiled into the binary, maintained upstream.
- **`arrow-json` with per-type encoder overrides to preserve every current spelling byte for byte.** Keeps today's non-finite strings, the JavaScript integer mode, and the raw-count dates in export by overriding the writer's encoders. Each override is OmniGraph code deciding a spelling, which is the thing being removed, and each preserves a behavior no caller depends on. The date formats and the option that keeps null keys were considered as builder options rather than overrides and rejected for the same reason: each is OmniGraph deciding a spelling.
- **`serde_json` with the `arbitrary_precision` feature.** Numbers carry their digit strings through `serde_json::Value`, so narrow floats survive. It is a workspace-wide feature flag that changes `Number` for every crate, allocates a string per number, and leaves both encoders in place.

- **A separate route for Arrow IPC, or the Arrow file format instead of the stream.** A second route duplicates the query request contract and its policy path; `Accept` selects a serialization of one request and keeps one contract. The stream format carries batches as they are produced; the file format needs the whole result before its footer can be written. Both rejected for phase 3.

**Precedent audit.** Export already writes bytes to its sink line by line, and both encoders' display fallback already lets `arrow_cast` render cells; both are the "let Arrow render" shape this RFC extends to JSON. Arrow IPC for results already exists as `QueryResult::to_arrow_ipc`; the route exposes it rather than adding a second serialization.

## Evidence and tests

- **Differential test (new, phase 1 gate, kept permanently).** In `omnigraph-compiler`: generated record batches over every Arrow type the catalog can produce, including nulls, nested lists, `Vector(N)`, structs, `LargeUtf8`, dates and datetimes, and integers beyond 2^53, rendered by the old encoder and by the new writer. While the old encoder exists the test compares the parsed `serde_json::Value` of every cell and asserts equality except for the phase 1 rows; after phase 1 deletes the old encoder the test compares against checked-in expected bytes, kept in the in-source test beside `to_json_bytes`, and becomes the guard against upstream spelling drift. The generated batches include a hand-built `Float32` column holding NaN and infinity, and an out-of-range date, so the `null` spelling and the date-range check are exercised. Phase 1 also verifies that the mutation list builders and the schema-default path route every float through the existing `checked_*` guards. Phase 1 confirms by running one query with colliding return names that the refusal is the planner's, not DataFusion's.
- **GQ logic tests (existing owner, RFC 0045).** Every case compares expected rows to actual rows; the corpus must stay green with no expectation edited in phase 1. Phase 1 adds a case pinning `F32` scalars and `Vector(N)` cells. The corpus holds no null-cell expectation today; after phase 1 the harness's parsed rows carry no null keys.
- **Export round trip (existing owner, `crates/omnigraph/tests/export.rs`).** Extended with a date and a datetime property exported and re-imported, asserting the string spelling on the line and equality after reload, and with an out-of-range date count, asserting the loader's typed refusal.
- **Server and CLI (existing owners).** The route suites `data_routes` and `openapi` in `crates/omnigraph-server/tests` gain response-shape assertions for `ReadOutput` and a test that one query returns identical batches as JSON and as Arrow IPC; the OpenAPI drift test is regenerated for the new content type; `crates/omnigraph-cli/tests/cli_data.rs` gains `--format arrow`. Entity fetch: `crates/omnigraph/tests/end_to_end.rs`, the `entity_at` typed-values test, extended in phase 2 with a `Blob` property, asserting the `base64:` spelling.
- **Surveyed.** `arrow-json` 58.3.0 writer options and encoders; the Polars JSON writer; the loader's date parsing; every caller of both encoders across the workspace.

## Rollout

1. **Query results via `arrow-json`.** Add the direct dependency, `to_json_bytes`, the differential test, the `RawValue` rows, the CLI parse, the planner's refusal of colliding return names, the loader's refusal of out-of-range date counts, and the date-range check before the writer; delete the JavaScript mode and the compiler's encoder. Ships alone; the query-result byte changes are the phase 1 rows in the behavior table. Phase 1 assumes the #618 fix has landed; if it has not, phase 1 carries the engine-encoder half of that fix. `implementation` advances to `partial`.
2. **Entity fetch and export via the same writer.** Delete the engine's encoder; entity fetch and export take the phase 2 rows of the behavior table; release note. `implementation` stays `partial`.
3. **Arrow IPC on the query routes.** Content negotiation, CLI `--format arrow`, OpenAPI and user docs. `implementation` advances to `complete`.

Each phase is one pull request and is independently revertible.

## Unresolved questions

None that block acceptance.

## Decision log

None yet.
