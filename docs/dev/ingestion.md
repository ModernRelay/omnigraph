# Ingestion

OmniGraph has one durable ingestion model: a bounded batch becomes visible through the ordinary `Load` writer and one graph commit. There is no separate streaming database, WAL, token ledger, lane lifecycle, or acknowledgement tier.

The rejected designs are retained in [RFC 0018](../rfcs/0018-ingest-wal.md) and [RFC 0026](../rfcs/0026-memwal-streaming-ingest.md). They are history, not implementation guidance.

## Current surfaces

### Strict graph batch

New HTTP integrations should send raw `application/x-ndjson` to:

```text
POST /graphs/{graph_id}/load/ndjson
```

The engine equivalent is `load_graph_batch_as`. Each nonblank line is exactly one logical envelope:

```json
{"type":"Person","data":{"id":"p1","name":"Ada"}}
{"edge":"Knows","from":"p1","to":"p2","data":{"since":2026}}
```

The strict parser rejects duplicate members, unknown fields, reserved physical columns, malformed envelopes, and noncanonical supplied node IDs. It accepts logical type and property names only; callers never select physical datasets, lanes, fragments, or bindings.

The HTTP handler authorizes both change and any requested branch creation before polling the body. It verifies `Content-Type`, rejects an oversized `Content-Length` early, and otherwise collects at most 32 MiB. The parser also bounds individual lines and per-table retained rows/Arrow bytes before durable effects.

### Compatibility loader

`POST /graphs/{graph_id}/load` and the engine `load_as` accept a JSON envelope whose `data` field contains loader-compatible NDJSON. This preserves historical coercions and shapes that the strict graph-batch boundary intentionally refuses.

`POST /graphs/{graph_id}/ingest`, the CLI `ingest` command, and the engine `ingest*` methods are deprecated compatibility aliases over the same loader. They do not have a separate fast path or durability contract. New integrations use `load` or `/load/ndjson`.

## Transaction contract

All surfaces converge on the same writer:

1. Resolve the target branch and optionally fork it from an explicit base.
2. Capture one accepted catalog and branch snapshot.
3. Parse, canonicalize, coalesce, validate, and enforce referential/cardinality constraints.
4. Stage at most one exact Lance transaction per touched table.
5. Arm ordinary recovery-v9 before the first durable table effect.
6. Commit all table effects, then publish their versions and graph lineage in one manifest CAS.
7. Return success only after the graph commit is durable and visible.

A missing target branch is created only when the caller explicitly supplies a base (`from` over HTTP/CLI). Without a base, a missing branch is an error; a typo never forks implicitly.

Load modes are shared across the surfaces:

- `merge` upserts by exact `id`;
- `append` strictly inserts absent IDs;
- `overwrite` replaces each named table's contents.

One batch may touch several logical declarations. Cross-table visibility is still atomic: readers see all published table versions or none. An error after recovery is armed returns recovery-required rather than claiming rollback or success.

The write envelope is bounded before arm. Keyed work is limited to 8,192 retained rows and 32 MiB of exact retained Arrow data per table, with operation-wide limits for carried external Blob payloads and other retained plans. The HTTP strict-batch body has its own 32 MiB ceiling. These limits bound one commit; clients split larger imports into explicit batches and accept one graph commit per batch.

## Blob and index behavior

External Blob references pass the graph's deny-by-default source policy. Overwrite may retain an allowed reference; keyed modes materialize selected external bytes within the same bounded preflight. See [blob.md](blob.md).

Load publishes logical data only. Declared physical indexes are derived and reconciled by `ensure_indices` or `optimize`; ingestion does not synchronously train ANN or FTS indexes.

## Why there is no MemWAL path

The removed experiment could durably admit rows into per-dataset Lance MemWALs, but OmniGraph still needed graph-level validation, cross-table ordering, recovery, fold, and one manifest publication. That extra coordinator duplicated authority and paid the graph commit cost later rather than removing it.

Current cluster configuration therefore rejects removed `streaming` declarations. Current manifests contain no stream profile, `_mem_wal`, `_stream_tokens`, lifecycle revision, receipt, or durable-but-not-visible acknowledgement state. A graph written by an unreleased experimental format must be exported with its matching binary and rebuilt.

If an application needs durable acceptance before graph visibility, put an external log or queue in front of OmniGraph. Its consumer submits bounded idempotent batches and checkpoints the external offset only after OmniGraph acknowledges the graph commit. That keeps each system's durability promise explicit.

## Test owners

- Parser, batching, branch creation, modes, bounds, and actor attribution: in-source `crates/omnigraph/src/loader/` tests.
- Atomic table staging and recovery: `crates/omnigraph/tests/writes.rs`, `recovery.rs`, and `failpoints.rs`.
- Strict and compatibility HTTP routes: `crates/omnigraph-server/tests/data_routes.rs`, auth coverage, and `openapi.rs`.
- CLI local/remote behavior and deprecation signal: `crates/omnigraph-cli/tests/system_local.rs`, `system_remote.rs`, and `parity_matrix.rs`.
- Absence of retired durable surfaces: `crates/omnigraph/tests/forbidden_apis.rs` and configuration rejection tests.
