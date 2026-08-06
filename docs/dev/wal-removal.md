# Ingestion after RFC-026

**Status:** implemented decision (2026-08-06)

RFC-018 and RFC-026 are rejected as product architecture. Their documents are
retained as historical design evidence, but they do not describe supported
storage, recovery, API, CLI, or operator behavior.

## Decision

High-rate ingestion is a transport profile over the ordinary graph writer, not
a second durability subsystem. `POST /graphs/{graph_id}/load/ndjson` accepts a
bounded raw `application/x-ndjson` graph batch with one strict logical node or
edge envelope per nonblank line. `data` may be omitted and defaults to `{}`;
IDs may be omitted and retain ordinary Load semantics (canonical `@key` node
IDs or generated IDs). The boundary rejects duplicate members, unknown or
physical fields, malformed values, and supplied noncanonical node IDs before
calling the shared Load transaction machinery. That machinery commits the
affected Lance tables through ordinary recovery-v9 and makes them visible in
one `__manifest` publication. The request is acknowledged only after that
graph commit is durable and visible.

There is no OmniGraph or Lance MemWAL firehose path, durable-but-not-visible
waiting room, per-table stream lifecycle, token ledger, hidden stream column,
or stream-specific recovery grammar. Producers that require durable acceptance
before graph visibility should put an external graph-level log such as Kafka or
Kinesis in front of bounded OmniGraph loads.

## Why

The implemented MemWAL path optimized per-dataset acceptance while OmniGraph's
unit of correctness is one graph commit spanning multiple datasets. It still
needed a second coordinator for validation, cross-table visibility, token
authority, replay, fold, correction, and lifecycle management. Benchmarking
showed that this coordination dominated the path and left it orders of
magnitude slower than direct load. The architecture bought a different
acknowledgement semantic, not useful ingestion throughput.

The lower-liability design reuses the graph transaction that already owns
validation, recovery, and visibility. It has one durability boundary and one
source of truth.

## Storage-format consequence

The current binary serves exactly internal manifest schema v6. V5 and v6 keep
the useful stable-table-identity and exact non-null `id` fencing work. The
unreleased v7-v19 development formats belonged to the rejected WAL design and
are abandoned; they are never decoded, migrated, or reinterpreted by the v6
binary. The last released v0.8.x format is v4 and crosses to v6 by
export/init/load rebuild.

Recovery sidecars remain at ordinary schema v9. Recovery-sidecar versions and
manifest-schema versions are separate version spaces.

## Historical material

- [RFC-018](../rfcs/0018-ingest-wal.md) records the original WAL proposal.
- [RFC-026](../rfcs/0026-memwal-streaming-ingest.md) records the implemented
  experiment and its accumulated correctness machinery.
- [Firehose path specs](firehose-path-specs.md) retain the implementation
  sequence and acceptance evidence; they are not an active plan.
