---
type: spec
title: "RFC-026 — MemWAL streaming ingest"
description: Adopts Lance MemWAL as OmniGraph's strategic streaming-write architecture, with durable per-row acknowledgement, graph-atomic folds, epoch-fenced quiescence, and explicit fresh-read cuts on the RFC-022 unified write path.
status: draft
tags: [eng, rfc, streaming, ingest, wal, memwal, lance, omnigraph]
timestamp: 2026-07-10
owner:
---

# RFC-026 — MemWAL streaming ingest

**Status:** Draft / for team review
**Date:** 2026-07-10
**Depends on:** [RFC-022](rfc-022-unified-write-path.md)'s unified write and
generic recovery-sidecar protocol, plus
[RFC-023](rfc-023-key-conflict-fencing.md) for the initial keyed graph-stream
mode. Durable heads from
[RFC-024](rfc-024-durable-table-heads.md) are compatible but not required.
**Surveyed:** omnigraph 0.8.1; Lance 9.0.0-beta.15 (`f24e42c1`); complete MemWAL format specification
**Audience:** engine, server, CLI, policy, and operations maintainers
**Open architecture review:** [RFC-022–027 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

## 0. Decision and risk posture

OmniGraph adopts Lance MemWAL as its strategic streaming-write architecture.
MemWAL is a major Lance architectural bet: a sharded LSM write path with durable
WAL entries, flushed Lance generations, merge progress committed with base-table
data, maintained indexes, and epoch-fenced writers. OmniGraph consumes that
architecture rather than building a WAL, shard protocol, or LSM reader.

This RFC does **not** characterize the architecture as experimental. The risk is
narrower: Rust API names, some format details, and operational helpers are still
maturing across Lance releases. We manage that API/format-maturity risk with a
small adapter, compile/runtime surface guards, a quiescence requirement before
Lance upgrades, and a fresh full-spec alignment audit on every bump. It is not a
reason to fork or reimplement MemWAL.

The contract is:

- stream acknowledgement means the row's WAL entry is durable;
- acknowledgement does not mean graph visibility;
- default queries see only the manifest-committed graph;
- a fold is an ordinary RFC-022 graph writer and is the sole visibility point;
- fresh reads are explicit and never claim cross-table atomicity.

## 1. Scope and non-goals

This RFC specifies enrollment, the public stream API, acknowledgement semantics,
folding, fold-time integrity, dead-letter atomicity, branch/schema quiescence,
fresh-read cuts, resource bounds, observability, testing, and upgrade posture.

It does not replace `load` or `mutate`, provide cross-query transactions, store
manifest mutations in MemWAL, create a second metadata authority, or weaken
default snapshot isolation. Stream-mode
deletes remain out of the first delivery and require the Lance tombstone surface
plus a separate acceptance pass.

## 2. Stream mode and key semantics

Initial delivery exposes
`@stream(mode="upsert", on_reject="strict")` on a node or edge type;
`on_reject` accepts `strict` or `dead_letter`.
It requires the table's immutable unenforced primary key to equal OmniGraph's
merge key: `id` for nodes and edges. All occurrences of one key map to one shard
and MemWAL applies last-write-wins ordering.

Public append mode is deliberately out of scope. Nodes and edges always have
logical identity; allowing a retry to append the same `id` twice would violate
that contract. A future explicitly keyless, non-graph append-only table class
may consume MemWAL append semantics under its own schema/API decision.

Stream ordering intentionally differs from the interactive fence:

- concurrent interactive same-key writes serialize or fail/retry loudly;
- same-key stream entries resolve by MemWAL generation/position order;
- duplicate keys inside one bulk-load input retain the existing load error.

The schema and user docs state all three together.

## 3. Enrollment is a recoverable inline commit

Schema apply records `@stream` intent only. First stream use enrolls the physical
table by creating the singleton `__lance_mem_wal` system index and its sharding
configuration.

Enrollment advances Lance HEAD inline. It therefore uses the RFC-022 generic
recovery protocol, not an ad-hoc state machine:

1. run and await RFC-022's synchronous recovery barrier;
2. authorize, pin the manifest/schema/table state, and prepare a complete
   `ReadSet` containing schema identity, table entry/head, PK metadata, stream
   intent/configuration, and lifecycle-row absence;
3. acquire any global claims and then the `(table, branch)` write queue in
   RFC-022 order, then freshly revalidate the complete `ReadSet`; a mismatch restarts
   before any inline effect;
4. verify RFC-023's already-installed PK; enrollment never performs a first-use
   PK migration; validate the sharding configuration;
5. write a generic sidecar with writer kind `stream_enrollment` and pre-commit
   table pin;
6. create the MemWAL index, advancing Lance HEAD;
7. publish the new table version plus `stream_state = OPEN` in one manifest CAS,
   including the table-head row when RFC-024 is active;
8. delete the sidecar best-effort after publication.

Recovery rolls the enrollment forward or back under the same classification and
audit machinery as other inline residuals. Repeating enrollment with identical
metadata is a no-op. A different PK, sharding spec, maintained-index set, or
writer-default configuration is a typed conflict.

No row is acknowledged until enrollment is manifest-committed.

Initial delivery supports one unsharded shard per `(table, main)`. Non-main
branches remain refused until the Lance branch-scoping question is proven by a
surface guard and end-to-end test. Later `bucket(id, N)` sharding must preserve
the one-key-to-one-shard rule.

## 4. New public API

The shipped `POST /graphs/{id}/ingest` path remains the deprecated, compatible
alias of `/load`. Streaming receives a new, non-conflicting surface:

```text
POST /graphs/{graph_id}/streams/{type_name}/ingest?branch=main
GET  /graphs/{graph_id}/streams
GET  /graphs/{graph_id}/streams/{type_name}
POST /graphs/{graph_id}/streams/{type_name}/fold
POST /graphs/{graph_id}/streams/{type_name}/quiesce
POST /graphs/{graph_id}/streams/{type_name}/resume
```

The ingest request and response use `Content-Type: application/x-ndjson` and
`Accept: application/x-ndjson`.

Each input line is one row payload. Each output line corresponds to the same
input ordinal:

```json
{"ordinal":17,"status":"durable","shard_id":"...","writer_epoch":8,"wal_position":42}
```

Synchronous validation failures return a per-row error before a WAL append.
Previously acknowledged rows in the same request remain durable; the response
is a stream, not an all-request transaction. Ordering, cancellation, and retry
rules are explicit:

- acknowledgements are emitted in input order for one HTTP stream;
- disconnecting does not cancel entries whose durability waiter resolved;
- a missing response is ambiguous; retrying the same `id` and payload may add
  another WAL entry but produces the same last-write-wins graph state;
- server shutdown stops admission, drains durability waiters up to a bound, and
  reports any unacknowledged tail as unknown to the client.

CLI commands mirror the new namespace rather than overloading deprecated
`omnigraph ingest`:

```text
omnigraph stream ingest <type> --data <ndjson> ...
omnigraph stream status [<type>] ...
omnigraph stream fold [<type>] ...
omnigraph stream quiesce [<type>] ...
omnigraph stream resume [<type>] ...
```

Every endpoint has a dedicated OpenAPI operation and handler tests. Ingest
passes the engine `stream_ingest` Cedar action and per-actor admission
accounting before acquiring a shard writer; fold/quiesce/resume use a separate
`stream_manage` action. Status is authorized like other graph operational
metadata. The same engine gates apply to embedded and remote CLI use.

## 5. Ack-path validation and writer lifecycle

Before append, OmniGraph applies checks that need no base-table read: Arrow
shape/type, required/default fields, enum/range/check constraints, reserved
columns, and stream mode. RI, cardinality, cross-version uniqueness, and
external embedding computation remain fold-time work.

One warm `ShardWriter` is held per active shard behind a bounded registry. The
registry has idle eviction and hard limits for resident writers, MemTable bytes,
unflushed WAL bytes, pending generations, and per-actor inflight bytes. Exceeding
a bound backpressures with a typed retryable response; it never drops a row.

Initial topology has one active ingest owner for each `(graph, table, main)`
shard. MemWAL's epoch fence makes restart/failover safe; it is not a load
balancer. A deployment with multiple server replicas must route a shard to its
current owner (or return a typed retry/redirect) instead of letting replicas
reclaim the epoch per request. General multi-owner routing waits for the
multi-shard phase and its ownership protocol.

## 6. Fold protocol

The fold consumes flushed generations in ascending order. Embeddings and
base-dependent validation run outside the table queue and register every probed
table plus stream configuration/generation in RFC-022's `ReadSet`. The commit
phase then:

1. stages accepted rows with Lance merge-insert and includes
   `merged_generations` in that transaction;
2. stages any rejection/audit rows required by §7;
3. acquires every affected queue in canonical sorted order and revalidates the
   complete `ReadSet`; any mismatch discards and replans the whole fold;
4. writes one generic RFC-022 recovery sidecar before the first
   `commit_staged` call;
5. commits every staged Lance transaction;
6. publishes all data/internal table versions and lineage in one `__manifest`
   CAS, including table heads when RFC-024 is active;
7. deletes the sidecar after successful publication.

The sidecar is mandatory even though merge-insert is staged. After
`commit_staged`, Lance HEAD and `merged_generations` have moved while the graph
manifest has not. A failure in that window is the ordinary multi-table recovery
gap, not invisible staged state.

MemWAL generation GC starts only after the exact fold is graph-visible, its
sidecar is resolved, index catchup permits reclamation, and no `FreshReadCut`
retention guard references the generation. Data-HEAD merge progress alone is
never permission to delete the only fresh-tier copy.

Concurrent folders reload `merged_generations`: a generation already committed
is skipped; otherwise the fold is replanned from current state. A forced fold
stops new flush creation for its cut, waits for in-flight durability waiters,
and never aborts an acknowledged row.

Fold lineage uses `omnigraph:ingest` as the mechanism actor and persists the
authenticated contributor actor for every folded WAL range in the same internal
audit participant. Commit and status output can therefore answer both who ran
the fold and who supplied the data after WAL GC.

## 7. Fold-time rejection is atomic

`strict` is the default. A permanent RI, cardinality, uniqueness, or embedding
failure stops the fold at the offending generation, marks the shard blocked,
and backpressures new ingestion once configured lag bounds are reached.

`dead_letter` is explicit. `_ingest_rejects` is then a versioned internal Lance
table and a participant in the same fold pipeline, not best-effort state outside
the commit protocol. Every reject has deterministic identity:

```text
(table_key, shard_id, generation, wal_position)
```

The fold stages reject rows, accepted rows, and merge progress before committing
any of them. The generic sidecar covers every participant; the single manifest
publish records both the base-table and reject-table versions. Replay is
idempotent by reject identity. There is no ordering in which progress can become
visible while the corresponding rejection is lost.

`stream status` reports blocked generations and typed reject details. Reject
retention is explicit and cannot be shorter than the WAL/fold audit retention
needed to explain a durable acknowledgement.

## 8. Epoch-fenced quiescence barrier

Branch operations, schema changes, stream teardown, and Lance upgrades require a
real barrier, not an empty check.

Each enrolled table has a durable
`stream_state:<stable-table-id>:<incarnation>` row in its manifest branch with
`OPEN | DRAINING | SEALED`, configuration hash, and epoch floor. The row is the
logical lifecycle authority and is updated by an RFC-022 CAS; MemWAL shard
epochs are the physical writer fence. Neither an in-memory registry nor an
empty-generation observation can substitute for both.
Lifecycle-only transitions are audited manifest metadata transactions; they do
not create graph-content commits or move `graph_head`.

The shared drain sequence is:

1. publish stream intent `OPEN -> DRAINING` for the target table/branch;
2. increment and persist each shard writer epoch and seal claims, fencing stale
   writers across processes;
3. reject or backpressure new appends;
4. wait for in-flight durability waiters, flush active MemTables, and fold every
   generation to empty;
5. verify shard manifests and base `merged_generations` agree on emptiness;
6. publish `DRAINING -> SEALED` with the verified generation/epoch cut;
7. for an operation-scoped drain, perform the guarded operation; persistent
   public quiesce stops after step 6.

Each lifecycle CAS is an RFC-022 authority-first metadata write; each fold is a
separate normal RFC-022 graph write. `DRAINING` fully encodes the target epoch
floor, so an interrupted epoch mutation is idempotently resumed from that row.
The sequence is not one giant sidecar spanning multiple commits.

There are two dispositions after the drain reaches `SEALED`:

- **operation-scoped drain** — branch/schema maintenance automatically publishes
  `SEALED -> OPEN` with a newer epoch only after the guarded operation succeeds
  and the stream contract remains compatible;
- **persistent quiesce** — the public `quiesce` command leaves the stream
  `SEALED`. It never auto-reopens. `stream resume` explicitly revalidates schema,
  PK, configuration, MemWAL format, and epoch, then publishes a newer `OPEN`
  state. Stream teardown deletes intent only from `SEALED`.

The barrier never holds the table write queue while waiting for a fold that
needs that queue. State transition and epoch fencing happen first; fold commit
then acquires the normal queue. Crash recovery resumes from the durable state
and epoch.

Schema apply must drain every affected enrolled type before changing fields,
constraints, PK, embeddings, or `@stream` and resumes only when compatible.

A Lance version upgrade requires persistent `stream quiesce --all`, but empty
generations alone are insufficient: the MemWAL system index, shard manifests,
epoch records, and generation directories may still use the old format. Before
the bump, the implementation must prove one of: (a) upstream guarantees and
cross-version tests cover every retained MemWAL artifact, (b) a public Lance
metadata migration converts them, or (c) OmniGraph tears down the enrolled
MemWAL metadata under recovery and re-enrolls after the bump. Without one of
those gates the upgrade refuses; `resume` never opens unverified old metadata.

## 9. Fresh-read cuts

Freshness is a first-class engine/IR enum:

```text
Committed
Fresh
```

At query planning, `Fresh` captures one `FreshReadCut` containing:

- the ordinary manifest snapshot;
- each selected shard-manifest version and writer epoch;
- included flushed-generation paths and maximum generation;
- the active same-process MemTable row-position watermark, when available;
- the base table's `merged_generations` and index-catchup state read from the
  exact table version selected by the manifest snapshot, never from live HEAD.

Capture uses a retrying handshake:

1. read the selected shard manifests/epochs, acquire Lance generation retention
   guards for the flushed files in the tentative cut, and under one
   same-process writer snapshot capture/pin any active-MemTable watermark;
2. pin the graph manifest snapshot and read `merged_generations` from each exact
   base-table version it selects;
3. re-read the shard manifest versions/epochs; any epoch/configuration change
   restarts the whole capture;
4. if a generation from step 1 disappeared, accept that only when the pinned
   base's `merged_generations` proves it is included; otherwise release guards,
   discard the whole graph snapshot, and retry from step 1;
5. exclude generations that appeared after step 1 and hold the generation and
   MemTable read guards captured in step 1 until query completion.

If Lance exposes no guard that prevents generation GC for the query lifetime,
cross-process `Fresh` does not ship. A missing generation is never interpreted
as “probably folded” against an older pinned base.

Execution never refreshes that cut mid-query. It excludes every flushed
generation `<= merged_generations[shard]`; otherwise old WAL data could outrank
or duplicate its newer base-table image.

Fresh reads have no cross-table atomicity. Same-process active MemTables provide
read-your-writes; other processes can promise only the latest flushed state
captured by their shard-manifest reads. The HTTP request and query docs state
those limits wherever the tier is exposed.

## 10. Observability and resource contracts

Per shard expose durable WAL position, replay position, active epoch, current
generation, flushed and merged generation, index catchup, pending rows/bytes,
oldest acknowledged age, last fold error, blocked reject, and quiescence state.

Metrics cover ack latency, durability-wait batching, fenced writers, replayed
entries, fold rows/bytes/generations, fold retries, lag, reject counts, and
sidecar recovery. Defaults for every byte/count/time bound are documented and
configuration changes are observable behavior.

`stream status` resolves the exact lifecycle rows and MemWAL metadata through a
structured, bounded access path; it may reuse RFC-024's scalar-index machinery
but cannot claim history-flat cost while scanning manifest history.

## 11. Acceptance gates

- Surface guards pin claim, append, durability waiter, flush, epoch fencing,
  staged merge with `merged_generations`, index catchup, and seal/reopen APIs.
- A WAL append failure emits no durable acknowledgement. Every acknowledged row
  survives crash, replay, fold, and recovery.
- Failpoints cover enrollment's inline-commit gap and every fold participant
  around sidecar, `commit_staged`, reject persistence, and manifest publish.
- Enrollment restarts without an inline effect when schema, PK, table head, or
  stream configuration changes between prepare and gated revalidation.
- Two folders converge exactly once; a fenced stale writer can never produce a
  false durable acknowledgement after WAL GC.
- Two server replicas do not epoch-ping-pong one shard; owner failover fences
  the old process before the new owner acknowledges.
- Quiescence tests race appends with branch and schema operations across two
  coordinators and prove no post-drain tail appears; failpoints cover every
  lifecycle-row, epoch-fence, fold, and `SEALED` boundary.
- Persistent quiesce never auto-reopens; explicit resume validates a newer
  epoch. Upgrade tests cover every retained MemWAL artifact through declared
  compatibility, migration, or teardown/re-enrollment.
- Fresh reads race capture with fold/GC, retry on an unexplained disappearing
  generation, hold generation/MemTable guards through execution, exclude merged
  generations, and document cross-table inconsistency explicitly.
- Server/OpenAPI tests preserve the old `/ingest` alias and cover the new route;
  CLI parity covers embedded and remote stream commands.
- Ack-path object-store operations are O(1) and flat in graph history and WAL
  depth. Fold cost is bounded by generations/rows folded, not graph history.
- S3 correctness runs against RustFS; API/format guards are rerun before every
  Lance bump.

## 12. Phasing

| Phase | Content | Gate |
|---|---|---|
| A | MemWAL adapter, surface guards, enrollment sidecar | inline-commit crash matrix |
| B | new ingest route/CLI, durable ack, strict fold | ack durability; API compatibility; cost budget |
| C | atomic dead letter, audit provenance, status/bounds | reject crash matrix; backpressure tests |
| D | epoch-fenced drain, persistent quiesce/resume, schema/branch/upgrade integration | two-coordinator race and format-transition suite |
| E | fresh cuts and maintained-index reads; cross-process `Fresh` ships only if the substrate generation-retention guard exists (§9), otherwise same-process only | cut consistency; merged-generation exclusion |
| F | multi-shard upsert and stream deletes | one-key-one-shard proof; Lance re-audit |
