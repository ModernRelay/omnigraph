---
type: spec
title: "RFC-026 — MemWAL streaming ingest"
description: Adopts Lance MemWAL as OmniGraph's strategic streaming-write architecture, with durability-watcher acknowledgement, graph-atomic folds, epoch-fenced quiescence, and explicit fresh-read cuts on the RFC-022 unified write path.
status: draft
tags: [eng, rfc, streaming, ingest, wal, memwal, lance, omnigraph]
timestamp: 2026-07-10
owner: OmniGraph maintainers
---

# RFC-026 — MemWAL streaming ingest

**Status:** Draft / Phase A foundation implemented; Phase B1 private core next; public streaming inactive
**Date:** 2026-07-10
**Gate E0 evaluated:** 2026-07-18
**Phase A foundation completed:** 2026-07-18
**Phase B1 preparation completed:** 2026-07-18 (RC.1 source audit and
one-generation bounded design)
**Author track:** Maintainer design series
**Depends on:** [RFC-022](0022-unified-write-path.md)'s unified write and
generic recovery-sidecar protocol, plus
[RFC-028](0028-stable-schema-identity.md)'s stable table identity and
incarnation contract, plus
[RFC-023](0023-key-conflict-fencing.md) for the initial keyed graph-stream
mode. Durable heads from
[RFC-024](0024-durable-table-heads.md) are compatible but not required.
**Surveyed:** omnigraph 0.8.1; Lance 9.0.0-rc.1 at git rev
`cec0b7dffe2d85c7e66dbe9d1f3891c297903a1d`; complete MemWAL table and
system-index specifications, including the durability and writer-fencing
changes carried forward from beta.17
**Audience:** engine, server, CLI, policy, and operations maintainers
**Open architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
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

One RC.1 API gap prevents the ideal enrollment adapter, not the bounded
evidence work:
`InitializeMemWalBuilder::execute` internally commits the MemWAL `CreateIndex`
transaction and returns only `Result<()>`, while initial shard-manifest creation
is a separate object-store effect reached through `mem_wal_writer`. The public
surface therefore cannot provide the caller-minted transaction identity and
reversible cross-process admission seal required by the general profile in
§3/§8. OmniGraph does not reach through private Lance modules or hand-roll
those objects. It also does not make upstream release timing a calendar
prerequisite: Gate E0 below confirms that a deliberately narrower profile can
classify the public effects exactly under OmniGraph's existing
single-live-writer-process boundary. The exact upstream receipt/seal remains
the preferred simplification and the gate for broader topology.

The contract is:

- stream acknowledgement means every row in one submitted Lance batch has
  crossed that batch's durability watcher; it does not promise one WAL entry or
  an addressable WAL position per row;
- acknowledgement does not mean graph visibility;
- default queries see only the manifest-committed graph;
- a fold is an ordinary RFC-022 graph writer and is the sole visibility point;
- fresh reads are explicit and never claim cross-table atomicity.

### 0.1 Gate E0: bounded-enrollment decision passed before activation

Gate E0 is a production-neutral decision harness, not streaming activation. It
uses the pinned public Lance surface to establish that one exact first
enrollment can be classified after success, failure, and lost acknowledgement
without deleting or adopting ambiguous state. Its evidence-backed initial
profile is:

- `main` only;
- one unsharded keyed-upsert shard per enrolled table;
- one live OmniGraph writer process for the graph, with a crash successor only
  after external exclusivity has been established;
- no raw Lance writer and no overlapping second OmniGraph writer process; and
- exclusive ownership of the enrolled table's base HEAD while the stream is
  `OPEN`: only the stream fold/recovery adapter may advance it. Every other
  graph writer, branch/schema operation, repair, index/optimize path, or cleanup
  that touches the table must refuse before effect or first drain the stream.

Gate E0 itself added no manifest rows, sidecars, `@stream`, public APIs, WAL
acknowledgements, or a format stamp. The Phase A implementation authorized by
that result has now activated internal schema v7 and the bounded production
foundation described in §12.2: recoverable empty enrollment, durable lifecycle
authority, process-local admission/exclusion, and strict format
refusal/rebuild. It still exposes no production enrollment entry point and
cannot append or acknowledge a row. RFC-026 therefore remains draft and public
streaming remains inactive until the later gates close.

The next implementation is deliberately split. **Phase B1** proves the private
engine lifecycle for one bounded generation, from admission through crash
replay and one strict RFC-022 fold. **Phase B2** is the later public strict
activation: schema intent, production first use, SDK/HTTP/CLI/OpenAPI/Cedar
parity, minimum durable contributor attribution and reclamation, and the
operator escape/correction path plus an explicit same-key `AckUnknown` retry
contract. No public caller is admitted merely because B1 is green.

## 1. Scope and non-goals

This RFC specifies enrollment, the public stream API, acknowledgement semantics,
folding, fold-time integrity, dead-letter atomicity, branch/schema quiescence,
fresh-read cuts, resource bounds, observability, testing, and upgrade posture.

It does not replace `load` or `mutate`, provide cross-query transactions, store
manifest mutations in MemWAL, create a second metadata authority, or weaken
default snapshot isolation. Stream-mode
deletes remain out of the first delivery and require the Lance tombstone surface
plus a separate acceptance pass.

The bounded initial profile additionally excludes non-main streams,
multi-shard ownership, overlapping writer processes, raw Lance writers,
cross-process `Fresh`, and concurrent interactive/maintenance HEAD movement on
an `OPEN` enrolled table. These are support-boundary refusals, not implicit
eventual-consistency modes.

## 2. Stream mode and key semantics

Phase B1 has no schema syntax and uses one fixed internal
`mode="upsert", on_reject="strict"` profile. Phase B2 initially exposes only
`@stream(mode="upsert", on_reject="strict")` on a node or edge type.
`on_reject="dead_letter"` remains a typed unsupported choice until Phase C
proves a restart-stable reject-row identity, consumes B2's durable contributor
attribution, and proves atomic rejection and retention.
It requires the table's immutable unenforced primary key to equal OmniGraph's
merge key: `id` for nodes and edges. All occurrences of one key map to one shard
and MemWAL applies last-write-wins ordering.

Every stream contract is bound to RFC-028's
`(stable_table_id, incarnation_id)` pair. A rename preserves that pair, so the
logical stream contract and ownership of reject history remain continuous.
That continuity does **not** authorize adoption of physical WAL artifacts. A
same-dataset rename preserves the current physical enrollment; a rename or
other SchemaApply that rematerializes the table binds the preserved logical
pair to a fresh physical enrollment and fresh shard namespace through §3 and
§8. Dropping and later re-adding the same name mints a new pair; the new table
cannot adopt the old table's WAL, lifecycle row, reject rows, epochs, or merge
progress.

Public append mode is deliberately out of scope. Nodes and edges always have
logical identity; allowing a retry to append the same `id` twice would violate
that contract. A future explicitly keyless, non-graph append-only table class
may consume MemWAL append semantics under its own schema/API decision.

Stream ordering intentionally differs from the interactive fence:

- interactive same-key writes outside an `OPEN` bounded stream serialize or
  fail/retry loudly; an `OPEN` bounded stream refuses such a write before
  effect until it is drained;
- same-key stream entries resolve by MemWAL generation/position order;
- duplicate keys inside one bulk-load input retain the existing load error.

The schema and user docs state all three together.

## 3. Enrollment is a recoverable multi-effect adapter

In the eventual Phase-B2 public contract, SchemaApply records `@stream` intent
and first stream use enrolls the physical table by creating the singleton
`__lance_mem_wal` system index and its sharding configuration. Phase A does not
yet parse or persist that intent. It implements the enrollment machinery behind
a crate-private method and a feature-gated failpoint seam, using one fixed
main-only/unsharded configuration so recovery and exclusion can be proven
before a production caller exists.

RFC-024 heads are optional. Every lifecycle row therefore carries two distinct
classes of evidence:

```text
StreamPhysicalBinding {
    stable_table_id,
    incarnation_id,
    table_location,
    table_branch,         // exactly main in the bounded profile
    enrollment_id,       // pre-minted UUID, never reused
    shard_ids,           // sorted UUID namespace, never reused
    stream_config_version,
    stream_config_hash,
}

CurrentHeadWitness {
    branch_identifier,
    table_version,
    transaction_uuid,
    manifest_e_tag,
}
```

`StreamPhysicalBinding` is stable for one physical enrollment. The
`CurrentHeadWitness` is not: it is the exact public Lance composite at the
currently accepted base HEAD, using the same capture discipline as RFC-024.
Every ordinary Lance commit changes at least the table version and current
transaction UUID and normally the manifest e_tag. Calling that composite a
stable “physical-ref incarnation” is incorrect.

The lifecycle binding is valid only when the current manifest table entry
resolves to the same stable table/incarnation and location/main ref, its
persisted `CurrentHeadWitness` equals the physical table's current witness, and
the table contains the expected singleton MemWAL index plus exactly the
recorded shard namespace/configuration. Path, branch name, numeric version,
timestamp, or the RFC-028 logical incarnation alone is never sufficient. Local
and S3/RustFS same-path/ref delete-recreate guards must change the witness; a
backend without that proof cannot activate streaming.

The bounded profile makes the changing witness tractable by granting the
`OPEN` stream exclusive authority to advance that base-table HEAD. Every fold
captures the prior witness and publishes the achieved table pointer and next
witness in the same `__manifest` CAS. Any unexpected movement is foreign or
ambiguous and fails closed. A path that needs to mutate or maintain the table
must drain first and participate in the lifecycle/witness transition; it may
not silently coexist with an `OPEN` stream. A long-lived Lance tag is not the
default enrollment anchor: it would pin the enrollment-time table snapshot and
can retain an old full-table file set across rewrites/compaction, while also
adding another auxiliary effect to enrollment. It remains only a measured
fallback if a later profile requires concurrent base writers.

The MemWAL index UUID is not used as enrollment identity because Lance may
replace that metadata entry while advancing merged-generation state. A mutable
table name, the RFC-028 logical pair alone, or a compatible-looking index is
never enough. RFC-024 may project related table/ref facts into a durable head,
but RFC-026 persists and validates this binding without depending on heads.
For the bounded profile, the pre-minted `enrollment_id` is also persisted as a
namespaced MemWAL `writer_config_defaults` marker together with the exact
configuration version. RC.1 explicitly permits arbitrary persisted default
keys, and merge-progress/index-metadata replacement must preserve them. This is
classifier evidence independent of the replaceable MemWAL index UUID; the
manifest lifecycle row remains the logical stream authority.

Enrollment advances Lance HEAD and provisions shard-manifest objects. The
preferred general-profile substrate remains one of these public,
surface-guarded shapes:

1. a caller-controlled staged/uncommitted MemWAL initialization transaction
   with an exact transaction identity, plus idempotent public APIs to provision,
   classify, seal/reopen, and reclaim a pre-minted shard manifest; or
2. one recoverable enrollment primitive that returns an exact receipt covering
   both the index transaction and initial shard objects, with documented
   classify/roll-forward/rollback semantics.

The RC.1 `execute() -> Result<()>` initializer plus separately claimed shard
writer satisfies neither general shape. Private-module access,
compatible-looking index inference, and direct object-store emulation remain
rejected. Gate E0 established that the public effects are nevertheless exact
enough for the bounded profile. Internal schema v7 and the private Phase A
adapter now activate the format/recovery foundation. `@stream`, production
first use, WAL row admission, and acknowledgement remain inactive. Phase B1
will reach first use only through a private engine seam; schema-declared first
use remains Phase B2.

With Gate E0 green, the implemented bounded enrollment uses one RFC-022
multi-effect sidecar, not an ad-hoc state machine:

1. run and await RFC-022's synchronous recovery barrier;
2. authorize, pin the manifest/schema/table state, and prepare a complete
   `ReadSet` containing schema identity, stable table ID and incarnation,
   location/main ref, exact pre-enrollment `CurrentHeadWitness`, PK metadata,
   the fixed Phase A configuration, and lifecycle-row absence. Public
   schema-declared intent and `SEALED` physical rebind remain later phases;
3. acquire any global claims and then the `(table, branch)` write queue in
   RFC-022 order, then freshly revalidate the complete `ReadSet`; a mismatch
   restarts before any physical effect;
4. verify RFC-023's already-installed PK; enrollment never performs a first-use
   PK migration; validate the sharding configuration;
5. pre-mint a never-reused enrollment UUID and one shard UUID, then arm a
   sidecar with writer kind `stream_enrollment`, the exact baseline witness,
   fixed intended binding/configuration (including the namespaced persisted
   enrollment marker), and the only allowed successor shape;
6. call the public initializer only while the base-HEAD/exclusive-admission
   gate is held. After success or a lost result, use the manifest-pinned direct
   physical URI opener at `VersionResolution::At(N)` (the evidence harness uses
   `DatasetBuilder::with_version(N)`), never latest resolution, and verify the
   exact captured `N` witness. Then use the pinned public
   `Dataset::has_successor_version` primitive to ask only whether `N + 1`
   exists. `Ok(false)` means no effect only while the same gate and cleanup/GC
   exclusion remain held. `Ok(true)` permits an exact `checkout_version(N + 1)`;
   that handle must report no `N + 2` successor before its transaction is
   accepted as exactly one singleton MemWAL `CreateIndex` reading `N`, with the
   intended unsharded configuration and no unrelated change. A probe error,
   overflow, detached-version boundary, same-version ABA, or existing `N + 2`
   is ambiguous and returns `RecoveryRequired`;
7. pass the pre-minted UUID to the public shard-writer path and accept only its
   exact empty initial shard state: expected shard/spec identity, epoch 1,
   generation 1, replay/cursor positions 0, no flushed generation, and no
   data-bearing WAL entry. A deterministic
   data-less fence sentinel is admissible only if Gate E0 first pins it as part
   of that exact state;
8. once either allowed effect exists, recover only by rolling forward to the
   fixed outcome. The bounded adapter never deletes/reclaims shard artifacts or
   restores the base table from inferred ownership. Intervening HEAD movement,
   a wrong index/config, a foreign shard, unexpected WAL data, or any ambiguity
   retains the sidecar and returns `RecoveryRequired`;
9. publish the achieved table version, `CurrentHeadWitness`, stable physical
   binding, exact pre-claim epoch floor, and `stream_state = OPEN` in one
   manifest CAS, including the table-head row when RFC-024 is active; and
10. resolve the enrollment sidecar before any future `put` is admitted. Phase A
    has no put/ack caller; Phase B1 must enter through this recovery barrier, so
    `OPEN` plus an unresolved enrollment can never acknowledge.

An exact no-effect intent may finalize without publication. The exact
index-only and index-plus-empty-shard states roll forward. No other state is
silently repaired, adopted, or rolled back. Repeating enrollment with the same
complete binding is a no-op only after exact validation. A different PK,
binding, sharding spec, maintained-index set, writer-default configuration, or
current-HEAD chain is a typed conflict.

No row is acknowledged until enrollment is manifest-committed, every sidecar
effect is resolved, and the exact bound shard is physically admitting writes.

The exclusive base-HEAD gate and cleanup/version-GC exclusion are load-bearing
from the final pre-effect check through classification and publication. Lance's
`has_successor_version` intentionally reports only whether the immediate next
manifest exists and may return false if an intermediate manifest was removed;
therefore recovery cannot interpret `Ok(false)` as no effect after cleanup was
allowed to race. The method is public but absent from the rendered guide, so a
compile/runtime surface guard pins it on every Lance bump.

RC.1 persists arbitrary writer defaults in the MemWAL index but does not apply
them to a caller-supplied `ShardWriterConfig`. The bounded adapter must therefore
reconstruct the exact persisted durable-write and buffer configuration before
opening the pre-minted shard; compatible defaults are not inferred. Recovery
classification refreshes the durable table tip and performs a read-only exact
inventory of the documented `_mem_wal/<shard>` layout. A foreign or malformed
prefix, loose object, unknown shard-manifest object, WAL entry, cursor movement,
or flushed generation fails closed. This inventory creates or mutates no raw
Lance object and is pinned by the Gate E0 guards on every Lance bump.

Initial delivery supports one unsharded shard per `(table, main)` and one live
writer process for the graph. Non-main branches remain refused until the Lance
branch-scoping question is proven by a surface guard and end-to-end test. Later
`bucket(id, N)` sharding must preserve the one-key-to-one-shard rule. General
overlapping-process enrollment/failover still requires the upstream receipt /
admission lifecycle or a separately accepted distributed fence.

## 4. Future public activation

This section is the Phase-B2 wire contract, not part of the Phase-B1 private
engine slice. Public activation remains blocked until the strict core is green,
durable contributor attribution and bounded reclamation exist, and a
persistent quiesce/rebuild/correction escape prevents an `OPEN` enrollment
from permanently excluding work. B2's escape is quiesce plus export/rebuild
into a different root. In-place SchemaApply and maintenance on an enrolled
table stay refused until Phase D supplies automatic drain, witness update, and
rebind.

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
{"ordinal":17,"status":"durable","enrollment_id":"...","shard_id":"...","writer_epoch":8}
```

RC.1 exposes a durability completion, not an exact per-put WAL receipt.
`BatchDurableWatcher::wait()` returns only `Result<()>`;
`WriteResult.batch_positions` names positions local to the active
MemTable/`BatchStore` and resets after rollover;
and `wal_stats().next_wal_entry_position` is a mutable next-position statistic.
None is an authoritative durable row address, so OmniGraph never derives or
publishes `wal_position` from those surfaces. One normalized Lance put may
cover several input ordinals; their response lines become eligible together
after the shared watcher succeeds and are still emitted in caller order.

The B1 core accepts one non-empty, contiguous ordinal range and validates the
whole normalized batch before invoking Lance; a validation failure is
all-or-nothing for that B1 call. The B2 HTTP/CLI adapter owns NDJSON parsing,
batching, and the reorder buffer. It closes and submits the preceding
contiguous valid run when it encounters an invalid line, emits the validation
error for that ordinal, and begins a new contiguous run afterward. It never
passes a non-contiguous range to B1. Previously acknowledged runs in the same
request remain durable; the response is a stream, not an all-request
transaction. Ordering, cancellation, and retry rules are explicit:

- acknowledgements are emitted in input order for one HTTP stream;
- disconnecting does not cancel entries whose durability waiter resolved;
- cancellation or failure after `put_no_wait` but before a successful watcher
  result is `AckUnknown`, not proof of non-durability;
- a missing response is ambiguous; retrying the same `id` and payload may add
  another WAL entry and preserves one-row cardinality, but is semantically safe
  only when no newer same-key value can interleave. If `Y(id)` lands between an
  ambiguous `X(id)` and retry `X(id)`, physical LWW order makes the retry
  overwrite `Y`. B2 therefore requires an accepted per-key sequencing or
  idempotency contract before public activation;
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
omnigraph stream resume [<type>] --abort-drain ...
```

Every endpoint has a dedicated OpenAPI operation and handler tests. Ingest
passes the engine `stream_ingest` Cedar action and per-actor admission
accounting before acquiring a shard writer; fold/quiesce/resume use a separate
`stream_manage` action. Status is authorized like other graph operational
metadata. The same engine gates apply to embedded and remote CLI use.

Phase B2 initially exposes only strict upsert. Public activation requires the
minimum `status`, explicit `fold`, persistent `quiesce`, `resume`/abort-drain,
rebuild preflight, a bounded strict-correction workflow, durable
authenticated-contributor attribution, a same-key `AckUnknown`
sequencing/idempotency contract, and a proved WAL/generation reclamation rule.
Minimum status includes lifecycle and
binding, current epoch, pending generations/bytes, last fold error, and whether
strict fold is blocked. It never claims to resolve one caller's `AckUnknown`.
Dead-letter row identity and operation, richer status, and configurable policy
remain Phase C. Automatic operation-scoped drain, SchemaApply/branch
integration, upgrade orchestration, and physical rebind remain Phase D.

## 5. Ack-path validation and writer lifecycle

Before append, OmniGraph applies checks that need no base-table read: Arrow
shape/type, required/default fields, enum/range/check constraints, reserved
columns, explicit non-null `id`, and stream mode. Streaming never reuses the
loader's random-ID fallback because retry safety depends on stable caller-owned
identity. RI, cardinality, cross-version uniqueness, and external embedding
computation remain fold-time work.

Phase B1 admits exactly one non-empty normalized `RecordBatch` per Lance
`put_no_wait`. Each call and the **entire active generation**, including any
duplicate batch submitted while that live generation is still active, are
capped at 8,192 rows and 32 MiB of materialized Arrow memory. The charged
representation is the exact stored MemWAL batch after normalization/defaulting and internal
`_tombstone=false` injection. These are hard OmniGraph reservations made before
the row put and use the sealed keyed writer's numeric limits; they are not
Lance's soft thresholds. A cold claim/replay may already have advanced the
shard epoch before the budget is known. Replayed residue is never admitted
back into a live generation: it is routed to the fold-only path described
below. A new batch that would cross either generation budget returns typed
`FoldRequired` without calling `put_no_wait` or adding a row/WAL batch. A
configured
`durable_write=true` writer must return a durability watcher; `None` after
invocation is `AckUnknown` plus writer retirement, never acknowledgement.

The private success value is deliberately status-only:

```text
DurableBatchAck {
  table_identity,
  enrollment_id,
  shard_id,
  writer_epoch,
  caller_ordinal_range,
  row_count,
}
```

It contains no generation, WAL entry, or batch-position coordinate. The typed
post-invocation `AckUnknown` outcome carries enough of the same binding and
caller ordinal context to report the ambiguity, but makes no durability claim.
RC.1 cannot later attribute replayed residue to that specific attempt, so the
attempt remains permanently ambiguous even when replay preserves its possible
effect. A same-payload retry is only cardinality-idempotent; without per-key
serialization it can overwrite a newer interleaving value.

One root-scoped registry is singleflight for the exact physical binding across
all `Omnigraph` handles in the process; it is never handle-local. Its serialized
per-binding worker owns the final authority check, generation-budget
reservation, the call to `put_no_wait`, and the watcher wait. This is required
because RC.1 may insert into the MemTable and then return `Err` while scheduling
the WAL flush. The worker, not the request task, therefore determines the
outcome after invocation.

The registry has idle eviction and hard global/per-table limits for resident
writer count, reserved normalized generation bytes, in-flight calls, and
pending generations. Per-actor inflight accounting starts with B2's
authenticated public caller. Exceeding a bound backpressures with a typed
retryable response; it never drops a row. RC.1's public MemTable estimate omits
some PK-index memory, so OmniGraph does not mislabel it a hard total-RSS bound;
actual index/RSS overhead is an evidence gate and remains observable.

Eviction waits for every OmniGraph-owned durability waiter; if that cannot be
proved, affected calls become `AckUnknown`. Retirement first closes the
serialized worker to new puts, then calls public `ShardWriter::abort` and awaits
its already-active handler through one background-owned abort task retained in
the retired registry entry. The caller deadline bounds waiting for that task;
it never cancels or drops the abort future. This matters because RC.1
`shutdown_all` takes the handler join handles before awaiting them—cancelling
that future can detach an active handler and make a later abort look empty. The
entry is removed only after the original abort completion settles. A deadline
keeps it retired and admission closed and returns typed `RecoveryRequired`; B1
never issues a second abort or reopens beside a possibly active handler. A
process restart reclassifies the durable state before admission. The
caller-quiesce precondition is
structurally guarded. Lance `ShardWriter::close` intentionally ignores final
WAL/frozen-flush completion errors, so `close() == Ok(())` is not durability
evidence and B1 never uses it for retirement. Resident-writer, reservation,
eviction, and durability-deadline defaults are selected by a checked-in
RSS/latency/backpressure instrument before row activation. Lance's soft
post-insert thresholds and potentially unbounded backpressure wait are not
credited as OmniGraph hard resource limits.

RC.1's durability watermark is writer-wide while MemTable batch positions
restart at zero after `freeze_memtable`. A watcher for generation `N + 1` can
therefore resolve from generation `N`'s old watermark. B1 does not use that
surface across rollover. Its bounded configuration prevents automatic
rollover: `max_memtable_batches=8,193` sits one above the worst-case 8,192
non-empty one-row calls, while `max_memtable_size` and
`max_unflushed_memtable_bytes` are fixed to a portable 1 GiB.
`max_memtable_rows=8,193` is also explicit and persisted, but RC.1 uses it to
size HNSW structures rather than as a MemTable rollover trigger, so the proof
does not credit it. MemTable mode stays on and maintained indexes
stay empty; a widest-legal normalized generation guard proves Lance's trigger
estimate—BatchStore bytes plus the PK Bloom filter—cannot reach either byte
threshold. RC.1 omits the mandatory PK index from that estimate, so its actual
memory is covered only by the separate RSS evidence gate, never by the hard
32-MiB Arrow reservation. The worker explicitly seals and drains its one
generation, retires that writer without admitting into the
replacement MemTable, folds the generation, then reopens the binding at a
higher epoch before the next put. An observed automatic rollover is
corruption/refusal, not a second watcher domain. An adversarial surface guard
delays generation `N + 1`'s WAL PUT after sealing `N` and proves the pinned RC.1
bug; adapter tests prove no B1 path can issue that second-generation put on the
same writer.

Before the first put, configuration is split into three explicit classes:

1. binding/correctness identity — topology, `durable_write=true`, MemTable on,
   empty maintained-index set, WAL buffer policy, and the no-auto-roll
   generation envelope/capacities; these are persisted, hashed, and read back;
2. OmniGraph-owned runtime policy — persistence retry/backoff, registry and
   deadline bounds, backpressure, logging, and statistics; these are explicit
   constants and metrics but changing them does not pretend to re-enroll the
   physical stream; and
3. injected runtime capabilities — the root's shared Lance `Session`, the
   base's store parameters when present, and `warmer=None`; these are validated
   at construction and are not scalar enrollment identity.

Compatible-looking RC defaults are never inferred. B1 bumps the persisted
stream configuration from v1 to v2 and the graph-format capability from
internal schema v7 to v8; its `StreamFold` recovery envelope is schema v11.
Phase-A v7/config-v1 roots contain no publicly acknowledged rows and are
refused rather than adopted in place; they move through export/init/load into
a different root.

The RC.1 audit records the currently implicit fields so none disappear during
implementation: `durable_write`; 10-MiB WAL buffer; 100-ms opportunistic WAL
flush interval; three WAL-persist retries with 50-ms base delay; 256-MiB /
100,000-row / 8,000-batch MemTable defaults; manifest-scan batch size two;
1-GiB unflushed threshold; 30-second backpressure logging; synchronous-index,
10,000-row / one-second async-index, and 60-second stats-log fields; zero
frozen-MemTable grace; MemTable enabled; empty HNSW overrides; and no warmer.
The shared `Session` and the base's store parameters when present are injected
runtime capabilities rather than persisted scalar defaults. B1 must explicitly
set each applicable field and either identity-bind it under class 1,
own it under class 2, or prove it semantically inactive on the pinned code path.
In RC.1, `sync_indexed_write`, the async-index fields, and
`stats_log_interval` have no production reads; they are still pinned explicitly
rather than described as active controls. `max_wal_flush_interval` is checked
only opportunistically during a put, not by a background timer, and durable
`put_no_wait` queues an immediate flush. The actual WAL handler always joins
the WAL append with every IndexStore update—including the mandatory PK
BTree—and the watcher advances only after that join. The audit values are
inventory, not automatically accepted OmniGraph limits.

The root-scoped worker registry is keyed by the exact
`(stable_table_id, incarnation_id, enrollment_id, shard_id)` binding. That is
a worker identity, **not** an admission-lock key. The outer process-local
admission lease reuses Phase A's one common `StreamAdmissionKey` domain,
`(stable_table_id, incarnation_id, resolved_physical_ref)`, where a missing
physical ref means main. `enrollment_id` and `shard_id` are revalidated under
that lease but never partition its lock domain. Every affected path for the
same table/ref—including ordinary graph writers that can move the base-table
HEAD, MemWAL append, fold, enrollment, drain, and stream recovery—must acquire
this same domain in its specified shared or exclusive mode; no enrollment- or
shard-keyed admission lock may be introduced beside it. Multiple shard-specific
workers may overlap only while holding shared leases on that common table/ref
domain, so an exclusive enrollment, drain, fold, or recovery waits for all of
them and for any ordinary writer.

Every admitted call, including one using an already-warm writer, first runs the
synchronous recovery barrier and resolves or refuses every stream recovery
kind relevant to this binding/authority before capturing `OPEN`, the complete
physical binding, `CurrentHeadWitness`, and that shard's epoch floor. B1's set
is `StreamEnrollment` plus `StreamFold`; B2 adds resume/abort-drain, and Phase D
adds rebind. A later format cannot silently reuse B1's shorter list. Stream
recovery keeps admission exclusively closed. Cold open, higher-epoch
reopen/replay, and a warm writer's final check all acquire the same shared
root-scoped admission lease **before** any `mem_wal_writer` claim. Under that
lease it first repeats the relevant-sidecar barrier and revalidates `OPEN`, the
binding/witness, exact graph-branch topology, and epoch floor from fresh
authority. If recovery is required, it releases shared admission, resolves the
intent under the exclusive recovery path, and restarts; it never claims from a
stale pre-lease capture. The lease is held through claim/replay validation and
either the complete put/watcher outcome or the quiesced abort-retirement
sequence. After a claim, and again immediately before **every** put, the worker
re-lists relevant sidecars and re-reads the lifecycle row, physical base HEAD,
and shard status: no relevant intent may exist, the binding and witness must
still be identical, state must still be `OPEN`, the shard must be active, and
the claimed epoch must exceed the recorded floor for that same shard. A
mismatch retires the writer through the quiesced `abort` sequence and returns a
typed retry without appending.

The bounded profile closes both the claim-to-check and check-to-put races with
that process-local lease. Drain takes it exclusively, so it cannot capture an
epoch floor while a cold claimant is advancing the shard and merely waiting to
run its final check. Drain waits out existing acknowledgement/retirement work
and keeps admission closed through `DRAINING` and `SEALED`. On restart, a
lifecycle state other than `OPEN` is reconstructed as closed before requests
are served. This is valid only because the support boundary excludes an
overlapping writer process; the lease is not advertised as distributed
fencing.

The complete invocation and durability wait are owned independently of the
requesting task so client cancellation cannot drop the lease or abandon an
unknown append. Success is returned only after
`BatchDurableWatcher::wait()` yields `Ok(())`. A validation, authority, budget,
or queue failure before invoking `put_no_wait` is row-effect-free rejection;
epoch claim/replay evidence may already exist. Once
invocation starts, `put_no_wait Err`, a missing watcher, cancellation,
deadline, fence, persistence error, or watcher failure without successful
completion is typed `AckUnknown`; the writer is retired and reopen/replay
preserves any durable residue without claiming which attempt produced it. A
public deadline is selected only from the Phase-B1 instrument; Lance's
unrelated commit timeout is not reused.

The general multi-process profile still requires the substrate admission seal:
after `DRAINING`, later claims must be refused across processes and existing
writers fenced before a post-check put. The exact upstream seal/reopen surface
therefore remains the expansion gate, even if Gate E0 accepts the bounded
profile.

Initial topology has one active ingest owner for each `(graph, table, main)`
shard and one live writer process for the graph. MemWAL's epoch fence permits a
crash successor to replay after external exclusivity; it is not a load
balancer, a distributed OmniGraph recovery fence, or permission for two server
replicas to overlap. Multi-replica routing/failover waits for the multi-shard
phase plus an accepted ownership protocol.

Phase B1 replaces Phase A's empty-epoch-1-only compatible-open validator before
the first put. Reopening the exact binding claims a higher epoch and replays
durable WAL state. Persisted validation uses the authoritative
`replay_after_wal_entry_position`, exact shard-manifest topology/generations,
and base `merged_generations`; `wal_entry_position_last_seen` is only a
read-side hint. Current and frozen MemTables are runtime state and are not
invented from the persisted shard manifest. The active validator accepts only
the bound shard's valid monotonic epoch, authoritative replay cursor,
generation topology, and merge progress for the exact configuration and
current witness; a foreign shard, binding/config mismatch, invalid status,
unexplained authoritative cursor regression, or corruption still fails closed.
Gate E0's rule that data or cursor movement is invalid remains the enrollment
classifier only and must not be reused as an active-stream validator.

Reopen has an exact fail-closed routing table before any put:

1. no unmerged flushed generation plus an empty active MemTable may admit;
2. no unmerged flushed generation plus a non-empty replayed active MemTable is
   fold-only and admits no put;
3. exactly one unmerged flushed generation plus an empty active MemTable is
   fold-only and resumes that generation;
4. an unmerged flushed generation plus non-empty active data, more than one
   unmerged generation, any frozen MemTable, an unexpected generation number,
   or replay beyond the 8,192-row/32-MiB cap fails closed.

This is stricter than merely rebuilding a reservation. In pinned RC.1,
`replay_memtable_from_wal` inserts the replayed batches into a fresh
`BatchStore` but does not advance that store's per-MemTable WAL-flush
watermark. A later put or plain `force_seal_active` therefore re-appends and
re-indexes the replayed prefix. Repeating a crash after that WAL PUT but before
the shard-manifest commit can multiply the replay tail until the fixed batch
capacity is exhausted.

B1 does not wait for an upstream release. Under the exclusive fold lease, case
2 snapshots public `in_memory_memtable_refs`, requires `frozen` to be empty and
the active range to be exactly the authoritatively replayed contiguous prefix,
then calls public `BatchStore::set_max_flushed_batch_position(len - 1)` before
`force_seal_active`. Every marked batch was just read successfully from durable
WAL; RC.1 already seeds the writer's covered-WAL cursor to the replay tip, so
the resulting generation stamps that exact cursor without writing the rows to
WAL or the PK index again. The adapter then drains, retires, and folds before
another put. This RC.1 compatibility bridge is isolated, source-guarded, and
removed when Lance initializes the replayed BatchStore watermark itself; it is
not a second WAL implementation.

Generation reservation is restart-derived, not a drifting runtime counter.
After claim/replay the worker snapshots `active.batch_store` and sums every
physical stored batch's rows plus `RecordBatch::get_array_memory_size()` in the
same post-tombstone representation charged pre-put. An empty admissible reopen
starts at zero; a non-empty result is validated against the hard cap and routed
to fold-only, never used to continue admission. The serialized worker updates
the derived total after each completed invocation while that one live
generation remains open. Surface guards pin `in_memory_memtable_refs`, public
BatchStore iteration/StoredBatch data, the watermark bridge, and accounting for
replayed duplicates; Lance's different `MemTableStats::estimated_size` is not
substituted for the 32-MiB contract.

## 6. Fold protocol

Phase B1 exposes one explicit private strict fold; there is no background
scheduler. It acquires the admission lease exclusively for the complete cut,
waits for admitted durability watchers, calls `force_seal_active`, waits for
`wait_for_flush_drain`, and captures the resulting immutable flushed-generation
cut. It immediately retires the writer and never admits a put into the empty
replacement MemTable created by sealing. This temporary same-process cut is
not the durable `DRAINING`/`SEALED` operator barrier from §8.

Seal and drain are background-owned, bounded-wait operations. RC.1 replaces the active MemTable before
all flush scheduling can report success, and its handler/channel or object-
store work can stall. The registry task—not the requesting future—owns the
seal/drain/abort sequence and the exclusive admission lease until it settles.
Any `force_seal_active` error is therefore generation-
effect-ambiguous: B1 closes the worker, starts the quiesced abort/reopen
classifier, and returns typed `RecoveryRequired` rather than claiming no
effect. Evidence-selected deadlines bound only the caller's wait. A deadline
keeps the original task and abort completion retained, the registry entry
retired, and admission closed; it does not cancel a Lance future, claim
quiescence, arm `StreamFold`, retry abort, or reopen beside the unresolved
handler.

`wait_for_flush_drain() == Ok(())` is not sufficient evidence on pinned RC.1.
The flush handler removes a completed watcher on both success and failure, so a
waiter that starts after a fast failed handler can observe an empty watcher
queue and return success. After the wait, B1 atomically re-reads
`in_memory_memtable_refs`, requires `frozen` to be empty, and independently
requires the authoritative latest shard manifest to contain the exact expected
generation and covered replay cursor. A retained frozen MemTable, absent or
mismatched generation, or cursor mismatch retires the writer and re-enters the
§5 fold-only classifier or fails closed; it never arms `StreamFold` and never
admits a put.

Seal/drain is intentionally before the `StreamFold` sidecar because it creates
fresh-tier generation state, not a base-table or graph-visible effect. Its
restart classifier is nevertheless explicit. A crash before the generation
manifest lands re-enters case 2 in §5 and uses the replay-watermark bridge
before resealing; a crash after the exact generation lands but before the
sidecar is case 3 and resumes that generation fold-only. Neither state may
admit a new put. Any active data beside an unmerged flushed generation, or any
second unmerged generation, fails closed rather than guessing which cut owns
the rows.

RC.1 writes the randomized generation dataset, Bloom filter, and PK sidecar
before it CASes the shard manifest. A crash in that interval can therefore
leave a generation directory that no manifest references. The active-stream
inventory classifies an unreferenced recognized `{hash}_gen_N/` subtree under
the bound shard—complete or partial—as derived orphan output: it is never
adopted as a generation, never scanned or folded, and never deleted by B1. It
does not make the durable WAL residue unrecoverable. Any loose object outside
that recognized generation-output subtree shape still fails closed. B2's
accepted reclamation protocol must prove when these orphans can be removed and
count them toward its hard retained-storage stop.

The fold excludes generations already covered by the base table's exact
`merged_generations` and requires exactly one remaining B1 generation. It
constructs an exact post-drain `ShardSnapshot` directly from the authoritative
latest shard-manifest revision captured under the exclusive lease and supplies
it to public `LsmScanner::without_base_table`; it does not use the eventual
MemWAL-index snapshot or a live MemTable reference. The scanner streams only
that generation through the root's shared Session and the base's store
parameters when present, and resolves same-key rows
last-write-wins before staging one exact-ID upsert.

Embeddings, other explicitly fold-derived fields, and base-dependent validation
run outside the table queue while admission remains closed. Defaults were
already fixed before acknowledgement and are not re-applied. The fully
materialized, deduplicated output
must still fit one sealed keyed transaction: at most 8,192 rows and 32 MiB. A
wide row or fold-time embedding that crosses either limit returns a typed
strict-blocked outcome before any table effect and leaves the acknowledged
generation durable. B1 never splits one generation across transactions and
never marks it merged after a partial prefix. With or without RFC-024, the
`ReadSet` carries schema identity, the complete stream
binding/configuration/generation cut, the base table's exact
`CurrentHeadWitness`, every probed table, and the conservative branch authority
token `(native branch incarnation, optional graph_head)`.
Absence of `graph_head` on a fresh branch is part of that token. Any publisher
retry compares the captured token and returns to full fold revalidation on a
change; it never reparents a validation-sensitive fold around a concurrent
commit. RFC-024 may later narrow false contention with table heads but is not a
correctness dependency. The commit phase then:

1. stages accepted rows with Lance merge-insert and includes the exact
   `MergedGeneration` cut in that same transaction;
2. acquires every affected queue in canonical sorted order and revalidates the
   complete `ReadSet`; any mismatch discards and replans the whole fold;
3. writes one dedicated schema-v11 `StreamFold` payload inside RFC-022's shared
   recovery envelope before `commit_staged`; the payload carries stable table
   identity, the exact physical binding and prior witness, shard/generation
   cut, exact transaction identity plus merged-generation set, and fixed
   lineage;
4. commits the staged Lance transaction with zero transparent conflict retries;
5. durably confirms the achieved table version, transaction, and merge-progress
   effect;
6. captures the achieved base-table witness and publishes the table version,
   next lifecycle `CurrentHeadWitness`, and fixed lineage in one
   `__manifest` CAS, including table heads when RFC-024 is active;
7. deletes the sidecar after successful publication.

The adapter is a distinct writer kind, not a Mutation payload with extra fields.
The sidecar is mandatory even though merge-insert is staged. After
`commit_staged`, Lance HEAD and `merged_generations` have moved while the graph
manifest has not. A failure in that window is the ordinary multi-table recovery
gap, not invisible staged state.

A commit-time key conflict follows RFC-023's partial-effect rule. If exact
classification proves that no fold participant advanced, the fold finalizes
the empty sidecar and may perform one bounded full replan from fresh authority.
If any participant advanced, or emptiness cannot be proved, it keeps the
sidecar and returns `RecoveryRequired`. No later fold is prepared until the
synchronous recovery barrier resolves that exact attempt; `merged_generations`
is never replanned around an unresolved partial fold.

MemWAL generation GC can start only after the exact fold is graph-visible, its
sidecar is resolved, index catchup permits reclamation, and no `FreshReadCut`
retention guard references the generation. Data-HEAD merge progress alone is
never permission to delete the only fresh-tier copy. B1 deliberately performs
no GC; B2 cannot expose production admission until the reclamation proof and a
hard retained-storage stop are implemented.

Concurrent folders reload `merged_generations`: a generation already committed
is skipped; otherwise the fold is replanned from current state. The explicit
fold stops new flush creation for its cut, waits for in-flight durability
waiters, and never aborts an acknowledged row. Default graph reads remain on
the old manifest pointer after the table commit and see the rows only after the
single manifest CAS.

Phase B1 records fixed fold mechanism lineage only because it has no public
caller. Durable authenticated-contributor attribution is a B2 activation gate:
RC.1 exposes no authoritative per-put WAL range and retaining the WAL cannot
recover actor identity that was never stored. Public audit must not infer
provenance from MemTable positions or WAL cursor statistics. The attribution
mechanism requires its own accepted design/evidence before public rows exist;
restart-stable dead-letter row identity may remain Phase C.

## 7. Fold-time rejection is atomic

Phase B1 is strict only. A permanent RI, cardinality, uniqueness, or embedding
failure returns a typed blocked outcome, publishes no table pointer or lineage,
and leaves the acknowledged generation durable and unmerged. B1 admits no next
generation while that one remains unmerged, so it intentionally has no
correction lane and makes no claim that a later row can unblock it. B2 cannot
activate until a bounded correction/disposition protocol preserves every ack,
survives crash, and cannot make the fold exceed one keyed transaction. No row
is silently dropped or credited to a dead letter.

`dead_letter` remains Phase C. The earlier proposed identity
`(stable_table_id, incarnation_id, shard_id, generation, wal_position)` is not
implementable on RC.1's public contract: the durability watcher returns no WAL
position, its exposed batch positions are MemTable-local, the next WAL cursor is
only a statistic, and one WAL entry may carry multiple rows. Phase C must prove
a public, restart-stable reject-row identity and consume B2's already-durable
contributor attribution; it may use neither mutable aliases/paths nor inferred
WAL statistics.

Once that identity exists, `_ingest_rejects` may become a versioned internal
Lance participant in the same fold/recovery/manifest pipeline. Until then there
is no reject table, reject retention promise, or dead-letter status surface.

## 8. Epoch-fenced quiescence barrier

Branch operations, schema changes, stream teardown, and Lance upgrades require a
real barrier, not an empty check.

Each enrolled table has a durable
`stream_state:<stable-table-id>:<incarnation-id>` row in its manifest branch with
`OPEN | DRAINING | SEALED`, configuration hash, an
`epoch_floor_by_shard: Map<shard_id, u64>`, the §3 physical binding, and the
current base-table `CurrentHeadWitness`. Epochs
are comparable only within the same enrollment and shard ID; a fresh shard in a
new binding may start at 1 and is fenced from its predecessor by enrollment and
shard identity, not by a larger number. The row is the logical lifecycle
authority and is updated by an RFC-022 CAS. MemWAL shard epochs are the shard
writer fence; the bounded profile pairs them with the process-local admission
lease and exclusive base-HEAD ownership. The general profile additionally
requires the cross-process substrate admission seal. Neither an empty-
generation observation nor an unscoped in-memory writer registry can
substitute for the profile's complete authority/fence pair.
Lifecycle-only transitions are audited manifest metadata transactions; they do
not create graph-content commits or move `graph_head`.

The bounded-profile drain sequence is:

1. acquire the root-scoped admission lease exclusively. This prevents a new
   final check/claim and waits until every append that passed its final check
   has resolved durability;
2. revalidate the exact `OPEN` binding, current-HEAD witness, and shard epoch,
   then publish `OPEN -> DRAINING` with the target epoch floor;
3. keep admission exclusively closed, claim/confirm the next shard epoch to
   fence a stale owner, and reject or backpressure every new append;
4. flush active MemTables and fold every
   generation to empty;
5. verify shard manifests and base `merged_generations` agree on emptiness;
6. publish `DRAINING -> SEALED` with the verified generation cut and exact
   achieved per-shard epoch map;
7. for an operation-scoped drain, perform the guarded operation; persistent
   public quiesce stops after step 6.

`OPEN -> DRAINING` and `DRAINING -> SEALED` are RFC-022 authority-first
metadata writes; each fold is a separate normal RFC-022 graph write.
`DRAINING` fully encodes every target per-shard floor and current-HEAD witness,
so restart reconstructs the admission gate closed and resumes the drain. A
`SEALED -> OPEN` transition is different: an exact `stream_resume` sidecar
covers the higher-epoch claim while the gate remains exclusively closed and is
resolved before any ack path proceeds. The drain itself is not one giant
sidecar spanning multiple commits.

`DRAINING` is not allowed to become an operator trap. B2 must also implement a
crash-safe abort transition for a quiesce that cannot finish: only when no
guarded schema/maintenance operation has begun and the exact binding/witness is
unchanged, `stream resume --abort-drain` arms a resume sidecar, claims a higher
epoch under the closed gate, and CASes `DRAINING -> OPEN`. If the drain is
strict-blocked, that transition is permitted only as part of B2's accepted
bounded correction protocol; otherwise it remains `DRAINING`. B1 exposes
neither transition and cannot deadlock a public operator because it has no
public caller.

For the general multi-process profile, step 3 must instead include the public
cross-process seal required by §3; it atomically refuses later claims and
fences a claimant that crossed another process's lifecycle check. The
process-local sequence is not evidence for that topology.

There are two dispositions after the drain reaches `SEALED`:

- **operation-scoped drain (Phase D)** — branch/schema maintenance automatically publishes
  `SEALED -> OPEN` only after the guarded operation succeeds, the stream
  contract remains compatible—including the bounded profile's exact
  no-named-graph-branch topology—and the §3 physical binding still names the
  exact table/ref and the guarded operation has published the table's new
  `CurrentHeadWitness` with its table pointer. A branch operation that makes
  that topology incompatible leaves the lifecycle `SEALED`; it cannot
  auto-resume. Under an exact resume sidecar,
  reopening the same binding
  advances each same-shard epoch above its recorded floor before the CAS; a
  pre-CAS failure keeps admission closed and recovery resumes or fails closed.
  If the operation rematerialized the table or changed/recreated its native
  ref, it must complete `stream_rebind` instead of applying this transition
  directly;
- **persistent quiesce (Phase B2)** — the public `quiesce` command leaves the stream
  `SEALED`. It never auto-reopens. `stream resume` explicitly revalidates schema,
  PK, configuration, MemWAL format, physical binding, current-HEAD witness,
  every same-shard epoch, and the exact graph-branch topology under the same
  closed admission/schema/branch gates, then publishes `OPEN`. In the bounded
  profile any named graph branch keeps the stream `SEALED`. Stream teardown
  deletes intent only from `SEALED`.

The barrier never holds the table write queue while waiting for a fold that
needs that queue. The separate admission gate closes first; fold commit then
acquires the normal table queue. Crash recovery resumes from the durable state,
current-HEAD witness, and per-shard epoch map.

Schema apply must drain every affected enrolled type before changing fields,
constraints, PK, embeddings, or `@stream` and resumes only when compatible.
RFC-028's current pure type rename retains the same dataset, identity, path,
and Lance version, so it does not by itself rebind the physical enrollment. If
a future schema feature supports a rematerializing rename while preserving the
logical pair, it cannot preserve the old physical enrollment:

1. drain, fold, fence, and publish the old binding as `SEALED`;
2. let SchemaApply rematerialize and publish the target table while leaving the
   lifecycle row `SEALED`;
3. run the recovery-covered §3 `stream_rebind` against the exact target, with a
   fresh never-reused enrollment UUID and shard UUID namespace; and
4. publish `OPEN` only in the manifest CAS that confirms the new binding and
   its initial per-shard epochs. Those epoch values are not compared with the
   old shard namespace.

The old shard namespace and artifacts remain retained until SchemaApply and
rebind sidecars are resolved and every fresh-read/recovery guard releases them.
A preserved physical enrollment never resets its generation or WAL-position
counters. A rebind never reuses an old shard UUID; pinned-Lance surface guards pin
that OmniGraph supplies a fresh UUID v4 to `mem_wal_writer` and that the new
namespace is disjoint from every prior binding for the logical table lifetime.

A Lance version upgrade requires persistent `stream quiesce --all`, but empty
generations alone are insufficient: the MemWAL system index, shard manifests,
epoch records, and generation directories may still use the old format. Before
the bump, the implementation must prove one of: (a) upstream guarantees and
cross-version tests cover every retained MemWAL artifact, (b) a public Lance
metadata migration converts them, or (c) OmniGraph tears down the enrolled
MemWAL metadata under recovery and re-enrolls after the bump. Without one of
those gates the upgrade refuses; `resume` never opens unverified old metadata.
This paragraph governs a Lance-only bump that preserves OmniGraph's current
internal format. An OmniGraph format rebuild follows §11 instead and never
opens retained source-graph MemWAL artifacts in the target graph.

## 9. Fresh-read cuts

Freshness is a first-class engine/IR enum:

```text
Committed
Fresh
```

At query planning, `Fresh` captures one `FreshReadCut` containing:

- the ordinary manifest snapshot;
- each selected table's exact lifecycle state, `StreamPhysicalBinding`, and
  `CurrentHeadWitness`;
- each selected shard-manifest version and writer epoch;
- included flushed-generation paths and maximum generation;
- the active same-process MemTable row-position watermark, when available;
- the base table's `merged_generations` and index-catchup state read from the
  exact table version selected by the manifest snapshot, never from live HEAD.

Capture uses a retrying handshake:

1. read each selected lifecycle row and require `OPEN`; capture its exact
   physical binding and current-HEAD witness, then read only that binding's
   shard manifests/epochs,
   acquire Lance generation retention guards for the flushed files in the
   tentative cut, and under one same-process writer snapshot capture/pin any
   active-MemTable watermark;
2. pin the graph manifest snapshot and require it to select the same lifecycle
   binding, current-HEAD witness, and physical base table/ref captured in step
   1; read
   `merged_generations` from each exact base-table version it selects. A
   `DRAINING`, `SEALED`, or different enrollment restarts the whole capture;
3. re-read the lifecycle rows, complete physical bindings, current-HEAD
   witnesses, shard manifest versions, and per-shard epochs. Any enrollment,
   witness, configuration, state, shard-set, manifest-version, or epoch change
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

Phase B1 records internal metrics and test probes for durability waits, replay,
fencing, resource refusal, fold rows/bytes/generations, visibility lag, and
sidecar recovery. It does not create a public status contract.

B2's minimum status surface exposes the exact lifecycle/binding, active epoch,
pending generation/row/byte totals, merged progress, last fold error,
strict-blocked state, and quiescence state. Richer index-catchup, age, and
reject detail may follow in Phase C. Lance's
`wal_entry_position_last_seen` and next-position statistics are explicitly
labeled hints; neither is presented as a durable per-row receipt or a way to
resolve one `AckUnknown` attempt.

Metrics cover ack latency, durability-wait batching, fenced writers, replayed
entries, fold rows/bytes/generations, fold retries, lag, reject counts, and
sidecar recovery. Defaults for every byte/count/time bound are documented and
configuration changes are observable behavior.

The acknowledgement cost claim is only for a warm, already-claimed writer in
steady state and includes both WAL append and synchronous in-memory index/PK
update performed before the watcher advances. Cold claim, higher-epoch reopen,
and replay are measured separately and may scale with the retained WAL tail.
The incremental **data scan** is measured against its selected generation and
rows. B1 performs no generation GC, and RC.1 shard manifests retain flushed-
generation entries, so authoritative shard-manifest fetch/decode/filter work
is also measured against accumulated already-merged generation metadata. The
shared graph-manifest publisher retains its separately documented uncompacted-
history term. Neither metadata term is relabeled history-flat here.

Phase-B2/Phase-C `stream status` resolves the exact lifecycle rows and MemWAL metadata through a
structured, bounded access path; it may reuse RFC-024's scalar-index machinery
but cannot claim history-flat cost while scanning manifest history.

## 11. Format activation and rebuild

Streaming is a graph-format capability, not a feature activated by the first
enrollment. Internal schema v7 now declares the Phase A foundation on every new
root: identity-keyed lifecycle rows are a recognized authority kind, the v10
enrollment intent is recoverable, and uncovered lifecycle/MemWAL mismatches are
refused. A physical enrollment adds one table's MemWAL index, empty shard, and
exact lifecycle row; it does not change the graph stamp.

V7 activation followed Gate E0 and the bounded enrollment/recovery,
writer-exclusion, lifecycle, crash, and refusal/rebuild evidence. It is not
activation of row streaming. The ordinary SDK, CLI, server, schema parser, and
OpenAPI contain no enrollment or stream entry point; only the feature-gated
fault-injection suite can call the private adapter. The public exact-enrollment
and cross-process admission-seal surface remains required before expanding
beyond the bounded profile.

Phase B1 is a second, explicit format gate rather than an in-place reinterpretation
of v7. It plans internal schema v8, stream-config v2, and recovery schema v11
for `StreamFold`. V8 is the first format allowed to contain acknowledged
data-bearing MemWAL state. A v7/config-v1 private enrollment is accepted only
by the Phase-A binary and contains no publicly acknowledged rows; the B1 binary
refuses it and requires export/init/load into a different v8 root. The v8
change ships only with genuine v7↔v8 refusal/rebuild tests.

Old binaries refuse a stream-capable graph before reading or writing any table.
A stream-capable binary refuses an older graph before running recovery. On a
compatible stamp it resolves or refuses every stream recovery kind relevant to
that format and binding before validation—B1 enrollment/fold, B2
resume/abort-drain, and Phase-D rebind included—then refuses an **uncovered**
partial-format state in which the stamp, accepted schema identity, enrolled
MemWAL metadata, and lifecycle authorities disagree. A covered stream crash is
recoverable intent, not format corruption. The engine never repairs an
uncovered mismatch by inferring ownership from a table name, path, or
compatible-looking system index.

The same rule will cover schema rematerialization once stream-aware SchemaApply
is implemented. A preserved `(stable_table_id, incarnation_id)` with a new physical
table is a `SEALED` stream awaiting the exact §3 rebind, not an `OPEN` stream
and not permission to attach old shards. Recovery classifies the old and new
bindings independently and retains the old artifacts until their sidecars and
read guards permit reclamation.

There is no in-place activation or rollback to an old format. A v6 graph moves
to v7, and a v7 graph moves to planned v8, only through the strict
export/init/load strand. Neither v6 nor v7 can contain publicly acknowledged
MemWAL rows, so the stream-specific quiesce steps below are vacuous for those
transitions; they become load-bearing for any later rebuild from a format that
exposes durable admission:

1. persistently quiesce every enrolled stream and fold every acknowledged row
   into the manifest-visible base tables;
2. verify `SEALED`, empty-generation, merged-generation, sidecar, and uncovered
   drift invariants on the source graph;
3. use the old-format binary to export each selected branch's current logical
   state;
4. use the new binary to initialize a different graph root and load the export
   through RFC-022;
5. validate the rebuilt graph before cutting clients over.

Ordinary export contains only manifest-visible graph state. WAL-only rows,
MemWAL indexes, shard manifests, epochs, lifecycle rows, reject history,
fresh-read guards, and fold checkpoints are not transferred. Therefore step 1
is mandatory: an acknowledged but unfolded row would otherwise be lost. The
new graph starts with no physical stream enrollment. Phase B2 must wire declared
first use through §3 before production can enroll after cutover. The rebuild also loses branches not
separately exported, commit DAG, snapshots, tombstones, recovery history, and
time travel, as specified by RFC-028's common format strand.

The retained source graph and its MemWAL artifacts remain owned by the old
binary and are never opened by the new target as migration input. A failed
target init/load cannot mutate the source. Independently released later format
capabilities require another rebuild; co-release with RFC-023, RFC-024, RFC-025,
or RFC-028 is allowed only after every participating RFC is independently
accepted and their combined init, refusal, recovery, and rebuild matrix passes.

Enrollment, fold, and recovery retain RFC-022's single-live-writer-process
support boundary, strengthened for the bounded profile by exclusive base-HEAD
ownership while `OPEN`. MemWAL's shard epoch permits a crash successor after
external exclusivity; it does not turn OmniGraph sidecar recovery into
distributed fencing or authorize overlapping replica failover.

## 12. Acceptance gates

### 12.1 Gate E0 — bounded enrollment decision (green)

Gate E0 is deliberately isolated from the production manifest schema and write
path. Its checked-in evidence suite must prove all of the following on the
pinned public RC.1 surface before the bounded profile can proceed:

- From exact baseline HEAD `N` with no MemWAL index, initializer success yields
  exactly `N + 1`, whose transaction reads `N` and contains only one singleton
  `__lance_mem_wal` `CreateIndex` with the requested unsharded configuration,
  namespaced enrollment ID, and configuration-version marker. The marker
  is classified independently of the index UUID and survives ordinary commits
  and merged-generation metadata updates.
- Discarding the initializer result and reopening produces the same exact
  allowed-successor classification; an index-only crash is therefore
  distinguishable from no effect without relying on the mutable Dataset
  handle.
- Passing a pre-minted UUID through the public shard-writer path produces only
  that shard with the expected unsharded spec, observable claimed epoch, empty
  generations, and no data-bearing WAL. Any deterministic data-less fence
  artifact is enumerated explicitly rather than treated as “empty enough.”
- The classifier truth table accepts only: exact no effect (finalize), the
  exact index-only successor (roll forward), and that successor plus the exact
  empty pre-minted shard (roll forward). Wrong configuration, an intervening
  HEAD, another index transaction, a foreign shard, unexpected WAL/generation
  data, a buried effect, or unreadable/ambiguous evidence yields
  `RecoveryRequired`; the harness never deletes or reclaims an artifact.
- `CurrentHeadWitness` is stable across an unchanged reopen, changes after an
  ordinary commit, and distinguishes same-path/same-version recreation on
  local FS and S3/RustFS. Starting from a freshly verified exact-`N` handle,
  the immediate `N + 1` and buried-`N + 2` successor probes are bounded in
  history depth; flatness is measured, not inferred from the tuple shape.

**Gate E0 result (2026-07-18): green for the bounded profile.** The final local
run reports 14 substantive tests plus one explicit S3 skip when no bucket is
configured. It covers the exact no-effect/index-only/index-plus-empty-shard
progression, high-level pre-minted `mem_wal_writer`, lost-result
reclassification, durable marker survival, buried-effect refusal, strict
inventory/error handling, and the complete local fail-closed matrix.

The earlier `checkout_latest`/`IOTracker` result was discarded because local
filesystem `read_dir` bypassed that tracker. The accepted classifier never
resolves latest. It opens exact `N`, uses the doc-hidden public
`Dataset::has_successor_version` for `N + 1`, and repeats it on exact `N + 1` to
reject a buried `N + 2`. Its `AttemptTracker` records every attempt before
forwarding, including failed and `NotFound` HEADs. At baseline versions 8 and
80 the complete shape is identical: four successful manifest HEADs, one
`NotFound` manifest HEAD, one successful manifest GET, and zero `list` or
`list_with_delimiter` calls. A Unix execute-only `_versions` tripwire proves
the exact probe succeeds while latest enumeration fails; an unreadable exact
HEAD returns an error rather than false/no-effect.

The configured RustFS exact cell passes non-vacuously. It covers no effect,
opaque initializer lost result, exact index-only state, the pre-minted empty
shard, unchanged reopen, and fail-closed foreign-shard, malformed-plus-loose-
root, durable-WAL, persisted-cursor, and corrupt-manifest states. It observes
the same six-attempt, zero-list exact-probe shape. Separate surface guards pin
`has_successor_version`, flush/drain, merged-generation state, and object-store
same-coordinate ABA; CI rejects a skipped Gate E0 or ABA cell.

This green decision authorizes only Phase A's production foundation: exact
roll-forward enrollment recovery, central lifecycle effect exclusion (with the
narrow `SEALED` native-branch exception), lifecycle/current-witness state, and
admission-lease races. It does not change
the schema parser, expose an API, or acknowledge a WAL row.
The general upstream receipt/seal path remains the preferred simplification and
the gate for broader topology.

### 12.2 Phase A — bounded production foundation (green)

Phase A is implemented at the deliberately narrow boundary established by E0:

- Internal schema v7 adds one identity-keyed
  `stream_state:<stable_table_id>:<incarnation_id>` authority row carrying the
  never-reused enrollment/shard binding, exact mutable current-HEAD witness,
  `OPEN | DRAINING | SEALED`, and per-shard epoch floor. Publication uses an
  expected-value CAS; identity is never inferred from alias or path.
- A dedicated schema-v10 `StreamEnrollment` sidecar owns the exact baseline,
  fixed lineage, intended config/binding, and only allowed `N -> N + 1`
  initializer effect. Recovery accepts exact no effect, exact index-only, or
  exact index-plus-empty-shard. Index-only provisions the pre-minted shard;
  the complete state publishes the table pointer plus `OPEN` row. Once an
  effect exists the adapter is roll-forward-only: it never restores the table
  or deletes/reclaims MemWAL artifacts.
- Enrollment is crate-private (with one feature-gated failpoint seam), supports
  canonical main and exactly one empty unsharded epoch-1 shard, and refuses
  while any named graph branch exists. There is no production SDK, CLI, HTTP,
  OpenAPI, or schema-language call site.
- One root-scoped process-local admission lease is acquired outside the
  schema → branch → sorted-table gates. Existing graph writers, SchemaApply,
  BranchMerge, EnsureIndices, Optimize, Repair, Cleanup, recovery, and native
  branch controls join the same admission/exclusion discipline and re-check
  lifecycle authority under their final gates. Phase A fences every
  base-table, schema, maintenance, repair-adoption, and recovery effect for any
  lifecycle row, including `SEALED`, because it has no witness-update/rebind
  adapter yet. Native branch create/delete is the narrow exception: it may
  proceed at `SEALED` because it does not advance the table HEAD, while
  `OPEN`/`DRAINING` still refuses it.
- The initial topology closes the cross-branch authority hole explicitly:
  enrollment requires a main-only graph, branch create/delete refuses an
  `OPEN` or `DRAINING` lifecycle, and open-time consistency refuses that
  lifecycle if a named branch exists. This is a bounded support rule, not
  branch-aware streaming.
- Read-write open resolves exact enrollment intents before serving. Read-only
  open refuses a pending enrollment intent. Compatible opens then validate
  every lifecycle against the manifest-selected identity/path/witness and
  exact physical empty-shard state, and reject an uncovered MemWAL index or
  other partial-format mismatch.
- V7 remains a strict strand: v7 refuses genuine v6 and v6 refuses v7; upgrade
  is export/init/load into a different root. Crash tests cover no-effect,
  index-only, and index-plus-empty-shard recovery, plus named-branch and
  uncovered-format refusals. Lower-level tests pin lifecycle CAS, typed writer
  exclusion, admission ordering, and the exact E0 classifier.

This is a production-format and recovery foundation, not a usable stream.
Phase A never calls the WAL row `put` path, never emits a durability
acknowledgement, never folds a generation, and never exposes drain/resume or
fresh reads. Although v7 can encode `DRAINING` and `SEALED`, their operational
transitions are not implemented: B2 owns minimum explicit quiesce/resume for a
public escape, while Phase D owns automatic operation drain and rebind.

### 12.3 Phase B1 — private strict core gates

Phase B1 is complete only when all of these are checked in. It does not expose
schema syntax or a production caller.

- Internal schema v8, stream-config v2, and schema-v11 `StreamFold` recovery
  activate data-bearing state. Genuine v7/config-v1 ↔ v8 refusal/rebuild tests
  pass; no Phase-A enrollment is silently adopted.
- Surface guards pin `put_no_wait`, watcher shape, forced seal/drain,
  quiesced `abort`, per-shard epoch fencing, staged merge plus
  `merged_generations`, and index catchup. They also pin public
  `in_memory_memtable_refs` plus BatchStore iteration and the RC.1 replay-
  watermark bridge for restart-derived reservations, `LsmScanner::without_base_table`, caller-supplied exact
  `ShardSnapshot`, generation tagging/streaming, and propagation of the shared
  Session and optional store parameters.
- The config guard pins the 8,193 BatchStore capacity and portable 1-GiB byte
  thresholds, proves the widest legal generation's RC.1 trigger estimate stays
  below them, and separately measures omitted PK-index/RSS overhead. It does
  not credit `max_memtable_rows` as a rollover trigger. The 10-MiB WAL buffer,
  opportunistic interval, immediate durable flush, WAL-plus-index join, and
  currently unread synchronous/async-index/stats fields are classified exactly
  as in §5 rather than assigned behavior from their names.
- An adversarial guard demonstrates RC.1's cross-rollover false-ack shape by
  delaying generation `N + 1`'s WAL PUT after `N` advanced the writer-wide
  watermark. The B1 adapter proves it prevents automatic rollover, seals one
  generation, retires that writer, and reopens at a higher epoch before the
  next put. `ShardWriter::close` is never treated as durability evidence.
- The root-scoped, cross-handle registry is singleflight per full stream
  binding, while every registry entry reuses the Phase-A table-identity plus
  resolved-physical-ref `StreamAdmissionKey`; enrollment/shard identity never
  creates a second admission domain. Core config/worker tests remain
  crate-internal; one `#[doc(hidden)]`,
  feature-gated test seam owns graph/failpoint/cost integration, and the
  forbidden-API guard exact-counts that seam. A two-handle claim/eviction race
  proves no self-fencing writer pair, no eviction past an in-flight waiter, and
  no put after the worker begins its `abort` retirement sequence.
- Cold claim/replay and every warm final-check/put hold the shared admission
  lease from before `mem_wal_writer` claims an epoch through durability or
  quiesced retirement. A same-table ordinary-writer-vs-drain test and a
  claim-vs-drain adversarial race prove that all paths contend on the common
  lock domain and that the exclusive drainer cannot capture a stale epoch floor
  while a claimant crosses it.
  Stale-capture-vs-fold/drain and late-sidecar races prove the under-lease
  pre-claim and pre-put sidecar/authority checks restart before any row effect.
- Every admitted B1 call is one non-empty normalized `RecordBatch` with one
  contiguous ordinal range. Both the call and the complete generation,
  including any duplicate batch submitted before an ambiguity retires that
  live writer, are hard-capped at 8,192 rows/32 MiB before
  the row put. Crossing the generation cap returns row-effect-free
  `FoldRequired` before `put_no_wait`; B1 never admits a second unmerged
  generation.
- Every put enters through the synchronous recovery barrier. The serialized
  worker owns the final authority check, `put_no_wait` invocation, and watcher.
  Only an error before invocation is proven effect-free. Any error, missing
  watcher, cancellation, or deadline after invocation is `AckUnknown`, retires
  the writer, and remains caller-ambiguous after replay.
- Phase A's empty validator is replaced before first put. Reopen validates the
  active configuration, shard/epoch authority, authoritative replay cursor,
  exact persisted generation topology, merge progress, and lifecycle witness,
  and routes only the four §5 active/flushed states. Non-empty replay is
  fold-only. Before sealing it, the guarded public BatchStore watermark bridge
  marks the exact replayed prefix durable; a repeated pre-manifest crash test
  proves WAL/batch count does not multiply. A flushed-unmerged generation also
  resumes fold-only, and no put can create a second unmerged generation.
- One private strict fold seals/drains exactly one generation, constructs an
  exact fresh-only snapshot, resolves last-write-wins by `id`, and materializes
  embeddings, other fold-derived fields, and base-dependent validation before
  effect; defaults were fixed pre-ack and are not re-applied. Its deduplicated result
  must still fit one 8,192-row/32-MiB keyed transaction; otherwise it remains
  durable and strict-blocked.
- Post-drain proof requires both empty frozen refs and the exact generation plus
  replay cursor in the authoritative shard manifest; a late waiter cannot
  trust `wait_for_flush_drain() == Ok(())` alone. Failpoints cover fast failed
  flush, flush-channel loss, handler/abort stall and deadlines, pre-/mid-
  generation output, and post-shard-manifest crashes before sidecar arm. An
  unreferenced recognized randomized generation subtree, complete or partial,
  is retained as derived orphan state, never adopted or deleted by B1; other
  loose state fails closed. A seal error or deadline never claims an
  effect-free/quiesced outcome or reopens admission beside the unresolved
  handler. A stalled-handler/caller-deadline/second-retirement adversarial test
  proves the original background-owned abort completion is retained: no future
  is cancelled, no second abort reports false success, and no reopen occurs
  before that exact completion settles.
- A dedicated schema-v11 `StreamFold` sidecar pins the binding, epoch, immutable
  cut, exact transaction, merged-generation set, and fixed lineage. Recovery
  classifier/serialization tests live with `recovery.rs`; `failpoints.rs`
  owns crash orchestration around arm, table commit, confirmation, and manifest
  publication. No later put or fold proceeds past an unresolved sidecar.
- `MergedGeneration` commits inside the Lance table transaction. One manifest
  CAS publishes the achieved table pointer, next lifecycle witness, and fixed
  lineage; it does not separately publish merge-progress metadata.
- Retrying an `AckUnknown` batch with identical stable IDs/payload may append it
  again and preserves one-row LWW cardinality only when no newer same-key value
  interleaves. Tests do not claim the original attempt was reconciled and pin
  the unsafe `X(unknown) -> Y(durable) -> retry X` overwrite. Strict rejection
  leaves durable unmerged input and B1 admits no correction generation; B2
  remains gated on a sequencing/idempotency contract.
- Warm steady-state ack object-store work is O(1) and flat in graph history;
  cold claim/reopen/replay is measured separately against retained WAL depth.
  Incremental fold **data scan** is bounded by the selected generation/rows;
  the instrument separately sweeps accumulated already-merged generation
  metadata in the retained shard manifest, while publisher cost retains the
  known graph-manifest-history term. Evidence runs locally and against
  configured RustFS.

### 12.4 Phase B2 — public strict activation gates

Phase B2 may begin only after B1 is green **and** accepted designs exist for
durable authenticated-contributor attribution, bounded WAL/generation
reclamation, strict correction/disposition, and same-key sequencing or
idempotency after `AckUnknown`. Public activation also requires a persistent
escape from an `OPEN` stream; a user must never be left with a table that
ordinary writers refuse but cannot be quiesced or rebuilt.

- `@stream(mode="upsert", on_reject="strict")`, production first use, SDK,
  HTTP, CLI, Cedar, and OpenAPI all route through the same private B1 core. The
  existing `/ingest` behavior remains compatible, and embedded/remote command
  parity is tested.
- The public acknowledgement is status-only and caller-ordered. It maps the
  private durable batch result back to each caller ordinal and reports the
  enrollment/shard/epoch binding, not a WAL position, generation, or Lance
  `batch_positions` value. The B2 adapter owns contiguous-run batching around
  invalid NDJSON lines and its bounded reorder buffer.
- Minimum operator controls expose authoritative status, explicit fold, plus
  persistent quiesce, resume/abort-drain, and rebuild preflight. Status includes exact
  lifecycle/binding, epoch, pending generations/bytes, last fold error, and
  strict-blocked state. Quiesce has a crash-safe cut, never auto-reopens, and
  proves that every acknowledged row is folded before export/cutover. Resume
  revalidates the same binding, no-named-branch topology, and epoch authority
  before advancing its epoch. A failed drain can return
  `DRAINING -> OPEN` only before a guarded operation and through the accepted
  correction protocol.
- Every public batch durably records its authenticated contributor before rows
  can become unattributable, and the fold audit consumes that evidence. A
  bounded reclamation protocol plus hard retained-bytes stop prevents public
  admission from growing the WAL forever. Neither contract is inferred from
  MemTable positions or mutable WAL statistics.
- Admission cancellation and server shutdown cannot abandon an in-flight
  durability waiter. A background-owned admission task reaches a durable
  outcome or typed `AckUnknown`. Status/replay preserves possible residue but
  never claims to reconcile that caller's attempt.
- The accepted retry contract prevents an ambiguous `X(id)`, newer durable
  `Y(id)`, retry `X(id)` sequence from silently restoring the stale `X` value.
  Tests pin that exact interleaving; one-row LWW cardinality alone is not
  accepted as semantic idempotency.
- `SEALED` enables safe export/rebuild into a different root. In-place
  SchemaApply/maintenance remains refused until Phase D.
- Genuine cross-version tests prove old-binary/new-format and
  new-binary/old-format refusal. Rebuild tests prove acknowledged rows survive
  only after quiesce/fold, and fail closed when an acknowledged row remains
  unfolded.

### 12.5 Later expansion gates

- Phase C defines restart-stable reject-row identity, using B2's durable
  contributor attribution, before adding atomic dead letters, reject
  retention, richer status, or configurable registry/memory/deadline limits.
  The old `(table, shard, generation, wal_position)` tuple is not accepted as
  identity.
- Phase D integrates automatic drain/resume with SchemaApply, branches,
  Optimize, Repair, Cleanup, and physical rebind. A rematerialized table stays
  `SEALED` until an exact sidecar-covered rebind publishes a new namespace.
- Phase E fresh reads race capture with fold/GC and rebind, retain generation
  guards through execution, exclude merged generations, and document their
  cross-table consistency boundary.
- Phase F multi-shard support requires a one-key-one-shard proof and a fresh
  Lance audit. Phase G overlapping-process ownership/failover still requires a
  public exact enrollment receipt plus cross-process seal/reopen, or a
  separately accepted distributed fence with adversarial recovery evidence.
- Surface, format, and S3/RustFS guards rerun before every Lance bump.

## 13. Phasing

| Phase | Content | Gate |
|---|---|---|
| E0 | production-neutral public-surface enrollment/witness classifier; no schema, API, sidecar, or format activation | **Passed 2026-07-18:** 14 substantive local cells, complete six-attempt zero-list 8/80 cost shape, Unix no-list/error tripwire, and one non-vacuous configured RustFS positive-plus-negative cell (§12.1) |
| A | bounded main/unsharded/single-live-writer enrollment adapter, all-lifecycle effect exclusion with only the `SEALED` native-branch exception, lifecycle/admission lease, then graph-format capability/refusal and strict rebuild | **Implemented 2026-07-18 (§12.2):** internal schema v7, recovery-v10 enrollment, durable lifecycle CAS, process-local exclusion, crash/partial-format refusal, and genuine v6↔v7 strand evidence; no public enrollment or row path |
| B1 | internal schema v8/config-v2, root-scoped one-generation admission worker, durability-watcher acknowledgement, conservative active-state reopen/replay, the pinned RC.1 replay-watermark bridge, and one explicit strict RFC-022 fold | no-rollover proof; whole-generation row/byte limits; claim/drain and replay/reseal crash guards; permanently ambiguous lost-ack retry; recovery-v11 fold crash matrix; manifest-only visibility; qualified local/RustFS cost evidence |
| B2 | public strict activation across schema, SDK, HTTP, CLI, Cedar, and OpenAPI, with minimum attribution, reclamation, status/fold, correction, same-key retry safety, and persistent quiesce/resume/abort-drain/rebuild escape | B1 green; accepted attribution/GC/correction/sequencing designs; API compatibility; cancellation/shutdown ownership; quiesce/fold/resume/abort-drain/rebuild survival matrix |
| C | restart-stable reject-row identity, atomic dead letter, richer status, and evidence-backed configurable bounds | reject crash matrix; reject-retention proof; backpressure and RSS/latency evidence |
| D | automatic operation drain, schema/branch/upgrade integration, and rematerialization rebind | two-coordinator race, old/new physical-binding crash matrix, and format-transition suite |
| E | fresh cuts and maintained-index reads; cross-process `Fresh` ships only if the substrate generation-retention guard exists (§9), otherwise same-process only | cut consistency; merged-generation exclusion |
| F | multi-shard upsert and stream deletes | one-key-one-shard proof; Lance re-audit |
| G | overlapping-process ownership/failover | public exact enrollment receipt plus cross-process seal/reopen, or a separately accepted distributed fence; adversarial multi-process recovery evidence |
