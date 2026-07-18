---
type: spec
title: "RFC-026 — MemWAL streaming ingest"
description: Adopts Lance MemWAL as OmniGraph's strategic streaming-write architecture, with durable per-row acknowledgement, graph-atomic folds, epoch-fenced quiescence, and explicit fresh-read cuts on the RFC-022 unified write path.
status: draft
tags: [eng, rfc, streaming, ingest, wal, memwal, lance, omnigraph]
timestamp: 2026-07-10
owner: OmniGraph maintainers
---

# RFC-026 — MemWAL streaming ingest

**Status:** Draft / Gate E0 passed; Phase A pending; production inactive
**Date:** 2026-07-10
**Gate E0 evaluated:** 2026-07-18
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

- stream acknowledgement means the row's WAL entry is durable;
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

Gate E0 added no manifest rows, sidecars, `@stream`, public APIs, WAL
acknowledgements, or a format stamp. Internal schema v6 remains the only served
production format. Its green result authorizes only the next implementation
slice (enrollment recovery, writer exclusion, and lifecycle integration); it
does not authorize durable stream admission. RFC-026 remains draft and
production inactive until the later gates close.

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

Initial delivery exposes
`@stream(mode="upsert", on_reject="strict")` on a node or edge type;
`on_reject` accepts `strict` or `dead_letter`.
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

Schema apply records `@stream` intent only. First stream use enrolls the physical
table by creating the singleton `__lance_mem_wal` system index and its sharding
configuration.

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
enough for the bounded profile. Until the following production adapter's later
activation gates pass, `@stream`, the streaming format, first enrollment, and
acknowledgement all remain inactive.

With Gate E0 green, the proposed bounded enrollment uses one RFC-022
multi-effect sidecar, not an ad-hoc state machine:

1. run and await RFC-022's synchronous recovery barrier;
2. authorize, pin the manifest/schema/table state, and prepare a complete
   `ReadSet` containing schema identity, stable table ID and incarnation,
   location/main ref, exact pre-enrollment `CurrentHeadWitness`, PK metadata,
   stream intent/configuration, and lifecycle-row absence for a first enrollment or
   the exact `SEALED` prior binding for a physical rebind;
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
   exact empty initial shard state: expected shard/spec identity, observable
   epoch, no flushed generation, and no data-bearing WAL entry. A deterministic
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
10. resolve the enrollment sidecar before admitting `put`. Ack paths run the
    recovery barrier, so `OPEN` plus an unresolved enrollment cannot
    acknowledge.

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

Admission is keyed by the exact
`(stable_table_id, incarnation_id, enrollment_id, shard_id)` binding. Before
claiming a writer, the append path captures `OPEN`, the complete physical
binding, `CurrentHeadWitness`, and that shard's epoch floor. After
`mem_wal_writer` claims an epoch, it re-reads the lifecycle row, physical base
HEAD, and shard status immediately before the first `put`: the binding and
witness must still be identical, state must still be `OPEN`, the shard must be
active, and the claimed epoch must exceed the recorded floor for that same
shard. A mismatch closes the writer and returns a typed retry without
appending.

The bounded profile closes the final check-to-put race with one root-scoped
process-local admission lease. Every append holds a shared lease from its final
`OPEN`/binding/witness/epoch check through durability resolution. Drain takes
the lease exclusively, waits out existing acknowledgements, and keeps it
closed through `DRAINING` and `SEALED`. On restart, a lifecycle state other than
`OPEN` is reconstructed as closed before requests are served. This is valid
only because the support boundary excludes an overlapping writer process; the
lease is not advertised as distributed fencing.

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

## 6. Fold protocol

The fold consumes flushed generations in ascending order. Embeddings and
base-dependent validation run outside the table queue. With or without
RFC-024, the `ReadSet` carries schema identity, the complete stream
binding/configuration/generation cut, the base table's exact
`CurrentHeadWitness`, every probed table, and the conservative branch authority
token `(native branch incarnation, optional graph_head)`.
Absence of `graph_head` on a fresh branch is part of that token. Any publisher
retry compares the captured token and returns to full fold revalidation on a
change; it never reparents a validation-sensitive fold around a concurrent
commit. RFC-024 may later narrow false contention with table heads but is not a
correctness dependency. The commit phase then:

1. stages accepted rows with Lance merge-insert and includes
   `merged_generations` in that transaction;
2. stages any rejection/audit rows required by §7;
3. acquires every affected queue in canonical sorted order and revalidates the
   complete `ReadSet`; any mismatch discards and replans the whole fold;
4. writes one generic RFC-022 recovery sidecar before the first
   `commit_staged` call;
5. commits every staged Lance transaction;
6. captures the achieved base-table witness and publishes all data/internal
   table versions, the next lifecycle `CurrentHeadWitness`, and lineage in one
   `__manifest` CAS, including table heads when RFC-024 is active;
7. deletes the sidecar after successful publication.

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
(stable_table_id, incarnation_id, shard_id, generation, wal_position)
```

The key never uses mutable `table_key`, type name, or path. A rename therefore
preserves ownership of reject history, while a same-name replacement cannot
claim the predecessor's rows. A same-dataset rename retains the shard namespace
and replay positions. A rematerializing rename uses §3's fresh never-reused
shard UUID namespace, so its generation/WAL positions cannot collide with the
old physical enrollment even though the logical table pair is preserved.

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

For the general multi-process profile, step 3 must instead include the public
cross-process seal required by §3; it atomically refuses later claims and
fences a claimant that crossed another process's lifecycle check. The
process-local sequence is not evidence for that topology.

There are two dispositions after the drain reaches `SEALED`:

- **operation-scoped drain** — branch/schema maintenance automatically publishes
  `SEALED -> OPEN` only after the guarded operation succeeds, the stream
  contract remains compatible, and the §3 physical binding still names the
  exact table/ref and the guarded operation has published the table's new
  `CurrentHeadWitness` with its table pointer. Under an exact resume sidecar,
  reopening the same binding
  advances each same-shard epoch above its recorded floor before the CAS; a
  pre-CAS failure keeps admission closed and recovery resumes or fails closed.
  If the operation rematerialized the table or changed/recreated its native
  ref, it must complete `stream_rebind` instead of applying this transition
  directly;
- **persistent quiesce** — the public `quiesce` command leaves the stream
  `SEALED`. It never auto-reopens. `stream resume` explicitly revalidates schema,
  PK, configuration, MemWAL format, physical binding, current-HEAD witness, and
  every same-shard epoch, then publishes `OPEN`. Stream teardown deletes intent
  only from `SEALED`.

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

## 11. Format activation and rebuild

Streaming is a graph-format capability, not a feature activated by the first
enrollment. A newly initialized stream-capable graph writes the complete
internal-format stamp, RFC-028 identity authority, and every other co-released
capability before it accepts data. First-use enrollment is permitted only on a
graph whose format already declares streaming; enrollment adds one table's
physical MemWAL index and exact lifecycle row but does not change the graph
format.

The capability stamp is not assigned merely because RC.1 MemWAL APIs are
present or because Gate E0's production-neutral harness passes. Initialization
can emit a stream-capable first state only after Gate E0 is green **and** the
bounded production enrollment/recovery, writer-exclusion, lifecycle,
refusal/rebuild, and crash gates below pass. The public exact-enrollment and
cross-process admission-seal surface remains required before expanding beyond
the bounded profile. Until the applicable profile is complete, both new-root
activation and first enrollment remain disabled.

Old binaries refuse a stream-capable graph before reading or writing any table.
A stream-capable binary refuses an older graph before running recovery. On a
compatible stamp it resolves any exact RFC-022 enrollment sidecar first, then
refuses an **uncovered** partial-format state in which the stamp, accepted
schema identity, enrolled MemWAL metadata, and lifecycle authorities disagree.
A covered enrollment crash is recoverable intent, not format corruption. The
engine never repairs an uncovered mismatch by inferring ownership from a table
name, path, or compatible-looking system index.

The same rule covers schema rematerialization inside an already stream-capable
graph. A preserved `(stable_table_id, incarnation_id)` with a new physical
table is a `SEALED` stream awaiting the exact §3 rebind, not an `OPEN` stream
and not permission to attach old shards. Recovery classifies the old and new
bindings independently and retains the old artifacts until their sidecars and
read guards permit reclamation.

There is no in-place activation or rollback to an old format. An existing graph
moves to a stream-capable format only through the strict export/init/load
strand:

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
new graph starts with no physical stream enrollment; first use enrolls each
declared stream through §3 after cutover. The rebuild also loses branches not
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
roll-forward enrollment recovery, central `OPEN`-table writer exclusion,
lifecycle/current-witness state, and admission-lease races. It does not change
internal schema v6, parse `@stream`, expose an API, or acknowledge a WAL row.
The general upstream receipt/seal path remains the preferred simplification and
the gate for broader topology.

### 12.2 Production activation gates after E0

- Surface guards pin claim, append, durability waiter, flush, per-shard epoch
  fencing, staged merge with `merged_generations`, and index catchup. They also
  pin RC.1's internal-commit initializer and separate shard effect so a Lance
  bump cannot silently invalidate the E0 classifier. The public exact receipt /
  seal/reopen surface remains required for the general multi-process profile.
- A WAL append failure emits no durable acknowledgement. Every acknowledged row
  survives crash, replay, fold, and recovery.
- Failpoints cover enrollment's index/shard multi-effect gap and every fold participant
  around sidecar, `commit_staged`, reject persistence, and manifest publish.
- A key conflict on fold participant N after participant 1 committed retains
  the exact sidecar, returns `RecoveryRequired`, and starts no replan until the
  recovery barrier resolves it; the no-effect case finalizes the empty intent
  before its bounded full replan.
- Enrollment restarts without an inline effect when schema, PK, table HEAD
  witness, or
  stream configuration changes between prepare and gated revalidation.
- Local and S3/RustFS same-path/ref delete-recreate races change the
  `CurrentHeadWitness` and make enrollment, ack, fold, recovery, and fresh
  capture refuse the replacement. A backend without a proven token refuses
  activation.
- While a bounded-profile lifecycle is `OPEN`, every non-fold table writer and
  maintenance/control path touching that table refuses before effect. After an
  operation-scoped drain, any allowed table commit advances the lifecycle
  witness atomically with its manifest table pointer before resume.
- A future rematerializing type rename preserves the RFC-028 logical pair, leaves the
  target `SEALED`, and cannot acknowledge until an exact sidecar-covered rebind
  publishes a fresh enrollment/shard namespace. Crash tests cover every old-
  drain, SchemaApply, rebind-effect, and rebind-publish boundary; recovery never
  adopts old shards by name/path or reuses their UUIDs.
- Two folders converge exactly once; a fenced stale writer can never produce a
  false durable acknowledgement after WAL GC.
- Without RFC-024, a concurrent graph commit that changes a probed-but-untouched
  table after fold validation changes the captured branch token, forces a full
  fold revalidation, and cannot be accepted by publisher reparenting.
- A second overlapping writer process is outside the bounded support contract
  and cannot be credited as safe merely because epochs prevent silent dual
  ownership. A crash successor acknowledges only after external exclusivity,
  recovery, a higher epoch, and complete lifecycle revalidation.
- Quiescence tests race appends with branch and schema operations across two
  coordinators and prove no post-drain tail appears; failpoints cover every
  lifecycle-row, admission-lease, per-shard epoch-fence, fold, and `SEALED`
  boundary. A claimant paused before its final lifecycle check and one paused
  after that check are both either included before the drain cut or refused
  before `put`. The general profile repeats this with the cross-process
  substrate admission seal.
- Persistent quiesce never auto-reopens; explicit same-binding resume advances
  each same-shard epoch, while a rebind starts an unrelated epoch namespace.
  Upgrade tests cover every retained MemWAL artifact through declared
  compatibility, migration, or teardown/re-enrollment.
- Genuine cross-version tests prove old-binary/new-format and
  new-binary/old-format refusal. Rebuild tests quiesce and fold acknowledged
  WAL rows before export, prove those rows survive in the target, and prove an
  unfolded acknowledged row makes export/cutover fail closed.
- Partial-format tests reject a capability stamp without matching identity and
  stream authorities, and reject enrolled MemWAL metadata without the declared
  capability, while an exact sidecar-covered enrollment gap recovers before
  consistency validation. No test simulates compatibility by merely rewinding
  a stamp.
- Fresh reads race capture with fold/GC and a completed physical rebind. They
  retry on a lifecycle/binding change or unexplained disappearing generation,
  hold generation/MemTable guards through execution, exclude merged
  generations, and document cross-table inconsistency explicitly; a cut can
  never pair an old shard namespace with the new base manifest.
- Server/OpenAPI tests preserve the old `/ingest` alias and cover the new route;
  CLI parity covers embedded and remote stream commands.
- Ack-path object-store operations are O(1) and flat in graph history and WAL
  depth. Fold cost is bounded by generations/rows folded, not graph history.
- S3 correctness runs against RustFS; API/format guards are rerun before every
  Lance bump.

## 13. Phasing

| Phase | Content | Gate |
|---|---|---|
| E0 | production-neutral public-surface enrollment/witness classifier; no schema, API, sidecar, or format activation | **Passed 2026-07-18:** 14 substantive local cells, complete six-attempt zero-list 8/80 cost shape, Unix no-list/error tripwire, and one non-vacuous configured RustFS positive-plus-negative cell (§12.1); production remains v6 |
| A | bounded main/unsharded/single-live-writer enrollment adapter, central OPEN-table exclusion, lifecycle/admission lease, then graph-format capability/refusal and strict rebuild | green E0; exact no-effect/roll-forward-only crash matrix; witness-chain and writer-refusal races; genuine cross-version/refusal and rebuild suite |
| B | new ingest route/CLI, durable ack, strict fold | ack durability; API compatibility; cost budget |
| C | atomic dead letter, audit provenance, status/bounds | reject crash matrix; backpressure tests |
| D | epoch-fenced drain, persistent quiesce/resume, schema/branch/upgrade integration, rematerialization rebind | two-coordinator race, old/new physical-binding crash matrix, and format-transition suite |
| E | fresh cuts and maintained-index reads; cross-process `Fresh` ships only if the substrate generation-retention guard exists (§9), otherwise same-process only | cut consistency; merged-generation exclusion |
| F | multi-shard upsert and stream deletes | one-key-one-shard proof; Lance re-audit |
| G | overlapping-process ownership/failover | public exact enrollment receipt plus cross-process seal/reopen, or a separately accepted distributed fence; adversarial multi-process recovery evidence |
