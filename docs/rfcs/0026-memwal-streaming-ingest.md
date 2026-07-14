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

**Status:** Draft / for team review
**Date:** 2026-07-10
**Author track:** Maintainer design series
**Depends on:** [RFC-022](0022-unified-write-path.md)'s unified write and
generic recovery-sidecar protocol, plus
[RFC-028](0028-stable-schema-identity.md)'s stable table identity and
incarnation contract, plus
[RFC-023](0023-key-conflict-fencing.md) for the initial keyed graph-stream
mode. Durable heads from
[RFC-024](0024-durable-table-heads.md) are compatible but not required.
**Surveyed:** omnigraph 0.8.1; Lance 9.0.0-beta.21 at git rev
`1aec14652dcbace23ac277fa8ced35000bea0c40`; complete MemWAL table and
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

One beta.21 API gap is a ship blocker, not evidence against the architecture:
`InitializeMemWalBuilder::execute` internally commits the MemWAL `CreateIndex`
transaction and returns only `Result<()>`, while initial shard-manifest creation
is a separate object-store effect reached through `mem_wal_writer`. The public
surface therefore cannot yet provide the exact pre-arm effect identities and
reversible admission seal required by §3/§8. OmniGraph does not reach through
private Lance modules or hand-roll those objects. Streaming activation waits
for the public recoverable-enrollment/seal gate defined below.

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

- concurrent interactive same-key writes serialize or fail/retry loudly;
- same-key stream entries resolve by MemWAL generation/position order;
- duplicate keys inside one bulk-load input retain the existing load error.

The schema and user docs state all three together.

## 3. Enrollment is a recoverable multi-effect adapter

Schema apply records `@stream` intent only. First stream use enrolls the physical
table by creating the singleton `__lance_mem_wal` system index and its sharding
configuration.

RFC-024 heads are optional, so this RFC carries its own exact physical binding
in every lifecycle row:

```text
StreamPhysicalBinding {
    table_location,
    table_branch,
    physical_ref_incarnation,
    enrollment_id,       // pre-minted UUID, never reused
    shard_ids,           // sorted UUID namespace, never reused
    stream_config_hash,
}
```

The binding is valid only when the current manifest table entry resolves to the
exact location/ref incarnation and that physical table contains the expected
MemWAL system index plus shard manifests for the recorded namespace.
`physical_ref_incarnation` is an opaque Lance/backend proof that remains stable
under ordinary HEAD advances but changes when the dataset or native ref is
deleted and recreated at the same path/name. For a non-main ref it includes the
native `BranchIdentifier`; for main it requires an equivalent dataset-root
incarnation proof. Path, branch name, numeric version, timestamp, and the
RFC-028 logical incarnation are not substitutes. A backend without a token
that passes local and S3/RustFS same-path/ref ABA guards cannot activate
streaming.

The MemWAL index UUID is not used as enrollment identity because Lance may
replace that metadata entry while advancing merged-generation state. A mutable
table name, the RFC-028 logical pair alone, or a compatible-looking index is
never enough. RFC-024 may project related table/ref facts into a durable head,
but RFC-026 persists and validates this binding without depending on heads.

Enrollment advances Lance HEAD and provisions shard-manifest objects. Before
implementation, Lance must expose one of these public, surface-guarded shapes:

1. a caller-controlled staged/uncommitted MemWAL initialization transaction
   with an exact transaction identity, plus idempotent public APIs to provision,
   classify, seal/reopen, and reclaim a pre-minted shard manifest; or
2. one recoverable enrollment primitive that returns an exact receipt covering
   both the index transaction and initial shard objects, with documented
   classify/roll-forward/rollback semantics.

The beta.21 `execute() -> Result<()>` initializer plus separately claimed shard
writer satisfies neither shape. Private-module access, compatible-looking
index inference, and direct object-store emulation are rejected. Until this
gate lands, `@stream` may be parsed/planned in draft work but the streaming
format and first enrollment do not activate.

Once that public gate exists, enrollment uses one RFC-022 multi-effect sidecar,
not an ad-hoc state machine:

1. run and await RFC-022's synchronous recovery barrier;
2. authorize, pin the manifest/schema/table state, and prepare a complete
   `ReadSet` containing schema identity, stable table ID and incarnation, exact
   physical-ref incarnation and pre-enrollment HEAD, PK metadata, stream
   intent/configuration, and lifecycle-row absence for a first enrollment or
   the exact `SEALED` prior binding for a physical rebind;
3. acquire any global claims and then the `(table, branch)` write queue in
   RFC-022 order, then freshly revalidate the complete `ReadSet`; a mismatch
   restarts before any physical effect;
4. verify RFC-023's already-installed PK; enrollment never performs a first-use
   PK migration; validate the sharding configuration;
5. pre-mint a never-reused enrollment UUID and shard UUID namespace, then arm a
   sidecar with writer kind `stream_enrollment` or `stream_rebind`, the exact
   target/ref token, the index transaction/receipt, the deterministic initial
   shard object set in physically non-admitting `SEALED` state, and the complete
   intended `StreamPhysicalBinding`;
6. commit the exact index effect, then provision the exact `SEALED` shard
   objects; neither effect admits appends;
7. classify both effects and mark the sidecar `EffectsConfirmed` only when the
   complete intended set exists with exact ownership;
8. publish the achieved table version, exact physical binding, per-shard epoch
   floors, and `stream_state = OPEN` in one manifest CAS, including the
   table-head row when RFC-024 is active;
9. activate the bound shard claims idempotently, then delete the sidecar
   best-effort. Ack paths run the recovery barrier and refuse a still-covered
   enrollment, so `OPEN` plus an unresolved sidecar cannot acknowledge.

Recovery rolls forward only after classifying both exact effects. Rollback
reclaims the exact empty owned shard objects first, then restores/removes the
owned index transaction; a foreign object, a nonempty shard, or an effect buried
by another transaction fails closed. It never infers ownership from the
preserved logical pair, a table name/path, or a compatible-looking index.
Repeating enrollment with the same exact binding is a no-op. A different PK,
physical binding, sharding spec, maintained-index set, or writer-default
configuration is a typed conflict.

No row is acknowledged until enrollment is manifest-committed, every sidecar
effect is resolved, and the exact bound shard is physically admitting writes.

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

Admission is keyed by the exact
`(stable_table_id, incarnation_id, enrollment_id, shard_id)` binding. Before
claiming a writer, the append path captures `OPEN`, the complete physical
binding, and that shard's epoch floor. After `mem_wal_writer` claims an epoch,
it re-reads the lifecycle row and physical shard status immediately before the
first `put`: the binding must still be identical, state must still be `OPEN`,
the shard must be admitting, and the claimed epoch must exceed the recorded
floor for that same shard. A mismatch closes the writer and returns a typed
retry without appending.

That check is paired with §8's substrate admission seal. After publishing
`DRAINING`, the drainer atomically prevents later claims and advances the epoch
of each existing shard. A writer that claimed before the seal either put first
and is included in the in-flight durability drain, or is fenced before its put;
a claimant after the seal is refused by the substrate. A check-then-put against
manifest state alone is insufficient. The public claim-seal/reopen primitive is
therefore part of §3's ship gate, not an optional optimization.

Initial topology has one active ingest owner for each `(graph, table, main)`
shard. MemWAL's epoch fence makes restart/failover safe; it is not a load
balancer. A deployment with multiple server replicas must route a shard to its
current owner (or return a typed retry/redirect) instead of letting replicas
reclaim the epoch per request. General multi-owner routing waits for the
multi-shard phase and its ownership protocol.

## 6. Fold protocol

The fold consumes flushed generations in ascending order. Embeddings and
base-dependent validation run outside the table queue. With or without
RFC-024, the `ReadSet` carries schema identity, the complete stream
binding/configuration/generation cut, every probed table, and the conservative
branch authority token `(native branch incarnation, optional graph_head)`.
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
6. publishes all data/internal table versions and lineage in one `__manifest`
   CAS, including table heads when RFC-024 is active;
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
`epoch_floor_by_shard: Map<shard_id, u64>`, and the §3 physical binding. Epochs
are comparable only within the same enrollment and shard ID; a fresh shard in a
new binding may start at 1 and is fenced from its predecessor by enrollment and
shard identity, not by a larger number. The row is the logical lifecycle
authority and is updated by an RFC-022 CAS; MemWAL shard epochs and admission
status are the physical writer fence. Neither an in-memory registry nor an
empty-generation observation can substitute for both.
Lifecycle-only transitions are audited manifest metadata transactions; they do
not create graph-content commits or move `graph_head`.

The shared drain sequence is:

1. publish stream intent `OPEN -> DRAINING` for the exact binding with a target
   epoch floor for every current shard;
2. through the public substrate primitive required by §3, seal admission for
   each shard and advance its epoch to at least that target, fencing stale
   writers and refusing later claims across processes;
3. reject or backpressure new appends; §5's post-claim check rejects a claimant
   that entered between steps 1 and 2, while the physical seal closes the final
   check-to-put race;
4. wait for in-flight durability waiters, flush active MemTables, and fold every
   generation to empty;
5. verify shard manifests and base `merged_generations` agree on emptiness;
6. publish `DRAINING -> SEALED` with the verified generation cut and exact
   achieved per-shard epoch map;
7. for an operation-scoped drain, perform the guarded operation; persistent
   public quiesce stops after step 6.

`OPEN -> DRAINING` and `DRAINING -> SEALED` are RFC-022 authority-first
metadata writes; each fold is a separate normal RFC-022 graph write.
`DRAINING` fully encodes every target per-shard floor, so interrupted physical
seals are idempotently resumed from that row. A `SEALED -> OPEN` transition is
different: an exact `stream_resume` sidecar covers the physical reopen and is
resolved before any ack path proceeds, so no naked admitting effect precedes
the CAS. The drain itself is not one giant sidecar spanning multiple commits.

There are two dispositions after the drain reaches `SEALED`:

- **operation-scoped drain** — branch/schema maintenance automatically publishes
  `SEALED -> OPEN` only after the guarded operation succeeds, the stream
  contract remains compatible, and the §3 physical binding still names the
  exact table/ref. Under an exact resume sidecar, reopening the same binding
  advances each same-shard epoch above its recorded floor before the CAS; a
  pre-CAS failure reseals those exact shards. If the operation rematerialized
  the table or changed/recreated its native ref incarnation, it must complete
  `stream_rebind` instead of applying this transition directly;
- **persistent quiesce** — the public `quiesce` command leaves the stream
  `SEALED`. It never auto-reopens. `stream resume` explicitly revalidates schema,
  PK, configuration, MemWAL format, physical binding, and every same-shard
  epoch, then publishes `OPEN`. Stream teardown deletes intent only from
  `SEALED`.

The barrier never holds the table write queue while waiting for a fold that
needs that queue. State transition and epoch fencing happen first; fold commit
then acquires the normal queue. Crash recovery resumes from the durable state
and per-shard epoch map.

Schema apply must drain every affected enrolled type before changing fields,
constraints, PK, embeddings, or `@stream` and resumes only when compatible. A
rematerializing type rename preserves RFC-028's logical pair but not the old
physical enrollment:

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
counters. A rebind never reuses an old shard UUID; beta.21 surface guards pin
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
- each selected table's exact lifecycle state and `StreamPhysicalBinding`;
- each selected shard-manifest version and writer epoch;
- included flushed-generation paths and maximum generation;
- the active same-process MemTable row-position watermark, when available;
- the base table's `merged_generations` and index-catchup state read from the
  exact table version selected by the manifest snapshot, never from live HEAD.

Capture uses a retrying handshake:

1. read each selected lifecycle row and require `OPEN`; capture its exact
   physical binding, then read only that binding's shard manifests/epochs,
   acquire Lance generation retention guards for the flushed files in the
   tentative cut, and under one same-process writer snapshot capture/pin any
   active-MemTable watermark;
2. pin the graph manifest snapshot and require it to select the same lifecycle
   binding and physical base table/ref captured in step 1; read
   `merged_generations` from each exact base-table version it selects. A
   `DRAINING`, `SEALED`, or different enrollment restarts the whole capture;
3. re-read the lifecycle rows, complete physical bindings, shard manifest
   versions, and per-shard epochs. Any enrollment, ref-incarnation,
   configuration, state, shard-set, manifest-version, or epoch change restarts
   the whole capture;
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

The capability stamp is not assigned merely because beta.21 MemWAL APIs are
present. Initialization can emit a stream-capable first state only after §3's
public exact-enrollment and admission-seal gate passes; otherwise both new-root
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

Enrollment, fold, and recovery retain RFC-022's single-writer-process support
boundary. MemWAL's shard epoch permits an explicitly routed acknowledgement
owner; it does not turn OmniGraph sidecar recovery into distributed fencing or
authorize general replica failover.

## 12. Acceptance gates

- Surface guards pin claim, append, durability waiter, flush, per-shard epoch
  fencing, staged merge with `merged_generations`, and index catchup. They also
  pin beta.21's blocking fact that initialization commits internally and shard
  creation is separate. Acceptance requires the public exact enrollment
  receipt/staging plus admission seal/reopen surface from §3; until then this
  gate remains red.
- A WAL append failure emits no durable acknowledgement. Every acknowledged row
  survives crash, replay, fold, and recovery.
- Failpoints cover enrollment's index/shard multi-effect gap and every fold participant
  around sidecar, `commit_staged`, reject persistence, and manifest publish.
- A key conflict on fold participant N after participant 1 committed retains
  the exact sidecar, returns `RecoveryRequired`, and starts no replan until the
  recovery barrier resolves it; the no-effect case finalizes the empty intent
  before its bounded full replan.
- Enrollment restarts without an inline effect when schema, PK, table head, or
  stream configuration changes between prepare and gated revalidation.
- Local and S3/RustFS same-path/ref delete-recreate races change the physical-ref
  incarnation and make enrollment, ack, fold, recovery, and fresh capture refuse
  the replacement. A backend without a proven token refuses activation.
- A rematerializing type rename preserves the RFC-028 logical pair, leaves the
  target `SEALED`, and cannot acknowledge until an exact sidecar-covered rebind
  publishes a fresh enrollment/shard namespace. Crash tests cover every old-
  drain, SchemaApply, rebind-effect, and rebind-publish boundary; recovery never
  adopts old shards by name/path or reuses their UUIDs.
- Two folders converge exactly once; a fenced stale writer can never produce a
  false durable acknowledgement after WAL GC.
- Without RFC-024, a concurrent graph commit that changes a probed-but-untouched
  table after fold validation changes the captured branch token, forces a full
  fold revalidation, and cannot be accepted by publisher reparenting.
- Two server replicas do not epoch-ping-pong one shard; owner failover fences
  the old process before the new owner acknowledges.
- Quiescence tests race appends with branch and schema operations across two
  coordinators and prove no post-drain tail appears; failpoints cover every
  lifecycle-row, per-shard epoch-fence, admission-seal, fold, and `SEALED`
  boundary. A claimant paused before its final lifecycle check and one paused
  after that check are both either included before the drain cut or fenced
  before `put`.
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
| A | graph-format capability/refusal gate, strict rebuild path, MemWAL adapter, surface guards, enrollment sidecar | public exact-enrollment plus admission-seal API gate; genuine cross-version/refusal and rebuild suite; multi-effect crash matrix |
| B | new ingest route/CLI, durable ack, strict fold | ack durability; API compatibility; cost budget |
| C | atomic dead letter, audit provenance, status/bounds | reject crash matrix; backpressure tests |
| D | epoch-fenced drain, persistent quiesce/resume, schema/branch/upgrade integration, rematerialization rebind | two-coordinator race, old/new physical-binding crash matrix, and format-transition suite |
| E | fresh cuts and maintained-index reads; cross-process `Fresh` ships only if the substrate generation-retention guard exists (§9), otherwise same-process only | cut consistency; merged-generation exclusion |
| F | multi-shard upsert and stream deletes | one-key-one-shard proof; Lance re-audit |
