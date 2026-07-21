# WAL Thinking

Working notes, updated 2026-07-21. Plain-language grounding for the WAL/streaming
discussion ([RFC-018](docs/rfcs/0018-ingest-wal.md) →
[RFC-026](docs/rfcs/0026-memwal-streaming-ingest.md)). Three parts: the
contract difference between an interactive graph commit and durable stream
admission, the expected performance shape and evidence still required, and the
build inventory.

Current boundary: RFC-026 Phase A and the Phase-B1 private core are built, but
public streaming is not. Gate R0 found and the follow-up fixed the one known
all-shape closure failure: a legal high-entropy near-cap generation is durably
acknowledged, materialized, folded, and published without lowering the
8,192-row/32-MiB admission cap. The fold now charges logical Arrow slices and
densifies selected rows before retaining them. The measured full-fold RSS delta
was 284,934,144 bytes (about 272 MiB); CI carries a 384-MiB remeasurement
tripwire for one exclusive fold, not a runtime allocator limit. Current
internal schema v8 preserves
Phase A's historical v7 foundation: exact empty main-only MemWAL enrollment recovery,
identity-keyed lifecycle authority, exclusion of ordinary local table/schema/
maintenance/repair/recovery effects for a lifecycle-bound table, and
partial-format refusal. Native branch controls alone may proceed at `SEALED`,
because they do not move table HEAD.

V8 adds stream-config v2 and recovery-v11 `StreamFold`. One feature-gated,
doc-hidden engine seam can privately admit an already-normalized physical
batch, acknowledge only after its Lance durability watcher and the same
writer's post-durability epoch check both succeed, route replay or one
flushed-unmerged generation fold-only, and publish one exact fold at the
`__manifest` CAS. It prevents MemTable rollover and retires the writer before a
successor generation can put. This is implementation/evidence machinery, not a
product surface. There is still no `@stream`, public enrollment or row
admission, SDK/HTTP/CLI/OpenAPI route, operator drain/resume workflow, or fresh
read. RFC-026 remains Draft.

RFC-026 now selects **unbounded retain-all on stock Lance** as the first storage
profile. OmniGraph deletes no `_mem_wal` object and advertises no file-count or
retained-byte limit. RC.1's missing durable materialization-attempt receipt and
complete physical-output envelope therefore remain useful facts, but no longer
block activation: the selected contract deliberately accepts monotonic storage
and loud provider-capacity exhaustion. Managed reclamation and the Lance patch
are deferred optimizations. Common compare-and-chain token, trusted
attribution, persistent lifecycle/correction, authorization, and product-
parity contracts remain specified and inactive.

---

## 1. Interactive graph commit vs WAL admission + later fold

The deepest way to see it is at the acknowledgement boundary:

- an **interactive write** acknowledges only after validation, durable recovery
  ownership, all affected Lance dataset commits, and one `__manifest`
  publication;
- a **stream write** acknowledges earlier, after checks that need no graph read
  and after its WAL data is durable;
- state-dependent checks and graph visibility happen later, in a **fold**.

These steps do not physically happen in one instant. The contract is that an
interactive caller receives success only after all of them complete, while a
stream caller explicitly chooses the earlier durability-only acknowledgement.

### Interactive write — graph-visible before acknowledgement

One `mutate` or `load` request may already contain many rows, statements, and
affected tables:

```text
one interactive request:

  1. resolve recovery and capture one coherent graph view
  2. validate and stage every affected table
  3. revalidate authority and arm one recovery sidecar
  4. commit one exact Lance transaction per affected dataset
  5. confirm the physical effects
  6. publish every achieved dataset version in one __manifest CAS
  7. finalize required local views and acknowledge the committed success;
     remove satisfied recovery residue best-effort

  after acknowledgement:
  ✓ physical effects are durable
  ✓ subsequent committed reads can select the new graph state
  ✓ validation and conflict checks completed
  ✓ one graph time-travel point exists
```

A query that captured the previous snapshot before publication continues to see
that old snapshot. Atomic publication means new committed reads never see only
some of the affected tables; it does not mutate an already-running query's
snapshot. A sidecar-cleanup failure after publication does not reverse success
or become a user error; a later recovery barrier can prove and finalize it.

The cost concern is real but currently qualitative: every successful
interactive request pays the recovery, per-dataset commit, and graph
publication protocol. Graph commits also pass through the serial
`graph_head`/`__manifest` authority point. Current throughput and critical-path
round trips must be measured on the current Lance pin before attaching a number
to that floor.

### Stream admission — durable before graph-visible

The crate-private core is always compiled, but this contract is executable only
through the feature-gated Phase-B1 engine seam. It is evidence in the current
binary, not a production or public caller path.

One admitted call—possibly containing several rows—follows a different
contract:

```text
one submitted stream batch:

  1. validate the already-normalized physical shape, types, required/default
     fields, enum/range/check rules, reserved fields, stream mode, and exact
     post-tombstone size preflight
  2. resolve recovery/authority and route it to the active owner of the table's
     MemWAL shard
  3. recompute and reserve the exact stored representation, inject required
     internal `_tombstone=false`, and submit exactly one bounded Lance WAL
     batch/put
  4. wait for that put's durability watcher and acknowledge the caller

  after acknowledgement:
  ✓ the submitted WAL data is durable and replayable
  ✓ checks that require no graph read have completed
  ✗ default graph reads do not see it yet
  ✗ graph-dependent checks have not completed
  ✗ no graph commit/time-travel point exists yet
```

Resource accounting is intentionally two-pass. Cheap raw bounds reject an
obviously over-cap batch, and raw-fit input receives exact post-tombstone
validation before recovery I/O. After the recovery/authority prelude, that
exact charge is recomputed and reserved before the caller can wait in the
same-key queue, acquire shared admission, detach, or cold-claim a writer.

The acknowledgement says only that the submitted batch crossed its Lance
durability watcher and the same writer then passed a post-durability epoch
check. Both are required acknowledgement authority. The watcher returns
success/failure, not a durable WAL row
coordinate. Lance's returned batch positions are active-MemTable positions
that reset after rollover, and its next-WAL-position statistic is a mutable
hint, so neither may appear as a receipt. If invocation may have had an effect
but the watcher outcome is lost, OmniGraph returns typed `AckUnknown`; replay
preserves possible durable residue but cannot attribute it to that caller
attempt. The attempt remains ambiguous and OmniGraph must never claim “not
durable.” Cancellation cannot abandon the worker once invocation starts.
The current private B1 seam has only cardinality-level same-key retry behavior:
ambiguous `X(id)`, then durable `Y(id)`, then retry `X(id)` can make stale `X`
newest again. The common B2 contract inventory closes that public-contract hole with an explicit
compare-and-chain token. A caller-stable `write_id` plus the opaque
`predecessor_token` returned by the server derives one stable successor token;
the predecessor must equal the complete current token before Lance is called.
An exact current retry is `already_durable`; a retry behind newer `Y` is
`StreamSequenceConflict`. Replay/LWW realizes the already-admitted chain but is
not itself the sequencing authority.

The warm, uncontended WAL persistence path is normally one conditional object
store PUT per submitted Lance batch. It is not an unconditional one-call
guarantee: first open, WAL-position discovery, replay, fencing, retries, and
multipart behavior can add operations.

MemWAL has exactly one active writer epoch per shard. Many producers may share
one routed, warm shard owner, but independent processes do not append
uncontended to the same shard: a new claimant fences its predecessor. Initial
RFC-026 delivery therefore uses one unsharded owner per `(table, main)`;
horizontal writer scaling comes later from deterministic key-based sharding.

### Fold — deferred graph publication, paid once per batch

The strict one-generation fold is implemented privately in B1. Phase A
established the binding, recovery, and exclusion preconditions it consumes;
B2's common contract inventory specifies the token participant and persistent operator lifecycle that
Phase B2 must implement before exposing the public fold.

B1 deliberately chooses a smaller contract: one writer owns exactly one
generation whose total admitted input is at most 8,192 rows and 32 MiB. It
prevents automatic rollover, explicitly seals/drains that generation, retires
the writer, and folds it in one sealed keyed transaction before reopening at a
higher epoch. On reopen, an empty active state may admit; replayed active rows
or one already-flushed unmerged generation are fold-only. Pinned RC.1 does not
mark replayed BatchStore entries WAL-flushed, so the exclusive fold path first
marks the exact authoritatively replayed prefix durable through the public
BatchStore watermark before sealing; otherwise replay/reseal can multiply WAL
entries across crashes. Broader trigger policy may later use rows, bytes,
maximum lag, resource pressure, or an operator request. The full target fold is:

```text
a fold:

  capture one exact stream binding and generation cut
    → run graph-dependent checks
       (referential integrity, cardinality, cross-version uniqueness, and
        other base-dependent validation)
      B1 consumes already-normalized physical rows; supplied physical vector
      columns pass through unchanged, with no external embedding call or
      unspecified fold-derived-field materialization
    → B1/B2 strict mode: stop and mark the shard blocked on a permanent failure
      Phase-C dead_letter mode: stage a typed reject in _ingest_rejects
    → stage accepted rows with Lance merge-insert and merged_generations
    → Phase C only: stage reject participants required by the disposition
    → arm one RFC-022 recovery sidecar
    → commit every affected Lance dataset
    → classify every achieved participant and durably record EffectsConfirmed
    → publish all achieved versions in one __manifest CAS
    → accepted rows and configured reject/audit state become visible together
    → remove the satisfied sidecar best-effort
```

The fold is a new stream-specific RFC-022 effect adapter on the existing
publication and recovery chassis. It reuses that chassis, but it adds stream
binding and generation authority, fold-time validation, merge progress, and
reject/audit atomicity.

The selected retain-all profile never deletes folded MemWAL data. WAL entries,
flushed generations, random failed-attempt subtrees, and fence sentinels remain
in the root even after the exact fold is graph-visible and recovery is settled.
That is deliberate: generic table-version cleanup leaves `_mem_wal` untouched,
and deleting the successor's empty WAL fence sentinel can let a stale writer
report a later WAL PUT as durable because RC.1 lacks a post-success epoch
check. OmniGraph therefore never deletes raw MemWAL paths.

A Lance-owned inspect/plan/execute protocol can add managed reclamation later.
Its attempt receipts, inventory, fencing, and accounting are no longer the next
activation slice because the first profile promises no retained-storage bound.

### Side by side

| | Interactive request | Stream row + later fold |
|---|---|---|
| Acknowledgement means | The graph commit is durable and published | The stream row is durably accepted for ordered processing |
| Graph visibility | Before success is returned | After a successful fold; lag depends on triggers, validation, bounds, and fold health |
| Pre-ack checks | Complete request validation | Checks that need no graph/base-table read |
| Deferred outcome | None after success | Strict fold may block; configured dead-letter may record a reject |
| Same-key behavior | Serialize, retry, or fail loudly | Public B2 compare-and-chain admission; Lance LWW only realizes the accepted chain on one shard |
| Versions | One graph commit per successful request, which may contain many rows | One graph commit per successful fold, plus one Lance version per affected participant |
| Crash story | RFC-022 recovery resolves any table-effect/publication gap | WAL replay preserves durable input; RFC-022 recovery still resolves fold effects/publication |
| Best fit | Edits requiring immediate graph visibility and synchronous rejection | High-rate feeds that accept delayed visibility and deferred state-dependent disposition |

### The trade-offs we deliberately accept

1. **Visibility lag.** “Durable” no longer means “present in default graph
   reads.” Visibility follows a successful fold and is not promised within an
   unmeasured number of seconds.
2. **Deferred graph-dependent disposition.** An acknowledged row may later
   block a strict fold or be atomically dead-lettered when that mode is
   configured. The acknowledgement cannot be withdrawn.
3. **More lifecycle machinery.** Enrollment, owner routing, backpressure,
   drain/seal/resume, replay, fold health, and cleanup become operator-visible
   responsibilities.
4. **Write and monotonic storage amplification.** Data may exist in a WAL
   entry, a flushed Lance generation, the base dataset, and maintained indexes.
   Under retain-all, those MemWAL copies are not reclaimed in place.

One line for the team:

> A graph commit means “the graph changed.” A WAL acknowledgement means “the
> system durably accepted this row for ordered processing.” A successful fold
> gives that row its contract-defined graph-visible or reject disposition.

---

## 2. Expected performance shape — and what is not measured yet

The expected win is **amortization**, not a proven request-count multiplier:

- the interactive path pays the graph publication protocol per successful
  request;
- the streaming acknowledgement path pays WAL durability per submitted Lance
  batch;
- a fold pays graph publication once for a generation batch.

That can materially improve acknowledgement latency and sustained admission
throughput when many logical rows share WAL and fold work. The magnitude is not
yet measured.

### RC.1 does not provide automatic 100 ms group commit

Lance RC.1 defaults to durable writes. In durable mode, each `ShardWriter::put`
triggers an immediate WAL flush and waits for it. The default 100 ms interval
is part of the same writer machinery and bounds buffered/non-durable
accumulation; it does not delay a durable put to form a guaranteed group-commit
window.

Therefore, predictable group commit requires an explicit OmniGraph admission
design that combines multiple logical rows into one submitted Lance `put` (or
another measured coalescing strategy) while preserving:

- ordered per-row acknowledgements;
- per-actor admission accounting and bounds;
- cancellation and ambiguous-response semantics;
- bounded latency for sparse traffic;
- typed propagation of flush and fencing failures.

Without that adapter, 100 sequential durable Lance puts normally target 100
separate WAL entries, not ten shared 100 ms flushes; retries may add PUT
attempts.

### What request volume actually depends on

The useful comparison is:

```text
interactive work = interactive requests × graph-publication work per request

stream admission = submitted WAL batches × WAL durability work per batch

fold work        = productive folds × work for their rows, bytes, generations,
                   indexes, validation, and dataset participants
```

The number of submitted WAL batches depends on the batching policy, row bytes,
latency bound, shard count, backpressure, retries, and multipart behavior.
Generation flushes and folds also scale with data volume and index work.
MemWAL changes the unit of amortization; it does not make storage operations
independent of write volume.

### What current evidence says

The checked-in current-path instruments establish bounded components, not a
complete end-to-end call count:

- a focused shallow single-insert run on 2026-07-17 observed 5 tracked
  data-table reads and 18 tracked `__manifest` reads;
- the schema/recovery adapter reports its text reads, existence checks,
  sidecar writes, and deletion separately;
- the bounded data-write test observed one data-table write;
- those counters overlap and do not identify sequential critical-path hops, so
  they cannot be added into a truthful “12 calls per write” result.

See [`write_cost.rs`](crates/omnigraph/tests/write_cost.rs) and
[`memwal_stream_cost.rs`](crates/omnigraph/tests/memwal_stream_cost.rs). B1 has
accepted component evidence for its measured fixture set: the current
post-containment warm already-claimed acknowledgement result is local, while
the configured-RustFS figures are a 2026-07-19 pre-containment baseline that
must be rerun before a current object-store claim. Cold replay, retained
generation metadata, selected-generation scan, widest-generation RSS, and the
known non-flat uncompacted-manifest fold term are reported separately. There is
still no matched end-to-end comparison of one-row interactive latency,
sustained branch throughput, and a public streaming workload. RFC-018's
historical single-digit language and upstream prototype results are not
measurements of that product comparison.

Consequently, the following are hypotheses rather than results:

- a specific current calls-per-write number;
- a one-PUT-per-100-ms streaming rate;
- a single-digit current branch-throughput ceiling;
- 8×, 80×, or 500× request-count reductions;
- a fixed two-times byte multiplier;
- any claim that WAL request volume stops scaling with input.

### Required benchmark before quoting a multiplier

The private B1 admission component instrument has current local evidence and a
historical pre-containment configured-RustFS baseline. It does not supply the
missing matched product comparison or a current object-store result.
Before quoting a multiplier, build and accept a separate end-to-end comparison:

1. fix schema, row shape, row bytes, table count, and index configuration;
2. compare interactive one-row requests, existing `load` batching, and stream
   admission at 1, 100, and 1,000 logical rows/sec, plus a sparse one-row/minute
   case;
3. declare batch size and maximum batch delay rather than relying on defaults;
4. record achieved throughput, backpressure, p50/p95/p99 acknowledgement
   latency, and fold visibility lag;
5. count GET/HEAD/LIST/PUT/DELETE and multipart operations separately for the
   acknowledgement and background paths;
6. record serial critical-path hops, uploaded bytes, monotonic retained bytes,
   fold rows/bytes/generations, and every affected dataset;
7. run multiple fresh trials and preserve the raw records.

The comparison must state the semantic difference: interactive success is
graph-visible, whereas stream success is durability-only.

The honest summary today is:

> MemWAL can move durable acknowledgement off the graph-publication path and
> amortize graph-visible publication over folds. RC.1 flushes durable data per
> submitted put, so predictable group commit is an OmniGraph integration
> responsibility. The improvement factor remains to be measured.

---

## 3. Inventory: Lance substrate vs OmniGraph integration

### Lance primitives we consume or plan to consume

These are real substrate capabilities, not a turnkey graph-streaming product:

| Lance primitive | What it gives us — and the boundary |
|---|---|
| **WAL entries on object storage** | Sequenced Arrow IPC entries with bit-reversed names. A warm uncontended append is normally one conditional PUT; open, recovery, retries, and fencing add work. |
| **Durability results/watchers** | The acknowledgement primitive only inside one active MemTable: RC.1 resets batch positions after rollover but retains one writer-wide watermark. B1 prevents rollover and retires the writer after one generation. Predictable multi-request group commit still requires an OmniGraph batching policy. |
| **Epoch-fenced shard writers and fence sentinels** | One active owner per shard and stale-writer detection. This is per-shard fencing, not OmniGraph's graph-wide distributed recovery fence. |
| **MemTables and flushed generations** | Recent rows live in WAL-backed memory and later in small Lance datasets. Maintained FTS/vector/scalar indexes exist only when explicitly configured and supported. |
| **`merged_generations`** | Merge progress updated atomically with a base-table merge. It supplies the marker needed for idempotent per-table folding; RFC-022 still owns graph publication and partial-effect recovery. |
| **WAL replay** | Durable entries are replayed to reconstruct the MemTable under the next valid claimant. The guarantee depends on respecting fencing, lifecycle, compatibility, and safe GC. |
| **LSM merging reads** | A scanner over the base table, selected flushed generations not yet safely replaceable by the base read plan, and optionally same-process active/frozen MemTables. OmniGraph must build and retain a coherent graph-level fresh-read cut. It does not query raw WAL files as a normal read source. |
| **Retention behavior** | Upstream describes when generations may be obsolete and warns that deleting WAL files can weaken fencing. The selected profile does not turn eligibility into deletion: generic cleanup ignores `_mem_wal`, OmniGraph never deletes raw paths, and retained storage grows monotonically. A later managed profile may consume a Lance-owned opaque reclamation primitive. |
| **Staged merge-insert** | `execute_uncommitted` plus atomic `merged_generations` fits the RFC-022 staged shape. There is no one-call MemWAL fold API. |
| **Shared sessions/store parameters in RC.1** | Writer-created generations and base-backed scanner paths reuse the base dataset's access context; fresh-only construction still receives that context from its caller. OmniGraph owns long-lived writer/session lifecycle and scanner integration. |
| **Key-based sharding** *(later)* | Horizontal scale when every occurrence of one key deterministically maps to one shard. |

OmniGraph already has reusable chassis: the manifest publisher, generic
recovery-sidecar framework, graph lineage, stable table identity, keyed write
adapter, and validation components. “Reusable” does not mean “unchanged”:
Phase A added stream lifecycle authority and enrollment recovery. Private B1
now adds one nominally bounded admission worker, watcher success plus the same
writer's post-durability epoch check, replay and flushed-generation
classification, and one recovery-owned fold. The all-shape closure repair is
implemented. Later phases still need the common token/attribution, persistent
operator lifecycle/correction, authorization, and product contracts; then the
public caller, reject participants, and fresh cuts. Managed cleanup is optional
future work rather than an activation prerequisite.

### What Phase A added

Phase A turns the enrollment classifier into a recoverable format foundation:

- internal schema v7 recognizes identity-keyed lifecycle rows and refuses v6;
- one schema-v10 intent binds exact main authority, the `N -> N + 1`
  initializer effect, pre-minted enrollment/shard IDs, fixed lineage, and the
  intended physical configuration;
- no effect retires the intent, index-only provisions the exact empty shard,
  and index-plus-empty-shard publishes the table pointer and `OPEN` row;
- once either effect exists, recovery only rolls forward. It never restores a
  table or deletes/reclaims MemWAL artifacts based on inference;
- a process-local admission lease sits outside the ordinary schema → branch →
  table gates. Any lifecycle row, including `SEALED`, fences current
  base-table/schema/maintenance/repair-adoption/recovery effects because Phase
  A cannot advance or rebind its witness. Native branch controls alone may
  proceed at `SEALED` because they do not move table HEAD;
- enrollment refuses if a named graph branch exists, and branch controls
  refuse while a lifecycle is `OPEN` or `DRAINING`; and
- compatible open validates the exact lifecycle/witness/empty-shard state and
  refuses an uncovered MemWAL index or other partial-format mismatch.

This deliberately stops before the first data entry. `DRAINING` and `SEALED`
are representable so the authority shape is fixed, but no drain or resume
workflow is implemented.

### What OmniGraph has built and must still build

| Responsibility | Required contract |
|---|---|
| **Format capability and refusal** | **Phase A historical foundation:** v7 stamp, strict v6↔v7 refusal, export/init/load rebuild, exact lifecycle validation, and uncovered-partial-format refusal. **Private B1 current format:** schema v8, stream-config v2, and recovery-v11 `StreamFold`; genuine v7↔v8 old/new-binary refusal and rebuild evidence remains green, and the widest legal fold now closes. **Specified common B2 format:** schema v9, config-v3, state-v2, and recovery-v12 add hidden token/attribution/lifecycle state; they remain inactive until their genuine v8↔v9 refusal/rebuild evidence passes. Retain-all requires no storage-budget format. |
| **`@stream` intent and enrollment** | **Foundation only:** Phase A binds stable table/incarnation identity, location/main ref, never-reused enrollment ID, one empty shard, fixed configuration, and the mutable current-HEAD witness under recovery. B1 is implemented but remains private. B2 makes `@stream` declaration leave the type `UNENROLLED`; an explicit request-idempotent enroll operation creates the logical stream incarnation and physical binding. Rebind remains later. |
| **Public surface** | **Phase B2, after private evidence:** explicit enroll, `POST /graphs/{graph_id}/streams/{type_name}/ingest`, minimum status/block-inspection/fold/quiesce/resume/abort-drain/correct/rebuild-preflight controls, `omnigraph stream …` commands, Cedar, and OpenAPI parity. Every mutating management call after enrollment compares a lifecycle revision and durably returns its bounded terminal receipt. Existing `/ingest` remains the deprecated load alias. |
| **Writer registry and routing** | **Phase B1 implemented privately:** one root-scoped, cross-handle registry owns the full binding and reuses the common table-identity plus resolved-physical-ref admission key; one serialized owner serves the initial `(table, main)` profile. One no-rollover generation has an 8,192-row / 32-MiB admission cap. Puts use exact charge → shared admission → same-key input queue → worker-mode inspection. Claim/replay starts under the shared admission lease. Empty reopen may admit; non-empty replay and one flushed-unmerged generation are fold-only, with exact accounting and the refusal marker installed before the opener releases its queue. Already-charged callers can overlap recovered replay transiently; the ledger records that overlap while refusing new charge. The exclusive fold validates replayed rows and uses the pinned public BatchStore watermark bridge before reseal. A cold fold reserves the full generation/resident/pending envelope before its owned opener and retains exclusive authority across the original seal deadline. Retirement stops puts before public `abort`; `close` is not durability evidence. Fold scanning now charges logical slices and densifies selected rows, so the near-cap closure cell succeeds. These are not public defaults. |
| **Durability batching** | **Phase B1 implemented privately:** one admitted call is one non-empty, already-normalized physical `RecordBatch` and one Lance put. The worker owns the final check, invocation, watcher, and post-durability epoch check; only watcher success followed by same-writer `check_fenced()` success acknowledges. Anything ambiguous after invocation is typed `AckUnknown`; replay preserves possible residue but never resolves that attempt. Automatic rollover is disabled and the writer retires before a successor-generation put. There is no hidden group-commit policy until an instrument justifies one. |
| **Pre-ack validation** | **Phase B1 implemented privately:** apply every rule that needs no graph/base-table read before WAL persistence. The private seam consumes physical vectors supplied in the normalized batch; it neither calls an external embedding provider nor invents unspecified fold-derived fields. |
| **Fold adapter** | **Phase B1 implemented privately and closure-green:** capture one exact stream binding and post-drain shard snapshot, independently prove empty frozen refs plus the exact authoritative generation/cursor (RC.1's drain waiter alone is insufficient), run base-dependent validation, require the LWW output to fit 8,192 rows / 32 MiB, stage one merge-insert with `merged_generations`, and publish the exact table/lifecycle/lineage outcome through recovery schema v11 plus one `__manifest` CAS. Scanner slices are charged by logical size and rebuilt densely before retention. Already-normalized physical vectors pass through unchanged. Seal/drain/abort stay background-owned across caller deadlines; recognized unreferenced generation subtrees are retained forever under the selected profile. |
| **Strict and dead-letter disposition** | **B1/common B2:** strict only; a permanent deferred-validation failure leaves durable unmerged input and blocks progress loudly. B1 has no correction lane. The common B2 inventory specifies bounded `REPLACE`/`WITHDRAW` correction over the immutable blocked cut, without a second generation or silent drop. **Phase C:** dead letters only after restart-stable reject identity and retention are defined. |
| **Policy, lineage, and audit** | B1 records only fixed mechanism lineage and has no public caller. The common B2 inventory stores trusted contributor/write metadata with the row, publishes current token state with the base version, and commits a fixed winner summary; it promises durable attribution for visible winners/current withdrawals, not unbounded audit retention for every superseded acknowledgement. Phase C consumes that evidence for rejects. |
| **Quiescence and rebind** | The common B2 inventory specifies durable `OPEN -> DRAINING -> SEALED -> OPEN`, strictly monotonic lifecycle revisions, bounded complete terminal management receipts, authoritative status, roll-forward-only resume/abort-drain recovery, rebuild preflight, and bounded correction. Retain-all is selected, but these lifecycle contracts still must be implemented and proved before public activation. Resume rechecks the bounded no-named-branch topology; an incompatible branch operation leaves the stream `SEALED`. `SEALED` permits export/rebuild, not in-place maintenance. Phase D integrates automatic operation drain and physical rebind. |
| **Fresh reads** *(later)* | Explicit committed/fresh IR mode, exact base-plus-MemWAL cut, merged-generation exclusion, retention guards, and documented lack of cross-table atomicity. |
| **Retention profile** | **Unbounded retain-all is selected:** no OmniGraph MemWAL GC, no file-count or retained-byte admission limit, and monotonic storage for the root. Provider exhaustion is an accepted loud operational risk. RC.1's missing durable attempt receipt and complete output envelope remain documented but do not block a contract that promises neither limit. **Managed reclamation** retains the Lance-owned durable inspect/plan/execute design as an optional later optimization. OmniGraph never deletes raw `_mem_wal` paths. |
| **Graph-manifest lifetime** | Retain-all also places no finite-lifetime bound on base/token or shared `__manifest` history. `GraphHistoryBudget` is not part of the first profile. If a later product promises a hard whole-root bound, it must earn a separate RFC and physical-growth evidence across every manifest publisher. |
| **Evidence and operations** | **B1 closure green:** surface guards, genuine v7↔v8 refusal/rebuild, the graph-level behavior/crash/race cells, and qualified cost/PK-index/RSS evidence pass. The legal high-entropy near-cap cell now acknowledges, materializes, folds, publishes exactly once, and retains every listed path. The full-fold RSS delta measured 284,934,144 bytes (~272 MiB); the 384-MiB check is a CI remeasurement tripwire for one exclusive fold, not a runtime allocator limit. The revision-pinned source audit and current-object sweeps remain useful descriptions of monotonic storage, not a rejected-gate disposition. Two guards continue to prove why generic cleanup and raw fence deletion are unsafe. Public metrics/status, API tests, CLI parity, and operator evidence remain absent by design. |

The table intentionally omits “small/medium” estimates. Atomic rejection,
quiescence, fresh-read retention, and any future GC are correctness protocols;
size them only after their implementation spikes expose the real work.

### The RC.1 enrollment gap, stated precisely

What RC.1 exposes:

```text
initialize MemWAL index
  → commits CreateIndex internally
  → mutates the Dataset handle
  → returns Result<()>

open the selected ShardWriter
  → separately creates/updates the shard manifest
  → claims an epoch with atomic, epoch-fenced writes
  → returns a writer that exposes the claimed epoch
```

Shard claiming is protected; it is not an “unprotected step that returns
nothing.” The missing upstream shape is one externally recoverable contract
covering both:

- ownership/classification of the MemWAL index-enrollment effect; and
- provisioning, admission sealing, reopening, and reclaiming the intended shard
  objects.

An upstream exact receipt and public admission lifecycle would simplify this
substantially. We may propose that change, but its review and release timing
is not a prerequisite for retain-all. Gate E0 passed; the
upstream shape still gates overlapping-process topology. Bounded Phase A and
the private B1 core are implemented, and the Gate R0 closure gap is repaired.
The common product and operational gates remain before public row activation.

### The RC.1 reclamation gap, stated precisely (B2b)

RC.1 exposes evidence-level raw listing plus manifest reads, but not a complete
Lance-owned classified inventory. Its generic
`cleanup_old_versions` walks ordinary Lance versions and leaves `_mem_wal`
objects untouched. Its public MemWAL surface has no safe delete/GC operation.
Raw object deletion is not an acceptable substitute: a checked-in adversarial
guard decodes and removes the successor's empty epoch-2 WAL fence sentinel and
demonstrates that the stale writer can still complete its WAL PUT and receive
watcher success even though an explicit check sees `PeerClaimedEpoch`. Deleting
the sentinel weakened the fence.

Managed reclamation remains a coherent future path: author a Lance-owned opaque
inspect/plan/execute contract with exact shard/base/history witnesses, durable
attempt/receipt recovery, whole-cut cursor proof, writer fencing before
deletion, conservative orphan classification, and backend-appropriate
inventory/accounting. That is deliberately deferred. The retain-all profile
does not need to prove those deletion or storage-bound contracts because it
performs no deletion and advertises no hard retained-storage limit.

The same applies to whole-root history. Base-table, token-table, and shared
manifest histories may grow. The first profile observes that growth but does
not pretend to cap it. A future `GraphHistoryBudget` or other finite-lifetime
promise is a separate format and evidence decision, not hidden prerequisite
work for retain-all.

### Gate E0: a no-wait decision, not activation

RFC-026 now has a production-neutral Gate E0. The question is narrower than
“can we ship streaming on RC.1?” It asks: “can public RC.1 state distinguish the
only effects our first enrollment is allowed to create after success, failure,
or a lost result without a history scan or ambiguous listing?” The answer is
yes for the bounded profile below.

The terminology matters. One value cannot serve two incompatible jobs:

- the **stable enrollment binding** is the logical table/incarnation,
  location/main ref, never-reused enrollment ID, pre-minted shard ID, and
  configuration hash. The enrollment ID/config version is also embedded in
  Lance's persisted namespaced writer defaults so lost-result classification
  does not depend on the replaceable MemWAL index UUID;
- the **current-HEAD witness** is the current `BranchIdentifier`, Lance version,
  transaction UUID, and manifest e_tag.

The witness changes on every ordinary Lance commit. It is not a stable
“physical-ref incarnation.” The bounded design makes that manageable by giving
an `OPEN` stream exclusive authority to advance its base-table HEAD. Only fold
or recovery may move it; Mutation/Load, BranchMerge, SchemaApply, Optimize,
EnsureIndices, repair, cleanup, and branch operations touching that table
currently refuse before effect (with the documented `SEALED` native-branch
exception); later operation adapters may drain first. Every allowed commit
publishes the next witness atomically with the table pointer and stream
lifecycle row.

We do not use a long-lived Lance tag as the default stable anchor. It would pin
the enrollment-time table snapshot—potentially an old full-table file set after
rewrite/compaction—and add another auxiliary effect to the enrollment crash
matrix. It remains a measured fallback only if a future profile truly needs
interactive base writers while a stream stays open.

Gate E0's evidence-backed support boundary is **main-only, one unsharded keyed
shard, and one live OmniGraph writer process for the graph**. A crash successor
is allowed only after external exclusivity. An overlapping second process and
raw Lance writers are unsupported, not silently “best effort.”

The E0 evidence suite proved:

1. exact baseline HEAD `N` with no MemWAL index;
2. initializer success yields exactly `N + 1`, whose transaction reads `N` and
   contains only the intended singleton MemWAL `CreateIndex`, exact unsharded
   configuration, and namespaced enrollment/config-version marker;
3. discarding the initializer result and reopening yields the same exact
   allowed-successor classification;
4. the public writer path creates/claims only the pre-minted shard, with the
   expected unsharded spec, observable epoch, no flushed generation, and no
   data-bearing WAL; any deterministic data-less fence artifact is enumerated;
5. the truth table accepts only no effect, the exact index-only successor, and
   that successor plus the exact empty shard. Wrong configuration, intervening
   HEAD, foreign shard, unexpected data, or ambiguity is `RecoveryRequired`;
6. once an allowed effect exists, the candidate is roll-forward-only. It never
   deletes, reclaims, or restores MemWAL state from inferred ownership; and
7. the current-HEAD witness is stable across unchanged reopen, changes after an
   ordinary commit, catches same-path/same-version recreation locally and on
   S3/RustFS, and can be classified with measured history-bounded work.

The first cost attempt was rejected: local `checkout_latest` may use filesystem
`read_dir` outside `IOTracker`. The accepted classifier instead opens exact `N`
through the manifest-pinned physical URI, probes only `N + 1` with the public
but guide-hidden `Dataset::has_successor_version`, then asks exact `N + 1`
whether a buried `N + 2` exists. `AttemptTracker` records attempts before
forwarding, including `NotFound` and errors. At baseline versions 8 and 80 it
records the same six-attempt shape—four successful manifest HEADs, one
`NotFound` manifest HEAD, one successful manifest GET—and zero list calls. A
Unix execute-only `_versions` tripwire proves the exact path works when latest
enumeration fails; an unreadable exact HEAD errors instead of becoming absence.

The local run has 14 substantive cells plus one explicit unconfigured-S3 skip.
The configured RustFS cell passes non-vacuously with the same six-attempt,
zero-list shape and covers the positive no-effect → lost-result index →
pre-minted empty-shard → unchanged-reopen sequence plus foreign shard,
malformed/loose root, durable WAL, persisted cursor, and corrupt-manifest
refusals. Separate surface guards own object-store ABA and pin the doc-hidden
successor, flush/drain, and merged-generation surfaces; CI rejects skipped E0
and ABA cells.

E0 itself added no `@stream`, lifecycle rows, a sidecar schema, public APIs,
WAL acknowledgements, or a format stamp. Phase A subsequently consumed that
proof in historical schema v7: the v10 enrollment intent, lifecycle authority,
writer exclusion, and refusal/rebuild. Private B1 now consumes that foundation
in current schema v8/config-v2 with private admission machinery and the
recovery-v11 `StreamFold` envelope.
This still is not stream release. The exact upstream receipt/seal remains the
preferred simplification and the broader-topology gate.

That Phase A work is not the Optimize proof copied to a new
writer kind. Optimize is content-preserving maintenance and grants no future
durability authority. Enrollment is the precondition for future
acknowledgements, so Phase A separately proves initialization, shard claim,
fixed binding publication, admission exclusion, restart, and foreign-state
refusal. Private B1 now implements first put, durability/lost-response
semantics, replay/fold-only reopening, and strict folding. Its genuine
cross-version/rebuild, the graph-level B1 suite, and qualified cost/RSS cells
remain green. Gate R0's legal high-entropy near-cap failure is now repaired and
the closure cell publishes successfully. The missing stock-RC.1 attempt receipt
and complete-output envelope remain factual limits on any future bounded-
storage claim, but do not block the selected unbounded retain-all profile. The
common token, attribution, lifecycle/correction, authorization, and product
contracts must still close before any public row acknowledgement.

### Bottom line

Lance supplies the WAL/LSM substrate: durable log entries, shard fencing,
replay, generations, LSM reads, staged merge primitives, merge progress, and
sharding. OmniGraph still owns the graph-hard parts: enrollment identity,
publication authority, graph-atomic folding and rejection, recovery,
lifecycle/quiescence, fresh snapshot cuts, format gating, policy/audit, and
cleanup integration.

The plan is:

1. retain Gate E0's green exact-version classifier as the RC.1 substrate gate;
2. keep Phase A's historical v7 enrollment recovery, all-lifecycle effect
   exclusion,
   narrow `SEALED` native-branch exception, lifecycle state, admission lease,
   and refusal/rebuild gates as the foundation preserved by current v8;
3. retain the repaired logical-slice/dense fold and its local/configured-RustFS
   near-cap closure plus full-fold RSS remeasurement instruments as regression
   gates; do not silently shrink the 8,192-row/32-MiB admission cap;
4. implement the selected unbounded retain-all profile: delete no MemWAL path,
   add no file/byte admission limit, expose observed growth only as advisory,
   and surface provider-capacity failure loudly through recovery;
5. retain the common B2 compare-and-chain token, trusted attribution,
   manifest-selected token participant, protocol-v2 lifecycle, and bounded
   `REPLACE`/`WITHDRAW` correction as the implementation contract;
6. keep B2b Lance-owned reclamation and a whole-root finite-lifetime budget as
   optional future profiles, activated only by a new measured need and their
   own RFC/evidence; do not block retain-all on either;
7. implement schema v9/config-v3/state-v2/recovery-v12 privately for the token,
   attribution, lifecycle, and correction contracts, and keep every product
   surface absent until those cross-version and crash matrices are green;
8. add schema/SDK/HTTP/CLI/Cedar/OpenAPI surfaces only after that private core
   passes; then activate Phase B2;
9. design and measure any later group-commit policy before claiming a
   performance multiplier;
10. pursue upstream replay-watermark and drain-error fixes plus the exact
   receipt/seal API in parallel; remove the RC.1 bridge when the first pair
   lands, and use the latter to simplify enrollment and broaden topology.

We tested the narrower support contract instead of waiting on the upstream
calendar. Phase A's v7 foundation and private B1's current v8/config-v2/v11
worker/fold core are implemented, and the widest admitted high-entropy shape
now closes. RFC-026 remains Draft because the public token, attribution,
lifecycle/correction, authorization, and product surfaces are not implemented.
The selected first storage posture is unbounded retain-all: no OmniGraph
MemWAL GC and no file/byte limit. Two negative RC.1 reclamation guards remain
checked in as rationale for that no-deletion rule. No schema-v9 contract,
production enrollment, acknowledgement, fold, operator, or public stream path
exists.
