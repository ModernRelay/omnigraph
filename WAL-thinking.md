# WAL Thinking

Working notes, updated 2026-07-18. Plain-language grounding for the WAL/streaming
discussion ([RFC-018](docs/rfcs/0018-ingest-wal.md) →
[RFC-026](docs/rfcs/0026-memwal-streaming-ingest.md)). Three parts: the
contract difference between an interactive graph commit and durable stream
admission, the expected performance shape and evidence still required, and the
build inventory.

Current boundary: RFC-026 Phase A is built, but streaming is not. Internal
schema v7 can recover and publish one exact empty main-only MemWAL enrollment,
persists its lifecycle authority, excludes ordinary local table/schema/
maintenance/repair/recovery effects for any lifecycle row, and refuses partial
format state. Native branch controls alone may proceed at `SEALED`, because
they do not move table HEAD. The enrollment method
is crate-private and exists only behind a feature-gated fault-injection seam.
There is still no `@stream`, public enrollment, WAL row put, durability
acknowledgement, fold, drain/resume operation, or fresh read.

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

This is the Phase B target contract, not an executable path in the current
binary.

One logical stream row follows a different contract:

```text
one stream row:

  1. validate shape, types, required/default fields, enum/range/check rules,
     reserved fields, and stream mode
  2. route it to the active owner of the table's MemWAL shard
  3. include it in a submitted Lance WAL batch
  4. wait for the durability result and acknowledge the caller

  after acknowledgement:
  ✓ the submitted WAL data is durable and replayable
  ✓ checks that require no graph read have completed
  ✗ default graph reads do not see it yet
  ✗ graph-dependent checks have not completed
  ✗ no graph commit/time-travel point exists yet
```

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

This is also future work. Phase A establishes the binding, recovery, and
exclusion preconditions a fold must consume; it does not merge a generation.

There is no fixed 30-second or 10,000-row contract. A folder may be triggered by
rows, bytes, maximum lag, resource pressure, or an operator request. It consumes
an ordered, durable generation cut:

```text
a fold:

  capture one exact stream binding and generation cut
    → run graph-dependent checks
       (referential integrity, cardinality, cross-version uniqueness,
        embeddings, and other base-dependent work)
    → strict mode: stop and mark the shard blocked on a permanent failure
      dead_letter mode: stage a typed reject in _ingest_rejects
    → stage accepted rows with Lance merge-insert and merged_generations
    → stage reject/audit participants required by the disposition
    → arm one RFC-022 recovery sidecar
    → commit every affected Lance dataset
    → classify every achieved participant and durably record EffectsConfirmed
    → publish all achieved versions in one __manifest CAS
    → accepted rows and configured reject/audit state become visible together
    → remove the satisfied sidecar best-effort
```

The fold is a new stream-specific RFC-022 effect adapter on the existing
publication and recovery chassis. It reuses that chassis, but it adds stream
binding and generation authority, fold-time validation, merge progress,
reject/audit atomicity, and later GC eligibility.

Folded data is not deleted from MemWAL immediately. WAL entries and flushed
generations become eligible for later GC only after the exact fold is
graph-visible, its recovery sidecar is resolved, maintained indexes have caught
up, retained versions no longer need the data, no fresh-read guard pins it, and
writer fencing remains safe.

### Side by side

| | Interactive request | Stream row + later fold |
|---|---|---|
| Acknowledgement means | The graph commit is durable and published | The stream row is durably accepted for ordered processing |
| Graph visibility | Before success is returned | After a successful fold; lag depends on triggers, validation, bounds, and fold health |
| Pre-ack checks | Complete request validation | Checks that need no graph/base-table read |
| Deferred outcome | None after success | Strict fold may block; configured dead-letter may record a reject |
| Same-key behavior | Serialize, retry, or fail loudly | Last-write-wins only when every occurrence of a key maps to the same shard |
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
4. **Write and storage amplification.** Data may exist in a WAL entry, a
   flushed Lance generation, the base dataset, and maintained indexes before
   obsolete copies can be reclaimed.

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

See [`write_cost.rs`](crates/omnigraph/tests/write_cost.rs). There is currently
no matched RC.1 RustFS/S3 benchmark of one-row interactive latency, sustained
branch throughput, or the not-yet-built streaming path. RFC-018's historical
single-digit language and upstream prototype results are not measurements of
the current OmniGraph path.

Consequently, the following are hypotheses rather than results:

- a specific current calls-per-write number;
- a one-PUT-per-100-ms streaming rate;
- a single-digit current branch-throughput ceiling;
- 8×, 80×, or 500× request-count reductions;
- a fixed two-times byte multiplier;
- any claim that WAL request volume stops scaling with input.

### Required benchmark before quoting a multiplier

Run a matched RC.1 RustFS/S3 instrument after the admission adapter exists:

1. fix schema, row shape, row bytes, table count, and index configuration;
2. compare interactive one-row requests, existing `load` batching, and stream
   admission at 1, 100, and 1,000 logical rows/sec, plus a sparse one-row/minute
   case;
3. declare batch size and maximum batch delay rather than relying on defaults;
4. record achieved throughput, backpressure, p50/p95/p99 acknowledgement
   latency, and fold visibility lag;
5. count GET/HEAD/LIST/PUT/DELETE and multipart operations separately for the
   acknowledgement and background paths;
6. record serial critical-path hops, uploaded bytes, retained bytes before and
   after GC, fold rows/bytes/generations, and every affected dataset;
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

### Lance primitives we plan to consume

These are real substrate capabilities, not a turnkey graph-streaming product:

| Lance primitive | What it gives us — and the boundary |
|---|---|
| **WAL entries on object storage** | Sequenced Arrow IPC entries with bit-reversed names. A warm uncontended append is normally one conditional PUT; open, recovery, retries, and fencing add work. |
| **Durability results/watchers** | The acknowledgement primitive. Predictable multi-request group commit still requires an OmniGraph batching policy. |
| **Epoch-fenced shard writers and fence sentinels** | One active owner per shard and stale-writer detection. This is per-shard fencing, not OmniGraph's graph-wide distributed recovery fence. |
| **MemTables and flushed generations** | Recent rows live in WAL-backed memory and later in small Lance datasets. Maintained FTS/vector/scalar indexes exist only when explicitly configured and supported. |
| **`merged_generations`** | Merge progress updated atomically with a base-table merge. It supplies the marker needed for idempotent per-table folding; RFC-022 still owns graph publication and partial-effect recovery. |
| **WAL replay** | Durable entries are replayed to reconstruct the MemTable under the next valid claimant. The guarantee depends on respecting fencing, lifecycle, compatibility, and safe GC. |
| **LSM merging reads** | A scanner over the base table, selected flushed generations not yet safely replaceable by the base read plan, and optionally same-process active/frozen MemTables. OmniGraph must build and retain a coherent graph-level fresh-read cut. It does not query raw WAL files as a normal read source. |
| **GC eligibility rules** | Upstream specifies when generations may be obsolete and warns that deleting WAL files can weaken fencing. RC.1 does not provide OmniGraph's graph-aware MemWAL garbage collector. |
| **Staged merge-insert** | `execute_uncommitted` plus atomic `merged_generations` fits the RFC-022 staged shape. There is no one-call MemWAL fold API. |
| **Shared sessions/store parameters in RC.1** | Writer-created generations and base-backed scanner paths reuse the base dataset's access context; fresh-only construction still receives that context from its caller. OmniGraph owns long-lived writer/session lifecycle and scanner integration. |
| **Key-based sharding** *(later)* | Horizontal scale when every occurrence of one key deterministically maps to one shard. |

OmniGraph already has reusable chassis: the manifest publisher, generic
recovery-sidecar framework, graph lineage, stable table identity, keyed write
adapter, and validation components. “Reusable” does not mean “unchanged”:
Phase A added stream lifecycle authority and enrollment recovery; later phases
still need row admission, fold read sets, reject/audit participants,
actor-range provenance, fresh cuts, and cleanup coordination.

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

### What OmniGraph must build

| Responsibility | Required contract |
|---|---|
| **Format capability and refusal** | **Phase A complete:** v7 stamp, strict v6↔v7 refusal, export/init/load rebuild, exact lifecycle validation, and uncovered-partial-format refusal. |
| **`@stream` intent and enrollment** | **Foundation only:** Phase A binds stable table/incarnation identity, location/main ref, never-reused enrollment ID, one empty shard, fixed configuration, and the mutable current-HEAD witness under recovery. Still missing: parser/SchemaApply intent, production first-use call, rebind, and witness advancement by folds. |
| **Public surface** | `POST /graphs/{graph_id}/streams/{type_name}/ingest`, status/fold/quiesce/resume endpoints, `omnigraph stream …` commands, and OpenAPI parity. Existing `/ingest` remains the deprecated load alias. |
| **Writer registry and routing** | Warm writers keyed by exact table identity, enrollment, and shard; one routed owner per initial `(table, main)`; bounded memory, inflight bytes, idle eviction, and backpressure. |
| **Durability batching** | Explicitly combine logical rows into submitted Lance puts while bounding latency and preserving ordered acknowledgements, actor accounting, cancellation, and typed failures. |
| **Pre-ack validation** | Run every rule that needs no graph/base-table read before WAL persistence. |
| **Fold adapter** | Capture stream binding and generation authority, run deferred checks, stage merge-insert with `merged_generations`, and publish through RFC-022. |
| **Strict and dead-letter disposition** | Strict blocking by default; configured rejects, accepted rows, audit, and merge progress committed atomically. |
| **Policy, lineage, and audit** | Dedicated Cedar actions plus fold actor and contributor-range provenance. |
| **Quiescence and rebind** | Durable `OPEN → DRAINING → SEALED`, stop admission, fence/drain owners, flush/fold, verify empty progress, resume or bind a fresh physical enrollment. |
| **Fresh reads** *(later)* | Explicit committed/fresh IR mode, exact base-plus-MemWAL cut, merged-generation exclusion, retention guards, and documented lack of cross-table atomicity. |
| **Cleanup** | Graph-aware generation/WAL GC integrated with visibility, recovery, time travel, indexes, fresh-read guards, and fencing safety. |
| **Evidence and operations** | Surface guards, enrollment/fold failpoints, race/crash matrix, cost budgets, metrics, status, API tests, and CLI parity. |

The table intentionally omits “small/medium” estimates. Atomic rejection,
quiescence, fresh-read retention, and GC are correctness protocols; size them
only after their implementation spikes expose the real work.

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
substantially. We should propose that change, but its review and release timing
is not a prerequisite for the bounded-profile decision. Gate E0 passed; the
upstream shape still gates overlapping-process topology, while bounded
Phase A is complete and row activation now depends on Phase B's production
proofs.

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
EnsureIndices, repair, cleanup, and branch operations touching that table must
refuse before effect or drain first. Every allowed commit publishes the next
witness atomically with the table pointer and stream lifecycle row.

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
WAL acknowledgements, or a format stamp. Phase A has since consumed that proof:
internal schema v7, the v10 enrollment intent, lifecycle authority, writer
exclusion, and refusal/rebuild are implemented. This still is not stream
release. The exact upstream receipt/seal remains the preferred simplification
and the broader-topology gate.

That Phase A work is not the Optimize proof copied to a new
writer kind. Optimize is content-preserving maintenance and grants no future
durability authority. Enrollment is the precondition for future
acknowledgements, so Phase A separately proves initialization, shard claim,
fixed binding publication, admission exclusion, restart, and foreign-state
refusal. Phase B must now prove first put, durability/lost-response semantics,
replay, and strict folding before a single row can be acknowledged.

### Bottom line

Lance supplies the WAL/LSM substrate: durable log entries, shard fencing,
replay, generations, LSM reads, staged merge primitives, merge progress, and
sharding. OmniGraph still owns the graph-hard parts: enrollment identity,
publication authority, graph-atomic folding and rejection, recovery,
lifecycle/quiescence, fresh snapshot cuts, format gating, policy/audit, and
cleanup integration.

The plan is:

1. retain Gate E0's green exact-version classifier as the RC.1 substrate gate;
2. keep Phase A's v7 enrollment recovery, all-lifecycle effect exclusion,
   narrow `SEALED` native-branch exception, lifecycle state, admission lease,
   and refusal/rebuild gates as the fixed foundation;
3. implement Phase B row admission, durable ack/replay, and strict fold as one
   main-only/unsharded/single-process slice;
4. design and measure explicit durability batching before claiming group
   commit or a performance multiplier;
5. pursue the exact upstream receipt/seal API in parallel, then use it to
   simplify the adapter and broaden topology when it lands.

We tested the narrower support contract instead of waiting on the upstream
calendar, and Phase A now implements it. RFC-026 remains draft: v7 stream
format/lifecycle foundations are active, but no production enrollment,
acknowledgement, fold, or public stream path exists.
