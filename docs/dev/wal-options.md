# WAL Options

**Status:** decision note; RFC-026 records the selected profile

**Updated:** 2026-07-21

**Surveyed substrate:** OmniGraph's pinned Lance 9.0.0-rc.1 plus the public
LanceDB MemWAL integration on Lance 9.1.0-beta.4

**Related:** [RFC-026](../rfcs/0026-memwal-streaming-ingest.md),
[WAL thinking](wal-thinking.md),
[Lance MemWAL PR](lance-memwal-pr.md), and
[architectural invariants](invariants.md)

## Executive summary

OmniGraph has already chosen Lance MemWAL as its streaming substrate. The open
decision is not whether to build a different WAL. It is how much lifecycle
machinery must be complete before OmniGraph exposes durable stream admission as
a product feature.

There are five relevant options:

1. **Unbounded retain-all on stock Lance:** never delete MemWAL objects inside
   a root and do not advertise a file-count or retained-byte limit. Storage
   grows monotonically; provider exhaustion is an accepted, loud operational
   risk for this first profile.
2. **A narrow local Lance patch with managed GC:** implement the missing
   attempt, receipt, accounting, fencing, and reclamation primitives in Lance,
   pin that reviewed revision now, and upstream it without depending on the
   upstream review calendar.
3. **Whole-root rotation or rebuild:** seal and fold a root, perform an external
   operator/service cutover to a fresh root, and retire the old root as an
   operational unit rather than deleting live MemWAL internals piecemeal.
4. **A LanceDB-style approach that acknowledges WAL writes:** acknowledge after
   the WAL write is durable, make graph visibility and compaction asynchronous,
   and accept that the public contract does not yet include a hard retained-
   storage bound or a complete online-GC proof.
5. **Keep the current direct-write path:** retain the existing graph-visible
   commit acknowledgement and use batching/group commit as latency relief.

The decision is **Option 1 now**. Preserve direct writes as the safe default,
run the first streaming profile on stock Lance with no OmniGraph MemWAL GC,
and keep the row, memory, recovery, sequencing, lifecycle, and terminal-
disposition contracts strict. The near-cap fold is now closed without lowering
the 8,192-row/32-MiB admission cap: fold scanning charges logical slices and
densifies selected rows before retention. The measured full-fold RSS delta was
284,934,144 bytes (about 272 MiB), guarded by a 384-MiB remeasurement tripwire.

Managed reclamation, a hard retained-storage envelope, whole-root rotation,
and an upstream Lance patch are deferred optimizations. They can improve the
operating lifetime later, but they are no longer activation dependencies.
Option 4 remains useful precedent for the acknowledgement boundary; OmniGraph's
selected profile keeps stronger fencing, recovery, sequencing, and terminal-
disposition requirements than the surveyed LanceDB path.

No option in this document authorizes public streaming today.

## The contracts that must not be conflated

There are three different success points:

```text
durable stream admission
        ↓
fold into one or more Lance base tables
        ↓
one __manifest publication makes the graph change visible
```

- **WAL admission success** means the submitted data is durable and
  recoverable after a crash.
- **Fold success** means the data has been resolved against graph state and
  written into the appropriate base Lance dataset or datasets.
- **Graph commit success** means one `__manifest` publication made the complete
  graph transition authoritative.

An interactive `mutate` or `load` call acknowledges the third point. A stream
API may acknowledge the first point, but it must not call that acknowledgement
a committed or graph-visible write.

The RFC-026 contract still requires every admitted write to have a provable
path to fold, correction or terminal disposition, quiescence, and `SEALED`
within the declared **row and memory** bounds. It deliberately does not promise
a retained-byte, file-count, or provider-quota bound for the selected profile.

## Current facts

The decision starts from these facts:

- RFC-022's direct write path is correct and implemented. It is expensive for
  many small writes because every request pays recovery, Lance table effects,
  and graph publication.
- RFC-026 Phase A and the private B1 row/fold core are implemented for one
  main-only, unsharded stream with one live writer process.
- A private B1 acknowledgement requires both Lance's durability watcher and a
  same-writer post-durability epoch check. It is not a graph-commit
  acknowledgement.
- `__manifest` remains the only graph-visibility point. A WAL, MemTable, or
  flushed generation is pending physical state until a fold publishes it.
- Gate R0 found an all-shape closure bug. It is now fixed: fold accounting uses
  logical Arrow slices and a dense `take`, the legal high-entropy near-cap
  generation publishes successfully, and the 8,192-row/32-MiB admission bound
  is unchanged.
- Stock Lance RC.1 creates a fresh randomized directory for each generation
  materialization attempt, writes objects before the shard-manifest CAS, and
  persists no cross-open materialization-attempt ID, cap, reservation, or
  terminal receipt.
- Generic Lance version cleanup does not own `_mem_wal`. Deleting WAL objects
  can weaken writer fencing, so OmniGraph does not delete raw `_mem_wal` paths.
- Ordinary object-store listing is not a complete retained-storage or billing
  account. It may omit incomplete multipart uploads, provider-retained
  versions, delete markers, locked objects, and local staging residue.

## Comparison

| Option | Early durable acknowledgement | `__manifest` remains graph visibility | Hard retained-storage bound | Online reuse of reclaimed space | Requires Lance change | Current disposition |
|---|---:|---:|---:|---:|---:|---|
| 1. Unbounded retain-all | Yes | Yes | No, deliberately | No | No | **Selected first profile** |
| 2. Local Lance patch + managed GC | Yes | Yes | Candidate | Yes | Yes | Deferred optional optimization |
| 3. Whole-root rotation/rebuild | Yes, when paired with 1 or 2 | Yes | Bounds the active root, not all retired roots by itself | Coarse-grained operator retirement | No Lance-internal GC change | Optional operational escape |
| 4. LanceDB-style WAL acknowledgement | Yes | Yes for committed reads; optional fresh reads would be a separate mode | No hard proof on the public OSS shape | No on the surveyed public OSS shape | No | Precedent; OmniGraph keeps stronger correctness contracts |
| 5. Direct-write path | No separate stream acknowledgement | Yes | Existing bounded request/recovery model | Ordinary Lance maintenance | No | Production default and safe fallback |

## Option 1: unbounded retain-all on stock Lance — selected

### Basic idea

Delete nothing inside `_mem_wal`. OmniGraph uses stock Lance's durable writer,
replay, generation, and fold machinery, but never interprets raw paths as safe
garbage and never promises a maximum retained-object count or byte total.

```text
open/claim writer and append WAL
        ↓
materialize and fold
        ↓
retain every MemWAL object indefinitely
```

Storage use is monotonic. If the provider runs out of capacity or refuses a
write, that failure is loud and typed through the existing write/recovery
boundary; the system must not silently drop acknowledged data or pretend the
failed write committed. Capacity monitoring is operational guidance, not a
correctness proof or admission watermark.

### What must exist

- The existing 8,192-row/32-MiB admission limit and one-resident-writer bound.
- A fold path that closes every admitted row shape within its separately
  measured working-memory envelope.
- Durable watcher-plus-post-fence acknowledgement, typed ambiguity, recovery
  barriers, compare-and-chain sequencing, attribution, lifecycle, and terminal
  disposition before a public caller exists.
- Operator-visible backlog and observed storage, explicitly labelled as
  advisory rather than a hard retained-storage or billing bound.
- No OmniGraph deletion of `_mem_wal`, including apparently orphaned random
  generation roots or WAL fence sentinels.

### Advantages

- Uses stock Lance's MemWAL write machinery.
- Performs no unsafe raw-path deletion.
- Avoids making a missing stock-Lance accounting/receipt API a release-calendar
  dependency.
- Is the shortest route to the selected durable-admission contract.

### Limitations

- Space never becomes reusable inside the root.
- Conservative row and Arrow-memory admission may fill one generation before
  provider capacity is exhausted; this is not a retained-storage reservation.
- Provider quota may be exhausted without OmniGraph predicting the exact point.
- Repeated failures or ambiguous materialization attempts can retain additional
  residue forever.
- A long-lived root may eventually require operator-provisioned capacity or a
  whole-root rotation. That is an operational consequence, not an advertised
  finite-root protocol.

### Decision gate

The storage-envelope/attempt-receipt questions remain recorded evidence, but
they are no longer activation gates because this profile makes no corresponding
bound. The closure gate at this layer is now green: every legal admitted
generation must continue to fold or reach a typed terminal/recovery state. The
near-cap closure cell remains a regression gate locally and on configured
RustFS.

## Option 2: a narrow local Lance patch with managed GC

### Basic idea

Implement the missing generally useful MemWAL lifecycle primitives in Lance
now, pin OmniGraph to the exact reviewed patch revision, and submit the same
upstream-shaped change to Lance. OmniGraph does not wait for upstream review or
a release to use it, but it also does not move Lance's physical ownership into
an OmniGraph raw-path collector.

The patch should expose an opaque Lance-owned protocol such as:

```text
inspect exact reclaimable cut
        ↓
durably persist attempt and exact plan
        ↓
fence/advance writer epoch and successor sentinel
        ↓
delete the Lance-owned object set
        ↓
persist terminal receipt
```

### Required capabilities

- Stable materialization-attempt identity or deterministic attempt-owned
  output.
- Durable started/completed/ambiguous receipts across lost responses and cold
  reopen.
- Source-derived maximum physical-growth reservations enforced before effects.
- Exact Lance-owned inventory or durable accounting, including multipart and
  provider-retained state within the supported storage profile.
- Whole-cut eligibility proving the generation is in the base table, required
  indexes have caught up, and retained history no longer needs it.
- Sentinel-first writer fencing and a post-success epoch check so deleting an
  old WAL slot cannot let a stale writer receive clean success.
- Idempotent inspect/plan/execute recovery after partial deletion.
- Bounded claim, reclaim, receipt, and checkpoint history.

### Advantages

- Enables indefinitely running roots rather than finite retain-all lifetimes.
- Makes periodic GC a safe execution policy over a proved Lance operation.
- Keeps knowledge of Lance's private object layout and fencing rules in Lance.
- Can eventually replace conservative permanent charges with reusable
  capacity.

### Limitations

- OmniGraph carries a temporary dependency delta until upstream accepts an
  equivalent implementation.
- Every Lance upgrade must rebase, audit, and rerun the exact fault and storage
  matrix.
- This is materially more work than the first retain-all milestone.
- If the patch becomes an indefinite OmniGraph-only series, it has become a
  product fork regardless of what we call it.

### Decision gate

Option 2 is production-ready only after local and real object-store crash tests
cover plan creation, lost results, partial deletion, stale plans, stale
writers, unknown objects, cold reconstruction, hard admission-watermark
arithmetic, and bounded history. Passing the Lance patch tests alone does not
activate OmniGraph's public API; the common RFC-026 token, attribution,
lifecycle, correction, authorization, and graph-history contracts still apply.

## Option 3: whole-root rotation or rebuild

### Basic idea

Avoid deleting individual objects inside a live MemWAL binding. Instead:

1. stop admission;
2. resolve recovery and fold every acknowledged row that can be closed;
3. reach a durable `SEALED` state;
4. export/rebuild into a fresh graph root;
5. perform a reviewed authority cutover to the fresh state; and
6. retire the old root under an operator-controlled retention policy after all
   processes that could write it are terminated.

The new root/path is never reused, so a stale writer targeting the retired root
cannot alter the new authoritative root.

This is a whole-graph operational cutover. It is not the same as rebinding one
table to a fresh physical lifetime inside the existing graph. Such an in-root
table rebind is inactive Phase-D work and would need its own recovery-owned
physical build plus one `__manifest` publication protocol before it could be
used here.

### What it solves

- Provides the escape hatch for Option 1 when permanent charges approach the
  finite-root limit.
- Quarantines ambiguous or historically messy physical state rather than
  teaching an application-level collector to interpret it.
- Reuses the project's strict export/import storage-format posture.

### What it does not solve

- Rotation does not prove that every row in an old root was folded. That proof
  must happen before the authoritative cutover.
- It bounds the active root, not the aggregate bytes of all retained retired
  roots.
- An old root is not safe to delete while a foreign/stale process can still
  write it or while operator retention requires it.
- Frequent whole-root rebuilds may be too expensive for steady-state
  operation.

Option 3 is therefore an optional coarse operational escape, not part of the
first retain-all activation contract and not a replacement for a future safe
online-GC protocol.

## Option 4: LanceDB-style acknowledgement of WAL writes

### What LanceDB currently does

The public LanceDB OSS path provides the clearest precedent for a narrower
contract:

```text
restricted merge_insert upsert
        ↓
validate and collect the input for one shard
        ↓
reuse a cached ShardWriter
        ↓
ShardWriter::put
        ↓
put returns; with the default configuration the WAL is durable
        ↓
return version = 0
        ↓
separate LSM-aware read (open PR) / external compactor
```

With Lance's default durable configuration, `put` waits for WAL persistence.
The result is intentionally not a normal base-table version: LanceDB returns
the submitted row count and `version = 0` because the insert/update split and
base-table effect are not known until later compaction. LanceDB permits
`durable_write = false`, and its adapter does not perform OmniGraph's separate
same-writer post-durability `check_fenced()`. Its clean return is therefore an
API precedent, not evidence for OmniGraph's acknowledgement guarantee.

At the surveyed public revision, ordinary native scans do not merge this fresh
tier; the proposed LSM-aware read path remains a separate open change. The
public repository also contains no complete MemWAL fold worker, retained-byte
quota, materialization-attempt ledger, or MemWAL-specific GC. Remote/Cloud
lifecycle endpoints delegate to private server code and therefore do not
provide public evidence for those guarantees.

This is not evidence that the missing lifecycle is impossible or that the
public adapter is an end-to-end production lifecycle. It shows only that
LanceDB exposes the earlier-admission API shape while the complete public OSS
read/fold/reclamation lifecycle remains separate or absent.

### The corresponding OmniGraph contract

An OmniGraph version of this option would acknowledge a **WAL write**, not a
graph commit:

```text
acknowledged means:
  ✓ the batch is durably present in MemWAL
  ✓ OmniGraph can replay it after a supported crash
  ✓ pre-admission shape and policy checks completed

acknowledged does not mean:
  ✗ the rows are visible to normal graph reads
  ✗ graph-state-dependent validation completed
  ✗ a graph commit or time-travel point exists
  ✗ storage occupied by the WAL has a proved hard lifetime bound
```

An acknowledged row may therefore later reach a committed graph version, be
rejected by a state-dependent constraint with a durable terminal reason, or
remain blocked behind recovery/operator action. It may never disappear
silently.

Normal committed reads would continue to use only `__manifest`-selected base
table versions. A separate explicitly weaker `Fresh` read could later merge
MemWAL data, but it would be a distinct read contract and must never silently
replace snapshot-isolated committed reads.

### What this buys us

- It removes fold and `__manifest` publication from the acknowledgement's
  critical path; the real latency/throughput impact still requires the checked
  local and object-store product instrument.
- Supports callers whose contract is durable buffering with delayed graph
  visibility.
- A smaller first product surface: admission status, asynchronous fold, and
  operator observation can precede online reclamation.
- Operational experience with real workloads before finalizing every long-
  lifetime optimization.

### What this gives up or defers

- No advertised hard retained-storage bound on the current stock-Lance/public-
  LanceDB shape.
- OmniGraph's explicit fold may converge acknowledged rows into the graph, but
  the surveyed public LanceDB `optimize`/cleanup paths neither perform that
  fold nor reclaim MemWAL. Retained MemWAL storage remains monotonic until a
  whole-root retirement or a Lance-owned GC primitive exists.
- A long outage, repeated crash window, or stalled compactor can accumulate
  physical residue until writes must be stopped or the root rebuilt.
- Without a complete attempt receipt, retrying an ambiguous call cannot be
  presented as exactly once merely because the logical payload matches.
- Fresh reads, if added, have additional snapshot/deduplication semantics and
  cannot provide graph-wide multi-dataset atomicity by reading table-local
  MemWAL state directly.
- The retained-storage observations remain advisory. The separate near-cap
  closure failure has been fixed and is no longer part of this trade-off.

### Minimum safety floor even under this weaker option

Choosing Option 4 must not mean copying every current LanceDB omission. At
minimum OmniGraph would still require:

- `durable_write = true`; an in-memory-only acknowledgement is not allowed;
- successful per-put durability-watcher completion followed by the same
  writer's post-durability `check_fenced()`;
- typed `AckUnknown` for every post-invocation ambiguous result;
- durable caller write IDs/tokens sufficient to prevent an unknown attempt
  from being silently replayed as a newer direct write;
- durable per-write status that eventually records graph publication, a typed
  terminal rejection/dead-letter disposition, or the exact recovery/operator
  blocker; an acknowledged write may never be silently dropped;
- the common RFC-026 trusted-attribution and compare-and-chain token contracts,
  so correction/retry cannot impersonate the acknowledged writer or reorder a
  same-key history;
- a durable strict-block and correction/withdrawal/dead-letter protocol for
  rows that fail deferred uniqueness, referential-integrity, cardinality, or
  other graph-state-dependent validation;
- persistent quiesce/resume/abort lifecycle revisions and bounded management
  receipts, so every acknowledged row has a proved publication or terminal
  disposition before rebuild;
- the existing synchronous recovery barrier before another relevant writer
  proceeds;
- one explicit API result whose vocabulary says `accepted` or `durable`, never
  `committed` or `visible`;
- strict main-only/unsharded/single-live-writer-process refusals until broader
  topology is separately proved;
- operator-visible fold backlog and observed storage, with monitoring and a
  loud stop/rebuild threshold clearly labelled advisory: ordinary inventory
  cannot prove billed bytes, so Option 4 explicitly accepts that a provider
  quota may be exhausted before the observation threshold; and
- no raw `_mem_wal` deletion by OmniGraph.

Fallback to the direct path is permitted only before the first possible WAL
effect. After `put` may have run, OmniGraph must resolve that attempt or return
`AckUnknown`/`RecoveryRequired`; it must never submit the same logical write
through the direct path merely because the WAL response was inconvenient.

### Governance consequence

RFC-026 now explicitly chooses the no-storage-bound part of this contract for
the retain-all profile. That decision does not import the surveyed LanceDB
adapter wholesale. The governing principle plus invariants 5, 6, 13, and 14
still apply:

- invariant 5 and the governing principle require acknowledged pending state
  to remain durably recoverable until graph publication or a contract-defined
  terminal disposition; Option 4 may not relax that rule;
- invariant 6 already distinguishes durable stream admission from a graph-
  visible write, so early acknowledgement itself is compatible;
- invariant 13 still requires bounded, typed, observable memory, recovery, and
  backpressure paths. Provider-capacity exhaustion is the explicit exception
  in the selected storage profile and must fail loudly; it is not evidence that
  the process can be left memory-unbounded; and
- invariant 14 requires an RFC and boundary-matched crash, compatibility,
  refusal, recovery, and measured-performance evidence for this observable
  guarantee change.

The selected decision is that durable buffering before fold/publication is
worth monotonic-storage risk and delayed visibility. OmniGraph still promises
recoverability and contract-defined disposition for acknowledged data; it does
not promise indefinitely bounded retained storage.

## Option 5: keep direct writes and improve batching

### Basic idea

Do not activate a public WAL admission contract yet. Continue to use the
implemented RFC-022 path:

```text
validate and stage
        ↓
arm recovery
        ↓
commit every affected Lance table
        ↓
publish once through __manifest
        ↓
acknowledge graph-visible success
```

Reduce its fixed cost through caller batching, server-side group scheduling
that preserves request semantics, shared Lance sessions/caches, and measured
access-shape improvements.

### Advantages

- Preserves the strongest and already implemented success contract.
- Introduces no new retained MemWAL state or recovery surface.
- Remains the correct path for callers requiring immediate graph visibility.
- Provides a safe pre-effect fallback when stream admission cannot begin.

### Limitations

- Every logical batch still pays the graph commit protocol.
- It retains the complete graph-commit critical path for every acknowledged
  request; the comparative latency/throughput effect must be measured rather
  than inferred.
- Client-side buffering before submission is not durably protected by
  OmniGraph.

Direct writes remain necessary under every other option. Streaming is an
additional acknowledgement contract, not a replacement for transactions that
must be graph-visible on return.

## Routes that are not valid options

### Blind periodic garbage collection

Running a timer does not establish that an object is garbage. A safe periodic
collector is the execution mechanism for Option 2 after Lance can prove an
exact eligible cut, fence writers, recover a partial deletion, and issue a
terminal receipt. “List everything unreferenced and old enough” may delete an
in-progress generation, history/index-required data, or a WAL sentinel needed
to fence a stale writer.

### A separate OmniGraph WAL

Building a new log, transaction manager, or LSM tree would duplicate Lance and
force OmniGraph to solve distributed fencing, replay, compaction, reclamation,
and backpressure twice. It remains denied by the architectural substrate rule.

### Silent fallback after ambiguity

No option may convert a possible durable WAL effect into a fresh direct write
without resolving the first attempt. That can duplicate, reorder, or overwrite
logical changes while returning an apparently clean result.

## Selected staged route

### Stage 1: preserve the baseline and keep closure green

- Keep direct writes as the production default.
- Keep MemWAL admission crate-private and feature-gated.
- Keep the widest-shape fold closure cell green without lowering admission.
- Re-run the one-exclusive-fold RSS instrument when its 384-MiB CI
  remeasurement tripwire is crossed; the tripwire is not a runtime allocator
  limit.
- Preserve the post-durability epoch check and `AckUnknown` behavior.

### Stage 2: implement the public-contract prerequisites privately

- Implement compare-and-chain sequencing, trusted attribution, durable
  lifecycle/correction, terminal management receipts, authorization, status,
  and shutdown ownership on the existing manifest/recovery chassis.
- Keep the topology main-only, unsharded, and one live writer process.
- Make observed storage/backlog visible, but label it advisory and add no
  artificial file or byte limit.
- Keep all public schema/SDK/HTTP/CLI/OpenAPI surfaces absent until this private
  contract and its crash/cross-version evidence pass.

### Stage 3: add product surfaces

- Expose explicit durable-admission vocabulary, fold/block status, lifecycle
  controls, Cedar, SDK, HTTP, CLI, and OpenAPI parity only after Stage 2 is
  evidence-green.
- Document monotonic retained storage and provider exhaustion as the selected
  operating posture.

### Later, only if operations justify it

- Add Option 3 when whole-root rotation has a concrete operator need.
- Implement Option 2 only when retained-storage cost justifies the Lance patch
  and its crash/inventory/fencing matrix.
- Treat any hard byte/file admission bound or `GraphHistoryBudget` as a new
  measured RFC decision, not prerequisite scaffolding for retain-all.

## Decision statement

OmniGraph chooses unbounded retain-all as the first streaming storage profile.
Lance MemWAL remains the substrate, `__manifest` remains the only graph-
visibility point, direct writes remain the production-safe default, and
OmniGraph performs no MemWAL GC. Row/memory/recovery/lifecycle/disposition
bounds remain strict; retained bytes and file count do not have an advertised
limit. A Lance-owned reclamation patch and whole-root rotation are optional
future improvements, not blockers for this profile.

## References

- [RFC-026: MemWAL streaming ingest](../rfcs/0026-memwal-streaming-ingest.md)
- [WAL thinking](wal-thinking.md)
- [Lance MemWAL PR proposal](lance-memwal-pr.md)
- [OmniGraph write-path state of affairs](writing-path-state-of-affairs.md)
- [Lance MemWAL format](https://lance.org/format/table/mem_wal/)
- [LanceDB MemWAL write integration](https://github.com/lancedb/lancedb/blob/8450683b2aba033b12d8fe4c6e1601cc4b733b91/rust/lancedb/src/table/merge/lsm.rs)
- [LanceDB proposed LSM read path, PR #3489](https://github.com/lancedb/lancedb/pull/3489)
- [LanceDB remote lifecycle integration, PR #3501](https://github.com/lancedb/lancedb/pull/3501)
