# Write Path: State of Affairs

**Type:** living architecture and execution summary
**Status:** current as of 2026-07-22
**Surveyed:** OmniGraph 0.8.1 development, internal manifest schema v9,
Lance 9.0.0-rc.1 at `cec0b7df`
**Scope:** the direct-publish graph write path, its RFC-022–028 family,
adjacent control and maintenance operations, known blockers, and the next
decision points

**Change-set boundary:** this page describes current main through RFC-026's
Phase A foundation, historical private Phase B1 core, the Gate R0/near-cap
closure work, the B2a unbounded retain-all gate, and the current private B2
compare-and-chain/token-fold slice.
RFC-024, RFC-025, and RFC-027 remain research-blocked at their recorded
evidence gates. Gate E0 first proved the bounded public Lance state classifier;
Phase A activated internal schema v7 with recoverable empty enrollment, durable
identity-keyed lifecycle authority, process-local admission/exclusion, and
strict format refusal/rebuild. Historical schema v8 added stream-config v2, a
root-scoped one-generation worker, watcher-backed durability followed by a
same-writer post-durability epoch check, exact replay/seal/retirement, and the
one-participant recovery-v11 fold. Gate R0 found that a legal high-entropy near-cap
generation could be acknowledged and materialized but could not fold because
shared-buffer capacity was charged instead of logical slice size. Admission,
replay, and fold now share `ArrayData::get_slice_memory_size` logical charging,
and fold rebuilds dense arrays with `take`; that exact shape
acknowledges, materializes, folds, and publishes. The measured RSS delta for
one exclusive full fold is 284,934,144 bytes (about 272 MiB). CI's 384-MiB
threshold is a remeasurement tripwire, not a runtime allocator or hard memory
limit.

Gate R0 historically rejected a *bounded, finite-lifetime* retain-all claim on
stock RC.1 because materialization has no durable attempt receipt or complete
physical-output envelope. We have deliberately dropped that claim. The first
profile is unbounded retain-all. Its private B2a gate is implemented: OmniGraph
never deletes a canonical durable MemWAL object, sets no retained-byte,
object-count, file-count, or history quota, and treats provider exhaustion as a
loud operational failure.
Lance may remove only a losing shard-manifest-CAS temporary staging object.
Complete and partial orphan output stays non-authoritative and is not descended
into, read, mutated, adopted, or deleted through retry/reopen; parent discovery
may still observe its prefix. Local and configured-RustFS provider-failure
tests pin those rules. The missing receipt/envelope therefore does not block
this profile. The 1/8/32/128 retained-history instrument keeps acknowledgement,
replay, fold, visibility, table, graph-manifest, adapter, object, and RSS terms
separate. It shows flat warm-ack operation shape alongside growing serialized
authority/combined history work; its timings, RSS, and LIST totals are advisory,
not quotas or SLOs. Managed reclamation remains optional Lance-owned work for a
later profile; a `GraphHistoryBudget` is not part of the immediate plan.

Internal schema v9 now activates stream-config v3, lifecycle state-v2, trusted
hidden stream-row metadata, and a manifest-selected graph-global
`_stream_tokens.lance` authority. Private admission derives canonical payload
and compare-and-chain token digests, supports exact idempotent retry and
same-generation chains, and treats every pre-wait authority capture as
provisional: it recaptures schema, binding, lifecycle, HEAD, and token authority
after shared admission and same-key queue ownership. Recovery-v12 owns exact
base and token transactions; only their exact joint outcome may reach the sole
manifest CAS, which advances both pointers, lifecycle, lineage, and a durable
fold-attribution commitment together. Recovery-v11 is historical v8 state and
is refused under v9. The genuine v8↔v9 gate proves two-way refusal and strict
export/init/load rebuild without exporting hidden trusted metadata.

The RFC remains Draft and the implementation remains reachable only through a
feature-gated private engine seam. There is still no `@stream` syntax,
production/public enrollment or put/ack/fold API, operator
status/drain/resume/correction surface, Cedar/HTTP/CLI/OpenAPI contract, or
fresh-read mode.

This page answers four practical questions:

1. What write-path architecture is actually implemented?
2. Which RFC decisions shipped, and which remain inactive?
3. What did implementation and measurement teach us?
4. What should we do next—and what should we deliberately not build yet?

It is an orientation document, not a second specification. Use
[invariants.md](invariants.md) for hard architectural rules and support
boundaries; the individual [RFCs](../rfcs/README.md) and
[review ledger](rfc-022-027-architecture-review.md) for decisions and acceptance
gates; [writes.md](writes.md) for current mechanics; [canon.md](canon.md) for
the narrative mental model; [lance.md](lance.md) for the pinned upstream
contract; and [testing.md](testing.md) for evidence ownership and commands.

## Executive judgment

The architecture is on the right track. The original write-path problem did
not require a new database inside OmniGraph; it required a disciplined
coordination layer over Lance:

- Lance owns table transactions, versions, branches, fragments, indexes,
  compaction, cleanup, and MemWAL.
- OmniGraph owns graph-wide authority, cross-table visibility, schema identity,
  validation, and recovery across independently committed Lance datasets.
- One `__manifest` commit is the only graph-content visibility point.
- Durable recovery ownership covers the gap between a Lance table effect and
  that manifest publication.
- Immutable, version-pinned state may be cached. Mutable authority is read or
  arbitrated durably; it is never supplied to a commit by an in-memory
  `GraphState`-like shadow.

The central chassis is implemented: [RFC-022](../rfcs/0022-unified-write-path.md),
[RFC-023](../rfcs/0023-key-conflict-fencing.md), and
[RFC-028](../rfcs/0028-stable-schema-identity.md) are complete at the documented
support boundary. The remaining RFCs are not a backlog to implement in numeric
order. RFC-024, RFC-025, and RFC-027 remain research-blocked. RFC-026 remains
strategic and Draft. Its production-neutral Gate E0 and Phase A foundation
passed their bounded gates. Internal schema v7 introduced exact main-only empty
MemWAL enrollment and local lifecycle exclusion; historical schema v8/config-v2
added one private no-rollover generation behind a root-scoped worker,
watcher-backed durability followed by a same-writer post-durability epoch check,
conservative replay, exact seal/retirement, and one recovery-v11 strict fold.
Current schema v9/config-v3/state-v2 preserves that bounded worker and activates
the private compare-and-chain row/fold core: canonical digests, trusted hidden
attribution, graph-global manifest-selected token authority, and exact
base-plus-token recovery-v12. It admits and folds only exact already-normalized physical rows—including
already-normalized vector values—and neither calls an external embedding
provider nor invents unspecified derived fields. Native branch controls alone
may proceed at `SEALED`, because they do not move table HEAD. This remains a
private format/correctness core, not a streaming product: no production caller
can enroll, put, acknowledge, or fold a row. Gate R0 exposed two distinct
facts. First, the original accounting could reject one legal admitted near-cap
shape after durable acknowledgement; logical-slice accounting plus dense
rebuilding now closes that shape. Second, stock RC.1 cannot prove a lifetime
bound for materialization attempts or their complete physical growth. The
selected first profile makes no such claim. Its private B2a gate retains every
canonical durable MemWAL object without an OmniGraph retained-byte,
object-count, file-count, or history quota, accepts loud provider exhaustion,
keeps complete/partial orphan roots inert, and measures retained-history terms
locally and on configured RustFS.
Neither a test-only attempt ledger nor managed reclamation is on the immediate
activation path. The next work is the remaining private lifecycle,
correction/status, and explicit-enrollment machinery, followed by product
contracts only after their evidence is green. B1's
adapter recheck contains a stale epoch from becoming a clean OmniGraph
acknowledgement; it is not a substrate retention/fencing primitive.

The largest remaining correctness boundary is topology: destructive recovery
is serialized across every handle in one process, but not against a live writer
in another process. OmniGraph must not advertise general multi-process writers
on one graph until a distributed fence closes that gap.

## The implemented correctness protocol

There is one correctness protocol with writer-specific physical adapters. It
does **not** mean every writer is forced through one identical Lance operation.

```text
resolve or refuse relevant recovery
    -> capture one immutable authority token and accepted catalog
    -> prepare and validate the complete operation
    -> acquire stream-admission domains
    -> StreamFold only: seal, drain, retire, and prove one immutable fresh-tier cut
    -> acquire schema -> branch -> stream-token -> sorted-table gates where applicable
    -> relist recovery and revalidate authority plus physical baselines
    -> durably arm an identity-bearing recovery intent
    -> apply writer-specific graph/table Lance effects
    -> exact adapters durably confirm the achieved outcome;
       Optimize retains a bounded complete-set classifier
    -> publish one graph-visible __manifest transition when needed
    -> delete the intent; a recovery resolution records an audit first
```

For the exact adapters, the authority token binds the facts that made planning
valid: the native Lance branch incarnation, exact optional graph head, accepted
schema identity, and relevant table versions/physical refs. A publisher may
retry a transient manifest CAS against that captured precondition. A semantic
full reprepare starts a new attempt instead of refreshing versions under an old
validation result; Optimize likewise reopens and replans a maintenance retry.

The five established graph-visible writer classes emit schema-v9 recovery
envelopes before their first independently durable effects. Phase A adds the
internal `StreamEnrollment` adapter with a dedicated schema-v10 envelope;
current private B2 adds the schema-v12 `StreamFold` adapter with exact
base-table and `_stream_tokens.lance` participants. Table ownership is
keyed by `(stable_table_id, table_incarnation_id)`, never inferred from an
alias, path, Lance version, or field ID. Mutation, Load, BranchMerge,
SchemaApply, and EnsureIndices use exact protocols in which `Armed` is
rollback-only and `EffectsConfirmed` may roll forward only under the captured
authority. StreamFold is also exact, but its immutable fresh-tier cut is proved
before arm: only the exact planned base plus token outcome may publish both
pointers, lifecycle state-v2, lineage, and attribution. An exact base-only
outcome may complete only the pre-minted token transaction; foreign, buried,
token-only, or ambiguous partial outcomes fail closed. Recovery-v11 is
historical v8 syntax and is refused under v9.
Optimize is the deliberate exception: its v9 envelope carries identity-bound
pins and a bounded complete-set maintenance classifier, with no exact
authority/fixed-lineage confirmation phase, because Lance exposes no
caller-minted maintenance transaction. Mutation and Load share one writer
adapter but retain distinct sidecar/audit kinds, so the established five
classes map to six `SidecarKind`s`; bounded enrollment is the seventh and
private fold is the eighth. Neither stream adapter is reachable from a
production API.
Foreign, buried, or ambiguous effects fail closed.

The sidecar is not a second WAL or transaction log. Lance remains the owner of
table transactions and durable data. The sidecar records the authority,
ownership, intended graph delta, and evidence needed to resolve the temporary
gap between those Lance effects and graph visibility.

### Implemented write adapters

| Writer | Physical adapter | Visibility and recovery posture |
|---|---|---|
| Mutation / load | One staged keyed or overwrite transaction per touched table; deletes stage too | Exact transaction identities, fixed lineage, and one final manifest publish. Effect-free insert/upsert contention may fully reprepare only under the typed rules; any owned or ambiguous effect becomes `RecoveryRequired`. |
| Branch merge | A bounded ordered chain of filtered inserts/upserts/deletes per changed table | One v9 intent covers physical effects, pointer-only updates, first-touch refs, and the complete merge delta. The target is never silently reparented after planning. |
| SchemaApply | Metadata-only pure type rename, exact staged rewrite for property/schema changes, or strict first-touch create | The fixed schema identity, table delta, lineage, and source/IR/state promotion are one recoverable outcome. Every supported rename preserves logical identity and table lifetime; only a pure type rename also preserves Lance version/index history. Drop/re-add does neither. |
| EnsureIndices | One staged mixed `CreateIndex` transaction per productive table | Indexes remain derived state. Exact transactions and the complete pointer delta publish once; untrainable vector work stays pending instead of breaking logical writes. |
| Optimize | Lance compaction, incremental index optimization, and missing-index materialization across productive tables | One graph-wide v9 envelope and at most one monotonic manifest/lineage publish. Provenance is bounded rather than exact because Lance does not expose a caller-minted maintenance transaction surface. |
| StreamEnrollment (internal Phase A foundation) | Lance's public internally committing singleton MemWAL initializer plus one pre-minted empty shard | One v10 intent classifies only no effect, exact `N + 1` index, or that index plus the exact empty shard, then publishes the pointer and `OPEN` lifecycle together. Once an effect exists it rolls forward only. The adapter is crate-private and has no row-admission caller. |
| StreamFold (private B2 core) | Before arm, seal/drain/retirement proves one immutable fresh-tier cut; afterward that generation becomes one exact staged keyed base transaction carrying its Lance `MergedGeneration` plus one exact staged `_stream_tokens.lance` transaction | One v12 intent binds the cut, both prestates and pre-minted transactions, planned token winners, fixed lineage, lifecycle state-v2 outcome, and attribution summary. Only exact base + exact token permits one manifest CAS to publish both pointers, refreshed `OPEN` witness/epoch floor, lineage, and attribution. An effect-free attempt leaves the generation fold-only for retry; an exact base-only residual may complete only its planned token effect. Foreign or ambiguous state fails closed. |

### Explicit adjacent paths

These operations do not create a side door around the protocol:

| Operation | Why it is different |
|---|---|
| Native graph-branch create/delete | Lance `BranchContents` is the sole logical authority. Create/delete use a smaller authority-derived control protocol, emit no graph lineage, and reconcile clone-tree/ref crash gaps within the single-writer-process boundary. |
| Repair | Repair is main-only and does not manufacture a new table effect. It classifies already-uncovered HEADs and publishes selected adoptions together in one manifest/lineage commit after explicit operator confirmation; suspicious or unverifiable drift additionally requires force. |
| Cleanup | Cleanup is destructive physical version GC, not graph publication. It refuses unresolved recovery and uncovered main-HEAD drift, protects lazy-branch inherited main versions, and performs graph-wide preflight before fault-isolated per-table GC. The CLI requires confirmation; the public engine method executes the requested cleanup directly. |

## What is done

| Area | Implemented state |
|---|---|
| Publication surface | The historical Run state machine and `__run__*` staging branches are gone. The graph-write storage surface is crate-private, sealed, and staged-only; source guards make a new durable gateway an explicit review event. |
| Lineage | `graph_commit` and `graph_head` live in `__manifest` and land in the same CAS as table pointers. The former secondary commit-graph datasets are retired. |
| Mutation / load | Multi-statement writes accumulate in `MutationStaging`, provide read-your-writes, stage deletes as well as constructive writes, and publish once after complete validation. D2 deliberately keeps one query constructive or destructive. |
| Identity / schema | Accepted SchemaIR v2 owns graph-scoped, monotonic, no-reuse type/property/incarnation IDs. Supported renames preserve logical identity and table lifetime; a property rename rewrites that lifetime, while only a pure type rename also preserves physical/index history. Drop/re-add mints a new lifetime. The strict strand serves only internal schema v9 and upgrades by export/init/load rebuild. |
| Key fencing | Every graph table has exact non-null physical `id` as Lance's unenforced PK. Strict insert/upsert use the sealed exact-`id` filtered adapter; bare keyed Append is forbidden; effect-free conflicts have typed reprepare or `KeyConflict` outcomes. |
| BranchMerge | The ordinary ordered diff remains the correctness path. A completely verified `omnigraph.insert_absence = "v1"` history permits the proven-insert shortcut without committing Append or weakening the final filter. |
| Validation | Value, enum, uniqueness, edge-RI, and cardinality use one catalog-derived, delta-scoped evaluator across mutation, load, and merge. Committed non-key uniqueness probes are batched by constraint group and bounded chunks. |
| Recovery | Same-process handles share ordered queues for the same canonical local root or identical normalized opaque remote/custom URI. Stream-admission domains are acquired outside schema → branch → stream-token → table gates where applicable. Mutation/load, SchemaApply, BranchMerge, EnsureIndices, StreamFold, and `refresh` heal roll-forward-only; Optimize, Repair, and Cleanup refuse pending recovery, while branch controls use specialized barriers. Read-write open performs the quiesced full sweep, including exact v10 enrollment and v12 base+token fold completion; read-only open never repairs and refuses unresolved stream recovery. Historical v11 folds are refused under v9. A resolved intent is audited internally with the original actor when present; no-effect cleanup need not create graph lineage. |
| RFC-026 Phase A (v7 foundation) | Internal schema v7 introduced identity-keyed lifecycle rows and exact empty main/unsharded enrollment. At that Phase-A-only boundary, any lifecycle row—including `SEALED`—rejected base-table, schema, maintenance, repair-adoption, and recovery effects under a process-local admission lease because no drain/fold witness-update adapter existed. Native branch create/delete alone could proceed at `SEALED` because it did not move table HEAD; `OPEN`/`DRAINING` refused it. Enrollment and open-time validation rejected named-branch overlap and any uncovered lifecycle/MemWAL mismatch. Schema v9 preserves these foundation guarantees. |
| RFC-026 Phase B1 (historical private core) | Internal schema v8/config-v2 added one root-scoped, cross-handle serialized worker and a hard-bounded 8,192-row/32-MiB logical dense-slice Arrow no-roll generation. Watcher success proves durability; a clean `DurableBatchAck` additionally requires the same `ShardWriter::check_fenced()` to succeed immediately afterward. Fence loss, epoch-read failure, owner-task failure, or deadline ambiguity is post-invocation `AckUnknown` plus worker retirement. Reopen/replay is conservative and exact drain proof precedes quiesced abort. Gate R0's deterministic legal high-entropy near-cap cell exposed sparse scanner arrays; logical-slice charging plus dense copies repaired it. Physical RSS is evidence only. V9 preserves those worker/closure mechanics, but recovery-v11 itself is historical only. |
| RFC-026 private B2 token/fold core | Internal schema v9/config-v3/state-v2 adds canonical payload/token digests, hidden trusted row metadata, exact idempotency and compare-and-chain classification, same-generation token overlays, and post-admission authority recapture. `_stream_tokens.lance` is graph-global durable sequencing state selected only by its manifest witness. Recovery-v12 owns exact base+token transactions and atomically publishes both pointers, lifecycle, lineage, and durable fold attribution. The genuine v8↔v9 refusal/rebuild gate is green. The slice remains private; lifecycle management, correction/status, public enrollment, authorization, and product surfaces are inactive. |
| Lance access | One process-wide `ObjectStoreRegistry` reuses clients. Each `Omnigraph` handle owns its cached data-table `Session`; one process-wide zero-cache control `Session` opens mutable tips. Only the object-store registry is shared between the data and control sessions. This is “cache the past, never the present,” not one global cached session. |
| Maintenance | EnsureIndices stages exact missing-index transactions. Optimize coordinates graph-wide compaction/index work under one bounded recovery envelope. Periodic optimize compacts `__manifest`, but unmaintained history-dependent paths are not globally flat. |

Selected enforced limits are 8,192 rows / 32 MiB per keyed Mutation/Load table,
8,192 rows / 32 MiB of logical dense-slice Arrow bytes for the complete private
stream generation (physical RSS remains an evidence tripwire), at most 32
pre-effect full reprepares, 8,192 rows / 32 MiB per BranchMerge chunk, one
aggregate 32-MiB retained merge-validation budget, at most 1,024 logical data
transactions per merged table, and a 1,026-version exact-recovery scan bound.
Optimize defaults to eight physical tasks and a five-attempt compaction retry
budget. This is not the complete constants catalog. The important loud outcomes
are `KeyConflict`, `ReadSetChanged`, `RecoveryRequired`, `AckUnknown`, and
`ResourceLimitExceeded`; none reports partial success.
See [writes.md](writes.md) and [constants.md](../user/reference/constants.md) for
retry rules, recovery classification, and the full owned limits.

## RFC implementation state

| RFC | Current disposition | What that means now |
|---|---|---|
| [022 — Unified graph-write protocol](../rfcs/0022-unified-write-path.md) | **Implemented** | The shared correctness state machine, per-writer adapters, recovery barrier, one visibility point, control exceptions, and test lattice are the stable chassis. Completion is scoped to one writer process per graph for destructive recovery. |
| [023 — Key-conflict fencing](../rfcs/0023-key-conflict-fencing.md) | **Implemented** | Internal schema v6 introduced exact-`id` PK metadata, closed keyed routing, typed conflicts, bounded replay, rebuild/refusal, and accepted performance evidence; v9 preserves that contract. |
| [024 — Durable table heads](../rfcs/0024-durable-table-heads.md) | **Research-blocked** | The first in-manifest BTREE candidate has a specified logical contract and flat indexed row/range work, but fails the complete physical-I/O gate. No head rows or heads format are active. |
| [025 — Checkpoint retention](../rfcs/0025-checkpoint-retention.md) | **Research-blocked** | Lance tag/pin semantics pass, but the proposed in-manifest registry access shape is not history-flat after compaction. No checkpoint rows, `ogcp_` production tags, API, or cleanup integration are active. |
| [026 — MemWAL streaming ingest](../rfcs/0026-memwal-streaming-ingest.md) | **Draft; private B2 token/fold core and retain-all profile implemented; public inactive** | Schema v9 preserves v7/recovery-v10 enrollment and the bounded B1 worker while activating config-v3/state-v2, canonical payload/token digests, trusted hidden attribution, manifest-selected `_stream_tokens.lance`, stale-authority revalidation after shared admission, and recovery-v12 exact base+token fold. One manifest CAS publishes both pointers, lifecycle, lineage, and fold attribution. Recovery-v11 is historical only. Gate R0's no-go still prohibits a finite storage promise, so the selected profile never deletes a canonical MemWAL object and sets no retained-byte/object/file/history quota. Genuine v8↔v9 refusal/rebuild is green. Explicit production enrollment, lifecycle management/correction/status, authorization, and all product surfaces remain inactive; B2b managed reclamation is optional future work. |
| [027 — Lineage merge deltas](../rfcs/0027-lineage-merge-deltas.md) | **Research-blocked** | The desired O(delta) classifier and fallback contract are specified. Selective live-row and deletion-delta discovery are not yet bounded, so `OrderedTableCursor` remains the correctness path. |
| [028 — Stable schema identity](../rfcs/0028-stable-schema-identity.md) | **Implemented** | Rename-stable IDs, table incarnation, identity-derived paths, schema/recovery integration, and strict rebuild activation were introduced in v5 and remain active in v9. |

## Blockers and constraints discovered

| Frontier | Evidence and current consequence | Exit condition |
|---|---|---|
| Distributed recovery fence | Process-local queues cannot stop a live foreign process; Lance restore may orphan its commits and native refs lack conditional compare-delete. Supported destructive recovery remains one writer process per graph. | A separately designed and adversarially tested distributed fence before multi-process writers, background compensation, or cross-process exact maintenance recovery. |
| History-flat authority | RFC-024/025 show flat BTREE rows/ranges/pages can coexist with history-growing manifest discovery or compacted bytes. No heads/checkpoint format is active; mutable tip caches and a second authority remain rejected. | A new Lance-native access shape—or revised measured operational contract—passes the original cold/warm, compacted/uncompacted, local/object-store gate. |
| Internal history GC | Safe live-writer cleanup needs a durable resurrection/retention boundary; otherwise a stalled writer can recreate a collected version. | An evidence-backed cleanup watermark/fence before automated `__manifest` version GC. |
| MemWAL delivery and closure | RC.1 initializes the system index and claims shards as separate effects without a caller-minted combined receipt or cross-process seal. Phase A recovers that gap exactly for main-only, one-shard, one-live-writer-process empty enrollment. At the row boundary, the durability watermark is writer-wide while batch positions reset after MemTable rollover, `put_no_wait` may mutate before returning `Err`, replay leaves its fresh BatchStore WAL watermark unset, and `wait_for_flush_drain` can lose a completed failure before a late waiter snapshots it. Neither batch positions nor WAL statistics are durable receipts. The private v9 slice now closes compare-and-chain sequencing, trusted attribution, stale-authority admission, and exact base+token fold recovery. | Implement and prove explicit production enrollment, revisioned lifecycle management, bounded correction/status, authorization, cancellation/shutdown, and product parity before adding a public caller. A public receipt/seal or accepted distributed fence remains the exit for overlapping processes and failover, not for the current single-live-writer-process profile. |
| MemWAL retained growth | A clean retained generation has measurable current objects, and the Gate R0 sweep proves that referenced currently listed immutable paths retain their class and size at one/four/eight folds. RC.1 still provides no durable cross-open attempt cap, complete physical-output receipt, or provider-billed-byte inventory. That prevents OmniGraph from promising a finite retained-storage bound. | Private B2a implements the deliberately unbounded profile: never delete a canonical durable `_mem_wal` object, keep complete/partial orphan subtrees non-authoritative and untouched below their roots through retry/reopen, impose no retained-byte/object/file/history quota, and fail loudly if the provider refuses further writes. Lance may remove only its losing manifest-CAS temp staging. The 1/8/32/128 local/RustFS instrument records separate advisory terms and shows combined retained-history work grows; it does not create a bound. Add attempt ledgers or physical-growth reservation only if a later profile claims bounded retention. |
| MemWAL reclamation (optional B2b) | Stock RC.1 exposes evidence-level raw listing and manifest reads, not a complete classified inventory or safe MemWAL delete/GC primitive. Generic `cleanup_old_versions` leaves `_mem_wal` unchanged. Worse, deleting the successor's empty WAL fence sentinel can let a stale writer complete a WAL PUT and report watcher success because RC.1 has no post-success epoch check. Private B1 contains that result for its own acknowledgement but cannot retract durable bytes or protect raw Lance callers. Raw path deletion in OmniGraph remains forbidden. | Keep B2b as optional Lance-owned durable inspect/plan/execute, attempt/receipt recovery, post-success fencing, bounded history checkpoint, strong inventory/accounting, and enforced-watermark work. It is not on the immediate retain-all activation path. |
| O(delta) merge | A version-column predicate is still O(rows) without a selective source, and deleted rows have no live version columns. Full ID differencing remains correct. | Bounded live-row and deletion/change discovery, exact shadow agreement, and a table-size-flat one-row-delete gate. |
| Optimize provenance | Compaction/reindex has no stable caller-minted transaction covering the complete effect. Optimize therefore uses bounded, not exact, provenance. | Both an upstream maintenance transaction API and distributed recovery fencing. |
| Remaining bounds/operations | Some long-running operations lack complete memory/time budgets; rollback waits for quiesced read-write open; recovery audit has no public query. | Incremental, independently owned hardening without widening format or topology. |

RFC-026's bounded implementation separates two facts that earlier wording
incorrectly collapsed. The enrollment binding is stable: logical
table/incarnation, location/main ref, never-reused enrollment/shard IDs, and
configuration. The public Lance composite of branch identifier, current table
version, transaction UUID, and manifest e_tag is a mutable
`CurrentHeadWitness`; every ordinary commit changes it. While a bounded stream
is `OPEN`, only fold/recovery may advance that base HEAD, and the next witness
must publish atomically with the table pointer. Other writer/control/
maintenance paths touching the table refuse pre-effect or drain first. Gate E0
proved the classifier and witness model with complete direct-probe and
object-store evidence; Phase A established the reversible support restriction.
The private worker consumes it for one nominally bounded generation, watcher-backed
durability plus the same-writer post-durability epoch check, replay, and strict
folding without pretending the process-local lease is a distributed fence,
inventing a durable WAL offset, or reusing a watcher across rollover. Fence
loss or uncertainty after durability is `AckUnknown`, never a clean ack. Gate
R0 exposed a sparse-buffer closure gap; the dense-copy repair closes the exact
legal near-cap shape without changing admission. V9 then adds durable
compare-and-chain authority: post-wait recapture prevents a stale admission,
same-generation overlays preserve exact retry/ordering, and recovery-v12 makes
the base and token effects one manifest-visible outcome with fold attribution.
Those results authorize neither raw MemWAL reclamation, a public product, nor a
broader topology.

The full known-gap ledger, including adjacent local-CAS and unsupported
multi-version-topology details, remains in
[invariants.md#known-gaps](invariants.md#known-gaps).

## How Lance 9.0.0-rc.1 influenced the plan

RC.1 mostly validated the direction and sharpened gates. It did not justify a
write-path redesign.

| RC.1 finding | Planning effect |
|---|---|
| Lance surfaces consumed by RFC-022/023—transactions, branches, key filters, staged indexes, compaction, and shared sessions—remain compatible; separately surveyed tag/cleanup behavior is also unchanged | Keep the current architecture. PR #364 passed 22 surface guards and 129 runnable failpoint tests; the Gate-0 follow-up adds the 23rd guard plus checkpoint cost evidence. No format redesign is needed. |
| Derived MemWAL datasets inherit the base store parameters and `Session`; `put_no_wait` returns an optional watcher whose completion is `Result<()>`, not a durable row coordinate. RC.1's watcher watermark spans the writer while active-MemTable batch positions reset on rollover; `put_no_wait` can also mutate before a later scheduling error. Replay leaves the fresh BatchStore watermark unset, and a late `wait_for_flush_drain` can miss a completed failure. | Shared-session propagation removes one integration concern, but the watcher is safe only inside B1's proved single-generation lifecycle. B1 treats watcher success as necessary durability evidence, then requires the same writer's `check_fenced()` to succeed before clean acknowledgement. Every post-invocation error or ambiguity is `AckUnknown`; rollover remains prevented, the public replay-watermark bridge handles fold-only reseal, generation proof comes from refs plus authoritative manifest state, and the writer retires/reopens before another generation. This does not create an exact combined enrollment receipt or cross-process seal. MemWAL is strategic, not experimental. |
| Generic cleanup ignores `_mem_wal`, and RC.1 does not recheck the writer epoch after a successful WAL PUT | B1 contains the clean-ack stale-epoch result for its own private caller by rechecking after watcher success and retiring on any fence/read ambiguity. The selected retain-all profile performs no raw-path collection, so generic cleanup's non-ownership is expected. B2b keeps Lance-owned reclamation, post-success fencing, durable receipts, inventory/accounting, and an enforceable growth reservation as optional future work; it does not block unbounded retain-all. |
| Experimental, opt-in `DataOverlay` was added | Do not adopt it now. OmniGraph does not enable feature flag 64 or emit the operation; unknown foreign overlay effects remain fail-closed. DataOverlay's experimental status says nothing about MemWAL's status. |
| RC.1 expands Lance write rejection from the three row-address names to all five surveyed virtual system-column names | OmniGraph now rejects `_rowid`, `_rowaddr`, `_rowoffset`, `_row_created_at_version`, and `_row_last_updated_at_version` during parsing and accepted-IR validation. A beta.21 development graph using a newly reserved row-version name must be exported with beta.21, renamed, and rebuilt. |
| A genuine ordinary-schema beta.21 V2_2 graph forward-opened, queried, and merge-wrote under RC.1 | No general storage-format migration. The reserved-name exception is explicit rather than hidden behind a format bump. |
| RFC-024's 10/100/1,000 run added a bounded one-operation boundary while the rejected compacted-byte slope remained | Durable heads stay research-blocked; do not weaken the gate. The RC made the no-go slightly clearer, not the design more attractive. |
| A separate RFC-025 Gate-0 follow-up run on the RC.1 pin—not part of PR #364—found the same complete-I/O problem for checkpoint authority | Keep the logical checkpoint/tag ordering, reject the current access shape, and stop before production format/API work. |
| Maintenance still has no caller-controlled exact transaction | Optimize's bounded adapter and single-writer-process recovery boundary remain necessary. |
| DataFusion moved 53 -> 54; Arrow and explicit V2_2 stayed stable | Mechanical dependency/type alignment, not an architecture change. |
| RC.1 requires Rust 1.91+ and remains a git-pinned prerelease | Continue the full alignment audit on every bump. Crates.io/stable release work remains gated on Lance 9 stable. |

The matched merge-all-changed diagnostics were effectively neutral: RC.1 was
about 0.6% slower at 10K and 1.9% at 100K in single matched runs, with similar
or slightly lower incremental RSS. That is not a roadmap signal.

## Next logical steps

### Now

1. **Keep the B1 closure and B2a retain-all evidence as permanent regression
   gates.** The exact 8,192-row high-entropy shape must continue to acknowledge,
   materialize, fold, and publish locally and on configured RustFS. Provider
   failures must remain typed; complete/partial orphan roots must remain inert;
   canonical durable MemWAL deletion must remain zero; and older retained roots
   must receive zero IO. Keep 384 MiB as a CI remeasurement tripwire for the
   isolated fold RSS delta, not as a runtime allocation promise or a retained-
   storage limit. Treat 1/8/32/128 LIST, timing, and RSS results as advisory.
2. **Finish the remaining B2 control plane privately.** Build
   request-idempotent first-use enrollment, authoritative status, persistent
   revisioned quiesce/resume/abort-drain receipts, and bounded
   `REPLACE`/`WITHDRAW` correction on the v9 token/fold authority already in
   place. Keep every step under recovery and the manifest visibility CAS. Do
   not add a storage quota, materialization-attempt ledger,
   `GraphHistoryBudget`, or raw `_mem_wal` deletion.
3. **Add product surfaces last.** Only after those private lifecycle,
   correction/status, and crash/race gates are green should schema intent, SDK,
   HTTP, CLI, Cedar, OpenAPI, and shutdown ownership converge on the same core.
4. **Keep B2b managed reclamation independent and optional.** If a bounded
   profile is scheduled later, author Lance-owned durable inspect/plan/execute,
   attempt/receipt recovery, post-success epoch fencing, strong inventory or
   durable accounting, and an enforced retained-storage watermark. A
   graph-global `GraphHistoryBudget` would require its own RFC and every-writer
   evidence. Never delete `_mem_wal` paths from OmniGraph.
5. **Keep RFC-024/025/027 stopped at their research no-gos.** Their blockers are
   independent of the v9 stream core; do not add RFC-024 heads, RFC-025 graph
   checkpoint rows/format, or RFC-027 lineage-delta state as incidental B2
   work.
6. **Give a distributed recovery fence its own design and evidence gate.**
   Define authority, expiry/renewal, fencing tokens, and crash semantics before
   implementation; require adversarial multi-process tests on local and object
   storage.
7. **Continue low-risk v9 hardening.** Add missing resource/time budgets,
   preserve cost-at-history-depth gates, and reduce constant factors only where
   the existing authority model remains intact.
8. **Coordinate upstream without making it the calendar.** The additional
   Lance asks are replay initializing the per-MemTable WAL watermark, drain
   completion that cannot lose a finished error, recoverable MemWAL
   enrollment/admission, conditional native ref operations, exact maintenance
   transaction provenance, and a bounded deletion/change-lineage source.

### Only when an evidence trigger fires

- **Public MemWAL row activation:** Phase A passed its bounded gate; the private
  v9 core now covers near-cap closure, unbounded retain-all, canonical
  compare-and-chain sequencing, trusted attribution, and exact base+token
  folding. The RFC remains Draft and public activation remains off until
  request-idempotent explicit enrollment, strict correction/disposition,
  persistent revisioned lifecycle with bounded management receipts, authorization,
  schema/SDK/API/CLI parity, cancellation ownership, and authoritative status
  pass their gates. Retained storage has no OmniGraph byte/file/history limit;
  provider exhaustion fails loudly. The exact upstream enrollment receipt/seal
  remains the preferred simplification and broader-topology gate, not a
  dependency for the current one-live-writer-process profile.
- **Durable heads or checkpoints:** when a new current-authority access shape
  exists, run the full decision instrument before adding production rows or a
  format stamp.
- **Lineage merge deltas:** when both live-row and deletion discovery are
  selective, shadow the new classifier against the existing truth-table path
  and prove cost flat in table size.
- **Exact Optimize provenance:** when Lance exposes the required maintenance
  transaction and the distributed fence exists, replace the bounded adapter
  rather than layering another classifier beside it.
- **Lance 9 stable:** repeat the full upstream diff/spec audit, surface guards,
  compatibility proof, failpoint suite, and cost instruments. Do not infer
  stable behavior from RC.1 by name alone.

## Explicit non-goals for the next slices

Do not:

- build a custom WAL, lock table, transaction manager, or buffer pool;
- restore a mutable in-memory graph tip as commit authority;
- create a second heads/checkpoint authority merely to escape the measured
  manifest-access cost;
- implement RFC-024, RFC-025, or RFC-027 production paths behind a nominal
  feature flag before their blocking gate closes;
- treat the private schema-v9 token/fold core as permission to expose a put/ack
  endpoint before explicit enrollment, correction, persistent lifecycle/status,
  authorization, and product-parity contracts are implemented and evidence-green;
- delete or rewrite `_mem_wal` objects from OmniGraph, or interpret generic
  Lance version cleanup as MemWAL reclamation;
- permit a base-table writer to advance any Phase A lifecycle's HEAD (including
  `SEALED`) before the Phase D witness-update/rebind adapter exists,
  or use a long-lived enrollment tag without first measuring retained files and
  bytes across rewrite/compaction/cleanup;
- treat process-local gates or exact transaction UUIDs as a distributed fence;
- infer table ownership from aliases, paths, matching versions, or compatible
  schemas;
- give mutable control datasets the same cached-session policy as immutable
  version-pinned data; or
- use private Lance APIs to make an upstream lifecycle gap appear closed.

## Evidence map

| Contract | Primary checked-in evidence |
|---|---|
| Lance surfaces and upgrade tripwires | [`lance_surface_guards.rs`](../../crates/omnigraph/tests/lance_surface_guards.rs) |
| No graph-write side doors | [`forbidden_apis.rs`](../../crates/omnigraph/tests/forbidden_apis.rs) |
| Mutation/load atomicity, conflicts, and resource limits | [`writes.rs`](../../crates/omnigraph/tests/writes.rs), [`consistency.rs`](../../crates/omnigraph/tests/consistency.rs) |
| Recovery and crash windows | [`failpoints.rs`](../../crates/omnigraph/tests/failpoints.rs), [`recovery.rs`](../../crates/omnigraph/tests/recovery.rs) |
| Schema identity and schema publication | [`schema_apply.rs`](../../crates/omnigraph/tests/schema_apply.rs), [RFC-028](../rfcs/0028-stable-schema-identity.md) |
| Key fencing and bounded branch adoption | [`merge_fast_forward.rs`](../../crates/omnigraph/tests/merge_fast_forward.rs), [`staged_tests.rs`](../../crates/omnigraph/src/table_store/staged_tests.rs), [RFC-023](../rfcs/0023-key-conflict-fencing.md) |
| Unified validation and merge semantics | [`validators.rs`](../../crates/omnigraph/tests/validators.rs), [`merge_truth_table.rs`](../../crates/omnigraph/tests/merge_truth_table.rs) |
| Native graph-branch controls | [`branching.rs`](../../crates/omnigraph/tests/branching.rs) |
| Maintenance and derived-index visibility | [`maintenance.rs`](../../crates/omnigraph/tests/maintenance.rs) |
| Warm read/write and merge cost at history depth | [`warm_read_cost.rs`](../../crates/omnigraph/tests/warm_read_cost.rs), [`write_cost.rs`](../../crates/omnigraph/tests/write_cost.rs), [`merge_cost.rs`](../../crates/omnigraph/tests/merge_cost.rs) |
| Durable-head decision gate | [`durable_head_lookup_cost.rs`](../../crates/omnigraph/tests/durable_head_lookup_cost.rs), [RFC-024](../rfcs/0024-durable-table-heads.md) |
| Checkpoint-retention Gate 0 | [`checkpoint_retention_cost.rs`](../../crates/omnigraph/tests/checkpoint_retention_cost.rs), [RFC-025](../rfcs/0025-checkpoint-retention.md) |
| MemWAL bounded-enrollment Gate E0 (green decision harness; no row path) | [`memwal_enrollment_gate.rs`](../../crates/omnigraph/tests/memwal_enrollment_gate.rs), [`lance_surface_guards.rs`](../../crates/omnigraph/tests/lance_surface_guards.rs), [RFC-026 §12.1](../rfcs/0026-memwal-streaming-ingest.md) |
| MemWAL Phase A lifecycle/exclusion/recovery | [`failpoints.rs`](../../crates/omnigraph/tests/failpoints.rs), [`forbidden_apis.rs`](../../crates/omnigraph/tests/forbidden_apis.rs), manifest/write-queue unit tests, [RFC-026 §12.2](../rfcs/0026-memwal-streaming-ingest.md) |
| MemWAL private Phase B1 admission/fold/crash and cost evidence | [`memwal_stream.rs`](../../crates/omnigraph/tests/memwal_stream.rs), [`memwal_stream_cost.rs`](../../crates/omnigraph/tests/memwal_stream_cost.rs), worker/recovery unit tests, [RFC-026 §12.3](../rfcs/0026-memwal-streaming-ingest.md) |
| MemWAL Gate R0 retention decision, current-object census, attempt reuse, repaired near-cap closure, and fold RSS tripwire | [`memwal_stream_cost.rs`](../../crates/omnigraph/tests/memwal_stream_cost.rs), [RFC-026 §0.2 and §12.4](../rfcs/0026-memwal-streaming-ingest.md) |
| MemWAL private B2a no-delete structure, provider-failure/orphan behavior, and 1/8/32/128 retained-history instrument | [`forbidden_apis.rs`](../../crates/omnigraph/tests/forbidden_apis.rs), [`memwal_stream.rs`](../../crates/omnigraph/tests/memwal_stream.rs), [`memwal_stream_cost.rs`](../../crates/omnigraph/tests/memwal_stream_cost.rs), shared [`helpers/memwal.rs`](../../crates/omnigraph/tests/helpers/memwal.rs), [RFC-026 §12.5](../rfcs/0026-memwal-streaming-ingest.md) |
| MemWAL private B2 canonical token/admission, stale-authority race, same-generation chain, recovery-v12 base+token fold, and attribution | [`memwal_stream.rs`](../../crates/omnigraph/tests/memwal_stream.rs), manifest/recovery/token unit tests, [`forbidden_apis.rs`](../../crates/omnigraph/tests/forbidden_apis.rs), [RFC-026 §12.6](../rfcs/0026-memwal-streaming-ingest.md) |
| MemWAL B2b reclamation ownership/no-go guards | [`lance_surface_guards.rs`](../../crates/omnigraph/tests/lance_surface_guards.rs), [RFC-026 §4.5.2](../rfcs/0026-memwal-streaming-ingest.md) |
| Cross-version refusal/rebuild, including the genuine final-v8↔v9 gate | [`crossversion_upgrade.rs`](../../crates/omnigraph-cli/tests/crossversion_upgrade.rs), [CI workflow](../../.github/workflows/ci.yml) |

Use [testing.md](testing.md) to find the existing owner before adding coverage.
Decision instruments and production correctness tests serve different purposes:
a green no-go-preservation test means the rejected result was reproduced, not
that the candidate became shippable. RFC-026 Gate E0 is green at a precise
boundary. Fourteen substantive local cells cover the state/negative matrix,
buried-effect refusal, and the exact-version cost proof; baseline versions 8
and 80 have the same complete six-attempt shape (four successful HEADs, one
`NotFound` HEAD, one successful GET) with zero lists. A Unix permissions
tripwire distinguishes exact probing from latest enumeration and forces an
unreadable exact HEAD to error. The configured RustFS cell passes non-vacuously
with the same zero-list shape and the declared positive plus listing-dependent
negative matrix; separate surface guards own S3 ABA and the doc-hidden Lance
surfaces. Phase A subsequently used that proof to activate v7 lifecycle,
recovery, exclusion, and refusal/rebuild. No acknowledgement or fold activation
followed from either gate alone. Private B1 passed its separate gate: the
complete graph-level admission/fold/crash suite now includes post-watcher epoch
loss as `AckUnknown` plus retirement, and the genuine v7↔v8 old/new-binary
refusal/rebuild remains green. The post-containment local warm-ack result stays
flat at 9 reads / 219 bytes; configured RustFS retains only its 2026-07-19
pre-containment baseline and requires rerun before a current object-store
ack-cost claim. Gate R0 first exposed, and the repaired regression cell now
closes, the deterministic high-entropy near-cap shape: acknowledgement and
materialization are followed by one successful fold and manifest publication.
The one/four/eight-fold census and source audit still prove that current LIST
cannot establish a lifetime/provider-billed bound and that RC.1 has no durable
attempt cap or reserve-first complete-output envelope. Those are accepted facts
for unbounded retain-all, not blockers. Private B2a now adds the structural no-
delete guard, shared strict classifier, local/configured-RustFS complete-orphan
provider matrix, local partial-orphan matrix, and 1/8/32/128 cost sweep. Older
roots receive zero IO, canonical durable MemWAL delete requests remain zero,
and only Lance's exact losing manifest-CAS temp staging may be removed. Warm ack
operation shape stays flat while serialized authority and combined retained-
history work grow; those measurements are advisory and create no ceiling. The
private v9 token/fold suite additionally pins canonical digests, typed binding/
sequence/idempotency conflicts, exact retry, same-generation chaining,
post-admission stale-authority refusal, exact two-participant recovery, and
durable fold attribution. The genuine final-v8↔v9 CI strand proves strict
two-way refusal and rebuild fidelity while excluding hidden trusted metadata
from export. The RFC remains Draft and every public surface remains inactive
pending lifecycle/correction/status, authorization, and product contracts.
B2b's two additional guards prove why stock
RC.1 generic cleanup and raw successor-fence-sentinel deletion cannot implement
a later managed profile; they are a no-go boundary for reclamation, not for
retain-all.

## Updating this page

Update this page when any of the following changes:

- a new graph-visible writer or control-path exception is added;
- recovery ownership, support topology, or the manifest visibility boundary
  changes;
- the internal schema or strict rebuild contract changes;
- an RFC moves between draft, research-blocked, accepted, and implemented;
- a Lance bump changes a consumed surface or closes an upstream gate;
- a cost instrument reverses a no-go or reveals a new history-dependent term;
  or
- a known gap is closed or promoted into the public support contract.

Keep detailed mechanics in [writes.md](writes.md), numeric evidence in the
owning RFC/test, and public behavior in the relevant [user docs](../user/index.md).
This page should remain the shortest accurate answer to “where is the write path
now, and what should we do next?”
