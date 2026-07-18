# Write Path: State of Affairs

**Type:** living architecture and execution summary
**Status:** current as of 2026-07-18
**Surveyed:** OmniGraph 0.8.1 development, internal manifest schema v7,
Lance 9.0.0-rc.1 at `cec0b7df`
**Scope:** the direct-publish graph write path, its RFC-022–028 family,
adjacent control and maintenance operations, known blockers, and the next
decision points

**Change-set boundary:** this page describes current main plus the RFC-026
Phase A foundation. RFC-024, RFC-025, and RFC-027 remain research-blocked at
their recorded evidence gates. Gate E0 first proved the bounded public Lance
state classifier; Phase A has now activated internal schema v7 with recoverable
empty enrollment, durable identity-keyed lifecycle authority, process-local
admission/exclusion, and strict format refusal/rebuild. It still adds no
`@stream` syntax, production/public enrollment call, WAL row put or
acknowledgement, fold, drain/resume operation, or fresh-read mode.

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
strategic and draft, but its production-neutral Gate E0 and Phase A foundation
are complete. Internal schema v7 can recover one exact main-only empty MemWAL
enrollment and durably exclude ordinary table/schema/maintenance/repair/recovery
effects for any materialized lifecycle inside one process. Native branch
controls alone may proceed at `SEALED`, because they do not move table HEAD.
That is a format and correctness foundation, not a
streaming product: no production caller can enroll or put a row. Phase B—row
admission, durable acknowledgements, and the first strict fold—is next.

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
    -> acquire stream-admission domains -> schema -> branch -> sorted-table gates
    -> relist recovery and revalidate authority plus physical baselines
    -> durably arm an identity-bearing recovery intent
    -> apply writer-specific Lance effects
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

The five established graph-visible writer classes emit schema-v9 recovery envelopes
before their first independently durable effects. Phase A adds one internal
`StreamEnrollment` adapter with a dedicated schema-v10 envelope. Table ownership is keyed by
`(stable_table_id, table_incarnation_id)`, never inferred from an alias, path,
Lance version, or field ID. Mutation, Load, BranchMerge, SchemaApply, and
EnsureIndices use exact protocols in which `Armed` is rollback-only and
`EffectsConfirmed` may roll forward only under the captured authority. Optimize
is the deliberate exception: its v9 envelope carries identity-bound pins and a
bounded complete-set maintenance classifier, with no exact authority/fixed-
lineage confirmation phase, because Lance exposes no caller-minted maintenance
transaction. Mutation and Load share one writer adapter but retain distinct
sidecar/audit kinds, so the established five classes map to six
`SidecarKind`s`; bounded enrollment is the seventh. It is not yet reachable
from a production API.
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
| Identity / schema | Accepted SchemaIR v2 owns graph-scoped, monotonic, no-reuse type/property/incarnation IDs. Supported renames preserve logical identity and table lifetime; a property rename rewrites that lifetime, while only a pure type rename also preserves physical/index history. Drop/re-add mints a new lifetime. The strict strand serves only internal schema v7 and upgrades by export/init/load rebuild. |
| Key fencing | Every graph table has exact non-null physical `id` as Lance's unenforced PK. Strict insert/upsert use the sealed exact-`id` filtered adapter; bare keyed Append is forbidden; effect-free conflicts have typed reprepare or `KeyConflict` outcomes. |
| BranchMerge | The ordinary ordered diff remains the correctness path. A completely verified `omnigraph.insert_absence = "v1"` history permits the proven-insert shortcut without committing Append or weakening the final filter. |
| Validation | Value, enum, uniqueness, edge-RI, and cardinality use one catalog-derived, delta-scoped evaluator across mutation, load, and merge. Committed non-key uniqueness probes are batched by constraint group and bounded chunks. |
| Recovery | Same-process handles share ordered queues for the same canonical local root or identical normalized opaque remote/custom URI. Stream-admission domains are acquired outside schema → branch → table gates. Mutation/load, SchemaApply, BranchMerge, EnsureIndices, and `refresh` heal roll-forward-only; Optimize, Repair, and Cleanup refuse pending recovery, while branch controls use specialized barriers. Read-write open performs the quiesced full sweep, including exact v10 enrollment completion; read-only open never repairs and refuses an enrollment intent. A resolved intent is audited internally with the original actor when present; no-effect cleanup need not create graph lineage. |
| RFC-026 Phase A | Internal schema v7 owns identity-keyed lifecycle rows and exact empty main/unsharded enrollment. Any lifecycle row—including `SEALED`—rejects base-table, schema, maintenance, repair-adoption, and recovery effects under a process-local admission lease because Phase A cannot yet advance/rebind its witness. Native branch create/delete alone may proceed at `SEALED` because it does not move table HEAD; `OPEN`/`DRAINING` refuses it. Enrollment and open-time validation reject the unsupported named-branch overlap and any uncovered lifecycle/MemWAL mismatch. There is no production enrollment or WAL row path. |
| Lance access | One process-wide `ObjectStoreRegistry` reuses clients. Each `Omnigraph` handle owns its cached data-table `Session`; one process-wide zero-cache control `Session` opens mutable tips. Only the object-store registry is shared between the data and control sessions. This is “cache the past, never the present,” not one global cached session. |
| Maintenance | EnsureIndices stages exact missing-index transactions. Optimize coordinates graph-wide compaction/index work under one bounded recovery envelope. Periodic optimize compacts `__manifest`, but unmaintained history-dependent paths are not globally flat. |

Selected enforced limits are 8,192 rows / 32 MiB per keyed Mutation/Load table,
at most 32 pre-effect full reprepares, 8,192 rows / 32 MiB per BranchMerge
chunk, one aggregate 32-MiB retained merge-validation budget, at most 1,024
logical data transactions per merged table, and a 1,026-version exact-recovery
scan bound. Optimize defaults to eight physical tasks and a five-attempt
compaction retry budget. This is not the complete constants catalog. The
important loud outcomes are `KeyConflict`, `ReadSetChanged`,
`RecoveryRequired`, and `ResourceLimitExceeded`; none reports partial success.
See [writes.md](writes.md) and [constants.md](../user/reference/constants.md) for
retry rules, recovery classification, and the full owned limits.

## RFC implementation state

| RFC | Current disposition | What that means now |
|---|---|---|
| [022 — Unified graph-write protocol](../rfcs/0022-unified-write-path.md) | **Implemented** | The shared correctness state machine, per-writer adapters, recovery barrier, one visibility point, control exceptions, and test lattice are the stable chassis. Completion is scoped to one writer process per graph for destructive recovery. |
| [023 — Key-conflict fencing](../rfcs/0023-key-conflict-fencing.md) | **Implemented** | Internal schema v6 introduced exact-`id` PK metadata, closed keyed routing, typed conflicts, bounded replay, rebuild/refusal, and accepted performance evidence; v7 preserves that contract. |
| [024 — Durable table heads](../rfcs/0024-durable-table-heads.md) | **Research-blocked** | The first in-manifest BTREE candidate has a specified logical contract and flat indexed row/range work, but fails the complete physical-I/O gate. No head rows or heads format are active. |
| [025 — Checkpoint retention](../rfcs/0025-checkpoint-retention.md) | **Research-blocked** | Lance tag/pin semantics pass, but the proposed in-manifest registry access shape is not history-flat after compaction. No checkpoint rows, `ogcp_` production tags, API, or cleanup integration are active. |
| [026 — MemWAL streaming ingest](../rfcs/0026-memwal-streaming-ingest.md) | **Draft; Phase A foundation implemented; Phase B next** | Internal schema v7, recovery-v10 empty enrollment, durable lifecycle CAS, process-local admission/exclusion, partial-format refusal, and strict v6↔v7 rebuild are active. MemWAL remains the strategic substrate. No production enrollment, `@stream`, WAL row put/ack, fold, drain/resume, or fresh-read surface exists. |
| [027 — Lineage merge deltas](../rfcs/0027-lineage-merge-deltas.md) | **Research-blocked** | The desired O(delta) classifier and fallback contract are specified. Selective live-row and deletion-delta discovery are not yet bounded, so `OrderedTableCursor` remains the correctness path. |
| [028 — Stable schema identity](../rfcs/0028-stable-schema-identity.md) | **Implemented** | Rename-stable IDs, table incarnation, identity-derived paths, schema/recovery integration, and strict rebuild activation were introduced in v5 and remain active in v7. |

## Blockers and constraints discovered

| Frontier | Evidence and current consequence | Exit condition |
|---|---|---|
| Distributed recovery fence | Process-local queues cannot stop a live foreign process; Lance restore may orphan its commits and native refs lack conditional compare-delete. Supported destructive recovery remains one writer process per graph. | A separately designed and adversarially tested distributed fence before multi-process writers, background compensation, or cross-process exact maintenance recovery. |
| History-flat authority | RFC-024/025 show flat BTREE rows/ranges/pages can coexist with history-growing manifest discovery or compacted bytes. No heads/checkpoint format is active; mutable tip caches and a second authority remain rejected. | A new Lance-native access shape—or revised measured operational contract—passes the original cold/warm, compacted/uncompacted, local/object-store gate. |
| Internal history GC | Safe live-writer cleanup needs a durable resurrection/retention boundary; otherwise a stalled writer can recreate a collected version. | An evidence-backed cleanup watermark/fence before automated `__manifest` version GC. |
| MemWAL delivery | RC.1 initializes the system index and claims shards as separate effects without a caller-minted combined receipt or cross-process seal. Phase A now recovers that gap exactly for main-only, one-shard, one-live-writer-process empty enrollment and excludes competing local writers. | Implement Phase B row admission/ack plus strict fold without widening topology. The public receipt/seal or a separately accepted distributed fence remains the exit for overlapping processes, multi-shard ownership, and general failover. |
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
object-store evidence; Phase A enforces this reversible support restriction.
The next slice must consume it for durable put/ack and strict folding without
pretending the process-local lease is a distributed fence.

The full known-gap ledger, including adjacent local-CAS and unsupported
multi-version-topology details, remains in
[invariants.md#known-gaps](invariants.md#known-gaps).

## How Lance 9.0.0-rc.1 influenced the plan

RC.1 mostly validated the direction and sharpened gates. It did not justify a
write-path redesign.

| RC.1 finding | Planning effect |
|---|---|
| Lance surfaces consumed by RFC-022/023—transactions, branches, key filters, staged indexes, compaction, and shared sessions—remain compatible; separately surveyed tag/cleanup behavior is also unchanged | Keep the current architecture. PR #364 passed 22 surface guards and 129 runnable failpoint tests; the Gate-0 follow-up adds the 23rd guard plus checkpoint cost evidence. No format redesign is needed. |
| Derived MemWAL datasets now inherit the base store parameters and `Session` | Strengthens the shared-session and remote-credential design and removes one integration concern. It does not create an exact combined enrollment receipt or cross-process seal. MemWAL is strategic, not experimental; Gate E0 passed the narrower exact-version public-state classifier without treating upstream timing as a production plan. |
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

1. **Build RFC-026 Phase B as one bounded vertical slice.** Add row admission
   and durable acknowledgements on the exact v7 binding, then fold a strict
   generation through the existing RFC-022 publish/recovery chassis. Keep it
   main-only, one shard, and one live writer process; do not pull drain/resume,
   fresh reads, dead letters, or multi-shard ownership into this slice.
2. **Instrument the new acknowledgement boundary before quoting a win.** Pin
   batching policy, sparse-traffic delay, object-store operations, ack latency,
   and fold visibility lag. Phase A establishes correctness, not performance.
3. **Keep RFC-024/025/027 stopped at their research no-gos.** Their blockers are
   independent of the v7 stream foundation; do not add heads, checkpoints, or
   lineage-delta format state as incidental Phase B work.
4. **Give a distributed recovery fence its own design and evidence gate.**
   Define authority, expiry/renewal, fencing tokens, and crash semantics before
   implementation; require adversarial multi-process tests on local and object
   storage.
5. **Continue low-risk v7 hardening.** Add missing resource/time budgets,
   preserve cost-at-history-depth gates, and reduce constant factors only where
   the existing authority model remains intact.
6. **Coordinate upstream without making it the calendar.** The useful
   Lance asks are recoverable MemWAL enrollment/admission, conditional native
   ref operations, exact maintenance transaction provenance, and a bounded
   deletion/change-lineage source.

### Only when an evidence trigger fires

- **MemWAL row activation:** Phase A is green. Durable acknowledgements and
  strict upsert/fold activate only when Phase B's put/durability/replay/fold
  crash matrix and acknowledgement cost budget are green. The exact upstream
  receipt/seal remains the preferred simplification and the broader-topology
  gate, not a calendar prerequisite for the bounded slice.
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
- treat the v7 Phase A foundation as permission to expose a put/ack endpoint
  before Phase B owns durability, replay, fold, and cost evidence;
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
| Cross-version refusal/rebuild, including v6↔v7 | [`crossversion_upgrade.rs`](../../crates/omnigraph-cli/tests/crossversion_upgrade.rs) |

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
follows from either gate alone.

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
