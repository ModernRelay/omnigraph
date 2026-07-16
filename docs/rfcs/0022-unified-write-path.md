---
type: spec
title: "RFC-022 — Unified graph-write protocol"
description: One correctness protocol for graph-visible writes, with synchronous recovery, complete read-set arbitration, writer-specific physical-effect adapters, and explicit control-plane exceptions.
status: implemented
tags: [eng, rfc, write-path, manifest, recovery, concurrency, lance, omnigraph]
timestamp: 2026-07-13
owner: OmniGraph maintainers
---

# RFC-022: Unified graph-write protocol

**Status:** Implemented on 2026-07-13
**Date:** 2026-07-12
**Author track:** Maintainer design series
**Surveyed:** OmniGraph 0.8.1 (`main`); Lance 9.0.0-beta.21, git rev `1aec1465`
**Audience:** engine and storage maintainers
**Architecture review:** [RFC-022–028 review ledger](../dev/rfc-022-027-architecture-review.md).
RFC-022's structural rollout and lifecycle findings are closed; the ledger
remains active for RFC-023–027.

**Implementation evidence:** [#343](https://github.com/ModernRelay/omnigraph/pull/343),
[#344](https://github.com/ModernRelay/omnigraph/pull/344),
[#345](https://github.com/ModernRelay/omnigraph/pull/345),
[#346](https://github.com/ModernRelay/omnigraph/pull/346),
[#347](https://github.com/ModernRelay/omnigraph/pull/347),
[#348](https://github.com/ModernRelay/omnigraph/pull/348),
[#349](https://github.com/ModernRelay/omnigraph/pull/349),
[#350](https://github.com/ModernRelay/omnigraph/pull/350),
[#351](https://github.com/ModernRelay/omnigraph/pull/351), and
[#353](https://github.com/ModernRelay/omnigraph/pull/353).

Implementation is complete for the documented single-writer-process support
boundary. This status does not claim distributed destructive recovery. Exact
Optimize provenance remains trigger-gated by §6.4, and MemWAL fold enrollment
remains owned by RFC-026.

**RFC-028 activation update (2026-07-15):** internal manifest schema v5 wraps
every active writer adapter in one identity-bearing recovery schema v9. The
persisted payload field names (`protocol_v3`, `protocol_v4`, `protocol_v7`, and
`protocol_v8`) retain the exact-effect shapes documented below, but their old
sidecar-generation numbers are implementation history, not the active envelope.
Every table pin/effect/delta carries stable table ID + incarnation; recovery
never infers missing identity from an alias. SchemaApply type rename is now
metadata-only on the same identity/path/version; only AddType is a first-touch
create. Optimize retains bounded maintenance provenance inside v9 rather than
claiming an exact caller-minted Lance maintenance transaction.

---

## 0. Summary

OmniGraph has one **correctness protocol** for every operation that changes
manifest-resolved graph state. The protocol does not require every writer to use
the same Lance primitive. A mutation can commit one staged transaction per table,
a branch merge can make several commits to a table, and optimize can use Lance
operations that have no staged API. Writer-specific **effect adapters** describe
those physical operations; one coordinator enforces the common safety rules.

A graph-visible write follows this state machine:

```text
recovery barrier
    → prepare pinned base + complete ReadSet + Effects
    → acquire ordered process-local gates
    → revalidate the complete ReadSet, or restart
    → durably arm recovery
    → apply writer-specific physical effects
    → publish exactly one graph-visible __manifest CAS
    → finalize derived state
```

The protocol has three hard boundaries:

1. Recovery is a synchronous pre-write safety barrier. It may also run in the
   background, but a writer never proceeds merely because recovery was scheduled.
2. Validation and merge classification are valid only for their complete read set.
   A changed probed table causes revalidation or a full restart; the writer never
   refreshes expected versions underneath a plan computed from an older base.
3. “One `__manifest` CAS” applies only to graph-visible commits. Native graph-branch
   ref creation/deletion and physical-only maintenance have explicit, smaller
   control protocols and do not manufacture graph commits.

This RFC deliberately does not combine key fencing, durable table heads,
checkpoint retention, MemWAL ingest, or lineage-based merge-delta discovery into
one format and rollout. They are focused follow-ups:

- [RFC-023 — Key-conflict fencing](0023-key-conflict-fencing.md)
- [RFC-024 — Durable table heads](0024-durable-table-heads.md)
- [RFC-025 — Checkpoint retention](0025-checkpoint-retention.md)
- [RFC-026 — MemWAL streaming ingest](0026-memwal-streaming-ingest.md)
- [RFC-027 — Lineage merge deltas](0027-lineage-merge-deltas.md)

## 1. Scope and authority

### 1.1 Graph-visible writes

A **graph-visible write** changes state resolved through a graph manifest snapshot:

- a node or edge table version;
- a registered or tombstoned table;
- accepted schema identity or schema-visible table metadata;
- a graph commit or graph head;
- any future logical marker that changes query or time-travel semantics.

For such an operation, the only visibility point is one successful `__manifest`
commit containing the entire graph delta. Per-table Lance commits before that point
are physical effects covered by recovery; they are not independently graph-visible.

### 1.2 Control operations

Two classes are not graph-visible commits:

1. Native graph-branch ref create/delete. Lance stores these refs outside the
   dataset-version chain and creates no new `__manifest` version for either action.
2. Physical-only maintenance whose result is content-equivalent and is not selected
   through a data-table pointer in `__manifest`, such as compacting `__manifest`
   itself or reclaiming unreachable files.

Sections 7 and 8 define their control protocols. Branch **merge** and a data-table
optimize whose new version must be published are graph-visible writes and remain in
the main protocol.

### 1.3 What “unified” means

Unified means one set of safety obligations, one coordinator state machine, and one
closed registry of effect adapters. It does **not** mean:

- one storage trait method for every Lance operation;
- exactly one physical commit per table;
- pretending native refs are manifest rows;
- treating process-local queues as a distributed transaction manager;
- moving commit recovery out of the correctness path.

## 2. Protocol objects

The names below are conceptual. Implementations may choose different Rust names,
but they must preserve the represented information and transitions.

```rust
struct PreparedWrite {
    operation_id: OperationId,
    writer_kind: WriterKind,
    target: BranchTarget,
    base: BaseView,
    read_set: ReadSet,
    effects: Effects,
    manifest_delta: ManifestDelta,
    lineage_intent: Option<LineageIntent>,
    recovery: RecoveryPlan,
}
```

### 2.1 `BaseView`

`BaseView` is the immutable state against which the operation was computed. It
contains at least:

- target manifest branch and its incarnation/freshness token;
- pinned manifest version;
- accepted schema identity;
- pinned table entries used by the operation;
- graph head when parentage, merge base, or branch semantics depend on it.

The base is captured only after the recovery barrier completes. A recovery pass may
advance the manifest or promote schema state, so a base captured before recovery is
not valid write input.

### 2.2 `ReadSet`

`ReadSet` is every authority value whose stability is required for the prepared
result to remain correct. It includes, as applicable:

- target branch incarnation;
- accepted schema identity;
- graph head;
- the table head for every table written;
- the table head for every table probed by uniqueness, referential-integrity,
  cardinality, policy-independent structural validation, or merge classification;
- writer-specific authority, such as an enrolled stream's configuration and merge
  generation.

A table belongs in the read set because its value affected the decision, not because
the writer happens to update it. Read-only dependencies are load-bearing.

The publisher must arbitrate every read-set member atomically with the manifest
commit. A fresh pre-check followed by an unconditional write is not a CAS. The
implementation must ensure that a concurrent change after the check contends on a
stable authority row or equivalent substrate token. If the current representation
cannot arbitrate a read-only dependency, that writer has not completed this RFC and
must not ship the outside-gate validation optimization.

### 2.3 `Effects`

`Effects` is an adapter-owned physical plan. Each effect declares:

- physical dataset and Lance branch/ref it targets;
- expected pre-state;
- whether it is staged, inline, ref-only, or zero-commit;
- the possible post-state shape: exact version, bounded range, or adapter-confirmed
  version;
- whether rollback is safe;
- how recovery distinguishes no movement, partial movement, completed movement,
  and an already-published result.

The generic coordinator never assumes `expected_version + 1`. That assumption is
valid for some mutation/load effects and false for branch merge, compaction, index
work, schema metadata changes, and no-op plans.

### 2.4 `ManifestDelta`

`ManifestDelta` is the complete logical result to publish. It contains table-version
journal entries, registrations/tombstones, mutable logical heads when present, schema
identity, and lineage rows as required by the operation. Its logical contents are
immutable for one prepared attempt. Physical version fields may be declared output
slots that the adapter binds to an allowed, confirmed effect result; binding such a
slot cannot widen or otherwise change the logical plan. A revalidation mismatch
discards the delta and restarts preparation.

### 2.5 `RecoveryPlan`

`RecoveryPlan` supplies the writer-specific classifier and compensation/roll-forward
rules. It must be serializable into a recovery sidecar before the first independently
durable physical effect. It is part of the commit protocol, not a best-effort
maintenance hint.

## 3. Normative invariants

1. **Recovery before base capture.** Every graph-visible writer runs and awaits the
   recovery barrier before pinning `BaseView`.
2. **No durable effect before durable intent.** On an existing physical ref,
   reclaimable uncommitted files may be staged first. A first-touch named-table
   transaction is different: Lance writes its uncommitted files into the opened
   branch tree, so Prepare retains the logical batch/predicate and pre-mints its
   transaction identity; after revalidation the recovery sidecar becomes durable,
   then the writer creates the target ref and stages those branch-local files under
   the held gates. In every case the sidecar precedes the first independently
   persistent physical effect, including a native table ref, data-table HEAD
   advance, or native tag/index mutation needed by the graph-visible result.
3. **Complete read-set validation.** Physical effects may start only while the fresh
   authority state equals the complete `ReadSet` used to prepare the operation.
4. **Recompute, do not patch.** On a pre-effect mismatch, discard the prepared plan
   and rerun validation/classification. Never refresh expected versions beneath an
   old plan.
5. **One graph visibility point.** A graph-visible write publishes one manifest CAS;
   no subset of its table effects becomes graph-visible independently.
6. **Adapters own effect truth.** Every effect-producing writer uses a registered
   adapter. There is no generic fallback that guesses version movement or recovery
   safety.
7. **Queues are local optimization.** Ordered process-local gates prevent avoidable
   in-process races and deadlocks. Cross-process correctness comes from Lance
   conflicts, manifest arbitration, and recovery.
8. **Recovery safety is synchronous.** Background sweeping is permitted, but a later
   overlapping writer waits for, performs, or fails on unresolved recovery before it
   can advance state.
9. **Derived work follows visibility.** Expensive index reconciliation, cache warming,
   and orphan reclaim may run asynchronously only when logical correctness does not
   depend on their completion.

These refine, and do not weaken, invariants 2, 3, 4, 5, 7, 9, 13, and 15 in
[`docs/dev/invariants.md`](../dev/invariants.md).

## 4. The graph-write state machine

### 4.1 Stage A — recovery barrier

Before accepting a base, the coordinator discovers pending recovery intents that can
overlap this operation. It must reach one of three outcomes:

1. all relevant intents are fully resolved;
2. each remaining intent is proven already satisfied and can be finalized safely;
3. the write fails with a typed recovery-required or live-writer-contention error.

“Spawn recovery and continue” is not an outcome. Listing sidecars may run concurrently
with non-authoritative I/O, but preparation cannot accept its base until recovery has
completed and the post-recovery schema/manifest state is available.

Recovery must not destructively act on a sidecar that may belong to a live writer
without ownership/fencing proof. It waits, returns contention, or uses a protocol that
proves the prior owner cannot continue.

Recovery classifiers use exact effect identity or confirmed post-state. A numeric
test such as `manifest_version >= observed_lance_head` is sufficient only when the
adapter independently proves lineage containment; version ordering alone is not that
proof. Exact identity cuts both ways: a roll-forward that loses its manifest CAS to
a concurrent writer which already published this intent's exact goal state is
**convergence, not failure** — the barrier records the audit outcome and removes the
intent rather than failing the write (preserving the concurrent-advance convergence
behavior fixed in #296). "Exact" forbids adopting an unrelated newer version; it does
not forbid recognizing one's own goal already achieved.

The barrier is a deliberate availability trade, stated plainly: an unresolvable
overlapping recovery intent — including a live foreign writer's sidecar that cannot
be fenced — fails every overlapping write with a typed error until resolved. Safety
outranks availability here by design; operators observe the condition through the
typed error and recovery telemetry rather than through silently degraded writes.

### 4.2 Stage B — prepare

Prepare runs without writer gates. It may:

- capture `BaseView`;
- evaluate the complete constraint set;
- classify a merge;
- compute embeddings or other deterministic payloads;
- build staged Lance transactions and reclaimable uncommitted objects for targets
  whose physical refs already exist;
- for a first-touch named-table target, retain the complete logical stage input and
  pre-mint its recovery transaction identity without creating the ref;
- construct the complete `ReadSet`, `Effects`, `ManifestDelta`, and `RecoveryPlan`.

Prepare must not advance a Lance HEAD or the graph's native branch refs. Staged files
that are not referenced by a committed Lance manifest are permitted and are
reclaimable if the attempt restarts.

After Stage E arms recovery, a first-touch adapter may create its declared target ref
under the held gates, stage branch-local files on that ref, bind the resulting Lance
transaction to the pre-minted identity, and only then advance HEAD. The ref is itself
an independently durable effect covered by the sidecar; recovery must classify both
sidecar-before-ref and ref-before-HEAD crash states.

Every validator registers its actual committed-state probes in `ReadSet`. Key fences
are an additional same-key conflict signal; they do not replace read-set arbitration
for non-key uniqueness, RI, cardinality, or merge-target stability.

### 4.3 Stage C — acquire ordered gates

The prepared effects declare their gate keys. One attempt acquires the complete set in
this total order:

1. durable global operation claims (migration, retention, or future claims) by
   deterministic claim key;
2. graph/schema-control key, if required;
3. target branch-control key, if required;
4. `(physical_branch, table_key)` keys in deterministic sorted order.

The gates are held through revalidation, recovery arming, physical effects, and the
manifest visibility decision. They may be released after a successful manifest CAS or
after a failed post-effect attempt has safely left its sidecar for recovery.

Acquiring a global claim is coordination, not a graph effect for §3's sidecar-order
rule: the claim record must itself contain an owner/fencing token and an explicit
crash-release/takeover contract. No data HEAD, tag, index, or logical authority may
move merely because the claim was acquired.

The merge-exclusive mutex may protect a coordinator swap, but it is not a semantic
cross-process lock and must not substitute for a target read set.

### 4.4 Stage D — revalidate or restart

With all gates held, the coordinator loads fresh authority state and compares every
member of `ReadSet`.

Revalidation also proves the physical pre-state that the new recovery intent would
claim. For every existing table ref in the effect envelope, live Lance `HEAD` must
equal the prepared manifest pin. `HEAD < pin` is an internal invariant failure;
`HEAD > pin` belongs either to an already-durable recovery intent or to uncovered
drift that requires explicit repair. The attempt must reject that state **before**
writing its own sidecar; a new sidecar may never retroactively claim a pre-existing
physical effect. A first-touch ref is the deliberate exception: it does not exist at
this point and follows the sidecar-before-ref protocol in §4.3 instead.

- If all members match, the attempt may arm recovery.
- If any member differs, no physical effect may run. The attempt releases its gates,
  discards staged state, and restarts from Prepare.
- A strict API may return a typed conflict instead of retrying, but it may not publish
  the stale plan.

Retries are bounded and observable. Retrying a merge means recomputing the merge base
and reclassifying against the new target. Retrying a validation-sensitive mutation
means rerunning the validators, including probes of tables the mutation does not
write.

### 4.5 Stage E — arm recovery

After successful revalidation, write the recovery sidecar durably before the first
independently durable physical effect. An effect-free or authority-first workflow
described in §4.9 may omit the sidecar. Otherwise the sidecar contains at least:

- operation id, writer kind, actor, and target branch/incarnation;
- pinned schema identity and complete read set;
- every physical target and expected pre-state;
- adapter recovery strategy;
- intended manifest delta and lineage intent, or a durable reference to them;
- confirmed post-state once the adapter reaches its all-effects-complete boundary.

A multi-step adapter first records its pre-state plan. After all its physical effects
finish, it durably records the exact confirmed post-state before manifest publish.
Until that confirmation exists, recovery treats the effect set as possibly partial.

### 4.6 Stage F — apply effects

The adapter applies its declared physical effects. A failure after recovery is armed
leaves the sidecar intact. The request must not delete it, silently adopt live HEAD, or
start a fresh plan around it.

The adapter returns exact achieved state to bind the physical output slots declared by
`ManifestDelta`. If achieved state differs from the prepared effect envelope, the
operation fails into recovery rather than widening its plan in place.

### 4.7 Stage G — manifest CAS

A graph-visible operation performs exactly one `__manifest` CAS carrying its complete
logical delta and lineage when the plan has `LineageIntent`. Metadata-only plans carry
their explicit authority/operation rows without manufacturing graph lineage. On every
CAS attempt, the commit authority re-reads and arbitrates the complete `ReadSet`.

The following cases are distinct:

- **Conflict before any physical effect:** safe bounded restart from Prepare.
- **Conflict after physical effects:** recovery case. Keep the sidecar; do not simply
  re-stage or point the manifest at whatever HEAD is now live.
- **CAS success:** the graph commit is visible atomically.

When durable table heads land under RFC-024, tombstoning a table must update its mutable
head to an explicit deleted state in this same CAS. A stale live head plus an immutable
tombstone history is not a valid O(tables) current-state representation.

### 4.8 Stage H — finalize

After CAS success:

- delete the sidecar best-effort when the workflow has one;
- refresh/invalidate process-local views;
- enqueue derived index reconciliation and orphan reclaim;
- record recovery/audit completion when applicable.

Sidecar deletion failure does not turn a durable successful graph commit into a user
error. The next recovery barrier proves the exact intent satisfied and removes the
artifact.

### 4.9 Authority-first control workflows

Some metadata workflows have no independently durable physical effect before their
manifest CAS. They may use an **authority-first** subtype:

1. run the recovery barrier, prepare, acquire gates/claims, and revalidate exactly as
   above;
2. publish the metadata transition as the first durable effect;
3. perform only idempotent work whose desired target is fully encoded by that
   transition and whose interruption cannot expose incorrect graph data.

No generic sidecar is required before step 2 because there is no pre-authority effect
to recover. The authority row itself is the durable recovery cursor. Checkpoint
deletion, a GC-boundary publish, and `OPEN -> DRAINING` stream intent are candidate
examples; each follow-up RFC must prove its post-CAS work is convergent and safe.

A long control workflow is not one giant RFC-022 attempt. Each lifecycle transition,
fold, schema/branch operation, and resume transition is a separate prepared write or
native-ref control step with its own read set and visibility point. If a later phase
needs a non-idempotent or independently visible effect not fully described by the
authority row, that phase uses a normal sidecar before the effect.

## 5. Crash contract

| Crash point | Required result |
|---|---|
| Before sidecar | No independently durable effect occurred; uncommitted objects are reclaimable. |
| Sidecar durable, no effect | Recovery aborts/finalizes the empty intent. |
| Some effects applied, not confirmed | The adapter rolls back, completes, or refuses safely according to its declared strategy. |
| All effects confirmed, before manifest CAS | Recovery rolls forward the exact confirmed manifest delta, or applies the adapter's explicit all-or-nothing rule. |
| Manifest CAS succeeded, sidecar remains | Recovery proves the exact intent visible, audits it, and removes the sidecar. |

Rollback is not assumed safe. Lance `Restore`, schema-file promotion, native refs, and
content-replacing operations have different concurrency properties; each adapter must
state which recovery direction is legal and under what fencing.

## 6. Writer-effect adapters

The adapter registry is closed by default: adding a graph-visible writer requires a
new adapter or an explicit use of an existing adapter. Code review and tests must be
able to enumerate every adapter and every entry point that invokes it.

### 6.1 Mutation and load

- Construct one staged effect per touched table where the Lance API permits it.
  Existing-table effects may stage in Prepare; first-touch named-table effects use
  the sidecar → target ref → branch-local stage ordering above.
- Put every uniqueness, RI, and cardinality probe in `ReadSet`.
- Revalidate or restart when a probed-but-untouched table changes.
- Preserve strict replacement semantics for overwrite/delete.
- Treat key-conflict fencing and strict keyed Append semantics as RFC-023 concerns;
  no fence is credited as protection until that RFC's rollout gates pass.

### 6.2 Branch merge

- Capture the exact source graph commit/snapshot and compute row classification
  against that immutable input outside the effect gates. The source commit is an
  effect precondition, not a member of the target publisher's atomic `ReadSet`:
  a target CAS cannot arbitrate a foreign source-branch row.
- Revalidate that the captured source incarnation and commit still identify the
  prepared input before effects. A later source-head advance is intentionally
  harmless: merge means "merge this captured source commit," not "whatever is
  latest when the target publishes." A future latest-at-publish contract would
  require a real source-branch fence held through target CAS.
- Include the target graph head and every target table used by classification or
  validation in `ReadSet`.
- Any target change before effects forces a complete reclassification; publishing a
  result computed against an old target and parenting it to a new live head is
  forbidden.
- The adapter supports zero, one, or several physical commits per table and records
  exact confirmed post-state before manifest publish.
- Lineage-based candidate discovery may replace the classifier only under RFC-027;
  this protocol does not assume it is O(delta).

### 6.3 Schema apply and storage migration

- Acquire the schema-control gate before effect application.
- Capture the main native branch identity, exact optional `graph_head:main`, and
  accepted schema identity; include every affected table in `ReadSet`.
- Pre-mint the original graph commit (including the initiating actor) and rollback
  commit. Cover schema staging-file promotion, exact table effects, registrations,
  updates, tombstones, and final schema identity with one recovery intent and one
  complete manifest delta.
- A non-noop schema change still needs that recovery intent when it has no table
  effects: schema-contract staging and promotion are independently durable state,
  so an empty table-pin set means “metadata-only SchemaApply,” not “effect-free.”
- The active schema-v9 envelope's retained `protocol_v7` payload plans one exact
  Lance transaction identity per independently durable effect: `Overwrite` for an
  existing table and a strict read-version-zero create for each AddType target. A
  pure type rename is metadata-only and preserves identity, path, version, and
  index history. These commits use zero transparent conflict retries; the achieved
  identity and version must equal the plan.
- The durable sidecar starts `Armed`. After every exact table effect and all three
  schema staging files are durable, one sidecar replacement confirms the achieved
  identities and the complete registration/update/tombstone delta, transitioning
  to `EffectsConfirmed`. `Armed` is rollback-only; `EffectsConfirmed` may roll
  forward only under the captured authority token.
- Publish the fixed original manifest outcome before promoting schema staging.
  A crash during promotion is completed only after recovery proves that exact
  commit and delta visible. Rollback discards target staging; an owned first-touch
  version-one dataset is deleted so a retry can recreate it.
- Recovery never treats a numerically newer table version as SchemaApply output.
  A disjoint graph-head winner is preserved while owned effects are compensated;
  an owned effect buried beneath same-table foreign movement is unverifiable and
  fails closed with the sidecar intact. A foreign winner at an unregistered
  first-touch path is preserved but never adopted or deleted; recovery may still
  compensate this attempt's other exact owned effects and retire its intent.
- Read-only open performs no recovery writes. If the fixed original manifest
  outcome is visible before the target schema identity is fully promoted, it
  fails with `RecoveryRequired` rather than exposing a torn manifest/catalog pair.
  An unparseable recovery sidecar remains tolerated only when no schema-staging
  artifact exists; their coexistence also fails closed because the sidecar may
  be the missing SchemaApply outcome proof.
- In-process query, export, graph-index, and blob-read entry points capture their
  manifest snapshot and an operation-local catalog rebuilt from the accepted
  contract under the schema-control gate; a stale handle ArcSwap is not
  authoritative. A read that starts before the apply keeps the old pair; one that
  overlaps publication waits for the fully promoted pair. This is process-local;
  a long-lived reader in another process still needs the distributed
  schema-publication fence called out in the remaining concurrency limitations.
- The parser retains historical schema-v5 bridge shapes for frozen fixtures, but
  RFC-028 does not provide an in-place upgrade path for genuine pre-v9 artifacts:
  those files lack explicit table identity and are refused before classification.
  They are never reinterpreted as active `protocol_v7` ownership proof or admitted
  by matching aliases, paths, target hashes, or versions.
- Write the sidecar before the first table HEAD advance, including unenforced-PK
  metadata backfill or other inline metadata commits.
- A branch-wide or graph-wide migration must enumerate every physical manifest/data
  branch it changes; updating main does not implicitly migrate older branch manifests.

### 6.4 Data-table optimize and index work

- The adapter may describe zero or multiple inline, content-preserving Lance commits.
- It records the exact achieved version rather than assuming one version of movement.
- If the new data-table version is selected through `__manifest`, publishing that
  pointer is a graph-visible commit and uses this protocol.
- EnsureIndices uses the active schema-v9 envelope with its retained exact
  `protocol_v8` payload. It plans every missing BTREE,
  FTS, and full-table vector artifact for one table against one pinned base and
  combines them into one staged `Operation::CreateIndex`, so a touched table
  advances exactly once. Before effects it captures native branch + exact graph
  head + schema authority, fixed original/rollback lineage, the exact transaction
  identity per table, and the complete table-pointer delta. First-touch named refs
  remain sidecar-before-ref and bind their exact Lance ref identity at
  confirmation.
- `Armed` `protocol_v8` effects are rollback-only. After every exact transaction lands at
  `read_version + 1`, the writer atomically records `EffectsConfirmed` with the
  complete fixed delta; only that state may roll forward under the unchanged
  authority token. Foreign movement is never adopted, and an owned effect buried
  by a same-table winner fails closed.
- The parser retains schema-v6 EnsureIndices shapes as historical fixtures only.
  A genuine identity-less v6 file is refused under RFC-028 and is never
  reinterpreted as `protocol_v8` ownership proof or upgraded by alias inference.
- Optimize uses one graph-wide schema-v9 identity envelope with a bounded
  maintenance payload. After its broad entry probe it acquires
  schema → main branch → every accepted-catalog table gate, loads one
  operation-local accepted catalog, relists recovery, and plans productive work
  from a fresh snapshot. One multi-pin sidecar covers every productive table;
  physical effects remain bounded-parallel, then one monotonic batch CAS publishes
  every still-needed pointer and one lineage commit. Maintenance-class monotonicity
  is load-bearing: a current pointer already at or beyond an achieved version is
  omitted, not rejected by strict graph-head/read-set OCC. Post-arm failure leaves
  the shared sidecar and returns `RecoveryRequired`; Full recovery rolls a
  complete effect set forward in one batch or compensates a partial set. Main stays
  held through final physical `__manifest` compaction. The bounded payload has no
  transaction/authority/fixed-lineage proof, so this supported adapter remains
  inside the single-writer-process recovery boundary and is not a distributed
  fence. Replacing it with exact provenance is deliberately deferred until both
  Lance exposes a stable public caller-controlled transaction API for the complete
  compact/reindex operation and OmniGraph has distributed recovery ownership or
  fencing. Exact identities without the latter would still leave destructive
  cross-process recovery unsafe.
- Logical operations never fail because a derived index is absent or behind.
- Physical-only internal-table maintenance remains the exception in Section 8.

### 6.5 MemWAL fold

RFC-026 owns enrollment, acknowledgement, quiescence, fresh-read semantics, and the
public ingest surface. Any fold that becomes graph-visible is an adapter here:

- fold-time validation contributes its complete read set;
- Lance `merged_generations` changes atomically with the base-table data commit;
- the sidecar covers the data-commit-to-manifest gap;
- one successful manifest CAS makes the folded graph state visible.

## 7. Native graph-branch ref control protocol

Creating or deleting a graph branch mutates a native Lance branch ref for
`__manifest`. Lance specifies that these operations do not generate a dataset version.
There is no target branch on which to publish before create, and no target remains on
which to publish after delete. They therefore cannot truthfully be instances of the
graph-visible manifest-CAS protocol.

The native control is not one physical mutation. At the pinned Lance revision:

- create first commits a shallow-cloned branch dataset under `tree/{branch}` and
  then writes `BranchContents`; the two phases are non-atomic and
  `BranchContents` is the sole logical authority;
- delete removes `BranchContents` first and then reclaims the branch directory.

Accordingly, a clone without `BranchContents` is unreachable physical garbage,
while an absent `BranchContents` after delete means the logical delete succeeded
even if directory cleanup returned an error.

Lance maps slash-separated branch names into nested physical directories and will
not reclaim an ancestor directory while a live descendant name exists. Live graph
branch names are therefore path-prefix-disjoint: `review/2026` is valid, but it
cannot coexist with a live `review` branch. This is checked before native create,
and legacy overlapping names must be deleted leaf-first. Without this namespace
invariant, `force_delete_branch` may return success while deliberately leaving an
ancestor clone-only dataset in place.

Their control protocol is:

1. run and await the recovery barrier;
2. quiesce enrolled streams as required by RFC-026;
3. acquire any active global claim, such as RFC-025's retention claim, and then
   the graph/branch-control gate in §4.3 order;
4. freshly capture source ref/version/incarnation and target absence (or the
   delete target's exact `BranchIdentifier`);
5. validate a create name and the path-prefix-disjoint namespace before Lance's
   shallow-clone phase;
6. execute the bounded native classifier below; and
7. reclaim owned per-table forks best-effort while the complete control-gate set
   remains held, then release the gates. A reclaim failure is logged for the
   cleanup reconciler; this implementation does not schedule asynchronous reclaim.

| Operation | Fresh physical state | Classifier action |
|---|---|---|
| create | no ref, no clone | idempotent force-reclaim (no-op), then create |
| create | clone only | force-reclaim the unreachable clone, then create |
| create | matching ref + openable clone observed after this invocation's ambiguous native result | complete; accept a lost acknowledgement |
| create | mismatching/broken ref | conflict; never adopt or delete it |
| delete | captured identifier still present | not deleted; return the native error |
| delete | ref absent, directory remains | logically deleted; retry reclaim best-effort |
| delete | ref and directory absent | complete |
| delete | different identifier present | delete/recreate ABA; conflict, never delete it |

Create retries the native call at most once after reclaiming an absent-ref tree.
It never adopts a matching ref that was already present before the invocation;
that remains ordinary `AlreadyExists` because there is no operation marker.
Matching completion requires the expected parent branch and version plus a target
identifier whose mapping is exactly the captured parent identifier followed by one
new `(parent_version, uuid)` element; the target dataset must also open. Lance does
not let the caller pre-mint that UUID, so the proof is intentionally scoped to the
supported single-writer-process boundary.

No separate graph-branch sidecar is written. Under the held target gate,
path-prefix-disjoint namespace, and supported topology, absent `BranchContents`
means there is no logical branch and any same-name tree is safe derived garbage;
the next same-name create is the targeted, idempotent reconciler. A recovery
sidecar would introduce a second authority and would incorrectly pull native
controls into graph lineage. First-touch **data-table** forks are different: their
mutation/load sidecar is already durable before branch creation. Full recovery
force-reclaims an absent-ref target as either a no-op (crash-before-clone) or that
intent's clone-only zombie before retiring the intent.

One compatibility case deliberately defers that cleanup. A legacy graph may
already contain path-prefix-overlapping live names. If an unresolved first-touch
intent targets an ancestor table tree while a live path-child still exists, Full
recovery leaves the sidecar durable instead of recursively deleting the child's
storage. Read-write open may complete for leaf-first remediation only when the
intent owns no physical table effect. If a multi-table attempt owns any effect
while another untouched fork is blocked by the child, rollback is
complete-or-error: open fails closed, the sidecar remains authoritative, and an
existing handle or offline Lance-level branch tool must remove the descendant
before the next Full recovery reclaims the untouched fork, compensates the owned
effect, and retires the sidecar. This distinction prevents legacy writers outside
the v3 barrier from preparing against an unresolved partial rollback.

Delete has one recovery disposition that create does not: after the complete
schema/target-branch/all-table gate set has waited out any live in-process owner,
an unresolved sidecar scoped to the branch being removed may be rendered
unreachable by the native ref deletion. A later heal records the orphan-discard
audit and retires it. Graph-global schema recovery still blocks the control, and
create/merge may not adopt this exception.

The native ref operation itself should enforce the freshly checked precondition or
surface concurrent ref mutation as a conflict — but at the pinned Lance revision it
does not: branch-ref creation is an existence check followed by an unconditional
put, and delete is not compare-and-delete by `BranchIdentifier` (the same substrate
gap for which RFC-025 §2.3 rejects a branch ref as a claim mechanism). Until Lance
ships conditional/CAS ref mutation, graph-branch create/delete therefore inherit the
documented single-writer-process support boundary — the same disposition RFC-023
§10 applies to recovery ownership — and multi-process branch operations are not
advertised. The upstream ask for a conditional ref primitive is filed alongside
this RFC; a process-local branch gate remains a local optimization, not the missing
cross-process guarantee.

These operations do not emit a synthetic graph commit. If a future product contract
requires a native ref mutation and manifest/audit rows to become atomic together, it
needs a separate multi-authority recovery protocol; this RFC does not claim an
atomicity the substrate does not provide.

This exception applies only to graph-level create/delete. Branch merge is a
graph-visible write. Lazy per-table forks created while preparing a branch write are
declared physical effects of that writer and remain subject to its recovery/reclaim
contract.

## 8. Physical-maintenance control protocol

Physical work that does not change manifest-resolved logical graph state does not
create graph lineage merely to fit the main protocol. Examples include:

- compacting `__manifest`, which is itself the authority and is read at its Lance HEAD;
- deleting versions/files already proven unreachable under the active retention
  contract;
- reclaiming orphaned branch refs or uncommitted objects;
- rebuilding derived physical state when no graph-visible data-table pointer changes.

Such work must still be idempotent, bounded, observable, and safe under concurrent
native Lance commits. It uses substrate conflict/retry semantics appropriate to the
operation. It must never expose partial logical graph state.

The exception is from graph-lineage publication, not from recovery safety. Maintenance
must run the recovery barrier or refuse before it can replace or delete an artifact
named by an unresolved sidecar. It must also acquire any relevant process-local gates;
as elsewhere, those gates are an optimization rather than cross-process authority.

Data-table compaction/index work that advances a version which graph reads must select
through `__manifest` is not exempt; Section 6.4 applies. Checkpoint reachability and
the mapping from graph checkpoint rows to Lance-native GC pins belong to RFC-025.

## 9. Concurrency and retry semantics

Process-local gates reduce same-process races and establish one deadlock-free order.
They do not coordinate two servers or CLIs. A conforming implementation remains safe
if every process has its own gate manager.

Cross-process safety comes from:

- complete read-set arbitration at the manifest authority;
- Lance transaction conflicts for physical table effects;
- durable recovery before physical HEAD movement;
- refusal to continue past unresolved overlapping recovery.

Retry rules are phase-specific:

- before effects, a read-set mismatch discards and recomputes the whole attempt;
- while applying an effect, the adapter may retry only when Lance guarantees the
  operation can be safely replanned from fresh physical state;
- after any effect, manifest contention is resolved through the armed recovery intent,
  not by silently rebasing the logical plan;
- retry exhaustion returns a typed, observable conflict.

## 10. Rollout and compatibility

This RFC authorizes a protocol refactor, not a manifest v5 format moment. RFC-023
through RFC-028 own their respective format and public-surface changes.

It also does not require a mutable-tip `GraphState` singleton. Three measured,
local latency fixes can land independently of the adapter conversion:

1. make the graph's shared Lance `Session` a required parameter of every
   manifest open/publisher path, so remote opens do not rebuild clients and cold
   metadata state;
2. capture one immutable operation-local manifest/lineage view and pass it down
   the call stack instead of reopening the same state repeatedly; and
3. remove the verified-redundant branch-idle refresh and the back-to-back second
   `branch_delete_as` refresh once their existing coverage asserts unchanged
   behavior.

These are narrow access-shape fixes, not a second commit-input authority. They
must preserve snapshot pinning and still cross the recovery/read-set barriers
defined above.

The implementation landed in this order:

1. `PreparedWrite`, `ReadSet`, effect-adapter, and recovery-plan concepts were
   introduced while preserving existing behavior.
2. Conservative branch-wide arbitration shipped first. Mutation/load captures
   `(Lance BranchIdentifier, exact optional graph_head, accepted schema identity)`;
   every publisher retry compares that token instead of reparenting. Because every
   supported graph-content and schema apply advances `graph_head:<branch>` before
   schema promotion, the shared head row atomically arbitrates probed-but-untouched
   same-branch dependencies. The native branch identifier detects delete/recreate
   ABA under the documented single-writer-process branch-control boundary. RFC-024
   later narrows false contention with table heads; it is not a correctness
   prerequisite for this coarse step. Existing live committed-state validation
   probes remain until the narrowed read set replaces them.

   > **Implementation note (updated 2026-07-12):** mutation/load now use this coarse
   > token, schema-v9 sidecars carrying the retained exact `protocol_v3` payload,
   > fixed lineage/rollback outcome ids,
   > zero transparent Lance commit retries, and bounded full reprepare before
   > effects. Branch merge now captures an immutable source commit/snapshot and
   > the target coarse token, computes its merge base from those captured ids,
   > and revalidates target authority plus source incarnation under the ordered
   > gates. Its schema-v9 recovery envelope's retained `protocol_v4` payload
   > distinguishes multi-commit HEAD
   > effects from first-touch ref-only forks, persists an ordered pre-minted
   > Lance transaction chain for every logical data effect plus fixed
   > merge/rollback lineage, and confirms the complete manifest delta (including
   > pointer-only slots) before publishing with `ExactGraphHead`. Those data
   > transactions commit with zero transparent conflict retries; recovery accepts
   > only their contiguous exact prefix (plus a derived `CreateIndex` tail after
   > the complete chain) and fails closed on foreign movement. A pre-effect target change
   > returns `ReadSetChanged`; any post-arm failure returns `RecoveryRequired`
   > and recovery never re-parents onto a target winner. SchemaApply now uses the
   > schema-v9 envelope's retained `protocol_v7` exact authority, fixed
   > actor-bearing lineage, exact existing-table
   > overwrite and first-touch create identities, and a complete confirmed
   > registration/update/tombstone delta. Its `Armed` attempts roll back, its
   > `EffectsConfirmed` attempts roll forward only under the captured token, and
   > schema staging is promoted only after the fixed manifest outcome is visible.
   > Owned first-touch paths are reclaimed on rollback. An unregistered foreign
   > first-touch winner is preserved but never adopted; foreign movement on an
   > existing manifest-owned table or a buried same-table effect fails closed.
   > Read-only open also refuses the fixed-manifest/pre-promotion window without
   > mutating it. Historical schema-v5 bridge shapes remain parser fixtures, but
   > identity-less artifacts are refused rather than upgraded. EnsureIndices now
   > uses the schema-v9 envelope's
   > retained `protocol_v8` exact authority,
   > one mixed CreateIndex transaction per table, fixed lineage/delta, exact
   > first-touch ownership; schema-v6 remains a historical parser fixture, not an
   > identity-upgrade path.
   > Optimize now has one graph-wide recovery/visibility envelope: one bounded
   > payload in an identity-bearing schema-v9 multi-pin sidecar, bounded-parallel
   > physical effects, and one maintenance-class
   > monotonic batch publish. Exact provenance is deferred until Lance exposes a
   > stable public caller-controlled maintenance transaction API and OmniGraph has
   > distributed recovery fencing; it is not an RFC-022 rollout gate. MemWAL is the
   > supported, strategic substrate selected by
   > RFC-026; its separate fold enrollment remains owned by that RFC.

   > **Branch-control/cleanup slice (2026-07-11):** native graph-branch create
   > and delete now use the §7 authority classifier, including pre-clone name
   > validation, prefix-disjoint live names, bounded clone-only recovery,
   > lost-acknowledgement classification, exact delete-incarnation fencing, and
   > no synthetic graph lineage. First-touch Full recovery also reclaims an
   > absent-ref clone-only table fork. Cleanup now protects every exact lazy
   > branch pin, computes `keep` from Lance's actual version list, refuses
   > uncovered main HEAD drift, and completes its live-root preflight before the
   > first table GC.
3. Every current graph-visible effect writer was converted. This is structurally complete
   as of 2026-07-13: Mutation/Load, BranchMerge, SchemaApply, and EnsureIndices use
   exact adapters; Optimize uses the bounded schema-v9 adapter described in §6.4.
   The deferred exact Optimize upgrade is not an RFC-022 rollout gate.
4. The supported writer set was closed. This is complete as of 2026-07-13:
   raw storage/coordinator/handle-cache modules are crate-private and public
   snapshots expose a read-only table/scan facade without Lance's raw scanner
   or physical plan. `crates/omnigraph/tests/forbidden_apis.rs` adds a
   defense-in-depth registry over public async inherent `Omnigraph` methods and
   loader conveniences, every crate-visible async low-level coordinator method,
   and exact per-file occurrences of registered durable-call shapes (including
   recovery). A new supported surface or durable gateway fails the guard until
   it is assigned an adapter or explicit exception. The source scanner covers
   the concrete method/UFCS/raw-Dataset and selected rename/macro shapes pinned
   by its self-tests; Rust visibility, not heuristic macro expansion, is the
   structural closure.
5. Superseded orchestration was removed after its crash and concurrency cells passed
   through the adapter. The old bypass surfaces are removed; writer-specific physical-effect
   implementations remain intentionally behind their registered adapters.
6. Further work is post-close-out latency or capability work: preserve the
   synchronous barrier and recovery classifications, and require the two §6.4
   triggers before replacing Optimize's bounded adapter.

The close-out intentionally narrows `TableStore`, `TableStorage`, the raw table
handle cache, `GraphCoordinator`, and `ManifestCoordinator` to crate visibility;
public `Snapshot::open` now yields a read-only table facade and safe scan builder
instead of Lance's writable `Dataset` / plan-exposing `Scanner`. That is a downstream Rust
source break and therefore lands on the v0.9 line, where registry publication
resumes; it has no CLI, wire, or storage-format effect. SDK callers use the
registered `Omnigraph` and snapshot-read surfaces instead. The retired low-level
methods expose writable Lance handles or bypass policy, recovery barriers, and
ordered writer gates, so they are not supported alternatives to the unified path.
The `failpoints` Cargo feature retains one doc-hidden, registry-classified
`TestOnly` manifest-publish helper solely for adversarial integration fixtures;
enabling fault injection is an explicit non-production exception to the SDK
surface described here.

Mixed writer binaries are not made safe by process-local gates. A deployment may
enable the new protocol only when every writer that can reach the graph obeys the same
sidecar and manifest-arbitration contract, or when a compatibility gate rejects older
writers.

## 11. Required tests and cost gates

### 11.1 Protocol conformance

- Keep the shipped boundary and registry in
  `crates/omnigraph/tests/forbidden_apis.rs` exhaustive for their declared
  surface: raw storage/coordinator types stay crate-private; public snapshots
  stay read-only without exposing raw scanners/plans; every public async inherent `Omnigraph`/loader entry point and
  crate-visible async coordinator method is classified; and every registered
  durable-call shape, including recovery execution, keeps its exact per-file
  count.
- Assert no sidecar-backed adapter can create an independently durable physical
  effect before its sidecar is durable.
- Enumerate authority-first workflows and assert their CAS is the first durable
  effect and every post-CAS action is idempotently derivable from its authority row.
- Assert one graph-visible operation produces exactly one manifest visibility commit.
- Assert branch create/delete and physical-maintenance exceptions produce no synthetic
  graph lineage.

### 11.2 Read-set races

- Two distinct ids racing on the same non-key `@unique` value cannot both publish.
- An edge insert racing deletion of an endpoint must revalidate or conflict.
- Cardinality probes of an untouched table participate in arbitration.
- A target advance after merge classification forces complete reclassification.
- A source branch advancing after capture does not silently substitute its new
  head: the merge either uses the captured source commit or fails its pre-effect
  source-incarnation/commit check. A latest-at-target-publish variant requires a
  real source fence and is not inferred from the target `ReadSet`.
- Run the same cells with separate `Omnigraph` handles sharing one root-scoped
  process-local gate manager, then with separate processes that do not share it.

### 11.3 Recovery

- Fail before sidecar, after sidecar, after each physical effect, after confirmation,
  after manifest CAS, and during sidecar deletion for every adapter.
- A later overlapping writer blocks, heals, or returns recovery-required; it never
  advances around the sidecar.
- If Optimize's entry probe passes and a SchemaApply or table-disjoint main intent
  arms before its branch gate is acquired, Optimize returns recovery-required with
  that intent's id; it does not create an Optimize sidecar, move another table,
  advance `graph_head:main`, or compact `__manifest` afterward.
- After Optimize's under-gate relist passes, a table-disjoint main writer started
  while productive table compaction is paused remains queued until Optimize
  releases its branch-wide effect envelope, then completes normally.
- A live foreign writer's sidecar is not destructively recovered without fencing.
- Recovery proves exact effect identity; a numerically newer unrelated version is not
  accepted as proof of ancestry.
- SchemaApply recovery preserves the initiating actor and fixed original lineage,
  rolls back an exact partial multi-table prefix, reclaims an owned first-touch
  dataset, and completes a crash after only part of schema staging was promoted.
- A live query on the applying handle waits across the fixed-manifest/catalog-swap
  window and then captures one coherent pair.
- A disjoint post-effect SchemaApply winner remains the rollback parent and keeps its
  table pin; a same-table winner burying the owned overwrite fails closed without a
  restore, manifest movement, or sidecar deletion.

### 11.4 Adapter-specific truth tables

- Mutation/load: zero/one table, multi-table, strict, non-strict, and
  probed-but-untouched dependency cases.
- Merge: adopt, rewrite, multi-commit, no-op, target advance, and partial-Phase-B crash.
- Schema: staging files (including partial promotion), registration-only, metadata
  HEAD advance, exact existing overwrite, strict first-touch create, fixed actor
  lineage, partial multi-table migration, foreign-winner races, and branch-local
  state.
- EnsureIndices: zero-work/no-sidecar, one or several touched tables, one mixed
  CreateIndex transaction and one version advance per table, pre-effect authority
  loss, exact confirmation, first-touch sidecar ordering/identity, partial-Phase-B
  rollback, confirmed roll-forward, and foreign/buried winner refusal. Retain a
  frozen schema-v6 parser fixture plus explicit refusal of identity-less input.
- Optimize: zero, one, and several Lance commits; two productive tables become
  visible through one lineage/manifest CAS; a complete multi-table effect set
  rolls forward together; a partial set exposes no pointer and compensates under
  one identity-bearing v9 sidecar; monotonic publish, retryable physical contention, no-work/no-sidecar,
  lost acknowledgement after the manifest CAS, pending-only vector exclusion, and
  history-flat manifest cost remain pinned. Exact provenance is a deferred upgrade,
  triggered only by a stable public Lance maintenance-transaction API plus
  distributed OmniGraph recovery fencing.
- MemWAL fold (supported strategic substrate; enrollment owned by RFC-026):
  merged-generation conflict and every fold crash boundary.
- Native graph branch control: invalid name before clone; live path-prefix
  collision before clone; clone-only create recovery on main and a named source;
  pre-existing matching ref remains `AlreadyExists`; current-call lost create
  acknowledgement is accepted only with exact parent/incarnation metadata and an
  openable target; delete with absent ref plus remaining tree is success and
  reclaims best-effort; same identifier preserves the native error; a recreated
  identifier is a typed conflict and survives; neither direction advances a
  manifest version or emits graph lineage.
- First-touch recovery with legacy path overlap: a proven no-effect intent may
  defer clone-only ancestor cleanup and return an open handle for leaf-first
  remediation; a mixed multi-table intent with one owned effect and one blocked
  untouched fork must fail read-write open, retain the sidecar without restoring
  or publishing, and converge after offline Lance-level leaf cleanup.

### 11.5 Cost gates

The protocol must not move validation, classification, embedding computation, or
staged-file construction under process-local gates. Measure gate hold time separately
from Prepare. Correctness gates precede latency optimization: a cost regression can
delay rollout, but it cannot justify touched-table-only validation or asynchronous
recovery safety.

## 12. Invariants and deny-list check

This design reinforces the existing architecture:

- Lance and `__manifest` remain the sources of truth; `PreparedWrite` is immutable
  attempt-local state, not a shadow mutable tip.
- Graph visibility remains manifest-atomic.
- Recovery remains part of the commit protocol.
- Logical constraints fail loudly and are revalidated when their inputs change.
- Derived indexes and reclaim work converge without becoming logical commit points.
- No custom WAL, transaction manager, buffer pool, or distributed lock is introduced.

The design rejects two tempting deny-list violations: treating process-local queues as
distributed correctness, and treating recovery as derivable background work that a
new writer may outrun.

Invariant 15 already supplies the applicable rule: a view of immutable,
version-pinned state may be cached, while an in-memory view of the mutable tip is only
a hint. Every use of mutable-tip state as write input is re-arbitrated by the commit
authority. Durable heads under RFC-024 are one possible authoritative representation;
this protocol does not require or bless a warm parallel truth path.

## 13. Drawbacks and rejected alternatives

### 13.1 One generic staged-storage method

Rejected. Lance does not expose staged forms for every operation, and existing writers
have materially different version movement and rollback safety. A generic method would
either lie about those differences or accumulate writer-kind conditionals. One
protocol plus explicit adapters has lower long-run liability.

### 13.2 Asynchronous heal with optimistic continuation

Rejected. Current recovery classification relies on later writers not advancing past
an unresolved sidecar. Scheduling a sweep does not establish that fact. Recovery may
be proactively asynchronous, but the next writer still crosses a synchronous barrier.

### 13.3 Touched-table-only CAS

Rejected. Non-key uniqueness, RI, cardinality, and merge classification read tables
the operation may not write. Ignoring those dependencies admits commits that were never
valid against one serial graph state.

### 13.4 Treat every state change as a manifest commit

Rejected. Native branch refs and physical maintenance have different substrate
visibility points. Manufacturing manifest commits would add coordination without
making the native mutation atomic with them.

## 14. Reversibility

The in-memory coordinator and adapter refactor is reversible. Recovery-sidecar schema
changes must be versioned and backward-compatible during rollout. This RFC alone does
not authorize a `__manifest` schema-stamp bump, a public wire change, or a new storage
substrate.

The correctness contract is intentionally difficult to reverse: after writers rely on
complete read-set arbitration and recovery-before-write, weakening either would
reintroduce silent integrity or recovery races. Focused irreversible changes are
reviewed in RFC-023 through RFC-028.

## 15. Follow-up RFC boundaries

- **RFC-023** owns PK annotation, fenced merge routing, strict keyed Append behavior,
  mixed fenced/unfenced rollout, conflict mapping, and both commit-order tests.
- **RFC-024** owns mutable table-head rows, explicit live/tombstoned head state,
  current-state read shape, migration, and cost gates.
- **RFC-025** owns checkpoint rows, Lance-native physical pins, cleanup reachability,
  pruned-through semantics, and checkpoint/cleanup crash ordering.
- **RFC-026** owns MemWAL enrollment, durable acknowledgements, fold/dead-letter
  behavior, stream quiescence, fresh reads, public surfaces, and upgrade fencing.
- **RFC-027** owns candidate discovery from row lineage, deletion discovery, fallback
  semantics, and evidence for any O(delta) merge claim.
- **RFC-028** owns graph-scoped schema identity, rename/drop lifecycle,
  allocation, SchemaApply integration, and the shared identity-format gate.

Those RFCs call this protocol when they produce a graph-visible write. None may weaken
the recovery barrier, omit read dependencies from `ReadSet`, or create a second graph
visibility point.
