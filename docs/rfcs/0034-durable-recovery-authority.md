---
rfc: "0034"
title: "Durable recovery authority and outcomes"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-08-13
updated: 2026-08-23
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0034: Durable recovery authority and outcomes

- **Depends on:** RFC 0022 unified graph-write protocol; RFC 0023 exact effect
  fencing; internal manifest schema v6 and recovery-v9; Lance 10.0.0.
- **Consumed by:** RFC 0036 atomic runtime activation and availability
  supervision.
- **Replaces:** the destructive in-place recovery boundary proposed by PR #488.
- **Audience:** engine, storage, schema, branch, maintenance, operations, and
  runtime-integration maintainers.

Merging this draft changes no product behavior by itself. Every implementation
slice remains separately reviewed and evidence-gated.

Normative terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** have their usual
RFC meaning.

---

## 0. Decision summary

OmniGraph will have one engine-owned recovery protocol for startup, explicit
operator recovery, and managed live recovery. It classifies durable authority,
resolves one bounded recovery unit at a time, and returns a typed durable
outcome. It does not mutate a serving runtime view, schedule retries, own HTTP
requests, or publish server readiness.

The decisions are:

1. Accepted schema state, graph `__manifest`, exact Lance table/ref state,
   recovery-v9 intents, and recovery audit are the only durable recovery
   authorities.
2. Engine entry points have explicit modes: `ReadOnlyProbe`,
   `ReadWriteNoRecovery`, `RollForwardOnly`, `ExclusiveRecovery`, and
   `VerifyFinal`.
3. A runtime candidate is built only through `ReadWriteNoRecovery`, which can
   inspect authority but cannot run any recovery action. An ordinary public
   read-write open is RollForwardOnly. Restore, native ref/path
   deletion, schema reversal, and every undo action require an opaque
   `ExclusiveRecoveryPermit`.
4. Automatic roll-forward is writer-adapter-specific. It may finish only a
   persisted, proved forward outcome; a timer or retry count never grants
   compensation authority.
5. V1 supports one externally designated read-write owner for a graph root.
   Managed recovery is RollForwardOnly; compensation is an offline operation
   after every serving process for the root is stopped. Process-local gates
   enforce local ordering, not distributed leadership.
6. Recovery work is streamed as independently classifiable units with
   persisted dependency ordering. A graph-atomic envelope is never split.
7. Every unit reports per-authority-dimension `Unchanged`, `Changed`, or
   `Unknown`. A failure after a durable effect is never described as
   effect-free.
8. A terminal recovery returns an opaque `FinalizedRecoveryGuard` that retains
   authority until its caller finishes installing a derived runtime or exits a
   quiesced offline operation.
9. Public view refresh remains non-mutating. It cannot call Restore, publish a
   recovery outcome, promote schema, append audit, or delete an intent/ref/path.
10. This RFC adds no storage format, WAL, transaction manager, durable job
    queue, or parallel recovery state machine.

## 1. Problem

### 1.1 Durable residue is expected

An ordinary graph write crosses three durable boundaries:

1. exact Lance effects on one or more tables or native refs;
2. one graph-manifest publication that makes the exact result visible; and
3. required recovery audit and intent cleanup.

Recovery-v9 persists an identity-bearing intent before the first effect. A
process loss or ambiguous object-store acknowledgement may therefore leave:

- every owned effect complete but not graph-visible;
- only part of a multi-table effect set complete;
- the fixed graph outcome visible while schema promotion, audit, or cleanup is
  incomplete;
- a native branch create/delete gap without an ordinary data sidecar; or
- a terminal outcome beside a stale intent.

This is an explicit recovery input, not permission to infer from numeric HEAD
movement. Reads through the last coherent graph manifest remain snapshot
isolated. New writes must not proceed across unresolved residue.

### 1.2 Forward completion and compensation are different authorities

Some residue has exactly one safe forward outcome: all exact effects are
confirmed and only fixed publication/finalization remains, or a writer adapter
has persisted another forward-only action such as schema promotion.

Other residue requires undo: Restore an earlier table state, delete or replace
an owned native ref/path, reverse schema staging, or remove a partially created
object. Those actions can supersede durable state and need a stronger authority
than ordinary write-entry healing.

One API that silently chooses between these classes makes its caller's
authority unknowable. This RFC makes the distinction structural.

### 1.3 Exact ownership is not distributed fencing

Lance `Dataset::restore()` reads the latest HEAD and appends a new version whose
contents equal an earlier selected version. Lance 10 intentionally treats
Restore as compatible with ordinary Append, Delete, Overwrite, CreateIndex,
Rewrite, Merge, Update, and related operations. A foreign writer can commit
after OmniGraph classifies an intent and before Restore lands; Restore may then
become the latest state and supersede that foreign commit.

Exact intent transaction identities prove which prior effects belong to one
OmniGraph operation. They do not prevent a later foreign effect. Process-local
schema/branch/table gates coordinate one address space only.

Relevant Lance contracts are the
[transaction specification](https://lance.org/format/table/transaction/),
[versioned reads and cleanup](https://lance.org/guide/read_and_write/), and
[branch/tag specification](https://lance.org/format/table/branch_tag/).

Each Lance upgrade also audits pinned source behavior for
`Dataset::restore`, Restore conflict resolution, native branch/ref operations,
and cleanup. A safer upstream change is reviewed rather than rejected merely
because it differs from today's behavior.

## 2. Goals and non-goals

### 2.1 Goals

The design MUST:

- preserve RFC 0022's exact ownership and one graph-manifest visibility
  boundary;
- expose one canonical recovery implementation to startup, operator, and
  managed-runtime callers;
- distinguish read-only inspection, automatic forward completion, exclusive
  compensation, and final verification;
- provide a no-effect writable open that cannot accidentally invoke recovery;
- derive every action from fresh durable authority under the appropriate
  capability;
- stream bounded recovery work without retaining an unbounded global plan;
- order dependent work from persisted lineage and incarnation witnesses;
- retain backward readability of already-valid recovery-v9 intents;
- expose exact partial durable progress and ambiguous acknowledgements;
- recapture durable authority after every invoked effect or unknown outcome;
- keep malformed, foreign, future-version, and unprovable state durable and
  fail closed;
- make the V1 sole-writer and replica-quiescence requirements explicit; and
- provide an opaque handoff that keeps recovery authority alive for a derived
  runtime installer.

### 2.2 Non-goals

This RFC does not define:

- runtime generations, registry publication, cache ownership, or readiness;
- graph-availability supervision, retry scheduling, backoff, or fairness;
- HTTP request cancellation, served-operation task ownership, or shutdown;
- public graph-status/OpenAPI fields;
- client idempotency keys or replay of an interrupted user operation;
- a distributed writer fence;
- durable reader leases for cleanup;
- detached-commit publication;
- a custom WAL, transaction manager, recovery queue, or buffer pool; or
- a new recovery, manifest, schema, or Lance format.

RFC 0035 owns served-operation lifetime and shutdown. RFC 0036 owns runtime
activation and supervision. RFC 0038 owns typed storage-failure classification,
which remains independent: an observed failure condition and durable recovery
progress are different contracts, and neither alone authorizes replay.

## 3. Terminology and invariants

**Durable authority** — accepted schema state, graph manifest, exact Lance
table/ref state, recovery intents, and recovery audit.

**Recovery unit** — one independently classifiable durable work item: normally
one recovery-v9 envelope, or a sidecarless native branch-control gap keyed by
exact stable identity/incarnation witnesses.

**Roll-forward** — execute only the persisted writer adapter's proved forward
outcome. This may confirm exact effects, publish a fixed manifest, promote fixed
schema staging, or finish a content-preserving maintenance outcome. It never
restores prior contents, deletes an owned ref/path, or undoes an effect.

**Compensation** — restore earlier contents, delete or replace an owned native
ref/path, reverse schema staging, or otherwise undo a partial outcome.

**Recovery authority** — an opaque capability that limits which recovery
actions the engine may invoke.

**Finalized recovery** — no remaining recovery unit, no unknown authority
dimension, required audit complete, intent terminally disposed, and final
durable authority freshly verified.

The normative invariants are:

1. **One source of truth.** A supervisor, retry record, or in-memory state never
   becomes durable recovery authority.
2. **One recovery owner.** Every writer kind and every caller uses the same
   discovery, classification, and per-unit implementation.
3. **Capability before classification.** Recovery obtains its mode/authority
   before any effectful decision, then re-reads the complete unit and witnesses.
4. **No implicit escalation.** RollForwardOnly never calls Restore, deletes an
   owned ref/path, or reverses an effect.
5. **Fresh authority after effects.** Every invoked effect is followed by
   durable recapture before another unit is classified.
6. **No false no-op.** Unknown acknowledgement or post-invocation cancellation
   is `Unknown` until recapture proves otherwise.
7. **Persisted dependency order.** Filenames, listing order, timestamps, and
   ULID order are not dependency authority.
8. **Graph atomicity.** A multi-table envelope remains one unit even when it is
   larger than a runtime scheduling quantum.
9. **Terminal handoff.** `FinalizedRecoveryGuard` exists only when recovery is
   terminal and keeps authority held until consumed.
10. **No destructive refresh.** A method named refresh/reload cannot mutate
    recovery authority.

## 4. Recovery modes and capability provenance

### 4.1 Engine modes

| Mode | Capability | Permitted durable behavior |
|---|---|---|
| `ReadOnlyProbe` | Hard read-only role | None. Any required effect is reported, not performed. |
| `ReadWriteNoRecovery` | `ReadWriteLeaderGuard` | None. Build a writable derived handle only when no recovery/control unit exists; otherwise return the exact non-clean `RecoveryDisposition`. |
| `RollForwardOnly` | `ReadWriteLeaderGuard` | Adapter-defined forward units. Compensation is never attempted. |
| `ExclusiveRecovery` | `ExclusiveRecoveryPermit` | Full recovery, including proved Restore/delete/undo. |
| `VerifyFinal` | Authority retained by the recovery session | No recovery effect; verify final durable authority. |

An ordinary public read-write open uses `RollForwardOnly`. Read-write startup may
use it to finish proved forward work, then `VerifyFinal`. A cancelable
runtime-candidate factory MUST use `ReadWriteNoRecovery`; it never calls
`RollForwardOnly` internally. That mode may read accepted schema, manifest, Lance,
and intent/control namespaces, but it has no recovery adapter or effectful
primitive in its type surface. Discovery of any pending unit or a changed final
witness returns the exact non-clean disposition and discards the candidate.

When a candidate follows managed recovery, the factory borrows the retained
`FinalizedRecoveryGuard` and its root-gate domain until installation or
discard. This closes the check/build/activate gap under the V1 sole-writer and
closed-admission boundary. A healthy initial open obtains the same terminal
guard through no-effect final verification. Read-only open cannot arm, mutate,
finalize, or delete recovery state.

### 4.2 V1 process role

Immutable applied configuration declares `HardReadOnly` or
`ExternallyDesignatedWriter`.

`ReadWriteLeaderGuard` proves the local configured writer role and the required
process-local root authority. It does not claim to detect a second
independently misconfigured writer. External deployment orchestration enforces
one read-write owner for a graph root.

Direct embedded, CLI, maintenance, and control-plane writers count as writer
processes. While a server writer is active they route effects through it or
remain quiesced. Broad object-store credentials do not create a supported
second writer.

### 4.3 Exclusive recovery permit

In V1 only the quiesced offline recovery entry point can mint an
`ExclusiveRecoveryPermit`. The server, its background supervisor, and an
ordinary embedded writable open cannot mint one. The permit contains:

- the designated-writer guard;
- an operator-supplied assertion that every server and embedded writer process
  for the graph root has stopped;
- proof that local read/write admission is closed and target-observing
  operations are drained; and
- held schema -> branch -> sorted-table root gates.

The permit is retained through final durable verification and handoff. Without
replica quiescence V1 refuses Restore, native ref/path deletion, and destructive
cleanup. This is an operational boundary, not distributed fencing.

RFC 0035 can prove only process-local admission and drain. That proof is useful
inside the offline entry point but is not proof that another process is
quiescent. Managed live compensation remains unsupported until a future
distributed fence or external replica-quiescence authority has its own RFC and
acceptance evidence.

## 5. Canonical recovery surface

Conceptually the engine owns:

```rust
enum RecoveryMode {
    ReadOnlyProbe,
    ReadWriteNoRecovery(ReadWriteLeaderGuard),
    RollForwardOnly(ReadWriteLeaderGuard),
    ExclusiveRecovery(ExclusiveRecoveryPermit),
    VerifyFinal(RecoverySessionGuard),
}

enum RecoveryDisposition {
    Clean,
    RollForwardRequired,
    NeedsCompensation,
    Blocked(RecoveryBlocker),
}

struct RecoveryOutcome {
    disposition: RecoveryDisposition,
    report: RecoveryReport,
    finalized_guard: Option<FinalizedRecoveryGuard>,
}

async fn discover_recovery_units(
    root: &GraphRoot,
    mode: &RecoveryMode,
) -> Result<RecoveryPlanStream, RecoveryFailure>;

async fn recover_one_unit(
    session: &mut RecoverySession,
    unit: RecoveryUnit,
) -> Result<RecoveryReport, RecoveryFailure>;

async fn verify_final(
    session: RecoverySession,
) -> Result<FinalizedRecoveryGuard, RecoveryFailure>;
```

Exact Rust ownership may differ, but:

- mode capabilities are unforgeable outside their designated constructors;
- the session retains authority and gates across invoked effects;
- discovery is streaming and does not duplicate classification in callers;
- startup, offline recovery, and managed runtime recovery orchestrate these
  same primitives;
- `ReadWriteNoRecovery` has no path to `recover_one_unit` and fails if
  discovery finds work;
- `VerifyFinal` cannot perform a recovery action; and
- dropping a pre-effect session is effect-free, while dropping an effectful
  session leaves durable intent as authority and can never yield a finalized
  guard.

`RecoveryDisposition` is the one vocabulary consumed by startup, embedded
open, offline recovery, and RFC 0036 supervision:

- `Clean` means final verification found no remaining or unknown unit;
- `RollForwardRequired` means at least one adapter-proved forward unit remains,
  including a continuation stopped before the next bounded unit;
- `NeedsCompensation` means terminal state requires Restore/delete/reversal and
  therefore the offline exclusive capability; and
- `Blocked` means malformed, foreign, ambiguous, invariant-violating, or other
  unprovable authority forbids an effect.

Only `Clean` under a writable final-verification authority carries
`finalized_guard: Some`. `ReadOnlyProbe` may return clean with no guard.
`ReadWriteNoRecovery` performs no effect: it returns `RollForwardRequired`,
`NeedsCompensation`, or `Blocked` exactly rather than collapsing them, while
its public convenience API may map any non-clean result to
`RecoveryRequired`. `RollForwardOnly` may converge to `Clean`, stop boundedly
as `RollForwardRequired`, or return `NeedsCompensation`/`Blocked`; it never
escalates authority.

`FinalizedRecoveryGuard` contains, opaquely:

```rust
struct FinalizedRecoveryGuard {
    report: RecoveryReport,
    witness: DurableAuthorityWitness,
    authority: RecoverySessionGuard,
}
```

It knows nothing about server registries or ArcSwap. A caller may consume it to
install a coherent derived runtime, or finish a quiesced offline operation. It
cannot be cloned into multiple competing installers.

A managed installer borrows the guard's verified graph/root-gate domain while
building derived state, then moves the same non-cloneable guard alongside that
inactive candidate until installation succeeds or the candidate is discarded.
It MUST NOT reopen an independent gate domain or consume the guard merely to
construct a long-lived writer facade. Installation releases the guard only
after its publication linearization point; discard releases it without
publishing.

## 6. Adapter-defined recovery actions

Automatic forward eligibility is defined by each RFC 0022 writer adapter, not a
generic `HEAD > pin` rule.

| Classification | Forward action | Mode |
|---|---|---|
| Exact owned effects complete; fixed graph publication/finalization remains | Publish/finalize the persisted outcome | `RollForwardOnly` or exclusive |
| SchemaApply owns exact forward staging and no reversal is needed | Promote and publish the fixed schema outcome | `RollForwardOnly` or exclusive |
| Optimize's identity-bound maintenance classifier proves a content-preserving forward result | Publish the proved maintenance outcome | `RollForwardOnly` or exclusive |
| Sidecarless native branch create/delete gap is proved from authoritative `BranchContents` | Finish/reclaim only that native control operation; emit no graph lineage | Adapter-defined forward or exclusive |
| Ordinary sidecar targets a graph branch that was authoritatively deleted (`OrphanedBranchDiscarded`) | Discard the orphan intent and publish its lineage-only recovery record on main | `RollForwardOnly` or exclusive |
| Partial outcome needs Restore, path/ref deletion, or schema reversal | Compensate exactly | Exclusive only |
| Live, ambiguous, foreign, malformed, future-version, or unprovable state | No effect | Block/operator |
| Invariant violation | No heuristic repair | Operator |

Every adapter specifies:

- stable operation and target identities;
- the expected base and fixed outcome;
- exact or deliberately bounded-loose ownership evidence;
- forward actions;
- compensation actions, if any;
- terminal audit and intent disposition; and
- deterministic failpoint coverage around each durable boundary.

Adding a writer kind without this adapter is forbidden.

## 7. Recovery protocol

### 7.1 Preflight and discovery

Before a recovery effect, the caller:

1. resolves immutable applied graph configuration and process role;
2. obtains the requested recovery capability;
3. opens durable authority without mutating it;
4. streams candidate recovery units; and
5. derives dependency order from persisted graph lineage, unit base/predecessor
   witnesses, schema-state identity, and branch incarnation.

SchemaApply is a graph-global barrier. A unit with an unknown predecessor or
dependency blocks dependent units. Independent branches may be processed in a
later attempt only when independence is structurally proved.

### 7.2 Bounds and compatibility

New writers prospectively enforce finite envelope metadata and effect/table
bounds. Previously valid recovery-v9 envelopes remain readable: discovery and
classification stream them without retaining an unbounded plan. A graph-atomic
envelope is never split to fit a newer scheduling budget.

Only violation of a format bound already normative when an envelope was
written is malformed. Runtime configuration separately bounds units admitted
per attempt. Reaching that budget stops before the next unit and returns a
continuation disposition; it does not abandon the current unit.

### 7.3 One-unit sequence

For each admitted unit the engine:

1. acquires schema -> branch -> sorted-table gates appropriate to the unit;
2. re-reads the complete unit, graph manifest, schema state, branch
   incarnation, exact table HEADs, and adapter-specific witnesses;
3. classifies the unit under the current mode;
4. records the effectful boundary immediately before the first call that may
   write durable state;
5. performs only the fixed adapter action permitted by the capability;
6. recaptures durable authority after every invoked action;
7. proves and publishes the fixed graph outcome if required;
8. appends required recovery audit;
9. terminally disposes the intent or control gap; and
10. returns a per-dimension report.

The next unit is discovered/classified from fresh authority. A failed unit is
never skipped in favor of a dependent unit.

### 7.4 Terminal verification

After the final unit the engine reopens durable authority in `VerifyFinal` mode
and proves:

- no remaining unit or unresolved control gap;
- no unknown changed dimension;
- graph manifest, accepted schema state, Lance pointers, and native refs agree;
- required audit is present; and
- every processed intent has terminal disposition.

Only then may it return `FinalizedRecoveryGuard`.

## 8. Truthful outcomes and failures

Each unit returns structured progress:

```rust
enum DurableChangeState {
    Unchanged,
    Changed,
    Unknown,
}

struct RecoveryReport {
    actions: Vec<RecoveryAction>,
    table_heads: BTreeMap<TableIdentity, DurableChangeState>,
    manifest: DurableChangeState,
    schema_artifacts: DurableChangeState,
    audit: DurableChangeState,
    intents: BTreeMap<OperationId, DurableChangeState>,
    refs_or_paths: BTreeMap<StableObjectIdentity, DurableChangeState>,
    remaining: Vec<RecoveryBlocker>,
    final_authority_witness: Option<DurableAuthorityWitness>,
}

struct RecoveryFailure {
    source: OmniError,
    report: RecoveryReport,
}
```

The aggregate is derived: any `Unknown` makes it unknown; otherwise any
`Changed` makes it changed. An ambiguous manifest acknowledgement cannot be
encoded as `manifest: Unchanged` merely because another dimension is unknown.

After any invoked effect whose acknowledgement is unknown, the next action is
one non-mutating durable recapture. It is not a replay and not an automatic
effect retry. If recapture proves the outcome, classification resumes. If it
remains unknown, the unit requires operator action.

A typed substrate failure classification may inform an external scheduler, but
this RFC never equates `Unknown` with transient or treats classification as
retry authority. RFC 0038 owns that separate classification.

Orphan-control disposition explicitly reports any main-lineage publication,
audit append, and cleanup. No caller may reduce it to an unqualified boolean
such as `processed` or `changed`.

## 9. Cancellation and ownership boundary

Recovery is effect-free only before the effectful boundary. Immediately before
the first possibly durable call, the session marks the unit effectful. A panic,
abort, task loss, or cancellation at or after that boundary leaves the outcome
unknown until recapture.

The engine does not claim to keep an async task alive after its caller drops
it. A managed runtime invoking effectful recovery MUST own the task through a
terminal engine result; RFC 0036 defines that recovery-task ownership, while
RFC 0035 owns served requests and writes. An offline tool remains the owning
caller. In both cases durable intent makes
process termination recoverable, but process loss is not reported as a clean
in-process cancellation.

An operation budget may prevent admission of the next unit. It is not
permission to drop an in-flight ambiguous storage future. Substrate operations
use finite request/retry deadlines where supported.

## 10. Topology and future fencing

### 10.1 Supported V1 topology

V1 supports one externally designated read-write owner per graph root. Other
replicas are hard read-only and cannot mint recovery capabilities. Exclusive
compensation is offline-only and additionally requires every serving process
for the root to be stopped because OmniGraph has no cross-process recovery
fence or durable reader lease for destructive ref/path cleanup. The managed
runtime performs RollForwardOnly recovery. A unit requiring compensation
leaves the graph write-blocked and operator-required.

Leader failover is safe only after the prior writer is known unable to reach
storage. An expiring lock object, replica count, process-local mutex, or
"first process to boot" convention is not a fence.

### 10.2 Future distributed fence

Overlapping writers require a monotonic epoch from a linearizable authority.
Every authoritative or destructive effect must atomically consume/validate the
epoch: Lance table-manifest commits, graph-manifest publication, intent
arm/confirm/delete, recovery audit, accepted schema/staging promotion,
first-touch creation/deletion, native ref lifecycle, Restore, and destructive
cleanup.

A Lance `CommitHandler` or external manifest store is only one table-manifest
integration point. It does not fence graph metadata, schema files, intents,
native refs, or path deletion. A future fencing RFC is complete only when a
paused old leader cannot perform any authoritative effect after a newer leader
takes ownership.

### 10.3 Detached commits

Detached publication is not adopted here. It changes version/history,
first-touch creation, branch/index lineage, and cleanup reachability, and needs
a separate feasibility issue and storage RFC.

## 11. Public and embedded boundary

A public view API, if provided, is explicitly non-mutating:

```rust
async fn reload_view(&self) -> Result<ViewReloadReport>;
```

It performs no Restore, recovery publication, schema promotion, audit append,
intent mutation, or native ref/path effect.

Ordinary read-write open is RollForwardOnly and returns `RecoveryRequired` when
compensation is needed. `ReadWriteNoRecovery` is the only writable runtime
candidate-open mode and returns the exact non-clean disposition for any pending
unit; a public open facade may map it to `RecoveryRequired` without erasing the
engine-owned result consumed by supervision.
Exclusive recovery is available only through the V1 quiesced offline entry
point. A method cannot infer authority from the fact that an `Omnigraph` was
opened read-write.

The concrete server activation and operator transport are RFC 0036 concerns.
The concrete close/drain proof and owned-task lifetime are RFC 0035 concerns.

## 12. Cost and observability

A healthy graph performs no periodic Full recovery merely to prove readiness.
Recovery discovery MAY use a cheap intent-namespace witness before a full
classification pass.

The recovery owner records bounded structured events for:

- operation/unit identity and adapter;
- authority mode;
- dependency and gate waits;
- actions and per-dimension change states;
- ambiguous acknowledgement and recapture;
- final witness or blocker; and
- storage requests/bytes and total unit duration.

No backend URI, path, credential, presigned query, or raw substrate error enters
a public failure. Full manifest-fold cost is measured across several history
depths; one scan is reduced amplification, not history-independent work.

## 13. Implementation sequence

### Phase 0 — modes and topology

- Make ordinary open RollForwardOnly.
- Add the effect-free `ReadWriteNoRecovery` candidate-open mode.
- Add hard read-only and externally designated writer roles.
- Define constructors for writer and exclusive recovery capabilities.
- Document sole-writer and replica-quiescence requirements.

### Phase 1 — canonical units and reports

- Extract streaming discovery and one-unit recovery from whole-open sweeping.
- Replace boolean/`Result<()>` summaries with `RecoveryReport`.
- Populate exact change dimensions for every existing writer adapter.
- Pin orphan-control, multi-unit partial progress, and ambiguous outcomes.

### Phase 2 — exclusive recovery and handoff

- Gate Restore/delete/reversal behind `ExclusiveRecoveryPermit`.
- Add final no-effect verification and `FinalizedRecoveryGuard`.
- Provide quiesced offline recovery first.
- Keep managed live recovery RollForwardOnly; compensation remains offline in
  V1.

## 14. Required acceptance evidence

Engine recovery owners in `src/db/manifest/recovery.rs` and
`tests/failpoints.rs` prove:

- all existing writer kinds use one discovery/classification/recovery owner;
- `ReadOnlyProbe`, `ReadWriteNoRecovery`, and `RollForwardOnly` cannot Restore,
  delete an owned ref/path, or reverse schema without an exclusive permit;
- `ReadWriteNoRecovery` performs zero recovery effects, returns the exact
  `RollForwardRequired`, `NeedsCompensation`, or `Blocked` disposition for every
  pending sidecar/control unit, and cannot reach a recovery adapter even when
  its future is cancelled;
- every caller consumes the same four `RecoveryDisposition` variants without
  string matching or a transport-local recovery taxonomy, and a bounded
  roll-forward continuation returns `RollForwardRequired` rather than false
  completion;
- each adapter's forward boundary is exact and independently tested;
- failures before/after every table effect, Restore, graph publication, schema
  promotion, audit append, and intent deletion converge deterministically;
- post-invocation panic/abort is unknown until recapture and cannot finalize;
- an error after one of several units carries exact partial progress;
- each later unit classifies from authority refreshed after the prior unit;
- persisted dependencies, schema barriers, and branch incarnations determine
  order rather than filenames or timestamps;
- malformed/foreign/future/unprovable units remain untouched;
- existing valid recovery-v9 envelopes remain readable and stream boundedly;
- the unit budget stops before N+1 without splitting unit N;
- sidecarless native branch create/delete gaps classify from `BranchContents`
  and never synthesize graph lineage;
- `OrphanedBranchDiscarded` for an ordinary sidecar on a deleted graph branch
  publishes and recaptures exactly one lineage-only recovery commit on main;
- `FinalizedRecoveryGuard` requires no remaining/unknown dimension and terminal
  audit/intent state;
- local writer/recovery operations serialize through root gates;
- a hard read-only process cannot reach an effectful recovery primitive;
- a two-read-write-process adversarial test demonstrates the unsupported
  Restore/orphan hazard until distributed fencing exists; and
- local and configured RustFS/S3 tests cover ambiguous remote outcomes, real
  e-tags, retry exhaustion, and post-effect recapture.

Lance surface guards detect changes to Restore conflict resolution, native ref
semantics, and cleanup behavior and block an upgrade pending review. They do not
require Lance to preserve an unsafe behavior.

`docs/dev/testing.md` maps these owners when implementation begins. No server
activation/cancellation test belongs to this RFC.

## 15. Security and operational boundaries

- Recovery authority is not client authorization and never accepts actor
  identity from an HTTP request.
- Recovered lineage/audit preserves the original/recovery actor contract from
  RFC 0022.
- Read-only replicas receive no write/recovery capability even with broad
  credentials.
- Public summaries redact placement and credentials; full bounded diagnostics
  remain in operator logs.
- Runbooks explain how to establish sole-writer and replica quiescence before
  exclusive compensation.
- Unknown or invariant-violating state is quarantined, never heuristically
  repaired.

## 16. Rejected alternatives

**Destructive public refresh.** Rejected because view freshness and durable
compensation have different authority, latency, failure, and observability.

**Lease-only leader lock.** Rejected because a paused holder can resume an
already-prepared commit after expiry. A lease is a fence only when every
authoritative commit consumes its monotonic epoch.

**HEAD check immediately before/after Restore.** The pre-check has a
check-then-act window; the post-check detects loss only after the destructive
commit landed.

**Separate startup and live recovery implementations.** Rejected because every
new writer kind would need two evolving classifiers.

**Automatic replay of the interrupted user operation.** Recovery establishes
graph state, not whether the caller observed success. Replay needs a separate
idempotency/receipt protocol.

**Durable recovery job queue.** Rejected because work is derivable from intents,
manifest, schema state, refs, and Lance. A wake hint is not durable authority.

## 17. Compatibility, drawbacks, and open questions

This RFC adds no persisted field or format. Existing valid recovery-v9 intents
remain readable. It intentionally narrows one observable embedded behavior:
ordinary read-write open becomes RollForwardOnly rather than silently
performing compensation. Release notes and API docs must call out that boundary.

Costs are an extra final verification open, stronger operator topology
requirements, structured recovery reports, and broader failpoint coverage.
These exceptional-path costs replace permanent duplicated classifiers and
implicit destructive authority.

Open implementation choices are limited to:

1. exact prospective unit-emission bounds compatible with every writer kind;
2. the opaque Rust shape used by the quiesced offline entry point to record its
   operator assertion and local drain proof.

None changes the central decision: one canonical engine recovery owner,
explicit capabilities, truthful durable outcomes, and no compensation without
exclusive authority.
