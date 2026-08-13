---
type: spec
title: "RFC-034 — Recovery authority and atomic runtime activation"
description: Recover an unavailable served graph from durable authority into a complete replacement runtime, then atomically activate it without destructively refreshing the serving handle in place.
status: draft
tags: [eng, rfc, recovery, server, availability, concurrency, lance, fencing, omnigraph]
timestamp: 2026-08-13
owner: OmniGraph maintainers
---

# RFC-034: Recovery authority and atomic runtime activation

- **Status:** Draft
- **Date:** 2026-08-13
- **Author track:** Maintainer design series
- **Depends on:** RFC-022 unified graph-write protocol; RFC-023 exact effect
  fencing; internal manifest schema v6 and recovery-v9; Lance 10.0.0.
- **Informs:** graph-availability supervision and server-owned write-lifetime
  work proposed in PRs #489 and #490.
- **Replaces:** the unmerged in-place Full-recovery direction proposed by PR
  #488. It does not supersede RFC-022 or change its durable protocol.
- **Audience:** engine, server, cluster, storage, operations, and release
  maintainers.

Merging this draft changes no product behavior by itself. Each phase remains a
separately reviewed implementation slice with its own acceptance gates.

Normative terms **MUST**, **MUST NOT**, **SHOULD**, and **MAY** have their
usual RFC meaning.

---

## 0. Decision summary

OmniGraph will recover a long-running served graph by building a complete fresh
runtime from durable authority and atomically replacing the registered runtime.
It will not turn a public view-refresh method into a rollback-capable storage
operation, and it will not repair the durable graph while publishing the
coordinator, schema, query registry, policy, or caches piecemeal into a serving
handle.

The decisions are:

1. The accepted schema contract, `__manifest`, exact Lance versions and native
   refs, recovery-v9 sidecars, and recovery audit remain the only durable
   authorities. Supervisor state is a derived projection and is rebuilt after
   process loss.
2. View refresh and durable recovery are separate operations. Any API named
   `refresh_view` or `reload_view` is non-mutating. Full recovery is explicit,
   controller-owned, and requires recovery authority.
3. A recovery attempt closes new write admission, re-discovers durable state,
   resolves bounded RFC-022 recovery units through capability-gated engine
   APIs, performs a no-recovery verification open, rebuilds the complete
   service wrapper, and publishes it through one registry pointer swap.
4. Requests capture one activated graph generation for their lifetime. A fresh
   activation owns fresh correctness-bearing caches; an old request cannot
   populate the new generation's dataset or graph-index cache.
5. Adapter-defined, persisted forward actions may run automatically on the
   current read-write owner. Restore, owned-path/ref deletion, and every undo
   action require explicit exclusive recovery authority.
6. V1 supports one externally designated read-write owner for a graph root at
   a time. The server enforces its configured read-only role locally; operator
   orchestration must enforce writer uniqueness. Process-local queues are not
   leader election or a distributed fence.
7. A future multi-writer design must provide a monotonic fencing epoch consumed
   by every Lance and graph-manifest commit. A lease checked outside the commit
   is insufficient.
8. Recovery returns a typed disposition and partial-progress report. An error
   after a durable effect is never reported as an effect-free failure.
9. Recovery tasks are singleflight, owned independently of callers, bounded by
   explicit work units, and observable through shutdown. A caller timeout does
   not cancel or replay an effectful attempt.
10. There is no new storage format, WAL, transaction manager, recovery queue,
    or parallel durable state machine.

This architecture deliberately spends an extra exceptional-path open and cold
cache warm-up to remove permanent coupling between recovery and every mutable
field of a serving engine handle.

## 1. Problem

### 1.1 Durable write residue is expected

An ordinary graph write has three relevant durable boundaries:

1. one or more exact Lance table effects;
2. one graph-manifest publication that makes the exact table set visible; and
3. recovery audit and sidecar cleanup.

Recovery-v9 persists the intent before the first effect. A process loss or an
ambiguous object-store response may therefore leave:

- every owned effect complete but not graph-visible;
- only part of a multi-table effect set complete;
- the fixed manifest outcome visible while schema promotion or cleanup is
  incomplete; or
- a stale sidecar whose terminal outcome is already visible.

That state is not corruption. It is an explicit input to the recovery
protocol. Reads through the last coherent graph manifest remain snapshot
isolated, while new writes must not proceed until the residue is classified and
resolved.

### 1.2 Open-time recovery is insufficient for a long-running server

Read-write open already performs Full RFC-022 recovery before returning a
handle. A long-running server can encounter a rollback-eligible residue after
startup, however. Roll-forward-only write-entry healing correctly refuses to
guess at destructive compensation, so subsequent writes return
`RecoveryRequired` until Full recovery runs.

Requiring a process restart is operationally safe but needlessly broad. The
server already has the configuration and task ownership needed to rebuild one
graph without restarting unrelated graphs.

### 1.3 Runtime activation and durable recovery are different transactions

Durable recovery is an idempotent sequence. It can restore table contents,
publish a manifest, promote schema artifacts, append an audit row, and delete a
sidecar. A later step may still fail. It is therefore not one rollback-able
in-memory transaction.

Runtime activation has a different requirement: no request may combine a
coordinator from one durable view with a schema, policy, query registry, branch
incarnation, or cache from another. That requirement can and should be one
atomic pointer publication.

Conflating these operations creates misleading guarantees. "The handle was not
updated" does not mean "storage was unchanged." Conversely, durable recovery
may complete successfully while rebuilding a service-only component fails. The
architecture needs to represent both truths.

### 1.4 The multi-process boundary is currently physical, not rhetorical

Lance `Dataset::restore()` reads the latest HEAD and appends a new version whose
contents equal an earlier selected version. Lance intentionally accepts Restore
beside ordinary Append, Delete, Overwrite, CreateIndex, Rewrite, Merge, Update,
and related operations. A foreign writer can therefore commit after OmniGraph
classifies a sidecar and before Restore lands; Restore may then become the new
HEAD and supersede that foreign commit.

Exact sidecar transaction identities prove which prior effects belong to an
intent. They do not prevent a later foreign effect. Process-local ordered gates
coordinate handles in one address space only.

Relevant Lance contracts are the
[transaction specification](https://lance.org/format/table/transaction/),
[versioned reads and cleanup](https://lance.org/guide/read_and_write/), and
[branch/tag specification](https://lance.org/format/table/branch_tag/).
Because the public pages do not expose every load-bearing detail, each Lance
upgrade also audits the pinned source for `Dataset::restore`, Restore conflict
resolution, `commit_detached`/`list_detached_manifests`, ordinary cleanup's
manifest enumeration, and native branch/ref operations. A safer upstream change
is reviewed, not rejected merely because it changes today's asymmetric Restore
behavior.

## 2. Goals and non-goals

### 2.1 Goals

The design MUST:

- recover one served graph without restarting unrelated graphs;
- preserve RFC-022's exact sidecar ownership, recovery decisions, and one
  graph-manifest visibility boundary;
- block new writes before managed recovery starts;
- distinguish automatic roll-forward from exclusive compensation;
- build coordinator, accepted schema/catalog, policy, embedding configuration,
  stored queries, branch binding, and correctness-bearing caches as one service
  generation;
- activate that generation with one registry publication and no fallible work
  afterward;
- keep every request on exactly one generation;
- make old cache fills structurally unable to poison a new generation;
- retain task ownership across caller cancellation and supervisor wait timeouts;
- represent partial durable progress honestly;
- prevent stale recovery attempts from overwriting newer state;
- keep healthy graphs off the Full-open path;
- make the one-writer topology explicit, enforce the read-only role locally,
  and require writer uniqueness from external orchestration;
  and
- leave a concrete evidence gate for future distributed fencing.

### 2.2 Non-goals

This RFC does not:

- introduce a custom WAL, transaction manager, durable recovery queue, or
  buffer pool;
- replay the user write that produced the residue;
- provide idempotency keys or recover a result receipt for a disconnected
  client;
- make an in-memory reader a durable cleanup lease;
- preserve warm dataset/index caches across activation;
- add runtime graph registration, removal, or configuration hot reload;
- make writable multi-replica serving safe without a commit-integrated fence;
- change recovery-sidecar, graph-manifest, Lance, or schema storage formats;
- adopt Lance detached commits in the first implementation;
- make `OpenMode::ReadOnly` a complete Rust typestate for every embedded API;
  or
- hide an unresolved recovery state behind a successful current-head read.

## 3. Terminology

**Durable authority** — the accepted schema contract, graph manifest, exact
Lance table/ref state, recovery sidecars, and recovery audit.

**Activated generation** — one completely constructed service view containing
the engine handle, accepted schema/catalog, policy, stored-query registry,
configuration digest, branch/manifest witnesses, and a generation-local cache
namespace.

**Recovery signal** — a wake-up hint, such as `RecoveryRequired` or discovery
of a sidecar. It is never authority and may be duplicated or lost.

**Recovery attempt** — one controller-owned, singleflight task for one graph.
It re-discovers durable authority and owns its lifetime independently of the
caller that supplied a signal.

**Roll-forward** — execute only the persisted writer adapter's proven forward
outcome. Depending on that adapter, this may confirm already-complete effects,
publish a fixed manifest outcome, promote fixed schema staging, or finish a
content-preserving maintenance outcome. It never restores prior contents,
deletes an owned path/ref, or otherwise undoes an effect.

**Compensation** — an action that restores earlier table contents, deletes or
replaces an owned native ref/path, reverts schema staging, or otherwise undoes a
partial owned outcome.

**Exclusive recovery authority** — proof, within the supported topology, that
no unfenced writer can commit while compensation is classified and performed.

**Activation** — one atomic registry store that makes a fresh complete
generation available to new requests.

## 4. Normative invariants

1. **One durable authority.** Runtime state is derived from storage; it never
   becomes a parallel recovery truth.
2. **One generation per request.** A request loads one generation once and
   carries that `Arc` for its lifetime.
3. **One activation publication.** Engine, schema/catalog, policy, query
   registry, write readiness, and cache namespace do not publish separately.
4. **Generation-owned caches.** Correctness-bearing dataset handles, graph
   indexes, and branch-incarnation state belong to one generation or include an
   unforgeable generation epoch in their key.
5. **No implicit destructive refresh.** View refresh performs no Restore,
   manifest publication, schema promotion, audit write, sidecar mutation, or
   native-ref/path deletion.
6. **Admission closes first.** Every recovery closes writes; effectful recovery
   also closes current-head reads. Compensation begins only after all operations
   that could observe or affect its targets are drained under closeable gates.
7. **Fresh classification under authority.** Recovery re-reads the sidecar,
   manifest, branch incarnation, and table HEADs after obtaining its authority
   and before every effectful decision.
8. **Recapture after effects.** Once a durable effect may have occurred, no new
   current-head operation is admitted until a coherent fresh generation is
   constructed or the prior generation's continued safety is structurally
   proved.
9. **No false effect-free result.** Unknown acknowledgement or cancellation
   after invoking storage is `Unknown`/effectful until fresh authority proves
   otherwise.
10. **Singleflight.** A graph has at most one opening or recovery task. Duplicate
    wake hints coalesce; only a proved configuration/authority change fences an
    older attempt's activation result.
11. **No foreign Restore.** Compensation does not run without exclusive writer
    authority. A process-local mutex is not such proof across processes.
12. **Bounded ownership.** Recovery work, retry, stalled-task accounting, and
    shutdown waiting have explicit bounds. A timeout that merely stops waiting
    is not reported as task termination.
13. **No replay.** Recovery establishes a coherent graph; it never repeats the
    initiating user request.
14. **Fail closed.** Malformed, foreign, unprovable, future-version, or
    invariant-violating intents remain durable and block writes.

## 5. Chosen architecture

### 5.1 Components

The design has five owners:

1. **RFC-022 recovery engine** — classifies and resolves one durable recovery
   envelope from fresh authority.
2. **Graph factory** — opens a graph and builds every service component from
   immutable applied configuration.
3. **Recovery controller** — owns signals, attempt lifetime, authority,
   scheduling, retry, and typed results for one graph.
4. **Graph registry entry** — stores immutable construction configuration and
   one atomically replaceable runtime state.
5. **Request admission** — captures one generation and enforces read/write
   readiness before invoking engine code.

No component adds a durable queue. On restart, the controller reconstructs
state from configuration and durable graph authority.

### 5.2 Activated graph generation

Conceptually:

```rust
struct ActivatedGraph {
    activation_id: u64,
    config_digest: String,
    read: Arc<GraphReadFacade>,
    write: Option<Arc<GraphWriteFacade>>,
    policy: Option<Arc<PolicyEngine>>,
    queries: Arc<QueryRegistry>,
    installation_witness: ActivationWitness,
    cache_namespace: CacheNamespace,
}

struct ActivationWitness {
    graph_identity: GraphIdentity,
    manifest_incarnation: ManifestIncarnation,
    manifest_version: u64,
    accepted_source_digest: String,
    accepted_ir_identity: SchemaIdentity,
    accepted_state_identity: SchemaStateIdentity,
    recovery_terminal_witness: RecoveryTerminalWitness,
}
```

The exact Rust ownership may differ, but the following are normative:

- the complete value is constructed before activation;
- its policy and stored queries are compiled against its engine catalog;
- its manifest/schema witnesses describe the same durable graph view;
- its correctness-bearing caches are not shared with an older activation; and
- its outer fields and cache namespace are not rebound after the generation
  becomes visible.

The contained engine may continue its existing operation-scoped coordinator
refresh inside that generation. The installation witness proves activation
coherence; it is not a promise that the graph will never advance. Registry
fields are private. The server exposes read methods through `GraphReadFacade`
and can construct `GraphWriteFacade` only by consuming a private writer-role
capability. A hard read-only generation has no write facade, and server routes
cannot recover a raw writer from its read facade.

Fresh generation ownership includes the Lance session/metadata cache,
manifest/branch projection, dataset-handle cache, and graph-index cache. Network
clients and object-store connection pools MAY be shared because they do not
cache graph identity or version selection. If a lower layer cannot avoid a
shared cache, its key includes a process nonce, graph identity, activation
sequence, stable branch/table incarnation, exact version, and e-tag where
available; an activation epoch never replaces the durable identity/version
parts of the key.

### 5.3 Graph registry entry

One registry entry owns:

```rust
struct GraphEntry {
    startup: Arc<GraphStartupConfig>,
    runtime: ArcSwap<GraphRuntimeState>,
    configuration_generation: AtomicU64,
    wake_dirty: AtomicBool,
    retired: RetiredGenerationTracker,
    transition: Mutex<()>,
    wake: Notify,
}
```

`GraphStartupConfig` is a fully resolved, applied, digest-bound construction
snapshot for the URI, graph identity, Cedar policy content, external-Blob
policy, embedding configuration, stored queries, process role, and server
exposure. A mutable file path is not construction authority: referenced policy
or query content is resolved, validated, and bound before the first recovery
effect. Recovery MUST rebuild from this snapshot rather than copying
potentially stale pieces out of the old runtime.

The short transition mutex protects only state comparison and the final
ArcSwap store. No graph or object-store I/O runs while it is held.

Retired generations are bounded. Effectful V1 recovery drains served operation
permits before it starts, so no request-owned old generation remains at
activation. Non-mutating reload may use RCU retirement, but it cannot begin a
third overlapping generation after reaching the configured retired-generation
limit; it waits for tracked requests to drain or leaves the old generation
installed. Server request/task ownership and deadlines make every retained Arc
observable. External arbitrary Arc clones are not part of the managed server
boundary.

### 5.4 Engine recovery modes and graph factory

Recovery authority is enforced by the engine API that can cause durable
effects, not merely by a controller convention. Managed construction has four
distinct modes:

| Mode | Capability | Durable behavior |
|---|---|---|
| `ReadOnlyProbe` | Read-only role | Performs no write. A pending intent that requires any effect makes the graph unavailable. |
| `ServingOpen` | `ReadWriteLeaderGuard` | May run adapter-defined RollForwardOnly units. Returns `RecoveryRequired` when compensation is needed. |
| `ExclusiveRecovery` | `ExclusiveRecoveryPermit` | May run Full recovery, including proved Restore/delete/undo actions. |
| `VerifyFinal` | Authority already held by the attempt | Performs no recovery effect; opens and verifies the final durable view for candidate construction. |

An ordinary public read-write open is `ServingOpen`; it cannot silently escalate
to Full recovery. `ExclusiveRecovery` is the only path that can call an engine
primitive capable of Restore, owned ref/path deletion, or schema reversal.
Read-write startup may request that mode only after acquiring the same exclusive
authority required during live recovery.

The graph factory prepares a complete candidate:

```rust
async fn recover_and_build(
    config: &GraphStartupConfig,
    authority: RecoveryAuthority,
) -> Result<RecoveryActivation, RecoveryFailure>;
```

`RecoveryActivation` owns the inactive candidate, final `ActivationWitness`,
terminal `RecoveryReport`, closed admission guards, recovery authority, held
root gates, expected configuration generation, and expected registry state.
Only the registry's activation method can consume it. The authority cannot drop
between recovery and the ArcSwap commit merely because a helper returned.

The operation:

1. resolves and validates every configuration component that does not depend on
   the final catalog before invoking a recovery effect;
2. discovers durable graph authority under the supplied recovery mode;
3. executes only the RFC-022 units permitted by that mode;
4. performs a `VerifyFinal` open from final durable authority;
5. validates accepted schema source, IR/state identity, and catalog;
6. attaches the prevalidated embedding and external-Blob configuration;
7. builds the prevalidated policy against the final graph scope;
8. type-checks pre-parsed stored queries against that catalog;
9. creates fresh generation-local caches; and
10. returns an inactive complete generation plus a recovery report.

Every fallible construction step precedes activation.

### 5.5 Recovery controller

The controller is server-owned and singleflight per graph. Wake hints are lossy,
duplicable notifications: repeated hints set one dirty bit and join the active
attempt. They do not advance the configuration/authority generation and cannot
livelock activation by endlessly invalidating a valid candidate. After an
attempt settles, a dirty bit causes one more fresh probe. Only a proved applied
configuration change, registry replacement, operator authority change, or a
fresh durable probe that observes a different authority witness invalidates a
candidate. Signal error text, operation ID, and retry count are diagnostic only.

The controller chooses one of three authority modes:

```rust
enum RecoveryAuthority {
    AutomaticRollForward(ReadWriteLeaderGuard),
    ExclusiveCompensation(ExclusiveRecoveryPermit),
    ReadOnlyProbe,
}
```

These tokens are private construction capabilities. Immutable applied startup
configuration declares either `HardReadOnly` or `ExternallyDesignatedWriter`.
A read-only process cannot mint either write-capable token.

`ReadWriteLeaderGuard` proves only the locally configured writer role plus the
process-local root authority required by the operation. V1 writer uniqueness is
an external deployment guarantee; this token does not pretend to detect another
misconfigured writer process.

`ExclusiveRecoveryPermit` additionally contains an externally supplied
replica-quiescence assertion, plus closed local read/write admission, drained
local operations, and held schema -> branch -> sorted-table gates. Without that
assertion V1 refuses Restore, native path/ref deletion, and destructive cleanup.
The permit is retained through final durable recapture and candidate activation.
Only a future commit-integrated fencing epoch and durable reader-lifetime design
can replace these external guarantees.

### 5.6 Request capture and closeable admission

Each registry state owns closeable read and write admission gates. Permit
acquisition, readiness observation, and generation capture are one synchronized
operation:

```text
registry lookup
  -> atomically acquire the required admission permit
  -> verify readiness while the gate is still open
  -> clone Arc<ActivatedGraph>
  -> capture the operation-local graph snapshot
  -> carry the permit, generation, and snapshot through terminal response
```

Closing a gate is linearizable with permit acquisition. Recovery cannot observe
zero admitted work while a request that previously observed `Ready` is still
between readiness and registration. The request MUST NOT reload the registry
midway. An admitted request returns its captured snapshot's data, fails loudly,
or is cancelled; it never retargets to the replacement.

V1 closes both gates and drains all admitted graph operations before any
recovery effect. This deliberately gives up some read availability to keep the
first implementation's lifetime proof uniform across forward and compensating
actions. A future optimization may let proved-independent immutable reads span
forward recovery only after it owns their retirement bound explicitly.
Served-work lifetime tracking therefore lands before effectful live recovery is
enabled, not afterward.

## 6. Recovery topology and Lance boundary

### 6.1 Supported V1 topology

V1 supports exactly one read-write owner for a graph root at a time.

- One process is the read-write leader. All mutation, load, schema, branch,
  maintenance, graph publication, and recovery effects run there.
- Other replicas open hard read-only generations. They do not arm sidecars,
  repair storage, delete intents, Restore datasets, or run an effectful recovery
  controller.
- A current-head request on a read-only replica first captures the engine's
  ordinary operation snapshot and compares its schema/state identity with the
  accepted-schema identity installed in the service generation. An ordinary
  data or lineage advance under the same schema remains eligible; the request
  linearizes at its captured snapshot. A service-coherence mismatch closes
  current-head admission and triggers a non-mutating `ReadOnlyProbe` rebuild.
  During the manifest-before-schema-promotion gap the rebuild fails closed
  rather than combining the new manifest with the old contract.
- Process-local schema/branch/table gates remain necessary inside the leader,
  because it may contain many handles and tasks.
- Holding a process-local gate is not evidence that the process is the leader.
- Leader failover is safe only after the prior writer is known unable to reach
  storage, or after a distributed fencing epoch makes its commits fail.
- Direct embedded, CLI, maintenance, or control-plane writers count as writer
  processes. While a server leader is active they must route effects through
  it or remain quiesced; broad object-store credentials do not confer a second
  supported writer role.

Deployment and server documentation MUST stop describing writable shared-root
replicas as a supported topology. The server can reject writes when configured
hard read-only, but without a distributed fence it cannot detect every
independently misconfigured second writer. External orchestration must enforce
writer uniqueness. A replica count, rolling-deployment setting, "first process
to boot" convention, or local mutex is not fencing.

Exclusive compensation, native path/ref deletion, and destructive cleanup also
require read replicas to be quiesced until durable reader leases exist. Ordinary
non-destructive leader publication may coexist with hard read-only replicas
only through the witness-and-captured-snapshot rule above.

### 6.2 Automatic versus exclusive actions

| Fresh classification | Permitted action | Required authority |
|---|---|---|
| No pending intent | Build/activate only if requested | Read-only probe |
| Writer adapter proves a persisted fixed forward outcome | Execute only that adapter's forward actions | Current read-write leader |
| SchemaApply has an exact owned staging promotion with no reversal | Promote and publish forward | Current read-write leader |
| Optimize's identity-bound maintenance classifier proves its content-preserving forward outcome | Publish the proven maintenance outcome | Current read-write leader |
| Orphan-branch finalization is proved and requires no ref/path deletion | Finalize, then recapture main lineage | Current read-write leader |
| Partial owned effects require Restore, ref/path deletion, schema-staging reversal, or other compensation | Compensate | Exclusive recovery authority |
| Intent is live, ambiguous, foreign, malformed, future-version, or cannot be re-proved | No physical effect; retain/quarantine | None can authorize guessing |
| Invariant violation | No heuristic repair | Operator diagnosis |

Ordinary write-entry healing remains RollForwardOnly. A background timer does
not gain compensation authority merely because a retry count or elapsed time
was exceeded.

Automatic forward recovery uses the same closeable admission, process-local
gates, final recapture, and authority-through-activation span as compensation.
Its weaker capability narrows the permitted effects; it does not weaken
serialization or activation proofs.

### 6.3 Exclusive compensation span

Compensation performs this exact sequence:

1. establish supported single-writer authority and replica quiescence;
2. close read and write admission and drain all local operations that may
   observe or affect the targets;
3. acquire schema -> branch -> sorted-table gates;
4. re-read the complete sidecar and every authority witness;
5. reclassify from that fresh base;
6. execute only effects proven by the persisted intent;
7. publish the fixed recovery outcome, append required audit, and prove terminal
   sidecar disposition;
8. recapture final durable authority while the permit remains held;
9. build a coherent candidate generation from that authority;
10. perform the final substrate witness probe before taking the short transition
    mutex;
11. under the mutex compare only request, configuration, and runtime generations,
    then activate atomically; and
12. re-enable writes only after activation and terminal durable state agree.

If the sequence fails, writes remain blocked and durable intent remains the
next attempt's authority.

### 6.4 Future distributed fence

Overlapping writer processes require a monotonically increasing writer epoch
from a linearizable authority. Every retry of every authoritative or destructive
effect — including Restore — must atomically validate the epoch while reserving
or publishing that effect. The fence covers Lance table-manifest commits, graph
`__manifest` publication, sidecar arm/confirm/delete, fixed recovery outcomes,
recovery audit, accepted schema and staging promotion, first-touch dataset
creation/deletion, native branch/ref lifecycle, and destructive cleanup.
Unreferenced immutable fragment uploads need not consume the fence only when a
stale owner cannot publish them and reachability GC remains safe.

A Lance `CommitHandler` or external manifest store is one table-manifest
integration point, not the whole solution: it does not by itself fence graph
metadata, schema files, sidecars, native refs, or path deletion. Distributed
authority requires its own RFC and two-process adversarial evidence. The design
is not complete until a paused stale leader cannot perform any authoritative
effect after a newer leader takes ownership.

An expiring object-store lease without commit-integrated fencing is explicitly
rejected. A paused holder can resume an already-prepared commit after lease
expiry; a final pre-effect lease read only narrows the race.

## 7. Runtime state machine

The conceptual state is:

```rust
enum GraphRuntimeState {
    Opening {
        configuration_generation: u64,
        retry: RetryState,
    },
    Serving {
        activation: Arc<ActivatedGraph>,
        admission: AdmissionState,
        recovery: RecoveryState,
    },
    Unavailable {
        failure: PublicFailure,
    },
}

enum RecoveryState {
    Ready,
    Pending,
    RecoveringPreEffect { attempt_id: AttemptId },
    RecoveringEffectful { attempt_id: AttemptId },
    NeedsRecapture { attempt_id: AttemptId },
    Blocked {
        phase: AttemptPhase,
        retry: Option<RetryState>,
        failure: PublicFailure,
    },
    OperatorRequired {
        phase: AttemptPhase,
        reason: PublicFailure,
    },
}

struct AdmissionState {
    read_ready: bool,
    write_ready: bool,
}
```

The implementation may collapse internal states when it preserves the
following transitions:

| Current | Event/outcome | Next |
|---|---|---|
| `Opening` | complete candidate built | `Serving(new, Ready, role-derived admission)` |
| `Opening` | positively transient failure | `Opening(retry_at)` |
| `Opening` | permanent/configuration/unknown failure | `Unavailable` |
| `Serving(Ready)` | recovery signal | `Serving(old, Pending)`; writes close |
| `Pending` | attempt admitted | `RecoveringPreEffect` |
| `RecoveringPreEffect` | first possibly durable call | `RecoveringEffectful` |
| Any recovery state | complete candidate, generation current | one swap to `Serving(new, Ready, role-derived admission)` |
| Any recovery state | positively transient failure | `Blocked(retry_at)` |
| Pre-effect state | permanent/configuration/invariant failure | `OperatorRequired` |
| Effectful state | unknown acknowledgement, panic, abort, or join failure | `NeedsRecapture` |
| `NeedsRecapture` | outcome proved | continue from proved durable state |
| `NeedsRecapture` | recapture remains unknown or violates an invariant | `OperatorRequired` |
| Active attempt | duplicate wake arrives | set dirty; current candidate remains eligible |
| Active attempt | configuration/authority generation changes | older result cannot publish `Ready` |

All counters reject overflow rather than wrapping into an ABA.

The runtime state is not durable truth. On restart it is reconstructed from the
applied graph configuration and durable graph state.

Readiness is part of the same atomically published runtime state; it is not
inferred loosely from an error label. `Pending` and pre-effect discovery close
writes. `RecoveringEffectful` and `NeedsRecapture` close both reads and writes.
A blocked/operator-required state records whether it failed before or after a
possible durable effect; post-effect states keep both admissions closed until a
fresh coherent activation. Hard read-only `Ready` means read-ready and
write-not-ready by role.

`Ready` additionally requires `RecoveryReport.remaining.is_empty()`, no
per-dimension `Unknown`, a terminal recovery witness, and a candidate whose
`ActivationWitness` matches final authority. A malformed or deferred intent may
leave a separately proved historical/read snapshot available, but it can never
produce write-ready or masquerade as terminal recovery.

## 8. Recovery and activation protocol

### 8.1 Trigger and quiescence

`RecoveryRequired`, open failure, an operator request, or a recovery probe MAY
wake the controller. The controller:

1. sets the coalesced wake bit and snapshots the configuration/authority
   generation;
2. atomically closes write admission and, before any effect, current-head read
   admission;
3. joins an existing attempt or becomes the sole owner;
4. waits for the admitted operations required by §5.6 to reach terminal engine
   results; and
5. acquires the authority appropriate to the requested action.

The initiating HTTP request never owns this task and is never replayed.

### 8.2 Bounded recovery units

One supervised recovery unit is one independently classifiable durable work
item. Most units are recovery-v9 sidecar envelopes. A sidecarless native-branch
control gap (for example a proved create/delete crash gap) is its own unit keyed
by exact stable branch/incarnation witnesses. An envelope may own many tables
because RFC-022's graph-atomic decision cannot be split. SchemaApply is
graph-global. Ordinary intents remain branch-scoped when independence is
structurally proven.

The engine exposes one canonical internal recovery owner, conceptually:

```rust
async fn discover_recovery_units(mode: RecoveryMode) -> RecoveryPlan;
async fn recover_one_unit(
    unit: RecoveryUnit,
    authority: RecoveryAuthority,
) -> Result<RecoveryReport, RecoveryFailure>;
```

Serving startup, exclusive startup, and the live controller orchestrate these
same primitives; they do not implement parallel recovery protocols. A final
candidate uses `VerifyFinal`, not an all-sidecar recovering open. The current
all-sidecar open behavior must be factored through these primitives before the
controller can claim unit-level budgets or gate release.

Recovery units are ordered deterministically. After each unit's durable
outcome, the next unit reopens fresh manifest authority. A failed intent is
never skipped in favor of a dependent later intent on the same branch.
Ordering is derived from persisted graph lineage, sidecar predecessor/base
witnesses, schema-state identity, and branch incarnation — never filename,
listing order, timestamp, or ULID sorting alone. SchemaApply is a graph-global
barrier. Unknown dependency blocks the dependent unit and prevents readiness.

New writers prospectively enforce finite bounds for envelope metadata bytes and
effects/tables per unit. Previously valid recovery-v9 envelopes remain readable:
discovery and classification stream them without retaining an unbounded plan,
and a single graph-atomic envelope is never split merely to fit a newer runtime
budget. Only violation of a bound already normative when that envelope was
written is malformed. Separately, runtime configuration bounds discovered units
per attempt, stalled tasks, and retired generations. Reaching the units-per-
attempt budget stops before admitting the next unit, releases gates where
independence allows, and schedules a fresh discovery.

An attempt budget stops admission of the next recovery unit. It does not drop
an effectful storage future and claim cancellation. Between units the graph
remains recovery-blocked.

Immediately before invoking the first call that may write durable state, the
controller atomically publishes `RecoveringEffectful`. A panic/abort before that
transition is pre-effect; a panic/abort at or after it is unknown until mandatory
recapture proves otherwise. The state transition is deliberately conservative:
it may classify a call that never reached storage as unknown, but never the
reverse.

### 8.3 Candidate construction

After permitted recovery units settle, the factory opens final durable
authority and builds the complete candidate described in §5.4. This fresh open
is intentional. Recovery is exceptional, and cold cache cost is lower liability
than teaching every future handle field how to participate in an in-place swap.

### 8.4 Activation

The attempt obtains the short transition mutex and verifies:

- its configuration/authority generation is still current;
- the applied configuration digest still matches;
- the registry still names the generation/state it expected; and
- the pre-mutex durable witness still matches the candidate.

The writer/exclusive authority and closed admission remain held from the final
durable recapture through activation. The final manifest/schema probe happens
before taking the mutex; under the V1 sole-writer contract no foreign effect may
land in the gap. No graph or object-store I/O runs under the short mutex. The
attempt then performs one `ArcSwap` store containing the candidate and `Ready`.
There is no fallible initialization after that store.

A stale attempt MUST NOT mark writes ready. The first implementation discards
its candidate and wakes the controller for the changed configuration or
authority. A duplicate hint alone is not staleness; its dirty bit schedules a
post-attempt probe without discarding an otherwise valid candidate.

### 8.5 Old generation retirement

For a non-mutating reload, requests already holding the prior `Arc` finish
naturally. Effectful V1 recovery has already drained managed request permits, so
activation cannot strand an untracked served request on the old generation.
Dataset and index caches are owned by that generation and disappear when its
final reference is dropped. Network pools may outlive it.

Runtime references are not durable GC leases. Existing cleanup rules still
require quiescence and may cause a separately raced old reader to finish or
fail loudly, but never retarget.

## 9. Read and write behavior during recovery

### 9.1 Writes

New writes close before recovery attempt admission. They receive a typed
service-unavailable result that distinguishes recovery from absence or policy
denial. Already-admitted server-owned writes continue to a terminal engine
result and retain their admission guards.

Recovery never automatically retries a user mutation, load, schema apply,
branch operation, or maintenance operation.

Served SchemaApply is not an ordinary in-generation write. Before its first
effect the server preflights resolved policy, external-Blob policy, embeddings,
and stored queries against the planned catalog as far as possible. Its durable
schema publication then flows through the same closed-admission,
candidate-build, and atomic-activation protocol. A server write facade cannot
publish a new accepted schema while leaving policy or stored queries compiled
against the old catalog. If a post-effect validation still fails, durable
progress is reported and the graph remains blocked pending recapture; the old
service generation is not labeled current.

### 9.2 Reads

An already-admitted read may finish on its captured generation because its
operation-local graph snapshot is immutable.

Before the first possible durable recovery effect, new reads MAY use the prior
generation if it remains a proved coherent view. Once a schema artifact or
graph manifest may have changed, V1 closes new current-head read admission until
a coherent generation is activated. This avoids silently serving an old
"latest" view after durable latest changed.

A future optimization may keep new reads available through a frozen immutable
read generation only after every read path is structurally generation-pure and
never re-probes mutable schema, branch, manifest, or cache state. That
optimization is not required for initial implementation.

Explicit historical reads MAY remain available only when their complete
snapshot/schema/incarnation proof is independent of the changing authority.
Otherwise they fail closed with the same recovery-unavailable result.

### 9.3 Terminal finalization before activation

V1 does not activate a graph-visible candidate while required recovery audit or
sidecar cleanup remains. Those steps are part of the terminal durable outcome.
If either fails or is ambiguous, the graph remains recovery-blocked and the next
attempt re-proves the visible outcome before finishing bookkeeping. This avoids
a second semi-ready serving state and ensures `Ready` always means that durable
recovery and runtime activation agree.

An old read admitted before the effect may finish only under the captured-
snapshot rules above. New reads do not enter a half-finalized generation.

## 10. Recovery result and failure model

Each recovery unit and its orchestration MUST return structured progress rather
than `Result<()>` plus a boolean.

```rust
struct RecoveryReport {
    actions: Vec<RecoveryAction>,
    table_heads: BTreeMap<TableIdentity, DurableChangeState>,
    manifest: DurableChangeState,
    schema_artifacts: DurableChangeState,
    audit: DurableChangeState,
    sidecars: BTreeMap<OperationId, DurableChangeState>,
    refs_or_paths: BTreeMap<StableObjectIdentity, DurableChangeState>,
    remaining: Vec<RecoveryBlocker>,
    final_authority_witness: Option<DurableAuthorityWitness>,
}

enum RecoveryAction {
    RolledForward { operation_id: String },
    Compensated { operation_id: String },
    OrphanDiscarded { operation_id: String },
    FinalizedVisibleOutcome { operation_id: String },
    AttemptedUnknown { operation_id: String, step: RecoveryStep },
    Deferred { operation_id: String, reason: RecoveryBlocker },
}

enum DurableChangeState {
    Unchanged,
    Changed,
    Unknown,
}

struct RecoveryFailure {
    source: OmniError,
    report: RecoveryReport,
}
```

The report is observability and retry context, not authority. Every retry
re-discovers storage.

The aggregate durable-change disposition is derived from the per-dimension
states: any `Unknown` makes the aggregate unknown; otherwise any `Changed`
makes it changed. A failure cannot encode an ambiguous manifest acknowledgement
as `manifest: Unchanged` merely because another dimension is already unknown.

An ambiguous object-store acknowledgement, cancellation after invoking
storage, or failure after one of several recovery units is never
`DurableChangeState::Unchanged` without a fresh proof.

Orphan-sidecar disposition explicitly reports its main-lineage publication,
audit append, and sidecar deletion. No caller may reduce that outcome to an
unqualified "processed" boolean.

## 11. Failure and cancellation matrix

| Window | Possible durable state | Required behavior |
|---|---|---|
| Before attempt admission | No new effect | Caller cancellation is effect-free. |
| After discovery, before first storage operation | Existing sidecar only | Prior coherent reads may continue; writes remain blocked. |
| After invoking storage, before outcome proof | Ambiguous | Mark effectful/unknown; inspect fresh authority. |
| After table Restore, before manifest publication | Lance HEAD may advance; old graph manifest remains | No new writes; old admitted reads may finish. |
| During schema promotion | Live/staging artifacts may be mixed | Block new current-head reads and writes; retry exact promotion or require operator. |
| After manifest publication, before audit/sidecar cleanup | Durable latest is newer than registry runtime and recovery is incomplete | Enter `NeedsRecapture`; admit no new latest operation; re-prove and finalize. |
| After terminal audit/sidecar cleanup, before activation | Durable recovery is complete; registry is old | Build/verify the candidate while admission and authority remain held. |
| After atomic activation | Durable and runtime state agree | Enter `Ready`. |

Dropping a caller future cancels only that wait. A caller deadline returns a
typed `RecoveryInProgress { attempt_id }`; it does not imply rollback, task
termination, or an effect-free outcome.

The controller MUST NOT abandon ownership of an effectful task merely because
a supervisor wait deadline expired. Individual storage operations need bounded
substrate retry/deadline behavior. A total attempt budget prevents starting
another recovery unit; it is not permission to drop the current unit halfway
through an ambiguous effect.

## 12. Scheduling, fairness, and shutdown

Each graph owns at most one active or stalled recovery task. A second signal
joins or fences that task; it never starts an overlapping attempt.

A finite global semaphore whose permits remain held by stalled tasks can starve
unrelated graphs. V1 therefore distinguishes:

- **active attempts**, which consume launch capacity; and
- **stalled attempts**, which retain per-graph ownership and observability but
  do not permanently consume another graph's launch slot.

Moving an attempt to the stalled set does not cancel it and does not permit a
second attempt for that graph. The maximum stalled set is the immutable
configured graph count. Active and stalled counts are separately observable.
The implementation MUST document any additional resource cost of a stalled
underlying storage future.

Each stalled attempt owns at most one in-flight substrate operation and one
controller task. Thus the configured graph count bounds retained tasks, but the
launch semaphore is explicitly not an execution-concurrency bound after an
operation stalls. A configured stalled-task ceiling stops admitting additional
effectful attempts and degrades readiness before process resources become
unbounded. Substrate calls still require terminal per-operation deadlines where
the backend supports them. The controller observes task completion, panic,
abort, and join failure; none may disappear without a state transition and
mandatory recapture when an effect might have been invoked.

Shutdown uses one overall budget and this order:

1. close served write admission;
2. drain server-owned writes;
3. stop supervisor producer loops;
4. stop new recovery attempt admission;
5. wait for owned active/stalled attempts within the remaining budget; and
6. terminate only with the complete durable authority — sidecars, schema
   artifacts, graph manifest, exact Lance state, native refs, and recovery audit
   — sufficient for next-boot classification if the budget is exhausted.

The server MUST NOT claim that recovery drained merely because it stopped
waiting. Hard process termination does not replay requests.

## 13. Failure classification and retry

Recovery scheduling consumes a conservative typed failure classification. The
classification may be owned by a sibling storage-failure RFC, but this RFC
requires at least:

```text
Transient     — positive typed evidence that retry is safe
Configuration — credentials, region, endpoint, unsupported setup
NotFound      — authoritative absence
Precondition  — existence/OCC/fence condition not satisfied
Permanent     — proved non-retryable invariant or data error
Unknown       — insufficient public evidence
```

Only `Transient` schedules automatic retry. `Unknown` does not mean transient.
Opaque wrapper errors and future variants default to `Unknown`. Retry uses
bounded exponential backoff with full jitter. `Retry-After` is emitted only
when a retry is actually scheduled.

An exception is not a retry: after any invoked effect whose acknowledgement is
unknown, the controller MUST perform one owned, non-mutating durable-authority
recapture before choosing a disposition. This is the `NeedsRecapture` state and
cannot start another effect. If recapture proves the outcome, normal
classification resumes. If recapture itself returns a positively typed
transient read failure, only that probe may be retried. A post-invocation task
panic, abort, or join failure also enters `NeedsRecapture`; a pre-effect panic is
an invariant failure. An unknown recapture becomes `OperatorRequired`. None
enters an immediate automatic effect loop.

Full backend details remain in bounded structured operator logs. Public status
never exposes a bucket, filesystem path, URI credentials, presigned query, or
raw substrate exception.

## 14. Server status and rolling compatibility

For each configured graph the server derives at least:

- state;
- `read_ready`;
- `write_ready`;
- retryability and retry time when scheduled;
- bounded failure class and summary;
- blocking operation/attempt ID when safe to expose;
- activation generation for logs/metrics; and
- active/stalled attempt counters.

New wire fields are optional during rolling deployment. A newer client must
accept an older `{ graph_id, uri }` graph record and apply documented defaults.
A state vocabulary exposed over the wire is forward-compatible: an unknown
future state cannot silently deserialize as ready.

The status route remains available when graph data routes are blocked. A graph
that never produced a coherent activation has no read availability. A prior
activation does not imply read readiness after durable latest may have changed;
the state machine follows §9.

## 15. Cost and observability

A ready graph performs no periodic Full open or full manifest fold merely to
prove that it remains ready. Signals are coalesced, and a cheap recovery probe
MAY list or witness the recovery namespace before admitting a Full attempt.

Metrics and structured events include:

- graph and attempt IDs;
- trigger class;
- authority mode;
- recovery units/actions;
- time waiting for write drain and gates;
- storage-operation and total attempt duration;
- active/stalled classification;
- durable-change state;
- candidate-build stage;
- activation success/stale-generation rejection;
- retry classification and deadline; and
- read/write readiness transitions.

No claim calls a Full manifest scan history-independent. Cost evidence compares
reads and bytes across several history depths; counting one open/scan at one
depth proves only reduced amplification, not flat cost.

## 16. Public and embedded API boundary

The server MUST NOT use public `Omnigraph::refresh()` as its Full-recovery
primitive.

Any future view API has non-mutating semantics:

```rust
async fn reload_view(&self) -> Result<ViewReloadReport>;
```

It may rebuild a coherent derived view, but it performs no Restore, recovery
publication, schema promotion, audit append, sidecar deletion, or native-ref
mutation.

Managed destructive recovery remains behind the graph factory/controller. If a
real embedded consumer later needs it, introduce a narrow opaque
`RecoveryController` or `RecoveryPermit`; do not infer authority from how an
ordinary `Omnigraph` was opened.

Manually constructed server/test entries may participate in automatic recovery
only when they provide a replacement factory carrying complete construction
authority. Absence is a typed managed-recovery-unavailable state, not permission
to copy unknown configuration from a live handle.

### 16.1 Managed operator entry point

V1 exposes exclusive compensation only as an operator action, conceptually:

```text
omnigraph cluster recover GRAPH --server PROFILE --exclusive --confirm-quiesced
```

The authenticated request reaches the target graph's controller in the one
configured writer process; it does not open a second ad-hoc writer. It requires
the externally supplied replica-quiescence assertion, closes and drains the
target graph, and leaves unrelated graphs serving. `--confirm-quiesced` records
an operator assertion; it is not misrepresented as a distributed fence. If the
deployment cannot establish sole-writer and replica quiescence, the supported
fallback is a one-shot offline recovery command after all processes serving that
graph root are stopped.

The action returns the typed attempt/report identity and may time out waiting,
but the controller retains task ownership. Tests cover authorization, wrong
process role, absent quiescence assertion, disconnected caller, and exact target
graph isolation. The final CLI/HTTP spelling may follow the control-plane
conventions current when Phase 4 lands, but an ordinary data request or ordinary
read-write open is never the exclusive-recovery entry point.

## 17. Implementation sequence

### Phase 0 — decision and topology

- Accept this RFC.
- Make one-read-write-owner support explicit across cluster, deployment, and
  server docs.
- Define hard read-only replica behavior and ensure served write routes cannot
  acquire write/recovery authority there.
- Record the immutable process role in applied startup configuration and require
  external orchestration to guarantee sole-writer uniqueness.
- Land the conservative typed failure classification or disable automatic
  retries until it exists.

### Phase 1 — truthful recovery results

- Replace boolean/`Result<()>` recovery summaries with `RecoveryReport` and
  partial-progress failures.
- Make every writer-kind recovery action populate exact changed dimensions.
- Pin orphan discard, multi-sidecar partial progress, and ambiguous storage
  outcomes.

### Phase 2 — fresh construction and activation

- Retain immutable applied `GraphStartupConfig` in each registry entry.
- Resolve and content-bind Cedar policy, external-Blob policy, embedding
  configuration, and stored queries before any effectful recovery step.
- Build a complete fresh activation through the canonical graph factory.
- Give activations fresh correctness-bearing caches.
- Publish the complete runtime and readiness through one ArcSwap store.
- Capture one activation per request.

### Phase 3 — controller and availability state

- Add singleflight signals, generation fencing, retry/backoff, active/stalled
  task ownership, and optional rolling-safe status fields.
- Add linearizable closeable read/write admission and server-owned operation
  lifetime tracking.
- Keep writes closed through recovery and activation.
- Implement the conservative read-admission rules in §9.

### Phase 4 — exclusive compensation

- Add explicit leader/exclusive-recovery authority.
- Extract the canonical RFC-022 discovery and one-envelope recovery primitives;
  make `ReadOnlyProbe`, `ServingOpen`, `ExclusiveRecovery`, and `VerifyFinal`
  explicit engine modes.
- Add every cancellation, SchemaApply, branch-ABA, and multi-sidecar gate in
  §18 before enabling managed exclusive compensation on the supported leader.

### Phase 5 — served operation lifetime and shutdown

- Finish the server-owned write and request-lifetime work atop the final
  controller; effectful Phase 4 remains disabled until this owner exists.
- Compose write drain, supervisor shutdown, recovery-attempt drain, and Axum
  connection cutoff under one honest budget.
- Run its failpoint suite in canonical CI with the server failpoint feature
  explicitly enabled.

### Future Gate 0 — detached publication

Evaluate Lance `commit_detached` as a separate no-production-change study. It
could stage table effects without advancing ordinary table HEAD, making failed
attempts a reachability-GC problem rather than a Restore problem. The gate must
first prove reopen, branch/history, first-touch creation, index/maintenance,
retention, cleanup, and bounded orphan behavior on local and S3 storage.

Lance 10 exposes detached manifests through `list_detached_manifests()` while
excluding them from ordinary `Dataset::versions()`. Normal cleanup enumerates
ordinary manifests, so data reachable only through a detached manifest can be
treated as unreferenced after the unverified-retention threshold (or immediately
under aggressive cleanup). `commit_detached` also cannot create the first
dataset. OmniGraph cleanup therefore MUST fail closed whenever graph-reachable
detached versions exist until detached manifests participate in reachability.

Gate 0 must test the next mutation based on a detached version;
`latest_version_id`/staleness; every `HEAD > pin`, `pin + 1`, and "at or beyond"
assumption; branch/tag refs to high-bit unordered IDs; index/Optimize lineage;
first-touch creation; and detached-only data surviving aged cleanup. These risks
make detached publication unsuitable for this RFC's first implementation.

### Future Gate 1 — distributed writer fencing

A separate RFC may replace the external sole-writer guarantee only after a
linearizable epoch is consumed by every authoritative effect listed in §6.4.
Its decisive test pauses leader A after its local check, advances the epoch and
commits through leader B, resumes A, and proves A cannot perform an ordinary
Lance commit, Restore, graph-manifest publication, sidecar mutation, schema
promotion, native-ref operation, or destructive path cleanup. This is future-
fence acceptance, not a claim that V1 can refuse every misconfigured writer.

## 18. Required acceptance evidence

### 18.1 Recovery engine

Extend existing RFC-022 owners rather than creating a parallel protocol suite:

- every writer kind rolls forward or compensates through the one canonical
  bounded recovery owner, followed by `VerifyFinal` open;
- failures before and after each table effect, Restore, graph publication,
  schema promotion, audit append, and sidecar deletion remain deterministic on
  retry;
- an error after one of several sidecars carries partial durable progress;
- orphan discard reports and immediately recaptures its main-lineage commit;
- every later sidecar classifies against authority refreshed after the prior
  outcome;
- malformed/foreign/future intents remain untouched and block writes;
- a live in-process writer and recovery serialize through root gates;
- ordinary/read-only/serving open cannot Restore, delete an owned ref/path, or
  reverse schema staging without an `ExclusiveRecoveryPermit`;
- SchemaApply promotion, identity-bound Optimize forward completion, and
  orphan-branch finalization each prove their adapter-specific forward boundary;
  and
- with many sidecars, an attempt budget prevents admission of unit N+1 and
  releases graph-wide gates between units wherever RFC-022 independence allows.

### 18.2 Atomic activation

Server integration tests prove:

- failure during recovery, open, schema/catalog build, policy load, embedding
  attachment, or stored-query validation never publishes a partial candidate;
- success stores the complete candidate and `Ready` exactly once;
- an old request retains its old activation while a later request receives the
  new activation;
- schema, catalog, policy, and query registry always share one generation;
- recovered SchemaApply that invalidates a stored query blocks activation;
- a changed configuration/authority generation prevents an older attempt from
  publishing ready;
- duplicate wake hints coalesce without invalidating a valid candidate or
  losing the latest blocked state; and
- a request paused after observing readiness but before permit acquisition
  cannot escape a closed admission gate; and
- a parked old request across three non-mutating reloads cannot exceed the
  configured retired-generation bound or publish components out of order.

### 18.3 Cache and branch ABA

Tests park an old generation's dataset-handle and graph-index cache misses
across activation, including e-tag-less local storage and same-name/same-version
branch deletion/recreation. Completion cannot populate or satisfy the new
generation's lookup. Old-incarnation recovery never restores, deletes, or
publishes through the recreated name.

### 18.4 Schema coherence

SchemaApply failpoint owners prove source, accepted IR/state identity, catalog,
manifest snapshot, policy, and stored queries activate as one unit. A read-only
view reload performs zero durable writes and either constructs a coherent view
or returns a typed recovery-required result.

### 18.5 Cancellation and scheduling

- caller timeout/disconnect does not cancel recovery;
- pre-effect failure is distinguished from ambiguous/post-effect failure;
- panic/abort immediately before the effectful-state transition remains
  pre-effect, while panic/abort immediately after it mandates recapture and can
  never become ready directly;
- one poison sidecar cannot cause overlapping attempts;
- four stalled attempts do not prevent an unrelated graph from beginning under
  the active/stalled scheduler model;
- no second attempt starts for a graph whose prior task is owned;
- shutdown never reports a task drained merely because its wait expired; and
- process termination leaves complete durable authority sufficient for
  next-boot recovery.

### 18.6 Topology and substrate

- server read-only replicas cannot reach graph writers, sidecar mutation,
  Restore, recovery publication, or ref deletion;
- a hard-read-only process parked across leader SchemaApply and branch
  delete/recreate returns its old captured snapshot or unavailable, never a
  mixed manifest/catalog/incarnation;
- a two-read-write-process adversarial test demonstrates that process-local
  queues are not shared and pins the Restore/orphan hazard as unsupported until
  distributed fencing exists;
- Lance upgrade guards detect any change to Restore conflict resolution,
  detached-manifest listing/cleanup, and native branch-identifier semantics and
  block upgrade pending review; and
- configured RustFS/S3 tests cover ambiguous remote outcomes, genuine ETags,
  retry exhaustion, and post-publication recapture.

### 18.7 Wire and cost

- old graph-list payloads deserialize in new clients;
- future/unknown state does not become ready;
- 503 and `Retry-After` match actual scheduler state;
- public failures redact storage placement and credentials;
- a ready graph performs no periodic Full open; and
- manifest reads/bytes are measured at multiple history depths without a false
  flat-cost claim.

### 18.8 Test ownership and negative injection

- Engine recovery primitives are owned beside
  `src/db/manifest/recovery.rs` and `tests/failpoints.rs`.
- Server state and activation are owned by `src/registry.rs` plus one focused
  `tests/recovery_activation.rs` suite.
- Schema-generation composition extends engine `tests/schema_apply.rs` and
  server `schema_routes.rs`/`stored_queries.rs`.
- Cache/branch ABA extends `warm_read_cost.rs` and
  `lance_surface_guards.rs`.
- `docs/dev/testing.md` maps every owner before implementation merges.

Canonical CI explicitly enables `omnigraph-server/failpoints`; a feature-gated
test target reporting zero tests fails its non-vacuity guard. Negative-injection
tests replace each critical boundary in turn: shared old/new cache namespace,
split component publication, dropped task ownership, missing exclusive permit,
false `Unknown -> Transient`, and absent admission registration. Each injection
must make the named acceptance owner fail.

## 19. Invariants and deny-list check

This RFC strengthens, rather than relaxes, the always-on invariants:

- **Atomic graph visibility:** RFC-022 remains the only durable publication
  protocol.
- **Snapshot isolation:** each request captures one activated generation and
  one operation-local graph snapshot.
- **Logical over physical state:** sidecars and physical HEADs never become a
  second public truth.
- **One source of truth, cheaply derived:** runtime status and generations are
  rebuilt from durable state and immutable configuration. There is no durable
  job queue.
- **No silent failures:** partial durable progress, cancellation, unknown
  classification, and stale activation are explicit.
- **Respect the substrate:** Restore's real conflict semantics define the
  exclusive-authority boundary.
- **No custom WAL/transaction manager:** recovery-v9 and Lance remain the
  protocol owners.
- **No cloud-only correctness:** the same authority and cache-generation rules
  apply to local, S3, and compatible stores.

The deliberate cold reopen is not forbidden cold re-derivation on every call.
It occurs only after an exceptional recovery signal. Healthy requests continue
to use a warm activated generation and cheap probes.

## 20. Security and operational boundaries

- Public failures never expose backend URIs, paths, credentials, presigned
  query strings, or raw Lance/object-store errors.
- Recovery obeys the same graph identity and applied policy/configuration
  binding as ordinary serving. Recovery authority does not grant a client
  operation authorization bypass.
- Actor identity for recovered lineage/audit remains the persisted original or
  recovery actor defined by RFC-022; the supervisor never accepts an actor from
  an HTTP request.
- Read-only replicas receive no write/recovery capability even if their
  credentials are accidentally broad.
- Operator documentation must state when a graph is read-only, write-blocked,
  retrying, stalled, or requires exclusive intervention.
- A support runbook must explain how to quiesce the sole writer before manual
  compensation and how to verify final readiness.

## 21. Drawbacks

- Recovery pays an extra graph open and begins with cold dataset/index caches.
- An active generation, inactive candidate, and the configured bounded set of
  retired generations may briefly coexist; memory pressure can delay reload.
- The registry must retain canonical construction configuration and a graph
  factory, including policy and stored-query validation.
- The conservative initial read boundary may temporarily reduce read
  availability after a durable recovery effect.
- One-writer topology limits horizontal write availability until a real fence
  exists.
- Structured recovery results and failpoint coverage add implementation work.

These costs occur on an exceptional path. They replace ongoing coupling in
every future coordinator, schema, cache, policy, query, and transport change.

## 22. Rejected alternatives

### 22.1 Destructive in-place `refresh`

Rejected. View freshness and durable compensation have different authority,
latency, cancellation, and observability contracts. Publishing a coordinator,
schema view, and cleared shared caches in sequence is not one structural
generation boundary.

### 22.2 Composite in-engine `ArcSwap<RuntimeView>` in this slice

Generation-local engine state is conceptually sound, but current ordinary
writes intentionally mutate coordinator state inside `Omnigraph`. Refactoring
the entire writer around an immutable engine generation is broader than the
recovery problem. The server already owns the safer complete-handle activation
boundary.

### 22.3 Restart-only recovery

Correct but operationally coarse. Fresh construction plus registry activation
reuses the same capability-gated RFC-022 recovery owner without restarting
unrelated graphs.

### 22.4 Background durable recovery queue

Rejected. Work is derivable from sidecars, manifest, schema artifacts, and
Lance state. Notifications wake a reconciler; they are not durable work
descriptions.

### 22.5 Lease-only leader lock

Rejected as a correctness fence. A paused holder can resume after expiry and
publish an already-prepared commit. A lease becomes a fence only when its
monotonic epoch is atomically consumed by every storage commit.

### 22.6 HEAD check immediately before or after Restore

Rejected. The pre-check has a check-then-act window; the post-check discovers
the lost update only after the destructive commit landed.

### 22.7 Automatic replay of the interrupted write

Rejected. Recovery proves graph state, not whether the client observed success.
Replay requires a separate idempotency and receipt protocol.

### 22.8 Detached commits now

Rejected for implementation, retained as Phase Future Gate 0. Detached versions
change version numbering, ordinary history enumeration, dataset creation,
branch/index behavior, and cleanup ownership. They are separately enumerable,
but ordinary cleanup does not treat them as the normal reachable history. They
require independent evidence and a storage migration design.

## 23. Compatibility and reversibility

The RFC adds no stored field and changes no graph, Lance, schema, manifest, or
sidecar format. Existing graphs and recovery-v9 intents remain readable by the
current open-time protocol.

It intentionally narrows an observable embedded behavior: ordinary read-write
open is RollForwardOnly and returns `RecoveryRequired` instead of silently
performing compensation. Exclusive startup/recovery remains available only
through an authority-bearing managed entry point. Release notes and API docs
must call out that safety boundary.

The server orchestration is reversible: operators can fall back to quiesced
managed exclusive startup recovery without migrating data. An ordinary
read-write open remains RollForwardOnly. Optional status fields follow the
normal additive rolling-wire policy.

The one-writer boundary is a compatibility clarification, not a new claim that
mixed versions may write concurrently. Mixed server versions MUST NOT act as
overlapping writers for one graph root.

## 24. Unresolved questions

The following are implementation choices for review, not licenses to weaken the
invariants:

1. Whether the initial server exposes a dedicated readiness endpoint or keeps
   readiness entirely in authenticated graph status.
2. The exact active/stalled scheduler limits and substrate-level terminal
   deadlines.
3. Whether embedded managed recovery has a demonstrated caller sufficient to
   justify a public opaque `RecoveryController` in the first implementation.
4. Whether a future frozen immutable read generation can preserve new read
   admission after durable progress without consulting mutable latest state.

None blocks the central decision: recovery is explicit and authoritative,
construction is fresh, activation is whole-generation, caches are generation
scoped, and compensation does not run without exclusive writer authority.
