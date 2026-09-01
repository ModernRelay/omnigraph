---
rfc: "0036"
title: "Atomic runtime activation and graph availability supervision"
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
blocked_on:
  - RFC 0034 acceptance and implementation
  - RFC 0035 acceptance and implementation
---

# RFC 0036: Atomic runtime activation and graph availability supervision

- **Depends on:** RFC 0034 durable recovery authority; RFC 0035 served-operation
  admission and lifetime; RFC 0038 typed storage-failure classification;
  internal manifest schema v6; Lance 10.0.0.
- **Replaces:** the unmerged graph-supervision direction in PR #489. Useful
  tests and operational requirements from that PR remain inputs, but its
  implementation is not the architecture.
- **Audience:** server, engine, cluster, API, operations, and release
  maintainers.

## 0. Decision summary

Each graph serves one immutable generation containing engine, accepted catalog,
policy, queries, providers, witnesses, readiness, and correctness caches. None
publishes independently.

A supervisor builds away from serving. Its supervision-owned task set obtains
RFC 0034's opaque `FinalizedRecoveryGuard` by recovery or no-effect verification.
The graph factory borrows that guard under the same root-gate domain,
opens a fresh verified engine without running recovery itself, and returns a
complete candidate which owns the guard. One shutdown-latched operation closes
every predecessor lane, registers and stores the fresh RFC 0035 cell, then
releases the guard. No fallible work follows predecessor close.

Requests pin one generation and its local caches; hard read-only replicas have no
write capability. Supervisors are singleflight, wakes coalesce without resetting
backoff, and bounded graph-local work cannot block peers. One state derives
readiness. Status is additive; unavailable entries require `include=all`.

## 1. Problem

The routed engine, catalog, policy, queries, providers, witnesses, and caches
must be coherent; replacing one can create a combination that never existed.
Lance versions pin reads but do not atomically refresh this composite, so it
needs one immutable value and one publication point.

Graphs can fail open, require recovery, or become schema-stale. Restarting peers
is too broad; discarding a provably safe old read view is too destructive.

## 2. Scope and dependency boundaries

This RFC defines complete generations, atomic activation, component coherence,
stale-attempt/cache-ABA fencing, hard read-only behavior, bounded fair
supervision, and rolling-safe status. A non-mutating candidate failure preserves
an old generation only while its admission contract remains safe.

### Dependency contract

RFC 0034 owns all durable recovery semantics, including recovery authority,
sidecar classification, roll-forward or compensation choice, Restore, audit,
and terminal durable cleanup. RFC 0036 sees only an opaque
`FinalizedRecoveryGuard` and RFC 0034's `RecoveryOutcome` with
`RecoveryDisposition::{Clean, RollForwardRequired, NeedsCompensation,
Blocked}`. It MUST NOT inspect a sidecar, infer terminal recovery from table
HEADs, or translate those variants into a second recovery vocabulary.

V1 managed supervision requests only RFC 0034 `RollForwardOnly` recovery and
cannot mint `ExclusiveRecoveryPermit`. `RecoveryDisposition::NeedsCompensation` becomes
`Blocked/OperatorRequired` with no retry; offline recovery may proceed only after
all serving processes for that graph stop. A healthy writable candidate uses
RFC 0034's effect-free `ReadWriteNoRecovery(ReadWriteLeaderGuard)`: it never runs
recovery, returns the exact non-clean disposition when a durable unit exists,
and on `Clean` returns the equivalent terminal `FinalizedRecoveryGuard`.

RFC 0035 owns served-operation admission, request/task lifetime, disconnect
behavior, per-graph draining, and shutdown ordering. RFC 0036 consumes its
indivisible serving cell: one non-reusable token, exact `ReadObserver` and `Write`
admission lanes, and one runtime `Arc`. A request retains the matching lane
permit and that `Arc` until terminal completion. RFC 0036 chooses which lanes a
transition closes; RFC 0035 implements close/drain and task settlement.

Fresh cells publish only through RFC 0035's process-wide
`ServingRegistrationLatch`: `register_and_publish` for an initial cell and
`replace_and_publish` for a replacement. Both linearize cell registration and
the registry store against `Running -> Stopping`; RFC 0035 owns their mechanics.

RFC 0036 also consumes RFC 0035's typed `TransitionDrain`. Required lanes close
before recovery or build, and only `Drained(DrainedProof)` authorizes progress.
`DrainPending` parks the strong drain and does not authorize work. Recovery tasks
join RFC 0035's one `ShutdownDeadline`; this RFC neither resets it nor defines cutoff.

RFC 0038 owns the mapping from substrate errors to stable condition classes. A
condition class is not replay authority. This RFC may consider `Transient` only
after its operation-local effect and recovery rules authorize another attempt;
it does not classify by matching error strings or redefine the taxonomy.

This RFC does not define recovery formats or choices, HTTP-write cancellation
or acknowledgement, shutdown, hot config reload, graph add/remove, multi-writer
fencing, durable reader leases, or warm correctness-cache transfer. It exposes
no raw storage diagnostic over HTTP.

## 3. Normative invariants

1. Applied configuration, Lance, and `__manifest` remain authoritative;
   runtime and supervisor state are derived.
2. A request captures one exact serving-cell token, lane permit, and generation;
   every runtime component and admission identity publishes in one store.
3. Published component bindings are immutable; mutable data tips may advance
   only within the generation's accepted-schema identity.
4. Candidates and correctness caches are fresh; old work cannot reach them.
5. `FinalizedRecoveryGuard` has one move path from its counted writable task,
   through the unactivated wrapper, to post-store release; factories borrow it.
6. Both factory modes are effect-free. Every writable candidate owns a finalized
   guard; managed recovery is roll-forward-only; read-only is never write-ready.
7. No recovery or build starts without `DrainedProof`; pending drains hold no
   global scheduler permit and never reopen a lane.
8. Readiness derives from one state. A closed gate never reopens; reuse of an
   unchanged runtime requires a fresh serving cell and a no-change proof.
9. Replacement closes every predecessor lane before publishing its fresh cell,
   all under the shutdown latch; no old permit can start after the store.
10. Generation residency, retries, queue depth, build time, and hot-path work
   are bounded and observable.

## 4. Immutable construction input

Every configured graph entry owns one `AppliedGraphConfig`:

```rust
struct AppliedGraphConfig {
    graph_key: GraphKey,
    canonical_uri: CanonicalGraphUri,
    role: ServingRole,
    policy_bundle: Option<DigestBound<CedarSource>>,
    stored_queries: DigestBound<Vec<StoredQuerySource>>,
    embedding: Option<ResolvedEmbeddingConfig>,
    external_blob_policy: ExternalBlobPolicy,
    applied_revision: AppliedRevision,
    config_digest: ConfigDigest,
}
```

Configuration is resolved, validated, canonicalized, and digest-bound before an
attempt. Applied content, not its mutable path, is the input. Credentials may be
private handles but never enter status. Duplicate keys/roots, unattributable
cluster policy, or invalid applied revision are cluster-global ambiguity and
abort startup; a graph-local failure instead creates a visible blocked entry.
A monotonic `config_generation` fences future reloads from older builds.

## 5. Complete runtime generation

Conceptually:

```rust
struct GraphGeneration {
    activation_id: ActivationId,
    config_digest: ConfigDigest,
    read: Arc<GraphReadFacade>,
    write: Option<Arc<GraphWriteFacade>>,
    catalog: Arc<Catalog>,
    policy: Option<Arc<PolicyEngine>>,
    queries: Arc<QueryRegistry>,
    providers: Arc<GraphProviders>,
    witness: GenerationWitness,
    caches: Arc<GenerationCaches>,
}

struct GenerationWitness {
    graph_identity: GraphIdentity,
    manifest_incarnation: ManifestIncarnation,
    manifest_version: u64,
    accepted_schema_identity: AcceptedSchemaIdentity,
    applied_revision: AppliedRevision,
    recovery: Option<RecoveryTerminalWitness>,
}

struct UnactivatedGeneration {
    cell: Arc<ServedAdmissionCell>, // handle slot is Arc<GraphGeneration>
    finalized_guard: Option<FinalizedRecoveryGuard>, // Some for every writer
}
```

RFC 0036 instantiates RFC 0035's opaque cell handle as `Arc<GraphGeneration>`.
The runtime, token, lanes, and executor therefore cannot mix across epochs.
Facades are private: reads cannot yield a writer. Each read-write
entry retains one private, persistent `ReadWriteLeaderGuard`; the effect-free
factory mode uses it to create each write facade. `FinalizedRecoveryGuard` is
installation authority only and never enters a facade. A read-only entry owns
no leader guard and creates no write facade.

The witness proves that engine catalog and schema came from one manifest view,
policy was compiled for the configured graph key, stored queries were checked
against that catalog, and service content matches `config_digest`. A rebuild
with a predecessor must retain its durable graph identity.

The `RecoveryTaskSet` graph slot owns every writable path and its non-cloneable
guard: roll-forward supplies it after recovery; clean `ReadWriteNoRecovery`
supplies its equivalent by effect-free verification. The factory borrows it
under the same root-gate domain, then the wrapper owns it. Task ownership remains
counted through activation/drop, with no handoff gap. Store precedes guard release;
failure, timeout, stale result, or discard drops it. Only `ReadOnlyProbe` may
produce a wrapper with `None`.

The witness is an installation proof, not a claim that data commits stop. Data
HEADs may advance normally; an accepted-schema identity change requires a new
generation.

## 6. Registry state and generation state machine

Each graph has one stable `GraphEntry`. Its atomically replaceable value is an
immutable `RuntimeState`:

```rust
enum RuntimeState {
    Opening { attempt: AttemptView },
    Serving { cell: Arc<ServedAdmissionCell> },
    Draining {
        attempt: AttemptView,
        drain: Arc<TransitionDrain<GraphGeneration>>,
        counts: Option<PermitCounts>,
    },
    Rebuilding {
        old: Option<Arc<ServedAdmissionCell>>,
        attempt: AttemptView,
    },
    Blocked {
        old: Option<Arc<ServedAdmissionCell>>,
        failure: PublicFailureClass,
        retry: Option<RetrySchedule>,
    },
}
```

`Unavailable` is represented by `Blocked { old: None, .. }`; there is no
second terminal state with subtly different routing. Public state names are a
projection and need not mirror Rust variants.

The state transitions are:

```text
Opening ──success──▶ Serving(G1/E1)       Opening ──failure──▶ Blocked
Serving ──close required lanes──▶ Draining
Draining ──DrainedProof──▶ Rebuilding     Draining ──pending──▶ parked Draining
Rebuilding ──Clean───▶ Serving(G2/E2)
           ├──pre-effect unchanged──▶ Serving(G1/E2) or Blocked(G1/E1)
           ├──RollForwardRequired───▶ fair recovery continuation
           ├──NeedsCompensation─────▶ Blocked(OperatorRequired)
           ├──Blocked───────────────▶ Blocked(G1/E1)
           └──stale─────────────────▶ discard candidate
```

The transition mask is external input, not a state-machine guess. Unselected
safe reads may remain open; selected lanes stay closed through `Draining` and
`Rebuilding`, and new writes are never admitted there.

Every admission lane is one-way. `E1` is never reopened or stored again as a
new `Serving` value after closure. A pre-effect failure may republish the same
G1 runtime only in fresh epoch E2, after a fresh proof that applied config,
durable authority, and accepted-schema identity did not change and no durable
recovery effect began. The transition proves required E1 lanes drained and
mints a new token and gates. If an effect may have occurred, the graph remains
`Blocked`; only an old lane that was never closed and is explicitly safe under
the external proof can contribute readiness.

Readiness is derived as follows:

- `read_ready` requires an open `ReadObserver` on the current/drain-held cell
  whose transition proof permits routing;
- `write_ready` is true only for `Serving`, with a write facade and open write
  lane; and
- a hard read-only role always reports `write_ready = false`.

`DrainPending` is internal `transition_drain_pending`/`blocked` and public
`draining`, with no retry or public counts.
`RecoveryDisposition::NeedsCompensation` projects blocked `operator_required`,
no retry, and only unclosed-lane readiness. `RecoveryDisposition::Blocked`
projects its RFC 0034-owned blocker without guessing retryability.

## 7. Candidate construction and atomic activation

### 7.1 Build protocol

For one attempt, the supervisor:

1. captures `AppliedGraphConfig`, `config_generation`, the expected runtime
   pointer, and a fresh attempt token;
2. with a predecessor, chooses a mask, calls `begin_transition_drain`, publishes
   `Draining { counts: None }`, and waits to `TransitionDeadline` (not shutdown's);
   effectful recovery selects `AdmissionMask::All`; no predecessor is vacuously
   drained;
3. on `Drained(proof)`, retains the proof and rechecks config/authority. On
   `DrainPending { counts }`, parks the strong drain with `Some(counts)`, releases
   any reserved build/recovery permit, and stops: no recovery/build runs. Only
   opaque `DrainReadyWake` wakes a recheck; the wake itself is never proof;
4. with proof, consumes the exact `RecoveryDisposition`. `RollForwardRequired`
   requests only `RollForwardOnly`; a bounded continuation yields fairly and
   retains closed graph-local state without a build permit.
   `NeedsCompensation` publishes `Blocked/OperatorRequired`, `Blocked` preserves
   the RFC 0034 blocker, and only `Clean` supplies the finalized guard;
5. invokes only `ReadWriteNoRecovery(ReadWriteLeaderGuard)` for writable
   construction, borrowing that existing guard after recovery or obtaining an
   equivalent guard by no-effect verification. A hard read-only path instead
   invokes `ReadOnlyProbe` and carries no guard;
6. builds fresh schema/catalog, policy, queries, providers, facades, and caches
   from one coherent view, then binds the generation into a fresh RFC 0035 cell;
7. checks the complete witness and moves any finalized guard into one
   `UnactivatedGeneration`; and
8. submits that wrapper for activation.

Neither factory mode performs recovery or falls back to Full. Writable open
returns `RollForwardRequired`, `NeedsCompensation`, or `Blocked` exactly for a
durable unit. The supervisor retains its drain proof and enters managed recovery
only for `RollForwardRequired`; a `Clean` result includes the guard.

Candidate construction is graph-state read-only. It may allocate local memory,
open network connections, and read object storage, but creates no durable graph
effect. Its scoped children may therefore be canceled at the build deadline.
The separately owned recovery task is not a child and is never canceled by that
deadline.

### 7.2 Activation linearization point

Activation takes the short transition mutex, performs no I/O, and checks:

- entry identity, `config_generation`, active attempt, and expected predecessor;
- config digest and predecessor durable graph identity;
- exact predecessor token/mask `DrainedProof`, or proof of no predecessor;
- the candidate admission token and both gates are fresh and unpublished; and
- residency limits permit retirement of the predecessor.

Failure drops the wrapper and guard. Under the transition mutex, initial activation
calls `register_and_publish(new, no_fail_store)`; replacement calls
`replace_and_publish(old, new, proof, no_fail_store)`, consuming the matching
token/mask `DrainedProof`. The latch first prunes dead entries
and rejects `Stopping` or capacity exhaustion without effect. Replacement then:

1. closes predecessor `AdmissionMask::All` and captures its token-bound drain;
2. registers a weak reference to the fresh cell; and
3. performs the validated, infallible `ArcSwap::store(Serving { cell: new })`.

The store is the activation point. Nothing fallible and no I/O/await follows G1
close: a G1 acquire either precedes close and joins its drain, or fails, so none
starts after G2 publication. The guard then releases and retirement receives the
G1 drain. All routing components come from the newly loaded state.

### 7.3 Request capture and retirement

Routing asks one loaded cell for its route-matching permit and
`Arc<GraphGeneration>`; token, permit, and `Arc` cannot mismatch. A raced closure
fails without reload. The read capture or owned-write task retains the pair.

Existing G1 permits retain its runtime. Outside both locks, an entry-owned,
request-independent retirement future awaits the `All` drain, obtains
`AllDrainedProof`, calls `deregister_drained(G1.token, proof)`, then drops its
strong cell reference. The bounded weak index cannot retain generations and prunes dead entries.

Deregistration and `Stopping` share the latch. If deregistration wins, both-lane
proof makes omission safe; if stop wins, its upgraded strong snapshot retains
G1 regardless of later index removal. At most one retiring generation is
admitted; another non-essential build waits.

## 8. Schema, policy, and query coherence

Data movement under the same accepted-schema identity may refresh the engine tip
through existing coherent snapshot logic. A different identity must never
appear behind the catalog of an in-flight generation. The engine exposes a
schema token; a fresh operation seeing a mismatch returns `GenerationStale`
before schema-dependent work and wakes the supervisor.

Policy and query content come only from `AppliedGraphConfig`, never reread
paths. All queries check against the candidate catalog. Any query or policy
compile failure rejects the whole candidate; it neither publishes a partial
registry nor borrows predecessor policy for availability.

## 9. Cache ownership and the ABA proof

`GenerationCaches` owns state-dependent Lance sessions/metadata, manifest and
branch projections, dataset handles, topology indexes, and compiled/planned
query artifacts. Connection pools, TLS clients, credential providers, and an
`ObjectStoreRegistry` may be shared because they do not select graph contents;
mutable-tip manifest and graph-index caches may not.

The ABA proof is structural:

1. Request R captures generation G1 and can reach only G1 caches.
2. G2 is built with a distinct cache namespace before activation.
3. Activation changes the entry pointer, not G1's fields.
4. A late asynchronous fill from R still holds G1 and writes only G1.
5. A new request captures G2 and cannot observe that fill.

G1/E2 retains G1's cache namespace only after the no-change proof; E1 stays
closed and E2's fresh token prevents serving-identity ABA.

An unavoidably shared lower cache key additionally includes process nonce,
activation ID, graph identity, stable table/branch incarnation, exact version,
and e-tag when available. Generation separation, not optional filesystem e-tags,
is the final same-path/version ABA defense. Cold warm-up is accepted; copying or
broad invalidation would weaken the proof.

## 10. Hard read-only replicas

A read-only server entry is a different capability, not a writable entry with
a boolean checked in handlers.

- Its applied role can construct only `GraphReadFacade`.
- Its factory always uses `ReadOnlyProbe`.
- It has no `ReadWriteLeaderGuard`; a finalized guard could not confer one.
- It cannot submit durable recovery or take the verified-recovery factory path.
- It never reports write readiness, including after a successful rebuild.
- Pending recovery leaves it blocked or serving only the exact old reads that
  RFC 0034/RFC 0035 explicitly deem safe.

An external writer may advance durable state. A cheap manifest/schema probe can
wake a read-only rebuild. Requests already on G1 remain pinned; a complete G2
replaces it only after verification. A transient G2 build failure does not
destroy G1.

This is local capability enforcement; RFC 0034 defines write topology.

## 11. Availability supervision

### 11.1 Ownership and wakes

One supervisor owns each stable graph entry for the server lifetime. It starts
from the configured entry even when no generation can open. Wake sources are
hints only:

- initial startup;
- admitted-operation `RecoveryRequired`/`GenerationStale`, or `DrainReadyWake`;
- completion of RFC 0034 recovery;
- a cheap durable freshness probe;
- an operator retry request; and
- a scheduled retry deadline.

Wakes set one dirty bit and notify the owner. Duplicate wakes coalesce. A wake
does not reset `consecutive_failures`, shorten an existing backoff, or create a
second attempt. After an attempt settles, the supervisor consumes the dirty bit
and re-probes authority before deciding whether more work exists.

The supervision layer owns one process-wide `RecoveryTaskSet`, the recovery
executor; each graph supervisor exclusively owns its graph slot. At most one
terminally owned writable-authority task exists per graph, tracked until a typed
failure or wrapper activation/drop, even if its waiter disappears. Only that
counted task receives `FinalizedRecoveryGuard`, lends it to the factory, and
retains the wrapper. A candidate timeout may cancel the effect-free factory
scope; it cannot detach the recovery task, which must settle and dispose of any
retained guard.

The task set is an RFC 0035 `ShutdownParticipant`. It receives the same absolute
`ShutdownDeadline` as connection and served-operation drain and may consume
only the time remaining. RFC 0035 owns registration and cutoff; RFC 0034 owns all
recovery behavior. This RFC defines only task ownership and scheduling.

Supervisor memory is not a durable queue. Process restart reconstructs work
from applied configuration and durable graph authority.

### 11.2 Retry and stale-attempt fencing

The supervisor consumes RFC 0038's typed condition class only after the
operation-local recovery disposition authorizes another attempt. Authorized
attempts whose condition is `Transient` use capped exponential backoff with
full jitter. Other conditions remain blocked until an explicit
operator/configuration/authority change. `Unknown` is not silently retryable,
and RFC 0038 exposes no generic retry decision.

`RecoveryDisposition::NeedsCompensation` bypasses managed retry: no wake can mint an
`ExclusiveRecoveryPermit`. Only completed offline recovery after all serving
processes stop supplies the authority change that permits a new open attempt.
`RecoveryDisposition::RollForwardRequired` is the only disposition that can
enqueue another managed recovery quantum. `RecoveryDisposition::Blocked`
retains its engine blocker and does not acquire retry semantics from this RFC.

Coalesced signals preserve the backoff ladder. Success resets it. Scheduling
uses a monotonic deadline; public wall time and `Retry-After` are projections,
not retry authority, and exist only when retry is scheduled.

Only one attempt runs per graph. Attempt identity is a tuple of process boot
nonce, entry identity, config generation, and monotonic attempt sequence. A
late result that fails the activation checks is discarded and recorded; it
cannot mark a newer request ready.

### 11.3 Global bounds and fairness

Candidate builds and recovery execution have separate process-wide concurrency
budgets and separate fair ready queues over graph IDs. In either queue, repeated
wakes occupy one position and launch order rotates after each task. A slow
candidate cannot consume recovery capacity, and repeated recovery for one graph
cannot starve another graph or consume all build permits.

Candidate build is non-mutating and cancellation-safe, so its deadline is a
hard execution bound: the scoped build task and its children are canceled and
the build permit is released. Recovery remains owned until activation/drop and
is never hidden inside that permit or abandoned by this deadline.

Both queue lengths are bounded by configured graph count; each graph has at most
one candidate and recovery task. Parked drains hold neither permit; independent
limits stay observable and no detached task collection grows.

### 11.4 Startup behavior

Cluster-global validation completes before listener bind. Default startup then
creates every configured entry, starts supervisors, and binds without waiting
for every graph to open. A graph route returns bounded 503 status until a
generation is ready; healthy graphs become available independently.

`--require-all-graphs` remains an operator-selected barrier. It has an explicit
deadline and fails startup if any graph is blocked or unready at that boundary.
It never waits forever. `/healthz` reports process liveness; graph readiness is
reported by graph status.

## 12. Status and rolling-safe wire contract

### 12.1 Listing semantics

The existing `GET /graphs` default continues to list only read-ready served
graphs, sorted by `graph_id`. This preserves the meaning consumed by older
clients.

`GET /graphs?include=all` lists every configured entry. `GraphInfo` keeps required
`graph_id`/`uri` and adds optional `availability { state, read_ready, write_ready,
role, failure_class, retry_at }`.

`availability` is `Option` with a Serde default. On an old server, absence means
legacy ready-list behavior: read ready, write capability unknown; old clients
ignore the addition. `state`, `role`, and `failure_class` are open strings, and
shared clients preserve/map unknown values rather than fail deserialization.

One graph's object comes from one `RuntimeState`; a list may span transitions
across graphs. Activation IDs, backend detail, credentials, and retry counters
are not public contract.

### 12.2 Route failure

False route readiness returns 503, not 404. A selected closed/draining lane uses
RFC 0035's no-`ErrorCode` lifecycle `kind=generation_draining`,
`outcome=not_started`; other bodies carry graph, state, broad failure class,
and optional retry. Pre-Cedar
responses contain no backend detail.

Unknown graph IDs remain 404. `Retry-After` is emitted only for a scheduled
retry and is derived from the same runtime state as the body.

OpenAPI marks the availability object optional, documents open-string values,
and lists 503 on every per-graph route that can fail at routing admission.

## 13. Cost and observability

Healthy routing does one registry lookup, one `Arc` clone, and RFC 0035 admission;
it never opens a graph, scans a manifest, or compiles policy/query content.
Freshness uses the cheap Lance/manifest probe and never reconstructs history per
request; same-schema data commits do not rebuild a generation.

Bounded-label metrics cover state/readiness/role, both queues/build time,
activation/stale/failure, retry, active/retiring generations, transition-pending
counts/wakes, schema-stale wakes, and cold start. Graph IDs are labels only
within configured cardinality. Backend
detail stays in logs; inherited Lance object-store metrics are not duplicated.

## 14. Implementation sequence

1. Build complete generations, exact factory modes, one guard path, and fresh caches.
2. Add stable entries, RFC 0035 cells/drains, latched activation, and retirement.
3. Add bounded fair supervision and retry, then status, metrics, docs, and OpenAPI.

No phase exposes a partial generation or a wake without a live supervisor.

## 15. Required acceptance evidence

Existing test owners are extended, not duplicated:

- `registry.rs`: G1-or-G2 atomicity, exact token/lane/runtime capture, coherent
  readiness, stale loss, and proof that G1 capture fails after the G2 store.
- transition tests: closed G1 never reopens; only an unchanged pre-effect proof
  yields G1/E2; pending drain releases scheduler permits, does no work, and only
  an opaque wake plus rechecked proof resumes it.
- factory/recovery tests: both exact effect-free modes, clean/recovered writable
  guards, all four exact `RecoveryDisposition` variants,
  RollForwardOnly/no-Exclusive, offline compensation,
  guard lifetime through activation/drop, and shutdown at wrapper handoff.
- `stored_queries.rs`, `schema_routes.rs`, `auth_policy.rs`: exact component
  binding, pre-execution schema-stale detection, and all-or-nothing failure.
- engine cache failpoints/server tests: a paused G1 fill cannot reach G2; local
  same-version ABA and RustFS e-tag paths remain fenced.
- read-only tests: no writer facade, recovery call, or write-ready status; safe
  pinned reads survive candidate failure.
- `multi_graph.rs`/system tests: zero-ready bind, graph isolation, capacity
  release including parked drains, fair separate bounds, timeout independence,
  backoff reset/coalescing, and bounded strict startup.
- RFC 0035 tests: proof/pending/final-wake and acquire/replace/store races;
  both-lane/shared-deadline recovery; weak-index bounds; and every
  shutdown/deregister interleaving.
- `boot_settings.rs`/`openapi.rs`: ready-only default, `include=all`, legacy and
  unknown-value decoding, safe 404/503, conditional retry, no backend detail.

Use the [test ownership map](../dev/testing.md), deterministic barriers, shared
cost harness, and RustFS transient/e-tag cells. RFC 0034 owns recovery evidence;
RFC 0035 owns lifetime/shutdown evidence. Every phase runs the canonical
feature-superset suite, denied-warning clippy, format, OpenAPI, and link checks.

## 16. Drawbacks and rejected alternatives

Fresh activation pays cold-cache latency because sharing would enter the
correctness protocol. Rejected: in-place or engine-only refresh; dropping a safe
old view; shared-cache invalidation; recovery inside a cancelable build; listing
unready graphs by default; and restart-only recovery except as emergency fallback.

## 17. Compatibility, reversibility, and open questions

This changes no durable format. Process-local supervision is reversible; additive
status and opt-in `include=all` preserve old list behavior.

Per the [invariants](../dev/invariants.md), [immutable versions](https://lance.org/guide/read_and_write/)
pin old reads; sharing stays graph-neutral; [branch versions](https://lance.org/guide/tags_and_branches/)
are not identity; [object-store metrics](https://lance.org/guide/observability/) remain inherited.

Before acceptance, settle concrete build/recovery/residency limits, the cheap
schema token, and boolean versus view-enum spelling for `include=all`.
