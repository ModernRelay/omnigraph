---
rfc: "2026-09-29-server-runtime-and-online-deployment"
title: "Server runtime and online deployment"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-09-29
updated: 2026-09-29
discussion: https://github.com/ModernRelay/omnigraph/pull/799
supersedes:
  - "0034"
  - "0035"
  - "0036"
  - "2026-09-10-server-lifecycle-and-online-deployment"
superseded_by: []
blocked_on:
  - "B: qualified admission bounds, completion reserve and retained-I/O settlement proof"
  - "E1: versioned validated-revision, result and active-witness encoding in the existing configuration ledger"
  - "E1: authorization, publication/finalization ordering, restart reconciliation and downgrade refusal"
  - "E1: effect-free construction/validation, qualified settled-engine reuse and bounded preparation"
  - "E2: independent historical capture, schema/query reconstruction and retention/cleanup ordering"
  - "F: durable data-submission identity, exact outcome binding and finite retention protocol"
---

# RFC: Server runtime and online deployment

## Summary

The server owns admitted writes through completion, activates schema and stored
queries without restarting, and reports what is known about execution,
publication and availability. The engine owns graph publication and schema
completion. The existing configuration ledger owns deployment input and applied
results. No new transaction manager, content-recovery log or job queue is added.

Delivery is incremental. The first online deployment milestone drains affected
graph requests and keeps the process and listener alive. A separate milestone
adds independently available historical reads during schema transitions. Neither
uses a graph reset or an implicit restart as its migration strategy. Unaffected
graphs continue serving.

This replaces four unimplemented server/recovery drafts with one proposal based
on final detached table pins. Their useful guarantees are consolidated here;
their sidecar, compensation and table-promotion designs are retired. Superseding
the drafts does not accept this proposal or imply it has been implemented.

## Motivation and baseline

The baseline is main `41a77f22` (29 September 2026):

- Table writes prepare detached versions and publish all participating pins in
  one graph-manifest commit. Published pins are final; there is no subsequent
  table promotion or ordinary content-write recovery sidecar.
- Schema publication can still precede installation of its staged contract.
  Initialization, native branch controls and cluster configuration operations
  retain their own completion rules. Writable open and refresh can have effects.
- HTTP handlers still await writes and hold their admission guards. The serving
  registry, stored queries and boot witness are constructed at startup.
  Sessions and the single production query engine do not implement activation.
- Boot readiness, bounded shutdown, input caps, actor admission, keyed-write
  limits and bounded OIDC trust-file refresh already exist. They are foundations,
  not proof of aggregate accounting or ownership through outstanding I/O.
- GQT supports engine-DST replay, per-step storage measurement and ordered
  concurrent query/mutation sessions on one handle. Server execution remains
  unsupported.

The old torn-table/optimizer recovery mechanism is gone. The remaining risks
include lost delivery of committed work, premature resource release, incomplete
schema installation, mixed schema/query bindings and persistent graph exclusion
after a transient startup failure. A safe storage publication protocol alone
does not establish these server contracts.

Current behavior lives in [writes](../dev/writes.md),
[recovery](../dev/recovery.md), [serving](../dev/control-plane.md) and
[testing](../dev/testing.md). This RFC changes no product behavior until its
individual increments are implemented and qualified.

## Observable behavior

| Situation | Required behavior |
|---|---|
| Incomplete or refused request | No write admission or graph effects. Return a typed refusal when delivery is possible. |
| Client disconnects after write admission | The original operation remains owned and counted; disconnect does not cancel or replay it. |
| Result delivery fails | Preserve exact known publication evidence. Unknown outcome is not proof of failure or permission to retry. |
| Schema/query deployment | Keep PID and listener. Selected graph admissions return 503 while draining/applying; admitted work settles. Activate coherent bindings together. |
| Invalid candidate before effects | Preserve the previous coherent service; resuming a closed admission epoch requires a new epoch and fresh authority validation. |
| Unresolved post-publication transition | Keep affected admissions closed until engine-owned completion and validation establish a safe view. |
| Known but unavailable graph | Keep it in authorized inventory and return 503 with its supported action; reserve 404 for an unknown graph. |
| Shutdown | Close admission and settle all registered participants against one absolute deadline. Cutoff reports unfinished work without claiming success. |

The first deployment class has fixed graph inventory, roots, policy,
credentials, provider, trust-binding and external-Blob-policy settings. Only
schema and stored-query content changes. Existing migration rules, including
non-main branch restrictions and explicit authorization of destructive changes,
still apply. Binary replacement, storage upgrades and physical relocation are
separate compatibility operations.

## Authority and completion

| Responsibility | Authority |
|---|---|
| Graph contents and lineage | Engine publication through `__manifest` |
| Accepted schema and unfinished installation | Accepted/staged contract bound to its exact publishing commit |
| Native controls, initialization and reclamation | Their existing engine protocols and graph-aware collector |
| Deployment input, base and applied result | Existing cluster configuration ledger and its serialization/CAS |
| Current serving activation and request lifetime | Server runtime and non-reusable admission epoch |
| Caller-visible outcome | Exact engine result plus independently recorded execution/delivery evidence |

Follow [Detached table commits](0067-detached-table-commits.md) and
[Detached-only tables](2026-09-21-detached-only-tables.md). Before publication,
detached content is unreachable. After publication, table effects are complete.
The server never promotes pins, repairs linear HEADs, compensates individual
tables or reconstructs a generic recovery-v9 classifier.

Schema installation remains engine work. Exact publication evidence determines
whether a staged contract is installed or discarded. Same-process supported
failures must permit later writes once faults stop; an error return and a
dropped future are different cases and need separate qualification. Keep
first-touch datasets, initialization claims and native branch controls under
their own ownership proofs. Cluster recovery records are not graph recovery
sidecars and are not removed by this design.

Current writable open and storage upgrade refuse legacy graph `__recovery/`
artifacts, preserve them and name compatible originating-build recovery.
Read-only behavior follows the current engine contract. Automatic retries never
reinterpret legacy evidence or gain destructive authority from elapsed time.

There is one designated mutation process for a served graph. An independent
optimizer, schema applier or writable opener is another writer even in the same
container. Process-local gates are not a distributed fence: a concurrent opener
can discard live schema staging, leaving an unsafe publication/install window.
This proposal keeps schema apply in the serving mutation process. Moving it
elsewhere requires separately qualified durable fencing.

## Operation ownership

Admission captures one runtime epoch and the operation's trusted actor, Session
settings, immutable inputs, target/preconditions and resource reservations. An
admitted write is registered with its owner before any await or engine effect;
there is no interval in which dropping the request can lose an admitted task.
No owned closure is invoked twice. Reads remain request-owned and cancel
cooperatively.

An ownership record covers child producers and accepted storage I/O, not just
the Rust task. Task return, panic, receiver loss or join is not proof that a
submitted request cannot finish later. Inputs and permits remain charged until
settlement, or an atomic transfer to another registered owner with the same
identity. Delivery buffers have separate finite accounting. A lost receiver
cannot retain unbounded completed results.

Reads hold their observer/resource permits through response-body and producer
settlement. Dropping a body requests cancellation; it does not itself release
capacity still used by a producer. Mutation, load, merge, branch controls,
schema application and maintenance all participate in ownership and draining.
Panic or channel loss reports unknown unless independent evidence proves the
outcome. Cutoff suppresses late successful responses; a stream whose headers
were sent ends through body error or closure, never a fabricated success.

Read/write admission lanes close atomically and never reopen. A request that
loses the close/acquire race refuses; it cannot silently resolve onto the newest
runtime. A replacement, including safe resumption after a pre-effect failure,
gets a fresh non-reusable epoch. Drain proves settlement for the selected epoch,
not merely zero HTTP connections. A timeout leaves it closed and grants no
authority to apply, replace, clean up or replay. A parked drain may release
unused scheduling capacity but retains ownership and revalidates after waking.

Shutdown, activation and registration share an ordering boundary. Once stopping
wins, no candidate activates and no new operation is admitted. The existing
process shutdown deadline covers active and retiring runtimes, candidate work,
streams and owned operations; none gets a fresh grace period. The final runtime
store is synchronous and infallible after its final checks; it includes
predecessor closure and registration of the successor, with no intervening I/O.
Participants deregister only after settlement. Cleanup/destructors must not turn
a proved drain into an unbounded shutdown. At cutoff the existing watchdog
terminates the process and preserves unresolved outcomes for supported startup
reconciliation.

## Serving views

A serving view binds graph/root incarnation, schema/catalog identity, stored
queries, policy, authentication requirements, provider/credential bindings,
external Blob policy and activation witness. Capture it before dependent routing
or authorization and retain it through the request. Accepted graph snapshots
remain per-attempt engine authority; a Session is not a pinned serving view.

Validate immutable resource content and effective bindings, not filenames.
Existing OIDC key/principal refresh stays within its fixed canonical binding;
requests capture consistent trust evidence and continue to enforce engine policy.
Historical selection never revives old authorization.

Candidate construction and validation are effect-free. Apply and schema/control
completion are separately owned effectful operations requiring qualified drain
and engine authority. Ordinary writable open/refresh cannot serve as effect-free
probes. Reuse of a settled engine is allowed for the first
schema/query class when all affected mutable users have settled, all other
effective bindings are unchanged, and schema/cache/identity validation passes.
Other change classes require their own qualified construction/reuse rule.

Activation rechecks the attempt identity, predecessor epoch, resource content,
ledger authority, engine completion, budget ownership, stopping state and
absolute deadline under the final synchronous ordering boundary. A candidate
prepared for an old epoch cannot win an ABA race against a later one. Check the
deadline even if its timer has not fired. Expiry prevents activation and requests
cancellation; unsettled work retains capacity until disposal actually settles.
Completed candidates remain charged until serving ownership adopts them.

## Online deployment

Deployment is declared through the existing configuration ledger. There is no
new deployment HTTP route, direct mutable-file reload or parallel job database.
The following **validated-but-effects-pending entry is new**: today `cluster
apply` performs effects itself and records the applied result.

1. Configuration validation/planning records immutable content-addressed input,
   intended per-resource digests, the exact base revision, affected graph set,
   initiating principal and approval evidence. The versioned encoding and
   authorization binding are acceptance gates, not existing capabilities.
2. The designated mutation-capable serving process observes the ledger at boot
   and a configured bounded interval. Observation-only refresh/import records
   are not deployments. Repeated observation resolves the original revision
   before deciding whether work remains; the server's own result is not new
   input. Verify content/provenance and authorize the complete effect set under
   current applied policy before effectful opens or writes.
3. Act in ledger order. An unstarted validated revision superseded by a later
   one is recorded as skipped, never executed. A base that is neither active,
   skipped nor refused is broken lineage and is refused before effects;
   successors are revalidated against the actual achieved state. Equal intended
   and active per-resource digests establish convergence, not `config_digest`
   alone. Once effects start, finish/classify that revision rather than silently
   replacing its inputs with a successor's.
4. Reserve bounded preparation/completion capacity, prevalidate the candidate,
   close affected admission and drain it. Revalidate exact authority after drain,
   then invoke the existing apply implementation within the serving mutation
   owner. Finish any published schema installation and validate schema/query
   bindings. No fallible preliminary check is deferred past avoidable effects.
   Existing cluster apply can sweep recovery records outside the desired diff.
   Healthy E1 therefore refuses pending cluster recovery, staged schema work and
   legacy graph artifacts before apply unless every completion participant is
   included in the verified, authorized and drained scope.
5. Record apply's own per-resource outcomes and digests in the existing ledger
   under its lock/CAS. Set `config_digest` only for full convergence. Multiple
   graphs may converge partially; this is not a cross-graph transaction. Build
   the next runtime and witness from the achieved projection, not the entire
   requested bundle.
6. Activate under the serving-view rules, then expose the observed activation.
   Keep validated input, durable applied result and observed active state distinct.
   An applied revision is not active merely because its write succeeded.

Original-result lookup must identify a validated revision after later revisions
complete, subject to current authorization and explicit retention. It never
replays the old apply or reactivates old configuration. Bind result evidence to
the original input, principal and graph incarnations; never sample a later HEAD
or ledger as the original result. Missing/expired evidence is not proof of
non-execution. A restart reports its own activation observation, not invented
activation history for the previous process.
Unresolved deployment or schema/control completion evidence remains authoritative
until terminal proof; lookup expiry cannot erase it, authorize disposal or permit
replay.

The durable encoding must connect deployment identity to exact graph effects,
finalization and retained results using existing authority. Before E1 acceptance,
specify publication ordering, repeated-observation behavior, crash reconciliation
and downgrade refusal. Startup resolves ambiguous prior transitions before an
effectful graph open or live readiness. A polling loop or in-memory drain flag
does not satisfy this gate; no blind re-application is allowed after a crash.

## Exact outcomes

Separate execution settlement from effect knowledge:

| Evidence | Permitted action |
|---|---|
| Not admitted, or settled with proved no effects | A fresh attempt only under the typed whole-command retry/precondition contract |
| Running, or external I/O settlement unproved | Observe the original owner; preserve identity and reservations; do not duplicate |
| Settled with an exact committed/no-op/compound result | Return that original result and any outstanding completion obligation |
| Settled but effects unknown | Reconcile exact evidence; settlement alone does not authorize replay |

Increment A returns a merge's own `CommitOutput` on both the branch merge route
and GQ merge through `/mutate`, and in CLI JSON: `graph_commit_id`,
`graph_manifest_version`, optional `graph_branch`, `parent_commit_id`,
`merged_parent_commit_id`, `actor_id`, and `created_at` in Unix microseconds.
Optional fields come from that publication. Fast-forward publishes a target
commit whose merged parent is the source head. `already_up_to_date` has
`commit: null`. Later history lookup cannot construct a receipt.

Missing receipt fields from an older server remain missing evidence. Even a
present `commit` is trusted as an own-publication receipt only from the release
that ships A, established through the existing server version capability check;
older servers can return a later HEAD. The implementation must name that release
and qualify mixed-version behavior before advertising this guarantee.

Optional source deletion is a separate effect. Its failure keeps the exact merge
receipt, `branch_deleted: false`, legacy `branch_delete_error` and structured
`branch_delete_error_details` (`ErrorOutput`). The successful compound merge
retains exit 0; retrying deletion must not replay the merge.

For issue [466](https://github.com/ModernRelay/omnigraph/issues/466), preserve
structured errors across ordinary/streamed responses and classify the entire
data-write command. Failures other than typed precondition failures carry
`command_outcome { execution, effects, action }`; `action` is `retry`, `refresh`,
`recover` or `reconcile` according to its supported continuation.

| CLI result | Contract |
|---|---|
| Exit 75 | Evidence permits a bounded caller retry of the whole command with unchanged preconditions and no earlier work able to produce its logical effects. Initially: verified single-request 429 `too_many_requests`, or effect-free preparation conflict on direct standalone append/merge load without `--from`. Forward `Retry-After` in structured output; the caller owns the attempt bound. |
| Exit 4 | Typed conditional mismatch; the write had no effect. Re-read and decide on a new precondition. |
| Exit 1 | Other failure, including malformed/truncated success after dispatch and unknown outcome. Generic 409/503, resource exhaustion, read-set conflict or completion-required error is not retry authorization. |
| Exit 0 | Exact successful/no-op result, including successful merge with separately reported optional deletion failure. |

A adds classification, not automatic retry. Compound loads/branch commands do not
inherit retry safety from their final subrequest. Read/validation and managed
lifecycle command families retain their existing exit contracts.

F separately qualifies durable data submission and lookup. The caller knows its
key before sending; scope binds stable principal, operation kind and original
graph/branch incarnations before resolving current names. Fingerprint semantic
input and preconditions; concurrent duplicates attach to one execution and
changed input conflicts. Protect lookup with current authorization and define
committed, no-op, refused and compound outcomes across crashes and finite
retention. Expired/missing records do not prove no effects. Process registration
is not durable acceptance; a result database cannot become graph-commit truth.

## Resource bounds

Extend existing body, actor and keyed-write limits. Bound aggregate ingress
before body collection/parsing, admitted/executing/queued work, actor-record
cardinality, retained inputs, decoded Arrow/vector/Blob data, staging,
validation, output, candidates and retired views. Each cap names its resource,
scope and lifetime; request byte estimates are not engine memory budgets. Count
shared allocations once and release them only after their last producer/user.

Choose immediate refusal or a finite pre-admission queue with bounded inputs and
wait. Absence of a configured queue means immediate refusal, not an unbounded
default. A queued request has no write effects; handoff into execution is atomic
with ownership registration. Fixed resource failures report the limiting resource.

Within the process budget, reserve bounded execution, memory and local I/O
capacity for completion of admitted work, authorized schema/control completion,
shutdown and status. Ordinary admission cannot consume it. These paths must not
wait for the data-lane permit of work they must settle. Separate the small status
allowance from larger completion work. Reserves do not guarantee storage progress,
grant recovery authority or extend deadlines. Minimum bounds and this reserve
ship with B, before detached execution can accumulate work.

Reserve candidate/activation headroom before closing healthy lanes. Insufficient
capacity refuses or defers within a bounded queue. Candidate expiry retains
accounting until settled disposal; repeated failures cannot leak generations.
Keep input deadlines, caller wait limits, read execution budgets, deployment
deadlines and the shared shutdown deadline distinct. Timeout never fabricates an
abort or permits non-idempotent replay.

Catalog publication currently rewrites all history rows. Bytes decoded/written
per publication grow with history; retained catalog bytes grow quadratically
without manifest-version retention. Flat storage-request counts do not establish
bounded cost. Qualify this gap in D's workload envelope and refuse unsupported
work explicitly; do not implicitly discard history or bypass publication to
meet a performance target.

Change-feed pages must make progress through every accepted retained commit.
Use the existing engine oversized-change behavior as a baseline; qualify encoded
payload limits, partial-commit cursor semantics, resumed pages and abandoned
bodies. A page budget cannot wedge a cursor or silently skip a change. Any
additional wire oversize outcome must be explicit and compatible with the
[retained-history contract](0030-cdc-time-travel.md).

## Historical reads

E1 may drain/refuse historical requests with other affected graph reads. It
preserves current supported history semantics and does not advertise independent
historical availability. This changes the delivery dependency of the September
10 proposal; it does not delete the stronger target below.

E2 admits both existing and newly acquired retained-version reads while live
schema apply is held. Capture exact data, schema lifetime, stored-query revision
and graph/resource incarnation independently of live mutable state. Reconstruct
after cache eviction and process restart; adapting current schema column names
does not establish historical interpretation. Never substitute current data,
schema or query source for unavailable history.

Current authorization protects historical requests and result lookup. Capture
retention protection before cleanup can authorize reclamation; an admitted read
keeps it until its body/producers settle even if the new-acquisition window
expires. Missing, expired or unsupported history is an explicit refusal. Bound
historical residency separately, register it for shutdown and preserve reader
protection under budget pressure. An open handle or object age is not a retention
proof. Cleanup and historical acquisition must share a qualified ordering.

Current capture uses the live schema gate and current accepted catalog. The
[shared-schema gate proposal](2026-09-18-shared-schema-gate.md) can improve
contention but is neither distributed fencing nor proof of E2. Its implementation
is not a prerequisite for E1's full affected-graph drain.

## Availability and supervision

Keep liveness distinct from readiness. Preserve `booted_serving_digest` and the
existing boot revision/CAS as boot facts. Add a versioned active witness with
achieved revision/digests; full deployment identity and per-resource results stay
behind authenticated current-state/result lookup. Compare active evidence to
intended per-resource digests, not a boot digest or successful apply alone.

Preserve registry membership and availability as distinct facts:
`served_graph_count` counts registry entries, including temporarily blocked
entries; quarantine has its own count. The legacy graph list selects live-read-ready
entries. A valid empty inventory is ready; shutdown is unready. Add an authorized
all-entry inventory, loading/deploying states and separate live-read, write and
historical availability where qualified. Known unavailable graphs remain
observable even when no runtime could be built. E2 preserves the target that
historical-only service can be ready, but must specify versioned aggregate
readiness and separate availability counts before changing this wire contract.

Status reads bounded snapshots outside blocked data lanes; it does not open a
graph, start completion or reconstruct history. Protect graph identities and
diagnostics with current authorization; scrub credentials, provider secrets,
query inputs and sensitive object URIs. Use additive optional fields and open
state strings: an older server's absent capability is unknown, never success.
Name the exact wire encoding and negotiation before E1 ships.

Broader supervision uses bounded fair scheduling, one active attempt per graph,
coalesced wakes and capped backoff. A new wake does not reset its retry budget.
Attempt identity fences stale callbacks. Retry only qualified transient failures;
unknown or persistent unsupported conditions remain explicit refusals. Report
phase, attempts, last classified failure, limiting resource and required action.
Emit `Retry-After` only when a finite retry is actually scheduled. Recovery status
cannot claim progress merely because a timer ran.

Automatic embedding remains a separate decision in
[Ingest-time embedding reconciliation](0015-ingest-embeddings.md). Declared
`@embed` metadata/provider configuration must not imply population occurred:
provide explicit load/capability diagnostics. Broader policy/provider/root/trust
binding changes require their own coherent authorization and secret-resolution
qualification; reuse existing bounded trust refresh within unchanged bindings.
A qualified read-only serving role exposes only read capabilities, uses
effect-free construction, cannot invoke mutation or schema/control completion,
and never reports write readiness. This broader role does not block E1.

## Invariants

The [architectural invariants](../dev/invariants.md) remain binding: one graph
publication, one accepted attempt, stable identities, derived indexes/caches,
server-resolved actors and engine policy enforcement, bounded observable failure,
and evidence at the boundary that owns the claim. This proposal introduces no
raw public Lance writer, custom WAL, manifest-derived queue, shadow commit truth,
cloud-only correctness path or process-local distributed-fencing claim.

## Compatibility

Online schema deployment is distinct from storage-format or binary replacement.
Current normal open serves v11. Supported standalone upgrades preserve branches,
IDs and retained history under an offline fleet, verified whole-root backup and
post-conversion checks; rollback restores that backup. Table promotion may occur
once inside the older-format upgrade, never in current serving completion.

Cluster-managed format conversion is currently refused until qualified. Before
deploying a newer format over an older cluster, implement and qualify its
history-preserving offline upgrade under
[Explicit storage upgrades](0064-explicit-storage-upgrades.md). Reset/export-only
replacement is not this RFC's deployment strategy. Do not advertise rolling
compatibility or let an old executable reopen upgraded state. Follow the
[upgrade support boundary](../user/operations/upgrade.md).

Wire additions must preserve older clients' documented fields and exit behavior.
Persisted deployment/result changes need explicit version admission and downgrade
refusal. In-memory ownership/activation can be reverted independently only while
no new durable format or stronger public contract is being consumed. No fallback
may silently restart or reset to make an unsupported online change succeed.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Keep restart as schema deployment | Reconstructs a coherent runtime but interrupts unrelated work and does not solve request ownership. |
| Refresh the handle or swap a query registry alone | Cannot establish coherent schema/query/policy/witness bindings or drain outstanding users. |
| Rebuild a generic sidecar recovery service | Reintroduces a removed mechanism; current content pins need no completion. |
| Separate apply process while serving | Schema staging decisions lack a qualified cross-process writer fence. |
| Require independent historical reads before any online change | Couples the useful first milestone to additional reconstruction/retention work; E1 and E2 have explicit different availability contracts. |
| Add a deployment endpoint and job store | Duplicates configuration authority and expands the trusted administrative surface. |

## Qualification

Extend the [existing owners](../dev/testing.md) and the separate
[self-contained server-testing proposal](2026-09-26-self-contained-server-testing.md).
This RFC owns product requirements; that proposal owns reusable runner and
process-containment mechanics. GQT/DST/seams/benchmark format decisions retain
their own RFCs. Unsupported targets, zero selected cases, unreached faults and
missing required services are not passing evidence.

| ID | Required observation and boundary |
|---|---|
| T1 | Every merge mode and both HTTP entry points return their own publication, or explicit no-op. Advance target after merge publication but before response construction. Engine receipt oracle plus real route evidence. |
| T2 | Commit successfully, then suppress/truncate the HTTP response or expire caller wait. One CLI submission; durable state proves the effect; caller reports unknown. Optional deletion failure preserves merge result. Real server/CLI. |
| T3 | Whole-command exits/actions preserve preconditions, old-server missing evidence and compound outcomes. Generic errors never authorize replay; `Retry-After` survives. CLI and wire owners. |
| T4 | Multi-table mutation/load/optimize and all merge modes fail before publication, after detached effects and across lost acknowledgment; schema installation fails separately. Exact final pins/history, no partial publication, same-handle progress, protected cleanup and named branches. Legacy artifacts refuse unchanged. Engine GQT/failpoint/DST owners. |
| T5 | Incomplete bodies have no effects. Disconnect admitted writes at reached boundaries; work remains owned/counted and a same-PID sentinel succeeds after settlement. Real sockets plus modeled server scheduling. |
| T6 | Retain an accepted backing-store write after caller cancellation, returned error and contained panic. No premature release, drain, activation or teardown; release I/O and verify effect/settlement. A hold before inner-store acceptance is insufficient. |
| T7 | Race shutdown with writes, stalled producers and activation under one deadline. Separately kill the actual server at durable boundaries and restart its root twice. Exact publication/schema completion and no duplicate apply or false readiness. |
| T8 | E1: warm schema/query deployment keeps PID/listener, coherent bindings and unaffected graph progress; invalid candidates and crashes are safe. E2 separately: old and new historical reads finish while apply is held, with reconstruction, current auth and cleanup protection after eviction/restart. |
| T9 | Reobserve a validated revision, including after restart; supersede it before effects, refuse broken lineage and resolve D1 after D2. One original execution/result; exact partial convergence; applied and observed activation remain distinct. Cluster/server owners. |
| T10 | Saturate each declared domain; refused requests have no effects. Charge disconnected work, buffers, queues, producers and candidate overlap until settlement. Real completion/status uses reserved capacity; allocation evidence reconciles with counters. |
| T11 | Page wide keys/rows/Blob values through a sentinel, including encoding expansion and abandoned bodies. Complete history and cursor progress; bounded authorized status remains usable during loading/drain/failure. Engine plus server owners. |

The September 28 audit ran 41 Rust test functions (including 84 detached-matrix
cells, schema application, wide-row feeds, lost acknowledgment and three two-table
optimize regressions) against Rust sources at `77507142`. Two issue-694 GQT cases
ran seeds 0 and 42 with matching replays: eight worker executions. All passed on
local filesystem/in-memory fixtures. These are engine evidence, not qualification
of HTTP disconnects, full-server restart, cloud backends or server performance.

`--- concurrent` now orders engine-DST query/mutation sessions on one handle;
it does not supply merge/schema controls, real sockets, server cancellation or
retained accepted I/O. Reuse its scheduling/measurement machinery where it fits,
without claiming those missing boundaries. Handle reopen is not a server restart.

Benchmarks report workload/build/backend identity, offered/admitted/completed/
refused/unknown work, latency distributions, memory, storage requests/bytes and
variance. Measurements do not set CI timing gates. Preserve these experiment IDs:

| ID | Workload / required distinction |
|---|---|
| B1 | Publication cost versus touched tables/rows and accumulated history; vary history depth, table count and concurrency. Include catalog bytes and RSS as well as requests. |
| B2 | Light read/write traffic with heavy merge/load across actors, graphs and branches; offered load, admission, fairness and recovery after a burst. |
| B3 | E1 admission-refusal window and candidate residency; E2 historical progress and retained-generation overlap separately. |
| B4 | Owned optimize/rebuild/cleanup under serving load; logical correctness, resource interference and protected retention. |
| B5 | Schema/control settlement, resumed admission and ownership/reclamation after failures; distinguish caller disconnect from process death. No table-promotion latency. |
| B6 | Wide feed/export/Blob delivery with slow and abandoned consumers; bytes, memory, cursor progress and producer release. |

`--measure` supports deterministic engine storage-cost assertions. Real server
latency, saturation and RSS require a server workload adapter in the existing
benchmark harness. Do not treat simulation wall time as a performance result.

The upstream review used complete relevant Lance documentation and pinned
crates.io Lance 11.0.0 implementations: detached commit/checkout, strict overwrite
with zero retries, branches/tags, schema evolution, cleanup and object-store
semantics. [Transactions](https://lance.org/format/table/transaction/),
[versioning](https://lance.org/format/table/versioning/) and
[branches](https://lance.org/guide/tags_and_branches/) inform the substrate
boundary; graph-wide retention, publication and runtime activation remain
OmniGraph obligations. Newer upstream documentation is not evidence of pinned
dependency behavior. No substrate extension is proposed here.

## Rollout

| Increment | Deliverable | Shipping gate |
|---|---|---|
| A | Own-publication merge receipts and conservative CLI outcomes | T1–T3, exact compatible wire/exit contract |
| B | Owned writes, read/stream accounting, close/drain and shared shutdown | T5–T7 and minimum T10 reserve/lifetime bounds |
| C | Remaining schema/control completion, legacy refusal and owned maintenance | T4/T6/T7, same-process progress and protected reclamation |
| D | Aggregate budgets, feed progress and embedding diagnostics | T10–T11; history-dependent workload qualification |
| E1 | Same-process schema/query activation from validated ledger revisions | B, relevant C/D bounds, T8.live/T9, crash/startup and wire gates |
| E2 | Independent historical availability during live transitions | T8.history/retention and historical T10/T11; no dependence on a surviving cache |
| F | Durable data-operation submission identity and original-result lookup | Accepted encoding, duplicate/crash/retention and authorization proof |
| G | Broader supervision and runtime binding changes | Per-change-class authority, coherent auth, bounded fair retry and secret handling |

A and focused C/D repairs can proceed alongside B. E1 does not depend on E2 or
F; it may pause historical requests and must say so. E2 preserves the stronger
target as a separate increment. Existing bug fixes need not wait for the whole
proposal. Offline cluster-format upgrade is a deployment prerequisite where
needed, not a prerequisite for implementing the server locally. Update actual
implementation state and user/developer documentation as each qualified slice
lands; merging this draft supplies no product qualification.

## Unresolved questions

Before accepting the affected increments, settle:

1. Cluster/server owners: versioned validated-revision, finalization/result and
   active-witness fields, authority binding, observation interval and retention.
2. Engine/server owners: exact effect-free capture/completion/reuse interface and
   the settlement proof covering late accepted I/O.
3. Server owners: reservation rules and admission domains for B; measured values
   remain an evidence gate rather than invented SLOs.
4. Engine/history owners: E2's schema/query reconstruction and retention protocol;
   F's durable identity encoding is a separate acceptance decision.

## Decision log

- 2026-09-29: Consolidated the four server/recovery drafts after detached table
  publication and final pins removed their content-recovery machinery. Preserved
  ownership, late-I/O, exact outcomes, coherent activation, authorization and
  retention obligations. Split the former E gate into E1 drained live activation
  and E2 independent historical availability; E1 no longer waits for E2. Kept
  ledger-declared deployment, same-process apply under the existing writer
  boundary, and no-reset migration. Updated the baseline for GQT concurrency,
  existing limits/trust refresh and history-dependent catalog costs. Separate
  storage and testing proposals retain their own decisions.
