---
rfc: "2026-10-01-engine-settlement-and-resource-bounds"
title: "Engine settlement and resource bounds"
track: maintainer
status: accepted
implementation: partial
authors:
  - OmniGraph maintainers
created: 2026-10-01
updated: 2026-10-03
discussion: null
supersedes: []
superseded_by: []
blocked_on:
  - "Native ownership hooks covering local persistence, multipart cleanup and child workers in an unmodified released Lance"
  - "Terminal outcome or effective fencing evidence for remotely accepted ambiguous writes"
  - "Measured catalog, native execution and completion envelopes enforced before effects"
  - "T6 settlement and T10 reserve/lifetime qualification on local and supported cloud paths"
---

# RFC: Engine settlement and resource bounds

## Summary

This decision extends B of
[Server runtime and online deployment](2026-09-29-server-runtime-and-online-deployment.md)
with ownership of graph-query producers and blocking workers, and operation-wide
limits on retained keyed batches, keyed parse estimates and removed IDs.
These implemented boundaries close specific lifetime and admission gaps.

Full B still requires ownership of accepted native I/O and protected completion
resources before runtime reuse. The pinned Lance 11.0.0 integration does not
establish that contract. The remaining sections specify its qualification gates;
acceptance does not enable online deployment or weaken the existing
[owned-operation contract](2026-09-30-owned-server-operations.md).

## Implemented boundary

`QueryContext::run_owned` runs the complete query, including successive search
passes, drops its completed execution future, then waits for registered graph
producers and blocking workers before returning success, an error or a panic. Each child
registers before dispatch. Its future, captured resources and abandoned result
remain ahead of its registration in destruction order. Dropping the query closes
new root registrations; existing children retain their ownership and add
descendants only through their own registration (`QueryWorkLease::child`), so a
nested root registration such as `WorkMemory::blocking` is refused after
closure. Worker panics follow the existing worker-error path. Execution panics,
including a completed future's destructor panic, wait for the registered
workers before resuming the original panic payload. The server now keeps read
execution alive after disconnect or MCP response expiry so these joins finish;
embedded callers that drop the query still only close registration. This joins
OmniGraph's workers, not opaque DataFusion tasks or Lance/storage I/O, and grants
no engine-reuse capability. Early termination, including `LIMIT`, and errors can
still wait for a running blocking worker. Ownership does not establish a finite
cancellation or early-completion latency; that cost remains to be measured.

Named mutations and keyed loads share a 32 MiB retained-batch allowance across
their touched tables, in addition to the existing per-table keyed limits.
Keyed parsing and removed-ID collection have their own 32 MiB operation-wide
allowances. The latter covers deletes, cascades and Overwrite replacement;
Overwrite's bulk input keeps its existing separate checks. Refusal precedes this
operation's table effects and publication, but an implicitly created load branch
may already have effects. These fixed allowances
preserve conservative accounting; they do not track every shared allocation or
form a combined memory/RSS cap.
Exact ownership and admission points are documented in
[execution](../dev/execution.md) and [writes](../dev/writes.md).

## Motivation and observable behavior

The server now owns admitted writes after caller disconnect. Its
`OperationRuntime::wait_logical_owners` deliberately proves only that registered
server work finished. A producer, blocking filesystem operation or multipart
cleanup can still be active below that boundary. Zero requests, zero gauges,
an exclusive schema gate and an awaited task are insufficient reuse evidence.

The remaining full-B contract requires:

| Situation | Required behavior before reuse can qualify |
|---|---|
| Capacity unavailable before admission | Immediate typed refusal; no effect and no waiting request queue. |
| Caller disappears | Original work and its reservations remain owned; no replay. |
| Engine call returns while a child or accepted I/O remains | Retain ownership and capacity; drain remains incomplete. |
| Ordinary capacity is exhausted | Already-admitted completion and bounded status retain their reserved capacity. |
| Effect or settlement cannot be established | Keep admission closed and retain uncertainty; the existing bounded process shutdown remains the containment path. |
| Drain succeeds | A root- and epoch-bound capability permits the next validated exclusive transition. It does not itself prove successful publication or authorize retry. |

The complete decision touches engine/core/storage integration and the server.
The supported wire line remains v0.12. The accepted umbrella specifies a narrower E1
transition that retains the same engine and resource owners. It needs its own
transition, resource and deployment-protocol qualification; this decision
neither qualifies it nor requires generic engine disposal/reuse as its mechanism.

## Required settlement mechanism

### One ownership tree

Full B requires the engine to create one operation scope before planning or other work that can
spawn a child. It captures canonical root incarnation, serving epoch, operation
identity, actor, settings and resource reservations. Cached datasets and stores
do not capture the first caller's scope: each operation passes its own context.
Task-local instrumentation and tracing are observations, not ownership transfer.

Register a child **before** its task, blocking job, native request or cleanup
can be accepted. The accepted executor owns that registration and the buffers
it uses. Dropping a waiter cannot release it. A child may register descendants
while holding its registration; that transfer cannot pass through a zero-owner
state. Root admission closure and the final empty-tree check share a synchronous
boundary. Cancellation requests cooperation but never certifies settlement.

Completion work inherits the original operation identity and reserved allowance.
Destructors that can start asynchronous work must transfer ownership before
returning. A panic, lost registration or unsupported native path makes the scope
unprovable and cannot be repaired by decrementing a counter. Registrations are
bounded by admitted scopes and child limits; this is no durable work queue.

Keep two facts separate:

- **Effect outcome:** the existing exact publication/no-op evidence, proved
  absence of effects, or unknown outcome. A schema publication contains both the
  accepted contract and table references; no contract installation remains owed.
- **Settlement:** no remaining worker or accepted request can change this
  operation's storage state or use its retained resources.

A known commit can still have unfinished cleanup. Settled work can still have
an unknown outcome. Neither fact substitutes for the other.

### Native and remote boundaries

The required native integration carries the operation context through local
read/write/persist/remove work, multipart parts/complete/abort, commit handlers,
child query producers and blocking workers. It reports acceptance before
dispatch and terminal settlement after the real owner finishes, including error
and unwind paths. Call-site wrappers around the returned future are insufficient.
It must use public APIs in an unmodified released dependency. A dependency
upgrade follows the normal compatibility qualification; no vendored Lance or
replacement storage protocol is introduced.

A client-side terminal error is not necessarily a terminal storage outcome.
For example, a remote conditional publication may complete after its response
times out. A subsequent read observing the old head cannot prove it will never
land. Such a request keeps the scope unprovable until existing publication
evidence plus a qualified backend terminal guarantee or effective fence resolve
it. A new request timeout, idle interval, process exit or lock expiry supplies no
such proof. Unsupported ambiguous cases retain fail-stop; startup reconciliation
must also respect unresolved remote effects. This proposal does not invent a
remote cancellation API or a distributed writer fence.

### General reuse and schema activation

General engine disposal/reuse requires closing one serving epoch to prevent new
root operations. Every in-process handle
for the same canonical root must participate in that admission boundary;
otherwise reuse refuses. The shared schema gate alone does not establish this.
External writer exclusion remains the separate deployment responsibility.
The engine waits for all admitted reads, writes, native children, retained
producers and affected response-body/output owners. Any missing coverage or
unresolved effect refuses reuse. Deadline expiry leaves the epoch closed and
cannot mint a capability.

Only then may the engine return a non-serializable, single-use reuse capability
bound to the canonical root incarnation, exact engine instance, closed epoch,
settlement generation and captured authority.
The consumer revalidates authority, stopping state and the original deadline
under the transition boundary. It cannot use the capability on another handle
or reopen the old epoch. Resumption uses a fresh epoch. Cancelling a drain waiter
leaves the closed engine, ownership tree and reservations retained by the drain
owner. Proposed private API shape: `seal(reason, absolute_deadline)` returns a
`DrainAttempt`; `wait(&mut self)` returns a non-cloneable `SettledEpoch` only on
success. No public constructor or unchecked boolean can manufacture that proof.

Current schema apply publishes its contract and table references atomically in
`__manifest`, as described in
[Schema contract in the manifest](2026-09-30-schema-contract-in-manifest.md).
There is no durable contract-file installation or schema-sentinel release to
continue. An error after proven publication can still report `RecoveryRequired`
with the committed outcome; coherent in-memory schema/catalog adoption must be
validated without replaying that publication. Engine `refresh` reads the
published contract and updates the in-memory view. It does not complete durable
schema work. Read-write local open still writes a capability probe, so it is not
an effect-free candidate-validation operation.

The umbrella's separately gated E1 transition finishes affected admitted requests and
registered query workers before applying on the same engine. It does not permit
old requests to resume with old query bindings against a new contract. Any
remaining native read tail needs separate proof that it cannot interfere with
the transition and remains owned and charged under finite limits; unclassified
tails refuse activation. That narrower qualification does not mint a
`SettledEpoch`, permit engine disposal or weaken this decision's generic native
settlement and resource guarantees. Uncertain writes retain process containment.

## Required resource contract

Use one admission hierarchy over existing resource owners. DataFusion retains
its memory pool and spill manager; Lance retains its caches, buffers and native
execution. Integrate reservations or conservatively reserve a qualified maximum
before entry. Do not allocate another memory pool merely to mirror usage.

Every resource limit has a unit, owner, lifetime and enforced acquisition point:

| Resource | Admission and lifetime |
|---|---|
| Retained decoded inputs and staged batches, bytes | Aggregate across every table in one operation before effects; charge expansion and shared Arrow allocations once, through the last consumer. Existing per-table limits remain additional guards. |
| Execution memory, bytes; workers, count | Reserve the qualified envelope of concurrent DataFusion/Lance contexts and child workers before dispatch; retain until the actual workers settle. |
| Graph caches and catalog, bytes | Charge shared values through their last borrower, including evicted values and retiring handles. Bound metadata reads, decoded history, publication copies and serialization before constructing them; request count alone proves no memory bound. |
| Local scratch, bytes | One process cap includes DataFusion spill, Lance spill and temporary staged datasets. Each backend keeps its native quota; deleting a waiter does not release capacity while files or writers remain. |
| Accepted I/O, count; retained I/O buffers, bytes | Register and reserve before native dispatch, separately for ordinary and completion work; release only with terminal settlement. |
| Output, bytes | Account for simultaneous Arrow source and encoded destination, then queued buffers through the last producer/body owner, including JSON escaping and Blob expansion. |

The existing 150 MiB ordered/query pool and 100 GiB per-execution scratch quota
are **not process budgets**. Graph sessions currently default to 6 GiB index and
1 GiB metadata caches. Native execution may share cached contexts; it cannot be
charged independently to every caller or assumed isolated. Merge has separate
validation, hydration and staged-file ownership. The implementation must join
these existing owners to the hierarchy instead of adding their nominal limits
to a request-size counter.

The inline schema source and serialized IR are independent byte dimensions in
catalog publication and retained manifest history. Measure both across history
depth, branch and participant counts, including cold reads and publication
copies. The publisher's 8 MiB retained-row cache threshold does not bound those
allocations; the small-file prefetch policy does not bound decoded memory.

First admit effect-free preparation under a finite preparation allowance. It
must bound metadata reads and decoding before materializing the participant and
catalog envelope; discovering the required size cannot itself allocate without
limit. Atomically acquire the resulting ordinary allowance **and** completion
allowance before admitting effects, transferring existing preparation charges
without double-counting. Failed acquisition releases settled preparation only.
Ordinary work cannot borrow completion or status capacity. Completion cannot need a permit
held by the work it must settle. Control-only completion cannot start another
ordinary mutation or unbounded history scan under a privileged label. Persistent
storage failure still yields bounded unresolved work. Scratch reserves protect
quota and dispatch capacity, not physical disk blocks: external disk exhaustion
remains a storage failure unless a separately qualified filesystem reservation
exists. Reserves do not establish storage availability.

For each independently bounded resource, enforce
`ordinary + reserved_completion + status <= configured_total` with checked
arithmetic. Shareable allocations have one owner; transfers never release and
reacquire the same charge. Insufficient candidate or completion headroom refuses
before closing healthy admission or starting effects. Limits on new work do not
discard retained graph history.

Do not advertise a universal RSS ceiling: allocator, runtime, page-cache and
unaccounted native allocations require separate measured headroom. Reusable
mode requires explicit finite deployment totals and a qualified workload profile
covering every invoked path. A path without an enforceable envelope is refused
in that mode. Process totals and minimum completion allowances are a
qualification gate below, not guessed from HTTP body sizes or the implemented
per-operation limits.

## Substrate evidence

Audited baseline: OmniGraph `14453300`, Lance/lance-io 11.0.0 at source commit
`ab6b5bbe46009ed78746b444df8db59a8bc5d842`, object_store 0.13.2,
DataFusion 54.0.0 and Tokio 1.52.3. Full relevant Lance guides and pinned source
were read; the live documentation describes newer APIs too, so it is not proof
that a hook exists in the pinned release.

- Lance's [local path selection](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-io/src/object_store.rs)
  bypasses an inner object-store wrapper for native file I/O. A Unix guard shows
  that a public `file-object-store` adapter can preserve canonical URI, path and
  store identity while exchanging datasets with the stock provider. It still
  starts unwrapped empty-directory cleanup; local copy semantics, local/cloud
  classification, caches and Windows UNC paths need separate qualification.
  No production provider changes. Shared control-object storage must participate
  in any complete integration.
- [ObjectWriter and LocalWriter](https://github.com/lance-format/lance/blob/ab6b5bbe46009ed78746b444df8db59a8bc5d842/rust/lance-io/src/object_writer.rs)
  include unjoined multipart abort on drop and blocking persistence whose job
  can outlive its future. Native object_store local multipart cleanup also
  launches work from drop.
- [Lance access](../../crates/omnigraph-core/src/lance_access.rs),
  [query resources](../../crates/omnigraph/src/engine/operators/memory.rs),
  [execution contexts](../../crates/omnigraph/src/engine/context.rs) and
  [catalog publication](../../crates/omnigraph-catalog/src/publisher.rs)
  show separate resource lifetimes that server ingress accounting does not own.
- Native `MergeInsert` selects cached DataFusion contexts internally;
  `LanceExecutionOptions` accepts numeric limits but no injected shared pool.
  `Session::with_spill_store` does not govern DataFusion's disk manager or merge
  temporary datasets. These are concrete integration gaps, not missing counters.
- The 2026-10-02 `lance_surface_guards.rs` read probes extend this evidence:
  dropped local reads still execute, separate standard scan schedulers retain
  independent read budgets, and dropped `spawn_cpu` receivers retain their
  captured inputs until the native jobs finish. The pinned helper submits
  eagerly to a private runtime; production KNN uses it. Owning GET futures and
  payloads through the public store wrapper would not own subsequent native
  CPU/decode producers. These are read-only resource-lifetime gaps, not evidence
  of graph corruption; the umbrella's narrower same-engine E1 remains unqualified.

The 2026-10-02 upstream audit also examined released
[Lance 12.0.0](https://github.com/lance-format/lance/releases/tag/v12.0.0)
and main `b0fa4f76cd8dde3b9a8e4076558f585a7688ebdc`. Neither supplies the missing
ownership boundary. The newer
[`spawn_cpu`](https://github.com/lance-format/lance/blob/v12.0.0/rust/lance-core/src/utils/tokio.rs)
preserves panic payloads, but abandoned callers still leave native work running;
the [scan scheduler](https://github.com/lance-format/lance/blob/v12.0.0/rust/lance-io/src/scheduler.rs)
still has no public asynchronous drain. `FragReadConfig::with_scan_scheduler`
already exists in Lance 11, but ordinary scans/search and internal execution
construct other schedulers and CPU workers. That fragment-level hook and numeric
execution limits do not establish operation-wide ownership. An upgrade alone
does not pass this gate.

## Qualification and rollout

Extend existing owners; no new GQT grammar or server simulator is required to
record these proofs. GQT owns query results. Real process/socket tests own
disconnect and shutdown. DST explores only scheduling and I/O it actually
controls; an engine-DST pass cannot certify native local blocking work.

| Gate | Existing owner and decisive observation |
|---|---|
| Query child ownership | Existing context, memory and producer tests: hold an actual worker or its returned resource after caller cancellation; query completion must wait and charges must remain. Cover success, error, panic, queued cancellation and one-thread nested progress. |
| Write representations | Existing staging, loader and mutation tests: multiple individually valid tables exceed one aggregate allowance; deletion/cascade scans refuse before retaining an excess ID. Verify unchanged publication and a subsequent small write. |
| Native limitation probes | `lance_surface_guards`: park the real blocking executor, drop native writer/upload/cleanup owners, observe persistence/cleanup occur after release through independent filesystem state. The public alternate-provider guard establishes addressing interchange only. These do not qualify reuse. |
| T6 settlement | Native/engine guards and server `boot_settings`/`data_routes`: hold actual accepted I/O or a child after its caller returns; no reuse capability or early capacity release; release and prove completion. Cover success, error, panic and remote response loss separately. |
| T10 resources | `engine_v2_memory`, loader/merge/catalog owners and server workload suites: saturate ordinary capacity while completion and status actually run; compare counters with independent buffer, worker and file lifetimes. Include multi-table inputs, preparation refusal before excess catalog allocation, cache eviction with a live borrower, and Arrow/JSON overlap. |
| Atomic schema publication | `schema_apply`, `failpoints`, `detached_commit_matrix`: faults before/after publication leave the complete old/new contract and table references together. Verify exact committed outcomes, coherent same-handle and previously opened-handle queries/writes, and no duplicate publication or durable installation step. |
| Sensitivity | Deliberately release an owner early, omit one reserve charge or declare settlement while I/O is held; the corresponding test must fail. A timer alone is no reached-fault witness. |
| Cost | Existing benchmark owners for B1/B2/B5: independent schema-source/IR bytes across retained history and branch/participant widths, mixed traffic, early query termination, cancellation and fault settlement; record offered/admitted/refused/completed/unknown work, latency, peak RSS, scratch, I/O bytes and requests. No CI wall-time threshold. |

On 2026-10-01 the full `lance_surface_guards` owner passed 56 tests, including
the native-lifetime probes and alternate-provider addressing check; its existing
branch-ref compatibility probe remained ignored. Negative controls suppressed
native persistence, retained the multipart owner, omitted the query-worker wait,
and released a worker's registration before destroying its abandoned result.
Each failed its corresponding regression; restored paths passed. Aggregate-write
regressions also failed before the limits were added. The HTTP regression proves
that refusal publishes nothing, keeps admission open and permits a subsequent
small write. This evidence supplies no full-B or performance claim.

1. Ship query-child ownership and the named write-representation limits with
   their regressions. Keep runtime reuse unavailable and preserve fail-stop for
   uncertain effectful operations. Native limitation probes remain qualification
   evidence, not a production settlement API.
2. Qualify complete native ownership and resource integration through public
   APIs, using a released dependency upgrade if the pinned hooks are insufficient.
   Extend the existing substrate guards; preserve local/S3 semantics and Azure's
   separate admission-wrapper and qualification boundary. A forced wrapper lane
   alone does not pass this gate.
   The first upstream target can be a read-only operation scope propagated
   through ordinary scans, search, take, Blob reads and cached loaders. It must
   register I/O and CPU/decode children before dispatch, retain abandoned outputs
   through destruction, close root admission and expose a joinable completion
   boundary with shared finite admission. Cached sessions cannot capture the
   first caller's scope. Qualifying that boundary can discharge E1's read-tail
   obligation; persistence and multipart cleanup remain separate full-B work.
3. Complete scope propagation, the remaining aggregate envelopes and protected
   completion together. Pass T6/T10 and atomic-schema/coherent-view validation
   before exposing generic engine reuse.
4. Qualify E1's same-engine transition separately under the umbrella's
   serving-view and deployment protocol. It cannot assume a reuse capability or
   advertise general native settlement. There is no deployment endpoint or
   alternate job store in this decision.

## Compatibility, invariants and alternatives

This decision adds no persistent graph format, graph reset, publication door,
request idempotency store or older-client adapter. Graph data, identities,
branches and retained history are preserved. Multi-table writes, large deletes
and Overwrite loads that previously succeeded can now receive
`ResourceLimitExceeded` (HTTP 413 with structured `resource_limit` details).
The removed-ID allowance charges each ID its UTF-8 length plus 24 bytes, so it
holds 671,088 IDs of 26 bytes summed over the touched tables. An Overwrite of a
type whose IDs are generated per load removes every committed ID, and a delete
shares the allowance with its cascaded edges. A delete can be split into several
commits; an Overwrite cannot.
These limits add no environment variable or session setting. Query success and error wait for registered
graph workers; cancelling a caller does not free a running worker's resources.
Full native settlement, completion reserves and runtime reuse remain unavailable.

Architectural invariants 2–5 and 11–13 remain binding. A task counter or quiet
interval is rejected because neither proves native settlement. Per-graph engine
subprocesses would change the in-process server architecture and still cannot
resolve remotely accepted writes; they are not an implicit fallback. A second
allocator, shadow recovery ledger, patched Lance or cloud-only shortcut would
create parallel ownership and remains outside this decision.

## Outstanding qualification

- Select and qualify the public native integration route and define its
  backend-specific terminal evidence. Current Lance 11.0.0 supplies no complete
  qualifying route through the existing integration.
- Measure and fix the initial workload profile, process totals, per-operation
  envelope and minimum completion reserve, including history-dependent catalog
  work with independent source/IR sizes and native execution contexts. Measure
  early-completion and cancellation latency as well as retained resources;
  joining graph workers alone bounds neither duration. Reject unsupported
  dimensions explicitly.

## Decision log

- 2026-10-03: The umbrella acceptance replaces the draft/proposal descriptions
  in Motivation and observable behavior, General reuse and schema activation,
  and Qualification and rollout with an accepted,
  separately gated E1 decision. Native settlement, resource bounds and engine
  reuse remain unqualified; this amendment grants no new reuse capability.

- 2026-10-02: Audited released Lance 12 and current upstream main; a dependency
  upgrade does not provide native settlement. Identified ordinary-read scope
  propagation as the first upstream integration target, without qualifying E1
  or weakening full B's write and remote-outcome requirements.

- 2026-10-02: Extended Implemented boundary and the existing producer test to
  join graph workers before propagating execution/destructor panics. Server
  read ownership keeps the execution alive after caller loss; the native
  limitation and runtime-reuse gates remain unchanged.

- 2026-10-02: Added read/CPU limitation probes to Substrate evidence. They
  extend the existing native-lifetime finding to read-only work without
  changing the accepted contract or asserting that E1 requires generic reuse.
- 2026-10-01: Initially drafted on the maintainer's instruction to proceed
  with full B. The source audit found missing native settlement/resource hooks;
  the first draft and its limitation probes kept runtime reuse unavailable.
- 2026-10-01: Accepted on the maintainer's instruction to build. The implemented
  slice owns graph-query children and bounds the named mutation/load
  representations. The native probes and public-provider experiment establish
  remaining qualification gaps; they do not provide a reusable drain or change
  storage routing. Implementation is partial until the full-B gates pass.
- 2026-10-02: Amended after atomic schema contracts landed. Replaced the
  implemented-boundary caveat about earlier schema completion, the effect-outcome
  statement that schema installation may remain owed, and the reuse section's
  contract-installation/sentinel continuation and successor-capability rules.
  Replaced its claim that engine `refresh` is effectful with read/adopt behavior,
  retaining the local writable-open probe boundary. Replaced the schema-
  continuation evidence gate and the rollout statements requiring it and making
  E1 consume the generic reuse capability. The motivation and activation section
  now distinguish the umbrella draft's separately unqualified same-engine E1
  transition. Generic settlement/resource guarantees remain accepted and partial;
  added independent source/IR-history costs and cancellation/early-completion
  latency to their qualification work.
