---
rfc: "2026-09-29-server-runtime-and-online-deployment"
title: "Server runtime and online deployment"
track: maintainer
status: draft
implementation: in-progress
authors:
  - OmniGraph maintainers
created: 2026-09-29
updated: 2026-10-01
discussion: https://github.com/ModernRelay/omnigraph/pull/799
supersedes:
  - "0034"
  - "0035"
  - "0036"
  - "2026-09-10-server-lifecycle-and-online-deployment"
superseded_by: []
blocked_on:
  - "B: native accepted-I/O settlement, completion-memory/local-I/O reserves and engine-work bounds"
  - "E1: versioned single-outstanding-deployment ledger, achieved-result and active-witness encoding"
  - "E1: authorization, writer admission/handoff, finalization ordering and crash reconciliation"
  - "E1: effect-free validation, settled-engine reuse and bounded preparation"
  - "Azure E1: separately accepted lease-preserving submission authority and qualification"
---

# RFC: Server runtime and online deployment

## Summary

The server owns admitted writes through completion, activates schema and stored
queries without restarting, and reports exact outcomes and graph availability.
The engine owns graph publication and schema completion; the existing cluster
ledger owns deployment input and applied results. No new transaction manager,
content-recovery log or job queue is added.

This proposal targets **v0.12 only**. Deploy the CLI, server and cluster tools as
one qualified v0.12 build. There is one wire contract, without older-client
adapters, legacy response aliases or a mixed-version support matrix. Version
refusal and data-preserving migration remain required.

Online deployment drains affected graph requests while keeping the PID and
listener alive. Unaffected graphs continue serving. A cluster accepts one
outstanding deployment; another submission is refused until its predecessor's
effects and required completion have settled.

## Scope and baseline

The baseline is main `4f3a09f0` (30 September 2026). Table writes already stage
detached versions and publish final pins together. Schema publication may still
precede contract installation. HTTP handlers await writes; registry and stored
queries are built at boot. Boot readiness, shutdown deadlines, input/actor limits
and bounded trust-file refresh exist, but do not establish ownership through
outstanding I/O or coherent online activation.

The initial deployment class changes only schema and stored queries. Graph
inventory, roots, policy, credentials, providers, trust bindings and external
Blob policy stay fixed. Existing schema migration and destructive-change
approval rules still apply. The following are outside this RFC:

- independently available historical reads during a schema transition;
- durable data-request idempotency keys and lookup after a lost response;
- live replacement of other runtime bindings, additional serving roles and
  general-purpose recovery scheduling;
- binary/storage upgrades, automatic embedding and new test-harness protocols.

Those capabilities need separate decisions. The former E2, F and G increments
are no longer implementation commitments here. Narrow retry of transient graph
startup/completion failures remains in scope to prevent sticky quarantine.
Current behavior and test ownership live in [writes](../dev/writes.md),
[recovery](../dev/recovery.md), [serving](../dev/control-plane.md) and
[testing](../dev/testing.md). This draft does not change shipped behavior.

## Observable behavior

| Situation | Required behavior |
|---|---|
| Unsupported wire contract, incomplete body or admission refusal | Refuse before graph effects; report the supported action when delivery is possible. |
| Client disconnects after write admission | The original operation remains owned and counted; disconnect does not cancel or replay it. |
| Result delivery fails | Preserve exact known publication evidence; unknown outcome never permits automatic retry. |
| Schema/query deployment | Keep PID/listener; affected admissions return 503 while admitted work settles and coherent bindings activate. |
| Invalid candidate before effects | Preserve previous coherent service; resumption uses a fresh admission epoch and validated authority. |
| Unresolved published transition | Keep affected admissions closed until engine completion and validation establish a safe view. |
| Known unavailable graph | Keep it in authorized inventory and return 503; reserve 404 for an unknown graph. |
| Shutdown | Close admission and settle registered work against one absolute deadline; cutoff never claims success. |

## Authority and completion

| Responsibility | Owner |
|---|---|
| Graph contents and lineage | Engine publication through `__manifest` |
| Accepted schema and unfinished installation | Accepted/staged contract bound to its publishing commit |
| Initialization, native controls and reclamation | Existing engine protocols and collector |
| Deployment input, achieved result and serialization | Existing cluster ledger and lock/CAS |
| Serving activation and request lifetime | Server runtime and non-reusable admission epoch |
| Caller outcome | Exact engine result plus execution/delivery evidence |

[Detached-only table pins](2026-09-21-detached-only-tables.md) are final at
publication. The server never promotes pins, repairs linear HEADs or compensates
individual tables. Schema installation remains engine work: publication evidence
determines whether staging is installed or discarded. Supported failures must
permit later writes on the same live handle once faults stop. Error return and
dropped futures need separate evidence. Initialization and native controls retain
their own completion rules; cluster recovery records remain cluster authority.
Unknown or legacy recovery evidence is preserved and refused under the current
[recovery contract](../dev/recovery.md), never reinterpreted by a new server.

Deployment configuration designates one mutation-capable serving process for the
complete canonical cluster root. Before enabling online mode or transferring
ownership, the operator stops and excludes other writers, including old binaries,
direct openers and maintenance jobs. The ledger lock/CAS serializes records; it
is not a distributed graph-writer fence. Timeout or lock expiry never authorizes
takeover. A deployment unable to maintain this exclusion refuses online mode.
A second writable opener can discard live schema staging, so schema apply stays
inside the serving mutation owner.

## Operation ownership

The accepted [Owned server operations](2026-09-30-owned-server-operations.md)
decision implements bounded request-independent write ownership and shared
shutdown as a foundation. Its server-task/body accounting does not qualify the
native-I/O settlement and completion reserves required by full B below. Its
read/write capacity is independent, proven engine pre-effect refusals remain
nonfatal, and uncertain completion contains the whole process with early
nonzero exit after remaining known logical owners finish.

The focused [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md)
proposal specifies full B's ownership mechanism, bounded preparation and
completion allowances. Its pinned-substrate audit and limitation probes keep
reuse gated on native hooks and remote terminal-outcome qualification; they do
not turn the owned-operation foundation into a reusable drain.

Before any await or effect, admission registers the write with its owner and
captures its epoch, trusted actor, Session settings, immutable inputs,
preconditions and resource reservations. No owned closure executes twice.
Reads remain request-owned and cancel cooperatively.

Ownership covers child producers and accepted storage I/O. Task return, panic,
receiver loss or join does not prove that a submitted request cannot finish
later. Inputs and permits stay charged until settlement or atomic transfer to a
registered successor owner with the same identity. Delivery buffers have finite
accounting; a lost receiver cannot retain unbounded completed results.

Read observer permits last through response-body and producer settlement.
Mutation, load, merge, branch controls, schema apply and maintenance participate
in ownership and draining. Panic/channel loss reports unknown unless independent
evidence establishes the outcome. Cutoff suppresses late success; an already
started response stream ends with a body error or closure.

Read/write admission lanes close atomically and never reopen. A request losing
the close/acquire race refuses instead of resolving onto a newer runtime. Safe
resumption gets a fresh epoch. Drain proves settlement of that epoch, not zero
HTTP connections; timeout leaves it closed and grants no apply, cleanup or replay
authority. A parked drain retains ownership and revalidates after waking.

Shutdown, activation and registration share one ordering boundary. Once stopping
wins, no candidate activates. The existing absolute shutdown deadline covers
active/retiring runtimes, candidates, streams and owned operations. The final
runtime swap is synchronous and infallible after validation; predecessor closure
and successor registration have no intervening I/O. Participants deregister only
after settlement. Disposal cannot extend the deadline; the existing watchdog
terminates at cutoff and leaves unresolved outcomes for startup reconciliation.

## Serving views

A serving view binds graph/root incarnation, schema/catalog identity, stored
queries, fixed authorization/provider bindings and activation witness. Capture it
before dependent routing or authorization and retain it through the request.
An engine snapshot remains per-attempt authority; a Session alone is not a
serving view. Existing trust-file refresh retains its qualified fixed binding.

Candidate construction and validation are effect-free; writable open/refresh is
not a probe. Completion and apply are owned effectful work requiring authority
and a verified drain. Reuse a settled engine only when all affected mutable users
have settled, other effective bindings are unchanged, and schema/cache/identity
validation passes.

Activation rechecks attempt identity, predecessor epoch, content, ledger
reference, engine completion, budget ownership, stopping state and absolute
deadline under the final synchronous boundary. An old candidate cannot activate
against a later epoch. Check expiry even if its timer has not fired. Expired
candidates remain charged until disposal settles; successful candidates transfer
their reservations to serving ownership.

## Online deployment

Use the existing configuration ledger; add no deployment HTTP route or parallel
job store. E1 introduces a new ledger version with explicit online mode. Install
it only under the stopped-writer transition above. In that mode:

- `cluster apply` validates, authorizes and records immutable input under the
  cluster lock/CAS. It performs no writable graph open, sweep or schema completion.
- Public Core direct apply and effectful `cluster refresh`/`import` refuse. Use
  read-only `observe` for inspection. Only the serving owner changes achieved
  results, retires completion artifacts or consumes associated approvals.
- `state.lock: false` and incompatible ledger clients refuse before effects.
  Version admission cannot revoke already-running or arbitrary storage writers;
  the operator must exclude them.
- Offline effects require explicit authorized handoff, stopped/contained prior
  work and completion reconciliation. Submission failure never triggers an
  offline fallback.

**One outstanding deployment per cluster.** Recording a validated revision
atomically reserves that slot under ledger serialization. A competing new
submission receives a typed busy refusal before being recorded or applied; there
is no waiting deployment queue, supersession or skipped-revision state. Inspection
and observation of the original revision remain available. The slot survives
restart and is released only by durable settlement of all attempted effects and
required schema/control completion. Unknown effects keep it occupied.

1. Record one immutable revision: content-addressed input, affected graphs,
   intended per-resource digests, initiating principal, approval evidence and
   exact **achieved base**. The base identifies the durable applied projection
   using its result revision, capture CAS and per-resource digests. Activation is
   a separate observation and is never used as the base's validity test.
2. The designated process observes pending work at boot and at a bounded interval.
   Resolve the original revision before deciding what remains. Verify provenance
   and authorize the whole effect set under current applied policy before any
   effectful open or write. Read-only observations are not deployments.
3. Reserve preparation/completion capacity, prevalidate, close affected admission
   and drain. Revalidate the achieved base and graph authority after drain. A
   stale base is a typed pre-effect refusal; a later attempt needs a new immutable
   input and fresh validation/authorization. Never silently rebase an approval.
   Later observation/CAS records alone do not change the achieved projection.
4. Apply within the serving mutation owner; finish any published schema contract
   and validate matching schema/query bindings. Current cluster apply may sweep
   outside the desired diff: refuse pending recovery or staged work unless every
   completion participant is in the verified, authorized and drained scope.
5. Persist apply's own per-resource outcomes and achieved digests under lock/CAS.
   Set `config_digest` only for full convergence. Multiple graphs may converge
   partially; this is not a cross-graph transaction. Build the candidate from the
   achieved projection, not the requested bundle.
6. Activate under the serving-view rules and record the observed active witness.
   Applied results and activation remain distinct. Before releasing the slot,
   settle candidate work and resolve or fence its activation attempt so it cannot
   later install an old view over a successor.

A settled pre-effect refusal leaves the previous achieved base intact. A settled
partial or applied-but-inactive result supplies an exact base for a corrective
revision once required completion is finished; lack of activation cannot wedge
the ledger. Reobserve or inspect the old revision without replaying its effects,
reactivating its configuration or sampling a later HEAD as its result. Lookup
uses current authorization and finite retention; unresolved completion authority
cannot expire. Missing results never prove non-execution.

The versioned encoding must bind each revision to its graph effects, finalization
and results. E1 requires publication ordering and crash/restart evidence before
acceptance: a poller and in-memory flag do not provide durable identity. Startup
reconciles unfinished transitions before effectful open or readiness; it never
blindly reapplies the bundle.

## Exact outcomes

| Evidence | Permitted action |
|---|---|
| Not admitted, or settled with proved no effects | Fresh attempt only under the typed whole-command retry/precondition contract |
| Running, or outstanding I/O unproved | Observe the original owner; preserve reservations; do not duplicate |
| Settled with an exact committed/no-op/compound result | Return that result and any completion obligation |
| Settled with unknown effects | Reconcile evidence; settlement alone never authorizes replay |

A2's accepted [Exact merge receipts](2026-09-30-exact-merge-receipts.md) decision
returns a merge's own `CommitOutput` on the branch merge route, GQ merge through
`/mutate`, and CLI JSON: `graph_commit_id`, `graph_manifest_version`, optional
`graph_branch`, `parent_commit_id`, `merged_parent_commit_id`, `actor_id` and
`created_at` in Unix microseconds. Optional fields come from that publication.
Fast-forward publishes a target commit whose merged parent is the source head;
`already_up_to_date` has `commit: null`. Later history cannot construct a receipt.
Missing required evidence is a protocol failure, never a legacy fallback.

Optional source deletion is a separate effect. Failure retains the exact merge
receipt, `branch_deleted: false` and `branch_delete_error_details` (`ErrorOutput`).
Remove the legacy `branch_delete_error` string alias. Exit 0 deliberately means
the merge succeeded; its separately reported deletion failure does not authorize
replaying the merge.

The accepted [Owned server operations](2026-09-30-owned-server-operations.md#whole-command-failure-outcomes)
decision owns A3's initial contract. For
[issue 466](https://github.com/ModernRelay/omnigraph/issues/466), preserve
structured errors through ordinary and streamed responses and classify the entire
data-write command. Failures carry
`command_outcome { execution, effects, action }`; supported actions are `retry`,
`refresh`, `recover` or `reconcile`.

| CLI result | Contract |
|---|---|
| Exit 75 | A bounded caller retry of the whole command is safe with unchanged preconditions and no earlier work still able to produce effects. Initially only a verified, typed preadmission HTTP 429 `too_many_requests` qualifies. Preserve `Retry-After`; the caller owns the attempt bound. Embedded append/merge load preparation conflicts remain exit 1 pending separate qualification. |
| Exit 4 | Verified HTTP 412 conditional mismatch with no earlier whole-command work able to produce effects; re-read and choose a new precondition. An embedded mismatch stays exit 1 because writable open may have completed earlier work. |
| Exit 1 | Other failure, including truncated/malformed success or unknown outcome. Generic 409/503, resource exhaustion, read-set conflict and completion-required errors never imply retry permission. |
| Exit 0 | Exact successful/no-op result, including merge with separately reported optional deletion failure. |

Classification adds no automatic retry. Compound commands do not inherit safety
from their final subrequest. Other command families keep their defined semantics;
this is not a promise to emulate older CLI versions. Durable data-request
idempotency and post-disconnect lookup are outside scope, so disconnected callers
may still receive an unknown outcome.

## Resource bounds

Extend existing input, actor and keyed-write limits. Bound aggregate ingress
before collection/parsing, admitted work, actor-record cardinality, retained
inputs, decoded data, staging, validation, output, candidates and retiring views.
Every cap names its resource, scope and lifetime. Count shared allocations once
and release them after their last producer/user; request bytes are not an engine
memory budget. Overload refuses before admission; this RFC adds no request queue.

Reserve bounded execution, memory and local I/O for finishing admitted work,
qualified completion, shutdown and status. Ordinary traffic cannot consume it,
and completion cannot wait on permits held by work it must settle. Keep the small
status allowance separate. Reserves neither guarantee storage progress nor extend
deadlines. Full B requires the reserves and independently qualified native-I/O
settlement specified by [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md).
The accepted owned-operation foundation bounds server tasks and retained inputs;
the engine decision adds graph-query child joins and specific aggregate
mutation/load representation limits. Neither a task join nor these limits grant
activation, reclamation or retry authority. Full native/resource qualification
remains outstanding.
Reserve candidate headroom before closing healthy admission; insufficient capacity
refuses. Keep input, caller-wait, read, deployment and shutdown deadlines distinct.

Catalog publication cost grows with history. Qualify request/byte/RSS costs over
the supported workload; flat request counts alone prove no bound. Refuse work
outside qualified limits without discarding history or bypassing publication.
Change-feed pages must progress through every accepted retained commit, including
wide rows and encoding expansion. Qualify cursor resumption, oversized changes
and abandoned bodies under the [retained-history contract](0030-cdc-time-travel.md).
Never silently skip a change or wedge a cursor to satisfy a page budget.

## Historical reads

Online deployment may drain/refuse affected historical requests with other reads.
It preserves supported history semantics outside the transition and makes no
promise of independently available historical service during apply. Historical
reconstruction, new retention protocols and historical-only readiness are outside
this RFC. The [shared-schema gate](2026-09-18-shared-schema-gate.md) does not remove
the full affected-graph drain required here.

## Availability and supervision

Keep liveness separate from readiness. Publish one authorized graph inventory
including loading, ready, deploying, blocked and stopping entries, with read/write
availability and a supported action. Replace the legacy list that omits blocked
graphs. Registry membership and availability are distinct facts;
`served_graph_count` counts registry entries. An empty valid inventory is ready;
shutdown is unready. No historical-only readiness mode is introduced.

Keep boot revision/digest as boot facts. The active witness identifies the
achieved revision and per-resource digests actually serving; exact deployment
results remain behind authorized lookup. Never infer activation from boot facts
or successful apply. Status reads bounded snapshots outside blocked data lanes;
it cannot open graphs, start completion or reconstruct history. Protect inventory
and diagnostics with current authorization and scrub secrets and sensitive inputs.

For transient startup or schema/control completion failure under unchanged
bindings, retain the graph entry and schedule bounded retry through its designated
owner. Use one attempt per graph, coalesced wakes, fair scheduling and capped
backoff; new wakes do not reset the retry budget. Attempt identity fences stale
callbacks. Persistent or unknown failures remain explicit refusals with required
action. Report phase, attempts, classified failure and limiting resource.
Supervisor `Retry-After` requires a finite scheduled retry; admission 429 may give
caller-backoff guidance without scheduling work. Neither header authorizes write
replay. A timer alone is not recovery progress.

Declared `@embed` configuration must not silently imply population occurred:
report the load/capability limitation. Automatic embedding remains owned by
[Ingest-time embedding reconciliation](0015-ingest-embeddings.md).

## Invariants

The [architectural invariants](../dev/invariants.md) remain binding. This proposal
adds no second publication authority, distributed-fencing claim or legacy recovery
engine. Tests must prove the boundary whose behavior is promised.

## Compatibility

**One v0.12 release-line contract.** CLI, server and cluster tools are upgraded
together to a qualified build. Earlier/later release lines, missing required
contract evidence and incompatible protocol shapes are refused, without warning-
and-continue, alternate response aliases or automatic downgrade. The accepted
[v0.12 HTTP admission](2026-09-30-v012-http-admission.md) decision owns A1's exact
header, public discovery, pre-effect refusal and CLI response validation. Package
version alone does not establish that contract or prove another increment
implemented. A response incompatibility after dispatch reports unknown effects,
not a proven no-effect refusal. No general surface-hash or mixed-version
negotiation framework is required.

v0.12 is a software release, not internal manifest stamp 12. Existing formats,
schema vintages and explicit offline upgrades follow the
[current storage contract](../dev/versioning.md#current-storage-contract).
Preserve graph data, identities, branches and retained history. Unsupported roots
refuse; a qualified offline upgrade may prepare them, never reset/export-only
replacement. Cluster-managed conversion remains gated by
[Explicit storage upgrades](0064-explicit-storage-upgrades.md). Ledger migration
and downgrade refusal are separate from wire admission; stopped-writer migration
and verified whole-root backup remain mandatory where that upgrade requires them.

Azure retains its mandatory admission wrapper. E1 online submission is unsupported
until a separately accepted, qualified lease-preserving path exists: the live root
lease prevents a second wrapped command from starting. Refuse unqualified online
attempts; never bypass, share or break the lease or write the ledger directly.
Offline apply retains its stopped-writer wrapper procedure.

The v0.12 OpenAPI, CLI contract, user guidance and tests ship together. Document
intentional breaks: structured deletion errors replace the string alias; one
inventory includes unavailable graphs; known unavailable graphs use 503, unknown
graphs 404, with authorization/disclosure rules intact. Generic 503 is not retry
permission. There is no promise to preserve older wire fields or exit behavior.
No unsupported online transition may silently restart or reset the graph.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Restart for every schema deployment | Interrupts unrelated work and does not solve request ownership. |
| Refresh a handle or swap only queries | Does not establish coherent bindings or settle old users. |
| Separate apply process while serving | Current schema staging lacks a qualified cross-process writer fence. |
| Queue and supersede deployments | Adds ordering states without a requirement for concurrent deployment submission. |
| Require historical overlap or durable data idempotency first | Couples live schema activation to separate storage and request protocols. |
| Add a deployment endpoint/job store or compatibility framework | Expands authority and support machinery beyond the single v0.12 contract. |

## Qualification

Extend [existing test owners](../dev/testing.md). The
[server-testing RFC](2026-09-26-self-contained-server-testing.md#server-coverage-requirements)
owns detailed scenarios and harness prerequisites; it cannot claim unimplemented
server controls. Keep T/B identifiers stable, with these acceptance obligations:

| IDs | Required evidence |
|---|---|
| T1–T3 | Own-publication receipts; lost delivery never replays; v0.12 admission/refusal and whole-command outcomes, including compound deletion failure. |
| T4 | Atomic publication, exact pins, schema completion and same-handle progress across faults; legacy evidence refuses unchanged. |
| T5–T7 | Disconnect, outstanding accepted I/O, shutdown and actual process restart preserve ownership and settlement through reached boundaries. |
| T8.live | Same PID/listener and coherent schema/query activation; unaffected graph progress; submission-only CLI, writer exclusion, Azure refusal and crash safety. T8.history/retention are deferred outside scope. |
| T9 | Concurrent new submission refuses while one revision is pending, including across restart. Settled partial/inactive D1 permits corrective D2; stale-base submission refuses without effects; original lookup never replays. |
| T10 | Declared bounds and completion/status reserves hold under saturation, slow/abandoned consumers and repeated failed deployments; reconcile counters with independent allocation/lifetime evidence. |
| T11 | Wide-feed progress and bounded authorized status; unavailable 503 versus unknown 404, correct disclosure and bounded transient startup retries. |

Reuse B1–B6 for publication/history cost, mixed serving load, deployment pause and
candidate residency, maintenance interference, failure settlement, and slow/wide
output respectively. B3's independent historical-overlap experiment is deferred.
Report build/backend/workload identity, offered/admitted/completed/refused/unknown
work, latency distributions, bytes/requests and memory. Timing belongs to the
benchmark harness, not CI correctness thresholds. Engine-DST scheduling and
`--measure` support engine evidence; they do not establish server process,
transport, retained-I/O or performance behavior. Missing services, zero selected
cases and unreached faults do not pass qualification.

## Rollout

| Increment | Deliverable | Shipping gate |
|---|---|---|
| A | A1 v0.12 HTTP admission; A2 own-publication merge receipts; A3 CLI outcomes | A1 follows [its accepted decision](2026-09-30-v012-http-admission.md); A2 is qualified under [Exact merge receipts](2026-09-30-exact-merge-receipts.md), including T1/T2; A3's initial typed-429 and qualified-HTTP-412 contract is qualified under [Owned server operations](2026-09-30-owned-server-operations.md) |
| B | Owned writes, read/stream accounting, drain and shared shutdown | [Owned server operations](2026-09-30-owned-server-operations.md) qualifies the task/body foundation. [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md) is accepted and partially implements query-child ownership and named aggregate write limits. Native settlement, completion reserves and runtime reuse remain unqualified under its T6/T10 gates. |
| C | Schema/control completion, owned maintenance and bounded transient startup retry | T4/T6/T7/T11; same-process progress and protected reclamation |
| D | Aggregate budgets, feed progress and embedding diagnostics | T10–T11 and workload qualification |
| E1 | Same-process schema/query activation with one outstanding deployment | B, relevant C/D bounds, T8.live/T9 and durable ledger/crash gates; Azure separately gated |

A and focused C/D repairs may proceed alongside B. E1 does not wait for deferred
historical serving or data idempotency. Implement slices against the v0.12 contract
and update their actual status, OpenAPI and user/developer documentation together.
Merging this draft supplies no product qualification.

## Unresolved questions

Before accepting each affected increment, its owners must specify:

1. CLI/server: any broader retry allowance needs separate whole-command proof.
   A3 and the B foundation are qualified under
   [Owned server operations](2026-09-30-owned-server-operations.md); A1 admission
   and A2 exact receipt contracts and evidence remain with their accepted owners.
2. Cluster/server: versioned pending slot, immutable input/achieved base, exact
   effect/finalization/result binding, active witness, observation interval,
   result retention, migration and stopped-writer handoff.
3. Engine/server: qualify the native settlement mechanism and measured
   admission/completion profile in [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md)
   before exposing a reuse capability.
4. Azure: lease-preserving online submission and backend qualification.

## Decision log

- 2026-10-01: Accepted the focused engine decision on the maintainer's build
  instruction. Query-child ownership and named aggregate write-representation
  limits implement part of B/D. Resource bounds, Rollout B and Unresolved item 3
  now distinguish that shipped scope from full-B qualification; E1 stays gated.

- 2026-10-01: Added the focused full-B proposal. Operation ownership now names
  its native/remote qualification gap. Resource bounds replaces "Full B requires
  these reserves and independently qualified native-I/O settlement" with the
  focused contract reference; the Rollout B shipping-gate sentence and Unresolved
  questions item 3 now identify that proposal and its outstanding acceptance
  gates. No runtime-reuse or online-deployment support is claimed.

- 2026-09-29: Consolidated the four server/recovery drafts after detached table
  publication and final pins removed their content-recovery machinery. Preserved
  ownership, late-I/O, exact outcomes, coherent activation, authorization and
  retention obligations. Split the former E gate into E1 drained live activation
  and E2 independent historical availability; E1 no longer waits for E2. Kept
  ledger-declared deployment, same-process apply under the existing writer
  boundary, and no-reset migration. Updated the baseline for GQT concurrency,
  existing limits/trust refresh and history-dependent catalog costs. Separate
  storage and testing proposals retain their own decisions.
- 2026-09-30: Replaced the implicit CLI handoff with explicit versioned online
  submission-only mode, operator-established writer exclusion and offline handoff
  requirements; effectful refresh/import cannot bypass owner-only result
  reconciliation. Replaced the active/skipped/refused predecessor whitelist with
  separate validated-parent and achieved-base rules, including corrective
  successors after settled partial/inactive results. Restored and gated Azure
  online admission. Scoped scheduled `Retry-After` to supervision and recorded
  the intentional known-unavailable 404-to-503 compatibility change. Updated
  qualification rows, the landed shared gate and storage-support reference.
- 2026-09-30: Narrowed support to one qualified v0.12 CLI/server/cluster-tool
  contract. Exact outcomes and Compatibility replace mixed-version receipt
  qualification, legacy deletion-error aliases and preservation of older wire
  fields with coordinated upgrade and pre-effect contract refusal. Availability
  replaces parallel legacy/new inventories with one authorized inventory.
  Online deployment replaces validated-parent ordering, supersession and skipped
  revisions with one durable outstanding slot; achieved-base checks, corrective
  partial-result successors and original-result reconciliation remain.
  Resource bounds now require immediate refusal rather than an optional request
  queue. Scope, Historical reads, Qualification, Rollout and acceptance gates
  remove the former E2/F/G commitments; bounded transient startup retry remains.
  Detailed test/benchmark recipes stay with their existing owners. No storage
  compatibility fence, data-preserving migration rule or implemented behavior
  changes; software v0.12 and internal manifest versions remain separate.

- 2026-09-30: Split A into independently scoped steps. The accepted v0.12 HTTP
  admission decision fixes A1's wire encoding and validation; Compatibility,
  Rollout and Unresolved questions now link to that owner. This umbrella remains
  draft: the instruction to build A1 does not accept the remaining increments.
- 2026-09-30: Moved A2's exact receipt contract and T1/T2 gates to the accepted
  Exact merge receipts decision. Exact outcomes and Rollout now identify that
  owner; Unresolved questions no longer calls A2's contract undecided. A2 is
  implemented and its T1/T2 qualification is recorded there. A3 and the remaining
  lifecycle/deployment increments stay proposals in this draft.
- 2026-09-30: Accepted A3 and the bounded operation-ownership foundation through
  Owned server operations. Operation ownership, Exact outcomes, Resource bounds,
  Rollout and Unresolved questions now distinguish its server-task/body contract
  from full B's unqualified native-I/O settlement and completion reserves. The
  Exit 75 row replaces the proposed embedded append/merge preparation-conflict
  allowance with typed preadmission HTTP 429 only; embedded conflicts remain
  exit 1 pending separate proof. Online activation remains gated on full B.

- 2026-09-30: Qualified the initial A3 contract and B's server-task/body foundation
  under Owned server operations. Rollout and open gates now distinguish completed
  local ownership/outcome evidence from native-I/O settlement, completion reserves
  and engine-work bounds that still gate reusable drain and online activation.
