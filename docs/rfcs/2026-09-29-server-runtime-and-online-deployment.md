---
rfc: "2026-09-29-server-runtime-and-online-deployment"
title: "Server runtime and online deployment"
track: maintainer
status: accepted
implementation: in-progress
authors:
  - OmniGraph maintainers
created: 2026-09-29
updated: 2026-10-03
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
  - "E1: same-engine transition qualification, coherent request bindings and bounded preparation/retirement"
  - "Existing-cluster rollout: qualified data-preserving offline conversion to storage v13"
  - "Azure E1: separately accepted lease-preserving submission authority and qualification"
---

# RFC: Server runtime and online deployment

## Summary

The server owns admitted writes through completion, activates schema and stored
queries without restarting, and reports exact outcomes and graph availability.
The engine publishes graph contents and their accepted schema together; the
existing cluster ledger owns deployment input and applied results. No new
transaction manager, content-recovery log or job queue is added.

This decision targets **v0.12 only**. Deploy the CLI, server and cluster tools as
one qualified v0.12 build. There is one wire contract, without older-client
adapters, legacy response aliases or a mixed-version support matrix. Version
refusal and data-preserving migration remain required.

Online deployment drains affected graph requests while keeping the PID and
listener alive. Unaffected graphs continue serving. A cluster accepts one
outstanding deployment; another submission is refused until its predecessor's
effects and required completion have settled.

## Acceptance boundary

The maintainer accepted the runtime foundations and durable-deployment direction
on 3 October 2026, with implementation and qualification proceeding separately.
The reviewed foundation implementation is [PR #844](https://github.com/ModernRelay/omnigraph/pull/844).
The operation, availability and prepared-schema contracts below are accepted;
`Unknown` establishes neither absence nor permission to replay.
Acceptance does not enable online activation, native runtime reuse, ledger v2
admission, existing-cluster conversion or Azure online submission. Those remain
closed until their explicit design and evidence gates below are met. The ledger
encoding must be completed in this canonical RFC before production persistence.

## Scope and baseline

The baseline is main `b0bfaef8` (3 October 2026). Detached table pins and the
accepted schema contract publish together. Live engine reads capture a matching
immutable snapshot/catalog without reopening. Request-independent write owners,
query-worker joins and named aggregate write limits have landed; native-I/O
settlement and comprehensive resource bounds remain unqualified. Registry and
stored queries are built at boot, and cluster-backed schema apply still requires
restart. The registry retains ready and blocked startup outcomes, with authorized
availability and aggregate readiness. This foundation and existing trust refresh
do not establish online activation or transient startup retry.

The initial deployment class changes only schema and stored queries. Graph
inventory, roots, policy, credentials, providers, trust bindings and external
Blob policy stay fixed. Schema apply remains main-only and requires a single
live branch. Existing migration validation still applies: unsupported changes
refuse before effects, and dropping declarations does not reclaim retained data
or require a destructive-schema override.
The following are outside this RFC:

- independently available historical reads during a schema transition;
- durable data-request idempotency keys and lookup after a lost response;
- live replacement of other runtime bindings, additional serving roles and
  general-purpose recovery scheduling;
- online binary/storage upgrades, automatic embedding and new test-harness protocols.

Those capabilities need separate decisions. The former E2, F and G increments
are no longer implementation commitments here. Narrow retry of transient graph
startup/control failures remains in scope to prevent sticky quarantine. Existing
clusters need a qualified offline v13 conversion before this rollout; its protocol
belongs to [Explicit storage upgrades](0064-explicit-storage-upgrades.md).
Current behavior and test ownership live in [writes](../dev/writes.md),
[recovery](../dev/recovery.md), [serving](../dev/control-plane.md) and
[testing](../dev/testing.md). Acceptance alone does not change shipped behavior.

## Observable behavior

| Situation | Required behavior |
|---|---|
| Unsupported wire contract, incomplete body or admission refusal | Refuse before graph effects; report the supported action when delivery is possible. |
| Client disconnects after write admission | The original operation remains owned and counted; disconnect does not cancel or replay it. |
| Result delivery fails | Preserve exact known publication evidence; unknown outcome never permits automatic retry. |
| Schema/query deployment | Keep PID/listener; affected admissions return 503 while admitted work settles and coherent bindings activate. |
| Invalid candidate before effects | Preserve previous coherent service; resumption uses a fresh admission epoch and validated authority. |
| Unresolved published transition | Keep affected admissions closed until exact outcomes, remaining control work and coherent-view validation establish safety. |
| Known unavailable graph | Keep it in authorized inventory and return 503; reserve 404 for an unknown graph. |
| Shutdown | Close admission and settle registered work against one absolute deadline; cutoff never claims success. |

## Authority and completion

| Responsibility | Owner |
|---|---|
| Graph contents, accepted schema and lineage | One engine publication through `__manifest` |
| Initialization, native controls and reclamation | Existing engine protocols and collector |
| Deployment input, achieved result and serialization | Existing cluster ledger and lock/CAS |
| Serving activation and request lifetime | Server runtime and non-reusable admission epoch |
| Caller outcome | Exact engine result plus execution/delivery evidence |

[Detached-only table pins](2026-09-21-detached-only-tables.md) and the
[schema contract](2026-09-30-schema-contract-in-manifest.md) are complete at
publication. The server never promotes pins, repairs linear HEADs, installs
schema files or compensates individual tables. Lost publication acknowledgements
still require exact outcome reconciliation; complete durable state does not
prove that the matching server view activated. Supported failures must permit
later writes on the same live handle once faults stop. Initialization and native
controls retain their completion rules; cluster records remain cluster authority.
Unknown or legacy recovery evidence is preserved and refused under the current
[recovery contract](../dev/recovery.md), never reinterpreted by a new server.

Deployment configuration designates one mutation-capable serving process for the
complete canonical cluster root. Before enabling online mode or transferring
ownership, the operator stops and excludes other writers, including old binaries,
direct openers and maintenance jobs. The ledger lock/CAS serializes records; it
is not a distributed graph-writer fence. Timeout or lock expiry never authorizes
takeover. A deployment unable to maintain this exclusion refuses online mode.
Native controls, reclamation and deployment result ownership still require this
exclusion. Atomic schema publication does not supply a distributed writer fence.

## Operation ownership

The accepted [Owned server operations](2026-09-30-owned-server-operations.md)
decision implements bounded request-independent write and read-handler ownership
and shared shutdown as a foundation. Its server-task/body accounting does not qualify the
native-I/O settlement and completion reserves required by full B below. Its
read/write capacity is independent, proven engine pre-effect refusals remain
nonfatal, and uncertain completion contains the whole process with early
nonzero exit after remaining known logical owners finish.

The focused [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md)
decision specifies full B's ownership mechanism, bounded preparation and
completion allowances. Its pinned-substrate audit and limitation probes keep
reuse gated on native hooks and remote terminal-outcome qualification; they do
not turn the owned-operation foundation into a reusable drain. E1 requires the
narrower same-engine transition below; it needs separate qualification and grants
no general engine-disposal or reuse capability.

Before any await or effect, admission registers the write with its owner and
captures its epoch, trusted actor, Session settings, immutable inputs,
preconditions and resource reservations. No owned closure executes twice.
After bounded body collection, read execution survives caller loss with its
input reservation and observer. MCP deadlines and cancellation end result
observation while execution keeps its concurrency slot. Streaming producers
still cancel cooperatively on body abandonment; that is not native settlement.

Ownership covers child producers and accepted storage I/O. Task return, panic,
receiver loss or join does not prove that a submitted request cannot finish
later. Inputs and permits stay charged until settlement or atomic transfer to a
registered successor owner with the same identity. Delivery buffers have finite
accounting; a lost receiver cannot retain unbounded completed results.

Read observer permits last through execution, pending result delivery,
response-body and producer settlement.
Mutation, load, merge, branch controls, schema apply and maintenance participate
in ownership and draining. Effectful-operation panic/channel loss reports unknown unless independent
evidence establishes the outcome. Cutoff suppresses late success; an already
started response stream ends with a body error or closure.

Read/write admission lanes close atomically and never reopen. A request losing
the close/acquire race refuses instead of resolving onto a newer runtime. Safe
resumption gets a fresh epoch. A full-B drain proves settlement of that epoch,
not zero HTTP connections; E1 proves the specific transition obligations below.
Timeout leaves admission closed and grants no apply, cleanup or replay authority.
A parked transition retains ownership and revalidates after waking.

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

Admission must atomically capture that view and a lease on its graph epoch.
Both HTTP routing and MCP graph selection use this boundary. Retain the lease
through body collection, engine work, response production and retained output;
disconnect or an MCP deadline cannot drop ownership of surviving workers.
Closing admission prevents new roots, not children finishing admitted work.
Read cancellation needs a request-independent completion owner or an engine
settlement handle; a middleware lease alone cannot prove those children finished.

Candidate validation uses read-only captured state. Current v13 engine `refresh`
reads storage and adopts an in-memory view; it no longer installs a contract.
Changing the serving engine's view still belongs to the owned transition, not
candidate validation. Read-write local open still performs a capability-probe
write and is not an effect-free probe.

E1 retains the same engine and its resource owners. Before apply it closes
affected admissions and finishes admitted requests and registered query workers,
including requests parked before engine snapshot capture. A request must use
matching stored-query and engine-contract identities throughout; an old request
cannot resume with old queries against a new contract. Writes, native controls,
exports and maintenance that can interfere with apply or reclamation must finish
with known outcomes. Apply then publishes atomically and activation validates
the exact achieved contract and query bindings. Other effective bindings stay fixed.

Any remaining native read tail must be proven unable to publish, reclaim or
change serving bindings, and remain owned and charged through its last use under
finite limits. Its resources cannot be disposed or reused to fund activation.
Unclassified tails or uncertain writes refuse the transition; uncertainty retains
the existing process-containment rule. This narrower proof must cover every
reachable path before E1 is qualified. Generic engine disposal/reuse still
requires full B; retaining an engine alone proves neither contract.

Activation rechecks attempt identity, predecessor epoch, content, ledger
reference, transition completion, budget ownership, stopping state and absolute
deadline under the final synchronous boundary. An old candidate cannot activate
against a later epoch. Check expiry even if its timer has not fired. Expired
candidates remain charged until disposal settles; successful candidates transfer
their reservations to serving ownership.
Safe resumption of the previous coherent view allocates a fresh epoch; a closed
epoch never reopens, so an old callback cannot regain admission authority.

## Online deployment

Use the existing configuration ledger; add no deployment HTTP route or parallel
job store. E1 introduces a new ledger version with explicit online mode. Install
it only under the stopped-writer transition above. In that mode:

- `cluster apply` validates, authorizes and records immutable input under the
  cluster lock/CAS. It performs no writable graph open, sweep or schema apply.
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
required control completion. Unknown effects keep it occupied.

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
   and satisfy E1's transition proof. Revalidate the achieved base and graph
   authority. A stale base is a typed pre-effect refusal; a later attempt needs
   new immutable input and fresh validation/authorization. Never silently rebase an approval.
   Later observation/CAS records alone do not change the achieved projection.
4. Apply within the serving mutation owner and verify the exact atomic contract
   publication and matching query bindings. Current cluster apply may sweep
   outside the desired diff: refuse pending control work unless every participant
   is in the verified, authorized and settled scope. No schema installation
   follows publication.
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

### Proposed ledger v2

Keep `__cluster/state.json` and its existing lock/conditional replacement. The
following fields are a proposed encoding, not an admitted storage version:

| Field | Meaning |
|---|---|
| `version: 2`, `mode: online`, `state_revision` | Strict version/mode admission; `state_revision` advances for each ledger replacement. It is not the achieved revision. |
| `applied_revision` | Existing serving-sufficient resource projection, plus `result_revision`, advanced only when that projection changes. `config_digest` remains absent for partial convergence. |
| `outstanding` | Null or one deployment: immutable `deployment_id`, input digest, initiating principal, approval/provenance evidence and achieved base; mutable per-graph effect records and activation attempt. |
| `deployment_results` | Bounded terminal records keyed by deployment ID: original input/base identity, exact per-resource outcomes and achieved-result reference. Their retention cannot remove outstanding authority. |
| `active_observation` | Achieved-result reference, serving digests, process incarnation and activation attempt observed installed. This is historical observation after process loss, never permission to activate. |

The immutable bundle contains normalized configuration semantics and exact schema
and query bytes, addressed by digest in the existing `__cluster/resources`
catalog. Persist and verify those bytes before the submission CAS; never reread
the operator's mutable files to execute accepted work. Bound ledger, bundle,
per-resource and retained-result bytes before reading/decoding or admission. A
lost submission acknowledgement is resolved by the original deployment ID and
input digest; an occupied slot is not permission to submit a duplicate.

The achieved base is `{ result_revision, resource_digests, capture_cas }`.
`capture_cas` records provenance; subsequent submission, effect or observation
records use a fresh CAS without invalidating an unchanged achieved projection.
Version the authorization receipt accordingly: current `PlanAuthorization` ties
validity to the whole ledger CAS and cannot be reused after submission changes
it. Recheck the original principal, exact effects and current applied policy;
persisted authorization evidence is not a bearer capability. Capture graph-head
preconditions after affected requests drain, so ordinary data writes between
submission and drain do not invalidate the configuration base.

Before invoking each schema effect, persist its engine-issued publication intent
under the ledger CAS: graph/root incarnation, exact predecessor, desired contract
digest, actor and preallocated graph commit identity. The engine must validate
that intent and return its own exact publication receipt, or an explicit no-op
with its captured contract identity. Preparation distinguishes an effectful
intent from a no-op certificate bound to the already accepted contract and
desired input. The no-op path cannot issue schema effects; after restart,
revalidate that exact binding and record the no-op, or refuse it as stale.
A no-op requires exact accepted contract/source identity, not just an empty
migration plan. Changed source bytes need a contract-only publication or an
explicit refusal until that path is qualified; never mark unaccepted bytes
achieved merely because they compile to the same schema shape.
A stored-query-only change publishes its
achieved result in the ledger and has no graph commit. A confirmed publication
and its matching query digests advance together in the achieved projection even
when activation fails; a proved pre-publication refusal keeps the previous
bindings. Never derive a receipt from a later HEAD or schema-text equality.

Restart first resolves every started intent from exact retained publication
evidence. A proved publication is recorded without applying again; a proved
pre-effect failure remains a refusal. Also qualify terminal non-publication:
the prior owner and all accepted work are stopped or fenced, protected exact
evidence proves the intended commit absent, and no late publication is possible.
This settles without replay, including a crash after intent CAS but before the
engine call. A sampled HEAD or an empty lookup alone does not prove absence.
Unproved absence or unresolved prior I/O keeps the slot occupied and forbids
replay. Protect the intent's predecessor and
publication evidence from cleanup until its result is recorded. Persist the
achieved result before starting activation; record activation separately. The
final CAS moves the settled record into bounded results and clears `outstanding`
only after effects, candidate work and activation are settled or fenced. A
successor may therefore start after a partial or inactive result, but no old
callback can install over it. Boot invalidates old process activation attempts
and creates a fresh serving witness without replaying graph effects.

The existing `__cluster/lock.json` can survive a killed holder. E1 reports
deployment recovery blocked until an operator establishes that the prior owner
and its accepted work cannot act, then uses the existing exact-ID
`cluster force-unlock` procedure. Unlocking does not settle an outstanding
intent; reconcile it under the rules above. PID or age alone cannot authorize
takeover. Qualify actual process death while holding the lock, since returning
through a failpoint runs its destructor and misses this boundary.

The engine now exposes serializable prepared schema intents and returns its own
`GraphCommit` plus exact source/IR contract identity in `SchemaApplyResult`.
Preparation distinguishes an exact no-op; changed source bytes publish one
contract-only commit even when the migration plan has no table steps. This fixes
the reproduced false convergence for comment-only cluster schema changes.
Execution revalidates the captured root, native main identity, manifest version,
predecessor, source/IR, actor and current policy before effects, without rebasing.

Read-only reconciliation checks the intent's single candidate version immediately
after its base, and verifies that version's own commit, head and contract. A
missing/pruned candidate or metadata-only interposition remains unknown. The
lookup limits selected evidence to three rows plus a duplicate detector; physical
scan I/O can still grow with the history stored in that version. No-op
reconciliation requires its exact captured authority still to be current.
This is positive publication evidence, not a terminal-absence or retention
guarantee. Current cluster recovery still compares digests and does not persist
these intents. Ledger integration, protected evidence and aggregate lookup bounds,
the real process-death matrix, and explicit stopped-writer ledger migration
remain implementation gates. Until qualified, v2 admission and online apply
remain disabled. The API contract lives in [writes](../dev/writes.md).

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
| Exit 4 | Verified HTTP 412 conditional mismatch with no earlier whole-command work able to produce effects; re-read and choose a new precondition. An embedded mismatch stays exit 1 until its whole-command pre-effect proof is qualified. |
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

Catalog publication copies accepted schema source and IR as well as catalog rows.
Qualify source/IR bytes and history depth independently, holding live/touched
data fixed; measure requests, copied/retained bytes and RSS. Flat request counts
alone prove no bound. Refuse work
outside qualified limits without discarding history or bypassing publication.
Change-feed pages must progress through every accepted retained commit, including
wide rows and encoding expansion. Qualify cursor resumption, oversized changes
and abandoned bodies under the [retained-history contract](0030-cdc-time-travel.md).
Never silently skip a change or wedge a cursor to satisfy a page budget. Preserve
exact ranged external-Blob references in feeds; reloadable-export refusal is a
separate contract, and the ranged-reference fix does not qualify wide-row progress.

## Historical reads

Online deployment may drain/refuse affected historical requests with other reads.
It preserves supported history semantics outside the transition and makes no
promise of independently available historical service during apply. Historical
reconstruction, new retention protocols and historical-only readiness are outside
this RFC. The [shared-schema gate](2026-09-18-shared-schema-gate.md) and immutable
read catalogs do not alone prove E1's request-binding or reclamation safety.

## Availability and supervision

Keep liveness separate from readiness. The implemented registry retains actual
startup outcomes as ready or blocked entries; shutdown projects all entries
as stopping. One authorized inventory reports read/write runtime availability,
sanitized failure and a supported action, replacing the separate quarantined
list. Credential graph scope precedes resolution. Blocked graphs disclose 503
only to graph-read or management-inventory authorized callers; an invalid graph
policy or configuration cannot authorize read disclosure. Other callers cannot
discover them through that distinction. Registry membership and availability are distinct
facts: `served_graph_count` counts registry entries; readiness also reports ready
and blocked counts. An empty valid inventory is ready; shutdown is unready.
The listener still starts after graph opening, and nonempty startup still
requires a healthy graph. Loading/deploying states and bounded transient retries
remain gated implementation work. No historical-only readiness mode is introduced.

Keep boot revision/digest as boot facts. The active witness identifies the
achieved revision and per-resource digests actually serving; exact deployment
results remain behind authorized lookup. Never infer activation from boot facts
or successful apply. Status reads bounded snapshots outside blocked data lanes;
it cannot open graphs, start completion or reconstruct history. Protect inventory
and diagnostics with current authorization and scrub secrets and sensitive inputs.

For transient startup or control completion failure under unchanged
bindings, retain the graph entry and schedule bounded retry through its designated
owner. Use one attempt per graph, coalesced wakes, fair scheduling and capped
backoff; new wakes do not reset the retry budget. Attempt identity fences stale
callbacks. Persistent or unknown failures remain explicit refusals with required
action. Unsafe external-Blob policy, an uncomparable storage root, digest mismatch
or unsupported format requires correction, not a transient retry. Candidate
activation repeats the existing root-disjointness, server-safe policy projection
and digest checks even though E1 keeps that policy fixed. Report phase, attempts,
classified failure and limiting resource.
Supervisor `Retry-After` requires a finite scheduled retry; admission 429 may give
caller-backoff guidance without scheduling work. Neither header authorizes write
replay. A timer alone is not recovery progress.

Declared `@embed` configuration must not silently imply population occurred:
report the load/capability limitation without treating a successful nullable load
as failed. Supplied vectors remain unchanged; omitted vectors follow schema
nullability, and ingestion does not call the provider. Automatic embedding remains owned by
[Ingest-time embedding reconciliation](0015-ingest-embeddings.md).

## Invariants

The [architectural invariants](../dev/invariants.md) remain binding. This decision
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

v0.12 is a software release, not internal manifest stamp 12. The current binary
serves storage v13 only; normal open never migrates. Supported standalone roots
have explicit offline routes under the
[current storage contract](../dev/versioning.md#current-storage-contract).
Preserve graph data, identities, branches and retained history. Unsupported roots
refuse; a qualified offline upgrade may prepare them, never reset/export-only
replacement. Existing-cluster rollout is blocked until
[Explicit storage upgrades](0064-explicit-storage-upgrades.md) qualifies a cluster
admission/conversion protocol; direct-path access must not bypass today's refusal.
That gate must prove serving/writer/maintenance exclusion, preserved graph and applied
configuration identities, interruption/resume across all graphs, and refusal to
serve partially converted state. Keep a verified whole-root backup and activate
only after conversion validation. This is an offline rollout prerequisite, not an
E1 deployment or mixed-format serving feature. Ledger migration and downgrade
refusal remain separate from wire admission and graph conversion.

Azure retains its mandatory admission wrapper. E1 online submission is unsupported
until a separately accepted, qualified lease-preserving path exists: the live root
lease prevents a second wrapped command from starting. Refuse unqualified online
attempts; never bypass, share or break the lease or write the ledger directly.
Offline apply retains its stopped-writer wrapper procedure.

The v0.12 OpenAPI, CLI contract, user guidance and tests ship together. Document
intentional breaks: structured deletion errors replace the string alias; one
inventory includes unavailable graphs without a `quarantined` alias;
`served_graph_count` includes those entries, with separate ready/blocked counts.
Known unavailable graphs use 503, unknown graphs 404, with
authorization/disclosure rules intact. Generic 503 is not retry
permission. There is no promise to preserve older wire fields or exit behavior.
No unsupported online transition may silently restart or reset the graph.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Restart for every schema deployment | Interrupts unrelated work and does not solve request ownership. |
| Refresh a handle or swap only queries | Does not establish coherent bindings or settle old users. |
| Separate apply process while serving | Atomic schema publication supplies no cross-process fence for native controls, reclamation or deployment ownership. |
| Require generic engine disposal/reuse before every E1 transition | Retaining the same engine may admit a narrower proof; every interfering effect and retained resource still needs qualification. |
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
| T4 | Atomic contract/table publication, exact pins and same-handle schema/cache progress across faults; legacy evidence refuses unchanged. |
| T5–T7 | Disconnect, outstanding accepted I/O, shutdown and actual process restart preserve ownership and settlement through reached boundaries. |
| T8.live | Same engine/PID/listener and coherent schema/query activation, including parked requests, native tails and cleanup/shutdown races; unaffected graph progress; submission-only CLI, writer exclusion, Azure refusal and crash safety. T8.history/retention are deferred outside scope. |
| T9 | Concurrent new submission refuses while one revision is pending, including across restart. Settled partial/inactive D1 permits corrective D2; stale-base submission refuses without effects; original lookup never replays. |
| T10 | Declared bounds and completion/status reserves hold under saturation, slow/abandoned consumers and repeated failed deployments; reconcile counters with independent allocation/lifetime evidence. |
| T11 | Wide-feed progress, explicit ingestion-embedding diagnostics and bounded authorized status; unavailable 503 versus unknown 404, correct disclosure, liveness/readiness, bounded transient startup retries and permanent policy refusals. |

Reuse B1–B6 for publication/history cost, mixed serving load, deployment pause and
candidate residency, maintenance interference, failure settlement, and slow/wide
output respectively. B1 varies schema source/IR bytes independently of history;
B5 measures query error/early-completion cancellation latency, including unchecked
CPU work and queued blocking jobs. No bounded cancellation latency is established
by current worker joins. B3's independent historical-overlap experiment is deferred.
Report build/backend/workload identity, offered/admitted/completed/refused/unknown
work, latency distributions, bytes/requests and memory. Timing belongs to the
benchmark harness, not CI correctness thresholds. GQT `--measure` now includes
Lance and wrapped control-store requests; refresh older count baselines and retain
its documented coverage limits. Neither that accounting nor engine-DST scheduling
establishes server process, transport, retained-I/O or performance behavior.
Missing services, zero selected cases and unreached faults do not pass qualification.

### Initial E1 qualification: not yet qualified

The 2026-10-02 probes against main `7fe1789a` and pinned Lance 11.0.0 establish:

- `stored_queries::parked_stored_invocation_requires_a_serving_transition_barrier`
  parks a real HTTP body after routing. Raw apply before releasing it mixes the
  old query with the new contract and fails; finishing it first, then applying
  and replacing the query binding on the same engine succeeds. This isolates
  the binding requirement; it does not implement same-listener activation.
- The three read/CPU guards in `lance_surface_guards.rs` observe independent
  retained inputs, actual late local reads and separate standard-scheduler read
  budgets after callers disappear. These read-only tails do not show graph
  corruption, but their lifetime is outside current aggregate ownership. An
  object-store wrapper could own GET payloads; it cannot alone own subsequent
  native CPU/decode work. The narrower E1 resource gate therefore remains open.
- `manifest_contract_history_curve` varies 16 KiB/1 MiB source, 4/512 enum
  values and 1/16 update checkpoints on main and a branch with four live person
  rows. A local debug run with 16 updates and 15,917-byte IR retained about
  0.98 MB/21.62 MB of manifest files for those source sizes. This is physical
  storage evidence, not RSS, a supported limit or a performance claim.

Online apply and ledger v2 remain unavailable. Exact engine schema intents and
receipts are implemented independently. Durable ledger execution and crash
reconciliation can be implemented next under the protocol gates above; enabling
online activation additionally requires owned, bounded native read/CPU lifetimes
and coherent serving-view qualification.

## Rollout

| Increment | Deliverable | Shipping gate |
|---|---|---|
| A | A1 v0.12 HTTP admission; A2 own-publication merge receipts; A3 CLI outcomes | A1 follows [its accepted decision](2026-09-30-v012-http-admission.md); A2 is qualified under [Exact merge receipts](2026-09-30-exact-merge-receipts.md), including T1/T2; A3's initial typed-429 and qualified-HTTP-412 contract is qualified under [Owned server operations](2026-09-30-owned-server-operations.md) |
| B | Owned writes, read/stream accounting, drain and shared shutdown | [Owned server operations](2026-09-30-owned-server-operations.md) qualifies the task/body foundation. [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md) is accepted and partially implements query-child ownership and named aggregate write limits. Native settlement, completion reserves and runtime reuse remain unqualified under its T6/T10 gates. |
| C | Remaining initialization/native-control completion, owned maintenance and bounded transient startup retry | Atomic schema publication is landed; T4/T6/T7/T11 retain same-process progress and protected reclamation |
| D | Aggregate budgets, feed progress and embedding diagnostics | T10–T11 and workload qualification |
| E1 | Same-engine schema/query activation with one outstanding deployment | Qualified E1 transition proof, relevant B/C/D ownership and bounds, T8.live/T9 and durable ledger/crash gates; generic reuse still requires full B; Azure separately gated |

A and focused C/D repairs may proceed alongside B. E1's prepared schema
publication interface is implemented independently: exact authority and intent,
own-publication receipts, source-bound no-ops, contract-only publications and
read-only committed-result reconciliation. Missing evidence remains unknown;
terminal non-publication additionally needs protected evidence and stopped or
fenced prior work. The [ledger protocol](#proposed-ledger-v2) consumes that
interface; neither slice enables activation before its transition qualification.
Existing-cluster rollout also requires qualified offline v13 conversion.
E1 does not wait for deferred
historical serving or data idempotency. Implement slices against the v0.12 contract
and update their actual status, OpenAPI and user/developer documentation together.
Acceptance or merge supplies no product qualification beyond the recorded evidence.

## Unresolved questions

Before implementing a durable encoding or enabling an affected increment, its owners must specify and qualify:

1. CLI/server: any broader retry allowance needs separate whole-command proof.
   A3 and the B foundation are qualified under
   [Owned server operations](2026-09-30-owned-server-operations.md); A1 admission
   and A2 exact receipt contracts and evidence remain with their accepted owners.
2. Cluster/server: qualify the proposed ledger v2 encoding, including integration
   of engine publication intents and exact receipts, bounded evidence lookup, observation
   interval, result retention, migration and stopped-writer handoff. The existing
   digest-based sweep cannot establish an online deployment's exact outcome.
3. Engine/server: qualify the native settlement mechanism and measured
   admission/completion profile in [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md)
   before exposing a reuse capability. Separately prove E1's same-engine
   transition, including request binding, interference and retained native work;
   otherwise that transition remains unsupported.
4. Azure: lease-preserving online submission and backend qualification.
5. Storage/cluster: qualify data-preserving offline conversion of existing
   cluster roots to v13 before rollout, under the storage-upgrade owner.

## Decision log

- 2026-10-03: The maintainer approved finishing the foundation review and then
  implementing durable deployment execution and crash recovery. Acceptance
  boundary, Scope, Operation ownership, Availability, Invariants, Rollout and
  Unresolved questions replace the draft-only disposition with an accepted
  architecture and explicit implementation gates. This accepts the reviewed
  ownership, availability and prepared-schema contracts; it does not admit the
  incomplete v2 encoding or claim terminal-absence, retention, native-lifetime,
  migration or Azure qualification. Earlier draft dispositions remain history.

- 2026-10-03: Rebased on main `b0bfaef8`. Scope replaces the obsolete
  destructive-schema approval requirement with current migration refusals and
  declaration-drop semantics. Prepared intents carry no removed schema-apply
  option; publication, exact-result and online-activation gates are unchanged.

- 2026-10-03: Implemented the engine prepared-schema interface, exact publication
  receipts, source-only contract publication and conservative read-only
  reconciliation. Online deployment, ledger intent persistence, protected
  evidence and terminal non-publication remain unqualified; updated Online
  deployment, Initial E1 qualification, Rollout and Unresolved questions.

- 2026-10-02: Reproduced source-only apply's false convergence and identified
  prepared schema publication as an independent E1 prerequisite. Native read
  ownership proceeds under the engine settlement decision; ledger v2 and online
  activation remain disabled until their own gates pass.

- 2026-10-02: Implemented the availability foundation: actual ready/blocked
  startup entries own inventory, sanitized status and authorized 503 disclosure.
  Readiness reports aggregate availability without graph identifiers. Loading,
  deploying, startup retries and online activation remain unimplemented.

- 2026-10-02: Operation ownership replaces request-owned reads with bounded
  server-owned handler execution and MCP tool execution after caller loss.
  Pending results retain observation capacity; read failures do not acquire
  write-uncertainty semantics. The focused engine decision covers panic joins.
  Native lifetime and E1 qualification remain open.

- 2026-10-02: Initial E1 qualification added deterministic parked-request and
  native read/CPU lifetime probes plus an independent source/IR/history
  instrument. Serving views now explicitly covers HTTP and MCP atomic
  admission, ownership after cancellation and fresh epochs on resumption.
  Online deployment specifies proposed ledger v2 fields, authorization and
  exact engine intent/receipt requirements; Unresolved item 2 names the
  remaining protocol gates. Qualification records the measured no-go without
  enabling online apply or changing accepted generic-settlement guarantees.

- 2026-10-02: Availability and Qualification make the existing embedding-
  limitation requirement testable without changing nullable-load success or
  permitting automatic embedding. The testing owner restores exact status
  assertions, merge-width dimensions, multi-graph partial-result evidence and
  candidate-expiry races; these qualify existing obligations, not new capabilities.

- 2026-10-02: Rebased on main `7fe1789a`. Scope, Authority and Online deployment
  replace "Schema publication may still precede contract installation", the
  unfinished-installation authority row, the second-opener staging rationale
  and "finish any published schema contract" with atomic manifest publication.
  Serving views replaces the combined writable-open/refresh prohibition with
  their distinct durable and in-memory effects. Operation ownership, Historical
  reads, Alternatives and Rollout E1 replace the unconditional full-B gate with
  a proposed, separately qualified same-engine transition; generic reuse retains
  full B. Compatibility adds the existing-cluster v13 conversion gate. Resource
  bounds, supervision and qualification add schema-size/cancellation evidence,
  permanent Blob-policy refusal and current measurement coverage. The embedded
  Exit 4 row removes obsolete open-time completion reasoning without broadening
  retry permission. No new product support or RFC acceptance is claimed.

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
