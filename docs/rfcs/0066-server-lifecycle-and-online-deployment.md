---
rfc: "0066"
title: "Server lifecycle and online deployment"
track: maintainer
status: draft
implementation: not-started
authors:
  - OmniGraph maintainers
created: 2026-09-10
updated: 2026-09-10
discussion: https://github.com/ModernRelay/omnigraph/pull/697
supersedes: []
superseded_by: []
blocked_on:
  - Crash and restart proof for deployment publication, finalization and activation
  - Effect-free candidate construction or qualified engine reuse for each supported change class
  - Versioned deployment request, result and active-witness compatibility contract
---

# RFC 0066: Server lifecycle and online deployment

## Summary

Schema and serving-configuration changes must become active without
restarting the server. The running server owns admitted writes, maintenance
and deployment execution; the engine remains responsible for atomic graph
publication and recovery. Deployment prepares and authorizes one exact
change, drains affected operations, applies through the existing writer,
finalizes its exact durable result, activates a coherent serving view and
verifies that activation. The process and listener remain alive. Unaffected
graphs continue serving.

This RFC defines the server upgrade as independently deliverable changes:
operation ownership, failure finalization, bounded resources, graph
availability and online deployment. It also defines the requirements for
recoverable request outcomes. Online deployment does not reset or recreate
a graph as a migration mechanism, hide multiple graph commits inside one
mutation, or silently fall back to a process restart. Affected graphs may
pause while their operations drain and their migration completes; this is
not a zero-downtime migration promise.

## Motivation

The server currently captures its stored-query registry and other serving
configuration at boot. Engine reads can capture newer accepted schema while
stored queries retain their earlier source. A schema/query update can
therefore leave a long-lived server with incompatible components even when
the engine can use the updated graph without reopening. Restart reconstructs
the components, but it is an unnecessarily broad normal deployment action.

Request lifetime is a separate reliability problem. HTTP handlers directly
await effectful engine operations. Dropping a handler may abandon work after
durable table effects but before graph publication. Keeping that work alive
addresses transport cancellation, but not ordinary engine failures:
[#694](https://github.com/ModernRelay/omnigraph/issues/694) reports a branch
merge returning a resource error and blocking subsequent writes until
restart. The current post-arm merge error path retains recovery ownership,
and live recovery deliberately defers some BranchMerge states.

The same missing lifecycle boundaries affect maintenance, recovery after
transient startup failures, status and resource accounting. A collection of
reload hooks, retry loops and longer HTTP timeouts would leave those
boundaries implicit. The server needs one operation owner and one coherent
activation path around the existing storage protocol.

Current code already provides important parts of the contract: staged
multi-table graph publication, recovery intents, graph-commit preconditions,
mutation/load receipts, boot readiness and bounded shutdown. This proposal
extends those owners. It does not assume every historical incident still
has its original mechanism on current source.

## User and operational behavior

### Online changes

A successful supported schema/configuration deployment leaves the same
server process and listening socket running. Subsequent admissions use the
new accepted schema and matching query/configuration bindings. No stale
stored query executes against a newly activated incompatible schema.
Unchanged graphs remain available throughout a scoped deployment.

The target includes schema, stored queries and runtime policy/provider/trust
configuration. Delivery begins with a fixed graph inventory and fixed
roots, policy, credentials, signed trust, provider and Blob-policy bindings.
Schema and stored-query changes are the first supported class. Later classes
use the same lifecycle after their authorization, secret resolution and compatibility
proofs pass. Initial scope restrictions are validated before effects and
reported explicitly, rather than silently restarting the process.

Existing schema migration rules still apply, including branch restrictions,
supported backfills, identity-preserving renames and destructive-change
authorization. Deployment does not make an unsupported migration valid.
Binary replacement, physical-root relocation and storage-format upgrades
are separate compatibility operations.

### Request outcomes

- An admitted write executes once to an engine-terminal result, contained
  panic or process shutdown cutoff. Client disconnect and caller wait
  timeout change delivery, not execution ownership.
- Reads cancel cooperatively. Their resource and admission permits remain
  held until response bodies and scoped producers actually settle.
- When an operation publishes, success identifies its own publication. A
  no-op explicitly has no new graph commit. A committed merge followed by
  failed optional source deletion reports that compound outcome accurately.
- An uncertain result preserves available operation identity and recovery
  disposition. Transport loss may leave the caller with neither until
  recoverable submission and lookup ship. It is never presented as proof
  that the write failed or as permission to submit it again.
- Temporarily unavailable graphs remain known and have distinct read/write
  availability. Protected diagnostics are available through authorized
  status, with finite retry guidance where progress is possible.

The first operation-ownership increment does not make an interrupted client
able to retrieve a result after process loss. Durable submission identity
and lookup have additional requirements below; they are necessary to close
[#513](https://github.com/ModernRelay/omnigraph/issues/513).

### Administrative access

Deployment uses an explicit authenticated operator interface to the running
server. A local transport is not authorization. The request binds an exact
configuration input, expected base and initiating principal; policy checks
cover every required effect before execution. Ordinary data credentials do
not acquire deployment privileges merely because a new endpoint exists.
The schema/admin exclusions of the existing
[signed-data credential profile](0053-offline-data-token-verification.md)
remain enforced; deployment requires its own explicit authorization.

Existing ownership of cluster-managed schemas remains enforced. A generic
schema mutation route must not become a second writer of the configuration
ledger. The online path invokes the same configuration validation, planning,
apply and graph publication implementation as existing supported tools.

## Design

### Ownership and relationship to existing RFCs

| Responsibility | Owner |
|---|---|
| Graph visibility, accepted schema and recovery evidence | Engine and existing storage protocol |
| Request admission, operation lifetime and shutdown | Server, following [RFC 0035](0035-served-operation-ownership.md) |
| Recovery classification and authority | Engine, following [RFC 0034](0034-durable-recovery-authority.md) |
| Coherent serving generations and graph supervision | Server, extending [RFC 0036](0036-atomic-runtime-activation.md) |
| Configuration validation, planning and durable applied state | Existing `omnigraph-cluster` implementation |
| Online deployment sequencing and its server-visible contract | This RFC |
| Exact data submission identity and recoverable outcomes | The separately gated outcome contract below |

RFCs 0034–0036 are drafts, not available implementation APIs. Their proposed
guards and effect-free constructors must be implemented and qualified
before a consumer relies on them. The ownership core can ship independently
of full supervision.

This RFC proposes two bounded extensions to RFC 0036: configuration
deployment becomes in scope, and a fully drained engine may be reused for
the initial schema/query class under the proof below. RFC 0036's existing
fresh-engine rule still applies elsewhere. This is not its existing
unchanged-schema reuse exception. Acceptance must reconcile the owning
draft's affected rules; it does not supersede that whole RFC or relax
recovery authority.

[RFC 0065](0065-isolated-branch-merge-publication.md) separately proposes
isolated merge preparation to keep failed preparation off accepted target
tables. It remains a draft with storage and compatibility gates. Server
operation ownership and exact outcome handling apply under either merge
representation; narrow failure-finalization fixes can ship independently
of that proposal.

### One serving view per admission

The published serving view contains graph identities and engine handles,
accepted schema/catalog bindings, stored-query registries, policy and
authentication bindings, providers, Blob policy, derived authentication
requirements and active revision evidence. Immutable unchanged components
may be shared, but their association belongs to one view.

A request captures this view before authentication-dependent routing,
authorization and query selection. Reads retain their admission permit and
generation through body and scoped-producer settlement. Writes retain them
through engine-terminal settlement; result delivery does not prolong the
write lane, and retains its own bounded buffer accounting. A request cannot
authorize against one policy, select a query from another registry and then
reload a third engine generation. Authoritative engine write-policy and
attempt revalidation remain required; admission does not replace them.

Current authentication requirements depend partly on graph policies while
other authentication fields live separately in `AppState`. A future
configuration update must publish those fields and derived requirements
together. The fixed-binding first slice verifies their effective values,
not just unchanged filenames.

### Operation admission and lifetime

Each graph generation has independently closeable read and write lanes.
Acquire, task registration and ownership transfer form one synchronous
handoff with no cancellation gap. An owned operation retains its inputs,
principal, exact serving view, workload reservation and permits until actual
settlement. The result receiver owns none of its execution lifetime.

Cover every effectful route: mutations, stored mutations, loads, branch
create/delete/merge, GQ branch statements, deployment and maintenance.
Deployment/recovery tasks have their own tracked lifetime and must not hold
a data-lane permit that they then wait to drain.

Closing an admission epoch is irreversible. Successful reactivation receives
a new token even if it reuses the same engine. A drain timeout returns
remaining work and leaves the selected lanes closed; it does not prove
quiescence, cancel a mutator, start recovery or permit replacement.

All tasks, connections, streams and transitions participate in one absolute
shutdown deadline. No participant restarts the clock. Cutoff reports
unfinished/unknown work and exits without claiming graceful completion.

### Online deployment sequence

```mermaid
flowchart LR
    A[Prepare exact input] --> B[Authorize and revalidate]
    B --> C[Close and drain affected lanes]
    C --> D[Apply in the running writer]
    D --> E[Finalize exact durable result]
    E --> F[Activate coherent view and new epoch]
    F --> G[Verify and return active witness]
```

1. Capture one immutable, bounded configuration input. Validate schemas,
   queries, references, supported change class and expected identities before
   graph effects. Capture semantic and exact-input identity; never reread a
   changing source midway through the operation.
2. Use the existing full-plan semantics and cluster serialization. Revalidate
   the expected applied state, configuration and graph/schema authority.
   Authorize the complete effect set using current effective policy. A
   candidate policy cannot authorize its own installation.
3. Derive affected graphs and lane requirements from that full plan. There
   is no caller-supplied target list that bypasses validation of the rest of
   the configuration. The initial schema-changing path drains all observers,
   writes and producers of each affected graph before schema effects.
4. Verify the permitted recovery state under authority. Existing apply may
   sweep recovery before diffing; the desired diff alone therefore does not
   bound its effects. The first healthy path refuses pending recovery rather
   than touching undrained graphs. A later path must include every recovery
   participant in its verified scope.
5. Execute the existing apply in the running designated writer. Preserve its
   graph recovery intents, schema gates, exact identity checks and durable
   applied-state publication. Do not launch another writable process beside
   the serving process.
6. Obtain the exact durable result from that publication path. It binds the
   input and base, applied-state revision/digest, complete per-resource
   outcomes, accepted schema identities and the operation's own graph
   publications. Finalization must not sample a later current ledger or HEAD.
7. Validate the replacement serving view against that exact achieved state.
   Hold serialization through activation, or use an equivalent exact
   authority guard that excludes a superseding deployment. Publish the new
   view and fresh admission cells atomically with respect to shutdown.
8. Return an active witness for this deployment. It distinguishes requested,
   durably applied and active state. Applying data successfully and failing
   activation are different outcomes.

One atomic runtime publication does not turn a multi-graph deployment into a
cross-graph transaction. Existing partial-convergence semantics remain.
An execution failure may leave different resources at different durable
boundaries. A coherent achieved projection may activate under its own
identity after verification; it must not claim the requested revision fully
active. Unresolved affected graphs remain closed. Unaffected graphs continue
using their verified bindings.

### Engine reuse and candidate construction

The initial schema/query deployment may reuse the current engine only when:

- Every affected read, write, maintenance operation, response body and scoped
  producer has settled; no old admission can reach the engine again.
- Graph identity/root and policy, provider, credential/trust and Blob-policy
  bindings are unchanged and verified.
- Accepted schema/catalog and complete publication authority have been
  verified after apply, including rename and drop/re-add lifetimes.
- Every retained state-dependent cache is keyed by exact accepted identity
  or has a proved invalidation/refresh path. No late producer can populate a
  cache using earlier authority.
- The new runtime bindings and admission epoch are fresh even though the
  engine allocation is retained.

Otherwise candidate construction requires a fresh effect-free factory under
the engine's verified authority. Current ordinary writable open can perform
full recovery, and `refresh()` can perform roll-forward/schema promotion.
Neither is an effect-free candidate-construction API. A pointer swap or
retained `Arc` alone does not preserve an old schema view.

### Failure, finalization and restart safety

The engine classifies effects and recovery; server lifecycle consumes that
classification. Separate substrate condition, whole-operation outcome,
recovery disposition and caller action. A transient storage error says
nothing by itself about whether an earlier effect occurred.

| Boundary or result | Required behavior |
|---|---|
| Invalid or unauthorized input before admission | Refuse with no graph effects |
| Drain pending | Keep ownership and selected lanes closed; report pending work |
| Proved pre-effect failure with unchanged authority | Retain or re-establish the old view in a fresh admission epoch |
| Complete exact durable result | Finalize and activate that result, without replay |
| Partial or unknown effect | Keep affected admission closed and expose the engine recovery disposition |
| Applied result but failed activation | Preserve the applied result; rebuild/activate from its exact authority |
| Lost response | Execution/result truth is unchanged; never infer failure or resubmit automatically |
| Process crash | Reconcile durable deployment and graph evidence before effectful open or active readiness |

An online deployment needs durable identity and enough publication evidence
to distinguish an unresolved transition from a completed result after a
process crash. Startup must not boot an incompatible registry, run unauthorized
recovery or report the requested deployment active merely because it sees a
newer ledger. An in-memory drain flag is insufficient.

This RFC requires that startup gate but does not pretend an existing durable
deployment-result encoding supplies it. Its representation must extend the
existing applied-state/recovery authority with exact operation binding and
retention rules. It must not become a second source of graph truth, custom
WAL or replay queue. Encoding, publication ordering, finalization and
downgrade refusal are acceptance gates for online deployment.

### Failed writes and recovery progress

[#694](https://github.com/ModernRelay/omnigraph/issues/694) is an explicit
failure-finalization requirement. Keeping a merge alive after disconnect
does not fix a merge that returns a resource error. The original writer must
classify and finalize its attempt under the same exact ownership proof used
by recovery. Proved effect-free intent may retire; complete exactly
confirmed effects may roll forward. First-touch refs are physical effects
even without data-HEAD movement. Unchanged graph-visible rows do not prove
absence of unpublished effects.

Current-owner finalization does not create general exclusive-compensation
authority. Partial effects requiring stronger authority remain explicitly
blocked until the engine's supported recovery procedure proves exclusion
and quiescence. No background loop may obtain that authority merely by
reopening the graph or waiting out a timeout. The routine failed-attempt
cases must recover write progress without a process restart.

For [#601](https://github.com/ModernRelay/omnigraph/issues/601), carry the
discovered artifact URI and object version through fresh reread,
classification and any authorized retirement. Detect path/body disagreement;
do not reconstruct a different deletion target or delete surprising names
automatically.

For [#602](https://github.com/ModernRelay/omnigraph/issues/602), retain the
immutable original manifest outcome and all intended/confirmed participant
witnesses. Reconstruct confirmation only when exact transaction and
incarnation evidence establish the complete result. A matching commit ID or
numeric HEAD is insufficient. Diagnostics alone do not close either
availability defect; supported resolution and subsequent write progress are
required.

### Maintenance and graph availability

Optimize, rebuild and cleanup use the same designated writer, admission and
operation ownership. A maintenance batch publishes one complete graph result
under the existing protocol. An independent optimizer process, even in the
same container, is another writer and cannot rely on server-local locks.

Cleanup additionally needs reader lifetime and retention proofs covering
snapshots, native refs and recovery pins. Missing index coverage is derived
performance state and must not make ordinary graph results incomplete.
Index status is read-only and must not trigger maintenance.

Keep every configured graph represented while opening, serving, draining,
recovering or blocked. Reads and writes can have different availability when
the engine proves an accepted read snapshot remains safe. Supervision is
bounded and singleflight per graph; notifications coalesce as hints without
resetting deadlines or retry budgets. A transient failure is retried only
when its effect/authority disposition permits another recovery attempt.
Unverifiable state remains blocked with an actionable diagnostic.

Reduce failure scope only when durable evidence proves independence. A
branch-local condition may permit other branches to proceed; a global schema
or unattributable recovery condition must not be ignored for availability.

### Exact outcomes and CLI retry guidance

Thread the engine's own publication result through every merge surface.
Reading latest target history after a merge can return another writer's
commit and is not a receipt. Configuration/query-only activation may produce
a new active revision without a new graph commit; no-op results preserve
that distinction.

For [#466](https://github.com/ModernRelay/omnigraph/issues/466), preserve
structured error details through ordinary and streamed HTTP/CLI paths and
centralize data-command outcome classification:

| Evidence | Caller action |
|---|---|
| Proved pre-effect transient refusal of the entire command | A bounded fresh attempt may be allowed |
| Failed caller precondition | Re-read and decide; do not discard the condition |
| Fixed resource or logical conflict | Correct the request or limiting condition |
| Recovery making bounded progress | Observe/back off according to the typed disposition |
| Recovery blocked | Follow the stated supported recovery action |
| Unknown earlier outcome | Reconcile the original operation; do not blindly resubmit |

A low-level retryable failure does not make a compound command replayable.
Direct reopen may complete earlier work. HTTP 409/503, topology and generic
`RecoveryRequired` are not retry authorization. Preserve existing CLI exit
contracts, including explicit precondition failures; new data-command exits
must be documented and qualified without globally remapping other command
families. The first increment adds classification, not an automatic retry
loop.

Durable data submission and lookup require a separately reviewable protocol:
the caller knows its key before sending; its scope includes the stable
authenticated principal and operation kind; key lookup binds the original
graph/branch incarnations before selecting current names; semantic input and
preconditions are fingerprinted; concurrent duplicates attach to one
execution; changed input under the same key conflicts. Current authorization
protects lookup. Committed, no-op, refused and compound outcomes have explicit
crash and finite-retention semantics. An expired/missing record is not proof
of no effects. No independent result database may become graph commit truth.
In-memory task registration alone cannot acknowledge durable acceptance.

### Resource bounds and feed progress

Bound aggregate ingress before collecting or parsing request bodies. Include
process-wide and per-actor concurrency, retained inputs, queue capacity and
actor-record cardinality. Account for decoded Arrow/vector/Blob buffers,
staging, merge validation, hydration and output until their actual lifetime
ends. Existing body/chunk limits and estimated request bytes do not measure
total engine work.

Separate input deadlines, caller response-wait deadlines, read-execution
budgets and the shared shutdown deadline. Refuse excessive work before
effects where the engine can prove that boundary. Budget exhaustion after
effects must preserve an exact recovery outcome. Never split one requested
atomic mutation into unannounced incremental commits.

Measure merge work against target size/history, delta size, selected columns
and row width. Small upload size does not bound merge scan memory. The
reported `ordered_scan_input_batch_bytes` failure in #694 needs its own
fixture and cost evidence; a feed failure using the same limit name does not
establish the merge's mechanism. Raising the cap is not a recovery fix.

For changes/feed and ordered export, use bounded key/address-first ordering
where applicable and hydrate payloads from exact pinned datasets. Preserve
order, cardinality and fragment/version identity. A supported wide row must
not permanently block the cursor. Keep the solo-oversized-change rule and
test Blob/encoding expansion and oversized identifiers. Do not skip a bad
commit or lower ingress limits to hide already accepted history.

### Status and embedding diagnostics

Preserve `/healthz` as process liveness and the `/readyz` fields
`booted_serving_digest`, `state_revision` and `state_cas` as boot facts. Add
active-deployment evidence and dynamic read/write availability without
relabeling those fields. Readiness, current status and active graph counts
derive from the graph lifecycle. A successful active witness binds
the exact achieved state, accepted schemas and fresh generation; it is not
manufactured from current HEAD.

The aggregate readiness contract is:

| Current serving state | `/readyz` |
|---|---|
| At least one graph can serve a supported read or write operation | 200, with explicit partial/read/write counts if other graphs are unavailable |
| Valid applied empty graph inventory | 200, with zero active graphs |
| Nonempty inventory with no available graph, including initial loading | 503 |
| Process shutdown has begun | 503 regardless of remaining graph availability |

Thus one graph's drain does not make healthy peers globally unready. HTTP
200 alone never proves a deployment completed; callers compare its exact
active witness. The changed readiness behavior requires versioned
compatibility qualification alongside its new fields.

Keep graph identifiers and protected recovery detail out of unauthenticated
readiness. Authorized inventory/status must preserve existing credential
scope and disclosure rules. A broader identity/discovery change requires
its own explicit compatibility contract.

Ordinary `@embed` currently declares metadata; the server does not populate
vectors automatically. Add useful missing/supplied embedding diagnostics
for the touched input. Automatic embedding population remains the separate
value-semantics proposal in [RFC 0015](0015-ingest-embeddings.md), including
provider provenance, stale values, supplied-vector precedence and update
races. Operation ownership alone does not deliver it.

## Invariants

The [architectural invariants](../dev/invariants.md) remain binding:

- Invariants 1–5: Lance retains per-table storage ownership; one graph
  publication contains all participants; attempts use coherent authority;
  independently durable effects remain recoverable.
- Invariants 6–9: schema/incarnation identity survives supported renames;
  caches and indexes remain derived; failures stay typed and loud; query
  semantics remain in typed compiler structures.
- Invariant 10: authentication establishes the principal, and authoritative
  engine checks still enforce permission for every mutating entry.
- Invariants 11–13: lifetime, memory, retries and queues are bounded;
  runtime/status are derived from authoritative state; tests qualify the
  boundary whose promise changes.

No custom WAL, transaction manager, manifest-derived job queue, synchronous
vector/FTS rebuild on content writes, public raw Lance writer, string-built
query semantics or second commit authority is introduced. Admission locks
are explicitly process-local, not distributed fencing. The existing
one-mutation-process support boundary remains; this RFC adds neither writer
takeover nor read-only replica enforcement.

## Compatibility and reversibility

Normal data requests retain their APIs and graph publication semantics.
Existing graph-commit preconditions are not idempotency keys. New deployment
requests, exact results and active witnesses need versioned capability
negotiation and unknown-field/error compatibility tests. A server that cannot
perform a requested online change refuses before effects; it must not
silently execute a restart-based path.

The ownership/resource increments need no new graph format. Durable
deployment/outcome evidence may require versioned extensions to existing
authority; their encoding and migration are unresolved acceptance work.
Older binaries must refuse incompatible evidence before effectful recovery.
Never remove schema fields, graph rows or recovery artifacts to make a
downgrade appear compatible.

Configuration rollback is a newly validated deployment against current
accepted state. It is not reuse of a stale registry or an assumption that
data effects were undone. Runtime activation cannot conceal a partial apply.
Local and object-store deployments use the same protocol; Azure retains its
existing admission-wrapper and qualification boundary.

## Alternatives

| Alternative | Why it does not meet the target |
|---|---|
| Restart after every schema/configuration change | Disrupts unrelated graphs and makes process replacement the normal deployment mechanism |
| Refresh engines on request or after errors | Leaves startup query/policy bindings stale and can perform recovery effects |
| Watch applied state and swap a registry | Notices schema effects too late and does not own drain, authority or crash recovery |
| Always reopen engines | Current open is effectful and discards reusable state without proving coherent activation |
| Pause serving and run an independent writable apply process | Introduces a writer-transfer and restart race that process-local admission cannot fence |
| Detach writes without accounting or durable evidence | Avoids one cancellation path but permits unbounded work and does not resolve lost outcomes |
| Replay transient errors or every `RecoveryRequired` | May repeat already committed or partially completed work |
| Require a complete supervisor/job system first | Delays independently useful ownership, receipt, recovery and feed fixes |

## Evidence and tests

### Investigation evidence

The source audit covered `5c4ca700d9dc5470bac4fd8d6a7f534699a2b137` and checked
the relevant paths through `fe8ae062765745060b8212d0f9bf550cbaedf665`.
The dependency is Lance 11.0.0, internal manifest schema v7 and recovery v9;
older draft references to Lance 10/v6 are not implementation assumptions.

The focused server baseline passed 362 parent tests. The engine selection
reported 351 passes, of which three environment-gated cases returned early;
348 engine cases actually executed. Four selected DST scenarios passed, but
two preserve known stale/misnamed-sidecar limitations rather than proving
healthy recovery. Those exceptions must become progress assertions when
their fixes land.

Local schema characterization showed an existing engine using new accepted
schema while its startup stored-query registry stayed stale. A replacement
registry could reuse that engine after completed apply. This demonstrates
feasibility, not unchanged-listener, concurrent activation or restart safety.
The backend-specific traversal report
[#494](https://github.com/ModernRelay/omnigraph/issues/494) retains separate
shared-memory and configured object-store qualification.

A local diagnostic accepted a 40 MiB payload and a later small-row update.
Unpaged diff succeeded while paged changes and repeated polls from the same
cursor failed with `ordered_scan_input_batch_bytes`, actual 41,943,850 bytes
against a 39,321,600-byte limit. The diagnostic exposed the remaining feed
gap; it did not reproduce #694's merge failure. The exact merge fixture,
real transport cancellation and online activation remain required evidence.

The investigation read the complete relevant Lance storage, transactions,
schema evolution, branches/cleanup, scanner/Blob, index and observability
pages and inspected pinned Lance 11 source. Relevant constraints include
[transaction ownership](https://lance.org/format/table/transaction/),
[schema evolution](https://lance.org/guide/data_evolution/),
[versioned reads and maintenance](https://lance.org/guide/read_and_write/),
[object-store behavior](https://lance.org/guide/object_store/) and
[Blob access](https://lance.org/guide/blob/). They support exact version-pinned
reads and explicit coordination; they do not make caller cancellation an
abort or require a process restart for every schema change.

### Acceptance matrix

Extend the owners in [the testing map](../dev/testing.md). Logical row/error
behavior belongs in GQT; mechanism, concurrency, scale and process lifetime
use their existing Rust owners.

| Contract | Required evidence | Existing owners |
|---|---|---|
| Owned writes and reads | Actual HTTP/1 disconnect, HTTP/2 reset, wait timeout, dropped receiver and body abandonment at each durable boundary; permits survive until settlement | Server `data_routes`, `stored_queries`, unit/process fixtures |
| Online schema/query changes | Same PID/listener; new query/schema together; unaffected graph remains usable; invalid query refuses before effects | Server `multi_graph`, `schema_routes`, `stored_queries`; cluster apply tests |
| Engine reuse | Warm-cache rename, drop/re-add, new fields/types, no late producers, fresh admission token; shared-memory and configured backend coverage | Engine `schema_apply`, `failpoints`, `warm_read_cost`; DST |
| Deployment crash safety | Crash before/after each graph effect, applied-state publication, finalization, activation and delivery; pending transition cannot boot ready incorrectly | Cluster failpoints; server boot/process fixtures |
| Failed merge progress (#694) | Resource failure and disconnect separately; exact physical/ref census; after verified resolution a no-op write, ordinary load and branch creation succeed in the same process | Merge/GQT owners, engine `failpoints`/`recovery`, server route/process owners |
| Multi-table maintenance | Observers see one coherent published snapshot; exact participant recovery after each effect; cleanup respects live readers/refs | Engine `maintenance`, `recovery`, `failpoints` |
| Recovery artifacts (#601/#602) | Misnamed/stale artifacts resolve under exact proof; subsequent writes progress; foreign/ambiguous artifacts are not destroyed | Recovery unit/integration owners; DST sidecar scenarios |
| Exact receipts and retry (#466) | Competing later commit cannot change receipt; merge/delete partial outcome; structured and streamed errors; no extra submissions on unknown results | Server `data_routes`; CLI `cli_data`, `parity_matrix` |
| Graph status | Loading, empty configuration, partial read/write availability, peer failure, retry exhaustion, shutdown; boot/active distinction and disclosure | Server `boot_settings`, `multi_graph`, `auth_policy`, `openapi` |
| Bounds and feed | Saturated admission/queues, actor cardinality, retained buffers, wide payload/key/Blob, encoding expansion and repeated cursor progress | Engine `changes`, `changes_cost`; server route/resource tests |
| Durable data outcomes | Same-key concurrency, changed payload, lost first response, process crash, graph/branch recreation, no-op, expiry and authorization changes | Outcome protocol tests plus server/CLI integration owners when that slice is designed |

The online deployment release gate requires zero implicit restarts in every
supported-change case and zero mixed schema/query activations. Every failure
case must end with an exact result or explicit unresolved ownership; no
false success, blind replay or silent partial result is allowed. Record
measured drain/migration/memory costs before setting availability promises.

Before implementation merges, run the canonical workspace feature graph,
both Clippy graphs, formatting, OpenAPI and repository checks, plus configured
backend suites for the changed authority. Local green tests are not live
object-store, transport or deployment qualification.

## Rollout

| Increment | Deliverable | Exit gate |
|---|---|---|
| A | Exact merge receipts and conservative typed CLI outcomes | Own-publication race and whole-command retry tests |
| B | Server-owned writes, read/stream accounting, close/drain and shared shutdown | Real transport cancellation and lifetime matrix |
| C | #694 failure finalization, #601/#602 resolution, owned maintenance | Recovery proof and same-process write progress; maintenance-specific authority |
| D | Aggregate resource bounds and wide-row feed progress; embedding diagnostics | Accepted history remains readable; limits preserve recovery |
| E | Online schema/query deployment, exact result, active witness and startup gate | Same-process activation and full crash/restart matrix; no implicit restart |
| F | Durable data-operation submission identity and lookup | Accepted encoding/retention contract and duplicate/crash proof |
| G | Broader recovery supervision and runtime policy/provider/trust changes | Coherent authorization/bindings, bounded recovery and safe secret resolution |

A, focused C repairs and D can proceed alongside B. E depends on B and the
healthy-state/publication/activation proofs, not a complete supervisor or F's
general data-operation identity. Later runtime configuration classes reuse
the same deployment lifecycle. Independent bug fixes may ship under their
accepted issues; this proposal must not become a reason to defer them.

Update implementation status as these contracts land, with user/developer
documentation and release notes describing only qualified behavior.

## Unresolved questions

Before accepting the online deployment slice, settle:

1. The versioned operator request/result and capability surface, including
   exact input identity, authorization and active-witness fields.
2. The durable deployment identity, publication/finalization encoding and
   startup reconciliation proof, including safe partial convergence and
   retention. Choose the smallest extension to existing authority.
3. The exact effect-free factory and settled-engine reuse interfaces, and
   the corresponding bounded amendments to RFCs 0034/0036.

General data-operation idempotency has its own acceptance gate and must not
be inferred from deployment identity or process-owned tasks. Automatic
embedding semantics, distributed writer fencing and replicas remain owned
by their separate proposals.

## Decision log

- 2026-09-10: Proposed server-owned execution and online schema/configuration
  deployment as the target. Routine configuration changes must keep the
  server process alive. Added explicit failed-merge finalization, typed
  caller actions, bounded resources and startup/activation evidence gates;
  separated them from durable data submission and broader runtime changes.
