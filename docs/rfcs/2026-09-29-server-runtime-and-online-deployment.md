---
rfc: "2026-09-29-server-runtime-and-online-deployment"
title: "Server runtime and online deployment"
track: maintainer
status: accepted
implementation: in-progress
authors:
  - OmniGraph maintainers
created: 2026-09-29
updated: 2026-10-06
discussion: https://github.com/ModernRelay/omnigraph/pull/799
supersedes:
  - "0034"
  - "0035"
  - "0036"
  - "2026-09-10-server-lifecycle-and-online-deployment"
superseded_by: []
blocked_on:
  - "B: native accepted-I/O settlement, completion-memory/local-I/O reserves and engine-work bounds"
  - "E0: backend-specific offline deployment/recovery qualification beyond the recorded evidence"
  - "E1: broader native resource and backend fault qualification beyond the recorded same-engine transition evidence"
  - "Existing-cluster rollout: qualified data-preserving offline conversion to storage v14"
  - "Azure: adversarial live-provider qualification under the retained admission wrapper"
---

# RFC: Server runtime and online deployment

## Summary

The server owns admitted writes through completion and reports exact outcomes
and graph availability. Ledger v2 is the sole deployment protocol: direct apply
bootstraps a stopped cluster; server-owned apply changes schemas and stored
queries, policy/provider/Blob bindings and graph additions without restarting.
The engine publishes graph contents and their accepted schema together; the
existing cluster ledger owns deployment input and applied results. No new
transaction manager, content-recovery log or job queue is added.

This decision targets **v0.12 only**. Deploy the CLI, server and cluster tools as
one qualified v0.12 build. There is one wire contract, without older-client
adapters, legacy response aliases or a mixed-version support matrix. Version
refusal and data-preserving migration remain required.

One exclusive owner executes a captured deployment: the stopped-cluster CLI or
its running server. Execution location is not a ledger mode. The HTTP CLI
submits immutable input and never opens a graph for writes. The server drains
affected requests while keeping its PID and listener alive; unaffected graphs
continue serving. One deployment may be outstanding. Ledger v1 is readable only
for explicit data-preserving conversion; no v1 executor or fallback remains.

## Acceptance boundary

The maintainer accepted the runtime foundations and durable-deployment direction
on 3 October 2026, with implementation and qualification proceeding separately.
The reviewed foundation implementation is [PR #844](https://github.com/ModernRelay/omnigraph/pull/844).
The operation, availability and prepared-schema contracts below are accepted;
`Unknown` establishes neither absence nor permission to replay.
This amendment accepts one ledger v2 protocol, including exclusive admission,
strict publication intents, graph creation, server-owned activation and no-reset
ledger conversion.
[PR #849](https://github.com/ModernRelay/omnigraph/pull/849) implements explicit
offline v2 conversion and admission; qualification remains limited to the
[recorded evidence](#e0-implementation-evidence).
The online path retains existing engines and their immutable read resources; it
does not claim generic native settlement or runtime disposal/reuse. Existing-
cluster graph-format conversion and provider qualification retain their own
evidence requirements. Accepted design is not evidence of a passing test.

## Scope and baseline

The baseline is main `b0bfaef8` (3 October 2026). Detached table pins and the
accepted schema contract publish together. Live engine reads capture a matching
immutable snapshot/catalog without reopening. Request-independent write owners,
query-worker joins and named aggregate write limits have landed; native-I/O
settlement and comprehensive resource bounds remain unqualified. The registry
captures coherent request bindings and supports atomic activation of a
deployment's affected views. Startup loading remains observable; transient
startup retry is separate work.

The deployment class covers graph creation and physical deletion, schemas,
stored queries, graph and cluster policies, embedding-provider definitions and
bindings, and external-Blob rules. Adoption, missing-root recreation,
schema-drift acceptance and catalog repair are outside ordinary deployment.
Retained graph roots, storage format and credential/trust configuration stay
fixed. Schema apply remains main-only and requires a single live branch.
Unsupported migrations refuse before effects; dropping schema declarations does
not reclaim retained data or require a destructive-schema override. Removing a
graph declaration deletes the whole managed root, including retained history.
The following are outside this RFC:

- independently available historical reads during a schema transition;
- durable data-request idempotency keys and lookup after a lost response;
- live credential/trust replacement, additional serving roles and
  general-purpose recovery scheduling;
- online binary/storage upgrades, automatic embedding and new test-harness protocols.

Those capabilities need separate decisions. The former E2, F and G increments
are no longer implementation commitments here. Narrow retry of transient graph
startup/control failures remains in scope to prevent sticky quarantine. Existing
clusters need a qualified offline v14 conversion before this rollout; its protocol
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
| Direct graph/schema/query deployment | Stop serving, acquire exclusive cluster admission, persist original outcomes and start serving explicitly after settlement. |
| Server-owned graph/schema/query deployment (E1) | Keep PID/listener; affected admissions return 503 while admitted work settles and coherent bindings activate. |
| Invalid online candidate before effects | Preserve previous coherent service; resumption uses a fresh admission epoch and validated authority. |
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

One exclusive admission owner covers the complete canonical cluster root: a
serving process or a direct operation. The existing persisted lock extends
to that lifetime and rechecks the ledger after acquiring it. This serializes
supported entry points; it does not fence arbitrary storage writers. Operators
exclude old binaries, raw/embedded openers and external maintenance for the whole
outstanding deployment, including recovery. A root unable to maintain that
exclusion cannot admit v2. Timeout, lock age or process ID never grants takeover.
Strict numeric schema publication fences only that publication; it does not
settle prior control-store I/O, native controls or reclamation. HTTP submission
runs within the designated serving process and its existing admission; it does
not acquire another root lock or authorize a second writer.

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
queries, immutable authorization/provider bindings and activation witness. Capture it
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

The implemented transition foundation reserves bounded process capacity before
closing a graph, owns HTTP/MCP roots through their final logical users and allows
exact-view resumption or exact achieved schema/query activation under fresh
epochs. Preparation and resumption bind the real process runtime and share
shutdown's synchronous boundary. The batch validates schema and query
replacements together. A runtime-binding replacement shares the existing engine
coordinator, writer gates, schema authority and Lance sessions without reopening
storage; its policy, provider client and Blob admission remain immutable. New
graphs supply validated handles. A completed refusal before effects
can explicitly abort, even after a drain deadline, restoring unchanged views
under fresh epochs. Their gates retain old descendants, so a subsequent
transition still waits for those owners. Expired or abandoned attempts without
that proof stay closed; they do not reset admission budgets.
The unchanged-view scheduling record owns no additional engine or resource permit.
Expiry or ticket abandonment retires only that record, so another ready graph
can transition. The registry retains the closed predecessor and descendants;
bounded inventory limits these views, and stale ticket identity cannot resume them.
This retirement does not dispose a native candidate or release its charges.
Logical request ownership does not by itself prove native settlement.

Candidate validation uses read-only captured state. Current v14 engine `refresh`
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
the exact achieved contract, queries and runtime bindings. Management policy and
graph views activate together. Old admitted views keep their own bindings until
they finish; candidate policy cannot authorize its own deployment.

Any remaining native read tail must be proven unable to publish, reclaim or
change serving bindings. Keep its immutable snapshot, input owners and existing
native scheduler limits until its last use. Its resources cannot be disposed or
reused to fund activation; graph schema publication never reclaims those inputs.
Unclassified tails or uncertain writes refuse the transition; uncertainty retains
the existing process-containment rule. This narrower proof must cover every
reachable path before E1 is qualified. Generic engine disposal/reuse still
requires full B; retaining an engine alone proves neither contract.

Activation rechecks attempt identity, predecessor epoch, content, ledger
reference, transition completion, budget ownership and stopping state under the
final synchronous boundary. Standalone transitions retain their absolute
deadline. A deployment transfers the drained transition to its owned executor
before full preparation; the drain deadline cannot expire completion or
activation authority. Process shutdown still fences installation. An old
candidate cannot activate against a later epoch. Expired standalone candidates
remain charged until disposal settles; successful candidates transfer their
reservations to serving ownership.
Safe resumption of the previous coherent view allocates a fresh epoch; a closed
epoch never reopens, so an old callback cannot regain admission authority.

## Durable deployment

`version: 2` is the sole operational encoding in `__cluster/state.json`; there
is no serialized execution mode. Fresh apply bootstraps v2 directly. Use the
existing resource catalog and persisted lock, without a job store or polling
executor. Unknown versions and `state.lock: false` refuse. Legacy import,
refresh, approval and sweep execution are removed. Explicit stopped-writer v1
conversion is the only legacy reader and preserves existing graph data.

### Ledger v2

| Field | Meaning |
|---|---|
| `version`, `ledger_id`, `state_revision`, `next_sequence` | Strict encoding; immutable ledger incarnation (ULID); checked monotonic replacement and deployment counters. A canonical deployment ID is `<ledger_id>:<sequence>:<nonce_ULID>`; sequences start at 1. |
| `applied_revision` | Serving-sufficient resource projection, an exact-inventory `schema_contracts` map, and `result_revision`; this revision advances only when achieved configuration changes. Full convergence alone sets `config_digest`. |
| `outstanding` | Null or one immutable input/base/authority identity, per-graph prepared intents and mutable exact outcomes, optional persisted settlement intents, and reserved completion capacity. |
| `deployment_results` | Bounded terminal records: original ID/input/base, exact outcomes, achieved-result reference, initiating actor and recovery executors. No persisted runtime activation or restart-needed claim. |

Submission reserves **one outstanding deployment per cluster** under lock/CAS.
A competing new ID refuses busy; there is no queue, supersession or timeout that
clears the slot. Ordinary apply proposes a fresh nonce and the observed next
sequence, then revalidates that exact sequence under admission. It emits the
full ID before acceptance CAS and graph effects and returns it in the final result. An optional caller-preallocated ID must carry
the same ledger incarnation, exact next sequence and a fresh nonce. A crash
before acceptance can leave the sequence available, but another submission gets
a different nonce: the exposed ID cannot alias that later deployment. Advance
`next_sequence` only in the acceptance CAS, without reuse or wraparound.

Lookup matches the **full ID**, never sequence alone. Resubmission of an existing
full ID additionally requires the same input digest and returns the original
record without another execution; an input mismatch refuses. No past sequence
can be accepted again, even with a new nonce or after result eviction. A lost
acceptance acknowledgement is resolved by the original full ID; it never grants
permission to allocate another one.

The achieved base is `{ result_revision, resource_digests, schema_contracts, capture_cas }`.
The CAS records capture provenance; later progress records use fresh CAS values
without invalidating an unchanged achieved projection. Applied and active are
distinct: direct execution records no activation, and a settled partial result is a valid base
for a corrective deployment. No operator must edit the ledger to get past an
inactive or partially converged predecessor.

Each applied `schema_contracts` entry retains the engine-issued source hash,
accepted IR hash, schema identity domain and identity version for that graph.
Conversion captures these from one coherent accepted engine view; successful
schema outcomes advance them with the resource projection; completed deletion
removes the graph's contract and resources together. Refusal and not-attempted
outcomes preserve them. Bind the same map in the achieved base,
reserve its encoded capacity, and compare the full identity before serving or
accepting changes to that graph, including runtime-only changes and after
terminal receipt eviction. Unaffected graphs are not opened during preparation;
their unavailability does not prevent an independent deployment. Matching or
changed source text alone cannot adopt a replacement graph or different accepted
IR. New preparation refuses unexpected roots and mismatched accepted contracts.
Missing managed roots refuse unless the requested effect is deletion. Accepted
deletion resumes its recorded root through absence, as specified below. Restore authoritative state
before a new deployment to a damaged graph;
status and boot never manufacture an identity or accept drift.


The immutable input contains normalized configuration semantics and exact schema
and stored-query bytes, including query deletions. Persist and verify their
content-addressed bytes before acceptance. Roots and format remain fixed;
runtime-binding changes are explicit differences from the applied projection.
New graph declarations receive exact prepared creation identities. Removed
graph declarations carry deletion intents bound to the current root and applied
schema contract. Validate the whole
effect set and every query against its intended accepted schema before the first
schema effect. Execution and recovery never reread mutable configuration files.
There is no digest-based sweep/import/refresh execution path.

### Admission, authority and limits

The server and every supported CLI graph writer, writable opener, cleanup and
native-control command acquire the same exclusive cluster admission **before**
opening graphs, then reread version and outstanding authority. Resolve membership for
both named graphs and direct paths inside a cluster; spelling a graph URI must
not bypass admission. The server retains ownership for its entire lifetime.
While `outstanding` exists, serving and other writers/cleanup refuse even after
a previous lock is removed; only the original executor or an explicitly
admitted reconciler can progress it. Read-only ledger lookup remains available.
The executor performs no recovery sweep or Lance version cleanup; schema
staging/publication keeps native automatic cleanup disabled. This exclusion
protects predecessor and candidate evidence until the terminal result CAS.
Explicit whole-graph deletion instead removes the root named by its durable
intent, after that graph's users have drained; no retained version in that root
survives the operation.

Read-only CLI graph consumers use a read-only opener and validate canonical
membership, captured ledger authority and the accepted contract without taking
writer admission. Stored-query validation binds its registry and contract to
the same captured ledger revision. Writers and native controls retain exclusive
admission.

New deployment preflight uses only read-only graph preparation before writable
opening or control-payload persistence. A completed preflight refusal releases
its exact admission explicitly; upgrade and admission-construction refusals
follow the same completed read-only boundary. Report an acquired deployment
lock identity before subsequent fallible work. An engine refusal while recovering
previously accepted work preserves that outstanding authority and its lock, but
retains its bounded diagnostic cause.

V2 admission fails closed on abandonment: dropping a future or guard never
removes the persisted lock. Explicit release requires proved settlement or
containment of all work that can still act, including accepted control I/O.
Generic server native settlement is not qualified, so logical drain or ordinary
shutdown alone cannot release its v2 lock. Until a narrower release proof is
qualified, stop the owner and establish quiescence, then use exact-ID unlock.
There is no automatic takeover. `cluster force-unlock` checks the exact lock ID.
This emergency procedure requires operator exclusion of new admissions and other
unlock/release operations until it finishes, as well as proof that the prior
owner, graph work and accepted ledger, resource and lock I/O cannot act later.
It makes no concurrent-unlock or conditional-delete guarantee. Process death,
PID and elapsed time alone do not prove remote I/O quiescent. Unlock does not
settle an outstanding intent.

Preserve the existing authority distinction. Persist `authority_kind` as
`storage_owner` or `authenticated_identity` and the immutable optional original
actor. Core storage-holder CLI access remains an explicit storage trust boundary;
`--as` supplies attribution, not bearer authentication or new policy authority.
Enforce installed graph policy. Identity-authorized APIs require a trusted
current identity and current applied cluster/graph policy for their exact effects.
Bind ledger incarnation, achieved base, immutable input, effect set and
principal in the accepted deployment. Candidate policy never authorizes a
change to an existing graph; new graph bindings are part of the authorized
creation input. No persisted approval is a capability. Recovery/lookup check the **current** caller's required
authority: identity-authorized recovery requires existing `ConfigManage` plus
graph `SchemaApply` and `Read` for schema settlement or graph deletion. Direct
deletion enforces those same graph actions whenever a policy is installed.
Deployment status and result
metadata require current `ConfigManage`, without graph-data `Read` on unrelated
graphs; they disclose no rows or stored query/policy source.
No new policy action or read-only right permits fence publication. Recovery can
settle an earlier actor's outcome without impersonating that actor; preserve
the original receipt attribution and record the actual recovery executor
separately. This adds no mandatory policy to storage-owner clusters that do not
already bind one.

| Encoded control limit | Maximum |
|---|---|
| Ledger read/replacement | 16 MiB |
| One configuration/schema/query/policy source | 1 MiB |
| Distinct source bytes in one immutable bundle | 8 MiB |
| Encoded immutable bundle | 16 MiB |
| Resources in applied/input projections | 4,096 |
| Retained terminal records | 32 records and 4 MiB total; at most 1 MiB each |
| Diagnostics in one terminal record | 4 KiB total, with explicit truncation |
| Actor / resource address / canonical root URI | 256 / 512 / 4,096 UTF-8 bytes |

The encoded bundle limit also bounds JSON escaping and its resource projection;
an input can fit the source-byte limit and still be refused before acceptance.
Enforce byte limits while reading, before decoding or unbounded buffering, and
associate the bounded ledger bytes with that same read's CAS token. Bound paths,
actors and collections before constructing the plan. Prepare **all** engine
intents before the first schema effect: source limits do not bound encoded IR,
escaping or duplicated predecessor/desired contracts. Preflight the encoded
outstanding record plus worst-case exact receipts/refusals and settlement
intents, including their IDs/actors, against the ledger
and result limits.
Reserve that completion capacity at acceptance; refuse oversized work before
any schema effect, not after an earlier graph has published. No later progress
record may exceed its reserve. Keep one current recovery executor per effect;
progress updates replace bounded metadata rather than append an attempt log.
Evict oldest terminal records at acceptance as needed; never evict outstanding
authority or require additional admitted ledger/result capacity to record
completion. This reserves encoded control capacity, not B's still-unqualified
native completion memory.

A retained/outstanding record with the requested sequence but another nonce
returns an identity mismatch, never that other record's receipt. If a sequence
below `next_sequence` is no longer retained, return `result_expired` with the
requested full ID's acceptance and outcome explicitly **unknown**: the high-water
mark proves only that some nonce consumed that sequence. Different ledger
incarnations and absence at or above `next_sequence` are distinct lookup results.
No missing/expired result proves non-execution or permits replay.

These are encoded control/input limits, not a native manifest-scan or process-RSS
bound. Exact engine lookup selects bounded evidence but may scan history-sized
physical data. E0 must report and qualify that offline cost envelope without
claiming general native bounds; E1 retains the aggregate lookup/native-resource
gate. A resource failure leaves the original outstanding record recoverable.

### Execution and recovery

Each graph advances `NotStarted` (prepared) → `Started` → `Settled`. Acceptance
stores every prepared intent/certificate and its reserved completion capacity in
`NotStarted`; that does not authorize an engine invocation by itself.

1. With exclusive admission and current authority, verify immutable input and
   achieved base, prepare every graph's exact schema intent/no-op certificate,
   validate queries and reserve completion capacity using read-only graph
   handles. Only after this preflight succeeds, persist immutable input and
   `outstanding`. After acceptance, open each affected graph for schema execution
   under the retained admission. The writable opener can perform a local
   capability probe; it is outside the read-only refusal-release boundary.
2. Revalidate captured authority, then durably CAS that graph to `Started` with
   its exact intent immediately before invocation. Invoke only after that CAS
   is confirmed. Execute each original schema intent at most once in deterministic
   graph order; persist `Settled` with its own exact receipt or proved refusal
   before continuing. Uncertain effects stop later schema execution and keep the
   slot occupied. An uncertain original intent is never prepared again or replayed.
3. After a lost owner and prior-work quiescence, settle `NotStarted` schema intents as
   `not_attempted` without an engine invocation or fence. Reconcile unresolved
   `Started` intents against their protected exact candidate; record positive
   results without applying again and use the fence protocol below when needed.
   Recovery does not continue original schema execution. A stale no-op certificate
   settles as a refusal: its invocation cannot issue schema effects, so it needs
   no fence and cannot leave an unknown-publication obligation.
4. Persist the exact achieved projection and terminal result, then clear the
   outstanding slot in that same CAS, only after every attempted effect and
   control obligation is settled or safely contained. Schema/query bindings
   advance together per graph when its intended schema is proved accepted;
   refused/not-attempted graphs retain their old bindings. Query-only effects
   change only the ledger. Multiple graphs can converge partially; this is no
   cross-graph transaction. Validate the achieved projection against accepted
   graph contracts before allowing serving. A live owner atomically installs
   the achieved bindings; a stopped-cluster apply needs a subsequent server start.

Graph creation prepares a canonical root, source/IR contract and preallocated
genesis identity before effects. Reconcile only that exact birth; matching source
text at the same path never proves ownership. A partial initialization retains
its exact claim. After prior-owner quiescence, settlement may remove only that
claim's verified unpublished, empty birth artifacts; a committed, foreign or
advanced graph is never reset. Interrupted settlement keeps the claim and is
repeatable. A later deployment can create a fresh graph after proved absence,
including a bounded empty local directory tree left by settlement. Files,
symlinks and cloud markers refuse; engine preparation still proves the target
empty before minting new creation authority.

A prepared schema intent uses **intent version 2**, binding canonical root,
native main incarnation, numeric manifest base `M`, predecessor, exact accepted
and desired source/IR identities, original actor and preallocated lineage ID/time.
Its only possible publication is numeric version `M + 1`, with no rebase onto a
later version even if an intervening metadata update preserves graph HEAD.
Reject old version-1 intents for this protocol; this is not a graph-storage
format bump or a compatibility fallback. Source-only changes still publish one
contract commit; an exact no-op certificate binds already accepted source/IR and
cannot issue schema effects. A migration with no table steps is not alone a no-op.

Reconciliation of a `Started` schema intent/certificate returns one of these
exact outcomes. The ledger additionally records proved refusals and `not_attempted`;
a stale no-op certificate is a refusal, never an effectful unknown:

| Outcome | Required evidence |
|---|---|
| `Committed` | The original candidate version's own commit, head and source/IR contract match its intent; return that publication's receipt. |
| `NoOp` | The exact no-op certificate still matches accepted authority. |
| `NotPublished` | A verified occupied `M + 1` excludes the original commit, or the neutral settlement fence has its own exact receipt. This proves no graph publication, not absence of staged files. |
| `Unknown` | Missing/pruned/malformed evidence, unresolved authority or I/O, or an unproved binding; retain outstanding and refuse replay. |

A sampled later HEAD or a missing `M + 1` never proves non-publication. If that
version is absent, an authorized recovery actor may prepare a **neutral lineage
settlement intent** bound to the original intent digest and same `M + 1`. Persist
its authored actor, preallocated ID/time and exact predecessor/contract before
invoking it. It publishes through the existing graph publication door, preserving
schema, data and pins while adding its own neutral lineage commit. Original
schema publication and settlement compete for that one version; neither rebases.
A valid occupied candidate proves which publication won. A foreign occupant
must be verified from that version's own identity and contract, not inferred from
HEAD. Its presence can prove the original `NotPublished` but does not authorize
adopting foreign schema into the applied projection. If the accepted contract no
longer matches that projection, report drift and refuse serving until
authoritative state is restored and their agreement is validated. Neither old applied digests nor
requested input can be relabeled as achieved to hide that mismatch.

A lost fence acknowledgement is reconciled by its persisted identity. Another
currently authorized recovery actor may finish that same settlement intent,
preserving its authored receipt while recording the actual executor in the
ledger. It never runs the original schema effect. Strict numeric CAS prevents a
late original schema publication after a proved occupied candidate; it does not
fence delayed control-store writes or make staged/native I/O settled. Prior-owner
quiescence and exclusive admission remain required independently.

### CLI and conversion

Direct execution and recovery use the existing global
`--cluster ROOT` selector. An explicit canonical storage URI permits recovery
independently of configuration discovery or current input files.

| Command | Behavior |
|---|---|
| `omnigraph --cluster ROOT cluster upgrade-ledger --writers-stopped` | Explicit stopped conversion of v1 to v2, or removal of obsolete completed-result activation fields in v2; conditionally replace the ledger without graph effects. |
| `omnigraph --cluster ROOT cluster status [--deployment-id ID]` | Bounded authorized lookup; return incarnation, next sequence, outstanding full ID and lock identity without graph opens or effects. |
| `omnigraph cluster plan --config DIR` | Observe without a writer lock; derive migration steps from the same captured preparation with the intended actor. |
| `omnigraph cluster apply --config DIR [--deployment-id ID]` | Bootstrap or execute under direct exclusive admission; allocate and emit the original ID unless supplied. |
| `omnigraph cluster plan --server URL --config DIR` | Observe changes and schema migrations against captured applied state without closing serving admission or taking a schema gate. |
| `omnigraph cluster apply --server URL --config DIR [--deployment-id ID] [--no-wait] [--timeout SECONDS]` | Submit once to the running owner; wait for durable acceptance, then observe that ID until convergence and activation unless `--no-wait` is set. |
| `omnigraph cluster status --server URL [--deployment-id ID [--wait [--timeout SECONDS]]]` | Observe cluster status or an exact deployment receipt; optional waiting polls the original ID without executing it. |
| `omnigraph --cluster ROOT cluster apply --deployment-id ID --writers-stopped` | Reconcile that original deployment only; never submit, use new files or replay schema effects. A terminal result is lookup-only. |
| `omnigraph --cluster ROOT cluster force-unlock LOCK_ID` | Exact-ID release under the operator-exclusion/quiescence procedure; outstanding evidence remains intact. |

Root-addressed forms do not load `--config`; the selectors are mutually exclusive.
`--writers-stopped` is the operator's explicit attestation that prior writers and
accepted graph/control I/O are quiescent and excluded. It supplies no technical
fence and does not replace lock admission or Azure's wrapper. Conversion and
reconciliation require it; read-only status does not. Routine apply does not need
manual status-and-copy, and may not allocate a new ID when resolving a previous
unknown result. Missing/expired original lookup never creates a replacement.
Hosted service workflows use the explicit `managed` command family and retain
their separate control-plane API. Folder context never redirects `cluster`
commands to that API. These flags do not manufacture a remotely authenticated
identity. Status needs no lock and cannot authorize a writer.

Conversion preserves graph data, native identities, branches/history, applied
resource digests, policy/provider bindings and existing audit/approval facts.
Require stopped serving/writers/maintenance, settled prior cluster work, bounded
valid input and exact agreement between the applied projection and accepted
resources. Refuse unresolved legacy recovery, drift or unsupported formats;
never reset, import over uncertainty or fabricate historical deployment receipts.
Start a new ledger incarnation at result revision 0 and next sequence 1, with
no outstanding/result records. Write verified referenced resources before the
single conversion CAS; an interrupted/lost-ack conversion rereads the root and
recognizes the same admitted v2 ledger. No automatic migration/downgrade occurs.
Graph-format conversion remains separately gated by the storage-upgrade decision.
The same explicit converter removes obsolete `activation` and `restart_required`
fields from completed v2 receipts after validating their old shape. It preserves
ledger identity, sequence and exact achieved outcomes and advances the ledger
CAS revision once. Ordinary readers use the same validation to recognize an old
completed receipt and return `ledger_upgrade_required` with the stopped-upgrade
command; they never consume converted state. Unknown fields, unsupported receipt
variants and outstanding work refuse conversion. Repeating a completed conversion
is a no-op.

### Deployment scope and preparation

Ordinary apply creates graphs, deletes removed graphs and changes configuration.
It has no lifecycle options file, unregister operation, schema-correction
override, graph adoption, missing-root recreation or catalog repair mode. Missing
or corrupt authoritative payloads must be restored; candidate sources cannot
substitute for current permissions.

Removing a graph declaration is destructive. Plan reports the delete effect;
apply requires existing exclusive root admission. Authenticated callers need
currently applied `ConfigManage` permission plus graph `Read` and `SchemaApply`.
Storage-owner callers also need an actor with those graph permissions when a
graph policy is installed. The intent binds the canonical derived graph
root and its applied schema contract before effects. A present root must match
that contract; an already-absent root can complete deletion. The same outstanding
ledger record persists `Started` before the first removal, then records
`Deleted` only after root absence is verified. Only that completed result removes
the graph's applied resources and contract. No second deletion ledger or queue
is introduced.

The delete traverses only the exact managed graph root. It removes every branch,
retained version, staged file, index and managed Blob object there; it never
follows external Blob references or a sibling-root prefix. This is whole-graph
lifetime deletion, not Lance cleanup with a retention window. Object-store
completion means active-namespace absence, not purging provider versions or
bypassing retention. A failure after `Started` retains outstanding authority
and admission; timeout does not prove a remote delete request has settled.
Accepted deletion cannot become a not-attempted or refused terminal outcome.
After supported owner handoff,
reconciliation resumes the original ID and root even if the manifest is already
gone, verifies absence, and completes the result. Before `Started`, a readable
root must still match the exact accepted contract. After `Started`, partial
removal may expose an older manifest; recovery accepts a readable contract from
the same graph lifetime and refuses a replacement identity. An unreadable partial
root remains owned by the original intent under the stated quiescence boundary.
Recovery does not create missing
storage, adopt replacement contents or reinterpret changed configuration as a
cancellation. A repeated completed-ID lookup never deletes again.

Online removal closes affected admissions and drains tracked request descendants
and response-body ownership before touching storage. The existing exclusive
owner remains responsible for accepted writes; uncertain completion keeps the
graph closed and triggers containment. Generic drain is not a new proof of all
native-I/O settlement and does not authorize admission-lock release or arbitrary
runtime reuse. Offline deletion and recovery retain stopped-owner/accepted-I/O
quiescence requirements and exclude raw or older writers. There is no retained
owner pool or readoption path.

Policy grants, revocations and rebinding use the currently applied authority.
Removing a provider still referenced by desired graphs fails configuration
validation. A provider change does not re-embed stored vectors. Blob rules retain
root-disjointness and server-safe projection checks. A terminal partial result
publishes only achieved graph state; serving must install achieved management
policy too, so a policy handoff cannot strand the corrective deployment.

Plan always observes without acquiring writer admission. Direct planning uses
shared effect-free preparation. Served planning captures source bytes, the ledger
and current runtime contracts, checks current authorization and deployment scope,
and derives schema migration steps from one accepted schema view. It never opens
a second engine or takes the exclusive schema gate. Relevant unavailable graphs,
schema drift, unsupported migrations and branch restrictions refuse. Physical
rewrite, creation and deletion eligibility remain the executor's post-drain checks.
Both plans include the observed ledger CAS, input digest, resource changes and
explicit managed-root deletion including history. They write no ledger or plan
resource and consume no deployment sequence. Apply checks its fresh accepted
base; an observed plan is neither a reservation nor permission to replay work.

## Online deployment

`POST /cluster/plan` accepts a bounded captured bundle and returns the observed
plan under current management and graph permissions. It uses the ordinary read
lane and resource limits, validates the serving projection, and never closes graph
admission or creates a deployment record.

`POST /cluster/deployments` accepts a client-known `deployment_id` and one
bounded `CapturedDeployment`. The CLI discovers the v0.12 contract, reads
`GET /cluster/deployments`, captures local source bytes bound to that authenticated
canonical root, allocates the next identity and prints it before POST. Omitted
storage selects that root; an explicit absolute root must match without client
storage lookup. The server independently revalidates canonical ownership. The CLI
does not write the ledger. `GET /cluster/deployments/{id}` observes the original
outcome; disconnection never authorizes a new ID or replay.

POST returns `202` only after the outstanding ledger CAS confirms acceptance,
with `{deployment, active, in_progress}`. A pre-effect refusal still returns its
error; an existing original ID is lookup-only. Returning acceptance releases the
HTTP observer, while the existing owned executor retains completion and activation.
No job queue, background poller or second durable operation record is introduced.

The CLI waits by default. `--no-wait` returns after confirmed acceptance, which
can itself require waiting for drain and preparation. `--timeout` bounds the
entire caller wait (default 300 seconds, maximum 3600), including acceptance; it
never cancels work. Each GET is separately bounded, and transient observation
failures may retry only GET within the same total budget. POST is sent at most
once. A lost POST response may be resolved through the original ID. Apply also
checks the immutable input digest on every outstanding or completed receipt;
an old ID can never confirm a different submitted bundle. Timeout
returns exit 5 and structured original-ID/last-observation output; it does not
assert deployment failure. `status --deployment-id ID --wait` resumes observation.

Exact GET returns only `{deployment, active, in_progress}`. `in_progress` derives
from the current process owner of that exact ID, not another durable state machine.
`NotRecorded` with a live owner means preparation; `Complete` without activation
can still be in progress. Without that owner, an outstanding record requires
recovery and an inactive complete result is terminal for this observation.
Expired receipts, identity mismatch and different-ledger results terminate waiting
without replay. A process-local observation is not proof of native-I/O quiescence.

Current management permission grants ordinary status access. A still-authenticated
initiator may additionally read its own exact durable receipt after a deployment
revokes its management permission, identified by recorded authenticated authority.
This grants neither current cluster inventory/sequence/lock observations nor any
write permission. A foreign, missing or expired receipt cannot establish that
exception; authentication and current permission remain required there.

The server resolves a trusted bearer actor, refuses graph-scoped data tokens,
checks applied `ConfigManage`, and delegates exact graph authorization and input
validation to the v2 executor. One process gate owns the complete operation,
including activation; competing POSTs refuse without queueing. The accepted
future belongs to the server through completion even if its HTTP caller leaves.

Before closing admission, validate captured input, current authorization and the
shared serving projection without opening graphs or acquiring schema gates.
Resolve required provider secrets and validate changed runtime bindings before
effects. Close only affected graph admissions in one batch and drain their request
descendants under a bounded deadline. Then prepare and execute exact intents
under the server's retained root admission, using existing engines for retained
graphs. The owned executor retains preparation, completion and activation after
drain; the drain deadline does not cancel it. Deletion keeps its predecessor owned until
the destructive operation settles; it never serves a partially deleted root.
Unaffected graphs continue serving. Install exact achieved schema
contracts, validated query registries, engine runtime bindings, management policy
and graph inventory in one registry snapshot under the shutdown/attempt
fence. Remove an inventory entry only for an exact durable `Deleted` outcome
belonging to this closed transition; never retain it for re-adoption.
Deterministic pre-effect
refusals resume the predecessor views under new epochs. Uncertain effects retain
closed admission and the existing process-containment rule.

Publish the activation identity in the same immutable registry snapshot as the
installed bindings. It contains canonical root, admission incarnation, original
ID/input digest, achieved revision and config digest. Keep the durable achieved
receipt unchanged; there is no post-activation ledger CAS or mutable durable
restart-needed flag. Status reports `active` only when this exact identity
matches the requested current receipt, its affected bindings are ready under
their installed schema contracts, deleted entries are absent, and process
admission remains open. Unrelated blocked graphs do not invalidate activation.
Partial convergence, unavailable
affected bindings and shutdown report inactive. Ordinary
row commits do not invalidate unchanged deployment bindings. Boot revision/digest
stays a boot fact.
Original-ID resubmission is lookup-only before graph closure, including a
historical inactive result. A late attempt cannot install an older view. A
terminal inactive or partially converged predecessor permits a corrective
successor; boot never replays graph effects.

The server retains its Azure wrapper lease throughout. HTTP submission uses no
second wrapped storage-writing process and does not break or share that lease.
Provider qualification remains a separate support boundary.

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

The implemented export/baseline transport retains its existing four-chunk,
eight-response allowance through queued, yielded, cloned and sliced buffer
ownership. Three outstanding frame credits leave one pending producer slot;
exhausted credits backpressure the admitted producer until capacity or response
closure. Only initial reservation acquisition has a short admission timeout;
there is no per-frame delivery deadline. The terminal baseline cursor shares
this close-aware wait. Engine export coalesces lines into at most 64 KiB chunks
using the same single pending slot, including a final partial flush. Existing
transport, route and TCP owners cover retained allocations, final release,
paused/resuming clients and complete response termination. This qualifies transport payload ownership only, not
full-row/Arrow encoding, native memory or RSS bounds.

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
startup as loading entries, then ready or blocked outcomes. A closed same-view transition
projects its graph as `transitioning` with action `wait_for_transition`; shutdown
projects all entries as stopping. Transitioning graphs count as unavailable in
readiness and retain the existing authorized-unavailability disclosure rule.
No finite retry time is promised. An explicit, proved pre-effect abort restores
unchanged service; expiry or abandonment alone leaves the graph unavailable
until a qualified recovery or restart.
One authorized inventory reports read/write runtime availability,
sanitized failure and a supported action, replacing the separate quarantined
list. Credential graph scope precedes resolution. Unavailable graphs disclose 503
only to graph-read or management-inventory authorized callers; an invalid graph
policy or configuration cannot authorize read disclosure. Other callers cannot
discover them through that distinction. Registry membership and availability are distinct
facts: `served_graph_count` counts registry entries; readiness also reports ready,
loading and blocked counts. Closed transitions count as blocked. An empty valid
inventory is ready; shutdown is unready. The listener starts after fixed identity,
trust, admission and policy validation, before graph opening. One process-owned
startup batch opens at most four graphs concurrently without retry or cancellation
timeout. Loading graphs return authorized 503 with action `wait_for_startup` and
no retry promise; health remains live and readiness stays 503 with status
`loading` until every startup attempt finishes. Default startup admits each
successful graph immediately for direct requests. Once loading finishes, a
nonempty inventory is ready if at least one graph is ready, with `degraded`
status when other graphs are blocked.
`--require-all-graphs` admits the successful batch atomically only after every
graph succeeds. Strict failure or a nonempty, entirely failed batch stops the
server. Shutdown retains entered opens and fences late installation through the
same process/registry boundary. Policies are captured once before listening.
Bounded transient startup retries remain separate implementation work.
This logical startup owner supplies no native-settlement or reuse proof.
No historical-only readiness mode is introduced.

Keep boot revision/digest as boot facts. Direct apply records achieved results
without a runtime claim. Online activation installs exact identity in the
registry snapshot alongside verified bindings. A normal configured boot captures
the latest matching converged receipt under root admission, then establishes
fresh runtime evidence when every configured graph opens successfully. Generic embedding
constructors do not attest deployment bindings. Missing/expired receipt identity
is not inferred from schema text or a successful apply. Status reads bounded snapshots outside blocked data lanes;
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
and digest checks for the achieved policy. Report phase, attempts,
classified failure and limiting resource.
Supervisor `Retry-After` requires a finite scheduled retry; admission 429 may give
caller-backoff guidance without scheduling work. Neither header authorizes write
replay. A timer alone is not recovery progress.

Declared `@embed` configuration does not imply population occurred. HTTP JSON,
NDJSON and JSON load results and CLI JSON now carry the required nullable
`embedding_generation` capability: `unsupported` when a loaded node declaration
has `@embed`, including when all vectors were supplied, and `null` otherwise.
Human CLI output provides actionable guidance. A successful nullable load remains
successful; supplied vectors remain unchanged, omissions follow schema nullability,
and ingestion does not call the provider. Automatic embedding remains owned by
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
serves storage v14 only; normal open never migrates. Supported standalone roots
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
E0/E1 deployment or mixed-format serving feature. E0's explicit no-reset ledger
conversion is separate from graph-format conversion and wire admission; it
refuses unsupported graph formats and never bypasses this rollout gate.

Azure retains its mandatory admission wrapper and qualification-preview boundary.
The running server executes HTTP submissions under its existing root lease; the
CLI submits over HTTP and requires no competing storage-writer lease. Direct
apply retains its stopped-writer wrapper procedure. Never bypass, share or break
the lease or write the ledger directly.

The v0.12 OpenAPI, CLI contract, user guidance and tests ship together. Document
intentional breaks: structured deletion errors replace the string alias; one
inventory includes unavailable graphs without a `quarantined` alias;
`served_graph_count` includes those entries, with separate ready/loading/blocked counts.
Known unavailable graphs use 503, unknown graphs 404, with
authorization/disclosure rules intact. Generic 503 is not retry
permission. There is no promise to preserve older wire fields or exit behavior.
No unsupported online transition may silently restart or reset the graph.

## Alternatives

| Alternative | Reason not selected |
|---|---|
| Permanently require restart for every schema deployment | E0 makes offline outcomes recoverable; E1 must remove unrelated serving interruption once qualified. |
| Refresh a handle or swap only queries | Does not establish coherent bindings or settle old users. |
| Separate apply process while serving | Atomic schema publication supplies no cross-process fence for native controls, reclamation or deployment ownership. |
| Require generic engine disposal/reuse before every E1 transition | Retaining the same engine may admit a narrower proof; every interfering effect and retained resource still needs qualification. |
| Queue and supersede deployments | Adds ordering states without a requirement for concurrent deployment submission. |
| Require historical overlap or durable data idempotency first | Couples live schema activation to separate storage and request protocols. |
| Add a separate deployment service, job store or compatibility framework | A bounded endpoint in the existing writer process supplies submission without another authority. |
| Add native tags or a retention store for E0 | Exclusive admission across outstanding lifetime protects evidence within the explicit operator-excluded writer boundary. |

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
| T8.live | Same engine/PID/listener and coherent schema/query activation; deletion waits for parked requests and held response bytes, then physically removes only its target and atomically removes its inventory entry. Cover shutdown races, unaffected graph progress, submission-only CLI and writer exclusion. Native-tail/provider claims require their own evidence. T8.history/retention are deferred outside scope. |
| T9 | Concurrent new submission refuses while one revision is pending, including across restart. Settled partial/inactive D1 permits corrective D2; stale-base submission refuses without effects; original lookup never replays. E0 additionally proves strict numeric publication/fence races and durable exclusive admission through recovery. |
| T10 | Declared bounds and completion/status reserves hold under saturation, slow/abandoned consumers and repeated failed deployments; reconcile counters with independent allocation/lifetime evidence. |
| T11 | Wide-feed progress, explicit ingestion-embedding diagnostics and bounded authorized status; unavailable 503 versus unknown 404, correct disclosure, liveness/readiness, bounded transient startup retries and permanent policy refusals. |

T11.feed extends `data_routes::change_feed_poll_advances_cursor_only_after_complete_commits`
with an actually updated row over 4 MiB, complete managed-Blob before/after
encoding, byte-driven continuation and a later sentinel commit. It checks that
an abandoned response and yielded byte slice retain the serving view, and that
replaying its page token delivers the same logical change before a completed
checkpoint advances. This HTTP evidence complements the sort-cap and sliced-parent
engine regressions in [PR #843](https://github.com/ModernRelay/omnigraph/pull/843);
it does not itself qualify that scan repair, native settlement, RSS bounds or
provider request counts.

The embedding diagnostic has focused HTTP regression coverage in `data_routes`
with a counted OpenAI-compatible endpoint, successful embedding calls before and
after the load matrix, and zero provider requests attributable to loads. The
same test checks exact supplied vectors, durable Arrow nulls, required-vector
refusal and unannotated loads; `parity_matrix` covers CLI load output.
T11.embed remains incomplete: [RFC 0045](0045-gq-logic-tests.md)
explicitly excludes `@embed` schemas, and GQT has no load step with outcome
expectations. Setup-only seed loading cannot supply that evidence. Changing
those accepted format boundaries requires its own decision; this work adds no
GQT protocol, native-lifetime or automatic-generation qualification.

E0 qualification extends engine `schema_apply.rs` and `detached_commit_matrix.rs`,
cluster lifecycle/failpoint tests, and CLI cluster lifecycle/system journeys. Prove
real child-process death (not destructor-running error returns) before/after
acceptance, original publication, fence publication and terminal ledger CAS; lost
acknowledgements; metadata interposition; changed/deleted input files; current
recovery authority with preserved attribution; partial then corrective deployment;
pre-acceptance process death followed by a different nonce consuming the same
sequence; full-ID mismatch and expired lookup; bound/reserve refusal before the
first graph effect;
and no-reset conversion/refusal. Race server boot, direct-path writers, cleanup
and native controls against admission and outstanding recovery. Prove abandoned
guards leave the lock and stale owners/control writes cannot act after supported
handoff. Local success alone does not qualify S3 or Azure behavior.

Whole-graph deletion extends these same cluster failpoint and CLI/server owners:
kill before `Started`, after partial removal, after verified absence and before
terminal result CAS; reconcile the original ID without a second effect identity.
Prove peer roots, external Blob objects and neighboring prefixes survive,
unauthorized or drifted preparation has no effects, all target branches/history
are gone at success, and a held response body delays removal. Repeat against the
actual provider before claiming its destructive/recovery qualification. These
are evidence gates, not completed results.

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

### E0 implementation evidence

PR #849 implements offline ledger v2, strict intent/fence settlement, bounded
input/results, lifetime admission, current recovery authorization and no-reset
ledger conversion. Local engine, cluster, CLI and server owners cover real
process death, partial correction, exact receipts, refused authority and retained
locks. In the local debug `manifest_schema_settlement_history_curve` instrument,
at 16 updates and 15,917-byte IR, increasing source from 16 KiB to 1 MiB kept
the first lookup after reopening at 22 requests (original/fence) or 31 (occupied), while
manifest reads grew from about 116.7 kB to 2.18 MB or 171.7 kB to 3.27 MB,
respectively. This file-storage sample with uncontrolled OS caches establishes
neither constant scan bytes, aggregate E1 or RSS bounds, nor native-I/O settlement.

At `2032f6ef`, local workspace and standalone GQT/clippy gates passed, as did CI
[workspace, RustFS default/failpoints, Azurite and clippy](https://github.com/ModernRelay/omnigraph/actions/runs/37082815427),
[GQT](https://github.com/ModernRelay/omnigraph/actions/runs/37082815098) and
[DST](https://github.com/ModernRelay/omnigraph/actions/runs/37082815106).
Azurite is emulator evidence; live Azure retains its wrapper and preview gate.
These results do not qualify untested provider profiles or existing-cluster v13
conversion. The original offline evidence does not qualify the later online path.

### Release-correction validation (2026-10-03)

The release-correction branch passed all 15 real CLI/server/proxy journeys in
`system_remote`, now required by the ordinary workspace gate, plus 13 RustFS
and 13 Azurite cases using the image digests pinned in CI. The backend runs
covered storage, cluster/offline deployment, serving and CLI owners, with
Azure lease/supervisor checks; no configured case skipped. This is local
emulator evidence, not live-provider or native-settlement qualification.

The [mixed HTTP diagnostic](../dev/testing.md#cost-tests-and-benchmarks) ran for
300 seconds on macOS arm64 with a debug server, local files, one actor/graph
and five closed-loop clients. It completed 11,890 reads, 1,062 writes and 144
full export/baseline streams; 2,086 competing streams received the expected
slot refusal and 70 were intentionally abandoned. No attempt failed or had
an unknown outcome. Every consumer mode ran, peers progressed during admitted
streams, and the final exported writes exactly matched acknowledged receipts.
Sampled RSS quarter medians rose from 202 to 294 MiB, with 305 MiB after the
recovery reads; read/write p95 attempt times were 40/43 ms, including client
validation. The workload added 1,062 rows and commits without maintenance:
it varies data, layout and history together and does not isolate a leak or
establish capacity, fairness or memory bounds.

The controlled release follow-up compared main `bb1daeec` with correction
snapshot `48288531` on the same Mac (Apple M5 Pro, 24 GiB RAM, local APFS).
Both binaries used the same compiler/profile and an explicit 100 MiB Lance
memory pool; these RSS values are not directly comparable to the earlier debug
soak's 1 GiB pool. Three interleaved ABBA blocks per state yielded six fresh
processes per build/state, with 25 reads and two exports for warmup, then 500
serial point reads and eight full exports per process. Every run restored identical
physical bytes, validated every response and verified no graph change.
All three states held the same 5,100 Person rows and the same remaining records.
Medians below summarize the six per-process medians:

| State | Person fragments | Main read (ms) | Corrected read (ms) | Main export (ms) | Corrected export (ms) | Corrected read-phase RSS (MiB) |
|---|---:|---:|---:|---:|---:|---:|
| Bulk-loaded | 3 | 1.13 | 1.12 | 49.11 | 19.13 | 135.8 |
| 1,000 small commits | 1,002 | 15.66 | 15.43 | 70.25 | 41.81 | 173.4 |
| Publicly optimized | 1 | 1.49 | 1.48 | 49.10 | 19.19 | 118.2 |

The equal-data small-commit state made reads about 13.8 times slower on the
corrected build; main showed the same effect. No code-regression signal crossed
the predeclared 20% margin plus the largest within-block same-build variation.
Complete export latency fell by about 40–61% in every paired block. This is a
local screening result, not equivalence, a capacity measurement or an RFC 0039
publishable archive. The baseline already includes the earlier server upgrade
slices; this does not compare the whole upgrade with a pre-upgrade release.
OS cache residency and host isolation were not qualified.
Public optimize also created indexes and changed manifest layout, so its
improvement does not isolate fragmentation from indexing or history.
Two additional fresh-process, 300-second controls held the fragmented graph
fixed while two readers ran alongside fast, paused and abandoned export/baseline
consumers. They completed 20,406 reads and 576 full streams, with 284 intentional
disconnects, no failures and unchanged fixture bytes. First-to-last-quarter RSS
medians moved 206.7→213.5 MiB in one run and 210.6→207.9 MiB in the other;
post-idle RSS was 209.8/208.1 MiB. Read medians likewise moved in opposite
directions (+5.2%/−6.4%). These runs did not reproduce sustained upward drift
without writes; they do not prove leak absence or characterize live-write
retention. A separate preparation-only trace rose from about 48 to 308 MiB
across 1,000 writes and remained there after five idle seconds. That single
trajectory includes startup warming, growing state and allocation history;
fixed-live-row write checkpoints with heap/cache observation and fresh-process
reopens remain the next profiling control.

The [testing guide](../dev/testing.md#cost-tests-and-benchmarks) owns the
reproducible comparison, retention controls and analysis commands. Native
settlement and a process-memory envelope remain unqualified under B2/B4/B6.

### Same-engine E1 evidence and its limits

The same-view runtime foundation now closes affected HTTP/MCP graph admission,
retains logical request/response owners and resumes the identical bindings under
a fresh epoch. Existing server owners cover a parked HTTP body, sibling progress,
disconnected writes, retained stream bytes, uncertainty and shutdown ordering.
These foundation tests establish request lifetime and same-view resumption. The
additional deployment owners below exercise schema/query replacement and graph
creation; none claims generic native settlement or a process-memory envelope.

The 2026-10-02 probes against main `7fe1789a` and pinned Lance 11.0.0 establish:

- `stored_queries::parked_stored_invocation_requires_a_serving_transition_barrier`
  parks a real HTTP body after routing. Raw apply before releasing it mixes the
  old query with the new contract and fails; finishing it first, then applying
  and replacing the query binding on the same engine succeeds. This isolates
  the binding requirement. Its extended positive control now exercises
  production same-view resumption on the existing router; its raw schema/query
  replacement remains an intentionally unqualified composition.
- The three read/CPU guards in `lance_surface_guards.rs` observe independent
  retained inputs, actual late local reads and separate standard-scheduler read
  budgets after callers disappear. These read-only tails do not show graph
  corruption, but their lifetime is outside current aggregate ownership. An
  object-store wrapper could own GET payloads; it cannot alone own subsequent
  native CPU/decode work. Those initial probes alone did not qualify retained-engine
  schema activation; the extended guard below supplies its narrower transition proof.
- `manifest_contract_history_curve` varies 16 KiB/1 MiB source, 4/512 enum
  values and 1/16 update checkpoints on main and a branch with four live person
  rows. A local debug run with 16 updates and 15,917-byte IR retained about
  0.98 MB/21.62 MB of manifest files for those source sizes. This is physical
  storage evidence, not RSS, a supported limit or a performance claim.

The 2026-10-03 implementation extends those existing owners:

- Registry tests activate schema/query bindings for multiple graphs atomically,
  retain the same engines, add new graphs, reject incomplete or stale candidates,
  and recheck shutdown and deadlines. A pre-effect drain-timeout abort restores
  unchanged views while retaining old request descendants for the next attempt.
- `boot_settings::live_deployment_retains_disconnected_owner_and_never_replays_original_id`
  parks an admitted HTTP body, proves sibling progress and competing-deployment
  refusal, disconnects the submitter, then observes successful activation. It
  checks authorization, pre-effect refusal, same-engine identity, exact-ID lookup,
  changed-input refusal and a successor that cannot be rolled back by an old ID.
- `cli_cluster_e2e::cluster_e2e_live_apply_changes_schema_queries_and_adds_graph_without_restart`
  runs the real CLI against a real listener, changes a schema and stored query,
  creates and queries another graph, preserves data and sibling availability,
  and verifies the same PID and listener throughout.
- The existing dropped-local-read guard now parks actual native file reads,
  drops their caller futures, applies a schema on the retained engine, and lets
  the old reads finish. Their immutable bytes remain valid and their completion
  cannot change the new graph head, contract or table bindings.
- Cluster and engine fault owners cover exact prepared births, partial creation,
  original-ID reconciliation without replay, interrupted owned cleanup and
  refusal to delete foreign, advanced or malformed birth evidence.

The same real CLI/server journey passed on local storage and on fresh pinned
RustFS and Azurite emulators. Both object-store cases are required CI cells with
checks against unconfigured skips. The final remote-capture path was rerun on
both emulators. The workspace suite, standalone GQT/DST suites, strict workspace
Clippy and documentation checks passed; ignored instrument/hunt cases were not
part of those default gates.

This evidence supports the retained-engine transition described here. It does
not establish generic native settlement, engine disposal/reuse, aggregate native
memory bounds or a process-RSS envelope. Live-provider fault qualification and
existing-cluster graph-format conversion remain separate gates.

## Rollout

| Increment | Deliverable | Shipping gate |
|---|---|---|
| A | A1 v0.12 HTTP admission; A2 own-publication merge receipts; A3 CLI outcomes | A1 follows [its accepted decision](2026-09-30-v012-http-admission.md); A2 is qualified under [Exact merge receipts](2026-09-30-exact-merge-receipts.md), including T1/T2; A3's initial typed-429 and qualified-HTTP-412 contract is qualified under [Owned server operations](2026-09-30-owned-server-operations.md) |
| B | Owned writes, read/stream accounting, drain and shared shutdown | [Owned server operations](2026-09-30-owned-server-operations.md) qualifies the task/body foundation. [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md) is accepted and partially implements query-child ownership and named aggregate write limits. Native settlement, completion reserves and runtime reuse remain unqualified under its T6/T10 gates. |
| C | Remaining initialization/native-control completion, owned maintenance and bounded transient startup retry | Atomic schema publication is landed; T4/T6/T7/T11 retain same-process progress and protected reclamation |
| D | Aggregate budgets and feed progress; embedding diagnostics implemented | T10–T11 and workload qualification, including the remaining T11.embed evidence above |
| E0 | Durable v2 execution, graph creation/deletion and exact recovery | PR #849 established schema/query execution; the sole v2 executor now also owns bootstrap and prepared graph creation. Qualification remains limited to recorded local/backend evidence. |
| E1 | Server-owned schema/query activation and graph inventory changes | Implemented with [same-engine transition evidence](#same-engine-e1-evidence-and-its-limits), T8.live/T9 and durable ledger/crash owners. Generic reuse and broader native bounds still require full B; Azure separately gated. |

A and focused C/D repairs may proceed alongside B. PR #849 extends the prepared
schema interface with strict intent version 2, neutral settlement fencing and
the [v2 ledger protocol](#ledger-v2). Protected evidence and
prior-owner/control-I/O quiescence remain independent obligations.
E0 does not wait for E1's retained-engine activation proof and does not claim it.
Existing-cluster rollout also requires qualified offline v14 conversion.
E1 does not wait for deferred
historical serving or data idempotency. Implement slices against the v0.12 contract
and update their actual status, OpenAPI and user/developer documentation together.
Acceptance or merge supplies no product qualification beyond the recorded evidence.

## Unresolved questions

Before enabling an affected increment, its owners must implement and qualify:

1. CLI/server: any broader retry allowance needs separate whole-command proof.
   A3 and the B foundation are qualified under
   [Owned server operations](2026-09-30-owned-server-operations.md); A1 admission
   and A2 exact receipt contracts and evidence remain with their accepted owners.
2. Cluster/engine/CLI: complete E0 qualification for each admitted backend and
   record its offline history-dependent lookup envelope. PR #849 supplies the
   implementation and [current evidence](#e0-implementation-evidence). The
   server-owned path adds the recorded same-engine evidence above; native
   aggregate bounds and broader backend fault coverage remain open. The bounded
   HTTP path and witness use the same v2 protocol.
3. Engine/server: qualify the native settlement mechanism and measured
   admission/completion profile in [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md)
   before exposing a reuse capability. Preserve the narrower E1 retained-engine
   assumptions and extend its request-binding, interference and native-tail
   evidence when new execution paths are introduced.
4. Azure: qualify live-provider lease-loss and accepted-I/O faults while the
   existing wrapped server owns HTTP deployments.
5. Storage/cluster: qualify data-preserving offline conversion of existing
   cluster roots to v14 before rollout, under the storage-upgrade owner.

## Decision log

- 2026-10-06: The maintainer approved served observational planning, durable
  acceptance followed by bounded CLI observation, and explicit `managed` command
  routing. Online deployment and CLI sections now define exact receipt reads for
  initiating actors, process-owned progress, caller wait outcomes, and the shared
  observed-plan limits. No new durable job authority is added.

- 2026-10-06: Review corrections amend the serving-transition and Online
  deployment sections: close and drain before full engine preparation, retain
  owned completion beyond the drain deadline, and qualify activation against
  affected bindings and deleted entries rather than unrelated graph health.
  Deployment scope now distinguishes exact-contract deletion preparation from
  same-lifetime recovery after `Started`; the graph-creation paragraph permits
  verified empty local settlement residue. The CLI and conversion section now specifies
  recognized old receipts without accepting them through normal reads.

- 2026-10-06: The maintainer requested physical graph deletion through ordinary
  `cluster apply`, with no deletion flag or unregister mode. This replaces the
  Scope and Deployment-scope sentences that refused graph removal, the blanket
  no-reclamation wording in Admission, and the E0/E1 graph-addition-only boundary.
  Desired removal records exact-root/contract intent in the existing ledger,
  drains affected serving work, resumes interrupted deletion by original ID, and
  removes applied/runtime inventory only after durable completion. Adoption,
  missing-root recreation and generic native settlement remain outside scope;
  deletion qualification is tracked separately from prior activation evidence.

- 2026-10-05: Simplify the deployment contract: remove deprecated transport
  aliases and lifecycle/repair overrides; make plan observational and derive its
  migration preview from shared preparation; keep activation evidence only in
  the atomic process runtime snapshot. Ordinary apply supports graph creation
  and configuration changes. Graph removal refuses pending a qualified physical
  deletion contract. Preserve exact durable outcomes and explicit no-reset
  conversion of obsolete completed-result fields.

- 2026-10-05: Restore declarative policy/provider/Blob lifecycle and explicit
  graph removal, adoption, recreation and catalog repair under the sole v2
  protocol. Share exact read-only preparation with plan, limit graph opens to
  affected resources, and activate achieved authorization coherently. Storage
  upgrade and native-I/O settlement remain separate work.

- 2026-10-03: Restore the original no-restart goal with one v2 execution protocol.
  Summary, scope, authority, serving views, durable deployment, CLI, online
  submission, availability, compatibility and alternatives now remove the
  offline/online ledger-mode split and v1 operational fallback. Add exact graph
  birth/recovery, HTTP submission under the existing server owner, atomic batch
  activation and an incarnation-bound witness. Keep data-preserving conversion
  and generic native-settlement/provider limits separate. This amendment does
  not treat acceptance as test evidence.


- 2026-10-03: Added controlled release A/B evidence separating code changes from
  equal-data small-commit aging and public maintenance effects, plus replicated
  fixed-state retention controls without a leak or process-bound claim.

- 2026-10-03: Recorded release-correction e2e, pinned-emulator and mixed HTTP
  diagnostic evidence. Required CI cells reject missing/ignored remote cases
  and skipped RustFS publication tests. The growing-data soak preserves the
  open performance, native-lifetime and E1 qualification boundaries.

- 2026-10-03: Corrected slow-client streaming, aggregate startup readiness and
  expired same-view scheduling. Read-only CLI consumers no longer acquire
  writer admission; completed read-only preflight refusals release explicitly.
  Every deployment validates achieved schema identity, with exact acknowledged
  correction bound into immutable input. Updated the affected runtime,
  availability, resource, admission and ledger requirements in place.

- 2026-10-03: Resource bounds records export/baseline transport ownership through
  retained chunks, clones and slices. The existing allowance now follows final
  buffer destruction; broader encoding, native-memory and RSS gates remain open.

- 2026-10-03: Extended T11.feed's existing HTTP continuation owner with a changed
  wide row, managed-Blob images and abandoned-delivery ownership/replay. The
  Qualification section distinguishes this transport evidence from the engine
  sort repair and remaining resource/native qualification.

- 2026-10-03: Implemented explicit load embedding capability diagnostics across
  HTTP and CLI output. Availability and Rollout record the implementation;
  Qualification records independent provider-request evidence and retains
  T11.embed's GQT format gate. Nullable loads remain successful and automatic
  generation stays outside scope.

- 2026-10-03: Implemented early listener and initial loading visibility. One owned
  batch retains at most four concurrent opens through shutdown; captured policy
  and loading-entry identity fence completion. Strict startup admits all graphs
  atomically, while default startup exposes ready siblings. Updated Scope and
  Availability; startup retries and native qualification remain unavailable.

- 2026-10-03: Implemented per-graph serving leases and bounded same-view
  transitions. Scope and baseline replaces the foundation-only availability
  description; Serving views records the narrow resumption capability;
  Availability replaces ready/blocked-only runtime projection; Initial E1
  qualification replaces the parked probe's no-runtime-integration description.
  The exact engine, schema, query and fixed bindings stay unchanged. Native
  settlement, online submission and schema/query activation remain unqualified.

- 2026-10-03: Record E0 implementation and bounded evidence in PR #849.
  Acceptance boundary replaces "Production v2 admission remains disabled";
  Durable offline deployment replaces its implementation shipping gate; CLI and
  conversion replaces "not currently available command forms". Initial E1
  qualification replaces "Online apply and ledger v2 admission remain
  unavailable", the version-1-only implementation claim and "E0 next implements".
  Rollout replaces "E0 adds" and its implementation-only gate; Unresolved
  questions item 2 and `blocked_on` retain backend qualification rather than
  implemented protocol work. The new evidence section records validation and its
  limits separately from implementation. E1, native reuse, existing-cluster v13
  conversion and Azure qualification boundaries are unchanged.

- 2026-10-03: Accepted E0's durable offline deployment protocol. Summary,
  Acceptance boundary, Scope, Observable behavior, Authority, Deployment,
  Availability, Compatibility, Qualification, Rollout and Unresolved questions
  replace the online-only proposed ledger with explicit offline v2: immutable
  inputs, sequence-plus-nonce original IDs, exact achieved bases, bounded completion
  reserves, current recovery authority and no-reset ledger conversion. Root CLI
  forms reuse `--cluster`; routine apply allocates and exposes its full ID before
  acceptance, without aliasing a later submission after pre-CAS loss. Retention
  high-water alone never proves a full ID accepted. Per-graph
  NotStarted/Started/Settled records distinguish never-invoked work from unknown
  publication. Stale no-ops refuse without fencing; foreign publication never
  silently becomes applied configuration. Strict intent version 2 and persisted
  neutral settlement fences supply a separately qualified non-publication proof. Lifetime exclusive admission protects
  evidence without tags; abandonment preserves the lock, and exact-ID handoff
  requires prior-owner/control-I/O quiescence. This accepts the protocol, not
  its implementation, backend qualification or native-resource bounds. E1's
  online mode, submission authority and activation witness remain gated.

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
