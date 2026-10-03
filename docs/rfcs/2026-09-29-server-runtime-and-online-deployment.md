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
  - "E0: offline ledger v2, bounded inputs/results, exclusive admission and data-preserving ledger conversion"
  - "E0: strict schema intents, settlement fences, current recovery authorization and process-death qualification"
  - "E1: online submission authority, activation witness and bounded observation"
  - "E1: same-engine transition qualification, coherent request bindings and bounded preparation/retirement"
  - "Existing-cluster rollout: qualified data-preserving offline conversion to storage v13"
  - "Azure E1: separately accepted lease-preserving submission authority and qualification"
---

# RFC: Server runtime and online deployment

## Summary

The server owns admitted writes through completion and reports exact outcomes
and graph availability. Durable offline schema/query deployment comes first;
online activation without restarting remains a separately qualified increment.
The engine publishes graph contents and their accepted schema together; the
existing cluster ledger owns deployment input and applied results. No new
transaction manager, content-recovery log or job queue is added.

This decision targets **v0.12 only**. Deploy the CLI, server and cluster tools as
one qualified v0.12 build. There is one wire contract, without older-client
adapters, legacy response aliases or a mixed-version support matrix. Version
refusal and data-preserving migration remain required.

The first deployment increment, E0, uses an explicit offline ledger mode and
requires the server to be stopped. It records immutable inputs, owns exact
schema outcomes and permits recovery without replay. E1 online deployment
drains affected graph requests while keeping the PID and listener alive.
Unaffected graphs continue serving. A cluster accepts one outstanding deployment; another submission is refused until its predecessor's
effects and required completion have settled.

## Acceptance boundary

The maintainer accepted the runtime foundations and durable-deployment direction
on 3 October 2026, with implementation and qualification proceeding separately.
The reviewed foundation implementation is [PR #844](https://github.com/ModernRelay/omnigraph/pull/844).
The operation, availability and prepared-schema contracts below are accepted;
`Unknown` establishes neither absence nor permission to replay.
This amendment accepts the offline ledger v2 protocol below, including exclusive
admission, strict publication intents and no-reset ledger conversion. Production
v2 admission remains disabled until E0 implementation and qualification pass.
Online activation, native runtime reuse, existing-cluster graph-format conversion
and Azure online submission retain their separate gates. Accepted design is not
evidence that any of those capabilities has shipped.

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
inventory, canonical roots, storage format, policy, credentials, providers, trust
bindings and external Blob policy stay fixed. E0 applies only to existing v13
graphs; graph creation/deletion, policy/provider edits and format conversion are
not deployment effects. Schema apply remains main-only and requires a single
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
| Offline schema/query deployment (E0) | Stop serving, acquire exclusive cluster admission, persist original outcomes and restart explicitly after settlement. |
| Online schema/query deployment (E1) | Keep PID/listener; affected admissions return 503 while admitted work settles and coherent bindings activate. |
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
serving process or an offline operation. E0 extends the existing persisted lock
to that lifetime and rechecks the ledger after acquiring it. This serializes
supported entry points; it does not fence arbitrary storage writers. Operators
exclude old binaries, raw/embedded openers and external maintenance for the whole
outstanding deployment, including recovery. A root unable to maintain that
exclusion cannot admit v2. Timeout, lock age or process ID never grants takeover.
Strict numeric schema publication fences only that publication; it does not
settle prior control-store I/O, native controls or reclamation. E1 retains one
designated mutation-capable serving process and needs its separate submission
and ownership protocol before online mode can be enabled.

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

## Durable offline deployment

E0 admits `version: 2, mode: offline` in the existing `__cluster/state.json`.
Use the existing content-addressed resource catalog and persisted cluster lock;
add no job store, retention tags or background executor. `mode: online`, unknown
versions/modes and `state.lock: false` refuse before graph effects. No failure
falls back to the old direct-apply/sweep path. The protocol below is accepted;
its implementation and backend qualification remain E0 shipping gates.

### Ledger v2

| Field | Meaning |
|---|---|
| `version`, `mode`, `ledger_id`, `state_revision`, `next_sequence` | Strict encoding/mode; immutable ledger incarnation (ULID); checked monotonic replacement and deployment counters. A canonical deployment ID is `<ledger_id>:<sequence>:<nonce_ULID>`; sequences start at 1. |
| `applied_revision` | Serving-sufficient resource projection, an exact-inventory `schema_contracts` map, and `result_revision`; this revision advances only when achieved configuration changes. Full convergence alone sets `config_digest`. |
| `outstanding` | Null or one immutable input/base/authority identity, per-graph prepared intents and mutable exact outcomes, optional persisted settlement intents, and reserved completion capacity. |
| `deployment_results` | Bounded terminal records: original ID/input/base, exact outcomes, achieved-result reference, initiating actor and recovery executors. No offline activation witness. |

Submission reserves **one outstanding deployment per cluster** under lock/CAS.
A competing new ID refuses busy; there is no queue, supersession or timeout that
clears the slot. Ordinary apply allocates a fresh nonce and the next sequence
under admission, emits the full ID before acceptance CAS and graph effects, and
returns it in the final result. An optional caller-preallocated ID must carry
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
distinct: E0 records no activation, and a settled partial result is a valid base
for a corrective deployment. No operator must edit the ledger to get past an
inactive or partially converged predecessor.

Each applied `schema_contracts` entry retains the engine-issued source hash,
accepted IR hash, schema identity domain and identity version for that graph.
Conversion captures these from one coherent accepted engine view; successful
schema outcomes advance them with the resource projection. Refusal and
not-attempted outcomes preserve them. Bind the same map in the achieved base,
reserve its encoded capacity, and compare the full identity before serving or
accepting query-only work, including after terminal receipt eviction. Matching
source text alone cannot adopt a recreated graph or different accepted IR.
Explicit authorized schema correction may establish a new exact contract;
status and boot never manufacture one from a matching text digest.

The immutable input contains normalized configuration semantics and exact schema
and stored-query bytes, including query deletions. Persist and verify their
content-addressed bytes before acceptance. Fixed inventory, roots, format and
runtime bindings must exactly match the applied projection. Validate the whole
effect set and every query against its intended accepted schema before the first
schema effect. Execution and recovery never reread mutable configuration files.
Current digest-based sweep/import/refresh cannot establish these outcomes and
must refuse to mutate a v2 ledger.

### Admission, authority and limits

The server and every supported CLI graph writer, writable opener, cleanup and
native-control command acquire the same exclusive cluster admission **before**
opening graphs, then reread version/mode/outstanding. Resolve membership for
both named graphs and direct paths inside a cluster; spelling a graph URI must
not bypass admission. The server retains ownership for its entire lifetime.
While `outstanding` exists, serving and other writers/cleanup refuse even after
a previous lock is removed; only the original executor or an explicitly
admitted reconciler can progress it. Read-only ledger lookup remains available.
The executor performs no sweep, cleanup or native reference mutation; schema
staging/publication must keep native automatic cleanup disabled. This exclusion
protects predecessor and candidate evidence until the terminal result CAS.

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
Version their approval receipt to bind ledger incarnation, achieved base, input,
effect set and principal; the old whole-ledger-CAS receipt cannot survive the
acceptance write. Candidate policy never authorizes itself, and no persisted
approval is a capability. Recovery/lookup check the **current** caller's required
authority: identity-authorized recovery requires existing `ConfigManage` plus
graph `SchemaApply` for settlement and `Read` for disclosed evidence, as needed.
No new policy action or read-only right permits fence publication. Recovery can
settle an earlier actor's outcome without impersonating that actor; preserve
the original receipt attribution and record the actual recovery executor
separately. This adds no mandatory policy to storage-owner clusters that do not
already bind one.

| Encoded control limit | E0 maximum |
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
intents, including their IDs/actors, against the ledger and result limits.
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
   validate queries, reserve completion capacity, and persist `outstanding`.
   Engine authority/preflight calls issue no schema/content effects. Today's
   read-write opener can still perform a local capability probe, so acquire
   admission before that open; do not describe the constructor as read-only.
2. Revalidate captured authority, then durably CAS that graph to `Started` with
   its exact intent immediately before invocation. Invoke only after that CAS
   is confirmed. Execute each original schema intent at most once in deterministic
   graph order; persist `Settled` with its own exact receipt or proved refusal
   before continuing. Uncertain effects stop later schema execution and keep the
   slot occupied. An uncertain original intent is never prepared again or replayed.
3. After a lost owner and prior-work quiescence, settle `NotStarted` graphs as
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
   graph contracts before allowing serving; restart explicitly from that projection.

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
longer matches that projection, report drift and refuse serving until explicit
authorized correction validates their agreement. Neither old applied digests nor
requested input can be relabeled as achieved to hide that mismatch.

A lost fence acknowledgement is reconciled by its persisted identity. Another
currently authorized recovery actor may finish that same settlement intent,
preserving its authored receipt while recording the actual executor in the
ledger. It never runs the original schema effect. Strict numeric CAS prevents a
late original schema publication after a proved occupied candidate; it does not
fence delayed control-store writes or make staged/native I/O settled. Prior-owner
quiescence and exclusive admission remain required independently.

### CLI and conversion

Reuse the existing global `--cluster ROOT` selector for root-addressed Core
commands. These extensions are the E0 implementation contract, not currently
available command forms. An explicit canonical storage URI permits recovery
independently of configuration discovery or current input files.

| Command | Behavior |
|---|---|
| `omnigraph --cluster ROOT cluster upgrade-ledger --writers-stopped` | Explicit conversion to ledger v2; validate existing applied resources and v13 graphs, then conditionally replace the ledger. |
| `omnigraph --cluster ROOT cluster status [--deployment-id ID]` | Bounded authorized lookup; return incarnation, next sequence, outstanding full ID and lock identity without graph opens or effects. |
| `omnigraph cluster apply --config DIR [--deployment-id ID]` | Validate/submit immutable input and execute offline under one admission owner; allocate and emit the original ID unless supplied. |
| `omnigraph --cluster ROOT cluster apply --deployment-id ID --writers-stopped` | Reconcile that original deployment only; never submit, use new files or replay schema effects. A terminal result is lookup-only. |
| `omnigraph --cluster ROOT cluster force-unlock LOCK_ID` | Exact-ID release under the operator-exclusion/quiescence procedure; outstanding evidence remains intact. |

Root-addressed forms do not load `--config`; the selectors are mutually exclusive.
`--writers-stopped` is the operator's explicit attestation that prior writers and
accepted graph/control I/O are quiescent and excluded. It supplies no technical
fence and does not replace lock admission or Azure's wrapper. Conversion and
reconciliation require it; read-only status does not. Routine apply does not need
manual status-and-copy, and may not allocate a new ID when resolving a previous
unknown result. Missing/expired original lookup never creates a replacement.
Managed remote runs keep their existing API; these flags do not manufacture a
remotely authenticated identity. Status needs no lock and cannot authorize a writer.

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

## Online deployment

E1 reuses the immutable-input/achieved-result protocol but is **not enabled by
E0**. `mode: online` refuses until its own encoding extension, authority and
qualification are accepted and implemented. No activation worker, polling loop
or active witness is implemented for offline mode. Offline apply while a server
holds admission refuses; it never silently takes over or restarts that server.

The online CLI only records validated/authorized immutable input; it never opens
a graph for writes, applies schema or sweeps. Submission needs a qualified path
that coexists with the serving owner's admission; E0's exclusive lock cannot
supply it. The designated server owns effects: close affected admissions, prove
the E1 transition, capture head preconditions, apply once and persist exact
achieved results before activation. Candidate and request bindings follow the
Serving views rules. Persist activation observations separately from achieved
results, including process incarnation, serving digests and activation attempt.
Resolve/fence every old activation attempt before releasing the outstanding
slot, so settled partial/inactive results allow corrective successors without a
late callback installing the predecessor. Boot never replays graph effects.
Azure online submission remains separately gated below.

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

Keep boot revision/digest as boot facts. E0 records achieved deployment results
behind authorized lookup and creates no active witness. E1's future active
witness identifies the achieved revision and per-resource digests actually
serving. Never infer activation from boot facts or successful apply. Status reads bounded snapshots outside blocked data lanes;
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
E0/E1 deployment or mixed-format serving feature. E0's explicit no-reset ledger
conversion is separate from graph-format conversion and wire admission; it
refuses unsupported graph formats and never bypasses this rollout gate.

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
| Permanently require restart for every schema deployment | E0 makes offline outcomes recoverable; E1 must remove unrelated serving interruption once qualified. |
| Refresh a handle or swap only queries | Does not establish coherent bindings or settle old users. |
| Separate apply process while serving | Atomic schema publication supplies no cross-process fence for native controls, reclamation or deployment ownership. |
| Require generic engine disposal/reuse before every E1 transition | Retaining the same engine may admit a narrower proof; every interfering effect and retained resource still needs qualification. |
| Queue and supersede deployments | Adds ordering states without a requirement for concurrent deployment submission. |
| Require historical overlap or durable data idempotency first | Couples live schema activation to separate storage and request protocols. |
| Add a deployment endpoint/job store or compatibility framework | Expands authority and support machinery beyond the single v0.12 contract. |
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
| T8.live | Same engine/PID/listener and coherent schema/query activation, including parked requests, native tails and cleanup/shutdown races; unaffected graph progress; submission-only CLI, writer exclusion, Azure refusal and crash safety. T8.history/retention are deferred outside scope. |
| T9 | Concurrent new submission refuses while one revision is pending, including across restart. Settled partial/inactive D1 permits corrective D2; stale-base submission refuses without effects; original lookup never replays. E0 additionally proves strict numeric publication/fence races and durable exclusive admission through recovery. |
| T10 | Declared bounds and completion/status reserves hold under saturation, slow/abandoned consumers and repeated failed deployments; reconcile counters with independent allocation/lifetime evidence. |
| T11 | Wide-feed progress, explicit ingestion-embedding diagnostics and bounded authorized status; unavailable 503 versus unknown 404, correct disclosure, liveness/readiness, bounded transient startup retries and permanent policy refusals. |

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

Online apply and ledger v2 admission remain unavailable. Exact version-1 engine
schema intents and receipts are implemented independently; they provide positive
publication evidence, not E0's strict version-2 terminal non-publication proof.
E0 next implements the accepted offline protocol and its gates above. Online
activation additionally requires owned, bounded native read/CPU lifetimes,
aggregate evidence-lookup bounds and coherent serving-view qualification.

## Rollout

| Increment | Deliverable | Shipping gate |
|---|---|---|
| A | A1 v0.12 HTTP admission; A2 own-publication merge receipts; A3 CLI outcomes | A1 follows [its accepted decision](2026-09-30-v012-http-admission.md); A2 is qualified under [Exact merge receipts](2026-09-30-exact-merge-receipts.md), including T1/T2; A3's initial typed-429 and qualified-HTTP-412 contract is qualified under [Owned server operations](2026-09-30-owned-server-operations.md) |
| B | Owned writes, read/stream accounting, drain and shared shutdown | [Owned server operations](2026-09-30-owned-server-operations.md) qualifies the task/body foundation. [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md) is accepted and partially implements query-child ownership and named aggregate write limits. Native settlement, completion reserves and runtime reuse remain unqualified under its T6/T10 gates. |
| C | Remaining initialization/native-control completion, owned maintenance and bounded transient startup retry | Atomic schema publication is landed; T4/T6/T7/T11 retain same-process progress and protected reclamation |
| D | Aggregate budgets, feed progress and embedding diagnostics | T10–T11 and workload qualification |
| E0 | Durable offline schema/query execution and exact recovery | Ledger bounds/reserves, exclusive admission and fail-closed abandonment, strict intent/fence publication, current authorization, real process-death matrix and explicit no-reset ledger conversion; qualify each admitted backend |
| E1 | Same-engine schema/query activation with one outstanding deployment | Qualified E1 transition proof, relevant B/C/D ownership and bounds, T8.live/T9 and durable ledger/crash gates; generic reuse still requires full B; Azure separately gated |

A and focused C/D repairs may proceed alongside B. The prepared schema
publication interface is implemented independently: exact authority and intent,
own-publication receipts, source-bound no-ops, contract-only publications and
read-only committed-result reconciliation. E0 adds strict intent version 2, neutral
settlement fencing and the [offline ledger protocol](#ledger-v2); protected
evidence and prior-owner/control-I/O quiescence are independent obligations.
E0 does not wait for E1's native reuse/activation proof and does not claim it.
Existing-cluster rollout also requires qualified offline v13 conversion.
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
2. Cluster/engine/CLI: E0's specified offline encoding, bounded control reads and
   completion reserves, strict intent/fence proof, exclusive entry-point coverage,
   recovery authorization and no-reset ledger conversion. Qualify its offline
   history-dependent lookup cost; E1 still needs an aggregate bound, coexistence
   of submission with serving admission, observation interval and active-witness
   extension. Existing digest-based sweep supplies none of those proofs.
3. Engine/server: qualify the native settlement mechanism and measured
   admission/completion profile in [Engine settlement and resource bounds](2026-10-01-engine-settlement-and-resource-bounds.md)
   before exposing a reuse capability. Separately prove E1's same-engine
   transition, including request binding, interference and retained native work;
   otherwise that transition remains unsupported.
4. Azure: lease-preserving online submission and backend qualification.
5. Storage/cluster: qualify data-preserving offline conversion of existing
   cluster roots to v13 before rollout, under the storage-upgrade owner.

## Decision log

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
