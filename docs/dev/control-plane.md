# Cluster control plane

The cluster control plane turns a local declarative bundle into a durable applied revision that servers can consume without the source checkout. It owns graph lifecycle, accepted schemas, stored queries, Cedar bundles, embedding-provider bindings, and external-Blob ingress policy. It does not execute row queries or mutations.

## Authority model

There are three distinct views:

1. **Desired configuration** — `cluster.yaml` and referenced files in an operator workspace.
2. **Applied ledger** — the durable state at the configured cluster storage root.
3. **Serving snapshot** — a validated projection of the applied ledger and content-addressed resources.

The desired bundle is input, not runtime authority. A server reads the applied revision; editing `cluster.yaml` changes nothing until apply. Server-owned apply activates
its achieved result in the same process; direct apply requires a subsequent
server start.

The CLI selects the storage root from its captured target. `--config` supplies
source only; an optional source `storage:` is an equality assertion against that
root. Relative assertions resolve against the source directory. Server capture
checks its authenticated canonical root, and the server independently validates
canonical identity under its admission. Graph roots are derived as
`graphs/<graph_id>.omni` beneath that root. Embedded/server callers retain their
existing configuration-owned root selection.

## Managed CLI sessions

A managed target supplies API, organization, authentication mechanism and optional
cluster identity. Folder context cannot select a destination. The CLI validates
operation applicability before credentials, HTTP or storage access. Legacy
lifecycle evidence has a separate preservation guard: an explicit target cannot
bypass pending/accepted custody migration.

The CLI's managed HTTP adapter obtains current access before each new request,
including status polls. A rotating credential pair lives only in the OS
keychain, independently of configuration directories and graph credentials.
Local origin-scoped locking and a persisted pending marker prevent duplicate
client exchanges; the provider owns credential rotation and session authority.
The API verifies provider access and maps it to a stable principal. An uncertain
exchange never authorizes a write replay.
The protocol and cache compatibility boundaries are specified in
[the identity and applied-policy RFC](../rfcs/2026-09-09-identity-credentials-and-applied-policy.md#provider-native-access-and-standard-clients); user behavior and limits
are in the [CLI reference](../user/cli/reference.md#managed-cluster-commands).

Ordinary managed deployment uses immutable `/plans` previews and scoped
`/deployments` delivery, separate from cloud lifecycle operations. The adapter
freezes a resolved source head before preview and sends a preview ID exactly
once for delivery. Polling observes the original product/native IDs and never
resubmits a write. Native execution, runtime activation and result archival
remain distinct facts; a saved result alone cannot prove an active runtime.
Cancellation targets only the initiating caller's unattempted queued delivery.

Read-only cluster member discovery uses the same selected organization and
authenticated adapter. It requires cluster `status` permission and validates the
response's cluster identity and bounded member records before rendering. The
directory reports explicit enrollment and cluster-specific control grants;
provider profile metadata is optional and applied Cedar remains the authority
for graph operations. The `cli_cluster` transport fixture owns the command's
scope, response validation and refusal tests.

## Durable layout

| Path | Role |
|---|---|
| `__cluster/state.json` | Applied ledger; v2 also owns outstanding deployment authority and bounded results |
| `__cluster/resources/` | Content-addressed queries, policies and immutable deployment bundles |
| `__cluster/recoveries/` | Legacy evidence read only during explicit conversion checks |
| `__cluster/approvals/` | Preserved legacy audit artifacts; no operational approval writer |
| `__cluster/lock.json` | Persisted control lock; v2 lifetime cluster admission |
| `graphs/<id>.omni/` | Derived graph roots managed through apply |

All stored control objects use the shared storage adapter. Filesystem replacement and object-store PUT/CAS details stay below that boundary; higher layers deal in versioned reads, conditional writes, and normalized roots.

The engine has no ordinary graph recovery sidecar: schema, table pins and
lineage publish together in `__manifest`. Ledger v2 stores prepared schema and
graph-birth and graph-deletion authority in one outstanding record. Legacy control evidence is
preserved for explicit conversion checks, never executed by a fallback sweep.

## Deployment ledger

`deployment.rs` and `deployment/execution.rs` own the sole v2 protocol. Fresh
apply bootstraps v2 directly; explicit stopped-writer conversion preserves an
existing v1 cluster's resources, identities and history. Conversion neither
resets graphs nor migrates their storage format. Unknown versions and
`state.lock: false` refuse. There is no offline/online mode field and no legacy
apply, import, refresh or approval executor.

The same explicit `cluster upgrade-ledger --writers-stopped` path removes
obsolete runtime fields from completed v2 receipts after validating their prior
shape and consistency. It preserves ledger identity, deployment sequence, exact
outcomes and applied resources with one conditional replacement. Normal decoding
uses that same validation to report `ledger_upgrade_required` for a recognized
prior receipt, never to accept converted state. Malformed or unknown state stays
invalid; outstanding work must finish under its originating build.

Apply manages graphs, schemas, queries, policies and provider/Blob bindings.
Runtime changes are validated before effects and activated with the achieved
revision. Retained roots, trust and storage format stay fixed. Removing a graph
from desired configuration deletes its exact managed root, including all branches,
retained history and managed Blob bytes. It does not follow external Blob
references or delete peer roots. Adoption, missing-root recreation and catalog
repair refuse. Normal engine open requires v14, v15 or v16; server HTTP requires v0.14.

The applied revision and every achieved base retain each graph's exact source/IR
digests and identity domain/version, captured coherently during conversion and
advanced by successful schema outcomes. Serving and preparation of every affected graph compare
this identity even after receipt eviction, rejecting a recreated graph with
identical schema text. Unaffected graph opens are skipped. Ordinary apply never
adopts foreign schema identity as an implicit correction.

Config capture reads each source once, preserving exact bytes by digest.
Immutable bundles and prepared engine intents are bounded before acceptance,
with ledger/result capacity reserved for completion. Bounded versioned storage
reads bind bytes and CAS token to the same GET; v2 writes compact JSON matching
the reservation calculation. Existing digest-named objects are verified before
reuse. The [configuration reference](../user/clusters/config.md#limits) owns the
numeric input/result limits; these are encoded control-state bounds, not a
native-I/O, full manifest-history scan or RSS bound.

The original ID is `<ledger_ULID>:<sequence>:<nonce_ULID>`; the nonce exists
before exposure and full ID plus input digest identifies resubmission. Acceptance
CAS consumes the exact next sequence. One outstanding deployment records each
graph as `NotStarted`, `Started` or `Settled`; the confirmed `Started` CAS
precedes schema, graph-creation or graph-deletion invocation. Schema and birth
recovery never replay the original invocation. They record `NotStarted` as not
attempted, or persist an engine-issued settlement intent before reconciling
`Started`. Strict numeric publication at the prepared base
plus one permits a neutral lineage fence or a qualified occupied-version proof
to establish nonpublication. Missing evidence remains unknown. Such proof
does not authorize adopting a foreign schema into the applied projection.

Deletion is recorded in that same outstanding record with its exact canonical
root and applied schema contract. `Started` is durable before any file removal.
Every deletion caller, including a direct storage owner, must satisfy an
installed graph policy's `read` and `schema_apply` gates before effects or recovery.
Accepted deletion cannot settle as refused or not attempted. An error leaves
it outstanding; reconciliation resumes only that root under exclusive admission,
including after its manifest is gone. Fresh deletion accepts an already-absent
root; a present root must match the applied contract before deletion starts.
After `Started`, recovery checks a readable manifest's graph lifetime rather than
its schema revision: partial removal can expose an older manifest of the same
graph. A replacement lifetime refuses, while an unreadable partial root remains
bound to its original intent.
Completion requires verified root absence before a `Deleted` outcome removes
the graph's applied resources and contract. A lost completion acknowledgement is
reconciled by original ID. No deletion queue, side record or retained-owner pool
is introduced, and matching source text never authorizes a replacement root.

A graph-birth token binds canonical root, exact source/IR contract and genesis
identity. Reconciliation accepts only that birth, not matching text. Under
explicit prior-owner quiescence, settlement may abandon an exact owned,
unpublished empty birth and leave a successor free to create a new identity.
Graph creation records its authenticated initiator in the deployment result;
engine genesis retains its existing actorless initialization contract. Later
schema commits record the actor in their engine receipt.
Foreign, committed or advanced roots are preserved and refused. The durable
claim remains until cleanup completes; interrupted cleanup is resumable.
A later creation may reuse a bounded empty local directory tree left by cleanup,
with engine preparation still proving an empty target. Files, symlinks and cloud
markers refuse; this does not adopt existing graph contents.

Terminal results advance only the achieved resources. Partial convergence is a
valid base for a corrective successor under the same owner or after a qualified
ownership handoff. Query-only work creates no
native commit and permits existing branches; schema apply retains its main-only,
single-live-branch restriction. Older completed receipts may expire, but consumed
sequences never execute again; after eviction even acceptance of a requested
nonce is unknown. Applied result revision is distinct from server activation.

`DeploymentCaller` preserves the storage-owner trust boundary and an optional
actor label. Authenticated identity callers recheck current cluster
`ConfigManage`; schema effects/recovery additionally require affected graph
`Read` and `SchemaApply`. Deployment status and results expose management metadata
under `ConfigManage`, without requiring data access on unrelated graphs.
Original authority stays immutable; recovery records the current executor while
preserving the authored engine receipt. The accepted
[server runtime RFC](../rfcs/2026-09-29-server-runtime-and-online-deployment.md)
owns the protocol rationale and qualification limits.

## Concurrency

Read-only `plan`, `observe` and deployment status neither acquire writer
admission nor change the ledger. Live observations are not execution or takeover
authority. Ledger publication uses the CAS read under the current owner.

V2 requires locking. `admission.rs` issues an opaque canonical-root/exact-lock-ID
capability only after acquiring that same lock and rechecking ledger/inventory.
Server, supported direct CLI graph writers, native controls and cleanup hold
it for their lifetime; outstanding work admits only exact-ID reconciliation.
Read-only CLI consumers instead validate canonical membership, captured ledger
CAS and accepted schema through `graph_read.rs`, without taking writer admission.
This is an observation, not a reclamation lease or a fence against later changes.
Dropping a guard or returning from writable work, including success, retains its
persisted lock. Completed read-only preflight refusals during new deployment,
conversion, admission construction or read-only server settings validation
explicitly release the exact lock. Recovery
of an accepted invocation retains admission even when its current preparation
refuses. Other explicit release requires qualified terminal graph and control
I/O; generic server drain does not establish that proof. Clean serving release
adds an explicit storage scope covering native and Lance writes, described below.
Other operator transfer requires stopped prior processes and accepted-I/O quiescence,
then exact-ID unlock with all admissions and other unlocks excluded until it
finishes. The backend has no conditional-delete guarantee; this is not a
distributed fencing lock. Older/raw/embedded writers outside the participating
doors remain operator-excluded across outstanding work and recovery. No tags or
additional retention store protect deployment evidence, and the executor never
runs cleanup.

Fresh S3 initialization has one narrower exception to exact-ID unlock:
`bootstrap_serving` executes only the initial cluster-scoped policy deployment
under a private retained bootstrap lock. It opens no graphs and returns a strict
16 KiB maximum receipt bound to the exact root, input, native ledger/result,
state CAS and original lock version. Native authority, graph or recovery
residue refuses initialization; unrelated opaque metadata grants no authority.
`claim_bootstrap_serving` validates those bindings and conditionally replaces
that same lock version with a new process-owned serving lock before capturing
the empty serving snapshot. It never deletes the lock or adopts an earlier
claim after an uncertain acknowledgement. Errors, cancellation and lost
responses retain exclusion. Already-issued bootstrap state updates use spent
CAS predecessors; immutable payload retries cannot alter achieved input. This
is only fresh zero-graph bootstrap, not native-I/O settlement or expiry-based
takeover. Later serving owners use clean release rather than replaying the
bootstrap receipt. The S3 lock key remains continuously present across normal
release and acquisition, so an older bootstrap create cannot recreate its owner.

Clean native shutdown closes logical admission, drains its existing operation
owners, then closes and drains an explicit `StorageIoScope` shared by control
adapters and graph Lance sessions. The wrappers count actual mutating requests
and multipart lifetimes. Physical settlement depends on the operation and the
key it names. The wrapper recognizes Lance 13's fresh `data/*.lance` files,
their `data/<file-key>/*.blob` sidecars, `_transactions/*.txn`,
`_deletions/*.arrow|*.bin`, flat files in `_indices/<uuid>/`, detached
`_versions/d*.manifest`, and numbered manifests created only if absent.
Late fresh files remain unreachable and a conditional create cannot replace
an existing publication. Such requests settle at the physical scope after
an error or cancellation, including multipart uploads. This does not prove
that a graph publication failed: the catalogue still owns the outcome of an
unacknowledged numbered-manifest publication. The commit handler every
dataset uses re-issues an ambiguous manifest request until the store answers
it and records whether a request was sent; the publisher marks a failure with
no request sent, or one the store refused, `Unpublished`, and only an outcome
its read-back cannot settle reaches the logical operation owner as uncertain.
Lance deletes an attempt's transaction file only when the handler calls a
missing version definitive, and only the commit's own request record may say
so: the catalog's per-commit handler answers from its probe, and a dataset's
shared handler never does. A manifest that lands late therefore finds its
transaction file on every open path; the transaction-delete hold below is the
served process's second line.

Refs, version hints, cluster control objects, unknown paths, if-match
replacements and recursive deletes remain conservative. A cancelled or
ambiguous request leaves its key unsettled even if a higher layer consumes the
error, and the scope logs its key and error. A later request to that key, or a
recursive delete over it, is refused before it is sent; other keys are
unaffected. The scope remembers at most `MAX_UNSETTLED_KEYS` (4096) keys and
past that refuses every mutation. Rename also accounts for removal of its
source. Single-attempt conditional conflicts and a delete answered "not found"
are settled refusals. `forbidden_apis.rs`
(`mutable_key_requests_are_registered_issue_1067`) registers every
unconditional request to a reused key in the engine, core, catalog, storage,
cluster, server and Azure admission crates with why its late landing is
harmless; a new one fails the guard until it is argued or made conditional or
fresh.

An unacknowledged manifest write may land after Lance reads it back as absent
and attempts to delete its transaction file. One sticky flag therefore
refuses transaction-file deletion throughout that scope, including rename
source deletion and recursive removal. This intentionally retains other
datasets' transaction files too, bounding the accounting state. Normal data
serving can continue, while transaction cleanup can be deferred until a
replacement scope. The collector must still establish its normal
reachability, authority and age predicates before reclaiming retained files;
replacing the scope is not permission to delete them. Definite conditional
conflicts do not set the hold. A successful multipart abort does not clear a
hold from an earlier unacknowledged completion.

Scoped stores hold two clients. Reads and fresh immutable writes go to one
with Lance's own bounded transport and AIMD retries; conditional requests and
mutable keys go to one that makes a single attempt, so their owners see every
attempt. Scoped control adapters keep three read retries within three minutes.
Local Lance uses its object-store file provider so direct file writes cannot
bypass the scope. Embedded defaults and Azure serving clients are unchanged; the
automatic scoped shutdown path applies only to local and S3 roots.

Only unique admission, successful startup and no unsettled key permit final
release. Original adapters remain permanently closed; a private single-attempt
client performs only exact-owner lock release under the same shutdown deadline.
S3 release CAS writes a unique version-2 released marker at the existing key;
ordinary acquisition CAS replaces it with a fresh held owner. Lost release
acknowledgement is an error, while its delayed CAS cannot affect a successor.
Local release uses awaited exact-owner deletion; automatic Azure release remains
unqualified and its prior retained-lock shutdown behavior is unchanged. Older
tools refuse the new released record. This supplies neither
distributed fencing nor generic runtime reuse.

On S3 roots a server's held lock is version 3 and carries `lease_seconds`
(`lease.rs`, `state_lock.rs`); a CLI writer or deployment holds an unleased
lock, because nothing would stop its writes when a lease fenced. Its owner
creates `__cluster/lease/<lock_id>/beat-<seq>.json` put-if-absent every third
of a lease, first before it admits work, and checks for
`__cluster/lease/<lock_id>/superseded.json` before each beat. The lease's
requests go through the root outside the owner's storage scope, on a client
with the backend's retries (`ClusterStore::lease_adapter`): a failed beat
neither unsettles the scope nor stops when the scope closes for a release.
A beat already present is the owner's own earlier request and counts from
the first send of that beat; an unanswered beat is sent again after a
quarter of a beat interval. A timer of its own fences the owner two thirds
of a lease after the first send of its last acknowledged beat, or at once on
a claim. The first beat races the same deadline counted from the send of the
lock's own write, because a successor may count silence from the moment the
record is visible; when it lapses, or the claim read after beat 0 finds a
claim, acquisition fails and the server does not serve. A fence fences the
owner's storage scope (`StorageIoScope::fence`) before it is reported: every
later request is refused, the parts and cleanup of an admitted multipart
upload included, and every admitted request is cancelled, so the owner sends
nothing after the fence however long its shutdown grace lasts. The server
then closes admission and exits 2 without releasing. A
serving acquisition that finds a version-3 lock watches the beat listing on
its own monotonic clock. A new beat refuses it. An empty listing starts the
silence and the first beat that appears restarts it, because the owner
writes its lock before its first beat lands; an empty listing after a beat
was seen refuses, like a new beat. A lease and a half of silence
lets it make sure the claim exists (a claim already there, from any
successor, only fences an owner that is somehow still running), then
replace `lock.json` by if-match on the version it first read. That if-match
alone decides the next owner: of two successors, or of a successor and the
owner's clean release (if-match on the version it read), exactly one
applies. A successor that stops between its claim and its if-match leaves
`lock.json` unchanged, so the next start takes it over the same way. A clean release stops
beating, deletes the lease directory, then releases as above. The lease
coordinates liveness only; requests a dead owner sent before it fenced are
fresh keys, conditional publications or audited mutable keys, which the
storage-outcomes RFC (2026-10-19) argues cannot corrupt state. Local roots
have no conditional update, so a successor could not replace `lock.json`
without racing an exact-owner delete the old owner already sent; local
locks, Azure locks and version-1 locks from older binaries carry no lease and
keep exact-ID unlock.

Do not bypass the cluster API with direct filesystem writes, edit `state.json`, or derive a second mutable inventory. Content digests and live observations are recomputed from the declared and durable authorities.

The distributed support boundary remains one mutation-capable writer process.
V2 admission adds cooperative exclusion on all backends without claiming native
I/O settlement. Azure writers also acquire the external admission lease through
`omnigraph-azure-admission`.

## Serving projection

`omnigraph-server` has one boot mode:

```text
--cluster <config-directory | file://root | s3://root | az://root>
```

A directory lets the server resolve the storage root from `cluster.yaml`; a URI reads the applied deployment artifact directly. There is no single-graph positional boot, `--target`, or independent graph add/remove API; inventory additions and destructive removals use deployment.

Serving captures ledger/resource digests, graph identity, expected contracts and validated policies before binding the listener. The captured registry initially contains loading or preflight-blocked entries, with the same captured policy later installed on the engine. One process-owned startup batch opens at most four graphs concurrently, checks accepted contracts, projects external-Blob policy, and validates stored queries. Each completed graph becomes ready or blocked through the process/registry admission boundary; shutdown prevents late activation and drains entered opens. Default startup admits healthy graphs individually. `--require-all-graphs` installs successful handles atomically only when every graph succeeds; any failure stops startup. A nonempty inventory with zero healthy graphs fails after the batch finishes. No attempt timeout, startup retry, reopen or native-settlement proof is introduced.

`POST /cluster/deployments` submits a bounded immutable bundle and original ID
to the running owner. The bearer actor needs applied cluster `ConfigManage` and
the graph permissions required by the shared executor. Graph-scoped data tokens
cannot deploy. One process gate owns execution and activation after caller
disconnect; no polling worker or second storage-writing CLI is involved.
POST returns 202 after the outstanding ledger CAS confirms acceptance. The CLI
then polls exact-ID GET; its wait deadline does not cancel the owned operation.
Exact GET returns the receipt, `active`, and `in_progress` derived from the
current process owner. That owner retains the deployment gate through pending
result delivery; its observation clears when the gate releases. A complete
receipt can precede activation. A missing receipt during preparation is not proof
of refusal, and an outstanding receipt
without a live owner requires recovery.

Exact receipt reads also permit the authenticated initiating actor recorded in
that receipt, even after management permission is revoked. This narrow observation
cannot disclose current inventory, next sequence or lock identity and grants no
effect permission. Aggregate status still needs current management permission.

`POST /cluster/plan` shares captured-input/current-policy validation and resource
diffs. It plans migrations against a coherent accepted schema view without graph
opens, a schema gate, writer admission, graph closure or ledger writes. The plan
reports its observed base and destructive root removal; physical execution checks
remain the post-drain executor's responsibility.

The controller validates captured input, current authorization and the serving
projection before closing admission, without opening affected graphs or acquiring
their schema gates. It resolves required provider secrets and validates runtime
bindings before effects. It reserves one batch transition, atomically closes affected graph
admission and drains request descendants, including held response bytes. Full
engine preparation then uses the retained live handles. The drain deadline does
not expire owned preparation, completion or activation; process shutdown remains
authoritative. A removal cannot delete storage
before this boundary; a pre-effect refusal can resume the unchanged predecessor
under a fresh epoch only when the existing abort proof permits it.
It executes under the server's lifetime admission, loads the achieved bindings,
and atomically installs exact contracts, query registries, immutable engine
runtime bindings, management policy and graph inventory. Rebound engine views
share the coordinator, write queues, schema and Lance sessions without reopening
storage. Unchanged siblings continue serving. Pre-effect refusals restore predecessor
views with new epochs. Uncertain effects use existing process containment.

Activation identity is installed in the same immutable registry snapshot as
serving bindings: canonical root, process incarnation, original ID/input digest,
achieved revision and config digest. The durable achieved receipt stays unchanged;
there is no second ledger write after activation. `GET /cluster/deployments/{id}`
reports active only for that exact current identity while its affected bindings
are ready under their installed schema contracts, deleted entries are absent,
and process admission remains open. An unrelated blocked graph does not invalidate
activation. Row commits do not invalidate unchanged deployment bindings. Partial convergence, an unavailable
affected binding and shutdown report inactive.

A normal configured boot captures the latest matching converged receipt under
root admission and establishes fresh runtime evidence only as all configured graph
opens complete successfully. Generic embedding constructors do not attest
deployment bindings.
Missing or expired receipt identity is not inferred from schema text or boot
revision alone. Old-ID submission validates immutable input and returns the
original record before graph closure; it cannot reinstall an old view. Boot
digests remain boot facts. Explicit OIDC public-admission refresh is separate below.

Protected graph and registry HTTP calls require the v0.14 contract header after
authentication and before graph resolution. CLI discovery and response validation
are specified by [wire compatibility](versioning.md#wire-compatibility); the
managed control-plane API and standard MCP/OAuth protocols are separate surfaces.
This admission check changes no ledger or serving-reload behavior.

Bearer authentication is a server concern. Cedar mutation enforcement also lives in the engine's `_as` APIs so embedded and CLI writers cannot bypass it. Cluster policy application publishes the bundles and bindings; it does not replace either enforcement layer.

The optional [offline signed-token trust](../rfcs/0053-offline-data-token-verification.md)
uses immutable public trust loaded before graph open. The Core's opt-in
root-bound serving snapshot supplies the canonical storage root from the same
resolution as the applied revision; the server checks that root against trust
without reading a managed identity marker. The verifier resolves
`principal:<sub>` and retains a private, verified version-2 identity. Permission
fields and unsupported token versions are refused. The common authorization
gate requires applied Cedar policy for protected operations, with no token-derived
graph/action ceiling. Static credential authority remains unchanged. Issuer
reachability is outside the serving request path. The profile boundary and
applied-policy ownership are described in
[Identity credentials and applied policy authorization](../rfcs/2026-09-09-identity-credentials-and-applied-policy.md).

`GET /graphs/discovery` accepts only the verified identity profile and returns
IDs and display names for the loading, ready and blocked graph inventory captured
at boot. It neither scans storage nor discloses availability, paths, policy,
schema, or query definitions. It needs no policy membership. The separate
`GET /graphs` uses the same registry with a `graph_list` gate and returns one
list containing runtime availability, sanitized failure and action. Neither inventory synthesizes graph
entries from the boot witness. Discovery still discloses no availability.

HTTP and MCP graph resolution atomically capture a serving view with its graph-epoch lease. A loading, blocked or
transitioning graph yields 503 only after graph `read` authorization on `main` or
management `graph_list` authorization; otherwise it remains undisclosed as 404.
An invalid graph policy or configuration cannot authorize the graph-read fallback.
Availability booleans describe the runtime, not a policy grant; route
authorization still applies to ready graphs.

The CLI's versioned keychain cache verifies endpoint and identity bindings before
replacement. Unsupported caches refuse without automatic replacement. The provider-native client acquires missing/expired identity
credentials before an operation using the selected managed target.
A server target keeps the
existing catalog unless `graphs list --discovery` is requested; the CLI never
infers routing from an arbitrary bearer token's unverified shape.

A replica reports what it booted from on `GET /readyz` (RFC 0049): the
applied `config_digest` as `booted_serving_digest`, the ledger revision and
CAS, and registry, ready, loading and blocked graph counts. `served_graph_count`
counts every registry entry; closed transitions contribute to the blocked count.
Status is `loading`, `serving`, `degraded`, `blocked` or `draining`;
readiness requires completed startup, a ready graph or valid empty inventory,
and open admission. Healthy graphs can serve directly while siblings load,
but an aggregate readiness probe remains 503 until loading finishes.
Graph IDs stay on the authenticated catalog routes under their respective
disclosure contracts. Status reads registry snapshots without graph/storage I/O;
shutdown makes every entry `stopping` and both availability flags false, retaining
any startup failure category.
Graceful shutdown is bounded by one deadline (`--shutdown-grace-seconds`,
default 25), kept by a thread and armed
by a listener installed before graphs open, after which the process exits 2
without claiming success.

Admitted HTTP writes run as registered server-owned operations. Their immutable
inputs, trusted actor, Session and capacity reservations survive a lost response
waiter. Merge and optional source deletion share one owner. Registration and
permanent admission closure have one synchronous ordering boundary; shutdown
waits for admitted writes and registered read bodies/server producers after HTTP
connections finish. After bounded body collection, read handlers also run in
owned tasks; losing the HTTP waiter leaves their engine future, read observer
and input reservation alive. MCP tool execution retains its own concurrency
permit after its caller cancels or reaches the response deadline. Completed
results occupy one observed delivery slot until consumed or dropped. A known
write retains its reservations in that slot: consuming it releases write
capacity before the handler responds, while abandoning it destroys the output
before releasing ownership. A read
error or panic does not trigger write uncertainty. An owned write whose outcome is unknown for a storage reason the engine scopes,
a publication in doubt (its version is created only if absent, so a late
landing precedes the branch's next publication or never happens) or
an opaque storage or I/O failure (whose mutable keys the scope holds unsettled),
reports its outcome and releases its reservations; admission stays open. A
write panic, a lost owner, `RecoveryRequired`, an opaque DataFusion failure or
an uncertain deployment activation closes admission for every graph and
signals the same bounded process shutdown, retaining unresolved reservations
until exit. The engine's completion evidence decides:
a failure it proves left the graph unchanged, before any effect or after
detached effects no manifest names (`BeforeEffect`, `Unpublished`), is
not itself a reason for logical containment, whatever status it maps to,
including a 500. Mutation, load, Blob write and merge mark opaque errors (storage, I/O, DataFusion, internal) at their pre-publication
steps, alongside the table store's staged-write and detached-commit sites,
and the publisher marks every failure whose manifest request cannot land.
A contained write logs the failure's redacted class before admission closes.
A typed refusal keeps its shape. Other opaque schema and maintenance
preparation errors without completion evidence still fail closed as uncertain. Proven publication followed by
`RecoveryRequired`, and uncertain publication itself, are never downgraded.
Read/write body and response lanes have independent capacity; bodyless reads
consume only read observers. Once HTTP connections and the remaining known
logical owners finish, uncertain completion exits 2 immediately, with the original
watchdog as the upper bound. This does not establish native-I/O settlement.

Served export and baseline transport divide each existing process reservation
into three outstanding frame credits and one sequential producer slot. The
`Bytes` owner holds its payload, frame credit and process lease through the last
clone or slice. Both routes share this sender; the baseline terminal record is
encoded into one bounded pending chunk only after snapshot production succeeds.
Only initial process-capacity admission has a short timeout. An admitted
producer waits for a frame credit or response closure, including the terminal
baseline cursor. Slow readers retain their bounded reservation until delivery
or disconnect; no short per-frame deadline truncates a valid stream. A producer
hands its failure to the sender, which ends the body with a constant error and
logs the failure's class, never its message: the HTTP trace layer logs a body
error's text, and an engine message can hold object URIs or credentials.
[Deployment limits](../user/deployment.md#admission-limits) distinguish this
transport allowance from engine encoding and native memory.

A same-view transition can close one graph while other graphs keep serving.
Preparation reserves bounded transition capacity before closing admission.
Its graph lease follows body collection, owned execution, producers, retained
results and yielded transport bytes; disconnect or MCP response expiry cannot
release surviving owners. After those logical owners finish, the transition
can resume only the exact existing engine, schema, queries and fixed bindings
under a fresh epoch. Close, resume and shutdown share a synchronous ordering
boundary. Expiry or an abandoned closed transition leaves that graph unavailable;
there is no automatic reopen or retry. Only its scheduling record is retired,
so other ready graphs can transition. The closed registry entry and descendants
retain the engine and resource charges; no native disposal or lock release is
implied. Bounded inventory limits retained closed views, and obsolete tickets
cannot affect a subsequent candidate. Status reports `transitioning` with
`wait_for_transition`, without a finite `Retry-After` promise.

The embedding entry point is `AppState::prepare_same_view`. It binds the actual
process runtime; callers cannot substitute a new runtime to bypass stopping.
It grants no schema/query replacement or deployment authority. The internal
batch capability used by `deployment.rs` validates exact achieved schema/query
bindings, graph additions and completed graph deletions under the same
shutdown/attempt fence. Inventory removal requires an exact durable `Deleted`
result for a graph closed by this transition; it is installed atomically with
surviving bindings and management policy. There is no retired-owner cache,
readoption path, root-recreation shortcut or public arbitrary-view replacement
endpoint. Interrupted deletion keeps affected admissions closed; it never
restores a partially deleted predecessor.

These registrations account for server lifetimes, not universal storage-I/O
settlement. A joined future or zero operation counter cannot authorize runtime
replacement, cleanup or replay. Schema activation has a narrower proof: it
retains the same engine and immutable read-tail owners, finishes interfering
writers/controls, and never reclaims old inputs. Native accepted-I/O settlement,
protected completion capacity, generic engine reuse and a process-RSS bound
remain unqualified. The accepted
[Owned server operations](../rfcs/2026-09-30-owned-server-operations.md) decision
owns this boundary and its evidence; the [workload module](../../crates/omnigraph-server/src/workload.rs)
owns admission limits.

### Public embedding APIs

Ordinary callers use `read_serving_snapshot` or
`read_serving_snapshot_from_storage`, `load_server_settings`, and `serve`.
`ServingSnapshot` and `ServerConfig` contain their ordinary public fields;
trust-disabled boot does not add root canonicalization solely for signed
credentials.

V2 serving acquires lifetime admission before reading its boot snapshot and
opening graphs. Settings retain that opaque ownership in `ServerConfig`; a
hand-built configuration cannot use a deployment/reconciliation capability as
serving authority. Read-only snapshot APIs alone do not grant serving admission.

Managed callers use `read_root_bound_serving_snapshot` or its `_from_storage`
counterpart to obtain an opaque `RootBoundServingSnapshot`. Its snapshot and
canonical-root accessors refer to the same opened store. Do not reconstruct
that binding by reopening a caller-supplied path. Server embedders use
`load_server_settings_with_data_token_trust` and
`serve_with_data_token_trust`; `ManagedServerConfig` keeps the configuration
and validated trust together. `with_shutdown_grace` changes only the shutdown
bound. A failed managed load must not be retried through ordinary `serve`.
The binary's `--data-token-trust FILE` selects this managed path.

`ResolvedActor` is a public identity projection, not proof of authentication.
Middleware and protected handlers retain `AuthenticatedActor`, whose private
state carries verified claims. `DataTokenTrust::verify_at`
returns the identity projection for existing callers;
`verify_authenticated_at` returns the opaque authenticated result used by the
server. Public actor construction cannot grant signed-token permissions.
`IdentityTokenClaims` is the strict signed identity type; its read-only accessor
on `AuthenticatedActor` exposes only verified claims.

Policy embedders must handle `PolicyAction::ConfigManage`,
`PolicyResourceKind::Cluster`, and `PolicyEngineKind::Cluster`. These public
enums are exhaustive, so an external exhaustive match must add the relevant
arm when upgrading. Preserving existing constructors and direct storage-holder
behavior does not remove that source compatibility requirement.

In-process hosts that assemble `AppState` and call its existing
`with_data_token_trust` method continue to own their graph/root binding. Use
the managed settings loader when the library should validate the applied
snapshot and trust binding together.

## Azure boundary

`az://container/prefix` uses the same control-object and graph-root model as
local and S3 storage. Code paths, Azurite integration, and a managed-identity
smoke deployment are qualified; the adversarial live-Azure matrix is still
pending. Treat Azure as a qualification preview.

Every mutation-capable Azure server, apply job, direct writer, and maintenance process must run through `omnigraph-azure-admission`. The admission crate may depend downward on shared storage; storage, engine, cluster, server, and CLI must not depend upward on it.

## Owners

- Configuration, diffing, deployment, recovery, and serving projection: `crates/omnigraph-cluster/src/`.
- Shared local/S3/Azure control-object storage: `crates/omnigraph-storage/`.
- Cluster-only boot and graph quarantine: `crates/omnigraph-server/src/settings.rs`.
- Operator commands and addressing: `crates/omnigraph-cli/`.
- Azure lease wrapper: `crates/omnigraph-azure-admission/`.

The public operating loop and configuration schema live in [Operating a cluster](../user/clusters/index.md) and its [configuration reference](../user/clusters/config.md).

## OIDC resource identity and MCP

The optional `--oidc-identity-trust FILE` profile validates the same canonical
serving root before opening graphs. Public JSON binds exact issuer, audience,
organization, account, cluster and incarnation, plus RSA keys and explicit
subject-to-stable-principal admission. Cedar remains the graph/schema authority.
The server reads no provider secret and performs no request-time network calls.
An external publisher refreshes a bounded, revisioned snapshot; local checks
run every five seconds and access refuses at the original 300-second snapshot
or token expiry. The exact format and limits are in
[Identity credentials and applied policy authorization](../rfcs/2026-09-09-identity-credentials-and-applied-policy.md#provider-native-access-and-standard-clients).

This profile enables `/.well-known/oauth-protected-resource` and `/mcp`. MCP
uses the official Rust SDK and the existing stored-query/discovery handlers.
The initial tools are read-only; invocation checks the stored query kind and
the same Cedar policy as HTTP. A registered resource or listed tool is never
a permission grant. Static-token deployments and the native signed-token
profiles retain their existing behavior.
