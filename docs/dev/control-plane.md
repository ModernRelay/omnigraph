# Cluster control plane

**Audience:** contributors changing cluster deployment, admission or serving
**Authority:** the deployment ledger, cluster admission and lock protocol,
serving projection and their support boundaries; operator behavior is in
[Operating a cluster](../user/clusters/index.md) and
[deployment](../user/deployment.md), the reasons in the linked decision
records, and storage-request settlement and the lease mechanics in the
`omnigraph-storage` and `omnigraph-cluster` doc comments named below

The cluster control plane turns a local declarative bundle into a durable applied revision that servers can consume without the source checkout. It owns graph lifecycle, accepted schemas, stored queries, Cedar bundles, embedding-provider bindings, and external-Blob ingress policy. It does not execute row queries or mutations.

## Authority model

Server telemetry is separate from deployment authority; its export rules are
the module doc of `crates/omnigraph-server/src/telemetry.rs`, and its
configuration is in the [observability guide](../user/operations/observability.md).

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

Managed profiles, targets, lifecycle custody, credential rotation, managed
deployment delivery and member discovery are the module docs of
`crates/omnigraph-cli/src/managed.rs` and its `managed/` submodules
(`lifecycle/pending.rs`, `auth/coordination.rs`, `deployment.rs`,
`members.rs`). User behavior and limits are in the
[CLI reference](../user/cli/reference.md#managed-cluster-commands) and the
[operator reference](../user/cli/targets.md); the reasons in the
[operator connections](../decisions/2026-10-20-operator-connections-and-credential-providers.md)
and [identity](../decisions/2026-09-09-identity-credentials-and-applied-policy.md)
records.

## Durable layout

| Path | Role |
|---|---|
| `__cluster/state.json` | Applied ledger; v2 also owns outstanding deployment authority and bounded results |
| `__cluster/resources/` | Content-addressed queries, policies and immutable deployment bundles |
| `__cluster/recoveries/` | Legacy evidence, never executed or swept: serving quarantines a graph a sidecar names, plan/status/observe warn, an identity-authorized plan refuses, and ledger conversion requires it resolved |
| `__cluster/approvals/` | Preserved legacy audit artifacts; no operational approval writer |
| `__cluster/lock.json` | Persisted control lock; v2 lifetime cluster admission |
| `__cluster/lease/<lock_id>/` | Heartbeats and the takeover claim of a leased S3 serving lock |
| `__cluster/operations.json` | The operation record: each admission scope's active operation, unresolved publications, bounded terminal results, the eviction mark and each kind's state per graph |
| `graphs/<id>.omni/` | Derived graph roots managed through apply |

All stored control objects use the shared storage adapter. Filesystem replacement and object-store PUT/CAS details stay below that boundary; higher layers deal in versioned reads, conditional writes, and normalized roots.

Ledger v2 stores prepared schema and graph-birth and graph-deletion authority
in one outstanding record.

## Operation record

`__cluster/operations.json` (`omnigraph_cluster::OperationRecord`) holds
recorded operations such as served `optimize`. Its states, admission order,
bounds, eviction mark and settlement by history are the doc comments of
`crates/omnigraph-cluster/src/operations.rs`, and the server's single writer,
gates and cancellation those of `crates/omnigraph-server/src/recorded_operations.rs`.
A graph deletion settles the lifetime's non-terminal records in
`complete_graph_deletion` (`OperationRecord::settle_graph_deleted`).

## Deployment ledger

`deployment.rs` and `deployment/execution.rs` own the sole v2 protocol.
Unknown versions and `state.lock: false` refuse. There is no offline/online
mode field and no legacy apply, import, refresh or approval executor. Ledger
conversion and `cluster upgrade-ledger --writers-stopped` are documented on
`upgrade_deployment_ledger` in `crates/omnigraph-cluster/src/deployment/execution.rs`.

Apply manages graphs, schemas, queries, policies and provider/Blob bindings.
Runtime changes are validated before effects and activated with the achieved
revision. Retained roots, trust and storage format stay fixed. Removing a graph
from desired configuration deletes its exact managed root, including all branches,
retained history and managed Blob bytes. It does not follow external Blob
references or delete peer roots. Adoption, missing-root recreation and catalog
repair refuse. Supported storage formats and the HTTP contract version are in
[versioning](versioning.md).

Schema identity retention across receipts is the module doc of
`crates/omnigraph-cluster/src/deployment.rs`.

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
reconciled by original ID. There is no deletion queue, side record or retained-owner pool, and matching source text never authorizes a replacement root.

The graph-birth claim and its cleanup are the module doc of
`crates/omnigraph-cluster/src/deployment/execution.rs`.

Terminal results advance only the achieved resources. Partial convergence is a
valid base for a corrective successor under the same owner or after a qualified
ownership handoff. Query-only work creates no
native commit and permits existing branches; schema apply retains its main-only,
single-live-branch restriction. Older completed receipts may expire, but consumed
sequences never execute again; after eviction even acceptance of a requested
nonce is unknown. Applied result revision is distinct from server activation.

Caller authority is documented on `DeploymentCaller` in
`crates/omnigraph-cluster/src/deployment.rs`. The
[server runtime decision](../decisions/2026-09-29-server-runtime-and-online-deployment.md)
owns the protocol rationale and qualification limits.

## Concurrency

Read-only `plan`, `observe` and deployment status take no writer admission
and change no ledger ([control-plane seams](../decisions/0049-control-plane-seams.md)).
Ledger publication uses the CAS read under the current owner.

V2 requires locking. `admission.rs` issues an opaque canonical-root/exact-lock-ID
capability only after acquiring that same lock and rechecking ledger/inventory.
Server, supported direct CLI graph writers, native controls and cleanup hold
it for their lifetime; outstanding work admits only exact-ID reconciliation.
Read-only CLI consumers instead validate canonical membership, captured ledger
CAS and accepted schema through `graph_read.rs`, without taking writer admission.
This is an observation, not a reclamation lease or a fence against later changes.
Dropping a guard or returning from writable work, including success, retains its
persisted lock unless the owner releases it explicitly as described here. Completed read-only preflight refusals during new deployment,
conversion, admission construction or read-only server settings validation
explicitly release the exact lock. Recovery
of an accepted invocation retains admission even when its current preparation
refuses. Other explicit release requires qualified terminal graph and control
I/O; generic server drain does not establish that proof. A direct deployment
(`apply_direct_bundle`) runs its control and graph writes in one explicit
storage scope and, when the deployment completes converged, releases through
the same scope settlement as clean serving release; Azure, a non-converged or
outstanding result, and an uncertain scope retain admission. Clean serving release
adds an explicit storage scope covering native and Lance writes, described below.
Other operator transfer requires stopped prior processes and accepted-I/O quiescence,
then exact-ID unlock with all admissions and other unlocks excluded until it
finishes. The backend has no conditional-delete guarantee; this is not a
distributed fencing lock. Older/raw/embedded writers outside the participating
doors remain operator-excluded across outstanding work and recovery. No tags or
additional retention store protect deployment evidence, and the executor never
runs cleanup.

Fresh S3 initialization is the one exception to exact-ID unlock; the
bootstrap receipt and its claim are the module doc of
`crates/omnigraph-cluster/src/deployment/bootstrap.rs`.

Clean native shutdown closes logical admission, drains its existing operation
owners, then closes and drains an explicit `StorageIoScope` shared by control
adapters and graph Lance sessions. How the scope settles each request by the
key it names is the doc comment of `crates/omnigraph-storage/src/scoped.rs`.
The catalogue owns the outcome of an unacknowledged numbered-manifest
publication. The commit handler every
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

The scope logs the key and error of a request it leaves unsettled. The
storage adapter's `write_text_if_absent` and `write_text_if_match` resolve
their answers by `omnigraph_storage::conditional`, the one policy the
manifest commit handler uses too; the re-issue and read-back rules are in the
doc comments of `conditional.rs`, `scoped.rs` and those two methods. Equal
bytes can be read as this request's only because every caller's `contents`
names its writer: a lock id, a lease sequence in the owner's directory, an
init claim's attempt id. The prepared graph-create claim names its apply for
that reason. A state or content-addressed object whose bytes are the effect is
the same write when the bytes match. Local filesystem puts finish before
they answer and publish last (`LocalFileSystem::new()`, fsync off), so a local
error is a refusal there. `forbidden_apis.rs`
(`mutable_key_requests_are_registered_issue_1067`) registers every
unconditional request to a reused key in the engine, core, catalog, storage,
cluster, server and Azure admission crates with why its late landing is
harmless; a new one fails the guard until it is argued or made conditional or
fresh.

The sticky transaction-delete hold and the two-client routing of a scoped
store are in the `scoped.rs` doc comment. Normal data serving continues
under the hold, while transaction cleanup can be deferred until a replacement
scope. A successful multipart abort does not clear a hold from an earlier
unacknowledged completion. Scoped control adapters keep three read retries
within three minutes. Local Lance uses its object-store file provider so
direct file writes cannot bypass the scope. Embedded defaults and Azure
serving clients are not scoped; the automatic scoped shutdown path applies
only to local and S3 roots.

Final lock release is documented on `ClusterAdmission::release_after_settlement`
(`admission.rs`) and `release_settled_lock` (`state_lock.rs`) in
`crates/omnigraph-cluster/src/`. Automatic Azure release remains unqualified,
and Azure shutdown retains the lock.

On S3 roots a server's held lock is version 3 and carries `lease_seconds`;
the heartbeat, self-fence, watch, claim and if-match takeover protocol is the
doc comment of `crates/omnigraph-cluster/src/lease.rs`. A CLI writer or
deployment holds an unleased lock, because nothing would stop its writes when
a lease fenced. The lease's requests use `ClusterStore`'s `lease_adapter`, so
a failed beat neither unsettles the scope nor stops when the scope closes for
a release. An unanswered beat is sent again after a quarter of a beat
interval. When the first beat's deadline lapses, or the reads after beat 0
find a claim or a lost record, acquisition fails and the server does not
serve. A fence fences the owner's storage scope (`StorageIoScope::fence`,
`StorageIoScope::fence_at`) before it is reported; the server then closes
admission and exits 2 without releasing. A watching successor's empty listing
starts the silence and the first beat that appears restarts it, because the
owner writes its lock before its first beat lands; an empty listing after a
beat was seen refuses, like a new beat. A clean release stops beating,
deletes the lease directory, then releases as above. A request a dead owner
sent to a mutable key (a ref, the version hint, a control object, a delete)
before it fenced can still land after a takeover; the owner's storage scope
only records its outcome as unknown, in that process. Local locks, Azure
locks and version-1 locks from older binaries carry no lease and keep
exact-ID unlock.

Do not bypass the cluster API with direct filesystem writes, edit `state.json`, or derive a second mutable inventory. Content digests and live observations are recomputed from the declared and durable authorities.

One mutation-capable writer process per cluster is the supported topology
(`G2`). V2 admission adds cooperative exclusion on all backends without claiming native
I/O settlement. Azure writers also acquire the external admission lease through
`omnigraph-azure-admission`.

## Serving projection

`omnigraph-server` boots only from `--cluster`, a configuration directory or a
storage-root URI ([server cluster boot](../decisions/0005-server-cluster-boot.md)).
There is no independent graph add/remove API; inventory additions and
destructive removals use deployment.

Serving captures ledger/resource digests, graph identity, expected contracts and validated policies before binding the listener. The captured registry initially contains loading or preflight-blocked entries, with the same captured policy later installed on the engine. One process-owned startup batch opens at most four graphs concurrently, checks accepted contracts, projects external-Blob policy, and validates stored queries. Each completed graph becomes ready or blocked through the process/registry admission boundary; shutdown prevents late activation and drains entered opens. Default startup admits healthy graphs individually. `--require-all-graphs` installs successful handles atomically only when every graph succeeds; any failure stops startup. A nonempty inventory with zero healthy graphs fails after the batch finishes. There is no attempt timeout, startup retry, reopen or native-settlement proof.

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
there is no second ledger write after activation. `GET /cluster/deployments/{deployment_id}`
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

Authentication and policy enforcement are `E6`. Cluster policy application
publishes the bundles and bindings; it does not replace either enforcement
layer.

Signed data tokens are verified offline against public trust loaded before
graph open ([offline data-token verification](../decisions/0053-offline-data-token-verification.md),
[identity credentials and applied policy](../decisions/2026-09-09-identity-credentials-and-applied-policy.md)).
The Core's opt-in root-bound serving snapshot supplies the canonical storage
root from the same resolution as the applied revision. `GET /graphs/discovery`
and `GET /graphs` read the same serving registry, as last installed by boot or
a deployment activation; neither scans storage.

HTTP and MCP graph resolution atomically capture a serving view with its graph-epoch lease. A loading, blocked or
transitioning graph yields 503 only after graph `read` authorization on `main` or
management `graph_list` authorization; otherwise it remains undisclosed as 404.
An invalid graph policy or configuration cannot authorize the graph-read fallback.
Availability booleans describe the runtime, not a policy grant; route
authorization still applies to ready graphs.

The CLI's versioned keychain cache verifies endpoint and identity bindings before
replacement. Unsupported caches refuse without automatic replacement. The provider-native client acquires missing/expired identity
credentials before an operation using the selected managed target.
Both managed and standalone targets request the
ordinary graph catalog unless `graphs list --discovery` is requested; the CLI never
infers routing from an arbitrary bearer token's unverified shape.

`GET /readyz` and the shutdown deadline are specified in
[control-plane seams](../decisions/0049-control-plane-seams.md) and
[deployment](../user/deployment.md). Shutdown makes every registry entry
`stopping` and both availability flags false, retaining any startup failure
category.

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

Served export and baseline bodies use the bounded transport of
`crates/omnigraph-server/src/export_transport.rs`.
[Deployment limits](../user/deployment.md#admission-limits) distinguish this
transport allowance from engine encoding and native memory.

A same-view transition can close one graph while other graphs keep serving.
Its leases, resume and expiry rules are the module doc of
`crates/omnigraph-server/src/registry.rs`; the embedding entry point is
`AppState::prepare_same_view`, and deployment inventory removal is
`GraphRegistry::activate_deployment`.

These registrations account for server lifetimes, not universal storage-I/O
settlement. A joined future or zero operation counter cannot authorize runtime
replacement, cleanup or replay. Schema activation has a narrower proof: it
retains the same engine and immutable read-tail owners, finishes interfering
writers/controls, and never reclaims old inputs. Native accepted-I/O settlement,
protected completion capacity, generic engine reuse and a process-RSS bound
remain unqualified. The accepted
[Owned server operations](../decisions/2026-09-30-owned-server-operations.md) decision
owns this boundary and its evidence; the [workload module](../../crates/omnigraph-server/src/workload.rs)
owns admission limits.

### Public embedding APIs

The embedding entry points are documented on the items themselves:
`read_serving_snapshot`, `read_root_bound_serving_snapshot` and
`RootBoundServingSnapshot` in `crates/omnigraph-cluster/src/serve.rs`;
`load_server_settings` and `load_server_settings_with_data_token_trust` in
`crates/omnigraph-server/src/settings.rs`; `ServerConfig`,
`ManagedServerConfig`, `serve_with_data_token_trust` and
`AppState::with_data_token_trust` in `crates/omnigraph-server/src/lib.rs`;
`DataTokenTrust::verify_at` and `verify_authenticated_at` in `data_tokens.rs`;
`ResolvedActor` and `AuthenticatedActor` in `identity.rs`.

Policy embedders must handle `PolicyAction::ConfigManage`,
`PolicyAction::Maintain`, `PolicyAction::Reclaim`,
`PolicyResourceKind::Cluster`, and `PolicyEngineKind::Cluster`. These public
enums are exhaustive, so an external exhaustive match must add the relevant
arm when upgrading. Preserving existing constructors and direct storage-holder
behavior does not remove that source compatibility requirement.

## Azure boundary

`az://container/prefix` uses the same control-object and graph-root model as
local and S3 storage. Azure writes are a preview (`G4`).

Every mutation-capable Azure process runs through `omnigraph-azure-admission`;
its dependency rule is the crate doc of `crates/omnigraph-azure-admission/src/lib.rs`.

## Support boundaries

- The server is cluster-only and ledger v2 is the sole operational protocol.
  Server-owned apply activates schemas, queries, policies, provider/Blob bindings
  under existing writer admission.
  Runtime views share engine authority and retain immutable request bindings.
  Direct apply bootstraps or deploys while serving is stopped. Legacy ledger
  conversion is explicit and preserves data; no v1 executor remains. Removing
  a graph declaration authorizes deletion of its exact managed root and all
  retained history, after affected admission closes and admitted work drains.
  The existing ledger records deletion intent before effects and owns resumption
  through verified root absence. Adoption and missing-root recreation remain
  unsupported; unexpected roots and schema drift refuse rather than infer
  identity from matching text.
  One outstanding durable authority owns completion/recovery. Generic drain
  does not prove native-I/O settlement. Clean server release additionally closes
  its explicit storage scope, drains accepted backend mutations and refuses
  sticky uncertainty; a converged direct apply releases by the same proof.
  S3 release/reacquisition uses the same lock key by CAS.
  Excluding raw and older writers remains an operator obligation.
- Schema apply retains its main-only, single-live-branch restriction and
  process-local gate ordering. Its contract and table references publish
  together; the removed file-installation protocol supplies no distributed
  lock. Branch creation/deletion and cleanup retain their existing
  single-writer-process control boundary.

## Owners

- Configuration, diffing, deployment, recovery, and serving projection: `crates/omnigraph-cluster/src/`.
- Shared local/S3/Azure control-object storage: `crates/omnigraph-storage/`.
- Cluster-only boot: `crates/omnigraph-server/src/settings.rs`; serving-snapshot
  graph quarantine: `crates/omnigraph-cluster/src/serve.rs`.
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
[Identity credentials and applied policy authorization](../decisions/2026-09-09-identity-credentials-and-applied-policy.md).

The server mounts `/mcp` whenever it has a bearer credential source: static
tokens, signed data tokens or OIDC. OIDC additionally enables
`/.well-known/oauth-protected-resource` and the metadata path advertised in the
authentication challenge. `/mcp` lists every tool; the only MCP boot setting is the
allowed-host list (`--mcp-allowed-host`). Every invocation uses the same
handlers and policy as HTTP.
A registered resource or listed tool is never a permission grant.

`omnigraph-mcp` owns the SDK integration, tool/resource catalog, settings,
argument decoding, result formatting and response deadlines. The server adapter
owns authentication, OAuth metadata, handler dispatch and admission, retaining
execution ownership when the caller stops waiting. It also supplies the canonical
`ApiError` projection for protocol refusals and handler failures, including the
unknown-effect disposition when an admitted write's result cannot be delivered.
The protocol crate wraps these bodies without a second error-code registry.
Both transports share one listener and serving lifecycle. The supported tools
and limits live in the [MCP guide](../user/operations/mcp.md).
