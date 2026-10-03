# Cluster control plane

The cluster control plane turns a local declarative bundle into a durable applied revision that servers can consume without the source checkout. It owns graph lifecycle, accepted schemas, stored queries, Cedar bundles, embedding-provider bindings, and external-Blob ingress policy. It does not own graph rows.

## Authority model

There are three distinct views:

1. **Desired configuration** — `cluster.yaml` and referenced files in an operator workspace.
2. **Applied ledger** — the durable state at the configured cluster storage root.
3. **Serving snapshot** — a validated projection of the applied ledger and content-addressed resources.

The desired bundle is input, not runtime authority. A server reads the applied revision; editing `cluster.yaml` changes nothing until a successful apply and server restart.

The storage root defaults to the configuration directory and may instead be a local path, `file://`, `s3://`, or `az://` root. Graph roots are derived as `graphs/<graph_id>.omni` beneath it.

## Managed CLI sessions

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

## Durable layout

| Path | Role |
|---|---|
| `__cluster/state.json` | Applied ledger; v2 also owns outstanding deployment authority and bounded results |
| `__cluster/resources/` | Content-addressed queries, policies and immutable deployment bundles |
| `__cluster/recoveries/` | V1 control-plane recovery sidecars |
| `__cluster/approvals/` | V1 digest-bound approval artifacts and consumption record |
| `__cluster/lock.json` | Persisted control lock; v2 lifetime cluster admission |
| `graphs/<id>.omni/` | Derived graph roots managed through apply |

All stored control objects use the shared storage adapter. Filesystem replacement and object-store PUT/CAS details stay below that boundary; higher layers deal in versioned reads, conditional writes, and normalized roots.

V1 cluster recovery sidecars remain a control-plane concern. The engine has no
ordinary graph recovery sidecar: graph schema, table pins and lineage publish
together in `__manifest`. V2 stores prepared schema authority in its one
outstanding ledger record, and uses the engine's exact publication evidence.

## V1 lifecycle operations

| Operation | Mutation | Responsibility |
|---|---|---|
| `validate` | None | Parse the whole bundle, normalize references, type-check schemas and queries, validate policies and bindings, and report all diagnostics. |
| `plan` | None | Compare desired resource digests with recorded/applied and observed state; compute dependencies and approval requirements. `--observe` takes no lock and labels the output `authority: observed`. |
| `approve` | Approval artifact | Bind one irreversible planned operation to exact before/after/config digests and an actor. |
| `apply` | Resources and ledger | Re-plan under the lock, execute eligible changes in dependency order, recover interrupted changes, then CAS the applied ledger. |
| `status` | None | Read the ledger, lock, recoveries, approvals, and current observations. |
| `refresh` | Ledger observations | Reconcile recorded observations with live resources without changing the desired bundle. |
| `observe` | None | `refresh` without the lock, the recovery sweep, or the write: report the statuses and observations `refresh` would record, labeled `authority: observed` with the exact `state_cas` read (RFC 0049). |
| `import` | Initial ledger | Adopt declared existing resources after validation and observation. |
| `force-unlock` | Lock only | Remove one exact lock ID after prior-owner and accepted-I/O quiescence, with admissions and concurrent unlocks excluded. |

Apply is idempotent. A no-op apply leaves the state bytes and revision untouched. Failures preserve the last durable ledger and leave enough sidecar evidence for the next status/apply/sweep to classify the interrupted operation.

Destructive graph deletion requires a matching unconsumed approval. Any relevant desired or observed digest change invalidates that approval. Approval files are retained with consumption metadata and summarized in the ledger.

## Offline deployment ledger

`deployment.rs` and `deployment/execution.rs` own the v2 offline protocol.
Explicit root-addressed conversion validates the applied v1 state under stopped
writers, preserves resources and graph history, and installs a fresh ledger
incarnation, sequence high-water mark and achieved-result revision. It does not
reset graphs, migrate graph storage or consult desired files. V2 supports only
existing graphs' schema and stored-query changes; root, graph inventory,
policies, provider/Blob bindings and storage format remain fixed. Normal engine
open requires v13 and server HTTP requires v0.12. Online activation is refused.
The applied revision and every achieved base retain each graph's exact source/IR
digests and identity domain/version, captured coherently during conversion and
advanced by successful schema outcomes. Serving and query-only deployment compare
this identity even after receipt eviction, rejecting a recreated graph with
identical schema text.

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
precedes schema invocation. Recovery never replays that invocation. It records
`NotStarted` as not attempted, or persists an engine-issued settlement intent
before reconciling `Started`. Strict numeric publication at the prepared base
plus one permits a neutral lineage fence or a qualified occupied-version proof
to establish nonpublication. Missing evidence remains unknown. Such proof
does not authorize adopting a foreign schema into the applied projection.

Terminal results advance only the achieved resources. Partial convergence is a
valid base for a corrective successor after unlock. Query-only work creates no
native commit and permits existing branches; schema apply retains its main-only,
single-live-branch restriction. Older completed receipts may expire, but consumed
sequences never execute again; after eviction even acceptance of a requested
nonce is unknown. Applied result revision is distinct from server activation.

`DeploymentCaller` preserves the storage-owner trust boundary and an optional
actor label. Authenticated identity callers recheck current cluster
`ConfigManage`, graph `Read`, and `SchemaApply` for schema effects/recovery.
Original authority stays immutable; recovery records the current executor while
preserving the authored engine receipt. The accepted
[server runtime RFC](../rfcs/2026-09-29-server-runtime-and-online-deployment.md)
owns the protocol rationale and remaining online-activation gates.

## Concurrency

V1 state-changing operations acquire `__cluster/lock.json` with storage-native create-if-absent semantics. Observe-only reads (`plan --observe`, `observe`) take no lock and write nothing; their output says so (`authority: observed`) and names the `state_cas` they read, and an existing lock is reported rather than refused. A v1 bundle that sets `state.lock: false` gets `authority: unlocked` on every command that would otherwise have held the lock. Final ledger publication is also conditional on the state version observed under the operation.

V2 requires locking. `admission.rs` issues an opaque canonical-root/exact-lock-ID
capability only after acquiring that same lock and rechecking ledger/inventory.
Server, supported direct CLI graph operations, native controls and cleanup hold
it for their lifetime; outstanding work admits only exact-ID reconciliation.
Dropping a guard or returning from a command, including success, retains its
persisted lock. Normal explicit release requires qualified terminal graph and
control I/O; generic server drain does not establish that proof. Operator
transfer therefore requires stopped prior processes and accepted-I/O quiescence,
then exact-ID unlock with all admissions and other unlocks excluded until it
finishes. The backend has no conditional-delete guarantee; this is not a
distributed fencing lock. Older/raw/embedded writers outside the participating
doors remain operator-excluded across outstanding work and recovery. No tags or
additional retention store protect deployment evidence, and the executor never
runs cleanup.

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

A directory lets the server resolve the storage root from `cluster.yaml`; a URI reads the applied deployment artifact directly. There is no single-graph positional boot, `--target`, or runtime graph add/remove API.

Serving captures ledger/resource digests, graph identity, expected contracts and validated policies before binding the listener. The fixed registry initially contains loading or preflight-blocked entries, with the same captured policy later installed on the engine. One process-owned startup batch opens at most four graphs concurrently, checks accepted contracts, projects external-Blob policy, and validates stored queries. Each completed graph becomes ready or blocked through the process/registry admission boundary; shutdown prevents late activation and drains entered opens. Default startup admits healthy graphs individually. `--require-all-graphs` installs successful handles atomically only when every graph succeeds; any failure stops startup. A nonempty inventory with zero healthy graphs fails after the batch finishes. No attempt timeout, startup retry, reopen or native-settlement proof is introduced.

Servers do not hot-reload applied graph configuration. Apply the new revision and restart every server that should serve it. Explicit OIDC public-admission snapshots have a separate bounded refresh contract below.

Protected graph and registry HTTP calls require the v0.12 contract header after
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
`principal:<sub>` and retains a private, verified credential profile:

- Version 1 keeps its existing per-graph action ceilings. Graph selection
  checks the ceiling before registry lookup; the common authorization gate
  checks actions before Cedar.
- Version 2 authenticates a cluster-bound identity and rejects permission
  fields. The same common gate requires applied Cedar policy for protected
  operations, with no token-derived graph/action ceiling.

Cedar must explicitly permit either signed profile even when no static
credentials exist. Static credential authority remains unchanged. Issuer
reachability is outside the serving request path. The profile boundary and
applied-policy ownership are described in
[Identity credentials and applied policy authorization](../rfcs/2026-09-09-identity-credentials-and-applied-policy.md).

`GET /graphs/discovery` accepts only the verified identity profile and returns
IDs and display names for the loading, ready and blocked graph inventory captured
at boot. It neither scans storage nor discloses availability, paths, policy,
schema, or query definitions. It needs no policy membership. The separate
`GET /graphs` uses the same registry with a `graph_list` gate and returns one
list containing runtime availability, sanitized failure and action. Version 1
filtering remains an additional restriction. Neither inventory synthesizes graph
entries from the boot witness. Discovery still discloses no availability.

HTTP and MCP graph resolution apply credential graph scope before lookup and
atomically capture a serving view with its graph-epoch lease. A loading, blocked or
transitioning graph yields 503 only after graph `read` authorization on `main` or
management `graph_list` authorization; otherwise it remains undisclosed as 404.
An invalid graph policy or configuration cannot authorize the graph-read fallback.
Availability booleans describe the runtime, not a policy grant; route
authorization still applies to ready graphs.

The CLI's versioned keychain cache records the issuance profile and verifies
its endpoint and identity bindings before replacement. A legacy issuance
request cannot return an identity profile, and restricted caches are never
silently upgraded. The provider-native client acquires missing/expired identity
credentials before an operation and selects discovery in a managed folder.
Explicit server addressing keeps the
existing catalog unless `graphs list --discovery` is requested; the CLI never
infers routing from an arbitrary bearer token's unverified shape.

A replica reports what it booted from on `GET /readyz` (RFC 0049): the
applied `config_digest` as `booted_serving_digest`, the ledger revision and
CAS, and registry, ready, loading and blocked graph counts. `served_graph_count`
counts every registry entry; closed transitions contribute to the blocked count.
Status is `loading`, `serving`, `degraded`, `blocked` or `draining`;
readiness requires a ready graph or valid empty inventory and open admission.
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
error or panic does not trigger write uncertainty. A write panic or explicitly
indeterminate owned completion closes admission for every graph and signals the
same bounded process shutdown, retaining
unresolved reservations until exit. Proven engine pre-effect refusals remain
nonfatal. Read/write body and response lanes have independent capacity; bodyless
reads consume only read observers. Once HTTP connections and the remaining known
logical owners finish, uncertain completion exits 2 immediately, with the original
watchdog as the upper bound. This does not establish native-I/O settlement.

Served export and baseline transport divide each existing process reservation
into three outstanding frame credits and one sequential producer slot. The
`Bytes` owner holds its payload, frame credit and process lease through the last
clone or slice. Both routes share this sender; the baseline terminal record is
encoded into one bounded pending chunk only after snapshot production succeeds.
A credit timeout is a stream error, never a successful truncated baseline.
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
there is no automatic reopen or retry. Status reports `transitioning` with
`wait_for_transition`, without a finite `Retry-After` promise.

The embedding entry point is `AppState::prepare_same_view`. It binds the actual
process runtime; callers cannot substitute a new runtime to bypass stopping.
It grants no schema/query replacement or deployment authority. There is no HTTP
transition endpoint or live deployment mode.

These registrations account for server lifetimes, not universal storage-I/O
settlement. A joined future or zero operation counter cannot authorize runtime
replacement, schema activation, cleanup or replay. Native accepted I/O and
protected completion memory/local-I/O capacity remain unqualified. Schema and
stored-query deployment still uses apply followed by restart. The accepted
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
state carries verified claims and graph selection. `DataTokenTrust::verify_at`
returns the identity projection for existing callers;
`verify_authenticated_at` returns the opaque authenticated result used by the
server. Public actor construction cannot grant signed-token permissions.
`DataTokenClaims` and `DataGrant` keep their version-1 shapes;
`IdentityTokenClaims` is a separate strict type. Read-only claim accessors on
`AuthenticatedActor` expose only the matching verified profile.

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

- Configuration, diffing, apply, sweep, and serving projection: `crates/omnigraph-cluster/src/`.
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
