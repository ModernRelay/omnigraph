---
rfc: "0029"
title: "Native Azure Blob storage"
track: public
status: accepted
implementation: in-progress
authors:
  - Roey Zalta (@roy2392)
created: 2026-08-05
updated: 2026-08-23
discussion: "https://github.com/ModernRelay/omnigraph/issues/509"
supersedes: []
superseded_by: []
blocked_on:
  - Adversarial live-Azure qualification matrix
---

# RFC 0029: Native Azure Blob storage

The backend, admission wrapper, hermetic Azurite coverage, packaging, and
reference deployment are implemented on the development line. A live Container
Apps deployment proved managed-identity authentication, lease-admitted
bootstrap/apply, authenticated serving, writes, queries, restart persistence,
and credential-safe logs. The disruptive concurrency, forced-termination, and
lease-break matrix in [Tests and acceptance gates](#live-azure-proof) remains
outstanding, so native Azure support remains a qualification preview and is not
advertised as production-supported.

## Summary

Azure Blob Storage is implemented as a native OmniGraph storage backend,
addressed by `az://<container>/<prefix>` URIs through the existing Lance and
Apache Arrow `object_store` abstractions. The default binaries and container
image understand Azure roots, use the same manifest, recovery, and storage
adapter paths as local and S3 roots, and authenticate without storage keys when
running under Azure managed identity. The checked-in Azure Container Apps
reference deployment chooses one live writer process for the whole cluster,
which preserves OmniGraph's existing per-graph writer/recovery boundary, using
an infinite Azure Blob process-admission lease rather than treating Container
Apps replica targets as a correctness guarantee. Repeatable Azurite coverage is
present; the still-pending adversarial live-Azure evidence defines the boundary
for a future production-support claim.

## Motivation

OmniGraph currently documents local filesystems and S3-compatible object stores.
An Azure operator can run a separate S3-compatible service or mount Azure Files,
but neither is a native Azure Blob deployment. Those approaches miss the main
operational reasons Azure customers choose the platform: Microsoft Entra RBAC,
managed identity, Blob-native conditional writes, and infrastructure that can be
deployed and audited with Azure-native tooling.

The concrete need is to deploy a company knowledge graph entirely on Azure and
be able to show customers a maintained, reproducible path rather than a private
fork. "The Lance data files happen to be portable" is not enough. The cluster
ledger, lock, approvals, catalog, recovery sidecars, graph manifests, and
backing datasets must all use the Azure backend, and the deployment must prove
restart persistence and authorization behavior.

The long-run liability is kept small by extending existing substrate seams:

- The [Lance object-store integration](https://lance.org/guide/object_store/)
  already supports Azure URIs and owns dataset I/O.
- Apache Arrow's [`object_store` Azure module](https://docs.rs/object_store/0.13.2/object_store/azure/),
  already used by `omnigraph-storage`, implements Azure Blob,
  ETag-conditioned update, conditional create, listing, copy, and delete.
- OmniGraph keeps one storage adapter and one graph publication/recovery path;
  it does not add an Azure-specific dataset/control-object client,
  cloud-specific manifest, or Azure-only engine.
- Authentication remains configuration at the storage boundary, not graph
  state or a new secret format.

## Scope and non-goals

This RFC includes:

- `az://` roots anywhere an S3/object-store root is accepted; capabilities
  intentionally restricted to the local filesystem remain local-only;
- Azure support in both Lance dataset I/O and OmniGraph control-object I/O;
- managed identity as the target production authentication path; the safe live
  smoke proof is complete and the required adversarial live-Azure proof remains
  pending;
- deterministic integration tests against Azurite and a documented live-Azure
  deployment proof;
- an Azure Container Apps reference deployment with a one-replica sizing target
  and one Blob-leased writer-admission wrapper shared by the app and bootstrap
  job;
- user, CLI, deployment, and contributor documentation for the new backend.

This RFC does not:

- promote multi-process writers, overlapping writer recovery, or multi-replica
  mutation serving to supported status;
- present the deployment lease as an engine-level fencing token or make
  arbitrary unwrapped writers safe;
- introduce a second graph format, Azure-specific manifest records, or an Azure
  data migration protocol;
- add an Azure Files/POSIX storage backend;
- add native Key Vault bearer-token loading or change server authentication;
- certify every Azure compute service, sovereign cloud, private-network shape,
  or ADLS Gen2 URI alias in the first delivery;
- make `abfs://`, `abfss://`, or HTTPS Blob URLs public OmniGraph storage URI
  aliases;
- make `az://` an allowed external Blob-cell value; that is a separate
  [RFC 0033](0033-blob-management.md) policy and evidence decision from placing
  an OmniGraph root on Azure Blob Storage.

## Guide-level explanation

### URI and cluster configuration

An Azure-backed cluster uses one Blob container and an optional object prefix:

```yaml
# cluster.yaml
version: 1
storage: az://omnigraph/clusters/company-brain
graphs:
  company:
    schema: schema.pg
```

The container is the URI authority. The storage account is deliberately not in
the URI; it comes from the environment so the same declaration can move between
accounts without embedding credentials or account-specific endpoints:

```bash
export AZURE_STORAGE_ACCOUNT_NAME=companybrainprod
export AZURE_STORAGE_CLIENT_ID=<user-assigned-managed-identity-client-id>

omnigraph cluster validate --config ./company-brain

# Every Azure writer, including apply and the mutation-capable server, enters
# through the same root-scoped admission wrapper. The checked-in deployment
# script performs this bootstrap/apply/start sequence automatically.
omnigraph-azure-admission run \
  --mode job \
  --root az://omnigraph/clusters/company-brain \
  -- \
  /bin/sh -eu -c '
    omnigraph cluster import --config "$1"
    omnigraph cluster apply --config "$1" --as operator@example.com
  ' omnigraph-bootstrap ./company-brain

OMNIGRAPH_SERVER_BEARER_TOKEN="..." \
  omnigraph-azure-admission run \
  --mode server \
  --root az://omnigraph/clusters/company-brain \
  -- \
  omnigraph-server \
    --cluster az://omnigraph/clusters/company-brain \
    --bind 0.0.0.0:8080
```

`AZURE_STORAGE_ACCOUNT_NAME` is required for real Azure Blob Storage. On Azure
Container Apps, `IDENTITY_ENDPOINT` and `IDENTITY_HEADER` are injected by the
[managed-identity runtime](https://learn.microsoft.com/en-us/azure/container-apps/managed-identity).
Some Container Apps revisions also inject the deprecated `MSI_ENDPOINT`
duplicate. OmniGraph accepts it only when `IDENTITY_ENDPOINT` is present with
the same value; it is never an independent endpoint selector.
`AZURE_STORAGE_CLIENT_ID` selects the intended user-assigned identity.
That identity receives `Storage Blob Data Contributor` scoped no wider than the
deployment requires.

For local tests, select Azurite through OmniGraph's captured emulator inputs:

```bash
export AZURE_STORAGE_USE_EMULATOR=true
export AZURE_STORAGE_ACCOUNT_NAME=devstoreaccount1
# Optional when Azurite is not at its default address:
export AZURITE_BLOB_STORAGE_URL=http://127.0.0.1:10000
```

These names are OmniGraph's exact configuration contract over the pinned
`object_store` 0.13.2 options. `AZURE_STORAGE_CLIENT_ID` is the canonical
client-ID key; `AZURE_CLIENT_ID` is also accepted as an upstream alias, but the
reference deployment uses the canonical spelling. In managed-identity mode,
the credential providers consume the Container Apps-provided
`IDENTITY_ENDPOINT` and `IDENTITY_HEADER`. A matching platform-provided
`MSI_ENDPOINT` is captured for drift detection and normalized to that same
endpoint; a conflicting or standalone legacy alias is refused. In emulator mode,
`AZURE_STORAGE_USE_EMULATOR=true` selects Azurite and
`AZURITE_BLOB_STORAGE_URL` overrides its default
`http://127.0.0.1:10000` address. OmniGraph captures that selection once, expands
it to an ordinary explicit HTTP endpoint containing the emulator account path,
and passes `use_emulator=false` to both builders so neither can reread mutable
process environment. `AZURE_STORAGE_ENDPOINT` (or its `AZURE_ENDPOINT` alias)
instead overrides the real Blob service endpoint; it is not the Azurite switch.
The explicit emulator account name remains required because it is part of the
effective endpoint and Lance cache identity. Dependency upgrades must re-audit
and test this contract rather than silently changing the documented environment.

Account keys, SAS tokens, client credentials, workload identity, bearer tokens,
and Azure CLI credentials remain available where the pinned `object_store`
builder supports them for dataset and control-object I/O. The narrow admission
client does not reproduce that entire credential chain: it supports the
Container Apps managed-identity endpoint, an explicitly supplied bearer token,
or Shared Key for Azurite. The reference preview uses managed identity; the
hermetic path uses Azurite. Documentation must not imply that every upstream
credential combination receives OmniGraph integration coverage or can run the
lease-admitted topology.

### Canonical URI semantics

The first public Azure form is exactly:

```text
az://<container>[/<object-prefix>]
```

- `<container>` is required and must be valid for Azure Blob Storage.
- The object prefix may be empty, although a dedicated cluster prefix is
  recommended.
- The storage backend accepts an empty prefix, but the Container Apps reference
  deployment requires a non-empty dedicated prefix and rejects the reserved
  top-level `__omnigraph_azure_admission/` namespace so its admission object
  stays unambiguous and outside graph-root cleanup.
- A trailing slash on a root is normalized away, matching S3 root behavior.
- Joining graph and control paths appends slash-delimited object keys beneath
  the captured root.
- One adapter is scoped to one container. A URI naming another container is a
  typed mismatch, not an implicit second client.
- Userinfo or account-qualified authorities, ports, query strings, fragments,
  embedded credentials, and unsupported Azure aliases are rejected rather than
  interpreted.
- Empty interior path segments, dot segments, backslashes, control characters,
  and percent encodings that decode to those forms or to path separators are
  rejected instead of being normalized into aliases.

The account and endpoint are configuration, not URI identity. Within one
process, users must not point the same normalized `az://` root at different
accounts. Changing those environment values requires a process restart, just as
changing S3 endpoint configuration does.

One shared canonical Azure-root type owns these checks and the captured account,
endpoint, container, and prefix. Dataset/control-object setup and the admission
wrapper consume that type; the wrapper does not independently reparse a raw
URI. Its lock-object key uses a versioned SHA-256 digest of the canonical
backend identity and root, so two accepted spellings cannot admit separate
writers for one cluster.

### Azure Container Apps reference deployment

![Proposed OmniGraph Azure reference architecture](assets/0029-azure-reference-architecture.png)

The checked-in deployment creates or connects the following resources:

- an Azure StorageV2 account and private Blob container;
- an Azure Container Registry for the application image and immutable
  bootstrap image;
- one user-assigned managed identity;
- `Storage Blob Data Contributor` for that identity scoped to the cluster
  container and `AcrPull` scoped to the registry;
- a Log Analytics workspace and Container Apps environment;
- one deployment-owned writer-admission Blob in a reserved container-level
  namespace outside the canonical cluster prefix;
- one manually invoked Container Apps bootstrap/apply Job using the same
  OmniGraph binaries and admission wrapper; and
- an externally reachable Container App with HTTPS ingress, `/healthz` probes,
  a secure bearer-token secret, and a one-replica sizing target.

The app receives `AZURE_STORAGE_ACCOUNT_NAME` and
`AZURE_STORAGE_CLIENT_ID`; the canonical cluster root is passed as the
`--cluster` argument. It does not receive a storage account key. The storage
container remains private, and deployment output must not print the server
bearer token or any credential.

The OSS deployment script packages `cluster.yaml` and every referenced schema
or policy file into an immutable bootstrap image layer and records its digest.
It first keeps the serving app inactive, runs the Job through the admission
wrapper to validate, import, and apply that exact bundle, waits for successful
completion and a positively confirmed lease release, and only then activates
the server revision. An update follows the same order after draining and
stopping the previous server.
The running server receives only the applied `--cluster az://...` root; it does
not read desired configuration from a commercial service or perform bootstrap
implicitly.

In this first public-cloud topology, "private" means anonymous Blob access is
disabled (`allowBlobPublicAccess = false`), shared-key authorization is
disabled, and runtime access uses Microsoft Entra ID plus container-scoped
RBAC. The Azure Blob public service endpoint remains enabled. Private endpoints
and VNet integration are explicitly outside this first qualification boundary.

Azure documents that replica quantities are
[targets, not guarantees](https://learn.microsoft.com/azure/container-apps/scale-app),
and that platform maintenance can temporarily pre-warm extra replicas.
`minReplicas = 1` and `maxReplicas = 1` are therefore sizing and steady-state
assertions only. They are not part of the correctness proof.

Both the serving app and every bootstrap/job execution enter through the same
PID-1 admission path: Tini forwards process-group signals and subreaps orphaned
descendants, while its supervised wrapper owns the lease protocol. Before any
OmniGraph child can open the cluster or run recovery, the wrapper derives one
canonical lock Blob from the exact storage account, container, and normalized
cluster root. It stores that permanent object at
`__omnigraph_azure_admission/v1/<canonical-root-sha256>/writer.lock`, a reserved
container-level namespace outside the cluster root and its lifecycle cleanup,
and creates it atomically if necessary. It generates a
cryptographically unique proposed lease ID, and positively acquires an
[infinite Blob lease](https://learn.microsoft.com/rest/api/storageservices/lease-blob)
with managed identity. Azure grants only one active lease for that Blob. A
pre-warmed replica, overlapping job execution, or second deployment therefore
stays alive but unready (or exits as a non-serving job) and never starts an
OmniGraph writer. A lost acquire response authorizes no child unless an
exact-ID renew positively proves that this wrapper owns the lease; any still
ambiguous result fails closed.

On `SIGTERM`, the wrapper stops admission, forwards the signal to the complete
child process group, and waits for the HTTP server, in-flight requests,
resident background writers, and descendants to finish. It releases the lease
with the exact owning lease ID only after the child group is gone. A generic
`EXIT` trap may not release a lease without that proof. The Container
Apps termination grace period must exceed that drain budget. An ambiguous
release response is reported as non-success and gives the old wrapper no right
to claim either lease state. It never authorizes a successor: the successor
still starts only after its own positively confirmed acquire. If Azure applied
the release, that acquire may safely succeed; if not, it remains blocked.
`SIGKILL`, OOM, host loss, or a child that cannot drain inside the grace budget
never reaches the release request and leaves the infinite lease held. An
unexpected server exit or nonzero bootstrap exit also strands the lease:
process death alone does not prove that an already accepted Azure Blob service
request cannot still complete. Only a wrapper-initiated graceful drain or a
successful bootstrap with every descendant gone reaches the release path.

The lease is a cooperative deployment admission mutex, not a storage fencing
token: it does not prevent an operator from launching a binary that bypasses
the wrapper, and breaking it cannot fence a paused old process. The reference
deployment never auto-breaks a lease based on time. Recovery requires an
explicit runbook that first freezes new admissions, closes ingress, deactivates
and enumerates every active, inactive, and deprovisioning revision, stops and
enumerates every job execution/replica, and proves stable zero processes beyond
the termination and Azure resource-control consistency windows. If old-process
death cannot be positively established, the runbook must hard-fence the old
runtime
(for example, rotate to a fresh identity, revoke the old identity's Blob role,
and wait through authorization propagation and token expiry) before breaking
and observing the lease as unlocked. Zero-downtime rolling mutation serving
remains outside the preview and target qualification boundary.

## Reference-level design

### Substrate activation

The workspace enables each existing dependency's `azure` feature
(`lance/azure` and `object_store/azure`). The Azure implementation is part of the normal
binaries and published server image; a URI accepted by the CLI must not fail
only because a packager omitted an undocumented feature.

No new storage crate or Azure SDK is introduced for dataset or control-object
I/O. Lance continues to open all Lance datasets through its public object-store
integration. OmniGraph control objects continue through
`omnigraph-storage::StorageAdapter`.

Blob admission leasing is a separate deployment concern: `object_store` does
not expose Azure's Blob Lease operations. One non-storage workspace crate,
`omnigraph-azure-admission`, owns this boundary. Its library contains the small
Rust Blob Lease REST client and typed outcomes; its binary is the PID-1 wrapper
and exposes the explicit lease inspection/recovery commands. The serving app
and bootstrap Job invoke that same binary rather than carrying separate lease
implementations.

`omnigraph-azure-admission` depends on `omnigraph-storage` for the canonical
Azure root and admission-object identity, but it does not depend on the engine,
server, CLI, or cluster crates. Its REST client uses the existing HTTP stack,
owns its managed-identity token acquisition, and may only create and inspect the
single reserved object and acquire, renew, or release its lease. The explicit
recovery command may break the lease only after the runbook's old-process proof.
The crate exposes no general graph/control-object read or write API and must
never become an alternate storage path.

### Backend selection and path mapping

The shared storage crate adds an `Azure` storage kind and an Azure URI codec:

```text
az://container/a/b.json  ->  container-scoped object path a/b.json
```

The codec performs scheme, container, and non-empty object-key checks at the
point an object is accessed. Root normalization, URI joining, write-queue
identity, graph-root construction, cluster handles, manifest metadata, and
dataset layout all classify Azure with the existing remote-object-store path.
Every exhaustive `Local | S3` decision is reviewed; Azure follows S3 only where
the distinction is genuinely local versus remote, not by accidental fallback.
Scheme dispatch is exhaustive: an `az://` root in an artifact without Azure
support is a typed unsupported-backend error, never a local path. Unknown or
disabled schemes fail before any object or dataset access.

At the first Azure open, the canonical Azure-root type snapshots every
recognized Azure selection option. The control adapter starts from
`MicrosoftAzureBuilder::new()` and replays only those immutable captured options,
then sets the URI's container and the captured explicit endpoint. Lance 10
receives the same explicit option snapshot and canonical URI through its Azure
provider. In Azurite mode that endpoint already includes the account path and
both clients are deliberately built as ordinary endpoint clients, not upstream
emulator clients. Neither builder calls `from_env()` or rereads
`AZURITE_BLOB_STORAGE_URL` after capture. The option snapshot is process-cached;
each control adapter replays it, while Lance receives a static-options accessor
whose stable content-derived ID participates in its process-wide object-store
registry. Upstream credential providers may still refresh short-lived
credentials without changing the captured location. Tests prove that both
clients resolve the same account, endpoint, container, and prefix. Changing
selection variables requires a process restart, so the OSS control-object
adapter and Lance dataset client cannot silently split across Azure locations.

### Conditional writes and recovery semantics

Azure Blob is marked as supporting conditional update. `object_store` maps:

- `PutMode::Create` to `If-None-Match: *`;
- `PutMode::Update` plus the prior ETag to `If-Match: <etag>`;
- a successful write to a new version token from the returned ETag.

This preserves the strong CAS behavior expected by the cluster ledger, state
lock, and other control objects. A failed precondition is the ordinary CAS-lost
outcome; authentication, throttling, timeout, and transport failures remain loud
storage errors. There is no read-then-overwrite fallback for Azure.

For every adapter marked `supports_conditional_update`, a versioned read and a
successful conditional write must return the backend ETag or fail with a typed
storage error. A content hash is never substituted for a missing remote ETag;
the local adapter's own hash token is unchanged. The shared adapter contract
includes test doubles that omit the read ETag and write-result ETag so both
fail-closed branches remain owned outside Azure-specific tests.

Azure text-object rename uses fixed-size, ETag-pinned range GETs, a
visibility-complete PUT, and then DELETE. Larger objects use sequential
multipart parts; unfinished blocks are provider-invisible and every ordinary
pre-complete failure is aborted. This avoids both object-sized copy allocation
and Azure Copy Blob's asynchronous completion window, but the operation is
still not atomic. A failed or lost multipart-complete response retains the
source and may leave both names, just like a crash after a successful PUT.
Recovery paths must continue to tolerate both source and destination after a
crash. Azure contract tests pin immediate destination
visibility; the existing provider-neutral recovery tests retain ownership of
the dual-source/destination crash shape.
Prefix deletion remains list plus delete and is likewise retryable rather than
transactional.

Azure changes only the physical object transport. Lance versions, graph
manifests, the single manifest publication door, recovery sidecars, branch
authority, and internal schema stamps retain their existing meaning.

### Authentication boundary

Dataset and control-object authentication stays inside the two upstream storage
builders. Their tokens are acquired by the pinned Azure credential provider and
are neither persisted in the graph nor surfaced through OmniGraph APIs. The
narrow admission client independently acquires a token for the same
user-assigned identity for each Blob request and keeps it private to that
request. The Azure reference deployment uses that identity so ACR pull and Blob
access can be independently scoped and audited.

The implementation must avoid logging credential-bearing environment values,
SAS query strings, token responses, or Container Apps secret values. URI
validation rejects query-bearing roots, which also prevents SAS credentials
from becoming persisted cluster identifiers.

The admission wrapper requests a fresh managed-identity access token for each
Blob request and never stores a token or request body in a file. Tokens and the
lease ID stay out of the child environment, command arguments, and logs. The
OmniGraph child still receives the Container Apps-managed identity endpoint and
header because its own Azure storage credential provider requires them; those
runtime values must not be logged.

### Required process topology for qualification

Native Blob conditional writes improve the backend contract but do not close
the known multi-process gaps above it. In particular, process-local recovery
serialization and non-conditional Lance branch-ref deletion remain unchanged.
The OSS `__cluster/lock.json` serializes cluster apply; it is not a graph-data
writer fence. The outer admission lease therefore covers the server,
bootstrap/apply jobs, direct CLI data writers, and maintenance for every graph
under the cluster root. The reference deployment deliberately chooses this
stronger cluster-wide topology instead of trying to coordinate one writer per
graph.

The preview deployment, and any future production-support claim, therefore use:

- one lease-admitted writer process for the cluster at a time;
- one Container Apps replica as the steady-state sizing target, while temporary
  extra replicas remain unable to start an OmniGraph child;
- the server, bootstrap job, job retries, and every deployment-supplied writer
  path enter through the same canonical lease wrapper;
- no concurrent CLI load/schema/maintenance writer while the serving process
  may mutate the same graph, and no direct binary/CLI bypass of the wrapper;
- restart recovery after the prior writer has stopped.

Read-only scaling may be designed and evidenced separately. This RFC does not
infer it from Blob consistency or Container Apps routing.

The native backend, admission wrapper, wrapper-admitted direct CLI workflow,
recovery runbook, and correctness tests are OSS deliverables. A commercial
Managed Control Plane may automate provisioning, drains, restarts, and evidence
collection, but it is not required to deploy, operate, recover, or safely serve
one Azure-backed cluster, and it does not replace any storage or lease authority
defined here.

## Tests and acceptance gates

Production-support acceptance requires evidence at each changed boundary. The
hermetic gates and safe live managed-identity smoke proof are complete; the
adversarial live gate remains pending.

### Unit and storage-contract tests

- selection, normalization, joining, and parsing for `az://` roots;
- refusal to treat `az://` or any unknown scheme as a local path when its
  backend is unavailable;
- refusal of missing/invalid containers, userinfo, ports, query/fragment
  credentials, path-alias encodings, empty object paths, and cross-container
  access;
- one-to-one tests for canonical root identity and its admission-object digest,
  including distinct digests for distinct accepted roots;
- the complete shared storage contract against Azurite: read, bounded read,
  overwrite, conditional create, versioned read, winning and losing ETag
  update, existence, direct-child listing, bounded listing, delete, recursive
  prefix delete, and Azure's read/visibility-complete-put/delete rename;
- a concurrency case proving exactly one conditional-create claimant wins and
  a stale ETag cannot overwrite the winner;
- the existing local, in-memory, and S3 suites remain green.

### Engine and cluster integration

Against Azurite, tests must exercise more than the control adapter:

1. validate, import, and apply an Azure-rooted cluster;
2. initialize a graph, load data, and run a query through Lance on `az://`;
3. perform a mutation and observe it through a fresh handle;
4. exit, reopen in a fresh process, and recover the same accepted data;
5. create, write, delete, and recreate a branch without confusing its old and
   new native identity; and
6. inspect the container to prove both cluster control objects and Lance graph
   objects were written beneath the declared prefix.

Both adapter-level stale-CAS races and at least one failpoint-driven graph
manifest publication recovery must run on the Azure backend. Azure cluster
state uses direct conditional ETag publication rather than rename; its stale-CAS
winner/loser path is the required cluster evidence. The separate non-atomic
rename case belongs to schema-contract staging: tests must leave both the
destination and source after the destination PUT and then recover them through
the ordinary provider-neutral `recover_schema_state_files` path. An
emulator-only happy path cannot establish the correctness claim.

### Build, documentation, and infrastructure

- the canonical workspace gate
  `cargo test --workspace --locked --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints`
  and the repository's required feature matrix pass with Azure compiled into
  normal artifacts;
- tests pin the documented managed-identity, endpoint, and Azurite environment
  contract of the exact `object_store` version, and a build/source guard keeps
  both `lance/azure` and `object_store/azure` enabled in normal artifacts;
- a dedicated CI job starts a pinned Azurite image, runs the shared storage,
  admission, engine, cluster, server-boot, and CLI Azure owners, and fails if
  any required Azure test reports itself skipped;
- a dependency guard pins the one-way
  `omnigraph-azure-admission -> omnigraph-storage` boundary and forbids the
  storage, engine, server, CLI, and cluster crates from importing the lease
  client;
- the container image builds from a clean checkout and contains `omnigraph`,
  `omnigraph-server`, and the `omnigraph-azure-admission` wrapper with `az://`
  support;
- Azure infrastructure compiles through `az bicep build` and its deployment
  script has a non-destructive validation mode;
- wrapper tests model simultaneous contenders, unique proposed lease IDs,
  ambiguous acquire/release responses, process-group draining, stuck-lease
  behavior after an ungraceful exit, and refusal to launch the child without a
  positively confirmed acquisition;
- public storage, cluster, CLI, deployment, and contributor docs name Azure's
  support and its single-writer boundary consistently;
- no generated credentials, subscription IDs, tenant IDs, or live resource
  names are committed.

### Live Azure proof

Before native Azure is presented as production-supported, the reference
deployment is run in a disposable Azure resource group with managed identity
and no storage key. A checked script must prove:

- the Container App reaches `Ready` with exactly one lease-owning child in
  steady state;
- the proof runs with the OSS deployment scripts, wrapper, and CLI and makes no
  commercial Managed Control Plane call;
- `/healthz` returns success;
- a protected endpoint returns `401` without a bearer token;
- authenticated graph listing, load or mutation, and query succeed;
- a new Container App revision or explicit stop-then-start restart reads the
  previously committed graph state; an in-place rolling revision restart is
  outside the topology because Azure may overlap replicas while the replacement
  correctly waits behind the old lease;
- forced concurrent replicas and overlapping manual job executions produce
  exactly one child interval and never two writer-capable children;
- a long-running mutation plus `SIGTERM` delays successor admission until the
  old process drains and exits;
- a hard-killed lease owner leaves every successor blocked until the runbook
  proves stable zero old processes and explicitly breaks the lease;
- an unexpected server crash or nonzero bootstrap leaves the lease stuck and
  admits no successor child;
- simultaneous waiters produce one positively confirmed winner;
- expected control and Lance objects exist in the private Blob container;
- application logs contain no tokens or storage credentials.

The PR records both image digests, deployment command, UTC time, redacted result
summary, and cleanup result. Cloud resource existence is supporting evidence;
the repeatable scripts and Azurite tests are the durable evidence.

## Invariants and deny-list check

- **Invariant 1 (respect the substrate):** Azure I/O uses public Lance and
  `object_store` features. There is no custom Azure storage engine or private
  Lance API.
- **Invariants 2 and 5 (one publication door and recovery):** backend selection
  does not create a second manifest publisher or recovery protocol. All effects
  retain existing Lance-plus-manifest ordering.
- **Invariant 3 (one accepted view):** URI/account configuration is fixed for
  the process; an operation cannot combine Azure roots or refresh environment
  configuration midway through a captured view.
- **Invariant 6 (strong consistency):** Azure control CAS uses real Blob ETags
  and conditional PUT. Failures are not downgraded to eventual consistency.
- **Invariant 11 (boundary separation):** Azure credentials and deployment
  concerns remain in storage/runtime configuration, outside compiler and engine
  semantics.
- **Invariants 13 and 14 (observable failure and matching evidence):** storage
  failures stay typed and loud; Azurite contract tests exercise the hermetic
  boundaries, the safe live managed-identity smoke test exercises the ordinary
  deployment path, and the pending adversarial live matrix must exercise the
  disruptive production boundary before support is claimed.
- **Invariant 15 (one source of truth):** Blob is another physical substrate for
  Lance and `__manifest`, not a mirrored store or Azure side registry.

The known single-writer-process gap is not weakened or reclassified. The
reference deployment admits one cooperative process through a fail-closed
infinite lease and documents quiesced upgrades; it does not treat Blob ETags,
replica targets, or the admission lease as a distributed recovery fence inside
OmniGraph.

No deny-list exception is requested. In particular, the design adds no custom
WAL/transaction manager, raw filesystem I/O for cluster state, cloud-only
correctness fork, parallel source of truth, or alternate publication path.

## Compatibility and rollout

`file://` and `s3://` behavior, configuration, and tests remain compatible.
`az://` is additive. Because the stored objects use the existing Lance and
OmniGraph formats, this RFC requires no internal-schema version bump and no
old/new binary format migration matrix.

Binaries released before Azure support are not safe clients for an `az://`
root: their incomplete scheme dispatch may interpret an unknown URI as a local
path. Operators must not downgrade an Azure deployment to such a binary. The
implementing release first makes the shared decoder reject every unknown or
disabled scheme, and deployment metadata and docs record the minimum Azure-aware
version before any Azure root is created.

One fail-closed correction applies to every backend marked as supporting
conditional updates: if a versioned read or successful conditional write omits
an ETag, OmniGraph now returns a storage error instead of synthesizing a content
hash that cannot be used as a valid `If-Match` token. Conforming S3 and Azure
stores are unaffected; an S3-compatible implementation that omitted ETags was
already unable to satisfy the documented strong-CAS contract.

There is no automatic relocation of an existing graph root between backends.
An operator moving data from local or S3 to Azure must quiesce writes and use a
documented export, fresh Azure init/import, load, verification, and cutover
workflow. Raw object copying is not claimed as a supported migration unless a
separate test proves every embedded physical reference remains valid.

Rollout order:

1. land fail-closed unknown-scheme handling, then Azure features, URI/storage
   plumbing, validation, and unit tests;
2. land Azurite storage, engine, cluster, and recovery integration coverage;
3. update all public surfaces and add the Container Apps reference deployment;
4. execute and record the safe live managed-identity smoke proof (complete);
5. execute the disruptive lease, concurrency, termination, and lease-break
   matrix (pending);
6. advertise production Azure support only after all preceding gates are
   present in one release; before then, describe the implemented backend only
   as a qualification preview.

## Drawbacks and alternatives

### Continue using an S3-compatible service on Azure

This works today but adds another stateful service and credential model. It
does not provide a native managed-identity-to-Blob path and is not the customer
claim this RFC needs to establish.

### Mount Azure Files and use the local backend

Rejected as the official cloud path. It routes distributed storage through
local-filesystem semantics, including the documented non-cross-process local
CAS behavior, and makes correctness depend on mount and filesystem details that
the engine does not certify.

### Use an Azure-specific SDK for storage I/O

Rejected. It would duplicate authentication, retries, conditional-write, and
listing behavior already owned by `object_store`, while risking divergence from
Lance's Azure client. The narrowly scoped admission-lease REST client described
above is not a competing storage path and exists only because `object_store`
does not expose the Blob Lease protocol.

### Add every Azure URI alias immediately

Rejected for the first delivery. One canonical `az://` form keeps URI identity,
help text, error behavior, and tests exact. `abfs://`, `abfss://`, sovereign-
cloud endpoints, and Fabric/OneLake can be added after their semantics and
credential paths receive their own evidence.

### Do nothing

Operators can keep using local volumes or S3-compatible stores. The cost is
that OmniGraph cannot honestly claim native Azure Blob or keyless Azure
deployment support, which blocks the concrete enterprise deployment need.

## Reversibility

The implementation is mechanically reversible before release because it adds
one backend behind existing abstractions and does not alter stored formats.
After release, `az://` becomes a public URI contract: removing it would strand
otherwise valid roots and therefore requires normal deprecation policy.

Azure-hosted data remains standard OmniGraph/Lance data in ordinary Blob
objects. The transport choice is operationally durable, but it does not create
an Azure-only graph format. The reference infrastructure can be deleted without
changing data semantics, subject to the operator's backup and retention policy.

## Unresolved questions

- Should adversarial live-Azure qualification remain a maintainer-run release
  gate, or can the project provide a least-privilege CI subscription for
  periodic validation?
- Which non-public-Azure environments, if any, should be explicitly certified
  after the initial public-cloud and Azurite support lands?
