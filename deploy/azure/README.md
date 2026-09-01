# Azure Container Apps reference deployment

This directory is the OSS reference topology for one Azure Blob-backed
OmniGraph cluster. It provisions a private Blob container, ACR, one
user-assigned managed identity, Log Analytics, a Container Apps environment,
one manual bootstrap Job, and one HTTPS Container App.

> **Qualification status:** implementation, packaging, Azurite integration, the
> checked-in infrastructure, and the safe live managed-identity smoke test are
> complete. Adversarial lease, concurrency, and termination qualification is
> still pending, so this is a preview deployment and not a production-support
> claim.

The Blob lease is a cooperative process-admission mutex. It protects only
processes started through `omnigraph-azure-admission`; it is not an engine
fencing token and does not make arbitrary parallel writers safe.

## Deploy

Prerequisites: Azure CLI with the Container Apps extension, Bicep, the
`omnigraph` CLI on `PATH`, and an Azure subscription where the operator can
create resources and role assignments. The source image must be pinned by a
manifest digest and contain `omnigraph`, `omnigraph-server`, and
`omnigraph-azure-admission`.

The bundle directory is immutable deployment input. Its `cluster.yaml` must use
the exact `az://<container>/<cluster-prefix>` passed to the script; all schemas,
queries, and policies referenced by it must be beneath that directory.

```bash
export OMNIGRAPH_AZURE_SERVER_BEARER_TOKEN='replace-me'

# Local, non-destructive checks: input shape plus `az bicep build`.
deploy/azure/deploy.sh validate \
  --bundle ./company-brain \
  --server-image ghcr.io/modernrelay/omnigraph-server@sha256:<digest>

# Creates the foundation, imports the server image, builds an immutable bundle
# image, proves managed-identity image/Blob readiness, runs
# validate/import/apply under the lease, confirms release, then and only then
# activates the serving app.
deploy/azure/deploy.sh deploy \
  --resource-group omnigraph-demo-rg \
  --location eastus \
  --name omnigraph-demo \
  --container omnigraph \
  --cluster-prefix clusters/company-brain \
  --bundle ./company-brain \
  --server-image ghcr.io/modernrelay/omnigraph-server@sha256:<digest> \
  --evidence-out ./azure-deployment-evidence.json
```

The script never prints the bearer token. The optional evidence file contains
only non-secret deployment inputs, the bundle hash, readiness-attempt count,
bootstrap Job execution identifier, the healthy result, UTC time, endpoint,
and two immutable image references. Keep it with the deployment record. This is
deployment output, not a substitute for the still-pending adversarial
live-Azure acceptance record required by RFC-0029.

Updates use the same command and order. The script closes ingress and
deactivates the old revision, builds a new bundle image, runs the lease-wrapped
Job, and activates the new server only after Job success and a positively
observed lease release. Zero-downtime mutation serving is not supported.
Do not use `az containerapp revision restart` for this writer topology: Azure
may overlap the old and replacement replicas, leaving the replacement correctly
blocked behind the old replica's lease and therefore unable to pass its startup
probe. An explicit restart is stop-then-start: deactivate, observe zero replicas
and an unlocked lease, then activate.

On a first deployment, Azure RBAC grants may take time to reach the data plane.
The script first runs the same Job resource in a read-only `readiness` mode. It
may retry that phase up to three times because it only pulls the image and
inspects the admission object; it does not acquire a lease or start OmniGraph.
It then switches the Job to `apply` and runs that lease-capable phase exactly
once. A failed or timed-out apply is never retried automatically: inspect the
lease and use the recovery runbook below.

## Writer boundary

- Keep `minReplicas = maxReplicas = 1`; this is a sizing target, not the lock.
- Do not run direct CLI writes while the server or bootstrap Job can write.
- Do not bypass the image entrypoint for an Azure-rooted server.
- Every bootstrap, apply, maintenance, or data writer must use the same
  canonical cluster root and admission wrapper.
- Anonymous Blob access and storage shared-key authorization remain disabled.
  Runtime Blob access uses the container-scoped managed-identity role.
- The wrapper gets a 90-second child-drain budget; Container Apps gets 150
  seconds before platform termination. The 60-second difference leaves an
  explicit window for exact-ID lease release after the child group is gone.

## Inspecting admission

Run the command from a trusted environment after authenticating an operator
identity that has Blob data access. Mint a storage-scoped token explicitly;
`AZURE_STORAGE_CLIENT_ID` alone works only inside Container Apps together with
its injected identity endpoint and secret header.

```bash
account_name='replace-with-storage-account'
cluster_root='az://omnigraph/clusters/company-brain'
export AZURE_STORAGE_ACCOUNT_NAME="$account_name"
export AZURE_STORAGE_TOKEN="$(az account get-access-token \
  --resource https://storage.azure.com/ \
  --query accessToken --output tsv)"

omnigraph-azure-admission inspect \
  --root "$cluster_root"

unset AZURE_STORAGE_TOKEN
```

Record the reported `root_sha256` before any recovery action.

## Direct CLI and maintenance jobs

The serving app owns the cluster-wide admission lease while it is running. To
perform an OSS CLI write or maintenance operation, first close ingress and
deactivate the app, wait for `inspect` to report `lease_status=unlocked`, and
then run the command as one lease-wrapped Job against the **cluster root**:

```bash
cluster_root=az://omnigraph/clusters/company-brain
graph_root="$cluster_root/graphs/knowledge.omni"

omnigraph-azure-admission run \
  --mode job \
  --root "$cluster_root" \
  --grace-seconds 90 \
  -- \
  omnigraph optimize "$graph_root"
```

Use the same wrapper shape for `load`, schema changes, `cleanup`, and direct
`cluster apply`; the child command may address a graph beneath the root, but
the admission `--root` is always the canonical cluster root shared by the app
and bootstrap Job. Run one such Job at a time and reactivate the server only
after success plus `inspect` reporting unlocked. A nonzero child, hard kill,
or ambiguous release deliberately strands the infinite lease; do not retry the
command automatically—follow the recovery runbook.

## Recovering a stranded infinite lease

A hard kill, OOM, host loss, unexpected server exit, failed bootstrap, or
ambiguous release intentionally leaves the infinite lease locked. Never break
it merely because no healthy replica is visible.

1. Freeze deploy automation and every direct writer path.
2. Close ingress and deactivate the Container App.
3. Enumerate every active, inactive, and deprovisioning app revision and prove
   that it has zero replicas. Stop and enumerate every bootstrap Job execution
   and replica too.
4. Wait through the configured termination grace period and Azure control-plane
   convergence, then repeat the zero-process observation. A paused or
   unreachable old process is not proof of death.
5. If death cannot be proved, hard-fence the old runtime first: move to a fresh
   identity, revoke the old identity's Blob role, and wait through RBAC
   propagation and access-token expiry. Do not continue while the old identity
   can still write.
6. Inspect the exact root, verify its digest out of band, then break only with
   both explicit confirmations:

   ```bash
   account_name='replace-with-storage-account'
   cluster_root='az://omnigraph/clusters/company-brain'
   verified_digest='replace-with-verified-root-sha256'
   export AZURE_STORAGE_ACCOUNT_NAME="$account_name"
   export AZURE_STORAGE_TOKEN="$(az account get-access-token \
     --resource https://storage.azure.com/ \
     --query accessToken --output tsv)"

   omnigraph-azure-admission inspect --root "$cluster_root"
   omnigraph-azure-admission break \
     --root "$cluster_root" \
     --confirm-root-sha256 "$verified_digest" \
     --confirm-no-old-processes
   omnigraph-azure-admission inspect --root "$cluster_root"

   unset AZURE_STORAGE_TOKEN
   ```

7. Require the final inspection to report `lease_status=unlocked`. Re-run the
   bootstrap deployment, then reactivate ingress only after `/healthz`
   succeeds and the committed graph can be queried.

If any old-process or identity-fence fact is uncertain, stop. The safe outcome
is a blocked deployment, not two writer-capable processes.
