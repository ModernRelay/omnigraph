# Deployment

OmniGraph can keep a cluster on a local filesystem, S3-compatible object
storage, or Azure Blob Storage. The server always boots from one cluster root
and exposes its healthy applied graphs under `/graphs/{id}/…`; use
`--require-all-graphs` when any quarantined graph must fail startup.

Start with [Operating a cluster](clusters/index.md) to create and apply the
deployment bundle.

## Binary

```bash
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-service":"secret"}' \
  omnigraph-server \
    --cluster /srv/omnigraph/company-brain \
    --bind 0.0.0.0:8080
```

For object storage, pass the applied storage root:

```bash
AWS_REGION=us-east-1 \
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-service":"secret"}' \
  omnigraph-server \
    --cluster s3://company-data/omnigraph/company-brain \
    --bind 0.0.0.0:8080
```

Use `GET /healthz` for process health. Add `--require-all-graphs` when a
quarantined graph should make the whole process fail startup.

## Container

The container entrypoint reads `OMNIGRAPH_CLUSTER` and binds to port 8080:

```bash
docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_CLUSTER=s3://company-data/omnigraph/company-brain \
  -e AWS_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e OMNIGRAPH_SERVER_BEARER_TOKENS_JSON \
  ghcr.io/modernrelay/omnigraph-server:<tag>
```

For a local cluster, mount the complete cluster directory and point
`OMNIGRAPH_CLUSTER` at the mount:

```bash
docker run --rm -p 8080:8080 \
  -v /srv/company-brain:/var/lib/omnigraph/cluster \
  -e OMNIGRAPH_CLUSTER=/var/lib/omnigraph/cluster \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN \
  ghcr.io/modernrelay/omnigraph-server:<tag>
```

Terminate TLS at a load balancer or trusted reverse proxy. Keep bearer tokens
and storage credentials in the platform's secret store.

## S3-compatible storage

For AWS S3, configure the standard AWS credential chain and region. For a
compatible service, these variables may also be needed:

```bash
export AWS_ENDPOINT_URL_S3=https://objects.example.com
export AWS_S3_FORCE_PATH_STYLE=true
```

Set `AWS_ALLOW_HTTP=true` only for a trusted local development endpoint. Do not
put credentials in `cluster.yaml` or graph URIs.

The same storage root must be visible to servers and out-of-band cluster or
maintenance jobs. Apply changes before restarting servers; the root's applied
revision is the deployment artifact.

## Azure Blob preview

Native `az://<container>/<prefix>` roots are implemented and tested with
Azurite. A live managed-identity smoke deployment is complete, but Azure
remains a qualification preview until the adversarial live-Azure matrix is
complete.

Every mutation-capable Azure process must be admitted under the cluster root:

```bash
AZURE_STORAGE_ACCOUNT_NAME=companygraph \
AZURE_STORAGE_CLIENT_ID=<managed-identity-client-id> \
OMNIGRAPH_SERVER_BEARER_TOKENS_JSON='{"act-service":"secret"}' \
  omnigraph-azure-admission run \
    --mode server \
    --root az://omnigraph/company-brain \
    -- \
    omnigraph-server \
      --cluster az://omnigraph/company-brain \
      --bind 0.0.0.0:8080
```

Use the same wrapper for bootstrap/apply jobs, direct graph writers, and
maintenance. The container entrypoint wraps an `az://` cluster automatically.
Replica-count settings are not a correctness fence: the admission lease is.

The checked-in [Azure reference deployment](../../deploy/azure/README.md)
contains the supported Container Apps topology, validation command, and
stuck-lease runbook. Do not break a lease until the owner has been identified
and stopped.

## Writer topology

Run one mutation-capable writer process per cluster unless an external system
provides equivalent writer ownership. This includes servers, direct CLI writes,
`cluster apply`, and maintenance. A cluster state lock serializes control-plane
operations but does not by itself fence graph writers.

Read replicas and zero-downtime overlapping writer replicas are not currently a
supported topology. Prefer stop-then-start replacement for a mutation-capable
server.

## Authentication and policy

Configure tokens from environment or a mounted secret file:

- `OMNIGRAPH_SERVER_BEARER_TOKEN`
- `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`
- `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE`
- `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` in the AWS-enabled build

Policy bundles come from the applied cluster. See
[HTTP server](operations/server.md) and
[Authorization and actors](operations/policy.md).

## Upgrade and backup

Back up the whole cluster root, not selected physical files. Before a release:

1. read the release notes;
2. quiesce writers;
3. verify backups or exports;
4. upgrade the fleet together;
5. restart and run representative reads and writes.

When a release changes the storage format, follow the
[export/rebuild guide](operations/upgrade.md) instead of attempting an in-place
migration.
