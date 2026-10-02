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

Use `GET /healthz` for process health and `GET /readyz` for readiness:
`/readyz` reports whether the replica is serving or draining, the applied
`config_digest` it booted from (`booted_serving_digest`), the ledger revision
it read, and how many graphs it serves and does not serve, and answers 503
once shutdown has begun. It is unauthenticated and therefore names no graph:
the authenticated `GET /graphs` lists the served graphs and, as `quarantined`,
the applied graphs this process does not serve. Add `--require-all-graphs`
when a quarantined graph should make the whole process fail startup.

An admitted write continues if its client disconnects. The server keeps its
capacity reserved through the whole operation, including optional source deletion
after a merge. A lost response can still leave the caller unsure whether the
write committed; follow the [failure outcome](operations/troubleshooting.md#failed-data-write-commands)
before submitting it again.

Shutdown is bounded: at SIGTERM the server closes operation admission and stops
accepting connections. It waits for admitted writes, read bodies and server
stream producers for at most `--shutdown-grace-seconds` (else
`OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`, else 25), then exits 2 with the unfinished
work logged. The deadline is kept by a thread, so a stalled request or a
blocked runtime cannot extend it. Signal handling begins after configuration
loading, before graph opening, and covers serving startup. Disconnected callers do not remove their writes from
this wait. A panic or uncertain completion in an admitted write closes admission
for every graph in that process and starts the same shutdown path without renewing
an existing deadline. After HTTP connections and the other known logical owners
finish, the process exits 2 immediately; the watchdog remains the upper bound.
Unresolved reservations stay charged until exit. This is process containment,
not proof that native storage I/O settled or that the engine can be reused.
A proven pre-effect failure, such as a failed initial schema read or a native-tag
retirement refusal, releases its reservation and keeps admission open. Error
status or a transient storage category alone does not establish that proof.
A clean drain exits 0. Set the orchestrator's own
termination grace longer than this value; a cutoff is crash-equivalent for the
work it interrupts, and the next open recovers it as after any crash.

## Admission limits

Operation and input admission refuse excess work immediately. Configure these
process-wide environment variables before startup. Byte values are integers
in bytes. Invalid numeric values log a warning and use the default; zero capacity
refuses the corresponding admission lane.

| Variable | Default | Resource |
|---|---:|---|
| `OMNIGRAPH_GLOBAL_INFLIGHT_MAX` | 64 | Admitted write operations across actors |
| `OMNIGRAPH_GLOBAL_BYTES_MAX` | 268435456 (256 MiB) | Estimated retained write-input bytes across actors |
| `OMNIGRAPH_PER_ACTOR_INFLIGHT_MAX` | 16 | Admitted writes per actor |
| `OMNIGRAPH_PER_ACTOR_BYTES_MAX` | 4294967296 (4 GiB) | Estimated retained write-input bytes per actor |
| `OMNIGRAPH_ACTIVE_ACTORS_MAX` | 1024 | Actors with admitted writes; idle records are removed |
| `OMNIGRAPH_INGRESS_INFLIGHT_MAX` | 64 | Write requests retained by collection, operations or responses |
| `OMNIGRAPH_INGRESS_BYTES_MAX` | 268435456 (256 MiB) | Reserved/retained write-body bytes |
| `OMNIGRAPH_READ_INGRESS_INFLIGHT_MAX` | 64 | Read/MCP requests with bodies retained by collection or responses |
| `OMNIGRAPH_READ_INGRESS_BYTES_MAX` | 67108864 (64 MiB) | Reserved/retained read/MCP body bytes |
| `OMNIGRAPH_BODY_TIMEOUT_SECONDS` | 30 | Time allowed to collect one complete request body |

The default ingress allowances total 320 MiB across the independent lanes;
configure both byte limits when setting an instance's input budget.
Body timeouts above 86400 seconds also warn and use the 30-second default.
Ingress reserves the route's maximum before reading, then reduces the reservation
to the actual body size. Body collection limits remain 1 MiB for ordinary JSON
requests, 32 MiB for bulk load/ingest and 64 KiB for MCP. MCP uses the read-body
lane after authentication. Registered bulk routes alone
receive the larger limit; a stored query named `load` or `ingest` keeps the
ordinary JSON limit. Stored-query admission uses the registry's typed read/write
kind. Disconnect does not release input or
operation capacity already transferred to a running write. The body timeout does
not cancel an admitted write. These input limits do not bound total process RSS
or all memory and I/O used by the engine. Size an instance for the graph, workload
and configured concurrency as well as request bytes.

The server allows 128 read-response observers and, independently, 64 write-response
observers. Bodies, yielded bytes and their server producers retain the slot until
their final owner releases it. Bodyless reads consume only a read observer; slow
reads cannot exhaust write-body or write-response capacity. Status routes bypass
ordinary admission so they remain callable during saturation.

The engine separately limits retained keyed batches, keyed parse estimates
and removed-ID collections across each operation's tables; see
[mutation limits](mutations/index.md#limits-and-conflicts). These fixed limits
return HTTP 413 with structured `resource_limit` details before the operation's
data fragments or publication, and leave the server available for smaller work.
They do not undo prior schema completion or an implicitly created load branch,
and do not establish a process memory ceiling or automatic retry safety.

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
