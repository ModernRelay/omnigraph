# Deployment

OmniGraph can keep a cluster on a local filesystem, S3-compatible object
storage, or Azure Blob Storage. The server always boots from one cluster root
and exposes its ready applied graphs under `/graphs/{id}/…`; use
`--require-all-graphs` when any blocked graph must fail startup.

Start with [Operating a cluster](clusters/index.md) to create and apply the
deployment bundle.

Servers use the v0.13 HTTP contract and open graph storage format v14. The
cluster ledger has a separate version: explicitly converting it to v2 preserves
graph data and history. See [cluster deployments](clusters/index.md).

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

Use `GET /healthz` for process liveness and `GET /readyz` for readiness.
The listener can accept health checks while graphs are still loading.

| Readiness state | HTTP | Meaning |
|---|---|---|
| `loading` | 503 | At least one graph is still opening. |
| `serving` | 200 | All graphs are ready, or the applied inventory is empty. |
| `degraded` | 200 | Startup finished; some graphs are unavailable but others can serve. |
| `blocked` | 503 | No graph is ready. A nonempty cluster whose startup opens all fail exits. |
| `draining` | 503 | Shutdown has begun. |

Readiness includes the booted configuration digest, ledger revision and
ready/loading/blocked graph counts. It is unauthenticated and names no graphs.
`--require-all-graphs` holds every graph unavailable until all open successfully
and makes any opening failure fatal. There are no automatic startup retries.

Authorized `GET /graphs` and `omnigraph graphs list --server URL --json` expose
each graph's state, read/write availability, suggested action and sanitized
failure category. Availability is independent of the caller's permissions.
Loading or transitioning entries tell callers to wait for their existing owner;
blocked entries require configuration correction or storage recovery. Details
are in server logs. Minimal identity discovery returns IDs and names only.

Known unavailable graphs return `503 graph_unavailable` only to callers allowed
to discover their availability; unknown graphs return 404. Neither response
authorizes replaying a write. Use exact deployment status to observe
[activation](clusters/index.md#deploy-without-restarting): durable completion
alone does not establish that the affected bindings are serving.

An admitted write continues after client disconnect, including optional source
removal after a merge. Executing reads also retain their capacity until execution
finishes. A lost write response can leave the outcome unknown; follow
[failure outcomes](operations/troubleshooting.md#failed-data-write-commands)
before submitting again.

On SIGTERM the server closes admission and drains entered graph opens, admitted
operations and response producers. `--shutdown-grace-seconds` overrides
`OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`; the default is 25 seconds. A clean drain exits
0. Unfinished work at the deadline is logged and the process exits 2. Set the
orchestrator's termination grace longer than the server's.

A panic or uncertain completion in startup or an admitted write closes admission
for **every graph in that process**. Once the other known operations finish, the
process exits 2; the grace deadline is the upper bound. A proven pre-effect
refusal keeps admission open. The error's HTTP status or storage category alone
does not prove whether effects started.

Drain does not prove native storage I/O has settled. Even a clean exit retains
the cluster lock. Follow [ownership transfer](#writer-topology) before starting
the next owner.

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
| `OMNIGRAPH_READ_INGRESS_INFLIGHT_MAX` | 64 | Read/MCP requests with bodies retained by collection, execution or responses |
| `OMNIGRAPH_READ_INGRESS_BYTES_MAX` | 67108864 (64 MiB) | Reserved/retained read/MCP body bytes |
| `OMNIGRAPH_BODY_TIMEOUT_SECONDS` | 30 | Time allowed to collect one complete request body |

The independent write/read ingress allowances total 320 MiB by default.
Collection reserves the route maximum, then reduces it to the actual body size.
Body limits are 1 MiB for ordinary JSON, 32 MiB for registered bulk-load routes,
64 KiB for MCP, and 16 MiB plus 1 KiB envelope for deployment submission.
A stored query named `load` still uses the ordinary JSON limit. Body timeouts
above 86400 seconds use the 30-second default.

The collection deadline ends when the body is complete; it does not cancel
execution. Disconnects do not release capacity held by executing work.
Read/MCP bodies have a separate lane from writes. Independently, 128 read-response
observers and 64 write-response observers retain capacity through execution and
response delivery. Status routes remain available during saturation.

Export and change-baseline responses share a fixed 2 MiB transport allowance:
eight 256 KiB reservations, each holding at most three outstanding 64 KiB chunks
and one pending producer chunk. New responses wait at most 250 ms for capacity
before HTTP 413. Admitted streams use backpressure without a per-chunk deadline,
so slow clients do not lose data merely by pausing. Interrupted baselines have
no usable terminal cursor. In-process consumers must release yielded buffers
to let producers continue.

These are input and transport bounds, **not a total process memory ceiling**.
Size instances for graph execution, native I/O and concurrency too. The engine's
separate [mutation limits](mutations/index.md#limits-and-conflicts) return
structured HTTP 413 refusals before data publication; they do not undo earlier
commands or an implicitly created load branch, and do not establish retry safety.

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
maintenance jobs. Submit configuration changes to the running
server with `cluster apply --server`; the root's applied revision remains the
deployment artifact. Direct apply requires ownership transfer before serving.

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

Use the same wrapper with `--mode job` and the same canonical `--root` for
bootstrap/apply jobs, `cluster upgrade-ledger`, root-addressed reconciliation,
`cluster force-unlock`, direct graph writers, and maintenance. Root-addressed
deployment status is read-only and does not need the lease. A v2 cluster's persisted
lock is additional admission; it does not replace the Azure lease or its
qualification boundary. The container entrypoint wraps an `az://` cluster automatically.
Replica-count settings are not a correctness fence: the admission lease is.

The checked-in [Azure reference deployment](../../deploy/azure/README.md)
contains the supported Container Apps topology, validation command, and
stuck-lease runbook. A stranded Azure lease and a retained v2 cluster lock are
separate: complete the lease recovery procedure before starting a wrapped
cluster-unlock or reconciliation job. Do not break a lease until the owner has
been identified and stopped.

## Writer topology

Run one mutation-capable writer process per cluster. This includes servers,
direct CLI writes, deployments, branch controls, and maintenance. On a v2
cluster, supported servers and direct CLI writers acquire the same exclusive
cluster admission before writable graph work and recheck the applied inventory.
CLI queries, exports, schema inspection/planning, lint and stored-query
validation check the applied identity without acquiring or changing that lock;
they refuse outstanding deployments or schema drift. Native maintenance,
including `repair` preview, uses writer admission. An outstanding deployment
admits only reconciliation of its exact original ID.
Keep older binaries, raw storage tools and embedded writers outside this
cooperating boundary stopped; the lock cannot fence their native storage I/O.

Admission remains held after an ordinary write command succeeds, a deployment
completes, or server work ends without settlement proof. A completed read-only
preflight refusal during new deployment, conversion or admission construction
releases its admission. Recovery of accepted work, cancellation and uncertain
effects retain it. A finished response, zero active HTTP
requests, lock age or a stopped PID alone does not prove that previously
accepted storage writes have settled. Before transferring ownership, stop the
prior owner and establish that its graph work and control-store I/O are terminal.
Exclude new admissions and other unlock attempts until the exact-ID unlock
finishes. Then start the next owner. Use the root-addressed
[status and recovery commands](clusters/index.md#inspect-and-recover-a-deployment)
to obtain the lock ID; unlocking never resolves an uncertain deployment.

A legacy v1 ledger refuses ordinary serving and writes. Stop its previous
owners and explicitly [convert the ledger](clusters/index.md#direct-deployments-and-conversion)
before using the current admission protocol.

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

Graph-format upgrades and cluster-ledger conversion are separate operations.
Follow the [storage upgrade guide](operations/upgrade.md) for qualified format
routes; `cluster upgrade-ledger` never resets or rewrites graph history.
