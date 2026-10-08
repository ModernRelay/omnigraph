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

Use `GET /healthz` for process health and `GET /readyz` for readiness:
`/readyz` reports `loading` (startup is still pending), `serving`,
`degraded` (some graphs unavailable), `blocked` (none ready), or `draining`.
It includes the applied `config_digest` it booted from
(`booted_serving_digest`), the ledger revision, and `served_graph_count`,
`ready_graph_count`, `loading_graph_count` and `blocked_graph_count`.
`served_graph_count` counts the whole registry. Graphs closed for a transition also
contribute to `blocked_graph_count`. Readiness stays 503 while any graph is
loading. Once startup finishes, it returns 200 when at least one graph is ready
or the applied inventory is empty, and 503 when all graphs are unavailable or
shutdown has begun. The listener opens after configuration and
authorization validation, before graphs open. Healthy graphs become available
as they finish loading, while readiness holds aggregate traffic until loading
finishes. A nonempty cluster with no healthy graph exits with a
startup error after its opening attempts finish. With `--require-all-graphs`,
all graphs remain unavailable until every graph opens successfully; any failure
stops the server. Wait for readiness before sending data requests: the printed
listening address and `/healthz` confirm only that the listener is live.

Readiness is unauthenticated and names no graph. Authorized `GET /graphs`
returns one `graphs` list, including blocked entries, with each graph's `state`
(`loading`, `ready`, `blocked`, `transitioning` or `stopping`), `read_available`, `write_available` and
`action`. Availability describes the runtime, not the caller's permissions.
Blocked entries include a sanitized `failure`: `invalid_configuration`,
`invalid_policy`, `invalid_external_blob_policy`, `open_failed` or
`invalid_stored_queries`. Details remain in server logs.
`apply_correction_or_restart` means fix a query, policy or provider configuration
through deployment, or restore damaged graph storage from a verified backup and
restart. It does not authorize adopting a different graph identity.
`wait_for_restart` describes shutdown.
`wait_for_startup` means the graph's one startup attempt has not completed
admission. It promises no retry time and does not trigger another open.
`wait_for_transition` means that graph's admissions are closed while its owner
finishes a transition. Wait for that owner, or restart if the transition was
abandoned or expired; there is no promised retry time. Other ready graphs can
continue serving and transitioning. Server-owned deployments activate schema and
stored-query changes through this transition; a proved pre-effect refusal restores
the unchanged views. See [live deployment](clusters/index.md#deploy-without-restarting).
Ready entries use `none`. There is no separate `quarantined` list or automatic startup retry.

`GET /cluster/deployments/{id}` separates the durable achieved result from its
current `active` status. Active means this process has installed that exact
deployment and all its graph bindings are ready. Loading, blocked graphs,
partial convergence and shutdown keep it false. A normal restart verifies its
own bindings before reporting the retained current deployment active; successful
apply alone is not activation. Activation does not rewrite the durable receipt.

Known loading, blocked or transitioning graphs return 503 (`graph_unavailable`) to callers authorized to
read `main` or list the management inventory; other callers cannot use this response to discover
them. Unknown graphs return 404. A 503 does not authorize replaying a write.
Add `--require-all-graphs` when any blocked graph should fail startup.
`omnigraph graphs list --server <name|url>` displays graph ID, state and URI;
`--json` preserves the full inventory fields. Minimal discovery remains IDs/names only.

An admitted write continues if its client disconnects. The server keeps its
capacity reserved through the whole operation, including optional source deletion
after a merge. A lost response can still leave the caller unsure whether the
write committed; follow the [failure outcome](operations/troubleshooting.md#failed-data-write-commands)
before submitting it again.

Once its complete request body is accepted, a read also keeps its capacity
reserved until execution finishes, even if the caller disconnects. MCP
cancellation and its 30-second response deadline stop waiting for the result;
an admitted read continues and retains its tool slot until execution finishes.

Shutdown is bounded: at SIGTERM the server closes operation admission and stops
accepting connections. It waits for entered graph opens, admitted writes, executing reads, read bodies
and server stream producers for at most `--shutdown-grace-seconds` (else
`OMNIGRAPH_SHUTDOWN_GRACE_SECONDS`, else 25), then exits 2 with the unfinished
work logged. The deadline is kept by a thread, so a stalled request or a
blocked runtime cannot extend it. Signal handling begins after configuration
loading, before graph opening, and covers serving startup. Disconnected callers
do not remove their executing work from this wait. A graph finishing startup
after shutdown begins cannot become available. A panic or uncertain
completion in startup or an admitted write closes admission
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
For a v2 cluster, even a clean exit retains its cluster lock. Follow the
[ownership-transfer procedure](#writer-topology) before starting the next owner.

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

The default ingress allowances total 320 MiB across the independent lanes;
configure both byte limits when setting an instance's input budget.
Body timeouts above 86400 seconds also warn and use the 30-second default.
Ingress reserves the route's maximum before reading, then reduces the reservation
to the actual body size. Body collection limits remain 1 MiB for ordinary JSON
requests, 32 MiB for bulk load and 64 KiB for MCP. Cluster deployment POSTs
allow 16 MiB plus 1 KiB for the request envelope; the captured bundle itself is
limited to 16 MiB. MCP uses the read-body
lane after authentication. Registered bulk routes alone
receive the larger limit; a stored query named `load` or `ingest` keeps the
ordinary JSON limit. Stored-query admission uses the registry's typed read/write
kind. Disconnect does not release input or
operation capacity already transferred to executing work. The body timeout ends
at complete collection; it does not cancel admitted execution. These input limits
do not bound total process RSS or all memory and I/O used by the engine. Size an
instance for the graph, workload
and configured concurrency as well as request bytes.

The server allows 128 read-response observers and, independently, 64 write-response
observers. Executing reads, pending results, bodies, yielded bytes and their server
producers retain the slot until their final owner releases it. Bodyless reads
consume only a read observer; slow
reads cannot exhaust write-body or write-response capacity. Status routes bypass
ordinary admission so they remain callable during saturation.

Export and change-baseline responses reserve 256 KiB each from a fixed 2 MiB
process allowance, permitting eight simultaneous reservations. Each response
allows three outstanding 64 KiB chunks plus one pending producer chunk; at most
two chunks wait in its queue. Yielded chunks, including retained clones and
slices, keep their reservation until released. A new response waits at most
250 ms for process capacity before HTTP 413. Once admitted, a producer waits
for chunk capacity or disconnect without a per-chunk deadline; ordinary queue
backpressure has no additional deadline. An interrupted baseline supplies no
usable terminal cursor. Consumers must release each received server buffer to
continue; an in-process consumer retaining every buffer can exhaust its lane.
An HTTP client may still collect its received response: those client buffers are
separate from the server's transport buffers. These limits cover transport
payload buffers, not engine row/Arrow encoding, native work or total process RSS.

The engine separately limits retained keyed batches, keyed parse estimates
and removed-ID collections across each operation's tables; see
[mutation limits](mutations/index.md#limits-and-conflicts). These fixed limits
return HTTP 413 with structured `resource_limit` details before the operation's
data fragments or publication, and leave the server available for smaller work.
They do not undo an implicitly created load branch or earlier command effects,
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
maintenance jobs. Submit schema/query changes and graph additions to the running
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
