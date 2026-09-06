# Cluster control plane

The cluster control plane turns a local declarative bundle into a durable applied revision that servers can consume without the source checkout. It owns graph lifecycle, accepted schemas, stored queries, Cedar bundles, embedding-provider bindings, and external-Blob ingress policy. It does not own graph rows.

## Authority model

There are three distinct views:

1. **Desired configuration** — `cluster.yaml` and referenced files in an operator workspace.
2. **Applied ledger** — the durable state at the configured cluster storage root.
3. **Serving snapshot** — a validated projection of the applied ledger and content-addressed resources.

The desired bundle is input, not runtime authority. A server reads the applied revision; editing `cluster.yaml` changes nothing until a successful apply and server restart.

The storage root defaults to the configuration directory and may instead be a local path, `file://`, `s3://`, or `az://` root. Graph roots are derived as `graphs/<graph_id>.omni` beneath it.

## Durable layout

| Path | Role |
|---|---|
| `__cluster/state.json` | Versioned applied ledger and resource status |
| `__cluster/resources/` | Content-addressed stored-query and policy payloads |
| `__cluster/recoveries/` | Control-plane operation sidecars |
| `__cluster/approvals/` | Digest-bound approval artifacts and consumption record |
| `__cluster/lock.json` | Exclusive persisted state-operation lock |
| `graphs/<id>.omni/` | Derived graph roots managed through apply |

All stored control objects use the shared storage adapter. Filesystem replacement and object-store PUT/CAS details stay below that boundary; higher layers deal in versioned reads, conditional writes, and normalized roots.

The cluster sidecars are separate from each graph's ordinary recovery-v9 sidecar. A control-plane operation may need both: the outer cluster record describes desired/applied resource progress, while the engine record owns graph-table publication.

## Lifecycle operations

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
| `force-unlock` | Lock only | Remove one exact stale lock ID after an operator proves no owner is alive. |

Apply is idempotent. A no-op apply leaves the state bytes and revision untouched. Failures preserve the last durable ledger and leave enough sidecar evidence for the next status/apply/sweep to classify the interrupted operation.

Destructive graph deletion requires a matching unconsumed approval. Any relevant desired or observed digest change invalidates that approval. Approval files are retained with consumption metadata and summarized in the ledger.

## Concurrency

State-changing operations acquire `__cluster/lock.json` with storage-native create-if-absent semantics. Observe-only reads (`plan --observe`, `observe`) take no lock and write nothing; their output says so (`authority: observed`) and names the `state_cas` they read, and an existing lock is reported rather than refused. A bundle that sets `state.lock: false` gets `authority: unlocked` on every command that would otherwise have held the lock. Final ledger publication is also conditional on the state version observed under the operation. The lock coordinates operator processes; graph-level manifest gates and recovery still own data correctness.

Do not bypass the cluster API with direct filesystem writes, edit `state.json`, or derive a second mutable inventory. Content digests and live observations are recomputed from the declared and durable authorities.

The current distributed support boundary is still one mutation-capable writer process unless an external fence proves exclusivity. Filesystem and S3 rely on that operator boundary. Azure writers must acquire the external admission lease through `omnigraph-azure-admission`.

## Serving projection

`omnigraph-server` has one boot mode:

```text
--cluster <config-directory | file://root | s3://root | az://root>
```

A directory lets the server resolve the storage root from `cluster.yaml`; a URI reads the applied deployment artifact directly. There is no single-graph positional boot, `--target`, or runtime graph add/remove API.

Serving verifies ledger/resource digests, builds each graph's query registry and embedding provider, projects external-Blob policy to the server-safe subset, and binds at most one Cedar bundle per graph plus one cluster-level bundle. A graph-local open or registry failure quarantines that graph while healthy graphs may continue. `--require-all-graphs` makes any quarantine a startup failure; zero healthy graphs always fails.

Servers do not hot-reload. Apply the new revision and restart every server that should serve it.

Bearer authentication is a server concern. Cedar mutation enforcement also lives in the engine's `_as` APIs so embedded and CLI writers cannot bypass it. Cluster policy application publishes the bundles and bindings; it does not replace either enforcement layer.

The optional [offline data-token profile](../rfcs/0053-offline-data-token-verification.md)
uses immutable public trust loaded before graph open. The Core's serving
snapshot supplies the canonical storage root from the same resolution as the
applied revision; the server checks that root against trust without reading a
managed identity marker. The verifier resolves `principal:<sub>` and retains
per-graph action ceilings. Graph selection checks the ceiling before registry
lookup; the common authorization gate checks actions before Cedar, which must
explicitly permit signed identities even when no static credentials exist.
Static credential authority remains unchanged. Issuer reachability is outside
the serving request path.

A replica reports what it booted from on `GET /readyz` (RFC 0049): the
applied `config_digest` as `booted_serving_digest`, the ledger revision and
CAS, and how many applied graphs it serves and does not; it answers 503 from
the shutdown signal on. Graph ids stay on the authenticated `GET /graphs`,
which also lists the quarantined ones. Graceful shutdown is bounded by one
deadline (`--shutdown-grace-seconds`, default 25), kept by a thread and armed
by a listener installed before graphs open, after which the process exits 2
without claiming success.

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
