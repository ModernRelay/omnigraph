# Deployment

This doc describes the public runtime contract for self-hosting Omnigraph. It
does not include environment-specific secrets, private infrastructure, or
internal deploy automation.

## Runtime Modes

Omnigraph supports two broad deployment shapes:

- local directory graphs
- `s3://` graphs on AWS S3 or S3-compatible object stores

The server binary and container image expose the same HTTP surface.

The server also has two **boot sources**: `omnigraph.yaml` (graph targets
declared in the per-operator config) or a **cluster directory**
(`omnigraph-server --cluster <dir>`), which serves the cluster control
plane's applied revision — see
[cluster-config.md](cluster-config.md#serving-from-the-cluster-the-mode-switch).
The two are exclusive per deployment; switching is a restart with a different
flag.

## Binary Deployment

Build or install:

- `omnigraph`
- `omnigraph-server`

On Windows, the binaries are `omnigraph.exe` and `omnigraph-server.exe`.

Run against a local graph:

```bash
omnigraph-server graph.omni --bind 0.0.0.0:8080
```

Run against an object-store-backed graph:

```bash
OMNIGRAPH_SERVER_BEARER_TOKEN="change-me" \
AWS_REGION="us-east-1" \
omnigraph-server s3://my-bucket/graphs/example/releases/2026-04-10-v0.1.0 \
  --bind 0.0.0.0:8080
```

## Cluster Mode in Containers (AWS, Railway)

A cluster-booted deployment has **two shapes** since the `storage:` root
(RFC-006):

- **Bucket, no volume (preferred for cloud)** — the cluster's ledger,
  catalog, and graph data live under an object-storage root
  (`storage: s3://bucket/prefix` in `cluster.yaml`). The server boots
  **config-free** from the bare URI; the container needs no volume at all:

  ```bash
  docker run -d \
    -e OMNIGRAPH_CLUSTER=s3://my-bucket/clusters/company-brain \
    -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
    -e OMNIGRAPH_SERVER_BEARER_TOKEN=... \
    -p 8080:8080 <image>
  ```

  Day-2 runs from any operator checkout of the config repo:
  `omnigraph cluster apply --config ./company-brain` (the `storage:` key
  routes every stored byte to the bucket), then restart the service. The
  state lock is genuinely cross-machine on object storage, so CI and
  operator shells contend safely.

- **Volume (file-rooted)** — the original shape: the whole cluster
  directory on a mounted volume. Still fully supported; the container
  contract:

```bash
docker run -d \
  -v /srv/company-brain:/var/lib/omnigraph/cluster \
  -e OMNIGRAPH_CLUSTER=/var/lib/omnigraph/cluster \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN=... \
  -p 8080:8080 <image>
```

`OMNIGRAPH_CLUSTER` is exclusive: combining it with `OMNIGRAPH_TARGET_URI`,
`OMNIGRAPH_CONFIG`, or `OMNIGRAPH_TARGET` fails fast (exit 64), the same
rule the server itself enforces. The image also ships the `omnigraph` CLI,
so the day-2 loop runs in-container with no `omnigraph.yaml`:

```bash
docker exec -it <container> sh -c \
  'omnigraph cluster apply --as <you> --config /var/lib/omnigraph/cluster'
# then restart the container to pick up the applied state
```

### AWS (ECS/Fargate + EFS)

1. Push the image to ECR (the `package.yml` workflow builds it).
2. Create an EFS filesystem; mount it in the task definition at
   `/var/lib/omnigraph/cluster`.
3. Task environment: `OMNIGRAPH_CLUSTER=/var/lib/omnigraph/cluster`, bearer
   tokens via Secrets Manager/SSM into `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON`
   (or the `--features aws` build's native Secrets Manager source).
4. ALB in front for TLS; target the container's 8080 with `/healthz` checks.
5. Day-2: ECS exec into the task → edit/upload config on the volume →
   `omnigraph cluster apply --as <you> --config /var/lib/omnigraph/cluster`
   → force a new deployment (restart).

For a deployment that doesn't need the cluster control plane, the classic
stateless shape — `OMNIGRAPH_TARGET_URI=s3://bucket/graph.omni`, no volume —
remains the simplest AWS architecture (see Binary/Container Deployment
above).

### Railway

1. Create a service from the image; attach a **volume** mounted at
   `/var/lib/omnigraph/cluster`.
2. Variables: `OMNIGRAPH_CLUSTER=/var/lib/omnigraph/cluster`,
   `OMNIGRAPH_SERVER_BEARER_TOKEN=<token>`. Railway terminates TLS at its
   edge and routes to the exposed 8080.
3. Day-2: `railway shell` (or `railway run`) → `omnigraph cluster apply
   --as <you> --config /var/lib/omnigraph/cluster` → redeploy/restart the
   service.

### Constraints (current honest list)

- **No hot reload** — applied changes serve on the next restart.
- **Single-writer apply** — run `cluster apply` from one place at a time
  (the state lock enforces this; CI or one operator shell, not both).
- **Multi-replica serving off a shared volume (EFS) is documented but
  unvalidated** — boot is lock-free read-only so it should compose, but it
  is not yet exercised by tests.

## One-Command Local RustFS Bootstrap

The easiest local S3-backed deployment path is:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/local-rustfs-bootstrap.sh | bash
```

The bootstrap:

- starts a local RustFS-backed object store
- creates a bucket and S3-backed Omnigraph graph
- loads the checked-in context fixture
- starts `omnigraph-server` on `127.0.0.1:8080`

Supported behavior:

- downloads the rolling `edge` binary when one exists for the current platform
- otherwise clones `ModernRelay/omnigraph` and builds from source
- reuses an existing RustFS container if it is already running

Useful overrides:

- `WORKDIR=/path/to/state`
- `BUCKET=omnigraph-local`
- `PREFIX=graphs/context`
- `RESET_REPO=1` to delete an existing partially initialized graph prefix before recreating it
- `BIND=127.0.0.1:8080`
- `RUSTFS_CONTAINER_NAME=omnigraph-rustfs-demo`

The bootstrap expects:

- Docker
- `curl`
- either a matching release asset or a local Rust toolchain plus `git`

If `aws` is not installed, the script attempts a user-local AWS CLI install via
`python3 -m pip`. Docker Desktop or another Docker daemon must already be
running.

If a previous bootstrap left objects behind under the selected `PREFIX` but did
not finish initializing the graph, rerun with `RESET_REPO=1` or choose a new
`PREFIX`.

## Container Deployment

Build the image:

```bash
docker build -t omnigraph-server:local .
```

Run against a local graph:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/graph.omni:/data/graph.omni" \
  omnigraph-server:local \
  /data/graph.omni --bind 0.0.0.0:8080
```

Run against an S3-backed graph:

```bash
docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN="change-me" \
  -e AWS_REGION="us-east-1" \
  omnigraph-server:local \
  s3://my-bucket/graphs/example/releases/2026-04-10-v0.1.0 \
  --bind 0.0.0.0:8080
```

### Container entrypoint env vars

When no positional args are given, the image entrypoint
(`docker/entrypoint.sh`) builds the server command from env vars:

| Var | Effect |
|---|---|
| `OMNIGRAPH_TARGET_URI` | Graph URI, passed as the positional argument. |
| `OMNIGRAPH_CONFIG` | Path to an `omnigraph.yaml`, passed as `--config`. Used to supply a `policy.file` (Cedar authorization). The config file and any relative `policy.file` must be mounted into the container. |
| `OMNIGRAPH_TARGET` | Graph name to select from the config's `graphs:` block (with `OMNIGRAPH_CONFIG`, when no `OMNIGRAPH_TARGET_URI`). |
| `OMNIGRAPH_BIND` | Listen address (default `0.0.0.0:8080`). |

`OMNIGRAPH_TARGET_URI` and `OMNIGRAPH_CONFIG` **compose**: set both to keep the
graph URI in the env var while loading policy from the config file (the
positional URI wins over any `graphs:` entry). To enable Cedar policy on a
container otherwise driven by `OMNIGRAPH_TARGET_URI`, mount the config dir and
add `OMNIGRAPH_CONFIG`:

```bash
docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN="change-me" \
  -e OMNIGRAPH_TARGET_URI="s3://my-bucket/graphs/example/releases/2026-04-10-v0.1.0" \
  -e OMNIGRAPH_CONFIG="/etc/omnigraph/omnigraph.yaml" \
  -v "$PWD/config:/etc/omnigraph:ro" \
  omnigraph-server:local
# /etc/omnigraph/omnigraph.yaml contains `policy: { file: policy.yaml }`;
# policy.yaml (+ optional policy.tests.yaml) sit beside it in the mount.
```

## Auth

The server can run unauthenticated for local development only when explicitly
started with `--unauthenticated` or `OMNIGRAPH_UNAUTHENTICATED=1`. Any shared or
internet-facing deployment should set a bearer token source.

### Token sources

The server reads bearer tokens from one of three places, in precedence order:

1. **AWS Secrets Manager** (build with `--features aws`, see below) — set
   `OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` to the secret ID or ARN.
2. **JSON file or env** — set one of:
   - `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` — path to a JSON `{"actor": "token", ...}` file.
   - `OMNIGRAPH_SERVER_BEARER_TOKENS_JSON` — the JSON literal inline.
3. **Single-token env** — `OMNIGRAPH_SERVER_BEARER_TOKEN` (assigns the
   implicit actor `default`).

Tokens are hashed with SHA-256 immediately on ingest; plaintext does not
persist in process memory after startup.

The health endpoint `/healthz` remains suitable for load balancer health checks
and is never gated.

## Build Variants

The server binary ships in two flavors:

| Variant | Command | Contents |
|---------|---------|----------|
| **Default** (on-prem / local dev) | `cargo build --release` | Core server, no AWS SDK |
| **AWS** | `cargo build --release --features aws` | Adds AWS Secrets Manager backend for bearer tokens |

Tagged release archives contain the default `omnigraph` and
`omnigraph-server` binaries on macOS / Linux, and `omnigraph.exe` plus
`omnigraph-server.exe` on Windows. AWS-enabled server binaries are built from
source with `cargo build --release --features aws -p omnigraph-server` when
needed.

The AWS build adds ~150 transitive deps and ~30-60s of first-build compile
time. Default builds don't pay that cost.

## AWS Secrets Manager

When the binary is built with `--features aws`, set
`OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` to the ARN or name of a Secrets
Manager secret whose `SecretString` is a JSON object of
`{"actor_id": "token", ...}`:

```bash
omnigraph-server-aws s3://my-bucket/graphs/example ...
  # Environment:
  # OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET=arn:aws:secretsmanager:us-east-1:123456789012:secret:omnigraph-tokens-AbCdEf
```

Credentials are resolved via the AWS default chain (env vars, shared config,
IMDSv2 instance role, ECS task role) — no explicit credential plumbing is
needed when running under an IAM instance role on EC2/ECS/EKS.

The IAM role must permit `secretsmanager:GetSecretValue` on the referenced
secret.

Setting the env var without building with `--features aws` is a hard error
with a rebuild instruction — it does not silently fall back to the env/file
source.

## S3-Compatible Storage

For S3-compatible backends such as RustFS or MinIO, set the usual AWS SDK
environment variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- optional `AWS_ENDPOINT_URL`
- optional `AWS_ENDPOINT_URL_S3`
- optional `AWS_ALLOW_HTTP=true`
- optional `AWS_S3_FORCE_PATH_STYLE=true`
