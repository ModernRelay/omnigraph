# Deployment

This doc describes the public runtime contract for self-hosting Omnigraph. It
does not include environment-specific secrets, private infrastructure, or
internal deploy automation.

## Runtime Modes

Omnigraph supports two broad deployment shapes:

- local directory graphs
- `s3://` graphs on AWS S3 or S3-compatible object stores

The server binary and container image expose the same HTTP surface.

The server has a single **boot source**: a **cluster directory**
(`omnigraph-server --cluster <dir | s3://…>`), which serves the cluster control
plane's applied revision — see
[cluster-config.md](clusters/config.md#serving-from-the-cluster-the-mode-switch).

## Binary Deployment

Build or install:

- `omnigraph`
- `omnigraph-server`

On Windows, the binaries are `omnigraph.exe` and `omnigraph-server.exe`.

The server boots from a cluster only (RFC-011) — there is no positional
`<URI>` / single-graph boot. Point it at a local cluster directory:

```bash
omnigraph-server --cluster ./company-brain --bind 0.0.0.0:8080
```

Or boot config-free from an object-storage-rooted cluster:

```bash
OMNIGRAPH_SERVER_BEARER_TOKEN="change-me" \
AWS_REGION="us-east-1" \
omnigraph-server --cluster s3://my-bucket/clusters/company-brain \
  --bind 0.0.0.0:8080
```

The server serves every graph in the cluster's applied revision under
`/graphs/{id}/...`. See [clusters](clusters/index.md) for authoring and
applying a cluster.

## Cluster Mode in Containers (AWS, Railway)

A cluster-booted deployment has **two shapes** since the `storage:` root:

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

`OMNIGRAPH_CLUSTER` is the server's only boot source. The image also
ships the `omnigraph` CLI, so the day-2 loop runs in-container:

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

For a stateless, volume-free deployment, root the cluster on object
storage and boot config-free with
`OMNIGRAPH_CLUSTER=s3://bucket/clusters/<name>` (the bucket-no-volume
shape above) — the simplest AWS architecture.

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

## Testing against S3 locally

To exercise the S3 storage path without a cloud account, run any S3-compatible
store in Docker and point the standard `AWS_*` environment at it. RustFS is
shown; MinIO works the same way.

```bash
docker run -d --name omnigraph-s3 -p 9000:9000 \
  -e RUSTFS_ACCESS_KEY=omnigraph -e RUSTFS_SECRET_KEY=omnigraph \
  -e RUSTFS_ALLOW_INSECURE_DEFAULT_CREDENTIALS=true \
  rustfs/rustfs:latest /data

export AWS_ACCESS_KEY_ID=omnigraph AWS_SECRET_ACCESS_KEY=omnigraph \
  AWS_REGION=us-east-1 AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000 \
  AWS_ALLOW_HTTP=true AWS_S3_FORCE_PATH_STYLE=true

# create the bucket once (any S3 client works)
aws --endpoint-url "$AWS_ENDPOINT_URL_S3" s3 mb s3://omnigraph-local
```

Now an `s3://…` URI works anywhere a graph or cluster root is expected. Root a
cluster on the bucket and serve it config-free:

```bash
# cluster.yaml
#   version: 1
#   storage: s3://omnigraph-local/clusters/demo
#   graphs: { demo: { schema: schema.pg } }

omnigraph cluster validate --config .
omnigraph cluster import   --config .
omnigraph cluster apply    --config . --as you
omnigraph load --data seed.jsonl --mode merge \
  s3://omnigraph-local/clusters/demo/graphs/demo.omni
omnigraph-server --cluster s3://omnigraph-local/clusters/demo \
  --bind 127.0.0.1:8080 --unauthenticated
```

The same `AWS_*` contract applies to a production object store — swap the
endpoint and credentials. CI exercises this path against containerized RustFS.

## Container Deployment

Build the image:

```bash
docker build -t omnigraph-server:local .
```

The server boots from a cluster only (RFC-011). Run against a cluster
directory on a mounted volume:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/company-brain:/var/lib/omnigraph/cluster" \
  omnigraph-server:local \
  --cluster /var/lib/omnigraph/cluster --bind 0.0.0.0:8080
```

Run config-free against an object-storage-rooted cluster:

```bash
docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN="change-me" \
  -e AWS_REGION="us-east-1" \
  omnigraph-server:local \
  --cluster s3://my-bucket/clusters/company-brain \
  --bind 0.0.0.0:8080
```

### Container entrypoint env vars

When no positional args are given, the image entrypoint
(`docker/entrypoint.sh`) builds the server command from env vars:

| Var | Effect |
|---|---|
| `OMNIGRAPH_CLUSTER` | Cluster boot source — a config directory or a storage-root URI, forwarded as `--cluster`. The only boot source. |
| `OMNIGRAPH_BIND` | Listen address (default `0.0.0.0:8080`). |
| `OMNIGRAPH_REQUIRE_ALL_GRAPHS` | When truthy, forwarded as `--require-all-graphs`: any graph-local quarantine or startup failure aborts cluster boot instead of serving the healthy subset. |

Per-graph and server-level Cedar policy come from the cluster's applied
revision (authored in `cluster.yaml` and published with `cluster apply`),
not from a separate config file. The cluster docker shapes — volume vs.
config-free object-storage root — are detailed under
[Cluster Mode in Containers](#cluster-mode-in-containers-aws-railway) above.

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
