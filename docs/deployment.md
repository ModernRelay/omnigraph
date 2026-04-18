# Deployment

This doc describes the public runtime contract for self-hosting Omnigraph. It
does not include environment-specific secrets, private infrastructure, or
internal deploy automation.

## Runtime Modes

Omnigraph supports two broad deployment shapes:

- local directory repos
- `s3://` repos on AWS S3 or S3-compatible object stores

The server binary and container image expose the same HTTP surface.

## Binary Deployment

Build or install:

- `omnigraph`
- `omnigraph-server`

Run against a local repo:

```bash
omnigraph-server ./repo.omni --bind 0.0.0.0:8080
```

Run against an object-store-backed repo:

```bash
OMNIGRAPH_SERVER_BEARER_TOKEN="change-me" \
AWS_REGION="us-east-1" \
omnigraph-server s3://my-bucket/repos/example/releases/2026-04-10-v0.1.0 \
  --bind 0.0.0.0:8080
```

## One-Command Local RustFS Bootstrap

The easiest local S3-backed deployment path is:

```bash
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph/main/scripts/local-rustfs-bootstrap.sh | bash
```

The bootstrap:

- starts a local RustFS-backed object store
- creates a bucket and S3-backed Omnigraph repo
- loads the checked-in context fixture
- starts `omnigraph-server` on `127.0.0.1:8080`

Supported behavior:

- downloads the rolling `edge` binary when one exists for the current platform
- otherwise clones `ModernRelay/omnigraph` and builds from source
- reuses an existing RustFS container if it is already running

Useful overrides:

- `WORKDIR=/path/to/state`
- `BUCKET=omnigraph-local`
- `PREFIX=repos/context`
- `RESET_REPO=1` to delete an existing partially initialized repo prefix before recreating it
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
not finish initializing the repo, rerun with `RESET_REPO=1` or choose a new
`PREFIX`.

## Container Deployment

Build the image:

```bash
docker build -t omnigraph-server:local .
```

Run against a local repo:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/repo.omni:/data/repo.omni" \
  omnigraph-server:local \
  /data/repo.omni --bind 0.0.0.0:8080
```

Run against an S3-backed repo:

```bash
docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN="change-me" \
  -e AWS_REGION="us-east-1" \
  omnigraph-server:local \
  s3://my-bucket/repos/example/releases/2026-04-10-v0.1.0 \
  --bind 0.0.0.0:8080
```

## Auth

The server can run unauthenticated for local development, but any shared or
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

Release artifacts are published with matching suffixes —
`omnigraph-server-<version>-<platform>.tar.gz` for the default build and
`omnigraph-server-<version>-<platform>-aws.tar.gz` for the AWS-enabled build.

The AWS build adds ~150 transitive deps and ~30-60s of first-build compile
time. Default builds don't pay that cost.

## AWS Secrets Manager

When the binary is built with `--features aws`, set
`OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET` to the ARN or name of a Secrets
Manager secret whose `SecretString` is a JSON object of
`{"actor_id": "token", ...}`:

```bash
omnigraph-server-aws s3://my-bucket/repos/example ...
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
