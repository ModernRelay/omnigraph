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
curl -fsSL https://raw.githubusercontent.com/ModernRelay/omnigraph-public/main/scripts/local-rustfs-bootstrap.sh | bash
```

The bootstrap:

- starts a local RustFS-backed object store
- creates a bucket and S3-backed Omnigraph repo
- loads the checked-in context fixture
- starts `omnigraph-server` on `127.0.0.1:8080`

Supported behavior:

- downloads the rolling `edge` binary when one exists for the current platform
- otherwise clones `ModernRelay/omnigraph-public` and builds from source
- reuses an existing RustFS container if it is already running

Useful overrides:

- `WORKDIR=/path/to/state`
- `BUCKET=omnigraph-local`
- `PREFIX=repos/context`
- `BIND=127.0.0.1:8080`
- `RUSTFS_CONTAINER_NAME=omnigraph-rustfs-demo`

The bootstrap expects:

- Docker
- `curl`
- either a matching release asset or a local Rust toolchain plus `git`

If `aws` is not installed, the script attempts a user-local AWS CLI install via
`python3 -m pip`. Docker Desktop or another Docker daemon must already be
running.

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
internet-facing deployment should set:

- `OMNIGRAPH_SERVER_BEARER_TOKEN`

The health endpoint `/healthz` remains suitable for load balancer health checks.

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
