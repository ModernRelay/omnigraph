# Container Runtime Contract

## Purpose

Omnigraph should use one portable OCI image for server runtime packaging before any ECS/Fargate
cutover.

This is the intended bridge:

- current runtime: `EC2 + S3`
- packaging: OCI image in ECR
- later compute migration: ECS/Fargate using the same image

The graph stays a separate deploy artifact.

For the current EC2 bridge phase, the live runtime selection is driven by two SSM parameters:

- `/omnigraph/server/image`
- `/omnigraph/server/target-uri`

Team access tokens should be stored under:

- `/omnigraph/server/tokens/<actor>`

## Runtime Contract

The server image runs `omnigraph-server` and expects one of these startup modes:

### Direct URI Mode

Preferred for cloud/runtime deployment:

- `OMNIGRAPH_TARGET_URI`
- optional `OMNIGRAPH_BIND` defaulting to `0.0.0.0:8080`

Example:

```bash
docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_TARGET_URI="s3://my-bucket/repos/mr-context-graph/releases/2026-04-02" \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN="..." \
  -e GEMINI_API_KEY="..." \
  -e AWS_REGION="us-east-1" \
  omnigraph-server:local
```

This starts:

```text
omnigraph-server s3://my-bucket/repos/mr-context-graph/releases/2026-04-02 --bind 0.0.0.0:8080
```

### Config Mode

Useful for local development or operator-managed config files:

- `OMNIGRAPH_CONFIG`
- optional `OMNIGRAPH_TARGET`
- optional `OMNIGRAPH_BIND`

Example:

```bash
docker run --rm -p 8080:8080 \
  -v "$PWD/omnigraph.yaml:/app/omnigraph.yaml:ro" \
  -e OMNIGRAPH_CONFIG="/app/omnigraph.yaml" \
  -e OMNIGRAPH_TARGET="active" \
  omnigraph-server:local
```

## Required Runtime Secrets / Env

For the current preview/runtime shape:

- `OMNIGRAPH_SERVER_BEARER_TOKEN`
- `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE`
- `GEMINI_API_KEY`
- `AWS_REGION`
- `OMNIGRAPH_SERVER_IMAGE` on EC2 bridge hosts

For S3-compatible local/on-prem backends like RustFS or MinIO:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- optional `AWS_ENDPOINT_URL`
- optional `AWS_ENDPOINT_URL_S3`
- optional `AWS_ALLOW_HTTP=true`
- optional `AWS_S3_FORCE_PATH_STYLE=true`

For AWS S3 on EC2 or ECS:

- prefer instance role / task role credentials
- do not set local S3 endpoint env vars

## Health And Networking

The image:

- listens on `0.0.0.0:8080` by default
- exposes port `8080`
- provides a container health check against `/healthz`

This is the same contract we want later for ECS target groups.

## Build Model

The current package flow is:

1. `omnigraph` triggers package via the reusable workflow in `ModernRelay/omnigraph-platform`
2. CodeBuild on Amazon Linux 2023 compiles `target/release/omnigraph-server`
3. Docker builds the runtime image from that compiled binary
4. the image is pushed to ECR with the commit SHA as the build tag
5. `dist/image.json` records `source_sha`, immutable `image_ref`, `image_digest`, and `artifact_prefix`

This avoids:

- glibc mismatch from Ubuntu-built binaries
- recompiling inside the container build

## Separation Of Concerns

The runtime image should contain:

- `omnigraph-server`
- startup/health behavior
- no embedded graph data

The graph remains separate:

- built from the seed repo
- published to a versioned S3 prefix
- selected at runtime through `OMNIGRAPH_TARGET_URI`

So deployment stays split:

- **server deploy** = choose image version
- **graph deploy** = choose target URI

The standard preview server deploy is now:

1. run `Package` from `omnigraph`
2. run `Deploy Preview Server` from `ModernRelay/omnigraph-platform`
3. that workflow updates `/omnigraph/server/image`
4. the EC2 host restarts `omnigraph-server.service`, which pulls the new image and runs it via `docker run`

## Local Validation

### Against RustFS / MinIO

```bash
docker build -t omnigraph-server:local .

docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_TARGET_URI="s3://mr-omni/graphs/mr-context-graph" \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN="demo-token" \
  -e GEMINI_API_KEY="$GEMINI_API_KEY" \
  -e AWS_REGION="us-east-1" \
  -e AWS_ACCESS_KEY_ID="rustfsadmin" \
  -e AWS_SECRET_ACCESS_KEY="rustfsadmin" \
  -e AWS_ENDPOINT_URL="http://host.docker.internal:9000" \
  -e AWS_ENDPOINT_URL_S3="http://host.docker.internal:9000" \
  -e AWS_ALLOW_HTTP="true" \
  -e AWS_S3_FORCE_PATH_STYLE="true" \
  omnigraph-server:local
```

### Against AWS S3

```bash
docker run --rm -p 8080:8080 \
  -e OMNIGRAPH_TARGET_URI="s3://omnigraph-repo-<account>-<region>/repos/mr-context-graph/releases/<release-id>" \
  -e OMNIGRAPH_SERVER_BEARER_TOKEN="..." \
  -e GEMINI_API_KEY="..." \
  -e AWS_REGION="us-east-1" \
  omnigraph-server:local
```

Use AWS credentials via your shell, instance profile, or task role.

## EC2 Bridge Phase

Before moving to ECS/Fargate, the intended next runtime step is:

- keep the current EC2 host
- pull the OCI image from ECR
- run it under systemd with `docker run`
- keep the graph in S3

That gives:

- one portable runtime artifact
- unchanged graph storage model
- easier rollback and eventual ECS migration

without changing compute and packaging at the same time.

The EC2 host bootstrap now installs Docker, grants the instance role ECR pull access, and writes a
Docker-based `omnigraph-server.service` unit. The service reads:

- `OMNIGRAPH_SERVER_IMAGE` from `/etc/omnigraph/server.env`
- `OMNIGRAPH_TARGET_URI` from `/etc/omnigraph/server.env`
- `OMNIGRAPH_SERVER_BEARER_TOKENS_FILE` from `/etc/omnigraph/server.env`

The bearer token file is generated from SSM path parameters at:

- `/omnigraph/server/tokens/<actor>`

Rollback is therefore:

- put the previous image ref back into `/omnigraph/server/image`
- restart `omnigraph-server.service`
