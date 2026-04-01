# Omnigraph S3 Platform Specs

## 1. Purpose

This document defines the target architecture after Omnigraph moves from a host-local repo to an S3-compatible repo.

This is the recommended futureproof direction because it separates:

- runtime compute
- graph storage
- build infrastructure
- deployment orchestration

It also removes the current dependency on one specific EC2 instance and one attached EBS volume.

## 2. Target Outcome

Target platform:

- Omnigraph repo stored under an S3 prefix
- Omnigraph server runtime deployed as a container
- ECS Fargate service behind ALB and CloudFront
- build and package jobs executed on Amazon Linux 2023
- image artifacts stored in ECR
- graph artifacts stored in S3
- deploy orchestration from GitHub Actions or CodePipeline
- RustFS as the canonical on-prem S3-compatible backend

Practical result:

- the server becomes stateless compute
- the graph becomes a versioned artifact
- deploys stop depending on copying binaries and repo folders onto one VM

## 3. Why S3 First

Today the runtime still assumes local metadata storage:

- only `LocalStorageAdapter` exists in `crates/omnigraph/src/storage.rs`
- `_schema.pg` is read and written through that adapter in `crates/omnigraph/src/db/omnigraph.rs`
- graph commit and run registry existence checks use `storage.exists(...)` in `crates/omnigraph/src/db/graph_coordinator.rs`

That means Omnigraph is not yet truly object-store-native even though Lance itself supports object storage.

S3 compatibility is the key boundary to fix first because:

- it removes the local-disk repo constraint
- it makes ECS/Fargate viable
- it makes graph deploys immutable and versioned
- it separates runtime failures from storage failures

## 4. Application Changes Required

Infra alone cannot deliver S3-backed Omnigraph. The app must change first.

### 4.1 Storage Adapter

Add an `S3StorageAdapter` implementing:

- `read_text`
- `write_text`
- `exists`

Expected scope:

- `_schema.pg`
- any future text metadata files
- existence checks for graph registry datasets

Implementation notes:

- use the AWS Rust SDK or `object_store`
- support `s3://bucket/prefix/...` URIs
- use `HeadObject` or equivalent for `exists`
- keep `LocalStorageAdapter` for local dev and tests

### 4.2 Repo Root Semantics

The following paths must work correctly under an S3 prefix:

- `_schema.pg`
- `_manifest.lance`
- `_graph_commits.lance`
- `_graph_runs.lance`
- `nodes/...`
- `edges/...`

`join_uri` and URI normalization must preserve scheme-aware path joining and must not assume local filesystem semantics.

### 4.3 Open / Init / Load / Refresh

The live code paths that need S3 correctness are:

- `Omnigraph::init`
- `Omnigraph::open`
- `GraphCoordinator::open`
- `GraphCoordinator::open_branch`
- any path that checks registry existence before open

Expected behavior:

- `omnigraph init s3://bucket/prefix`
- `omnigraph load s3://bucket/prefix`
- `omnigraph-server` target URI can be `s3://bucket/prefix`

### 4.4 Locking and Concurrency

S3 does not provide local filesystem locking semantics.

Before using a single S3-backed repo from multiple writers, Omnigraph should define one of:

- single-writer process policy
- optimistic transaction policy with manifest/version conflict handling only
- external distributed lock

For the first milestone, the recommended rule is:

- one writer deployment at a time
- read replicas may exist later, but concurrent writes are not part of the initial S3 cutover

### 4.5 Test Bar

Required before infra cutover:

- local integration tests against S3-compatible object storage, with RustFS as the canonical on-prem backend
- end-to-end repo lifecycle test:
  - init
  - load
  - open
  - snapshot
  - read
  - run publish
- regression tests for `_schema.pg`, `_graph_commits.lance`, and `_graph_runs.lance` existence checks over S3

## 5. Runtime Architecture After S3

Once the repo is truly S3-backed, the runtime should move to ECS.

Recommended topology:

- 1 VPC
- 2 public subnets for ALB
- 2 private subnets for ECS tasks
- 1 ECS cluster
- 1 ECS service with desired count `1` initially
- 1 Fargate task definition for `omnigraph-server`
- 1 ALB target group to the ECS service
- 1 CloudFront distribution in front of ALB
- 1 ECR repository for `omnigraph-server`
- 1 S3 bucket for Omnigraph graph artifacts and repo prefixes
- 1 S3 bucket for CI/CD artifacts if needed
- 1 CloudWatch log group
- SSM Parameter Store or Secrets Manager for bearer token and Gemini key

Why ECS/Fargate becomes reasonable only here:

- the server no longer depends on attached EBS state
- tasks can be replaced without moving repo data
- scale and rollback are image-driven, not disk-driven

## 6. Storage Layout on S3

Use one dedicated bucket or one clearly separated prefix for graph repos.

Recommended layout:

- `s3://<bucket>/repos/<graph-name>/current/`
- `s3://<bucket>/repos/<graph-name>/releases/<release-id>/`

Recommended rule:

- never mutate the currently served graph in place during deploy
- build a new versioned repo prefix
- flip the runtime to the new prefix only after validation

Example:

- `s3://omnigraph-data/repos/mr-omni/releases/2026-03-31-a7f3d33/`
- ECS task env or config points at that exact prefix

This gives rollback by changing the repo URI back to the previous prefix.

## 7. CI/CD Design

Use a split pipeline.

### 7.1 CI

Keep GitHub Actions plus Blacksmith for fast PR and branch validation:

- lint
- unit tests
- integration tests that do not need AL2023 runtime compatibility
- RustFS-backed `s3://` integration tests in the `CI` workflow

### 7.2 Build

Move release packaging to AWS CodeBuild on Amazon Linux 2023.

Reason:

- avoids the glibc mismatch already seen with Ubuntu-built Linux artifacts
- gives a deterministic runtime-compatible build environment

CodeBuild outputs:

- container image for `omnigraph-server`
- optional CLI artifact for operators
- graph artifact metadata
- native Linux tarball built against AL2023-compatible userspace

### 7.3 Image Packaging

Package the server as a container image and push to ECR.

The container should contain:

- `omnigraph-server`
- optional `omnigraph` CLI for diagnostics only
- a minimal runtime base image

### 7.4 Graph Build

Graph build should be a separate pipeline concern from server image build.

Recommended model:

- server repo builds server image
- seed/graph repo builds graph artifact

Graph build outputs:

- a complete Omnigraph repo under a versioned S3 prefix
- or a tarball uploaded to S3 and then expanded into the target prefix by a deployment step

The important contract is:

- runtime deploy chooses an image version
- graph deploy chooses a repo prefix version

Those two should be independently promotable.

### 7.5 Deploy

Recommended deploy orchestrator:

- GitHub Actions manually dispatches deploys
- GitHub Actions assumes an AWS role via OIDC
- GitHub Actions updates ECS task definition and service
- GitHub Actions uses a separate `Package` workflow to invoke CodeBuild

Deploy steps:

1. build or select server image in ECR
2. build or select graph repo prefix in S3
3. register new ECS task definition revision with:
   - new image tag
   - new `OMNIGRAPH_TARGET_URI`
4. update ECS service
5. wait for steady state
6. smoke test `/healthz`, `/snapshot`, and one read query

### 7.6 Rollback

Rollback should be configuration-based, not host-repair-based.

Rollback inputs:

- previous ECR image tag
- previous S3 repo prefix

Rollback action:

- deploy previous ECS task definition or register a new one pointing back to prior values

## 8. Secrets and Configuration

Use Secrets Manager or SSM Parameter Store for:

- bearer token
- Gemini API key

Task env should include:

- `OMNIGRAPH_SERVER_BEARER_TOKEN`
- `GEMINI_API_KEY`
- `OMNIGRAPH_TARGET_URI=s3://...`

Prefer ECS task secrets injection over writing secrets to files on disk.

## 9. Terraform Changes

This is the concrete Terraform delta from the current EC2 + EBS design.

### 9.1 Keep

Keep these with minor adaptation:

- VPC
- public/private subnets
- ALB
- CloudFront
- Route53
- SSM Parameter Store or Secrets Manager

### 9.2 Remove or Retire

These become obsolete after the S3 cutover:

- dedicated runtime EC2 instance
- EBS volume for active repo
- EC2 Instance Connect endpoint
- systemd bootstrap for server runtime
- host-mounted repo path assumptions

### 9.3 Add

#### Runtime

- `aws_ecr_repository`
- `aws_ecs_cluster`
- `aws_ecs_task_definition`
- `aws_ecs_service`
- task execution role
- task role
- CloudWatch log group for ECS task logs
- target group attachment from ALB to ECS

#### Storage

- `aws_s3_bucket` for graph repos
- bucket versioning
- bucket encryption
- optional lifecycle rules

#### Build

- `aws_codebuild_project`
- CodeBuild service role
- optional S3 artifact bucket if separate from graph bucket

#### IAM

ECS task role permissions:

- `s3:GetObject`
- `s3:ListBucket`
- possibly `s3:PutObject` only if runtime writes are allowed
- `ssm:GetParameter` or Secrets Manager read

CodeBuild permissions:

- ECR push
- S3 read/write
- CloudWatch Logs
- optional Parameter Store access

#### CI OIDC

- IAM OIDC provider for GitHub if not already present
- deploy role assumable from GitHub Actions

### 9.4 Recommended Module Split

Recommended Terraform structure:

- `network/`
- `runtime-ecs/`
- `storage-s3/`
- `build-codebuild/`
- `edge/`
- `identity/`

If kept in one root module, still separate by file:

- `ecr.tf`
- `ecs.tf`
- `s3.tf`
- `codebuild.tf`
- `iam_ci.tf`

## 10. Recommended Rollout Plan

### Phase 1

Implement S3 support in Omnigraph and prove it locally:

- local S3-compatible integration tests
- server reading from `s3://...`

### Phase 2

Add build plane:

- CodeBuild AL2023
- ECR
- graph artifact bucket

Keep runtime on EC2 temporarily if desired during validation.

### Phase 3

Move runtime to ECS Fargate:

- one task
- same ALB / CloudFront edge
- S3-backed repo

### Phase 4

Add deploy automation and rollback:

- GitHub OIDC
- deploy workflow
- smoke tests

## 11. Recommendation

Recommended target architecture:

- S3-backed Omnigraph repo
- ECS Fargate runtime
- CodeBuild AL2023 release builds
- ECR for server images
- GitHub Actions as orchestration layer

Recommended immediate next move:

- do not update the existing runtime Terraform first
- first implement and validate S3 support in Omnigraph
- in parallel, prepare Terraform for:
  - S3 graph bucket
  - ECR
  - CodeBuild
  - ECS service

The key principle is:

- storage boundary first
- runtime migration second
- CI/CD formalization alongside the migration
