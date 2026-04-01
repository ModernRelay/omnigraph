# Terraform Update Plan

## Goal

Update the AWS Terraform stack so Omnigraph can:

- keep the current runtime shape for now: `EC2 + ALB + CloudFront`
- stop depending on EBS for the live graph repo
- store the live repo in `S3`
- build deployable Amazon Linux 2023 artifacts in `CodeBuild`
- keep GitHub Actions as the control plane
- be ready for a later ECS/Fargate cutover without rewriting the storage or build story

This plan is intentionally not resource-scarce. If a resource materially improves packaging, deploy safety, rollback, or future migration, add it now.

## Decision Summary

### What stays

- `infra/vpc.tf`
- `infra/alb.tf`
- `infra/cdn.tf`
- `infra/dns.tf`
- the current EC2 runtime host in `infra/compute.tf`
- SSM bearer token and Gemini key in `infra/secrets.tf`

### What changes now

- add an S3 repo bucket for live Omnigraph repos
- add a separate S3 artifact bucket for build and deploy artifacts
- add an S3 access logs bucket
- add a gateway VPC endpoint for S3 so the private EC2 host can reach S3 without relying on NAT
- add CodeBuild on Amazon Linux 2023 for packaging
- add a GitHub OIDC role so Actions can start builds and read results without static AWS keys
- add an ECR repository now, even though runtime is still EC2
- extend the EC2 instance role so the current server can read and write the S3 repo bucket
- add a runtime SSM parameter for the active Omnigraph target URI

### What does not change yet

- no ECS cluster
- no Fargate service
- no ECR-backed runtime deploy
- no ALB target group migration
- no CloudFront origin change

## Terraform Resources To Add

## 1. S3 Repo Storage

Add a dedicated bucket for Omnigraph repos.

Recommended resource set:

- `aws_s3_bucket.omnigraph_repo`
- `aws_s3_bucket_versioning.omnigraph_repo`
- `aws_s3_bucket_server_side_encryption_configuration.omnigraph_repo`
- `aws_s3_bucket_public_access_block.omnigraph_repo`
- `aws_s3_bucket_lifecycle_configuration.omnigraph_repo`
- `aws_s3_bucket_logging.omnigraph_repo`
- `aws_s3_bucket_policy.omnigraph_repo_tls_only`

Recommended naming:

- bucket: `<project>-repo-<account>-<region>`

Recommended layout inside the bucket:

- `repos/<graph>/releases/<release-id>/`
- `repos/<graph>/scratch/`

Recommended release-id format:

- `YYYY-MM-DD-<git-sha>`

Rules:

- the server should point at an exact release prefix, not a mutable `current/` prefix
- rollback should mean switching back to the previous exact S3 URI, not rewriting objects in place
- enable bucket versioning anyway for operator recovery

Recommended lifecycle:

- keep all current repo objects
- optionally expire incomplete multipart uploads after `7` days
- do not auto-delete old releases until deploy/rollback discipline is stable

## 2. S3 Artifact Storage

Add a separate bucket for package artifacts.

Recommended resource set:

- `aws_s3_bucket.omnigraph_artifacts`
- `aws_s3_bucket_versioning.omnigraph_artifacts`
- `aws_s3_bucket_server_side_encryption_configuration.omnigraph_artifacts`
- `aws_s3_bucket_public_access_block.omnigraph_artifacts`
- `aws_s3_bucket_lifecycle_configuration.omnigraph_artifacts`
- `aws_s3_bucket_logging.omnigraph_artifacts`
- `aws_s3_bucket_policy.omnigraph_artifacts_tls_only`

What goes here:

- native AL2023 binary tarballs
- `SHA256SUMS`
- graph repo archives if we choose to package them
- optional source bundles uploaded from GitHub Actions

Recommended lifecycle:

- keep release artifacts for at least `90` days
- keep tagged release artifacts indefinitely if practical
- expire failed or scratch prefixes aggressively

Recommended layout:

- `builds/<git-sha>/omnigraph-linux-x86_64.tar.gz`
- `builds/<git-sha>/SHA256SUMS`
- `graphs/<graph>/<release-id>/...`

## 3. S3 Access Logs Bucket

Add a dedicated logs bucket instead of mixing access logs into repo or artifact buckets.

Recommended resource set:

- `aws_s3_bucket.s3_logs`
- `aws_s3_bucket_server_side_encryption_configuration.s3_logs`
- `aws_s3_bucket_public_access_block.s3_logs`
- `aws_s3_bucket_lifecycle_configuration.s3_logs`
- `aws_s3_bucket_policy.s3_logs_tls_only`

Lifecycle:

- retain logs `30` to `90` days

## 4. S3 VPC Endpoint

Because the runtime instance is in a private subnet, add a gateway endpoint for S3.

Recommended resource:

- `aws_vpc_endpoint.s3`

Attach it to:

- `aws_route_table.private`

Why:

- private EC2 to S3 traffic should not depend on the NAT gateway
- this lowers cost and removes a useless failure point once the graph repo lives in S3

## 5. CodeBuild Packaging

Add a dedicated CodeBuild project for AL2023 packaging.

Recommended resources:

- `aws_cloudwatch_log_group.codebuild_package`
- `aws_iam_role.codebuild_package`
- `aws_iam_role_policy.codebuild_package`
- `aws_codebuild_project.package`

CodeBuild responsibilities:

- build `omnigraph-server`
- build `omnigraph`
- produce `omnigraph-linux-x86_64.tar.gz`
- optionally build and push a container image to ECR
- optionally emit graph packaging metadata

CodeBuild should use:

- Amazon Linux 2023 standard image
- privileged mode enabled only if building Docker images in the same project

The buildspec already exists:

- [buildspec.package.yml](/Users/andrew/code/omnigraph/buildspec.package.yml)

The GitHub Actions wrapper already exists:

- [package.yml](/Users/andrew/code/omnigraph/.github/workflows/package.yml)

Terraform should wire CodeBuild to match that workflow, not invent a second packaging path.

## 6. ECR Repository

Add ECR now, even though runtime is still EC2.

Recommended resources:

- `aws_ecr_repository.omnigraph_server`
- `aws_ecr_lifecycle_policy.omnigraph_server`

Why now:

- CodeBuild already has optional image push logic
- image packaging should be validated before the later ECS cutover
- this avoids another Terraform pass just to start producing OCI artifacts

Recommended lifecycle:

- keep recent untagged images
- retain semver and branch-tagged images

## 7. GitHub OIDC

Add GitHub OIDC for Actions-to-AWS auth.

Recommended resources:

- `aws_iam_openid_connect_provider.github`
- `aws_iam_role.github_actions`
- `aws_iam_role_policy.github_actions_ci`

This role should allow:

- `codebuild:StartBuild`
- `codebuild:BatchGetBuilds`
- `s3:GetObject`
- `s3:PutObject`
- `s3:ListBucket`
- `ecr:DescribeRepositories`

Scope it to:

- this repository only
- the main branch and explicit workflow refs

GitHub repo variables expected by the existing workflow:

- `AWS_REGION`
- `AWS_ROLE_TO_ASSUME`
- `AWS_CODEBUILD_PACKAGE_PROJECT`

## 8. Runtime Target URI Parameter

Add a separate SSM parameter for the live repo URI.

Recommended resource:

- `aws_ssm_parameter.repo_target_uri`

Recommended name:

- `/${var.project_name}/server/target-uri`

Why:

- the runtime target should be explicit configuration, not baked into user-data assumptions about local disk
- the deploy step can update this parameter when promoting a new S3 release prefix
- it provides one canonical source of truth for what graph is supposed to be live

## 9. EC2 Role Extension

Keep the current EC2 host for now, but extend its IAM role.

Update:

- `aws_iam_role_policy.read_ssm_secrets`
- add a new policy like `aws_iam_role_policy.s3_repo_access`

Required permissions:

- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`
- `s3:ListBucket`

Scope:

- repo bucket
- specific repo prefixes if you want tighter scoping

Optional but useful:

- `s3:GetBucketLocation`

The EC2 host should not need static AWS keys. Use the instance profile only.

## 10. Optional But Worth Adding Now

These are not strictly required for first cutover, but are worth adding now:

- `aws_cloudwatch_metric_alarm` for ALB `5XX`
- `aws_cloudwatch_metric_alarm` for CodeBuild failures
- `aws_cloudwatch_metric_alarm` for EC2 status check failure
- `aws_ssm_parameter.package_project_name` if you want the project name also stored in AWS

I would not block the cutover on alarms, but I would add them in the same Terraform pass if time allows.

## File-By-File Terraform Changes

## New Files

Create these files:

- `infra/s3.tf`
- `infra/build.tf`
- `infra/oidc.tf`
- `infra/logs.tf`

### `infra/s3.tf`

Put here:

- repo bucket resources
- artifact bucket resources
- S3 logs bucket resources
- S3 gateway VPC endpoint

### `infra/build.tf`

Put here:

- CodeBuild log group
- CodeBuild role and policies
- CodeBuild project
- ECR repository and lifecycle policy

### `infra/oidc.tf`

Put here:

- GitHub OIDC provider
- GitHub Actions assume role
- GitHub Actions IAM policy

### `infra/logs.tf`

Put here:

- CloudWatch alarms for CodeBuild, ALB, and EC2

## Existing Files To Update

### `infra/variables.tf`

Add variables for:

- `repo_bucket_name`
- `artifact_bucket_name`
- `logs_bucket_name`
- `repo_graph_name`
- `initial_repo_target_uri`
- `codebuild_compute_type`
- `codebuild_image`
- `github_repository`
- `github_oidc_subjects`
- `enable_ecr_packaging`

Optional:

- `repo_release_retention_days`
- `artifact_retention_days`

### `infra/secrets.tf`

Keep current parameters and add:

- `aws_ssm_parameter.repo_target_uri`

Do not move bearer token or Gemini key out of SSM.

### `infra/compute.tf`

Keep EC2 and EBS for now, but update:

- instance IAM role to include S3 repo access
- user-data to render `OMNIGRAPH_TARGET_URI`
- user-data to render `AWS_REGION`
- user-data to stop assuming the live repo is always `/var/lib/omnigraph/data/context.omni`

Recommended runtime behavior:

- if `OMNIGRAPH_TARGET_URI` is set, use that
- otherwise fall back to local disk only for rollback/testing

Keep EBS attached in this phase as a rollback safety net. Do not delete it yet.

### `infra/outputs.tf`

Add outputs for:

- repo bucket name
- artifact bucket name
- logs bucket name
- CodeBuild project name
- ECR repository URL
- repo target URI parameter name
- GitHub OIDC role ARN

## Build and Deploy Flow After This Update

## Package Flow

1. GitHub Actions runs tests as it does now.
2. `Package` workflow assumes the GitHub OIDC role.
3. GitHub Actions starts the CodeBuild project.
4. CodeBuild builds on AL2023.
5. CodeBuild publishes:
   - native tarball to the artifact bucket
   - optional image to ECR

## Graph Promotion Flow

1. Build graph repo locally or in CI.
2. Publish graph repo to:
   - `s3://<repo-bucket>/repos/<graph>/releases/<release-id>/`
3. Update `/${project}/server/target-uri` to that exact S3 URI.
4. Restart `omnigraph-server` on EC2.
5. Smoke test:
   - `/healthz`
   - authenticated `/snapshot`
   - authenticated `/read`

## Rollback Flow

1. Point `repo_target_uri` back to the previous release prefix.
2. Restart the service.
3. If packaging changed too, roll the binary back using the previous artifact from the artifact bucket.

## Recommended Naming

- repo bucket:
  - `omnigraph-repo-<account>-<region>`
- artifact bucket:
  - `omnigraph-artifacts-<account>-<region>`
- logs bucket:
  - `omnigraph-s3-logs-<account>-<region>`
- ECR repo:
  - `omnigraph-server`
- CodeBuild project:
  - `omnigraph-package-al2023`

## What Not To Add Yet

Do not add these in this pass:

- `aws_ecs_cluster`
- `aws_ecs_task_definition`
- `aws_ecs_service`
- `aws_lb_target_group` changes for ECS
- `aws_codedeploy_*`
- `aws_codepipeline_*`

Those belong to the later post-S3-validation compute migration.

## Concrete Minimum Done Definition

Terraform update is complete when all of these exist:

- repo bucket with versioning, encryption, public access block, lifecycle, and TLS-only policy
- artifact bucket with versioning, encryption, public access block, lifecycle, and TLS-only policy
- logs bucket
- S3 gateway VPC endpoint on the private route table
- CodeBuild packaging project on AL2023
- CodeBuild IAM role and log group
- GitHub OIDC provider and role
- ECR repository
- `repo_target_uri` SSM parameter
- EC2 IAM policy for repo bucket access

At that point, the stack is ready for:

- `EC2 + S3-backed Omnigraph repo`
- AL2023-compatible server packaging
- a later clean move to ECS/Fargate without redoing storage or packaging
