# Omnigraph Demo Infra Specs

## 1. Purpose

This document defines the infrastructure shape for the first Omnigraph demo deployment.

Chosen deployment model:

- AWS EC2
- one attached EBS data volume
- one Omnigraph server process
- bearer-token auth in the application
- no S3-backed repo for this milestone

This spec is for a demo deployment, not a production HA design.

## 2. Deployment Decision

### In scope

- single Omnigraph repo
- single EC2 instance
- single attached EBS volume for repo data
- TLS terminated at AWS edge or load balancer
- bearer token for all API calls except health check
- remote access to read/change/run APIs

### Out of scope

- multi-instance deployment
- autoscaling
- multi-AZ state replication
- S3-backed Lance repo
- public unauthenticated API access
- server-side init/load/branch-admin APIs

## 3. Current Application Constraints

These constraints drive the infra design:

- The live runtime is local-filesystem oriented for repo metadata and existence checks.
- The server is one process holding one shared `Omnigraph` handle.
- The HTTP API currently exposes `healthz`, `snapshot`, `read`, `change`, and `run` endpoints.
- The server does not expose init, bulk load, branch-create, or branch-merge endpoints.
- The repo must exist on disk before the service starts.

Practical implication:

- the Omnigraph repo must live on a mounted local volume
- initial repo creation and data loading happen out of band
- the safe demo topology is one instance per repo

## 4. Target AWS Topology

Recommended topology:

- 1 VPC
- 2 public subnets for the load balancer
- 2 private subnets for compute placement
- 1 internet-facing Application Load Balancer
- 1 target group pointing to the Omnigraph instance on port `8080`
- 1 EC2 instance in a private subnet
- 1 dedicated EBS volume mounted on the instance for Omnigraph data
- 1 IAM instance profile
- 1 security group for the ALB
- 1 security group for the EC2 instance
- 1 SSM Parameter Store or Secrets Manager secret for the bearer token
- optional Route53 record for the demo hostname
- optional CloudWatch log group and alarms

## 5. Networking

### Listener and routing

- ALB listener `443` for HTTPS
- optional ALB listener `80` redirecting to `443`
- target group protocol `HTTP`
- target group port `8080`
- health check path `/healthz`

### Security groups

ALB security group:

- allow inbound `443` from approved demo CIDRs or `0.0.0.0/0` if necessary
- allow outbound to EC2 security group on `8080`

EC2 security group:

- allow inbound `8080` only from the ALB security group
- no public inbound SSH
- use SSM Session Manager for admin access
- allow outbound internet access via NAT or equivalent for package/artifact retrieval if needed

### DNS

- create one hostname, for example `omnigraph-demo.<domain>`
- Route53 alias record should point to the ALB

## 6. Compute

### Instance shape

Use one general-purpose Linux EC2 instance.

Minimum starting point:

- `2` vCPU
- `8` GiB RAM

Preferred if the demo includes search-heavy or write-heavy flows:

- `4` vCPU
- `16` GiB RAM

### Operating system

Use a standard Linux AMI with:

- `systemd`
- SSM agent
- CloudWatch agent if logs/metrics will be shipped

### Scaling policy

For this milestone:

- no autoscaling group
- no scale-out
- one instance is the authoritative server for the repo

## 7. Storage

### EBS volume

Use one dedicated data volume for Omnigraph repo state.

Recommended starting point:

- volume type `gp3`
- separate from the root volume
- size based on expected demo dataset plus headroom

### Mount point

Recommended layout:

- Omnigraph root data dir: `/var/lib/omnigraph`
- active repo path: `/var/lib/omnigraph/data/demo.omni`

The volume should be mounted persistently through `/etc/fstab` or equivalent bootstrap logic.

### Backup

For demo safety:

- enable scheduled EBS snapshots
- snapshot before any major demo data refresh

## 8. Instance Filesystem Layout

Recommended layout:

- binary: `/opt/omnigraph/bin/omnigraph-server`
- config dir: `/etc/omnigraph`
- main config: `/etc/omnigraph/omnigraph.yaml`
- env file: `/etc/omnigraph/server.env`
- repo root: `/var/lib/omnigraph/data/demo.omni`

Optional:

- bootstrap scripts: `/opt/omnigraph/bootstrap`

## 9. Omnigraph Config

Recommended `omnigraph.yaml` on the instance:

```yaml
targets:
  local:
    uri: /var/lib/omnigraph/data/demo.omni

server:
  target: local
  bind: 0.0.0.0:8080

policy: {}
```

Startup command:

```bash
/opt/omnigraph/bin/omnigraph-server --config /etc/omnigraph/omnigraph.yaml
```

## 10. Authentication

### Required behavior

Bearer token auth is required for the demo.

Expected request format:

```http
Authorization: Bearer <token>
```

### Auth policy

- `/healthz` is unauthenticated
- all other endpoints require a valid bearer token
- missing token returns `401`
- invalid token returns `401`

### Secret source

Store the bearer token in:

- AWS SSM Parameter Store `SecureString`
- or AWS Secrets Manager

Bootstrap should materialize it into:

- `/etc/omnigraph/server.env`

Recommended server env var:

```bash
OMNIGRAPH_SERVER_BEARER_TOKEN=<secret>
```

### CLI compatibility

The remote CLI will need a way to send the same token.

Recommended CLI env var:

```bash
OMNIGRAPH_BEARER_TOKEN=<secret>
```

## 11. Process Management

Use a `systemd` unit.

Recommended unit shape:

- service name: `omnigraph-server.service`
- `EnvironmentFile=/etc/omnigraph/server.env`
- `ExecStart=/opt/omnigraph/bin/omnigraph-server --config /etc/omnigraph/omnigraph.yaml`
- restart policy: `on-failure`
- run as dedicated `omnigraph` user

The service should start only after:

- EBS volume is mounted
- repo path exists
- config and env files are in place

## 12. Repo Bootstrap Workflow

Because the server does not expose init/load endpoints, the repo must be provisioned separately.

Accepted bootstrap workflows:

- create and load the repo on the EC2 instance before first service start
- create the repo elsewhere, package it, and copy it onto the mounted EBS volume

The bootstrap flow must guarantee that:

- `_schema.pg` exists
- `_manifest.lance` exists
- the repo is readable by the `omnigraph` service user

## 13. Logging And Monitoring

### Logging

Use one of:

- journald only for the first demo
- CloudWatch agent forwarding systemd logs

The server already emits tracing logs, so infra should preserve stdout/stderr or journal output.

### Minimum alarms

Recommended CloudWatch alarms:

- EC2 instance status check failed
- ALB target unhealthy
- disk usage high on the Omnigraph data volume
- optional CPU or memory saturation

### Health checks

Use:

- ALB health check path `/healthz`
- health check success code `200`

## 14. Security Model

For this demo deployment:

- TLS terminates at the ALB
- the EC2 instance is not publicly reachable
- only the ALB can reach the application port
- only SSM is used for operator shell access
- bearer token is the only application-level auth mechanism

This is sufficient for a controlled demo environment.

## 15. Terraform Resource Checklist

Terraform should provision at least:

- VPC and subnet placement, if not using an existing network
- internet-facing ALB
- ALB target group and listeners
- Route53 alias record, if DNS is managed here
- EC2 instance
- EBS data volume
- EBS attachment
- security groups
- IAM role and instance profile
- SSM parameter or Secrets Manager secret for the bearer token
- optional CloudWatch log group and alarms

Terraform should also support:

- instance user-data bootstrap
- repo path mount configuration
- config file rendering
- systemd unit installation

## 16. Required Application Work Before Deploy

Infra alone is not enough. The app still needs a small deployment-facing patch set:

1. bearer-token middleware in `omnigraph-server`
2. remote CLI support for sending the bearer token header
3. deployment packaging for the binary and config files

These are small compared with adding S3 support and should be treated as part of the demo deploy effort.

## 17. Explicit Non-Goals For This Milestone

Do not build for this deploy:

- S3 repo support
- multi-instance write coordination
- autoscaling
- blue/green stateful cutover
- tenant isolation
- fine-grained RBAC

## 18. Success Criteria

The demo infra is complete when all of the following are true:

1. the EC2 instance boots and mounts the EBS volume
2. the Omnigraph repo exists at the configured local path
3. the server starts under `systemd`
4. the ALB health check passes on `/healthz`
5. authenticated `read` requests succeed through the public demo hostname
6. unauthenticated non-health requests return `401`
7. the repo survives instance restart because data is on EBS

## 19. Bottom Line

The first demo deploy is a simple stateful service:

- one EC2 instance
- one EBS-backed Omnigraph repo
- one bearer token
- one ALB in front

That matches the current application shape and avoids the highest-risk work item, which is S3-backed repo support.
