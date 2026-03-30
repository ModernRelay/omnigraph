# Omnigraph Infrastructure

## Live Environment

| Resource | Value |
|---|---|
| Endpoint | `https://d1lk4b7ad2a56j.cloudfront.net` |
| CloudFront | `d1lk4b7ad2a56j.cloudfront.net` |
| ALB (origin) | `omnigraph-885988689.us-east-1.elb.amazonaws.com` |
| Instance | `i-04ed54b3cd720fc21` |
| Region | `us-east-1` |

## Access

```bash
aws ssm start-session --target i-04ed54b3cd720fc21 --region us-east-1
```

## Terraform

Backend: Terraform Cloud — `collective-lab/omni-context`

```bash
cd infra
terraform plan
terraform apply
```

## Deployment

The instance is provisioned with systemd unit, config, EBS mount, and bearer token. To bring the service online:

1. Build the `omnigraph-server` binary for `x86_64-unknown-linux-gnu`
2. Copy it to `/opt/omnigraph/bin/omnigraph-server` on the instance
3. Bootstrap the repo at `/var/lib/omnigraph/data/context.omni`
4. Start the service: `systemctl start omnigraph-server`

## Bearer Token

Stored in SSM Parameter Store at `/omnigraph/server/bearer-token`.

CLI usage:

```bash
export OMNIGRAPH_BEARER_TOKEN="<token>"
```

Request format:

```
Authorization: Bearer <token>
```

## Filesystem Layout

| Path | Purpose |
|---|---|
| `/opt/omnigraph/bin/omnigraph-server` | Server binary |
| `/etc/omnigraph/omnigraph.yaml` | Server config |
| `/etc/omnigraph/server.env` | Environment (bearer token) |
| `/var/lib/omnigraph/data/context.omni` | Repo data (EBS volume) |
