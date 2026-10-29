# OmniGraph user guide

OmniGraph is a typed property-graph engine with atomic graph commits,
versioned branches, full-text and vector search, and local or object-storage
deployment.

## Get started

- [Install](install.md)
- [Quickstart](quickstart.md): schema → load → query → branch → merge
- [Core concepts](concepts/index.md)

## Model and data

- [Schema language](schema/index.md)
- [Query language](queries/index.md), [session settings](queries/settings.md), [explain](queries/explain.md), [explaining a load statement](queries/explain-load.md), and the [engine catalogue](queries/catalogue.md)
- [Traversal patterns and edge selections](queries/traversal.md)
- [Diagnostics](queries/diagnostics.md)
- [Mutations and loading](mutations/index.md) and [conditional mutations](mutations/conditional.md)
- [Search](search/index.md) and [embeddings](search/embeddings.md)
- [Blob values](blobs.md)
- [Storage and durability](concepts/storage.md)

## History and collaboration

- [Branches, commits, and snapshots](branching/index.md)
- [Commit changes, diffs, branch review, change feeds, and baselines](branching/changes.md)
- [Merging and conflicts](branching/merge.md)

## CLI and HTTP

- [CLI guide](cli/index.md)
- [CLI reference](cli/reference.md)
- [Operator profiles, targets and credentials](cli/targets.md)
- [Open Studio on a server target](cli/studio.md)
- [Managed data credentials and queries](cli/managed-data.md)
- [Managed creation, configuration and deletion](cli/managed-lifecycle.md)
- [Managed organization members and invitations](cli/managed-team.md)
- [HTTP server](operations/server.md)
- [MCP](operations/mcp.md)
- [Troubleshooting](operations/troubleshooting.md)

## Operate a deployment

- [Operating a cluster](clusters/index.md)
- [`cluster.yaml` reference](clusters/config.md)
- [Deployment](deployment.md)
- [Storage failures and restart](operations/storage-failures.md)
- [Server logs and traces](operations/observability.md)
- [Authorization and actors](operations/policy.md)
- [Maintenance](operations/maintenance.md)
- [Served maintenance](operations/served-maintenance.md)
- [S3 cluster roots](operations/s3-cluster-roots.md): deployment, recovery and maintenance with the server stopped
- [Upgrade from 0.13 to 0.14](operations/upgrade-0.14.md): checklist, YAML and CLI replacements
- [Storage-format upgrades](operations/upgrade.md)

## Releases and internals

User-visible changes are recorded in the [release notes](../releases/).
See [0.14 highlights and upgrade actions](../releases/v0.14.0-overview.md)
for a short overview of the release.
Contributors should start with the [developer guide](../dev/index.md). Design
decisions and proposals live in [RFCs](../rfcs/).
