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
- [Query language](queries/index.md)
- [Mutations and loading](mutations/index.md)
- [Search](search/index.md) and [embeddings](search/embeddings.md)
- [Blob values](blobs.md)
- [Storage and durability](concepts/storage.md)

## History and collaboration

- [Branches, commits, and snapshots](branching/index.md)
- [Commit changes, change feeds, and baselines](branching/changes.md)
- [Merging and conflicts](branching/merge.md)

## CLI and HTTP

- [CLI guide](cli/index.md)
- [CLI reference](cli/reference.md)
- [Managed data credentials and queries](cli/managed-data.md)
- [HTTP server](operations/server.md)
- [Troubleshooting](operations/troubleshooting.md)

## Operate a deployment

- [Operating a cluster](clusters/index.md)
- [`cluster.yaml` reference](clusters/config.md)
- [Deployment](deployment.md)
- [Authorization and actors](operations/policy.md)
- [Maintenance](operations/maintenance.md)
- [Storage-format upgrades](operations/upgrade.md)

## Releases and internals

User-visible changes are recorded in the [release notes](../releases/).
Contributors should start with the [developer guide](../dev/index.md). Design
decisions and proposals live in [RFCs](../rfcs/).
