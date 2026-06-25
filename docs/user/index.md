# User Docs

**Audience:** users, CLI users, HTTP clients, and self-hosting operators

This is the public-facing entry point. These docs describe behavior, commands,
configuration, and operational contracts without requiring knowledge of internal
recovery mechanics or contributor-only invariants. They are organized by topic —
start with install, then follow the section that matches your task.

## Start here

| Goal | Read |
|---|---|
| Install OmniGraph | [install.md](install.md) |
| Run the core loop end to end | [quickstart.md](quickstart.md) |
| Understand the model | [concepts/index.md](concepts/index.md) |
| Run the CLI | [cli/index.md](cli/index.md) |
| Look up every CLI flag and config field | [cli/reference.md](cli/reference.md) |

## Schema & queries

| Goal | Read |
|---|---|
| Write schemas (the `.pg` language) | [schema/index.md](schema/index.md) |
| Read schema-lint diagnostic codes | [schema/lint.md](schema/lint.md) |
| Write queries (the `.gq` language) | [queries/index.md](queries/index.md) |
| Write data — inserts, updates, deletes | [mutations/index.md](mutations/index.md) |
| Use vector / full-text / hybrid search | [search/index.md](search/index.md) |
| Generate embeddings | [search/embeddings.md](search/embeddings.md) |
| Build and use indexes | [search/indexes.md](search/indexes.md) |

## Branching & version control

| Goal | Read |
|---|---|
| Work with branches and commits | [branching/index.md](branching/index.md) |
| Read past versions (time travel) | [branching/time-travel.md](branching/time-travel.md) |
| Merge branches and resolve conflicts | [branching/merge.md](branching/merge.md) |
| Coordinate multi-query workflows | [branching/transactions.md](branching/transactions.md) |
| Read diffs and change feeds | [branching/changes.md](branching/changes.md) |

## Operations

| Goal | Read |
|---|---|
| Deploy the binary or container | [deployment.md](deployment.md) |
| Use HTTP endpoints | [operations/server.md](operations/server.md) |
| Connect an MCP agent (Claude, Cursor, …) | [operations/mcp.md](operations/mcp.md) |
| Compact, repair, and clean old versions | [operations/maintenance.md](operations/maintenance.md) |
| Configure Cedar authorization | [operations/policy.md](operations/policy.md) |
| Track actors and audit behavior | [operations/audit.md](operations/audit.md) |
| Interpret errors and output formats | [operations/errors.md](operations/errors.md) |

## Clusters

| Goal | Read |
|---|---|
| Deploy and operate a cluster (how-to) | [clusters/index.md](clusters/index.md) |
| Reference every `cluster.yaml` key and command | [clusters/config.md](clusters/config.md) |

## Concepts & reference

| Goal | Read |
|---|---|
| Understand the model and L1/L2 framing | [concepts/index.md](concepts/index.md) |
| Understand graph layout and URI support | [concepts/storage.md](concepts/storage.md) |
| Look up constants and tunables | [reference/constants.md](reference/constants.md) |

## Releases

Release notes live in [releases/](../releases/). Use them for user-visible
changes between versions, not for contributor design history.

## Boundary

User docs focus on stable behavior. If a paragraph needs to explain internal
sidecars, Lance API blockers, or test strategy, it probably belongs in
[docs/dev/index.md](../dev/index.md) or a developer-area document instead.
