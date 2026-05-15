# User Docs

**Audience:** users, CLI users, HTTP clients, and self-hosting operators

This is the public-facing entry point. These docs should describe behavior,
commands, configuration, and operational contracts without requiring knowledge
of MRs, internal recovery mechanics, or contributor-only invariants.

## Start Here

| Goal | Read |
|---|---|
| Install OmniGraph | [install.md](install.md) |
| Run the CLI locally | [cli.md](cli.md) |
| Look up every CLI flag and config field | [cli-reference.md](cli-reference.md) |
| Write schemas | [schema-language.md](schema-language.md) |
| Read schema-lint diagnostic codes | [schema-lint.md](schema-lint.md) |
| Write queries and mutations | [query-language.md](query-language.md) |
| Use embeddings | [embeddings.md](embeddings.md) |

## Operate A Repo

| Goal | Read |
|---|---|
| Understand repo layout and URI support | [storage.md](storage.md) |
| Work with branches, commits, and snapshots | [branches-commits.md](branches-commits.md) |
| Coordinate multi-query workflows | [transactions.md](transactions.md) |
| Read diffs and change feeds | [changes.md](changes.md) |
| Build and use indexes | [indexes.md](indexes.md) |
| Compact and clean old versions | [maintenance.md](maintenance.md) |
| Interpret errors and output formats | [errors.md](errors.md) |

## Run The Server

| Goal | Read |
|---|---|
| Deploy the binary or container | [deployment.md](deployment.md) |
| Use HTTP endpoints | [server.md](server.md) |
| Configure Cedar authorization | [policy.md](policy.md) |
| Track actors and audit behavior | [audit.md](audit.md) |

## Releases

Release notes live in [releases/](../releases/). Use them for user-visible
changes between versions, not for contributor design history.

## Boundary

User docs should focus on stable behavior. If a paragraph needs to explain
internal sidecars, Lance API blockers, MR numbers, test strategy, or review
rules, it probably belongs in [docs/dev/index.md](../dev/index.md) or a developer-area document
instead.
