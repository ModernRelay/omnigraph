# Developer Docs

**Audience:** contributors, maintainers, and coding agents

This is the contributor-facing entry point. These docs explain architecture,
invariants, implementation contracts, test ownership, and upstream Lance
constraints. User-facing behavior should still be documented through
[docs/user/index.md](../user/index.md) and the relevant public reference docs.

## Required For Every Non-Trivial Change

| Need | Read |
|---|---|
| Architectural rules, known gaps, deny-list | [invariants.md](invariants.md) |
| Upstream Lance source-of-truth index | [lance.md](lance.md) |
| Existing test coverage and test placement | [testing.md](testing.md) |

## Architecture And Storage

| Area | Read |
|---|---|
| System structure, L1/L2 framing, component diagrams | [architecture.md](architecture.md) |
| On-disk layout, manifest schema, URI behavior | [storage.md](../user/storage.md) |
| Direct-publish writes, D2, staged writes, recovery sidecars | [runs.md](runs.md) |
| Query execution, mutation execution, loader flow | [execution.md](execution.md) |
| DataFusion: current state, passive wins, future improvements | [datafusion-future-improvements.md](datafusion-future-improvements.md) |
| Index lifecycle and graph topology indexes | [indexes.md](../user/indexes.md) |
| Branch and commit internals | [branches-commits.md](../user/branches-commits.md) |
| Three-way merge implementation and conflicts | [merge.md](merge.md) |
| Diff/change-feed implementation | [changes.md](../user/changes.md) |
| Branch protection policy | [branch-protection.md](branch-protection.md) |
| CODEOWNERS source of truth | [codeowners.md](codeowners.md) |

## Language, Runtime, And Boundaries

| Area | Read |
|---|---|
| Schema grammar, catalog, migration planner | [schema-language.md](../user/schema-language.md) |
| Query grammar, IR, lints, mutation restrictions | [query-language.md](../user/query-language.md) |
| Embedding client and `@embed` integration | [embeddings.md](../user/embeddings.md) |
| Cedar policy surface and server gating | [policy.md](../user/policy.md) |
| Server auth, OpenAPI, endpoint handlers | [server.md](../user/server.md) |
| Error taxonomy and serialization | [errors.md](../user/errors.md) |
| Constants and tunables | [constants.md](../user/constants.md) |
| Transaction model public contract | [transactions.md](../user/transactions.md) |

## Project Operations

| Area | Read |
|---|---|
| CI and release workflows | [ci.md](ci.md) |
| Install and deployment packaging | [install.md](../user/install.md), [deployment.md](../user/deployment.md) |
| Release history | [releases/](../releases/) |

## Active Implementation Plans

Working documents for in-flight feature work. Removed when the work lands.

| Area | Read |
|---|---|
| Schema-lint chassis v1 (MR-694) — `--allow-data-loss`, soft/hard drops | [schema-lint-v1-plan.md](schema-lint-v1-plan.md) |

## Boundary

Developer docs may mention implementation details, stale gaps, upstream Lance
blockers, and review rules. User docs should not require that context unless
the detail changes the public contract.
