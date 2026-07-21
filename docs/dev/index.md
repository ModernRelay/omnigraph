# Developer Docs

**Audience:** contributors, maintainers, and coding agents

This is the contributor-facing entry point. These docs explain architecture,
invariants, implementation contracts, test ownership, and upstream Lance
constraints. User-facing behavior should still be documented through
[docs/user/index.md](../user/index.md) and the relevant public reference docs.

New to the codebase, or re-anchoring after time away? Read
[canon.md](canon.md) — the linear narrative of the whole system (why it
exists, how a read/write/merge/crash unfolds, deliberate exclusions, risks,
roadmap). The per-area docs below remain the mechanical authority.

## Required For Every Non-Trivial Change

| Need | Read |
|---|---|
| Architectural rules, known gaps, deny-list | [invariants.md](invariants.md) |
| Versioning & compatibility policy (release / wire / storage / Lance) | [versioning.md](versioning.md) |
| Upstream Lance source-of-truth index | [lance.md](lance.md) |
| Existing test coverage and test placement | [testing.md](testing.md) |

## Architecture And Storage

| Area | Read |
|---|---|
| System structure, L1/L2 framing, component diagrams | [architecture.md](architecture.md) |
| On-disk layout, manifest schema, URI behavior | [storage.md](../user/concepts/storage.md) |
| Direct-publish writes, D2, staged writes, recovery sidecars | [writes.md](writes.md) |
| Write-path state of affairs — shipped RFCs, current boundaries, blockers, upstream alignment, and next gates | [writing-path-state-of-affairs.md](writing-path-state-of-affairs.md) |
| Query execution, mutation execution, loader flow | [execution.md](execution.md) |
| Index lifecycle and graph topology indexes | [indexes.md](../user/search/indexes.md) |
| Branch and commit internals | [branches-commits.md](../user/branching/index.md) |
| Three-way merge implementation and conflicts | [merge.md](merge.md) |
| Diff/change-feed implementation | [changes.md](../user/branching/changes.md) |
| Branch protection policy | [branch-protection.md](branch-protection.md) |

## Language, Runtime, And Boundaries

| Area | Read |
|---|---|
| Schema grammar, catalog, migration planner | [schema-language.md](../user/schema/index.md) |
| Query grammar, IR, lints, mutation restrictions | [query-language.md](../user/queries/index.md) |
| Embedding client and `@embed` integration | [embeddings.md](../user/search/embeddings.md) |
| Cedar policy surface and server gating | [policy.md](../user/operations/policy.md) |
| Server auth, OpenAPI, endpoint handlers | [server.md](../user/operations/server.md) |
| Error taxonomy and serialization | [errors.md](../user/operations/errors.md) |
| Constants and tunables | [constants.md](../user/reference/constants.md) |
| Transaction model public contract | [transactions.md](../user/branching/transactions.md) |
| User-doc coherence cleanup ledger | [docs-issues.md](docs-issues.md) |

## Project Operations

| Area | Read |
|---|---|
| CI and release workflows | [ci.md](ci.md) |
| Install and deployment packaging | [install.md](../user/install.md), [deployment.md](../user/deployment.md) |
| Release history | [releases/](../releases/) |

## Contribution & Governance

| Area | Read |
|---|---|
| How to contribute (external) | [CONTRIBUTING.md](../../CONTRIBUTING.md) |
| Governance model, roles, decision authority | [GOVERNANCE.md](../../GOVERNANCE.md) |
| RFC process — public contribution and maintainer design-series tracks | [rfcs/](../rfcs/) |

`docs/rfcs/` is the durable home for both formal tracks. Every formal RFC uses
the four-digit `NNNN-*` filename namespace; explicit `Author track` and `Status`
metadata select the public contribution or maintainer design-series lifecycle.
Public RFC merge means acceptance, while maintainer design series may remain
draft across merged revisions. Existing `docs/dev/rfc-00N-*` files are legacy
internal design and implementation records that retain their current lifecycle.

## Case Studies

Worked write-ups of specific bugs — root cause, fix, and the reasoning that
ruled out the tempting-but-wrong alternatives. Read these for the debugging
pattern, not just the outcome.

| Area | Read |
|---|---|
| camelCase property filters lowercased at runtime (#283) — two engine→Lance boundaries, two different fixes | [bug-case-fix.md](bug-case-fix.md) |

## Active Design Reviews

Review ledgers record open findings against proposed architecture. They remain
as durable disposition history after closure, so RFC backlinks stay valid.

| Area | Read |
|---|---|
| RFC-022–028 split architecture review — RFC-022/023/028 implemented; RFC-024/025/027 research-blocked; RFC-026 Phase A/B1 private core implemented with widest-shape closure green, unbounded retain-all selected, and common public/B2b contracts inactive; dependency corrections and acceptance evidence remain tracked | [rfc-022-027-architecture-review.md](rfc-022-027-architecture-review.md) |

## Active Implementation Plans

Working documents for in-flight feature work. Removed when the work lands.

| Area | Read |
|---|---|
| Schema-lint chassis v1 (MR-694) — `--allow-data-loss`, soft/hard drops | [schema-lint-v1-plan.md](schema-lint-v1-plan.md) |
| Inline + stored queries, request/response envelope, MCP (MR-656 / MR-976 / MR-969) | [rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md) |
| Config & CLI architecture — layered config, client targeting, file naming (MR-973 / MR-974 / MR-981) | [rfc-002-config-cli-architecture.md](rfc-002-config-cli-architecture.md) |
| MCP server surface — full tool parity, stored queries, modular auth (MR-969 / MR-956 / MR-974) | [rfc-003-mcp-server-surface.md](rfc-003-mcp-server-surface.md) |
| Future cluster control plane — declarative as-code config, JSON state ledger, reconciler | [cluster-config-specs.md](cluster-config-specs.md), [cluster-axioms.md](cluster-axioms.md), [cluster-config-implementation-spec.md](cluster-config-implementation-spec.md) |
| Cluster graph & schema apply — Phase 4 sidecars, roll-forward recovery, approval artifacts | [rfc-004-cluster-graph-schema-apply.md](rfc-004-cluster-graph-schema-apply.md) |
| Server boots from cluster state — Phase 5 mode switch, applied-revision serving | [rfc-005-server-cluster-boot.md](rfc-005-server-cluster-boot.md) |
| Per-operator config — `~/.omnigraph/` identity, keyed credentials, named servers (the operator slice of RFC-002) | [rfc-007-operator-config.md](rfc-007-operator-config.md) |
| Deprecate `omnigraph.yaml` — one concern per config surface; key-by-key migration map and staged retirement | [rfc-008-deprecate-omnigraph-yaml.md](rfc-008-deprecate-omnigraph-yaml.md) |
| Unify CLI embedded/remote access paths — parity referee, shared wire-DTO crate, `GraphClient` trait, declared plane capabilities | [rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md) |
| Restructure the CLI around explicit planes — one graph-addressing model, declared capability surface, plane-grouped help (expands RFC-009 Phase 4) | [rfc-010-cli-planes-restructure.md](rfc-010-cli-planes-restructure.md) |
| CLI refactoring — one addressing & config model post-`omnigraph.yaml`: scope + `--graph` + derived access path, served-default / privileged-direct, profiles, named queries, capability classifier (completes RFC-008) | [rfc-011-cli-refactoring.md](rfc-011-cli-refactoring.md) |
| Provider-independent embedding configuration — one resolved `EmbeddingConfig` + sealed provider enum (Gemini/OpenAI/Mock), identity recorded in the schema IR, query-time same-space validation, NFR floor | [rfc-012-embedding-provider-config.md](rfc-012-embedding-provider-config.md) |
| Write-path latency — capture-once `WriteTxn`, version-pinned opens, one `GraphPublishAuthority` fed declarative `PublishPlan`s, manifest-authoritative lineage, epoch fence, bounded history (compaction + cleanup), and an IO-counted cost contract (`iss-write-s3-roundtrip-amplification`, `iss-991`) | [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md) |
| RFC-013 handoff — current-state map, latest validation, and concrete next actions for finishing write-path latency and correctness work | [handoff-rfc-013-write-path.md](handoff-rfc-013-write-path.md) |
| Write-latency roadmap — validated cost model (the 6-LIST warm-write trace), the two root causes (un-GC'd `_versions/`; re-resolving latest by listing), and the layered fix (GC + capture-once reuse); how commit-graph-table retirement feeds in | [write-latency-roadmap.md](write-latency-roadmap.md) |

## Boundary

Developer docs may mention implementation details, stale gaps, upstream Lance
blockers, and review rules. User docs should not require that context unless
the detail changes the public contract.
