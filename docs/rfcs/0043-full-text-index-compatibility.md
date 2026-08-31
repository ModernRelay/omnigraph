---
rfc: "0043"
title: "Full-text index compatibility and explicit rebuild"
track: maintainer
status: accepted
implementation: complete
authors:
  - Andrew
created: 2026-08-31
updated: 2026-08-31
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0043: Full-text index compatibility and explicit rebuild

## Summary

Upgrade to Lance 11.0.0 with explicit, branch-scoped full-text index rebuilding
and fail-closed search compatibility checks. An index is usable for full-text
search only when its immutable artifact carries proof of the supported analyzer
generation. A newer table version or a graph-wide upgrade flag is not proof.

This does not change graph format v6, recovery schema v9, row data, or retained
history. It deliberately does not promise full-text search against old indexes,
mixed-version serving, or automatic rebuilding during writes.

## Motivation

Lance 10 used rust-stemmers 1.2.0; Lance 11 uses frostem 1.20260821.3. Default
English tokenization changes: `organism` and `university` acquire different
stems. A three-row Lance 10 fixture returned one match each for `organism`,
`university`, and `running`. Reading that exact index with Lance 11 returned
zero, zero, and one without error; fully rebuilding it restored all three.
Lance's persisted tokenizer parameters do not identify the stemming algorithm
generation. Dataset writer metadata and posting-file encoding versions cannot
establish index provenance.

The surveyed upstream change is [Lance PR 8183](https://github.com/lance-format/lance/pull/8183),
including the replacement of a stemmer with a known UTF-8 panic. Reverting the
dependency is not a safe compatibility strategy.

## User and operational behavior

`omnigraph rebuild-full-text-indexes <URI> --branch main [--json]` rebuilds full-text
indexes from the selected branch's current rows. The engine exposes the same
operation, including an actor-aware entry point. The command uses direct graph
storage, not a new HTTP maintenance endpoint. It reports the exact published
graph commit and rebuilt type/property pairs. No work is reported as rebuilt
without successful publication.

Operators stop the old serving/writing fleet, preserve a recoverable backup,
upgrade all readers and writers, rebuild each branch that needs full-text search,
and verify representative searches before resuming service. Ordinary row,
traversal, scalar, and vector reads do not require this rebuild. Full-text
queries against an uncertified index return a rebuild-required diagnostic;
they never fall through to a silently incomplete indexed result.

Historical snapshots retain their original indexes. Branching or restoring an
old snapshot may therefore require another rebuild on the resulting live branch.
There is no rewrite of historical commits. A rebuild of one branch does not
claim to migrate another branch.

## Design

### Artifact-scoped proof

A completed full build writes one small compatibility certificate inside its
new index UUID directory before the existing staged CreateIndex transaction is
published. The certificate is bounded JSON, using Lance's public object-store
and base-path resolution APIs and native index file inventory. Shallow clones
and garbage collection keep their existing ownership rules. The reader checks
the actual object size before a bounded read, without parsing a Lance footer.
It contains a versioned analyzer generation, the
exact index UUID, and a digest of the immutable index details and file inventory.
Mutable table version, coverage, name, and branch location are not provenance.
The certificate is not an authenticity mechanism against a malicious writer.

The supported Lance/analyzer dependencies are pinned explicitly. Changing them
requires a compatibility audit and either evidence that the generation remains
valid or a new generation. Missing, malformed, mismatched, or unknown proof
refuses full-text use. Storage failures remain storage failures.

Search checks the actual snapshot-selected index segments for the requested
columns, including full-text predicates through the read-only SDK. Ordinary
queries do not read certificates. All relevant segments must be compatible;
checking only one segment or the latest table writer is insufficient.

### Rebuild and maintenance

The rebuild reuses the existing EnsureIndices staging, first-touch branch fork,
ordered gates, exact recovery identity, and one graph publication. It replaces
all full-text segments for each rebuilt column, including historical index names,
while preserving unrelated indexes. Rebuilding always uses engine-default
English analysis; external custom tokenizer settings are not retained.
Policy is enforced before effects and actor
attribution travels with the same publication. Unsupported physical full-text
inventory must not be silently reported as migrated.

Append/delete and stable-row-ID compaction preserve the immutable index proof.
Incremental full-text index folding is excluded from ordinary optimize in this
first slice: it can create a new UUID by merging old postings and has no
pre-publication compatibility hook. Other index optimization and compaction
remain available. New unindexed rows remain queryable through Lance's tail
scan; operators use the explicit full rebuild to refresh full-text coverage.
This avoids an inline full-text rebuild or a second provenance/recovery system.
Any other unproven replacement index must pass the same refusal check.

## Invariants

One accepted snapshot drives planning, and one graph publication exposes all
rebuilt tables. First-touch physical effects retain the existing recovery
authority. Indexes remain derived state: no row data or schema identity is
inferred from index names, and a compatibility failure is explicit rather than
a wrong result. Policy, bounded retries, and durable acknowledgements use the
existing writer contract. No custom WAL, migration queue, raw public Lance writer,
or inline full-text rebuild is added.

## Compatibility and reversibility

The new certificate is an additional derived index file, not a graph-format
bump. Uncertified indexes are conservatively refused even if an external builder
happened to use a compatible analyzer. A full rebuild establishes proof from
rows. Old binaries do not enforce this contract and must not read rebuilt
indexes; downgrades require restoring the pre-upgrade backup and old fleet.
Deleting a certificate removes search availability, not rows. The practical
cost is rebuilding each branch that needs search and retaining backups for
rollback.

## Alternatives

- Ship without a gate: reproduces silent missing results.
- Mark the graph or latest dataset version upgraded: unsound for history,
  branch inheritance, and retained old index segments.
- Fork Lance or revert frostem: a much larger maintenance boundary, and the old
  stemmer has an upstream correctness defect.
- Automatic inline rebuilds or a migration service: unnecessary infrastructure
  and unbounded foreground work; the explicit operation is sufficient.
- Certify incremental outputs after publication: creates an additional recovery
  obligation. Leave that optimization for a separately proven upstream hook.

## Evidence and tests

Extend the existing search, maintenance, branching, policy, failpoint, CLI data,
and CLI plane owners. Required cases include incompatible search refusal with
ordinary reads intact; full rebuild restoring changed-stem searches; all-column
replacement; inherited-branch isolation; retained historical refusal; empty data;
policy denial before effects; one publication and recovery after partial effects;
and full-text correctness after ordinary writes and optimize. Certificate tests
cover wrong UUID/generation/artifact inventory and absent or malformed proof.

The audit used final Lance 11.0.0 source and complete upstream full-text format,
tokenizer, index, maintenance, versioning, branch, and migration documentation.
Local qualification exercised the workspace test graph, the saved Lance 10
artifact regression, genuine `tokio_unstable` deterministic simulations, and
the AWS-enabled server tests. Workspace and simulation-specific Clippy checks
passed with warnings denied. Live S3/Azure qualification and production cutover
remain separate operator work; local tests do not establish those outcomes.

## Rollout

Ship the engine guard and rebuild operation together with the thin CLI and
upgrade instructions. Qualify them before production cutover. No production
migration, deployment, or data change is performed by this implementation task.

## Unresolved questions

None for this slice. Incremental full-text proof propagation is out of scope.

## Decision log

- 2026-08-31: Maintainer accepted explicit rebuilding, old-snapshot full-text
  refusal, and a controlled v11-only cutover, and authorized implementation.
  Artifact-scoped proof and reuse of the existing writer keep that boundary
  enforceable without a Lance fork or migration framework.
