# OmniGraph RFCs

RFCs are durable decision records for changes that are costly to reverse or
wide enough that implementation review alone cannot establish the right shape.
All formal RFCs live in this directory and use one lifecycle, one metadata
schema, and one number namespace. `track` records where a proposal came from;
it does not change the review or acceptance rules.

The architectural invariants remain the hard boundary for every change. An RFC
explains why a particular design is worth its long-term liability and records
the evidence behind the decision; it cannot waive
[the invariants or deny-list](../dev/invariants.md).

## When an RFC is required

Write an RFC before implementing:

- a new query, schema, CLI, HTTP, or SDK contract;
- an on-disk format, wire format, recovery protocol, or compatibility change;
- a new storage substrate or dependency that constrains future architecture;
- a cross-cutting correctness, authorization, or operational boundary; or
- an exception to a deny-list item or another hard-to-reverse choice.

Routine bug fixes, narrow refactors, dependency maintenance, and documentation
corrections normally do not need an RFC. If the code can land safely and be
reverted cheaply without committing the project to a lasting contract, an
issue and implementation PR are usually enough.

## File and heading format

- Filename: `YYYY-MM-DD-kebab-title.md`. The date is the day drafting started
  and equals the `created` frontmatter field. The author chooses it alone; no
  registry, issue, or PR has to allocate anything first.
- Heading: `# RFC: Title`.
- The RFC id is the filename without `.md`. A reference is a link whose text
  is the RFC title and whose target is the canonical RFC filename.
- `0000-template.md` is reserved and is not an RFC.
- Two RFCs may share a date; their slugs differ. Two RFCs never share a slug
  on the same day. A retitled RFC keeps its filename; the decision log records
  the new title.
- Do not create `pre-merge`, `final`, `v2`, `internal`, or review-ledger copies.
  Revise the canonical file; preserve meaningful changes in its decision log.

### Numbered RFCs 0001 to 0068

RFCs 0001 to 0068 use `NNNN-kebab-title.md`, the heading
`# RFC NNNN: Title`, and the reference label `RFC NNNN`. That namespace is
closed at 0068: no new number is allocated, and `scripts/check-docs.py`
rejects any numbered filename outside the allocated and reserved numbers it
lists. Numbers reserved by PRs that were open
when the namespace closed (0047 and 0048 by PR #606; 0050 by the
`rfc/0050-engine-crate-topology` branch; 0056 by PR #670; 0059 by PR #675;
0060 by PR #677; 0067 and 0068 by PR #725) may still land under their
reserved numbers. Every other gap
is historical and is never reused or backfilled.

The numbered scheme allocated an identifier only at merge, so every draft
carried a working number that went stale whenever another RFC merged first,
and the rename touched the filename, frontmatter, heading, registry row, and
every reference. A date the author owns removes the allocation step.

## Required frontmatter

Every RFC uses exactly this schema:

```yaml
---
rfc: "2026-09-15-short-descriptive-title"
title: "Short descriptive title"
track: maintainer
status: draft
implementation: not-started
authors:
  - Name or handle
created: 2026-09-15
updated: 2026-09-15
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---
```

Field rules:

- `rfc` is the RFC id: the filename without `.md`. Numbered RFCs keep their
  four-digit string.
- `title` matches the heading text.
- `track` is `public` or `maintainer`; both follow the same lifecycle.
- `status` is one of `draft`, `accepted`, `rejected`, or `superseded`.
- `implementation` is one of `not-started`, `in-progress`, `partial`,
  `complete`, `removed`, or `n/a`.
- `authors` is a non-empty list.
- `created` and `updated` use `YYYY-MM-DD`; `created` equals the filename's
  date prefix.
- `discussion` is a durable issue/PR URL or `null`.
- `supersedes` and `superseded_by` contain RFC ids as quoted strings. Update
  both sides when the relationship applies to the whole decision.
- `blocked_on` contains concrete evidence or dependency gates. Research being
  blocked is not a lifecycle status; it is a draft with a non-empty list.

Frontmatter is the only status and date authority. Do not repeat it in a table
or a hand-written `Status`, `Date`, `Author track`, or `Implementation` field.
The body may contain a dated disposition or implementation note when it adds
context rather than restating metadata.

## Lifecycle

```text
draft ──maintainer decision──▶ accepted ──later replacement──▶ superseded
  │                                  │
  └──maintainer decision──▶ rejected └──implementation progresses separately
```

- **Draft**: under design and review. A draft may live on the main branch so
  review and evidence have one durable home, but merge alone does not accept
  it. Product behavior must not claim authority from a draft.
- **Accepted**: maintainers approved the decision and its stated boundaries.
  Implementation status moves independently as work lands.
- **Rejected**: the decision was not adopted. Keep the record and concise
  rationale; use `removed` if an experiment shipped and was later deleted,
  otherwise `n/a` or `not-started` as appropriate.
- **Superseded**: another RFC owns the current decision. Link both directions
  and keep the old rationale intact.

Acceptance requires all invariants, compatibility consequences, operational
boundaries, and owned evidence gates to be explicit. A blocker owned by another
RFC does not block an independently reviewable decision, but hidden shared
dependencies do.

## Process

1. Copy [the template](0000-template.md) to `YYYY-MM-DD-kebab-title.md`, dated
   the day drafting starts.
2. Set every frontmatter field and open a PR in `draft` status.
3. Review the problem, user/operational behavior, invariants, substrate
   alignment, compatibility, evidence, alternatives, and rollout.
4. Record material review outcomes in the RFC's decision log. Do not maintain a
   separate review ledger. A post-merge amendment rewrites the body sentences
   it changes and its Decision-log entry names each sentence it supersedes, so
   the body alone stays current.
5. A maintainer decision changes the lifecycle to `accepted` or `rejected`.
6. Implementation PRs link the accepted RFC and update `implementation` plus
   any durable evidence or support boundary in the canonical file.
7. A later incompatible decision gets a new RFC and supersedes the old one.

For Lance-dependent work, follow [the Lance reading protocol](../dev/lance.md)
and record the exact upstream version and surfaces reviewed. For test planning,
extend existing owners according to [the test map](../dev/testing.md).

## Registry

This table is the human index for the canonical RFC corpus. The first column
links the canonical file; its text is the number for numbered RFCs and the
`created` date for dated ones. Rows are in creation order: numbered RFCs first,
then dated RFCs by date.

| RFC | Decision | Track | Status | Implementation |
|---|---|---|---|---|
| [0001](0001-fragment-adopt-branch-merge.md) | Branch merge by fragment adoption | maintainer | draft | not-started |
| [0002](0002-config-cli-architecture.md) | Config and CLI architecture | maintainer | superseded | partial |
| [0003](0003-mcp-server-surface.md) | MCP server surface | maintainer | draft | not-started |
| [0004](0004-cluster-graph-schema-apply.md) | Cluster graph and schema apply | maintainer | accepted | complete |
| [0005](0005-server-cluster-boot.md) | Server boot from cluster state | maintainer | accepted | partial |
| [0006](0006-object-storage-cluster-roots.md) | Object-storage cluster roots | maintainer | accepted | complete |
| [0007](0007-operator-config.md) | Per-operator configuration | maintainer | accepted | complete |
| [0008](0008-retire-omnigraph-yaml.md) | Retire `omnigraph.yaml` | maintainer | accepted | complete |
| [0009](0009-unified-access-paths.md) | Unified embedded and remote access paths | maintainer | accepted | complete |
| [0010](0010-cli-planes.md) | Explicit CLI planes | maintainer | superseded | partial |
| [0011](0011-cli-addressing-and-config.md) | CLI addressing and configuration | maintainer | accepted | complete |
| [0012](0012-embedding-provider-config.md) | Provider-independent embedding configuration | maintainer | accepted | complete |
| [0013](0013-write-path-latency.md) | Write-path latency and bounded history cost | maintainer | superseded | partial |
| [0015](0015-ingest-embeddings.md) | Ingest-time `@embed` reconciliation | maintainer | draft | not-started |
| [0018](0018-ingest-wal.md) | Streaming-ingest WAL on Lance MemWAL | maintainer | rejected | removed |
| [0019](0019-heads-and-fences.md) | Heads and fences | maintainer | superseded | partial |
| [0022](0022-unified-write-path.md) | Unified graph-write protocol | maintainer | accepted | complete |
| [0023](0023-key-conflict-fencing.md) | Substrate-native key-conflict fencing | maintainer | accepted | complete |
| [0024](0024-durable-table-heads.md) | Durable table heads | maintainer | draft | not-started |
| [0025](0025-checkpoint-retention.md) | Checkpoint-pinned retention | maintainer | draft | not-started |
| [0026](0026-memwal-streaming-ingest.md) | MemWAL streaming ingest | maintainer | rejected | removed |
| [0027](0027-lineage-merge-deltas.md) | Lineage-based merge deltas | maintainer | draft | not-started |
| [0028](0028-stable-schema-identity.md) | Stable schema identity and table incarnation | maintainer | accepted | complete |
| [0029](0029-azure-blob-storage.md) | Native Azure Blob storage | public | accepted | in-progress |
| [0030](0030-cdc-time-travel.md) | Graph change feed and retained-history contract | maintainer | accepted | partial |
| [0031](0031-comparative-cost-harness.md) | Comparative cost harness | maintainer | draft | not-started |
| [0032](0032-adversarial-correctness-harness.md) | Adversarial correctness harness | maintainer | draft | not-started |
| [0033](0033-blob-management.md) | Blob management | maintainer | accepted | partial |
| [0034](0034-durable-recovery-authority.md) | Durable recovery authority and outcomes | maintainer | draft | not-started |
| [0035](0035-served-operation-ownership.md) | Served operation ownership | maintainer | draft | not-started |
| [0036](0036-atomic-runtime-activation.md) | Atomic runtime activation and graph availability supervision | maintainer | draft | not-started |
| [0037](0037-deterministic-simulation-harness.md) | Deterministic simulation harness | public | accepted | in-progress |
| [0038](0038-typed-storage-failures.md) | Typed storage failures | public | accepted | complete |
| [0039](0039-end-to-end-benchmark.md) | The end-to-end benchmark | public | accepted | in-progress |
| [0040](0040-system-column-namespace.md) | System column namespace | public | accepted | in-progress |
| [0041](0041-inline-stored-queries.md) | Inline and stored queries | maintainer | accepted | partial |
| [0042](0042-incarnation-suffixed-branch-refs.md) | Incarnation-suffixed native branch refs | maintainer | accepted | complete |
| [0043](0043-full-text-index-compatibility.md) | Full-text index compatibility and explicit rebuild | maintainer | accepted | complete |
| [0044](0044-edge-keys.md) | Edge keys: derived edge identity | maintainer | draft | in-progress |
| [0045](0045-gq-logic-tests.md) | GQ logic tests | maintainer | draft | partial |
| [0046](0046-index-status.md) | Read-only index status | maintainer | draft | not-started |
| [0047](0047-search-plan-truth.md) | Search plan truth: projectable ranking, deterministic order, and loud search failures | public | draft | not-started |
| [0048](0048-search-contracts.md) | Search contracts and retrieval algebra | public | draft | not-started |
| [0049](0049-control-plane-seams.md) | Control-plane seams: observe, readiness witness, bounded shutdown | maintainer | accepted | partial |
| [0051](0051-json-output-via-arrow.md) | JSON output via Arrow | maintainer | draft | partial |
| [0052](0052-managed-control-plane-cli.md) | Managed control-plane CLI | maintainer | accepted | complete |
| [0053](0053-offline-data-token-verification.md) | Offline data-token verification | maintainer | accepted | complete |
| [0054](0054-default-actor-provenance.md) | Default graph actor provenance | maintainer | rejected | removed |
| [0055](0055-gq-branch-statements.md) | Branch statements in GQ | maintainer | draft | in-progress |
| [0057](0057-bounded-merge-preparation.md) | Accepted-context reuse and bounded merge preparation | maintainer | accepted | in-progress |
| [0058](0058-retained-merged-ancestry.md) | Retained merged ancestry | maintainer | draft | not-started |
| [0061](0061-managed-cluster-lifecycle.md) | Managed cluster lifecycle and config preparation | maintainer | accepted | complete |
| [0062](0062-manifest-version-clock.md) | Manifest version as the table registration clock | maintainer | draft | in-progress |
| [0063](0063-self-contained-branch-lineage.md) | Self-contained branch lineage | maintainer | draft | in-progress |
| [0064](0064-explicit-storage-upgrades.md) | Explicit storage upgrades | maintainer | accepted | in-progress |
| [0065](0065-isolated-branch-merge-publication.md) | Isolated branch merge publication | maintainer | draft | not-started |
| [0066](0066-one-seam-type.md) | One seam type for test-time behavior substitution | maintainer | draft | in-progress |
| [0067](0067-detached-table-commits.md) | Detached table commits | maintainer | accepted | complete |
| [0068](0068-graph-commit-record.md) | Graph commit record | maintainer | draft | not-started |
| [2026-09-09](2026-09-09-identity-credentials-and-applied-policy.md) | Identity credentials and applied policy authorization | maintainer | accepted | complete |
| [2026-09-14](2026-09-14-compatibility-surfaces.md) | Compatibility surfaces | maintainer | draft | not-started |
| [2026-09-16](2026-09-16-session-settings.md) | Session settings | maintainer | draft | in-progress |
| [2026-09-24](2026-09-24-shared-expression-model.md) | Shared expression model | maintainer | draft | in-progress |
