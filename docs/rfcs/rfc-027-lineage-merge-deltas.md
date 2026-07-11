---
type: spec
title: "RFC-027 — Lineage-based merge deltas"
description: Research specification for replacing full-width branch-merge classification with Lance row-version lineage, explicitly blocked on a sublinear deletion-delta source and enforceable I/O cost gates.
status: research-blocked
tags: [eng, rfc, merge, lineage, change-feed, performance, lance, omnigraph]
timestamp: 2026-07-10
owner:
---

# RFC-027 — Lineage-based merge deltas

**Status:** Research / blocked on deletion-delta discovery
**Date:** 2026-07-10
**Depends on:** [RFC-022](rfc-022-unified-write-path.md)'s unified branch-merge
pipeline and capture-once write view
**Surveyed:** omnigraph 0.8.1; Lance 9.0.0-beta.15 (`f24e42c1`)
**Audience:** merge, storage, and performance maintainers
**Open architecture review:** [RFC-022–027 review ledger](../dev/rfc-022-027-architecture-review.md).
Findings marked **BLOCKER** must be dispositioned before acceptance.

---

## 0. Status and decision boundary

The direction is recommended: branch merge should discover changed row IDs from
storage lineage, then read wide values only for those candidates. The proposed
replacement is **not implementation-ready** and this RFC does not authorize
removing `OrderedTableCursor`.

Two facts block the O(delta) claim:

1. filtering `_row_last_updated_at_version` is still a physical O(rows) column
   scan unless a substrate index or change-log makes it selective;
2. a deleted row is absent from the target snapshot, so its version columns
   cannot identify it. The current implementation finds deletions by scanning
   and differencing both complete ID sets.

The RFC advances only when both candidate discovery and deletion discovery have
measured costs bounded by the changed working set. Until then the existing merge
classifier remains the correctness fallback.

## 1. Problem

Current three-way merge classification streams base, source, and target tables
and compares row signatures. A one-row change can therefore read every row and
every wide property, including embeddings and blobs, while holding merge-wide
coordination longer than necessary.

`changes/mod.rs` is narrower but not yet asymptotically different:

- changed live rows are filtered by `_row_last_updated_at_version`;
- insert/update classification builds the full base ID set;
- deletion classification scans both ID sets;
- cross-branch fallback scans and compares complete ordered rows.

Reusing that code unchanged is O(rows), not O(delta). This RFC replaces those
specific scans rather than relabeling them.

## 2. Target contract

For each table touched since the merge base:

1. discover the source and target candidate row IDs from Lance metadata;
2. classify insert, update, and delete without loading unrelated user columns;
3. join the two candidate sets into the existing merge truth table;
4. fetch complete rows only for candidates whose disposition needs values;
5. stage the resulting delta through RFC-022 and publish once.

The semantic oracle remains `merge_truth_table`. This RFC changes candidate
discovery and I/O, not conflict kinds, delete/update precedence, constraint
validation, or manifest atomicity.

## 3. Live-row candidates

On a stable-row-ID table in one physical lineage, Lance exposes:

- `_row_created_at_version` — first creation of the logical row;
- `_row_last_updated_at_version` — latest modification of the logical row.

For merge-base table version `Vb` and side version `Vs`, a live row is:

- an insert when `Vb < created_at <= Vs`;
- an update when `created_at <= Vb` and
  `Vb < last_updated_at <= Vs`.

This classification removes the current full base-ID membership set. Candidate
scans project only `id`, edge endpoints when applicable, stable row ID, and the
two version columns.

It does **not** by itself make the scan O(delta). Phase R1 must prove one of:

1. Lance can build and maintain a scalar index over the version columns with
   correct partial-coverage fallback;
2. transaction/fragment metadata exposes an equivalent bounded changed-row
   iterator;
3. an upstream Lance change-feed primitive supplies these IDs directly.

An index is derived state. Missing or partial coverage falls back to a correct
narrow scan and is reported; it never makes merge fail or omit candidates.

## 4. Deletion delta is the blocker

Deleted logical rows have no live record whose version columns can be filtered.
The current `deleted_ids_by_set_diff` scans all IDs at base and side. That term
alone prevents an O(delta) merge, even if inserts and updates become selective.

Research must disposition these substrate-shaped options:

### 4.1 Deletion-vector and stable-row-ID lineage

Walk only transactions/fragments changed between `Vb` and `Vs`, compare their
deletion vectors, and translate newly deleted offsets to stable row IDs. This is
acceptable only if it handles merge-insert rewrites, updates that move rows,
compaction, fragment reuse, and deletion-vector materialization without scanning
unaffected fragments.

### 4.2 Upstream Lance change log

Consume a public Lance API that yields durable inserted/updated/deleted stable
row IDs by version range. A source-level prototype or private API is evidence,
not a dependency; the production surface must be public and pinned by
`lance_surface_guards.rs`.

### 4.3 Atomic OmniGraph deletion deltas

Write immutable per-commit deleted-ID rows in the same manifest CAS as the graph
commit. This is first-class commit metadata, not a side channel. It adds storage
and format liability and therefore needs a separate format decision before use;
it is not silently folded into v5.

Until one option passes the cost and correctness gates, delete-bearing histories
use the existing classifier. There is no "usually O(delta)" claim that excludes
deletes without saying so in plan and metrics.

## 5. Branch lineage and unsupported operations

Lance version numbers are branch-local and can overlap. Candidate discovery must
use each manifest entry's physical `(table_path, table_branch, table_version)`;
it must not subtract graph commit numbers or compare equal numeric versions from
different branch lineages.

Research must specify behavior for:

- a lazy fork that still physically reads its parent table branch;
- the first branch-owned write after a fork;
- compaction and index-only versions between base and side;
- overwrite, restore, schema rewrite, and hard-drop operations;
- tables without stable row IDs or version metadata;
- a table dropped or introduced on only one side.

Any shape not proven lineage-compatible takes the correctness fallback and
records a typed reason. Physical metadata gaps never weaken the logical merge.

## 6. Proposed planner shape

```text
ResolveMergeBase
  -> DiscoverSideDelta(source)
  -> DiscoverSideDelta(target)
  -> JoinCandidateIds
  -> FetchCandidateRows
  -> ExistingTruthTableAndValidation
  -> RFC022StageAndPublish
```

`DiscoverSideDelta` returns ordered, typed operations keyed by table and logical
row ID. Candidate order is deterministic. Payload fetches use structured key
lookups or SIP; they do not synthesize string `IN` filters or read embedding/blob
columns for candidates whose disposition needs only identity.

The merge base, physical entries, and candidate cuts are captured once before
heavy work. After acquiring publish queues, RFC-022's OCC/read-set rule either
confirms the cut or restarts discovery; it never publishes a delta classified
against a moved side.

## 7. Fallback and rollout safety

`OrderedTableCursor` remains the universal fallback through the research and
shadow phases. Fallback reasons are closed enum values, including:

- `DeletionDeltaUnavailable`;
- `VersionIndexUncovered`;
- `CrossLineageUnsupported`;
- `StableRowIdsUnavailable`;
- `OverwriteOrRestoreInRange`;
- `LineageMetadataInconsistent`.

Before enabling the new path, shadow mode runs both classifiers on the same
captured snapshots and compares ordered operation sets and merge outcomes. A
mismatch fails the test or records a production diagnostic; it never silently
chooses the new answer.

## 8. Cost contract

Use `helpers::cost` with fixed `delta = 1` and table sizes of at least 10k,
100k, and 1M rows, both scalar-only and embedding-bearing. Measure candidate
discovery separately from candidate payload fetch.

The acceptance target for a true lineage path is:

- insert and update discovery I/O is flat within named slack as rows grow;
- delete discovery I/O is flat within the same discipline;
- bytes read scale with candidate identity/version columns plus fetched delta
  payload, not total row width;
- peak RSS is bounded by candidate batch size, not table size;
- graph-history depth and unrelated catalog width do not change the curve.

Local tests gate scan/fragment terms. S3 tests gate object-store RPC and opener
terms that local FS cannot expose. A benchmark without an asserted I/O slope is
supporting evidence, not the acceptance gate.

If only live-row indexing passes, this RFC may prototype or shadow an
insert/update-only path, but it does not authorize production shipping. A
partial fast path requires a separately accepted RFC that names its O(rows)
delete fallback and operational value. This RFC and the default-classifier
O(delta) claim remain blocked until R2 passes.

## 9. Correctness gates

- `merge_truth_table` remains byte-for-byte the semantic disposition oracle.
- Property tests compare lineage and cursor classifiers over inserts, updates,
  deletes, endpoint moves, cycles, and conflicting constraints.
- Dedicated cases cover compaction, index maintenance, restore, overwrite,
  lazy forks, and same-number/different-branch versions.
- A one-row delete in a 1M-row table is the blocker test: the RFC cannot leave
  research status until it is both correct and flat in table size.
- Surface guards pin every Lance version-column, stable-row-ID, deletion-vector,
  transaction, and index behavior used by the chosen implementation.
- Fault tests move a side after discovery and prove OCC restarts instead of
  publishing stale classification.

## 10. Observability

For every merge table report classifier path, fallback reason, source/target
candidate counts, deleted-ID discovery mechanism, fragments and bytes read,
payload columns fetched, discovery latency, and whether shadow results matched.

Metrics distinguish logical delta size from physical discovery work. This is
required to catch an apparently correct path silently regressing to O(rows).

## 11. Research plan

| Phase | Content | Exit criterion |
|---|---|---|
| R0 | Instrument current cursor and `changes` paths; build shadow comparison harness | semantic and I/O baselines |
| R1 | Prove selective live-row discovery and branch-version mapping | insert/update flat-cost gate |
| R2 | Prototype all deletion-delta options against pinned Lance | one option passes delete correctness + flat-cost gate |
| R3 | Shadow new classifier across the full merge truth table and histories | zero mismatches; explicit fallback ledger |
| R4 | Enable lineage path behind a scoped feature/config gate | production diagnostics within budgets |
| R5 | Make lineage default and consider cursor retirement | all fallbacks dispositioned; no physical gap can break merge |

The RFC remains **research / blocked** through R2. Choosing an OmniGraph commit
delta format in §4.3 requires its own format amendment before R3.
