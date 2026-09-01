# Branch merge

**Audience:** engine contributors
**Authority:** current merge routes and invariants; public conflict behavior is
in [the user merge guide](../user/branching/merge.md)

`Omnigraph::branch_merge` performs a graph-level three-way merge from the
source branch into a target branch. The merge base, source, and target are
resolved once; final publication revalidates their exact graph and native-ref
identities.

## Per-table decision

For every table lifetime present in the accepted catalogs, merge compares:

- the table visible at the merge base;
- the source result;
- the current target result.

Stable table/incarnation identity decides whether entries belong to the same
lifetime. Aliases and paths are not merge identity. A rename preserves a
lifetime; drop/re-add is a different declaration even if the name is reused.

The table classifier chooses one of four routes:

1. **No change:** source contributes nothing.
2. **Pointer adoption:** target still equals the base and the exact source table
   state can become the target's visible pointer without copying rows. A
   first-touch lazy target stays on this ref-only route.
3. **Proven insertion replay:** target still permits data replay and the
   complete retained source interval proves a contiguous sequence of exact-ID,
   insertion-only transactions.
4. **General ordered merge:** stream base/source/target in logical `id` order,
   classify each row, and stage the selected delta.

An optimization miss is not a merge failure. Missing transaction history,
unknown certificate fields, incomplete ancestry, or an unfamiliar Lance shape
falls back to the general route.

## Proven insertion route

The internal `omnigraph.insert_absence = "v1"` transaction property says that
the filtered physical IDs were proven absent from that transaction's effective
parent. Merge accepts it only when every version in the complete source
interval is present and structurally proves:

- one exact previous-version link and transaction UUID;
- an insertion-only filtered Update over exactly physical `id`;
- no removed/updated fragments or unrelated maintenance effect;
- complete nested schema/index-coverage metadata;
- physical-row totals matching the manifest delta;
- exact source and target native branch incarnations under the final gates.

The route stages bounded immutable fragments, then commits the same certified
filter-bearing Update shape. It performs no target MergeInsert join or target
ID preflight. The marker is not a signature and raw Lance writers remain
unsupported. RFC 0023 owns the detailed proof and performance evidence.

## General route

The fallback is an ordered three-way cursor merge:

- each production cursor requests 8,192 rows and 32 MiB per decoded batch;
- Blob-bearing rows are materialized under the same operation budget;
- all selected constructive rows stage as upserts and removals as deletes;
- the transaction plan is pre-minted and bounded before recovery arm;
- selected validation deltas share one operation-wide memory budget.

Logical conflicts are computed before effects. Value constraints, uniqueness,
referential integrity, and cardinality run through the shared validator against
the target plus the complete selected delta.

## Cost model

Route selection determines the cost class. For one table, let `N` be the live
rows in an input image, `delta` the selected rows, `K` the retained source
versions, and `C` the bounded publish chunks.

| Route | Classification | Publication |
|---|---|---|
| Pointer adoption | Metadata-only when no validation delta is needed; otherwise the delta may require base/source ordered scans | Native-ref or manifest-pointer change; no row copy |
| Proven insertion replay | Walk `K <= 1,024` transaction records and scan only the certified source interval | `C <= 1,024` join-free fenced inserts; no target ID preflight or MergeInsert join |
| Adopt with delta | At least two full ordered scans, base and source | New rows use preflighted fenced inserts; changed rows use update-only `KnownPresentUpdate`; deletes are chunked |
| General three-way | At least three full ordered scans, base, source, and target | Constructive rows use insertion-capable upsert; deletes are chunked |

Insertion-capable upsert forces Lance's v2 path (`use_index(false)`) so the
transaction carries the exact-`id` conflict filter. Each constructive chunk
may therefore join the full target. `KnownPresentUpdate` can use an `id` index
when its coverage is safe, falls back to the full join otherwise, and never
inserts.

Every ordered cursor asks Lance to sort the full table by logical `id`. The
sort is `O(N log N)` and consumes all input before producing its first row. An
`id` BTREE accelerates filters but does not provide ordered enumeration, so it
does not remove this sort. The cursor projects full logical rows, including
vectors and Blob descriptors, so wide rows increase read and spill cost.

Ordered scans run in a bounded spill context: each execution has a 150 MiB
memory pool, a 100 GiB scratch quota, and a 37.5 MiB cap on the batches fed
into a sort. If spilling is disabled, the scratch quota is exhausted, or an
indivisible row exceeds the hard cap, merge fails loudly; it never returns a
partial result. The 8,192-row and 32 MiB scanner settings are batch targets,
not hard decoded-batch limits.

Validation is delta-scoped and retains at most 32 MiB of projected scalar
state. Usable physical indexes reduce uniqueness and relationship probe cost;
missing coverage falls back to scans without changing correctness. Tables and
chunks publish sequentially inside the one recovery envelope, and all routes
defer index construction to reconciliation.

Cost tests cap common fast-forward manifest opens/scans at three and diverged
merges at four. Each scan still folds the surviving append-only `__manifest`
history, so tiny merges can slow down on an uncompacted graph; `optimize` is
the operational remedy.

## Diagnosing a slow merge

`MergeWriteProbes` is a task-local test and benchmark seam; production leaves
it unset, so timing does not read the clock. Its top-level timing flow is:

`OuterPrepare` -> ((`ProvenInsertHistory` -> `ProvenInsertPlanScan`) | `TableWalk`)
-> `CandidateValidation` -> `FinalRevalidation` -> `RecoveryArm` ->
`PhysicalPublish` -> `RecoveryConfirm` -> `ManifestPublish` -> `RecoveryCleanup`
-> `OuterRestoreRefresh`.

The parenthesized classification routes are chosen per table, so a mixed-table
operation can record both route families. `TableWalk` covers one general
three-way ordered walk and merged-row staging; for Blob tables it begins after
the operation-wide descriptor preflight. `KeyedStage` and `KeyedCommit` are
sub-buckets of `PhysicalPublish`.
`merge_timing_snapshot` reports total, maximum, and exact interval count for
every phase; the count remains meaningful when a short duration rounds down to
zero microseconds. Structural probes identify the chosen data path. The columns
below correspond to
`ordered_cursor_scan_calls`, `stage_fenced_insert_calls`,
`stage_known_present_update_calls`, `stage_merge_insert_calls`, and
`strict_insert_preflight_calls`:

| Route | Ordered cursors | Fenced inserts | Known-present updates | MergeInsert upserts | Strict-insert preflights |
|---|---:|---:|---:|---:|---:|
| Proven insertion replay | `0` | `C` | `0` | `0` | `0` |
| Adopt with delta | at least `2` | insert chunks | changed-row chunks | `0` | insert chunks |
| General three-way | at least `3` | `0` | `0` | constructive-row chunks | `0` |

Blob descriptor selection can add cursor passes, so the fallback counts are
lower bounds. Other useful signals are:

- high `ProvenInsertHistory` means the retained transaction walk dominates;
- high `ProvenInsertPlanScan` means scanning or materializing the certified
  source interval dominates;
- any ordered cursor on an insert-only merge means the provenance proof missed
  and classification fell back;
- high `KeyedStage` means target lookup/join or Blob materialization dominates;
- high `ManifestPublish` with a tiny delta points to manifest history or CAS
  retries;
- validation projected-byte counters expose pressure on the 32 MiB delta
  budget, while Blob payload and external-probe counters isolate object cost.

The probe set also records requested cursor batch bounds, raw proven-insert
batch sizes, and legacy whole-delta scans. `stage_vector_index_calls` and
`scan_staged_combined_calls` should remain zero on current merge routes.

## Conflicts

The engine reports structured conflict kinds:

- `DivergentInsert`
- `DivergentUpdate`
- `DeleteVsUpdate`
- `OrphanEdge`
- `UniqueViolation`
- `CardinalityViolation`
- `ValueConstraintViolation`

Conflict detection never silently picks a winner. The HTTP layer maps the
structured result to its public 409 representation.

## Publication and recovery

All productive table routes feed one BranchMerge recovery sidecar. Pointer
changes, table effects, target authority, and pre-minted graph lineage are fixed
before the first effect. The target becomes visible through one manifest CAS.

After recovery arm, a failed table link or publish retains recovery ownership
and returns `RecoveryRequired`. Merge does not re-run semantic classification
around a committed prefix. Full recovery either publishes the complete
confirmed result or compensates the owned partial set before visibility.

## Outcomes

`MergeOutcome` is one of:

- `AlreadyUpToDate` — source adds no target-visible change;
- `FastForward` — the target adopts source state without a divergent
  three-way result;
- `Merged` — a productive three-way merge publishes a new graph commit.

## Owners

- `crates/omnigraph/src/exec/merge.rs` — classification and execution.
- `crates/omnigraph/tests/merge_truth_table.rs` — operation-pair semantics.
- `crates/omnigraph/tests/merge_fast_forward.rs` — pointer/proven-insert routes
  and bounded fallback.
- `crates/omnigraph/tests/branching.rs` — branch identity and Blob behavior.
- `crates/omnigraph/tests/merge_cost.rs` — delta scope and manifest-history
  cost contracts, not semantics.
- `crates/omnigraph/src/instrumentation.rs` — route and timing probes.

See [writes.md](writes.md), [recovery.md](recovery.md), and
[RFC 0023](../rfcs/0023-key-conflict-fencing.md).
