# Detached-only tables

**Decides:** a pin's detached version is the table's version for its whole life: nothing promotes it onto linear history, and `cleanup` is Omnigraph's own tracing collector over `__manifest` pins.
**Contract:** keeps E7, read:5, reclaim:1, reclaim:4, maintain:3
**Code:** `crates/omnigraph/src/db/omnigraph/collector.rs`, `crates/omnigraph/src/db/omnigraph/optimize.rs`, `crates/omnigraph/src/db/omnigraph/promotion.rs`, `crates/omnigraph/src/changes/candidate_scan.rs`, `crates/omnigraph-core/src/staging.rs`, `crates/omnigraph-core/src/metadata.rs`, `crates/omnigraph/tests/integration/maintenance.rs`

## Context

Replaying a published detached transaction onto linear history (promotion)
costs one more commit per touched table per write, allows only transaction
kinds whose replay conflicts with its own twin, and admits a blocked state: a
foreign linear commit at a pin's target stops every later promotion on that
table, and nothing resolves it. Stock `cleanup_old_versions` does not trace
detached manifests and deletes files only they reference, so Omnigraph decides
reclamation either way. Lance's row-lineage stamps on a detached commit are
not monotonic, so change windows over them are wrong without promotion.

## Decision

- No writer, `cleanup` or other command promotes a pin. `published_dataset_version`
  is the table's logical version inside one `__manifest` lineage and names no
  Lance version. Any transaction kind Lance accepts detached may be staged.
- Each registration records `omnigraph.last_linear_version`, and every writer
  that rebuilds the row copies it forward. A staged pin above it opens its
  detached version directly; a pin at or below it is a historical promoted row
  and resolves through its linear twin by transaction uuid.
- A graph branch writes on the dataset and native ref its inherited
  registration names. No per-table fork is created; a fork an older graph made
  stays valid while a row names it.
- `cleanup` never calls `cleanup_old_versions` on a graph table. Its roots are
  the pins of every `__manifest` version that `--keep` and `--older-than`
  retain on each live branch, graph snapshots and table versions held by
  native tags, and selected merge bases. It sweeps a once-published detached
  manifest that no root needs, with the files only it references.
- A detached manifest no `__manifest` version named is unpublished staging. It
  is swept only when the authority it recorded,
  `omnigraph.staged_against_branch_incarnation` and
  `omnigraph.staged_against_graph_head`, is provably dead: the incarnation is
  gone, or the branch's head is strictly past it and no captured version names
  the staging. Undecidable ownership retains it.
- A linear commit above the last linear version is foreign: never adopted,
  never deleted, reported per table as `foreign_versions`. No read or write
  resolves a graph table's linear HEAD.
- An object that no listed manifest references is deleted only when older than
  seven days (`UNVERIFIED_THRESHOLD_DAYS`).
- Change discovery reads the commit's own transaction: inserts and updates
  from its new fragments, deletes from `omnigraph.deleted_ids` or the file
  `omnigraph.deleted_ids_path` names. An interval the transaction cannot prove
  falls to the later rungs of
  [Change discovery by fragment and row identity](2026-10-26-change-discovery-by-fragment-identity.md).

## Rejected

- **Keep promotion:** carries the reconciler, the twin proof, the staging rule,
  GC deferral and a blocked state with no resolution.
- **Lazy promotion:** keeps every mechanism and lengthens the pending window,
  in which the change feed falls back to an unbounded exact comparison.
- **Collector with an age horizon:** a writer can outlive any age between its
  staging and its publication.
- **Version columns written by Omnigraph:** a second copy of the graph version
  on every row, which every writer must keep in agreement with `__manifest`
  (invariant 12).
- **Wait for Lance to trace detached manifests:** no upstream movement; the
  request stays open and nothing waits on it.

## Not built

- A writer whose publication was refused, not in doubt, deletes its own
  detached manifests before it prepares again. The collector reclaims them
  instead.
