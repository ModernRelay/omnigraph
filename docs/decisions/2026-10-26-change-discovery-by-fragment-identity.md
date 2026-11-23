# Change discovery by fragment and row identity

**Decides:** a change read over a window with no adjacency proof skips identical fragments, pairs the remaining live rows by `_rowid` and skips those with equal update stamps, then runs the exact ordered merge over the candidates alone, but only where `from` is on `to`'s first-parent chain with no merge commit after it.
**Contract:** keeps read:5, read:6, read:12
**Code:** `crates/omnigraph/src/changes/row_identity.rs`, `crates/omnigraph/src/changes/candidate_scan.rs`, `crates/omnigraph-compiler/src/settings.rs`, `crates/omnigraph/tests/integration/changes_cost.rs`, `crates/omnigraph/tests/lance_surface_guards.rs`

## Context

Commit changes, the feed and both graph diffs compare a changed type's two
pinned table versions. Pruning by one adjacent transaction covers a single
write only. Without more, a branch with several commits, a three-way merge's
chain of chunks and a window across `optimize` take the exact ordered merge,
which reads both whole tables, so a review of ten changed rows grows with the
table.

## Decision

- The candidate ladder per changed type: the adjacency proof if one exists;
  else this rule when `change_lineage` is not `off` and the ancestry
  restriction holds; else the exact ordered merge.
- Fragments pair by id. A pair with the same resolved data and overlay files
  and the same deletion file is skipped unread.
- For every other fragment, the inline `_rowid` and
  `_row_last_updated_at_version` sequences are decoded, masked by its deletion
  vector. A `_rowid` live on both sides with equal stamps is unchanged. Every
  other live row is a candidate on its side.
- Each side runs the exact ordered merge over its touched fragments, narrowed by
  a structured `_rowid` in-list. The merge, by `id`, decides the operation, so
  results, order, tokens and cursors are those of the unnarrowed merge.
- Any doubt takes the exact merge: an absent, spilled or short sequence, more
  than 65,536 candidates on a side, more than 8 Mi held rows.
- The rule applies only where `from` lies on `to`'s first-parent chain with no
  merge commit after it, decided from lineage the `to` coordinator holds. A diff
  of diverged heads, a merge commit's page, and a window across a merge take
  the exact merge. A continuation decides it for the commits its token names.
- `change_lineage` is `on` by default, `verify` in debug builds (both paths run
  and the page is refused at the first disagreement), `off` the prior path. The
  REST change routes use the process default `OMNIGRAPH_CHANGE_LINEAGE`.
- Counters `change_intervals_row_identity` and `change_intervals_exact_merge`
  name the route each interval took; cost tests assert them.
- Nothing is persisted.

## Rejected

- **Fragment identity alone:** `optimize` rewrites every fragment, so a window
  across a compaction reads every row on both sides.
- **A proven `read_version` chain from `to` back to `from`:** one transaction
  read per link, and it breaks across compaction, overwrite and reclaimed
  intermediate chunks.
- **Change sets recorded at commit:** a second record beside the rows and a
  format change, for a cost existing metadata reaches.
- **Ordered stamp tests against the fork version:** the stamp is the read
  version plus one, so both lineages after a fork count from the same base.
- **A live `_rowid` set difference alone:** an update keeps its `_rowid`, so an
  updated row is live on both sides.

