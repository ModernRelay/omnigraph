# Vendored `lance-table` 7.0.0 + lance#7480 (omnigraph patch pin)

This directory is the **pristine `lance-table` 7.0.0 crates.io source** (unpacked
from the published `.crate`) carrying exactly one upstream fix, cherry-picked
from [lance-format/lance#7480](https://github.com/lance-format/lance/pull/7480)
(merged to Lance main 2026-07-01, first present in no release ≤ 8.0.0):

- `src/rowids/index.rs` — `RowIdIndex::new` no longer asserts that overlapping
  row-id chunks densely tile their range (an update-style `merge_insert`
  legally reuses the updated rows' stable ids in new fragments while the
  superseded fragment keeps its full sequence + a deletion vector; a later
  delete leaves the union short of the span). The real invariant — the same
  live id claimed by two fragments — is now a hard error in
  `merge_overlapping_chunks` instead. Upstream's regression unit test is
  included.

Without the fix, any filtered read that builds the row-id index on such a
table fails: `rowids/index.rs:50` "Wrong range" debug assert; "all columns in
a record batch must have the same length" (or a silently-wrong batch) in
release. Bug: [lance#7444](https://github.com/lance-format/lance/issues/7444),
tracked as `iss-merge-rowid-overlap-corrupts-filtered-reads` /
`blk-lance-7444` on the dev graph.

Wired up via `[patch.crates-io] lance-table = { path = "vendor/lance-table" }`
in the workspace root `Cargo.toml`.

## Removal condition

Delete this directory and the `[patch.crates-io]` entry at the **first Lance
bump whose `lance-table` ships lance#7480** — 9.0.0, or a backported 8.0.1 if
upstream cuts one. The runtime guard
`crates/omnigraph/tests/lance_surface_guards.rs::filtered_scan_tolerates_merge_update_row_id_overlap`
pins the fixed behavior: it goes red if the patch is dropped too early or a
future bump regresses the fix.

## Verifying the delta

```bash
# The full diff vs the published crate should be ONLY the #7480 hunk + this README:
tar -xzf ~/.cargo/registry/cache/index.crates.io-*/lance-table-7.0.0.crate -C /tmp
diff -ru /tmp/lance-table-7.0.0 vendor/lance-table
```
