# Lineage-based merge deltas

**Decides:** a merge finds the rows that can differ between base and side from Lance fragment and deletion-file metadata, reads only those candidates by id, and falls back to the full ordered three-way walk whenever the metadata cannot prove the candidate set.
**Contract:** keeps E5, E7, merge:1, merge:2
**Code:** `crates/omnigraph/src/exec/merge.rs`, `crates/omnigraph-compiler/src/settings.rs`, `crates/omnigraph/src/ordered_cursor.rs`
**Cited as:** RFC 0027

## Context

The three-way classifier streamed base, source and target tables in full and compared rows, so a one-row change read every row and every wide property, embeddings and Blobs included. The obvious replacement, filtering Lance's row-version columns, is still a full column scan and cannot see a deleted row, which is absent from the side it was deleted on. Deletions decide whether a merge is bounded by its change or by its table.

## Decision

- A fragment with the same data files, overlay files and deletion file in two versions holds identical live rows in both, because Lance files are uniquely named and never rewritten in place. Every changed, new or deleted row therefore lies in a fragment that is new or rewritten on one side, or in the deletion-vector difference of a shared fragment.
- The candidate set is the ids in those places: a superset. False positives, such as rows compaction moved, classify as unchanged through the shared comparison; false negatives cannot occur while fragment files stay immutable.
- Candidates are fetched by `id IN (...)` chunks scoped to the fragments that can hold them, and pass through the unchanged three-way truth table and the shared constraint validator.
- The plan is gated: an unprovable shape or a candidate set over its byte budget falls back to the ordered walk before any effect, and the fallback is counted. An optimization miss is never a merge failure.
- The `merge_lineage` setting selects `on` (the release default), `off` (the walk only) or `verify` (both, failing the merge on any difference); a debug build defaults to `verify`.
- The ordered walk stays as the correctness fallback.

## Rejected

- **Filter `_row_created_at_version` and `_row_last_updated_at_version`:** still a scan of every row, and it cannot identify deletions.
- **Consume an upstream Lance change log:** no public, pinned API yields deleted row ids by version range.
- **Write per-commit deleted-id rows into the `__manifest` publication:** a storage format liability that needs its own format decision, for data the fragment metadata already implies.
- **Remove the ordered walk:** no metadata path covers every shape, and a physical gap must never break or weaken a logical merge.
