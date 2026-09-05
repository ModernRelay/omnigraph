# Merging Branches

A merge integrates one branch into another and publishes the target change
atomically across the whole graph.

```bash
omnigraph branch merge review/2026-04-25 --into main \
  --store s3://bucket/graph.omni
```

The source is positional. `--into` defaults to `main`.

A merge preserves changes already integrated into a branch when you later
merge that branch back. For example, after merging a new edge from `main`
into `review`, editing only a node on `review` and merging it into `main`
preserves that edge.

## Outcomes

- **Already up to date**: the target already contains the source changes.
- **Fast-forward**: the target has not diverged and advances to the source state.
- **Merged**: both branches changed, so OmniGraph performs a three-way,
  entity-level merge and creates a commit with two parents.

The source branch is unchanged by the merge. Use `--delete-branch` for the
normal review-branch lifecycle:

```bash
omnigraph branch merge review/2026-04-25 --into main --delete-branch \
  --store s3://bucket/graph.omni
```

Deletion happens after a successful merge and has its own authorization check.
If deletion is denied or the source still has descendants, the merge remains
successful and durable; the CLI prints a warning and the HTTP response reports
the deletion failure. You can delete the branch later.

Deleting a branch is irreversible and may make branch-only commits unavailable.
It also releases old history for a later cleanup.

## Conflicts

When both sides changed the same logical data incompatibly, the merge returns a
structured conflict list and publishes nothing.

| Kind | Meaning |
|---|---|
| `divergent_insert` | Both branches inserted the same id. |
| `divergent_update` | Both branches updated the same entity differently. |
| `delete_vs_update` | One branch deleted an entity the other updated. |
| `orphan_edge` | The result would contain an edge whose endpoint was deleted. |
| `unique_violation` | The result would violate uniqueness. |
| `cardinality_violation` | The result would violate edge cardinality. |
| `value_constraint_violation` | The result would violate an enum, range, or other value constraint. |

Each conflict identifies the affected type and, when applicable, entity id. The
HTTP server returns conflicts with status `409`. Reconcile the data on one or
both branches, then run the merge again.

## Merge classification mode

`OMNIGRAPH_MERGE_LINEAGE` selects how a branch merge finds what changed. `on`
(the release default) discovers candidates from Lance version metadata —
fragment lists and deletion files — and reads only the changed data, so merge
cost tracks the delta size instead of the dataset size; a fail-closed
precondition gate falls back to the full three-way scan whenever any
assumption cannot be proven (Blob-bearing schema, differing schemas or storage
paths across the pins, version pins not matching the manifest entries, missing
stable identifiers or the exact-id primary-key contract, non-linear history,
or a candidate set past its byte budget). `off` forces the full three-way scan
everywhere — the operational fallback if merge results are ever in question.
`verify` runs both, compares their decisions entity by entity, publishes the
scan's result, and fails the merge loudly on any divergence (the debug-build
default, used for validation; it costs both paths). A merge that succeeds
produces the same result in every mode; only cost differs. An unrecognized
value logs a warning and behaves as `off`.

## After a large merge

Indexes do not define merge correctness. Newly merged entities remain queryable even
when index coverage has not caught up, but some searches may scan them. Run
`omnigraph optimize` after a large merge to restore efficient layout and index
coverage.

See [Branches, Commits, and History](index.md) for the complete branch workflow.
