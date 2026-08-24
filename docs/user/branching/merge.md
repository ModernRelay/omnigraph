# Merging Branches

A merge integrates one branch into another and publishes the target change
atomically across the whole graph.

```bash
omnigraph branch merge review/2026-04-25 --into main \
  --store s3://bucket/graph.omni
```

The source is positional. `--into` defaults to `main`.

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

## After a large merge

Indexes do not define merge correctness. Newly merged entities remain queryable even
when index coverage has not caught up, but some searches may scan them. Run
`omnigraph optimize` after a large merge to restore efficient layout and index
coverage.

See [Branches, Commits, and History](index.md) for the complete branch workflow.
