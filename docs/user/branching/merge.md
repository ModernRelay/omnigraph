# Merging Branches

Merging integrates the changes on one branch into another. OmniGraph merges are
**three-way and row-level**: it compares both branches against their common
ancestor and merges each node/edge table row by row, then publishes the result as
**one atomic commit** across the whole graph.

```bash
omnigraph branch merge review/2026-04-25 --into main s3://bucket/graph.omni
```

`branch merge <source> [--into <target>]` merges `<source>` into `<target>`
(default `main`).

## Outcomes

A merge resolves to one of three outcomes:

- **Already up to date** — the target already contains every change on the source;
  nothing to do.
- **Fast-forward** — the target has no changes the source lacks, so the target
  simply advances to the source.
- **Merged** — both sides diverged; a new merge commit is created with two parents.

## Indexes after a merge

A merge does not build or rebuild indexes on the rows it brings into the target.
Newly merged rows (and any index a table does not yet have) are covered the next
time `optimize` runs — indexes are derived state, and reads stay correct in the
meantime via brute-force scan over the not-yet-covered rows. This keeps a merge
fast (it never pays an inline vector/FTS rebuild on the publish path), at the
cost of brute-force search latency on freshly merged rows until the next
`optimize`. Run `omnigraph optimize` after a large merge to restore full index
coverage.

## Conflicts

When both branches changed the same data incompatibly, the merge fails with a
structured list of conflicts (the HTTP server returns `409` with a
`merge_conflicts[]` array). No partial result is published — the merge is
all-or-nothing. The conflict kinds are:

| Kind | Meaning |
|---|---|
| `DivergentInsert` | The same id was inserted on both branches. |
| `DivergentUpdate` | The same row was updated differently on both branches. |
| `DeleteVsUpdate` | One side deleted a row the other side updated. |
| `OrphanEdge` | An edge references a node the other side deleted. |
| `UniqueViolation` | The merged result would violate a unique constraint. |
| `CardinalityViolation` | The merged result would violate an edge cardinality constraint. |
| `ValueConstraintViolation` | The merged result would violate a value constraint (enum/range). |

Each conflict carries the table, the row id (when applicable), the kind, and a
message. Resolve conflicts by reconciling the two branches — typically by making
the conflicting change on one side and re-merging.

See [branches & commits](index.md) for the branch and commit-DAG model, and
[changes](changes.md) for diffing two branches before you merge.
