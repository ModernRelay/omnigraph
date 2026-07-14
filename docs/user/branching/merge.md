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

## Deleting the source branch

A merge never touches the source branch — it stays in `branch list`, holds its
per-table storage, and pins the main versions it inherited against
[`cleanup`](../operations/maintenance.md) GC until someone deletes it. Since a
successful merge guarantees the target no longer depends on the source's
storage, the recommended lifecycle is to delete the source right after merging.

`--delete-branch` does that in one step (over HTTP, set `delete_branch: true`
on `POST /branches/merge`):

```bash
omnigraph branch merge review/2026-04-25 --into main --delete-branch s3://bucket/graph.omni
```

The deletion runs after the merge has landed, under its **own** `branch_delete`
policy check — an actor allowed to merge but not to delete branches gets the
merge without the deletion. Deletion runs on every successful outcome,
including `already_up_to_date` (the "already merged, clean me up" case). A
refusal or failure — policy denial, a dependent descendant branch, a branch
another branch's tables still fork from — never fails the request: the merge is
durable by then, so the CLI exits 0 with a stderr warning, and the response
reports `branch_deleted: false` plus `branch_delete_error` (on success,
`branch_deleted: true`). Deleting a branch is irreversible: its own commit
history is not retained (only the merge commit on the target records the merged
state).

## Outcomes

A merge resolves to one of three outcomes:

- **Already up to date** — the target already contains every change on the source;
  nothing to do.
- **Fast-forward** — the target has no changes the source lacks, so the target
  simply advances to the source.
- **Merged** — both sides diverged; a new merge commit is created with two parents.

## Indexes after a merge

A **fast-forward** merge (the common case — the target had no conflicting
changes, so the source's rows are adopted) does not build or rebuild indexes on
the rows it brings into the target. Newly merged rows (and any index a table does
not yet have) are covered the next time `optimize` runs — indexes are derived
state, and reads stay correct in the meantime via brute-force scan over the
not-yet-covered rows. This keeps a fast-forward merge fast (it never pays an
inline vector/FTS rebuild on the publish path), at the cost of brute-force search
latency on freshly merged rows until the next `optimize`.

A **three-way** merge (the `Merged` outcome — both branches changed the table and
the rows were reconciled) still rebuilds the table's indexes inline today, as part
of the publish. So a Merged-outcome merge of an embedding-bearing table pays the
index-build cost up front.

Either way, run `omnigraph optimize` after a large merge to restore (or, for the
fast-forward path, establish) full index coverage.

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
