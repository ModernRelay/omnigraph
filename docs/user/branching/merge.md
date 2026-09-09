# Merging Branches

A merge integrates one branch into another and publishes the target change
atomically across the whole graph.

```bash
omnigraph branch merge review/2026-04-25 --into main \
  --store s3://bucket/graph.omni
```

The source is positional. `--into` defaults to `main`.

The same merge as a GQ statement, sent to `POST /mutate` with no request
target, name, or parameters:

```text
branch merge "review/2026-04-25" into main
```

`into` defaults to `main`; a name with `/`, `-`, or `.` is quoted, a bare
identifier such as `main` is not. The answer is a `ChangeOutput` with
`outcome.kind = "merged"` and `outcome.merge` one of `already_up_to_date`,
`fast_forward`, or `merged`; after a `fast_forward` or `merged` result,
`commit` is the target's newest commit. The statement has no `--delete-branch`
composition: follow it with `branch delete <source>`. A merge statement takes
no commit precondition -- `POST /mutate/if-graph-commit` and `--if-commit`
refuse one beside it -- and no front offers a conditional merge today.

A merge preserves changes already integrated into a branch when you later
merge that branch back. For example, after merging a new edge from `main`
into `review`, editing only a node on `review` and merging it into `main`
preserves that edge. A merge whose source and target tables reached the same
version count on different branches fast-forwards; earlier releases refused
it with a `table version … already exists` error.

## Outcomes

- **Already up to date**: the target already contains the source changes.
- **Fast-forward**: the target has not diverged and advances to the source state.
- **Merged**: both branches changed, so OmniGraph performs a three-way,
  entity-level merge and creates a commit with two parents.

When only the source changed a table since the merge base, a merge into a
named branch can adopt that exact table snapshot without copying its rows.
This also works when the target already has its own table history. Tables
changed on both sides still use the normal three-way merge and constraint
checks.

The target can continue writing after adoption. Other live branches keep their
current table snapshots through those target writes and cleanup;
creating a replacement branch is unnecessary. Cleanup retains the underlying
histories they still need. Avoiding a table copy does not eliminate the reads
needed to validate a merge.

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

A merge classifies both sides against their merge base, the nearest commit
both branches descend from, counting a merged branch as an ancestor. Two
branches that each merged the same third branch therefore share that
branch's commit as their base, and an entity only one of them changed after
that import merges cleanly. The record of such a commit lives in the branch
it was merged from, and the merge reads it from any live branch whose
lineage still holds it. Once no live branch holds it, the base falls back to
the older common commit, so an entity both sides received from the deleted
branch and one side then changed can report `divergent_update`.

Each conflict identifies the affected type and, when applicable, entity id. The
HTTP server returns conflicts with status `409`, the same answer for
`POST /branches/merge` and for a `branch merge` statement on `POST /mutate`.
Reconcile the data on one or both branches, then run the merge again.

### Edges inserted on both sides

Whether the same edge added on both branches converges depends on the edge
type's declared identity:

- No declaration: edge ids are generated, so each side's insert is its own
  row and the merge keeps both. This is the documented multiset default;
  parallel edges are legitimate data.
- `@unique(src, dst)`: each branch's write succeeds on its own, and the
  merge reports `unique_violation`. This is the available guard on
  releases that predate edge keys.
- `@key(src, dst)`: both sides derive the same id, so identical inserts
  converge to one row with no conflict. If the sides disagree on a non-key
  property, the merge reports `divergent_insert` on the edge type with the
  derived id as the entity id (for `@key(src, dst)` a JSON array such as
  `["Alice","Bob"]`; the elements are the endpoint node ids, so they are
  generated ids when the endpoint type declares no key). Reconcile it like any divergent insert: align the
  property on one branch (insert the same key again with the agreed
  values; the insert upserts), then merge again.

## Merge classification mode

`OMNIGRAPH_MERGE_LINEAGE` selects how a branch merge finds what changed. `on`
(the release default) discovers candidates from Lance version metadata —
fragment lists and deletion files — and compares candidate rows. Known deleted
row positions use bounded direct reads. Candidate filtering may still scan data
when an applicable index is absent; a fail-closed
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

Later writes create fresh table storage when needed and leave unused former
storage for explicit cleanup. They do not reclaim a previous table history as
part of preparing the write or merge.

## After a large merge

Indexes do not define merge correctness. Newly merged entities remain queryable even
when index coverage has not caught up, but some searches may scan them. Run
`omnigraph optimize` after a large merge to restore efficient layout and index
coverage.

See [Branches, Commits, and History](index.md) for the complete branch workflow.
