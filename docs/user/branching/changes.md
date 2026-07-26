# Change Detection / Diff

Shows what changed between two read targets (branches, snapshots, or commits) —
the "what does this branch actually change" question a review needs.

The feed is an **identity** feed: it reports *which* entities changed and *how*,
not their property values. Read values on demand with a query or `entity_at`
once you know the ids. This keeps a diff of a large branch bounded.

## Tiers

Ask for the cheap tier first and page rows only on drill-down.

| Tier | Returns | Use |
|---|---|---|
| **Summary** | per-op counts + affected type names | landing view |
| **Rows** | one bounded page of changes | drill-down |
| **Values** | property values, via query / `entity_at` | row detail |

`from` and `to` are always required. An unbounded whole-graph diff is not an
exposed operation.

## CLI

```bash
# What changed on the branch, at a glance
omnigraph diff --from main --to review/2026-04-25 --summary --store s3://bucket/graph.omni

# Page the individual changes
omnigraph diff --from main --to review/2026-04-25 --limit 50 --format table --store …

# Narrow by type, kind, or operation
omnigraph diff --from main --to review/2026-04-25 --types Person,Company --ops insert,delete --store …

# Diff two commits instead of two branches
omnigraph diff --from-snapshot <commit-a> --to-snapshot <commit-b> --store …
```

When more changes follow, the output ends with a resume token; pass it back as
`--cursor`.

## HTTP

```
GET /graphs/{graph_id}/diff/summary?from=main&to=review
GET /graphs/{graph_id}/diff?from=main&to=review&limit=50&cursor=<token>
```

Query parameters: `from` / `to` (branch) or `from_snapshot` / `to_snapshot`
(snapshot or commit id) — exactly one per side; `types`, `kinds`, `ops` as
comma-separated filters; `limit` (default 100, clamped to 1000); `cursor`.

Both endpoints require the `read` action on **both** sides. A grant covering
only one branch is refused — a diff discloses content from both.

`stats` on the row endpoint describes the **returned page**. Call
`/diff/summary` for whole-diff totals.

## Ordering and paging

Changes are totally ordered by `(table_key, id)`. `cursor` is a keyset position
on that order, not an offset, so paging cannot skip or repeat a row — both
snapshots are immutable, so a page boundary is stable. Treat the token as
opaque; pass back exactly what you received.

## How it works

1. **Manifest diff**: skip sub-tables whose `(table_version, table_branch)` is
   unchanged. A branch that touched two types does not pay for the rest.
2. **Lineage check**:
   - Same branch lineage → use the per-row `_row_last_updated_at_version`
     column to find touched rows, then classify against the ids present at the
     `from` version.
   - Different lineages → compare id-ordered row sets from both snapshots.
     This is the path a branch-vs-main review takes for every table the branch
     wrote.
3. **Row-level diff**: classify Insert / Update / Delete.

Update detection compares a fixed-width digest of each row's user-visible
columns. Every column participates, including `Vector` and `Blob` — a row whose
only change is its embedding is reported as an `Update` — but only the digest is
retained per row, so cost does not scale with schema width.

### Cost notes

Both paths still read the rows they classify: the same-lineage filter on
`_row_last_updated_at_version` is a column scan, and the cross-lineage path
reads both id-ordered sets. `limit` bounds the response and stops the walk from
opening further tables; it does not bound work within a single table. Plan for a
diff to cost roughly a scan of the tables the branch touched.

## Public API (embedded SDK)

- `diff_between(from: ReadTarget, to: ReadTarget, filter: &ChangeFilter) -> ChangeSet`
- `diff_commits(from_commit_id, to_commit_id, filter) -> ChangeSet` — cross-branch safe.
- `diff_summary_between(from, to, filter) -> ChangeSummary` — totals without the row list.

## Types

```
ChangeOp: Insert | Update | Delete
EntityKind: Node | Edge
EntityChange { table_key, kind, type_name, id, op, manifest_version, endpoints?: {src, dst} }
ChangeCursor { table_key, id }
ChangeFilter { kinds?, type_names?, ops?, limit?, after? }
ChangeSet { from_version, to_version, branch?, changes[], stats, next_cursor? }
ChangeSummary { from_version, to_version, stats }
ChangeStats { inserts, updates, deletes, types_affected[] }
```

Design rationale, prior art, and why the diff is a route rather than a `.gq`
construct: [RFC-029](../../rfcs/0029-change-feed-surface.md).
