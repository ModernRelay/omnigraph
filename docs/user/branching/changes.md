# Change Detection / Diff

Diffing two read targets uses a three-level algorithm:

1. **Graph-manifest diff**: skip datasets whose legacy `(table_version, table_branch)` fields are unchanged.
2. **Lineage check**:
   - Same branch lineage → fast path: use the per-row `_row_last_updated_at_version` column to classify Insert/Update/Delete.
   - Different lineages → ID-based streaming comparison.
3. **Entity-level diff**: streaming, no full materialization.

## Public API

- `diff_between(from: ReadTarget, to: ReadTarget, filter: Option<ChangeFilter>) -> ChangeSet`
- `diff_commits(from_commit_id, to_commit_id, filter)` — cross-branch safe.

## Types

```
ChangeOp: Insert | Update | Delete
EntityKind: Node | Edge
EntityChange { kind, type_name, id, op, published_dataset_version, endpoints?: {src, dst} }
ChangeFilter { kinds?, type_names?, ops? }
ChangeSet { from_graph_manifest_version, to_graph_manifest_version, graph_branch?, changes[], stats }
```

## Ordering

Changed dataset lifetimes are grouped by entity kind and type name (edges before
nodes), with immutable dataset identity as the hidden tie-breaker when one type
name identifies multiple lifetimes across the compared snapshots. Entity order
within one dataset is not a public guarantee; callers that need their own total
order must sort the returned changes explicitly.

## Commit changes: exact per-commit entity diffs

`GET /graphs/{graph_id}/commits/{commit_id}/changes` (CLI:
`omnigraph commit changes <commit_id>`, SDK: `commit_changes_page`) returns
the entity changes one commit made **relative to its first parent**, in graph
vocabulary only:

- One **cause** per block: `graph_commit_id`, `parent_commit_id`,
  `merged_parent_commit_id` (merge commits are diffed against their first
  parent; the merged parent rides along for DAG-aware callers),
  `authored_branch` (the branch the commit originally landed on), `actor_id`,
  and `authored_at` — authorship time in Unix epoch microseconds, minted
  before dataset effects and stable across retries. It is deliberately not a
  commit or publication timestamp.
- Changes carry `kind` (node | edge), `type` (`id` — an opaque graph type
  identity that survives a supported rename and changes after drop/re-add —
  plus `name`), the logical `id`, `op`, and exact logical images: an insert
  has only `after`, an update exact `before` **and** `after`, a delete only
  `before`. Edge images embed `endpoints: {from, to}` per image.
- No response ever exposes backing datasets, incarnations, physical versions,
  fragments, or row addresses.

Order within a block is frozen: nodes before edges, then opaque type
identity, then `id`, then operation rank (insert < update < delete). Physical
no-ops — rewrites of identical values, compaction moving data — never surface
as changes; a physical-only commit (for example `optimize`) is an **empty
block**.

Large blocks paginate: a page carries `next_page_token` until the block is
complete. The token continues *that bounded response only* — it is never a
feed cursor — and binds the exact commit and filter scope. Filters
(`kind`, `type`, `op`; all repeatable) select graph concepts only; unknown
query parameters are rejected with 400.

The CLI auto-consumes those pages without rebuilding one in-memory result:
JSON keeps one output array open and human output prints each change before the
next page is fetched. `--page-token` instead fetches and prints exactly one raw
page. If a later auto-pagination request fails, the command exits nonzero and
stdout may contain the already-emitted prefix; redirect through a temporary
file and rename it after exit 0 when an all-or-nothing output file is required.
The embedded API exposes the bounded `commit_changes_page` primitive directly.

The parentless genesis commit has no diff (409, `parentless_commit`):
bootstrap from a baseline instead. A commit whose parent/child pair crosses a
schema change (a property add/drop rewriting a dataset, or dropping a type that
still holds data) is refused (409, `schema_boundary`) rather than guessed —
schema evolution is never synthesized into entity changes.

## The change feed

`GET /graphs/{graph_id}/changes` (CLI: `omnigraph changes poll`) streams
those blocks in **first-parent order** along one branch:

```
omnigraph changes poll --start beginning --json      # replay history
omnigraph changes poll --cursor <cursor> --json      # resume durably
```

- The first request picks an explicit start: `now` (default — capture the
  head, no replay), `beginning` (including inherited history on a named
  branch), or `after:<commit_id>` (an exact commit on the branch's
  first-parent chain). A missing cursor is never an implicit `beginning`.
- Each poll captures the branch head as its **cut**; commits landing mid-poll
  wait for the next poll. Empty blocks advance the feed.
- The durable **cursor** appears only on a terminal page and only after
  complete commits — a page that ends inside a block carries only
  `next_page_token`, so a partial block can never be checkpointed. The CLI
  consumes page tokens incrementally and prints the cursor only after the
  terminal page; the embedded API returns one bounded page at a time.
  `caught_up` on a terminal page says whether more complete commits already
  wait. A later-page CLI failure can leave a partial stdout prefix but never
  prints or advances the terminal cursor.
- The server persists **no consumer state**: the cursor is opaque,
  caller-owned, and valid from any handle or process. It binds the graph,
  the branch and its incarnation (deleting and recreating a branch invalidates
  its cursors), and the filter scope; any mismatch — or using a page token as
  a cursor — is a typed 400 with the stable prefix `change cursor rejected:`.
- Delivery is **at least once**: retrying a cursor may replay the complete
  next commit. Apply blocks idempotently by `graph_commit_id` and persist the
  cursor together with its blocks.

## Change baselines

A cursor is not a retention lease: `cleanup` may reclaim dataset versions a
retained commit pins. When the feed can no longer continue contiguously it
returns **410** with `change_feed_gap` (`cursor`,
`first_unreadable_commit_id`); direct diffs of a reclaimed commit return the
same gap. Retrying cannot succeed — recovery is the baseline handshake:

```
omnigraph changes baseline --out snapshot.jsonl --json
```

`POST /graphs/{graph_id}/changes/baseline` streams one exact data-only entity
snapshot pinned at a coherently captured head, then exactly one terminal
record `{"baseline": {snapshot_commit_id, resume_cursor}}`. The terminal
record is sent only after every snapshot record succeeded, so an interrupted
stream never yields a usable cursor. Install the snapshot durably **before**
the cursor; a commit landing after the capture is the first block the resumed
feed yields. A baseline is a full data export and requires the `export`
policy action (the feed and commit diffs require `read`).

The snapshot honors the scope's `kind` and `type` filters; `op` binds only
the resume cursor's feed scope.

The CLI's durable `--out` installer is currently POSIX-only. It fails before
capture on other platforms because the contract requires a file fsync,
atomic replacement, and parent-directory fsync before printing the cursor;
Windows needs a future write-through replacement implementation rather than a
weaker success claim.
