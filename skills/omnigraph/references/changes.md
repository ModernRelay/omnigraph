# Commits, Change Feeds, and Baselines

Use exact positions rather than time-based polling when a consumer must process
graph changes durably.

## Read and write positions

Read query JSON includes the `graph_commit_id` pinned with the returned rows
when the read snapshot has an effective graph head; a fresh pre-commit graph can
omit it. A conditional mutation requires an ID returned by the read.
An effectful data `mutate --json` or `load --json` returns `commit` with the exact
commit published by that attempt; a no-op mutation returns `"commit": null`.

Protect a mutation derived from a read with `--if-commit <graph_commit_id>`.
Any intervening branch commit fails without effects (CLI exit `4`, HTTP `412`).
Re-read and decide again rather than retrying the stale mutation.

## Branch statements

Canonical `POST /mutate` also accepts a single GQ `branch create`, `branch
delete`, or `branch merge` statement without `name`, `params`, or a request
`branch`. Its `outcome.kind` is `created`, `deleted`, or `merged`; a merged
outcome carries `merge` (`already_up_to_date`, `fast_forward`, or `merged`).
Affected entity counts are zero because these are branch controls.

Creation and deletion return `commit: null` despite changing branch state. A
merge's optional `commit` is a target-head lookup after its gates are released,
so another writer may already have moved it; it is not the data mutation's
exact-attempt receipt. Inspect the outcome and relevant history. `branch list`
goes to canonical `POST /query` and returns sorted `name` rows; branch statements
are refused on deprecated query/mutation routes and the conditional-write route.

## Inspect one commit

```bash
omnigraph commit changes <commit-id> --store graph.omni --json
```

The commit is compared with its first parent. Inserts contain `after`, updates
contain `before` and `after`, and deletes contain `before`; edge images also
carry endpoints. Filter with repeatable `--kind`, `--type`, and `--op`.

Images retain null-valued property keys: `null` means a null cell, while absence
means that property was outside the commit's schema. Dates and floats use
current JSON spelling. A changed user schema is still a refused schema-boundary
diff; the system-column spelling change alone preserves that user schema.

Large results use `next_page_token`. The CLI normally follows every page;
`--page-token` fetches exactly one. A page token continues one commit result and
is not a feed cursor. Parentless commits and unsupported schema-boundary diffs return `409`;
capture a baseline instead of treating them as empty changes.

## Follow a branch

```bash
omnigraph changes poll --start now --store graph.omni --json
omnigraph changes poll --start beginning --store graph.omni --json
omnigraph changes poll --start after:<commit-id> --store graph.omni --json
omnigraph changes poll --cursor <cursor> --store graph.omni --json
```

Each poll captures a fixed branch head and walks complete commits in first-parent
order. `now` is the default. A durable cursor appears only on the terminal page;
an intermediate page has only `next_page_token`. `caught_up` says whether the
terminal page reached the captured head.

Delivery is at least once. Apply each block idempotently by `graph_commit_id`,
then atomically persist the terminal cursor with the applied blocks. Cursors are
opaque and bound to graph, branch lifetime, and filter scope; the server stores
no consumer position. Reusing a cursor with a different scope, or passing a
page token as a cursor, returns `400`.

A feed that reaches a user-schema change returns the blocks completed before it
with `caught_up: false`; the next poll returns `409 change_diff_refusal`
(`reason: schema_boundary`). Capture a new baseline, as for a retention gap.

## Recover from retention gaps

Cleanup can reclaim history needed by a cursor. A `410 change_feed_gap` cannot
be repaired by retrying the same cursor. Capture and install a new baseline:

```bash
omnigraph changes baseline --out snapshot.jsonl --store graph.omni --json
```

The HTTP equivalent streams snapshot entity records followed by one terminal
record containing `snapshot_commit_id` and `resume_cursor`. An interrupted
stream has no usable cursor. Install the complete snapshot durably before
saving that cursor; resumed delivery begins after the captured snapshot.
Kind/type filters scope the snapshot; an operation filter applies only to the
resumed feed.

HTTP uses `GET /graphs/{id}/changes` for polling and
`POST /graphs/{id}/changes/baseline` for the snapshot handshake. Snapshot entity
records use the export envelope with top-level `id`, separate from user `data`.

On POSIX, CLI `--out` syncs and atomically replaces the snapshot file before it
prints the handshake to JSON stdout. Baselines require Cedar `export`; commit
changes and feed polling require `read`.

Canonical contracts: [change feeds](https://github.com/ModernRelay/omnigraph/blob/v0.11.0/docs/user/branching/changes.md)
and [conditional mutations](https://github.com/ModernRelay/omnigraph/blob/v0.11.0/docs/user/mutations/index.md#conditional-mutations).
