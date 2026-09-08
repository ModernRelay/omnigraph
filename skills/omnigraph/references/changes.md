# Commits, Change Feeds, and Baselines

Use exact positions rather than time-based polling when a consumer must process
graph changes durably.

## Read and write positions

Read query JSON includes the `graph_commit_id` pinned with the returned rows
when the read snapshot has an effective graph head; a fresh pre-commit graph can
omit it. A conditional mutation requires an ID returned by the read.
An effectful `mutate --json` or `load --json` returns `commit` with the exact
commit published by that attempt; a no-op mutation returns `"commit": null`.

Protect a mutation derived from a read with `--if-commit <graph_commit_id>`.
Any intervening branch commit fails without effects (CLI exit `4`, HTTP `412`).
Re-read and decide again rather than retrying the stale mutation.

## Inspect one commit

```bash
omnigraph commit changes <commit-id> --store graph.omni --json
```

The commit is compared with its first parent. Inserts contain `after`, updates
contain `before` and `after`, and deletes contain `before`; edge images also
carry endpoints. Filter with repeatable `--kind`, `--type`, and `--op`.

Large results use `next_page_token`. The CLI normally follows every page;
`--page-token` fetches exactly one. A page token continues one commit result and
is not a feed cursor. Parentless commits and schema-boundary diffs return `409`;
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
no consumer position.

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

On POSIX, CLI `--out` syncs and atomically replaces the snapshot file before it
prints the handshake to JSON stdout. Baselines require Cedar `export`; commit
changes and feed polling require `read`.

Canonical contracts: [change feeds](../../../docs/user/branching/changes.md)
and [conditional mutations](../../../docs/user/mutations/index.md#conditional-mutations).
