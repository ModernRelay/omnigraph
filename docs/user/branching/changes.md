# Changes and Change Feeds

OmniGraph can describe what one commit changed or deliver an ordered feed of
changes on a branch. Both surfaces report logical nodes and edges, not storage
details.

## Inspect one commit

```bash
omnigraph commit changes <commit-id> --store graph.omni --json
```

The equivalent HTTP route is
`GET /graphs/{graph_id}/commits/{commit_id}/changes`. The commit is compared
with its first parent. Each response includes:

- `cause`: the commit id, parent, optional merged parent, authored branch,
  optional actor, and authorship time in Unix microseconds.
- `changes`: inserts with `after`, updates with `before` and `after`,
  and deletes with `before`. Each image contains `properties`; an edge image
  also contains `endpoints: {from, to}`.

Filter with repeatable `--kind node|edge`, `--type <name>`, and
`--op insert|update|delete` options. `--limit` defaults to 1,000 and may be
at most 8,192. Results are deterministic by entity kind, type identity, logical
id, and operation.

Large results are paginated. The CLI normally follows every
`next_page_token`; passing `--page-token` fetches exactly one page. A page
token continues that one commit result and is not a change-feed cursor.

A parentless commit returns `409` with reason `parentless_commit`. A diff
that crosses a schema boundary returns `409` with reason
`schema_boundary`; take a new baseline instead of treating either response
as an empty change.

## Follow a branch

```bash
# Start at the current head. Existing history is not replayed.
omnigraph changes poll --start now --store graph.omni --json

# Replay the branch's first-parent history.
omnigraph changes poll --start beginning --store graph.omni --json

# Continue after a known commit, or resume from a saved cursor.
omnigraph changes poll --start after:<commit-id> --store graph.omni --json
omnigraph changes poll --cursor <cursor> --store graph.omni --json
```

`now` is the default when neither `--start` nor `--cursor` is supplied.
Use `--branch` to follow a branch other than `main`. The same kind, type,
operation filters, and `--limit` supported by `commit changes` are available
here.

Each poll captures a fixed branch head and returns complete commits in
first-parent order. A commit with no logical entity changes may still appear
as an empty block and advance the feed.

The durable `cursor` appears only on the terminal page, after every returned
commit block is complete. A page ending partway through a commit has only a
`next_page_token`. On the terminal page, `caught_up` says whether the poll
reached its captured head. The CLI consumes feed page tokens itself and prints
the cursor only after reaching that terminal page.

Delivery is at least once: retrying a cursor may replay the next complete
commit. Apply each block idempotently by `graph_commit_id`, then persist the
terminal cursor atomically with the applied blocks.

Cursors are opaque and caller-owned; the server stores no consumer position.
A cursor is bound to its graph, branch lifetime, and filter scope. Reusing it
with a different scope, or using a page token as a cursor, returns `400`.

The HTTP route is `GET /graphs/{graph_id}/changes`. Its `cursor`, `start`,
and `page_token` parameters are mutually exclusive.

## Recover from a retention gap

A cursor does not prevent `cleanup` from reclaiming old history. When a feed
or commit diff can no longer be read, the server returns `410` with
`change_feed_gap`, including `first_unreadable_commit_id` and, when
available, the rejected `cursor`. Retrying the same cursor cannot close the
gap; install a new baseline:

```bash
omnigraph changes baseline --out snapshot.jsonl --store graph.omni --json
```

If a poll has already accumulated complete readable blocks before it reaches a
gap, it first returns those blocks with `caught_up: false`; polling the
returned cursor then produces the deterministic `410`.

The HTTP equivalent is `POST /graphs/{graph_id}/changes/baseline`. Send `{}`
for the default `main` scope or include branch and filter fields. The
`application/x-ndjson` response streams an exact entity snapshot followed by
one terminal record:

```json
{"baseline":{"snapshot_commit_id":"...","resume_cursor":"..."}}
```

An interrupted stream has no terminal record and therefore no usable cursor.
Install the complete snapshot durably before saving `resume_cursor`; the
resumed feed begins with commits after the captured snapshot.

Baseline kind and type filters select the snapshot contents. An operation
filter applies only to the resumed feed. Baselines require the `export`
policy action; commit changes and feed polling require `read`.

The CLI's durable `--out` installation is available on POSIX platforms. The
file contains only snapshot entity records, not the terminal handshake. The CLI
syncs and atomically replaces it before printing
`{"snapshot_commit_id":"...","resume_cursor":"..."}` to JSON stdout.
