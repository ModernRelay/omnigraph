# Changes, Diffs and Change Feeds

OmniGraph can describe what one commit changed, compare any two points in
history, review what a branch changed, or deliver an ordered feed of changes
on a branch. Every surface reports logical nodes and edges, not storage
details, in one change shape.

HTTP calls follow the [HTTP contract](../operations/server.md#http-contract),
including the required request header and response check before consuming a
JSON or streamed body.

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
  also contains `endpoints: {from, to}`. A Blob property holds its export
  value (`base64:…` or an external URI), or, for a stored reference to a byte
  range of an object, `{"uri", "offset", "length"}` (see
  [Blobs](../blobs.md)).

Filter with repeatable `--kind node|edge`, `--type <name>`, and
`--op insert|update|delete` options. A page holds at most 8,192 changes; the
HTTP routes default to 1,000. Results are deterministic by entity kind, type
identity, logical id, and operation.

Large results are paginated. The CLI normally follows every
`next_page_token` and asks for 8,192 changes per page, because every page
resumes its comparison where the last one stopped; passing `--page-token`
fetches exactly one page, of 1,000 changes unless `--limit` sets the size. A
page token continues that one commit result and is not a change-feed cursor.

A parentless commit returns `409` with reason `parentless_commit`. A diff
that crosses a schema boundary returns `409` with reason
`schema_boundary`; take a new baseline instead of treating either response
as an empty change.

## Compare two points

```bash
omnigraph diff --from <commit-id> --to main --store graph.omni --json
```

Each side is a branch name, meaning its head when the diff starts, or a commit
id: a published commit, an unpublished proposal, or an unattached snapshot. A
string that names both a branch and a commit is refused; name the branch's
head by its commit id instead. The equivalent HTTP route is
`GET /graphs/{graph_id}/diff?from=<side>&to=<side>`.

The result is the net change from the `from` state to the `to` state, in the
`commit changes` change shape and order, with `from_commit_id` and
`to_commit_id` naming the exact commits the sides resolved to. There is no
`cause`: a range of commits has no single author. Intermediate history is not
replayed, so an update that was later reverted produces nothing, and a delete
followed by an equal reinsert produces nothing. A diff from a commit's first
parent to the commit equals that commit's `commit changes`, without the cause.

Comparing two branch heads also shows what the target changed since the
branches diverged, reversed. To review a branch, use the branch form below.

The kind, type and operation filters, `--limit` and `--page-token` work as for
`commit changes`. A page token keeps the commits the first page resolved, so
later pages answer for them even after the branches move. A diff across a
schema change returns `409` with reason `schema_boundary`, and a side whose
history was reclaimed returns `410` with `change_feed_gap`.

Cost grows with the size of each changed type, not with the number of
changes: apart from a single commit, every changed type is compared in full.
Types unchanged between the two points are skipped.

## Review a branch

```bash
omnigraph branch diff feature --into main --store graph.omni --json
omnigraph branch merge-base feature --into main --store graph.omni --json
```

`branch diff` reports what `feature` changed since its merge base with `main`
(`main` when `--into` is omitted): the net change from the merge base to the
head of `feature`. Changes `main` made after the merge base do not appear. The
response also names the base as `base_commit_id`. `branch merge-base` prints
that commit alone, the one a merge of `feature` into `main` would classify
against. While `feature` has never merged `main` in, the base is on its
first-parent history, and `changes poll --branch feature --start
after:<merge-base>` replays its commits since then; a merge commit in that
range still carries the changes it brought in. A base off `feature`'s
first-parent history is refused as a start: for example, after `main` is
merged into a `feature` that had diverged from it, the base is the `main`
commit that merge brought in. `branch diff` gives the net change from any
base.

The HTTP routes are `GET /graphs/{graph_id}/branches/diff?source=&target=` and
`GET /graphs/{graph_id}/branches/merge-base?source=&target=`. To see whether
the merge would succeed, run it as a [dry run](merge.md#preview-a-merge).

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

A cursor does not prevent `cleanup` from reclaiming old history. When a feed,
commit diff or diff can no longer be read, the server returns `410` with
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

Snapshot records use the export line format, and a Blob property holds the
same value shapes as a change image. That includes the
`{"uri", "offset", "length"}` object for a stored reference to a byte range of
an object, which export refuses; a snapshot holding one does not reload with
`load` (see [Blobs](../blobs.md)).

An interrupted stream has no terminal record and therefore no usable cursor.
Install the complete snapshot durably before saving `resume_cursor`; the
resumed feed begins with commits after the captured snapshot.

Baseline kind and type filters select the snapshot contents. An operation
filter applies only to the resumed feed. Baselines require the `export`
policy action; commit changes, diffs and feed polling require `read`. A diff
needs `read` on the branch each side resolves to; a side the actor cannot read
answers `404` exactly as a side that names nothing. `branch diff` and
`branch merge-base` need `read` on both branches, every page included, and
answer `403` otherwise.

The CLI's durable `--out` installation is available on POSIX platforms. The
file contains only snapshot entity records, not the terminal handshake. The CLI
syncs and atomically replaces it before printing
`{"snapshot_commit_id":"...","resume_cursor":"..."}` to JSON stdout.
