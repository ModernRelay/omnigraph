# Branches, Commits, and History

A branch is an isolated, durable graph history. Use branches to prepare and
review a multi-step change without exposing intermediate results on `main`.

## Branch workflow

```bash
# Create an isolated branch from main.
omnigraph branch create review/add-benchmark --from main --store graph.omni

# Write and inspect it.
omnigraph load --data benchmark.jsonl --mode append \
  --branch review/add-benchmark graph.omni
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' \
  --branch review/add-benchmark --store graph.omni

# Publish its result to main, then remove the source branch.
omnigraph branch merge review/add-benchmark --into main --delete-branch \
  --store graph.omni
```

List and delete branches with:

```bash
omnigraph branch list --store graph.omni
omnigraph branch delete review/abandoned --store graph.omni
```

Creating a branch defaults to `main` when `--from` is omitted. To start a
branch at a past commit instead of a branch head, use `--from-commit`; see
[start a branch at a commit](#start-a-branch-at-a-commit). A load can create a
missing target branch by combining `--branch <name>` with `--from <base>`.

Each branch operation is also a GQ statement, so a client that already sends
`.gq` source needs no second transport. The three control writes go to
`POST /mutate`, and the listing and the branch reads to `POST /query`, each
with no request target, name, or parameters:

```text
branch create "review/add-benchmark" from main
branch create "fix/import" from commit "<commit-id>"
branch merge "review/add-benchmark" into main
branch delete "review/abandoned"
branch list
branch diff "review/add-benchmark" into main
branch merge-base "review/add-benchmark" into main
branch merge "review/add-benchmark" into main dry run
```

`from` defaults to `main` and `into` defaults to `main`. A name outside the
identifier alphabet (a lowercase letter or `_`, then letters, digits, or `_`),
such as one containing `/`, `-`, or `.`, is quoted, as above; `main`, `b0`,
and `_x` are bare. A control write answers a
`ChangeOutput` whose `outcome.kind` is `created`, `deleted`, or `merged`
(`outcome.merge` holds the merge result), with zero affected counts; a merge
that publishes a commit reports it in `commit`. `branch list` answers one row
per branch in column `name`, sorted by name. Each statement runs under the same
policy check as its HTTP route (`POST /branches`, `DELETE /branches/{branch}`,
`POST /branches/merge`, `GET /branches`), and `branch list` is a read, so it is
refused on `POST /mutate` as a control write is refused on `POST /query`. The
branch reads `branch diff`, `branch merge-base` and `branch merge … dry run`
answer rows as `branch list` does and take their routes' checks; see
[reviewing a branch](changes.md#review-a-branch) and
[previewing a merge](merge.md#preview-a-merge). A bare `dry run` after the
names is the clause, so a branch named `dry` is quoted.

Branches are cheap until written: unchanged data remains shared with the source.
A branch remains after a normal merge, so prefer `--delete-branch` or delete it
when review is complete. Live branches retain the history they depend on and can
prevent `omnigraph cleanup` from reclaiming old data.

## Branch names

Branch names may contain `/`, but live names must not be path prefixes of one
another. For example, `review` and `review/alice` cannot coexist. `main` is
reserved. A parent branch can be deleted while descendants remain. A path segment
may not end in `.` followed by 26 upper-case letters and digits: that shape is
reserved for OmniGraph's internal per-life branch identity. Ordinary dotted
names such as `release.1.2` are fine.

A branch name is used exactly as written. A name that is empty, has leading or
trailing whitespace, or contains a control character is refused wherever a
branch is named: the branch a read or write targets, `from`, merge sources and
targets, branch create and delete, Blob writes, change feeds and branch
statements. The server answers `400 Bad Request` before authorization, the
embedded engine returns a bad-request error before any effect, and the CLI
reports either error. Names are never trimmed, so `main ` does not name
`main`.

Deleting a branch and creating another with the same name yields a fresh
branch lifetime. Readers that captured the deleted branch fail with a typed
error rather than seeing the replacement. Deletion retires the logical name
and leaves native manifest history and table storage for explicit `omnigraph cleanup`; it starts no background table-reclaim work. Cleanup preserves data still needed by
other branches and their underlying histories.

Branch-control operations are safe across handles in one writer process. Do not
run branch create/delete control concurrently from separate writer processes
against the same graph.

## Atomicity model

OmniGraph does not provide connection-scoped `BEGIN` and `ROLLBACK`.

| Scope | Guarantee |
|---|---|
| One mutation query | All statements publish as one commit or none become visible. |
| One load request | The complete batch publishes as one commit or none becomes visible. |
| Several commands on a branch | Each command that publishes a change is a durable branch commit. Earlier commands are not rolled back if a later one fails. |
| Branch merge | The resulting source state becomes visible on the target in one atomic commit. |

Deleting an abandoned branch discards that workspace from normal access, but it
is an explicit lifecycle action rather than transaction rollback.

## Commits

Every write that publishes a change records a graph commit. A successful
mutation that matches no entities publishes no commit. A commit includes its
id, parent, branch, actor when known, and timestamp. Merge commits have two
parents.

```bash
omnigraph commit list graph.omni --branch main
omnigraph commit show <commit-id> --uri graph.omni
```

`commit list` is newest first. Omitting `--branch` shows history reachable from
`main`; selecting a branch includes the history inherited at its fork plus its
own commits.

A long history can be read a page at a time:

```bash
omnigraph commit list graph.omni --branch main --limit 50 --json
omnigraph commit list graph.omni --branch main --page-token <token> --json
```

`--limit` (1 to 1,000) returns one page with `head_commit_id`, the head the
listing resolved, and `next_page_token` while older commits remain. Pass the
token back with the same `--branch` for the next page, of 100 commits unless
`--limit` sets the size. Later pages answer for that head even after the
branch moves, so a page never repeats or skips a commit. A page reads only the
history it returns; without `--limit` or `--page-token` the whole history is
read and listed at once. Served under a policy, a caller whose `read` does
not cover every branch continues only from a commit of the branch's history,
and is refused with `400` otherwise: a commit the branch still holds, or one a
branch the caller may read published, costs nothing more, and any other is
looked for down the branch's history, which costs what the whole listing
costs. Served, the same listing is
`GET /commits?branch=<name>&limit=<n>` continued with `page_token=<token>`.
Paging applies to a branch's history, not to `--unpublished` or
`--unattached`.

### Start a branch at a commit

A branch can start at a published graph commit instead of a branch's head. It
holds that commit's rows and history, as if it had been created while the
commit was the head. Use it to try a fix or a revert where a change was made,
then merge the branch as usual; the commit is its merge base.

```bash
omnigraph commit list graph.omni --branch main --json
omnigraph branch create fix/import --from-commit <commit-id> --store graph.omni
```

The statement is `branch create "fix/import" from commit "<commit-id>"`, and
`POST /branches` takes the id in `from_commit`, which excludes `from` (a
request with both answers `400`). An id no commit has answers `404`. The
receipt names the branch the new
branch was forked off in `from`, for a commit start the branch that wrote the
commit, and the commit it starts at in `from_commit_id`. Every receipt carries
`from_commit_id`: for a start from a branch it is that branch's head when the
branch was created. A branch starts only at a commit whose whole snapshot is
still there under the graph's current schema; otherwise nothing is created,
and the error's `code` is `branch_start_refusal`, with `action: fix`:

| `from_commit` names | Answer | `branch_start_refusal.reason` |
|---|---|---|
| A commit whose table versions `cleanup` reclaimed, wholly or in a run that stopped part way | `410` | `not_retained`, with the type in `type_name` |
| A commit written under an earlier schema than the graph's | `409` | `schema_changed` |
| A commit written before the storage upgrade | `409` | `earlier_storage_format` |
| A commit of a branch that was deleted, even if a branch has since been created under its name | `409` | `branch_deleted`, with the branch in `branch` |
| An [unpublished commit](#unpublished-commits) | `409` | `unpublished_commit`; publish it, then start at the published commit |
| An [unattached snapshot](#unattached-branches) | `409` | `unattached_snapshot`; it never becomes branch history |

While the new branch lives, `cleanup` keeps the commit's table versions, so a
later start at the same commit succeeds too.

### Unpublished commits

A mutation or a merge written `as commit` records its result as an unpublished
commit: the branch head is unchanged, the result is readable at the commit's
id (`--snapshot <id>`), and the commit lists with `published: false`.

```gq
query propose_bob() as commit {
    insert Person { name: "bob", age: 41 }
}

branch merge feature into main as commit

commit publish "<commit-id>"
commit drop "<commit-id>"
```

`commit publish` installs the commit on the branch it recorded as its target.
The published commit keeps the actor and timestamp of the write that created
it; the publish records no publisher. A published commit is answered as it
stands by a repeat publish and is never dropped. An unpublished commit expires
once an ordinary write moves its target branch's head past the commit's
parent, or when that branch is deleted or recreated, and can then only be
dropped: a publish is refused (a moved head fails its precondition), reads at
its id (`--snapshot`, `commit show`, `commit changes`) answer `not found`, and
it leaves the `--unpublished` listing.

```bash
omnigraph branch merge feature --into main --as-commit --uri graph.omni
omnigraph commit list graph.omni --branch main --unpublished
omnigraph commit publish <commit-id> --uri graph.omni
omnigraph commit drop <commit-id> --uri graph.omni
```

Served, the same control writes are `POST /commits/{commit_id}/publish`
(the published commit), `DELETE /commits/{commit_id}` (204), and
`GET /commits?unpublished=true&branch=<name>`; `POST /branches/merge` takes
`as_commit`, which excludes `delete_branch`. Publishing and dropping run under
the `commit_publish` and `commit_drop` policy actions on the recorded target
branch ([Policy](../operations/policy.md)).

### Unattached branches

A mutation written `as unattached branch` records its result as an unattached
snapshot: the branch head is unchanged, the result is readable at the
snapshot's id (`--snapshot <id>`), and the snapshot never publishes. A second
write `on "<id>"` (or `on $base` with a `String` parameter) builds on that
snapshot and returns a new id; the caller keeps the latest id as its tip. The
snapshot's branch is the one the first write ran on (`main` when the request
names none); a write `on` a snapshot runs on that recorded branch, and a
request that names another branch is refused. Two writes on one base each get
their own snapshot and see nothing of each other's.

```gq
query add_person($name: String) as unattached branch {
    insert Person { name: $name }
}

query add_address($id: String, $base: String) as unattached branch on $base {
    insert Address { owner: $id, city: "Delft" }
}
```

A write that changes nothing returns `commit: null` and creates no snapshot;
the caller keeps its previous tip. The receipt's `commit` carries
`kind: "unattached"`, `published: false` and `root_commit_id`, the first
snapshot of the chain (its own id on that first snapshot); `parent_commit_id`
is the base snapshot, or the branch head the first write ran on. A base must
be a live unattached snapshot: a published commit or an unpublished commit is
refused.

```bash
omnigraph commit list graph.omni --branch main --unattached
omnigraph query people --query queries.gq --snapshot <snapshot-id> --store graph.omni
omnigraph commit drop <snapshot-id> --uri graph.omni
```

`commit list --unattached` shows the branch's live snapshots, newest first,
flat; `parent_commit_id` tells the chains apart. `commit drop` of a snapshot
also drops every snapshot built on it; dropping the first one discards the
whole chain, and a snapshot built on another chain is untouched. `commit
publish` of an unattached snapshot is refused with HTTP 409 and the error
code `unattached_cannot_publish`; nothing is written. Every snapshot of a
chain expires when an ordinary write moves the branch head past the head the
first write captured, or when the branch is deleted or recreated: expired
snapshots answer `not found` to reads and writes and leave the listing. Dropping or expiring
a snapshot promises no storage reclamation; `cleanup` reports what it retains.

Served, the listing is `GET /commits?unattached=true&branch=<name>`
(`unattached` and `unpublished` together are refused) and the drop is the same
`DELETE /commits/{commit_id}` (204). Writing needs `change` on the recorded
branch; listing and reading a snapshot need `read` on it
([Policy](../operations/policy.md)).

## Historical reads

Every read targets either a live branch or an immutable graph commit. Take a
`graph_commit_id` from `commit list --json` and pass it as the snapshot:

```bash
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' \
  --snapshot <graph-commit-id> --store graph.omni
```

A query stays on one snapshot for its entire lifetime. Wildcard traversal is
refused on an explicit historical target because historical schema membership
is not reconstructed. Explicit edge alternatives retain named-edge historical
read rules.

A historical query is written against the current schema. A type renamed since
the commit still reads under its current name, but a property reads only where
the commit stores that same property under its current name. A property added
since the commit, renamed since, or dropped and re-added with the same name is
refused at that commit, naming the property, rather than answering with null or
with the dropped property's values. A whole-node `return { $p }` reads every
current property, so it is refused at commits made before a property was added.
Table data written before v0.10 records no property identity, so a commit that
reads such data refuses its properties too, unless that data is still the
table's current version.

Historical reads can eventually fail after destructive cleanup removes the
versions that commit needs. Branch deletion can likewise end access to branch-only history.

## Changes and feeds

Inspect the logical changes made by one commit, compare two points, or review
what a branch changed since it left `main`:

```bash
omnigraph commit changes <commit-id> --store graph.omni --json
omnigraph diff --from <commit-id> --to main --store graph.omni --json
omnigraph branch diff feature --store graph.omni --json
```

For continuous consumption, `omnigraph changes poll` follows complete commits
on one branch and returns an opaque resume cursor. If cleanup has reclaimed
required history, create a coherent snapshot and new cursor with
`omnigraph changes baseline`.

See [Changes, Diffs and Change Feeds](changes.md) for filters, pagination,
cursor checkpointing, and `410` retention-gap recovery.

See [Merging Branches](merge.md) for merge outcomes and conflict handling.
