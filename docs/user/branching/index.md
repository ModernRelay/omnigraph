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

Creating a branch defaults to `main` when `--from` is omitted. A load can create
a missing target branch by combining `--branch <name>` with `--from <base>`.

Branches are cheap until written: unchanged data remains shared with the source.
A branch remains after a normal merge, so prefer `--delete-branch` or delete it
when review is complete. Live branches retain the history they depend on and can
prevent `omnigraph cleanup` from reclaiming old data.

Branch names may contain `/`, but live names must not be path prefixes of one
another. For example, `review` and `review/alice` cannot coexist. `main` is
reserved. A branch with descendants must be deleted leaf-first.

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

## Historical reads

Every read targets either a live branch or an immutable graph commit. Take a
`graph_commit_id` from `commit list --json` and pass it as the snapshot:

```bash
omnigraph query sources_for_claim --query queries.gq \
  --params '{"claim":"lower-latency"}' \
  --snapshot <graph-commit-id> --store graph.omni
```

A query stays on one snapshot for its entire lifetime. Historical reads can
eventually fail after destructive cleanup removes the versions that commit
needs. Branch deletion can likewise end access to branch-only history.

## Changes and feeds

Inspect the logical changes made by one commit:

```bash
omnigraph commit changes <commit-id> --store graph.omni --json
```

For continuous consumption, `omnigraph changes poll` follows complete commits
on one branch and returns an opaque resume cursor. If cleanup has reclaimed
required history, create a coherent snapshot and new cursor with
`omnigraph changes baseline`.

See [Changes and Change Feeds](changes.md) for filters, pagination, cursor
checkpointing, and `410` retention-gap recovery.

See [Merging Branches](merge.md) for merge outcomes and conflict handling.
