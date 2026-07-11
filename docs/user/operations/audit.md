# Audit & Actor Tracking

Every write in OmniGraph records **who made it**. The actor id is persisted on the
graph commit, so the commit history is an audit trail of which actor changed the
graph and when.

## Where the actor comes from

The actor is resolved differently depending on the front end, but it always lands
on the commit:

- **HTTP server** — the actor is resolved **server-side from the bearer token**. A
  client cannot set its own actor id; it is derived from the authenticated token.
  See [policy](policy.md) for how tokens map to actors.
- **CLI / embedded** — the actor is self-declared through one resolution chain:

  1. `--as <actor>` on the command,
  2. then `operator.actor` in `~/.omnigraph/config.yaml` (see the
     [CLI reference](../cli/reference.md)),
  3. otherwise none.

This difference is intentional: storage credentials imply a self-declared actor,
while a server resolves the actor from a token it trusts.

## Reading the audit trail

Actor ids are stored on each commit in the [commit graph](../branching/index.md).
List commits to see who made each change:

```bash
omnigraph commit list graph.omni
```

System-initiated writes use reserved actor ids. Rollback and legacy recovery
commits use `omnigraph:recovery`, while a v3 roll-forward preserves the
interrupted writer's original commit id and actor. Exact recovery actions and
per-table outcomes are stored in the internal
`_graph_commit_recoveries.lance` audit table; the CLI does not currently expose
that table, so `commit list` alone is not a complete recovery log.

## What is tracked

Every successful publish — load, change, branch merge, and schema apply — appends a
commit carrying the resolving actor. Because publishes are atomic, the actor on a
commit is exactly the actor responsible for that whole change.
