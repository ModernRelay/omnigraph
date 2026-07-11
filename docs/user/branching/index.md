# Branches, Commits, Snapshots

## L1 — Lance per-dataset branches

Lance supports branching at the dataset level: a branch is a named lineage of versions, and a copy-on-write fork creates a new branch from a source branch at a given version.

## L2 — Graph-level branches

OmniGraph builds *graph branches* on top with one authoritative `__manifest`
ref whose table entries form a coherent graph snapshot; data-table forks are
created lazily on first write:

- **Create** (`branch create` / `branch create --from <target>`) — the name `main` is disallowed; fails if the branch exists. Logically atomic: the branch becomes visible only when its authoritative ref exists, so a name never half-exists in graph reads. If storage interruption leaves only Lance's unreachable shallow-clone directory, the next same-name create reclaims it and retries automatically.
- **List** (`branch list`) — returns public branches, **filtering the internal** `__schema_apply_lock__` branch.
- **Delete** (`branch delete`) — refuses if there are descendants on the branch, or if it is the current branch. Once its authoritative ref is removed, the branch is gone from every snapshot even if reclaiming a now-unreachable storage directory needs a later retry. Owned per-table forks are reclaimed best-effort; same-name create/first write safely reconciles relevant leftovers, and [`cleanup`](../operations/maintenance.md) remains the general backstop.
- **Lazy forking**: a branch only forks a sub-table when that sub-table is first mutated on it. Pure-read branches share storage with their source. If two writers race to first-write the same branch, the loser gets a retryable "refresh and retry".
- **Names are path-prefix-disjoint while live.** Slash-separated names are
  supported (`review/2026-07`), but `review` and `review/2026-07` cannot both be
  live because Lance stores them in overlapping physical directories. Choose
  sibling names, or delete the existing ancestor/descendant first.

Graph branch create/delete are coordinated across handles in one writer
process. Until Lance exposes conditional native ref mutation, separate writer
processes must not concurrently control branches on the same graph.

For a legacy graph that already contains path-prefix-overlapping live names,
recovery also preserves the leaf-first escape hatch. If read-write open finds an
unresolved first-touch sidecar for an ancestor table fork while a live path-child
remains, it never deletes the child's storage. When the interrupted write owns no
table effect, it leaves the sidecar in place and completes the open so you can
delete the descendant branch leaf-first. When the same sidecar owns a partial
table effect, open fails closed because cleanup and rollback must finish together;
remove the descendant through an already-open handle or an offline Lance-level
branch tool, then reopen. The next read-write open reclaims the ancestor residue,
rolls back the partial effect, and retires the sidecar. `omnigraph repair` is not
that offline tool: it correctly refuses to run while a recovery sidecar is pending.

## L2 — Commit graph

Each graph commit carries a ULID id, the manifest branch and version it published, its parent commit (two parents for a merge commit, one for a linear commit), the actor who made it, and a creation timestamp.

- Every successful publish (load / change / merge / schema apply) appends one commit.
- Merge commits have two parents; linear commits have one.
- Inspect history with `commit list` and `commit show`.

## L2 — Snapshots & time travel

Reading a branch at a past version, or a single entity at a past version, is
covered on the [time travel](time-travel.md) page. Merging branches and the
conflict kinds are on the [merge](merge.md) page.

## L2 — Internal system branches

- `__schema_apply_lock__` — serializes schema migrations; filtered from `branch list` but used internally.

## L2 — Recovery audit trail

Interrupted multi-table writes are recovered automatically the next time the
graph is opened read-write. Each completed recovery is recorded internally in
`_graph_commit_recoveries.lance`. A roll-forward keeps the interrupted
writer's original commit id and actor; rollback and legacy recovery commits use
the reserved actor `omnigraph:recovery`. Consequently, `commit list` is not a
complete recovery log, and the CLI does not currently expose a query for the
internal recovery-audit table.
