# Branches, Commits, Snapshots

## L1 — Lance per-dataset branches

Lance supports branching at the dataset level: a branch is a named lineage of versions, and a copy-on-write fork creates a new branch from a source branch at a given version.

## L2 — Graph-level branches

OmniGraph builds *graph branches* on top by branching every sub-table coherently:

- **Create** (`branch create` / `branch create --from <target>`) — the name `main` is disallowed; fails if the branch exists. Atomic: the new branch becomes visible all-or-nothing, so a name never half-exists.
- **List** (`branch list`) — returns public branches, **filtering the internal** `__schema_apply_lock__` branch.
- **Delete** (`branch delete`) — refuses if there are descendants on the branch, or if it is the current branch. Once deleted, the branch is gone from every snapshot. The owned per-table forks are reclaimed best-effort; if that reclaim hits a transient object-store error, the leftover storage is reclaimed later by the [`cleanup`](../operations/maintenance.md) command. One consequence: if a delete's reclaim fails, reusing that branch name before the next `cleanup` surfaces a clear error pointing at `cleanup`.
- **Lazy forking**: a branch only forks a sub-table when that sub-table is first mutated on it. Pure-read branches share storage with their source. If two writers race to first-write the same branch, the loser gets a retryable "refresh and retry".

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
