# Branches, Commits, Snapshots

## L1 — Lance per-dataset branches

Lance supports branching at the dataset level: a branch is a named lineage of versions, and `fork_branch_from_state(source_branch, target_branch, source_version)` creates a copy-on-write fork.

## L2 — Graph-level branches

OmniGraph builds *graph branches* on top by branching every sub-table coherently:

- `branch_create(name)` / `branch_create_from(target, name)` — disallowed name `main`; fails if branch exists; ensures the schema-apply lock is idle. Atomic and authority-first like `branch_delete`: it flips the `__manifest` branch (authority), then creates the derived commit-graph branch, force-dropping any orphaned commit-graph ref left by an incomplete prior delete (the manifest branch is fresh, so a same-named commit-graph branch is provably a zombie). If commit-graph creation fails, the manifest branch is rolled back so the name never half-exists.
- `branch_list()` — returns public branches, **filters internal** `__run__…` and `__schema_apply_lock__` prefixes.
- `branch_delete(name)` — refuses if there are descendants or active runs on the branch. The manifest is the single authority for branch existence: deletion flips the `__manifest` branch ref first (one atomic op), after which the branch is gone from every snapshot. The owned per-table forks and the commit-graph branch are derived state, reclaimed best-effort with `force_delete_branch` after the flip. A failure during that reclaim (transient object-store error) does not fail the call or block the authority flip; the leftover forks are unreachable orphans that the [`cleanup`](maintenance.md) reconciler converges. One consequence: if a delete's best-effort reclaim fails, reusing that branch name before the next `cleanup` surfaces a clear error pointing at `cleanup` (the stale fork would otherwise collide on first write).
- **Lazy forking**: a branch only forks a sub-table when that sub-table is first mutated on it. Pure-read branches share fragments with their source. A fork collision is classified by the manifest authority, not by Lance branch versions: if the live manifest already records the fork on the active branch, a concurrent first-write won and the caller gets a retryable "refresh and retry"; if the manifest does not, a physical branch there is an orphan and the caller is pointed at `cleanup`.
- `sync_branch(branch)` — re-binds the in-memory handle to the latest head of the branch.

## L2 — Commit graph (`db/commit_graph.rs`)

In-memory shape of a graph commit:

```
GraphCommit {
  graph_commit_id: ULID,
  manifest_branch: Option<String>,
  manifest_version: u64,
  parent_commit_id: Option<String>,
  merged_parent_commit_id: Option<String>,   // populated for merge commits
  actor_id: Option<String>,                  // joined in-memory from _graph_commit_actors.lance, NOT a column on _graph_commits.lance
  created_at: i64 (microseconds since epoch),
}
```

Storage is split across two Lance datasets (both with stable row IDs):

- `_graph_commits.lance` — every column above *except* `actor_id`.
- `_graph_commit_actors.lance` — optional separate `(graph_commit_id, actor_id)` map, created on demand. The `actor_id` field above is populated by joining this dataset in-memory at load time.

Notes:

- Every successful publish (load / change / merge / schema_apply) appends one commit.
- Merge commits have two parents; linear commits have one.
- API: `list_commits(branch)`, `get_commit(id)`, `head_commit_id_for_branch(branch)`.

## L2 — Snapshots & time travel

- `snapshot()` — current snapshot for the bound branch; cached.
- `snapshot_of(target)` — snapshot at a `ReadTarget` (branch | snapshot id).
- `snapshot_at_version(v: u64)` — historical snapshot from any manifest version.
- `entity_at(table_key, id, version)` — single-entity time travel without building a full snapshot.
- A `Snapshot` is a `(version, HashMap<table_key, SubTableEntry>)` — cheap to build, snapshot-isolated cross-table reads.

## L2 — Internal system branches

Filtered from `branch_list()` but visible to internals:

- `__schema_apply_lock__` — serializes schema migrations.
- `__run__<run-id>` — legacy from the pre-v0.4.0 Run state machine (removed in MR-771). The branch-name guard predicate `is_internal_run_branch` is kept as defense-in-depth so users cannot create a branch matching the legacy prefix; the filter will be removed once production legacy branches are swept (MR-770).

## L2 — Recovery audit trail

The five migrated writers (`MutationStaging::finalize`, `schema_apply`, `branch_merge`, `ensure_indices`, `optimize_all_tables`) protect their multi-table commits with a sidecar at `__recovery/{ulid}.json` written before Phase B and deleted after Phase C. The next `Omnigraph::open` (gated on `OpenMode::ReadWrite`) runs the recovery sweep in `crates/omnigraph/src/db/manifest/recovery.rs`: classify per-table state, decide all-or-nothing per sidecar, roll forward / back, record an audit row.

Audit rows live in `_graph_commit_recoveries.lance` (sibling to `_graph_commits.lance`) and reference the commit graph by `graph_commit_id`. The linked recovery commit is identified by that same `graph_commit_id`, and `actor_id="omnigraph:recovery"` is stored in `_graph_commit_actors.lance` (joined by `graph_commit_id`) — `_graph_commits.lance` itself does not carry the `actor_id` column. To find recoveries for a specific original actor: `omnigraph commit list --filter actor=omnigraph:recovery`, then join to `_graph_commit_recoveries.lance` by `graph_commit_id` to read `recovery_for_actor`. Schema: see `crates/omnigraph/src/db/recovery_audit.rs`.
