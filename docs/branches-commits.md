# Branches, Commits, Snapshots

## L1 — Lance per-dataset branches

Lance supports branching at the dataset level: a branch is a named lineage of versions, and `fork_branch_from_state(source_branch, target_branch, source_version)` creates a copy-on-write fork.

## L2 — Graph-level branches

OmniGraph builds *graph branches* on top by branching every sub-table coherently:

- `branch_create(name)` / `branch_create_from(target, name)` — disallowed name `main`; fails if branch exists; ensures the schema-apply lock is idle.
- `branch_list()` — returns public branches, **filters internal** `__run__…` and `__schema_apply_lock__` prefixes.
- `branch_delete(name)` — refuses if there are descendants or active runs on the branch; cleans up owned per-branch fragments.
- **Lazy forking**: a branch only forks a sub-table when that sub-table is first mutated on it. Pure-read branches share fragments with their source.
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

## L2 — Recovery audit trail (MR-847)

The four migrated writers (`MutationStaging::finalize`, `schema_apply`, `branch_merge`, `ensure_indices`) protect their multi-table commits with a sidecar at `__recovery/{ulid}.json` written before Phase B and deleted after Phase C. The next `Omnigraph::open` (gated on `OpenMode::ReadWrite`) runs the recovery sweep in `crates/omnigraph/src/db/manifest/recovery.rs`: classify per-table state, decide all-or-nothing per sidecar, roll forward / back, record an audit row.

Audit rows live in `_graph_commit_recoveries.lance` (sibling to `_graph_commits.lance`) and reference the commit graph by `graph_commit_id`. The linked `_graph_commits.lance` row carries `actor_id="omnigraph:recovery"` (the system actor). To find recoveries for a specific original actor: `omnigraph commit list --filter actor=omnigraph:recovery`, then join to `_graph_commit_recoveries.lance` by `graph_commit_id` to read `recovery_for_actor`. Schema: see `crates/omnigraph/src/db/recovery_audit.rs`.
