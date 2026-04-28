# Runs (transactional graph mutations)

`db/run_registry.rs` + run lifecycle in `db/omnigraph.rs`. Stored in `_graph_runs.lance` and `_graph_run_actors.lance`.

## RunRecord

```
RunRecord {
  run_id: RunId (ULID),
  target_branch: String,           // where the run will publish
  run_branch: "__run__<id>",       // ephemeral isolation branch
  base_snapshot_id: String,
  base_manifest_version: u64,
  operation_hash: Option<String>,  // idempotency key
  actor_id: Option<String>,
  status: Running | Published | Failed | Aborted,
  published_snapshot_id: Option<String>,
  created_at, updated_at: i64 (microseconds),
}
```

## Lifecycle

1. `begin_run(target_branch, op_hash)` / `begin_run_as(target_branch, op_hash, actor_id)` — forks `__run__<id>` from the target's current head, appends a `RunRecord`.
2. Mutations on `run_branch` (via the normal write APIs) — isolated from concurrent activity on the target.
3. `publish_run(id)` / `publish_run_as(id, actor)`:
   - **Fast path**: if the target hasn't moved since `base_snapshot_id`, promote the run snapshot directly.
   - **Merge path**: if it has moved, perform a three-way merge (see [merge.md](merge.md)) into the target.
   - On success: `status = Published`, `published_snapshot_id` set, run branch cleaned up asynchronously.
4. `abort_run(id)` / `fail_run(id)` — terminal; cleans up run branch best-effort.

## Idempotency

`operation_hash` is an optional field clients can use to detect a duplicate `begin_run` retry.

## Cleanup

`cleanup_terminal_run_branches_for_target(branch)` is called as branches change; failures are swallowed (lazy cleanup on next branch op).
