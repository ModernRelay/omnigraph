# Audit / Actor tracking

- `Omnigraph::audit_actor_id: Option<String>` is the actor in effect.
- `_as` variants of every write API let callers override the actor: `mutate_as`, `ingest_as`, `branch_merge_as`, `apply_schema_as`, etc.
- Actor IDs are persisted on `GraphCommit.actor_id` with split storage in `_graph_commit_actors.lance` (the commit graph is split into `_graph_commits.lance` for the linkage and `_graph_commit_actors.lance` for the actor map).
- HTTP server uses the bearer-token actor automatically; CLI uses the local user / explicit env (no implicit actor).
- Pre-v0.4.0 repos also stored actor IDs on `RunRecord.actor_id` in `_graph_runs.lance` / `_graph_run_actors.lance`. The Run state machine was removed in MR-771; those files are inert post-v0.4.0 and reclaimed by MR-770's production sweep.
