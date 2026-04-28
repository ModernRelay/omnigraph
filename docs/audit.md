# Audit / Actor tracking

- `Omnigraph::audit_actor_id: Option<String>` is the actor in effect.
- `_as` variants of every write API let callers override the actor: `begin_run_as`, `publish_run_as`, `ingest_as`, `mutate_as`, `branch_merge_as`, etc.
- Actor IDs are persisted both on `RunRecord.actor_id` and on `GraphCommit.actor_id`, with optional split storage in `_graph_commit_actors.lance` and `_graph_run_actors.lance`.
- HTTP server uses the bearer-token actor automatically; CLI uses the local user / explicit env (no implicit actor).
