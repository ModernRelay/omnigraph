- The server logs Lance's `lance::dataset_events` and `lance::file_audit`
  targets only at warning and error, whatever `RUST_LOG` says, as it already
  does for `rmcp`. At `info` they logged every table's storage URI, every file
  Lance created or deleted, and each delete predicate with its entity IDs.
  Embedders using their own subscriber should apply the same filter. See
  [server logging][server-restricted-log-targets].

[server-restricted-log-targets]: ../docs/user/operations/server.md#logs
