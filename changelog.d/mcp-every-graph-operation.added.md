- `/mcp` now has a tool for every graph operation that is not a byte stream.
  New read tools `diff`, `branches_diff`, `branches_merge_base` and
  `blobs_stat` call the same handlers and policy actions as their HTTP routes,
  and `commits_list` takes `listing` to list unpublished proposals and
  unattached snapshots. New write tools `commits_publish`, `commits_drop` and
  `blobs_clear` are listed only with `--mcp-allow-writes`. With writes
  enabled, `mutate` with `name` and no `query` runs a stored mutation through
  the stored-query route, and through its conditional route when
  `if_graph_commit` is given. `queries` now returns the route's whole catalog,
  stored mutations included. Blob payloads, `export` and the change-feed
  baseline stay HTTP and CLI only, since MCP tools do not carry byte streams.
  See [MCP tools][mcp-every-operation-tools].

[mcp-every-operation-tools]: ../docs/user/operations/mcp.md#tools
