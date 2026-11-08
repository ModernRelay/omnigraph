- An MCP write tool now waits 30 seconds for its result instead of 60, below the
  MCP TypeScript SDK's default 60 second request timeout, so a client on that
  default receives the `outcome_unknown` answer instead of timing out first. The
  write itself still runs to completion. The server instructions now say that a
  missing response is also an unknown outcome and how to reconcile it, and that
  `graphs` needs a `graph_list` permit for a credential that is not an identity.
  See [MCP][mcp-write-wait-guide].

[mcp-write-wait-guide]: ../docs/user/operations/mcp.md
