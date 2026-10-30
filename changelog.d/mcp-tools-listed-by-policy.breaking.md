- `/mcp` lists every tool under its HTTP route's policy action, and `query` now
  takes GQ source or a stored read's `name`, replacing the ad-hoc `read` tool.
  `--mcp-allow-adhoc` and `--mcp-allow-writes` stop the server from starting;
  `OMNIGRAPH_MCP_ALLOW_*` variables are ignored. Narrow an agent's policy to
  keep it read-only. See [MCP][mcp-tools-by-policy-guide].

[mcp-tools-by-policy-guide]: ../docs/user/operations/mcp.md
