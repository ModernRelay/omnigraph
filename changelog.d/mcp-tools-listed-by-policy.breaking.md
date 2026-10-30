- `/mcp` now lists every tool, including `read` and the write tools, under their
  HTTP routes' policy actions. `--mcp-allow-adhoc` and `--mcp-allow-writes` now
  stop the server from starting, and `OMNIGRAPH_MCP_ALLOW_*` variables are
  ignored: narrow an agent's policy to keep it read-only. See
  [MCP][mcp-tools-by-policy-guide].

[mcp-tools-by-policy-guide]: ../docs/user/operations/mcp.md
