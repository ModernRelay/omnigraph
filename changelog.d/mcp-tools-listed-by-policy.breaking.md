- `/mcp` now lists every tool, including `read` and the write tools, under the
  same policy actions as their HTTP routes. `--mcp-allow-adhoc`,
  `--mcp-allow-writes` and their environment variables are removed, and a
  server given either flag refuses to start: remove it, and narrow an agent's
  policy to keep it read-only. See [MCP][mcp-tools-by-policy-guide].

[mcp-tools-by-policy-guide]: ../docs/user/operations/mcp.md
