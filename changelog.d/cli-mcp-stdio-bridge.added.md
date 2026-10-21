- `omnigraph mcp --target NAME` connects an agent that starts its MCP servers as
  child processes, such as Claude Code or Codex in a terminal, to an existing
  server target's `/mcp` over stdio. It sends the target's stored token, which
  the agent never sees, is bound to one graph (`--graph` or the target's
  `default_graph`), and lists and admits only read-only tools unless `--write`
  is given; direct and managed targets refuse, and it never starts a server.
  See [connecting a stdio agent][cli-mcp-stdio-agent].

[cli-mcp-stdio-agent]: ../docs/user/operations/mcp.md#connect-a-stdio-agent
