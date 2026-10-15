- `/mcp` is now mounted whenever the server has a bearer credential source
  (static tokens, signed data tokens or OIDC), so a self-hosted server reaches
  Claude Code, Cursor and other MCP clients with its existing token; OAuth
  discovery stays OIDC-only, `--mcp-allowed-host` admits extra `Host` names, and
  without OIDC any request carrying an `Origin` header is refused.
  New read tools `health`, `snapshot`, `branches_list`, `commits_list`,
  `commits_get`, `commits_changes` and `changes_poll`, and resources for the
  graph list and each graph's schema and branches, call the same handlers and
  policy actions as their HTTP routes. Write tools `mutate`, `load`,
  `branches_create`, `branches_delete` and `branches_merge` are listed only with
  `--mcp-allow-writes`; they never retry, honour `if_graph_commit`, and report
  `outcome_unknown` instead of guessing. See [MCP][mcp-every-credential-guide].

[mcp-every-credential-guide]: ../docs/user/operations/mcp.md
