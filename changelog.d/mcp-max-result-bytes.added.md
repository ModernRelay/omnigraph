- `--mcp-max-result-bytes` lowers the bound on successful MCP reads below 1 MiB, so a broad read gets `result_too_large` before a client truncates it. Refusals and writes keep 1 MiB. See [MCP settings][mcp-result-bound].

[mcp-result-bound]: ../docs/user/operations/mcp.md#settings
