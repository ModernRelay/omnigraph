- `--mcp-max-result-bytes` lowers the MCP tool-result bound below 1 MiB, so a broad read is refused with `result_too_large`, naming the limit, before a client truncates it. See [MCP settings][mcp-result-bound].

[mcp-result-bound]: ../docs/user/operations/mcp.md#settings
