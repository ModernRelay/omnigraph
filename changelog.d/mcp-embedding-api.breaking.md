- Rust hosts of `omnigraph-mcp`: `Host::start_tool` returns `Encoded` (`Encoded::new(&dto)` replaces `bounded_result`), `HostError::from_output` no longer fails, `router` takes a docs revision and `McpSettings::resolve` a setup URL (pass `None` for either), and `ProtocolError` adds `ReadOverBudget`, which an exhaustive match must handle. See [embedding APIs][mcp-embedding].

[mcp-embedding]: ../docs/dev/control-plane.md#public-embedding-apis
