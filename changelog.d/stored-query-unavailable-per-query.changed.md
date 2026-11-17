- A stored query that fails to compile, after a binary upgrade for instance, no longer blocks its graph: the graph serves, and that query alone is unavailable. `queries list` (CLI, HTTP, MCP) and `GET /graphs` report its diagnostic, `GET /readyz` counts it, and invoking it answers HTTP 409 `stored_query_unavailable` with the same diagnostic. `OMNIGRAPH_STRICT_STORED_QUERIES=1` keeps refusing the whole graph. See [stored queries at upgrade][stored-query-unavailable-guide].

[stored-query-unavailable-guide]: ../docs/user/queries/upgrading-to-gq3.md#stored-queries-at-upgrade
