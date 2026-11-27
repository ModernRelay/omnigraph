- `omnigraph graphs list` lists only the graphs your credential holds any permission on, and every other graph answers `graph not found` (404) on every route instead of 403; `og_catalog` keeps its own authorization. `--catalog` lists every graph with `graph_list`. Upgrade the CLI with the server: an older CLI cannot read the new listing; this one reports an older server's listing as `unfiltered`. See [policy][graphs-list-readable-policy].

[graphs-list-readable-policy]: ../docs/user/operations/policy.md#actions
