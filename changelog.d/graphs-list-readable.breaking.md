- `omnigraph graphs list` now lists the graphs your credential can read, with any credential, and `--discovery` is removed. Pass `--catalog` for every graph with its state and root, which still needs `graph_list`. An identity credential no longer sees graphs it cannot read. See [managed data access][graphs-list-readable-guide].

[graphs-list-readable-guide]: ../docs/user/cli/managed-data.md#discover-graphs
