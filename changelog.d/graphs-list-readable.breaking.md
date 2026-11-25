- `omnigraph graphs list` lists only the graphs your credential holds any permission on, and every other graph now answers 404 on every route, ready or not, instead of 403. Pass `--catalog` for every graph with its state and root, which still needs `graph_list`. A CLI older than this server cannot read the listing. See [policy][graphs-list-readable-policy].

[graphs-list-readable-policy]: ../docs/user/operations/policy.md#actions
