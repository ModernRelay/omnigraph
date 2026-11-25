- `omnigraph graphs list` lists only the graphs your credential can read on any branch, and a graph on which it holds no permission now answers 404 on every route, ready or not, instead of 403. Pass `--catalog` for every graph with its state and root, which still needs `graph_list`. See [policy][graphs-list-readable-policy].

[graphs-list-readable-policy]: ../docs/user/operations/policy.md#actions
