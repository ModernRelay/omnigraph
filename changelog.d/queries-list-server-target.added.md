- `omnigraph queries list` takes a server target and lists one served graph's
  stored queries, selected with `--graph` or the target's `default_graph`. It
  reads `GET /graphs/{id}/queries`, and `--json` prints that response body:
  each parameter carries its catalog `kind` rather than the GQ `type` the
  direct cluster-root form prints. The cluster-root form is unchanged, and
  `queries validate` still requires a cluster root. See the
  [CLI reference][queries-list-server-target].

[queries-list-server-target]: ../docs/user/cli/reference.md#commands
