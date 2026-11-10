- A graph without an applied policy now refuses `read` to bearer tokens, as it
  refuses every other action; tokens are not scoped to graphs, so the old
  permit reached every such graph on the server. Apply a policy to each graph
  a token should read, or set `OMNIGRAPH_ALLOW_READS_WITHOUT_POLICY=1` on the
  server to keep the old behaviour. See [authentication][no-policy-reads].

[no-policy-reads]: ../docs/user/operations/server.md#authentication
