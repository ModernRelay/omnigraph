- `--unauthenticated` now refuses to start on a non-loopback `--bind`, such as
  the container's `0.0.0.0:8080`, because an open server permits every graph
  action to anyone who reaches it. Bind a loopback address, configure tokens, or
  set `OMNIGRAPH_UNAUTHENTICATED_ALLOW_REMOTE=1`. See
  [authentication][unauthenticated-loopback].

[unauthenticated-loopback]: ../docs/user/operations/server.md#authentication
