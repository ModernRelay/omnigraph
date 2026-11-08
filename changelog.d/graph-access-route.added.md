- **Ask a graph what your credential may do.** `GET /graphs/{graph_id}/access`
  and the MCP `access` tool return the caller's actor, the actions its
  credential allows on the whole graph, and, for a named branch and target
  branch, the actions allowed there, decided by the installed policy exactly
  as the routes that perform them decide. See
  [Learn your access][graph-access-route-guide].

[graph-access-route-guide]: ../docs/user/operations/policy.md#learn-your-access
