- `GET /graphs/discovery` and MCP `graphs` answer every credential with the graphs it can read on any branch, so a static token or OAuth client needs no `graph_list`; none gets 403. See [policy][graphs-discovery-policy].

[graphs-discovery-policy]: ../docs/user/operations/policy.md#actions
