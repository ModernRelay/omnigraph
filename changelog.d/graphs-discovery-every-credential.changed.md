- `GET /graphs/discovery` and MCP `graphs` answer every credential with the graphs it can read, so a static token or OAuth client needs no `graph_list` and gets an empty list, not 403. See [policy][graphs-discovery-policy].

[graphs-discovery-policy]: ../docs/user/operations/policy.md#actions
