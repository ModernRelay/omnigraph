- The server keeps the `client_id` of a verified OIDC access token beside the
  person's actor. `GET /graphs/{id}/access` answers it as `client_id`, and the
  `policy decision` log line records it, so two OAuth clients acting for one
  person can be told apart. The actor stays `principal:<id>` for policy,
  `Omnigraph-If-Actor` and admission budgets. Static tokens and signed data
  credentials name no client. See [sign-in and discovery][oidc-client].

[oidc-client]: ../docs/user/operations/mcp.md#sign-in-and-discovery
