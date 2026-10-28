- The server keeps the OAuth client of a verified OIDC token beside the
  person's actor: `GET /graphs/{id}/access` answers it as `client_id`, and the
  policy decision log records it. See [sign-in][oidc-client].

[oidc-client]: ../docs/user/operations/mcp.md#sign-in-and-discovery
