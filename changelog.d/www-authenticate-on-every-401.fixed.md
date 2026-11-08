- Every 401 from the server carries a `WWW-Authenticate` challenge, not only a
  401 from `/mcp` on a server with an OIDC resource identity. The challenge is
  `Bearer`, with `error="invalid_token"` when the server refused the bearer
  token sent, and with OIDC it names the protected-resource metadata on every
  route, so a REST client can discover OAuth sign-in. The `Bearer` scheme is
  now case-insensitive: `bearer <token>` authenticates instead of being refused
  as a missing token. See [authentication][www-authenticate-guide].

[www-authenticate-guide]: ../docs/user/operations/server.md#authentication
