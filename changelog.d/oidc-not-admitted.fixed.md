- A server with an OIDC resource identity no longer answers 401
  `error="invalid_token"` to a valid access token whose subject the admission
  file does not name. That answer told OAuth and MCP clients such as Claude to
  sign in again, which issues a token for the same subject, so they looped or
  reported a failed sign-in. The server now answers 403 with the new error
  code `not_admitted` and no challenge. A valid token while the admission
  snapshot is expired gets 503 `service_unavailable` with `Retry-After`,
  instead of the same 401. A forged, expired or misaddressed token still gets
  401 `invalid_token`, and the token's own checks still run first. See
  [MCP][oidc-not-admitted-guide].

[oidc-not-admitted-guide]: ../docs/user/operations/mcp.md
