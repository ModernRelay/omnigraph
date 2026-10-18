- The release archives now include the server's AWS Secrets Manager
  bearer-token source (`OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET`), as the
  container image already did, so a server from an archive, the stable image
  and the edge image accept the same token configuration. See
  [HTTP server][release-archives-aws-token-source].

[release-archives-aws-token-source]: ../docs/user/operations/server.md
