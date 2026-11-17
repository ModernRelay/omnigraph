- A set static token variable now requires bearer authentication whatever it holds. A source with no tokens, such as a `{}` file, starts in default-deny mode (policy mode with a policy) instead of being refused, and with `--unauthenticated` it no longer runs open. To run an open server, unset the variable. See [authentication][configured-token-source].

[configured-token-source]: ../docs/user/operations/bearer-tokens.md#sources
