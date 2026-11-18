- Static bearer-token actors may no longer start with `principal:`, the prefix of signed and OIDC identities, so a server whose token configuration names one refuses to start: rename that actor and its policy rules. The AWS Secrets Manager source now also refuses duplicate token values. See [tokens][reserved-actor-prefix].

[reserved-actor-prefix]: ../docs/user/operations/server.md#authentication
