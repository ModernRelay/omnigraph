- `cluster token --ttl` help and docs now say it requests a lifetime: the managed service may issue a shorter credential, and the result's `expires_at` reports the one issued. See [managed data][cluster-token-ttl-guide].

[cluster-token-ttl-guide]: ../docs/user/cli/managed-data.md
