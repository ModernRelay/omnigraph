- A recorded operation whose cluster state or operation record cannot be read
  answers 503 `service_unavailable` with `effects: none`, not 409 `conflict`,
  so a caller retries it. See [recorded operations][recorded-operations-guide].

[recorded-operations-guide]: ../docs/user/operations/recorded-operations.md
