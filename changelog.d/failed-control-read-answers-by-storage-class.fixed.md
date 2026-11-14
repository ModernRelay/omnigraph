- A failed storage read on the served deployment routes (`cluster apply` or `plan` against a server, deployment status) answers 503 to retry, or 500 if it repeats, not 409. See [recorded operations][recorded-operations-guide].

[recorded-operations-guide]: ../docs/user/operations/recorded-operations.md
