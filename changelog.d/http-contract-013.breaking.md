- The CLI and server now require exactly `Omnigraph-Http-Api: 0.13`; `0.12`, missing, repeated and combined values are refused before graph access or body decoding. Upgrade the CLI, server and HTTP integrations together, including the current routes, request/response shapes and credentials; changing the header alone does not migrate an old integration. See the [HTTP contract][http-contract-013].

[http-contract-013]: ../docs/user/operations/server.md#http-contract
