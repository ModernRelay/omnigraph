- The CLI and server now require exactly `Omnigraph-Http-Api: 0.14`; `0.13`,
  missing, repeated and combined values are refused before graph access or body
  decoding, and the CLI refuses a server that answers discovery with `0.13`
  before sending. Contract `0.14` adds the `Omnigraph-If-Actor` request
  header. Upgrade the CLI, server, Studio and HTTP integrations together. See
  the [HTTP contract][http-contract-014].

[http-contract-014]: ../docs/user/operations/server.md#http-contract
