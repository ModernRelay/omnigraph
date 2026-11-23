# One client for embedded and served access

**Decides:** CLI graph commands run through one `GraphClient` with an embedded and a remote variant, over wire types shared with the server in `omnigraph-api-types`, and an embedded-versus-remote parity matrix referees the two.
**Contract:** keeps E6
**Code:** `crates/omnigraph-cli/src/client.rs`, `crates/omnigraph-api-types`, `crates/omnigraph-server/src/api.rs`, `crates/omnigraph-cli/tests/integration/parity_matrix.rs`
**Cited as:** RFC 0009

## Context

Graph semantics cannot differ between the two paths: the server handlers and
the CLI's embedded arm call the same engine `_as` entry points, and policy is
enforced inside them. What can differ is everything around those calls. With a
fork per command on whether the address is remote, the result of one
operation is mapped three times: engine result to HTTP response in the
server, HTTP response to CLI output, and engine result to CLI output. Three
mappings agree only by discipline, and nothing compares the two paths.

## Decision

- The wire request and response types, and the one engine-result-to-wire
  mapping per operation, live in `omnigraph-api-types`. The server re-exports
  them (`omnigraph-server::api`) and derives `openapi.json` from them; the CLI
  builds them on the embedded path and deserializes the same types on the
  remote path.
- `omnigraph-api-types` holds plain serde and utoipa DTOs and depends on
  neither axum nor the server; transport stays at the boundary.
- Each command body is written once against `GraphClient`. The variants differ
  at construction: `Embedded` carries a self-declared actor and a captured
  storage binding; `Remote` carries a bearer token and the server resolves the
  actor.
- A third variant, `Catalogue`, serves the store-free `og_catalog` scope and
  refuses every verb but reads and `schema show`.
- The parity matrix runs each forked verb against a local graph and a spawned
  server on a twin copy with the same policy and actor, scrubs a declared
  volatile allowlist, and requires identical JSON and exit codes. A known
  divergence is listed in the test, never repaired silently.
- `--json` output is observable contract; a change to it is a release-noted
  decision, not a refactor side effect.

## Rejected

- **Route embedded use through a localhost server:** buys parity the shared
  types and the matrix already give, and breaks the embedded, CLI and test
  story.
- **Generate a Rust client from the OpenAPI spec:** round-trips Rust types
  through a lossy intermediate when the types can be shared at compile time;
  codegen stays for foreign-language clients.
- **Unify the trust model:** the self-declared direct actor and the
  token-resolved served actor are different authorities on purpose.
- **Keep per-command forks with more tests:** leaves N mappings of one
  contract, the bug class the shared crate removes.
