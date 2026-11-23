# HTTP contract admission

**Decides:** the CLI and the server speak exactly one HTTP contract, named by an `Omnigraph-Http-Api` header that every protected request must carry once and every response returns, and a mismatch is refused before any graph is touched.
**Contract:** keeps E4, E6
**Code:** `crates/omnigraph-api-types/src/lib.rs`, `crates/omnigraph-server/src/http_contract.rs`, `crates/omnigraph-cli/src/graph_http.rs`

## Context

Clients and servers from different builds disagree about request and response shapes. A package version does not say which wire shapes a server speaks, and a client that decodes a body it does not understand can report success for a write whose outcome it cannot know. Negotiating between versions would add adapters and aliases that every route then carries.

## Decision

- The header value is the single constant `HTTP_API_CONTRACT` in `omnigraph-api-types`, independent of the package version and the storage stamp. An incompatible change to a released contract takes another value.
- Protected graph and registry routes authenticate first, then require exactly one header with exactly that value, before graph resolution, body decoding or execution. A missing, duplicate, combined or other value is 400 `api_contract_mismatch` in the ordinary `ErrorOutput`, without echoing the sent value.
- Every ordinary response carries the header, refusals and streams included. `/healthz`, `/readyz` and `/openapi.json` do not require it. MCP and OAuth metadata use their own standard protocols and are outside it.
- Before each graph or registry request the CLI sends an unauthenticated `HEAD /healthz` with a five second bound and requires the exact header. A failed probe sends no data request and says so.
- The CLI checks the response header before decoding any body. A missing or wrong header after dispatch is a protocol failure with effects unknown, never a proof of non-execution.
- Graph HTTP clients follow no redirects and retry nothing automatically.
- A configured server URL is the service root, including any proxy prefix. The CLI never guesses a root by stripping a `/graphs` segment.
- The header is a protocol discriminator, never authentication or permission.

## Rejected

- **Infer the contract from the package version:** a version does not identify the wire shapes a server implements.
- **Fall back for old clients, or negotiate between versions:** every route would carry adapters and aliases; one contract, upgraded together, needs none.
- **Cache a server's capability across requests:** the backend or a proxy can change between requests; the server's own check is the fence.
