# HTTP API conventions

**Decides:** the HTTP API follows one written style, a table in [the HTTP API guide](../dev/http-api.md) that a test checks against the generated `openapi.json`, and every deviation is a named row, so an unlisted one fails CI.
**Contract:** keeps E4, E6, write:3
**Code:** `crates/omnigraph-server/tests/integration/openapi/conventions.rs`, `crates/omnigraph-api-types/src/operations.rs`, `crates/omnigraph-server/src/http_contract.rs`, `crates/omnigraph-server/src/handlers.rs`

## Context

The spec grew one route at a time, and each route chose its own operationId form, parameter names, schema suffixes and list bounds. Every client, generated SDK and the MCP surface pays for each extra form. A style guide with no check drifts; a style rule that copies a fact the code already owns, such as statuses, drifts from it.

## Decision

- The conventions are one table that `spec_follows_the_api_conventions` reads. No rule has a second copy in Rust, and an unused exception fails the check.
- Each graph operation is named once in the operation inventory in `omnigraph-api-types`; its operationId is that name in lowerCamelCase with no prefix. Flat classes outside `/graphs/{graph_id}` (graph management, deployment control, probes, protected-resource metadata) keep pinned ids.
- Graph paths sit under `/graphs/{graph_id}`: a collection is a plural noun, one resource a singular noun, an action its command word. Path parameters name their resource (`branch_name`, `commit_id`, `query_name`, `deployment_id`). Query parameters are snake_case, and a choice among values is one enum, not exclusive booleans: commit `listing` is `history`, `unpublished` or `unattached`, and the retired boolean keys are refused.
- Branch actions share `/branches/{branch_name}` through one `BranchAction` dispatch, so any branch name, `merge` included, can be deleted, and other methods answer 405 with `Allow`.
- GET reads and takes no body; a read whose input does not fit a URL is a POST. Creates and runs answer 200 with their receipt; commit drop answers 204.
- `ErrorCode` owns every code, its statuses and its default action, and `describe_contract` adds the router's responses. The returned `disposition.action` alone authorizes a retry. An uncertain write is 500 `outcome_unknown` with `action: reconcile`. A `limit` above its ceiling is 413.
- Omnigraph headers are `Omnigraph-*` in Title-Case; an Omnigraph precondition is `Omnigraph-If-*` and fails with 412. The graph-commit precondition keeps two fail-closed routes (`…/mutate/if-graph-commit`, `…/queries/{query_name}/if-graph-commit`); the unconditional routes refuse the header with 400.
- Servers send one `ErrorOutput`. The change routes publish `ChangeErrorOutput`, a projection whose allowlist turns any other engine error into a fixed 500, so no storage identifier reaches the wire.
- Requests are `*Request`, responses `*Output`, enums carry no suffix. Identifiers are `*_id`, the actor is `actor_id`, times are `*_at` in epoch microseconds.
- Every `limit` states its default and ceiling in the schema. A list is paged from its first release; bounding the existing unpaged lists is a separate design.
- The spec covers every graph, management and deployment route, each tagged from a declared `tags` array. `/openapi.json` and `/mcp` are documented elsewhere; `og_catalog` is a reserved graph id documented on `graph_id`.

## Rejected

- **Fix each inconsistency when its route is touched:** spreads breaking renames over many contract values, so clients migrate many times.
- **An external linter such as Spectral:** a public ruleset cannot check the inventory rendering, the registry-derived statuses or the pinned classes.
- **A status table in the guide:** a second list beside `ErrorCode` that drifts from what the server sends.
- **One mutate route with an optional graph-commit header:** a client that drops the header runs an unconditional write.
- **Move branch actions off `/branches/{branch_name}`:** every client would migrate for path shape alone.
- **201 for creates and 400 for a `limit` over its ceiling:** both change what callers observe for no gain.

