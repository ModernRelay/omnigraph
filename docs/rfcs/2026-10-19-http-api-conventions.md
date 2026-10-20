---
rfc: "2026-10-19-http-api-conventions"
title: "HTTP API conventions"
track: maintainer
status: draft
implementation: not-started
authors:
  - Ragnor Comerford
created: 2026-10-19
updated: 2026-10-19
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: HTTP API conventions

## Summary

The HTTP API gets one written style, enforced by a test over the generated
`openapi.json`. The style covers paths and parameters, methods and statuses,
the error body, headers, schemas, pagination and the spec itself. Operations
that break it are renamed or reshaped. Each incompatible change ships in a
batch with one HTTP contract bump, never by itself. This RFC changes no
operation's meaning, authorization or storage effect.

## Motivation

The spec grew one route at a time, and each route made its own local choices.
An audit of the spec at contract 0.14 found the following:

- **operationIds** use four styles: 22 are `cluster_` plus camelCase, 4 are
  `cluster_` plus snake_case, 5 are snake_case, 4 are camelCase and 2 are a
  single word. The `cluster_` prefix is left over from routes that no longer
  exist.
- **Path parameters** are `{graph_id}`, `{commit_id}`, `{branch}` and `{name}`,
  except `/cluster/deployments/{id}`.
- **A branch named `merge`, `diff` or `merge-base`** collides with the static
  routes `/branches/merge`, `/branches/diff` and `/branches/merge-base`: its
  delete reaches the static route and answers 405. That defect is fixed on its
  own, outside this RFC. The convention below keeps it from recurring.
- **Statuses** are inconsistent:
  - deletes answer 200 or 204;
  - creates answer 200, never 201;
  - a `limit` above its ceiling answers 413, which is meant for bodies;
  - some reads hide a policy denial as 404 and others answer 403, under no
    stated rule;
  - 401 carries no `WWW-Authenticate`.
- **Responses in the spec**: 500 is documented on 11 of 36 operations, and the
  `/cluster/*` routes document no 401.
- **Schemas**:
  - 62 names end in `*Output`, 11 in `*Request` and 5 in `*Response`;
  - 7 enum names end in `Output` and 13 don't;
  - `BlobEntityKind` duplicates `EntityKindOutput`;
  - `ChangeErrorOutput` is a second error body;
  - `AccessOutput.actor` sits beside `actor_id` in 9 schemas.
- **Pagination**:
  - the parameter names `page_token`, `limit` and `next_page_token` agree;
  - commits bound `limit` to 1–1000, while diffs and changes state no ceiling;
  - `/commits` without a limit lists the whole history;
  - two mutually exclusive booleans (`unpublished`, `unattached`) stand in for
    one enum;
  - branch, graph and query lists are not paginated.
- **Coverage**:
  - `/openapi.json`, `/mcp` and the catalogue routes are served but not in the
    spec;
  - there is no `tags` array;
  - six parameters have no description.
- **MCP tools** use other names for the same operations:
  - MCP `query` runs a stored read, while HTTP `POST /query` runs an inline one;
  - MCP `read` runs an inline query;
  - tool arguments are `graph` and `name` where HTTP uses `graph_id` and
    `branch`.

Each choice was cheap when it was made. Five more routes without a written
rule would add five more styles, and every client, SDK and the MCP surface
would keep paying for each one. One written rule with a test is the smaller
long-term cost.

## User and operational behavior

Clients see the conventions below. Nothing changes until a batch ships, and
each batch is announced with its contract version in a breaking release note.

### Paths and parameters

- **Resource paths** are plural nouns under `/graphs/{graph_id}`. An action
  that is not CRUD on a resource is a sub-resource noun (`/merges`, `/diffs`),
  never a verb, and never a static segment beside a `{param}` in the same
  collection.
- **Path parameters** name their resource: `{graph_id}`, `{branch_name}`,
  `{commit_id}`, `{query_name}`, `{deployment_id}`.
- **Query parameters** are snake_case. Booleans are bare adjectives. A choice
  among values is a string enum, not mutually exclusive booleans.

### Methods and statuses

- **Methods**: GET reads and never takes a body. A read whose input does not
  fit a URL is `POST` on a `…/queries`-style resource and is documented as
  safe. PUT replaces, DELETE removes, POST creates or runs.
- **Success statuses**: 200 returns a body, 201 creates a resource and names it
  in the body, 204 returns nothing.
- **Error statuses**:
  - 400: any invalid input, including an out-of-range `limit`;
  - 401: carries `WWW-Authenticate` (RFC #1017 appendix A1);
  - 403: a denial;
  - 404: a missing resource, and a denial only under one written
    existence-hiding rule, stated per resource;
  - 409: a state conflict;
  - 412: only for a failed `If-*` precondition;
  - 413: only for a body;
  - 429: per actor, with `Retry-After`;
  - 503: unavailable for the scope the code names, the server, a graph or one
    branch with a pending publication, with `Retry-After` when retrying is safe.

### Error body and headers

- **The error body** is one `ErrorOutput` with `error`, `code` and
  `disposition` required. Each code has one meaning, a detail field that
  carries the code's own name, and documentation of the statuses it is sent
  under. `ChangeErrorOutput` folds into it.
- **Headers** are written in Title-Case as `Omnigraph-*`. A request
  precondition is `Omnigraph-If-*` and answers 412. Standard headers are used
  where they apply: `If-Match`/`ETag`, `Retry-After`, `Cache-Control`,
  `Content-Type`.

### Schemas and pagination

- **Schema names**: request bodies are `*Request` and responses `*Output`.
  Enums carry no suffix. One schema exists per concept.
- **Field names**: identifiers are `*_id`, the actor is `actor_id`, times are
  `*_at` in int64 epoch microseconds, and an absent optional field is omitted
  rather than `null`.
- **Lists** are `{<plural>: [...], next_page_token}`. `limit` has a documented
  default and maximum, every list is bounded, and a list without a page token
  returns at most the default.

### The spec

- **Coverage**: every served route is in the spec, `/mcp` included as a
  described operation.
- **operationIds** are lowerCamel `verbNoun` with no prefix, and every
  operation has a tag from a declared `tags` array.
- **Shared responses** (400, 401, 404, 405, 412, 429, 500, 503) are injected
  into every protected operation from one place, as the contract header is.
- **MCP tools** take the operation names and argument names of the HTTP
  operation they wrap, as RFC #1017's one operation table requires.

## Design

**One source.** The conventions live in `docs/dev/http-api.md`, a developer
guide this RFC adds. `openapi::spec_follows_the_api_conventions`, a new
server test beside the OpenAPI drift test, checks the generated spec against
them:

- operationId style and uniqueness;
- path parameter names, and no static segment beside a `{param}`;
- Title-Case header names, and that `If-*` alone answers 412;
- the required error-body fields;
- schema name suffixes;
- every list operation's `limit` bounds;
- a description on every parameter;
- the shared responses on every protected operation;
- a tag on every operation.

An exception is a named, reviewed row in the test, as `forbidden_apis` pins
its exceptions, so a new deviation fails CI instead of joining the spec.

**Batches.** A batch is the set of renames and reshapes that ship under one
HTTP contract bump. The server accepts exactly one contract, so a batch moves
clients and servers together, as 0.13 and 0.14 did. Additive changes, such as
new optional fields, newly documented responses and descriptions, need no
bump and land whenever the lint does.

## Invariants

- Invariant 9: query semantics stay typed. The conventions rename transport
  shapes, never query semantics.
- Invariant 10: trust stays at the boundary. Only `Omnigraph-If-*` headers can
  narrow a request, and no header supplies identity.
- Invariant 11: failures are bounded and observable. The error body and
  `Retry-After` make the outcome explicit on every refusal.
- Invariant 12: one source of truth. The conventions guide is the source, the
  lint derives from it, and the MCP names derive from the operation table.

No deny-list item is involved.

## Compatibility and reversibility

The lint and additive documentation are free to land and to revert. Each
breaking batch is one contract bump with a breaking release note listing
every rename. Only `openapi.json` and the HTTP routes change: no storage,
query or authorization contract. Old route spellings are not kept as aliases,
because the server accepts one contract. A reverted batch is another bump.

## Alternatives

- **Do nothing.** Every new route keeps adding a style, the generated SDKs
  keep exposing four operationId conventions, and the MCP and HTTP names keep
  drifting.
- **Fix each inconsistency when its route is touched.** This spreads
  breaking renames over many bumps, so clients migrate many times.
- **Adopt an external linter (Spectral) with a public ruleset.** A public
  ruleset covers generic rules, but not the ones specific to OmniGraph, such
  as `If-*` alone answering 412, the shared-response set or the error-body
  fields. A Rust test over the generated spec keeps the check beside the
  existing OpenAPI drift test and needs no new toolchain. Spectral can still
  run in CI later if the generic rules are wanted.

## Evidence and tests

- `crates/omnigraph-server/tests/integration/openapi.rs` owns the new lint,
  beside `openapi_spec_is_up_to_date`.
- Each batch updates the existing route owners: `data_routes`, `auth_policy`,
  `schema_routes`, `stored_queries`, `multi_graph`, `boot_settings` and `mcp`,
  plus the CLI's `GraphHttpClient` and managed fixtures.
- The branch-name collision gets its own regression in `data_routes` with its
  fix.

## Rollout

1. **The lint and the guide.** Exceptions are pinned as they stand today.
   This is additive and can land now.
2. **The 0.14 batch, with the RFC #1017 stack.** The stack already bumps the
   contract. It brings the error body in line (`disposition`, required fields,
   one code per meaning, documented statuses), plus `Omnigraph-If-Actor`,
   Title-Case headers and the shared responses.
3. **The next contract batch**:
   - operationIds without the prefix;
   - path parameter names;
   - 201 and 204;
   - `limit` bounds, and 400 above them;
   - `*Response` renamed to `*Output`;
   - enum suffixes;
   - the `ChangeErrorOutput` fold;
   - `actor` renamed to `actor_id`;
   - pagination on branch, graph and query lists;
   - `WWW-Authenticate` on 401.
4. **Route reshapes**, each listed in the batch it ships with:
   - `/mutate/if-graph-commit` and `/queries/{name}/if-graph-commit` fold into
     an optional `Omnigraph-If-Graph-Commit` header on the main routes;
   - `/load` and `/load/ndjson` become one operation selected by
     `Content-Type`;
   - `/diff` and `/branches/diff` merge;
   - the static `/branches/*` actions move off the branch collection;
   - MCP tool names follow the operation table.

`implementation` moves to `in-progress` with step 1 and to `complete` when the
lint carries no exception rows.

## Unresolved questions

- Should the existence-hiding rule be "hide on every read of a resource the
  caller cannot read", or only on commits and stored queries as today?
- Should the static branch actions move to `/graphs/{graph_id}/merges` and
  similar top-level resources, or under `/graphs/{graph_id}/branches/{branch_name}/…`?

## Decision log

- 2026-10-19: Drafted from an audit of the spec at contract 0.14. Ragnor
  chose to fix the branch-name collision now, on its own, and to carry the
  remaining conventions and migrations in this RFC.
