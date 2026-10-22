---
rfc: "2026-10-19-http-api-conventions"
title: "HTTP API conventions"
track: maintainer
status: accepted
implementation: complete
authors:
  - Ragnor Comerford
created: 2026-10-19
updated: 2026-10-22
discussion: https://github.com/ModernRelay/omnigraph/pull/1089
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC: HTTP API conventions

## Summary

The HTTP API gets one written style, held by a test over the generated
`openapi.json`. The style covers names, paths and parameters, headers, schemas,
lists and the spec's coverage. It adds no second source beside the owners that
already hold a contract: the `ErrorCode` registry owns codes and statuses, the
operation table of [Graph access and cross-process writes][graph-access] owns
operation names, and the existing code owns branch-action dispatch, the
fail-closed graph-commit routes and the change-route error schema.

Today's deviations are pinned as named exceptions, so a new one fails CI. One
batch of renames removes most of them within HTTP contract 0.14, which is still
in development and has not been released. Merged prerelease code does not
establish a released contract. The renames change spellings only: operationIds,
path parameter names, schema names, one response field and one query parameter.
No route moves, and no status, operation meaning, authorization or storage
effect changes.

## Motivation

The spec grew one route at a time, and each route made its own local choices.
An audit of the prerelease `openapi.json` at contract 0.14, 41 operations on
34 paths, found the following:

- **operationIds** use six forms: 22 are `cluster_` plus camelCase, 4
  `cluster_` plus snake_case, 4 `cluster_` plus one word (`cluster_export`,
  `cluster_load`, `cluster_mutate`, `cluster_query`), 5 snake_case (the four
  deployment ids and `health_head`), 4 camelCase (`listGraphs`,
  `discoverGraphs` and the two OAuth metadata ids) and 2 one word (`health`,
  `readiness`). `nest_paths_under_cluster_prefix` adds `cluster_` to every
  per-graph operation so a nested id cannot collide with a flat one. The
  router no longer serves any flat protected route.
- **Path parameters** are `{graph_id}`, `{commit_id}`, `{branch}` and `{name}`,
  plus `{id}` on `/cluster/deployments/{id}` and `{resource_path}` on the OAuth
  metadata path.
- **Schemas**:
  - of 100 schemas, 65 names end in `Output`, 11 in `Request` and 5 in
    `Response`;
  - of 20 string enums, 6 end in `Output` and 14 carry no suffix;
  - `BlobEntityKind` and `EntityKindOutput` hold the same two values;
  - `AccessOutput.actor` names the actor that 9 other schemas call `actor_id`.
- **Lists**:
  - `page_token`, `limit` and `next_page_token` agree everywhere;
  - only the commit list puts its ceiling on the schema (`maximum: 1000`); the
    diffs and commit changes name a ceiling in prose only, and the change
    feed's `limit` has no description;
  - `GET /commits` without `limit` and `page_token` lists the whole history;
  - two exclusive booleans, `unpublished` and `unattached`, select a listing
    that one enum could;
  - branch, graph and query lists are not paged.
- **Responses in the spec**:
  - 500 is documented on 11 of 41 operations, and no write documents the 500
    `outcome_unknown` it can send;
  - the `/cluster/*` routes document no 401;
  - a graph-route 401 carries no `WWW-Authenticate`, while `/mcp` sends one
    when OIDC is configured.
- **Existence hiding** has no written rule. A graph the caller may not see, a
  stored query it may not invoke, a diff side or a commit's changes on a
  branch it may not read, and a commit it may not branch from, publish or drop
  answer 404 `not_found`. Other denials answer 403 `forbidden`,
  `GET …/commits/{commit_id}` among them.
- **Coverage**:
  - `/openapi.json`, `/mcp` and the catalogue routes under
    `/graphs/og_catalog` are served but not in the spec;
  - every operation has a tag, but the spec declares no top-level `tags` array;
  - six parameters have no description: the change feed's `limit`, `kind`,
    `type` and `op`, and `branch` on the commit list and the snapshot.
- **MCP tools** use a third naming form (`branches_list`, `commits_get`)
  beside the CLI's `branch list`. MCP `query` runs a stored read where
  `POST …/query` runs an inline one, and MCP `read` runs the inline query.
  Tool arguments call the graph `graph` where HTTP says `graph_id`.

Statuses are not on this list. `ApiError::status` sends the status of the code
a response answers, from the `ErrorCode` registry, so the status gaps above
are documentation gaps.

Each choice was cheap when it was made. Five more routes without a written
rule would add five more forms, and every client, SDK and the MCP surface
would keep paying for each one. One written rule with a test is the smaller
long-term cost.

## User and operational behavior

Clients see the conventions below in the finalized unreleased 0.14 contract.
Clients built against earlier development commits
must adopt the renamed fields and selectors; the HTTP contract header remains
0.14. A breaking release note records those changes.

### Names

- **One name per operation.** The [graph-access RFC's one set of
  operations][one-set] puts one table in `omnigraph-api-types` that names each
  graph operation once, with its route, MCP tool and CLI command or a declared
  exception. The operationId of an operation the table binds is the table's
  name in lowerCamelCase, with no prefix: `branch list` becomes `branchList`.
  The inventory test that binds the table's routes, tools and commands also
  checks the operationId. This RFC chooses no operationId ahead of the table,
  and until the table lands the lint pins today's ids.
- **Paths outside `/graphs/{graph_id}`** form classes with their own shapes.
  The lint knows each class as a row:

| Class | Paths | operationIds |
|---|---|---|
| Graph management | `/graphs`, `/graphs/discovery` | `listGraphs`, `discoverGraphs` |
| Deployment control | `/cluster/plan`, `/cluster/deployments`, `/cluster/deployments/{id}` | `deployment_plan`, `deployment_apply`, `deployment_status`, `deployment_lookup` |
| Process probes | `/healthz`, `/readyz` | `health`, `health_head`, `readiness` |
| Protected-resource metadata | `/.well-known/oauth-protected-resource`, `/.well-known/oauth-protected-resource/{resource_path}` | `oauthProtectedResourceMetadata`, `oauthProtectedResourceMetadataAtResourcePath` |
| Not in the spec | `/openapi.json`, `/mcp`, `/graphs/og_catalog/…` | none; see [Coverage](#coverage) |

The first four classes stay flat and get no prefix. The implementation derives
their path classification from `omnigraph-api-types::operations::HTTP_OPERATIONS`
instead of keeping a separate `ALWAYS_FLAT_PATHS` list in `handlers.rs`.
Their operationIds are pinned inventory entries; graph operationIds render
from their canonical names. The protected-resource metadata keeps its ids for
good, because RFC 9728 defines that operation and its path, not OmniGraph.

### Paths and parameters

- **Graph paths** sit under `/graphs/{graph_id}`. A collection is a plural
  noun (`/branches`, `/commits`, `/queries`, `/changes`), one resource is a
  singular noun (`/schema`, `/snapshot`, `/access`, `/blob`), and an action is
  its command word: on the graph (`/query`, `/mutate`, `/load`, `/export`,
  `/diff`) or on one item (`/commits/{commit_id}/publish`).
- **Branch actions share `/branches/{branch}` through one dispatch.** The
  router matches one route for the whole shape, and `BranchAction` in
  `handlers.rs` owns each action's name, method and handler: POST
  `/branches/merge` merges, GET or HEAD `/branches/diff` and
  `/branches/merge-base` read, and DELETE of any name, these three included,
  deletes that branch. Any other method answers 405 with an `Allow` header
  naming what that name serves. A new action on a collection joins such a
  dispatch, so it never shadows an item name. Moving these URLs would be a
  breaking batch of its own and needs a client reason stronger than path
  shape.
- **Path parameters** name their resource: `{graph_id}`, `{branch_name}`,
  `{commit_id}`, `{query_name}`, `{deployment_id}`. `{resource_path}` keeps
  its protocol name.
- **Query parameters** are snake_case. A boolean is a bare adjective
  (`detailed`, as [#1012][pr-1012] proposes). A choice among values is one
  string enum, not exclusive booleans.

### Methods and statuses

- **Methods**: GET reads and takes no body. A read whose input does not fit a
  URL is a POST (`POST …/query`, `POST …/export`). PUT replaces, DELETE
  removes, POST creates or runs.
- **Success**: 200 returns a body and 204 returns none, as the routes do
  today. A create answers 200 with its receipt (`POST …/branches` returns
  `BranchCreateOutput`), and so does a delete that reports what it removed
  (`DELETE …/branches/{branch}` returns `BranchDeleteOutput`). Commit drop has
  no body and answers 204. `mutate`, `load`, `query` and `merge` run work and
  create no REST resource, so they answer 200. This RFC adds no 201. Other
  success and redirect statuses keep their HTTP meaning: 202 when
  `POST /cluster/deployments` durably accepts a deployment, and 206, 302 and
  304 on Blob GET (302 and 304 on HEAD too).
- **Errors**: `ErrorCode` in `omnigraph-api-types` owns every code, its
  statuses and its default action. The returned `disposition.action` is
  authoritative: `ApiError` accounts for the request's effects and recovery
  intent before answering. Clients must not infer permission to retry from
  the code or status alone. `ApiError::status` sends the status of the code a
  response answers; the guide and the lint read statuses from the registry
  and copy none.
  - An error body answers 503 with `service_unavailable`, `graph_unavailable`
    or `recovery_required`. When the returned action is `retry`, the caller
    honors `Retry-After`; `reconcile` or `recover` must be completed instead.
    For example, closed admission after an owned write can answer 503
    `service_unavailable` with `effects: unknown`, `action: reconcile` and no
    `Retry-After`. `recovery_required` retains its code and asks for recovery,
    even with unknown effects. `/readyz` answers 503 as a probe result while
    the replica is not ready.
  - An uncertain write completion that requires reconciliation answers 500
    `outcome_unknown`, with `effects: unknown` and `action: reconcile`; a
    recovery intent keeps `recovery_required` and `action: recover`. The
    caller reads the target before writing again and never blindly replays
    the write, as [Storage outcomes per operation and restartable
    ownership][storage-outcomes] and the [dispositions][dispositions] of the
    graph-access RFC decide. An unknown storage publication alone blocks
    nothing: later writes to its branch proceed.
  - 413 `payload_too_large` covers a body and bounded work: a `limit` above its
    ceiling, and the entity, byte and chain ceilings of mutate, load, merge,
    diffs and changes. It asks the caller to split the work. A zero `limit` is
    400, because the request itself is malformed.
  - 429 `too_many_requests` covers three caps: an actor's, the server's, and
    the MCP endpoint's concurrent tool calls.
  - 412 answers a failed precondition: `actor_mismatch`, or
    `precondition_failed` for a graph commit or a Blob `If-Match`.
  - The spec uses the error statuses 400, 401, 403, 404, 405, 408, 409, 410,
    412, 413, 415, 416, 424, 429, 500 and 503. Draft [#951][pr-951] adds 422
    `view_limit_exceeded`.

### Headers and preconditions

- **Spelling**: OmniGraph headers are `Omnigraph-*`, published in Title-Case
  as `display_header_name` writes them. Standard headers keep their names and
  HTTP meaning: `If-Match`, `If-None-Match`, `Range`, `If-Range` and `ETag` on
  Blob cells, and `Retry-After`, `Cache-Control`, `Allow` and `Content-Type`.
- **Preconditions**: an OmniGraph precondition is an `Omnigraph-If-*` header,
  and a failed one answers 412 with its code. Standard conditional headers keep
  their HTTP statuses: a Blob `If-None-Match` match answers 304, and a failed
  Blob `If-Match` answers 412.
- **The graph-commit precondition keeps its two fail-closed routes.**
  `POST …/mutate/if-graph-commit` and `POST …/queries/{name}/if-graph-commit`
  require `Omnigraph-If-Graph-Commit` and refuse a missing, empty, duplicate or
  entity-tag value with 400. `POST …/mutate` and `POST …/queries/{name}`
  refuse the header with 400 and name the conditional route
  (`reject_graph_commit_expected_head`). A dropped header therefore never runs
  an unconditional write. MCP `mutate` takes the conditional handler when
  `if_graph_commit` is set. This RFC chooses these two operations over one
  route with an optional header. A future precondition whose absence would
  turn a conditional write into an unconditional one takes the same shape.
- **`Omnigraph-If-Actor`** stays optional on every protected route. It only
  checks the actor the credential already resolved: a match changes nothing,
  and a mismatch answers 412 `actor_mismatch` before graph lookup.

### Error bodies

- **One runtime body.** Servers serialize `ErrorOutput`, with `error`, `code`
  and `disposition` on every error response, and typed details beside them.
- **A route family publishes the subset it can set.** The change routes
  (`…/changes`, `…/changes/baseline`, `…/commits/{commit_id}/changes`,
  `…/diff`, `…/branches/diff`, `…/branches/merge-base`) publish
  `ChangeErrorOutput`, a wire-compatible projection of `ErrorOutput`.
  `change_route_error` is an allowlist and turns every other engine error into
  a fixed 500, so a new `OmniError` cannot put a storage identifier on the
  wire. The projection does not advertise `key_conflict`, `merge_conflicts` or
  `read_set_conflict`, which those routes cannot send. This RFC keeps it.

### Schemas and fields

- **Schema names**: a request body is `*Request`, a response is `*Output`, and
  an enum carries no suffix. One schema exists per concept. A published
  projection such as `ChangeErrorOutput` is a named row in the table.
- **Field names**: identifiers are `*_id`, the actor is `actor_id`, and times
  are `*_at` in int64 epoch microseconds, as the commit and write schemas
  already are. A new optional field is omitted when absent rather than `null`.
  Existing fields keep their encoding.

### Lists

- **Shape**: a page carries its items under a plural name beside
  `next_page_token`, with `limit` and `page_token` parameters, as every paged
  list does today.
- **Bounds are documented**: every `limit` states its default and its ceiling,
  in its description and as the schema's `minimum` and `maximum`. A `limit`
  above the ceiling keeps its 413. Today only the commit list does this.
- **A new list is paged from its first release.** Today's unbounded lists stay
  as they are: `GET /commits` without `limit` and `page_token` lists the whole
  history, and the branch, graph and query lists are not paged. Bounding them
  changes what a caller receives, so it is a page-token design of its own and
  not part of this RFC. That design also owns the branch-list `details` that
  #1012 adds, so the branch page is designed once.

### Coverage

- **Every graph, management and deployment route is in the spec**, with a tag
  from a declared top-level `tags` array.
- **Two surfaces are documented elsewhere.** `GET /openapi.json` is not
  self-listed, as the [server guide](../user/operations/server.md) says. `/mcp`
  is the MCP endpoint: MCP's `tools/list` and the
  [MCP guide](../user/operations/mcp.md) describe its tools, which one OpenAPI
  operation cannot.
- **The catalogue is a reserved graph id.** `og_catalog` is the scope every
  binary serves without a store, and the `og_` prefix is reserved for system
  graph ids. Only `POST /graphs/og_catalog/query` and
  `GET /graphs/og_catalog/schema` serve it, with the bodies of `POST …/query`
  and `GET …/schema`, and the query refuses `branch` and `snapshot`. Every
  other path under the prefix answers 405 `method_not_allowed` before any
  effect (`handlers/catalogue.rs`). The spec documents the reserved id and
  these refusals on the `graph_id` parameter, not as a second query operation
  ([catalogue guide](../user/queries/catalogue.md)).

## Design

**One table.** `docs/dev/http-api.md`, a developer guide this RFC adds, holds
the conventions as one table: each rule, the paths and operations it applies
to, and each pinned exception with its reason. `openapi::spec_follows_the_api_conventions`,
a new test in `crates/omnigraph-server/tests/integration/openapi.rs` beside
`openapi_spec_is_up_to_date`, reads that table and checks the generated spec
against it, as `skill_command_index_lists_every_public_command` in
`crates/omnigraph-cli/src/skill.rs` reads the skill's command index. The
guide's prose explains the table. No rule has a second copy in Rust.

The lint checks shape:

- operationId uniqueness, and the table's rendering for each operation the
  operation table binds;
- path classes and path parameter names;
- parameter names, and a description on every parameter;
- header spelling;
- schema name suffixes;
- each `limit`'s documented default and ceiling;
- a tag on every operation, from the declared `tags` array.

It reads statuses instead of listing them. A response whose description names
a registry code must document a status that `ErrorCode::http_statuses` lists
for that code. On today's spec that check finds no mismatch. The responses the
router produces stay with `describe_contract` in `http_contract.rs`, which
already adds 400 `api_contract_mismatch`, 404, 405 and 412 `actor_mismatch`
to every protected operation, 429 and 503 to graph and deployment routes, and
408 and 413 to those of them that take a body. The lint adds no response to
any operation.

Response coverage is checked independently of those descriptions. The
OpenAPI owner's assertions require the errors
each handler can send, including `outcome_unknown` for an operation that can
submit an owned write. Required statuses come from `ErrorCode::http_statuses`.
Removing a required response, or its named code, must fail even when no
remaining description mentions that code. The operation table's capabilities
drive the shared write-outcome assertions, without another route or status
registry. HTTP method alone cannot identify a write: POST also serves reads.

The lint extends the checks that already live in that file:
`openapi_operations_have_tags`, `multi_mode_operation_ids_are_unique`, the
Title-Case header check in
`openapi_describes_api_contract_admission_and_response_identity`, and
`change_error_output_schema_matches_every_change_route_detail`, which pins the
change-route projection.

An exception is a named, reviewed row in the table, as `forbidden_apis` pins
its exceptions, so a new deviation fails CI instead of joining the spec. Some
rows are permanent classes, such as protocol-defined paths and process
probes. Each other row names the batch or design that removes it.

**Batches.** A batch is the set of renames that ship under one HTTP contract
value. Protected HTTP routes accept exactly one: `require_contract` refuses
every other value before graph lookup. An incompatible change to a released
contract requires a new value and moves HTTP clients and servers together.
This batch completes the unreleased 0.14 contract, so it keeps 0.14; the header
does not distinguish earlier development builds from the final release. MCP
has its separate migration obligations below. Additive changes, such as newly
documented responses, bounds and descriptions, need no new value and land with
the lint.

**Not in this RFC.** Statuses, operation names, the change-route schema, the
conditional routes and the branch-action URLs keep their owners. Merging
`/load` with `/load/ndjson`, or `/diff` with `/branches/diff`, would merge
operations with different bodies and meanings; the operation table decides
operations, so neither merge is proposed here. Bounded lists are the separate
page-token design above. `WWW-Authenticate` on graph-route 401s is appendix
[A1][a1] of the graph-access RFC and issue
[#1020](https://github.com/ModernRelay/omnigraph/issues/1020).

## Invariants

- Invariant 9: query semantics stay typed. The conventions rename transport
  shapes, never query semantics.
- Invariant 10: trust stays at the boundary. No header supplies the trusted
  actor: the bearer credential resolves it, and `Omnigraph-If-Actor` only
  checks the actor the credential already resolved.
- Invariant 11: failures are bounded and observable. Only the returned
  `disposition.action` authorizes a retry; the registry's default cannot
  override unknown effects or a recovery intent. An uncertain completion
  requires reconciliation or recovery before another write, never blind
  replay.
- Invariant 12: one source of truth. The conventions are one table that the
  lint reads. Statuses stay in `ErrorCode`, router responses in
  `describe_contract`, operation names in the operation table and the
  change-route schema in `ChangeErrorOutput`. This RFC copies none of them.

No deny-list item is involved.

## Compatibility and reversibility

The lint, its table and the additive documentation are free to land and to
revert. The rename batch completes HTTP contract 0.14 before release, with one
breaking release-note row per change:

| Change | Where | Breaking-note row |
|---|---|---|
| operationIds rendered from the operation table, without `cluster_` | Every operation under `/graphs/{graph_id}` | Regenerate SDKs. Method names change; the wire does not. |
| `{branch}` becomes `{branch_name}`, `{name}` becomes `{query_name}`, `{id}` becomes `{deployment_id}` | `/branches/{branch}`; `/queries/{name}` and `/queries/{name}/if-graph-commit`; `/cluster/deployments/{id}` | SDK argument names change; URLs do not. |
| `*Response` becomes `*Output`; enums drop `Output`; `BlobEntityKind` and `EntityKindOutput` become one `EntityKind` | The 5 `*Response` schemas and the 6 `*Output` enums | SDK type names change; values do not. |
| `AccessOutput.actor` becomes `actor_id` | `GET …/access`, MCP `access`, `omnigraph access --json` | Read `actor_id`. The CLI's access protocol moves from 1 to 2; MCP clients follow the tool-schema migration below. |
| `unpublished` and `unattached` become one `listing` enum: `history` (the default), `unpublished`, `unattached` | `GET …/commits` | Send `listing=unpublished` or `listing=unattached` instead of the corresponding true boolean. Non-history listings still refuse `limit` and `page_token`; retired selectors are rejected as described below. |

`multi_mode_openapi_uses_inventory_operation_ids` asserts the table's ids
without `cluster_`. Uniqueness rests on
`multi_mode_operation_ids_are_unique` and the lint. Old spellings are not kept
as aliases. Protected HTTP routes continue to require 0.14 and refuse other
values with 400 `api_contract_mismatch` before graph lookup. A prerelease
client sending 0.14 is not automatically refused: it must use the final
response fields and selectors. The batch changes names in `openapi.json` and
two wire spellings, `actor_id` and `listing`; no storage, query or authorization
contract changes. After 0.14 is released, an incompatible change or reversal
requires another contract value; pre-release corrections do not.

The HTTP contract header does not fence `/mcp`. Renaming MCP `access.actor`
to `actor_id` is an intentional breaking tool-output change in the same
release. After the server upgrade, MCP clients reconnect and refresh cached
tool schemas through `tools/list`; clients with fixed response types must
update them to `actor_id` before reconnecting. Old MCP clients are not
automatically refused. The release note must say so. MCP protocol negotiation
stays unchanged, with no field alias or additional version mechanism.

For commit listing, either retired query key, `unpublished` or `unattached`,
answers 400 `bad_request` regardless of its value or whether `listing` is also
present. Query deserialization must not silently ignore these keys and fall
back to `history`, even when the caller sends the required 0.14 header. A
caller that previously sent a false boolean omits it or sends
`listing=history`. This is a narrow refusal for the two retired selectors;
it does not change unknown-query-parameter handling on other routes.

## Alternatives

- **Do nothing.** Every new route keeps adding a form, the generated SDKs keep
  exposing six operationId forms, and the MCP and HTTP names keep drifting.
- **Fix each inconsistency when its route is touched.** This spreads breaking
  renames over many contract values, so clients migrate many times.
- **Adopt an external linter (Spectral) with a public ruleset.** A public
  ruleset covers generic rules, but not the ones specific to OmniGraph: the
  operation-table rendering, the registry-derived status check or the pinned
  classes. A Rust test over the generated spec stays in the existing owner and
  needs no new toolchain. Spectral can still run in CI later if the generic
  rules are wanted.
- **A status table in the guide.** It would be a second list beside
  `ErrorCode`, and it would drift from what `ApiError::status` sends.
- **One mutate route with an optional `Omnigraph-If-Graph-Commit`.** A client
  that drops the header would run an unconditional write.
- **Moving the branch actions off `/branches/{branch}`.** `BranchAction`
  already lets every branch name be deleted and keeps the action URLs; a move
  would make every client migrate for path shape alone.
- **201 for creates, and 400 for a `limit` above its ceiling.** Both change
  what callers observe. A create already returns its receipt with 200, and 413
  tells the caller to split the work, where 400 says the request is malformed.
- **One error schema for every route.** It would advertise conflict details on
  change routes that cannot send them.

## Evidence and tests

- `crates/omnigraph-server/tests/integration/openapi.rs` owns the new lint,
  beside `openapi_spec_is_up_to_date`, and extends the checks named in
  [Design](#design). Exercise the coverage check against a generated-spec copy
  with a required write response removed, and one with only its code removed;
  both must fail. A newly inventoried write must require the same outcome.
- The existing `ApiError` disposition and retry-header tests remain the owner
  for 503 `service_unavailable` after an owned write: unknown effects,
  reconciliation and no `Retry-After`, alongside ordinary retry and recovery
  cases. Documentation must agree with these final response facts.
- `data_routes` extends its commit-list cases with the unchanged 0.14 header:
  omitted `listing` and `listing=history` select history, while
  `listing=unpublished` and `listing=unattached` select their distinct
  populations. Each retired key is
  refused with 400 for true, false and empty values, alone or beside
  `listing`; invalid enum values and the non-history paging refusals stay
  covered.
- The existing MCP catalog and server `mcp` parity owners cover initialization
  without the HTTP contract header, rediscovery through `tools/list`, and
  `access` execution. The output schema requires `actor_id` and has no `actor`
  property; the structured result contains `actor_id`, omits `actor`, and
  equals the HTTP access body. MCP protocol negotiation and the HTTP
  contract-header exemption remain covered.
- The rename batch updates the existing route owners: `data_routes`,
  `auth_policy`, `schema_routes`, `stored_queries`, `multi_graph`,
  `boot_settings` and `mcp`, plus the CLI's `GraphHttpClient`, its access
  command and managed fixtures, and
  `multi_mode_openapi_uses_inventory_operation_ids`.
- The conventions this RFC writes down are already held:
  `data_routes::remote_branch_delete_flow_works` creates and deletes branches
  named `merge`, `diff` and `merge-base` and checks the 405 `Allow` header of
  every other method, and `data_routes::mutate_graph_commit_precondition_issue_365`
  holds the fail-closed conditional route.

## Rollout

Implemented in the following PRs, in merge order:

- [#1131](https://github.com/ModernRelay/omnigraph/pull/1131): OpenAPI
  conventions, the documentation-driven lint and additive documentation.
- [#1132](https://github.com/ModernRelay/omnigraph/pull/1132): the shared
  operation inventory, checked against actual HTTP registrations, MCP tools
  and CLI commands.
- [#1133](https://github.com/ModernRelay/omnigraph/pull/1133): finalized the
  unreleased HTTP contract 0.14 with SDK naming, `actor_id`, commit `listing`,
  access protocol 2 and MCP client migration.

The implementation retains HTTP contract 0.14, which is unreleased. The
graph-access changes in [#1074](https://github.com/ModernRelay/omnigraph/pull/1074)
and [#1076](https://github.com/ModernRelay/omnigraph/pull/1076) introduced
`disposition`, the code registry and `Omnigraph-If-Actor` during prerelease
development; this batch completes that same contract. HTTP admission does
not establish compatibility with earlier development shapes or MCP clients.

Only permanent exceptions and the pagination-owned conditional default remain
in the conventions table. List pagination and the existence-hiding policy
question below are outside this RFC's completed scope.

## Unresolved questions

- **Existence hiding.** `not_found` already says the caller may not learn that
  a resource exists, and [Motivation](#motivation) lists where a denial answers
  404 today and where it answers 403. Draft #951 refuses a view-restricted
  read with 403 `view_restricted`, and a hidden name there produces the same
  error as an undeclared one. Which rule decides between 404 and 403 for each
  resource, and should #951's view model settle it rather than this RFC?

## Decision log

- 2026-10-19: Drafted from an audit of the spec at contract 0.14. Ragnor
  chose to fix the branch-name collision now, on its own, and to carry the
  remaining conventions and migrations in this RFC.
- 2026-10-21: Revised after review on #1089 so that no rule sits beside a
  contract the code already enforces. Statuses stay with `ErrorCode` and
  `describe_contract`: the status table, the shared-response injection, the
  pending-publication 503, 201 and the 413-to-400 move are gone, and an
  unsettled write stays 500 `outcome_unknown`. Names derive from the
  graph-access operation table, the `cluster_` prefix drops in the rename
  batch, and the non-graph path classes are named. The fail-closed conditional
  routes, `ChangeErrorOutput` and the `BranchAction` dispatch stay; the
  branch-action move and the route merges are dropped. The draft then assigned
  the renames to a contract after 0.14 (corrected below); bounded lists become
  a page-token design shared with #1012, the audit counts were refreshed
  against `openapi.json`, and the conventions became one table the lint reads.
- 2026-10-22: Clarified that the final response disposition controls retry,
  including reconciliation after a 503 and the recovery exception. Separated
  MCP's breaking tool-schema migration from HTTP contract admission, required
  explicit refusal of retired commit-list selectors, and added required-error
  coverage checks to the existing OpenAPI owner.
- 2026-10-22: Recorded the three stacked implementation PRs and initially
  assigned contract 0.15 to the rename batch (corrected below). Flat-path
  classification now derives from the shared operation inventory; the
  implementation remains in progress until the stack merges.
- 2026-10-22: Corrected the release assumption: 0.14 remains unreleased, and
  merged graph-access code is prerelease development. The rename batch stays
  within HTTP contract 0.14; #1133 standardizes that contract instead of
  advancing it to 0.15. Access protocol 2, the MCP client upgrade and explicit
  retired-selector refusal remain. New HTTP contract values are required for
  incompatible changes after release, not for this pre-release correction.
- 2026-10-22: Accepted and completed through
  [#1131](https://github.com/ModernRelay/omnigraph/pull/1131),
  [#1132](https://github.com/ModernRelay/omnigraph/pull/1132) and
  [#1133](https://github.com/ModernRelay/omnigraph/pull/1133). The conventions,
  operation inventory and rename batch are implemented with HTTP contract
  0.14 and CLI access protocol 2. Pagination and existence hiding retain
  their separate owners.

[graph-access]: 2026-10-16-graph-access-serving-and-cross-process-writes.md
[one-set]: 2026-10-16-graph-access-serving-and-cross-process-writes.md#one-set-of-operations
[dispositions]: 2026-10-16-graph-access-serving-and-cross-process-writes.md#dispositions-and-refusals
[a1]: 2026-10-16-graph-access-serving-and-cross-process-writes.md#a1-www-authenticate-on-every-401
[storage-outcomes]: 2026-10-19-storage-outcomes-and-restartable-ownership.md
[pr-1012]: https://github.com/ModernRelay/omnigraph/pull/1012
[pr-951]: https://github.com/ModernRelay/omnigraph/pull/951
