---
rfc: "0003"
title: "MCP server surface"
track: maintainer
status: draft
implementation: partial
authors:
  - OmniGraph maintainers
created: 2026-06-01
updated: 2026-10-20
discussion: https://github.com/ModernRelay/omnigraph/issues/991
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0003: MCP server surface
**Tickets:** MR-969 (stored queries + MCP exposure), MR-956 (federated auth /
WorkOS OAuth), MR-974 (agent setup surface), MR-668 (multi-graph server)
**Builds on:** [RFC 0041](0041-inline-stored-queries.md) (stored-query registry,
`GET /queries`, `POST /queries/{name}`, coarse `invoke_query`) and
[Identity, credentials and applied policy](2026-09-09-identity-credentials-and-applied-policy.md),
which shipped the `/mcp` transport and its initial tools.
**Replaces:** the obsolete MCP-transport sketch removed from RFC 0041. See
[Relationship to RFC 0041](#relationship-to-rfc-0041).

## Summary

`omnigraph-server` is the only MCP implementation for OmniGraph. Its `/mcp`
endpoint speaks stateless Streamable HTTP through the official Rust SDK and
projects existing HTTP handlers as MCP tools and resources. Every tool calls the
handler its HTTP route calls, under the same actor and the same Cedar action; the
MCP layer adds no authorization path of its own. Tool visibility is never
permission.

This RFC decides the surface in two shippable phases on top of the shipped
transport:

1. **Self-contained discovery and useful read tools.** Protected-resource
   metadata is served at the path the OAuth challenge advertises; a refused
   bearer gets `error="invalid_token"`; a `schema` tool and an opt-in ad-hoc
   `read` tool let a model answer questions about a graph.
2. **MCP for every authenticated deployment.** `/mcp` is mounted whenever the
   server has a bearer credential source (static tokens, signed data tokens or
   OIDC); read parity with the `@modernrelay/omnigraph-mcp` npm package; opt-in
   write tools that keep the package's write contract.

After a release ships both phases, the npm package is deprecated in favour of the
server's `/mcp`. It is retired, not kept as a stdio proxy.

## Relationship to RFC 0041

RFC 0041 is the parent decision for inline and stored queries. This RFC owns MCP
transport and revises that earlier sketch where shipped behavior or the MCP wire
protocol diverged.

1. **Transport.** RFC 0041 sketched a bespoke `GET /mcp/tools` + `POST /mcp/invoke`
   pair. That is not the MCP wire protocol. This RFC uses MCP JSON-RPC over
   Streamable HTTP and reuses the stored-query catalog as a projection source.
2. **Exposure.** Cluster serving catalogs every registered query; this RFC adopts
   that registry. Per-actor selection of stored queries is a Cedar concern once
   per-query scope exists (deferred, see [Rollout](#rollout)).
3. **Schema introspection.** RFC 0041 lists schema introspection through MCP as a
   non-goal. This RFC revises that: the `schema` tool and schema resource exist
   and are gated by `read`. A curated agent denied `read` never sees the schema,
   so the intent is kept by policy rather than by omission.

## What shipped before this amendment

The phases below have since shipped (see [Rollout](#rollout)). Before them, the
identity RFC shipped, behind `--oidc-identity-trust` only:

- `GET /.well-known/oauth-protected-resource` (root path only) and a 401 from
  `/mcp` whose challenge names `<resource origin>/.well-known/oauth-protected-resource<resource path>`;
- `/mcp` on `rmcp` (pinned exactly), stateless (`legacy_session_mode = false`),
  JSON responses, no `Mcp-Session-Id`;
- three read-only tools: `graphs` (discovery), `queries` (the stored-read catalog
  with mutations filtered out) and `query` (a named stored read invoked with
  `expect_mutation: false`);
- limits of 64 KiB per request, 1 MiB per complete tool result, a 30 second wait
  per read and 16 concurrent tool executions; cancellation and the deadline stop
  the caller's wait while the execution keeps its ingress reservation, observer,
  graph capture and slot until it finishes.

Requests are admitted by the server's ordinary ingress (`/mcp` is a read-class
route with its own 64 KiB body bound) and authenticated per HTTP request by
`require_bearer_auth`; no MCP session retains an actor.

## Motivation

- **The tools cannot answer questions.** Only pre-written stored reads run. There
  is no schema and no ad-hoc read, although the server already has both as
  Cedar-gated handlers (`GET /schema`, `POST /query`).
- **Discovery is not self-contained.** A resource with a path (`https://host/mcp`)
  advertises a metadata URL the server does not serve; every deployment needs a
  proxy rewrite before Claude can connect.
- **A refused token is indistinguishable from no token.** RFC 6750 §3.1 defines
  `error="invalid_token"` for that case; clients use it to refresh.
- **MCP exists only with OIDC.** Servers on static or signed data tokens, which
  covers most self-hosted deployments, have no MCP and fall back to the npm
  package.
- **Two implementations drift.** The npm package has 15 tools including writes,
  the server has three. Fixes and policy semantics diverge, and the package is
  stdio-only, so Claude on the web cannot use it.

## User and operational behavior

### Endpoint and mounting

One endpoint, `POST /mcp`, server-scoped. Graph-scoped tools take a `graph`
argument; there are no per-graph MCP endpoints.

| Server credential sources | `/mcp` | Protected-resource metadata | Challenge on a 401 |
|---|---|---|---|
| none (`--unauthenticated` or policy-only) | not mounted | not served | `Bearer` |
| static bearer tokens and/or signed data tokens | mounted | not served | `Bearer` |
| OIDC identity trust (alone or with the above) | mounted | served | `Bearer resource_metadata="<url>"` |

Authentication is the server's existing middleware: static tokens, then signed
data tokens, then OIDC. A 401 happens before the request body is collected.
Every 401 the server returns, from `/mcp` and from every REST route alike,
carries `WWW-Authenticate` as RFC 9110 §15.5.2 requires; a refused bearer
credential adds `error="invalid_token"` (see [Challenge](#challenge)). A plain
`Bearer` challenge advertises no OAuth flow.

### Discovery (OIDC only)

- `GET /.well-known/oauth-protected-resource` keeps answering the metadata.
- `GET /.well-known/oauth-protected-resource{resource path}`, where the resource
  path is the configured resource URL's path without a trailing `/`, answers the
  same body. The path is computed once from the fixed boot binding and compared
  with the request path byte for byte; any other suffix is 404. This is the URL
  the challenge advertises and the first URL MCP clients probe.
- A 401 from any route with no credential carries
  `WWW-Authenticate: Bearer resource_metadata="<url>"`. A 401 after a presented
  bearer credential was refused carries
  `WWW-Authenticate: Bearer error="invalid_token", resource_metadata="<url>"`.
- The server has one resource identifier, so every route's challenge names the
  same metadata. RFC 9728 §3.3 has a client that followed a challenge check
  that the metadata's `resource` matches the URL it requested. An identifier
  naming the service root (`https://graphs.example.com`) is a path prefix of
  every route's URL, which the MCP TypeScript SDK accepts
  (`checkResourceAllowed`) and a REST client can compare the same way; one
  naming `/mcp` matches only that endpoint, so a REST client must not use its
  metadata.

### Host and Origin

`rmcp` refuses a disallowed `Host` with 403. Allowed hosts are the SDK's loopback
defaults, the OIDC resource authority when configured, and every host named by the
new allowed-host setting. With OIDC, the resource origin is the allowed browser
origin, as today. Without OIDC there is no resource origin, so a request carrying
any `Origin` header is refused with 403; native clients (Claude Code, Cursor) send
none. That refusal is OmniGraph's own layer, not the SDK's: see
[Host and Origin checks](#host-and-origin-checks).

### Settings

MCP settings are process boot settings, spelled like the server's other boot
settings: a flag and an equivalent environment variable, read once at boot.

| Flag | Environment | Default | Effect |
|---|---|---|---|
| `--mcp-allow-adhoc` | `OMNIGRAPH_MCP_ALLOW_ADHOC=1` | off | lists and admits the ad-hoc `read` tool |
| `--mcp-allow-writes` | `OMNIGRAPH_MCP_ALLOW_WRITES=1` | off | lists and admits the write tools |
| `--mcp-allowed-host <host[:port]>` (repeatable) | `OMNIGRAPH_MCP_ALLOWED_HOSTS=a,b` | none | adds allowed `Host` values |

Both opt-ins default off even when an applied Cedar policy would deny the actions:
they are defence in depth that an operator turns on deliberately, independent of
policy. A tool that is off is absent from `tools/list` and a call to it is refused
as an unknown tool. These settings never grant anything: an enabled tool still
runs only what Cedar allows the actor.

### Tool catalog

Every graph-scoped tool resolves its graph through the registry exactly as an HTTP
request does (unknown and denied graphs are 404, an authorized unavailable graph is
503) and then calls the named handler.

| Tool | Phase | Handler (HTTP route) | Cedar action | Annotations |
|---|---|---|---|---|
| `graphs` | shipped | `server_graphs_discovery` (`GET /graphs/discovery`) for an identity credential; otherwise `server_graphs_list` (`GET /graphs`) narrowed to ids and names | identity, or `graph_list` | read-only |
| `queries` | shipped | `server_list_queries` (`GET /graphs/{id}/queries`), mutations filtered out | `read` | read-only |
| `query` | shipped | `server_invoke_query` (`POST /graphs/{id}/queries/{name}`), `expect_mutation: false` | `invoke_query`, then `read` | read-only |
| `schema` | 1 | `server_schema_get` (`GET /graphs/{id}/schema`) | `read` | read-only |
| `read` | 1, opt-in | `server_query` (`POST /graphs/{id}/query`) | `read` | read-only |
| `health` | 2 | `server_health` (`GET /healthz`) | none | read-only |
| `snapshot` | 2 | `server_snapshot` (`GET /graphs/{id}/snapshot`) | `read` | read-only |
| `branches_list` | 2 | `server_branch_list` (`GET /graphs/{id}/branches`) | `read` | read-only |
| `commits_list` | 2 | `server_commit_list` (`GET /graphs/{id}/commits`) | `read` | read-only |
| `commits_get` | 2 | `server_commit_show` (`GET /graphs/{id}/commits/{commit_id}`) | `read` | read-only |
| `commits_changes` | 2 | `server_commit_changes` (`GET /graphs/{id}/commits/{commit_id}/changes`) | `read` | read-only |
| `changes_poll` | 2 | `server_changes_feed` (`GET /graphs/{id}/changes`) | `read` | read-only |
| `mutate` | 2, opt-in | `server_mutate` / `server_mutate_if_graph_commit` | `change` (branch statements: their branch action) | destructive |
| `load` | 2, opt-in | `server_load` (`POST /graphs/{id}/load`) | `change` (+ `branch_create` with `from`) | destructive |
| `branches_create` | 2, opt-in | `server_branch_create` (`POST /graphs/{id}/branches`) | `branch_create` | not destructive |
| `branches_delete` | 2, opt-in | `server_branch_delete` (`DELETE /graphs/{id}/branches/{name}`) | `branch_delete` | destructive, idempotent |
| `branches_merge` | 2, opt-in | `server_branch_merge` (`POST /graphs/{id}/branches/merge`) | `branch_merge` | destructive |

Names follow the shipped tools. The package's `schema_get` is `schema`; its ad-hoc
`query` is `read`, because the shipped `query` already names the stored read; its
`graphs_list` is the shipped `graphs`. Discovery admits only identity
credentials (OIDC or signed identity tokens), so for any other credential, such
as a static token, `graphs` calls `GET /graphs` under `graph_list`, as the
package did, and returns only ids and names. `graph_list` is server-scoped, and
without an applied cluster policy every server-scoped action is denied, so on a
default static-token server `graphs` returns 403. The tool description and the
server instructions say so, and tell a model to ask for a `graph_list` permit or
to use a graph id the user supplies. `read` also serves what `POST /query`
serves: `branch list`, `branch diff`, `branch merge-base`, `branch merge … dry run`
and `explain query …`. `mutate` serves the branch write statements `POST /mutate`
serves. Stored mutations stay unavailable through `query`; they arrive with
per-query tools (deferred).

Schema application, deployment and cluster configuration are never MCP tools;
they are operator-owned through `omnigraph cluster plan/apply`.

### Tool results

- Success: `structuredContent` is the handler's JSON response body, equal in
  value to the HTTP route's body except where the catalog names a projection
  (`queries` without stored mutations, the `graphs` fallback's ids and names),
  plus the SDK's text duplicate for older clients.
- Failure: `isError: true` with `structuredContent` either
  `{"status": <http status>, "error": <the route's ErrorOutput>}` for a handler
  refusal, or `{"code": "...", "message": "..."}` for an MCP-side refusal
  (`invalid_arguments`, `capacity_exceeded`, `cancelled`, `deadline_exceeded`,
  `result_too_large`). A write whose wait ended before its result adds
  `"outcome_unknown": true`.
- Tools whose success shape is fixed declare an `outputSchema`; it admits both the
  success shape and the error envelope, because clients validate any
  `structuredContent` they receive against it.
- `queries` returns each stored read's typed parameter descriptors, so a model can
  call `query` correctly.

Server `instructions` and tool descriptions tell a model to call `schema` (or read
the schema resource) first, then `read` or a stored `query`, and summarize the
write contract below.

### Resources (phase 2)

| URI | Content | Gate |
|---|---|---|
| `omnigraph://graphs` | the `graphs` result, JSON | as `graphs` |
| `omnigraph://graphs/{graph}/schema` (template) | the `.pg` source, `text/plain` | `read` |
| `omnigraph://graphs/{graph}/branches` (template) | `GET /branches` JSON | `read` |
| `omnigraph://best-practices/index` | index of the references below, `text/markdown` | any authenticated actor |
| `omnigraph://best-practices/{topic}` for `queries`, `data`, `schema`, `search`, `changes` | `skills/omnigraph/references/{topic}.md` | any authenticated actor |

The package's `omnigraph://schema` and `omnigraph://branches` were single-graph:
the package was configured with one graph id. The server is multi-graph, so these
are resource templates naming the graph. The best-practices bodies are the skill
references already maintained in this repository and embedded at build time
through the `omnigraph-skill` crate, so the server ships exactly the references
written beside its source.

### Write contract (phase 2)

The write tools keep the npm package's contract:

- **Conditional writes.** `mutate` takes `if_graph_commit`; when present it calls
  the conditional route (`/mutate/if-graph-commit`), so a stale head fails with 412
  before effects. A read's `graph_commit_id` is the value to pass.
- **No automatic retry.** The server never replays a tool call.
- **A lost response is an unknown outcome.** Writes register through the server's
  owned-write boundary, so the end of the caller's wait never cancels them. The
  server waits 30 seconds for a write, as for a read, which is below the MCP
  TypeScript SDK's default 60 second request timeout, so a client on that default
  normally receives the server's answer. When the server's wait ends first, the
  result is `isError: true` with `outcome_unknown: true`; a handler error the
  server marks completion-uncertain carries the same flag. When the client gives
  up first, or cancels (in Streamable HTTP a cancellation closes the request, after
  which the server sends nothing for it), no result arrives at all, and the caller
  must treat that the same way. Atomic publication does not make a write
  exactly-once after a lost acknowledgement ([first-principles data systems,
  §B.1](../dev/systems.md#b1-a-guarantee-record)). Before replaying, reconcile: read the target with
  `read` or `snapshot` and compare its `graph_commit_id` with the one the write was
  based on, or list the branch's commits with `commits_list` and look for the
  write's effect. A replay of `mutate` should carry `if_graph_commit` so a write
  that did land fails with 412 instead of applying twice.
- **Bounds.** A write's arguments, including `load` data, fit in the 64 KiB MCP
  request; larger loads use `POST /load/ndjson`. Each write reserves write ingress
  for its argument bytes and per-actor write admission as its HTTP route does.
  Unlike the HTTP route, it is observed in the read lane and shares the 16 MCP
  slots with reads, so read pressure can refuse it with `capacity_exceeded`. An
  HTTP write takes the separate write observer lane, which exists so slow readers
  cannot block writes; `/mcp` is one read-class route whose operation is known
  only after its body is parsed. A separate MCP write slot pool with the write
  observer, or classifying on the `Mcp-Method` and `Mcp-Name` headers before body
  collection, would close the gap; this RFC accepts the trade-off because MCP
  writes are opt-in and the refusal is explicit and retryable.

## Design

### One handler, many tools

`GraphTools` implements the SDK's `ServerHandler`. Each tool is a static
definition (name, description, input schema, optional output schema, annotations,
the setting that enables it) and one dispatch arm that deserializes its arguments
(`deny_unknown_fields`) and calls the HTTP handler function with the request's
`AuthenticatedActor`. No business logic or authorization is duplicated.

### Admission and lifetimes

`/mcp` stays a read-class ingress route: the body is collected under a 64 KiB read
reservation and the response is observed in the read lane. A tool call or resource
read runs through the shipped `run_read`: it takes one of 16 slots, spawns the
execution on the request's observer with its ingress lease, slot and graph capture,
and races the caller's cancellation and deadline. The execution keeps those
reservations until it finishes. A write tool additionally reserves write ingress
for its argument bytes and hands it to the handler, whose per-actor admission and
owned-write registration are unchanged. The graph is selected at most once per
HTTP request through the existing `McpGraphRequest` slot, so a selected graph's
capture follows the response and its yielded bytes.

### Discovery routes

The metadata handler is shared by the root route and a wildcard route under
`/.well-known/oauth-protected-resource/`. The wildcard handler compares the raw
request path with the advertised path computed once at router construction; it
never echoes or decodes the request path. Using a wildcard instead of registering
the configured path as a literal route keeps an unusual resource path from
reaching the router's path syntax.

### Host and Origin checks

`rmcp` 3.3.0 checks `Host` against its allowed list and `Origin` against its
allowed origins, but treats an empty origin list as unrestricted. With OIDC the
list holds the resource origin. Without OIDC, OmniGraph's router adds a layer
between authentication and body admission that refuses any request carrying
`Origin` with 403, and an integration test pins it, so an SDK upgrade that changes
its default cannot remove the refusal silently.

### Challenge

The challenge belongs to the authentication decision, not to a router.
Authentication reads the request's one `Authorization` header once: the scheme
is case-insensitive (RFC 9110 §11.1) and one or more spaces precede the token
(RFC 6750 §2.1). Another scheme, a missing header or a `Bearer` without a token
presents no credential; a bearer token that does not authenticate, or a bearer
credential among several `Authorization` headers, is a refused one. The 401
records which. One layer outside every router renders the server's challenge on
every 401 response, including one a handler returns: `Bearer`, plus
`error="invalid_token"` when a presented bearer credential was refused (RFC 6750
§3.1, with or without OIDC), plus `resource_metadata` with OIDC. Both forms are
built once from the boot binding, never from a request's `Host`, `Origin` or
path, and never reflect the credential or a refusal reason. `/mcp` keeps only
its own `Cache-Control: no-store`. The OpenAPI document records the header on
every protected operation's 401.

## Invariants

- **Trust at the boundary, enforcement at the engine (invariant 10).** Actors come
  only from `require_bearer_auth`; embedder-supplied extensions are discarded as
  today. Every tool reaches the engine through a handler that applies its Cedar
  gate; write tools reach the same `_as` entry points as HTTP writes.
- **Bounded and observable (invariant 11).** Request, result, wait, concurrency and
  ingress bounds are explicit; refusals are typed. Writes report an unknown outcome
  instead of acknowledging or retrying.
- **No acknowledgement before durable visibility.** A write tool returns success
  only with the handler's success body.
- **Query semantics stay typed (invariant 9).** Tools pass GQ source and typed
  parameters to the existing compiler path; no string-built semantics.
- No deny-list shape is introduced: no queue, no shadow state, no process-local
  lock presented as fencing.

## Compatibility and reversibility

- Additive. Without credential sources nothing changes. Existing tool names,
  arguments and result shapes are unchanged; `graphs` gains an output schema.
- OpenAPI gains one documented route,
  `/.well-known/oauth-protected-resource/{resource_path}`. `/mcp` stays out of the
  REST document.
- Every 401 gains a `WWW-Authenticate` header and the scheme in `Authorization`
  is read case-insensitively; a response body and status are unchanged. A
  client that read a missing challenge as "no OAuth" now reads the
  `resource_metadata` parameter's presence instead.
- Turning the opt-ins off removes tools without affecting HTTP.
- The npm package's users move by pointing their client at `https://<server>/mcp`
  with the same token. Tool names differ where noted above. Reverting this RFC means
  keeping the package; nothing durable depends on the server surface.

## Alternatives

- **Keep the npm package as a stdio↔HTTP proxy** (this RFC's previous text). It
  keeps a second package to version and release for no capability the server
  lacks: Claude Code and Cursor speak Streamable HTTP with an `Authorization`
  header, and Claude on the web cannot use stdio. If local Claude Desktop use
  needs stdio, an `omnigraph mcp --server <url>` bridge belongs in the Rust CLI.
- **Per-graph endpoints** (`/graphs/{id}/mcp`, this RFC's previous §5.7). One
  endpoint with a `graph` argument already shipped, lets a client discover graphs
  without reconfiguration, and keeps one OAuth resource per server.
- **Dynamic per-stored-query tools now.** Without per-query `invoke_query` scope
  every actor would see every stored query as a tool; deferred until that scope
  exists.
- **Default the opt-ins on when a policy is applied.** Rejected for now: a policy
  written for HTTP clients was not reviewed for an LLM caller, and an explicit
  switch costs one flag.
- **A hand-rolled JSON-RPC layer.** The SDK already shipped and owns framing,
  negotiation and cancellation.

## Evidence and tests

The owner is `crates/omnigraph-server/tests/integration/mcp.rs`, extended rather
than duplicated:

- discovery at both metadata paths, 404 for other suffixes, both challenge forms;
- `tools/list` snapshot per setting combination (names, annotations, presence of
  `outputSchema`, absence of disabled tools) and `initialize` instructions;
- `schema` and `read` results equal to the HTTP routes' bodies for an allowed actor;
  denied actors get the same refusal envelope as other tools; `read` refuses
  `insert`/`update`/`delete` and `branch create` with the manifest version
  unchanged; the 64 KiB, 1 MiB and deadline bounds hold;
- phase 2: a static-token server answers with the token and 401 without it, refuses
  a foreign `Host` and any `Origin`; each parity tool equals its route; resources
  list and read; write tools are absent by default, honour `if_graph_commit` (412
  with no effect), and report `outcome_unknown` when the wait ends first.

`openapi.json` and its drift test cover the new route. In-source `mcp.rs` tests keep
owning `run_read` ownership through cancellation and the result bound.

The server-wide challenge is owned by
`integration/auth_policy.rs::issue_1020_every_401_carries_the_one_bearer_challenge`:
it takes every protected operation from the served OpenAPI document, adds `/mcp`
and the engine catalogue, and requires each to answer an absent, refused,
foreign-scheme and duplicated credential with 401 and the exact challenge on a
static-token, a signed-data-token and an OIDC server, and the document to
record the header on that 401. In-source `challenge.rs` tests own the header
reading.

## Rollout

1. **Phases 1 and 2, shipped together** in
   [#994](https://github.com/ModernRelay/omnigraph/pull/994) (`cea15256`); the
   separate phase 1 PR, #993, closed unmerged. Metadata at the advertised path,
   `invalid_token`, `schema`, opt-in `read`, descriptions, instructions and output
   schemas; mounting for every credential source with the allowed-host setting,
   read parity tools, graph resources and opt-in write tools; the identity RFC's
   "initial tools" paragraph, the MCP user guide, and `via mcp` in the
   self-contained server testing RFC.
2. **Best-practices resources** in
   [#995](https://github.com/ModernRelay/omnigraph/pull/995), with the 30 second
   write wait and the `graph_list` instruction from this review in
   [#999](https://github.com/ModernRelay/omnigraph/pull/999).
   `implementation` stays `partial` until these and the package retirement land.
3. **Server-wide challenge**, issue
   [#1020](https://github.com/ModernRelay/omnigraph/issues/1020): every 401
   carries the challenge `/mcp` sent alone, and the scheme is case-insensitive.
4. **Package retirement**, after a release contains phases 1 and 2: a final
   `@modernrelay/omnigraph-mcp` version whose README and binary print a
   deprecation pointing at the server's `/mcp`, then npm deprecation.
5. **Deferred, separate decisions:** per-stored-query `invoke_query` scope (a query
   name in `PolicyRequest` and the Cedar schema); one MCP tool per stored query
   filtered by that scope; a Rust stdio bridge only if local desktop clients need
   it; a token-lifetime cap separate from the 300 second admission-snapshot bound
   only if live qualification shows provider access tokens live longer.

## Unresolved questions

None block acceptance. Live qualification against the managed control plane will
answer whether a provider's access tokens outlive the 300 second bound and whether
claude.ai sends an `Origin` header on connector calls; either answer is handled by
a deferred item or the existing allowed-origin rule.

## Decision log

- 2026-10-20: Amended for the server-wide challenge (issue
  [#1020](https://github.com/ModernRelay/omnigraph/issues/1020)). Every 401
  carries `WWW-Authenticate`, `error="invalid_token"` follows a refused bearer
  credential with or without OIDC, and the `Authorization` scheme is read once,
  case-insensitively, by authentication. Superseded sentences: the mounting
  table's "not served; 401 carries no challenge"; "Without OIDC, a 401 from
  `/mcp`, like every other 401 the server returns today, carries no
  `WWW-Authenticate` header", "RFC 9110 §15.5.2 asks for one; a plain
  `WWW-Authenticate: Bearer`, which advertises no OAuth flow, belongs to a
  server-wide follow-up rather than to `/mcp` alone"; "A 401 from `/mcp` with no
  credential carries"; and the Challenge section's "The challenge layer records
  whether the request carried a `Bearer` credential before authentication runs,
  and adds `error="invalid_token"` only to a 401 for a request that did" (a
  second reading of the header that accepted `bearer` while authentication
  refused it).
- 2026-10-02: The `schema_apply` tool loses its `allow_data_loss` argument,
  following the removal of that field from the schema-apply route: a drop
  reclaims nothing at apply, so there is no destructive mode to select.
- 2026-10-14: Review of the amendment. The write wait drops from 60 to 30 seconds,
  below the MCP TypeScript SDK's default request timeout, and a lost response is
  stated as an unknown outcome with its reconcile step (supersedes "the 60 second
  write wait"). MCP writes are recorded as observed in the read lane and sharing
  the 16 slots (supersedes "exactly as its HTTP route does"). The best-practices
  resources are open to any authenticated actor (supersedes "identity required").
  Success bodies equal the route's except named projections (supersedes
  "byte-equal in value"). Rollout records that phases 1 and 2 shipped together.
- 2026-10-13: Amended for phases 1 and 2 (issue
  [#991](https://github.com/ModernRelay/omnigraph/issues/991)), recording the
  transport and tools the identity RFC shipped. Superseded sentences of the previous
  draft: "the stdio package becomes a thin stdio↔HTTP proxy (local on-ramp) over it"
  (the package is retired instead); the `schema_apply` and `ingest` parity rows and
  "13 tools … 2 resources" (schema application stays operator-owned; `load` replaces
  `ingest`; the package now has 15 tools and 4 resources); "`query`/`mutate` as
  canonical with `read`/`change` retained as deprecated-but-live aliases" (`query` is
  the shipped stored read, `read` the ad-hoc read, no aliases); the §5.4
  Cedar-filtered `tools/list` (listing follows settings only, and visibility is not
  permission); the §5.6 hand-rolled default (the SDK shipped); the §5.7 per-graph
  endpoints (one `/mcp` with a `graph` argument); the §5.10 text-only results (results
  are structured with output schemas where fixed); "RFC 9728 … slots in later" (it
  shipped with OIDC trust); Open Questions 1 to 8, each answered above or deferred.
