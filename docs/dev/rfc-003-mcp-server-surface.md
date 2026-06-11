# RFC: MCP Server Surface for `omnigraph-server` — Full Tool Parity, Stored Queries, Modular Auth

**Status:** Reference implementation shipped in [omnigraph#157](https://github.com/ModernRelay/omnigraph/pull/157) (proved rmcp 1.7 on edition 2024, the auth-extension passthrough, and the full tool/resource/Cedar surface). **This RFC is now the canonical spec for a clean reimplementation from `main`** that lands the surface with a dedicated `omnigraph-mcp` crate from the start — build from the **[Implementation Blueprint](#implementation-blueprint-canonical--build-the-fresh-implementation-from-this)** below, which incorporates the as-built reality and supersedes the §5 sketches where they differ. Deferred to follow-ups: per-query `invoke_query` scope (PR 0b), the OAuth/RFC-9728 layer (MR-956), and the stdio→proxy collapse.
**Date:** 2026-06-01
**Tickets:** MR-969 (stored queries + MCP exposure — the surface this completes), MR-956 (federated auth / WorkOS OAuth — the auth substrate this consumes), MR-971 (per-server credential resolver), MR-974 (agent setup surface — the installer that wires this), MR-668 (multi-graph server — shipped, the routing this builds on)
**Builds on:** [omnigraph#128](https://github.com/ModernRelay/omnigraph/pull/128) (`ragnorc/stored-queries-mcp`) — the shipped stored-query registry, `GET /queries`, `POST /queries/{name}`, and the coarse `invoke_query` gate.
**Supersedes:** the MCP-transport portion of [rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md) (`/mcp/tools` + `/mcp/invoke`). See [Relationship to RFC-001](#relationship-to-rfc-001).
**Target release:** v0.8.x (phased — see Rollout)

## Summary

Add a first-class **MCP (Model Context Protocol) server surface to `omnigraph-server`**, exposed over **Streamable HTTP**, that projects the server's operations as MCP tools and resources for LLM clients (Claude Code/Desktop/web, Cursor, etc.). Two populations of tools share one projection path:

1. **Built-in operational tools** — parity with the existing `@modernrelay/omnigraph-mcp` stdio package's operational tool set (`health`, `snapshot`, `schema_get`, `branches_list`, `commits_list`, `commits_get`, ad-hoc read/write, `ingest`, branch ops, `schema_apply`) and its core resources (`omnigraph://schema`, `omnigraph://branches`). (Exact counts moved over time — the package is now at 16 tools / ~9 resources; see [Relationship](#relationship-to-the-modernrelayomnigraph-mcp-stdio-package). The authoritative as-built catalog is [B.5](#b5-tool-catalog-13-built-ins--cedar-mapping): 13 tools.) (Server-scoped graph discovery — a `graphs_list` tool / `omnigraph://graphs` resource — was considered but **dropped from MCP**; it stays REST-only via `GET /graphs`. See [B.10](#b10-routing-resolved).)
2. **Dynamic stored-query tools** — one MCP tool per `mcp.expose: true` entry in the `queries:` registry (MR-969 / #128), with parameters typed from the `.gq` declaration via the shipped `query_catalog_entry` / `param_descriptor` projection.

Every tool is **authorized by the server's existing Cedar policy engine**. The MCP layer never implements its own authentication: it consumes an **already-resolved `ResolvedActor`** from the server's bearer middleware (`require_bearer_auth` today; the `TokenVerifier` seam when MR-956 lands), so the **same MCP endpoint serves on-prem (static or customer-OIDC tokens) and our cloud (WorkOS OAuth) by configuration only**. Cloud OAuth is an additive layer (RFC 9728 protected-resource metadata) that slots in with zero MCP changes.

The end-state collapses two diverging tool implementations into one: the in-server MCP is the canonical, Cedar-gated, remotely-reachable surface; the stdio package becomes a thin stdio↔HTTP proxy (local on-ramp) over it.

> **Key caveat, stated up front (see §5.9 below):** the headline "a token scoped via Cedar to a *specific set* of stored queries" requires **per-query `invoke_query` scope**, which is *designed* (rfc-001) but **not yet implemented** — the shipped action is coarse (any stored query on the graph, or none). Per-actor Cedar curation works today for *built-in vs ad-hoc vs admin* tools and for *stored-vs-ad-hoc*; sub-selecting individual stored queries per actor is gated on a prerequisite (PR 0b). Until then, stored-query curation is graph-level (registry membership + `mcp.expose`).

## Implementation Blueprint (canonical — build the fresh implementation from this)

> This section is the **authoritative, verified spec**. A reference implementation shipped in [omnigraph#157](https://github.com/ModernRelay/omnigraph/pull/157) and proved every choice here: rmcp **1.7.0** integrates on edition 2024, the auth-extension passthrough works, all conformance MUSTs are met, and the surface splits cleanly into a transport crate + a server backend. Build a clean implementation from `main` by following B.1–B.13 in order. Where this blueprint and the older §5 design sketches differ, **the blueprint wins** (the §5 text is retained as design rationale). Every reuse point named here exists in the engine/server today.

### B.1 Crate architecture & dependency direction

Split the surface into a **transport crate** plus a **server-side backend**:

- **`crates/omnigraph-mcp`** (new) — the rmcp Streamable-HTTP transport, the `McpBackend` trait, and rmcp model re-exports. `rmcp` (+ `tower-http`'s `limit` feature) live **only** here.
- **`omnigraph-server`** depends on `omnigraph-mcp` and *implements* `McpBackend`. All omnigraph-specific tool/resource/Cedar/dispatch logic lives in the server.

**The dependency MUST go `omnigraph-server → omnigraph-mcp`, never the reverse.** The server binary mounts `/mcp`, so a `mcp → server` dependency cycles at the package level (`server-bin → omnigraph-mcp → server-lib`), which Cargo rejects. The trait inverts the direction: the crate defines the seam, the server fills it. This is also *why* the crate cannot reach server internals (`AppState`, `do_*`, `authorize`, `api::*`) — it abstracts over them.

`omnigraph-mcp/Cargo.toml`: `edition = "2024"`, version-locked to the workspace. Deps: `rmcp = { version = "1.7", default-features = false, features = ["server", "transport-streamable-http-server"] }`, `axum` (for `Router`), `http`, `tower-http = { features = ["limit"] }`, `tokio`, `async-trait`, `serde_json`. Add `"crates/omnigraph-mcp"` to the workspace `members`; in `omnigraph-server/Cargo.toml` drop `rmcp` and the `tower-http` `limit` feature, add `omnigraph-mcp`.

### B.2 The `McpBackend` seam

Use `#[async_trait]` (boxed futures sidestep the async-fn-in-trait `Send`-bound friction; the cost is negligible at MCP QPS, and the server already depends on `async-trait`):

```rust
#[async_trait]
pub trait McpBackend: Clone + Send + Sync + 'static {
    fn server_info(&self) -> ServerInfo;
    async fn list_tools(&self, parts: &http::request::Parts) -> Result<Vec<Tool>, McpError>;
    async fn call_tool(&self, parts: &http::request::Parts, name: &str, args: JsonObject)
        -> Result<CallToolResult, McpError>;
    async fn list_resources(&self, parts: &http::request::Parts) -> Result<Vec<Resource>, McpError>;
    async fn read_resource(&self, parts: &http::request::Parts, uri: &str)
        -> Result<ReadResourceResult, McpError>;
}
```

**Why `&http::request::Parts` (the load-bearing mechanism — verified in rmcp source and `#157`).** rmcp's `StreamableHttpService` injects the original request's `http::request::Parts` into `RequestContext.extensions`. The server's `require_bearer_auth` + `resolve_graph_handle` middleware run *before* the MCP service and insert `ResolvedActor` + `Arc<GraphHandle>` into the request's extensions. The crate hands `&Parts` to the backend; the backend reads its own types from `parts.extensions`. The crate never names an omnigraph type → auth stays decoupled (§5.8) and the crate is reusable. The crate re-exports the rmcp model types the backend needs: `McpError (= rmcp::ErrorData)`, `Tool`, `CallToolResult`, `Content`, `JsonObject`, `Resource`, `RawResource`, `ResourceContents`, `ReadResourceResult`, `ServerInfo`, `ServerCapabilities`, `ToolAnnotations`, `Annotated` — so the server uses rmcp types via `omnigraph_mcp::…` and carries no direct rmcp dep.

### B.3 Transport (lives in `omnigraph-mcp`)

- A generic `struct McpService<B>` implements rmcp's `ServerHandler`, delegating each method to `B` after extracting `&Parts` from `ctx.extensions` once (missing → `McpError::internal_error`). `get_info → backend.server_info()`; `initialize`/`ping` use rmcp's defaults.
- `pub fn mcp_router<B: McpBackend>(backend: B, body_limit: usize, hosts: McpHostPolicy) -> axum::Router`:
  - `config = StreamableHttpServerConfig::default().with_stateful_mode(false).with_json_response(true).with_allowed_hosts(hosts.allowed_hosts).with_allowed_origins(hosts.allowed_origins)` (`StreamableHttpServerConfig` is `#[non_exhaustive]` — build from `Default`, mutate via the `with_*` setters). **Do not keep rmcp's fixed loopback `allowed_hosts` default** — it `403`s every non-loopback client *before* bearer auth (a deploy footgun); derive the policy per B.3.1.
  - `let svc = StreamableHttpService::new(move || Ok(McpService::new(backend.clone())), Arc::new(LocalSessionManager::default()), config)`; return `Router::new().route_service("/mcp", svc).layer(tower_http::limit::RequestBodyLimitLayer::new(body_limit))`. rmcp reads the body directly (not via an axum extractor), so axum's `DefaultBodyLimit` does **not** bound `/mcp`; the tower-http layer does.
- **Stateless mode delivers these conformance MUSTs for free (verified against rmcp 1.7 source):** `GET`/`DELETE /mcp` → `405` (with `Allow`); a disallowed `Host`/`Origin` → `403` (per the `McpHostPolicy`, B.3.1); `MCP-Protocol-Version` → `400` on unsupported, default `2025-03-26` when absent. **No conformance middleware is needed** (the §5.6 "honour the header" footnote is satisfied by rmcp).

**B.3.1 Host/Origin policy — derived from deployment, never a fixed default.** The `Host` header is not a security boundary for a bearer-authed server (the token is); rmcp's loopback `allowed_hosts` default exists to protect *local* dev servers from browser DNS-rebinding, and for any non-loopback deploy it just rejects legitimate clients. So `omnigraph-server` computes `McpHostPolicy` once at startup from what it already knows (the `--bind` address + any configured public host) and passes it in:
- **Loopback bind** (`127.0.0.1` / `::1` / `localhost`) → loopback `allowed_hosts` (rebind protection genuinely matters locally).
- **Non-loopback bind** (`0.0.0.0` / a public IP) → the operator chose remote reachability: allow the configured public host(s) if set, else **disable Host-allowlisting** (accept any `Host`) and log it — bearer auth + `Origin` validation are the real controls.
- `allowed_origins` is the actual browser-attack control: default empty (off — non-browser MCP clients send no `Origin`), configurable for browser-based clients. The class "one fixed default that is wrong for some deployments" is closed by making the policy a function of the deployment.
- **Client/test obligations rmcp enforces:** the request must carry `Accept: application/json, text/event-stream` (both), `Content-Type: application/json`, and a `Host` header. rmcp negotiates `protocolVersion` (a recent client sees `2025-11-25`).

### B.4 Server-side backend (lives in `omnigraph-server`)

`struct OmnigraphMcpBackend { state: AppState }` (derive `Clone` — `AppState` is already `#[derive(Clone)]`, `Arc`-backed) implements `McpBackend`. Per request it resolves the actor + handle from `parts.extensions.get::<ResolvedActor>()` / `get::<Arc<GraphHandle>>()`.

**Reuse, never reinvent.** First factor **10 thin `do_*` fns** out of the inline `server_*` HTTP handlers (each is `authorize_request(...) → engine call → DTO`) so REST and MCP dispatch one path: `do_snapshot`, `do_schema_get`, `do_branches_list`, `do_commits_list`, `do_commit_show`, `do_ingest`, `do_branch_create`, `do_branch_delete`, `do_branch_merge`, `do_schema_apply`. (No `do_graphs_list` — `graphs_list` is not an MCP tool, see B.10; `server_graphs_list` stays inline for `GET /graphs`.) Land that as a behavior-neutral refactor commit first (it keeps the REST handlers as thin wrappers; all server tests stay green). Then reuse as-is: `run_query` / `run_mutate` (already decoupled from request bodies), `authorize` → `Authz { Allowed, Denied(msg) }` (with `Err` reserved for operational 401/500), `api::query_catalog_entry` / `ParamKind` / `read_output`, `ApiError` (add `pub(crate) status_code()` + `message_str()` accessors for the error classifier). Mount in `build_app`: `.merge(mcp::mcp_router(state))` inside the `per_graph_protected` group, where the server's thin `mcp::mcp_router(state) = omnigraph_mcp::mcp_router(OmnigraphMcpBackend::new(state), INGEST_REQUEST_BODY_LIMIT_BYTES)`.

Represent the built-ins as a `Builtin` enum (one variant per tool; `descriptor` / `gate` / `call` as match arms) — lower liability than ~14 unit structs + `dyn` + `async-trait` per tool. Stored-query tools are a sibling populator over `handle.queries`.

### B.5 Tool catalog (13 built-ins) + Cedar mapping

Each built-in reuses the **exact `PolicyAction` its REST route enforces**. (No `graphs_list` — server-scoped graph discovery is REST-only, see B.10.)

| MCP tool | Scope | Cedar action |
|---|---|---|
| `health` | server | none (liveness/version) |
| `snapshot`, `schema_get`, `branches_list`, `commits_list`, `commits_get` | graph | `Read` |
| `query` (ad-hoc read) | graph | `Read` (`run_query` self-authorizes) |
| `mutate` (ad-hoc write) | graph | `Change` |
| `ingest` (NDJSON) | graph | `Change` (+ `BranchCreate` when `from` forks) |
| `branches_create` / `branches_delete` / `branches_merge` | graph | `BranchCreate` / `BranchDelete` / `BranchMerge` |
| `schema_apply` (`allow_data_loss`) | graph | `SchemaApply` |
| *stored query* | graph | `InvokeQuery` (coarse) then inner `Read`/`Change` |

Baked-in decisions (resolve the Open Questions):
- **Tool ids are `query`/`mutate` only — no `read`/`change` aliases.** The server HTTP surface already deprecated `/read`,`/change`; a fresh in-server MCP has no legacy clients to keep, so it exposes only the canonical ids. [Open Q7 → resolved: no aliases.]
- **Ad-hoc `query`/`mutate` are always exposed, Cedar-only** — no `mcp.allow_adhoc` switch. [Open Q3 → resolved: always-on + Cedar.]

Annotations (rmcp defaults `destructiveHint` **and** `openWorldHint` to **true**, so set them explicitly via `ToolAnnotations::new().read_only(b).destructive(b).open_world(b)`): read tools → `read_only(true).open_world(false)`; `mutate`/`ingest`/`branches_delete`/`branches_merge`/`schema_apply` → `read_only(false).destructive(true).open_world(false)`; `branches_create` (additive) → `read_only(false).destructive(false).open_world(false)`.

### B.6 Stored-query tools

One MCP tool per `mcp.expose` registry entry (named by its `tool_name`), projected from the **same `api::query_catalog_entry`** the `GET /queries` catalog uses; parameters → JSON Schema per B.7. The outer gate is the **coarse `InvokeQuery`** action (all exposed queries on the graph, or none — per-query scope is deferred, see B.13); the call then runs the registry source through `run_query` / `run_mutate`, whose inner `Read` / `Change` gate the body — the **double-gate of `POST /queries/{name}`**. Skip a stored tool whose name collides with a built-in (built-ins win, so the catalog never has a duplicate tool name). Dispatch reads the query params from `args.params` and the knobs from `args.branch` / `args.snapshot` (B.7's nested envelope), so a query parameter named `branch`/`snapshot` cannot be shadowed.

### B.7 `ParamKind` → JSON Schema (stored-query params)

| `ParamKind` | JSON Schema |
|---|---|
| String / Bool / Int / Float | `{"type":"string"}` / `{"type":"boolean"}` / `{"type":"integer"}` / `{"type":"number"}` |
| BigInt (i64/u64) | `{"type":"string","pattern":"^-?\\d+$"}` (JSON numbers lose precision >2⁵³) |
| Date / DateTime | `{"type":"string","format":"date"}` / `{"type":"string","format":"date-time"}` |
| Blob | `{"type":"string","contentEncoding":"base64"}` |
| Vector | `{"type":"array","items":{"type":"number"},"minItems":dim,"maxItems":dim}` — **`dim` is intrinsic to the kind** (`ParamKind::Vector { dim: u32 }`, never an `Option`), so a dimensionless vector is unrepresentable and the `unwrap_or(0)` footgun (a 0-length-array schema that rejects all input) cannot exist; if a `dim` is ever genuinely unknown, **omit** `minItems`/`maxItems` rather than emit `0`. |
| List | `{"type":"array","items":<item_kind schema>}` (scalar items only) |

**Envelope (correct by construction).** The tool's `input_schema` **nests the query params under a `params` object**, mirroring the `POST /queries/{name}` request: `{ "params": { <the query's typed params> }, "branch"?: string, "snapshot"?: string }`. The invocation knobs and the query's own parameters then live in **separate namespaces and cannot collide** — a query param named `branch`/`snapshot` is harmless. `nullable == false` params go in the inner `params.required`. For a **mutation** tool, omit `snapshot` from the schema and set `additionalProperties: false`, so "mutation against a snapshot" is *unrepresentable* (the REST path `400`s it; the MCP schema makes it impossible). Fold `instruction` into the description.

### B.8 `list` / `call` semantics

- **`list_tools` / `list_resources`** are Cedar-filtered. For each tool/resource, run the **same authorization the call path runs, with default args** (the default branch `main`) — **not** a separate `branch: None` probe (a `branch: None` request matches no `branch_scope` rule, so it would *hide* tools the actor can actually call on a scoped branch). Sharing the authorization input makes list and call agree by construction for the common case. Emit only `Allowed`; an `Err` (operational) propagates as a JSON-RPC error; a `Denied` hides. Stored-query tools list as a group iff the coarse `InvokeQuery` is allowed.
- **`call_tool`:** an unknown tool **or** a denied tool returns the **identical** `unknown tool: <name>` (`-32602`) so the catalog can't be probed without the grant. Route **every** `do_*`/`run_*` result through **one canonical `classify(Result<_, ApiError>) -> Result<CallToolResult, McpError>`** — the single source of truth, used at every dispatch site so the mapping can't drift: `Ok` → success JSON content; `Err` 4xx/409 → `CallToolResult { isError: true }` (the model self-corrects, per the 2025-11-25 SEP-1303 split); `Err` operational 5xx → JSON-RPC error. (Do not keep two mappers — a single `classify` replaces the `api_error_to_tool` / `api_operational_error` split that drifted in the reference impl.) A missing/bad bearer is an HTTP `401` at the boundary *before* rmcp.
- **Branch-scope residual (R7):** because visibility uses the default branch, a grant scoped to a *non-default* branch is an **over-show** — the tool lists and `call_tool` is the authoritative gate. Over-showing is the safe direction (the agent sees the tool, tries it, gets a clear deny); under-showing a callable tool is the failure mode the default-branch probe avoids.

### B.9 Resources

Two resources: `omnigraph://schema` (`Read` → `do_schema_get`) and `omnigraph://branches` (`Read` → `do_branches_list`, JSON text). (No `omnigraph://graphs` — server-scoped, dropped with `graphs_list`; see B.10.) `list_resources`/`read_resource` Cedar-filtered + masked exactly like tools. Advertise the `resources` capability only because both handlers are backed (don't advertise a capability whose `read` would 404).

### B.10 Routing (RESOLVED)

`/mcp` lives in the `per_graph_protected` route group: single mode → `POST /mcp`; multi mode → `POST /graphs/{graph_id}/mcp` (per-graph isolation; consistent with the `/graphs/{id}/...` REST cluster routing). [Open Q5 → resolved: per-graph, final.]

**Decided: the MCP surface has no server-scoped tools or resources.** `graphs_list` and `omnigraph://graphs` are **dropped from MCP** — graph discovery is a REST/admin concern, served by `GET /graphs`. Every MCP tool/resource is graph-scoped, the per-graph `/mcp` is fully clean, and there is no flat server-level `/mcp`. (If a concrete need to enumerate graphs *over MCP* ever arises, add a flat server-level `POST /mcp` in the `management` group — bearer-only, no graph handle, server-scoped tools only — but do not build it speculatively.)

**Do not** consolidate to a single flat `/mcp` that takes `graph_id` per call: MCP's `tools/list` cannot carry a graph, so it can't list per-graph stored-query tools; it also breaks isolation, pollutes every tool's `input_schema`, and diverges from the URL-scoped REST routing.

### B.11 Auth (decoupled; OAuth is a committed fast-follow)

The handler consumes an already-resolved `ResolvedActor` and **branches on nothing** about how the token was verified (§5.8). Static bearer works **today** with the developer clients; the consumer connectors need OAuth, a planned additive layer that changes zero MCP code (it only swaps the bearer middleware behind a `TokenVerifier`, and serves RFC 9728 metadata).

| Integration | Static bearer (this surface) | Note |
|---|---|---|
| Claude Code, Cursor, VS Code | ✅ | `claude mcp add --transport http <url>/mcp --header "Authorization: Bearer <tok>"` |
| Claude Messages API MCP connector | ✅ | caller passes `authorization_token` → `Authorization: Bearer` |
| claude.ai web / Claude Desktop connectors | ❌ needs OAuth fast-follow | requires OAuth 2.1 + PKCE (S256) + RFC 9728 + DCR/CIMD/custom client id+secret |
| ChatGPT developer-mode connectors | ❌ needs OAuth fast-follow | OAuth 2.1 (CIMD/DCR/PKCE) or "no auth"; no static-bearer mode |

OAuth fast-follow (MR-956): serve `/.well-known/oauth-protected-resource` + `WWW-Authenticate` on 401, front a managed AS (WorkOS AuthKit by default) that supports DCR + PKCE, validate audience-bound JWTs offline → `ResolvedActor`. Keep it **config-gated/dual-mode** so a server that does not advertise OAuth lets the dev clients keep using the static `Authorization` header (avoids the Claude Code header-vs-OAuth conflict).

### B.12 Tests & verification

- **Protocol:** `initialize` handshake + advertised `{tools, resources}` caps; `tools/list` shape; `tools/call` happy path; JSON-RPC errors (`-32601`/`-32602`); `resources/list` + `resources/read`; `GET /mcp` → 405; `MCP-Protocol-Version` 400/default; `Origin` → 403.
- **Cedar (coarse):** a read-only actor sees the read tools but not `mutate`/`ingest`/`branches_*`/`schema_apply`; a denied `tools/call` masks byte-identically to an unknown one; stored queries listed only with `invoke_query`; the double-gate (an `invoke_query`-only actor sees a stored tool but the call surfaces `isError` when the inner `read` denies).
- **Dispatch:** a `mutate` call writes end-to-end (proves the actor/handle extension passthrough); a malformed query → `isError:true`, not a JSON-RPC error.
- **Resources:** list + read of `schema`/`branches`; a denied read masks as unknown.
- **Auth decoupling / no-bearer:** `/mcp` 401s without a bearer (before rmcp) and 200s with one; the suite is green under the static-hash verifier (and a mock `ResolvedActor` source proves verifier-agnosticism).
- **Crate-level:** a tiny `omnigraph-mcp/tests/` with a trivial `McpBackend` impl serving `initialize` + `GET→405` proves the crate stands alone; add an rmcp surface-guard there pinning `StreamableHttpServerConfig` field names + the `ServerHandler` method shapes.
- **Verification commands:** `cargo build --workspace --locked`; `cargo tree -p omnigraph-server -e normal | grep rmcp` shows rmcp **only** transitively under `omnigraph-mcp`; `cargo test -p omnigraph-server --test server` (incl. the `mcp_*` cases, black-box over `build_app`) + `--test openapi` (no `/mcp` leak — it carries no `#[utoipa::path]`); live smoke: run the server with a bearer + policy, `curl` `initialize`/`tools/list`/`tools/call`/`GET→405`.

### B.13 Decisions locked

- **rmcp 1.7** (not hand-rolled) — verified to integrate on edition 2024. [Open Q2 → resolved.]
- **Coarse `invoke_query` only**; per-query scope deferred (PR 0b — adds a query-name dimension to `PolicyRequest` + the Cedar schema). [The headline caveat.]
- **Ad-hoc `query`/`mutate` always exposed, Cedar-only**; no `mcp.allow_adhoc`. [Open Q3 → resolved.]
- **`query`/`mutate` ids only**, no `read`/`change` aliases. [Open Q7 → resolved.]
- **Per-graph `/mcp` routing**; `graphs_list`/`omnigraph://graphs` **dropped from MCP** (graph discovery via REST `GET /graphs`); no server-scoped MCP tools. [Open Q5 → resolved.]
- **text-JSON `content` for v1**; `structuredContent`/`outputSchema` deferred. [Open Q4 → resolved.]
- **BigInt as JSON string.** [Open Q1 → resolved.]
- **Static bearer now, OAuth/RFC-9728 fast-follow.**

## Relationship to RFC-001

[rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md) (MR-656 / MR-976 / MR-969) is the parent design for stored queries + the response envelope + MCP. This RFC is the **detailed MCP-transport design** that #128 left for a follow-up, and it **revises rfc-001 in three places where the shipped code or the MCP wire protocol diverged from rfc-001's sketch**:

1. **Transport shape.** rfc-001 sketched `GET /mcp/tools` + `POST /mcp/invoke` (a bespoke REST pair). **That is not the MCP wire protocol — real MCP clients cannot connect to it.** This RFC implements actual MCP JSON-RPC over Streamable HTTP and reuses `query_catalog_entry` as a *projection source*, not a parallel surface. (rfc-001's own Open Question already leaned toward Streamable HTTP.)
2. **Exposure config.** rfc-001 specified inline `.gq` pragmas (`@mcp(expose=…)`, default `expose=false`). **#128 shipped a different mechanism:** YAML `queries.<name>.mcp.expose` in `omnigraph.yaml`, **default `true`** (declaring a query in the manifest *is* the opt-in). This RFC builds on the shipped YAML form; the `.gq`-pragma design in rfc-001 is superseded for exposure.
3. **Schema introspection.** rfc-001 lists "Schema introspection through MCP" as a **non-goal** ("agents see types through declared return shapes"). This RFC **revises that**: the operational-parity tools include `schema_get` and `omnigraph://schema` — *because the shipped stdio package already exposes both*. The non-goal is achieved by *policy*, not omission: `schema_get`/`omnigraph://schema` are Cedar-gated by `Read`, and the recommended locked-down agent policy denies `Read`, so a curated agent still never sees the schema. (rfc-001's intent is preserved; the mechanism moves from "don't build it" to "build it, gate it.")

Everything else in rfc-001 (two-paths-one-engine, per-query `invoke_query` *as the intended scope*, the response envelope, multi-graph per-graph endpoints) this RFC consumes unchanged.

> **Numbering note:** the `TokenVerifier`/WorkOS auth design is referred to in code (`crates/omnigraph-server/src/identity.rs`) as "RFC 0001," which is a *different* document from this repo's `docs/dev/rfc-001-queries-envelope-mcp.md`. To avoid the collision this RFC cites the auth substrate as **MR-956** throughout, never "RFC 0001."

## Reconciliation with shipped code (historical — pre-MCP, against #128 HEAD)

> *Historical: this was the gap analysis against `#128` (the stored-query REST foundation) before the MCP surface was built. The three ❌ items below — the MCP protocol surface, and the `TokenVerifier` — were subsequently built/addressed in [#157](https://github.com/ModernRelay/omnigraph/pull/157) (transport, tools, resources) except per-query scope and OAuth, which remain deferred. For the current build instructions see the [Implementation Blueprint](#implementation-blueprint-canonical--build-the-fresh-implementation-from-this).*

Verified against `crates/omnigraph-server/src/{lib.rs,api.rs}` and `crates/omnigraph-policy/src/lib.rs` at the `#128` branch head:

- ✅ `GET /queries` returns the `mcp.expose == true` subset as `QueriesCatalogOutput { queries: [QueryCatalogEntry] }`, each with typed `ParamDescriptor`s, `tool_name`, `description`, `instruction`, and a `mutation` flag. **MCP-ready projection, but exposed as bespoke REST/JSON — not the MCP wire protocol.**
- ✅ `POST /queries/{name}` route exists (`server_invoke_query`, `lib.rs`).
- ✅ `query_catalog_entry()` / `param_descriptor()` with an exhaustive `ScalarType → ParamKind` map (a new scalar is a compile error).
- ✅ `InvokeQuery` Cedar action defined in `omnigraph-policy`.
- ✅ **`InvokeQuery` IS enforced** at `POST /queries/{name}`: `server_invoke_query` calls `authorize(PolicyAction::InvokeQuery)` and **masks a denial to a 404 identical to "unknown query"** so the catalog isn't probeable (the denial-masking the previous draft of this RFC reported as missing is shipped — it lives in `lib.rs`, not `api.rs`). The stored-mutation path is already double-gated: `InvokeQuery` outer, then `Change` inside `run_mutate`.
- ✅ **Reuse path exists:** `run_query` / `run_mutate` are already decoupled from their HTTP request bodies and take registry-supplied `(source, name, params, branch/snapshot)`. MCP `tools/call` for both stored and ad-hoc tools delegates to these — no new business logic.
- ❌ **Per-query (`invoke_query[name]`) scope is NOT implemented.** `PolicyRequest` carries only `{action, branch, target_branch}` — **no query-name dimension** — and the action is documented coarse ("permits *any* stored query on the graph"). rfc-001 *designed* per-name scope; it is unbuilt. This RFC's per-query Cedar filtering (§5.4) and recommended agent policy (§5.9) depend on it → tracked as **PR 0b**.
- ❌ No MCP protocol surface (`initialize`/`tools/list`/`tools/call`, JSON-RPC, transport).
- ❌ No `TokenVerifier` trait yet — `require_bearer_auth` resolves a `ResolvedActor` inline (static-hash). The trait/`OidcJwtVerifier` are MR-956 (draft). The MCP layer's only requirement — *consume `ResolvedActor`* — is satisfiable today.

Stack (verified `Cargo.toml`): Axum + utoipa (OpenAPI) + `omnigraph-policy` (Cedar) + `futures` + `tokio`. **No MCP crate present.** `edition = "2024"`.

## Motivation

- **One curated, safe, remotely-reachable tool surface.** MR-969's thesis: hand an LLM a token Cedar-scoped to a set of tools and it sees exactly those typed tools — cannot construct ad-hoc queries it isn't permitted, cannot read the schema it isn't permitted, cannot reach other graphs. Today the only MCP is the stdio package: local-only, full surface, ungated.
- **Parity, so the in-server MCP can be the single implementation.** Operators/agents already depend on the operational tools. Supporting them server-side behind one Cedar gate lets the stdio package degrade to a proxy and removes two diverging tool sets.
- **On-prem and cloud from one endpoint.** A managed cloud (WorkOS OAuth) and an on-prem/air-gapped deploy (static or customer-OIDC tokens) must serve the same MCP without forks or MCP-specific auth.
- **Foundation for the agent on-ramp (MR-974).** `omnigraph mcp install --agent <tool>` needs a decided transport + a stable endpoint.

## Goals

- Project built-in tools + stored queries as MCP tools through **one** registry abstraction.
- `tools/list` and the callable set are **identical for argument-independent authorization**, both driven by Cedar (see §5.4 for the branch-scoped caveat).
- The MCP layer is **auth-method-agnostic**: it consumes `ResolvedActor`, never a raw token, never branches on how auth happened.
- The same endpoint works on-prem (static/OIDC) and cloud (WorkOS OAuth), switched by config; cloud OAuth is additive (RFC 9728).
- No new business logic: MCP tools delegate to the same `run_query`/`run_mutate`/branch/schema functions the HTTP routes call.
- Behaviour-neutral when unused: no MCP traffic = no change.

## Non-Goals

- **Building/hosting an OAuth authorization server.** The server is a Resource Server; WorkOS AuthKit+Connect is the AS (MR-956). The MCP endpoint validates tokens, never issues them, never holds client secrets.
- **OAuth/WorkOS implementation itself** — MR-956's work. This RFC leaves a clean RFC-9728 hook and consumes `ResolvedActor`.
- **MCP prompts, elicitation, `tools/list_changed`, resource subscriptions, server-initiated messages.** None needed → enables a stateless POST-only transport (§5.6).
- **stdio transport inside the server.** stdio stays in the TS package (now a proxy).
- **Cross-graph tool listing.** Per-graph catalogs only (MR-969 + RFC-002 non-goal).
- **Hot reload of the query registry.** Restart-only (MR-969).

## Background

`omnigraph-server` (Axum) already implements every operation this RFC exposes as an authenticated HTTP route; each authorizes via a `PolicyAction` against the Cedar policy for a server-resolved actor and calls into the engine. The existing stdio MCP package is a *client* of these routes (it owns no business logic). MR-956 will introduce a `TokenVerifier` trait (`StaticHashTokenVerifier` today inline, `OidcJwtVerifier` for OIDC/WorkOS) producing the `ResolvedActor { actor_id, tenant_id: Option, scopes: Vec<Scope>, source }` that already exists in `identity.rs` and is consumed by Cedar — token *validation* is offline (cached JWKS), so on-prem/air-gapped has no request-path dependency on the cloud.

## Design

> *§5 is the original design sketch (design rationale). Where it differs from the [Implementation Blueprint](#implementation-blueprint-canonical--build-the-fresh-implementation-from-this) above, the Blueprint is authoritative. Notable divergences proven out by [#157](https://github.com/ModernRelay/omnigraph/pull/157): the §5.1 per-tool `McpTool` trait became a `Builtin` enum + an `McpBackend` crate trait (B.1–B.4); §5.6's rmcp-vs-hand-roll is resolved to rmcp 1.7 (B.3); §5.7's "server tools on a per-graph endpoint" is resolved in B.10; the §5.2 `read`/`change` aliases are dropped (B.5).*

### 5.1 One tool model: a `McpTool` trait, two populators

Both built-in and stored-query tools implement one trait so `tools/list` / `tools/call` never special-case:

```rust
trait McpTool: Send + Sync {
    fn name(&self) -> &str;                       // MCP tool id (stable)
    fn title(&self) -> Option<&str>;
    fn description(&self) -> &str;
    fn input_schema(&self) -> serde_json::Value;  // JSON Schema (draft 2020-12)
    fn annotations(&self) -> ToolAnnotations;     // readOnlyHint / destructiveHint / idempotentHint
    /// The Cedar request(s) this call requires, given parsed args. Used BOTH at
    /// list-time (dry-run filter, default args) and call-time (enforce, real args).
    fn authorization(&self, args: &ToolArgs) -> Vec<PolicyRequest>;
    async fn call(&self, ctx: &GraphCtx, args: ToolArgs) -> Result<ToolOutput, ToolError>;
}
```

- **Built-ins**: ~14 static impls, each delegating to the *same* function its HTTP route calls (`run_query`, `run_mutate`, branch ops, `apply_schema_as`, …). `input_schema` authored once (or derived from each route's existing `utoipa`/`ToSchema` DTO).
- **Stored queries**: generated `McpTool` instances, one per `mcp.expose` entry; `input_schema` from `param_descriptor` (§5.3); `authorization` → `InvokeQuery` (coarse today; `InvokeQuery{name}` after PR 0b) then the inner `Read`/`Change`.

`ToolRegistry` for a graph = the static built-ins + the dynamic stored-query tools resolved from that graph's `GraphHandle` registry.

### 5.2 Tool catalog (parity) and Cedar mapping

Each built-in **reuses the exact `PolicyAction` its HTTP route already enforces** — verified against the handlers in `lib.rs`, not invented:

| MCP tool | Scope | Read/Mutate | Cedar action (verified from route) |
|---|---|---|---|
| `health` | server | read | none (liveness/version) |
| `graphs_list` *(new)* | server | read | `GraphList` |
| `snapshot` | graph | read | `Read` |
| `schema_get` | graph | read | `Read` |
| `branches_list` | graph | read | `Read` |
| `commits_list`, `commits_get` | graph | read | `Read` |
| `read` (ad-hoc `.gq`) / `query` *(alias)* | graph | read | `Read` |
| `change` (ad-hoc `.gq`) / `mutate` *(alias)* | graph | mutate | `Change` |
| `ingest` (NDJSON) | graph | mutate | `Change` (+ `BranchCreate` when forking a new branch) |
| `branches_create` | graph | mutate | `BranchCreate` |
| `branches_delete` | graph | mutate | `BranchDelete` |
| `branches_merge` | graph | mutate | `BranchMerge` |
| `schema_apply` (`allow_data_loss`) | graph | mutate | `SchemaApply` |
| **stored query** (`find_user`, …) | graph | inferred | `InvokeQuery` (coarse; `InvokeQuery{name}` after PR 0b) + inner `Read`/`Change` |

There is **no `Ingest` and no separate `snapshot`/`Export` action** — `ingest` enforces `Change`, `snapshot` enforces `Read`. (`Export` exists but maps to the `/export` route, which this RFC does not expose as a tool.)

**Tool id parity vs. canonicalization.** The shipped stdio package uses tool ids **`read`/`change`** (and calls the deprecated `/read`,`/change` routes). The server HTTP surface canonicalized to `/query`,`/mutate` with `/read`,`/change` deprecated (MR-656). To keep existing package clients working *and* align with the server, the MCP exposes **`query`/`mutate` as canonical with `read`/`change` retained as deprecated-but-live aliases** (both dispatch to the same handler). Open Q7 asks whether to drop the aliases later.

Resources (§5.5): `omnigraph://schema`, `omnigraph://branches` (parity), plus `omnigraph://graphs` *(new)* — each gated by the same action as its list/get route (`Read`, `Read`, `GraphList`).

### 5.3 `ParamDescriptor → JSON Schema` (stored-query tools)

| `ParamKind` | JSON Schema | Notes |
|---|---|---|
| String | `{"type":"string"}` | |
| Bool | `{"type":"boolean"}` | |
| Int (i32/u32) | `{"type":"integer"}` | |
| BigInt (i64/u64) | `{"type":"string","pattern":"^-?\\d+$"}` | JSON numbers lose precision >2⁵³ → string (matches the shipped `api.rs` rationale). (Open Q1) |
| Float (f32/f64) | `{"type":"number"}` | |
| Date | `{"type":"string","format":"date"}` | |
| DateTime | `{"type":"string","format":"date-time"}` | |
| Blob | `{"type":"string","contentEncoding":"base64"}` | |
| Vector | `{"type":"array","items":{"type":"number"},"minItems":dim,"maxItems":dim}` | uses `vector_dim` |
| List | `{"type":"array","items":<item_kind schema>}` | scalar items only (grammar guarantees) |

`nullable == false` → param is in `required`. Annotations: `mutation` → `{readOnlyHint:false, destructiveHint:true}`; else `{readOnlyHint:true}`. `description` → tool description; `instruction` → appended to description (or `_meta`). (The shipped `check()` already warns when an `mcp.expose` query declares a `Vector` param an LLM can't supply.)

For built-in tools the schema is hand-authored from the route DTO; e.g. `query` → `{source: string, branch?: string, params?: object}`; `schema_apply` → `{schema: string, allow_data_loss?: boolean}`; `ingest` → `{ndjson: string, mode?: "merge"|"append"|"overwrite", branch?: string}`.

### 5.4 `tools/list` (Cedar-filtered) and `tools/call` (dispatch + masking)

- **`tools/list`**: build the `ToolRegistry`; for each tool evaluate `authorization(default_args)` against the actor's Cedar policy; **emit only tools that authorize**. Authz decisions memoized per request. Stored-query tools additionally require `mcp.expose: true`.
  - **Exactness caveat (R7 is conditional):** the listed set equals the callable set **only for tools whose authorization is argument-independent** (`health`, `graphs_list`, `snapshot`, `schema_get`, `branches_list`, `commits_*`, ad-hoc `query`/`mutate`, and stored queries under the *coarse* action). For **branch-scoped tools** (`branches_create`/`merge` with `target_branch_scope`, and any branch-scoped `Read`/`Change` rule), list-time uses `default_args` (e.g. branch `main`) and cannot know the real target, so the listed set is a *best-effort approximation* of callability — a call may still be denied (or, rarely, a hidden tool would have been allowed). `tools/call` is always the authoritative gate. The contract is: **list never shows a tool the actor can't ever call; for branch-scoped tools it may show one the actor can call only on some branches.**
- **`tools/call`**: resolve `name` → `McpTool` (masked-404 if unknown *or* `mcp.expose:false`); parse+validate args against `input_schema`; enforce `authorization(args)` (mutations stay double-gated: `InvokeQuery` then `Change`); on success `call`. **Denial masking** lives in one place (the dispatcher): an authz denial is returned identically to "unknown tool" (§5.10), reusing the same deny≡missing principle already shipped at `POST /queries/{name}`.

### 5.5 Resources

Advertise `resources` capability (`subscribe:false, listChanged:false`). `resources/list` → the URIs the actor may read; `resources/read` → schema `.pg` text / branches JSON / (multi-graph) graphs JSON, each gated by the corresponding action (`Read`, `Read`, `GraphList`). A locked-down agent denied `Read` simply never sees `omnigraph://schema` or `omnigraph://branches` — this is how rfc-001's "agents don't introspect schema" intent is met *by policy* (§Relationship-to-RFC-001).

### 5.6 Transport: Streamable HTTP, stateless, POST-only

- **Streamable HTTP** (MCP's current standard; we're already an HTTP server). One endpoint per scope (§5.7).
- Because the server emits **no** server-initiated messages, implement the **minimal conformant** shape: client `POST`s JSON-RPC, server replies `application/json`. **No SSE channel, no `Mcp-Session-Id`, stateless** — each request authenticated independently via the bearer middleware. Honour the `MCP-Protocol-Version` header. SSE/sessions can be added later if subscriptions land.
- **JSON-RPC methods:** `initialize` (advertise `{tools:{listChanged:false}, resources:{listChanged:false, subscribe:false}}` + serverInfo/version), `notifications/initialized` (no-op ack), `ping`, `tools/list`, `tools/call`, `resources/list`, `resources/read`. `prompts/list` returns empty if probed.
- **Library decision (Open Q2):** spike `rmcp` (official Rust MCP SDK) for conformance + Streamable-HTTP/Axum on edition 2024; **fall back to a hand-rolled ~150 LOC JSON-RPC-over-POST** (only the methods above) on friction. Given the tiny surface, hand-roll is an acceptable default.

### 5.7 Endpoint routing (server- vs graph-scoped)

- **Single-graph mode:** `POST /mcp` — graph tools + server tools (`health`, `graphs_list`).
- **Multi-graph mode (MR-668):** `POST /graphs/{graph_id}/mcp` — graph-scoped tools for that graph; plus a server-level `POST /mcp` exposing only server-scoped tools (`health`, `graphs_list`). A per-graph endpoint never lists another graph's tools (isolation, tested). Mirrors the shipped `/graphs/{graph_id}/…` cluster routing. (Open Q5: confirm naming + whether server tools also appear on the per-graph endpoint.)

### 5.8 Modular / decoupled auth (the cross-cutting requirement)

**Invariant (load-bearing, satisfiable today):** the MCP handler receives an **already-resolved `ResolvedActor`** and **branches on nothing** about how the token was verified. No token parsing, no method check, no OAuth inside the MCP module. Today that actor comes from `require_bearer_auth`; when MR-956 lands it comes from a `TokenVerifier` — the MCP code is identical either way.

```
request → [auth middleware: ResolvedActor] → [MCP route] → Cedar → McpTool
```

**Server side — auth is config, not code:**

| Deployment | Verifier | MCP change |
|---|---|---|
| On-prem, static bearer | `require_bearer_auth` / `StaticHashTokenVerifier` | none |
| On-prem, customer IdP | `OidcJwtVerifier` → customer issuer (MR-956) | none |
| Our cloud | `OidcJwtVerifier` → WorkOS, `tenant_id = Some(org_id)` (MR-956) | none |

Token validation is offline (cached JWKS) — on-prem/air-gapped keeps working with no request-path cloud dependency. The MCP endpoint never terminates OAuth and never holds a client secret (Resource Server only).

**Cloud client negotiation — additive, no MCP changes:** when MR-956 lands, the server publishes RFC 9728 `/.well-known/oauth-protected-resource` and returns `WWW-Authenticate: Bearer ..., resource_metadata="..."` on 401. A compliant MCP client (Claude) then auto-negotiates: static bearer to an on-prem endpoint; on a cloud 401 it discovers the WorkOS AS and runs OAuth/PKCE itself — **same endpoint URL, zero client-side branching.** This RFC only requires that MCP routes flow through the standard 401 path so that hook can be added later without touching MCP.

**Multi-user identity pass-through (cloud):** the *caller's* token (a WorkOS JWT, audience-bound per-tenant) must reach the server so Cedar enforces per-user/per-tenant policy — never a shared service token. The MCP endpoint validates it offline and maps `org_id → tenant_id`. This is why the **remote path is the in-server HTTP MCP that Claude connects to directly** (its token flows through), not a stdio bridge impersonating a user.

**Client-side credential acquisition (CLI/SDK/proxy) — pluggable `CredentialSource`** (RFC-002 §5, MR-971), keyed by server name, so OAuth is a future *sibling key*, not a re-key:

```yaml
servers:
  onprem: { endpoint: https://og.internal:8080, auth: { token: { env: OG_TOKEN } } }
  edge:   { endpoint: https://og-edge,          auth: { token: { command: [vault, read, -field=token, secret/og] } } }
  cloud:  { endpoint: https://api.omnigraph.cloud, auth: { oauth: { issuer: workos } } }   # future sibling
```

Implicit chain when `auth:` omitted: `OMNIGRAPH_TOKEN_<NAME>` → keychain `omnigraph:<name>` → `[<name>]` in `~/.omnigraph/credentials`; legacy `bearer_token_env` honoured. Secrets never inlined.

### 5.9 Safety model — Cedar is the gate, default-deny is the floor

With ad-hoc `query`/`mutate`/`schema_apply` present as tools, the **only** thing protecting an untrusted agent is the Cedar policy. Therefore:

- **Default-deny when tokens are configured** (MR-723, shipped) is the floor — an actor with no grants sees an empty tool list.
- **What works today (coarse action):** a policy can hide all ad-hoc tools and admin tools per-actor (`deny Read, Change, SchemaApply, Branch*`) while allowing stored queries (`allow InvokeQuery`). That already reproduces "can't run ad-hoc, can't read schema, can only call stored queries" — the agent sees *every* exposed stored query plus nothing else.
- **What needs PR 0b (per-query scope):** selecting *which* stored queries an actor may call (`allow InvokeQuery [find_user, list_orders]`, deny the rest). The shipped `invoke_query` is coarse (all stored queries or none). Until PR 0b adds a query-name dimension to `PolicyRequest` + the Cedar schema (rfc-001's intended design), per-actor sub-selection of stored queries is **not expressible**; curation is graph-level (which `.gq` files are registered + `mcp.expose`).
- `schema_apply`, `branches_delete`, ad-hoc `mutate` require an explicit admin-tier grant; never in a default agent policy.
- (Open Q3) Optional `mcp.allow_adhoc` server switch defaulting **off** for the ad-hoc `query`/`mutate` tools — defence-in-depth independent of Cedar, and independent of PR 0b.

### 5.10 Result shaping and error mapping

- **Success:** `tools/call` returns `content: [{type:"text", text:<json>}]` where `<json>` is the route's existing output envelope (read rows / mutation summary, i.e. `ReadOutput` / `ChangeOutput`). (Open Q4: also emit `structuredContent` + `outputSchema` — defer; text-JSON for v1.)
- **Tool execution error** (bad params after schema validation, engine error): result with `isError:true` + a text content block.
- **Authorization denial / unknown tool / `mcp.expose:false`:** a single JSON-RPC error (`-32602`, message `"unknown tool"`) — identical for all three so policy isn't probeable (same principle as the shipped `POST /queries/{name}` 404 masking).
- **Auth failure** (bad/absent bearer): HTTP 401 from the middleware *before* MCP — carries `WWW-Authenticate` (the RFC 9728 hook), never masked as a tool error. (This is exactly the path the shipped `authorize`/`authorize_request` split preserves: operational failures keep their status; only *denials* are masked.)

## Relationship to the `@modernrelay/omnigraph-mcp` stdio package

Surface of the package (`omnigraph-ts`, `@modelcontextprotocol/sdk@^1.29.0`, **stdio only**). *Figures refreshed 2026-06: the package re-synced to the engine in [omnigraph-ts#11](https://github.com/ModernRelay/omnigraph-ts/pull/11) and is now at `0.6.1` — not the `0.3.0` this RFC was first drafted against.* It exposes **16 tools** (`health`, `snapshot`, `query`, `read`, `schema_get`, `branches_list`, `graphs_list`, `commits_list`, `commits_get`, `mutate`, `change`, `ingest`, `branches_create`, `branches_delete`, `branches_merge`, `schema_apply` — note it already canonicalized `query`/`mutate` with `read`/`change` as deprecated aliases, and added `graphs_list`) and **~9 resources** (`omnigraph://schema`, `omnigraph://branches`, `omnigraph://graphs`, plus a vendored `omnigraph://best-practices/*` cookbook). It is a thin client over the SDK → HTTP routes and **forwards the caller's bearer verbatim** (no inspection).

Once parity lands, **collapse to one implementation**: the in-server MCP is canonical (Cedar-gated, remote-capable, the path that becomes a Claude-web connector via MR-956). The stdio package degrades to a **thin stdio↔HTTP proxy** forwarding JSON-RPC (and the incoming `Authorization`) to `/mcp` — staying the local on-ramp for Claude Code/Desktop while sharing one tool set, one Cedar gate. Transition: keep the current independent stdio package on its `0.6.x` line; ship proxy mode in a later TS minor once the server endpoint is GA. (The package already re-synced to `0.6.1` in [omnigraph-ts#11](https://github.com/ModernRelay/omnigraph-ts/pull/11); its client-side stored-query-tools attempt, [omnigraph-ts#7](https://github.com/ModernRelay/omnigraph-ts/pull/7), was **closed** in favor of this server-side surface.)

## Testing

> *Superseded by [B.12](#b12-tests--verification), which is the authoritative test plan. The list below is the original §5-era sketch; ignore its references to `graphs_list`, `read`/`change` aliases, and the per-query `invoke_query` test (all resolved/dropped — see B.13).*

- **Protocol conformance:** `initialize` handshake + advertised capabilities; `tools/list` shape; `tools/call` happy path; JSON-RPC error envelopes (`-32601` unknown method, `-32602` invalid params / unknown tool); `resources/list` + `resources/read`.
- **Cedar filtering (coarse, today):** an actor with `allow InvokeQuery` + `deny Read/Change` sees *all* exposed stored queries but **not** `query`/`mutate`/`schema_get`; `tools/call query` returns masked "unknown tool"; an admin sees the full catalog.
- **Cedar filtering (per-query, gated on PR 0b):** actor scoped to `InvokeQuery [find_user]` sees *only* `find_user`; `tools/call list_orders` masks. **This test ships with PR 0b**, not PR 1 — it cannot pass against the coarse action.
- **Parity per built-in:** each tool round-trips against the same expectations as its HTTP route (reuse route tests); `read`/`change` aliases dispatch identically to `query`/`mutate`.
- **Double-gating:** a stored mutation requires both `InvokeQuery` and `Change`; `schema_apply` requires `SchemaApply`.
- **`mcp.expose:false`:** absent from `GET /queries` and MCP `tools/list`; still service-callable by name through `POST /queries/{name}` when the actor has `invoke_query`, but not MCP-callable.
- **Schema generation:** table-driven over every `ParamKind` incl. nullable / list / vector(dim).
- **Branch-scoped list approximation:** assert the documented R7 caveat — a branch-scoped policy lists `branches_create`, and `tools/call` is the authoritative gate (a denied target still 403s/masks).
- **Multi-graph isolation:** `/graphs/a/mcp` never lists graph `b`'s tools; server `/mcp` exposes only server tools.
- **Auth decoupling:** the MCP suite is green under the current `require_bearer_auth` and under a mock OIDC `ResolvedActor` source — proving verifier-agnosticism. A 401 carries `WWW-Authenticate`.
- **OpenAPI:** the JSON-RPC endpoint is not REST — document only the envelope in utoipa (or exclude); keep `openapi.json` drift test green (`OMNIGRAPH_UPDATE_OPENAPI=1` to regenerate on intentional change).
- **Cross-repo smoke (optional):** point `@modelcontextprotocol/sdk` (TS) at the HTTP endpoint in an `omnigraph-ts` integration test.

## Rollout — phased by risk

> *Superseded by the Blueprint's build order ([B.4](#b4-server-side-backend-lives-in-omnigraph-server) → crate ([B.1](#b1-crate-architecture--dependency-direction)–[B.3](#b3-transport-lives-in-omnigraph-mcp)) → backend → tools/stored/resources). The PR phases below are the original §5-era plan; they still mention `graphs_list`, `read`/`change` aliases, and the `mcp.allow_adhoc` switch, all of which the locked decisions ([B.13](#b13-decisions-locked)) drop. PR 0b (per-query `invoke_query` scope) remains the one deferred follow-up.*

- **PR 0a — extract the reusable invoke path (small).** The coarse `invoke_query` gate + 404 denial-masking are **already shipped** in `server_invoke_query`. Extract the read/mutate dispatch into `invoke_stored_query(handle, name, params, branch/snapshot, actor)` so MCP `tools/call` and the HTTP route share one path. No behaviour change. *(Replaces the previous draft's "PR 0 — wire the gate", which was already done.)*
- **PR 0b — per-query `invoke_query` scope (the safety prerequisite).** Add a query-name dimension to `PolicyRequest` + the Cedar schema (rfc-001's intended design), wire it at `POST /queries/{name}` and in the stored-query `McpTool::authorization`. Independently useful (the `allow InvokeQuery [find_user]` policy). **Gates the per-query Cedar-filtering test and §5.9's recommended agent policy.**
- **PR 1 — MCP transport + read-only parity + stored-query reads.** Endpoint(s), `initialize`/`tools/list`/`tools/call`/`resources/*`, the `McpTool` registry, Cedar-filtered listing, the read-only built-ins (`health`, `graphs_list`, `snapshot`, `read`/`query`, `schema_get`, `branches_list`, `commits_*`) + resources + stored-query *reads*. All auth-agnostic.
- **PR 2 — mutating parity + stored-query mutations.** `change`/`mutate`, `ingest`, `branches_create/delete/merge`, `schema_apply`, stored-query mutations + the `mcp.allow_adhoc` switch.
- **PR 3 — docs + agent on-ramp hook.** `docs/user/server.md` MCP section (incl. the recommended agent policy + the coarse-vs-per-query caveat), `openapi.json` sync, the `omnigraph mcp install` config target (MR-974), and the downstream `omnigraph-ts` re-sync/proxy follow-up.
- **Later (separate, MR-956):** RFC 9728 protected-resource metadata + WorkOS — slots in with zero MCP changes.
- **Later (TS minor):** stdio package → proxy mode.

## Migration / backwards compatibility

- **Additive.** No `queries:` and no MCP traffic → today's behaviour unchanged. New endpoints are new routes.
- **Cedar default-deny** (when tokens configured) means MCP exposes nothing until an actor is granted — safe by default.
- The stdio package keeps working unchanged; proxy mode is opt-in later.
- `openapi.json` only gains the documented MCP envelope; existing REST routes untouched.

## Open Questions

> *Resolved during [#157](https://github.com/ModernRelay/omnigraph/pull/157) — see [B.13](#b13-decisions-locked) for the locked decisions. Q1→string, Q2→rmcp 1.7, Q3→always-on Cedar-only, Q4→text-JSON v1, Q5→per-graph routing (graphs_list per B.10), Q6→stateless POST confirmed, Q7→no `read`/`change` aliases. Q8 (PR 0b shape: Cedar resource vs context attribute) remains open, gated on the deferred per-query scope work. The items below are kept as the original decision context.*

1. **BigInt/u64 as JSON string** (recommended, precision-safe) vs number.
2. **`rmcp` vs hand-rolled** JSON-RPC (spike `rmcp` on edition 2024; default to hand-roll on friction).
3. **Default-off `mcp.allow_adhoc`** for ad-hoc `query`/`mutate` (recommended) vs always-on + Cedar-only.
4. **`structuredContent` + `outputSchema`** now vs text-JSON v1 (recommend v1 text-JSON).
5. **Endpoint paths:** `/mcp` + `/graphs/{id}/mcp` — confirm naming and whether server-scoped tools also appear on the per-graph endpoint.
6. **Stateless POST-only** confirmed (no near-term server-initiated messages) — revisit only if subscriptions land.
7. **Legacy alias tools** (`read`/`change`): keep for client compat (the shipped package uses them), or drop and rely on `query`/`mutate`?
8. **PR 0b shape:** per-query scope as a Cedar *resource* (`StoredQuery::"find_user"`) vs a `query_name` *context attribute* + policy condition — affects how `allow InvokeQuery [list]` is authored.
