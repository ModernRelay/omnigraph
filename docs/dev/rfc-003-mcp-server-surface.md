# RFC-003: MCP Server Surface for `omnigraph-server`

**Status:** Proposed — buildable implementation spec.
**Date:** 2026-06-13
**Audience:** server/engine maintainers.
**Tickets:** MR-969 (stored queries + MCP exposure), MR-956 (OAuth/RFC-9728 layer),
MR-971 (per-server credential resolver — landed as RFC-007), MR-974 (`omnigraph mcp install`).
**Builds on:** [rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md)
(stored queries + the response envelope), [rfc-005-server-cluster-boot.md](rfc-005-server-cluster-boot.md)
(multi-graph boot), [rfc-007-operator-config.md](rfc-007-operator-config.md)
(client credential model), [rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md)
(one-contract/two-implementations posture).
**Target release:** v0.8.x.

**Re-validated against main** 2026-06-16 (post RFC-009 `omnigraph-api-types`/`GraphClient`,
RFC-011 cluster-only server, RFC-009 canonical `POST /load`, RFC-012 embeddings): every
`file:line` and `Reuses` citation below was re-checked against the merged tree; the
deltas are folded in (cluster-only routing in [§15](#15-routing--reuses-build_app),
the DTO crate move in [§9](#9-stored-query-projection), `/ingest`→`/load`, and the
per-query `expose`/`tool_name` deferral in [§17](#17-decisions--rollout)).

**Validated against** (re-checked 2026-06-13): MCP protocol revision **`2025-11-25`**
(modelcontextprotocol.io), the official Rust SDK **`rmcp 1.7.0`** (crates.io /
github.com/modelcontextprotocol/rust-sdk), and current tool/security best practice
(Anthropic engineering, MCP spec security pages). Provider compatibility was
checked against the live docs of Claude Code/Desktop/web, the Claude Messages API
MCP connector, OpenAI's Responses API + ChatGPT connectors, Cursor, VS Code Copilot,
and OpenCode. Code snippets marked **`Reuses`** are present in `omnigraph-server`
today, cited to `file:line`; snippets marked **`New`** are the code to add.

---

## 1. Summary

Add a first-class **MCP (Model Context Protocol) server surface to
`omnigraph-server`**, served over **Streamable HTTP**, that projects the server's
operations as MCP **tools** and **resources** for LLM clients. Two tool populations
share one projection path:

1. **Built-in operational tools** — graph read/mutate, schema get/apply, branch
   create/delete/merge/list, commit list/get, NDJSON load, and a `health` liveness
   tool, plus resources `omnigraph://schema` and `omnigraph://branches`.
2. **Dynamic stored-query tools** — projected from the graph's loaded stored-query
   registry: either one typed tool per query (small catalogs) or a
   discovery + execute meta-tool pair (large catalogs) — see [§9](#9-stored-query-projection).

Every tool is **authorized by the server's existing Cedar policy engine**. The MCP
layer performs **no authentication of its own**: it consumes an already-resolved
actor identity from the server's bearer/OAuth middleware, so the **same endpoint
serves on-prem (static or customer-OIDC tokens) and cloud (OAuth 2.1) by
configuration only**. The transport is **stateless JSON over a single POST
endpoint** — the minimal conformant Streamable-HTTP shape, since the server emits no
server-initiated messages.

The surface is built so the existing local stdio MCP package can later collapse into
a thin stdio↔HTTP proxy over it, leaving one Cedar-gated, remotely-reachable tool
set ([§13](#13-provider-compatibility)).

## 2. Goals

- Project built-in tools **and** stored queries through **one** registry abstraction,
  so `tools/list` / `tools/call` never special-case a population.
- Make `tools/list` and the callable set agree for argument-independent authorization,
  both driven by Cedar; `tools/call` is always the authoritative gate.
- Keep the MCP layer **auth-method-agnostic**: it consumes a resolved actor, never a
  raw token, and never branches on how authentication happened.
- Add **no business logic**: tools delegate to the same engine functions the HTTP
  routes call.
- Be **code-mode-friendly** (typed schemas, structured output, stable names,
  progressive disclosure) and **maximally client-compatible** (Streamable HTTP +
  bearer today, OAuth 2.1 + RFC 9728 as an additive layer).
- Behaviour-neutral when unused: no MCP traffic ⇒ no change.

## 3. Non-Goals

- **Hosting an OAuth authorization server.** The server is a **Resource Server** only
  (validates tokens, never issues them, never holds client secrets). The AS is a
  separate concern (MR-956).
- **MCP prompts, elicitation, sampling, tasks, `tools/list_changed` subscriptions,
  resource subscriptions, server-initiated messages** — none required, which is what
  permits the stateless POST-only transport. (`tools/list_changed` is reconsidered
  only if the registry gains runtime reload.)
- **stdio transport inside the server** — stdio stays in the TS package (later a proxy).
- **Client-side "code mode" machinery** (TS wrapper generation, sandboxes, tool
  search/deferral) — those are client/runtime concerns; see [§12](#12-code-mode-compatibility)
  for what the server does to support them and what it deliberately does not build.
- **Cross-graph tool listing** — per-graph catalogs only.

## 4. Protocol target

Target MCP revision **`2025-11-25`** (current). `rmcp 1.7.0` advertises this as its
latest and negotiates down to any of `2024-11-05 / 2025-03-26 / 2025-06-18 /
2025-11-25`; an absent `MCP-Protocol-Version` header defaults to `2025-03-26` and an
unsupported one is a `400`. Revision `2025-06-18` is the floor we rely on for two
features: **structured tool output** (`outputSchema` + `structuredContent`) and the
**OAuth Resource-Server** model. From `2025-11-25` we adopt: **input-validation
errors as tool-execution errors** (SEP-1303), **JSON Schema 2020-12** as the default
dialect, **`403` on invalid `Origin`**, and **`WWW-Authenticate` made optional** with
a `.well-known` fallback.

**Transport shape (stateless Streamable HTTP).** The server exposes one endpoint that
accepts `POST` (and answers `GET`/`DELETE` with `405 + Allow: POST`). For a JSON-RPC
*request* it returns one `application/json` object; it opens no SSE stream, assigns no
`Mcp-Session-Id`, and treats every request independently — a fully conformant
stateless server. It **MUST** validate `Origin` (`403` on mismatch) and honor
`MCP-Protocol-Version`. `rmcp` delivers all of these in stateless mode (§7).

## 5. Crate architecture

Two crates; `rmcp` is contained to one of them.

```
omnigraph-server  (implements McpBackend; all omnigraph tool/Cedar/dispatch logic)
        │ depends on
        ▼
omnigraph-mcp     (rmcp Streamable-HTTP transport, the McpBackend trait, rmcp model re-exports)
        │ depends on
        ▼
rmcp 1.7 + tower-http(limit) + axum + http
```

The dependency **must** go `server → mcp`. The server binary mounts `/mcp`, so a
`mcp → server` edge cycles at the package level (`server-bin → omnigraph-mcp →
server-lib`), which Cargo rejects. The trait inverts the direction — the crate
defines the seam, the server fills it — which is also why the crate can never name an
omnigraph type (`AppState`, `GraphHandle`, the handlers); it abstracts over them.

`crates/omnigraph-mcp/Cargo.toml`:

```toml
[package]
name = "omnigraph-mcp"
edition = "2024"                 # rmcp 1.7 is itself edition 2024 — no friction
version.workspace = true

[dependencies]
# `server` is on by rmcp's default features; `transport-streamable-http-server`
# pulls in the tower service + http stack. Do NOT enable rmcp's `local` feature —
# it cfg's the StreamableHttpService tower wiring out.
rmcp = { version = "1.7", default-features = false, features = ["server", "transport-streamable-http-server"] }
axum       = { workspace = true }
http       = "1"
tower-http = { workspace = true, features = ["limit"] }
tokio      = { workspace = true }
async-trait = { workspace = true }
serde_json = { workspace = true }
```

Add `"crates/omnigraph-mcp"` to the workspace `members`; in
`omnigraph-server/Cargo.toml` add `omnigraph-mcp` and **no direct `rmcp` dep**
(verified absent today). The verification gate is `cargo tree -p omnigraph-server -e
normal | grep rmcp` showing rmcp only transitively under `omnigraph-mcp`.

## 6. The `McpBackend` seam — `New` in `omnigraph-mcp`

```rust
// crates/omnigraph-mcp/src/lib.rs
use async_trait::async_trait;

// rmcp model types re-exported so the server speaks rmcp via `omnigraph_mcp::…`
// and carries no direct rmcp dependency.
pub use rmcp::model::{
    CallToolResult, Content, RawResource, ReadResourceResult, Resource,
    ResourceContents, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
};
pub use rmcp::ErrorData as McpError;        // JSON-RPC error type (method_not_found=-32601, invalid_params=-32602, internal_error=-32603)
pub type JsonObject = serde_json::Map<String, serde_json::Value>;

#[async_trait]
pub trait McpBackend: Clone + Send + Sync + 'static {
    fn server_info(&self) -> ServerInfo;
    async fn list_tools(&self, parts: &http::request::Parts) -> Result<Vec<Tool>, McpError>;
    async fn call_tool(&self, parts: &http::request::Parts, name: &str, args: JsonObject) -> Result<CallToolResult, McpError>;
    async fn list_resources(&self, parts: &http::request::Parts) -> Result<Vec<Resource>, McpError>;
    async fn read_resource(&self, parts: &http::request::Parts, uri: &str) -> Result<ReadResourceResult, McpError>;
}
```

`&http::request::Parts` is the decoupling mechanism. The crate hands the backend the
request parts; the backend reads **its own** types out of `parts.extensions`. The
crate never names an omnigraph type, so it is reusable and auth stays decoupled (§8).

> `rmcp`'s own `ServerHandler` trait uses RPITIT (`-> impl Future + …`), not
> `async-trait`. Our `McpBackend` deliberately uses `#[async_trait]`: it is
> implemented once by the server, the boxed future is negligible at MCP QPS, and the
> server already depends on `async-trait`. Either style compiles on edition 2024.

## 7. Transport — `New` in `omnigraph-mcp`

```rust
// crates/omnigraph-mcp/src/transport.rs
use std::sync::Arc;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::never::NeverSessionManager,     // stateless ⇒ reject all session ops
};

pub struct McpHostPolicy {
    pub allowed_hosts:   Option<Vec<String>>,   // None ⇒ accept any Host (rely on bearer + Origin)
    pub allowed_origins: Option<Vec<String>>,   // None/empty ⇒ Origin check off (non-browser clients send none)
}

pub fn mcp_router<B: McpBackend>(backend: B, body_limit: usize, hosts: McpHostPolicy) -> axum::Router {
    // StreamableHttpServerConfig is #[non_exhaustive]; its Default is stateful_mode=true,
    // json_response=false, allowed_hosts=loopback. ALL THREE must be overridden for a
    // remote stateless JSON server — build from Default and flip via the with_* setters.
    let mut config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true);
    config = match hosts.allowed_hosts {
        Some(list) => config.with_allowed_hosts(list),
        None        => config.disable_allowed_hosts(),     // accept any Host
    };
    if let Some(origins) = hosts.allowed_origins { config = config.with_allowed_origins(origins); }

    // service_factory returns Result<S, io::Error>; NeverSessionManager pairs with stateless mode.
    let svc = StreamableHttpService::new(
        move || Ok(McpService::new(backend.clone())),
        Arc::new(NeverSessionManager::default()),
        config,
    );

    axum::Router::new()
        .route_service("/mcp", svc)
        // rmcp reads the body directly (not via an axum extractor), so axum's
        // DefaultBodyLimit does NOT bound /mcp — the tower-http layer does.
        .layer(tower_http::limit::RequestBodyLimitLayer::new(body_limit))
}
```

`McpService<B>` implements rmcp's `ServerHandler`, pulls `&Parts` out of the request
context once, and delegates each method to `B`. rmcp's `StreamableHttpService`
**consumes the body and injects the remaining `http::request::Parts` into
`RequestContext.extensions`** (this is documented and load-bearing — see §8); inside
the handler, `ctx.extensions.get::<http::request::Parts>()` returns those parts.

**Conformance the stateless transport gives for free** (verified in rmcp 1.7
`tower.rs`): `GET`/`DELETE /mcp → 405` with `Allow: POST`; a disallowed `Host` →
`403`; a present-but-invalid `Origin` → `403` (Origin checked only when
`allowed_origins` is non-empty); `MCP-Protocol-Version` → `400` on unsupported,
default `2025-03-26` when absent. **No conformance middleware is needed.**

**Host/Origin policy is derived from the deployment, never a fixed default.** rmcp's
default `allowed_hosts` is loopback-only — correct for local dev (DNS-rebinding
defense) but it would `403` every remote client. `omnigraph-server` computes
`McpHostPolicy` once at startup from `--bind`: loopback bind → keep the loopback
allow-list; non-loopback bind → allow the configured public host(s), else disable
Host-allowlisting and log it (bearer + `Origin` are the real controls). `Origin`
validation defaults off (non-browser MCP clients send no `Origin`) and is enabled for
browser-based clients.

## 8. Auth & identity — `Reuses` the server's middleware

The backend consumes an already-resolved actor and **branches on nothing** about how
the token was verified. Two values are injected into the request extensions by
middleware that runs **before** the MCP service:

```rust
// Reuses — crates/omnigraph-server/src/identity.rs:186
pub struct ResolvedActor { pub actor_id: Arc<str>, pub tenant_id: Option<TenantId>, pub scopes: Vec<Scope>, pub source: AuthSource }

// Reuses — crates/omnigraph-server/src/registry.rs:37
pub struct GraphHandle {
    pub key: GraphKey, pub uri: String,
    pub engine: Arc<Omnigraph>,
    pub policy:  Option<Arc<PolicyEngine>>,   // None ⇒ no per-graph Cedar gate
    pub queries: Option<Arc<QueryRegistry>>,  // None ⇒ no stored queries for this graph
}
```

The middleware order is fixed in `build_app` (`lib.rs:876`; the two `route_layer`s at
`lib.rs:929-936`): the **outer** layer `require_bearer_auth` injects
`Extension<ResolvedActor>` (or `401`); the **inner** layer `resolve_graph_handle`
injects `Extension<Arc<GraphHandle>>`. Both land in `request.extensions()`, which rmcp
copies into `RequestContext.extensions`.

```rust
// New — crates/omnigraph-server/src/mcp/mod.rs
#[derive(Clone)]
pub struct OmnigraphMcpBackend { state: AppState }   // AppState is Arc-backed #[derive(Clone)]

impl OmnigraphMcpBackend {
    fn ctx<'a>(&self, parts: &'a http::request::Parts) -> Result<(&'a ResolvedActor, &'a Arc<GraphHandle>), McpError> {
        let actor  = parts.extensions.get::<ResolvedActor>()
            .ok_or_else(|| McpError::internal_error("actor missing from request extensions", None))?;
        let handle = parts.extensions.get::<Arc<GraphHandle>>()
            .ok_or_else(|| McpError::internal_error("graph handle missing from request extensions", None))?;
        Ok((actor, handle))
    }
}
```

**Auth posture (spec-aligned, MCP 2025-11-25 authorization).** The server is a
Resource Server. Per-request validation only — **sessions are never used for
authentication** (the transport is stateless, which makes this structural). Token
**audience must be validated** and **token passthrough is prohibited**: if a tool
later needs to reach an upstream API, the server acts as a separate OAuth client and
must not forward the client's token.

- **Static bearer (today).** `require_bearer_auth` resolves a `ResolvedActor` from a
  SHA-256 hash match. Works for the developer/agent clients (§13).
- **OAuth 2.1 + RFC 9728 (additive, MR-956).** Serve
  `/.well-known/oauth-protected-resource`; on `401`, optionally add
  `WWW-Authenticate: Bearer resource_metadata="…"` (header is optional in 2025-11-25
  given the well-known fallback). Clients run OAuth 2.1 + PKCE + RFC 8707 resource
  indicators themselves; the server validates audience-bound JWTs offline (cached
  JWKS), so on-prem/air-gapped keeps working. This swaps the bearer middleware behind
  a `TokenVerifier` and changes **zero** MCP code.

> **Compatibility caveat to honor (Claude Code issue #59467).** Advertising RFC-9728
> Protected-Resource-Metadata can cause some clients (Claude Code today) to **ignore
> a static `Authorization` header and force the OAuth flow**. So PRM advertisement
> must be **config-gated**: a deployment serving developer clients over static bearer
> does not advertise OAuth; a deployment targeting consumer connectors does. The MCP
> routes only need to flow through the standard `401` path so the hook can be added
> without touching MCP code.

## 9. Stored-query projection

The projection source is the same `query_catalog_entry` the `GET /queries` catalog
uses (`crates/omnigraph-server/src/api.rs:13`). The param/catalog DTOs moved to the
shared `omnigraph-api-types` crate (RFC-009 Phase 2) and are re-exported through
`api.rs` (`pub use omnigraph_api_types::*`), so the `Reuses` types below still resolve
via `omnigraph_server::api::…`. Real types:

```rust
// Reuses — crates/omnigraph-api-types/src/lib.rs:355 (re-exported via omnigraph-server/src/api.rs)
pub enum ParamKind { String, Bool, Int, BigInt, Float, Date, DateTime, Blob, Vector, List }

// Reuses — crates/omnigraph-api-types/src/lib.rs:373
pub struct ParamDescriptor {
    pub name: String,
    pub kind: ParamKind,
    pub item_kind: Option<ParamKind>,   // Some(scalar) when kind == List
    pub vector_dim: Option<u32>,        // Some(dim) when kind == Vector — the dimension lives here, not in the kind
    pub nullable: bool,
}

// Reuses — crates/omnigraph-server/src/queries.rs:29
pub struct StoredQuery { pub name: String, pub source: Arc<str>, pub decl: QueryDecl, pub expose: bool, pub tool_name: Option<String> }
impl StoredQuery { pub fn is_mutation(&self) -> bool; pub fn effective_tool_name(&self) -> &str; }   // queries.rs:45,55
pub struct QueryRegistry { /* by_name: BTreeMap<String, StoredQuery>; .lookup(&name) */ }            // queries.rs:64
```

A query is declared in the cluster's `cluster.yaml graphs.<id>.queries` (a directory
to discover, an explicit file list, or a `name: { file: … }` map); `cluster apply`
publishes it to the content-addressed catalog, and the server loads that graph's
applied registry into `handle.queries` at boot (`settings.rs:71-111`). The
`StoredQuery` struct carries `expose: bool` and `tool_name: Option<String>`, **but
cluster boot currently forces `expose: true, tool_name: None` for every applied
query** (`settings.rs:83-84`, the §D5 bridge — see [§17](#17-decisions--rollout)). So
today the projection lists every applied query and names each by its query name; the
`expose`/`tool_name` plumbing is wired but inert until the cluster catalog grows the
per-query metadata. The projection reads `handle.queries` and is agnostic to the
declaration source. (The legacy single-graph `omnigraph.yaml queries:` map is removed
— RFC-011 made the server cluster-only; there is no other declaration source.)

### 9.1 `ParamDescriptor → JSON Schema` (`New`)

JSON Schema 2020-12. The `Vector` dimension is an `Option<u32>` on the descriptor:
when present, constrain the array length; when absent, **omit** `minItems`/`maxItems`
(never emit `0`, which would reject all input).

```rust
// New — crates/omnigraph-server/src/mcp/schema.rs
use serde_json::{json, Value};

fn scalar_schema(kind: ParamKind) -> Value {
    match kind {
        ParamKind::String   => json!({ "type": "string" }),
        ParamKind::Bool     => json!({ "type": "boolean" }),
        ParamKind::Int      => json!({ "type": "integer" }),
        ParamKind::BigInt   => json!({ "type": "string", "pattern": r"^-?\d+$" }), // i64/u64 lose precision >2^53 as JSON numbers
        ParamKind::Float    => json!({ "type": "number" }),
        ParamKind::Date     => json!({ "type": "string", "format": "date" }),
        ParamKind::DateTime => json!({ "type": "string", "format": "date-time" }),
        ParamKind::Blob     => json!({ "type": "string", "contentEncoding": "base64" }),
        ParamKind::Vector | ParamKind::List => unreachable!("composite kinds handled in param_schema"),
    }
}

fn param_schema(p: &ParamDescriptor) -> Value {
    match p.kind {
        ParamKind::Vector => {
            let mut s = json!({ "type": "array", "items": { "type": "number" } });
            if let Some(dim) = p.vector_dim { s["minItems"] = json!(dim); s["maxItems"] = json!(dim); }
            s
        }
        ParamKind::List => json!({ "type": "array", "items": p.item_kind.map(scalar_schema).unwrap_or_else(|| json!({"type":"string"})) }),
        scalar => scalar_schema(scalar),
    }
}
```

### 9.2 Two projection modes (small vs large catalogs)

Tool-overload is real: model accuracy degrades sharply as a single client's tool
count climbs past a few dozen, and clients that don't defer tool loading (e.g.
OpenCode) pay the full `tools/list` token cost. So the projection has two modes,
selected per graph by a `mcp.stored_query_mode` setting (default `auto`):

- **`per_query` (small/stable catalogs).** One tool per `expose: true` query, named by
  `effective_tool_name()`, with a fully typed `input_schema`. This is the richest
  surface — each query is a first-class typed tool, ideal for code-mode runtimes that
  compile tools into a typed API.
- **`meta` (large/dynamic catalogs).** Two tools instead of N: `stored_query_list(filter?,
  detail_level?)` (returns names + descriptions; full param schema only at higher
  detail) and `stored_query_run(name, params, branch?, snapshot?)`. This keeps
  `tools/list` small and mirrors the progressive-disclosure shape (`search` + `execute`)
  that scales to hundreds of queries.
- **`auto`** picks `per_query` below a threshold (default 24 exposed queries) and `meta`
  at or above it; the threshold is configurable. The boundary and count are logged so
  a deployment never silently flips modes.

### 9.3 Envelope (collision-free by construction)

In `per_query` mode the tool's `input_schema` **nests query params under `params`**,
mirroring `POST /queries/{name}`:

```jsonc
{ "type": "object",
  "properties": {
    "params":   { "type": "object", "properties": { /* per-param param_schema(...) */ }, "required": [ /* nullable == false */ ] },
    "branch":   { "type": "string" },
    "snapshot": { "type": "string" }      // omit for mutation tools — mutation-against-snapshot is unrepresentable
  },
  "additionalProperties": false }
```

Knobs (`branch`/`snapshot`) and the query's own params live in separate namespaces, so
a query parameter literally named `branch`/`snapshot` cannot collide. A stored tool
whose `effective_tool_name()` collides with a built-in is skipped (built-ins win).

## 10. Tool catalog + Cedar mapping — `Reuses` `PolicyAction`

Each built-in reuses the **exact `PolicyAction` its REST route enforces**:

```rust
// Reuses — crates/omnigraph-policy/src/lib.rs:16
pub enum PolicyAction {
    Read, Export, Change, SchemaApply,
    BranchCreate, BranchDelete, BranchMerge,
    Admin,        // reserved, no call site yet
    GraphList,    // server-scoped (resource_kind == Server)
    InvokeQuery,  // graph-scoped, coarse (no per-query dimension yet)
}
```

| MCP tool | Scope | Cedar action |
|---|---|---|
| `health` | server | none (liveness/version) |
| `graph_snapshot`, `schema_get`, `branch_list`, `commit_list`, `commit_get` | graph | `Read` |
| `graph_query` (ad-hoc read) | graph | `Read` (`run_query` self-authorizes) |
| `graph_mutate` (ad-hoc write) | graph | `Change` |
| `graph_load` (NDJSON) | graph | `Change` (+ `BranchCreate` **iff** `from` is present — see §11) |
| `branch_create` / `branch_delete` / `branch_merge` | graph | `BranchCreate` / `BranchDelete` / `BranchMerge` |
| `schema_apply` (`allow_data_loss`) | graph | `SchemaApply` |
| stored query (`per_query`) / `stored_query_run` (`meta`) | graph | `InvokeQuery` (coarse) then inner `Read`/`Change` |

**Naming.** Tool ids are **domain-qualified `snake_case`** (`graph_query`,
`branch_merge`, `schema_apply`, …) within the spec's `[A-Za-z0-9_.-]`, 1–128-char
constraint. Domain qualification (rather than bare `query`/`mutate`) reduces
cross-server collisions when a client loads omnigraph alongside other MCP servers;
clients that auto-prefix by connection name (e.g. OpenCode → `omnigraph_graph_query`)
compose cleanly. Names are a stability contract (Hyrum's Law) — don't churn them.

**Annotations (set explicitly).** MCP annotation defaults are pessimistic
(`readOnlyHint=false`, `destructiveHint=true`, `idempotentHint=false`,
`openWorldHint=true`), so an unannotated read tool is mistaken for a destructive
open-world writer. Set them via rmcp's `ToolAnnotations` (`read_only_hint`,
`destructive_hint`, `idempotent_hint`, `open_world_hint`):

- read tools (`graph_query`, `graph_snapshot`, `schema_get`, `branch_list`,
  `commit_*`, stored *reads*) → `read_only_hint = true`, `open_world_hint = false`.
- writers (`graph_mutate`, `graph_load`, `branch_delete`, `branch_merge`,
  `schema_apply`) → `read_only_hint = false`, `destructive_hint = true`,
  `open_world_hint = false`. Clients use `destructiveHint` to drive human-confirmation
  prompts.
- `branch_create` (additive) → `destructive_hint = false`.

Annotations are **advisory hints, not a security boundary** (clients may ignore them);
**Cedar is the enforcement boundary.**

Represent built-ins as a `Builtin` enum (one variant per tool; `descriptor` / `gate` /
`call` as match arms) — lower liability than ~13 unit structs + `dyn`. Stored-query
tools are a sibling populator over `handle.queries`.

**`list_tools` / `list_resources` are Cedar-filtered** by running the *same*
authorization the call path runs, with **default args (branch `main`)** — not a
`branch: None` probe (which matches no `branch_scope` rule and would hide tools the
actor can call on a scoped branch). Over-showing a branch-scoped grant is the safe
direction; `call_tool` is the authoritative gate.

## 11. Dispatch reuse + error classification

`call_tool` adds no business logic. Reuse points (all in `handlers.rs`):

```rust
pub(crate) enum Authz { Allowed, Denied(String) }                                  // handlers.rs:313
pub(crate) fn authorize(actor: Option<&ResolvedActor>, policy: Option<&PolicyEngine>, request: PolicyRequest) -> Result<Authz, ApiError>; // :334 — Err = operational 401/500
pub(crate) async fn run_query(handle: Arc<GraphHandle>, actor: Option<&ResolvedActor>, query: &str, name: Option<&str>, params_json: Option<&Value>, branch: Option<String>, snapshot: Option<String>, reject_mutations: bool) -> Result<(String, ReadTarget, QueryResult), ApiError>; // :711
pub(crate) async fn run_mutate(state: AppState, handle: Arc<GraphHandle>, actor: Option<&ResolvedActor>, query: &str, name: Option<&str>, params_json: Option<&Value>, branch: String) -> Result<ChangeOutput, ApiError>; // :645
```

`PolicyRequest` carries `{ action, branch, target_branch }` only — **no actor
identity** (server-resolved, supplied separately) and **no query-name dimension**
(the coarse-`invoke_query` caveat):

```rust
// Reuses — crates/omnigraph-policy/src/lib.rs:251
pub struct PolicyRequest { pub action: PolicyAction, pub branch: Option<String>, pub target_branch: Option<String> }
```

**The stored-query double-gate + deny-masking pattern** (`handlers.rs:913`,
`server_invoke_query`) is the contract `call_tool` mirrors for stored queries:

```rust
// Reuses (pattern) — outer InvokeQuery gate; deny == missing so the catalog can't be probed
match authorize(actor, handle.policy.as_deref(), PolicyRequest {
    action: PolicyAction::InvokeQuery, branch: None, target_branch: None,   // graph-scoped: NO branch dimension
})? {
    Authz::Allowed => {}
    Authz::Denied(_) => return Err(ApiError::not_found("stored query not found")),
}
let stored = handle.queries.as_ref().and_then(|r| r.lookup(&name)).ok_or_else(|| ApiError::not_found("stored query not found"))?;
// inner gate runs in run_mutate (Change) / run_query (Read); a stored mutation is double-gated.
```

**`graph_load` (NDJSON)** wraps the unified `load_as` via `run_ingest` (the canonical
`server_load` handler, `handlers.rs:1320`; `POST /ingest` / `server_ingest`,
`handlers.rs:1360`, is a `#[deprecated]` alias emitting RFC-9745 headers — RFC-009
Phase 5): a missing branch with **no `from` is a `404`, never an implicit fork**;
`BranchCreate` is consulted only when `from` is present, then `Change` for the load.
The tool's `input_schema` is `{ data: string, branch?: string, from?: string,
mode?: "merge"|"append"|"overwrite" }`, `additionalProperties: false` (the same
`IngestRequest` shape, `omnigraph-api-types/src/lib.rs:496`).

**Error classification (`New`, one mapper, SEP-1303-aligned).** `ApiError`'s fields are
private (`lib.rs:280`, and still carry no public status/message accessors), so add
`pub(crate) fn status_code(&self)`/`message_str(&self)` accessors. Then one `classify`
is used at every dispatch site:

```rust
// New — the single source of truth
fn classify(r: Result<CallToolResult, ApiError>) -> Result<CallToolResult, McpError> {
    match r {
        Ok(out) => Ok(out),
        // Semantic failures (bad params, validation, business 4xx/409) → isError result,
        // fed back to the model so it self-corrects (MCP 2025-11-25 SEP-1303).
        Err(e) if e.status_code().is_client_error() => Ok(CallToolResult::error(vec![Content::text(e.message_str())])),
        // Operational failures (5xx) → JSON-RPC protocol error.
        Err(e) => Err(McpError::internal_error(e.message_str().to_owned(), None)),
    }
}
```

Two cases are protocol errors, not `isError`, so the catalog isn't probeable and
malformed calls are unambiguous: an **unknown OR denied tool** returns an identical
`McpError::invalid_params("unknown tool: <name>")` (`-32602`), and a structurally
malformed call (failing the `tools/call` shape) is a protocol error. A missing/bad
bearer is an HTTP `401` at the boundary, before rmcp.

## 12. Code-mode compatibility

"Code mode" (Anthropic's *Code execution with MCP*; Cloudflare's *Code Mode*) is a
**client/runtime** technique: the client compiles a server's tools into a typed code
API (TS modules / a sandbox), the model writes code against it, and intermediate
results are filtered in the sandbox instead of round-tripping through the model
context (reported ~98% context savings on large workflows). It runs over **standard
`tools/list` + `tools/call`** and **requires no new server endpoints**; credentials
stay in the transport and the runtime holds them (the sandbox never sees the bearer).

The server's job is to be a *good source* for that compilation. Concrete server-side
choices this RFC adopts:

1. **Strict, fully-typed `input_schema`** (§9.1) with `additionalProperties: false`,
   enums for `mode`/`format`, explicit `required` — these compile into precise TS
   input types.
2. **Structured output** — see §13.1: declare `outputSchema` and return
   `structuredContent` so generated code gets typed *returns*, not `any`.
3. **Stable, descriptive tool names + rich descriptions** (§10) — names become
   function names; descriptions become doc comments.
4. **Progressive disclosure for large catalogs** — the `meta` projection mode (§9.2)
   keeps `tools/list` small (`stored_query_list` + `stored_query_run`), the same
   `search` + `execute` shape code-mode runtimes prefer.
5. **Pagination** on every list-style result (cursor-based) so a code-mode run fetches
   and filters incrementally without context blow-up.
6. **Schemas as resources** (§14) — expose the graph schema (and per-query param
   schemas) as MCP resources, the on-demand channel code-mode clients pull from.
7. **Auth in the transport only** — never require secrets as tool *arguments* (that
   would put them in model context / generated code and break the sandbox's credential
   isolation).

The server deliberately does **not** build TS-wrapper generation, sandboxes, tool
search/deferral, or PII tokenization — those are client/runtime concerns, and there is
no ratified "tools-as-code" MCP spec to target.

## 13. Provider compatibility

**Transport: Streamable HTTP is the universal target** — every current client below
supports it for remote servers, and it is the recommended transport over deprecated
HTTP+SSE.

**Auth splits the ecosystem into two tiers:**

| Client | Remote transport | Auth that works | Notes |
|---|---|---|---|
| **Claude Code** (CLI) | Streamable HTTP | static bearer header **and** OAuth 2.1 | `claude mcp add --transport http <url>/mcp --header "Authorization: Bearer …"`. Advertising RFC-9728 can override the static header (issue #59467) — gate PRM. |
| **Cursor** | Streamable HTTP | static header **and** OAuth 2.1 | `"headers": {"Authorization": "Bearer ${env:…}"}` in `mcp.json`. |
| **VS Code** (Copilot agent) | Streamable HTTP | static header **and** OAuth | needs VS Code ≥ 1.101 for remote + OAuth; auto-detects `401` → sign-in. |
| **OpenCode** | remote HTTP | static header **and** OAuth (auto, DCR) | `mcp` block in `opencode.json`; auto-prefixes tools `omnigraph_…`; **no progressive disclosure** → keep the static surface tight (favors `meta` mode at scale). |
| **Claude Messages API** (`mcp_servers`) | Streamable HTTP (+SSE) | pre-acquired token via `authorization_token` | forwards a token; never runs OAuth. Static bearer fits directly. Pin the beta header you target. |
| **OpenAI Responses API** (`mcp` tool) | Streamable HTTP (+SSE) | arbitrary `headers` (static bearer) or pre-acquired `authorization` | forwards a token; never runs OAuth. `require_approval` gates tool calls. |
| **ChatGPT** (developer mode/connectors) | Streamable HTTP (+SSE) | OAuth, **No-Auth**, or Mixed | beta; OAuth is the clean path. |
| **Claude Desktop** (custom connectors) | Streamable HTTP (+SSE) | **OAuth 2.1 or authless** | no static-header field — bearer-only deployments are unreachable without a gateway. |
| **Claude.ai web** (custom connectors) | Streamable HTTP (+SSE) | **OAuth 2.1 + RFC 9728** (or authless) | server **must** serve RFC-9728 PRM; no static-header field. |

**Phased auth recommendation:**

- **Phase 1 — static bearer (this RFC).** Reaches Claude Code, Cursor, VS Code
  Copilot, OpenCode, the Claude Messages API connector, and the OpenAI Responses API —
  the entire developer/agent/API tier. This is the correct launch posture.
- **Phase 2 — OAuth 2.1 + RFC 9728 (MR-956, additive).** Required to reach **claude.ai
  web** and **Claude Desktop** custom connectors and the clean ChatGPT path. The same
  endpoint accepts validated OAuth access tokens and (still) static bearers; PRM
  advertisement stays config-gated because of the #59467 header-override behavior.

Because the resource server validates whatever token arrives on `Authorization`,
both tiers hit one endpoint with no MCP-layer branching.

### 13.1 Result shaping & structured output

For typed, machine-consumable results (`graph_query`, stored-query reads,
`branch_list`, `commit_*`, `schema_get`) the tool declares an **`outputSchema`** and
returns **`structuredContent`** (the route's existing `ReadOutput` / listing DTOs,
which already derive `ToSchema`), **and also** mirrors the JSON in a text `Content`
block for clients that don't parse structured content. Plain text-JSON is used where a
fixed schema is awkward. (Some clients still mishandle `structuredContent: null` —
emit an empty object, never `null`, when there is no structured payload.)

## 14. Resources

Two resources: `omnigraph://schema` (`Read` → schema `.pg` text) and
`omnigraph://branches` (`Read` → branches JSON). Both are Cedar-filtered and
deny-masked exactly like tools — a locked-down agent denied `Read` never sees them,
which is how the "agents don't introspect schema" intent is met by *policy*, not
omission. Advertise the `resources` capability with `subscribe:false,
listChanged:false` (both handlers are backed — don't advertise a capability whose
`read` would 404). Exposing the schema as a resource is also the on-demand channel
code-mode clients pull from (§12).

No `omnigraph://graphs` resource and no `graphs_list` tool — server-scoped graph
discovery stays REST-only via `GET /graphs` (§15).

## 15. Routing — `Reuses` `build_app`

`/mcp` is merged **into `per_graph_protected`**, which `build_app` always nests under
`/graphs/{graph_id}`. RFC-011 made the server **cluster-only** — there is no flat
single-graph route group and no `match state.routing()`, so `/mcp` is **always**
`/graphs/{graph_id}/mcp` (even a single-graph boot builds a one-graph registry keyed
by `default`; `GraphRouting` is now `{ registry, config_path }`):

```rust
// Reuses — crates/omnigraph-server/src/lib.rs:876 (abridged)
let per_graph_protected = Router::new()
    .route("/snapshot", get(server_snapshot))
    // … /query /mutate /queries /queries/{name} /schema /schema/apply /load /branches /commits …
    .merge(mcp::mcp_router(state.clone()))                       // ← ADD: brings its own tower-http body-limit layer
    .route_layer(middleware::from_fn_with_state(state.clone(), resolve_graph_handle))  // inner: injects Arc<GraphHandle> (lib.rs:929)
    .route_layer(middleware::from_fn_with_state(state.clone(), require_bearer_auth));  // outer: injects ResolvedActor / 401 (lib.rs:933)

let management = Router::new()
    .route("/graphs", get(server_graphs_list))                  // GraphList — server-scoped, REST-only
    .route_layer(middleware::from_fn_with_state(state.clone(), require_bearer_auth));

// RFC-011 cluster-only: per-graph routes ALWAYS nest under /graphs/{graph_id};
// there is no flat mode and no routing match. (lib.rs:953)
let protected = Router::new()
    .nest("/graphs/{graph_id}", per_graph_protected)            // → POST /graphs/{id}/mcp
    .merge(management);
```

`mcp::mcp_router(state)` is the server's thin wrapper:
`omnigraph_mcp::mcp_router(OmnigraphMcpBackend::new(state), INGEST_REQUEST_BODY_LIMIT_BYTES /* lib.rs:148, 32 MiB */, host_policy_from_bind(…))`.
Merging the router (rather than `.route("/mcp", …)`) keeps the `/mcp`-specific body
limit from leaking onto the other routes.

**No server-scoped MCP.** Every MCP tool/resource is graph-scoped. `tools/list` can't
carry a graph id, so a single flat `/mcp` taking `graph_id` per call couldn't list
per-graph stored-query tools and would break isolation — hence per-graph routing. A
future server-level flat `/mcp` (bearer-only, no handle, server-scoped tools only)
would live in the `management` group, but is not built speculatively.

### 15.1 Multi-graph model

omnigraph's MCP is **per-graph**: one isolated MCP server per graph, with the graph
identity in the **URL path**, never in tool arguments or output. The server is
cluster-only (RFC-011), so the router **always** nests the whole protected group under
`/graphs/{graph_id}` (`lib.rs:954`) — this per-graph model is now the only model, not a
multi-mode special case. Each `/graphs/{id}/mcp` endpoint's `initialize` / `tools/list`
/ `tools/call` / `resources/*` operate only on that graph and can never list or touch
another graph's tools.

- **Discovery is REST-only, not an MCP tool.** `graphs_list` / `omnigraph://graphs`
  are deliberately absent from MCP. Which graphs exist is answered by `GET /graphs`
  (multi-mode only) → `GraphListResponse { graphs: [{ graph_id, uri }] }`
  (`api.rs:703`), gated by the server-scoped `GraphList` Cedar action and
  **default-denied without a server policy** (the registry — graph ids + storage URIs
  — is never leaked until an operator authorizes it). An operator discovers graphs via
  REST, then points each MCP client connection at the relevant `/graphs/{id}/mcp`; no
  single MCP connection ever sees the full graph list.

- **Clients configure one connection per graph.** Tool ids are identical across graphs
  (each is its own server), so the **connection name is the namespace**: a client that
  auto-prefixes yields `og-sales_graph_query` vs `og-hr_graph_query`.

  ```bash
  claude mcp add og-sales --transport http https://host/graphs/sales/mcp --header "Authorization: Bearer …"
  claude mcp add og-hr    --transport http https://host/graphs/hr/mcp    --header "Authorization: Bearer …"
  ```

- **Stored queries are per-graph state.** Each graph owns its registry
  (`GraphHandle.queries`, `registry.rs:55`), loaded from that graph's declaration
  (`cluster.yaml graphs.<id>.queries`). So a query is exposed only on its own graph's
  endpoint; the same query *name* may exist on multiple graphs with different
  definitions (no cross-graph collision — different servers). `effective_tool_name()`
  uniqueness is enforced **per graph** at registry load (`duplicate_tool_name`), not
  across graphs. The projection mode (`per_query` vs `meta`, §9.2) is chosen from
  *that graph's* exposed-query count, so a small graph can show one typed tool per
  query while a large graph on the same server uses the `stored_query_list` +
  `stored_query_run` meta pair. `InvokeQuery` is evaluated against *that graph's*
  `handle.policy`, so an actor can be allowed stored queries on one graph and denied
  on another, independently. The per-graph catalog is also discoverable over REST at
  `GET /graphs/{id}/queries`.

So `tools/list` on `/graphs/sales/mcp` returns sales' built-ins + sales' stored
queries; the same call on `/graphs/hr/mcp` returns hr's — two disjoint catalogs, each
Cedar-filtered to the actor.

## 16. Tests & verification

MCP tests land in a new `crates/omnigraph-server/tests/mcp.rs` suite (black-box over
`build_app`); stored-query *projection* tests extend `stored_queries.rs`.

- **Protocol:** `initialize` + advertised `{tools, resources}` caps; `tools/list`
  shape + pagination; `tools/call` happy path; `GET /mcp → 405`;
  `MCP-Protocol-Version` 400/default; `Origin → 403`; unknown/denied tool →
  identical `-32602`.
- **Cedar:** a read-only actor sees read tools but not writers; a denied call masks
  byte-identically to an unknown one; stored queries appear only with `invoke_query`;
  the double-gate (an `invoke_query`-only actor sees a stored tool but the call
  surfaces `isError` when the inner `Read` denies).
- **Dispatch:** a `graph_mutate` writes end-to-end (proves the actor/handle extension
  passthrough); a malformed query → `isError:true`, not a protocol error; `graph_load`
  with a missing branch and no `from` → `isError` (404), with `from` → forks.
- **Schema generation:** table-driven over every `ParamKind` incl. nullable, list, and
  `vector` **with and without `vector_dim`** (the absent-dim path omits
  `minItems`/`maxItems`).
- **Structured output:** `outputSchema` present and `structuredContent` validates
  against it; the text mirror is present; never emits `structuredContent: null`.
- **Projection modes:** `per_query` below the threshold, `meta` at/above it, with the
  switch logged.
- **Auth decoupling:** `/mcp` `401`s without a bearer (before rmcp) and `200`s with
  one; green under the static-hash verifier and a mock OIDC `ResolvedActor`.
- **Crate-level:** `omnigraph-mcp/tests/` with a trivial `McpBackend` proving the
  crate stands alone (`initialize` + `GET → 405`), plus an **rmcp surface guard**
  pinning `StreamableHttpServerConfig`'s `with_*` setters, `NeverSessionManager`, the
  `ServerHandler` method shapes, and the `RequestContext.extensions →
  http::request::Parts` passthrough — the smoke check on any rmcp bump.

Verification commands:

```bash
cargo build --workspace --locked
cargo tree -p omnigraph-server -e normal | grep rmcp     # rmcp only transitively under omnigraph-mcp
cargo test -p omnigraph-server --test mcp
cargo test -p omnigraph-server --test stored_queries
cargo test -p omnigraph-server --test openapi            # /mcp carries no #[utoipa::path]; no REST drift
```

## 17. Decisions & rollout

**Locked:** rmcp 1.7 (official SDK); MCP target `2025-11-25`; stateless JSON over a
single `/mcp` POST (`NeverSessionManager`, `stateful_mode=false`, `json_response=true`);
`McpBackend` crate-trait seam with `&Parts` passthrough; `Builtin` enum + stored-query
populator; domain-qualified `snake_case` tool ids; annotations set explicitly; coarse
`InvokeQuery` with the double-gate; per-graph `/mcp` routing, no server-scoped MCP;
`structuredContent` + `outputSchema` (with a text mirror) for typed results;
`vector_dim: Option<u32>` handled with omit-on-absent; auth consumed as a resolved
actor, validated per-request, never passed through.

**Open / deferred:**
- **OAuth 2.1 + RFC 9728 (MR-956)** — additive Phase 2; PRM advertisement config-gated
  (issue #59467).
- **Per-query `expose` / `tool_name` (cluster Phase 6, the §D5 bridge)** — the
  `StoredQuery` fields exist and the projection already reads them, but cluster boot
  forces `expose: true, tool_name: None` for every applied query (`settings.rs:83-84`),
  so today every applied query is listed and named by its query name. Per-query
  exposure/naming controls (`mcp.expose`, `tool_name`) land when the cluster catalog
  grows the metadata — no projection change is then needed.
- **Per-query `invoke_query` scope (PR 0b)** — add a query-name dimension to
  `PolicyRequest` + the Cedar schema so an actor can be scoped to *specific* stored
  queries. Until then curation is graph-level (registry membership; `expose` once
  Phase 6 lands).
- **`tools/list_changed`** — only if the registry gains runtime reload.
- **stdio → proxy collapse** — the local stdio package degrades to a stdio↔HTTP proxy
  over `/mcp` once this surface is GA, leaving one tool set and one Cedar gate, the
  same one-contract posture as [rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md).

**Rollout:** (1) the `omnigraph-mcp` crate + transport + a trivial backend (crate
stands alone); (2) the server backend — extension passthrough, `Builtin` enum,
read-only tools + resources, Cedar-filtered listing, the `classify` mapper; (3)
mutating tools + stored-query projection (both modes) + structured output; (4) docs +
the `omnigraph mcp install` on-ramp (MR-974); (5) OAuth/RFC-9728 (MR-956) and the
stdio proxy as separate follow-ups.
