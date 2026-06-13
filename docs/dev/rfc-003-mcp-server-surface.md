# RFC-003: MCP Server Surface for `omnigraph-server` — Full Tool Parity, Stored Queries, Modular Auth

**Status:** Proposed — buildable implementation spec, **validated against `main`
at the 0.7.0 development head** (post-v0.6.2; workspace bumped to 0.7.0 in
`dedd647`). A reference implementation shipped in
[omnigraph#157](https://github.com/ModernRelay/omnigraph/pull/157) (proved rmcp
1.7 on edition 2024, the auth-extension passthrough, and the full
tool/resource/Cedar surface); this RFC is the canonical spec for a clean
reimplementation from `main` with a dedicated `omnigraph-mcp` crate.
**Date:** 2026-06-13
**Tickets:** MR-969 (stored queries + MCP exposure), MR-956 (OAuth/RFC-9728
fast-follow — the auth substrate), MR-971 (per-server credential resolver — landed
as RFC-007), MR-974 (`omnigraph mcp install`), MR-668/RFC-005 (multi-graph server — shipped).
**Builds on:** [rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md)
(stored queries + response envelope + the parent MCP design),
[rfc-005-server-cluster-boot.md](rfc-005-server-cluster-boot.md) (multi-graph boot),
[rfc-007-operator-config.md](rfc-007-operator-config.md) (the landed client
credential model), [rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md)
(the embedded/remote `GraphClient` unification this surface is a sibling of), and
[omnigraph#128](https://github.com/ModernRelay/omnigraph/pull/128) (the shipped
stored-query registry, `GET /queries`, `POST /queries/{name}`, the coarse
`invoke_query` gate).
**Supersedes:** the MCP-transport portion of `rfc-001` (`GET /mcp/tools` + `POST
/mcp/invoke`). See [Relationship to RFC-001](#relationship-to-rfc-001).
**Target release:** v0.8.x (per RFC-009, MCP-adjacent work is post-v0.7.0).

> **Provenance.** Every snippet labelled **`EXISTING (reuse)`** is copied verbatim
> from `origin/main` with a `file:line` citation, re-read from source on
> 2026-06-13 — verify with `git show origin/main:<file>`. Snippets labelled
> **`NEW (build this)`** are the interfaces to add. The one behavior this repo
> cannot verify — that rmcp 1.7 forwards the request's `http::request::Parts`
> into `RequestContext.extensions` — was proven by the reference impl
> [omnigraph#157](https://github.com/ModernRelay/omnigraph/pull/157); it is
> carried forward as a cited assumption, pinned by a crate-level surface guard
> (§11). This document absorbed the standalone implementation-spec draft
> (formerly `rfc-010`); there is one MCP RFC.

## Summary

Add a first-class **MCP (Model Context Protocol) server surface to
`omnigraph-server`**, exposed over **Streamable HTTP**, projecting the server's
operations as MCP tools and resources for LLM clients (Claude Code/Desktop/web,
Cursor, ChatGPT dev-mode, etc.). Two tool populations share one projection path:

1. **Built-in operational tools** — `health`, `snapshot`, `schema_get`,
   `branches_list`, `commits_list`, `commits_get`, ad-hoc `query`/`mutate`,
   `load` (NDJSON), `branches_create`/`branches_delete`/`branches_merge`,
   `schema_apply` — plus resources `omnigraph://schema` and `omnigraph://branches`.
   The authoritative catalog is [§6](#6-tool-catalog-13-built-ins--cedar-mapping)
   (13 tools). Server-scoped graph discovery (`graphs_list` / `omnigraph://graphs`)
   is **dropped from MCP** — it stays REST-only via `GET /graphs` ([§9](#9-routing--existing-reuse-validated)).
2. **Dynamic stored-query tools** — one MCP tool per `expose: true` entry in the
   graph's loaded stored-query registry, typed from the `.gq` declaration via the
   shipped `query_catalog_entry` / `ParamDescriptor` projection ([§7](#7-stored-query-projection--existing-reuse--the-corrected-schema)).

Every tool is **authorized by the server's existing Cedar policy engine**. The MCP
layer never implements its own authentication: it consumes an **already-resolved
`ResolvedActor`** from the server's bearer middleware, so the **same MCP endpoint
serves on-prem (static or customer-OIDC tokens) and cloud (WorkOS OAuth) by
configuration only** — cloud OAuth is an additive RFC-9728 layer that slots in with
zero MCP changes. The end-state collapses two diverging tool implementations into
one: the in-server MCP is canonical; the stdio package degrades to a thin
stdio↔HTTP proxy over it.

> **Key caveat, up front:** the headline "a token Cedar-scoped to a *specific set*
> of stored queries" requires **per-query `invoke_query` scope**, which is *designed*
> (rfc-001) but **not yet implemented** — the shipped action is coarse (any stored
> query on the graph, or none; `policy/src/lib.rs:59-73`). Per-actor Cedar curation
> works today for *built-in vs ad-hoc vs admin* tools and for *stored-vs-ad-hoc*;
> sub-selecting individual stored queries per actor is gated on PR 0b ([§12](#12-decisions--locked--open)).

---

## 0. What changed since the first blueprint draft (validation deltas)

The first blueprint of this RFC was authored ~79 commits behind the current head.
Building from it verbatim would miss these; each is corrected in the noted section.

| # | The first draft said | `main` reality | §  |
|---|---|---|---|
| D1 | `ParamKind::Vector { dim: u32 }`, "dim intrinsic, never an `Option`" | `ParamKind::Vector` is a **unit** variant; dim is `ParamDescriptor.vector_dim: Option<u32>` | §7 |
| D2 | handlers / `server_invoke_query` / auth in `lib.rs`; "verified against `{lib.rs,api.rs}`" | moved to **`handlers.rs`**; stored-query config in **`config.rs`**; DTOs in **`api.rs`** | §4–§8 |
| D3 | `ingest` tool wraps `ingest_as`; "(`+BranchCreate` when `from` forks)" | `server_ingest` calls **`load_as`**; missing branch **without `from` → 404**, no implicit fork; `ingest_as` shim retired server-side; `ingest` is now a **deprecated CLI alias of `load`** | §8 |
| D4 | client creds: `servers: { onprem: { endpoint, auth: { token\|oauth } } }`; chain env→**keychain**→file | `OperatorServer { url }` (field is `url`, **no `auth:` block**, "no tokens in this file, ever"); chain is **env → credentials file** (no keychain step) | §10 |
| D5 | tests: `cargo test -p omnigraph-server --test server` (incl. `mcp_*`), `--test server server_opens_s3…` | `server.rs` monolith split; suites are `auth_policy.rs / data_routes.rs / schema_routes.rs / stored_queries.rs / multi_graph.rs / boot_settings.rs / s3.rs / openapi.rs`. S3 → `--test s3` | §11 |
| D6 | "`queries.<name>.mcp.expose` in `omnigraph.yaml`" | field survives (default `true`); multi-graph declaration is **`cluster.yaml graphs.<id>.queries`** with file-discovery (`677320c`); RFC-008 deprecates `omnigraph.yaml` | §7.3 |

Everything else from the blueprint (the crate split, the `McpBackend` seam,
stateless Streamable-HTTP transport, the per-graph routing decision, the
coarse-`invoke_query` caveat, "no `read`/`change` aliases", "no `graphs_list` in
MCP") **validated against code and carries forward unchanged.**

---

## 1. Crate architecture & dependency direction

Two crates. `rmcp` lives **only** in `omnigraph-mcp`; `omnigraph-server` carries
no direct rmcp dep.

```
omnigraph-server (impl McpBackend, all omnigraph logic)
        │   depends on
        ▼
omnigraph-mcp    (rmcp transport, McpBackend trait, rmcp model re-exports)
        │   depends on
        ▼
rmcp 1.7 + tower-http(limit) + axum + http
```

The dependency **must** go `server → mcp`, never the reverse: the server binary
mounts `/mcp`, so `mcp → server` would cycle at the package level
(`server-bin → omnigraph-mcp → server-lib`). The trait inverts it — the crate
defines the seam, the server fills it — which is also why the crate can never
name `AppState`, `GraphHandle`, `do_*`, or `api::*`.

`omnigraph-mcp/Cargo.toml`:

```toml
[package]
name = "omnigraph-mcp"
edition = "2024"           # matches the workspace (omnigraph-server is edition 2024)
version.workspace = true

[dependencies]
rmcp = { version = "1.7", default-features = false, features = ["server", "transport-streamable-http-server"] }
axum = { workspace = true }
http = "1"
tower-http = { workspace = true, features = ["limit"] }
tokio = { workspace = true }
async-trait = { workspace = true }
serde_json = { workspace = true }
```

Workspace edit — add the member (current `members`, `Cargo.toml`):

```toml
members = [
    "crates/omnigraph-compiler",
    "crates/omnigraph",
    "crates/omnigraph-cli",
    "crates/omnigraph-cluster",
    "crates/omnigraph-policy",
    "crates/omnigraph-server",
    "crates/omnigraph-mcp",      # NEW
]
```

In `omnigraph-server/Cargo.toml`: add `omnigraph-mcp = { workspace = true }`;
do **not** add `rmcp`. (Confirmed absent today: `git grep rmcp origin/main --
'**/Cargo.toml'` is empty.)

---

## 2. The `McpBackend` seam — `NEW (build this)` in `omnigraph-mcp`

```rust
// crates/omnigraph-mcp/src/lib.rs
use async_trait::async_trait;

// rmcp model types re-exported so the server uses them via `omnigraph_mcp::…`
// and carries no direct rmcp dependency.
pub use rmcp::model::{
    Annotated, CallToolResult, Content, RawResource, ReadResourceResult, Resource,
    ResourceContents, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
};
pub use rmcp::ErrorData as McpError;
pub type JsonObject = serde_json::Map<String, serde_json::Value>;

#[async_trait]
pub trait McpBackend: Clone + Send + Sync + 'static {
    fn server_info(&self) -> ServerInfo;

    async fn list_tools(&self, parts: &http::request::Parts)
        -> Result<Vec<Tool>, McpError>;

    async fn call_tool(&self, parts: &http::request::Parts, name: &str, args: JsonObject)
        -> Result<CallToolResult, McpError>;

    async fn list_resources(&self, parts: &http::request::Parts)
        -> Result<Vec<Resource>, McpError>;

    async fn read_resource(&self, parts: &http::request::Parts, uri: &str)
        -> Result<ReadResourceResult, McpError>;
}
```

`&http::request::Parts` is the whole decoupling mechanism. The crate hands the
backend the request parts; the backend reads **its own** types out of
`parts.extensions` (§4). The crate never names an omnigraph type. `#[async_trait]`
boxes the futures — negligible at MCP QPS and it sidesteps the
async-fn-in-trait `Send`-bound friction; the server already depends on
`async-trait`.

---

## 3. Transport — `NEW (build this)` in `omnigraph-mcp`

```rust
// crates/omnigraph-mcp/src/transport.rs
use std::sync::Arc;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

pub struct McpHostPolicy {
    pub allowed_hosts: Option<Vec<String>>,    // None ⇒ accept any Host
    pub allowed_origins: Option<Vec<String>>,  // None/empty ⇒ off (non-browser clients send no Origin)
}

pub fn mcp_router<B: McpBackend>(backend: B, body_limit: usize, hosts: McpHostPolicy) -> axum::Router {
    // StreamableHttpServerConfig is #[non_exhaustive] — build from Default, mutate via with_*.
    let config = StreamableHttpServerConfig::default()
        .with_stateful_mode(false)
        .with_json_response(true)
        .with_allowed_hosts(hosts.allowed_hosts)
        .with_allowed_origins(hosts.allowed_origins);

    let svc = StreamableHttpService::new(
        move || Ok(McpService::new(backend.clone())),
        Arc::new(LocalSessionManager::default()),
        config,
    );

    axum::Router::new()
        .route_service("/mcp", svc)
        // rmcp reads the body directly (NOT via an axum extractor), so axum's
        // DefaultBodyLimit does NOT bound /mcp — the tower-http layer does.
        .layer(tower_http::limit::RequestBodyLimitLayer::new(body_limit))
}
```

`McpService<B>` implements rmcp's `ServerHandler`, extracts `&Parts` from
`ctx.extensions` once (missing → `McpError::internal_error`), and delegates each
method to `B`. `get_info → backend.server_info()`; `initialize`/`ping` use rmcp
defaults.

**Stateless mode gives these conformance MUSTs for free** (rmcp 1.7): `GET`/`DELETE
/mcp → 405` with `Allow`; disallowed `Host`/`Origin → 403`; `MCP-Protocol-Version
→ 400` on unsupported, default `2025-03-26` when absent. No conformance
middleware needed.

**Host/Origin policy is derived from the deployment, never a fixed default.**
rmcp's loopback `allowed_hosts` default would `403` every non-loopback client
before bearer auth. The server computes `McpHostPolicy` from its `--bind`: loopback
bind → loopback allow-list (rebind protection matters locally); non-loopback bind
→ allow the configured public host(s) or disable Host-allowlisting and log it
(bearer + `Origin` are the real controls). `allowed_origins` is the browser-attack
control: default off; configurable for browser-based clients.

---

## 4. The extension passthrough — `EXISTING (reuse)` types, `NEW` backend

The two values the backend needs are injected into the request extensions by
middleware that runs **before** the MCP service. Both are real types:

```rust
// EXISTING (reuse) — crates/omnigraph-server/src/identity.rs:187
pub struct ResolvedActor {
    pub actor_id: Arc<str>,
    pub tenant_id: Option<TenantId>,
    pub scopes: Vec<Scope>,
    pub source: AuthSource,        // Static today; Oidc when MR-956 lands
}
```

```rust
// EXISTING (reuse) — crates/omnigraph-server/src/registry.rs:37
pub struct GraphHandle {
    pub key: GraphKey,
    pub uri: String,
    pub engine: Arc<Omnigraph>,
    pub policy: Option<Arc<PolicyEngine>>,        // None ⇒ no per-graph Cedar gate
    pub queries: Option<Arc<QueryRegistry>>,      // None ⇒ no stored queries for this graph
}
```

The middleware order is fixed in `build_app` (`lib.rs:909-956`, see §9): the outer
layer `require_bearer_auth` injects `Extension<ResolvedActor>` (or 401); the inner
layer `resolve_graph_handle` injects `Extension<Arc<GraphHandle>>`. Both land in
`request.extensions()`, which rmcp forwards into `RequestContext.extensions`.

```rust
// NEW (build this) — crates/omnigraph-server/src/mcp/mod.rs
#[derive(Clone)]
pub struct OmnigraphMcpBackend { state: AppState }   // AppState is #[derive(Clone)], Arc-backed

impl OmnigraphMcpBackend {
    fn ctx<'a>(&self, parts: &'a http::request::Parts)
        -> Result<(&'a ResolvedActor, &'a Arc<GraphHandle>), McpError>
    {
        let actor = parts.extensions.get::<ResolvedActor>()
            .ok_or_else(|| McpError::internal_error("actor missing from request extensions", None))?;
        let handle = parts.extensions.get::<Arc<GraphHandle>>()
            .ok_or_else(|| McpError::internal_error("graph handle missing from request extensions", None))?;
        Ok((actor, handle))
    }
}

// The server's thin wrapper — the only place the two crates meet:
pub fn mcp_router(state: AppState) -> axum::Router {
    omnigraph_mcp::mcp_router(
        OmnigraphMcpBackend { state },
        INGEST_REQUEST_BODY_LIMIT_BYTES,        // lib.rs:137 — 32 MiB, reused
        host_policy_from_bind(/* … */),
    )
}
```

---

## 5. Reuse points for dispatch — `EXISTING (reuse)`, all in `handlers.rs`

The backend adds **no business logic**. `call_tool` routes to the same functions
the REST handlers call.

### 5.1 The authorization gate

```rust
// crates/omnigraph-server/src/handlers.rs:336
pub(crate) enum Authz { Allowed, Denied(String) }

// crates/omnigraph-server/src/handlers.rs:357 — Err reserved for operational 401/500
pub(crate) fn authorize(
    actor: Option<&ResolvedActor>,
    policy: Option<&PolicyEngine>,
    request: PolicyRequest,
) -> Result<Authz, ApiError>;

// crates/omnigraph-server/src/handlers.rs:438 — denial ⇒ 403
pub(crate) fn authorize_request(
    actor: Option<&ResolvedActor>,
    policy: Option<&PolicyEngine>,
    request: PolicyRequest,
) -> Result<(), ApiError>;
```

`PolicyRequest` carries **no actor identity** (dropped from the type — the MR-731
invariant) and **no query-name dimension** (the coarse-`invoke_query` caveat):

```rust
// crates/omnigraph-policy/src/lib.rs:252
pub struct PolicyRequest {
    pub action: PolicyAction,
    pub branch: Option<String>,
    pub target_branch: Option<String>,
}
// crates/omnigraph-policy/src/lib.rs:259
pub struct PolicyDecision { pub allowed: bool, pub matched_rule_id: Option<String>, pub message: String }
// crates/omnigraph-policy/src/lib.rs:509
impl PolicyEngine { pub fn authorize(&self, actor_id: &str, request: &PolicyRequest) -> Result<PolicyDecision>; }
```

### 5.2 The read / mutate runners (already decoupled from request bodies)

```rust
// crates/omnigraph-server/src/handlers.rs:731 — self-authorizes Read
pub(crate) async fn run_query(
    handle: Arc<GraphHandle>,
    actor: Option<&ResolvedActor>,
    query: &str,
    name: Option<&str>,
    params_json: Option<&Value>,
    branch: Option<String>,
    snapshot: Option<String>,
    reject_mutations: bool,
) -> Result<(String, ReadTarget, omnigraph_compiler::result::QueryResult), ApiError>;

// crates/omnigraph-server/src/handlers.rs:665 — self-authorizes Change + admission
pub(crate) async fn run_mutate(
    state: AppState,
    handle: Arc<GraphHandle>,
    actor: Option<&ResolvedActor>,
    query: &str,
    name: Option<&str>,
    params_json: Option<&Value>,
    branch: String,
) -> Result<ChangeOutput, ApiError>;
```

`run_query` returns the raw `QueryResult`; project it for the wire with the
existing `read_output(query_name, &target, result) -> ReadOutput` (`api.rs:634`).

### 5.3 The canonical double-gate + deny-as-404 pattern to mirror

`server_invoke_query` (`handlers.rs:933`) is exactly the shape `call_tool` must
reproduce for stored-query tools — reproduced here as the contract:

```rust
// EXISTING (reuse pattern) — handlers.rs:953  (outer InvokeQuery gate, deny == missing)
match authorize(actor_ref, handle.policy.as_deref(), PolicyRequest {
    action: PolicyAction::InvokeQuery,
    branch: None,            // graph-scoped: NO branch dimension on the outer gate
    target_branch: None,
})? {
    Authz::Allowed => {}
    Authz::Denied(_) => return Err(ApiError::not_found(NOT_FOUND)),  // "stored query not found"
}
let stored = handle.queries.as_ref()
    .and_then(|r| r.lookup(&name))
    .ok_or_else(|| ApiError::not_found(NOT_FOUND))?;                 // same 404 on a miss
// … then the inner gate runs inside run_mutate (Change) / run_query (Read).
if is_mutation {
    if req.snapshot.is_some() {                                      // mutation-against-snapshot unrepresentable
        return Err(ApiError::bad_request("stored mutation cannot target a snapshot"));
    }
    // run_mutate(…)  ← inner Change gate
} else {
    // run_query(…)   ← inner Read gate
}
```

For MCP, the same masking principle applies but the wire encoding differs: an
**unknown tool OR a denied tool** returns the identical `unknown tool: <name>`
JSON-RPC error (`-32602`) so the catalog can't be probed without the grant (§6).

### 5.4 Error classification — `NEW (build this)`, one mapper

`ApiError`'s fields are **private** (`lib.rs:296`), so add two accessors:

```rust
// EXISTING shape — crates/omnigraph-server/src/lib.rs:296
pub struct ApiError {
    status: StatusCode, code: ErrorCode, message: String,
    merge_conflicts: Vec<api::MergeConflictOutput>,
    manifest_conflict: Option<api::ManifestConflictOutput>,
}

// NEW: add accessors so the MCP classifier can read status/message.
impl ApiError {
    pub(crate) fn status_code(&self) -> StatusCode { self.status }
    pub(crate) fn message_str(&self) -> &str { &self.message }
}
```

```rust
// NEW (build this) — the single source of truth, used at EVERY dispatch site
fn classify(r: Result<CallToolResult, ApiError>) -> Result<CallToolResult, McpError> {
    match r {
        Ok(out) => Ok(out),
        Err(e) if e.status_code().is_client_error() =>          // 4xx / 409 → model self-corrects
            Ok(CallToolResult::error(vec![Content::text(e.message_str())])),  // isError: true
        Err(e) => Err(McpError::internal_error(e.message_str().to_owned(), None)),  // 5xx → JSON-RPC error
    }
}
```

One `classify`, not the `api_error_to_tool` / `api_operational_error` split that
drifted in the reference impl. A missing/bad bearer is an HTTP 401 at the boundary
*before* rmcp.

---

## 6. Tool catalog (13 built-ins) + Cedar mapping

Each built-in reuses the **exact `PolicyAction` its REST route enforces**. The full
action vocabulary on `main`:

```rust
// EXISTING — crates/omnigraph-policy/src/lib.rs:18
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
| `snapshot`, `schema_get`, `branches_list`, `commits_list`, `commits_get` | graph | `Read` |
| `query` (ad-hoc read) | graph | `Read` (`run_query` self-authorizes) |
| `mutate` (ad-hoc write) | graph | `Change` |
| `load` (NDJSON; **renamed from `ingest`** — see §8) | graph | `Change` (+ `BranchCreate` **iff `from` present**) |
| `branches_create` / `branches_delete` / `branches_merge` | graph | `BranchCreate` / `BranchDelete` / `BranchMerge` |
| `schema_apply` (`allow_data_loss`) | graph | `SchemaApply` |
| *stored query* | graph | `InvokeQuery` (coarse) then inner `Read`/`Change` |

Locked decisions (validated): **tool ids are `query`/`mutate` only** — no
`read`/`change` aliases (the REST surface already deprecated `/read`, `/change`; a
fresh MCP has no legacy clients). **Ad-hoc `query`/`mutate` are always exposed,
Cedar-only** — no `mcp.allow_adhoc`. **No `graphs_list` / `omnigraph://graphs`** —
graph discovery stays REST-only via `GET /graphs` (server-scoped `GraphList`, §9).

Annotations (rmcp defaults `destructiveHint` **and** `openWorldHint` to true — set
explicitly): read tools → `read_only(true).open_world(false)`;
`mutate`/`load`/`branches_delete`/`branches_merge`/`schema_apply` →
`read_only(false).destructive(true).open_world(false)`; `branches_create`
(additive) → `destructive(false)`.

Represent built-ins as a `Builtin` enum (one variant per tool; `descriptor`/`gate`/`call`
as match arms) — lower liability than ~13 unit structs + `dyn` + `async-trait`.
Stored-query tools are a sibling populator over `handle.queries`.

`list_tools`/`list_resources` are Cedar-filtered by running the **same**
authorization the call path runs, with **default args (branch `main`)** — not a
`branch: None` probe (which matches no `branch_scope` rule and would hide callable
tools). Over-showing a branch-scoped grant is the safe direction; `call_tool` is
the authoritative gate. (The contract: list never shows a tool the actor can't ever
call; for branch-scoped tools it may show one callable only on some branches.)

---

## 7. Stored-query projection — `EXISTING (reuse)` + the **corrected** schema

The projection source is the same `query_catalog_entry` the `GET /queries` catalog
uses. The real types (note `Vector` is a **unit** variant; the dim is a separate
`Option<u32>` field — this is delta **D1**):

```rust
// EXISTING — crates/omnigraph-server/src/api.rs:346
pub enum ParamKind { String, Bool, Int, BigInt, Float, Date, DateTime, Blob, Vector, List }

// crates/omnigraph-server/src/api.rs:362
pub struct ParamDescriptor {
    pub name: String,
    pub kind: ParamKind,
    pub item_kind: Option<ParamKind>,   // Some(scalar) when kind == List
    pub vector_dim: Option<u32>,        // Some(dim) when kind == Vector  ← dim lives HERE, not in the kind
    pub nullable: bool,
}

// crates/omnigraph-server/src/api.rs:453
pub fn query_catalog_entry(query: &StoredQuery) -> QueryCatalogEntry;  // → { name, tool_name, description, instruction, mutation, params }
```

```rust
// EXISTING — crates/omnigraph-server/src/queries.rs:32
pub struct StoredQuery {
    pub name: String,
    pub source: Arc<str>,
    pub decl: QueryDecl,
    pub expose: bool,             // default true
    pub tool_name: Option<String>,
}
impl StoredQuery {
    pub fn is_mutation(&self) -> bool;           // queries.rs:51
    pub fn effective_tool_name(&self) -> &str;   // queries.rs:60 — tool_name override else name
}
pub struct QueryRegistry { /* by_name: BTreeMap<String, StoredQuery> */ }   // queries.rs:67; .lookup(&name)
```

### 7.1 `ParamDescriptor → JSON Schema` — `NEW (build this)`, corrected for the real types

```rust
// NEW (build this) — corrects the blueprint's nonexistent `ParamKind::Vector { dim }`.
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
        // Vector/List handled by param_schema (they carry sub-structure).
        ParamKind::Vector | ParamKind::List => unreachable!("composite kinds handled in param_schema"),
    }
}

fn param_schema(p: &ParamDescriptor) -> Value {
    match p.kind {
        ParamKind::Vector => {
            let mut s = json!({ "type": "array", "items": { "type": "number" } });
            // dim is Option<u32> on the descriptor. Present ⇒ constrain length.
            // Absent ⇒ OMIT minItems/maxItems — never emit 0 (a 0-length-array
            // schema rejects all input; this is the footgun the first draft
            // misattributed to a type that doesn't exist).
            if let Some(dim) = p.vector_dim {
                s["minItems"] = json!(dim);
                s["maxItems"] = json!(dim);
            }
            s
        }
        ParamKind::List => {
            let item = p.item_kind.map(scalar_schema).unwrap_or_else(|| json!({ "type": "string" }));
            json!({ "type": "array", "items": item })  // grammar guarantees scalar items
        }
        scalar => scalar_schema(scalar),
    }
}
```

### 7.2 Envelope (correct by construction)

The tool's `input_schema` **nests query params under `params`**, mirroring
`POST /queries/{name}`, so the invocation knobs and the query's own params live in
separate namespaces and cannot collide:

```jsonc
{ "type": "object",
  "properties": {
    "params": { "type": "object", "properties": { /* per-param param_schema(...) */ }, "required": [ /* nullable==false */ ] },
    "branch":   { "type": "string" },
    "snapshot": { "type": "string" }   // OMIT for mutation tools (mutation-against-snapshot is unrepresentable)
  },
  "additionalProperties": false }
```

Dispatch reads query params from `args.params` and knobs from `args.branch` /
`args.snapshot`, so a query parameter literally named `branch`/`snapshot` is
harmless. Skip a stored tool whose `effective_tool_name()` collides with a
built-in (built-ins win). The outer gate is coarse `InvokeQuery`; the inner gate
is the query body's `Read`/`Change` — the same double-gate as §5.3.

### 7.3 Where stored queries are declared (delta D6)

`expose` + `tool_name` survive with default `expose: true` (`config.rs:132`,
`queries.rs:43`). The **declaration source** moved: multi-graph servers declare
queries in **`cluster.yaml graphs.<id>.queries`** with Terraform-shaped file
discovery (`queries: queries/` discovers top-level `*.gq`, loud on parse/dup
errors — `677320c`); the single-graph `omnigraph.yaml queries:` map still parses
but is the **legacy** surface RFC-008 deprecates. The MCP projection is agnostic
to the source — it reads the loaded `QueryRegistry` on `handle.queries` — but the
blueprint's "`in omnigraph.yaml`" phrasing is corrected to "the graph's loaded
stored-query registry (`cluster.yaml` discovery, or legacy `omnigraph.yaml`)."

---

## 8. The `load` tool — `EXISTING (reuse)`, ingest/load unified (delta D3)

The first draft described an `ingest` tool wrapping `ingest_as` with implicit
fork-from-`main`. **That is gone.** `server_ingest` now calls `load_as` directly and
fork is opt-in by `from`:

```rust
// EXISTING — crates/omnigraph-server/src/handlers.rs:1210  (abridged to the load-bearing logic)
let branch = request.branch.unwrap_or_else(|| "main".to_string());
let from = request.from;                                  // Option<String>
let mode = request.mode.unwrap_or(LoadMode::Merge);
if !branch_exists {
    match from.as_deref() {
        None => return Err(ApiError::not_found(           // ← no implicit fork; a typo'd branch is a 404
            format!("branch '{branch}' not found; pass `from` to create it"))),
        Some(from) => authorize_request(actor, handle.policy.as_deref(), PolicyRequest {
            action: PolicyAction::BranchCreate,           // ← BranchCreate consulted ONLY when a fork happens
            branch: Some(from.to_string()), target_branch: Some(branch.clone()),
        })?,
    }
}
authorize_request(actor, handle.policy.as_deref(), PolicyRequest {
    action: PolicyAction::Change, branch: Some(branch.clone()), target_branch: None,
})?;
let result = handle.engine.load_as(&branch, from.as_deref(), &request.data, mode, actor_id).await?;  // unified load_as
```

DTOs (`api.rs:497` / `:208`):

```rust
pub struct IngestRequest {                 // request body shape unchanged on the wire
    pub branch: Option<String>,            // default "main"; without `from`, must already exist (else 404)
    pub from:   Option<String>,            // parent branch; presence = opt into fork-if-missing
    pub mode:   Option<LoadMode>,          // default Merge
    pub data:   String,                    // NDJSON: {"type":"<T>","data":{…}} per line
}
pub struct IngestOutput {
    pub uri: String, pub branch: String,
    pub base_branch: Option<String>,       // echoes `from`; NULL when absent (was non-null in the blueprint era)
    pub branch_created: bool, pub mode: LoadMode,
    pub tables: Vec<IngestTableOutput>, pub actor_id: Option<String>,
}
```

**Spec decisions for the MCP tool:**

1. **Name the tool `load`, not `ingest`.** The CLI now ships `ingest` only as a
   *"Deprecated alias of `load --from <base>`"* (`cli.rs:111`); `load` is the
   unified command. A fresh MCP with no legacy clients should expose the canonical
   id — consistent with §6's "no `read`/`change` aliases" decision. (The HTTP route
   stays `POST /ingest` for back-compat; only the MCP tool id is canonicalized.)
2. **`do_load` wraps `load_as`** via the same body as `server_ingest`.
3. **Input schema carries `from?`** and documents the 404-on-missing-branch:
   `{ data: string, branch?: string, from?: string, mode?: "merge"|"append"|"overwrite" }`,
   `additionalProperties: false`. There is no silent fork-from-main to represent.

---

## 9. Routing — `EXISTING (reuse)`, validated

`/mcp` is added **into `per_graph_protected`**, so it is flat in single mode and
nested under `/graphs/{graph_id}` in multi mode automatically — no separate
wiring. The real `build_app` (`lib.rs:907`):

```rust
// EXISTING — crates/omnigraph-server/src/lib.rs:915 (abridged)
let per_graph_protected = Router::new()
    .route("/snapshot", get(server_snapshot))
    // … /query /mutate /queries /queries/{name} /schema /schema/apply /ingest /branches /commits …
    .route("/mcp", post(server_mcp))                          // ← ADD HERE (or .merge(mcp_router(state)))
    .route_layer(middleware::from_fn_with_state(state.clone(), resolve_graph_handle))  // inner: injects Arc<GraphHandle>
    .route_layer(middleware::from_fn_with_state(state.clone(), require_bearer_auth));  // outer: injects ResolvedActor / 401

let management = Router::new()
    .route("/graphs", get(server_graphs_list))               // GraphList — server-scoped, NOT in MCP
    .route_layer(middleware::from_fn_with_state(state.clone(), require_bearer_auth));

let protected = match state.routing() {
    GraphRouting::Single { .. } => per_graph_protected.merge(management),                  // → POST /mcp
    GraphRouting::Multi  { .. } => Router::new()
        .nest("/graphs/{graph_id}", per_graph_protected).merge(management),                // → POST /graphs/{id}/mcp
};
```

Because `mcp_router` returns its own `Router` with a tower-http body-limit layer,
prefer `.merge(mcp::mcp_router(state.clone()))` into `per_graph_protected` over a
bare `.route("/mcp", …)` so the `/mcp`-specific limit doesn't leak onto the other
routes. **No server-scoped MCP** — `graphs_list` / `omnigraph://graphs` are dropped;
graph discovery is the `management` group's `GET /graphs`. A future server-level
flat `/mcp` (bearer-only, no handle) would live in `management`, but is not built
speculatively. **Do not** consolidate to a single flat `/mcp` that takes `graph_id`
per call: MCP's `tools/list` can't carry a graph, so it couldn't list per-graph
stored-query tools, and it would break isolation.

---

## 10. Auth & the client on-ramp — `EXISTING (reuse)` RFC-007 (delta D4)

**Server side** (unchanged from the blueprint and correct): the backend consumes an
already-resolved `ResolvedActor` and **branches on nothing** about how the token was
verified. Static bearer works today; OAuth/RFC-9728 (MR-956) is an additive layer
that swaps the bearer middleware behind a `TokenVerifier` and serves
`/.well-known/oauth-protected-resource` — **zero MCP changes**. Token validation is
offline (cached JWKS), so on-prem/air-gapped keeps working with no request-path
cloud dependency; the endpoint is a Resource Server only (never issues tokens,
never holds a client secret). Multi-user identity passes through: the *caller's*
token (a per-tenant audience-bound JWT) reaches the server so Cedar enforces
per-user policy — never a shared service token.

```
request → [require_bearer_auth: ResolvedActor] → [resolve_graph_handle: GraphHandle] → [/mcp] → Cedar → tool
```

| Integration | Static bearer | Note |
|---|---|---|
| Claude Code, Cursor, VS Code | ✅ | `claude mcp add --transport http <url>/mcp --header "Authorization: Bearer <tok>"` |
| Claude Messages API MCP connector | ✅ | caller passes `authorization_token` |
| claude.ai web / Desktop, ChatGPT dev-mode | ❌ needs OAuth fast-follow | OAuth 2.1 + PKCE + RFC 9728 |

**Client side — the *shipped* RFC-007 model (delta D4).** The landed operator config
is:

```rust
// EXISTING — crates/omnigraph-cli/src/operator.rs:74
pub(crate) struct OperatorServer { pub(crate) url: String }   // field is `url`, NOT `endpoint`; NO `auth:` block
```

```yaml
# ~/.omnigraph/config.yaml  (operator config — "No tokens in this file, ever")
servers:
  prod:    { url: https://og.internal:8080 }
  edge:    { url: https://og-edge:8080 }
  cloud:   { url: https://api.omnigraph.cloud }     # OAuth is resolved by the client, not declared here
```

Token resolution is a **two-step keyed chain** (`operator.rs:224`), then the legacy
`bearer_token_env` fallback — **there is no keychain step** (the blueprint's middle
`omnigraph:<name>` keychain link does not exist):

```rust
// EXISTING — crates/omnigraph-cli/src/operator.rs:229
// 1. OMNIGRAPH_TOKEN_<NAME>  (env; `intel-dev` → OMNIGRAPH_TOKEN_INTEL_DEV)
// 2. [<name>] token = …      in ~/.omnigraph/credentials  (0600; over-permissive ⇒ loud error)
pub(crate) fn resolve_keyed_token(server: &str) -> Result<Option<String>>;
```

The `omnigraph mcp install --agent <tool>` on-ramp (MR-974) writes the agent's MCP
client config pointing at `<server url>/mcp` with the resolved bearer; it reuses
this chain, so OAuth becomes a future *sibling* of the token chain, not a re-key.

---

## 11. Tests & verification (corrected suite names — delta D5)

The `server.rs` monolith is gone. MCP tests land in a **new
`crates/omnigraph-server/tests/mcp.rs`** suite (black-box over `build_app`);
stored-query *projection* tests extend the existing **`stored_queries.rs`**. Cover:

- **Protocol:** `initialize` + advertised `{tools, resources}`; `tools/list` shape;
  `tools/call` happy path; JSON-RPC errors (`-32601`/`-32602`); `resources/list` +
  `resources/read`; `GET /mcp → 405`; `MCP-Protocol-Version` 400/default; `Origin → 403`.
- **Cedar (coarse):** read-only actor sees read tools but not `mutate`/`load`/`branches_*`/`schema_apply`;
  a denied `tools/call` masks byte-identically to an unknown tool; stored queries
  list only with `invoke_query`; the double-gate (an `invoke_query`-only actor sees
  a stored tool but the call surfaces `isError` when the inner `Read` denies).
- **Dispatch:** a `mutate` call writes end-to-end (proves the actor/handle extension
  passthrough); a malformed query → `isError:true`, not a JSON-RPC error. A `load`
  with a missing branch and **no `from` → `isError`** (404 classified); with `from`
  → forks.
- **Resources:** list + read of `schema`/`branches`; a denied read masks as unknown.
- **Schema generation:** table-driven over every `ParamKind` incl. nullable / list /
  `vector` **with and without `vector_dim`** (the absent-dim path must omit
  `minItems`/`maxItems`, never emit 0 — the D1 regression guard).
- **Auth decoupling / no-bearer:** `/mcp` 401s without a bearer (before rmcp) and
  200s with one; green under the static-hash verifier and a mock `ResolvedActor`.
- **Crate-level:** `omnigraph-mcp/tests/` with a trivial `McpBackend` proving the
  crate stands alone (`initialize` + `GET → 405`), plus an rmcp **surface guard**
  pinning `StreamableHttpServerConfig`'s `with_*` setters and the `ServerHandler`
  method shapes (the only place the unverifiable rmcp-`Parts`-passthrough assumption
  is anchored).

Verification commands (corrected):

```bash
cargo build --workspace --locked
cargo tree -p omnigraph-server -e normal | grep rmcp     # rmcp ONLY under omnigraph-mcp, never direct
cargo test -p omnigraph-server --test mcp                # NEW suite (NOT --test server)
cargo test -p omnigraph-server --test stored_queries     # projection tests
cargo test -p omnigraph-server --test openapi            # no /mcp leak (it carries no #[utoipa::path])
OMNIGRAPH_UPDATE_OPENAPI=1 cargo test -p omnigraph-server --test openapi   # if the REST surface intentionally changed
```

---

## 12. Decisions — locked & open

**Locked** (validated against code): rmcp 1.7; stateless POST-only Streamable
HTTP; `McpBackend` crate-trait seam with `&Parts` passthrough; `Builtin` enum +
stored-query populator; coarse `InvokeQuery` only; ad-hoc `query`/`mutate`
always-on Cedar-only; `query`/`mutate` ids (no `read`/`change`); **`load` id (not
`ingest`)**; per-graph `/mcp` routing; no server-scoped MCP / no `graphs_list`;
text-JSON content for v1; BigInt as JSON string; `vector_dim: Option<u32>` handled
with omit-on-absent.

**Open (deferred follow-ups):**
- **PR 0b — per-query `invoke_query` scope.** Add a query-name dimension to
  `PolicyRequest` + the Cedar schema (rfc-001's intended design). `InvokeQuery` is
  documented coarse today (`policy/src/lib.rs:59-73`) and `branch_scope` on it is
  rejected by `validate()`. Until PR 0b, stored-query curation is graph-level
  (registry membership + `expose`), not per-actor sub-selection — the headline caveat.
- **MR-956 — OAuth/RFC-9728** layer (additive, zero MCP changes).
- **stdio → proxy collapse** (see below).
- **`structuredContent` / `outputSchema`** beyond text-JSON.

---

## Relationship to RFC-001

[rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md) (MR-656 / MR-976
/ MR-969) is the parent design for stored queries + the response envelope + MCP.
This RFC is the **detailed MCP-transport design** that #128 left for a follow-up,
and it **revises rfc-001 in three places where the shipped code or the MCP wire
protocol diverged from rfc-001's sketch**:

1. **Transport shape.** rfc-001 sketched `GET /mcp/tools` + `POST /mcp/invoke` (a
   bespoke REST pair). **That is not the MCP wire protocol — real MCP clients
   cannot connect to it.** This RFC implements actual MCP JSON-RPC over Streamable
   HTTP and reuses `query_catalog_entry` as a *projection source*, not a parallel
   surface.
2. **Exposure config.** rfc-001 specified inline `.gq` pragmas (`@mcp(expose=…)`,
   default `expose=false`). **#128 shipped a different mechanism:** the
   `expose`/`tool_name` metadata in YAML, **default `true`** (declaring a query *is*
   the opt-in). This RFC builds on the shipped form; the `.gq`-pragma design is
   superseded. (The declaration *file* has since moved to `cluster.yaml` discovery
   for multi-graph servers — see [§7.3](#73-where-stored-queries-are-declared-delta-d6).)
3. **Schema introspection.** rfc-001 lists "Schema introspection through MCP" as a
   **non-goal**. This RFC **revises that**: the operational-parity tools include
   `schema_get` and `omnigraph://schema` — *because the shipped stdio package
   already exposes both*. The non-goal is achieved by *policy*, not omission: both
   are Cedar-gated by `Read`, and the recommended locked-down agent policy denies
   `Read`, so a curated agent still never sees the schema.

Everything else in rfc-001 (two-paths-one-engine, per-query `invoke_query` *as the
intended scope*, the response envelope, multi-graph per-graph endpoints) this RFC
consumes unchanged.

> **Numbering note:** the `TokenVerifier`/WorkOS auth design is referred to in code
> (`crates/omnigraph-server/src/identity.rs`) as "RFC 0001," a *different* document
> from this repo's `docs/dev/rfc-001-queries-envelope-mcp.md`. To avoid the
> collision this RFC cites the auth substrate as **MR-956** throughout.

---

## Motivation

- **One curated, safe, remotely-reachable tool surface.** MR-969's thesis: hand an
  LLM a token Cedar-scoped to a set of tools and it sees exactly those typed tools —
  cannot construct ad-hoc queries it isn't permitted, cannot read the schema it
  isn't permitted, cannot reach other graphs. Today the only MCP is the stdio
  package: local-only, full surface, ungated.
- **Parity, so the in-server MCP can be the single implementation.** Operators/agents
  already depend on the operational tools. Serving them server-side behind one Cedar
  gate lets the stdio package degrade to a proxy and removes two diverging tool sets.
- **On-prem and cloud from one endpoint.** A managed cloud (WorkOS OAuth) and an
  on-prem/air-gapped deploy (static or customer-OIDC tokens) must serve the same MCP
  without forks or MCP-specific auth.
- **Foundation for the agent on-ramp (MR-974).** `omnigraph mcp install --agent
  <tool>` needs a decided transport + a stable endpoint.

## Goals

- Project built-in tools + stored queries as MCP tools through **one** registry
  abstraction.
- `tools/list` and the callable set are **identical for argument-independent
  authorization**, both driven by Cedar (see §6 for the branch-scoped caveat).
- The MCP layer is **auth-method-agnostic**: it consumes `ResolvedActor`, never a
  raw token, never branches on how auth happened.
- The same endpoint works on-prem (static/OIDC) and cloud (WorkOS OAuth), switched
  by config; cloud OAuth is additive (RFC 9728).
- No new business logic: MCP tools delegate to the same `run_query`/`run_mutate`/
  branch/schema functions the HTTP routes call.
- Behaviour-neutral when unused: no MCP traffic = no change.

## Non-Goals

- **Building/hosting an OAuth authorization server.** The server is a Resource
  Server; WorkOS AuthKit+Connect is the AS (MR-956). The MCP endpoint validates
  tokens, never issues them, never holds client secrets.
- **OAuth/WorkOS implementation itself** — MR-956's work. This RFC leaves a clean
  RFC-9728 hook and consumes `ResolvedActor`.
- **MCP prompts, elicitation, `tools/list_changed`, resource subscriptions,
  server-initiated messages.** None needed → enables the stateless POST-only
  transport (§3).
- **stdio transport inside the server.** stdio stays in the TS package (now a proxy).
- **Cross-graph tool listing.** Per-graph catalogs only.
- **Hot reload of the query registry.** Restart-only (MR-969).

## Background

`omnigraph-server` (Axum) already implements every operation this RFC exposes as an
authenticated HTTP route; each authorizes via a `PolicyAction` against the Cedar
policy for a server-resolved actor and calls into the engine. The existing stdio MCP
package is a *client* of these routes (it owns no business logic). MR-956 will
introduce a `TokenVerifier` trait (`StaticHashTokenVerifier` today inline,
`OidcJwtVerifier` for OIDC/WorkOS) producing the `ResolvedActor` that already exists
in `identity.rs` (§4) and is consumed by Cedar — token *validation* is offline
(cached JWKS), so on-prem/air-gapped has no request-path dependency on the cloud.

## Relationship to the `@modernrelay/omnigraph-mcp` stdio package

The package (`omnigraph-ts`, `@modelcontextprotocol/sdk`, **stdio only**) is a thin
client over the SDK → HTTP routes and **forwards the caller's bearer verbatim** (no
inspection). It exposes the operational tools and the `omnigraph://schema` /
`omnigraph://branches` resources (plus a vendored best-practices cookbook). *Its
exact tool/resource counts and version live in the separate `omnigraph-ts` repo and
are not re-verified here.*

Once parity lands, **collapse to one implementation**: the in-server MCP is canonical
(Cedar-gated, remote-capable, the path that becomes a Claude-web connector via
MR-956). The stdio package degrades to a **thin stdio↔HTTP proxy** forwarding
JSON-RPC (and the incoming `Authorization`) to `/mcp` — staying the local on-ramp
for Claude Code/Desktop while sharing one tool set and one Cedar gate. This is the
same "one contract, two implementations only where semantics genuinely differ"
posture as [rfc-009-unify-access-paths.md](rfc-009-unify-access-paths.md).
