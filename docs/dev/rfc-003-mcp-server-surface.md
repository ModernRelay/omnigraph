# RFC: MCP Server Surface for `omnigraph-server` — Full Tool Parity, Stored Queries, Modular Auth

**Status:** Proposed
**Date:** 2026-06-01
**Tickets:** MR-969 (stored queries + MCP exposure — the surface this completes), MR-956 (federated auth / WorkOS OAuth — the auth substrate this consumes), MR-971 (per-server credential resolver), MR-974 (agent setup surface — the installer that wires this), MR-668 (multi-graph server — shipped, the routing this builds on)
**Builds on:** [omnigraph#128](https://github.com/ModernRelay/omnigraph/pull/128) (`ragnorc/stored-queries-mcp`) — the shipped stored-query registry, `GET /queries`, `POST /queries/{name}`, and the coarse `invoke_query` gate.
**Supersedes:** the MCP-transport portion of [rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md) (`/mcp/tools` + `/mcp/invoke`). See [Relationship to RFC-001](#relationship-to-rfc-001).
**Target release:** v0.8.x (phased — see Rollout)

## Summary

Add a first-class **MCP (Model Context Protocol) server surface to `omnigraph-server`**, exposed over **Streamable HTTP**, that projects the server's operations as MCP tools and resources for LLM clients (Claude Code/Desktop/web, Cursor, etc.). Two populations of tools share one projection path:

1. **Built-in operational tools** — parity with the existing `@modernrelay/omnigraph-mcp` stdio package's **13 tools** (`health`, `snapshot`, `read`, `schema_get`, `branches_list`, `commits_list`, `commits_get`, `change`, `ingest`, `branches_create`, `branches_delete`, `branches_merge`, `schema_apply`) and its **2 resources** (`omnigraph://schema`, `omnigraph://branches`), plus a new server-scoped `graphs_list` tool and an `omnigraph://graphs` resource (multi-graph mode).
2. **Dynamic stored-query tools** — one MCP tool per `mcp.expose: true` entry in the `queries:` registry (MR-969 / #128), with parameters typed from the `.gq` declaration via the shipped `query_catalog_entry` / `param_descriptor` projection.

Every tool is **authorized by the server's existing Cedar policy engine**. The MCP layer never implements its own authentication: it consumes an **already-resolved `ResolvedActor`** from the server's bearer middleware (`require_bearer_auth` today; the `TokenVerifier` seam when MR-956 lands), so the **same MCP endpoint serves on-prem (static or customer-OIDC tokens) and our cloud (WorkOS OAuth) by configuration only**. Cloud OAuth is an additive layer (RFC 9728 protected-resource metadata) that slots in with zero MCP changes.

The end-state collapses two diverging tool implementations into one: the in-server MCP is the canonical, Cedar-gated, remotely-reachable surface; the stdio package becomes a thin stdio↔HTTP proxy (local on-ramp) over it.

> **Key caveat, stated up front (see §5.9 below):** the headline "a token scoped via Cedar to a *specific set* of stored queries" requires **per-query `invoke_query` scope**, which is *designed* (rfc-001) but **not yet implemented** — the shipped action is coarse (any stored query on the graph, or none). Per-actor Cedar curation works today for *built-in vs ad-hoc vs admin* tools and for *stored-vs-ad-hoc*; sub-selecting individual stored queries per actor is gated on a prerequisite (PR 0b). Until then, stored-query curation is graph-level (registry membership + `mcp.expose`).

## Relationship to RFC-001

[rfc-001-queries-envelope-mcp.md](rfc-001-queries-envelope-mcp.md) (MR-656 / MR-976 / MR-969) is the parent design for stored queries + the response envelope + MCP. This RFC is the **detailed MCP-transport design** that #128 left for a follow-up, and it **revises rfc-001 in three places where the shipped code or the MCP wire protocol diverged from rfc-001's sketch**:

1. **Transport shape.** rfc-001 sketched `GET /mcp/tools` + `POST /mcp/invoke` (a bespoke REST pair). **That is not the MCP wire protocol — real MCP clients cannot connect to it.** This RFC implements actual MCP JSON-RPC over Streamable HTTP and reuses `query_catalog_entry` as a *projection source*, not a parallel surface. (rfc-001's own Open Question already leaned toward Streamable HTTP.)
2. **Exposure config.** rfc-001 specified inline `.gq` pragmas (`@mcp(expose=…)`, default `expose=false`). **#128 shipped a different mechanism:** YAML `queries.<name>.mcp.expose` in `omnigraph.yaml`, **default `true`** (declaring a query in the manifest *is* the opt-in). This RFC builds on the shipped YAML form; the `.gq`-pragma design in rfc-001 is superseded for exposure.
3. **Schema introspection.** rfc-001 lists "Schema introspection through MCP" as a **non-goal** ("agents see types through declared return shapes"). This RFC **revises that**: the operational-parity tools include `schema_get` and `omnigraph://schema` — *because the shipped stdio package already exposes both*. The non-goal is achieved by *policy*, not omission: `schema_get`/`omnigraph://schema` are Cedar-gated by `Read`, and the recommended locked-down agent policy denies `Read`, so a curated agent still never sees the schema. (rfc-001's intent is preserved; the mechanism moves from "don't build it" to "build it, gate it.")

Everything else in rfc-001 (two-paths-one-engine, per-query `invoke_query` *as the intended scope*, the response envelope, multi-graph per-graph endpoints) this RFC consumes unchanged.

> **Numbering note:** the `TokenVerifier`/WorkOS auth design is referred to in code (`crates/omnigraph-server/src/identity.rs`) as "RFC 0001," which is a *different* document from this repo's `docs/dev/rfc-001-queries-envelope-mcp.md`. To avoid the collision this RFC cites the auth substrate as **MR-956** throughout, never "RFC 0001."

## Reconciliation with shipped code (verified against `ragnorc/stored-queries-mcp` HEAD)

Verified against `crates/omnigraph-server/src/{lib.rs,api.rs}` and `crates/omnigraph-policy/src/lib.rs` at the current branch head (not the #128 PR body, and not `api.rs` alone):

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

Verified surface of the package (`omnigraph-ts`, pkg version `0.3.0`, `@modelcontextprotocol/sdk@^1.29.0`, **stdio only**): **13 tools** (`health`, `snapshot`, `read`, `schema_get`, `branches_list`, `commits_list`, `commits_get`, `change`, `ingest`, `branches_create`, `branches_delete`, `branches_merge`, `schema_apply`) and **2 resources** (`omnigraph://schema`, `omnigraph://branches`). It is a thin client over the SDK → HTTP routes and **forwards the caller's bearer verbatim** (no inspection).

Once parity lands, **collapse to one implementation**: the in-server MCP is canonical (Cedar-gated, remote-capable, the path that becomes a Claude-web connector via MR-956). The stdio package degrades to a **thin stdio↔HTTP proxy** forwarding JSON-RPC (and the incoming `Authorization`) to `/mcp` — staying the local on-ramp for Claude Code/Desktop while sharing one tool set, one Cedar gate. Transition: keep the current independent stdio package on its `0.3.x`/`0.6.x` line; ship proxy mode in a later TS minor once the server endpoint is GA. (Note: the package is currently several minors behind the server — its vendored `spec/openapi.json` predates the stored-query routes — so it needs the standard re-sync regardless of MCP work.)

## Testing

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

1. **BigInt/u64 as JSON string** (recommended, precision-safe) vs number.
2. **`rmcp` vs hand-rolled** JSON-RPC (spike `rmcp` on edition 2024; default to hand-roll on friction).
3. **Default-off `mcp.allow_adhoc`** for ad-hoc `query`/`mutate` (recommended) vs always-on + Cedar-only.
4. **`structuredContent` + `outputSchema`** now vs text-JSON v1 (recommend v1 text-JSON).
5. **Endpoint paths:** `/mcp` + `/graphs/{id}/mcp` — confirm naming and whether server-scoped tools also appear on the per-graph endpoint.
6. **Stateless POST-only** confirmed (no near-term server-initiated messages) — revisit only if subscriptions land.
7. **Legacy alias tools** (`read`/`change`): keep for client compat (the shipped package uses them), or drop and rely on `query`/`mutate`?
8. **PR 0b shape:** per-query scope as a Cedar *resource* (`StoredQuery::"find_user"`) vs a `query_name` *context attribute* + policy condition — affects how `allow InvokeQuery [list]` is authored.
