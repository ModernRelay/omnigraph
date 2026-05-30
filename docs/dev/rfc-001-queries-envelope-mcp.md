# RFC: Inline + Stored Queries, Request/Response Envelope, MCP

**Status:** Proposed
**Date:** 2026-05-28
**Tickets:** MR-656 (inline `-e` + URL rename), MR-668 (multi-graph, shipped), MR-976 (Phase 1 envelope parent: MR-977 / MR-978 / MR-979 / MR-980), MR-969 (stored queries + MCP)
**Target release:** v0.6.x patch series (MR-656 + Phase 1) → v0.7.0 (MR-969 PRs 1-3)

## Summary

OmniGraph today exposes `POST /read` and `POST /change` with a weakly-contracted body (counts only on writes) and no per-query authorization. This RFC consolidates the work landing across three Linear tickets into one coherent design:

1. **MR-656**: rename `/read` → `/query` and `/change` → `/mutate`, add inline `-e` CLI flag, ship three-channel deprecation on the legacy URLs. **In flight, PR #110.**
2. **Envelope hardening** (this RFC adds it as a Phase 1 before MR-969): make today's mutation surface agent-grade with idempotency keys, preconditions, deadlines, and a structured response envelope carrying `audit_id`, `commit_id`, `snapshot_id`, and cost stats.
3. **MR-969**: add a stored-query registry, `POST /queries/{name}`, a new `InvokeQuery` Cedar action with per-query scope, inline pragmas in `.gq` (`@description`, `@returns`, `@mcp`), and MCP transport over the same routing primitive.

The bet: inline and stored queries serve different stages of the same lifecycle, run through the same engine code, and are gated by different Cedar actions. HelixDB collapsed to stored-only. Postgres has neither stored-query Cedar nor MCP. The window for an OSS, declarative, agent-grade graph query surface is open.

## Motivation

Three problems today:

- **Mutation responses are too thin.** `ChangeOutput { node_count, edge_count }` is the entire memory the API has of what just happened. No `commit_id`, no `audit_id`, no `snapshot_id`. Agents reporting results have nothing to cite. Humans can't reproduce a read.
- **No agent-safe surface.** Cedar gates `read` and `change` at the action level. A token either runs *any* query or *no* query of that kind. There is no way to express "this agent can invoke `find_user` and nothing else."
- **No discovery primitive.** Agents need a tool list. SDKs need a stable contract per operation. Both are absent.

The MR-656 rename solves the cosmetic asymmetry (`/read` was a poor pair for the future `/queries/{name}`). The envelope work and MR-969 solve the substantive gaps.

## Non-Goals

- Compiled query bundles (HelixDB's `queries.json` shape). `.gq` files are already declarative; the file *is* the artifact.
- Hot reload of the registry. Restart-only matches the multi-graph operational model from MR-668.
- Per-query rate limits in v1. Existing `WorkloadController` covers the bulk of the risk. Punt to a future ticket.
- Cross-graph tool listing in MCP. Agents loop over per-graph endpoints when they need multi-graph access. Avoid namespacing in the contract.
- Web dashboard / control-plane management of the registry. Operators edit `.gq` + `policy.yaml` and restart.
- Schema introspection through MCP. Schema is an operator concern; agents see types through declared return shapes on the queries they're allowed to invoke.
- Per-environment override files. Environment-specific differences live in `policy.yaml`, which already has per-env variants.

## Background

OmniGraph runs on Lance 6.x with a property graph layered on top: typed nodes/edges in per-type Lance datasets, atomic multi-table commits via a `__manifest` table, branchable and time-travelable through Lance versioning. The HTTP server (`omnigraph-server`) is Axum + utoipa with bearer-token auth and Cedar policy enforcement at every `_as` writer.

MR-668 shipped multi-graph mode in v0.6.0. One server process can host 1-10 graphs, with per-graph endpoints under `/graphs/{id}/...`. Cedar policy resolves against `Server::"root"` (for management actions) and `Graph::"prod"` (for per-graph actions).

MR-656 is currently in PR #110 (CONFLICTING / DIRTY against main; rebase planned). It renames the URL surface, adds inline source support, and ships three-channel deprecation (OpenAPI `deprecated: true`, RFC 9745 `Deprecation: true` header, RFC 8288 successor `Link`).

## Design

### Two paths, one engine

| Dimension | Inline (`/query`, `/mutate`) | Stored (`/queries/{name}`) |
|---|---|---|
| Source location | Request body | `queries/*.gq` on disk |
| Parse + typecheck | Per request | Once at server boot |
| Cedar action | `read` / `change` | `invoke_query` (per-name scope) |
| MCP-exposed | No (not enumerable) | Yes (when `@mcp(expose=true)`) |
| Output schema | Inferred | Declared via `@returns`, asserted at boot |
| Audit log shape | Records query hash | Records query name |
| Failure visibility | Runtime 400 | Boot-time refusal |

Both paths converge in the engine:

```
POST /query         ─parse→─┐
POST /mutate        ─parse→─┤
                             ├─→ run_query / run_mutate(ast, params, branch) ─→ envelope
POST /queries/{name} ───────┤
POST /mcp/invoke ───────────┘   (MCP adapter on top of the same call)
```

The MR-656 rebase widens `run_query` / `run_mutate` to accept a parsed AST or source string. Inline parses on each call. Stored looks up the pre-parsed AST in the registry. Same execution path beyond that point.

### Cedar split (the LLM-safe wedge)

Inline and stored coexist safely because they're gated by different actions:

```yaml
# Production policy — agents locked to a curated stored-query set
- deny:
    actors: { group: agents }
    actions: [read, change]            # blocks /query, /mutate, /read, /change

- allow:
    actors: { group: agents }
    actions: [invoke_query]
    resource: Graph::"prod"
    query_scope: { names: [find_user, list_orders, search_docs] }
```

The agent's effective surface: three stored queries by name. Cannot compose inline. Cannot enumerate schema. Cannot read arbitrary entities. A developer in the same deployment with `dev-engineers` group membership might have `[read, change, invoke_query]` allowed — full access to both paths.

Same server, same data, two completely different API surfaces depending on token. This is the posture MR-969 calls "LLM-safe API surface."

### `.gq` pragmas

Stored queries self-describe at the top of the source file:

```gq
@description("Look up a user by ID. Returns name, email, last_login.")
@returns({ name: String, email: String, last_login: DateTime? })
@mcp(expose=true)

query find_user($id: String) {
  match { $u: User { id: $id } }
  return { $u.name, $u.email, $u.last_login }
}
```

Three pragmas in v1:

- `@description("...")` — string surfaced in `omnigraph queries explain` and MCP tool descriptions.
- `@returns({...})` — optional output type assertion. Compiler verifies the inferred type matches; mismatch fails server startup.
- `@mcp(expose=true|false, tool_name="alt_name"?)` — controls MCP visibility. Default is `expose=false` (callable via HTTP, hidden from MCP). `tool_name` defaults to the query name.

Pragmas live in source, not in a separate YAML registry. Drop a file in `queries/`, restart, the registry picks it up. The full agent contract is reviewable in one diff.

### Request envelope ("before")

Today's request carries auth + body. The envelope adds five fields, all optional:

```http
POST /graphs/prod/queries/find_user
Authorization: Bearer <token>
Idempotency-Key: 01HXYZ...              # mutations only
If-Match: 01HABC...                     # optimistic concurrency
X-Deadline: 2026-05-28T19:30:00Z        # or X-Timeout-Ms: 5000
X-Trace-Id: 01HDEF...
Content-Type: application/json

{
  "params":  { "id": "u-42" },
  "branch":  "main",
  "expect":  "read_only",               # scope assertion
  "dry_run": false,                     # mutations only
  "fields":  ["name", "email"]          # result projection
}
```

Field semantics:

| Field | Applies to | Purpose |
|---|---|---|
| `Idempotency-Key` | Mutations | Server caches `(token, key)` → response for 10 minutes. Replays return cached response with `Idempotency-Replay: true` header. Prevents double-write on retry. |
| `If-Match` | Mutations | Run only if branch HEAD matches the given commit ID. 412 Precondition Failed otherwise. Enables read-then-write without races. |
| `X-Deadline` / `X-Timeout-Ms` | All | Server respects; returns 504-typed error past the deadline. Bounds execution for context-budget-constrained callers. |
| `X-Trace-Id` | All | Caller-supplied; server echoes back. Lets agents correlate multi-call sequences. |
| `expect` | All | Caller asserts shape: `"read_only"`, `{"max_rows_scanned": 10000}`. Server validates against parsed AST or planner estimate; rejects before running. |
| `dry_run` | Mutations | Returns what *would* happen without committing. Implemented via scratch branch + diff + discard. |
| `fields` | Reads | Server returns only listed columns. Saves bandwidth + agent context window. |

All five fields are optional; today's call shape continues working.

### Response envelope ("after")

The response envelope replaces today's bare-result shape with a structured wrapper. Every endpoint (inline, stored, MCP) returns the same envelope:

```json
{
  "result": { "name": "Alice", "email": "alice@..." },
  "audit_id": "01HGHI...",
  "snapshot_id": "01HJKL...",
  "commit_id": null,
  "stats": {
    "rows_scanned": 1,
    "ms_elapsed": 4,
    "bytes_read": 128
  },
  "warnings": []
}
```

Response headers:

| Header | When | Purpose |
|---|---|---|
| `Idempotency-Replay: true\|false` | Mutations | Was this response served from the idempotency cache? |
| `X-Trace-Id` | All | Echo of the request's trace ID, or server-minted if absent. |
| `Deprecation: true` | `/read`, `/change` only | RFC 9745 signal from MR-656. |
| `Link: </query>; rel="successor-version"` | `/read`, `/change` only | RFC 8288 successor pointer from MR-656. |

Body envelope fields:

| Field | When | Purpose |
|---|---|---|
| `result` | All | The actual response payload. Shape determined by the query's return type. |
| `audit_id` | All | ULID for the audit log entry. Lets the caller cite exactly what ran. |
| `snapshot_id` | All | Manifest snapshot the query observed. Reproducibility — replay with `?snapshot=<id>`. |
| `commit_id` | Mutations | ULID of the new commit. Null for reads. Lets the caller cite what changed. |
| `stats` | All | `{rows_scanned, ms_elapsed, bytes_read}`. Lets agents learn what's expensive. |
| `warnings` | All | Non-fatal observations: deprecated property access, full-scan despite available index, scan exceeded soft row limit. Empty array when none. |

The envelope is the API's *memory of what happened*. Without `audit_id` + `commit_id` + `snapshot_id`, agent reports are hearsay and reads are not reproducible. With them, provenance is a first-class property of every response.

### MCP integration with multi-graph

MCP routes are per-graph, matching the rest of MR-668's hierarchy:

```
GET  /graphs/{id}/mcp/tools     # tool list for this graph, this token
POST /graphs/{id}/mcp/invoke    # invoke a tool on this graph
```

Single-mode collapses to `/mcp/tools` and `/mcp/invoke` at the root (same shape, no `/graphs/{id}` prefix). Both modes route through identical handler code.

Tool list response:

```json
{
  "tools": [
    {
      "name": "find_user",
      "description": "Look up a user by ID.",
      "inputSchema":  { "id": { "type": "string", "required": true } },
      "outputSchema": { "name": "string", "email": "string", "last_login": "datetime?" },
      "read_only": true
    }
  ],
  "graph_id": "prod",
  "snapshot_id": "01HJKL..."
}
```

The tool list is the subset of registered queries where (a) `@mcp(expose=true)` in source and (b) Cedar permits `invoke_query` for this token on this name on this graph. Computed per request — cheap because it's just iterating the registry + one Cedar evaluation per name.

**Token scoping.** Most tokens carry one graph claim. Cross-graph access requires multiple Cedar rules (one per graph) and is uncommon. Agents that genuinely operate across graphs loop over `/graphs/{id}/mcp/tools` themselves. The contract stays clean; graph renames don't break tool names.

**Discovery.** Agents are told their MCP URL at provisioning: `https://omnigraph.example.com/graphs/prod/mcp`. Token authorizes; URL identifies. Same model as every OAuth-style API.

**`/mcp/invoke` is a protocol adapter.** Unwrap MCP protocol envelope, call the same code path as `/queries/{name}`, wrap the response in MCP shape. No new execution semantics.

### CLI surface

The CLI mirrors the HTTP routes. Post-MR-656 and post-MR-969:

```bash
# Inline (MR-656)
omnigraph query  -e 'query test() { ... }'                    # /query
omnigraph mutate -e 'query bump() { update ... }'             # /mutate

# Stored (MR-969)
omnigraph queries list                                        # GET /queries (future)
omnigraph queries explain find_user                           # show params + return shape + source
omnigraph queries invoke find_user --param id=u-42            # POST /queries/find_user

# Pragma + registry validation
omnigraph lint queries/find_user.gq                           # parses + verifies pragmas
omnigraph queries lint                                        # validates the whole registry
```

`omnigraph queries invoke` reads bearer + URL from `omnigraph.yaml` like the other remote commands. Local invocations work the same way the existing `omnigraph query`/`mutate` do.

### Lifecycle

The promotion path from inline to stored is the load-bearing DX story:

```
1. EXPLORE      omnigraph query -e 'query find_user($id: String) { ... }' --params '{"id": "u-42"}'
                  └─ POST /query, iterate freely

2. STABILIZE    write queries/find_user.gq with @description, @returns, @mcp pragmas
                  └─ git diff shows the full agent contract in one file

3. AUTHORIZE    add Cedar rule allowing invoke_query for the appropriate actor group
                  └─ scope_names: [find_user]

4. DEPLOY       restart server
                  └─ /queries/find_user goes live
                  └─ /mcp/tools auto-lists it for any token with invoke_query[find_user]

5. RETIRE       deny: read change for the agent group
                  └─ inline access closed; stored remains
                  └─ MR-969's "LLM-safe API surface" reached
```

Same `.gq` source through all five steps. No rewrite. No language shift. The pragmas are the only added syntax between exploration and production.

## Migration

Existing callers see no breakage:

- `POST /read` and `POST /change` keep working, now with `Deprecation: true` headers (MR-656).
- `ChangeRequest` field names `query_source` / `query_name` accepted as serde aliases (MR-656).
- `aliases:` block in `omnigraph.yaml` unchanged; both `read`/`change` and `query`/`mutate` accepted as `command:` values (MR-656).
- New envelope fields are additive; old clients ignoring them keep working.
- `Idempotency-Key`, `If-Match`, `X-Deadline` are opt-in headers; absence is the current behavior.

Callers move at their own pace. The envelope upgrades + URL rename ship in v0.6.x (small PRs). Stored queries + MCP ship in v0.7.0.

## Sequencing

**Phase 1: envelope (v0.6.x, before MR-969).** Four small PRs, ~100-200 LOC each.

1. Wrap responses in the structured envelope. Add `audit_id`, `snapshot_id`, `commit_id`, `stats`, `warnings`. Backward-compatible if we keep today's top-level fields and add new ones alongside; cleaner break if we move to nested `result.*`. Pick one and live with it.
2. Honor `Idempotency-Key` on `/mutate` (and the deprecated `/change`). Server-side cache keyed by `(token, key)`.
3. Honor `If-Match` on `/mutate`. Wire through to the publisher CAS layer.
4. Honor `X-Deadline` / `X-Timeout-Ms` on every endpoint. Return 504-typed error past deadline.

**Phase 2: MR-969 PR 1 (registry).** The stored-query registry, `/queries/{name}` route, `InvokeQuery` Cedar action with per-name scope, `.gq` pragma parsing (`@description`, `@returns`, `@mcp`), read-vs-mutate classification at registry load. Inline keeps working unchanged.

**Phase 3: MR-969 PR 2 (MCP).** `/graphs/{id}/mcp/tools` and `/graphs/{id}/mcp/invoke`. Tool schemas projected from declared return types and parameter declarations. Single-graph-scoped tokens.

**Phase 4: MR-969 PR 3 (Cedar deny-on-ad-hoc sugar).** Small Cedar-language addition so operators can lock down `/read` / `/query` while keeping `/queries/*` open. Independent of PRs 1-2.

**Phase 5: deferred.**
- Cross-graph MCP namespacing (wait for usage signal).
- Per-query rate limits (extend `WorkloadController`).
- Schema introspection as a separate Cedar action (3-line PR).
- CLI verb consolidation (`omnigraph call <name>`).
- Cache warming (HelixDB-style; not load-bearing).

## Rejected Alternatives

**Per-environment override files (`_overrides.yaml`).** Initial design had a sparse YAML file for per-env tweaks: MCP exposure, row caps, kill-switch, param locks. Rejected because every override candidate either belongs in source (`@mcp` flag), Cedar policy (per-actor visibility, per-env), or `omnigraph.yaml` (operator config). Splitting query metadata across files makes it harder to review what an agent can see. Keep source authoritative; let Cedar express the per-env differences.

**Compiled query bundle (HelixDB's `queries.json`).** HelixDB compiles their Rust-DSL queries to JSON. Rejected because `.gq` files are already declarative. The file is the artifact. Reviewers diff source, not bytecode.

**Stored-queries-only (HelixDB's posture).** Rejected because the personal-graph / dev-iteration use case dies without inline. Inline `-e` is the REPL for human exploration; stored is the contract for production agents. Both first-class.

**Cross-graph tool-name prefixing (`prod.find_user`).** Rejected because graph renames would break agent contracts. Per-graph URLs let graph identity live in the URL, not in tool names.

**Body-field graph dispatch (`{tool, graph, params}`).** Rejected because it doubles the contract surface (every tool is identified by two fields). Per-graph URLs are simpler.

**Pragmas in YAML instead of source.** Rejected because two-file definitions (source + metadata YAML) make diffs harder to review and create drift opportunities. Source is the source of truth.

**Pragmas as in-source comments (`#[mcp]` HelixDB-style).** Considered; chose `@mcp(...)` because comment-flavored pragmas conflate documentation and machine-readable metadata. The `@` prefix makes the pragma's role explicit.

## Open Questions

1. **Envelope breakage vs additive.** Phase 1.1 wraps responses in a structured envelope. Do we keep today's top-level fields *and* add new ones (additive, ugly), or move result to `result.*` (clean break, requires SDK updates)? Lean toward additive — let the new envelope coexist with the old shape until v0.7.0, then collapse.

2. **`@returns` strictness.** Should mismatched declared-vs-inferred return type be a boot-time error or a warning? Lean toward error — silent drift defeats the assertion's purpose. Operators who want flexibility omit `@returns`.

3. **MCP protocol transport.** Streamable HTTP (the new MCP standard) vs stdio (Anthropic's original). Both have Rust crates. Lean toward streamable HTTP since we're already an HTTP server.

4. **Stored mutation routing.** A `.gq` file that contains both reads and writes — does the registry reject it at load (parse-time D2 rule from MR-656), or accept and classify as "mixed"? Lean toward reject. Mixed queries are a footgun; force operators to split.

5. **`expect` field strictness.** `expect: "read_only"` against a parsed mutating query is an obvious 400. But `expect: {max_rows_scanned: 10000}` requires planner estimates that don't exist today. Either ship `expect` with only the "read_only" assertion in v1 and grow it, or wait for the planner. Lean toward shipping the partial form.

6. **CLI `queries invoke` shape.** Today's `omnigraph query` takes a file or alias. `omnigraph queries invoke find_user` takes a stored query name. Should `omnigraph query --name find_user` also work (auto-detect)? Cleaner to keep them separate verbs — the stored vs inline distinction is part of the contract.

## References

- MR-656: [Support inline query strings in CLI and HTTP server](https://linear.app/modernrelay/issue/MR-656)
- MR-668: [Multi-graph server mode](https://linear.app/modernrelay/issue/MR-668) (shipped, PR #119)
- MR-969: [Stored queries with MCP exposure and per-query Cedar authorization](https://linear.app/modernrelay/issue/MR-969)
- PR #110: [feat: inline query strings in CLI and HTTP server](https://github.com/ModernRelay/omnigraph/pull/110)
- HelixDB docs: [docs.helix-db.com/llms-full.txt](https://docs.helix-db.com/llms-full.txt) — `#[mcp]` macro, scoped API keys, stored query model
- RFC 9745 (`Deprecation` header)
- RFC 8288 (`Link` relations, `successor-version`)
- MCP spec: [modelcontextprotocol.io](https://modelcontextprotocol.io)
- [invariants.md](./invariants.md) — substrate boundaries this work respects
- [../user/server.md](../user/server.md) — current HTTP surface (post-MR-656 picks up the `/query`+`/mutate` rename and deprecation)
