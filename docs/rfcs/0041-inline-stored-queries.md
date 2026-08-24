---
rfc: "0041"
title: "Inline and stored queries"
track: maintainer
status: accepted
implementation: partial
authors:
  - OmniGraph maintainers
created: 2026-05-28
updated: 2026-08-23
discussion: null
supersedes: []
superseded_by: []
blocked_on: []
---

# RFC 0041: Inline and stored queries
**Tickets:** MR-656 (inline `-e` + URL rename), MR-668 (multi-graph, shipped),
MR-976 (historical envelope proposal), and MR-969 (stored queries; MCP follow-up
is now RFC 0003)
**Historical target:** v0.6.x patch series (MR-656) through the stored-query
work later shipped in the v0.10 line.

> **Current boundary:** Inline `/query` and `/mutate`, boot-validated stored
> queries, `GET /queries`, `POST /queries/{name}`, and the coarse
> `invoke_query` Cedar gate ship. The richer request/response envelope below
> remains design history, not a current contract. MCP transport is owned by the
> still-draft [RFC 0003](0003-mcp-server-surface.md) and is intentionally absent
> from this RFC.

## Summary

OmniGraph today exposes `POST /read` and `POST /change` with a weakly-contracted body (counts only on writes) and no per-query authorization. This RFC consolidates the work landing across three Linear tickets into one coherent design:

1. **MR-656**: rename `/read` → `/query` and `/change` → `/mutate`, add inline `-e` CLI flag, ship three-channel deprecation on the legacy URLs. **In flight, PR #110.**
2. **Envelope hardening** (this RFC adds it as a Phase 1 before MR-969): make today's mutation surface agent-grade with idempotency keys, preconditions, deadlines, and a structured response envelope carrying `audit_id`, `commit_id`, `snapshot_id`, and cost stats.
3. **MR-969**: add a stored-query registry, `GET /queries`,
   `POST /queries/{name}`, and an `InvokeQuery` Cedar action. The shipped gate
   is graph-scoped; the invoked query still passes the ordinary `read` or
   `change` gate for its body.

The bet: inline and stored queries serve different stages of the same lifecycle,
run through the same engine code, and are gated by different Cedar actions.
Inline remains the exploration surface; stored queries are the reviewed,
boot-validated service surface.

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
- Web dashboard / control-plane management of the registry. Operators edit `.gq` + `policy.yaml` and restart.
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
| Catalogued | No (not enumerable) | Yes when the registry entry has `expose: true` |
| Output schema | Inferred | Inferred and type-checked at boot |
| Audit log shape | Records query hash | Records query name |
| Failure visibility | Runtime 400 | Boot-time refusal |

Both paths converge in the engine:

```
POST /query         ─parse→─┐
POST /mutate        ─parse→─┤
                             ├─→ run_query / run_mutate(ast, params, branch) ─→ envelope
POST /queries/{name} ───────┘
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
```

The shipped `invoke_query` action grants access to the graph's stored-query
surface as a whole. It does not yet scope the grant by query name. A stored
read also passes the ordinary `read` gate; a stored mutation also passes the
ordinary `change` gate. Callers without `invoke_query` receive the same 404 for
a denied query and an unknown query, preventing registry-name probing.

Same server, same data, two completely different API surfaces depending on token. This is the posture MR-969 calls "LLM-safe API surface."

### Query metadata and registry exposure

Stored queries carry human-facing metadata in `.gq`:

```gq
query find_user($id: String)
  @description("Look up a user by ID.")
  @instruction("Use for exact ID lookups.") {
  match { $u: User { id: $id } }
  return { $u.name, $u.email, $u.last_login }
}
```

- `@description("...")` is concise catalog documentation.
- `@instruction("...")` tells a caller when to use the query.
- `expose` and the optional `tool_name` live on the applied cluster catalog
  entry. They control `GET /queries` membership and naming, not authorization.

The server reads the applied cluster catalog, asserts that each registry key
matches a query declaration, parses and type-checks every entry against the
live graph schema, and quarantines a graph whose registry is invalid.

### Historical request-envelope proposal (not shipped)

The remainder of this section records a rejected/deferred expansion. These
headers and fields are not part of the current query contract.

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

### Historical response-envelope proposal (not shipped)

The proposal would have replaced the bare result shape with one wrapper for
inline and stored endpoints:

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

### CLI surface

The CLI mirrors the HTTP routes. Post-MR-656 and post-MR-969:

```bash
# Inline (MR-656)
omnigraph query  -e 'query test() { ... }'                    # /query
omnigraph mutate -e 'query bump() { update ... }'             # /mutate

# Stored (served graph; omit --query/-e)
omnigraph query find_user --server prod --graph knowledge --params '{"id":"u-42"}'
omnigraph mutate update_user --server prod --graph knowledge --params '{"id":"u-42"}'

# Registry validation
omnigraph lint --query queries/find_user.gq
omnigraph queries validate --cluster ./company-brain --graph knowledge
```

Stored invocations resolve the server, credentials, and graph through the
operator config/profile model from [RFC 0011](0011-cli-addressing-and-config.md).
Ad-hoc file and inline invocations can also run directly against a store.

### Lifecycle

The promotion path from inline to stored is the load-bearing DX story:

```
1. EXPLORE      omnigraph query -e 'query find_user($id: String) { ... }' --params '{"id": "u-42"}'
                  └─ POST /query, iterate freely

2. STABILIZE    write queries/find_user.gq with @description/@instruction metadata
                  └─ git diff shows the full agent contract in one file

3. AUTHORIZE    add Cedar rule allowing invoke_query for the appropriate actor group
                  └─ graph-scoped invoke permission; read/change remains the inner gate

4. DEPLOY       restart server
                  └─ /queries/find_user goes live
                  └─ GET /queries lists exposed catalog entries

5. RETIRE       deny: read change for the agent group
                  └─ inline access closed; stored remains
                  └─ MR-969's "LLM-safe API surface" reached
```

Same `.gq` source through all five steps. No rewrite. No language shift. The pragmas are the only added syntax between exploration and production.

## Migration

Existing callers see no breakage:

- `POST /read` and `POST /change` keep working, now with `Deprecation: true` headers (MR-656).
- `ChangeRequest` field names `query_source` / `query_name` accepted as serde aliases (MR-656).
- New envelope fields are additive; old clients ignoring them keep working.
- `Idempotency-Key`, `If-Match`, `X-Deadline` are opt-in headers; absence is the current behavior.

The URL rename and stored-query registry shipped. The historical envelope
expansion did not; clients must use the current typed `ReadOutput` and
`ChangeOutput` wire shapes.

## Sequencing

**Phase 1: envelope (v0.6.x, before MR-969).** Four small PRs, ~100-200 LOC each.

1. Wrap responses in the structured envelope. Add `audit_id`, `snapshot_id`, `commit_id`, `stats`, `warnings`. Backward-compatible if we keep today's top-level fields and add new ones alongside; cleaner break if we move to nested `result.*`. Pick one and live with it.
2. Honor `Idempotency-Key` on `/mutate` (and the deprecated `/change`). Server-side cache keyed by `(token, key)`.
3. Honor `If-Match` on `/mutate`. Wire through to the publisher CAS layer.
4. Honor `X-Deadline` / `X-Timeout-Ms` on every endpoint. Return 504-typed error past deadline.

**Stored-query outcome:** The registry, `GET /queries`,
`POST /queries/{name}`, graph-scoped `InvokeQuery` Cedar action,
`@description`/`@instruction` metadata, and read-vs-mutate classification are
shipped. Per-query-name authorization is not.

**MCP follow-up:** extracted to [RFC 0003](0003-mcp-server-surface.md). No MCP
transport route is accepted by this RFC.

**Phase 4: MR-969 PR 3 (Cedar deny-on-ad-hoc sugar).** Small Cedar-language addition so operators can lock down `/read` / `/query` while keeping `/queries/*` open. Independent of PRs 1-2.

**Phase 5: deferred.**
- Per-query rate limits (extend `WorkloadController`).
- Schema introspection as a separate Cedar action (3-line PR).
- CLI verb consolidation (`omnigraph call <name>`).
- Cache warming (HelixDB-style; not load-bearing).

## Rejected Alternatives

**Per-environment override files (`_overrides.yaml`).** Rejected. Query
documentation belongs in `.gq`; catalog visibility and naming belong in the
applied cluster declaration; authorization belongs in Cedar policy. A third
override file would create drift.

**Compiled query bundle (HelixDB's `queries.json`).** HelixDB compiles their Rust-DSL queries to JSON. Rejected because `.gq` files are already declarative. The file is the artifact. Reviewers diff source, not bytecode.

**Stored-queries-only (HelixDB's posture).** Rejected because the personal-graph / dev-iteration use case dies without inline. Inline `-e` is the REPL for human exploration; stored is the contract for production agents. Both first-class.

**Pragmas in YAML instead of source.** Rejected because two-file definitions (source + metadata YAML) make diffs harder to review and create drift opportunities. Source is the source of truth.

## Open Questions

1. **Envelope breakage vs additive.** Phase 1.1 wraps responses in a structured envelope. Do we keep today's top-level fields *and* add new ones (additive, ugly), or move result to `result.*` (clean break, requires SDK updates)? Lean toward additive — let the new envelope coexist with the old shape until v0.7.0, then collapse.

2. **Stored mutation routing.** A `.gq` file that contains both reads and writes — does the registry reject it at load (parse-time D2 rule from MR-656), or accept and classify as "mixed"? Lean toward reject. Mixed queries are a footgun; force operators to split.

3. **`expect` field strictness.** `expect: "read_only"` against a parsed mutating query is an obvious 400. But `expect: {max_rows_scanned: 10000}` requires planner estimates that don't exist today. Either ship `expect` with only the "read_only" assertion in v1 and grow it, or wait for the planner. Lean toward shipping the partial form.

4. **CLI invocation shape.** The shipped CLI selects a stored query by name
through `omnigraph query <name>` or `omnigraph mutate <name>` and selects
inline source with `--query`/`-e`; it does not add a parallel `queries invoke`
verb.

## References

- MR-656: [Support inline query strings in CLI and HTTP server](https://linear.app/modernrelay/issue/MR-656)
- MR-668: [Multi-graph server mode](https://linear.app/modernrelay/issue/MR-668) (shipped, PR #119)
- MR-969: [Stored queries with MCP exposure and per-query Cedar authorization](https://linear.app/modernrelay/issue/MR-969)
- PR #110: [feat: inline query strings in CLI and HTTP server](https://github.com/ModernRelay/omnigraph/pull/110)
- RFC 9745 (`Deprecation` header)
- RFC 8288 (`Link` relations, `successor-version`)
- [invariants.md](../dev/invariants.md) — substrate boundaries this work respects
- [../user/server.md](../user/operations/server.md) — current HTTP surface (post-MR-656 picks up the `/query`+`/mutate` rename and deprecation)
