# Multi-Graph Omnigraph Server — Design & Implementation Plan

Status: proposed
Author: @ragnorc
Last updated: 2026-04-17

## Problem

The `omnigraph-server` process serves exactly one graph. `AppState` holds a single `Arc<RwLock<Omnigraph>>`, opened from one URI at startup. ModernRelay manages hundreds of repositories per deployment (one per user/team). Running a separate server process per graph is operationally impractical.

## Outcome

One `omnigraph-server` process can route requests to many graphs backed by shared storage. Single-graph deployments continue to work unchanged.

---

## Requirements

| ID | Requirement |
|----|-------------|
| R0 | One server process routes requests to N graphs backed by shared storage |
| R1 | Existing single-graph deployments work unchanged (opt-in multi) |
| R2 | Writes to different graphs proceed concurrently |
| R3 | At most one writer per underlying graph at any moment (lock invariant) |
| R4 | Cold graph is opened on first request; no startup preload |
| R5 | Bound memory when graph count exceeds process capacity |
| R6 | Concurrent requests to a cold graph do not double-open or double-init |
| R7 | Management API: create, list, info, delete graphs |
| R8 | `graph_id` cannot escape the storage root (no path traversal) |
| R9 | Service-to-service forwarding of per-user actor identity, gated by token capability |

---

## Design

### Registry: permanent slot, evictable payload

A `GraphRegistry` holds a `DashMap<GraphId, Arc<Slot>>`. Slot lifetime is permanent once created — the `RwLock` inside the slot is the graph's single point of synchronization and must never be replaced.

```rust
pub struct GraphRegistry {
    base_uri: String,
    slots: DashMap<GraphId, Arc<Slot>>,
    max_resident: usize,
    eviction_interval: Duration,
    server_policy: Option<Arc<PolicyEngine>>, // governs management actions
}

pub struct Slot {
    id: GraphId,
    uri: String,
    lock: tokio::sync::RwLock<SlotState>,
    last_accessed: AtomicU64,
}

pub enum SlotState {
    Empty,
    Open(Omnigraph, PolicyEngine),
    Tombstoned,
}
```

**Why this shape:** the `RwLock` lives on the `Slot`, not on the `Omnigraph`. Eviction drops the `Omnigraph` (and its `PolicyEngine`) but preserves the lock identity. Concurrent requests that race on a cold graph queue on the slot's write-lock; the second waiter observes `Open` and skips `Omnigraph::open()`. This is how R3 and R6 are satisfied together without an extra coordination primitive.

### Access pattern

```rust
impl GraphRegistry {
    pub async fn with_graph<F, R>(&self, id: &GraphId, f: F) -> Result<R>
    where
        F: FnOnce(&Omnigraph, &PolicyEngine) -> R,
    {
        let slot = self.get_or_insert_slot(id);
        let guard = slot.lock.read().await;
        match &*guard {
            SlotState::Open(graph, policy) => {
                slot.touch();
                Ok(f(graph, policy))
            }
            SlotState::Tombstoned => Err(not_found(id)),
            SlotState::Empty => {
                drop(guard);
                self.open_and_run(&slot, f).await
            }
        }
    }
}
```

Writers acquire `lock.write()`; the `with_graph_mut` variant mirrors the read path.

### Eviction

A background task sweeps slots ranked by `last_accessed` at `eviction_interval`. For each slot over the `max_resident` threshold, it acquires `slot.lock.write().await` (waits for in-flight writers to finish), transitions `Open → Empty`, and drops the `Omnigraph` + `PolicyEngine`. Slot remains in the map; lock identity preserved.

### `GraphId` — type-system guarantee for R8

```rust
pub struct GraphId(String);

impl TryFrom<String> for GraphId {
    type Error = InvalidGraphId;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        if GRAPH_ID_RE.is_match(&s) { Ok(Self(s)) } else { Err(InvalidGraphId) }
    }
}
```

`GRAPH_ID_RE = ^[a-zA-Z0-9_-]{1,64}$`. The Axum `Path<GraphId>` extractor fails with 400 before `resolve_graph` runs. Management endpoints accept `GraphId` in the body via the same conversion. Path traversal is unrepresentable.

### Token capabilities — R9

```rust
pub struct TokenRecord {
    pub actor: Arc<str>,
    pub caps: TokenCaps,
}

pub struct TokenCaps {
    pub may_forward_actor: bool,
}
```

`require_bearer_auth` sets `AuthenticatedActor(token.actor)` in extensions. If `X-Actor-Id` is present:
- `token.caps.may_forward_actor == true` → replace extension with forwarded actor (string validated for format/length)
- otherwise → 403

The `PolicyEngine` sees the same `AuthenticatedActor` type it sees today. No policy schema change.

### Policy model

Decisions on the four tensions raised during shaping:

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Policy ownership | Server-owned | Avoids self-gated policy edits; matches "ops controls graph lifecycle, owner controls data" |
| Actor enumeration | Drop `known_actors`; model ownership as `Graph.owner` attribute | Scales to forwarded actor IDs; templated policy covers the 90% case |
| Engine lifetime | Resident inside `SlotState::Open` (evicted with payload) | Memory scales with active graphs, not total |
| Reload | Explicit: server write path updates both storage and in-memory engine | Single-server deployments only for now; TTL-based invalidation deferred |

**Cedar schema changes:**

- Rename entity type `Omnigraph::Repo` → `Omnigraph::Graph` (type cleanup — "Repo" appears only in `policy.rs`, no external surface).
- Add `owner: Actor` attribute on `Graph`.
- Add `Omnigraph::Server` entity type (singleton `Server::"root"`) as the resource for management actions `GraphCreate`, `GraphDelete`, `GraphList`.
- Rename internal `repo_id` → `graph_id` throughout `policy.rs`.

**Graph creation** (`POST /graphs`):

```json
{
  "graph_id": "repo-01j5abc",
  "schema_source": "type Person { id: String @id }",
  "owner_actor": "act-andrew",
  "policy_source": null
}
```

- If `policy_source` is provided, use it verbatim.
- Otherwise, generate from the owner-only template: `owner_actor` becomes `Graph.owner`; the template permits all graph-scoped actions when `principal == resource.owner`.
- Server-level engine authorizes the request against `Server::"root"` with `action == GraphCreate` and `context.owner_actor` for fine-grained rules.

**Policy storage:** `{base_uri}/policies/{graph_id}.yaml`, not `{graph_uri}/policy.yaml`. Server-owned, outside the graph's own storage prefix.

**Trust boundary:** Omnigraph is the authorization layer. Authenticity of forwarded actors is delegated to the token holder; `may_forward_actor: true` tokens are trusted to have authenticated the user upstream.

### Routing

```rust
pub fn build_app(state: AppState) -> Router {
    match state.mode() {
        ServerMode::Single => build_single_graph_app(state), // today's router
        ServerMode::Multi  => build_multi_graph_app(state),
    }
}
```

Multi-mode:

```rust
Router::new()
    .route("/healthz", get(server_health))
    .route("/openapi.json", get(server_openapi))
    .nest("/graphs/{graph_id}", graph_routes)   // all existing handlers, prefixed
    .merge(management_routes)                    // POST/GET/DELETE /graphs, GET /graphs/{id}
    .layer(TraceLayer::new_for_http())
    .with_state(state)
```

The `resolve_graph` middleware extracts `Path<GraphId>`, calls `registry.get_or_insert_slot`, stashes `Arc<Slot>` in request extensions. Handlers change from `State<AppState>` to `Extension<Arc<Slot>>` and replace `state.db.read_owned()` with `slot.lock.read().await`. The change is mechanical across ~15 handlers.

### Configuration

```yaml
server:
  mode: multi                            # "single" (default) | "multi"
  base_uri: s3://modernrelay-omnigraph
  bind: 0.0.0.0:8080
  max_resident: 256
  eviction_interval: 60s

policy:
  server_policy_file: ./server-policy.yaml
```

Single mode is the default; existing deployments need no change.

### Management endpoints

| Method | Path | Action | Resource |
|--------|------|--------|----------|
| POST | `/graphs` | `GraphCreate` | `Server::"root"` (context: `owner_actor`, `graph_id`) |
| GET | `/graphs` | `GraphList` | `Server::"root"` |
| GET | `/graphs/{id}` | `GraphInfo` | `Graph::"{id}"` |
| DELETE | `/graphs/{id}` | `GraphDelete` | `Graph::"{id}"` |

`DELETE` acquires the slot's write-lock (drains in-flight writers), transitions to `Tombstoned`, deletes policy + storage, then removes the slot from the map. Concurrent requests that subsequently acquire the lock observe `Tombstoned` and return 404.

### OpenAPI

Two specs, selected by mode:
- Single mode — unchanged, 16 paths.
- Multi mode — all graph-scoped paths nested under `/graphs/{graph_id}/`, plus four management paths.

`tests/openapi.rs` grows a `#[mode("multi")]` fixture asserting the prefixed path set and the new management schemas.

---

## What does NOT change

- `Omnigraph` struct, `Omnigraph::open()`, `Omnigraph::init()` — untouched.
- Query execution, mutation execution, branch management — untouched.
- Cedar's graph-level rules (read, write, schema_apply, branch ops, runs, commits) — untouched except for the `Repo → Graph` entity rename.
- Schema compiler, storage layer (Lance, S3) — untouched.

The entire change is confined to the server routing, registry, and policy bootstrap layers.

---

## Implementation slices

Each slice ends in a demo-able state. Target ≤9.

### V1 — Single-graph refactor on the Slot primitive

Introduce `Slot` and `SlotState` inside `AppState` for the single-graph case, without changing routes. Prove the read/write/eviction paths against the existing test suite.

- `crates/omnigraph-server/src/registry.rs` (new): `Slot`, `SlotState`, `GraphId`.
- `AppState` owns `Arc<Slot>` instead of `Arc<RwLock<Omnigraph>>`.
- Handlers call `slot.lock.{read,write}().await` on `SlotState::Open`.
- No behavior change; existing tests pass unchanged.

Demo: `cargo test -p omnigraph-server` green on single-mode routes with the new primitive.

### V2 — Registry and multi-mode routing

Add `GraphRegistry`, `ServerMode::Multi`, and `build_multi_graph_app`. Wire `resolve_graph` middleware. All graph-scoped handlers work under `/graphs/{graph_id}/`.

- `GraphRegistry::get_or_insert_slot`, `with_graph`, `with_graph_mut`.
- `resolve_graph` middleware inserts `Arc<Slot>` as extension.
- Handlers switch from `State<AppState>` to `Extension<Arc<Slot>>`.

Demo: start server in multi mode, hit `/graphs/foo/read` with a pre-created graph at `s3://.../foo/`, get a valid response.

### V3 — `GraphId` validation and token capabilities

- `GraphId` newtype with regex validation at the Axum extractor.
- `TokenRecord { actor, caps }` replaces the raw token map.
- `X-Actor-Id` forwarding gated by `may_forward_actor`.
- `tests/server.rs` gains path-traversal and forwarding auth tests.

Demo: `curl /graphs/..%2Fother/read` → 400; `curl -H 'X-Actor-Id: someone' /graphs/foo/read` with a non-forwarding token → 403.

### V4 — Cedar rename and `Server` entity type

- `Omnigraph::Repo` → `Omnigraph::Graph` throughout `policy.rs`.
- `repo_id` → `graph_id` rename.
- Add `Omnigraph::Server` singleton; `GraphCreate`/`GraphDelete`/`GraphList` actions.
- `PolicyEngine::authorize_server_action` entry point for management.

Demo: server-level policy file with rule "actor X may create graphs" is enforced by `POST /graphs`.

### V5 — Management endpoints

- `POST /graphs` with `schema_source`, `owner_actor`, optional `policy_source`.
- `GET /graphs` (scan `base_uri` for manifests, report open/closed).
- `GET /graphs/{id}`.
- `DELETE /graphs/{id}` with drain semantics.
- Policy template generator for owner-only default.

Demo: create graph via HTTP, list, read, delete, observe 404 on subsequent access.

### V6 — LRU eviction

- Background sweeper task.
- `max_resident` enforcement via `slot.lock.write().await` on LRU slots.
- Metrics: `omnigraph_resident_graphs`, `omnigraph_evictions_total`, `omnigraph_graph_opens_total`.

Demo: set `max_resident: 2`, touch three graphs, observe the LRU one transitioned to `Empty`; subsequent access reopens.

### V7 — OpenAPI multi-mode

- Dynamic `ApiDoc` generation based on `ServerMode`.
- `tests/openapi.rs` multi-mode fixture.
- Update docs (`docs/deployment.md`, possibly `docs/cli.md`) with multi-mode setup.

Demo: `GET /openapi.json` on a multi-mode server returns the prefixed path set with management endpoints; spec validates against utoipa's OpenAPI 3.1 schema.

---

## Test strategy

- **V1** is covered by the existing single-mode suite — the Slot primitive is invisible to callers.
- **V2–V5** add a new `tests/multi_graph.rs` with fixtures that create N graphs, exercise per-graph auth, concurrent writes across graphs, and management flows.
- **V3** adds path-traversal unit tests against `GraphId::try_from` and forwarding auth tests in the integration suite.
- **V6** adds an eviction test that sets `max_resident: 1`, opens two graphs sequentially, and asserts the first was evicted.
- **V7** extends `tests/openapi.rs` with a multi-mode branch that asserts the full prefixed path set plus the four management paths.

All existing tests must pass with `mode: single` as the default.

---

## Out of scope

- Multi-server coordination (policy TTL / pub-sub invalidation) — deferred; single-server deployments only.
- Per-graph bearer token scoping (today all tokens are global service tokens).
- Migration tooling for existing single-mode deployments to pick `graph_id`s.
- Process-per-graph reverse-proxy alternative (called "operationally impractical" in the problem statement).

---

## Open questions

1. **Resident cost per graph.** Worth a spike to measure actual memory of an opened `Omnigraph` against a ModernRelay-sized graph before finalizing `max_resident`.
2. **DELETE drain timeout.** How long should `slot.lock.write()` wait before returning 409? Propose 30s default, configurable.
3. **Owner-only template content.** First draft of the generated policy YAML should be reviewed before V5 lands.

---

## Scope estimate

| Component | LOC |
|-----------|-----|
| `registry.rs` (Slot, GraphRegistry, GraphId) | ~300 |
| `lib.rs` routing (two builders, middleware) | ~150 |
| `lib.rs` handlers (mechanical `State → Extension` refactor across 17 handlers) | ~120 |
| `api.rs` (management request/response types) | ~60 |
| `config.rs` (mode, base_uri, max_resident, eviction_interval) | ~40 |
| `policy.rs` (Repo→Graph rename, Server entity, GraphCreate/Delete/List actions, owner attribute, template generator) | ~180 |
| `auth.rs` (TokenRecord, capability check, X-Actor-Id handling) | ~80 |
| Tests (`multi_graph.rs`, traversal, forwarding, eviction, openapi multi) | ~700 |
| **Total** | **~1,630** |

The proposal's original estimate (~615) under-counts tests and the policy work. The per-handler refactor is mechanical but the test parity with single-mode is substantial.
