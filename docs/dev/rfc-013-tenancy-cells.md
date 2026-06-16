# RFC-013: Tenancy model — cluster-as-tenant cells, pooled compute

**Status:** Proposed — general architecture (server topology, identity, deployment).
**Date:** 2026-06-16
**Audience:** server / cluster / platform maintainers.
**Builds on:** [rfc-005-server-cluster-boot.md](rfc-005-server-cluster-boot.md)
(cluster-only boot, applied-revision serving), [rfc-011-cli-refactoring.md](rfc-011-cli-refactoring.md)
(the store/cluster/server ontology), [rfc-004-cluster-graph-schema-apply.md](rfc-004-cluster-graph-schema-apply.md)
(ledger/recovery/approvals — what makes a cell self-contained), [rfc-007-operator-config.md](rfc-007-operator-config.md)
(keyed credentials / secret resolution).
**Consumed by:** [rfc-003-mcp-server-surface.md](rfc-003-mcp-server-surface.md) (the MCP
surface is one tenant-scoped consumer of this model, not its driver — see §6).
**Target release:** v0.9.x (cell refactor) → cloud GA (pooled tier + WorkOS/OIDC).

---

## 1. Summary

This RFC fixes the **tenancy model** for OmniGraph as a server/platform concern —
independent of any one surface (HTTP data plane, MCP, CLI). It resolves an
ambiguity that currently sits half-built in the code: the server is **cluster-only**
(one cluster per process — [rfc-005](rfc-005-server-cluster-boot.md)), yet the
identity layer carries **pooled multi-tenant scaffolding** (`GraphKey.tenant_id`,
`ResolvedActor.tenant_id`, "Cloud will set `Some(...)` from the OAuth `org_id`
claim"). Those two point at *different* tenancy architectures. We pick one:

> **The cluster is the tenant is the cell.** A cell is the unit of data isolation —
> its own storage root, catalog, Cedar policy bundle, and token source. Isolation is
> **structural** (by deployment), never a row-level `tenant_id` filter on shared
> data. **Density comes from one server process hosting many cells**, not from
> pooling tenants into one graph.

The one structural change: lift today's per-cluster server runtime into a
**`CellRuntime`** value and let the server hold a map of them, resolved per request
by host (or path) **before** authentication. The entire per-graph stack beneath —
handlers, Cedar enforcement, stored queries, the RFC-003 MCP backend — is unchanged;
it gains one outer dimension, not a rewrite. Identity maps **WorkOS Organization →
cell, 1:1**, with a per-cell OAuth audience, so a token for one tenant cannot be
verified against another's endpoint. The result is best-practice **tiered isolation**
(silo the data, pool the compute) on one binary: dedicated/on-prem (1 process : 1
cell) and pooled cloud (1 process : N cells) are the *same code*, different topology.

## 2. Goals

- **Decide the tenancy model** and make the code stop implying two.
- **Isolation by construction:** a tenant cannot reach or enumerate another tenant's
  data even if a Cedar policy is missing or a handler has a bug.
- **Density without row-level pooling:** amortize compute across tenants while keeping
  each tenant's storage, catalog, policy, and tokens fully separate.
- **One binary, tiered topology:** dedicated, pooled, and on-prem are deployment
  choices, not forks. No cloud-only correctness.
- **Additive to the substrate:** no change to the manifest/commit/Cedar invariants;
  the data plane and MCP surface ride on top unchanged.

## 3. Non-Goals

- **Row-level (pooled-into-one-graph) multi-tenancy.** OmniGraph has no engine-level
  row security; pooling tenants into a shared graph would make isolation depend on a
  per-query filter — the highest-risk pattern, explicitly rejected (§4).
- **A new in-process `tenant_id` authorization dimension.** Cedar stays per-graph /
  per-cell; the cell boundary does the tenant isolation (§5.5).
- **Hosting an OAuth Authorization Server.** Each cell is a Resource Server; the AS is
  WorkOS or the customer's IdP (§5.4), same posture as [rfc-003](rfc-003-mcp-server-surface.md) §3.
- **Cross-cell queries / cross-tenant joins.** A cell is a hard boundary by design.

## 4. The tenancy decision (and why)

The industry framing is **silo / pool / bridge**, refined to **tiered + cell-based**:

| Model | Isolation | Density | Fit |
|---|---|---|---|
| **Silo** — infra per tenant | strongest (structural) | worst | few, large, regulated |
| **Pool** — shared infra, logical `tenant_id` | weakest (code-dependent) | best | many small — *only with engine RLS* |
| **Bridge** — silo some layers, pool others | tunable | tunable | most real products |

**Why not pool the data tier.** A pooled data store puts isolation in one place:
every read/write must carry the tenant filter; one miss is a cross-tenant breach, and
leaked customer data cannot be rotated. Postgres has Row-Level Security precisely to
move that check into the engine. **OmniGraph has no RLS equivalent and no row-level
tenant filtering** — so pooling tenants into a shared graph would adopt the most
dangerous isolation pattern without the substrate support that makes it safe.
Disqualifying for a data substrate.

**Therefore: silo the data, pool the compute (the bridge model, biased to
storage-silo because it is a database).** The isolation unit is the **cluster**, which
already gives separate storage root + catalog + policy bundle + token source. A
cluster is also a natural **cell** (a bounded blast-radius unit, the cell-based
architecture pattern): a fault or breach is contained to one tenant, never the fleet.
Density is then a *compute* concern — one process serving many cells — not a data
concern. This is the model the code is already ~80% built for; the gap is
compute-density routing, not isolation.

## 5. Design

### 5.1 The Cell abstraction

A **cell** is exactly today's whole single-cluster server runtime, lifted into a
value. The fields that are per-cluster today (registry, token table, server policy,
boot source) move off `AppState` onto a `CellRuntime`:

```rust
// new — a cell == one cluster's runtime == one tenant
pub struct CellRuntime {
    pub cell_id:       CellId,                       // == cluster id == tenant id (audit/log key, NOT an isolation check)
    pub registry:      Arc<GraphRegistry>,           // this cell's graphs (GraphHandle{ engine, policy, queries })
    pub auth:          CellAuth,                      // per-cell token source — §5.4
    pub server_policy: Option<Arc<PolicyEngine>>,    // this cell's server-scoped Cedar (GraphList, …)
    pub config_path:   PathBuf,                       // this cell's cluster boot source (applied revision)
}
```

Everything inside a cell — `GraphHandle`, per-graph Cedar bundles, stored-query
registries, `GraphKey { tenant_id, graph_id }` — is **unchanged**. Graph ids are
unique *within a cell*, which is all that is needed; there is no global graph
namespace and no route↔key reconciliation problem because the cell is resolved first.

### 5.2 Server: one process, many cells

```rust
pub struct AppState {
    pub cells:    Arc<CellRegistry>,     // host/prefix -> Arc<CellRuntime>   (the ONLY new top-level field)
    pub workload: Arc<WorkloadController>, // admission control (see Deferred — per-cell fairness)
}

// today's single `Multi { graphs, config_path, server_policy }` (rfc-005) becomes ONE cell;
// the server boots N of them.
pub enum ServerConfigMode {
    MultiCluster { cells: Vec<CellBootConfig> },
}
```

Boot opens each cell's applied revision (the existing rfc-005 path, run N times,
bounded-concurrency) and inserts `Arc<CellRuntime>` into the `CellRegistry` keyed by
its host (or path prefix). A **dedicated/on-prem** deployment boots a one-entry map; a
**pooled** deployment boots many.

### 5.3 Routing & middleware — one new outer hop

The existing `build_app` nests per-graph routes under `/graphs/{graph_id}` with two
`route_layer`s (`resolve_graph_handle` inner, `require_bearer_auth` outer). We add
**one outermost layer**, `resolve_cell`, and rebind two existing layers to read the
cell instead of `AppState`:

```rust
let per_graph_protected = Router::new()
    .route("/snapshot", get(server_snapshot))
    // … /query /mutate /queries /schema /load /branches /commits …
    .merge(mcp::mcp_router(state.clone()))                       // RFC-003 — unchanged
    .route_layer(from_fn_with_state(state.clone(), resolve_graph_handle))  // inner: reads CELL.registry  (was AppState.routing)
    .route_layer(from_fn_with_state(state.clone(), require_bearer_auth))   // mid:   reads CELL.auth      (was AppState.bearer_tokens)
    .route_layer(from_fn_with_state(state.clone(), resolve_cell));         // OUTER: injects Arc<CellRuntime>   ← NEW
```

Request lifecycle:

```
resolve_cell        host/prefix → Arc<CellRuntime>            (404 unknown cell)   ← NEW, outermost
  └─ require_bearer_auth   validate token vs CELL.auth → ResolvedActor   (401)     ← now cell-scoped
       └─ resolve_graph_handle   {graph_id} in CELL.registry → Arc<GraphHandle> (404) ← now cell-scoped
            └─ handler / MCP   run_query · run_mutate · /mcp · Cedar enforce(...)   ← UNCHANGED
```

The only handler-adjacent edits: `require_bearer_auth` reads `cell.auth`,
`resolve_graph_handle` / `server_graphs_list` read `cell.registry`. The isolation is
in the ordering: **cell-A's token table and registry are unreachable from a cell-B
request** because the cell is resolved first and everything downstream reads *that*
cell.

**Cell selector — host-based (recommended) vs path-based:**

| | Host-based | Path-based |
|---|---|---|
| Selector | `Host: tenant-a.omnigraph.example.com` | `/clusters/{cell_id}/…` |
| OAuth audience | the per-tenant origin (natural RFC 8707 resource) | `…/clusters/{cell_id}` |
| Origin/CORS | isolated per subdomain (free) | shared origin |
| DNS/cert | wildcard `*.example.com` → pooled fleet | one host |
| `resolve_cell` | `cells.by_host(host)` | `cells.by_prefix(first_segment)` |

Host-based wins for cloud (per-tenant audience, Origin, and cookie boundaries fall out
for free). Path-based is the simple on-prem/dev shape. `resolve_cell` abstracts which.

### 5.4 Identity & auth — per cell, two modes

```rust
pub enum CellAuth {
    Static(Arc<[(BearerTokenHash, Arc<str>)]>),   // on-prem / self-host / dev — today's path, per cell
    Oidc(Arc<dyn TokenVerifier>),                  // WorkOS (or customer IdP) for this org — cloud
}
```

- **WorkOS Organization → cell, 1:1.** The cell's `Oidc` verifier is configured with
  *that org's* issuer + audience. A token minted for `tenant-a`'s audience **fails
  verification at `tenant-b`** (wrong `aud`) — structural isolation that runs *before*
  Cedar and is independent of policy completeness.
- **Same Resource-Server endpoint, mode by cell.** `require_bearer_auth` dispatches on
  `cell.auth`. Static and OIDC cells coexist in one process. This is the
  `TokenVerifier` seam already drafted in `identity.rs` ("RFC 0001 step 1 adds
  `AuthSource::Oidc` when the `OidcJwtVerifier` ships"); WorkOS is one implementation.
- **`ResolvedActor` mapping:** `actor_id` ← `sub`; `tenant_id` ← the **cell id** (for
  audit/log clarity — *not* an isolation mechanism, since the endpoint already
  isolated); `scopes` ← the OAuth `scope`/roles claim; `source` ← `Oidc`/`Static`.
  This repurposes `tenant_id` from vestigial pooled scaffolding into "which cell logged
  this," which is honest. **Identity stays server-resolved, never client-set** (the
  MR-731 invariant, now applied per cell).
- **Per-cell OAuth discovery:** each cell serves its own
  `/.well-known/oauth-protected-resource` → that org's WorkOS AS, with the cell's
  audience. Per-tenant PRM → per-tenant OAuth → per-tenant audience. (The RFC-003 §8
  PRM config-gate for issue #59467 becomes a per-cell flag.)

### 5.5 Authorization — Cedar stays per-graph / per-cell

The cell boundary already guarantees a cell-A actor never reaches cell-B's policy
engine, so **no tenant dimension is added to authorization**:

- `PolicyRequest { action, branch, target_branch }` and `ResourceScope`
  (Graph / Branch / TargetBranch) — **unchanged**. The principal stays `actor_id`.
- `authorize`'s **default-deny-except-`Read`** becomes *safe*: "readable on missing
  policy" now means the tenant's *own* graphs, not cross-tenant. The exact hazard that
  would make this dangerous under pooled tenancy is structurally absent.
- `GET /graphs` reads `cell.registry`, so it enumerates only the tenant's own graphs
  and storage URIs — no cross-tenant topology leak.

This is the payoff of cluster-as-tenant: the in-process tenant machinery a pooled
model would require (tenant-keyed routing, a tenant Cedar principal, a tenant-aware
deny default, a tenant-filtered enumeration) is **not built because it is not needed**.

### 5.6 Control plane — the one legitimately pooled component

A small **Cell Registry** holds *metadata only* (no tenant data):

```
org_id (WorkOS)  ──▶  cell_id  ──▶  { storage_root, issuer, audience, host, tier }
```

Onboarding a tenant is provisioning-as-code — the thing that makes silo *operable*
(automated, not N hand-built stacks):

```
1. WorkOS Organization created / detected.
2. `cluster apply` a NEW cell on a fresh storage root (own bucket/prefix), with the
   org's schema.pg / queries / policy        → ledger + recovery + approvals (rfc-004).
3. Register org_id → cell_id (+ issuer/audience/host) in the Cell Registry.
4. Cell goes live:
     • dedicated tier → its own process boots that one cell (today's exact path).
     • pooled tier   → the fleet HOT-LOADS the cell into the CellRegistry map.
5. DNS: tenant-a.example.com → the pooled fleet (wildcard); the host selects the cell.
```

Step 4's pooled hot-load is the **one new runtime-mutation primitive**, and it is
deliberately **cell-granular, not graph-granular**: [rfc-011](rfc-011-cli-refactoring.md)
closes runtime *graph*-add inside a cluster (correct — it mutates a live registry),
but loading a **whole, independently-validated cell** is just "open a cluster" — its
own ledger/recovery/catalog, nothing in any other cell moves. Far safer than the thing
rfc-011 forbids. Eviction = drop the `Arc<CellRuntime>` from the map; in-flight
requests keep their `Arc`.

### 5.7 Deployment tiers — same binary, different topology

| Tier | Topology | Mode | Use |
|---|---|---|---|
| **Dedicated** | 1 process : 1 cell | `MultiCluster { cells: [one] }` | enterprise / regulated / data-residency |
| **Pooled** | 1 process : N cells | `MultiCluster { cells: [many] }` | SMB / free / long tail |
| **On-prem** | 1 process : 1 cell, `Static` auth | `MultiCluster { cells: [one], Static }` | air-gapped / self-host |

A tenant graduates pooled → dedicated by moving its cell to its own process — **no data
migration** (the storage root does not move; the cell is already self-contained).

### 5.8 Workload scaling — readers vs writers

The substrate makes read and write scaling **asymmetric**, and the cell model inherits
and exploits that asymmetry. It is the deployment-shaping fact, so it is part of the
tenancy design, not an afterthought.

**Reads scale horizontally and statelessly.** The object store is the source of truth;
a read is snapshot-isolated and holds no shared mutable state, so any number of
processes can open a snapshot and serve reads with **zero coordination**. Add read
replicas (even per-region) freely. Per-cell independence means per-tenant read scaling
is independent. Limits are real but ordinary: object-store request throughput
(per-prefix S3 limits), index residency + CPU/RAM for *heavy* reads (vector ANN, FTS,
aggregations), and in-process cache warmth (a cold tenant in a pooled fleet pays a
cold-open). Freshness is the one catch — a replica's engine handle may lag live HEAD
([rfc-011](rfc-011-cli-refactoring.md) note), so the read path is eventual within the
refresh window unless pinned.

**Writes are serialized per `(table, branch)` and scale by partitioning, not by adding
writers.** A single graph's main-branch write throughput is bounded by commit latency
(object-store round-trips for stage + manifest CAS) and degrades under concurrency
(one-winner CAS → retry). You scale writes by *partitioning the write set*:

- more **branches** — different `(table, branch)` queues don't contend; bulk loads fan
  out onto review branches and **merge** serializes the integration;
- more **graphs**, more **cells** — fully independent write paths.

Multiple replicas writing the *same* `(cell, graph, branch)` contend on manifest CAS —
the documented one-winner-CAS territory (invariants.md known gaps: "Local
`write_text_if_match` is not a cross-process CAS"; "recovery serialized against live
writers in-process only"). So horizontal write scaling on one branch needs a **single
active writer/coordinator**, not N racing replicas.

**The deployment shape this implies — split the roles (CQRS at the deployment layer):**

| Role | Scaling | Topology |
|---|---|---|
| **Read fleet** | horizontal, stateless, regional | many replicas behind a LB; each opens snapshots from object store |
| **Write tier** | one active coordinator per `(cell, graph, branch)` | small; consistent-hash or a per-`(graph,branch)` lease routes writes to a single owner to bound CAS thrash |
| **Maintenance** | single-coordinator, out-of-band | `optimize`/`cleanup`/reindex as async jobs ([rfc-011](rfc-011-cli-refactoring.md) D11), never inline with serving |
| **Heavy reads** | own pool (optional) | vector/FTS/aggregation isolated so they don't starve point reads |

- **Read-your-write:** the write envelope returns `commit_id` / `snapshot_id`
  ([rfc-001](rfc-001-queries-envelope-mcp.md)); a client needing RYW pins its next read
  to that snapshot, or is routed (sticky) to the writer / a freshly-refreshed replica.
  Otherwise reads are eventual within the handle-refresh window — make that explicit per
  [rfc-003](rfc-003-mcp-server-surface.md)'s "no silent eventual consistency."

**Cell-model interaction.** Read scaling is per-cell-independent and horizontal; write
scaling is per-`(cell, graph, branch)`. The cell bounds blast radius and lets a hot
tenant get a dedicated write coordinator (or its own process — the dedicated tier),
while the long tail shares a pooled read fleet. The one genuinely hard coordination
problem is a **pooled write fleet**: two replicas must not own the same `(cell, graph,
branch)`, so it needs routing affinity or a lease — which ties directly to the
cross-process-CAS known gap and must be closed before pooled *writes* (pooled *reads*
are unaffected). Single-writer-per-partition + many-readers is exactly what an
object-store, log/Git-structured engine wants; the design states it rather than fighting
it.

## 6. How the surfaces ride on top

This is a server/topology change; the surfaces are consumers and need little or no
change:

- **HTTP data plane.** Every protected route already resolves `Arc<GraphHandle>` from a
  request extension; it now comes from the cell's registry. Handlers are unchanged.
- **MCP ([rfc-003](rfc-003-mcp-server-surface.md)).** The MCP backend "consumes a
  resolved actor and branches on nothing about how the token was verified" and mounts
  under `per_graph_protected`. So `/graphs/{id}/mcp` simply lives under a cell now:
  `https://tenant-a.example.com/graphs/{id}/mcp`. Per-graph isolation (rfc-003 §15.1)
  is *sufficient* under cluster-as-tenant — each tenant's MCP clients point at their
  own cell's endpoints; the discovery/enumeration concerns that would bite a pooled
  model do not apply. MCP is **one** tenant-scoped consumer, not the reason for this
  RFC.
- **CLI ([rfc-011](rfc-011-cli-refactoring.md)).** A `--server <name|url>` scope already
  addresses one served endpoint; a per-tenant subdomain is just a server URL. No new
  addressing concept — the cell is reached as a server.

## 7. Migration & backward compatibility

Today's `omnigraph-server --cluster <one>` is a `MultiCluster` with **one cell** and a
**host-agnostic** `resolve_cell` (any host → the sole cell). Therefore:

- Existing single-cluster deployments keep working unchanged (one cell; `resolve_cell`
  is identity).
- `--cluster <dir|s3>` stays the dedicated/on-prem entry point.
- A pooled fleet boots from a cell list (e.g. repeated `--cluster`, or a
  `--cells <registry>` source).
- RFC-003 MCP, OpenAPI generation, and CLI addressing are unchanged; `/graphs/{id}/…`
  just lives under a cell.
- `ResolvedActor.tenant_id` / `GraphKey.tenant_id` are **repurposed to the cell id**
  (or removed) — they stop implying pooled-row tenancy. This is the cleanup that ends
  the two-models ambiguity.

## 8. Invariants & deny-list check

- **§11 transport/auth at the boundary:** cell auth + `resolve_cell` live only in
  `omnigraph-server`; engine/compiler/cluster crates never learn cells exist. ✓
- **§12 no client-set identity:** both the cell (host/prefix → registry) and the actor
  (token → `ResolvedActor`) are server-resolved; neither is client-settable. ✓
- **No cloud-only correctness / no fork:** cells are a server-layer wrapper; the OSS
  `Static`-auth cell is first-class, OIDC is additive. ✓
- **Strong consistency / manifest atomicity (§1–§6):** untouched — this adds an outer
  routing dimension, not a write path. Each cell's engine keeps its own snapshot
  isolation and manifest publish. ✓
- **No state that drifts:** the Cell Registry is control-plane metadata; per-cell state
  remains the cluster's applied revision (rfc-005), derived from the ledger. ✓
- **Least privilege:** a leaked cell token reaches one tenant; rotation is per-cell;
  storage credentials stay with the server process, never operators. ✓

## 9. Decisions, open questions, deferred

**Decided:**
- Cluster = tenant = cell; silo data, pool compute. No row-level pooling.
- Cells resolved before auth; per-cell token source; per-cell Cedar.
- WorkOS Organization → cell 1:1; per-cell OAuth audience.
- `tenant_id` repurposed to cell id (audit), not an isolation mechanism.

**Open / deferred:**
- **Per-cell vs process-wide admission control.** `WorkloadController` is process-wide
  today; pooled cells want per-cell fairness (noisy-neighbor). Make `workload` a
  per-cell value or add per-cell quotas. This is the one shared resource pooling
  reintroduces — design before the pooled tier ships.
- **Cell hot-load / evict protocol.** Control-plane push vs poll of the Cell Registry;
  pin the consistency story (a cell appears atomically or not at all; eviction drains).
- **Cell Registry storage.** Its own OmniGraph graph, a control DB, or a config object
  — metadata-only either way. Decide ownership and durability.
- **`TokenVerifier` trait shape.** Still draft in `identity.rs`; this RFC fixes *where*
  it plugs in (per-cell `CellAuth::Oidc`), not its exact signature.
- **Scope semantics.** `ResolvedActor.scopes` is currently `[Full]` and read nowhere;
  when OIDC populates real scopes, decide whether/how they feed Cedar context (a
  behavior change to sequence deliberately, not silently).
- **Pooled-write coordination (§5.8).** A pooled write fleet needs a single active
  owner per `(cell, graph, branch)` (consistent-hash or lease) and the cross-process
  CAS gap closed first. Pooled *reads* need none of this; gate pooled *writes* on it.
  Read-fleet freshness (handle-refresh lag) + read-your-write pinning is the companion
  decision.

## 10. Relationship to prior RFCs

[rfc-005](rfc-005-server-cluster-boot.md) made the server boot one cluster from its
applied revision; this RFC makes the server boot *many* such clusters as cells and
resolves one per request. [rfc-011](rfc-011-cli-refactoring.md) fixed the
store/cluster/server ontology and closed runtime graph mutation; this RFC adds the
*cell* as the tenant unit and the one safe runtime mutation (cell hot-load) that does
not violate rfc-011's reasoning. [rfc-004](rfc-004-cluster-graph-schema-apply.md)'s
ledger/recovery/approvals are what make a cell a self-contained, independently
provisionable unit. [rfc-003](rfc-003-mcp-server-surface.md) is a consumer: its
per-graph MCP surface becomes per-tenant for free once each cluster is a tenant. The
net: tenancy is decided once, at the server topology layer, and every surface inherits
it.
