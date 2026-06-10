# Cluster Config Implementation Spec And Blast Radius

**Status:** Draft / implementation planning
**Type:** Downstream design spec
**Date:** 2026-06-08
**Relationship:** companion to [cluster-config-specs.md](cluster-config-specs.md)
and [cluster-axioms.md](cluster-axioms.md). The high-level spec explains why
the cluster control plane should exist; this file names what must change
downstream and how large the blast radius is.

<!-- Spec note: this file exists so the user-facing cluster spec can stay
readable. Keep implementation inventories, rollout phases, and test ownership
here instead of expanding the narrative spec into an encyclopedia. -->

## Executive Summary

Overall blast radius: **very high**.

This is not a small extension to `omnigraph.yaml`. The target design creates a
new shared cluster desired-state document, a locked state ledger, a cluster
manifest publisher, and a reconciler that coordinates resources above a single
graph. The existing config system remains useful, but its role changes:

- `omnigraph.yaml` / global config remains the per-operator and startup bridge.
- `cluster.yaml` becomes shared desired state for a deployment.
- The cluster state ledger becomes the authoritative record of applied reality.
- Server/runtime surfaces eventually read from the cluster catalog instead of
  only from process-start config.

Safe rollout requires an additive path. Do not replace the current config,
server, or policy behavior in one step.

## Current Surfaces Surveyed

| Surface | Current behavior | Why it matters |
|---|---|---|
| `omnigraph-config::OmnigraphConfig` | Layered global/state/project config for CLI and server startup; strict `version: 1`; named maps replace wholesale | A cluster spec needs different ownership and merge semantics; do not stretch this type until it becomes ambiguous |
| `omnigraph-server::load_server_settings` | Opens either one selected graph or every configured embedded graph in multi mode | Cluster config changes startup, registry identity, and eventually runtime reconcile |
| `GraphRegistry` | Holds open graph handles; production registry is startup-only today; runtime insert is test-only | Cluster apply wants graph add/remove/reload as real control-plane operations |
| `omnigraph-queries::QueryRegistry` | Loads `.gq` files from `queries:` and honors `mcp.expose` for catalog listing | Target cluster config removes exposure from the registry and moves list/invoke to policy |
| `omnigraph-policy::PolicyAction` | Per-graph actions plus server-scoped `graph_list`; `invoke_query` is graph-scoped and coarse | Cluster plan/apply and per-query exposure need new policy scope without breaking coarse rules |
| Engine graph manifest | Graph-level atomic visibility via `__manifest`, expected table versions, and recovery sidecars | Cluster apply needs a higher-level publisher; Lance still commits per dataset |
| Schema apply | Existing plan/apply/lock shape for one graph; soft/hard drops already modeled | This is the prototype resource reconciler, but cluster apply cannot call it blindly and then claim cluster atomicity |
| Public docs/tests | Config, policy, server, and query behavior are already documented and tested | Every behavior change below has user docs and test fallout |

## Compatibility Stance

<!-- Spec note: keep `cluster.yaml` separate from `omnigraph.yaml` because the
current file is deliberately layered and partly per-operator. Collapsing shared
cluster intent into it would blur the source-of-truth split the high-level spec
is trying to create. -->

1. `cluster.yaml` is a new target-state file, not `omnigraph.yaml` v2.
2. Existing `omnigraph.yaml` keeps working for CLI, server boot, aliases,
   graph locators, bearer-token env lookup, and the current stored-query
   registry.
3. Initial cluster commands are explicit: `omnigraph cluster validate`,
   `omnigraph cluster plan`, `omnigraph cluster apply`, `omnigraph cluster
   status`, `omnigraph cluster refresh`, and `omnigraph cluster import`.
4. Cluster config is one shared folder, resolved from the command's cluster
   root or explicit path. It is not merged from global + project + active
   context layers.
5. The per-operator connection layer selects the cluster root and actor
   identity. It is not committed into `cluster.yaml`.
6. `mcp.expose` remains supported in current `omnigraph.yaml` until the
   per-query policy replacement ships.
7. **Single ownership (axiom 15).** While `omnigraph.yaml` and the cluster
   catalog coexist, each fact is read from exactly one source at a time.
   Phase 5 server boot is an exclusive mode switch — boot from cluster state
   XOR from `omnigraph.yaml` — never a precedence-merge of both. No phase may
   introduce a surface that reads the same fact (graph set, query registry,
   policy wiring, bind address) from both sources with tie-break rules.
8. **`omnigraph.yaml` shrinks; it does not get deprecated.** Its terminal role
   is the per-operator layer: connection/cluster selection, the operator's
   credential reference, active graph/branch context, CLI ergonomics, and
   purely personal aliases (target home: the operator's global config dir per
   RFC-002). Shared-truth keys migrate to `cluster.yaml`; per-operator keys
   never do.
9. **Bridges carry sunsets.** Every compatibility bridge names its replacement
   and the phase that removes it (`mcp.expose` → Phase 6 policy-owned exposure
   is the template). A bridge without an exit is a review-blocking finding.

## Terraform-Aligned Schema Validation

<!-- Spec note: Terraform is strict for resource/provider/module configuration,
but looser for variable-value inputs such as `.tfvars` and `TF_VAR_*`. For
cluster desired state we borrow the strict resource-schema posture because
`cluster.yaml` is shared intent, not an operator-local variable bag. -->

Every field in target-state `cluster.yaml` must be **honored or rejected**:

- If a field is part of the declared resource schema, it must affect
  validation, plan, apply, state, or status.
- If a field is misspelled, placed under the wrong resource kind, or reserved
  for a future phase, `cluster validate` / `cluster plan` must fail with a
  typed diagnostic.
- Compatibility warnings are allowed only in an explicit migration window for
  old schema versions. They are not allowed in the target schema.
- Free-form extension areas must be named as such, for example `labels`,
  `metadata`, `vars`, or `provider_options`; accidental unknown keys are never
  treated as extension data.

Examples:

```yaml
graphs:
  knowledge:
    schema: ./knowledge.pg
    lables: { team: platform }       # invalid: typo, use `labels`

pipelines:
  github_sync:
    source: { kind: github, token: ${GITHUB_TOKEN} }
    into:
      - { graph: engineering, map: ./github.map.yaml }
    retry_magic: true                # invalid unless `retry_magic` is in schema
```

```yaml
graphs:
  knowledge:
    schema: ./knowledge.pg
    labels: { team: platform }       # valid free-form metadata bucket
    provider_options:
      lance:
        compaction_window: daily     # valid only if this extension is declared
```

## Typed Resource And Provider Addresses

<!-- Spec note: this is the Terraform-aligned version of "typed locators".
The target cluster spec should not ask later code to guess whether a string is a
graph name, query name, server endpoint, storage URI, source connector, or
credential reference. References carry their kind. -->

<!-- Fix (2026-06-08): resolved the "shorthand may exist" (here) vs "bare strings
are bad shape" (below) contradiction. The rule is now explicit: bare names ARE
valid shorthand in a field whose schema fixes the referent kind (normalized to a
typed address); "bad shape" means a value whose KIND is ambiguous or WRONG, not
merely bare. This also makes the high-level spec's bare examples (policy
`graphs:`/`applies_to:` lists, pipeline `into.graph`, dashboard `graphs:`) valid. -->
A locator is a typed address to another declared thing. **Internally — in plan and
state — every reference is a typed address** (axiom 9). At the config *surface* a
field may accept **bare shorthand when its schema fixes the referent kind** (a
policy `applies_to:` list is graph refs; a pipeline `into.graph` is a graph id) —
the parser normalizes it to the typed address before planning. A value whose
*kind* is ambiguous or wrong (a `source:` that could be a connector type, an
instance, or a provider) has no safe normalization and must be a typed
`provider.*` address or an explicit inline block.

Target address forms:

```text
graph.<graph_id>
schema.<graph_id>
query.<graph_id>.<query_name>
policy.<policy_name>
ui.dashboard.<dashboard_name>
pipeline.<pipeline_name>
provider.storage.<provider_name>
provider.source.<provider_name>
provider.embedding.<provider_name>
```

Bad shape — the value's **kind is ambiguous or wrong**, not merely bare:

```yaml
pipelines:
  github_sync:
    source: github                             # AMBIGUOUS kind: connector type, instance, or provider?
                                               #   → provider.source.<name> or inline { kind: github, ... }
policies:
  base_rbac:
    applies_to: [query.knowledge.find_experts] # WRONG kind: a query address in a graph-ref field
```

OK shorthand (kind fixed by the field → normalized):

```yaml
policies:
  base_rbac:
    applies_to: [knowledge, engineering]       # bare names in a graph-ref field → graph.knowledge, graph.engineering
```

Target shape:

```yaml
providers:
  storage:
    prod_graphs:
      kind: s3
      bucket: company
      prefix: prod
  source:
    github_org:
      kind: github
      token: ${GITHUB_TOKEN}

graphs:
  knowledge:
    storage: provider.storage.prod_graphs
    path: graphs/knowledge.omni
    schema: ./knowledge.pg
  engineering:
    storage: provider.storage.prod_graphs
    path: graphs/engineering.omni
    schema: ./engineering.pg

policies:
  base_rbac:
    file: ./base_rbac.policy.yaml
    applies_to:
      - graph.knowledge
      - graph.engineering

pipelines:
  github_sync:
    source: provider.source.github_org
    into:
      - { graph: graph.engineering, map: ./github_to_engineering.map.yaml }
      - { graph: graph.knowledge,   map: ./github_to_people.map.yaml }
```

<!-- Fix (2026-06-08): this example shows the EXPLICIT/external graph-storage case
(`storage:` + `path:`). It is not the default — per "Known High-Risk Design
Decisions" §2 and the cluster storage layout, graph roots derive to
`ClusterRoot/graphs/<id>.omni` by default; an external storage provider is the
opt-in. The pipeline `into.graph` here is typed (`graph.engineering`); the bare
`{ graph: engineering, ... }` shorthand is equally valid (normalized). -->

Validation rules:

- A field that expects a graph address accepts `graph.<id>`, not
  `query.<graph>.<name>` or an arbitrary string.
- A field that expects a query address accepts `query.<graph>.<name>`, and the
  planner validates both the graph and the query symbol.
- A field that expects a source provider accepts `provider.source.<name>`, not
  `provider.storage.<name>`.
- A field that expects storage accepts `provider.storage.<name>` or an explicit
  storage block, not a server URL or source connector.
<!-- Fix (2026-06-08): shorthand is a present rule, not "future syntax" — it is how
the high-level spec's bare examples are valid. -->
- A field whose schema **fixes the kind** accepts bare shorthand (e.g. `knowledge`
  in a graph-ref field) and normalizes it to the typed address; a kind-ambiguous
  or wrong-kind value is rejected with a typed diagnostic.
- Plan and state always store the **normalized typed address**, regardless of
  whether the surface used shorthand.

## Target Components

Preferred split:

| Component | Responsibility | Depends on |
|---|---|---|
| `omnigraph-cluster` crate | Cluster spec types, path resolution, resource graph, plan model, state backend traits, apply orchestration | `omnigraph-config` only for shared simple config types if needed; avoid server deps |
| `omnigraph` engine additions | Graph lifecycle primitives, schema-apply integration, recovery hooks for graph moves during cluster apply; optional future cluster manifest publisher if JSON state is not enough | Lance, existing graph manifest/recovery |
| `omnigraph-cli` | `cluster *` commands, plan rendering, approval collection, state lock UX | `omnigraph-cluster`, engine |
| `omnigraph-server` | Optional boot from cluster state, registry reload, status endpoints, policy-filtered query catalog | `omnigraph-cluster`, engine, policy |
| `omnigraph-policy` | Cluster/server actions, per-query list/invoke scope, approval policy predicates | none above server |
| `omnigraph-queries` | Registry without exposure side-channel; dependency metadata for downstream validation | compiler/config |
| `omnigraph-api-types` | New status/plan/apply response types if cluster HTTP endpoints ship | serde only |

If the first implementation avoids a new crate, keep the same boundary in
modules. The important constraint is that cluster spec parsing must not drag
HTTP/server code into compiler or engine crates.

## Resource Model

Resource identity is stable and typed:

```text
ClusterRoot
ResourceKey = <kind>/<scope>/<name>
ResourceAddress = <kind>.<name> | <kind>.<graph_id>.<name>
ProviderAddress = provider.<kind>.<name>

graph/cluster/knowledge
schema/graph:knowledge/main
query/graph:knowledge/find_experts
policy/cluster/base_rbac
ui/cluster/dashboard.overview
pipeline/cluster/github_sync
alias/cluster/experts
embedding/cluster/default
```

<!-- Fix (2026-06-08): resource key uses `dashboard.overview` (dot) to match the
address form `ui.dashboard.<dashboard_name>` — was `dashboard:overview`. `dashboard`
is the only ui sub-kind today. -->

Resource records carry:

| Field | Meaning |
|---|---|
| `kind` | Graph, Schema, Query, PolicyBundle, UiSpec, Binding, Alias, EmbeddingConfig, Pipeline |
| `scope` | Cluster or graph id |
| `name` | Stable resource name inside scope |
| `fingerprint` | Content hash of the normalized spec and all referenced files |
| `dependencies` | Resource keys this resource references |
| `observed` | Applied graph manifest version, policy digest, query digest, schedule id, etc. |
| `status` | `Pending`, `Planned`, `Applying`, `Applied`, `Drifted`, `Blocked`, `Error` |
| `conditions` | Typed details such as `ActualAppliedStatePending`, `NeedsApproval`, `DependencyMissing`, `PartialPipelineRun` |

The planner builds a dependency graph from these records and uses it for both
validation and blast-radius reporting.

## Terraform-Style Validate / Plan / Apply

The cluster workflow deliberately mirrors Terraform's safe sequence:

```text
cluster validate   # parse + schema-check desired config, no state mutation
cluster plan       # diff desired config against state, with optional refresh
cluster apply      # apply an accepted fresh plan and update state
cluster status     # read state-backed deployed reality
cluster refresh    # repair/import observations from actual cluster state
```

Implementation rollout follows the same safety posture: ship parser/validate
first, then read-only plan, then state backend and lock, then apply.

The plan is a structured artifact, not just terminal text. It must include:

| Plan field | Why it exists |
|---|---|
| `desired_revision` | Git commit / config digest being evaluated |
| `resource_digests` | Exact digest of every schema, query, policy, UI, pipeline, and map file |
| `dependencies` | Edges such as query -> graph/schema, dashboard -> query, pipeline -> source provider + graph |
| `state_observations` | Applied revision, resource fingerprints, graph manifest versions, status conditions, and drift |
| `changes` | Create/update/delete/replace/refresh-only operations |
| `blast_radius` | Downstream resources to revalidate or affected behavior to surface |
| `approvals_required` | Irreversible/data-loss or compatibility-narrowing gates |

`cluster apply` must reject a stale plan when state, resource digests, or
observed graph versions no longer match the plan base. The operator or agent
must re-plan or explicitly refresh first.

## Cluster Storage Layout

Target Phase-1 cluster-root layout:

```text
<cluster-root>/
  __cluster/
    state.json
    lock.json
    status/
      <resource-address>.json
    approvals/
      <ulid>.json
    recoveries/
      <ulid>.json
    resources/
      query/<graph>/<name>/<digest>.gq
      policy/<name>/<digest>.yaml
      ui/<name>/<digest>.dashboard.yaml
      pipeline/<name>/<digest>.pipeline.yaml
  graphs/
    <graph_id>.omni/
```

<!-- Spec note: JSON is the baseline because it matches Terraform state, is
easy to inspect/repair, and avoids bootstrapping Lance datasets before the
control-plane semantics are proven. -->
The exact filenames can change, but the shape cannot:

- There is one cluster-control namespace under the cluster root.
- Graph data remains in ordinary OmniGraph graph roots.
- State is a locked/CAS-updated JSON document, not a Lance dataset.
- Status, approval, and recovery ledgers are append-only or per-resource JSON
  records until table semantics are proven necessary.
- Resource payloads are content-addressed by digest so apply can be idempotent.
- Cluster state is not inferred from the operator's working tree.
- A Lance-backed control-plane store is a future backend option only if
  row-level queryability/history or tighter publish fencing justifies it.

## State Backend Protocol

### Cluster-Hosted JSON State

When `state.backend: cluster`, the baseline backend stores JSON documents under
`<cluster-root>/__cluster/` and protects `state.json` with object-store lock/CAS.
It is cluster-hosted, but it is still a separate state write from graph Lance
manifest movement.

Apply protocol:

1. Acquire the cluster state lock.
2. Read current `state.json` and backend CAS token / object generation.
3. Validate plan base still matches state.
4. Write a cluster recovery sidecar before any graph manifest or non-idempotent
   resource can move.
5. Write content-addressed resource payloads and perform any required graph
   manifest movements.
6. CAS-update `state.json` with the new applied revision, resource
   fingerprints, observed graph versions, status references, and approval /
   recovery references.
7. If step 6 fails after actual resources moved, do not acknowledge success.
   Surface `ActualAppliedStatePending` and require `refresh` / `import` repair.
8. Delete the sidecar and release the lock only after the state outcome is
   recorded.

### External State

<!-- Spec note: external state is a separate commit domain. The protocol below
prevents an apply from returning success after the cluster moved but the state
ledger failed to record that movement. -->

When `state.backend` points outside the cluster root, the same JSON state shape
lives in an external store. It is locked and CAS-updated, but it is not atomic
with Lance or OmniGraph manifests.

Apply protocol:

1. Acquire the external state lock.
2. Read state and CAS token.
3. Validate plan base still matches state.
4. Write a cluster recovery sidecar.
5. Perform the cluster resource changes.
6. CAS-update external state with the new applied revision, statuses, and the
   observed graph manifest / resource versions it records.
7. If step 6 fails, do not acknowledge success. Surface
   `ActualAppliedStatePending` and require `refresh` / `import` repair.
8. Release the external lock only after the state outcome is recorded.

This mode can be strongly coordinated, but it must never be documented as one
atomic commit across both stores.

### Future Lance-Backed State

A Lance-backed state/status/approval/recovery store is deliberately not the
baseline. It becomes attractive only if JSON files become a real liability:
large status sets need structured filtering, approval/recovery history needs
table scans, or cluster apply needs a manifest publisher that can fence state
and graph-version pins together. Until then, Lance datasets add bootstrapping,
schema migration, and control-plane recovery surface without enough benefit.

## Cluster Manifest Publisher

The cluster publisher is a possible later layer above today's graph publisher.
It does not replace Lance or the per-graph `__manifest` table, and it is not
required for Phase-1 JSON state / read-only plan.

Required semantics:

| Requirement | Detail |
|---|---|
| Expected-version CAS | Every resource in an apply group supplies its expected current version/fingerprint |
| Resource changes | Register/update/tombstone resource payloads and graph version pins |
| Graph-head fencing | If a graph schema/lifecycle operation moves a graph manifest, the cluster manifest records the exact graph manifest version |
| Sidecar coverage | Any graph or cluster resource that can move before cluster publish must be recoverable all-or-nothing |
| Deterministic publish order | Sidecars and apply groups process in stable order |
| Loud partials | If a group cannot be rolled back or forward in-process, status records the condition before more apply work proceeds |

The risky case is nested publish:

```text
schema apply moves graph:knowledge manifest
cluster apply has not yet published query/policy/state records
process crashes
```

That is not safe unless the cluster sidecar records enough information to roll
the graph movement forward into the cluster manifest or roll it back using the
same recovery discipline as current graph recovery.

## Plan Model

Plan output is a durable, replay-checked proposal, not just pretty text:

```text
Plan {
  plan_id,
  desired_revision,
  base_state_revision,
  base_state_cas,
  changes[],
  apply_groups[],
  approvals_required[],
  blast_radius,
  diagnostics[]
}
```

Each change records:

| Field | Meaning |
|---|---|
| `resource` | Stable `ResourceKey` |
| `operation` | Create, Update, Delete, Replace, RefreshOnly |
| `reversibility` | Reversible, Recoverable, CompatibilityNarrowing, IrreversibleDataLoss |
| `effect` | ConfigOnly, Catalog, GraphDefinition, GraphDataRewrite, DataPlaneSchedule |
| `downstream` | Resources that must be revalidated or will observe changed behavior |
| `approval` | None, HumanRequired, PolicyRequired, AlreadySatisfied |

`apply` must re-read state and reject stale plans unless an explicit
`--refresh` / `--replan` path recomputes the plan.

## Downstream Dependency Rules

These are the concrete "what requires downstream" rules.

| Changed resource | Must revalidate / recompute downstream | Blocking failures |
|---|---|---|
| Graph create/delete/rename | Policies, queries, aliases, dashboards, pipelines, bindings, server registry, state graph set | Dangling graph references; duplicate URI; invalid `GraphId`; graph delete without irreversible approval |
| Schema | Stored queries, pipeline maps, UI bindings/query outputs, embedding/index config, data-impact preview, policy predicates once row/type pushdown exists | Unsupported migration; query breakage; missing target type/property; hard drop without approval |
| Stored query | Aliases, UI bindings, policy list/invoke grants, MCP/tool catalog compatibility, typed params | Query file parse/type errors; registry key != `query <name>`; removed query still referenced |
| Policy bundle | Query catalog visibility, graph/server action authorization, approval gates, bootstrap permissions | Invalid Cedar/YAML; server-scoped action in graph policy; per-query list/invoke gap unhandled |
| UI/dashboard | Query bindings, graph refs, output field expectations, policy visibility for referenced queries | Binding to missing graph/query/param/output |
| Alias | CLI command resolution, graph/query refs, shared-vs-personal boundary | Dangling graph/query; mutation alias pointing at read-only context |
| Embedding config | Schema `@embed` columns, model dimension, index rebuild/reconcile, env refs | Dimension mismatch; missing env ref; unsupported model/provider |
| Pipeline definition | Target graph schemas, mapping files, env refs, scheduler/runtime state, per-target run ledger | Missing target graph/type/property; overwrite mode without approval; source secret missing |
| Binding | Referenced source/surface pair, dependency order, visibility policy | Missing source or target; incompatible params |
| State backend config | Lock implementation, import/refresh protocol, apply acknowledgements | Backend missing CAS/lock; state CAS failure after graph/resource movement |

## Blast Radius Matrix

| Area | Required downstream change | Blast radius | Notes |
|---|---|---|---|
| Config parsing | Add strict `cluster.yaml` parser, path/env-ref resolver, resource fingerprints, no layered merge | High | Separate from `OmnigraphConfig`; existing config tests still need backcompat coverage |
| CLI | Add `cluster validate/plan/apply/status/refresh/import`, plan rendering, approval flags, actor threading | High | Must not change existing command selection or `omnigraph use` behavior |
| State backend | Add JSON state document, status/approval/recovery records, lock/CAS, and import/refresh repair | High | Must not silently succeed after state CAS failure |
| Optional cluster publisher | Add a cluster manifest plus table-backed state/status store only if stronger all-or-nothing apply is required | Very high | Touches core atomicity and recovery invariants |
| Recovery | Add cluster sidecars and failpoint coverage for graph-move-before-state-publish gaps | Very high | Any missed sidecar is a correctness bug |
| Graph lifecycle | First-class graph resource create/delete/rename or stable-id story | High | Current server add/remove is intentionally not exposed |
| Schema apply integration | Make schema apply cluster-aware or wrap it with cluster recovery | High | Existing schema apply cannot be treated as cluster atomic by assertion |
| Query registry | Remove target-state exposure flag, add dependency metadata, keep `mcp.expose` bridge | Medium/high | Catalog behavior is observable public API |
| Policy | Add cluster plan/apply/admin actions and per-query list/invoke scope | High | Needs docs, tests, Cedar schema migration, and compatibility with coarse `invoke_query` |
| Server registry | Boot from cluster state, eventually reload/reconcile graph handles, expose statuses | High | Affects routing, OpenAPI, auth, and workload admission |
| API types/OpenAPI | Plan/status/apply DTOs if HTTP management endpoints ship | Medium/high | OpenAPI drift must be regenerated |
| UI specs | New renderer/spec validator/binding checker | High | New product surface, not currently implemented |
| Pipelines | New scheduler/runtime/connector/mapping/idempotency/run ledger | Very high | Second data-plane seam; large product and correctness surface |
| Embeddings | Cluster-level defaults, env refs, model/dimension validation, index interaction | Medium | Existing embedding code is mostly offline/client-side |
| Docs | User docs for cluster config, policy, server, CLI; dev docs for invariants/testing | High | Public contract changes |
| Tests | New cluster suites plus extensions to config/server/policy/recovery/schema/query tests | High | Needs boundary-matched coverage |

## Reversibility And Approval Tiers

| Tier | Examples | Gate |
|---|---|---|
| Display-only | Dashboard layout, non-breaking alias addition | No approval beyond policy |
| Catalog behavior | Add query, hide/list query via policy, add policy grant | Policy check; no data-loss approval |
| Compatibility narrowing | Future validated enum narrowing, query param removal, policy removal that revokes access | Explicit compatibility warning; may require human approval by policy |
| Recoverable definition rewrite | Soft schema drop, graph schema rename, index rebuild | Plan warning; no data-loss approval unless policy requires |
| Irreversible data loss | Graph delete, hard schema drop, cleanup-triggered prior-version reclamation, overwriting pipeline target | Human approval artifact recorded in audit ledger |

Future enum narrowing belongs in `CompatibilityNarrowing` unless the migration
also drops/coerces data or triggers cleanup. That distinction matters for plan
wording and for policy predicates.

## Rollout Phases

<!-- Spec note: the only safe path is staged. The cluster control plane crosses
config, engine, server, policy, and data-plane-adjacent surfaces; a big-bang
replacement would make every invariant harder to audit. -->

### Phase 0: Documentation And Parser Skeleton

- Add cluster spec types and strict parser behind an unused feature/module.
- Implement `cluster validate --config <folder>` with no state backend.
- Validate file paths, env refs, duplicate resource keys, and dependency graph.
- No behavior change to `omnigraph.yaml`, server boot, or query exposure.

### Phase 1: Read-Only Planning

- Add `cluster plan` against a mock/imported state snapshot.
- Produce plan JSON and human output.
- Reuse existing schema migration planner for schema resources.
- Validate stored queries against desired schema.
- Compute downstream dependencies and blast radius.
- Still no apply.

### Phase 2: State Backend And Lock

- Add `state.backend: cluster` JSON storage and lock/CAS.
- Add external backend trait only if lock + CAS semantics are explicit.
- Add `cluster status`, `refresh`, and `import`.
- Persist `AppliedRevision`, `ResourceStatus`, and audit references in JSON.

### Phase 3: Config-Only Apply

- Apply query, policy, UI, alias, embedding, and pipeline definition resources
  that do not move graph manifests.
- Publish by writing content-addressed resource payloads and CAS-updating
  `state.json`.
- Keep server boot from `omnigraph.yaml`; cluster state is inspectable but not
  yet serving traffic.

### Phase 4: Graph And Schema Apply

Detailed design: [rfc-004-cluster-graph-schema-apply.md](rfc-004-cluster-graph-schema-apply.md)
(cluster sidecar schema, roll-forward-only recovery matrix, approval artifacts,
actor threading, 4A/4B/4C staging).

- Add graph create/delete as cluster resources.
- Make schema apply cluster-aware, with sidecar coverage for graph manifest
  movements before JSON state publish.
- Gate irreversible data-loss operations with approval artifacts.
- Consider a cluster manifest publisher only if the JSON sidecar + repair path
  is not strong enough for the accepted safety contract.

### Phase 5: Server Reads Cluster Catalog

- Allow server startup from cluster state.
- Add status and catalog endpoints as needed.
- Keep the current `omnigraph.yaml` startup path as compatibility mode — an
  **exclusive mode switch** per deployment (cluster state XOR `omnigraph.yaml`),
  never a merged read of both (Compatibility Stance #7, axiom 15).
- Regenerate OpenAPI for any HTTP surface.

### Phase 6: Policy-Owned Query Exposure

- Add per-query policy scope for list/invoke.
- Filter `GET /queries` by actor.
- Keep coarse `invoke_query` as a broad allow rule for compatibility until
  docs and migrations say it can be narrowed.
- Deprecate and later remove `mcp.expose` from target-state cluster config.

### Phase 7: Pipeline Runtime

- Add scheduler/worker/runtime.
- Add source connector contracts, mapping validation, idempotency keys,
  per-target run status, and retry behavior.
- Treat fan-out execution as data-plane writes unless explicitly staged through
  branch/merge.

## Test Ownership

Tests must prove the Terraform-style workflow, not just individual parsers.
The minimum behavior contract:

```text
validate catches bad config
plan is deterministic and complete
apply only applies a fresh accepted plan
state changes are locked and durable
drift and partial convergence are visible, not silent
```

| Change | Existing coverage to extend | New coverage likely needed |
|---|---|---|
| Cluster parser | `omnigraph-config` inline config tests for strictness/path resolution | `omnigraph-cluster` parser/dependency tests |
| Plan dependency graph | Schema planner tests, query registry tests | Golden plan JSON for cross-resource downstream impacts |
| State lock/backend | Existing schema apply lock tests as model | JSON state CAS/lock race tests |
| Optional cluster manifest publisher | `crates/omnigraph/src/db/manifest/tests.rs` | Cluster publisher CAS, expected-version, deterministic order tests if that backend ships |
| Cluster recovery | `recovery.rs`, `failpoints.rs` | Phase B -> state publish failpoints, external state CAS failure tests |
| Schema cluster apply | `schema_apply.rs`, failpoints schema apply cases | Nested graph/cluster recovery tests |
| Query exposure policy | `omnigraph-policy` invoke_query tests, server query catalog tests | Per-query list/invoke allow/deny and no-probing tests |
| Server cluster boot | `omnigraph-server/tests/server.rs`, `openapi.rs` | Boot from cluster state, registry reload/status tests |
| CLI cluster commands | `omnigraph-cli/tests/cli.rs`, `system_local.rs` | `cluster validate/plan/apply/status` system tests |
| Pipelines | None today | New runtime/mapping/idempotency/run-ledger suites |

Workflow-specific tests:

| Workflow area | Required assertions |
|---|---|
| Parser / validate | Unknown fields, wrong-kind typed addresses, missing providers, inline secret values, dangling graph/query/pipeline refs, and future-phase fields fail with typed diagnostics |
| Plan goldens | Given config + imported/fake state, plan JSON contains stable resource digests, dependency edges, state observations, proposed changes, blast radius, and approval gates in deterministic order |
| Fresh-plan apply | Changing config digest, state revision, resource digest, or observed graph manifest version after planning makes `cluster apply` reject and require re-plan/refresh |
| State lock / CAS | Concurrent applies against the same backend cannot both succeed; loser gets a typed lock/CAS conflict |
| Recovery / partial apply | Fail after graph/resource movement but before cluster state publish; assert recovery or status surfaces `ActualAppliedStatePending`/sidecar state and never returns success |
| Server/runtime phase | Before cluster state drives routing or registry reload, tests are hermetic: no real home dir, no real global config, no real credentials, no ignored remote tests |
| Pipeline phase | Fan-out run records per-target status, commit ids, retryability, and idempotency keys; no aggregate success unless every target succeeded |

Hard gates:

- Do not ship `cluster apply` until `cluster validate` and read-only
  `cluster plan` have hermetic tests.
- Do not ship graph/schema-moving apply until failpoint recovery tests prove the
  Phase B -> state publish gap is covered. (Stage 3B delivered the apply-side
  half: `omnigraph-cluster` has failpoint infrastructure and tests for the
  crash-after-payload and state-CAS-race windows of config-only apply, plus
  catalog payload verification in status/refresh. Graph-moving sidecar
  coverage remains Phase 4 work.)

For docs-only changes, `scripts/check-agents-md.sh` is enough. For
implementation phases, run the boundary tests above before widening to
`cargo test --workspace --locked`.

## User-Visible Documentation Fallout

The following public docs must change when the corresponding phase ships:

| Phase | User docs |
|---|---|
| Parser/validate | New `docs/user/cluster-config.md`; CLI reference for `cluster validate` |
| Plan/apply | CLI reference, transactions, policy, errors |
| State backend | Storage, deployment, constants, maintenance |
| Server cluster boot | Server, deployment, OpenAPI |
| Policy query exposure | Policy, server, query language / stored-query registry docs |
| Pipelines | New pipeline user guide, deployment, audit, errors |
| Embeddings config | Embeddings, indexes |

Do not ship a user-visible command, flag, env var, endpoint, or config key
without updating the corresponding user doc in the same PR.

## Known High-Risk Design Decisions

1. **Cluster root identity.** Decide whether `metadata.name` is a label or
   identity. Prefer root-derived stable identity plus display name to avoid a
   rename breaking resource identity.
2. **Graph storage derivation.** The high-level sample omits graph storage.
   Implementation should derive graph roots under `ClusterRoot/graphs/<id>.omni`
   by default and treat external graph roots as a separate, explicit feature.
3. **Nested apply.** Schema apply and graph lifecycle cannot move a graph
   manifest outside cluster sidecar coverage.
4. **External state.** Must expose pending repair instead of returning success
   when graph/resource movement succeeds and external state CAS fails.
5. **Per-query policy.** Catalog filtering must avoid probing leaks: callers
   without list/invoke permission should not distinguish hidden from missing.
6. **Pipeline fan-out.** Do not promise atomic multi-graph ingestion unless the
   runtime uses a real branch/merge or equivalent protocol for every target.
7. **Drift correction.** Reconciler-initiated deletes are the same data-loss
   class as human-requested deletes.

## Exit Criteria For A Real RFC

Before implementation begins beyond parser/validate, the RFC must answer:

1. Exact JSON state/status/approval/recovery schemas and object-store paths.
2. Exact sidecar JSON schema and recovery decision matrix.
3. State backend interface and supported lock/CAS implementations.
4. Cluster apply group syntax and dependency ordering rules.
5. Plan JSON schema, including blast-radius and approval fields.
6. Bootstrap authority and first-actor story.
7. Server startup and migration path from `omnigraph.yaml`.
8. Per-query policy schema and compatibility bridge for `mcp.expose`.
9. Pipeline runtime owner, status schema, and idempotency contract.
