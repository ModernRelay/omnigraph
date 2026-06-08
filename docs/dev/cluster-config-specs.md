# Cluster Config Spec — Declarative, As-Code, Agent-Operated

**Status:** Draft / thinking-in-progress
**Type:** Architecture direction
**Date:** 2026-06-07
**Relationship:** generalizes today's `omnigraph.yaml` graph/query/policy configuration surface ([CLI reference](../user/cli-reference.md), [server docs](../user/server.md)) into a future cluster control plane. The distilled rules are in [cluster-axioms.md](cluster-axioms.md); detailed downstream implementation spec and blast-radius assessment in [cluster-config-implementation-spec.md](cluster-config-implementation-spec.md). This is a proposed architecture, not an implemented RFC.

> **Implementation status.** The examples below describe the full target schema.
> Stage 2B only accepts the read-only subset documented in
> [cluster-config.md](../user/cluster-config.md). Future-phase fields such as
> `env_file`, `apply`, `providers`, `pipelines`, `embeddings`, `ui`, `aliases`,
> and `bindings` are intentionally rejected with typed diagnostics until their
> reconciler semantics are implemented.

> **Revision 2026-06-07 — full commitment to the Terraform paradigm.** Three changes from the earlier draft: (1) **state is an authoritative, locked ledger in a backend** (server-hosted *or* a separate cloud store), not "a mostly-rebuildable projection"; (2) `plan` is framed as the **CLI diff between local config and state**; (3) **ETL pipelines** (external data sources) are a first-class config asset — a second seam, alongside schema, where a definition triggers a data-plane effect. The full set of config assets (incl. **aliases**, **embeddings**) is enumerated below.

---

## The problem (the Sarah/Bob test)

Two operators, Sarah and Bob, administer the same OmniGraph deployment. Sarah adds new queries, changes a schema, adds a dashboard, updates policies, and wires in a new data feed.

**How does Bob find out?**

Today he can't — not cleanly. Sarah's changes land in many different places via many different mechanisms:

- schema → the schema-apply path, accepted state in `_schema.pg`, `_schema.ir.json`, `__schema_state.json`, and table versions in the graph manifest
- queries → `.gq` files passed per request or resolved through CLI query roots / aliases; not durable cluster state
- policies → `policy.file` in `omnigraph.yaml`, pointing at Cedar/YAML files that are usually GitOps'd externally
- aliases → CLI sugar in each operator's `omnigraph.yaml`
- external data → ad-hoc `load`/`ingest` scripts, cron jobs, glue code that lives nowhere durable
- UI → undefined

There is no single diff that spans them, no single change record attributed to Sarah, no one place Bob (or Bob's agent) reads to answer "what is this deployment, and what changed?" The state is **fragmented**, and fragmentation is hostile to the one thing an agent must do: reason over the system *as a whole*.

A design passes only if it answers the Sarah/Bob test directly.

---

## Thesis

The unit of declarative state is the **cluster** (the deployment), described by **a single config, as code, in version control**, operated by an **agent** through a plan/apply/reconcile loop against an authoritative state ledger.

Every surface is a declarative as-code artifact — schema (`.pg`), queries (`.gq`), policies (`.yaml`), UI (`.yaml`), aliases, **ETL pipelines**, and embeddings config. The UI is not a separately-deployed application; it is a declarative spec, a first-class resource reconciled exactly like the others.

Three pillars, none optional:

1. **DECLARATIVE** — you describe the desired end state, not the steps. The reconciler computes the steps.
2. **AS CODE** — the config is declarative text in a repo, version-controlled. This is the **source of truth for *intent***.
3. **OPERATED BY AGENT** — an agent authors config changes and drives reconciliation as an authenticated actor, with policy and approval gates. No human state-management burden.

This is **Terraform's model, taken literally**: config (as code) is desired truth; **state is an authoritative, locked ledger** of what has been applied — held in a backend (the cluster, or a separate cloud store); `plan` diffs config against state; `apply` converges reality to config and updates state — applied at **cluster** scope, with OmniGraph as its own data-aware provider and an agent as the controller.

---

## Why as-code (the recursion argument)

"As code" is not branding. It is the structural property that makes a self-describing system well-founded.

Consider the rejected alternative: model the cluster's definition *as a graph* (a meta-graph whose nodes are graphs/policies/queries/UI). To describe a graph you need a schema. The meta-graph's schema is either:

- **hardcoded** → the base case is *code* (you smuggled code in at the bottom anyway), or
- **another graph** → infinite regress, no base case.

Graph-describing-graph never terminates. **Code is the base case.** A declarative config needs no meta-describer because it is parsed by the engine's compiled code — not described by more user-space data.

> **Declarative-as-code terminates. Declarative-as-data (a graph of graphs) recurses.**

This is also why **config** must live **outside** the running system: reviewable (PRs), reproducible (clone + apply), diffable as text, and editable by an agent — without depending on the running system to describe its own intent.

Corollary on direction: change flows **code → cluster, never the reverse.** You do not edit the running system and call that intent. (State, separately, *records* what the cluster currently is — see the next section — but it is never where you express what it *should* be.)

---

## Why per-cluster, not per-graph

The definition Sarah changed does not *belong* to any single graph:

1. **Policies cross-cut graphs.** "Member can't delete on any graph," "who may list/create/delete graphs" — cluster facts. No graph could own them.
2. **"Which graphs exist" has no home in a per-graph model.** The set of graphs is state *above* any graph.
3. **Queries, UI, pipelines, and aliases span graphs.** The MCP/tool catalog an agent discovers is the *cluster's* surface; a dashboard renders multiple graphs; a pipeline may fan out into several.
4. **Cross-graph apply groups.** Sarah may add a graph *and* wire it into the UI *and* grant policy access *and* attach a feed as one logical change — only the cluster can express, plan, and eventually fence that as one apply group.
5. **Operators operate clusters.** Bob is Sarah's peer on a *deployment*, not a graph. The collaboration unit is the cluster.

The graph is a *resource within* the cluster, not the unit of operation.

The mirror question — *why not per-fleet?* — is the same one this section used against per-graph, one level up. A fleet of clusters may eventually want its own declarative spec describing which clusters exist. That recursion is real but **out of scope here**: this proposal stops at the cluster because the cluster is the unit two operators collaborate over. Fleet is the next scope up, named and deferred, not denied.

---

## The model: config / state / reconcile (the Terraform model, literally)

| Layer | What it is | Source of truth for… | Who manages it |
|---|---|---|---|
| **Config** (as code, a folder of files) | Desired state of the whole cluster — graphs, schemas, policies, queries, UI, bindings, aliases, embeddings, ETL pipelines | **Intent** ("what it should be") | Operators/agents, in version control |
| **State** (a locked ledger in a backend) | The authoritative record of what has been applied — applied revision, per-resource fingerprints, observed graph/table versions, audit-record references, resource conditions | **Deployed reality** ("what is") | The reconciler; humans don't hand-edit it |
| **Actual cluster** | The realized *definition* of the running graphs — schema/policies/queries/UI/pipelines as actually in force | — (reality itself) | The engine; `apply` converges it to config |

**`plan`** = `diff(config, state)` → proposed change set (optionally refreshed against the actual cluster).
**`apply`** = acquire the state lock → converge actual → config → **update state** → release lock. Apply does **not** acknowledge success until the state update succeeds; if actual moved but the state write failed, the next `plan` / `refresh` must surface the non-success state and repair or import it before more work proceeds.

### State is an authoritative, locked ledger — not a throwaway projection

This is the 2026-06-07 revision. State is treated exactly as Terraform treats `tfstate`:

- **Authoritative.** State is the trusted record of what is deployed. `plan` diffs config against **state** (fast, deterministic), not against a full live scan of the cluster on every command. "What exists" is answered from state.
- **In a backend.** State lives in a configurable backend: the **cluster's own object-store backend**, or a **separate cloud store** (e.g. a different bucket/account) — the operator's choice, mirroring Terraform's local/S3/remote backends. The config declares which.
- **JSON first.** The baseline state format is Terraform-style JSON documents (`state.json` plus status/approval/recovery JSON records) protected by backend lock/CAS. Lance control-plane datasets are a possible later backend only if row-level history, queryability, or tighter publish fencing justifies the added machinery.
- **Atomicity depends on backend and publish scope.** A JSON state backend, even when stored under the cluster root, is a separate CAS step from graph Lance manifest moves. If actual resources move but the state write fails, apply must surface `ActualAppliedStatePending` (or equivalent) and require refresh/import repair instead of pretending one atomic commit covered every object. A future Lance-backed state backend or cluster manifest publisher may tighten this, but that is not the Phase-1 assumption.
- **Locked.** `plan`/`apply` acquire a **state lock** before touching state, so two operators (or two agents) cannot converge concurrently and corrupt the ledger. This generalizes the existing `__schema_apply_lock__` from schema scope to cluster scope.
- **Reconstructable, but not casually rebuilt.** OmniGraph's edge over opaque-cloud Terraform: the running cluster is self-describing (manifests, commit logs), so a lost state ledger can be **imported / refreshed** from the live cluster. That is a *resilience* property — not licence to treat state as disposable. State is protected and backed up like any source of truth.
- **One slice is never reconstructable.** Who *approved* an irreversible apply cannot be re-derived from a manifest scan. That approval/audit record lives in the **durable audit ledger** (baseline: append-only JSON records in the state backend; future: a Lance table only if needed). State *references* it by id; it never *is* it.

**The control plane reconciles definition, not data.** The reconcile loop converges the cluster's *definition* — schema, policies, queries, UI, bindings, aliases, pipelines, and the set of graphs. It does **not** converge **data**: rows, edges, and vectors are data-plane content, mutated by `load`/`mutate` and by **pipeline execution**, versioned by the commit DAG, and they sit entirely outside the reconcile loop. (`load`/`mutate` never appear in `cluster.yaml`.) **Two** definition kinds *trigger* a data-plane effect without owning data — schema and ETL pipelines (see "ETL pipelines" below).

### Cluster resource model

Minimum vocabulary:

- **ClusterRoot** — the object-store prefix / control namespace for one deployment.
- **DesiredRevision** — git commit, `cluster.yaml` digest, and per-resource digests.
- **ResourceKind** — `Graph`, `Schema`, `Query`, `PolicyBundle`, `UiSpec`, `Binding`, `Alias`, `EmbeddingConfig`, **`Pipeline`** (ETL), and future cluster-scoped resources.
- **ResourceAddress** — normalized typed references between resources, such as `graph.knowledge`, `query.knowledge.find_experts`, `policy.base_rbac`, and `pipeline.github_sync`; illustrative YAML may use shorthand, but plan/state store the typed form.
- **ProviderAddress** — typed references to provider instances, such as `provider.storage.prod_graphs`, `provider.source.github_org`, and `provider.embedding.default`; provider addresses keep storage, external sources, and embedding providers from being inferred from ambiguous strings.
- **StateBackend** — where the JSON state ledger is stored: `cluster` (this deployment's own backend) or an external store (a separate bucket/account).
- **StateLock** — the cluster-scope lock acquired before plan/apply.
- **AppliedRevision** — the durable, locked record (the heart of state) of which desired revision is applied, with audit-record references, resource fingerprints, and graph/table version observations.
- **ResourceStatus** — `Pending | Planned | Applying | Applied | Drifted | Blocked | Error`, with typed conditions and observed actual state.
- **ApplyGroup** — the explicit atomicity unit. Default is one independent resource per group; cross-resource references force planner-derived groups, and user-declared groups may opt into larger atomicity only for resources the active backend protocol can fence or repair. Baseline JSON state supports small, explicit groups; larger all-or-nothing groups require a future cluster publisher or equivalent proof.

---

## State: backend, lock, and the config ↔ state diff

The CLI is the operator's window onto the gap between config and state.

The Terraform-aligned workflow is:

```text
cluster validate   # parse + schema-check desired config, no state mutation
cluster plan       # diff desired config against state, with optional refresh
cluster apply      # apply an accepted fresh plan and update state
cluster status     # read what state says is deployed now
cluster refresh    # update/import state observations from actual cluster state
```

`plan` is the central artifact. It records the desired revision, resource
digests for every referenced file, dependency edges between resources, observed
state fingerprints / graph manifest versions, proposed changes, and approval
gates. The human output below is a rendering of that structured plan, not the
only representation.

```
  $ omnigraph cluster plan
    config ./   →   diff against state   (backend: cluster · lock: acquired)

    ~ schema    knowledge    hard-drop Person.legacy_id              ⚠ prior versions reclaimed — needs approval
    + query     knowledge.find_experts                              (new stored query)
    - query     knowledge.orphan_pages                              (removed)
    ~ policy    base_rbac    grant invoke find_experts → members    (this is what EXPOSES the new query)
    + pipeline  saas_sync           notion → knowledge, hourly
    ~ ui        dashboards.overview  add panel "experts"
    + alias     experts
    ─────────────────────────────────────────────────────────────────────
    6 changes · 1 requires approval (hard schema drop on knowledge) · run `apply` to converge
```

<!-- Audit fix: enum narrowing is not implemented today; hard drops are the
current supported irreversible schema path, so the example must not teach a
future migration tier as if it already exists. -->
That output **is** the answer to the Sarah/Bob test: one diff, spanning every surface, attributed to a git commit and concrete resource digests, with data-impact peeked (axiom-6 schema seam), dependency fallout visible, observed state compared, and approval gates surfaced *before* anything moves. Drift (someone poked the live cluster out-of-band) shows up here too — `plan` reconciles state against the actual cluster and flags resources whose observed version no longer matches the ledger.

<!-- Audit fix: JSON state is the baseline. It is inspectable and Terraform-like,
but it remains a separate CAS step from graph manifest movement. -->
`apply` then: acquire **state lock** → execute the change set (ordered/grouped per the planner) → **CAS-update the JSON state ledger** with the new applied revision/status observations → release the lock. For config-only resources, content-addressed payload writes can happen before the state CAS because state is the publish point. For graph/schema moves, the graph manifest may move before the state CAS; a crash or CAS failure there leaves a loud repair/import condition and no success acknowledgement, not a silently successful atomic apply. A future cluster manifest publisher can tighten this gap, but the baseline protocol does not assume it.

---

## ETL pipelines (the second data-plane seam)

External data — from another database, an API, a file drop, a stream — is a first-class config asset, not glue code that lives nowhere.

A **Pipeline** is declared in config: a **source** (e.g. `notion`, `github`, `slack`, `gdrive`, `postgres`, `http`, `s3-files`, `kafka`), an optional **schedule/trigger**, and **one or more target graphs**, each with its own **mapping/transform** (external records → graph types & properties). A single feed can **fan out across graphs** — e.g. a GitHub sync that populates both the `engineering` graph and the people/teams in `knowledge`. It is reconciled like any resource — `apply` creates / updates / deletes / (re)schedules the pipeline *definition*. This is the canonical "company brain" move: the deployment's graphs are continuously assembled from the SaaS tools the org already uses.

The crucial boundary (axiom 6, axiom 13): the pipeline **definition** is control-plane and reconciled; the pipeline's **execution** — actually pulling rows and writing them — is a **data-plane effect** that produces ordinary `load`/`mutate` commits *outside* the reconcile loop. The reconciler converges the pipeline; the rows it ingests are never reconciled state (just as a cron *definition* is config but its output is not). This makes ETL the **second seam** where a definition triggers a data-plane effect — schema being the first (a migration conforms existing rows; ETL ingests new ones).

Consequences that fall out of the existing model:

- **`plan` previews the pipeline, not the data.** "pipeline `saas_sync`: notion → `knowledge`, hourly" is a definition diff; it does not scan the source (data-volume-independent), the same way schema `plan` previews impact only at the bounded, opt-in data peek.
- **Source credentials come from the `.env` file** (axiom 10): `token: ${NOTION_TOKEN}` — resolved from the gitignored `.env` file per deployment, never inline.
- **Reversibility gradient applies** (axiom 8): a pipeline that *appends* is reversible-ish; one configured to *overwrite* a target is a data-loss path and hits the irreversible-op gate.
- **Referential integrity is plan-time** (axiom 9): a pipeline whose `into:` names a graph/type the same revision removes is a fail-closed `plan` error.
- **Fan-out is statusful, not magically atomic.** A pipeline execution that writes to several graphs is a set of ordinary per-target graph writes unless the pipeline explicitly stages through a branch/merge protocol that can fence those targets. A failed run may therefore leave `engineering=Applied`, `knowledge=Error` (for example), and the pipeline run ledger must expose per-target status, commit ids, retryability, and idempotency keys. Control-plane `apply` only converges the definition/schedule; it never means every future data-plane target has ingested successfully.

---

## Config assets — the full set

Everything below is **shared cluster config** (in the folder, version-controlled, secret-free) unless marked per-operator. The rule of thumb: if two operators must agree on it, it's config; if it's how *you personally* reach or view the cluster, it's per-operator.

| Asset | In config? | Notes |
|---|---|---|
| **Graphs** (the set that exists) | ✅ config | the named graphs; their existence is cluster state |
| **Schema** (`.pg`, **one per graph**) | ✅ config | also encodes indexes (`@index`/`@unique`/vector), constraints, and search (`@embed`) — so indexes & search are reconciled *via* schema |
| **Stored queries** (`.gq`, **per graph**) | ✅ config | a `.gq` file declares **many** named queries; the registry declares which exist (name → file, key must match the `query <name>` symbol). **Target design:** exposure — who may list/invoke each — is a policy decision, not a registry flag. **Current compatibility bridge:** shipped `omnigraph.yaml` still has `queries.<name>.mcp.expose`, and the HTTP catalog is not Cedar-filtered per query yet. Aliases & bindings reference a query by name |
| **Policy bundles** (`.yaml`) | ✅ config | YAML (not Cedar files); **shared across graphs** via `applies_to: [cluster \| <graph refs>]` (many-to-many; fix 2026-06-08 unified the old `scope:`/`graphs:` split). Gates actions **and query exposure** (who may list/invoke each stored query) |
| **UI specs / dashboards** (`.yaml`) | ✅ config | first-class resources; a dashboard **reads from several graphs** (`graphs: [...]`) |
| **Bindings** | ✅ config | wiring between resources (query ⇄ UI surface) |
| **Aliases** | ✅ config* | CLI shortcut to a stored query: `{ command, query: <.gq file>, name: <symbol>, args, format }` — `query` is the **file**, `name` the **query symbol** in it. See note |
| **Embeddings config** | ✅ config | model + dimension + which fields embed; the **API key comes from the `.env` file** (`${…}`) |
| **ETL pipelines** | ✅ config | source → transform → **one or more target graphs**; source credentials come from the `.env` file |
| **Apply settings** | ✅ config | `apply.default_grain`, grouping/ordering hints |
| **State backend + lock** | ✅ config | where the ledger lives, whether to lock |
| **Secrets (`.env` file)** | ✅ ref'd by config; values **gitignored** | a separate `.env` of secret values, referenced as `${NAME}`; never committed (OmniGraph's standard env-file convention) |
| **Connection** (which cluster URI) | ❌ per-operator | how *you* reach the cluster |
| **Operator token** | ❌ per-operator (secret) | each operator's own credential to reach the cluster |
| **CLI prefs** (output format, table layout, active graph/branch selection) | ❌ per-operator | personal ergonomics, not shared truth |

\* **Aliases — the one with a split.** A shared alias that names a cluster resource (a stored query, a dashboard) is config — it's a vocabulary the whole team relies on, and it belongs in the spec (often it *is* just the stored-query catalog entry, since that already carries name + params + tool metadata). A *purely personal* shortcut (your own command abbreviations) stays in the per-operator layer. When in doubt: if it should survive `git clone` and be the same for Bob as for Sarah, it's config.

---

## The synthesis (beyond vanilla Terraform)

Embracing Terraform does not mean stopping at Terraform. Three extensions make this specifically right for OmniGraph and the agentic future:

1. **OmniGraph is its own data-aware provider, and `plan` can peek across the data boundary.** A Terraform provider CRUDs resources blind to your data. Here, the control-plane resource is the schema **definition** (declarative, reconciled); converging it *triggers* a data-plane **effect** — currently soft/hard drops, rewrites, and index creation, with future validated migrations such as enum narrowing or `String`→`enum` conversion once the planner grows that tier. The leverage is that `plan`, before applying the definition change, can *peek* at bounded data-plane consequence and report it — **"hard-dropping this property requires approval and will make prior versions unreachable after cleanup"** or, in the future, **"narrowing this enum will fail on 37 rows"** — which Terraform structurally cannot do. This is deliberate and bounded: a data peek makes that `plan` cost scale with data volume, so it is **opt-in / bounded** (sampled or skippable for large tables), and it never makes the control plane the owner of data. Schema and ETL pipelines are the **two** seams where the control plane reaches into the data plane; everywhere else `plan` is data-volume-independent.

2. **JSON state first, explicit partials, optional stronger fencing later.** Terraform apply is not transactional — partial applies are a real failure mode. Lance commits are per dataset, and today's OmniGraph manifest atomicity is graph-scoped: one graph commit flips the relevant sub-table versions together, protected by expected table versions and recovery sidecars. The first cluster-control backend should match Terraform's shape: a locked JSON state document plus append-only JSON status/approval/recovery records. That keeps Phase 1 inspectable and narrow. Cluster-level all-or-nothing apply is a later capability only if we add a **cluster manifest publisher** or Lance-backed state backend that fences graph *version pins*, query catalogs, policy bundles, UI specs, pipeline definitions, recovery sidecars, and state as one commit protocol. Until that exists, apply must surface partial convergence as `ResourceStatus`, not pretend it was atomic.

3. **Agent-as-controller fuses Terraform with Kubernetes.** Terraform contributes the as-code config (truth outside the system, recursion-terminating) and the locked state ledger. Kubernetes contributes *continuous* reconciliation (controllers watch, not apply-on-demand). The agent is both author and controller: it reads a config change, runs the data-aware plan, evaluates blast radius against the reversibility gradient, **auto-applies the reversible parts only when policy permits, and escalates irreversible / data-loss gates to a human approval artifact recorded in the audit ledger and referenced by state.**

> Terraform's as-code config + locked state × Kubernetes' continuous reconciliation × the agent as the controller that bridges them — on OmniGraph's data-aware, atomic substrate.

---

## Concrete shape (illustrative)

The config is **a set of files in one folder** (flat, Terraform-style — the extension carries the type):

```
 company-brain/
 ├── cluster.yaml              # the spec (graphs, policies, ui, bindings, aliases, pipelines, state, vars ref)
 ├── .env          # SECRET VALUES — gitignored, never committed
 ├── knowledge.pg · engineering.pg                                  # schemas (one per graph)        (.pg)
 ├── knowledge.gq · engineering.gq                                  # query files — each holds MANY queries  (.gq)
 ├── cluster_admin.policy.yaml · base_rbac.policy.yaml · knowledge_pii.policy.yaml   # shared policy bundles
 ├── overview.dashboard.yaml   # cross-graph UI spec                                     (.dashboard.yaml)
 └── notion_to_knowledge.map.yaml · github_to_engineering.map.yaml · github_to_people.map.yaml  # pipeline maps
```

Secrets live in a gitignored `.env` file (OmniGraph's standard env-file convention); the config references them as `${NAME}`:

```bash
# .env  —  secret values; gitignored; never committed. Referenced in cluster.yaml as ${NAME}.
NOTION_TOKEN=…
GITHUB_TOKEN=…
EMBEDDING_API_KEY=…
```

Resource relationships (so the wiring is unambiguous):

```
   cluster ──has many──► graph ──has one──► schema
                           └────has──► query file(s) (.gq) ──each declares MANY──► query <name> { … } symbols
   registry entry  key = the query <name> symbol  ──points to──► its .gq file   (queries: { <name>: { file } })
                   (registry says a query EXISTS; it carries NO expose flag)
   policy bundle ──applies to──► { cluster | one or MANY graphs }   (SHARED, many-to-many)
                 └──governs query EXPOSURE──► who may LIST / INVOKE each stored query  (no `expose:` in the registry)
   alias           (command, query = .gq FILE, name = symbol, args, format)  ──selects one query from that file
   binding         names a query by registry name (graph.queryName)  ──► resolved to (file, symbol)
   dashboard ──reads from──► one or MANY graphs
   pipeline  ──writes into──► one or MANY graphs
   secrets   ──live in──► a separate gitignored `.env` file; config uses ${NAME}
```

```yaml
# cluster.yaml — desired state of the whole deployment (config = source of truth for INTENT)
version: 1
metadata:
  name: company-brain

state:                                   # the authoritative ledger's backend (Terraform-style)
  backend: cluster                       #   "cluster" = this deployment's own store; or s3://… (a separate store)
  lock: true                             # acquire a state lock before plan/apply

env_file: ./.env                         # secret VALUES live in a gitignored .env file; referenced below as ${NAME}

apply:
  default_grain: resource                # references may force groups; explicit groups request more atomicity

graphs:                                  # the cluster's graphs — each is ONE schema + a set of named queries
  knowledge:                             # people · teams · docs · decisions · projects
    schema: ./knowledge.pg               # desired schema; reconciler runs (and plan previews) the migration
    queries:                             # the graph's stored (named) queries; KEY must match a `query <name>` in the file
      find_experts: { file: ./knowledge.gq }   # ─┐ `query find_experts` and `query related_docs`
      related_docs: { file: ./knowledge.gq }    # ─┘ both live in knowledge.gq.  Who may LIST/INVOKE → policy (not here)
  engineering:                           # repos · services · incidents · PRs
    schema: ./engineering.pg
    queries:
      service_owners: { file: ./engineering.gq }
      open_incidents: { file: ./engineering.gq }

policies:                                # policy BUNDLES (YAML) — SHARED across graphs (many-to-many).
                                         # Policy ALSO governs query EXPOSURE: who may list/invoke each stored query.
                                         # Fix (2026-06-08): unified the binding field on `applies_to:` (was a
                                         # `scope:` + `graphs:` split) — one field, takes `cluster` or graph refs;
                                         # bare graph names are shorthand for `graph.<id>` (see impl-spec typed addresses).
  cluster_admin:                         # cluster-scoped: graph_list, create/delete, management
    file: ./cluster_admin.policy.yaml
    applies_to: [cluster]
  base_rbac:                             # read/write + which roles may invoke which queries, across both graphs
    file: ./base_rbac.policy.yaml
    applies_to: [knowledge, engineering]
  knowledge_pii:                         # an extra bundle, only for knowledge
    file: ./knowledge_pii.policy.yaml
    applies_to: [knowledge]

pipelines:                               # ETL — ONE pipeline may write into SEVERAL graphs (definition only)
  saas_sync:                             # the "company brain" move: assemble graphs from the SaaS tools
    source: { kind: notion, token: ${NOTION_TOKEN} }    # secret via ${NAME}, never inline
    schedule: "0 * * * *"                # hourly; execution is a data-plane effect, not reconciled state
    into:                                # fans out across graphs
      - { graph: knowledge, map: ./notion_to_knowledge.map.yaml }
  github_sync:
    source: { kind: github, token: ${GITHUB_TOKEN} }
    schedule: "*/15 * * * *"
    into:
      - { graph: engineering, map: ./github_to_engineering.map.yaml }
      - { graph: knowledge,   map: ./github_to_people.map.yaml }   # same feed enriches a SECOND graph

embeddings:                              # semantic search over docs/decisions; key via the `.env` file
  model: gemini-embedding-2
  dimension: 3072
  api_key: ${EMBEDDING_API_KEY}

ui:                                      # dashboards read from SEVERAL graphs
  dashboards:
    overview:
      file: ./overview.dashboard.yaml
      graphs: [knowledge, engineering]   # cross-graph

aliases:                                 # CLI shortcuts.  ⚠ an alias's `query:` is the .gq FILE PATH;
                                         #    `name:` selects the query SYMBOL inside it (a file declares many).
  experts:   { command: query, graph: knowledge,   query: ./knowledge.gq,   name: find_experts,    args: [topic], format: table }
  incidents: { command: query, graph: engineering, query: ./engineering.gq, name: open_incidents,                 format: table }

bindings:                                # wiring between resources
  - query: knowledge.find_experts
    surface: ui.dashboards.overview
```

<!-- Audit fix: the sample shows the target policy-owned exposure model. The
current server still uses mcp.expose for catalog membership until per-query
policy filtering lands. -->
What this is *not*: it is **not** a graph, and it carries **no credentials** — only secret *references* (`${…}`). It is parsed by the engine (the base case), describes the desired cluster, and is the thing two operators diff and review.

The **state ledger** lives in the configured backend (the cluster, or a separate cloud store), versioned, CAS-updated, schema-versioned, locked during apply, agent-managed — the authoritative record of what is deployed. The baseline backend is JSON, so even cluster-hosted state is published through a state CAS and repaired explicitly if graph/resource movement happened first. A future cluster publisher can tighten that boundary, but it is not assumed by the high-level spec.

---

## Boundaries that hold (orthogonal correctness, not Terraform-bias)

1. **Secrets live in a `.env` file, never inline in config.** The committed config is what the cluster *is* (shared, reviewable, as code) and carries **no secret values** — only `${NAME}` references. The values (embedding API key, pipeline source credentials, per-deployment settings) live in a separate **`.env` file** — which is **gitignored and never committed**, and supplied per deployment. Separately, an *operator's own token* (how they personally reach the cluster) belongs to the per-operator connection layer, not the cluster config or its `.env` file.

2. **The reversibility gradient gates apply — including drift correction.** Dropping a graph, hard-dropping schema data, or an overwriting pipeline is irreversible data loss; a future validated enum narrowing is a compatibility-narrowing migration unless it also drops or coerces stored values; recoloring a dashboard is not. Unified config, unified plan — but **tiered gates inside apply**, keyed to physics, not to who operates it. The gate applies to **drift correction too**: converging actual→config can mean *dropping* something added out-of-band — a data-loss path that hits the same gate. A reconciler "just fixing drift" is never an exception.

3. **Agents are actors, not ambient authority.** The reconciler runs with a resolved actor or service account, subject to Cedar policy. If it applies on behalf of a human, the durable audit ledger carries both the controller actor and the approving human / approval artifact, and state references that ledger entry. Client-supplied actor identity is never trusted.

4. **Status is explicit when apply is not atomic.** A unified plan does not imply a unified commit. If an apply group partially converges, the cluster must expose `ResourceStatus` and typed conditions until reconciliation finishes or rolls back. Silent partial success is forbidden.

5. **State integrity is protected.** State is locked during apply and stored durably in its backend. The baseline state backend is JSON plus lock/CAS, so state update failures surface a repair/import condition before success is acknowledged. A lost ledger is recoverable (import/refresh from the self-describing cluster), but state is never treated as disposable.

---

## Relationship to current config

This is not green field, but it is also not today's `omnigraph.yaml`. The current file is a shared convenience for CLI and server startup: named graph targets, server defaults, query roots, aliases, embeddings model, auth env-file lookup, and `policy.file`. It is **not** the cluster's source of truth, it has no separate state ledger, and parts of it are intentionally per-operator.

This proposal:

- **splits** per-operator connection/credential/preference config from shared cluster config,
- **adds** `cluster.yaml` + a flat config folder as the full declarative cluster config (graphs, schemas, query catalog, policy bundles, UI specs, bindings, **aliases**, **embeddings**, **ETL pipelines**),
- **adds** the **JSON state ledger** (authoritative, locked, in a backend) and the `cluster plan`/`apply` loop,
- **adds** the reconciler (with OmniGraph as its own data-aware provider), while treating a cluster manifest publisher as a later option rather than the baseline,
- **lets an agent drive** plan/apply/continuous-reconcile.

The connection/credential/preference layer remains per operator: it points at a cluster, resolves that operator's identity, and holds personal ergonomics. The cluster config stays shared, secret-free, and reviewable; the state ledger stays authoritative and locked.

Implementation gate: the Terraform-style workflow must be testable in order.
`cluster validate` must catch bad config before any apply path exists;
read-only `cluster plan` must have deterministic structured-plan tests before
state mutation ships; and graph/schema-moving apply must have recovery tests for
the gap between graph/resource movement and JSON state publish. Otherwise the
control plane can look declarative while still hiding drift or partial success.

---

## Open questions

1. **Cluster state layout.** What exact JSON documents / object-store paths hold `AppliedRevision`, `ResourceStatus`, approval records, recovery records, sidecars, and resource content for query/policy/UI/pipeline specs? What evidence would justify a future Lance-backed state backend?
2. **State backend options.** Beyond "cluster" and "a separate bucket," what backends are first-class (a different account, a remote control service)? How is the backend itself bootstrapped and its lock implemented (object-store CAS vs an external lock service)?
3. **State import / refresh.** The exact actual-state scan that reconstructs a conservative `AppliedRevision` when the ledger is lost, and which fields become `Unknown`.
4. **Apply grain syntax.** Apply defaults to per-resource `ApplyGroup`; cross-resource references force planner-derived groups; user-declared groups opt into more atomicity. What's the YAML, and which combinations can the publisher actually fence?
5. **Pipeline runtime.** Where do pipelines *execute* (in the server? a worker? an external scheduler?), how are runs observed in `ResourceStatus`, and how does a failed/partial run reconcile vs. retry?
6. **Continuous reconciliation trigger.** Watch-and-converge (k8s-style) vs. apply-on-config-change. The agent-as-controller model leans toward continuous.
7. **Tenant partitioning (cloud).** A cluster may host multiple tenants; config/state is then tenant-partitioned, consistent with the reserved `GraphKey { tenant_id, graph_id }`. Tenant resolved from the token, never the config.
8. **Bootstrap — config, state, *and* authority.** How a cluster comes into existence from an initial config (`init` seeds; cluster owns; git mirrors for CI/DR), the first state write, and the chicken-and-egg of the very first apply (which needs an actor before any cluster exists to resolve policy against — so the bootstrap actor is necessarily out-of-band and privileged). Security-sensitive; needs an explicit story.
9. **Alias scoping.** Where exactly the shared/personal alias line falls, and whether shared aliases are just stored-query catalog entries.
10. **UI render and safety model.** Generic engine-side renderer vs. thin client, allowed components, query-binding validation, policy propagation, sandboxing, version compatibility.
11. **Cluster identity vs. `metadata.name`.** Is `metadata.name` a label or stable identity? If identity, renaming loses it — the stable-ID-across-rename gap already in `invariants.md`. Decide whether identity keys on `name` or on `ClusterRoot`, and reuse the existing known-gap framing.
12. **Resource dependency ordering.** Explicit dependency DAG (Terraform) vs. eventual convergence with retries (k8s). The most consequential unmade fork: it decides whether `plan` can promise an apply *order* before any data moves.
13. **Query exposure in policy (supersedes `mcp.expose`).** *Today* the stored-query registry carries a per-query `mcp.expose` flag and invocation is gated with the coarse `invoke_query` Cedar action — with **per-query authorization a documented gap** (the catalog isn't Cedar-filtered per query yet). This design **folds exposure fully into policy and drops the flag**: a stored query's visibility (catalog membership) and invocability are both policy decisions, so the catalog `GET /queries` returns each actor's policy-permitted set. The open work is the exact policy predicates for *list* vs *invoke* per query, and retiring `mcp.expose`.

---

## Prior art

- **Terraform** — declarative infra *as code*; config is desired truth, **state is an authoritative ledger in a backend**, **state locking** serializes applies, `plan` diffs config↔state, providers do the CRUD. The core model adopted here, taken literally.
- **Kubernetes** — one cluster store, many resource types under one API; controllers reconcile continuously; cluster-level RBAC. The continuous-reconciliation half of the synthesis.
- **dbt / Airflow / Dagster** — declarative, as-code data pipelines with lineage. Prior art for the **ETL-pipeline-as-config** asset (the second data-plane seam).
- **OmniGraph's own schema-apply** — already a faithful plan/apply/state/drift loop for the `schema` resource type, with `__schema_apply_lock__` as the lock seed; the reconciler this generalizes.
