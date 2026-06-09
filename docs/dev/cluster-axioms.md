# Cluster Control-Plane Axioms

**Type:** Standing design filter
**Status:** Draft / thinking-in-progress
**Date:** 2026-06-07
**Relationship:** the distilled axioms behind [cluster-config-specs.md](cluster-config-specs.md). The downstream implementation inventory and blast-radius assessment live in [cluster-config-implementation-spec.md](cluster-config-implementation-spec.md). The high-level spec is the argument; this is the checklist. Hold any config / control-plane / deployment proposal against these and cite them by number (e.g. "violates axiom 5").

This file is intentionally short and stable. The axioms are phrased so other
docs can reference "axiom 6" without churn. The motivating requirement comes
first; the core axioms are what the design is *based on*; the derived rules are
consequences that follow from them.

> **Revision 2026-06-07 — committed to the Terraform paradigm.** State is now an
> **authoritative, locked ledger in a backend** (no longer framed as a
> "mostly-rebuildable projection"); `plan` is a **config ↔ state diff**; and
> **ETL pipelines** join schema as config-defined resources that trigger
> data-plane effects. Secrets live in a gitignored **`.env`** file (`${NAME}`),
> and **query exposure is a policy decision** (no registry `expose:` flag).
> Axioms **2, 5, 6** revised; **12, 13, 14** added. The earlier
> "state is just a rebuildable projection; config is the *only* truth" framing is
> superseded — see axiom 5.
>
> **Revision 2026-06-08 — JSON state first.** The baseline state backend is now
> Terraform-style JSON documents plus backend lock/CAS, not Lance control-plane
> datasets. Lance remains a possible later backend only if row-level history or
> queryability justifies the extra machinery.

---

## Tenet 0 — the motivating requirement

**0. The Sarah/Bob test.** If one operator changes schema / queries / policies / UI / pipelines / aliases, another operator (or their agent) must learn *what the deployment is and what changed* from **one source, one history, one diff**. Fragmentation across separate mechanisms is the failure the whole design exists to eliminate. Every other axiom is in service of passing this test.

---

## Core axioms (what the design is based on)

**1. The cluster is the unit of declarative state.** Not the graph (policies, queries, UI, and pipelines cross-cut graphs; "which graphs exist" has no per-graph home), not the fleet (the next scope up — named and deferred). The cluster is what two operators collaborate over; a graph is a *resource within* it.

**2. Two sources of truth, for two different questions — config for *intent*, state for *deployed reality*.** The version-controlled **config** (a set of files in one folder) is the source of truth for what the cluster *should be*. The **state ledger** is the source of truth for what *is* currently deployed. Change flows one way only: you edit config and `apply` converges the cluster (**code → cluster**, never edit-the-cluster-and-call-it-intent). But "what exists right now" is read from **state**, not re-derived from the world on every command. `plan` is the diff between the two.

**3. Declarative, not imperative.** You describe the desired end state; the reconciler computes the steps. No runtime mutation API that makes the running system the place *intent* lives.

**4. As-code is structural, not stylistic (the recursion argument).** Code is the base case; modeling the definition *as data* (a meta-graph describing graphs) recurses with no base case. Config must live **outside** the running system so it is reviewable (PRs), reproducible (clone + apply), diffable as text, and editable by an agent — without the system having to describe itself.

<!-- Audit fix: JSON keeps the first backend Terraform-shaped and inspectable.
Lance datasets are future optimization, not the baseline state format. -->
**5. The Terraform model: config / state / reconcile — and state is an authoritative, locked ledger.** Config (as code) = desired truth. **State = the authoritative record of what has been applied**, held in a **backend** — the cluster's own object-store backend *or* a separate cloud store, the operator's choice, exactly like a Terraform backend. The baseline representation is JSON documents (`state.json`, status/approval/recovery JSON records) protected by backend lock/CAS, not Lance control-plane tables. State is **locked** during apply so two operators cannot converge concurrently. `validate` parses and schema-checks desired config; `plan` = `diff(config, state)` as a structured artifact with resource digests, dependency edges, state observations, proposed changes, blast radius, and approval gates; `apply` converges the cluster from an accepted fresh plan and **updates state**, and does not acknowledge success until state has recorded the result. A cluster-hosted JSON backend is still a separate state CAS step from graph Lance manifest moves; failures surface a repair/import condition instead of being described as cross-object all-or-nothing. A future Lance-backed state backend or cluster manifest publisher is optional and must earn its complexity by needing row-level queryability/history or tighter publish fencing. Because OmniGraph's running cluster is self-describing (manifests, commit logs), state is *reconstructable* by import/refresh if lost — its edge over opaque-cloud Terraform — but it is **treated as the source of truth for current reality, not casually regenerated**. The one slice that can never be reconstructed (who approved an irreversible apply) lives in the durable audit ledger; state references it (axiom 11).

**6. The control plane reconciles definition, not data — across two data-plane seams.** Definition — schema, policies, queries, UI, bindings, aliases, ETL **pipelines**, embeddings config, and the set of graphs — is reconciled. Data — rows, edges, vectors — is data-plane content, versioned by the commit DAG and produced by `load` / `mutate` and **pipeline execution**, sitting **outside** the reconcile loop. Exactly two definition kinds *trigger* a data-plane effect without owning data: **schema** (a migration conforms existing rows; `plan` previews its impact) and **ETL pipelines** (their execution ingests external data). The loop converges their *definitions*; the data they produce is never what it reconciles.

**7. Operated by agent (agent-as-controller).** An agent authors config changes and drives reconciliation as an authenticated actor, subject to policy and approval gates — no human state-management burden. This fuses Terraform's as-code config with Kubernetes' continuous reconciliation.

---

## Derived rules (consequences of the axioms)

**8. The reversibility gradient gates apply — including drift correction.** Irreversible / data-loss operations (drop a graph, hard-drop schema data, a pipeline that overwrites) and compatibility-narrowing migrations (for example, future validated enum narrowing) are gated; reversible ones (recolor a dashboard) are not. The gate is keyed to physics, not to who operates it, and a reconciler "just fixing drift" is never an exception.

**9. Atomicity and referential integrity are plan-time, not runtime.** `ApplyGroup` is the atomicity unit; cross-resource references *force* grouping (mandatory, not opt-in); references use typed resource/provider addresses (`graph.knowledge`, `query.knowledge.find_experts`, `provider.source.github_org`) so the planner can reject wrong-kind or missing targets before apply — bare names in a kind-fixed field are accepted shorthand and normalized to the typed address (fix 2026-06-08), while a kind-ambiguous value (e.g. `source: github`) is rejected; a reference to a missing or being-removed resource is a fail-closed `plan` error, not a deferred runtime failure.

**10. Secrets live in a `.env` file; connection/identity is per-operator.** The committed cluster config carries **no secret values** — only `${NAME}` references. The values (embedding API keys, pipeline **source credentials**, per-deployment settings) live in a separate **`.env` file** — which is gitignored and supplied per deployment, never committed. Separately, an operator's own connection (which cluster, which token) is the per-operator layer, distinct from both the shared config and its `.env` file.

**11. Approvals and audit live in a durable ledger, not inline in state.** State *references* the audit record by id. In the baseline, that ledger is append-only JSON records in the state backend; a future Lance table is an implementation option, not a requirement. This keeps the bulk of state reconstructable and keeps approval facts — "who authorized this irreversible apply" — where loss is impossible.

**12. State lives in a backend and is locked.** The state ledger is stored in a configurable backend — the cluster's own backend, or a separate cloud store — and `plan`/`apply` acquire a **state lock** first, so concurrent applies serialize instead of racing. (Generalizes the existing `__schema_apply_lock__` from schema scope to cluster scope.) The backend choice is part of the safety model: the first backend should be JSON plus object-store lock/CAS; any Lance-backed state backend needs its own RFC-level proof that the table semantics are worth the control-plane complexity.

**13. Pipelines are definition; their execution is data-plane.** An ETL pipeline (external source → transform → target graph) is **declared in config and reconciled like any resource**; *running* it produces ordinary data-plane writes (`load`/`mutate`) outside the reconcile loop. `apply` converges the pipeline's *definition* (create / update / delete / schedule); the rows it ingests are never reconciled. A fan-out run over several graphs is statusful rather than magically atomic: each target records commit id, status, retryability, and idempotency key unless the pipeline explicitly uses a branch/merge protocol that can fence the whole target set. Source credentials are secret references (axiom 10).

<!-- Audit fix: current shipped behavior still has mcp.expose and coarse
invoke_query. This axiom is the target control-plane rule, not a statement
about today's server catalog. -->
**14. Exposure is a policy decision, not a config flag.** Target design: which stored queries (and the tools/dashboards built on them) an actor may **list or invoke** is decided by the policy layer (Cedar: `invoke_query` + catalog visibility), not by a per-query `expose:` boolean. The registry only says a query *exists* (name → file); **policy says who may see and run it**, so the MCP catalog (`GET /queries`) becomes each actor's policy-permitted set. This supersedes the engine's current `mcp.expose` flag only after per-query `invoke_query` scope and Cedar-filtered catalog listing land; until then, proposals must state the compatibility bridge to today's `mcp.expose` + coarse invocation gate.

---

## The one-line compression

**One cluster; config (a folder of files) is desired truth and a locked state ledger in a backend is deployed truth; `plan` diffs them, `apply` converges the cluster and updates state, an agent drives the loop — reconciling the cluster's *definition* (schema, policies, queries, UI, pipelines, …) and never its data — so any operator sees the whole system and its history from one place.**

---

## How to use this file

- **Reviewing a proposal:** walk axioms 0–14; any conflict is the burden of the proposer to justify. The most common tensions:
  - Treating the *running system* as the source of truth for **intent** → axioms 2, 4 (intent lives in config).
  - Treating state as a throwaway derivation rather than an authoritative, locked, backend-held ledger → axiom 5, 12.
  - A runtime config-mutation API instead of declarative apply → axiom 3.
  - "State" meaning a per-operator selection rather than the applied-cluster ledger → axiom 5.
  - The control plane reconciling (or owning) data — including treating pipeline *rows* as reconciled state → axiom 6, 13.
  - Treating fan-out pipeline execution as atomic without a branch/merge protocol or per-target status ledger → axiom 13.
  - Per-graph or per-server scoping of cluster-level definition → axiom 1.
  - Bare string references that force the planner to guess whether `knowledge` means a graph, query, provider, or path → axiom 9.
  - A secret value (token, embedding key, pipeline source credential) inline in config instead of in the gitignored `.env` file → axiom 10.
  - A per-query `expose:`/visibility flag in target-state cluster config instead of governing list/invoke in policy; or failing to account for today's `mcp.expose` compatibility bridge → axiom 14.
  - Shipping `apply` before hermetic `validate` + read-only `plan` tests, or shipping graph/schema-moving apply before recovery tests for the graph/resource-moved-before-cluster-publish gap → axiom 5 and axiom 12.
- **Citing:** reference axioms by number in PRs and review comments so the rationale is stable across renames and refactors.
