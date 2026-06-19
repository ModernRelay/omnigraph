# RFC: Server Boots from Cluster State — Phase 5 of the Cluster Control Plane

**Status:** Landed (5A policy bindings #175; 5B/5C the `--cluster` boot mode — one PR)
**Implementation deviations:** (1) cluster mode reuses `ServerConfigMode::Multi` (a new settings *source*, not a new enum variant; `config_path` carries the cluster dir). (2) Stored queries load via `QueryRegistry::from_specs` from verified blob *content*, not blob paths. (3) More than one policy bundle binding a single scope is a boot error (the serving pipeline holds one bundle per graph + one server-level; stacking is a later slice). (4) `GET /graphs` keeps its closed-by-default contract — without a cluster-bound bundle there is no server-level Cedar engine, so enumeration refuses. (5) Graph-attributed startup failures quarantine that graph by default; operators can restore all-or-nothing boot with `--require-all-graphs` / `OMNIGRAPH_REQUIRE_ALL_GRAPHS=1`.
**Date:** 2026-06-10
**Builds on:** Phase 4 complete ([rfc-004-cluster-graph-schema-apply.md](rfc-004-cluster-graph-schema-apply.md), Landed): `cluster apply` converges graphs, schemas, stored queries, and policies into the cluster catalog. Normative context: [cluster-config-specs.md](cluster-config-specs.md) (the migration model's "window 2"), [cluster-axioms.md](cluster-axioms.md) (axiom 15), [cluster-config-implementation-spec.md](cluster-config-implementation-spec.md) (Phase 5 rollout, Compatibility Stance #7–#9, exit criterion 7).
**Target release:** unversioned (phased — see Sequencing).

## Summary

Give `omnigraph-server` a second boot source: `omnigraph-server --cluster <dir>` reads its graph set, stored queries, and Cedar policies from the **cluster catalog** — `state.json`'s applied revision plus the content-addressed blobs under `__cluster/resources/` — instead of `omnigraph.yaml`. This is the moment "applied" finally means "serving": the standing caveat in every cluster doc since Stage 3A ("the server still boots from `omnigraph.yaml`") retires for deployments that flip the switch.

Three commitments:

1. **An exclusive mode switch, never a merge** (axiom 15, Compatibility Stance #7). `--cluster <dir>` is mutually exclusive with the positional URI, `--target`, and `--config`. In cluster mode, `omnigraph.yaml` is not read at all — not for graphs, not for queries, not for policies. There is no precedence, no key-level aliasing, no fallback read. A deployment serves from one source.
2. **The server serves the *applied* revision, not the desired config.** What's live is what `cluster apply` converged: graph roots recorded in state, query/policy content at the *applied* digests from the content-addressed catalog. Un-applied config drift never leaks into serving — the serving surface and the ledger cannot disagree (axiom 5 extended to the data path).
3. **The state ledger becomes serving-sufficient.** Today one fact needed to serve is missing from state: a policy's `applies_to` bindings live only in `cluster.yaml`. A prerequisite slice (5A) records binding metadata into the applied revision at apply time, so a booting server reads state + blobs and nothing else. Without this, "boot from state" would silently become "boot from state *and* config" — the merged read axiom 15 forbids.

## Motivation

Phase 4 closed the convergence loop but left it inert: an operator can declare, plan, approve, and apply an entire deployment, and the running server ignores all of it. The Sarah/Bob test still fails at the last step — Sarah's applied change is visible in `cluster status` but Bob's clients hit a server still wired to a hand-maintained `omnigraph.yaml`. Phase 5 makes the catalog the serving source, which is also the precondition for Phase 6 (policy-owned query exposure must filter a catalog the server actually reads).

## Non-Goals

- **Runtime reconciliation / hot reload.** Cluster-mode boot is static, exactly like today's boot: the server reads the applied revision once at startup; picking up a newer applied state means restarting the process. The registry's runtime-mutation seam (the test-only `insert()` + mutate `Mutex` in `registry.rs`) stays future-proofing for a later watch-and-reload slice, not this RFC.
- **Policy-owned query exposure** (Phase 6) — but this RFC defines the bridge it sunsets (§D5).
- **Remote cluster roots.** `--cluster <dir>` is a local directory in this phase, same as the `cluster` CLI commands; S3-hosted cluster roots arrive with external state backends.
- **Retiring `omnigraph.yaml` server boot.** It remains a fully supported mode indefinitely (Compatibility Stance #8: the file's job shrinks; the *server-role* keys become inert only for deployments that switch).
- **New management endpoints** (`/cluster/status` etc.) — noted as future work; this RFC changes the boot source, not the HTTP surface (beyond OpenAPI regen if anything shifts).

## Background (verified against main)

- **Server boot today** (`omnigraph-server/src/main.rs`, `lib.rs:891-1029`): `load_server_settings` applies a four-rule mode inference (positional URI / `--target` / `server.graph` → Single; `--config` + `graphs:` → Multi), builds `ServerConfigMode::{Single,Multi}` with per-graph `GraphStartupConfig {graph_id, uri, policy_file, queries}`, loads `QueryRegistry` from `.gq` files at settings time (identity-checked), type-checks queries at engine open (`validate_and_attach`), loads Cedar via `PolicyEngine::load_graph`/`load_server`, installs it with `with_policy`, and assembles `GraphRegistry::from_handles` (startup-only; lock-free `ArcSwap` reads). Bind address and bearer tokens come from flags/env, not from graph config. No reload machinery exists.
- **The catalog today** (`omnigraph-cluster`): `state.json` records `applied_revision.resources` (address → digest) for `graph.*`, `schema.*`, `query.<graph>.<name>`, `policy.<name>`, plus statuses, observations (incl. tombstones), approval and recovery records. Query/policy *content* lives content-addressed at `__cluster/resources/query/<graph>/<name>/<digest>.gq` and `policy/<name>/<digest>.yaml`. Graph roots are derived: `<dir>/graphs/<id>.omni`.
- **The gap**: state records a policy's *digest* only; `applies_to` (cluster vs graph refs) lives in `cluster.yaml`. Queries are fine — their graph binding is encoded in the address itself.

## Design

### D1. The mode switch

New server flag: `omnigraph-server --cluster <dir>` (the directory containing `cluster.yaml`, `__cluster/`, and `graphs/`). Mutually exclusive — a hard startup error, not a precedence rule — with the positional URI, `--target`, and `--config`. `--bind`, `--unauthenticated`, and the bearer-token env vars keep working identically: listen address and credentials are **process-operational facts**, not cluster facts (they differ per replica/host and never belonged to the shared catalog; if a `serve:` section ever joins `cluster.yaml`, that's a separate proposal).

Mode inference gains rule 0: `--cluster <dir>` → **Cluster mode**, which is always multi-graph routing (`/graphs/{graph_id}/...`), even for a single declared graph. No flat-route legacy surface in cluster mode — it's a new mode with no compatibility debt to carry.

### D2. What the server reads (the applied revision, and only it)

`load_server_settings` grows a cluster branch that reads, in order:

1. `__cluster/state.json` — **missing state is a boot error** ("run `cluster import` + `cluster apply` first"). Invalid or unattributable recovery sidecars under `__cluster/recoveries/` are also a boot error: a server must not start if it cannot prove the blast radius. Valid graph-attributed sidecars quarantine that graph by default and are logged as `cluster_recovery_pending`; `--require-all-graphs` promotes them back to a boot error.
2. **Graph set** = state's `graph.<id>` resources (tombstoned graphs are absent by construction). Each graph's URI is the derived root `<dir>/graphs/<id>.omni`. A recorded graph whose root does not open quarantines that graph by default; `--require-all-graphs` restores the original fail-fast posture.
3. **Stored queries** = state's `query.<graph>.<name>` entries, content loaded from the catalog blob at the recorded digest. Blob-missing or digest-mismatched is a boot error (the catalog verification semantics from Stage 3B, applied at boot). Queries type-check at engine open exactly as today (`validate_and_attach` — unchanged).
4. **Policies** = state's `policy.<name>` entries, content from catalog blobs, bindings from the applied metadata of D3: bundles bound to `cluster` load as the server-level Cedar engine (`PolicyEngine::load_server`); bundles bound to graphs load per-graph (`PolicyEngine::load_graph`) and install via `with_policy` — the existing two-gate structure, unchanged.
5. `cluster.yaml` is parsed **only** to validate that the directory is a cluster root (and for nothing else — explicitly not for resource content; a divergence between desired config and applied state is *served as applied*, visible via `cluster plan`).

Everything downstream of settings construction — `GraphStartupConfig`, parallel engine opens, `GraphRegistry::from_handles`, routing middleware, auth, workload admission, OpenAPI — is reused as-is. Cluster mode is a new *source* for the same boot pipeline, not a new pipeline.

### D3. Prerequisite: serving metadata in the applied revision (slice 5A)

State's `StateResource` records only a digest. To make the ledger serving-sufficient, `cluster apply` (and the sweep's roll-forwards) additionally record **binding metadata** for policy resources at apply time:

```json
"applied_revision": {
  "resources": {
    "policy.base_rbac": {
      "digest": "<sha256>",
      "applies_to": ["cluster", "graph.knowledge"]
    }
  }
}
```

- Additive and optional (`#[serde(default)]`) — existing state files parse unchanged; a policy entry without `applies_to` (applied before 5A) is a **boot error in cluster mode** with the remedy "re-run `cluster apply`" (one apply rewrites the metadata; the digest needn't change — the metadata write is part of the state mutation, not the blob).
- `applies_to` is normalized to typed addresses (`cluster` | `graph.<id>`) at apply time, mirroring the validator's normalization.
- Queries need no equivalent: the address (`query.<graph>.<name>`) already carries the binding, and the registry key/symbol invariant is enforced at apply (validate) time.
- This is deliberately *applied* metadata, not config mirroring: if `cluster.yaml` changes a binding, the server keeps serving the old binding until `cluster apply` converges it — the same contract as every other resource.

### D4. Readiness and failure posture

Cluster-global failures are fail-fast, matching the server's existing stance (bad policy YAML refuses boot). Graph-local failures quarantine the affected graph by default so a single bad graph cannot crash-loop an otherwise healthy cluster. Operators who prefer the original all-or-nothing contract pass `--require-all-graphs` or set `OMNIGRAPH_REQUIRE_ALL_GRAPHS=1`, which promotes every graph-local quarantine/open/settings failure to a boot error.

| Condition | Behavior |
|---|---|
| `state.json` missing / unparseable / unsupported version | boot error |
| invalid/unreadable/unattributable recovery sidecars | boot error (run any state-mutating cluster command to sweep or inspect) |
| valid graph-attributed recovery sidecars | quarantine that graph; strict mode boot error |
| recorded graph root missing or unopenable | quarantine that graph; strict mode boot error |
| query/policy blob missing or digest-mismatched | boot error (run `cluster refresh` + `apply` to self-heal, then restart) |
| policy entry without `applies_to` metadata | boot error ("re-run cluster apply", D3) |
| stored query fails parse/type-check against the live schema | quarantine that graph; strict mode boot error |
| embedding provider configuration for one graph cannot resolve | quarantine that graph; strict mode boot error |
| every applied graph is quarantined or fails startup | boot error (`cluster_no_healthy_graphs`) |
| state lock held | **not** an error — boot takes no lock; it reads a point-in-time snapshot of an immutable-once-written state file (the CAS discipline means a concurrent apply produces a *new* file atomically; the server reads whichever was current at open) |

### D5. MCP presentation (`@mcp(expose, tool_name)`) in cluster mode

**Superseded (v0.8.0).** The old "bridge" — cluster mode force-lists every stored
query because the cluster registry had no `expose` flag — is gone. Per-query MCP
presentation is now carried in the `.gq` **source** via the `@mcp(expose: …,
tool_name: …)` annotation (re-parsed at boot from the content-addressed query
blob), so cluster mode honors it the same as any other deployment; the boot path
no longer hardcodes `expose: true`. Default with no `@mcp`: exposed, tool name =
query name.

Crucially this splits two axes the original bridge conflated: **`expose` is
presentation** (does the query appear on the agent tool surface — `tools/list` /
`GET /queries`), carried in source; **authorization** (who may invoke a query)
stays the coarse `invoke_query` Cedar action, with a per-query refinement the
durable future direction. An `expose: false` query is still HTTP/service-callable
by name for any caller holding `invoke_query`. See
[../user/queries/index.md](../user/queries/index.md#annotations) and
[../user/operations/mcp.md](../user/operations/mcp.md).

### D6. Migration path (exit criterion 7)

For an operator running multi-graph from `omnigraph.yaml`:

1. Author `cluster.yaml` declaring the same graphs/queries/policies; place existing graph roots under `<dir>/graphs/<id>.omni` (or start fresh).
2. `cluster import` (observes live graphs) → `cluster plan` → `cluster apply` (publishes queries/policies into the catalog; with 5A, records policy bindings).
3. Restart the server with `--cluster <dir>` instead of `--config omnigraph.yaml`.
4. `omnigraph.yaml`'s `graphs:`/`serve:`/`queries:`/`policy:` keys are now inert for this deployment; the file remains the CLI's per-operator config.

Rollback is the same switch in reverse — nothing in cluster mode mutates `omnigraph.yaml` or the graphs in a way the yaml mode can't serve.

### D7. Invariants and axioms check

- *Axiom 15 / Stance #7*: exclusive flag, hard mutual-exclusion error, zero `omnigraph.yaml` reads in cluster mode — no fact has two readers.
- *Axiom 5*: the server serves deployed reality (applied digests), never desired intent; D3 keeps the ledger the single serving source.
- *Axiom 12*: boot reads without the lock but relies on the atomic-replace write discipline; it never writes state.
- *Axiom 14 / Stance #9*: the expose-all bridge is named, scoped to cluster mode, and carries its Phase 6 sunset.
- *Loud failures (deny-list)*: every degraded condition is either a typed cluster-global boot error with a remedy or an explicit graph quarantine logged at startup; no silent fallback to the yaml. `--require-all-graphs` is the opt-in all-or-nothing mode for operators who treat any degraded graph as fatal.
- *Respect the boundaries*: `omnigraph-cluster` stays free of HTTP; the server reads the catalog through a small read-only loader (either a `pub` read surface on `omnigraph-cluster` or a thin module in the server consuming the documented file formats — implementation picks the one that keeps `omnigraph-cluster` dependency-light; the state/blob formats are already a documented contract).

## Sequencing

| Slice | Scope | Gate |
|---|---|---|
| **5A: serving metadata in state** | `applies_to` recorded on policy resources at apply + sweep roll-forward; additive state schema; `status`/plan surfacing | In-crate tests: metadata written/rolled-forward; old state parses; re-apply backfills |
| **5B: `--cluster` boot mode** | Flag + mode inference rule 0; catalog loader (state → `GraphStartupConfig`s + registries + policy engines); readiness table; OpenAPI regen if surface shifts | Server tests: boot from a converged fixture dir, serve `/graphs/{id}/query` + stored queries + Cedar gates; D4 cluster-global rows refuse boot; graph-local rows quarantine by default and refuse under `--require-all-graphs`; e2e: `cluster apply` then serve — "applied means serving" |
| **5C: docs + caveat retirement** | `cluster-config.md` mode-switch section; `server.md`/`deployment.md`; retire the "not serving" caveats for cluster-mode deployments; migration guide (D6) | `check-agents-md.sh`; doc accuracy review |

## Exit-criteria coverage

Answers implementation-spec exit criterion 7 (server startup + migration path) in full; touches 1 (state schema gains policy binding metadata — additive). Criteria 8 (per-query policy) and 9 (pipelines — descoped to a separate project) remain.

## Open Questions

1. **Loader home**: `pub` read-only API on `omnigraph-cluster` (server gains the dependency) vs a server-side reader of the documented formats. Leaning `omnigraph-cluster` API — one parser for the state schema beats two drifting ones; the crate stays HTTP-free either way.
2. **Boot-time blob re-hash**: D4 requires digest verification at boot; for large catalogs a stat-only fast path with full hashes behind a flag may matter later. Start with full verification (catalogs are small).
3. **`GET /graphs` enrichment**: cluster mode could expose applied digests/revision in the enumeration — deferred until a consumer exists.
4. **Watch-and-reload**: the natural follow-up once cluster mode exists; the registry's mutation seam is ready, but reload semantics (drain? cutover?) deserve their own design.

## References

- [rfc-004-cluster-graph-schema-apply.md](rfc-004-cluster-graph-schema-apply.md) — the convergence machinery this serves
- [cluster-config-specs.md](cluster-config-specs.md) §Migration model — window 2 is this RFC
- [cluster-axioms.md](cluster-axioms.md) — axioms 5, 12, 14, 15
- [cluster-config-implementation-spec.md](cluster-config-implementation-spec.md) — Phase 5 rollout, Compatibility Stance #7–#9, blast-radius rows for the server registry
- `crates/omnigraph-server/src/lib.rs` (`load_server_settings`, `ServerConfigMode`, `GraphRegistry`) — the boot pipeline this extends without forking
