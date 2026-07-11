# RFC: Cluster Graph & Schema Apply — Phase 4 of the Cluster Control Plane

**Status:** Landed (4A #170, 4B #171, 4C — all shipped)
**Implementation deviations:** (1) D3 row 8 retires the stale delete sidecar and lets the still-approved delete re-propose and retry, instead of a pending-block — prefix removal is idempotent, so the retry is the repair. (2) The approver/actor flag is the CLI's existing global `--as`, not a dedicated `--actor`/`--by`. (3) Consumed approval artifacts are rewritten with `consumed_at` rather than moved into state — the file and the ledger record both survive independently (axiom 11).
**Date:** 2026-06-10
**Builds on:** cluster Stages 1–3B (shipped: validate/plan/status/refresh/import/force-unlock, config-only `cluster apply` with content-addressed catalog publish, catalog payload verification, failpoint-proven crash/CAS recovery for the apply protocol). Normative context: [cluster-config-specs.md](cluster-config-specs.md), [cluster-axioms.md](cluster-axioms.md), [cluster-config-implementation-spec.md](cluster-config-implementation-spec.md).
**Target release:** unversioned (phased — see Sequencing); no cluster functionality is in a tagged release yet.

## Summary

Extend `cluster apply` from config-only resources (stored queries, policy bundles) to **graph-moving resources**: graph create, cluster-driven schema apply, and graph delete. This is the nested-publish territory the implementation spec flags as its highest-risk decision: a graph's Lance manifest can move (via the engine's own atomic publish) *before* the cluster's JSON state CAS lands, and a crash in that window must never be silent, never acknowledged as success, and never repaired by guessing.

Three design commitments make the phase tractable:

1. **Cluster recovery is roll-forward-only.** The engine's recovery sidecars (`__recovery/{ulid}.json`, the open-time sweep in `db/manifest/recovery.rs`) already make every graph-level operation atomic *within the graph* — a schema apply either fully published or fully recovers at the next open. The cluster therefore never rolls a graph back. Cluster sidecars exist to **classify and record**: after a crash, the sweep observes the live graph, decides "moved / didn't move / moved unexpectedly," and either rolls the *cluster state* forward to match observable reality (axiom 5) or surfaces a loud pending-repair condition. The cluster holds no second transaction log and no rollback hammer — that would duplicate substrate behavior the engine already owns (invariant: respect the substrate).
2. **Irreversible operations require a digest-bound approval artifact.** Graph delete and `allow_data_loss` schema applies consume an explicit `__cluster/approvals/{ulid}.json` record bound to the exact change digests, written by a new `cluster approve` command and retired into the state ledger's `approval_records` (the durable audit reference of axiom 11).
3. **The operator identity becomes explicit.** `cluster apply` gains an actor, threaded to the engine's `apply_schema_as` so Cedar enforcement and commit attribution work unchanged. The cluster control plane adds no policy engine of its own (transport/auth stay at the boundary).

## Motivation

After Stage 3B, the control plane converges everything *except* the resources that define the data plane. The Sarah/Bob test is half-passed: Bob can see that Sarah changed a schema (`plan` shows the deferred change; `refresh` shows drift), but the system cannot act on it — Sarah still applies schemas with the per-graph tool and the cluster ledger trails reality. Graph creation is worse: a new graph in `cluster.yaml` blocks every dependent query and policy with `dependency_missing` until someone runs `omnigraph init` by hand at exactly the derived path. Phase 4 closes the loop: desired config in, converged deployment out, for the full resource vocabulary of Stage 1.

The implementation spec's hard gate for this phase — failpoint recovery tests proving the movement-before-state-publish gap — was deliberately front-loaded: Stage 3B shipped the failpoint infrastructure and the apply-side crash/CAS tests. What remains is the design this RFC supplies: the sidecar schema, the recovery decision matrix, the approval artifact, and the ordering rules.

## Non-Goals

- **Server boot from cluster state** (Phase 5) — applied graphs/schemas still serve nothing; the server boots from `omnigraph.yaml` until the explicit per-deployment mode switch (axiom 15).
- **Policy-owned query exposure / `mcp.expose` retirement** (Phase 6).
- **Pipelines, embeddings, UI, aliases, bindings, providers, `env_file`** (Phase 7 and reserved fields).
- **External or Lance-backed state backends**; the local JSON backend + lock/CAS remains the substrate.
- **A cluster manifest publisher.** Deferred, per the spec: it becomes interesting only if the sidecar + repair path proves too weak for the accepted safety contract. Nothing in this design forecloses it.
- **Multi-graph atomic apply groups.** Cross-graph convergence remains statusful-partial per resource; one graph's failure never pretends to fence another's success.
- **Graph rename.** Stable-identity-across-rename is an open known gap at the schema level already; graph rename compounds it and is explicitly out of scope (see Open Questions).

## Background

What Phase 4 builds on (all shipped):

- **The engine's recovery discipline.** Writers that can advance Lance HEAD before manifest publish write `__recovery/{ulid}.json` sidecars carrying per-table pins (`expected_version`, `post_commit_pin`); `Omnigraph::open` in read-write mode classifies every pinned table (`NoMovement` / `RolledPastExpected` / `UnexpectedAtP1` / `UnexpectedMultistep` / `InvariantViolation`) and decides all-or-nothing: roll forward via one manifest publish, or roll back via `Dataset::restore`. Every completed action records an audit row with the interrupted writer's actor in `recovery_for_actor`. Schema-v3 Mutation/Load and schema-v4 BranchMerge roll-forward publish the sidecar's pre-minted lineage and therefore preserve that writer's commit id and actor; v4 additionally proves each multi-commit data effect from its ordered pre-minted Lance transaction chain. Rollback and legacy recovery commits use `omnigraph:recovery`. The cluster inherits the *vocabulary* of this design but not its mechanics — see the roll-forward-only argument below.
- **The engine's schema-apply surface.** `apply_schema_as(desired_source, SchemaApplyOptions { allow_data_loss }, actor)` returns `SchemaApplyResult { supported, applied, manifest_version, steps }`; `preview_schema_apply_with_options` returns the migration plan plus desired catalog without applying; the `__schema_apply_lock__` branch serializes schema applies graph-wide and refuses to run while user branches exist. Policy enforcement (`enforce(SchemaApply, TargetBranch("main"), actor)`) happens before the lock.
- **Graph init.** `Omnigraph::init(uri, schema_source)` with a strict preflight (errors if schema artifacts exist) and an atomic `_schema.pg` claim. A documented gap: a failed init does not clean up Lance datasets or `__manifest/` it already created.
- **No engine graph-delete primitive.** Deleting a graph today means removing its object-store prefix. This RFC works with that fact rather than waiting on a primitive.
- **Cluster state and observations.** `state.json` (locked, CAS-checked, atomically replaced) already records per-resource digests, statuses, `observations["graph.<id>"]` with `manifest_version` and live schema digest, plus empty `approval_records` / `recovery_records` placeholders reserved for this phase.
- **Stage 3A/3B apply mechanics.** Dispositions (`applied`/`derived`/`deferred`/`blocked`), content-addressed catalog publish before the state CAS, persisted-statuses contract on write failure, idempotent re-apply, payload verification with the drift + self-heal loop, and failpoints `cluster_apply.after_payload_phase` / `cluster_apply.before_state_write`.

## Design

### D1. Resource semantics: which dispositions change

The Stage 3A classifier gains executable rows. Everything else (catalog resources, `derived` composites, blocked dependents) is unchanged:

| Change | Stage 3A disposition | Phase 4 disposition |
|---|---|---|
| `graph.<id>` Create | Deferred | **Applied** (4A): `Omnigraph::init` at the derived root |
| `schema.<id>` Create | Deferred | **Applied with the graph create** (the init carries the schema) |
| `schema.<id>` Update | Deferred | **Applied** (4B): `apply_schema_as` against the live graph |
| `graph.<id>` / `schema.<id>` Delete | Deferred | **Applied behind approval** (4C): prefix removal |
| `query.*`/`policy.*` blocked on the above | Blocked | Unblocked in the same apply once the dependency lands (ordering, D5) |

Graph roots remain **derived**: `ClusterRoot/graphs/<id>.omni` (high-risk decision #2 dispositioned: external graph roots are a separate, explicit future feature, not this phase).

### D2. Cluster recovery sidecar (exit criterion 2, first half)

Written under the state lock **before** any engine call that can move or create a graph manifest; deleted only **after** the cluster state CAS that records the outcome lands.

```json
{
  "schema_version": 1,
  "operation_id": "<ulid>",
  "started_at": "<rfc3339>",
  "actor": "<id or null>",
  "kind": "graph_create | schema_apply | graph_delete",
  "graph_id": "<id>",
  "graph_uri": "<derived root>",
  "observed_manifest_version": 7,
  "expected_manifest_version": null,
  "desired_schema_digest": "<sha256 of the schema source being applied>",
  "state_cas_base": "sha256:<state.json digest at sidecar write>"
}
```

Path: `__cluster/recoveries/{operation_id}.json`, atomic write (temp + rename, the `write_state` discipline). Notes:

- `observed_manifest_version` is the live graph's main-branch manifest version read at sidecar-write time (`null` for `graph_create` — no graph yet). This is the fencing value: apply refuses to proceed if it differs from the version recorded in `observations["graph.<id>"]` at plan time *and* re-observed under the lock (the same recompute-under-lock posture as Stage 3A's diff).
- `expected_manifest_version` starts `null` and is **rewritten into the sidecar immediately after the engine call returns** with `SchemaApplyResult.manifest_version` (or the post-init observation). A crash before that rewrite leaves `null`, which the sweep treats as "engine call outcome unknown — classify by observation only." For `graph_delete` the field is **always `null`** — prefix removal produces no new manifest version, so there is no rewrite step for that kind; delete sidecars are classified purely by root presence + state tombstone (D3 rows 7/7b/8).
- `state_cas_base` is **recorded for audit and diagnostics only — the sweep decision logic never consults it.** The sweep re-reads `state.json` under the lock and performs ordinary CAS-checked writes, so an independent state mutation between sidecar write and sweep is handled by the CAS like any other concurrent write, not by this field. Its value is forensic: a recovery audit entry can show which state revision the interrupted operation departed from.
- One sidecar per graph-moving resource operation. Apply processes graph-moving operations strictly sequentially (D5), so at most one sidecar is pending per apply run *per graph*, and the sweep processes sidecars in ULID order.

### D3. Recovery decision matrix — roll-forward-only (exit criterion 2, second half)

**Why no rollback.** The engine's sidecars already guarantee that a schema apply is atomic within the graph: by the time any cluster-visible manifest version moved, the engine either fully published or will recover all-or-nothing at its next read-write open. A cluster-level rollback would mean un-publishing a successfully published graph commit — rewriting substrate history the cluster does not own, duplicating the engine's transaction discipline (deny-list: custom transaction manager; state that drifts from what it can be derived from). The cluster's job after a crash is therefore *epistemic*, not transactional: observe what the graph actually is, and converge the ledger to it or refuse loudly.

**Sweep trigger.** The sweep runs at the start of every state-mutating cluster command (`apply`, `refresh`, `import`), under the state lock, before the command's own work — mirroring the engine's open-time sweep gating (read-only `status`/`plan`/`validate` report pending sidecars as a warning, `cluster_recovery_pending`, but do not act).

| # | Sidecar kind | Observation | Decision |
|---|---|---|---|
| 1 | any | Graph at `observed_manifest_version` (nothing moved) | Engine call never landed. Delete sidecar; the command's own plan/apply re-proposes the change. |
| 2 | `graph_create` / `schema_apply` | Graph at `expected_manifest_version`; state already records the outcome | Crash fell between state CAS and sidecar delete. Delete sidecar; done. |
| 3 | `schema_apply` | Graph at `expected_manifest_version` (or, when `expected` is `null`, live schema digest == `desired_schema_digest`); state stale | **Roll the cluster state forward**: record the live schema digest, recompute the graph composite, set statuses `applied`, append a `recovery_records` entry (audit), CAS-write, delete sidecar. |
| 4 | `graph_create` | Graph opens read-only and its schema digest == `desired_schema_digest`; state stale | Same roll-forward as #3 (the create completed). |
| 5 | `graph_create` | Root exists but the graph does not open (the engine's partial-init gap) | Status `error`, condition `graph_create_incomplete`, message: remove the root and re-run apply. **No auto-delete** — reconciler-initiated deletion is the same data-loss class as human deletion (high-risk decision #7). Sidecar kept until the operator acts and a sweep observes a clean state. |
| 6 | any | Graph at any other version (out-of-band movement during the crash window) | Status `drifted`, condition `actual_applied_state_pending`; sidecar kept; the command refuses graph-moving work for that graph until `cluster refresh` re-observes and the operator re-plans. No success is acknowledged for the interrupted operation. |
| 7 | `graph_delete` | Root absent; state already tombstoned | The delete kind's analog of row 2 (no manifest exists to version-check): crash fell between state CAS and sidecar delete. Delete sidecar; done. |
| 7b | `graph_delete` | Root absent; state stale | Roll forward: tombstone the graph subtree out of state (D6), record audit, delete sidecar. Idempotent — re-entry after a crash mid-row lands in row 7. |
| 8 | `graph_delete` | Root present (delete crashed mid-prefix-removal or never started) | If the approval artifact is still attached (D4), the delete is re-proposed by plan and re-runnable; status `drifted`, condition `graph_delete_incomplete`. Partial prefix removal leaves an unopenable graph — same operator message as #5. |

Rows 3, 4 and 7b are the only mutations the sweep performs, and each is an ordinary CAS-checked state write under the lock — the sweep introduces no new write machinery.

### D4. Approval artifacts (exit criteria 1-partial and 6-partial; axioms 8 and 11)

The irreversible tier — graph delete, `allow_data_loss` schema apply (hard drops) — requires a recorded human decision that survives any reconstruction of state. `plan` already emits `approvals_required`; Phase 4 adds the consumption side.

**Artifact** (`__cluster/approvals/{approval_id}.json`, written by the new command, never by apply):

```json
{
  "schema_version": 1,
  "approval_id": "<ulid>",
  "resource": "graph.scratch",
  "operation": "delete",
  "reason": "<the approvals_required reason from plan>",
  "bound_config_digest": "<desired config digest the plan was computed from>",
  "bound_before_digest": "<state digest of the resource, or null>",
  "bound_after_digest": "<desired digest, or null for delete>",
  "approved_by": "<operator id, required>",
  "created_at": "<rfc3339>"
}
```

**Flow.** `cluster approve <resource-address> --config <dir> --by <operator>` re-runs the plan under the lock, locates the pending gated change for that address, prints it, and writes the artifact bound to the exact digests. `cluster apply` executes a gated change only when a pending artifact matches **all** bound digests — a stale approval (config moved since) matches nothing, is reported (`approval_stale` warning), and the change stays `blocked` with condition `approval_required`. On successful execution the artifact file is moved into `state.approval_records[approval_id]` in the same state CAS that records the outcome (the state references the audit fact; losing state does not lose the approval, which is also why `import` preserves `approval_records` it finds — see D7).

`allow_data_loss` is **never** a CLI flag on `cluster apply`; destructive promotion is expressed only through an approval artifact for the specific schema change. The default schema apply path runs with `allow_data_loss: false` (soft drops), which the spec's tier table classes as a recoverable definition rewrite — plan warning, no artifact.

### D5. Actor, ordering, and apply groups (exit criterion 4)

**Actor.** `cluster apply --actor <id>` / `cluster approve --by <id>`, with `OMNIGRAPH_CLUSTER_ACTOR` as the env fallback. The actor is threaded to `apply_schema_as` (so engine-side Cedar enforcement fires wherever a policy checker is installed and graph commits are attributed), recorded in sidecars, approvals, and `recovery_records`. The cluster adds no policy engine: graph-moving operations inherit the engine's gate; catalog-only operations remain ungated as today. When no actor is supplied and the target graph has no policy checker, behavior is unchanged from Stage 3A (`None` actor, as the engine's no-actor variants do); when a checker is installed the engine's existing "actor required" error surfaces as a typed diagnostic (`actor_required`).

**Ordering.** Deterministic, dependency-shaped, within one apply run:

1. graph creates (with their schemas) — ULID-stable order by graph id
2. schema applies — sequential, one graph at a time (each holds that graph's `__schema_apply_lock__`; the cluster state lock already serializes cluster-side)
3. catalog writes (queries/policies) — the Stage 3A path, unchanged
4. deletes last (catalog deletes, then approved graph deletes)

Each graph-moving operation is its own apply group: sidecar → engine call → sidecar update → continue. The **state CAS stays single and final** (one write at the end recording every outcome), preserving Stage 3A's protocol; sidecars cover the widened gap between individual engine calls and that final CAS. A failure mid-sequence stops graph-moving work, reports per-resource statuses for everything already done (loud partials), and leaves sidecars for the sweep. Cross-graph atomicity is explicitly not promised.

**Failpoints.** Each engine-call boundary gets a failpoint (`cluster_apply.before_graph_create`, `cluster_apply.after_graph_create`, `cluster_apply.before_schema_apply`, `cluster_apply.after_schema_apply`, `cluster_apply.before_graph_delete`) so every row of the D3 matrix is testable with the Stage 3B harness.

### D6. Graph delete (4C)

With no engine primitive, delete is cluster-orchestrated prefix removal: verify the approval artifact → sidecar (`kind: graph_delete`, current manifest version recorded) → recursively remove `ClusterRoot/graphs/<id>.omni` → state CAS that tombstones the graph subtree (graph, schema, and its queries removed from `applied_revision.resources` and `resource_statuses`; observation replaced by a tombstone record `{deleted_at, approval_id}`) → delete sidecar. Catalog blobs of the graph's queries stay (GC remains a later stage, consistent with Stage 3A deletes). The engine gap (no atomic prefix delete; partial removal leaves an unopenable root) is handled by D3 row 8, and this RFC registers a desire for an engine-level `destroy_graph` primitive as future work, not a dependency.

### D7. Plan and import integration

- **Plan** gains real data impact for schema updates: where Stage 3A showed only a digest diff, Phase 4 calls `preview_schema_apply_with_options` against the live graph (read-only) and embeds the migration steps + drop warnings in the change record — the "data-aware provider peek" from the high-level spec, bounded to graphs the plan already observes. Failure to preview (graph unreachable) degrades to the digest diff with a warning, never blocks planning.
- **Import/refresh** already observe live graphs; Phase 4 makes `import` preserve `approval_records` and pending `recoveries/` it finds (state reconstruction must not orphan audit facts or pending repairs).

### D8. Invariants and axioms check

- *Respect the substrate / no custom transaction manager*: cluster never rolls back graphs; engine sidecars own intra-graph atomicity (D3).
- *Axiom 5 (state = deployed reality)*: recovery converges the ledger to observation, never observation to ledger.
- *Axiom 8 (reversibility gates apply, including drift correction)*: approval artifacts for the irreversible tier; sweep never auto-deletes (D3 rows 5/8).
- *Axiom 9 (plan-time integrity)*: ordering is planner-derived from existing dependency edges; no runtime discovery.
- *Axiom 11 (approvals in a durable ledger)*: artifacts are files first, state-referenced after consumption; reconstructable state never re-derives who approved.
- *Axiom 12 (locked state)*: every new write path (sidecars, approvals consumption, sweep) runs under the existing state lock.
- *Axiom 15 (single owner / mode switch)*: nothing here reads from or writes to `omnigraph.yaml`; applied graphs still serve nothing until Phase 5.
- *Loud partials (deny-list)*: every crash window lands in a typed status/condition; no path acknowledges unverified success.

## Migration / Compatibility

Additive. Stage 3A/3B behavior is unchanged for catalog-only configs; existing state files gain no required fields (`approval_records`/`recovery_records` already exist, empty). New CLI surface: `cluster approve`, `--actor` on `cluster apply`. A deployment that never declares schema changes or graph creates sees identical behavior to Stage 3B. The honored-or-rejected posture continues: no new `cluster.yaml` fields are introduced by this phase (graph roots stay derived).

## Sequencing

| Stage | Scope | Gate |
|---|---|---|
| **4A graph create** | `Omnigraph::init` at derived roots; create-intent sidecar; D3 rows 1/2/4/5; dependents unblock in-run | Failpoint tests for crash-before/after-init; e2e: declare graph → apply → import-less convergence |
| **4B schema apply** | Full sidecar lifecycle; roll-forward sweep (D3 rows 3/6); actor threading; plan data-impact preview; soft-drop default | Failpoint tests per matrix row; e2e: schema evolution fully cluster-driven (replaces the Stage 3A defer→manual→refresh loop) |
| **4C graph delete** | `cluster approve` + artifact consumption; prefix removal; tombstones; D3 rows 7/7b/8 | Failpoint tests incl. partial-removal; e2e: gated delete refused without artifact, executed with it, stale artifact rejected |

Each stage is a separate PR with boundary-matched tests (the Stage 1–3B discipline). 4A ships first because it moves no existing manifest; 4B is the heart; 4C last because it is the only irreversible-tier executor and consumes the approval machinery 4B's hard-drop path also needs.

## Exit-criteria coverage (implementation spec)

| # | Criterion | This RFC |
|---|---|---|
| 1 | State/status/approval/recovery schemas + paths | **Approval + recovery schemas: answered** (D2, D4). State/status: unchanged from shipped Stage 2A/3A. |
| 2 | Sidecar schema + recovery decision matrix | **Answered** (D2, D3) |
| 3 | State backend interface / lock+CAS | Unchanged (local JSON backend, shipped) — out of scope |
| 4 | Apply group syntax + dependency ordering | **Answered** (D5): per-resource groups, fixed kind-ordering; no user-declared group syntax this phase |
| 5 | Plan JSON schema incl. blast radius + approvals | **Extended** (D7 preview embedding); base schema shipped |
| 6 | Bootstrap authority + first-actor | **Partial** (D5 actor threading); cluster bootstrap authority remains open (below) |
| 7 | Server startup migration | Phase 5 — deferred |
| 8 | Per-query policy / `mcp.expose` bridge | Phase 6 — deferred |
| 9 | Pipeline runtime | Phase 7 — deferred |

## Open Questions

1. **Bootstrap authority.** The first apply against a fresh cluster has no policy engine to consult and no actor registry; today the answer is "whoever holds the object store wins." The durable story (out-of-band privileged bootstrap actor, per the high-level spec §open-questions) is unresolved and blocks nothing in this phase, since graph-level Cedar still gates wherever installed.
2. **Approval expiry.** Artifacts are digest-bound, so config drift invalidates them naturally; is wall-clock expiry also wanted (operator hygiene), or does digest binding suffice?
3. **Sweep on read-only commands.** This RFC has `status`/`plan` only *warn* about pending sidecars. If operator feedback shows the warn-but-don't-repair posture causes confusion, promoting `plan` to run the sweep (it already takes the lock) is a compatible change.
4. **Graph rename.** Deliberately out of scope; interacts with the rename-stable-identity known gap in [invariants.md](invariants.md). A rename today is delete + create — i.e., gated, lossy, and honest about it.
5. **Engine `destroy_graph` primitive.** 4C's prefix removal is correct but unatomic; if the engine grows a graph-destroy primitive with its own recovery, D6 collapses onto it (the cluster code is shaped to delegate).

## References

- [cluster-config-implementation-spec.md](cluster-config-implementation-spec.md) — phases, exit criteria, high-risk decisions, approval tiers
- [cluster-axioms.md](cluster-axioms.md) — axioms 5, 8, 9, 11, 12, 15
- [cluster-config-specs.md](cluster-config-specs.md) — the data-aware provider peek; state/ledger model
- `crates/omnigraph/src/db/manifest/recovery.rs` — the engine sidecar + classifier this design mirrors in vocabulary and deliberately does not duplicate in mechanics
- [writes.md](writes.md), [invariants.md](invariants.md) — engine recovery protocol and the deny-list this design is checked against
