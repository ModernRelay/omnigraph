# Unlimited-history latency — the end-to-end plan

**Type:** forward execution plan (the live plan for this phase)
**Audience:** engine / storage maintainers
**Companions:** [latency.md](latency.md) (current-state synthesis — read for the validated cost model and production evidence), [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md) (design archive), [write-latency-roadmap.md](write-latency-roadmap.md) (the bounded-history layered fix), [lance.md](lance.md), [versioning.md](versioning.md).

This is the end-to-end plan to make the engine serve **unlimited history at constant warm latency**. It states the goal, what is already done, the bounded-vs-unlimited fork, the LanceDB/Lance prior art that shapes the design, the phased plan (gate first), how it maps onto RFC-013's remaining steps, and the naming we adopt from the substrate.

---

## 1. Goal and non-goals

**Goal:** a warm single-row write is **O(1) in commit-history depth**, time-travel reads any version in **O(1)**, and **no history is ever deleted** (no GC). This is what the production `personal` graph needs — it is a memory graph where deep time-travel is a feature, not a luxury.

**Non-goal:** bounded history via GC. Garbage-collecting `__manifest` versions (RFC-013 Layer 1) makes the lists cheap by *deleting* history. That is a legitimate *separate* profile for deployments that do not need deep time-travel, and it is **mutually exclusive** with this goal. We do not pursue it here.

**Release constraint that drives sequencing:** 0.8.0 already forces a hard `v3 → v4` rebuild (strand + commit-graph retirement). **0.8.0 should be the last release that ever requires a rebuild.** So the one on-disk format change this work needs (head-pointer rows, `v4 → v5`) is folded into 0.8.0, so operators rebuild **once**. Deferring it would force a *second* rebuild later. This overrides the otherwise-correct instinct to defer a format change until a metric demands it: here the migration economics dominate.

---

## 2. Where we are (validated this cycle)

The cost model and production evidence are in [latency.md](latency.md); the short version:

- A warm single-row write does **~6 `_versions/` LISTs + 1 full `__manifest` scan + ~5 writes**. On R2 each LIST is a one-page read (production-measured: **205 KB at 689 versions**). Wall ≈ round-trip-count × RTT; ~94 % of wall is I/O wait.
- **Production ground truth** (read-only inspection of both graphs): both are **V2 naming** (so the bounded one-page model holds, no V1 worst case), both **internal-schema v3**. On the old R2 graph the *retired* `_graph_commits.lance` (723 versions) and `_graph_commit_actors.lance` (681) out-versioned `__manifest` (689) — they were the **two biggest per-op list amplifiers**, so Phase B retirement is a top-tier win, not a tidy-up.
- **Already shipped:** Phase 7 (lineage in `__manifest`), Phase B (retire the two tables), `__manifest` compaction (RFC-013 step 2), WriteTxn / opener bypass (step 3a — the *mutation* open is already pinned), #307 (one publish scan), the read-path warm-up. Confirmed by source: the warm `known_state` is **already folded incrementally** O(touched-tables) per commit, so the in-memory state the warm-publish step needs already exists.

---

## 3. The bounded-vs-unlimited fork

| | Bounded history (NOT this plan) | Unlimited history (this plan) |
|---|---|---|
| Latest-resolve | keep listing, make the list short via **GC (Layer 1+2)** | **stop listing**: pinned opens + forward-probe |
| Manifest scan | compaction bounds I/O; small keep-window | warm publish reuses folded `known_state`; **head-pointer rows** bound cold-open |
| Time-travel | lost below the keep-window | full, any version, O(1) (already true) |
| History | bounded | unbounded |
| Storage space | bounded | **unbounded (inherent — the price of keeping all history)** |
| GC | required | **never** |

The only thing unlimited history cannot escape is **storage space** (unread objects cost storage, not latency). Everything else is made O(1) by capturing once and reusing, not by deleting.

---

## 4. Prior art: LanceDB / Lance (researched against `lancedb/lancedb` + `lance-format/lance`)

LanceDB sits on the same `lance` substrate and hits the same S3 list-amplification, so its choices are direct evidence.

- **`read_consistency_interval` / `ConsistencyMode { Lazy, Strong, Eventual }`** (`lancedb/rust/lancedb/src/table/dataset.rs`). The configurable freshness budget for a warm handle. **Default Lazy = 0 IO between explicit reloads.** Strong checks every read; Eventual uses a TTL with background refresh inside a pre-TTL window (`BackgroundCache` + `refresh_window`) so a read never blocks unless fully expired. Freshness is checked via Lance's own `checkout_latest()`, not a hand-rolled list. This is a cleaner, named version of our implicit per-read probe, and a user-facing knob.
- **Lance deliberately rejected the fully-optimistic write.** `lance/rust/lance/src/dataset/write/commit.rs::test_commit_iops` comment: a fully-optimistic 0-read commit "would mean wasted write requests (txn + manifest) if there was a conflict. We choose to be pessimistic for more consistent performance." Their warm commit is **1 read (latest-version check) + 2 writes, flat in history** (`test_reuse_session`), because the shared `Session` metadata cache serves everything except the one latest-version check. Conflict retries reuse cached manifests, not re-reads.
- **`IoStats` exposes `num_stages`** (disjoint in-flight periods = round-trip/hop count) **and `requests`** (the full RPC log, dumped into `assert_io_eq!/lt!/gt!` panics) — `lance/rust/lance-io/src/utils/tracking_store.rs`. We already wrap this `IOTracker`; we just don't surface those two fields.
- **`test_reload_resets_consistency_timer`** is LanceDB's named regression for "a list call on every query after the interval expires" — the exact class our gate guards.
- **Version hint** (`latest_version_hint.json` + `probe_versions_upward`) is gated OFF for lexically-ordered stores. Our R2 is lexical V2 (production-confirmed), so the hint is unavailable, but a pinned-version open saves the **entire** list RPC anyway — which is the same outcome the hint would give.

**What LanceDB does that we should adopt:** the `read_consistency_interval` abstraction, the `num_stages` hop metric, the request-log-on-failure diagnostics, and the discipline of keeping one cheap pre-commit check rather than going fully optimistic. **Targets for our gate** (their measured warm commit): `read_iops ≈ 1`, `num_stages ≈ 3`, flat across depth.

---

## 5. The plan: U0 → U1 → U2 (all targeting 0.8.0)

- **U0 (now): the depth-swept S3 cost gate.** The acceptance test for everything below. §6.
- **U1 (no on-disk format change; freely reversible):**
  - **Layer 4 — warm publish.** Thread the already-folded `known_state` into publish attempt 0; fall back to cold `load_publish_state` only on real CAS contention. Keep ONE cheap pre-commit freshness check (the forward-probe), per Lance's lesson; do not go fully blind. (`db/manifest/publisher.rs`, `db/manifest.rs`.)
  - **Layer 3 — pinned-version opens** on the remaining write opens (staging open, data-table commit) via `open_table_dataset(location, pinned_version, session)`. List-free; the CAS stays the fence. (`db/omnigraph/table_ops.rs`, `exec/staging.rs`.)
  - **Forward-probe freshness + `ConsistencyMode`.** Replace the residual `latest_version_id` LIST with a forward HEAD-probe of `manifest_path(V+1)` from the coordinator's known version, behind a named `ConsistencyMode`/`read_consistency_interval` (default Strong-ish; Lazy/Eventual as a staleness budget for read-heavy graphs). (`db/manifest.rs`, `db/graph_coordinator.rs`.)
  - **Shared `Session`** on `open_dataset_tracked` / `open_manifest_dataset`, closing the cold-TLS-per-open asymmetry with `open_table_dataset`. (`src/instrumentation.rs`, `db/manifest/layout.rs`.)
  - **Branch-delete parallelize** — `buffer_unordered` over `owned_tables` in `cleanup_deleted_branch_tables`, pinned opens for those tables. (`db/omnigraph.rs`.)
- **U2 (the one format change, `v4 → v5`, in 0.8.0):** head-pointer `table_head:<table>` rows. §9.

---

## 6. U0 — the depth-swept S3 cost gate (the immediate deliverable)

An S3-backed test that builds commit-history depth, **compacts at each depth (allowed) but never GCs**, and asserts a warm single-row write's cost is **flat across depth** and **under a ceiling that ratchets down as each U1 layer lands**. It fails the moment the write path re-introduces an O(history) round-trip.

**Metrics** (most-faithful-to-the-reports first):
1. **`num_stages`** (round-trip/hops) — the reports' "wall ≈ round-trip-count × RTT" maps directly to this.
2. **`read_iops`** (total per-write read requests).
3. **LIST count** (the `_versions/` resolves) — ratchets 6 → ~1 as Layers 3/4 land.

Target after U1 (LanceDB-anchored): `read_iops ≈ 1`, `num_stages ≈ 3`, LIST ≈ 1 (or 0 with forward-probe), flat across depth.

**Harness additions** (`crates/omnigraph/tests/helpers/cost.rs`): surface `num_stages` and a `list_requests` count into `IoCounts` from the `IOTracker`'s `incremental_stats()` (`IoStats { num_stages, requests, .. }` is already available), counting LISTs by method on BOTH the `__manifest` ground-truth tracker and the data-table wrapper; adopt Lance's `assert_io_eq!/lt!/gt!` shape (dump the request log into the panic, reusing `last_manifest_reads()`).

**The test** (extend `crates/omnigraph/tests/write_cost_s3.rs`; bucket-gated; CI `rustfs_integration`): reuse `s3_graph`, `commit_many`/`commit_many_as`, `measure_insert`, `assert_flat`, the anchor-depth sweep `[10, 50, 100]`. **Run inside `cost_harness`** (the current `write_cost_s3.rs` test does not — required so the warm-coordinator probe's reads are ground truth). Compact at each depth (no `cleanup`). Assert `num_stages`/`read_iops`/`list_requests` flat across depth AND under a shallow-depth ceiling set to today's value (green now, ratchets per layer). Keep an `assert_grows`-without-compaction companion so the gate has teeth.

**Reuse map:** `helpers/cost.rs` (`IoCounts`, `assert_flat`, `measure_insert`, `cost_harness`, `last_manifest_reads`, `s3_graph`, `PrefixCounter`); `helpers/mod.rs` (`commit_many`, `commit_many_as`); `write_cost.rs` (`internal_table_scans_are_flat_in_history`, `internal_table_scans_grow_without_compaction`, `write_op_count_ceiling_at_shallow_depth` — the patterns to mirror on S3). Gotchas: `#![recursion_limit = "512"]`, `cost_harness` mandatory for ground-truth counting.

**LANDED (U0).** `warm_write_cost_flat_and_bounded_in_history_on_s3` (`write_cost_s3.rs`), measured on RustFS. **Baseline a warm single-row write must beat:** `manifest_list=6`, `num_stages=13`, `manifest_reads=9`, `data_opener=4`, `total=14` — **identical at depth 10 and 50** (perfectly flat with compaction, no GC). Depth capped at 50 (depth 100 trips an unrelated Lance FTS-index-builder panic during `optimize_indices` on S3; tracked separately). Each U1 layer ratchets the ceiling down toward `manifest_list ≈ 1`, `num_stages ≈ 3`.

---

## 6b. U1 execution plan — the full correct-by-design publish refactor (RFC-013 step 5, ALL writers)

Decision: do the **full** refactor, not a minimal bolt-on. Implement RFC-013 §4.1's `GraphPublishAuthority` fed declarative `PublishPlan`s across all five writers, a unified open path, and a `ConsistencyMode` freshness policy. Correctness-and-efficiency-by-design over preserving legacy code (pre-release; no backwards-compat). The legacy shape removed: the **split** where `ManifestCoordinator` owns the warm state but a stateless publisher re-opens + re-scans cold every call, and four writers hand-roll their publish. The full design lives in [rfc-013-write-path-latency.md](rfc-013-write-path-latency.md) §4.1 (the type spec) — now being built on this branch.

**Type decision — EVOLVE, do not fork** (invariant 15): bundle `publish(changes, expected, lineage)` + the pinned base into `PublishPlan { base, actions, lineage, expected }`; keep `ManifestChange` as the lowered manifest-row vocabulary + a thin `TableAction → ManifestChange` lowering; grow `WriteTxn { base }` with `session`+`handles`. `TableAction` starts thin (`Append/Upsert/Overwrite/Delete`), variants phased in per writer.

**Correctness spine every phase preserves:** CAS sole-authority (the `__manifest` merge-insert; the pre-check is non-atomic); §7.1 `graph_head:<branch>` serialization (already in place — disjoint same-branch writers overlap so the loser retries); recovery all-or-nothing (redo `plan.apply()` is live-and-recovery identical; roll-BACK `Dataset::restore` stays open-time-only); snapshot isolation; strict-vs-non-strict (Update/Delete/SchemaRewrite keep cold drift guards; only Insert/Merge get the warm path).

**Phased landing** (each independently shippable, gate-measured):

| Phase | Change | Gate | Status |
|---|---|---|---|
| **1a. Unified open chokepoint** | `open_dataset_internal(location, VersionResolution::{Latest\|At(u64)}, session, wrapper)`; the two openers become thin shims | neutral | **DONE** (`instrumentation.rs`) |
| **1b. Session on manifest opens** | thread `ReadCaches.session` to `open_manifest_dataset` | neutral (wall only) | **DEFERRED into Phase 3** — `open_manifest_dataset` has ~15 callers and the coordinator doesn't hold the session; the per-write beneficiary (the publisher's per-attempt open) is *subsumed* by Phase 3's warm-handle reuse, so fold it there rather than thread it invasively now |
| **0. Safety net** (prereq for P3) | cross-process multi-writer harness (in-process failpoints can't reproduce the corruption); land §5.5 interleave + write-skew guards; `assert_grows`-no-compaction gate companion | neutral | **DONE** (0.1a/0.1b `cross_process_writes.rs` + `failpoints.rs`, 0.2 `cli_cross_process.rs`, 0.3 S3 negative control; the `omnigraph-dst` Cohort/convergence core landed) |
| **2. Authority skeleton** | `PublishPlan` as the declarative publish input; the publisher trait takes the plan. Byte-identical | neutral | **DONE** (`aa77683a`) |
| **3. Mutation writer → PublishPlan (FIRST)** | the gate-moving writer; **Layer 4 warm publish** (attempt 0 uses the folded `known_state`, reload cold only on CAS conflict) + **Layer 3 pinned non-strict opens** (`open_dataset_internal(At(pinned))`); the Phase-1b session on the staging open deferred to Phase C (S3-gated) | **biggest ratchet:** `manifest_reads`+`manifest_list`↓↓, `data_opener`→~0 | **DONE** (3.1–3.4; the warm probe applies to every `commit_changes_with_lineage` caller, so all lineage writers share it) |
| **3.5. Warm-fold hardening** | make warm ≡ cold **by construction**, not by comment: (a) `fold_published_state` — the publish fold derives its version from the base and **fails closed to a re-scan when the commit landed ≠ base+1** (Lance internally rebases past a concurrent *disjoint* `__manifest` commit; today's only such racer is content-preserving compaction, but U2's head-pointer rows make it logical); (b) `ManifestState` retains the tombstone pair-set and ONE assembly funnel builds the publish state for both warm and cold. Ports Lance's `Transaction { read_version, operation }` coupling + SlateDB's one-register/fail-closed shapes | neutral (re-scan only on the rare rebase) | **THIS CHANGE** |
| **4. Forward-probe + ConsistencyMode** | `manifest_path_for_version` + HEAD `V+1` replaces the `latest_version_id()` LIST; `ConsistencyMode { Strong, Lazy, Eventual }` + `read_consistency_interval`, default Strong; preserve `ManifestIncarnation` | `manifest_list`→~1 | TODO |
| **5. Remaining writers** | schema_apply → branch_merge → optimize onto `TableAction` plans, one PR each, behind their oracles; `compact_internal_table` stays special-cased; `ensure_indices` deferred | neutral | TODO (the warm *probe* already covers them; this migrates their plan-building) |
| **6. Recovery sidecar == serialized PublishPlan** | Phase C + recovery both call one `plan.apply()` (redo); the roll-BACK classifier stays untouched | small ↓ | TODO |
| **7. `writer_epoch` fence** | DEFER unless a multi-writer topology ships in 0.8.0; behind a linearizable conditional-put store only | neutral | DEFER |

**Hardest traps:** (A) warm attempt-0 is a *pre-check optimization only* — the CAS still arbitrates; on CAS loss attempts 1+ cold-reload + re-resolve lineage; (B) `plan.apply()` is redo only, roll-back stays open-time; (C) `ConsistencyMode::Lazy` must keep read-your-writes (the coordinator's own `known_state` is authoritative for its own commits; the mode only gates probing for *foreign* advances); (E) a strict op must never route through the warm/pinned path. **0.8.0 scope:** Phases 1–6 + U2 (the head-pointer format change); defer Phase 7 + Layer 1/2 GC (mutually exclusive with unlimited history).

**Phase 0/3 validation pass (subagent, 2026-06-28; after Phase 2 `aa77683a`).** Plan + adversarial source-validation against the live code. Seven load-bearing assumptions confirmed: (1) `known_state` is incrementally folded O(touched-tables) and adopted post-publish (`manifest.rs:516-529`), authoritative for read-your-writes; (2) the publisher CAS loop cold-reloads `load_publish_state` AND re-resolves the lineage parent per-attempt inside the retry (`publisher.rs:695-747`), so warm attempt-0 being a stale pre-check is safe; (3) the strict/non-strict split is `MutationOpKind::strict_pre_stage_version_check` (`db/mod.rs:49-66`), only Insert/Merge non-strict; (4) the residual latest-resolve LIST is the non-strict drift probe `latest_version_id()` in `staging.rs:728-740` (the Phase-4 forward-probe target); (5) `DatasetBuilder::with_version(N)` is list-free (pinned GET, no `_versions/` enumeration). Corrections found (the value of the pass):
- **The non-strict staging open is `Latest` today, not pinned.** `reopen_for_mutation` non-strict arm (`table_ops.rs:916-929`) opens HEAD via `open_dataset_head_for_write` and explicitly discards `expected_version` (`let _ = expected_version;`, line 925). This `_versions/` LIST is a chunk of the gate's `data_opener=4` term. Layer 3 (Commit 3.1) pins it to `At(expected)`.
- **`open_dataset_at_state` does NOT save the LIST.** It does `open_dataset_head` (a `Latest` LIST) then `checkout_version` (`table_store.rs:287-299`). Layer 3 MUST add a true `At(v)` pinned opener (`open_table_dataset` → `open_dataset_internal(At(v))`, list-free), never reuse `open_dataset_at_state`. If a reviewer sees `open_dataset_at_state` on the Layer-3 path, the LIST elimination silently fails and only the S3 `data_opener` gate catches it. *(Historical: `open_dataset_at_state` has since been removed entirely on main — the trap can no longer be fallen into.)*
- **`known_state` carries no lineage parent.** The warm attempt-0 parent must be threaded as a `head_hint` from `GraphCoordinator`'s in-memory commit-graph head (`graph_coordinator.rs` cache, zero IO); the cold retry path re-resolves authoritatively off fresh `lineage_rows`. A stale `head_hint` co-occurs with a stale warm state, which the one freshness probe catches and falls through to cold.

**Phase 3 commit decomposition (validated):** 3.1 Layer-3 non-strict pinned staging open (true `At(v)` opener + shared `Session`; strict path byte-identical; gate: `data_opener`↓). 3.2 Layer-4 warm-publish scaffolding wired `warm = None` (byte-identical seam, Phase-2 discipline). 3.3 Layer-4 activation (thread `head_hint`, freshness-probe-gated warm attempt-0; gate: `manifest_reads`+`manifest_list`↓, the gate-moving commit). 3.4 ratchet the S3 ceilings to measured + docs.

### Phase 0 detailed plan — cross-process multi-writer safety net (production-code-neutral; lands before any Phase-3 change)

**Why it is mandatory and why it is genuinely new.** Phase 3's warm-attempt-0 is correct only because a stale warm pre-check loses the `__manifest` CAS and the next attempt cold-reloads + re-parents. The single failure mode that exercises this is **two independent coordinators with diverging `known_state`**: handle B's warm state goes stale exactly when handle A commits. In-process failpoints cannot reproduce it (one `known_state`), and the existing DST concurrent walk cannot either (it shares ONE `Arc<Omnigraph>`, so one coordinator + the in-process write queue serializes). Phase 0 adds the diverging-coordinator dimension and pins it as a regression before Phase 3 makes the warm path load-bearing.

**Reuse decision (checked the experimental harness PRs).** The DST harness lives in **#309** (`dst-extract-crate`, OPEN, tests-only: the `omnigraph-dst` dev crate with a `Backend` trait [`Embedded` + `Cli` subprocess], the white-box battery `head_eq_manifest` / `no_duplicate_live_row_ids` / `no_duplicate_keys` / `dataset_validate`, a known/novel classifier, a `FaultAdapter`, and a shared-handle concurrent walk) and **#304** (`dst-determinism`, OPEN: the only src change, a deterministic-replay seam). #303/#305 are CLOSED, superseded by #309. **Phase 0 builds on COMMITTED infrastructure, not on the unmerged crate**, so it is independently shippable: the engine `tests/helpers/failpoint.rs::Rendezvous` (deterministic interleave), `tests/helpers/cost.rs` (`assert_grows`/`cost_harness`/`s3_graph`), and the CLI `tests/support` binary-spawn harness. It **aligns with** #309 by reusing its invariant *vocabulary* by name so the two converge: if #309 lands, Phase 0's targeted cells become characterization regressions next to the generative walk, and the diverging-coordinator dimension folds into #309's `concurrent_walk` (give its actors SEPARATE `Omnigraph::open(uri)` handles instead of `Arc::clone`). #304's determinism seam is related but not a dependency (`Rendezvous` is deterministic by construction).

**Tier 1 — two engine handles, one process (deterministic, every-PR).** Two `Omnigraph::open(uri)` on one local-FS graph have independent coordinators / `known_state`. New file `crates/omnigraph/tests/cross_process_writes.rs`:
- `two_handles_disjoint_table_writes_form_linear_chain` (non-gated): handle A writes `node:Person`, handle B a different node type; both contend the shared `graph_head:<branch>` §7.1 row; assert both commit, single linear lineage chain, no fork. Mirrors `manifest::tests::n_concurrent_disjoint_writers_converge_to_one_linear_chain` but across two coordinators.
- `stale_warm_precheck_loses_cas_then_cold_reloads` (`failpoints`-gated, `#[serial]`): pin handle B at version N, advance handle A to N+1, then write on B parked at a `Rendezvous` at the publish boundary. Assert OUTCOME (regime-independent, so green now AND after Phase 3): B commits, B parents off A's commit (not off N), row count reflects A's foreign write + B's own (read-your-writes + foreign visibility). Phase 3 changes the internal path it drives (warm→cold fallback) while the asserted outcome holds, which is what makes it the right red-team net.
- `fork_reclaim_race_across_handles` (`failpoints`-gated): two handles write the same not-yet-forked named branch, racing `reclaim_orphaned_fork_and_refork`; assert one-winner-CAS, one fork, both rows, no corruption (the documented in-process-safe-only exposure).

**Tier 2 — two spawned `omnigraph` CLI processes (cross-address-space oracle, opt-out-gated).** SHIPPED as `crates/omnigraph-cli/tests/cli_cross_process.rs::two_cli_processes_disjoint_inserts_converge_or_hit_known_drift`, driving `omnigraph_dst::Cohort<Cli>` (the only tier with genuinely separate address spaces — no shared `Session`, no in-process write queue, no shared `known_state`). Honors `OMNIGRAPH_SKIP_SYSTEM_E2E=1`.

**Phase 0 FINDING (Tier 2, Phase-3-relevant).** Two concurrent cross-process non-strict inserts to disjoint tables hit **RC-1-class uncovered drift** ~5% of runs: one writer's table ends with Lance HEAD ahead of the manifest (`"…ahead of manifest version…; run `omnigraph repair`"`). This is the documented *in-process-only recovery* gap (see [invariants.md](invariants.md) Known Gaps: "Recovery is serialized against live writers in-process only") reproduced ACROSS processes — a process that leaves a sidecar-covered HEAD-ahead-of-manifest residual and exits is not healed by the OTHER live process. So Tier 2 is a DST-style characterization: it tolerates the *classified* known bug (`classify_backend` → "RC-1 stale-view"), but asserts every run that there is **no NOVEL load error and the lineage never forks** (drift leaves a table HEAD ahead, it never forks the commit chain), and asserts strict row-convergence only when both writers commit cleanly. **Watch item for Phase 3:** Phase 3 reworks exactly this non-strict publish path (warm attempt-0 + pinned opens); this cell is the cross-process net for whether that change moves the RC-1 rate. Closing the underlying gap needs the cross-process serialization primitive the invariants doc already scopes (lease-based use of the schema-apply lock), out of scope for Phase 0/U1.

**Gate teeth — `assert_grows` S3 negative control.** Add `warm_write_cost_grows_without_compaction_on_s3` to `write_cost_s3.rs`: the flat test's body with the `db.optimize()` between depths REMOVED and `assert_flat`→`assert_grows` on `manifest_reads` (+ `manifest_num_stages`), proving the flat gate is flat because of compaction + the fix, not because nothing is measured. The local twin already exists (`write_cost.rs::internal_table_scans_grow_without_compaction`).

**Phase 0 commit decomposition (each compiles + passes on today's code):** 0.1 Tier-1 file (the non-gated linear-chain test + the two `failpoints` rendezvous cells); 0.2 Tier-2 CLI cross-process test (opt-out-gated); 0.3 `assert_grows` S3 negative control + `testing.md` updates. All three assert current behavior, so Phase 0 is genuinely neutral and lands before any Phase-3 production change.

#### Phase 0 abstractions — extend `omnigraph-dst`, do not hand-roll one-off files

The `omnigraph-dst` crate (#309) already owns the reusable harness vocabulary: a `Send + Sync` `Backend` trait (`Embedded` with white-box `db()` + `Cli` subprocess), a `run_battery` invariant **registry** (one line per invariant), structured `classify`/`Finding` (known/novel, no free-text matching), the `Model` oracle, the `OpKind`/`Rng`/`step` op alphabet, and `FaultAdapter` (storage-CAS-loss via `open_with_storage`). Every abstraction is **single-writer**: `Backend` wraps one handle, and the only concurrent walk shares one `Arc<Omnigraph>` (one coordinator), so the diverging-warm-state-across-coordinators surface is absent. Phase 0 adds that surface as TWO new general abstractions in the crate, mirroring the existing shapes, so Phase 3 / Phase 5 / U2 / the epoch fence each extend them with one value or one registry line rather than a new file:

- **`cohort` module — `Cohort<B: Backend>`** (generic over the existing `Backend`, so ONE abstraction drives both tiers). `Cohort::open_embedded(uri, n)` opens `n` independent `Omnigraph::open(uri)` handles = `n` coordinators with `n` independent `known_state` (the in-process diverging-warm surface); `Cohort::spawn_cli(bin, uri, n)` is the cross-address-space surface (`n` separate processes, no shared `Session`/queue). `writer(i) -> &B`, and it composes with `FaultAdapter` per-writer (`open_embedded_faulted`). The generic is the win: the in-process tier and the cross-process tier are the same code over `Embedded` vs `Cli`, not two hand-rolled files.
- **`convergence` battery — `run_convergence_battery(...) -> Vec<(&str, Result<(), Finding>)>`** (the registry shape of `run_battery`, returning the same `Finding`). Entries: `lineage_is_linear_chain` (the `commit list` parents form one chain, no fork), `all_writers_converge` (post-quiesce every writer reads the same branch head + row set), `one_winner_cas` (under contention exactly one attempt-0 won, the rest retried), `no_orphan_fork` (no manifest-unreferenced per-table branch ref). Adding a convergence property later is one line here.

**Earn-it boundary (per the engineering-over-time lens):** `Cohort` and the `convergence` registry are earned now — the multi-writer dimension recurs across Phase 3 (warm path), Phase 5 (the other writers), U2 (head-pointer concurrency), and the epoch fence (5+ future scenarios that otherwise each fork a file). The **interleave primitive stays minimal**: a `Cohort::interleave_at_failpoint(op_a, op_b, failpoint)` two-writer rendezvous over the engine's existing `Rendezvous`, NOT a full N-writer `Schedule` DSL. Grow to a data-driven `Schedule` only when an N>2 deterministic interleave is actually needed; speculating it now is unearned surface.

With these in place the Phase 0 tests are THIN drivers: each is "open a `Cohort`, run an interleave (or a free race), assert `run_convergence_battery` + the relevant `run_battery` entries." `two_handles_disjoint_table_writes_form_linear_chain` = `Cohort::open_embedded(uri, 2)` + disjoint writes + `lineage_is_linear_chain`. `stale_warm_precheck_loses_cas_then_cold_reloads` = `interleave_at_failpoint` parked at the publish boundary + `all_writers_converge` + `one_winner_cas`. The Tier-2 cross-process test reuses the SAME convergence battery over `Cohort<Cli>`.

**Home decision (open question for the operator).** The lowest-liability home for `cohort` + `convergence` is `omnigraph-dst` itself (one home for all harness abstractions; Phase 3/5/U2 extend the crate, never an engine-tests helper). That implies **splitting #309: land its stable CORE on `main` now** (`Backend`/`Embedded`/`Cli`, `Model`, `op`, the white-box battery + `classify`, `FaultAdapter` — tests-only, zero production risk, the substrate every future multi-writer + recovery test wants) and **add `cohort` + `convergence` to it as part of Phase 0**, while the experimental generative-walk / fuzz / cross-backend / coverage suites (`tests/dst/*`) keep reviewing separately on `dst-extract-crate`. This maps exactly onto "do not merge #309 as-is, but it is relevant": merge the core, defer the walks. The alternative (put `cohort`/`convergence` under the engine's committed `tests/helpers/` over raw `Omnigraph` handles, fold into the crate later) is independently shippable with no crate dependency, at the cost of a temporary parallel `Backend`-like surface to reconcile when the core lands. Recommendation: split-and-land-core; it removes the reconciliation step and gives the abstractions their permanent home on the first move.

---

## 6c. Phase C — the publish-authority convergence (post-#318)

What remains of the §6b decision after Phases 0–3.5 land, with the earned/deferred boundary made explicit. The design stance: each piece lands in its **target shape** (the optimal design, validated against Lance/LanceDB/SlateDB idioms — see §8), not a call-site patch that a later phase redoes. Phase 3.5 is the first instance: the fold's version-coupling and the one-funnel assembly are the publish authority's state discipline, landed early because the warm path made them load-bearing.

- **C1 — session on the pinned staging open.** Grow `WriteTxn { branch, base }` with the shared per-graph `Session` (from `ReadCaches`) so `open_table_pinned` stops paying a cold TLS handshake per open on object stores. Deliberately deferred by Phase 3.1 (`omnigraph.rs`'s `WriteTxn` doc records why): the benefit is an object-store phenomenon invisible on local FS, so it lands WITH its own S3 cost gate, not blind. This is also the anti-forgetting fix — the session becomes a field of the txn (the Lance `Dataset.session` shape), not a per-call `Option<&Session>` argument.
- **C2 — name the freshness decision `ConsistencyMode::Strong`.** The read path and the write path both ask "is my warm handle still the live head?" via `probe_latest_incarnation().matches(..)`; give the decision one named home (LanceDB's `ConsistencyMode`, `Strong` only — the `None→Lazy / Zero→Strong / d>0→Eventual` interval mapping and `BackgroundCache` are built only when the Lazy/Eventual *feature* ships). This is also where the probe-vs-no-probe tuning question lives: SlateDB commits warm with ZERO probes (the CAS is the freshness check); Lance keeps one pessimistic check ("wasted write requests on conflict"). We follow Lance today; a measured S3 comparison can revisit.
- **C3 — `TableAction` plans across the writers (RFC-013 §4.1).** `PublishPlan` grows from `{changes, expected, lineage, warm}` to `{base, actions: Map<TableKey, Vec<TableAction>>, lineage, expected}` with a thin `TableAction → ManifestChange` lowering; schema_apply → branch_merge → optimize migrate one PR each (§6b Phase 5). The four hand-rolled `Vec<ManifestChange>` builders collapse; variants are phased in per writer, never speculatively.
- **Non-goals (unearned surface, declined explicitly):** `Eventual`/`BackgroundCache` before the feature; a general invariant/poison registry while there are exactly two fold invariants (the direct checks in `fold_published_state` are cheaper than a framework); merging the strict/non-strict staging opens (genuinely divergent SI semantics — the strict arm keeps live-HEAD read-modify-write, the non-strict arm keeps the pinned base).

---

## 7. RFC-013 mapping — interleave or not

- **Interleave (they ARE this phase):** Layers 3/4. The full `GraphPublishAuthority` / `PublishPlan` unification (RFC-013 step 5) is the clean home for Layer 4, but is a large refactor — do Layer 4 in the current publisher first; earn the refactor later.
- **Do NOT interleave (different profile, conflicts):** Layer 1 (GC `__manifest`) + Layer 2 (Q8 watermark). They delete history. Keep as a separate optional bounded-history track.
- **Pull forward (orthogonal, cheap, help now):** shared `Session`, branch-delete parallelize.
- **Already done (re-tense the RFC accordingly):** WriteTxn/opener bypass, internal-table compaction, Phase 7, Phase B, the cost-gate harness.

---

## 8. Naming / abstraction alignment with LanceDB

Principle: adopt the substrate's vocabulary where the concept is identical (we are an OSS library on Lance), keep our names where the concept is genuinely ours (multi-table-per-graph coordination, which LanceDB lacks).

- **Adopt:** `ConsistencyMode { Lazy, Strong, Eventual }` + `read_consistency_interval` (the freshness abstraction); `num_stages` + `requests`-log + `assert_io_eq!/lt!/gt!` (cost harness); a forward-probe helper named after `probe_versions_upward` / `checkout_latest`; `BackgroundCache` + `refresh_window` only if/when Eventual is built (don't add the abstraction before the feature).
- **Keep ours:** `ManifestCoordinator` / `GraphCoordinator`, `known_state`, `assemble_manifest_state` / `fold_inputs`, the `GraphNamespacePublisher` / `ManifestBatchPublisher` / `PublishPlan` family, and **"incarnation"** (branch-recreate identity Lance has no concept of). We already use Lance's `Session` / `ObjectStoreRegistry` / `IOTracker` directly.

---

## 9. U2 — the head-pointer format change (`v4 → v5`, lands in 0.8.0)

**What:** add a mutable `table_head:<table>` row kind to `__manifest` — the exact analog of the existing `graph_head:<branch>` lineage pointer — pointing at each table's current version. `assemble_manifest_state` and the publish/cold-open read **O(tables)** head rows instead of reducing **O(commits)** accumulated immutable `table_version` rows. The immutable `table_version` rows stay for time-travel (read from the historical Lance manifest version, not the hot path).

**Why it's the asymptotic wall:** `table_version` AND `graph_commit` rows accumulate one per commit (validated: `version_object_id` is per-version, rows are immutable, `assemble_manifest_state` reduces them). Compaction bounds the scan's I/O but not its row decode; GC bounds the list but not the rows; warm publish (Layer 4) avoids the scan on the warm path but **cold-open** and the **contention re-scan** still pay O(commits). Head-pointers bound those to O(tables). Phase 7 made this heavier by folding lineage into the manifest.

**Why now:** it bumps `INTERNAL_MANIFEST_SCHEMA_VERSION` to 5, and under the strand model a v5 binary refuses a v4 graph (export/import). Folding it into 0.8.0's single rebuild means operators rebuild once.

**Touch points:** `db/manifest/state.rs` (the `object_type` set + assembly), `db/manifest/publisher.rs` (write/update the head row in the publish CAS), `db/manifest.rs`, recovery (`db/manifest/recovery.rs`), and time-travel (`snapshot_at`). Needs Phase-7-depth recovery + snapshot + concurrency tests, plus the v4→v5 refusal test.

---

## 10. Acceptance criteria and verification

1. The U0 gate is GREEN at today's ceiling, and the `assert_grows`-without-compaction companion proves the term genuinely grows uncompacted (the gate has teeth).
2. As each U1 layer lands, ratchet the ceiling down toward `read_iops ≈ 1`, `num_stages ≈ 3`, LIST ≈ 1, staying flat across depth.
3. Run locally against RustFS/MinIO: `OMNIGRAPH_S3_TEST_BUCKET` + `AWS_*` (incl. `AWS_ENDPOINT_URL_S3`); `cargo test -p omnigraph-engine --test write_cost_s3`. Skips gracefully without the env; CI runs it in `rustfs_integration`.
4. For U2: recovery + time-travel coverage across the format change, and the v4→v5 internal-schema refusal test.

---

## 11. Risks and open questions

- **0.8.0 scope.** U2 is the only piece that bumps the on-disk format and MUST land in 0.8.0 or it forces a second rebuild. If 0.8.0 timing cannot absorb U2's risk, the fallback is to ship U1 in 0.8.0 and accept that U2 later forces another rebuild — decide explicitly at release-cut time.
- **Decide checkpoint reference-set vs never-GC BEFORE building U2.** SlateDB's checkpoint model (RFC-0004) shows "deep time-travel" and "bounded storage" are not mutually exclusive: a **checkpoint** is a durable, TTL'd, refresh-heartbeated row pinning a manifest version against GC (`expire = 0` = pin forever), and GC prunes exactly what no live checkpoint or the current head references. On omnigraph that shape is natural — checkpoint rows in `__manifest` next to `graph_head`/`graph_commit`, the same row-CAS discipline. It would dissolve this plan's bounded-vs-unlimited fork (§3): the memory graph pins the versions it wants instead of disabling GC wholesale, and the scalar Layer-2 watermark becomes the degenerate one-checkpoint case. This changes what U2's head-pointer rows and any future GC must support, so the decision (not the implementation) belongs before U2's format change — 0.8.0 is the one-rebuild window.
- **U2 makes the Phase-3.5 fold guard load-bearing.** Head-pointer rows are the first *logical, non-lineage* `__manifest` writer — exactly the disjoint-rebase shape the fold's fail-closed re-derive exists for (today's only disjoint racer, internal-table compaction, is content-preserving and masks the class). Do not build U2 on a binary without Phase 3.5.
- **Do not conflate compaction (allowed; bounds scan I/O) with GC/cleanup (forbidden for unlimited history).** The gate compacts, never cleans.
- **Layer 4 design.** Heed Lance's evidence: reuse warm state but keep one cheap pre-commit check; a fully-optimistic 0-read commit wastes write RPCs on conflict.
- **`ConsistencyMode` default** (Strong vs Lazy) is an open product question — Strong is safest (read-your-writes); Lazy/Eventual is a per-graph opt-in for read-heavy memory graphs.
- The local twin of the gate stays `write_cost.rs` (the list/round-trip cost is a per-RPC phenomenon invisible on local FS).
