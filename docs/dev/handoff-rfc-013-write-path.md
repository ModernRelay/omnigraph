# Handoff: finishing RFC-013 (write-path latency + correctness)

> **Status update (strand-and-retire work):** the `_graph_commits.lance` /
> `_graph_commit_actors.lance` datasets are **retired** (Phase B — lineage lives in
> `__manifest`; the `CommitGraph` is a pure projection). References to those tables
> below are historical: `optimize` now compacts **`__manifest` only** among the
> internal tables, and the per-write `_graph_commits` scan term is gone. See
> [versioning.md](versioning.md) and [invariants.md](invariants.md).

**Status:** living handoff. **Source of truth is [`rfc-013-write-path-latency.md`](rfc-013-write-path-latency.md)** —
this doc is the *current-state map + the decisions/validation from the latest work cycle
+ the concrete next actions*. When they disagree, the RFC wins (and fix this doc).

**Audience:** the engineer/agent who picks up RFC-013 next.

> **Two threads, one RFC.** RFC-013 has been worked in two overlapping lines by different
> cycles, with different sub-numbering. Don't let the numbering confuse you:
> - **Thread A — per-write call-count / RTT collapse + concurrency correctness** (steps 3b /
>   4 / 5, Design A / `PublishPlan`, #297 / #254 / #296 / #298). This is **§1–§10 below** (the
>   original body of this doc). Read it for the concurrency model and the convergent fix.
> - **Thread B — `__manifest` scan amplification + the unbounded `_versions/` chain** (the
>   investigation framed as **PR1 / PR2 / PR3 / PR4**). This is **§A below (read it first)** —
>   it is the most recent cycle (2026-06-26) and where the live branch sits.
>
> They meet at the internal-table maintenance area (Thread A's "step 2a/2b" == Thread B's
> "compaction is done / PR1 = bound the chain"). §A maps the two framings.

---

## §A. Current state — the `__manifest` scan + version-chain thread (2026-06-26) — READ FIRST

The latest cycle attacked **write latency that grows with graph age on object storage** and a
**hard open failure on a large `_versions/` chain**. The working plan (survives context resets,
**not in the repo**) is `~/.claude/plans/in-the-mean-time-humble-reef.md` — pull it for the full
PR1/PR2/PR3/PR4 decomposition, the assumptions-validated-against-code list, and the critical-files
map. This section is the durable summary.

### A.1 The problem (root cause)

Two interacting terms, both centered on the internal `__manifest` Lance dataset (the cross-table
catalog; one dataset, 217 tables on a real graph):

- **Term 1 — repeated full `__manifest` scans per write.** Each read of `__manifest` was a bare
  `dataset.scan()` (no filter/projection/index), cost **O(fragments F)**. A publish did **4** such
  scans (OCC re-capture + `load_publish_state` + the `use_index(false)` merge-insert join +
  post-publish read-back). `F` grows **+1 per write**, so per-write cost climbs with history.
- **Term 2 — unbounded `_versions/` chain.** `cleanup` version-GC **excludes** the internal tables
  (`all_table_keys` is node/edge-only, `db/omnigraph/optimize.rs`), so `__manifest/_versions/`
  grows without bound (768 objects measured). Lance **lists** that prefix on every open; once large,
  a store like RustFS times out serving the list page → branch ops take minutes or **fail outright**.
  Unfixable via the public CLI today (`cleanup --keep` can't target `__manifest`).

Validated three ways: a multi-agent workflow (readers + adversarial refutation), Lance 7.0.0 source,
and live branch-op probes on a RustFS mirror. **Key Lance fact (closed the open-path investigation):**
on standard S3-class stores (R2/RustFS, not S3 Express) Lance sets `list_is_lexically_ordered = true`,
so it **never** uses `latest_version_hint.json` — it always lists `_versions/`. So the version hint is
**not** a lever; only **bounding the chain (PR1)** fixes the open path. (Corollary: on production R2 the
open is one cheap list page regardless of chain length, so the chain barely affects R2 open latency —
the RustFS *failure* is a RustFS list-page limit; the R2 ~16s write latency is Term-1 fragment
amplification = PR2's target.)

### A.2 What is LANDED on `main`

- **Step 2a** — #291 originally added compaction for all three then-current
  internal tables (`__manifest`, `_graph_commits`, and `_graph_commit_actors`).
  After strand-and-retire removed the two lineage datasets, the live behavior is
  `__manifest`-only internal compaction. A periodically-compacted graph therefore
  keeps the remaining Term-1 flat. (`__manifest` cleanup/version-GC is still open.)
- **Phase 7 / #299** (`1c5cb874`) — graph lineage lives in `__manifest` (`graph_commit` +
  `graph_head:<branch>` rows in the same publish merge-insert; `CommitGraph` is
  now a projection and the former lineage datasets are gone;
  v3→v4 internal-schema migration; schema-version floor). This removed the per-write commit-graph
  scan and closed the manifest→commit-graph atomicity + commit-graph-parent-under-concurrency gaps.
  **This is the base everything below builds on.**

### A.3 What is IN FLIGHT — branch `ragnorc/read-lance-table-docs` = **PR #307** (OPEN, base `main`)

Ten commits ahead of `origin/main`. PR #307 includes the scan-halving work, the fold
correctness fix, and PR2.1's ground-truth cost harness.

1. **PR2 — halve per-write `__manifest` scans** (`4ac3cde4` + tripwire `52a7e0cd`). Three moves:
   - **#1a probe-gate the OCC re-capture** (`occ_snapshot_for_branch`, mirrors the read path's
     `resolve_target_inner`): replace the cold re-scan with a cheap incarnation **probe**; reuse the
     warm coordinator on match, cold-scan only on mismatch.
   - **#1b in-memory post-publish fold** (`fold_inputs` in `publisher.rs`; `PublishOutcome.known_state`):
     build the new `ManifestState` from `existing_versions ∪ pending rows` instead of re-scanning.
   - **#1c projection** on `read_manifest_scan` (drop `base_objects` always, `object_id` off the
     table-state path).
   - Net: per-write `__manifest` scans **4 → 2**; the two inherent publisher scans
     (`load_publish_state` + the `use_index(false)` merge-join) remain O(F).
2. **Fold correctness fix** (`5537cd95` test, `245cb26d` fix) — a reviewer (Cursor Bugbot High +
   Codex P2) caught that `#1b`'s fold dropped a **same-version owner-branch handoff**: a `table_version`
   row UPDATEd in place at the same Lance version with a new `table_branch` (merge-insert `UpdateAll`
   on the deterministic `version_object_id`) was appended after `existing_versions`, and
   `assemble_manifest_state` kept the stale first entry, so the warm coordinator held the wrong fork
   until refresh. Fix: key the fold's version entries by `(table_key, table_version)` so a pending row
   **replaces** the existing one (mirroring `UpdateAll`). Test-first repro in
   `db/manifest/tests.rs::test_post_publish_fold_reflects_owner_branch_handoff` (red→green).
3. **PR2.1 — ground-truth cost harness** (`fd73f01b`, `59d9ff39`, `3cd2b2c1`, `383022e8`, `9f1e5b6e`).
   Rebuilt `tests/helpers/cost.rs` on lance's IO-counted idiom (`incremental_stats()` deltas; one
   `IOTracker` per class). Added `cost_harness(body)` / `GraphIoMeter`: it installs one `__manifest`
   tracker **before the coordinator opens**, so the tracker rides every handle (init + each
   post-publish reassignment at `db/manifest.rs:590`). `manifest_reads` is now **ground truth**
   (handle-age-irrelevant), closing the blind spot where a per-op tracker installed at measure time
   could not see reads on the long-lived warm handle. `last_manifest_reads()` dumps the read log for
   `assert_io_eq!`-style failure diagnostics. Outside `cost_harness`, `measure` falls back to
   fresh-per-op, so `write_cost_s3.rs` is untouched. (Kept the bespoke `PrefixCounter` for the
   opener/scan split — lance does the same with `throttle_store`/`failing_store`, and the
   request-log alternative would couple to unstable debug method-strings.)

### A.4 The accurate measurement (PR2.1's payoff — what it told us)

The old (fresh-only) harness **undercounted writes**: `#1a`'s probe rides the warm handle, and its
reads escaped the per-op tracker (they showed only as `version_probes=1`). Ground truth counts them
and reveals **a write's freshness probe does ~3 `__manifest` object-store RPCs** (a *read*'s probe is
a 0-IO cache hit). So, apples-to-apples (both ground truth), per-write `__manifest` ops:

| | depth 10 | depth 100 | slope |
|---|---|---|---|
| Pre-PR2 (4 cold scans) | 50 | 410 | +4/write |
| Post-PR2 (ground truth) | 28 | 208 | +2/write |

- PR2 roughly **halved** the per-write manifest work **and its growth slope** (+4 → +2/write).
- The **compacted/maintained floor is ~5 RPCs/write, flat in history** — the 3-RPC probe now dominates
  it (it is O(1), not O(F)). So `#1a` made the OCC re-capture O(1), it did not make it free.
- For latency: a periodically-compacted graph has bounded, history-independent per-write manifest
  cost; an unmaintained graph still grows at half the rate (PR1 flattens the residual). The probe and
  RFC #7264 are the levers for the compacted floor. (The harness measures op *count*, the latency proxy
  on object stores; the ~16s R2 figure is the open-path chain = PR1, separate.)
- Regression guard: `write_cost.rs::manifest_reads_capture_warm_probe` (fresh=11 vs ground-truth=14)
  goes red if the ground-truth wiring reverts.

### A.5 What is LEFT (priority order) — Thread B

1. **PR1a — manual `__manifest`-only cleanup** *(available now, no new invariant; HIGHEST priority —
   it is the only thing that fixes the hard open **failures**)*. Add `all_table_keys_internal()` +
   `cleanup_internal_tables()` reusing the generic `cleanup_all_tables` loop (`optimize.rs`); refuse on
   a pending recovery sidecar. Safe **only on a quiesced graph** (no concurrent writers → no Q8
   resurrection race). Shrinks `_versions/` (768 → keep-N). This is RFC **step 2b's available half**.
   Pair with a **V2-naming surface guard** (protects the one-page open fast-path).
2. **PR3 (the available half) — branch-op de-amplification.** Branch **merge** candidate-scoping
   (avoid 3 full cross-branch snapshots + union-all-keys upfront, `exec/merge.rs`); **parallelize** the
   branch-delete loop (`ensure_branch_delete_safe` snapshots every other branch — O(branches)). Each
   per-branch scan is already cheaper post-PR2 (#1c projection).
3. **Design-gated / deferred:**
   - **PR1b — the Q8 durable boundary watermark** for SAFE automated/cadenced GC under live writers
     (Lance version create is a bare `PutMode::Create` with no monotonic guard → a stalled writer can
     resurrect a GC'd version = silent lost write on R2/S3). Invariant-level, partially MTT-redundant.
     **This is the same design point as Thread A's "step 2b / Q8 watermark" in §8.** Design deliberately
     or wait for RFC #7264.
   - **PR3 branch-delete O(1)** — needs a cross-branch dependency index (the `table_branch` dependency
     is genuinely cross-branch with no index today).
   - **PR4 / RFC #7264** — Lance native branch-aware `BatchCreateTableVersions`; manifest read → O(1),
     per-write fragment append gone; retires most of PR1/PR2. Upstream-blocked.
4. **Low-leverage:** ~~retire the vestigial `_graph_commits`/`_graph_commit_actors` datasets~~ **DONE**
   (Phase B of the strand-and-retire work — the tables are gone, branch authority is `__manifest`, the
   `CommitGraph` is a pure `__manifest` projection). Still open: a bitmap index on `__manifest` (no builder
   exists; `use_index(false)` means it can't serve the CAS join anyway — a `graph_head:<branch>` point-lookup
   is the better variant).

### A.6 Critical files (Thread B)

- `db/manifest/state.rs` — `read_manifest_state` / `read_manifest_scan` / `assemble_manifest_state` (the
  shared reduction both the fold and the scan feed).
- `db/manifest/publisher.rs` — `fold_inputs` / `PublishOutcome` / `is_owner_branch_handoff` (publisher.rs:267,
  the same-version handoff the fold must honor) / the merge-insert CAS.
- `db/manifest.rs` — `commit_changes_with_lineage` (adopts the fold; `self.dataset = dataset` reassignment
  at :590, the reason the cost tracker must be installed before open) + the probes.
- `db/omnigraph.rs` — `occ_snapshot_for_branch` (#1a), `resolved_branch_target`, `ensure_branch_delete_safe` (PR3).
- `exec/staging.rs` `commit_all`; `exec/merge.rs` (PR3); `db/omnigraph/optimize.rs` (`all_table_keys`,
  `cleanup_all_tables` — PR1).
- `tests/helpers/cost.rs` (the harness), `tests/write_cost.rs` / `warm_read_cost.rs` / `write_cost_s3.rs`,
  `tests/writes.rs` / `consistency.rs` / `composite_flow.rs` (must stay green).

### A.7 Gotchas (Thread B, learned this cycle)

- **A per-op object-store wrapper cannot see a long-lived handle's reads.** That was the measurement
  blind spot. The fix is to install the tracker before the handle opens (`cost_harness`), not at measure
  time. A write's warm-handle probe is **3 RPCs** that hid behind `version_probes=1`.
- **`cost_harness` must wrap the WHOLE test body** (the graph must open inside it), and the body future
  must be **`Box::pin`-ed** — wrapping a whole test body in another async layer overflows the test
  thread's stack (these cost tests already raise `recursion_limit`).
- **The fold must mirror merge-insert identity.** `version_object_id(table_key, version)` is
  deterministic, so a same-version handoff is an in-place `UpdateAll`; the in-memory fold must key by
  `(table_key, version)` and replace, or the warm coordinator desyncs from a fresh re-scan. The
  byte-identity guard is `writes.rs::post_publish_fold_matches_fresh_reopen`.
- **`lance-io` `test-util`** is enabled in dev-deps (gives `IoStats.requests` + `assert_io_eq!`,
  diagnostics only); production builds exclude dev-deps so they never see it.

### A.8 Immediate next action

The natural next PR is **PR1a** (no design gate, fixes the RustFS open failures). Run and confirm
the relevant test gate before starting or stacking that follow-up.

---

## 0. TL;DR — where we are and what's next

RFC-013 makes the write path fast **and** correct on object storage (217 Lance tables
under one `__manifest` catalog, on R2/S3). It is sequenced as steps; read §9 of the RFC
for the canonical list. Current reality:

**Landed on `main`:**
- **Step 1** — Tier-1 cost gate + the shared `helpers::cost` harness (#288).
- **Step 3a** — opener bypass: write opens go direct (`Dataset::open` by URI + version)
  instead of the Lance-namespace builder (#288). **This already banked the dominant
  depth win** — see §2 below; it reframes everything.
- **Step 2a** — internal-table compaction: #291 originally covered
  `__manifest` plus the two then-live lineage datasets. The current
  post-retirement implementation compacts only `__manifest`. Plus the RFC
  latency-model correction (#292).
- **Step 4 / Phase 7** — graph lineage moved into `__manifest` (#299 `1c5cb874`):
  `graph_commit` + mutable `graph_head:<branch>` in the publish merge-insert,
  with `CommitGraph` projected directly from those rows and no separate lineage
  dataset. **The base for the live branch (§A).**
- **Optimize-vs-write race** — optimize survives a cross-process write race on the
  same table (#297, **LANDED** — origin/main `6d4606a8`; see §6 for why it's not
  redundant with Design A). Step 3b stacks on top of this.

**Open PRs (land these; relationships in §7):**
- **#296** `correctness-by-design-fix` — recovery roll-forward converges on a concurrent
  manifest advance (the fix for the flaky `iss-schema-apply-reopen-recovery-race`).
  **MERGED to main and integrated into this branch** — the converge helper now threads
  Phase-7's manifest-CAS recovery `graph_commit_id` (see `converge_or_defer_roll_forward`).
- **#295** `docs/rfc-013-step-3b` — the step-3b RFC doc.
- **#254** `ragnorc/bug-4-schema-apply-occ` — schema-apply vs optimize false-fail
  (same op-class family as #297, logical side).

**Step 3b is DONE** (capture-once `WriteTxn`, schema-once + open-collapse; see §4) on
`rfc-013-step-3b-writetxn-v2`. **Phase 7 (step 4) has since LANDED on `main` (#299 `1c5cb874`)**
— lineage now lives in `__manifest` (see §A.2). **Next for Thread A: the big one — Design A /
`PublishPlan` unification (step 5)** — see §5, the convergent fix for the bug *class* this area
keeps generating, which also absorbs 3b's deferred session-aware write opens. **Next for Thread B
(the live branch, §A): PR1a** (manual `__manifest` cleanup — fixes the RustFS open failures).

---

## 1. The corrected mental model (read this before touching anything)

Three reframes from the latest cycle that the older RFC prose may not fully reflect:

### 1a. 3a already won the depth fight → the residual is constant-factor + RTT
Before 3a, the write re-opened each table through the lance-namespace builder ~13×, and
that path was **O(depth)** (it re-opened `__manifest` + `list_table_versions` per open —
**not** a Lance back-walk; the root cause was OmniGraph's own namespace round-trips, not
Lance — validated against Lance source). 3a swapped it for the direct opener, which is
**O(1)** (`from_uri(loc).with_version(N)` = arithmetic path + one HEAD). So:

- The dominant **O(depth) data-table** term is **gone**.
- Step 2a flattened the secondary **internal-table** scan term.
- What remains is the **~110-hop serial backbone × RTT + compute** — a constant in
  depth. The latency model is **`wall = (serial_hops + ops/effective_concurrency)·RTT
  + compute`**; on a capped store (R2) the op-count term re-enters wall-clock, on an
  unlimited store it parallelizes away. Measured: prod one-row write 27→15.76s after
  2a; the remaining 15.76s is the serial backbone — **step 3b's target**, not step 2's.
- Step 3b's win is therefore the **call-count/RTT collapse** (redundant opens, the
  flat-46 schema reads), NOT a depth slope. Don't expect a depth-slope improvement from
  3b; gate it on the constant-factor (S3 round-trips), not a curve.

### 1b. Two op classes, two commit models (the §6.6 principle)
Every concurrency bug in this area is **one op class using the other's commit model**:

| class | examples | commutes? | correct commit model |
|---|---|---|---|
| **maintenance** | compaction (`Rewrite`), `optimize_indices` | yes (content-preserving) | Lance native rebase + app reopen/replan on real overlap + **monotonic manifest fast-forward** — no epoch, no read-set |
| **logical mutation** | load / mutate / merge / delete | no (lost-update, write-skew) | strict cross-process OCC: read-set + write-set CAS under the `writer_epoch` fence |

Applying strict OCC + equality-CAS uniformly is the mistake: too strong for maintenance
(false conflicts — #297's bug), too weak for logical cross-process (§6.5 corruption).

### 1c. The root liability (what keeps generating these bugs)
Lance gives **per-table atomic commits** but **no cross-table/cross-step atomicity**, so
every multi-commit op advances per-table Lance HEAD **before** the manifest references it
(the "A-before-B window"). The resulting `HEAD vs manifest` delta is **ambiguous**
(external drift? my own in-flight work? a crashed writer?), and **many uncoordinated code
paths each re-interpret it** (4 writers + the maintenance path + recovery + the write-path
drift guard). Each interpreter is a fresh chance to misclassify. That is the bug class:
- §6.5 cross-process logical corruption,
- #297's own-HEAD-drift misclassification,
- the flaky write-path "HEAD ahead of manifest, run repair" guard,
- the recovery classifier edges.

**The convergent fix is Design A (one publish authority — step 5); Lance MTT eventually
retires the window entirely.** See §5.

### 1d. The second facet: the write base is a stale pin (no probe)
The READ path resolves its base behind a freshness probe (`resolve_target_inner`
omnigraph.rs:~1072 → `probe_latest_incarnation` → `refresh_manifest_only`); the WRITE path
does NOT (`resolved_branch_target` omnigraph.rs:~778 returns the warm `coord.snapshot()` for
the bound branch, no probe). So a long-lived server's write base lags the live manifest. That
single staleness feeds **two distinct failure modes**, both surfaced this cycle:

1. **Stale validation *reads* → integrity under-enforced.** Write-path RI checks read
   committed state off the stale base. 3b's collapse #1 made it worse for edge `@card`:
   `edge_cardinality_read_handle` (mutation.rs:~614) scans the pinned `txn.base` instead of
   live HEAD (was live HEAD pre-3b), so a concurrent edge committed after `txn` capture is
   uncounted → a `@card` max can be exceeded (cursor **High** / codex **P1** on #298,
   **VALID**). **#298 fix: restore the live-HEAD read for that scan** (un-regress; gate-safe —
   the `data_open_count` gate is a node insert) + a deterministic regression test (commit A's
   edge, then B validates → must see A) + correct the wrong "pinned base == live HEAD" doc
   comment (mutation.rs:~605-613, which assumes a single writer). The *structural* liability
   underneath: there is **no unified write-validation read-set** — endpoint
   (`ensure_node_id_exists`, warm `snapshot_for_branch`), cardinality (mutation: pinned
   `txn.base`; loader: warm `snapshot_for_branch` — the SAME check forks per write path),
   commit drift guard (live `fresh_snapshot_for_branch`), and uniqueness
   (`enforce_unique_constraints_intra_batch`, intra-batch only — cross-version uniqueness is a
   documented gap). Three freshness levels chosen ad hoc, none re-validated at commit → the
   §7.1 TOCTOU class, and each new constraint forks the pattern again.

2. **Stale OCC *pin* → false-fail on a maintenance advance.** A served strict update/delete
   pins the stale base version, then false-fails `ExpectedVersionMismatch` after an external
   `optimize` advanced `__manifest` — even though the advance was content-preserving
   compaction the logical write should fast-forward past (invariant 7). It's the **write-side
   mirror of #297/§6.6** (#297 made optimize fast-forward past a logical write; this is a
   logical write that must fast-forward past optimize). A served read clears it (the read
   probes the shared coordinator). Validated repro on prod (omnigraph.ragnor.co) +
   `writes.rs::served_strict_delete_after_external_optimize_advance_auto_refreshes`
   (`#[ignore]` on branch `fix/write-path-stale-view-probe`). **The naive "just probe" fix is
   proven wrong** — a blanket probe silently refreshes past *logical* advances too, breaking
   `consistency::stale_handle_strict_mutation_returns_read_set_changed_then_refresh_retry` (the deliberate
   cross-process lost-update OCC primitive). The fix must **discriminate by op class**.

**Both fold into Design A (step 5), same as §1c.** `open_txn`'s one warm probe makes the base
fresh (absorbs maintenance advances cheaply); the **op-class-aware strict precondition** —
derive from Lance's per-version transaction metadata (all `Rewrite`/`ReserveFragments` =
maintenance → fast-forward the pin; any `Append`/`Update`/`Delete`/`Merge` = logical → fail
loudly; NO parallel marker, invariant 1/15) — is the correctness fence for anything that lands
after. And the §7.1 read-set-in-CAS unifies the validation read-set + re-validates it under the
`graph_head` contention. So **the stale-view false-fail, the cardinality/validation-read-set
liability, and #297's mirror are one bug** (the write base is a stale, un-probed, un-classified
pin) with **one home: the single PublishPlan delta-interpreter** (§1c + §5). Strong corroboration
of Design A — three symptoms, one fix.

---

## 2. Validated facts — do NOT re-derive these

Established this cycle against **Lance 7.0.0 source**
(`~/.cargo/registry/src/index.crates.io-*/lance-7.0.0`) and current engine code. Cited so
you can trust them without re-investigating.

**Lance (upstream):**
- `from_uri(loc).with_version(N).load()` and `checkout_version(N)` are **O(1)** (computed
  V2 path `_versions/{u64::MAX-N:020}.manifest` + one HEAD; no listing/back-walk).
  (`lance-table/src/io/commit.rs` `default_resolve_version`.)
- A shared `Arc<Session>` (`DatasetBuilder::with_session`) warms metadata/index caches
  keyed by `(URI, version, e_tag)`. Caveat: the *first* manifest read on open is uncached
  — the Session warms the *scan/index* metadata, not the first open. **`WriteParams` *does*
  carry a `session` field** (`lance/src/dataset/write.rs`), but it only matters on the
  `WriteDestination::Uri` arm; OmniGraph's staged path always drives off an **already-open
  `Dataset`**, and Lance takes the store/session from that handle. So to attach the shared
  Session to a write base, open read-style (`open_table_dataset` → `from_uri().with_version()
  .with_session()`) and drive the staged write off that handle.
- A held `Arc<Dataset>` at a pinned version is `Send + Sync`, immutable, safe to reuse for
  many scans/count/staged-write base in one txn (OmniGraph's `TableHandleCache` already
  relies on this).
- **No compaction `RetryExecutor`** (only Delete/MergeInsert/Update have one).
  `commit_compaction` commits a fixed `Rewrite` via `apply_commit` direct. In
  `commit_transaction`, a semantic `RetryableCommitConflict` **escapes the retry loop**
  via `?` at `io/commit.rs:979`; the loop only retries the OCC `CommitConflict`
  (`:1096`), and even that re-rebases the *same* transaction (never re-plans). ⇒
  **compaction needs app-level reopen+REPLAN; you cannot "set conflict_retries" and let
  Lance own it.**
- `check_rewrite_txn`: a `Rewrite` rebases **cleanly** past a concurrent `Append`/disjoint
  `Update`/`Delete` (preserving both); only a same-fragment overlap yields a retryable
  conflict. ⇒ the common concurrent insert/update/delete is rebased for free; the app
  retry fires only on real overlap.

**Engine (internal):**
- Read path (post-#268) already has the capture-once machinery: `Snapshot` (`db/manifest.rs`),
  warm `GraphCoordinator` behind a `latest_version_id`/incarnation probe, a held
  `TableHandleCache` keyed `(table,branch,version,e_tag)`, **one shared `Session` per
  graph** (`read_caches.session`). **Writes bypass all of it by construction**
  (`resolved_branch_target` returns `read_caches: None`; the 3a write opener attaches no
  session and opens by latest, not pinned version).
- A single write opens each table **3–4×** (accumulation → staging reopen → commit
  drift-guard → publish prepare), each a fresh cold open. `validate_schema_contract`
  (`db/schema_state.rs`, via `ensure_schema_state_valid`) runs uncached (~3 `read_text`
  + 2 `exists`) at every resolve point (~the flat-46). Both are constant-factor, flat in
  depth — 3b's targets.
- Strict-op guards are the lost-update floor (3 layers: pre-stage `ensure_expected_version`
  `table_store.rs`; commit-time strict drift `exec/staging.rs`; publisher CAS
  `publisher.rs`). Capture-once **supplies** the pinned operand — never remove a guard.
- Fork-on-first-write authority reads (`classify_fork_ref` → `fresh_snapshot_for_branch`)
  must stay **fresh** (not served from a pinned base).
- Cost harness: `helpers::cost` (`measure`/`measure_with_staged`/`IoCounts`/`assert_flat`/
  `local_graph`/`s3_graph`). The schema-once assert can reuse `CountingStorageAdapter`
  (`warm_read_cost.rs::warm_query_validates_schema_contract_once`) with **zero** prod
  change; an open-count assert wants a small `open_count` AtomicU64 in `QueryIoProbes`
  (copy the `probe_count`/`record_probe` pattern). The forbidden-API guard
  (`tests/forbidden_apis.rs`) makes an instrumentation-level counter complete.

---

## 3. The #297 cycle (this branch) — what it is, and the lesson

`fix-optimize-concurrency-race` (5 commits): a CLI `optimize` racing a served write on the
same table failed (Lance Rewrite lost, or the equality-CAS publish lost). Fix: unify both
compaction paths on the internal path's **reopen+replan** shape, with a **two-level retry**
— outer loop reopens+replans on a real Lance overlap; inner Phase-C loop makes the manifest
publish a **monotonic fast-forward** (advance to compacted version `N`, or no-op when the
manifest already moved to `≥ N`), never the strict equality CAS. Sidecar written once;
in-process queue kept as a contention reducer (not the cross-process guard); no `writer_epoch`.

**Two review rounds surfaced two follow-on bugs I introduced with the retry loop** — both
fixed, both regression-tested (own-HEAD-drift via negative control):
1. **Own-HEAD-drift misclassification** (`56d004e0`): the drift guard re-ran every
   iteration and, after a partial Phase-B commit (auto_cleanup strip or compact, then a
   later op conflicts), saw `HEAD > manifest` from *our own* covered work and deleted the
   sidecar + returned `skipped_for_drift` (stranding uncovered drift). Fix: track
   `head_advanced`; the drift guard fires only when `!head_advanced`.
2. **Publish exhaustion spurious error** (`e9d16a2c`): the publish loop returned `Err` on
   its final retry even if the conflict meant a concurrent writer already published `≥ N`
   (postcondition met). Fix: re-check `current >= state.version` on exhaustion.

**The lesson (write it on the wall):** *wrapping a sequence of side-effecting commits in a
retry silently converts every "checked once, before any side effect" precondition into
"re-checked after partial side effects."* That's a distinct bug class; it needs
fault-injection tests **at each commit boundary**, not just end-to-end concurrency tests.
(The `optimize.before_compact` / `optimize.inject_reindex_conflict` failpoints exist for
exactly this.)

**Temporary mechanism flag:** `head_advanced` is an in-memory proxy for "is this HEAD
movement mine." Under Design A the authority answers that from the plan/sidecar **identity**
— so `head_advanced` is the part that gets *replaced*, while the monotonic-publish +
reopen/replan **semantics** are permanent. (Noted in RFC §6.6.)

---

## 4. DONE: Step 3b — capture-once `WriteTxn` (shipped on `rfc-013-step-3b-writetxn-v2`)

**Delivered:** on the **table-touch hot path**, a single `mutate`/`load` validates the schema
contract once at capture and once at the RFC-022 pre-effect gate, with one cheap trailing
identity-marker read fencing capture (never per table), and opens each touched data table
**at most once** — a constant-factor/RTT win (not a depth-slope win;
1a). Two cost gates in `write_cost.rs` lock it (both on a node insert):
`write_schema_io_is_bounded_to_capture_fence_and_effect_gate` (7 `read_text` / 4 `exists`) and
`keyed_insert_opens_table_at_most_once` (`data_open_count <= 1`, was 4). The carrier is the
operation-local `WriteTxn { branch, base, authority, catalog }`; the catalog is built from the
exact accepted IR named by the authority token. It **converges into** step 5's `PublishPlan`.

**Not "once" everywhere (scope, not regression):** edge endpoint / cardinality RI validation
(`ensure_node_id_exists`, the loader's RI + cardinality) still resolves through
`snapshot_for_branch` and re-validates the schema — and reads **warm**, not live. Threading
`txn.base` there to make it "once" would re-introduce the stale-read class the #298 cardinality
fix removed (it now reads live HEAD). Doing schema-once *and* fresh reads for those validations
needs the unified, re-checked read-set — **step 4 §7.1** (§1d). So #298 **un-regresses
cardinality only; it does not close write-validation freshness.** No edge-insert/load schema-once
gate yet (only the node gates above).

Commits (off merged-#297 main):
- **Stage 0** — scope `open_count` → `data_open_count`/`internal_open_count` by URI class
  (historically, before lineage-table retirement, `open_dataset_tracked` also opened
  `__manifest`/`_graph_commits`, so the raw counter conflated them and the gate was
  unreachable). Re-baselined RED 4.
- **Commit A (schema-once)** — capture `txn` once at entry (the single validation); the 4
  validation sites collapse: S1 (entry `ensure_schema_state_valid`) removed; S3a
  (`open_for_mutation_on_branch`) + S3b (`prepare_updates_for_commit`) source `txn.base`;
  S4 (`commit_all`) uses new `fresh_snapshot_for_branch_unchecked` (the OCC manifest re-read
  minus the schema re-validation). `fresh_snapshot_for_branch{,_unchecked}` now read the
  manifest directly via `ManifestCoordinator` (drops a spurious commit-graph `exists` probe;
  same `Snapshot`).
- **Commit B (open collapse 4→1)** — #1 accumulation open ELIMINATED (the node path discarded
  the handle; read `txn.base.entry().table_version`); #2 staging open KEPT (the one open);
  #3 commit drift-guard reads live HEAD via `entry.dataset.dataset().latest_version_id()` (a
  cheap manifest-pointer probe off the staged handle, not a fresh open); #4 index build reuses
  the `commit_staged` handle threaded through `CommittedMutation`/`prepare_updates_for_commit`.
- **Commit B.1 + cleanup** — named the two positional returns (`OpenedForMutation`,
  `CommittedMutation`) + a `debug_assert` pinning the open-skip contract; **removed the
  unearned `WriteTxn.session` field** (the collapse uses skip/probe/reuse, not a session).

**RFC §4.1 corrections — how they resolved:**
1. *Thread the evolving handle, not a version-keyed cache* → realized as collapse #4 (carry
   the `commit_staged` handle forward into the index build).
2. *Don't forbid re-resolution* → honored: the commit-time OCC re-read
   (`fresh_snapshot_for_branch_unchecked` — fresh manifest, only schema-revalidation dropped)
   and the fork-authority reads stay fresh.
3. *Minimal carrier* → `WriteTxn { branch, base }` (even the `session` from the original
   sketch was dropped as unearned).

**Deferred to step 5 (NOT in this PR):** session-aware write base opens. The one remaining
open (#2) stays a HEAD open; warming the shared `Session` across writes is an object-store
(S3) phenomenon invisible on local FS, so it earns its own `write_cost_s3.rs` gate in step 5,
where `txn` becomes the non-optional publish carrier. No new concurrency test was needed here:
#2 stays a HEAD open (no pinned+session base introduced), so the publisher CAS + #3 live-HEAD
probe fences are unchanged (covered by the green `writes.rs`/`consistency.rs`).

**Guardrails (don't regress):** schema validation is deliberately uncached for drift
detection — collapse to 1 *per write*, never cache across writes on a long-lived handle
(`lifecycle::long_lived_handle_rejects_schema_*`). The commit-time fresh read is OCC
machinery, not redundancy. Keep all 3 strict-op guards. Keep fork-authority reads fresh.
Pin the *correct* branch (server-bound-to-main writing a feature branch falls to a fresh
open). A branch `rfc-013-step-3b-writetxn` exists off an earlier main; rebase onto the
post-#297 main before starting.

---

## 5. Design A — the `PublishPlan` unification (step 5) = the convergent fix

**This is the real fix for the bug class in §1c.** Collapse the four hand-rolled writers +
the maintenance path into **one `publish(txn, plan)` authority** where the CAS + bounded
retry is **unconditional and unbypassable** (no caller can "hold the queue → skip the CAS").
Properties:
- **One interpreter of the `HEAD vs manifest` delta** — and "is this my work?" is answered
  by the plan/sidecar **identity**, not a re-derived comparison. The own-HEAD-drift bug, the
  §6.5 writers, the write-path guard — all close *by construction*.
- **Recovery = the same `PublishPlan` re-applied** — the crash-recovery interpreter and the
  live interpreter become the same code (`iss-merge-recovery-partial-rollforward` gone).
- Each `TableAction` commits by its **class** (§1b): `Rewrite` = maintenance (Lance rebase
  + reopen/replan + monotonic fast-forward, **no epoch**); load/mutate = logical (strict OCC
  + `writer_epoch`).

**Why it composes with Lance MTT (don't over-build):**
- The **unification itself is convergent** — when MTT lands, it slots *underneath* the same
  authority; nothing wasted. Build this.
- The **`writer_epoch`** is the one MTT-redundant piece (MTT's commit-handler lease subsumes
  a cross-process fence). Build it *last and minimally*, gated on actually deploying
  multi-writer topologies. Per the deny-list, don't reimplement what the substrate will own.

**Sequencing judgment (this cycle's strongest signal):** the bug density here (this PR alone
= 3 review rounds, all "a writer re-interprets the delta") means the current N-writers interim
is high integrated-over-time liability. **Consider pulling the *convergent half* of step 5
(the single authority + recovery-as-plan) forward — possibly ahead of 3b** — because it stops
the bug class rather than patching instances. #297 + #254 are the *de-risking inputs*: they
validate the maintenance-class and logical-class commit models in isolation first, so Design
A implements a known spec rather than designing under refactor pressure. Do NOT build more
substrate-shaped scaffolding (custom WAL / job queue / second coordination table) to paper
over the window — strictly higher liability than either Design A or waiting for MTT.

**Deeper-than-A (post-MTT or as Lance exposes uncommitted variants):** all-uncommitted-fragments
+ one manifest commit would shrink the A-before-B window itself. Lance still exposes no
uncommitted `compact_files` / `optimize_indices`; beta.21 does expose the full-table vector
stage OmniGraph needs, while #6666 remains about generic multi-segment publication (delete
#6658 shipped). Track the maintenance primitives; migrate vector through the reconciler.

### 5.1 Step-5 design constraints inherited from the #295 spec review
3b shipped a **minimal** `WriteTxn { branch, base }` (schema-once + open-collapse via
eliminate/probe/thread) and **deferred** the full §4.1 opener-unification — the pinned-base
opener, the shared-`Session` open, the write-local **handle cache**, and the strict-op
conflict-timing move — to step 5. So the greptile-bot comments on the #295 *spec* were **moot
for #298** (which built none of those constructs) but are **load-bearing constraints for step
5** when it builds them. Bank them:
1. **Handle cache must be `Send + Sync`** (`Mutex<HashMap<…, Dataset>>`, not `RefCell`) if
   `WriteTxn::open(&self)` is shared across concurrent stage futures — a `RefCell` compiles
   but panics when two stages poll. Or make it `&mut self` (no parallel-stage sharing). This
   is the deny-list "in-process-only `Dataset` impls — `Send + Sync`" item.
2. **The strict-op timing move needs an explicit retry contract.** If step 5 moves
   strict-op conflict detection from open-time `ensure_expected_version` to commit-time CAS
   (the §4.1 pinned-base design), it MUST specify: the txn is **discarded after any commit**
   (success or conflict — the handle cache is commit-invalidated), and the retry **re-opens a
   fresh `WriteTxn` at the new HEAD** (never re-stages against the stale pinned base — that
   reproduces the lost-update). **This is the same retry/refresh contract as the stale-view
   false-fail (§1d.2)** — the op-class-aware precondition + "fresh base on retry" are one
   design point. Today (#298) strict ops keep open-at-HEAD + `ensure_expected_version`, so the
   contract is unchanged; step 5 owns it the moment it pins strict reads to the base.
3. **The opener-equivalence test must be non-trivial.** A differential test that only passes
   when `HEAD == base` proves nothing about pinning. To actually prove "`WriteTxn::open`
   returns the pinned base, not HEAD," the test must **advance the branch HEAD externally
   (direct Lance write), then assert the txn open still reads the base version** — and that a
   strict write then fails `ExpectedVersionMismatch` at commit (verifying the timing move).

---

## 6. Why #297 is still needed even if you do Design A
- Design A **relocates** #297's maintenance-class commit logic into the authority's
  `TableAction::Rewrite` path; it does not eliminate it. #297 is the *validated spec + tests*.
- The two regression tests + §6.6 are the **contract** Design A must keep green.
- The prod bug is **live**; Design A is the largest write-path change in the RFC. Don't hold a
  correctness fix hostage to a big refactor, and don't do a big refactor under bug-fix urgency.
- Genuinely throwaway under Design A: only the loop's *location* + the `head_advanced` proxy
  (~a dozen lines). Everything else relocates or persists. **#297 LANDED.**

---

## 7. Open PRs and their relationships
- **#297** — maintenance-class fix (optimize vs write). **LANDED** (origin/main `6d4606a8`);
  step 3b stacks on it.
- **#254** — logical-class fix (schema-apply vs optimize false-fail). Same op-class family;
  both are de-risking inputs for Design A's per-class commit models.
- **#296** — recovery roll-forward converges on concurrent manifest advance. The fix
  for the flaky `iss-schema-apply-reopen-recovery-race`. It touches `recovery.rs` and is
  *aligned* with #297's "postcondition is the state, not winning the CAS" principle. **#296
  landed on main first and is merged into this branch:** the converge helper
  (`converge_or_defer_roll_forward`) was reconciled with Phase-7's manifest-CAS roll-forward —
  on convergence the audit references the winner's folded `graph_commit_id` (the current
  `graph_head`), not a freshly minted one.
- **#295** — the step-3b RFC doc (apply §4's three corrections to it).

---

## 8. Remaining RFC steps after 3b (RFC §9 is canonical)
- **#298 follow-up (do on the 3b PR, before merge): the edge-`@card` stale-read regression**
  (§1d.1). Restore the live-HEAD cardinality scan, add the deterministic regression test, fix
  the wrong doc comment. Small, gate-safe, un-regresses an integrity check (invariant 9). The
  residual concurrent TOCTOU is the §7.1 gap (step 4) — un-widen here, don't over-reach.
- **Step 4 / Phase 7** (`iss-991`): **LANDED on `main` as #299 (`1c5cb874`).** Lineage now lives
  in `__manifest` (`graph_commit` + mutable `graph_head:<branch>` in the same merge-insert;
  `CommitGraph` is projected from those rows and the old lineage dataset is
  gone). Removed the per-write `commit_graph.refresh`; closed the
  manifest→commit-graph atomicity + commit-graph-parent-under-concurrency gaps. *(Historical note,
  kept for the §7.1 framing it carried:)* it
  carries the §7.1 *concurrent* write-skew fix (needs the `graph_head` contention row) —
  **frame §7.1 as "unify the entire write-validation read-set" (endpoint + cardinality +
  cross-version uniqueness), not merely "add `graph_head`"** (§1d.1): the bespoke
  `edge_cardinality_read_handle` and the mutation-vs-loader freshness fork dissolve into one
  pinned read-set re-validated under the `graph_head` contention, or the liability survives as
  a second special-case.
- **Step 5 / Design A** — §5 above. **Acceptance item: the served-strict-write stale-view
  false-fail** (§1d.2) — the op-class-aware precondition + `open_txn` probe. The contract is
  two tests passing *together*: un-ignore
  `writes.rs::served_strict_delete_after_external_optimize_advance_auto_refreshes` (goes green)
  *while* `consistency::stale_handle_strict_mutation_returns_read_set_changed_then_refresh_retry` stays green
  (maintenance fast-forwards; logical fails loudly). Self-contained enough to ship standalone
  like #297 if prod pain is acute; otherwise fold into the single PublishPlan delta-interpreter.
- **Step 2b** — internal-table cleanup + the Q8 monotonic watermark (a Lance boundary tag).
  Deferred: only the secondary version-count/space term, touches the read/open path, and is
  MTT-redundant. Land when version-count cost bites.
- **§7.1 sequential write-skew** (`iss-overwrite-orphans-committed-edges`) — inbound-RI
  validation on node removal; independent, ships anytime.
- **#20** — the prod per-write `storage.ops` span metric (RFC §5.3), still owed.
- Branch ops: Lance `Clone` for create (`iss-691`).

---

## 9. Gotchas / traps (learned the hard way)
- **In-process queue ≠ cross-process lock.** Any "I hold the queue → skip the retry/CAS"
  reasoning is a bug across processes. This is the recurring trap.
- **Monotonic publish must be `≥`-conditional, never "no assertion."** The `__manifest`
  merge-insert is unconditional `UpdateAll` keyed on `object_id` (`publisher.rs:379`), so
  the equality (or monotonic) pre-check is the *only* guard — dropping it lets `UpdateAll`
  regress a newer version = lost write.
- **The drift guard interprets an ambiguous delta.** Re-evaluating it in a retry over
  self-mutated state is how #297's follow-on bug happened. Gate any HEAD-vs-manifest
  interpretation on "have *we* committed yet."
- **`compact_files` fires Lance's auto_cleanup GC hook** (commits with
  `skip_auto_cleanup=false`, no override) — optimize strips stale `lance.auto_cleanup.*`
  config before compacting to stay non-destructive on upgraded graphs. The strip is a
  separate commit (relevant to the partial-commit retry trap).
- **Lance rebases the common concurrent case for free** — so the data-table conflict usually
  surfaces as the manifest fast-forward, not a Lance error. The Lance-Rewrite-overlap path is
  rare and needs failpoint injection to test.

---

## 10. Verification (the gate)
- `cargo test --workspace --locked` — the canonical gate (matches CI).
- `cargo test -p omnigraph-engine --features failpoints --test failpoints optimize` —
  the optimize concurrency/recovery tests.
- `cargo test -p omnigraph-engine --test write_cost` / `write_cost_s3` (bucket-gated) —
  cost gates (3b adds the schema-once + open-count asserts here).
- `cargo test -p omnigraph-engine --test maintenance` — optimize/repair/cleanup.
- Re-read [`invariants.md`](invariants.md), [`lance.md`](lance.md), [`testing.md`](testing.md)
  before each change (always-on requirement).

Lance source for re-validation:
`/Users/ragnor/.cargo/registry/src/index.crates.io-*/lance-7.0.0` (key files: `io/commit.rs`,
`io/commit/conflict_resolver.rs`, `dataset/optimize.rs`, `dataset/write/retry.rs`,
`dataset/builder.rs`).
