# Handoff: finishing RFC-013 (write-path latency + correctness)

**Status:** living handoff. **Source of truth is [`rfc-013-write-path-latency.md`](rfc-013-write-path-latency.md)** —
this doc is the *current-state map + the decisions/validation from the latest work cycle
+ the concrete next actions*. When they disagree, the RFC wins (and fix this doc).

**Audience:** the engineer/agent who picks up RFC-013 next.

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
- **Step 2a** — internal-table compaction: `optimize` now compacts `__manifest` /
  `_graph_commits` / `_graph_commit_actors` (#291). Plus the RFC latency-model
  correction (#292).

**Open PRs (land these; relationships in §7):**
- **#297** `fix-optimize-concurrency-race` — optimize survives a cross-process write
  race (the work this cycle produced). **Merge it** (see §6 for why it's not redundant
  with Design A).
- **#296** `correctness-by-design-fix` — recovery roll-forward converges on a concurrent
  manifest advance (this is the fix for the flaky `iss-schema-apply-reopen-recovery-race`).
- **#295** `docs/rfc-013-step-3b` — the step-3b RFC doc.
- **#254** `ragnorc/bug-4-schema-apply-occ` — schema-apply vs optimize false-fail
  (same op-class family as #297, logical side).

**Next step to implement: Step 3b (capture-once `WriteTxn`)** — see §4. After that:
Phase 7 (step 4), then the big one — **Design A / `PublishPlan` unification (step 5)** —
see §5, which is the convergent fix for the bug *class* this area keeps generating.

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

## 4. NEXT: Step 3b — capture-once `WriteTxn` (designed + validated this cycle)

**Goal:** collapse the redundant per-write opens (3–4×/table → 1), warm the shared
`Session` on write opens, and validate the schema contract **once** per write — making
mid-write re-resolution unrepresentable (invariant 3). **Constant-factor/RTT win, not a
depth-slope win** (1a).

**Scope:** the hot writers only — `load_as` + `mutate_as` (they share `MutationStaging`).
NOT the 4-writer→`PublishPlan` collapse (that's step 5). NOT Phase 7. NOT recovery-off-hot-path.

**Three design corrections to RFC §4.1 (apply these — the old sketch is wrong on them):**
1. **Thread the evolving handle, not a version-keyed `HandleCache`.** The opens aren't N
   reads of one immutable version; they're the table at *successive HEADs* (staging
   commits, index build commits). Carry the handle Lance returns from each commit forward;
   don't re-open by key. (The read-side version-keyed cache is correctly NOT reused for
   write handles.)
2. **Drop "re-resolution unrepresentable" as the invariant.** Three fresh reads are
   load-bearing correctness machinery, not waste: the commit-time `fresh_snapshot_for_branch`
   (cross-process OCC), the post-commit HEAD re-derive (HEAD moved between stages), and the
   fork-authority reads. Model "pinned read **base** for the pre-first-commit phase +
   named commit-protocol re-reads," not "forbid all re-derivation."
3. **Ship a minimal carrier**, not the §4.1 bundle: `WriteTxn { branch, base: Snapshot,
   session, schema_validated_once }`. It **converges into** step 5's `PublishPlan` (step 5
   grows it), so it's lower-liability than scattering localized fixes.

**Likely-eliminable open:** the accumulation open (#1) may use its `Dataset` only for
`ds.version()`, and the version is already in `base.entry(table).table_version` — so it may
be *deleted*, not cached (verify during impl).

**Implementation sequence (test-first):**
1. **Cost gate (red).** Add to `write_cost.rs`: `validate_schema_contract_calls == 1`
   (reuse `CountingStorageAdapter`, zero prod change) and `opens ≤ |touched_tables|` (add
   `open_count` to `QueryIoProbes`). Red against current code.
2. **Thread the `Session` into the write opener** (`open_dataset_head_for_write` opens
   read-style by pinned version with `Some(&session)`).
3. **Introduce `WriteTxn`**, captured once at entry of `mutate_as`/`load_as`, threaded
   through staging; serve opens from one forwarded handle; re-probe version at the commit
   drift-guard.
4. **Collapse schema validation to once-per-write** (single point:
   `resolved_branch_target`/`fresh_snapshot_for_branch` both call `ensure_schema_state_valid`
   first-line).
5. Gate green; `cargo test --workspace --locked`; S3 round-trip check on `write_cost_s3.rs`.

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
+ one manifest commit would shrink the A-before-B window itself, blocked today by Lance not
exposing uncommitted variants for `compact_files` / `optimize_indices` / vector index (#6666
open; delete #6658 shipped). Track, don't build yet.

---

## 6. Why #297 is still needed even if you do Design A
- Design A **relocates** #297's maintenance-class commit logic into the authority's
  `TableAction::Rewrite` path; it does not eliminate it. #297 is the *validated spec + tests*.
- The two regression tests + §6.6 are the **contract** Design A must keep green.
- The prod bug is **live**; Design A is the largest write-path change in the RFC. Don't hold a
  correctness fix hostage to a big refactor, and don't do a big refactor under bug-fix urgency.
- Genuinely throwaway under Design A: only the loop's *location* + the `head_advanced` proxy
  (~a dozen lines). Everything else relocates or persists. **Merge #297.**

---

## 7. Open PRs and their relationships
- **#297** (this) — maintenance-class fix (optimize vs write). Merge.
- **#254** — logical-class fix (schema-apply vs optimize false-fail). Same op-class family;
  both are de-risking inputs for Design A's per-class commit models.
- **#296** — recovery roll-forward converges on concurrent manifest advance. This is the fix
  for the flaky `iss-schema-apply-reopen-recovery-race` (the handoff in
  `handoff-schema-apply-recovery-flake.md`). It touches `recovery.rs` and is *aligned* with
  #297's "postcondition is the state, not winning the CAS" principle — reconcile the monotonic
  publish with #296's converge helper if #296 lands first.
- **#295** — the step-3b RFC doc (apply §4's three corrections to it).

---

## 8. Remaining RFC steps after 3b (RFC §9 is canonical)
- **Step 4 / Phase 7** (`iss-991`): lineage into `__manifest` (publish `graph_commit` +
  mutable `graph_head:<branch>` in the same merge-insert; `_graph_commits` becomes a
  projection). Removes the per-write `commit_graph.refresh`; closes the manifest→commit-graph
  atomicity + commit-graph-parent-under-concurrency gaps. **Hard prereq: step 2 (done).**
  Carries the §7.1 *concurrent* write-skew fix (needs the `graph_head` contention row).
- **Step 5 / Design A** — §5 above.
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
