# Handoff: flaky schema-apply → reopen recovery race

**Type:** bug investigation handoff (not yet fixed)
**Status:** root-caused to a layer + hypothesis; exact mechanism and fix NOT yet validated
**Severity:** medium — flaky CI; a real (rare) schema-apply-then-reopen failure under load
**Scope:** pre-existing on `main`; **independent of** RFC-013 step 2 (internal-table
compaction, PR #291) and step 3a (#288) — those paths never touch schema apply or
the recovery sweep, and the full `--workspace` gate passes clean on a re-run.

> Do **not** "fix" this by changing the test to use a single handle. That was
> empirically shown to *reduce but not eliminate* the flake (see Experiments), so it
> would mask a real product race. This is a correct-by-design fix in the engine, not
> a test edit.

---

## 1. Symptom

The test
`crates/omnigraph-server/tests/schema_routes.rs::schema_apply_route_hard_drops_property_with_allow_data_loss`
intermittently fails. The HTTP schema apply **succeeds** (`applied == true`); the
*subsequent* `Omnigraph::open(graph)` (which the test does to verify the catalog)
panics on `.unwrap()` with:

```
OmniError::Manifest(Conflict,
  "stale view of node:Person: expected manifest version 5 but current is 7",
  ExpectedVersionMismatch { expected: 5, actual: 7 })
```

The values (5, 7) vary; the shape is always "recovery roll-forward expected version
N, manifest is at M > N." It is raised from the **open-time recovery sweep**, i.e.
inside `Omnigraph::open`, not from the apply itself.

---

## 2. Reproduction

- **Needs sibling-test parallelism (CPU contention).** Running the target test
  *alone* is rock-solid (0/20 failures). The flake only appears when other tests in
  the same binary run concurrently and perturb the timing inside the apply→reopen
  sequence.
- Fast repro loop (≈13–40% per run):
  ```bash
  cargo test -p omnigraph-server --test schema_routes --no-run
  for i in $(seq 1 15); do
    cargo test -p omnigraph-server --test schema_routes 2>&1 \
      | grep -q "schema_apply_route_hard_drops_property_with_allow_data_loss ... FAILED" \
      && echo "iter $i FAIL"
  done
  ```
- It originally surfaced in a full `cargo test --workspace` run (max parallelism).
- Each test uses its own `tempfile::tempdir()`, so this is **not** cross-test shared
  state — it's a timing race inside one test's own graph.

---

## 3. Experiments run (the discriminating evidence)

Each variant was stress-run under the full `schema_routes` suite (parallel siblings):

| Variant | Flake rate |
|---|---|
| Target test in isolation (no sibling parallelism) | **0/20** |
| **Control** — as written (server handle + out-of-band `Omnigraph::open` load + reopen) | 6/15 ≈ 40% |
| Drop the live server handle (`drop(app)`) before the reopen | 4/15 ≈ 27% |
| Remove the out-of-band separate-handle load | 2/15 ≈ 13% |
| Remove the load **and** drop the server handle (≈ single-handle) | 8/20 ≈ 40% |

**Interpretation:**
- It is **concurrency-triggered**, not a topology bug: 0% isolated, flaky under
  parallel load.
- **No single factor eliminates it.** Removing the out-of-band load roughly halves
  the rate (it amplifies the race) but leaves a ~13% base. Dropping the live server
  handle does not clearly help. So the "single-handle test" patch is a **band-aid**,
  not the fix.
- The residual base rate with the out-of-band load removed means there is a real
  race in the **schema-apply → reopen → recovery** path itself.

Caveat on the experiments: `drop(app)` may not synchronously tear down the server's
engine handle (it can be held by an `Arc`/spawned task), so the "single-handle"
rows are not airtight. This is one of the things to validate (§6).

---

## 4. Root-cause hypothesis (NOT yet proven)

The failing path is the **open-time recovery sweep's roll-forward** raising
`ExpectedVersionMismatch` from the publisher's `check_expected_table_versions`.

The hard-drop schema apply (`allow_data_loss=true` → `DropMode::Hard`) is a
**multi-step migration**: it performs several Lance commits + `__manifest` publishes,
advancing `node:Person`'s manifest version across multiple versions (e.g. 5 → … → 7).
To be crash-safe across the Lance-HEAD-before-manifest-publish gap, schema apply
writes a **recovery sidecar** (`__recovery/{ulid}.json`) pinning per-table
`expected_version` / `post_commit_pin` before its Phase B.

Hypothesis: under CPU contention, the timing of (a) the migration's multi-version
advancement, (b) the sidecar's Phase-D deletion, and (c) a later/over­lapping
`Omnigraph::open` recovery sweep interleaves such that the recovery roll-forward
reads a sidecar whose pinned `expected` is **stale relative to a manifest that
legitimately advanced several versions**, and **re-publishes at the stale `expected`
instead of recognizing the migration already completed** → `expected 5, actual 7`.

In other words: the recovery classifier / roll-forward likely does not correctly
handle a table whose manifest is **already past `post_commit_pin`** by more than one
step (multi-step migration), or a sidecar whose operation has already fully
committed. The single-step assumption baked into the Optimize-style pin
(`post_commit_pin = expected_version + 1`) may not generalize to multi-commit schema
migrations.

---

## 5. Likely solution (correct-by-design, surgical)

Make the **open-time recovery classifier idempotent against a manifest that advanced
past the sidecar's pin**:

- If the table's current manifest/Lance version is already `>= post_commit_pin`
  (operation completed, possibly across multiple versions), classify it as
  *already-rolled-forward / completed* (the `RolledPastExpected` family) and **delete
  the sidecar without republishing** — never attempt a publish at the stale
  `expected`.
- Ensure the schema-apply sidecar records a pin that the classifier can interpret for
  a **multi-step** migration (a range / "completed at or beyond" semantics), not a
  strict single-step `expected + 1`.

This also hardens *real* crash recovery for multi-step schema apply (not just the
test), and is small + local to `recovery.rs` (+ possibly the schema-apply sidecar
write in `schema_apply.rs`). It does **not** rearchitect recovery.

Per repo rule 12 (test-first for bug fixes): land a **deterministic** repro first —
ideally a failpoint that forces the interleaving (pause after the migration's commits
but before sidecar delete, then run an open) so the red→green is reliable, not a
stress-loop probability. See the `failpoints.rs` pattern + the schema-apply failpoints
already in the tree.

---

## 6. What MUST be validated before fixing

1. **Which sidecar is being rolled forward?** Confirm it is the *schema-apply*
   sidecar (vs the out-of-band `load`'s sidecar, vs another writer). Instrument /
   log the sidecar `operation_id`, `kind`, and `SidecarTablePin` at the point the
   recovery sweep raises the error.
2. **The exact classifier path.** Trace which `TableClassification` arm the failing
   table hits (`recovery.rs::classify_table`, ~L600) and which roll-forward call
   raises `ExpectedVersionMismatch` (`heal_pending_sidecars_roll_forward` ~L761,
   `roll_forward_all` ~L1215, `restore`+publish ~L1275). Confirm it is the
   multi-step-advanced / already-completed case being mishandled.
3. **Is `post_commit_pin = expected + 1` the bug?** Verify the hard-drop migration
   advances `node:Person` by **>1** version, and that the sidecar pins a single-step
   `+1`, so the classifier can't recognize completion at +2.
4. **Engine-level reproduction (no server).** Build a deterministic engine-level
   repro: persistent handle applies a multi-step hard-drop, then a fresh
   `Omnigraph::open` — ideally with a failpoint forcing the interleave — to confirm
   the bug is in the engine recovery path and not server-specific (runtime, handle
   lifecycle). The current evidence is server-test-only.
5. **Is the out-of-band load *necessary or only amplifying*?** Confirm the ~13% base
   rate (load removed) is the same root cause, not a second distinct race. If the
   load is required, the bug is specifically about a second writer's version
   advancement; if not, it's purely intra-apply.
6. **`drop(app)` cleanliness.** Verify whether the server's engine handle is truly
   gone after `drop(app)` (it may be `Arc`-held). If not, the "single-handle"
   experiments don't isolate the live-handle factor and should be redone with a
   genuinely single-handle setup.

---

## 7. Relationship to Lance MTT

This bug lives in the **recovery-sidecar roll-forward**, which exists only to bridge
the Lance-HEAD-before-manifest-publish gap in omnigraph's faked multi-table
atomicity. `invariants.md` already calls recovery sidecars "scaffolding to remove
once the substrate closes the gap." Lance **MTT** (native atomic multi-table commits,
RFC §8 / lance#7264) closes that gap → retires the sidecar → **eliminates this bug
class.**

Implications:
- **Don't wait for MTT** — it is the "strategic exit, not a current dependency,"
  uncertain and far off, and this bug is live now.
- **Don't over-invest** — keep the fix surgical (classifier idempotency), because the
  whole sidecar layer is MTT-disposable. A surgical fix retires cleanly with the
  layer; a recovery rearchitecture would be throwaway.

---

## 8. Key pointers

- Failing test: `crates/omnigraph-server/tests/schema_routes.rs`
  → `schema_apply_route_hard_drops_property_with_allow_data_loss` (~L777,
  `#[tokio::test(flavor = "multi_thread")]`).
- Error type: `OmniError::Manifest` / `ManifestConflictDetails::ExpectedVersionMismatch`
  (`crates/omnigraph/src/error.rs`); raised by `check_expected_table_versions`
  (`crates/omnigraph/src/db/manifest/publisher.rs`, ~L356).
- Recovery sweep + classifier: `crates/omnigraph/src/db/manifest/recovery.rs`
  — `TableClassification` (~L335), `classify_table` (~L600), roll-forward
  (`heal_pending_sidecars_roll_forward` ~L761, `roll_forward_all` ~L1215, restore +
  publish ~L1275).
- Schema-apply sidecar write: `crates/omnigraph/src/db/omnigraph/schema_apply.rs`
  (the `SidecarKind` schema-apply pins; `db.coordinator.write().refresh()` ~L692).
- Open entry point that runs the sweep: `Omnigraph::open` (read-write mode) →
  `db/manifest/recovery.rs` sweep.
- Repro: §2 above. Stress under `schema_routes` suite parallelism; 0% isolated.

---

## 9. Suggested next steps

1. Add tracing at the recovery roll-forward error site (sidecar kind/id, pins,
   observed vs expected) and capture a failing run (§6.1, §6.2).
2. Reproduce deterministically at the engine level with a failpoint (§6.4) — this is
   the red test (rule 12).
3. Implement the classifier-idempotency fix (§5) in a separate commit; confirm
   red→green and that the stress loop goes to 0 failures over ≥50 iterations.
4. Keep it a standalone PR (not bundled with RFC-013 follow-ons).
