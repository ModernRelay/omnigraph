# Experiments 2.1–2.4 — DEFERRED (rationale)

**Ticket:** MR-925 §2 (lower-priority experiments).
**Date:** 2026-05-12.

The ticket §0.3 acceptance criteria require all **seven** high-priority
experiments (§1.1–§1.7) and at least **six** reference-system code-dives
(§3.1–§3.6) to be complete before posting the rollup comment. §2.1–§2.4
are explicitly framed as "calibrations and incremental validations that
**don't gate the RFC** but are worth doing" and "**can run during Phase 0**."

This writeup explicitly defers all four §2.x experiments with rationale,
so the next agent (or Phase-0 owner) doesn't have to re-discover the
deferral decision.

---

## 2.1 `scan_by_key_set` extended benchmark — DEFER

**Reason:** MR-376 already validated 72× speedup at hop-1 / 100k nodes on
local FS. The extension matrix in §2.1 (cold S3 vs warm local; selectivity
sweep; |keys| / |dataset| ratio sweep; BTREE-routed vs direct
`Dataset::take`) is **calibration**, not capability gating. The §5.3 cost
gate in MR-737 can ship without these numbers; we just won't have a
hard-tuned threshold for "when should we prefer scan-by-key-set vs
re-scan" until they're collected.

**When to run:** During Phase 0 MR-737 §5.3 implementation, before
landing the cost-gate parameter. Estimated 2 days. Owner: same person
implementing §5.3.

**Risk of deferring:** Low. The cost gate has a sensible default (always
prefer scan-by-key-set unless |keys| / |dataset| > 0.1); the calibration
just tightens it.

---

## 2.2 `Hash([key], N)` partitioning elimination — DEFER

**Reason:** Validates DataFusion's `EXPLAIN` shows `RepartitionExec`
elimination for capability-advertised plans. The capability advertising
in MR-737 §5.6 is a quality-of-life optimization, not a correctness
requirement. We can ship §5.6 without this validation and add it as a
follow-up.

**When to run:** During Phase 0 MR-737 §5.6 implementation, half-day spike.

**Risk of deferring:** Low-medium. If DataFusion's optimizer does NOT
honor our capability advertisements, the perf impact is a redundant
`RepartitionExec` insertion — measurable in `EXPLAIN ANALYZE` but not a
correctness issue. The risk is sub-optimal partitioning, not wrong results.

---

## 2.3 Extension-rate propagation through `StatisticsRegistry` — DEFER

**Reason:** Validates the §5.7 cost-model plumbing for custom column
statistics. The cost model itself can ship with default statistics (no
custom registry); the registry is an extension point for **better**
cost choices, not **correct** cost choices. Deferring keeps §5.7's
v1 narrower without breaking it.

**When to run:** When MR-737 §5.7 (cost-model surface) is being
implemented — likely Phase 1, not Phase 0. Estimated 1 day.

**Risk of deferring:** Low. Default cost models from `JoinExec`,
`HashJoinExec`, etc. are battle-tested; custom column statistics are
incremental.

---

## 2.4 DataFusion API churn audit (47 → 53) — DEFER

**Reason:** Calibrates §11 (Risk) and informs **upgrade-cycle budget**,
not Phase-0 design. Phase 0 pins to DataFusion 52.5 (the substrate
pin we validated in §1.3 and §1.5). Knowing the breakage rate for
future upgrades is a maintenance-cost input, not an entry-criterion.

**When to run:** Before any DataFusion minor-version bump after Phase 0
ships. Estimated 1–2 days. Owner: the engineer planning the bump.

**Risk of deferring:** Zero for Phase 0. The audit is for future
planning; Phase 0 doesn't care about DF 47 or DF 53, only DF 52.5.

---

## Summary

All four §2.x experiments are **deferred to Phase 0 or later** with
the rationale that they are calibration / risk-quantification work,
not capability gating. The ticket §0.3 acceptance criteria require
§1.x (7/7 done) and §3.x (≥ 6/6 to do) but **do not** require §2.x.
This deferral preserves the ticket's stated scope.

If the §2.x experiments need to be re-prioritized (e.g. if §1.x findings
expose a calibration gap), they can be picked up individually; each is
small (½–2 days) and independent.
