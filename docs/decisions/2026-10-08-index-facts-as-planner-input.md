# Index facts as planner input

**Decides:** the v2 planner reads the scalar index facts of every table a query reads from the pinned Lance manifest, records each root scan's access path as Lance's own plan-time answer, and execution replays that choice instead of re-deciding it.
**Contract:** keeps E1, E7, read:11
**Code:** `crates/omnigraph-planner/src/source.rs`, `crates/omnigraph-planner/src/scan_access.rs`, `crates/omnigraph-planner/src/logical.rs`, `crates/omnigraph-planner/src/cost.rs`, `crates/omnigraph/src/engine/plan_source.rs`, `crates/omnigraph/src/engine/plan_source/scan_access`, `crates/omnigraph/src/engine/graph.rs`, `crates/omnigraph-gqt/cases/v2/planner/issue_854_key_index_probe.gqt`

## Context

The planner held no fact about indexes. Lance decided alone and silently
whether an index served a pushed filter, so the plan could not say how a
scan ran and no test could pin it. A lookup by a `String @key` read the
whole key column, because nothing in the planner knew the id BTREE would
answer it. Expand was costed on an assumed BTREE and corrected at run time
by a probe the explain did not show. All three needed the same input: the
indexes the table really has.

## Decision

- `PlanSource::index_facts` returns one fact per user index name on a table
  at the pinned dataset version: column, kind (`btree`, `inverted`,
  `vector`, `unknown`), name, and coverage as a known fraction or unknown.
  Segments of one name are unioned; Lance system indexes are excluded.
  Facts are read once, at the open `gather` already performs.
- The facts are a pure function of the dataset pin the plan records, so no
  new assumption is stored and replay safety is the existing pin check.
- The planner does not restate Lance's sargability rule. For a root scan
  whose filter is fully known at plan time, `index_split` builds the same
  Lance read execution builds and records the probe and residual Lance put
  in it as `ScanAccess::IndexProbe`. No scalar-index node gives
  `Sequential`.
- A scan whose read gains inputs at execution (nearest, full text, eligible
  ids, search filter, join filter, non-immutable expression) records
  `Runtime` with that input and leaves index use to Lance. A dependent scan
  keeps `id_lookup` and names the id BTREE when it is usable.
- `scan_access` is an asynchronous finalization stage after every rewrite.
  Planning for execution (`accept_query`) and `explain_query` both await it before a plan is bound,
  cached, executed or explained. Lookup errors propagate; none reads as an
  empty catalog.
- Execution sets `use_scalar_index` from the plan: on for a probe, off for
  sequential, Lance's default for runtime. The plan wins over what Lance
  would choose alone.
- Expand coverage is derived from the edge table's `src` and `dst` facts
  and marked `coverage_provenance: pinned_index_facts`; such a plan skips
  the run-time coverage probe. A missing field means `legacy_assumed`,
  which keeps the run-time correction for saved plans.
- A single scalar `String @key` equality, cast-free, on values of the key's
  own type, with a usable id BTREE, gains the conjunct `id = canonical key`
  and keeps the written equality. Numeric, temporal, float and composite
  keys are not narrowed, since their historical ids may differ from the
  current renderer, and a cast can merge keys a derived id would split.
- A missing, partial or unusable index never refuses a query and never
  changes rows. There is no probe-versus-scan cost comparison.

## Rejected

- **Keep the run-time correction and extend it to root scans:** the
  decision lives in two places, the explain shows only the first, and no
  case can pin what ran.
- **Restate Lance's sargability rule in the planner:** the copy drifts with
  each Lance release and cannot name the probe Lance builds when indexes
  meet on one scan, so the plan could record a probe that never ran.
- **Record a plan-time tree for every scan:** ranked, gated and dependent
  scans gain inputs no pinned value supplies, so the tree would be a guess
  or need query work during planning.
- **Record index state in the `__manifest` registration:** a second copy of
  what the Lance manifest already holds at the pinned version.
- **Feed the planner from an index status surface:** a maintenance view
  without the per-fragment detail Lance's selection depends on.
- **Use the explain statistic as coverage provenance:** it cannot tell
  assumed coverage from gathered coverage and is not part of the saved plan.
