# Benchmark catalog

This directory contains the checked-in inputs for the end-to-end benchmark
harness. A case describes one benchmark point. A suite selects cases and says
how many samples to collect. These files are experiment definitions, not run
records, fixtures, or result storage.

## Layout

- `cases/*.case-v1.yaml` assigns the fixture, workload, environment, and
  protocol for one point.
- `suites/*.suite-v1.yaml` lists cases to run. Suite files live directly in
  `suites/`, whose parent is the authoritative catalog root. A `case` path is
  resolved relative to the suite file that contains it, and its canonical
  target must remain under the same catalog's `cases/` directory. Suite and
  referenced-case symlinks cannot escape the catalog.

The `version` field selects the document schema. Keep identifiers and enum
values in kebab-case. The scenario and fixture-builder versions identify the
code contracts that interpret the remaining typed fields.

Index state is an inventory rather than a global label. Use `indexes: []` for
an unindexed fixture; each indexed entry names its `table`, `column`, `kind`,
and `freshness`. Synthetic branch-merge builder v1 supports only
`compaction_recency: not-optimized`, because OmniGraph optimization
materializes physical indexes outside this builder's exact inventory contract.

`protocol.deadline_seconds` is required. Set it to an integer from 1 through
3600, or to YAML `null` when the measured operation has no deadline; the
executor must still enforce a separate bounded safety watchdog. Leading and
trailing whitespace in S3 identity fields and index table/column names is
normalized before validation and hashing.

Case definitions deliberately contain no source branch, system-under-test
build, machine identity, AWS account, bucket URI, result location, or
credentials. A runner binds those invocation facts and records them with the
result. Repetition count is run quantity, so it belongs to the suite rather
than the case's experiment identity.

## Validate and inspect

From the repository root:

```bash
cargo run -p omnigraph-bench -- \
  case validate benchmarks/cases/branch-merge-d50-warm.case-v1.yaml

cargo run -p omnigraph-bench -- \
  case list benchmarks/cases

cargo run -p omnigraph-bench -- \
  suite validate benchmarks/suites/local-smoke.suite-v1.yaml

cargo run -p omnigraph-bench -- \
  suite plan benchmarks/suites/local-smoke.suite-v1.yaml
```

Each command accepts `--json` for machine-readable output. `suite plan` also
accepts `--case <ID>` to select one case from a suite.

Validation loads every referenced case and checks cross-field rules, including
checked scale budgets, table bounds, warmth declarations, and reset/backend
compatibility. Planning expands the suite into ordered run entries; it does
not execute a benchmark.

The checked-in smoke point declares APFS on local NVMe storage. Validation is
host-independent, but the future runner must verify those declared environment
facts and refuse to measure on a mismatched host. S3-compatible cases likewise
carry region, storage class, implementation/version, bucket-versioning state,
and a digest pin for MinIO or RustFS images in their point identity.

## Delivery boundary

This first slice stops at a validated, versioned execution plan. The remaining
pieces land behind that contract in this order:

1. A runner builds and freezes fixtures, performs one operation per repetition,
   restores the declared reset state, and verifies non-vacuous results.
2. Telemetry writes immutable JSON run records to a content-addressed archive.
   A query database is a rebuildable projection, never the source of truth.
3. An AWS adapter binds declared S3 and machine facts, applies budget and
   lifecycle controls, runs the same plans, and uploads the same record format.

Keeping execution, persistence, and cloud orchestration out of this slice lets
reviewers stabilize experiment identity before any point ids or result records
escape into infrastructure.

## Add a case

1. Add a versioned case file with explicit levels for every typed factor.
2. Reference it from a suite with a path relative to that suite.
3. Run both `case validate` and `suite validate`.
4. Inspect `suite plan` before handing the plan to a runner.

Do not encode a profile such as `micro` or `realistic` in a case. Profiles are
derived from the declared factor levels. Benchmark timing is evidence and does
not gate CI; deterministic count contracts remain in their existing owning
tests.
