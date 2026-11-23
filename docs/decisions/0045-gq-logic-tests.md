# GQ logic tests and the frozen reference engine

**Decides:** query behavior is tested by self-contained `.gqt` files run by a private test package, every fix carries a regression that CI checks for, and engine v2 is compared against a frozen engine v1.
**Contract:** none; serves invariant 13
**Code:** `crates/omnigraph-gqt`, `crates/omnigraph-gqt-core`, `crates/omnigraph-gqt-served`, `crates/omnigraph-reference-engine/tests/frozen.rs`, `scripts/check-fix-regression.py`, `.github/workflows/fix-regression-gate.yml`, `.github/workflows/gq-logic-tests.yml`
**Cited as:** RFC 0045

## Context

A query-behavior regression written in Rust costs a whole test function, so
few get written, and nothing forced a fix to carry one. Many defects are pure
input to output: a schema, some rows, a query, a wrong count. The
sqllogictest lineage (SQLite, DuckDB, DataFusion) shows that such tests are
written when each is a small data file. The syntax of the format is owned by
[the GQT README](../../crates/omnigraph-gqt/README.md); this record holds
only the choices behind it.

## Decision

- Query-behavior tests default to `.gqt` cases under
  `crates/omnigraph-gqt/cases/`. A Rust test needs a reason the format
  cannot express: mechanism, scale, process environment, concurrency.
- A case is one self-contained file: schema, seed rows, steps and their
  expectations. Each case is its own libtest test and runs on a fresh graph.
- The package is private: `publish = false`, not a default member, excluded
  from `Test Workspace`, and owned by its own required `GQ Logic Tests`
  context.
- Every case declares its execution in a required runner section. There is
  no default target, storage, seed or timeout. A combination the runner does
  not support is refused, never skipped or replaced by another environment.
- The format fails closed: an unknown section, an unknown header key or a
  missing required header is a refusal, so an older runner never misreads a
  newer case.
- An issue case is named `issue_N_*` and carries a `# red_on:` line, because
  a regression nobody watched fail guards nothing.
- `Fix Regression Gate` reads the closing keywords of a pull request that
  changes code and requires, per closed issue, an added or changed
  `issue_N` case or test-attributed Rust test. It runs from the base branch's
  copy. The `no-repro` label, applied by a maintainer, waives it.
- Expectations live beside the query. Bless rewrites only expectation
  bodies, and the diff is the review.
- `expect ordered` is refused where the engine's order is not total, such as
  a query with no `order` clause.
- A seam fires at the step it is placed before, and delivery is proved by the
  installed decider, not by error text (see
  [the seam type](0066-one-seam-type.md)).
- Engine v1 is frozen in `omnigraph-reference-engine`, its sources pinned by
  SHA-256. A query step reaches it only through `--- expect same as v1`, which
  adds a row comparison with v2. A defect seen on v1 is fixed on v2.
- A corpus case runs within ten seconds of wall time. A slower case lives
  under `cases_slow/`, which only a nightly workflow runs and the gate does
  not count.

## Rejected

- **Adopt sqllogictest-rs and `.slt`:** a case is a schema-plus-seed world
  built through init and load, which no GQ statement stream expresses, and
  whitespace-separated rows lose the row-object match.
- **insta snapshots:** they split the query from its expectation and move
  review into a separate tool.
- **A Python suite over HTTP:** a second toolchain, a server per test, and
  end-to-end flakiness.
- **Select environments from the CLI or CI:** a copied regression then loses
  the storage and fault conditions it needs.
- **Arm faults once at file start:** setup can consume the occurrence meant
  for the later operation.
- **A gate that runs the matching Rust test:** it compiles every crate's
  tests, and libtest's filter has no word boundary (`issue_563` selects
  `issue_5630`).
