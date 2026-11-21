# Testing

**Audience:** anyone adding or changing a test
**Authority:** the testing rules and the route to the owning suite; every
owner, fixture and command is in [test ownership](test-ownership.md)

Read this before changing code: find the existing owner, run it as a clean
baseline, and extend it instead of creating a parallel fixture.

## Rules

1. Test at the boundary that owns the promise. Compiler behavior belongs in
   compiler tests; engine guarantees belong at the public engine API; HTTP and
   CLI behavior belongs at those transports.
2. Prefer one new assertion, fixture row, or parameter over another
   `init_and_load` test. When the change fixes an issue, the
   `Fix Regression Gate` keys on `issue_N` in the test's name or the `.gqt`
   case's file name: extend an owner test by renaming it to carry `issue_N` in
   the same change, or add a row to the `issue_N_*.gqt` case ([ci.md](ci.md)).
3. Test logical results and durable state. Inspect Lance internals only for a
   compatibility fence, recovery fault, or physical-cost contract.
4. Every failure path must prove what did *not* move: manifest head, table
   head, lineage, schema staging, or external I/O as appropriate.
5. Time and RSS measurements are decision instruments, not ordinary
   correctness gates. Deterministic operation counts may be CI contracts.

The invariants behind these rules are in [invariants.md](invariants.md).
Lance-dependent changes also require the upstream review and guards described
in [lance.md](lance.md).

## Where a test goes

| The change touches | Owner | Detail |
|---|---|---|
| Rows, counts, result column types or errors of a query | A `.gqt` case under `crates/omnigraph-gqt/cases/`. A Rust test needs a reason the format cannot express: mechanism, scale, process environment, concurrency | [GQT README](../../crates/omnigraph-gqt/README.md) |
| Compiler, planner, storage, core, catalog, policy, cluster or MCP internals | In-source tests beside the code | [Test layout](test-ownership.md#test-layout) |
| An engine guarantee | `crates/omnigraph/tests/`, at the public engine API, with `tests/helpers/` | [Engine ownership](test-ownership.md#engine-ownership) |
| A crash window or a durable effect | `failpoints.rs`, `detached_commit_matrix.rs`, `integration/recovery.rs`. A new writer updates every layer | [Recovery and failpoints](test-ownership.md#recovery-and-failpoints) |
| A Blob promise | Split across the engine, cluster, server and CLI owners | [Blob behavior](test-ownership.md#blob-behavior) |
| A Lance behavior Omnigraph depends on | `lance_surface_guards.rs` and its fence | [Lance compatibility](#lance-compatibility) |
| An HTTP route, authentication, MCP or OpenAPI | `crates/omnigraph-server/tests/`, by public route. Never test a server promise only through the engine facade | [Server and CLI ownership](test-ownership.md#server-and-cli-ownership) |
| A CLI command, its output or its delivery | `crates/omnigraph-cli/tests/`, by plane | [Server and CLI ownership](test-ownership.md#server-and-cli-ownership) |
| A storage stamp, an upgrade or a predecessor binary | CLI `integration/crossversion_upgrade.rs`, engine `db/upgrade/tests.rs` | [Cross-version tests](test-ownership.md#cross-version-rebuild-and-storage-upgrade), [support matrix](versioning.md#storage-upgrade-support-matrix) |
| A bound plan reproducing its run | `integration/plan_replay.rs`, `scrubbed_replay.rs` | [Plan replay](test-ownership.md#plan-replay) |
| The core IR's semantics: an operator, a lowered form, a lexical rule | `crates/omnigraph-reference`, updated in the same change; GQT compares every read step on the direct engine with it | [Test layout](test-ownership.md#test-layout), [GQT README](../../crates/omnigraph-gqt/README.md#reference-comparison) |
| A law over generated schemas, graphs and reads | The law's owner, with its nightly twin | [Generated laws](test-ownership.md#generated-laws) |
| How the respelled corpus answers against edition 2 | `scripts/cross-edition.py` and `crates/omnigraph-gqt/cross-edition.tsv` | [Cross-edition comparison](test-ownership.md#cross-edition-comparison) |
| What an operation reads or writes in storage | The DST golden and the engine `integration/*_cost.rs` curve tests | [DST storage-cost golden](#dst-storage-cost-golden) |
| The tokens an analyzer profile emits | The full-text fingerprint golden, never refreshed alone | [Full-text fingerprint golden](test-ownership.md#full-text-fingerprint-golden) |
| Wall time or memory | `omnigraph-bench`; evidence, never a pass/fail assertion | [Benchmark harness](test-ownership.md#benchmark-harness) |

Loose integration suites of a crate are modules of one
`tests/integration/main.rs`. A file stays a separate binary under `tests/`
only when its tests need their own process: failpoints armed for the whole
process, a re-executed binary, a mutated process environment.

Server and CLI suites start processes through their `tests/support` modules,
which own hermetic configuration, binary startup and temporary roots. Do not
copy that setup into a new suite.

Cloud suites skip unless their environment is set (`OMNIGRAPH_S3_TEST_BUCKET`,
`OMNIGRAPH_AZURE_TEST_CONTAINER` and the documented credentials). A configured
CI backend treats a skip as failure, and a local file pass does not qualify S3
or Azure.

## Lance compatibility

Run this first for every Lance change:

```bash
cargo test -p omnigraph-engine --test lance_surface_guards
```

The guards pin only substrate behavior Omnigraph depends on. A red guard is a
required design review, not a test to weaken. If an upstream limitation
disappears, remove the workaround and replace its negative guard with the
native behavior the caller now requires. Qualify a dependency bump in both
debug and release profiles, starting from bytes the genuine predecessor wrote,
and never enable an experimental capability to make qualification pass. The
full procedure is in
[test ownership](test-ownership.md#lance-compatibility); the fences are in
[Lance compatibility fences](lance-fences.md).

## Commands

```bash
cargo test -p omnigraph-engine --test integration traversal_indexed::   # one suite
cargo test -p omnigraph-engine --test writes concurrent                  # a separate binary
cargo test -p omnigraph-server --test integration data_routes::
cargo test -p omnigraph-cli --test integration cli_data::
cargo test -p omnigraph-cluster --test failpoints --features failpoints
cargo test -p omnigraph-gqt --test gq_logic_tests issue_563             # matching case names
```

Run one test with `--test integration <name>::<test> -- --exact`. The
canonical workspace graph, formatting and Clippy commands are in
[AGENTS.md](../../AGENTS.md#build-and-test); [ci.md](ci.md) lists the exact
gates. GQT, DST, plan replay and OpenAPI commands are in
[test ownership](test-ownership.md#commands).

## Cost tests and benchmarks

A correctness test may assert a deterministic logical or object-store
operation count when the count is part of the design contract. Wall time and
peak RSS depend on the host and belong in the `omnigraph-bench` harness, whose
results are evidence, not assertions. Workload definitions live in
[`benchmarks/`](../../benchmarks/README.md); the cost owners and the harness
are in [test ownership](test-ownership.md#cost-tests-and-benchmarks).

### DST storage-cost golden

`crates/omnigraph-dst/cost_table.txt` records the exact object-store calls of
each labeled operation, and `dst_bench_cost_count_golden` compares a seeded run
with it, so a cost change fails as a named line. A change to what an operation
reads or writes in storage adds or updates its row. A contract that a cost
stays flat in something records the operation at two sizes under two labels.

A changed line is a cost change. Regenerate only on purpose, and say in the
pull request why each changed line moved; a line that moved for no stated
reason is a regression to find, not a golden to refresh. Regenerate, then
verify, from `crates/omnigraph-dst`:

```bash
DST_REGEN_COSTS=1 cargo test --locked --test scenarios dst_bench_cost_count_golden
cargo test --locked --test scenarios dst_bench_cost_count_golden
```

How to label a scenario, and how the golden relates to the curve tests, is in
[test ownership](test-ownership.md#dst-storage-cost-golden).

## Before every task

1. Search existing tests by public API, error variant, route, and durable
   object name.
2. Run the narrowest existing owner as a clean baseline.
3. Extend that owner unless the behavior crosses a genuinely new public
   boundary.
4. Run the focused owner again, then the canonical workspace graph in
   proportion to risk.
5. For docs, workflow, or API changes, also run the repository link, pin and
   OpenAPI checks that own those generated contracts.
