# Testing

This is the ownership map for OmniGraph's tests. Read it before changing code: find the existing owner, run it as a clean baseline, and extend it instead of creating a parallel fixture.

## Rules

1. Test at the boundary that owns the promise. Compiler behavior belongs in compiler tests; engine guarantees belong at the public engine API; HTTP and CLI behavior belongs at those transports.
2. Prefer one new assertion, fixture row, or parameter over another `init_and_load` test.
3. Test logical results and durable state. Inspect Lance internals only for a compatibility fence, recovery fault, or physical-cost contract.
4. Every failure path must prove what did *not* move: manifest head, table head, lineage, sidecar, or external I/O as appropriate.
5. Time and RSS measurements are decision instruments, not ordinary correctness gates. Deterministic operation counts may be CI contracts.

The invariants behind these rules are in [invariants.md](invariants.md). Lance-dependent changes also require the upstream review and guards described in [lance.md](lance.md).

## Test layout

| Package | Primary owners | Shared support |
|---|---|---|
| `omnigraph-compiler` | In-source parser, catalog, type-checking, lowering, and lint tests | Module-local fixtures |
| `omnigraph-storage` | In-source control-object storage, CAS, locking, and URI tests | Module-local fixtures |
| `omnigraph-engine` | `crates/omnigraph/tests/` plus focused in-source tests | `tests/helpers/` and `tests/fixtures/` |
| `omnigraph-policy` | In-source Cedar policy parsing and evaluation tests | Module-local fixtures |
| `omnigraph-cluster` | In-source lifecycle tests; `tests/failpoints.rs`; `tests/s3_cluster.rs` | Module-local fixtures |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | `tests/support/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | `tests/support/mod.rs` |
| `omnigraph-dst` | `crates/omnigraph-dst/tests/` (`scenarios.rs`, `lane_b.rs`, `torn_init.rs`) plus in-source proofs | Crate-local fixtures. Deterministic simulation; needs `--cfg tokio_unstable` (the crate-local `.cargo/config.toml` sets it when cargo runs from the crate dir; every test file is `#![cfg(tokio_unstable)]`-gated and the crate compiles empty without it, so the default workspace gate is unaffected). `#[ignore]`d tests are fleet/hunt instruments driven by the DST workflows |
| `omnigraph-bench` | In-source configuration tests and `crates/omnigraph-bench/tests/` | Checked-in cases and suites under `benchmarks/` |

Do not copy server or CLI process setup into a new suite. Their support modules own hermetic configuration, binary startup, temporary roots, and common assertions.

## Engine ownership

The engine integration suite is grouped by behavior, not implementation module:

| Concern | Existing owners |
|---|---|
| Initialization and representative journeys | `lifecycle.rs`, `end_to_end.rs`, `composite_flow.rs`, `consistency.rs` |
| Query results and operators | `aggregation.rs`, `literal_filters.rs`, `ordering.rs`, `traversal.rs`, `traversal_indexed.rs`, `proptest_equivalence.rs`, `gq_logic_tests.rs` (walks the `.gqt` cases under `tests/gq_logic_tests/`) |
| Search and physical indexes | `search.rs`, `scalar_indexes.rs`, `lance_surface_guards.rs`, `rrf_prefilter_gate.rs` (the rrf plan gate's differential oracle and fences), `repro_issue_563.rs` (`#[ignore]`d overflow-scale symptom tier) |
| Writes, validation, schema, and policy | `writes.rs`, `validators.rs`, `schema_apply.rs`, `policy_engine_chassis.rs` |
| Branches, snapshots, diffs, and merges | `branching.rs`, `point_in_time.rs`, `changes.rs`, `merge_truth_table.rs`, `merge_fast_forward.rs` |
| Recovery and crash windows | `recovery.rs`, `failpoints.rs`, `failpoint_names_guard.rs`, in-source manifest/recovery tests |
| Maintenance and substrate fences | `maintenance.rs`, `lance_surface_guards.rs`, `lance_version_columns.rs`, `forbidden_apis.rs` |
| Export and lineage | `export.rs`, `lineage_projection.rs` |
| Cost and benchmark contracts | `write_cost.rs`, `write_cost_s3.rs`, `warm_read_cost.rs`, `branch_control_cost.rs`, `merge_cost.rs`, `changes_cost.rs`, the checkpoint/head lookup instruments, and `benchmark_scenario_contract.rs` |

Use `tests/helpers/mod.rs` for the standard graph, snapshots, row reads, Blob selectors, and bounded Blob collection. Recovery helpers belong in `tests/helpers/recovery.rs`; object-store counters belong in `tests/helpers/cost.rs`.

`changes_cost.rs` owns the change-feed cost boundary: transaction-footprint
candidate scans, bounded page work, and caught-up versus backlog polling curves.

### Recovery and failpoints

Recovery tests must cover the protocol layer, the writer, and the user-visible reopening behavior:

- in-source tests own sidecar encoding, validation, classification, and exact publication rules;
- `tests/recovery.rs` owns deterministic completed, partial, ambiguous, and foreign-effect outcomes;
- `tests/failpoints.rs` owns crash windows around durable effects;
- the writer's normal integration owner proves pre-arm failures leave no residue.

When adding a new writer or sidecar field, update all three layers. See [recovery.md](recovery.md).

### Blob behavior

Blob coverage is deliberately split:

- engine `end_to_end.rs`, `branching.rs`, and in-source Blob tests own logical cell selection, snapshots, integrity, ranges, external classification, and write admission;
- cluster tests own persisted external-source policy and serving projections;
- server `data_routes.rs`, `auth_policy.rs`, and `openapi.rs` own GET/HEAD, auth, conditions, ranges, redirects, backpressure, and schema drift;
- CLI `cli_data.rs` owns `blob get/stat`; `parity_matrix.rs` compares embedded and remote results.

Do not exercise a server promise solely through the engine facade. The complete contract is summarized in [blob.md](blob.md).

### Lance compatibility

Run this first for every Lance change:

```bash
cargo test -p omnigraph-engine --test lance_surface_guards
```

The guards pin only substrate behavior OmniGraph actually depends on: version and row columns, transaction witnesses, primary-key conflict filters, branch/ref cleanup, index coverage, stable row IDs, vector ordering fences, and Blob reads through compaction. If an upstream limitation disappears, remove the workaround and its guard together.

## Server and CLI ownership

Server suites are organized by public route: `auth_policy`, `data_routes`, `schema_routes`, `stored_queries`, `multi_graph`, `boot_settings`, object-store coverage in `s3`, and the generated contract in `openapi`.

CLI suites own their named planes: cluster lifecycle, data commands, stored queries, schema/config, cross-version rebuild, embedded/remote parity, and local/remote system journeys. Keep `OMNIGRAPH_HOME` hermetic by using `tests/support::cli()` or `cli_process()`.

The system tests start workspace binaries on ephemeral localhost ports. Set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` only in constrained local sandboxes; CI's configured owners must not skip.

## Commands

Focused iteration:

```bash
cargo test -p omnigraph-engine --test traversal
cargo test -p omnigraph-engine --test writes concurrent
cargo test -p omnigraph-engine --test gq_logic_tests
cargo test -p omnigraph-server --test data_routes
cargo test -p omnigraph-cli --test cli_data
cargo test -p omnigraph-cluster --test failpoints --features failpoints
cargo test -p omnigraph-bench --locked
```

`OMNIGRAPH_GQ_LOGIC_TESTS=<substr>[,<substr>]` restricts the gq logic-test run
to case files whose name contains a value; `OMNIGRAPH_GQ_BLESS=1` rewrites the
failing step's expect rows in place (local workflow only, never CI). The walker
keeps at most `OMNIGRAPH_GQ_JOBS=<n>` cases in flight (default: the machine's
available parallelism) and fails a case that exceeds
`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS=<n>` seconds (default 10); every `ok`/`FAIL`
line carries the case's elapsed time, and a case over budget belongs in a
`heavy-repro:` `#[ignore]`d test under `tests/repro_issue_*.rs`, not the
corpus.

Canonical workspace graph:

```bash
cargo test --workspace --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
```

The feature-superset command is the canonical graph because it compiles the current tree once with failpoint hooks present but inert unless a test enables one. Also run formatting and both Clippy graphs before pushing; [ci.md](ci.md) lists the exact gates.

AWS server support has a separate feature owner:

```bash
cargo test -p omnigraph-server --features aws
```

S3-backed tests skip unless `OMNIGRAPH_S3_TEST_BUCKET` and the corresponding AWS endpoint/credential variables are set. Azure-backed tests skip unless `OMNIGRAPH_AZURE_TEST_CONTAINER` and the documented Azure/Azurite variables are set. A configured CI backend treats a skip as failure.

### OpenAPI

`crates/omnigraph-server/tests/openapi.rs` regenerates the specification in memory and compares it with `openapi.json`. For an intentional API change:

```bash
OMNIGRAPH_UPDATE_OPENAPI=1 \
  cargo test -p omnigraph-server --test openapi openapi_spec_is_up_to_date
```

Commit the generated file with the API change. CI checks drift; it never updates the file.

## Cost tests and benchmarks

Correctness tests may assert deterministic logical or object-store operation counts when the count is part of the design contract. Wall time and peak RSS depend on the host and belong in the `omnigraph-bench` scenario harness; benchmark results are evidence rather than pass/fail assertions. Declarative benchmark cases and suites live under `benchmarks/`; the engine's deterministic benchmark contracts remain in `crates/omnigraph/tests/`.

The current runner executes the narrow, fail-closed local envelope documented
in `crates/omnigraph-bench/README.md`. It requires a release binary, restores
every repetition at the fixture's stable path from a never-opened APFS
clonefile template or a verified Linux/XFS plain-copy template. Plain-copy
reads fixture bytes before measurement and declares the page cache uncontrolled.
The runner contains each measured merge in a fresh SHA-attested,
hard-deadline worker process, and verifies exact target/source/main state.
Fixture and repetition children clear the host environment, pin locale, and
receive protocol-owned scratch siblings as `TMPDIR` and cwd; measured workers
also use their per-repetition scratch as `OMNIGRAPH_MERGE_STAGING_DIR`. The only
inherited engine setting is the modeled `LANCE_MEM_POOL_SIZE`; Tokio/Rayon
thread-count overrides are refused before execution. A
run without `--archive` emits diagnostic output only:

```bash
cargo run --release --locked -p omnigraph-bench -- \
  suite run benchmarks/suites/local-smoke.suite-v1.yaml
```

The imported-fixture `fixture run-graph` path is separate from durable suite
execution. Its fixed FinGraph node-and-edge merge adapter supports qualified
macOS/APFS clonefiles or Linux/XFS directly backed by EC2 instance-store NVMe;
EBS is refused. The registered source stays quiescent and is never opened as a
database. Every repetition restores the prepared physical tree at the exact
same active path. Source and scratch ownership must remain exclusive:
metadata-only checks detect observable stat drift, not every same-length
rewrite within a filesystem timestamp tick. Byte identity comes from the
verified copy or forced-clone contract. Before freezing, Linux requires free
space for one more prepared-tree copy plus 1 GiB. Use a dedicated benchmark
mount: this path calls `syncfs` after freezing and after every restore, outside
timing, to finish data and directory writeback across that filesystem. It records a distinct
`xfs-plain-copy-syncfs-same-active-path` reset, not the durable suite's existing
plain-copy treatment. Fresh workers attest matching process-effective machine
identities; copying leaves the OS page cache uncontrolled. Reports remain
`claim_eligible: false` and `durable_record: false`, with no archive publication
or AWS dispatch. Commands live in the
[FinGraph diagnostic guide](../../benchmarks/README.md#fingraph-diagnostic-runner).
Within `omnigraph-bench`, `reset.rs` owns copy/path integrity tests,
`environment.rs` owns backend qualification, and `real_graph_run.rs` owns the
platform, capacity, writeback, worker-identity, and native merge regressions.

Do not archive diagnostic JSON as telemetry. To publish authoritative
`suite run` records, first commit the exact source under test, build the release binary from
that clean tree, and pass `--archive <DIR>`. The commit records source
provenance; the executable digest and normalized build/engine facts bind the
exact SUT bytes. Source revalidation compares raw tracked source bytes without
Git clean filters, disables replacement objects and permissive stat-cache
modes, and refuses hidden index flags or ignored untracked source inputs.
Profile-file LTO/codegen/strip values are declarations, not
effective compiler facts: Cargo does not expose the final target rustc command
to build scripts, so records mark effective codegen options unproved until
controlled infrastructure supplies a digest-bound receipt. Raw timing records remain
useful evidence, but that absence cannot authorize a performance conclusion.
Accordingly, the projection reports `claim_eligible: false` even for complete
local acquisitions until a controlled digest-bound build receipt supplies that
proof. Acquisition status and global claim eligibility are separate facts.
Validate records independently with
`archive verify`; rebuild the disposable OmniGraph read model with `projection
rebuild --archive <DIR> --root <DIR>`. The content-addressed canonical JSON is
authority. Projection generations and `CURRENT` may always be deleted and
rebuilt from it. Archive verification streams a fixed invocation inventory;
archive writers and inventory capture coordinate at the immutable pointer
publication boundary. The current publication guarantee is local Unix
file/directory durability through every descriptor-rooted ancestor back to the
captured archive root. Readers fix the pointer inventory under the publication
lock, then durability-close each yielded record once or fail; a substantive sync failure after pointer visibility is
`possibly_published`, never success. Projection queries and rebuild verification use bounded,
exclusive pages whose continuation cursors are pinned to an immutable
generation, and publication verifies a canonical digest over every projected
field rather than keys alone.

Process-effective machine evidence is captured in each isolated repetition
worker immediately before it declares readiness. All repetitions in a run must
match exactly; the CLI does not reuse a session-start machine snapshot.

Archive-mode suite execution publishes and releases each complete raw run
before starting the next suite entry. Its command result contains a completed
count and immutable receipts instead of duplicating the raw samples already in
the authoritative records. If pointer visibility succeeds but bounded
directory-sync recovery cannot prove durability, the JSON failure includes a
`possibly_published` identity. Pass its invocation and record digest to
`archive reconcile`; only a `durable`, `absent`, or `conflict` result closes the
specific ambiguity. Do that before retrying under a different invocation. If
an acquisition fails after at least one fully verified repetition, the CLI
publishes only that prefix as a `censored`, permanently claim-ineligible record
and still exits nonzero. A rep-zero failure publishes nothing, and a merely
settled repetition never enters durable samples. If
record construction or publication fails before authority exists, the bounded
failure output retains that one complete execution or censored verified prefix
as state-neutral `unpublished_run` evidence. Human mode prints the same complete
JSON envelope.

Keep measurement fixtures separate from production schemas and recovery state. A no-go result belongs in the RFC or issue that consumed the experiment, not as a permanent narrative in this map.

## Before every task

1. Read [invariants.md](invariants.md).
2. Use [lance.md](lance.md) to identify and read every relevant full upstream page.
3. Search existing tests by public API, error variant, route, and durable object name.
4. Run the narrowest existing owner as a clean baseline.
5. Extend that owner unless the behavior crosses a genuinely new public boundary.
6. Run the focused owner again, then the canonical workspace graph in proportion to risk.
7. For docs, workflow, or API changes, also run the repository link/pin/OpenAPI checks that own those generated contracts.
