# Testing

This is the ownership map for OmniGraph's tests. Read it before changing code: find the existing owner, run it as a clean baseline, and extend it instead of creating a parallel fixture.

## Rules

1. Test at the boundary that owns the promise. Compiler behavior belongs in compiler tests; engine guarantees belong at the public engine API; HTTP and CLI behavior belongs at those transports.
2. Prefer one new assertion, fixture row, or parameter over another `init_and_load` test. When the change fixes an issue, the `Fix Regression Gate` keys on `issue_N` in the test's name or the `.gqt` case's file name: extend an owner test by renaming it to carry `issue_N` in the same change, or add a row to the `issue_N_*.gqt` case (`docs/dev/ci.md`).
3. Test logical results and durable state. Inspect Lance internals only for a compatibility fence, recovery fault, or physical-cost contract.
4. Every failure path must prove what did *not* move: manifest head, table head, lineage, schema staging, or external I/O as appropriate.
5. Time and RSS measurements are decision instruments, not ordinary correctness gates. Deterministic operation counts may be CI contracts.

The invariants behind these rules are in [invariants.md](invariants.md). Lance-dependent changes also require the upstream review and guards described in [lance.md](lance.md).

## Test layout

| Package | Primary owners | Shared support |
|---|---|---|
| `omnigraph-compiler` | In-source parser, catalog, type-checking, lowering, and lint tests | Module-local fixtures |
| `omnigraph-planner` | In-source optimizer/cost tests and `tests/query_plan.rs`, `tests/registry.rs`, `tests/lower_walk.rs`; plan assertions over real snapshots live in GQT | `PlanSource` fixtures for metadata and refusal states |
| `omnigraph-storage` | In-source control-object storage, CAS, locking, and URI tests | Module-local fixtures |
| `omnigraph-seams` | In-source tests of the seam type: slot scopes, the guard, the decision behaviors; `tests/failpoint_names_guard.rs`, the source walk over the engine, cluster and DST crates and the `.gqt` corpus that keeps every seam catalogued, crossed and armed | None |
| `omnigraph-engine` | `crates/omnigraph/tests/` plus focused in-source tests | `tests/helpers/` and `tests/fixtures/` |
| `omnigraph-policy` | In-source Cedar policy parsing and evaluation tests | Module-local fixtures |
| `omnigraph-cluster` | In-source lifecycle tests; `tests/failpoints.rs`; `tests/s3_cluster.rs` | Module-local fixtures |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | `tests/support/mod.rs` |
| `omnigraph-cli` | `crates/omnigraph-cli/tests/` | `tests/support/mod.rs` |
| `omnigraph-dst` | `crates/omnigraph-dst/tests/` (`scenarios.rs`, `lane_b.rs`, `torn_init.rs`) plus in-source proofs | Crate-local fixtures. Deterministic simulation; needs `--cfg tokio_unstable` (the workspace `.cargo/config.toml` sets it for every build; the default workspace gate excludes the crate by name). Run from `crates/omnigraph-dst`: its `[env]`-only `.cargo/config.toml` supplies the pool trio that `require_pool_env` asserts at process start. `#[ignore]`d tests are fleet/hunt instruments driven by the DST workflows |
| `omnigraph-bench` | In-source configuration tests and `crates/omnigraph-bench/tests/` | Checked-in cases and suites under `benchmarks/` |
| `omnigraph-gqt` | `tests/gq_logic_tests.rs`, one libtest test per `.gqt` case (`datatest-stable`, `harness = false`), plus in-source format self-tests and the corpus layout check | The `.gqt` corpus under `crates/omnigraph-gqt/cases/`; format in RFC 0045 |

Do not copy server or CLI process setup into a new suite. Their support modules own hermetic configuration, binary startup, temporary roots, and common assertions.

## Engine ownership

The engine integration suite is grouped by behavior, not implementation module:

| Concern | Existing owners |
|---|---|
| Initialization and representative journeys | `lifecycle.rs`, `end_to_end.rs`, `composite_flow.rs`, `consistency.rs` |
| Query results and operators | `aggregation.rs`, `literal_filters.rs`, `ordering.rs`, `traversal.rs`, `traversal_indexed.rs`, `proptest_equivalence.rs`; the `.gqt` corpus lives in `omnigraph-gqt` (`crates/omnigraph-gqt/cases/`) |
| V2 execution, memory and frozen v1 | `engine_v2.rs`, `engine_v2_memory.rs`, `v1_frozen.rs`; `repro_issue_703.rs` and `repro_issue_723.rs` own ignored scale symptoms |
| V2 plan nodes' operators and the execution report | In-source `engine/report/tests.rs` owns which operator each node builds and what its row says (`ran`, `actual_rows`, `drained`); `crates/omnigraph-planner/tests/lower_walk.rs` owns the walk's order and refusals; the `ran` lines of `--- expect plan` read the report of the case's own run |
| V2 plan sufficiency: the bound plan alone reproduces a run | `crates/omnigraph/tests/engine_v2_plan_replay.rs` (see [Plan replay](#plan-replay)): one query per node kind with a switch or a ladder, replayed twice through `Session::replay_bound_plan` from the serialized `BoundPlan`, equal rows and the same trace required; `crates/omnigraph-planner/tests/bound_plan.rs` owns the serialized form; in-source `engine/search.rs` tests own the declared ladder's stepping; `engine/plan_source.rs` tests own that the assumed memory limit sizes the plan, the lowering and the pool |
| V2 scrubbed replay: no input beside the bound plan and the snapshot reaches execution | `engine_v2_scrubbed_replay.rs` owns the mechanism (the `std::env` grep over `crates/omnigraph/src/engine/`, the replay under an ambient memory limit and an ambient `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` the plan did not capture, `ann_nprobes` as a plan field, the `profile` rows); `engine_v2_plan_replay.rs` owns the snapshot pins (an edge write or an insert after planning refuses the replay); the inventory cases `cases/v2/planner/input_*.gqt` own one input class each (`engine`, `ann_nprobes`, index build state), and the clock class is owned by `engine_v2_plan_replay.rs`; `engine_v2_memory.rs` owns `ran id_lookup` under a refused build |
| V2 declared switches: the report names the side a run took | `cases/v2/planner/switch_ran_on_the_report.gqt` (the `ran` line of `--- expect plan`); in-source `engine/report/tests.rs` owns the switch's row and a scan whose plan declares no fallback |
| Search and physical indexes | `search.rs`, `scalar_indexes.rs`, `lance_surface_guards.rs`, `rrf_prefilter_gate.rs` (the rrf plan gate's differential oracle and fences), `repro_issue_563.rs` (`#[ignore]`d overflow-scale symptom tier) |
| Writes, validation, schema, and policy | `writes.rs`, `validators.rs`, `schema_apply.rs`, `policy_engine_chassis.rs` |
| Branches, snapshots, diffs, and merges | `branching.rs`, `point_in_time.rs`, `changes.rs`, `merge_truth_table.rs`, `merge_fast_forward.rs` |
| Recovery and crash windows | `recovery.rs`, `failpoints.rs` (including the `live_handle_*` liveness owners: a live handle writes again once faults stop, without reopening), `detached_commit_matrix.rs` (the RFC 0067 writer × window × fault × recovery-actor matrix over the insert, multi-table, load, cleanup, ensure-indices, full-text-rebuild, merge, schema-apply, optimize and system-column-upgrade writers; the same-handle liveness actor runs by default and `OMNIGRAPH_MATRIX=full` adds the other-process and cleanup actors), in-source manifest/recovery tests |
| Maintenance and substrate fences | `maintenance.rs`, `lance_surface_guards.rs`, `lance_version_columns.rs`, `forbidden_apis.rs` |
| Export and lineage | `export.rs`, `lineage_projection.rs` |
| Legacy-vintage graphs (`id`/`src`/`dst` spellings, born at the current stamp) | `legacy_columns.rs` — load, query, export round trip, evolution; needs `--features failpoints` |
| System-column upgrade (RFC 0040 step 3: respelling in place on a served graph, no stamp change since v10) | `system_column_upgrade.rs` — check and execute, preflight refusals, every window before the manifest commit leaving no residue, a post-commit failure finished by the next read-write open or the same handle's next write, pending pins after a skipped promotion, the control-object cost; needs `--features failpoints`. Route composition and the default target: `upgrade/tests.rs` |
| Cost and benchmark contracts | `write_cost.rs`, `write_cost_s3.rs`, `warm_read_cost.rs`, `branch_control_cost.rs`, `merge_cost.rs`, `changes_cost.rs`, the checkpoint/head lookup instruments, and `benchmark_scenario_contract.rs` |

Use `tests/helpers/mod.rs` for the standard graph, snapshots, row reads, Blob selectors, and bounded Blob collection. Recovery helpers belong in `tests/helpers/recovery.rs`; object-store counters belong in `tests/helpers/cost.rs`.

`changes_cost.rs` owns the change-feed cost boundary: transaction-footprint
candidate scans, bounded page work, and caught-up versus backlog polling curves.

### Recovery and failpoints

Crash tests must cover the writer, the promotion that follows it, and the user-visible reopening behavior:

- `tests/failpoints.rs` owns crash windows around durable effects: after a detached effect, before and after publication, between promotions, where the graph is unchanged or a pin stays pending;
- `tests/detached_commit_matrix.rs` owns the writer × window × fault × recovery-actor matrix under one oracle;
- `tests/recovery.rs` owns what is left of open-time recovery: a clean open creates nothing, a sidecar from an older build refuses a read-write open and not a read-only one, and a read-only open never touches schema staging;
- `tests/lance_surface_guards.rs` owns the twin-replay rules promotion depends on;
- the writer's normal integration owner proves pre-effect failures leave no residue.

To add a seam: declare it beside the site it guards, above the item that
crosses it, with
`decide_seam! { pub static NAME = ("area.place", Op, [Effect, ..]); }`
(imported from `crate::seams`; the compiler records the file and line as
the seam's `site()`, and a hand-written `Seam::decide` is refused); add its
path to the `catalog!` list in `crates/omnigraph/src/seams/catalog.rs`; call
`fail`, `skip` or `contention` (imported from `crate::seams`) with it at a
site between two steps (one declared effect), or `guarded` around the one
operation it wraps (every outcome that operation can have; a further action
there is then a case, not an edit). A private module on the path from the
crate root to the declaring file becomes `pub(crate)` for the re-export.
`crates/omnigraph-seams/tests/failpoint_names_guard.rs` checks the index, that no static is left
in the catalog, under a test module or without `pub`, that a single-effect
helper takes a seam declaring exactly its effect, that production code under
`src/` crosses it, and that test code (an integration target, a `tests.rs` /
`…_tests.rs` file, or any item gated on `cfg(test)`), the DST crate or a
`.gqt` case (the `at` of a `--- seam` body, decoded as the runner decodes
it) arms it; a crossing never counts as arming. `scripts/seam_corpus.py`
lists every seam with where it is declared and which cases cover it.

When adding a new writer, update all of these layers. See [recovery.md](recovery.md).

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

The guards pin only substrate behavior OmniGraph actually depends on: version and row columns, transaction witnesses, primary-key conflict filters, branch/ref cleanup, index coverage, stable row IDs, vector ordering fences, Blob reads through compaction, and the detached-commit privacy, twin replay and self-conflict rules that RFC 0067 builds on. If an upstream limitation disappears, remove the workaround and its guard together.

## Server and CLI ownership

Server suites are organized by public route: `auth_policy`, `data_routes`, `schema_routes`, `stored_queries`, `multi_graph`, `boot_settings`, object-store coverage in `s3`, and the generated contract in `openapi`.

CLI suites own their named planes: cluster lifecycle, data commands, stored queries, schema/config, cross-version rebuild, embedded/remote parity, and local/remote system journeys. Keep `OMNIGRAPH_HOME` hermetic by using `tests/support::cli()` or `cli_process()`.

The cross-version rebuild owner, `crossversion_upgrade.rs`, skips each predecessor case when its binary is not configured, so a local `cargo test -p omnigraph-cli --test crossversion_upgrade` is green even while CI's `V5 ↔ V10 Format Fence` is red. To run the fence locally, build the predecessor CLI from the commit `ci.yml` pins as `FINAL_INTERNAL_V5_COMMIT` (`git worktree add <dir> <sha>`, then `cargo build --locked -p omnigraph-cli --bin omnigraph` inside it) and run the exact case with that binary:

```bash
OMNIGRAPH_V5_BIN=<dir>/target/debug/omnigraph cargo test --locked -p omnigraph-cli --test crossversion_upgrade current_v10_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v10 -- --exact --nocapture
```

The older seams work the same way with released binaries: `OMNIGRAPH_OLD_BIN` (0.7.2) and `OMNIGRAPH_PREVIOUS_BIN` (0.8.1). `OMNIGRAPH_V6_BIN` (the 0.10.0 release) owns the v6↔v10 fence. RFC 0062 introduced v7's registration clock, RFC 0042's native-ref retirement metadata requires v8, RFC 0040's system columns stamped new graphs v9, and RFC 0067's detached table commits stamp every graph v10. The v0.9 journey is a different case, a fully exercised v6 graph — branches, edges, vectors, full-text and blobs — that the current binary refuses and that is rebuilt from a 0.9 export; `Test Workspace` runs both on every pull request that changes engine input, with the releases it installs.

The separate `Storage Upgrade Compatibility` CI job requires genuine v0.9 and
v0.10 local standalone journeys: the v6 → v7 → v8 route with `--to-format 8`
first, then the default route to v10 on the same branched fixture, which the
journey asserts keeps every branch and every table byte. It fails
missing predecessor binaries, missing cases and skipped required cases. Engine
storage-upgrade tests own direct v7 → v8 conversion, exact pending v6 → v7
recovery before composition, explicit target 7, deferred check reporting,
v8 no-op admission with retained retired refs, the v8 and v9 → v10 stamp
step (`storage_upgrade_default_route_takes_a_legacy_v8_graph_to_v10`,
`storage_upgrade_default_route_takes_a_v9_graph_to_v10`) and the synthetic
v6/v7 → v10 composition (`storage_upgrade_default_route_takes_a_synthetic_v6_graph_to_v10`;
no genuine predecessor binary executes that step yet). Keep the normal-open
format fences: explicit conversion does not grant serving support for
v6/v7/v8/v9.
See the [support matrix](versioning.md#storage-upgrade-support-matrix).

The system tests start workspace binaries on ephemeral localhost ports. Set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` only in constrained local sandboxes; CI's configured owners must not skip.

## Commands

Focused iteration:

```bash
cargo test -p omnigraph-engine --test traversal
cargo test -p omnigraph-engine --test writes concurrent
cargo test -p omnigraph-server --test data_routes
cargo test -p omnigraph-cli --test cli_data
cargo test -p omnigraph-cluster --test failpoints --features failpoints
cargo test -p omnigraph-bench --locked
```

GQT commands run from any directory inside the checkout; the workspace Cargo
configuration enables the DST runtime that corpus files request:

```bash
cargo test -p omnigraph-gqt --locked                            # complete corpus and harness tests
cargo test -p omnigraph-gqt --test gq_logic_tests issue_563      # matching case names
cargo test -p omnigraph-gqt --test gq_logic_tests -- --list      # one line per case
```

Discovery includes every `.gqt` file below `cases/`, recursively. Shared
cases live at its root; v2-specific cases live in `v2/`, and plan assertions
in `v2/planner/`. A v2 case still explicitly selects `engine = v2`; directory
placement does not select an engine.

Every case is its own libtest test named `case::<relative/path>.gqt`, registered
at run time (`datatest-stable`), so the ordinary name filter selects cases, a
case-only pull request needs no Rust change, and `cargo-nextest` sees each
case (an IDE's test-results view lists cases from the
libtest-shaped output; no per-case gutter runnable exists, since no source
item does). `--test-threads=<n>` bounds how many cases run
concurrently (default: the machine's available parallelism); each case fails
if it exceeds `OMNIGRAPH_GQ_CASE_TIMEOUT_SECS=<n>` seconds (default 10);
`OMNIGRAPH_GQ_BLESS=1` rewrites the failing step's expect rows, or its shape
lines, in place (local workflow only, never CI). Every rows step carries a `--- expect shape`
section, one `<name>: <type>` line per result column in `.pg` property syntax
(`p.age: I32?`), checked against the executed result before the rows, and the
executed schema is also checked against the compiler's inferred schema, so a
wrongly typed column fails even when every cell is null (RFC 0045
§Comparison semantics). Every `ok`/`FAIL` line carries the case's elapsed
time, and a case over budget belongs in a `heavy-repro:` `#[ignore]`d test
under `crates/omnigraph/tests/repro_issue_*.rs`, not the corpus. A name filter
that matches no case is libtest's ordinary green zero-test run; read the
`filtered out` count.

Canonical workspace graph:

```bash
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-dst --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
cargo test -p omnigraph-gqt --locked --lib --test runner_dispatch
```

The feature-superset command compiles the current tree with failpoint hooks present but inert unless a test enables one; it also runs the seams crate's seam guard, the check a cases-only change can turn red (CI runs it again in `GQT (ordinary)`, where `Test Workspace` is skipped). The separate `GQ Logic Tests` context owns GQT: the `runner_dispatch` command above covers dispatch (CI also runs it with `RUSTFLAGS` cleared to prove unavailable-DST refusal), and the complete corpus command runs both execution targets. Neither command substitutes for the other. Also run formatting and both workspace Clippy graphs plus configured GQT Clippy; [ci.md](ci.md) lists the exact gates.

AWS server support has a separate feature owner:

```bash
cargo test -p omnigraph-server --features aws
```

S3-backed tests skip unless `OMNIGRAPH_S3_TEST_BUCKET` and the corresponding AWS endpoint/credential variables are set. Azure-backed tests skip unless `OMNIGRAPH_AZURE_TEST_CONTAINER` and the documented Azure/Azurite variables are set. A configured CI backend treats a skip as failure.

### Plan replay

Query behavior has two test tiers. A `.gqt` case owns what is visible in rows, counts, result column types, or errors. A Rust test owns what the format cannot express: mechanism, scale, process environment, concurrency. The claim that a v2 run is a function of its bound plan and the snapshot is of the second kind: no case reaches the replay door, and the GQT runner applies no replay check of its own to a step (its own checks are the report-shape parse of the `ran` lines' input, `schema_drift` and `check_pin`). The runner once replayed every v2 step against its report (two per-case replay invariants); that fence went when the plan started carrying everything execution reads, because a per-case replay in the same process found nothing the tests below do not find once, and cost every case a second run. The mapping from plan nodes to operators needs no check here: its rule is in [execution.md](execution.md#v2-plan-lowering-one-node-one-operator).

`crates/omnigraph/tests/engine_v2_plan_replay.rs` owns the replay. Each test gathers one query through `Session::query_inspected`, serializes the `BoundPlan` it emitted through the planner's mirrors, reads it back (it must read back equal), and executes it twice through `Session::replay_bound_plan`, a door that takes the bound plan and the engine context and nothing else about the query. Each replay must return the run's rows and repeat its trace: `id`, `operator` and `status` per row, `rung` and `ran` per attempt, and `actual_rows` wherever both attempts are `drained`. `drained` says the consumer pulled the operator's stream to its end; below an operator that stopped early (a `Limit`, a `Sort` with a fetch), a producer's count is a lower bound that depends on how far ahead its channel ran, and is not compared. One query per node kind that has a switch or a ladder: a `HashJoin` over an `Expand` (both switches on the report), a `nearest` ladder with an edge (two rungs), an `rrf` fusion, a `Limit` of zero (everything below skipped), a bulk `AntiJoin`, an `Aggregate`. Three more tests own the pins: after an edge write the replay of a traversal is refused, after an insert the replay of an unfiltered count is refused, and a write to a table the plan never read leaves the replay accepted, because `engine::plan_pins_snapshot` compares the plan's `Assumptions.datasets` (path, Lance branch and version per table key the planner read, or its absence) against the snapshot.

The scrubbed side is `crates/omnigraph/tests/engine_v2_scrubbed_replay.rs` (process environment, which no case can express): a grep that fails on any `std::env` read under `crates/omnigraph/src/engine/` outside `plan_source.rs`, the one permitted reader of configuration, before planning; two replays that set an ambient value the plan did not capture (the task-local memory limit, `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`) and require the captured one to win; and the replay of a plan gathered under one `ann_nprobes` in a session set to another. `Session::replay_bound_plan` takes no settings argument and sizes the pool from the plan's memory limit. The inventory sweep is one case per input class that a case can set two ways, `cases/v2/planner/input_*.gqt` (`engine`, `ann_nprobes`, index build state), each requiring equal rows or a plan line that differs, and the clock class is one of the replay plans of `engine_v2_plan_replay.rs`; the `ran` line and the `nprobes` claim of `--- expect plan` are what a differing plan line is spelled with.

The `--- expect plan` of an inspected step reads the explain document of that same `Executed`, never a second planning run; its `ran` lines read the report of that run (`crates/omnigraph-gqt/src/report.rs` reads the rows). A parameter refusal, a settings error and a query that failed produce no `Executed` and keep their ordinary checks.

### GQT execution through DST

GQT files select their execution target in a `--- runner` YAML section before
the schema. The file owns its storage, seeds and explicit faults;
the runner preserves GQT assertions and uses isolated seeded processes.
The workspace Cargo configuration enables the Tokio runtime DST cases need
from any directory inside the checkout; a build that overrides it (an env
`RUSTFLAGS` without the cfg, as CI's refusal step does) explicitly refuses
DST cases. The [GQT README](../../crates/omnigraph-gqt/README.md)
defines supported targets, hooks, replay observations, and limits. The configured
CI owner enrolls the complete corpus.

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
run without `--archive` emits diagnostic output only. The empty `RUSTFLAGS`
clears the workspace's development `--cfg tokio_unstable`; the runner refuses
a build whose build script saw encoded Rust flags:

```bash
RUSTFLAGS= cargo run --release --locked -p omnigraph-bench -- \
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
