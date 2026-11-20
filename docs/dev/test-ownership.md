# Test ownership

**Audience:** anyone looking for the suite, fixture or command that owns a behavior
**Authority:** the detailed ownership reference; the rules and the short map
are in [testing.md](testing.md)

Find the existing owner here, run it as a clean baseline, and extend it
instead of creating a parallel fixture.

## Test layout

Loose integration suites share a `tests/integration/main.rs` root, with one
`mod <name>;` per suite. Run one suite with `--test integration <name>::`, and
one test with `--test integration <name>::<test> -- --exact`; a CI job that
needs a module's test names it that way too, since nextest and libtest report
it as `<name>::<test>`. Custom harnesses and tests that need process isolation
remain under `tests/` as separate binaries: a file that arms failpoints for the
whole process, re-executes its own binary (`current_exe`), mutates the process
environment, or, for the server's `multi_graph.rs`, exhausts the process-wide
export budget and must not share a binary with the other export tests.

| Package | Primary owners | Shared support |
|---|---|---|
| `omnigraph-compiler` | In-source parser, catalog, type-checking, lowering, and lint tests; `query::check::tests::compile_growth`, which holds compile work (term nodes, expression nodes, steps) linear in each construct family and compiles the sizes the old bounds refused | Module-local fixtures; `arbitrary`, the generators every [generated law](#generated-laws) draws from |
| `omnigraph-planner` | In-source optimizer/cost tests and `tests/integration/query_plan.rs`, `tests/integration/registry.rs`, `tests/integration/lower_walk.rs`; plan assertions over real snapshots live in GQT | `PlanSource` fixtures for metadata and refusal states |
| `omnigraph-storage` | In-source control-object storage, CAS, locking, and URI tests | Module-local fixtures |
| `omnigraph-seams` | In-source tests of the seam type: slot scopes, the guard, the decision behaviors; `tests/failpoint_names_guard.rs`, the source walk over the engine, core, catalog, cluster and DST crates and the `.gqt` corpus that keeps every seam catalogued, crossed and armed | None |
| `omnigraph-core` | In-file `#[cfg(test)]` tests of the error type, branch names, branch control, Lance clone, metadata, full-text analysis (the probe corpus and the [measured-fingerprint golden](#full-text-fingerprint-golden)), full-text compatibility and instrumentation | Module-local fixtures; `fts/` holds the probe corpus and the golden |
| `omnigraph-catalog` | In-source tests: `crates/omnigraph-catalog/src/tests.rs` for `__manifest` publication, state and lineage, plus in-file tests in `migrations.rs` and `retention.rs` | Module-local fixtures; `omnigraph-core`'s `test-util` helpers |
| `omnigraph-engine` | `crates/omnigraph/tests/` plus focused in-source tests | `tests/helpers/` and `tests/fixtures/` |
| `omnigraph-policy` | In-source Cedar policy parsing and evaluation tests | Module-local fixtures |
| `omnigraph-cluster` | In-source lifecycle, deployment and admission tests; `tests/failpoints.rs`; `tests/identity_recovery.rs`; `tests/s3_cluster.rs` | Module-local fixtures |
| `omnigraph-mcp` | In-source protocol, settings, catalog and bounded-result tests | Module-local fixtures |
| `omnigraph-server` | `crates/omnigraph-server/tests/` | `tests/support/mod.rs` |
| `omnigraph-cli` | In-source application, runtime, parsing, output and transport tests plus `crates/omnigraph-cli/tests/` | `tests/support/mod.rs` |
| `omnigraph-skill` | In-source `src/lib.rs`: the embedded table is sorted and `file` finds every path. The CLI owns the rest. In-source `src/skill.rs` holds the skill's command index (`references/commands.md`) and condensed reference (`SKILL.md`) to the clap tree and its storage-format line to `omnigraph version`; `integration/cli_schema_config.rs` owns `skill export` and `skill show`, including byte identity with `skills/omnigraph`; `scripts/check-docs.py` holds `metadata.version` and the declared HTTP contract to the CLI | None |
| `omnigraph-dst` | `crates/omnigraph-dst/tests/` (`scenarios.rs`, `integration/lane_b.rs`, `integration/torn_init.rs`) plus in-source proofs | Crate-local fixtures. Deterministic simulation; needs `--cfg tokio_unstable` (the workspace `.cargo/config.toml` sets it for every build; the default workspace gate excludes the crate by name). Run from `crates/omnigraph-dst`: its `[env]`-only `.cargo/config.toml` supplies the pool trio that `require_pool_env` asserts at process start. `#[ignore]`d tests are fleet/hunt instruments driven by the DST workflows |
| `omnigraph-bench` | In-source configuration tests, the local-route agreement tests (`routes_agree` in `src/gqt_tests.rs`: one scenario on the direct, object-store and read-through routes with the same verification and equal storage calls) and `crates/omnigraph-bench/tests/` | Checked-in cases and suites under `benchmarks/` |
| `omnigraph-gqt-core` | In-source format, expectation and ordinary execution tests | Shared parser and executor used by the GQT runner and benchmark harness |
| `omnigraph-reference` | In-source `src/tests.rs`, one test per core operator of the RFC's operator table, `Fix` and `Call` included, and per lowered form (`optional`, `union`'s positional names, a call bound in the caller's scope, `distance`, an edge binding, `limit $n`, the lazy `if` and `coalesce`, an inline read's group per outer row, a block against an outer scalar, `rrf`'s one row per target), the lexical rules (any-term matching, a null document, the edit budget, a profile's whole pipeline, `english_v1`'s stemmer and stop list included, `bm25_v1`), parameter binding and the refusals; `tests/dependencies.rs`, which holds the crate to the compiler, `serde_json`, `libm`, the Snowball stemmer (`frostem`) and `unicode-normalization`, and the GQT runner to being its one dependent | Module-local fixtures; GQT's comparison of every read step on the direct engine ([GQT README](../../crates/omnigraph-gqt/README.md#reference-comparison)) is its corpus-wide use |
| `omnigraph-gqt` | `tests/gq_logic_tests.rs`, one libtest test per `.gqt` case (`datatest-stable`, `harness = false`), `tests/gq_layout_tests.rs`, every case again under each physical layout, the generated laws in `tests/integration/law_*.rs`, plus runner self-tests, the corpus layout check and `tests/integration/query_corpus.rs`, which parses every documented query text (the guides' and the skill's ```` ```gq ```` blocks, the `.gq` files) and type-checks each `.gq` file that has a `.pg` beside it | The `.gqt` corpus under `crates/omnigraph-gqt/cases/`; author-marked slow cases under `crates/omnigraph-gqt/cases_slow/`, run by the `GQT slow nightly` workflow and never by `cargo test`; format in RFC 0045 |

Do not copy server or CLI process setup into a new suite. Their support modules own hermetic configuration, binary startup, temporary roots, and common assertions.

Test helpers that live in `omnigraph-core` or `omnigraph-catalog` and are reached by another crate's tests are gated `#[cfg(any(test, feature = "test-util"))]`. A plain `#[cfg(test)]` is not enough: `cfg(test)` is set per crate, so a dependent crate's test build compiles the base crate without it and cannot see the helper. The engine enables `test-util` on both crates through its dev-dependencies in `crates/omnigraph/Cargo.toml`, so the helpers exist only in test builds and never in a release artifact. `omnigraph-cluster` follows the same pattern: the server enables its `test-util` feature through its dev-dependencies to read a local cluster's serving snapshot with the storage root spelled as an `s3://` prefix, which is how the server's strict-boot test reaches the external Blob base overlap quarantine without an object store. `forbidden_apis.rs::split_crate_test_util_is_enabled_only_by_dev_dependencies` refuses a production enable of any of the three crates' `test-util` (`SPLIT_CRATE_PACKAGES` plus `TEST_UTIL_SEAM_PACKAGES`).

`tests/forbidden_apis.rs` walks the engine, `omnigraph-core` and `omnigraph-catalog` sources; a line that carries the sentinel comment `// forbidden-api-allow: <reason>`, on the line itself or the line above, is exempt from the lexical deny-list only (the structural graph-write guard still counts it), so every exemption shows up in review.

## Engine ownership

The engine integration suite is grouped by behavior, not implementation module:

| Concern | Existing owners |
|---|---|
| Initialization and representative journeys | `integration/lifecycle.rs`, `integration/end_to_end.rs`, `integration/composite_flow.rs`, `integration/consistency.rs` |
| Query results and operators | `integration/aggregation.rs`, `integration/literal_filters.rs`, `integration/ordering.rs`, `integration/traversal_indexed.rs`, `integration/traversal_adaptive.rs`, `integration/proptest_equivalence.rs`; the `.gqt` corpus lives in `omnigraph-gqt` (`crates/omnigraph-gqt/cases/`) |
| Execution and memory | `integration/execution.rs`, `integration/execution_memory.rs`; `integration/repro_issue_703.rs` and `integration/repro_issue_723.rs` own ignored scale symptoms |
| Plan nodes' operators and the execution report | In-source `engine/report/tests.rs` owns which operator each node builds and what its row says (`ran`, `actual_rows`, `drained`); `crates/omnigraph-planner/tests/integration/lower_walk.rs` owns the walk's order and refusals; the `ran` lines of `--- expect plan` read the report of the case's own run |
| Plan sufficiency: the bound plan alone reproduces a run | `crates/omnigraph/tests/integration/plan_replay.rs` (see [Plan replay](#plan-replay)): one query per node kind with a switch or a ladder, replayed twice through `Session::replay_bound_plan` from its serialized replay envelope, equal rows, the same trace and the same search decisions required; `crates/omnigraph-planner/tests/integration/bound_plan.rs` owns the serialized form; in-source `engine/search.rs` tests own the declared ladder's stepping; `engine/plan_source.rs` tests own that the assumed memory limit sizes the plan, the lowering and the pool |
| Plan acceptance: every read plan checked against its checked query | In-source `crates/omnigraph-planner/src/validate/tests.rs` owns one broken plan per check (binding, traversal and search identity, a fusion's arms checked scope by scope, predicate retention and origin, block strategy, row cut, order and tie-break including fused and grouped searches, projection and score origin, hydration, declared policies, an exact `bm25`'s scoring corpus), the key-narrowing conjunct admitted only with its re-derived id, exact-fragment membership over admitted and excluded forms, the derivation's rules and rejections (hydrated members included), entry parity under an exhausted limit, and the `instrument:` derivation cost curve; `engine/plan_source/key_tests.rs` owns the narrowed key on a real snapshot and its refusal when edited; `integration/plan_replay.rs` owns that a saved plan failing well-formedness or a requirement is a bad request at the replay door; `validate/replay.rs` and `validate/budget.rs` tests own decoding limits, version refusals and budget charges; `integration/execution.rs::every_read_door_shares_one_acceptance` owns that the run, both explain doors and the inspected run accept one plan and refuse alike; GQT cases claim the scope with `validation scope <scope>` in `--- expect plan`; `validate/tests.rs` also owns the placement check's broken plans, derived from the core read (a conjunct written at or after the first one that can fail moved into a scan or tested before an earlier one), and in-source `placement::tests` the law itself, a stand-in selecting node included |
| Scrubbed replay: no input beside the bound plan and the snapshot reaches execution | `scrubbed_replay.rs` owns the mechanism (the `std::env` grep over `crates/omnigraph/src/engine/`, the replay under an ambient memory limit and an ambient `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER` the plan did not capture, `ann_nprobes` as a plan field, the `profile` rows); `integration/plan_replay.rs` owns the snapshot pins (an edge write or an insert after planning refuses the replay); the inventory cases `cases/planner/input_*.gqt` own one input class each (`ann_nprobes`, index build state), and the clock class is owned by `integration/plan_replay.rs`; `integration/execution_memory.rs` owns `ran id_lookup` under a refused build |
| Declared switches: the report names the side a run took | `cases/planner/switch_ran_on_the_report.gqt` (the `ran` line of `--- expect plan`); in-source `engine/report/tests.rs` owns the switch's row and a scan whose plan declares no fallback |
| Search and physical indexes | `search.rs`, `integration/scalar_indexes.rs`, `lance_surface_guards.rs`, `cases/rrf_*.gqt` (fusion rows against the reference interpreter, every population shape included), in-source `engine/operators/fuse.rs` tests (ranks counted once per target per arm), `integration/repro_issue_563.rs` (`#[ignore]`d overflow-scale symptom tier) |
| Writes, validation, schema, and policy | `writes.rs`, `integration/validators.rs`, `schema_apply.rs`, `policy_engine_chassis.rs` |
| Branches, snapshots, diffs, and merges | `integration/branching.rs`, `integration/point_in_time.rs`, `integration/changes.rs`, `integration/merge_truth_table.rs`, `integration/merge_fast_forward.rs` |
| Recovery and crash windows | `integration/recovery.rs`, `failpoints.rs` (including the `live_handle_*` liveness owners: a live handle writes again once faults stop, without reopening), `detached_commit_matrix.rs` (the RFC 0067 writer × window × fault × recovery-actor matrix over the insert, multi-table, load, cleanup, ensure-indices, full-text-rebuild, merge, schema-apply, optimize, system-column-upgrade and Blob put and clear writers; the same-handle liveness actor runs by default and `OMNIGRAPH_MATRIX=full` adds the other-process and cleanup actors), in-source manifest/recovery tests |
| Writer epoch: the row, the check, the holder and the claim | In-source `crates/omnigraph-catalog/src/tests.rs` (`v17_…`) owns the row's round trip, the check's outcomes, the claim's one version, two claims racing, the branch-identity precondition and the refusal below stamp 17, a second process being a publisher with a holder registry of its own; `failpoints.rs` (`branch_claim_…`) owns the door across two real processes through the child-process helper, since the holder is process-wide: a parked publish fenced, a claim above a late write, the race, a recreated branch, a lost acknowledgement, an in-doubt claim, a write whose future is dropped after its commit, a replaced owner's delete that matches nothing in its view and its claim left in doubt, a branch its own process deletes, and what an owner reads of a bound branch another process deleted, until its next write; `policy_engine_chassis.rs` owns the `branch_claim` gate; `integration/write_cost.rs`, `integration/write_cost_s3.rs` and `integration/branch_control_cost.rs` own that a write on a held bound branch issues the requests of an unclaimed one less its head probes, and that a claim costs one publish; `integration/warm_read_cost.rs` (`held_bound_branch_…`) owns that a read there issues no probe, that a second handle of the owner process reaches its publish without one, and that a held branch that is not the bound one is read as before its claim; in-source `v17_the_holder_version_follows_each_landed_publication` owns the version the skip compares and its withdrawal by a failed or dropped attempt; `forbidden_apis.rs` owns that the claim alone writes the row and the holder, that a held head moves only where its version is recorded or withdrawn, and that a branch is retired only where its holder is dropped; DST `WorldOp::WriterClaim` (`Scenario::writer_claims`, fault-free universes only) runs claims among random operations of one process, and the `WriterClaim` rows of the [storage-cost golden](#dst-storage-cost-golden) pin a first claim's object-store calls; the `.gqt` case `held_branch_owner_reads_its_own_writes_across_restart.gqt` owns that the branch's owner reads back its own writes before and after a restart, through the `# writer: held` header pin ([GQT README](../../crates/omnigraph-gqt/README.md#plan-expectations)), which is how a case runs on a held branch; `held_branch_owner_reads_a_write_whose_publish_stays_in_doubt.gqt`, `held_branch_contended_writes_land_and_are_read.gqt` and `held_branch_write_that_fails_before_its_publish_changes_nothing.gqt` own a held publish left in doubt, held writes under contention, and a held write that fails before its publish followed by one that matches nothing; gqt-core's `writer_header_…` tests own the pin's one value, its refusal on `--store` and the runner's claim, and the bench's `raw_dataset_plan_…` and `writer_pin_is_refused_on_a_workload_file` tests the refusal of a dataset recipe and of a workload file |
| Maintenance and substrate fences | `integration/maintenance.rs`, `lance_surface_guards.rs`, `integration/lance_version_columns.rs`, `forbidden_apis.rs` |
| Export and lineage | `integration/export.rs`, `integration/lineage_projection.rs` |
| Legacy-vintage graphs (`id`/`src`/`dst` spellings, born at the current stamp) | `integration/legacy_columns.rs` — load, query, export round trip, evolution; needs `--features failpoints` |
| System-column upgrade (RFC 0040 step 3: respelling in place on a supported standalone graph; vintage is independent of the storage stamp) | `system_column_upgrade.rs`: check and execute, preflight refusals, every window before the manifest commit leaving no residue, a complete contract and table state after a post-commit failure, same-handle retry, the control-object cost; needs `--features failpoints` |
| Cost and benchmark contracts | `integration/write_cost.rs`, `integration/write_cost_s3.rs`, `integration/warm_read_cost.rs`, `integration/branch_control_cost.rs`, `integration/merge_cost.rs`, `integration/changes_cost.rs`, the checkpoint/head lookup instruments, the correlated-block strategy instrument `integration/anti_join_cost.rs`, the GQT benchmark adapter tests in `omnigraph-bench`, and, for the exact storage calls of one operation, the [DST storage-cost golden](#dst-storage-cost-golden) |

Use `tests/helpers/mod.rs` for the standard graph, snapshots, row reads, Blob selectors, and bounded Blob collection. Recovery helpers belong in `tests/helpers/recovery.rs`; object-store counters belong in `tests/helpers/cost.rs`; graphs whose rows trip the ordered-scan sorter cap belong in `tests/helpers/wide_rows.rs`.

`integration/changes_cost.rs` owns the change-feed cost boundary: transaction-footprint
candidate scans, bounded page work, and caught-up versus backlog polling curves.

### Recovery and failpoints

Crash tests must cover the writer and the user-visible reopening behavior:

- `tests/failpoints.rs` owns crash windows around durable effects: after a detached effect and before publication, where the graph is unchanged, and after publication, where the pin is complete;
- `tests/detached_commit_matrix.rs` owns the writer × window × fault × recovery-actor matrix under one oracle;
- `tests/integration/recovery.rs` owns manifest-only contract admission, ignored orphan schema artifacts and read-only opens without writes; a sidecar from an older build refuses read-write but not read-only open. Read-write open of a local root retains its temporary create-if-absent capability probe;
- `tests/lance_surface_guards.rs` owns the Lance detached-commit facts the pin and the collector depend on;
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

The collector retry owners in `failpoints.rs` exercise each sweep window
on file, RustFS/S3 and Azurite. The between-manifests fixture also reopens
through `open_with_io_scope`, covering served file I/O's separate Lance
provider. The DST dead-staging pin exercises the census, hunt and fleet
constructors; its maintenance sensitivity owner requires a failed deferred
cleanup retry to fail the universe. Neither local emulators nor shared-memory
DST qualify live Azure lease loss or delayed accepted writes.

When adding a new writer, update all of these layers. See [recovery.md](recovery.md).

### Blob behavior

Blob coverage is deliberately split:

- engine `integration/end_to_end.rs`, `integration/branching.rs`, and in-source Blob tests own logical cell selection, snapshots, integrity, ranges, external classification, and write admission;
- engine `integration/export.rs` owns export's batched managed Blob read (one read per Blob column per batch, in request order, shared by the change-feed baseline) and `integration/changes.rs` the change images' reads;
- engine `integration/maintenance.rs` owns Blob compaction (the batch derived from a row's summed Blob columns, fragments with deleted rows, per-task sizing in `integration/maintenance.rs::optimize_sizes_each_compaction_task_from_its_own_fragments`, external references counting nothing in `integration/maintenance.rs::optimize_does_not_size_a_blob_batch_by_external_references`);
- cluster tests own persisted external-source policy and serving projections;
- server `integration/data_routes.rs`, `integration/auth_policy.rs`, and `integration/openapi.rs` own GET/HEAD, auth, conditions, ranges, redirects, backpressure, and schema drift;
- CLI `integration/cli_data.rs` owns `blob get/stat/put/clear`; `integration/parity_matrix.rs` compares embedded and remote results; `integration/system_remote.rs` owns a put's or clear's lost delivery through the fault proxy.

Do not exercise a server promise solely through the engine facade. The complete contract is summarized in [blob.md](blob.md).

### Lance compatibility

Run this first for every Lance change:

```bash
cargo test -p omnigraph-engine --test lance_surface_guards
```

The guards pin only substrate behavior Omnigraph actually depends on: version and row columns, transaction witnesses, primary-key conflict filters, branch/ref cleanup, the stock-cleanup sweep of old files used only by detached versions (with the attached manifest timestamp preceding the file and the file preceding the seven-day cutoff), index coverage and segment ownership, the full-text tokenizer a profile's native parameters build, stable row IDs, native V2 vector ordering, Blob reads through compaction, and the detached-commit privacy, twin replay and self-conflict rules that RFC 0067 builds on. If an upstream limitation disappears, remove the workaround and replace its negative guard with the native behavior the caller now requires. A persisted layout still needs its own format decision.

Qualify a dependency bump in both debug and release profiles: upstream unstable
capabilities can have different gates under `debug_assertions`. Run the native
surface guards in each profile, then the affected write, schema, branch,
search, maintenance, Blob and bounded-memory owners listed in
[the compatibility fences](lance-fences.md). Keep the
canonical workspace, GQT, Clippy and configured object-store gates as separate
evidence; a local file pass does not qualify S3 or Azure.

Experimental-overlay fixtures keep their positive debug checks and assert
release refusal with unstable opt-ins absent. The collector owner also runs
cleanup and proves that the original and overlay inventories remain intact;
the planning-cost owner continues its modern and legacy cases after refusal.
Do not enable experimental capabilities to make release qualification pass.

Cross-version evidence must begin with bytes written by the genuine predecessor,
not a current fixture with a changed writer version. Check indexed and uncovered
search, nullable vectors, managed Blob payloads, branches and historical reads
before and after candidate writes, compaction and cleanup. Restore a whole-root
backup and read the candidate-written root with the predecessor before claiming
a qualified rollback. This is independent of graph-stamp conversion; unchanged
V2_2 files and an unchanged graph stamp do not establish reader compatibility
on their own.

The core `captured_provider` test runs isolated subprocesses because native
provider environment snapshots are process-wide. It exercises S3 and Azure
control-object and native Lance reads/conditional creates against fake HTTP
endpoints, destination aliases and ambient-option drift, including cached Azure
options and rotating credential providers. Storage's `binding` unit tests own
option classification and fixed defaults; these are not live-cloud qualification.
The engine's `bound_storage.rs` (S3-configured, run by the RustFS job; its own
binary because it re-executes itself) carries a bound handle's whole workload
past that capture: a child whose ambient endpoint variables name a recording
decoy, with unsigned requests and disabled conditional puts, runs writes, the
`@card` live re-read, a racing publish, a branch create, merge and delete,
historical CSR and commit-addressed reads, the change feed, export and
maintenance, and the decoy must see no request. The in-source
`changes::enumerate` test owns the change feed's fresh re-prove session.

### Plan replay

Query behavior has two test tiers. A `.gqt` case owns what is visible in rows, counts, result column types, or errors. A Rust test owns what the format cannot express: mechanism, scale, process environment, concurrency. The claim that a run is a function of its bound plan and the snapshot is of the second kind: no case reaches the replay door. On every successful rows step, the GQT runner checks the report shape, `schema_drift`, `check_pin`, and the plan root's complete declared result schema against both the compiler's retained declarations and the executed columns. Result-schema inference reads the checked query's stored types; it checks propagation and execution, not an independent inference algorithm. Inference compares nullability flags; execution compares names in order, full Arrow types (including node Struct members), and null cells in declared non-null results. It also serializes and deserializes the executed `BoundPlan`, requires equal physical explain documents, and directly compares stored schemas, return declarations, named node-object types and complete block predicates/specs. This checks that the plan's stored decisions survive the mirror without executing another query. The mapping from plan nodes to operators needs no check here: its rule is in [execution.md](execution.md#v2-plan-lowering-one-node-one-operator).

`crates/omnigraph/tests/integration/plan_replay.rs` owns the replay, including refusal of altered result schemas and missing, duplicate, unused or altered node-object declarations. Each test gathers one query through `Session::query_inspected`, serializes the run's replay envelope (`Executed::replay_envelope`: the query's source and name, the accepted scope, the schema digest, the `BoundPlan` through the planner's mirrors and an exact-fragment member's derivation), reads it back (the plan must read back equal), and executes it twice through `Session::replay_bound_plan`, a door that recompiles the named query against the target's catalog, binds it to the plan's own values, accepts the plan again and takes no session setting. Each replay must return the run's rows, record the run's search decisions (gate verdicts and probe attempts) and repeat its trace: `id`, `operator` and `status` per row, `rung` and `ran` per attempt, and `actual_rows` wherever both attempts are `drained`. `drained` says the consumer pulled the operator's stream to its end; below an operator that stopped early (a `Limit`, a `Sort` with a fetch), a producer's count is a lower bound that depends on how far ahead its channel ran, and is not compared. One query per node kind that has a switch or a ladder: a `HashJoin` over an `Expand` (both switches on the report), a `nearest` ladder with an edge (two rungs), an `rrf` fusion and a three-arm fusion with exact arms whose edited arm is refused, a `Limit` of zero (everything below skipped), a bulk `AntiJoin`, an `Aggregate`. Three more tests own the pins: after an edge write the replay of a traversal is refused, after an insert the replay of an unfiltered count is refused, and a write to a table the plan never read leaves the replay accepted, because `engine::plan_pins_snapshot` compares the plan's `Assumptions.datasets` (path, Lance branch and version per table key the planner read, or its absence) against the snapshot; a pin mismatch and a changed schema are conflicts. The envelope's own outcomes are tested there too: another envelope version asks for the query again, an exact-fragment member replays only with its unedited derivation, an edited plan that drops a predicate is invalid evidence, and an exact `bm25`, which reads no full-text index, replays to its run's rows over a partially indexed property.

The scrubbed side is `crates/omnigraph/tests/scrubbed_replay.rs` (process environment, which no case can express): a grep that fails on any `std::env` read under `crates/omnigraph/src/engine/` outside `plan_source.rs`, the one permitted reader of configuration, before planning; two replays that set an ambient value the plan did not capture (the task-local memory limit, `OMNIGRAPH_EXPAND_INDEXED_MAX_FRONTIER`) and require the captured one to win; and the replay of a plan gathered under one `ann_nprobes` in a session set to another, a fusion's `nearest` arm included. `Session::replay_bound_plan` takes no settings argument and sizes the pool from the plan's memory limit. The inventory sweep is one case per input class that a case can set two ways, `cases/planner/input_*.gqt` (`ann_nprobes`, index build state), each requiring equal rows or a plan line that differs, and the clock class is one of the replay plans of `integration/plan_replay.rs`; the `ran` line and the `nprobes` claim of `--- expect plan` are what a differing plan line is spelled with.

GQT also owns the public ANN diagnostics contract: every inspected query checks
metric types and names, attempt identities, scan/probe attribution and same-run
profile preservation. Existing ranked-scan cases use `diagnostics present` to
refuse silent metric loss, including one missing RRF arm; see the
[GQT plan assertions](../../crates/omnigraph-gqt/README.md#plan-expectations).
Rust retains controlled partition layouts and independent native-counter oracles
in `search.rs`, plus synthetic missing/zero/unknown/maximum metric inputs and
replay separation in `engine/search.rs` and `engine/report/tests.rs`.

The `--- expect plan` of an inspected step reads the explain document of that same `Executed`, never a second planning run; its `ran` lines read the report of that run (`crates/omnigraph-gqt-core/src/report.rs` reads the rows). A parameter refusal, a settings error and a query that failed produce no `Executed` and keep their ordinary checks.

### Generated laws

The GQ 3 implementation is held to laws over generated inputs, not to a
handful of examples. One generator module, `omnigraph_compiler::arbitrary`
(gated on `cfg(test)` or the compiler's `test-util` feature, which only
dev-dependencies enable; `forbidden_apis.rs` pins that), draws schemas,
graphs, well-typed GQ 3 reads and scalar expressions from one choice
stream, so proptest shrinks the schema, the graph and the query to minimal
forms. Each law lives with the owner of its boundary:

| Law | Owner | Per pull request | Nightly |
|---|---|---|---|
| Generated reads type-check (generator soundness), and every registry scalar row, `stddev`, `percentile`, `collect`, `collect(distinct …)`, `nulls first` and `nulls last` is drawn | compiler `arbitrary::tests` | 2,000 reads | 100,000 |
| A displayed expression types to the expression: the generator's `display` mode (no list membership, whose display is an infix `contains`; wide literal pools; `now()` and `match_terms` beside every other form) draws a predicate and a value, and the text `IRExpr`'s `Display` writes for them type-checks to the same declaration | compiler `query::syntax::tests::round_trip` | 300 expression pairs | 20,000 |
| 1. Engine equals the reference interpreter, under every traversal mode and with every index built, over reads that draw every registry scalar row (`cast` between the pairs it converts, `round` with and without `digits:`, the rows that can fail included) on values with Unicode whitespace, case mappings, negative zero and the texts `cast` reads or refuses, order keys with `nulls first` and `nulls last`, and `collect` as an aggregate, with `distinct` and as a block; two run-time refusals with different codes the read can each raise agree, since the RFC does not fix which one a read whose rows fail in different ways reports; metamorphic: a tautology, reversed conjuncts that cannot fail, the read respelled with the admitted spellings (`case`, `\|\|`, `<>`, single quotes, another letter case), `rrf` arms swapped. Writes are not drawn: the reference evaluates reads only | `omnigraph-gqt` `integration/law_engine_reference.rs` | 64 reads | 2,000 |
| 1b. The placement law: a `where` of two to five conjuncts mixing ones that can fail (a division or remainder over a stored zero, an overflowing product, `sqrt`, `ln`, `left` and `cast` over a stored value, a block read as a value whose own `where` can fail, a guard over them) with pure ones in every written position, after elements that can fail (a `let`, a correlated `from` and an `optional` over a division), answers as the reference does, rows or the same refusal, before and after its pure prefix is reversed and with every index built | `omnigraph-gqt` `integration/law_engine_reference.rs` (`ReadOptions::placement`) | 64 reads | 2,000 |
| 2. Per-row, bound-parameter, folded and reference evaluation of a scalar expression agree, and its type is the RFC's join: arithmetic, guards and comparisons, and every registry scalar row but `size` (`cast` between every pair of the inputs' types it converts) | `omnigraph-gqt` `integration/law_scalar_evaluators.rs` | 300 expressions | 6,000 |
| 3. Typed expressions and the charged term grow by a constant per unit of every construct family (`optional`s, `let`s, guards, nested registry rows, a `case`'s `when`s, blocks, calls) | compiler `check::tests::compile_cost` | 200 reads | 5,000 |
| 4. Every single edit of a generated plan's replay envelope is refused or replays the run | engine `integration/plan_replay.rs` | 24 plans, 40 edits each | 300 plans, 100 edits |
| Every refusal resting on a claim about every conforming graph holds it: with the refusal let through by the checker's test-only switch (`query::check::soundness`), the reference interpreter finds no row (a comparison with `null` in `where`, a literal outside its property's domain under `=` and `in`, a reversed triple or selection, a hop range past one hop between two types, two references of two types under `=`) or a null column on every row (a comparison with `null` in `return`); a selection with one member along the written order returns rows on some generated graph | `omnigraph-gqt` `integration/law_refusal_soundness.rs` | 1,000 schemas and graphs | 50,000 |
| Edition 2 and edition 3 answer alike the generated reads both express: a test-only tree of bindings, triples in declared direction, filters, a negated pattern, return items, a grouped count and a total `order` with `limit`, printed once as edition-2 text for the pinned merge base's `omnigraph` CLI (`OMNIGRAPH_EDITION_2_BIN`) and once as edition-3 text for the engine; equal rows, both refusing, or the one difference the cross-edition table sanctions as `results.hop-range-self-loop`, recognized by re-running both with the start node excluded. Without the binary it skips; `OMNIGRAPH_REQUIRE_EDITION_2=1` fails instead | `omnigraph-gqt` `integration/law_two_editions.rs`, run by `Cross-Edition Comparison` | 48 reads | 1,500 |
| 6. Search answers do not depend on index state: over generated cases of one node type declaring every index kind (BTREE, BITMAP, LABEL_LIST, NGRAM, full text under each analyzer profile, a vector index under each distance; `arbitrary::search`), filters on each indexed column, `contains` over a name and over its `lower`, `match_terms` in both modes with every edit budget, and `distance`, `nearest`, `bm25` and `rrf` of two to four arms with and without `weights` answer as the reference does with no index built, with every index built, and with the indexes built over the first rows before new keys are added and earlier rows replaced whole; the indexed states probe each scalar family. A lone `match_terms` under the profiles without a stemmer keeps the rows a dynamic-programming edit distance admits; a `distance` or `nearest` read returns the RFC's distance computed apart from both implementations, every row or the `k` smallest distances under the tie rule; an `rrf` answers alike under every permutation of its arms and weights, its scores are the formula over each arm's own answer with equal scores broken by the smaller key, and a cut keeps the largest scores under that rule. `cosine` vectors have power-of-two norms, so every distance is exact in `F32` | `omnigraph-gqt` `integration/law_search.rs` | 40 cases of four reads, about 30 s | 2,000 |
| The engine's banded edit distance equals the full dynamic-programming table on generated term pairs (`arbitrary::search::{term, edited}`: a term and the same term after up to four edits, transpositions among them, over ASCII, an accented letter, a sharp s, an ideograph and a digit), at every budget from 0 to 2 | engine `engine::lexical_scan::tests` | 20,000 pairs | 1,000,000 |
| 6b. Every direct-engine case keeps its rows under each physical layout | `omnigraph-gqt` `tests/gq_layout_tests.rs` ([GQT README](../../crates/omnigraph-gqt/README.md#physical-layouts)) | layouts `a`, `b` | `a` to `d` |
| 8. An index path is cheaper than the scan it replaces, in object-store requests to the table it reads under the DST `--measure` ledger, one measured read per case: with every index built the read's requests are fewer than with none (layout `a`); at 4,096 rows in eight fragments and 32,768 in 64 with the same matching rows, the index path's requests stay within a factor of 2 while the scan's grow at least four times; shapes are a key equality (`__id` BTREE), a BTREE, BITMAP, NGRAM and LABEL_LIST probe and a traversal's destination by `id_lookup`, drawing the matching rows and where they sit; every index kind a corpus step's `--- expect plan` claims is one of the shapes; a destination read by `id_lookup` with no identity index keeps its requests within a factor of 2 as its frontier grows four times, over 65,536 rows in 16 fragments. The corpus's own index-path steps are not measured: each seed loads in one fragment, which no scan pays more for than a probe. A BTREE equality on clustered data, which fragment value ranges let a scan prune, is pinned as a `hunt:` | `omnigraph-gqt` `integration/law_index_cost.rs` | 6 shapes, 4 generated reads, the frontier pair | 60 generated reads |
| One plan for one meaning: every canonical form section 6 lists (lint's `L203` and `L206` to `L216`: `case`, `\|\|`, `<>`, single quotes and another letter case among them) has its canonical spelling's typed IR, logical and physical plan, `logical_hash` and rows, over fixed pairs and over generated reads rewritten at random positions (the admitted spellings by `arbitrary::respell_spellings`, from the parse tree); the spellings the grammar refuses with a printed fix (`==`, `&&`, `x not in`, `starts with`, `between`) suggest one with the canonical IR; lint's printed byte-range fix on every flagged corpus read keeps the typed IR and `logical_hash` | `omnigraph-gqt` `integration/law_canonical_forms.rs` | 200 reads | 5,000 |

The nightly twins are `#[ignore = "nightly: …"]` tests enrolled by exact
name: the compiler's and the engine's in `tests-slow-nightly.yml`, the
`omnigraph-gqt` ones and the four layouts in `gqt-slow-nightly.yml`.
Seeds are deterministic (proptest's ChaCha RNG); a failing case is written
to the law's file under `tests/regressions/` beside its crate, checked in,
and replayed first by every later run. To hunt locally, set
`OMNIGRAPH_LAW_CASES` (the count) and `OMNIGRAPH_LAW_SEED` (another seed);
`OMNIGRAPH_LAW_TRACE` prints the reads that returned no row. A failure
prints the shrunk query, the schema, the graph and both answers. Every
generator finding is a defect: fixed at the root with the shrunk case as a
regression (a `.gqt` case when rows show it), or pinned as a
`#[ignore = "hunt: …"]` test beside its law when the fix is not contained.

## Server and CLI ownership

CLI `system_remote` runs the actual CLI, server and fault proxy in the ordinary
workspace gate; its required-cell checks reject removed or ignored cases. The
lost-delivery matrix covers disconnect, truncated response, proxy 504 and caller
timeout without replaying a committed merge or commit publication/drop (a drop's
empty response has no body to truncate). Output unit tests inject write and flush
failures; application tests check baseline file-lock custody through cursor flush,
transfer receipt failures after a single dispatched write, and raw stream failures
without mixed diagnostics. `blob_cli` unit tests own bounded stdin capture and
interrupted reads. Extend `cli_data` for load consent and source-read ordering,
Blob preconditions and output-file lifecycle; reuse its HTTP fixture and the
existing parity matrix. Query semantics remain in GQT, not duplicated in these
CLI boundary tests.
Schema-family application tests verify that failed result delivery preserves an
applied schema and its actor with one publication, and that an undeliverable
target line precedes writable open. The same application owner injects write and
flush failures into mutation-scope diagnostics; `cli_data` checks ordinary and GQ
branch scopes while keeping JSON stdout parseable. Init output failure leaves a readable graph;
lint report failures and subsequent invocations remain reusable. Extend the
existing `cli_schema_config`, `cli_cluster` and `cli_data` owners for addressing,
cluster ownership and offline source-read ordering.

Maintenance delivery tests use the existing output and application owners.
They cover partial writes and flush failures, full-text rebuild publication and
actor retention, and target-output failure before writable admission. Extend
`cli_data` and `cli_cluster` for cleanup preview/consent and maintenance addressing;
`crossversion_upgrade` retains ownership of storage-upgrade compatibility.

Cluster delivery tests extend the application and output owners. Direct apply
tests fail supplied stdout/stderr writes and flushes, then inspect the original
receipt, the released lock of the converged apply, single deployment result and
stable graph history. The
served adapter's tests verify identity delivery before POST and receipt/timeout output failures without
resubmission. `cli_cluster` and `cli_queries` own exact unlock refusals, graph
filtering and wrong-plane refusal before configuration or HTTP access. The existing
`system_remote` deployment matrix owns real server completion after delivery loss;
these CLI changes require no additional query-semantic GQT cases.

Operator schema/routing owners cover whole-document validity, independent
profile/target defaults, single-identity managed profiles, standalone auth
selection and immutable target-to-profile bindings.
Credential-source owners cover bounded stdin/env/file/exec acquisition, absence
of fallback, helper failures and secret redaction. Target/application tests own
static set/remove, no-network/no-helper/noninteractive status and pipe-only export
before secret acquisition. Managed authentication tests own independent profile
sessions, principal/session collision refusal, refresh coordination and logout
cache clearing; lifecycle fixtures retain original-identity recovery checks.
They also own zero, one and multiple active organizations at login, refresh
when the access token expires during selection, and revocation after an
abandoned choice. Application tests own interactive and machine-mode dispatch
and the JSON refusal on stdout. Authentication tests cover each credential
check: a provider response that
does not fit the published sign-in configuration is never reported as a saved
credential's fault. `cli_cluster` owns `organization list` beside the member
and invitation lists, including the 1,000-membership bound.
Transport owners cover prefix boundaries, no redirects, explicit proxy selection,
CA capture and unsupported trust combinations. Extend the shared served-client
fixtures across managed and standalone graph commands instead of adding an
allowlist-specific test setup.

`commands/local` retains version, skill and embedding output ownership.
`skill.rs` owns exact file delivery and its broken-pipe exception; extend
`cli_schema_config` for process behavior. Credential replacement during discovery
must not change the token used by an already captured invocation.

Application and
managed-output tests own completion, partial-write and flush failures; managed
authentication/lifecycle fixtures own the pre-dispatch diagnostic gates and
accepted-identity recovery output when stderr fails. Application tests keep
single-result stdout intact and preserve delivered refusal statuses; served
cluster tests retain typed refusals through failed hints. The binary's channel
barrier test proves reported failures do not wait for blocked runtime work;
`cli_schema_config` owns help/version/refusal statuses with closed streams.
These are CLI boundary contracts; query-semantic GQT cases remain unchanged.

The operator-config owners also cover read-only YAML and parent directories with
writable private credential state, preservation/conflict checks when relocating
legacy state, and explicit profile creation versus default-profile login.
The existing client and managed-auth fixtures delay discovery or a later request
past credential expiry and assert that no authenticated operation is dispatched.
Standalone flat-auth fixtures retain distinct-target identity isolation, including
two targets sharing an endpoint; no auth-name fallback remains.

The `system_remote` deployment matrix holds a real
admitted request through drain, disconnects or times out the caller before
durable acceptance, and loses replies after acceptance. Exact-ID observation
must finish with one ledger result, one schema publication and the same server
PID; neither apply nor recovery polling may resubmit. The old Unix-gateway
fixture does not qualify a version-1 operator transport: Unix socket targets are
unsupported. Keep lower-level transport coverage distinct from public resolver
capability/refusal tests.

Server suites are organized by public route: `auth_policy`, `data_routes`, `schema_routes`, `stored_queries`, `multi_graph`, `boot_settings`, object-store coverage in `s3`, and the generated contract in `openapi`.

The OpenAPI owner checks the [HTTP conventions](http-api.md), their named
exceptions, and required error responses. Its negative cases remove a required
response or code from a spec copy and require rejection; a regenerated snapshot
alone is not proof of complete response coverage.

`omnigraph-mcp` owns protocol, catalog and formatting tests. Server
`integration/mcp.rs` keeps the real HTTP fixture for authentication, policy,
tool/route and error-disposition parity, limits and response lifetimes. The
server adapter's in-source tests own canonical protocol-refusal projection,
including lost and oversized write results; `boot_settings` covers MCP across
shutdown and restart. Extend these owners instead of adding a second server
fixture to the MCP crate.

Per-graph serving transitions extend these owners: in-source `registry` tests
own capture/close ordering, drain-only deployment deadlines, affected activation
scope, schema identity and candidate bounds;
`operations`, `ingress` and the server's MCP adapter own detached execution and
output lifetimes.
`stored_queries` parks a request before engine capture, `data_routes` retains
disconnected writes and stream bytes, and `boot_settings`/`mcp` check authorized
availability. The same owners cover coherent schema/query batch activation; these assertions
are not generic native settlement. Server `boot_settings` exercises authenticated
submission, parked requests, caller disconnect, pre-effect refusal, durable
acceptance before completion, activation-in-progress observation and exact receipt
access after management handoff. It also holds a merge across served schema
planning to prove preview does not wait for the graph gate, and its
`owned_shutdown` journeys own process containment: a panic or an uncertain
owned write exits 2 and retains the lock, while a proven unpublished staging
failure with settled storage I/O is a 500 the server keeps serving through,
with a clean SIGTERM still releasing the lock. In-source
`deployment` tests suspend observer read futures across owner start, finish and complete
turnover, exercising bounded re-observation for aggregate and exact status.
CLI `cli_cluster` owns submit-once polling, transient 429/503 and truncated-body
retries, malformed-receipt refusal, terminal outcomes and caller timeout without
replay. Its managed fixtures cover status/history scope and filters, explicit
managed target selection, and unsupported target-kind refusals before credentials
or external access. Default target selection refuses applicable retired folder
routing; explicit direct targets never route through it. Legacy lifecycle custody
remains guarded even with explicit selection.
Managed deployment fixtures also exercise immutable preview/delivery bodies,
original-ID recovery, current authorization refusals, bounded waiting through
uncertain delivery/archival, and queued-only cancellation. They do not replace
native server execution tests or establish deployed service compatibility.
CLI `cli_cluster_e2e` proves one PID/listener survives schema/query
replacement, graph addition, policy grant/revocation and management handoff;
the original submitter retains only its exact receipt access after restart.
Extend that same journey for graph deletion, proving target storage/history
removal, peer preservation and unchanged PID/listener, including a served deletion
preview followed by `--no-wait` submission and exact-ID `status --wait`. It checks
the achieved configuration again after restart. The local journey also exercises
counted embedding-provider replacement, external-Blob admission changes, catalog
integrity reporting, schema-drift refusal and missing-root refusal. S3/Azure wrappers
share the transport-independent phases; local success does not qualify their
storage-fault paths. CI requires the local journey to execute successfully.

CLI suites own their named planes: cluster lifecycle, data commands, stored queries, schema/config, cross-version rebuild, embedded/remote parity, and local/remote system journeys. Keep `OMNIGRAPH_HOME` hermetic by using `tests/support::cli()` or `cli_process()`.

CLI `integration/cli_mcp.rs` owns the stdio MCP bridge against a real served cluster and a stand-in endpoint: refusals before any I/O, tool listing with and without the bridge's `--write`, graph binding, reads equal to the server's own `/mcp` answers, a Unix-socket endpoint refused before it connects, `--as`, credential re-reading from the target's `auth` source after a 401 without resending, the server's `WWW-Authenticate` challenge relayed with a 401, managed resource identity renewal without replay or control-token forwarding, wrong-incarnation and actor-override refusals, cancellation, and unreachable servers or lost answers that start nothing and resend nothing. In-source `commands/mcp` tests own framing, binding and narrowing rules, and that the bridge names no server or MCP implementation crate.

CLI `integration/cli_data.rs` owns `studio` through stand-in platform openers on `PATH` and a stand-in desktop entry: the `--json` link and its digests equal to `target inspect --json`, the registered handler's own program receiving the link, no credential read, `studio_unavailable` that runs neither `xdg-open` nor a browser on `PATH`, and direct, managed and inline targets refused before any I/O. In-source `commands/studio` tests own the link format, each platform's opener behind an injected command runner, so no test opens anything, and desktop-entry lookup and `Exec` expansion; `target_identity` tests own the `home` and `binding` digests.

Deployment tests extend these owners: cluster `tests.rs` pins no-reset
ledger conversion, captured source bytes, exact-ID lookup, bounded results and
exact applied schema identity after receipt eviction; `admission.rs` pins lifetime
exclusion and exact reconciliation admission. Cluster `tests/failpoints.rs` owns
interruption windows, killed-process recovery and corrective successors, including
deletion before start, during partial removal, after root absence and before
terminal ledger acknowledgement, same-lifetime older manifest survivors,
replacement refusal and corrective creation in empty local settlement residue;
`tests/identity_recovery.rs` owns current-actor authorization and adoption of a
persisted settlement without replacing its author. CLI
`tests/integration/cli_cluster_e2e.rs` owns the root-only deployment round trip, and
`tests/integration/cli_cluster.rs` owns CLI admission. Engine `tests/schema_apply.rs` owns
strict prepared publication and settlement proofs, with actor checks in
`tests/policy_engine_chassis.rs`; catalog tests own numeric CAS. The storage
in-source contract owns bounded same-GET bytes/tokens, and cluster
`tests/s3_cluster.rs` owns the shared S3/Azure backend journey. Passing Azurite is
not qualification of live Azure lease-loss or delayed accepted writes.

The system tests start workspace binaries on ephemeral localhost ports. Set `OMNIGRAPH_SKIP_SYSTEM_E2E=1` only in constrained local sandboxes; CI's configured owners must not skip.

### Cross-version rebuild and storage upgrade

The cross-version rebuild owner, `integration/crossversion_upgrade.rs`, skips each predecessor case when its binary is not configured, so a local `cargo test -p omnigraph-cli --test integration crossversion_upgrade::` is green even while CI's `V5 ↔ V10 Format Fence` is red. To run the fence locally, build the predecessor CLI from the commit `ci.yml` pins as `FINAL_INTERNAL_V5_COMMIT` (`git worktree add <dir> <sha>`, then `cargo build --locked -p omnigraph-cli --bin omnigraph` inside it) and run the exact case with that binary:

```bash
OMNIGRAPH_V5_BIN=<dir>/target/debug/omnigraph cargo test --locked -p omnigraph-cli --test integration crossversion_upgrade::current_v10_refuses_and_rebuilds_genuine_v5_and_v5_refuses_v10 -- --exact --nocapture
```

The v6↔v10 fence works the same way with a released binary: `OMNIGRAPH_V6_BIN` (the 0.10.0 release). The stamps between them are described in [versioning](versioning.md#current-storage-contract). The v0.9 journey is a different case, a fully exercised v6 graph — branches, edges, vectors, full-text and blobs — that the current binary refuses and that is rebuilt from a 0.9 export; `Test Workspace` runs both on every pull request that changes engine input, with the releases it installs.

The separate `Storage Upgrade Compatibility` CI job requires the
`storage_upgrade` cases of `integration/crossversion_upgrade.rs` (the report on a fresh
graph, the cluster-path refusal and the six genuine journeys:
`genuine_lance11_storage_upgrade_round_trip`,
`genuine_v13_storage_upgrade_preserves_history`,
`genuine_v0_11_0_storage_upgrade_preserves_history`,
`genuine_v0_11_0_storage_upgrade_after_predecessor_cleanup`,
`genuine_v0_10_0_to_stamp_8_storage_upgrade_preserves_history` and
`genuine_v0_10_0_to_stamp_9_by_default_storage_upgrade_preserves_history`), the engine
`db::upgrade::tests`, `lance_version_columns` and `forbidden_apis`. Missing
cases, empty runs and skipped required cases fail the job.

The Lance dependency journey uses `OMNIGRAPH_LANCE11_BIN`. CI builds the exact
main commit `0a62bb7b2aa5b99b012f413807c216277f7f9596`, pinned as
`LANCE11_SOURCE_COMMIT`, and the test checks its actual manifest writer version
is Lance 11.0.0. The predecessor writes graph format 16 and the candidate's
publishes keep it; this journey calls no `upgrade`. The predecessor builds full-text and vector indexes, managed Blobs,
nullable outer vectors, an uncovered search tail, branches and history. Cold
candidate reads must leave its whole root unchanged, including index UUIDs and
analyzer certificates. Candidate writes, an explicit full-text rebuild, optimize and cleanup are followed by
predecessor reads of the new state and retained snapshots, including an old
untrained analyzer declaration. Rollback in place after the candidate built or
rebuilt an index is not supported
([upgrade guide](../user/operations/upgrade.md#lance-substrate-upgrades-v010-v014)), so
the predecessor must refuse or answer as the candidate does: its full-text query
on main, whose postings the candidate rebuilt with fingerprint (format 2)
certificates, must fail with its certificate refusal and return no rows, while
its key lookup and its `contains` filters, planned through the NGRAM index the
candidate's optimize built, must equal the candidate's answers. Finally the test
restores its offline whole-root byte-copy backup at the original path, the
supported rollback, and reads it with both binaries. This qualifies those
exercised shapes, not arbitrary external Lance datasets or every possible
downgrade.

The stamp-13 journey needs the stamp-13 CLI: the job builds it from the commit
`ci.yml` pins as `STAMP_13_SOURCE_COMMIT` and exports `OMNIGRAPH_V13_BIN`. It
proves its predecessor by behavior (that binary's `snapshot --json` reports
`internal_schema_version` 13 and the current `upgrade --check` observes 13),
builds branches, a merge, a deleted branch, a recreated one and a fork of a
deleted branch with the old binary, upgrades, and compares commit history,
rows, `cleanup` and a backup restore after it.

The 0.11.x journeys need the released CLIs, which the job installs:
`OMNIGRAPH_V011_BIN` (0.11.0, writes stamp 9) and `OMNIGRAPH_V6_BIN` (0.10.0).
`genuine_v0_11_0_storage_upgrade_preserves_history` runs the same script as
the stamp-13 journey plus a schema apply (a new type and a nullable property
on the existing `Doc`, with a commit before and one after it) and an
`optimize` with the old binary, asserts that the three root schema objects
are byte-identical after `completed`, and compares what the two commits
around the apply answer for the added property before and after the upgrade.
`genuine_v0_11_0_storage_upgrade_after_predecessor_cleanup`
runs the old binary's `cleanup --keep 1` before the upgrade. After the
upgrade, a write and a merge, `cleanup --older-than 7d` refuses a table on a
pre-upgrade linear pin. `cleanup --keep 1` then passes and keeps every
`__manifest` version; `--older-than 7d` and `--keep 100` still refuse, and
`--older-than 0s` passes, as the upgrade guide
describes. A commit the old binary refused as reclaimed must be refused after
the upgrade, any other failure of a read fails the journey, and at least one
commit must still be served.
`genuine_v0_10_0_to_stamp_8_storage_upgrade_preserves_history` builds the
graph with 0.10.0, takes it to stamp 8 with the 0.11.0
`upgrade --to-format 8`, and upgrades from there;
`genuine_v0_10_0_to_stamp_9_by_default_storage_upgrade_preserves_history`
lets the 0.11.0 `upgrade` run to its default target (stamp 9, the three
handlers through the system-column respelling). That route needs a graph
with only main, so 0.10.0 merges and deletes `review` before 0.11.0's default
conversion, and 0.11.0 forks `temp` after it. The journey asserts the reads at
every commit main lists right after the predecessor's conversion, those
written before the respelling among them. Main builds 10 to 13 also
print `0.11.0`, so these journeys prove their source by the stamp
`snapshot --json` reports, not by `--version`.

Each journey resolver reads its variable, else the binary under
`target/storage-upgrade-binaries/` (`lance-11/`, `stamp-13/`, `v0.11.0/`, `v0.10.0/`).
Without a binary the journey prints a skip line locally and panics when
`OMNIGRAPH_REQUIRE_STORAGE_UPGRADE_TESTS=1`, as CI sets. The v6 format fence
reads `OMNIGRAPH_V6_BIN` alone and skips when it is unset, whatever that
variable says. To run the
crossversion scope locally, build the pinned Lance 11 and stamp-13 predecessors
as for the v5 fence above, install the two releases with `scripts/install.sh` (`VERSION=v0.11.0`
and `VERSION=v0.10.0`, each with its `INSTALL_DIR`), place the four binaries
in those directories and run the job's sequence (bash: the cases live in the
CLI's `integration` binary, where a bare `crossversion_upgrade::` filter would
add the module's other journeys, so the exact names come from the compiled
listing, discovery must succeed and the list must be nonempty before Cargo runs
them, and the log must equal the generated file):

```bash
set -euo pipefail
features=omnigraph-engine/failpoints,omnigraph-cluster/failpoints
scratch=$(mktemp -d)
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked --test integration --features "$features" crossversion_upgrade:: -- --list 2>&1 | tee "$scratch/list.log"
python3 scripts/check-storage-upgrade-ci.py --list-cases crossversion --compiled "$scratch/list.log" > "$scratch/cases.txt"
cases=()
while IFS= read -r name; do
  cases+=("$name")
done < "$scratch/cases.txt"
[ "${#cases[@]}" -gt 0 ]
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked --test integration --features "$features" -- --exact --test-threads=1 "${cases[@]}" 2>&1 | tee "$scratch/run.log"
python3 scripts/check-storage-upgrade-ci.py --check-log crossversion "$scratch/run.log" --cases "$scratch/cases.txt"
```

`db/upgrade/tests.rs` owns the `upgrade_storage` protocol over
`legacy::write::LegacyHistory`, the test-only writer of
`omnigraph-catalog` that replays scripted publishes: `create` in the stamp-13
overwrite order, `create_stamped` in the flat shape of stamps 8 and 9, for
which the test module also writes the three root schema objects. It covers
the reports (`already_current` with no write on a fresh graph,
`unsupported_source`, `unsupported_target`, the pending-marker reports,
`--check` leaving the store untouched), conversion and equivalence of main,
named refs and fresh forks, the pre-fence refusals and bounds, every seam
interrupted and rerun, and the post-upgrade reads: commit list and change feed
across the upgrade, numeric snapshots below it, merges on a legacy base,
retired refs, leftover merge-input tags and `cleanup`. The census, plan,
locator codecs and legacy read arms are owned by the `legacy_` tests of
`omnigraph-catalog` (`tests.rs`, `history.rs`). A fixture cannot drift from
the predecessor unnoticed only because the genuine journey is required; change
both together. Keep ordinary-open refusal for all pre-v14 stamps.
`schema_apply.rs`, `system_column_upgrade.rs` and historical-read owners cover
atomic contract publication, first-touch retry and historical reads under
the schema accepted at their commit (`point_in_time.rs`, beside the GQT cases
that apply a schema between commits). The catalog tests own row uniqueness, projection and validation;
`lance_surface_guards.rs` owns the filtered packed-record scan. See the
[support matrix](versioning.md#storage-upgrade-support-matrix).

### Manual 0.12 cluster upgrade qualification

`genuine_v0_12_0_cluster_ledger_upgrade_preserves_live_deployment` is an ignored,
Unix-only release qualification test. It creates genuine 0.12 receipts, stops
the old server, converts the ledger, checks data/history/schema identity and
historical reads, then applies live schema and policy changes. Storage is
served at format 14 until the candidate's first publish stamps a branch 16.
Ordinary CI keeps current-version live-deployment coverage; it does
not download 0.12 or run this journey.

Run explicitly from the repository root when qualifying that upgrade path,
using a native build with Cargo's default `target/` directory.
Both predecessor variables are required; missing binaries fail. The installer
verifies the official archive checksum. Copy the freshly built candidate server
so another build cannot replace it during qualification:

```bash
set -euo pipefail
qualification_dir=$(mktemp -d)
REPO_SLUG=ModernRelay/omnigraph VERSION=v0.12.0 INSTALL_DIR="$qualification_dir/v012" bash scripts/install.sh
qualification_features=omnigraph-engine/failpoints,omnigraph-cluster/failpoints
cargo build --locked -p omnigraph-cli -p omnigraph-server -p omnigraph-engine -p omnigraph-cluster --features "$qualification_features"
cp target/debug/omnigraph-server "$qualification_dir/omnigraph-server"
env RUST_MIN_STACK=16777216 \
  OMNIGRAPH_V012_BIN="$qualification_dir/v012/omnigraph" \
  OMNIGRAPH_V012_SERVER_BIN="$qualification_dir/v012/omnigraph-server" \
  "CARGO_BIN_EXE_omnigraph-server=$qualification_dir/omnigraph-server" \
  cargo test --locked -p omnigraph-cli -p omnigraph-server -p omnigraph-engine -p omnigraph-cluster \
    --features "$qualification_features" --test integration \
    crossversion_upgrade::genuine_v0_12_0_cluster_ledger_upgrade_preserves_live_deployment \
    -- --exact --ignored --test-threads=1 --nocapture
```

Qualification requires `1 passed; 0 failed; 0 ignored`; an empty or ignored run
is not evidence. Keep its output with the release qualification record.

## Commands

The `cargo dev-*` aliases build every loop below on the canonical graph's one
feature set, and new worktrees can start from a cloned warm `target/`; see
[local builds](local-builds.md).

Focused iteration:

```bash
cargo test -p omnigraph-engine --test integration traversal_indexed::
cargo test -p omnigraph-engine --test writes concurrent
cargo test -p omnigraph-server --test integration data_routes::
cargo test -p omnigraph-cli --test integration cli_data::
cargo test -p omnigraph-cluster --test failpoints --features failpoints
cargo test -p omnigraph-bench --locked
```

GQT commands run from any directory inside the checkout; the workspace Cargo
configuration enables the DST runtime that corpus files request:

```bash
cargo test -p omnigraph-gqt --locked                            # complete corpus and harness tests
cargo test -p omnigraph-gqt --test gq_logic_tests issue_563      # matching case names
cargo test -p omnigraph-gqt --test gq_logic_tests -- --list      # one line per case
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- cases/dst_restart_preserves_rows.gqt --measure   # store requests per step under DST
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- cases/concurrent_read_beside_publish.gqt --measure   # a `--- concurrent scripted` block: sessions overlap under its `order:` line, one cost row per session
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- cases/dst_restart_preserves_rows.gqt --measure --baseline /tmp/gqt-cost.tsv --write-baseline   # record a cost baseline anywhere on disk; --baseline alone prints the delta
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- --store file:///path/to/graph /path/to/queries.gqt
cargo run -p omnigraph-gqt --bin omnigraph-gqt -- --check cases/   # every format error of every case, each with its code and fix; runs nothing
```

Schema and seed are optional together; a file without them needs `--store`
and belongs outside the automatically discovered corpus. Dataset-only files
with schema and seed may have zero steps. `--store` opens the supplied root,
skips fixture preparation, and runs ordinary steps, including writes and
restart, on that root. Its backend must match the declared direct-engine
environment; DST and server targets are refused. External-store reports
cannot replay because the data is not frozen. `--measure` requires at least
one selected DST environment; direct-only selections fail before execution.

Discovery includes every `.gqt` file below `cases/`, recursively. Shared
cases live at its root, plan assertions in `planner/`, catalogue reads in
`catalogue/` and compiler refusals in `diagnostics/`. Directory placement
selects nothing.

Every case is its own libtest test named `case::<relative/path>.gqt`, registered
at run time (`datatest-stable`), so the ordinary name filter selects cases, a
case-only pull request needs no Rust change, and `cargo-nextest` sees each
case (an IDE's test-results view lists cases from the
libtest-shaped output; no per-case gutter runnable exists, since no source
item does). `--test-threads=<n>` bounds how many cases run
concurrently (default: the machine's available parallelism). Each case fails
when it runs past the `timeout_ms` its `--- runner` section declares; the field
is required, and the corpus refuses a value above 10000 before the case runs.
No environment variable changes a budget: a set
`OMNIGRAPH_GQ_CASE_TIMEOUT_SECS` makes every case fail with `invalid_case`. On
a loaded host, run fewer cases at once with a lower `--test-threads`.
`OMNIGRAPH_GQ_BLESS=1` rewrites the failing step's expect rows, or its shape
lines, in place (local workflow only, never CI). Every rows step carries a `--- expect shape`
section, one `<name>: <type>` line per result column in `.pg` property syntax
(`p.age: I32?`), checked against the executed result before the rows, and the
executed schema is also checked against the compiler's inferred schema, so a
wrongly typed column fails even when every cell is null (RFC 0045
§Comparison semantics). Every `ok`/`FAIL` line carries the case's elapsed
time, and a case over budget leaves the corpus: it moves to
`crates/omnigraph-gqt/cases_slow/`, the nightly slow tier
([GQT README](../../crates/omnigraph-gqt/README.md#slow-cases)), when the
format can express it, and a symptom the format cannot express belongs in a
`heavy-repro:` `#[ignore]`d test in a
`crates/omnigraph/tests/integration/repro_issue_*.rs` module. A name filter
that matches no case is libtest's ordinary green zero-test run; read the
`filtered out` count.

GQT plan assertions also pin stored expression types and conversions:
`type $p.age: I64?`, `cast $p.age: I64? -> F64?`, and `no cast $p.age`.
Positive type claims require a match and compare every non-cast node with the
same rendered expression; cast claims compare the child and target types.
Every successful rows step validates the typed explain trees and compares
explain before and after the executed bound plan's serialization round trip.
The root and node-object schema checks remain direct stored-field comparisons.
`block aggregate sum($c.amount): sum(I64?) exact_integer round_to_nearest -> F64?`
pins the block's leaf implementation independently from comparison casts.
Every rows step also requires both `typed_left` and `typed_right`, validates
block signature/spec agreement, and directly compares the stored block tree
and spec after serialization. Planner saved-plan tests mutate CountRows types,
aggregate signatures, specs, comparison casts and bounds without reading rows.

Canonical workspace graph:

```bash
cargo test --workspace --exclude omnigraph-gqt --exclude omnigraph-gqt-served --exclude omnigraph-dst --locked \
  --features omnigraph-engine/failpoints,omnigraph-cluster/failpoints
cargo test -p omnigraph-gqt --locked --lib --test runner_dispatch
```

The feature-superset command compiles the current tree with failpoint hooks present but inert unless a test enables one; it also runs the seams crate's seam guard, the check a cases-only change can turn red (CI runs it again in `GQT (ordinary)`, where `Test Workspace` is skipped). The separate `GQ Logic Tests` context owns GQT: the `runner_dispatch` command above covers dispatch (CI also runs it with `RUSTFLAGS` cleared to prove unavailable-DST refusal), and the complete corpus command runs both execution targets. Neither command substitutes for the other. Also run formatting and both workspace Clippy graphs plus configured GQT Clippy; [ci.md](ci.md) lists the exact gates.

AWS server support has a separate feature owner:

```bash
cargo test -p omnigraph-server --features aws
```

S3-backed tests skip unless `OMNIGRAPH_S3_TEST_BUCKET` and the corresponding AWS endpoint/credential variables are set. Azure-backed tests skip unless `OMNIGRAPH_AZURE_TEST_CONTAINER` and the documented Azure/Azurite variables are set. A configured CI backend treats a skip as failure.

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

### Cross-edition comparison

GQ 3 replaced the query language in one break, and the corpus was respelled
with it. `scripts/cross-edition.py` makes "the respelled corpus answers as
edition 2 did" a check: for every `.gqt` case present at the pinned edition-2
commit (`EDITION_2_SOURCE_COMMIT` in `ci.yml`, the merge base with `main`) and
in this tree, under the same path or one git's rename detection pairs, it runs
the commit's text of the case with that commit's `omnigraph-gqt` and this
tree's text with this tree's runner. Both texts get one rewrite, the same on
both sides: the `--- runner` section becomes a single deterministic-simulation
environment (in-memory store, seed 0), because only that environment's trace
(`--trace`) records each step's result: a read's Arrow schema and rows, a
write's affected counts, an error's text. Steps pair by kind and declared
query name in sequence order, so an inserted or removed step does not shift
the rest. For each pair the script compares column names, Arrow types, rows (a
multiset unless both steps' rows are ordered), affected counts, and the error:
whether the step failed and its diagnostic code. A difference passes only when
`crates/omnigraph-gqt/cross-edition.tsv` lists it by case, current step
ordinal (`e2:<n>` for an edition-2 step with no counterpart) and aspect,
naming the bullet of the GQ 3 RFC's Compatibility and reversibility section
that sanctions it (`BULLETS` in the script, each with the sentence it cites)
and why; a row that excuses no difference fails too. A row citing an `open:`
key (`OPEN` in the script) is a difference no Compatibility bullet names: it
does not fail the job, which prints it as a warning, and the key leaves the
list when the RFC decides it. A case both editions fail at the same step with
the same message under the simulation is a warning, its later steps
uncompared; any other run failure is an error.

```bash
git worktree add --detach /tmp/e2 <EDITION_2_SOURCE_COMMIT>
(cd /tmp/e2 && cargo build --locked -p omnigraph-gqt --bin omnigraph-gqt --target-dir /tmp/e2-target)
cargo build --locked -p omnigraph-gqt --bin omnigraph-gqt
python3 scripts/cross-edition.py run --edition-2-runner /tmp/e2-target/debug/omnigraph-gqt \
  --current-runner target/debug/omnigraph-gqt --base-ref <EDITION_2_SOURCE_COMMIT> --work /tmp/cross-edition
```

With `--measure`, each side also runs under `--measure`, and an aligned
step whose store requests (seed 0, deterministic under the simulation) rose
is a difference of aspect `requests`, which a row passes by naming the
reason (`COSTS` in the script, or an `open:` key); CI runs it as its own
pass, since the measure ledger moves a concurrent block's park points. A
case whose two simulation runs of seed 0 measure different requests
(`replay_mismatch`, seen on 4-CPU runners for a CSV load and a concurrent
block) is a warning, and its requests are not compared. The report lists
every changed step's requests and class deltas under `costs`.
`--case <path>` (repeatable) selects cases; `--reuse` compares the traces an
earlier run left under `--work`.

`fix_edition_2::printed_fixes_respell_the_edition_2_corpus_into_its_hand_rewrite`
in `omnigraph-gqt`'s `integration` binary is the gate for `omnigraph lint
--fix`: it gives every query step edition 2 ran in the commit's tree
(`OMNIGRAPH_EDITION_2_TREE`; the GQT and benchmark corpora and the `.gq`
files beside a `.pg`) to the fix loop with review fixes applied, requires
every one whose refusals print fixes to converge, and compares each
declaration's typed IR with the hand rewrite of the same case and name in
this tree. A difference passes as a row of
`crates/omnigraph-gqt/lint-fix-edition-2.tsv` naming a reason (`REASONS` in
the test; an `open:` reason prints a warning), and a row that matches nothing
fails; a keyed `insert` the fix leaves, where the hand rewrite wrote the
respelling map's `replace`, is recognized by applying that row. Without the
tree it skips; `OMNIGRAPH_REQUIRE_EDITION_2=1` fails instead.

Fix coverage over edition-2 text is an instrument beside it,
`fix_coverage::fix_coverage_over_edition_2_text` in `omnigraph-gqt`'s
`integration` binary (`#[ignore = "instrument: …"]`): point
`OMNIGRAPH_FIX_COVERAGE_DIR` at the edition-2 commit's `cases/` or at stored
queries exported as `<name>.gq` beside `<name>.pg`, and it applies every
printed suggestion pass by pass and prints counts only (compiled as written,
respelled by the fixes per pass, stuck by code and message shape), so it may
run over a private registry; `OMNIGRAPH_FIX_COVERAGE_SHOW=1` also prints the
stuck text, for a corpus whose text may be shown. The report (`report.json` under `--work`)
holds every difference with both observations.

### OpenAPI

`crates/omnigraph-server/tests/integration/openapi.rs` regenerates the specification in memory and compares it with `openapi.json`. For an intentional API change:

```bash
OMNIGRAPH_UPDATE_OPENAPI=1 \
  cargo test -p omnigraph-server --test integration openapi::openapi_spec_is_up_to_date
```

Commit the generated file with the API change. CI checks drift; it never updates the file.

## Cost tests and benchmarks

The active performance workload definitions live in GQT under
[`benchmarks/`](../../benchmarks/README.md). Unsupported maintenance, real
concurrency, historical HTTP and streaming instruments are preserved in
[`benchmarks/deferred/`](../../benchmarks/deferred/README.md), outside Cargo
discovery. Cleanup and optimization belong in GQ before GQT can exercise them.
Historical HTTP records remain readable by `scripts/analyze-http-perf.py` and
`scripts/analyze-http-retention.py`; those instruments remain deferred.
[Read-only served acquisition](../../crates/omnigraph-bench/README.md#served-read-only-acquisition)
is available for GQT query and traversal workloads.

Correctness tests may assert deterministic logical or object-store operation counts when the count is part of the design contract. Wall time and peak RSS depend on the host and belong in the `omnigraph-bench` scenario harness; benchmark results are evidence rather than pass/fail assertions. Declarative benchmark cases and suites live under `benchmarks/`; deterministic engine cost contracts remain in `crates/omnigraph/tests/`.

The checkpoint and durable-head lookup instruments keep logical rows, ranges,
fragments, and index pages exact while bounding local compacted physical I/O
separately. Lance 13 batches page and column reads before nearby byte ranges
coalesce; equal logical work can read fewer bytes through more physical ranges.
Their local budgets cover the measured 20/80-publish samples. They do not qualify
larger histories or the S3 path, and do not activate either proposed format.
Re-audit those budgets on a substrate change instead of widening every counter
or requiring an older byte-growth pattern to persist.

### DST storage-cost golden

`crates/omnigraph-dst/cost_table.txt` records the exact object-store calls of
each labeled operation on deterministic memory storage: `l.` verbs reach Lance
(`__manifest`, `__history` and the table datasets), `a.` verbs the control
objects behind `StorageAdapter`. `dst_bench_cost_count_golden` in
`crates/omnigraph-dst/tests/scenarios.rs` builds the table from a seeded run of
random operations and from labeled scenarios (`labeled_cost_rows`), refuses a
build that does not replay identically, and compares it with the file, so a
cost change fails as a named line. The `DST pinned suite` job of `dst.yml` runs
it on every pull request that changes engine input, as a reporting context.

Add or update a row when:

- a change adds an operation whose storage cost is part of its contract, or
  changes what an existing one reads or writes: a route, a page or
  continuation, a write path, a maintenance step. Add a labeled scenario that
  measures that one operation on a reopened handle, with setup and
  verification under `_`-prefixed labels, as `merge_history_cost_rows` does;
- the contract is that a cost stays flat in something (history depth, live
  branches, rows): record the operation at two sizes of it under two labels,
  as `MergeTargetHistory2` and `MergeTargetHistory32` do. Equal rows show the
  claim in the golden itself.

A changed line is a cost change. Regenerate only on purpose, and say in the
pull request why each changed line moved; a line that moved for no stated
reason is a regression to find, not a golden to refresh.

The golden pins exact counts on one backend. It does not replace the curve
tests in `crates/omnigraph/tests/` (`write_cost.rs`, `warm_read_cost.rs`,
`changes_cost.rs` and the other cost owners above), which assert a shape (flat
or growing) through the `IoCounts` harness; a storage-cost contract has both.
Query operator cost belongs to GQT (`--measure`), and wall time and memory to
the benchmark harness.

Regenerate, then verify, from `crates/omnigraph-dst`, whose Cargo
configuration the suite needs:

```bash
DST_REGEN_COSTS=1 cargo test --locked --test scenarios dst_bench_cost_count_golden
cargo test --locked --test scenarios dst_bench_cost_count_golden
```

### Full-text fingerprint golden

`crates/omnigraph-core/fts/measured_fingerprints.golden` records, for every
analyzer profile, the measured fingerprints of its native configuration and of
its profile analyzer: digests of the tokens each emits over the probe corpus
`crates/omnigraph-core/fts/probe_corpus.txt`
([Lance compatibility fences](lance-fences.md#full-text-compatibility)).
`fts_analysis::tests::measured_fingerprints_are_the_golden` compares them with
what the binary measures, so a dependency bump that changes what a profile
emits fails by name. Such a failure is a profile change, and a profile never
changes once accepted (the GQ 3 RFC, section 4): keep the old behavior (pin or
patch the dependency) or define a new profile. Never refresh the golden alone.

Regenerate only for a new profile or a reviewed change of the probe corpus,
both of which change what every certificate records; say in the pull request
why each changed line moved. The test refuses to regenerate when `CI` is set:

```bash
OMNIGRAPH_REGEN_FTS_FINGERPRINTS=1 cargo dev-nextest \
  -E 'package(omnigraph-core) & test(measured_fingerprints_are_the_golden)'
cargo dev-nextest -E 'package(omnigraph-core) & test(/fts_analysis::/)'
```

The resolved-lockfile guard (`fts_compat::tests::analyzer_dependencies_are_the_audited_resolved_set`)
pins the analyzer's dependency versions; the golden pins its behavior.

### Benchmark harness

The harness's contract is in the
[benchmark harness README](../../crates/omnigraph-bench/README.md) and the
[benchmark catalog](../../benchmarks/README.md). This section records how its
evidence is produced and which tests own it.

For embedded execution, the benchmark runner executes `gqt-v1` dataset/query
pairs through the same
production core used by GQT correctness tests. It selects one engine operation
by ordinal and exact header/body echo, then closes its clock and logical
counters before expectations and subsequent explicit verification. Reads,
mutations, branch controls, single-call generated loads, and engine restart
use the same adapter. Bench wall-clock still requires a qualified release
binary; GQT correctness and DST execution do not acquire benchmark timings.

Every embedded repetition uses a fresh SHA-attested process and restores the dataset
at its stable active path from a never-opened APFS clonefile or verified
Linux/XFS copy. The persistent dataset cache holds its lock through worker
verification and containment. Index requirements are unioned across dataset
and queries and applied with the seed before updates/deletes. This ordering
is exercised by the shipped stale-index pairs.

Served read-only programs use a pre-provisioned graph and fresh client workers.
They do not reset the long-running server. The selected HTTP request and result
materialization are timed; expectations remain outside the interval. Server
identity comes from a declared deployment receipt, while server storage counts
and RSS remain absent. Client build, machine and RSS evidence stay separate.

```bash
RUSTFLAGS= cargo run --release --locked -p omnigraph-bench -- \
  run local-fast --dataset-cache /qualified/cache
```

The empty `RUSTFLAGS` clears the workspace development cfg; the existing
release guard refuses encoded flags and unmodeled runtime overrides.
Children clear their environment, pin locale, and use protocol-owned scratch
siblings for `TMPDIR` and merge staging. Containment must be proved before
cleanup; a later assertion or protocol failure cannot turn Settled timing
into a passing sample.

`gqt_tests.rs` owns public pair/directory discovery, selection, engine receipts,
index preparation, and mixed archive/projection coverage. `gqt_served_tests.rs`
owns served admission, client/server evidence separation, protocol containment
and measured HTTP callback ordering. `dataset_identity`,
`dataset_cache`, and `dataset_worker` own logical history, cache integrity,
and contained building; `gqt_runner`, `gqt_supervisor`, and `gqt_protocol` own
operation boundaries and worker admission. `registered_fixture`, `reset`, and
`environment` retain byte-copy, stable-path, and backend qualification checks.
The retired `real_graph_run` and Rust fixture builder remain test oracles,
with no production execution route.

The slow nightly GQT workflow also explicitly selects the 21 ordinary `benchmarks/fixtures` recipes, including the
full 800,000-row D50 dataset and its 64-commit history variant with post-build assertions. Reduced legacy parity
cases validate generator equivalence but do not stand in for that scale.
Dataset/query catalog parsing is a normal correctness test; selected-operation
wall-clock and unpinned benchmark counters remain report-only. The new D50
warm prefix is 24 authored aggregate reads, a distinct cache program from
the retired Rust scans.

Registered FinBench uses the ordinary `gqt-v1` suite with a logical reference,
schema-less GQT preparation, and `--fixture ID=BUNDLE`. Current registered sources must be main-only,
relocation-self-contained, and have deterministic node keys. Original source
IDs are additionally hashed before preparation; generated transfer IDs use
explicit logical equivalence. Commands and limits are in the
[benchmark catalog](../../benchmarks/README.md#registered-finbench-merge).

Do not archive diagnostic JSON as telemetry. To publish authoritative
`suite run` records, build the benchmark client from clean committed source
and pass `--archive <DIR>`. Its commit, executable digest and build facts
identify the observed client. For embedded execution these also identify the
SUT; served SUT facts come from the declared deployment receipt.
Source revalidation compares raw tracked source bytes without
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
`archive verify`; rebuild the disposable Omnigraph read model with `projection
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
