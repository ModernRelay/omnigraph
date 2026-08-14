# OmniGraph Benchmarks: Current State

**Snapshot date:** 2026-08-14

**Primary design:** [RFC-031 — Comparative Cost Harness](docs/rfcs/0031-comparative-cost-harness.md)

## Executive summary

OmniGraph currently has useful benchmark and cost-testing foundations, but it
does not yet have the comparative harness specified by RFC-031.

Today we can answer three narrower questions:

1. Did a selected operation start performing more logical storage work as
   graph history or data size grew?
2. How much wall time and peak process memory does the current build use for a
   small set of stateful scenarios?
3. For the standard indexed fixture, how do one node insert and one edge insert
   compare in logical Lance object-store calls on local FS and RustFS?

We cannot yet answer the intended release question:

> Did the candidate release regress against the previous release across
> embedded and served cold/warm lifecycles, and what changed in time, logical
> operations, physical object-store attempts, transferred bytes, memory, and
> estimated cost?

RFC-031 remains a draft. Its defining cross-build, public-lifecycle, physical
HTTP accounting, reporting, and release-qualification deliverables have not
landed. The small node-versus-edge v0 described below is a focused regression
gate and evidence format, not the comparative release harness.

## Capability status

| Capability | Current state |
|---|---|
| In-process structural I/O regression tests | Implemented |
| Same-build macrobenchmarks | Implemented for five scenarios |
| Wall-clock measurement | Implemented in the scenario harness |
| Peak process RSS | Implemented with `wait4`; Linux memory caps are available |
| Local-filesystem fixtures | Implemented |
| Bucket-gated S3 cost tests | Implemented; the v0 comparator runs post-merge, while other cells remain on demand |
| Matched node-versus-edge insert logical trip comparison | Implemented with AB/BA fresh fixtures, correctness checks, ceilings, and schema-v1 JSONL |
| Local node-versus-edge CI gate | Implemented and declared required; JSONL and the full test log are retained for 14 days |
| Post-merge RustFS node-versus-edge qualification | Implemented against the pinned RustFS image |
| Manual traversal and HTTP load examples | Implemented |
| Complete cold `Omnigraph::open` structural guard | Missing |
| Previous-release versus candidate comparison | Missing |
| Embedded and spawned-server lifecycle matrix | Missing |
| Physical HTTP attempt/retry accounting | Missing |
| Complete payload-byte accounting | Missing |
| Cross-build result and post-state equivalence | Missing |
| General RFC-031 record envelope and reporter | Missing; v0 has one success-only, single-benchmark record |
| General cross-build thresholds, baselines, and machine-readable waivers | Missing; v0 has fixed per-backend upper ceilings |
| Full benchmark-contract CI and release qualification | Missing |
| Checked-in release evidence bundles | Missing |

## Tooling and machinery in use today

The current system is custom Rust code built into the workspace. There is no
external benchmark framework coordinating the different tools.

### Structural cost tests

The flow is:

```text
cargo test
  -> construct a real OmniGraph/Lance fixture
  -> wrap selected storage operations with in-process counters
  -> perform a read, write, merge, or control operation
  -> assert that counts are fixed, bounded, or grow as expected
```

[`crates/omnigraph/tests/helpers/cost.rs`](crates/omnigraph/tests/helpers/cost.rs)
contains the shared machinery. It records selected data and manifest reads and
writes, latest-version probes, dataset opens, scans, and staged writer
routes. The tests use real OmniGraph and Lance code rather than a fake storage
engine.

[`crates/omnigraph/tests/helpers/bench_v0.rs`](crates/omnigraph/tests/helpers/bench_v0.rs)
owns only the node-versus-edge pairing, correctness oracle, ceilings, identity
stamps, and JSONL envelope. It is imported by the two comparator targets rather
than compiled into every integration test.

These are structural cost measurements. They observe calls at the wrapped
storage boundary; they do not count every physical HTTP attempt, SDK retry, TLS
byte, or object-store billable action. The v0 comparator additionally records
four fresh-fixture AB/BA samples, post-operation correctness, fixture/build
identity, ceilings, and the metric boundary as one JSONL success record.

Current suites include:

| Test | Purpose |
|---|---|
| [`warm_read_cost.rs`](crates/omnigraph/tests/warm_read_cost.rs) | Warm/cold read resolution, schema/cache, and traversal-build costs |
| [`write_cost.rs`](crates/omnigraph/tests/write_cost.rs) | Write, index, optimize, opener, scan, history-growth, and matched node-versus-edge insert costs |
| [`write_cost_s3.rs`](crates/omnigraph/tests/write_cost_s3.rs) | Bucket-gated object-store terms that local filesystem tests cannot expose |
| [`merge_cost.rs`](crates/omnigraph/tests/merge_cost.rs) | Merge route costs and the known manifest-history amplification term |
| [`branch_control_cost.rs`](crates/omnigraph/tests/branch_control_cost.rs) | Branch-control storage costs |
| [`durable_head_lookup_cost.rs`](crates/omnigraph/tests/durable_head_lookup_cost.rs) | RFC-024 decision instrument |
| [`checkpoint_retention_cost.rs`](crates/omnigraph/tests/checkpoint_retention_cost.rs) | RFC-025 decision instrument |

The RFC-024 and RFC-025 tests preserve specific rejected-design conclusions;
they are not general release benchmarks.

### Scenario benchmark harness

[`crates/omnigraph/benches/scenarios.rs`](crates/omnigraph/benches/scenarios.rs)
is a custom `harness = false` Cargo benchmark target.

Its flow is:

```text
cargo bench
  -> parent process starts a fresh copy of the benchmark executable
  -> child creates or opens a real fixture and performs one workload
  -> parent reaps the child with wait4
  -> wall time, peak RSS, and scenario metrics are emitted as JSONL
```

The parent/child boundary isolates crashes and out-of-memory outcomes. On Linux,
`RLIMIT_AS` can enforce and verify a requested address-space cap. The RFC-023
adoption scenarios split setup, measured operation, and verification into
separate processes so setup memory does not contaminate the measured operation.

Current scenarios are:

- `merge-all-changed`
- `nearest-prefilter`
- `fenced-small-upsert`
- `fenced-adopt-all-new`
- `general-merge-updates`

Every run can append a record to `--out <path>`,
`OMNIGRAPH_BENCH_RESULTS`, or the default
`crates/omnigraph/benches/results.jsonl`. The default file is intentionally
gitignored and host-specific.

This harness measures only the current benchmark binary. Its `--baseline`
option selects a scenario-local comparator; it does not select an older
OmniGraph release.

Criterion is deliberately not used. These are cold, stateful, multi-second
workloads where process isolation, peak memory, and visible failures matter
more than many warm in-process iterations.

### Manual examples

- [`bench_expand.rs`](crates/omnigraph/examples/bench_expand.rs) measures graph
  traversal behavior.
- [`bench_concurrent_http.rs`](crates/omnigraph-server/examples/bench_concurrent_http.rs)
  measures concurrent `/change` traffic.
- [`bench_actor_isolation.rs`](crates/omnigraph-server/examples/bench_actor_isolation.rs)
  measures heavy-ingest versus light-change actor isolation.

The HTTP examples drive an in-process Axum/Tower application. They exercise the
request path but do not launch a packaged server and measure a real network
lifecycle.

### Backends and system facilities

The current tools use:

- Cargo's built-in test and benchmark target support.
- Real OmniGraph, Lance, Arrow, object-store, and server code.
- Local temporary directories for default fixtures.
- S3 or an S3-compatible endpoint when `OMNIGRAPH_S3_TEST_BUCKET` and the usual
  AWS variables are configured.
- Rust task-local counters and storage wrappers for structural I/O accounting.
- Unix `wait4`/`ru_maxrss` for child-process peak memory.
- Linux `RLIMIT_AS` for optional memory-cap enforcement.
- JSON Lines for scenario and v0 comparator output.

## What can be run now

### Structural cost suite

```bash
cargo test -p omnigraph-engine --locked \
  --test warm_read_cost \
  --test write_cost \
  --test merge_cost \
  --test branch_control_cost
```

This is the best current command for checking whether known structural costs
remain bounded. It does not produce a stable-versus-edge performance report.

### Node-versus-edge insert trip comparison

```bash
cargo test -p omnigraph-engine --locked --test write_cost \
  node_vs_edge_insert_lance_object_store_trips -- --exact --nocapture
```

The test runs fresh-fixture node→edge and edge→node pairs and refuses
order-dependent counts. Its 2026-08-14 local observation was 14 logical Lance
object-store calls for the node insert and 34 for the edge insert. The edge arm
opened three data tables and paid endpoint-validation reads; the node arm opened
one. These counts include data-table and `__manifest` logical reads/writes, but
exclude non-Lance control objects, SDK retries, and exact physical HTTP attempts.

Set `OMNIGRAPH_COST_BENCH_RESULTS=/path/to/results.jsonl` to retain the
schema-v1 record; an explicitly configured path that cannot be written is a
test failure. The PR job runs this exact cell and uploads its JSONL plus
complete Cargo log. Upper ceilings allow genuine reductions, while
non-zero meter sentinels and exact AB/BA repeatability prevent a disconnected
counter from passing vacuously.

### Scenario protocol contract

```bash
cargo test -p omnigraph-engine --locked \
  --test benchmark_scenario_contract
```

This verifies scenario caps, process-phase boundaries, child protocol, and
source-shape contracts. It does not run the workloads or assess performance.

### One macrobenchmark scenario

```bash
cargo bench -p omnigraph-engine --bench scenarios -- \
  --scenario merge-all-changed \
  --rows 20000 \
  --dims 256 \
  --runs 1
```

Replace the scenario name with another supported scenario as needed. These
runs can take seconds or minutes and produce host-specific observations.

### Bucket-gated S3 costs

```bash
OMNIGRAPH_S3_TEST_BUCKET=... \
AWS_REGION=... \
cargo test -p omnigraph-engine --locked --test write_cost_s3
```

For an S3-compatible service, configure the documented credentials and
`AWS_ENDPOINT_URL_S3` as well. The tests skip when the required bucket is not
configured.

The focused RustFS qualification cell is:

```bash
cargo test -p omnigraph-engine --locked --test write_cost_s3 \
  node_vs_edge_insert_lance_object_store_trips_on_s3 -- --exact --nocapture
```

With the S3 environment configured, it creates four indexed fixtures below one
unique prefix, measures AB/BA, emits one JSONL record, and removes that exact
prefix even after a panic. Its pinned RustFS result is 37 logical calls for the
node insert and 57 for the edge insert. These are logical wrapper calls; they do
not prove real-AWS HTTP-attempt counts.

### Manual traversal benchmark

```bash
cargo run --release -p omnigraph-engine --example bench_expand
```

### Manual in-process HTTP benchmarks

```bash
cargo run --release -p omnigraph-server --example bench_concurrent_http -- \
  --tables 16 --actors 16 --ops-per-actor 1000 --mode disjoint

cargo run --release -p omnigraph-server --example bench_actor_isolation -- \
  --light-actors 4 --light-ops-per-actor 50 \
  --heavy-batches 200 --heavy-rows-per-batch 200 --inflight-cap 1
```

## CI and artifact state

- The scenario benchmark is deliberately not part of `cargo test --workspace`
  and is not a performance CI gate.
- `benchmark_scenario_contract` is an ordinary integration test, not a workload
  run.
- The local cost tests are ordinary integration tests. The focused v0 cell has
  a `Node vs Edge Logical Cost` PR job, and the declarative branch-protection
  policy requires that context.
- The default post-merge/tag/manual RustFS shard runs the matching
  S3-compatible v0 cell and rejects its unconfigured skip. Other S3 cost suites
  remain on demand.
- V0 JSONL/log evidence is retained as a short-lived CI artifact. There are no
  tracked RFC-031 reports, manifests, price tables, baselines, waivers, or
  release evidence bundles.
- There is no protected benchmark environment, pre-tag qualification workflow,
  or release artifact promotion step.

The repository therefore has regression instruments but no durable benchmark
history that can serve as a release baseline.

## RFC-031 gaps

### 1. Required first landing: cold-open guard

RFC-031 requires a focused `crates/omnigraph/tests/cold_open_cost.rs` before the
larger harness. It is absent.

The missing guard must measure the complete graph-open path, include local and
RustFS/object-store cells, pin the intended structural terms, and demonstrate
that a seeded historical regression fails while the current implementation
passes. Existing warm-read and isolated dataset-open tests do not cover the
same public cold lifecycle.

### 2. Comparative harness crate

There is no `crates/omnigraph-bench` workspace crate and no driver that can:

- select exact stable and edge CLI/server binaries;
- construct equivalent fresh roots for both builds;
- run alternating AB/BA samples;
- exercise embedded cold, server startup, served first-request, and served
  warm-request coordinates;
- normalize public outputs across versions;
- compare pre-state, result, and post-state before admitting a sample; or
- emit typed failure records without silently replacing failed samples.

### 3. Logical and physical accounting

There is no non-default `bench-metrics` build feature, process-global Lance
metrics recorder, adapter-internal counters, or bounded server control channel.
Lance is not currently built with its optional `metrics` feature.

There is also no counting HTTP proxy with streaming bodies, drain-safe reset,
request/outcome classification, TLS/SigV4 support, no-bypass proof, or qualified
physical action and byte reconciliation.

### 4. Records and reporting

V0 emits one success-only schema-v1 record for its single current-build
comparison, including build/fixture identity and invocation-local AB/BA sample
keys. It is not the RFC record model. The following general artifacts do not
exist:

- canonical deterministic IDs and deduplication/conflict rules;
- full run/sample/coordinate/pair identities and cross-build equivalence;
- manifest, dependency, cache, thread, and retry stamps;
- typed failure records for refusals, crashes, timeouts, and mismatches;
- direct/proxied matched-run aggregation;
- signed deltas, ratios, medians, minima, and maxima;
- structural, timing, action, byte, and RSS threshold evaluation;
- payload-action density and estimated currency cost;
- machine-readable, owned, expiring waivers; and
- Markdown and machine-readable reports.

### 5. Executable manifests and evidence

Neither `v1-release-smoke.yaml` nor `v1-historical-sensitivity.yaml` exists.
There is no deterministic fixture generator, checked-in grid, input digesting,
time/cost admission model, pilot qualification, seeded ablation set, or
acceptance evidence.

Consequently, none of RFC-031's mandatory R1-R9/C1-C2 coordinates or proxy,
equivalence, determinism, and historical-sensitivity gates can currently run.

### 6. Release integration

There is a dedicated fast v0 logical-cost job and a declared branch-protection
context.
There is no full-harness contract job, protected `benchmark-s3` environment,
pre-tag candidate qualification, exact qualified archive promotion, or
evidence-bundle release attachment.

## RFC issues to settle before implementation

The design direction is sound, but several details should be made executable:

1. **Add formal implementation milestones.** The RFC specifies a first landing,
   a large V1 harness, qualification evidence, and release integration without
   defining explicit phase completion states.
2. **Repin the compared artifacts.** The motivating v0.8.1 versus RFC-026-era
   edge comparison is historical. V1 should name exact current stable and edge
   artifact sources and say when Lance/object-store changes invalidate proxy
   qualification.
3. **Define metrics epochs and byte availability.** Lance's recorder is
   process-global and cumulative. The control protocol should use a custom
   recorder with monotonic snapshot deltas or define equivalent epoch
   semantics. [Lance observability](https://lance.org/guide/observability/)
   documents request bytes for only supported operations, so unavailable bytes
   must remain explicitly absent rather than inferred.
4. **Specify physical-byte semantics.** The proxy contract must distinguish
   payload versus wire bytes, compressed/chunked bodies, partial or failed
   bodies, multipart operations, copies, and S3 operations encoded as HTTP
   queries.
5. **Pin lifecycle boundaries.** Warm priming must be explicit, particularly
   for mutating workloads, and RSS measurement must define readiness,
   measurement, shutdown, and process-reaping boundaries.
6. **Key timing baselines by execution environment.** Runner class/image,
   CPU, RustFS image/configuration, AWS region placement, thread/cache settings,
   retries, pagination, and multipart sizes can materially change results.
7. **Make governance quantitative.** Define threshold values, baseline
   promotion, first-release behavior, noise treatment, waiver approval/expiry,
   and reproducible ablation commits or digests.

## Recommended implementation order

The RFC does not formally number phases; this is the practical dependency
order inferred from it:

1. **Cold-open structural guard:** land `cold_open_cost.rs`, local and RustFS
   coverage, and its focused CI wiring.
2. **Core contracts:** add `omnigraph-bench`, shared process/RSS/identity/JSONL
   primitives, versioned records, public-output adapters, fixtures, and fast
   contract tests.
3. **Measurement machinery:** add the direct/proxied driver, counting proxy,
   `bench-metrics` recorder and IPC, reporter, thresholds, and waivers.
4. **Qualification:** land smoke and historical manifests, budget admission,
   deterministic ablations, and all correctness/evidence gates.
5. **Release handoff:** qualify the exact candidate archive before tagging,
   promote it without rebuilding, and attach the complete evidence bundle.

Shared history synchronization or a benchmark-results database is explicitly
deferred by RFC-031 and is not a current implementation gap.

## Verification performed for this snapshot

The focused warm-read, write, merge, and branch-control suites completed with
32 passing tests. `benchmark_scenario_contract` completed with 7 passing tests.
The local comparator repeated exactly across both AB/BA pairs in two runs:
node 14 (12 reads, 2 writes), edge 34 (32 reads, 2 writes).
The exact node-versus-edge RustFS cell passed in two matching current-main runs
against the pinned RustFS 1.0.0-beta.12 image: node 37 (30 reads, 7 writes),
edge 57 (50 reads, 7 writes). Both runs left no keys below their dedicated test
prefixes.
