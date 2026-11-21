//! The measured free-running block: a workload whose measured step is a
//! `--- concurrent measured` block, planned once per swept value and run
//! through the embedded repetition for its declared duration.
use super::*;
use serde_json::Value;

const FIXTURE: &str = r#"# issue: none
# notes: One keyed table with sixteen generated rows on main.

--- runner
timeout_ms: 60000
environments:
  - target: omnigraph-engine
    storage: local-filesystem

--- schema
node Chunk { slug: String @key n: I64 }

--- seed generate: v1 seed: 7
tables:
  - kind: node
    name: Chunk
    rows: 16
    commits: 1
    batch_rows: 16
    columns:
      slug: {kind: key, prefix: seed-, width: 2}
      n: {kind: ordinal, start: 0, step: 1}
"#;

const WORKLOAD: &str = r#"# issue: none
# notes: W writers append dealt rows beside two readers for two measured seconds.

--- runner
timeout_ms: 60000
environments:
  - target: omnigraph-engine
    storage: local-filesystem

--- bench
warmup_seconds: 0
duration_seconds: 2
sweep: { W: [1, 3] }

--- concurrent measured
writer x${W}: load generate: v1 seed: 42 mode: insert
  tables:
    - kind: node
      name: Chunk
      rows: 40960
      commits: 10
      columns:
        slug: {kind: key, prefix: cw-, width: 5}
        n: {kind: ordinal, start: 1000, step: 1}
reader x2: query seeded_row() { match { $c: Chunk } where $c.slug = "seed-07" return { $c.n } }
--- expect
writer: ok
reader: ok

--- query branch: main
query seeded_row() { match { $c: Chunk } where $c.slug = "seed-07" return { $c.slug, $c.n } }
--- expect unordered
{"c.slug":"seed-07","c.n":7}
--- expect shape
c.slug: String
c.n: I64
"#;

const CONFIG: &str = "version: 2\ndefaults:\n  repetitions: 1\nscenarios:\n- id: writers\n  fixture: fixtures/chunks.gqt\n  workload: workloads/writers.gqt\n";

fn short_catalog(directory: &Path) -> crate::catalog::Catalog {
    catalog_of(directory, WORKLOAD)
}

/// The catalog of one scenario, `writers`, over the sixteen-row fixture and
/// `workload`.
fn catalog_of(directory: &Path, workload: &str) -> crate::catalog::Catalog {
    catalog_over(directory, FIXTURE, workload)
}

fn catalog_over(directory: &Path, fixture: &str, workload: &str) -> crate::catalog::Catalog {
    for child in ["fixtures", "workloads"] {
        std::fs::create_dir_all(directory.join(child)).unwrap();
    }
    std::fs::write(directory.join("fixtures/chunks.gqt"), fixture).unwrap();
    std::fs::write(directory.join("workloads/writers.gqt"), workload).unwrap();
    std::fs::write(directory.join("benchmarks.yaml"), CONFIG).unwrap();
    crate::catalog::Catalog::load(&directory.join("benchmarks.yaml")).unwrap()
}

const RUNNER: &str = r#"# issue: none
# notes: A measured block on one route of the bench host that fails or runs beside another block.

--- runner
timeout_ms: 60000
environments:
  - target: omnigraph-engine
    storage: local-filesystem

"#;

const SEEDED_ROW: &str = r#"
--- query branch: main
query seeded_row() { match { $c: Chunk } where $c.slug = "seed-07" return { $c.slug, $c.n } }
--- expect unordered
{"c.slug":"seed-07","c.n":7}
--- expect shape
c.slug: String
c.n: I64
"#;

/// A workload measuring `block` for `seconds` with no warmup, followed by
/// the steps of `after`.
fn block_workload(seconds: u32, block: &str, after: &str) -> String {
    format!("{RUNNER}--- bench\nwarmup_seconds: 0\nduration_seconds: {seconds}\n\n{block}{after}")
}

/// The first point of the `writers` scenario over `workload`.
fn first_point(workload: &str) -> PlannedGqt {
    let directory = tempfile::tempdir().unwrap();
    catalog_of(directory.path(), workload)
        .plan_in("writers", &recorded_environment())
        .unwrap()
}

fn number(value: &Value, path: &str) -> u64 {
    path.split('.')
        .fold(value, |value, key| &value[key])
        .as_u64()
        .unwrap_or_else(|| panic!("`{path}` is not a number in {value}"))
}

/// The statements the labels of `block` were acknowledged for in its warmup.
fn warmup_acked(block: &Value) -> u64 {
    let labels = block["labels"].as_array().unwrap();
    labels
        .iter()
        .map(|label| number(label, "warmup_acked"))
        .sum()
}

/// What every label of a measured block reports, whatever its statement.
fn assert_label(label: &Value, name: &str, kind: &str, workers: u64, window_us: u64) -> u64 {
    assert_eq!(label["label"], name);
    assert_eq!(label["kind"], kind, "{name}");
    assert_eq!(number(label, "workers"), workers, "{name}");
    let acked = number(label, "acked");
    assert!(acked > 0, "{name}: no statement was acknowledged");
    let (least, most) = (
        number(label, "per_worker.min"),
        number(label, "per_worker.max"),
    );
    assert!(
        least <= most && least * workers <= acked && acked <= most * workers,
        "{name}: per-worker counts {least}..={most} do not bound {acked} over {workers} workers"
    );
    let (p50, p99, longest) = (
        number(label, "service_time_us.p50"),
        number(label, "service_time_us.p99"),
        number(label, "service_time_us.max"),
    );
    assert!(0 < p50 && p50 <= p99 && p99 <= longest, "{name}");
    let by_second: Vec<u64> = label["acked_by_second"]
        .as_array()
        .unwrap_or_else(|| panic!("{name}: no per-second series"))
        .iter()
        .map(|count| count.as_u64().unwrap())
        .collect();
    assert_eq!(by_second.len(), 2, "{name}: one count per measured second");
    assert_eq!(
        by_second.iter().sum::<u64>() + number(label, "acked_in_drain"),
        acked,
        "{name}: every measured acknowledgement lands in a second or in the drain"
    );
    assert_eq!(
        u128::from(number(label, "acked_per_second_millis")),
        u128::from(acked) * 1_000_000_000 / u128::from(window_us),
        "{name}"
    );
    label["authority_conflicts"]
        .as_u64()
        .unwrap_or_else(|| panic!("{name}: conflicts are a count"));
    acked + number(label, "warmup_acked")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn measured_block_loops_every_worker_for_its_duration_and_reports_each_label() {
    let directory = tempfile::tempdir().unwrap();
    let suite = short_catalog(directory.path())
        .resolve_in(Some("writers"), None, &recorded_environment())
        .unwrap();
    assert_eq!(suite.runs.len(), 2, "one run per swept worker count");
    let mut points = std::collections::BTreeSet::new();
    for (run, (workers, warmup)) in suite.runs.iter().zip([(1u64, 0u32), (3, 1)]) {
        let plan = run.case.gqt().unwrap();
        assert!(points.insert(plan.planned_sha256.clone()));
        let plan = plan.clone().with_block_run(Some(warmup), None).unwrap();
        let (sample, _) = run_sample(&plan)
            .await
            .unwrap_or_else(|error| panic!("W = {workers}: {error}"));
        assert_eq!(sample.outcome, "expectations-passed");
        let sample = serde_json::to_value(&sample).unwrap();
        let block = &sample["concurrent"];
        assert_eq!(block["driver"], "closed-loop");
        assert_eq!(block["claim_grade"], false);
        assert_eq!(number(block, "warmup_seconds"), u64::from(warmup));
        assert_eq!(number(block, "measured_seconds"), 2);
        assert_eq!(number(block, "sweep.W"), workers);
        let window_us = number(block, "window_us");
        assert!(
            window_us >= 2_000_000,
            "the window is the declared duration"
        );
        assert!(
            number(&sample, "elapsed_us")
                >= u64::from(warmup) * 1_000_000 + window_us + number(block, "drain_us")
        );
        assert_eq!(
            warmup_acked(block) > 0,
            warmup > 0,
            "W = {workers}: a statement is a warmup's only under a declared warmup"
        );
        let labels = block["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 2, "one row per label, in declaration order");
        let written = assert_label(&labels[0], "writer", "load", workers, window_us);
        assert_label(&labels[1], "reader", "query", 2, window_us);
        assert_eq!(
            number(block, "readback.rows"),
            written,
            "every acknowledged dealt row is read back, the warmup's included"
        );
        assert!(number(block, "readback.compared") > 0);
        let receipts = sample["steps"].as_array().unwrap();
        assert_eq!(receipts[0]["ordinal"], 1);
        assert_eq!(receipts[0]["kind"], "concurrent");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_record_round_trips_and_refuses_what_its_samples_do_not_show() {
    use crate::gqt_record::{
        AnyRunRecordV1, ConcurrentRunRecordV1, canonical_bytes, parse, validate,
        validate_concurrent,
    };
    let directory = tempfile::tempdir().unwrap();
    let suite = short_catalog(directory.path())
        .resolve_in(Some("writers"), None, &recorded_environment())
        .unwrap();
    let inner = authority_record(suite.runs[1].case.gqt().unwrap().clone()).await;
    assert!(
        validate(&inner).is_err(),
        "a block's samples are refused outside the concurrent record"
    );
    let observed = inner.measurements.raw_samples[0]
        .concurrent
        .clone()
        .unwrap();
    let record = ConcurrentRunRecordV1 {
        format_version: 1,
        driver: observed.driver,
        claim_grade: false,
        warmup_seconds: 0,
        measured_seconds: 2,
        sweep: observed.sweep.clone(),
        tokio_unstable: observed.tokio_unstable,
        record: inner,
    };
    validate_concurrent(&record).unwrap();
    let bytes = canonical_bytes(&record).unwrap();
    let AnyRunRecordV1::Concurrent(parsed) = parse(&bytes).unwrap() else {
        panic!("a concurrent record parses as its own variant");
    };
    assert_eq!(*parsed, record);
    type Mutation = fn(&mut ConcurrentRunRecordV1);
    let mutations: &[(&str, Mutation)] = &[
        ("claim grade", |r| r.claim_grade = true),
        ("another duration", |r| r.measured_seconds = 3),
        ("another swept value", |r| {
            r.sweep.insert("W".into(), 1);
        }),
        ("a label's count", |r| {
            let sample = &mut r.record.measurements.raw_samples[0];
            sample.concurrent.as_mut().unwrap().labels[0].acked += 1;
        }),
        ("a label's workers", |r| {
            let sample = &mut r.record.measurements.raw_samples[0];
            sample.concurrent.as_mut().unwrap().labels[0].workers = 2;
        }),
        ("a missing observation", |r| {
            r.record.measurements.raw_samples[0].concurrent = None;
        }),
        ("a warmup count under no warmup", |r| {
            let sample = &mut r.record.measurements.raw_samples[0];
            sample.concurrent.as_mut().unwrap().labels[1].warmup_acked = 1;
        }),
    ];
    for (name, mutate) in mutations {
        let mut changed = record.clone();
        mutate(&mut changed);
        assert!(validate_concurrent(&changed).is_err(), "{name}");
    }
    for (warmup, measured) in [(0, 0), (0, 3601), (3601, 2)] {
        let mut changed = record.clone();
        (changed.warmup_seconds, changed.measured_seconds) = (warmup, measured);
        let refusal = validate_concurrent(&changed).unwrap_err().message;
        assert!(
            refusal.contains("warmup is 0 to 3600 seconds and its measured run 1 to 3600"),
            "a run of {warmup} and {measured} seconds: {refusal}"
        );
    }
}

#[test]
fn a_block_run_from_the_command_line_keeps_the_point_and_is_refused_elsewhere() {
    let directory = tempfile::tempdir().unwrap();
    let block = short_catalog(directory.path())
        .plan_in("writers", &recorded_environment())
        .unwrap();
    let longer = block.clone().with_block_run(Some(1), Some(7)).unwrap();
    assert_eq!(
        longer.block,
        Some(BlockRun {
            warmup_seconds: 1,
            duration_seconds: 7
        })
    );
    assert_eq!(
        longer.planned_sha256, block.planned_sha256,
        "a run setting is outside the point's identity"
    );
    let minute = std::time::Duration::from_secs(60);
    assert_eq!(
        longer.past_declared_run(minute),
        std::time::Duration::from_secs(68),
        "a deadline counts past the warmup and the duration the run was given"
    );
    assert_eq!(plan("tiny-read").past_declared_run(minute), minute);
    assert!(block.with_block_run(None, Some(0)).is_err());
    let refused = plan("tiny-read").with_block_run(Some(1), None).unwrap_err();
    assert!(
        refused.contains("measures no `--- concurrent measured` block"),
        "{refused}"
    );
}

#[test]
fn a_bench_section_names_the_repetitions_between_the_entry_and_the_defaults() {
    let directory = tempfile::tempdir().unwrap();
    short_catalog(directory.path());
    std::fs::write(
        directory.path().join("workloads/writers.gqt"),
        WORKLOAD.replace(
            "duration_seconds: 2\n",
            "duration_seconds: 2\nrepetitions: 3\n",
        ),
    )
    .unwrap();
    let repetitions = |config: &str, named: Option<u32>| {
        let path = directory.path().join("benchmarks.yaml");
        std::fs::write(&path, config).unwrap();
        crate::catalog::Catalog::load(&path)
            .unwrap()
            .resolve_in(Some("writers"), named, &recorded_environment())
            .unwrap()
            .runs
            .iter()
            .map(|run| run.repetitions)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        repetitions(CONFIG, None),
        [3, 3],
        "the section outranks the catalog's defaults, on every swept point"
    );
    let entry = CONFIG.replace(
        "  workload: workloads/writers.gqt\n",
        "  workload: workloads/writers.gqt\n  repetitions: 2\n",
    );
    assert_eq!(repetitions(&entry, None), [2, 2]);
    assert_eq!(repetitions(&entry, Some(4)), [4, 4]);
}

#[test]
fn concurrent_writes_plans_one_point_per_swept_worker_count() {
    let suite = recorded_catalog()
        .resolve_in(Some("concurrency"), None, &recorded_environment())
        .unwrap();
    let mut points = std::collections::BTreeSet::new();
    let mut swept = Vec::new();
    for run in &suite.runs {
        let plan = run.case.gqt().unwrap();
        assert_eq!(plan.definition.id, "concurrent-writes");
        let measured = &plan.definition.workload.measured_step;
        assert_eq!(measured.ordinal, 1);
        assert!(measured.text.starts_with("--- concurrent measured\n"));
        assert!(points.insert(plan.planned_sha256.clone()));
        let plan = serde_json::to_value(plan).unwrap();
        assert_eq!(number(&plan, "block.warmup_seconds"), 5);
        assert_eq!(number(&plan, "block.duration_seconds"), 30);
        swept.push(number(&plan, "queries.sweep.W"));
    }
    assert_eq!(swept, [1, 2, 4, 8, 16]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_runs_at_its_first_and_last_swept_worker_count() {
    let suite = recorded_catalog()
        .resolve_in(Some("concurrency"), None, &recorded_environment())
        .unwrap();
    let runs = [suite.runs.first().unwrap(), suite.runs.last().unwrap()];
    for (run, workers) in runs.into_iter().zip([1u64, 16]) {
        let plan = run
            .case
            .gqt()
            .unwrap()
            .clone()
            .with_block_run(Some(0), Some(2))
            .unwrap();
        let (sample, _) = run_sample(&plan)
            .await
            .unwrap_or_else(|error| panic!("W = {workers}: {error}"));
        assert_eq!(sample.outcome, "expectations-passed", "W = {workers}");
        let sample = serde_json::to_value(&sample).unwrap();
        let block = &sample["concurrent"];
        assert_eq!(number(block, "sweep.W"), workers);
        assert_eq!(number(block, "measured_seconds"), 2);
        assert_eq!(
            warmup_acked(block),
            0,
            "W = {workers}: the run was given no warmup"
        );
        let window_us = number(block, "window_us");
        let labels = block["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 2, "W = {workers}");
        let written = assert_label(&labels[0], "writer", "load", workers, window_us);
        assert_label(&labels[1], "reader", "query", 4, window_us);
        assert_eq!(number(block, "readback.rows"), written, "W = {workers}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_statement_stops_every_worker_and_fails_the_repetition_unsettled() {
    let block = r#"--- concurrent measured
lost on nosuch: query add() { insert Chunk { slug: "lost", n: 1 } }
reader x2: query seeded_row() { match { $c: Chunk } where $c.slug = "seed-07" return { $c.n } }
--- expect
lost: ok
reader: ok
"#;
    let plan = first_point(&block_workload(600, block, SEEDED_ROW));
    let started = std::time::Instant::now();
    let error = run_sample(&plan).await.unwrap_err();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(120),
        "the readers stop with the failed writer, not at the declared 600 seconds"
    );
    assert_eq!(error.code, "gqt_measure_failed", "{error}");
    assert!(
        error.message.contains("session `lost` on its run 0") && error.message.contains("nosuch"),
        "{error}"
    );
    assert!(
        error.context.gqt_settled_sample.is_none(),
        "a block that never settled leaves no sample"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dealt_stream_that_runs_out_fails_the_block_on_the_run_past_its_last_row() {
    let block = r#"--- concurrent measured
writer: load generate: v1 seed: 42 mode: insert
  tables:
    - kind: node
      name: Chunk
      rows: 2
      commits: 1
      columns:
        slug: {kind: key, prefix: cw-, width: 5}
        n: {kind: ordinal, start: 1000, step: 1}
reader: query seeded_row() { match { $c: Chunk } where $c.slug = "seed-07" return { $c.n } }
--- expect
writer: ok
reader: ok
"#;
    let error = run_sample(&first_point(&block_workload(600, block, SEEDED_ROW)))
        .await
        .unwrap_err();
    assert_eq!(error.code, "gqt_measure_failed", "{error}");
    assert!(
        error.message.contains("session `writer` on its run 2"),
        "one worker takes row `s` on its run `s`, and the recipe holds two: {error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acknowledged_row_missing_at_the_readback_fails_the_settled_repetition() {
    let block = r#"--- concurrent measured
writer: load generate: v1 seed: 42 mode: insert
  tables:
    - kind: node
      name: Chunk
      rows: 40960
      commits: 10
      columns:
        slug: {kind: key, prefix: cw-, width: 5}
        n: {kind: ordinal, start: 1000, step: 1}
eraser: query erase() { match { $c: Chunk } where $c.slug = "cw-00000" delete $c }
--- expect
writer: ok
eraser: ok
"#;
    let error = run_sample(&first_point(&block_workload(2, block, SEEDED_ROW)))
        .await
        .unwrap_err();
    assert_eq!(error.code, "gqt_verification_failed", "{error}");
    assert!(
        error.message.contains("dealt row 0 (slug = \"cw-00000\")")
            && error.message.contains("the readback found no such row"),
        "{error}"
    );
    let sample = error
        .context
        .gqt_settled_sample
        .as_ref()
        .expect("the block settled before its readback");
    assert_eq!(sample.outcome, "verification-failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_readback_looks_for_each_labels_rows_in_that_labels_key_column() {
    let block = r#"--- concurrent measured
slugged: load generate: v1 seed: 42 mode: insert
  tables:
    - kind: node
      name: Chunk
      rows: 40960
      commits: 10
      columns:
        slug: {kind: key, prefix: cw-, width: 5}
        n: {kind: ordinal, start: 1000, step: 1}
coded: load generate: v1 seed: 43 mode: insert
  tables:
    - kind: node
      name: Chunk
      rows: 40960
      commits: 10
      columns:
        code: {kind: key, prefix: cc-, width: 5}
        slug: {kind: key, prefix: cs-, width: 5}
        n: {kind: ordinal, start: 50000, step: 1}
--- expect
slugged: ok
coded: ok
"#;
    let directory = tempfile::tempdir().unwrap();
    let fixture = FIXTURE.replace("n: I64 }", "n: I64 code: String? }");
    let workload = block_workload(2, block, SEEDED_ROW);
    let plan = catalog_over(directory.path(), &fixture, &workload)
        .plan_in("writers", &recorded_environment())
        .unwrap();
    let (sample, _) = run_sample(&plan)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(sample.outcome, "expectations-passed");
    let sample = serde_json::to_value(&sample).unwrap();
    let block = &sample["concurrent"];
    let window_us = number(block, "window_us");
    let labels = block["labels"].as_array().unwrap();
    let written = assert_label(&labels[0], "slugged", "load", 1, window_us)
        + assert_label(&labels[1], "coded", "load", 1, window_us);
    assert_eq!(
        number(block, "readback.rows"),
        written,
        "`slugged` is read back by `slug` and `coded` by `code`, the first key column of each recipe"
    );
}

const BESIDE: &str = r#"
--- concurrent
extra xWORKERS: load generate: v1 seed: 9 mode: insert
  tables:
    - kind: node
      name: Chunk
      rows: WORKERS
      commits: 1
      columns:
        slug: {kind: key, prefix: px-, width: 3}
        n: {kind: ordinal, start: 5000, step: 1}
--- expect
extra: ok

--- query branch: main
query chunks() { match { $c: Chunk } return { count($c) as n } }
--- expect unordered
{"n": TOTAL}
--- expect shape
n: I64
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_beside_the_measured_one_runs_once_under_the_bound_of_eight() {
    let measured = r#"--- concurrent measured
reader x9: query seeded_row() { match { $c: Chunk } where $c.slug = "seed-07" return { $c.n } }
--- expect
reader: ok
"#;
    let beside = |workers: u32| {
        BESIDE
            .replace("WORKERS", &workers.to_string())
            .replace("TOTAL", &(16 + workers).to_string())
    };
    let (sample, _) = run_sample(&first_point(&block_workload(1, measured, &beside(2))))
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        sample.outcome, "expectations-passed",
        "two workers over a recipe of two rows leave eighteen; a second pass has no row to take"
    );
    let directory = tempfile::tempdir().unwrap();
    let refused = catalog_of(directory.path(), &block_workload(1, measured, &beside(9)))
        .plan_in("writers", &recorded_environment())
        .unwrap_err();
    assert!(
        refused[0]
            .message
            .contains("the bench host runs at most 8 workers under one label; step 2 declares 9"),
        "the measured block's nine readers are admitted and the other block's are not: {refused:?}"
    );
}

#[test]
fn a_route_that_plans_one_point_refuses_a_workload_swept_over_several_values() {
    use crate::gqt_case::{explicit_pair_in_environment, load_with_root, override_sources};
    let directory = tempfile::tempdir().unwrap();
    let swept = short_catalog(directory.path())
        .plan_in("writers", &recorded_environment())
        .unwrap();
    let config = directory.path().join("benchmarks.yaml");
    let workload = directory.path().join("workloads/writers.gqt");
    let refusals = [
        load_with_root(&config, swept.definition.clone(), directory.path()),
        override_sources(swept.clone(), None, Some(&workload)),
        explicit_pair_in_environment(
            &directory.path().join("fixtures/chunks.gqt"),
            &workload,
            swept.definition.workload.measured_step.clone(),
            recorded_environment(),
            swept.definition.protocol.reset,
            Some(60),
        ),
    ];
    for refusal in refusals {
        let refusal = refusal.unwrap_err();
        assert!(
            refusal.contains("sweeps `W` over 2 values") && refusal.contains("catalog scenario"),
            "{refusal}"
        );
    }
    let single = tempfile::tempdir().unwrap();
    let one = catalog_of(single.path(), &WORKLOAD.replace("[1, 3]", "[3]"))
        .plan_in("writers", &recorded_environment())
        .unwrap();
    let loaded = load_with_root(
        &single.path().join("benchmarks.yaml"),
        one.definition.clone(),
        single.path(),
    )
    .unwrap();
    assert_eq!(
        loaded.planned_sha256, one.planned_sha256,
        "a sweep of one value is one point on every route"
    );
}

#[cfg(unix)]
mod supervised {
    use super::*;
    use crate::gqt_protocol::{
        ChildFrameV2, RepetitionInputV2, WORKER_PROTOCOL_VERSION, digest_worker_executable,
        write_frame,
    };
    use crate::gqt_supervisor::{SupervisionInput, supervise_repetition};
    use crate::supervisor::tests::{machine_identity, worker_build, worker_script};
    use std::time::{Duration, Instant};

    const EXCHANGE: &str = r#"
[ "$1" = __gqt-worker-v2 ] || exit 89
IFS= read -r request || exit 90
IFS= read -r frame < "$0.ready" || exit 92
printf '%s\n' "$frame"
IFS= read -r begin || exit 91
IFS= read -r hold < "$0.hold" || exit 95
/bin/sleep "$hold"
IFS= read -r frame < "$0.settled" || exit 93
printf '%s\n' "$frame"
IFS= read -r frame < "$0.complete" || exit 94
printf '%s\n' "$frame"
exit 0
"#;

    fn sidecar(worker: &Path, suffix: &str) -> PathBuf {
        let mut path = worker.as_os_str().to_owned();
        path.push(format!(".{suffix}"));
        PathBuf::from(path)
    }

    fn write_sidecar_frame(worker: &Path, suffix: &str, frame: &ChildFrameV2) {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, frame).unwrap();
        std::fs::write(sidecar(worker, suffix), bytes).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_supervisor_ceiling_counts_past_a_measured_blocks_declared_run() {
        let directory = tempfile::tempdir().unwrap();
        let plan = short_catalog(directory.path())
            .plan_in("writers", &recorded_environment())
            .unwrap()
            .with_block_run(Some(0), Some(1))
            .unwrap();
        let declared = Duration::from_secs(1);
        let record = authority_record(plan.clone()).await;
        let fixture = record.fixture.unwrap();
        let mut sample = record.measurements.raw_samples.into_iter().next().unwrap();
        sample.peak_rss_bytes = None;
        let settled = Duration::from_micros(sample.elapsed_us);
        assert!(settled > declared, "the block ran for its declared second");
        let bound = plan
            .bind(
                &fixture.handoff.summary.logical_content_sha256,
                &fixture.handoff.summary.algorithm,
            )
            .unwrap();
        let execution = RepetitionInputV2::Embedded {
            repetition_root: directory.path().join("active"),
            physical_digest: fixture.handoff.physical.clone(),
            metadata_digest: fixture.handoff.template_metadata.clone(),
            fixture_manifest_sha256: "b".repeat(64),
            route: LocalRoute::Direct,
        };

        let (_guard, scripts, worker) = worker_script(EXCHANGE);
        let mut build = worker_build();
        build.executable_sha256 = digest_worker_executable(&worker).unwrap();
        write_sidecar_frame(
            &worker,
            "ready",
            &ChildFrameV2::Ready {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: 1,
                point_id: bound.point_id.clone(),
                case_digest: bound.plan.case_digest.clone(),
                worker_build: Box::new(build.clone()),
                machine: Box::new(machine_identity()),
                proof: execution.proof().unwrap(),
            },
        );
        write_sidecar_frame(
            &worker,
            "settled",
            &ChildFrameV2::Settled {
                protocol_version: WORKER_PROTOCOL_VERSION,
                repetition: 1,
                elapsed_us: sample.elapsed_us,
            },
        );
        write_sidecar_frame(
            &worker,
            "complete",
            &ChildFrameV2::Complete {
                protocol_version: WORKER_PROTOCOL_VERSION,
                point_id: bound.point_id.clone(),
                case_digest: bound.plan.case_digest.clone(),
                sample: Box::new(sample.clone()),
            },
        );
        let scratch = scripts.path().join("worker-scratch-00000001");
        std::fs::create_dir(&scratch).unwrap();
        let supervise = |hold: Duration, deadline: Duration| {
            std::fs::write(
                sidecar(&worker, "hold"),
                format!("{}\n", hold.as_secs_f64()),
            )
            .unwrap();
            supervise_repetition(SupervisionInput {
                worker_executable: worker.clone(),
                expected_worker_executable_sha256: build.executable_sha256.clone(),
                expected_machine: None,
                execution: execution.clone(),
                repetition: 1,
                case: bound.clone(),
                worker_scratch_root: scratch.clone(),
                deadline: Some(deadline),
                auxiliary_deadline_override: Some(Duration::from_secs(10)),
            })
        };

        let before_settled = settled - Duration::from_millis(1);
        let mut admitted = supervise(settled + Duration::from_millis(50), before_settled)
            .unwrap_or_else(|error| {
                panic!("a block settling inside its declared run is admitted: {error}")
            })
            .sample;
        assert!(admitted.peak_rss_bytes.is_some_and(|bytes| bytes > 0));
        admitted.peak_rss_bytes = None;
        assert_eq!(admitted, sample);

        let deadline = Duration::from_secs(1);
        let started = Instant::now();
        let error = supervise(Duration::from_secs(60), deadline).unwrap_err();
        let waited = started.elapsed();
        assert_eq!(error.code, "operation_deadline_exceeded");
        assert!(
            error.message.contains(
                "within the declared 1 second deadline past the measured block's declared 1 second run"
            ),
            "{}",
            error.message
        );
        assert!(
            waited >= deadline + declared,
            "the worker was killed after {waited:?}, before the deadline had counted past the declared run"
        );
        let child = error
            .context
            .child_process
            .as_ref()
            .expect("a killed worker leaves containment evidence");
        assert_eq!(child.stage, "measure-timeout");
        assert!(child.direct_child_reaped && child.process_group_gone);
    }
}
