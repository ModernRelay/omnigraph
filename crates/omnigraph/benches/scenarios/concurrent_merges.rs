//! Concurrent-merges diagnostic: `--writers N` merges into N independent
//! targets at once, against a matched single merge on the same graph.
//!
//! A merge pair is a source and a target forked from `main`, each holding
//! `--delta-rows` inserted `Chunk` rows under its own keys, so every merge is
//! a genuine three-way merge (never a fast-forward) with no conflict. The
//! graph holds N + 2 pairs: the batch's N, and two controls merged alone,
//! one before the batch and one after, so every merge sees the same branch
//! count and the two controls expose drift across the run.
//!
//! Every measured phase opens a fresh handle outside its timer. The batch
//! runs N tasks over clones of one `Session` (the production server shape),
//! released together by a barrier; the batch clock starts before the
//! release, so it bounds every task's start. `last_over_single` is the
//! batch's last completion over the controls' mean: 1.0 is perfect overlap,
//! N is full serialization.
//!
//! One batch per repetition on the host's clock: overlap evidence, not a
//! throughput or latency claim, so the record says `claim_grade: false`.
//! A fresh handle then requires exact row counts on every branch and reads
//! merged source keys back on each target; any merge error, any outcome
//! other than `Merged`, or a verification mismatch fails the run.

use std::sync::Arc;
use std::time::{Duration, Instant};

use omnigraph::Session;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget};
use omnigraph::loader::LoadMode;
use omnigraph::settings::SessionSettings;

use super::run_target::{RunTarget, attestation, spawn_watchdog, validate_target_uri};
use super::{Args, current_process_peak_rss_bytes, helpers, rfc023_limits, rfc023_scenarios};

/// Pair names carry a three-digit index, and the controls take two more.
const MAX_MERGES: usize = 256;

/// Fixture build, three measured phases and verification; generous because
/// one merge against a latency-injected store takes tens of seconds.
const WATCHDOG_BUDGET: Duration = Duration::from_secs(30 * 60);

pub(super) fn is_scenario(name: &str) -> bool {
    name == "concurrent-merges"
}

pub(super) fn validate_args(args: &Args) -> Result<(), String> {
    if args.writers == 0 || args.writers > MAX_MERGES {
        return Err(format!(
            "--writers (the number of concurrent merges) must be in 1..={MAX_MERGES}"
        ));
    }
    if args.delta_rows == 0 {
        return Err("--delta-rows must be greater than zero".into());
    }
    if args.baseline {
        return Err("concurrent-merges has no baseline arm".into());
    }
    if args.phase.is_some() || args.fixture_root.is_some() {
        return Err("--phase/--fixture-root are internal to phased adopt children".into());
    }
    validate_target_uri(args)?;
    rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows)?;
    rfc023_limits::derive_chunk_plan(
        args.dims,
        &MergePair::new(MAX_MERGES + 1).source,
        args.delta_rows,
    )?;
    Ok(())
}

/// A source and a target forked from `main`. Each branch's rows are keyed
/// by the branch name, so the pairs never share a key.
struct MergePair {
    source: String,
    target: String,
}

impl MergePair {
    fn new(index: usize) -> Self {
        Self {
            source: format!("m{index:03}-src"),
            target: format!("m{index:03}-dst"),
        }
    }
}

/// One measured merge: offsets from its phase's clock, and the outcome.
struct MergeTiming {
    target: String,
    started_us: u64,
    completed_us: u64,
    outcome: Result<MergeOutcome, String>,
}

impl MergeTiming {
    fn service_us(&self) -> u64 {
        self.completed_us.saturating_sub(self.started_us)
    }

    fn error(&self) -> Option<String> {
        match &self.outcome {
            Ok(MergeOutcome::Merged) => None,
            Ok(other) => Some(format!(
                "merge into {}: expected a three-way Merged outcome, got {other:?}",
                self.target
            )),
            Err(error) => Some(format!("merge into {}: {error}", self.target)),
        }
    }
}

/// A merge's disposition, or its error rendered for the record.
fn disposition(
    result: omnigraph::error::Result<omnigraph::db::MergeResult>,
) -> Result<MergeOutcome, String> {
    result
        .map(|merged| merged.outcome)
        .map_err(|error| error.to_string())
}

async fn open_session(root_uri: &str) -> Session {
    Session::from_defaults(
        Arc::new(
            Omnigraph::open(root_uri)
                .await
                .expect("open concurrent-merges fixture"),
        ),
        SessionSettings::default(),
    )
}

/// One merge alone on a fresh handle; the open sits outside the timer.
async fn merge_alone(root_uri: &str, pair: &MergePair) -> MergeTiming {
    let session = open_session(root_uri).await;
    let started = Instant::now();
    let outcome = session.branch_merge(&pair.source, &pair.target).await;
    MergeTiming {
        target: pair.target.clone(),
        started_us: 0,
        completed_us: started.elapsed().as_micros() as u64,
        outcome: disposition(outcome),
    }
}

/// Every pair's merge at once on one fresh handle, released by a barrier.
async fn merge_batch(root_uri: &str, pairs: &[MergePair]) -> Vec<MergeTiming> {
    let session = open_session(root_uri).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(pairs.len() + 1));
    let handles: Vec<_> = pairs
        .iter()
        .map(|pair| {
            let session = session.clone();
            let barrier = Arc::clone(&barrier);
            let (source, target) = (pair.source.clone(), pair.target.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                let started = Instant::now();
                let outcome = session.branch_merge(&source, &target).await;
                (target, started, Instant::now(), disposition(outcome))
            })
        })
        .collect();
    let batch_start = Instant::now();
    barrier.wait().await;
    let mut timings = Vec::with_capacity(handles.len());
    for handle in handles {
        let (target, started, completed, outcome) = handle.await.expect("merge task join");
        let offset = |at: Instant| at.saturating_duration_since(batch_start).as_micros() as u64;
        timings.push(MergeTiming {
            target,
            started_us: offset(started),
            completed_us: offset(completed),
            outcome,
        });
    }
    timings
}

pub(super) async fn run(args: &Args) -> serde_json::Value {
    let target = RunTarget::prepare(args, "cm").await;
    let watchdog = spawn_watchdog("concurrent-merges", WATCHDOG_BUDGET);
    let merges = args.writers;
    let batch: Vec<MergePair> = (0..merges).map(MergePair::new).collect();
    let control_before = MergePair::new(merges);
    let control_after = MergePair::new(merges + 1);

    // ---- Fixture (outside every timer) ----------------------------------
    let setup_started = Instant::now();
    let db = Session::from_defaults(
        Arc::new(
            Omnigraph::init(&target.root_uri, &rfc023_scenarios::graph_schema(args.dims))
                .await
                .expect("initialize concurrent-merges fixture"),
        ),
        SessionSettings::default(),
    );
    let patterns = rfc023_scenarios::vector_json_patterns(args.dims, args.seed);
    let base_plan =
        rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows).expect("validated shape");
    rfc023_scenarios::load_graph_rows(
        &db,
        "main",
        "base",
        args.rows,
        base_plan.batch_rows,
        &patterns,
        LoadMode::Append,
    )
    .await;
    let delta_plan =
        rfc023_limits::derive_chunk_plan(args.dims, &control_after.source, args.delta_rows)
            .expect("validated shape");
    let all_pairs: Vec<&MergePair> = batch
        .iter()
        .chain([&control_before, &control_after])
        .collect();
    for pair in &all_pairs {
        for branch in [&pair.source, &pair.target] {
            db.branch_create(branch).await.expect("create merge branch");
            rfc023_scenarios::load_graph_rows(
                &db,
                branch,
                branch,
                args.delta_rows,
                delta_plan.batch_rows,
                &patterns,
                LoadMode::Append,
            )
            .await;
        }
    }
    drop(db);
    let setup_wall_us = setup_started.elapsed().as_micros() as u64;

    // ---- Measured: control, batch, control ------------------------------
    let before = merge_alone(&target.root_uri, &control_before).await;
    let pre_batch_peak_rss = current_process_peak_rss_bytes();
    let timings = merge_batch(&target.root_uri, &batch).await;
    let post_batch_peak_rss = current_process_peak_rss_bytes();
    let after = merge_alone(&target.root_uri, &control_after).await;

    let errors: Vec<String> = [&before, &after]
        .into_iter()
        .chain(&timings)
        .filter_map(MergeTiming::error)
        .collect();
    let single_us = (before.service_us() + after.service_us()) as f64 / 2.0;
    let mut completions: Vec<u64> = timings.iter().map(|t| t.completed_us).collect();
    completions.sort_unstable();
    let last_us = completions.last().copied().unwrap_or(0);
    let mean_completion_us = completions.iter().sum::<u64>() / completions.len() as u64;
    let ratio = |value: f64| {
        if single_us > 0.0 {
            value / single_us
        } else {
            0.0
        }
    };

    // ---- Verification on a fresh handle ----------------------------------
    // Every branch's row count is exact: main keeps the seed, a source adds
    // its delta, and every target (batch and both controls) holds its own
    // delta plus its source's.
    let mut verification_failures: Vec<String> = Vec::new();
    let verify = open_session(&target.root_uri).await;
    let mut expected_rows: Vec<(&str, usize)> = vec![("main", args.rows)];
    for pair in &all_pairs {
        expected_rows.push((pair.source.as_str(), args.rows + args.delta_rows));
        expected_rows.push((pair.target.as_str(), args.rows + 2 * args.delta_rows));
    }
    for (branch, expected) in &expected_rows {
        let actual = helpers::count_rows_branch(&verify, branch, "node:Chunk").await;
        if actual != *expected {
            verification_failures.push(format!(
                "branch {branch}: expected {expected} Chunk rows, found {actual}"
            ));
        }
    }
    // The first and last source key read back by key on every target.
    let readback_src =
        "query get($slug: String) { match { $c: Chunk { slug: $slug } } return { $c.slug } }";
    let mut sampled_readback = 0usize;
    for pair in &all_pairs {
        for row in [0, args.delta_rows - 1] {
            let slug = format!("{}-{row:010}", pair.source);
            let params = helpers::params(&[("$slug", slug.as_str())]);
            match verify
                .query(
                    ReadTarget::branch(&pair.target),
                    readback_src,
                    "get",
                    &params,
                )
                .await
            {
                Ok(result) if result.num_rows() == 1 => {}
                Ok(result) => verification_failures.push(format!(
                    "branch {}: merged key '{slug}' read back {} rows, expected 1",
                    pair.target,
                    result.num_rows()
                )),
                Err(error) => verification_failures.push(format!(
                    "branch {}: readback of merged key '{slug}' failed: {error}",
                    pair.target
                )),
            }
            sampled_readback += 1;
        }
    }
    drop(verify);

    let per_merge: Vec<serde_json::Value> = timings
        .iter()
        .map(|timing| {
            serde_json::json!({
                "target": timing.target,
                "started_us": timing.started_us,
                "completed_us": timing.completed_us,
                "service_us": timing.service_us(),
            })
        })
        .collect();
    let passed = errors.is_empty() && verification_failures.is_empty();
    let record = serde_json::json!({
        "routing": "production-session-branch-merge",
        "production_path": true,
        "driver": "one-shot-batch",
        "claim_grade": false,
        "claim_note": "one batch per repetition on the host's clock: overlap evidence, not a \
                       throughput or latency claim",
        "target_backend": target.backend,
        "target_root": target.root_uri,
        "shape": {
            "merges": merges,
            "merge_pairs": all_pairs.len(),
            "branches": 1 + 2 * all_pairs.len(),
            "seeded_rows": args.rows,
            "delta_rows_per_branch": args.delta_rows,
            "merge_kind": "three-way; source and target each insert disjoint keys; no conflicts",
        },
        "setup_wall_us": setup_wall_us,
        "control_us": {
            "before": before.service_us(),
            "after": after.service_us(),
            "mean": single_us,
            "drift": if before.service_us() > 0 {
                after.service_us() as f64 / before.service_us() as f64
            } else {
                0.0
            },
        },
        "batch": {
            "completion_us": completions,
            "first_completion_us": completions.first().copied().unwrap_or(0),
            "last_completion_us": last_us,
            "mean_completion_us": mean_completion_us,
            "last_over_single": ratio(last_us as f64),
            "mean_over_single": ratio(mean_completion_us as f64),
            "per_merge": per_merge,
        },
        "errors": errors,
        "operation_pre_peak_rss_bytes": pre_batch_peak_rss,
        "operation_post_peak_rss_bytes": post_batch_peak_rss,
        "rss_boundary": "process high-water mark; the pre value already covers fixture seeding \
                         and the first control, so an unchanged post value means the batch did \
                         not raise the peak",
        "measurement_boundary": "each merge's clock wraps Session::branch_merge; the batch clock \
                                 starts before the barrier releases its tasks; fixture build, \
                                 handle opens, verification and teardown are outside every clock",
        "attestation": attestation(),
        "verification": {
            "branches_counted": expected_rows.len(),
            "sampled_readback": sampled_readback,
            "failures": verification_failures,
            "passed": passed,
        },
    });
    assert!(
        passed,
        "concurrent-merges run invalid: errors={errors:?} verification={verification_failures:?}"
    );

    watchdog.abort();
    target.teardown(args, "concurrent-merges").await;
    record
}
