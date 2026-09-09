//! RFC 0048 selection cost instrument. The input is a persisted binding
//! relation, not an OmniGraph graph traversal or a complete retrieval query.
//! Every physical alternative is checked against a scalar, independent oracle.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::NullEquality;
use datafusion::dataframe::DataFrame;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions_window::rank::dense_rank;
use datafusion::functions_window::row_number::row_number;
use datafusion::logical_expr::{ExprFunctionExt, JoinType, LogicalPlanBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::datafusion::LanceTableProvider;
use lance::dataset::WriteParams;
use lance::dataset::builder::DatasetBuilder;
use lance::io::exec::TakeExec;
use lance_core::datatypes::OnMissing;
use lance_file::version::LanceFileVersion;
use lance_io::object_store::ObjectStoreParams;
use lance_io::utils::tracking_store::IOTracker;

#[derive(Debug, Clone, serde::Serialize)]
pub struct Tuning {
    pub plan: String,
    pub group_select: String,
    pub join_filters: bool,
    pub reattach_cut: bool,
    pub late_payload: bool,
    pub fanout: usize,
    pub groups: usize,
    pub quota: u64,
    pub partitions: usize,
    pub memory_mb: usize,
    pub scratch_mb: u64,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            plan: "default".into(),
            group_select: "dedup".into(),
            join_filters: true,
            reattach_cut: false,
            late_payload: false,
            fanout: 4,
            groups: 64,
            quota: 2,
            partitions: 4,
            memory_mb: 128,
            scratch_mb: 1024,
        }
    }
}

pub fn validate(
    tuning: &Tuning,
    rows: usize,
    selectivity: f64,
    text_bytes: usize,
) -> Result<(), String> {
    if !matches!(tuning.plan.as_str(), "default" | "hash" | "merge") {
        return Err("--selection-plan must be default, hash or merge".into());
    }
    if !matches!(tuning.group_select.as_str(), "dedup" | "dense") {
        return Err("--group-select must be dedup or dense".into());
    }
    if tuning.group_select == "dense" && tuning.reattach_cut {
        return Err("--reattach-cut is only meaningful with --group-select dedup".into());
    }
    if rows == 0 || rows > 1_000_000 || !(1..=64).contains(&tuning.fanout) {
        return Err("selection needs 1..=1000000 targets and 1..=64 paths per target".into());
    }
    if !(1..=4096).contains(&tuning.groups) || !(1..=64).contains(&tuning.partitions) {
        return Err("selection needs 1..=4096 groups and 1..=64 partitions".into());
    }
    if !selectivity.is_finite() || !(0.0..=1.0).contains(&selectivity) {
        return Err("selection selectivity must be finite and between zero and one".into());
    }
    if text_bytes > 1_048_576
        || !(1..=65536).contains(&tuning.memory_mb)
        || tuning.scratch_mb > 65536
    {
        return Err(
            "selection payload <= 1 MiB, pool 1..=65536 MiB and scratch <= 65536 MiB".into(),
        );
    }
    Ok(())
}

// Integer scores intentionally include ties; id is the explicit tie-breaker.
fn score(id: u64, seed: u64) -> i64 {
    let mut x = id.wrapping_add(seed).wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    ((x ^ (x >> 31)) % 1009) as i64
}

fn group(id: u64, path: usize, tuning: &Tuning) -> Option<i64> {
    // Adjacent paths duplicate a membership, while later pairs can enter
    // another group. Zero is a real nullable group, not an omitted edge.
    let value = (id as usize + path / 2) % tuning.groups;
    (value != 0).then_some(value as i64)
}

fn payload(binding: u64, seed: u64, bytes: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
    let mut state = binding.wrapping_add(seed).wrapping_add(1);
    let bytes = (0..bytes)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            ALPHABET[(state >> 58) as usize]
        })
        .collect();
    String::from_utf8(bytes).unwrap()
}

fn oracle(rows: usize, seed: u64, eligible: usize, k: usize, tuning: &Tuning) -> Vec<u64> {
    let mut targets: Vec<_> = (0..rows as u64)
        .filter(|id| *id < eligible as u64)
        .collect();
    targets.sort_by_key(|id| (std::cmp::Reverse(score(*id, seed)), *id));
    targets.truncate(k);
    let mut by_group = BTreeMap::<Option<i64>, BTreeSet<u64>>::new();
    for id in targets {
        for path in 0..tuning.fanout {
            by_group
                .entry(group(id, path, tuning))
                .or_default()
                .insert(id);
        }
    }
    let mut pairs = BTreeSet::new();
    for (group, ids) in by_group {
        let mut ids: Vec<_> = ids.into_iter().collect();
        ids.sort_by_key(|id| (std::cmp::Reverse(score(*id, seed)), *id));
        for id in ids.into_iter().take(tuning.quota as usize) {
            pairs.insert((id, group));
        }
    }
    let mut bindings: Vec<_> = (0..rows as u64 * tuning.fanout as u64)
        .filter(|binding| {
            let id = *binding / tuning.fanout as u64;
            let path = *binding as usize % tuning.fanout;
            pairs.contains(&(id, group(id, path, tuning)))
        })
        .collect();
    bindings.sort_by_key(|binding| {
        let id = *binding / tuning.fanout as u64;
        (std::cmp::Reverse(score(id, seed)), id, *binding)
    });
    bindings
}

fn metric_rows(plan: &Arc<dyn ExecutionPlan>, rows: &mut Vec<serde_json::Value>) {
    let metrics = plan.metrics();
    rows.push(serde_json::json!({
        "operator": plan.name(),
        "output_rows": metrics.as_ref().and_then(|m| m.output_rows()),
        "spill_count": metrics.as_ref().and_then(|m| m.spill_count()),
        "spilled_bytes": metrics.as_ref().and_then(|m| m.spilled_bytes()),
        "native_iops": metrics.as_ref().and_then(|m| m.sum_by_name("iops")).map(|m| m.as_usize()),
        "native_read_bytes": metrics.as_ref().and_then(|m| m.sum_by_name("bytes_read")).map(|m| m.as_usize()),
    }));
    for child in plan.children() {
        metric_rows(child, rows);
    }
}

pub async fn run(
    rows: usize,
    seed: u64,
    selectivity: f64,
    k: usize,
    text_bytes: usize,
    tuning: &Tuning,
) -> serde_json::Value {
    validate(tuning, rows, selectivity, text_bytes).expect("validated selection fixture");
    let eligible = (rows as f64 * selectivity).floor() as usize;
    let expected = oracle(rows, seed, eligible, k, tuning);
    let directory = tempfile::tempdir().expect("selection fixture directory");
    let uri = directory.path().join("bindings.lance");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("group_key", DataType::Int64, true),
        Field::new("score", DataType::Int64, false),
        Field::new("binding_id", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let binding_count = rows * tuning.fanout;
    let batch_rows = (4 * 1024 * 1024 / (text_bytes + 40)).clamp(1, 4096);
    let batch_schema = schema.clone();
    let batch_tuning = tuning.clone();
    let batches = (0..binding_count).step_by(batch_rows).map(move |start| {
        let tuning = &batch_tuning;
        let end = (start + batch_rows).min(binding_count);
        RecordBatch::try_new(
            batch_schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(
                    (start..end).map(|b| (b / tuning.fanout) as u64),
                )),
                Arc::new(Int64Array::from(
                    (start..end)
                        .map(|b| group((b / tuning.fanout) as u64, b % tuning.fanout, tuning))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|b| score((b / tuning.fanout) as u64, seed)),
                )),
                Arc::new(UInt64Array::from_iter_values(start as u64..end as u64)),
                Arc::new(StringArray::from_iter_values(
                    (start..end).map(|b| payload(b as u64, seed, text_bytes)),
                )),
            ],
        )
    });
    // Setup is outside query timing. Parent peak RSS includes this setup and
    // the independent oracle; it must not be described as query-only RSS.
    let setup_started = Instant::now();
    let dataset = Dataset::write(
        RecordBatchIterator::new(batches, schema),
        uri.to_str().unwrap(),
        Some(WriteParams {
            max_rows_per_file: 8192,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await
    .expect("write selection bindings");
    let fragments = dataset.get_fragments().len();
    drop(dataset);
    let setup_ms = setup_started.elapsed().as_secs_f64() * 1000.0;

    let tracker = IOTracker::default();
    let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(tuning.memory_mb * 1024 * 1024));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(pool.clone())
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_max_temp_directory_size(tuning.scratch_mb * 1024 * 1024),
            )
            .build()
            .unwrap(),
    );
    let mut config = SessionConfig::new()
        .with_target_partitions(tuning.partitions)
        .with_batch_size(256);
    config
        .options_mut()
        .optimizer
        .enable_join_dynamic_filter_pushdown = tuning.join_filters;
    if tuning.plan != "default" {
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        config.options_mut().optimizer.prefer_hash_join = tuning.plan == "hash";
    }
    let ctx = SessionContext::new_with_config_rt(config, runtime.clone());
    let query_started = Instant::now();
    let dataset = DatasetBuilder::from_uri(uri.to_str().unwrap())
        .with_store_params(ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(tracker.clone())),
            ..Default::default()
        })
        .load()
        .await
        .expect("reopen native source");
    let dataset = Arc::new(dataset);
    let bindings = ctx
        .read_table(Arc::new(
            LanceTableProvider::new_with_ordering(
                dataset.clone(),
                tuning.late_payload,
                false,
                false,
            )
            .with_batch_size(256),
        ))
        .unwrap();
    let bindings = if tuning.late_payload {
        bindings
            .select(vec![
                col("id"),
                col("group_key"),
                col("score"),
                col("binding_id"),
                col("_rowid"),
            ])
            .unwrap()
    } else {
        bindings
    };
    let eligible_bindings = bindings.filter(col("id").lt(lit(eligible as u64))).unwrap();
    let order = || vec![col("score").sort(false, false), col("id").sort(true, false)];
    let targets = eligible_bindings
        .clone()
        .select(vec![col("id"), col("score")])
        .unwrap()
        .distinct()
        .unwrap()
        .sort(order())
        .unwrap()
        .limit(0, Some(k))
        .unwrap();
    let cut_bindings = eligible_bindings
        .clone()
        .join(
            targets.select(vec![col("id")]).unwrap(),
            JoinType::LeftSemi,
            &["id"],
            &["id"],
            None,
        )
        .unwrap();
    let selected = if tuning.group_select == "dense" {
        // The score is constant for each target/group pair, and target id is
        // the final tie key. Duplicate paths share one dense rank, so the
        // window can preserve all winning bindings without a nullable rejoin.
        cut_bindings
            .window(vec![
                dense_rank()
                    .partition_by(vec![col("group_key")])
                    .order_by(order())
                    .build()
                    .unwrap()
                    .alias("position"),
            ])
            .unwrap()
            .filter(col("position").lt_eq(lit(tuning.quota)))
            .unwrap()
    } else {
        let winners = cut_bindings
            .clone()
            .select(vec![col("id"), col("group_key"), col("score")])
            .unwrap()
            .distinct()
            .unwrap()
            .window(vec![
                row_number()
                    .partition_by(vec![col("group_key")])
                    .order_by(order())
                    .build()
                    .unwrap()
                    .alias("position"),
            ])
            .unwrap()
            .filter(col("position").lt_eq(lit(tuning.quota)))
            .unwrap()
            .select(vec![
                col("id").alias("winner_id"),
                col("group_key").alias("winner_group"),
            ])
            .unwrap();
        // Winners are already a subset of the cut. Joining those pairs back to
        // the incoming bindings is sufficient; repeating the cut on the probe
        // side duplicates target deduplication, sorting and native reads.
        let reattach_input = if tuning.reattach_cut {
            cut_bindings
        } else {
            eligible_bindings
        };
        let logical = LogicalPlanBuilder::from(reattach_input.into_unoptimized_plan())
            .join_detailed(
                winners.into_unoptimized_plan(),
                JoinType::LeftSemi,
                (vec!["id", "group_key"], vec!["winner_id", "winner_group"]),
                None,
                NullEquality::NullEqualsNull,
            )
            .unwrap()
            .build()
            .unwrap();
        DataFrame::new(ctx.state(), logical)
    };
    let mut final_order = order();
    final_order.push(col("binding_id").sort(true, false));
    let frame = selected.sort(final_order).unwrap();
    let plan = frame
        .create_physical_plan()
        .await
        .expect("selection physical plan");
    // Carry native row IDs from the accepted dataset, never infer them from
    // logical binding IDs. Hydrate only the final ordered winners through a
    // public native ExecutionPlan using the same caller TaskContext.
    let plan: Arc<dyn ExecutionPlan> = if tuning.late_payload {
        let projection = dataset
            .empty_projection()
            .union_column("payload", OnMissing::Error)
            .unwrap();
        Arc::new(
            TakeExec::try_new(dataset, plan, projection)
                .unwrap()
                .expect("payload is absent from narrow input"),
        )
    } else {
        plan
    };
    let planning_ms = query_started.elapsed().as_secs_f64() * 1000.0;
    let plan_text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    if k > 0 && eligible > 0 && tuning.quota > 0 {
        assert!(plan_text.contains("Lance"), "{plan_text}");
        let expected_join = if tuning.plan == "merge" && tuning.partitions > 1 {
            "SortMergeJoinExec"
        } else {
            "HashJoinExec"
        };
        assert!(plan_text.contains(expected_join), "{plan_text}");
    }
    let execution_started = Instant::now();
    let mut output_rows = 0;
    let mut output_arrow_bytes = 0;
    let mut oracle_error = None;
    let result: datafusion::error::Result<()> = async {
        let mut stream = datafusion::physical_plan::execute_stream(plan.clone(), ctx.task_ctx())?;
        while let Some(batch) = stream.try_next().await? {
            output_arrow_bytes += batch.get_array_memory_size();
            let ids = batch
                .column_by_name("binding_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let texts = batch
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                if Some(&ids.value(row)) != expected.get(output_rows) && oracle_error.is_none() {
                    oracle_error = Some(format!(
                        "binding row {output_rows}: got {}, expected {:?}",
                        ids.value(row),
                        expected.get(output_rows)
                    ));
                }
                assert!(!texts.is_null(row));
                assert_eq!(texts.value(row), payload(ids.value(row), seed, text_bytes));
                output_rows += 1;
            }
        }
        if output_rows != expected.len() && oracle_error.is_none() {
            oracle_error = Some(format!(
                "selection returned {output_rows} rows, expected {}",
                expected.len()
            ));
        }
        Ok(())
    }
    .await;
    let execution_ms = execution_started.elapsed().as_secs_f64() * 1000.0;
    let io = tracker.incremental_stats();
    let mut operators = Vec::new();
    metric_rows(&plan, &mut operators);
    let error = result.err().map(|error| error.to_string());
    // CollectLeft keeps its shared build result on the physical plan. Query
    // cleanup must drop that plan as well as the stream. Capture metrics first.
    let pool_after_stream = pool.reserved();
    drop(plan);
    drop(ctx);
    let cleanup_started = Instant::now();
    let scratch_files = || -> usize {
        runtime
            .disk_manager
            .temp_dir_paths()
            .iter()
            .map(|path| std::fs::read_dir(path).unwrap().count())
            .sum()
    };
    let cleanup_complete = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while pool.reserved() != 0
            || runtime.disk_manager.spilling_progress().active_files_count != 0
            || scratch_files() != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let scratch_files_after = scratch_files();
    serde_json::json!({
        "routing": "native_lance_binding_relation_datafusion_selection",
        "measurement_boundary": "open, plan, execute and streamed result verification; setup excluded from timings; parent peak RSS includes setup and oracle; OS page cache uncontrolled",
        "tuning": tuning,
        "targets": rows, "bindings": binding_count, "eligible_targets": eligible,
        "source_window": k, "payload_bytes": text_bytes, "fragments": fragments,
        "payload_profile": "deterministic_ascii_v1",
        "setup_ms": setup_ms, "planning_ms": planning_ms, "execution_ms": execution_ms,
        "expected_rows": expected.len(), "output_rows": output_rows,
        "output_arrow_bytes": output_arrow_bytes,
        "object_store_read_iops": io.read_iops, "object_store_read_bytes": io.read_bytes,
        "io_boundary": "object-store wrapper excludes direct local-file reads; native per-operator iops/bytes_read include scan scheduler reads; do not add these as disjoint totals",
        "oracle_pass": if error.is_none() { Some(oracle_error.is_none()) } else { None },
        "oracle_error": oracle_error,
        "error": error,
        "pool_reserved_after_stream": pool_after_stream,
        "cleanup_complete": cleanup_complete,
        "scratch_files_after": scratch_files_after,
        "scratch_accounting_clear": runtime.disk_manager.used_disk_space() == 0,
        "cleanup_ms": cleanup_started.elapsed().as_secs_f64() * 1000.0,
        "pool_reserved_after": pool.reserved(), "scratch_bytes_after": runtime.disk_manager.used_disk_space(),
        "physical_plan": plan_text, "operators": operators,
    })
}
