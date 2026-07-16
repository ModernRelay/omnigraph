//! RFC-023 key-fencing decision instruments.
//!
//! The small-upsert and one-ceiling probes intentionally isolate Lance's
//! substrate routes. The all-new adopt scenario is different: a setup child
//! builds and validates the identical persisted OmniGraph fixture, a fresh
//! measured child identically opens it and executes exactly one selected arm,
//! and a third unmeasured child verifies final state. Its normal arm measures
//! production `Omnigraph::branch_merge`; `--baseline` substitutes only that
//! operation with a non-production direct Lance streaming Append. The measured
//! child's `wait4` HWM therefore excludes setup and symmetric post-checks.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    Array as _, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray,
};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
use futures::TryStreamExt as _;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::write::merge_insert::UncommittedMergeInsert;
use lance::dataset::{
    CommitBuilder, InsertBuilder, MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode,
    WriteParams,
};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget, SnapshotTable};
use omnigraph::instrumentation::{MergeWriteProbes, with_merge_write_probes};
use omnigraph::loader::LoadMode;
use sha2::{Digest as _, Sha256};

use super::{Args, rfc023_limits, seeded_vector};

const SMALL_UPSERT_ROWS: usize = 32;

pub(super) fn validate_args(args: &Args) -> Result<(), String> {
    match args.scenario.as_str() {
        "fenced-small-upsert" => {
            rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows)?;
            rfc023_limits::require_single_batch(args.dims, "upsert-new", SMALL_UPSERT_ROWS)?;
        }
        "fenced-adopt-all-new" => {
            rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows)?;
            rfc023_limits::derive_strict_chunk_plan(args.dims, "adopt-new", args.rows)?;
            if args.child {
                let phase = args
                    .phase
                    .as_deref()
                    .ok_or_else(|| "phased adopt child is missing --phase".to_string())?;
                if !matches!(phase, "setup" | "operation" | "verify") {
                    return Err(format!("unknown phased adopt child phase '{phase}'"));
                }
                if args.fixture_root.as_deref().is_none_or(str::is_empty) {
                    return Err("phased adopt child is missing --fixture-root".to_string());
                }
            } else if args.phase.is_some() || args.fixture_root.is_some() {
                return Err(
                    "--phase/--fixture-root are internal to phased adopt children".to_string(),
                );
            }
        }
        _ => {
            if args.phase.is_some() || args.fixture_root.is_some() {
                return Err("--phase/--fixture-root require fenced-adopt-all-new".to_string());
            }
        }
    }
    Ok(())
}

fn keyed_vector_schema(dims: usize) -> SchemaRef {
    assert!(dims > 0, "--dims must be greater than zero");
    assert!(
        i32::try_from(dims).is_ok(),
        "--dims exceeds Arrow's i32 width"
    );
    let item: FieldRef = Arc::new(Field::new("item", DataType::Float32, true));
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false).with_metadata(
            [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
                .into_iter()
                .collect(),
        ),
        Field::new("value", DataType::Int32, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(item, dims as i32),
            false,
        ),
    ]))
}

fn write_params(mode: WriteMode) -> WriteParams {
    WriteParams {
        mode,
        enable_stable_row_ids: true,
        data_storage_version: Some(LanceFileVersion::V2_2),
        auto_cleanup: None,
        skip_auto_cleanup: true,
        ..Default::default()
    }
}

fn batch_from_rows(
    schema: SchemaRef,
    dims: usize,
    rows: impl IntoIterator<Item = (String, i32, u64)>,
) -> Result<RecordBatch, ArrowError> {
    let mut ids = Vec::new();
    let mut values = Vec::new();
    let mut vectors = Vec::new();
    for (id, value, vector_seed) in rows {
        vectors.extend_from_slice(&seeded_vector(vector_seed, &id, dims, 0.0));
        ids.push(id);
        values.push(value);
    }

    let item = match schema.field(2).data_type() {
        DataType::FixedSizeList(item, _) => item.clone(),
        other => panic!("embedding field has unexpected type {other:?}"),
    };
    let embeddings = FixedSizeListArray::new(
        item,
        dims as i32,
        Arc::new(Float32Array::from(vectors)),
        None,
    );
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(Int32Array::from(values)),
            Arc::new(embeddings),
        ],
    )
}

/// Lazily generates embedding-bearing batches. In particular, the all-new
/// delta is never collected into one Vec<RecordBatch> before Lance consumes
/// it, so any delta-width peak comes from the route being measured.
struct VectorBatchIter {
    schema: SchemaRef,
    dims: usize,
    prefix: &'static str,
    seed: u64,
    cursor: usize,
    end: usize,
    batch_rows: usize,
}

impl VectorBatchIter {
    fn new(
        schema: SchemaRef,
        dims: usize,
        prefix: &'static str,
        seed: u64,
        rows: usize,
        batch_rows: usize,
    ) -> Self {
        assert!(batch_rows > 0, "RFC-023 benchmark batch size is zero");
        assert!(
            batch_rows <= rfc023_limits::KEYED_WRITE_MAX_ROWS,
            "RFC-023 benchmark batch exceeds the production row cap"
        );
        Self {
            schema,
            dims,
            prefix,
            seed,
            cursor: 0,
            end: rows,
            batch_rows,
        }
    }
}

impl Iterator for VectorBatchIter {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor == self.end {
            return None;
        }
        let start = self.cursor;
        let end = start.saturating_add(self.batch_rows).min(self.end);
        self.cursor = end;
        let prefix = self.prefix;
        let seed = self.seed;
        let batch = batch_from_rows(
            self.schema.clone(),
            self.dims,
            (start..end).map(move |row| {
                let id = format!("{prefix}-{row:010}");
                let value = ((row as u64 ^ seed) & i32::MAX as u64) as i32;
                (id, value, seed)
            }),
        );
        if let Ok(batch) = &batch {
            let actual_bytes = batch.get_array_memory_size() as u64;
            assert!(
                actual_bytes <= rfc023_limits::KEYED_WRITE_MAX_BYTES,
                "derived RFC-023 batch used {actual_bytes} bytes, above the production {}-byte cap",
                rfc023_limits::KEYED_WRITE_MAX_BYTES
            );
        }
        Some(batch)
    }
}

async fn seed_dataset(
    uri: &str,
    args: &Args,
    schema: SchemaRef,
    batch_rows: usize,
) -> (Dataset, u64) {
    assert!(args.rows > 0, "--rows must be greater than zero");
    let seed_start = Instant::now();
    let reader = RecordBatchIterator::new(
        VectorBatchIter::new(
            schema.clone(),
            args.dims,
            "base",
            args.seed,
            args.rows,
            batch_rows,
        ),
        schema,
    );
    let dataset = Dataset::write(reader, uri, Some(write_params(WriteMode::Create)))
        .await
        .expect("seed RFC-023 benchmark dataset");
    (dataset, seed_start.elapsed().as_millis() as u64)
}

/// Small mixed insert/update workload used by RFC-023 §11.4. `--baseline`
/// keeps MergeInsertBuilder's default index-enabled routing; the normal run
/// forces v2 (`use_index(false)`) so exact-PK writes carry Lance's key filter.
pub(super) async fn fenced_small_upsert(args: &Args) -> serde_json::Value {
    let seed_plan = rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows)
        .expect("validated RFC-023 seed chunk plan");
    let source_plan =
        rfc023_limits::require_single_batch(args.dims, "upsert-new", SMALL_UPSERT_ROWS)
            .expect("validated RFC-023 small-upsert chunk plan");
    let schema = keyed_vector_schema(args.dims);
    let dir = tempfile::tempdir().expect("tempdir");
    let uri = dir.path().join("fenced-small-upsert.lance");
    let (mut dataset, seed_ms) = seed_dataset(
        uri.to_str().unwrap(),
        args,
        schema.clone(),
        seed_plan.batch_rows,
    )
    .await;

    let index_start = Instant::now();
    dataset
        .create_index_builder(&["id"], IndexType::BTree, &ScalarIndexParams::default())
        .replace(true)
        .await
        .expect("build id BTREE");
    let index_ms = index_start.elapsed().as_millis() as u64;

    // Half updates and half inserts when the target is large enough. Keeping
    // the source at a fixed 32 rows exposes whether v2 work grows with target
    // size rather than with the requested change.
    let existing_rows = (SMALL_UPSERT_ROWS / 2).min(args.rows);
    let inserted_rows = SMALL_UPSERT_ROWS - existing_rows;
    let update_seed = args.seed ^ 0x0230_0001;
    let source_batch = batch_from_rows(
        schema.clone(),
        args.dims,
        (0..existing_rows)
            .map(|row| {
                (
                    format!("base-{row:010}"),
                    (10_000 + row) as i32,
                    update_seed,
                )
            })
            .chain((0..inserted_rows).map(|row| {
                (
                    format!("upsert-new-{row:010}"),
                    (20_000 + row) as i32,
                    update_seed,
                )
            })),
    )
    .expect("build small upsert batch");
    let source_batch_bytes = source_batch.get_array_memory_size() as u64;
    assert!(source_batch_bytes <= rfc023_limits::KEYED_WRITE_MAX_BYTES);
    let source = RecordBatchIterator::new(vec![Ok(source_batch)], schema);

    let dataset = Arc::new(dataset);
    let mut builder = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .expect("create merge-insert builder");
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0);
    let routing = if args.baseline {
        "default-index-enabled"
    } else {
        builder.use_index(false);
        "forced-v2-key-filter"
    };

    let operation_start = Instant::now();
    let stage_start = Instant::now();
    let staged = builder
        .try_build()
        .expect("build merge-insert job")
        .execute_uncommitted(source)
        .await
        .expect("stage small upsert");
    let stage_ms = stage_start.elapsed().as_millis() as u64;
    let UncommittedMergeInsert {
        transaction,
        affected_rows,
        stats,
        inserted_rows_filter,
    } = staged;
    let filter_present = inserted_rows_filter.is_some();
    let filter_field_ids = inserted_rows_filter.map(|filter| filter.field_ids);

    let commit_start = Instant::now();
    let mut commit = CommitBuilder::new(dataset)
        .with_skip_auto_cleanup(true)
        .with_max_retries(0);
    if let Some(affected_rows) = affected_rows {
        commit = commit.with_affected_rows(affected_rows);
    }
    let committed = commit
        .execute(transaction)
        .await
        .expect("commit small upsert");
    let commit_ms = commit_start.elapsed().as_millis() as u64;
    let operation_wall_ms = operation_start.elapsed().as_millis() as u64;
    let final_rows = committed.count_rows(None).await.expect("count final rows");

    serde_json::json!({
        "rows": args.rows,
        "dims": args.dims,
        "source_rows": SMALL_UPSERT_ROWS,
        "source_batch_rows": source_plan.batch_rows,
        "source_batch_bytes": source_batch_bytes,
        "source_batch_bytes_estimate": source_plan.estimated_full_batch_bytes,
        "keyed_row_limit": rfc023_limits::KEYED_WRITE_MAX_ROWS,
        "keyed_byte_limit": rfc023_limits::KEYED_WRITE_MAX_BYTES,
        "recovery_transaction_limit": rfc023_limits::RECOVERY_MAX_TRANSACTIONS,
        "planned_fenced_transactions": source_plan.transaction_count,
        "routing": routing,
        "baseline": args.baseline,
        "seed_ms": seed_ms,
        "index_ms": index_ms,
        "stage_ms": stage_ms,
        "commit_ms": commit_ms,
        "operation_wall_ms": operation_wall_ms,
        "inserted_rows_filter": filter_present,
        "filter_field_ids": filter_field_ids,
        "num_inserted_rows": stats.num_inserted_rows,
        "num_updated_rows": stats.num_updated_rows,
        "bytes_written": stats.bytes_written,
        "files_written": stats.num_files_written,
        "final_rows": final_rows,
        "raw_source_bytes": SMALL_UPSERT_ROWS as u64 * args.dims as u64 * 4,
    })
}

fn graph_schema(dims: usize) -> String {
    format!("node Chunk {{\n  slug: String @key\n  embedding: Vector({dims})\n}}\n")
}

fn vector_json_patterns(dims: usize, seed: u64) -> Vec<String> {
    (0..16)
        .map(|pattern| {
            let id = format!("benchmark-vector-pattern-{pattern}");
            let vector = seeded_vector(seed ^ pattern as u64, &id, dims, 0.0);
            let mut json = String::with_capacity(dims.saturating_mul(12));
            for (index, value) in vector.iter().enumerate() {
                if index > 0 {
                    json.push(',');
                }
                write!(&mut json, "{value}").expect("write vector JSON");
            }
            json
        })
        .collect()
}

fn graph_jsonl_chunk(prefix: &str, start: usize, end: usize, vector_patterns: &[String]) -> String {
    let approximate_row_bytes = vector_patterns
        .first()
        .map_or(128, |vector| vector.len().saturating_add(128));
    let mut jsonl = String::with_capacity((end - start).saturating_mul(approximate_row_bytes));
    for row in start..end {
        let vector = &vector_patterns[row % vector_patterns.len()];
        writeln!(
            &mut jsonl,
            "{{\"type\":\"Chunk\",\"data\":{{\"slug\":\"{prefix}-{row:010}\",\"embedding\":[{vector}]}}}}"
        )
        .expect("write graph benchmark JSONL");
    }
    jsonl
}

async fn load_graph_rows(
    db: &Omnigraph,
    branch: &str,
    prefix: &str,
    rows: usize,
    batch_rows: usize,
    vector_patterns: &[String],
    first_mode: LoadMode,
) -> (u64, u64) {
    let start = Instant::now();
    let mut cursor = 0;
    let mut max_json_chunk_bytes = 0_u64;
    while cursor < rows {
        let end = cursor.saturating_add(batch_rows).min(rows);
        let jsonl = graph_jsonl_chunk(prefix, cursor, end, vector_patterns);
        max_json_chunk_bytes = max_json_chunk_bytes.max(jsonl.len() as u64);
        let mode = if cursor == 0 {
            first_mode
        } else {
            LoadMode::Append
        };
        let result = db
            .load(branch, &jsonl, mode)
            .await
            .unwrap_or_else(|error| panic!("load {prefix} rows {cursor}..{end}: {error}"));
        let loaded_rows = result
            .nodes_loaded
            .values()
            .chain(result.edges_loaded.values())
            .copied()
            .sum::<usize>();
        assert_eq!(loaded_rows, end - cursor, "loader row-count drift");
        cursor = end;
    }
    (start.elapsed().as_millis() as u64, max_json_chunk_bytes)
}

fn only_node_table_uri(root: &std::path::Path) -> String {
    let nodes = root.join("nodes");
    let mut table_paths = std::fs::read_dir(&nodes)
        .unwrap_or_else(|error| panic!("read benchmark node-table directory {nodes:?}: {error}"))
        .map(|entry| entry.expect("read benchmark node-table entry"))
        .filter(|entry| {
            entry
                .file_type()
                .expect("read benchmark node-table entry type")
                .is_dir()
        })
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    assert_eq!(
        table_paths.len(),
        1,
        "single-table RFC-023 benchmark created an unexpected physical layout: {table_paths:?}"
    );
    table_paths
        .pop()
        .expect("one node table")
        .to_str()
        .expect("UTF-8 benchmark table URI")
        .to_string()
}

fn adopt_fixture_root(args: &Args) -> &std::path::Path {
    std::path::Path::new(
        args.fixture_root
            .as_deref()
            .expect("validated phased-adopt fixture root"),
    )
}

fn adopt_source_plan(args: &Args) -> rfc023_limits::ChunkPlan {
    rfc023_limits::derive_strict_chunk_plan(args.dims, "adopt-new", args.rows)
        .expect("validated RFC-023 source chunk plan")
}

/// Phase 1: build, persist, and validate the exact same real OmniGraph history
/// for either comparator arm. This child exits before the measured child is
/// spawned, so none of its allocations can raise the operation HWM.
pub(super) async fn fenced_adopt_setup(args: &Args) -> serde_json::Value {
    let target_plan = rfc023_limits::derive_chunk_plan(args.dims, "base", args.rows)
        .expect("validated RFC-023 target chunk plan");
    let source_plan = adopt_source_plan(args);
    let root = adopt_fixture_root(args);
    assert!(
        std::fs::read_dir(root)
            .expect("read empty phased-adopt fixture root")
            .next()
            .is_none(),
        "phased-adopt setup root must start empty"
    );
    let uri = root.to_str().expect("UTF-8 benchmark fixture root");

    let init_start = Instant::now();
    let db = Omnigraph::init(uri, &graph_schema(args.dims))
        .await
        .expect("initialize production RFC-023 benchmark graph");
    let init_ms = init_start.elapsed().as_millis() as u64;

    let target_vectors = vector_json_patterns(args.dims, args.seed);
    let (target_load_ms, max_target_json_chunk_bytes) = load_graph_rows(
        &db,
        "main",
        "base",
        args.rows,
        target_plan.batch_rows,
        &target_vectors,
        LoadMode::Overwrite,
    )
    .await;

    let branch_start = Instant::now();
    db.branch_create("adopt-source")
        .await
        .expect("create production benchmark source branch");
    let branch_create_ms = branch_start.elapsed().as_millis() as u64;

    let source_vectors = vector_json_patterns(args.dims, args.seed ^ 0x0230_0002);
    let (source_load_ms, max_source_json_chunk_bytes) = load_graph_rows(
        &db,
        "adopt-source",
        "adopt-new",
        args.rows,
        source_plan.batch_rows,
        &source_vectors,
        LoadMode::Append,
    )
    .await;
    let table_uri = only_node_table_uri(root);

    let verify_start = Instant::now();
    let main_snapshot = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .expect("capture prepared main setup snapshot");
    let main_table = main_snapshot
        .open("node:Chunk")
        .await
        .expect("open prepared main setup table");
    let setup_main_rows = main_table
        .count_rows(None)
        .await
        .expect("count prepared main setup rows");
    assert_eq!(setup_main_rows, args.rows, "prepared main row-count drift");
    let setup_main_table_version = main_table.version();

    let source_snapshot = db
        .snapshot_of(ReadTarget::branch("adopt-source"))
        .await
        .expect("capture prepared source setup snapshot");
    let source_table = source_snapshot
        .open("node:Chunk")
        .await
        .expect("open prepared source setup table");
    let setup_source_rows = source_table
        .count_rows(None)
        .await
        .expect("count prepared source setup rows");
    assert_eq!(
        setup_source_rows,
        args.rows * 2,
        "prepared source branch must contain inherited main plus the all-new delta"
    );
    let setup_source_table_version = source_table.version();
    let physical_main = DatasetBuilder::from_uri(&table_uri)
        .load()
        .await
        .expect("open prepared physical main table");
    assert_eq!(
        physical_main
            .count_rows(None)
            .await
            .expect("count prepared physical main rows"),
        args.rows
    );
    let physical_source = DatasetBuilder::from_uri(&table_uri)
        .with_branch("adopt-source", None)
        .load()
        .await
        .expect("open prepared physical source table");
    assert_eq!(
        physical_source
            .count_rows(None)
            .await
            .expect("count prepared physical source rows"),
        args.rows * 2
    );
    let setup_verify_ms = verify_start.elapsed().as_millis() as u64;
    let setup_fingerprint = format!(
        "phased-omnigraph-adopt-v2:rows={}:dims={}:seed={}:main-v{}-rows{}:source-v{}-rows{}",
        args.rows,
        args.dims,
        args.seed,
        setup_main_table_version,
        setup_main_rows,
        setup_source_table_version,
        setup_source_rows,
    );

    serde_json::json!({
        "rows": args.rows,
        "dims": args.dims,
        "source_rows": args.rows,
        "source_batch_rows": source_plan.batch_rows,
        "source_batch_bytes": null,
        "source_batch_bytes_estimate": source_plan.estimated_full_batch_bytes,
        "max_source_json_chunk_bytes": max_source_json_chunk_bytes,
        "max_target_json_chunk_bytes": max_target_json_chunk_bytes,
        "keyed_row_limit": rfc023_limits::KEYED_WRITE_MAX_ROWS,
        "keyed_byte_limit": rfc023_limits::KEYED_WRITE_MAX_BYTES,
        "recovery_transaction_limit": rfc023_limits::RECOVERY_MAX_TRANSACTIONS,
        "planned_fenced_transactions": source_plan.transaction_count,
        "setup_fingerprint": setup_fingerprint,
        "setup_main_rows": setup_main_rows,
        "setup_source_rows": setup_source_rows,
        "setup_main_table_version": setup_main_table_version,
        "setup_source_table_version": setup_source_table_version,
        "init_ms": init_ms,
        "target_load_ms": target_load_ms,
        "branch_create_ms": branch_create_ms,
        "source_load_ms": source_load_ms,
        "setup_verify_ms": setup_verify_ms,
        "seed_ms": init_ms
            + target_load_ms
            + branch_create_ms
            + source_load_ms
            + setup_verify_ms,
        "index_ms": null,
        "stage_ms": null,
        "commit_ms": null,
        "raw_source_bytes": args.rows as u64 * args.dims as u64 * 4,
    })
}

/// Phase 2 baseline: fair operation-only comparator for production branch
/// adopt. The caller has already identically opened the fixture with
/// `Omnigraph::open` and recorded the pre-operation HWM.
///
/// The measured operation intentionally bypasses OmniGraph: it streams only
/// the all-new rows from the prepared source branch into the prepared main
/// physical dataset with Lance `WriteMode::Append`. This is not graph-visible
/// publication and cannot satisfy the production path gate by itself. It does
/// no post-operation scan; final state belongs exclusively to phase 3. Raw
/// Lance cannot access the private Session held by the identically opened
/// OmniGraph handle, so this lower-level comparator opens one extra Session and
/// explicitly shares it between its main/source Dataset handles.
async fn direct_lance_append_baseline(
    args: &Args,
    table_uri: &str,
    source_plan: &rfc023_limits::ChunkPlan,
    operation_open_ms: u64,
    operation_pre_peak_rss_bytes: u64,
) -> serde_json::Value {
    let probes = MergeWriteProbes::default();
    let append_params = write_params(WriteMode::Append);
    let operation_start = Instant::now();
    let _appended = with_merge_write_probes(probes.clone(), async {
        let main_table = DatasetBuilder::from_uri(table_uri)
            .load()
            .await
            .expect("open prepared physical main dataset");
        let source_table = DatasetBuilder::from_uri(table_uri)
            .with_session(main_table.session())
            .with_branch("adopt-source", None)
            .load()
            .await
            .expect("open prepared physical source branch");
        let mut source_scanner = source_table.scan();
        source_scanner
            .filter("id LIKE 'adopt-new-%'")
            .expect("filter prepared all-new source rows");
        source_scanner.batch_size(source_plan.batch_rows);
        source_scanner.batch_size_bytes(rfc023_limits::KEYED_WRITE_MAX_BYTES);
        let source: datafusion::physical_plan::SendableRecordBatchStream = source_scanner
            .try_into_stream()
            .await
            .expect("stream prepared all-new source rows")
            .into();

        InsertBuilder::new(Arc::new(main_table))
            .with_params(&append_params)
            .execute_stream(source)
            .await
            .expect("stream direct Lance Append into prepared main dataset")
    })
    .await;
    let operation_elapsed = operation_start.elapsed();
    let operation_wall_ms = operation_elapsed.as_millis() as u64;
    let operation_wall_us = u64::try_from(operation_elapsed.as_micros()).unwrap_or(u64::MAX);
    let operation_post_peak_rss_bytes = super::current_process_peak_rss_bytes();
    let operation_hwm_increase_bytes = operation_post_peak_rss_bytes
        .checked_sub(operation_pre_peak_rss_bytes)
        .filter(|increase| *increase > 0);

    assert_eq!(
        probes.stage_merge_insert_calls(),
        0,
        "direct Lance comparator unexpectedly used the keyed adapter"
    );
    assert_eq!(
        probes.stage_fenced_insert_calls(),
        0,
        "direct Lance comparator unexpectedly used the proven fenced-insert adapter"
    );
    assert_eq!(
        probes.stage_append_calls(),
        0,
        "direct Lance comparator unexpectedly used OmniGraph's staged Append adapter"
    );
    assert_eq!(
        probes.scan_staged_combined_calls(),
        0,
        "direct Lance comparator materialized OmniGraph staged state"
    );
    assert_eq!(
        probes.ordered_cursor_scan_calls(),
        0,
        "direct Lance comparator unexpectedly entered OmniGraph's ordered merge diff"
    );

    serde_json::json!({
        "routing": "direct-lance-append-substrate-baseline",
        "measurement_boundary": "operation_wall_ms starts after the common fresh Omnigraph::open and covers Lance InsertBuilder::execute_stream (WriteMode::Append; non-production comparator); no post-op scan",
        "comparator_session_model": "one extra raw Lance Session shared by physical main/source handles; private OmniGraph Session is inaccessible",
        "production_path": false,
        "baseline": true,
        "operation_open_ms": operation_open_ms,
        "operation_wall_ms": operation_wall_ms,
        "operation_wall_us": operation_wall_us,
        "operation_pre_peak_rss_bytes": operation_pre_peak_rss_bytes,
        "operation_post_peak_rss_bytes": operation_post_peak_rss_bytes,
        "operation_hwm_censored": operation_hwm_increase_bytes.is_none(),
        "operation_hwm_increase_bytes": operation_hwm_increase_bytes,
        "merge_outcome": null,
        "exact_id_filter_verified_by_adapter": false,
        "probe_stage_merge_insert_calls": probes.stage_merge_insert_calls(),
        "probe_stage_merge_insert_rows": probes.stage_merge_insert_rows(),
        "probe_stage_fenced_insert_calls": probes.stage_fenced_insert_calls(),
        "probe_stage_fenced_insert_rows": probes.stage_fenced_insert_rows(),
        "probe_stage_append_calls": probes.stage_append_calls(),
        "probe_stage_append_rows": probes.stage_append_rows(),
        "probe_scan_staged_combined_calls": probes.scan_staged_combined_calls(),
        "probe_ordered_cursor_scan_calls": probes.ordered_cursor_scan_calls(),
        "probe_validation_scan_batches": probes.validation_scan_batches(),
        "probe_validation_scan_projected_bytes": probes.validation_scan_projected_bytes(),
        "probe_proven_insert_raw_batch_calls": probes.proven_insert_raw_batch_calls(),
        "probe_proven_insert_raw_batch_max_bytes": probes.proven_insert_raw_batch_max_bytes(),
        "probe_stage_vector_index_calls": probes.stage_vector_index_calls(),
        "probe_phase_us": merge_phase_metrics(&probes),
        "transaction_count": 1,
        "planned_transaction_count": 1,
        "observed_transaction_count": 1,
        "planned_fenced_transaction_count": source_plan.transaction_count,
        "num_inserted_rows": args.rows,
        "num_updated_rows": 0,
    })
}

fn merge_phase_metrics(probes: &MergeWriteProbes) -> serde_json::Value {
    serde_json::json!({
        "outer_prepare": probes.outer_prepare_us(),
        "proven_insert_history": probes.proven_insert_history_us(),
        "proven_insert_plan_scan": probes.proven_insert_plan_scan_us(),
        "candidate_validation": probes.candidate_validation_us(),
        "final_revalidation": probes.final_revalidation_us(),
        "recovery_arm": probes.recovery_arm_us(),
        "physical_publish": probes.physical_publish_us(),
        "keyed_stage_total": probes.keyed_stage_total_us(),
        "keyed_stage_max": probes.keyed_stage_max_us(),
        "keyed_commit_total": probes.keyed_commit_total_us(),
        "keyed_commit_max": probes.keyed_commit_max_us(),
        "recovery_confirm": probes.recovery_confirm_us(),
        "manifest_publish": probes.manifest_publish_us(),
        "recovery_cleanup": probes.recovery_cleanup_us(),
        "outer_restore_refresh": probes.outer_restore_refresh_us(),
    })
}

/// Phase 2: measured bulk all-new operation for RFC-023's #277 production
/// regression gate.
///
/// Both arms first perform the same fresh `Omnigraph::open` against the
/// persisted phase-1 root, then record `RUSAGE_SELF` before the selected
/// operation. The immediate post-operation HWM is captured before route
/// assertions and no final-state scan runs in this process.
pub(super) async fn fenced_adopt_operation(args: &Args) -> serde_json::Value {
    let root = adopt_fixture_root(args);
    let uri = root.to_str().expect("UTF-8 benchmark fixture root");
    let open_start = Instant::now();
    let db = Omnigraph::open(uri)
        .await
        .expect("fresh-open phased RFC-023 benchmark fixture");
    let operation_open_ms = open_start.elapsed().as_millis() as u64;
    let table_uri = only_node_table_uri(root);
    let source_plan = adopt_source_plan(args);
    let operation_pre_peak_rss_bytes = super::current_process_peak_rss_bytes();

    if args.baseline {
        // Keep `db` alive through the operation so both arms retain the same
        // freshly opened OmniGraph handle and caches in their measured child.
        let metrics = direct_lance_append_baseline(
            args,
            &table_uri,
            &source_plan,
            operation_open_ms,
            operation_pre_peak_rss_bytes,
        )
        .await;
        drop(db);
        return metrics;
    }

    let probes = MergeWriteProbes::default();
    let operation_start = Instant::now();
    let outcome = with_merge_write_probes(probes.clone(), db.branch_merge("adopt-source", "main"))
        .await
        .expect("run production branch merge");
    let operation_elapsed = operation_start.elapsed();
    let operation_wall_ms = operation_elapsed.as_millis() as u64;
    let operation_wall_us = u64::try_from(operation_elapsed.as_micros()).unwrap_or(u64::MAX);
    let operation_post_peak_rss_bytes = super::current_process_peak_rss_bytes();
    let operation_hwm_increase_bytes = operation_post_peak_rss_bytes
        .checked_sub(operation_pre_peak_rss_bytes)
        .filter(|increase| *increase > 0);
    assert_eq!(
        outcome,
        MergeOutcome::FastForward,
        "all-new unchanged-target workload must take the adopt path"
    );

    let fenced_calls = probes.stage_fenced_insert_calls();
    let fenced_rows = probes.stage_fenced_insert_rows();
    assert_eq!(
        fenced_calls, source_plan.transaction_count as u64,
        "production adopt did not preserve the predeclared row/byte-bounded filtered transaction plan"
    );
    assert_eq!(
        fenced_rows, args.rows as u64,
        "production adopt did not fence every all-new row"
    );
    assert_eq!(
        probes.stage_merge_insert_calls(),
        0,
        "proven production adopt unexpectedly ran Lance's target merge join"
    );
    assert_eq!(
        probes.stage_append_calls(),
        0,
        "production graph merge selected bare Append"
    );
    assert_eq!(
        probes.stage_append_rows(),
        0,
        "production graph merge recorded bare-Append rows"
    );
    assert_eq!(
        probes.scan_staged_combined_calls(),
        0,
        "production graph merge materialized the whole delta"
    );
    let ordered_cursor_scan_calls = probes.ordered_cursor_scan_calls();
    assert_eq!(
        ordered_cursor_scan_calls, 0,
        "production all-new workload missed the proven source-interval route and entered the general ordered diff"
    );
    let strict_insert_preflight_calls = probes.strict_insert_preflight_calls();
    assert_eq!(
        strict_insert_preflight_calls, 0,
        "durably proven source absence must eliminate every target key preflight"
    );

    serde_json::json!({
        "routing": "production-omnigraph-branch-merge",
        "measurement_boundary": "operation_wall_ms starts after the common fresh Omnigraph::open and covers Omnigraph::branch_merge; no post-op scan",
        "production_path": true,
        "baseline": false,
        "operation_open_ms": operation_open_ms,
        "operation_wall_ms": operation_wall_ms,
        "operation_wall_us": operation_wall_us,
        "operation_pre_peak_rss_bytes": operation_pre_peak_rss_bytes,
        "operation_post_peak_rss_bytes": operation_post_peak_rss_bytes,
        "operation_hwm_censored": operation_hwm_increase_bytes.is_none(),
        "operation_hwm_increase_bytes": operation_hwm_increase_bytes,
        "merge_outcome": "fast_forward",
        "exact_id_filter_verified_by_adapter": true,
        "probe_stage_merge_insert_calls": probes.stage_merge_insert_calls(),
        "probe_stage_merge_insert_rows": probes.stage_merge_insert_rows(),
        "probe_stage_fenced_insert_calls": fenced_calls,
        "probe_stage_fenced_insert_rows": fenced_rows,
        "probe_stage_append_calls": probes.stage_append_calls(),
        "probe_stage_append_rows": probes.stage_append_rows(),
        "probe_scan_staged_combined_calls": probes.scan_staged_combined_calls(),
        "probe_ordered_cursor_scan_calls": ordered_cursor_scan_calls,
        "probe_strict_insert_preflight_calls": strict_insert_preflight_calls,
        "probe_validation_scan_batches": probes.validation_scan_batches(),
        "probe_validation_scan_projected_bytes": probes.validation_scan_projected_bytes(),
        "probe_proven_insert_raw_batch_calls": probes.proven_insert_raw_batch_calls(),
        "probe_proven_insert_raw_batch_max_bytes": probes.proven_insert_raw_batch_max_bytes(),
        "probe_stage_vector_index_calls": probes.stage_vector_index_calls(),
        "probe_phase_us": merge_phase_metrics(&probes),
        "source_scan_batch_plan": source_plan.transaction_count,
        "transaction_count": fenced_calls,
        "planned_transaction_count": source_plan.transaction_count,
        "observed_transaction_count": fenced_calls,
        "planned_fenced_transaction_count": source_plan.transaction_count,
        "num_inserted_rows": fenced_rows,
        "num_updated_rows": 0,
    })
}

fn fixture_vector_component(state: &mut u64) -> f32 {
    ((super::xorshift64(state) >> 11) as f32 / (1_u64 << 53) as f32) * 2.0 - 1.0
}

fn verify_fixture_vector(
    observed: &Float32Array,
    offset: usize,
    dims: usize,
    domain_seed: u64,
    ordinal: usize,
    id: &str,
) {
    let pattern = ordinal % 16;
    let pattern_id = format!("benchmark-vector-pattern-{pattern}");
    let effective_seed = domain_seed ^ pattern as u64;
    let mut norm_state = effective_seed ^ super::fnv1a64(&pattern_id);
    if norm_state == 0 {
        norm_state = 0x9e37_79b9_7f4a_7c15;
    }
    let initial_state = norm_state;
    let mut norm_squared = 0_f64;
    for _ in 0..dims {
        let value = fixture_vector_component(&mut norm_state);
        norm_squared += (value as f64) * (value as f64);
    }
    let norm = norm_squared.sqrt() as f32;
    let mut value_state = initial_state;
    for dimension in 0..dims {
        let mut expected = fixture_vector_component(&mut value_state);
        if norm > f32::EPSILON {
            expected /= norm;
        }
        let actual = observed.value(offset + dimension);
        assert_eq!(
            actual.to_bits(),
            expected.to_bits(),
            "verified embedding differs at {id}[{dimension}]"
        );
    }
}

/// Exact, bounded verification of the benchmark's complete row domain.
///
/// The scan projects only the fixture's `id`, `slug`, and `embedding` columns
/// and carries the same row/byte batch limits as production. A bit per possible
/// `(prefix, ordinal)` proves every expected ID appears exactly once and rejects
/// duplicates, omissions, malformed IDs, and foreign prefixes. Each slug and
/// every vector component is checked against the deterministic fixture
/// generator while its batch is live. The retained allocation is bounded by
/// the RFC-023 recovery ceiling: at most two prefixes × 1,024 transactions ×
/// 8,192 rows = 2 MiB. The canonical SHA-256 names the fully checked row
/// contract without retaining payloads.
enum VerificationTable<'a> {
    Physical(&'a Dataset),
    Snapshot(&'a SnapshotTable),
}

async fn verify_id_content(
    table: VerificationTable<'_>,
    rows_per_prefix: usize,
    expected_prefixes: &[(&str, u64)],
    dims: usize,
    scan_batch_rows: usize,
    scan_batch_bytes_target: u64,
) -> serde_json::Value {
    assert!(!expected_prefixes.is_empty(), "expected ID domain is empty");
    assert!(
        expected_prefixes.len() <= 2,
        "RFC-023 verifier supports at most the base/adopt domains"
    );
    let max_rows_per_prefix = rfc023_limits::KEYED_WRITE_MAX_ROWS
        .checked_mul(rfc023_limits::RECOVERY_MAX_TRANSACTIONS)
        .expect("RFC-023 verification row ceiling overflow");
    assert!(
        rows_per_prefix <= max_rows_per_prefix,
        "verification domain exceeds the recovery-bounded row ceiling"
    );
    let expected_slots = rows_per_prefix
        .checked_mul(expected_prefixes.len())
        .expect("RFC-023 verification domain overflow");
    let bitset_words = expected_slots.div_ceil(u64::BITS as usize);
    let bitset_bytes = bitset_words
        .checked_mul(std::mem::size_of::<u64>())
        .expect("RFC-023 verification bitset overflow");
    let max_bitset_bytes = max_rows_per_prefix
        .checked_mul(2)
        .and_then(|slots| slots.checked_add(7))
        .map(|bits| bits / 8)
        .expect("RFC-023 maximum verification bitset overflow");
    assert!(bitset_bytes <= max_bitset_bytes);
    assert!(scan_batch_bytes_target > 0);
    assert!(scan_batch_bytes_target <= rfc023_limits::KEYED_WRITE_MAX_BYTES);

    let markers = expected_prefixes
        .iter()
        .map(|(prefix, _)| format!("{prefix}-"))
        .collect::<Vec<_>>();
    let mut seen = vec![0_u64; bitset_words];
    let mut prefix_counts = vec![0_usize; expected_prefixes.len()];
    let mut observed_rows = 0_usize;

    let mut stream = match table {
        VerificationTable::Physical(dataset) => {
            let mut scanner = dataset.scan();
            scanner
                .project(&["id", "slug", "embedding"])
                .expect("project exact physical-row verification scan");
            scanner.batch_size(scan_batch_rows);
            scanner.batch_size_bytes(scan_batch_bytes_target);
            // Lance's byte target overrides its row target. Strict final
            // batching restores the verifier's hard output-row ceiling.
            scanner.strict_batch_size(true);
            scanner
                .try_into_stream()
                .await
                .expect("stream exact physical-row verification scan")
        }
        VerificationTable::Snapshot(table) => {
            let mut scanner = table.scan();
            scanner
                .project(&["id", "slug", "embedding"])
                .expect("project exact snapshot-row verification scan");
            scanner.batch_size(scan_batch_rows);
            scanner.batch_size_bytes(scan_batch_bytes_target);
            // Keep graph-visible verification on the manifest-pinned handle;
            // strict batching is forwarded without exposing raw Lance state.
            scanner.strict_batch_size(true);
            scanner
                .try_into_stream()
                .await
                .expect("stream exact snapshot-row verification scan")
        }
    };
    while let Some(batch) = stream
        .try_next()
        .await
        .expect("read exact-row verification batch")
    {
        let batch = rfc023_limits::compact_oversized_verification_slice(batch)
            .unwrap_or_else(|error| panic!("bound exact-row verification batch: {error}"));
        assert!(
            batch.num_rows() <= scan_batch_rows,
            "verification scan exceeded its row batch bound"
        );
        assert!(
            batch.get_array_memory_size() as u64 <= rfc023_limits::KEYED_WRITE_MAX_BYTES,
            "verification scan exceeded its byte batch bound"
        );
        assert_eq!(
            batch.num_columns(),
            3,
            "verification scan lost exact-row projection"
        );
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("verification ID column must be Utf8");
        assert_eq!(ids.null_count(), 0, "verification ID column contains nulls");
        let slugs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("verification slug column must be Utf8");
        assert_eq!(
            slugs.null_count(),
            0,
            "verification slug column contains nulls"
        );
        let embeddings = batch
            .column(2)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("verification embedding column must be FixedSizeList");
        assert_eq!(
            embeddings.null_count(),
            0,
            "verification embedding column contains nulls"
        );
        assert_eq!(
            embeddings.value_length() as usize,
            dims,
            "verification embedding width drifted"
        );
        let embedding_values = embeddings
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("verification embedding values must be Float32");
        assert_eq!(
            embedding_values.null_count(),
            0,
            "verification embedding values contain nulls"
        );
        for (row_index, id) in ids.iter().flatten().enumerate() {
            assert_eq!(
                slugs.value(row_index),
                id,
                "verified row's public key differs from its exact id"
            );
            let (prefix_index, suffix) = markers
                .iter()
                .enumerate()
                .find_map(|(index, marker)| id.strip_prefix(marker).map(|rest| (index, rest)))
                .unwrap_or_else(|| panic!("unexpected ID in verified content: {id}"));
            assert_eq!(
                suffix.len(),
                10,
                "verified ID does not use the fixture's fixed-width ordinal: {id}"
            );
            assert!(
                suffix.bytes().all(|byte| byte.is_ascii_digit()),
                "verified ID contains a non-decimal ordinal: {id}"
            );
            let ordinal = suffix
                .parse::<usize>()
                .unwrap_or_else(|error| panic!("parse verified ID ordinal {id}: {error}"));
            assert!(
                ordinal < rows_per_prefix,
                "verified ID ordinal is outside the fixture domain: {id}"
            );
            let slot = prefix_index
                .checked_mul(rows_per_prefix)
                .and_then(|base| base.checked_add(ordinal))
                .expect("verified ID slot overflow");
            let word = slot / u64::BITS as usize;
            let mask = 1_u64 << (slot % u64::BITS as usize);
            assert_eq!(
                seen[word] & mask,
                0,
                "duplicate ID in verified content: {id}"
            );
            seen[word] |= mask;
            verify_fixture_vector(
                embedding_values,
                usize::try_from(embeddings.value_offset(row_index))
                    .expect("verification embedding offset must be non-negative"),
                dims,
                expected_prefixes[prefix_index].1,
                ordinal,
                id,
            );
            prefix_counts[prefix_index] += 1;
            observed_rows += 1;
        }
    }

    assert_eq!(
        observed_rows, expected_slots,
        "verified content row count does not match its exact ID domain"
    );
    for (index, count) in prefix_counts.iter().enumerate() {
        assert_eq!(
            *count, rows_per_prefix,
            "verified {} domain is incomplete",
            expected_prefixes[index].0
        );
    }
    for slot in 0..expected_slots {
        let word = slot / u64::BITS as usize;
        let mask = 1_u64 << (slot % u64::BITS as usize);
        assert_ne!(
            seen[word] & mask,
            0,
            "verified ID domain has a missing slot"
        );
    }

    let mut canonical = Sha256::new();
    canonical.update(format!("dims={dims};rows={rows_per_prefix}\n").as_bytes());
    let mut canonical_id = String::with_capacity(32);
    for (prefix, seed) in expected_prefixes {
        canonical.update(format!("prefix={prefix};seed={seed}\n").as_bytes());
        for ordinal in 0..rows_per_prefix {
            canonical_id.clear();
            write!(&mut canonical_id, "{prefix}-{ordinal:010}\n")
                .expect("format canonical verified ID");
            canonical.update(canonical_id.as_bytes());
        }
    }
    let per_prefix = expected_prefixes
        .iter()
        .zip(prefix_counts)
        .map(|((prefix, _), count)| ((*prefix).to_string(), serde_json::json!(count)))
        .collect::<serde_json::Map<_, _>>();
    serde_json::json!({
        "rows": observed_rows,
        "rows_per_prefix": per_prefix,
        "canonical_row_contract_sha256": format!("{:x}", canonical.finalize()),
        "fingerprint_version": "sha256-canonical-exact-fixture-row-contract-v2",
        "projected_columns": ["id", "slug", "embedding"],
        "payload_values_verified": true,
        "scan_batch_rows": scan_batch_rows,
        "scan_batch_bytes_target": scan_batch_bytes_target,
        "scan_batch_bytes": rfc023_limits::KEYED_WRITE_MAX_BYTES,
        "exact_seen_bitset_bytes": bitset_bytes,
        "exact_seen_bitset_max_bytes": max_bitset_bytes,
    })
}

/// Phase 3: fresh, unmeasured verification of both physical and logical final
/// state. Keeping these scans out of phase 2 makes the arm boundary symmetric:
/// neither operation gets credited or charged for its postcondition checks.
pub(super) async fn fenced_adopt_verify(args: &Args) -> serde_json::Value {
    let root = adopt_fixture_root(args);
    let uri = root.to_str().expect("UTF-8 benchmark fixture root");
    let verify_start = Instant::now();
    let db = Omnigraph::open(uri)
        .await
        .expect("fresh-open phased RFC-023 fixture for verification");
    let table_uri = only_node_table_uri(root);

    let main_snapshot = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .expect("capture graph-visible main after measured operation");
    let manifest_main = main_snapshot
        .open("node:Chunk")
        .await
        .expect("open graph-visible main after measured operation");
    let manifest_main_version = manifest_main.version();

    let source_snapshot = db
        .snapshot_of(ReadTarget::branch("adopt-source"))
        .await
        .expect("capture graph-visible source after measured operation");
    let manifest_source = source_snapshot
        .open("node:Chunk")
        .await
        .expect("open graph-visible source after measured operation");
    let manifest_source_version = manifest_source.version();

    let physical_main = DatasetBuilder::from_uri(&table_uri)
        .load()
        .await
        .expect("open physical main after measured operation");
    let physical_source = DatasetBuilder::from_uri(&table_uri)
        .with_branch("adopt-source", None)
        .load()
        .await
        .expect("open physical source after measured operation");
    let source_plan = adopt_source_plan(args);
    let complete_domain = [("base", args.seed), ("adopt-new", args.seed ^ 0x0230_0002)];
    let base_domain = [("base", args.seed)];
    let physical_main_content = verify_id_content(
        VerificationTable::Physical(&physical_main),
        args.rows,
        &complete_domain,
        args.dims,
        source_plan.batch_rows,
        source_plan.estimated_full_batch_bytes,
    )
    .await;
    let physical_source_content = verify_id_content(
        VerificationTable::Physical(&physical_source),
        args.rows,
        &complete_domain,
        args.dims,
        source_plan.batch_rows,
        source_plan.estimated_full_batch_bytes,
    )
    .await;
    let manifest_main_content = verify_id_content(
        VerificationTable::Snapshot(&manifest_main),
        args.rows,
        if args.baseline {
            &base_domain
        } else {
            &complete_domain
        },
        args.dims,
        source_plan.batch_rows,
        source_plan.estimated_full_batch_bytes,
    )
    .await;
    let manifest_source_content = verify_id_content(
        VerificationTable::Snapshot(&manifest_source),
        args.rows,
        &complete_domain,
        args.dims,
        source_plan.batch_rows,
        source_plan.estimated_full_batch_bytes,
    )
    .await;
    let fingerprint = |content: &serde_json::Value| {
        content
            .get("canonical_row_contract_sha256")
            .and_then(serde_json::Value::as_str)
            .expect("verified content fingerprint")
            .to_string()
    };
    assert_eq!(
        fingerprint(&physical_main_content),
        fingerprint(&physical_source_content),
        "physical source content changed or physical main missed adopted IDs"
    );
    assert_eq!(
        fingerprint(&physical_source_content),
        fingerprint(&manifest_source_content),
        "graph-visible source content changed during adopt measurement"
    );
    if !args.baseline {
        assert_eq!(
            fingerprint(&manifest_main_content),
            fingerprint(&physical_main_content),
            "production main did not graph-publish its exact physical content"
        );
    }
    let physical_main_rows = physical_main_content["rows"]
        .as_u64()
        .expect("verified physical main row count");
    let physical_source_rows = physical_source_content["rows"]
        .as_u64()
        .expect("verified physical source row count");
    let manifest_visible_final_rows = manifest_main_content["rows"]
        .as_u64()
        .expect("verified graph-visible main row count");
    let manifest_source_rows = manifest_source_content["rows"]
        .as_u64()
        .expect("verified graph-visible source row count");
    let verify_wall_ms = verify_start.elapsed().as_millis() as u64;

    serde_json::json!({
        "verify_wall_ms": verify_wall_ms,
        "final_rows": physical_main_rows,
        "manifest_visible_final_rows": manifest_visible_final_rows,
        "manifest_source_final_rows": manifest_source_rows,
        "physical_source_final_rows": physical_source_rows,
        "verify_main_table_version": manifest_main_version,
        "verify_source_table_version": manifest_source_version,
        "physical_main_content": physical_main_content,
        "physical_source_content": physical_source_content,
        "manifest_main_content": manifest_main_content,
        "manifest_source_content": manifest_source_content,
    })
}
