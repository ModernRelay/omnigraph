#![cfg(feature = "failpoints")]

//! RFC-026 Phase-B1 cost evidence.
//!
//! Keep the terms separate.  A warm already-claimed acknowledgement, a cold
//! claim/replay, one selected flushed-generation scan, retained shard-manifest
//! metadata, and the shared graph-manifest publisher are different costs with
//! different scaling variables.  Combining them into one latency number would
//! hide exactly the regressions this instrument exists to expose.

mod helpers;

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use fail::FailScenario;
use lance::dataset::mem_wal::schema_with_tombstone;
use lance::dataset::mem_wal::write::StoredBatch;
use lance_core::utils::bloomfilter::sbbf::Sbbf;
use lance_io::utils::tracking_store::{IOTracker, IoStats};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph::instrumentation::{CountingStorageAdapter, StorageReadCounts};
use omnigraph::storage::storage_for_uri;
use serial_test::serial;

use helpers::cost::with_raw_io_trackers;

const STREAM_SCHEMA: &str = r#"
node Person { score: I32 }
node History { tick: I32 }
"#;
const PAYLOAD_SCHEMA: &str = "node Person { payload: String }\n";
const TABLE: &str = "node:Person";
const ADD_HISTORY: &str = r#"
query add_history($tick: I32) {
    insert History { tick: $tick }
}
"#;

const WIDEST_ROWS: usize = 8_192;
const WIDEST_PAYLOAD_BYTES: usize = 4_032;
const HARD_ARROW_BYTES: usize = 32 * 1024 * 1024;
const NO_AUTO_ROLL_BYTES: usize = 1024 * 1024 * 1024;
const NO_AUTO_ROLL_ROWS: usize = 8_193;
const NO_AUTO_ROLL_BATCHES: usize = 8_193;

const RSS_CHILD_ENV: &str = "OMNIGRAPH_MEMWAL_COST_CHILD";

#[derive(Debug, Clone, Copy, Default)]
struct PathIo {
    total_reads: u64,
    total_read_bytes: u64,
    total_writes: u64,
    total_written_bytes: u64,
    wal_reads: u64,
    wal_writes: u64,
    shard_manifest_reads: u64,
    shard_manifest_writes: u64,
    generation_reads: u64,
    generation_writes: u64,
    generation_data_reads: u64,
    generation_data_writes: u64,
    pk_sidecar_reads: u64,
    pk_sidecar_writes: u64,
}

impl PathIo {
    fn from_stats(stats: &IoStats) -> Self {
        let mut out = Self {
            total_reads: stats.read_iops,
            total_read_bytes: stats.read_bytes,
            total_writes: stats.write_iops,
            total_written_bytes: stats.written_bytes,
            ..Self::default()
        };
        for request in &stats.requests {
            let path = request.path.as_ref();
            let write = matches!(
                request.method,
                "put_opts" | "put_part" | "copy" | "rename" | "delete"
            );
            let read = !write;
            let memwal = path.contains("_mem_wal/");
            let wal = memwal && (path.contains("/wal/") || path.ends_with("/wal"));
            let shard_manifest =
                memwal && (path.contains("/manifest/") || path.ends_with("/manifest"));
            let generation = memwal && path.contains("_gen_");
            let generation_data = generation
                && (path.contains("/data/") || path.ends_with("/data") || path.ends_with(".lance"));
            let pk_sidecar = generation && path.contains("/_pk_index/");
            if read && wal {
                out.wal_reads += 1;
            }
            if write && wal {
                out.wal_writes += 1;
            }
            if read && shard_manifest {
                out.shard_manifest_reads += 1;
            }
            if write && shard_manifest {
                out.shard_manifest_writes += 1;
            }
            if read && generation {
                out.generation_reads += 1;
            }
            if write && generation {
                out.generation_writes += 1;
            }
            if read && generation_data {
                out.generation_data_reads += 1;
            }
            if write && generation_data {
                out.generation_data_writes += 1;
            }
            if read && pk_sidecar {
                out.pk_sidecar_reads += 1;
            }
            if write && pk_sidecar {
                out.pk_sidecar_writes += 1;
            }
        }
        out
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AdapterIo {
    read_text: u64,
    read_text_if_exists: u64,
    exists: u64,
    read_text_versioned: u64,
    list_dir: u64,
    write_text: u64,
    delete: u64,
}

impl AdapterIo {
    fn snapshot(counts: &StorageReadCounts) -> Self {
        Self {
            read_text: counts.read_text(),
            read_text_if_exists: counts.read_text_if_exists(),
            exists: counts.exists(),
            read_text_versioned: counts.read_text_versioned(),
            list_dir: counts.list_dir(),
            write_text: counts.write_text(),
            delete: counts.delete(),
        }
    }

    fn since(self, before: Self) -> Self {
        Self {
            read_text: self.read_text.saturating_sub(before.read_text),
            read_text_if_exists: self
                .read_text_if_exists
                .saturating_sub(before.read_text_if_exists),
            exists: self.exists.saturating_sub(before.exists),
            read_text_versioned: self
                .read_text_versioned
                .saturating_sub(before.read_text_versioned),
            list_dir: self.list_dir.saturating_sub(before.list_dir),
            write_text: self.write_text.saturating_sub(before.write_text),
            delete: self.delete.saturating_sub(before.delete),
        }
    }

    fn operations(self) -> u64 {
        self.read_text
            + self.read_text_if_exists
            + self.exists
            + self.read_text_versioned
            + self.list_dir
            + self.write_text
            + self.delete
    }
}

#[derive(Debug)]
struct HistorySample {
    depth: u64,
    ack_elapsed_us: u128,
    ack_table: PathIo,
    ack_manifest: PathIo,
    ack_adapter: AdapterIo,
    fold_elapsed_us: u128,
    fold_table: PathIo,
    fold_manifest: PathIo,
}

#[derive(Debug)]
struct ColdReplaySample {
    retained_batches: u64,
    elapsed_us: u128,
    table: PathIo,
}

#[derive(Debug)]
struct FoldRowsSample {
    rows: usize,
    elapsed_us: u128,
    table: PathIo,
    physical_generation_data_bytes: u64,
}

#[derive(Debug)]
struct RetainedMetadataSample {
    merged_generations: u64,
    serialized_manifest_bytes: u64,
    cold_elapsed_us: u128,
    table: PathIo,
}

async fn initialize_history(uri: &str, depth: u64, compact_before_enrollment: bool) {
    let db = Omnigraph::init(uri, STREAM_SCHEMA).await.unwrap();
    for tick in 0..depth {
        db.mutate(
            "main",
            ADD_HISTORY,
            "add_history",
            &helpers::int_params(&[("$tick", tick as i64)]),
        )
        .await
        .unwrap();
    }
    if compact_before_enrollment {
        db.optimize()
            .await
            .expect("normalize internal/data fragments before the flat ack measurement");
    }
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
}

async fn init_enrolled(uri: &str, schema: &str) {
    let db = Omnigraph::init(uri, schema).await.unwrap();
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
}

async fn table_schema(db: &Omnigraph) -> Arc<Schema> {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(TABLE).await.unwrap();
    Arc::new(Schema::from(table.schema()))
}

fn score_batch(schema: Arc<Schema>, id_prefix: &str, rows: usize) -> RecordBatch {
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "score"]
    );
    let ids = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("{id_prefix}-{row:05}")),
    )) as ArrayRef;
    let scores = Arc::new(Int32Array::from_iter_values(
        (0..rows).map(|row| i32::try_from(row).unwrap()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, scores]).unwrap()
}

async fn payload_batch(db: &Omnigraph, rows: usize, payload_bytes: usize) -> RecordBatch {
    let schema = table_schema(db).await;
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "payload"]
    );
    let ids = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("rss-{row:05}")),
    )) as ArrayRef;
    let payload = "x".repeat(payload_bytes);
    let values = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| payload.as_str()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, values]).unwrap()
}

async fn history_sample_at_uri(
    uri: &str,
    depth: u64,
    compact_before_enrollment: bool,
) -> HistorySample {
    initialize_history(uri, depth, compact_before_enrollment).await;

    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, adapter_counts) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
        let db = Arc::new(Omnigraph::open_with_storage(uri, adapter).await.unwrap());
        let schema = table_schema(&db).await;
        let first = score_batch(Arc::clone(&schema), &format!("d{depth}-warmup"), 1);
        let measured = score_batch(schema, &format!("d{depth}-measured"), 1);

        // This first put performs the cold claim.  The tracker is deliberately
        // persistent across it and the measured put: the warm writer carries
        // the object-store wrapper it acquired when it was opened.
        db.failpoint_stream_b1_for_test(TABLE, Some(first), 0)
            .await
            .unwrap();
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();
        let adapter_before = AdapterIo::snapshot(&adapter_counts);

        let ack_started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, Some(measured), 1)
            .await
            .unwrap();
        let ack_elapsed_us = ack_started.elapsed().as_micros();
        let ack_table = PathIo::from_stats(&table_tracker.incremental_stats());
        let ack_manifest = PathIo::from_stats(&manifest_tracker.incremental_stats());
        let ack_adapter = AdapterIo::snapshot(&adapter_counts).since(adapter_before);

        let fold_started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        let fold_elapsed_us = fold_started.elapsed().as_micros();
        let fold_table = PathIo::from_stats(&table_tracker.incremental_stats());
        let fold_manifest = PathIo::from_stats(&manifest_tracker.incremental_stats());

        HistorySample {
            depth,
            ack_elapsed_us,
            ack_table,
            ack_manifest,
            ack_adapter,
            fold_elapsed_us,
            fold_table,
            fold_manifest,
        }
    })
    .await
}

async fn local_history_sample(depth: u64, compact: bool) -> HistorySample {
    let dir = tempfile::tempdir().unwrap();
    history_sample_at_uri(dir.path().to_str().unwrap(), depth, compact).await
}

fn assert_warm_ack_evidence(samples: &[HistorySample], backend: &str) {
    assert!(samples.len() >= 2);
    for sample in samples {
        eprintln!(
            "B1 warm ack {backend} depth={}: elapsed_us={} table_reads={} table_read_bytes={} table_writes={} table_written_bytes={} wal_writes={} manifest_reads={} adapter_ops={}",
            sample.depth,
            sample.ack_elapsed_us,
            sample.ack_table.total_reads,
            sample.ack_table.total_read_bytes,
            sample.ack_table.total_writes,
            sample.ack_table.total_written_bytes,
            sample.ack_table.wal_writes,
            sample.ack_manifest.total_reads,
            sample.ack_adapter.operations(),
        );
        assert!(
            sample.ack_table.wal_writes > 0,
            "warm ack recorded no WAL write; detached-task probe propagation regressed: {sample:?}"
        );
        assert_eq!(
            sample.ack_table.generation_writes, 0,
            "ack must not flush/roll a generation"
        );
        assert_eq!(
            sample.ack_manifest.total_writes, 0,
            "ack must not publish graph visibility"
        );
    }
    let shallow = &samples[0];
    let deep = &samples[samples.len() - 1];
    assert!(
        deep.ack_table.total_reads <= shallow.ack_table.total_reads + 2,
        "warm ack table reads grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_table.total_writes <= shallow.ack_table.total_writes + 2,
        "warm ack table writes grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_manifest.total_reads <= shallow.ack_manifest.total_reads + 4,
        "warm ack graph-authority reads grew with compacted graph history: {samples:#?}"
    );
    assert!(
        deep.ack_adapter.operations() <= shallow.ack_adapter.operations() + 2,
        "warm ack schema/sidecar adapter operations grew with graph history: {samples:#?}"
    );
}

#[tokio::test]
#[serial]
async fn warm_already_claimed_ack_is_flat_in_compacted_graph_history_local() {
    let mut samples = Vec::new();
    for depth in [8, 80] {
        samples.push(local_history_sample(depth, true).await);
    }
    assert_warm_ack_evidence(&samples, "local");
}

#[tokio::test]
#[serial]
async fn warm_already_claimed_ack_is_measured_on_configured_rustfs() {
    let Some(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET").ok() else {
        eprintln!("SKIP: OMNIGRAPH_S3_TEST_BUCKET not set; RustFS B1 ack cost not measured");
        return;
    };

    let mut samples = Vec::new();
    for depth in [8, 80] {
        let uri = format!(
            "s3://{bucket}/cost-tests/memwal-b1-ack-{}-{depth}",
            ulid::Ulid::new()
        );
        let sample = history_sample_at_uri(&uri, depth, true).await;
        storage_for_uri(&uri)
            .unwrap()
            .delete_prefix(&uri)
            .await
            .expect("configured RustFS cleanup failed");
        samples.push(sample);
    }
    assert_warm_ack_evidence(&samples, "rustfs");
}

async fn cold_replay_sample(retained_batches: u64) -> ColdReplaySample {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    init_enrolled(uri, STREAM_SCHEMA).await;

    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let (adapter, _) = CountingStorageAdapter::new(storage_for_uri(uri).unwrap());
        let mut db = Arc::new(
            Omnigraph::open_with_storage(uri, Arc::clone(&adapter))
                .await
                .unwrap(),
        );
        let schema = table_schema(&db).await;
        for ordinal in 0..retained_batches {
            let batch = score_batch(
                Arc::clone(&schema),
                &format!("cold-{retained_batches}-{ordinal}"),
                1,
            );
            if ordinal + 1 == retained_batches {
                let error = {
                    let _after_durable =
                        ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
                    db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                        .await
                        .expect_err("the final durable entry retires the warm writer")
                };
                assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
            } else {
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                    .await
                    .unwrap();
            }
        }

        // Reopen the graph, then reset: the measured interval starts at the
        // stream's cold claim/replay, not at generic graph open.
        drop(db);
        db = Arc::new(Omnigraph::open_with_storage(uri, adapter).await.unwrap());
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let started = Instant::now();
        let error = {
            let _before_seal = ScopedFailPoint::new(names::STREAM_B1_BEFORE_FORCE_SEAL, "return");
            db.failpoint_stream_b1_for_test(TABLE, None, 0)
                .await
                .expect_err("stop after cold claim/replay and before generation output")
        };
        assert!(
            error
                .to_string()
                .contains(names::STREAM_B1_BEFORE_FORCE_SEAL),
            "{error:?}"
        );
        let elapsed_us = started.elapsed().as_micros();
        let table = PathIo::from_stats(&table_tracker.incremental_stats());

        // Complete the same durable tail so each fixture leaves no live worker.
        let _ = manifest_tracker.incremental_stats();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        ColdReplaySample {
            retained_batches,
            elapsed_us,
            table,
        }
    })
    .await
}

#[tokio::test]
#[serial]
async fn cold_claim_replay_is_reported_separately_by_retained_wal_depth() {
    let _scenario = FailScenario::setup();
    let mut samples = Vec::new();
    for retained_batches in [1, 8, 32] {
        let sample = cold_replay_sample(retained_batches).await;
        eprintln!(
            "B1 cold replay retained_batches={}: elapsed_us={} wal_reads={} table_reads={} aggregate_table_read_bytes={} generation_writes={}",
            sample.retained_batches,
            sample.elapsed_us,
            sample.table.wal_reads,
            sample.table.total_reads,
            sample.table.total_read_bytes,
            sample.table.generation_writes,
        );
        assert!(
            sample.table.wal_reads > 0,
            "cold claim did not observe retained WAL: {sample:?}"
        );
        assert_eq!(
            sample.table.generation_writes, 0,
            "pre-seal cold replay measurement must exclude generation output"
        );
        samples.push(sample);
    }
    assert!(
        samples.last().unwrap().table.total_read_bytes
            > samples.first().unwrap().table.total_read_bytes,
        "aggregate cold claim/replay bytes must expose retained-WAL depth: {samples:#?}"
    );
}

async fn fold_rows_sample(rows: usize) -> FoldRowsSample {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    init_enrolled(uri, STREAM_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();
    with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(uri).await.unwrap());
        let schema = table_schema(&db).await;
        let batch = score_batch(schema, &format!("fold-{rows}"), rows);
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .unwrap();
        let _ = table_tracker.incremental_stats();
        let _ = manifest_tracker.incremental_stats();

        let started = Instant::now();
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .unwrap();
        let elapsed_us = started.elapsed().as_micros();
        let table = PathIo::from_stats(&table_tracker.incremental_stats());
        assert_eq!(
            helpers::count_rows(&db, TABLE).await,
            rows,
            "the measured fold must select exactly this generation's rows"
        );
        FoldRowsSample {
            rows,
            elapsed_us,
            table,
            physical_generation_data_bytes: generation_data_file_bytes(dir.path()),
        }
    })
    .await
}

#[tokio::test]
#[serial]
async fn one_generation_fold_data_scan_tracks_selected_rows() {
    let mut samples = Vec::new();
    for rows in [1, 4_096] {
        let sample = fold_rows_sample(rows).await;
        eprintln!(
            "B1 fold selected_rows={}: elapsed_us={} generation_data_reads={} generation_reads={} observed_range_read_bytes={} physical_generation_data_bytes={} pk_sidecar_reads={}",
            sample.rows,
            sample.elapsed_us,
            sample.table.generation_data_reads,
            sample.table.generation_reads,
            sample.table.total_read_bytes,
            sample.physical_generation_data_bytes,
            sample.table.pk_sidecar_reads,
        );
        assert!(
            sample.table.generation_data_reads > 0,
            "fold recorded no selected-generation data read; detached-task probe propagation regressed: {sample:?}"
        );
        samples.push(sample);
    }
    assert!(
        samples[1].physical_generation_data_bytes > samples[0].physical_generation_data_bytes,
        "the selected generation's physical data must expose row volume: {samples:#?}"
    );
}

#[tokio::test]
#[serial]
async fn fold_keeps_known_uncompacted_graph_manifest_history_term_visible() {
    let mut samples = Vec::new();
    for depth in [8, 80] {
        let sample = local_history_sample(depth, false).await;
        eprintln!(
            "B1 fold uncompacted_history={}: elapsed_us={} graph_manifest_reads={} graph_manifest_read_bytes={} generation_data_reads={}",
            sample.depth,
            sample.fold_elapsed_us,
            sample.fold_manifest.total_reads,
            sample.fold_manifest.total_read_bytes,
            sample.fold_table.generation_data_reads,
        );
        assert!(sample.fold_table.generation_data_reads > 0, "{sample:?}");
        samples.push(sample);
    }
    assert!(
        samples[1].fold_manifest.total_reads > samples[0].fold_manifest.total_reads
            || samples[1].fold_manifest.total_read_bytes
                > samples[0].fold_manifest.total_read_bytes,
        "the shared fold authority/publisher's known uncompacted graph-manifest term disappeared; do not replace it with a false history-flat claim: {samples:#?}"
    );
}

fn max_shard_manifest_bytes(root: &Path) -> u64 {
    fn walk(path: &Path, max_bytes: &mut u64) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(&path, max_bytes);
                continue;
            }
            let is_shard_manifest = path.extension().is_some_and(|ext| ext == "binpb")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "_mem_wal")
                && path
                    .components()
                    .any(|component| component.as_os_str() == "manifest");
            if is_shard_manifest {
                *max_bytes = (*max_bytes).max(entry.metadata().unwrap().len());
            }
        }
    }

    let mut max_bytes = 0;
    walk(root, &mut max_bytes);
    assert!(
        max_bytes > 0,
        "no local shard manifest found under {root:?}"
    );
    max_bytes
}

fn generation_data_file_bytes(root: &Path) -> u64 {
    fn walk(path: &Path, total: &mut u64) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(&path, total);
                continue;
            }
            let in_generation = path
                .components()
                .any(|component| component.as_os_str().to_string_lossy().contains("_gen_"));
            let in_data = path
                .components()
                .any(|component| component.as_os_str() == "data");
            if in_generation && in_data {
                *total = total.saturating_add(entry.metadata().unwrap().len());
            }
        }
    }

    let mut total = 0;
    walk(root, &mut total);
    assert!(total > 0, "no flushed-generation data found under {root:?}");
    total
}

#[tokio::test]
#[serial]
async fn retained_merged_generation_metadata_cost_is_swept_explicitly() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap();
    init_enrolled(uri, STREAM_SCHEMA).await;
    let table_tracker = IOTracker::default();
    let manifest_tracker = IOTracker::default();

    let samples = with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
        let db = Arc::new(Omnigraph::open(uri).await.unwrap());
        let schema = table_schema(&db).await;
        let mut completed = 0u64;
        let mut ordinal = 0u64;
        let mut samples = Vec::new();

        for checkpoint in [1u64, 4, 8] {
            while completed < checkpoint {
                let batch = score_batch(
                    Arc::clone(&schema),
                    &format!("metadata-normal-{completed}"),
                    1,
                );
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                    .await
                    .unwrap();
                ordinal += 1;
                db.failpoint_stream_b1_for_test(TABLE, None, 0)
                    .await
                    .unwrap();
                completed += 1;
            }

            let serialized_manifest_bytes = max_shard_manifest_bytes(dir.path());
            let pending = score_batch(
                Arc::clone(&schema),
                &format!("metadata-measured-{checkpoint}"),
                1,
            );
            let error = {
                let _after_durable =
                    ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
                db.failpoint_stream_b1_for_test(TABLE, Some(pending), ordinal)
                    .await
                    .expect_err("retire the writer so the metadata probe is a cold claim")
            };
            assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
            ordinal += 1;
            let _ = table_tracker.incremental_stats();
            let _ = manifest_tracker.incremental_stats();

            let started = Instant::now();
            let error = {
                let _before_seal =
                    ScopedFailPoint::new(names::STREAM_B1_BEFORE_FORCE_SEAL, "return");
                db.failpoint_stream_b1_for_test(TABLE, None, 0)
                    .await
                    .expect_err("measure retained metadata before generation output")
            };
            assert!(
                error
                    .to_string()
                    .contains(names::STREAM_B1_BEFORE_FORCE_SEAL),
                "{error:?}"
            );
            samples.push(RetainedMetadataSample {
                merged_generations: checkpoint,
                serialized_manifest_bytes,
                cold_elapsed_us: started.elapsed().as_micros(),
                table: PathIo::from_stats(&table_tracker.incremental_stats()),
            });

            let _ = manifest_tracker.incremental_stats();
            db.failpoint_stream_b1_for_test(TABLE, None, 0)
                .await
                .unwrap();
            completed += 1;
        }
        samples
    })
    .await;

    for sample in &samples {
        eprintln!(
            "B1 retained merged_generations={}: largest_retained_manifest_bytes={} cold_elapsed_us={} shard_manifest_reads={} aggregate_table_read_bytes={}",
            sample.merged_generations,
            sample.serialized_manifest_bytes,
            sample.cold_elapsed_us,
            sample.table.shard_manifest_reads,
            sample.table.total_read_bytes,
        );
        assert!(sample.table.shard_manifest_reads > 0, "{sample:?}");
    }
    assert!(
        samples.last().unwrap().serialized_manifest_bytes
            > samples.first().unwrap().serialized_manifest_bytes,
        "B1 intentionally retains merged generation entries; the serialized shard metadata must expose that growth: {samples:#?}"
    );
}

fn widest_accounting_batch(payload_bytes: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids = Arc::new(StringArray::from_iter_values(
        (0..WIDEST_ROWS).map(|row| format!("rss-{row:05}")),
    )) as ArrayRef;
    let payload = "x".repeat(payload_bytes);
    let values = Arc::new(StringArray::from_iter_values(
        (0..WIDEST_ROWS).map(|_| payload.as_str()),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, values]).unwrap()
}

fn widest_generation_estimates(payload_bytes: usize) -> (usize, usize, usize) {
    let caller = widest_accounting_batch(payload_bytes);
    let mut columns = caller.columns().to_vec();
    columns.push(Arc::new(BooleanArray::from(vec![false; WIDEST_ROWS])) as ArrayRef);
    let stored =
        RecordBatch::try_new(schema_with_tombstone(caller.schema().as_ref()), columns).unwrap();
    let arrow_reservation = stored.get_array_memory_size();
    let batch_store_estimate = StoredBatch::new(stored, 0, 0).estimated_size;
    let bloom_estimate = Sbbf::with_ndv_fpp(8_192, 0.00057)
        .unwrap()
        .estimated_memory_size();
    (arrow_reservation, batch_store_estimate, bloom_estimate)
}

fn fragmented_generation_estimates() -> (usize, usize, usize, usize) {
    fn one_batch(payload_bytes: usize, row: usize) -> (usize, usize) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let caller = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![format!("rss-{row:05}")])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x".repeat(payload_bytes)])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut columns = caller.columns().to_vec();
        columns.push(Arc::new(BooleanArray::from(vec![false])) as ArrayRef);
        let stored =
            RecordBatch::try_new(schema_with_tombstone(caller.schema().as_ref()), columns).unwrap();
        let arrow = stored.get_array_memory_size();
        let batch_store = StoredBatch::new(stored, row as u64, row).estimated_size;
        (arrow, batch_store)
    }

    // Every id is fixed-width (`rss-00000` .. `rss-08191`). Tune the largest
    // uniform one-row payload whose exact post-tombstone accounting remains a
    // legal 32-MiB generation. Then construct and sum all 8,192 exact
    // `StoredBatch` estimates rather than extrapolating one monolithic batch.
    let mut low = 0usize;
    let mut high = 4_096usize;
    while low < high {
        let candidate = low + (high - low).div_ceil(2);
        let (one_arrow, _) = one_batch(candidate, 0);
        if one_arrow.saturating_mul(WIDEST_ROWS) <= HARD_ARROW_BYTES {
            low = candidate;
        } else {
            high = candidate - 1;
        }
    }
    let payload_bytes = low;
    let mut arrow_sum = 0usize;
    let mut batch_store_sum = 0usize;
    for row in 0..WIDEST_ROWS {
        let (arrow, batch_store) = one_batch(payload_bytes, row);
        arrow_sum = arrow_sum.checked_add(arrow).unwrap();
        batch_store_sum = batch_store_sum.checked_add(batch_store).unwrap();
    }
    let bloom_estimate = Sbbf::with_ndv_fpp(8_192, 0.00057)
        .unwrap()
        .estimated_memory_size();
    (payload_bytes, arrow_sum, batch_store_sum, bloom_estimate)
}

#[cfg(unix)]
fn normalized_peak_rss_bytes(rusage: &libc::rusage) -> u64 {
    #[cfg(target_os = "macos")]
    let peak = rusage.ru_maxrss as u64;
    #[cfg(not(target_os = "macos"))]
    let peak = (rusage.ru_maxrss as u64) * 1024;
    peak
}

#[cfg(unix)]
fn wait4_rusage(pid: i32) -> (i64, u64) {
    let mut status: libc::c_int = 0;
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
    let reaped = loop {
        let result = unsafe { libc::wait4(pid, &mut status, 0, &mut rusage) };
        if result != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
            break result;
        }
    };
    assert_eq!(reaped, pid, "wait4 reaped an unexpected process");
    let exit = if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status) as i64
    } else if libc::WIFSIGNALED(status) {
        -(libc::WTERMSIG(status) as i64)
    } else {
        i64::MIN
    };
    (exit, normalized_peak_rss_bytes(&rusage))
}

#[cfg(unix)]
fn run_rss_child(mode: &str) -> u64 {
    use std::process::{Command, Stdio};

    let child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("widest_legal_generation_cost_child")
        .arg("--ignored")
        .arg("--nocapture")
        .env(RSS_CHILD_ENV, mode)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = i32::try_from(child.id()).unwrap();
    let (exit, peak) = wait4_rusage(pid);
    // `wait4` reaped the child; dropping the handle only closes its pipes.
    drop(child);
    assert_eq!(exit, 0, "{mode} widest-generation RSS child failed");
    peak
}

#[test]
fn widest_legal_generation_records_no_roll_estimates_and_peak_rss() {
    let (arrow_reservation, batch_store_estimate, bloom_estimate) =
        widest_generation_estimates(WIDEST_PAYLOAD_BYTES);
    let (one_byte_over_arrow_reservation, _, _) =
        widest_generation_estimates(WIDEST_PAYLOAD_BYTES + 1);
    let rc_trigger_estimate = batch_store_estimate + bloom_estimate;
    let (
        fragmented_payload_bytes,
        fragmented_arrow_reservation,
        fragmented_batch_store_estimate,
        fragmented_bloom_estimate,
    ) = fragmented_generation_estimates();
    let fragmented_rc_trigger_estimate =
        fragmented_batch_store_estimate + fragmented_bloom_estimate;
    assert!(
        arrow_reservation > 31 * 1024 * 1024 && arrow_reservation <= HARD_ARROW_BYTES,
        "fixture must be a concrete near-cap legal generation, got {arrow_reservation} bytes"
    );
    assert!(
        one_byte_over_arrow_reservation > HARD_ARROW_BYTES,
        "the next uniform payload byte must cross the exact Arrow cap; legal={} next={one_byte_over_arrow_reservation}",
        arrow_reservation,
    );
    assert!(WIDEST_ROWS < NO_AUTO_ROLL_ROWS);
    assert!(1 < NO_AUTO_ROLL_BATCHES);
    assert!(rc_trigger_estimate < NO_AUTO_ROLL_BYTES);
    assert!(
        fragmented_arrow_reservation > 31 * 1024 * 1024
            && fragmented_arrow_reservation <= HARD_ARROW_BYTES,
        "fragmented fixture must also be a concrete near-cap legal generation"
    );
    assert!(WIDEST_ROWS < NO_AUTO_ROLL_BATCHES);
    assert!(fragmented_rc_trigger_estimate < NO_AUTO_ROLL_BYTES);

    #[cfg(unix)]
    {
        let baseline_peak_rss = run_rss_child("baseline");
        let widest_peak_rss = run_rss_child("widest");
        let signed_process_delta = i128::from(widest_peak_rss) - i128::from(baseline_peak_rss);
        eprintln!(
            "B1 widest legal generation: rows={WIDEST_ROWS} one_batch_payload_bytes_per_row={WIDEST_PAYLOAD_BYTES} one_batch_post_tombstone_arrow_reservation={arrow_reservation} one_batch_batch_store_estimate={batch_store_estimate} rc_pk_bloom_estimate={bloom_estimate} one_batch_rc_roll_trigger_estimate={rc_trigger_estimate} fragmented_batches={WIDEST_ROWS} fragmented_payload_bytes_per_row={fragmented_payload_bytes} fragmented_post_tombstone_arrow_reservation={fragmented_arrow_reservation} fragmented_batch_store_estimate={fragmented_batch_store_estimate} fragmented_rc_roll_trigger_estimate={fragmented_rc_trigger_estimate} no_roll_bytes={NO_AUTO_ROLL_BYTES} rss_shape=one_batch baseline_peak_rss={baseline_peak_rss} widest_peak_rss={widest_peak_rss} signed_whole_process_delta={signed_process_delta}"
        );
        assert!(
            widest_peak_rss > baseline_peak_rss,
            "the isolated widest generation should have a visible whole-process RSS cost"
        );
    }

    #[cfg(not(unix))]
    eprintln!(
        "B1 widest legal generation: rows={WIDEST_ROWS} one_batch_payload_bytes_per_row={WIDEST_PAYLOAD_BYTES} one_batch_post_tombstone_arrow_reservation={arrow_reservation} one_batch_batch_store_estimate={batch_store_estimate} rc_pk_bloom_estimate={bloom_estimate} one_batch_rc_roll_trigger_estimate={rc_trigger_estimate} fragmented_batches={WIDEST_ROWS} fragmented_payload_bytes_per_row={fragmented_payload_bytes} fragmented_post_tombstone_arrow_reservation={fragmented_arrow_reservation} fragmented_batch_store_estimate={fragmented_batch_store_estimate} fragmented_rc_roll_trigger_estimate={fragmented_rc_trigger_estimate}; peak RSS unavailable on this platform"
    );
}

/// Subprocess half of `widest_legal_generation_records_no_roll_estimates_and_peak_rss`.
/// Keep it non-`serial`: the parent waits for this process and must not share an
/// interprocess serial-test lock with it.
#[test]
#[ignore = "subprocess helper; exercised by the widest-generation cost cell"]
fn widest_legal_generation_cost_child() {
    let Some(mode) = std::env::var_os(RSS_CHILD_ENV) else {
        return;
    };
    let mode = mode.to_string_lossy().into_owned();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let dir = tempfile::tempdir().unwrap();
            let uri = dir.path().to_str().unwrap();
            init_enrolled(uri, PAYLOAD_SCHEMA).await;
            let db = Arc::new(Omnigraph::open(uri).await.unwrap());
            let batch = match mode.as_str() {
                "baseline" => payload_batch(&db, 1, 1).await,
                "widest" => payload_batch(&db, WIDEST_ROWS, WIDEST_PAYLOAD_BYTES).await,
                other => panic!("unknown RSS child mode '{other}'"),
            };
            let table_tracker = IOTracker::default();
            let manifest_tracker = IOTracker::default();
            with_raw_io_trackers(&table_tracker, &manifest_tracker, async {
                let _ = table_tracker.incremental_stats();
                db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
                    .await
                    .unwrap();
                let table = PathIo::from_stats(&table_tracker.incremental_stats());
                assert!(table.wal_writes > 0, "{table:?}");
                assert_eq!(table.generation_writes, 0, "{table:?}");
            })
            .await;
        });
}
