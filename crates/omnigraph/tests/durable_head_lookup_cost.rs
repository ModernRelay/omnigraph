//! RFC-024 Gate A: production-shaped, test-only evidence for durable table-head
//! lookup.  This deliberately does not use OmniGraph's production manifest
//! schema or publisher: the heads format is not accepted yet.  Instead it models
//! the relevant `__manifest` columns and publication shape in an isolated Lance
//! dataset, so the physical lookup candidate can be accepted or rejected before
//! any irreversible format code lands.
//!
//! Each logical publish atomically merge-inserts immutable table-version and
//! graph-commit journal rows together with mutable `table_head:<stable_id>` and
//! `graph_head:main` rows.  Head lookup uses a structured DataFusion `IN`
//! expression over `object_id`; the optional acceleration is Lance's BTREE on
//! that same column.  The matrix covers absent, reconciled, one-uncovered,
//! growing-unreconciled, and reconciled-after-tail index states over
//! compacted/uncompacted history, with a cold first lookup and a warm repeat
//! over the same open Dataset and shared `Session`.
//!
//! `ExecutionSummaryCounts::{iops,requests,bytes_read,parts_loaded}` are public,
//! stable summary fields on the pinned Lance surface.  The
//! `fragments_scanned`, `ranges_scanned`, and `rows_scanned` entries in
//! `all_counts` are explicitly documented upstream as debug/subject-to-change;
//! this dedicated substrate fixture intentionally requires the pinned RC.1 names
//! so a Lance upgrade fails visibly and triggers a fresh alignment audit.

mod helpers;

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::{
    Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array, new_null_array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{col, lit};
use futures::{FutureExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::scanner::ExecutionSummaryCounts;
use lance::dataset::{
    MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams,
};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::ScalarIndexParams;
use lance_io::utils::tracking_store::IOTracker;

use helpers::cost::open_tracked_lance_dataset;

const CATALOG_WIDTH: usize = 10;
// Both samples exceed the fixed catalog width, so every mutable head has
// rotated at least once.  This isolates history growth from result-fragment
// fanout saturation.
const DEFAULT_DEPTHS: [u64; 2] = [20, 80];
const UNRECONCILED_TAIL: u64 = 8;
const INDEX_NAME: &str = "rfc024_object_id_btree";

const LOOKUP_COLUMNS: [&str; 11] = [
    "object_id",
    "object_type",
    "location",
    "metadata",
    "base_objects",
    "table_key",
    "stable_table_id",
    "table_incarnation_id",
    "table_version",
    "table_branch",
    "row_count",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum HistoryLayout {
    Compacted,
    Uncompacted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IndexState {
    Absent,
    Reconciled,
    OneUncovered,
    GrowingTail,
    ReconciledAfterTail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AccessMode {
    ColdOpen,
    WarmRepeat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageBackend {
    Local,
    S3,
}

/// One measured lookup.  Keeping this concise and backend-neutral makes the
/// local and bucket-gated S3 tests exercise exactly the same matrix.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LookupCost {
    object_store_reads: u64,
    object_store_read_bytes: u64,
    scan_iops: u64,
    scan_requests: u64,
    scan_read_bytes: u64,
    indices_loaded: u64,
    /// Lance's `parts_loaded`: BTREE pages loaded from storage (performance guide).
    index_pages_loaded: u64,
    fragments_scanned: u64,
    ranges_scanned: u64,
    rows_scanned: u64,
    output_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CurvePoint {
    base_depth: u64,
    journal_depth: u64,
    layout: HistoryLayout,
    index_state: IndexState,
    access: AccessMode,
    total_fragments: u64,
    uncovered_fragments: u64,
    cost: LookupCost,
}

type LookupCurve = Vec<CurvePoint>;

#[derive(Debug, Clone)]
struct ManifestRow {
    object_id: String,
    object_type: String,
    location: Option<String>,
    metadata: Option<String>,
    table_key: String,
    stable_table_id: Option<u64>,
    table_incarnation_id: Option<u64>,
    table_version: Option<u64>,
    table_branch: Option<String>,
    row_count: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
struct TableHeadMetadata {
    state: String,
    stable_table_id: u64,
    incarnation_id: u64,
    physical_ref_incarnation: String,
    schema_ir_hash: String,
    head_graph_commit_id: Option<String>,
}

struct HeadFixture {
    uri: String,
    dataset: Dataset,
    head_versions: Vec<u64>,
    journal_depth: u64,
}

impl HeadFixture {
    async fn create(uri: String, catalog_width: usize) -> Self {
        let mut rows = Vec::with_capacity(catalog_width * 3 + 2);
        let head_versions = vec![1; catalog_width];
        for offset in 0..catalog_width {
            let stable_id = offset as u64 + 1;
            rows.push(registration_row(stable_id));
            rows.push(version_row(stable_id, 1, 0));
            rows.push(head_row(stable_id, 1, 0));
        }
        rows.extend(lineage_rows(0));

        let schema = manifest_schema();
        let batch = rows_to_batch(&rows, &schema);
        let reader = RecordBatchIterator::new([Ok(batch)], schema);
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            data_storage_version: Some(LanceFileVersion::V2_2),
            auto_cleanup: None,
            skip_auto_cleanup: true,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, &uri, Some(params))
            .await
            .unwrap_or_else(|err| panic!("failed to create RFC-024 fixture at {uri}: {err}"));

        Self {
            uri,
            dataset,
            head_versions,
            journal_depth: 0,
        }
    }

    async fn publish_until(&mut self, target_depth: u64) {
        assert!(target_depth >= self.journal_depth);
        while self.journal_depth < target_depth {
            self.publish_one().await;
        }
    }

    async fn publish_one(&mut self) {
        let next_depth = self.journal_depth + 1;
        let offset = (self.journal_depth as usize) % self.head_versions.len();
        let stable_id = offset as u64 + 1;
        self.head_versions[offset] += 1;
        let table_version = self.head_versions[offset];

        let mut rows = vec![
            version_row(stable_id, table_version, next_depth),
            head_row(stable_id, table_version, next_depth),
        ];
        rows.extend(lineage_rows(next_depth));
        let schema = manifest_schema();
        let batch = rows_to_batch(&rows, &schema);
        let reader = RecordBatchIterator::new([Ok(batch)], schema);

        // Mirrors the production manifest publisher's lowest gateway: one
        // MergeInsert, exact object_id key, no transparent conflict retries, and
        // no index-dependent write semantics.
        let mut builder = MergeInsertBuilder::try_new(
            Arc::new(self.dataset.clone()),
            vec!["object_id".to_string()],
        )
        .unwrap();
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(0)
            .use_index(false)
            .skip_auto_cleanup(true);
        let (dataset, stats) = builder
            .try_build()
            .unwrap()
            .execute_reader(Box::new(reader))
            .await
            .unwrap_or_else(|err| panic!("fixture publish at depth {next_depth} failed: {err}"));
        assert_eq!(stats.num_inserted_rows, 2, "journal rows must be immutable inserts");
        assert_eq!(stats.num_updated_rows, 2, "table + graph heads must update in place");
        self.dataset = Arc::try_unwrap(dataset).unwrap_or_else(|arc| (*arc).clone());
        self.journal_depth = next_depth;
    }

    async fn compact(&mut self) {
        compact_files(&mut self.dataset, CompactionOptions::default(), None)
            .await
            .unwrap_or_else(|err| panic!("fixture compaction failed: {err}"));
    }

    async fn create_object_id_index(&mut self) {
        self.dataset
            .create_index_builder(
                &["object_id"],
                IndexType::BTree,
                &ScalarIndexParams::default(),
            )
            .name(INDEX_NAME.to_string())
            .replace(true)
            .await
            .unwrap_or_else(|err| panic!("fixture BTREE creation failed: {err}"));
    }

    async fn reconcile_object_id_index(&mut self) {
        self.dataset
            .optimize_indices(&OptimizeOptions::default())
            .await
            .unwrap_or_else(|err| panic!("fixture BTREE reconciliation failed: {err}"));
    }

    async fn index_coverage(&self) -> Option<(u64, u64)> {
        let object_id_field = self.dataset.schema().field("object_id").unwrap().id;
        let indices = self.dataset.load_indices().await.unwrap();
        let segments: Vec<_> = indices
            .iter()
            .filter(|index| index.name == INDEX_NAME && index.fields == vec![object_id_field])
            .collect();
        if segments.is_empty() {
            return None;
        }

        let total = self.dataset.fragments().len() as u64;
        let uncovered = self
            .dataset
            .fragments()
            .iter()
            .filter(|fragment| {
                let id = u32::try_from(fragment.id).expect("test fragment id fits u32");
                !segments.iter().any(|segment| {
                    segment
                        .fragment_bitmap
                        .as_ref()
                        .is_some_and(|bitmap| bitmap.contains(id))
                })
            })
            .count() as u64;
        Some((total, uncovered))
    }

    async fn lookup_pair(&self) -> [(AccessMode, LookupCost); 2] {
        let tracker = IOTracker::default();
        let session = Arc::new(Session::default());
        let dataset = tracked_open(&self.uri, session, &tracker).await;
        let (cold_summary, cold_batches) =
            execute_head_lookup(&dataset, self.head_versions.len()).await;
        let cold_object_io = tracker.incremental_stats();
        validate_heads(&cold_batches, &self.head_versions);
        let cold_cost = lookup_cost(&cold_summary, &cold_object_io, &cold_batches);

        // The second lookup reuses the exact Dataset and Session from the cold
        // first lookup.  Reset only the tracker delta, not any Lance cache.
        let (warm_summary, warm_batches) =
            execute_head_lookup(&dataset, self.head_versions.len()).await;
        validate_heads(&warm_batches, &self.head_versions);
        let warm_object_io = tracker.incremental_stats();
        let warm_cost = lookup_cost(&warm_summary, &warm_object_io, &warm_batches);

        [
            (AccessMode::ColdOpen, cold_cost),
            (AccessMode::WarmRepeat, warm_cost),
        ]
    }
}

fn manifest_schema() -> SchemaRef {
    let object_id_metadata: HashMap<String, String> = [(
        LANCE_UNENFORCED_PRIMARY_KEY.to_string(),
        "true".to_string(),
    )]
    .into_iter()
    .collect();
    Arc::new(Schema::new(vec![
        Field::new("object_id", DataType::Utf8, false).with_metadata(object_id_metadata),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new(
            "base_objects",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("table_key", DataType::Utf8, false),
        Field::new("stable_table_id", DataType::UInt64, true),
        Field::new("table_incarnation_id", DataType::UInt64, true),
        Field::new("table_version", DataType::UInt64, true),
        Field::new("table_branch", DataType::Utf8, true),
        Field::new("row_count", DataType::UInt64, true),
    ]))
}

fn table_incarnation(stable_id: u64) -> u64 {
    10_000 + stable_id
}

fn table_key(stable_id: u64) -> String {
    format!("node:Type{stable_id}")
}

fn table_location(stable_id: u64) -> String {
    format!(
        "nodes/{stable_id:016x}-{:016x}",
        table_incarnation(stable_id)
    )
}

fn registration_row(stable_id: u64) -> ManifestRow {
    ManifestRow {
        object_id: format!(
            "table:{stable_id:016x}:{:016x}",
            table_incarnation(stable_id)
        ),
        object_type: "table".to_string(),
        location: Some(table_location(stable_id)),
        metadata: None,
        table_key: table_key(stable_id),
        stable_table_id: Some(stable_id),
        table_incarnation_id: Some(table_incarnation(stable_id)),
        table_version: None,
        table_branch: None,
        row_count: None,
    }
}

fn table_metadata(stable_id: u64, graph_commit: u64) -> String {
    serde_json::json!({
        "state": "live",
        "stable_table_id": stable_id,
        "incarnation_id": table_incarnation(stable_id),
        "physical_ref_incarnation": format!("fixture-ref-{stable_id}"),
        "schema_ir_hash": "fixture-schema-v1",
        "head_graph_commit_id": format!("fixture-commit-{graph_commit:020}"),
    })
    .to_string()
}

fn version_row(stable_id: u64, table_version: u64, graph_commit: u64) -> ManifestRow {
    ManifestRow {
        object_id: format!(
            "table_version:{stable_id:016x}:{:016x}:{table_version:020}",
            table_incarnation(stable_id)
        ),
        object_type: "table_version".to_string(),
        location: None,
        metadata: Some(table_metadata(stable_id, graph_commit)),
        table_key: table_key(stable_id),
        stable_table_id: Some(stable_id),
        table_incarnation_id: Some(table_incarnation(stable_id)),
        table_version: Some(table_version),
        table_branch: None,
        row_count: Some(table_version),
    }
}

fn head_row(stable_id: u64, table_version: u64, graph_commit: u64) -> ManifestRow {
    ManifestRow {
        object_id: format!("table_head:{stable_id}"),
        object_type: "table_head".to_string(),
        location: Some(table_location(stable_id)),
        metadata: Some(table_metadata(stable_id, graph_commit)),
        table_key: table_key(stable_id),
        stable_table_id: Some(stable_id),
        table_incarnation_id: Some(table_incarnation(stable_id)),
        table_version: Some(table_version),
        table_branch: None,
        row_count: Some(table_version),
    }
}

fn lineage_rows(graph_commit: u64) -> [ManifestRow; 2] {
    let commit_id = format!("fixture-commit-{graph_commit:020}");
    [
        ManifestRow {
            object_id: commit_id.clone(),
            object_type: "graph_commit".to_string(),
            location: None,
            metadata: Some(
                serde_json::json!({
                    "parent_commit_id": graph_commit.checked_sub(1).map(|parent| {
                        format!("fixture-commit-{parent:020}")
                    }),
                    "created_at": graph_commit,
                })
                .to_string(),
            ),
            table_key: String::new(),
            stable_table_id: None,
            table_incarnation_id: None,
            table_version: Some(graph_commit + 1),
            table_branch: None,
            row_count: None,
        },
        ManifestRow {
            object_id: "graph_head:main".to_string(),
            object_type: "graph_head".to_string(),
            location: None,
            metadata: Some(
                serde_json::json!({ "head_commit_id": commit_id }).to_string(),
            ),
            table_key: String::new(),
            stable_table_id: None,
            table_incarnation_id: None,
            table_version: None,
            table_branch: None,
            row_count: None,
        },
    ]
}

fn rows_to_batch(rows: &[ManifestRow], schema: &SchemaRef) -> RecordBatch {
    let object_ids: Vec<_> = rows.iter().map(|row| row.object_id.as_str()).collect();
    let object_types: Vec<_> = rows.iter().map(|row| row.object_type.as_str()).collect();
    let locations: Vec<_> = rows.iter().map(|row| row.location.as_deref()).collect();
    let metadata: Vec<_> = rows.iter().map(|row| row.metadata.as_deref()).collect();
    let table_keys: Vec<_> = rows.iter().map(|row| row.table_key.as_str()).collect();
    let table_branches: Vec<_> = rows.iter().map(|row| row.table_branch.as_deref()).collect();
    let list_type = schema.field_with_name("base_objects").unwrap().data_type();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(object_ids)),
            Arc::new(StringArray::from(object_types)),
            Arc::new(StringArray::from(locations)),
            Arc::new(StringArray::from(metadata)),
            new_null_array(list_type, rows.len()),
            Arc::new(StringArray::from(table_keys)),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.stable_table_id).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.table_incarnation_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.table_version).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(table_branches)),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.row_count).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn tracked_open(uri: &str, session: Arc<Session>, tracker: &IOTracker) -> Dataset {
    // The wrapper is attached before `load`, so cold latest-manifest metadata
    // reads are included.  Wrapping an already-open Dataset would miss them.
    open_tracked_lance_dataset(uri, session, tracker)
        .await
        .unwrap_or_else(|err| panic!("tracked open failed for {uri}: {err}"))
}

async fn execute_head_lookup(
    dataset: &Dataset,
    catalog_width: usize,
) -> (ExecutionSummaryCounts, Vec<RecordBatch>) {
    let summaries = Arc::new(Mutex::new(Vec::<ExecutionSummaryCounts>::new()));
    let callback_summaries = Arc::clone(&summaries);
    let mut scanner = dataset.scan();
    scanner
        .project(&LOOKUP_COLUMNS)
        .unwrap()
        .filter_expr(col("object_id").in_list(
            (1..=catalog_width)
                .map(|stable_id| lit(format!("table_head:{stable_id}")))
                .collect(),
            false,
        ))
        .use_scalar_index(true)
        .scan_stats_callback(Arc::new(move |summary| {
            callback_summaries.lock().unwrap().push(summary.clone());
        }));

    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let summaries = summaries.lock().unwrap();
    assert_eq!(
        summaries.len(),
        1,
        "one completed lookup must emit exactly one execution summary"
    );
    (summaries[0].clone(), batches)
}

fn required_debug_metric(summary: &ExecutionSummaryCounts, name: &str) -> u64 {
    u64::try_from(
        *summary
            .all_counts
            .get(name)
            .unwrap_or_else(|| panic!("pinned RC.1 scan summary omitted `{name}`: {summary:?}")),
    )
    .unwrap()
}

fn lookup_cost(
    summary: &ExecutionSummaryCounts,
    object_io: &lance_io::utils::tracking_store::IoStats,
    batches: &[RecordBatch],
) -> LookupCost {
    LookupCost {
        object_store_reads: object_io.read_iops,
        object_store_read_bytes: object_io.read_bytes,
        scan_iops: summary.iops as u64,
        scan_requests: summary.requests as u64,
        scan_read_bytes: summary.bytes_read as u64,
        indices_loaded: summary.indices_loaded as u64,
        index_pages_loaded: summary.parts_loaded as u64,
        fragments_scanned: required_debug_metric(summary, "fragments_scanned"),
        ranges_scanned: required_debug_metric(summary, "ranges_scanned"),
        rows_scanned: required_debug_metric(summary, "rows_scanned"),
        output_rows: batches.iter().map(|batch| batch.num_rows() as u64).sum(),
    }
}

fn validate_heads(batches: &[RecordBatch], expected_versions: &[u64]) {
    let mut actual = BTreeMap::<u64, u64>::new();
    for batch in batches {
        let object_ids = batch
            .column_by_name("object_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let object_types = batch
            .column_by_name("object_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let metadata = batch
            .column_by_name("metadata")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let stable_ids = batch
            .column_by_name("stable_table_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let incarnation_ids = batch
            .column_by_name("table_incarnation_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let versions = batch
            .column_by_name("table_version")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            let stable_id = stable_ids.value(row);
            assert_eq!(object_ids.value(row), format!("table_head:{stable_id}"));
            assert_eq!(object_types.value(row), "table_head");
            assert_eq!(incarnation_ids.value(row), table_incarnation(stable_id));
            let decoded: TableHeadMetadata = serde_json::from_str(metadata.value(row)).unwrap();
            assert_eq!(decoded.state, "live");
            assert_eq!(decoded.stable_table_id, stable_id);
            assert_eq!(decoded.incarnation_id, table_incarnation(stable_id));
            assert_eq!(
                decoded.physical_ref_incarnation,
                format!("fixture-ref-{stable_id}")
            );
            assert_eq!(decoded.schema_ir_hash, "fixture-schema-v1");
            assert!(decoded.head_graph_commit_id.is_some());
            assert!(actual.insert(stable_id, versions.value(row)).is_none());
        }
    }

    let expected: BTreeMap<_, _> = expected_versions
        .iter()
        .enumerate()
        .map(|(offset, version)| (offset as u64 + 1, *version))
        .collect();
    assert_eq!(actual, expected, "lookup must return each exact current head once");
}

async fn record_state(
    curve: &mut LookupCurve,
    fixture: &HeadFixture,
    base_depth: u64,
    layout: HistoryLayout,
    index_state: IndexState,
    backend: StorageBackend,
) {
    let (total_fragments, uncovered_fragments) = fixture
        .index_coverage()
        .await
        .unwrap_or_else(|| (fixture.dataset.fragments().len() as u64, fixture.dataset.fragments().len() as u64));
    for (access, cost) in fixture.lookup_pair().await {
        let point = CurvePoint {
            base_depth,
            journal_depth: fixture.journal_depth,
            layout,
            index_state,
            access,
            total_fragments,
            uncovered_fragments,
            cost,
        };
        eprintln!(
            "RFC024 backend={:?} depth={} actual={} layout={:?} index={:?} access={:?} fragments={}/{} object_io={}/{} scan_io={}/{} pages={} scanned=f{}/r{}/rows{} output={}",
            backend,
            point.base_depth,
            point.journal_depth,
            point.layout,
            point.index_state,
            point.access,
            point.total_fragments - point.uncovered_fragments,
            point.total_fragments,
            point.cost.object_store_reads,
            point.cost.object_store_read_bytes,
            point.cost.scan_iops,
            point.cost.scan_read_bytes,
            point.cost.index_pages_loaded,
            point.cost.fragments_scanned,
            point.cost.ranges_scanned,
            point.cost.rows_scanned,
            point.cost.output_rows,
        );
        curve.push(point);
    }
}

async fn run_matrix(
    base_uri: &str,
    depths: &[u64],
    backend: StorageBackend,
) -> LookupCurve {
    let mut curve = Vec::new();
    for layout in [HistoryLayout::Uncompacted, HistoryLayout::Compacted] {
        for &depth in depths {
            let uri = format!(
                "{}/{:?}-{}-{}",
                base_uri.trim_end_matches('/'),
                layout,
                depth,
                ulid::Ulid::new()
            );
            let mut fixture = HeadFixture::create(uri, CATALOG_WIDTH).await;
            fixture.publish_until(depth).await;
            if layout == HistoryLayout::Compacted {
                fixture.compact().await;
            }

            assert!(fixture.index_coverage().await.is_none());
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::Absent,
                backend,
            )
            .await;

            fixture.create_object_id_index().await;
            assert_eq!(fixture.index_coverage().await.unwrap().1, 0);
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::Reconciled,
                backend,
            )
            .await;

            fixture.publish_one().await;
            assert_eq!(fixture.index_coverage().await.unwrap().1, 1);
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::OneUncovered,
                backend,
            )
            .await;

            for _ in 1..UNRECONCILED_TAIL {
                fixture.publish_one().await;
            }
            assert_eq!(
                fixture.index_coverage().await.unwrap().1,
                UNRECONCILED_TAIL
            );
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::GrowingTail,
                backend,
            )
            .await;

            fixture.reconcile_object_id_index().await;
            assert_eq!(fixture.index_coverage().await.unwrap().1, 0);
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::ReconciledAfterTail,
                backend,
            )
            .await;
        }
    }
    curve
}

fn point(
    curve: &[CurvePoint],
    depth: u64,
    layout: HistoryLayout,
    state: IndexState,
    access: AccessMode,
) -> CurvePoint {
    *curve
        .iter()
        .find(|point| {
            point.base_depth == depth
                && point.layout == layout
                && point.index_state == state
                && point.access == access
        })
        .unwrap_or_else(|| panic!("missing curve point {depth}/{layout:?}/{state:?}/{access:?}"))
}

fn assert_flat(label: &str, shallow: u64, deep: u64) {
    assert_eq!(
        deep, shallow,
        "{label} must be flat as journal history grows"
    );
}

fn assert_grows(label: &str, shallow: u64, deep: u64) {
    assert!(
        deep > shallow,
        "{label} must remain an explicit known-growth blocker until the substrate behavior changes: shallow={shallow}, deep={deep}"
    );
}

fn assert_non_growing(label: &str, shallow: u64, deep: u64) {
    assert!(
        deep <= shallow,
        "{label} must not grow with journal history: shallow={shallow}, deep={deep}"
    );
}

fn assert_bounded_growth(label: &str, shallow: u64, deep: u64, slack: u64) {
    assert!(
        deep <= shallow + slack,
        "{label} exceeded its fixed bound: shallow={shallow}, deep={deep}, slack={slack}"
    );
}

fn assert_bounded_pair(label: &str, shallow: u64, deep: u64, ceiling: u64) {
    assert!(
        shallow <= ceiling && deep <= ceiling,
        "{label} exceeded its absolute bound: shallow={shallow}, deep={deep}, ceiling={ceiling}"
    );
}

fn assert_reconciled_slopes(
    curve: &[CurvePoint],
    depths: &[u64],
    backend: StorageBackend,
) {
    let shallow_depth = depths[0];
    let deep_depth = *depths.last().unwrap();
    assert!(deep_depth > shallow_depth);

    for layout in [HistoryLayout::Uncompacted, HistoryLayout::Compacted] {
        for access in [AccessMode::ColdOpen, AccessMode::WarmRepeat] {
            let shallow = point(
                curve,
                shallow_depth,
                layout,
                IndexState::Reconciled,
                access,
            );
            let deep = point(
                curve,
                deep_depth,
                layout,
                IndexState::Reconciled,
                access,
            );

            assert_flat(
                "reconciled rows_scanned proxy",
                shallow.cost.rows_scanned,
                deep.cost.rows_scanned,
            );
            assert_eq!(shallow.cost.rows_scanned, CATALOG_WIDTH as u64);
            assert_flat(
                "reconciled ranges scanned",
                shallow.cost.ranges_scanned,
                deep.cost.ranges_scanned,
            );
            assert_eq!(shallow.cost.ranges_scanned, CATALOG_WIDTH as u64);
            assert_flat(
                "reconciled result fragments",
                shallow.cost.fragments_scanned,
                deep.cost.fragments_scanned,
            );
            assert_eq!(
                shallow.cost.fragments_scanned,
                match layout {
                    HistoryLayout::Uncompacted => CATALOG_WIDTH as u64,
                    HistoryLayout::Compacted => 1,
                }
            );
            assert_flat(
                "reconciled BTREE pages",
                shallow.cost.index_pages_loaded,
                deep.cost.index_pages_loaded,
            );
            assert_eq!(
                shallow.cost.index_pages_loaded,
                match access {
                    AccessMode::ColdOpen => 1,
                    AccessMode::WarmRepeat => 0,
                }
            );
            // RC.1 crosses at most one additional range-read boundary between
            // the 10- and 1,000-commit endpoints. The candidate already fails
            // Gate A on physical byte curves; keep this second no-go visible
            // and bounded instead of describing the read cost as flat.
            assert_bounded_growth(
                "BLOCKER: reconciled scan I/O operations",
                shallow.cost.scan_iops,
                deep.cost.scan_iops,
                1,
            );
            assert_bounded_growth(
                "BLOCKER: reconciled scan requests",
                shallow.cost.scan_requests,
                deep.cost.scan_requests,
                1,
            );

            match (layout, access) {
                (HistoryLayout::Uncompacted, _) => assert_bounded_growth(
                    "BLOCKER: reconciled uncompacted scan bytes",
                    shallow.cost.scan_read_bytes,
                    deep.cost.scan_read_bytes,
                    128,
                ),
                (HistoryLayout::Compacted, AccessMode::ColdOpen) => assert_grows(
                    "BLOCKER: compacted cold scan bytes",
                    shallow.cost.scan_read_bytes,
                    deep.cost.scan_read_bytes,
                ),
                (HistoryLayout::Compacted, AccessMode::WarmRepeat) => assert_non_growing(
                    "compacted warm scan bytes",
                    shallow.cost.scan_read_bytes,
                    deep.cost.scan_read_bytes,
                ),
            }

            match (layout, access, backend) {
                (
                    HistoryLayout::Uncompacted,
                    AccessMode::ColdOpen,
                    StorageBackend::Local,
                ) => {
                    // Local filesystem manifest discovery is outside the range
                    // read wrapper.  This is a control, not evidence that remote
                    // cold open is flat.
                    assert_non_growing(
                        "local uncompacted cold object reads",
                        shallow.cost.object_store_reads,
                        deep.cost.object_store_reads,
                    );
                    assert_non_growing(
                        "local uncompacted cold object bytes",
                        shallow.cost.object_store_read_bytes,
                        deep.cost.object_store_read_bytes,
                    );
                }
                (
                    HistoryLayout::Uncompacted,
                    AccessMode::ColdOpen,
                    StorageBackend::S3,
                ) => {
                    // Known RFC-024 no-go: opening latest on object storage
                    // still walks history-dependent manifest metadata before
                    // the flat BTREE lookup begins.
                    assert_grows(
                        "BLOCKER: S3 uncompacted cold-open object reads",
                        shallow.cost.object_store_reads,
                        deep.cost.object_store_reads,
                    );
                    assert_grows(
                        "BLOCKER: S3 uncompacted cold-open object bytes",
                        shallow.cost.object_store_read_bytes,
                        deep.cost.object_store_read_bytes,
                    );
                }
                (HistoryLayout::Uncompacted, AccessMode::WarmRepeat, _) => {
                    assert_flat(
                        "uncompacted warm object reads",
                        shallow.cost.object_store_reads,
                        deep.cost.object_store_reads,
                    );
                    assert_bounded_growth(
                        "uncompacted warm object bytes",
                        shallow.cost.object_store_read_bytes,
                        deep.cost.object_store_read_bytes,
                        128,
                    );
                }
                (
                    HistoryLayout::Compacted,
                    AccessMode::ColdOpen,
                    StorageBackend::Local,
                ) => {
                    assert_non_growing(
                        "compacted cold object reads",
                        shallow.cost.object_store_reads,
                        deep.cost.object_store_reads,
                    );
                    // Local manifest/object bookkeeping is non-monotonic as
                    // compacted files cross encoding boundaries. It is a
                    // bounded control; scan bytes above carry the no-go slope.
                    assert_bounded_pair(
                        "local compacted cold object bytes",
                        shallow.cost.object_store_read_bytes,
                        deep.cost.object_store_read_bytes,
                        8 * 1024,
                    );
                }
                (
                    HistoryLayout::Compacted,
                    AccessMode::ColdOpen,
                    StorageBackend::S3,
                ) => {
                    assert_flat(
                        "S3 compacted cold object reads",
                        shallow.cost.object_store_reads,
                        deep.cost.object_store_reads,
                    );
                    // Exact head lookup performs a fixed number of reads, but
                    // their byte ranges still grow with the compacted data file.
                    assert_grows(
                        "BLOCKER: S3 compacted cold object bytes",
                        shallow.cost.object_store_read_bytes,
                        deep.cost.object_store_read_bytes,
                    );
                }
                (
                    HistoryLayout::Compacted,
                    AccessMode::WarmRepeat,
                    StorageBackend::Local,
                ) => {
                    assert_flat(
                        "local compacted warm object reads",
                        shallow.cost.object_store_reads,
                        deep.cost.object_store_reads,
                    );
                    assert_flat(
                        "local compacted warm object bytes",
                        shallow.cost.object_store_read_bytes,
                        deep.cost.object_store_read_bytes,
                    );
                }
                (
                    HistoryLayout::Compacted,
                    AccessMode::WarmRepeat,
                    StorageBackend::S3,
                ) => {
                    assert_flat(
                        "S3 compacted warm object reads",
                        shallow.cost.object_store_reads,
                        deep.cost.object_store_reads,
                    );
                    assert_grows(
                        "BLOCKER: S3 compacted warm object bytes",
                        shallow.cost.object_store_read_bytes,
                        deep.cost.object_store_read_bytes,
                    );
                }
            }
        }
    }
}

fn assert_matrix_contract(
    curve: &[CurvePoint],
    depths: &[u64],
    backend: StorageBackend,
) {
    assert_eq!(curve.len(), depths.len() * 2 * 5 * 2);
    for &depth in depths {
        for layout in [HistoryLayout::Uncompacted, HistoryLayout::Compacted] {
            for access in [AccessMode::ColdOpen, AccessMode::WarmRepeat] {
                let absent = point(curve, depth, layout, IndexState::Absent, access);
                let full = point(curve, depth, layout, IndexState::Reconciled, access);
                let one = point(curve, depth, layout, IndexState::OneUncovered, access);
                let tail = point(curve, depth, layout, IndexState::GrowingTail, access);
                let reconciled_after_tail = point(
                    curve,
                    depth,
                    layout,
                    IndexState::ReconciledAfterTail,
                    access,
                );

                for sample in [absent, full, one, tail, reconciled_after_tail] {
                    assert_eq!(sample.cost.output_rows, CATALOG_WIDTH as u64);
                }
                assert_eq!(absent.uncovered_fragments, absent.total_fragments);
                assert!(absent.cost.fragments_scanned > 0);
                assert!(absent.cost.rows_scanned >= CATALOG_WIDTH as u64);
                assert_eq!(absent.cost.indices_loaded, 0);
                assert_eq!(absent.cost.index_pages_loaded, 0);

                assert_eq!(full.uncovered_fragments, 0);
                // Lance reports the result-row fetches in these counters even
                // when every candidate came from the BTREE.  Reconciled work is
                // therefore bounded by catalog width, not expected to be zero.
                assert!(full.cost.fragments_scanned > 0);
                assert!(full.cost.fragments_scanned <= CATALOG_WIDTH as u64);
                assert_eq!(full.cost.rows_scanned, CATALOG_WIDTH as u64);

                assert_eq!(one.uncovered_fragments, 1);
                assert!(one.cost.fragments_scanned <= full.cost.fragments_scanned + 1);
                assert!(one.cost.rows_scanned >= CATALOG_WIDTH as u64);
                assert!(one.cost.rows_scanned <= CATALOG_WIDTH as u64 + 4);

                assert_eq!(tail.uncovered_fragments, UNRECONCILED_TAIL);
                assert!(
                    tail.cost.fragments_scanned
                        <= full.cost.fragments_scanned + UNRECONCILED_TAIL
                );
                assert!(tail.cost.rows_scanned >= CATALOG_WIDTH as u64);
                assert!(
                    tail.cost.rows_scanned
                        <= CATALOG_WIDTH as u64 + UNRECONCILED_TAIL * 4
                );
                assert!(
                    tail.cost.rows_scanned > one.cost.rows_scanned,
                    "growing uncovered tail must be visible in rows_scanned"
                );
                assert!(
                    tail.cost.ranges_scanned > one.cost.ranges_scanned,
                    "growing uncovered tail must be visible in scanned-range work"
                );

                assert_eq!(reconciled_after_tail.uncovered_fragments, 0);
                assert_eq!(
                    reconciled_after_tail.cost.rows_scanned,
                    CATALOG_WIDTH as u64,
                    "index reconciliation must restore catalog-width rows_scanned"
                );
                assert_eq!(
                    reconciled_after_tail.cost.ranges_scanned,
                    CATALOG_WIDTH as u64,
                    "index reconciliation must remove uncovered-tail range work"
                );
                if access == AccessMode::ColdOpen {
                    assert!(reconciled_after_tail.cost.indices_loaded > 0);
                }
                assert!(
                    tail.cost.rows_scanned > reconciled_after_tail.cost.rows_scanned,
                    "reconciliation must remove the rows_scanned tail penalty"
                );
            }

            // A cold reconciled lookup must visibly use the BTREE.  A warm
            // repeat may load zero pages because the shared Session cached them.
            let cold = point(
                curve,
                depth,
                layout,
                IndexState::Reconciled,
                AccessMode::ColdOpen,
            );
            assert!(cold.cost.indices_loaded > 0);
            assert!(cold.cost.index_pages_loaded > 0);

            if layout == HistoryLayout::Compacted {
                assert_eq!(
                    point(
                        curve,
                        depth,
                        layout,
                        IndexState::Reconciled,
                        AccessMode::ColdOpen,
                    )
                    .total_fragments,
                    1,
                    "compacted base history should have one fragment before tail writes"
                );
            }
        }
    }

    // Compare reconciled endpoints separately from per-state correctness.
    assert_reconciled_slopes(curve, depths, backend);

    // Negative control: without the derived index, the rows_scanned proxy must
    // grow with uncompacted journal history.  This proves the instrument can see the
    // original RFC-024 problem instead of reporting vacuous zeros.
    let shallow = point(
        curve,
        depths[0],
        HistoryLayout::Uncompacted,
        IndexState::Absent,
        AccessMode::ColdOpen,
    );
    let deep = point(
        curve,
        *depths.last().unwrap(),
        HistoryLayout::Uncompacted,
        IndexState::Absent,
        AccessMode::ColdOpen,
    );
    assert!(deep.cost.rows_scanned > shallow.cost.rows_scanned);
}

async fn run_with_cleanup<RunFuture, CleanupFuture>(
    run: RunFuture,
    cleanup: CleanupFuture,
    cleanup_context: &str,
) where
    RunFuture: Future<Output = ()>,
    CleanupFuture: Future<Output = Result<(), String>>,
{
    let run_result = AssertUnwindSafe(run).catch_unwind().await;
    let cleanup_result = cleanup.await;

    match run_result {
        Ok(()) => cleanup_result.unwrap_or_else(|error| panic!("{cleanup_context}: {error}")),
        Err(payload) => {
            if let Err(error) = cleanup_result {
                eprintln!("{cleanup_context} after the guarded operation also failed: {error}");
            }
            resume_unwind(payload);
        }
    }
}

#[tokio::test]
async fn cleanup_runs_when_guarded_matrix_panics() {
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleanup_witness = Arc::clone(&cleaned);
    let result = AssertUnwindSafe(run_with_cleanup(
        async { panic!("intentional matrix failure") },
        async move {
            cleanup_witness.store(true, Ordering::SeqCst);
            Ok(())
        },
        "test cleanup",
    ))
    .catch_unwind()
    .await;

    assert!(result.is_err(), "the original matrix panic must propagate");
    assert!(
        cleaned.load(Ordering::SeqCst),
        "cleanup must run before the original matrix panic propagates"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_durable_head_lookup_matrix_is_correct_and_observable() {
    let dir = tempfile::tempdir().unwrap();
    let base_uri = dir.path().join("rfc024-head-cost");
    let curve = run_matrix(
        base_uri.to_str().unwrap(),
        &DEFAULT_DEPTHS,
        StorageBackend::Local,
    )
    .await;
    assert_matrix_contract(&curve, &DEFAULT_DEPTHS, StorageBackend::Local);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_durable_head_lookup_matrix_is_correct_and_observable() {
    let Ok(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
        eprintln!(
            "SKIP s3_durable_head_lookup_matrix_is_correct_and_observable: \
             OMNIGRAPH_S3_TEST_BUCKET unset"
        );
        return;
    };
    // Once configured, every error below is fatal.  A down or misconfigured
    // RustFS/S3 endpoint must not turn the evidence gate into a silent skip.
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let base_uri = format!(
        "s3://{bucket}/{prefix}/rfc024-head-cost/{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    );
    let (store, path) = lance_io::object_store::ObjectStore::from_uri(&base_uri)
        .await
        .expect("configured RFC-024 S3/RustFS fixture prefix must resolve");
    run_with_cleanup(
        async {
            let curve = run_matrix(&base_uri, &DEFAULT_DEPTHS, StorageBackend::S3).await;
            assert_matrix_contract(&curve, &DEFAULT_DEPTHS, StorageBackend::S3);
        },
        async move {
            store
                .remove_dir_all(path)
                .await
                .map_err(|error| error.to_string())
        },
        "configured RFC-024 S3/RustFS fixture cleanup must succeed",
    )
    .await;
}

/// On-demand decision-scale run.  It intentionally exercises the complete
/// local matrix at 10/100/1,000 publishes; keep it out of default CI because it
/// performs thousands of real Lance commits and index builds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "RFC-024 decision instrument: thousands of real Lance commits"]
async fn local_durable_head_lookup_matrix_at_one_thousand_commits() {
    let dir = tempfile::tempdir().unwrap();
    let base_uri = dir.path().join("rfc024-head-cost-decision");
    let depths = [10, 100, 1_000];
    let curve = run_matrix(
        base_uri.to_str().unwrap(),
        &depths,
        StorageBackend::Local,
    )
    .await;
    assert_matrix_contract(&curve, &depths, StorageBackend::Local);
}
