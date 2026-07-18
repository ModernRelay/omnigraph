//! RFC-025 pre-activation decision instrument for checkpoint-registry access.
//!
//! This is deliberately a production-neutral Lance fixture.  It models the
//! proposed checkpoint name, header, table, and GC-boundary rows in an isolated
//! dataset without changing OmniGraph's accepted manifest schema or format
//! stamp.  Fixed live-checkpoint count and catalog width are held constant while
//! unrelated manifest journal history grows.
//!
//! The three measured operations are the complete authority reads required by
//! the proposed API: checkpoint list, exact name lookup (name reservation plus
//! header/table projection), and cleanup-root planning.  Every operation is
//! measured from a tracked cold open and then repeated on the same Dataset and
//! Session.  Object-store reads/bytes include the cold open; Lance execution
//! summaries separately report scan I/O and the pinned RC.1 debug counters.
//!
//! The fixture is a decision gate, not evidence that the format has shipped.
//! Its assertions preserve observed blockers rather than treating flat
//! `rows_scanned` as proof of flat physical I/O.

mod helpers;

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, RecordBatch, RecordBatchIterator, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::logical_expr::Expr;
use datafusion::prelude::{col, lit};
use futures::{FutureExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::dataset::scanner::ExecutionSummaryCounts;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use lance::datatypes::LANCE_UNENFORCED_PRIMARY_KEY;
use lance::index::DatasetIndexExt;
use lance::session::Session;
use lance_file::version::LanceFileVersion;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use lance_io::utils::tracking_store::IOTracker;

use helpers::cost::open_tracked_lance_dataset;

const CATALOG_WIDTH: usize = 10;
const LIVE_CHECKPOINTS: usize = 3;
const DEFAULT_DEPTHS: [u64; 2] = [20, 80];
const UNCOVERED_TAIL: u64 = 8;
const ROWS_PER_HISTORY_PUBLISH: u64 = 3;

const OBJECT_ID_INDEX: &str = "rfc025_object_id_btree";
const OBJECT_TYPE_INDEX: &str = "rfc025_object_type_btree";
const CHECKPOINT_ID_INDEX: &str = "rfc025_checkpoint_id_btree";

const PROJECTION: [&str; 13] = [
    "object_id",
    "object_type",
    "checkpoint_id",
    "checkpoint_name",
    "state",
    "generation",
    "stable_table_id",
    "table_incarnation_id",
    "table_path",
    "table_branch",
    "table_version",
    "physical_ref_incarnation",
    "gc_cutoff",
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
    BoundedTail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AccessMode {
    ColdOpen,
    WarmRepeat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AuthorityOperation {
    List,
    Show,
    CleanupPlan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageBackend {
    Local,
    S3,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AuthorityCost {
    object_store_reads: u64,
    object_store_read_bytes: u64,
    scan_iops: u64,
    scan_requests: u64,
    scan_read_bytes: u64,
    indices_loaded: u64,
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
    operation: AuthorityOperation,
    access: AccessMode,
    total_fragments: u64,
    uncovered_fragments: u64,
    cost: AuthorityCost,
}

type CostCurve = Vec<CurvePoint>;

#[derive(Debug, Clone)]
struct AuthorityRow {
    object_id: String,
    object_type: String,
    checkpoint_id: Option<String>,
    checkpoint_name: Option<String>,
    state: Option<String>,
    generation: Option<u64>,
    stable_table_id: Option<u64>,
    table_incarnation_id: Option<u64>,
    table_path: Option<String>,
    table_branch: Option<String>,
    table_version: Option<u64>,
    physical_ref_incarnation: Option<String>,
    gc_cutoff: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct Coverage {
    total_fragments: u64,
    uncovered_fragments: u64,
}

struct RetentionFixture {
    uri: String,
    dataset: Dataset,
    journal_depth: u64,
}

impl RetentionFixture {
    async fn create(uri: String) -> Self {
        let mut rows = checkpoint_authority_rows();
        rows.push(graph_head_row(0));

        let schema = authority_schema();
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
            .unwrap_or_else(|error| panic!("failed to create RFC-025 fixture at {uri}: {error}"));

        Self {
            uri,
            dataset,
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
        let stable_id = (self.journal_depth as usize % CATALOG_WIDTH) as u64 + 1;
        let rows = vec![
            table_version_history_row(stable_id, next_depth),
            graph_commit_row(next_depth),
            graph_head_row(next_depth),
        ];
        let schema = authority_schema();
        let batch = rows_to_batch(&rows, &schema);
        let reader = RecordBatchIterator::new([Ok(batch)], schema);

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
            .unwrap_or_else(|error| {
                panic!("RFC-025 fixture publish at depth {next_depth} failed: {error}")
            });
        assert_eq!(stats.num_inserted_rows, 2);
        assert_eq!(stats.num_updated_rows, 1);
        self.dataset = Arc::try_unwrap(dataset).unwrap_or_else(|arc| (*arc).clone());
        self.journal_depth = next_depth;
    }

    async fn compact(&mut self) {
        compact_files(&mut self.dataset, CompactionOptions::default(), None)
            .await
            .unwrap_or_else(|error| panic!("RFC-025 fixture compaction failed: {error}"));
    }

    async fn create_authority_indices(&mut self) {
        for (column, name) in [
            ("object_id", OBJECT_ID_INDEX),
            ("object_type", OBJECT_TYPE_INDEX),
            ("checkpoint_id", CHECKPOINT_ID_INDEX),
        ] {
            self.dataset
                .create_index_builder(&[column], IndexType::BTree, &ScalarIndexParams::default())
                .name(name.to_string())
                .replace(true)
                .await
                .unwrap_or_else(|error| panic!("RFC-025 {column} BTREE creation failed: {error}"));
        }
    }

    async fn coverage(&self) -> Option<Coverage> {
        let indices = self.dataset.load_indices().await.unwrap();
        let total_fragments = self.dataset.fragments().len() as u64;
        let mut maximum_uncovered = 0;

        for (column, name) in [
            ("object_id", OBJECT_ID_INDEX),
            ("object_type", OBJECT_TYPE_INDEX),
            ("checkpoint_id", CHECKPOINT_ID_INDEX),
        ] {
            let field_id = self.dataset.schema().field(column).unwrap().id;
            let segments: Vec<_> = indices
                .iter()
                .filter(|index| index.name == name && index.fields == vec![field_id])
                .collect();
            if segments.is_empty() {
                return None;
            }
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
            maximum_uncovered = maximum_uncovered.max(uncovered);
        }

        Some(Coverage {
            total_fragments,
            uncovered_fragments: maximum_uncovered,
        })
    }

    async fn operation_pair(
        &self,
        operation: AuthorityOperation,
    ) -> [(AccessMode, AuthorityCost); 2] {
        let tracker = IOTracker::default();
        let session = Arc::new(Session::default());
        let dataset = open_tracked_lance_dataset(&self.uri, session, &tracker)
            .await
            .unwrap_or_else(|error| panic!("tracked RFC-025 open failed: {error}"));

        let cold = execute_operation(&dataset, operation).await;
        let cold_io = tracker.incremental_stats();
        let cold_cost = authority_cost(&cold.summaries, &cold_io, cold.output_rows);

        let warm = execute_operation(&dataset, operation).await;
        let warm_io = tracker.incremental_stats();
        let warm_cost = authority_cost(&warm.summaries, &warm_io, warm.output_rows);

        [
            (AccessMode::ColdOpen, cold_cost),
            (AccessMode::WarmRepeat, warm_cost),
        ]
    }
}

fn authority_schema() -> SchemaRef {
    let object_id_metadata: HashMap<String, String> =
        [(LANCE_UNENFORCED_PRIMARY_KEY.to_string(), "true".to_string())]
            .into_iter()
            .collect();
    Arc::new(Schema::new(vec![
        Field::new("object_id", DataType::Utf8, false).with_metadata(object_id_metadata),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("checkpoint_id", DataType::Utf8, true),
        Field::new("checkpoint_name", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("generation", DataType::UInt64, true),
        Field::new("stable_table_id", DataType::UInt64, true),
        Field::new("table_incarnation_id", DataType::UInt64, true),
        Field::new("table_path", DataType::Utf8, true),
        Field::new("table_branch", DataType::Utf8, true),
        Field::new("table_version", DataType::UInt64, true),
        Field::new("physical_ref_incarnation", DataType::Utf8, true),
        Field::new("gc_cutoff", DataType::UInt64, true),
    ]))
}

fn checkpoint_id(index: usize) -> String {
    format!("fixture-checkpoint-{index:04}")
}

fn checkpoint_name(index: usize) -> String {
    format!("checkpoint-{index:02}")
}

/// Test-only reference contract for RFC-025's eventual checkpoint-name-v1
/// normalizer.  Identity is deliberately ASCII and case-sensitive: successful
/// normalization returns the input unchanged, so no case folding or Unicode
/// equivalence can collapse two authority keys.
fn normalize_checkpoint_name_v1(name: &str) -> Result<&str, &'static str> {
    if name.is_empty() || name.len() > 128 {
        return Err("checkpoint name must be 1..=128 bytes");
    }
    if !name.is_ascii() {
        return Err("checkpoint name must be ASCII");
    }
    if name.starts_with("__") || name.starts_with("ogcp_") {
        return Err("checkpoint name uses a reserved prefix");
    }
    if name.starts_with('/') || name.ends_with('/') {
        return Err("checkpoint name cannot start or end with '/'");
    }
    if name.contains("//") {
        return Err("checkpoint name cannot contain an empty segment");
    }
    if name.contains("..") {
        return Err("checkpoint name cannot contain '..'");
    }
    if name.contains('\\') {
        return Err("checkpoint name cannot contain a backslash");
    }
    if name.split('/').any(|segment| segment.ends_with(".lock")) {
        return Err("checkpoint name segments cannot end with '.lock'");
    }
    if !name.split('/').all(|segment| {
        segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    }) {
        return Err("checkpoint segments allow only [A-Za-z0-9._-]");
    }
    Ok(name)
}

fn table_incarnation(stable_id: u64) -> u64 {
    10_000 + stable_id
}

fn table_path(stable_id: u64) -> String {
    format!(
        "nodes/{stable_id:016x}-{:016x}",
        table_incarnation(stable_id)
    )
}

fn checkpoint_authority_rows() -> Vec<AuthorityRow> {
    let mut rows = Vec::new();
    for checkpoint_index in 1..=LIVE_CHECKPOINTS {
        let id = checkpoint_id(checkpoint_index);
        let generated_name = checkpoint_name(checkpoint_index);
        let name = normalize_checkpoint_name_v1(&generated_name)
            .expect("fixture checkpoint name must satisfy V1")
            .to_string();
        rows.push(AuthorityRow {
            object_id: format!("checkpoint_name:v1:{name}"),
            object_type: "checkpoint_name".to_string(),
            checkpoint_id: Some(id.clone()),
            checkpoint_name: Some(name.clone()),
            state: Some("live".to_string()),
            generation: Some(1),
            stable_table_id: None,
            table_incarnation_id: None,
            table_path: None,
            table_branch: None,
            table_version: None,
            physical_ref_incarnation: None,
            gc_cutoff: None,
        });
        rows.push(AuthorityRow {
            object_id: format!("checkpoint:{id}"),
            object_type: "checkpoint".to_string(),
            checkpoint_id: Some(id.clone()),
            checkpoint_name: Some(name),
            state: Some("live".to_string()),
            generation: Some(1),
            stable_table_id: None,
            table_incarnation_id: None,
            table_path: Some("__manifest".to_string()),
            table_branch: Some("main".to_string()),
            table_version: Some(checkpoint_index as u64 + 1),
            physical_ref_incarnation: Some(format!("manifest-ref-{checkpoint_index}")),
            gc_cutoff: None,
        });
        for offset in 0..CATALOG_WIDTH {
            let stable_id = offset as u64 + 1;
            rows.push(AuthorityRow {
                object_id: format!(
                    "checkpoint_table:{id}:{stable_id:016x}:{:016x}",
                    table_incarnation(stable_id)
                ),
                object_type: "checkpoint_table".to_string(),
                checkpoint_id: Some(id.clone()),
                checkpoint_name: None,
                state: Some("live".to_string()),
                generation: Some(1),
                stable_table_id: Some(stable_id),
                table_incarnation_id: Some(table_incarnation(stable_id)),
                table_path: Some(table_path(stable_id)),
                table_branch: Some("main".to_string()),
                table_version: Some(checkpoint_index as u64 + stable_id),
                physical_ref_incarnation: Some(format!("table-ref-{checkpoint_index}-{stable_id}")),
                gc_cutoff: None,
            });
        }
    }

    for lineage in 0..=CATALOG_WIDTH {
        rows.push(AuthorityRow {
            object_id: format!("gc_boundary:fixture-lineage-{lineage:04}"),
            object_type: "gc_boundary".to_string(),
            checkpoint_id: None,
            checkpoint_name: None,
            state: Some("live".to_string()),
            generation: None,
            stable_table_id: (lineage > 0).then_some(lineage as u64),
            table_incarnation_id: (lineage > 0).then_some(table_incarnation(lineage as u64)),
            table_path: Some(if lineage == 0 {
                "__manifest".to_string()
            } else {
                table_path(lineage as u64)
            }),
            table_branch: Some("main".to_string()),
            table_version: None,
            physical_ref_incarnation: Some(format!("lineage-ref-{lineage}")),
            gc_cutoff: Some(0),
        });
    }
    rows
}

fn empty_history_row(object_id: String, object_type: &str) -> AuthorityRow {
    AuthorityRow {
        object_id,
        object_type: object_type.to_string(),
        checkpoint_id: None,
        checkpoint_name: None,
        state: None,
        generation: None,
        stable_table_id: None,
        table_incarnation_id: None,
        table_path: None,
        table_branch: None,
        table_version: None,
        physical_ref_incarnation: None,
        gc_cutoff: None,
    }
}

fn table_version_history_row(stable_id: u64, depth: u64) -> AuthorityRow {
    let mut row = empty_history_row(
        format!(
            "table_version:{stable_id:016x}:{:016x}:{depth:020}",
            table_incarnation(stable_id)
        ),
        "table_version",
    );
    row.stable_table_id = Some(stable_id);
    row.table_incarnation_id = Some(table_incarnation(stable_id));
    row.table_path = Some(table_path(stable_id));
    row.table_branch = Some("main".to_string());
    row.table_version = Some(depth + 1);
    row
}

fn graph_commit_row(depth: u64) -> AuthorityRow {
    let mut row = empty_history_row(format!("fixture-graph-commit-{depth:020}"), "graph_commit");
    row.generation = Some(depth);
    row
}

fn graph_head_row(depth: u64) -> AuthorityRow {
    let mut row = empty_history_row("graph_head:main".to_string(), "graph_head");
    row.generation = Some(depth);
    row
}

fn rows_to_batch(rows: &[AuthorityRow], schema: &SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.object_id.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.object_type.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.checkpoint_id.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.checkpoint_name.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.state.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.generation).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.stable_table_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| row.table_incarnation_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.table_path.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.table_branch.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.table_version).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.physical_ref_incarnation.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|row| row.gc_cutoff).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

#[derive(Debug)]
struct OperationResult {
    summaries: Vec<ExecutionSummaryCounts>,
    output_rows: u64,
}

async fn execute_scan(
    dataset: &Dataset,
    filter: Expr,
) -> (ExecutionSummaryCounts, Vec<RecordBatch>) {
    let summaries = Arc::new(Mutex::new(Vec::<ExecutionSummaryCounts>::new()));
    let callback_summaries = Arc::clone(&summaries);
    let mut scanner = dataset.scan();
    scanner
        .project(&PROJECTION)
        .unwrap()
        .filter_expr(filter)
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
    assert_eq!(summaries.len(), 1);
    (summaries[0].clone(), batches)
}

async fn execute_operation(dataset: &Dataset, operation: AuthorityOperation) -> OperationResult {
    match operation {
        AuthorityOperation::List => {
            let (summary, batches) =
                execute_scan(dataset, col("object_type").eq(lit("checkpoint"))).await;
            assert_eq!(object_ids(&batches), expected_checkpoint_headers());
            OperationResult {
                summaries: vec![summary],
                output_rows: row_count(&batches),
            }
        }
        AuthorityOperation::Show => {
            let expected_name = checkpoint_name(1);
            let (name_summary, name_batches) = execute_scan(
                dataset,
                col("object_id").eq(lit(format!("checkpoint_name:v1:{expected_name}"))),
            )
            .await;
            assert_eq!(
                object_ids(&name_batches),
                BTreeSet::from([format!("checkpoint_name:v1:{expected_name}")])
            );
            let resolved_id = only_checkpoint_id(&name_batches);
            assert_eq!(resolved_id, checkpoint_id(1));

            let authority_types =
                col("object_type").in_list(vec![lit("checkpoint"), lit("checkpoint_table")], false);
            let (projection_summary, projection_batches) = execute_scan(
                dataset,
                col("checkpoint_id")
                    .eq(lit(resolved_id))
                    .and(authority_types),
            )
            .await;
            assert_eq!(
                object_ids(&projection_batches),
                expected_checkpoint_projection(1)
            );
            OperationResult {
                summaries: vec![name_summary, projection_summary],
                output_rows: row_count(&name_batches) + row_count(&projection_batches),
            }
        }
        AuthorityOperation::CleanupPlan => {
            let (summary, batches) = execute_scan(
                dataset,
                col("object_type").in_list(
                    vec![
                        lit("checkpoint"),
                        lit("checkpoint_table"),
                        lit("gc_boundary"),
                    ],
                    false,
                ),
            )
            .await;
            assert_eq!(object_ids(&batches), expected_cleanup_roots());
            OperationResult {
                summaries: vec![summary],
                output_rows: row_count(&batches),
            }
        }
    }
}

fn object_ids(batches: &[RecordBatch]) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for batch in batches {
        let values = batch
            .column_by_name("object_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                ids.insert(values.value(row).to_string()),
                "duplicate authority row"
            );
        }
    }
    ids
}

fn only_checkpoint_id(batches: &[RecordBatch]) -> String {
    assert_eq!(row_count(batches), 1);
    let values = batches[0]
        .column_by_name("checkpoint_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!values.is_null(0));
    values.value(0).to_string()
}

fn row_count(batches: &[RecordBatch]) -> u64 {
    batches.iter().map(|batch| batch.num_rows() as u64).sum()
}

fn expected_checkpoint_headers() -> BTreeSet<String> {
    (1..=LIVE_CHECKPOINTS)
        .map(|index| format!("checkpoint:{}", checkpoint_id(index)))
        .collect()
}

fn expected_checkpoint_projection(index: usize) -> BTreeSet<String> {
    let id = checkpoint_id(index);
    std::iter::once(format!("checkpoint:{id}"))
        .chain((1..=CATALOG_WIDTH).map(|stable_id| {
            format!(
                "checkpoint_table:{id}:{stable_id:016x}:{:016x}",
                table_incarnation(stable_id as u64)
            )
        }))
        .collect()
}

fn expected_cleanup_roots() -> BTreeSet<String> {
    (1..=LIVE_CHECKPOINTS)
        .flat_map(|index| expected_checkpoint_projection(index).into_iter())
        .chain(
            (0..=CATALOG_WIDTH).map(|lineage| format!("gc_boundary:fixture-lineage-{lineage:04}")),
        )
        .collect()
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

fn authority_cost(
    summaries: &[ExecutionSummaryCounts],
    object_io: &lance_io::utils::tracking_store::IoStats,
    output_rows: u64,
) -> AuthorityCost {
    AuthorityCost {
        object_store_reads: object_io.read_iops,
        object_store_read_bytes: object_io.read_bytes,
        scan_iops: summaries.iter().map(|summary| summary.iops as u64).sum(),
        scan_requests: summaries
            .iter()
            .map(|summary| summary.requests as u64)
            .sum(),
        scan_read_bytes: summaries
            .iter()
            .map(|summary| summary.bytes_read as u64)
            .sum(),
        indices_loaded: summaries
            .iter()
            .map(|summary| summary.indices_loaded as u64)
            .sum(),
        index_pages_loaded: summaries
            .iter()
            .map(|summary| summary.parts_loaded as u64)
            .sum(),
        fragments_scanned: summaries
            .iter()
            .map(|summary| required_debug_metric(summary, "fragments_scanned"))
            .sum(),
        ranges_scanned: summaries
            .iter()
            .map(|summary| required_debug_metric(summary, "ranges_scanned"))
            .sum(),
        rows_scanned: summaries
            .iter()
            .map(|summary| required_debug_metric(summary, "rows_scanned"))
            .sum(),
        output_rows,
    }
}

async fn record_state(
    curve: &mut CostCurve,
    fixture: &RetentionFixture,
    base_depth: u64,
    layout: HistoryLayout,
    index_state: IndexState,
    backend: StorageBackend,
) {
    let coverage = fixture.coverage().await.unwrap_or(Coverage {
        total_fragments: fixture.dataset.fragments().len() as u64,
        uncovered_fragments: fixture.dataset.fragments().len() as u64,
    });
    for operation in [
        AuthorityOperation::List,
        AuthorityOperation::Show,
        AuthorityOperation::CleanupPlan,
    ] {
        for (access, cost) in fixture.operation_pair(operation).await {
            let point = CurvePoint {
                base_depth,
                journal_depth: fixture.journal_depth,
                layout,
                index_state,
                operation,
                access,
                total_fragments: coverage.total_fragments,
                uncovered_fragments: coverage.uncovered_fragments,
                cost,
            };
            eprintln!(
                "RFC025 backend={:?} depth={} actual={} layout={:?} index={:?} op={:?} access={:?} fragments={}/{} object_io={}/{} scan_io={}/{}/{} pages={} scanned=f{}/r{}/rows{} output={}",
                backend,
                point.base_depth,
                point.journal_depth,
                point.layout,
                point.index_state,
                point.operation,
                point.access,
                point.total_fragments - point.uncovered_fragments,
                point.total_fragments,
                point.cost.object_store_reads,
                point.cost.object_store_read_bytes,
                point.cost.scan_iops,
                point.cost.scan_requests,
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
}

async fn run_matrix(base_uri: &str, depths: &[u64], backend: StorageBackend) -> CostCurve {
    let mut curve = Vec::new();
    for layout in [HistoryLayout::Uncompacted, HistoryLayout::Compacted] {
        for &depth in depths {
            let uri = format!(
                "{}/{layout:?}-{depth}-{}",
                base_uri.trim_end_matches('/'),
                ulid::Ulid::new()
            );
            let mut fixture = RetentionFixture::create(uri).await;
            fixture.publish_until(depth).await;
            if layout == HistoryLayout::Compacted {
                fixture.compact().await;
            }

            assert!(fixture.coverage().await.is_none());
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::Absent,
                backend,
            )
            .await;

            fixture.create_authority_indices().await;
            assert_eq!(fixture.coverage().await.unwrap().uncovered_fragments, 0);
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::Reconciled,
                backend,
            )
            .await;

            for _ in 0..UNCOVERED_TAIL {
                fixture.publish_one().await;
            }
            assert_eq!(
                fixture.coverage().await.unwrap().uncovered_fragments,
                UNCOVERED_TAIL
            );
            record_state(
                &mut curve,
                &fixture,
                depth,
                layout,
                IndexState::BoundedTail,
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
    operation: AuthorityOperation,
    access: AccessMode,
) -> CurvePoint {
    *curve
        .iter()
        .find(|point| {
            point.base_depth == depth
                && point.layout == layout
                && point.index_state == state
                && point.operation == operation
                && point.access == access
        })
        .unwrap_or_else(|| {
            panic!("missing point {depth}/{layout:?}/{state:?}/{operation:?}/{access:?}")
        })
}

fn expected_rows(operation: AuthorityOperation) -> u64 {
    match operation {
        AuthorityOperation::List => LIVE_CHECKPOINTS as u64,
        AuthorityOperation::Show => (CATALOG_WIDTH + 2) as u64,
        AuthorityOperation::CleanupPlan => {
            (LIVE_CHECKPOINTS * (CATALOG_WIDTH + 1) + CATALOG_WIDTH + 1) as u64
        }
    }
}

fn scans_per_operation(operation: AuthorityOperation) -> u64 {
    match operation {
        AuthorityOperation::Show => 2,
        AuthorityOperation::List | AuthorityOperation::CleanupPlan => 1,
    }
}

fn assert_flat(label: &str, shallow: u64, deep: u64) {
    assert_eq!(deep, shallow, "{label} must be flat as history grows");
}

fn assert_grows(label: &str, shallow: u64, deep: u64) {
    assert!(
        deep > shallow,
        "{label} must remain an explicit no-go until substrate behavior changes: shallow={shallow}, deep={deep}"
    );
}

fn assert_changes(label: &str, shallow: u64, deep: u64) {
    assert_ne!(
        deep, shallow,
        "{label} must remain an explicit history-sensitive no-go until substrate behavior changes"
    );
}

fn assert_non_growing(label: &str, shallow: u64, deep: u64) {
    assert!(
        deep <= shallow,
        "{label} grew with history: shallow={shallow}, deep={deep}"
    );
}

fn assert_bounded_growth(label: &str, shallow: u64, deep: u64, slack: u64) {
    assert!(
        deep <= shallow + slack,
        "{label} exceeded its fixed bound: shallow={shallow}, deep={deep}, slack={slack}"
    );
}

fn assert_reconciled_physical_slopes(
    curve: &[CurvePoint],
    depths: &[u64],
    backend: StorageBackend,
) {
    let shallow_depth = depths[0];
    let deep_depth = *depths.last().unwrap();
    assert!(deep_depth > shallow_depth);

    for layout in [HistoryLayout::Uncompacted, HistoryLayout::Compacted] {
        for operation in [
            AuthorityOperation::List,
            AuthorityOperation::Show,
            AuthorityOperation::CleanupPlan,
        ] {
            for access in [AccessMode::ColdOpen, AccessMode::WarmRepeat] {
                let shallow = point(
                    curve,
                    shallow_depth,
                    layout,
                    IndexState::Reconciled,
                    operation,
                    access,
                );
                let deep = point(
                    curve,
                    deep_depth,
                    layout,
                    IndexState::Reconciled,
                    operation,
                    access,
                );

                assert_flat(
                    "reconciled rows_scanned proxy",
                    shallow.cost.rows_scanned,
                    deep.cost.rows_scanned,
                );
                assert_flat(
                    "reconciled scanned ranges",
                    shallow.cost.ranges_scanned,
                    deep.cost.ranges_scanned,
                );
                assert_flat(
                    "reconciled result fragments",
                    shallow.cost.fragments_scanned,
                    deep.cost.fragments_scanned,
                );
                assert_flat(
                    "reconciled index pages",
                    shallow.cost.index_pages_loaded,
                    deep.cost.index_pages_loaded,
                );
                match layout {
                    HistoryLayout::Uncompacted => {
                        assert_flat(
                            "reconciled uncompacted scan I/O operations",
                            shallow.cost.scan_iops,
                            deep.cost.scan_iops,
                        );
                        assert_flat(
                            "reconciled uncompacted scan requests",
                            shallow.cost.scan_requests,
                            deep.cost.scan_requests,
                        );
                    }
                    HistoryLayout::Compacted => {
                        // At the decision-scale endpoint Lance crosses one
                        // additional range-read boundary in the compacted data
                        // file.  Keep that physical transition visible and
                        // bounded instead of calling the operation count flat.
                        assert_bounded_growth(
                            "reconciled compacted scan I/O operations",
                            shallow.cost.scan_iops,
                            deep.cost.scan_iops,
                            1,
                        );
                        assert_bounded_growth(
                            "reconciled compacted scan requests",
                            shallow.cost.scan_requests,
                            deep.cost.scan_requests,
                            1,
                        );
                    }
                }

                match (layout, access) {
                    (HistoryLayout::Uncompacted, _) => assert_flat(
                        "reconciled uncompacted scan bytes",
                        shallow.cost.scan_read_bytes,
                        deep.cost.scan_read_bytes,
                    ),
                    (HistoryLayout::Compacted, AccessMode::ColdOpen) => assert_changes(
                        "BLOCKER: history-sensitive compacted cold scan bytes",
                        shallow.cost.scan_read_bytes,
                        deep.cost.scan_read_bytes,
                    ),
                    (HistoryLayout::Compacted, AccessMode::WarmRepeat) => assert_changes(
                        "BLOCKER: history-sensitive compacted warm scan bytes",
                        shallow.cost.scan_read_bytes,
                        deep.cost.scan_read_bytes,
                    ),
                }

                match (layout, access, backend) {
                    (HistoryLayout::Uncompacted, AccessMode::ColdOpen, StorageBackend::Local) => {
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
                    (HistoryLayout::Uncompacted, AccessMode::ColdOpen, StorageBackend::S3) => {
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
                        assert_flat(
                            "uncompacted warm object bytes",
                            shallow.cost.object_store_read_bytes,
                            deep.cost.object_store_read_bytes,
                        );
                    }
                    (HistoryLayout::Compacted, AccessMode::ColdOpen, _) => {
                        assert_bounded_growth(
                            "compacted cold object reads",
                            shallow.cost.object_store_reads,
                            deep.cost.object_store_reads,
                            1,
                        );
                        assert_changes(
                            "BLOCKER: history-sensitive compacted cold object bytes",
                            shallow.cost.object_store_read_bytes,
                            deep.cost.object_store_read_bytes,
                        );
                    }
                    (HistoryLayout::Compacted, AccessMode::WarmRepeat, StorageBackend::Local) => {
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
                    (HistoryLayout::Compacted, AccessMode::WarmRepeat, StorageBackend::S3) => {
                        assert_flat(
                            "S3 compacted warm object reads",
                            shallow.cost.object_store_reads,
                            deep.cost.object_store_reads,
                        );
                        assert_changes(
                            "BLOCKER: history-sensitive S3 compacted warm object bytes",
                            shallow.cost.object_store_read_bytes,
                            deep.cost.object_store_read_bytes,
                        );
                    }
                }
            }
        }
    }
}

fn assert_matrix_contract(curve: &[CurvePoint], depths: &[u64], backend: StorageBackend) {
    assert_eq!(curve.len(), depths.len() * 2 * 3 * 3 * 2);
    for &depth in depths {
        for layout in [HistoryLayout::Uncompacted, HistoryLayout::Compacted] {
            for operation in [
                AuthorityOperation::List,
                AuthorityOperation::Show,
                AuthorityOperation::CleanupPlan,
            ] {
                for access in [AccessMode::ColdOpen, AccessMode::WarmRepeat] {
                    let absent = point(curve, depth, layout, IndexState::Absent, operation, access);
                    let reconciled = point(
                        curve,
                        depth,
                        layout,
                        IndexState::Reconciled,
                        operation,
                        access,
                    );
                    let tail = point(
                        curve,
                        depth,
                        layout,
                        IndexState::BoundedTail,
                        operation,
                        access,
                    );

                    for sample in [absent, reconciled, tail] {
                        assert_eq!(sample.cost.output_rows, expected_rows(operation));
                    }
                    assert_eq!(absent.uncovered_fragments, absent.total_fragments);
                    assert_eq!(absent.cost.indices_loaded, 0);
                    assert_eq!(absent.cost.index_pages_loaded, 0);
                    assert!(absent.cost.rows_scanned >= expected_rows(operation));

                    assert_eq!(reconciled.uncovered_fragments, 0);
                    assert_eq!(reconciled.cost.rows_scanned, expected_rows(operation));
                    assert_eq!(tail.uncovered_fragments, UNCOVERED_TAIL);
                    assert!(tail.cost.rows_scanned >= reconciled.cost.rows_scanned);
                    assert!(
                        tail.cost.rows_scanned
                            <= reconciled.cost.rows_scanned
                                + UNCOVERED_TAIL
                                    * ROWS_PER_HISTORY_PUBLISH
                                    * scans_per_operation(operation)
                    );
                    assert!(
                        tail.cost.fragments_scanned
                            <= reconciled.cost.fragments_scanned
                                + UNCOVERED_TAIL * scans_per_operation(operation)
                    );
                    assert!(
                        tail.cost.ranges_scanned
                            <= reconciled.cost.ranges_scanned
                                + UNCOVERED_TAIL * scans_per_operation(operation)
                    );
                    if access == AccessMode::ColdOpen {
                        assert!(reconciled.cost.indices_loaded > 0);
                        assert!(reconciled.cost.index_pages_loaded > 0);
                    }
                }
            }
        }
    }

    for operation in [
        AuthorityOperation::List,
        AuthorityOperation::Show,
        AuthorityOperation::CleanupPlan,
    ] {
        let shallow = point(
            curve,
            depths[0],
            HistoryLayout::Uncompacted,
            IndexState::Absent,
            operation,
            AccessMode::ColdOpen,
        );
        let deep = point(
            curve,
            *depths.last().unwrap(),
            HistoryLayout::Uncompacted,
            IndexState::Absent,
            operation,
            AccessMode::ColdOpen,
        );
        assert!(
            deep.cost.rows_scanned > shallow.cost.rows_scanned,
            "absent-index {operation:?} must expose history growth"
        );
    }

    assert_reconciled_physical_slopes(curve, depths, backend);
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
                eprintln!("{cleanup_context} after matrix failure also failed: {error}");
            }
            resume_unwind(payload);
        }
    }
}

#[test]
fn checkpoint_name_v1_is_ascii_case_sensitive_and_collision_free() {
    let accepted = [
        "a",
        "Prod",
        "prod",
        "release/2026-07-17",
        "team_a/model.v1",
        "Ogcp_user",
        &"x".repeat(128),
    ];
    for name in accepted {
        assert_eq!(normalize_checkpoint_name_v1(name), Ok(name));
    }
    assert_ne!(
        normalize_checkpoint_name_v1("Prod").unwrap(),
        normalize_checkpoint_name_v1("prod").unwrap(),
        "case folding would collide distinct checkpoint authority keys"
    );

    let too_long = "x".repeat(129);
    let rejected = [
        "",
        "café",
        "/leading",
        "trailing/",
        "double//slash",
        "dot..dot",
        "back\\slash",
        "writer.lock",
        "parent/child.lock/leaf",
        "__internal",
        "ogcp_owned",
        "space name",
        "question?",
        too_long.as_str(),
    ];
    for name in rejected {
        assert!(
            normalize_checkpoint_name_v1(name).is_err(),
            "reference normalizer unexpectedly accepted {name:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn local_checkpoint_retention_matrix_is_exact_and_records_the_current_no_go() {
    let dir = tempfile::tempdir().unwrap();
    let base_uri = dir.path().join("rfc025-retention-cost");
    let curve = run_matrix(
        base_uri.to_str().unwrap(),
        &DEFAULT_DEPTHS,
        StorageBackend::Local,
    )
    .await;
    assert_matrix_contract(&curve, &DEFAULT_DEPTHS, StorageBackend::Local);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_checkpoint_retention_matrix_is_exact_and_records_the_current_no_go() {
    let Ok(bucket) = std::env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
        eprintln!(
            "SKIP s3_checkpoint_retention_matrix_is_exact_and_records_the_current_no_go: \
             OMNIGRAPH_S3_TEST_BUCKET unset"
        );
        return;
    };
    let prefix = std::env::var("OMNIGRAPH_S3_TEST_PREFIX")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "omnigraph-itests".to_string());
    let base_uri = format!(
        "s3://{bucket}/{prefix}/rfc025-retention-cost/{}-{}",
        std::process::id(),
        ulid::Ulid::new()
    );
    let (store, path) = lance_io::object_store::ObjectStore::from_uri(&base_uri)
        .await
        .expect("configured RFC-025 S3/RustFS fixture prefix must resolve");
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
        "configured RFC-025 S3/RustFS fixture cleanup must succeed",
    )
    .await;
}

/// On-demand decision-scale run.  It performs three complete authority reads
/// across both physical layouts and all index states at 10/100/1,000 real
/// Lance commits, so it remains outside the every-PR test path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "RFC-025 decision instrument: thousands of real Lance commits"]
async fn local_checkpoint_retention_matrix_at_one_thousand_commits() {
    let dir = tempfile::tempdir().unwrap();
    let base_uri = dir.path().join("rfc025-retention-cost-decision");
    let depths = [10, 100, 1_000];
    let curve = run_matrix(base_uri.to_str().unwrap(), &depths, StorageBackend::Local).await;
    assert_matrix_contract(&curve, &depths, StorageBackend::Local);
}
