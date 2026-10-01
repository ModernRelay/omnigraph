//! Native tests for traversal batching, work admission and the bound-edge spill path.

use super::*;
use crate::engine::context::QueryContext;
use arrow_array::Int64Array;
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use std::num::NonZeroU64;

use arrow_array::StringArray;
use datafusion::datasource::memory::MemorySourceConfig;
use futures::TryStreamExt;
use omnigraph_compiler::settings::SessionSettings;
use omnigraph_compiler::traversal::{EdgeMember, EdgeSelection};
use omnigraph_compiler::types::Direction;
use std::sync::atomic::Ordering;

use crate::Session;
use crate::db::{Omnigraph, ReadTarget};
use crate::engine::graph::GraphIndexHandle;
use crate::engine::operators::ExpandExec;
use crate::error::OmniError;
use crate::instrumentation::{
    QueryIoProbes, QueryMemoryProbes, with_query_io_probes, with_query_memory_probes,
};
use crate::loader::LoadMode;

async fn windowed(partitions: &[usize], cap: u64, stop_after_first: bool) -> (Vec<Vec<i64>>, bool) {
    let rows: usize = partitions.iter().sum();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|row| (row / 2) as i64),
        ))],
    )
    .unwrap();
    let mut offset = 0;
    let batches: Vec<_> = partitions
        .iter()
        .map(|&size| {
            let piece = batch.slice(offset, size);
            offset += size;
            Ok(piece)
        })
        .collect();
    let input = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter(batches),
    ));
    let ctx = QueryContext::with_traversal_limit(64 * 1024 * 1024, NonZeroU64::new(cap)).unwrap();
    let memory = Arc::new(WorkMemory::new(ctx.task_ctx(), "window test").unwrap());
    let mut windows = SourceWindows::new(input, schema, Arc::clone(&memory));
    let mut output = Vec::new();
    while let Some(window) = windows.next().await.unwrap() {
        output.push(
            window
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec(),
        );
        if stop_after_first {
            break;
        }
    }
    (output, memory.charge_traversal(1).is_err())
}

#[tokio::test]
async fn source_windows_preserve_duplicates_and_admission_across_batch_boundaries_issue_659() {
    let rows = SOURCE_WINDOW_ROWS * 2 + 3;
    let expected: Vec<_> = (0..rows).map(|row| (row / 2) as i64).collect();
    for partitions in [
        vec![rows],
        vec![1; rows],
        vec![0, 37, SOURCE_WINDOW_ROWS, rows - SOURCE_WINDOW_ROWS - 37],
    ] {
        let (windows, exhausted) = windowed(&partitions, rows as u64, false).await;
        assert_eq!(
            windows.iter().map(Vec::len).collect::<Vec<_>>(),
            [SOURCE_WINDOW_ROWS, SOURCE_WINDOW_ROWS, 3]
        );
        assert_eq!(windows.into_iter().flatten().collect::<Vec<_>>(), expected);
        assert!(exhausted, "each source occurrence consumes one work unit");
    }
}

#[tokio::test]
async fn stopping_after_a_window_does_not_admit_the_rest_of_a_large_input_batch_issue_659() {
    for partitions in [
        vec![SOURCE_WINDOW_ROWS * 3],
        vec![1; SOURCE_WINDOW_ROWS * 3],
    ] {
        let (windows, exhausted) = windowed(&partitions, SOURCE_WINDOW_ROWS as u64, true).await;
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].len(), SOURCE_WINDOW_ROWS);
        assert!(exhausted);
    }
}

const EDGE_SCHEMA: &str = r#"
node Person { name: String @key }
edge Knows: Person -> Person { payload: String }
edge Likes: Person -> Person { payload: String }
"#;

async fn graph_environment(seed: Vec<serde_json::Value>) -> (tempfile::TempDir, Arc<GraphEnv>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Session::from_defaults(
        Arc::new(
            Omnigraph::init(dir.path().to_str().unwrap(), EDGE_SCHEMA)
                .await
                .unwrap(),
        ),
        SessionSettings::default(),
    );
    let lines = seed
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    db.load_jsonl(&lines, LoadMode::Overwrite).await.unwrap();
    let snapshot = db.resolve_snapshot("main").await.unwrap();
    let (view, catalog) = db
        .capture_read_view(ReadTarget::Snapshot(snapshot))
        .await
        .unwrap();
    (
        dir,
        Arc::new(GraphEnv {
            snapshot: view.snapshot,
            catalog,
            graph_index: Arc::new(GraphIndexHandle::none()),
        }),
    )
}

fn people() -> Vec<serde_json::Value> {
    ["a", "b", "c", "d"]
        .into_iter()
        .map(|name| serde_json::json!({"type":"Person","data":{"name":name}}))
        .collect()
}

fn edge(kind: &str, id: &str, from: &str, to: &str, payload: &str) -> serde_json::Value {
    serde_json::json!({"edge":kind,"id":id,"from":from,"to":to,"data":{"payload":payload}})
}

fn selected_step(bound: bool, max_hops: u32) -> ExpandStep {
    ExpandStep {
        src: "p".into(),
        dst: "q".into(),
        execution: ExpandExecution::Budgeted(EdgeSelection::Alternation(vec![
            EdgeMember {
                edge_type: "Knows".into(),
                direction: Direction::Out,
            },
            EdgeMember {
                edge_type: "Likes".into(),
                direction: Direction::In,
            },
        ])),
        src_type: "Person".into(),
        dst_type: "Person".into(),
        min_hops: 1,
        max_hops,
        edge_binding: bound.then(|| "e".into()),
        frontier_estimate: None,
    }
}

fn sources(partitions: &[usize]) -> Arc<dyn ExecutionPlan> {
    let rows: usize = partitions.iter().sum();
    let schema = Arc::new(Schema::new(vec![
        Field::new("p.__id", DataType::Utf8, false),
        Field::new("occurrence", DataType::UInt32, false),
    ]));
    let wide = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                "a", rows,
            ))),
            Arc::new(UInt32Array::from_iter_values(0..rows as u32)),
        ],
    )
    .unwrap();
    let mut offset = 0;
    let batches = partitions
        .iter()
        .map(|&rows| {
            let batch = wide.slice(offset, rows);
            offset += rows;
            batch
        })
        .collect();
    MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap()
}

async fn execute_selected(
    input: Arc<dyn ExecutionPlan>,
    env: &Arc<GraphEnv>,
    step: ExpandStep,
    ctx: &QueryContext,
) -> crate::error::Result<Vec<RecordBatch>> {
    let plan = ExpandExec::try_new(input, step, Arc::clone(env))?;
    let stream = plan
        .execute(0, ctx.task_ctx())
        .map_err(|error| ctx.classify(error))?;
    stream
        .try_collect()
        .await
        .map_err(|error| ctx.classify(error))
}

fn rows(batches: &[RecordBatch], bound: bool) -> Vec<(u32, Vec<String>)> {
    let fields: &[&str] = if bound {
        &[
            "q.__id",
            "e.~edge_type",
            "e.__id",
            "e.__src",
            "e.__dst",
            "e.payload",
        ]
    } else {
        &["q.__id"]
    };
    let mut rows = Vec::new();
    for batch in batches {
        let occurrence = batch
            .column_by_name("occurrence")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let columns: Vec<_> = fields
            .iter()
            .map(|name| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
            })
            .collect();
        for row in 0..batch.num_rows() {
            rows.push((
                occurrence.value(row),
                columns
                    .iter()
                    .map(|column| column.value(row).to_owned())
                    .collect(),
            ));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn selected_expand_batches_preserve_rows_work_and_one_open_per_member_issue_659() {
    let mut seed = people();
    seed.extend([
        edge("Knows", "first", "a", "b", "knows-first"),
        edge("Knows", "second", "b", "d", "knows-second"),
        edge("Likes", "first", "c", "a", "likes-first"),
        edge("Likes", "second", "d", "c", "likes-second"),
    ]);
    let (_dir, env) = graph_environment(seed).await;
    let source_rows = SOURCE_WINDOW_ROWS + 3;
    for (bound, max_hops, exact_work) in [(true, 1, 40_987), (false, 1, 24_593), (false, 2, 40_991)]
    {
        let destinations: Vec<Vec<String>> = if bound {
            [
                ["b", "Knows", "first", "a", "b", "knows-first"],
                ["c", "Likes", "first", "c", "a", "likes-first"],
            ]
            .into_iter()
            .map(|values| values.map(str::to_owned).to_vec())
            .collect()
        } else {
            (if max_hops == 1 {
                vec!["b", "c"]
            } else {
                vec!["b", "c", "d"]
            })
            .into_iter()
            .map(|id| vec![id.to_string()])
            .collect()
        };
        let mut expected: Vec<_> = (0..source_rows as u32)
            .flat_map(|occurrence| {
                destinations
                    .iter()
                    .map(move |row| (occurrence, row.clone()))
            })
            .collect();
        expected.sort();
        for partitions in [
            vec![source_rows],
            vec![1; source_rows],
            vec![0, 17, 4_096, source_rows - 4_113],
        ] {
            let ctx =
                QueryContext::with_traversal_limit(64 * 1024 * 1024, NonZeroU64::new(exact_work))
                    .unwrap();
            let probes = QueryIoProbes::default();
            let result = with_query_io_probes(
                probes.clone(),
                execute_selected(
                    sources(&partitions),
                    &env,
                    selected_step(bound, max_hops),
                    &ctx,
                ),
            )
            .await
            .unwrap();
            let actual = rows(&result, bound);
            assert_eq!(actual.len(), expected.len());
            assert!(
                actual == expected,
                "bound={bound}, hops={max_hops}, partitions={}, first difference={:?}",
                partitions.len(),
                actual
                    .iter()
                    .zip(&expected)
                    .find(|(actual, expected)| actual != expected)
            );
            assert_eq!(
                probes.data_open_count.load(Ordering::Relaxed),
                2,
                "the uncached snapshot opens each member once across both source windows"
            );
            assert_eq!(probes.expand_indexed_runs.load(Ordering::Relaxed), 1);
            assert_eq!(probes.expand_csr_runs.load(Ordering::Relaxed), 0);
            let work = WorkMemory::new(ctx.task_ctx(), "post-expand work probe").unwrap();
            let error = work.error(work.charge_traversal(1).unwrap_err());
            assert!(
                matches!(error, OmniError::ResourceLimitExceeded { resource, limit, actual }
                if resource == "traversal_work_limit" && limit == exact_work && actual == exact_work + 1)
            );
        }
    }
}

#[tokio::test]
async fn empty_wildcard_records_one_indexed_commitment_without_data_opens_issue_659() {
    let (_dir, env) = graph_environment(people()).await;
    for bound in [false, true] {
        let ctx = QueryContext::with_traversal_limit(4 * 1024 * 1024, NonZeroU64::new(1)).unwrap();
        let mut step = selected_step(bound, 1);
        step.execution = ExpandExecution::Budgeted(EdgeSelection::Wildcard(vec![]));
        let probes = QueryIoProbes::default();
        let batches = with_query_io_probes(
            probes.clone(),
            execute_selected(sources(&[1]), &env, step, &ctx),
        )
        .await
        .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        assert_eq!(probes.expand_indexed_runs.load(Ordering::Relaxed), 1);
        assert_eq!(probes.expand_csr_runs.load(Ordering::Relaxed), 0);
        assert_eq!(probes.data_open_count.load(Ordering::Relaxed), 0);
    }
}

async fn released(probes: &QueryMemoryProbes) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while probes.active_blocking_work() != 0 || probes.reserved_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("every traversal worker and reservation must be released");
}

#[tokio::test]
async fn selected_bound_sort_spills_and_refuses_its_scratch_quota_issue_659() {
    const MIB: u64 = 1024 * 1024;
    let mut seed = people();
    for member in ["Knows", "Likes"] {
        for index in 0..64 {
            let payload = (0..1024)
                .map(|position| char::from(b'!' + ((position * 37 + index * 17) % 90) as u8))
                .collect::<String>();
            let (src, dst) = if member == "Knows" {
                ("a", "b")
            } else {
                ("b", "a")
            };
            seed.push(edge(member, &format!("edge{index:03}"), src, dst, &payload));
        }
    }
    let (_dir, env) = graph_environment(seed).await;
    let sufficient = QueryMemoryProbes::default();
    let result = with_query_memory_probes(sufficient.clone(), async {
        let ctx =
            QueryContext::with_scratch_limit(4 * MIB, NonZeroU64::new(20_000), 64 * MIB).unwrap();
        assert!(matches!(
            ctx.task_ctx()
                .session_config()
                .options()
                .execution
                .spill_compression,
            datafusion::common::config::SpillCompression::Uncompressed
        ));
        execute_selected(sources(&[128]), &env, selected_step(true, 1), &ctx).await
    })
    .await
    .unwrap();
    assert_eq!(
        result.iter().map(RecordBatch::num_rows).sum::<usize>(),
        16_384
    );
    let metrics = sufficient.execution_metrics();
    assert!(
        metrics.iter().any(|metric| metric.operator == "SortExec"
            && metric.spill_count > 0
            && metric.spilled_rows > 0
            && metric.spilled_bytes > MIB as usize),
        "the bound pair Sort must spill more bytes than the later quota: {metrics:#?}"
    );
    drop(result);
    released(&sufficient).await;

    let refused = QueryMemoryProbes::default();
    let error = with_query_memory_probes(refused.clone(), async {
        let ctx = QueryContext::with_scratch_limit(4 * MIB, NonZeroU64::new(20_000), MIB).unwrap();
        execute_selected(sources(&[128]), &env, selected_step(true, 1), &ctx).await
    })
    .await
    .unwrap_err();
    assert!(
        matches!(error, OmniError::ResourceLimitExceeded { ref resource, limit, actual }
        if resource == "query_scratch_bytes" && limit == MIB && actual > MIB),
        "{error:?}"
    );
    released(&refused).await;
}
