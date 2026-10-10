//! These fixtures require a 200k-row I/O meter and persisted historical IDs that today's loader normalizes.
use super::*;
use crate::db::{Omnigraph, ReadTarget};
use crate::engine::{EmbeddingResolver, EngineContext, GraphIndexHandle};
use crate::instrumentation::{ProbedStores, QueryIoProbes, with_query_io_probes};
use crate::table_store::TableStore;
use arrow_array::{ArrayRef, Date32Array, Float32Array, Int64Array, RecordBatch, StringArray};
use lance::index::DatasetIndexExt;
use lance_index::{IndexType, scalar::ScalarIndexParams};
use lance_io::utils::tracking_store::IOTracker;
use omnigraph_core::handle_cache::TableHandleCache;
use omnigraph_core::metadata::TableVersionMetadata;
use omnigraph_planner::{BoundPlan, PhysicalNode};

#[derive(Debug, Default)]
struct Io {
    requests: u64,
    bytes: u64,
}
fn drain(stores: &ProbedStores) -> Io {
    let mut io = Io::default();
    for store in stores.stores() {
        let stats = store.io_stats_incremental();
        io.requests += stats.read_iops;
        io.bytes += stats.read_bytes;
    }
    io
}
struct Observed {
    planning: Io,
    execution: Io,
    rows: serde_json::Value,
    explain: Explain,
}
async fn run(
    snapshot: &Snapshot,
    catalog: &Arc<Catalog>,
    text: &str,
    params: &ParamMap,
) -> Observed {
    run_with_access(snapshot, catalog, text, params, false).await
}
async fn run_with_access(
    snapshot: &Snapshot,
    catalog: &Arc<Catalog>,
    text: &str,
    params: &ParamMap,
    sequential: bool,
) -> Observed {
    let mut snapshot = snapshot.clone();
    snapshot.set_read_caches(omnigraph_catalog::SnapshotReadCaches {
        session: Arc::new(lance::session::Session::default()),
        handles: Arc::new(TableHandleCache::default()),
    });
    let probes = QueryIoProbes {
        table_wrapper: Some(Arc::new(IOTracker::default())),
        ..Default::default()
    };
    let stores = probes.table_stores.clone();
    with_query_io_probes(probes, async {
        let statement = omnigraph_compiler::find_read_statement(text, "lookup").unwrap();
        let checked =
            omnigraph_compiler::query::typecheck::typecheck_query(catalog, statement.decl())
                .unwrap();
        let ir = omnigraph_compiler::lower_query(catalog, statement.decl(), &checked).unwrap();
        let settings = SessionSettings::default();
        let source = QuerySource::gather(&ir, catalog, &snapshot, params, &settings)
            .await
            .unwrap();
        assert!(
            source
                .index_facts("node:Person")
                .iter()
                .any(|fact| matches!(
                    fact.kind,
                    omnigraph_planner::IndexKind::Btree { usable: true }
                ) && fact.column == "__id")
        );
        let ExplainedQuery { explain, physical } = explain_query(&source).await.unwrap();
        let mut bound = crate::engine::bind::bind(physical, &source, &EmbeddingResolver::explain())
            .await
            .unwrap();
        if sequential {
            let scans: Vec<_> = bound
                .plan
                .live()
                .filter_map(|(id, node)| matches!(node, PhysicalNode::Scan { .. }).then_some(id))
                .collect();
            for id in scans {
                let Some(PhysicalNode::Scan { spec, .. }) = bound.plan.node_mut(id) else {
                    unreachable!()
                };
                spec.access = Some(omnigraph_planner::ScanAccess::Sequential);
            }
        }
        let encoded = serde_json::to_vec(&bound).unwrap();
        let bound: BoundPlan = serde_json::from_slice(&encoded).unwrap();
        assert!(
            bound.plan.live().any(
                |(_, node)| matches!(node,PhysicalNode::Scan {spec,..} if spec.access.is_some())
            )
        );
        let planning = drain(&stores);
        let context = EngineContext {
            snapshot: &snapshot,
            catalog,
            graph_index: Arc::new(GraphIndexHandle::none()),
        };
        let run = crate::engine::execute(bound, &context).await.unwrap();
        let execution = drain(&stores);
        Observed {
            planning,
            execution,
            rows: run.result.to_rust_json().unwrap(),
            explain,
        }
    })
    .await
}
async fn pin(root: &str, snapshot: &mut Snapshot, batch: RecordBatch) {
    let path = "planner-key-fixture";
    let mut dataset = TableStore::write_dataset(&format!("{root}/{path}"), batch)
        .await
        .unwrap();
    dataset
        .create_index(
            &["__id"],
            IndexType::BTree,
            Some("identity_probe".into()),
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    let entry = snapshot.raw_mut().entries.get_mut("node:Person").unwrap();
    entry.dataset_path = path.into();
    entry.published_dataset_version = dataset.version().version;
    entry.native_dataset_branch = None;
    entry.entity_count = dataset.count_rows(None).await.unwrap() as u64;
    entry.version_metadata = TableVersionMetadata::from_dataset(root, path, &dataset).unwrap();
}
fn batch(catalog: &Catalog, mut columns: HashMap<&str, ArrayRef>) -> RecordBatch {
    let schema = catalog.node_types["Person"].arrow_schema.clone();
    let arrays = schema
        .fields()
        .iter()
        .map(|field| columns.remove(field.name().as_str()).unwrap())
        .collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}
#[tokio::test]
async fn planning_cost_key_lookup_at_200k_rows_is_within_twice_identity_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap();
    let db = Omnigraph::init(root, "node Person {\n slug: String @key\n}")
        .await
        .unwrap();
    let (mut view, catalog) = db
        .capture_read_view(ReadTarget::branch("main"))
        .await
        .unwrap();
    let slugs: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..200_000).map(|n| format!("person-{n:06}")),
    ));
    pin(
        root,
        &mut view.snapshot,
        batch(
            &catalog,
            HashMap::from([("__id", slugs.clone()), ("slug", slugs)]),
        ),
    )
    .await;
    let params = ParamMap::from([("value".into(), Literal::String("person-167891".into()))]);
    let keyed = run(
        &view.snapshot,
        &catalog,
        "query lookup($value: String) { match { $p: Person $p.slug = $value } return { $p.slug } }",
        &params,
    )
    .await;
    let identity = run(
        &view.snapshot,
        &catalog,
        "query lookup($value: String) { match { $p: Person $p.@id = $value } return { $p.slug } }",
        &params,
    )
    .await;
    assert_eq!(keyed.rows, serde_json::json!([{"p.slug":"person-167891"}]));
    assert_eq!(keyed.rows, identity.rows);
    assert!(keyed.explain.passes.contains(&"key_to_id"));
    assert!(!identity.explain.passes.contains(&"key_to_id"));
    eprintln!(
        "planning_cost 200k keyed planning={:?} execution={:?}; identity planning={:?} execution={:?}",
        keyed.planning, keyed.execution, identity.planning, identity.execution
    );
    assert!(
        identity.execution.bytes > 0 && identity.planning.bytes > 0,
        "cost instrument must observe cold I/O"
    );
    assert!(keyed.execution.bytes <= 2 * identity.execution.bytes);
    assert!(
        keyed.planning.bytes + keyed.execution.bytes
            <= 2 * (identity.planning.bytes + identity.execution.bytes)
    );
    fn probe(node: &serde_json::Value) -> bool {
        node["access"] == "index_probe"
            || node["inputs"]
                .as_array()
                .is_some_and(|inputs| inputs.iter().any(probe))
    }
    assert!(probe(keyed.explain.physical_plan.as_ref().unwrap()));
    let sequential = run_with_access(
        &view.snapshot,
        &catalog,
        "query lookup($value: String) { match { $p: Person $p.slug = $value } return { $p.slug } }",
        &params,
        true,
    )
    .await;
    assert_eq!(sequential.rows, keyed.rows);
    assert!(
        sequential.execution.bytes > 2 * keyed.execution.bytes,
        "saved sequential access must disable the available index: {:?}",
        sequential.execution
    );
}
#[tokio::test]
async fn persisted_historical_key_spellings_are_never_narrowed_to_current_ids() {
    for (declaration, columns, query, params, historical) in [
        (
            "day: Date @key",
            HashMap::from([("day", Arc::new(Date32Array::from(vec![19723])) as ArrayRef)]),
            "query lookup($value: Date) { match { $p: Person $p.day = $value } return { $p.@id as id } }",
            ParamMap::from([("value".into(), Literal::Date("2024-01-01".into()))]),
            "2024-01-01",
        ),
        (
            "score: F32 @key",
            HashMap::from([(
                "score",
                Arc::new(Float32Array::from(vec![16_777_216.0])) as ArrayRef,
            )]),
            "query lookup($value: F32) { match { $p: Person $p.score = $value } return { $p.@id as id } }",
            ParamMap::from([("value".into(), Literal::Float(16_777_216.0))]),
            "16777217",
        ),
        (
            "tenant: String\n slot: I64\n @key(tenant, slot)",
            HashMap::from([
                (
                    "tenant",
                    Arc::new(StringArray::from(vec!["acme"])) as ArrayRef,
                ),
                ("slot", Arc::new(Int64Array::from(vec![7])) as ArrayRef),
            ]),
            "query lookup() { match { $p: Person { tenant: \"acme\", slot: 7 } } return { $p.@id as id } }",
            ParamMap::new(),
            "7",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let db = Omnigraph::init(root, &format!("node Person {{\n {declaration}\n}}"))
            .await
            .unwrap();
        let (mut view, catalog) = db
            .capture_read_view(ReadTarget::branch("main"))
            .await
            .unwrap();
        let mut columns = columns;
        columns.insert("__id", Arc::new(StringArray::from(vec![historical])));
        pin(root, &mut view.snapshot, batch(&catalog, columns)).await;
        let observed = run(&view.snapshot, &catalog, query, &params).await;
        assert_eq!(observed.rows, serde_json::json!([{"id":historical}]));
        assert!(!observed.explain.passes.contains(&"key_to_id"));
    }
}
