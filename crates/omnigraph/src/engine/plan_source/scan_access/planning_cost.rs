//! Cold planning, execution and storage-format seams are not observable in a GQT plan claim.
use super::*;
use arrow_array::{ArrayRef, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use bytes::Bytes;
use futures::{TryStreamExt, stream::BoxStream};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{DataOverlayGroup, Operation, Transaction};
use lance::dataset::{CommitBuilder, WriteParams};
use lance::index::DatasetIndexExt;
use lance::io::WrappingObjectStore;
use lance_file::{version::LanceFileVersion, writer::FileWriterOptions};
use lance_index::{IndexType, scalar::ScalarIndexParams};
use lance_io::object_store::{ObjectStore as LanceStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::utils::CachedFileSize;
use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
use lance_table::format::{DataFile, ExternalFile, RowIdMeta};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as StoreResult, path::Path,
};
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_compiler::query::ast::{CompOp, Literal};
use omnigraph_compiler::{ExprType, ScalarType};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
struct Cost {
    requests: u64,
    bytes: u64,
}
fn total(costs: &Costs) -> Cost {
    costs.values().fold(Cost::default(), |mut total, cost| {
        total.requests += cost.requests;
        total.bytes += cost.bytes;
        total
    })
}
fn planning_budget(kind: &str) -> Costs {
    match kind {
        "modern" => BTreeMap::from([(
            "manifest",
            Cost {
                requests: 2,
                bytes: 855,
            },
        )]),
        "overlay" => BTreeMap::from([
            (
                "manifest",
                Cost {
                    requests: 2,
                    bytes: 894,
                },
            ),
            (
                "row_ids",
                Cost {
                    requests: 1,
                    bytes: 6,
                },
            ),
            (
                "deletions",
                Cost {
                    requests: 1,
                    bytes: 698,
                },
            ),
        ]),
        "legacy" => BTreeMap::from([
            (
                "manifest",
                Cost {
                    requests: 2,
                    bytes: 920,
                },
            ),
            (
                "index",
                Cost {
                    requests: 3,
                    bytes: 0,
                },
            ),
        ]),
        _ => panic!("unknown fixture"),
    }
}
type Costs = BTreeMap<&'static str, Cost>;
#[derive(Debug, Clone, Default)]
struct Tracker(Arc<Mutex<Costs>>);
impl Tracker {
    fn record(&self, path: &Path, bytes: u64) {
        let path = path.as_ref();
        let class = if path.contains("/_versions/") || path.ends_with(".manifest") {
            "manifest"
        } else if path.contains("/external-rowids-") {
            "row_ids"
        } else if path.contains("/_deletions/") {
            "deletions"
        } else if path.contains("/_indices/") {
            "index"
        } else if path.contains("/data/") {
            "data"
        } else {
            "other"
        };
        let mut costs = self.0.lock().unwrap();
        let cost = costs.entry(class).or_default();
        cost.requests += 1;
        cost.bytes += bytes;
    }
    fn drain(&self) -> Costs {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}
impl WrappingObjectStore for Tracker {
    fn wrap(&self, _: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(TrackingStore {
            target,
            tracker: self.clone(),
        })
    }
}
#[derive(Debug)]
struct TrackingStore {
    target: Arc<dyn ObjectStore>,
    tracker: Tracker,
}
impl std::fmt::Display for TrackingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlanningCost({})", self.target)
    }
}
#[async_trait::async_trait]
impl ObjectStore for TrackingStore {
    async fn get_opts(&self, path: &Path, options: GetOptions) -> StoreResult<GetResult> {
        let head = options.head;
        let result = self.target.get_opts(path, options).await;
        let bytes = if head {
            0
        } else {
            result
                .as_ref()
                .map_or(0, |result| result.range.end - result.range.start)
        };
        self.tracker.record(path, bytes);
        result
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.tracker.record(&prefix.cloned().unwrap_or_default(), 0);
        self.target.list(prefix)
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.tracker.record(&prefix.cloned().unwrap_or_default(), 0);
        self.target.list_with_offset(prefix, offset)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> StoreResult<ListResult> {
        self.tracker.record(&prefix.cloned().unwrap_or_default(), 0);
        self.target.list_with_delimiter(prefix).await
    }
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.target.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.target.put_multipart_opts(path, options).await
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<Path>>,
    ) -> BoxStream<'static, StoreResult<Path>> {
        self.target.delete_stream(paths)
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> StoreResult<()> {
        self.target.copy_opts(from, to, options).await
    }
}
fn reader(batch: RecordBatch) -> impl arrow_array::RecordBatchReader + Send + 'static {
    let schema = batch.schema();
    RecordBatchIterator::new(vec![Ok(batch)], schema)
}
async fn commit(ds: Dataset, op: Operation) -> Dataset {
    let version = ds.version().version;
    CommitBuilder::new(Arc::new(ds))
        .execute(Transaction::new(version, op, None))
        .await
        .unwrap()
}
async fn fixture(uri: &str, schema: Arc<Schema>, legacy: bool) -> Dataset {
    let batch = RecordBatch::try_new(
        schema.clone(),
        schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "__id" => Arc::new(StringArray::from_iter_values(
                    (0..12).map(|i| format!("p{i}")),
                )) as ArrayRef,
                "age" => {
                    Arc::new(Int32Array::from_iter_values((0..12).map(|i| i * 10))) as ArrayRef
                }
                name => panic!("unexpected field {name}"),
            })
            .collect(),
    )
    .unwrap();
    let mut ds = Dataset::write(
        reader(batch),
        uri,
        Some(WriteParams {
            enable_stable_row_ids: true,
            max_rows_per_file: 6,
            data_storage_version: Some(if legacy {
                LanceFileVersion::Legacy
            } else {
                LanceFileVersion::V2_2
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    ds.create_index(
        &["age"],
        IndexType::BTree,
        Some("age_idx".into()),
        &ScalarIndexParams::default(),
        true,
    )
    .await
    .unwrap();
    if legacy {
        let current = ds
            .load_indices()
            .await
            .unwrap()
            .iter()
            .find(|index| index.name == "age_idx")
            .unwrap()
            .clone();
        let mut old = current.clone();
        old.index_details = None;
        old.index_version = 0;
        ds = commit(
            ds,
            Operation::CreateIndex {
                new_indices: vec![old],
                removed_indices: vec![current],
            },
        )
        .await;
    }
    ds
}
async fn overlay(mut ds: Dataset, uri: &str) -> Dataset {
    ds.delete("__id = 'p2'").await.unwrap();
    assert!(ds.manifest().fragments[0].deletion_file.is_some());
    let (store, base) = LanceStore::from_uri_and_params(
        Arc::new(ObjectStoreRegistry::default()),
        uri,
        &Default::default(),
    )
    .await
    .unwrap();
    let mut fragments = ds.manifest().fragments.as_ref().clone();
    for fragment in &mut fragments {
        let Some(RowIdMeta::Inline(encoded)) = &fragment.row_id_meta else {
            panic!("expected inline row IDs")
        };
        let bytes = encoded.to_vec();
        let name = format!("external-rowids-fragment-{}.bin", fragment.id);
        store
            .inner
            .put(
                &base.clone().join(name.clone()),
                PutPayload::from(Bytes::from(bytes.clone())),
            )
            .await
            .unwrap();
        fragment.row_id_meta = Some(RowIdMeta::External(ExternalFile {
            path: name,
            offset: 0,
            size: bytes.len() as u64,
        }));
    }
    let schema = ds.schema().clone();
    ds = commit(
        ds,
        Operation::Merge {
            fragments,
            schema,
            preserves_nullability: true,
        },
    )
    .await;
    let fragment_id = ds.manifest().fragments[0].id;
    let field_id = ds.schema().field("age").unwrap().id;
    let filename = "age-overlay.lance";
    let version = ds.manifest().data_storage_format.lance_file_format();
    let writer = store
        .create(&base.clone().join("data").join(filename))
        .await
        .unwrap();
    let mut writer = lance_file::versions::create_writer(
        version,
        writer,
        ds.schema().project_by_ids(&[field_id], true),
        FileWriterOptions::default(),
    )
    .unwrap();
    writer
        .write_column(0, Arc::new(Int32Array::from(vec![999])) as ArrayRef)
        .await
        .unwrap();
    let summary = writer.finish().await.unwrap();
    let mut data_file = DataFile::new_unstarted(filename, version);
    data_file.fields = writer
        .field_id_to_column_indices()
        .iter()
        .map(|(field, _)| *field as i32)
        .collect::<Vec<_>>()
        .into();
    data_file.column_indices = writer
        .field_id_to_column_indices()
        .iter()
        .map(|(_, column)| *column as i32)
        .collect::<Vec<_>>()
        .into();
    data_file.file_size_bytes = CachedFileSize::new(summary.size_bytes);
    commit(
        ds,
        Operation::DataOverlay {
            groups: vec![DataOverlayGroup {
                fragment_id,
                overlays: vec![DataOverlayFile {
                    data_file,
                    coverage: OverlayCoverage::dense([1u32].into_iter().collect()),
                    committed_version: 0,
                }],
            }],
        },
    )
    .await
}
async fn measure(
    uri: &str,
    version: u64,
    catalog: &omnigraph_compiler::catalog::Catalog,
    value: i64,
    finalized: bool,
) -> (Costs, Costs, Vec<String>) {
    let tracker = Tracker::default();
    let ds = DatasetBuilder::from_uri(uri)
        .with_version(version)
        .with_session(Arc::new(lance::session::Session::default()))
        .with_store_params(ObjectStoreParams {
            object_store_wrapper: Some(Arc::new(tracker.clone())),
            ..Default::default()
        })
        .load()
        .await
        .unwrap();
    assert!(!super::super::indexes::gather(&ds).await.unwrap().is_empty());
    let ty = ExprType::Value {
        scalar: ScalarType::I32,
        list: false,
        nullable: false,
    };
    let filters = vec![IRExpr::comparison(
        IRExpr::PropAccess {
            variable: "p".into(),
            property: "age".into(),
            ty: ty.clone(),
        },
        CompOp::Eq,
        IRExpr::Literal(Literal::Integer(value), ty),
    )];
    let params = ParamMap::new();
    let read =
        NodeRead::for_index_split(ds.clone(), "Person", &filters, &params, catalog, None).unwrap();
    let (plan, filter) = read.plan_with_filter().await.unwrap();
    if finalized {
        assert!(matches!(
            inspect(&plan, &ds, filter).await.unwrap(),
            ScanAccess::IndexProbe { .. }
        ));
    }
    let planning = tracker.drain();
    let planned_signature = super::tests::filter_signature(&plan);
    drop(plan);
    let plan = read.plan(None, |_| Ok(())).await.unwrap();
    assert_eq!(planned_signature, super::tests::filter_signature(&plan));
    let batches: Vec<RecordBatch> = lance_datafusion::exec::execute_plan(plan, Default::default())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let execution = tracker.drain();
    let mut ids = Vec::new();
    for batch in batches {
        ids.extend(
            batch
                .column_by_name("__id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|id| id.unwrap().to_string()),
        );
    }
    ids.sort();
    (planning, execution, ids)
}
#[tokio::test]
async fn planning_cost_matches_one_cold_builder_for_modern_overlay_and_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let db =
        crate::db::Omnigraph::init(dir.path().to_str().unwrap(), "node Person {\n age: I32\n}")
            .await
            .unwrap();
    let (_, catalog) = db
        .capture_read_view(crate::db::ReadTarget::branch("main"))
        .await
        .unwrap();
    for kind in ["modern", "overlay", "legacy"] {
        let authority = dir
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .trim_start_matches('.');
        let uri = format!("shared-memory://planner-{authority}/{kind}");
        let mut ds = fixture(
            &uri,
            catalog.node_types["Person"].arrow_schema.clone(),
            kind == "legacy",
        )
        .await;
        if kind == "overlay" {
            ds = overlay(ds, &uri).await;
        }
        for value in if kind == "overlay" {
            vec![10, 20, 999]
        } else {
            vec![10]
        } {
            let baseline = measure(&uri, ds.version().version, &catalog, value, false).await;
            let finalized = measure(&uri, ds.version().version, &catalog, value, true).await;
            let planning_total = total(&finalized.0);
            let execution_total = total(&finalized.1);
            let combined = Cost {
                requests: planning_total.requests + execution_total.requests,
                bytes: planning_total.bytes + execution_total.bytes,
            };
            eprintln!(
                "planning_cost {kind} value={value} baseline={:?} finalized={:?} first_execution={:?} total={combined:?}",
                baseline.0, finalized.0, finalized.1
            );
            let budget = planning_budget(kind);
            for (class, cost) in &finalized.0 {
                let ceiling = budget.get(class).expect("unexpected planning object class");
                assert!(
                    cost.requests <= ceiling.requests && cost.bytes <= ceiling.bytes,
                    "{kind}/{class}: {cost:?} exceeds {ceiling:?}"
                );
            }
            assert_eq!(
                finalized.0, baseline.0,
                "{kind}: finalization exceeded one ordinary builder"
            );
            assert_eq!(finalized.2, baseline.2);
            assert_eq!(
                finalized.2,
                if kind == "overlay" && value != 999 {
                    Vec::<String>::new()
                } else {
                    vec!["p1".into()]
                }
            );
            assert!(
                !finalized.0.contains_key("other"),
                "unexpected planning object class"
            );
            assert!(
                !finalized.0.contains_key("data"),
                "planning must not read row data"
            );
            if kind == "modern" {
                assert!(!finalized.0.contains_key("index"));
            }
            if kind == "overlay" {
                assert!(
                    finalized
                        .0
                        .get("row_ids")
                        .is_some_and(|cost| cost.bytes > 0)
                );
                assert!(
                    finalized
                        .0
                        .get("deletions")
                        .is_some_and(|cost| cost.bytes > 0)
                );
            }
            if kind == "legacy" {
                assert!(
                    finalized
                        .0
                        .get("index")
                        .is_some_and(|cost| cost.requests > 0 && cost.bytes == 0)
                );
            }
        }
    }
}
