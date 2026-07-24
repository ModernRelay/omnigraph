#![cfg(feature = "failpoints")]

//! RFC-026 Phase-B1 private row-admission/fold integration owner.
//!
//! These tests reach the single doc-hidden graph seam. They intentionally do
//! not create a public SDK, schema, HTTP, or CLI streaming contract.

mod helpers;

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use fail::FailScenario;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance::Dataset;
use lance::dataset::mem_wal::{DatasetMemWalExt, ShardWriterConfig};
use lance::index::DatasetIndexExt;
use lance::io::WrappingObjectStore;
use lance_index::mem_wal::MEM_WAL_INDEX_NAME;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    UploadPart,
};
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::OmniError;
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
use serial_test::serial;

use helpers::memwal::CurrentMemWalInventory;

const STREAM_SCHEMA: &str = "node Person { score: I32 }\n";
const TWO_TABLE_STREAM_SCHEMA: &str = r#"
node Person { score: I32 }
node Company { score: I32 }
"#;
const UNIQUE_STREAM_SCHEMA: &str = r#"
node Person {
    score: I32
    @unique(score)
}
"#;
const PAYLOAD_STREAM_SCHEMA: &str = "node Person { payload: String }\n";
const TABLE: &str = "node:Person";
const INSERT_PERSON: &str = r#"
query insert_person($score: I32) {
    insert Person { score: $score }
}
"#;
const INSERT_COMPANY: &str = r#"
query insert_company($score: I32) {
    insert Company { score: $score }
}
"#;

const PROVIDER_FAILURE_MESSAGE: &str = "injected RFC-026 provider exhaustion";
const PROVIDER_PUT_TIMEOUT: Duration = Duration::from_secs(45);
const PROVIDER_FOLD_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderFailureTarget {
    PartialGeneration,
    ShardManifest,
    WalEntry,
}

impl ProviderFailureTarget {
    fn matches(self, location: &ObjectPath) -> bool {
        let path = location.to_string();
        let is_mem_wal = path.split('/').any(|part| part == "_mem_wal");
        if !is_mem_wal {
            return false;
        }
        match self {
            Self::PartialGeneration => {
                generation_root_in_path(&path).is_some() && path.ends_with("/bloom_filter.bin")
            }
            Self::ShardManifest => {
                path.contains("/manifest/") && !path.ends_with("version_hint.json")
            }
            Self::WalEntry => path.contains("/wal/") && path.contains(".arrow"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderAccessKind {
    Put,
    MultipartStart,
    MultipartPart,
    MultipartComplete,
    MultipartAbort,
    Get,
    Delete,
    List,
    ListResult,
    ListWithOffset,
    ListWithDelimiter,
    CopyFrom,
    CopyTo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderAccess {
    kind: ProviderAccessKind,
    path: String,
}

#[derive(Debug, Default)]
struct ProviderFailureState {
    active: Option<ProviderFailureTarget>,
    failed: Vec<(ProviderFailureTarget, String)>,
    accesses: Vec<ProviderAccess>,
    successful_generation_writes: usize,
}

/// Fails real object-store writes made by Lance's detached MemWAL workers.
///
/// `QueryIoProbes` installs this on the table handle before the worker is
/// opened, and OmniGraph propagates that task-local wrapper into detached put
/// and fold owners. This therefore exercises the provider boundary rather than
/// translating an OmniGraph failpoint into a provider-shaped error.
#[derive(Debug, Clone, Default)]
struct ProviderWriteFailure(Arc<Mutex<ProviderFailureState>>);

impl ProviderWriteFailure {
    fn arm(&self, target: ProviderFailureTarget) {
        let mut state = self.0.lock().unwrap();
        state.active = Some(target);
        state.successful_generation_writes = 0;
    }

    fn disarm(&self) {
        self.0.lock().unwrap().active = None;
    }

    fn failed_paths(&self, target: ProviderFailureTarget) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .failed
            .iter()
            .filter(|(actual, _)| *actual == target)
            .map(|(_, path)| path.clone())
            .collect()
    }

    fn access_checkpoint(&self) -> usize {
        self.0.lock().unwrap().accesses.len()
    }

    fn accesses_since(&self, checkpoint: usize) -> Vec<ProviderAccess> {
        self.0.lock().unwrap().accesses[checkpoint..].to_vec()
    }

    fn successful_generation_writes(&self) -> usize {
        self.0.lock().unwrap().successful_generation_writes
    }

    fn record(&self, kind: ProviderAccessKind, location: &ObjectPath) {
        self.0.lock().unwrap().accesses.push(ProviderAccess {
            kind,
            path: location.to_string(),
        });
    }

    fn record_successful_write(&self, location: &ObjectPath) {
        if generation_root_in_path(&location.to_string()).is_some() {
            self.0.lock().unwrap().successful_generation_writes += 1;
        }
    }

    fn record_write_and_maybe_fail(
        &self,
        kind: ProviderAccessKind,
        location: &ObjectPath,
    ) -> Option<object_store::Error> {
        let mut state = self.0.lock().unwrap();
        state.accesses.push(ProviderAccess {
            kind,
            path: location.to_string(),
        });
        let target = state.active?;
        if !target.matches(location) {
            return None;
        }
        state.failed.push((target, location.to_string()));
        Some(object_store::Error::Generic {
            store: "rfc026-provider-failure",
            source: Box::new(std::io::Error::other(PROVIDER_FAILURE_MESSAGE)),
        })
    }

    fn assert_no_access_under_roots_since(
        &self,
        checkpoint: usize,
        roots: &BTreeSet<String>,
        boundary: &str,
    ) {
        let offending = self
            .accesses_since(checkpoint)
            .into_iter()
            .filter(|access| {
                roots
                    .iter()
                    .any(|root| path_is_at_or_under(&access.path, root))
            })
            .collect::<Vec<_>>();
        assert!(
            offending.is_empty(),
            "{boundary} must not descend into, read, write, copy, or delete at/below retained unreferenced roots {roots:?}; observed {offending:?}"
        );
    }
}

impl WrappingObjectStore for ProviderWriteFailure {
    fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(ProviderFailingStore {
            target,
            failure: self.clone(),
        })
    }
}

#[derive(Debug)]
struct ProviderFailingStore {
    target: Arc<dyn ObjectStore>,
    failure: ProviderWriteFailure,
}

impl fmt::Display for ProviderFailingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProviderFailingStore({})", self.target)
    }
}

#[derive(Debug)]
struct ProviderTrackedMultipart {
    target: Box<dyn MultipartUpload>,
    failure: ProviderWriteFailure,
    location: ObjectPath,
}

#[async_trait]
impl MultipartUpload for ProviderTrackedMultipart {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.failure
            .record(ProviderAccessKind::MultipartPart, &self.location);
        self.target.put_part(data)
    }

    async fn complete(&mut self) -> ObjectStoreResult<PutResult> {
        self.failure
            .record(ProviderAccessKind::MultipartComplete, &self.location);
        let result = self.target.complete().await;
        if result.is_ok() {
            self.failure.record_successful_write(&self.location);
        }
        result
    }

    async fn abort(&mut self) -> ObjectStoreResult<()> {
        self.failure
            .record(ProviderAccessKind::MultipartAbort, &self.location);
        self.target.abort().await
    }
}

#[async_trait]
impl ObjectStore for ProviderFailingStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if let Some(error) = self
            .failure
            .record_write_and_maybe_fail(ProviderAccessKind::Put, location)
        {
            return Err(error);
        }
        let result = self.target.put_opts(location, payload, opts).await;
        if result.is_ok() {
            self.failure.record_successful_write(location);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        if let Some(error) = self
            .failure
            .record_write_and_maybe_fail(ProviderAccessKind::MultipartStart, location)
        {
            return Err(error);
        }
        let target = self.target.put_multipart_opts(location, opts).await?;
        Ok(Box::new(ProviderTrackedMultipart {
            target,
            failure: self.failure.clone(),
            location: location.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.failure.record(ProviderAccessKind::Get, location);
        self.target.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        let failure = self.failure.clone();
        self.target.delete_stream(
            locations
                .map(move |location| {
                    if let Ok(location) = &location {
                        failure.record(ProviderAccessKind::Delete, location);
                    }
                    location
                })
                .boxed(),
        )
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.failure.record(
            ProviderAccessKind::List,
            &prefix.cloned().unwrap_or_default(),
        );
        let failure = self.failure.clone();
        self.target
            .list(prefix)
            .map(move |result| {
                if let Ok(metadata) = &result {
                    failure.record(ProviderAccessKind::ListResult, &metadata.location);
                }
                result
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.failure.record(
            ProviderAccessKind::ListWithOffset,
            &prefix.cloned().unwrap_or_default(),
        );
        self.failure
            .record(ProviderAccessKind::ListWithOffset, offset);
        let failure = self.failure.clone();
        self.target
            .list_with_offset(prefix, offset)
            .map(move |result| {
                if let Ok(metadata) = &result {
                    failure.record(ProviderAccessKind::ListResult, &metadata.location);
                }
                result
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.failure.record(
            ProviderAccessKind::ListWithDelimiter,
            &prefix.cloned().unwrap_or_default(),
        );
        let result = self.target.list_with_delimiter(prefix).await?;
        for metadata in &result.objects {
            self.failure
                .record(ProviderAccessKind::ListResult, &metadata.location);
        }
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.failure.record(ProviderAccessKind::CopyFrom, from);
        if let Some(error) = self
            .failure
            .record_write_and_maybe_fail(ProviderAccessKind::CopyTo, to)
        {
            return Err(error);
        }
        let result = self.target.copy_opts(from, to, options).await;
        if result.is_ok() {
            self.failure.record_successful_write(to);
        }
        result
    }
}

async fn mem_wal_inventory(uri: &str) -> CurrentMemWalInventory {
    let inventory = helpers::memwal::current_mem_wal_inventory(uri).await;
    assert!(
        inventory.unknown_paths().is_empty(),
        "provider-failure inventory must fail closed on unknown paths: {:?}",
        inventory.unknown_paths()
    );
    inventory
}

fn generation_root_in_path(path: &str) -> Option<String> {
    let parts = path.split('/').collect::<Vec<_>>();
    let index = parts
        .iter()
        .position(|part| helpers::memwal::is_generation_root(part))?;
    Some(parts[..=index].join("/"))
}

fn path_is_at_or_under(path: &str, root: &str) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn assert_inventory_retained(
    before: &CurrentMemWalInventory,
    after: &CurrentMemWalInventory,
    boundary: &str,
) {
    before.assert_path_class_size_retained_by(after);
    assert!(
        before.generation_roots.is_subset(&after.generation_roots),
        "{boundary} must retain every previously listed generation root"
    );
}

async fn enroll_stream_at(uri: &str) {
    let db = Omnigraph::init(uri, STREAM_SCHEMA)
        .await
        .expect("provider-failure fixture must initialize");
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .expect("provider-failure fixture must enroll its one stream table");
}

async fn init_enrolled() -> (tempfile::TempDir, Arc<Omnigraph>) {
    init_enrolled_with_schema(STREAM_SCHEMA).await
}

async fn init_enrolled_with_schema(schema: &str) -> (tempfile::TempDir, Arc<Omnigraph>) {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Omnigraph::init(dir.path().to_str().unwrap(), schema)
            .await
            .unwrap(),
    );
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    (dir, db)
}

#[tokio::test]
#[serial]
async fn public_snapshot_hides_the_memwal_system_index() {
    let (dir, db) = init_enrolled().await;
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snapshot.entry(TABLE).unwrap();
    let table_uri = format!(
        "{}/{}",
        dir.path().to_str().unwrap().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    let raw = Dataset::open(&table_uri).await.unwrap();
    assert!(
        raw.load_indices()
            .await
            .unwrap()
            .iter()
            .any(|index| index.name == MEM_WAL_INDEX_NAME),
        "the enrolled physical table must actually carry Lance's MemWAL system index"
    );

    let public = snapshot.open(TABLE).await.unwrap();
    assert!(
        public
            .load_indices()
            .await
            .unwrap()
            .iter()
            .all(|index| index.name != MEM_WAL_INDEX_NAME),
        "public index reflection must omit the private MemWAL system index"
    );
}

async fn physical_batch(db: &Omnigraph, rows: &[(String, i32)]) -> RecordBatch {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(TABLE).await.unwrap();
    let schema = Arc::new(Schema::from(table.schema()));
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "score"],
        "fixture must expose the exact normalized physical schema"
    );
    let ids = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|(id, _)| id.as_str()),
    )) as ArrayRef;
    let scores = Arc::new(Int32Array::from_iter_values(
        rows.iter().map(|(_, score)| *score),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, scores]).unwrap()
}

/// Exercise one private B2 compare-and-chain occurrence without exposing its
/// protocol types through the integration-test boundary.
#[allow(clippy::too_many_arguments)]
async fn b2_put_score(
    db: &Arc<Omnigraph>,
    stream_incarnation_id: &str,
    logical_id: &str,
    score: i32,
    caller_ordinal: u64,
    write_id: &str,
    predecessor_token: Option<&str>,
    contributor_id: &str,
) -> Result<String, OmniError> {
    let batch = physical_batch(db, &[(logical_id.to_string(), score)]).await;
    db.failpoint_stream_b2_for_test(
        TABLE,
        batch,
        caller_ordinal,
        stream_incarnation_id,
        write_id,
        predecessor_token,
        contributor_id,
    )
    .await
}

async fn physical_payload_batch(db: &Omnigraph, id: &str, payload_bytes: usize) -> RecordBatch {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(TABLE).await.unwrap();
    let schema = Arc::new(Schema::from(table.schema()));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![id])) as ArrayRef,
            Arc::new(StringArray::from(vec!["x".repeat(payload_bytes)])) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn visible_rows(db: &Omnigraph) -> Vec<(String, i32)> {
    let batches = helpers::read_table(db, TABLE).await;
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row).to_string(), scores.value(row)));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
#[serial]
async fn admission_rejects_empty_and_non_exact_physical_batches_without_visibility() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let exact = physical_batch(&db, &[("shape".to_string(), 1)]).await;
    let empty = RecordBatch::new_empty(exact.schema());
    let empty_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(empty), 0)
        .await
        .expect_err("an admitted B1 call must contain at least one row");
    assert!(
        empty_error.to_string().contains("non-empty"),
        "{empty_error:?}"
    );

    let wrong_schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Int32, false),
        Field::new("id", DataType::Utf8, false),
    ]));
    let wrong = RecordBatch::try_new(
        wrong_schema,
        vec![
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["wrong-shape"])) as ArrayRef,
        ],
    )
    .unwrap();
    let schema_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(wrong), 0)
        .await
        .expect_err("stream admission requires the exact accepted physical schema");
    assert!(
        schema_error.to_string().contains("exactly match"),
        "{schema_error:?}"
    );

    assert!(visible_rows(&db).await.is_empty());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        version_before
    );

    let valid = physical_batch(&db, &[("valid".to_string(), 3)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(valid), 0)
        .await
        .expect("shape rejection must not poison the stream worker");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(visible_rows(&db).await, vec![("valid".to_string(), 3)]);
}

#[tokio::test]
#[serial]
async fn durable_put_is_manifest_invisible_until_one_explicit_fold() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let batch = physical_batch(&db, &[("p1".to_string(), 10)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 41)
        .await
        .expect("watcher success is the private durability acknowledgement");
    assert_eq!(visible_rows(&db).await, Vec::<(String, i32)>::new());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        version_before
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("one generation folds through recovery-v11");
    assert_eq!(visible_rows(&db).await, vec![("p1".to_string(), 10)]);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        version_before + 1
    );
}

#[tokio::test]
#[serial]
async fn fold_resolves_same_id_last_write_wins_before_staging() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;

    for (ordinal, score) in [(0, 10), (1, 20)] {
        let batch = physical_batch(&db, &[("same".to_string(), score)]).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
            .await
            .unwrap();
    }
    assert!(visible_rows(&db).await.is_empty());
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(visible_rows(&db).await, vec![("same".to_string(), 20)]);
}

#[tokio::test]
#[serial]
async fn whole_generation_row_and_byte_caps_refuse_before_a_second_put_effect() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let full = (0..8_192)
        .map(|row| (format!("p{row:04}"), row))
        .collect::<Vec<_>>();
    let full_batch = physical_batch(&db, &full).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(full_batch), 0)
        .await
        .expect("the exact row cap is admissible");

    let over = physical_batch(&db, &[("over".to_string(), 9_999)]).await;
    let error = db
        .failpoint_stream_b1_for_test(TABLE, Some(over), 8_192)
        .await
        .expect_err("one row beyond the complete-generation cap must be effect-free");
    assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    let visible = visible_rows(&db).await;
    assert_eq!(visible.len(), 8_192);
    assert!(!visible.iter().any(|(id, _)| id == "over"));

    // The root memory permit must not turn this same-generation byte crossing
    // into a generic ResourceLimitExceeded before the worker can classify it.
    let (_payload_dir, payload_db) = init_enrolled_with_schema(PAYLOAD_STREAM_SCHEMA).await;
    let first = physical_payload_batch(&payload_db, "within-byte-cap", 30 * 1024 * 1024).await;
    payload_db
        .failpoint_stream_b1_for_test(TABLE, Some(first), 0)
        .await
        .expect("the first payload remains below the post-tombstone byte cap");
    let over = physical_payload_batch(&payload_db, "crosses-byte-cap", 3 * 1024 * 1024).await;
    let error = payload_db
        .failpoint_stream_b1_for_test(TABLE, Some(over), 1)
        .await
        .expect_err("same-generation byte overflow must request a fold");
    assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
    payload_db
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the admitted payload remains foldable after byte-cap refusal");
    let rows = helpers::read_table(&payload_db, TABLE).await;
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[tokio::test]
#[serial]
async fn oversized_first_batch_is_refused_before_detach_or_put_invocation() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let rows = (0..8_193)
        .map(|row| (format!("oversized-{row:04}"), row))
        .collect::<Vec<_>>();
    let oversized = physical_batch(&db, &rows).await;
    let error = {
        let _must_not_reach_put =
            ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(oversized), 0)
            .await
            .expect_err("an oversized first batch must be refused synchronously")
    };
    assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
    assert!(visible_rows(&db).await.is_empty());

    let valid = physical_batch(&db, &[("after-oversized".to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(valid), 0)
        .await
        .expect("pre-detach refusal must not claim or poison the worker");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(
        visible_rows(&db).await,
        vec![("after-oversized".to_string(), 7)]
    );
}

#[tokio::test]
#[serial]
async fn pre_invocation_failure_is_effect_free_and_worker_remains_usable() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let rejected = physical_batch(&db, &[("rejected".to_string(), 1)]).await;

    let error = {
        let _before_invoke = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(rejected), 0)
            .await
            .expect_err("the pre-invocation boundary must reject this batch")
    };
    assert!(
        !matches!(error, OmniError::AckUnknown { .. }),
        "a failure before put_no_wait is effect-free, not acknowledgement-ambiguous: {error:?}"
    );
    assert!(
        error
            .to_string()
            .contains(names::STREAM_B1_BEFORE_PUT_INVOKE),
        "{error:?}"
    );
    assert!(visible_rows(&db).await.is_empty());

    let accepted = physical_batch(&db, &[("accepted".to_string(), 2)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(accepted), 1)
        .await
        .expect("an effect-free rejection must leave the same worker usable");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the later admitted batch must fold normally");
    assert_eq!(visible_rows(&db).await, vec![("accepted".to_string(), 2)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn post_invocation_failure_is_ack_unknown_and_replay_is_fold_only() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let replay_rows = (0..8_192)
        .map(|row| (format!("ambiguous-{row:04}"), row))
        .collect::<Vec<_>>();
    let batch = physical_batch(&db, &replay_rows).await;
    let retired =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_RETIREMENT_RELEASE);

    let error = {
        let _failpoint =
            ScopedFailPoint::new(names::STREAM_B1_AFTER_PUT_INVOKE_BEFORE_WATCHER, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .expect_err("anything after invocation is permanently ambiguous")
    };
    assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
    assert!(visible_rows(&db).await.is_empty());

    retired.wait_until_reached().await;
    retired.release();

    let classified_abort =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_BEFORE_ABORT);
    let classified_retired =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_RETIREMENT_RELEASE);
    let rejected_after_classify =
        physical_batch(&db, &[("rejected-after-classify".to_string(), 70)]).await;
    let classified_error = {
        let _after_classify = ScopedFailPoint::new(
            names::STREAM_B1_AFTER_COLD_CLASSIFY_BEFORE_FINAL_AUTHORITY,
            "return",
        );
        db.failpoint_stream_b1_for_test(TABLE, Some(rejected_after_classify), 10_000)
            .await
            .expect_err("final authority failure must retain the classified replay worker")
    };
    assert!(
        classified_error
            .to_string()
            .contains(names::STREAM_B1_AFTER_COLD_CLASSIFY_BEFORE_FINAL_AUTHORITY),
        "{classified_error:?}"
    );
    classified_abort.wait_until_reached().await;
    let blocked_while_retiring =
        physical_batch(&db, &[("blocked-while-retiring".to_string(), 71)]).await;
    let blocked_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(blocked_while_retiring), 10_001)
        .await
        .expect_err("classified replay must remain fold-only while its abort is parked");
    assert!(
        matches!(blocked_error, OmniError::FoldRequired { .. }),
        "the exact replay marker must outlive final authority failure: {blocked_error:?}"
    );
    classified_abort.release();
    classified_retired.wait_until_reached().await;
    classified_retired.release();

    // Park the first cold caller only after it owns charge, shared admission,
    // and the input queue. Two more callers can then become charged queue
    // waiters before the opener observes durable replay. Replay accounting
    // must be installed immediately, the waiters must drain as FoldRequired,
    // and no caller may be misreported as a generic root ResourceLimit.
    let charged = Arc::new(AtomicUsize::new(0));
    let charged_probe = Arc::clone(&charged);
    let _charged = ScopedFailPoint::with_callback(
        names::STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT,
        move || {
            charged_probe.fetch_add(1, Ordering::SeqCst);
        },
    );
    let before_prepare = helpers::failpoint::Rendezvous::park_first(
        names::STREAM_B1_AFTER_INPUT_QUEUE_BEFORE_PREPARE,
    );
    let mut callers = Vec::new();
    for ordinal in 10_010_u64..=10_012 {
        let caller_db = Arc::clone(&db);
        let caller_batch = physical_batch(
            &db,
            &[(
                format!("must-not-admit-beside-replay-{ordinal}"),
                ordinal as i32,
            )],
        )
        .await;
        callers.push(tokio::spawn(async move {
            caller_db
                .failpoint_stream_b1_for_test(TABLE, Some(caller_batch), ordinal)
                .await
        }));
        if ordinal == 10_010 {
            before_prepare.wait_until_reached().await;
        }
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while charged.load(Ordering::SeqCst) != 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all replay-race callers must be charged before the cold claim resumes");
    before_prepare.release();
    for caller in callers {
        let error = tokio::time::timeout(Duration::from_secs(20), caller)
            .await
            .expect("cold replay opener/waiter did not settle")
            .unwrap()
            .expect_err("replay and an unmerged caller generation may never coexist");
        assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
    }

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("durable replay residue must route fold-only, never admit beside it");
    assert_eq!(visible_rows(&db).await, replay_rows);
}

#[tokio::test]
#[serial]
async fn crash_after_base_effect_repairs_token_effect_then_open_rolls_forward() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("recover".to_string(), 33)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _failpoint = ScopedFailPoint::new(
            names::STREAM_FOLD_POST_BASE_COMMIT_PRE_TOKEN_COMMIT,
            "return",
        );
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the exact base-only effect must retain recovery-v12 ownership")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "the unconfirmed Lance HEAD is not graph-visible"
    );
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("open-time recovery must roll the exact fold forward");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("recover".to_string(), 33)]
    );
}

#[tokio::test]
#[serial]
async fn crash_after_both_effects_reconstructs_confirmation_then_open_rolls_forward() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("recover-both".to_string(), 34)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _failpoint = ScopedFailPoint::new(
            names::STREAM_FOLD_POST_TOKEN_COMMIT_PRE_CONFIRM,
            "return",
        );
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("both exact effects without confirmation must retain recovery-v12 ownership")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "neither exact Lance effect is graph-visible before the manifest CAS"
    );
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("open-time recovery must reconstruct confirmation and roll forward");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("recover-both".to_string(), 34)]
    );
}

#[tokio::test]
#[serial]
async fn fold_sidecar_arm_failure_leaves_no_table_effect_and_retries_the_exact_cut() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let table_version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry(TABLE)
        .unwrap()
        .table_version;
    let batch = physical_batch(&db, &[("arm-failure".to_string(), 34)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _sidecar_write = ScopedFailPoint::new(names::RECOVERY_SIDECAR_WRITE, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("sidecar arm must precede the exact base-table effect")
    };
    assert!(
        error.to_string().contains(names::RECOVERY_SIDECAR_WRITE),
        "{error:?}"
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry(TABLE)
            .unwrap()
            .table_version,
        table_version_before,
        "failed recovery arm must not advance the base-table pointer"
    );
    assert!(visible_rows(&db).await.is_empty());

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the exact flushed cut must remain fold-only and retryable");
    assert_eq!(
        visible_rows(&db).await,
        vec![("arm-failure".to_string(), 34)]
    );
}

#[tokio::test]
#[serial]
async fn fold_confirmation_failure_reconstructs_exact_n_plus_one_on_reopen() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let audit_before = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    let batch = physical_batch(&db, &[("confirm-failure".to_string(), 35)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _confirm = ScopedFailPoint::new(names::RECOVERY_SIDECAR_CONFIRM, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the exact table effect must remain owned while confirmation fails")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1
    );
    assert!(visible_rows(&db).await.is_empty());
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("recovery must reconstruct confirmation from the exact N+1 transaction");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("confirm-failure".to_string(), 35)]
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let audit_after = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(&audit_after[..audit_before.len()], audit_before.as_slice());
    assert_eq!(audit_after.len(), audit_before.len() + 1);
    assert_eq!(
        audit_after.last().map(String::as_str),
        Some("RolledForward")
    );
}

#[tokio::test]
#[serial]
async fn confirmed_fold_refuses_before_publish_then_reopen_rolls_forward() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("pre-publish".to_string(), 36)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _before_publish =
            ScopedFailPoint::new(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("confirmed effects must not become visible around a failed publish")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1
    );
    assert!(visible_rows(&db).await.is_empty());
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("open must publish the fixed confirmed fold");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("pre-publish".to_string(), 36)]
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
}

#[tokio::test]
#[serial]
async fn fold_audit_failure_after_manifest_publish_converges_exactly_once() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let audit_before = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    let batch = physical_batch(&db, &[("post-publish-audit".to_string(), 37)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _audit = ScopedFailPoint::new(names::RECOVERY_RECORD_AUDIT, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("audit failure after publication must remain loudly recoverable")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1
    );
    assert_eq!(
        helpers::recovery::recovery_audit_kinds(dir.path()).await,
        audit_before
    );
    drop(db);

    let reopened = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("re-entry must recognize the fixed visible lineage and finish its audit");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("post-publish-audit".to_string(), 37)]
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let audit_after = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(&audit_after[..audit_before.len()], audit_before.as_slice());
    assert_eq!(audit_after.len(), audit_before.len() + 1);
    assert_eq!(
        audit_after.last().map(String::as_str),
        Some("RolledForward")
    );
    drop(reopened);

    Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("a second recovery pass must be idempotent");
    assert_eq!(
        helpers::recovery::recovery_audit_kinds(dir.path()).await,
        audit_after
    );
}

#[tokio::test]
#[serial]
async fn fold_sidecar_delete_failure_keeps_success_and_next_barrier_cleans_up() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let audit_before = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    let batch = physical_batch(&db, &[("cleanup-retry".to_string(), 38)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    {
        let _delete = ScopedFailPoint::new(names::RECOVERY_SIDECAR_DELETE, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect("cleanup failure after graph visibility must not turn success into ambiguity");
        assert_eq!(
            visible_rows(&db).await,
            vec![("cleanup-retry".to_string(), 38)]
        );
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1
        );
    }

    db.refresh()
        .await
        .expect("the next recovery barrier must consume the already-visible sidecar");
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let audit_after = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(&audit_after[..audit_before.len()], audit_before.as_slice());
    assert_eq!(audit_after.len(), audit_before.len() + 1);
    assert_eq!(
        audit_after.last().map(String::as_str),
        Some("RolledForward")
    );
}

#[tokio::test]
#[serial]
async fn independently_opened_handles_share_one_writer_domain() {
    let _scenario = FailScenario::setup();
    let (dir, first) = init_enrolled().await;
    let second = Arc::new(Omnigraph::open(dir.path().to_str().unwrap()).await.unwrap());

    let left = physical_batch(&first, &[("left".to_string(), 1)]).await;
    let right = physical_batch(&second, &[("right".to_string(), 2)]).await;
    let (left_result, right_result) = tokio::join!(
        first.failpoint_stream_b1_for_test(TABLE, Some(left), 0),
        second.failpoint_stream_b1_for_test(TABLE, Some(right), 1),
    );
    left_result.expect("first handle put");
    right_result.expect("second handle must reuse the root-scoped worker");

    first
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .unwrap();
    assert_eq!(
        visible_rows(&first).await,
        vec![("left".to_string(), 1), ("right".to_string(), 2)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn ordinary_writer_waits_out_the_fold_exclusive_admission_domain() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("streamed".to_string(), 1)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let rendezvous = helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_BEFORE_FORCE_SEAL);
    let fold_db = Arc::clone(&db);
    let fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    rendezvous.wait_until_reached().await;

    let writer_db = Arc::clone(&db);
    let mut ordinary = tokio::spawn(async move {
        writer_db
            .mutate(
                "main",
                INSERT_PERSON,
                "insert_person",
                &helpers::int_params(&[("$score", 99)]),
            )
            .await
    });
    let early = tokio::time::timeout(Duration::from_millis(200), &mut ordinary).await;
    rendezvous.release();
    assert!(
        early.is_err(),
        "ordinary mutation must wait before taking inner write gates while fold owns exclusive admission"
    );

    tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold remained blocked after release")
        .unwrap()
        .unwrap();
    let ordinary_error = ordinary
        .await
        .unwrap()
        .expect_err("OPEN lifecycle still refuses an ordinary base-table writer");
    assert!(
        ordinary_error
            .to_string()
            .contains("stream lifecycle is OPEN"),
        "{ordinary_error:?}"
    );
    assert_eq!(visible_rows(&db).await, vec![("streamed".to_string(), 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn fold_refuses_an_unresolved_main_sidecar_on_another_table() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
    let batch = physical_batch(&db, &[("streamed".to_string(), 1)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    // Park after the fold's initial recovery barrier and exact physical cut,
    // but before it takes the graph-write gates. A different main-table writer
    // can now leave an effected sidecar in precisely that list-to-gate gap.
    let fold_rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR);
    let fold_db = Arc::clone(&db);
    let fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    fold_rendezvous.wait_until_reached().await;

    let mutation_error = {
        let _after_effect = ScopedFailPoint::new(names::MUTATION_POST_TABLE_COMMIT, "return");
        db.mutate(
            "main",
            INSERT_COMPANY,
            "insert_company",
            &helpers::int_params(&[("$score", 7)]),
        )
        .await
        .expect_err("the other-table mutation must leave an effected recovery intent")
    };
    let mutation_operation_id = match mutation_error {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("expected mutation RecoveryRequired, got {other:?}"),
    };
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![mutation_operation_id.clone()]
    );

    fold_rendezvous.release();
    let fold_error = tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold did not leave its graph-write gate")
        .unwrap()
        .expect_err("fold must not publish around another main-table recovery intent");
    match fold_error {
        OmniError::RecoveryRequired { operation_id, .. } => {
            assert_eq!(operation_id, mutation_operation_id)
        }
        other => panic!("expected fold RecoveryRequired, got {other:?}"),
    }
    assert!(visible_rows(&db).await.is_empty());
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![mutation_operation_id]
    );

    drop(db);
    let db = Arc::new(
        Omnigraph::open(dir.path().to_str().unwrap())
            .await
            .expect("the original mutation sidecar must remain recoverable"),
    );
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("fold may publish only after the older intent resolves");
    assert_eq!(visible_rows(&db).await, vec![("streamed".to_string(), 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn put_corridor_is_bounded_and_one_order_prevents_fold_cycle() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("claimed-before-drain".to_string(), 4)]).await;
    let mut queued_batches = Vec::new();
    for ordinal in 1_u64..=33 {
        queued_batches.push((
            ordinal,
            physical_batch(
                &db,
                &[(format!("bounded-waiter-{ordinal}"), ordinal as i32)],
            )
            .await,
        ));
    }
    let rendezvous = helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_BEFORE_PUT_INVOKE);

    let put_db = Arc::clone(&db);
    let put = tokio::spawn(async move {
        put_db
            .failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
    });
    rendezvous.wait_until_reached().await;

    // The parked first put owns the per-key input corridor. Thirty-one more
    // callers may enter the configured 32-call/32-MiB bounded corridor; the
    // next two must fail loudly instead of accumulating as unaccounted batch
    // futures behind the queue or fold gate.
    let charged_waiters = Arc::new(AtomicUsize::new(0));
    let charged_waiters_probe = Arc::clone(&charged_waiters);
    let _charged_waiter_probe = ScopedFailPoint::with_callback(
        names::STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT,
        move || {
            charged_waiters_probe.fetch_add(1, Ordering::SeqCst);
        },
    );
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
    for (ordinal, batch) in queued_batches {
        let put_db = Arc::clone(&db);
        let result_tx = result_tx.clone();
        tokio::spawn(async move {
            let result = put_db
                .failpoint_stream_b1_for_test(TABLE, Some(batch), ordinal)
                .await;
            let _ = result_tx.send((ordinal, result));
        });
    }
    drop(result_tx);

    let mut rejected_ordinals = Vec::new();
    for _ in 0..2 {
        let (ordinal, result) = tokio::time::timeout(Duration::from_secs(20), result_rx.recv())
            .await
            .expect("queued put burst did not reach its in-flight bound")
            .expect("queued put result channel closed before the bound was exercised");
        match result {
            Err(OmniError::ResourceLimitExceeded {
                resource,
                limit: 32,
                actual: 33,
            }) if resource == "stream_inflight_calls" => rejected_ordinals.push(ordinal),
            other => panic!("expected bounded waiter refusal, got {other:?}"),
        }
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        while charged_waiters.load(Ordering::SeqCst) != 31 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("every admitted waiter must be charged and inside shared admission");

    let fold_db = Arc::clone(&db);
    let mut fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    let early = tokio::time::timeout(Duration::from_millis(200), &mut fold).await;
    assert!(
        early.is_err(),
        "an exclusive fold must wait for the cold claimant's shared admission lease"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "neither a claimed writer nor a pending fold may bypass manifest visibility"
    );

    rendezvous.release();
    tokio::time::timeout(Duration::from_secs(20), put)
        .await
        .expect("cold put remained parked after rendezvous release")
        .unwrap()
        .expect("cold claimant must complete its durability watcher");

    let mut admitted_ordinals = Vec::new();
    for _ in 0..31 {
        let (ordinal, result) = tokio::time::timeout(Duration::from_secs(20), result_rx.recv())
            .await
            .expect("bounded queued put did not drain after rendezvous release")
            .expect("queued put result channel closed before every admitted input settled");
        result.expect("every charged waiter inside the bound must durably settle");
        admitted_ordinals.push(ordinal);
    }
    assert!(result_rx.recv().await.is_none());
    tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold remained blocked after the claimant released shared admission")
        .unwrap()
        .expect("fold must cut and publish the generation admitted before it");

    admitted_ordinals.sort_unstable();
    rejected_ordinals.sort_unstable();
    assert_eq!(admitted_ordinals.len(), 31);
    assert_eq!(rejected_ordinals.len(), 2);
    let mut expected = vec![("claimed-before-drain".to_string(), 4)];
    expected.extend(admitted_ordinals.into_iter().map(|ordinal| {
        (
            format!("bounded-waiter-{ordinal}"),
            i32::try_from(ordinal).unwrap(),
        )
    }));
    expected.sort();
    assert_eq!(visible_rows(&db).await, expected);

    // Every put now acquires shared admission before the same-key input
    // queue. Park an owner at that exact boundary, queue an exclusive fold,
    // then add another put behind the fair/write-preferring admission lock.
    // The owner must still take the free input queue and finish, allowing the
    // fold and then the later put to settle in order. A queue-first owner here
    // recreates the historical owner -> fold -> waiter -> owner cycle.
    drop(_charged_waiter_probe);
    let ordered_owner = physical_batch(&db, &[("ordered-owner".to_string(), 101)]).await;
    let ordered_waiter = physical_batch(&db, &[("ordered-waiter".to_string(), 102)]).await;
    let ordered_rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT);
    let owner_db = Arc::clone(&db);
    let owner = tokio::spawn(async move {
        owner_db
            .failpoint_stream_b1_for_test(TABLE, Some(ordered_owner), 100)
            .await
    });
    ordered_rendezvous.wait_until_reached().await;

    let fold_db = Arc::clone(&db);
    let mut ordered_fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut ordered_fold)
            .await
            .is_err(),
        "the fold must queue behind the owner's shared admission"
    );

    let waiter_db = Arc::clone(&db);
    let mut waiter = tokio::spawn(async move {
        waiter_db
            .failpoint_stream_b1_for_test(TABLE, Some(ordered_waiter), 101)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut waiter)
            .await
            .is_err(),
        "the later put must wait behind the already-queued exclusive fold"
    );

    ordered_rendezvous.release();
    tokio::time::timeout(Duration::from_secs(20), owner)
        .await
        .expect("shared-first owner deadlocked before the input queue")
        .unwrap()
        .expect("shared-first owner must durably settle");
    let fold_error = tokio::time::timeout(Duration::from_secs(20), ordered_fold)
        .await
        .expect("exclusive fold deadlocked behind the shared-first owner")
        .unwrap()
        .expect_err("the charged post-fold waiter must keep the full-cap fold reservation honest");
    assert!(
        matches!(
            fold_error,
            OmniError::ResourceLimitExceeded { ref resource, .. }
                if resource == "stream_reserved_arrow_bytes"
        ),
        "the effect-free fold retry must be a typed Arrow-budget refusal: {fold_error:?}"
    );
    tokio::time::timeout(Duration::from_secs(20), waiter)
        .await
        .expect("later put remained blocked after the exclusive fold")
        .unwrap()
        .expect("later put must join the still-open generation after the effect-free fold refusal");

    let after_ordered_fold = visible_rows(&db).await;
    assert!(
        !after_ordered_fold
            .iter()
            .any(|(id, _)| id == "ordered-owner" || id == "ordered-waiter"),
        "the effect-free fold refusal must leave the complete generation manifest-invisible"
    );
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the full generation must remain foldable after queued charge drains");
    let ordered_rows = visible_rows(&db).await;
    assert!(
        ordered_rows
            .iter()
            .any(|(id, score)| id == "ordered-owner" && *score == 101)
    );
    assert!(
        ordered_rows
            .iter()
            .any(|(id, score)| id == "ordered-waiter" && *score == 102)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn abort_stall_keeps_original_retirement_and_admission_until_exact_abort_settles() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("owned-by-original-abort".to_string(), 8)]).await;
    let abort_rendezvous = helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_ABORT_STALL);

    let error = {
        let _after_durable = ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .expect_err("lost acknowledgement must retire through the owned abort task")
    };
    assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
    abort_rendezvous.wait_until_reached().await;

    let second = physical_batch(&db, &[("must-not-reopen".to_string(), 9)]).await;
    let second_error = tokio::time::timeout(
        Duration::from_secs(5),
        db.failpoint_stream_b1_for_test(TABLE, Some(second), 1),
    )
    .await
    .expect("a second put should refuse the retired slot, not wait forever")
    .expect_err("the stalled original abort must prevent a second writer claim");
    assert!(
        second_error
            .to_string()
            .contains("original abort completion has not settled"),
        "{second_error:?}"
    );

    let fold_db = Arc::clone(&db);
    let mut fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    let early = tokio::time::timeout(Duration::from_millis(200), &mut fold).await;
    assert!(
        early.is_err(),
        "exclusive fold must remain outside the shared admission domain held by original retirement"
    );
    assert!(visible_rows(&db).await.is_empty());

    abort_rendezvous.release();
    tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold did not resume after the exact original abort settled")
        .unwrap()
        .expect("durable residue must cold-reopen fold-only after retirement settles");
    assert_eq!(
        visible_rows(&db).await,
        vec![("owned-by-original-abort".to_string(), 8)],
        "the refused second put must not appear and the first durable residue folds once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn post_watcher_epoch_loss_is_ack_unknown_not_a_clean_ack() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("fenced-after-durable".to_string(), 18)]).await;
    let after_watcher =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_WATCHER_SUCCESS);

    let put_db = Arc::clone(&db);
    let put = tokio::spawn(async move {
        put_db
            .failpoint_stream_b1_for_test(TABLE, Some(batch), 23)
            .await
    });
    after_watcher.wait_until_reached().await;

    // The first writer has received watcher success but has not crossed
    // OmniGraph's post-durability fence check. Claim the same physical shard
    // directly through Lance so the old writer loses its epoch in that exact
    // window. This is intentionally a test-only foreign-writer injector.
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snapshot.entry(TABLE).unwrap();
    let table_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    let dataset = Dataset::open(&table_uri).await.unwrap();
    let shard_ids = dataset.list_mem_wal_latest_shard_ids().await.unwrap();
    assert_eq!(
        shard_ids.len(),
        1,
        "B1's unsharded topology must expose exactly one claimed shard"
    );
    let shard_id = shard_ids[0];
    let successor = dataset
        .mem_wal_writer(
            shard_id,
            ShardWriterConfig::new(shard_id)
                .with_shard_spec_id(1)
                .with_durable_write(true),
        )
        .await
        .expect("the foreign successor must durably claim the next writer epoch");
    assert!(
        successor.epoch() > 1,
        "the foreign writer must claim a successor epoch, got {}",
        successor.epoch()
    );

    after_watcher.release();
    let error = tokio::time::timeout(Duration::from_secs(20), put)
        .await
        .expect("the fenced put did not settle after release")
        .unwrap()
        .expect_err("epoch loss after watcher success must never return a clean acknowledgement");
    match error {
        OmniError::AckUnknown {
            caller_ordinal_start,
            caller_ordinal_end,
            reason,
            ..
        } => {
            assert_eq!((caller_ordinal_start, caller_ordinal_end), (23, 23));
            assert!(
                reason.contains("post-durability writer fence check failed")
                    && reason.contains("peer claimed epoch"),
                "AckUnknown must retain the exact post-watcher epoch-loss cause: {reason}"
            );
        }
        other => panic!("post-watcher epoch loss must be AckUnknown, got {other:?}"),
    }
    assert!(
        visible_rows(&db).await.is_empty(),
        "watcher success alone must not make the row graph-visible"
    );

    successor
        .abort()
        .await
        .expect("the test-only successor must retire cleanly");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the ambiguous durable row must remain replayable and fold exactly once");
    assert_eq!(
        visible_rows(&db).await,
        vec![("fenced-after-durable".to_string(), 18)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cancelling_request_after_invocation_does_not_cancel_durable_worker_ownership() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("cancelled-caller".to_string(), 17)]).await;
    let after_watcher =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_WATCHER_SUCCESS);

    let put_db = Arc::clone(&db);
    let put = tokio::spawn(async move {
        put_db
            .failpoint_stream_b1_for_test(TABLE, Some(batch), 12)
            .await
    });
    after_watcher.wait_until_reached().await;
    put.abort();
    assert!(put.await.unwrap_err().is_cancelled());

    let fold_db = Arc::clone(&db);
    let mut fold =
        tokio::spawn(async move { fold_db.failpoint_stream_b1_for_test(TABLE, None, 0).await });
    let early = tokio::time::timeout(Duration::from_millis(200), &mut fold).await;
    assert!(
        early.is_err(),
        "exclusive fold must remain blocked while the detached owner still holds the post-watcher fence boundary"
    );

    after_watcher.release();
    tokio::time::timeout(Duration::from_secs(20), fold)
        .await
        .expect("fold must wait for the detached fence/ack owner, not hang")
        .unwrap()
        .expect("watcher-success residue must reopen fold-only and publish once");
    assert_eq!(
        visible_rows(&db).await,
        vec![("cancelled-caller".to_string(), 17)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn repeated_replay_reseal_before_generation_manifest_does_not_multiply_rows() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("replayed-once".to_string(), 21)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    // Each failure happens after a cold claim/replay has reconstructed the
    // active prefix but before that attempt can seal a generation manifest.
    // The third attempt therefore exercises the replay-watermark bridge after
    // two consecutive restart classifications of the same durable WAL rows.
    let fail_before_manifest = ScopedFailPoint::new(names::STREAM_B1_BEFORE_FORCE_SEAL, "2*return");
    for cycle in 0..2 {
        let error = db
            .failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the pre-generation-manifest crash must interrupt this fold");
        assert!(
            error
                .to_string()
                .contains(names::STREAM_B1_BEFORE_FORCE_SEAL),
            "cycle {cycle}: {error:?}"
        );
    }
    drop(fail_before_manifest);

    tokio::time::timeout(
        Duration::from_secs(20),
        db.failpoint_stream_b1_for_test(TABLE, None, 0),
    )
    .await
    .expect("final replay/reseal did not settle")
    .expect("watermark bridge must let the retained WAL prefix fold once");
    assert_eq!(
        visible_rows(&db).await,
        vec![("replayed-once".to_string(), 21)],
        "repeated replay/reseal must not append the same WAL prefix again"
    );
}

#[tokio::test]
#[serial]
async fn post_force_seal_failure_is_typed_recovery_and_restarts_from_exact_cut() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let batch = physical_batch(&db, &[("sealed-before-sidecar".to_string(), 31)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _after_seal = ScopedFailPoint::new(names::STREAM_B1_AFTER_FORCE_SEAL, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("a post-seal failure has an ambiguous generation effect")
    };
    match error {
        OmniError::RecoveryRequired {
            operation_id,
            reason,
        } => {
            assert!(operation_id.starts_with("stream-cut:"), "{operation_id}");
            assert!(reason.contains("pre-sidecar MemWAL cut"), "{reason}");
        }
        other => panic!("post-seal ambiguity must be typed RecoveryRequired: {other:?}"),
    }
    assert!(
        visible_rows(&db).await.is_empty(),
        "sealing a generation is not graph publication"
    );

    tokio::time::timeout(
        Duration::from_secs(20),
        db.failpoint_stream_b1_for_test(TABLE, None, 0),
    )
    .await
    .expect("retained cut owner did not settle")
    .expect("the next fold must classify and publish the exact retained cut");
    assert_eq!(
        visible_rows(&db).await,
        vec![("sealed-before-sidecar".to_string(), 31)]
    );
}

#[tokio::test]
#[serial]
async fn flushed_unmerged_generation_resumes_fold_only_and_refuses_a_second_generation() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let first = physical_batch(&db, &[("first-generation".to_string(), 1)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(first), 0)
        .await
        .unwrap();

    let error = {
        let _pre_sidecar =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the exact flushed generation must survive before sidecar arm")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR),
        "{error:?}"
    );
    assert!(visible_rows(&db).await.is_empty());

    let second = physical_batch(&db, &[("second-generation".to_string(), 2)]).await;
    let second_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(second), 1)
        .await
        .expect_err("an unmerged flushed generation must route reopen fold-only");
    assert!(
        matches!(second_error, OmniError::FoldRequired { .. }),
        "{second_error:?}"
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the same flushed generation must resume and publish");
    assert_eq!(
        visible_rows(&db).await,
        vec![("first-generation".to_string(), 1)]
    );
}

#[tokio::test]
#[serial]
async fn strict_fold_validation_failure_keeps_manifest_old_and_stream_fold_only() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Omnigraph::init(dir.path().to_str().unwrap(), UNIQUE_STREAM_SCHEMA)
            .await
            .unwrap(),
    );
    db.mutate(
        "main",
        INSERT_PERSON,
        "insert_person",
        &helpers::int_params(&[("$score", 7)]),
    )
    .await
    .expect("seed the committed uniqueness conflict before enrollment");
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    let manifest_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let duplicate = physical_batch(&db, &[("duplicate-id".to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
        .await
        .expect("base-dependent uniqueness is fold-time work");
    let fold_error = db
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect_err("strict fold must reject a committed uniqueness conflict");
    assert!(fold_error.to_string().contains("unique"), "{fold_error:?}");
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before,
        "validation failure occurs before sidecar, table effect, or manifest CAS"
    );
    let visible = visible_rows(&db).await;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].1, 7);

    let correction = physical_batch(&db, &[("later".to_string(), 8)]).await;
    let correction_error = db
        .failpoint_stream_b1_for_test(TABLE, Some(correction), 1)
        .await
        .expect_err("B1 admits no correction generation beside strict-blocked input");
    assert!(
        matches!(correction_error, OmniError::FoldRequired { .. }),
        "{correction_error:?}"
    );
}

#[tokio::test]
#[serial]
async fn b2_happy_fold_exact_retry_and_typed_conflicts_are_effect_free() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let first_write = "11111111-1111-4111-8111-111111111111";
    let token = b2_put_score(
        &db,
        &incarnation,
        "same-key",
        1,
        7,
        first_write,
        None,
        "agent:a",
    )
    .await
    .expect("one B2 occurrence must cross the complete durability boundary");
    assert!(visible_rows(&db).await.is_empty());
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the attributed occurrence must fold through recovery-v12");
    assert_eq!(visible_rows(&db).await, vec![("same-key".to_string(), 1)]);
    let manifest_after_fold = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    // Every outcome below is decided from durable token/base authority. If
    // any path reaches Lance, the failpoint turns the mistake into a loud test
    // failure instead of letting an accidental second WAL put pass unnoticed.
    let _must_not_invoke_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");

    let retry = b2_put_score(
        &db,
        &incarnation,
        "same-key",
        1,
        7,
        first_write,
        None,
        "agent:a",
    )
    .await
    .expect("an exact durable retry must be idempotent without a second put");
    assert_eq!(retry, token);

    let binding_error = b2_put_score(
        &db,
        "22222222-2222-4222-8222-222222222222",
        "binding-conflict",
        2,
        8,
        "33333333-3333-4333-8333-333333333333",
        None,
        "agent:a",
    )
    .await
    .expect_err("a stale stream incarnation must fail before WAL invocation");
    match binding_error {
        OmniError::StreamBindingChanged {
            current_stream_incarnation_id,
            ..
        } => assert_eq!(current_stream_incarnation_id, incarnation),
        other => panic!("expected StreamBindingChanged, got {other:?}"),
    }

    let sequence_error = b2_put_score(
        &db,
        &incarnation,
        "same-key",
        2,
        8,
        "44444444-4444-4444-8444-444444444444",
        None,
        "agent:a",
    )
    .await
    .expect_err("a missing predecessor must fail before WAL invocation");
    match sequence_error {
        OmniError::StreamSequenceConflict {
            logical_id,
            current_token,
            ..
        } => {
            assert_eq!(logical_id, "same-key");
            assert_eq!(current_token.as_deref(), Some(token.as_str()));
        }
        other => panic!("expected StreamSequenceConflict, got {other:?}"),
    }

    let idempotency_error = b2_put_score(
        &db,
        &incarnation,
        "same-key",
        99,
        7,
        first_write,
        None,
        "agent:a",
    )
    .await
    .expect_err("reusing one occurrence for another payload must fail before WAL invocation");
    match idempotency_error {
        OmniError::StreamIdempotencyConflict {
            logical_id,
            current_token,
            ..
        } => {
            assert_eq!(logical_id, "same-key");
            assert_eq!(current_token, token);
        }
        other => panic!("expected StreamIdempotencyConflict, got {other:?}"),
    }

    assert_eq!(visible_rows(&db).await, vec![("same-key".to_string(), 1)]);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_after_fold,
        "exact retries and typed conflicts must be graph-effect-free"
    );
}

#[tokio::test]
#[serial]
async fn b2_same_generation_chain_folds_only_the_latest_value() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();

    let first = b2_put_score(
        &db,
        &incarnation,
        "chained",
        10,
        20,
        "55555555-5555-4555-8555-555555555555",
        None,
        "agent:a",
    )
    .await
    .unwrap();
    let second = b2_put_score(
        &db,
        &incarnation,
        "chained",
        20,
        21,
        "66666666-6666-4666-8666-666666666666",
        Some(&first),
        "agent:b",
    )
    .await
    .expect("the next occurrence may chain from the confirmed in-generation token");
    assert_ne!(first, second);
    assert!(visible_rows(&db).await.is_empty());

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("one fold must publish the latest chained winner and token together");
    assert_eq!(visible_rows(&db).await, vec![("chained".to_string(), 20)]);
}

#[tokio::test]
#[serial]
async fn b2_ack_unknown_retry_cannot_overwrite_a_later_winner() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let x_write = "99999999-9999-4999-8999-999999999991";
    let y_write = "99999999-9999-4999-8999-999999999992";
    let retired =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_RETIREMENT_RELEASE);

    let error = {
        let _after_invoke =
            ScopedFailPoint::new(names::STREAM_B1_AFTER_PUT_INVOKE_BEFORE_WATCHER, "return");
        b2_put_score(
            &db,
            &incarnation,
            "ambiguous-chain",
            10,
            40,
            x_write,
            None,
            "agent:x",
        )
        .await
        .expect_err("post-invocation failure must be AckUnknown")
    };
    let candidate_x = match error {
        OmniError::AckUnknown {
            admission_attempt_id,
            logical_write_ids,
            unconfirmed_candidate_token,
            ..
        } => {
            let attempt = admission_attempt_id.expect("B2 ambiguity carries its attempt id");
            assert_eq!(attempt.len(), 36, "attempt id must be canonical UUID text");
            assert_eq!(
                attempt
                    .bytes()
                    .enumerate()
                    .filter_map(|(index, byte)| (byte == b'-').then_some(index))
                    .collect::<Vec<_>>(),
                [8, 13, 18, 23],
                "attempt id must be canonical UUID text"
            );
            assert_eq!(logical_write_ids, vec![x_write.to_string()]);
            unconfirmed_candidate_token.expect("B2 ambiguity carries its candidate token")
        }
        other => panic!("expected AckUnknown, got {other:?}"),
    };
    assert!(visible_rows(&db).await.is_empty());

    // Let the detached watcher/abort owner settle before cold replay opens the
    // exact durable prefix. The candidate above remains correlation only until
    // this fold publishes both base and token authority.
    retired.wait_until_reached().await;
    retired.release();
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the ambiguous X occurrence must replay and fold exactly once");

    {
        let _must_not_put =
            ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
        let exact_x = b2_put_score(
            &db,
            &incarnation,
            "ambiguous-chain",
            10,
            40,
            x_write,
            None,
            "agent:x",
        )
        .await
        .expect("the exact retry becomes provably durable after fold");
        assert_eq!(exact_x, candidate_x);
    }

    let token_y = b2_put_score(
        &db,
        &incarnation,
        "ambiguous-chain",
        20,
        41,
        y_write,
        Some(&candidate_x),
        "agent:y",
    )
    .await
    .expect("Y must compare-and-chain from X's now-durable token");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("Y must fold and become the current winner");

    let stale_x = {
        let _must_not_put =
            ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
        b2_put_score(
            &db,
            &incarnation,
            "ambiguous-chain",
            10,
            40,
            x_write,
            None,
            "agent:x",
        )
        .await
        .expect_err("an old exact occurrence cannot displace a later winner")
    };
    match stale_x {
        OmniError::StreamSequenceConflict {
            current_token, ..
        } => assert_eq!(current_token.as_deref(), Some(token_y.as_str())),
        other => panic!("stale X must be a StreamSequenceConflict, got {other:?}"),
    }
    assert_eq!(
        visible_rows(&db).await,
        vec![("ambiguous-chain".to_string(), 20)],
        "the ambiguous retry must never overwrite Y"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn b2_revalidates_authority_after_waiting_for_shared_admission() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B2_AFTER_PROVISIONAL_AUTHORITY);

    // X captures an empty token/base authority, then parks before it owns
    // shared admission or the same-key queue.
    let stale_db = Arc::clone(&db);
    let stale_incarnation = incarnation.clone();
    let stale = tokio::spawn(async move {
        b2_put_score(
            &stale_db,
            &stale_incarnation,
            "raced",
            1,
            30,
            "77777777-7777-4777-8777-777777777777",
            None,
            "agent:x",
        )
        .await
    });
    rendezvous.wait_until_reached().await;

    // Y is the only live owner. It durably appends and folds while X remains
    // outside the admission domain, changing both base and token authority.
    let winner = b2_put_score(
        &db,
        &incarnation,
        "raced",
        2,
        31,
        "88888888-8888-4888-8888-888888888888",
        None,
        "agent:y",
    )
    .await
    .expect("the second arrival must pass the park-first rendezvous");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("Y must publish before X acquires shared admission");

    let must_not_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
    rendezvous.release();
    let stale_error = tokio::time::timeout(Duration::from_secs(20), stale)
        .await
        .expect("stale X did not resume after rendezvous release")
        .unwrap()
        .expect_err("X must classify against Y's final authority, not its provisional snapshot");
    drop(must_not_put);
    match stale_error {
        OmniError::StreamSequenceConflict {
            logical_id,
            current_token,
            ..
        } => {
            assert_eq!(logical_id, "raced");
            assert_eq!(current_token.as_deref(), Some(winner.as_str()));
        }
        other => panic!("stale X must return StreamSequenceConflict, got {other:?}"),
    }
    assert_eq!(
        visible_rows(&db).await,
        vec![("raced".to_string(), 2)],
        "the stale provisional capture must neither overwrite nor re-append Y"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn provider_write_failures_preserve_acknowledged_rows_and_type_outcomes_local() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();
    enroll_stream_at(&uri).await;

    let failure = ProviderWriteFailure::default();
    let probes = QueryIoProbes {
        table_wrapper: Some(Arc::new(failure.clone())),
        ..Default::default()
    };
    with_query_io_probes(probes, async {
        let db = Arc::new(
            Omnigraph::open(&uri)
                .await
                .expect("the enrolled fixture must reopen under the provider wrapper"),
        );
        let version_before = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version();

        // A cold writer must first durably claim its epoch in the shard
        // manifest. Failing that provider write happens before put_no_wait, so
        // this refusal must remain effect-free and must not become AckUnknown.
        failure.arm(ProviderFailureTarget::ShardManifest);
        let rejected = physical_batch(&db, &[("claim-refused".to_string(), 1)]).await;
        let error = tokio::time::timeout(
            PROVIDER_PUT_TIMEOUT,
            db.failpoint_stream_b1_for_test(TABLE, Some(rejected), 0),
        )
        .await
        .expect("the failed cold claim must settle")
        .expect_err("provider exhaustion must refuse the cold claim");
        failure.disarm();
        assert!(matches!(error, OmniError::Lance(_)), "{error:?}");
        assert!(
            !matches!(error, OmniError::AckUnknown { .. }),
            "a failed pre-invocation claim cannot be acknowledgement-ambiguous"
        );
        assert!(
            error.to_string().contains(PROVIDER_FAILURE_MESSAGE),
            "the typed substrate error must retain its provider cause: {error:?}"
        );
        assert!(
            !failure
                .failed_paths(ProviderFailureTarget::ShardManifest)
                .is_empty(),
            "the wrapper must have reached the detached cold-claim manifest write"
        );
        assert!(visible_rows(&db).await.is_empty());
        assert_eq!(
            db.snapshot_of(ReadTarget::branch("main"))
                .await
                .unwrap()
                .version(),
            version_before,
            "provider refusal before put invocation cannot advance graph visibility"
        );

        // Establish one clean durability acknowledgement, then fail the next
        // real WAL write. Lance has already accepted the second invocation into
        // its asynchronous writer machinery, so OmniGraph must return
        // AckUnknown while preserving the first durable row for replay.
        let acknowledged = physical_batch(&db, &[("acknowledged".to_string(), 2)]).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(acknowledged), 1)
            .await
            .expect("the first row must cross the complete durability boundary");

        failure.arm(ProviderFailureTarget::WalEntry);
        let ambiguous = physical_batch(&db, &[("wal-refused".to_string(), 3)]).await;
        let error = tokio::time::timeout(
            PROVIDER_PUT_TIMEOUT,
            db.failpoint_stream_b1_for_test(TABLE, Some(ambiguous), 2),
        )
        .await
        .expect("the failed WAL persistence retries must settle")
        .expect_err("a failed post-invocation WAL write cannot acknowledge cleanly");
        failure.disarm();
        assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
        assert!(
            error.to_string().contains(PROVIDER_FAILURE_MESSAGE),
            "AckUnknown must retain the provider failure cause: {error:?}"
        );
        assert!(
            !failure
                .failed_paths(ProviderFailureTarget::WalEntry)
                .is_empty(),
            "the wrapper must have reached the detached WAL persistence task"
        );
        assert!(
            visible_rows(&db).await.is_empty(),
            "neither a clean WAL ack nor a failed WAL invocation is graph publication"
        );

        tokio::time::timeout(
            PROVIDER_FOLD_TIMEOUT,
            db.failpoint_stream_b1_for_test(TABLE, None, 0),
        )
        .await
        .expect("provider recovery fold must settle")
        .expect("the earlier durable WAL row must remain replayable");
        assert_eq!(
            visible_rows(&db).await,
            vec![("acknowledged".to_string(), 2)],
            "provider exhaustion must neither lose a prior clean ack nor invent durability for the failed WAL write"
        );
        assert_eq!(
            db.snapshot_of(ReadTarget::branch("main"))
                .await
                .unwrap()
                .version(),
            version_before + 1,
            "only the recovery fold's manifest CAS may make the acknowledged row visible"
        );
    })
    .await;
}

async fn provider_generation_failure_retains_unreferenced_generation_at(
    uri: &str,
    target: ProviderFailureTarget,
) {
    enroll_stream_at(uri).await;
    let failure = ProviderWriteFailure::default();
    let probes = QueryIoProbes {
        table_wrapper: Some(Arc::new(failure.clone())),
        ..Default::default()
    };

    with_query_io_probes(probes, async {
        let db = Arc::new(
            Omnigraph::open(uri)
                .await
                .expect("the enrolled fixture must reopen under the provider wrapper"),
        );
        let version_before = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version();
        let row = physical_batch(&db, &[("retained-after-failure".to_string(), 41)]).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
            .await
            .expect("the row must be durably acknowledged before the fold failure");
        let before_failure = mem_wal_inventory(uri).await;
        assert!(
            before_failure.generation_roots.is_empty(),
            "the fixture must begin with WAL only and no flushed generation"
        );

        // The complete-output cell fails the shard-manifest publication after
        // every generation object exists. The partial-output cell fails the
        // later bloom write after the generation's Lance data objects exist but
        // before the MemWAL generation envelope is complete. Neither path has
        // shard-manifest authority for its randomized root.
        failure.arm(target);
        let error = tokio::time::timeout(
            PROVIDER_FOLD_TIMEOUT,
            db.failpoint_stream_b1_for_test(TABLE, None, 0),
        )
        .await
        .expect("the failed generation publication must settle")
        .expect_err("a provider failure after the physical cut must block the fold");
        failure.disarm();
        match error {
            OmniError::RecoveryRequired {
                operation_id,
                reason,
            } => {
                assert!(operation_id.starts_with("stream-cut:"), "{operation_id}");
                assert!(reason.contains(PROVIDER_FAILURE_MESSAGE), "{reason}");
            }
            other => panic!("post-cut provider failure must require recovery: {other:?}"),
        }
        assert!(
            !failure.failed_paths(target).is_empty(),
            "the wrapper must reach the selected provider failure after generation output"
        );
        if target == ProviderFailureTarget::PartialGeneration {
            assert!(
                failure.successful_generation_writes() > 0,
                "partial-generation injection must follow at least one successful generation object write"
            );
            assert!(
                failure
                    .failed_paths(target)
                    .iter()
                    .all(|path| path.ends_with("/bloom_filter.bin")),
                "the partial-generation cell must fail the deterministic post-data bloom boundary"
            );
        }
        assert!(visible_rows(&db).await.is_empty());
        assert_eq!(
            db.snapshot_of(ReadTarget::branch("main"))
                .await
                .unwrap()
                .version(),
            version_before,
            "a flushed generation is not graph-visible without the graph manifest CAS"
        );

        let after_failure = mem_wal_inventory(uri).await;
        assert_inventory_retained(
            &before_failure,
            &after_failure,
            "failed generation publication",
        );
        let failed_roots = after_failure.generation_roots.clone();
        assert!(
            !failed_roots.is_empty(),
            "generation output must exist before its injected shard-manifest failure"
        );
        let referenced_after_failure = after_failure.referenced_generation_roots.clone();
        assert!(
            failed_roots.is_disjoint(&referenced_after_failure),
            "a provider-refused generation publication must not authorize its randomized output: failed={failed_roots:?}, referenced={referenced_after_failure:?}"
        );
        if target == ProviderFailureTarget::PartialGeneration {
            for root in &failed_roots {
                assert!(
                    !after_failure
                        .objects
                        .contains_key(&format!("{root}/bloom_filter.bin")),
                    "the failed bloom write must leave a physically incomplete generation envelope"
                );
                assert!(
                    !after_failure
                        .objects
                        .keys()
                        .any(|path| path_is_at_or_under(path, &format!("{root}/_pk_index"))),
                    "failure before the PK sidecar must leave a physically incomplete generation envelope"
                );
            }
        }
        // From this point the orphan roots are completely identified. The
        // independent inventory reads above intentionally bypass this engine
        // wrapper; every subsequent engine access remains in this log.
        let orphan_access_checkpoint = failure.access_checkpoint();

        // The retained WAL/cut owns progress. A new row cannot open a side
        // generation around it; recovery must fold the exact durable prefix.
        let blocked = physical_batch(&db, &[("must-not-pass-recovery".to_string(), 42)]).await;
        let blocked_error = tokio::time::timeout(
            PROVIDER_PUT_TIMEOUT,
            db.failpoint_stream_b1_for_test(TABLE, Some(blocked), 1),
        )
            .await
            .expect("fold-only admission refusal must settle")
            .expect_err("unresolved retained state must block fresh admission");
        assert!(
            matches!(blocked_error, OmniError::FoldRequired { .. }),
            "retained provider residue must reopen fold-only: {blocked_error:?}"
        );
        failure.assert_no_access_under_roots_since(
            orphan_access_checkpoint,
            &failed_roots,
            "blocked admission",
        );

        tokio::time::timeout(
            PROVIDER_FOLD_TIMEOUT,
            db.failpoint_stream_b1_for_test(TABLE, None, 0),
        )
        .await
        .expect("the recovery fold must settle after provider service resumes")
        .expect("the retained WAL prefix must fold through a fresh generation");
        assert_eq!(
            visible_rows(&db).await,
            vec![("retained-after-failure".to_string(), 41)],
            "the unreferenced residue must be inert while the authoritative retry publishes exactly once"
        );

        let after_retry = mem_wal_inventory(uri).await;
        assert_inventory_retained(
            &after_failure,
            &after_retry,
            "successful recovery retry",
        );
        let retry_roots = after_retry.generation_roots.clone();
        let fresh_retry_roots = retry_roots
            .difference(&failed_roots)
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            fresh_retry_roots.len(),
            1,
            "one recovery retry must materialize exactly one fresh randomized generation: failed={failed_roots:?}, retry={retry_roots:?}"
        );
        let referenced_after_retry = after_retry.referenced_generation_roots.clone();
        assert!(
            failed_roots.is_disjoint(&referenced_after_retry),
            "recovery must not retroactively adopt provider-refused output"
        );
        assert!(
            fresh_retry_roots.is_subset(&referenced_after_retry),
            "the exact fresh retry root must become shard-manifest-authoritative: fresh={fresh_retry_roots:?}, referenced={referenced_after_retry:?}"
        );
        failure.assert_no_access_under_roots_since(
            orphan_access_checkpoint,
            &failed_roots,
            "authoritative recovery retry",
        );
        drop(db);

        let reopened = Arc::new(
            Omnigraph::open(uri)
                .await
                .expect("the recovered graph must survive a cold reopen"),
        );
        assert_eq!(
            visible_rows(&reopened).await,
            vec![("retained-after-failure".to_string(), 41)],
            "cold recovery must ignore retained unreferenced output and preserve one visible row"
        );
        let after_reopen = mem_wal_inventory(uri).await;
        assert_inventory_retained(&after_failure, &after_reopen, "cold graph reopen");
        assert!(
            failed_roots.is_subset(&after_reopen.generation_roots),
            "retain-all permits no raw `_mem_wal` deletion during reopen"
        );
        let referenced_after_reopen = after_reopen.referenced_generation_roots.clone();
        assert!(
            failed_roots.is_disjoint(&referenced_after_reopen),
            "cold reopen must keep every provider-refused root non-authoritative"
        );
        failure.assert_no_access_under_roots_since(
            orphan_access_checkpoint,
            &failed_roots,
            "recovery plus cold reopen",
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn provider_shard_manifest_failure_retains_unreferenced_generation_local() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    provider_generation_failure_retains_unreferenced_generation_at(
        dir.path().to_str().unwrap(),
        ProviderFailureTarget::ShardManifest,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn provider_partial_generation_failure_retains_inert_unreferenced_residue_local() {
    let _scenario = FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();
    provider_generation_failure_retains_unreferenced_generation_at(
        dir.path().to_str().unwrap(),
        ProviderFailureTarget::PartialGeneration,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn s3_provider_shard_manifest_failure_retains_unreferenced_generation() {
    let _scenario = FailScenario::setup();
    let Some(uri) = helpers::s3_test_graph_uri("memwal-b2a-provider") else {
        eprintln!("SKIP s3_provider_shard_manifest_failure_retains_unreferenced_generation");
        return;
    };

    provider_generation_failure_retains_unreferenced_generation_at(
        &uri,
        ProviderFailureTarget::ShardManifest,
    )
    .await;

    // This destroys the test's unique isolated graph root after all retention
    // assertions. It is fixture teardown, not a production MemWAL GC path.
    let (store, root) = lance_io::object_store::ObjectStore::from_uri(&uri)
        .await
        .expect("configured S3/RustFS fixture must resolve for cleanup");
    store
        .remove_dir_all(root)
        .await
        .expect("configured S3/RustFS fixture cleanup must succeed");
}
