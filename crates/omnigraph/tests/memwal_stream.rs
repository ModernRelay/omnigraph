#![cfg(feature = "failpoints")]

//! RFC-026 Phase-B1 private row-admission/fold integration owner.
//!
//! These tests reach the single doc-hidden graph seam. They intentionally do
//! not create a public SDK, schema, HTTP, or CLI streaming contract.

mod helpers;

use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, ListArray, RecordBatch,
    StringArray,
};
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
use omnigraph::db::{Omnigraph, ReadTarget, StreamTableStatus};
use omnigraph::error::OmniError;
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_policy::{PolicyChecker, PolicyEngine};
use serial_test::serial;

use helpers::memwal::{CurrentMemWalInventory, MemWalObjectKind};
use helpers::stream_authority::enable_stream_profile;

const STREAM_SCHEMA: &str = "node Person { score: I32 }\n";
const NULLABLE_STREAM_SCHEMA: &str = "node Person { score: I32? }\n";
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
const MIN_CARD_STREAM_SCHEMA: &str = r#"
node Person { name: String @key }
edge Knows: Person -> Person @card(2..)
"#;
const EDGE_STREAM_SCHEMA: &str = r#"
node Person { name: String @key }
edge Knows: Person -> Person
"#;
const TYPED_STREAM_SCHEMA: &str = r#"
node Person {
    slug: String @key
    title: String
    score: I32
    state: enum(open, closed)
    tags: [String]
    embedding: Vector(2)? @embed("title", model="test-model")
    @range(score, 0..10)
    @check(title, "^[A-Z].*")
}
"#;
const BLOB_STREAM_SCHEMA: &str = r#"
node Person {
    score: I32
    content: Blob?
}
"#;
const RANGE_STREAM_SCHEMA: &str = r#"
node Person {
    score: I32
    @range(score, 0..10)
}
"#;
const PAYLOAD_STREAM_SCHEMA: &str = "node Person { payload: String }\n";
const TABLE: &str = "node:Person";
const MIN_CARD_EDGE_TABLE: &str = "edge:Knows";
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
    let cluster_uri = uri
        .strip_suffix("/graphs/knowledge.omni")
        .expect("stream test graph URI must use the checked cluster graph mapping");
    let db = Omnigraph::init(uri, STREAM_SCHEMA)
        .await
        .expect("provider-failure fixture must initialize");
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .expect("provider-failure fixture must enroll its one stream table");
    enable_stream_profile(&db, cluster_uri).await;
}

struct EnrolledGraphDir {
    _cluster: tempfile::TempDir,
    graph: PathBuf,
}

impl EnrolledGraphDir {
    fn path(&self) -> &Path {
        &self.graph
    }

    fn cluster_uri(&self) -> String {
        format!(
            "file://{}",
            self.graph
                .parent()
                .and_then(Path::parent)
                .expect("enrolled graph lives below <cluster>/graphs")
                .display()
        )
    }
}

async fn init_enrolled() -> (EnrolledGraphDir, Arc<Omnigraph>) {
    init_enrolled_with_schema(STREAM_SCHEMA).await
}

async fn init_enrolled_with_schema(schema: &str) -> (EnrolledGraphDir, Arc<Omnigraph>) {
    init_enrolled_with_profile(schema, true).await
}

async fn init_enrolled_disabled() -> (EnrolledGraphDir, Arc<Omnigraph>) {
    init_enrolled_with_profile(STREAM_SCHEMA, false).await
}

async fn init_enrolled_served_with_schema(schema: &str) -> (EnrolledGraphDir, Arc<Omnigraph>) {
    let (dir, db) = init_enrolled_with_schema(schema).await;
    let cluster_uri = dir.cluster_uri();
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    (dir, db)
}

async fn init_unenrolled_served_with_schema(schema: &str) -> (EnrolledGraphDir, Arc<Omnigraph>) {
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), schema)
            .await
            .unwrap(),
    );
    let cluster_uri = format!("file://{}", cluster.path().display());
    enable_stream_profile(&db, &cluster_uri).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    (
        EnrolledGraphDir {
            _cluster: cluster,
            graph,
        },
        db,
    )
}

async fn init_enrolled_with_profile(
    schema: &str,
    enable_profile: bool,
) -> (EnrolledGraphDir, Arc<Omnigraph>) {
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), schema)
            .await
            .unwrap(),
    );
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    if enable_profile {
        enable_stream_profile(&db, &format!("file://{}", cluster.path().display())).await;
    }
    (
        EnrolledGraphDir {
            _cluster: cluster,
            graph,
        },
        db,
    )
}

async fn reopen_enrolled(dir: &EnrolledGraphDir) -> Arc<Omnigraph> {
    Arc::new(
        Omnigraph::open(dir.path().to_str().unwrap())
            .await
            .expect("enrolled graph must reopen"),
    )
}

async fn stream_lane(db: &Omnigraph) -> StreamTableStatus {
    let status = db.stream_status().await.unwrap();
    assert_eq!(status.tables.len(), 1, "fixture owns exactly one lane");
    status.tables.into_iter().next().unwrap()
}

fn epoch_floor(lane: &StreamTableStatus) -> u64 {
    assert_eq!(
        lane.epoch_floor_by_shard.len(),
        1,
        "fixture uses one unsharded stream"
    );
    lane.epoch_floor_by_shard[0].1
}

fn assert_no_recovery_sidecars(dir: &EnrolledGraphDir) {
    assert!(
        helpers::recovery::sidecar_operation_ids(dir.path()).is_empty(),
        "lifecycle operation must leave no recovery sidecar"
    );
}

fn stream_data_block_token(error: &OmniError) -> &str {
    match error {
        OmniError::StreamDataBlocked { block_token } => block_token,
        other => panic!("expected typed stream DataBlock error, got {other:?}"),
    }
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

async fn physical_edge_batch(db: &Omnigraph, rows: &[(String, String, String)]) -> RecordBatch {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(MIN_CARD_EDGE_TABLE).await.unwrap();
    let schema = Arc::new(Schema::from(table.schema()));
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "src", "dst"],
        "edge fixture must expose the exact normalized physical schema"
    );
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(id, _, _)| id.as_str()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, src, _)| src.as_str()),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|(_, _, dst)| dst.as_str()),
            )) as ArrayRef,
        ],
    )
    .unwrap()
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

/// Exercise the hidden authenticated one-row ingress boundary while retaining
/// the same private wire-envelope shape used by the lower B2 tests.
#[allow(clippy::too_many_arguments)]
async fn authorized_ingest_score(
    db: &Arc<Omnigraph>,
    stream_incarnation_id: &str,
    logical_id: &str,
    score: i32,
    caller_ordinal: u64,
    write_id: &str,
    predecessor_token: Option<&str>,
    actor_id: &str,
) -> Result<(String, bool), OmniError> {
    let raw_json = serde_json::to_vec(&serde_json::json!({
        "$stream": {
            "stream_incarnation_id": stream_incarnation_id,
            "write_id": write_id,
            "predecessor_token": predecessor_token,
        },
        "id": logical_id,
        "score": score,
    }))
    .unwrap();
    db.failpoint_stream_ingest_one_as_for_test(TABLE, &raw_json, caller_ordinal, actor_id)
    .await
}

fn ndjson_score_line(
    stream_incarnation_id: &str,
    logical_id: &str,
    score: i32,
    write_id: &str,
    predecessor_token: Option<&str>,
) -> Vec<u8> {
    let mut line = serde_json::to_vec(&serde_json::json!({
        "$stream": {
            "stream_incarnation_id": stream_incarnation_id,
            "write_id": write_id,
            "predecessor_token": predecessor_token,
        },
        "id": logical_id,
        "score": score,
    }))
    .unwrap();
    line.push(b'\n');
    line
}

fn parse_ndjson_outcomes(outcomes: Vec<String>) -> Vec<serde_json::Value> {
    outcomes
        .into_iter()
        .map(|outcome| serde_json::from_str(&outcome).expect("outcome must be valid JSON"))
        .collect()
}

async fn prepare_stream_ingest(
    db: &Omnigraph,
    enrollment_request_id: &str,
    witness_json: Option<&str>,
    actor_id: &str,
) -> Result<(String, Option<String>, Option<String>), OmniError> {
    db.failpoint_prepare_stream_ingest_as_for_test(
        TABLE,
        enrollment_request_id,
        witness_json,
        actor_id,
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
async fn admission_rejects_value_violation_before_wal_or_manifest_effect() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_with_schema(RANGE_STREAM_SCHEMA).await;
    let version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let inventory_before = mem_wal_inventory(db.uri()).await;
    let wal_before = inventory_before.object_count(MemWalObjectKind::Wal);
    let invalid = physical_batch(&db, &[("out-of-range".to_string(), 11)]).await;

    let error = db
        .failpoint_stream_b1_for_test(TABLE, Some(invalid), 0)
        .await
        .expect_err("row-local value validation must reject before retained admission");
    assert!(error.to_string().contains("@range"), "{error:?}");

    let inventory_after = mem_wal_inventory(db.uri()).await;
    assert_eq!(
        inventory_after.object_count(MemWalObjectKind::Wal),
        wal_before,
        "a value violation must not append a WAL object"
    );
    assert_eq!(
        inventory_after.objects, inventory_before.objects,
        "a value violation must leave every listed MemWAL object path, class, and size unchanged, including shard-manifest hints"
    );
    assert_eq!(
        inventory_after.generation_roots, inventory_before.generation_roots,
        "a value violation must not create or remove a listed MemWAL generation"
    );
    assert_eq!(
        inventory_after.referenced_generation_roots,
        inventory_before.referenced_generation_roots,
        "a value violation must leave shard-manifest generation authority unchanged"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        version_before,
        "a value violation must not publish manifest authority"
    );
    assert!(visible_rows(&db).await.is_empty());
}

#[tokio::test]
#[serial]
async fn durable_put_is_graph_content_invisible_until_one_explicit_fold() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let table_version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry(TABLE)
        .unwrap()
        .table_version;

    let batch = physical_batch(&db, &[("p1".to_string(), 10)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 41)
        .await
        .expect("watcher success is the private durability acknowledgement");
    assert_eq!(visible_rows(&db).await, Vec::<(String, i32)>::new());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry(TABLE)
            .unwrap()
            .table_version,
        table_version_before,
        "claim receipts may advance operational manifest authority but not graph content"
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("one generation folds through recovery-v11");
    assert_eq!(visible_rows(&db).await, vec![("p1".to_string(), 10)]);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry(TABLE)
            .unwrap()
            .table_version,
        table_version_before + 1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_accepts_an_empty_successor_after_an_ordinary_published_fold() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let row = physical_batch(&db, &[("ordinary-before-quiesce".to_string(), 13)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("fixture row must be durably acknowledged");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the ordinary fold must publish before quiesce");

    let folded = stream_lane(&db).await;
    assert_eq!(folded.lifecycle, "OPEN");
    assert_eq!(folded.last_fold_outcome.as_deref(), Some("PUBLISHED"));
    let folded_epoch = epoch_floor(&folded);
    let drain_id = "90909090-9090-4090-8090-909090909090";

    db.failpoint_stream_quiesce_for_test(
        TABLE,
        drain_id,
        folded.lifecycle_revision,
        "operator:ordinary-fold-quiesce",
    )
    .await
    .expect("the retained published prefix plus one new sentinel proves the successor empty");

    let sealed = stream_lane(&db).await;
    assert_eq!(sealed.lifecycle, "SEALED");
    assert_eq!(
        epoch_floor(&sealed),
        folded_epoch + 1,
        "quiesce must need exactly one drain claim after the published fold"
    );
    assert_eq!(
        visible_rows(&db).await,
        vec![("ordinary-before-quiesce".to_string(), 13)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_persists_a_stable_data_block_for_a_permanent_validation_failure() {
    let _scenario = FailScenario::setup();
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), UNIQUE_STREAM_SCHEMA)
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
    enable_stream_profile(&db, &format!("file://{}", cluster.path().display())).await;
    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };

    let duplicate = physical_batch(&db, &[("drain-blocked".to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
        .await
        .expect("base-dependent uniqueness remains fold-time work");
    let before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let before_lane = stream_lane(&db).await;
    let drain_id = "96969696-9696-4696-8696-969696969696";
    let actor = "operator:strict-data-block";

    let first_error = db
        .failpoint_stream_quiesce_for_test(
            TABLE,
            drain_id,
            before_lane.lifecycle_revision,
            actor,
        )
        .await
        .expect_err("a permanent drain validator failure must publish a strict data block");
    let blocked = stream_lane(&db).await;
    let block_token = blocked
        .strict_block_token
        .clone()
        .expect("DRAINING validation failure must expose its durable block token");
    assert_eq!(stream_data_block_token(&first_error), block_token);
    assert_eq!(blocked.lifecycle, "DRAINING");
    assert_eq!(blocked.drain_id.as_deref(), Some(drain_id));
    assert_eq!(
        blocked.last_fold_outcome.as_deref(),
        Some("STRICT_BLOCKED")
    );
    assert_eq!(blocked.last_fold_graph_commit_id, None);
    assert!(
        blocked.lifecycle_revision > before_lane.lifecycle_revision,
        "start, claim, and block publications must advance lifecycle authority"
    );
    let after = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(
        after.version() > before.version(),
        "the strict block is an operational manifest publication"
    );
    assert_eq!(
        after.entry(TABLE).unwrap().table_version,
        before.entry(TABLE).unwrap().table_version,
        "strict blocking must not publish the rejected base-table rows"
    );
    let visible = visible_rows(&db).await;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].1, 7);
    assert_no_recovery_sidecars(&dir);

    let blocked_version = after.version();
    let exact_retry_error = db
        .failpoint_stream_quiesce_for_test(
            TABLE,
            drain_id,
            before_lane.lifecycle_revision,
            actor,
        )
        .await
        .expect_err("an exact retry must return the selected block");
    assert_eq!(stream_data_block_token(&exact_retry_error), block_token);
    assert_eq!(exact_retry_error.to_string(), first_error.to_string());
    assert_eq!(stream_lane(&db).await, blocked);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        blocked_version,
        "an exact blocked retry is lifecycle-inert"
    );

    drop(db);
    let reopened = reopen_enrolled(&dir).await;
    let reopen_error = reopened
        .failpoint_stream_quiesce_for_test(
            TABLE,
            drain_id,
            before_lane.lifecycle_revision,
            actor,
        )
        .await
        .expect_err("cold reopen must retain the same selected block");
    assert_eq!(stream_data_block_token(&reopen_error), block_token);
    assert_eq!(reopen_error.to_string(), first_error.to_string());
    assert_eq!(stream_lane(&reopened).await, blocked);
    assert_eq!(
        reopened
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        blocked_version,
        "cold exact retry must not mint another block or claim"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_persists_a_stable_data_block_for_fresh_source_min_cardinality() {
    let _scenario = FailScenario::setup();
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), MIN_CARD_STREAM_SCHEMA)
            .await
            .unwrap(),
    );
    load_jsonl(
        &db,
        r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .expect("seed valid endpoints before enrolling the edge table");
    db.failpoint_enroll_stream_table_for_test(MIN_CARD_EDGE_TABLE)
        .await
        .unwrap();
    enable_stream_profile(&db, &format!("file://{}", cluster.path().display())).await;
    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };

    let edge = physical_edge_batch(
        &db,
        &[("edge-1".to_string(), "Alice".to_string(), "Bob".to_string())],
    )
    .await;
    db.failpoint_stream_b1_for_test(MIN_CARD_EDGE_TABLE, Some(edge), 0)
        .await
        .expect("row-local admission must retain the graph-valid edge");
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let before_lane = stream_lane(&db).await;
    let drain_id = "97979797-9797-4797-8797-979797979797";
    let actor = "operator:min-card-data-block";

    let first_error = db
        .failpoint_stream_quiesce_for_test(
            MIN_CARD_EDGE_TABLE,
            drain_id,
            before_lane.lifecycle_revision,
            actor,
        )
        .await
        .expect_err("the under-min fresh source must publish a strict DataBlock");
    let blocked = stream_lane(&db).await;
    let block_token = blocked
        .strict_block_token
        .clone()
        .expect("minimum-cardinality evidence must select the streamed edge");
    assert_eq!(stream_data_block_token(&first_error), block_token);
    assert_eq!(blocked.lifecycle, "DRAINING");
    assert_eq!(blocked.drain_id.as_deref(), Some(drain_id));
    assert_eq!(blocked.last_fold_outcome.as_deref(), Some("STRICT_BLOCKED"));
    let after = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(
        after.version() > before.version(),
        "the DataBlock must be durable manifest state"
    );
    assert_eq!(
        after.entry(MIN_CARD_EDGE_TABLE).unwrap().table_version,
        before.entry(MIN_CARD_EDGE_TABLE).unwrap().table_version,
        "the under-min edge must remain outside the visible base table"
    );
    assert!(
        helpers::read_table(&db, MIN_CARD_EDGE_TABLE)
            .await
            .is_empty()
    );
    assert_no_recovery_sidecars(&dir);

    let blocked_version = after.version();
    let exact_retry_error = db
        .failpoint_stream_quiesce_for_test(
            MIN_CARD_EDGE_TABLE,
            drain_id,
            before_lane.lifecycle_revision,
            actor,
        )
        .await
        .expect_err("an exact retry must return the selected block");
    assert_eq!(stream_data_block_token(&exact_retry_error), block_token);
    assert_eq!(exact_retry_error.to_string(), first_error.to_string());
    assert_eq!(stream_lane(&db).await, blocked);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        blocked_version,
        "an exact blocked retry must be lifecycle-inert"
    );

    drop(db);
    let reopened = reopen_enrolled(&dir).await;
    let reopen_error = reopened
        .failpoint_stream_quiesce_for_test(
            MIN_CARD_EDGE_TABLE,
            drain_id,
            before_lane.lifecycle_revision,
            actor,
        )
        .await
        .expect_err("cold reopen must retain the same selected block");
    assert_eq!(stream_data_block_token(&reopen_error), block_token);
    assert_eq!(reopen_error.to_string(), first_error.to_string());
    assert_eq!(stream_lane(&reopened).await, blocked);
    assert_eq!(
        reopened
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        blocked_version,
        "cold exact retry must not mint another block or claim"
    );
    assert!(
        helpers::read_table(&reopened, MIN_CARD_EDGE_TABLE)
            .await
            .is_empty()
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_reopens_and_folds_an_acknowledged_post_claim_row_exactly_once() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let epoch_before_claim = epoch_floor(&stream_lane(&db).await);
    let batch = physical_batch(&db, &[("quiesced".to_string(), 17)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .expect("the post-sentinel row must be durable before quiesce");
    let after_put = stream_lane(&db).await;
    assert!(
        epoch_floor(&after_put) > epoch_before_claim,
        "the cold put must first publish its empty writer claim"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "a durability acknowledgement is not graph publication"
    );
    drop(db);

    let reopened = reopen_enrolled(&dir).await;
    let expected_revision = stream_lane(&reopened).await.lifecycle_revision;
    let drain_id = "91919191-9191-4191-8191-919191919191";

    reopened
        .failpoint_stream_quiesce_for_test(
            TABLE,
            drain_id,
            expected_revision,
            "operator:quiesce-test",
        )
        .await
        .expect("quiesce must preserve, fold, and publish the post-sentinel row");

    let terminal = stream_lane(&reopened).await;
    assert_eq!(terminal.lifecycle, "SEALED");
    assert_eq!(terminal.drain_id, None);
    assert_eq!(terminal.last_fold_outcome.as_deref(), Some("PUBLISHED"));
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("quiesced".to_string(), 17)]
    );
    assert_no_recovery_sidecars(&dir);

    reopened
        .failpoint_stream_quiesce_for_test(
            TABLE,
            drain_id,
            expected_revision,
            "operator:quiesce-test",
        )
        .await
        .expect("an exact delayed retry must return from its selected terminal receipt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_reuses_a_published_drain_fold_after_receipt_arm_reopen() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let row = physical_batch(&db, &[("published-drain-fold".to_string(), 19)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("fixture row must be durably acknowledged");
    let initial = stream_lane(&db).await;
    let drain_id = "a2a2a2a2-a2a2-42a2-82a2-a2a2a2a2a2a2";
    let actor = "operator:published-drain-fold";

    let error = {
        let _receipt_arm = ScopedFailPoint::new(
            names::STREAM_LIFECYCLE_RECEIPT_POST_SIDECAR_PRE_TOKEN_COMMIT,
            "return",
        );
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
            .await
            .expect_err("quiesce must stop after publishing the drain fold")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_LIFECYCLE_RECEIPT_POST_SIDECAR_PRE_TOKEN_COMMIT),
        "{error:?}"
    );
    let folded = stream_lane(&db).await;
    assert_eq!(folded.lifecycle, "DRAINING");
    assert_eq!(folded.last_fold_outcome.as_deref(), Some("PUBLISHED"));
    let folded_epoch = epoch_floor(&folded);
    assert_eq!(
        visible_rows(&db).await,
        vec![("published-drain-fold".to_string(), 19)]
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1,
        "the armed lifecycle receipt must remain for recovery"
    );
    drop(db);

    let reopened = reopen_enrolled(&dir).await;
    let recovered = stream_lane(&reopened).await;
    assert_eq!(recovered.lifecycle, "DRAINING");
    assert_eq!(epoch_floor(&recovered), folded_epoch);
    assert_no_recovery_sidecars(&dir);

    reopened
        .failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
        .await
        .expect("restart must reuse the published drain-fold empty successor");
    let terminal = stream_lane(&reopened).await;
    assert_eq!(terminal.lifecycle, "SEALED");
    assert_eq!(
        epoch_floor(&terminal),
        folded_epoch,
        "published drain-fold continuation must not mint a fresh writer epoch"
    );
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("published-drain-fold".to_string(), 19)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn empty_quiesce_is_idempotent_and_refuses_rebound_or_stale_requests() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let expected_revision = stream_lane(&db).await.lifecycle_revision;
    let drain_id = "92929292-9292-4292-8292-929292929292";
    let actor = "operator:empty-quiesce-test";
    db.failpoint_stream_quiesce_for_test(TABLE, drain_id, expected_revision, actor)
        .await
        .expect("an empty lane must reach SEALED through a receipt-backed fence cut");

    let terminal = stream_lane(&db).await;
    assert_eq!(terminal.lifecycle, "SEALED");
    assert_eq!(terminal.last_fold_outcome, None);
    assert!(visible_rows(&db).await.is_empty());
    assert_no_recovery_sidecars(&dir);

    let (left, right) = tokio::join!(
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, expected_revision, actor),
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, expected_revision, actor),
    );
    left.expect("first concurrent exact retry must replay the terminal receipt");
    right.expect("second concurrent exact retry must replay the terminal receipt");

    for (revision, rebound_actor) in [
        (expected_revision, "operator:replacement"),
        (expected_revision + 1, actor),
    ] {
        let error = db
            .failpoint_stream_quiesce_for_test(TABLE, drain_id, revision, rebound_actor)
            .await
            .expect_err("the same operation ID cannot be rebound");
        assert!(
            matches!(error, OmniError::StreamLifecycleIdempotencyConflict { .. }),
            "{error:?}"
        );
    }

    let stale = db
        .failpoint_stream_quiesce_for_test(
            TABLE,
            "93939393-9393-4393-8393-939393939393",
            expected_revision,
            actor,
        )
        .await
        .expect_err("a new operation ID may not retarget a stale OPEN revision");
    assert!(
        matches!(
            stale,
            OmniError::StreamLifecycleChanged {
                expected_revision: observed,
                current_revision,
                ..
            } if observed == expected_revision && current_revision == terminal.lifecycle_revision
        ),
        "{stale:?}"
    );
    assert_eq!(stream_lane(&db).await, terminal);
    assert_no_recovery_sidecars(&dir);

    let _must_not_invoke_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
    let raw_json = serde_json::to_vec(&serde_json::json!({
        "$stream": {
            "stream_incarnation_id": incarnation,
            "write_id": "50505050-5050-4050-8050-505050505050",
            "predecessor_token": null,
        },
        "id": "after-seal",
        "score": 9,
    }))
    .unwrap();
    let sealed = db
        .failpoint_stream_ingest_one_as_for_test(
            TABLE,
            &raw_json,
            1,
            "agent:after-seal",
        )
        .await
        .expect_err("ordinary admission must not cross a SEALED lifecycle");
    let OmniError::Manifest(manifest_error) = sealed else {
        panic!("expected typed stream lifecycle conflict, got {sealed:?}");
    };
    assert!(
        matches!(
            manifest_error.details,
            Some(omnigraph::error::ManifestConflictDetails::StreamLifecycleConflict {
                ref lifecycle,
                ..
            }) if lifecycle == "SEALED"
        ),
        "{manifest_error:?}"
    );
    assert!(visible_rows(&db).await.is_empty());

    db.branch_create("sealed-prepare")
        .await
        .expect("SEALED stream authority permits named branches");
    let (prepared, _, prepared_incarnation) = prepare_stream_ingest(
        &db,
        "17171717-1717-4717-8717-171717171717",
        None,
        "agent:sealed-prepare",
    )
    .await
    .expect("prepare must resolve a SEALED lane before applying enrollment topology checks");
    assert_eq!(prepared, "already_enrolled");
    assert_eq!(prepared_incarnation.as_deref(), Some(incarnation.as_str()));
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_continues_the_same_drain_after_crashing_before_its_claim() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let initial = stream_lane(&db).await;
    let drain_id = "94949494-9494-4494-8494-949494949494";
    let actor = "operator:pre-claim-retry";

    let error = {
        let _before_claim = ScopedFailPoint::new(names::RECOVERY_SIDECAR_WRITE, "return");
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
            .await
            .expect_err("claim-sidecar refusal must interrupt the new drain")
    };
    assert!(
        error.to_string().contains(names::RECOVERY_SIDECAR_WRITE),
        "{error:?}"
    );
    let draining = stream_lane(&db).await;
    assert_eq!(draining.lifecycle, "DRAINING");
    assert_eq!(draining.drain_id.as_deref(), Some(drain_id));
    assert_eq!(draining.lifecycle_revision, initial.lifecycle_revision + 1);
    assert_eq!(
        epoch_floor(&draining),
        epoch_floor(&initial),
        "failure before the claim sidecar must not move shard authority"
    );
    assert_no_recovery_sidecars(&dir);

    db.failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
        .await
        .expect("the exact request must continue its durable DRAINING descriptor");
    assert_eq!(stream_lane(&db).await.lifecycle, "SEALED");
    assert!(visible_rows(&db).await.is_empty());
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_replays_an_unsealed_claim_after_reopen() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let row = physical_batch(&db, &[("claim-before-seal".to_string(), 21)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("fixture row must be durably acknowledged");
    let initial = stream_lane(&db).await;
    let drain_id = "95959595-9595-4595-8595-959595959595";
    let actor = "operator:claim-before-seal";
    let retired =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_RETIREMENT_RELEASE);

    let error = {
        let _before_seal = ScopedFailPoint::new(names::STREAM_B1_BEFORE_FORCE_SEAL, "return");
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
            .await
            .expect_err("the fresh drain claim must stop before sealing")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. })
            && error
                .to_string()
                .contains(names::STREAM_B1_BEFORE_FORCE_SEAL),
        "{error:?}"
    );
    retired.wait_until_reached().await;
    retired.release();
    let failed = stream_lane(&db).await;
    assert_eq!(failed.lifecycle, "DRAINING");
    assert!(
        epoch_floor(&failed) > epoch_floor(&initial),
        "the terminal drain claim must be selected before sealing"
    );
    assert!(visible_rows(&db).await.is_empty());
    assert_no_recovery_sidecars(&dir);
    drop(db);

    let reopened = reopen_enrolled(&dir).await;
    reopened
        .failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
        .await
        .expect("restart must make one fresh claim for the unsealed replay");
    let terminal = stream_lane(&reopened).await;
    assert_eq!(terminal.lifecycle, "SEALED");
    assert!(
        epoch_floor(&terminal) > epoch_floor(&failed),
        "non-empty unsealed replay requires one successor claim"
    );
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("claim-before-seal".to_string(), 21)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_reuses_the_exact_flushed_cut_after_reopen() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let row = physical_batch(&db, &[("sealed-before-fold".to_string(), 31)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("fixture row must be durably acknowledged");
    let initial = stream_lane(&db).await;
    let drain_id = "96969696-9696-4696-8696-969696969696";
    let actor = "operator:sealed-before-fold";

    let error = {
        let _after_drain =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR, "return");
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
            .await
            .expect_err("the proven flushed cut must stop before fold-sidecar arm")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR),
        "{error:?}"
    );
    let failed = stream_lane(&db).await;
    assert_eq!(failed.lifecycle, "DRAINING");
    assert!(epoch_floor(&failed) > epoch_floor(&initial));
    assert!(visible_rows(&db).await.is_empty());
    assert_no_recovery_sidecars(&dir);
    drop(db);

    let reopened = reopen_enrolled(&dir).await;
    reopened
        .failpoint_stream_quiesce_for_test(TABLE, drain_id, initial.lifecycle_revision, actor)
        .await
        .expect("restart must reuse and publish the exact flushed generation");
    let terminal = stream_lane(&reopened).await;
    assert_eq!(terminal.lifecycle, "SEALED");
    assert_eq!(
        epoch_floor(&terminal),
        epoch_floor(&failed),
        "a proven flushed cut must not mint another writer claim"
    );
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("sealed-before-fold".to_string(), 31)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn disabled_profile_refuses_fresh_quiesce_without_effect() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_disabled().await;
    let before_status = db.stream_status().await.unwrap();
    let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let expected_revision = before_status.tables[0].lifecycle_revision;

    let error = db
        .failpoint_stream_quiesce_for_test(
            TABLE,
            "97979797-9797-4797-8797-979797979797",
            expected_revision,
            "operator:disabled-quiesce",
        )
        .await
        .expect_err("DISABLED profile cannot start a fresh quiesce");
    assert!(
        matches!(
            error,
            OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "DISABLED"
        ),
        "{error:?}"
    );
    assert_eq!(db.stream_status().await.unwrap(), before_status);
    let after_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(after_snapshot.version(), before_snapshot.version());
    assert_eq!(
        after_snapshot.entry(TABLE).unwrap().table_version,
        before_snapshot.entry(TABLE).unwrap().table_version
    );
    assert!(visible_rows(&db).await.is_empty());
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_receipt_recovery_boundaries_reopen_cleanly_and_replay_once() {
    let _scenario = FailScenario::setup();
    struct Boundary {
        name: &'static str,
        drain_id: &'static str,
        failpoint: &'static str,
        action: &'static str,
        terminal_on_reopen: bool,
    }
    let boundaries = [
        Boundary {
            name: "armed before token effect",
            drain_id: "98989898-9898-4898-8898-989898989898",
            failpoint: names::STREAM_LIFECYCLE_RECEIPT_POST_SIDECAR_PRE_TOKEN_COMMIT,
            action: "return",
            terminal_on_reopen: false,
        },
        Boundary {
            name: "token effect before confirmation",
            drain_id: "99999999-9999-4999-8999-999999999999",
            failpoint: names::RECOVERY_SIDECAR_CONFIRM,
            // The stock-manifest checkpoint and terminal empty claim each
            // confirm before the lifecycle receipt.
            action: "2*off->return",
            terminal_on_reopen: true,
        },
        Boundary {
            name: "confirmed receipt before terminal manifest",
            drain_id: "a0a0a0a0-a0a0-40a0-80a0-a0a0a0a0a0a0",
            failpoint: names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH,
            // The terminal empty claim publishes before the lifecycle
            // receipt; the stock-manifest checkpoint uses its own publisher.
            action: "1*off->return",
            terminal_on_reopen: true,
        },
        Boundary {
            name: "terminal manifest before audit",
            drain_id: "a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1",
            failpoint: names::RECOVERY_RECORD_AUDIT,
            // The terminal empty claim audits before the lifecycle receipt;
            // the stock-manifest checkpoint is not a terminal audit outcome.
            action: "1*off->return",
            terminal_on_reopen: true,
        },
    ];

    for boundary in boundaries {
        let (dir, db) = init_enrolled().await;
        let expected_revision = stream_lane(&db).await.lifecycle_revision;
        let actor = format!("operator:{}", boundary.name.replace(' ', "-"));
        let error = {
            let _cut = ScopedFailPoint::new(boundary.failpoint, boundary.action);
            db.failpoint_stream_quiesce_for_test(
                TABLE,
                boundary.drain_id,
                expected_revision,
                &actor,
            )
            .await
            .expect_err(boundary.name)
        };
        assert!(
            error.to_string().contains(boundary.failpoint),
            "{}: {error:?}",
            boundary.name
        );
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1,
            "{} must retain exactly its lifecycle-receipt sidecar",
            boundary.name
        );
        assert!(
            visible_rows(&db).await.is_empty(),
            "{} cannot invent graph content",
            boundary.name
        );
        drop(db);

        let reopened = reopen_enrolled(&dir).await;
        assert_no_recovery_sidecars(&dir);
        let after_open = stream_lane(&reopened).await;
        assert_eq!(
            after_open.lifecycle,
            if boundary.terminal_on_reopen {
                "SEALED"
            } else {
                "DRAINING"
            },
            "{}",
            boundary.name
        );

        reopened
            .failpoint_stream_quiesce_for_test(TABLE, boundary.drain_id, expected_revision, &actor)
            .await
            .unwrap_or_else(|error| panic!("{} did not continue: {error:?}", boundary.name));
        let terminal = stream_lane(&reopened).await;
        assert_eq!(terminal.lifecycle, "SEALED", "{}", boundary.name);
        assert!(visible_rows(&reopened).await.is_empty());
        assert_no_recovery_sidecars(&dir);
        let audit = helpers::recovery::recovery_audit_kinds(dir.path()).await;

        reopened
            .failpoint_stream_quiesce_for_test(TABLE, boundary.drain_id, expected_revision, &actor)
            .await
            .unwrap_or_else(|error| {
                panic!("{} exact terminal replay failed: {error:?}", boundary.name)
            });
        assert_eq!(stream_lane(&reopened).await, terminal);
        assert_eq!(
            helpers::recovery::recovery_audit_kinds(dir.path()).await,
            audit,
            "{} exact replay must not duplicate its audit",
            boundary.name
        );
        assert_no_recovery_sidecars(&dir);
    }
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
        let _failpoint =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_TOKEN_COMMIT_PRE_CONFIRM, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err(
                "both exact effects without confirmation must retain recovery-v12 ownership",
            )
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
async fn crash_after_arm_before_any_effect_retires_the_intent_effect_free() {
    // The armed-but-no-effect boundary: the recovery-v14 intent is durable
    // while both exact Lance participants are untouched.  Recovery must take
    // the `EffectFree` arm — retire the intent and publish nothing — rather
    // than roll a phantom outcome forward.  The acknowledged generation stays
    // durable and folds exactly once afterwards.
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let table_version_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .entry(TABLE)
        .unwrap()
        .table_version;
    let batch = physical_batch(&db, &[("effect-free".to_string(), 36)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();

    let error = {
        let _failpoint =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_SIDECAR_PRE_BASE_COMMIT, "return");
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("an armed intent must retain recovery ownership")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains("stopped after arming its recovery intent"),
        "the refusal must come from the armed-pre-effect boundary, got: {error:?}"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1,
        "the armed intent must survive for open-time classification"
    );
    assert!(visible_rows(&db).await.is_empty());
    drop(db);

    let reopened = Arc::new(
        Omnigraph::open(dir.path().to_str().unwrap())
            .await
            .expect("open-time recovery must retire an effect-free intent"),
    );
    assert!(
        helpers::recovery::sidecar_operation_ids(dir.path()).is_empty(),
        "effect-free classification must retire the intent"
    );
    assert_eq!(
        reopened
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry(TABLE)
            .unwrap()
            .table_version,
        table_version_before,
        "an effect-free retirement must not advance the base-table pointer"
    );
    assert!(
        visible_rows(&reopened).await.is_empty(),
        "no participant committed, so nothing may become graph-visible"
    );

    reopened
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the durable generation must remain fold-only and retryable");
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("effect-free".to_string(), 36)],
        "the acknowledged row must fold exactly once after the effect-free retirement"
    );
}

#[tokio::test]
#[serial]
async fn fold_confirmation_failure_reconstructs_exact_n_plus_one_on_reopen() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
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
    let audit_after_failure = helpers::recovery::recovery_audit_kinds(dir.path()).await;
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
    assert_eq!(
        &audit_after[..audit_after_failure.len()],
        audit_after_failure.as_slice()
    );
    assert_eq!(audit_after.len(), audit_after_failure.len() + 1);
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
    let batch = physical_batch(&db, &[("post-publish-audit".to_string(), 37)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .unwrap();
    let audit_before = helpers::recovery::recovery_audit_kinds(dir.path()).await;

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
    let audit_after_failure = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(
        audit_after_failure, audit_before,
        "the failed fold audit must leave no partial audit row"
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
    assert_eq!(
        &audit_after[..audit_before.len()],
        audit_before.as_slice()
    );
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
    let audit_before_cleanup = helpers::recovery::recovery_audit_kinds(dir.path()).await;

    db.refresh()
        .await
        .expect("the next recovery barrier must consume the already-visible sidecar");
    assert!(helpers::recovery::sidecar_operation_ids(dir.path()).is_empty());
    let audit_after = helpers::recovery::recovery_audit_kinds(dir.path()).await;
    assert_eq!(
        audit_after, audit_before_cleanup,
        "cleanup retry must not duplicate the already-recorded fold audit"
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
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
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
    let (dir, db) = init_enrolled_served_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
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
    let inventory_before_abort = mem_wal_inventory(db.uri()).await;

    successor
        .abort()
        .await
        .expect("the test-only successor must retire cleanly");
    let fold_error = db
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect_err("an unledgered foreign epoch must remain fail-closed");
    assert!(
        matches!(fold_error, OmniError::RecoveryRequired { .. }),
        "{fold_error:?}"
    );
    assert!(
        fold_error
            .to_string()
            .contains("stream_claim_physical_prestate"),
        "the refusal must identify the physical epoch/read-set mismatch: {fold_error:?}"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "an unledgered foreign epoch cannot authorize graph publication"
    );
    let inventory_after_refusal = mem_wal_inventory(db.uri()).await;
    assert_inventory_retained(
        &inventory_before_abort,
        &inventory_after_refusal,
        "unledgered foreign-epoch refusal",
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
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), UNIQUE_STREAM_SCHEMA)
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
    enable_stream_profile(&db, &format!("file://{}", cluster.path().display())).await;

    let duplicate = physical_batch(&db, &[("duplicate-id".to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
        .await
        .expect("base-dependent uniqueness is fold-time work");
    let snapshot_before_fold = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let fold_error = db
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect_err("strict fold must reject a committed uniqueness conflict");
    assert!(fold_error.to_string().contains("unique"), "{fold_error:?}");
    let snapshot_after_fold = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        snapshot_after_fold.version(),
        snapshot_before_fold.version(),
        "validation failure occurs before any fold manifest CAS"
    );
    assert_eq!(
        snapshot_after_fold.entry(TABLE).unwrap().table_version,
        snapshot_before_fold.entry(TABLE).unwrap().table_version,
        "validation failure occurs before any fold base-table effect"
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
async fn authorized_ingest_happy_fold_retry_and_conflicts_are_effect_free() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_unenrolled_served_with_schema(STREAM_SCHEMA).await;
    let request_id = "10101010-1010-4010-8010-101010101010";
    let manifest_before_prepare = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let inventory_before_prepare = mem_wal_inventory(db.uri()).await;
    let (challenge, witness, incarnation) = prepare_stream_ingest(&db, request_id, None, "agent:a")
        .await
        .expect("an eligible absent lane must return compare evidence");
    assert_eq!(challenge, "witness_required");
    assert!(incarnation.is_none());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before_prepare,
        "the bodyless challenge must not publish"
    );
    assert_eq!(
        mem_wal_inventory(db.uri()).await.objects,
        inventory_before_prepare.objects,
        "the bodyless challenge must not create MemWAL objects"
    );
    assert_no_recovery_sidecars(&dir);
    let (prepared, no_witness, incarnation) =
        prepare_stream_ingest(&db, request_id, witness.as_deref(), "agent:a")
            .await
            .expect("echoing the exact witness must enroll one OPEN lane");
    assert_eq!(prepared, "enrolled");
    assert!(no_witness.is_none());
    let incarnation = incarnation.expect("enrollment returns its durable incarnation");
    let first_write = "11111111-1111-4111-8111-111111111111";
    let (token, already_durable) = authorized_ingest_score(
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
    .expect("one authorized occurrence must cross the complete durability boundary");
    assert!(!already_durable);
    assert!(visible_rows(&db).await.is_empty());
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the attributed occurrence must fold through recovery-v14");
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

    let (retry, retry_was_durable) = authorized_ingest_score(
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
    assert!(retry_was_durable);

    let actor_rebind = authorized_ingest_score(
        &db,
        &incarnation,
        "same-key",
        1,
        7,
        first_write,
        None,
        "agent:b",
    )
    .await
    .expect_err("the authenticated actor is part of idempotency identity");
    assert!(
        matches!(actor_rebind, OmniError::StreamIdempotencyConflict { .. }),
        "{actor_rebind:?}"
    );

    let binding_error = authorized_ingest_score(
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

    let sequence_error = authorized_ingest_score(
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

    let idempotency_error = authorized_ingest_score(
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
async fn prepare_stream_ingest_replays_actor_bound_durable_receipt() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_unenrolled_served_with_schema(STREAM_SCHEMA).await;
    let request_id = "12121212-1212-4212-8212-121212121212";
    let (_, witness, _) = prepare_stream_ingest(&db, request_id, None, "agent:a")
        .await
        .unwrap();
    let witness = witness.expect("challenge returns an opaque witness");

    {
        let _audit = ScopedFailPoint::new(names::RECOVERY_RECORD_AUDIT, "return");
        let error = prepare_stream_ingest(&db, request_id, Some(&witness), "agent:a")
            .await
            .expect_err("lost post-publication audit result must stay recoverable");
        assert!(
            matches!(error, OmniError::RecoveryRequired { .. }),
            "{error:?}"
        );
    }

    drop(db);
    let db = helpers::stream_authority::bind_checked_stream_runtime(
        reopen_enrolled(&dir).await,
        &dir.cluster_uri(),
    )
    .await;
    let (replayed, _, incarnation) =
        prepare_stream_ingest(&db, request_id, Some(&witness), "agent:a")
            .await
            .expect("retry must heal and replay the durable enrollment receipt");
    assert_eq!(replayed, "enrolled");
    let incarnation = incarnation.expect("the receipt carries its stream incarnation");
    let manifest_after_replay = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let (exact_retry, _, retry_incarnation) =
        prepare_stream_ingest(&db, request_id, Some(&witness), "agent:a")
            .await
            .expect("an exact request retry must remain idempotent");
    assert_eq!(exact_retry, "enrolled");
    assert_eq!(retry_incarnation.as_deref(), Some(incarnation.as_str()));
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_after_replay,
        "receipt replay must not publish a second enrollment"
    );

    let (already_enrolled, _, current_incarnation) =
        prepare_stream_ingest(&db, request_id, None, "agent:a")
            .await
            .expect("an existing lane needs no witness challenge");
    assert_eq!(already_enrolled, "already_enrolled");
    assert_eq!(
        current_incarnation.as_deref(),
        Some(incarnation.as_str())
    );

    let mut altered_witness: serde_json::Value =
        serde_json::from_str(&witness).expect("the opaque witness is valid JSON");
    altered_witness["current_head"]["transaction_uuid"] =
        serde_json::json!("34343434-3434-4434-8434-343434343434");
    let altered_witness =
        serde_json::to_string(&altered_witness).expect("the altered witness remains valid JSON");
    let intent_rebind =
        prepare_stream_ingest(&db, request_id, Some(&altered_witness), "agent:a")
            .await
            .expect_err("one request id cannot be rebound to another enrollment intent");
    assert!(intent_rebind.to_string().contains("different enrollment intent"));

    let actor_rebind = prepare_stream_ingest(&db, request_id, Some(&witness), "agent:b")
        .await
        .expect_err("one request id cannot be rebound to another actor");
    assert!(actor_rebind.to_string().contains("another actor"));
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_after_replay,
        "validated replay conflicts must remain graph-effect-free"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn prepare_stream_ingest_rearms_same_request_after_zero_effect_crash() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_unenrolled_served_with_schema(STREAM_SCHEMA).await;
    let request_id = "18181818-1818-4818-8818-181818181818";
    let (_, witness, _) = prepare_stream_ingest(&db, request_id, None, "agent:a")
        .await
        .unwrap();
    let witness = witness.expect("challenge returns an opaque witness");

    {
        let _before_index =
            ScopedFailPoint::new(names::STREAM_ENROLLMENT_POST_SIDECAR_PRE_INDEX, "return");
        prepare_stream_ingest(&db, request_id, Some(&witness), "agent:a")
            .await
            .expect_err("the pre-index crash must leave a zero-effect sidecar");
    }
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1
    );

    let (outcome, _, incarnation) =
        prepare_stream_ingest(&db, request_id, Some(&witness), "agent:a")
            .await
            .expect("the same request may re-arm after zero-effect retirement");
    assert_eq!(outcome, "enrolled");
    assert!(incarnation.is_some());
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn prepare_stream_ingest_refreshes_a_stale_witness_effect_free() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_unenrolled_served_with_schema(STREAM_SCHEMA).await;
    let request_id = "16161616-1616-4616-8616-161616161616";
    let (_, witness, _) = prepare_stream_ingest(&db, request_id, None, "agent:a")
        .await
        .unwrap();
    let witness = witness.expect("challenge returns an opaque witness");
    load_jsonl(
        &db,
        r#"{"type":"Person","data":{"id":"moved-head","score":5}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("the checked runtime may move an absent lane's ordinary table head");
    let manifest_after_write = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let inventory_before_retry = mem_wal_inventory(db.uri()).await;
    let (outcome, refreshed, incarnation) =
        prepare_stream_ingest(&db, request_id, Some(&witness), "agent:a")
            .await
            .expect("stale eligibility must return fresh compare evidence");
    assert_eq!(outcome, "witness_required");
    assert_ne!(refreshed.as_deref(), Some(witness.as_str()));
    assert!(incarnation.is_none());
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_after_write
    );
    assert_eq!(
        mem_wal_inventory(db.uri()).await.objects,
        inventory_before_retry.objects
    );
    let mut foreign_witness: serde_json::Value =
        serde_json::from_str(&witness).expect("the opaque witness is valid JSON");
    foreign_witness["graph_identity_digest"] = serde_json::json!(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    let foreign_witness =
        serde_json::to_string(&foreign_witness).expect("foreign witness remains valid JSON");
    let foreign = prepare_stream_ingest(&db, request_id, Some(&foreign_witness), "agent:a")
        .await
        .expect_err("one request occurrence cannot be retargeted to another graph lifetime");
    assert!(
        matches!(
            foreign,
            OmniError::Manifest(ref error)
                if matches!(
                    error.details,
                    Some(omnigraph::error::ManifestConflictDetails::ReadSetChanged { .. })
                )
        ),
        "{foreign:?}"
    );
    assert!(db.stream_status().await.unwrap().tables.is_empty());
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_prepare_stream_ingest_converges_on_one_lane() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_unenrolled_served_with_schema(STREAM_SCHEMA).await;
    let request_a = "13131313-1313-4313-8313-131313131313";
    let request_b = "14141414-1414-4414-8414-141414141414";
    let (_, witness, _) = prepare_stream_ingest(&db, request_a, None, "agent:a")
        .await
        .unwrap();
    let witness = witness.expect("challenge returns an opaque witness");
    let manifest_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let (left, right) = tokio::join!(
        prepare_stream_ingest(&db, request_a, Some(&witness), "agent:a"),
        prepare_stream_ingest(&db, request_b, Some(&witness), "agent:a"),
    );
    let mut outcomes = vec![left.unwrap(), right.unwrap()];
    outcomes.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(outcomes[0].0, "already_enrolled");
    assert_eq!(outcomes[1].0, "enrolled");
    assert_eq!(outcomes[0].2, outcomes[1].2);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before + 1,
        "exactly one enrollment publication must win"
    );
    let status = db.stream_status().await.unwrap();
    assert_eq!(status.tables.len(), 1);
    assert_eq!(status.tables[0].lifecycle, "OPEN");
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn prepare_stream_ingest_rejects_blob_table_before_enrollment() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_unenrolled_served_with_schema(BLOB_STREAM_SCHEMA).await;
    let error = prepare_stream_ingest(&db, "15151515-1515-4515-8515-151515151515", None, "agent:a")
        .await
        .expect_err("Blob-bearing tables cannot open a lane before fold supports Blob");
    assert!(
        error.to_string().contains("Blob-bearing table"),
        "{error:?}"
    );
    assert!(db.stream_status().await.unwrap().tables.is_empty());
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn authorized_ingest_checks_policy_then_runtime_before_input() {
    const STREAM_POLICY: &str = r#"
version: 1
groups:
  ingesters: [act-allowed]
  observers: [act-denied]
rules:
  - id: stream-ingesters
    allow:
      actors: { group: ingesters }
      actions: [stream_ingest]
"#;

    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let policy_path = dir.path().join("stream-policy.yaml");
    std::fs::write(&policy_path, STREAM_POLICY).unwrap();
    let policy = PolicyEngine::load_graph(&policy_path, dir.path().to_str().unwrap()).unwrap();
    let db = match Arc::try_unwrap(db) {
        Ok(db) => db,
        Err(_) => panic!("policy fixture must own the sole engine handle"),
    };
    let db = Arc::new(db.with_policy(Arc::new(policy) as Arc<dyn PolicyChecker>));
    let manifest_before_refusals = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    {
        // Both refusals must happen before recovery, JSON parsing, or a Lance put.
        let _must_not_enter_recovery =
            ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
        let _must_not_invoke_put =
            ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
        let denied_prepare = prepare_stream_ingest(
            &db,
            "20202020-2020-4020-8020-202020202020",
            None,
            "act-denied",
        )
        .await
        .expect_err("Cedar must reject prepare before recovery");
        assert!(
            matches!(denied_prepare, OmniError::Policy(_)),
            "{denied_prepare:?}"
        );
        let denied = db
            .failpoint_stream_ingest_one_as_for_test(
                TABLE,
                b"{",
                1,
                "act-denied",
            )
            .await
            .expect_err("Cedar must reject an actor without stream_ingest");
        assert!(matches!(denied, OmniError::Policy(_)), "{denied:?}");

        let no_runtime_prepare = prepare_stream_ingest(
            &db,
            "21212121-2121-4121-8121-212121212121",
            None,
            "act-allowed",
        )
        .await
        .expect_err("prepare requires an exact checked serving runtime");
        assert!(
            matches!(
                no_runtime_prepare,
                OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "ENABLED"
            ),
            "{no_runtime_prepare:?}"
        );
        let no_runtime = db
            .failpoint_stream_ingest_one_as_for_test(
                TABLE,
                b"{",
                2,
                "act-allowed",
            )
            .await
            .expect_err("an enabled ambient handle is not a serving runtime");
        assert!(
            matches!(
                no_runtime,
                OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "ENABLED"
            ),
            "{no_runtime:?}"
        );
    }
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before_refusals,
        "authorization/runtime refusals must be graph-effect-free"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn ndjson_fragmented_distinct_rows_share_one_physical_attempt_and_fold_in_order() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();

    let body = [
        ndjson_score_line(
            &incarnation,
            "ndjson-a",
            11,
            "60606060-6060-4060-8060-606060606061",
            None,
        ),
        ndjson_score_line(
            &incarnation,
            "ndjson-b",
            12,
            "60606060-6060-4060-8060-606060606062",
            None,
        ),
        ndjson_score_line(
            &incarnation,
            "ndjson-c",
            13,
            "60606060-6060-4060-8060-606060606063",
            None,
        ),
    ]
    .concat();
    let first_cut = 17;
    let second_cut = body.len() - 9;
    let outcomes = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_ndjson_as_for_test(
            TABLE,
            vec![
                body[..first_cut].to_vec(),
                body[first_cut..second_cut].to_vec(),
                body[second_cut..].to_vec(),
            ],
            "agent:ndjson-batch",
        )
        .await
        .expect("fragmented NDJSON must admit one distinct-key physical run"),
    );

    assert_eq!(outcomes.len(), 3);
    for (ordinal, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome["ordinal"], ordinal as u64);
        assert_eq!(outcome["status"], "durable");
        assert!(outcome["stream_token"].as_str().is_some());
        assert_eq!(
            outcome["unconfirmed_candidate_token"],
            serde_json::Value::Null
        );
        assert_eq!(outcome["origin"]["kind"], "admission");
        assert_eq!(
            outcome["origin"]["admission_attempt_id"],
            outcome["admission_attempt_id"]
        );
        assert!(outcome["enrollment_id"].as_str().is_some());
        assert!(outcome["shard_id"].as_str().is_some());
        assert!(
            outcome["writer_epoch"]
                .as_u64()
                .is_some_and(|epoch| epoch > 0)
        );
        assert_eq!(outcome["terminal_correction"], serde_json::Value::Null);
    }
    let attempts = outcomes
        .iter()
        .map(|outcome| {
            outcome["admission_attempt_id"]
                .as_str()
                .expect("durable result carries its physical attempt id")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        attempts.len(),
        1,
        "all three rows must share one physical Lance invocation"
    );
    assert!(
        visible_rows(&db).await.is_empty(),
        "stream durability remains invisible until fold publication"
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the multi-row physical run must fold through the existing path");
    assert_eq!(
        visible_rows(&db).await,
        vec![
            ("ndjson-a".to_string(), 11),
            ("ndjson-b".to_string(), 12),
            ("ndjson-c".to_string(), 13),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn ndjson_cold_replay_refusal_reports_the_achieved_claim_epoch() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let replay = physical_batch(&db, &[("cold-replay-source".to_string(), 17)]).await;
    let retired =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_AFTER_RETIREMENT_RELEASE);

    let error = {
        let _after_invoke =
            ScopedFailPoint::new(names::STREAM_B1_AFTER_PUT_INVOKE_BEFORE_WATCHER, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(replay), 0)
            .await
            .expect_err("the seed invocation must become acknowledgement-ambiguous")
    };
    assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
    retired.wait_until_reached().await;
    retired.release();

    let epoch_before_replay_claim = epoch_floor(&stream_lane(&db).await);
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let outcomes = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_ndjson_as_for_test(
            TABLE,
            vec![ndjson_score_line(
                &incarnation,
                "must-wait-for-replay-fold",
                18,
                "67676767-6767-4767-8767-676767676761",
                None,
            )],
            "agent:ndjson-cold-replay",
        )
        .await
        .expect("cold replay refusal is an ordered per-line result"),
    );

    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0]["status"], "stream_fold_required");
    let reported_epoch = outcomes[0]["writer_epoch"]
        .as_u64()
        .expect("a post-claim refusal must carry current writer authority");
    let achieved_epoch = epoch_floor(&stream_lane(&db).await);
    assert!(
        achieved_epoch > epoch_before_replay_claim,
        "cold replay classification must first publish a successor writer claim"
    );
    assert_eq!(
        reported_epoch, achieved_epoch,
        "the refusal must not report the pre-claim writer epoch"
    );
    assert!(outcomes[0]["enrollment_id"].as_str().is_some());
    assert!(outcomes[0]["shard_id"].as_str().is_some());
    assert!(visible_rows(&db).await.is_empty());
}

#[tokio::test]
#[serial]
async fn ndjson_invalid_lines_and_token_boundaries_preserve_caller_order() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let actor = "agent:ndjson-boundaries";
    let existing_write = "61616161-6161-4161-8161-616161616161";
    let (existing_token, already_durable) = authorized_ingest_score(
        &db,
        &incarnation,
        "ndjson-existing",
        21,
        90,
        existing_write,
        None,
        actor,
    )
    .await
    .expect("seed one current token");
    assert!(!already_durable);

    let body = [
        ndjson_score_line(
            &incarnation,
            "ndjson-before-invalid",
            22,
            "62626262-6262-4262-8262-626262626261",
            None,
        ),
        b"{\n".to_vec(),
        ndjson_score_line(
            &incarnation,
            "ndjson-after-invalid",
            23,
            "62626262-6262-4262-8262-626262626262",
            None,
        ),
        ndjson_score_line(&incarnation, "ndjson-existing", 21, existing_write, None),
        ndjson_score_line(
            &incarnation,
            "ndjson-existing",
            24,
            "62626262-6262-4262-8262-626262626264",
            None,
        ),
        ndjson_score_line(&incarnation, "ndjson-existing", 99, existing_write, None),
    ]
    .concat();
    let outcomes = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_ndjson_as_for_test(TABLE, vec![body], actor)
            .await
            .expect("line-local failures and token boundaries are request outcomes"),
    );

    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome["status"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "durable",
            "invalid",
            "durable",
            "already_durable",
            "stream_sequence_conflict",
            "stream_idempotency_conflict",
        ]
    );
    for (ordinal, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome["ordinal"], ordinal as u64);
    }
    assert_eq!(outcomes[3]["stream_token"], existing_token);
    assert_eq!(outcomes[3]["origin"]["kind"], "admission");
    assert_eq!(outcomes[4]["current_token"], existing_token);
    assert_eq!(outcomes[5]["current_token"], existing_token);
    for outcome in &outcomes[3..] {
        assert!(outcome["enrollment_id"].as_str().is_some());
        assert!(outcome["shard_id"].as_str().is_some());
        assert!(
            outcome["writer_epoch"]
                .as_u64()
                .is_some_and(|epoch| epoch > 0)
        );
    }
    assert_ne!(
        outcomes[0]["admission_attempt_id"], outcomes[2]["admission_attempt_id"],
        "an invalid line must close the preceding physical run"
    );
}

#[tokio::test]
#[serial]
async fn ndjson_ack_unknown_stops_admission_but_local_invalidity_still_wins() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let body = [
        ndjson_score_line(
            &incarnation,
            "ndjson-ambiguous",
            31,
            "63636363-6363-4363-8363-636363636361",
            None,
        ),
        b"{\n".to_vec(),
        ndjson_score_line(
            &incarnation,
            "ndjson-uninvoked-tail",
            32,
            "63636363-6363-4363-8363-636363636362",
            None,
        ),
    ]
    .concat();

    let outcomes = {
        let _after_durable = ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
        parse_ndjson_outcomes(
            db.failpoint_stream_ingest_ndjson_as_for_test(
                TABLE,
                vec![body],
                "agent:ndjson-ambiguous",
            )
            .await
            .expect("AckUnknown is an ordered per-line result, not a request error"),
        )
    };

    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome["status"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["ack_unknown", "invalid", "stream_retry_required"]
    );
    assert!(
        outcomes[0]["unconfirmed_candidate_token"]
            .as_str()
            .is_some()
    );
    assert_eq!(outcomes[0]["stream_token"], serde_json::Value::Null);
    assert_eq!(outcomes[1]["blocking_ordinal"], serde_json::Value::Null);
    assert_eq!(outcomes[2]["blocking_ordinal"], 0);
    assert_eq!(outcomes[2]["blocking_status"], "ack_unknown");
    assert_eq!(
        outcomes[2]["blocking_admission_attempt_id"],
        outcomes[0]["admission_attempt_id"]
    );
    assert!(outcomes[0]["enrollment_id"].as_str().is_some());
    assert!(outcomes[0]["shard_id"].as_str().is_some());
    assert!(
        outcomes[0]["writer_epoch"]
            .as_u64()
            .is_some_and(|epoch| epoch > 0)
    );
    assert_eq!(
        outcomes[2]["blocking_enrollment_id"],
        outcomes[0]["enrollment_id"]
    );
    assert_eq!(outcomes[2]["blocking_shard_id"], outcomes[0]["shard_id"]);
    assert_eq!(
        outcomes[2]["blocking_writer_epoch"],
        outcomes[0]["writer_epoch"]
    );
    assert_eq!(
        outcomes[2]["stream_token"],
        serde_json::Value::Null,
        "an uninvoked tail must not inherit a confirmed token"
    );
    assert_eq!(
        outcomes[2]["unconfirmed_candidate_token"],
        serde_json::Value::Null,
        "an uninvoked tail must not inherit another occurrence's candidate"
    );
}

#[tokio::test]
#[serial]
async fn ndjson_disconnect_before_body_poll_never_invokes_a_physical_put() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let before_put = helpers::failpoint::Rendezvous::park_first(names::STREAM_B1_BEFORE_PUT_INVOKE);

    db.failpoint_stream_ingest_ndjson_cancel_for_test(
        TABLE,
        vec![ndjson_score_line(
            &incarnation,
            "ndjson-cancelled",
            41,
            "64646464-6464-4464-8464-646464646461",
            None,
        )],
        "agent:ndjson-cancelled",
    )
    .await
    .expect("closing the result consumer must join the request task");

    assert!(
        !before_put.reached(),
        "disconnect must win before the request task polls or invokes the body"
    );
    before_put.release();
    assert!(visible_rows(&db).await.is_empty());
}

#[tokio::test]
#[serial]
async fn ndjson_intrinsic_token_oversize_outranks_ack_unknown_blocker() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    // The normalized/stored row remains below the 32-MiB generation bound,
    // while the exact authority projection repeats this key in its canonical
    // recovery payload and therefore cannot fit an empty legal token plan.
    let oversized_id = "x".repeat(17 * 1024 * 1024);
    let body = [
        ndjson_score_line(
            &incarnation,
            "ndjson-ambiguous-before-oversize",
            51,
            "65656565-6565-4565-8565-656565656561",
            None,
        ),
        ndjson_score_line(
            &incarnation,
            &oversized_id,
            52,
            "65656565-6565-4565-8565-656565656562",
            None,
        ),
    ]
    .concat();

    let outcomes = {
        let _after_durable = ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
        parse_ndjson_outcomes(
            db.failpoint_stream_ingest_ndjson_as_for_test(
                TABLE,
                vec![body],
                "agent:ndjson-oversize",
            )
            .await
            .expect("intrinsic oversize is a line result after AckUnknown"),
        )
    };
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome["status"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["ack_unknown", "stream_input_too_large"]
    );
    assert_eq!(outcomes[1]["blocking_ordinal"], serde_json::Value::Null);
    assert!(
        outcomes[1]["actual"].as_u64().unwrap() > outcomes[1]["limit"].as_u64().unwrap(),
        "the terminal result must report the exact violated empty-plan bound"
    );
}

#[tokio::test]
#[serial]
async fn authorized_json_ingest_rejects_invalid_shape_and_reserved_fields_effect_free() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(NULLABLE_STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let envelope = serde_json::json!({
        "stream_incarnation_id": incarnation,
        "write_id": "30303030-3030-4030-8030-303030303030",
        "predecessor_token": null,
    });
    let envelope_json = serde_json::to_string(&envelope).unwrap();
    let valid_row = || {
        serde_json::json!({
            "$stream": envelope.clone(),
            "id": "strict-row",
            "score": 3,
        })
    };
    let cases = vec![
        ("malformed JSON", b"{".to_vec(), "invalid stream JSON"),
        ("non-object JSON", b"[]".to_vec(), "invalid stream JSON"),
        (
            "missing envelope",
            serde_json::to_vec(&serde_json::json!({"id": "strict-row", "score": 3})).unwrap(),
            "invalid stream JSON",
        ),
        (
            "non-object envelope",
            serde_json::to_vec(&serde_json::json!({
                "$stream": [],
                "id": "strict-row",
                "score": 3,
            }))
            .unwrap(),
            "invalid stream JSON",
        ),
        (
            "missing predecessor",
            serde_json::to_vec(&serde_json::json!({
                "$stream": {
                    "stream_incarnation_id": incarnation,
                    "write_id": "30303030-3030-4030-8030-303030303030",
                },
                "id": "strict-row",
                "score": 3,
            }))
            .unwrap(),
            "invalid stream JSON",
        ),
        (
            "duplicate envelope field",
            format!(
                r#"{{"$stream":{{"stream_incarnation_id":"{incarnation}","write_id":"30303030-3030-4030-8030-303030303030","write_id":"31313131-3131-4131-8131-313131313131","predecessor_token":null}},"id":"strict-row","score":3}}"#
            )
            .into_bytes(),
            "invalid stream JSON",
        ),
        (
            "duplicate envelope",
            format!(
                r#"{{"$stream":{envelope_json},"$stream":{envelope_json},"id":"strict-row","score":3}}"#
            )
            .into_bytes(),
            "invalid stream JSON",
        ),
        (
            "duplicate row field",
            format!(
                r#"{{"$stream":{envelope_json},"id":"strict-row","id":"forged","score":3}}"#
            )
            .into_bytes(),
            "invalid stream JSON",
        ),
        (
            "body-supplied contributor",
            serde_json::to_vec(&serde_json::json!({
                "$stream": {
                    "stream_incarnation_id": incarnation,
                    "write_id": "30303030-3030-4030-8030-303030303030",
                    "predecessor_token": null,
                    "contributor_id": "forged",
                },
                "id": "strict-row",
                "score": 3,
            }))
            .unwrap(),
            "invalid stream JSON",
        ),
        (
            "body-supplied origin",
            serde_json::to_vec(&serde_json::json!({
                "$stream": {
                    "stream_incarnation_id": incarnation,
                    "write_id": "30303030-3030-4030-8030-303030303030",
                    "predecessor_token": null,
                    "origin": {"kind": "admission"},
                },
                "id": "strict-row",
                "score": 3,
            }))
            .unwrap(),
            "invalid stream JSON",
        ),
        (
            "nil stream incarnation",
            serde_json::to_vec(&serde_json::json!({
                "$stream": {
                    "stream_incarnation_id": "00000000-0000-0000-0000-000000000000",
                    "write_id": "30303030-3030-4030-8030-303030303030",
                    "predecessor_token": null,
                },
                "id": "strict-row",
                "score": 3,
            }))
            .unwrap(),
            "must be non-nil",
        ),
        (
            "noncanonical write id",
            serde_json::to_vec(&serde_json::json!({
                "$stream": {
                    "stream_incarnation_id": incarnation,
                    "write_id": "30303030-3030-4030-8030-30303030303A",
                    "predecessor_token": null,
                },
                "id": "strict-row",
                "score": 3,
            }))
            .unwrap(),
            "canonical lowercase",
        ),
        (
            "malformed predecessor token",
            serde_json::to_vec(&serde_json::json!({
                "$stream": {
                    "stream_incarnation_id": incarnation,
                    "write_id": "30303030-3030-4030-8030-303030303030",
                    "predecessor_token": "sha256:bad",
                },
                "id": "strict-row",
                "score": 3,
            }))
            .unwrap(),
            "invalid stream JSON",
        ),
        (
            "missing id",
            serde_json::to_vec(&serde_json::json!({
                "$stream": envelope.clone(),
                "score": 3,
            }))
            .unwrap(),
            "requires explicit field 'id'",
        ),
        (
            "null id",
            serde_json::to_vec(&serde_json::json!({
                "$stream": envelope.clone(),
                "id": null,
                "score": 3,
            }))
            .unwrap(),
            "requires non-null string field 'id'",
        ),
        (
            "non-string id",
            serde_json::to_vec(&serde_json::json!({
                "$stream": envelope.clone(),
                "id": 7,
                "score": 3,
            }))
            .unwrap(),
            "field 'id' must be a string",
        ),
        (
            "unknown field",
            serde_json::to_vec(&{
                let mut row = valid_row();
                row.as_object_mut()
                    .unwrap()
                    .insert("typo".to_string(), serde_json::json!(true));
                row
            })
            .unwrap(),
            "unknown stream input field 'typo'",
        ),
        (
            "Lance tombstone",
            serde_json::to_vec(&{
                let mut row = valid_row();
                row.as_object_mut()
                    .unwrap()
                    .insert("_tombstone".to_string(), serde_json::json!(false));
                row
            })
            .unwrap(),
            "reserved physical state",
        ),
        (
            "trusted stream metadata",
            serde_json::to_vec(&{
                let mut row = valid_row();
                row.as_object_mut().unwrap().insert(
                    "__omnigraph_stream_v1$".to_string(),
                    serde_json::json!("forged"),
                );
                row
            })
            .unwrap(),
            "reserved physical state",
        ),
        (
            "case-insensitive trusted stream metadata",
            serde_json::to_vec(&{
                let mut row = valid_row();
                row.as_object_mut().unwrap().insert(
                    "__OMNIGRAPH_STREAM_V1$".to_string(),
                    serde_json::json!("forged"),
                );
                row
            })
            .unwrap(),
            "reserved physical state",
        ),
        (
            "Lance virtual column",
            serde_json::to_vec(&{
                let mut row = valid_row();
                row.as_object_mut()
                    .unwrap()
                    .insert("_rowid".to_string(), serde_json::json!(1));
                row
            })
            .unwrap(),
            "reserved physical state",
        ),
        (
            "wrong nullable scalar type",
            serde_json::to_vec(&serde_json::json!({
                "$stream": envelope.clone(),
                "id": "strict-row",
                "score": "not-an-int",
            }))
            .unwrap(),
            "expects Int32",
        ),
    ];

    let manifest_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let inventory_before = mem_wal_inventory(db.uri()).await;
    let _must_not_enter_recovery =
        ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
    let _must_not_invoke_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
    for (ordinal, (case, raw_json, expected)) in cases.into_iter().enumerate() {
        let error = db
            .failpoint_stream_ingest_one_as_for_test(
                TABLE,
                &raw_json,
                ordinal as u64,
                "agent:strict",
            )
            .await
            .expect_err(case);
        assert!(error.to_string().contains(expected), "{case}: {error:?}");
    }

    let many_values = format!("{}0", "0,".repeat(140_000));
    let structurally_amplified = format!(
        r#"{{"$stream":{envelope_json},"id":"strict-row","score":[{many_values}]}}"#
    );
    let error = db
        .failpoint_stream_ingest_one_as_for_test(
            TABLE,
            structurally_amplified.as_bytes(),
            98,
            "agent:strict",
        )
        .await
        .expect_err("a small raw body may not amplify into an unbounded JSON DOM");
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 131_072,
                actual: 131_073,
            } if resource == "stream_json_structural_slots"
        ),
        "{error:?}"
    );

    let oversized = vec![b' '; (32 * 1024 * 1024) + 1];
    let error = db
        .failpoint_stream_ingest_one_as_for_test(
            TABLE,
            &oversized,
            99,
            "agent:strict",
        )
        .await
        .expect_err("one raw JSON line above 32 MiB must fail before parsing");
    assert!(
        matches!(
            error,
            OmniError::ResourceLimitExceeded {
                ref resource,
                limit: 33_554_432,
                actual: 33_554_433,
            } if resource == "stream raw JSON bytes"
        ),
        "{error:?}"
    );

    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before,
        "invalid stream JSON must not move graph authority"
    );
    let inventory_after = mem_wal_inventory(db.uri()).await;
    assert_eq!(
        inventory_after.objects, inventory_before.objects,
        "invalid stream JSON must not move MemWAL object paths, classes, or sizes"
    );
    assert_eq!(
        inventory_after.generation_roots, inventory_before.generation_roots,
        "invalid stream JSON must not create a generation"
    );
    assert_eq!(
        inventory_after.referenced_generation_roots,
        inventory_before.referenced_generation_roots,
        "invalid stream JSON must not move generation authority"
    );
    assert!(visible_rows(&db).await.is_empty());
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn authorized_json_ingest_normalizes_and_folds_one_edge() {
    let _scenario = FailScenario::setup();
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), EDGE_STREAM_SCHEMA)
            .await
            .unwrap(),
    );
    load_jsonl(
        &db,
        r#"{"type":"Person","data":{"name":"Alice"}}
{"type":"Person","data":{"name":"Bob"}}"#,
        LoadMode::Overwrite,
    )
    .await
    .expect("seed valid endpoints before enrolling the edge table");
    db.failpoint_enroll_stream_table_for_test(MIN_CARD_EDGE_TABLE)
        .await
        .unwrap();
    let cluster_uri = format!("file://{}", cluster.path().display());
    enable_stream_profile(&db, &cluster_uri).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };
    let incarnation = db
        .failpoint_stream_incarnation_for_test(MIN_CARD_EDGE_TABLE)
        .await
        .unwrap();
    {
        let _must_not_enter_recovery =
            ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
        let invalid = [
            (
                "missing src",
                serde_json::json!({
                    "$stream": {
                        "stream_incarnation_id": incarnation,
                        "write_id": "41414141-4141-4141-8141-414141414141",
                        "predecessor_token": null,
                    },
                    "id": "edge-missing-src",
                    "dst": "Bob",
                }),
                "requires explicit field 'src'",
            ),
            (
                "non-string dst",
                serde_json::json!({
                    "$stream": {
                        "stream_incarnation_id": incarnation,
                        "write_id": "42424242-4242-4242-8242-424242424242",
                        "predecessor_token": null,
                    },
                    "id": "edge-bad-dst",
                    "src": "Alice",
                    "dst": 7,
                }),
                "field 'dst' must be a string",
            ),
        ];
        for (case, row, expected) in invalid {
            let error = db
                .failpoint_stream_ingest_one_as_for_test(
                    MIN_CARD_EDGE_TABLE,
                    &serde_json::to_vec(&row).unwrap(),
                    0,
                    "agent:edge",
                )
                .await
                .expect_err(case);
            assert!(error.to_string().contains(expected), "{case}: {error:?}");
        }
    }
    let raw_json = serde_json::to_vec(&serde_json::json!({
        "$stream": {
            "stream_incarnation_id": incarnation,
            "write_id": "40404040-4040-4040-8040-404040404040",
            "predecessor_token": null,
        },
        "id": "edge-1",
        "src": "Alice",
        "dst": "Bob",
    }))
    .unwrap();
    let (_, already_durable) = db
        .failpoint_stream_ingest_one_as_for_test(
            MIN_CARD_EDGE_TABLE,
            &raw_json,
            0,
            "agent:edge",
        )
        .await
        .expect("the strict normalizer must admit the accepted edge shape");
    assert!(!already_durable);
    assert!(
        helpers::read_table(&db, MIN_CARD_EDGE_TABLE)
            .await
            .is_empty(),
        "durable MemWAL acknowledgement must not bypass fold visibility"
    );

    db.failpoint_stream_b1_for_test(MIN_CARD_EDGE_TABLE, None, 0)
        .await
        .expect("the normalized edge must fold through the existing B2 path");
    let batches = helpers::read_table(&db, MIN_CARD_EDGE_TABLE).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = &batches[0];
    let string_value = |column: &str| {
        batch
            .column_by_name(column)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    assert_eq!(string_value("id"), "edge-1");
    assert_eq!(string_value("src"), "Alice");
    assert_eq!(string_value("dst"), "Bob");
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn authorized_json_ingest_normalizes_typed_values_and_rejects_value_errors() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(TYPED_STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let valid = serde_json::json!({
        "$stream": {
            "stream_incarnation_id": incarnation,
            "write_id": "50505050-5050-4050-8050-505050505050",
            "predecessor_token": null,
        },
        "id": "doc-1",
        "slug": "doc-1",
        "title": "Accepted",
        "score": 7,
        "state": "open",
        "tags": ["stream", "strict"],
        "embedding": [0.25, 0.75],
    });
    db.failpoint_stream_ingest_one_as_for_test(
        TABLE,
        &serde_json::to_vec(&valid).unwrap(),
        0,
        "agent:typed",
    )
    .await
    .expect("vectors, lists, and enums must normalize through the one-row seam");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the normalized typed row must fold through the existing core");
    let batches = helpers::read_table(&db, TABLE).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let batch = &batches[0];
    let score = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(score.value(0), 7);
    let state = batch
        .column_by_name("state")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(state.value(0), "open");
    let tags = batch
        .column_by_name("tags")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(tags.value(0), "stream");
    assert_eq!(tags.value(1), "strict");
    let embedding = batch
        .column_by_name("embedding")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .value(0);
    let embedding = embedding.as_any().downcast_ref::<Float32Array>().unwrap();
    assert_eq!([embedding.value(0), embedding.value(1)], [0.25, 0.75]);

    let invalid_row = |write_id: &str, id: &str, field: &str, value: serde_json::Value| {
        let mut row = valid.clone();
        let object = row.as_object_mut().unwrap();
        object.insert("id".to_string(), serde_json::json!(id));
        object.insert("slug".to_string(), serde_json::json!(id));
        object.insert(field.to_string(), value);
        object
            .get_mut("$stream")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("write_id".to_string(), serde_json::json!(write_id));
        row
    };
    let mut missing_embedding = invalid_row(
        "51515151-5151-4151-8151-515151515151",
        "doc-missing-vector",
        "score",
        serde_json::json!(5),
    );
    missing_embedding
        .as_object_mut()
        .unwrap()
        .remove("embedding");
    let mut mismatched_key = invalid_row(
        "53535353-5353-4353-8353-535353535353",
        "doc-key",
        "score",
        serde_json::json!(5),
    );
    mismatched_key
        .as_object_mut()
        .unwrap()
        .insert("slug".to_string(), serde_json::json!("different-key"));
    let invalid = vec![
        (
            "caller-supplied embed vector",
            missing_embedding,
            "requires caller-supplied @embed vector",
        ),
        (
            "vector dimension",
            invalid_row(
                "52525252-5252-4252-8252-525252525252",
                "doc-vector",
                "embedding",
                serde_json::json!([0.5]),
            ),
            "expects 2 dimensions",
        ),
        (
            "canonical key",
            mismatched_key,
            "does not match its canonical @key id",
        ),
        (
            "enum",
            invalid_row(
                "54545454-5454-4454-8454-545454545454",
                "doc-enum",
                "state",
                serde_json::json!("unknown"),
            ),
            "invalid enum value",
        ),
        (
            "range",
            invalid_row(
                "55555555-5555-4555-8555-555555555555",
                "doc-range",
                "score",
                serde_json::json!(11),
            ),
            "@range violation",
        ),
        (
            "check",
            invalid_row(
                "56565656-5656-4656-8656-565656565656",
                "doc-check",
                "title",
                serde_json::json!("lowercase"),
            ),
            "@check violation",
        ),
    ];
    let manifest_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let inventory_before = mem_wal_inventory(db.uri()).await;
    let _must_not_enter_recovery =
        ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
    let _must_not_invoke_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
    for (ordinal, (case, row, expected)) in invalid.into_iter().enumerate() {
        let error = db
            .failpoint_stream_ingest_one_as_for_test(
                TABLE,
                &serde_json::to_vec(&row).unwrap(),
                ordinal as u64 + 1,
                "agent:typed",
            )
            .await
            .expect_err(case);
        assert!(error.to_string().contains(expected), "{case}: {error:?}");
    }
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before,
        "normalization/value failures must not move graph authority"
    );
    let inventory_after = mem_wal_inventory(db.uri()).await;
    assert_eq!(
        inventory_after.objects, inventory_before.objects,
        "normalization/value failures must not move MemWAL inventory"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn blob_bearing_table_is_rejected_before_any_stream_put() {
    let _scenario = FailScenario::setup();
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let schema_owner = Omnigraph::init(graph.to_str().unwrap(), STREAM_SCHEMA)
        .await
        .unwrap();
    let stale_handle = Arc::new(Omnigraph::open(graph.to_str().unwrap()).await.unwrap());
    schema_owner
        .apply_schema(BLOB_STREAM_SCHEMA)
        .await
        .expect("the schema owner must add the nullable Blob property");
    load_jsonl(
        &schema_owner,
        r#"{"type":"Person","data":{"score":0,"content":null}}"#,
        LoadMode::Overwrite,
    )
    .await
    .expect("an ordinary keyed write must leave an enrollable post-apply head");
    schema_owner
        .failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    let cluster_uri = format!("file://{}", cluster.path().display());
    enable_stream_profile(&schema_owner, &cluster_uri).await;
    let db =
        helpers::stream_authority::bind_checked_stream_runtime(stale_handle, &cluster_uri).await;
    assert!(
        db.catalog()
            .node_types
            .get("Person")
            .unwrap()
            .blob_properties
            .is_empty(),
        "the exercising handle must retain its pre-apply public catalog"
    );
    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let manifest_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let inventory_before = mem_wal_inventory(db.uri()).await;
    let _must_not_enter_recovery =
        ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
    let _must_not_invoke_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");

    let raw = serde_json::json!({
        "$stream": {
            "stream_incarnation_id": incarnation,
            "write_id": "57575757-5757-4757-8757-575757575757",
            "predecessor_token": null,
        },
        "id": "asset-json",
        "score": 1,
        "content": null,
    });
    let error = db
        .failpoint_stream_ingest_one_as_for_test(
            TABLE,
            &serde_json::to_vec(&raw).unwrap(),
            0,
            "agent:blob",
        )
        .await
        .expect_err("the JSON seam must reject the whole Blob-bearing table");
    assert!(
        error.to_string().contains("Blob-bearing table")
            && error
                .to_string()
                .contains("fold cannot materialize Blob values"),
        "{error:?}"
    );

    let mut content = lance::blob::BlobArrayBuilder::new(1);
    content.push_null().unwrap();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
            lance::blob::blob_field("content", true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["asset-physical"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            content.finish().unwrap(),
        ],
    )
    .unwrap();
    let error = db
        .failpoint_stream_b1_for_test(TABLE, Some(batch.clone()), 1)
        .await
        .expect_err("the legacy lower B1 seam must enforce the same table-level refusal");
    assert!(
        error.to_string().contains("Blob-bearing table"),
        "{error:?}"
    );
    let error = db
        .failpoint_stream_b2_for_test(
            TABLE,
            batch,
            2,
            &incarnation,
            "58585858-5858-4858-8858-585858585858",
            None,
            "agent:blob",
        )
        .await
        .expect_err("the lower B2 seam must enforce the same table-level refusal");
    assert!(
        error.to_string().contains("Blob-bearing table"),
        "{error:?}"
    );

    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before,
        "Blob refusal must not move graph authority"
    );
    let inventory_after = mem_wal_inventory(db.uri()).await;
    assert_eq!(
        inventory_after.objects, inventory_before.objects,
        "Blob refusal must not move MemWAL inventory"
    );
    assert_no_recovery_sidecars(&dir);
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
        let _must_not_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
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
        let _must_not_put = ScopedFailPoint::new(names::STREAM_B1_BEFORE_PUT_INVOKE, "return");
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
        OmniError::StreamSequenceConflict { current_token, .. } => {
            assert_eq!(current_token.as_deref(), Some(token_y.as_str()))
        }
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
    let uri = format!(
        "{}/graphs/knowledge.omni",
        dir.path().to_str().unwrap().trim_end_matches('/')
    );
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
        let table_version_before = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry(TABLE)
            .unwrap()
            .table_version;

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
                .entry(TABLE)
                .unwrap()
                .table_version,
            table_version_before,
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
                .entry(TABLE)
                .unwrap()
                .table_version,
            table_version_before + 1,
            "only the recovery fold may advance the base table and make the acknowledged row visible"
        );
    })
    .await;
}

async fn provider_generation_failure_retains_unreferenced_generation_at(
    cluster_uri: &str,
    target: ProviderFailureTarget,
) {
    let uri = format!(
        "{}/graphs/knowledge.omni",
        cluster_uri.trim_end_matches('/')
    );
    let uri = uri.as_str();
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
        let row = physical_batch(&db, &[("retained-after-failure".to_string(), 41)]).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
            .await
            .expect("the row must be durably acknowledged before the fold failure");
        let snapshot_before_failure = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap();
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
        let snapshot_after_failure = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap();
        assert_eq!(
            snapshot_after_failure.version(),
            snapshot_before_failure.version(),
            "a flushed generation is not graph-visible without the graph manifest CAS"
        );
        assert_eq!(
            snapshot_after_failure.entry(TABLE).unwrap().table_version,
            snapshot_before_failure.entry(TABLE).unwrap().table_version,
            "provider-refused generation output cannot advance the base-table pointer"
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

/// RFC-026 §4.6: with a lane actually enrolled, status projects the durable
/// logical stream incarnation plus the `lifecycle_revision` that every
/// mutating management verb passes back as its expected-revision compare
/// token.
#[tokio::test]
#[serial]
async fn stream_status_reports_the_enrolled_lane_and_its_compare_token() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;

    let status = db.stream_status().await.unwrap();
    assert_eq!(status.tables.len(), 1, "one enrolled lane: {status:?}");
    let lane = &status.tables[0];
    assert_eq!(lane.table_key, TABLE);
    assert_eq!(lane.lifecycle, "OPEN");
    assert_eq!(
        lane.lifecycle_revision, 1,
        "enrollment publishes the initial lifecycle revision"
    );
    let expected_stream_incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    assert_eq!(
        lane.stream_incarnation_id, expected_stream_incarnation,
        "status returns the exact logical incarnation used to fence every stream request"
    );
    assert!(!lane.enrollment_id.is_empty());
    assert_ne!(
        lane.stream_incarnation_id, lane.enrollment_id,
        "logical stream incarnation and physical enrollment are distinct identities"
    );
    assert_eq!(
        lane.epoch_floor_by_shard.len(),
        1,
        "the unsharded profile enrolls exactly one shard"
    );
    // Nothing has folded or blocked yet, and the lane is not draining.
    assert_eq!(lane.drain_id, None);
    assert_eq!(lane.strict_block_token, None);
    assert_eq!(lane.last_fold_outcome, None);
    // An OPEN lane is undrained — the exact condition that makes a disable
    // refuse as pending-until-drained, and that quiesce will clear.
    assert!(status.undrained());

    // Status is lifecycle-inert: re-reading it moves nothing.
    assert_eq!(db.stream_status().await.unwrap(), status);
}
