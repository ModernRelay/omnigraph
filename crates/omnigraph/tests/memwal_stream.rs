#![cfg(feature = "failpoints")]

//! RFC-026 Phase-B1 private row-admission/fold integration owner.
//!
//! These tests reach the single doc-hidden graph seam. They intentionally do
//! not create a public SDK, schema, HTTP, or CLI streaming contract.

mod helpers;

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
use omnigraph::db::{
    CleanupPolicyOptions, GraphStreamChunkSource, GraphStreamIngestStart, GraphStreamPendingStatus,
    GraphStreamRebuildBlocker, Omnigraph, ReadTarget, StreamAuthorityRetirementPlan,
    StreamDataCorrectionAction, StreamDataCorrectionRequest, StreamOldestUncoveredAgeStatus,
    StreamPendingGenerationStatus, StreamRebuildBlockReason, StreamTablePhysicalOperationalStatus,
    StreamTableStatus, StreamTokenIndexCoverageStatus,
};
use omnigraph::error::OmniError;
use omnigraph::failpoints::{ScopedFailPoint, names};
use omnigraph::instrumentation::{QueryIoProbes, with_query_io_probes};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph_policy::{PolicyChecker, PolicyEngine};
use serial_test::serial;

use helpers::memwal::{CurrentMemWalInventory, MemWalObjectKind};
use helpers::stream_authority::{
    assert_offline_stream_profile_authority_available, bind_checked_served_export,
    bind_checked_unmanaged_terminal_export, confirm_stream_authority_retirement,
    disable_stream_profile, enable_stream_profile, plan_stream_authority_retirement,
    rebind_stream_table_offline, try_disable_stream_profile,
};

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
const F6B2_NODE_EDGE_SCHEMA: &str = r#"
node Person { score: I32 }
node Company { score: I32 }
edge Knows: Person -> Person
edge WorksAt: Company -> Person
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
const COMPANY_TABLE: &str = "node:Company";
const MIN_CARD_EDGE_TABLE: &str = "edge:Knows";
const WORKS_AT_EDGE_TABLE: &str = "edge:WorksAt";
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
const LEGACY_WRITER_MUTATIONS: &str = r#"
query legacy_insert($score: I32) {
    insert Person { score: $score }
}

query legacy_update($old_score: I32, $score: I32) {
    update Person set { score: $score } where score = $old_score
}

query legacy_delete($score: I32) {
    delete Person where score = $score
}
"#;
const LEGACY_WRITER_JSONL: &str = r#"{"type":"Person","data":{"id":"legacy-writer","score":99}}"#;

const PROVIDER_FAILURE_MESSAGE: &str = "injected RFC-026 provider exhaustion";
const PROVIDER_PUT_TIMEOUT: Duration = Duration::from_secs(45);
const PROVIDER_FOLD_TIMEOUT: Duration = Duration::from_secs(90);
const F6B2_PROCESS_MODE_ENV: &str = "OMNIGRAPH_F6B2_PROCESS_MODE";
const F6B2_PROCESS_GRAPH_ENV: &str = "OMNIGRAPH_F6B2_PROCESS_GRAPH";
const F6B2_PROCESS_CLUSTER_ENV: &str = "OMNIGRAPH_F6B2_PROCESS_CLUSTER";
const F6B2_PROCESS_READY_ENV: &str = "OMNIGRAPH_F6B2_PROCESS_READY";

/// Run one composed debug-build recovery scenario outside libtest's 2-MiB
/// thread. The production operations are independently exercised on ordinary
/// Tokio stacks; this helper bounds only the large future assembled by a test
/// that chains served shutdown and a complete offline fold/disable cycle.
fn on_big_stack<F>(body: impl FnOnce() -> F + Send + 'static)
where
    F: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(body());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Park the first two arrivals at one driver boundary independently.
///
/// The first stop lets a test add work after a finite round is frozen. The
/// second stop observes the completed first round before the newly triggered
/// lane can run. Both waits are condition-driven and bounded; no elapsed-time
/// guess determines correctness.
struct TwoRoundRendezvous {
    reached: [Arc<AtomicBool>; 2],
    release: [Arc<AtomicBool>; 2],
    _failpoint: ScopedFailPoint,
}

impl TwoRoundRendezvous {
    fn new() -> Self {
        let reached = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        let release = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        let hits = Arc::new(AtomicUsize::new(0));
        let callback_reached = reached.clone();
        let callback_release = release.clone();
        let _failpoint =
            ScopedFailPoint::with_callback(names::STREAM_DRIVER_POST_ROUND_FREEZE, move || {
                let hit = hits.fetch_add(1, Ordering::SeqCst);
                let (Some(reached), Some(release)) =
                    (callback_reached.get(hit), callback_release.get(hit))
                else {
                    return;
                };
                reached.store(true, Ordering::SeqCst);
                for _ in 0..6_000 {
                    if release.load(Ordering::SeqCst) {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                panic!("stream-driver round rendezvous {hit} timed out");
            });
        Self {
            reached,
            release,
            _failpoint,
        }
    }

    async fn wait_for(&self, round: usize) {
        let reached = self
            .reached
            .get(round)
            .unwrap_or_else(|| panic!("unknown stream-driver round {round}"));
        tokio::time::timeout(Duration::from_secs(20), async {
            while !reached.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("stream-driver round {round} was never frozen"));
    }

    fn release(&self, round: usize) {
        self.release
            .get(round)
            .unwrap_or_else(|| panic!("unknown stream-driver round {round}"))
            .store(true, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderFailureTarget {
    PartialGeneration,
    ShardManifest,
    WalEntry,
    BaseTableRead,
}

impl ProviderFailureTarget {
    fn matches(self, kind: ProviderAccessKind, location: &ObjectPath) -> bool {
        let path = location.to_string();
        let is_mem_wal = path.split('/').any(|part| part == "_mem_wal");
        match self {
            Self::PartialGeneration => {
                kind != ProviderAccessKind::Get
                    && is_mem_wal
                    && generation_root_in_path(&path).is_some()
                    && path.ends_with("/bloom_filter.bin")
            }
            Self::ShardManifest => {
                kind != ProviderAccessKind::Get
                    && is_mem_wal
                    && path.contains("/manifest/")
                    && !path.ends_with("version_hint.json")
            }
            Self::WalEntry => {
                kind != ProviderAccessKind::Get
                    && is_mem_wal
                    && path.contains("/wal/")
                    && path.contains(".arrow")
            }
            Self::BaseTableRead => kind == ProviderAccessKind::Get && !is_mem_wal,
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

/// Fails selected real object-store accesses made by Lance.
///
/// `QueryIoProbes` installs this on the table handle before the worker is
/// opened, and OmniGraph propagates that task-local wrapper into detached put
/// and fold owners. Write targets exercise those workers; the base-table read
/// target exercises export. Both cross the provider boundary rather than
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

    fn record_and_maybe_fail(
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
        if !target.matches(kind, location) {
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

struct ArmBaseReadFailureAfterFirstLine {
    output: Vec<u8>,
    failure: ProviderWriteFailure,
    armed: bool,
}

impl ArmBaseReadFailureAfterFirstLine {
    fn new(failure: ProviderWriteFailure) -> Self {
        Self {
            output: Vec::new(),
            failure,
            armed: false,
        }
    }
}

impl Write for ArmBaseReadFailureAfterFirstLine {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.output.extend_from_slice(bytes);
        if !self.armed && self.output.contains(&b'\n') {
            self.failure.arm(ProviderFailureTarget::BaseTableRead);
            self.armed = true;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
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
            .record_and_maybe_fail(ProviderAccessKind::Put, location)
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
            .record_and_maybe_fail(ProviderAccessKind::MultipartStart, location)
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
        if let Some(error) = self
            .failure
            .record_and_maybe_fail(ProviderAccessKind::Get, location)
        {
            return Err(error);
        }
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
            .record_and_maybe_fail(ProviderAccessKind::CopyTo, to)
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
    let db = Arc::new(
        Omnigraph::init(uri, STREAM_SCHEMA)
            .await
            .expect("provider-failure fixture must initialize"),
    );
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

async fn stream_lane_for(db: &Omnigraph, table_key: &str) -> StreamTableStatus {
    db.stream_status()
        .await
        .unwrap()
        .tables
        .into_iter()
        .find(|lane| lane.table_key == table_key)
        .unwrap_or_else(|| panic!("fixture has no stream lane for {table_key}"))
}

async fn checked_export_jsonl(
    db: &Arc<Omnigraph>,
    branch: &str,
) -> std::result::Result<String, OmniError> {
    db.capture_served_export_cut(branch, &[], &[])
        .await?
        .into_jsonl()
        .await
}

async fn checked_export_jsonl_to_bytes(
    db: &Arc<Omnigraph>,
    branch: &str,
    output: &mut Vec<u8>,
) -> std::result::Result<(), OmniError> {
    db.capture_served_export_cut(branch, &[], &[])
        .await?
        .write_to(output)
        .await
}

fn exported_type_row_count(export: &str, type_name: &str) -> usize {
    export
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|row| row.get("type").and_then(serde_json::Value::as_str) == Some(type_name))
        .count()
}

#[tokio::test]
#[serial]
async fn authority_retirement_unblocks_exact_provenance_rebuild_and_freezes_source() {
    let _scenario = FailScenario::setup();
    let fixture = prepare_authority_retirement_fixture().await;
    let (main_export, archive_export) = confirm_retirement_and_export_frozen_cut(&fixture).await;
    assert_retired_source_refuses_writers(&fixture).await;
    assert_retired_schema_staging_remains_untouched(&fixture).await;
    assert_retirement_confirmation_is_idempotent(&fixture).await;
    assert_retired_export_rebuilds_without_authority(&main_export).await;
    assert_retired_export_rebuilds_without_authority(&archive_export).await;
    assert_retired_export_rejects_tampered_member(&main_export).await;
    assert_retired_named_branch_healer_refuses_orphan_discard(&fixture).await;
}

#[test]
#[serial]
fn dead_letter_list_payload_retirement_and_export_form_one_operational_exit() {
    on_big_stack(|| async {
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
        let dir = EnrolledGraphDir {
            _cluster: cluster,
            graph,
        };
        let cluster_uri = dir.cluster_uri();
        enable_stream_profile(&db, &cluster_uri).await;
        let served = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;

        let logical_id = "dead-letter-operational-exit";
        let duplicate = physical_batch(&served, &[(logical_id.to_string(), 7)]).await;
        served
            .failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
            .await
            .expect("the conflicting occurrence must be durably acknowledged");
        let open = stream_lane(&served).await;
        served
            .failpoint_start_stream_open_after_fold_drain_for_test(
                TABLE,
                "d1d1d1d1-d1d1-41d1-81d1-d1d1d1d1d1d1",
                open.lifecycle_revision,
                "operator:dead-letter-exit",
            )
            .await
            .expect("the exact acknowledged generation must become the disable drain cut");
        served.shutdown_stream_fold_driver().await.unwrap();
        drop(served);

        let offline = reopen_enrolled(&dir).await;
        let disable_error = {
            let _fold_arm =
                ScopedFailPoint::new(names::STREAM_FOLD_POST_TOKEN_COMMIT_PRE_CONFIRM, "return");
            try_disable_stream_profile(&offline, &cluster_uri)
                .await
                .expect_err("disable must stop after committing the dead-letter token effect")
        };
        assert!(
            disable_error
                .to_string()
                .contains(names::STREAM_FOLD_POST_TOKEN_COMMIT_PRE_CONFIRM),
            "{disable_error:?}"
        );
        let draining = stream_lane(&offline).await;
        assert_eq!(draining.lifecycle, "DRAINING");
        assert_eq!(draining.last_fold_outcome, None);
        drop(offline);

        let offline = reopen_enrolled(&dir).await;
        let recovered = stream_lane(&offline).await;
        assert_eq!(recovered.lifecycle, "DRAINING");
        assert_eq!(recovered.last_fold_outcome.as_deref(), Some("PUBLISHED"));
        assert_no_recovery_sidecars(&dir);
        disable_stream_profile(&offline, &cluster_uri).await;
        let status = offline.stream_status().await.unwrap();
        assert_eq!(status.profile_mode, "DISABLED");
        assert_eq!(status.tables[0].lifecycle, "SEALED");
        let visible = visible_rows(&offline).await;
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].1, 7);
        assert_ne!(
            visible[0].0, logical_id,
            "the losing occurrence must not become a visible base row"
        );

        let mut ambient_output = Vec::new();
        let ambient = offline
            .export_jsonl_to_writer("main", &[], &[], &mut ambient_output)
            .await
            .expect_err("an ambient enrolled export must refuse before output");
        assert!(matches!(
            ambient,
            OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "DISABLED"
        ));
        assert!(ambient_output.is_empty());

        let ambient_cut = match offline.capture_served_export_cut("main", &[], &[]).await {
            Ok(_) => panic!("an unbound enrolled cut must refuse before output"),
            Err(error) => error,
        };
        assert!(matches!(
            ambient_cut,
            OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "DISABLED"
        ));

        let checked_export =
            bind_checked_served_export(reopen_enrolled(&dir).await, &cluster_uri).await;
        let mut blocked_output = Vec::new();
        let blocked = checked_export_jsonl_to_bytes(&checked_export, "main", &mut blocked_output)
            .await
            .unwrap_err();
        assert!(matches!(
            blocked,
            OmniError::StreamExportBlocked {
                withdrawn_token_count: 0,
                dead_lettered_token_count: 1,
            }
        ));
        assert!(
            blocked_output.is_empty(),
            "terminal sequencing authority must refuse before any export byte"
        );
        drop(checked_export);

        let list =
            helpers::stream_authority::list_stream_dead_letters(&offline, &cluster_uri, None)
                .await
                .expect("current terminal authority must be listable while stopped");
        assert_eq!(list.entries.len(), 1);
        assert!(list.next_cursor.is_none());
        assert_eq!(list.entries[0].table_key, TABLE);
        assert_eq!(list.entries[0].logical_id, logical_id);
        assert_eq!(list.entries[0].reason_code, "UNIQUE_VIOLATION");

        let payloads = helpers::stream_authority::export_stream_dead_letter_payloads(
            &offline,
            &cluster_uri,
            None,
        )
        .await
        .expect("descriptor-selected payload export must verify the immutable object");
        assert_eq!(payloads.entries.len(), 1);
        assert!(payloads.next_cursor.is_none());
        assert_eq!(payloads.entries[0].authority, list.entries[0]);
        let payload: serde_json::Value =
            serde_json::from_str(payloads.entries[0].payload.get()).unwrap();
        assert_eq!(payload["id"], logical_id);
        assert_eq!(payload["score"], 7);

        let plan = plan_stream_authority_retirement(&offline, &cluster_uri).await;
        assert_eq!(plan.withdrawn_token_count, 0);
        assert_eq!(plan.dead_lettered_token_count, 1);
        let retirement_id = "e2e2e2e2-e2e2-42e2-82e2-e2e2e2e2e2e2";
        let retired = confirm_stream_authority_retirement(
            &offline,
            &cluster_uri,
            retirement_id,
            &plan.plan_digest,
        )
        .await
        .expect("the exact terminal cut must retire for rebuild");
        assert!(retired.changed);
        assert_eq!(retired.dead_lettered_token_count, 1);

        let checked_export =
            bind_checked_served_export(reopen_enrolled(&dir).await, &cluster_uri).await;
        let exported = checked_export_jsonl(&checked_export, "main")
            .await
            .expect("receipt-bound retirement must release checked logical export");
        drop(checked_export);
        let provenance: serde_json::Value =
            serde_json::from_str(exported.lines().next().unwrap()).unwrap();
        assert_eq!(
            provenance["_omnigraph_export_provenance"]["receipt"]["dead_lettered_token_count"],
            1
        );
        assert_no_recovery_sidecars(&dir);
    });
}

struct AuthorityRetirementFixture {
    _dir: EnrolledGraphDir,
    db: Arc<Omnigraph>,
    cluster_uri: String,
    plan: StreamAuthorityRetirementPlan,
}

#[inline(never)]
async fn prepare_authority_retirement_fixture() -> AuthorityRetirementFixture {
    let (dir, served, _, _) = init_productive_sealed_lane().await;
    let enabled_export = served.export_jsonl("main", &[], &[]).await.unwrap_err();
    assert!(matches!(
        enabled_export,
        OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "ENABLED"
    ));
    drop(served);
    let db = reopen_enrolled(&dir).await;
    let cluster_uri = dir.cluster_uri();
    disable_stream_profile(&db, &cluster_uri).await;
    let before_rebind = stream_lane(&db).await;
    let rebound_enrollment = rebind_stream_table_offline(
        &db,
        &cluster_uri,
        TABLE,
        "96969696-9696-4696-8696-969696969696",
        before_rebind.lifecycle_revision,
    )
    .await
    .unwrap();
    assert_ne!(rebound_enrollment, before_rebind.enrollment_id);
    db.branch_create("archive").await.unwrap();
    db.failpoint_withdraw_stream_token_for_retirement_test(
        TABLE,
        "ordinary-before-quiesce",
        "97979797-9797-4797-8797-979797979797",
    )
    .await
    .unwrap();

    let mut ambient_output = Vec::new();
    let ambient = db
        .export_jsonl_to_writer("main", &[], &[], &mut ambient_output)
        .await
        .expect_err("an ambient disabled enrolled export must refuse");
    assert!(matches!(
        ambient,
        OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "DISABLED"
    ));
    assert!(ambient_output.is_empty());

    let checked_export =
        bind_checked_served_export(reopen_enrolled(&dir).await, &cluster_uri).await;
    let mut blocked_output = Vec::new();
    let blocked = checked_export_jsonl_to_bytes(&checked_export, "main", &mut blocked_output)
        .await
        .unwrap_err();
    assert!(matches!(
        blocked,
        OmniError::StreamExportBlocked {
            withdrawn_token_count: 1,
            dead_lettered_token_count: 0,
        }
    ));
    assert!(
        blocked_output.is_empty(),
        "WITHDRAWN authority must refuse before any checked export byte"
    );
    let operational = checked_export
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect("checked terminal owner must observe the withdrawn authority");
    assert_eq!(operational.token_ledger.present_count, 0);
    assert_eq!(operational.token_ledger.withdrawn_count, 1);
    assert_eq!(operational.token_ledger.dead_lettered_count, 0);
    assert_eq!(operational.token_ledger.terminal_sample.len(), 1);
    assert_eq!(
        operational.token_ledger.terminal_sample[0].logical_id,
        "ordinary-before-quiesce"
    );
    assert_eq!(
        operational.token_ledger.terminal_sample[0].disposition,
        "WITHDRAWN"
    );
    assert!(!operational.token_ledger.terminal_sample_has_more);
    match operational.token_ledger.lookup_index_coverage {
        StreamTokenIndexCoverageStatus::Known {
            uncovered_fragments,
            ..
        } => assert!(
            uncovered_fragments > 0,
            "the post-index withdrawal must leave a real uncovered token fragment"
        ),
        ref other => panic!("withdrawal fixture must expose exact uncovered coverage: {other:?}"),
    }
    assert!(matches!(
        operational.token_ledger.oldest_uncovered_age,
        StreamOldestUncoveredAgeStatus::Unavailable { .. }
    ));
    assert!(!operational.rebuild.ready);
    assert!(operational.rebuild.reasons.iter().any(|reason| matches!(
        reason,
        StreamRebuildBlockReason::TerminalTokenAuthority {
            withdrawn_count: 1,
            dead_lettered_count: 0,
        }
    )));
    drop(checked_export);

    let plan = plan_stream_authority_retirement(&db, &cluster_uri).await;
    assert_eq!(
        plan_stream_authority_retirement(&db, &cluster_uri).await,
        plan,
        "planning the same stopped-writer cut must be deterministic"
    );

    AuthorityRetirementFixture {
        _dir: dir,
        db,
        cluster_uri,
        plan,
    }
}

#[inline(never)]
async fn confirm_retirement_and_export_frozen_cut(
    fixture: &AuthorityRetirementFixture,
) -> (String, String) {
    let AuthorityRetirementFixture {
        _dir: dir,
        db,
        cluster_uri,
        plan,
    } = fixture;
    let retirement_id = "89898989-8989-4898-8898-898989898989";
    let confirmed =
        confirm_stream_authority_retirement(&db, &cluster_uri, retirement_id, &plan.plan_digest)
            .await
            .unwrap();
    assert!(confirmed.changed);
    assert_eq!(confirmed.retirement_id, retirement_id);
    assert_eq!(db.stream_status().await.unwrap().profile_mode, "RETIRED");

    let mut ambient_output = Vec::new();
    db
        .export_jsonl_to_writer("main", &[], &[], &mut ambient_output)
        .await
        .expect(
            "the receipt-verified ambient RETIRED compatibility bridge must remain alongside served export",
        );
    let ambient_export = String::from_utf8(ambient_output).unwrap();
    let ambient_provenance: serde_json::Value =
        serde_json::from_str(ambient_export.lines().next().unwrap()).unwrap();
    assert_eq!(
        ambient_provenance["_omnigraph_export_provenance"]["receipt"]["retirement_id"],
        retirement_id
    );

    let checked_export =
        bind_checked_unmanaged_terminal_export(reopen_enrolled(dir).await, cluster_uri).await;
    {
        let _receipt_failure =
            ScopedFailPoint::new(names::STREAM_RETIREMENT_RECEIPT_VALIDATE, "return");
        let error = checked_export
            .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
            .await
            .expect_err("status must not report RETIRED rebuild readiness without provenance");
        assert!(
            error
                .to_string()
                .contains(names::STREAM_RETIREMENT_RECEIPT_VALIDATE),
            "{error:?}"
        );
    }
    let operational = checked_export
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect("RETIRED status must re-prove its selected retirement receipt");
    assert_eq!(operational.durable.profile_mode, "RETIRED");
    assert_eq!(operational.token_ledger.withdrawn_count, 1);
    assert!(
        operational.rebuild.ready,
        "receipt-verified RETIRED authority releases the terminal-token rebuild block: {:?}",
        operational.rebuild.reasons
    );
    let main_export = checked_export_jsonl(&checked_export, "main").await.unwrap();
    assert_eq!(
        checked_export_jsonl(&checked_export, "main").await.unwrap(),
        main_export
    );
    let archive_export = checked_export_jsonl(&checked_export, "archive")
        .await
        .unwrap();
    drop(checked_export);
    assert_eq!(
        archive_export.lines().skip(1).collect::<Vec<_>>(),
        main_export.lines().skip(1).collect::<Vec<_>>(),
        "both frozen branches export the same logical rows"
    );
    let provenance: serde_json::Value =
        serde_json::from_str(main_export.lines().next().unwrap()).unwrap();
    let archive_provenance: serde_json::Value =
        serde_json::from_str(archive_export.lines().next().unwrap()).unwrap();
    assert_eq!(
        provenance["_omnigraph_export_provenance"]["receipt"]["retirement_id"],
        retirement_id
    );
    assert_eq!(
        provenance["_omnigraph_export_provenance"]["branch_member"]["branch"],
        "main"
    );
    assert_eq!(
        archive_provenance["_omnigraph_export_provenance"]["branch_member"]["branch"],
        "archive"
    );
    assert_ne!(
        provenance["_omnigraph_export_provenance"]["branch_member"],
        archive_provenance["_omnigraph_export_provenance"]["branch_member"],
        "each export must identify its exact member of the frozen root cut"
    );
    assert_eq!(
        provenance["_omnigraph_export_provenance"]["receipt"]["export_cut_digest"],
        archive_provenance["_omnigraph_export_provenance"]["receipt"]["export_cut_digest"]
    );
    assert_eq!(
        provenance["_omnigraph_export_provenance"]["source_schema_ir_hash"],
        archive_provenance["_omnigraph_export_provenance"]["source_schema_ir_hash"]
    );
    assert_eq!(
        provenance["_omnigraph_export_provenance"]["ordered_branch_member_digests"],
        archive_provenance["_omnigraph_export_provenance"]["ordered_branch_member_digests"],
        "every branch export must carry the same complete receipt-cut preimage"
    );
    assert_ne!(
        provenance["_omnigraph_export_provenance"]["selected_member_index"],
        archive_provenance["_omnigraph_export_provenance"]["selected_member_index"],
        "main and archive must select their own member of the shared cut proof"
    );
    for proof in [&provenance, &archive_provenance] {
        let proof = &proof["_omnigraph_export_provenance"];
        let selected_index = proof["selected_member_index"].as_u64().unwrap() as usize;
        assert_eq!(
            proof["branch_member"]["branch_member_digest"],
            proof["ordered_branch_member_digests"][selected_index],
            "the selected leaf must be present at its claimed cut-proof index"
        );
    }

    (main_export, archive_export)
}

#[inline(never)]
async fn assert_retired_source_refuses_writers(fixture: &AuthorityRetirementFixture) {
    let db = &fixture.db;
    let retirement_id = "89898989-8989-4898-8898-898989898989";
    db.sync_branch("archive").await.unwrap();
    let branch_create_error = db.branch_create_from("archive", "late").await.unwrap_err();
    let branch_delete_error = db.branch_delete("archive").await.unwrap_err();
    let index_error = db.ensure_indices_on("archive").await.unwrap_err();
    let schema_error = db.apply_schema(STREAM_SCHEMA).await.unwrap_err();
    for error in [
        branch_create_error,
        branch_delete_error,
        index_error,
        schema_error,
    ] {
        assert!(matches!(
            error,
            OmniError::StreamAuthorityRetired {
                retirement_id: ref observed,
                export_cut_digest: ref cut,
            } if observed == retirement_id && cut == &fixture.plan.export_cut_digest
        ));
    }

    let writer_error = db.optimize().await.unwrap_err();
    assert!(matches!(
        writer_error,
        OmniError::StreamAuthorityRetired {
            retirement_id: ref observed,
            export_cut_digest: ref cut,
        } if observed == retirement_id && cut == &fixture.plan.export_cut_digest
    ));
}

#[inline(never)]
async fn assert_retired_schema_staging_remains_untouched(fixture: &AuthorityRetirementFixture) {
    const CONTRACT_FILES: [(&str, &str); 3] = [
        ("_schema.pg", "_schema.pg.staging"),
        ("_schema.ir.json", "_schema.ir.json.staging"),
        ("__schema_state.json", "__schema_state.json.staging"),
    ];

    let root = fixture._dir.path();
    let expected = CONTRACT_FILES
        .iter()
        .map(|(live, staging)| {
            let bytes = std::fs::read(root.join(live)).unwrap();
            std::fs::write(root.join(staging), &bytes).unwrap();
            ((*staging).to_string(), bytes)
        })
        .collect::<Vec<_>>();

    let reopened = Omnigraph::open(root.to_str().unwrap())
        .await
        .expect("mutable reopen may serve a retired graph without reconciling schema staging");
    assert_eq!(
        reopened.stream_status().await.unwrap().profile_mode,
        "RETIRED"
    );
    let query = reopened
        .query(
            ReadTarget::branch("main"),
            "query retired_read() { match { $p: Person } return { $p.score } }",
            "retired_read",
            &Default::default(),
        )
        .await
        .expect("RETIRED is a writer fence, not a query fence");
    assert_eq!(query.num_rows(), 1);
    drop(reopened);

    let refresh_error = fixture.db.refresh().await.unwrap_err();
    assert!(matches!(
        refresh_error,
        OmniError::StreamAuthorityRetired {
            retirement_id: ref observed,
            export_cut_digest: ref cut,
        } if observed == "89898989-8989-4898-8898-898989898989"
            && cut == &fixture.plan.export_cut_digest
    ));
    for (staging, bytes) in expected {
        assert_eq!(
            std::fs::read(root.join(&staging)).unwrap(),
            bytes,
            "RETIRED cold-open/refresh must not promote, rewrite, or delete {staging}"
        );
    }
}

#[inline(never)]
async fn assert_retirement_confirmation_is_idempotent(fixture: &AuthorityRetirementFixture) {
    let AuthorityRetirementFixture {
        db,
        cluster_uri,
        plan,
        ..
    } = fixture;
    let retirement_id = "89898989-8989-4898-8898-898989898989";
    let retry =
        confirm_stream_authority_retirement(&db, &cluster_uri, retirement_id, &plan.plan_digest)
            .await
            .unwrap();
    assert!(!retry.changed);
    assert_eq!(retry.export_cut_digest, plan.export_cut_digest);
    let different = confirm_stream_authority_retirement(
        &db,
        &cluster_uri,
        "87878787-8787-4878-8878-878787878787",
        &plan.plan_digest,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        different,
        OmniError::StreamAuthorityRetired {
            retirement_id: ref observed,
            ..
        } if observed == retirement_id
    ));
}

#[inline(never)]
async fn assert_retired_export_rebuilds_without_authority(main_export: &str) {
    let rebuilt_dir = tempfile::tempdir().unwrap();
    let rebuilt = Omnigraph::init(rebuilt_dir.path().to_str().unwrap(), STREAM_SCHEMA)
        .await
        .unwrap();
    load_jsonl(&rebuilt, &main_export, LoadMode::Overwrite)
        .await
        .unwrap();
    let rebuilt_status = rebuilt.stream_status().await.unwrap();
    assert_eq!(rebuilt_status.profile_mode, "DISABLED");
    assert!(rebuilt_status.tables.is_empty());
    let rebuilt_export = rebuilt.export_jsonl("main", &[], &[]).await.unwrap();
    assert_eq!(
        rebuilt_export,
        main_export
            .lines()
            .skip(1)
            .map(|line| format!("{line}\n"))
            .collect::<String>(),
        "rebuild imports logical rows but no retirement provenance or sequencing authority"
    );
}

#[inline(never)]
async fn assert_retired_export_rejects_tampered_member(main_export: &str) {
    let mut lines = main_export.lines();
    let mut provenance: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
    let version = provenance["_omnigraph_export_provenance"]["branch_member"]["manifest_version"]
        .as_u64()
        .unwrap();
    provenance["_omnigraph_export_provenance"]["branch_member"]["manifest_version"] =
        serde_json::Value::from(version + 1);
    let tampered_export = std::iter::once(provenance.to_string())
        .chain(lines.map(str::to_string))
        .map(|line| format!("{line}\n"))
        .collect::<String>();

    let rebuilt_dir = tempfile::tempdir().unwrap();
    let rebuilt = Omnigraph::init(rebuilt_dir.path().to_str().unwrap(), STREAM_SCHEMA)
        .await
        .unwrap();
    let error = load_jsonl(&rebuilt, &tampered_export, LoadMode::Overwrite)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("retirement export branch-member witness/digest mismatch"),
        "{error}"
    );
}

#[inline(never)]
async fn assert_retired_named_branch_healer_refuses_orphan_discard(
    fixture: &AuthorityRetirementFixture,
) {
    const OPERATION_ID: &str = "01H000000000000000000000RF";

    let snapshot = fixture
        .db
        .snapshot_of(ReadTarget::branch("archive"))
        .await
        .unwrap();
    let entry = snapshot.entry(TABLE).unwrap();
    let lane = stream_lane(&fixture.db).await;
    let identity = serde_json::json!({
        "stable_table_id": lane.stable_table_id,
        "table_incarnation_id": lane.table_incarnation_id,
    });
    let table_path = format!(
        "{}/{}",
        fixture.db.uri().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    let sidecar = serde_json::json!({
        "schema_version": 1,
        "operation_id": OPERATION_ID,
        "started_at": "0",
        "branch": "missing-after-retirement",
        "actor_id": null,
        "writer_kind": "Mutation",
        "tables": [{
            "identity": identity,
            "table_key": TABLE,
            "table_path": table_path,
            "expected_version": 999,
            "post_commit_pin": 1000,
            "table_branch": "missing-after-retirement"
        }]
    });
    let recovery_dir = fixture._dir.path().join("__recovery");
    std::fs::create_dir_all(&recovery_dir).unwrap();
    let sidecar_path = recovery_dir.join(format!("{OPERATION_ID}.json"));
    std::fs::write(&sidecar_path, serde_json::to_vec_pretty(&sidecar).unwrap()).unwrap();
    let audit_before = helpers::recovery::recovery_audit_kinds(fixture._dir.path()).await;

    let error = fixture
        .db
        .load(
            "archive",
            r#"{"type":"Person","data":{"score":99}}"#,
            LoadMode::Merge,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OmniError::StreamAuthorityRetired {
            retirement_id: ref observed,
            export_cut_digest: ref cut,
        } if observed == "89898989-8989-4898-8898-898989898989"
            && cut == &fixture.plan.export_cut_digest
    ));
    assert!(
        sidecar_path.exists(),
        "canonical-main RETIRED must fence orphan-sidecar deletion from an archive-bound handle"
    );
    assert_eq!(
        helpers::recovery::recovery_audit_kinds(fixture._dir.path()).await,
        audit_before,
        "canonical-main RETIRED must fence orphan-discard audit publication"
    );
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

async fn init_productive_sealed_lane() -> (
    EnrolledGraphDir,
    Arc<Omnigraph>,
    StreamTableStatus,
    StreamTableStatus,
) {
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let row = physical_batch(&db, &[("ordinary-before-quiesce".to_string(), 13)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("fixture row must be durably acknowledged");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the ordinary fold must publish before quiesce");
    let folded = stream_lane(&db).await;
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        "90909090-9090-4090-8090-909090909090",
        folded.lifecycle_revision,
        "operator:ordinary-fold-quiesce",
    )
    .await
    .expect("the retained published prefix plus one new sentinel proves the successor empty");
    let sealed = stream_lane(&db).await;
    (dir, db, folded, sealed)
}

async fn init_productive_mixed_sealed_lane() -> (EnrolledGraphDir, Arc<Omnigraph>, StreamTableStatus)
{
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), TWO_TABLE_STREAM_SCHEMA)
            .await
            .unwrap(),
    );

    // Keep both tables productive while only Person owns stream authority.
    // Each load publishes one small fragment per table, giving Optimize real
    // compaction work without manufacturing physical state outside OmniGraph.
    for ordinal in 0..4 {
        load_jsonl(
            &db,
            &format!(
                "{{\"type\":\"Person\",\"data\":{{\"id\":\"p{ordinal}\",\"score\":{ordinal}}}}}\n\
                 {{\"type\":\"Company\",\"data\":{{\"id\":\"c{ordinal}\",\"score\":{ordinal}}}}}"
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    let cluster_uri = format!("file://{}", cluster.path().display());
    enable_stream_profile(&db, &cluster_uri).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let open = stream_lane(&db).await;
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        "93939393-9393-4393-8393-939393939393",
        open.lifecycle_revision,
        "operator:mixed-optimize-quiesce",
    )
    .await
    .expect("an empty enrolled lane must quiesce before checked Optimize");
    let sealed = stream_lane(&db).await;
    assert_eq!(sealed.lifecycle, "SEALED");
    (
        EnrolledGraphDir {
            _cluster: cluster,
            graph,
        },
        db,
        sealed,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn checked_export_cut_is_immutable_single_slot_and_releases_writer_gates() {
    let _scenario = FailScenario::setup();
    let (dir, served, sealed) = init_productive_mixed_sealed_lane().await;
    assert_eq!(sealed.lifecycle, "SEALED");
    drop(served);

    let cluster_uri = dir.cluster_uri();
    let writer_db = reopen_enrolled(&dir).await;
    disable_stream_profile(&writer_db, &cluster_uri).await;
    writer_db
        .branch_create("frozen-export")
        .await
        .expect("a sealed disabled graph may create the named export branch");
    let export_db =
        bind_checked_unmanaged_terminal_export(reopen_enrolled(&dir).await, &cluster_uri).await;

    let unknown = match export_db
        .capture_served_export_cut("main", &["Missing".to_string()], &[])
        .await
    {
        Ok(_) => panic!("an unknown export filter must fail during cut capture"),
        Err(error) => error,
    };
    assert!(
        unknown
            .to_string()
            .contains("unknown export type 'Missing'"),
        "invalid filters must stay pre-output typed capture failures: {unknown:?}"
    );

    let rendezvous =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_EXPORT_POST_CUT_CAPTURE);
    let capture_db = Arc::clone(&export_db);
    let capture = tokio::spawn(async move {
        capture_db
            .capture_served_export_cut("frozen-export", &[], &[])
            .await
    });
    rendezvous.wait_until_reached().await;

    rendezvous.release();
    let cut = tokio::time::timeout(Duration::from_secs(20), capture)
        .await
        .expect("checked export capture did not resume")
        .unwrap()
        .expect("the terminal checked cut must capture");

    let second = match export_db.capture_served_export_cut("main", &[], &[]).await {
        Ok(_) => panic!("a live immutable cut must own the sole root export slot"),
        Err(error) => error,
    };
    assert!(matches!(
        second,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: 1,
            actual: 2,
        } if resource == "stream_export_slots"
    ));
    let delete_while_pinned = writer_db
        .branch_delete("frozen-export")
        .await
        .expect_err("a live cut must exclude named-branch delete/recreate ABA");
    assert!(matches!(
        delete_while_pinned,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: 1,
            actual: 2,
        } if resource == "stream_export_slots"
    ));

    let mut cleanup_db = Omnigraph::open(dir.path().to_str().unwrap())
        .await
        .expect("the destructive-control probe must reopen the graph");
    let cleanup_while_pinned = cleanup_db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect_err("a live cut must retain every exact selected Lance version");
    assert!(matches!(
        cleanup_while_pinned,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: 1,
            actual: 2,
        } if resource == "stream_export_slots"
    ));
    let schema_while_pinned = writer_db
        .apply_schema(TWO_TABLE_STREAM_SCHEMA)
        .await
        .expect_err("a live cut must exclude schema apply before planning or effects");
    assert!(matches!(
        schema_while_pinned,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: 1,
            actual: 2,
        } if resource == "stream_export_slots"
    ));

    tokio::time::timeout(
        Duration::from_secs(20),
        writer_db.mutate(
            "main",
            INSERT_COMPANY,
            "insert_company",
            &helpers::int_params(&[("$score", 99)]),
        ),
    )
    .await
    .expect("a live cut must not retain the capture gates through output")
    .expect("ordinary disabled-profile writer must commit while the cut retains only its slot");

    let frozen = cut.into_jsonl().await.unwrap();
    assert_eq!(
        exported_type_row_count(&frozen, "Company"),
        4,
        "a later table commit must not retarget an already captured export cut"
    );

    let rebuilt_dir = tempfile::tempdir().unwrap();
    let rebuilt = Omnigraph::init(
        rebuilt_dir.path().to_str().unwrap(),
        TWO_TABLE_STREAM_SCHEMA,
    )
    .await
    .expect("a checked cut rebuilds only into a fresh initialized graph");
    load_jsonl(&rebuilt, &frozen, LoadMode::Overwrite)
        .await
        .expect("ordinary load imports the frozen logical cut into the fresh target");
    assert_eq!(helpers::count_rows(&rebuilt, TABLE).await, 4);
    assert_eq!(helpers::count_rows(&rebuilt, COMPANY_TABLE).await, 4);
    let rebuilt_status = rebuilt.stream_status().await.unwrap();
    assert_eq!(rebuilt_status.profile_mode, "DISABLED");
    assert!(
        rebuilt_status.tables.is_empty(),
        "checked export/import must not transfer lifecycle, binding, or token authority"
    );
    assert_eq!(
        rebuilt.export_jsonl("main", &[], &[]).await.unwrap(),
        frozen,
        "the fresh target must contain exactly the captured logical cut"
    );

    writer_db
        .branch_delete("frozen-export")
        .await
        .expect("consuming the cut must release branch-control exclusion");
    drop(cleanup_db);

    let current = checked_export_jsonl(&export_db, "main")
        .await
        .expect("consuming the first cut must release the root slot");
    assert_eq!(
        exported_type_row_count(&current, "Company"),
        5,
        "a fresh cut must observe the writer that committed after the frozen cut"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn checked_pristine_export_cut_retains_versions_until_consumed() {
    let _scenario = FailScenario::setup();
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), TWO_TABLE_STREAM_SCHEMA)
            .await
            .unwrap(),
    );
    for ordinal in 0..4 {
        load_jsonl(
            &db,
            &format!(
                "{{\"type\":\"Company\",\"data\":{{\"id\":\"c{ordinal}\",\"score\":{ordinal}}}}}"
            ),
            LoadMode::Merge,
        )
        .await
        .unwrap();
    }
    let writer_db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let cluster_uri = format!("file://{}", cluster.path().display());
    let export_db = bind_checked_served_export(db, &cluster_uri).await;
    let cut = export_db
        .capture_served_export_cut("main", &[], &[])
        .await
        .expect("managed pristine DISABLED state may capture a checked cut");

    writer_db
        .mutate(
            "main",
            INSERT_COMPANY,
            "insert_company",
            &helpers::int_params(&[("$score", 99)]),
        )
        .await
        .expect("a later ordinary write must advance the main table");
    let mut cleanup_db = Omnigraph::open(graph.to_str().unwrap()).await.unwrap();
    let blocked = cleanup_db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect_err("cleanup must not collect a live cut's exact versions");
    assert!(matches!(
        blocked,
        OmniError::ResourceLimitExceeded {
            ref resource,
            limit: 1,
            actual: 2,
        } if resource == "stream_export_slots"
    ));

    let frozen = cut.into_jsonl().await.unwrap();
    assert_eq!(
        exported_type_row_count(&frozen, "Company"),
        4,
        "the pinned cut must remain readable after main advances"
    );
    cleanup_db
        .cleanup(CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        })
        .await
        .expect("consuming the cut must release exact-version GC exclusion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn checked_export_preserves_post_start_storage_error_and_releases_slot() {
    let _scenario = FailScenario::setup();
    let (dir, served, sealed) = init_productive_mixed_sealed_lane().await;
    assert_eq!(sealed.lifecycle, "SEALED");
    drop(served);

    let cluster_uri = dir.cluster_uri();
    let offline = reopen_enrolled(&dir).await;
    disable_stream_profile(&offline, &cluster_uri).await;
    drop(offline);

    let failure = ProviderWriteFailure::default();
    let probes = QueryIoProbes {
        table_wrapper: Some(Arc::new(failure.clone())),
        ..Default::default()
    };
    with_query_io_probes(probes, async {
        let export_db = bind_checked_served_export(reopen_enrolled(&dir).await, &cluster_uri).await;
        let cut = export_db
            .capture_served_export_cut("main", &[], &[])
            .await
            .expect("checked two-table export cut must capture before failure is armed");
        let mut writer = ArmBaseReadFailureAfterFirstLine::new(failure.clone());
        let error = cut
            .write_to(&mut writer)
            .await
            .expect_err("a pinned table read after output starts must surface provider failure");
        failure.disarm();

        assert!(
            !writer.output.is_empty() && writer.output.contains(&b'\n'),
            "one complete NDJSON row must be visible before the later storage error"
        );
        assert!(matches!(error, OmniError::Lance(_)), "{error:?}");
        assert!(
            error.to_string().contains(PROVIDER_FAILURE_MESSAGE),
            "post-start export error must preserve its provider cause: {error:?}"
        );
        assert!(
            !failure
                .failed_paths(ProviderFailureTarget::BaseTableRead)
                .is_empty(),
            "the failure must come from a real pinned base-table GET"
        );

        let retry = checked_export_jsonl(&export_db, "main")
            .await
            .expect("the failed cut must release the root export slot");
        assert_eq!(exported_type_row_count(&retry, "Company"), 4);
        assert_eq!(exported_type_row_count(&retry, "Person"), 4);
    })
    .await;
}

async fn raw_table_head(db: &Omnigraph, table_key: &str) -> u64 {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let entry = snapshot.entry(table_key).unwrap();
    let table_uri = format!(
        "{}/{}",
        db.uri().trim_end_matches('/'),
        entry.table_path.trim_start_matches('/')
    );
    Dataset::open(&table_uri).await.unwrap().version().version
}

async fn raw_stream_token_head(db: &Omnigraph) -> u64 {
    let token_uri = format!("{}/_stream_tokens.lance", db.uri().trim_end_matches('/'));
    Dataset::open(&token_uri).await.unwrap().version().version
}

fn assert_streaming_runtime_refusal<T>(
    result: Result<T, OmniError>,
    expected_mode: &str,
    operation: &str,
) {
    match result {
        Err(OmniError::StreamingRequiresClusterRuntime { mode }) => {
            assert_eq!(mode, expected_mode, "{operation} refused in the wrong mode");
        }
        Err(error) => panic!("{operation} returned the wrong refusal: {error:?}"),
        Ok(_) => panic!("{operation} unexpectedly bypassed the stream-profile fence"),
    }
}

async fn assert_legacy_writers_require_cluster_runtime(
    dir: &EnrolledGraphDir,
    db: &Arc<Omnigraph>,
    expected_mode: &str,
) {
    let before_status = db.stream_status().await.unwrap();
    assert_eq!(before_status.profile_mode, expected_mode);
    let before_manifest_head = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    let before_table_head = raw_table_head(db, TABLE).await;
    let before_token_head = raw_stream_token_head(db).await;
    assert_no_recovery_sidecars(dir);

    let insert_params = helpers::int_params(&[("$score", 99)]);
    let update_params = helpers::int_params(&[("$old_score", 98), ("$score", 99)]);
    let delete_params = helpers::int_params(&[("$score", 99)]);
    let mutations = [
        ("insert", "legacy_insert", &insert_params),
        ("update", "legacy_update", &update_params),
        ("delete", "legacy_delete", &delete_params),
    ];
    for (kind, query_name, params) in mutations {
        assert_streaming_runtime_refusal(
            db.mutate("main", LEGACY_WRITER_MUTATIONS, query_name, params)
                .await,
            expected_mode,
            &format!("mutate {kind}"),
        );
        assert_streaming_runtime_refusal(
            db.mutate_as(
                "main",
                LEGACY_WRITER_MUTATIONS,
                query_name,
                params,
                Some("operator:legacy-writer-matrix"),
            )
            .await,
            expected_mode,
            &format!("mutate_as {kind}"),
        );
    }

    for (mode, name) in [
        (LoadMode::Append, "Append"),
        (LoadMode::Merge, "Merge"),
        (LoadMode::Overwrite, "Overwrite"),
    ] {
        assert_streaming_runtime_refusal(
            db.load("main", LEGACY_WRITER_JSONL, mode).await,
            expected_mode,
            &format!("load {name}"),
        );
        assert_streaming_runtime_refusal(
            db.load_as(
                "main",
                None,
                LEGACY_WRITER_JSONL,
                mode,
                Some("operator:legacy-writer-matrix"),
            )
            .await,
            expected_mode,
            &format!("load_as {name}"),
        );
    }

    let nonexistent_path = dir
        .path()
        .join("legacy-writer-input-must-not-be-read.jsonl");
    assert!(!nonexistent_path.exists());
    let nonexistent_path = nonexistent_path.to_str().unwrap();
    for (mode, name) in [
        (LoadMode::Append, "Append"),
        (LoadMode::Merge, "Merge"),
        (LoadMode::Overwrite, "Overwrite"),
    ] {
        assert_streaming_runtime_refusal(
            db.load_file("main", nonexistent_path, mode).await,
            expected_mode,
            &format!("load_file {name} nonexistent path"),
        );
        assert_streaming_runtime_refusal(
            db.load_file_as(
                "main",
                None,
                nonexistent_path,
                mode,
                Some("operator:legacy-writer-matrix"),
            )
            .await,
            expected_mode,
            &format!("load_file_as {name} nonexistent path"),
        );
    }

    assert_eq!(
        db.stream_status().await.unwrap(),
        before_status,
        "legacy writer refusals must not move stream authority"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_manifest_head,
        "legacy writer refusals must not publish a manifest version"
    );
    assert_eq!(
        raw_table_head(db, TABLE).await,
        before_table_head,
        "legacy writer refusals must not commit the enrolled table"
    );
    assert_eq!(
        raw_stream_token_head(db).await,
        before_token_head,
        "legacy writer refusals must not commit stream-token authority"
    );
    assert_no_recovery_sidecars(dir);
}

async fn management_receipt_revisions(db: &Omnigraph, operation_kind: &str) -> Vec<(u64, u64)> {
    let token_uri = format!("{}/_stream_tokens.lance", db.uri().trim_end_matches('/'));
    let dataset = Dataset::open(&token_uri).await.unwrap();
    let mut scanner = dataset.scan();
    scanner
        .project(&["record_tag", "record_payload_json"])
        .unwrap();
    let mut batches = scanner.try_into_stream().await.unwrap();
    let mut revisions = Vec::new();
    while let Some(batch) = batches.next().await {
        let batch = batch.unwrap();
        let tags = batch
            .column_by_name("record_tag")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let payloads = batch
            .column_by_name("record_payload_json")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if tags.value(row) != "STREAM_MANAGEMENT_RECEIPT_V1" || payloads.is_null(row) {
                continue;
            }
            let payload: serde_json::Value = serde_json::from_str(payloads.value(row)).unwrap();
            if payload["operation_kind"] == operation_kind {
                revisions.push((
                    payload["from_revision"].as_u64().unwrap(),
                    payload["to_revision"].as_u64().unwrap(),
                ));
            }
        }
    }
    revisions.sort_unstable();
    revisions
}

#[test]
#[serial]
fn checked_offline_disable_adopts_open_after_fold_and_preserves_its_row() {
    on_big_stack(|| async {
        let _scenario = FailScenario::setup();
        let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
        let row = physical_batch(&served, &[("adopted-disable-row".to_string(), 43)]).await;
        served
            .failpoint_stream_b1_for_test(TABLE, Some(row), 0)
            .await
            .expect("the fixture row must be durable before its maintenance drain starts");
        let open = stream_lane(&served).await;
        let drain_id = "b3b3b3b3-b3b3-43b3-83b3-b3b3b3b3b3b3";
        let draining_revision = served
            .failpoint_start_stream_open_after_fold_drain_for_test(
                TABLE,
                drain_id,
                open.lifecycle_revision,
                "operator:open-after-fold-fixture",
            )
            .await
            .expect("the exact OPEN_AFTER_FOLD drain must be durable");
        let draining = stream_lane(&served).await;
        assert_eq!(draining.lifecycle, "DRAINING");
        assert_eq!(draining.drain_id.as_deref(), Some(drain_id));
        assert_eq!(draining.lifecycle_revision, draining_revision);
        assert_eq!(draining_revision, open.lifecycle_revision + 1);
        assert!(visible_rows(&served).await.is_empty());
        let cluster_uri = dir.cluster_uri();

        served.shutdown_stream_fold_driver().await.unwrap();
        drop(served);
        let offline = reopen_enrolled(&dir).await;
        disable_stream_profile(&offline, &cluster_uri).await;

        let terminal = offline.stream_status().await.unwrap();
        assert_eq!(terminal.profile_mode, "DISABLED");
        assert_eq!(terminal.tables[0].lifecycle, "SEALED");
        let adoption_revision = draining_revision + 1;
        assert_eq!(
            management_receipt_revisions(&offline, "DISABLE_DRAIN_ADOPTION").await,
            vec![(draining_revision, adoption_revision)],
            "disable must select exactly one metadata-only adoption revision"
        );
        assert!(terminal.tables[0].lifecycle_revision > adoption_revision);
        assert_eq!(
            visible_rows(&offline).await,
            vec![("adopted-disable-row".to_string(), 43)]
        );
        assert_no_recovery_sidecars(&dir);
    });
}

fn assert_disable_adoption_recovery_boundary(
    failpoint: &'static str,
    logical_id: &'static str,
    drain_id: &'static str,
) {
    on_big_stack(move || async move {
        let _scenario = FailScenario::setup();
        let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
        let row = physical_batch(&served, &[(logical_id.to_string(), 44)]).await;
        served
            .failpoint_stream_b1_for_test(TABLE, Some(row), 0)
            .await
            .unwrap();
        let open = stream_lane(&served).await;
        let draining_revision = served
            .failpoint_start_stream_open_after_fold_drain_for_test(
                TABLE,
                drain_id,
                open.lifecycle_revision,
                "operator:lost-adoption-fixture",
            )
            .await
            .unwrap();
        let cluster_uri = dir.cluster_uri();
        served.shutdown_stream_fold_driver().await.unwrap();
        drop(served);
        let offline = reopen_enrolled(&dir).await;

        let first_error = {
            let _receipt_boundary = ScopedFailPoint::new(failpoint, "return");
            try_disable_stream_profile(&offline, &cluster_uri)
                .await
                .expect_err("the first adoption attempt must stop at its receipt boundary")
        };
        assert!(
            first_error.to_string().contains(failpoint),
            "{first_error:?}"
        );
        let interrupted = offline.stream_status().await.unwrap();
        assert_eq!(interrupted.profile_mode, "DISABLING");
        assert_eq!(interrupted.tables[0].lifecycle, "DRAINING");
        assert_eq!(interrupted.tables[0].lifecycle_revision, draining_revision);
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1,
            "the exact adoption receipt intent must remain armed for recovery"
        );

        try_disable_stream_profile(&offline, &cluster_uri)
            .await
            .expect("the same fixed disable occurrence must recover and converge");
        let terminal = offline.stream_status().await.unwrap();
        assert_eq!(terminal.profile_mode, "DISABLED");
        assert_eq!(terminal.tables[0].lifecycle, "SEALED");
        assert_eq!(
            management_receipt_revisions(&offline, "DISABLE_DRAIN_ADOPTION").await,
            vec![(draining_revision, draining_revision + 1)],
            "recovery/retry must publish the deterministic adoption exactly once"
        );
        assert_eq!(
            visible_rows(&offline).await,
            vec![(logical_id.to_string(), 44)]
        );
        assert_no_recovery_sidecars(&dir);
    });
}

#[test]
#[serial]
fn checked_offline_disable_recovers_armed_adoption_intent_and_retries_same_occurrence() {
    assert_disable_adoption_recovery_boundary(
        names::STREAM_LIFECYCLE_RECEIPT_POST_SIDECAR_PRE_TOKEN_COMMIT,
        "recovered-armed-adoption-row",
        "b4b4b4b4-b4b4-44b4-84b4-b4b4b4b4b4b4",
    );
}

#[test]
#[serial]
fn checked_offline_disable_recovers_exact_adoption_token_effect_and_retries_same_occurrence() {
    assert_disable_adoption_recovery_boundary(
        names::STREAM_LIFECYCLE_RECEIPT_POST_TOKEN_COMMIT_PRE_CONFIRM,
        "recovered-exact-adoption-row",
        "b6b6b6b6-b6b6-46b6-86b6-b6b6b6b6b6b6",
    );
}

#[test]
#[serial]
fn checked_offline_disable_corrects_adopted_open_after_fold_data_block_and_converges() {
    on_big_stack(|| async {
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
        let dir = EnrolledGraphDir {
            _cluster: cluster,
            graph,
        };
        let cluster_uri = dir.cluster_uri();
        enable_stream_profile(&db, &cluster_uri).await;
        let served = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;

        let logical_id = "adopted-disable-blocked";
        let duplicate = physical_batch(&served, &[(logical_id.to_string(), 7)]).await;
        served
            .failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
            .await
            .expect("the uniqueness-conflicting row must be durably acknowledged while served");
        let open = stream_lane(&served).await;
        let drain_id = "b5b5b5b5-b5b5-45b5-85b5-b5b5b5b5b5b5";
        let draining_revision = served
            .failpoint_start_stream_open_after_fold_drain_for_test(
                TABLE,
                drain_id,
                open.lifecycle_revision,
                "operator:blocked-open-after-fold",
            )
            .await
            .expect("the maintenance-style OPEN_AFTER_FOLD drain must persist");
        served.shutdown_stream_fold_driver().await.unwrap();
        drop(served);

        let offline = reopen_enrolled(&dir).await;
        let disable_error = {
            let _overflow = force_dead_letter_object_overflow_once();
            try_disable_stream_profile(&offline, &cluster_uri)
                .await
                .expect_err(
                    "disable adoption must park when the conflict's dead-letter object overflows",
                )
        };
        let blocked = stream_lane(&offline).await;
        let block_token = blocked
            .strict_block_token
            .clone()
            .expect("the adopted drain must expose its exact DataBlock token");
        assert_eq!(stream_data_block_token(&disable_error), block_token);
        assert_eq!(
            offline.stream_status().await.unwrap().profile_mode,
            "DISABLING"
        );
        assert_eq!(blocked.lifecycle, "DRAINING");
        assert_eq!(blocked.drain_id.as_deref(), Some(drain_id));
        assert_eq!(blocked.last_fold_outcome.as_deref(), Some("STRICT_BLOCKED"));
        assert_eq!(
            management_receipt_revisions(&offline, "DISABLE_DRAIN_ADOPTION").await,
            vec![(draining_revision, draining_revision + 1)],
            "the blocked disable must already have selected exactly one adoption receipt"
        );
        assert_no_recovery_sidecars(&dir);

        let page = offline
            .failpoint_show_stream_data_block_for_test(TABLE, &block_token, None)
            .await
            .expect("offline inspection must reconstruct the adopted drain's exact block");
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].logical_key, logical_id);
        assert_eq!(page.entries[0].violation_code, "UNIQUE_VIOLATION");
        assert!(
            page.entries[0]
                .allowed_actions
                .iter()
                .any(|action| action == "REPLACE")
        );
        let correction = StreamDataCorrectionRequest {
            protocol_version: 1,
            block_token,
            correction_id: "c6c6c6c6-c6c6-46c6-86c6-c6c6c6c6c6c6".to_string(),
            expected_lifecycle_revision: blocked.lifecycle_revision,
            actions: vec![StreamDataCorrectionAction::Replace {
                ordinal: page.entries[0].ordinal,
                logical_key: logical_id.to_string(),
                current_blocked_winner_stream_token: page.entries[0]
                    .current_blocked_winner_stream_token
                    .clone(),
                write_id: "d7d7d7d7-d7d7-47d7-87d7-d7d7d7d7d7d7".to_string(),
                row: serde_json::json!({"id": logical_id, "score": 8}),
            }],
            expected_plan_digest: None,
        };
        let corrected = offline
            .failpoint_correct_stream_data_block_for_test(
                TABLE,
                correction,
                "operator:adopted-disable-correction",
            )
            .await
            .expect("the exact replacement must clear the adopted drain's DataBlock");
        assert!(corrected.changed);
        let corrected_lane = stream_lane(&offline).await;
        assert_eq!(corrected_lane.lifecycle, "DRAINING");
        assert_eq!(corrected_lane.strict_block_token, None);
        assert_eq!(corrected_lane.drain_id.as_deref(), Some(drain_id));
        let corrected_rows = visible_rows(&offline).await;
        assert_eq!(corrected_rows.len(), 2);
        assert!(corrected_rows.contains(&(logical_id.to_string(), 8)));
        assert!(
            corrected_rows
                .iter()
                .any(|(id, score)| id != logical_id && *score == 7),
            "replacement must preserve the original committed winner"
        );
        assert_no_recovery_sidecars(&dir);

        try_disable_stream_profile(&offline, &cluster_uri)
            .await
            .expect("the same fixed disable occurrence must resume the corrected drain");
        let terminal = offline.stream_status().await.unwrap();
        assert_eq!(terminal.profile_mode, "DISABLED");
        assert_eq!(terminal.tables[0].lifecycle, "SEALED");
        let rows = visible_rows(&offline).await;
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&(logical_id.to_string(), 8)));
        assert!(
            rows.iter()
                .any(|(id, score)| id != logical_id && *score == 7),
            "replacement must preserve the original committed winner"
        );
        assert_eq!(
            management_receipt_revisions(&offline, "DISABLE_DRAIN_ADOPTION").await,
            vec![(draining_revision, draining_revision + 1)],
            "correction and disable retry must not duplicate the adoption receipt"
        );
        assert_no_recovery_sidecars(&dir);
    });
}

#[test]
#[serial]
fn checked_offline_disable_drains_open_lane_and_publishes_disabled() {
    on_big_stack(|| async {
        let _scenario = FailScenario::setup();
        let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
        let row = physical_batch(&served, &[("offline-disable-row".to_string(), 41)]).await;
        served
            .failpoint_stream_b1_for_test(TABLE, Some(row), 0)
            .await
            .expect("the served writer must acknowledge the durable row before shutdown");
        assert_eq!(stream_lane(&served).await.lifecycle, "OPEN");
        let cluster_uri = dir.cluster_uri();

        served
            .shutdown_stream_fold_driver()
            .await
            .expect("served-runtime shutdown must retire the writer and release its idle owner");
        drop(served);
        let offline = reopen_enrolled(&dir).await;
        disable_stream_profile(&offline, &cluster_uri).await;

        let status = offline.stream_status().await.unwrap();
        assert_eq!(status.profile_mode, "DISABLED");
        assert_eq!(status.tables.len(), 1);
        assert_eq!(status.tables[0].lifecycle, "SEALED");
        assert_eq!(
            visible_rows(&offline).await,
            vec![("offline-disable-row".to_string(), 41)],
            "offline disable must fold the retained row before terminalizing"
        );
        assert_no_recovery_sidecars(&dir);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn checked_runtime_shutdown_releases_offline_authority_after_ingest() {
    let _scenario = FailScenario::setup();
    let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let row = physical_batch(&served, &[("shutdown-handoff-row".to_string(), 42)]).await;
    served
        .failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("the served writer must acknowledge the durable row before shutdown");
    let cluster_uri = dir.cluster_uri();

    served
        .shutdown_stream_fold_driver()
        .await
        .expect("served-runtime shutdown must retire the writer and release its idle owner");
    drop(served);
    let offline = reopen_enrolled(&dir).await;
    assert_offline_stream_profile_authority_available(&offline, &cluster_uri).await;
    assert_eq!(
        offline.stream_status().await.unwrap().profile_mode,
        "ENABLED"
    );
    assert!(
        visible_rows(&offline).await.is_empty(),
        "runtime shutdown must retain acknowledged bytes only in durable WAL"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn checked_offline_rebind_retries_effect_free_intent_and_selects_fresh_sealed_binding() {
    let _scenario = FailScenario::setup();
    let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let open = stream_lane(&served).await;
    served
        .failpoint_stream_quiesce_for_test(
            TABLE,
            "a6a6a6a6-a6a6-46a6-86a6-a6a6a6a6a6a6",
            open.lifecycle_revision,
            "operator:offline-rebind-quiesce",
        )
        .await
        .expect("the empty enrolled lane must seal before offline rebind");
    let sealed = stream_lane(&served).await;
    assert_eq!(sealed.lifecycle, "SEALED");
    let old_stream_incarnation = sealed.stream_incarnation_id.clone();
    let old_enrollment = sealed.enrollment_id.clone();
    let old_shard = sealed.epoch_floor_by_shard[0].0.clone();
    let cluster_uri = dir.cluster_uri();

    // Offline authority is process-local as well as durable: release the
    // served handle before reopening the graph as the stopped writer.
    assert_eq!(
        Arc::strong_count(&served),
        1,
        "the sealed fixture must not retain a served engine handle"
    );
    drop(served);
    let db = reopen_enrolled(&dir).await;
    disable_stream_profile(&db, &cluster_uri).await;
    let disabled = stream_lane(&db).await;
    assert_eq!(disabled.lifecycle, "SEALED");
    assert_eq!(disabled.lifecycle_revision, sealed.lifecycle_revision);
    let before_head = raw_table_head(&db, TABLE).await;
    let before_inventory = mem_wal_inventory(db.uri()).await;
    let rebind_id = "a7a7a7a7-a7a7-47a7-87a7-a7a7a7a7a7a7";

    let error = {
        let _failpoint =
            ScopedFailPoint::new(names::STREAM_REBIND_POST_SIDECAR_PRE_PHYSICAL, "return");
        rebind_stream_table_offline(
            &db,
            &cluster_uri,
            TABLE,
            rebind_id,
            disabled.lifecycle_revision,
        )
        .await
        .expect_err("the armed v18 intent must stop before either physical table effect")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_REBIND_POST_SIDECAR_PRE_PHYSICAL),
        "{error:?}"
    );
    assert_eq!(raw_table_head(&db, TABLE).await, before_head);
    assert_eq!(stream_lane(&db).await, disabled);
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1,
        "the effect-free v18 intent must remain available to the retry barrier"
    );

    let fresh_scope = rebind_stream_table_offline(
        &db,
        &cluster_uri,
        TABLE,
        rebind_id,
        disabled.lifecycle_revision,
    )
    .await
    .expect("retry must discard the effect-free intent and perform the exact rebind");
    let rebound = stream_lane(&db).await;
    let rebound_status = db.stream_status().await.unwrap();
    assert_eq!(rebound_status.profile_mode, "DISABLED");
    assert_eq!(rebound.lifecycle, "SEALED");
    assert_eq!(rebound.lifecycle_revision, disabled.lifecycle_revision + 1);
    assert_eq!(rebound.stream_incarnation_id, old_stream_incarnation);
    assert_ne!(rebound.enrollment_id, old_enrollment);
    assert_eq!(rebound.epoch_floor_by_shard.len(), 1);
    let fresh_shard = &rebound.epoch_floor_by_shard[0].0;
    assert_ne!(fresh_shard, &old_shard);
    assert_ne!(fresh_scope, old_enrollment);
    assert_ne!(fresh_scope, old_shard);
    assert_ne!(fresh_scope, *fresh_shard);
    assert_ne!(fresh_scope, rebind_id);
    assert_eq!(raw_table_head(&db, TABLE).await, before_head + 2);
    let rebound_inventory = mem_wal_inventory(db.uri()).await;
    assert_inventory_retained(
        &before_inventory,
        &rebound_inventory,
        "offline physical rebind",
    );
    assert_no_recovery_sidecars(&dir);

    let retry_scope = rebind_stream_table_offline(
        &db,
        &cluster_uri,
        TABLE,
        rebind_id,
        disabled.lifecycle_revision,
    )
    .await
    .expect("a lost successful response must return the selected rebind occurrence");
    assert_eq!(retry_scope, fresh_scope);
    assert_eq!(stream_lane(&db).await, rebound);
    assert_eq!(raw_table_head(&db, TABLE).await, before_head + 2);
    let retry_inventory = mem_wal_inventory(db.uri()).await;
    assert_eq!(retry_inventory.objects, rebound_inventory.objects);
    assert_eq!(
        retry_inventory.generation_roots,
        rebound_inventory.generation_roots
    );
    assert_eq!(
        retry_inventory.referenced_generation_roots,
        rebound_inventory.referenced_generation_roots
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn rebind_rolls_forward_confirmed_physical_and_ledger_without_a_third_table_head() {
    let _scenario = FailScenario::setup();
    let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let open = stream_lane(&served).await;
    served
        .failpoint_stream_quiesce_for_test(
            TABLE,
            "b6b6b6b6-b6b6-46b6-86b6-b6b6b6b6b6b6",
            open.lifecycle_revision,
            "operator:rebind-recovery-quiesce",
        )
        .await
        .expect("the fresh empty lane must seal before offline rebind");
    let sealed = stream_lane(&served).await;
    assert_eq!(sealed.lifecycle, "SEALED");
    let cluster_uri = dir.cluster_uri();

    assert_eq!(
        Arc::strong_count(&served),
        1,
        "offline rebind requires the served engine owner to stop"
    );
    drop(served);
    let db = reopen_enrolled(&dir).await;
    disable_stream_profile(&db, &cluster_uri).await;
    let disabled = stream_lane(&db).await;
    assert_eq!(disabled.lifecycle, "SEALED");
    let before_head = raw_table_head(&db, TABLE).await;
    let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        before_snapshot.entry(TABLE).unwrap().table_version,
        before_head,
        "the fixture must begin with one coherent manifest/table HEAD"
    );

    let rebind_id = "b7b7b7b7-b7b7-47b7-87b7-b7b7b7b7b7b7";
    let error = {
        let _before_publish =
            ScopedFailPoint::new(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH, "return");
        rebind_stream_table_offline(
            &db,
            &cluster_uri,
            TABLE,
            rebind_id,
            disabled.lifecycle_revision,
        )
        .await
        .expect_err("confirmed rebind effects must stop before manifest publication")
    };
    assert!(
        error
            .to_string()
            .contains(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH),
        "{error:?}"
    );

    assert_eq!(
        raw_table_head(&db, TABLE).await,
        before_head + 2,
        "index replacement must finish at the exact N+2 physical HEAD"
    );
    assert_eq!(
        stream_lane(&db).await,
        disabled,
        "the pre-publish crash must leave the old lifecycle selected"
    );
    let failed_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        failed_snapshot.entry(TABLE).unwrap().table_version,
        before_head,
        "the old manifest must not expose either physical rebind effect"
    );

    let sidecar_id = helpers::recovery::single_sidecar_operation_id(dir.path());
    let sidecar: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            dir.path()
                .join("__recovery")
                .join(format!("{sidecar_id}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    let protocol = &sidecar["protocol_v18"]["payload"];
    assert_eq!(protocol["request"]["rebind_id"], rebind_id);
    assert_eq!(protocol["phase"], "LEDGER_EFFECTS_CONFIRMED");
    assert_eq!(
        protocol["physical"]["dropped_head"]["table_version"].as_u64(),
        Some(before_head + 1)
    );
    assert_eq!(
        protocol["physical"]["fresh_index_head"]["table_version"].as_u64(),
        Some(before_head + 2)
    );
    assert_eq!(
        protocol["physical"]["confirmed_update"]["table_version"].as_u64(),
        Some(before_head + 2)
    );
    assert_eq!(
        protocol["ledger"]["confirmed_transaction"], protocol["ledger"]["planned_transaction"],
        "the durable token-ledger effect must be the pre-minted transaction"
    );
    assert_eq!(
        protocol["ledger"]["confirmed_head"],
        protocol["ledger"]["next_authority"]["current_head_witness"],
        "the confirmed ledger HEAD must be the sole next token authority"
    );
    let confirmed_token_head = protocol["ledger"]["confirmed_head"]["table_version"]
        .as_u64()
        .unwrap();
    assert_eq!(
        protocol["terminal"]["management_receipt"]["operation_id"],
        rebind_id
    );
    assert_eq!(
        protocol["terminal"]["next_lifecycle"]["lifecycle"],
        "SEALED"
    );
    assert_eq!(
        protocol["terminal"]["next_lifecycle"]["current_head_witness"]["table_version"].as_u64(),
        Some(before_head + 2)
    );
    assert_eq!(
        protocol["terminal"]["next_lifecycle"]["current_binding_receipt_id"],
        protocol["planned_binding_receipt"]["record_id"]
    );
    let fresh_scope = protocol["planned_binding_receipt"]["binding_scope_id"]
        .as_str()
        .unwrap()
        .to_string();
    let fresh_enrollment = protocol["intended_binding"]["enrollment_id"]
        .as_str()
        .unwrap()
        .to_string();
    let fresh_shard = protocol["intended_binding"]["shard_ids"][0]
        .as_str()
        .unwrap()
        .to_string();

    drop(failed_snapshot);
    drop(before_snapshot);
    drop(db);
    let db = reopen_enrolled(&dir).await;
    let rebound = stream_lane(&db).await;
    assert_eq!(rebound.lifecycle, "SEALED");
    assert_eq!(rebound.lifecycle_revision, disabled.lifecycle_revision + 1);
    assert_eq!(rebound.enrollment_id, fresh_enrollment);
    assert_eq!(rebound.epoch_floor_by_shard[0].0, fresh_shard);
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .entry(TABLE)
            .unwrap()
            .table_version,
        before_head + 2,
        "cold recovery must select the already-confirmed N+2 table"
    );
    assert_eq!(raw_stream_token_head(&db).await, confirmed_token_head);
    assert_eq!(
        raw_table_head(&db, TABLE).await,
        before_head + 2,
        "cold recovery must not manufacture an N+3 retry effect"
    );
    assert_no_recovery_sidecars(&dir);

    db.branch_create("rebind-replay-topology")
        .await
        .expect("a fresh public branch must be allowed after terminal rebind");
    let before_replay_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let before_replay_manifest = before_replay_snapshot.version();
    let before_replay_table = raw_table_head(&db, TABLE).await;
    let before_replay_token = raw_stream_token_head(&db).await;
    assert_eq!(
        rebind_stream_table_offline(
            &db,
            &cluster_uri,
            TABLE,
            rebind_id,
            disabled.lifecycle_revision,
        )
        .await
        .expect("the terminal ledger receipt must replay the selected occurrence"),
        fresh_scope
    );
    let fresh_error = rebind_stream_table_offline(
        &db,
        &cluster_uri,
        TABLE,
        "b8b8b8b8-b8b8-48b8-88b8-b8b8b8b8b8b8",
        disabled.lifecycle_revision,
    )
    .await
    .expect_err("a fresh rebind occurrence must still obey current topology");
    assert!(
        fresh_error
            .to_string()
            .contains("stream rebind requires a main-only graph"),
        "{fresh_error:?}"
    );
    assert!(
        fresh_error.to_string().contains("rebind-replay-topology"),
        "{fresh_error:?}"
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        before_replay_manifest,
        "selected-receipt replay must precede fresh topology refusal and stay manifest-free"
    );
    assert_eq!(raw_table_head(&db, TABLE).await, before_replay_table);
    assert_eq!(raw_stream_token_head(&db).await, before_replay_token);
    assert_eq!(stream_lane(&db).await, rebound);
    assert_no_recovery_sidecars(&dir);
}

fn stream_data_block_token(error: &OmniError) -> &str {
    match error {
        OmniError::StreamDataBlocked { block_token } => block_token,
        other => panic!("expected typed stream DataBlock error, got {other:?}"),
    }
}

fn force_dead_letter_object_overflow_once() -> ScopedFailPoint {
    ScopedFailPoint::new(names::STREAM_DEAD_LETTER_FORCE_OBJECT_LIMIT, "1*return")
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
    physical_score_batch(db, TABLE, rows).await
}

async fn physical_score_batch(
    db: &Omnigraph,
    table_key: &str,
    rows: &[(String, i32)],
) -> RecordBatch {
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let table = snapshot.open(table_key).await.unwrap();
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

fn graph_node_line(
    type_name: &str,
    logical_id: &str,
    score: i32,
    write_id: &str,
    predecessor_token: Option<&str>,
) -> Vec<u8> {
    let mut line = serde_json::to_vec(&serde_json::json!({
        "type": type_name,
        "data": {"id": logical_id, "score": score},
        "$stream": {
            "write_id": write_id,
            "predecessor_token": predecessor_token,
        },
    }))
    .unwrap();
    line.push(b'\n');
    line
}

fn graph_edge_line(
    type_name: &str,
    logical_id: &str,
    from: &str,
    to: &str,
    write_id: &str,
    predecessor_token: Option<&str>,
) -> Vec<u8> {
    let mut line = serde_json::to_vec(&serde_json::json!({
        "edge": type_name,
        "from": from,
        "to": to,
        "data": {"id": logical_id},
        "$stream": {
            "write_id": write_id,
            "predecessor_token": predecessor_token,
        },
    }))
    .unwrap();
    line.push(b'\n');
    line
}

fn assert_graph_stream_result_redacted(outcome: &serde_json::Value) {
    const ALLOWED_KEYS: &[&str] = &[
        "actual",
        "blocking_ordinal",
        "blocking_status",
        "current_token",
        "id",
        "kind",
        "limit",
        "message",
        "ordinal",
        "scope",
        "status",
        "stream_token",
        "type",
        "unconfirmed_candidate_token",
        "write_id",
    ];
    let object = outcome
        .as_object()
        .expect("graph stream result must be one flat tagged object");
    for key in object.keys() {
        assert!(
            ALLOWED_KEYS.contains(&key.as_str()),
            "graph result leaked unreviewed field '{key}': {outcome}"
        );
    }
    assert!(
        object
            .values()
            .all(|value| !value.is_null() && !value.is_object() && !value.is_array()),
        "graph result must omit null fields and nested private evidence: {outcome}"
    );
    let encoded = outcome.to_string();
    for forbidden in [
        "node:",
        "edge:",
        "stream_incarnation_id",
        "enrollment_id",
        "shard_id",
        "writer_epoch",
        "admission_attempt_id",
        "origin",
        "terminal_correction",
        "terminal_dead_letter",
        "receipt",
        "revision",
        "digest",
        "object_location",
    ] {
        assert!(
            !encoded.contains(forbidden),
            "graph result leaked '{forbidden}': {outcome}"
        );
    }
}

fn parse_ndjson_outcomes(outcomes: Vec<String>) -> Vec<serde_json::Value> {
    outcomes
        .into_iter()
        .map(|outcome| serde_json::from_str(&outcome).expect("outcome must be valid JSON"))
        .collect()
}

fn graph_body_poll_probe(polled: Arc<AtomicBool>) -> GraphStreamChunkSource {
    Box::pin(futures::stream::poll_fn(move |_| {
        polled.store(true, Ordering::SeqCst);
        std::task::Poll::Ready(None::<Result<Vec<u8>, OmniError>>)
    }))
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
    visible_score_rows(db, TABLE).await
}

async fn visible_score_rows(db: &Omnigraph, table_key: &str) -> Vec<(String, i32)> {
    let batches = helpers::read_table(db, table_key).await;
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

async fn wait_for_visible_rows(db: &Omnigraph, expected: &[(String, i32)]) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if visible_rows(db).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("resident stream fold driver did not publish the expected rows");
}

async fn wait_for_visible_score_rows(db: &Omnigraph, table_key: &str, expected: &[(String, i32)]) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if visible_score_rows(db, table_key).await == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resident stream fold driver did not publish the expected table rows");
}

fn stream_fold_driver_status(db: &Omnigraph) -> serde_json::Value {
    serde_json::from_str(&db.failpoint_stream_fold_driver_status_for_test())
        .expect("stream fold driver status must be valid JSON")
}

fn assert_process_local_driver_state(status: &serde_json::Value, expected_state: &str) {
    assert_eq!(status["scope"], "process_local_advisory");
    assert_eq!(status["authoritative"], false);
    assert_eq!(status["state"], expected_state);
    assert!(
        status["pending_triggers"].is_array(),
        "process-local pending triggers remain advisory diagnostics: {status}"
    );
}

async fn wait_for_stream_fold_driver_idle(db: &Omnigraph) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = stream_fold_driver_status(db);
            if status["pending_triggers"]
                .as_array()
                .is_some_and(|entries| entries.is_empty())
            {
                return status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resident stream fold driver did not clear its finite-round work")
}

async fn wait_for_published_open_fold(db: &Omnigraph, minimum_count: u64) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let status = stream_fold_driver_status(db);
            if status["published_open_folds"]
                .as_u64()
                .is_some_and(|count| count >= minimum_count)
                && status["last_completion"]["outcome"] == "published_open_fold"
            {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("resident driver status did not observe the published OPEN fold")
}

async fn visible_edge_rows(db: &Omnigraph) -> Vec<(String, String, String)> {
    let mut rows = Vec::new();
    for batch in helpers::read_table(db, MIN_CARD_EDGE_TABLE).await {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let src = batch
            .column_by_name("src")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dst = batch
            .column_by_name("dst")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row).to_string(),
                src.value(row).to_string(),
                dst.value(row).to_string(),
            ));
        }
    }
    rows.sort();
    rows
}

async fn wait_for_node_edge_rows(
    db: &Omnigraph,
    expected_nodes: &[(String, i32)],
    expected_edges: &[(String, String, String)],
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if visible_rows(db).await == expected_nodes
                && visible_edge_rows(db).await == expected_edges
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resident stream fold driver did not publish the node/edge cut");
}

async fn init_f6b2_node_edge_fixture() -> (EnrolledGraphDir, Arc<Omnigraph>) {
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), F6B2_NODE_EDGE_SCHEMA)
            .await
            .unwrap(),
    );
    load_jsonl(
        &db,
        r#"{"type":"Person","data":{"id":"Alice","score":1}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("seed the endpoint visible before the two-lane stream cut");
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    db.failpoint_enroll_stream_table_for_test(COMPANY_TABLE)
        .await
        .unwrap();
    db.failpoint_enroll_stream_table_for_test(MIN_CARD_EDGE_TABLE)
        .await
        .unwrap();
    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };
    let cluster_uri = dir.cluster_uri();
    enable_stream_profile(&db, &cluster_uri).await;
    (dir, db)
}

async fn init_f6b8_resume_driver_fixture() -> (EnrolledGraphDir, Arc<Omnigraph>, StreamTableStatus)
{
    init_f6b8_resume_driver_fixture_for(TABLE, "f6b80000-0000-4000-8000-000000000001").await
}

async fn init_f6b8_resume_driver_fixture_for(
    table_key: &str,
    quiesce_id: &str,
) -> (EnrolledGraphDir, Arc<Omnigraph>, StreamTableStatus) {
    let (dir, db) = init_f6b2_node_edge_fixture().await;
    let cluster_uri = dir.cluster_uri();
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let open = stream_lane_for(&db, table_key).await;
    db.failpoint_stream_quiesce_for_test(
        table_key,
        quiesce_id,
        open.lifecycle_revision,
        "operator:f6b8-fixture",
    )
    .await
    .expect("the selected empty lane must seal before the resume/driver race");
    let sealed = stream_lane_for(&db, table_key).await;
    assert_eq!(sealed.lifecycle, "SEALED");
    assert_no_recovery_sidecars(&dir);
    (dir, db, sealed)
}

fn spawn_f6b2_process(mode: &str, graph: &Path, cluster_uri: &str, ready_path: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("f6b2_stream_process")
        .arg("--ignored")
        .arg("--nocapture")
        .env(F6B2_PROCESS_MODE_ENV, mode)
        .env(F6B2_PROCESS_GRAPH_ENV, graph)
        .env(F6B2_PROCESS_CLUSTER_ENV, cluster_uri)
        .env(F6B2_PROCESS_READY_ENV, ready_path)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|error| panic!("failed to start F6b2 {mode} process: {error}"))
}

fn wait_for_process_marker(path: &Path, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if std::fs::read_to_string(path).ok().as_deref() == Some("acknowledged") {
            return;
        }
        if let Some(status) = child.try_wait().expect("query F6b2 child status") {
            panic!("F6b2 acknowledgement process exited before its marker: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "F6b2 acknowledgement process did not reach its durable marker"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn publish_process_marker(path: &Path) {
    std::fs::write(path, b"acknowledged").expect("publish F6b2 durable-ack marker");
    std::fs::File::open(path)
        .unwrap()
        .sync_all()
        .expect("flush F6b2 durable-ack marker");
}

fn run_and_kill_ack_process(mode: &str, graph: &Path, cluster_uri: &str, ready_path: &Path) {
    if ready_path.exists() {
        std::fs::remove_file(ready_path).unwrap();
    }
    let mut child = spawn_f6b2_process(mode, graph, cluster_uri, ready_path);
    wait_for_process_marker(ready_path, &mut child);
    child
        .kill()
        .unwrap_or_else(|error| panic!("force-terminate the acknowledged {mode} process: {error}"));
    let killed = child
        .wait()
        .unwrap_or_else(|error| panic!("reap force-terminated F6b2 {mode} process: {error}"));
    assert!(
        !killed.success(),
        "the {mode} acknowledgement owner must die"
    );
}

fn wait_for_process_exit(child: &mut Child, mode: &str) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().expect("query F6b2 child status") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("F6b2 {mode} process exceeded its bounded completion window");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

async fn wait_for_stream_lifecycle(db: &Omnigraph, expected: &str) -> StreamTableStatus {
    wait_for_stream_lifecycle_for(db, TABLE, expected).await
}

async fn wait_for_stream_lifecycle_for(
    db: &Omnigraph,
    table_key: &str,
    expected: &str,
) -> StreamTableStatus {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let lane = stream_lane_for(db, table_key).await;
            if lane.lifecycle == expected {
                return lane;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("resident stream fold driver did not reach the expected lifecycle")
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
        inventory_after.referenced_generation_roots, inventory_before.referenced_generation_roots,
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

struct F6aCandidateRuntimeFixture {
    dir: EnrolledGraphDir,
    stream_incarnation_id: String,
}

#[inline(never)]
async fn f6a_publish_mixed_candidate_cut() -> F6aCandidateRuntimeFixture {
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), UNIQUE_STREAM_SCHEMA)
            .await
            .unwrap(),
    );
    load_jsonl(
        &db,
        r#"{"type":"Person","data":{"id":"existing","score":7}}"#,
        LoadMode::Merge,
    )
    .await
    .expect("seed the exact committed uniqueness winner before enabling streaming");
    assert_eq!(visible_rows(&db).await, vec![("existing".to_string(), 7)]);

    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };
    let cluster_uri = dir.cluster_uri();
    enable_stream_profile(&db, &cluster_uri).await;
    let served = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    served
        .start_stream_fold_driver()
        .await
        .expect("the checked serving runtime starts its resident fold driver");
    let started = stream_fold_driver_status(&served);
    assert_process_local_driver_state(&started, "running");
    assert_eq!(started["pending_triggers"].as_array().unwrap().len(), 0);
    assert_eq!(started["published_open_folds"], 0);
    assert_eq!(started["last_completion"], serde_json::Value::Null);
    assert_eq!(started["last_error"], serde_json::Value::Null);

    let enrollment_request_id = "f6a00000-0000-4000-8000-000000000001";
    let (challenge, witness, incarnation) =
        prepare_stream_ingest(&served, enrollment_request_id, None, "agent:f6a-candidate")
            .await
            .expect("bodyless prepare returns compare evidence for the absent lane");
    assert_eq!(challenge, "witness_required");
    assert!(incarnation.is_none());
    let witness = witness.expect("the bodyless prepare challenge carries an exact witness");
    let (prepared, next_witness, incarnation) = prepare_stream_ingest(
        &served,
        enrollment_request_id,
        Some(&witness),
        "agent:f6a-candidate",
    )
    .await
    .expect("echoing the exact witness lazily enrolls one OPEN lane");
    assert_eq!(prepared, "enrolled");
    assert!(next_witness.is_none());
    let incarnation = incarnation.expect("lazy enrollment returns its durable incarnation");

    let body = [
        ndjson_score_line(
            &incarnation,
            "loser",
            7,
            "f6a10000-0000-4000-8000-000000000001",
            None,
        ),
        ndjson_score_line(
            &incarnation,
            "winner",
            9,
            "f6a10000-0000-4000-8000-000000000002",
            None,
        ),
    ]
    .concat();
    let outcomes = parse_ndjson_outcomes(
        served
            .failpoint_stream_ingest_ndjson_as_for_test(TABLE, vec![body], "agent:f6a-candidate")
            .await
            .expect("the mixed candidate batch crosses the durable NDJSON boundary"),
    );
    assert_eq!(outcomes.len(), 2);
    for (ordinal, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome["ordinal"], ordinal as u64);
        assert_eq!(outcome["status"], "durable");
        assert!(outcome["stream_token"].as_str().is_some());
    }
    assert_ne!(outcomes[0]["stream_token"], outcomes[1]["stream_token"]);
    let acknowledged = stream_fold_driver_status(&served);
    assert_process_local_driver_state(&acknowledged, "running");

    wait_for_visible_rows(
        &served,
        &[("existing".to_string(), 7), ("winner".to_string(), 9)],
    )
    .await;
    let completed = wait_for_published_open_fold(&served, 1).await;
    assert_process_local_driver_state(&completed, "running");
    assert_eq!(completed["published_open_folds"], 1);
    assert!(completed["pending_triggers"].as_array().unwrap().is_empty());
    assert_eq!(
        completed["last_completion"]["outcome"],
        "published_open_fold"
    );
    assert!(
        completed["last_completion"]["event_sequence"]
            .as_u64()
            .is_some_and(|sequence| sequence > 0)
    );
    assert_eq!(completed["last_error"], serde_json::Value::Null);

    served
        .shutdown_stream_fold_driver()
        .await
        .expect("the first resident driver joins after publishing its finite cut");
    let stopped = stream_fold_driver_status(&served);
    assert_process_local_driver_state(&stopped, "stopped");
    assert!(stopped["pending_triggers"].as_array().unwrap().is_empty());
    assert_eq!(stopped["last_error"], serde_json::Value::Null);
    drop(served);
    assert_no_recovery_sidecars(&dir);

    F6aCandidateRuntimeFixture {
        dir,
        stream_incarnation_id: incarnation,
    }
}

#[inline(never)]
async fn f6a_inspect_terminal_candidate(fixture: &F6aCandidateRuntimeFixture) -> String {
    let offline = reopen_enrolled(&fixture.dir).await;
    let cluster_uri = fixture.dir.cluster_uri();
    let list = helpers::stream_authority::list_stream_dead_letters(&offline, &cluster_uri, None)
        .await
        .expect("offline inspection lists the selected terminal occurrence");
    assert_eq!(list.entries.len(), 1);
    assert!(list.next_cursor.is_none());
    let terminal = &list.entries[0];
    assert_eq!(terminal.table_key, TABLE);
    assert_eq!(terminal.logical_id, "loser");
    assert_eq!(terminal.reason_code, "UNIQUE_VIOLATION");
    let occurrence_token = terminal.occurrence_token.clone();

    let exported =
        helpers::stream_authority::export_stream_dead_letter_payloads(&offline, &cluster_uri, None)
            .await
            .expect("offline inspection verifies and exports the selected terminal payload");
    assert_eq!(exported.entries.len(), 1);
    assert!(exported.next_cursor.is_none());
    assert_eq!(exported.entries[0].authority, list.entries[0]);
    let payload: serde_json::Value =
        serde_json::from_str(exported.entries[0].payload.get()).unwrap();
    assert_eq!(payload["id"], "loser");
    assert_eq!(payload["score"], 7);
    assert_no_recovery_sidecars(&fixture.dir);
    occurrence_token
}

#[inline(never)]
async fn f6a_publish_correction_and_disable(
    fixture: &F6aCandidateRuntimeFixture,
    terminal_token: &str,
) {
    let cluster_uri = fixture.dir.cluster_uri();
    let reopened = reopen_enrolled(&fixture.dir).await;
    let served =
        helpers::stream_authority::bind_checked_stream_runtime(reopened, &cluster_uri).await;
    served
        .start_stream_fold_driver()
        .await
        .expect("the rebound checked runtime starts the resident driver");
    let restarted = stream_fold_driver_status(&served);
    assert_process_local_driver_state(&restarted, "running");

    let outcomes = parse_ndjson_outcomes(
        served
            .failpoint_stream_ingest_ndjson_as_for_test(
                TABLE,
                vec![ndjson_score_line(
                    &fixture.stream_incarnation_id,
                    "loser",
                    8,
                    "f6a20000-0000-4000-8000-000000000001",
                    Some(terminal_token),
                )],
                "agent:f6a-candidate",
            )
            .await
            .expect("the terminal token authorizes one corrected ordinary successor"),
    );
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0]["ordinal"], 0);
    assert_eq!(outcomes[0]["status"], "durable");
    assert!(outcomes[0]["stream_token"].as_str().is_some());
    assert_ne!(outcomes[0]["stream_token"], terminal_token);

    wait_for_visible_rows(
        &served,
        &[
            ("existing".to_string(), 7),
            ("loser".to_string(), 8),
            ("winner".to_string(), 9),
        ],
    )
    .await;
    let completed = wait_for_published_open_fold(&served, 1).await;
    assert_process_local_driver_state(&completed, "running");
    assert!(completed["pending_triggers"].as_array().unwrap().is_empty());
    assert_eq!(
        completed["last_completion"]["outcome"],
        "published_open_fold"
    );
    assert_eq!(completed["last_error"], serde_json::Value::Null);

    served
        .shutdown_stream_fold_driver()
        .await
        .expect("the corrected-cut driver joins before offline handoff");
    let stopped = stream_fold_driver_status(&served);
    assert_process_local_driver_state(&stopped, "stopped");
    assert!(stopped["pending_triggers"].as_array().unwrap().is_empty());
    drop(served);

    let offline = reopen_enrolled(&fixture.dir).await;
    let list = helpers::stream_authority::list_stream_dead_letters(&offline, &cluster_uri, None)
        .await
        .expect("a PRESENT correction removes the terminal token from the current list");
    assert!(list.entries.is_empty());
    assert!(list.next_cursor.is_none());
    assert_offline_stream_profile_authority_available(&offline, &cluster_uri).await;
    disable_stream_profile(&offline, &cluster_uri).await;
    let terminal = offline.stream_status().await.unwrap();
    assert_eq!(terminal.profile_mode, "DISABLED");
    assert_eq!(terminal.tables.len(), 1);
    assert_eq!(terminal.tables[0].lifecycle, "SEALED");
    assert_eq!(
        visible_rows(&offline).await,
        vec![
            ("existing".to_string(), 7),
            ("loser".to_string(), 8),
            ("winner".to_string(), 9),
        ]
    );
    assert_no_recovery_sidecars(&fixture.dir);
}

#[test]
#[serial]
fn candidate_runtime_composes_lazy_prepare_mixed_fold_terminal_correction_and_disable() {
    on_big_stack(|| async {
        let _scenario = FailScenario::setup();
        let fixture = f6a_publish_mixed_candidate_cut().await;
        let terminal_token = f6a_inspect_terminal_candidate(&fixture).await;
        f6a_publish_correction_and_disable(&fixture, &terminal_token).await;
    });
}

/// Subprocess-only half of the F6b2 forced-termination acceptance cell.
///
/// Keep this helper non-`serial`: the parent deliberately holds serial_test's
/// cross-process lock while waiting for this exact child invocation.
#[test]
#[ignore = "subprocess helper; exercised by F6b2 operational acceptance"]
fn f6b2_stream_process() {
    let Some(mode) = std::env::var_os(F6B2_PROCESS_MODE_ENV) else {
        return;
    };
    let mode = mode.to_string_lossy().into_owned();
    let graph = std::env::var(F6B2_PROCESS_GRAPH_ENV).expect("F6b2 graph path");
    let cluster_uri = std::env::var(F6B2_PROCESS_CLUSTER_ENV).expect("F6b2 cluster URI");
    let ready_path = PathBuf::from(
        std::env::var_os(F6B2_PROCESS_READY_ENV).expect("F6b2 acknowledgement marker path"),
    );

    on_big_stack(move || async move {
        let _scenario = FailScenario::setup();
        let db = Arc::new(
            Omnigraph::open(&graph)
                .await
                .expect("F6b2 child opens graph"),
        );
        let served = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
        match mode.as_str() {
            "ack-node" => {
                let node = physical_batch(&served, &[("Bob".to_string(), 2)]).await;
                served
                    .failpoint_stream_b1_for_test(TABLE, Some(node), 0)
                    .await
                    .expect("child durably acknowledges the node generation");
                assert_eq!(visible_rows(&served).await, vec![("Alice".to_string(), 1)]);
                assert!(visible_edge_rows(&served).await.is_empty());
                publish_process_marker(&ready_path);
                std::future::pending::<()>().await;
            }
            "ack-edge" => {
                let edge = physical_edge_batch(
                    &served,
                    &[(
                        "crash-edge".to_string(),
                        "Alice".to_string(),
                        "Bob".to_string(),
                    )],
                )
                .await;
                served
                    .failpoint_stream_b1_for_test(MIN_CARD_EDGE_TABLE, Some(edge), 1)
                    .await
                    .expect("child durably acknowledges the edge generation");
                assert_eq!(visible_rows(&served).await, vec![("Alice".to_string(), 1)]);
                assert!(visible_edge_rows(&served).await.is_empty());
                publish_process_marker(&ready_path);
                std::future::pending::<()>().await;
            }
            "recover" => {
                served
                    .start_stream_fold_driver()
                    .await
                    .expect("replacement process cold-starts the resident driver");
                wait_for_node_edge_rows(
                    &served,
                    &[("Alice".to_string(), 1), ("Bob".to_string(), 2)],
                    &[(
                        "crash-edge".to_string(),
                        "Alice".to_string(),
                        "Bob".to_string(),
                    )],
                )
                .await;
                let completed = wait_for_published_open_fold(&served, 2).await;
                assert_eq!(completed["last_error"], serde_json::Value::Null);
                served
                    .shutdown_stream_fold_driver()
                    .await
                    .expect("replacement process joins every recovered owner");
            }
            other => panic!("unknown F6b2 process mode '{other}'"),
        }
    });
}

#[test]
#[serial]
fn process_kill_cold_recovers_node_and_edge_then_rebinds_and_resumes() {
    on_big_stack(|| async {
        let _scenario = FailScenario::setup();
        let (dir, db) = init_f6b2_node_edge_fixture().await;
        let cluster_uri = dir.cluster_uri();
        let ready_path = dir.path().parent().unwrap().join("f6b2-durable-ack.ready");
        drop(db);

        run_and_kill_ack_process("ack-node", dir.path(), &cluster_uri, &ready_path);
        run_and_kill_ack_process("ack-edge", dir.path(), &cluster_uri, &ready_path);

        let unpublished = reopen_enrolled(&dir).await;
        assert_eq!(
            visible_rows(&unpublished).await,
            vec![("Alice".to_string(), 1)],
            "durable MemWAL acknowledgement is not graph visibility"
        );
        assert!(visible_edge_rows(&unpublished).await.is_empty());
        drop(unpublished);

        let mut recovery = spawn_f6b2_process("recover", dir.path(), &cluster_uri, &ready_path);
        let recovered = wait_for_process_exit(&mut recovery, "recovery");
        assert!(
            recovered.success(),
            "F6b2 recovery process failed: {recovered}"
        );

        let offline = reopen_enrolled(&dir).await;
        assert_eq!(
            visible_rows(&offline).await,
            vec![("Alice".to_string(), 1), ("Bob".to_string(), 2)]
        );
        assert_eq!(
            visible_edge_rows(&offline).await,
            vec![(
                "crash-edge".to_string(),
                "Alice".to_string(),
                "Bob".to_string(),
            )]
        );
        assert_no_recovery_sidecars(&dir);

        disable_stream_profile(&offline, &cluster_uri).await;
        let disabled = offline.stream_status().await.unwrap();
        assert_eq!(disabled.profile_mode, "DISABLED");
        assert_eq!(disabled.tables.len(), 3);
        assert!(
            disabled
                .tables
                .iter()
                .all(|lane| lane.lifecycle == "SEALED")
        );

        let old_person_binding = stream_lane_for(&offline, TABLE).await;
        let rebound_enrollment = rebind_stream_table_offline(
            &offline,
            &cluster_uri,
            TABLE,
            "f6b20000-0000-4000-8000-000000000001",
            old_person_binding.lifecycle_revision,
        )
        .await
        .expect("offline rebind selects one fresh sealed Person binding");
        assert_ne!(rebound_enrollment, old_person_binding.enrollment_id);
        assert_eq!(stream_lane_for(&offline, TABLE).await.lifecycle, "SEALED");

        enable_stream_profile(&offline, &cluster_uri).await;
        drop(offline);
        let reopened = reopen_enrolled(&dir).await;
        assert_eq!(stream_lane_for(&reopened, TABLE).await.lifecycle, "SEALED");
        let served =
            helpers::stream_authority::bind_checked_stream_runtime(reopened, &cluster_uri).await;
        let sealed_person = stream_lane_for(&served, TABLE).await;
        served
            .failpoint_stream_resume_for_test(
                TABLE,
                "f6b20000-0000-4000-8000-000000000002",
                sealed_person.lifecycle_revision,
                false,
                "operator:f6b2-resume",
            )
            .await
            .expect("the rebound Person lane resumes under checked runtime authority");
        let node = physical_batch(&served, &[("Carol".to_string(), 3)]).await;
        served
            .failpoint_stream_b1_for_test(TABLE, Some(node), 2)
            .await
            .expect("the rebound node writer acknowledges a new generation");
        served
            .failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect("the rebound node generation folds before the edge resumes");

        let sealed_edge = stream_lane_for(&served, MIN_CARD_EDGE_TABLE).await;
        served
            .failpoint_stream_resume_for_test(
                MIN_CARD_EDGE_TABLE,
                "f6b20000-0000-4000-8000-000000000003",
                sealed_edge.lifecycle_revision,
                false,
                "operator:f6b2-resume",
            )
            .await
            .expect("the recovered edge lane resumes after the node worker retires");
        let edge = physical_edge_batch(
            &served,
            &[(
                "resumed-edge".to_string(),
                "Alice".to_string(),
                "Carol".to_string(),
            )],
        )
        .await;
        served
            .failpoint_stream_b1_for_test(MIN_CARD_EDGE_TABLE, Some(edge), 3)
            .await
            .expect("the resumed edge writer acknowledges a new generation");
        served
            .failpoint_stream_b1_for_test(MIN_CARD_EDGE_TABLE, None, 0)
            .await
            .expect("the resumed edge generation folds after the node generation");
        assert_eq!(
            visible_rows(&served).await,
            vec![
                ("Alice".to_string(), 1),
                ("Bob".to_string(), 2),
                ("Carol".to_string(), 3),
            ]
        );
        assert_eq!(
            visible_edge_rows(&served).await,
            vec![
                (
                    "crash-edge".to_string(),
                    "Alice".to_string(),
                    "Bob".to_string(),
                ),
                (
                    "resumed-edge".to_string(),
                    "Alice".to_string(),
                    "Carol".to_string(),
                ),
            ]
        );
        served.shutdown_stream_fold_driver().await.unwrap();
        assert_no_recovery_sidecars(&dir);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn finite_round_runs_captured_edge_before_later_node_trigger() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_f6b2_node_edge_fixture().await;
    let cluster_uri = dir.cluster_uri();
    let node_owner = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let node = physical_batch(&node_owner, &[("Bob".to_string(), 2)]).await;
    node_owner
        .failpoint_stream_b1_for_test(TABLE, Some(node), 0)
        .await
        .expect("the node endpoint is durable before its owner retires");
    node_owner.shutdown_stream_fold_driver().await.unwrap();
    drop(node_owner);

    let edge_owner = reopen_enrolled(&dir).await;
    let edge_owner =
        helpers::stream_authority::bind_checked_stream_runtime(edge_owner, &cluster_uri).await;
    let edge = physical_edge_batch(
        &edge_owner,
        &[(
            "round-edge".to_string(),
            "Alice".to_string(),
            "Bob".to_string(),
        )],
    )
    .await;
    edge_owner
        .failpoint_stream_b1_for_test(MIN_CARD_EDGE_TABLE, Some(edge), 1)
        .await
        .expect("the dependent edge is durable before its owner retires");
    edge_owner.shutdown_stream_fold_driver().await.unwrap();
    drop(edge_owner);

    let served = reopen_enrolled(&dir).await;
    let served = helpers::stream_authority::bind_checked_stream_runtime(served, &cluster_uri).await;
    let late_node =
        physical_score_batch(&served, COMPANY_TABLE, &[("late-company".to_string(), 4)]).await;

    let rounds = TwoRoundRendezvous::new();
    served.start_stream_fold_driver().await.unwrap();
    rounds.wait_for(0).await;

    let late_call_started = Arc::new(AtomicBool::new(false));
    let late_db = Arc::clone(&served);
    let late_started = Arc::clone(&late_call_started);
    let mut late_put = tokio::spawn(async move {
        late_started.store(true, Ordering::SeqCst);
        late_db
            .failpoint_stream_b1_for_test(COMPANY_TABLE, Some(late_node), 2)
            .await
    });
    tokio::time::timeout(Duration::from_secs(20), async {
        while !late_call_started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late node task never reached its put call");
    tokio::task::yield_now().await;
    assert!(
        !late_put.is_finished(),
        "the frozen round must retain exclusive admission ahead of the late node put"
    );

    rounds.release(0);
    rounds.wait_for(1).await;
    tokio::time::timeout(Duration::from_secs(20), &mut late_put)
        .await
        .expect("late node put stayed fenced after the captured round completed")
        .unwrap()
        .expect("late node work must become durable before the next round runs");
    assert_eq!(
        visible_rows(&served).await,
        vec![("Alice".to_string(), 1), ("Bob".to_string(), 2)],
        "the first captured node must publish before its dependent edge"
    );
    assert_eq!(
        visible_edge_rows(&served).await,
        vec![(
            "round-edge".to_string(),
            "Alice".to_string(),
            "Bob".to_string(),
        )],
        "the captured edge must use a fresh post-node snapshot"
    );
    assert!(
        visible_score_rows(&served, COMPANY_TABLE).await.is_empty(),
        "node work admitted after the round cut must wait for the next round"
    );
    let first_round = stream_fold_driver_status(&served);
    assert_eq!(first_round["published_open_folds"], 2);
    assert_eq!(
        first_round["last_completion"]["outcome"],
        "published_open_fold"
    );
    assert_eq!(first_round["last_error"], serde_json::Value::Null);

    rounds.release(1);
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if visible_score_rows(&served, COMPANY_TABLE).await
                == vec![("late-company".to_string(), 4)]
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the next finite round did not publish the late node");
    let completed = wait_for_published_open_fold(&served, 3).await;
    assert_eq!(completed["last_error"], serde_json::Value::Null);
    served.shutdown_stream_fold_driver().await.unwrap();
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn resident_driver_empty_startup_is_effect_free_and_timer_folds_once() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let manifest_before = before.version();
    let table_before = before.entry(TABLE).unwrap().table_version;

    db.start_stream_fold_driver()
        .await
        .expect("checked served runtime starts the hidden fold driver");
    let ambient = reopen_enrolled(&dir).await;
    let error = ambient
        .shutdown_stream_fold_driver()
        .await
        .expect_err("an ambient same-root handle must not stop the server-owned driver");
    assert!(
        matches!(error, OmniError::StreamingAuthorityMismatch { .. }),
        "{error:?}"
    );
    let status = stream_fold_driver_status(&db);
    assert_process_local_driver_state(&status, "running");
    drop(ambient);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let after_empty_start = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(after_empty_start.version(), manifest_before);
    assert_eq!(
        after_empty_start.entry(TABLE).unwrap().table_version,
        table_before
    );

    let batch = physical_batch(&db, &[("automatic".to_string(), 10)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .expect("durable put arms the resident timer");
    wait_for_visible_rows(&db, &[("automatic".to_string(), 10)]).await;
    let published = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let published_manifest = published.version();
    let published_table = published.entry(TABLE).unwrap().table_version;

    tokio::time::sleep(Duration::from_millis(1_250)).await;
    let after_retained_wal = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(after_retained_wal.version(), published_manifest);
    assert_eq!(
        after_retained_wal.entry(TABLE).unwrap().table_version,
        published_table
    );
    db.shutdown_stream_fold_driver()
        .await
        .expect("resident driver joins after its finite round");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn resident_driver_same_handle_restart_installs_fresh_startup_discovery() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    db.start_stream_fold_driver()
        .await
        .expect("the first driver run establishes its discovery generation");
    db.shutdown_stream_fold_driver()
        .await
        .expect("the first driver run stops cleanly");

    let batch = physical_batch(&db, &[("between-runs".to_string(), 12)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .expect("a stopped driver can still receive one durable occurrence");
    let before = stream_fold_driver_status(&db);
    let before_trigger = before["pending_triggers"]
        .as_array()
        .unwrap()
        .first()
        .expect("the durable occurrence owns one ordinary timer trigger");
    let before_sequence = before_trigger["trigger_sequence"].as_u64().unwrap();

    let round =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_DRIVER_BEFORE_ROUND_ACQUIRE);
    db.start_stream_fold_driver()
        .await
        .expect("the same handle starts a second driver generation");
    round.wait_until_reached().await;
    let restarted = stream_fold_driver_status(&db);
    let restarted_trigger = restarted["pending_triggers"]
        .as_array()
        .unwrap()
        .first()
        .expect("startup discovery retains the cold lane until its parked round");
    assert_eq!(
        restarted_trigger["trigger_sequence"].as_u64().unwrap(),
        before_sequence + 1,
        "the second start must install an independent urgent discovery trigger"
    );
    round.release();

    wait_for_visible_rows(&db, &[("between-runs".to_string(), 12)]).await;
    db.shutdown_stream_fold_driver()
        .await
        .expect("the restarted driver joins after publishing the cold lane");
}

#[tokio::test]
#[serial]
async fn resident_driver_requires_the_exact_checked_served_runtime() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled().await;
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();

    let error = db
        .start_stream_fold_driver()
        .await
        .expect_err("ambient enabled handle must not mint fold authority");
    assert!(
        matches!(error, OmniError::StreamingRequiresClusterRuntime { .. }),
        "{error:?}"
    );
    let after = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(after.version(), before.version());
    assert_eq!(
        after.entry(TABLE).unwrap().table_version,
        before.entry(TABLE).unwrap().table_version
    );
    let status = stream_fold_driver_status(&db);
    assert_process_local_driver_state(&status, "stopped");
    assert!(status["pending_triggers"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn resident_driver_restart_discovers_unpublished_wal_but_not_published_retained_wal() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let batch = physical_batch(&db, &[("cold-discovery".to_string(), 11)]).await;
    let error = {
        let _after_watcher = ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
        db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
            .expect_err("fixture leaves a durable ambiguous generation unpublished")
    };
    assert!(matches!(error, OmniError::AckUnknown { .. }), "{error:?}");
    assert!(visible_rows(&db).await.is_empty());
    drop(db);

    let cluster_uri = dir.cluster_uri();
    let reopened = reopen_enrolled(&dir).await;
    let reopened =
        helpers::stream_authority::bind_checked_stream_runtime(reopened, &cluster_uri).await;
    reopened
        .start_stream_fold_driver()
        .await
        .expect("startup discovery arms the cold OPEN lane");
    wait_for_visible_rows(&reopened, &[("cold-discovery".to_string(), 11)]).await;
    reopened
        .shutdown_stream_fold_driver()
        .await
        .expect("first cold driver joins");
    let published = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    let published_manifest = published.version();
    let published_table = published.entry(TABLE).unwrap().table_version;
    drop(reopened);

    let reopened = reopen_enrolled(&dir).await;
    let reopened =
        helpers::stream_authority::bind_checked_stream_runtime(reopened, &cluster_uri).await;
    reopened
        .start_stream_fold_driver()
        .await
        .expect("published retained WAL is still discoverable but already covered");
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    let unchanged = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(unchanged.version(), published_manifest);
    assert_eq!(
        unchanged.entry(TABLE).unwrap().table_version,
        published_table
    );
    reopened.shutdown_stream_fold_driver().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn sealed_ensure_indices_refreshes_a_productive_lane_and_preserves_resume() {
    let _scenario = FailScenario::setup();
    let (dir, db, folded, sealed) = init_productive_sealed_lane().await;
    assert_eq!(folded.lifecycle, "OPEN");
    assert_eq!(folded.last_fold_outcome.as_deref(), Some("PUBLISHED"));
    let folded_epoch = epoch_floor(&folded);
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

    let sealed_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let sealed_table_version = sealed_snapshot.entry(TABLE).unwrap().table_version;
    let ordinary_error = db
        .ensure_indices()
        .await
        .expect_err("ordinary EnsureIndices must remain fenced by SEALED authority");
    let OmniError::Manifest(manifest_error) = ordinary_error else {
        panic!("expected typed lifecycle conflict, got {ordinary_error:?}");
    };
    assert!(
        matches!(
            manifest_error.details,
            Some(omnigraph::error::ManifestConflictDetails::StreamLifecycleConflict {
                ref lifecycle,
                ref operation,
                ..
            }) if lifecycle == "SEALED" && operation == "ensure_indices"
        ),
        "{manifest_error:?}"
    );
    let after_refusal = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        after_refusal.version(),
        sealed_snapshot.version(),
        "ordinary EnsureIndices refusal must not advance graph authority"
    );
    assert_eq!(
        after_refusal.entry(TABLE).unwrap().table_version,
        sealed_table_version,
        "ordinary EnsureIndices refusal must not publish a table effect"
    );
    assert_eq!(stream_lane(&db).await, sealed);
    assert_no_recovery_sidecars(&dir);

    let pending = db
        .failpoint_stream_sealed_ensure_indices_for_test("operator:sealed-indices")
        .await
        .expect("checked SEALED maintenance must build the deferred id index");
    assert!(pending.is_empty());
    let maintained = stream_lane(&db).await;
    let mut expected_maintained = sealed.clone();
    expected_maintained.lifecycle_revision += 1;
    assert_eq!(
        maintained, expected_maintained,
        "maintenance may refresh only the exposed lifecycle revision while staying SEALED"
    );
    let maintained_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        maintained_snapshot.entry(TABLE).unwrap().table_version,
        sealed_table_version + 1,
        "productive index work must publish one exact table HEAD"
    );
    assert_eq!(
        visible_rows(&db).await,
        vec![("ordinary-before-quiesce".to_string(), 13)],
        "index maintenance must preserve graph content"
    );
    assert_no_recovery_sidecars(&dir);

    let before_no_work_status = db.stream_status().await.unwrap();
    let before_no_work_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let pending = db
        .failpoint_stream_sealed_ensure_indices_for_test("operator:sealed-indices")
        .await
        .expect("a repeated checked maintenance call must recognize no work");
    assert!(pending.is_empty());
    let after_no_work_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        db.stream_status().await.unwrap(),
        before_no_work_status,
        "no-work maintenance must not advance lifecycle authority"
    );
    assert_eq!(
        after_no_work_snapshot.version(),
        before_no_work_snapshot.version(),
        "no-work maintenance must not publish graph lineage"
    );
    assert_eq!(
        after_no_work_snapshot.entry(TABLE).unwrap().table_version,
        before_no_work_snapshot.entry(TABLE).unwrap().table_version,
        "no-work maintenance must not advance the table HEAD"
    );
    assert_no_recovery_sidecars(&dir);

    db.failpoint_stream_resume_for_test(
        TABLE,
        "91919191-9191-4191-8191-919191919191",
        maintained.lifecycle_revision,
        false,
        "operator:sealed-indices",
    )
    .await
    .expect("the maintained SEALED proof must remain resumable");
    let open = stream_lane(&db).await;
    assert_eq!(open.lifecycle, "OPEN");
    assert_eq!(open.lifecycle_revision, maintained.lifecycle_revision + 1);
    assert!(epoch_floor(&open) > epoch_floor(&maintained));

    let (_, already_durable) = authorized_ingest_score(
        &db,
        &open.stream_incarnation_id,
        "after-maintenance",
        14,
        1,
        "92919191-9291-4291-8291-929191919191",
        None,
        "operator:sealed-indices",
    )
    .await
    .expect("the resumed writer must accept authenticated ingress");
    assert!(!already_durable);
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the resumed generation must fold normally");
    assert_eq!(
        visible_rows(&db).await,
        vec![
            ("after-maintenance".to_string(), 14),
            ("ordinary-before-quiesce".to_string(), 13),
        ]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn sealed_ensure_indices_effects_confirmed_reopens_with_atomic_authority() {
    let _scenario = FailScenario::setup();
    let (dir, db, _, sealed) = init_productive_sealed_lane().await;
    let prior_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let prior_manifest_version = prior_snapshot.version();
    let prior_table_version = prior_snapshot.entry(TABLE).unwrap().table_version;

    let error = {
        let _failpoint = ScopedFailPoint::new(
            names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
            "return",
        );
        db.failpoint_stream_sealed_ensure_indices_for_test("operator:sealed-indices-recovery")
            .await
            .expect_err("the confirmed v16 intent must stop before manifest publication")
    };
    assert!(
        matches!(&error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains(names::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT),
        "{error:?}"
    );

    let unpublished = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(unpublished.version(), prior_manifest_version);
    assert_eq!(
        unpublished.entry(TABLE).unwrap().table_version,
        prior_table_version,
        "the graph pointer must remain on the prior table HEAD"
    );
    assert_eq!(
        stream_lane(&db).await,
        sealed,
        "the unpublished lifecycle successor must remain invisible"
    );
    assert_eq!(
        visible_rows(&db).await,
        vec![("ordinary-before-quiesce".to_string(), 13)]
    );

    let operation_ids = helpers::recovery::sidecar_operation_ids(dir.path());
    assert_eq!(
        operation_ids.len(),
        1,
        "confirmed v16 intent must remain durable"
    );
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{}.json", operation_ids[0]));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 16);
    assert_eq!(sidecar["writer_kind"], "StreamSealedMaintenance");
    assert_eq!(sidecar["protocol_v8"]["effect_phase"], "EffectsConfirmed");
    assert_eq!(sidecar["protocol_v16"]["kind"], "StreamSealedEnsureIndices");
    assert!(
        sidecar["protocol_v16"]["payload"]["next_lifecycles"]
            .as_array()
            .is_some_and(|entries| entries.len() == 1),
        "EffectsConfirmed v16 must bind the complete lifecycle successor"
    );

    drop(unpublished);
    drop(prior_snapshot);
    drop(db);
    let reopened = reopen_enrolled(&dir).await;
    let recovered_snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        recovered_snapshot.version(),
        prior_manifest_version + 1,
        "recovery must publish one graph-authority transition"
    );
    assert_eq!(
        recovered_snapshot.entry(TABLE).unwrap().table_version,
        prior_table_version + 1,
        "recovery must select the exact confirmed index HEAD"
    );
    let recovered = stream_lane(&reopened).await;
    let mut expected_recovered = sealed;
    expected_recovered.lifecycle_revision += 1;
    assert_eq!(
        recovered, expected_recovered,
        "the table pointer and SEALED lifecycle refresh must roll forward together"
    );
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("ordinary-before-quiesce".to_string(), 13)],
        "index recovery must preserve graph content"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn sealed_ensure_indices_armed_effect_compensates_with_refreshed_proof() {
    let _scenario = FailScenario::setup();
    let (dir, db, _, sealed) = init_productive_sealed_lane().await;
    let prior_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let prior_manifest_version = prior_snapshot.version();
    let prior_table_version = prior_snapshot.entry(TABLE).unwrap().table_version;

    let error = {
        let _failpoint =
            ScopedFailPoint::new(names::ENSURE_INDICES_POST_EFFECTS_PRE_CONFIRM, "return");
        db.failpoint_stream_sealed_ensure_indices_for_test("operator:sealed-indices-rollback")
            .await
            .expect_err("the v16 intent must stop after its index effect but remain Armed")
    };
    assert!(
        matches!(&error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains(names::ENSURE_INDICES_POST_EFFECTS_PRE_CONFIRM),
        "{error:?}"
    );

    let unpublished = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(unpublished.version(), prior_manifest_version);
    assert_eq!(
        unpublished.entry(TABLE).unwrap().table_version,
        prior_table_version,
        "the unconfirmed CreateIndex effect must not move the graph pointer"
    );
    assert_eq!(
        stream_lane(&db).await,
        sealed,
        "Armed physical work must not publish lifecycle authority"
    );
    assert_eq!(
        visible_rows(&db).await,
        vec![("ordinary-before-quiesce".to_string(), 13)]
    );

    let operation_ids = helpers::recovery::sidecar_operation_ids(dir.path());
    assert_eq!(
        operation_ids.len(),
        1,
        "the Armed v16 intent must remain durable"
    );
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{}.json", operation_ids[0]));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 16);
    assert_eq!(sidecar["writer_kind"], "StreamSealedMaintenance");
    assert_eq!(sidecar["protocol_v8"]["effect_phase"], "Armed");
    assert!(sidecar["protocol_v16"]["payload"]["next_lifecycles"].is_null());
    assert!(sidecar["protocol_v16"]["payload"]["rollback_lifecycles"].is_null());

    drop(unpublished);
    drop(prior_snapshot);
    let reopened = reopen_enrolled(&dir).await;
    let compensated_snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        compensated_snapshot.version(),
        prior_manifest_version + 1,
        "compensation must publish one graph-authority transition"
    );
    assert_eq!(
        compensated_snapshot.entry(TABLE).unwrap().table_version,
        prior_table_version + 2,
        "CreateIndex followed by compensation must publish the exact Restore HEAD"
    );
    let compensated = stream_lane(&reopened).await;
    let mut expected_compensated = sealed;
    expected_compensated.lifecycle_revision += 1;
    assert_eq!(
        compensated, expected_compensated,
        "the Restore HEAD must be selected with one refreshed SEALED proof"
    );
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("ordinary-before-quiesce".to_string(), 13)],
        "compensation must preserve graph content"
    );
    assert_no_recovery_sidecars(&dir);

    drop(compensated_snapshot);
    drop(reopened);
    let pending = db
        .failpoint_stream_sealed_ensure_indices_for_test("operator:sealed-indices-rollback")
        .await
        .expect("the checked handle must refresh and rebuild the compensated index");
    assert!(pending.is_empty());
    let rebuilt_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        rebuilt_snapshot.entry(TABLE).unwrap().table_version,
        prior_table_version + 3,
        "the missing index must still require one real CreateIndex commit"
    );
    let rebuilt = stream_lane(&db).await;
    let mut expected_rebuilt = compensated;
    expected_rebuilt.lifecycle_revision += 1;
    assert_eq!(rebuilt, expected_rebuilt);
    assert_eq!(
        visible_rows(&db).await,
        vec![("ordinary-before-quiesce".to_string(), 13)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn sealed_indices_then_optimize_refresh_productive_authority_and_preserve_resume() {
    let _scenario = FailScenario::setup();
    let (dir, db, initially_sealed) = init_productive_mixed_sealed_lane().await;
    let before_indices = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let pending = db
        .failpoint_stream_sealed_ensure_indices_for_test("operator:sealed-indices-then-optimize")
        .await
        .expect("checked EnsureIndices must build both deferred id indices");
    assert!(pending.is_empty());
    let sealed = stream_lane(&db).await;
    assert_eq!(sealed.lifecycle, "SEALED");
    assert_eq!(
        sealed.lifecycle_revision,
        initially_sealed.lifecycle_revision + 1,
        "productive EnsureIndices refreshes the enrolled SEALED witness exactly once"
    );
    let after_indices = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    for table_key in [TABLE, COMPANY_TABLE] {
        assert!(
            after_indices.entry(table_key).unwrap().table_version
                > before_indices.entry(table_key).unwrap().table_version,
            "checked EnsureIndices must publish real work for {table_key}"
        );
    }
    assert_eq!(helpers::count_rows(&db, TABLE).await, 4);
    assert_eq!(helpers::count_rows(&db, COMPANY_TABLE).await, 4);
    assert_no_recovery_sidecars(&dir);

    let sealed_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let sealed_person_version = sealed_snapshot.entry(TABLE).unwrap().table_version;
    let sealed_company_version = sealed_snapshot.entry("node:Company").unwrap().table_version;
    let sealed_commit_count = db.list_commits(Some("main")).await.unwrap().len();

    let ordinary_error = db
        .optimize()
        .await
        .expect_err("ambient Optimize must remain fenced by productive SEALED authority");
    let OmniError::Manifest(manifest_error) = ordinary_error else {
        panic!("expected typed lifecycle conflict, got {ordinary_error:?}");
    };
    assert!(
        matches!(
            manifest_error.details,
            Some(omnigraph::error::ManifestConflictDetails::StreamLifecycleConflict {
                ref lifecycle,
                ref operation,
                ..
            }) if lifecycle == "SEALED" && operation == "optimize"
        ),
        "{manifest_error:?}"
    );
    let after_refusal = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(after_refusal.version(), sealed_snapshot.version());
    assert_eq!(
        after_refusal.entry(TABLE).unwrap().table_version,
        sealed_person_version
    );
    assert_eq!(
        after_refusal.entry("node:Company").unwrap().table_version,
        sealed_company_version
    );
    assert_eq!(stream_lane(&db).await, sealed);
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        sealed_commit_count
    );
    assert_no_recovery_sidecars(&dir);

    let stats = db
        .failpoint_stream_sealed_optimize_for_test("operator:sealed-optimize")
        .await
        .expect("checked SEALED Optimize must compact the enrolled and ordinary siblings");
    for table_key in [TABLE, "node:Company"] {
        assert!(
            stats
                .iter()
                .any(|stat| stat.table_key == table_key && stat.committed),
            "{table_key} must complete productive Optimize work: {stats:?}"
        );
    }
    let maintained = stream_lane(&db).await;
    let mut expected_maintained = sealed.clone();
    expected_maintained.lifecycle_revision += 1;
    assert_eq!(
        maintained, expected_maintained,
        "only the productive enrolled lane needs one refreshed SEALED witness"
    );
    let maintained_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        sealed_commit_count + 1,
        "all productive table pointers and lifecycle authority publish in one graph commit"
    );
    assert!(maintained_snapshot.entry(TABLE).unwrap().table_version > sealed_person_version);
    assert!(
        maintained_snapshot
            .entry("node:Company")
            .unwrap()
            .table_version
            > sealed_company_version
    );
    assert_eq!(helpers::count_rows(&db, TABLE).await, 4);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 4);
    assert_no_recovery_sidecars(&dir);

    let before_no_work_status = db.stream_status().await.unwrap();
    let before_no_work_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let before_no_work_commit_count = db.list_commits(Some("main")).await.unwrap().len();
    let repeated = db
        .failpoint_stream_sealed_optimize_for_test("operator:sealed-optimize")
        .await
        .expect("a repeated checked Optimize must recognize no data-table work");
    assert!(
        repeated
            .iter()
            .filter(|stat| stat.table_key == TABLE || stat.table_key == "node:Company")
            .all(|stat| !stat.committed),
        "the converged data tables must be true no-ops: {repeated:?}"
    );
    let after_no_work_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(db.stream_status().await.unwrap(), before_no_work_status);
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        before_no_work_commit_count,
        "no-work Optimize must not create graph lineage"
    );
    assert_eq!(
        after_no_work_snapshot.entry(TABLE).unwrap().table_version,
        before_no_work_snapshot.entry(TABLE).unwrap().table_version
    );
    assert_eq!(
        after_no_work_snapshot
            .entry("node:Company")
            .unwrap()
            .table_version,
        before_no_work_snapshot
            .entry("node:Company")
            .unwrap()
            .table_version
    );
    assert_no_recovery_sidecars(&dir);

    db.failpoint_stream_resume_for_test(
        TABLE,
        "94949494-9494-4494-8494-949494949494",
        maintained.lifecycle_revision,
        false,
        "operator:sealed-optimize",
    )
    .await
    .expect("the Optimize-refreshed SEALED proof must remain resumable");
    let open = stream_lane(&db).await;
    assert_eq!(open.lifecycle, "OPEN");
    assert_eq!(open.lifecycle_revision, maintained.lifecycle_revision + 1);

    let (_, already_durable) = authorized_ingest_score(
        &db,
        &open.stream_incarnation_id,
        "after-optimize",
        14,
        1,
        "95959595-9595-4595-8595-959595959595",
        None,
        "operator:sealed-optimize",
    )
    .await
    .expect("the resumed writer must accept authenticated ingress");
    assert!(!already_durable);
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the resumed generation must fold normally");
    assert_eq!(helpers::count_rows(&db, TABLE).await, 5);
    assert_eq!(helpers::count_rows(&db, "node:Company").await, 4);
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn sealed_optimize_effects_confirmed_reopens_with_atomic_authority() {
    let _scenario = FailScenario::setup();
    let (dir, db, _, sealed) = init_productive_sealed_lane().await;
    let prior_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let prior_manifest_version = prior_snapshot.version();
    let prior_table_version = prior_snapshot.entry(TABLE).unwrap().table_version;

    let error = {
        let _failpoint =
            ScopedFailPoint::new(names::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT, "return");
        db.failpoint_stream_sealed_optimize_for_test("operator:sealed-optimize-recovery")
            .await
            .expect_err("the confirmed v17 intent must stop before manifest publication")
    };
    assert!(
        matches!(&error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        error
            .to_string()
            .contains(names::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT),
        "{error:?}"
    );

    let achieved_table_head = raw_table_head(&db, TABLE).await;
    assert!(
        achieved_table_head > prior_table_version,
        "productive Optimize must have an exact achieved HEAD before confirmation"
    );
    let unpublished = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(unpublished.version(), prior_manifest_version);
    assert_eq!(
        unpublished.entry(TABLE).unwrap().table_version,
        prior_table_version
    );
    assert_eq!(stream_lane(&db).await, sealed);

    let operation_ids = helpers::recovery::sidecar_operation_ids(dir.path());
    assert_eq!(operation_ids.len(), 1);
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{}.json", operation_ids[0]));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 17);
    assert_eq!(sidecar["protocol_v17"]["kind"], "StreamSealedOptimize");
    assert_eq!(
        sidecar["protocol_v17"]["payload"]["effect_phase"],
        "EffectsConfirmed"
    );
    assert_eq!(
        sidecar["protocol_v17"]["payload"]["confirmed_outputs"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(
        sidecar["protocol_v17"]["payload"]["next_lifecycles"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );

    drop(unpublished);
    drop(prior_snapshot);
    drop(db);
    let reopened = reopen_enrolled(&dir).await;
    let recovered_snapshot = reopened
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(recovered_snapshot.version(), prior_manifest_version + 1);
    assert_eq!(
        recovered_snapshot.entry(TABLE).unwrap().table_version,
        achieved_table_head,
        "recovery must select only the exact confirmed Optimize HEAD"
    );
    let recovered = stream_lane(&reopened).await;
    let mut expected_recovered = sealed;
    expected_recovered.lifecycle_revision += 1;
    assert_eq!(
        recovered, expected_recovered,
        "the table pointer and refreshed SEALED proof must roll forward together"
    );
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("ordinary-before-quiesce".to_string(), 13)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn sealed_optimize_partial_armed_effect_restores_with_matching_lifecycle_proof() {
    let _scenario = FailScenario::setup();
    let (dir, db, sealed) = init_productive_mixed_sealed_lane().await;
    let prior_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let prior_manifest_version = prior_snapshot.version();
    let prior_person_pin = prior_snapshot.entry(TABLE).unwrap().table_version;
    let prior_company_pin = prior_snapshot.entry("node:Company").unwrap().table_version;
    assert_eq!(raw_table_head(&db, TABLE).await, prior_person_pin);
    assert_eq!(raw_table_head(&db, "node:Company").await, prior_company_pin);

    let error = {
        // The first physical task stops before compaction while its sibling is
        // allowed to finish. Recovery must compensate the proper prefix under
        // the one v17 intent without publishing either table early.
        let _failpoint = ScopedFailPoint::new(names::OPTIMIZE_BEFORE_COMPACT, "1*return");
        db.failpoint_stream_sealed_optimize_for_test("operator:sealed-optimize-rollback")
            .await
            .expect_err("a partial Armed Optimize must retain recovery ownership")
    };
    assert!(
        matches!(&error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );

    let unpublished = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(unpublished.version(), prior_manifest_version);
    assert_eq!(
        unpublished.entry(TABLE).unwrap().table_version,
        prior_person_pin
    );
    assert_eq!(
        unpublished.entry("node:Company").unwrap().table_version,
        prior_company_pin
    );
    assert_eq!(stream_lane(&db).await, sealed);

    let effected_person_head = raw_table_head(&db, TABLE).await;
    let effected_company_head = raw_table_head(&db, "node:Company").await;
    let person_moved = effected_person_head > prior_person_pin;
    let company_moved = effected_company_head > prior_company_pin;
    assert_ne!(
        person_moved, company_moved,
        "exactly one bounded-parallel table task must complete: Person {prior_person_pin}->{effected_person_head}, Company {prior_company_pin}->{effected_company_head}"
    );

    let operation_ids = helpers::recovery::sidecar_operation_ids(dir.path());
    assert_eq!(operation_ids.len(), 1);
    let sidecar_path = dir
        .path()
        .join("__recovery")
        .join(format!("{}.json", operation_ids[0]));
    let sidecar: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(sidecar_path).unwrap()).unwrap();
    assert_eq!(sidecar["schema_version"], 17);
    assert_eq!(sidecar["protocol_v17"]["kind"], "StreamSealedOptimize");
    assert_eq!(sidecar["protocol_v17"]["payload"]["effect_phase"], "Armed");
    assert!(sidecar["protocol_v17"]["payload"]["confirmed_outputs"].is_null());

    drop(unpublished);
    drop(prior_snapshot);
    drop(db);
    let recovered = reopen_enrolled(&dir).await;
    let compensated_snapshot = recovered
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        compensated_snapshot.version(),
        prior_manifest_version + 1,
        "one compensating graph commit must select every restored pointer and proof"
    );
    let compensated_person_pin = compensated_snapshot.entry(TABLE).unwrap().table_version;
    let compensated_company_pin = compensated_snapshot
        .entry("node:Company")
        .unwrap()
        .table_version;
    if person_moved {
        assert!(compensated_person_pin > effected_person_head);
    } else {
        assert_eq!(compensated_person_pin, prior_person_pin);
    }
    if company_moved {
        assert!(compensated_company_pin > effected_company_head);
    } else {
        assert_eq!(compensated_company_pin, prior_company_pin);
    }
    let compensated = stream_lane(&recovered).await;
    let mut expected_compensated = sealed;
    if person_moved {
        expected_compensated.lifecycle_revision += 1;
    }
    assert_eq!(
        compensated, expected_compensated,
        "only a restored enrolled table receives a refreshed SEALED proof"
    );
    assert_eq!(helpers::count_rows(&recovered, TABLE).await, 4);
    assert_eq!(helpers::count_rows(&recovered, "node:Company").await, 4);
    assert_no_recovery_sidecars(&dir);

    drop(compensated_snapshot);
    let cluster_uri = dir.cluster_uri();
    let recovered =
        helpers::stream_authority::bind_checked_stream_runtime(recovered, &cluster_uri).await;
    let stats = recovered
        .failpoint_stream_sealed_optimize_for_test("operator:sealed-optimize-rollback")
        .await
        .expect("the compensated graph must remain retryable");
    for table_key in [TABLE, "node:Company"] {
        assert!(
            stats
                .iter()
                .any(|stat| stat.table_key == table_key && stat.committed),
            "the restored productive table must be replanned: {stats:?}"
        );
    }
    let retried = stream_lane(&recovered).await;
    assert_eq!(retried.lifecycle, "SEALED");
    assert_eq!(
        retried.lifecycle_revision,
        compensated.lifecycle_revision + 1
    );
    assert_eq!(helpers::count_rows(&recovered, TABLE).await, 4);
    assert_eq!(helpers::count_rows(&recovered, "node:Company").await, 4);
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_persists_a_stable_data_block_when_dead_letter_object_overflows() {
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
    let before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let raw_base_head_before = raw_table_head(&db, TABLE).await;
    let (_, current_token_before) = db
        .failpoint_stream_token_lookup_for_cost_test(TABLE, "drain-blocked")
        .await
        .expect("the overflow fixture token lookup must succeed");
    assert!(
        current_token_before.is_none(),
        "an admitted but unfolded occurrence must not yet be selected as current authority"
    );
    let dead_letter_objects_before = dead_letter_object_names(&dir);
    assert!(
        dead_letter_objects_before.is_empty(),
        "the overflow fixture must begin without dead-letter object residue"
    );
    let before_lane = stream_lane(&db).await;
    let drain_id = "96969696-9696-4696-8696-969696969696";
    let actor = "operator:strict-data-block";

    let first_error = {
        let _overflow = force_dead_letter_object_overflow_once();
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, before_lane.lifecycle_revision, actor)
            .await
            .expect_err("dead-letter object overflow must publish a strict data block")
    };
    let blocked = stream_lane(&db).await;
    let block_token = blocked
        .strict_block_token
        .clone()
        .expect("DRAINING dead-letter object overflow must expose its durable block token");
    assert_eq!(stream_data_block_token(&first_error), block_token);
    assert_eq!(blocked.lifecycle, "DRAINING");
    assert_eq!(blocked.drain_id.as_deref(), Some(drain_id));
    assert_eq!(blocked.last_fold_outcome.as_deref(), Some("STRICT_BLOCKED"));
    assert_eq!(blocked.last_fold_graph_commit_id, None);
    assert!(
        blocked.lifecycle_revision > before_lane.lifecycle_revision,
        "start, claim, and block publications must advance lifecycle authority"
    );
    let after = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
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
    assert_eq!(
        raw_table_head(&db, TABLE).await,
        raw_base_head_before,
        "overflow must become a DataBlock before a raw base-table effect"
    );
    let (_, current_token_after) = db
        .failpoint_stream_token_lookup_for_cost_test(TABLE, "drain-blocked")
        .await
        .expect("the selected DataBlock token lookup must succeed");
    assert_eq!(
        current_token_after, current_token_before,
        "DataBlock evidence must not select any current-token authority"
    );
    assert_eq!(
        dead_letter_object_names(&dir),
        dead_letter_objects_before,
        "overflow must become a DataBlock before canonical-object creation"
    );
    let selected_dead_letters =
        helpers::stream_authority::list_stream_dead_letters(&db, &dir.cluster_uri(), None)
            .await
            .expect("the blocked selected token cut must remain inspectable");
    assert!(
        selected_dead_letters.entries.is_empty(),
        "DataBlock evidence must not publish DEAD_LETTERED current-token authority"
    );
    assert_no_recovery_sidecars(&dir);

    let blocked_version = after.version();
    let exact_retry_error = db
        .failpoint_stream_quiesce_for_test(TABLE, drain_id, before_lane.lifecycle_revision, actor)
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

    let abort_error = db
        .failpoint_stream_resume_for_test(
            TABLE,
            "8a8a8a8a-8a8a-4a8a-8a8a-8a8a8a8a8a8a",
            blocked.lifecycle_revision,
            true,
            "operator:blocked-abort",
        )
        .await
        .expect_err("abort-drain must not reopen around a selected strict block");
    assert!(
        abort_error.to_string().contains("strict-blocked"),
        "{abort_error:?}"
    );
    assert_eq!(stream_lane(&db).await, blocked);
    assert_no_recovery_sidecars(&dir);

    drop(db);
    let reopened = reopen_enrolled(&dir).await;
    let reopen_error = reopened
        .failpoint_stream_quiesce_for_test(TABLE, drain_id, before_lane.lifecycle_revision, actor)
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

async fn prepare_unique_conflict_data_block_via_object_overflow(
    logical_id: &str,
    drain_id: &str,
    actor: &str,
    seed_prior_value_for_logical_id: bool,
) -> (EnrolledGraphDir, Arc<Omnigraph>, StreamTableStatus, u64) {
    let cluster = tempfile::tempdir().unwrap();
    let graph = cluster.path().join("graphs/knowledge.omni");
    let db = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), UNIQUE_STREAM_SCHEMA)
            .await
            .unwrap(),
    );
    if seed_prior_value_for_logical_id {
        let seed = [
            serde_json::json!({
                "type": "Person",
                "data": {"id": logical_id, "score": 1},
            }),
            serde_json::json!({
                "type": "Person",
                "data": {"id": "uniqueness-conflict", "score": 7},
            }),
        ]
        .into_iter()
        .map(|row| row.to_string())
        .collect::<Vec<_>>()
        .join("\n");
        load_jsonl(&db, &seed, LoadMode::Overwrite)
            .await
            .expect("seed the prior winner and uniqueness conflict before enrollment");
    } else {
        db.mutate(
            "main",
            INSERT_PERSON,
            "insert_person",
            &helpers::int_params(&[("$score", 7)]),
        )
        .await
        .expect("seed the committed uniqueness conflict before enrollment");
    }
    db.failpoint_enroll_stream_table_for_test(TABLE)
        .await
        .unwrap();
    enable_stream_profile(&db, &format!("file://{}", cluster.path().display())).await;
    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };

    let duplicate = physical_batch(&db, &[(logical_id.to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
        .await
        .expect("base-dependent uniqueness remains fold-time work");
    let open_revision = stream_lane(&db).await.lifecycle_revision;
    let error = {
        let _overflow = force_dead_letter_object_overflow_once();
        db.failpoint_stream_quiesce_for_test(TABLE, drain_id, open_revision, actor)
            .await
            .expect_err(
                "the uniqueness conflict must publish a DataBlock when its dead-letter object overflows",
            )
    };
    let blocked = stream_lane(&db).await;
    assert_eq!(
        stream_data_block_token(&error),
        blocked.strict_block_token.as_deref().unwrap()
    );
    assert_no_recovery_sidecars(&dir);
    (dir, db, blocked, open_revision)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn data_block_show_and_replace_unstrand_the_exact_drain() {
    let _scenario = FailScenario::setup();
    let logical_id = "replace-blocked";
    let drain_id = "a1a1a1a1-a1a1-41a1-81a1-a1a1a1a1a1a1";
    let actor = "operator:data-block-replace";
    let (dir, db, blocked, open_revision) =
        prepare_unique_conflict_data_block_via_object_overflow(logical_id, drain_id, actor, false)
            .await;
    let block_token = blocked.strict_block_token.clone().unwrap();

    drop(db);
    let reopened = reopen_enrolled(&dir).await;
    let page = reopened
        .failpoint_show_stream_data_block_for_test(TABLE, &block_token, None)
        .await
        .expect("cold inspection must reconstruct the exact immutable blocked generation");
    assert_eq!(page.block_token, block_token);
    assert_eq!(page.lifecycle_revision, blocked.lifecycle_revision);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].ordinal, 0);
    assert_eq!(page.entries[0].logical_key, logical_id);
    assert_eq!(page.entries[0].table_key, TABLE);
    assert_eq!(page.entries[0].violation_code, "UNIQUE_VIOLATION");
    assert_eq!(page.entries[0].allowed_actions, ["REPLACE", "WITHDRAW"]);
    assert!(page.next_cursor.is_none());

    let correction_id = "b2b2b2b2-b2b2-42b2-82b2-b2b2b2b2b2b2";
    let request = StreamDataCorrectionRequest {
        protocol_version: 1,
        block_token: block_token.clone(),
        correction_id: correction_id.to_string(),
        expected_lifecycle_revision: blocked.lifecycle_revision,
        actions: vec![StreamDataCorrectionAction::Replace {
            ordinal: page.entries[0].ordinal,
            logical_key: logical_id.to_string(),
            current_blocked_winner_stream_token: page.entries[0]
                .current_blocked_winner_stream_token
                .clone(),
            write_id: "c3c3c3c3-c3c3-43c3-83c3-c3c3c3c3c3c3".to_string(),
            row: serde_json::json!({"id": logical_id, "score": 8}),
        }],
        expected_plan_digest: None,
    };
    let corrected = reopened
        .failpoint_correct_stream_data_block_for_test(TABLE, request.clone(), actor)
        .await
        .expect("a valid replacement must publish one exact base/token correction");
    assert!(corrected.changed);
    assert_eq!(corrected.correction_id, correction_id);
    let corrected_lane = stream_lane(&reopened).await;
    assert_eq!(corrected_lane.lifecycle, "DRAINING");
    assert_eq!(
        corrected_lane.lifecycle_revision,
        corrected.lifecycle_revision
    );
    assert_eq!(corrected_lane.strict_block_token, None);
    assert_eq!(
        corrected_lane.last_fold_outcome.as_deref(),
        Some("PUBLISHED")
    );
    assert_eq!(corrected_lane.drain_id.as_deref(), Some(drain_id));
    let rows = visible_rows(&reopened).await;
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(logical_id.to_string(), 8)));
    assert!(
        rows.iter()
            .any(|(id, score)| id != logical_id && *score == 7),
        "replacement must preserve the prior visible base row"
    );
    assert_no_recovery_sidecars(&dir);

    let corrected_version = corrected.manifest_version;
    let replay = reopened
        .failpoint_correct_stream_data_block_for_test(TABLE, request.clone(), actor)
        .await
        .expect(
            "receipt-first correction replay must ignore the now-cleared block and stale revision",
        );
    assert!(!replay.changed);
    assert_eq!(replay.correction_id, corrected.correction_id);
    assert_eq!(replay.plan_digest, corrected.plan_digest);
    assert_eq!(replay.graph_commit_id, corrected.graph_commit_id);
    assert_eq!(replay.lifecycle_revision, corrected.lifecycle_revision);
    assert_eq!(replay.manifest_version, corrected_version);

    let mut wrong_revision_retry = request.clone();
    wrong_revision_retry.expected_lifecycle_revision = wrong_revision_retry
        .expected_lifecycle_revision
        .checked_add(1)
        .unwrap();
    let wrong_revision_error = reopened
        .failpoint_correct_stream_data_block_for_test(TABLE, wrong_revision_retry, actor)
        .await
        .expect_err("receipt replay must bind the exact expected lifecycle revision");
    assert!(
        matches!(
            wrong_revision_error,
            OmniError::StreamLifecycleIdempotencyConflict { .. }
        ),
        "{wrong_revision_error:?}"
    );

    reopened
        .failpoint_stream_quiesce_for_test(TABLE, drain_id, open_revision, actor)
        .await
        .expect("the corrected fold proof must let the original drain reach SEALED");
    assert_eq!(stream_lane(&reopened).await.lifecycle, "SEALED");
    let replay_after_later_manifest = reopened
        .failpoint_correct_stream_data_block_for_test(TABLE, request, actor)
        .await
        .expect("the correction receipt must retain its original result after a later manifest");
    assert!(!replay_after_later_manifest.changed);
    assert_eq!(
        replay_after_later_manifest.manifest_version, corrected_version,
        "receipt-first replay must return the correction's manifest, not the current graph head"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn correction_id_reuse_on_a_later_block_is_effect_free() {
    let _scenario = FailScenario::setup();
    let actor = "operator:correction-id-reuse";
    let first_logical_id = "first-correction-id-owner";
    let first_drain_id = "a4a4a4a4-a4a4-44a4-84a4-a4a4a4a4a4a4";
    let correction_id = "b5b5b5b5-b5b5-45b5-85b5-b5b5b5b5b5b5";
    let (dir, db, first_blocked, first_open_revision) =
        prepare_unique_conflict_data_block_via_object_overflow(
            first_logical_id,
            first_drain_id,
            actor,
            false,
        )
        .await;
    let first_block_token = first_blocked.strict_block_token.clone().unwrap();
    let first_page = db
        .failpoint_show_stream_data_block_for_test(TABLE, &first_block_token, None)
        .await
        .unwrap();
    db.failpoint_correct_stream_data_block_for_test(
        TABLE,
        StreamDataCorrectionRequest {
            protocol_version: 1,
            block_token: first_block_token,
            correction_id: correction_id.to_string(),
            expected_lifecycle_revision: first_blocked.lifecycle_revision,
            actions: vec![StreamDataCorrectionAction::Replace {
                ordinal: first_page.entries[0].ordinal,
                logical_key: first_logical_id.to_string(),
                current_blocked_winner_stream_token: first_page.entries[0]
                    .current_blocked_winner_stream_token
                    .clone(),
                write_id: "c6c6c6c6-c6c6-46c6-86c6-c6c6c6c6c6c6".to_string(),
                row: serde_json::json!({"id": first_logical_id, "score": 8}),
            }],
            expected_plan_digest: None,
        },
        actor,
    )
    .await
    .expect("the first correction must bind the operation id");
    db.failpoint_stream_quiesce_for_test(TABLE, first_drain_id, first_open_revision, actor)
        .await
        .expect("the first corrected drain must seal");
    let sealed = stream_lane(&db).await;
    db.failpoint_stream_resume_for_test(
        TABLE,
        "d7d7d7d7-d7d7-47d7-87d7-d7d7d7d7d7d7",
        sealed.lifecycle_revision,
        false,
        actor,
    )
    .await
    .expect("the same stream incarnation must reopen for a later block");
    let reopened = stream_lane(&db).await;

    let second_logical_id = "second-correction-id-owner";
    let duplicate = physical_batch(&db, &[(second_logical_id.to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(duplicate), 0)
        .await
        .expect("the later conflicting generation must become durable");
    let second_drain_id = "e8e8e8e8-e8e8-48e8-88e8-e8e8e8e8e8e8";
    let block_error = {
        let _overflow = force_dead_letter_object_overflow_once();
        db.failpoint_stream_quiesce_for_test(
            TABLE,
            second_drain_id,
            reopened.lifecycle_revision,
            actor,
        )
        .await
        .expect_err(
            "the later uniqueness conflict must publish another DataBlock when its dead-letter object overflows",
        )
    };
    let second_blocked = stream_lane(&db).await;
    let second_block_token = second_blocked.strict_block_token.clone().unwrap();
    assert_eq!(stream_data_block_token(&block_error), second_block_token);
    let second_page = db
        .failpoint_show_stream_data_block_for_test(TABLE, &second_block_token, None)
        .await
        .unwrap();
    let reused_request = StreamDataCorrectionRequest {
        protocol_version: 1,
        block_token: second_block_token,
        correction_id: correction_id.to_string(),
        expected_lifecycle_revision: second_blocked.lifecycle_revision,
        actions: vec![StreamDataCorrectionAction::Replace {
            ordinal: second_page.entries[0].ordinal,
            logical_key: second_logical_id.to_string(),
            current_blocked_winner_stream_token: second_page.entries[0]
                .current_blocked_winner_stream_token
                .clone(),
            write_id: "f9f9f9f9-f9f9-49f9-89f9-f9f9f9f9f9f9".to_string(),
            row: serde_json::json!({"id": second_logical_id, "score": 9}),
        }],
        expected_plan_digest: None,
    };
    let before_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    let before_table_head = raw_table_head(&db, TABLE).await;
    let before_token_head = raw_stream_token_head(&db).await;
    let before_rows = visible_rows(&db).await;
    let before_commit_count = db.list_commits(Some("main")).await.unwrap().len();

    let error = db
        .failpoint_correct_stream_data_block_for_test(TABLE, reused_request, actor)
        .await
        .expect_err("one correction operation id cannot be rebound to a later block");
    assert!(
        matches!(error, OmniError::StreamLifecycleIdempotencyConflict { .. }),
        "{error:?}"
    );
    let after_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(after_snapshot.version(), before_snapshot.version());
    assert_eq!(
        after_snapshot.entry(TABLE).unwrap().table_version,
        before_snapshot.entry(TABLE).unwrap().table_version
    );
    assert_eq!(raw_table_head(&db, TABLE).await, before_table_head);
    assert_eq!(raw_stream_token_head(&db).await, before_token_head);
    assert_eq!(stream_lane(&db).await, second_blocked);
    assert_eq!(visible_rows(&db).await, before_rows);
    assert_eq!(
        db.list_commits(Some("main")).await.unwrap().len(),
        before_commit_count
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn data_block_withdraw_uses_a_marker_only_base_effect_and_unstrands_the_drain() {
    let _scenario = FailScenario::setup();
    let logical_id = "withdraw-blocked";
    let drain_id = "d4d4d4d4-d4d4-44d4-84d4-d4d4d4d4d4d4";
    let actor = "operator:data-block-withdraw";
    let (dir, db, blocked, _open_revision) =
        prepare_unique_conflict_data_block_via_object_overflow(logical_id, drain_id, actor, true)
            .await;
    let cluster_uri = dir.cluster_uri();
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let blocked_manifest_version = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();
    db.start_stream_fold_driver()
        .await
        .expect("the resident owner must park the blocked DRAINING lane");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        blocked_manifest_version,
        "a selected DataBlock must park without manifest churn"
    );
    assert_eq!(stream_lane(&db).await, blocked);
    let block_token = blocked.strict_block_token.clone().unwrap();
    let page = db
        .failpoint_show_stream_data_block_for_test(TABLE, &block_token, None)
        .await
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert!(
        page.entries[0]
            .allowed_actions
            .iter()
            .any(|action| action == "WITHDRAW")
    );
    let request = StreamDataCorrectionRequest {
        protocol_version: 1,
        block_token,
        correction_id: "e5e5e5e5-e5e5-45e5-85e5-e5e5e5e5e5e5".to_string(),
        expected_lifecycle_revision: blocked.lifecycle_revision,
        actions: vec![StreamDataCorrectionAction::Withdraw {
            ordinal: page.entries[0].ordinal,
            logical_key: logical_id.to_string(),
            current_blocked_winner_stream_token: page.entries[0]
                .current_blocked_winner_stream_token
                .clone(),
        }],
        expected_plan_digest: None,
    };
    let corrected = db
        .failpoint_correct_stream_data_block_for_test(TABLE, request.clone(), actor)
        .await
        .expect("all-WITHDRAW must still commit an exact marker-only base transaction");
    assert!(corrected.changed);
    let lane = stream_lane(&db).await;
    assert!(
        matches!(lane.lifecycle, "DRAINING" | "SEALED"),
        "correction may race the resident continuation but must not reopen the lane"
    );
    assert_eq!(lane.strict_block_token, None);
    let rows = visible_rows(&db).await;
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .any(|(id, score)| id == logical_id && *score == 1),
        "withdrawal must preserve the older visible winner for the same logical key"
    );
    assert!(
        rows.iter()
            .any(|(id, score)| id == "uniqueness-conflict" && *score == 7)
    );
    wait_for_stream_lifecycle(&db, "SEALED").await;
    db.shutdown_stream_fold_driver()
        .await
        .expect("the corrected resident drain must join cleanly");
    assert_no_recovery_sidecars(&dir);

    drop(db);
    let reopened = reopen_enrolled(&dir).await;
    let replay = reopened
        .failpoint_correct_stream_data_block_for_test(TABLE, request, actor)
        .await
        .expect("the immutable withdrawal receipt must survive cold reopen");
    assert!(!replay.changed);
    assert_eq!(replay.graph_commit_id, corrected.graph_commit_id);
    assert_eq!(stream_lane(&reopened).await.lifecycle, "SEALED");
    assert_no_recovery_sidecars(&dir);
}

struct DataBlockCorrectionRecoveryBoundary {
    failpoint: &'static str,
    logical_id: &'static str,
    drain_id: &'static str,
    correction_id: &'static str,
    write_id: &'static str,
    recovery_publishes: bool,
}

async fn assert_data_block_correction_recovery_boundary(
    boundary: DataBlockCorrectionRecoveryBoundary,
) {
    let actor = "operator:data-block-recovery";
    let (dir, db, blocked, open_revision) = prepare_unique_conflict_data_block_via_object_overflow(
        boundary.logical_id,
        boundary.drain_id,
        actor,
        false,
    )
    .await;
    let block_token = blocked.strict_block_token.clone().unwrap();
    let page = db
        .failpoint_show_stream_data_block_for_test(TABLE, &block_token, None)
        .await
        .unwrap();
    let request = StreamDataCorrectionRequest {
        protocol_version: 1,
        block_token: block_token.clone(),
        correction_id: boundary.correction_id.to_string(),
        expected_lifecycle_revision: blocked.lifecycle_revision,
        actions: vec![StreamDataCorrectionAction::Replace {
            ordinal: page.entries[0].ordinal,
            logical_key: boundary.logical_id.to_string(),
            current_blocked_winner_stream_token: page.entries[0]
                .current_blocked_winner_stream_token
                .clone(),
            write_id: boundary.write_id.to_string(),
            row: serde_json::json!({"id": boundary.logical_id, "score": 8}),
        }],
        expected_plan_digest: None,
    };

    let error = {
        let _boundary = ScopedFailPoint::new(boundary.failpoint, "return");
        db.failpoint_correct_stream_data_block_for_test(TABLE, request.clone(), actor)
            .await
            .expect_err("the injected crash cell must retain recovery ownership")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(
        visible_rows(&db)
            .await
            .iter()
            .all(|(id, _)| id != boundary.logical_id),
        "an unconfirmed correction participant must not become graph-visible"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1,
        "the injected correction cell must remain recovery-owned"
    );
    drop(db);

    if !boundary.recovery_publishes {
        let inspect_error = match Omnigraph::open_read_only(dir.path().to_str().unwrap()).await {
            Ok(_) => panic!("read-only block inspection must refuse pending recovery"),
            Err(error) => error,
        };
        assert!(
            matches!(inspect_error, OmniError::RecoveryRequired { .. }),
            "{inspect_error:?}"
        );
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1,
            "read-only inspection must leave recovery ownership untouched"
        );
    }

    let reopened = reopen_enrolled(&dir).await;
    assert_no_recovery_sidecars(&dir);
    let result = if boundary.recovery_publishes {
        let lane = stream_lane(&reopened).await;
        assert_eq!(lane.lifecycle, "DRAINING");
        assert_eq!(lane.strict_block_token, None);
        let replay = reopened
            .failpoint_correct_stream_data_block_for_test(TABLE, request.clone(), actor)
            .await
            .expect("recovery-published correction must replay from its receipt");
        assert!(!replay.changed);
        replay
    } else {
        let lane = stream_lane(&reopened).await;
        assert_eq!(
            lane.strict_block_token.as_deref(),
            Some(block_token.as_str())
        );
        reopened
            .failpoint_correct_stream_data_block_for_test(TABLE, request.clone(), actor)
            .await
            .expect("effect-free recovery must leave the exact correction retryable")
    };
    assert_eq!(result.correction_id, boundary.correction_id);
    assert!(
        visible_rows(&reopened)
            .await
            .contains(&(boundary.logical_id.to_string(), 8))
    );
    reopened
        .failpoint_stream_quiesce_for_test(TABLE, boundary.drain_id, open_revision, actor)
        .await
        .expect("every recovered correction must let the original drain finish");
    assert_eq!(stream_lane(&reopened).await.lifecycle, "SEALED");
    assert_no_recovery_sidecars(&dir);
}

#[test]
#[serial]
fn data_block_correction_recovery_closes_each_two_participant_crash_cell() {
    on_big_stack(|| async {
        let _scenario = FailScenario::setup();
        let boundaries = [
            DataBlockCorrectionRecoveryBoundary {
                failpoint: names::STREAM_FOLD_POST_SIDECAR_PRE_BASE_COMMIT,
                logical_id: "correction-arm-only",
                drain_id: "11111111-1111-4111-8111-111111111111",
                correction_id: "21111111-1111-4111-8111-111111111111",
                write_id: "31111111-1111-4111-8111-111111111111",
                recovery_publishes: false,
            },
            DataBlockCorrectionRecoveryBoundary {
                failpoint: names::STREAM_FOLD_POST_BASE_COMMIT_PRE_TOKEN_COMMIT,
                logical_id: "correction-base-only",
                drain_id: "12222222-2222-4222-8222-222222222222",
                correction_id: "22222222-2222-4222-8222-222222222222",
                write_id: "32222222-2222-4222-8222-222222222222",
                recovery_publishes: true,
            },
            DataBlockCorrectionRecoveryBoundary {
                failpoint: names::STREAM_FOLD_POST_TOKEN_COMMIT_PRE_CONFIRM,
                logical_id: "correction-both-effects",
                drain_id: "13333333-3333-4333-8333-333333333333",
                correction_id: "23333333-3333-4333-8333-333333333333",
                write_id: "33333333-3333-4333-8333-333333333333",
                recovery_publishes: true,
            },
        ];

        for boundary in boundaries {
            Box::pin(assert_data_block_correction_recovery_boundary(boundary)).await;
        }
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_persists_a_stable_data_block_when_min_cardinality_dead_letter_object_overflows() {
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

    let first_error = {
        let _overflow = force_dead_letter_object_overflow_once();
        db.failpoint_stream_quiesce_for_test(
            MIN_CARD_EDGE_TABLE,
            drain_id,
            before_lane.lifecycle_revision,
            actor,
        )
        .await
        .expect_err(
            "the under-min fresh source must publish a DataBlock when its dead-letter object overflows",
        )
    };
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
        .failpoint_stream_ingest_one_as_for_test(TABLE, &raw_json, 1, "agent:after-seal")
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
async fn sealed_resume_advances_epoch_replays_its_receipt_and_installs_the_writer() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let initial = stream_lane(&db).await;
    let quiesce_id = "1a1a1a1a-1a1a-4a1a-8a1a-1a1a1a1a1a1a";
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        quiesce_id,
        initial.lifecycle_revision,
        "operator:resume-fixture",
    )
    .await
    .expect("empty fixture lane must quiesce");
    let sealed = stream_lane(&db).await;
    assert_eq!(sealed.lifecycle, "SEALED");
    let resume_id = "2a2a2a2a-2a2a-4a2a-8a2a-2a2a2a2a2a2a";
    let actor = "operator:resume";

    db.failpoint_stream_resume_for_test(TABLE, resume_id, sealed.lifecycle_revision, false, actor)
        .await
        .expect("SEALED lane must resume through one recovery-owned claim");
    let open = stream_lane(&db).await;
    assert_eq!(open.lifecycle, "OPEN");
    assert_eq!(open.lifecycle_revision, sealed.lifecycle_revision + 1);
    assert!(
        epoch_floor(&open) > epoch_floor(&sealed),
        "resume must fence the sealed writer epoch before opening admission"
    );
    assert_no_recovery_sidecars(&dir);

    db.failpoint_stream_quiesce_for_test(
        TABLE,
        quiesce_id,
        initial.lifecycle_revision,
        "operator:resume-fixture",
    )
    .await
    .expect("the earlier quiesce receipt remains idempotent after a later resume");

    db.failpoint_stream_resume_for_test(TABLE, resume_id, sealed.lifecycle_revision, false, actor)
        .await
        .expect("an exact lost-result retry must replay the selected receipt");
    let rebound = db
        .failpoint_stream_resume_for_test(
            TABLE,
            resume_id,
            sealed.lifecycle_revision,
            false,
            "operator:different",
        )
        .await
        .expect_err("one resume occurrence cannot be rebound to another actor");
    assert!(
        matches!(
            rebound,
            OmniError::StreamLifecycleIdempotencyConflict { .. }
        ),
        "{rebound:?}"
    );
    let stale = db
        .failpoint_stream_resume_for_test(
            TABLE,
            "3a3a3a3a-3a3a-4a3a-8a3a-3a3a3a3a3a3a",
            sealed.lifecycle_revision,
            false,
            actor,
        )
        .await
        .expect_err("a new occurrence cannot reuse the stale sealed revision");
    assert!(matches!(stale, OmniError::StreamLifecycleChanged { .. }));

    let row = physical_batch(&db, &[("after-resume".to_string(), 41)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("the resume-owned resident writer must accept the next row");
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the resumed generation must fold normally");
    assert_eq!(
        visible_rows(&db).await,
        vec![("after-resume".to_string(), 41)]
    );
    let after_fold = stream_lane(&db).await;
    db.failpoint_stream_resume_for_test(TABLE, resume_id, sealed.lifecycle_revision, false, actor)
        .await
        .expect("the selected resume receipt remains idempotent after a later fold");
    assert_eq!(stream_lane(&db).await, after_fold);
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn frozen_driver_round_fences_resume_then_releases_its_empty_root_slot() {
    let _scenario = FailScenario::setup();
    let (dir, db, sealed) = init_f6b8_resume_driver_fixture().await;
    let rounds = TwoRoundRendezvous::new();
    db.start_stream_fold_driver()
        .await
        .expect("the checked runtime starts its finite-round driver");
    rounds.wait_for(0).await;

    let install = helpers::failpoint::Rendezvous::park_first(names::STREAM_RESUME_INSTALL_OWNER);
    let resume_db = Arc::clone(&db);
    let mut resume = tokio::spawn(async move {
        resume_db
            .failpoint_stream_resume_for_test(
                TABLE,
                "f6b80000-0000-4000-8000-000000000002",
                sealed.lifecycle_revision,
                false,
                "operator:f6b8-driver-first",
            )
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut resume)
            .await
            .is_err()
            && !install.reached(),
        "the frozen finite round must fence resume before detached installation"
    );

    rounds.release(0);
    install.wait_until_reached().await;
    install.release();
    tokio::time::timeout(Duration::from_secs(20), &mut resume)
        .await
        .expect("resume stayed fenced after the prior finite round")
        .expect("resume task must join")
        .expect("resume must install its empty writer");
    let open = stream_lane_for(&db, TABLE).await;
    assert_eq!(open.lifecycle, "OPEN");

    rounds.wait_for(1).await;
    rounds.release(1);
    let idle = wait_for_stream_fold_driver_idle(&db).await;
    assert_eq!(idle["last_error"], serde_json::Value::Null);

    let company =
        physical_score_batch(&db, COMPANY_TABLE, &[("after-resume-slot".to_string(), 8)]).await;
    db.failpoint_stream_b1_for_test(COMPANY_TABLE, Some(company), 8)
        .await
        .expect("the resumed empty writer must release the sole root slot");
    wait_for_visible_score_rows(&db, COMPANY_TABLE, &[("after-resume-slot".to_string(), 8)]).await;
    wait_for_published_open_fold(&db, 1).await;

    let body = ndjson_score_line(
        &open.stream_incarnation_id,
        "caller-shaped-after-resume",
        9,
        "f6b80000-0000-4000-8000-000000000003",
        None,
    );
    let outcomes = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_ndjson_as_for_test(TABLE, vec![body], "agent:f6b8-driver-first")
            .await
            .expect("caller-shaped ingress must cold-claim after empty retirement"),
    );
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0]["status"], "durable");
    wait_for_visible_rows(
        &db,
        &[
            ("Alice".to_string(), 1),
            ("caller-shaped-after-resume".to_string(), 9),
        ],
    )
    .await;
    let completed = wait_for_published_open_fold(&db, 2).await;
    assert_eq!(completed["last_error"], serde_json::Value::Null);

    db.shutdown_stream_fold_driver()
        .await
        .expect("the resumed runtime must join cleanly");
    let stopped = stream_fold_driver_status(&db);
    assert_process_local_driver_state(&stopped, "stopped");
    assert!(stopped["pending_triggers"].as_array().unwrap().is_empty());
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn resume_empty_housekeeping_precedes_lower_sorted_cold_lane_in_same_round() {
    let _scenario = FailScenario::setup();
    let (dir, db, sealed) = init_f6b8_resume_driver_fixture().await;
    let company = stream_lane_for(&db, COMPANY_TABLE).await;
    assert!(
        (company.stable_table_id, company.table_incarnation_id)
            < (sealed.stable_table_id, sealed.table_incarnation_id),
        "the cold Company lane must sort before the resumed Person lane"
    );

    let cold =
        physical_score_batch(&db, COMPANY_TABLE, &[("cold-before-resume".to_string(), 7)]).await;
    db.failpoint_stream_b1_for_test(COMPANY_TABLE, Some(cold), 0)
        .await
        .expect("the lower-sorted lane must own one durable unmerged tail");
    db.shutdown_stream_fold_driver()
        .await
        .expect("runtime shutdown must leave the durable tail cold");
    assert!(visible_score_rows(&db, COMPANY_TABLE).await.is_empty());

    let install = helpers::failpoint::Rendezvous::park_first(names::STREAM_RESUME_INSTALL_OWNER);
    let resume_db = Arc::clone(&db);
    let sealed_revision = sealed.lifecycle_revision;
    let mut resume = tokio::spawn(async move {
        resume_db
            .failpoint_stream_resume_for_test(
                TABLE,
                "f6b80000-0000-4000-8000-000000000006",
                sealed_revision,
                false,
                "operator:f6b8-same-round",
            )
            .await
    });
    install.wait_until_reached().await;

    let rounds = TwoRoundRendezvous::new();
    db.start_stream_fold_driver()
        .await
        .expect("cold discovery must queue the lower-sorted Company lane");
    install.release();
    tokio::time::timeout(Duration::from_secs(20), &mut resume)
        .await
        .expect("resume installation did not settle")
        .expect("resume task must join")
        .expect("Person resume must publish OPEN");

    rounds.wait_for(0).await;
    rounds.release(0);
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if visible_score_rows(&db, COMPANY_TABLE).await
                == vec![("cold-before-resume".to_string(), 7)]
            {
                return;
            }
            if rounds.reached[1].load(Ordering::SeqCst) {
                rounds.release(1);
                panic!("the lower-sorted cold lane required a retry round");
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first finite round did not publish the cold tail");
    rounds.release(1);

    let idle = wait_for_stream_fold_driver_idle(&db).await;
    assert!(idle["published_open_folds"].as_u64().unwrap() >= 1);
    assert_eq!(idle["last_error"], serde_json::Value::Null);
    db.shutdown_stream_fold_driver()
        .await
        .expect("the same-round handoff fixture must shut down cleanly");
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cancelled_resume_transfers_its_root_fence_until_driver_handoff_and_shutdown() {
    let _scenario = FailScenario::setup();
    let (dir, db, sealed) = init_f6b8_resume_driver_fixture().await;
    let sealed_revision = sealed.lifecycle_revision;
    let install = helpers::failpoint::Rendezvous::park_first(names::STREAM_RESUME_INSTALL_OWNER);
    let resume_db = Arc::clone(&db);
    let resume = tokio::spawn(async move {
        resume_db
            .failpoint_stream_resume_for_test(
                TABLE,
                "f6b80000-0000-4000-8000-000000000004",
                sealed_revision,
                false,
                "operator:f6b8-cancelled",
            )
            .await
    });
    install.wait_until_reached().await;
    resume.abort();
    assert!(
        resume
            .await
            .expect_err("the caller task must be cancelled")
            .is_cancelled(),
        "cancelling the caller must detach rather than cancel resume ownership"
    );

    let before_round =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_DRIVER_BEFORE_ROUND_ACQUIRE);
    let rounds = TwoRoundRendezvous::new();
    db.start_stream_fold_driver()
        .await
        .expect("the checked runtime starts with cold eligible lanes");
    before_round.wait_until_reached().await;
    before_round.release();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), rounds.wait_for(0))
            .await
            .is_err(),
        "the detached resume installer must retain the root fence"
    );

    let shutdown_db = Arc::clone(&db);
    let mut shutdown = tokio::spawn(async move { shutdown_db.shutdown_stream_fold_driver().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut shutdown)
            .await
            .is_err(),
        "shutdown must wait for the detached resume installation"
    );

    install.release();
    let open = wait_for_stream_lifecycle_for(&db, TABLE, "OPEN").await;
    assert_eq!(open.lifecycle_revision, sealed_revision + 1);
    rounds.wait_for(0).await;
    rounds.release(0);
    tokio::time::timeout(Duration::from_secs(30), &mut shutdown)
        .await
        .expect("shutdown did not join the resumed writer and driver")
        .expect("shutdown task must join")
        .expect("shutdown must settle every detached owner");

    let stopped = stream_fold_driver_status(&db);
    assert_process_local_driver_state(&stopped, "stopped");
    assert!(stopped["pending_triggers"].as_array().unwrap().is_empty());
    assert_eq!(stopped["last_error"], serde_json::Value::Null);
    assert_eq!(visible_rows(&db).await, vec![("Alice".to_string(), 1)]);
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_exact_resume_retry_does_not_retire_the_winners_writer() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let initial = stream_lane(&db).await;
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        "2b2b2b2b-2b2b-4b2b-8b2b-2b2b2b2b2b2b",
        initial.lifecycle_revision,
        "operator:resume-race-fixture",
    )
    .await
    .expect("race fixture lane must quiesce");
    let sealed = stream_lane(&db).await;
    let sealed_revision = sealed.lifecycle_revision;
    let resume_id = "2c2c2c2c-2c2c-4c2c-8c2c-2c2c2c2c2c2c";
    let actor = "operator:resume-race";
    let first_sidecar = helpers::failpoint::Rendezvous::park_first(names::RECOVERY_SIDECAR_WRITE);

    let first_db = Arc::clone(&db);
    let first = tokio::spawn(async move {
        first_db
            .failpoint_stream_resume_for_test(TABLE, resume_id, sealed_revision, false, actor)
            .await
    });
    first_sidecar.wait_until_reached().await;

    let second_db = Arc::clone(&db);
    let mut second = tokio::spawn(async move {
        second_db
            .failpoint_stream_resume_for_test(TABLE, resume_id, sealed_revision, false, actor)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut second)
            .await
            .is_err(),
        "the exact retry must wait outside the winner's exclusive resume"
    );

    first_sidecar.release();
    first
        .await
        .expect("winning resume task must join")
        .expect("winning resume must publish OPEN");
    second
        .await
        .expect("exact retry task must join")
        .expect("exact retry must replay the selected result");
    let open = stream_lane(&db).await;

    let row = physical_batch(&db, &[("resume-race-warm".to_string(), 42)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("the winner's resident writer must remain installed");
    assert_eq!(
        epoch_floor(&stream_lane(&db).await),
        epoch_floor(&open),
        "the waiting exact retry must not retire the winner and force another cold claim"
    );
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the winner's generation must remain foldable");
    assert_eq!(
        visible_rows(&db).await,
        vec![("resume-race-warm".to_string(), 42)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn resume_rolls_forward_a_confirmed_ledger_before_open_publication() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let initial = stream_lane(&db).await;
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        "9a9a9a9a-9a9a-4a9a-8a9a-9a9a9a9a9a9a",
        initial.lifecycle_revision,
        "operator:resume-recovery-fixture",
    )
    .await
    .expect("fixture lane must quiesce");
    let sealed = stream_lane(&db).await;
    let resume_id = "abababab-abab-4bab-8bab-abababababab";
    let actor = "operator:resume-recovery";

    let error = {
        let _before_publish =
            ScopedFailPoint::new(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH, "return");
        db.failpoint_stream_resume_for_test(
            TABLE,
            resume_id,
            sealed.lifecycle_revision,
            false,
            actor,
        )
        .await
        .expect_err("confirmed resume ledger must stop before the lifecycle CAS")
    };
    assert!(
        error
            .to_string()
            .contains(names::RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH),
        "{error:?}"
    );
    assert_eq!(stream_lane(&db).await, sealed);
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()).len(),
        1,
        "the exact resume owner must remain durable"
    );

    db.failpoint_stream_resume_for_test(TABLE, resume_id, sealed.lifecycle_revision, false, actor)
        .await
        .expect("the exact retry must publish the confirmed receipt and OPEN row");
    let open = stream_lane(&db).await;
    assert_eq!(open.lifecycle, "OPEN");
    assert!(epoch_floor(&open) > epoch_floor(&sealed));
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn resume_retries_a_lost_terminal_sidecar_delete() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let initial = stream_lane(&db).await;
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        "acacacac-acac-4cac-8cac-acacacacacac",
        initial.lifecycle_revision,
        "operator:resume-delete-fixture",
    )
    .await
    .expect("fixture lane must quiesce");
    let sealed = stream_lane(&db).await;
    let resume_id = "adadadad-adad-4dad-8dad-adadadadadad";
    let actor = "operator:resume-delete";

    {
        let _delete = ScopedFailPoint::new(names::RECOVERY_SIDECAR_DELETE, "return");
        db.failpoint_stream_resume_for_test(
            TABLE,
            resume_id,
            sealed.lifecycle_revision,
            false,
            actor,
        )
        .await
        .expect("cleanup failure after OPEN publication must not turn success into ambiguity");
        assert_eq!(stream_lane(&db).await.lifecycle, "OPEN");
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1
        );
    }

    db.failpoint_stream_resume_for_test(TABLE, resume_id, sealed.lifecycle_revision, false, actor)
        .await
        .expect("receipt-first retry must finish stale sidecar cleanup");
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn named_branch_keeps_a_sealed_lane_closed_without_claiming_an_epoch() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let initial = stream_lane(&db).await;
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        "4a4a4a4a-4a4a-4a4a-8a4a-4a4a4a4a4a4a",
        initial.lifecycle_revision,
        "operator:branch-fixture",
    )
    .await
    .expect("fixture lane must quiesce");
    let sealed = stream_lane(&db).await;
    db.branch_create("sealed-resume-blocker")
        .await
        .expect("SEALED authority permits the native branch control");

    let error = db
        .failpoint_stream_resume_for_test(
            TABLE,
            "5a5a5a5a-5a5a-4a5a-8a5a-5a5a5a5a5a5a",
            sealed.lifecycle_revision,
            false,
            "operator:branch-resume",
        )
        .await
        .expect_err("bounded resume must refuse a named graph branch");
    assert!(error.to_string().contains("main-only"), "{error:?}");
    assert_eq!(stream_lane(&db).await, sealed);
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn guarded_abort_reopens_only_an_empty_unblocked_drain() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let initial = stream_lane(&db).await;
    let drain_id = "6a6a6a6a-6a6a-4a6a-8a6a-6a6a6a6a6a6a";
    let quiesce_error = {
        let _before_claim = ScopedFailPoint::new(names::RECOVERY_SIDECAR_WRITE, "return");
        db.failpoint_stream_quiesce_for_test(
            TABLE,
            drain_id,
            initial.lifecycle_revision,
            "operator:abort-fixture",
        )
        .await
        .expect_err("fixture must stop after publishing DRAINING and before its claim")
    };
    assert!(
        quiesce_error
            .to_string()
            .contains(names::RECOVERY_SIDECAR_WRITE),
        "{quiesce_error:?}"
    );
    let draining = stream_lane(&db).await;
    assert_eq!(draining.lifecycle, "DRAINING");
    assert_eq!(draining.drain_id.as_deref(), Some(drain_id));
    assert_no_recovery_sidecars(&dir);

    let resume_id = "7a7a7a7a-7a7a-4a7a-8a7a-7a7a7a7a7a7a";
    let actor = "operator:abort";
    db.failpoint_stream_resume_for_test(TABLE, resume_id, draining.lifecycle_revision, true, actor)
        .await
        .expect("an empty unguarded drain may abort through a higher epoch");
    let open = stream_lane(&db).await;
    assert_eq!(open.lifecycle, "OPEN");
    assert_eq!(open.lifecycle_revision, draining.lifecycle_revision + 1);
    assert!(epoch_floor(&open) > epoch_floor(&draining));
    db.failpoint_stream_resume_for_test(TABLE, resume_id, draining.lifecycle_revision, true, actor)
        .await
        .expect("an exact abort retry must replay its receipt");
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn abort_refuses_an_unmerged_cut_before_claim_and_quiesce_can_continue() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let row = physical_batch(&db, &[("abort-must-not-strand".to_string(), 43)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .expect("fixture row must be durable in the active WAL generation");
    let before = stream_lane(&db).await;
    drop(db);

    // Reopen without a resident writer so the acknowledged row exists only
    // in retained, unflushed WAL. Cursor/generation scalars alone call this
    // state AdmitOrReplay; abort must still prove the WAL suffix is empty.
    let db = reopen_enrolled(&dir).await;

    let drain_id = "bababaab-baba-4aba-8aba-bab5bab5bab5";
    {
        let _before_claim = ScopedFailPoint::new(names::RECOVERY_SIDECAR_WRITE, "return");
        db.failpoint_stream_quiesce_for_test(
            TABLE,
            drain_id,
            before.lifecycle_revision,
            "operator:abort-nonempty-fixture",
        )
        .await
        .expect_err("fixture must stop after DRAINING but before its physical claim");
    }
    let draining = stream_lane(&db).await;
    assert_eq!(draining.lifecycle, "DRAINING");
    assert_no_recovery_sidecars(&dir);

    let error = db
        .failpoint_stream_resume_for_test(
            TABLE,
            "cacacaca-caca-4aca-8aca-cac5cac5cac5",
            draining.lifecycle_revision,
            true,
            "operator:abort-nonempty",
        )
        .await
        .expect_err("abort must reject an unmerged cut before claiming another epoch");
    assert!(
        error.to_string().contains("empty physical cut"),
        "{error:?}"
    );
    assert_eq!(stream_lane(&db).await, draining);
    assert_no_recovery_sidecars(&dir);

    db.failpoint_stream_quiesce_for_test(
        TABLE,
        drain_id,
        before.lifecycle_revision,
        "operator:abort-nonempty-fixture",
    )
    .await
    .expect("the original drain must remain able to fold and seal its cut");
    assert_eq!(stream_lane(&db).await.lifecycle, "SEALED");
    assert_eq!(
        visible_rows(&db).await,
        vec![("abort-must-not-strand".to_string(), 43)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn quiesce_continues_the_same_drain_after_crashing_before_its_claim() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
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

    db.start_stream_fold_driver()
        .await
        .expect("startup must discover and continue the durable DRAINING descriptor");
    wait_for_stream_lifecycle(&db, "SEALED").await;
    db.shutdown_stream_fold_driver()
        .await
        .expect("the continued drain must join cleanly");
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

    let cluster_uri = dir.cluster_uri();
    let reopened = reopen_enrolled(&dir).await;
    let reopened =
        helpers::stream_authority::bind_checked_stream_runtime(reopened, &cluster_uri).await;
    reopened
        .start_stream_fold_driver()
        .await
        .expect("restart discovery must continue the exact flushed generation");
    let terminal = wait_for_stream_lifecycle(&reopened, "SEALED").await;
    assert_eq!(
        epoch_floor(&terminal),
        epoch_floor(&failed),
        "a proven flushed cut must not mint another writer claim"
    );
    assert_eq!(
        visible_rows(&reopened).await,
        vec![("sealed-before-fold".to_string(), 31)]
    );
    reopened
        .shutdown_stream_fold_driver()
        .await
        .expect("the cold continued drain must join cleanly");
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

#[test]
#[serial]
fn legacy_writers_refuse_without_cluster_runtime_while_enabled_or_disabling() {
    on_big_stack(|| async {
        let _scenario = FailScenario::setup();
        let (dir, db) = init_enrolled().await;

        assert_legacy_writers_require_cluster_runtime(&dir, &db, "ENABLED").await;

        let row = physical_batch(&db, &[("legacy-writer-disable-cut".to_string(), 41)]).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
            .await
            .expect("the disable fixture row must be durable before the first drain attempt");
        let cluster_uri = dir.cluster_uri();
        let first_disable = {
            let _pre_sidecar =
                ScopedFailPoint::new(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR, "return");
            try_disable_stream_profile(&db, &cluster_uri)
                .await
                .expect_err("the first disable must stop after entering DISABLING")
        };
        assert!(
            first_disable
                .to_string()
                .contains(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR),
            "{first_disable:?}"
        );
        let interrupted = db.stream_status().await.unwrap();
        assert_eq!(interrupted.profile_mode, "DISABLING");
        assert_eq!(interrupted.tables[0].lifecycle, "DRAINING");
        assert_no_recovery_sidecars(&dir);

        assert_legacy_writers_require_cluster_runtime(&dir, &db, "DISABLING").await;

        try_disable_stream_profile(&db, &cluster_uri)
            .await
            .expect("retry must finish the interrupted disable");
        let terminal = db.stream_status().await.unwrap();
        assert_eq!(terminal.profile_mode, "DISABLED");
        assert_eq!(terminal.tables[0].lifecycle, "SEALED");
        assert_eq!(
            visible_rows(&db).await,
            vec![("legacy-writer-disable-cut".to_string(), 41)]
        );
        assert_no_recovery_sidecars(&dir);
    });
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
async fn whole_generation_caps_refuse_then_the_resident_driver_folds_for_retry() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let full = (0..8_192)
        .map(|row| (format!("p{row:04}"), row))
        .collect::<Vec<_>>();
    let full_batch = physical_batch(&db, &full).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(full_batch), 0)
        .await
        .expect("the exact row cap is admissible");

    let error = b2_put_score(
        &db,
        &incarnation,
        "over",
        9_999,
        8_192,
        "98989898-9898-4898-8898-989898989898",
        None,
        "agent:cap-retry",
    )
    .await
    .expect_err("the real B2 reserve path must refuse one row beyond the generation cap");
    assert!(matches!(error, OmniError::FoldRequired { .. }), "{error:?}");
    assert!(visible_rows(&db).await.is_empty());

    db.start_stream_fold_driver()
        .await
        .expect("cap pressure retained before startup must wake the driver");
    wait_for_visible_rows(&db, &full).await;

    b2_put_score(
        &db,
        &incarnation,
        "over",
        9_999,
        8_192,
        "98989898-9898-4898-8898-989898989898",
        None,
        "agent:cap-retry",
    )
    .await
    .expect("producer retry succeeds after the automatic cap fold");
    let mut expected = full;
    expected.push(("over".to_string(), 9_999));
    expected.sort();
    wait_for_visible_rows(&db, &expected).await;
    db.shutdown_stream_fold_driver().await.unwrap();
    drop(db);

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
async fn cancelling_request_before_invocation_keeps_shutdown_joined_to_the_detached_owner() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    db.start_stream_fold_driver().await.unwrap();
    let batch = physical_batch(&db, &[("pre-invocation-cancel".to_string(), 16)]).await;
    let before_prepare = helpers::failpoint::Rendezvous::park_first(
        names::STREAM_B1_AFTER_INPUT_QUEUE_BEFORE_PREPARE,
    );

    let put_db = Arc::clone(&db);
    let put = tokio::spawn(async move {
        put_db
            .failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
            .await
    });
    before_prepare.wait_until_reached().await;
    put.abort();
    assert!(put.await.unwrap_err().is_cancelled());

    let shutdown_db = Arc::clone(&db);
    let mut shutdown = tokio::spawn(async move { shutdown_db.shutdown_stream_fold_driver().await });
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut shutdown)
            .await
            .is_err(),
        "shutdown must remain joined to detached work that has not invoked Lance yet"
    );

    before_prepare.release();
    tokio::time::timeout(Duration::from_secs(20), shutdown)
        .await
        .expect("shutdown must settle after the detached owner and its automatic fold")
        .unwrap()
        .expect("detached pre-invocation cancellation remains foldable");
    assert_eq!(
        visible_rows(&db).await,
        vec![("pre-invocation-cancel".to_string(), 16)]
    );
    let status = stream_fold_driver_status(&db);
    assert_process_local_driver_state(&status, "stopped");
    assert!(status["pending_triggers"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn cancelling_request_after_invocation_preserves_worker_and_fold_trigger_ownership() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    db.start_stream_fold_driver().await.unwrap();
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

    tokio::time::sleep(Duration::from_millis(1_250)).await;
    assert!(
        visible_rows(&db).await.is_empty(),
        "automatic exclusive fold must remain blocked while the detached owner still holds the post-watcher fence boundary"
    );

    after_watcher.release();
    wait_for_visible_rows(&db, &[("cancelled-caller".to_string(), 17)]).await;
    db.shutdown_stream_fold_driver()
        .await
        .expect("automatic fold waits for the detached fence/ack owner and joins");
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
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
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

    let status = db
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect("status must observe the retained flushed generation without replaying it");
    let shards = match &status.tables[0].physical {
        StreamTablePhysicalOperationalStatus::Observed { shards } => shards,
        other => panic!("flushed generation must retain an observable binding: {other:?}"),
    };
    let shard = &shards[0];
    match shard.pending {
        StreamPendingGenerationStatus::UnavailableFlushed { generation, reason } => {
            assert_eq!(generation + 1, shard.current_generation);
            assert!(reason.contains("LWW winner projection"), "{reason}");
        }
        ref other => panic!(
            "a flushed LWW projection must not be mislabeled as exact admitted accounting: {other:?}"
        ),
    }
    assert!(status.rebuild.reasons.iter().any(|reason| matches!(
        reason,
        StreamRebuildBlockReason::PendingGenerationUnavailable { table_key }
            if table_key == TABLE
    )));

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

struct DeadLetterCrashFixture {
    dir: EnrolledGraphDir,
    db: Arc<Omnigraph>,
    incarnation: String,
    logical_id: String,
    write_id: String,
    terminal_token: String,
    manifest_version_before_fold: u64,
    table_version_before_fold: u64,
}

async fn prepare_all_diverted_dead_letter_crash_fixture(
    logical_id: &str,
    write_id: &str,
) -> DeadLetterCrashFixture {
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
    let cluster_uri = format!("file://{}", cluster.path().display());
    enable_stream_profile(&db, &cluster_uri).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let (terminal_token, already_durable) = authorized_ingest_score(
        &db,
        &incarnation,
        logical_id,
        7,
        0,
        write_id,
        None,
        "agent:dead-letter-crash",
    )
    .await
    .expect("base-dependent uniqueness must remain fold-time work");
    assert!(!already_durable);
    let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    DeadLetterCrashFixture {
        dir: EnrolledGraphDir {
            _cluster: cluster,
            graph,
        },
        db,
        incarnation,
        logical_id: logical_id.to_string(),
        write_id: write_id.to_string(),
        terminal_token,
        manifest_version_before_fold: snapshot.version(),
        table_version_before_fold: snapshot.entry(TABLE).unwrap().table_version,
    }
}

fn dead_letter_object_names(dir: &EnrolledGraphDir) -> Vec<String> {
    let object_dir = dir.path().join("__dead_letter/v1");
    if !object_dir.exists() {
        return Vec::new();
    }
    let mut names = std::fs::read_dir(object_dir)
        .unwrap()
        .map(|entry| {
            entry
                .unwrap()
                .file_name()
                .into_string()
                .expect("dead-letter object names are canonical ASCII ULIDs")
        })
        .collect::<Vec<_>>();
    names.sort();
    names
}

async fn assert_dead_letter_crash_boundary_converges(
    failpoint: &'static str,
    logical_id: &str,
    write_id: &str,
    object_exists_before_reopen: bool,
) {
    let fixture = prepare_all_diverted_dead_letter_crash_fixture(logical_id, write_id).await;
    let error = {
        let _failpoint = ScopedFailPoint::new(failpoint, "return");
        fixture
            .db
            .failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the selected dead-letter recovery occurrence must remain durable")
    };
    assert!(
        matches!(error, OmniError::RecoveryRequired { .. }),
        "{error:?}"
    );
    assert!(error.to_string().contains(failpoint), "{error:?}");
    assert!(
        visible_rows(&fixture.db)
            .await
            .iter()
            .all(|(id, _)| id != logical_id),
        "an unpublished losing occurrence must remain invisible"
    );

    let sidecar_ids = helpers::recovery::sidecar_operation_ids(fixture.dir.path());
    assert_eq!(sidecar_ids.len(), 1, "the v21 owner must remain armed");
    let operation_id = sidecar_ids[0].clone();
    let expected_object_name = format!("{operation_id}.ndjson");
    let expected_object_location = format!("__dead_letter/v1/{expected_object_name}");
    let expected_object_path = fixture.dir.path().join(&expected_object_location);
    let expected_objects = if object_exists_before_reopen {
        vec![expected_object_name.clone()]
    } else {
        Vec::new()
    };
    assert_eq!(
        dead_letter_object_names(&fixture.dir),
        expected_objects,
        "the failpoint must stop at its exact declared object boundary"
    );

    let exact_object_bytes = object_exists_before_reopen.then(|| {
        std::fs::read(&expected_object_path)
            .expect("the post-object failpoint must leave the exact descriptor-owned bytes")
    });

    let DeadLetterCrashFixture {
        dir,
        db,
        incarnation,
        logical_id,
        write_id,
        terminal_token,
        manifest_version_before_fold,
        table_version_before_fold,
    } = fixture;
    let cluster_uri = dir.cluster_uri();
    drop(db);

    if let Some(exact_object_bytes) = &exact_object_bytes {
        std::fs::write(&expected_object_path, b"{\"tampered\":true}\n").unwrap();
        let error = match Omnigraph::open(dir.path().to_str().unwrap()).await {
            Ok(_) => {
                panic!("effect-free recovery must still verify an existing exact-path object")
            }
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("dead-letter object"),
            "{error:?}"
        );
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()),
            [operation_id.clone()],
            "object corruption must fail closed without retiring its owner"
        );
        std::fs::write(&expected_object_path, exact_object_bytes).unwrap();
    }

    let effect_free =
        Arc::new(Omnigraph::open(dir.path().to_str().unwrap()).await.expect(
            "open-time recovery must verify residue and retire the effect-free v21 intent",
        ));
    assert!(
        helpers::recovery::sidecar_operation_ids(dir.path()).is_empty(),
        "effect-free recovery must retire the v21 sidecar"
    );
    let effect_free_snapshot = effect_free
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert_eq!(
        effect_free_snapshot.version(),
        manifest_version_before_fold,
        "an object alone is inert and effect-free recovery must not publish"
    );
    assert_eq!(
        effect_free_snapshot.entry(TABLE).unwrap().table_version,
        table_version_before_fold,
        "neither Lance participant moved, so recovery must not invent a base effect"
    );
    assert!(
        visible_rows(&effect_free)
            .await
            .iter()
            .all(|(id, _)| id != &logical_id),
        "effect-free recovery must not expose the losing occurrence"
    );
    assert_eq!(
        dead_letter_object_names(&dir),
        expected_objects,
        "effect-free recovery must retain existing object residue without creating any object"
    );

    let served =
        helpers::stream_authority::bind_checked_stream_runtime(effect_free, &cluster_uri).await;
    served
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the retained acknowledged generation must retry and publish one terminal cut");
    let recovered_snapshot = served
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap();
    assert!(
        recovered_snapshot.version() > manifest_version_before_fold,
        "the logical retry must publish its terminal graph cut; cold retry may first publish a higher-epoch claim"
    );
    assert_eq!(
        recovered_snapshot.entry(TABLE).unwrap().table_version,
        table_version_before_fold + 1,
        "all-diverted retry must publish the marker-only merged-generation effect"
    );
    assert!(
        visible_rows(&served)
            .await
            .iter()
            .all(|(id, _)| id != &logical_id),
        "the terminal occurrence must never become a base row"
    );
    drop(served);

    let reopened = Arc::new(
        Omnigraph::open(dir.path().to_str().unwrap())
            .await
            .expect("the recovered terminal cut must reopen cleanly"),
    );
    let dead_letters =
        helpers::stream_authority::list_stream_dead_letters(&reopened, &cluster_uri, None)
            .await
            .expect("offline inspection must observe recovered terminal authority");
    assert_eq!(dead_letters.entries.len(), 1);
    let terminal = &dead_letters.entries[0];
    assert_eq!(terminal.logical_id, logical_id);
    assert_eq!(terminal.occurrence_token, terminal_token);
    assert_ne!(
        terminal.object_location, expected_object_location,
        "an effect-free sidecar is retired; the logical retry owns a fresh selected descriptor"
    );
    let selected_object_name = Path::new(&terminal.object_location)
        .file_name()
        .and_then(|name| name.to_str())
        .expect("selected dead-letter location has one canonical object name")
        .to_string();
    let mut final_objects = vec![selected_object_name.clone()];
    if object_exists_before_reopen {
        final_objects.push(expected_object_name.clone());
        final_objects.sort();
    }
    assert_eq!(
        dead_letter_object_names(&dir),
        final_objects,
        "only one descriptor is selected; pre-publication object residue remains inert retain-all state"
    );

    let reopened =
        helpers::stream_authority::bind_checked_stream_runtime(reopened, &cluster_uri).await;
    let (retry_token, already_durable) = authorized_ingest_score(
        &reopened,
        &incarnation,
        &logical_id,
        7,
        1,
        &write_id,
        None,
        "agent:dead-letter-crash",
    )
    .await
    .expect("the exact occurrence must replay as the selected terminal authority");
    assert_eq!(retry_token, terminal_token);
    assert!(already_durable);
    assert_eq!(
        dead_letter_object_names(&dir),
        final_objects,
        "terminal replay must not create another object or terminal decision"
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn dead_letter_crash_after_sidecar_before_object_recovers_one_selected_object() {
    let _scenario = FailScenario::setup();
    assert_dead_letter_crash_boundary_converges(
        names::STREAM_DEAD_LETTER_POST_SIDECAR_PRE_OBJECT,
        "dead-letter-pre-object",
        "71717171-7171-4171-8171-717171717171",
        false,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn dead_letter_crash_after_object_before_base_verifies_residue_and_selects_one_terminal() {
    let _scenario = FailScenario::setup();
    assert_dead_letter_crash_boundary_converges(
        names::STREAM_DEAD_LETTER_POST_OBJECT_PRE_BASE_COMMIT,
        "dead-letter-post-object",
        "72727272-7272-4272-8272-727272727272",
        true,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn all_diverted_dead_letter_advances_marker_and_accepts_an_ordinary_successor() {
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
    let cluster_uri = format!("file://{}", cluster.path().display());
    enable_stream_profile(&db, &cluster_uri).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let dir = EnrolledGraphDir {
        _cluster: cluster,
        graph,
    };
    let incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    let logical_id = "duplicate-id";
    let write_id = "91919191-9191-4191-8191-919191919191";
    let (terminal_token, already_durable) = authorized_ingest_score(
        &db,
        &incarnation,
        logical_id,
        7,
        0,
        write_id,
        None,
        "agent:dead-letter",
    )
    .await
    .expect("base-dependent uniqueness is fold-time work");
    assert!(!already_durable);
    let snapshot_before_fold = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert!(
        dead_letter_object_names(&dir).is_empty(),
        "the all-diverted fixture must begin without dead-letter object residue"
    );
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the losing occurrence must publish terminal authority");
    let snapshot_after_fold = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(
        snapshot_after_fold.version(),
        snapshot_before_fold.version() + 1,
        "an all-diverted fold still publishes one graph cut"
    );
    assert_eq!(
        snapshot_after_fold.entry(TABLE).unwrap().table_version,
        snapshot_before_fold.entry(TABLE).unwrap().table_version + 1,
        "the marker-only base transaction must advance Lance merged-generation authority"
    );
    let visible = visible_rows(&db).await;
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].1, 7);
    assert_ne!(visible[0].0, logical_id);

    let object_dir = dir.path().join("__dead_letter/v1");
    let object_count = std::fs::read_dir(&object_dir)
        .expect("dead-letter fold must create its canonical object directory")
        .count();
    assert_eq!(object_count, 1);

    let (retry_token, retry_already_durable) = authorized_ingest_score(
        &db,
        &incarnation,
        logical_id,
        7,
        1,
        write_id,
        None,
        "agent:dead-letter",
    )
    .await
    .expect("the exact losing occurrence must remain idempotently terminal");
    assert_eq!(retry_token, terminal_token);
    assert!(retry_already_durable);
    assert_eq!(
        std::fs::read_dir(&object_dir).unwrap().count(),
        object_count,
        "an exact retry must not create another dead-letter object"
    );

    let successor_write_id = "92929292-9292-4292-8292-929292929292";
    let (successor_token, successor_already_durable) = authorized_ingest_score(
        &db,
        &incarnation,
        logical_id,
        8,
        2,
        successor_write_id,
        Some(&terminal_token),
        "agent:dead-letter",
    )
    .await
    .expect("a corrected ordinary occurrence may name the terminal token as predecessor");
    assert!(!successor_already_durable);
    assert_ne!(successor_token, terminal_token);
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the corrected successor must fold through the ordinary path");
    let visible = visible_rows(&db).await;
    assert_eq!(visible.len(), 2);
    assert!(
        visible
            .iter()
            .any(|(id, score)| id == logical_id && *score == 8)
    );

    let stale = authorized_ingest_score(
        &db,
        &incarnation,
        logical_id,
        7,
        3,
        write_id,
        None,
        "agent:dead-letter",
    )
    .await
    .expect_err("the superseded terminal occurrence cannot resurrect the key");
    assert!(
        matches!(
            stale,
            OmniError::StreamSequenceConflict {
                ref logical_id,
                current_token: Some(ref current),
                ..
            } if logical_id == "duplicate-id" && current == &successor_token
        ),
        "{stale:?}"
    );
    assert_eq!(
        std::fs::read_dir(&object_dir).unwrap().count(),
        object_count,
        "ordinary correction preserves immutable dead-letter residue without another object"
    );

    let (mixed_loser_token, _) = authorized_ingest_score(
        &db,
        &incarnation,
        "mixed-loser",
        7,
        4,
        "93939393-9393-4393-8393-939393939393",
        None,
        "agent:dead-letter",
    )
    .await
    .unwrap();
    let (mixed_winner_token, _) = authorized_ingest_score(
        &db,
        &incarnation,
        "mixed-winner",
        9,
        5,
        "94949494-9494-4494-8494-949494949494",
        None,
        "agent:dead-letter",
    )
    .await
    .unwrap();
    assert_ne!(mixed_loser_token, mixed_winner_token);
    let mixed_before = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("a mixed fold must publish its independent winner and terminal loser together");
    let mixed_after = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
    assert_eq!(mixed_after.version(), mixed_before.version() + 1);
    assert_eq!(
        mixed_after.entry(TABLE).unwrap().table_version,
        mixed_before.entry(TABLE).unwrap().table_version + 1
    );
    let visible = visible_rows(&db).await;
    assert!(
        visible
            .iter()
            .any(|(id, score)| id == "mixed-winner" && *score == 9)
    );
    assert!(visible.iter().all(|(id, _)| id != "mixed-loser"));
    assert_eq!(
        std::fs::read_dir(&object_dir).unwrap().count(),
        object_count + 1,
        "one mixed generation creates exactly one additional fold-owned object"
    );
    assert_no_recovery_sidecars(&dir);
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
    assert_eq!(current_incarnation.as_deref(), Some(incarnation.as_str()));

    let mut altered_witness: serde_json::Value =
        serde_json::from_str(&witness).expect("the opaque witness is valid JSON");
    altered_witness["current_head"]["transaction_uuid"] =
        serde_json::json!("34343434-3434-4434-8434-343434343434");
    let altered_witness =
        serde_json::to_string(&altered_witness).expect("the altered witness remains valid JSON");
    let intent_rebind = prepare_stream_ingest(&db, request_id, Some(&altered_witness), "agent:a")
        .await
        .expect_err("one request id cannot be rebound to another enrollment intent");
    assert!(
        intent_rebind
            .to_string()
            .contains("different enrollment intent")
    );

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
        let _must_not_enter_recovery = ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
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
            .failpoint_stream_ingest_one_as_for_test(TABLE, b"{", 1, "act-denied")
            .await
            .expect_err("Cedar must reject an actor without stream_ingest");
        assert!(matches!(denied, OmniError::Policy(_)), "{denied:?}");
        let denied_graph_polled = Arc::new(AtomicBool::new(false));
        let denied_graph = db
            .start_served_graph_stream_ingest_as(
                "act-denied",
                None,
                graph_body_poll_probe(Arc::clone(&denied_graph_polled)),
            )
            .await
            .expect_err("served graph ingress must authorize before challenging or polling");
        assert!(
            matches!(denied_graph, OmniError::Policy(_)),
            "{denied_graph:?}"
        );
        assert!(!denied_graph_polled.load(Ordering::SeqCst));

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
            .failpoint_stream_ingest_one_as_for_test(TABLE, b"{", 2, "act-allowed")
            .await
            .expect_err("an enabled ambient handle is not a serving runtime");
        assert!(
            matches!(
                no_runtime,
                OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "ENABLED"
            ),
            "{no_runtime:?}"
        );
        let no_runtime_graph_polled = Arc::new(AtomicBool::new(false));
        let no_runtime_graph = db
            .start_served_graph_stream_ingest_as(
                "act-allowed",
                None,
                graph_body_poll_probe(Arc::clone(&no_runtime_graph_polled)),
            )
            .await
            .expect_err("ambient graph ingress cannot mint a served challenge");
        assert!(
            matches!(
                no_runtime_graph,
                OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "ENABLED"
            ),
            "{no_runtime_graph:?}"
        );
        assert!(!no_runtime_graph_polled.load(Ordering::SeqCst));
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
async fn served_graph_ndjson_challenges_before_body_and_streams_only_redacted_lines() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_unenrolled_served_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
    let manifest_before = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .unwrap()
        .version();

    let challenge_polled = Arc::new(AtomicBool::new(false));
    let authority_token = match db
        .start_served_graph_stream_ingest_as(
            "agent:served-graph",
            None,
            graph_body_poll_probe(Arc::clone(&challenge_polled)),
        )
        .await
        .expect("checked served ingest returns an authority challenge")
    {
        GraphStreamIngestStart::TokenRequired { authority_token } => authority_token,
        GraphStreamIngestStart::Ready(_) => panic!("a missing token cannot start body polling"),
    };
    assert!(!challenge_polled.load(Ordering::SeqCst));
    assert_eq!(authority_token.len(), "sha256:".len() + 64);
    assert!(
        authority_token
            .strip_prefix("sha256:")
            .is_some_and(|digest| digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()))
    );
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before,
        "the challenge must not publish graph or enrollment state"
    );
    assert!(db.stream_status().await.unwrap().tables.is_empty());

    let repeated_polled = Arc::new(AtomicBool::new(false));
    let repeated = match db
        .start_served_graph_stream_ingest_as(
            "agent:served-graph",
            None,
            graph_body_poll_probe(Arc::clone(&repeated_polled)),
        )
        .await
        .expect("unchanged authority returns the same challenge")
    {
        GraphStreamIngestStart::TokenRequired { authority_token } => authority_token,
        GraphStreamIngestStart::Ready(_) => panic!("a repeated challenge cannot start a request"),
    };
    assert_eq!(repeated, authority_token);
    assert!(!repeated_polled.load(Ordering::SeqCst));

    for supplied in [
        "not-a-token".to_string(),
        format!("sha256:{}", "0".repeat(64)),
    ] {
        let refused_polled = Arc::new(AtomicBool::new(false));
        let error = db
            .start_served_graph_stream_ingest_as(
                "agent:served-graph",
                Some(&supplied),
                graph_body_poll_probe(Arc::clone(&refused_polled)),
            )
            .await
            .expect_err("malformed and stale tokens must refuse effect-free");
        assert!(
            matches!(error, OmniError::StreamingAuthorityMismatch { .. }),
            "{error:?}"
        );
        assert!(!refused_polled.load(Ordering::SeqCst));
        assert!(
            !error.to_string().contains(&authority_token),
            "a stale-token refusal must not disclose replacement authority"
        );
    }
    assert_eq!(
        db.snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .version(),
        manifest_before
    );
    assert!(db.stream_status().await.unwrap().tables.is_empty());

    let body: GraphStreamChunkSource = Box::pin(futures::stream::iter([Ok(graph_node_line(
        "Person",
        "served-person",
        17,
        "50505050-5050-4050-8050-505050505050",
        None,
    ))]));
    let mut handle = match db
        .start_served_graph_stream_ingest_as("agent:served-graph", Some(&authority_token), body)
        .await
        .expect("an exact graph token starts the existing request owner")
    {
        GraphStreamIngestStart::Ready(handle) => handle,
        GraphStreamIngestStart::TokenRequired { .. } => {
            panic!("the exact token cannot be rechallenged")
        }
    };
    let line = handle
        .recv()
        .await
        .expect("served result receive succeeds")
        .expect("one input produces one result");
    assert_eq!(line.last(), Some(&b'\n'));
    assert_eq!(line.iter().filter(|byte| **byte == b'\n').count(), 1);
    let outcome: serde_json::Value =
        serde_json::from_slice(&line[..line.len() - 1]).expect("result line is valid JSON");
    assert_eq!(outcome["status"], "durable");
    assert_eq!(outcome["scope"], "row");
    assert_eq!(outcome["kind"], "node");
    assert_eq!(outcome["type"], "Person");
    assert_eq!(outcome["id"], "served-person");
    assert_graph_stream_result_redacted(&outcome);
    assert!(handle.recv().await.unwrap().is_none());
}

#[tokio::test]
#[serial]
async fn served_graph_ndjson_flushes_one_complete_run_before_body_eof() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_unenrolled_served_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
    let challenge_body: GraphStreamChunkSource = Box::pin(futures::stream::pending());
    let authority_token = match db
        .start_served_graph_stream_ingest_as("agent:low-rate", None, challenge_body)
        .await
        .expect("checked served ingest returns an authority challenge")
    {
        GraphStreamIngestStart::TokenRequired { authority_token } => authority_token,
        GraphStreamIngestStart::Ready(_) => panic!("a missing token cannot start body polling"),
    };

    let (body_sender, body_receiver) =
        futures::channel::mpsc::unbounded::<Result<Vec<u8>, OmniError>>();
    body_sender
        .unbounded_send(Ok(graph_node_line(
            "Person",
            "low-rate-person",
            19,
            "53535353-5353-4353-8353-535353535353",
            None,
        )))
        .unwrap();
    let body: GraphStreamChunkSource = Box::pin(body_receiver);
    let mut handle = match db
        .start_served_graph_stream_ingest_as("agent:low-rate", Some(&authority_token), body)
        .await
        .expect("the exact graph token starts the request")
    {
        GraphStreamIngestStart::Ready(handle) => handle,
        GraphStreamIngestStart::TokenRequired { .. } => {
            panic!("the exact token cannot be rechallenged")
        }
    };

    // Keep `body_sender` alive: after delivering the complete line the source
    // is Pending, not EOF. The fixed coalescing boundary must submit the run.
    let line = tokio::time::timeout(Duration::from_secs(5), handle.recv())
        .await
        .expect("one low-rate graph row must acknowledge while the body remains pending")
        .expect("served result receive succeeds")
        .expect("one input produces one result");
    assert_eq!(line.last(), Some(&b'\n'));
    let outcome: serde_json::Value =
        serde_json::from_slice(&line[..line.len() - 1]).expect("result line is valid JSON");
    assert_eq!(outcome["status"], "durable");
    assert_eq!(outcome["id"], "low-rate-person");
    assert_graph_stream_result_redacted(&outcome);

    drop(body_sender);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), handle.recv())
            .await
            .expect("request reaches EOF after the pending body is closed")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[serial]
async fn graph_ndjson_authorizes_one_graph_not_each_lazy_lane() {
    struct GraphDecisionCounter {
        calls: AtomicUsize,
    }

    impl PolicyChecker for GraphDecisionCounter {
        fn check(
            &self,
            action: omnigraph_policy::PolicyAction,
            scope: &omnigraph_policy::ResourceScope,
            actor: &str,
        ) -> std::result::Result<(), omnigraph_policy::PolicyError> {
            assert_eq!(action, omnigraph_policy::PolicyAction::StreamIngest);
            assert_eq!(scope, &omnigraph_policy::ResourceScope::Graph);
            assert_eq!(actor, "agent:graph-policy");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    let _scenario = FailScenario::setup();
    let (_dir, db) = init_unenrolled_served_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
    let checker = Arc::new(GraphDecisionCounter {
        calls: AtomicUsize::new(0),
    });
    let policy: Arc<dyn PolicyChecker> = checker.clone();
    let db = match Arc::try_unwrap(db) {
        Ok(db) => Arc::new(db.with_policy(policy)),
        Err(_) => panic!("graph policy fixture must own the sole engine handle"),
    };
    let body = [
        graph_node_line(
            "Person",
            "policy-person",
            1,
            "40404040-4040-4040-8040-404040404040",
            None,
        ),
        graph_node_line(
            "Company",
            "policy-company",
            2,
            "40404040-4040-4040-8040-404040404041",
            None,
        ),
    ]
    .concat();

    let outcomes = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(vec![body], "agent:graph-policy")
            .await
            .expect("one graph-scoped decision authorizes both private lanes"),
    );
    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome["status"] == "durable"),
        "{outcomes:#?}"
    );
    assert_eq!(
        checker.calls.load(Ordering::SeqCst),
        1,
        "lazy per-declaration enrollment must not re-authorize physical tables"
    );
    let lanes = db
        .stream_status()
        .await
        .unwrap()
        .tables
        .into_iter()
        .map(|lane| lane.table_key)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        lanes,
        BTreeSet::from([TABLE.to_string(), COMPANY_TABLE.to_string()])
    );
}

#[tokio::test]
#[serial]
async fn graph_ndjson_alternates_node_edge_node_with_private_lazy_enrollment() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_unenrolled_served_with_schema(F6B2_NODE_EDGE_SCHEMA).await;
    let body = [
        graph_node_line(
            "Person",
            "graph-a",
            11,
            "41414141-4141-4141-8141-414141414141",
            None,
        ),
        graph_edge_line(
            "Knows",
            "graph-edge",
            "graph-a",
            "graph-a",
            "42424242-4242-4242-8242-424242424242",
            None,
        ),
        graph_node_line(
            "Person",
            "graph-b",
            12,
            "43434343-4343-4343-8343-434343434343",
            None,
        ),
    ]
    .concat();

    let outcomes = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(
            vec![body[..31].to_vec(), body[31..].to_vec()],
            "agent:graph-ingest",
        )
        .await
        .expect("one graph request must route alternating logical declarations"),
    );
    assert_eq!(outcomes.len(), 3);
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| outcome["status"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["durable", "durable", "durable"],
        "{outcomes:#?}"
    );
    assert_eq!(outcomes[0]["kind"], "node");
    assert_eq!(outcomes[0]["type"], "Person");
    assert_eq!(outcomes[0]["id"], "graph-a");
    assert_eq!(outcomes[1]["kind"], "edge");
    assert_eq!(outcomes[1]["type"], "Knows");
    assert_eq!(outcomes[1]["id"], "graph-edge");
    assert_eq!(outcomes[2]["kind"], "node");
    assert_eq!(outcomes[2]["id"], "graph-b");
    for (ordinal, outcome) in outcomes.iter().enumerate() {
        assert_eq!(outcome["ordinal"], ordinal as u64);
        assert_eq!(outcome["scope"], "row");
        assert!(outcome["stream_token"].as_str().is_some());
        assert_graph_stream_result_redacted(outcome);
    }

    let lanes = db
        .stream_status()
        .await
        .unwrap()
        .tables
        .into_iter()
        .map(|lane| lane.table_key)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        lanes,
        BTreeSet::from([TABLE.to_string(), MIN_CARD_EDGE_TABLE.to_string()]),
        "only declarations reached by valid rows are enrolled"
    );
    assert_eq!(
        visible_rows(&db).await,
        vec![("graph-a".to_string(), 11)],
        "switching away from the first node lane reuses the resident fold coordinator"
    );
    assert_eq!(
        visible_edge_rows(&db).await,
        vec![(
            "graph-edge".to_string(),
            "graph-a".to_string(),
            "graph-a".to_string(),
        )],
        "switching back to a node lane settles the edge lane through the same coordinator"
    );

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the final resident node lane publishes its remaining occurrence");
    assert_eq!(
        visible_rows(&db).await,
        vec![("graph-a".to_string(), 11), ("graph-b".to_string(), 12)]
    );
    assert_eq!(
        visible_edge_rows(&db).await,
        vec![(
            "graph-edge".to_string(),
            "graph-a".to_string(),
            "graph-a".to_string(),
        )]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn graph_ndjson_handoff_uses_the_driver_node_before_edge_round() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_f6b2_node_edge_fixture().await;
    let cluster_uri = dir.cluster_uri();

    let node_owner = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let bob = physical_batch(&node_owner, &[("Bob".to_string(), 2)]).await;
    node_owner
        .failpoint_stream_b1_for_test(TABLE, Some(bob), 0)
        .await
        .expect("Bob is durably acknowledged before the first runtime retires");
    node_owner.shutdown_stream_fold_driver().await.unwrap();
    drop(node_owner);

    let db = reopen_enrolled(&dir).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    assert_eq!(visible_rows(&db).await, vec![("Alice".to_string(), 1)]);
    let startup =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_DRIVER_POST_DISCOVERY_PRE_NOTIFY);
    let start_db = Arc::clone(&db);
    let start = tokio::spawn(async move { start_db.start_stream_fold_driver().await });
    startup.wait_until_reached().await;
    let body = [
        graph_edge_line(
            "Knows",
            "graph-dependent-edge",
            "Alice",
            "Bob",
            "48484848-4848-4848-8848-484848484848",
            None,
        ),
        graph_node_line(
            "Person",
            "Carol",
            3,
            "49494949-4949-4949-8949-494949494949",
            None,
        ),
    ]
    .concat();

    let outcomes = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(vec![body], "agent:graph-driver-round")
            .await
            .expect("the graph handoff reuses one finite driver round"),
    );
    assert_eq!(outcomes.len(), 2);
    let visible_after_handoff = visible_rows(&db).await;
    let visible_edges_after_handoff = visible_edge_rows(&db).await;
    let driver = stream_fold_driver_status(&db);
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome["status"] == "durable"),
        "outcomes={outcomes:#?}\nvisible={visible_after_handoff:#?}\nedges={visible_edges_after_handoff:#?}\ndriver={driver:#?}"
    );
    assert_eq!(
        visible_after_handoff,
        vec![("Alice".to_string(), 1), ("Bob".to_string(), 2)],
        "the cold acknowledged node must publish before the resident dependent edge"
    );
    assert_eq!(
        visible_edges_after_handoff,
        vec![(
            "graph-dependent-edge".to_string(),
            "Alice".to_string(),
            "Bob".to_string(),
        )]
    );
    assert_eq!(driver["published_open_folds"], 2);
    assert_eq!(driver["last_completion"]["outcome"], "published_open_fold");
    assert_eq!(driver["last_error"], serde_json::Value::Null);

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the final resident node lane publishes Carol");
    assert_eq!(
        visible_rows(&db).await,
        vec![
            ("Alice".to_string(), 1),
            ("Bob".to_string(), 2),
            ("Carol".to_string(), 3),
        ]
    );
    startup.release();
    start
        .await
        .expect("the parked driver startup task must join")
        .expect("driver startup must finish after installing every discovered trigger");
    db.shutdown_stream_fold_driver()
        .await
        .expect("the startup-race fixture must stop its driver");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn graph_ndjson_releases_resident_before_folding_distinct_endpoint_lanes() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_f6b2_node_edge_fixture().await;
    let cluster_uri = dir.cluster_uri();

    let company_owner =
        helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let cold_company =
        physical_score_batch(&company_owner, COMPANY_TABLE, &[("ColdCo".to_string(), 7)]).await;
    company_owner
        .failpoint_stream_b1_for_test(COMPANY_TABLE, Some(cold_company), 0)
        .await
        .expect("the Company endpoint is acknowledged before process restart");
    company_owner.shutdown_stream_fold_driver().await.unwrap();
    drop(company_owner);

    let db = reopen_enrolled(&dir).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let first = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(
            vec![graph_edge_line(
                "Knows",
                "resident-knows",
                "Alice",
                "Alice",
                "50505050-5050-4050-8050-505050505050",
                None,
            )],
            "agent:graph-resident-first",
        )
        .await
        .expect("the first request leaves one productive edge lane resident"),
    );
    assert_eq!(first[0]["status"], "durable");
    assert!(
        visible_score_rows(&db, COMPANY_TABLE).await.is_empty(),
        "the unrelated cold Company endpoint must remain with its own trigger"
    );

    let node_switch = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(
            vec![graph_node_line(
                "Company",
                "FreshCo",
                8,
                "51515151-5151-4151-8151-515151515151",
                None,
            )],
            "agent:graph-resident-first",
        )
        .await
        .expect("a fresh node-first request releases a foreign resident edge"),
    );
    assert_eq!(node_switch[0]["status"], "durable", "{node_switch:#?}");
    assert_eq!(
        visible_edge_rows(&db).await,
        vec![(
            "resident-knows".to_string(),
            "Alice".to_string(),
            "Alice".to_string(),
        )],
        "the foreign edge resident must publish before Company becomes resident"
    );
    assert_eq!(
        visible_score_rows(&db, COMPANY_TABLE).await,
        vec![("ColdCo".to_string(), 7)],
        "the cold target tail must publish before the new Company occurrence is admitted"
    );

    let second = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(
            vec![graph_edge_line(
                "WorksAt",
                "resident-then-endpoints",
                "FreshCo",
                "Alice",
                "52525252-5252-4252-8252-525252525252",
                None,
            )],
            "agent:graph-resident-first",
        )
        .await
        .expect("the second request releases the resident before its two endpoint lanes"),
    );
    assert_eq!(second[0]["status"], "durable", "{second:#?}");
    assert_eq!(
        visible_score_rows(&db, COMPANY_TABLE).await,
        vec![("ColdCo".to_string(), 7), ("FreshCo".to_string(), 8)],
        "the Company target and its cold predecessor must publish before WorksAt admission"
    );
    assert_eq!(
        visible_edge_rows(&db).await,
        vec![(
            "resident-knows".to_string(),
            "Alice".to_string(),
            "Alice".to_string(),
        )],
        "the prior resident edge must publish before the dependency fold"
    );

    db.failpoint_stream_b1_for_test(WORKS_AT_EDGE_TABLE, None, 0)
        .await
        .expect("the final heterogeneous edge lane folds cleanly");
    assert_eq!(helpers::count_rows(&db, WORKS_AT_EDGE_TABLE).await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn graph_ndjson_reports_empty_owner_cleanup_before_retrying_the_target() {
    let _scenario = FailScenario::setup();
    let (dir, db, sealed) = init_f6b8_resume_driver_fixture().await;
    db.failpoint_stream_resume_for_test(
        TABLE,
        "f6b80000-0000-4000-8000-000000000008",
        sealed.lifecycle_revision,
        false,
        "operator:graph-cleanup-failure",
    )
    .await
    .expect("the fixture installs one empty foreign resident");

    let line = graph_node_line(
        "Company",
        "AfterCleanup",
        9,
        "53535353-5353-4353-8353-535353535353",
        None,
    );
    let first = {
        let _cleanup = ScopedFailPoint::new(names::STREAM_DRIVER_EMPTY_OWNER_CLEANUP, "return");
        parse_ndjson_outcomes(
            db.failpoint_stream_ingest_graph_ndjson_as_for_test(
                vec![line.clone()],
                "agent:graph-cleanup-failure",
            )
            .await
            .expect("the cleanup failure is a logical line outcome"),
        )
    };
    assert_eq!(first[0]["status"], "stream_authority_changed", "{first:#?}");
    assert!(
        visible_score_rows(&db, COMPANY_TABLE).await.is_empty(),
        "the target lane must not be physically invoked after cleanup fails"
    );
    let failed = stream_fold_driver_status(&db);
    let failed_triggers = failed["pending_triggers"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|trigger| trigger["consecutive_failures"] == 1)
        .count();
    assert_eq!(failed_triggers, 1, "{failed:#?}");
    assert!(
        failed["last_error"]["message"]
            .as_str()
            .unwrap()
            .contains(names::STREAM_DRIVER_EMPTY_OWNER_CLEANUP),
        "the driver must retain the actual cleanup failure: {failed:#?}"
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    let retry = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(
            vec![line],
            "agent:graph-cleanup-failure",
        )
        .await
        .expect("the exact logical retry proceeds after cleanup backoff"),
    );
    assert_eq!(retry[0]["status"], "durable", "{retry:#?}");
    db.failpoint_stream_b1_for_test(COMPANY_TABLE, None, 0)
        .await
        .expect("the target lane folds after the successful retry");
    assert_eq!(
        visible_score_rows(&db, COMPANY_TABLE).await,
        vec![("AfterCleanup".to_string(), 9)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn graph_ndjson_ignores_an_unrelated_nonresident_retry_backoff() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_f6b2_node_edge_fixture().await;
    let cluster_uri = dir.cluster_uri();
    let owner = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let cold = physical_score_batch(&owner, COMPANY_TABLE, &[("ColdBackoff".to_string(), 7)]).await;
    owner
        .failpoint_stream_b1_for_test(COMPANY_TABLE, Some(cold), 0)
        .await
        .expect("the unrelated Company tail is acknowledged before restart");
    owner.shutdown_stream_fold_driver().await.unwrap();
    drop(owner);

    let db = reopen_enrolled(&dir).await;
    let db = helpers::stream_authority::bind_checked_stream_runtime(db, &cluster_uri).await;
    let failed = {
        let _attempt = ScopedFailPoint::new(names::STREAM_DRIVER_CANDIDATE_ATTEMPT, "return");
        parse_ndjson_outcomes(
            db.failpoint_stream_ingest_graph_ndjson_as_for_test(
                vec![graph_node_line(
                    "Company",
                    "NotInvoked",
                    8,
                    "54545454-5454-4454-8454-545454545454",
                    None,
                )],
                "agent:graph-unrelated-backoff",
            )
            .await
            .expect("the injected target failure is a logical line outcome"),
        )
    };
    assert_eq!(
        failed[0]["status"], "stream_authority_changed",
        "{failed:#?}"
    );
    let before_status = stream_fold_driver_status(&db);
    let before = before_status["pending_triggers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|trigger| trigger["consecutive_failures"] == 1)
        .expect("the Company driver attempt owns one retry deadline")
        .clone();

    let unrelated = parse_ndjson_outcomes(
        db.failpoint_stream_ingest_graph_ndjson_as_for_test(
            vec![graph_node_line(
                "Person",
                "Bob",
                2,
                "55555555-5555-4555-8555-555555555555",
                None,
            )],
            "agent:graph-unrelated-backoff",
        )
        .await
        .expect("an unrelated node route must not run the failed Company lane"),
    );
    assert_eq!(unrelated[0]["status"], "durable", "{unrelated:#?}");
    assert!(
        visible_score_rows(&db, COMPANY_TABLE).await.is_empty(),
        "the unrelated request must not fold the backed-off Company tail"
    );
    let after_status = stream_fold_driver_status(&db);
    let after = after_status["pending_triggers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|trigger| trigger["table_identity"] == before["table_identity"])
        .expect("the unrelated request must preserve the Company trigger");
    assert_eq!(after["trigger_sequence"], before["trigger_sequence"]);
    assert_eq!(after["consecutive_failures"], 1);
    assert_eq!(after_status["last_error"], before_status["last_error"]);

    db.failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the unrelated Person occurrence folds normally");
    assert_eq!(
        visible_rows(&db).await,
        vec![("Alice".to_string(), 1), ("Bob".to_string(), 2)]
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn graph_ndjson_invalid_and_blob_rows_do_not_enroll_private_lanes() {
    let _scenario = FailScenario::setup();
    let (_dir, malformed_db) = init_unenrolled_served_with_schema(F6B2_NODE_EDGE_SCHEMA).await;
    let mut malformed = serde_json::to_vec(&serde_json::json!({
        "type": "Company",
        "data": {"id": "missing-score"},
        "$stream": {
            "write_id": "44444444-4444-4444-8444-444444444444",
            "predecessor_token": null,
        },
    }))
    .unwrap();
    malformed.push(b'\n');
    let malformed_outcomes = parse_ndjson_outcomes(
        malformed_db
            .failpoint_stream_ingest_graph_ndjson_as_for_test(
                vec![malformed],
                "agent:graph-invalid",
            )
            .await
            .expect("schema-invalid graph rows are line-local"),
    );
    assert_eq!(malformed_outcomes[0]["status"], "invalid");
    assert_eq!(malformed_outcomes[0]["scope"], "row");
    assert_graph_stream_result_redacted(&malformed_outcomes[0]);
    assert!(
        malformed_db
            .stream_status()
            .await
            .unwrap()
            .tables
            .is_empty()
    );

    let (_dir, blob_db) = init_unenrolled_served_with_schema(BLOB_STREAM_SCHEMA).await;
    let blob_outcomes = parse_ndjson_outcomes(
        blob_db
            .failpoint_stream_ingest_graph_ndjson_as_for_test(
                vec![graph_node_line(
                    "Person",
                    "blob-row",
                    7,
                    "45454545-4545-4545-8545-454545454545",
                    None,
                )],
                "agent:graph-blob",
            )
            .await
            .expect("unsupported Blob declarations are line-local"),
    );
    assert_eq!(blob_outcomes[0]["status"], "invalid");
    assert_eq!(blob_outcomes[0]["scope"], "row");
    assert_graph_stream_result_redacted(&blob_outcomes[0]);
    assert!(blob_db.stream_status().await.unwrap().tables.is_empty());
}

#[tokio::test]
#[serial]
async fn graph_ndjson_ack_unknown_blocks_later_declarations_without_enrollment() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_unenrolled_served_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
    let body = [
        graph_node_line(
            "Person",
            "ambiguous-person",
            31,
            "46464646-4646-4646-8646-464646464646",
            None,
        ),
        graph_node_line(
            "Company",
            "must-not-enroll",
            32,
            "47474747-4747-4747-8747-474747474747",
            None,
        ),
    ]
    .concat();
    let outcomes = {
        let _after_durable = ScopedFailPoint::new(names::STREAM_B1_AFTER_WATCHER_SUCCESS, "return");
        parse_ndjson_outcomes(
            db.failpoint_stream_ingest_graph_ndjson_as_for_test(
                vec![body],
                "agent:graph-ambiguous",
            )
            .await
            .expect("AckUnknown is an ordered graph blocker"),
        )
    };
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0]["status"], "ack_unknown");
    assert_eq!(outcomes[0]["scope"], "graph");
    assert!(
        outcomes[0]["unconfirmed_candidate_token"]
            .as_str()
            .is_some()
    );
    assert_eq!(outcomes[1]["status"], "stream_retry_required");
    assert_eq!(outcomes[1]["scope"], "graph");
    assert_eq!(outcomes[1]["blocking_ordinal"], 0);
    assert_eq!(outcomes[1]["blocking_status"], "ack_unknown");
    for outcome in &outcomes {
        assert_graph_stream_result_redacted(outcome);
    }

    let lanes = db.stream_status().await.unwrap().tables;
    assert_eq!(lanes.len(), 1);
    assert_eq!(lanes[0].table_key, TABLE);
    assert!(
        lanes.iter().all(|lane| lane.table_key != COMPANY_TABLE),
        "a graph-wide blocker must prevent lazy enrollment of later declarations"
    );
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
    let _must_not_enter_recovery = ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
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
    let structurally_amplified =
        format!(r#"{{"$stream":{envelope_json},"id":"strict-row","score":[{many_values}]}}"#);
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
        .failpoint_stream_ingest_one_as_for_test(TABLE, &oversized, 99, "agent:strict")
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
        inventory_after.referenced_generation_roots, inventory_before.referenced_generation_roots,
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
        let _must_not_enter_recovery = ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
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
        .failpoint_stream_ingest_one_as_for_test(MIN_CARD_EDGE_TABLE, &raw_json, 0, "agent:edge")
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
    let _must_not_enter_recovery = ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
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
    let schema_owner = Arc::new(
        Omnigraph::init(graph.to_str().unwrap(), STREAM_SCHEMA)
            .await
            .unwrap(),
    );
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
    let _must_not_enter_recovery = ScopedFailPoint::new(names::RECOVERY_SIDECAR_LIST, "return");
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

#[tokio::test]
#[serial]
async fn checked_operational_status_reports_one_coherent_read_only_physical_cut() {
    let _scenario = FailScenario::setup();
    let (_dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let stream_incarnation = db
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    authorized_ingest_score(
        &db,
        &stream_incarnation,
        "status-pending",
        7,
        1,
        "74747474-7474-4474-8474-747474747474",
        None,
        "agent:status",
    )
    .await
    .unwrap();
    let manifest_before = db.version_of(ReadTarget::branch("main")).await.unwrap();

    let status = db
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(status.cut_manifest_version, manifest_before);
    assert_eq!(status.durable.profile_mode, "ENABLED");
    assert_eq!(status.tables.len(), 1);
    let table = &status.tables[0];
    assert_eq!(table.durable.table_key, TABLE);
    assert_eq!(table.durable.lifecycle, "OPEN");
    let shards = match &table.physical {
        StreamTablePhysicalOperationalStatus::Observed { shards } => shards,
        other => panic!("resident generation must have an observed physical cut: {other:?}"),
    };
    assert_eq!(shards.len(), 1);
    let shard = &shards[0];
    assert!(shard.observed_writer_epoch >= shard.authoritative_epoch_floor);
    assert_eq!(shard.current_generation, shard.base_merged_generation + 1);
    match shard.pending {
        StreamPendingGenerationStatus::Exact {
            generation,
            rows,
            arrow_bytes,
            batches,
            source,
        } => {
            assert_eq!(generation, shard.current_generation);
            assert_eq!(rows, 1);
            assert!(arrow_bytes > 0);
            assert_eq!(batches, 1);
            assert_eq!(source, "RESIDENT_ADMIT");
        }
        ref other => panic!("resident pending generation must be exact: {other:?}"),
    }
    assert_eq!(status.token_ledger.present_count, 0);
    assert_eq!(status.token_ledger.withdrawn_count, 0);
    assert_eq!(status.token_ledger.dead_lettered_count, 0);
    assert!(status.token_ledger.terminal_sample.is_empty());
    assert!(!status.token_ledger.terminal_sample_has_more);
    match &status.token_ledger.lookup_index_coverage {
        StreamTokenIndexCoverageStatus::Known {
            total_fragments,
            covered_fragments,
            uncovered_fragments,
        } => assert_eq!(covered_fragments + uncovered_fragments, *total_fragments),
        StreamTokenIndexCoverageStatus::Unknown { reason, .. } => {
            assert!(!reason.is_empty(), "unknown coverage must stay explained")
        }
        other => panic!("unexpected token-index coverage status: {other:?}"),
    }
    assert!(!status.driver.authoritative);
    assert_eq!(status.driver.scope, "PROCESS_LOCAL_ADVISORY");
    assert!(!status.rebuild.ready);
    assert!(status.rebuild.reasons.iter().any(|reason| matches!(
        reason,
        StreamRebuildBlockReason::ProfileNotTerminal { mode } if *mode == "ENABLED"
    )));
    assert!(status.rebuild.reasons.iter().any(|reason| matches!(
        reason,
        StreamRebuildBlockReason::PendingGeneration { table_key } if table_key == TABLE
    )));
    assert_eq!(
        db.version_of(ReadTarget::branch("main")).await.unwrap(),
        manifest_before,
        "status must not publish a graph-manifest version"
    );
    let repeated = db
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(repeated.cut_manifest_version, status.cut_manifest_version);
    assert_eq!(repeated.durable, status.durable);
    assert_eq!(repeated.tables, status.tables);
    assert_eq!(repeated.token_ledger, status.token_ledger);
    assert_eq!(repeated.relevant_recovery, status.relevant_recovery);
    assert_eq!(repeated.rebuild, status.rebuild);
    assert_eq!(repeated.driver.state, status.driver.state);
    assert_eq!(
        repeated.driver.pending_triggers.len(),
        status.driver.pending_triggers.len(),
        "the advisory due-in countdown may move, but status must not add driver work"
    );

    let graph_status = db
        .capture_served_graph_stream_status()
        .await
        .expect("the served bridge must reuse the same checked status owner");
    assert_eq!(graph_status.manifest_version, manifest_before);
    assert_eq!(graph_status.profile_mode, "ENABLED");
    assert_eq!(
        graph_status.profile_revision,
        status.durable.profile_revision
    );
    assert_eq!(graph_status.enrolled_declarations.len(), 1);
    let declaration = &graph_status.enrolled_declarations[0];
    assert_eq!(declaration.declaration.kind, "node");
    assert_eq!(declaration.declaration.type_name, "Person");
    assert_eq!(declaration.lifecycle, "OPEN");
    assert_eq!(
        declaration.lifecycle_revision,
        status.tables[0].durable.lifecycle_revision
    );
    assert!(declaration.drain.is_none());
    assert!(declaration.strict_block.is_none());
    assert!(declaration.last_fold.is_none());
    assert!(matches!(
        declaration.pending,
        GraphStreamPendingStatus::Exact {
            rows: 1,
            arrow_bytes,
            batches: 1,
        } if arrow_bytes > 0
    ));
    assert_eq!(graph_status.token_counts.present, 0);
    assert_eq!(graph_status.token_counts.withdrawn, 0);
    assert_eq!(graph_status.token_counts.dead_lettered, 0);
    assert_eq!(graph_status.recovery_pending_count, 0);
    assert_eq!(graph_status.driver.state, status.driver.state);
    assert_eq!(
        graph_status.driver.pending_count,
        u64::try_from(status.driver.pending_triggers.len()).unwrap()
    );
    assert!(!graph_status.rebuild.ready);
    assert!(
        graph_status
            .rebuild
            .blockers
            .iter()
            .any(|blocker| matches!(blocker, GraphStreamRebuildBlocker::ProfileNotTerminal))
    );
    assert!(graph_status.rebuild.blockers.iter().any(|blocker| matches!(
        blocker,
        GraphStreamRebuildBlocker::PendingWork { declaration }
            if declaration.kind == "node" && declaration.type_name == "Person"
    )));
    assert!(
        graph_status
            .rebuild
            .blockers
            .iter()
            .all(|blocker| !matches!(blocker, GraphStreamRebuildBlocker::RecoveryPending { .. })),
        "zero internal sidecars must remain zero graph-level recovery blockers"
    );
    assert_eq!(
        db.version_of(ReadTarget::branch("main")).await.unwrap(),
        manifest_before,
        "the served graph projection must remain read-only"
    );

    let busy = db
        .failpoint_stream_operational_status_for_test(Duration::ZERO)
        .await
        .unwrap_err();
    assert!(matches!(
        busy,
        OmniError::StreamStatusBusy { ref phase }
            if phase == "immutable rebuild preflight"
    ));
    assert_eq!(
        db.version_of(ReadTarget::branch("main")).await.unwrap(),
        manifest_before,
        "deadline refusal is effect-free"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn operational_status_times_out_only_the_blocked_authority_cut_and_cancels_cleanly() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let manifest_before = db.version_of(ReadTarget::branch("main")).await.unwrap();

    let admitted =
        helpers::failpoint::Rendezvous::park_first(names::STREAM_STATUS_POST_OBSERVATION_ADMISSION);
    let first_db = Arc::clone(&db);
    let first = tokio::spawn(async move {
        first_db
            .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
            .await
    });
    admitted.wait_until_reached().await;
    let overlapping = db
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect_err("a second root observation must refuse without waiting");
    assert!(matches!(
        overlapping,
        OmniError::StreamStatusBusy { ref phase }
            if phase == "another checked status observation is already in progress"
    ));
    admitted.release();
    first
        .await
        .expect("the admitted observation task must remain live")
        .expect("the admitted observation must complete after release");
    db.failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect("a completed observation must release its root slot");

    let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let blocker_db = Arc::clone(&db);
    let blocker = tokio::spawn(async move {
        blocker_db
            .failpoint_hold_stream_status_root_for_test(acquired_tx, release_rx)
            .await;
    });
    acquired_rx
        .await
        .expect("the deterministic root owner must close the status cut");

    let busy = db
        .failpoint_stream_operational_status_with_deadlines_for_test(
            Duration::from_secs(10),
            Duration::from_millis(50),
        )
        .await
        .expect_err("the deterministic root owner must hold the authority cut");
    assert!(matches!(
        busy,
        OmniError::StreamStatusBusy { ref phase }
            if phase == "exclusive authority cut"
    ));
    assert_eq!(
        db.version_of(ReadTarget::branch("main")).await.unwrap(),
        manifest_before,
        "timing out a waiting status cut is effect-free"
    );

    release_tx
        .send(())
        .expect("the deterministic root owner must still be live");
    blocker.await.unwrap();
    db.failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect("the canceled cut must retain no gate or queued ownership");
    db.start_stream_fold_driver()
        .await
        .expect("the canceled cut must not prevent the resident driver from starting");
    let batch = physical_batch(&db, &[("status-after-timeout".to_string(), 32)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .expect("ordinary admission must reacquire authority after cut cancellation");
    wait_for_visible_rows(&db, &[("status-after-timeout".to_string(), 32)]).await;
    db.shutdown_stream_fold_driver().await.unwrap();
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn operational_status_terminal_sample_is_the_first_eight_current_keys_and_marks_more() {
    let _scenario = FailScenario::setup();
    let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let rows = (0..9)
        .map(|ordinal| (format!("status-terminal-{ordinal:02}"), ordinal))
        .collect::<Vec<_>>();
    let batch = physical_batch(&served, &rows).await;
    served
        .failpoint_stream_b1_for_test(TABLE, Some(batch), 0)
        .await
        .expect("the terminal-sample fixture must acknowledge one bounded generation");
    served
        .failpoint_stream_b1_for_test(TABLE, None, 0)
        .await
        .expect("the terminal-sample fixture must publish its base/token cut");
    let folded = stream_lane(&served).await;
    served
        .failpoint_stream_quiesce_for_test(
            TABLE,
            "a0a0a0a0-a0a0-40a0-80a0-a0a0a0a0a0a0",
            folded.lifecycle_revision,
            "operator:status-terminal-sample",
        )
        .await
        .expect("the terminal-sample fixture must seal cleanly");
    drop(served);

    let offline = reopen_enrolled(&dir).await;
    let cluster_uri = dir.cluster_uri();
    disable_stream_profile(&offline, &cluster_uri).await;
    for ordinal in 0..9 {
        offline
            .failpoint_withdraw_stream_token_for_retirement_test(
                TABLE,
                &format!("status-terminal-{ordinal:02}"),
                &format!("b0000000-0000-4000-8000-{ordinal:012}"),
            )
            .await
            .expect("each current PRESENT token must become terminal authority");
    }

    let checked = bind_checked_served_export(reopen_enrolled(&dir).await, &cluster_uri).await;
    let status = checked
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect("checked terminal status must retain only its bounded sample");
    assert_eq!(status.token_ledger.withdrawn_count, 9);
    assert_eq!(status.token_ledger.dead_lettered_count, 0);
    assert_eq!(status.token_ledger.terminal_sample.len(), 8);
    assert!(status.token_ledger.terminal_sample_has_more);
    assert_eq!(
        status
            .token_ledger
            .terminal_sample
            .iter()
            .map(|entry| entry.logical_id.clone())
            .collect::<Vec<_>>(),
        (0..8)
            .map(|ordinal| format!("status-terminal-{ordinal:02}"))
            .collect::<Vec<_>>()
    );
    assert_no_recovery_sidecars(&dir);
}

#[tokio::test]
#[serial]
async fn operational_status_marks_unopened_durable_wal_as_cold_replay_unavailable() {
    let _scenario = FailScenario::setup();
    let (dir, served) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let stream_incarnation = served
        .failpoint_stream_incarnation_for_test(TABLE)
        .await
        .unwrap();
    authorized_ingest_score(
        &served,
        &stream_incarnation,
        "status-cold-replay",
        29,
        1,
        "76767676-7676-4676-8676-767676767676",
        None,
        "agent:status",
    )
    .await
    .unwrap();
    served.shutdown_stream_fold_driver().await.unwrap();
    drop(served);

    let reopened = helpers::stream_authority::bind_checked_stream_runtime(
        reopen_enrolled(&dir).await,
        &dir.cluster_uri(),
    )
    .await;
    let status = reopened
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .unwrap();
    let shards = match &status.tables[0].physical {
        StreamTablePhysicalOperationalStatus::Observed { shards } => shards,
        other => panic!("cold replay must retain an observable physical cut: {other:?}"),
    };
    assert!(matches!(
        shards[0].pending,
        StreamPendingGenerationStatus::UnavailableColdReplay { .. }
    ));
    assert!(status.rebuild.reasons.iter().any(|reason| matches!(
        reason,
        StreamRebuildBlockReason::PendingGenerationUnavailable { table_key }
            if table_key == TABLE
    )));
    reopened.shutdown_stream_fold_driver().await.unwrap();
}

#[tokio::test]
#[serial]
async fn full_operational_status_requires_the_checked_serving_owner() {
    let _scenario = FailScenario::setup();
    let (_dir, ambient) = init_enrolled().await;
    let error = ambient
        .failpoint_stream_operational_status_for_test(Duration::from_secs(1))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OmniError::StreamingRequiresClusterRuntime { ref mode } if mode == "ENABLED"
    ));
    assert_eq!(
        ambient.stream_status().await.unwrap().profile_mode,
        "ENABLED"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn disabling_operational_status_uses_the_checked_offline_apply_owner() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled().await;
    let row = physical_batch(&db, &[("status-disabling".to_string(), 19)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .unwrap();
    let cluster_uri = dir.cluster_uri();
    let error = {
        let _after_drain =
            ScopedFailPoint::new(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR, "return");
        try_disable_stream_profile(&db, &cluster_uri)
            .await
            .expect_err("fixture must stop after persisting DISABLING and its lane drain")
    };
    assert!(
        error
            .to_string()
            .contains(names::STREAM_FOLD_POST_DRAIN_PRE_SIDECAR),
        "{error:?}"
    );
    assert_no_recovery_sidecars(&dir);

    let status = helpers::stream_authority::disabling_operational_status(&db, &cluster_uri)
        .await
        .unwrap();
    assert_eq!(status.durable.profile_mode, "DISABLING");
    assert_eq!(status.tables.len(), 1);
    let table = &status.tables[0];
    assert_eq!(table.durable.lifecycle, "DRAINING");
    let drain = table
        .drain
        .as_ref()
        .expect("disable owns one durable drain");
    assert_eq!(drain.goal, "SEALED");
    assert_eq!(drain.phase, "DRAINING");
    let shards = match &table.physical {
        StreamTablePhysicalOperationalStatus::Observed { shards } => shards,
        other => panic!("stable disabling cut must remain observable: {other:?}"),
    };
    assert!(matches!(
        shards[0].pending,
        StreamPendingGenerationStatus::UnavailableFlushed { .. }
    ));
    assert!(status.rebuild.reasons.iter().any(|reason| matches!(
        reason,
        StreamRebuildBlockReason::ProfileNotTerminal { mode } if *mode == "DISABLING"
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn operational_status_reports_recovery_before_and_after_its_base_effect() {
    let _scenario = FailScenario::setup();
    for (failpoint, physical_unavailable) in [
        (names::STREAM_FOLD_POST_SIDECAR_PRE_BASE_COMMIT, false),
        (names::STREAM_FOLD_POST_BASE_COMMIT_PRE_TOKEN_COMMIT, true),
    ] {
        let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
        let row = physical_batch(&db, &[(format!("status-recovery-{failpoint}"), 23)]).await;
        db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
            .await
            .unwrap();
        let error = {
            let _boundary = ScopedFailPoint::new(failpoint, "return");
            db.failpoint_stream_b1_for_test(TABLE, None, 0)
                .await
                .expect_err("fold boundary must retain one recovery owner")
        };
        assert!(matches!(error, OmniError::RecoveryRequired { .. }));
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1
        );
        let manifest_before = db.version_of(ReadTarget::branch("main")).await.unwrap();

        let status = db
            .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(status.relevant_recovery.len(), 1);
        let operation_id = status.relevant_recovery[0].operation_id.clone();
        assert!(status.rebuild.reasons.iter().any(|reason| matches!(
            reason,
            StreamRebuildBlockReason::RelevantRecovery { operation_id: blocked }
                if blocked == &operation_id
        )));
        match (&status.tables[0].physical, physical_unavailable) {
            (
                StreamTablePhysicalOperationalStatus::UnavailableRecovery {
                    operation_ids,
                    reason,
                },
                true,
            ) => {
                assert_eq!(operation_ids, &[operation_id]);
                assert!(!reason.is_empty());
            }
            (StreamTablePhysicalOperationalStatus::Observed { .. }, false) => {}
            (other, expected) => panic!(
                "unexpected recovery physical status at {failpoint}: {other:?}, unavailable={expected}"
            ),
        }
        assert_eq!(
            db.version_of(ReadTarget::branch("main")).await.unwrap(),
            manifest_before,
            "status must not resolve or publish the retained recovery owner"
        );
        assert_eq!(
            helpers::recovery::sidecar_operation_ids(dir.path()).len(),
            1,
            "status must leave the sidecar untouched"
        );
        drop(db);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn operational_status_refuses_unowned_base_head_movement() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(STREAM_SCHEMA).await;
    let row = physical_batch(&db, &[("status-unowned-head".to_string(), 37)]).await;
    db.failpoint_stream_b1_for_test(TABLE, Some(row), 0)
        .await
        .unwrap();
    let error = {
        let _boundary = ScopedFailPoint::new(
            names::STREAM_FOLD_POST_BASE_COMMIT_PRE_TOKEN_COMMIT,
            "return",
        );
        db.failpoint_stream_b1_for_test(TABLE, None, 0)
            .await
            .expect_err("the base participant must move under retained recovery")
    };
    assert!(matches!(error, OmniError::RecoveryRequired { .. }));
    let operation_id = helpers::recovery::single_sidecar_operation_id(dir.path());
    std::fs::remove_file(
        dir.path()
            .join("__recovery")
            .join(format!("{operation_id}.json")),
    )
    .unwrap();

    let error = db
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect_err("movement without its exact recovery owner must never become a status cut");
    assert!(matches!(
        error,
        OmniError::StreamStatusChanged { ref member }
            if member.contains("base table HEAD")
    ));
    db.shutdown_stream_fold_driver().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn operational_status_reports_and_rebuild_blocks_on_named_branch_mutation_recovery() {
    let _scenario = FailScenario::setup();
    let (dir, db) = init_enrolled_served_with_schema(TWO_TABLE_STREAM_SCHEMA).await;
    let open = stream_lane(&db).await;
    db.failpoint_stream_quiesce_for_test(
        TABLE,
        "75757575-7575-4575-8575-757575757575",
        open.lifecycle_revision,
        "operator:status-recovery",
    )
    .await
    .expect("the empty enrolled lane must seal before named-branch creation");
    const RECOVERY_BRANCH: &str = "status-recovery";
    db.branch_create(RECOVERY_BRANCH)
        .await
        .expect("SEALED stream authority permits the named recovery fixture branch");

    // Leave one ordinary Mutation recovery owner on the unenrolled sibling
    // table of a named branch. The checked stream cut must inventory every
    // graph writer, not only stream protocols, canonical-main sidecars, or
    // sidecars overlapping an enrolled lane.
    let mutation_error = {
        let _after_effect = ScopedFailPoint::new(names::MUTATION_POST_TABLE_COMMIT, "return");
        db.mutate(
            RECOVERY_BRANCH,
            INSERT_COMPANY,
            "insert_company",
            &helpers::int_params(&[("$score", 31)]),
        )
        .await
        .expect_err("ordinary mutation must retain its post-effect recovery owner")
    };
    let operation_id = match mutation_error {
        OmniError::RecoveryRequired { operation_id, .. } => operation_id,
        other => panic!("expected ordinary Mutation recovery, got {other:?}"),
    };
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![operation_id.clone()]
    );
    let manifest_before = db.version_of(ReadTarget::branch("main")).await.unwrap();

    let status = db
        .failpoint_stream_operational_status_for_test(Duration::from_secs(10))
        .await
        .expect("checked status must report ordinary recovery without resolving it");
    assert_eq!(status.relevant_recovery.len(), 1);
    let recovery = &status.relevant_recovery[0];
    assert_eq!(recovery.operation_id, operation_id);
    assert_eq!(recovery.operation_kind, "MUTATION");
    assert_eq!(recovery.schema_version, 9);
    assert_eq!(recovery.branch.as_deref(), Some(RECOVERY_BRANCH));
    assert!(recovery.state_digest.starts_with("sha256:"));
    assert!(status.rebuild.reasons.iter().any(|reason| matches!(
        reason,
        StreamRebuildBlockReason::RelevantRecovery { operation_id: blocked }
            if blocked == &operation_id
    )));
    assert!(
        matches!(
            status.tables[0].physical,
            StreamTablePhysicalOperationalStatus::Observed { .. }
        ),
        "an ordinary sidecar on the unenrolled sibling blocks rebuild without falsifying the enrolled lane's physical cut"
    );
    assert_eq!(
        db.version_of(ReadTarget::branch("main")).await.unwrap(),
        manifest_before,
        "status must not publish while reporting ordinary recovery"
    );
    assert_eq!(
        helpers::recovery::sidecar_operation_ids(dir.path()),
        vec![operation_id],
        "status must leave the ordinary sidecar untouched"
    );
}
