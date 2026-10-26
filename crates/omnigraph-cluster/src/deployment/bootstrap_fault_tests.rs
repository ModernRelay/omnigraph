//! The in-memory backend supplies real atomic create/CAS semantics. This test
//! adapter records the actual requests and injects lost acknowledgements only
//! after the backend committed them; no production failpoint substitutes for
//! an uncertain storage response.
use super::*;
use omnigraph_storage::{
    ListDirBounds, ObjectStorageAdapter, Result as StorageResult, StorageAdapter,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Notify};

const URI_PREFIX: &str = "s3://bootstrap-tests/";
const ROOT: &str = super::tests::ROOT;

#[derive(Clone, Debug)]
struct Write {
    uri: String,
    payload: String,
    expected_version: Option<String>,
    predecessor: Option<(String, String)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Point {
    LockCreate,
    StateCreate,
    StateAccepted,
    StateTerminal,
    Claim,
    Release,
    MaintenanceAccepted,
    MaintenanceInput,
    MaintenancePublication,
    MaintenanceSettlement,
    MaintenanceTerminal,
    MaintenanceRecoveryTerminal,
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    LostResponse,
    CancelAfterCommit,
}

#[derive(Debug)]
struct Controlled {
    inner: ObjectStorageAdapter,
    uri_prefix: Option<&'static str>,
    allow_fixture_setup: bool,
    writes: Mutex<Vec<Write>>,
    forbidden_calls: AtomicUsize,
    fault: Mutex<Option<(Point, Fault)>>,
    committed: Notify,
    claim_barrier: Mutex<Option<Arc<Barrier>>>,
    claim_attempts: AtomicUsize,
}

impl Controlled {
    fn new() -> Self {
        Self {
            inner: ObjectStorageAdapter::in_memory(),
            uri_prefix: Some(URI_PREFIX),
            allow_fixture_setup: false,
            writes: Mutex::default(),
            forbidden_calls: AtomicUsize::new(0),
            fault: Mutex::default(),
            committed: Notify::new(),
            claim_barrier: Mutex::default(),
            claim_attempts: AtomicUsize::new(0),
        }
    }

    fn key<'a>(&self, uri: &'a str) -> &'a str {
        match self.uri_prefix {
            Some(prefix) => uri.strip_prefix(prefix).expect("test-only S3 namespace"),
            None => uri,
        }
    }

    fn local() -> Self {
        Self {
            inner: ObjectStorageAdapter::local(),
            uri_prefix: None,
            ..Self::new()
        }
    }

    fn point(uri: &str, payload: &str, cas: bool) -> Option<Point> {
        if uri.ends_with("/__cluster/lock.json") {
            Some(
                if serde_json::from_str::<serde_json::Value>(payload).unwrap()["release_id"]
                    .is_string()
                {
                    Point::Release
                } else if cas {
                    Point::Claim
                } else {
                    Point::LockCreate
                },
            )
        } else if uri.ends_with("/__cluster/state.json") {
            if !cas {
                Some(Point::StateCreate)
            } else {
                match serde_json::from_str::<serde_json::Value>(payload).unwrap()["state_revision"]
                    .as_u64()
                {
                    Some(2) => Some(Point::StateAccepted),
                    Some(3) => Some(Point::StateTerminal),
                    _ => None,
                }
            }
        } else if uri.ends_with("/__cluster/maintenance.json") {
            let journal: serde_json::Value = serde_json::from_str(payload).unwrap();
            Some(if journal["outstanding"].is_null() {
                if journal["results"].as_array().unwrap().last().unwrap()["outcome"]
                    == "interrupted"
                {
                    Point::MaintenanceRecoveryTerminal
                } else {
                    Point::MaintenanceTerminal
                }
            } else if !journal["outstanding"]["settlement"].is_null() {
                Point::MaintenanceSettlement
            } else if !journal["outstanding"]["publication"].is_null() {
                Point::MaintenancePublication
            } else if journal["outstanding"]["input"].is_null() {
                Point::MaintenanceAccepted
            } else {
                Point::MaintenanceInput
            })
        } else {
            None
        }
    }

    async fn after_commit(&self, point: Option<Point>) -> StorageResult<()> {
        let fault = {
            let mut active = self.fault.lock().unwrap();
            if active.is_some_and(|(expected, _)| Some(expected) == point) {
                active.take().map(|(_, fault)| fault)
            } else {
                None
            }
        };
        match fault {
            Some(Fault::LostResponse) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "injected response lost after backend commit",
            )
            .into()),
            Some(Fault::CancelAfterCommit) => {
                self.committed.notify_one();
                std::future::pending::<StorageResult<()>>().await
            }
            None => Ok(()),
        }
    }

    fn forbid<T>(&self, operation: &str) -> StorageResult<T> {
        self.forbidden_calls.fetch_add(1, Ordering::SeqCst);
        Err(std::io::Error::other(format!(
            "forbidden bootstrap storage operation: {operation}"
        ))
        .into())
    }

    async fn read(&self, name: &str) -> String {
        self.read_text(&format!("{ROOT}/{name}")).await.unwrap()
    }

    fn arm(&self, point: Point, fault: Fault) {
        *self.fault.lock().unwrap() = Some((point, fault));
    }

    async fn assert_no_forbidden_calls(&self) {
        // A regression to asynchronous Drop cleanup must run before asserting
        // the DELETE trap. In-memory operations otherwise need never yield.
        tokio::task::yield_now().await;
        assert_eq!(self.forbidden_calls.load(Ordering::SeqCst), 0);
    }
}

fn is_lease_key(uri: &str) -> bool {
    uri.contains("/__cluster/lease/")
}

pub(super) fn memory_adapter() -> Arc<dyn StorageAdapter> {
    let mut adapter = Controlled::new();
    adapter.allow_fixture_setup = true;
    Arc::new(adapter)
}

#[async_trait::async_trait]
impl StorageAdapter for Controlled {
    async fn read_text(&self, uri: &str) -> StorageResult<String> {
        self.inner.read_text(self.key(uri)).await
    }
    async fn read_text_if_exists(&self, uri: &str) -> StorageResult<Option<String>> {
        self.inner.read_text_if_exists(self.key(uri)).await
    }
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> StorageResult<Option<String>> {
        self.inner
            .read_text_if_exists_bounded(self.key(uri), max_bytes)
            .await
    }
    async fn read_bytes_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> StorageResult<Option<Vec<u8>>> {
        self.inner
            .read_bytes_if_exists_bounded(self.key(uri), max_bytes)
            .await
    }
    async fn exists(&self, uri: &str) -> StorageResult<bool> {
        self.inner.exists(self.key(uri)).await
    }
    async fn read_text_versioned(&self, uri: &str) -> StorageResult<(String, String)> {
        self.inner.read_text_versioned(self.key(uri)).await
    }
    async fn read_text_versioned_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> StorageResult<Option<(String, String)>> {
        self.inner
            .read_text_versioned_if_exists_bounded(self.key(uri), max_bytes)
            .await
    }
    async fn list_dir(&self, uri: &str) -> StorageResult<Vec<String>> {
        self.inner.list_dir(self.key(uri)).await.map(|entries| {
            entries
                .into_iter()
                .map(|entry| match self.uri_prefix {
                    Some(prefix) => format!("{prefix}{entry}"),
                    None => entry,
                })
                .collect()
        })
    }
    async fn list_dir_bounded(
        &self,
        uri: &str,
        suffix: &str,
        bounds: ListDirBounds,
    ) -> StorageResult<Vec<String>> {
        self.inner
            .list_dir_bounded(self.key(uri), suffix, bounds)
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| match self.uri_prefix {
                        Some(prefix) => format!("{prefix}{entry}"),
                        None => entry,
                    })
                    .collect()
            })
    }
    async fn write_text(&self, uri: &str, contents: &str) -> StorageResult<()> {
        if self.allow_fixture_setup {
            self.inner.write_text(self.key(uri), contents).await
        } else {
            self.forbid("unconditional text PUT")
        }
    }
    async fn write_bytes(&self, uri: &str, contents: &[u8]) -> StorageResult<()> {
        if self.allow_fixture_setup {
            self.inner.write_bytes(self.key(uri), contents).await
        } else {
            self.forbid("unconditional bytes PUT")
        }
    }
    async fn rename_text(&self, _from: &str, _to: &str) -> StorageResult<()> {
        self.forbid("rename")
    }
    // The lease deletes only its owner's own heartbeats, keys no other
    // process writes; every other delete stays forbidden, so a delayed
    // delete can never reach the lock key a successor holds.
    async fn delete(&self, uri: &str) -> StorageResult<()> {
        if is_lease_key(uri) {
            self.inner.delete(self.key(uri)).await
        } else {
            self.forbid("DELETE")
        }
    }
    async fn delete_prefix(&self, uri: &str) -> StorageResult<()> {
        if is_lease_key(uri) {
            self.inner.delete_prefix(self.key(uri)).await
        } else {
            self.forbid("prefix DELETE")
        }
    }

    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> StorageResult<bool> {
        let created = self
            .inner
            .write_text_if_absent(self.key(uri), contents)
            .await?;
        if created {
            self.writes.lock().unwrap().push(Write {
                uri: uri.into(),
                payload: contents.into(),
                expected_version: None,
                predecessor: None,
            });
            self.after_commit(Self::point(uri, contents, false)).await?;
        }
        Ok(created)
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected: &str,
    ) -> StorageResult<Option<String>> {
        let predecessor = self.inner.read_text_versioned(self.key(uri)).await?;
        if uri.ends_with("/__cluster/lock.json") {
            self.claim_attempts.fetch_add(1, Ordering::SeqCst);
            let barrier = self.claim_barrier.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait().await;
            }
        }
        let version = self
            .inner
            .write_text_if_match(self.key(uri), contents, expected)
            .await?;
        if version.is_some() {
            assert_eq!(
                predecessor.1, expected,
                "record the real backend predecessor"
            );
            self.writes.lock().unwrap().push(Write {
                uri: uri.into(),
                payload: contents.into(),
                expected_version: Some(expected.into()),
                predecessor: Some(predecessor),
            });
            self.after_commit(Self::point(uri, contents, true)).await?;
        }
        Ok(version)
    }
}

fn fixture() -> (
    tempfile::TempDir,
    CapturedDeployment,
    ClusterStore,
    Arc<Controlled>,
) {
    let (directory, bundle, store, _) = super::tests::fixture();
    let adapter = Arc::new(Controlled::new());
    let store = store.with_bootstrap_test_adapter(adapter.clone());
    (directory, bundle, store, adapter)
}

#[tokio::test]
async fn maintenance_committed_journal_writes_do_not_replay_after_loss_or_cancellation() {
    use crate::{
        IdentityAuthorization, MaintenanceAcceptance, MaintenanceCompletion, MaintenanceLookup,
        accept_maintenance, bind_maintenance_input, bind_maintenance_publication,
        complete_maintenance, maintenance_lookup,
    };
    use omnigraph::db::OptimizeInput;

    for point in [
        Point::MaintenanceAccepted,
        Point::MaintenanceInput,
        Point::MaintenancePublication,
        Point::MaintenanceTerminal,
    ] {
        for fault in [Fault::LostResponse, Fault::CancelAfterCommit] {
            // Reuse the real applied graph/policy fixture and the existing
            // fault adapter. Only the adapter's post-commit response is lost;
            // this test does not claim cloud settlement or graph execution.
            let (directory, admission) = crate::maintenance::tests::fixture().await;
            admission.release_after_settlement().await.unwrap();
            let adapter = Arc::new(Controlled::local());
            let store = ClusterStore::for_storage_root(directory.path().to_str().unwrap())
                .unwrap()
                .with_bootstrap_test_adapter(adapter.clone());
            let admission =
                crate::admission::acquire_with_store(&store, crate::ClusterAdmissionPurpose::Serve)
                    .await
                    .unwrap()
                    .unwrap();
            let actor = IdentityAuthorization::authenticated("principal:owner").unwrap();
            let request = crate::maintenance::tests::request(&admission).await;
            adapter.arm(point, fault);
            {
                let invoke = async {
                    let MaintenanceAcceptance::Accepted(mut accepted) =
                        accept_maintenance(&admission, request.clone(), &actor).await?
                    else {
                        panic!("fresh request was already recorded")
                    };
                    bind_maintenance_input(
                        &admission,
                        &mut accepted,
                        OptimizeInput {
                            before_commit: "retained-input".into(),
                            report_max_bytes: 16384,
                            schema_contract: omnigraph::db::SchemaContractDigest {
                                source_hash: "source".into(),
                                schema_ir_hash: "ir".into(),
                                schema_identity_domain: "domain".into(),
                                schema_identity_version: 1,
                            },
                            source_tables: 1,
                            source_fragments: 1,
                            source_rows: 1,
                            source_bytes: 128,
                            source_files: 1,
                        },
                    )
                    .await?;
                    if point == Point::MaintenancePublication {
                        bind_maintenance_publication(
                            &admission,
                            &mut accepted,
                            omnigraph::db::OptimizePublication {
                                before_manifest_version: 1,
                                intent_nonce: ulid::Ulid::new().to_string(),
                                schema_contract: omnigraph::db::SchemaContractDigest {
                                    source_hash: "source".into(),
                                    schema_ir_hash: "ir".into(),
                                    schema_identity_domain: "domain".into(),
                                    schema_identity_version: 1,
                                },
                                actor: Some(actor.actor().into()),
                                created_at: 1,
                                report: omnigraph::db::OptimizeReport {
                                    before_commit: "retained-input".into(),
                                    after_commit: "retained-input".into(),
                                    tables: Vec::new(),
                                    source_tables: 1,
                                    source_fragments: 1,
                                    source_rows: 1,
                                    source_bytes: 128,
                                    source_files: 1,
                                },
                            },
                        )
                        .await?;
                    }
                    complete_maintenance(
                        &admission,
                        accepted,
                        MaintenanceCompletion::Refused {
                            code: "fixture_preflight_refusal".into(),
                            message: "no graph effect was attempted".into(),
                        },
                    )
                    .await
                };
                match fault {
                    Fault::LostResponse => {
                        let error = invoke.await.unwrap_err();
                        assert_eq!(error.code, "maintenance_write_uncertain", "{point:?}");
                    }
                    Fault::CancelAfterCommit => {
                        tokio::pin!(invoke);
                        tokio::time::timeout(std::time::Duration::from_secs(5), async {
                            tokio::select! {
                                _ = adapter.committed.notified() => {}
                                result = &mut invoke => panic!("expected committed blocked write: {result:?}"),
                            }
                        })
                        .await
                        .expect("real journal write must commit before cancellation");
                    }
                }
            }
            assert!(adapter.fault.lock().unwrap().is_none());
            let retained =
                maintenance_lookup(&admission, "knowledge", &request.operation_id, &actor)
                    .await
                    .unwrap();
            match (&retained, point) {
                (MaintenanceLookup::Complete { .. }, Point::MaintenanceTerminal) => {}
                (MaintenanceLookup::Outstanding { invocation, .. }, Point::MaintenanceAccepted) => {
                    assert!(invocation.input.is_none());
                }
                (MaintenanceLookup::Outstanding { invocation, .. }, Point::MaintenanceInput) => {
                    assert!(invocation.input.is_some());
                }
                (
                    MaintenanceLookup::Outstanding { invocation, .. },
                    Point::MaintenancePublication,
                ) => {
                    assert!(invocation.publication.is_some());
                    assert!(invocation.settlement.is_none());
                }
                _ => panic!("wrong retained state after {point:?}: {retained:?}"),
            }
            let writes = adapter.writes.lock().unwrap().len();
            let MaintenanceAcceptance::Existing(existing) =
                accept_maintenance(&admission, request, &actor)
                    .await
                    .unwrap()
            else {
                panic!("lost response or cancellation must never mint an execution token")
            };
            assert_eq!(
                serde_json::to_value(existing).unwrap(),
                serde_json::to_value(retained).unwrap()
            );
            assert_eq!(adapter.writes.lock().unwrap().len(), writes);
            drop(admission);
            adapter.assert_no_forbidden_calls().await;
        }
    }
}

#[tokio::test]
async fn maintenance_recovery_journal_loss_keeps_the_original_settlement_and_result() {
    use crate::{
        ClusterAdmissionPurpose, IdentityAuthorization, MaintenanceAcceptance,
        MaintenanceCompletion, MaintenancePublication, accept_maintenance, bind_maintenance_input,
        bind_maintenance_publication, reconcile_maintenance,
    };
    use omnigraph::db::{
        Omnigraph, OptimizeInput, OptimizePublication, OptimizeReport, ReadTarget,
    };

    for point in [
        Point::MaintenanceSettlement,
        Point::MaintenanceRecoveryTerminal,
    ] {
        for fault in [Fault::LostResponse, Fault::CancelAfterCommit] {
            let (directory, admission) = crate::maintenance::tests::fixture().await;
            admission.release_after_settlement().await.unwrap();
            let root = directory.path().to_str().unwrap();
            let adapter = Arc::new(Controlled::local());
            let store = ClusterStore::for_storage_root(root)
                .unwrap()
                .with_bootstrap_test_adapter(adapter.clone());
            let admission =
                crate::admission::acquire_with_store(&store, ClusterAdmissionPurpose::Serve)
                    .await
                    .unwrap()
                    .unwrap();
            let actor = IdentityAuthorization::authenticated("principal:owner").unwrap();
            let request = crate::maintenance::tests::request(&admission).await;
            let MaintenanceAcceptance::Accepted(mut accepted) =
                accept_maintenance(&admission, request.clone(), &actor)
                    .await
                    .unwrap()
            else {
                panic!("fresh invocation must be accepted")
            };

            // This fixture exercises the journal boundary, not optimization's
            // physical work. Its prepared publication has the real base and
            // contract; recovery must publish at most one neutral competitor.
            let graph_uri = store.graph_root("knowledge");
            let db = Omnigraph::open(&graph_uri).await.unwrap();
            let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
            let base_version = snapshot.graph_manifest_version();
            let before = snapshot.graph_head(None).unwrap().to_string();
            let schema_contract = db.schema_contract_digest();
            let input = OptimizeInput {
                before_commit: before.clone(),
                schema_contract: schema_contract.clone(),
                report_max_bytes: 16384,
                source_tables: 1,
                source_fragments: 1,
                source_rows: 1,
                source_bytes: 128,
                source_files: 1,
            };
            bind_maintenance_input(&admission, &mut accepted, input)
                .await
                .unwrap();
            bind_maintenance_publication(
                &admission,
                &mut accepted,
                OptimizePublication {
                    before_manifest_version: base_version,
                    intent_nonce: ulid::Ulid::new().to_string(),
                    schema_contract,
                    actor: Some(actor.actor().into()),
                    created_at: 1,
                    report: OptimizeReport {
                        before_commit: before.clone(),
                        after_commit: before,
                        tables: Vec::new(),
                        source_tables: 1,
                        source_fragments: 1,
                        source_rows: 1,
                        source_bytes: 128,
                        source_files: 1,
                    },
                },
            )
            .await
            .unwrap();
            drop(snapshot);
            drop(db);
            drop(accepted);
            let lock_id = admission.lock_id().to_string();
            drop(admission);
            crate::force_unlock_storage_root(root, &lock_id)
                .await
                .unwrap();
            let recovery = crate::admission::acquire_with_store(
                &store,
                ClusterAdmissionPurpose::MaintenanceRecovery {
                    operation_id: request.operation_id.clone(),
                },
            )
            .await
            .unwrap()
            .unwrap();
            adapter.arm(point, fault);
            {
                let invoke =
                    reconcile_maintenance(&recovery, "knowledge", &request.operation_id, &actor);
                match fault {
                    Fault::LostResponse => {
                        let error = invoke.await.unwrap_err();
                        assert_eq!(error.code, "maintenance_write_uncertain", "{point:?}");
                    }
                    Fault::CancelAfterCommit => {
                        tokio::pin!(invoke);
                        tokio::time::timeout(std::time::Duration::from_secs(5), async {
                            tokio::select! {
                                _ = adapter.committed.notified() => {}
                                result = &mut invoke => panic!("expected committed blocked recovery write: {result:?}"),
                            }
                        }).await.expect("recovery write must commit before cancellation");
                    }
                }
            }
            assert!(adapter.fault.lock().unwrap().is_none());
            let retained: serde_json::Value =
                serde_json::from_str(&store.read_maintenance().await.unwrap().unwrap().0).unwrap();
            let retained_invocation = if point == Point::MaintenanceSettlement {
                assert_eq!(
                    retained["outstanding"]["request"]["operation_id"],
                    request.operation_id
                );
                &retained["outstanding"]
            } else {
                assert!(retained["outstanding"].is_null());
                &retained["results"].as_array().unwrap().last().unwrap()["invocation"]
            };
            let settlement = retained_invocation["settlement"].clone();
            assert!(!settlement.is_null());
            let result =
                reconcile_maintenance(&recovery, "knowledge", &request.operation_id, &actor)
                    .await
                    .unwrap();
            assert_eq!(
                serde_json::to_value(&result.invocation.settlement).unwrap(),
                settlement
            );
            assert!(matches!(
                result.completion,
                MaintenanceCompletion::Interrupted {
                    publication: MaintenancePublication::Unpublished
                }
            ));
            let writes = adapter.writes.lock().unwrap().len();
            assert_eq!(
                reconcile_maintenance(&recovery, "knowledge", &request.operation_id, &actor)
                    .await
                    .unwrap(),
                result
            );
            assert_eq!(
                adapter.writes.lock().unwrap().len(),
                writes,
                "retained terminal recovery must not write or mint another settlement"
            );
            for expected in [
                Point::MaintenanceSettlement,
                Point::MaintenanceRecoveryTerminal,
            ] {
                assert_eq!(
                    adapter
                        .writes
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|write| Controlled::point(
                            &write.uri,
                            &write.payload,
                            write.expected_version.is_some()
                        ) == Some(expected))
                        .count(),
                    1,
                    "{expected:?} must retain its first committed write"
                );
            }
            let db = Omnigraph::open(&graph_uri).await.unwrap();
            assert_eq!(
                db.graph_manifest_version_of(ReadTarget::branch("main"))
                    .await
                    .unwrap(),
                base_version + 1
            );
            drop(db);
            drop(recovery);
            adapter.assert_no_forbidden_calls().await;
        }
    }
}

#[tokio::test]
async fn backend_recorded_late_conditional_writes_cannot_replace_claimed_state() {
    let (_directory, bundle, store, adapter) = fixture();
    let receipt = bootstrap_in_store(&store, &bundle, &super::tests::owner())
        .await
        .unwrap();
    let bootstrap_writes = adapter.writes.lock().unwrap().clone();
    let state_writes: Vec<_> = bootstrap_writes
        .iter()
        .filter(|write| write.uri.ends_with(CLUSTER_STATE_FILE))
        .collect();
    assert_eq!(state_writes.len(), 3);
    assert!(state_writes[0].expected_version.is_none());
    assert!(
        state_writes[1..]
            .iter()
            .all(|write| write.predecessor.is_some())
    );
    let admitted = claim_in_store(&store, &receipt).await.unwrap();
    let state = adapter.read(CLUSTER_STATE_FILE).await;
    let lock = adapter.read(CLUSTER_LOCK_FILE).await;
    for write in &bootstrap_writes {
        if let Some(expected) = &write.expected_version {
            // Replay the exact old PUT, including the real predecessor token,
            // directly against the backend. There is no fresh store pre-read.
            assert!(
                adapter
                    .inner
                    .write_text_if_match(adapter.key(&write.uri), &write.payload, expected)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert!(
                !adapter
                    .inner
                    .write_text_if_absent(adapter.key(&write.uri), &write.payload)
                    .await
                    .unwrap()
            );
        }
    }
    assert_eq!(adapter.read(CLUSTER_STATE_FILE).await, state);
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
    drop(admitted);
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test]
async fn both_claimants_reach_cas_with_the_same_old_etag_but_only_one_wins() {
    let (_directory, bundle, store, adapter) = fixture();
    let receipt = bootstrap_in_store(&store, &bundle, &super::tests::owner())
        .await
        .unwrap();
    *adapter.claim_barrier.lock().unwrap() = Some(Arc::new(Barrier::new(2)));
    let (one, two) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            claim_in_store(&store, &receipt),
            claim_in_store(&store, &receipt)
        )
    })
    .await
    .expect("both claimants must reach their exact-version CAS");
    *adapter.claim_barrier.lock().unwrap() = None;
    assert_eq!(adapter.claim_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
    let lock = adapter.read(CLUSTER_LOCK_FILE).await;
    drop((one, two));
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test]
async fn committed_bootstrap_writes_with_lost_responses_retain_exclusion() {
    for point in [
        Point::LockCreate,
        Point::StateCreate,
        Point::StateAccepted,
        Point::StateTerminal,
    ] {
        let (_directory, bundle, store, adapter) = fixture();
        adapter.arm(point, Fault::LostResponse);
        assert!(
            bootstrap_in_store(&store, &bundle, &super::tests::owner())
                .await
                .is_err()
        );
        assert!(
            adapter.fault.lock().unwrap().is_none(),
            "fault must run after a real write"
        );
        let lock = adapter.read(CLUSTER_LOCK_FILE).await;
        assert!(
            bootstrap_in_store(&store, &bundle, &super::tests::owner())
                .await
                .is_err()
        );
        assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
        adapter.assert_no_forbidden_calls().await;
    }
}

#[tokio::test]
async fn committed_claim_with_lost_response_grants_no_replay_admission() {
    let (_directory, bundle, store, adapter) = fixture();
    let receipt = bootstrap_in_store(&store, &bundle, &super::tests::owner())
        .await
        .unwrap();
    adapter.arm(Point::Claim, Fault::LostResponse);
    assert!(claim_in_store(&store, &receipt).await.is_err());
    assert!(adapter.fault.lock().unwrap().is_none());
    let lock = adapter.read(CLUSTER_LOCK_FILE).await;
    assert_ne!(
        serde_json::from_str::<serde_json::Value>(&lock).unwrap()["lock_id"],
        receipt.bootstrap_lock_id
    );
    assert!(claim_in_store(&store, &receipt).await.is_err());
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test]
async fn cancellation_after_backend_commit_never_releases_or_adopts_ownership() {
    for point in [
        Point::LockCreate,
        Point::StateCreate,
        Point::StateAccepted,
        Point::StateTerminal,
        Point::Claim,
    ] {
        let (_directory, bundle, store, adapter) = fixture();
        let caller = super::tests::owner();
        let receipt = if point == Point::Claim {
            Some(bootstrap_in_store(&store, &bundle, &caller).await.unwrap())
        } else {
            None
        };
        adapter.arm(point, Fault::CancelAfterCommit);
        {
            let operation = async {
                if let Some(receipt) = &receipt {
                    claim_in_store(&store, receipt).await.map(|_| ())
                } else {
                    bootstrap_in_store(&store, &bundle, &caller)
                        .await
                        .map(|_| ())
                }
            };
            tokio::pin!(operation);
            tokio::select! {
                result = &mut operation => panic!("operation must wait after commit: {result:?}"),
                () = adapter.committed.notified() => {},
                () = tokio::time::sleep(std::time::Duration::from_secs(5)) => panic!("injected storage commit was not reached"),
            }
            // Leaving the scope cancels the actual suspended storage future.
        }
        let lock = adapter.read(CLUSTER_LOCK_FILE).await;
        if let Some(receipt) = &receipt {
            assert!(claim_in_store(&store, receipt).await.is_err());
        } else {
            assert!(bootstrap_in_store(&store, &bundle, &caller).await.is_err());
        }
        assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
        adapter.assert_no_forbidden_calls().await;
    }
}

#[tokio::test]
async fn clean_release_and_concurrent_reacquisition_keep_delayed_writes_harmless() {
    use crate::admission::{ClusterAdmissionPurpose, acquire_with_store};

    let (_directory, bundle, store, adapter) = fixture();
    let receipt = bootstrap_in_store(&store, &bundle, &super::tests::owner())
        .await
        .unwrap();
    let (_, _, owner) = claim_in_store(&store, &receipt).await.unwrap().into_parts();
    let owner = owner.unwrap();
    let old_id = owner.lock_id().to_owned();
    let state = adapter.read(CLUSTER_STATE_FILE).await;
    assert!(store.release_settled("not-the-owner").await.is_err());
    owner.release_after_settlement().await.unwrap();
    let released = adapter.read(CLUSTER_LOCK_FILE).await;
    let released_value: serde_json::Value = serde_json::from_str(&released).unwrap();
    assert_eq!(released_value["version"], 2);
    assert_eq!(released_value["lock_id"], old_id);
    assert!(released_value["release_id"].is_string());
    let mut observations = store.observations();
    let mut diagnostics = Vec::new();
    store
        .observe_lock(&mut observations, &mut diagnostics)
        .await;
    assert!(!observations.locked);
    assert!(observations.lock_id.is_none());
    assert!(diagnostics.is_empty());
    // A released marker must not be deleted through an old exact-ID repair.
    assert!(
        store
            .force_unlock(&old_id, &mut observations)
            .await
            .is_err()
    );
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, released);
    let old_writes = adapter.writes.lock().unwrap().clone();

    *adapter.claim_barrier.lock().unwrap() = Some(Arc::new(Barrier::new(2)));
    let (one, two) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            acquire_with_store(&store, ClusterAdmissionPurpose::Serve),
            acquire_with_store(&store, ClusterAdmissionPurpose::Serve)
        )
    })
    .await
    .expect("both ordinary claimants reach marker CAS");
    *adapter.claim_barrier.lock().unwrap() = None;
    let next = match (one, two) {
        (Ok(Some(owner)), Err(error)) | (Err(error), Ok(Some(owner))) => {
            assert_eq!(error.code, "state_lock_held");
            owner
        }
        other => panic!("exactly one successor must acquire: {other:?}"),
    };
    assert_ne!(next.lock_id(), old_id);
    next.validate_serving().unwrap();
    let current = adapter.read(CLUSTER_LOCK_FILE).await;
    for write in old_writes {
        if let Some(expected) = write.expected_version {
            assert!(
                adapter
                    .inner
                    .write_text_if_match(adapter.key(&write.uri), &write.payload, &expected)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else if is_lease_key(&write.uri) {
            // The old owner's beats left with its released lease, so a
            // delayed beat can land again, but only under the old owner's own
            // lease directory, which no successor watches.
            assert!(
                write.uri.contains(&format!("/__cluster/lease/{old_id}/")),
                "{}",
                write.uri
            );
            adapter
                .inner
                .write_text_if_absent(adapter.key(&write.uri), &write.payload)
                .await
                .unwrap();
        } else {
            assert!(
                !adapter
                    .inner
                    .write_text_if_absent(adapter.key(&write.uri), &write.payload)
                    .await
                    .unwrap()
            );
        }
    }
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, current);
    assert_eq!(adapter.read(CLUSTER_STATE_FILE).await, state);
    assert!(claim_in_store(&store, &receipt).await.is_err());
    drop(next);
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, current);
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test]
async fn lost_release_acknowledgement_never_replays_over_the_successor() {
    use crate::admission::{ClusterAdmissionPurpose, acquire_with_store};

    for fault in [Fault::LostResponse, Fault::CancelAfterCommit] {
        let (_directory, bundle, store, adapter) = fixture();
        let receipt = bootstrap_in_store(&store, &bundle, &super::tests::owner())
            .await
            .unwrap();
        let (_, _, owner) = claim_in_store(&store, &receipt).await.unwrap().into_parts();
        let owner = owner.unwrap();
        let old_id = owner.lock_id().to_owned();
        adapter.arm(Point::Release, fault);
        match fault {
            Fault::LostResponse => {
                assert!(owner.release_after_settlement().await.is_err());
            }
            Fault::CancelAfterCommit => {
                let release = owner.release_after_settlement();
                tokio::pin!(release);
                tokio::select! {
                    result = &mut release => panic!("release must wait after commit: {result:?}"),
                    () = adapter.committed.notified() => {},
                    () = tokio::time::sleep(std::time::Duration::from_secs(5)) => panic!("release did not reach backend"),
                }
            }
        }
        assert!(adapter.fault.lock().unwrap().is_none());
        let release = adapter.writes.lock().unwrap().last().unwrap().clone();
        let next = acquire_with_store(&store, ClusterAdmissionPurpose::Serve)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(next.lock_id(), old_id);
        let current = adapter.read(CLUSTER_LOCK_FILE).await;
        assert!(
            adapter
                .inner
                .write_text_if_match(
                    adapter.key(&release.uri),
                    &release.payload,
                    release.expected_version.as_deref().unwrap()
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(store.release_settled(&old_id).await.is_err());
        assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, current);
        drop(next);
        adapter.assert_no_forbidden_calls().await;
    }
}
