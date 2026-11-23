//! The in-memory backend supplies real atomic create/CAS semantics. This test
//! adapter records the actual requests and injects lost acknowledgements only
//! after the backend committed them; no production failpoint substitutes for
//! an uncertain storage response.
use super::*;
use omnigraph_storage::{
    ListDirBounds, ObjectStorageAdapter, Result as StorageResult, StorageAdapter, StorageError,
    StorageFailure, StorageFailureKind,
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
    /// A same-identity rerun's exact-version replace of its bootstrap lock.
    Reenter,
    FreshnessReceipt,
    LedgerReceipt,
    DeploymentReceipt,
    StateCreate,
    StateAccepted,
    StateTerminal,
    Claim,
    Release,
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    LostResponse,
    CancelAfterCommit,
}

#[derive(Debug)]
struct Controlled {
    inner: ObjectStorageAdapter,
    allow_fixture_setup: bool,
    writes: Mutex<Vec<Write>>,
    forbidden_calls: AtomicUsize,
    fault: Mutex<Option<(Point, Fault)>>,
    committed: Notify,
    claim_barrier: Mutex<Option<Arc<Barrier>>>,
    claim_attempts: AtomicUsize,
    /// `(only once the lock exists, remaining)` injected `exists()` errors.
    exists_fault: Mutex<Option<(bool, usize)>>,
}

impl Controlled {
    fn new() -> Self {
        Self {
            inner: ObjectStorageAdapter::in_memory(),
            allow_fixture_setup: false,
            writes: Mutex::default(),
            forbidden_calls: AtomicUsize::new(0),
            fault: Mutex::default(),
            committed: Notify::new(),
            claim_barrier: Mutex::default(),
            claim_attempts: AtomicUsize::new(0),
            exists_fault: Mutex::default(),
        }
    }

    fn key<'a>(&self, uri: &'a str) -> &'a str {
        uri.strip_prefix(URI_PREFIX)
            .expect("test-only S3 namespace")
    }

    fn point(uri: &str, payload: &str, cas: bool) -> Option<Point> {
        if uri.ends_with("/__cluster/lock.json") {
            let record = serde_json::from_str::<serde_json::Value>(payload).unwrap();
            Some(if record["release_id"].is_string() {
                Point::Release
            } else if cas && record["operation"] == "bootstrap_serving" {
                Point::Reenter
            } else if cas {
                Point::Claim
            } else {
                Point::LockCreate
            })
        } else if uri.ends_with("/__cluster/bootstrap/freshness.json") {
            Some(Point::FreshnessReceipt)
        } else if uri.ends_with("/__cluster/bootstrap/ledger.json") {
            Some(Point::LedgerReceipt)
        } else if uri.ends_with("/__cluster/bootstrap/deployment.json") {
            Some(Point::DeploymentReceipt)
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
            // What the storage layer reports for a conditional write whose
            // re-issues and read-back did not settle it.
            Some(Fault::LostResponse) => Err(StorageError::Unresolved(StorageFailure::new(
                StorageFailureKind::Transient,
                "injected response lost after backend commit",
            ))),
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

    /// Fail the next `failures` existence probes, only once the lock exists
    /// when `after_lock`, as a stalled response body does.
    fn arm_exists(&self, after_lock: bool, failures: usize) {
        *self.exists_fault.lock().unwrap() = Some((after_lock, failures));
    }

    fn disarm_exists(&self) {
        *self.exists_fault.lock().unwrap() = None;
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
        let locked = self
            .inner
            .exists(self.key(&format!("{ROOT}/{CLUSTER_LOCK_FILE}")))
            .await?;
        let inject = match self.exists_fault.lock().unwrap().as_mut() {
            Some((after_lock, remaining)) if *remaining > 0 && (locked || !*after_lock) => {
                *remaining -= 1;
                true
            }
            _ => false,
        };
        if inject {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "injected stalled response body",
            )
            .into());
        }
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
                .map(|entry| format!("{URI_PREFIX}{entry}"))
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
                    .map(|entry| format!("{URI_PREFIX}{entry}"))
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
async fn backend_recorded_late_conditional_writes_cannot_replace_claimed_state() {
    let (_directory, bundle, store, adapter) = fixture();
    let receipt = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
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
    let receipt = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
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
            bootstrap_in_store(
                &store,
                &bundle,
                &super::tests::owner(),
                &super::tests::request()
            )
            .await
            .is_err()
        );
        assert!(
            adapter.fault.lock().unwrap().is_none(),
            "fault must run after a real write"
        );
        let lock = adapter.read(CLUSTER_LOCK_FILE).await;
        assert!(
            bootstrap_in_store(
                &store,
                &bundle,
                &super::tests::owner(),
                &super::tests::request()
            )
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
    let receipt = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
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
            Some(
                bootstrap_in_store(&store, &bundle, &caller, &super::tests::request())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        adapter.arm(point, Fault::CancelAfterCommit);
        {
            let operation = async {
                if let Some(receipt) = &receipt {
                    claim_in_store(&store, receipt).await.map(|_| ())
                } else {
                    bootstrap_in_store(&store, &bundle, &caller, &super::tests::request())
                        .await
                        .map(|_| ())
                        .map_err(BootstrapFailure::into_diagnostic)
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
            assert!(
                bootstrap_in_store(&store, &bundle, &caller, &super::tests::request())
                    .await
                    .is_err()
            );
        }
        assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
        adapter.assert_no_forbidden_calls().await;
    }
}

#[tokio::test]
async fn clean_release_and_concurrent_reacquisition_keep_delayed_writes_harmless() {
    use crate::admission::{ClusterAdmissionPurpose, acquire_with_store};

    let (_directory, bundle, store, adapter) = fixture();
    let receipt = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
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

/// The first direct apply on a fresh root bootstraps the ledger and releases
/// that lock before the deployment. When the release acknowledgement is lost
/// after the backend committed it, the lock is free, so the diagnostic must not
/// say it is held: it names the admission to compare with `cluster status`.
#[tokio::test]
async fn lost_bootstrap_release_acknowledgement_is_reported_unconfirmed_issue_1359() {
    let (_directory, bundle, store, adapter) = fixture();
    adapter.arm(Point::Release, Fault::LostResponse);
    let error =
        crate::deployment::execution::bootstrap_ledger(&store, &bundle, &super::tests::owner())
            .await
            .unwrap_err();
    assert!(adapter.fault.lock().unwrap().is_none());
    let lock: serde_json::Value =
        serde_json::from_str(&adapter.read(CLUSTER_LOCK_FILE).await).unwrap();
    assert!(lock["release_id"].is_string(), "{lock}");
    let lock_id = lock["lock_id"].as_str().unwrap();
    assert!(!error.message.contains("remains held"), "{error:?}");
    assert!(error.message.contains(lock_id), "{error:?}");
    assert!(error.message.contains("cluster status --json"), "{error:?}");
    assert!(
        error.message.contains("lost after backend commit"),
        "{error:?}"
    );
    assert_eq!(error.code, "cluster_admission_release_failed");
    assert_eq!(error.path, CLUSTER_LOCK_FILE);
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test]
async fn lost_release_acknowledgement_never_replays_over_the_successor() {
    use crate::admission::{ClusterAdmissionPurpose, acquire_with_store};

    for fault in [Fault::LostResponse, Fault::CancelAfterCommit] {
        let (_directory, bundle, store, adapter) = fixture();
        let receipt = bootstrap_in_store(
            &store,
            &bundle,
            &super::tests::owner(),
            &super::tests::request(),
        )
        .await
        .unwrap();
        let (_, _, owner) = claim_in_store(&store, &receipt).await.unwrap().into_parts();
        let owner = owner.unwrap();
        let old_id = owner.lock_id().to_owned();
        adapter.arm(Point::Release, fault);
        match fault {
            Fault::LostResponse => {
                let error = crate::deployment::execution::release_converged_deployment(
                    owner,
                    &receipt.deployment_id,
                )
                .await
                .unwrap_err();
                assert_eq!(error.code, "cluster_admission_release_failed");
                assert!(error.message.contains(&receipt.deployment_id), "{error:?}");
                assert!(error.message.contains(&old_id), "{error:?}");
                assert!(
                    error.message.contains("lost after backend commit"),
                    "{error:?}"
                );
                assert!(error.message.contains("cluster status"), "{error:?}");
                assert!(!error.message.contains("remains held"), "{error:?}");
                assert_eq!(error.path, CLUSTER_LOCK_FILE);
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

fn outcome(
    code: &str,
    step: BootstrapStep,
    effects: BootstrapEffects,
    retryable: bool,
) -> BootstrapOutcome {
    BootstrapOutcome {
        code: code.into(),
        step,
        effects,
        retryable,
    }
}

fn lock_json(text: &str) -> serde_json::Value {
    serde_json::from_str(text).unwrap()
}

/// Leave this identity's lock retained, as the 2026-11-23 failure did: the
/// freshness probe after the lock keeps failing until its deadline.
async fn fail_after_lock(store: &ClusterStore, bundle: &CapturedDeployment, adapter: &Controlled) {
    adapter.arm_exists(true, usize::MAX);
    let failure = bootstrap_in_store(
        store,
        bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
    .await
    .unwrap_err();
    adapter.disarm_exists();
    assert_eq!(
        failure.outcome(),
        outcome(
            "bootstrap_freshness_unknown",
            BootstrapStep::Freshness,
            BootstrapEffects::Confirmed,
            true
        )
    );
}

// The 2026-11-23 failure: a transient freshness read after the lock.
#[tokio::test(start_paused = true)]
async fn same_identity_rerun_resumes_its_retained_lock_with_one_lineage() {
    let (_directory, bundle, store, adapter) = fixture();
    fail_after_lock(&store, &bundle, &adapter).await;
    let retained = adapter.read(CLUSTER_LOCK_FILE).await;
    let first = lock_json(&retained);
    assert_eq!(first["version"], 4);
    assert_eq!(first["operation"], "bootstrap_serving");
    assert_eq!(first["incarnation"], "inc-1");
    assert_eq!(first["request_id"], "req-1");
    assert_eq!(first["attempt"], 1);
    assert!(
        !adapter
            .inner
            .exists(adapter.key(&format!("{ROOT}/{CLUSTER_STATE_FILE}")))
            .await
            .unwrap()
    );

    // Without the caller's exclusion proof nothing changes.
    let refused = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        refused.outcome(),
        outcome(
            "bootstrap_exclusion_required",
            BootstrapStep::Lock,
            BootstrapEffects::None,
            false
        )
    );
    assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, retained);

    let receipt = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::resume(),
    )
    .await
    .unwrap();
    let lock_writes: Vec<_> = adapter
        .writes
        .lock()
        .unwrap()
        .iter()
        .filter(|write| write.uri.ends_with(CLUSTER_LOCK_FILE))
        .cloned()
        .collect();
    // One lineage: one create, then one CAS from the exact created version.
    assert_eq!(lock_writes.len(), 2);
    assert!(lock_writes[0].expected_version.is_none());
    let (predecessor, predecessor_version) = lock_writes[1].predecessor.clone().unwrap();
    assert_eq!(predecessor, retained);
    assert_eq!(
        lock_writes[1].expected_version.as_deref(),
        Some(predecessor_version.as_str())
    );
    let current = lock_json(&adapter.read(CLUSTER_LOCK_FILE).await);
    assert_eq!(current["attempt"], 2);
    assert_eq!(current["incarnation"], "inc-1");
    assert_eq!(current["request_id"], "req-1");
    assert_ne!(current["lock_id"], first["lock_id"]);
    assert_eq!(current["lock_id"], receipt.bootstrap_lock_id.as_str());
    for path in [FRESHNESS_RECEIPT, LEDGER_RECEIPT, DEPLOYMENT_RECEIPT] {
        assert!(
            adapter
                .read(path)
                .await
                .contains("\"incarnation\":\"inc-1\"")
        );
    }
    claim_in_store(&store, &receipt).await.unwrap();
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test(start_paused = true)]
async fn a_retained_lock_of_another_identity_or_a_legacy_lock_is_never_re_entered() {
    let (_directory, bundle, store, adapter) = fixture();
    fail_after_lock(&store, &bundle, &adapter).await;
    let retained = adapter.read(CLUSTER_LOCK_FILE).await;
    let refused = outcome(
        "bootstrap_root_not_fresh",
        BootstrapStep::Lock,
        BootstrapEffects::None,
        false,
    );
    for identity in [
        BootstrapIdentity::new("inc-2", Some("req-1".into())).unwrap(),
        BootstrapIdentity::new("inc-1", Some("req-2".into())).unwrap(),
        BootstrapIdentity::new("inc-1", None).unwrap(),
    ] {
        let request = BootstrapRequest {
            identity,
            prior_attempts: PriorAttempts::Excluded,
        };
        let failure = bootstrap_in_store(&store, &bundle, &super::tests::owner(), &request)
            .await
            .unwrap_err();
        assert_eq!(failure.outcome(), refused);
        assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, retained);
    }
    adapter.assert_no_forbidden_calls().await;

    // A legacy bootstrap lock names no identity; a serving lock is not a
    // bootstrap lock. Neither is ever re-entered.
    for legacy in [
        r#"{"version":1,"lock_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","operation":"bootstrap_serving","created_at":"2026-11-23T00:00:00Z","pid":1}"#,
        r#"{"version":1,"lock_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","operation":"serve","created_at":"2026-11-23T00:00:00Z","pid":1}"#,
    ] {
        let (_directory, bundle, store, adapter) = fixture();
        assert!(
            adapter
                .inner
                .write_text_if_absent(adapter.key(&format!("{ROOT}/{CLUSTER_LOCK_FILE}")), legacy)
                .await
                .unwrap()
        );
        let failure = bootstrap_in_store(
            &store,
            &bundle,
            &super::tests::owner(),
            &super::tests::resume(),
        )
        .await
        .unwrap_err();
        assert_eq!(failure.outcome(), refused);
        assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, legacy);
        assert!(adapter.writes.lock().unwrap().is_empty());
        adapter.assert_no_forbidden_calls().await;
    }
}

#[tokio::test(start_paused = true)]
async fn transient_freshness_probe_errors_are_retried_within_the_deadline() {
    let (_directory, bundle, store, adapter) = fixture();
    adapter.arm_exists(true, 3);
    let start = tokio::time::Instant::now();
    let receipt = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
    .await
    .unwrap();
    assert_eq!(adapter.exists_fault.lock().unwrap().unwrap().1, 0);
    assert!(start.elapsed() < crate::store::BOOTSTRAP_FRESHNESS_DEADLINE);
    claim_in_store(&store, &receipt).await.unwrap();
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test(start_paused = true)]
async fn a_persistent_probe_error_before_the_lock_fails_at_the_deadline_with_no_effects() {
    let (_directory, bundle, store, adapter) = fixture();
    adapter.arm_exists(false, usize::MAX);
    let start = tokio::time::Instant::now();
    let failure = bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
    .await
    .unwrap_err();
    assert!(start.elapsed() >= crate::store::BOOTSTRAP_FRESHNESS_DEADLINE);
    assert_eq!(
        failure.outcome(),
        outcome(
            "bootstrap_freshness_unknown",
            BootstrapStep::Freshness,
            BootstrapEffects::None,
            true
        )
    );
    assert!(
        failure
            .diagnostic()
            .message
            .contains("injected stalled response")
    );
    assert!(adapter.writes.lock().unwrap().is_empty());
    assert!(
        !adapter
            .inner
            .exists(adapter.key(&format!("{ROOT}/{CLUSTER_LOCK_FILE}")))
            .await
            .unwrap()
    );
    // Nothing was written, so a plain rerun starts over.
    adapter.disarm_exists();
    bootstrap_in_store(
        &store,
        &bundle,
        &super::tests::owner(),
        &super::tests::request(),
    )
    .await
    .unwrap();
    adapter.assert_no_forbidden_calls().await;
}

#[tokio::test]
async fn an_unconfirmed_write_is_classified_and_a_rerun_refuses_past_it() {
    for (point, step) in [
        (Point::StateCreate, BootstrapStep::Ledger),
        (Point::StateAccepted, BootstrapStep::Deployment),
    ] {
        let (_directory, bundle, store, adapter) = fixture();
        adapter.arm(point, Fault::LostResponse);
        let failure = bootstrap_in_store(
            &store,
            &bundle,
            &super::tests::owner(),
            &super::tests::request(),
        )
        .await
        .unwrap_err();
        assert!(adapter.fault.lock().unwrap().is_none());
        assert_eq!(failure.step(), step, "{point:?}");
        assert_eq!(
            failure.effects(),
            BootstrapEffects::Unconfirmed,
            "{point:?}"
        );
        assert!(!failure.retryable(), "{point:?}");
        let lock = adapter.read(CLUSTER_LOCK_FILE).await;
        let state = adapter.read(CLUSTER_STATE_FILE).await;
        // The rerun refuses before it re-enters the lock.
        let refused = bootstrap_in_store(
            &store,
            &bundle,
            &super::tests::owner(),
            &super::tests::resume(),
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused.outcome(),
            outcome(
                "bootstrap_step_unconfirmed",
                step,
                BootstrapEffects::None,
                false
            ),
            "{point:?}"
        );
        assert_eq!(adapter.read(CLUSTER_LOCK_FILE).await, lock);
        assert_eq!(adapter.read(CLUSTER_STATE_FILE).await, state);
        adapter.assert_no_forbidden_calls().await;
    }
}

#[tokio::test]
async fn a_lost_answer_on_a_step_receipt_that_landed_resumes() {
    for (point, step) in [
        (Point::FreshnessReceipt, BootstrapStep::Freshness),
        (Point::LedgerReceipt, BootstrapStep::Ledger),
        (Point::DeploymentReceipt, BootstrapStep::Deployment),
    ] {
        let (_directory, bundle, store, adapter) = fixture();
        adapter.arm(point, Fault::LostResponse);
        let failure = bootstrap_in_store(
            &store,
            &bundle,
            &super::tests::owner(),
            &super::tests::request(),
        )
        .await
        .unwrap_err();
        assert!(adapter.fault.lock().unwrap().is_none());
        assert_eq!(failure.step(), step, "{point:?}");
        assert_eq!(
            failure.effects(),
            BootstrapEffects::Unconfirmed,
            "{point:?}"
        );
        let receipt = bootstrap_in_store(
            &store,
            &bundle,
            &super::tests::owner(),
            &super::tests::resume(),
        )
        .await
        .unwrap();
        claim_in_store(&store, &receipt).await.unwrap();
        adapter.assert_no_forbidden_calls().await;
    }
}
