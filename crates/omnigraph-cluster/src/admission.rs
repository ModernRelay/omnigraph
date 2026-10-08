//! Root-wide ownership for the v2 deployment protocol.
//!
//! The persisted lock is exclusion among participating storage-owner doors;
//! it is not native-I/O fencing or permission to reclaim an abandoned owner.

use std::{collections::BTreeMap, sync::Arc};

use omnigraph::db::SchemaContractDigest;

use crate::state_lock::StateLockGuard;
use crate::store::ClusterStore;
use crate::{CLUSTER_STATE_FILE, Diagnostic};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterAdmissionPurpose {
    Serve,
    GraphOperation,
    Deployment,
    Reconcile { deployment_id: String },
}

impl ClusterAdmissionPurpose {
    fn operation(&self) -> &'static str {
        match self {
            Self::Serve => "serve",
            Self::GraphOperation => "graph_operation",
            Self::Deployment => "deployment",
            Self::Reconcile { .. } => "deployment_reconcile",
        }
    }
}

/// Lifetime admission to a v2 cluster. Clones share the same persisted lock.
///
/// Dropping this value never unlocks. In particular, returning from a command,
/// draining HTTP owners, or stopping a runtime is not a native settlement proof.
#[derive(Debug, Clone)]
pub struct ClusterAdmission(Arc<AdmissionOwner>);

#[derive(Debug)]
struct AdmissionOwner {
    store: ClusterStore,
    guard: StateLockGuard,
    canonical_root: String,
    schema_contracts: BTreeMap<String, SchemaContractDigest>,
    serving_deployment: Option<crate::DeploymentResult>,
    purpose: ClusterAdmissionPurpose,
}

impl ClusterAdmission {
    /// Only the private policy-only bootstrap protocol can transfer an
    /// already-held lock into an admission. Neither owner is ever unlocked.
    pub(crate) fn from_bootstrap_lock(
        store: ClusterStore,
        guard: StateLockGuard,
        serving_deployment: Option<crate::DeploymentResult>,
    ) -> Self {
        let purpose = if serving_deployment.is_some() {
            ClusterAdmissionPurpose::Serve
        } else {
            ClusterAdmissionPurpose::Deployment
        };
        Self(Arc::new(AdmissionOwner {
            canonical_root: store
                .canonical_root()
                .expect("bootstrap has a validated S3 root"),
            store,
            guard,
            schema_contracts: BTreeMap::new(),
            serving_deployment,
            purpose,
        }))
    }

    pub fn canonical_root(&self) -> &str {
        &self.0.canonical_root
    }

    pub fn lock_id(&self) -> &str {
        self.0.guard.lock_id()
    }

    /// Explicit storage lifetime acquired before the first native lock request.
    pub fn io_scope(&self) -> Option<omnigraph_storage::StorageIoScope> {
        self.0.store.io_scope()
    }

    pub(crate) fn store(&self) -> ClusterStore {
        self.0.store.clone()
    }

    /// The running server and direct deployment executor use the same durable
    /// root ownership. Check the exact persisted owner before control effects.
    pub(crate) async fn validate_deployment(&self) -> Result<(), Diagnostic> {
        if !matches!(
            self.0.purpose,
            ClusterAdmissionPurpose::Serve | ClusterAdmissionPurpose::Deployment
        ) {
            return Err(crate::deployment::refusal(
                "cluster_admission_purpose_mismatch",
                "this owner cannot deploy",
            ));
        }
        self.validate_current_lock().await
    }

    /// Completion uses the accepted deployment's current base, not the graph
    /// inventory captured when a long-lived server first acquired this owner.
    pub(crate) async fn validate_completion(&self, deployment_id: &str) -> Result<(), Diagnostic> {
        match &self.0.purpose {
            ClusterAdmissionPurpose::Serve | ClusterAdmissionPurpose::Deployment => {}
            ClusterAdmissionPurpose::Reconcile {
                deployment_id: original,
            } if original == deployment_id => {}
            _ => {
                return Err(crate::deployment::refusal(
                    "cluster_admission_purpose_mismatch",
                    "this owner cannot complete the accepted deployment",
                ));
            }
        }
        self.validate_current_lock().await
    }

    async fn validate_current_lock(&self) -> Result<(), Diagnostic> {
        let mut observations = self.0.store.observations();
        let mut diagnostics = Vec::new();
        self.0
            .store
            .observe_lock(&mut observations, &mut diagnostics)
            .await;
        if let Some(error) = diagnostics.into_iter().next() {
            return Err(error);
        }
        if observations.lock_id.as_deref() != Some(self.lock_id()) {
            return Err(crate::deployment::refusal(
                "cluster_admission_lost",
                "writer no longer owns the exact cluster admission",
            ));
        }
        Ok(())
    }

    /// A recovery/deployment owner cannot be reinterpreted as a serving owner.
    pub fn validate_serving(&self) -> Result<(), Diagnostic> {
        if self.0.purpose != ClusterAdmissionPurpose::Serve {
            return Err(Diagnostic::error(
                "cluster_admission_purpose_mismatch",
                "__cluster/lock.json",
                "serving requires its own lifetime admission captured before the serving snapshot",
            ));
        }
        Ok(())
    }

    /// The exact achieved contract captured with this serving admission.
    /// This lookup opens no engine. Startup compares it with each opened graph
    /// independently, so an unavailable graph does not block healthy peers.
    pub fn expected_serving_schema_contract(
        &self,
        graph_uri: &str,
    ) -> Result<&SchemaContractDigest, Diagnostic> {
        self.validate_serving()?;
        let graph_id = self.admitted_graph_id(graph_uri)?;
        Ok(self
            .0
            .schema_contracts
            .get(&graph_id)
            .expect("admitted graph"))
    }

    /// The current converged receipt captured under this admission. This is
    /// startup input, not evidence that this process has installed its bindings.
    pub fn serving_deployment(&self) -> Option<&crate::DeploymentResult> {
        self.0.serving_deployment.as_ref()
    }

    /// Explicitly release a uniquely owned admission after the caller has
    /// proved all graph/native effects and accepted control I/O settled.
    ///
    /// This method does not establish that proof. Error, cancellation, an
    /// outstanding clone, or abandonment never authorizes release. On S3 the
    /// held version becomes a unique released marker by CAS; the key remains
    /// present so a delayed release or original bootstrap create cannot
    /// replace a successor. A lost release acknowledgement remains an error,
    /// never a reason to delete or retry against a newer version. Operators
    /// must still exclude concurrent administrative force-unlock.
    pub async fn release_after_settlement(self) -> Result<(), Diagnostic> {
        let owner = Arc::try_unwrap(self.0).map_err(|owner| {
            Diagnostic::error(
                "cluster_admission_in_use",
                "__cluster/lock.json",
                format!(
                    "admission {} still has live owners; retaining its lock",
                    owner.guard.lock_id()
                ),
            )
        })?;
        owner.store.release_settled(owner.guard.lock_id()).await
    }

    /// Verify a graph's current membership while this root remains excluded.
    /// Alias spellings are accepted only when they resolve inside this root's
    /// canonical graph layout; graph symlinks escaping that layout are refused.
    pub async fn validate_graph_uri(&self, graph_uri: &str) -> Result<(), Diagnostic> {
        self.admitted_graph_id(graph_uri).map(|_| ())
    }

    fn admitted_graph_id(&self, graph_uri: &str) -> Result<String, Diagnostic> {
        admitted_graph_id(self.canonical_root(), &self.0.schema_contracts, graph_uri)
    }

    /// Only a completed read-only preflight may call this. Abandonment and
    /// cancellation never enter this release path; writable work must retain
    /// admission until independently settled.
    pub(crate) async fn release_refused_preflight(self, refusal: Diagnostic) -> Diagnostic {
        let lock_id = self.lock_id().to_owned();
        match self.release_after_settlement().await {
            Ok(()) => refusal,
            Err(error) => release_failure(refusal, &lock_id, error),
        }
    }
}

/// Shared membership semantics for exclusive admission and read-only captures.
pub(crate) fn admitted_graph_id(
    canonical_root: &str,
    schema_contracts: &BTreeMap<String, SchemaContractDigest>,
    graph_uri: &str,
) -> Result<String, Diagnostic> {
    let canonical = canonical_graph_uri(graph_uri)?;
    let expected_prefix = format!("{}/graphs/", canonical_root.trim_end_matches('/'));
    let canonical = canonical_store_uri(&canonical)?;
    let graph_id = canonical
        .strip_prefix(&expected_prefix)
        .and_then(|tail| tail.strip_suffix(".omni"))
        .filter(|id| !id.is_empty() && !id.contains('/'))
        .ok_or_else(|| {
            Diagnostic::error(
                "cluster_graph_root_mismatch",
                omnigraph_storage::redacted_storage_uri(graph_uri),
                "graph root is outside the admitted cluster's canonical graph layout",
            )
        })?;
    if !schema_contracts.contains_key(graph_id) {
        return Err(Diagnostic::error(
            "graph_not_applied",
            format!("graph.{graph_id}"),
            "graph is not in the admitted cluster's applied inventory",
        ));
    }
    Ok(graph_id.to_string())
}

fn release_failure(refusal: Diagnostic, lock_id: &str, error: Diagnostic) -> Diagnostic {
    crate::deployment::refusal(
        "cluster_preflight_release_failed",
        format!(
            "admission {lock_id}: release could not be confirmed; {}: {}; release error: {}",
            refusal.code, refusal.message, error.message
        ),
    )
}

/// The caller has completed a read-only phase after confirmed lock creation.
/// This is deliberately not a destructor or a general error-unlock mechanism.
pub(crate) async fn release_refused_preflight(
    store: &ClusterStore,
    lock_id: &str,
    refusal: Diagnostic,
) -> Diagnostic {
    match store.release_settled(lock_id).await {
        Ok(()) => refusal,
        Err(error) => release_failure(refusal, lock_id, error),
    }
}

/// Admit a storage-owner operation addressed directly to its storage root.
/// A v1 ledger must be explicitly migrated under stopped-writer exclusion.
pub async fn acquire_cluster_admission(
    storage_root: &str,
    purpose: ClusterAdmissionPurpose,
) -> Result<Option<ClusterAdmission>, Diagnostic> {
    let mut store = ClusterStore::for_storage_root(storage_root)?;
    if purpose == ClusterAdmissionPurpose::Serve {
        store = store.with_io_scope(omnigraph_storage::StorageIoScope::new())?;
    }
    acquire_with_store(&store, purpose).await
}

/// Admit an embedded graph door before any writable open or native control.
/// Standalone graphs retain their existing admission contract; cluster v1 is refused.
pub async fn acquire_graph_admission(
    graph_uri: &str,
    purpose: ClusterAdmissionPurpose,
) -> Result<Option<ClusterAdmission>, Diagnostic> {
    let Some(root) = crate::serve::cluster_root_for_graph_uri(graph_uri).await? else {
        return Ok(None);
    };
    let admission = acquire_cluster_admission(&root, purpose).await?;
    if let Some(owner) = admission {
        if let Err(refusal) = owner.validate_graph_uri(graph_uri).await {
            return Err(owner.release_refused_preflight(refusal).await);
        }
        return Ok(Some(owner));
    }
    Ok(None)
}

pub(crate) async fn acquire_with_store(
    store: &ClusterStore,
    purpose: ClusterAdmissionPurpose,
) -> Result<Option<ClusterAdmission>, Diagnostic> {
    let mut observations = store.observations();
    let state = store.read_state(&mut observations).await?.state;
    let Some(state) = state else {
        return Ok(None);
    };
    if state.version != 2 {
        return Err(crate::deployment::refusal(
            "ledger_upgrade_required",
            "explicitly migrate the stopped cluster to ledger v2 before serving or writing",
        ));
    }
    let canonical_root = store.canonical_root()?;
    let mut guard = store
        .acquire_lock(purpose.operation(), &mut observations)
        .await?;
    // No awaited work may occur between receiving the lock and disabling the
    // legacy destructor's unlock. Cancellation after this point fails closed.
    guard.hold_on_drop();
    // This entire phase only reads control state. A returned refusal proves
    // this owner started no native or control write beyond its completed lock.
    let captured = async {
        let state = store
            .read_state(&mut observations)
            .await?
            .state
            .ok_or_else(missing_state)?;
        if state.version != 2 {
            return Err(Diagnostic::error(
                "cluster_admission_version_changed",
                CLUSTER_STATE_FILE,
                "cluster version changed while acquiring admission",
            ));
        }
        let allowed = match (&purpose, state.outstanding_deployment_id()) {
            (ClusterAdmissionPurpose::Reconcile { deployment_id }, Some(outstanding)) => {
                deployment_id == outstanding
            }
            (ClusterAdmissionPurpose::Reconcile { .. }, None) => false,
            (_, None) => true,
            (_, Some(_)) => false,
        };
        if !allowed {
            return Err(Diagnostic::error(
                "cluster_deployment_outstanding",
                CLUSTER_STATE_FILE,
                "only reconciliation of the exact outstanding deployment is admitted; serving and other writers are refused",
            ));
        }
        let serving_deployment = state.deployment_results.as_ref().and_then(|results| {
            results.iter().rev().find(|result| {
                result.converged
                    && Some(result.result_revision) == state.applied_revision.result_revision
                    && result.config_digest.is_some()
                    && result.config_digest == state.applied_revision.config_digest
            }).cloned()
        });
        Ok::<_, Diagnostic>((state
            .applied_revision
            .schema_contracts
            .expect("validated v2 state has exact achieved contracts"), serving_deployment))
    }.await;
    let (schema_contracts, serving_deployment) = match captured {
        Ok(captured) => captured,
        Err(error) => return Err(release_refused_preflight(store, guard.lock_id(), error).await),
    };
    Ok(Some(ClusterAdmission(Arc::new(AdmissionOwner {
        store: store.clone(),
        guard,
        canonical_root,
        schema_contracts,
        serving_deployment,
        purpose,
    }))))
}

/// Canonical process-owner identity for a graph URI, including local aliases.
pub fn canonical_graph_uri(graph_uri: &str) -> Result<String, Diagnostic> {
    omnigraph_storage::normalize_root_uri(graph_uri)
        .and_then(|root| omnigraph_storage::write_queue_root_identity(&root))
        .map_err(|error| {
            Diagnostic::error(
                "storage_root_invalid",
                omnigraph_storage::redacted_storage_uri(graph_uri),
                format!("could not resolve graph storage identity: {error}"),
            )
        })
}

fn canonical_store_uri(uri: &str) -> Result<String, Diagnostic> {
    let kind = omnigraph_storage::storage_kind_for_uri(uri)
        .map_err(|error| Diagnostic::error("storage_root_invalid", "storage", error.to_string()))?;
    match kind {
        omnigraph_storage::StorageKind::Local => {
            Ok(format!("file://{}", uri.trim_start_matches("file://")))
        }
        _ => Ok(uri.to_string()),
    }
}

fn missing_state() -> Diagnostic {
    Diagnostic::error(
        "cluster_state_missing",
        CLUSTER_STATE_FILE,
        "the admitted cluster no longer has applied state",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state_fixture(version: u32, outstanding: bool) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("__cluster")).unwrap();
        std::fs::create_dir_all(dir.path().join("graphs/knowledge.omni")).unwrap();
        let mut state = json!({
            "version": 1,
            "state_revision": 1,
            "applied_revision": {"resources": {
                "graph.knowledge": {"digest": "a".repeat(64)},
                "schema.knowledge": {"digest": "c".repeat(64)}
            }}
        });
        if version == 2 {
            state["version"] = json!(2);
            state["ledger_id"] = json!("01K00000000000000000000000");
            state["next_sequence"] = json!(if outstanding { 2 } else { 1 });
            state["deployment_results"] = json!([]);
            state["applied_revision"]["result_revision"] = json!(0);
            state["applied_revision"]["schema_contracts"] = json!({
                "knowledge": {
                    "source_hash": "c".repeat(64),
                    "schema_ir_hash": format!("sha256:{}", "e".repeat(64)),
                    "schema_identity_domain": "01K00000000000000000000010",
                    "schema_identity_version": 2
                }
            });
            if outstanding {
                state["outstanding"] = json!({
                    "id": "01K00000000000000000000000:1:01K00000000000000000000001",
                    "input_digest": "b".repeat(64),
                    "authorization": {
                        "version": 2,
                        "ledger_id": "01K00000000000000000000000",
                        "authority": {"kind": "storage_owner", "actor": null},
                        "base": {"result_revision": 0, "resource_digests": {}, "schema_contracts": {}, "capture_cas": format!("sha256:{}", "d".repeat(64))},
                        "input_digest": "b".repeat(64),
                        "policy_digests": {},
                        "effects": []
                    },
                    "graphs": {},
                    "reserved_ledger_bytes": 4096,
                    "reserved_result_bytes": 1024
                });
            }
        }
        let mut state: crate::types::ClusterState = serde_json::from_value(state).unwrap();
        let digest = crate::expected_state_graph_resource_digest(
            &state,
            "knowledge",
            &state.applied_revision.resources["graph.knowledge"],
        );
        state
            .applied_revision
            .resources
            .get_mut("graph.knowledge")
            .unwrap()
            .digest = digest;
        let resource_digests = crate::config::state_resource_digests(&state);
        if let Some(pending) = &mut state.outstanding {
            pending.authorization.base.resource_digests = resource_digests;
            pending.authorization.base.schema_contracts =
                state.applied_revision.schema_contracts.clone().unwrap();
        }
        std::fs::write(
            dir.path().join(CLUSTER_STATE_FILE),
            serde_json::to_vec(&state).unwrap(),
        )
        .unwrap();
        dir
    }

    #[tokio::test]
    async fn v1_requires_migration_and_standalone_keeps_existing_admission() {
        let dir = state_fixture(1, false);
        let graph = dir.path().join("graphs/knowledge.omni");
        assert_eq!(
            acquire_graph_admission(graph.to_str().unwrap(), ClusterAdmissionPurpose::Serve)
                .await
                .unwrap_err()
                .code,
            "ledger_upgrade_required"
        );
        assert!(!dir.path().join("__cluster/lock.json").exists());
        assert!(
            acquire_graph_admission(
                dir.path().join("standalone.omni").to_str().unwrap(),
                ClusterAdmissionPurpose::GraphOperation
            )
            .await
            .unwrap()
            .is_none()
        );
    }

    #[tokio::test]
    async fn v2_admission_is_exclusive_and_abandonment_never_unlocks() {
        let dir = state_fixture(2, false);
        let root = dir.path().to_str().unwrap();
        let owner = acquire_cluster_admission(root, ClusterAdmissionPurpose::GraphOperation)
            .await
            .unwrap()
            .unwrap();
        let lock_id = owner.lock_id().to_string();
        let clone = owner.clone();
        let error = owner.release_after_settlement().await.unwrap_err();
        assert_eq!(error.code, "cluster_admission_in_use");
        assert_eq!(clone.lock_id(), lock_id);
        let blocked = acquire_cluster_admission(root, ClusterAdmissionPurpose::Deployment)
            .await
            .unwrap_err();
        assert_eq!(blocked.code, "state_lock_held");
        drop(clone);
        assert!(dir.path().join("__cluster/lock.json").exists());
        let blocked = acquire_graph_admission(
            dir.path().join("graphs/knowledge.omni").to_str().unwrap(),
            ClusterAdmissionPurpose::GraphOperation,
        )
        .await
        .unwrap_err();
        assert_eq!(blocked.code, "state_lock_held");
    }

    #[tokio::test]
    async fn explicit_settled_release_allows_the_next_owner() {
        let dir = state_fixture(2, false);
        let root = dir.path().to_str().unwrap();
        let owner = acquire_cluster_admission(root, ClusterAdmissionPurpose::GraphOperation)
            .await
            .unwrap()
            .unwrap();
        let graph = dir.path().join("graphs/knowledge.omni");
        assert_eq!(
            owner
                .expected_serving_schema_contract(graph.to_str().unwrap())
                .unwrap_err()
                .code,
            "cluster_admission_purpose_mismatch"
        );
        owner.release_after_settlement().await.unwrap();
        assert!(!dir.path().join("__cluster/lock.json").exists());
        let next = acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
            .await
            .unwrap()
            .unwrap();
        let expected = next
            .expected_serving_schema_contract(graph.to_str().unwrap())
            .unwrap();
        assert_eq!(expected.source_hash, "c".repeat(64));
        // The admitted contract is metadata. Even an absent manifest does not
        // prevent its capture; graph-open errors belong to per-graph startup.
        assert!(!graph.join("__manifest").exists());
        next.release_after_settlement().await.unwrap();
    }

    #[tokio::test]
    async fn outstanding_allows_only_exact_original_reconciliation() {
        let dir = state_fixture(2, true);
        let root = dir.path().to_str().unwrap();
        let state_before = std::fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
        let graph = dir.path().join("graphs/knowledge.omni");
        let refused = crate::GraphReadAuthority::capture(graph.to_str().unwrap(), None)
            .await
            .unwrap_err();
        assert_eq!(refused.code, "cluster_deployment_outstanding");
        assert!(!dir.path().join("__cluster/lock.json").exists());
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            state_before
        );
        for purpose in [
            ClusterAdmissionPurpose::Serve,
            ClusterAdmissionPurpose::GraphOperation,
            ClusterAdmissionPurpose::Deployment,
            ClusterAdmissionPurpose::Reconcile {
                deployment_id: "01K00000000000000000000000:1:01K00000000000000000000002".into(),
            },
        ] {
            let error = acquire_cluster_admission(root, purpose).await.unwrap_err();
            assert_eq!(error.code, "cluster_deployment_outstanding");
            assert!(!dir.path().join("__cluster/lock.json").exists());
        }
        let owner = acquire_cluster_admission(
            root,
            ClusterAdmissionPurpose::Reconcile {
                deployment_id: "01K00000000000000000000000:1:01K00000000000000000000001".into(),
            },
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            std::fs::read(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            state_before
        );
        owner.release_after_settlement().await.unwrap();
        assert_eq!(
            acquire_cluster_admission(root, ClusterAdmissionPurpose::Serve)
                .await
                .unwrap_err()
                .code,
            "cluster_deployment_outstanding"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn graph_aliases_share_admission_and_inventory_cannot_escape_it() {
        let dir = state_fixture(2, false);
        let alias_dir = tempfile::tempdir().unwrap();
        let alias = alias_dir.path().join("any-name");
        std::os::unix::fs::symlink(dir.path().join("graphs/knowledge.omni"), &alias).unwrap();
        let owner = acquire_graph_admission(
            alias.to_str().unwrap(),
            ClusterAdmissionPurpose::GraphOperation,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            acquire_cluster_admission(dir.path().to_str().unwrap(), ClusterAdmissionPurpose::Serve)
                .await
                .unwrap_err()
                .code,
            "state_lock_held"
        );
        owner.release_after_settlement().await.unwrap();
        let unknown = dir.path().join("graphs/unknown.omni");
        assert_eq!(
            acquire_graph_admission(
                unknown.to_str().unwrap(),
                ClusterAdmissionPurpose::GraphOperation
            )
            .await
            .unwrap_err()
            .code,
            "graph_not_applied"
        );
        assert!(!dir.path().join("__cluster/lock.json").exists());

        std::fs::remove_dir(dir.path().join("graphs/knowledge.omni")).unwrap();
        std::os::unix::fs::symlink(alias_dir.path(), dir.path().join("graphs/knowledge.omni"))
            .unwrap();
        assert_eq!(
            acquire_graph_admission(
                dir.path().join("graphs/knowledge.omni").to_str().unwrap(),
                ClusterAdmissionPurpose::GraphOperation
            )
            .await
            .unwrap_err()
            .code,
            "cluster_graph_root_mismatch"
        );
        assert!(!dir.path().join("__cluster/lock.json").exists());
    }
}
