//! A single initial policy deployment with no graph/native-I/O effects.
//! The retained lock is transferred by CAS, never removed or reconstructed.

use super::*;
use crate::admission::ClusterAdmission;
use crate::state_lock::StateLockGuard;
use omnigraph_storage::StorageKind;

#[cfg(test)]
#[path = "bootstrap_fault_tests.rs"]
mod fault_tests;

pub const MAX_BOOTSTRAP_SERVING_RECEIPT_BYTES: usize = 16 * 1024;

/// Exact native authority projected for the first serving owner. This is not
/// a bearer credential: claiming requires storage access and revalidates every
/// binding against the existing native ledger, immutable input and lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapServingReceipt {
    pub version: u32,
    pub canonical_root: String,
    pub bootstrap_lock_id: String,
    pub bootstrap_lock_version: String,
    pub ledger_id: String,
    pub state_revision: u64,
    pub state_cas: String,
    pub deployment_id: String,
    pub input_digest: String,
    pub config_digest: String,
    pub result_revision: u64,
}

impl BootstrapServingReceipt {
    fn validate(&self) -> Result<(), Diagnostic> {
        if self.version != 1
            || self.canonical_root.len() > 4096
            || self.bootstrap_lock_version.is_empty()
            || self.bootstrap_lock_version.len() > 1024
            || self.bootstrap_lock_version.chars().any(char::is_control)
            || Ulid::from_string(&self.bootstrap_lock_id).is_err()
            || Ulid::from_string(&self.ledger_id).is_err()
            || !authorization::valid_digest(&self.input_digest)
            || !authorization::valid_digest(&self.config_digest)
            || !self
                .state_cas
                .strip_prefix("sha256:")
                .is_some_and(authorization::valid_digest)
            || self.state_revision != 3
            || self.result_revision != 1
            || encoded_size(self)? > MAX_BOOTSTRAP_SERVING_RECEIPT_BYTES
        {
            return Err(refusal(
                "bootstrap_receipt_invalid",
                "invalid bootstrap handoff receipt",
            ));
        }
        let id = DeploymentId::parse(&self.deployment_id)?;
        if id.ledger != self.ledger_id || id.sequence != 1 {
            return Err(refusal(
                "bootstrap_receipt_invalid",
                "receipt is not the first deployment",
            ));
        }
        Ok(())
    }
}

/// Initialize one fresh S3 cluster with cluster-scoped management policy only.
/// The private bootstrap owner cannot execute another deployment. Failure or
/// cancellation after acquisition retains its lock; repeat initialization
/// refuses. There is no unlock, deployment replay, graph open or background
/// engine work here. Transport retries keep their original preconditions.
pub async fn bootstrap_serving(
    config_dir: impl AsRef<Path>,
    caller: &DeploymentCaller,
) -> Result<BootstrapServingReceipt, Diagnostic> {
    let bundle = capture_deployment(config_dir)?;
    require_s3(&bundle.canonical_root)?;
    let store = ClusterStore::for_storage_root(&bundle.canonical_root)?;
    bootstrap_in_store(&store, &bundle, caller).await
}

fn require_s3(root: &str) -> Result<(), Diagnostic> {
    if omnigraph_storage::storage_kind_for_uri(root).ok() != Some(StorageKind::S3) {
        return Err(refusal(
            "bootstrap_handoff_backend_unsupported",
            "bootstrap serving handoff requires an S3 backend with native conditional updates",
        ));
    }
    Ok(())
}

fn validate_bootstrap_bundle(
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
) -> Result<(), Diagnostic> {
    execution::validate_bundle(bundle, &bundle.canonical_root)?;
    caller.authority()?;
    if bundle.resources.is_empty()
        || bundle.resources.iter().any(|(address, resource)| {
            !matches!(resource_kind(address), ResourceKind::Policy(_))
                || resource.applies_to.as_deref() != Some(&["cluster".to_owned()][..])
                || resource.embedding_provider.is_some()
                || resource.embedding_profile.is_some()
                || resource.external_blob_policy.is_some()
        })
    {
        return Err(refusal(
            "bootstrap_policy_only",
            "fresh serving bootstrap accepts cluster-scoped management policies only",
        ));
    }
    execution::authorize_bootstrap_bundle(bundle, caller)
}

async fn bootstrap_in_store(
    store: &ClusterStore,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
) -> Result<BootstrapServingReceipt, Diagnostic> {
    validate_bootstrap_bundle(bundle, caller)?;
    store.require_fresh_bootstrap(false).await?;
    let (guard, lock_version) = store.acquire_bootstrap_lock().await?;
    // The lock is retained from this point, even if the second freshness read
    // or any later validation, write or response is interrupted.
    store.require_fresh_bootstrap(true).await?;
    let state = execution::empty_ledger();
    store
        .write_state(&state, None, &mut store.observations())
        .await?;
    let admission = ClusterAdmission::from_bootstrap_lock(store.clone(), guard, None);
    let mut effects_started = false;
    let result = execution::execute_captured_deployment_in_store(
        store,
        bundle,
        None,
        caller,
        &admission,
        &BTreeMap::new(),
        |_, _, _| {},
        |_| {},
        &mut effects_started,
    )
    .await?;
    let DeploymentLookup::Complete { result } = result else {
        return Err(refusal(
            "bootstrap_outcome_unknown",
            "initial deployment is not terminal",
        ));
    };
    let (state, state_cas) = execution::read_existing(store).await?;
    let receipt = BootstrapServingReceipt {
        version: 1,
        canonical_root: bundle.canonical_root.clone(),
        bootstrap_lock_id: admission.lock_id().to_owned(),
        bootstrap_lock_version: lock_version,
        ledger_id: state.ledger_id.clone().unwrap(),
        state_revision: state.state_revision,
        state_cas,
        deployment_id: result.id,
        input_digest: result.input_digest,
        config_digest: bundle.config_digest.clone(),
        result_revision: result.result_revision,
    };
    receipt.validate()?;
    validate_terminal(store, &receipt).await?;
    store
        .verify_bootstrap_lock(&receipt.bootstrap_lock_id, &receipt.bootstrap_lock_version)
        .await?;
    // No effects follow terminal verification. Dropping this private owner
    // retains the original lock; only the exact claim below can replace it.
    Ok(receipt)
}

/// Claim a retained fresh bootstrap lock for this process and return its exact
/// admitted serving snapshot. A failed/lost CAS acknowledgement grants no
/// admission. Replaying a receipt never adopts an already claimed lock.
pub async fn claim_bootstrap_serving(
    root: &str,
    receipt: &BootstrapServingReceipt,
) -> Result<AdmittedServingSnapshot, Vec<Diagnostic>> {
    let result = async {
        receipt.validate()?;
        require_s3(root)?;
        let store = ClusterStore::for_storage_root(root)?;
        if store.canonical_root()? != receipt.canonical_root {
            return Err(refusal(
                "bootstrap_root_mismatch",
                "selected root differs from the handoff receipt",
            ));
        }
        claim_in_store(&store, receipt).await
    }
    .await;
    result.map_err(|error| vec![error])
}

async fn claim_in_store(
    store: &ClusterStore,
    receipt: &BootstrapServingReceipt,
) -> Result<AdmittedServingSnapshot, Diagnostic> {
    receipt.validate()?;
    let result = validate_terminal(store, receipt).await?;
    let guard: StateLockGuard = store
        .claim_bootstrap_lock(&receipt.bootstrap_lock_id, &receipt.bootstrap_lock_version)
        .await?;
    // There is intentionally no read/adopt fallback after an uncertain CAS.
    let admission = ClusterAdmission::from_bootstrap_lock(store.clone(), guard, Some(result));
    validate_terminal(store, receipt).await?;
    let snapshot = serve::read_snapshot_with_store(store)
        .await
        .map_err(|mut errors| errors.remove(0))?;
    if !snapshot.graphs.is_empty()
        || !snapshot.applied_graphs.is_empty()
        || !snapshot.queries.is_empty()
        || !snapshot.diagnostics.is_empty()
        || snapshot.state_revision != receipt.state_revision
        || snapshot.state_cas.as_deref() != Some(receipt.state_cas.as_str())
        || snapshot.config_digest.as_deref() != Some(receipt.config_digest.as_str())
    {
        return Err(refusal(
            "bootstrap_snapshot_mismatch",
            "claimed bootstrap snapshot changed",
        ));
    }
    Ok(AdmittedServingSnapshot::from_bootstrap(
        snapshot,
        receipt.canonical_root.clone(),
        admission,
    ))
}

async fn validate_terminal(
    store: &ClusterStore,
    receipt: &BootstrapServingReceipt,
) -> Result<DeploymentResult, Diagnostic> {
    if store.canonical_root()? != receipt.canonical_root {
        return Err(refusal("bootstrap_root_mismatch", "bootstrap root changed"));
    }
    let (state, cas) = execution::read_existing(store).await?;
    let result = state
        .deployment_results
        .as_ref()
        .filter(|results| results.len() == 1)
        .and_then(|results| results.first())
        .ok_or_else(|| {
            refusal(
                "bootstrap_state_mismatch",
                "expected exactly one terminal bootstrap result",
            )
        })?;
    if state.version != 2
        || state.ledger_id.as_deref() != Some(receipt.ledger_id.as_str())
        || state.state_revision != receipt.state_revision
        || cas != receipt.state_cas
        || state.next_sequence != Some(2)
        || state.outstanding.is_some()
        || !state
            .applied_revision
            .schema_contracts
            .as_ref()
            .is_some_and(BTreeMap::is_empty)
        || state.applied_revision.result_revision != Some(receipt.result_revision)
        || state.applied_revision.config_digest.as_deref() != Some(receipt.config_digest.as_str())
        || result.id != receipt.deployment_id
        || result.input_digest != receipt.input_digest
        || result.config_digest.as_deref() != Some(receipt.config_digest.as_str())
        || result.result_revision != receipt.result_revision
        || !result.converged
        || !result.graphs.is_empty()
        || !result.recovery_executors.is_empty()
        || result.base.result_revision != 0
        || !result.base.resource_digests.is_empty()
        || !result.base.schema_contracts.is_empty()
        || !state.recovery_records.is_empty()
        || !state.approval_records.is_empty()
    {
        return Err(refusal(
            "bootstrap_state_mismatch",
            "native state differs from the exact initial policy deployment",
        ));
    }
    let bundle = store.read_deployment_bundle(&receipt.input_digest).await?;
    // The result's authority remains attribution, not a fresh identity. Input
    // authorization occurred before bootstrap; claiming storage ownership
    // does not execute or impersonate that initiating actor.
    validate_bootstrap_bundle(&bundle, &DeploymentCaller::storage_owner(None))?;
    if bundle.config_digest != receipt.config_digest
        || bundle.canonical_root != receipt.canonical_root
        || serde_json::to_value(&bundle.resources).unwrap()
            != serde_json::to_value(&state.applied_revision.resources).unwrap()
    {
        return Err(refusal(
            "bootstrap_state_mismatch",
            "bootstrap input and achieved policy differ",
        ));
    }
    authorization::AppliedPolicies::load(store, &state).await?;
    store.require_no_bootstrap_graphs_or_recovery().await?;
    Ok(result.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_storage::StorageAdapter;
    use std::sync::Arc;

    pub(super) const ROOT: &str = "s3://bootstrap-tests/root";
    const POLICY: &str = "version: 1\ngroups:\n  owners: [principal:owner]\nrules:\n  - id: configure\n    allow: {actors: {group: owners}, actions: [config_manage]}\n";

    pub(super) fn fixture() -> (
        tempfile::TempDir,
        CapturedDeployment,
        ClusterStore,
        Arc<dyn StorageAdapter>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("policy.yaml"), POLICY).unwrap();
        fs::write(dir.path().join(CLUSTER_CONFIG_FILE), format!(
            "version: 1\nstorage: {ROOT}\npolicies:\n  management:\n    file: policy.yaml\n    applies_to: [cluster]\n"
        )).unwrap();
        let bundle = capture_deployment(dir.path()).unwrap();
        let adapter = super::fault_tests::memory_adapter();
        let store = ClusterStore::for_storage_root(ROOT)
            .unwrap()
            .with_bootstrap_test_adapter(adapter.clone());
        (dir, bundle, store, adapter)
    }

    pub(super) fn owner() -> DeploymentCaller {
        DeploymentCaller::storage_owner(Some("principal:owner".into()))
    }

    pub(super) async fn bytes(adapter: &Arc<dyn StorageAdapter>, name: &str) -> String {
        adapter.read_text(&format!("{ROOT}/{name}")).await.unwrap()
    }

    #[tokio::test]
    async fn bootstrap_claim_preserves_exact_result_and_state_without_unlock() {
        let (_dir, bundle, store, adapter) = fixture();
        adapter
            .write_text(&format!("{ROOT}/application/opaque.json"), "opaque")
            .await
            .unwrap();
        adapter
            .write_text(
                &format!("{ROOT}/__cluster/application_metadata.json"),
                "opaque",
            )
            .await
            .unwrap();
        let receipt = bootstrap_in_store(&store, &bundle, &owner()).await.unwrap();
        assert_eq!(receipt.input_digest, bundle.input_digest().unwrap());
        let state = bytes(&adapter, CLUSTER_STATE_FILE).await;
        let before = bytes(&adapter, CLUSTER_LOCK_FILE).await;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&before).unwrap()["operation"],
            "bootstrap_serving"
        );
        let captured = claim_in_store(&store, &receipt).await.unwrap();
        let (snapshot, root, admission) = captured.into_parts();
        assert_eq!(root, ROOT);
        assert!(snapshot.graphs.is_empty());
        assert_eq!(
            snapshot.state_cas.as_deref(),
            Some(receipt.state_cas.as_str())
        );
        let admission = admission.unwrap();
        admission.validate_serving().unwrap();
        assert_ne!(admission.lock_id(), receipt.bootstrap_lock_id);
        assert_eq!(bytes(&adapter, CLUSTER_STATE_FILE).await, state);
        let lock = bytes(&adapter, CLUSTER_LOCK_FILE).await;
        assert_ne!(lock, before);
        drop(admission);
        assert_eq!(bytes(&adapter, CLUSTER_LOCK_FILE).await, lock);
        assert!(claim_in_store(&store, &receipt).await.is_err());
        assert!(bootstrap_in_store(&store, &bundle, &owner()).await.is_err());
        assert_eq!(bytes(&adapter, CLUSTER_STATE_FILE).await, state);
        assert_eq!(bytes(&adapter, CLUSTER_LOCK_FILE).await, lock);
    }

    #[tokio::test]
    async fn receipt_mismatch_and_native_residue_refuse_without_claiming() {
        let (_dir, bundle, store, adapter) = fixture();
        let receipt = bootstrap_in_store(&store, &bundle, &owner()).await.unwrap();
        let before = bytes(&adapter, CLUSTER_LOCK_FILE).await;
        for changed in [
            BootstrapServingReceipt {
                canonical_root: "s3://bootstrap-tests/other".into(),
                ..receipt.clone()
            },
            BootstrapServingReceipt {
                input_digest: "0".repeat(64),
                ..receipt.clone()
            },
            BootstrapServingReceipt {
                bootstrap_lock_version: "unknown".into(),
                ..receipt.clone()
            },
            BootstrapServingReceipt {
                state_revision: 4,
                ..receipt.clone()
            },
            BootstrapServingReceipt {
                version: 2,
                ..receipt.clone()
            },
        ] {
            assert!(claim_in_store(&store, &changed).await.is_err());
            assert_eq!(bytes(&adapter, CLUSTER_LOCK_FILE).await, before);
        }
        adapter
            .write_text(
                &format!("{ROOT}/graphs/unrecorded.omni/__manifest/data"),
                "residue",
            )
            .await
            .unwrap();
        assert!(claim_in_store(&store, &receipt).await.is_err());
        assert_eq!(bytes(&adapter, CLUSTER_LOCK_FILE).await, before);
        let mut value = serde_json::to_value(&receipt).unwrap();
        value["extra"] = serde_json::json!(true);
        assert!(serde_json::from_value::<BootstrapServingReceipt>(value).is_err());
    }

    #[tokio::test]
    async fn bootstrap_rejects_existing_native_authority_and_unauthorized_identity() {
        for path in [
            CLUSTER_STATE_FILE,
            CLUSTER_LOCK_FILE,
            "__cluster/resources/unknown",
            "__cluster/recoveries/old.json",
            "graphs/old.omni/data",
            "__manifest/_versions/1.manifest",
        ] {
            let (_dir, bundle, store, adapter) = fixture();
            adapter
                .write_text(&format!("{ROOT}/{path}"), "existing")
                .await
                .unwrap();
            assert!(
                bootstrap_in_store(&store, &bundle, &owner()).await.is_err(),
                "{path}"
            );
            assert_eq!(
                adapter.read_text(&format!("{ROOT}/{path}")).await.unwrap(),
                "existing"
            );
            if path != CLUSTER_LOCK_FILE {
                assert!(
                    !adapter
                        .exists(&format!("{ROOT}/{CLUSTER_LOCK_FILE}"))
                        .await
                        .unwrap()
                );
            }
        }
        let (dir, bundle, store, adapter) = fixture();
        let denied = DeploymentCaller::AuthenticatedIdentity(
            IdentityAuthorization::bootstrap_config_dir("principal:denied", dir.path()).unwrap(),
        );
        assert!(bootstrap_in_store(&store, &bundle, &denied).await.is_err());
        let allowed = DeploymentCaller::AuthenticatedIdentity(
            IdentityAuthorization::bootstrap_config_dir("principal:owner", dir.path()).unwrap(),
        );
        validate_bootstrap_bundle(&bundle, &allowed).unwrap();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            format!("version: 1\nstorage: {ROOT}\n"),
        )
        .unwrap();
        let empty = capture_deployment(dir.path()).unwrap();
        assert_eq!(
            bootstrap_in_store(&store, &empty, &owner())
                .await
                .unwrap_err()
                .code,
            "bootstrap_policy_only"
        );
        fs::write(
            dir.path().join("people.pg"),
            "node Person { name: String @key }\n",
        )
        .unwrap();
        fs::write(dir.path().join(CLUSTER_CONFIG_FILE), format!(
            "version: 1\nstorage: {ROOT}\ngraphs:\n  people:\n    schema: people.pg\npolicies:\n  management:\n    file: policy.yaml\n    applies_to: [cluster]\n"
        )).unwrap();
        let graphs = capture_deployment(dir.path()).unwrap();
        assert_eq!(
            bootstrap_in_store(&store, &graphs, &owner())
                .await
                .unwrap_err()
                .code,
            "bootstrap_policy_only"
        );
        assert!(!adapter.exists(&format!("{ROOT}/__cluster")).await.unwrap());
        assert!(!adapter.exists(&format!("{ROOT}/graphs")).await.unwrap());
        assert!(require_s3("file:///tmp/bootstrap").is_err());
        assert!(require_s3("az://container/root").is_err());
    }

    #[tokio::test]
    async fn changed_policy_payload_refuses_before_lock_claim() {
        let (_dir, bundle, store, adapter) = fixture();
        let receipt = bootstrap_in_store(&store, &bundle, &owner()).await.unwrap();
        let lock = bytes(&adapter, CLUSTER_LOCK_FILE).await;
        let digest = &bundle.resources["policy.management"].digest;
        adapter
            .write_text(
                &format!("{ROOT}/__cluster/resources/policy/management/{digest}.yaml"),
                "corrupt policy payload",
            )
            .await
            .unwrap();
        assert!(claim_in_store(&store, &receipt).await.is_err());
        assert_eq!(bytes(&adapter, CLUSTER_LOCK_FILE).await, lock);
    }
}
