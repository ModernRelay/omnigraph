//! A single initial policy deployment with no graph/native-I/O effects.
//! The retained lock is transferred by CAS, never removed or reconstructed.
//!
//! First bootstrap runs as named steps (`BootstrapStep`). Its progress has two
//! records and no others: `lock.json`, which names the bootstrap identity, and
//! the ledger `state.json`. A rerun of the same identity, whose caller asserts
//! that every earlier attempt is excluded, re-enters its own retained lock by
//! exact-version CAS, reads the ledger and continues from the one state of it
//! that this bootstrap's own steps leave (`progress`); every other state
//! refuses and changes nothing.

use super::*;
use crate::admission::ClusterAdmission;
use crate::state_lock::{BootstrapLockState, StateLockGuard};
use crate::store::BootstrapProbeDeadline;
use omnigraph_storage::{StorageFailureKind, StorageKind};
use std::sync::Arc;

use super::write_effects::{EffectLog, RecordingAdapter, WriteEffects};

#[cfg(test)]
#[path = "bootstrap_fault_tests.rs"]
mod fault_tests;

pub const MAX_BOOTSTRAP_SERVING_RECEIPT_BYTES: usize = 16 * 1024;

/// Who a first bootstrap belongs to: the caller's cluster incarnation and,
/// when it has one, the request that started it. Omnigraph compares them and
/// never interprets them. A retained bootstrap lock is re-entered only by the
/// identity it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapIdentity {
    incarnation: String,
    request_id: Option<String>,
}

impl BootstrapIdentity {
    /// Each field is 1 to 128 bytes of ASCII letters, digits, `.`, `_`, `:`
    /// and `-`.
    pub fn new(
        incarnation: impl Into<String>,
        request_id: Option<String>,
    ) -> Result<Self, Diagnostic> {
        let identity = Self {
            incarnation: incarnation.into(),
            request_id,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    fn validate(&self) -> Result<(), Diagnostic> {
        if !crate::state_lock::valid_bootstrap_identity_field(&self.incarnation)
            || !self
                .request_id
                .as_deref()
                .is_none_or(crate::state_lock::valid_bootstrap_identity_field)
        {
            return Err(Diagnostic::error(
                "bootstrap_identity_invalid",
                CLUSTER_LOCK_FILE,
                "a bootstrap incarnation and request id are 1 to 128 bytes of ASCII letters, digits, '.', '_', ':' and '-'",
            ));
        }
        Ok(())
    }
}

/// The caller's statement about earlier attempts of the same identity. It has
/// no default: the caller decides, and Omnigraph never infers it from time,
/// heartbeats or process ids.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorAttempts {
    /// Every earlier process of this identity has terminated and sends
    /// nothing more; the caller proved it (for example, by observing the
    /// process exit). Only this permits re-entering a retained lock.
    Excluded,
    /// The caller cannot prove that. A retained lock of this identity refuses
    /// with `bootstrap_exclusion_required` and nothing changes.
    NotExcluded,
}

/// One first-bootstrap request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRequest {
    pub identity: BootstrapIdentity,
    pub prior_attempts: PriorAttempts,
}

/// The named steps of first bootstrap, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapStep {
    /// Capture, root and caller validation; no storage request.
    Input,
    /// Create, or re-enter by exact-version CAS, the bootstrap lock.
    Lock,
    /// Effect-free probes that the root holds no native authority or data.
    Freshness,
    /// Create the empty ledger.
    Ledger,
    /// The initial cluster-scoped policy deployment.
    Deployment,
    /// Verify the terminal state and return the serving receipt.
    Receipt,
}

/// The fixed-vocabulary outcome of a failed bootstrap, for a caller that must
/// act on it without reading the message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapOutcome {
    pub code: String,
    pub step: BootstrapStep,
    pub effects: WriteEffects,
    pub retryable: bool,
}

/// A failed bootstrap: its diagnostic, the step that failed and the effects
/// of the writes the attempt sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapFailure {
    diagnostic: Diagnostic,
    step: BootstrapStep,
    effects: WriteEffects,
    /// The storage class of the failed request: the diagnostic's, for a read,
    /// or the recording adapter's, for a write refused without applying.
    storage_failure: Option<StorageFailureKind>,
    /// This attempt's initial deployment was accepted and did not complete,
    /// so a rerun meets it outstanding and refuses.
    deployment_outstanding: bool,
}

impl BootstrapFailure {
    fn new(diagnostic: Diagnostic, step: BootstrapStep, effects: WriteEffects) -> Self {
        Self {
            storage_failure: diagnostic.storage_failure,
            deployment_outstanding: false,
            diagnostic,
            step,
            effects,
        }
    }

    pub fn diagnostic(&self) -> &Diagnostic {
        &self.diagnostic
    }

    pub fn into_diagnostic(self) -> Diagnostic {
        self.diagnostic
    }

    pub fn code(&self) -> &str {
        &self.diagnostic.code
    }

    pub fn step(&self) -> BootstrapStep {
        self.step
    }

    pub fn effects(&self) -> WriteEffects {
        self.effects
    }

    /// A rerun of the same request, with `PriorAttempts::Excluded` once the
    /// failed process is gone, can make progress: no write has an unknown
    /// outcome, the attempt left no accepted initial deployment incomplete
    /// (a rerun refuses that state), and the failure is a storage request (a
    /// read, a probe, or a write refused without applying) whose class is
    /// transient or unclassified. Advice only; the rerun's checks decide.
    pub fn retryable(&self) -> bool {
        self.effects != WriteEffects::Unconfirmed
            && !self.deployment_outstanding
            && matches!(
                self.storage_failure,
                Some(StorageFailureKind::Transient | StorageFailureKind::Unknown)
            )
    }

    pub fn outcome(&self) -> BootstrapOutcome {
        BootstrapOutcome {
            code: self.diagnostic.code.clone(),
            step: self.step,
            effects: self.effects,
            retryable: self.retryable(),
        }
    }
}

impl std::fmt::Display for BootstrapFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}: {} (step {:?}, effects {:?})",
            self.diagnostic.code, self.diagnostic.message, self.step, self.effects
        )
    }
}

impl std::error::Error for BootstrapFailure {}

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
            || !valid_state_cas(&self.state_cas)
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

fn valid_state_cas(cas: &str) -> bool {
    cas.strip_prefix("sha256:")
        .is_some_and(authorization::valid_digest)
}

/// What `state.json` shows of this bootstrap's initial deployment once it is
/// complete: every field the serving receipt takes from the ledger.
struct Deployed {
    ledger_id: String,
    state_revision: u64,
    state_cas: String,
    deployment_id: String,
    config_digest: String,
    result_revision: u64,
}

/// Where a bootstrap of one input stands, read from `state.json` alone.
enum Progress {
    /// No ledger: check freshness, create the ledger, then deploy.
    Freshness,
    /// Exactly the empty ledger at revision 1, which under this identity's
    /// lock only this bootstrap's own create can leave: deploy.
    Deployment,
    /// The initial deployment of this input is complete: verify and return.
    Receipt(Deployed),
}

/// `state` is exactly the ledger `execution::empty_ledger` creates.
fn is_empty_ledger(state: &ClusterState) -> bool {
    let empty = ClusterState {
        ledger_id: state.ledger_id.clone(),
        ..execution::empty_ledger()
    };
    matches!(
        (serde_json::to_value(state), serde_json::to_value(&empty)),
        (Ok(found), Ok(expected)) if found == expected
    )
}

/// The one rule for the state a rerun refuses (`bootstrap_deployment_outstanding`):
/// this input's initial deployment accepted in the ledger and not complete. A
/// rerun applies it to the ledger it reads; an attempt applies it to the
/// ledger entry its acceptance wrote, so `retryable` never advises a rerun
/// that would refuse.
fn own_outstanding(lookup: &DeploymentLookup, input_digest: &str) -> bool {
    matches!(
        lookup,
        DeploymentLookup::Outstanding { input_digest: pending, .. } if pending.as_str() == input_digest
    )
}

/// The rerun decision table. It reads nothing but the ledger snapshot; the
/// lock was read, and attributed to this identity, before. Each row is a
/// state this bootstrap's own steps leave, so no other process's effect is
/// adopted:
///
/// | `state.json` | Decision |
/// |---|---|
/// | absent | `Freshness`: repeat the read-only check, create the ledger |
/// | exactly the empty v2 ledger at revision 1 | `Deployment` |
/// | revision 3, one converged result of this input, nothing outstanding | `Receipt` |
/// | revision 2, sequence 1 of this input outstanding | refuse `bootstrap_deployment_outstanding` |
/// | anything else | refuse `bootstrap_state_mismatch` |
///
/// `validate_terminal` then checks every remaining field of a `Receipt`.
fn progress(
    snapshot: &crate::store::StateSnapshot,
    input_digest: &str,
    step: &mut BootstrapStep,
) -> Result<Progress, Diagnostic> {
    let (Some(state), Some(state_cas)) = (&snapshot.state, &snapshot.state_cas) else {
        *step = BootstrapStep::Freshness;
        return Ok(Progress::Freshness);
    };
    *step = BootstrapStep::Deployment;
    if is_empty_ledger(state) {
        return Ok(Progress::Deployment);
    }
    let first = |id: &str| DeploymentId::parse(id).is_ok_and(|id| id.sequence == 1);
    if let Some(pending) = state
        .outstanding
        .as_ref()
        .filter(|_| state.state_revision == 2)
    {
        if first(&pending.id)
            && own_outstanding(&execution::lookup(state, &pending.id)?, input_digest)
        {
            return Err(Diagnostic::error(
                "bootstrap_deployment_outstanding",
                CLUSTER_STATE_FILE,
                format!(
                    "the initial deployment {} was accepted and its outcome is not recorded; first bootstrap does not reconcile it",
                    pending.id
                ),
            ));
        }
    }
    let complete = match state.deployment_results.as_deref() {
        Some([result])
            if state.version == 2
                && state.state_revision == 3
                && state.outstanding.is_none()
                && first(&result.id) =>
        {
            match execution::lookup(state, &result.id)? {
                DeploymentLookup::Complete { result }
                    if result.converged && result.input_digest == input_digest =>
                {
                    Some(result)
                }
                _ => None,
            }
        }
        _ => None,
    };
    match (complete, state.ledger_id.as_ref()) {
        (
            Some(DeploymentResult {
                id,
                config_digest: Some(config_digest),
                result_revision,
                ..
            }),
            Some(ledger_id),
        ) => {
            *step = BootstrapStep::Receipt;
            Ok(Progress::Receipt(Deployed {
                ledger_id: ledger_id.clone(),
                state_revision: state.state_revision,
                state_cas: state_cas.clone(),
                deployment_id: id,
                config_digest,
                result_revision,
            }))
        }
        _ => {
            if state.state_revision == 1 {
                *step = BootstrapStep::Ledger;
            }
            Err(refusal(
                "bootstrap_state_mismatch",
                "the ledger is not a state this bootstrap's own steps leave; nothing is adopted",
            ))
        }
    }
}

/// Read the ledger and decide where this bootstrap stands. Run before a lock
/// re-entry, so a rerun that would refuse changes nothing, and again under
/// the lock, where its answer is the one acted on.
async fn inspect(
    store: &ClusterStore,
    input_digest: &str,
    step: &mut BootstrapStep,
) -> Result<Progress, Diagnostic> {
    let snapshot = store.read_state(&mut store.observations()).await?;
    progress(&snapshot, input_digest, step)
}

/// Initialize one fresh S3 cluster with cluster-scoped management policy only.
/// The private bootstrap owner cannot execute another deployment. Failure or
/// cancellation after acquisition retains its lock. Only a rerun of the same
/// identity whose caller asserts `PriorAttempts::Excluded` re-enters it, and
/// it continues from the state the ledger records. There is no unlock, graph
/// open or background engine work here. Transport retries keep their
/// original preconditions.
pub async fn bootstrap_serving(
    config_dir: impl AsRef<Path>,
    caller: &DeploymentCaller,
    request: &BootstrapRequest,
) -> Result<BootstrapServingReceipt, BootstrapFailure> {
    let before_storage =
        |diagnostic| BootstrapFailure::new(diagnostic, BootstrapStep::Input, WriteEffects::None);
    let bundle = capture_deployment(config_dir).map_err(before_storage)?;
    require_s3(&bundle.canonical_root).map_err(before_storage)?;
    let store = ClusterStore::for_storage_root(&bundle.canonical_root)
        .and_then(|store| store.with_io_scope(omnigraph_storage::StorageIoScope::new()))
        .map_err(before_storage)?;
    bootstrap_in_store(&store, &bundle, caller, request).await
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

/// One attempt, with every control request recorded so a failure states the
/// effects of what was actually sent.
async fn bootstrap_in_store(
    store: &ClusterStore,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
    request: &BootstrapRequest,
) -> Result<BootstrapServingReceipt, BootstrapFailure> {
    let log = Arc::new(EffectLog::default());
    let recorded = store.clone().with_wrapped_adapter(|inner| {
        Arc::new(RecordingAdapter::new(inner, log.clone()))
            as Arc<dyn omnigraph_storage::StorageAdapter>
    });
    let mut step = BootstrapStep::Input;
    let mut deployment_outstanding = false;
    bootstrap_attempt(
        &recorded,
        bundle,
        caller,
        request,
        &mut step,
        &mut deployment_outstanding,
    )
    .await
    .map_err(|diagnostic| {
        let scope_uncertain = recorded
            .io_scope()
            .is_some_and(|scope| scope.is_uncertain());
        let effects = log.effects().max(if scope_uncertain {
            WriteEffects::Unconfirmed
        } else {
            WriteEffects::None
        });
        let mut failure = BootstrapFailure::new(diagnostic, step, effects);
        failure.storage_failure = failure.storage_failure.or(log.refused_write());
        failure.deployment_outstanding = deployment_outstanding;
        failure
    })
}

fn not_fresh(reason: &str) -> Diagnostic {
    Diagnostic::error(
        "bootstrap_root_not_fresh",
        CLUSTER_LOCK_FILE,
        format!("existing native authority or graph data prevents initial bootstrap: {reason}"),
    )
}

async fn bootstrap_attempt(
    store: &ClusterStore,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
    request: &BootstrapRequest,
    step: &mut BootstrapStep,
    deployment_outstanding: &mut bool,
) -> Result<BootstrapServingReceipt, Diagnostic> {
    // The one probe budget of this attempt: every freshness and
    // graph-or-recovery probe below shares it.
    let deadline = BootstrapProbeDeadline::start();
    *step = BootstrapStep::Input;
    let identity = &request.identity;
    identity.validate()?;
    validate_bootstrap_bundle(bundle, caller)?;
    let input_digest = bundle.input_digest()?;

    *step = BootstrapStep::Lock;
    let observed = store
        .read_bootstrap_lock_state(identity.incarnation(), identity.request_id())
        .await?;
    let (guard, lock_version) = match observed {
        BootstrapLockState::Absent => {
            *step = BootstrapStep::Freshness;
            store.require_fresh_bootstrap(false, deadline).await?;
            *step = BootstrapStep::Lock;
            store
                .acquire_bootstrap_lock(identity.incarnation(), identity.request_id())
                .await?
                .ok_or_else(|| not_fresh("another lock was created before this attempt's"))?
        }
        BootstrapLockState::Own { lock, version } => {
            if request.prior_attempts != PriorAttempts::Excluded {
                return Err(Diagnostic::error(
                    "bootstrap_exclusion_required",
                    CLUSTER_LOCK_FILE,
                    "this bootstrap identity's earlier attempt retains the lock; rerun with PriorAttempts::Excluded only after proving that attempt's process terminated",
                ));
            }
            // A rerun that would refuse below refuses here, before the lock
            // changes.
            inspect(store, &input_digest, step).await?;
            *step = BootstrapStep::Lock;
            store
                .reenter_bootstrap_lock(&lock, &version)
                .await?
                .ok_or_else(|| {
                    Diagnostic::error(
                        "bootstrap_lock_changed",
                        CLUSTER_LOCK_FILE,
                        "the retained bootstrap lock changed after it was read; nothing was re-entered",
                    )
                })?
        }
        BootstrapLockState::Other(reason) => return Err(not_fresh(reason)),
    };
    // The lock is retained from this point, even if a later read, write or
    // response is interrupted.
    let admission = ClusterAdmission::from_bootstrap_lock(store.clone(), guard, None);

    let mut progress = inspect(store, &input_digest, step).await?;
    let deployed = loop {
        progress = match progress {
            Progress::Freshness => {
                *step = BootstrapStep::Freshness;
                store.require_fresh_bootstrap(true, deadline).await?;
                *step = BootstrapStep::Ledger;
                store
                    .write_state(&execution::empty_ledger(), None, &mut store.observations())
                    .await?;
                Progress::Deployment
            }
            Progress::Deployment => {
                *step = BootstrapStep::Deployment;
                store
                    .require_no_bootstrap_graphs_or_recovery(deadline)
                    .await?;
                let mut effects_started = false;
                let result = execution::execute_captured_deployment_in_store(
                    store,
                    bundle,
                    None,
                    caller,
                    &admission,
                    &BTreeMap::new(),
                    |_, _, _| {},
                    |accepted| *deployment_outstanding = own_outstanding(&accepted, &input_digest),
                    &mut effects_started,
                )
                .await?;
                *deployment_outstanding = false;
                if !matches!(result, DeploymentLookup::Complete { .. }) {
                    return Err(refusal(
                        "bootstrap_outcome_unknown",
                        "initial deployment is not terminal",
                    ));
                }
                // The ledger, not the returned result, is the record acted on.
                match inspect(store, &input_digest, step).await? {
                    complete @ Progress::Receipt(_) => complete,
                    _ => {
                        return Err(refusal(
                            "bootstrap_state_mismatch",
                            "native state differs from the exact initial policy deployment",
                        ));
                    }
                }
            }
            Progress::Receipt(deployed) => break deployed,
        }
    };

    *step = BootstrapStep::Receipt;
    let receipt = BootstrapServingReceipt {
        version: 1,
        canonical_root: bundle.canonical_root.clone(),
        bootstrap_lock_id: admission.lock_id().to_owned(),
        bootstrap_lock_version: lock_version,
        ledger_id: deployed.ledger_id,
        state_revision: deployed.state_revision,
        state_cas: deployed.state_cas,
        deployment_id: deployed.deployment_id,
        input_digest,
        config_digest: deployed.config_digest,
        result_revision: deployed.result_revision,
    };
    receipt.validate()?;
    validate_terminal(store, &receipt, deadline).await?;
    store
        .verify_bootstrap_lock(&receipt.bootstrap_lock_id, &receipt.bootstrap_lock_version)
        .await?;
    require_settled_io(store).await?;
    // No effects follow terminal verification. Dropping this private owner
    // retains the lock; only the exact claim below can replace it.
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
        let store = ClusterStore::for_storage_root(root)?
            .with_io_scope(omnigraph_storage::StorageIoScope::new())?;
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
    // A claim is not an attempt; its own probes share one budget.
    let deadline = BootstrapProbeDeadline::start();
    let result = validate_terminal(store, receipt, deadline).await?;
    let guard: StateLockGuard = store
        .claim_bootstrap_lock(&receipt.bootstrap_lock_id, &receipt.bootstrap_lock_version)
        .await?;
    // There is intentionally no read/adopt fallback after an uncertain CAS.
    let admission = ClusterAdmission::from_bootstrap_lock(store.clone(), guard, Some(result));
    validate_terminal(store, receipt, deadline).await?;
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
    require_settled_io(store).await?;
    Ok(AdmittedServingSnapshot::from_bootstrap(
        snapshot,
        receipt.canonical_root.clone(),
        admission,
    ))
}

async fn require_settled_io(store: &ClusterStore) -> Result<(), Diagnostic> {
    if let Some(scope) = store.io_scope() {
        scope.wait_idle().await;
        if scope.is_uncertain() {
            return Err(refusal(
                "bootstrap_io_uncertain",
                "a native storage request has an uncertain outcome; retaining ownership",
            ));
        }
    }
    Ok(())
}

async fn validate_terminal(
    store: &ClusterStore,
    receipt: &BootstrapServingReceipt,
    deadline: BootstrapProbeDeadline,
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
    store
        .require_no_bootstrap_graphs_or_recovery(deadline)
        .await?;
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

    pub(super) fn identity() -> BootstrapIdentity {
        BootstrapIdentity::new("inc-1", Some("req-1".into())).unwrap()
    }

    /// A first attempt, whose caller cannot assert exclusion.
    pub(super) fn request() -> BootstrapRequest {
        BootstrapRequest {
            identity: identity(),
            prior_attempts: PriorAttempts::NotExcluded,
        }
    }

    /// A rerun whose caller proved the earlier attempt's process terminated.
    pub(super) fn resume() -> BootstrapRequest {
        BootstrapRequest {
            identity: identity(),
            prior_attempts: PriorAttempts::Excluded,
        }
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
        let receipt = bootstrap_in_store(&store, &bundle, &owner(), &request())
            .await
            .unwrap();
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
        assert!(
            bootstrap_in_store(&store, &bundle, &owner(), &request())
                .await
                .is_err()
        );
        assert_eq!(bytes(&adapter, CLUSTER_STATE_FILE).await, state);
        assert_eq!(bytes(&adapter, CLUSTER_LOCK_FILE).await, lock);
    }

    #[tokio::test]
    async fn receipt_mismatch_and_native_residue_refuse_without_claiming() {
        let (_dir, bundle, store, adapter) = fixture();
        let receipt = bootstrap_in_store(&store, &bundle, &owner(), &request())
            .await
            .unwrap();
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
                bootstrap_in_store(&store, &bundle, &owner(), &request())
                    .await
                    .is_err(),
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
        assert!(
            bootstrap_in_store(&store, &bundle, &denied, &request())
                .await
                .is_err()
        );
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
            bootstrap_in_store(&store, &empty, &owner(), &request())
                .await
                .unwrap_err()
                .code(),
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
            bootstrap_in_store(&store, &graphs, &owner(), &request())
                .await
                .unwrap_err()
                .code(),
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
        let receipt = bootstrap_in_store(&store, &bundle, &owner(), &request())
            .await
            .unwrap();
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
