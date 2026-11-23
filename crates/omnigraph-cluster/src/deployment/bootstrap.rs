//! A single initial policy deployment with no graph/native-I/O effects.
//! The retained lock is transferred by CAS, never removed or reconstructed.
//!
//! First bootstrap runs as named steps (`BootstrapStep`). The lock step's
//! record is the lock itself, which names its bootstrap identity; each later
//! step with a storage effect writes a step receipt under
//! `__cluster/bootstrap/` once its effect is confirmed. A rerun of the same
//! identity, whose caller asserts that every earlier attempt is excluded,
//! re-enters its own retained lock by exact-version CAS, verifies the
//! receipts and continues at the first step without one. It replays a step
//! only when the step is idempotent by construction and refuses
//! `bootstrap_step_unconfirmed` otherwise.

use super::*;
use crate::admission::ClusterAdmission;
use crate::state_lock::{BootstrapLockState, StateLockGuard};
use omnigraph_storage::{StorageFailureKind, StorageKind};
use std::sync::Arc;

#[path = "bootstrap_effects.rs"]
mod effects;
pub use effects::BootstrapEffects;
use effects::{EffectLog, RecordingAdapter};

#[cfg(test)]
#[path = "bootstrap_fault_tests.rs"]
mod fault_tests;

pub const MAX_BOOTSTRAP_SERVING_RECEIPT_BYTES: usize = 16 * 1024;
/// Bound on every step receipt under `__cluster/bootstrap/`.
pub const MAX_BOOTSTRAP_STEP_RECEIPT_BYTES: usize = 4 * 1024;
const FRESHNESS_RECEIPT: &str = "__cluster/bootstrap/freshness.json";
const LEDGER_RECEIPT: &str = "__cluster/bootstrap/ledger.json";
const DEPLOYMENT_RECEIPT: &str = "__cluster/bootstrap/deployment.json";

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
    pub effects: BootstrapEffects,
    pub retryable: bool,
}

/// A failed bootstrap: its diagnostic, the step that failed and the effects
/// of the writes the attempt sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapFailure {
    diagnostic: Diagnostic,
    step: BootstrapStep,
    effects: BootstrapEffects,
}

impl BootstrapFailure {
    fn new(diagnostic: Diagnostic, step: BootstrapStep, effects: BootstrapEffects) -> Self {
        Self {
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

    pub fn effects(&self) -> BootstrapEffects {
        self.effects
    }

    /// A rerun of the same request, with `PriorAttempts::Excluded` once the
    /// failed process is gone, can make progress: no write has an unknown
    /// outcome, and the failure is a storage read or probe whose class is
    /// transient or unclassified. Advice only; the rerun's checks decide.
    pub fn retryable(&self) -> bool {
        self.effects != BootstrapEffects::Unconfirmed
            && matches!(
                self.diagnostic.storage_failure,
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

/// The bootstrap a step receipt belongs to: its identity and the exact input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptOwner {
    incarnation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    input_digest: String,
}

/// `__cluster/bootstrap/freshness.json`: the root was fresh under this
/// identity's lock. Its bytes are deterministic, so a late copy is identical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FreshnessReceipt {
    version: u32,
    step: BootstrapStep,
    bootstrap: ReceiptOwner,
}

/// `__cluster/bootstrap/ledger.json`: this bootstrap created the ledger
/// `ledger_id`, whose revision 1 bytes have `state_cas`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LedgerReceipt {
    version: u32,
    step: BootstrapStep,
    bootstrap: ReceiptOwner,
    ledger_id: String,
    state_cas: String,
}

/// `__cluster/bootstrap/deployment.json`: the initial deployment completed
/// and left the ledger at `state_revision` with `state_cas`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeploymentReceipt {
    version: u32,
    step: BootstrapStep,
    bootstrap: ReceiptOwner,
    ledger_id: String,
    deployment_id: String,
    config_digest: String,
    result_revision: u64,
    state_revision: u64,
    state_cas: String,
}

trait StepReceipt: Serialize + serde::de::DeserializeOwned {
    const STEP: BootstrapStep;
    const PATH: &'static str;
    fn header(&self) -> (u32, BootstrapStep, &ReceiptOwner);
    fn valid_facts(&self) -> bool;
}

impl StepReceipt for FreshnessReceipt {
    const STEP: BootstrapStep = BootstrapStep::Freshness;
    const PATH: &'static str = FRESHNESS_RECEIPT;
    fn header(&self) -> (u32, BootstrapStep, &ReceiptOwner) {
        (self.version, self.step, &self.bootstrap)
    }
    fn valid_facts(&self) -> bool {
        true
    }
}

impl StepReceipt for LedgerReceipt {
    const STEP: BootstrapStep = BootstrapStep::Ledger;
    const PATH: &'static str = LEDGER_RECEIPT;
    fn header(&self) -> (u32, BootstrapStep, &ReceiptOwner) {
        (self.version, self.step, &self.bootstrap)
    }
    fn valid_facts(&self) -> bool {
        Ulid::from_string(&self.ledger_id).is_ok() && valid_state_cas(&self.state_cas)
    }
}

impl StepReceipt for DeploymentReceipt {
    const STEP: BootstrapStep = BootstrapStep::Deployment;
    const PATH: &'static str = DEPLOYMENT_RECEIPT;
    fn header(&self) -> (u32, BootstrapStep, &ReceiptOwner) {
        (self.version, self.step, &self.bootstrap)
    }
    fn valid_facts(&self) -> bool {
        Ulid::from_string(&self.ledger_id).is_ok()
            && DeploymentId::parse(&self.deployment_id)
                .is_ok_and(|id| id.ledger == self.ledger_id && id.sequence == 1)
            && authorization::valid_digest(&self.config_digest)
            && self.result_revision == 1
            && self.state_revision == 3
            && valid_state_cas(&self.state_cas)
    }
}

fn receipt_invalid(path: &str, message: impl Into<String>) -> Diagnostic {
    Diagnostic::error("bootstrap_receipt_invalid", path, message.into())
}

async fn read_receipt<R: StepReceipt>(
    store: &ClusterStore,
    owner: &ReceiptOwner,
) -> Result<Option<R>, Diagnostic> {
    let Some(text) = store
        .read_bootstrap_record(R::PATH, MAX_BOOTSTRAP_STEP_RECEIPT_BYTES)
        .await?
    else {
        return Ok(None);
    };
    let receipt: R = serde_json::from_str(&text).map_err(|error| {
        receipt_invalid(R::PATH, format!("step receipt does not decode: {error}"))
    })?;
    let (version, step, recorded) = receipt.header();
    if version != 1 || step != R::STEP || !receipt.valid_facts() {
        return Err(receipt_invalid(
            R::PATH,
            "step receipt is not a valid version 1 receipt of its step",
        ));
    }
    if recorded != owner {
        return Err(receipt_invalid(
            R::PATH,
            "step receipt names another bootstrap identity or input",
        ));
    }
    Ok(Some(receipt))
}

/// Create-if-absent. Existing identical bytes are this step's receipt, written
/// by this attempt's own earlier copy or, for the deterministic freshness
/// receipt, by an earlier attempt; anything else refuses.
async fn write_receipt<R: StepReceipt>(
    store: &ClusterStore,
    receipt: &R,
) -> Result<(), Diagnostic> {
    let text = serde_json::to_string(receipt)
        .map_err(|error| receipt_invalid(R::PATH, error.to_string()))?;
    if text.len() > MAX_BOOTSTRAP_STEP_RECEIPT_BYTES {
        return Err(receipt_invalid(R::PATH, "step receipt exceeds its bound"));
    }
    if store.create_bootstrap_record(R::PATH, &text).await? {
        return Ok(());
    }
    match store
        .read_bootstrap_record(R::PATH, MAX_BOOTSTRAP_STEP_RECEIPT_BYTES)
        .await?
    {
        Some(existing) if existing == text => Ok(()),
        _ => Err(Diagnostic::error(
            "bootstrap_receipt_conflict",
            R::PATH,
            "another step receipt was created first; never overwrite a receipt",
        )),
    }
}

/// The first step without a receipt, found by reads alone.
enum Progress {
    Freshness,
    Ledger,
    Deployment(LedgerReceipt),
    Receipt(DeploymentReceipt),
}

fn unconfirmed(path: &str, message: &str) -> Diagnostic {
    Diagnostic::error("bootstrap_step_unconfirmed", path, message)
}

/// Read the receipts and the ledger and decide where this bootstrap stands.
/// Run before a lock re-entry, so a rerun that would refuse changes nothing,
/// and again under the lock, where its answer is the one acted on.
async fn inspect(
    store: &ClusterStore,
    owner: &ReceiptOwner,
    step: &mut BootstrapStep,
) -> Result<Progress, Diagnostic> {
    *step = BootstrapStep::Freshness;
    let freshness = read_receipt::<FreshnessReceipt>(store, owner).await?;
    *step = BootstrapStep::Ledger;
    let ledger = read_receipt::<LedgerReceipt>(store, owner).await?;
    *step = BootstrapStep::Deployment;
    let deployment = read_receipt::<DeploymentReceipt>(store, owner).await?;
    let snapshot = store.read_state(&mut store.observations()).await?;
    let state_cas = snapshot.state_cas.as_deref();
    let ledger_id = snapshot
        .state
        .as_ref()
        .and_then(|state| state.ledger_id.as_deref());
    match (freshness, ledger, deployment) {
        (None, None, None) => {
            *step = BootstrapStep::Freshness;
            Ok(Progress::Freshness)
        }
        (Some(_), None, None) => {
            *step = BootstrapStep::Ledger;
            if snapshot.state.is_some() {
                return Err(unconfirmed(
                    CLUSTER_STATE_FILE,
                    "a ledger exists that no ledger receipt attributes to this bootstrap; its creation is unconfirmed",
                ));
            }
            Ok(Progress::Ledger)
        }
        (Some(_), Some(ledger), None) => {
            *step = BootstrapStep::Deployment;
            if ledger_id != Some(ledger.ledger_id.as_str())
                || state_cas != Some(ledger.state_cas.as_str())
            {
                return Err(unconfirmed(
                    CLUSTER_STATE_FILE,
                    "the ledger left its receipted initial revision and no deployment receipt records the outcome",
                ));
            }
            Ok(Progress::Deployment(ledger))
        }
        (Some(_), Some(ledger), Some(deployment)) => {
            *step = BootstrapStep::Receipt;
            if deployment.ledger_id != ledger.ledger_id {
                return Err(receipt_invalid(
                    DEPLOYMENT_RECEIPT,
                    "deployment receipt names another ledger",
                ));
            }
            if ledger_id != Some(deployment.ledger_id.as_str())
                || state_cas != Some(deployment.state_cas.as_str())
            {
                return Err(refusal(
                    "bootstrap_state_mismatch",
                    "the ledger differs from the receipted initial deployment",
                ));
            }
            Ok(Progress::Receipt(deployment))
        }
        _ => Err(receipt_invalid(
            "__cluster/bootstrap",
            "a step receipt exists without the receipts of the steps before it",
        )),
    }
}

/// Initialize one fresh S3 cluster with cluster-scoped management policy only.
/// The private bootstrap owner cannot execute another deployment. Failure or
/// cancellation after acquisition retains its lock. Only a rerun of the same
/// identity whose caller asserts `PriorAttempts::Excluded` re-enters it, and
/// it continues from the step receipts. There is no unlock, graph open or
/// background engine work here. Transport retries keep their original
/// preconditions.
pub async fn bootstrap_serving(
    config_dir: impl AsRef<Path>,
    caller: &DeploymentCaller,
    request: &BootstrapRequest,
) -> Result<BootstrapServingReceipt, BootstrapFailure> {
    let before_storage = |diagnostic| {
        BootstrapFailure::new(diagnostic, BootstrapStep::Input, BootstrapEffects::None)
    };
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
    bootstrap_attempt(&recorded, bundle, caller, request, &mut step)
        .await
        .map_err(|diagnostic| {
            let scope_uncertain = recorded
                .io_scope()
                .is_some_and(|scope| scope.is_uncertain());
            let effects = log.effects().max(if scope_uncertain {
                BootstrapEffects::Unconfirmed
            } else {
                BootstrapEffects::None
            });
            BootstrapFailure::new(diagnostic, step, effects)
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
) -> Result<BootstrapServingReceipt, Diagnostic> {
    *step = BootstrapStep::Input;
    let identity = &request.identity;
    identity.validate()?;
    validate_bootstrap_bundle(bundle, caller)?;
    let owner = ReceiptOwner {
        incarnation: identity.incarnation.clone(),
        request_id: identity.request_id.clone(),
        input_digest: bundle.input_digest()?,
    };

    *step = BootstrapStep::Lock;
    let observed = store
        .read_bootstrap_lock_state(identity.incarnation(), identity.request_id())
        .await?;
    let (guard, lock_version) = match observed {
        BootstrapLockState::Absent => {
            *step = BootstrapStep::Freshness;
            store.require_fresh_bootstrap(false).await?;
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
            inspect(store, &owner, step).await?;
            *step = BootstrapStep::Lock;
            if lock.bootstrap_attempt() >= crate::state_lock::MAX_BOOTSTRAP_ATTEMPTS {
                return Err(Diagnostic::error(
                    "bootstrap_attempts_exhausted",
                    CLUSTER_LOCK_FILE,
                    format!(
                        "this bootstrap identity has used all {} attempts",
                        crate::state_lock::MAX_BOOTSTRAP_ATTEMPTS
                    ),
                ));
            }
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

    let mut progress = inspect(store, &owner, step).await?;
    let deployed = loop {
        progress = match progress {
            Progress::Freshness => {
                *step = BootstrapStep::Freshness;
                store.require_fresh_bootstrap(true).await?;
                write_receipt(
                    store,
                    &FreshnessReceipt {
                        version: 1,
                        step: BootstrapStep::Freshness,
                        bootstrap: owner.clone(),
                    },
                )
                .await?;
                Progress::Ledger
            }
            Progress::Ledger => {
                *step = BootstrapStep::Ledger;
                store.require_no_bootstrap_graphs_or_recovery().await?;
                let state = execution::empty_ledger();
                let mut observations = store.observations();
                store.write_state(&state, None, &mut observations).await?;
                let receipt = LedgerReceipt {
                    version: 1,
                    step: BootstrapStep::Ledger,
                    bootstrap: owner.clone(),
                    ledger_id: state.ledger_id.clone().expect("an empty ledger has an id"),
                    state_cas: observations
                        .state_cas
                        .expect("a confirmed ledger write records its CAS"),
                };
                write_receipt(store, &receipt).await?;
                Progress::Deployment(receipt)
            }
            Progress::Deployment(ledger) => {
                *step = BootstrapStep::Deployment;
                store.require_no_bootstrap_graphs_or_recovery().await?;
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
                let receipt = DeploymentReceipt {
                    version: 1,
                    step: BootstrapStep::Deployment,
                    bootstrap: owner.clone(),
                    ledger_id: ledger.ledger_id,
                    deployment_id: result.id,
                    config_digest: bundle.config_digest.clone(),
                    result_revision: result.result_revision,
                    state_revision: state.state_revision,
                    state_cas,
                };
                if state.ledger_id.as_deref() != Some(receipt.ledger_id.as_str())
                    || !receipt.valid_facts()
                {
                    return Err(refusal(
                        "bootstrap_state_mismatch",
                        "native state differs from the exact initial policy deployment",
                    ));
                }
                write_receipt(store, &receipt).await?;
                Progress::Receipt(receipt)
            }
            Progress::Receipt(deployment) => break deployment,
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
        input_digest: owner.input_digest,
        config_digest: deployed.config_digest,
        result_revision: deployed.result_revision,
    };
    receipt.validate()?;
    validate_terminal(store, &receipt).await?;
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
            "__cluster/bootstrap/freshness.json",
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
