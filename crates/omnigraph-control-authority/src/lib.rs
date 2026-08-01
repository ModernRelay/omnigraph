//! Cycle-free control-plane authority primitives.
//!
//! This leaf owns the persisted cluster lock and the only production factories
//! for validated offline and served-runtime guards. It depends on the concrete
//! shared storage handle, never on the engine, cluster, server, or a
//! caller-implemented trait. The engine consumes these non-forgeable guards
//! into its narrower checked capabilities and revalidates graph-manifest
//! authority before any effect.

use std::collections::{BTreeMap, BTreeSet};
use std::process;
use std::sync::{Mutex, OnceLock};

use omnigraph_storage::{
    StorageAdapter, StorageError, StorageHandle, StorageKind, join_uri, normalize_root_uri,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

const LOCK_VERSION: u32 = 1;
const CLUSTER_STATE_VERSION: u32 = 1;
const CLUSTER_STATE_FILE: &str = "__cluster/state.json";
const CLUSTER_LOCK_FILE: &str = "__cluster/lock.json";

#[derive(Debug, Error)]
pub enum AuthorityError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("could not encode cluster state lock: {0}")]
    LockEncode(serde_json::Error),
    #[error("could not parse cluster state lock: {0}")]
    LockParse(serde_json::Error),
    #[error("unsupported cluster state lock version {0}; expected 1")]
    LockVersion(u32),
    #[error("invalid control authority binding: {0}")]
    InvalidBinding(String),
    #[error("could not parse canonical cluster state: {0}")]
    StateParse(serde_json::Error),
    #[error("unsupported cluster state version {0}; expected 1")]
    StateVersion(u32),
    #[error("cluster state changed while control authority was being validated")]
    StateChanged,
    #[error("cluster state does not contain the required runtime authority: {0}")]
    RuntimeState(String),
    #[error(
        "a stream runtime is already registered for graph '{graph_id}' in cluster '{cluster_root}'"
    )]
    RuntimeAlreadyRegistered {
        cluster_root: String,
        graph_id: String,
    },
    #[error(
        "offline stream authority cannot be minted while this process owns a live runtime for graph '{graph_id}' in cluster '{cluster_root}'"
    )]
    RuntimeStillRegistered {
        cluster_root: String,
        graph_id: String,
    },
}

/// Exact persisted `__cluster/lock.json` wire shape.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateLockFile {
    version: u32,
    lock_id: String,
    operation: String,
    created_at: String,
    pid: u32,
}

impl StateLockFile {
    pub fn parse(text: &str) -> Result<Self, AuthorityError> {
        let lock: Self = serde_json::from_str(text).map_err(AuthorityError::LockParse)?;
        if lock.version != LOCK_VERSION {
            return Err(AuthorityError::LockVersion(lock.version));
        }
        Ok(lock)
    }

    pub fn lock_id(&self) -> &str {
        &self.lock_id
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }
}

/// Result of the storage-native conditional-create lock acquisition.
#[derive(Debug)]
pub enum StateLockAcquire {
    Acquired(StateLockGuard),
    Held,
}

/// Exclusive persisted cluster-state lock.
///
/// Private fields and the lack of `Clone`, `Default`, `Serialize`, or
/// `Deserialize` make possession non-forgeable. The only constructor performs
/// the concrete backend's atomic create-if-absent operation.
#[derive(Debug)]
pub struct StateLockGuard {
    adapter: std::sync::Arc<dyn StorageAdapter>,
    uri: String,
    cluster_root: String,
    kind: StorageKind,
    lock: StateLockFile,
}

impl StateLockGuard {
    pub fn lock_id(&self) -> &str {
        self.lock.lock_id()
    }

    pub fn operation(&self) -> &str {
        self.lock.operation()
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Normalized cluster root derived from the exact persisted lock location.
    ///
    /// This is intentionally not accepted from an authority caller: the lock
    /// itself determines which cluster the caller currently owns.
    pub fn cluster_root(&self) -> &str {
        &self.cluster_root
    }
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        match self.kind {
            // Preserve deterministic local release: cluster command tests and
            // callers observe the lock gone when the command returns. A stale
            // guard must never remove a replacement owner's lock.
            StorageKind::Local => {
                let path = self.uri.trim_start_matches("file://");
                let still_owned = std::fs::read_to_string(path)
                    .ok()
                    .and_then(|text| StateLockFile::parse(&text).ok())
                    .is_some_and(|lock| lock.lock_id() == self.lock_id());
                if still_owned {
                    let _ = std::fs::remove_file(path);
                }
            }
            // A short-lived CLI must not exit before object-store deletion
            // completes. Current-thread runtimes cannot block_in_place, so
            // they retain the prior best-effort spawned fallback.
            StorageKind::S3 => {
                let adapter = self.adapter.clone();
                let uri = self.uri.clone();
                let lock_id = self.lock_id().to_string();
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                        tokio::task::block_in_place(move || {
                            handle.block_on(async move {
                                release_remote_lock_if_owned(&adapter, &uri, &lock_id).await;
                            });
                        });
                    } else {
                        handle.spawn(async move {
                            release_remote_lock_if_owned(&adapter, &uri, &lock_id).await;
                        });
                    }
                }
            }
        }
    }
}

async fn release_remote_lock_if_owned(
    adapter: &std::sync::Arc<dyn StorageAdapter>,
    uri: &str,
    lock_id: &str,
) {
    let still_owned = adapter
        .read_text_if_exists(uri)
        .await
        .ok()
        .flatten()
        .and_then(|text| StateLockFile::parse(&text).ok())
        .is_some_and(|lock| lock.lock_id() == lock_id);
    if still_owned {
        let _ = adapter.delete(uri).await;
    }
}

/// Acquire an exclusive state lock through the concrete shared storage path.
pub async fn acquire_state_lock(
    storage: &StorageHandle,
    lock_uri: &str,
    operation: &str,
) -> Result<StateLockAcquire, AuthorityError> {
    let cluster_root = cluster_root_from_lock_uri(lock_uri)?;
    let adapter = storage.adapter();
    let lock = StateLockFile {
        version: LOCK_VERSION,
        lock_id: Ulid::new().to_string(),
        operation: operation.to_string(),
        created_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
        pid: process::id(),
    };
    let payload = serde_json::to_string_pretty(&lock).map_err(AuthorityError::LockEncode)?;
    if adapter.write_text_if_absent(lock_uri, &payload).await? {
        return Ok(StateLockAcquire::Acquired(StateLockGuard {
            adapter,
            uri: lock_uri.to_string(),
            cluster_root,
            kind: storage.kind(),
            lock,
        }));
    }

    Ok(StateLockAcquire::Held)
}

/// Operation class bound into a checked control-plane authority.
///
/// The engine consumes this value; no caller-supplied boolean can retarget an
/// already checked profile, maintenance, block-control, or retirement
/// operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityOperationClass {
    StreamProfileEnable,
    StreamProfileDisable,
    /// Offline stream-aware schema and physical-binding maintenance.
    ///
    /// The lower guard proves only cluster-lock and stopped-process ownership.
    /// The engine must additionally prove the exact graph is `DISABLED` at the
    /// request's expected profile revision before minting its narrower checked
    /// maintenance capability.
    StreamMaintenance,
    /// Offline inspection and correction of one exact strict stream block.
    ///
    /// This is deliberately distinct from general stream maintenance. A
    /// blocked lane is still `DRAINING`, normally under an `ENABLED` or
    /// `DISABLING` profile, while rebind and schema maintenance remain
    /// `DISABLED`-only.
    StreamBlockControl,
    /// Irreversible, graph-wide stream-authority retirement for rebuild.
    ///
    /// This is deliberately distinct from ordinary offline maintenance.  A
    /// retirement confirmation must be able to cross the checked boundary
    /// after its exact receipt has already made the profile `RETIRED`, while
    /// rebind/schema maintenance must remain `DISABLED`-only.
    StreamAuthorityRetirement,
    StreamRuntime,
}

impl AuthorityOperationClass {
    pub fn requested_streaming_enabled(self) -> Option<bool> {
        match self {
            Self::StreamProfileEnable => Some(true),
            Self::StreamProfileDisable => Some(false),
            Self::StreamMaintenance
            | Self::StreamBlockControl
            | Self::StreamAuthorityRetirement
            | Self::StreamRuntime => None,
        }
    }

    fn offline_lock_operation(self) -> Option<&'static str> {
        match self {
            Self::StreamProfileEnable
            | Self::StreamProfileDisable
            | Self::StreamMaintenance
            | Self::StreamBlockControl => Some("apply"),
            Self::StreamAuthorityRetirement => Some(STREAM_AUTHORITY_RETIREMENT_LOCK_OPERATION),
            Self::StreamRuntime => None,
        }
    }
}

/// Persisted cluster-lock operation used by both halves of the irreversible
/// retirement handshake.  Exporting the spelling keeps the cluster command
/// and the lower authority validator on one exact contract.
pub const STREAM_AUTHORITY_RETIREMENT_LOCK_OPERATION: &str = "stream-retire-for-rebuild";

/// Facts supplied by cluster apply after it has validated desired
/// configuration and read the current state under the persisted lock.
pub struct OfflineAuthorityRequest<'a> {
    pub graph_id: &'a str,
    pub graph_store_uri: &'a str,
    pub expected_state_cas: &'a str,
    pub state_revision: u64,
    pub declaration_revision: &'a str,
    pub declaration_digest: &'a str,
    pub expected_profile_revision: u64,
    pub operation_id: &'a str,
    pub operation: AuthorityOperationClass,
    pub actor: &'a str,
    pub confirm_stream_offline: bool,
}

/// Validated stopped-writer authority retained for the persisted-lock lifetime.
///
/// Private fields plus the absence of `Clone`, `Default`, and serde traits
/// prevent callers from manufacturing or extending this authority. The
/// borrowed lock makes the supported `stop -> locked apply -> restart`
/// topology structural inside one apply process.
#[derive(Debug)]
pub struct ValidatedOfflineGuard<'lock> {
    _lock: &'lock StateLockGuard,
    cluster_root: String,
    graph_id: String,
    graph_store_uri: String,
    expected_state_cas: String,
    state_revision: u64,
    declaration_revision: String,
    declaration_digest: String,
    expected_profile_revision: u64,
    operation_id: String,
    operation: AuthorityOperationClass,
    actor: String,
    _runtime_exclusion: ExclusiveProcessRegistration,
}

impl ValidatedOfflineGuard<'_> {
    pub fn cluster_root(&self) -> &str {
        &self.cluster_root
    }

    pub fn graph_id(&self) -> &str {
        &self.graph_id
    }

    pub fn graph_store_uri(&self) -> &str {
        &self.graph_store_uri
    }

    pub fn expected_state_cas(&self) -> &str {
        &self.expected_state_cas
    }

    pub fn state_revision(&self) -> u64 {
        self.state_revision
    }

    pub fn declaration_revision(&self) -> &str {
        &self.declaration_revision
    }

    pub fn declaration_digest(&self) -> &str {
        &self.declaration_digest
    }

    pub fn expected_profile_revision(&self) -> u64 {
        self.expected_profile_revision
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn operation(&self) -> AuthorityOperationClass {
        self.operation
    }

    pub fn actor(&self) -> &str {
        &self.actor
    }
}

/// Validate and mint one offline streaming authority under the actual state
/// lock.
///
/// Profile changes, maintenance, block control, and authority retirement
/// deliberately share the same concrete stopped-process proof. Their engine
/// capabilities remain distinct: this lower guard cannot turn one operation
/// class into another.
pub async fn validate_offline_guard<'lock>(
    lock: &'lock StateLockGuard,
    request: OfflineAuthorityRequest<'_>,
) -> Result<ValidatedOfflineGuard<'lock>, AuthorityError> {
    let Some(required_lock_operation) = request.operation.offline_lock_operation() else {
        return Err(invalid_binding(
            "offline guard operation must be a streaming profile transition, stream maintenance, stream-block control, or stream-authority retirement",
        ));
    };
    if lock.operation() != required_lock_operation {
        return Err(invalid_binding(format!(
            "offline streaming authority for {:?} requires the '{required_lock_operation}' cluster lock",
            request.operation
        )));
    }
    if !request.confirm_stream_offline {
        let message = match request.operation {
            AuthorityOperationClass::StreamMaintenance => {
                "streaming maintenance requires explicit confirm_stream_offline"
            }
            AuthorityOperationClass::StreamBlockControl => {
                "stream-block control requires explicit confirm_stream_offline"
            }
            AuthorityOperationClass::StreamAuthorityRetirement => {
                "stream-authority retirement requires explicit confirm_stream_offline"
            }
            AuthorityOperationClass::StreamProfileEnable
            | AuthorityOperationClass::StreamProfileDisable => {
                "streaming profile changes require explicit confirm_stream_offline"
            }
            AuthorityOperationClass::StreamRuntime => unreachable!(
                "runtime operation was rejected before offline confirmation validation"
            ),
        };
        return Err(invalid_binding(message));
    }
    validate_held_offline_lock(lock, required_lock_operation).await?;
    validate_common_request(
        request.graph_id,
        request.graph_store_uri,
        lock.cluster_root(),
        request.expected_state_cas,
        request.declaration_revision,
        request.declaration_digest,
        request.expected_profile_revision,
        request.operation_id,
        request.actor,
    )?;
    let state = read_canonical_state(&lock.adapter, lock.cluster_root()).await?;
    validate_state_identity(&state, request.expected_state_cas, request.state_revision)?;
    let graph_store_uri = validated_graph_store(
        lock.cluster_root(),
        request.graph_id,
        request.graph_store_uri,
    )?;
    let runtime_exclusion =
        ExclusiveProcessRegistration::acquire_offline(lock.cluster_root(), request.graph_id)?;

    Ok(ValidatedOfflineGuard {
        _lock: lock,
        cluster_root: lock.cluster_root.clone(),
        graph_id: request.graph_id.to_string(),
        graph_store_uri,
        expected_state_cas: request.expected_state_cas.to_string(),
        state_revision: request.state_revision,
        declaration_revision: request.declaration_revision.to_string(),
        declaration_digest: request.declaration_digest.to_string(),
        expected_profile_revision: request.expected_profile_revision,
        operation_id: request.operation_id.to_string(),
        operation: request.operation,
        actor: request.actor.to_string(),
        _runtime_exclusion: runtime_exclusion,
    })
}

async fn validate_held_offline_lock(
    lock: &StateLockGuard,
    required_operation: &str,
) -> Result<(), AuthorityError> {
    let persisted = lock.adapter.read_text(lock.uri()).await?;
    let persisted = StateLockFile::parse(&persisted)?;
    if persisted.lock_id() != lock.lock_id() || persisted.operation() != required_operation {
        return Err(invalid_binding(format!(
            "cluster '{required_operation}' lock changed before offline authority validation"
        )));
    }
    Ok(())
}

/// Canonical, cloneable serving-snapshot evidence. This value is not runtime
/// authority: only [`mint_runtime_guard`] registers the sole process-local
/// writer and produces the non-cloneable guard consumed by the engine.
#[derive(Clone)]
pub struct RuntimeAuthorityBinding {
    storage: StorageHandle,
    cluster_root: String,
    graph_id: String,
    graph_store_uri: String,
    expected_state_cas: String,
    state_revision: u64,
    declaration_revision: String,
    declaration_digest: String,
    profile_mode: String,
    profile_revision: u64,
}

impl std::fmt::Debug for RuntimeAuthorityBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeAuthorityBinding")
            .field("cluster_root", &self.cluster_root)
            .field("graph_id", &self.graph_id)
            .field("graph_store_uri", &self.graph_store_uri)
            .field("expected_state_cas", &self.expected_state_cas)
            .field("state_revision", &self.state_revision)
            .field("declaration_revision", &self.declaration_revision)
            .field("declaration_digest", &self.declaration_digest)
            .field("profile_mode", &self.profile_mode)
            .field("profile_revision", &self.profile_revision)
            .finish_non_exhaustive()
    }
}

impl RuntimeAuthorityBinding {
    pub fn cluster_root(&self) -> &str {
        &self.cluster_root
    }

    pub fn graph_id(&self) -> &str {
        &self.graph_id
    }

    pub fn graph_store_uri(&self) -> &str {
        &self.graph_store_uri
    }

    pub fn expected_state_cas(&self) -> &str {
        &self.expected_state_cas
    }

    pub fn state_revision(&self) -> u64 {
        self.state_revision
    }

    pub fn declaration_revision(&self) -> &str {
        &self.declaration_revision
    }

    pub fn declaration_digest(&self) -> &str {
        &self.declaration_digest
    }

    pub fn profile_mode(&self) -> &str {
        &self.profile_mode
    }

    pub fn profile_revision(&self) -> u64 {
        self.profile_revision
    }
}

pub struct RuntimeBindingRequest<'a> {
    pub graph_id: &'a str,
    pub graph_store_uri: &'a str,
    pub expected_state_cas: &'a str,
    pub state_revision: u64,
    pub declaration_revision: &'a str,
    pub declaration_digest: &'a str,
    pub profile_mode: &'a str,
    pub profile_revision: u64,
}

/// Load and validate the canonical enabled runtime binding for one serving
/// graph. The resulting binding remains harmless and cloneable; server startup
/// later consumes it through [`mint_runtime_guard`].
pub async fn validate_runtime_binding(
    storage: &StorageHandle,
    cluster_root: &str,
    request: RuntimeBindingRequest<'_>,
) -> Result<RuntimeAuthorityBinding, AuthorityError> {
    validate_nonempty("profile_mode", request.profile_mode)?;
    validate_common_request(
        request.graph_id,
        request.graph_store_uri,
        cluster_root,
        request.expected_state_cas,
        request.declaration_revision,
        request.declaration_digest,
        request.profile_revision,
        "runtime-binding-validation",
        "omnigraph:stream-runtime",
    )?;
    let cluster_root = normalize_root_uri(cluster_root)?;
    let graph_store_uri =
        validated_graph_store(&cluster_root, request.graph_id, request.graph_store_uri)?;
    let state = read_canonical_state(&storage.adapter(), &cluster_root).await?;
    validate_runtime_state(
        &state,
        request.graph_id,
        request.expected_state_cas,
        request.state_revision,
        request.declaration_revision,
        request.declaration_digest,
        request.profile_mode,
        request.profile_revision,
    )?;

    Ok(RuntimeAuthorityBinding {
        storage: storage.clone(),
        cluster_root,
        graph_id: request.graph_id.to_string(),
        graph_store_uri,
        expected_state_cas: request.expected_state_cas.to_string(),
        state_revision: request.state_revision,
        declaration_revision: request.declaration_revision.to_string(),
        declaration_digest: request.declaration_digest.to_string(),
        profile_mode: request.profile_mode.to_string(),
        profile_revision: request.profile_revision,
    })
}

/// Non-cloneable live runtime authority with an exclusive process-local
/// registration. The registration is not a distributed lease; it is the local
/// half of the externally enforced single-writer topology.
#[derive(Debug)]
pub struct ValidatedRuntimeGuard {
    binding: RuntimeAuthorityBinding,
    operation_id: String,
    operation: AuthorityOperationClass,
    actor: String,
    _registration: ExclusiveProcessRegistration,
}

impl ValidatedRuntimeGuard {
    pub fn cluster_root(&self) -> &str {
        self.binding.cluster_root()
    }

    pub fn graph_id(&self) -> &str {
        self.binding.graph_id()
    }

    pub fn graph_store_uri(&self) -> &str {
        self.binding.graph_store_uri()
    }

    pub fn expected_state_cas(&self) -> &str {
        self.binding.expected_state_cas()
    }

    pub fn state_revision(&self) -> u64 {
        self.binding.state_revision()
    }

    pub fn declaration_revision(&self) -> &str {
        self.binding.declaration_revision()
    }

    pub fn declaration_digest(&self) -> &str {
        self.binding.declaration_digest()
    }

    pub fn profile_mode(&self) -> &str {
        self.binding.profile_mode()
    }

    pub fn profile_revision(&self) -> u64 {
        self.binding.profile_revision()
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn operation(&self) -> AuthorityOperationClass {
        self.operation
    }

    pub fn actor(&self) -> &str {
        &self.actor
    }
}

/// Re-read the exact applied state and register the sole in-process runtime.
pub async fn mint_runtime_guard(
    binding: RuntimeAuthorityBinding,
    operation_id: &str,
    actor: &str,
) -> Result<ValidatedRuntimeGuard, AuthorityError> {
    validate_nonempty("operation_id", operation_id)?;
    validate_nonempty("actor", actor)?;
    let state = read_canonical_state(&binding.storage.adapter(), &binding.cluster_root).await?;
    validate_runtime_state(
        &state,
        &binding.graph_id,
        &binding.expected_state_cas,
        binding.state_revision,
        &binding.declaration_revision,
        &binding.declaration_digest,
        &binding.profile_mode,
        binding.profile_revision,
    )?;
    let registration =
        ExclusiveProcessRegistration::acquire_runtime(&binding.cluster_root, &binding.graph_id)?;
    Ok(ValidatedRuntimeGuard {
        binding,
        operation_id: operation_id.to_string(),
        operation: AuthorityOperationClass::StreamRuntime,
        actor: actor.to_string(),
        _registration: registration,
    })
}

#[derive(Debug)]
struct ExclusiveProcessRegistration {
    key: String,
}

impl ExclusiveProcessRegistration {
    fn acquire_runtime(cluster_root: &str, graph_id: &str) -> Result<Self, AuthorityError> {
        let key = format!("{cluster_root}\0{graph_id}");
        let mut registrations = runtime_registrations()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !registrations.insert(key.clone()) {
            return Err(AuthorityError::RuntimeAlreadyRegistered {
                cluster_root: cluster_root.to_string(),
                graph_id: graph_id.to_string(),
            });
        }
        Ok(Self { key })
    }

    fn acquire_offline(cluster_root: &str, graph_id: &str) -> Result<Self, AuthorityError> {
        let key = format!("{cluster_root}\0{graph_id}");
        let mut registrations = runtime_registrations()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !registrations.insert(key.clone()) {
            return Err(AuthorityError::RuntimeStillRegistered {
                cluster_root: cluster_root.to_string(),
                graph_id: graph_id.to_string(),
            });
        }
        Ok(Self { key })
    }
}

impl Drop for ExclusiveProcessRegistration {
    fn drop(&mut self) {
        runtime_registrations()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.key);
    }
}

fn runtime_registrations() -> &'static Mutex<BTreeSet<String>> {
    static REGISTRATIONS: OnceLock<Mutex<BTreeSet<String>>> = OnceLock::new();
    REGISTRATIONS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[derive(Debug, Deserialize)]
struct CanonicalClusterState {
    version: u32,
    #[serde(default)]
    state_revision: u64,
    applied_revision: CanonicalAppliedRevision,
}

#[derive(Debug, Deserialize)]
struct CanonicalAppliedRevision {
    #[serde(default)]
    resources: BTreeMap<String, CanonicalStateResource>,
}

#[derive(Debug, Deserialize)]
struct CanonicalStateResource {
    digest: String,
    #[serde(default)]
    declaration_revision: Option<String>,
    #[serde(default)]
    streaming_enabled: Option<bool>,
    #[serde(default)]
    profile_revision: Option<u64>,
    #[serde(default)]
    profile_mode: Option<String>,
}

#[derive(Debug)]
struct LoadedCanonicalState {
    state: CanonicalClusterState,
    state_cas: String,
}

async fn read_canonical_state(
    adapter: &std::sync::Arc<dyn StorageAdapter>,
    cluster_root: &str,
) -> Result<LoadedCanonicalState, AuthorityError> {
    let state_uri = join_uri(cluster_root, CLUSTER_STATE_FILE);
    let text = adapter.read_text(&state_uri).await?;
    let state: CanonicalClusterState =
        serde_json::from_str(&text).map_err(AuthorityError::StateParse)?;
    if state.version != CLUSTER_STATE_VERSION {
        return Err(AuthorityError::StateVersion(state.version));
    }
    Ok(LoadedCanonicalState {
        state,
        state_cas: format!("sha256:{}", sha256_hex(text.as_bytes())),
    })
}

fn validate_state_identity(
    state: &LoadedCanonicalState,
    expected_state_cas: &str,
    state_revision: u64,
) -> Result<(), AuthorityError> {
    if state.state_cas != expected_state_cas || state.state.state_revision != state_revision {
        return Err(AuthorityError::StateChanged);
    }
    Ok(())
}

fn validate_runtime_state(
    loaded: &LoadedCanonicalState,
    graph_id: &str,
    expected_state_cas: &str,
    state_revision: u64,
    declaration_revision: &str,
    declaration_digest: &str,
    profile_mode: &str,
    profile_revision: u64,
) -> Result<(), AuthorityError> {
    validate_state_identity(loaded, expected_state_cas, state_revision)?;
    let graph_address = format!("graph.{graph_id}");
    if !loaded
        .state
        .applied_revision
        .resources
        .contains_key(&graph_address)
    {
        return Err(AuthorityError::RuntimeState(format!(
            "applied graph resource '{graph_address}' is missing"
        )));
    }
    let streaming_address = format!("streaming.{graph_id}");
    let streaming = loaded
        .state
        .applied_revision
        .resources
        .get(&streaming_address)
        .ok_or_else(|| {
            AuthorityError::RuntimeState(format!(
                "applied streaming resource '{streaming_address}' is missing"
            ))
        })?;
    if streaming.digest != declaration_digest {
        return Err(AuthorityError::RuntimeState(
            "streaming declaration digest does not match the serving snapshot".to_string(),
        ));
    }
    if streaming.declaration_revision.as_deref() != Some(declaration_revision) {
        return Err(AuthorityError::RuntimeState(
            "streaming declaration revision is missing or changed".to_string(),
        ));
    }
    if streaming.profile_mode.as_deref() != Some(profile_mode) || profile_mode != "ENABLED" {
        return Err(AuthorityError::RuntimeState(
            "streaming runtime authority requires exact ENABLED profile-mode metadata".to_string(),
        ));
    }
    if streaming.streaming_enabled != Some(true) {
        return Err(AuthorityError::RuntimeState(
            "streaming runtime authority requires an enabled applied profile".to_string(),
        ));
    }
    if streaming.profile_revision != Some(profile_revision) {
        return Err(AuthorityError::RuntimeState(
            "streaming profile revision is missing or changed".to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_common_request(
    graph_id: &str,
    graph_store_uri: &str,
    cluster_root: &str,
    expected_state_cas: &str,
    declaration_revision: &str,
    declaration_digest: &str,
    profile_revision: u64,
    operation_id: &str,
    actor: &str,
) -> Result<(), AuthorityError> {
    validate_graph_id(graph_id)?;
    validate_nonempty("graph_store_uri", graph_store_uri)?;
    validate_nonempty("cluster_root", cluster_root)?;
    validate_state_cas(expected_state_cas)?;
    validate_nonempty("declaration_revision", declaration_revision)?;
    validate_nonempty("declaration_digest", declaration_digest)?;
    if profile_revision == 0 {
        return Err(invalid_binding("profile_revision must be nonzero"));
    }
    validate_nonempty("operation_id", operation_id)?;
    validate_nonempty("actor", actor)?;
    Ok(())
}

fn validated_graph_store(
    cluster_root: &str,
    graph_id: &str,
    requested_store: &str,
) -> Result<String, AuthorityError> {
    let expected = normalize_root_uri(&join_uri(cluster_root, &format!("graphs/{graph_id}.omni")))?;
    let requested = normalize_root_uri(requested_store)?;
    if requested != expected {
        return Err(invalid_binding(format!(
            "graph/store mapping mismatch: expected '{expected}', got '{requested}'"
        )));
    }
    Ok(expected)
}

fn cluster_root_from_lock_uri(lock_uri: &str) -> Result<String, AuthorityError> {
    let suffix = format!("/{CLUSTER_LOCK_FILE}");
    let root = lock_uri.strip_suffix(&suffix).ok_or_else(|| {
        invalid_binding(format!(
            "state lock must be stored at '<cluster>/{CLUSTER_LOCK_FILE}'"
        ))
    })?;
    normalize_root_uri(root).map_err(AuthorityError::from)
}

fn validate_graph_id(graph_id: &str) -> Result<(), AuthorityError> {
    let mut chars = graph_id.chars();
    let valid = chars
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if !valid {
        return Err(invalid_binding(
            "graph_id must start with an ASCII letter or '_' and contain only ASCII letters, digits, '_', or '-'",
        ));
    }
    Ok(())
}

fn validate_state_cas(state_cas: &str) -> Result<(), AuthorityError> {
    let Some(digest) = state_cas.strip_prefix("sha256:") else {
        return Err(invalid_binding(
            "expected_state_cas must use the sha256:<hex> form",
        ));
    };
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_binding(
            "expected_state_cas must contain exactly 64 hexadecimal digits",
        ));
    }
    Ok(())
}

fn validate_nonempty(name: &str, value: &str) -> Result<(), AuthorityError> {
    if value.trim().is_empty() {
        return Err(invalid_binding(format!("{name} must be nonempty")));
    }
    Ok(())
}

fn invalid_binding(message: impl Into<String>) -> AuthorityError {
    AuthorityError::InvalidBinding(message.into())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_storage::storage_handle_for_uri;

    fn write_state(dir: &tempfile::TempDir, value: serde_json::Value) -> String {
        let text = serde_json::to_string_pretty(&value).unwrap();
        let state_path = dir.path().join(CLUSTER_STATE_FILE);
        std::fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        std::fs::write(state_path, &text).unwrap();
        format!("sha256:{}", sha256_hex(text.as_bytes()))
    }

    #[tokio::test]
    async fn state_lock_is_exclusive_and_releases_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let storage = storage_handle_for_uri(&root).unwrap();
        let lock_uri = format!("{root}/__cluster/lock.json");
        let guard = match acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap()
        {
            StateLockAcquire::Acquired(guard) => guard,
            StateLockAcquire::Held => panic!("fresh lock unexpectedly held"),
        };
        assert_eq!(guard.operation(), "apply");
        assert_eq!(guard.uri(), lock_uri);
        assert_eq!(
            guard.cluster_root(),
            normalize_root_uri(&root).unwrap().as_str()
        );
        assert!(!guard.lock_id().is_empty());
        assert!(dir.path().join("__cluster/lock.json").exists());
        let persisted = std::fs::read_to_string(dir.path().join("__cluster/lock.json")).unwrap();
        let parsed = StateLockFile::parse(&persisted).unwrap();
        assert_eq!(parsed.lock_id(), guard.lock_id());
        assert_eq!(parsed.operation(), "apply");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&persisted)
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            5,
            "the persisted lock wire shape has exactly five fields"
        );

        assert!(matches!(
            acquire_state_lock(&storage, &lock_uri, "apply")
                .await
                .unwrap(),
            StateLockAcquire::Held
        ));

        drop(guard);
        assert!(!dir.path().join("__cluster/lock.json").exists());

        let reacquired = acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap();
        assert!(matches!(reacquired, StateLockAcquire::Acquired(_)));
    }

    #[tokio::test]
    async fn offline_guard_binds_actual_lock_root_state_and_explicit_confirmation() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let state_cas = write_state(
            &dir,
            serde_json::json!({
                "version": 1,
                "state_revision": 7,
                "applied_revision": {
                    "config_digest": "old-config",
                    "resources": {}
                }
            }),
        );
        let storage = storage_handle_for_uri(&root).unwrap();
        let lock_uri = format!("{root}/{CLUSTER_LOCK_FILE}");
        let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap()
        {
            StateLockAcquire::Acquired(lock) => lock,
            StateLockAcquire::Held => panic!("fresh lock unexpectedly held"),
        };
        let graph_store = format!("{}/graphs/knowledge.omni", dir.path().display());

        let refused = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 7,
                declaration_revision: "stream-declaration-v1",
                declaration_digest: "stream-declaration",
                expected_profile_revision: 1,
                operation_id: "stable-operation",
                operation: AuthorityOperationClass::StreamProfileEnable,
                actor: "operator",
                confirm_stream_offline: false,
            },
        )
        .await
        .unwrap_err();
        assert!(refused.to_string().contains("confirm_stream_offline"));

        let guard = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 7,
                declaration_revision: "stream-declaration-v1",
                declaration_digest: "stream-declaration",
                expected_profile_revision: 1,
                operation_id: "stable-operation",
                operation: AuthorityOperationClass::StreamProfileEnable,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(guard.cluster_root(), dir.path().to_string_lossy());
        assert_eq!(guard.graph_id(), "knowledge");
        assert_eq!(guard.graph_store_uri(), graph_store);
        assert_eq!(guard.expected_state_cas(), state_cas);
        assert_eq!(guard.state_revision(), 7);
        assert_eq!(guard.declaration_revision(), "stream-declaration-v1");
        assert_eq!(guard.declaration_digest(), "stream-declaration");
        assert_eq!(guard.expected_profile_revision(), 1);
        assert_eq!(guard.operation_id(), "stable-operation");
        assert_eq!(
            guard.operation(),
            AuthorityOperationClass::StreamProfileEnable
        );
        assert_eq!(guard.actor(), "operator");
        drop(guard);

        let maintenance = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 7,
                declaration_revision: "stream-declaration-v1",
                declaration_digest: "stream-declaration",
                expected_profile_revision: 1,
                operation_id: "maintenance-operation",
                operation: AuthorityOperationClass::StreamMaintenance,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            maintenance.operation(),
            AuthorityOperationClass::StreamMaintenance
        );
        assert_eq!(maintenance.operation_id(), "maintenance-operation");
        assert_eq!(maintenance.operation().requested_streaming_enabled(), None);
        drop(maintenance);

        let block_control = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 7,
                declaration_revision: "stream-block-declaration-v1",
                declaration_digest: "stream-block-declaration",
                expected_profile_revision: 1,
                operation_id: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                operation: AuthorityOperationClass::StreamBlockControl,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            block_control.operation(),
            AuthorityOperationClass::StreamBlockControl
        );
        assert_eq!(
            block_control.operation_id(),
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            block_control.operation().requested_streaming_enabled(),
            None
        );
        drop(block_control);

        std::fs::write(
            dir.path().join(CLUSTER_LOCK_FILE),
            serde_json::json!({
                "version": 1,
                "lock_id": "replacement-owner",
                "operation": "apply",
                "created_at": "1970-01-01T00:00:00Z",
                "pid": 1
            })
            .to_string(),
        )
        .unwrap();
        let stale = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 7,
                declaration_revision: "stream-declaration-v1",
                declaration_digest: "stream-declaration",
                expected_profile_revision: 1,
                operation_id: "stable-operation",
                operation: AuthorityOperationClass::StreamProfileEnable,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap_err();
        assert!(
            stale
                .to_string()
                .contains("lock changed before offline authority validation")
        );
        drop(lock);
        assert!(
            dir.path().join(CLUSTER_LOCK_FILE).exists(),
            "a stale guard must not delete the replacement owner's lock"
        );
    }

    #[tokio::test]
    async fn retirement_guard_requires_its_distinct_lock_and_offline_confirmation() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let state_cas = write_state(
            &dir,
            serde_json::json!({
                "version": 1,
                "state_revision": 9,
                "applied_revision": {
                    "config_digest": "retirement-config",
                    "resources": {
                        "graph.knowledge": { "digest": "graph-digest" }
                    }
                }
            }),
        );
        let storage = storage_handle_for_uri(&root).unwrap();
        let lock_uri = format!("{root}/{CLUSTER_LOCK_FILE}");
        let graph_store = format!("{}/graphs/knowledge.omni", dir.path().display());

        let apply_lock = match acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap()
        {
            StateLockAcquire::Acquired(lock) => lock,
            StateLockAcquire::Held => panic!("fresh apply lock unexpectedly held"),
        };
        let wrong_lock = validate_offline_guard(
            &apply_lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 9,
                declaration_revision: "retirement-control-v1",
                declaration_digest: "retirement-control",
                expected_profile_revision: 4,
                operation_id: "retirement-operation",
                operation: AuthorityOperationClass::StreamAuthorityRetirement,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap_err();
        assert!(
            wrong_lock
                .to_string()
                .contains(STREAM_AUTHORITY_RETIREMENT_LOCK_OPERATION)
        );
        drop(apply_lock);

        let retirement_lock = match acquire_state_lock(
            &storage,
            &lock_uri,
            STREAM_AUTHORITY_RETIREMENT_LOCK_OPERATION,
        )
        .await
        .unwrap()
        {
            StateLockAcquire::Acquired(lock) => lock,
            StateLockAcquire::Held => panic!("fresh retirement lock unexpectedly held"),
        };
        let missing_confirmation = validate_offline_guard(
            &retirement_lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 9,
                declaration_revision: "retirement-control-v1",
                declaration_digest: "retirement-control",
                expected_profile_revision: 4,
                operation_id: "retirement-operation",
                operation: AuthorityOperationClass::StreamAuthorityRetirement,
                actor: "operator",
                confirm_stream_offline: false,
            },
        )
        .await
        .unwrap_err();
        assert!(
            missing_confirmation
                .to_string()
                .contains("confirm_stream_offline")
        );

        let guard = validate_offline_guard(
            &retirement_lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 9,
                declaration_revision: "retirement-control-v1",
                declaration_digest: "retirement-control",
                expected_profile_revision: 4,
                operation_id: "retirement-operation",
                operation: AuthorityOperationClass::StreamAuthorityRetirement,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            guard.operation(),
            AuthorityOperationClass::StreamAuthorityRetirement
        );
        assert_eq!(guard.operation().requested_streaming_enabled(), None);
    }

    #[tokio::test]
    async fn runtime_guard_rereads_enabled_state_and_registers_one_local_owner() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let state_cas = write_state(
            &dir,
            serde_json::json!({
                "version": 1,
                "state_revision": 11,
                "applied_revision": {
                    "config_digest": "config-revision",
                    "resources": {
                        "graph.knowledge": {
                            "digest": "graph-digest"
                        },
                        "streaming.knowledge": {
                            "digest": "stream-declaration",
                            "declaration_revision": "stream-declaration-v1",
                            "streaming_enabled": true,
                            "profile_mode": "ENABLED",
                            "profile_revision": 4
                        }
                    }
                }
            }),
        );
        let storage = storage_handle_for_uri(&root).unwrap();
        let graph_store = format!("{}/graphs/knowledge.omni", dir.path().display());
        let binding = validate_runtime_binding(
            &storage,
            &root,
            RuntimeBindingRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 11,
                declaration_revision: "stream-declaration-v1",
                declaration_digest: "stream-declaration",
                profile_mode: "ENABLED",
                profile_revision: 4,
            },
        )
        .await
        .unwrap();
        assert_eq!(binding.profile_revision(), 4);

        let first = mint_runtime_guard(binding.clone(), "server-boot-1", "runtime")
            .await
            .unwrap();
        let duplicate = mint_runtime_guard(binding.clone(), "server-boot-2", "runtime")
            .await
            .unwrap_err();
        assert!(matches!(
            duplicate,
            AuthorityError::RuntimeAlreadyRegistered { .. }
        ));
        let lock_uri = format!("{root}/{CLUSTER_LOCK_FILE}");
        let lock = match acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap()
        {
            StateLockAcquire::Acquired(lock) => lock,
            StateLockAcquire::Held => panic!("fresh apply lock unexpectedly held"),
        };
        let offline = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 11,
                declaration_revision: "next-stream-declaration-v1",
                declaration_digest: "next-stream-declaration",
                expected_profile_revision: 4,
                operation_id: "offline-while-running",
                operation: AuthorityOperationClass::StreamProfileDisable,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            offline,
            AuthorityError::RuntimeStillRegistered { .. }
        ));
        let block_control = validate_offline_guard(
            &lock,
            OfflineAuthorityRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &state_cas,
                state_revision: 11,
                declaration_revision: "stream-declaration-v1",
                declaration_digest: "stream-declaration",
                expected_profile_revision: 4,
                operation_id: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                operation: AuthorityOperationClass::StreamBlockControl,
                actor: "operator",
                confirm_stream_offline: true,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            block_control,
            AuthorityError::RuntimeStillRegistered { .. }
        ));
        drop(lock);
        drop(first);

        let second = mint_runtime_guard(binding.clone(), "server-boot-3", "runtime")
            .await
            .unwrap();
        assert_eq!(second.operation(), AuthorityOperationClass::StreamRuntime);
        assert_eq!(second.profile_revision(), 4);
        drop(second);

        let next_state_cas = write_state(
            &dir,
            serde_json::json!({
                "version": 1,
                "state_revision": 12,
                "applied_revision": {
                    "config_digest": "unrelated-config-change",
                    "resources": {
                        "graph.knowledge": {"digest": "graph-digest"},
                        "policy.audit": {"digest": "changed-policy-digest"},
                        "streaming.knowledge": {
                            "digest": "stream-declaration",
                            "declaration_revision": "stream-declaration-v1",
                            "streaming_enabled": true,
                            "profile_mode": "ENABLED",
                            "profile_revision": 4
                        }
                    }
                }
            }),
        );
        assert!(matches!(
            mint_runtime_guard(binding.clone(), "server-boot-4", "runtime").await,
            Err(AuthorityError::StateChanged)
        ));

        let refreshed_binding = validate_runtime_binding(
            &storage,
            &root,
            RuntimeBindingRequest {
                graph_id: "knowledge",
                graph_store_uri: &graph_store,
                expected_state_cas: &next_state_cas,
                state_revision: 12,
                declaration_revision: binding.declaration_revision(),
                declaration_digest: binding.declaration_digest(),
                profile_mode: binding.profile_mode(),
                profile_revision: binding.profile_revision(),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            refreshed_binding.declaration_revision(),
            "stream-declaration-v1",
            "an unrelated cluster-config revision must not renew the stream declaration"
        );
        let runtime_after_unrelated_change =
            mint_runtime_guard(refreshed_binding, "server-boot-5", "runtime")
                .await
                .unwrap();
        assert_eq!(runtime_after_unrelated_change.profile_revision(), 4);
    }
}
