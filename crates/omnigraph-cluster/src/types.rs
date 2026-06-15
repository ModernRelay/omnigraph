//! Public output/diagnostic types and internal state/sidecar/approval
//! models (moved verbatim from lib.rs in the modularization).

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Diagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub path: String,
    pub message: String,
}

impl Diagnostic {
    pub(crate) fn error(code: impl Into<String>, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            severity: DiagnosticSeverity::Error,
            path: path.into(),
            message: message.into(),
        }
    }

    pub(crate) fn warning(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            severity: DiagnosticSeverity::Warning,
            path: path.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResourceSummary {
    pub address: String,
    pub kind: String,
    pub digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dependency {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidateOutput {
    pub ok: bool,
    pub config_dir: String,
    pub config_file: String,
    pub resource_digests: BTreeMap<String, String>,
    pub resources: Vec<ResourceSummary>,
    pub dependencies: Vec<Dependency>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DesiredRevision {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StateObservations {
    pub state_path: String,
    pub lock_path: String,
    pub state_found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_config_digest: Option<String>,
    pub state_revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_cas: Option<String>,
    pub resource_count: usize,
    pub locked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_id: Option<String>,
    pub lock_acquired: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquired_lock_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_pid: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_age_seconds: Option<u64>,
}

impl StateObservations {
    pub(crate) fn observe_lock_metadata(&mut self, lock: &StateLockFile) {
        self.locked = true;
        self.lock_id = Some(lock.lock_id.clone());
        self.lock_operation = Some(lock.operation.clone());
        self.lock_created_at = Some(lock.created_at.clone());
        self.lock_pid = Some(lock.pid);
        self.lock_age_seconds = lock_age_seconds(&lock.created_at);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLifecycleStatus {
    Pending,
    Planned,
    Applying,
    Applied,
    Drifted,
    Blocked,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ResourceStatusRecord {
    pub status: ResourceLifecycleStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanOperation {
    Create,
    Update,
    Delete,
}

/// How `cluster apply` treats a planned change in the current stage.
///
/// `Applied` changes execute (config-only query/policy catalog writes).
/// `Derived` marks a `graph.<id>` composite-digest update that converges
/// automatically once its applied query digests land in state. `Deferred`
/// changes need a later phase (graph/schema lifecycle or schema content).
/// `Blocked` query/policy changes are gated by an unapplied or missing
/// dependency.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplyDisposition {
    Applied,
    Derived,
    Deferred,
    Blocked,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PlanChange {
    pub resource: String,
    pub operation: PlanOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disposition: Option<ApplyDisposition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// True for a policy change whose file digest is unchanged but whose
    /// `applies_to` bindings differ from the applied revision (including the
    /// pre-5A backfill case).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub binding_change: bool,
    /// For schema updates: the engine's migration plan against the live
    /// graph (RFC-004 §D7's data-aware preview). Absent when the preview is
    /// unavailable (warning `schema_preview_unavailable`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration: Option<SchemaMigrationPlan>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BlastRadius {
    pub resource: String,
    pub affected: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ApprovalRequirement {
    pub resource: String,
    pub reason: String,
    /// True when a valid (digest-matching, unconsumed) approval artifact is
    /// pending for this change.
    pub satisfied: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanOutput {
    pub ok: bool,
    pub config_dir: String,
    pub desired_revision: DesiredRevision,
    pub resource_digests: BTreeMap<String, String>,
    pub dependencies: Vec<Dependency>,
    pub state_observations: StateObservations,
    pub changes: Vec<PlanChange>,
    pub blast_radius: Vec<BlastRadius>,
    pub approvals_required: Vec<ApprovalRequirement>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusOutput {
    pub ok: bool,
    pub config_dir: String,
    pub state_observations: StateObservations,
    pub resource_digests: BTreeMap<String, String>,
    pub resource_statuses: BTreeMap<String, ResourceStatusRecord>,
    pub observations: BTreeMap<String, serde_json::Value>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateSyncOperation {
    Refresh,
    Import,
}

#[derive(Debug, Clone, Serialize)]
pub struct StateSyncOutput {
    pub ok: bool,
    pub operation: StateSyncOperation,
    pub config_dir: String,
    pub state_observations: StateObservations,
    pub resource_digests: BTreeMap<String, String>,
    pub resource_statuses: BTreeMap<String, ResourceStatusRecord>,
    pub observations: BTreeMap<String, serde_json::Value>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ForceUnlockOutput {
    pub ok: bool,
    pub config_dir: String,
    pub state_observations: StateObservations,
    pub lock_removed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

/// Output of config-only `cluster apply`. "Applied" means recorded in the
/// local cluster catalog (`__cluster/`); nothing applied here serves traffic —
/// the server still boots from `omnigraph.yaml` until the server-boot stage.
#[derive(Debug, Clone, Serialize)]
pub struct ApplyOutput {
    pub ok: bool,
    pub config_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    pub desired_revision: DesiredRevision,
    pub state_observations: StateObservations,
    /// Every planned change, with `disposition`/`reason` always populated.
    pub changes: Vec<PlanChange>,
    pub applied_count: usize,
    /// Deferred + Blocked changes (Derived composite updates count as neither).
    pub deferred_count: usize,
    /// True when state matches the desired revision after this apply.
    pub converged: bool,
    /// False for a no-op re-apply: state bytes (and revision) were left untouched.
    pub state_written: bool,
    /// The statuses as persisted: post-apply on success, the pre-apply on-disk
    /// snapshot when the state write fails (never unpersisted in-memory state).
    pub resource_statuses: BTreeMap<String, ResourceStatusRecord>,
    pub diagnostics: Vec<Diagnostic>,
}

/// A digest-bound human approval for an irreversible operation (RFC-004
/// §D4). Written by `cluster approve`, consumed by apply. The file is never
/// deleted on consumption — it is rewritten with `consumed_at` and also
/// summarized into the state ledger's `approval_records`, so the audit fact
/// survives the loss of either store (axiom 11).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ApprovalArtifact {
    pub(crate) schema_version: u32,
    pub(crate) approval_id: String,
    pub(crate) resource: String,
    pub(crate) operation: String,
    pub(crate) reason: String,
    pub(crate) bound_config_digest: String,
    #[serde(default)]
    pub(crate) bound_before_digest: Option<String>,
    #[serde(default)]
    pub(crate) bound_after_digest: Option<String>,
    pub(crate) approved_by: String,
    pub(crate) created_at: String,
    #[serde(default)]
    pub(crate) consumed_at: Option<String>,
    #[serde(default)]
    pub(crate) consumed_by_operation: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApproveOutput {
    pub ok: bool,
    pub config_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<PlanOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone)]
pub(crate) struct DesiredCluster {
    pub(crate) config_dir: PathBuf,
    pub(crate) config_digest: String,
    /// The declared `storage:` root, if any (None ⇒ the config dir itself).
    pub(crate) storage_root: Option<String>,
    pub(crate) state_lock: bool,
    pub(crate) graphs: Vec<DesiredGraph>,
    pub(crate) resource_digests: BTreeMap<String, String>,
    pub(crate) resources: Vec<ResourceSummary>,
    pub(crate) dependencies: Vec<Dependency>,
    /// `policy.<name>` address -> normalized applies_to refs.
    pub(crate) policy_bindings: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone)]
pub(crate) struct DesiredGraph {
    pub(crate) id: String,
    pub(crate) schema_digest: String,
}

#[derive(Debug)]
pub(crate) struct ParsedConfig {
    pub(crate) raw: Option<RawClusterConfig>,
    pub(crate) diagnostics: Vec<Diagnostic>,
    pub(crate) config_dir: PathBuf,
    pub(crate) config_file: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct ClusterSettings {
    pub(crate) state_lock: bool,
    pub(crate) storage_root: Option<String>,
}

#[derive(Debug)]
pub(crate) struct LoadOutcome {
    pub(crate) desired: Option<DesiredCluster>,
    pub(crate) diagnostics: Vec<Diagnostic>,
    pub(crate) config_dir: PathBuf,
    pub(crate) config_file: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawClusterConfig {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) metadata: Metadata,
    /// Storage root URI for everything the cluster stores: the state
    /// ledger, catalog, sidecars, approvals, and derived graph roots.
    /// Absent ⇒ `file://<config-dir>` (the original layout, byte-compatible).
    /// `s3://bucket/prefix` puts the whole cluster on object storage.
    #[serde(default)]
    pub(crate) storage: Option<String>,
    #[serde(default)]
    pub(crate) state: StateConfig,
    #[serde(default)]
    pub(crate) graphs: BTreeMap<String, GraphConfig>,
    #[serde(default)]
    pub(crate) policies: BTreeMap<String, PolicyConfig>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Metadata {
    pub(crate) name: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StateConfig {
    pub(crate) backend: Option<String>,
    pub(crate) lock: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphConfig {
    pub(crate) schema: PathBuf,
    #[serde(default)]
    pub(crate) queries: QueriesDecl,
    /// Optional per-graph embedding provider profile (RFC-012 Phase 5).
    #[serde(default)]
    pub(crate) embeddings: Option<EmbeddingProfile>,
}

/// A graph's embedding provider profile (RFC-012 Phase 5). `provider`/`base_url`/
/// `model` default exactly as the engine's `EmbeddingConfig::from_env` does;
/// `api_key` is a `${NAME}` env reference resolved at serving boot, never an
/// inline secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingProfile {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    pub api_key: String,
}

impl EmbeddingProfile {
    /// Resolve into an engine `EmbeddingConfig`, reading the `${NAME}` api-key
    /// reference from process env. Errors if `api_key` is not a `${NAME}`
    /// reference or the named var is unset.
    pub fn resolve(&self) -> Result<omnigraph::embedding::EmbeddingConfig, String> {
        let api_key = resolve_secret_ref(&self.api_key)?;
        omnigraph::embedding::EmbeddingConfig::from_parts(
            self.provider.as_deref(),
            self.base_url.clone(),
            self.model.clone(),
            api_key,
        )
        .map_err(|e| e.to_string())
    }
}

/// Resolve a `${NAME}` secret reference from process env. Rejects an inline value
/// (anything not wrapped in `${…}`) so secrets never sit in the cluster config.
fn resolve_secret_ref(value: &str) -> Result<String, String> {
    let name = value
        .trim()
        .strip_prefix("${")
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            format!("embedding api_key must be a ${{NAME}} env reference, got '{}'", value.trim())
        })?;
    std::env::var(name).map_err(|_| format!("embedding api_key env var '{name}' is not set"))
}

/// How a graph declares its stored queries. Terraform-style: the `.gq`
/// files ARE the declaration — point at them (or a directory) and every
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryConfig {
    pub(crate) file: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PolicyConfig {
    pub(crate) file: PathBuf,
    pub(crate) applies_to: Vec<String>,
}

// Stage 2A/2B accept these forward-compatible state sections so existing
// ledgers won't churn while approval/recovery semantics are staged later.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClusterState {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) state_revision: u64,
    pub(crate) applied_revision: AppliedRevisionState,
    #[serde(default)]
    pub(crate) resource_statuses: BTreeMap<String, ResourceStatusRecord>,
    #[serde(default)]
    pub(crate) approval_records: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub(crate) recovery_records: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub(crate) observations: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AppliedRevisionState {
    #[serde(default)]
    pub(crate) config_digest: Option<String>,
    #[serde(default)]
    pub(crate) resources: BTreeMap<String, StateResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StateResource {
    pub(crate) digest: String,
    /// Policy resources only: the applied `applies_to` bindings, normalized
    /// to typed refs (`cluster` | `graph.<id>`). Recorded so the state
    /// ledger is serving-sufficient for the Phase-5 server boot (RFC-005
    /// §D3). Absent on pre-5A entries (backfilled by the next apply) and on
    /// non-policy resources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) applies_to: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StateLockFile {
    pub(crate) version: u32,
    pub(crate) lock_id: String,
    pub(crate) operation: String,
    pub(crate) created_at: String,
    pub(crate) pid: u32,
}

/// Recovery-intent record for a graph-moving apply operation (RFC-004 §D2).
/// Written under the state lock before the engine call that can create or
/// move a graph manifest; deleted only after the cluster state CAS that
/// records the outcome lands. The sweep (§D3) classifies survivors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoverySidecar {
    pub(crate) schema_version: u32,
    pub(crate) operation_id: String,
    pub(crate) started_at: String,
    #[serde(default)]
    pub(crate) actor: Option<String>,
    pub(crate) kind: RecoverySidecarKind,
    pub(crate) graph_id: String,
    pub(crate) graph_uri: String,
    #[serde(default)]
    pub(crate) observed_manifest_version: Option<u64>,
    #[serde(default)]
    pub(crate) expected_manifest_version: Option<u64>,
    pub(crate) desired_schema_digest: String,
    #[serde(default)]
    pub(crate) state_cas_base: Option<String>,
    /// For graph_delete: the approval this operation consumes; lets a sweep
    /// roll-forward consume it too.
    #[serde(default)]
    pub(crate) approval_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoverySidecarKind {
    GraphCreate,
    SchemaApply,
    GraphDelete,
}

#[derive(Debug, Default)]
pub(crate) struct SweepOutcome {
    /// Graphs whose sidecar was kept (rows 5/6): graph-moving work for them
    /// is blocked until the operator repairs and re-observes.
    pub(crate) pending_graphs: BTreeSet<String>,
    /// Sidecars whose outcome is recorded (rows 2/4): deleted only after the
    /// command's state write lands, so a CAS failure re-sweeps them.
    /// Store URIs (the storage layer addresses everything by URI).
    pub(crate) completed_sidecars: Vec<String>,
    /// Approval artifacts consumed by a roll-forward (delete row 7b): their
    /// files are rewritten with consumed_at only after the state write lands.
    pub(crate) consumed_approvals: Vec<String>,
}

#[cfg(test)]
mod embedding_profile_tests {
    use super::EmbeddingProfile;

    #[test]
    fn resolves_secret_from_env_and_applies_defaults() {
        // SAFETY: a unique var name, no concurrent reader.
        unsafe { std::env::set_var("OG_TEST_EMBED_KEY_A", "secret-x") };
        let profile = EmbeddingProfile {
            provider: Some("openai-compatible".to_string()),
            base_url: None,
            model: Some("m".to_string()),
            api_key: "${OG_TEST_EMBED_KEY_A}".to_string(),
        };
        let config = profile.resolve().unwrap();
        assert_eq!(config.api_key, "secret-x");
        assert_eq!(config.model, "m");
        unsafe { std::env::remove_var("OG_TEST_EMBED_KEY_A") };
    }

    #[test]
    fn rejects_inline_api_key() {
        let profile = EmbeddingProfile {
            provider: None,
            base_url: None,
            model: None,
            api_key: "sk-inline".to_string(),
        };
        let err = profile.resolve().unwrap_err();
        assert!(err.contains("${NAME}"), "got: {err}");
    }

    #[test]
    fn errors_on_unset_secret() {
        let profile = EmbeddingProfile {
            provider: None,
            base_url: None,
            model: None,
            api_key: "${OG_TEST_DEFINITELY_UNSET_VAR}".to_string(),
        };
        let err = profile.resolve().unwrap_err();
        assert!(err.contains("not set"), "got: {err}");
    }

    #[test]
    fn rejects_unknown_provider() {
        unsafe { std::env::set_var("OG_TEST_EMBED_KEY_B", "x") };
        let profile = EmbeddingProfile {
            provider: Some("cohere".to_string()),
            base_url: None,
            model: None,
            api_key: "${OG_TEST_EMBED_KEY_B}".to_string(),
        };
        let err = profile.resolve().unwrap_err();
        assert!(err.contains("unknown embedding provider"), "got: {err}");
        unsafe { std::env::remove_var("OG_TEST_EMBED_KEY_B") };
    }
}
