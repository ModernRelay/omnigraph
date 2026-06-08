use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;

use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph_compiler::build_catalog;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::query::typecheck::typecheck_query_decl;
use omnigraph_compiler::schema::parser::parse_schema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

pub const CLUSTER_CONFIG_FILE: &str = "cluster.yaml";
pub const CLUSTER_GRAPHS_DIR: &str = "graphs";
pub const CLUSTER_STATE_DIR: &str = "__cluster";
pub const CLUSTER_STATE_FILE: &str = "__cluster/state.json";
pub const CLUSTER_LOCK_FILE: &str = "__cluster/lock.json";

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
    fn error(code: impl Into<String>, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            severity: DiagnosticSeverity::Error,
            path: path.into(),
            message: message.into(),
        }
    }

    fn warning(
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
    fn observe_lock_metadata(&mut self, lock: &StateLockFile) {
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

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PlanChange {
    pub resource: String,
    pub operation: PlanOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_digest: Option<String>,
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

#[derive(Debug, Clone)]
struct DesiredCluster {
    config_dir: PathBuf,
    config_digest: String,
    state_lock: bool,
    graphs: Vec<DesiredGraph>,
    resource_digests: BTreeMap<String, String>,
    resources: Vec<ResourceSummary>,
    dependencies: Vec<Dependency>,
}

#[derive(Debug, Clone)]
struct DesiredGraph {
    id: String,
    schema_digest: String,
}

#[derive(Debug)]
struct ParsedConfig {
    raw: Option<RawClusterConfig>,
    diagnostics: Vec<Diagnostic>,
    config_dir: PathBuf,
    config_file: PathBuf,
}

#[derive(Debug, Clone, Copy)]
struct ClusterSettings {
    state_lock: bool,
}

#[derive(Debug)]
struct LoadOutcome {
    desired: Option<DesiredCluster>,
    diagnostics: Vec<Diagnostic>,
    config_dir: PathBuf,
    config_file: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawClusterConfig {
    version: u32,
    #[serde(default)]
    metadata: Metadata,
    #[serde(default)]
    state: StateConfig,
    #[serde(default)]
    graphs: BTreeMap<String, GraphConfig>,
    #[serde(default)]
    policies: BTreeMap<String, PolicyConfig>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    name: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateConfig {
    backend: Option<String>,
    lock: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphConfig {
    schema: PathBuf,
    #[serde(default)]
    queries: BTreeMap<String, QueryConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryConfig {
    file: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyConfig {
    file: PathBuf,
    applies_to: Vec<String>,
}

// Stage 2A/2B accept these forward-compatible state sections so existing
// ledgers won't churn while approval/recovery semantics are staged later.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterState {
    version: u32,
    #[serde(default)]
    state_revision: u64,
    applied_revision: AppliedRevisionState,
    #[serde(default)]
    resource_statuses: BTreeMap<String, ResourceStatusRecord>,
    #[serde(default)]
    approval_records: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    recovery_records: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    observations: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppliedRevisionState {
    #[serde(default)]
    config_digest: Option<String>,
    #[serde(default)]
    resources: BTreeMap<String, StateResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateResource {
    digest: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateLockFile {
    version: u32,
    lock_id: String,
    operation: String,
    created_at: String,
    pid: u32,
}

#[derive(Debug)]
struct LocalStateBackend {
    state_dir: PathBuf,
    state_path: PathBuf,
    lock_path: PathBuf,
}

#[derive(Debug)]
struct StateSnapshot {
    state: Option<ClusterState>,
    state_cas: Option<String>,
}

#[derive(Debug)]
struct StateLockGuard {
    path: PathBuf,
}

pub fn validate_config_dir(config_dir: impl AsRef<Path>) -> ValidateOutput {
    let outcome = load_desired(config_dir.as_ref());
    let (resource_digests, resources, dependencies) = match outcome.desired {
        Some(desired) => (
            desired.resource_digests,
            desired.resources,
            desired.dependencies,
        ),
        None => (BTreeMap::new(), Vec::new(), Vec::new()),
    };
    let ok = !has_errors(&outcome.diagnostics);

    ValidateOutput {
        ok,
        config_dir: display_path(&outcome.config_dir),
        config_file: display_path(&outcome.config_file),
        resource_digests,
        resources,
        dependencies,
        diagnostics: outcome.diagnostics,
    }
}

pub fn plan_config_dir(config_dir: impl AsRef<Path>) -> PlanOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let backend = LocalStateBackend::new(&outcome.config_dir);
    let mut observations = backend.observations();

    let Some(desired) = outcome.desired else {
        return PlanOutput {
            ok: false,
            config_dir: display_path(&outcome.config_dir),
            desired_revision: DesiredRevision {
                config_digest: None,
            },
            resource_digests: BTreeMap::new(),
            dependencies: Vec::new(),
            state_observations: observations,
            changes: Vec::new(),
            blast_radius: Vec::new(),
            approvals_required: Vec::new(),
            diagnostics,
        };
    };

    if has_errors(&diagnostics) {
        return PlanOutput {
            ok: false,
            config_dir: display_path(&desired.config_dir),
            desired_revision: DesiredRevision {
                config_digest: Some(desired.config_digest),
            },
            resource_digests: desired.resource_digests,
            dependencies: desired.dependencies,
            state_observations: observations,
            changes: Vec::new(),
            blast_radius: Vec::new(),
            approvals_required: Vec::new(),
            diagnostics,
        };
    }

    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock("plan", &mut observations) {
            Ok(guard) => Some(guard),
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                None
            }
        }
    } else {
        diagnostics.push(Diagnostic::warning(
            "state_lock_disabled",
            "state.lock",
            "state.lock is false; plan read state without acquiring the cluster state lock",
        ));
        None
    };

    let mut prior_resources = BTreeMap::new();
    if !has_errors(&diagnostics) {
        match backend.read_state(&mut observations) {
            Ok(snapshot) => {
                if let Some(state) = snapshot.state {
                    prior_resources = state_resource_digests(&state);
                }
            }
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
    }

    let changes = if has_errors(&diagnostics) {
        Vec::new()
    } else {
        diff_resources(&prior_resources, &desired.resource_digests)
    };
    let blast_radius = compute_blast_radius(&changes, &desired.dependencies);
    let approvals_required = compute_approvals(&changes);
    let ok = !has_errors(&diagnostics);

    PlanOutput {
        ok,
        config_dir: display_path(&desired.config_dir),
        desired_revision: DesiredRevision {
            config_digest: Some(desired.config_digest),
        },
        resource_digests: desired.resource_digests,
        dependencies: desired.dependencies,
        state_observations: observations,
        changes,
        blast_radius,
        approvals_required,
        diagnostics,
    }
}

pub fn status_config_dir(config_dir: impl AsRef<Path>) -> StatusOutput {
    let parsed = parse_cluster_config(config_dir.as_ref());
    let mut diagnostics = parsed.diagnostics;
    let backend = LocalStateBackend::new(&parsed.config_dir);
    let mut observations = backend.observations();
    backend.observe_lock(&mut observations, &mut diagnostics);

    let mut resource_digests = BTreeMap::new();
    let mut resource_statuses = BTreeMap::new();
    let mut state_observation_records = BTreeMap::new();

    if let Some(raw) = parsed.raw.as_ref() {
        let _settings = validate_cluster_header(raw, &mut diagnostics);
        if !has_errors(&diagnostics) {
            match backend.read_state(&mut observations) {
                Ok(snapshot) => {
                    if let Some(state) = snapshot.state {
                        resource_digests = state_resource_digests(&state);
                        resource_statuses = state.resource_statuses;
                        state_observation_records = state.observations;
                    } else {
                        diagnostics.push(Diagnostic::warning(
                            "state_missing",
                            CLUSTER_STATE_FILE,
                            "state.json is missing; no applied cluster revision has been recorded",
                        ));
                    }
                }
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
    }

    StatusOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&parsed.config_dir),
        state_observations: observations,
        resource_digests,
        resource_statuses,
        observations: state_observation_records,
        diagnostics,
    }
}

pub fn force_unlock_config_dir(
    config_dir: impl AsRef<Path>,
    lock_id: impl AsRef<str>,
) -> ForceUnlockOutput {
    let parsed = parse_cluster_config(config_dir.as_ref());
    let mut diagnostics = parsed.diagnostics;
    let backend = LocalStateBackend::new(&parsed.config_dir);
    let mut observations = backend.observations();
    let mut lock_removed = false;

    if let Some(raw) = parsed.raw.as_ref() {
        let _settings = validate_cluster_header(raw, &mut diagnostics);
        if !has_errors(&diagnostics) {
            match backend.force_unlock(lock_id.as_ref(), &mut observations) {
                Ok(()) => lock_removed = true,
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
    }

    ForceUnlockOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&parsed.config_dir),
        state_observations: observations,
        lock_removed,
        diagnostics,
    }
}

pub async fn refresh_config_dir(config_dir: impl AsRef<Path>) -> StateSyncOutput {
    sync_config_dir(config_dir.as_ref(), StateSyncOperation::Refresh).await
}

pub async fn import_config_dir(config_dir: impl AsRef<Path>) -> StateSyncOutput {
    sync_config_dir(config_dir.as_ref(), StateSyncOperation::Import).await
}

async fn sync_config_dir(config_dir: &Path, operation: StateSyncOperation) -> StateSyncOutput {
    let outcome = load_desired(config_dir);
    let mut diagnostics = outcome.diagnostics;
    let backend = LocalStateBackend::new(&outcome.config_dir);
    let mut observations = backend.observations();

    let Some(desired) = outcome.desired else {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&outcome.config_dir),
            state_observations: observations,
            resource_digests: BTreeMap::new(),
            resource_statuses: BTreeMap::new(),
            observations: BTreeMap::new(),
            diagnostics,
        };
    };

    if has_errors(&diagnostics) {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&desired.config_dir),
            state_observations: observations,
            resource_digests: desired.resource_digests,
            resource_statuses: BTreeMap::new(),
            observations: BTreeMap::new(),
            diagnostics,
        };
    }

    let operation_label = state_sync_operation_label(operation);
    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock(operation_label, &mut observations) {
            Ok(guard) => Some(guard),
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                None
            }
        }
    } else {
        diagnostics.push(Diagnostic::warning(
            "state_lock_disabled",
            "state.lock",
            format!(
                "state.lock is false; {operation_label} wrote state without acquiring the cluster state lock"
            ),
        ));
        None
    };

    if has_errors(&diagnostics) {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&desired.config_dir),
            state_observations: observations,
            resource_digests: desired.resource_digests,
            resource_statuses: BTreeMap::new(),
            observations: BTreeMap::new(),
            diagnostics,
        };
    }

    let snapshot = match backend.read_state(&mut observations) {
        Ok(snapshot) => snapshot,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return StateSyncOutput {
                ok: false,
                operation,
                config_dir: display_path(&desired.config_dir),
                state_observations: observations,
                resource_digests: desired.resource_digests,
                resource_statuses: BTreeMap::new(),
                observations: BTreeMap::new(),
                diagnostics,
            };
        }
    };

    let expected_cas = snapshot.state_cas;
    let mut state = match (operation, snapshot.state) {
        (StateSyncOperation::Refresh, Some(state)) => state,
        (StateSyncOperation::Refresh, None) => {
            diagnostics.push(Diagnostic::error(
                "state_missing",
                CLUSTER_STATE_FILE,
                "refresh requires an existing state.json; run `cluster import` to bootstrap state",
            ));
            return StateSyncOutput {
                ok: false,
                operation,
                config_dir: display_path(&desired.config_dir),
                state_observations: observations,
                resource_digests: BTreeMap::new(),
                resource_statuses: BTreeMap::new(),
                observations: BTreeMap::new(),
                diagnostics,
            };
        }
        (StateSyncOperation::Import, Some(state)) => {
            diagnostics.push(Diagnostic::error(
                "state_already_exists",
                CLUSTER_STATE_FILE,
                "import creates initial state only when state.json is missing; use `cluster refresh` for an existing state ledger",
            ));
            return StateSyncOutput {
                ok: false,
                operation,
                config_dir: display_path(&desired.config_dir),
                state_observations: observations,
                resource_digests: state_resource_digests(&state),
                resource_statuses: state.resource_statuses,
                observations: state.observations,
                diagnostics,
            };
        }
        (StateSyncOperation::Import, None) => initial_import_state(&desired),
    };

    let graph_error_count = observe_declared_graphs(&desired, &mut state).await;
    if graph_error_count > 0 {
        diagnostics.push(Diagnostic::error(
            "graph_observation_error",
            CLUSTER_GRAPHS_DIR,
            format!("{graph_error_count} graph observation(s) failed"),
        ));
    }

    if operation == StateSyncOperation::Import && has_errors(&diagnostics) {
        return StateSyncOutput {
            ok: false,
            operation,
            config_dir: display_path(&desired.config_dir),
            state_observations: observations,
            resource_digests: state_resource_digests(&state),
            resource_statuses: state.resource_statuses,
            observations: state.observations,
            diagnostics,
        };
    }

    if operation == StateSyncOperation::Import {
        state.state_revision = 1;
    } else {
        state.state_revision = state.state_revision.saturating_add(1);
    }

    match backend.write_state(&state, expected_cas.as_deref(), &mut observations) {
        Ok(()) => {}
        Err(diagnostic) => diagnostics.push(diagnostic),
    }

    let resource_digests = state_resource_digests(&state);
    let ok = !has_errors(&diagnostics);

    StateSyncOutput {
        ok,
        operation,
        config_dir: display_path(&desired.config_dir),
        state_observations: observations,
        resource_digests,
        resource_statuses: state.resource_statuses,
        observations: state.observations,
        diagnostics,
    }
}

fn parse_cluster_config(config_dir: &Path) -> ParsedConfig {
    let config_dir = config_dir.to_path_buf();
    let config_file = config_dir.join(CLUSTER_CONFIG_FILE);
    let mut diagnostics = Vec::new();

    if !config_dir.is_dir() {
        diagnostics.push(Diagnostic::error(
            "config_dir_not_found",
            display_path(&config_dir),
            "`--config` must point at a directory containing cluster.yaml",
        ));
        return ParsedConfig {
            raw: None,
            diagnostics,
            config_dir,
            config_file,
        };
    }

    let text = match fs::read_to_string(&config_file) {
        Ok(text) => text,
        Err(err) => {
            diagnostics.push(Diagnostic::error(
                "cluster_config_read_error",
                CLUSTER_CONFIG_FILE,
                format!("could not read cluster.yaml: {err}"),
            ));
            return ParsedConfig {
                raw: None,
                diagnostics,
                config_dir,
                config_file,
            };
        }
    };

    diagnostics.extend(duplicate_key_diagnostics(&text));
    diagnostics.extend(future_field_diagnostics(&text));
    if has_errors(&diagnostics) {
        return ParsedConfig {
            raw: None,
            diagnostics,
            config_dir,
            config_file,
        };
    }

    let raw = match serde_yaml::from_str::<RawClusterConfig>(&text) {
        Ok(raw) => Some(raw),
        Err(err) => {
            diagnostics.push(Diagnostic::error(
                "invalid_cluster_yaml",
                CLUSTER_CONFIG_FILE,
                format!("could not parse cluster.yaml: {err}"),
            ));
            None
        }
    };

    ParsedConfig {
        raw,
        diagnostics,
        config_dir,
        config_file,
    }
}

fn validate_cluster_header(
    raw: &RawClusterConfig,
    diagnostics: &mut Vec<Diagnostic>,
) -> ClusterSettings {
    if raw.version != 1 {
        diagnostics.push(Diagnostic::error(
            "unsupported_cluster_config_version",
            "version",
            format!(
                "unsupported cluster config version {}; this build supports version 1",
                raw.version
            ),
        ));
    }
    if let Some(name) = raw.metadata.name.as_deref() {
        if name.trim().is_empty() {
            diagnostics.push(Diagnostic::error(
                "empty_metadata_name",
                "metadata.name",
                "metadata.name must not be empty when provided",
            ));
        }
    }
    if let Some(backend) = raw.state.backend.as_deref() {
        if backend != "cluster" {
            diagnostics.push(Diagnostic::error(
                "unsupported_state_backend",
                "state.backend",
                "Stage 2C supports only omitted state.backend or `cluster`",
            ));
        }
    }

    ClusterSettings {
        state_lock: raw.state.lock.unwrap_or(true),
    }
}

impl LocalStateBackend {
    fn new(config_dir: &Path) -> Self {
        let state_dir = config_dir.join(CLUSTER_STATE_DIR);
        Self {
            state_path: config_dir.join(CLUSTER_STATE_FILE),
            lock_path: config_dir.join(CLUSTER_LOCK_FILE),
            state_dir,
        }
    }

    fn observations(&self) -> StateObservations {
        StateObservations {
            state_path: display_path(&self.state_path),
            lock_path: display_path(&self.lock_path),
            state_found: false,
            applied_config_digest: None,
            state_revision: 0,
            state_cas: None,
            resource_count: 0,
            locked: false,
            lock_id: None,
            lock_acquired: false,
            acquired_lock_id: None,
            lock_operation: None,
            lock_created_at: None,
            lock_pid: None,
            lock_age_seconds: None,
        }
    }

    fn read_state(
        &self,
        observations: &mut StateObservations,
    ) -> Result<StateSnapshot, Diagnostic> {
        let text = match fs::read_to_string(&self.state_path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(StateSnapshot {
                    state: None,
                    state_cas: None,
                });
            }
            Err(err) => {
                return Err(Diagnostic::error(
                    "state_read_error",
                    CLUSTER_STATE_FILE,
                    format!("could not read state file: {err}"),
                ));
            }
        };

        observations.state_found = true;
        let state_cas = format!("sha256:{}", sha256_hex(text.as_bytes()));
        observations.state_cas = Some(state_cas.clone());

        let state = serde_json::from_str::<ClusterState>(&text).map_err(|err| {
            Diagnostic::error(
                "invalid_state_json",
                CLUSTER_STATE_FILE,
                format!("could not parse state JSON: {err}"),
            )
        })?;

        if state.version != 1 {
            return Err(Diagnostic::error(
                "unsupported_state_version",
                "state.version",
                format!(
                    "unsupported cluster state version {}; this build supports version 1",
                    state.version
                ),
            ));
        }

        observations.applied_config_digest = state.applied_revision.config_digest.clone();
        observations.state_revision = state.state_revision;
        observations.resource_count = state.applied_revision.resources.len();

        Ok(StateSnapshot {
            state: Some(state),
            state_cas: Some(state_cas),
        })
    }

    fn write_state(
        &self,
        state: &ClusterState,
        expected_cas: Option<&str>,
        observations: &mut StateObservations,
    ) -> Result<(), Diagnostic> {
        fs::create_dir_all(&self.state_dir).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_DIR,
                format!("could not create cluster state directory: {err}"),
            )
        })?;

        let current_cas = self.current_state_cas()?;
        if current_cas.as_deref() != expected_cas {
            return Err(Diagnostic::error(
                "state_cas_mismatch",
                CLUSTER_STATE_FILE,
                "state.json changed while the command was running; re-run the command against the latest state",
            ));
        }

        let mut payload = serde_json::to_string_pretty(state).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not encode state JSON: {err}"),
            )
        })?;
        payload.push('\n');

        let tmp_path = self
            .state_dir
            .join(format!("state.json.tmp.{}", Ulid::new()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|err| {
                Diagnostic::error(
                    "state_write_error",
                    display_path(&tmp_path),
                    format!("could not create temporary state file: {err}"),
                )
            })?;
        file.write_all(payload.as_bytes()).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                display_path(&tmp_path),
                format!("could not write temporary state file: {err}"),
            )
        })?;
        file.sync_all().map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                display_path(&tmp_path),
                format!("could not sync temporary state file: {err}"),
            )
        })?;
        drop(file);

        if let Err(err) = fs::rename(&tmp_path, &self.state_path) {
            let _ = fs::remove_file(&tmp_path);
            return Err(Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not replace state.json atomically: {err}"),
            ));
        }

        let written = fs::read_to_string(&self.state_path).map_err(|err| {
            Diagnostic::error(
                "state_write_error",
                CLUSTER_STATE_FILE,
                format!("could not read state.json after write: {err}"),
            )
        })?;
        observations.state_found = true;
        observations.applied_config_digest = state.applied_revision.config_digest.clone();
        observations.state_revision = state.state_revision;
        observations.state_cas = Some(format!("sha256:{}", sha256_hex(written.as_bytes())));
        observations.resource_count = state.applied_revision.resources.len();

        Ok(())
    }

    fn current_state_cas(&self) -> Result<Option<String>, Diagnostic> {
        match fs::read(&self.state_path) {
            Ok(bytes) => Ok(Some(format!("sha256:{}", sha256_hex(&bytes)))),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(Diagnostic::error(
                "state_read_error",
                CLUSTER_STATE_FILE,
                format!("could not read state file for CAS check: {err}"),
            )),
        }
    }

    fn acquire_lock(
        &self,
        operation: &str,
        observations: &mut StateObservations,
    ) -> Result<StateLockGuard, Diagnostic> {
        fs::create_dir_all(&self.state_dir).map_err(|err| {
            Diagnostic::error(
                "state_lock_error",
                CLUSTER_STATE_DIR,
                format!("could not create cluster state directory: {err}"),
            )
        })?;

        let lock_id = Ulid::new().to_string();
        let lock = StateLockFile {
            version: 1,
            lock_id: lock_id.clone(),
            operation: operation.to_string(),
            created_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
            pid: process::id(),
        };
        let payload = serde_json::to_string_pretty(&lock).map_err(|err| {
            Diagnostic::error(
                "state_lock_error",
                CLUSTER_LOCK_FILE,
                format!("could not encode state lock: {err}"),
            )
        })?;

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.lock_path)
        {
            Ok(mut file) => {
                if let Err(err) = file.write_all(payload.as_bytes()) {
                    // No guard exists yet, so clean up the create-new file here
                    // instead of leaving a stale partial lock for the next run.
                    drop(file);
                    let _ = fs::remove_file(&self.lock_path);
                    return Err(Diagnostic::error(
                        "state_lock_error",
                        CLUSTER_LOCK_FILE,
                        format!("could not write state lock: {err}"),
                    ));
                }
                observations.lock_acquired = true;
                observations.acquired_lock_id = Some(lock_id.clone());
                Ok(StateLockGuard {
                    path: self.lock_path.clone(),
                })
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                self.observe_lock_metadata_lossy(observations);
                Err(Diagnostic::error(
                    "state_lock_held",
                    CLUSTER_LOCK_FILE,
                    state_lock_held_message(observations),
                ))
            }
            Err(err) => Err(Diagnostic::error(
                "state_lock_error",
                CLUSTER_LOCK_FILE,
                format!("could not acquire state lock: {err}"),
            )),
        }
    }

    fn force_unlock(
        &self,
        requested_lock_id: &str,
        observations: &mut StateObservations,
    ) -> Result<(), Diagnostic> {
        let text = match fs::read_to_string(&self.lock_path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Err(Diagnostic::error(
                    "state_lock_missing",
                    CLUSTER_LOCK_FILE,
                    "cluster state lock is not present; nothing was unlocked",
                ));
            }
            Err(err) => {
                return Err(Diagnostic::error(
                    "state_lock_read_error",
                    CLUSTER_LOCK_FILE,
                    format!("could not read state lock: {err}"),
                ));
            }
        };
        observations.locked = true;
        let lock = parse_lock_file_for_unlock(&text)?;
        observations.observe_lock_metadata(&lock);

        if lock.lock_id != requested_lock_id {
            return Err(Diagnostic::error(
                "state_lock_id_mismatch",
                CLUSTER_LOCK_FILE,
                format!(
                    "cluster state lock id is {}; refusing to unlock with requested id {requested_lock_id}",
                    lock.lock_id
                ),
            ));
        }

        fs::remove_file(&self.lock_path).map_err(|err| {
            Diagnostic::error(
                "state_unlock_error",
                CLUSTER_LOCK_FILE,
                format!("could not remove state lock: {err}"),
            )
        })
    }

    fn observe_lock(
        &self,
        observations: &mut StateObservations,
        diagnostics: &mut Vec<Diagnostic>,
    ) {
        if self.lock_path.exists() {
            observations.locked = true;
            match fs::read_to_string(&self.lock_path) {
                Ok(text) => match serde_json::from_str::<StateLockFile>(&text) {
                    Ok(lock) if lock.version == 1 => {
                        observations.observe_lock_metadata(&lock);
                    }
                    Ok(lock) => diagnostics.push(Diagnostic::warning(
                        "unsupported_state_lock_version",
                        CLUSTER_LOCK_FILE,
                        format!("unsupported cluster state lock version {}", lock.version),
                    )),
                    Err(err) => diagnostics.push(Diagnostic::warning(
                        "invalid_state_lock",
                        CLUSTER_LOCK_FILE,
                        format!("could not parse state lock: {err}"),
                    )),
                },
                Err(err) => diagnostics.push(Diagnostic::warning(
                    "state_lock_read_error",
                    CLUSTER_LOCK_FILE,
                    format!("could not read state lock: {err}"),
                )),
            }
        }
    }

    fn observe_lock_metadata_lossy(&self, observations: &mut StateObservations) {
        observations.locked = true;
        if let Ok(text) = fs::read_to_string(&self.lock_path) {
            if let Ok(lock) = serde_json::from_str::<StateLockFile>(&text) {
                if lock.version == 1 {
                    observations.observe_lock_metadata(&lock);
                }
            }
        }
    }
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn parse_lock_file_for_unlock(text: &str) -> Result<StateLockFile, Diagnostic> {
    let lock = serde_json::from_str::<StateLockFile>(text).map_err(|err| {
        Diagnostic::error(
            "invalid_state_lock",
            CLUSTER_LOCK_FILE,
            format!("could not parse state lock: {err}"),
        )
    })?;
    if lock.version != 1 {
        return Err(Diagnostic::error(
            "unsupported_state_lock_version",
            CLUSTER_LOCK_FILE,
            format!("unsupported cluster state lock version {}", lock.version),
        ));
    }
    Ok(lock)
}

fn state_lock_held_message(observations: &StateObservations) -> String {
    match observations.lock_id.as_deref() {
        Some(lock_id) => format!(
            "cluster state lock already exists (lock id {lock_id}); run `omnigraph cluster force-unlock {lock_id}` only after confirming no cluster operation is active"
        ),
        None => "cluster state lock already exists; remove it only after confirming no cluster operation is active".to_string(),
    }
}

fn state_resource_digests(state: &ClusterState) -> BTreeMap<String, String> {
    state
        .applied_revision
        .resources
        .iter()
        .map(|(address, resource)| (address.clone(), resource.digest.clone()))
        .collect()
}

fn initial_import_state(desired: &DesiredCluster) -> ClusterState {
    ClusterState {
        version: 1,
        state_revision: 0,
        applied_revision: AppliedRevisionState {
            config_digest: Some(desired.config_digest.clone()),
            resources: BTreeMap::new(),
        },
        resource_statuses: BTreeMap::new(),
        approval_records: BTreeMap::new(),
        recovery_records: BTreeMap::new(),
        observations: BTreeMap::new(),
    }
}

async fn observe_declared_graphs(desired: &DesiredCluster, state: &mut ClusterState) -> usize {
    let mut graph_error_count = 0;
    for graph in &desired.graphs {
        let graph_address = graph_address(&graph.id);
        let schema_address = schema_address(&graph.id);
        let graph_path = desired
            .config_dir
            .join(CLUSTER_GRAPHS_DIR)
            .join(format!("{}.omni", graph.id));
        let graph_uri = display_path(&graph_path);
        let observed_at = now_rfc3339();

        if !graph_path.exists() {
            state.applied_revision.resources.remove(&graph_address);
            state.applied_revision.resources.remove(&schema_address);
            state.observations.insert(
                graph_address.clone(),
                graph_observation_json(GraphObservationJson {
                    address: &graph_address,
                    graph_uri: &graph_uri,
                    observed_at: &observed_at,
                    exists: false,
                    manifest_version: None,
                    schema_digest: None,
                    desired_schema_digest: &graph.schema_digest,
                    schema_matches_desired: Some(false),
                    error: Some("derived graph root is missing"),
                }),
            );
            set_resource_status(
                state,
                &graph_address,
                ResourceLifecycleStatus::Drifted,
                "graph_missing",
                "derived graph root is missing",
            );
            set_resource_status(
                state,
                &schema_address,
                ResourceLifecycleStatus::Drifted,
                "graph_missing",
                "derived graph root is missing",
            );
            continue;
        }

        match observe_live_graph(&graph_uri).await {
            Ok(observation) => {
                let schema_matches = observation.schema_digest == graph.schema_digest;
                state.applied_revision.resources.insert(
                    schema_address.clone(),
                    StateResource {
                        digest: observation.schema_digest.clone(),
                    },
                );
                let query_digests = state_query_digests_for_graph(state, &graph.id);
                let graph_digest_value = graph_digest(
                    &graph.id,
                    Some(&observation.schema_digest),
                    Some(&query_digests),
                );
                state.applied_revision.resources.insert(
                    graph_address.clone(),
                    StateResource {
                        digest: graph_digest_value,
                    },
                );
                state.observations.insert(
                    graph_address.clone(),
                    graph_observation_json(GraphObservationJson {
                        address: &graph_address,
                        graph_uri: &graph_uri,
                        observed_at: &observed_at,
                        exists: true,
                        manifest_version: Some(observation.manifest_version),
                        schema_digest: Some(observation.schema_digest.as_str()),
                        desired_schema_digest: &graph.schema_digest,
                        schema_matches_desired: Some(schema_matches),
                        error: None,
                    }),
                );
                if schema_matches {
                    set_resource_status_applied(state, &graph_address);
                    set_resource_status_applied(state, &schema_address);
                } else {
                    set_resource_status(
                        state,
                        &graph_address,
                        ResourceLifecycleStatus::Drifted,
                        "schema_mismatch",
                        "live schema digest differs from desired schema digest",
                    );
                    set_resource_status(
                        state,
                        &schema_address,
                        ResourceLifecycleStatus::Drifted,
                        "schema_mismatch",
                        "live schema digest differs from desired schema digest",
                    );
                }
            }
            Err(error) => {
                graph_error_count += 1;
                state.observations.insert(
                    graph_address.clone(),
                    graph_observation_json(GraphObservationJson {
                        address: &graph_address,
                        graph_uri: &graph_uri,
                        observed_at: &observed_at,
                        exists: true,
                        manifest_version: None,
                        schema_digest: None,
                        desired_schema_digest: &graph.schema_digest,
                        schema_matches_desired: None,
                        error: Some(error.as_str()),
                    }),
                );
                set_resource_status(
                    state,
                    &graph_address,
                    ResourceLifecycleStatus::Error,
                    "graph_observation_error",
                    error.as_str(),
                );
                set_resource_status(
                    state,
                    &schema_address,
                    ResourceLifecycleStatus::Error,
                    "graph_observation_error",
                    error.as_str(),
                );
            }
        }
    }
    graph_error_count
}

struct LiveGraphObservation {
    manifest_version: u64,
    schema_digest: String,
}

async fn observe_live_graph(graph_uri: &str) -> Result<LiveGraphObservation, String> {
    let db = Omnigraph::open_read_only(graph_uri)
        .await
        .map_err(|err| err.to_string())?;
    let snapshot = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(|err| err.to_string())?;
    let schema_source = db.schema_source();
    Ok(LiveGraphObservation {
        manifest_version: snapshot.version(),
        schema_digest: sha256_hex(schema_source.as_bytes()),
    })
}

struct GraphObservationJson<'a> {
    address: &'a str,
    graph_uri: &'a str,
    observed_at: &'a str,
    exists: bool,
    manifest_version: Option<u64>,
    schema_digest: Option<&'a str>,
    desired_schema_digest: &'a str,
    schema_matches_desired: Option<bool>,
    error: Option<&'a str>,
}

fn graph_observation_json(observation: GraphObservationJson<'_>) -> serde_json::Value {
    json!({
        "kind": "graph",
        "address": observation.address,
        "graph_uri": observation.graph_uri,
        "observed_at": observation.observed_at,
        "exists": observation.exists,
        "manifest_version": observation.manifest_version,
        "schema_digest": observation.schema_digest,
        "desired_schema_digest": observation.desired_schema_digest,
        "schema_matches_desired": observation.schema_matches_desired,
        "error": observation.error,
    })
}

fn state_query_digests_for_graph(state: &ClusterState, graph_id: &str) -> BTreeMap<String, String> {
    let prefix = format!("query.{graph_id}.");
    state
        .applied_revision
        .resources
        .iter()
        .filter_map(|(address, resource)| {
            address
                .strip_prefix(&prefix)
                .map(|name| (name.to_string(), resource.digest.clone()))
        })
        .collect()
}

fn set_resource_status_applied(state: &mut ClusterState, address: &str) {
    state.resource_statuses.insert(
        address.to_string(),
        ResourceStatusRecord {
            status: ResourceLifecycleStatus::Applied,
            conditions: Vec::new(),
            message: None,
        },
    );
}

fn set_resource_status(
    state: &mut ClusterState,
    address: &str,
    status: ResourceLifecycleStatus,
    condition: &str,
    message: &str,
) {
    state.resource_statuses.insert(
        address.to_string(),
        ResourceStatusRecord {
            status,
            conditions: vec![condition.to_string()],
            message: Some(message.to_string()),
        },
    );
}

fn load_desired(config_dir: &Path) -> LoadOutcome {
    let parsed = parse_cluster_config(config_dir);
    let config_dir = parsed.config_dir;
    let config_file = parsed.config_file;
    let mut diagnostics = parsed.diagnostics;
    let Some(raw) = parsed.raw else {
        return LoadOutcome {
            desired: None,
            diagnostics,
            config_dir,
            config_file,
        };
    };
    let settings = validate_cluster_header(&raw, &mut diagnostics);

    let mut resources = BTreeMap::new();
    let mut dependencies = BTreeSet::new();
    let mut graph_query_digests: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut graph_schema_digests: BTreeMap<String, String> = BTreeMap::new();

    for (graph_id, graph) in &raw.graphs {
        validate_id(
            "graph id",
            &format!("graphs.{graph_id}"),
            graph_id,
            &mut diagnostics,
        );
        let graph_address = graph_address(graph_id);
        let schema_address = schema_address(graph_id);
        dependencies.insert(Dependency {
            from: schema_address.clone(),
            to: graph_address.clone(),
        });

        let schema_path = resolve_config_path(&config_dir, &graph.schema);
        let schema_source = match fs::read_to_string(&schema_path) {
            Ok(source) => {
                let digest = sha256_hex(source.as_bytes());
                graph_schema_digests.insert(graph_id.clone(), digest.clone());
                resources.insert(
                    schema_address.clone(),
                    ResourceSummary {
                        address: schema_address.clone(),
                        kind: "schema".to_string(),
                        digest,
                        path: Some(display_path(&schema_path)),
                    },
                );
                Some(source)
            }
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_file_missing",
                    format!("graphs.{graph_id}.schema"),
                    format!(
                        "could not read schema file '{}': {err}",
                        schema_path.display()
                    ),
                ));
                None
            }
        };

        let catalog = schema_source.and_then(|source| match parse_schema(&source) {
            Ok(schema) => match build_catalog(&schema) {
                Ok(catalog) => Some(catalog),
                Err(err) => {
                    diagnostics.push(Diagnostic::error(
                        "schema_catalog_error",
                        format!("graphs.{graph_id}.schema"),
                        err.to_string(),
                    ));
                    None
                }
            },
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_parse_error",
                    format!("graphs.{graph_id}.schema"),
                    err.to_string(),
                ));
                None
            }
        });

        for (query_name, query) in &graph.queries {
            validate_id(
                "query name",
                &format!("graphs.{graph_id}.queries.{query_name}"),
                query_name,
                &mut diagnostics,
            );
            let query_address = query_address(graph_id, query_name);
            dependencies.insert(Dependency {
                from: query_address.clone(),
                to: graph_address.clone(),
            });
            dependencies.insert(Dependency {
                from: query_address.clone(),
                to: schema_address.clone(),
            });

            let query_path = resolve_config_path(&config_dir, &query.file);
            match fs::read_to_string(&query_path) {
                Ok(source) => {
                    let digest = sha256_hex(source.as_bytes());
                    graph_query_digests
                        .entry(graph_id.clone())
                        .or_default()
                        .insert(query_name.clone(), digest.clone());
                    resources.insert(
                        query_address.clone(),
                        ResourceSummary {
                            address: query_address,
                            kind: "query".to_string(),
                            digest,
                            path: Some(display_path(&query_path)),
                        },
                    );
                    validate_query_source(
                        graph_id,
                        query_name,
                        &source,
                        catalog.as_ref(),
                        &mut diagnostics,
                    );
                }
                Err(err) => diagnostics.push(Diagnostic::error(
                    "query_file_missing",
                    format!("graphs.{graph_id}.queries.{query_name}.file"),
                    format!(
                        "could not read query file '{}': {err}",
                        query_path.display()
                    ),
                )),
            }
        }
    }

    for graph_id in raw.graphs.keys() {
        let digest = graph_digest(
            graph_id,
            graph_schema_digests.get(graph_id),
            graph_query_digests.get(graph_id),
        );
        resources.insert(
            graph_address(graph_id),
            ResourceSummary {
                address: graph_address(graph_id),
                kind: "graph".to_string(),
                digest,
                path: None,
            },
        );
    }

    for (policy_name, policy) in &raw.policies {
        validate_id(
            "policy name",
            &format!("policies.{policy_name}"),
            policy_name,
            &mut diagnostics,
        );
        if policy.applies_to.is_empty() {
            diagnostics.push(Diagnostic::error(
                "policy_missing_applies_to",
                format!("policies.{policy_name}.applies_to"),
                "policy.applies_to must name `cluster` or at least one graph",
            ));
        }

        let policy_address = policy_address(policy_name);
        for (idx, target) in policy.applies_to.iter().enumerate() {
            match normalize_policy_target(target) {
                PolicyTarget::Cluster => {}
                PolicyTarget::Graph(graph_id) => {
                    if raw.graphs.contains_key(&graph_id) {
                        dependencies.insert(Dependency {
                            from: policy_address.clone(),
                            to: graph_address(&graph_id),
                        });
                    } else {
                        diagnostics.push(Diagnostic::error(
                            "dangling_graph_reference",
                            format!("policies.{policy_name}.applies_to[{idx}]"),
                            format!(
                                "policy references graph `{graph_id}`, but no graph with that id is declared"
                            ),
                        ));
                    }
                }
                PolicyTarget::WrongKind(kind) => diagnostics.push(Diagnostic::error(
                    "wrong_kind_reference",
                    format!("policies.{policy_name}.applies_to[{idx}]"),
                    format!("policy applies_to expects graph refs or `cluster`, got `{kind}`"),
                )),
            }
        }

        let policy_path = resolve_config_path(&config_dir, &policy.file);
        match fs::read(&policy_path) {
            Ok(bytes) => {
                resources.insert(
                    policy_address.clone(),
                    ResourceSummary {
                        address: policy_address,
                        kind: "policy".to_string(),
                        digest: sha256_hex(&bytes),
                        path: Some(display_path(&policy_path)),
                    },
                );
            }
            Err(err) => diagnostics.push(Diagnostic::error(
                "policy_file_missing",
                format!("policies.{policy_name}.file"),
                format!(
                    "could not read policy file '{}': {err}",
                    policy_path.display()
                ),
            )),
        }
    }

    let mut resource_digests = BTreeMap::new();
    let mut resource_list = Vec::new();
    for (address, resource) in resources {
        resource_digests.insert(address, resource.digest.clone());
        resource_list.push(resource);
    }
    let dependencies: Vec<_> = dependencies.into_iter().collect();
    let graphs = raw
        .graphs
        .keys()
        .map(|graph_id| DesiredGraph {
            id: graph_id.clone(),
            schema_digest: graph_schema_digests
                .get(graph_id)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    let config_digest = desired_config_digest(&raw, &resource_digests);

    LoadOutcome {
        desired: Some(DesiredCluster {
            config_dir: config_dir.clone(),
            config_digest,
            state_lock: settings.state_lock,
            graphs,
            resource_digests,
            resources: resource_list,
            dependencies,
        }),
        diagnostics,
        config_dir,
        config_file,
    }
}

fn validate_query_source(
    graph_id: &str,
    query_name: &str,
    source: &str,
    catalog: Option<&omnigraph_compiler::catalog::Catalog>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let path = format!("graphs.{graph_id}.queries.{query_name}");
    match parse_query(source) {
        Ok(query_file) => {
            let Some(query_decl) = query_file.queries.iter().find(|q| q.name == query_name) else {
                diagnostics.push(Diagnostic::error(
                    "query_key_mismatch",
                    path,
                    format!("no `query {query_name}` declaration found in the referenced .gq file"),
                ));
                return;
            };
            if let Some(catalog) = catalog {
                if let Err(err) = typecheck_query_decl(catalog, query_decl) {
                    diagnostics.push(Diagnostic::error(
                        "query_typecheck_error",
                        format!("graphs.{graph_id}.queries.{query_name}"),
                        err.to_string(),
                    ));
                }
            } else {
                diagnostics.push(Diagnostic::warning(
                    "query_typecheck_skipped",
                    format!("graphs.{graph_id}.queries.{query_name}"),
                    "query parsed, but type-check was skipped because the graph schema is invalid",
                ));
            }
        }
        Err(err) => diagnostics.push(Diagnostic::error(
            "query_parse_error",
            path,
            err.to_string(),
        )),
    }
}

fn diff_resources(
    prior: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
) -> Vec<PlanChange> {
    let mut changes = Vec::new();
    for (address, after) in desired {
        match prior.get(address) {
            None => changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Create,
                before_digest: None,
                after_digest: Some(after.clone()),
            }),
            Some(before) if before != after => changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Update,
                before_digest: Some(before.clone()),
                after_digest: Some(after.clone()),
            }),
            Some(_) => {}
        }
    }
    for (address, before) in prior {
        if !desired.contains_key(address) {
            changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Delete,
                before_digest: Some(before.clone()),
                after_digest: None,
            });
        }
    }
    changes.sort_by(|a, b| a.resource.cmp(&b.resource));
    changes
}

fn compute_blast_radius(changes: &[PlanChange], dependencies: &[Dependency]) -> Vec<BlastRadius> {
    changes
        .iter()
        .filter_map(|change| {
            let affected: Vec<_> = dependencies
                .iter()
                .filter_map(|dep| (dep.to == change.resource).then_some(dep.from.clone()))
                .collect();
            (!affected.is_empty()).then(|| BlastRadius {
                resource: change.resource.clone(),
                affected,
            })
        })
        .collect()
}

fn compute_approvals(changes: &[PlanChange]) -> Vec<ApprovalRequirement> {
    changes
        .iter()
        .filter_map(|change| {
            if change.operation == PlanOperation::Delete
                && (change.resource.starts_with("graph.") || change.resource.starts_with("schema."))
            {
                Some(ApprovalRequirement {
                    resource: change.resource.clone(),
                    reason: "delete may remove deployed graph or schema definition".to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn duplicate_key_diagnostics(text: &str) -> Vec<Diagnostic> {
    #[derive(Debug)]
    struct Frame {
        indent: isize,
        path: String,
        keys: BTreeSet<String>,
    }

    let mut diagnostics = Vec::new();
    let mut stack = vec![Frame {
        indent: -1,
        path: String::new(),
        keys: BTreeSet::new(),
    }];

    for (line_idx, line) in text.lines().enumerate() {
        let line_without_comment = strip_comment(line);
        if line_without_comment.trim().is_empty() {
            continue;
        }
        let indent = line_without_comment
            .chars()
            .take_while(|ch| *ch == ' ')
            .count() as isize;
        let trimmed = line_without_comment.trim_start();
        if trimmed.starts_with('-') {
            continue;
        }
        let Some((raw_key, raw_value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = raw_key.trim();
        if key.is_empty() || key.starts_with('{') || key.starts_with('[') {
            continue;
        }

        while stack.last().is_some_and(|frame| indent <= frame.indent) {
            stack.pop();
        }
        let parent = stack.last_mut().expect("root frame is always present");
        let full_path = if parent.path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", parent.path, key)
        };
        if !parent.keys.insert(key.to_string()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_yaml_key",
                full_path.clone(),
                format!("duplicate YAML key `{key}` on line {}", line_idx + 1),
            ));
        }
        if raw_value.trim().is_empty() {
            stack.push(Frame {
                indent,
                path: full_path,
                keys: BTreeSet::new(),
            });
        }
    }

    diagnostics
}

fn future_field_diagnostics(text: &str) -> Vec<Diagnostic> {
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(text) else {
        return Vec::new();
    };
    let Some(mapping) = value.as_mapping() else {
        return Vec::new();
    };
    let future_fields = [
        "apply",
        "env_file",
        "providers",
        "pipelines",
        "embeddings",
        "ui",
        "aliases",
        "bindings",
    ];
    mapping
        .keys()
        .filter_map(|key| key.as_str())
        .filter(|key| future_fields.contains(key))
        .map(|key| {
            Diagnostic::error(
                "future_phase_field",
                key,
                format!("`{key}` is reserved for a later cluster-control phase"),
            )
        })
        .collect()
}

fn strip_comment(line: &str) -> String {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escaped = false;

    for (idx, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_double_quote => escaped = true,
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '#' if !in_single_quote && !in_double_quote => return line[..idx].to_string(),
            _ => {}
        }
    }

    line.to_string()
}

fn validate_id(kind: &str, path: &str, value: &str, diagnostics: &mut Vec<Diagnostic>) {
    let mut chars = value.chars();
    let valid = chars
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if !valid {
        diagnostics.push(Diagnostic::error(
            "invalid_resource_id",
            path,
            format!("{kind} `{value}` must start with a letter or `_` and contain only ASCII letters, digits, `_`, or `-`"),
        ));
    }
}

enum PolicyTarget {
    Cluster,
    Graph(String),
    WrongKind(String),
}

fn normalize_policy_target(value: &str) -> PolicyTarget {
    if value == "cluster" {
        PolicyTarget::Cluster
    } else if let Some(graph_id) = value.strip_prefix("graph.") {
        PolicyTarget::Graph(graph_id.to_string())
    } else if value.contains('.') {
        PolicyTarget::WrongKind(value.to_string())
    } else {
        PolicyTarget::Graph(value.to_string())
    }
}

fn graph_address(graph_id: &str) -> String {
    format!("graph.{graph_id}")
}

fn schema_address(graph_id: &str) -> String {
    format!("schema.{graph_id}")
}

fn query_address(graph_id: &str, query_name: &str) -> String {
    format!("query.{graph_id}.{query_name}")
}

fn policy_address(policy_name: &str) -> String {
    format!("policy.{policy_name}")
}

fn resolve_config_path(config_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    }
}

fn graph_digest(
    graph_id: &str,
    schema_digest: Option<&String>,
    query_digests: Option<&BTreeMap<String, String>>,
) -> String {
    let mut input = format!(
        "graph\0{graph_id}\0schema\0{}\0",
        schema_digest.map_or("", String::as_str)
    );
    if let Some(query_digests) = query_digests {
        for (name, digest) in query_digests {
            input.push_str("query\0");
            input.push_str(name);
            input.push('\0');
            input.push_str(digest);
            input.push('\0');
        }
    }
    sha256_hex(input.as_bytes())
}

fn desired_config_digest(
    raw: &RawClusterConfig,
    resource_digests: &BTreeMap<String, String>,
) -> String {
    let mut input = String::from("cluster-config\0");
    // Hash parsed semantics, not raw YAML bytes, so comments and formatting do
    // not create a new desired revision and the digest cannot drift from parse.
    let config_semantics =
        serde_json::to_string(raw).expect("raw cluster config must serialize deterministically");
    input.push_str(&config_semantics);
    input.push('\0');
    for (address, digest) in resource_digests {
        input.push_str(address);
        input.push('\0');
        input.push_str(digest);
        input.push('\0');
    }
    sha256_hex(input.as_bytes())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn lock_age_seconds(created_at: &str) -> Option<u64> {
    let created_at = OffsetDateTime::parse(created_at, &Rfc3339).ok()?;
    Some(
        (OffsetDateTime::now_utc() - created_at)
            .whole_seconds()
            .max(0) as u64,
    )
}

fn state_sync_operation_label(operation: StateSyncOperation) -> &'static str {
    match operation {
        StateSyncOperation::Refresh => "refresh",
        StateSyncOperation::Import => "import",
    }
}

fn has_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use omnigraph::db::Omnigraph;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    const SCHEMA: &str = r#"
node Person {
  name: String @key
  age: I32?
}
"#;

    const QUERY: &str = r#"
query find_person($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name, $p.age }
}
"#;

    fn fixture() -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
        fs::write(dir.path().join("people.gq"), QUERY).unwrap();
        fs::write(dir.path().join("base.policy.yaml"), "rules: []\n").unwrap();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
metadata:
  name: test
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge]
"#,
        )
        .unwrap();
        dir
    }

    async fn init_derived_graph(root: &Path) {
        let graph_dir = root.join(CLUSTER_GRAPHS_DIR);
        fs::create_dir_all(&graph_dir).unwrap();
        let graph = graph_dir.join("knowledge.omni");
        Omnigraph::init(graph.to_string_lossy().as_ref(), SCHEMA)
            .await
            .unwrap();
    }

    fn write_lock_file(config_dir: &Path, lock_id: &str, operation: &str) {
        let state_dir = config_dir.join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("lock.json"),
            json!({
                "version": 1,
                "lock_id": lock_id,
                "operation": operation,
                "created_at": "1970-01-01T00:00:00Z",
                "pid": 123
            })
            .to_string(),
        )
        .unwrap();
    }

    #[test]
    fn valid_minimal_config() {
        let dir = fixture();
        let out = validate_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.resource_digests.contains_key("graph.knowledge"));
        assert!(out.resource_digests.contains_key("schema.knowledge"));
        assert!(
            out.dependencies
                .iter()
                .any(|dep| dep.from == "policy.base" && dep.to == "graph.knowledge")
        );
    }

    #[test]
    fn unknown_field_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\nwat: true\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(out.diagnostics[0].message.contains("unknown field"));
    }

    #[test]
    fn future_phase_field_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\npipelines: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "future_phase_field");
    }

    #[test]
    fn duplicate_yaml_key_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\ngraphs: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "duplicate_yaml_key");
    }

    #[test]
    fn duplicate_yaml_key_rejection_keeps_quoted_hashes() {
        let diagnostics =
            duplicate_key_diagnostics("\"name#display\": one\n\"name#display\": two\n");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, "duplicate_yaml_key");
    }

    #[test]
    fn missing_schema_query_and_policy_files() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./missing.pg
    queries:
      find_person: { file: ./missing.gq }
policies:
  base:
    file: ./missing.policy.yaml
    applies_to: [knowledge]
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains("schema_file_missing"));
        assert!(codes.contains("query_file_missing"));
        assert!(codes.contains("policy_file_missing"));
    }

    #[test]
    fn wrong_kind_and_dangling_refs_fail() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [query.knowledge.find_person, missing]
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains("wrong_kind_reference"));
        assert!(codes.contains("dangling_graph_reference"));
    }

    #[test]
    fn query_key_mismatch_fails() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      different: { file: ./people.gq }
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "query_key_mismatch");
    }

    #[test]
    fn query_typecheck_failure_fails() {
        let dir = fixture();
        fs::write(
            dir.path().join("people.gq"),
            "query find_person() { match { $d: DoesNotExist } return { $d.name } }\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "query_typecheck_error")
        );
    }

    #[test]
    fn missing_state_plans_creates() {
        let dir = fixture();
        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.state_found);
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_acquired);
        assert!(
            out.changes
                .iter()
                .all(|c| c.operation == PlanOperation::Create)
        );
        assert!(out.changes.iter().any(|c| c.resource == "graph.knowledge"));
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn config_digest_ignores_yaml_comments_and_formatting() {
        let dir = fixture();
        let first = plan_config_dir(dir.path());
        assert!(first.ok, "{:?}", first.diagnostics);

        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
# Same semantic config as the fixture, intentionally rendered differently.
version: 1
metadata: { name: test }
state: { backend: cluster, lock: true }
graphs:
  knowledge:
    schema: ./people.pg
    queries: { find_person: { file: ./people.gq } }
policies:
  base:
    file: ./base.policy.yaml
    applies_to:
      - knowledge
"#,
        )
        .unwrap();

        let second = plan_config_dir(dir.path());
        assert!(second.ok, "{:?}", second.diagnostics);
        assert_eq!(
            first.desired_revision.config_digest,
            second.desired_revision.config_digest
        );
    }

    #[test]
    fn existing_state_plans_update_and_delete_deterministically() {
        let dir = fixture();
        let first = plan_config_dir(dir.path());
        let state_dir = dir.path().join("__cluster");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            serde_json::to_string_pretty(&json!({
                "version": 1,
                "applied_revision": {
                    "config_digest": "old",
                    "resources": {
                        "graph.knowledge": { "digest": first.resource_digests["graph.knowledge"] },
                        "policy.old": { "digest": "abc" },
                        "schema.knowledge": { "digest": "old-schema" }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        let rendered: Vec<_> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), &change.operation))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("policy.base", &PlanOperation::Create),
                ("policy.old", &PlanOperation::Delete),
                ("query.knowledge.find_person", &PlanOperation::Create),
                ("schema.knowledge", &PlanOperation::Update),
            ]
        );
    }

    #[test]
    fn old_minimal_state_json_still_plans_with_default_revision() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{
  "version": 1,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" }
    }
  }
}"#,
        )
        .unwrap();

        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 0);
        assert!(out.state_observations.state_cas.is_some());
        assert!(out.changes.iter().any(|change| {
            change.resource == "graph.knowledge" && change.operation == PlanOperation::Update
        }));
    }

    #[test]
    fn extended_state_json_status_surfaces_statuses() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        let state = r#"{
  "version": 1,
  "state_revision": 42,
  "applied_revision": {
    "config_digest": "applied-config",
    "resources": {
      "graph.knowledge": { "digest": "graph-digest" }
    }
  },
  "resource_statuses": {
    "graph.knowledge": {
      "status": "applied",
      "conditions": ["healthy"],
      "message": "ready"
    }
  },
  "approval_records": {},
  "recovery_records": {},
  "observations": {
    "graph.knowledge": { "manifest_version": 12 }
  }
}"#;
        fs::write(state_dir.join("state.json"), state).unwrap();

        let out = status_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.state_observations.state_found);
        assert_eq!(out.state_observations.state_revision, 42);
        assert_eq!(
            out.state_observations.state_cas.as_deref(),
            Some(format!("sha256:{}", sha256_hex(state.as_bytes())).as_str())
        );
        assert_eq!(
            out.resource_digests
                .get("graph.knowledge")
                .map(String::as_str),
            Some("graph-digest")
        );
        assert_eq!(
            out.resource_statuses["graph.knowledge"].status,
            ResourceLifecycleStatus::Applied
        );
    }

    #[test]
    fn missing_state_status_succeeds_with_warning() {
        let dir = fixture();
        let out = status_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.state_found);
        assert_eq!(out.state_observations.state_revision, 0);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_missing")
        );
    }

    #[test]
    fn invalid_state_status_fails() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join("state.json"), "{").unwrap();

        let out = status_config_dir(dir.path());
        assert!(!out.ok);
        assert!(out.state_observations.state_found);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_state_json")
        );
    }

    #[test]
    fn status_surfaces_full_lock_metadata() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "refresh");

        let out = status_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.state_observations.locked);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("refresh")
        );
        assert_eq!(
            out.state_observations.lock_created_at.as_deref(),
            Some("1970-01-01T00:00:00Z")
        );
        assert_eq!(out.state_observations.lock_pid, Some(123));
        assert!(out.state_observations.lock_age_seconds.is_some());
    }

    #[test]
    fn force_unlock_matching_id_removes_lock() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");

        let out = force_unlock_config_dir(dir.path(), "held-lock");
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.lock_removed);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("plan")
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn force_unlock_wrong_id_fails_and_preserves_lock() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");

        let out = force_unlock_config_dir(dir.path(), "other-lock");
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_id_mismatch")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn force_unlock_missing_lock_fails() {
        let dir = fixture();

        let out = force_unlock_config_dir(dir.path(), "held-lock");
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(!out.state_observations.locked);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_missing")
        );
    }

    #[test]
    fn force_unlock_invalid_lock_json_fails_and_preserves_lock() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(state_dir.join("lock.json"), "{").unwrap();

        let out = force_unlock_config_dir(dir.path(), "held-lock");
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "invalid_state_lock")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn force_unlock_unsupported_lock_version_fails_and_preserves_lock() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("lock.json"),
            r#"{"version":2,"lock_id":"held-lock","operation":"plan","created_at":"1970-01-01T00:00:00Z","pid":123}"#,
        )
        .unwrap();

        let out = force_unlock_config_dir(dir.path(), "held-lock");
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_lock_version")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn force_unlock_external_state_backend_rejected() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: s3://state-bucket/cluster
graphs:
  knowledge:
    schema: ./people.pg
"#,
        )
        .unwrap();

        let out = force_unlock_config_dir(dir.path(), "held-lock");
        assert!(!out.ok);
        assert!(!out.lock_removed);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_backend")
        );
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn plan_succeeds_after_force_unlock() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");

        let locked = plan_config_dir(dir.path());
        assert!(!locked.ok);
        assert!(
            locked
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );

        let unlocked = force_unlock_config_dir(dir.path(), "held-lock");
        assert!(unlocked.ok, "{:?}", unlocked.diagnostics);

        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
    }

    #[test]
    fn plan_reports_state_cas_revision_and_removes_lock() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        let state = r#"{
  "version": 1,
  "state_revision": 7,
  "applied_revision": {
    "config_digest": "old",
    "resources": {
      "graph.knowledge": { "digest": "old-graph" }
    }
  }
}"#;
        fs::write(state_dir.join("state.json"), state).unwrap();

        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 7);
        assert_eq!(
            out.state_observations.state_cas.as_deref(),
            Some(format!("sha256:{}", sha256_hex(state.as_bytes())).as_str())
        );
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_id.is_none());
        assert!(out.state_observations.lock_acquired);
        assert!(out.state_observations.acquired_lock_id.is_some());
        assert!(
            !dir.path().join(CLUSTER_LOCK_FILE).exists(),
            "plan must release lock before returning"
        );
    }

    #[test]
    fn existing_lock_makes_plan_fail() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("lock.json"),
            r#"{
  "version": 1,
  "lock_id": "held-lock",
  "operation": "plan",
  "created_at": "2026-06-08T00:00:00Z",
  "pid": 123
}"#,
        )
        .unwrap();

        let out = plan_config_dir(dir.path());
        assert!(!out.ok);
        assert!(out.state_observations.locked);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert!(!out.state_observations.lock_acquired);
        assert!(out.state_observations.acquired_lock_id.is_none());
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("plan")
        );
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "state_lock_held"
                && diagnostic.message.contains("force-unlock held-lock")
        }));
    }

    #[test]
    fn state_lock_false_bypasses_lock_with_warning() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: cluster
  lock: false
graphs:
  knowledge:
    schema: ./people.pg
"#,
        )
        .unwrap();

        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.locked);
        assert!(!out.state_observations.lock_acquired);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_disabled")
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[test]
    fn external_state_backend_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "unsupported_state_backend");
    }

    #[test]
    fn external_state_backend_plan_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();
        let out = plan_config_dir(dir.path());
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_backend")
        );
    }

    #[tokio::test]
    async fn import_missing_state_creates_state_with_graph_observation() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;

        let out = import_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 1);
        assert!(out.state_observations.state_cas.is_some());
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_acquired);
        assert!(out.state_observations.acquired_lock_id.is_some());
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
        assert_eq!(
            out.resource_digests
                .get("schema.knowledge")
                .map(String::as_str),
            Some(sha256_hex(SCHEMA.as_bytes()).as_str())
        );
        assert!(out.observations["graph.knowledge"]["manifest_version"].is_number());
        assert_eq!(
            out.observations["graph.knowledge"]["schema_matches_desired"],
            true
        );

        let state: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap())
                .unwrap();
        assert_eq!(state["state_revision"], 1);
        assert_eq!(
            state["resource_statuses"]["graph.knowledge"]["status"],
            "applied"
        );
    }

    #[tokio::test]
    async fn import_existing_state_fails() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();

        let out = import_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_already_exists")
        );
    }

    #[tokio::test]
    async fn refresh_missing_state_fails() {
        let dir = fixture();
        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_missing")
        );
    }

    #[tokio::test]
    async fn refresh_existing_minimal_state_increments_revision_and_updates_cas() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"config_digest":"old","resources":{"graph.knowledge":{"digest":"old"}}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 1);
        assert!(out.state_observations.state_cas.is_some());
        assert!(!out.state_observations.locked);
        assert!(out.state_observations.lock_acquired);
        assert_eq!(
            out.resource_statuses["graph.knowledge"].status,
            ResourceLifecycleStatus::Applied
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn refresh_records_live_schema_digest_and_manifest_version() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"state_revision":4,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.state_observations.state_revision, 5);
        assert_eq!(
            out.observations["graph.knowledge"]["schema_digest"],
            sha256_hex(SCHEMA.as_bytes())
        );
        assert!(out.observations["graph.knowledge"]["manifest_version"].is_u64());
    }

    #[tokio::test]
    async fn missing_derived_graph_root_marks_drifted_and_plans_creates() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{"graph.knowledge":{"digest":"old-graph"},"schema.knowledge":{"digest":"old-schema"}}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(
            out.resource_statuses["graph.knowledge"].status,
            ResourceLifecycleStatus::Drifted
        );
        assert!(!out.resource_digests.contains_key("graph.knowledge"));
        assert_eq!(out.observations["graph.knowledge"]["exists"], false);

        let plan = plan_config_dir(dir.path());
        assert!(plan.ok, "{:?}", plan.diagnostics);
        assert!(plan.changes.iter().any(|change| {
            change.resource == "graph.knowledge" && change.operation == PlanOperation::Create
        }));
        assert!(plan.changes.iter().any(|change| {
            change.resource == "schema.knowledge" && change.operation == PlanOperation::Create
        }));
    }

    #[tokio::test]
    async fn live_schema_mismatch_marks_drifted_and_causes_plan_update() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        fs::write(
            dir.path().join("people.pg"),
            SCHEMA.replace("age: I32?", "age: I32?\n  nickname: String?"),
        )
        .unwrap();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{"graph.knowledge":{"digest":"old-graph"},"schema.knowledge":{"digest":"old-schema"}}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(
            out.resource_statuses["schema.knowledge"].status,
            ResourceLifecycleStatus::Drifted
        );
        assert_eq!(
            out.observations["graph.knowledge"]["schema_matches_desired"],
            false
        );

        let plan = plan_config_dir(dir.path());
        assert!(plan.ok, "{:?}", plan.diagnostics);
        assert!(plan.changes.iter().any(|change| {
            change.resource == "schema.knowledge" && change.operation == PlanOperation::Update
        }));
    }

    #[tokio::test]
    async fn existing_lock_makes_refresh_fail() {
        let dir = fixture();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();
        fs::write(
            state_dir.join("lock.json"),
            r#"{"version":1,"lock_id":"held-lock","operation":"refresh","created_at":"2026-06-08T00:00:00Z","pid":123}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(out.state_observations.locked);
        assert_eq!(out.state_observations.lock_id.as_deref(), Some("held-lock"));
        assert!(!out.state_observations.lock_acquired);
        assert_eq!(
            out.state_observations.lock_operation.as_deref(),
            Some("refresh")
        );
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "state_lock_held"
                && diagnostic.message.contains("force-unlock held-lock")
        }));
    }

    #[tokio::test]
    async fn state_lock_false_bypasses_refresh_lock_with_warning() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
state:
  backend: cluster
  lock: false
graphs:
  knowledge:
    schema: ./people.pg
"#,
        )
        .unwrap();
        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            r#"{"version":1,"applied_revision":{"resources":{}}}"#,
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.locked);
        assert!(!out.state_observations.lock_acquired);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_disabled")
        );
    }

    #[tokio::test]
    async fn external_state_backend_refresh_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "unsupported_state_backend")
        );
    }

    #[tokio::test]
    async fn import_graph_open_error_does_not_create_state() {
        let dir = fixture();
        fs::create_dir_all(dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni")).unwrap();

        let out = import_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_observation_error")
        );
        assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
    }
}
