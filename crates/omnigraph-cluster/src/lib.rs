use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;

use omnigraph::db::{Omnigraph, ReadTarget, SchemaApplyOptions};
use omnigraph_compiler::SchemaMigrationPlan;
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

pub mod failpoints;

pub const CLUSTER_CONFIG_FILE: &str = "cluster.yaml";
pub const CLUSTER_GRAPHS_DIR: &str = "graphs";
pub const CLUSTER_STATE_DIR: &str = "__cluster";
pub const CLUSTER_STATE_FILE: &str = "__cluster/state.json";
pub const CLUSTER_LOCK_FILE: &str = "__cluster/lock.json";
pub const CLUSTER_RESOURCES_DIR: &str = "__cluster/resources";
pub const CLUSTER_RECOVERIES_DIR: &str = "__cluster/recoveries";
pub const CLUSTER_APPROVALS_DIR: &str = "__cluster/approvals";

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
struct ApprovalArtifact {
    schema_version: u32,
    approval_id: String,
    resource: String,
    operation: String,
    reason: String,
    bound_config_digest: String,
    #[serde(default)]
    bound_before_digest: Option<String>,
    #[serde(default)]
    bound_after_digest: Option<String>,
    approved_by: String,
    created_at: String,
    #[serde(default)]
    consumed_at: Option<String>,
    #[serde(default)]
    consumed_by_operation: Option<String>,
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

/// Recovery-intent record for a graph-moving apply operation (RFC-004 §D2).
/// Written under the state lock before the engine call that can create or
/// move a graph manifest; deleted only after the cluster state CAS that
/// records the outcome lands. The sweep (§D3) classifies survivors.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoverySidecar {
    schema_version: u32,
    operation_id: String,
    started_at: String,
    #[serde(default)]
    actor: Option<String>,
    kind: RecoverySidecarKind,
    graph_id: String,
    graph_uri: String,
    #[serde(default)]
    observed_manifest_version: Option<u64>,
    #[serde(default)]
    expected_manifest_version: Option<u64>,
    desired_schema_digest: String,
    #[serde(default)]
    state_cas_base: Option<String>,
    /// For graph_delete: the approval this operation consumes; lets a sweep
    /// roll-forward consume it too.
    #[serde(default)]
    approval_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RecoverySidecarKind {
    GraphCreate,
    SchemaApply,
    GraphDelete,
}

#[derive(Debug, Default)]
struct SweepOutcome {
    /// Graphs whose sidecar was kept (rows 5/6): graph-moving work for them
    /// is blocked until the operator repairs and re-observes.
    pending_graphs: BTreeSet<String>,
    /// Sidecars whose outcome is recorded (rows 2/4): deleted only after the
    /// command's state write lands, so a CAS failure re-sweeps them.
    completed_sidecars: Vec<PathBuf>,
    /// Approval artifacts consumed by a roll-forward (delete row 7b): their
    /// files are rewritten with consumed_at only after the state write lands.
    consumed_approvals: Vec<String>,
}

#[derive(Debug)]
struct LocalStateBackend {
    state_dir: PathBuf,
    state_path: PathBuf,
    lock_path: PathBuf,
    recoveries_dir: PathBuf,
    approvals_dir: PathBuf,
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

pub async fn plan_config_dir(config_dir: impl AsRef<Path>) -> PlanOutput {
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

    // Plan is read-only: pending sidecars are reported, never acted on
    // (RFC-004 open question 3 keeps read-only commands warn-only).
    warn_pending_recovery_sidecars(&desired.config_dir, &mut diagnostics);

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

    let mut changes = if has_errors(&diagnostics) {
        Vec::new()
    } else {
        diff_resources(&prior_resources, &desired.resource_digests)
    };
    // Plan previews dispositions without sweeping; a pending recovery is
    // surfaced as the cluster_recovery_pending warning above instead.
    let artifacts = backend.list_approval_artifacts(&mut diagnostics);
    let approved = approved_resources(
        &artifacts,
        &changes,
        &desired.config_digest,
        &mut diagnostics,
    );
    classify_changes(&mut changes, &desired.dependencies, &BTreeSet::new(), &approved);

    // Embed real migration steps for schema updates so plan is a data-aware
    // preview; failures degrade to the digest diff with a warning.
    for change in &mut changes {
        if change.operation != PlanOperation::Update {
            continue;
        }
        let ResourceKind::Schema(graph_id) = resource_kind(&change.resource) else {
            continue;
        };
        let graph_uri = display_path(
            &desired
                .config_dir
                .join(CLUSTER_GRAPHS_DIR)
                .join(format!("{graph_id}.omni")),
        );
        let source_path = desired
            .resources
            .iter()
            .find(|resource| resource.address == change.resource)
            .and_then(|resource| resource.path.clone());
        let preview = match source_path {
            Some(path) => preview_schema_migration(&graph_uri, &path).await,
            None => Err("no schema source recorded".to_string()),
        };
        match preview {
            Ok(migration) => change.migration = Some(migration),
            Err(err) => diagnostics.push(Diagnostic::warning(
                "schema_preview_unavailable",
                change.resource.clone(),
                format!("could not preview the schema migration: {err}"),
            )),
        }
    }
    let blast_radius = compute_blast_radius(&changes, &desired.dependencies);
    let approvals_required = compute_approvals(&changes, &approved);
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

/// Config-only `cluster apply` (Stage 3A): execute the query/policy subset of
/// the plan against the local cluster catalog. The plan is recomputed under
/// the state lock, so freshness is structural; the state CAS inside
/// `write_state` is the second fence. Graph/schema changes are never executed
/// here — they are deferred to the graph-lifecycle phase and reported loudly.
///
/// Payloads are content-addressed and written BEFORE the state CAS because
/// state is the publish point: a failure after payload writes leaves inert
/// digest-named blobs and no success acknowledgement; re-running apply is the
/// repair.
/// Options for `cluster apply`. `actor` attributes graph-moving operations
/// (recorded in sidecars and audit entries, threaded to the engine's
/// `apply_schema_as` so Cedar enforcement fires wherever a policy checker is
/// installed).
#[derive(Debug, Clone, Default)]
pub struct ApplyOptions {
    pub actor: Option<String>,
}

pub async fn apply_config_dir(config_dir: impl AsRef<Path>) -> ApplyOutput {
    apply_config_dir_with_options(config_dir, ApplyOptions::default()).await
}

pub async fn apply_config_dir_with_options(
    config_dir: impl AsRef<Path>,
    options: ApplyOptions,
) -> ApplyOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let backend = LocalStateBackend::new(&outcome.config_dir);
    let mut observations = backend.observations();

    let actor_for_output = options.actor.clone();
    let early_return = |config_dir: String,
                        config_digest: Option<String>,
                        observations: StateObservations,
                        changes: Vec<PlanChange>,
                        resource_statuses: BTreeMap<String, ResourceStatusRecord>,
                        diagnostics: Vec<Diagnostic>| {
        ApplyOutput {
            ok: !has_errors(&diagnostics),
            config_dir,
            actor: actor_for_output.clone(),
            desired_revision: DesiredRevision {
                config_digest,
            },
            state_observations: observations,
            changes,
            applied_count: 0,
            deferred_count: 0,
            converged: false,
            state_written: false,
            resource_statuses,
            diagnostics,
        }
    };

    let Some(desired) = outcome.desired else {
        return early_return(
            display_path(&outcome.config_dir),
            None,
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    };

    if has_errors(&diagnostics) {
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    }

    // Named guard: the lock must be held until the state outcome is recorded.
    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock("apply", &mut observations) {
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
            "state.lock is false; apply wrote state without acquiring the cluster state lock",
        ));
        None
    };

    if has_errors(&diagnostics) {
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    }

    let snapshot = match backend.read_state(&mut observations) {
        Ok(snapshot) => snapshot,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                Vec::new(),
                BTreeMap::new(),
                diagnostics,
            );
        }
    };
    let expected_cas = snapshot.state_cas;
    let Some(mut state) = snapshot.state else {
        diagnostics.push(Diagnostic::error(
            "state_missing",
            CLUSTER_STATE_FILE,
            "apply requires an existing state.json; run `cluster import` to bootstrap state",
        ));
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            Vec::new(),
            BTreeMap::new(),
            diagnostics,
        );
    };

    // Snapshot the as-read state BEFORE the sweep so sweep mutations count as
    // changes for the final dirty check and get persisted by the state CAS.
    let before_value =
        serde_json::to_value(&state).expect("cluster state must serialize deterministically");
    let sweep = sweep_recovery_sidecars(&backend, &mut state, &mut diagnostics).await;

    let prior_resources = state_resource_digests(&state);
    let mut changes = diff_resources(&prior_resources, &desired.resource_digests);
    let approval_artifacts = backend.list_approval_artifacts(&mut diagnostics);
    let approved = approved_resources(
        &approval_artifacts,
        &changes,
        &desired.config_digest,
        &mut diagnostics,
    );
    classify_changes(
        &mut changes,
        &desired.dependencies,
        &sweep.pending_graphs,
        &approved,
    );

    // Defensive invariant: nothing the approval gate covers may be executable
    // WITHOUT a matching approval. Gated changes with a valid artifact are the
    // sanctioned exception (stage 4C).
    let approvals = compute_approvals(&changes, &approved);
    let approval_violation = changes.iter().any(|change| {
        change.disposition == Some(ApplyDisposition::Applied)
            && approvals
                .iter()
                .any(|approval| approval.resource == change.resource && !approval.satisfied)
    });
    if approval_violation {
        diagnostics.push(Diagnostic::error(
            "apply_approval_invariant_violation",
            "changes",
            "an executable change requires approval; refusing to apply",
        ));
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            changes,
            state.resource_statuses,
            diagnostics,
        );
    }

    // Graph creates execute first (RFC-004 §D5), sequentially, sidecar-fenced:
    // sidecar written before the init, rewritten with the post-init manifest
    // version, deleted only after the final state CAS lands. A failure stops
    // further graph-moving work and demotes that graph's dependents.
    let source_paths: BTreeMap<&str, &str> = desired
        .resources
        .iter()
        .filter_map(|resource| {
            resource
                .path
                .as_deref()
                .map(|path| (resource.address.as_str(), path))
        })
        .collect();
    let graph_creates_to_run: Vec<String> = changes
        .iter()
        .filter(|change| {
            change.disposition == Some(ApplyDisposition::Applied)
                && change.operation == PlanOperation::Create
                && matches!(resource_kind(&change.resource), ResourceKind::Graph(_))
        })
        .filter_map(|change| change.resource.strip_prefix("graph.").map(str::to_string))
        .collect();
    let mut completed_op_sidecars: Vec<PathBuf> = Vec::new();
    let mut failed_graphs: BTreeMap<String, FailedGraphOrigin> = BTreeMap::new();
    let mut graph_moving_aborted = false;
    for graph_id in &graph_creates_to_run {
        if graph_moving_aborted {
            // A prior create failed: stop graph-moving work (loud partials).
            diagnostics.push(Diagnostic::warning(
                "graph_create_skipped",
                graph_address(graph_id),
                "skipped after an earlier graph create failed in this run",
            ));
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
            continue;
        }
        let Some(desired_graph) = desired.graphs.iter().find(|graph| &graph.id == graph_id)
        else {
            continue;
        };
        let graph_uri = display_path(
            &desired
                .config_dir
                .join(CLUSTER_GRAPHS_DIR)
                .join(format!("{graph_id}.omni")),
        );
        let mut sidecar = RecoverySidecar {
            schema_version: 1,
            operation_id: Ulid::new().to_string(),
            started_at: now_rfc3339(),
            actor: options.actor.clone(),
            kind: RecoverySidecarKind::GraphCreate,
            graph_id: graph_id.clone(),
            graph_uri: graph_uri.clone(),
            observed_manifest_version: None,
            expected_manifest_version: None,
            desired_schema_digest: desired_graph.schema_digest.clone(),
            state_cas_base: expected_cas.clone(),
            approval_id: None,
        };
        let sidecar_path = match backend.write_recovery_sidecar(&sidecar) {
            Ok(path) => path,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
                graph_moving_aborted = true;
                continue;
            }
        };
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.before_graph_create") {
            // Simulated crash before the init: the sidecar stays for the
            // sweep (row 1: root absent -> intent removed next run).
            diagnostics.push(diagnostic);
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
            graph_moving_aborted = true;
            continue;
        }
        // Re-read + re-verify the schema source under the lock — the same
        // TOCTOU posture as write_resource_payload.
        let schema_source = source_paths
            .get(schema_address(graph_id).as_str())
            .ok_or_else(|| {
                Diagnostic::error(
                    "graph_create_failed",
                    graph_address(graph_id),
                    "no schema source recorded for graph",
                )
            })
            .and_then(|path| {
                fs::read_to_string(Path::new(path)).map_err(|err| {
                    Diagnostic::error(
                        "graph_create_failed",
                        graph_address(graph_id),
                        format!("could not read schema source '{path}': {err}"),
                    )
                })
            })
            .and_then(|source| {
                if sha256_hex(source.as_bytes()) == desired_graph.schema_digest {
                    Ok(source)
                } else {
                    Err(Diagnostic::error(
                        "resource_content_changed",
                        schema_address(graph_id),
                        "schema source changed while apply was running; re-run `cluster apply`",
                    ))
                }
            });
        let schema_source = match schema_source {
            Ok(source) => source,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                let _ = fs::remove_file(&sidecar_path); // nothing moved
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
                graph_moving_aborted = true;
                continue;
            }
        };
        match Omnigraph::init(&graph_uri, &schema_source).await {
            Ok(_) => {}
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "graph_create_failed",
                    graph_address(graph_id),
                    format!("could not initialize graph at '{graph_uri}': {err}"),
                ));
                // The sidecar stays: the sweep classifies whether the failed
                // init left a partial root (row 5) or nothing (row 1).
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphCreate);
                graph_moving_aborted = true;
                continue;
            }
        }
        // Record the post-init pin in the sidecar (best effort — a failure
        // here leaves expected = null and the sweep classifies by digest).
        if let Ok(db) = Omnigraph::open_read_only(&graph_uri).await {
            if let Ok(snapshot) = db.snapshot_of(ReadTarget::branch("main")).await {
                sidecar.expected_manifest_version = Some(snapshot.version());
                if let Err(diagnostic) = backend.write_recovery_sidecar(&sidecar) {
                    diagnostics.push(diagnostic);
                }
            }
        }
        // Crash point: the graph exists, the cluster state does not record it
        // yet. A failure here must acknowledge nothing; the next run's sweep
        // rolls the ledger forward (row 4).
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_graph_create") {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                changes,
                state.resource_statuses,
                diagnostics,
            );
        }
        completed_op_sidecars.push(sidecar_path);
    }

    // Schema applies execute next (RFC-004 §D5): the first cluster operation
    // that moves an EXISTING graph manifest, sidecar-fenced the same way.
    let schema_updates_to_run: Vec<String> = changes
        .iter()
        .filter(|change| {
            change.disposition == Some(ApplyDisposition::Applied)
                && change.operation == PlanOperation::Update
                && matches!(resource_kind(&change.resource), ResourceKind::Schema(_))
        })
        .filter_map(|change| change.resource.strip_prefix("schema.").map(str::to_string))
        .collect();
    for graph_id in &schema_updates_to_run {
        if graph_moving_aborted {
            diagnostics.push(Diagnostic::warning(
                "schema_apply_skipped",
                schema_address(graph_id),
                "skipped after an earlier graph-moving operation failed in this run",
            ));
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
            continue;
        }
        let Some(desired_graph) = desired.graphs.iter().find(|graph| &graph.id == graph_id)
        else {
            continue;
        };
        let graph_uri = display_path(
            &desired
                .config_dir
                .join(CLUSTER_GRAPHS_DIR)
                .join(format!("{graph_id}.omni")),
        );
        // Read-write open: the engine's own recovery sweep runs here, which
        // is exactly what we want before moving its manifest.
        let db = match Omnigraph::open(&graph_uri).await {
            Ok(db) => db,
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_apply_failed",
                    schema_address(graph_id),
                    format!("could not open graph at '{graph_uri}': {err}"),
                ));
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        };
        let observed_manifest_version = match db.snapshot_of(ReadTarget::branch("main")).await {
            Ok(snapshot) => Some(snapshot.version()),
            Err(_) => None,
        };
        let mut sidecar = RecoverySidecar {
            schema_version: 1,
            operation_id: Ulid::new().to_string(),
            started_at: now_rfc3339(),
            actor: options.actor.clone(),
            kind: RecoverySidecarKind::SchemaApply,
            graph_id: graph_id.clone(),
            graph_uri: graph_uri.clone(),
            observed_manifest_version,
            expected_manifest_version: None,
            desired_schema_digest: desired_graph.schema_digest.clone(),
            state_cas_base: expected_cas.clone(),
            approval_id: None,
        };
        let sidecar_path = match backend.write_recovery_sidecar(&sidecar) {
            Ok(path) => path,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        };
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.before_schema_apply") {
            // Simulated crash before the engine call: the sidecar stays; the
            // sweep retires it next run (ledger still consistent with live).
            diagnostics.push(diagnostic);
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
            graph_moving_aborted = true;
            continue;
        }
        // Re-read + digest-verify the desired schema source under the lock.
        let schema_source = source_paths
            .get(schema_address(graph_id).as_str())
            .ok_or_else(|| {
                Diagnostic::error(
                    "schema_apply_failed",
                    schema_address(graph_id),
                    "no schema source recorded for graph",
                )
            })
            .and_then(|path| {
                fs::read_to_string(Path::new(path)).map_err(|err| {
                    Diagnostic::error(
                        "schema_apply_failed",
                        schema_address(graph_id),
                        format!("could not read schema source '{path}': {err}"),
                    )
                })
            })
            .and_then(|source| {
                if sha256_hex(source.as_bytes()) == desired_graph.schema_digest {
                    Ok(source)
                } else {
                    Err(Diagnostic::error(
                        "resource_content_changed",
                        schema_address(graph_id),
                        "schema source changed while apply was running; re-run `cluster apply`",
                    ))
                }
            });
        let schema_source = match schema_source {
            Ok(source) => source,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                let _ = fs::remove_file(&sidecar_path); // nothing moved
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        };
        // Soft drops only: allow_data_loss stays false until the approval
        // artifacts of stage 4C exist (RFC-004 §D4).
        match db
            .apply_schema_as(
                &schema_source,
                SchemaApplyOptions::default(),
                options.actor.as_deref(),
            )
            .await
        {
            Ok(result) => {
                sidecar.expected_manifest_version = Some(result.manifest_version);
                if let Err(diagnostic) = backend.write_recovery_sidecar(&sidecar) {
                    diagnostics.push(diagnostic);
                }
            }
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_apply_failed",
                    schema_address(graph_id),
                    format!("schema apply failed on '{graph_uri}': {err}"),
                ));
                // Sidecar stays; the sweep retires it (live digest unchanged
                // == ledger consistent) or flags real movement.
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::SchemaApply);
                graph_moving_aborted = true;
                continue;
            }
        }
        // Crash point: the manifest moved, the ledger does not record it yet.
        // A failure here acknowledges nothing; the sweep rolls forward.
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_schema_apply") {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                changes,
                state.resource_statuses,
                diagnostics,
            );
        }
        completed_op_sidecars.push(sidecar_path);
    }

    if !failed_graphs.is_empty() {
        demote_dependents_of_failed_graphs(&mut changes, &failed_graphs, &desired.dependencies);
    }

    for change in &changes {
        match change.disposition {
            Some(ApplyDisposition::Deferred) => diagnostics.push(Diagnostic::warning(
                "apply_unsupported_change",
                change.resource.clone(),
                "graph/schema changes are not applied in this stage; they are deferred to the graph-lifecycle phase",
            )),
            Some(ApplyDisposition::Blocked) => diagnostics.push(Diagnostic::warning(
                "apply_dependency_blocked",
                change.resource.clone(),
                format!(
                    "blocked by an unapplied or missing dependency ({})",
                    change.reason.as_deref().unwrap_or("dependency")
                ),
            )),
            _ => {}
        }
    }

    // Payload phase: content-addressed writes before the state CAS. Any
    // failure aborts before state moves; blobs already written are inert.
    // Gate on payload-phase errors only — sweep errors (e.g. a kept row-5
    // sidecar) must not abort the run, or their statuses would never persist.
    let errors_before_payloads = count_errors(&diagnostics);
    for change in &changes {
        if change.disposition != Some(ApplyDisposition::Applied)
            || change.operation == PlanOperation::Delete
        {
            continue;
        }
        let kind = resource_kind(&change.resource);
        let digest = change
            .after_digest
            .as_deref()
            .expect("create/update always carries an after digest");
        let Some(target) = payload_path(&desired.config_dir, &kind, digest) else {
            continue;
        };
        let Some(source) = source_paths.get(change.resource.as_str()) else {
            diagnostics.push(Diagnostic::error(
                "resource_payload_write_error",
                change.resource.clone(),
                "no source file recorded for resource",
            ));
            continue;
        };
        if let Err(diagnostic) =
            write_resource_payload(&target, Path::new(source), digest, &change.resource)
        {
            diagnostics.push(diagnostic);
        }
    }
    if count_errors(&diagnostics) > errors_before_payloads {
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            changes,
            state.resource_statuses,
            diagnostics,
        );
    }

    // Crash point: payloads are on disk, state has not moved. A failure here
    // must leave state.json byte-identical and acknowledge nothing; re-running
    // apply repairs via the skip-if-exists blob reuse.
    if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_payload_phase") {
        diagnostics.push(diagnostic);
        return early_return(
            display_path(&desired.config_dir),
            Some(desired.config_digest),
            observations,
            changes,
            state.resource_statuses,
            diagnostics,
        );
    }

    // Approved graph deletes execute LAST (RFC-004 §D5): catalog writes for
    // surviving resources land first, then the irreversible work.
    let graph_deletes_to_run: Vec<String> = changes
        .iter()
        .filter(|change| {
            change.disposition == Some(ApplyDisposition::Applied)
                && change.operation == PlanOperation::Delete
                && matches!(resource_kind(&change.resource), ResourceKind::Graph(_))
        })
        .filter_map(|change| change.resource.strip_prefix("graph.").map(str::to_string))
        .collect();
    let mut executed_deletes: Vec<(String, Option<String>)> = Vec::new(); // (graph_id, approval_id)
    let mut consumed_approval_ids: Vec<String> = Vec::new();
    for graph_id in &graph_deletes_to_run {
        if graph_moving_aborted {
            diagnostics.push(Diagnostic::warning(
                "graph_delete_skipped",
                graph_address(graph_id),
                "skipped after an earlier graph-moving operation failed in this run",
            ));
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
            continue;
        }
        let graph_addr = graph_address(graph_id);
        // Re-locate the consumable approval (classification verified one exists).
        let approval_id = approval_artifacts
            .iter()
            .map(|(_, artifact)| artifact)
            .find(|artifact| {
                artifact.consumed_at.is_none()
                    && artifact.resource == graph_addr
                    && artifact.bound_config_digest == desired.config_digest
            })
            .map(|artifact| artifact.approval_id.clone());
        let graph_uri = display_path(
            &desired
                .config_dir
                .join(CLUSTER_GRAPHS_DIR)
                .join(format!("{graph_id}.omni")),
        );
        let observed_manifest_version = match Omnigraph::open_read_only(&graph_uri).await {
            Ok(db) => match db.snapshot_of(ReadTarget::branch("main")).await {
                Ok(snapshot) => Some(snapshot.version()),
                Err(_) => None,
            },
            Err(_) => None, // partial/unopenable roots still get deleted
        };
        let sidecar = RecoverySidecar {
            schema_version: 1,
            operation_id: Ulid::new().to_string(),
            started_at: now_rfc3339(),
            actor: options.actor.clone(),
            kind: RecoverySidecarKind::GraphDelete,
            graph_id: graph_id.clone(),
            graph_uri: graph_uri.clone(),
            observed_manifest_version,
            expected_manifest_version: None, // no post-op manifest exists
            desired_schema_digest: String::new(),
            state_cas_base: expected_cas.clone(),
            approval_id: approval_id.clone(),
        };
        let sidecar_path = match backend.write_recovery_sidecar(&sidecar) {
            Ok(path) => path,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
                graph_moving_aborted = true;
                continue;
            }
        };
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.before_graph_delete") {
            // Simulated crash before removal: row 8 retires the intent and
            // the still-valid approval lets a later run retry.
            diagnostics.push(diagnostic);
            failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
            graph_moving_aborted = true;
            continue;
        }
        match fs::remove_dir_all(PathBuf::from(&graph_uri)) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {} // already gone
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "graph_delete_failed",
                    graph_addr.clone(),
                    format!("could not remove graph root '{graph_uri}': {err}"),
                ));
                failed_graphs.insert(graph_id.clone(), FailedGraphOrigin::GraphDelete);
                graph_moving_aborted = true;
                continue;
            }
        }
        // Crash point: the root is gone, the ledger does not record it yet.
        // The sweep rolls forward (row 7b) and consumes the approval.
        if let Err(diagnostic) = failpoints::maybe_fail("cluster_apply.after_graph_delete") {
            diagnostics.push(diagnostic);
            return early_return(
                display_path(&desired.config_dir),
                Some(desired.config_digest),
                observations,
                changes,
                state.resource_statuses,
                diagnostics,
            );
        }
        executed_deletes.push((graph_id.clone(), approval_id.clone()));
        if let Some(approval_id) = approval_id {
            consumed_approval_ids.push(approval_id);
        }
        completed_op_sidecars.push(sidecar_path);
    }
    if !failed_graphs.is_empty() {
        demote_dependents_of_failed_graphs(&mut changes, &failed_graphs, &desired.dependencies);
    }

    // State mutation. Apply owns query/policy statuses only; graph/schema
    // statuses belong to refresh/import observation and must not be clobbered
    // (the sweep above is the one exception: it owns recovery statuses).
    let mut new_state = state.clone();
    for change in &changes {
        match change.disposition {
            Some(ApplyDisposition::Applied) => match change.operation {
                PlanOperation::Create | PlanOperation::Update => {
                    new_state.applied_revision.resources.insert(
                        change.resource.clone(),
                        StateResource {
                            digest: change
                                .after_digest
                                .clone()
                                .expect("create/update always carries an after digest"),
                        },
                    );
                    set_resource_status_applied(&mut new_state, &change.resource);
                }
                PlanOperation::Delete => {
                    new_state.applied_revision.resources.remove(&change.resource);
                    new_state.resource_statuses.remove(&change.resource);
                }
            },
            Some(ApplyDisposition::Blocked) => {
                // The sweep owns recovery statuses (Drifted/Error with their
                // conditions); a generic Blocked must not clobber them.
                if change.reason.as_deref() != Some("cluster_recovery_pending") {
                    set_resource_status(
                        &mut new_state,
                        &change.resource,
                        ResourceLifecycleStatus::Blocked,
                        change.reason.as_deref().unwrap_or("dependency_not_applied"),
                        "waiting on an unapplied or missing dependency",
                    );
                }
            }
            _ => {}
        }
    }
    for (graph_id, approval_id) in &executed_deletes {
        tombstone_graph_subtree(
            &mut new_state,
            graph_id,
            approval_id.as_deref(),
            options.actor.as_deref(),
        );
        if let Some(approval_id) = approval_id {
            record_approval_consumed(&mut new_state, approval_id, "apply");
        }
    }
    recompute_state_graph_digests(&mut new_state, &desired);

    let residual = diff_resources(
        &state_resource_digests(&new_state),
        &desired.resource_digests,
    );
    let converged = residual.is_empty();
    if converged {
        new_state.applied_revision.config_digest = Some(desired.config_digest.clone());
    }

    let after_value =
        serde_json::to_value(&new_state).expect("cluster state must serialize deterministically");
    let mut state_written = false;
    let mut state_write_failed = false;
    if after_value != before_value {
        new_state.state_revision = new_state.state_revision.saturating_add(1);
        // The failpoint error routes through state_write_failed so the
        // persisted-statuses revert contract below is exercised; a cfg_callback
        // on this point can mutate state.json to simulate a concurrent writer,
        // making write_state's CAS check fail organically.
        let write_result = failpoints::maybe_fail("cluster_apply.before_state_write")
            .and_then(|()| backend.write_state(&new_state, expected_cas.as_deref(), &mut observations));
        match write_result {
            Ok(()) => state_written = true,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                state_write_failed = true;
            }
        }
    }
    // Completed (rows 2/4) sweep sidecars are deleted only once their outcome
    // is durably recorded; on a failed write they stay and re-sweep next run.
    if !state_write_failed {
        for sidecar_path in sweep
            .completed_sidecars
            .iter()
            .chain(completed_op_sidecars.iter())
        {
            let _ = fs::remove_file(sidecar_path);
        }
        let mut all_consumed = sweep.consumed_approvals.clone();
        all_consumed.extend(consumed_approval_ids.iter().cloned());
        mark_approvals_consumed(&backend, &all_consumed);
    }
    // On a failed state write, report the statuses that are actually on disk
    // (the pre-apply snapshot), not the in-memory mutations that were never
    // persisted — automation reading `resource_statuses` independently of `ok`
    // must not see phantom status updates.
    let resource_statuses = if state_write_failed {
        state.resource_statuses
    } else {
        new_state.resource_statuses
    };

    let applied_count = changes
        .iter()
        .filter(|change| change.disposition == Some(ApplyDisposition::Applied))
        .count();
    let deferred_count = changes
        .iter()
        .filter(|change| {
            matches!(
                change.disposition,
                Some(ApplyDisposition::Deferred) | Some(ApplyDisposition::Blocked)
            )
        })
        .count();

    ApplyOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&desired.config_dir),
        actor: options.actor.clone(),
        desired_revision: DesiredRevision {
            config_digest: Some(desired.config_digest),
        },
        state_observations: observations,
        changes,
        applied_count,
        deferred_count,
        converged,
        state_written,
        resource_statuses,
        diagnostics,
    }
}

/// Record a digest-bound human approval for a gated (irreversible) change —
/// today: graph deletes. The artifact binds to the exact desired config
/// digest and the change's before/after digests, so config or state drift
/// invalidates it automatically (a stale approval can never authorize a
/// different change).
pub async fn approve_config_dir(
    config_dir: impl AsRef<Path>,
    resource: &str,
    approved_by: &str,
) -> ApproveOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let backend = LocalStateBackend::new(&outcome.config_dir);
    let mut observations = backend.observations();

    let fail = |config_dir: String, diagnostics: Vec<Diagnostic>| ApproveOutput {
        ok: false,
        config_dir,
        approval_id: None,
        resource: None,
        operation: None,
        approved_by: None,
        diagnostics,
    };

    let Some(desired) = outcome.desired else {
        return fail(display_path(&outcome.config_dir), diagnostics);
    };
    if has_errors(&diagnostics) {
        return fail(display_path(&desired.config_dir), diagnostics);
    }

    let _lock_guard = if desired.state_lock {
        match backend.acquire_lock("approve", &mut observations) {
            Ok(guard) => Some(guard),
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                return fail(display_path(&desired.config_dir), diagnostics);
            }
        }
    } else {
        diagnostics.push(Diagnostic::warning(
            "state_lock_disabled",
            "state.lock",
            "state.lock is false; approve ran without acquiring the cluster state lock",
        ));
        None
    };

    let state = match backend.read_state(&mut observations) {
        Ok(snapshot) => match snapshot.state {
            Some(state) => state,
            None => {
                diagnostics.push(Diagnostic::error(
                    "state_missing",
                    CLUSTER_STATE_FILE,
                    "approve requires an existing state.json; run `cluster import` first",
                ));
                return fail(display_path(&desired.config_dir), diagnostics);
            }
        },
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return fail(display_path(&desired.config_dir), diagnostics);
        }
    };

    let prior_resources = state_resource_digests(&state);
    let changes = diff_resources(&prior_resources, &desired.resource_digests);
    let gates = compute_approvals(&changes, &BTreeSet::new());
    let Some(change) = changes.iter().find(|change| {
        change.resource == resource && gates.iter().any(|gate| gate.resource == resource)
    }) else {
        diagnostics.push(Diagnostic::error(
            "approval_not_required",
            resource,
            "no pending change for this resource requires approval (check `cluster plan`)",
        ));
        return fail(display_path(&desired.config_dir), diagnostics);
    };

    let artifact = ApprovalArtifact {
        schema_version: 1,
        approval_id: Ulid::new().to_string(),
        resource: change.resource.clone(),
        operation: match change.operation {
            PlanOperation::Create => "create",
            PlanOperation::Update => "update",
            PlanOperation::Delete => "delete",
        }
        .to_string(),
        reason: gates
            .iter()
            .find(|gate| gate.resource == resource)
            .map(|gate| gate.reason.clone())
            .unwrap_or_default(),
        bound_config_digest: desired.config_digest.clone(),
        bound_before_digest: change.before_digest.clone(),
        bound_after_digest: change.after_digest.clone(),
        approved_by: approved_by.to_string(),
        created_at: now_rfc3339(),
        consumed_at: None,
        consumed_by_operation: None,
    };
    if let Err(diagnostic) = backend.write_approval_artifact(&artifact) {
        diagnostics.push(diagnostic);
        return fail(display_path(&desired.config_dir), diagnostics);
    }

    ApproveOutput {
        ok: !has_errors(&diagnostics),
        config_dir: display_path(&desired.config_dir),
        approval_id: Some(artifact.approval_id),
        resource: Some(artifact.resource),
        operation: Some(change.operation.clone()),
        approved_by: Some(artifact.approved_by),
        diagnostics,
    }
}

pub fn status_config_dir(config_dir: impl AsRef<Path>) -> StatusOutput {
    let parsed = parse_cluster_config(config_dir.as_ref());
    let mut diagnostics = parsed.diagnostics;
    let backend = LocalStateBackend::new(&parsed.config_dir);
    let mut observations = backend.observations();
    backend.observe_lock(&mut observations, &mut diagnostics);
    warn_pending_recovery_sidecars(&parsed.config_dir, &mut diagnostics);

    let mut resource_digests = BTreeMap::new();
    let mut resource_statuses = BTreeMap::new();
    let mut state_observation_records = BTreeMap::new();

    if let Some(raw) = parsed.raw.as_ref() {
        let _settings = validate_cluster_header(raw, &mut diagnostics);
        if !has_errors(&diagnostics) {
            match backend.read_state(&mut observations) {
                Ok(snapshot) => {
                    if let Some(state) = snapshot.state {
                        // Read-only point-in-time catalog check: report the
                        // findings as diagnostics; persisting Drifted statuses
                        // is refresh's job. Status never writes state.
                        for (address, finding) in
                            verify_catalog_payloads(&parsed.config_dir, &state)
                        {
                            diagnostics.push(payload_finding_diagnostic(&address, &finding));
                        }
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

    // Recovery sweep first (RFC-004 §D3): classify any interrupted graph
    // operation before observation/verification so a rolled-forward outcome
    // is what those passes see.
    let sweep = sweep_recovery_sidecars(&backend, &mut state, &mut diagnostics).await;

    // Catalog payload verification must run BEFORE graph observation: removing
    // a drifted query digest first means the live-graph composite recompute
    // below already excludes it, so the persisted graph.<id> composite stays
    // consistent and the next plan shows exactly the create + derived update.
    for (address, finding) in verify_catalog_payloads(&desired.config_dir, &state) {
        diagnostics.push(payload_finding_diagnostic(&address, &finding));
        match finding {
            PayloadFinding::Missing => {
                state.applied_revision.resources.remove(&address);
                set_resource_status(
                    &mut state,
                    &address,
                    ResourceLifecycleStatus::Drifted,
                    "payload_missing",
                    "catalog payload blob is missing; re-run `cluster apply` to republish",
                );
            }
            PayloadFinding::Mismatch { .. } => {
                state.applied_revision.resources.remove(&address);
                set_resource_status(
                    &mut state,
                    &address,
                    ResourceLifecycleStatus::Drifted,
                    "payload_mismatch",
                    "catalog payload blob does not match the recorded digest; re-run `cluster apply` to republish",
                );
            }
            // Transient IO must not trigger a spurious republish: keep the
            // digest, surface the error, let a later clean refresh converge.
            PayloadFinding::ReadError(error) => {
                set_resource_status(
                    &mut state,
                    &address,
                    ResourceLifecycleStatus::Error,
                    "payload_read_error",
                    &error,
                );
            }
        }
    }

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
        Ok(()) => {
            // Completed sweep sidecars are deleted only after their outcome
            // is durably recorded; on failure they stay and re-sweep.
            for sidecar_path in &sweep.completed_sidecars {
                let _ = fs::remove_file(sidecar_path);
            }
            mark_approvals_consumed(&backend, &sweep.consumed_approvals);
        }
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
            recoveries_dir: config_dir.join(CLUSTER_RECOVERIES_DIR),
            approvals_dir: config_dir.join(CLUSTER_APPROVALS_DIR),
            state_dir,
        }
    }

    /// List approval artifacts in ULID (filename) order; unparseable files
    /// warn and stay on disk for the operator.
    fn list_approval_artifacts(
        &self,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<(PathBuf, ApprovalArtifact)> {
        let mut paths = Vec::new();
        match fs::read_dir(&self.approvals_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        paths.push(path);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => diagnostics.push(Diagnostic::warning(
                "approval_read_error",
                CLUSTER_APPROVALS_DIR,
                format!("could not list approval artifacts: {err}"),
            )),
        }
        paths.sort();
        let mut artifacts = Vec::new();
        for path in paths {
            match fs::read_to_string(&path)
                .map_err(|err| err.to_string())
                .and_then(|text| {
                    serde_json::from_str::<ApprovalArtifact>(&text).map_err(|err| err.to_string())
                }) {
                Ok(artifact) if artifact.schema_version == 1 => artifacts.push((path, artifact)),
                Ok(artifact) => diagnostics.push(Diagnostic::warning(
                    "unsupported_approval_version",
                    display_path(&path),
                    format!(
                        "unsupported approval artifact version {}; leaving it in place",
                        artifact.schema_version
                    ),
                )),
                Err(err) => diagnostics.push(Diagnostic::warning(
                    "invalid_approval_artifact",
                    display_path(&path),
                    format!("could not parse approval artifact ({err}); leaving it in place"),
                )),
            }
        }
        artifacts
    }

    /// Atomically write (or rewrite, e.g. on consumption) an approval artifact.
    fn write_approval_artifact(&self, artifact: &ApprovalArtifact) -> Result<PathBuf, Diagnostic> {
        fs::create_dir_all(&self.approvals_dir).map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                CLUSTER_APPROVALS_DIR,
                format!("could not create approvals directory: {err}"),
            )
        })?;
        let target = self
            .approvals_dir
            .join(format!("{}.json", artifact.approval_id));
        let mut payload = serde_json::to_string_pretty(artifact).map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                display_path(&target),
                format!("could not encode approval artifact: {err}"),
            )
        })?;
        payload.push('\n');
        let tmp_path = self
            .approvals_dir
            .join(format!("{}.json.tmp.{}", artifact.approval_id, Ulid::new()));
        fs::write(&tmp_path, payload.as_bytes()).map_err(|err| {
            Diagnostic::error(
                "approval_write_error",
                display_path(&tmp_path),
                format!("could not write approval artifact: {err}"),
            )
        })?;
        if let Err(err) = fs::rename(&tmp_path, &target) {
            let _ = fs::remove_file(&tmp_path);
            return Err(Diagnostic::error(
                "approval_write_error",
                display_path(&target),
                format!("could not move approval artifact into place: {err}"),
            ));
        }
        Ok(target)
    }

    /// List recovery sidecars in ULID (filename) order. Unparseable files are
    /// reported as warnings and skipped — they stay on disk for the operator.
    fn list_recovery_sidecars(
        &self,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<(PathBuf, RecoverySidecar)> {
        let mut paths = Vec::new();
        match fs::read_dir(&self.recoveries_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "json") {
                        paths.push(path);
                    }
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                diagnostics.push(Diagnostic::warning(
                    "recovery_sidecar_read_error",
                    CLUSTER_RECOVERIES_DIR,
                    format!("could not list recovery sidecars: {err}"),
                ));
            }
        }
        paths.sort();
        let mut sidecars = Vec::new();
        for path in paths {
            match fs::read_to_string(&path)
                .map_err(|err| err.to_string())
                .and_then(|text| {
                    serde_json::from_str::<RecoverySidecar>(&text).map_err(|err| err.to_string())
                }) {
                Ok(sidecar) if sidecar.schema_version == 1 => sidecars.push((path, sidecar)),
                Ok(sidecar) => diagnostics.push(Diagnostic::warning(
                    "unsupported_recovery_sidecar_version",
                    display_path(&path),
                    format!(
                        "unsupported recovery sidecar version {}; leaving it in place",
                        sidecar.schema_version
                    ),
                )),
                Err(err) => diagnostics.push(Diagnostic::warning(
                    "invalid_recovery_sidecar",
                    display_path(&path),
                    format!("could not parse recovery sidecar ({err}); leaving it in place"),
                )),
            }
        }
        sidecars
    }

    /// Atomically write (or rewrite) a recovery sidecar; returns its path.
    fn write_recovery_sidecar(&self, sidecar: &RecoverySidecar) -> Result<PathBuf, Diagnostic> {
        fs::create_dir_all(&self.recoveries_dir).map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                CLUSTER_RECOVERIES_DIR,
                format!("could not create recoveries directory: {err}"),
            )
        })?;
        let target = self
            .recoveries_dir
            .join(format!("{}.json", sidecar.operation_id));
        let mut payload = serde_json::to_string_pretty(sidecar).map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                display_path(&target),
                format!("could not encode recovery sidecar: {err}"),
            )
        })?;
        payload.push('\n');
        let tmp_path = self
            .recoveries_dir
            .join(format!("{}.json.tmp.{}", sidecar.operation_id, Ulid::new()));
        fs::write(&tmp_path, payload.as_bytes()).map_err(|err| {
            Diagnostic::error(
                "recovery_sidecar_write_error",
                display_path(&tmp_path),
                format!("could not write recovery sidecar: {err}"),
            )
        })?;
        if let Err(err) = fs::rename(&tmp_path, &target) {
            let _ = fs::remove_file(&tmp_path);
            return Err(Diagnostic::error(
                "recovery_sidecar_write_error",
                display_path(&target),
                format!("could not move recovery sidecar into place: {err}"),
            ));
        }
        Ok(target)
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

/// Recovery sweep (RFC-004 §D3): runs at the start of every state-mutating
/// cluster command, under the state lock, before the command's own work.
/// Roll-forward-only — the engine's own sidecars make each graph-level
/// operation atomic within the graph, so the cluster never rolls a graph
/// back; it converges the ledger to observable reality or refuses loudly.
/// Mutations ride the calling command's CAS-checked state write; completed
/// sidecars are deleted only after that write lands.
async fn sweep_recovery_sidecars(
    backend: &LocalStateBackend,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
) -> SweepOutcome {
    let mut outcome = SweepOutcome::default();
    for (path, sidecar) in backend.list_recovery_sidecars(diagnostics) {
        match sidecar.kind {
            RecoverySidecarKind::GraphCreate => {
                sweep_graph_create_sidecar(path, sidecar, state, diagnostics, &mut outcome).await;
            }
            RecoverySidecarKind::SchemaApply => {
                sweep_schema_apply_sidecar(path, sidecar, state, diagnostics, &mut outcome).await;
            }
            RecoverySidecarKind::GraphDelete => {
                sweep_graph_delete_sidecar(path, sidecar, state, diagnostics, &mut outcome);
            }
        }
    }
    outcome
}

async fn sweep_graph_create_sidecar(
    path: PathBuf,
    sidecar: RecoverySidecar,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
    outcome: &mut SweepOutcome,
) {
    let graph_address = graph_address(&sidecar.graph_id);
    let schema_addr = schema_address(&sidecar.graph_id);
    let graph_path = PathBuf::from(&sidecar.graph_uri);

    // Row 1: nothing moved — the init never landed. The sidecar is pure
    // intent; remove it and let the command's own plan re-propose the create.
    if !graph_path.exists() {
        let _ = fs::remove_file(&path);
        return;
    }

    match Omnigraph::open_read_only(&sidecar.graph_uri).await {
        Ok(db) => {
            let live_digest = sha256_hex(db.schema_source().as_bytes());
            let recorded = state
                .applied_revision
                .resources
                .get(&schema_addr)
                .map(|resource| resource.digest.clone());
            if recorded.as_deref() == Some(live_digest.as_str()) {
                // Row 2: crash fell between the state CAS and sidecar delete.
                outcome.completed_sidecars.push(path);
            } else if live_digest == sidecar.desired_schema_digest {
                // Row 4: the create completed on the graph; roll the cluster
                // state forward to observable reality.
                state.applied_revision.resources.insert(
                    schema_addr.clone(),
                    StateResource {
                        digest: live_digest.clone(),
                    },
                );
                let query_digests = state_query_digests_for_graph(state, &sidecar.graph_id);
                let composite =
                    graph_digest(&sidecar.graph_id, Some(&live_digest), Some(&query_digests));
                state
                    .applied_revision
                    .resources
                    .insert(graph_address.clone(), StateResource { digest: composite });
                set_resource_status_applied(state, &graph_address);
                set_resource_status_applied(state, &schema_addr);
                state.recovery_records.insert(
                    sidecar.operation_id.clone(),
                    json!({
                        "kind": "graph_create",
                        "graph_id": sidecar.graph_id,
                        "outcome": "rolled_forward",
                        "recovered_at": now_rfc3339(),
                        "actor": sidecar.actor,
                    }),
                );
                diagnostics.push(Diagnostic::warning(
                    "cluster_recovery_rolled_forward",
                    graph_address.clone(),
                    "an interrupted graph create had completed on the graph; cluster state was rolled forward to match",
                ));
                outcome.completed_sidecars.push(path);
            } else {
                // Row 6: the graph moved to something the sidecar did not
                // intend. Refuse to guess; require refresh + operator re-plan.
                set_resource_status(
                    state,
                    &graph_address,
                    ResourceLifecycleStatus::Drifted,
                    "actual_applied_state_pending",
                    "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
                );
                set_resource_status(
                    state,
                    &schema_addr,
                    ResourceLifecycleStatus::Drifted,
                    "actual_applied_state_pending",
                    "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
                );
                diagnostics.push(Diagnostic::warning(
                    "cluster_recovery_pending",
                    graph_address.clone(),
                    "an interrupted graph create left unexpected graph state; graph-moving work is blocked until repaired",
                ));
                outcome.pending_graphs.insert(sidecar.graph_id.clone());
            }
        }
        Err(err) => {
            // Row 5: partial root (the engine's documented init gap). Never
            // auto-delete — reconciler deletes are the same data-loss class
            // as human deletes; the operator removes the root explicitly.
            set_resource_status(
                state,
                &graph_address,
                ResourceLifecycleStatus::Error,
                "graph_create_incomplete",
                "graph root exists but cannot be opened; remove the graph root and re-run `cluster apply`",
            );
            set_resource_status(
                state,
                &schema_addr,
                ResourceLifecycleStatus::Error,
                "graph_create_incomplete",
                "graph root exists but cannot be opened; remove the graph root and re-run `cluster apply`",
            );
            diagnostics.push(Diagnostic::error(
                "graph_create_incomplete",
                graph_address.clone(),
                format!(
                    "graph root '{}' exists but cannot be opened ({err}); remove the graph root and re-run `cluster apply`",
                    sidecar.graph_uri
                ),
            ));
            outcome.pending_graphs.insert(sidecar.graph_id.clone());
        }
    }
}

async fn sweep_schema_apply_sidecar(
    path: PathBuf,
    sidecar: RecoverySidecar,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
    outcome: &mut SweepOutcome,
) {
    let graph_address = graph_address(&sidecar.graph_id);
    let schema_addr = schema_address(&sidecar.graph_id);

    // Digest-based classification: robust to unrelated manifest movement;
    // the sidecar's version pins stay forensic.
    let live_digest = match Omnigraph::open_read_only(&sidecar.graph_uri).await {
        Ok(db) => sha256_hex(db.schema_source().as_bytes()),
        Err(err) => {
            // Cannot verify the interrupted operation — refuse to guess.
            diagnostics.push(Diagnostic::warning(
                "cluster_recovery_pending",
                graph_address.clone(),
                format!(
                    "an interrupted schema apply cannot be verified (graph '{}' did not open: {err}); graph-moving work is blocked until repaired",
                    sidecar.graph_uri
                ),
            ));
            outcome.pending_graphs.insert(sidecar.graph_id.clone());
            return;
        }
    };

    let recorded = state
        .applied_revision
        .resources
        .get(&schema_addr)
        .map(|resource| resource.digest.clone());
    if recorded.as_deref() == Some(live_digest.as_str()) {
        // Ledger consistent with the live graph (the apply never landed, or
        // landed and was recorded): the sidecar is stale intent — retire it.
        outcome.completed_sidecars.push(path);
    } else if live_digest == sidecar.desired_schema_digest {
        // RFC-004 §D3 row 3: the schema apply completed on the graph; roll
        // the cluster state forward to observable reality.
        state.applied_revision.resources.insert(
            schema_addr.clone(),
            StateResource {
                digest: live_digest.clone(),
            },
        );
        let query_digests = state_query_digests_for_graph(state, &sidecar.graph_id);
        let composite = graph_digest(&sidecar.graph_id, Some(&live_digest), Some(&query_digests));
        state
            .applied_revision
            .resources
            .insert(graph_address.clone(), StateResource { digest: composite });
        set_resource_status_applied(state, &graph_address);
        set_resource_status_applied(state, &schema_addr);
        state.recovery_records.insert(
            sidecar.operation_id.clone(),
            json!({
                "kind": "schema_apply",
                "graph_id": sidecar.graph_id,
                "outcome": "rolled_forward",
                "recovered_at": now_rfc3339(),
                "actor": sidecar.actor,
            }),
        );
        diagnostics.push(Diagnostic::warning(
            "cluster_recovery_rolled_forward",
            graph_address.clone(),
            "an interrupted schema apply had completed on the graph; cluster state was rolled forward to match",
        ));
        outcome.completed_sidecars.push(path);
    } else {
        // Row 6: live schema is neither the recorded nor the desired digest.
        set_resource_status(
            state,
            &graph_address,
            ResourceLifecycleStatus::Drifted,
            "actual_applied_state_pending",
            "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
        );
        set_resource_status(
            state,
            &schema_addr,
            ResourceLifecycleStatus::Drifted,
            "actual_applied_state_pending",
            "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
        );
        diagnostics.push(Diagnostic::warning(
            "cluster_recovery_pending",
            graph_address.clone(),
            "an interrupted schema apply left unexpected graph state; graph-moving work is blocked until repaired",
        ));
        outcome.pending_graphs.insert(sidecar.graph_id.clone());
    }
}

fn sweep_graph_delete_sidecar(
    path: PathBuf,
    sidecar: RecoverySidecar,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
    outcome: &mut SweepOutcome,
) {
    let graph_address = graph_address(&sidecar.graph_id);
    let root = PathBuf::from(&sidecar.graph_uri);

    if root.exists() {
        // Row 8: the delete never completed. Prefix removal is idempotent and
        // works on partial roots, so the repair is simply the re-proposed,
        // still-approved delete on a later run — retire the stale intent.
        diagnostics.push(Diagnostic::warning(
            "graph_delete_incomplete",
            graph_address,
            "a previous graph delete did not complete; it will be re-proposed by plan and can be retried under its approval",
        ));
        outcome.completed_sidecars.push(path);
        return;
    }

    if !state.applied_revision.resources.contains_key(&graph_address) {
        // Row 7: already tombstoned (or never recorded); crash fell between
        // the state CAS and sidecar delete.
        outcome.completed_sidecars.push(path);
        return;
    }

    // Row 7b: the root is gone, the ledger is stale — roll forward the
    // tombstone, consume the approval the sidecar carries, audit.
    tombstone_graph_subtree(state, &sidecar.graph_id, sidecar.approval_id.as_deref(), sidecar.actor.as_deref());
    state.recovery_records.insert(
        sidecar.operation_id.clone(),
        json!({
            "kind": "graph_delete",
            "graph_id": sidecar.graph_id,
            "outcome": "rolled_forward",
            "recovered_at": now_rfc3339(),
            "actor": sidecar.actor,
        }),
    );
    if let Some(approval_id) = &sidecar.approval_id {
        record_approval_consumed(state, approval_id, &sidecar.operation_id);
        outcome.consumed_approvals.push(approval_id.clone());
    }
    diagnostics.push(Diagnostic::warning(
        "cluster_recovery_rolled_forward",
        graph_address,
        "an interrupted graph delete had completed on disk; cluster state was rolled forward to match",
    ));
    outcome.completed_sidecars.push(path);
}

/// Remove a graph's subtree (graph, schema, queries) from the ledger and
/// leave a tombstone observation. Idempotent.
fn tombstone_graph_subtree(
    state: &mut ClusterState,
    graph_id: &str,
    approval_id: Option<&str>,
    actor: Option<&str>,
) {
    let graph_addr = graph_address(graph_id);
    let schema_addr = schema_address(graph_id);
    let query_prefix = format!("query.{graph_id}.");
    state.applied_revision.resources.remove(&graph_addr);
    state.applied_revision.resources.remove(&schema_addr);
    state
        .applied_revision
        .resources
        .retain(|address, _| !address.starts_with(&query_prefix));
    state.resource_statuses.remove(&graph_addr);
    state.resource_statuses.remove(&schema_addr);
    state
        .resource_statuses
        .retain(|address, _| !address.starts_with(&query_prefix));
    state.observations.insert(
        graph_addr,
        json!({
            "kind": "tombstone",
            "deleted_at": now_rfc3339(),
            "approval_id": approval_id,
            "actor": actor,
        }),
    );
}

/// Record approval consumption in the state ledger. The artifact FILE is
/// rewritten with consumed_at only after the state write lands, so a failed
/// CAS leaves the approval valid for the retry.
fn record_approval_consumed(state: &mut ClusterState, approval_id: &str, operation_id: &str) {
    state.approval_records.insert(
        approval_id.to_string(),
        json!({
            "consumed_at": now_rfc3339(),
            "consumed_by_operation": operation_id,
        }),
    );
}

/// Mark approval artifact files consumed on disk (post-CAS).
fn mark_approvals_consumed(backend: &LocalStateBackend, approval_ids: &[String]) {
    if approval_ids.is_empty() {
        return;
    }
    let mut sink = Vec::new();
    for (_, mut artifact) in backend.list_approval_artifacts(&mut sink) {
        if approval_ids.contains(&artifact.approval_id) && artifact.consumed_at.is_none() {
            artifact.consumed_at = Some(now_rfc3339());
            let _ = backend.write_approval_artifact(&artifact);
        }
    }
}

/// Read-only commands report pending sidecars without acting on them.
fn warn_pending_recovery_sidecars(config_dir: &Path, diagnostics: &mut Vec<Diagnostic>) {
    let recoveries_dir = config_dir.join(CLUSTER_RECOVERIES_DIR);
    let Ok(entries) = fs::read_dir(&recoveries_dir) else {
        return;
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    for name in names {
        diagnostics.push(Diagnostic::warning(
            "cluster_recovery_pending",
            format!("{CLUSTER_RECOVERIES_DIR}/{name}"),
            "a recovery sidecar from an interrupted apply is pending; the next state-mutating command will classify it",
        ));
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

/// RFC-004 §D7: the data-aware preview — the engine's migration plan for a
/// desired schema against the live graph, computed read-only (no lock).
async fn preview_schema_migration(
    graph_uri: &str,
    schema_path: &str,
) -> Result<SchemaMigrationPlan, String> {
    let source = fs::read_to_string(schema_path).map_err(|err| err.to_string())?;
    let db = Omnigraph::open_read_only(graph_uri)
        .await
        .map_err(|err| err.to_string())?;
    let preview = db
        .preview_schema_apply_with_options(&source, SchemaApplyOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    Ok(preview.plan)
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
                disposition: None,
                reason: None,
                migration: None,
            }),
            Some(before) if before != after => changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Update,
                before_digest: Some(before.clone()),
                after_digest: Some(after.clone()),
                disposition: None,
                reason: None,
                migration: None,
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
                disposition: None,
                reason: None,
                migration: None,
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

fn compute_approvals(
    changes: &[PlanChange],
    approved: &BTreeSet<String>,
) -> Vec<ApprovalRequirement> {
    // One gate per subtree: the graph.<id> delete carries its schema and
    // queries, so a schema delete whose graph is also deleted is not listed.
    let graph_deletes: BTreeSet<String> = changes
        .iter()
        .filter(|change| change.operation == PlanOperation::Delete)
        .filter_map(|change| change.resource.strip_prefix("graph.").map(str::to_string))
        .collect();
    changes
        .iter()
        .filter_map(|change| {
            if change.operation != PlanOperation::Delete {
                return None;
            }
            let gated = match resource_kind(&change.resource) {
                ResourceKind::Graph(_) => true,
                ResourceKind::Schema(graph) => !graph_deletes.contains(&graph),
                _ => false,
            };
            gated.then(|| ApprovalRequirement {
                resource: change.resource.clone(),
                reason: "delete may remove deployed graph or schema definition".to_string(),
                satisfied: approved.contains(&change.resource),
            })
        })
        .collect()
}

/// Resources with a valid (digest-matching, unconsumed) pending approval.
/// Near-misses — an artifact for the same resource whose bound digests no
/// longer match — warn as `approval_stale` and never authorize anything.
fn approved_resources(
    artifacts: &[(PathBuf, ApprovalArtifact)],
    changes: &[PlanChange],
    config_digest: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> BTreeSet<String> {
    let mut approved = BTreeSet::new();
    for change in changes {
        let candidates: Vec<&ApprovalArtifact> = artifacts
            .iter()
            .map(|(_, artifact)| artifact)
            .filter(|artifact| artifact.consumed_at.is_none() && artifact.resource == change.resource)
            .collect();
        if candidates.is_empty() {
            continue;
        }
        let matched = candidates.iter().any(|artifact| {
            artifact.bound_config_digest == config_digest
                && artifact.bound_before_digest == change.before_digest
                && artifact.bound_after_digest == change.after_digest
        });
        if matched {
            approved.insert(change.resource.clone());
        } else {
            diagnostics.push(Diagnostic::warning(
                "approval_stale",
                change.resource.clone(),
                "an approval artifact exists but its bound digests no longer match the plan; re-run `cluster approve`",
            ));
        }
    }
    approved
}

#[derive(Debug, PartialEq, Eq)]
enum ResourceKind {
    Graph(String),
    Schema(String),
    Query { graph: String, name: String },
    Policy(String),
    Unknown,
}

fn resource_kind(address: &str) -> ResourceKind {
    if let Some(graph) = address.strip_prefix("graph.") {
        ResourceKind::Graph(graph.to_string())
    } else if let Some(graph) = address.strip_prefix("schema.") {
        ResourceKind::Schema(graph.to_string())
    } else if let Some(rest) = address.strip_prefix("query.") {
        match rest.split_once('.') {
            Some((graph, name)) => ResourceKind::Query {
                graph: graph.to_string(),
                name: name.to_string(),
            },
            None => ResourceKind::Unknown,
        }
    } else if let Some(name) = address.strip_prefix("policy.") {
        ResourceKind::Policy(name.to_string())
    } else {
        ResourceKind::Unknown
    }
}

/// Classify every planned change with the disposition config-only apply gives
/// it. Stage 3A executes only query/policy catalog writes; graph/schema
/// movement is a later phase, and `graph.<id>` composite updates whose schema
/// component is unchanged converge automatically once query digests land.
fn classify_changes(
    changes: &mut [PlanChange],
    dependencies: &[Dependency],
    pending_recovery: &BTreeSet<String>,
    approved: &BTreeSet<String>,
) {
    let mut schema_creates = BTreeSet::new();
    let mut schema_pending = BTreeSet::new();
    let mut graph_creates = BTreeSet::new();
    let mut graph_deletes = BTreeSet::new();
    for change in changes.iter() {
        match resource_kind(&change.resource) {
            ResourceKind::Schema(graph) => match change.operation {
                PlanOperation::Create => {
                    schema_creates.insert(graph);
                }
                // Schema updates execute in-run before catalog writes (4B)
                // and no longer block dependents; deletes (4C) still do.
                PlanOperation::Update => {}
                PlanOperation::Delete => {
                    schema_pending.insert(graph);
                }
            },
            ResourceKind::Graph(graph) => match change.operation {
                PlanOperation::Create => {
                    graph_creates.insert(graph);
                }
                PlanOperation::Delete => {
                    graph_deletes.insert(graph);
                }
                PlanOperation::Update => {}
            },
            _ => {}
        }
    }
    // A schema Create is satisfied by its paired graph create (the init
    // carries the schema); a standalone schema Create stays pending.
    for graph in &schema_creates {
        if !graph_creates.contains(graph) {
            schema_pending.insert(graph.clone());
        }
    }
    // Subtree deletes ride the approved graph delete.
    let rides_approved_delete = |graph: &str| {
        graph_deletes.contains(graph)
            && approved.contains(&graph_address(graph))
            && !pending_recovery.contains(graph)
    };

    for change in changes.iter_mut() {
        let (disposition, reason) = match resource_kind(&change.resource) {
            ResourceKind::Schema(graph) => match change.operation {
                PlanOperation::Create
                    if graph_creates.contains(&graph)
                        && !pending_recovery.contains(&graph) =>
                {
                    // Applied with the graph create — the init carries it.
                    (ApplyDisposition::Applied, None)
                }
                PlanOperation::Update if !pending_recovery.contains(&graph) => {
                    // Stage 4B: schema updates execute via the engine's
                    // schema apply (soft drops only; allow_data_loss is 4C).
                    (ApplyDisposition::Applied, None)
                }
                PlanOperation::Create | PlanOperation::Update => {
                    (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                }
                PlanOperation::Delete if graph_deletes.contains(&graph) => {
                    if rides_approved_delete(&graph) {
                        (ApplyDisposition::Applied, None)
                    } else if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else {
                        (ApplyDisposition::Blocked, Some("approval_required"))
                    }
                }
                _ => (ApplyDisposition::Deferred, Some("apply_unsupported_kind")),
            },
            ResourceKind::Graph(graph) => match change.operation {
                PlanOperation::Create => {
                    if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else {
                        (ApplyDisposition::Applied, None)
                    }
                }
                PlanOperation::Update if !schema_pending.contains(&graph) => {
                    (ApplyDisposition::Derived, None)
                }
                // Stage 4C: an approved graph delete executes (the
                // irreversible tier — gated by a digest-bound artifact).
                PlanOperation::Delete => {
                    if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else if rides_approved_delete(&graph) {
                        (ApplyDisposition::Applied, None)
                    } else {
                        (ApplyDisposition::Blocked, Some("approval_required"))
                    }
                }
                _ => (ApplyDisposition::Deferred, Some("apply_unsupported_kind")),
            },
            ResourceKind::Query { graph, .. } => match change.operation {
                PlanOperation::Delete => {
                    if rides_approved_delete(&graph) {
                        // Tombstoned with the approved graph delete.
                        (ApplyDisposition::Applied, None)
                    } else if graph_deletes.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("approval_required"))
                    } else {
                        (ApplyDisposition::Applied, None)
                    }
                }
                PlanOperation::Create | PlanOperation::Update => {
                    if pending_recovery.contains(&graph) {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else if schema_pending.contains(&graph) {
                        (
                            ApplyDisposition::Blocked,
                            Some("dependency_not_applied"),
                        )
                    } else {
                        // A graph create in the same plan no longer blocks:
                        // creates execute first in the same apply run.
                        (ApplyDisposition::Applied, None)
                    }
                }
            },
            ResourceKind::Policy(_) => match change.operation {
                PlanOperation::Delete => (ApplyDisposition::Applied, None),
                PlanOperation::Create | PlanOperation::Update => {
                    let blocked_pending = dependencies.iter().any(|dep| {
                        dep.from == change.resource
                            && dep
                                .to
                                .strip_prefix("graph.")
                                .is_some_and(|graph| pending_recovery.contains(graph))
                    });
                    if blocked_pending {
                        (ApplyDisposition::Blocked, Some("cluster_recovery_pending"))
                    } else {
                        (ApplyDisposition::Applied, None)
                    }
                }
            },
            ResourceKind::Unknown => {
                (ApplyDisposition::Deferred, Some("apply_unsupported_kind"))
            }
        };
        change.disposition = Some(disposition);
        change.reason = reason.map(str::to_string);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailedGraphOrigin {
    GraphCreate,
    SchemaApply,
    GraphDelete,
}

/// After a graph-moving operation fails mid-run, every change that depended
/// on that graph flips from Applied to Blocked so the output and the
/// persisted statuses tell the truth about what this run actually executed.
/// The originating change carries the failure code; dependents carry
/// `dependency_not_applied`.
fn demote_dependents_of_failed_graphs(
    changes: &mut [PlanChange],
    failed: &BTreeMap<String, FailedGraphOrigin>,
    dependencies: &[Dependency],
) {
    for change in changes.iter_mut() {
        if change.disposition != Some(ApplyDisposition::Applied) {
            continue;
        }
        let demote_reason = match resource_kind(&change.resource) {
            ResourceKind::Graph(graph) => match failed.get(&graph) {
                Some(FailedGraphOrigin::GraphCreate) => Some("graph_create_failed"),
                Some(FailedGraphOrigin::GraphDelete) => Some("graph_delete_failed"),
                Some(FailedGraphOrigin::SchemaApply) => Some("dependency_not_applied"),
                None => None,
            },
            ResourceKind::Schema(graph) => match failed.get(&graph) {
                Some(FailedGraphOrigin::SchemaApply) => Some("schema_apply_failed"),
                Some(FailedGraphOrigin::GraphCreate) | Some(FailedGraphOrigin::GraphDelete) => {
                    Some("dependency_not_applied")
                }
                None => None,
            },
            ResourceKind::Query { graph, .. } if failed.contains_key(&graph) => {
                Some("dependency_not_applied")
            }
            ResourceKind::Policy(_) => {
                let blocked = dependencies.iter().any(|dep| {
                    dep.from == change.resource
                        && dep
                            .to
                            .strip_prefix("graph.")
                            .is_some_and(|graph| failed.contains_key(graph))
                });
                blocked.then_some("dependency_not_applied")
            }
            _ => None,
        };
        if let Some(reason) = demote_reason {
            change.disposition = Some(ApplyDisposition::Blocked);
            change.reason = Some(reason.to_string());
        }
    }
}

/// Content-addressed catalog path for an applied resource payload. Extensions
/// are fixed per kind (`.gq` / `.yaml`) regardless of the source file's name,
/// so the catalog layout cannot drift with operator file conventions.
fn payload_path(config_dir: &Path, kind: &ResourceKind, digest: &str) -> Option<PathBuf> {
    let resources_dir = config_dir.join(CLUSTER_RESOURCES_DIR);
    match kind {
        ResourceKind::Query { graph, name } => Some(
            resources_dir
                .join("query")
                .join(graph)
                .join(name)
                .join(format!("{digest}.gq")),
        ),
        ResourceKind::Policy(name) => Some(
            resources_dir
                .join("policy")
                .join(name)
                .join(format!("{digest}.yaml")),
        ),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum PayloadFinding {
    Missing,
    Mismatch { actual_digest: String },
    ReadError(String),
}

/// Verify every catalog-backed resource digest in state against its
/// content-addressed blob under `__cluster/resources/`. Graph, schema, and
/// unknown addresses have no payloads and are skipped. Read-only; findings
/// are deterministic (BTreeMap order). Payloads are small (queries, policy
/// bundles), so a full digest re-hash is cheap.
fn verify_catalog_payloads(
    config_dir: &Path,
    state: &ClusterState,
) -> Vec<(String, PayloadFinding)> {
    let mut findings = Vec::new();
    for (address, resource) in &state.applied_revision.resources {
        let kind = resource_kind(address);
        let Some(path) = payload_path(config_dir, &kind, &resource.digest) else {
            continue;
        };
        match fs::read(&path) {
            Ok(bytes) => {
                let actual_digest = sha256_hex(&bytes);
                if actual_digest != resource.digest {
                    findings.push((address.clone(), PayloadFinding::Mismatch { actual_digest }));
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                findings.push((address.clone(), PayloadFinding::Missing));
            }
            Err(err) => {
                findings.push((
                    address.clone(),
                    PayloadFinding::ReadError(format!(
                        "could not read catalog payload '{}': {err}",
                        path.display()
                    )),
                ));
            }
        }
    }
    findings
}

fn payload_finding_diagnostic(address: &str, finding: &PayloadFinding) -> Diagnostic {
    match finding {
        PayloadFinding::Missing => Diagnostic::warning(
            "catalog_payload_missing",
            address,
            "catalog payload blob is missing; re-run `cluster apply` to republish",
        ),
        PayloadFinding::Mismatch { actual_digest } => Diagnostic::warning(
            "catalog_payload_mismatch",
            address,
            format!(
                "catalog payload blob does not match the recorded digest (actual sha256:{actual_digest}); re-run `cluster apply` to republish"
            ),
        ),
        // An unverifiable blob must not report healthy.
        PayloadFinding::ReadError(error) => {
            Diagnostic::error("catalog_payload_read_error", address, error.clone())
        }
    }
}

/// Write one content-addressed payload blob. Idempotent: an existing
/// digest-named file is trusted as-is. The digest re-check is the apply-side
/// TOCTOU detector — the source file changing between `load_desired` and the
/// payload write must fail loudly, never publish mismatched content.
fn write_resource_payload(
    target: &Path,
    source: &Path,
    expected_digest: &str,
    resource: &str,
) -> Result<(), Diagnostic> {
    if target.exists() {
        return Ok(());
    }
    let bytes = fs::read(source).map_err(|err| {
        Diagnostic::error(
            "resource_payload_write_error",
            resource,
            format!("could not read resource source '{}': {err}", source.display()),
        )
    })?;
    if sha256_hex(&bytes) != expected_digest {
        return Err(Diagnostic::error(
            "resource_content_changed",
            resource,
            format!(
                "resource source '{}' changed while apply was running; re-run `cluster apply`",
                source.display()
            ),
        ));
    }
    let parent = target.parent().expect("payload path always has a parent");
    fs::create_dir_all(parent).map_err(|err| {
        Diagnostic::error(
            "resource_payload_write_error",
            resource,
            format!("could not create payload directory: {err}"),
        )
    })?;
    let file_name = target
        .file_name()
        .expect("payload path always has a file name")
        .to_string_lossy();
    let tmp_path = parent.join(format!("{file_name}.tmp.{}", Ulid::new()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .map_err(|err| {
            Diagnostic::error(
                "resource_payload_write_error",
                resource,
                format!("could not create temporary payload file: {err}"),
            )
        })?;
    let write_result = file
        .write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|err| {
            Diagnostic::error(
                "resource_payload_write_error",
                resource,
                format!("could not write payload file: {err}"),
            )
        });
    drop(file);
    if let Err(diagnostic) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(diagnostic);
    }
    if let Err(err) = fs::rename(&tmp_path, target) {
        let _ = fs::remove_file(&tmp_path);
        return Err(Diagnostic::error(
            "resource_payload_write_error",
            resource,
            format!("could not move payload file into place: {err}"),
        ));
    }
    Ok(())
}

/// Recompute the composite `graph.<id>` digests for state-resident graphs from
/// state's own schema/query components. Without this, an applied query change
/// would leave the prior composite digest in state and `graph.<id>` would show
/// a phantom update in every later plan — apply could never converge.
fn recompute_state_graph_digests(state: &mut ClusterState, desired: &DesiredCluster) {
    for graph in &desired.graphs {
        let graph_address = graph_address(&graph.id);
        if !state.applied_revision.resources.contains_key(&graph_address) {
            continue;
        }
        let schema_digest = state
            .applied_revision
            .resources
            .get(&schema_address(&graph.id))
            .map(|resource| resource.digest.clone());
        let query_digests = state_query_digests_for_graph(state, &graph.id);
        let digest = graph_digest(&graph.id, schema_digest.as_ref(), Some(&query_digests));
        state
            .applied_revision
            .resources
            .insert(graph_address, StateResource { digest });
    }
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

fn count_errors(diagnostics: &[Diagnostic]) -> usize {
    diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
        .count()
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

    #[tokio::test]
    async fn missing_state_plans_creates() {
        let dir = fixture();
        let out = plan_config_dir(dir.path()).await;
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

    #[tokio::test]
    async fn config_digest_ignores_yaml_comments_and_formatting() {
        let dir = fixture();
        let first = plan_config_dir(dir.path()).await;
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

        let second = plan_config_dir(dir.path()).await;
        assert!(second.ok, "{:?}", second.diagnostics);
        assert_eq!(
            first.desired_revision.config_digest,
            second.desired_revision.config_digest
        );
    }

    #[tokio::test]
    async fn existing_state_plans_update_and_delete_deterministically() {
        let dir = fixture();
        let first = plan_config_dir(dir.path()).await;
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

        let out = plan_config_dir(dir.path()).await;
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

    #[tokio::test]
    async fn old_minimal_state_json_still_plans_with_default_revision() {
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

        let out = plan_config_dir(dir.path()).await;
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

    #[tokio::test]
    async fn plan_succeeds_after_force_unlock() {
        let dir = fixture();
        write_lock_file(dir.path(), "held-lock", "plan");

        let locked = plan_config_dir(dir.path()).await;
        assert!(!locked.ok);
        assert!(
            locked
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );

        let unlocked = force_unlock_config_dir(dir.path(), "held-lock");
        assert!(unlocked.ok, "{:?}", unlocked.diagnostics);

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
    }

    #[tokio::test]
    async fn plan_reports_state_cas_revision_and_removes_lock() {
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

        let out = plan_config_dir(dir.path()).await;
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

    #[tokio::test]
    async fn existing_lock_makes_plan_fail() {
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

        let out = plan_config_dir(dir.path()).await;
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

    #[tokio::test]
    async fn state_lock_false_bypasses_lock_with_warning() {
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

        let out = plan_config_dir(dir.path()).await;
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

    #[tokio::test]
    async fn external_state_backend_plan_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();
        let out = plan_config_dir(dir.path()).await;
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

        let plan = plan_config_dir(dir.path()).await;
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

        let plan = plan_config_dir(dir.path()).await;
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

    // ---- config-only apply (Stage 3A) ----

    /// Seed a state.json that simulates "graph exists with the desired schema,
    /// queries/policies not yet applied" by borrowing the desired digests.
    fn write_applyable_state(config_dir: &Path) {
        let out = validate_config_dir(config_dir);
        assert!(out.ok, "{:?}", out.diagnostics);
        let schema_digest = out.resource_digests.get("schema.knowledge").unwrap().clone();
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            config_dir,
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
            ],
        );
    }

    fn write_state_resources(config_dir: &Path, resources: &[(&str, &str)]) {
        let resource_map: serde_json::Map<String, serde_json::Value> = resources
            .iter()
            .map(|(address, digest)| ((*address).to_string(), json!({ "digest": digest })))
            .collect();
        let state_dir = config_dir.join(CLUSTER_STATE_DIR);
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            serde_json::to_string_pretty(&json!({
                "version": 1,
                "state_revision": 1,
                "applied_revision": { "resources": resource_map }
            }))
            .unwrap(),
        )
        .unwrap();
    }

    fn read_state_json(config_dir: &Path) -> serde_json::Value {
        serde_json::from_str(&fs::read_to_string(config_dir.join(CLUSTER_STATE_FILE)).unwrap())
            .unwrap()
    }

    fn query_payload_path(config_dir: &Path, digest: &str) -> std::path::PathBuf {
        config_dir
            .join(CLUSTER_RESOURCES_DIR)
            .join("query/knowledge/find_person")
            .join(format!("{digest}.gq"))
    }

    fn policy_payload_path(config_dir: &Path, digest: &str) -> std::path::PathBuf {
        config_dir
            .join(CLUSTER_RESOURCES_DIR)
            .join("policy/base")
            .join(format!("{digest}.yaml"))
    }

    #[tokio::test]
    async fn apply_without_state_fails_with_state_missing() {
        let dir = fixture();
        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_missing"
                    && diagnostic.message.contains("cluster import"))
        );
        assert!(!dir.path().join(CLUSTER_STATE_FILE).exists());
        assert!(!dir.path().join(CLUSTER_RESOURCES_DIR).exists());
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn apply_writes_payloads_state_and_statuses() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let desired = validate_config_dir(dir.path());
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap()
            .clone();
        let policy_digest = desired.resource_digests.get("policy.base").unwrap().clone();
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(out.applied_count, 2);
        assert_eq!(out.deferred_count, 0);
        assert!(out.converged);
        assert!(out.state_written);

        let query_blob = query_payload_path(dir.path(), &query_digest);
        assert_eq!(fs::read_to_string(&query_blob).unwrap(), QUERY);
        let policy_blob = policy_payload_path(dir.path(), &policy_digest);
        assert_eq!(fs::read_to_string(&policy_blob).unwrap(), "rules: []\n");

        let state = read_state_json(dir.path());
        assert_eq!(state["state_revision"], 2);
        let resources = &state["applied_revision"]["resources"];
        assert_eq!(
            resources["query.knowledge.find_person"]["digest"],
            query_digest
        );
        assert_eq!(resources["policy.base"]["digest"], policy_digest);
        let expected_composite = graph_digest(
            "knowledge",
            Some(&schema_digest),
            Some(
                &[("find_person".to_string(), query_digest.clone())]
                    .into_iter()
                    .collect(),
            ),
        );
        assert_eq!(resources["graph.knowledge"]["digest"], expected_composite);
        assert_eq!(
            state["applied_revision"]["config_digest"],
            desired_revision_digest(&out)
        );
        assert_eq!(
            state["resource_statuses"]["query.knowledge.find_person"]["status"],
            "applied"
        );
        assert_eq!(state["resource_statuses"]["policy.base"]["status"], "applied");
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    fn desired_revision_digest(out: &ApplyOutput) -> String {
        out.desired_revision.config_digest.clone().unwrap()
    }

    #[tokio::test]
    async fn apply_update_changes_query_digest_and_keeps_old_blob() {
        let dir = fixture();
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();
        let old_digest = "0".repeat(64);
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
                ("query.knowledge.find_person", old_digest.as_str()),
            ],
        );
        let old_blob = query_payload_path(dir.path(), &old_digest);
        fs::create_dir_all(old_blob.parent().unwrap()).unwrap();
        fs::write(&old_blob, "old query source").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let new_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap();
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["query.knowledge.find_person"]["digest"],
            *new_digest
        );
        assert_eq!(fs::read_to_string(&old_blob).unwrap(), "old query source");
        assert!(query_payload_path(dir.path(), new_digest).exists());
    }

    #[tokio::test]
    async fn apply_deletes_removed_resources_but_keeps_blobs() {
        let dir = fixture();
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();
        let stale_query_digest = "1".repeat(64);
        let stale_policy_digest = "2".repeat(64);
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
                ("query.knowledge.orphan", stale_query_digest.as_str()),
                ("policy.old", stale_policy_digest.as_str()),
            ],
        );
        let stale_blob = dir
            .path()
            .join(CLUSTER_RESOURCES_DIR)
            .join("policy/old")
            .join(format!("{stale_policy_digest}.yaml"));
        fs::create_dir_all(stale_blob.parent().unwrap()).unwrap();
        fs::write(&stale_blob, "old policy").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged);
        let state = read_state_json(dir.path());
        let resources = &state["applied_revision"]["resources"];
        assert!(resources.get("query.knowledge.orphan").is_none());
        assert!(resources.get("policy.old").is_none());
        assert!(
            state["resource_statuses"]
                .get("query.knowledge.orphan")
                .is_none()
        );
        // Deleted resources leave their content-addressed blobs in place; GC is
        // a later stage.
        assert_eq!(fs::read_to_string(&stale_blob).unwrap(), "old policy");
        // The composite no longer includes the orphan query.
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap()
            .clone();
        let expected_composite = graph_digest(
            "knowledge",
            Some(&schema_digest),
            Some(&[("find_person".to_string(), query_digest)].into_iter().collect()),
        );
        assert_eq!(resources["graph.knowledge"]["digest"], expected_composite);
    }

    #[tokio::test]
    async fn apply_schema_update_and_dependent_query_in_one_run() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        // Schema update + a query update that depends on the new field: one
        // apply executes the schema migration first, then the catalog write.
        fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
        fs::write(
            dir.path().join("people.gq"),
            "\nquery find_person($name: String) {\n  match { $p: Person { name: $name } }\n  return { $p.name, $p.bio }\n}\n",
        )
        .unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged, "{out:?}");
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Derived)
        );
        // The live graph carries the new schema.
        let db = Omnigraph::open_read_only(&derived_graph_uri(dir.path(), "knowledge"))
            .await
            .unwrap();
        let desired = validate_config_dir(dir.path());
        assert_eq!(
            sha256_hex(db.schema_source().as_bytes()),
            desired.resource_digests["schema.knowledge"]
        );
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            desired.resource_digests["schema.knowledge"]
        );
        // Sidecar retired after the CAS landed.
        assert!(
            !dir.path().join(CLUSTER_RECOVERIES_DIR).exists()
                || fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                    .unwrap()
                    .next()
                    .is_none()
        );
    }

    #[tokio::test]
    async fn apply_unsupported_schema_change_fails_loudly() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        // Property type changes are unsupported by the engine planner.
        fs::write(
            dir.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n  age: I64?\n}\n",
        )
        .unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "schema_apply_failed"
                && diagnostic.message.contains("changing property type")
        }));
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["schema.knowledge"].reason.as_deref(),
            Some("schema_apply_failed")
        );
        // The live schema and the ledger are unchanged.
        let state = read_state_json(dir.path());
        let desired = validate_config_dir(dir.path());
        assert_ne!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            desired.resource_digests["schema.knowledge"]
        );
        // Second run: the sweep retires the stale sidecar (ledger consistent)
        // and the run fails just as loudly — idempotent loudness.
        let second = apply_config_dir(dir.path()).await;
        assert!(!second.ok);
        assert!(
            second
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_apply_failed")
        );
    }

    #[tokio::test]
    async fn apply_blocks_schema_update_while_recovery_pending() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_state_resources(dir.path(), &[("schema.knowledge", "stale-digest")]);
        fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
        // A pending sidecar whose intent matches neither live nor recorded.
        write_schema_apply_sidecar(dir.path(), "knowledge", "intended-digest", "01PENDS");

        let out = apply_config_dir(dir.path()).await;
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["schema.knowledge"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
    }

    #[tokio::test]
    async fn apply_creates_graph_and_unblocks_dependents() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged, "{out:?}");
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Stage 4A: the create executes, and its dependents apply in-run.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["policy.base"].disposition,
            Some(ApplyDisposition::Applied)
        );
        // The graph exists on disk and opens; state records everything.
        let graph_uri = derived_graph_uri(dir.path(), "knowledge");
        let db = Omnigraph::open_read_only(&graph_uri).await.unwrap();
        let desired = validate_config_dir(dir.path());
        assert_eq!(
            sha256_hex(db.schema_source().as_bytes()),
            desired.resource_digests["schema.knowledge"]
        );
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            desired.resource_digests["schema.knowledge"]
        );
        assert_eq!(
            state["resource_statuses"]["graph.knowledge"]["status"],
            "applied"
        );
        // The create's sidecar was retired after the state CAS landed.
        assert!(
            !dir.path().join(CLUSTER_RECOVERIES_DIR).exists()
                || fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                    .unwrap()
                    .next()
                    .is_none()
        );
    }

    #[tokio::test]
    async fn apply_create_failure_blocks_dependents_and_keeps_sidecar() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);
        // Make the init fail its strict preflight: a junk _schema.pg already
        // sits at the derived root (the engine refuses to overwrite it).
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "junk").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_create_failed")
        );
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Dependents are demoted: the run tells the truth about what executed.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].reason.as_deref(),
            Some("dependency_not_applied")
        );
        assert_eq!(
            by_resource["policy.base"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert!(!out.converged);
        // The sidecar stays for the sweep to classify next run.
        assert!(
            fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                .unwrap()
                .next()
                .is_some()
        );
        // No graph digests moved.
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .as_object()
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn apply_blocks_graph_delete_without_approval() {
        let dir = fixture();
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired
            .resource_digests
            .get("schema.knowledge")
            .unwrap()
            .clone();
        let graph_composite =
            graph_digest("knowledge", Some(&schema_digest), Some(&BTreeMap::new()));
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_composite.as_str()),
                ("schema.knowledge", schema_digest.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
                ("query.old.q", "5555"),
            ],
        );

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.converged);
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Stage 4C: deletes are gated, not deferred — every subtree change
        // blocks on the single graph-level approval.
        assert_eq!(
            by_resource["graph.old"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["graph.old"].reason.as_deref(),
            Some("approval_required")
        );
        assert_eq!(
            by_resource["schema.old"].reason.as_deref(),
            Some("approval_required")
        );
        assert_eq!(
            by_resource["query.old.q"].reason.as_deref(),
            Some("approval_required")
        );
        // State intact; nothing destroyed without the artifact.
        let state = read_state_json(dir.path());
        let resources = &state["applied_revision"]["resources"];
        assert_eq!(resources["graph.old"]["digest"], "3333");
        assert_eq!(resources["schema.old"]["digest"], "4444");
        assert_eq!(resources["query.old.q"]["digest"], "5555");
    }

    #[tokio::test]
    async fn approve_writes_digest_bound_artifact() {
        let dir = fixture();
        write_applyable_state(dir.path());
        // Seed a deletable subtree.
        let state = read_state_json(dir.path());
        let graph_digest_str = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let schema_digest_str = state["applied_revision"]["resources"]["schema.knowledge"]
            ["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_digest_str.as_str()),
                ("schema.knowledge", schema_digest_str.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
            ],
        );

        let out = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let approval_id = out.approval_id.clone().unwrap();
        let artifact: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                dir.path()
                    .join(CLUSTER_APPROVALS_DIR)
                    .join(format!("{approval_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(artifact["resource"], "graph.old");
        assert_eq!(artifact["operation"], "delete");
        assert_eq!(artifact["approved_by"], "andrew");
        assert_eq!(artifact["bound_before_digest"], "3333");
        assert!(artifact["bound_after_digest"].is_null());
        assert!(artifact["bound_config_digest"].is_string());
        assert!(artifact["consumed_at"].is_null());

        // A non-gated address is refused.
        let not_gated = approve_config_dir(dir.path(), "query.knowledge.find_person", "andrew").await;
        assert!(!not_gated.ok);
        assert!(
            not_gated
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "approval_not_required")
        );
    }

    #[tokio::test]
    async fn stale_approval_is_ignored() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let state = read_state_json(dir.path());
        let graph_digest_str = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let schema_digest_str = state["applied_revision"]["resources"]["schema.knowledge"]
            ["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", graph_digest_str.as_str()),
                ("schema.knowledge", schema_digest_str.as_str()),
                ("graph.old", "3333"),
            ],
        );
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(approved.ok, "{:?}", approved.diagnostics);
        // The config moves after approval: the bound config digest no longer
        // matches and the artifact authorizes nothing.
        fs::write(dir.path().join("base.policy.yaml"), "rules: [] # moved\n").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "approval_stale"),
            "{:?}",
            out.diagnostics
        );
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(
            by_resource["graph.old"].reason.as_deref(),
            Some("approval_required")
        );
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["graph.old"]["digest"],
            "3333"
        );
    }

    #[tokio::test]
    async fn compute_approvals_one_gate_per_subtree() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let state = read_state_json(dir.path());
        let g = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let sc = state["applied_revision"]["resources"]["schema.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            dir.path(),
            &[
                ("graph.knowledge", g.as_str()),
                ("schema.knowledge", sc.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
                ("query.old.q", "5555"),
            ],
        );
        let plan = plan_config_dir(dir.path()).await;
        let gated: Vec<&str> = plan
            .approvals_required
            .iter()
            .map(|gate| gate.resource.as_str())
            .collect();
        assert_eq!(gated, vec!["graph.old"], "{plan:?}");
        assert!(!plan.approvals_required[0].satisfied);
    }

    #[tokio::test]
    async fn apply_is_idempotent() {
        let dir = fixture();
        write_applyable_state(dir.path());

        let first = apply_config_dir(dir.path()).await;
        assert!(first.ok, "{:?}", first.diagnostics);
        assert!(first.state_written);
        let state_after_first = fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap();

        let second = apply_config_dir(dir.path()).await;
        assert!(second.ok, "{:?}", second.diagnostics);
        assert!(second.changes.is_empty());
        assert_eq!(second.applied_count, 0);
        assert!(second.converged);
        assert!(!second.state_written);
        let state_after_second = fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
        assert_eq!(state_after_first, state_after_second);
        assert_eq!(second.state_observations.state_revision, 2);
    }

    #[tokio::test]
    async fn apply_respects_held_lock() {
        let dir = fixture();
        write_applyable_state(dir.path());
        write_lock_file(dir.path(), "held-lock", "plan");

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_held")
        );
        // The held lock survives a refused apply, and nothing was written.
        assert!(dir.path().join(CLUSTER_LOCK_FILE).exists());
        assert!(!dir.path().join(CLUSTER_RESOURCES_DIR).exists());
        let state = read_state_json(dir.path());
        assert_eq!(state["state_revision"], 1);
    }

    #[tokio::test]
    async fn apply_state_lock_false_bypasses_with_warning() {
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
    queries:
      find_person:
        file: ./people.gq
"#,
        )
        .unwrap();
        write_applyable_state(dir.path());

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.state_written);
        assert!(!out.state_observations.lock_acquired);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_lock_disabled")
        );
        assert!(!dir.path().join(CLUSTER_LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn apply_skips_existing_payload_blob() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let desired = validate_config_dir(dir.path());
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap()
            .clone();
        // Content-addressed blobs are trusted by name: an existing file is
        // never rewritten.
        let blob = query_payload_path(dir.path(), &query_digest);
        fs::create_dir_all(blob.parent().unwrap()).unwrap();
        fs::write(&blob, "pre-existing").unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert_eq!(fs::read_to_string(&blob).unwrap(), "pre-existing");
    }

    #[tokio::test]
    async fn apply_invalid_config_fails_before_lock() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nnot_a_field: true\n",
        )
        .unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        // Config errors bail before the lock or any state directory exists.
        assert!(!dir.path().join(CLUSTER_STATE_DIR).exists());
    }

    /// When the state write fails after payloads landed, the output must
    /// report the statuses actually on disk — not the unpersisted in-memory
    /// mutations (phantom `applied` entries would mislead automation that
    /// reads `resource_statuses` independently of `ok`).
    #[cfg(unix)]
    #[tokio::test]
    async fn apply_state_write_failure_reports_persisted_statuses() {
        use std::os::unix::fs::PermissionsExt;

        let dir = fixture();
        // lock: false so the only write into __cluster/ is state.json itself.
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
    queries:
      find_person:
        file: ./people.gq
"#,
        )
        .unwrap();
        write_applyable_state(dir.path());
        // Pre-create the payload blob so the payload phase is a no-op and the
        // failure lands exactly at the state write.
        let desired = validate_config_dir(dir.path());
        let query_digest = desired
            .resource_digests
            .get("query.knowledge.find_person")
            .unwrap();
        let blob = query_payload_path(dir.path(), query_digest);
        fs::create_dir_all(blob.parent().unwrap()).unwrap();
        fs::write(&blob, QUERY).unwrap();

        let state_dir = dir.path().join(CLUSTER_STATE_DIR);
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o555)).unwrap();
        // Running as root ignores permission bits; skip rather than flake.
        if fs::write(state_dir.join("probe"), b"x").is_ok() {
            let _ = fs::remove_file(state_dir.join("probe"));
            fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o755)).unwrap();
            eprintln!("skipping: permissions are not enforced (running as root)");
            return;
        }

        let out = apply_config_dir(dir.path()).await;
        fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o755)).unwrap();

        assert!(!out.ok);
        assert!(!out.state_written);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "state_write_error"),
            "{:?}",
            out.diagnostics
        );
        // The seeded state has no statuses; the failed apply must not invent
        // the in-memory `applied` ones it failed to persist.
        assert!(
            out.resource_statuses.is_empty(),
            "unpersisted statuses leaked into output: {:?}",
            out.resource_statuses
        );
    }

    // ---- catalog payload verification (Stage 3B) ----

    /// Converge a fixture dir and return the query blob path.
    async fn converge_fixture(config_dir: &Path) -> std::path::PathBuf {
        write_applyable_state(config_dir);
        let out = apply_config_dir(config_dir).await;
        assert!(out.ok && out.converged, "{:?}", out.diagnostics);
        let desired = validate_config_dir(config_dir);
        query_payload_path(
            config_dir,
            desired
                .resource_digests
                .get("query.knowledge.find_person")
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn status_reports_missing_payload_read_only() {
        let dir = fixture();
        let blob = converge_fixture(dir.path()).await;
        let state_before = fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap();
        fs::remove_file(&blob).unwrap();

        let out = status_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "catalog_payload_missing"
                && diagnostic.path == "query.knowledge.find_person"
        }));
        // Read-only: persisted statuses and state bytes untouched.
        assert_eq!(
            out.resource_statuses["query.knowledge.find_person"].status,
            ResourceLifecycleStatus::Applied
        );
        assert_eq!(
            fs::read_to_string(dir.path().join(CLUSTER_STATE_FILE)).unwrap(),
            state_before
        );
    }

    #[tokio::test]
    async fn refresh_removes_digest_and_drifts_on_missing_payload() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        fs::remove_file(&blob).unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "catalog_payload_missing")
        );
        let status = &out.resource_statuses["query.knowledge.find_person"];
        assert_eq!(status.status, ResourceLifecycleStatus::Drifted);
        assert!(status.conditions.contains(&"payload_missing".to_string()));
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .get("query.knowledge.find_person")
                .is_none(),
            "{state}"
        );
    }

    #[tokio::test]
    async fn refresh_drifts_on_corrupted_payload() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        fs::write(&blob, "corrupted content").unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let status = &out.resource_statuses["query.knowledge.find_person"];
        assert_eq!(status.status, ResourceLifecycleStatus::Drifted);
        assert!(status.conditions.contains(&"payload_mismatch".to_string()));
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .get("query.knowledge.find_person")
                .is_none()
        );
    }

    #[tokio::test]
    async fn refresh_flags_unreadable_payload_as_error() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        // A same-named directory yields a non-NotFound IO error portably.
        fs::remove_file(&blob).unwrap();
        fs::create_dir(&blob).unwrap();

        let out = refresh_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "catalog_payload_read_error")
        );
        let status = &out.resource_statuses["query.knowledge.find_person"];
        assert_eq!(status.status, ResourceLifecycleStatus::Error);
        assert!(status.conditions.contains(&"payload_read_error".to_string()));
        // Transient IO keeps the digest: no spurious republish.
        let state = read_state_json(dir.path());
        assert!(
            state["applied_revision"]["resources"]
                .get("query.knowledge.find_person")
                .is_some()
        );
    }

    #[tokio::test]
    async fn payload_drift_self_heals_through_refresh_plan_apply() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        let blob = converge_fixture(dir.path()).await;
        let original = fs::read_to_string(&blob).unwrap();
        fs::remove_file(&blob).unwrap();

        let refresh = refresh_config_dir(dir.path()).await;
        assert!(refresh.ok, "{:?}", refresh.diagnostics);

        let plan = plan_config_dir(dir.path()).await;
        let query_change = plan
            .changes
            .iter()
            .find(|change| change.resource == "query.knowledge.find_person")
            .expect("plan must propose recreating the query");
        assert_eq!(query_change.operation, PlanOperation::Create);
        assert_eq!(query_change.disposition, Some(ApplyDisposition::Applied));

        let apply = apply_config_dir(dir.path()).await;
        assert!(apply.ok && apply.converged, "{:?}", apply.diagnostics);
        assert_eq!(fs::read_to_string(&blob).unwrap(), original);

        let status = status_config_dir(dir.path());
        assert!(
            !status
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.starts_with("catalog_payload")),
            "{:?}",
            status.diagnostics
        );
    }

    #[test]
    fn verification_skips_graph_and_schema_resources() {
        let dir = fixture();
        write_applyable_state(dir.path()); // graph + schema digests only, no blobs

        let out = status_config_dir(dir.path());
        assert!(
            !out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code.starts_with("catalog_payload")),
            "{:?}",
            out.diagnostics
        );
    }

    // ---- recovery sidecars + sweep (Stage 4A) ----

    fn derived_graph_uri(config_dir: &Path, graph_id: &str) -> String {
        display_path(
            &config_dir
                .join(CLUSTER_GRAPHS_DIR)
                .join(format!("{graph_id}.omni")),
        )
    }

    fn write_create_sidecar(
        config_dir: &Path,
        graph_id: &str,
        desired_schema_digest: &str,
        operation_id: &str,
    ) -> PathBuf {
        let dir = config_dir.join(CLUSTER_RECOVERIES_DIR);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{operation_id}.json"));
        fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "schema_version": 1,
                "operation_id": operation_id,
                "started_at": "1970-01-01T00:00:00Z",
                "kind": "graph_create",
                "graph_id": graph_id,
                "graph_uri": derived_graph_uri(config_dir, graph_id),
                "desired_schema_digest": desired_schema_digest,
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    #[tokio::test]
    async fn sweep_removes_sidecar_when_root_absent() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let sidecar = write_create_sidecar(dir.path(), "knowledge", "irrelevant", "01ROW1");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        // Row 1: nothing moved; intent removed, run proceeds normally.
        assert!(!sidecar.exists());
        assert!(out.converged);
    }

    #[tokio::test]
    async fn sweep_rolls_forward_completed_create() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_state_resources(dir.path(), &[]); // state predates the create
        let desired = validate_config_dir(dir.path());
        let schema_digest = desired.resource_digests["schema.knowledge"].clone();
        let sidecar = write_create_sidecar(dir.path(), "knowledge", &schema_digest, "01ROW4");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
        );
        // Row 4: ledger converged to observable reality, audit recorded,
        // sidecar retired after the CAS landed.
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            schema_digest
        );
        assert!(
            state["recovery_records"]
                .as_object()
                .unwrap()
                .values()
                .any(|record| record["outcome"] == "rolled_forward"
                    && record["graph_id"] == "knowledge")
        );
        assert!(!sidecar.exists());
        // With the graph rolled forward, the same run converges the catalog.
        assert!(out.converged, "{out:?}");
    }

    #[tokio::test]
    async fn sweep_completes_already_recorded_create() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path()); // state already records graph+schema
        let desired = validate_config_dir(dir.path());
        let sidecar = write_create_sidecar(
            dir.path(),
            "knowledge",
            &desired.resource_digests["schema.knowledge"],
            "01ROW2",
        );

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        // Row 2: outcome was already durable; no audit entry, sidecar retired.
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            state["recovery_records"]
                .as_object()
                .is_none_or(|records| records.is_empty()),
            "{state}"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_sidecar_for_incomplete_root() {
        let dir = fixture();
        write_applyable_state(dir.path());
        // A root that exists but cannot be opened: the engine's partial-init gap.
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "junk").unwrap();
        let sidecar = write_create_sidecar(dir.path(), "knowledge", "whatever", "01ROW5");

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_create_incomplete")
        );
        // Row 5: never auto-delete; sidecar and root stay for the operator,
        // and the Error status is persisted by the run's state write.
        assert!(sidecar.exists());
        assert!(root.exists());
        let state = read_state_json(dir.path());
        assert_eq!(state["resource_statuses"]["graph.knowledge"]["status"], "error");
        assert!(
            state["resource_statuses"]["graph.knowledge"]["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|condition| condition == "graph_create_incomplete")
        );
    }

    #[tokio::test]
    async fn sweep_flags_unexpected_schema_as_pending() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);
        // Live graph exists with a schema the sidecar never intended.
        let graph_dir = dir.path().join(CLUSTER_GRAPHS_DIR);
        fs::create_dir_all(&graph_dir).unwrap();
        Omnigraph::init(
            &derived_graph_uri(dir.path(), "knowledge"),
            "\nnode Other {\n  name: String @key\n}\n",
        )
        .await
        .unwrap();
        let desired = validate_config_dir(dir.path());
        let sidecar = write_create_sidecar(
            dir.path(),
            "knowledge",
            &desired.resource_digests["schema.knowledge"],
            "01ROW6",
        );

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics); // warning, not error
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending")
        );
        // Row 6: refuse to guess; sidecar kept, Drifted persisted.
        assert!(sidecar.exists());
        let state = read_state_json(dir.path());
        assert_eq!(
            state["resource_statuses"]["graph.knowledge"]["status"],
            "drifted"
        );
        assert!(
            state["resource_statuses"]["graph.knowledge"]["conditions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|condition| condition == "actual_applied_state_pending")
        );
    }

    #[tokio::test]
    async fn apply_blocks_create_while_recovery_pending() {
        let dir = fixture();
        write_state_resources(dir.path(), &[]);
        // A kept (row 5) sidecar: partial root that cannot be opened.
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "junk").unwrap();
        let sidecar = write_create_sidecar(dir.path(), "knowledge", "whatever", "01PEND");

        let out = apply_config_dir(dir.path()).await;
        assert!(!out.ok); // row 5 is an error condition
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // The pending recovery blocks the create and its dependents; the
        // executor never attempts the init.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Blocked)
        );
        assert_eq!(
            by_resource["graph.knowledge"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
        assert_eq!(
            by_resource["policy.base"].reason.as_deref(),
            Some("cluster_recovery_pending")
        );
        assert!(sidecar.exists());
        // The sweep's Error status is what persists — not a generic Blocked.
        let state = read_state_json(dir.path());
        assert_eq!(state["resource_statuses"]["graph.knowledge"]["status"], "error");
    }

    #[tokio::test]
    async fn plan_embeds_migration_preview_for_schema_update() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        fs::write(
            dir.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let schema_change = out
            .changes
            .iter()
            .find(|change| change.resource == "schema.knowledge")
            .unwrap();
        let migration = schema_change.migration.as_ref().expect("preview embedded");
        assert!(migration.supported);
        assert!(
            serde_json::to_string(&migration.steps)
                .unwrap()
                .contains("add_property"),
            "{migration:?}"
        );
    }

    #[tokio::test]
    async fn plan_warns_when_preview_unavailable() {
        let dir = fixture();
        write_applyable_state(dir.path()); // digests recorded, but no live root
        fs::write(
            dir.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n",
        )
        .unwrap();

        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let schema_change = out
            .changes
            .iter()
            .find(|change| change.resource == "schema.knowledge")
            .unwrap();
        assert!(schema_change.migration.is_none());
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "schema_preview_unavailable")
        );
    }

    fn write_schema_apply_sidecar(
        config_dir: &Path,
        graph_id: &str,
        desired_schema_digest: &str,
        operation_id: &str,
    ) -> PathBuf {
        let dir = config_dir.join(CLUSTER_RECOVERIES_DIR);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{operation_id}.json"));
        fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "schema_version": 1,
                "operation_id": operation_id,
                "started_at": "1970-01-01T00:00:00Z",
                "kind": "schema_apply",
                "graph_id": graph_id,
                "graph_uri": derived_graph_uri(config_dir, graph_id),
                "desired_schema_digest": desired_schema_digest,
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    const SCHEMA_V2: &str = "\nnode Person {\n  name: String @key\n  age: I32?\n  bio: String?\n}\n";

    #[tokio::test]
    async fn sweep_retires_schema_sidecar_when_ledger_consistent() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path()); // state digest == live digest
        let sidecar =
            write_schema_apply_sidecar(dir.path(), "knowledge", "never-applied", "01SROW1");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            state["recovery_records"]
                .as_object()
                .is_none_or(|records| records.is_empty())
        );
    }

    #[tokio::test]
    async fn sweep_rolls_forward_completed_schema_apply() {
        let dir = fixture();
        init_derived_graph(dir.path()).await;
        write_applyable_state(dir.path());
        // The schema apply completed on the graph out-of-process...
        let graph_uri = derived_graph_uri(dir.path(), "knowledge");
        let db = Omnigraph::open(&graph_uri).await.unwrap();
        db.apply_schema(SCHEMA_V2).await.unwrap();
        // ...the desired config matches it, and the sidecar records the intent.
        fs::write(dir.path().join("people.pg"), SCHEMA_V2).unwrap();
        let desired = validate_config_dir(dir.path());
        let v2_digest = desired.resource_digests["schema.knowledge"].clone();
        let sidecar = write_schema_apply_sidecar(dir.path(), "knowledge", &v2_digest, "01SROW3");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
        );
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert_eq!(
            state["applied_revision"]["resources"]["schema.knowledge"]["digest"],
            v2_digest
        );
        assert!(
            state["recovery_records"]
                .as_object()
                .unwrap()
                .values()
                .any(|record| record["kind"] == "schema_apply"
                    && record["outcome"] == "rolled_forward")
        );
        assert!(out.converged, "{out:?}");
    }

    #[tokio::test]
    async fn sweep_flags_unexpected_schema_apply_state_as_pending() {
        let dir = fixture();
        init_derived_graph(dir.path()).await; // live = v1
        write_state_resources(dir.path(), &[("schema.knowledge", "stale-digest")]);
        // Sidecar intended a digest that is neither live nor recorded.
        let sidecar =
            write_schema_apply_sidecar(dir.path(), "knowledge", "intended-digest", "01SROW6");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics); // warnings only
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending")
        );
        assert!(sidecar.exists());
        let state = read_state_json(dir.path());
        assert_eq!(
            state["resource_statuses"]["schema.knowledge"]["status"],
            "drifted"
        );
    }

    #[tokio::test]
    async fn sweep_keeps_schema_sidecar_for_unopenable_root() {
        let dir = fixture();
        write_applyable_state(dir.path());
        let root = dir.path().join(CLUSTER_GRAPHS_DIR).join("knowledge.omni");
        fs::create_dir_all(&root).unwrap(); // exists, won't open
        let sidecar =
            write_schema_apply_sidecar(dir.path(), "knowledge", "whatever", "01SROWX");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics); // warning: cannot verify
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending")
        );
        assert!(sidecar.exists());
    }

    /// Seed: converged knowledge subtree + a stale `old` graph subtree with a
    /// real directory on disk.
    fn seed_deletable_state(config_dir: &Path) {
        write_applyable_state(config_dir);
        let state = read_state_json(config_dir);
        let g = state["applied_revision"]["resources"]["graph.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        let sc = state["applied_revision"]["resources"]["schema.knowledge"]["digest"]
            .as_str()
            .unwrap()
            .to_string();
        write_state_resources(
            config_dir,
            &[
                ("graph.knowledge", g.as_str()),
                ("schema.knowledge", sc.as_str()),
                ("graph.old", "3333"),
                ("schema.old", "4444"),
                ("query.old.q", "5555"),
            ],
        );
        let root = config_dir.join(CLUSTER_GRAPHS_DIR).join("old.omni");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("_schema.pg"), "stale").unwrap();
    }

    #[tokio::test]
    async fn apply_executes_approved_graph_delete() {
        let dir = fixture();
        seed_deletable_state(dir.path());
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(approved.ok, "{:?}", approved.diagnostics);
        let approval_id = approved.approval_id.clone().unwrap();

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.converged, "{out:?}");
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        assert_eq!(by_resource["graph.old"].disposition, Some(ApplyDisposition::Applied));
        assert_eq!(by_resource["schema.old"].disposition, Some(ApplyDisposition::Applied));
        assert_eq!(by_resource["query.old.q"].disposition, Some(ApplyDisposition::Applied));
        // The root is gone; the subtree is tombstoned out of the ledger.
        assert!(!dir.path().join(CLUSTER_GRAPHS_DIR).join("old.omni").exists());
        let state = read_state_json(dir.path());
        let resources = state["applied_revision"]["resources"].as_object().unwrap();
        assert!(!resources.contains_key("graph.old"));
        assert!(!resources.contains_key("schema.old"));
        assert!(!resources.contains_key("query.old.q"));
        assert_eq!(state["observations"]["graph.old"]["kind"], "tombstone");
        assert_eq!(state["observations"]["graph.old"]["approval_id"], approval_id);
        // Approval consumed in BOTH stores: ledger summary + artifact file.
        assert!(state["approval_records"][&approval_id]["consumed_at"].is_string());
        let artifact: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                dir.path()
                    .join(CLUSTER_APPROVALS_DIR)
                    .join(format!("{approval_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(artifact["consumed_at"].is_string(), "{artifact}");
        // Sidecar retired.
        assert!(
            fs::read_dir(dir.path().join(CLUSTER_RECOVERIES_DIR))
                .map(|mut entries| entries.next().is_none())
                .unwrap_or(true)
        );
        // A consumed approval authorizes nothing further (idempotent re-apply).
        let again = apply_config_dir(dir.path()).await;
        assert!(again.ok && again.converged && !again.state_written, "{again:?}");
    }

    fn write_delete_sidecar(
        config_dir: &Path,
        graph_id: &str,
        approval_id: Option<&str>,
        operation_id: &str,
    ) -> PathBuf {
        let dir = config_dir.join(CLUSTER_RECOVERIES_DIR);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{operation_id}.json"));
        fs::write(
            &path,
            serde_json::to_string_pretty(&json!({
                "schema_version": 1,
                "operation_id": operation_id,
                "started_at": "1970-01-01T00:00:00Z",
                "kind": "graph_delete",
                "graph_id": graph_id,
                "graph_uri": derived_graph_uri(config_dir, graph_id),
                "desired_schema_digest": "",
                "approval_id": approval_id,
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    #[tokio::test]
    async fn sweep_retires_delete_sidecar_when_tombstoned() {
        let dir = fixture();
        write_applyable_state(dir.path()); // no graph.old in state, no root
        let sidecar = write_delete_sidecar(dir.path(), "old", None, "01DROW7");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            state["recovery_records"]
                .as_object()
                .is_none_or(|records| records.is_empty())
        );
    }

    #[tokio::test]
    async fn sweep_rolls_forward_completed_delete() {
        let dir = fixture();
        seed_deletable_state(dir.path());
        // Approve, then simulate: root removed, state stale, sidecar present.
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        let approval_id = approved.approval_id.unwrap();
        fs::remove_dir_all(dir.path().join(CLUSTER_GRAPHS_DIR).join("old.omni")).unwrap();
        let sidecar = write_delete_sidecar(dir.path(), "old", Some(&approval_id), "01DROW7B");

        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_rolled_forward")
        );
        assert!(!sidecar.exists());
        let state = read_state_json(dir.path());
        assert!(
            !state["applied_revision"]["resources"]
                .as_object()
                .unwrap()
                .contains_key("graph.old")
        );
        assert_eq!(state["observations"]["graph.old"]["kind"], "tombstone");
        assert!(state["approval_records"][&approval_id]["consumed_at"].is_string());
        assert!(
            state["recovery_records"]
                .as_object()
                .unwrap()
                .values()
                .any(|record| record["kind"] == "graph_delete"
                    && record["outcome"] == "rolled_forward")
        );
        // The artifact file is marked consumed post-CAS.
        let artifact: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(
                dir.path()
                    .join(CLUSTER_APPROVALS_DIR)
                    .join(format!("{approval_id}.json")),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(artifact["consumed_at"].is_string());
        assert!(out.converged, "{out:?}");
    }

    #[tokio::test]
    async fn sweep_reproposes_incomplete_delete() {
        let dir = fixture();
        seed_deletable_state(dir.path()); // root present
        let approved = approve_config_dir(dir.path(), "graph.old", "andrew").await;
        assert!(approved.ok);
        let sidecar = write_delete_sidecar(dir.path(), "old", approved.approval_id.as_deref(), "01DROW8");

        // Row 8: the stale intent is retired with a warning, and the same run
        // re-executes the still-approved delete to completion.
        let out = apply_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "graph_delete_incomplete")
        );
        assert!(!sidecar.exists());
        assert!(!dir.path().join(CLUSTER_GRAPHS_DIR).join("old.omni").exists());
        assert!(out.converged, "{out:?}");
    }

    #[test]
    fn status_warns_on_pending_recovery_sidecar() {
        let dir = fixture();
        write_applyable_state(dir.path());
        write_create_sidecar(dir.path(), "knowledge", "irrelevant", "01STATUS");

        let out = status_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "cluster_recovery_pending"
                    && diagnostic.severity == DiagnosticSeverity::Warning)
        );
    }

    #[tokio::test]
    async fn plan_annotates_apply_dispositions() {
        let dir = fixture();
        let out = plan_config_dir(dir.path()).await;
        assert!(out.ok, "{:?}", out.diagnostics);
        let by_resource: BTreeMap<&str, &PlanChange> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), change))
            .collect();
        // Stage 4A: graph/schema creates are executable, and dependents ride
        // the same run — plan previews exactly that.
        assert_eq!(
            by_resource["graph.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["schema.knowledge"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["query.knowledge.find_person"].disposition,
            Some(ApplyDisposition::Applied)
        );
        assert_eq!(
            by_resource["policy.base"].disposition,
            Some(ApplyDisposition::Applied)
        );
    }
}
