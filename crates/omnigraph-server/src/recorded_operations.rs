//! Recorded operations: an optimize run through the serving owner, kept in
//! the cluster's operation record.
//!
//! A caller submits an optimize under its own operation id. Admission and
//! every change of the operation's state go through the cluster's record,
//! `__cluster/operations.json`, which this process, the lock holder, writes
//! with one conditional put each time: at acceptance, when the run starts,
//! before its publication and at its end. The run is an owned server
//! operation and holds its graph request, so a deployment that affects the
//! graph cancels it before draining it, and shutdown cancels it too.
//!
//! The record has one writer in this process: every read a write follows and
//! every write hold the cluster admission's record gate
//! (`ClusterAdmission::lock_operations`), the deletion's settlement included,
//! and the live runs this process holds change under that same gate, so a
//! lookup never sees a run that has ended but whose record is still active.
//! Each admission scope also has one in-process gate, a mutex taken with a
//! try-lock by the handler and held by the owned operation until it drops. The
//! acceptance put is the operation's first step, not the handler's, under the
//! record gate the handler still holds: two submissions cannot both read an
//! empty slot and both start, and a client disconnect cannot separate a
//! durable acceptance from execution.
//!
//! A write whose answer is lost stops the run's writes. The next admission or
//! lookup re-applies the lost change to the record as it then stands, settles
//! an `accepted` or `running` operation no live run holds as `interrupted`,
//! and advances a `publishing` one by one page of its settlement branch's
//! history, a walk that runs outside the gate and is applied only to a record
//! whose cursor is still the one it started from.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as SyncMutex, PoisonError};
use std::time::Duration;

use axum::{Extension, Json, extract::State, http::StatusCode};
use omnigraph::db::{
    ArtifactRefresh, AuthorityCapture, ConfigurationClear, OptimizeBeforePublish,
    OptimizeCancellation, OptimizeConcurrency, OptimizeLimits, OptimizeReport, PublicationLookup,
    RunCancellation, TableCeilings, TableOptimizeOutcome,
};
use omnigraph_cluster::{
    AdmissionRefusal, AdmissionScope, Admitted, ClusterAdmission, Operation, OperationKind,
    OperationRecord, OperationRequest, OperationState, OperationWrite, OperationsLock,
    OperationsUpdate, OptimizeOptions, Resolution, ResolutionProgress, TableOutcome, TailOutcome,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, oneshot};
use tokio::time::Instant;
use tracing::Instrument;
use utoipa::ToSchema;

use crate::extract::{ApiJson, ApiPath};
use crate::operations::OwnedResult;
use crate::serving::GraphRequest;
use crate::workload::IngressLease;
use crate::{ApiError, AppState, AuthenticatedActor, ErrorCode, PolicyAction, PolicyRequest};

type Actor = Option<Extension<AuthenticatedActor>>;

/// Operator settings for the recorded-operation routes and the optimize kind,
/// read once at startup. Every ceiling is the server's: a request names only
/// its tables and how long writers wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationSettings {
    /// The routes answer 503 unless an operator enables them.
    pub enabled: bool,
    /// The gate hold of a request that names none.
    pub default_gate_hold: Duration,
    /// The largest gate hold a request may ask for.
    pub max_gate_hold: Duration,
    /// The adjacency artifact bound every served run applies.
    pub artifact_write_limit: u64,
    /// The per-table ceilings every served run applies.
    pub table_ceilings: TableCeilings,
    /// The run byte budget, when set.
    pub run_source_bytes: Option<u64>,
    /// How many tables a served run plans ahead of the one it rewrites.
    pub planning_concurrency: usize,
    /// Commits one resolution attempt reads of a settlement branch's chain.
    pub resolution_walk_limit: usize,
}

impl Default for OperationSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            default_gate_hold: Duration::from_secs(60),
            max_gate_hold: Duration::from_secs(300),
            artifact_write_limit: 512 * 1024 * 1024,
            table_ceilings: TableCeilings::default(),
            run_source_bytes: None,
            planning_concurrency: 8,
            resolution_walk_limit: 256,
        }
    }
}

impl OperationSettings {
    /// `OMNIGRAPH_OPERATION_ROUTES` (`true` or `false`),
    /// `OMNIGRAPH_OPERATION_RESOLUTION_WALK_LIMIT`,
    /// `OMNIGRAPH_OPTIMIZE_GATE_HOLD_SECONDS`,
    /// `OMNIGRAPH_OPTIMIZE_MAX_GATE_HOLD_SECONDS`,
    /// `OMNIGRAPH_OPTIMIZE_ARTIFACT_WRITE_LIMIT_BYTES`,
    /// `OMNIGRAPH_OPTIMIZE_PLANNING_CONCURRENCY`, the per-table ceilings
    /// `OMNIGRAPH_OPTIMIZE_MAX_SOURCE_FRAGMENTS`, `_MAX_SOURCE_ROWS`,
    /// `_MAX_SOURCE_BYTES`, `_MAX_BLOB_BYTES` and `_MAX_INDEX_ROWS`, and the
    /// run budget `OMNIGRAPH_OPTIMIZE_RUN_SOURCE_BYTES`, over the defaults.
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut settings = Self::default();
        if let Some(value) = lookup("OMNIGRAPH_OPERATION_ROUTES") {
            settings.enabled = match value.as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                other => {
                    return Err(format!(
                        "OMNIGRAPH_OPERATION_ROUTES must be true or false, not '{other}'"
                    ));
                }
            };
        }
        let number = |name: &str| -> Result<Option<u64>, String> {
            lookup(name)
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map_err(|_| format!("{name} must be a whole number, not '{value}'"))
                })
                .transpose()
        };
        if let Some(seconds) = number("OMNIGRAPH_OPTIMIZE_GATE_HOLD_SECONDS")? {
            settings.default_gate_hold = Duration::from_secs(seconds);
        }
        if let Some(seconds) = number("OMNIGRAPH_OPTIMIZE_MAX_GATE_HOLD_SECONDS")? {
            settings.max_gate_hold = Duration::from_secs(seconds);
        }
        if let Some(bytes) = number("OMNIGRAPH_OPTIMIZE_ARTIFACT_WRITE_LIMIT_BYTES")? {
            settings.artifact_write_limit = bytes;
        }
        if let Some(count) = number("OMNIGRAPH_OPTIMIZE_PLANNING_CONCURRENCY")? {
            if count == 0 {
                return Err(
                    "OMNIGRAPH_OPTIMIZE_PLANNING_CONCURRENCY must be at least 1".to_string()
                );
            }
            settings.planning_concurrency = count as usize;
        }
        if let Some(count) = number("OMNIGRAPH_OPERATION_RESOLUTION_WALK_LIMIT")? {
            if count == 0 {
                return Err(
                    "OMNIGRAPH_OPERATION_RESOLUTION_WALK_LIMIT must be at least 1".to_string(),
                );
            }
            settings.resolution_walk_limit = count as usize;
        }
        let mut ceilings = TableCeilings::default();
        if let Some(value) = number("OMNIGRAPH_OPTIMIZE_MAX_SOURCE_FRAGMENTS")? {
            ceilings = ceilings.with_source_fragments(value);
        }
        if let Some(value) = number("OMNIGRAPH_OPTIMIZE_MAX_SOURCE_ROWS")? {
            ceilings = ceilings.with_source_rows(value);
        }
        if let Some(value) = number("OMNIGRAPH_OPTIMIZE_MAX_SOURCE_BYTES")? {
            ceilings = ceilings.with_source_bytes(value);
        }
        if let Some(value) = number("OMNIGRAPH_OPTIMIZE_MAX_BLOB_BYTES")? {
            ceilings = ceilings.with_blob_bytes(value);
        }
        if let Some(value) = number("OMNIGRAPH_OPTIMIZE_MAX_INDEX_ROWS")? {
            ceilings = ceilings.with_index_rows(value);
        }
        settings.table_ceilings = ceilings;
        settings.run_source_bytes = number("OMNIGRAPH_OPTIMIZE_RUN_SOURCE_BYTES")?;
        if settings.default_gate_hold > settings.max_gate_hold {
            return Err("the default optimize gate hold exceeds its ceiling".to_string());
        }
        if let (Some(table), Some(budget)) = (
            settings.table_ceilings.source_bytes,
            settings.run_source_bytes,
        ) && table > budget
        {
            return Err(
                "OMNIGRAPH_OPTIMIZE_MAX_SOURCE_BYTES exceeds OMNIGRAPH_OPTIMIZE_RUN_SOURCE_BYTES"
                    .to_string(),
            );
        }
        Ok(settings)
    }

    fn limits(&self, options: &OptimizeOptions) -> OptimizeLimits {
        let limits = OptimizeLimits::default()
            .with_max_gate_hold(
                options
                    .max_gate_hold_ms
                    .map_or(self.default_gate_hold, Duration::from_millis),
            )
            .with_artifact_bytes(self.artifact_write_limit)
            .with_table_ceilings(self.table_ceilings);
        match self.run_source_bytes {
            Some(budget) => limits.with_run_source_bytes(budget),
            None => limits,
        }
    }
}

/// A run this process holds, and how to stop it.
struct LiveRun {
    operation_id: String,
    graph: String,
    cancellation: OptimizeCancellation,
    /// Why a cancellation was requested, kept as the terminal reason.
    cancel_reason: Option<String>,
}

/// A run's final record change that no write has applied yet. The outcome is
/// known here, so the change is applied again to the record as it stands at
/// the next access, never as an old snapshot over a newer record, and kept
/// until a write applies it or the record no longer holds the run active.
enum PendingEnding {
    Finish(Box<Operation>),
    Defer {
        operation_id: String,
        reason: String,
    },
}

pub(crate) struct RecordedOperations {
    settings: OperationSettings,
    pending: SyncMutex<BTreeMap<String, PendingEnding>>,
    /// One admission gate per scope, held by the owned operation.
    gates: BTreeMap<AdmissionScope, Arc<Mutex<()>>>,
    /// The runs this process holds; changed only under the record gate.
    live: SyncMutex<Vec<LiveRun>>,
}

impl Default for RecordedOperations {
    fn default() -> Self {
        Self::new(OperationSettings::default())
    }
}

impl RecordedOperations {
    pub(crate) fn new(settings: OperationSettings) -> Self {
        Self {
            settings,
            pending: SyncMutex::default(),
            gates: BTreeMap::from([(AdmissionScope::Maintenance, Arc::default())]),
            live: SyncMutex::default(),
        }
    }

    fn live_ids(&self) -> Vec<String> {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|run| run.operation_id.clone())
            .collect()
    }

    fn set_live(&self, run: LiveRun) {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(run);
    }

    fn clear_live(&self, operation_id: &str) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        live.retain(|run| run.operation_id != operation_id);
    }

    fn cancel_reason(&self, operation_id: &str) -> Option<String> {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|run| run.operation_id == operation_id)
            .and_then(|run| run.cancel_reason.clone())
    }

    /// [`Self::cancel`] under the record gate, so a submission mid-admission
    /// is either not yet accepted or already live and told.
    pub(crate) fn cancel_under(
        &self,
        _lock: &OperationsLock,
        operation_id: Option<&str>,
        graphs: Option<&[String]>,
        reason: &str,
    ) -> bool {
        self.cancel(operation_id, graphs, reason)
    }

    /// Cancel the live run `operation_id` (every live run when `None`) on one
    /// of `graphs` (any graph when `None`), recording `reason`. Returns
    /// whether a run was told.
    pub(crate) fn cancel(
        &self,
        operation_id: Option<&str>,
        graphs: Option<&[String]>,
        reason: &str,
    ) -> bool {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        let mut told = false;
        for run in live.iter_mut() {
            if operation_id.is_some_and(|id| id != run.operation_id)
                || graphs.is_some_and(|graphs| !graphs.contains(&run.graph))
            {
                continue;
            }
            run.cancel_reason.get_or_insert_with(|| reason.to_string());
            run.cancellation.cancel();
            told = true;
        }
        told
    }

    fn require_enabled(&self) -> Result<(), ApiError> {
        if self.settings.enabled {
            Ok(())
        } else {
            Err(ApiError::new(
                ErrorCode::ServiceUnavailable,
                "the recorded-operation routes are not enabled on this server",
            ))
        }
    }

    fn gate(&self, scope: AdmissionScope) -> Arc<Mutex<()>> {
        Arc::clone(self.gates.get(&scope).expect("every scope has a gate"))
    }
}

/// `POST /graphs/{graph_id}/operations/optimize`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartOptimizeRequest {
    /// A canonical ULID the caller chose; a repeated submission with the same
    /// request answers the existing record.
    operation_id: String,
    /// Only these table keys (`node:<Type>`, `edge:<Type>`), in this order.
    #[serde(default)]
    tables: Option<Vec<String>>,
    /// How long writers to main wait at most, within the server's ceiling.
    #[serde(default)]
    max_gate_hold_ms: Option<u64>,
}

impl StartOptimizeRequest {
    fn request(&self) -> OperationRequest {
        OperationRequest::Optimize(OptimizeOptions {
            tables: self.tables.clone(),
            max_gate_hold_ms: self.max_gate_hold_ms,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationLookup {
    Found,
    /// The record does not hold this id for this graph. It proves neither
    /// that the operation never ran nor that it may be replayed.
    Unknown,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct OperationOutput {
    lookup: OperationLookup,
    /// The operation's record; absent for an `unknown` lookup.
    #[schema(value_type = Option<Object>)]
    operation: Option<Operation>,
    /// Whether this server is running it now.
    in_progress: bool,
}

#[derive(Deserialize)]
pub(crate) struct OperationPath {
    operation_id: String,
}

fn authenticated(actor: Actor) -> Result<AuthenticatedActor, ApiError> {
    actor.map(|Extension(actor)| actor).ok_or_else(|| {
        ApiError::forbidden(
            "recorded operations require an authenticated actor; this server resolves none",
        )
    })
}

/// The grant a kind requires, on its start route and on lookup and cancel of
/// its records: `maintain` on the settlement branch for `optimize`.
fn authorize(
    handle: &GraphRequest,
    actor: &AuthenticatedActor,
    kind: OperationKind,
) -> Result<(), ApiError> {
    let (action, branch) = match kind {
        OperationKind::Optimize => (PolicyAction::Maintain, kind.settlement_branch()),
    };
    crate::handlers::authorize_request(
        Some(actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action,
            branch: Some(branch.to_string()),
            target_branch: None,
        },
    )
}

fn admission(state: &AppState) -> Result<ClusterAdmission, ApiError> {
    state.cluster_admission.clone().ok_or_else(|| {
        ApiError::conflict("a recorded operation requires a v2 cluster with serving admission")
    })
}

fn refusal(error: omnigraph_cluster::Diagnostic) -> ApiError {
    ApiError::conflict(format!("[{}] {}", error.code, error.message))
}

fn outcome_unknown(operation_id: &str, error: &str) -> ApiError {
    ApiError::new(
        ErrorCode::OutcomeUnknown,
        format!(
            "the operation record write for {operation_id} has no definite answer ({error}); look the operation up by its id before submitting another"
        ),
    )
}

fn request_mismatch(operation_id: &str) -> ApiError {
    ApiError::conflict(format!(
        "[operation_request_mismatch] operation id {operation_id} names a different request"
    ))
}

/// One page of the settlement branch's history for each unresolved
/// publication, read outside the record gate from the record as it stands.
/// Each answer carries the resolution progress it started from, so the gate
/// applies it only to a record that has not moved on since.
async fn walk_unresolved(
    state: &AppState,
    owner: &ClusterAdmission,
) -> Vec<(String, Option<ResolutionProgress>, Resolution)> {
    let Ok(Some((record, _))) = owner.peek_operations().await else {
        return Vec::new();
    };
    let limit = state.recorded.settings.resolution_walk_limit;
    let mut walks = Vec::new();
    for operation in &record.unresolved {
        let (Some(nonce), Some(version)) = (&operation.nonce, operation.input_manifest_version)
        else {
            continue;
        };
        let Some(engine) = state
            .routing
            .registry
            .list()
            .into_iter()
            .find(|view| view.key.graph_id.as_str() == operation.graph)
            .map(|view| Arc::clone(&view.engine))
        else {
            continue;
        };
        let covered = operation
            .resolution
            .as_ref()
            .map_or(0, |progress| progress.visited);
        let cursor = operation
            .resolution
            .as_ref()
            .and_then(|progress| progress.cursor.clone());
        let resolution = match engine
            .resolve_publication(
                Some(&operation.settlement_branch),
                nonce,
                operation.input_commit.as_deref(),
                version,
                cursor.as_deref(),
                limit,
            )
            .await
        {
            Ok(PublicationLookup::Committed { commit_id }) => Resolution::Committed {
                published_commit: commit_id,
            },
            Ok(PublicationLookup::NotCommitted) => Resolution::NotCommitted,
            Ok(PublicationLookup::HeadUnchanged) => Resolution::InDoubt {
                reason: format!(
                    "the graph head of {} is still the input head",
                    operation.settlement_branch
                ),
            },
            Ok(PublicationLookup::Walking { next, visited }) => Resolution::Walking {
                cursor: Some(next),
                visited: covered + visited,
            },
            Ok(PublicationLookup::Unproven { reason }) => Resolution::InDoubt { reason },
            Ok(other) => Resolution::InDoubt {
                reason: format!("{other:?}"),
            },
            Err(error) => Resolution::InDoubt {
                reason: format!(
                    "reading the history of {}: {error}",
                    operation.settlement_branch
                ),
            },
        };
        walks.push((
            operation.operation_id.clone(),
            operation.resolution.clone(),
            resolution,
        ));
    }
    walks
}

fn record_refusal(error: omnigraph_cluster::Diagnostic) -> ApiError {
    match error.code.as_str() {
        "operation_record_outcome_unknown" => ApiError::new(
            ErrorCode::OutcomeUnknown,
            format!(
                "{}; look the operation up by its id before submitting another",
                error.message
            ),
        ),
        // Nothing applied: the store or this process's storage scope refused
        // the request before sending it. A scope keeps refusing a key whose
        // earlier request is unsettled until the server is replaced.
        "operation_record_refused" => ApiError::new(
            ErrorCode::ServiceUnavailable,
            format!(
                "{}; nothing was accepted, and a refusal that names an unsettled earlier request holds until the server restarts",
                error.message
            ),
        ),
        _ => refusal(error),
    }
}

/// Every record change this process makes, under the gate: the pending endings
/// applied (and dropped once applied or superseded), orphans settled, the
/// walks applied where the record still matches, then `change`. The record
/// comes back as written; a failed write keeps every pending ending.
async fn update<T>(
    state: &AppState,
    owner: &ClusterAdmission,
    lock: &OperationsLock,
    walks: &[(String, Option<ResolutionProgress>, Resolution)],
    change: impl FnOnce(&mut OperationRecord) -> (bool, T),
) -> Result<OperationsUpdate<T>, ApiError> {
    let pending = std::mem::take(
        &mut *state
            .recorded
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    let live = state.recorded.live_ids();
    let outcome = owner
        .update_operations(lock, |record| {
            let mut changed = false;
            for (operation_id, ending) in &pending {
                // Applied while the run is still active; superseded, and so
                // dropped, once the record settled it another way.
                changed |= record.active_mut(operation_id).is_some()
                    && match ending {
                        PendingEnding::Finish(operation) => {
                            record.finish((**operation).clone()).is_ok()
                        }
                        PendingEnding::Defer {
                            operation_id,
                            reason,
                        } => record
                            .defer_publication(operation_id, reason.clone())
                            .is_ok(),
                    };
            }
            changed |= record.interrupt_orphans(&live);
            for (operation_id, started_at, resolution) in walks {
                if record
                    .unresolved
                    .iter()
                    .any(|op| op.operation_id == *operation_id && op.resolution == *started_at)
                {
                    changed |= record.settle(operation_id, resolution.clone());
                }
            }
            let (more, value) = change(record);
            Ok((changed || more, value))
        })
        .await;
    match outcome {
        Ok(update) => Ok(update),
        Err(error) => {
            // Whatever failed, the endings are still known and still owed.
            state
                .recorded
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(pending);
            Err(record_refusal(error))
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn output(state: &AppState, operation: Option<Operation>) -> (StatusCode, Json<OperationOutput>) {
    let live = state.recorded.live_ids();
    let in_progress = operation
        .as_ref()
        .is_some_and(|op| live.iter().any(|id| id == &op.operation_id));
    let status = match &operation {
        Some(op) if !op.state.is_terminal() => StatusCode::ACCEPTED,
        _ => StatusCode::OK,
    };
    (
        status,
        Json(OperationOutput {
            lookup: if operation.is_some() {
                OperationLookup::Found
            } else {
                OperationLookup::Unknown
            },
            operation,
            in_progress,
        }),
    )
}

/// The ceiling a new request must respect; a repeat of a retained id is
/// answered before this, so lowering the ceiling never hides a record.
fn validate_new(
    settings: &OperationSettings,
    input: &StartOptimizeRequest,
) -> Result<(), ApiError> {
    if let Some(hold) = input.max_gate_hold_ms
        && Duration::from_millis(hold) > settings.max_gate_hold
    {
        return Err(ApiError::bad_request(format!(
            "max_gate_hold_ms {hold} exceeds this server's ceiling of {} ms",
            settings.max_gate_hold.as_millis()
        )));
    }
    Ok(())
}

/// What the owned operation answers the handler once its acceptance put is
/// settled.
type Acceptance = Result<Operation, ApiError>;

#[utoipa::path(
    post, path = "/operations/optimize", tag = "operations",
    request_body = StartOptimizeRequest,
    responses(
        (status = 200, description = "The operation is terminal", body = OperationOutput),
        (status = 202, description = "Durably accepted, or still running; look it up by its id until it is terminal", body = OperationOutput),
        (status = 400, description = "A non-canonical operation_id, or a max_gate_hold_ms above this server's ceiling", body = crate::api::ErrorOutput),
        (status = 403, body = crate::api::ErrorOutput),
        (status = 409, description = "The same id with a different request (`[operation_request_mismatch]`), an id at or before the eviction mark (`[operation_id_expired]`), an outstanding deployment, four unresolved publications, or a record from another ledger (`[operation_record_foreign]`)", body = crate::api::ErrorOutput),
        (status = 429, description = "Another operation is active in the maintenance scope, or that scope's admission gate is held across an acceptance; honor `Retry-After`", body = crate::api::ErrorOutput),
        (status = 500, description = "Internal failure (code `internal`), or a record write with no definite answer (code `outcome_unknown`); look the operation up by its id", body = crate::api::ErrorOutput),
        (status = 503, description = "The routes are not enabled, admission is closed, or the operation record write was refused and nothing was accepted (a refusal naming an unsettled earlier request holds until the server restarts)", body = crate::api::ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Submit a recorded optimize run on the graph's main branch.
pub(crate) async fn start_optimize(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Actor,
    request: Result<ApiJson<StartOptimizeRequest>, ApiError>,
) -> Result<(StatusCode, Json<OperationOutput>), ApiError> {
    let actor = authenticated(actor)?;
    authorize(&handle, &actor, OperationKind::Optimize)?;
    state.recorded.require_enabled()?;
    let ApiJson(input) = request?;
    if !omnigraph_cluster::canonical_operation_id(&input.operation_id) {
        return Err(ApiError::bad_request(
            "operation_id must be a canonical ULID",
        ));
    }
    let owner = admission(&state)?;
    let graph = handle.key.graph_id.to_string();
    let principal = actor.actor_id.to_string();
    let kind = OperationKind::Optimize;
    let schema_identity_domain = handle
        .engine
        .schema_contract_digest()
        .schema_identity_domain;

    let walks = walk_unresolved(&state, &owner).await;
    let lock = owner.lock_operations().await;
    let loaded = update(&state, &owner, &lock, &walks, |_| (false, ())).await?;
    let OperationsUpdate {
        record: mut admitted,
        version,
        outstanding_deployment,
        ..
    } = loaded;
    let start_at = if input.tables.is_none() {
        admitted
            .kind_state
            .optimize
            .get(&graph)
            .map(|state| state.resume_at.clone())
    } else {
        None
    };
    let operation = Operation::accepted(
        input.operation_id.clone(),
        graph.clone(),
        schema_identity_domain,
        input.request(),
        principal,
        start_at,
    );
    // A retained id answers first, whatever the ceiling, the mark or the
    // slot says: the same request returns the record, another is a mismatch.
    if let Some(existing) = admitted.find(&graph, &input.operation_id) {
        return if existing.request_digest == operation.request_digest {
            Ok(output(&state, Some(existing.clone())))
        } else {
            Err(request_mismatch(&input.operation_id))
        };
    }
    validate_new(&state.recorded.settings, &input)?;
    if let Some(deployment) = &outstanding_deployment {
        return Err(ApiError::conflict(format!(
            "deployment {deployment} is outstanding; submit the operation after it completes"
        )));
    }
    match admitted.admit(operation.clone(), now_ms()) {
        Ok(Admitted::New) => {}
        Ok(Admitted::Existing(existing)) => return Ok(output(&state, Some(*existing))),
        Err(AdmissionRefusal::DifferentRequest) => {
            return Err(request_mismatch(&input.operation_id));
        }
        Err(AdmissionRefusal::Active {
            scope,
            operation_id,
        }) => {
            return Err(ApiError::too_many_requests(format!(
                "operation {operation_id} is active in the {} scope of this cluster; retry after it finishes",
                scope.as_str()
            )));
        }
        Err(AdmissionRefusal::Unresolved) => {
            return Err(ApiError::conflict(
                "four publications are unresolved; a write on their settlement branches settles them",
            ));
        }
        Err(AdmissionRefusal::Expired { eviction_mark_ms }) => {
            return Err(ApiError::conflict(format!(
                "[operation_id_expired] operation id {} is at or before the record's eviction mark ({eviction_mark_ms} ms); mint a new id",
                input.operation_id
            )));
        }
        Err(AdmissionRefusal::FutureDated { latest_ms }) => {
            return Err(ApiError::bad_request(format!(
                "operation id {} is dated after this server's clock allows ({latest_ms} ms); mint an id from the current time",
                input.operation_id
            )));
        }
    }
    // The scope's gate: the active slot was free, so only a submission whose
    // acceptance is in flight holds it, and the record gate orders that.
    let gate = state
        .recorded
        .gate(kind.scope())
        .try_lock_owned()
        .map_err(|_| {
            ApiError::too_many_requests(format!(
                "an operation is being admitted in the {} scope; retry",
                kind.scope().as_str()
            ))
        })?;

    let accepted_at = Instant::now();
    let (accepted, acceptance) = oneshot::channel::<Acceptance>();
    let run_state = state.clone();
    let run_handle = handle.clone();
    let span = tracing::info_span!(
        "recorded_operation",
        operation_id = %input.operation_id,
        kind = kind.as_str(),
        graph = %operation.graph,
    );
    let operation_id = input.operation_id.clone();
    let submitted = state.operations.submit(
        (ingress, handle, gate),
        async move {
            // The acceptance put is the operation's first step, under the
            // record gate the handler took; the run is live before the gate
            // is released, so no lookup can see an accepted record no run holds.
            // A deployment closes the graph's admission before it cancels the
            // live runs under this gate, so a graph no longer ready here is
            // being drained: accept nothing rather than start a run its
            // cancellation has already passed.
            let still_admitted = matches!(
                run_state.routing.registry.get(&run_handle.key),
                crate::registry::RegistryLookup::Ready(_)
            );
            if !still_admitted {
                drop(lock);
                let _ = accepted.send(Err(ApiError::new(
                    ErrorCode::ServiceUnavailable,
                    format!(
                        "graph {} is closing for a deployment; nothing was accepted",
                        operation.graph
                    ),
                )));
                return OwnedResult::from(Ok(()));
            }
            let written = owner
                .write_operations(&lock, &admitted, version.as_deref())
                .await;
            let answer = match written {
                Ok(OperationWrite::Written { .. }) => Ok(operation.clone()),
                Ok(OperationWrite::Conflict) => Err(ApiError::conflict(
                    "the operation record changed during admission; retry",
                )),
                Ok(OperationWrite::Refused { error }) => Err(ApiError::new(
                    ErrorCode::ServiceUnavailable,
                    format!(
                        "the operation record write was refused and nothing was accepted: {error}"
                    ),
                )),
                Ok(OperationWrite::InDoubt { error }) => {
                    Err(outcome_unknown(&operation_id, &error))
                }
                Err(error) => Err(refusal(error)),
            };
            let cancellation = OptimizeCancellation::new();
            let landed = answer.is_ok();
            if landed {
                run_state.recorded.set_live(LiveRun {
                    operation_id: operation_id.clone(),
                    graph: operation.graph.clone(),
                    cancellation: cancellation.clone(),
                    cancel_reason: None,
                });
            }
            drop(lock);
            let _ = accepted.send(answer);
            if !landed {
                return OwnedResult::from(Ok(()));
            }
            run(
                run_state,
                owner,
                run_handle,
                operation,
                cancellation,
                accepted_at,
            )
            .await
        }
        .instrument(span),
    )?;
    let result = submitted.result();
    tokio::pin!(result);
    tokio::select! {
        biased;
        accepted = acceptance => match accepted {
            Ok(Ok(operation)) => Ok(output(&state, Some(operation))),
            Ok(Err(error)) => Err(error),
            // The operation ended before it answered: read what it left.
            Err(_) => {
                result.await?;
                let owner = admission(&state)?;
                Ok(output(&state, owner_record(&owner, &input.operation_id).await))
            }
        },
        result = &mut result => {
            result?;
            let owner = admission(&state)?;
            Ok(output(&state, owner_record(&owner, &input.operation_id).await))
        }
    }
}

/// The operation as the record holds it, read for a response.
async fn owner_record(owner: &ClusterAdmission, operation_id: &str) -> Option<Operation> {
    let (record, _) = owner.peek_operations().await.ok()??;
    record
        .operations()
        .find(|op| op.operation_id == operation_id)
        .cloned()
}

/// Apply `change` to the active operation and write the record once.
async fn transition(
    state: &AppState,
    owner: &ClusterAdmission,
    operation_id: &str,
    change: impl FnOnce(&mut Operation),
) -> Result<(), String> {
    let lock = owner.lock_operations().await;
    let updated = update(state, owner, &lock, &[], |record| {
        match record.active_mut(operation_id) {
            Some(active) => {
                change(active);
                (true, true)
            }
            None => (false, false),
        }
    })
    .await
    .map_err(|error| error.message.to_string())?;
    if updated.value {
        Ok(())
    } else {
        Err(format!(
            "operation {operation_id} no longer holds its scope's active slot"
        ))
    }
}

/// How the run ends in the record.
enum Ending {
    Terminal(Box<Operation>),
    /// The publication may have landed: the settlement branch's history
    /// settles it.
    Unresolved(String),
    /// A record write was in doubt: write nothing more; a later read settles.
    Silent,
}

async fn run(
    state: AppState,
    owner: ClusterAdmission,
    handle: GraphRequest,
    mut operation: Operation,
    cancellation: OptimizeCancellation,
    accepted_at: Instant,
) -> OwnedResult<()> {
    let started = Instant::now();
    let id = operation.operation_id.clone();

    // `running` in doubt: no table work starts, and the next admission or
    // lookup records the run `interrupted`.
    if let Err(error) = transition(&state, &owner, &id, |active| {
        active.state = OperationState::Running;
    })
    .await
    {
        tracing::warn!(operation_id = %id, %error, "recorded operation did not start");
        state.recorded.clear_live(&id);
        return Ok(()).into();
    }

    // Shutdown and containment close admission; the run stops at its next
    // safe boundary.
    let watch = {
        let operations = state.operations.clone();
        let recorded = Arc::clone(&state.recorded);
        let id = id.clone();
        tokio::spawn(async move {
            operations.wait_closed().await;
            recorded.cancel(Some(&id), None, "the server is shutting down");
        })
    };

    let publishing_landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let publishing_in_doubt = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook = {
        let state = state.clone();
        let owner = owner.clone();
        let id = id.clone();
        let landed = Arc::clone(&publishing_landed);
        let in_doubt = Arc::clone(&publishing_in_doubt);
        OptimizeBeforePublish::new(move |publication| {
            let state = state.clone();
            let owner = owner.clone();
            let id = id.clone();
            let landed = Arc::clone(&landed);
            let in_doubt = Arc::clone(&in_doubt);
            async move {
                let tables = publication.tables.iter().map(table_outcome).collect();
                match transition(&state, &owner, &id, |active| {
                    active.state = OperationState::Publishing;
                    active.nonce = Some(publication.nonce.clone());
                    active.input_commit = publication.input_commit.clone();
                    active.input_manifest_version = Some(publication.input_manifest_version);
                    if let Some(outcome) = active.optimize_outcome_mut() {
                        outcome.set_tables(tables);
                    }
                })
                .await
                {
                    Ok(()) => {
                        landed.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok(())
                    }
                    Err(error) => {
                        in_doubt.store(true, std::sync::atomic::Ordering::SeqCst);
                        Err(omnigraph::error::OmniError::manifest_internal(format!(
                            "the operation record write before publication failed: {error}"
                        )))
                    }
                }
            }
        })
    };

    let settings = &state.recorded.settings;
    let OperationRequest::Optimize(options) = &operation.request;
    let mut request = omnigraph::db::OptimizeRequest::default()
        .with_limits(settings.limits(options))
        .with_concurrency(OptimizeConcurrency::new(settings.planning_concurrency, 1))
        .with_authority(AuthorityCapture::UnderGates)
        .with_cancellation(cancellation)
        .with_before_publish(hook);
    if let Some(tables) = &options.tables {
        request = request.with_tables(tables.clone());
    } else if let Some(start_at) = operation
        .optimize_outcome()
        .and_then(|outcome| outcome.start_at.clone())
    {
        request = request.with_start_at(start_at);
    }
    let result = handle
        .engine
        .optimize_as(Some(operation.principal.as_str()), request)
        .await;
    watch.abort();
    let cancel_reason = state.recorded.cancel_reason(&id);

    operation.timings.queued_ms = Some(started.duration_since(accepted_at).as_millis() as u64);
    operation.timings.compute_ms = Some(started.elapsed().as_millis() as u64);
    let (ending, failure) = match result {
        Ok(report) => (
            Ending::Terminal(Box::new(terminal_from_report(
                operation,
                &report,
                cancel_reason,
            ))),
            None,
        ),
        Err(error) => {
            let landed = publishing_landed.load(std::sync::atomic::Ordering::SeqCst);
            let in_doubt = publishing_in_doubt.load(std::sync::atomic::Ordering::SeqCst);
            let uncertain = matches!(
                error.completion_evidence(),
                Some(omnigraph::error::CompletionEvidence::Uncertain) | None
            );
            let message = error.to_string();
            let ending = if in_doubt {
                Ending::Silent
            } else if landed && uncertain {
                Ending::Unresolved(format!("the publication has no definite answer: {message}"))
            } else {
                operation.state = OperationState::Failed;
                operation.reason = Some(omnigraph_cluster::truncate_detail(message));
                Ending::Terminal(Box::new(operation))
            };
            (ending, Some(ApiError::from_omni(error)))
        }
    };
    {
        // The run leaves the live set in the same step as its final write, so
        // a lookup under the gate never meets an ended run still marked active.
        let lock = owner.lock_operations().await;
        let pending = match &ending {
            Ending::Terminal(terminal) => Some(PendingEnding::Finish(terminal.clone())),
            Ending::Unresolved(reason) => Some(PendingEnding::Defer {
                operation_id: id.clone(),
                reason: reason.clone(),
            }),
            Ending::Silent => None,
        };
        let written = update(&state, &owner, &lock, &[], |record| {
            let changed = match &ending {
                Ending::Terminal(terminal) => record.finish((**terminal).clone()).is_ok(),
                Ending::Unresolved(reason) => record.defer_publication(&id, reason.clone()).is_ok(),
                Ending::Silent => false,
            };
            state.recorded.clear_live(&id);
            (changed, ())
        })
        .await;
        if let Err(error) = written {
            // Whatever kept the write from landing, the outcome is known
            // here; it is applied again at the next access to the record as
            // it then stands.
            state.recorded.clear_live(&id);
            if let Some(pending) = pending {
                state
                    .recorded
                    .pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id.clone(), pending);
            }
            tracing::warn!(operation_id = %id, error = %error.message, "the recorded operation's final record write did not land; a later access re-applies it");
        }
    }
    match failure {
        // A failure that leaves storage in doubt contains the process, as any
        // other owned write's does; the record already holds the outcome.
        Some(error) if error.contains_process() => Err(error).into(),
        _ => Ok(()).into(),
    }
}

fn table_outcome(stat: &omnigraph::db::DatasetOptimizeStats) -> TableOutcome {
    let (reason, detail) = match stat.outcome {
        TableOptimizeOutcome::Deferred(reason) => (Some(reason.as_str()), Some(reason.to_string())),
        TableOptimizeOutcome::Skipped(reason) => (Some(reason.as_str()), Some(reason.to_string())),
        TableOptimizeOutcome::Failed => {
            (None, stat.failure.as_ref().map(|error| error.to_string()))
        }
        _ => (None, None),
    };
    TableOutcome {
        type_key: stat.type_key.clone(),
        outcome: stat.outcome.as_str().to_string(),
        reason: reason.map(str::to_string),
        detail: detail.map(omnigraph_cluster::truncate_detail),
        fragments_removed: stat.fragments_removed as u64,
        fragments_added: stat.fragments_added as u64,
        duration_ms: stat.duration.as_millis() as u64,
        source_bytes: stat.source_bytes,
    }
}

fn tail(outcome: &str, detail: Option<String>) -> TailOutcome {
    TailOutcome {
        outcome: outcome.to_string(),
        detail: detail.map(omnigraph_cluster::truncate_detail),
    }
}

fn terminal_from_report(
    mut operation: Operation,
    report: &OptimizeReport,
    cancel_reason: Option<String>,
) -> Operation {
    operation.input_commit = report.input_commit.clone();
    operation.input_manifest_version = Some(report.input_manifest_version);
    operation.published_commit = report.graph_commit_id.clone();
    operation.timings.gate_hold_ms = Some(report.gate_hold.as_millis() as u64);
    operation.timings.writer_yield_ms = Some(report.writer_yield.as_millis() as u64);
    if let Some(outcome) = operation.optimize_outcome_mut() {
        outcome.set_tables(report.tables.iter().map(table_outcome).collect());
        outcome.resume_at = report.resume_at.clone();
        outcome.configuration_clear = Some(tail(
            report.configuration_clear.as_str(),
            match &report.configuration_clear {
                ConfigurationClear::Failed(error) | ConfigurationClear::Unknown(error) => {
                    Some(error.clone())
                }
                _ => None,
            },
        ));
        outcome.artifact = Some(tail(
            report.artifact.as_str(),
            match &report.artifact {
                ArtifactRefresh::Skipped(reason) => Some(reason.as_str().to_string()),
                ArtifactRefresh::Failed(error) => Some(error.clone()),
                _ => None,
            },
        ));
    }
    match report.cancellation {
        RunCancellation::Cancelled => {
            operation.state = OperationState::Cancelled;
            operation.reason = Some(cancel_reason.unwrap_or_else(|| "cancelled".to_string()));
        }
        RunCancellation::ArrivedLate => {
            operation.state = OperationState::Succeeded;
            operation.reason = Some(format!(
                "a cancellation arrived after the publication started{}",
                cancel_reason
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default()
            ));
        }
        _ => operation.state = OperationState::Succeeded,
    }
    operation
}

/// Find the record under the gate and authorize the actor for its kind's
/// grant; an unknown id is authorized for the start kind, so the refusal
/// says nothing about which ids exist.
async fn find_authorized(
    state: &AppState,
    handle: &GraphRequest,
    actor: &AuthenticatedActor,
    operation_id: &str,
    cancel_by: Option<&str>,
) -> Result<Option<Operation>, ApiError> {
    let owner = admission(state)?;
    let graph = handle.key.graph_id.to_string();
    let walks = walk_unresolved(state, &owner).await;
    let lock = owner.lock_operations().await;
    let updated = update(state, &owner, &lock, &walks, |record| {
        (false, record.find(&graph, operation_id).cloned())
    })
    .await?;
    let operation = updated.value;
    let kind = operation
        .as_ref()
        .map_or(OperationKind::Optimize, Operation::kind);
    authorize(handle, actor, kind)?;
    if let (Some(actor), Some(op)) = (cancel_by, &operation)
        && !op.state.is_terminal()
        && op.state != OperationState::Publishing
    {
        state.recorded.cancel_under(
            &lock,
            Some(&op.operation_id),
            None,
            &format!("cancelled by {actor}"),
        );
    }
    Ok(operation)
}

#[utoipa::path(
    get, path = "/operations/{operation_id}", tag = "operations",
    params(("operation_id" = String, Path, description = "The id the caller submitted")),
    responses(
        (status = 200, description = "The operation's record, or `lookup: unknown`", body = OperationOutput),
        (status = 403, body = crate::api::ErrorOutput),
        (status = 409, description = "A record from another ledger at this root, or the record changed during resolution", body = crate::api::ErrorOutput),
        (status = 500, description = "Internal failure (code `internal`), or a resolution write with no definite answer (code `outcome_unknown`); retry the lookup", body = crate::api::ErrorOutput),
        (status = 503, description = "The routes are not enabled", body = crate::api::ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Look a recorded operation up by its original id, whatever its kind.
pub(crate) async fn get_operation(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    actor: Actor,
    ApiPath(path): ApiPath<OperationPath>,
) -> Result<Json<OperationOutput>, ApiError> {
    let actor = authenticated(actor)?;
    authorize(&handle, &actor, OperationKind::Optimize)?;
    state.recorded.require_enabled()?;
    let operation = find_authorized(&state, &handle, &actor, &path.operation_id, None).await?;
    Ok(output(&state, operation).1)
}

#[utoipa::path(
    post, path = "/operations/{operation_id}/cancel", tag = "operations",
    params(("operation_id" = String, Path, description = "The id the caller submitted")),
    responses(
        (status = 200, description = "The operation is terminal, unchanged; or `lookup: unknown`", body = OperationOutput),
        (status = 202, description = "Cancellation requested; a `publishing` operation is unchanged", body = OperationOutput),
        (status = 403, body = crate::api::ErrorOutput),
        (status = 409, description = "A record from another ledger at this root, or the record changed during resolution", body = crate::api::ErrorOutput),
        (status = 500, description = "Internal failure (code `internal`), or a resolution write with no definite answer (code `outcome_unknown`); look the operation up by its id", body = crate::api::ErrorOutput),
        (status = 503, description = "The routes are not enabled", body = crate::api::ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Ask a recorded operation to stop at its next safe boundary.
pub(crate) async fn cancel_operation(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    actor: Actor,
    ApiPath(path): ApiPath<OperationPath>,
) -> Result<(StatusCode, Json<OperationOutput>), ApiError> {
    let actor = authenticated(actor)?;
    authorize(&handle, &actor, OperationKind::Optimize)?;
    state.recorded.require_enabled()?;
    let operation = find_authorized(
        &state,
        &handle,
        &actor,
        &path.operation_id,
        Some(actor.actor_id.as_ref()),
    )
    .await?;
    Ok(output(&state, operation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_default_off_and_read_their_variables() {
        assert_eq!(
            OperationSettings::from_lookup(|_| None).unwrap(),
            OperationSettings::default()
        );
        let settings = OperationSettings::from_lookup(|name| match name {
            "OMNIGRAPH_OPERATION_ROUTES" => Some("true".to_string()),
            "OMNIGRAPH_OPTIMIZE_GATE_HOLD_SECONDS" => Some("30".to_string()),
            "OMNIGRAPH_OPTIMIZE_MAX_SOURCE_BYTES" => Some("1000".to_string()),
            "OMNIGRAPH_OPTIMIZE_RUN_SOURCE_BYTES" => Some("5000".to_string()),
            "OMNIGRAPH_OPERATION_RESOLUTION_WALK_LIMIT" => Some("16".to_string()),
            _ => None,
        })
        .unwrap();
        assert!(settings.enabled);
        assert_eq!(settings.default_gate_hold, Duration::from_secs(30));
        assert_eq!(settings.table_ceilings.source_bytes, Some(1000));
        assert_eq!(settings.run_source_bytes, Some(5000));
        assert_eq!(settings.resolution_walk_limit, 16);
        assert!(
            OperationSettings::from_lookup(
                |name| (name == "OMNIGRAPH_OPERATION_ROUTES").then(|| "yes".to_string())
            )
            .is_err()
        );
        assert!(
            OperationSettings::from_lookup(
                |name| (name == "OMNIGRAPH_OPTIMIZE_GATE_HOLD_SECONDS").then(|| "600".to_string())
            )
            .is_err(),
            "a default above the ceiling is refused"
        );
        assert!(
            OperationSettings::from_lookup(|name| match name {
                "OMNIGRAPH_OPTIMIZE_MAX_SOURCE_BYTES" => Some("9".to_string()),
                "OMNIGRAPH_OPTIMIZE_RUN_SOURCE_BYTES" => Some("8".to_string()),
                _ => None,
            })
            .is_err(),
            "a table byte ceiling above the run budget is refused"
        );
    }

    #[test]
    fn cancellation_reaches_only_the_named_run() {
        let runtime = RecordedOperations::default();
        let cancellation = OptimizeCancellation::new();
        runtime.set_live(LiveRun {
            operation_id: "01JBQZ00000000000000000001".to_string(),
            graph: "g".to_string(),
            cancellation: cancellation.clone(),
            cancel_reason: None,
        });
        assert!(!runtime.cancel(Some("01JBQZ00000000000000000002"), None, "other"));
        assert!(!runtime.cancel(None, Some(&["h".to_string()][..]), "other graph"));
        assert!(!cancellation.is_cancelled());
        assert!(runtime.cancel(None, Some(&["g".to_string()][..]), "deployment d"));
        assert!(cancellation.is_cancelled());
        assert_eq!(
            runtime
                .cancel_reason("01JBQZ00000000000000000001")
                .as_deref(),
            Some("deployment d")
        );
        runtime.clear_live("01JBQZ00000000000000000001");
        assert!(runtime.live_ids().is_empty());
    }
}
