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
//! Each admission scope has one in-process gate, a mutex taken with a try-lock
//! by the handler and held by the owned operation until it drops, so the check
//! of the scope's active slot, the registration and the acceptance put happen
//! under one gate: two submissions cannot both read an empty slot and both
//! start. The acceptance put is the operation's first step, not the handler's.
//!
//! A write whose answer is lost stops the run's writes. The next admission or
//! lookup reads the record back: an `accepted` or `running` operation no live
//! run holds is `interrupted`, a `publishing` one waits on its settlement
//! branch's history, one page of the first-parent chain per attempt, and a
//! lost terminal write is re-issued before any other.

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
    OperationRecord, OperationRequest, OperationState, OperationWrite, OptimizeOptions, Resolution,
    TableOutcome, TailOutcome,
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

#[derive(Default)]
struct RecordState {
    /// A terminal write whose answer was lost. This process alone writes the
    /// record and knows the outcome, so it is re-issued against whatever a
    /// re-read shows before any other write.
    unsettled: Option<OperationRecord>,
}

pub(crate) struct RecordedOperations {
    settings: OperationSettings,
    /// Serializes every read and write of the record in this process.
    record: Arc<Mutex<RecordState>>,
    /// One admission gate per scope, held by the owned operation.
    gates: BTreeMap<AdmissionScope, Arc<Mutex<()>>>,
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
            record: Arc::default(),
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

    fn clear_live(&self, operation_id: &str) -> Option<String> {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        let index = live
            .iter()
            .position(|run| run.operation_id == operation_id)?;
        live.remove(index).cancel_reason
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

/// The record, read under the gate after settling a lost terminal write, and
/// checked against the cluster's ledger.
struct Loaded {
    record: OperationRecord,
    version: Option<String>,
    outstanding_deployment: Option<String>,
}

async fn load(owner: &ClusterAdmission, state: &mut RecordState) -> Result<Loaded, ApiError> {
    let (ledger_id, outstanding_deployment) = owner.operations_context().await.map_err(refusal)?;
    let mut read = owner.read_operations().await.map_err(refusal)?;
    if let Some(intended) = state.unsettled.as_ref() {
        match &read {
            Some((current, _)) if current == intended => {}
            _ => {
                let expected = read.as_ref().map(|(_, version)| version.as_str());
                match owner
                    .write_operations(intended, expected)
                    .await
                    .map_err(refusal)?
                {
                    OperationWrite::Written { version } => {
                        read = Some((intended.clone(), version));
                    }
                    OperationWrite::Conflict => {
                        return Err(ApiError::conflict(
                            "the operation record changed while a terminal write was unsettled",
                        ));
                    }
                    OperationWrite::InDoubt { error } => {
                        return Err(ApiError::new(
                            ErrorCode::OutcomeUnknown,
                            format!(
                                "a terminal operation record write is still unsettled ({error}); retry later"
                            ),
                        ));
                    }
                }
            }
        }
        state.unsettled = None;
    }
    let (record, version) = match read {
        Some((record, version)) => (record, Some(version)),
        None => (OperationRecord::new(ledger_id.clone()), None),
    };
    if record.ledger_id != ledger_id {
        return Err(ApiError::conflict(format!(
            "[operation_record_foreign] the operation record names ledger {} but this cluster's ledger is {ledger_id}; move the record aside",
            record.ledger_id
        )));
    }
    Ok(Loaded {
        record,
        version,
        outstanding_deployment,
    })
}

/// Settle operations no live run holds, then advance each unresolved
/// publication whose graph this server serves by one page of its settlement
/// branch's history.
async fn resolve(state: &AppState, record: &mut OperationRecord) -> bool {
    let live = state.recorded.live_ids();
    let mut changed = record.interrupt_orphans(&live);
    let limit = state.recorded.settings.resolution_walk_limit;
    for operation in record.unresolved.clone() {
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
        changed |= record.settle(&operation.operation_id, resolution);
    }
    changed
}

/// Load and resolve under the gate, writing the resolution when it changed
/// the record.
async fn load_resolved(
    state: &AppState,
    owner: &ClusterAdmission,
    guard: &mut RecordState,
) -> Result<Loaded, ApiError> {
    let mut loaded = load(owner, guard).await?;
    if resolve(state, &mut loaded.record).await {
        match owner
            .write_operations(&loaded.record, loaded.version.as_deref())
            .await
            .map_err(refusal)?
        {
            OperationWrite::Written { version } => loaded.version = Some(version),
            OperationWrite::Conflict => {
                return Err(ApiError::conflict(
                    "the operation record changed during resolution; retry",
                ));
            }
            OperationWrite::InDoubt { error } => {
                return Err(ApiError::new(
                    ErrorCode::OutcomeUnknown,
                    format!(
                        "resolving the operation record has no definite answer ({error}); retry later"
                    ),
                ));
            }
        }
    }
    Ok(loaded)
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

fn validate(settings: &OperationSettings, input: &StartOptimizeRequest) -> Result<(), ApiError> {
    if !omnigraph_cluster::canonical_operation_id(&input.operation_id) {
        return Err(ApiError::bad_request(
            "operation_id must be a canonical ULID",
        ));
    }
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
        (status = 503, description = "The routes are not enabled, or admission is closed", body = crate::api::ErrorOutput),
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
    validate(&state.recorded.settings, &input)?;
    let owner = admission(&state)?;
    let graph = handle.key.graph_id.to_string();
    let principal = actor.actor_id.to_string();
    let kind = OperationKind::Optimize;

    // The scope's gate: taken here, held by the owned operation until it
    // drops, so the slot check, the registration and the acceptance put are
    // one admission.
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
    let mut record = Arc::clone(&state.recorded.record).lock_owned().await;
    let mut loaded = load_resolved(&state, &owner, &mut record).await?;
    if let (Some(mark), Some(time)) = (
        loaded.record.eviction_mark_ms,
        omnigraph_cluster::operation_id_time_ms(&input.operation_id),
    ) && time <= mark
    {
        return Err(ApiError::conflict(format!(
            "[operation_id_expired] operation id {} is at or before the record's eviction mark; mint a new id",
            input.operation_id
        )));
    }
    let start_at = if input.tables.is_none() {
        loaded
            .record
            .kind_state
            .optimize
            .get(&graph)
            .map(|state| state.resume_at.clone())
    } else {
        None
    };
    let schema_identity_domain = handle
        .engine
        .schema_contract_digest()
        .schema_identity_domain;
    let operation = Operation::accepted(
        input.operation_id.clone(),
        graph.clone(),
        schema_identity_domain,
        input.request(),
        principal,
        start_at,
    );
    if let Some(existing) = loaded.record.find(&graph, &input.operation_id) {
        return if existing.request_digest == operation.request_digest {
            Ok(output(&state, Some(existing.clone())))
        } else {
            Err(request_mismatch(&input.operation_id))
        };
    }
    if let Some(deployment) = &loaded.outstanding_deployment {
        return Err(ApiError::conflict(format!(
            "deployment {deployment} is outstanding; submit the operation after it completes"
        )));
    }
    match loaded.record.admit(operation.clone()) {
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
    }

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
    let admitted = loaded.record;
    let version = loaded.version;
    let submitted = state.operations.submit(
        (ingress, handle, gate),
        async move {
            // The acceptance put is the operation's first step, under the
            // record gate the handler still holds.
            let written = owner.write_operations(&admitted, version.as_deref()).await;
            let answer = match written {
                Ok(OperationWrite::Written { .. }) => Ok(operation.clone()),
                Ok(OperationWrite::Conflict) => Err(ApiError::conflict(
                    "the operation record changed during admission; retry",
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
            drop(record);
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
    );
    let response = match submitted {
        Ok(response) => response,
        // Nothing was written: the gate and the record guard dropped with
        // the future.
        Err(error) => return Err(error),
    };
    let result = response.result();
    tokio::pin!(result);
    tokio::select! {
        biased;
        accepted = acceptance => match accepted {
            Ok(Ok(operation)) => Ok(output(&state, Some(operation))),
            Ok(Err(error)) => Err(error),
            Err(_) => result.await.map(|()| output(&state, None)),
        },
        result = &mut result => {
            result?;
            let accepted = {
                let _record = state.recorded.record.lock().await;
                owner_record(&state, &input.operation_id).await
            };
            Ok(output(&state, accepted))
        }
    }
}

/// The operation as the record holds it, read for a response.
async fn owner_record(state: &AppState, operation_id: &str) -> Option<Operation> {
    let owner = state.cluster_admission.as_ref()?;
    let (record, _) = owner.read_operations().await.ok()??;
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
    let mut record = state.recorded.record.lock().await;
    let mut loaded = load(owner, &mut record)
        .await
        .map_err(|error| error.message.to_string())?;
    let Some(active) = loaded.record.active_mut(operation_id) else {
        return Err(format!(
            "operation {operation_id} no longer holds its scope's active slot"
        ));
    };
    change(active);
    match owner
        .write_operations(&loaded.record, loaded.version.as_deref())
        .await
    {
        Ok(OperationWrite::Written { .. }) => Ok(()),
        Ok(OperationWrite::Conflict) => Err("the operation record changed".to_string()),
        Ok(OperationWrite::InDoubt { error }) => Err(error),
        Err(error) => Err(error.message),
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
    let cancel_reason = state.recorded.clear_live(&id);

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
                operation.reason = Some(message);
                Ending::Terminal(Box::new(operation))
            };
            (ending, Some(ApiError::from_omni(error)))
        }
    };
    {
        let mut record = state.recorded.record.lock().await;
        let written = async {
            let mut loaded = load(&owner, &mut record).await.ok()?;
            match &ending {
                Ending::Terminal(terminal) => {
                    loaded.record.finish((**terminal).clone()).ok()?;
                }
                Ending::Unresolved(reason) => {
                    loaded.record.defer_publication(&id, reason.clone()).ok()?;
                }
                Ending::Silent => return Some(()),
            }
            match owner
                .write_operations(&loaded.record, loaded.version.as_deref())
                .await
            {
                Ok(OperationWrite::Written { .. }) => Some(()),
                Ok(OperationWrite::InDoubt { .. }) if matches!(ending, Ending::Terminal(_)) => {
                    record.unsettled = Some(loaded.record);
                    Some(())
                }
                _ => None,
            }
        }
        .await;
        if written.is_none() {
            tracing::warn!(operation_id = %id, "the recorded operation's final record write did not land; a later read settles it");
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
) -> Result<Option<Operation>, ApiError> {
    let owner = admission(state)?;
    let graph = handle.key.graph_id.to_string();
    let mut record = state.recorded.record.lock().await;
    let loaded = load_resolved(state, &owner, &mut record).await?;
    let operation = loaded.record.find(&graph, operation_id).cloned();
    drop(record);
    let kind = operation
        .as_ref()
        .map_or(OperationKind::Optimize, Operation::kind);
    authorize(handle, actor, kind)?;
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
    let operation = find_authorized(&state, &handle, &actor, &path.operation_id).await?;
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
    let operation = find_authorized(&state, &handle, &actor, &path.operation_id).await?;
    if let Some(op) = &operation
        && !op.state.is_terminal()
        && op.state != OperationState::Publishing
    {
        state.recorded.cancel(
            Some(&op.operation_id),
            None,
            &format!("cancelled by {}", actor.actor_id),
        );
    }
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
            runtime.clear_live("01JBQZ00000000000000000001").as_deref(),
            Some("deployment d")
        );
        assert!(runtime.live_ids().is_empty());
    }
}
