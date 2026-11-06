//! Served maintenance: a remote optimize run through the serving owner.
//!
//! A caller submits an optimize under its own maintenance id. Admission and
//! every change of the operation's state go through the cluster's record,
//! `__cluster/maintenance.json`, which this process, the lock holder, writes
//! with one conditional put each time: at acceptance, when the run starts,
//! before its publication and at its end. The run is an owned server
//! operation and holds its graph request, so a deployment that affects the
//! graph cancels it before draining it, and shutdown cancels it too.
//!
//! A write whose answer is lost stops the run's writes. The next admission or
//! lookup reads the record back: an `accepted` or `running` operation no live
//! run holds is `interrupted`, a `publishing` one waits on main's history,
//! and a lost terminal write is re-issued before any other.

use std::sync::{Arc, Mutex as SyncMutex, PoisonError};
use std::time::Duration;

use axum::{Extension, Json, extract::State, http::StatusCode};
use omnigraph::db::{
    ArtifactRefresh, ConfigurationClear, OptimizeBeforePublish, OptimizeCancellation,
    OptimizeLimits, OptimizeReport, PublicationLookup, RunCancellation, TableCeilings,
    TableOptimizeOutcome,
};
use omnigraph_cluster::{
    Admitted, ClusterAdmission, MaintenanceLedger, MaintenanceOperation, MaintenanceRefusal,
    MaintenanceState, MaintenanceTableOutcome, MaintenanceTailOutcome, MaintenanceWrite,
    OptimizeOptions, Resolution,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};
use tokio::time::Instant;
use tracing::Instrument;
use utoipa::ToSchema;

use crate::extract::{ApiJson, ApiPath};
use crate::operations::OwnedResult;
use crate::serving::GraphRequest;
use crate::workload::IngressLease;
use crate::{ApiError, AppState, AuthenticatedActor, ErrorCode, PolicyAction, PolicyRequest};

type Actor = Option<Extension<AuthenticatedActor>>;

const MAIN: &str = "main";

/// Operator settings for the maintenance routes, read once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceSettings {
    /// The routes answer 503 unless an operator enables them.
    pub enabled: bool,
    /// The gate hold of a request that names none.
    pub default_gate_hold: Duration,
    /// The largest gate hold a request may ask for.
    pub max_gate_hold: Duration,
    /// The adjacency artifact bound every served run applies.
    pub artifact_write_limit: u64,
}

impl Default for MaintenanceSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            default_gate_hold: Duration::from_secs(60),
            max_gate_hold: Duration::from_secs(300),
            artifact_write_limit: 512 * 1024 * 1024,
        }
    }
}

impl MaintenanceSettings {
    /// `OMNIGRAPH_MAINTENANCE_ROUTES` (`true` or `false`),
    /// `OMNIGRAPH_MAINTENANCE_GATE_HOLD_SECONDS`,
    /// `OMNIGRAPH_MAINTENANCE_MAX_GATE_HOLD_SECONDS` and
    /// `OMNIGRAPH_MAINTENANCE_ARTIFACT_WRITE_LIMIT_BYTES`, over the defaults.
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut settings = Self::default();
        if let Some(value) = lookup("OMNIGRAPH_MAINTENANCE_ROUTES") {
            settings.enabled = match value.as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                other => {
                    return Err(format!(
                        "OMNIGRAPH_MAINTENANCE_ROUTES must be true or false, not '{other}'"
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
        if let Some(seconds) = number("OMNIGRAPH_MAINTENANCE_GATE_HOLD_SECONDS")? {
            settings.default_gate_hold = Duration::from_secs(seconds);
        }
        if let Some(seconds) = number("OMNIGRAPH_MAINTENANCE_MAX_GATE_HOLD_SECONDS")? {
            settings.max_gate_hold = Duration::from_secs(seconds);
        }
        if let Some(bytes) = number("OMNIGRAPH_MAINTENANCE_ARTIFACT_WRITE_LIMIT_BYTES")? {
            settings.artifact_write_limit = bytes;
        }
        if settings.default_gate_hold > settings.max_gate_hold {
            return Err("the default maintenance gate hold exceeds its ceiling".to_string());
        }
        Ok(settings)
    }
}

/// The run this process holds, and how to stop it.
struct LiveRun {
    maintenance_id: String,
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
    unsettled: Option<MaintenanceLedger>,
}

#[derive(Default)]
pub(crate) struct MaintenanceRuntime {
    settings: MaintenanceSettings,
    /// Serializes every read and write of the record in this process.
    record: Mutex<RecordState>,
    live: SyncMutex<Option<LiveRun>>,
}

impl MaintenanceRuntime {
    pub(crate) fn new(settings: MaintenanceSettings) -> Self {
        Self {
            settings,
            ..Self::default()
        }
    }

    fn live_id(&self) -> Option<String> {
        self.live
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(|live| live.maintenance_id.clone())
    }

    fn clear_live(&self, maintenance_id: &str) -> Option<String> {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        if live
            .as_ref()
            .is_some_and(|run| run.maintenance_id == maintenance_id)
        {
            live.take().and_then(|run| run.cancel_reason)
        } else {
            None
        }
    }

    /// Cancel the live run, if it is `maintenance_id` (any run when `None`)
    /// on one of `graphs` (any graph when `None`), recording `reason`.
    pub(crate) fn cancel(
        &self,
        maintenance_id: Option<&str>,
        graphs: Option<&[String]>,
        reason: &str,
    ) -> bool {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(run) = live.as_mut() else {
            return false;
        };
        if maintenance_id.is_some_and(|id| id != run.maintenance_id)
            || graphs.is_some_and(|graphs| !graphs.contains(&run.graph))
        {
            return false;
        }
        run.cancel_reason.get_or_insert_with(|| reason.to_string());
        run.cancellation.cancel();
        true
    }

    fn require_enabled(&self) -> Result<(), ApiError> {
        if self.settings.enabled {
            Ok(())
        } else {
            Err(ApiError::new(
                ErrorCode::ServiceUnavailable,
                "the maintenance routes are not enabled on this server",
            ))
        }
    }
}

/// `POST /graphs/{graph_id}/maintenance/optimize`.
#[derive(Debug, Clone, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartOptimizeRequest {
    /// A canonical ULID the caller chose; a repeated submission with the same
    /// request answers the existing record.
    maintenance_id: String,
    /// Only these table keys (`node:<Type>`, `edge:<Type>`), in this order.
    #[serde(default)]
    tables: Option<Vec<String>>,
    #[serde(default)]
    max_gate_hold_ms: Option<u64>,
    #[serde(default)]
    max_source_fragments: Option<u64>,
    #[serde(default)]
    max_source_rows: Option<u64>,
    #[serde(default)]
    max_source_bytes: Option<u64>,
    #[serde(default)]
    max_blob_bytes: Option<u64>,
    #[serde(default)]
    max_index_rows: Option<u64>,
    #[serde(default)]
    run_source_bytes: Option<u64>,
}

impl StartOptimizeRequest {
    fn options(&self) -> OptimizeOptions {
        OptimizeOptions {
            tables: self.tables.clone(),
            max_gate_hold_ms: self.max_gate_hold_ms,
            max_source_fragments: self.max_source_fragments,
            max_source_rows: self.max_source_rows,
            max_source_bytes: self.max_source_bytes,
            max_blob_bytes: self.max_blob_bytes,
            max_index_rows: self.max_index_rows,
            run_source_bytes: self.run_source_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MaintenanceLookup {
    Found,
    /// The record does not hold this id for this graph. It proves neither
    /// that the operation never ran nor that it may be replayed.
    Unknown,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct MaintenanceOperationOutput {
    lookup: MaintenanceLookup,
    /// The operation's record; absent for an `unknown` lookup.
    #[schema(value_type = Option<Object>)]
    operation: Option<MaintenanceOperation>,
    /// Whether this server is running it now.
    in_progress: bool,
}

#[derive(Deserialize)]
pub(crate) struct MaintenancePath {
    maintenance_id: String,
}

fn authenticated(actor: Actor) -> Result<AuthenticatedActor, ApiError> {
    actor.map(|Extension(actor)| actor).ok_or_else(|| {
        ApiError::forbidden(
            "maintenance routes require an authenticated actor; this server resolves none",
        )
    })
}

fn authorize(handle: &GraphRequest, actor: &AuthenticatedActor) -> Result<(), ApiError> {
    crate::handlers::authorize_request(
        Some(actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Maintain,
            branch: Some(MAIN.to_string()),
            target_branch: None,
        },
    )
}

fn admission(state: &AppState) -> Result<ClusterAdmission, ApiError> {
    state.cluster_admission.clone().ok_or_else(|| {
        ApiError::conflict("served maintenance requires a v2 cluster with serving admission")
    })
}

fn refusal(error: omnigraph_cluster::Diagnostic) -> ApiError {
    ApiError::conflict(format!("[{}] {}", error.code, error.message))
}

fn outcome_unknown(maintenance_id: &str, error: &str) -> ApiError {
    ApiError::new(
        ErrorCode::OutcomeUnknown,
        format!(
            "the maintenance record write for {maintenance_id} has no definite answer ({error}); look the operation up by its id before submitting another"
        ),
    )
}

/// The record, read under the gate after settling a lost terminal write, and
/// checked against the cluster's ledger.
struct Loaded {
    ledger: MaintenanceLedger,
    version: Option<String>,
    outstanding_deployment: Option<String>,
}

async fn load(owner: &ClusterAdmission, record: &mut RecordState) -> Result<Loaded, ApiError> {
    let (ledger_id, outstanding_deployment) = owner.maintenance_context().await.map_err(refusal)?;
    let mut read = owner.read_maintenance().await.map_err(refusal)?;
    if let Some(intended) = record.unsettled.as_ref() {
        match &read {
            Some((current, _)) if current == intended => {}
            _ => {
                let expected = read.as_ref().map(|(_, version)| version.as_str());
                match owner
                    .write_maintenance(intended, expected)
                    .await
                    .map_err(refusal)?
                {
                    MaintenanceWrite::Written { version } => {
                        read = Some((intended.clone(), version));
                    }
                    MaintenanceWrite::Conflict => {
                        return Err(ApiError::conflict(
                            "the maintenance record changed while a terminal write was unsettled",
                        ));
                    }
                    MaintenanceWrite::InDoubt { error } => {
                        return Err(ApiError::new(
                            ErrorCode::OutcomeUnknown,
                            format!(
                                "a terminal maintenance record write is still unsettled ({error}); retry later"
                            ),
                        ));
                    }
                }
            }
        }
        record.unsettled = None;
    }
    let (ledger, version) = match read {
        Some((ledger, version)) => (ledger, Some(version)),
        None => (MaintenanceLedger::new(ledger_id.clone()), None),
    };
    if ledger.ledger_id != ledger_id {
        return Err(ApiError::conflict(format!(
            "[maintenance_record_foreign] the maintenance record names ledger {} but this cluster's ledger is {ledger_id}; move the record aside",
            ledger.ledger_id
        )));
    }
    Ok(Loaded {
        ledger,
        version,
        outstanding_deployment,
    })
}

/// Settle operations no live run holds, then apply main's history to each
/// unresolved publication whose graph this server serves.
async fn resolve(state: &AppState, ledger: &mut MaintenanceLedger) -> bool {
    let live = state.maintenance.live_id();
    let mut changed = ledger.interrupt_orphans(live.as_deref());
    for operation in ledger.unresolved.clone() {
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
        let resolution = match engine
            .resolve_publication(nonce, operation.input_commit.as_deref(), version)
            .await
        {
            Ok(PublicationLookup::Committed { commit_id }) => Resolution::Committed {
                published_commit: commit_id,
            },
            Ok(PublicationLookup::NotCommitted) => Resolution::NotCommitted,
            Ok(PublicationLookup::HeadUnchanged) => Resolution::InDoubt {
                reason: "main's graph head is still the input head".to_string(),
            },
            Ok(PublicationLookup::Unproven { reason }) => Resolution::InDoubt { reason },
            Ok(other) => Resolution::InDoubt {
                reason: format!("{other:?}"),
            },
            Err(error) => Resolution::InDoubt {
                reason: format!("reading main's history: {error}"),
            },
        };
        changed |= ledger.settle(&operation.maintenance_id, resolution);
    }
    changed
}

/// Load and resolve under the gate, writing the resolution when it changed
/// the record.
async fn load_resolved<'a>(
    state: &AppState,
    owner: &ClusterAdmission,
    record: &mut MutexGuard<'a, RecordState>,
) -> Result<Loaded, ApiError> {
    let mut loaded = load(owner, record).await?;
    if resolve(state, &mut loaded.ledger).await {
        match owner
            .write_maintenance(&loaded.ledger, loaded.version.as_deref())
            .await
            .map_err(refusal)?
        {
            MaintenanceWrite::Written { version } => loaded.version = Some(version),
            MaintenanceWrite::Conflict => {
                return Err(ApiError::conflict(
                    "the maintenance record changed during resolution; retry",
                ));
            }
            MaintenanceWrite::InDoubt { error } => {
                return Err(ApiError::new(
                    ErrorCode::OutcomeUnknown,
                    format!(
                        "resolving the maintenance record has no definite answer ({error}); retry later"
                    ),
                ));
            }
        }
    }
    Ok(loaded)
}

fn output(
    state: &AppState,
    operation: Option<MaintenanceOperation>,
) -> (StatusCode, Json<MaintenanceOperationOutput>) {
    let live = state.maintenance.live_id();
    let in_progress = operation
        .as_ref()
        .is_some_and(|op| live.as_deref() == Some(op.maintenance_id.as_str()));
    let status = match &operation {
        Some(op) if !op.state.is_terminal() => StatusCode::ACCEPTED,
        _ => StatusCode::OK,
    };
    (
        status,
        Json(MaintenanceOperationOutput {
            lookup: if operation.is_some() {
                MaintenanceLookup::Found
            } else {
                MaintenanceLookup::Unknown
            },
            operation,
            in_progress,
        }),
    )
}

fn validate(settings: &MaintenanceSettings, input: &StartOptimizeRequest) -> Result<(), ApiError> {
    if !omnigraph_cluster::canonical_maintenance_id(&input.maintenance_id) {
        return Err(ApiError::bad_request(
            "maintenance_id must be a canonical ULID",
        ));
    }
    if let Some(hold) = input.max_gate_hold_ms
        && Duration::from_millis(hold) > settings.max_gate_hold
    {
        return Err(ApiError::new(
            ErrorCode::PayloadTooLarge,
            format!(
                "max_gate_hold_ms {hold} exceeds this server's ceiling of {} ms",
                settings.max_gate_hold.as_millis()
            ),
        ));
    }
    if let (Some(table), Some(budget)) = (input.max_source_bytes, input.run_source_bytes)
        && table > budget
    {
        return Err(ApiError::bad_request(
            "max_source_bytes may not exceed run_source_bytes",
        ));
    }
    Ok(())
}

#[utoipa::path(
    post, path = "/maintenance/optimize", tag = "maintenance",
    request_body = StartOptimizeRequest,
    responses(
        (status = 200, description = "The operation is terminal", body = MaintenanceOperationOutput),
        (status = 202, description = "Durably accepted, or still running; look it up by its id until it is terminal", body = MaintenanceOperationOutput),
        (status = 400, body = crate::api::ErrorOutput),
        (status = 403, body = crate::api::ErrorOutput),
        (status = 409, description = "The same id with a different request, an outstanding deployment, four unresolved publications, or a record from another ledger", body = crate::api::ErrorOutput),
        (status = 413, description = "A limit above this server's ceiling", body = crate::api::ErrorOutput),
        (status = 429, description = "Another maintenance operation is active in the cluster; its id is in the message", body = crate::api::ErrorOutput),
        (status = 500, description = "Internal failure (code `internal`), or a record write with no definite answer (code `outcome_unknown`); look the operation up by its id", body = crate::api::ErrorOutput),
        (status = 503, description = "The routes are not enabled, or admission is closed", body = crate::api::ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Submit a served optimize run on the graph's main branch.
pub(crate) async fn start_optimize(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Actor,
    request: Result<ApiJson<StartOptimizeRequest>, ApiError>,
) -> Result<(StatusCode, Json<MaintenanceOperationOutput>), ApiError> {
    let actor = authenticated(actor)?;
    authorize(&handle, &actor)?;
    state.maintenance.require_enabled()?;
    let ApiJson(input) = request?;
    validate(&state.maintenance.settings, &input)?;
    let owner = admission(&state)?;
    let graph = handle.key.graph_id.to_string();
    let principal = actor.actor_id.to_string();

    let mut record = state.maintenance.record.lock().await;
    let mut loaded = load_resolved(&state, &owner, &mut record).await?;
    let start_at = if input.tables.is_none() {
        loaded.ledger.resume_at.get(&graph).cloned()
    } else {
        None
    };
    let operation = MaintenanceOperation::accepted(
        input.maintenance_id.clone(),
        graph.clone(),
        input.options(),
        principal,
        start_at,
    );
    if let Some(existing) = loaded.ledger.find(&graph, &input.maintenance_id) {
        return if existing.request_digest == operation.request_digest {
            Ok(output(&state, Some(existing.clone())))
        } else {
            Err(ApiError::conflict(format!(
                "[maintenance_request_mismatch] maintenance id {} names a different request",
                input.maintenance_id
            )))
        };
    }
    if let Some(deployment) = &loaded.outstanding_deployment {
        return Err(ApiError::conflict(format!(
            "deployment {deployment} is outstanding; submit maintenance after it completes"
        )));
    }
    let accepted_at = Instant::now();
    match loaded.ledger.admit(operation.clone()) {
        Ok(Admitted::New) => {}
        Ok(Admitted::Existing(existing)) => return Ok(output(&state, Some(*existing))),
        Err(MaintenanceRefusal::DifferentRequest) => {
            return Err(ApiError::conflict(format!(
                "[maintenance_request_mismatch] maintenance id {} names a different request",
                input.maintenance_id
            )));
        }
        Err(MaintenanceRefusal::Active { maintenance_id }) => {
            return Err(ApiError::too_many_requests(format!(
                "maintenance operation {maintenance_id} is active in this cluster; retry after it finishes"
            )));
        }
        Err(MaintenanceRefusal::Unresolved) => {
            return Err(ApiError::conflict(
                "four maintenance publications are unresolved; a write on their graphs' main settles them",
            ));
        }
    }
    match owner
        .write_maintenance(&loaded.ledger, loaded.version.as_deref())
        .await
        .map_err(refusal)?
    {
        MaintenanceWrite::Written { .. } => {}
        MaintenanceWrite::Conflict => {
            return Err(ApiError::conflict(
                "the maintenance record changed during admission; retry",
            ));
        }
        MaintenanceWrite::InDoubt { error } => {
            return Err(outcome_unknown(&input.maintenance_id, &error));
        }
    }

    let cancellation = OptimizeCancellation::new();
    *state
        .maintenance
        .live
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(LiveRun {
        maintenance_id: input.maintenance_id.clone(),
        graph,
        cancellation: cancellation.clone(),
        cancel_reason: None,
    });
    drop(record);
    let run_state = state.clone();
    let run_handle = handle.clone();
    let span = tracing::info_span!(
        "maintenance_optimize",
        maintenance_id = %input.maintenance_id,
        graph = %operation.graph,
    );
    let submitted = state.operations.submit(
        (ingress, handle),
        run(
            run_state,
            owner,
            run_handle,
            operation,
            cancellation,
            accepted_at,
        )
        .instrument(span),
    );
    if let Err(error) = submitted {
        // The accepted record has no live run; the next admission or lookup
        // records it `interrupted`.
        state.maintenance.clear_live(&input.maintenance_id);
        return Err(error);
    }
    let accepted = {
        let _record = state.maintenance.record.lock().await;
        owner_record(&state, &input.maintenance_id).await
    };
    Ok(output(&state, accepted))
}

/// The operation as the record holds it, read for a response.
async fn owner_record(state: &AppState, maintenance_id: &str) -> Option<MaintenanceOperation> {
    let owner = state.cluster_admission.as_ref()?;
    let (ledger, _) = owner.read_maintenance().await.ok()??;
    ledger
        .active
        .iter()
        .chain(&ledger.unresolved)
        .chain(&ledger.terminal)
        .find(|op| op.maintenance_id == maintenance_id)
        .cloned()
}

/// Apply `change` to the active operation and write the record once.
async fn transition(
    state: &AppState,
    owner: &ClusterAdmission,
    maintenance_id: &str,
    change: impl FnOnce(&mut MaintenanceOperation),
) -> Result<(), String> {
    let mut record = state.maintenance.record.lock().await;
    let mut loaded = load(owner, &mut record)
        .await
        .map_err(|error| error.message.to_string())?;
    let Some(active) = loaded.ledger.active_mut(maintenance_id) else {
        return Err(format!(
            "maintenance operation {maintenance_id} no longer holds the active slot"
        ));
    };
    change(active);
    match owner
        .write_maintenance(&loaded.ledger, loaded.version.as_deref())
        .await
    {
        Ok(MaintenanceWrite::Written { .. }) => Ok(()),
        Ok(MaintenanceWrite::Conflict) => Err("the maintenance record changed".to_string()),
        Ok(MaintenanceWrite::InDoubt { error }) => Err(error),
        Err(error) => Err(error.message),
    }
}

/// How the run ends in the record.
enum Ending {
    Terminal(Box<MaintenanceOperation>),
    /// The publication may have landed: main's history settles it.
    Unresolved(String),
    /// A record write was in doubt: write nothing more; a later read settles.
    Silent,
}

async fn run(
    state: AppState,
    owner: ClusterAdmission,
    handle: GraphRequest,
    mut operation: MaintenanceOperation,
    cancellation: OptimizeCancellation,
    accepted_at: Instant,
) -> OwnedResult<()> {
    let started = Instant::now();
    let id = operation.maintenance_id.clone();

    if let Err(error) = transition(&state, &owner, &id, |active| {
        active.state = MaintenanceState::Running;
    })
    .await
    {
        tracing::warn!(maintenance_id = %id, %error, "maintenance run did not start");
        state.maintenance.clear_live(&id);
        return Ok(()).into();
    }

    // Shutdown and containment close admission; the run stops at its next
    // safe boundary.
    let watch = {
        let operations = state.operations.clone();
        let maintenance = Arc::clone(&state.maintenance);
        let id = id.clone();
        tokio::spawn(async move {
            operations.wait_closed().await;
            maintenance.cancel(Some(&id), None, "the server is shutting down");
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
                    active.state = MaintenanceState::Publishing;
                    active.nonce = Some(publication.nonce.clone());
                    active.input_commit = publication.input_commit.clone();
                    active.input_manifest_version = Some(publication.input_manifest_version);
                    active.tables = tables;
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
                            "the maintenance record write before publication failed: {error}"
                        )))
                    }
                }
            }
        })
    };

    let settings = &state.maintenance.settings;
    let options = &operation.options;
    let limits = OptimizeLimits::default()
        .with_max_gate_hold(
            options
                .max_gate_hold_ms
                .map_or(settings.default_gate_hold, Duration::from_millis),
        )
        .with_artifact_bytes(settings.artifact_write_limit)
        .with_table_ceilings(table_ceilings(options));
    let limits = match options.run_source_bytes {
        Some(budget) => limits.with_run_source_bytes(budget),
        None => limits,
    };
    let mut request = omnigraph::db::OptimizeRequest::default()
        .with_limits(limits)
        .with_cancellation(cancellation)
        .with_before_publish(hook);
    if let Some(tables) = &options.tables {
        request = request.with_tables(tables.clone());
    } else if let Some(start_at) = &operation.start_at {
        request = request.with_start_at(start_at.clone());
    }
    let result = handle
        .engine
        .optimize_as(Some(operation.principal.as_str()), request)
        .await;
    watch.abort();
    let cancel_reason = state.maintenance.clear_live(&id);

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
                operation.state = MaintenanceState::Failed;
                operation.reason = Some(message);
                Ending::Terminal(Box::new(operation))
            };
            (ending, Some(ApiError::from_omni(error)))
        }
    };
    {
        let mut record = state.maintenance.record.lock().await;
        let written = async {
            let mut loaded = load(&owner, &mut record).await.ok()?;
            match &ending {
                Ending::Terminal(terminal) => {
                    loaded.ledger.finish((**terminal).clone()).ok()?;
                }
                Ending::Unresolved(reason) => {
                    loaded.ledger.defer_publication(&id, reason.clone()).ok()?;
                }
                Ending::Silent => return Some(()),
            }
            match owner
                .write_maintenance(&loaded.ledger, loaded.version.as_deref())
                .await
            {
                Ok(MaintenanceWrite::Written { .. }) => Some(()),
                Ok(MaintenanceWrite::InDoubt { .. }) if matches!(ending, Ending::Terminal(_)) => {
                    record.unsettled = Some(loaded.ledger);
                    Some(())
                }
                _ => None,
            }
        }
        .await;
        if written.is_none() {
            tracing::warn!(maintenance_id = %id, "the maintenance run's final record write did not land; a later read settles it");
        }
    }
    match failure {
        // A failure that leaves storage in doubt contains the process, as any
        // other owned write's does; the record already holds the outcome.
        Some(error) if error.contains_process() => Err(error).into(),
        _ => Ok(()).into(),
    }
}

fn table_ceilings(options: &OptimizeOptions) -> TableCeilings {
    let mut ceilings = TableCeilings::default();
    if let Some(value) = options.max_source_fragments {
        ceilings = ceilings.with_source_fragments(value);
    }
    if let Some(value) = options.max_source_rows {
        ceilings = ceilings.with_source_rows(value);
    }
    if let Some(value) = options.max_source_bytes {
        ceilings = ceilings.with_source_bytes(value);
    }
    if let Some(value) = options.max_blob_bytes {
        ceilings = ceilings.with_blob_bytes(value);
    }
    if let Some(value) = options.max_index_rows {
        ceilings = ceilings.with_index_rows(value);
    }
    ceilings
}

fn table_outcome(stat: &omnigraph::db::DatasetOptimizeStats) -> MaintenanceTableOutcome {
    let (reason, detail) = match stat.outcome {
        TableOptimizeOutcome::Deferred(reason) => (Some(reason.as_str()), Some(reason.to_string())),
        TableOptimizeOutcome::Skipped(reason) => (Some(reason.as_str()), Some(reason.to_string())),
        TableOptimizeOutcome::Failed => {
            (None, stat.failure.as_ref().map(|error| error.to_string()))
        }
        _ => (None, None),
    };
    MaintenanceTableOutcome {
        type_key: stat.type_key.clone(),
        outcome: stat.outcome.as_str().to_string(),
        reason: reason.map(str::to_string),
        detail,
        fragments_removed: stat.fragments_removed as u64,
        fragments_added: stat.fragments_added as u64,
        duration_ms: stat.duration.as_millis() as u64,
        source_bytes: stat.source_bytes,
    }
}

fn terminal_from_report(
    mut operation: MaintenanceOperation,
    report: &OptimizeReport,
    cancel_reason: Option<String>,
) -> MaintenanceOperation {
    operation.tables = report.tables.iter().map(table_outcome).collect();
    operation.input_commit = report.input_commit.clone();
    operation.input_manifest_version = Some(report.input_manifest_version);
    operation.published_commit = report.graph_commit_id.clone();
    operation.resume_at = report.resume_at.clone();
    operation.timings.gate_hold_ms = Some(report.gate_hold.as_millis() as u64);
    operation.configuration_clear = Some(MaintenanceTailOutcome {
        outcome: report.configuration_clear.as_str().to_string(),
        detail: match &report.configuration_clear {
            ConfigurationClear::Failed(error) | ConfigurationClear::Unknown(error) => {
                Some(error.clone())
            }
            _ => None,
        },
    });
    operation.artifact = Some(MaintenanceTailOutcome {
        outcome: report.artifact.as_str().to_string(),
        detail: match &report.artifact {
            ArtifactRefresh::Skipped(reason) => Some(reason.as_str().to_string()),
            ArtifactRefresh::Failed(error) => Some(error.clone()),
            _ => None,
        },
    });
    match report.cancellation {
        RunCancellation::Cancelled => {
            operation.state = MaintenanceState::Cancelled;
            operation.reason = Some(cancel_reason.unwrap_or_else(|| "cancelled".to_string()));
        }
        RunCancellation::ArrivedLate => {
            operation.state = MaintenanceState::Succeeded;
            operation.reason = Some(format!(
                "a cancellation arrived after the publication started{}",
                cancel_reason
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default()
            ));
        }
        _ => operation.state = MaintenanceState::Succeeded,
    }
    operation
}

#[utoipa::path(
    get, path = "/maintenance/{maintenance_id}", tag = "maintenance",
    params(("maintenance_id" = String, Path, description = "The id the caller submitted")),
    responses(
        (status = 200, description = "The operation's record, or `lookup: unknown`", body = MaintenanceOperationOutput),
        (status = 403, body = crate::api::ErrorOutput),
        (status = 409, description = "A record from another ledger at this root, or the record changed during resolution", body = crate::api::ErrorOutput),
        (status = 500, description = "Internal failure (code `internal`), or a resolution write with no definite answer (code `outcome_unknown`); retry the lookup", body = crate::api::ErrorOutput),
        (status = 503, description = "The routes are not enabled", body = crate::api::ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Look a served maintenance operation up by its original id.
pub(crate) async fn get_maintenance_operation(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    actor: Actor,
    ApiPath(path): ApiPath<MaintenancePath>,
) -> Result<Json<MaintenanceOperationOutput>, ApiError> {
    let actor = authenticated(actor)?;
    authorize(&handle, &actor)?;
    state.maintenance.require_enabled()?;
    let owner = admission(&state)?;
    let graph = handle.key.graph_id.to_string();
    let mut record = state.maintenance.record.lock().await;
    let loaded = load_resolved(&state, &owner, &mut record).await?;
    let operation = loaded.ledger.find(&graph, &path.maintenance_id).cloned();
    drop(record);
    Ok(output(&state, operation).1)
}

#[utoipa::path(
    post, path = "/maintenance/{maintenance_id}/cancel", tag = "maintenance",
    params(("maintenance_id" = String, Path, description = "The id the caller submitted")),
    responses(
        (status = 200, description = "The operation is terminal, unchanged; or `lookup: unknown`", body = MaintenanceOperationOutput),
        (status = 202, description = "Cancellation requested; a `publishing` operation is unchanged", body = MaintenanceOperationOutput),
        (status = 403, body = crate::api::ErrorOutput),
        (status = 409, description = "A record from another ledger at this root, or the record changed during resolution", body = crate::api::ErrorOutput),
        (status = 500, description = "Internal failure (code `internal`), or a resolution write with no definite answer (code `outcome_unknown`); look the operation up by its id", body = crate::api::ErrorOutput),
        (status = 503, description = "The routes are not enabled", body = crate::api::ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Ask a served maintenance operation to stop at its next safe boundary.
pub(crate) async fn cancel_maintenance_operation(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    actor: Actor,
    ApiPath(path): ApiPath<MaintenancePath>,
) -> Result<(StatusCode, Json<MaintenanceOperationOutput>), ApiError> {
    let actor = authenticated(actor)?;
    authorize(&handle, &actor)?;
    state.maintenance.require_enabled()?;
    let owner = admission(&state)?;
    let graph = handle.key.graph_id.to_string();
    let mut record = state.maintenance.record.lock().await;
    let loaded = load_resolved(&state, &owner, &mut record).await?;
    let operation = loaded.ledger.find(&graph, &path.maintenance_id).cloned();
    drop(record);
    if let Some(op) = &operation
        && !op.state.is_terminal()
        && op.state != MaintenanceState::Publishing
    {
        state.maintenance.cancel(
            Some(&op.maintenance_id),
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
            MaintenanceSettings::from_lookup(|_| None).unwrap(),
            MaintenanceSettings::default()
        );
        let settings = MaintenanceSettings::from_lookup(|name| match name {
            "OMNIGRAPH_MAINTENANCE_ROUTES" => Some("true".to_string()),
            "OMNIGRAPH_MAINTENANCE_GATE_HOLD_SECONDS" => Some("30".to_string()),
            _ => None,
        })
        .unwrap();
        assert!(settings.enabled);
        assert_eq!(settings.default_gate_hold, Duration::from_secs(30));
        assert!(
            MaintenanceSettings::from_lookup(
                |name| (name == "OMNIGRAPH_MAINTENANCE_ROUTES").then(|| "yes".to_string())
            )
            .is_err()
        );
        assert!(
            MaintenanceSettings::from_lookup(|name| (name
                == "OMNIGRAPH_MAINTENANCE_GATE_HOLD_SECONDS")
                .then(|| "600".to_string()))
            .is_err(),
            "a default above the ceiling is refused"
        );
    }

    #[test]
    fn cancellation_reaches_only_the_named_run() {
        let runtime = MaintenanceRuntime::default();
        let cancellation = OptimizeCancellation::new();
        *runtime.live.lock().unwrap() = Some(LiveRun {
            maintenance_id: "01JBQZ00000000000000000001".to_string(),
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
    }
}
