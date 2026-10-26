//! Server-owned optimization. Native receipts own invocation status; this
//! module only owns the task lifetime and the process admission boundary.

use std::{
    io::Read as _,
    sync::{Arc, Mutex, PoisonError},
};

use axum::{Extension, Json, extract::State, http::StatusCode};
use omnigraph::db::OptimizeLimits;
use omnigraph_cluster::{
    IdentityAuthorization, MaintenanceAcceptance, MaintenanceCompletion, MaintenanceLookup,
    MaintenanceRequest, MaintenanceStatus,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{OwnedMutexGuard, oneshot};
use tracing::Instrument;
use utoipa::ToSchema;

use crate::extract::{ApiJson, ApiPath, ApiQuery};
use crate::operations::OwnedResult;
use crate::registry::RegistryCapture;
use crate::workload::IngressLease;
use crate::{ApiError, AppState, AuthenticatedActor, GraphId, GraphKey};

pub(crate) const REQUEST_BYTES: usize = 4096;
const CONFIG_BYTES: u64 = 4096;

#[derive(Default)]
pub(crate) struct MaintenanceRuntime {
    limits: Option<OptimizeLimits>,
    running: Mutex<(u64, Option<String>)>,
}

impl MaintenanceRuntime {
    async fn observe<T, F>(
        &self,
        id: &str,
        mut read: impl FnMut() -> F,
    ) -> Result<(T, bool), ApiError>
    where
        F: std::future::Future<Output = Result<T, ApiError>>,
    {
        for _ in 0..3 {
            let (generation, _) = self.observation(id);
            let value = read().await?;
            let (observed_generation, in_progress) = self.observation(id);
            if generation == observed_generation {
                return Ok((value, in_progress));
            }
        }
        Err(ApiError::new(
            crate::api::ErrorCode::ServiceUnavailable,
            "maintenance ownership changed during lookup; repeat this GET",
        ))
    }

    fn observation(&self, id: &str) -> (u64, bool) {
        let current = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        (current.0, current.1.as_deref() == Some(id))
    }

    fn own(self: &Arc<Self>, id: String, gate: OwnedMutexGuard<()>) -> RunningMaintenance {
        let mut current = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        current.0 = current.0.wrapping_add(1);
        current.1 = Some(id);
        RunningMaintenance {
            runtime: Arc::clone(self),
            gate: Some(gate),
        }
    }
}

struct RunningMaintenance {
    runtime: Arc<MaintenanceRuntime>,
    gate: Option<OwnedMutexGuard<()>>,
}

impl Drop for RunningMaintenance {
    fn drop(&mut self) {
        let mut current = self
            .runtime
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // A successor cannot install its marker until this transition finishes.
        // Releasing the gate in a separate tuple field would let this Drop
        // erase the successor's marker after it acquired that gate.
        drop(self.gate.take());
        current.0 = current.0.wrapping_add(1);
        current.1 = None;
    }
}

impl AppState {
    /// Enable served optimization with explicit operator-owned input ceilings.
    /// These are source bounds, not a process CPU/memory or storage-rate quota.
    pub fn with_maintenance_limits(self, limits: OptimizeLimits) -> color_eyre::Result<Self> {
        validate_limits(&limits).map_err(|error| color_eyre::eyre::eyre!(error.message))?;
        Ok(self.with_optional_maintenance_limits(Some(limits)))
    }

    pub(crate) fn with_optional_maintenance_limits(
        mut self,
        limits: Option<OptimizeLimits>,
    ) -> Self {
        self.maintenance = Arc::new(MaintenanceRuntime {
            limits,
            running: Mutex::new((0, None)),
        });
        self
    }
}

pub(crate) fn load_limits_from_env(
    has_identity_trust: bool,
) -> color_eyre::Result<Option<OptimizeLimits>> {
    let Some(path) = std::env::var_os("OMNIGRAPH_MAINTENANCE_CONFIG") else {
        return Ok(None);
    };
    load_limits_from_path(std::path::Path::new(&path), has_identity_trust).map(Some)
}

fn load_limits_from_path(
    path: &std::path::Path,
    has_identity_trust: bool,
) -> color_eyre::Result<OptimizeLimits> {
    require_capacity_profile(has_identity_trust)
        .map_err(|error| color_eyre::eyre::eyre!(error.message))?;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)?;
    color_eyre::eyre::ensure!(
        bytes.len() as u64 <= CONFIG_BYTES,
        "maintenance configuration exceeds 4 KiB"
    );
    let limits: OptimizeLimits = yaml_serde::from_slice(&bytes)?;
    validate_limits(&limits).map_err(|error| color_eyre::eyre::eyre!(error.message))?;
    Ok(limits)
}

fn validate_limits(limits: &OptimizeLimits) -> Result<(), ApiError> {
    if limits.max_tables == 0
        || limits.max_fragments == 0
        || limits.max_rows == 0
        || limits.max_files == 0
        || limits.max_source_bytes == 0
    {
        return Err(ApiError::bad_request(
            "optimization limits must all be positive",
        ));
    }
    Ok(())
}

fn require_capacity_profile(has_identity_trust: bool) -> Result<(), ApiError> {
    if has_identity_trust {
        return Err(ApiError::conflict(
            "served optimization with cluster-bound identity trust is disabled until resource capacity admission is available",
        ));
    }
    Ok(())
}

fn ceilings(state: &AppState) -> Result<&OptimizeLimits, ApiError> {
    // This is a server property, not a credential property: using a static
    // token or attaching trust after limits cannot bypass required admission.
    // Exact result lookup remains available without enabling fresh work.
    require_capacity_profile(
        state.data_token_trust.is_some() || state.oidc_identity_trust.is_some(),
    )?;
    state.maintenance.limits.as_ref().ok_or_else(|| ApiError::conflict(
        "served optimization is disabled; the server operator must configure finite maintenance limits",
    ))
}

fn check_ceiling(request: &OptimizeLimits, ceiling: &OptimizeLimits) -> Result<(), ApiError> {
    validate_limits(request)?;
    if request.max_tables > ceiling.max_tables
        || request.max_fragments > ceiling.max_fragments
        || request.max_rows > ceiling.max_rows
        || request.max_files > ceiling.max_files
        || request.max_source_bytes > ceiling.max_source_bytes
    {
        return Err(ApiError::bad_request(
            "requested optimization limits exceed server ceilings",
        ));
    }
    Ok(())
}

fn identity(
    actor: Option<Extension<AuthenticatedActor>>,
) -> Result<(AuthenticatedActor, IdentityAuthorization), ApiError> {
    let actor = actor
        .map(|Extension(actor)| actor)
        .ok_or_else(|| ApiError::forbidden("optimization requires an authenticated actor"))?;
    let identity = IdentityAuthorization::authenticated(actor.actor_id_str()).map_err(refusal)?;
    Ok((actor, identity))
}

fn owner(state: &AppState) -> Result<&omnigraph_cluster::ClusterAdmission, ApiError> {
    state
        .cluster_admission
        .as_ref()
        .ok_or_else(|| ApiError::conflict("optimization requires native serving admission"))
}

fn refusal(error: omnigraph_cluster::Diagnostic) -> ApiError {
    let message = format!("[{}] {}", error.code, error.message);
    match error.code.as_str() {
        "policy_denied" | "graph_policy_required" | "identity_invalid" => {
            ApiError::forbidden(message)
        }
        "maintenance_id_invalid" | "maintenance_graph_invalid" | "maintenance_limits_invalid" => {
            ApiError::bad_request(message)
        }
        "maintenance_write_uncertain" | "maintenance_read_error" => ApiError::internal(message),
        _ => ApiError::conflict(message),
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphSelector {
    graph: String,
}

/// Positive source ceilings for one optimization; none may exceed the server's
/// advertised limits. They do not bound process memory, CPU or total I/O.
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MaintenanceLimits)]
pub(crate) struct Limits {
    #[schema(minimum = 1)]
    max_tables: u32,
    #[schema(minimum = 1)]
    max_fragments: u64,
    #[schema(minimum = 1)]
    max_rows: u64,
    #[schema(minimum = 1)]
    max_source_bytes: u64,
    #[schema(minimum = 1)]
    max_files: u64,
}

impl From<OptimizeLimits> for Limits {
    fn from(value: OptimizeLimits) -> Self {
        Self {
            max_tables: value.max_tables,
            max_fragments: value.max_fragments,
            max_rows: value.max_rows,
            max_source_bytes: value.max_source_bytes,
            max_files: value.max_files,
        }
    }
}

impl From<Limits> for OptimizeLimits {
    fn from(value: Limits) -> Self {
        Self {
            max_tables: value.max_tables,
            max_fragments: value.max_fragments,
            max_rows: value.max_rows,
            max_source_bytes: value.max_source_bytes,
            max_files: value.max_files,
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = MaintenanceKind)]
pub(crate) enum Kind {
    Optimize,
}

/// Submit exactly once. An uncertain response permits only GET observation of
/// this original operation identity, never a replacement ID or storage fallback.
#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = MaintenanceRequest)]
pub(crate) struct SubmitRequest {
    /// Canonical ledger ULID, next sequence and fresh ULID nonce, joined by `:`.
    #[schema(example = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW")]
    operation_id: String,
    /// An applied graph ID. Optimization always targets its main branch.
    graph: String,
    kind: Kind,
    limits: Limits,
}

impl From<SubmitRequest> for MaintenanceRequest {
    fn from(value: SubmitRequest) -> Self {
        Self {
            operation_id: value.operation_id,
            graph: value.graph,
            kind: match value.kind {
                Kind::Optimize => omnigraph_cluster::MaintenanceKind::Optimize,
            },
            limits: value.limits.into(),
        }
    }
}

#[derive(Serialize, ToSchema)]
#[schema(as = MaintenanceStatus)]
pub(crate) struct Status {
    /// Native ledger identity used in the next operation ID.
    ledger_id: String,
    #[schema(minimum = 1)]
    next_sequence: u64,
    /// Outstanding operation for this graph, or null. Status reads create no journal.
    #[schema(required = true)]
    outstanding_id: Option<String>,
}

impl From<MaintenanceStatus> for Status {
    fn from(value: MaintenanceStatus) -> Self {
        Self {
            ledger_id: value.ledger_id,
            next_sequence: value.next_sequence,
            outstanding_id: value.outstanding_id,
        }
    }
}

#[derive(Serialize, ToSchema)]
#[schema(as = MaintenanceStatusOutput)]
pub(crate) struct StatusOutput {
    status: Status,
    limits: Limits,
}

// Schema-only views keep HTTP schema dependencies out of native Core and the
// engine. The native receipt remains the wire value; the parity test below
// round-trips every native variant through these views without losing fields.
#[allow(dead_code)]
mod receipt_schema {
    use super::*;

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
    #[schema(as = MaintenanceLookup)]
    #[allow(clippy::large_enum_variant)]
    pub(super) enum Lookup {
        Outstanding {
            operation_id: String,
            graph: String,
            invocation: Invocation,
        },
        Complete {
            result: Result,
        },
        ResultExpired {
            operation_id: String,
        },
        NotRecorded {
            operation_id: String,
        },
        IdentityMismatch {
            operation_id: String,
        },
        DifferentLedger {
            operation_id: String,
        },
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
    #[schema(as = MaintenanceResult)]
    pub(super) enum Result {
        Success {
            operation_id: String,
            graph: String,
            invocation: Invocation,
            report: Report,
        },
        Refused {
            operation_id: String,
            graph: String,
            invocation: Invocation,
            code: String,
            message: String,
        },
        Interrupted {
            operation_id: String,
            graph: String,
            invocation: Invocation,
            publication: PublicationOutcome,
        },
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
    #[schema(as = MaintenancePublication)]
    pub(super) enum PublicationOutcome {
        Unpublished,
        Published { report: Report },
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = MaintenanceInvocation)]
    pub(super) struct Invocation {
        request: SubmitRequest,
        actor: String,
        canonical_root: String,
        result_revision: u64,
        /// Applied configuration contract at admission, not an execution-time head.
        schema_contract: SchemaContract,
        /// Accepted graph commit and source inventory, bound before physical work.
        #[schema(required = true)]
        input: Option<Input>,
        #[schema(required = true)]
        publication: Option<Publication>,
        #[schema(required = true)]
        settlement: Option<Settlement>,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = OptimizePublication)]
    struct Publication {
        before_manifest_version: u64,
        intent_nonce: String,
        schema_contract: SchemaContract,
        #[schema(required = true)]
        actor: Option<String>,
        created_at: i64,
        report: Report,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = OptimizeSettlement)]
    struct Settlement {
        original_digest: String,
        lineage: SettlementLineage,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = OptimizeSettlementLineage)]
    struct SettlementLineage {
        graph_commit_id: String,
        #[schema(required = true)]
        branch: Option<String>,
        #[schema(required = true)]
        actor_id: Option<String>,
        #[schema(required = true)]
        merged_parent_commit_id: Option<String>,
        created_at: i64,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = MaintenanceSchemaContract)]
    struct SchemaContract {
        source_hash: String,
        schema_ir_hash: String,
        schema_identity_domain: String,
        schema_identity_version: u32,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = OptimizeInput)]
    struct Input {
        before_commit: String,
        report_max_bytes: u64,
        schema_contract: SchemaContract,
        source_tables: u32,
        source_fragments: u64,
        source_rows: u64,
        source_bytes: u64,
        source_files: u64,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = OptimizeReport)]
    pub(super) struct Report {
        before_commit: String,
        after_commit: String,
        tables: Vec<TableStats>,
        source_tables: u32,
        source_fragments: u64,
        source_rows: u64,
        source_bytes: u64,
        source_files: u64,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = OptimizeTableStats)]
    struct TableStats {
        type_key: String,
        fragments_removed: usize,
        fragments_added: usize,
        committed: bool,
        #[schema(required = true)]
        skipped: Option<SkipReason>,
        #[schema(required = true)]
        published_dataset_version: Option<u64>,
        #[schema(required = true)]
        lance_head_version: Option<u64>,
        pending_indexes: Vec<PendingIndex>,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(rename_all = "snake_case")]
    #[schema(as = OptimizeSkipReason)]
    enum SkipReason {
        DriftNeedsRepair,
    }

    #[derive(Deserialize, Serialize, ToSchema)]
    #[serde(deny_unknown_fields)]
    #[schema(as = OptimizePendingIndex)]
    struct PendingIndex {
        type_key: String,
        property: String,
        reason: String,
    }
}

#[derive(Serialize, ToSchema)]
#[schema(as = MaintenanceOutput, example = json!({"maintenance":{"status":"not_recorded","operation_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW"},"in_progress":false}))]
pub(crate) struct Output {
    /// Native lookup tagged by `status`: outstanding, complete, result_expired,
    /// not_recorded, identity_mismatch or different_ledger. Complete contains
    /// `result` with operation_id, graph, invocation and outcome: success with
    /// its exact report, or refused with code/message. Only success proves work
    /// completed; missing/expired evidence never permits replay.
    #[schema(value_type = receipt_schema::Lookup)]
    maintenance: MaintenanceLookup,
    /// Current process owns this ID. Ownership alone is not durable acceptance;
    /// an outstanding receipt without an owner remains unresolved.
    in_progress: bool,
}

#[utoipa::path(
    get, path = "/cluster/maintenance", operation_id = "maintenanceStatus", tag = "cluster",
    params(("graph" = String, Query, description = "Graph to optimize; scope is main")),
    responses((status = 200, body = StatusOutput), (status = 400, body = crate::api::ErrorOutput),
        (status = 403, body = crate::api::ErrorOutput), (status = 409, body = crate::api::ErrorOutput),
        (status = 500, body = crate::api::ErrorOutput)),
    security(("bearer_token" = []))
)]
pub(crate) async fn status(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiQuery(selector): ApiQuery<GraphSelector>,
) -> Result<Json<StatusOutput>, ApiError> {
    let (_, identity) = identity(actor)?;
    let limits = ceilings(&state)?.clone();
    let status = omnigraph_cluster::maintenance_status(owner(&state)?, &selector.graph, &identity)
        .await
        .map_err(refusal)?;
    Ok(Json(StatusOutput {
        status: status.into(),
        limits: limits.into(),
    }))
}

#[utoipa::path(
    get, path = "/cluster/maintenance/{operation_id}", operation_id = "maintenanceLookup", tag = "cluster",
    params(("operation_id" = String, Path, description = "Original maintenance identity"),
        ("graph" = String, Query, description = "Exact graph of the original operation")),
    responses((status = 200, body = Output), (status = 400, body = crate::api::ErrorOutput),
        (status = 403, body = crate::api::ErrorOutput), (status = 409, body = crate::api::ErrorOutput),
        (status = 500, body = crate::api::ErrorOutput),
        (status = 503, description = "Owner changed during observation; retry this GET", body = crate::api::ErrorOutput)),
    security(("bearer_token" = []))
)]
pub(crate) async fn lookup(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(id): ApiPath<String>,
    ApiQuery(selector): ApiQuery<GraphSelector>,
) -> Result<Json<Output>, ApiError> {
    let (_, identity) = identity(actor)?;
    // Receipt access remains available when new optimization is disabled.
    // If the process owner finishes during the read, repeat the read so an old
    // Outstanding record is never presented as ownerless terminal evidence.
    let admission = owner(&state)?;
    let (maintenance, in_progress) = state
        .maintenance
        .observe(&id, || async {
            omnigraph_cluster::maintenance_lookup(admission, &selector.graph, &id, &identity)
                .await
                .map_err(refusal)
        })
        .await?;
    Ok(Json(Output {
        maintenance,
        in_progress,
    }))
}

#[utoipa::path(
    post, path = "/cluster/maintenance", operation_id = "optimize", tag = "cluster", request_body = SubmitRequest,
    responses((status = 200, body = Output), (status = 202, description = "Durable acceptance; inspect the original identity", body = Output),
        (status = 400, body = crate::api::ErrorOutput), (status = 403, body = crate::api::ErrorOutput),
        (status = 409, body = crate::api::ErrorOutput), (status = 415, body = crate::api::ErrorOutput),
        (status = 503, body = crate::api::ErrorOutput),
        (status = 500, description = "Internal failure (code `internal`) or uncertain optimization (code `outcome_unknown`); inspect the original identity without resubmission", body = crate::api::ErrorOutput)),
    security(("bearer_token" = []))
)]
pub(crate) async fn submit(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
    Extension(ingress): Extension<IngressLease>,
    request: Result<ApiJson<SubmitRequest>, ApiError>,
) -> Result<(StatusCode, Json<Output>), ApiError> {
    let (actor, identity) = identity(actor)?;
    let ApiJson(request) = request?;
    let request: MaintenanceRequest = request.into();
    let admission = owner(&state)?.clone();
    let (existing, in_progress) = state
        .maintenance
        .observe(&request.operation_id, || async {
            omnigraph_cluster::lookup_maintenance_request(&admission, &request, &identity)
                .await
                .map_err(refusal)
        })
        .await?;
    if let Some(maintenance) = existing {
        return Ok((
            StatusCode::OK,
            Json(Output {
                maintenance,
                in_progress,
            }),
        ));
    }
    check_ceiling(&request.limits, ceilings(&state)?)?;
    let gate = Arc::clone(&state.deployments.gate)
        .try_lock_owned()
        .map_err(|_| {
            ApiError::conflict(
                "native configuration or maintenance is running; inspect its original identity",
            )
        })?;
    // Native checks current applied policy before we capture graph ownership.
    omnigraph_cluster::maintenance_status(&admission, &request.graph, &identity)
        .await
        .map_err(refusal)?;
    let key = GraphKey::cluster(
        GraphId::try_from(request.graph.as_str())
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
    );
    let (capture, _) = state
        .routing
        .registry
        .capture_with_policy(&state.operations, &key)?;
    let RegistryCapture::Ready(graph) = capture else {
        return Err(ApiError::conflict("graph is unavailable for optimization"));
    };
    let reservation = state
        .workload
        .try_admit(&actor.actor_id, REQUEST_BYTES as u64)
        .map_err(ApiError::from_workload_reject)?;
    let running = state.maintenance.own(request.operation_id.clone(), gate);
    let span = tracing::info_span!("maintenance.optimize", operation_id = %request.operation_id, graph = %request.graph);
    let (accepted, acceptance) = oneshot::channel();
    let response = state.operations.submit(
        (running, reservation, ingress, graph.clone()),
        async move {
            let mut uncertain = false;
            let result = async {
                let token =
                    match omnigraph_cluster::accept_maintenance(&admission, request, &identity)
                        .await
                    {
                        Ok(MaintenanceAcceptance::Existing(maintenance)) => {
                            return Ok(Output {
                                maintenance,
                                in_progress: false,
                            });
                        }
                        Ok(MaintenanceAcceptance::Accepted(token)) => token,
                        Err(error) => {
                            uncertain = error.code == "maintenance_write_uncertain";
                            return Err(refusal(error));
                        }
                    };
                // Everything after acceptance retains uncertainty until the exact
                // terminal record has been durably written.
                uncertain = true;
                tracing::info!("native optimization accepted");
                let pending = omnigraph_cluster::maintenance_lookup(
                    &admission,
                    &token.request().graph,
                    &token.request().operation_id,
                    &identity,
                )
                .await
                .map_err(refusal)?;
                let _ = accepted.send(pending);
                let limits = token.request().limits.clone();
                let token = tokio::sync::Mutex::new(token);
                let binding_error = tokio::sync::Mutex::new(None);
                let execution = graph
                    .engine
                    .optimize_with_limits_recorded_as(
                        &limits,
                        Some(identity.actor()),
                        |input| {
                            let token = &token;
                            let binding_error = &binding_error;
                            let admission = &admission;
                            async move {
                                let mut token = token.lock().await;
                                match omnigraph_cluster::bind_maintenance_input(
                                    admission, &mut token, input,
                                )
                                .await
                                {
                                    Ok(()) => Ok(()),
                                    Err(error) => {
                                        let message = format!("[{}] {}", error.code, error.message);
                                        if error.code != "maintenance_result_capacity" {
                                            *binding_error.lock().await = Some(error);
                                        }
                                        Err(omnigraph::error::OmniError::manifest(message))
                                    }
                                }
                            }
                        },
                        |publication| {
                            let token = &token;
                            let binding_error = &binding_error;
                            let admission = &admission;
                            async move {
                                let mut token = token.lock().await;
                                match omnigraph_cluster::bind_maintenance_publication(
                                    admission,
                                    &mut token,
                                    publication,
                                )
                                .await
                                {
                                    Ok(()) => Ok(()),
                                    Err(error) => {
                                        *binding_error.lock().await = Some(error);
                                        Err(omnigraph::error::OmniError::manifest(
                                            "optimization publication could not be durably bound",
                                        ))
                                    }
                                }
                            }
                        },
                    )
                    .await;
                // A failed journal write is not an engine preflight refusal: the
                // remote outcome may be unknown even though graph effects did not
                // begin. Retain the invocation and contain this process.
                if let Some(error) = binding_error.into_inner() {
                    return Err(refusal(error));
                }
                let completion = match execution {
                    Ok(report) => MaintenanceCompletion::Success { report },
                    Err(failure) if !failure.effects_started => MaintenanceCompletion::Refused {
                        code: "optimization_preflight_refused".into(),
                        message: failure.error.to_string().chars().take(256).collect(),
                    },
                    Err(failure) => return Err(ApiError::from_omni(failure.error)),
                };
                let result = omnigraph_cluster::complete_maintenance(
                    &admission,
                    token.into_inner(),
                    completion,
                )
                .await
                .map_err(refusal)?;
                uncertain = false;
                tracing::info!("native optimization result retained");
                Ok(Output {
                    maintenance: MaintenanceLookup::Complete { result },
                    in_progress: false,
                })
            }
            .await;
            if uncertain {
                tracing::error!(
                    "native optimization unresolved; retain original identity and ownership"
                );
            }
            OwnedResult { result, uncertain }
        }
        .instrument(span),
    )?;
    let result = response.result();
    tokio::pin!(result);
    tokio::select! {
        biased;
        result = &mut result => result.map(|output| (StatusCode::OK, Json(output))),
        accepted = acceptance => match accepted {
            Ok(maintenance) => Ok((StatusCode::ACCEPTED, Json(Output { maintenance, in_progress: true }))),
            Err(_) => result.await.map(|output| (StatusCode::OK, Json(output))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_identity_trust_cannot_enable_source_only_maintenance_limits() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("maintenance.yaml");
        std::fs::write(
            &path,
            "max_tables: 10\nmax_fragments: 100\nmax_rows: 1000\nmax_source_bytes: 100000\nmax_files: 100\n",
        )
        .unwrap();
        assert!(load_limits_from_path(&path, false).is_ok());
        let error = load_limits_from_path(&path, true).unwrap_err();
        assert!(error.to_string().contains("resource capacity admission"));
    }

    #[test]
    fn documented_receipt_preserves_every_native_variant_and_field() {
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let mut invocation = omnigraph_cluster::MaintenanceInvocation {
            request: MaintenanceRequest {
                operation_id: id.into(),
                graph: "knowledge".into(),
                kind: omnigraph_cluster::MaintenanceKind::Optimize,
                limits: OptimizeLimits {
                    max_tables: 32,
                    max_fragments: 1000,
                    max_rows: 100000,
                    max_source_bytes: 134217728,
                    max_files: 2000,
                },
            },
            actor: "act-operator".into(),
            canonical_root: "file:///graph-root".into(),
            result_revision: 2,
            schema_contract: omnigraph::db::SchemaContractDigest {
                source_hash: "source".into(),
                schema_ir_hash: "ir".into(),
                schema_identity_domain: "domain".into(),
                schema_identity_version: 1,
            },
            input: None,
            publication: None,
            settlement: None,
        };
        let mut lookups = vec![MaintenanceLookup::Outstanding {
            operation_id: id.into(),
            graph: "knowledge".into(),
            invocation: invocation.clone(),
        }];
        let input = omnigraph::db::OptimizeInput {
            before_commit: "before".into(),
            report_max_bytes: 16384,
            schema_contract: invocation.schema_contract.clone(),
            source_tables: 2,
            source_fragments: 10,
            source_rows: 100,
            source_bytes: 2000,
            source_files: 20,
        };
        invocation.input = Some(input.clone());
        lookups.push(MaintenanceLookup::Outstanding {
            operation_id: id.into(),
            graph: "knowledge".into(),
            invocation: invocation.clone(),
        });
        let tables = serde_json::from_value(serde_json::json!([
            {"type_key":"node:Person","fragments_removed":4,"fragments_added":1,
             "committed":true,"skipped":null,"published_dataset_version":null,
             "lance_head_version":null,"pending_indexes":[{"type_key":"node:Person",
                "property":"embedding","reason":"no trainable vectors"}]},
            {"type_key":"edge:Knows","fragments_removed":0,"fragments_added":0,
             "committed":false,"skipped":"drift_needs_repair","published_dataset_version":4,
             "lance_head_version":5,"pending_indexes":[]}
        ]))
        .unwrap();
        let report = omnigraph::db::OptimizeReport {
            before_commit: input.before_commit.clone(),
            after_commit: "after".into(),
            tables,
            source_tables: input.source_tables,
            source_fragments: input.source_fragments,
            source_rows: input.source_rows,
            source_bytes: input.source_bytes,
            source_files: input.source_files,
        };
        invocation.publication = Some(omnigraph::db::OptimizePublication {
            before_manifest_version: 3,
            intent_nonce: "01ARZ3NDEKTSV4RRFFQ69G5FAW".into(),
            schema_contract: invocation.schema_contract.clone(),
            actor: Some(invocation.actor.clone()),
            created_at: 42,
            report: report.clone(),
        });
        invocation.settlement = Some(serde_json::from_value(serde_json::json!({
            "original_digest":"a".repeat(64),
            "lineage":{"graph_commit_id":"01ARZ3NDEKTSV4RRFFQ69G5FAX",
                "branch":null,"actor_id":"act-recovery","merged_parent_commit_id":null,"created_at":43}
        })).unwrap());
        lookups.push(MaintenanceLookup::Outstanding {
            operation_id: id.into(),
            graph: "knowledge".into(),
            invocation: invocation.clone(),
        });
        for completion in [
            MaintenanceCompletion::Success {
                report: report.clone(),
            },
            MaintenanceCompletion::Refused {
                code: "optimization_preflight_refused".into(),
                message: "source exceeds configured ceiling".into(),
            },
            MaintenanceCompletion::Interrupted {
                publication: omnigraph_cluster::MaintenancePublication::Unpublished,
            },
            MaintenanceCompletion::Interrupted {
                publication: omnigraph_cluster::MaintenancePublication::Published { report },
            },
        ] {
            lookups.push(MaintenanceLookup::Complete {
                result: omnigraph_cluster::MaintenanceResult {
                    operation_id: id.into(),
                    graph: "knowledge".into(),
                    invocation: invocation.clone(),
                    completion,
                },
            });
        }
        lookups.extend([
            MaintenanceLookup::ResultExpired {
                operation_id: id.into(),
            },
            MaintenanceLookup::NotRecorded {
                operation_id: id.into(),
            },
            MaintenanceLookup::IdentityMismatch {
                operation_id: id.into(),
            },
            MaintenanceLookup::DifferentLedger {
                operation_id: id.into(),
            },
        ]);
        for lookup in lookups {
            let native = serde_json::to_value(lookup).unwrap();
            let documented: receipt_schema::Lookup =
                serde_json::from_value(native.clone()).unwrap();
            assert_eq!(serde_json::to_value(documented).unwrap(), native);
        }
    }

    #[test]
    fn documented_request_preserves_native_shape_and_refuses_unknown_input() {
        let body = serde_json::json!({
            "operation_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV:1:01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "graph":"knowledge", "kind":"optimize",
            "limits":{"max_tables":32,"max_fragments":1000,"max_rows":100000,"max_source_bytes":134217728,"max_files":2000}
        });
        let native: MaintenanceRequest = serde_json::from_value::<SubmitRequest>(body.clone())
            .unwrap()
            .into();
        assert_eq!(serde_json::to_value(native).unwrap(), body);
        for malformed in [
            {
                let mut value = body.clone();
                value["actor"] = "caller-selected".into();
                value
            },
            {
                let mut value = body.clone();
                value["kind"] = "cleanup".into();
                value
            },
            {
                let mut value = body.clone();
                value["limits"]["unbounded"] = true.into();
                value
            },
            {
                let mut value = body;
                value["limits"].as_object_mut().unwrap().remove("max_files");
                value
            },
        ] {
            assert!(serde_json::from_value::<SubmitRequest>(malformed).is_err());
        }
    }

    #[tokio::test]
    async fn owner_turnover_keeps_the_gate_until_observation_is_cleared() {
        struct Dropped(std::sync::mpsc::Sender<()>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.send(()).unwrap();
            }
        }

        let runtime = Arc::new(MaintenanceRuntime::default());
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let permit = Arc::clone(&gate).lock_owned().await;
        let running = runtime.own("old".into(), permit);
        let (sent, received) = std::sync::mpsc::channel();
        // Hold observation so teardown cannot clear the old marker. A drop
        // signal starts teardown deterministically; the owner must still hold
        // the gate while waiting to clear its marker.
        let early_successor = {
            let current = runtime.running.lock().unwrap();
            let reservation = (Dropped(sent), running);
            let dropping = std::thread::spawn(move || drop(reservation));
            received
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            let early_successor = Arc::clone(&gate).try_lock_owned().ok();
            drop(current);
            dropping.join().unwrap();
            early_successor
        };
        assert!(
            early_successor.is_none(),
            "a successor could acquire the gate before the old owner cleared its marker"
        );
        assert!(!runtime.observation("old").1);
        let permit = Arc::clone(&gate).lock_owned().await;
        let successor = runtime.own("new".into(), permit);
        assert!(runtime.observation("new").1);
        drop(successor);
        assert!(!runtime.observation("new").1);
    }

    #[tokio::test]
    async fn observation_rechecks_complete_owner_turnover_and_bounds_churn() {
        let runtime = Arc::new(MaintenanceRuntime::default());
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let mut reads = 0;
        let (value, running) = runtime
            .observe("original", || {
                reads += 1;
                if reads == 1 {
                    // Both observations would say false without the generation:
                    // an entire accepted operation ran during the first read.
                    let owner = runtime.own(
                        "original".into(),
                        Arc::clone(&gate).try_lock_owned().unwrap(),
                    );
                    drop(owner);
                }
                std::future::ready(Ok(reads))
            })
            .await
            .unwrap();
        assert_eq!(value, 2);
        assert!(!running);

        let mut reads = 0;
        let result = runtime
            .observe("original", || {
                reads += 1;
                drop(runtime.own(
                    "original".into(),
                    Arc::clone(&gate).try_lock_owned().unwrap(),
                ));
                std::future::ready(Ok(()))
            })
            .await;
        assert!(
            result.is_err(),
            "owner churn must never make stale evidence terminal"
        );
        assert_eq!(reads, 3, "one request cannot observe indefinitely");
    }
}
