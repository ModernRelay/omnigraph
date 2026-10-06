//! A single server-owned deployment runs under the server's existing root admission.
//! Input and graph outcomes belong to the cluster ledger; this controller owns
//! only request lifetime, graph admission and activation of serving bindings.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::{
    Extension, Json,
    extract::{Path, State},
};
use omnigraph_cluster::{
    CapturedDeployment, DeploymentActivation, DeploymentCaller, DeploymentLookup, DeploymentStatus,
    IdentityAuthorization,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use utoipa::ToSchema;

use crate::operations::OwnedResult;
use crate::workload::IngressLease;
use crate::{
    ApiError, AppState, AuthenticatedActor, GraphId, GraphKey, PolicyAction, PolicyRequest,
    RegistryLookup,
};

pub(crate) const REQUEST_BYTES: usize = omnigraph_cluster::MAX_BUNDLE_BYTES + 1024;
// One absolute transition deadline, including drain and activation. Expiry after
// effects never reopens old bindings. The process shutdown deadline remains authoritative.
const TRANSITION_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Default)]
pub(crate) struct DeploymentRuntime {
    gate: Arc<Mutex<()>>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeploymentRequest {
    deployment_id: String,
    #[schema(value_type = Object)]
    deployment: CapturedDeployment,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct DeploymentResponse {
    #[schema(value_type = Object)]
    deployment: DeploymentLookup,
    active: bool,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct DeploymentStatusResponse {
    #[schema(value_type = Object)]
    status: DeploymentStatus,
    active: bool,
}

fn caller(state: &AppState, actor: &AuthenticatedActor) -> Result<DeploymentCaller, ApiError> {
    if actor.data_claims().is_some() {
        return Err(ApiError::forbidden(
            "graph-scoped credentials cannot deploy cluster configuration",
        ));
    }
    crate::handlers::authorize_request(
        Some(actor),
        state.server_policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::ConfigManage,
            branch: None,
            target_branch: None,
        },
    )?;
    Ok(DeploymentCaller::AuthenticatedIdentity(
        IdentityAuthorization::authenticated(actor.actor_id_str()).map_err(refusal)?,
    ))
}

fn admission(state: &AppState) -> Result<&omnigraph_cluster::ClusterAdmission, ApiError> {
    state.cluster_admission.as_ref().ok_or_else(|| {
        ApiError::conflict("online deployment requires a v2 cluster with serving admission")
    })
}

fn refusal(error: omnigraph_cluster::Diagnostic) -> ApiError {
    let message = format!("[{}] {}", error.code, error.message);
    if matches!(
        error.code.as_str(),
        "policy_denied" | "cluster_policy_required" | "graph_policy_required" | "identity_invalid"
    ) {
        ApiError::forbidden(message)
    } else {
        ApiError::conflict(message)
    }
}

fn uncertain(message: impl Into<String>) -> ApiError {
    ApiError::internal(message)
}

fn active_result(
    state: &AppState,
    lookup: Option<&DeploymentLookup>,
    current_revision: u64,
) -> bool {
    let Some(owner) = state.cluster_admission.as_ref() else {
        return false;
    };
    if state.operations.snapshot().closed {
        return false;
    }
    matches!(lookup, Some(DeploymentLookup::Complete { result })
        if result.result_revision == current_revision
            && result.activation.as_ref().is_some_and(|active|
                active.process_incarnation == owner.lock_id()
                && active.result_revision == current_revision)
            && result.graphs.keys().all(|id| GraphId::try_from(id.as_str()).is_ok_and(|id|
                matches!(state.routing.registry.get(&GraphKey::cluster(id)), RegistryLookup::Ready(_)))))
}

#[utoipa::path(
    get, path = "/cluster/deployments", tag = "cluster", operation_id = "deployment_status",
    responses((status = 200, body = DeploymentStatusResponse), (status = 403, body = crate::api::ErrorOutput)),
    security(("bearer_token" = []))
)]
pub(crate) async fn status(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
) -> Result<Json<DeploymentStatusResponse>, ApiError> {
    lookup_status(&state, &actor, None).await.map(Json)
}

#[utoipa::path(
    get, path = "/cluster/deployments/{id}", tag = "cluster", operation_id = "deployment_lookup",
    params(("id" = String, Path, description = "Original deployment identity")),
    responses((status = 200, body = DeploymentStatusResponse), (status = 403, body = crate::api::ErrorOutput)),
    security(("bearer_token" = []))
)]
pub(crate) async fn lookup(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
    Path(id): Path<String>,
) -> Result<Json<DeploymentStatusResponse>, ApiError> {
    lookup_status(&state, &actor, Some(&id)).await.map(Json)
}

async fn lookup_status(
    state: &AppState,
    actor: &AuthenticatedActor,
    id: Option<&str>,
) -> Result<DeploymentStatusResponse, ApiError> {
    let caller = caller(state, actor)?;
    let owner = admission(state)?;
    let status = omnigraph_cluster::deployment_status(owner.canonical_root(), id, &caller)
        .await
        .map_err(refusal)?;
    let active = active_result(state, status.lookup.as_ref(), status.result_revision);
    Ok(DeploymentStatusResponse { status, active })
}

#[utoipa::path(
    post, path = "/cluster/deployments", tag = "cluster", operation_id = "deployment_apply",
    request_body = DeploymentRequest,
    responses((status = 200, body = DeploymentResponse), (status = 400, body = crate::api::ErrorOutput),
        (status = 403, body = crate::api::ErrorOutput), (status = 409, body = crate::api::ErrorOutput),
        (status = 413, body = crate::api::ErrorOutput), (status = 503, body = crate::api::ErrorOutput)),
    security(("bearer_token" = []))
)]
pub(crate) async fn apply(
    State(state): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
    Extension(ingress): Extension<IngressLease>,
    request: Result<Json<DeploymentRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<DeploymentResponse>, ApiError> {
    let caller = caller(&state, &actor)?;
    let Json(request) = request.map_err(|error| ApiError::bad_request(error.body_text()))?;
    let owner = admission(&state)?.clone();
    if owner.canonical_root() != request.deployment.canonical_root() {
        return Err(ApiError::conflict(
            "deployment root differs from the serving cluster",
        ));
    }
    if request.deployment_id.is_empty() || request.deployment_id.len() > 75 {
        return Err(ApiError::bad_request(
            "a canonical deployment_id is required",
        ));
    }
    let gate = state
        .deployments
        .gate
        .clone()
        .try_lock_owned()
        .map_err(|_| {
            ApiError::conflict("a deployment is already running; observe its original identity")
        })?;
    let reservation = state
        .workload
        .try_admit(&actor.actor_id, REQUEST_BYTES as u64)
        .map_err(ApiError::from_workload_reject)?;
    let operation_state = state.clone();
    state
        .operations
        .submit((gate, reservation, ingress), async move {
            let result = execute(operation_state, owner, caller, request).await;
            OwnedResult::from(result)
        })?
        .result()
        .await
        .map(Json)
}

async fn execute(
    state: AppState,
    owner: omnigraph_cluster::ClusterAdmission,
    caller: DeploymentCaller,
    request: DeploymentRequest,
) -> Result<DeploymentResponse, ApiError> {
    // An original identity is observation-only. Validate immutable input through
    // the shared executor before returning it, without closing graph admission.
    let status = omnigraph_cluster::deployment_status(
        owner.canonical_root(),
        Some(&request.deployment_id),
        &caller,
    )
    .await
    .map_err(refusal)?;
    if !matches!(status.lookup, Some(DeploymentLookup::NotRecorded)) {
        let mut effects_started = false;
        let deployment = omnigraph_cluster::apply_captured_deployment(
            &request.deployment,
            Some(&request.deployment_id),
            &caller,
            &owner,
            &BTreeMap::new(),
            |_, _, _| {},
            &mut effects_started,
        )
        .await
        .map_err(refusal)?;
        return Ok(DeploymentResponse {
            active: active_result(&state, Some(&deployment), status.result_revision),
            deployment,
        });
    }
    // Validate and authorize before pausing healthy graphs. No client-supplied
    // path is opened for writes; the server's root admission is authoritative.
    let affected =
        omnigraph_cluster::deployment_affected_graphs(&request.deployment, &owner, &caller)
            .await
            .map_err(refusal)?;
    validate_serving_candidate(&request.deployment, &owner, &caller, &affected).await?;
    let mut keys = Vec::new();
    for id in &affected {
        let key = GraphKey::cluster(
            GraphId::try_from(id.as_str())
                .map_err(|error| ApiError::bad_request(error.to_string()))?,
        );
        match state.routing.registry.get(&key) {
            RegistryLookup::Ready(_) => keys.push(key),
            RegistryLookup::Gone => {} // a new graph becomes visible only after achieved publication
            _ => {
                return Err(ApiError::conflict(format!(
                    "graph {id} is unavailable for deployment"
                )));
            }
        }
    }
    let transition = state
        .routing
        .registry
        .prepare_transition(
            &state.operations,
            &keys,
            Instant::now() + TRANSITION_TIMEOUT,
        )
        .map_err(|error| ApiError::conflict(error.to_string()))?
        .close()
        .map_err(|error| ApiError::conflict(error.to_string()))?;
    if let Err(error) = transition.wait_requests().await {
        return Err(abort_before_effects(
            &state,
            transition,
            ApiError::conflict(error.to_string()),
        ));
    }
    let drained = match transition.engines() {
        Ok(engines) => engines,
        Err(error) => {
            return Err(abort_before_effects(
                &state,
                transition,
                ApiError::conflict(error.to_string()),
            ));
        }
    };
    let mut live: BTreeMap<_, _> = state
        .routing
        .registry
        .list()
        .into_iter()
        .map(|view| (view.key.graph_id.to_string(), Arc::clone(&view.engine)))
        .collect();
    live.extend(
        drained
            .into_iter()
            .map(|(key, engine)| (key.graph_id.to_string(), engine)),
    );
    let mut effects_started = false;
    let applied = omnigraph_cluster::apply_captured_deployment(
        &request.deployment,
        Some(&request.deployment_id),
        &caller,
        &owner,
        &live,
        |_, _, _| {},
        &mut effects_started,
    )
    .await;
    let applied = match applied {
        Ok(applied) => applied,
        Err(error) if !effects_started => {
            return Err(abort_before_effects(&state, transition, refusal(error)));
        }
        Err(error) => {
            return Err(uncertain(format!(
                "deployment {} requires reconciliation: [{}] {}",
                request.deployment_id, error.code, error.message
            )));
        }
    };
    let DeploymentLookup::Complete { result } = &applied else {
        // Lookup is never execution permission. A recorded unresolved operation
        // cannot activate a candidate prepared for this request.
        transition
            .resume_same_views()
            .map_err(|error| uncertain(error.to_string()))?;
        return Ok(DeploymentResponse {
            active: active_result(&state, Some(&applied), status.result_revision),
            deployment: applied,
        });
    };
    let contracts = omnigraph_cluster::applied_deployment_contracts(&owner, result.result_revision)
        .await
        .map_err(|error| {
            uncertain(format!(
                "deployment applied; activation contract unavailable: {}",
                error.message
            ))
        })?;
    let snapshot = omnigraph_cluster::read_serving_snapshot_from_storage(owner.canonical_root())
        .await
        .map_err(|_| {
            uncertain("deployment applied; achieved serving snapshot could not be loaded")
        })?;
    let settings = crate::settings::settings_from_snapshot(
        std::path::Path::new(owner.canonical_root()),
        None,
        false,
        false,
        snapshot,
    )
    .map_err(|error| {
        uncertain(format!(
            "deployment applied; serving configuration is invalid: {error}"
        ))
    })?;
    let crate::ServerConfigMode::Multi { graphs, .. } = settings.mode;
    let mut bindings = HashMap::new();
    let mut additions = Vec::new();
    for graph in graphs {
        if !affected.contains(&graph.graph_id) {
            continue;
        }
        let contract = contracts
            .get(&graph.graph_id)
            .ok_or_else(|| uncertain("achieved schema contract is missing"))?
            .clone();
        let key = GraphKey::cluster(
            GraphId::try_from(graph.graph_id.as_str())
                .map_err(|error| uncertain(error.to_string()))?,
        );
        if keys.contains(&key) {
            if graph.startup_failure.is_some() {
                return Err(uncertain("applied graph has invalid serving bindings"));
            }
            bindings.insert(key, (contract, graph.queries));
        } else {
            let prepared =
                crate::prepare_single_graph(graph, Some(contract.clone())).map_err(|error| {
                    uncertain(format!("new graph serving preparation failed: {error}"))
                })?;
            let opened = crate::open_prepared_graph(prepared)
                .await
                .map_err(|error| uncertain(format!("new graph serving open failed: {error}")))?;
            additions.push((opened.handle, contract));
        }
    }
    transition
        .activate(bindings, additions)
        .map_err(|error| uncertain(format!("deployment applied; activation refused: {error}")))?;
    let activated = omnigraph_cluster::record_deployment_activation(
        owner.canonical_root(),
        &request.deployment_id,
        &owner,
        DeploymentActivation {
            process_incarnation: owner.lock_id().to_owned(),
            result_revision: result.result_revision,
            config_digest: result
                .config_digest
                .clone()
                .ok_or_else(|| uncertain("applied deployment did not converge"))?,
        },
        &contracts,
    )
    .await
    .map_err(|error| {
        uncertain(format!(
            "deployment activated; activation record is uncertain: {}",
            error.message
        ))
    })?;
    let deployment = DeploymentLookup::Complete { result: activated };
    Ok(DeploymentResponse {
        active: active_result(&state, Some(&deployment), result.result_revision),
        deployment,
    })
}

/// Resolve runtime-only settings using the same serving projection and graph
/// preparation as startup, before accepting any deployment or graph effect.
/// Unaffected bindings remain installed and are not reconfigured by this apply.
async fn validate_serving_candidate(
    deployment: &CapturedDeployment,
    owner: &omnigraph_cluster::ClusterAdmission,
    caller: &DeploymentCaller,
    affected: &[String],
) -> Result<(), ApiError> {
    let mut snapshot =
        omnigraph_cluster::preview_deployment_serving_snapshot(deployment, owner, caller)
            .await
            .map_err(refusal)?;
    snapshot
        .graphs
        .retain(|graph| affected.contains(&graph.graph_id));
    snapshot
        .quarantined_graphs
        .retain(|graph| affected.contains(&graph.graph_id));
    snapshot
        .queries
        .retain(|query| affected.contains(&query.graph_id));
    snapshot
        .applied_graphs
        .retain(|graph| affected.contains(graph));
    let settings = crate::settings::settings_from_snapshot(
        std::path::Path::new(owner.canonical_root()),
        None,
        false,
        true,
        snapshot,
    )
    .map_err(|error| {
        ApiError::conflict(format!(
            "deployment serving configuration is invalid: {error}"
        ))
    })?;
    let crate::ServerConfigMode::Multi { graphs, .. } = settings.mode;
    for graph in graphs {
        crate::prepare_single_graph(graph, None).map_err(|error| {
            ApiError::conflict(format!("deployment serving preparation failed: {error}"))
        })?;
    }
    Ok(())
}

fn abort_before_effects(
    state: &AppState,
    transition: crate::serving::GraphTransition,
    refusal: ApiError,
) -> ApiError {
    match transition.abort_before_effects() {
        Ok(_) => refusal,
        Err(_) if state.operations.snapshot().closed => ApiError::admission_closed(),
        Err(error) => uncertain(format!(
            "pre-effect refusal could not restore coherent serving: {error}"
        )),
    }
}
