//! A single server-owned deployment runs under the server's existing root admission.
//! Input and graph outcomes belong to the cluster ledger; this controller owns
//! only request lifetime, graph admission and activation of serving bindings.

use std::collections::{BTreeMap, HashSet};
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
        state
            .routing
            .registry
            .snapshot_ref()
            .server_policy
            .as_deref(),
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
    let message = message.into();
    tracing::error!(reason = %message, "deployment requires reconciliation");
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
        && result.graphs.iter().all(|(id, outcome)| GraphId::try_from(id.as_str()).is_ok_and(|id| {
            let current = state.routing.registry.get(&GraphKey::cluster(id));
            if matches!(outcome, omnigraph_cluster::GraphDeploymentResult::Deleted { .. }) {
                matches!(current, RegistryLookup::Gone)
            } else {
                matches!(current, RegistryLookup::Ready(_))
            }
        })))
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
    for id in request.deployment.options().recreate_graphs.keys() {
        let key = GraphKey::cluster(
            GraphId::try_from(id.as_str())
                .map_err(|error| ApiError::bad_request(error.to_string()))?,
        );
        if matches!(
            state.routing.registry.get(&key),
            RegistryLookup::Ready(_) | RegistryLookup::Transitioning(_)
        ) {
            return Err(ApiError::conflict(format!(
                "graph {id} retains a runtime owner; restart after correcting the missing root before recreation"
            )));
        }
    }
    let retained = validate_serving_candidate(&state, &request.deployment, &owner, &caller).await?;
    let mut keys = Vec::new();
    for id in &affected {
        let key = GraphKey::cluster(
            GraphId::try_from(id.as_str())
                .map_err(|error| ApiError::bad_request(error.to_string()))?,
        );
        match state.routing.registry.get(&key) {
            RegistryLookup::Ready(_)
            | RegistryLookup::Transitioning(_)
            | RegistryLookup::Blocked(_) => keys.push(key),
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
        .prepare_deployment_transition(
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
    live.extend(retained);
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
                "deployment applied; activation contracts unavailable: {}",
                error.message
            ))
        })?;
    let snapshot =
        omnigraph_cluster::read_deployment_serving_snapshot(owner.canonical_root(), &affected)
            .await
            .map_err(|_| uncertain("deployment applied; achieved serving snapshot unavailable"))?;
    let settings = crate::settings::deployment_settings_from_snapshot(
        std::path::Path::new(owner.canonical_root()),
        snapshot,
    )
    .map_err(|error| {
        uncertain(format!(
            "deployment applied; achieved bindings invalid: {error}"
        ))
    })?;
    let crate::ServerConfigMode::Multi {
        graphs,
        server_policy,
        ..
    } = settings.mode;
    let server_policy =
        prepare_management_policy(server_policy).map_err(|error| uncertain(error.message))?;
    let mut handles = Vec::new();
    let mut unavailable = Vec::new();
    let mut removals: HashSet<_> = keys
        .iter()
        .filter(|key| !contracts.contains_key(key.graph_id.as_str()))
        .cloned()
        .collect();
    for graph in graphs {
        let id = graph.graph_id.clone();
        let contract = contracts
            .get(&id)
            .ok_or_else(|| uncertain("achieved schema contract is missing"))?
            .clone();
        let key = GraphKey::cluster(
            GraphId::try_from(id.as_str()).map_err(|error| uncertain(error.to_string()))?,
        );
        removals.remove(&key);
        let uri = omnigraph::storage::normalize_root_uri(&graph.uri)
            .map_err(|error| uncertain(format!("achieved graph URI is invalid: {error}")))?;
        let prepared = match crate::prepare_single_graph(graph, Some(contract.clone())) {
            Ok(prepared) => prepared,
            Err(error) if !live.contains_key(&id) => {
                unavailable.push(Arc::new(crate::BlockedGraph {
                    key,
                    uri,
                    policy: error.policy,
                    failure: error.failure,
                }));
                continue;
            }
            Err(error) => {
                return Err(uncertain(format!(
                    "applied runtime bindings invalid: {error}"
                )));
            }
        };
        if let Some(engine) = live.get(&id) {
            let policy = prepared
                .pending
                .policy
                .as_ref()
                .map(|policy| Arc::clone(policy) as Arc<dyn omnigraph_policy::PolicyChecker>);
            let engine = Arc::new(
                engine
                    .with_runtime_bindings(
                        policy,
                        prepared.cfg.embedding.clone().map(Arc::new),
                        prepared.cfg.external_blob_policy.clone(),
                    )
                    .map_err(|error| {
                        uncertain(format!("applied runtime bindings invalid: {error}"))
                    })?,
            );
            crate::verify_server_schema_contract(&engine, Some(&contract))
                .map_err(|error| uncertain(format!("applied graph contract changed: {error}")))?;
            let queries = crate::validate_and_attach(prepared.cfg.queries, &engine.catalog(), &id)
                .map_err(|error| {
                    uncertain(format!("applied stored queries are invalid: {error}"))
                })?;
            handles.push((
                Arc::new(crate::GraphHandle {
                    key,
                    uri: prepared.pending.uri.clone(),
                    engine,
                    policy: prepared.pending.policy.clone(),
                    queries,
                }),
                contract,
            ));
        } else {
            match crate::open_prepared_graph(prepared).await {
                Ok(opened) => handles.push((opened.handle, contract)),
                Err(error) => unavailable.push(Arc::new(crate::BlockedGraph {
                    key,
                    uri,
                    policy: error.policy,
                    failure: error.failure,
                })),
            }
        }
    }
    let fully_ready = unavailable.is_empty();
    transition
        .activate_deployment(handles, removals, unavailable, server_policy)
        .map_err(|error| uncertain(format!("deployment applied; activation refused: {error}")))?;
    if result.config_digest.is_none() || !fully_ready {
        // Serving follows the exact achieved schema/query/runtime projection.
        // Partial convergence never acknowledges the desired revision active.
        return Ok(DeploymentResponse {
            deployment: applied,
            active: false,
        });
    }
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
    state: &AppState,
    deployment: &CapturedDeployment,
    owner: &omnigraph_cluster::ClusterAdmission,
    caller: &DeploymentCaller,
) -> Result<BTreeMap<String, Arc<omnigraph::db::Omnigraph>>, ApiError> {
    let snapshot =
        omnigraph_cluster::preview_deployment_serving_snapshot(deployment, owner, caller)
            .await
            .map_err(refusal)?;
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
    let crate::ServerConfigMode::Multi {
        graphs,
        server_policy,
        ..
    } = settings.mode;
    prepare_management_policy(server_policy)?;
    state
        .routing
        .registry
        .validate_owner_capacity(
            &graphs
                .iter()
                .map(|graph| graph.uri.clone())
                .collect::<Vec<_>>(),
        )
        .map_err(|error| ApiError::conflict(error.to_string()))?;
    let mut retained = BTreeMap::new();
    for graph in graphs {
        let id = graph.graph_id.clone();
        let prepared = crate::prepare_single_graph(graph, None).map_err(|error| {
            ApiError::conflict(format!("deployment serving preparation failed: {error}"))
        })?;
        let retired = state
            .routing
            .registry
            .retained_engine(&prepared.pending.uri)
            .map_err(|error| ApiError::conflict(error.to_string()))?;
        if let Some(engine) = &retired {
            let confirmation = deployment.options().adopt_graphs.get(&id).ok_or_else(|| {
                ApiError::conflict(format!("graph {id} retains a deleted runtime owner; exact adoption or restart is required"))
            })?;
            let snapshot = engine
                .snapshot_of(omnigraph::db::ReadTarget::branch("main"))
                .await
                .map_err(|error| ApiError::from_omni(error.before_effect()))?;
            if engine.schema_contract_digest() != confirmation.contract
                || snapshot.graph_manifest_version() != confirmation.graph_manifest_version
            {
                return Err(ApiError::conflict(format!(
                    "graph {id} retained owner does not match the adoption confirmation"
                )));
            }
            retained.insert(id, Arc::clone(engine));
        }
        let engine = match state.routing.registry.get(&prepared.pending.key) {
            RegistryLookup::Ready(view) | RegistryLookup::Transitioning(view) => {
                Some(Arc::clone(&view.engine))
            }
            _ => retired,
        };
        if let Some(engine) = engine {
            let policy = prepared
                .pending
                .policy
                .as_ref()
                .map(|policy| Arc::clone(policy) as Arc<dyn omnigraph_policy::PolicyChecker>);
            let engine = engine
                .with_runtime_bindings(
                    policy,
                    prepared.cfg.embedding.clone().map(Arc::new),
                    prepared.cfg.external_blob_policy.clone(),
                )
                .map_err(|error| {
                    ApiError::conflict(format!("deployment runtime bindings are invalid: {error}"))
                })?;
            drop(engine);
        }
    }
    Ok(retained)
}

fn prepare_management_policy(
    source: Option<crate::PolicySource>,
) -> Result<Option<Arc<crate::PolicyEngine>>, ApiError> {
    match source {
        Some(crate::PolicySource::Inline(source)) => {
            Some(crate::PolicyEngine::load_cluster_from_source(&source))
        }
        Some(crate::PolicySource::File(path)) => Some(crate::PolicyEngine::load_cluster(&path)),
        None => None,
    }
    .transpose()
    .map_err(|error| ApiError::conflict(format!("invalid management policy: {error}")))
    .map(|policy| policy.map(Arc::new))
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
