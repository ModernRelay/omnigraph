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
    CapturedDeployment, DeploymentCaller, DeploymentLookup, DeploymentStatus,
    GraphDeploymentResult, IdentityAuthorization,
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
// Bound only the pre-effect drain. The owned executor retains completion and
// activation after drainage; the process shutdown deadline remains authoritative.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);

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

/// Installed together with graph bindings; durable completion stays in the
/// cluster ledger, while activation belongs only to this process incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActiveDeployment {
    canonical_root: String,
    process_incarnation: String,
    id: String,
    input_digest: String,
    result_revision: u64,
    config_digest: String,
    /// Installed scope: true requires a ready binding, false requires absence.
    /// Boot expands the ready set to its complete captured inventory.
    graphs: HashMap<GraphKey, bool>,
}

impl ActiveDeployment {
    pub(crate) fn new(
        owner: &omnigraph_cluster::ClusterAdmission,
        result: &omnigraph_cluster::DeploymentResult,
    ) -> Option<Self> {
        result.converged.then_some(())?;
        Some(Self {
            canonical_root: owner.canonical_root().to_owned(),
            process_incarnation: owner.lock_id().to_owned(),
            id: result.id.clone(),
            input_digest: result.input_digest.clone(),
            result_revision: result.result_revision,
            config_digest: result.config_digest.clone()?,
            graphs: result
                .graphs
                .iter()
                .map(|(id, outcome)| {
                    Some((
                        GraphKey::cluster(GraphId::try_from(id.as_str()).ok()?),
                        !matches!(outcome, GraphDeploymentResult::Deleted { .. }),
                    ))
                })
                .collect::<Option<_>>()?,
        })
    }
}

/// Seed only the admission's exact boot input. Loading/blocked entries cannot
/// report it active; every installed view must first pass startup verification.
pub(crate) fn initialize_boot_activation(state: &AppState) {
    let activation = state.cluster_admission.as_ref().and_then(|owner| {
        owner
            .serving_deployment()
            .filter(|result| result.config_digest == state.witness.booted_serving_digest)
            .and_then(|result| ActiveDeployment::new(owner, result))
    });
    let activation = activation.map(|mut activation| {
        activation.graphs.extend(
            state
                .routing
                .registry
                .snapshot_ref()
                .graphs
                .keys()
                .cloned()
                .map(|key| (key, true)),
        );
        activation
    });
    state.routing.registry.initialize_deployment(activation);
}

fn active_result(
    state: &AppState,
    lookup: Option<&DeploymentLookup>,
    current_revision: u64,
) -> bool {
    let Some(owner) = state.cluster_admission.as_ref() else {
        return false;
    };
    let Some(DeploymentLookup::Complete { result }) = lookup else {
        return false;
    };
    if !result.converged || result.result_revision != current_revision {
        return false;
    }
    state
        .operations
        .while_open(|| {
            let snapshot = state.routing.registry.snapshot_ref();
            Ok::<_, ApiError>(snapshot.deployment.as_ref().is_some_and(|active| {
                active.canonical_root == owner.canonical_root()
                    && active.process_incarnation == owner.lock_id()
                    && active.id == result.id
                    && active.input_digest == result.input_digest
                    && active.result_revision == result.result_revision
                    && Some(&active.config_digest) == result.config_digest.as_ref()
                    && active.graphs.iter().all(|(key, present)| {
                        if *present {
                            matches!(snapshot.graphs.get(key), Some(crate::GraphEntry::Ready(view)) if view.contract_is_current())
                        } else {
                            !snapshot.graphs.contains_key(key)
                        }
                    })
            }))
        })
        .unwrap_or(false)
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
    let preview =
        omnigraph_cluster::prepare_deployment_preview(&request.deployment, &owner, &caller)
            .await
            .map_err(refusal)?;
    let affected = preview.affected_graphs;
    validate_serving_candidate(&state, &owner, preview.serving)?;
    let desired = request.deployment.graph_ids();
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
            RegistryLookup::Gone if desired.contains(id) => {} // new graph
            RegistryLookup::Gone => {
                return Err(ApiError::conflict(format!(
                    "graph {id} is absent from the serving registry; restart before deletion"
                )));
            }
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
        .prepare_deployment_transition(&state.operations, &keys, Instant::now() + DRAIN_TIMEOUT)
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
    if let Err(error) = transition.retain_deployment_completion() {
        return Err(abort_before_effects(
            &state,
            transition,
            ApiError::conflict(error.to_string()),
        ));
    }
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
    for graph in graphs {
        let id = graph.graph_id.clone();
        let contract = contracts
            .get(&id)
            .ok_or_else(|| uncertain("achieved schema contract is missing"))?
            .clone();
        let key = GraphKey::cluster(
            GraphId::try_from(id.as_str()).map_err(|error| uncertain(error.to_string()))?,
        );
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
    let activation = if unavailable.is_empty() {
        ActiveDeployment::new(&owner, result)
    } else {
        None
    };
    let deleted = result
        .graphs
        .iter()
        .filter_map(|(id, result)| match result {
            GraphDeploymentResult::Deleted { contract } => Some((id, contract.clone())),
            _ => None,
        })
        .map(|(id, contract)| {
            Ok((
                GraphKey::cluster(
                    GraphId::try_from(id.as_str()).map_err(|error| uncertain(error.to_string()))?,
                ),
                contract,
            ))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    transition
        .activate_deployment(handles, unavailable, deleted, server_policy, activation)
        .map_err(|error| uncertain(format!("deployment applied; activation refused: {error}")))?;
    Ok(DeploymentResponse {
        active: active_result(&state, Some(&applied), result.result_revision),
        deployment: applied,
    })
}

/// Resolve runtime-only settings using the same serving projection and graph
/// preparation as startup, before accepting any deployment or graph effect.
/// Unaffected bindings remain installed and are not reconfigured by this apply.
fn validate_serving_candidate(
    state: &AppState,
    owner: &omnigraph_cluster::ClusterAdmission,
    snapshot: omnigraph_cluster::ServingSnapshot,
) -> Result<(), ApiError> {
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
    for graph in graphs {
        let prepared = crate::prepare_single_graph(graph, None).map_err(|error| {
            ApiError::conflict(format!("deployment serving preparation failed: {error}"))
        })?;
        let engine = match state.routing.registry.get(&prepared.pending.key) {
            RegistryLookup::Ready(view) | RegistryLookup::Transitioning(view) => {
                Some(Arc::clone(&view.engine))
            }
            _ => None,
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
    Ok(())
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
