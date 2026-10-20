//! HTTP route handlers, the bearer-auth middleware, per-request
//! authorization, and the cluster-prefix OpenAPI rewrite (moved
//! verbatim from lib.rs in the modularization).

use super::*;
use crate::api::{GraphAvailability, GraphAvailabilityAction};
use crate::operations::OwnedResult;
use crate::redacted_cause::RedactedCause;
use crate::registry::{GraphEntry, RegistryCapture, StartupFailure};
use crate::serving::GraphRequest;
use crate::workload::{AdmissionGuard, IngressLease};
use futures::StreamExt;
use omnigraph::Session;
use omnigraph::db::{BranchCreated, MergeResult, SnapshotId};
use omnigraph::settings::{SettingId, SettingValue};
use omnigraph_compiler::Literal;
use omnigraph_compiler::query::ast::{
    BranchStmt, EmptyFile, FileBody, MutationWriteMode, QueryDecl, QueryFile, UnattachedBase,
};
use tracing::Instrument;

/// Inputs, actor admission and execution leave the request together. Dropping
/// this waiter never cancels the registered write.
async fn owned_write<T, F>(
    state: &AppState,
    admission: AdmissionGuard,
    ingress: IngressLease,
    graph: GraphRequest,
    operation: F,
) -> std::result::Result<T, ApiError>
where
    T: Send + 'static,
    F: std::future::Future<Output = std::result::Result<T, ApiError>> + Send + 'static,
{
    state
        .operations
        .submit((admission, ingress, graph), async move {
            operation.await.into()
        })?
        .result()
        .await
}

pub(crate) mod catalogue;
mod dispatch;

use dispatch::{
    CommitControlOutcome, Door, ReadDispatch, branch_read_body, classify, commit_control_body,
    commit_write_at_read_door, control_write_at_read_door, explain_at_write_door,
    load_at_read_door, read_at_write_door, refuse_empty_file, refuse_explain,
    refuse_process_settings, refuse_statement_envelope, refuse_wrong_door, run_branch_statement,
    run_commit_statement, run_load_statement, session_with_prefix, show_at_write_door,
};

/// Liveness probe.
///
/// Returns server status and version. Unauthenticated; safe to call from any
/// caller. Use this to confirm the server is reachable before invoking other
/// endpoints.
#[utoipa::path(
    get,
    path = "/healthz",
    tag = "health",
    operation_id = "health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthOutput),
    ),
)]
pub(crate) async fn server_health() -> Json<HealthOutput> {
    Json(HealthOutput {
        status: "ok".to_string(),
        version: crate::api::build::reported_version(SERVER_VERSION),
        internal_schema_version: SERVER_INTERNAL_SCHEMA_VERSION,
        source_version: crate::api::build::SOURCE_COMMIT.map(str::to_string),
    })
}

/// Readiness witness (RFC 0049).
///
/// Unauthenticated, and therefore minimal: it reports whether this replica
/// is serving or draining, the applied `config_digest` it booted from, the
/// ledger revision and CAS it read, and registry/ready/loading/blocked counts. Graph
/// ids stay behind authenticated catalog endpoints. Partial availability is
/// ready but degraded after loading finishes; loading, no available graph or
/// shutdown returns 503. A valid
/// empty registry is ready. `/healthz` reports liveness independently.
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "health",
    operation_id = "readiness",
    responses(
        (status = 200, description = "Serving", body = ReadinessOutput),
        (status = 503, description = "Graphs loading, no graph available or stopping", body = ReadinessOutput),
    ),
)]
pub(crate) async fn server_ready(
    State(state): State<AppState>,
) -> (StatusCode, Json<ReadinessOutput>) {
    let draining = state.draining.load(std::sync::atomic::Ordering::SeqCst)
        || state.operations.snapshot().closed;
    let entries = state.routing().registry.entries();
    let served_graph_count = entries.len();
    let ready_graph_count = entries
        .iter()
        .filter(|entry| matches!(entry, GraphEntry::Ready(_)))
        .count();
    let loading_graph_count = entries
        .iter()
        .filter(|entry| matches!(entry, GraphEntry::Loading(_)))
        .count();
    let blocked_graph_count = served_graph_count - ready_graph_count - loading_graph_count;
    let ready =
        !draining && loading_graph_count == 0 && (served_graph_count == 0 || ready_graph_count > 0);
    let output = ReadinessOutput {
        ready,
        status: if draining {
            "draining"
        } else if loading_graph_count > 0 {
            "loading"
        } else if !ready {
            "blocked"
        } else if blocked_graph_count > 0 {
            "degraded"
        } else {
            "serving"
        }
        .to_string(),
        booted_serving_digest: state.witness.booted_serving_digest.clone(),
        state_revision: state.witness.state_revision,
        state_cas: state.witness.state_cas.clone(),
        served_graph_count,
        ready_graph_count,
        loading_graph_count,
        blocked_graph_count,
        shutdown_grace_seconds: state.shutdown_grace.as_secs(),
    };
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(output))
}

#[utoipa::path(
    get,
    path = "/graphs",
    tag = "management",
    operation_id = "listGraphs",
    responses(
        (status = 200, description = "List of registered graphs", body = GraphListResponse),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// List every graph currently registered with this server.
///
/// Cedar-gated by the server-level policy via the `graph_list` action
/// against `Omnigraph::Server::"root"`.
///
/// Order: alphabetical by `graph_id` (server-sorted so clients see
/// deterministic output across requests).
pub(crate) async fn server_graphs_list(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> std::result::Result<Json<GraphListResponse>, ApiError> {
    let snapshot = state.routing().registry.snapshot_ref();

    // Capture management authorization and inventory from one activated
    // registry snapshot. When no server policy is
    // configured, `authorize_request_server` falls through to the MR-723
    // default-deny semantics (every non-Read action denied for an
    // authenticated actor). `GraphList` is not `Read`, so without a server
    // policy the request gets 403 — which is the right default (don't leak
    // the registry until the operator explicitly authorizes it).
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        snapshot.server_policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::GraphList,
            branch: None,
            target_branch: None,
        },
    )?;

    let stopping = state.draining.load(std::sync::atomic::Ordering::SeqCst)
        || state.operations.snapshot().closed;
    let mut graphs: Vec<GraphInfo> = snapshot
        .graphs
        .values()
        .map(|entry| {
            let failure = match &entry {
                GraphEntry::Loading(_) | GraphEntry::Ready(_) | GraphEntry::Transitioning(_) => {
                    None
                }
                GraphEntry::Blocked(graph) => Some(graph.failure),
            };
            let available = !stopping && matches!(entry, GraphEntry::Ready(_));
            GraphInfo {
                graph_id: entry.key().graph_id.as_str().to_string(),
                uri: entry.uri().to_string(),
                state: if stopping {
                    GraphAvailability::Stopping
                } else {
                    match &entry {
                        GraphEntry::Loading(_) => GraphAvailability::Loading,
                        GraphEntry::Ready(_) => GraphAvailability::Ready,
                        GraphEntry::Transitioning(_) => GraphAvailability::Transitioning,
                        GraphEntry::Blocked(_) => GraphAvailability::Blocked,
                    }
                },
                read_available: available,
                write_available: available,
                failure,
                action: if stopping {
                    GraphAvailabilityAction::WaitForRestart
                } else {
                    match &entry {
                        GraphEntry::Loading(_) => GraphAvailabilityAction::WaitForStartup,
                        GraphEntry::Ready(_) => GraphAvailabilityAction::None,
                        GraphEntry::Transitioning(_) => GraphAvailabilityAction::WaitForTransition,
                        GraphEntry::Blocked(_) => GraphAvailabilityAction::ApplyCorrectionOrRestart,
                    }
                },
            }
        })
        .collect();
    graphs.sort_by(|a, b| a.graph_id.cmp(&b.graph_id));
    Ok(Json(GraphListResponse { graphs }))
}

#[utoipa::path(
    get,
    path = "/graphs/discovery",
    tag = "management",
    operation_id = "discoverGraphs",
    responses(
        (status = 200, description = "Authenticated minimal graph inventory", body = GraphDiscoveryResponse),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Identity credential required", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
pub(crate) async fn server_graphs_discovery(
    State(state): State<AppState>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> std::result::Result<Json<GraphDiscoveryResponse>, ApiError> {
    let actor = actor.ok_or_else(|| ApiError::unauthorized("missing bearer token"))?;
    if !actor.is_identity() {
        return Err(ApiError::forbidden(
            "graph discovery requires an admitted identity credential",
        ));
    }
    // Identity comes from the complete startup registry. Never scan storage or
    // include per-graph status, roots, diagnostics, schema, or policy contents.
    let ids: std::collections::BTreeSet<String> = state
        .routing()
        .registry
        .entries()
        .into_iter()
        .map(|entry| entry.key().graph_id.as_str().to_owned())
        .collect();
    Ok(Json(GraphDiscoveryResponse {
        graphs: ids
            .into_iter()
            .map(|graph_id| GraphDiscoveryEntry {
                display_name: graph_id.clone(),
                graph_id,
            })
            .collect(),
    }))
}

pub(crate) async fn server_openapi(
    State(state): State<AppState>,
) -> Json<utoipa::openapi::OpenApi> {
    // `served_openapi` is the single nesting source — the protected
    // routes always live under `/graphs/{graph_id}/...` (public/management
    // paths `/healthz`, `/graphs` stay flat). Building from it here means
    // the runtime spec and the committed `openapi.json` share one nesting
    // pass and can't drift.
    let mut doc = crate::served_openapi();
    if !state.requires_bearer_auth() {
        strip_security(&mut doc);
    }
    Json(doc)
}

/// Path prefix used to namespace per-graph routes in multi mode.
/// Kept in sync with the `Router::nest(...)` invocation in `build_app`.
const CLUSTER_PATH_PREFIX: &str = "/graphs/{graph_id}";

/// Operation-id prefix applied to every cloned cluster operation.
/// Decision 7 in the implementation plan — keeps operation IDs unique
/// across the spec when both flat and nested variants ever appear in
/// the same generation pass.
const CLUSTER_OPERATION_ID_PREFIX: &str = "cluster_";

/// Paths that stay flat in every server mode (public or server-level,
/// no per-graph dependency). Update this list when adding new
/// always-flat endpoints. `/graphs` is the management enumeration —
/// it lives at the root in both single mode (405) and multi mode, and
/// must never be rewritten to `/graphs/{graph_id}/graphs`.
const ALWAYS_FLAT_PATHS: &[&str] = &[
    "/healthz",
    "/readyz",
    "/graphs",
    "/graphs/discovery",
    "/cluster/plan",
    "/cluster/deployments",
    "/cluster/deployments/{id}",
    "/.well-known/oauth-protected-resource",
    "/.well-known/oauth-protected-resource/{resource_path}",
];

/// In multi-mode `server_openapi`, every protected path-item is
/// reattached under the cluster prefix. Operation IDs gain the
/// `cluster_` prefix so SDK generators don't collide if/when both
/// surfaces are merged. Every rewritten operation also declares the
/// required `{graph_id}` path parameter so the served OpenAPI document
/// remains internally valid.
///
/// Removing the flat protected paths matches the runtime router —
/// in multi mode, requests to `/snapshot` etc. return 404, so the
/// spec must agree.
pub(crate) fn nest_paths_under_cluster_prefix(doc: &mut utoipa::openapi::OpenApi) {
    let original = std::mem::take(&mut doc.paths.paths);
    let mut rewritten = std::collections::BTreeMap::new();
    for (path, mut item) in original {
        if ALWAYS_FLAT_PATHS.contains(&path.as_str()) {
            rewritten.insert(path, item);
            continue;
        }
        rename_operation_ids(&mut item, CLUSTER_OPERATION_ID_PREFIX);
        add_cluster_graph_id_parameter(&mut item);
        let new_path = format!("{CLUSTER_PATH_PREFIX}{path}");
        rewritten.insert(new_path, item);
    }
    doc.paths.paths = rewritten;
}

pub(crate) fn add_cluster_graph_id_parameter(item: &mut utoipa::openapi::PathItem) {
    for op in path_item_operations_mut(item) {
        let parameters = op.parameters.get_or_insert_with(Vec::new);
        let has_graph_id = parameters
            .iter()
            .any(|param| param.name == "graph_id" && param.parameter_in == ParameterIn::Path);
        if !has_graph_id {
            parameters.insert(0, graph_id_path_parameter());
        }
    }
}

pub(crate) fn graph_id_path_parameter() -> Parameter {
    let mut parameter = Parameter::new("graph_id");
    parameter.parameter_in = ParameterIn::Path;
    parameter.description = Some("Graph id to route the request to.".to_string());
    parameter.schema = Some(Object::with_type(Type::String).into());
    parameter
}

/// Prefix every operation_id in this PathItem with `prefix`.
pub(crate) fn rename_operation_ids(item: &mut utoipa::openapi::PathItem, prefix: &str) {
    for op in path_item_operations_mut(item) {
        if let Some(id) = op.operation_id.as_deref() {
            op.operation_id = Some(format!("{prefix}{id}"));
        }
    }
}

pub(crate) fn path_item_operations_mut(
    item: &mut utoipa::openapi::PathItem,
) -> impl Iterator<Item = &mut utoipa::openapi::path::Operation> {
    [
        item.get.as_mut(),
        item.post.as_mut(),
        item.put.as_mut(),
        item.delete.as_mut(),
        item.options.as_mut(),
        item.head.as_mut(),
        item.patch.as_mut(),
        item.trace.as_mut(),
    ]
    .into_iter()
    .flatten()
}

pub(crate) fn strip_security(doc: &mut utoipa::openapi::OpenApi) {
    if let Some(components) = doc.components.as_mut() {
        components.security_schemes.clear();
    }
    for path_item in doc.paths.paths.values_mut() {
        for op in [
            path_item.get.as_mut(),
            path_item.post.as_mut(),
            path_item.put.as_mut(),
            path_item.delete.as_mut(),
            path_item.options.as_mut(),
            path_item.head.as_mut(),
            path_item.patch.as_mut(),
            path_item.trace.as_mut(),
        ]
        .into_iter()
        .flatten()
        {
            op.security = None;
        }
    }
}

pub(crate) async fn require_bearer_auth(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, ApiError> {
    // Request extensions supplied by an embedder are projections, never an
    // authentication result for this request. Rebuild both from its credential.
    request.extensions_mut().remove::<ResolvedActor>();
    request.extensions_mut().remove::<AuthenticatedActor>();
    if !state.requires_bearer_auth() {
        return Ok(next.run(request).await);
    }

    if request.headers().get_all(AUTHORIZATION).iter().count() != 1 {
        return Err(ApiError::unauthorized("one bearer credential is required"));
    }

    let Some(header) = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(ApiError::unauthorized("missing bearer token"));
    };

    let Some(provided_token) = header.strip_prefix("Bearer ") else {
        return Err(ApiError::unauthorized("missing bearer token"));
    };

    let Some(actor) = state.authenticate_bearer_token(provided_token) else {
        return Err(ApiError::unauthorized("invalid bearer token"));
    };
    request.extensions_mut().insert(actor.actor().clone());
    request.extensions_mut().insert(actor);

    Ok(next.run(request).await)
}

/// Refuse a request whose `omnigraph-expect-actor` header names another actor
/// than its credential resolved to. It runs after authentication and the
/// contract check, before graph resolution or any body work, and can only
/// narrow a request: a matching value changes nothing, and the header never
/// supplies an actor.
pub(crate) async fn require_expected_actor(
    request: Request,
    next: Next,
) -> std::result::Result<Response, ApiError> {
    let mut values = request.headers().get_all(EXPECT_ACTOR_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(next.run(request).await);
    };
    if values.next().is_some() {
        return Err(ApiError::bad_request(format!(
            "at most one {EXPECT_ACTOR_HEADER} header is allowed"
        )));
    }
    let expected = value
        .to_str()
        .ok()
        .filter(|actor| !actor.is_empty() && actor.trim() == *actor)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "{EXPECT_ACTOR_HEADER} must name one actor, without surrounding whitespace"
            ))
        })?;
    let resolved = request
        .extensions()
        .get::<AuthenticatedActor>()
        .map(|actor| actor.actor_id.as_ref());
    if resolved == Some(expected) {
        return Ok(next.run(request).await);
    }
    let message = match resolved {
        Some(actor) => {
            format!("the credential resolves to actor '{actor}', not the expected '{expected}'")
        }
        None => format!(
            "the server runs without authentication and resolves no actor, not the expected '{expected}'"
        ),
    };
    Err(ApiError::new(ErrorCode::ActorMismatch, message))
}

/// Routing middleware (RFC-011 cluster-only). Resolves the active graph
/// for the request and injects `GraphRequest` as an extension so
/// handlers can extract it via `Extension<GraphRequest>`.
///
/// Routes are always nested under `/graphs/{graph_id}/...`. The
/// middleware extracts `{graph_id}` from the URI path and looks it up in
/// the registry. Unknown graphs return 404; authorized blocked graphs return 503.
///
/// The middleware fires AFTER `require_bearer_auth`, so the actor is
/// already in the request extensions (or auth was off entirely).
pub(crate) async fn resolve_graph_handle(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, ApiError> {
    // `Router::nest("/graphs/{graph_id}", inner)` rewrites
    // `request.uri().path()` to the inner suffix (e.g. `/snapshot`).
    // The pre-rewrite URI is preserved in the `OriginalUri`
    // request extension by axum's router; we read from there to
    // extract `{graph_id}`. Fall back to the current URI only if
    // the extension is missing, which shouldn't happen for
    // nested routes but is safe defensive code.
    let original_path: String = request
        .extensions()
        .get::<OriginalUri>()
        .map(|OriginalUri(uri)| uri.path().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let graph_id_str = original_path
        .strip_prefix("/graphs/")
        .and_then(|rest| rest.split('/').next())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            ApiError::bad_request("cluster route missing /graphs/{graph_id} prefix".to_string())
        })?;
    let graph_id = GraphId::try_from(graph_id_str.to_string())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let key = GraphKey::cluster(graph_id.clone());
    let handle = resolve_registered_graph(
        &state,
        &key,
        request.extensions().get::<AuthenticatedActor>(),
    )?;

    // Per-request observability. `Span::current().record` would silently
    // no-op here because no upstream `#[tracing::instrument(...)]` macro
    // declares a `graph_id` field; emit an explicit event instead so the
    // routing decision actually lands in logs.
    info!(graph_id = %handle.key.graph_id, "graph routed");

    request.extensions_mut().insert(handle);
    ingress::admit(&state, request, next).await
}

/// HTTP and MCP share identity and unavailable-graph disclosure.
pub(crate) fn resolve_registered_graph(
    state: &AppState,
    key: &GraphKey,
    actor: Option<&AuthenticatedActor>,
) -> std::result::Result<GraphRequest, ApiError> {
    let (captured, server_policy) = state
        .routing()
        .registry
        .capture_with_policy(&state.operations, key)?;
    let (policy, invalid_policy, message) = match captured {
        RegistryCapture::Loading(graph) => (
            graph.policy.clone(),
            false,
            "graph is loading; wait for its startup attempt to complete",
        ),
        RegistryCapture::Ready(handle) => return Ok(handle),
        RegistryCapture::Gone => return Err(ApiError::not_found("graph not found")),
        RegistryCapture::Transitioning(view) => (
            view.policy.clone(),
            false,
            "graph admission is closed for a serving transition",
        ),
        RegistryCapture::Blocked(graph) => (
            graph.policy.clone(),
            matches!(
                graph.failure,
                StartupFailure::InvalidPolicy | StartupFailure::InvalidConfiguration
            ),
            "graph is unavailable; an operator must apply an explicit correction or restart after fixing startup configuration",
        ),
    };
    // Invalid policy is not equivalent to the operator choosing no policy.
    // Both startup failures and transitions preserve authorization before
    // disclosing a known graph's unavailability.
    let readable = !invalid_policy
        && matches!(
            authorize(
                actor,
                policy.as_deref(),
                PolicyRequest {
                    action: PolicyAction::Read,
                    branch: Some("main".into()),
                    target_branch: None,
                }
            )?,
            Authz::Allowed
        );
    let listable = readable
        || matches!(
            authorize(
                actor,
                server_policy.as_deref(),
                PolicyRequest {
                    action: PolicyAction::GraphList,
                    branch: None,
                    target_branch: None,
                }
            )?,
            Authz::Allowed
        );
    if !listable {
        return Err(ApiError::not_found("graph not found"));
    }
    Err(ApiError::new(ErrorCode::GraphUnavailable, message))
}

pub(crate) fn log_policy_decision(
    actor_id: &str,
    request: &PolicyRequest,
    decision: &PolicyDecision,
) {
    info!(
        actor_id = actor_id,
        action = %request.action,
        branch = request.branch.as_deref().unwrap_or(""),
        target_branch = request.target_branch.as_deref().unwrap_or(""),
        allowed = decision.allowed,
        matched_rule_id = decision.matched_rule_id.as_deref().unwrap_or(""),
        "policy decision"
    );
}

/// The allow/deny **decision** an authorization check produces, kept
/// separate from the operational failures (`Err`) that can occur while
/// computing it. [`authorize_request`] collapses `Denied` to a 403; a caller
/// that needs to remap a denial without also remapping operational failures
/// (the stored-query invoke handler hides a denial as a 404) matches on this
/// directly, so a real 401 (missing bearer) or 500 (policy-evaluation error)
/// keeps its true status instead of being masked as the denial's response.
pub(crate) enum Authz {
    Allowed,
    Denied(String),
}

/// HTTP-layer Cedar policy gate, returning the allow/deny [`Authz`] decision
/// and reserving `Err` for operational failures (400 non-canonical branch
/// name, 401 missing bearer, 500 policy-evaluation error). Every route names
/// its branches in the [`PolicyRequest`], so the request's names are refused
/// here, before any rule and in every runtime mode, when they are not
/// canonical: the decision judges a name as written and the engine opens that
/// same name. Two sources of the policy engine:
///   * Per-graph handler — passes `handle.policy.as_deref()` so the
///     graph's Cedar rules govern read/change/branch_*.
///   * Management handler — captures the current registry snapshot policy so
///     server-level Cedar rules govern management access coherently with its
///     graph inventory.
///
/// The MR-731 invariant lives inside this function: actor identity is
/// supplied as a separate argument from the resolved bearer match. The
/// `PolicyRequest` struct itself does not carry identity (the field was
/// dropped from the type), so handlers cannot smuggle it through the
/// request. See `actor_id_resolves_from_bearer_token_ignoring_client_supplied_headers`
/// at `tests/server.rs`.
pub(crate) fn authorize(
    actor: Option<&AuthenticatedActor>,
    policy: Option<&PolicyEngine>,
    request: PolicyRequest,
) -> std::result::Result<Authz, ApiError> {
    require_canonical_branches([request.branch.as_deref(), request.target_branch.as_deref()])?;
    if let Some(actor) = actor {
        if actor.source == AuthSource::SignedData && policy.is_none() {
            return Ok(Authz::Denied(
                "signed data credentials require an applied Cedar policy permit".to_string(),
            ));
        }
    }
    let Some(engine) = policy else {
        // No PolicyEngine installed. Three runtime states can reach this:
        //
        // * **Open mode** (`--unauthenticated`): no tokens, no policy.
        //   Per-graph operations are open by operator opt-in (they
        //   accepted "trust the network" for graph data).
        // * **DefaultDeny mode**: tokens configured but no policy. The
        //   request went through bearer auth, so `actor` is Some. Only
        //   per-graph `Read` is permitted; other per-graph actions
        //   return 403. Closes the "configured auth but forgot the
        //   policy file" trap from MR-723.
        // * Either of the above with a **server-scoped** action
        //   (`graph_list`, `config_manage`).
        //
        // Server-scoped actions are always denied here, regardless of
        // mode or actor presence. The management surface leaks server
        // topology (graph IDs + URIs that may contain S3 bucket paths
        // or internal hostnames) — operators who opted into Open mode
        // accepted exposure of graph DATA, not exposure of server
        // topology. Closing the management surface by default in every
        // runtime state means the docstring contract on
        // `server_graphs_list` ("don't leak the registry until the
        // operator explicitly authorizes it") holds uniformly; the
        // cluster must be bootstrapped with an explicit cluster-scoped
        // policy bundle.
        if request.action.resource_kind() == PolicyResourceKind::Server {
            return Ok(Authz::Denied(
                "server-scoped actions require an applied cluster policy permit; \
                 declare the cluster policy when bootstrapping. The management surface \
                 is closed by default, including with --unauthenticated."
                    .to_string(),
            ));
        }
        if actor.is_some() && request.action != PolicyAction::Read {
            return Ok(Authz::Denied(
                "server runs in default-deny mode (bearer tokens configured but no \
                 applied policy bundle). Only `read` actions are permitted. Other \
                 actions require an applied graph policy."
                    .to_string(),
            ));
        }
        return Ok(Authz::Allowed);
    };
    let Some(actor) = actor else {
        return Err(ApiError::unauthorized("missing bearer token"));
    };
    // SECURITY INVARIANT (MR-731): actor identity is supplied to the
    // policy engine here as a separate argument, sourced from the
    // bearer-token match resolved by `require_bearer_auth`. The
    // `PolicyRequest` struct itself no longer carries `actor_id` (it
    // was dropped from the type), so handlers cannot smuggle identity
    // through the request body and there is no overwrite step that
    // could be skipped. The principle is codified in
    // `docs/dev/invariants.md` Hard Invariant 11 ("clients cannot set
    // actor identity directly") and pinned by the regression test
    // `actor_id_resolves_from_bearer_token_ignoring_client_supplied_headers`
    // in `crates/omnigraph-server/tests/server.rs`.
    let actor_id = actor.actor_id.as_ref();
    let decision = engine
        .authorize(actor_id, &request)
        .map_err(|err| ApiError::internal(format!("policy: {err}")))?;
    log_policy_decision(actor_id, &request, &decision);
    if decision.allowed {
        Ok(Authz::Allowed)
    } else {
        Ok(Authz::Denied(decision.message))
    }
}

/// Refuse a request's branch names that are not canonical
/// ([`omnigraph_policy::ensure_canonical_branch_name`]) as a 400. [`authorize`]
/// applies it to every [`PolicyRequest`]; a route that looks a branch up before
/// it authorizes applies it first.
pub(crate) fn require_canonical_branches<'a>(
    names: impl IntoIterator<Item = Option<&'a str>>,
) -> std::result::Result<(), ApiError> {
    for name in names.into_iter().flatten() {
        omnigraph_policy::ensure_canonical_branch_name(name)
            .map_err(|error| ApiError::bad_request(error.to_string()))?;
    }
    Ok(())
}

/// Thin wrapper over [`authorize`] for the handlers that treat any denial as a
/// 403: a denial becomes `ApiError::forbidden`, and operational failures
/// (400 non-canonical branch name, 401 missing bearer, 500 policy-evaluation
/// error) propagate unchanged. The
/// stored-query invoke handler does **not** use this — it consumes the
/// [`Authz`] decision directly to hide a denial as a 404 while letting an
/// operational failure keep its true status.
pub(crate) fn authorize_request(
    actor: Option<&AuthenticatedActor>,
    policy: Option<&PolicyEngine>,
    request: PolicyRequest,
) -> std::result::Result<(), ApiError> {
    match authorize(actor, policy, request)? {
        Authz::Allowed => Ok(()),
        Authz::Denied(message) => Err(ApiError::forbidden(message)),
    }
}

#[utoipa::path(
    get,
    path = "/snapshot",
    tag = "snapshots",
    operation_id = "getSnapshot",
    params(SnapshotQuery),
    responses(
        (status = 200, description = "Graph snapshot", body = api::SnapshotOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Read the current snapshot of a branch.
///
/// Returns the graph-manifest version plus per-dataset metadata (path,
/// published dataset version, entity count) for every backing dataset on the
/// branch. Defaults to `main` when `branch` is omitted. Read-only.
pub(crate) async fn server_snapshot(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiQuery(query): ApiQuery<SnapshotQuery>,
) -> std::result::Result<Json<api::SnapshotOutput>, ApiError> {
    let branch = query.branch.unwrap_or_else(|| "main".to_string());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    // One resolution: the stamp is read from the snapshot's own manifest
    // version, so both halves of the response describe one graph version.
    let (snapshot, internal_schema_version) = {
        let db = &handle.engine;
        let snapshot = db
            .snapshot_of(ReadTarget::branch(branch.as_str()))
            .await
            .map_err(ApiError::from_omni)?;
        let internal_schema_version = db
            .internal_schema_version_at(&snapshot)
            .await
            .map_err(ApiError::from_omni)?;
        (snapshot, internal_schema_version)
    };
    let output = snapshot_payload(&branch, &snapshot, internal_schema_version)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(output))
}

#[utoipa::path(
    get,
    path = "/access",
    tag = "access",
    operation_id = "getAccess",
    params(api::AccessQuery),
    responses(
        (status = 200, description = "The actions the caller's credential may take", body = api::AccessOutput),
        (status = 400, description = "An unknown or repeated parameter, a target branch without a branch, or a branch name that is not canonical", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 500, description = "The installed policy could not be evaluated", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// What the caller's own credential may do on this graph.
///
/// Evaluates the graph's installed policy for the resolved actor once per
/// action, with each action's branch and target branch shaped as the route
/// that performs it shapes them. It needs no graph action itself, reads no
/// graph data, and never checks whether a named branch exists.
pub(crate) async fn server_access(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiQuery(query): ApiQuery<api::AccessQuery>,
) -> std::result::Result<Json<api::AccessOutput>, ApiError> {
    Ok(Json(access_body(
        &handle,
        actor.as_ref().map(|Extension(actor)| actor),
        query,
    )?))
}

/// The access route's answer, shared by `GET /access` and the MCP `access`
/// tool.
pub(crate) fn access_body(
    handle: &GraphRequest,
    actor: Option<&AuthenticatedActor>,
    query: api::AccessQuery,
) -> std::result::Result<api::AccessOutput, ApiError> {
    if query.target_branch.is_some() && query.branch.is_none() {
        return Err(ApiError::bad_request(
            "target_branch needs branch: it names where branch_create and branch_merge from branch would land",
        ));
    }
    require_canonical_branches([query.branch.as_deref(), query.target_branch.as_deref()])?;
    let policy = handle.policy.as_deref();
    api::AccessOutput::evaluate(
        actor.map(|actor| actor.actor_id.to_string()),
        handle.key.graph_id.to_string(),
        query.branch,
        query.target_branch,
        |access, branch, target_branch| {
            let request = PolicyRequest {
                action: access.into(),
                branch: branch.map(str::to_string),
                target_branch: target_branch.map(str::to_string),
            };
            Ok(matches!(authorize(actor, policy, request)?, Authz::Allowed))
        },
    )
}

#[utoipa::path(
    post,
    path = "/query",
    tag = "queries",
    operation_id = "query",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query results", body = ReadOutput),
        (status = 400, description = "Bad request - also returned when the query body contains mutations (use POST /mutate, for write queries), when a control write statement (`branch create`, `branch delete`, `branch merge`) arrives here instead of POST /mutate, when a request target accompanies a branch statement, and when a name or parameters accompany a branch statement", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "A full-text index requires explicit rebuilding (full_text_index_rebuild_required) or has no segment at the snapshot (full_text_index_required); neither is cleared by retrying", body = ErrorOutput),
        (status = 424, description = "A load explain could not probe an allowed external source", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Execute an inline read query.
///
/// Designed for ad-hoc exploration and AI-agent tool-use: short field
/// names (`query`, `name`) match the CLI `-e` flag and the GQ `query`
/// keyword. Mutations (`insert`/`update`/`delete`) are rejected with 400
/// -- use `POST /mutate` for writes. Select a branch or snapshot and obtain
/// the pinned graph-commit token with the response. Cedar authorizes Read for
/// read queries and Change for `explain load csv`.
///
/// The GQ statement `branch list` is also served here, with no `branch`,
/// `snapshot`, `name`, or `params`: it answers one result per branch (field
/// `name`, byte order) under the same `read` check as `GET /branches`. So are
/// the branch control reads, each under its route's checks and errors:
/// `branch diff <source> [into <target>]` and `branch diff <from> to <to>`
/// (one result per entity change, fields `kind`, `type`, `id`, `op`,
/// `before`, `after`; the compared commits and the continuation token in
/// `page`; `after page "<token>"` and `limit <n>` continue and bound it),
/// `branch merge-base <source> [into <target>]` and
/// `branch merge <source> [into <target>] dry run` (one result each).
/// `branch create`, `branch delete`, and `branch merge` are rejected with
/// 400; send them to `POST /mutate`.
///
/// The GQ statement `explain query …` is served here too: the query is not
/// run, and the result describes its v2 logical, physical and available
/// DataFusion trees, one result per node (fields `tree`, `depth`, `node`, `detail`), followed by
/// `plan` entries for the passes and the document's other fields, under the
/// request's target and `params`. `explain load csv …` answers the accepted
/// load plan the same way (`logical_initial`, `logical`, `physical`, then
/// `plan` entries), under the request's `branch` alone. It requires the load's
/// Change decision before probing the external source's metadata; it reads
/// no payload byte and publishes nothing.
pub(crate) async fn server_query(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiJson(request): ApiJson<QueryRequest>,
) -> std::result::Result<Json<ReadOutput>, ApiError> {
    let session = state.session(&handle, request.settings.as_ref())?;
    let output = run_query(
        handle,
        session,
        actor.as_ref().map(|Extension(actor)| actor),
        Door::Query,
        &request.query,
        request.name.as_deref(),
        request.params.as_ref(),
        request.branch,
        request.snapshot,
    )
    .await?
    .into_read_output()?;
    Ok(Json(output))
}

/// A result the JSON writer refuses to render answers 500 (RFC 0051), never 400.
fn render_error(err: impl std::fmt::Display) -> ApiError {
    ApiError::internal(err.to_string())
}

/// OpenAPI-only marker for an unstructured octet-stream response body.
#[derive(utoipa::ToSchema)]
#[schema(value_type = String, format = Binary)]
#[allow(dead_code)]
struct BlobBinaryBody(Vec<u8>);

#[utoipa::path(
    get,
    path = "/blob",
    tag = "blobs",
    operation_id = "getBlob",
    params(
        BlobReadQuery,
        ("If-Match" = Option<String>, Header, description = "Strong entity-tag-list precondition, including `*`, evaluated before If-None-Match and Range."),
        ("Range" = Option<String>, Header, description = "One `bytes` range. Malformed, unknown-unit, and multiple ranges are ignored in V1."),
        ("If-None-Match" = Option<String>, Header, description = "Weak entity-tag-list comparison, including `*`, evaluated before Range."),
        ("If-Range" = Option<String>, Header, description = "One strong entity tag. A mismatch causes the complete representation to be served."),
    ),
    responses(
        (status = 200, description = "Complete managed Blob", body = inline(BlobBinaryBody), content_type = "application/octet-stream",
            headers(
                ("Accept-Ranges" = String, description = "The literal value `bytes` for managed content"),
                ("Content-Length" = u64, description = "Exact served payload length"),
                ("ETag" = String, description = "Strong validator for the selected managed Blob"),
                ("Omnigraph-Snapshot-Id" = String, description = "Exact resolved graph snapshot"),
            )),
        (status = 206, description = "One satisfiable managed byte range", body = inline(BlobBinaryBody), content_type = "application/octet-stream",
            headers(
                ("Accept-Ranges" = String),
                ("Content-Length" = u64),
                ("Content-Range" = String),
                ("ETag" = String),
                ("Omnigraph-Snapshot-Id" = String),
            )),
        (status = 302, description = "External Blob descriptor; the server does not dereference it",
            headers(
                ("Location" = String, description = "Exact stored absolute URI"),
                ("Cache-Control" = String, description = "The literal value `no-store`"),
                ("Omnigraph-Snapshot-Id" = String, description = "Exact resolved graph snapshot"),
            )),
        (status = 304, description = "If-None-Match matched the managed Blob validator",
            headers(
                ("Accept-Ranges" = String),
                ("Content-Length" = u64, description = "Complete managed Blob length, as required for a valid 304 Content-Length"),
                ("ETag" = String),
                ("Omnigraph-Snapshot-Id" = String),
            )),
        (status = 400, description = "Invalid selector, target, or non-Blob property", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Unknown entity or null Blob cell", body = ErrorOutput),
        (status = 412, description = "If-Match did not strongly match the selected managed Blob validator", body = ErrorOutput,
            headers(
                ("Accept-Ranges" = String),
                ("ETag" = String),
                ("Omnigraph-Snapshot-Id" = String),
            )),
        (status = 416, description = "Requested managed byte range is unsatisfiable", body = ErrorOutput,
            headers(
                ("Accept-Ranges" = String),
                ("Content-Range" = String, description = "Unsatisfied range in the form `bytes */N`"),
                ("ETag" = String),
                ("Omnigraph-Snapshot-Id" = String),
            )),
        (status = 500, description = "Stored Blob integrity or pre-header delivery refusal, including ranged external descriptors that cannot be redirected", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Deliver one logical node or edge Blob cell.
///
/// Managed content is streamed through the bounded transport. External
/// descriptors redirect without target-store I/O. Authorization and target
/// resolution share the exact helper used by `/query`.
pub(crate) async fn server_blob_get(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    headers: HeaderMap,
    ApiQuery(query): ApiQuery<BlobReadQuery>,
) -> std::result::Result<Response, ApiError> {
    let read = read_blob_for_delivery(&handle, actor.as_ref().map(|Extension(actor)| actor), query)
        .await?;
    blob_transport::serve_blob_get(read, &headers).inspect_err(log_blob_transport_internal)
}

#[utoipa::path(
    head,
    path = "/blob",
    tag = "blobs",
    operation_id = "headBlob",
    params(
        BlobReadQuery,
        ("If-Match" = Option<String>, Header, description = "Strong entity-tag-list precondition, including `*`, evaluated before If-None-Match."),
        ("If-None-Match" = Option<String>, Header, description = "Weak entity-tag-list comparison, including `*`. Range and If-Range are ignored for HEAD."),
        ("Range" = Option<String>, Header, description = "Accepted but ignored for HEAD; metadata always describes the complete selected Blob."),
        ("If-Range" = Option<String>, Header, description = "Accepted but ignored for HEAD together with Range."),
    ),
    responses(
        (status = 200, description = "Managed Blob metadata with no response body",
            headers(
                ("Accept-Ranges" = String, description = "The literal value `bytes`"),
                ("Content-Length" = u64, description = "Complete managed Blob length"),
                ("ETag" = String, description = "Strong validator for the selected managed Blob"),
                ("Omnigraph-Snapshot-Id" = String, description = "Exact resolved graph snapshot"),
            )),
        (status = 302, description = "External Blob descriptor; the server does not dereference it",
            headers(
                ("Location" = String, description = "Exact stored absolute URI"),
                ("Cache-Control" = String, description = "The literal value `no-store`"),
                ("Omnigraph-Snapshot-Id" = String, description = "Exact resolved graph snapshot"),
            )),
        (status = 304, description = "If-None-Match matched the managed Blob validator",
            headers(
                ("Accept-Ranges" = String),
                ("Content-Length" = u64, description = "Complete managed Blob length, as required for a valid 304 Content-Length"),
                ("ETag" = String),
                ("Omnigraph-Snapshot-Id" = String),
            )),
        (status = 400, description = "Invalid selector, target, or non-Blob property; HEAD responses have no body"),
        (status = 401, description = "Unauthorized; HEAD responses have no body"),
        (status = 403, description = "Forbidden; HEAD responses have no body"),
        (status = 404, description = "Unknown entity or null Blob cell; HEAD responses have no body"),
        (status = 412, description = "If-Match did not strongly match the selected managed Blob validator; HEAD responses have no body",
            headers(
                ("Accept-Ranges" = String),
                ("ETag" = String),
                ("Omnigraph-Snapshot-Id" = String),
            )),
        (status = 500, description = "Stored Blob integrity or pre-header delivery refusal, including ranged external descriptors that cannot be redirected; HEAD responses have no body"),
    ),
    security(("bearer_token" = [])),
)]
/// Return the status and representation headers for one Blob cell.
///
/// This is a distinct handler rather than Axum's automatic GET-to-HEAD
/// fallback. It never calls `BlobReader::read_range`; Range and If-Range are
/// deliberately ignored while If-None-Match is still evaluated.
pub(crate) async fn server_blob_head(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    headers: HeaderMap,
    ApiQuery(query): ApiQuery<BlobReadQuery>,
) -> std::result::Result<Response, ApiError> {
    let read = read_blob_for_delivery(&handle, actor.as_ref().map(|Extension(actor)| actor), query)
        .await?;
    blob_transport::serve_blob_head(read, &headers).inspect_err(log_blob_transport_internal)
}

#[utoipa::path(
    put,
    path = "/blob",
    tag = "blobs",
    operation_id = "putBlob",
    params(
        BlobWriteQuery,
        ("If-Match" = Option<String>, Header, description = "`*` to require a value in the cell, or a list of strong entity tags one of which must equal the cell's current ETag. Weak tags never match. The write evaluates it against the branch head it applies to."),
    ),
    request_body(
        content = inline(BlobBinaryBody),
        content_type = "application/octet-stream",
        description = "The raw bytes to store: at most 33554432 bytes (32 MiB), inclusive."
    ),
    responses(
        (status = 200, description = "The bytes are published in one graph commit", body = BlobWriteOutput,
            headers(
                ("ETag" = String, description = "Strong validator of the stored value; equals `etag` in the body"),
            )),
        (status = 400, description = "Invalid selector, query parameter, If-Match field or target, a non-Blob property, or a carried external reference the graph's policy denies", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Unknown branch, or no entity with this id", body = ErrorOutput),
        (status = 408, description = "The request body did not arrive before the body deadline", body = ErrorOutput),
        (status = 409, description = "Write-authority conflict, or the branch's incarnation or accepted schema changed while the write retried", body = ErrorOutput),
        (status = 412, description = "If-Match did not hold for the cell; `blob_precondition_failure` names its current validator", body = ErrorOutput,
            headers(
                ("ETag" = String, description = "The cell's current validator, when it holds a managed value"),
            )),
        (status = 413, description = "The body exceeds the server's Blob put limit (the process `write_max_bytes`, at most 32 MiB), or the value with the entity's carried Blob payloads exceeds it", body = ErrorOutput),
        (status = 415, description = "Content-Type must be application/octet-stream", body = ErrorOutput),
        (status = 424, description = "An allowed external Blob source carried from the entity could not be read", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "Write admission is closed or an overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Replace one Blob value of an existing node or edge with the request body.
///
/// The cell's old value is never read; the entity's other cells are carried
/// unchanged. Authorization of `change` on the branch runs before the body is
/// read. Once admitted the write is owned by the server: a disconnect loses
/// only the response, never cancels or replays the write.
pub(crate) async fn server_blob_put(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiQuery(query): ApiQuery<BlobWriteQuery>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    let ingress = request
        .extensions()
        .get::<IngressLease>()
        .cloned()
        .ok_or_else(|| ApiError::internal("missing ingress reservation"))?;
    let precondition = blob_write_precondition(request.headers())?;
    let actor = actor.as_ref().map(|Extension(actor)| actor);
    let branch = query.branch.clone().unwrap_or_else(|| "main".to_string());
    authorize_blob_write(&handle, actor, &branch)?;

    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if !matches!(content_type, Some(value) if value.eq_ignore_ascii_case("application/octet-stream"))
    {
        return Err(ApiError::unsupported_media_type(
            "a Blob put requires Content-Type: application/octet-stream",
        ));
    }
    let declared = match request.headers().get(CONTENT_LENGTH) {
        None => None,
        Some(value) => Some(
            value
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| ApiError::bad_request("invalid Content-Length"))?,
        ),
    };
    let limit = state.blob_put_limit();
    if let Some(declared) = declared.filter(|declared| *declared > limit) {
        return Err(blob_put_too_large(limit, declared));
    }

    let deadline = request
        .extensions()
        .get::<ingress::BodyDeadline>()
        .copied()
        .ok_or_else(|| ApiError::internal("missing request body deadline"))?;
    let bytes = tokio::time::timeout_at(
        deadline.0,
        collect_blob_put_body(request.into_body(), declared, limit),
    )
    .await
    .map_err(|_| ingress::body_timeout())??;
    ingress
        .shrink(bytes.len() as u64)
        .map_err(ApiError::from_workload_reject)?;
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let admission = state
        .workload
        .try_admit(&actor_arc, bytes.len() as u64)
        .map_err(ApiError::from_workload_reject)?;

    let session = state.session(&handle, None)?;
    let selector = BlobSelectorOutput::from(&query);
    let cell = blob_write_cell(query);
    let actor_id = actor.map(|actor| actor.actor_id.to_string());
    let output = owned_write(&state, admission, ingress, handle.clone(), async move {
        let outcome = session
            .put_blob_at_as(&branch, cell, bytes, precondition, actor_id.as_deref())
            .await
            .map_err(ApiError::from_omni)?;
        Ok(api::blob_write_output(selector, branch, &outcome, actor_id))
    })
    .await?;
    blob_write_response(output)
}

#[utoipa::path(
    delete,
    path = "/blob",
    tag = "blobs",
    operation_id = "clearBlob",
    params(
        BlobWriteQuery,
        ("If-Match" = Option<String>, Header, description = "`*` to require a value in the cell, or a list of strong entity tags one of which must equal the cell's current ETag. A null cell satisfies neither form."),
    ),
    responses(
        (status = 200, description = "The cell is null. `commit` is the clear's publication, or `null` when the cell already was null and nothing was published", body = BlobWriteOutput),
        (status = 400, description = "Invalid selector, query parameter, If-Match field or target, a non-Blob or non-nullable property, or a carried external reference the graph's policy denies", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Unknown branch, or no entity with this id", body = ErrorOutput),
        (status = 409, description = "Write-authority conflict, or the branch's incarnation or accepted schema changed while the write retried", body = ErrorOutput),
        (status = 412, description = "If-Match did not hold for the cell; `blob_precondition_failure` names its current validator", body = ErrorOutput,
            headers(
                ("ETag" = String, description = "The cell's current validator, when it holds a managed value"),
            )),
        (status = 413, description = "The entity's carried Blob payloads exceed the write's payload limit", body = ErrorOutput),
        (status = 424, description = "An allowed external Blob source carried from the entity could not be read", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "Write admission is closed or an overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Set one nullable Blob value of an existing node or edge to null.
///
/// Clearing a cell that is already null publishes nothing. Like a put, the
/// clear is owned by the server once admitted.
pub(crate) async fn server_blob_delete(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    headers: HeaderMap,
    ApiQuery(query): ApiQuery<BlobWriteQuery>,
) -> std::result::Result<Response, ApiError> {
    let precondition = blob_write_precondition(&headers)?;
    let actor = actor.as_ref().map(|Extension(actor)| actor);
    let branch = query.branch.clone().unwrap_or_else(|| "main".to_string());
    authorize_blob_write(&handle, actor, &branch)?;
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let admission = state
        .workload
        .try_admit(&actor_arc, 0)
        .map_err(ApiError::from_workload_reject)?;

    let session = state.session(&handle, None)?;
    let selector = BlobSelectorOutput::from(&query);
    let cell = blob_write_cell(query);
    let actor_id = actor.map(|actor| actor.actor_id.to_string());
    let output = owned_write(&state, admission, ingress, handle.clone(), async move {
        let outcome = session
            .clear_blob_at_as(&branch, cell, precondition, actor_id.as_deref())
            .await
            .map_err(ApiError::from_omni)?;
        Ok(api::blob_write_output(selector, branch, &outcome, actor_id))
    })
    .await?;
    blob_write_response(output)
}

fn blob_write_precondition(
    headers: &HeaderMap,
) -> std::result::Result<Option<omnigraph::BlobPrecondition>, ApiError> {
    api::parse_blob_if_match(
        headers
            .get_all(axum::http::header::IF_MATCH)
            .iter()
            .map(|value| value.as_bytes()),
    )
    .map_err(ApiError::bad_request)
}

/// A Blob write needs `change` on its branch, checked before any body byte
/// is read.
fn authorize_blob_write(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
    branch: &str,
) -> std::result::Result<(), ApiError> {
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Change,
            branch: Some(branch.to_string()),
            target_branch: None,
        },
    )
}

fn blob_write_cell(query: BlobWriteQuery) -> omnigraph::BlobCell {
    omnigraph::BlobCell {
        entity: match query.entity {
            api::BlobEntityKind::Node => omnigraph::EntityKind::Node,
            api::BlobEntityKind::Edge => omnigraph::EntityKind::Edge,
        },
        type_name: query.r#type,
        id: query.id,
        property: query.property,
    }
}

/// The refusal of a put body over the limit: the same resource, limit and
/// message the engine reports for an oversized embedded put.
fn blob_put_too_large(limit: u64, actual: u64) -> ApiError {
    ApiError::from_omni(OmniError::resource_limit(
        omnigraph::BLOB_WRITE_PAYLOAD_RESOURCE,
        limit,
        actual,
    ))
}

/// Collect a put body into one buffer, sized from a declared length that is
/// already within the limit. The engine adopts the buffer without a copy.
async fn collect_blob_put_body(
    body: Body,
    declared: Option<u64>,
    limit: u64,
) -> std::result::Result<Bytes, ApiError> {
    let capacity = declared.map_or(Ok(0), usize::try_from).map_err(|_| {
        ApiError::bad_request("Content-Length does not fit in this platform's memory")
    })?;
    let mut data = Vec::with_capacity(capacity);
    let mut body = body.into_data_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|err| {
            ApiError::bad_request(format!("failed to read Blob request body: {err}"))
        })?;
        let actual = data.len().saturating_add(chunk.len()) as u64;
        if actual > limit {
            return Err(blob_put_too_large(limit, actual));
        }
        data.extend_from_slice(&chunk);
    }
    if declared.is_some_and(|declared| declared != data.len() as u64) {
        return Err(ApiError::bad_request(
            "Blob request body length does not match its Content-Length",
        ));
    }
    Ok(Bytes::from(data))
}

/// A write receipt with its `ETag` header when the cell holds a managed value.
fn blob_write_response(output: BlobWriteOutput) -> std::result::Result<Response, ApiError> {
    let mut headers = HeaderMap::new();
    if let Some(etag) = output.etag.as_deref() {
        headers.insert(
            axum::http::header::ETAG,
            axum::http::HeaderValue::from_str(etag)
                .map_err(|_| ApiError::internal("a managed Blob ETag is not a header value"))?,
        );
    }
    Ok((StatusCode::OK, headers, Json(output)).into_response())
}

async fn read_blob_for_delivery(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
    query: BlobReadQuery,
) -> std::result::Result<omnigraph::BlobRead, ApiError> {
    let target =
        resolve_authorized_read_target_with_cause(handle, actor, query.branch, query.snapshot)
            .await
            .map_err(|(mapped, cause)| redact_blob_api_error(mapped, "target", cause))?;
    let entity = match query.entity {
        api::BlobEntityKind::Node => omnigraph::EntityKind::Node,
        api::BlobEntityKind::Edge => omnigraph::EntityKind::Edge,
    };
    handle
        .engine
        .read_blob_at(
            target,
            omnigraph::BlobCell {
                entity,
                type_name: query.r#type,
                id: query.id,
                property: query.property,
            },
        )
        .await
        .map_err(map_blob_read_error)
}

/// Keep physical placement and persisted identity details behind the
/// graph-level Blob surface. Selector/auth/not-found failures retain their
/// typed client disposition; every pre-header internal failure is redacted.
fn map_blob_read_error(error: OmniError) -> ApiError {
    let (mapped, cause) = engine_error_with_cause(error);
    redact_blob_api_error(mapped, "cell", cause)
}

/// Log a 500 the transport built itself before response headers. Its message
/// describes the server's own refusal and holds no engine text, so the
/// response is returned as built.
fn log_blob_transport_internal(refused: &ApiError) {
    if refused.status() == StatusCode::INTERNAL_SERVER_ERROR {
        error!(
            error_kind = "blob_pre_header_internal",
            stage = "transport",
            error_variant = "unclassified",
            "Blob delivery failed before response headers"
        );
    }
}

/// Redact a pre-header internal failure. The log carries the stage and the
/// error's class (never its message, which can hold object URIs or
/// credentials); the response carries a constant.
fn redact_blob_api_error(
    mapped: ApiError,
    stage: &'static str,
    cause: Option<RedactedCause>,
) -> ApiError {
    if mapped.status() == StatusCode::INTERNAL_SERVER_ERROR {
        error!(
            error_kind = "blob_pre_header_internal",
            stage,
            error_variant = cause.map_or("unclassified", |cause| cause.variant),
            storage_kind = ?cause.and_then(|cause| cause.storage_kind),
            manifest_kind = ?cause.and_then(|cause| cause.manifest_kind),
            "Blob delivery failed before response headers"
        );
        ApiError::internal("Blob delivery failed before response headers")
    } else {
        mapped
    }
}

#[utoipa::path(
    post,
    path = "/export",
    tag = "queries",
    operation_id = "export",
    request_body = ExportRequest,
    responses(
        (status = 200, description = "Exported data as NDJSON", content_type = "application/x-ndjson"),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Export authority conflict", body = ErrorOutput),
        (status = 413, description = "Export cut or transport capacity exhausted", body = ErrorOutput),
        (status = 415, description = "Request body must use application/json", body = ErrorOutput),
        (status = 404, description = "Branch not found", body = ErrorOutput),
        (status = 503, description = "Recovery required", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Stream the contents of a branch as NDJSON.
///
/// Emits one JSON object per line (`application/x-ndjson`). Filter with
/// `type_names` (node/edge type names); an empty list streams the entire branch.
/// Suitable for large exports — the response is streamed, not buffered.
/// Read-only.
pub(crate) async fn server_export(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(observer): Extension<operations::ReadObserver>,
    Extension(input): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiJson(request): ApiJson<ExportRequest>,
) -> std::result::Result<Response, ApiError> {
    let branch = requested_branch(request.branch.as_deref())?;
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Export,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    // Reserve the bounded response transport before capturing the root cut so
    // a saturated client population can never hold graph authority while it
    // waits for process memory. Both operations finish before the 200 headers.
    let queue_lease = state
        .export_transport
        .reserve()
        .await
        .map_err(ApiError::from_omni)?;
    let cut = handle
        .engine
        .capture_served_export_cut(&branch, &request.type_names)
        .await
        .map_err(ApiError::from_omni)?;
    let producer_queue_lease = Arc::clone(&queue_lease);
    let (tx, body_stream) = export_transport::channel(queue_lease, "export");
    tokio::spawn(
        async move {
            // Declared first so wrapped producer/input resources drop before
            // the final graph request owner on every exit path.
            let _producer_graph = handle;
            let _producer_observer = observer;
            let _producer_input = input;
            // The producer half prevents disconnect from recycling queue bytes
            // until every pending send/scan future owned by this task is gone.
            let _producer_queue_lease = producer_queue_lease;
            let closed_tx = tx.clone();
            let data_tx = tx.clone();
            let export = cut.write_chunks(move |chunk| {
                let data_tx = data_tx.clone();
                async move { data_tx.send_chunk(chunk).await }
            });
            tokio::pin!(export);
            tokio::select! {
                biased;
                _ = closed_tx.closed() => {
                    // Cancelling the pinned export future drops its move-only cut.
                }
                (cut, result) = &mut export => {
                    tx.finish(cut, result.err()).await;
                }
            }
        }
        .in_current_span(),
    );
    let body = Body::from_stream(body_stream);
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, "application/x-ndjson; charset=utf-8")],
        body,
    )
        .into_response())
}

/// Parse a mutation's graph-head precondition, if present.
///
/// `Omnigraph-If-Graph-Commit` deliberately carries one raw graph commit id,
/// not an HTTP entity tag. Keeping this graph-level CAS off `If-Match`
/// preserves the standard header for representation-specific strong ETags
/// (including the blob-cell contract). Duplicate values and entity-tag syntax
/// are rejected rather than silently reinterpreted.
fn graph_commit_expected_head(
    headers: &axum::http::HeaderMap,
) -> std::result::Result<Option<String>, ApiError> {
    let mut values = headers
        .get_all(api::GRAPH_COMMIT_PRECONDITION_HEADER)
        .iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::bad_request(
            "Omnigraph-If-Graph-Commit must be sent exactly once",
        ));
    }
    let value = value
        .to_str()
        .map_err(|_| ApiError::bad_request("Omnigraph-If-Graph-Commit is not valid UTF-8"))?
        .trim();
    if value.is_empty() {
        return Err(ApiError::bad_request(
            "Omnigraph-If-Graph-Commit must name a graph commit id",
        ));
    }
    if value == "*"
        || value.starts_with("W/")
        || value.starts_with('"')
        || value.ends_with('"')
        || value.contains(',')
    {
        return Err(ApiError::bad_request(
            "Omnigraph-If-Graph-Commit must contain one raw graph commit id, not entity-tag syntax",
        ));
    }
    Ok(Some(value.to_string()))
}

fn require_graph_commit_expected_head(
    headers: &axum::http::HeaderMap,
) -> std::result::Result<String, ApiError> {
    graph_commit_expected_head(headers)?.ok_or_else(|| {
        ApiError::bad_request(
            "Omnigraph-If-Graph-Commit is required on this conditional mutation route",
        )
    })
}

fn reject_graph_commit_expected_head(
    headers: &axum::http::HeaderMap,
    conditional_path: &str,
) -> std::result::Result<(), ApiError> {
    if headers.contains_key(api::GRAPH_COMMIT_PRECONDITION_HEADER) {
        return Err(ApiError::bad_request(format!(
            "Omnigraph-If-Graph-Commit requires the fail-closed conditional route {conditional_path}"
        )));
    }
    Ok(())
}

/// Shared backend for `/mutate` (canonical), `/mutate/if-graph-commit`,
/// and the stored-mutation arm of
/// `/queries/{name}`. Returns the bare `ChangeOutput`; each route handler
/// wraps it.
///
/// Order: parse and classify first; a branch statement then passes
/// [`refuse_wrong_door`] and [`refuse_statement_envelope`], and otherwise
/// runs the same handler body as its `/branches` route (its own Cedar action
/// and admission check). A mutation body selects its declaration, then takes
/// its target: the request `branch` (defaulting to `main` only here, so a
/// defaulted target is never mistaken for a spelled one), or for a child
/// unattached write the recorded target of its base; then the `Change` check
/// on that target, admission, and the engine call.
pub(crate) async fn run_mutate(
    state: AppState,
    handle: GraphRequest,
    ingress: IngressLease,
    session: Session,
    actor: Option<&AuthenticatedActor>,
    door: Door,
    query: &str,
    name: Option<&str>,
    params_json: Option<&Value>,
    branch: Option<String>,
    expected_head: Option<&str>,
) -> std::result::Result<ChangeOutput, ApiError> {
    let file = classify(query)?;
    refuse_process_settings(&file.settings)?;
    refuse_empty_file(&file)?;
    let queries = match file.body {
        FileBody::Queries(queries) => queries,
        FileBody::Branch(stmt) => {
            refuse_wrong_door(door, &stmt)?;
            refuse_statement_envelope(
                branch.is_some(),
                name.is_some() || params_json.is_some(),
                expected_head.is_some(),
            )?;
            return match stmt {
                BranchStmt::Write(write) => {
                    let session = session_with_prefix(&session, &file.settings)?;
                    run_branch_statement(&state, &handle, &session, actor, ingress, write).await
                }
                BranchStmt::List | BranchStmt::Read(_) => {
                    Err(read_at_write_door(stmt.statement_name()))
                }
            };
        }
        FileBody::Commit(write) => {
            if door == Door::Query {
                return Err(commit_write_at_read_door(&write));
            }
            refuse_statement_envelope(
                branch.is_some(),
                name.is_some() || params_json.is_some(),
                expected_head.is_some(),
            )?;
            let session = session_with_prefix(&session, &file.settings)?;
            return run_commit_statement(&state, &handle, &session, actor, ingress, write).await;
        }
        FileBody::Load(stmt) => {
            if door == Door::Query {
                return Err(load_at_read_door(&stmt));
            }
            refuse_statement_envelope(
                false,
                name.is_some() || params_json.is_some(),
                expected_head.is_some(),
            )?;
            let session = session_with_prefix(&session, &file.settings)?;
            return run_load_statement(
                &state,
                &handle,
                &session,
                actor,
                ingress,
                stmt,
                branch,
                query.len(),
            )
            .await;
        }
        FileBody::Show(id) => return Err(show_at_write_door(id)),
        FileBody::Explain(_) | FileBody::ExplainLoad(_) => {
            refuse_explain(door)?;
            return Err(explain_at_write_door());
        }
    };
    let decl = select_named_query_decl(queries, name)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let selected_name = decl.name;
    let params = query_params_from_json(&decl.params, params_json)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let branch = match &decl.write_mode {
        MutationWriteMode::Unattached { base: Some(base) } => {
            unattached_child_target(
                &session,
                handle.policy.as_deref(),
                actor,
                base,
                &params,
                branch,
            )
            .await?
        }
        MutationWriteMode::Publish
        | MutationWriteMode::AsCommit
        | MutationWriteMode::Unattached { base: None } => {
            let branch = branch.unwrap_or_else(|| "main".to_string());
            authorize_request(
                actor,
                handle.policy.as_deref(),
                PolicyRequest {
                    action: PolicyAction::Change,
                    branch: Some(branch.clone()),
                    target_branch: None,
                },
            )?;
            branch
        }
    };
    let actor_arc = actor
        .map(|a| Arc::clone(&a.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor.map(|a| a.actor_id.as_ref());
    // Per-actor admission: bound concurrent in-flight mutations and
    // estimated bytes per actor. Cedar runs FIRST so denied requests
    // don't consume admission slots. Estimate uses the request body
    // size as a coarse proxy; engine memory pressure can run higher.
    let est_bytes =
        query.len() as u64 + params_json.map(|p| p.to_string().len() as u64).unwrap_or(0);
    let admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;

    let query = query.to_owned();
    let expected_head = expected_head.map(str::to_owned);
    let actor_id = actor_id.map(str::to_owned);
    owned_write(&state, admission, ingress, handle.clone(), async move {
        let receipt = session
            .mutate_as_with_expected_head_receipt(
                &branch,
                &query,
                &selected_name,
                &params,
                actor_id.as_deref(),
                expected_head.as_deref(),
            )
            .await
            .map_err(ApiError::from_omni)?;
        Ok(ChangeOutput {
            branch: receipt.branch,
            query_name: selected_name,
            affected_nodes: receipt.result.affected_nodes,
            affected_edges: receipt.result.affected_edges,
            actor_id,
            commit: receipt.commit.as_ref().map(api::commit_output),
            outcome: None,
        })
    })
    .await
}

/// The recorded target a child unattached write addresses, after the `change`
/// check on it: a denial is masked as the commit 404, and a present request
/// `branch` must then spell the target (an omitted one is accepted).
async fn unattached_child_target(
    session: &Session,
    policy: Option<&PolicyEngine>,
    actor: Option<&AuthenticatedActor>,
    base: &UnattachedBase,
    params: &ParamMap,
    branch: Option<String>,
) -> std::result::Result<String, ApiError> {
    let base_id = match base {
        UnattachedBase::Literal(id) => id.clone(),
        UnattachedBase::Param(name) => match params.get(name) {
            Some(Literal::String(id)) => id.clone(),
            _ => {
                return Err(ApiError::bad_request(format!(
                    "parameter '${name}' names the base of an unattached branch and must be a String"
                )));
            }
        },
    };
    let target = session
        .commit_target_branch(&base_id)
        .await
        .map_err(|error| change_route_commit_lookup_error(error, &base_id))?;
    match authorize(
        actor,
        policy,
        PolicyRequest {
            action: PolicyAction::Change,
            branch: Some(target.clone()),
            target_branch: None,
        },
    )? {
        Authz::Allowed => {}
        Authz::Denied(_) => {
            return Err(ApiError::not_found(format!("commit '{base_id}' not found")));
        }
    }
    if let Some(requested) = branch
        && requested != target
    {
        return Err(ApiError::bad_request(format!(
            "branch '{requested}' is not the recorded target '{target}' of unattached branch \
             '{base_id}'; name that branch or omit the branch"
        )));
    }
    Ok(target)
}

/// Shared backend for `/query` and the stored-read arm of `/queries/{name}`.
///
/// Order: parse and classify first; `branch list` then passes
/// [`refuse_wrong_door`] and [`refuse_statement_envelope`], and otherwise runs
/// the handler body of `GET /branches` (a scope-free `read` check); `show`
/// passes the same refusals and the same scope-free `read` check before it
/// reads the session's effective settings. A declared query resolves and
/// authorizes its read target, refuses mutations, and runs.
///
/// Intentionally does **not** take [`AppState`] (unlike [`run_mutate`]):
/// reads use the bounded server observer lane, so there is no `state.workload` consumer.
pub(crate) async fn run_query(
    handle: GraphRequest,
    session: Session,
    actor: Option<&AuthenticatedActor>,
    door: Door,
    query: &str,
    name: Option<&str>,
    params_json: Option<&Value>,
    branch: Option<String>,
    snapshot: Option<String>,
) -> std::result::Result<ReadDispatch, ApiError> {
    let file = classify(query)?;
    refuse_process_settings(&file.settings)?;
    refuse_empty_file(&file)?;
    let queries = match file.body {
        FileBody::Queries(queries) => queries,
        FileBody::Branch(stmt) => {
            refuse_wrong_door(door, &stmt)?;
            refuse_statement_envelope(
                branch.is_some() || snapshot.is_some(),
                name.is_some() || params_json.is_some(),
                false,
            )?;
            return match stmt {
                BranchStmt::List => Ok(ReadDispatch::BranchList(
                    branch_list_body(&handle, actor).await?,
                )),
                BranchStmt::Read(read) => Ok(ReadDispatch::BranchRead(
                    branch_read_body(&handle, &session, actor, read).await?,
                )),
                BranchStmt::Write(write) => Err(control_write_at_read_door(&write)),
            };
        }
        FileBody::Commit(write) => {
            refuse_statement_envelope(
                branch.is_some() || snapshot.is_some(),
                name.is_some() || params_json.is_some(),
                false,
            )?;
            return Err(commit_write_at_read_door(&write));
        }
        FileBody::Load(stmt) => {
            refuse_statement_envelope(
                branch.is_some() || snapshot.is_some(),
                name.is_some() || params_json.is_some(),
                false,
            )?;
            return Err(load_at_read_door(&stmt));
        }
        FileBody::Show(id) => {
            refuse_statement_envelope(
                branch.is_some() || snapshot.is_some(),
                name.is_some() || params_json.is_some(),
                false,
            )?;
            authorize_scope_free_read(&handle, actor)?;
            let session = session_with_prefix(&session, &file.settings)?;
            return Ok(ReadDispatch::Show(session.show(id)));
        }
        body @ FileBody::Explain(_) => {
            refuse_explain(door)?;
            body.into_read_declarations()
                .map_err(ApiError::bad_request)?
        }
        FileBody::ExplainLoad(stmt) => {
            refuse_explain(door)?;
            refuse_statement_envelope(
                snapshot.is_some(),
                name.is_some() || params_json.is_some(),
                false,
            )?;
            let branch = branch.unwrap_or_else(|| "main".to_string());
            authorize_request(
                actor,
                handle.policy.as_deref(),
                PolicyRequest {
                    action: PolicyAction::Change,
                    branch: Some(branch.clone()),
                    target_branch: None,
                },
            )?;
            let target = ReadTarget::Branch(branch);
            let session = session_with_prefix(&session, &file.settings)?;
            let (result, graph_commit_id) = session
                .explain_load(target.clone(), &stmt)
                .await
                .map_err(ApiError::from_omni)?;
            return Ok(ReadDispatch::Rows {
                query_name: stmt.statement_name().to_string(),
                target,
                result,
                graph_commit_id,
            });
        }
    };
    let target = resolve_authorized_read_target(&handle, actor, branch, snapshot).await?;
    let query_decl = select_named_query_decl(queries, name)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if !query_decl.mutations.is_empty() {
        return Err(ApiError::bad_request(format!(
            "query '{}' contains mutations (insert/update/delete); use POST /mutate for write queries",
            query_decl.name
        )));
    }
    let selected_name = query_decl.name.clone();
    let params = query_params_from_json(&query_decl.params, params_json)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let (result, graph_commit_id) = session
        .query_with_head(target.clone(), query, &selected_name, &params)
        .await
        .map_err(ApiError::from_omni)?;
    Ok(ReadDispatch::Rows {
        query_name: selected_name,
        target,
        result,
        graph_commit_id,
    })
}

/// Resolve one branch-or-snapshot read target and apply the graph's Cedar
/// `read` gate. Every HTTP carrier that accepts this target shape uses this
/// helper so snapshot-to-policy-branch resolution cannot drift by route.
pub(crate) async fn resolve_authorized_read_target(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
    branch: Option<String>,
    snapshot: Option<String>,
) -> std::result::Result<ReadTarget, ApiError> {
    resolve_authorized_read_target_with_cause(handle, actor, branch, snapshot)
        .await
        .map_err(|(mapped, _)| mapped)
}

/// [`resolve_authorized_read_target`], also returning the log-safe class of
/// an engine failure beside the mapped error, so a redacting caller can log
/// the class the mapping discards. Refusals that are not engine failures
/// carry no class.
async fn resolve_authorized_read_target_with_cause(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
    branch: Option<String>,
    snapshot: Option<String>,
) -> std::result::Result<ReadTarget, (ApiError, Option<RedactedCause>)> {
    if branch.is_some() && snapshot.is_some() {
        return Err((
            ApiError::bad_request("request may specify branch or snapshot, not both"),
            None,
        ));
    }

    let target = read_target_from_request(branch, snapshot);
    let policy_branch = match &target {
        ReadTarget::Branch(branch) => Some(branch.clone()),
        ReadTarget::Snapshot(_) if handle.policy.is_some() && actor.is_some() => handle
            .engine
            .resolved_branch_of(target.clone())
            .await
            .map(|branch| branch.or_else(|| Some("main".to_string())))
            .map_err(engine_error_with_cause)?,
        ReadTarget::Snapshot(_) => None,
    };
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: policy_branch,
            target_branch: None,
        },
    )
    .map_err(|refused| (refused, None))?;
    Ok(target)
}

fn engine_error_with_cause(error: OmniError) -> (ApiError, Option<RedactedCause>) {
    let cause = RedactedCause::of(&error);
    (ApiError::from_omni(error), Some(cause))
}

#[utoipa::path(
    post,
    path = "/mutate",
    tag = "mutations",
    operation_id = "mutate",
    request_body = ChangeRequest,
    responses(
        (status = 200, description = "Mutation results", body = ChangeOutput),
        (status = 400, description = "Bad request - also returned when `branch list` or a branch control read (`branch diff`, `branch merge-base`, `branch merge … dry run`) arrives here instead of POST /query, when a request target accompanies a branch statement, when a name or parameters accompany a branch statement, and when a commit precondition accompanies a branch statement", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "`branch delete` of a branch that does not exist, as DELETE /branches/{branch} answers", body = ErrorOutput),
        (status = 409, description = "Write-authority conflict; also `branch create` of a branch that already exists, and a conflicting `branch merge`, whose body carries `merge_conflicts`", body = ErrorOutput),
        (status = 413, description = "Keyed write exceeds the per-commit entity or byte ceiling", body = ErrorOutput),
        (status = 424, description = "An allowed external Blob source could not be probed or read", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Apply a GQ mutation to a branch (canonical mutation endpoint).
///
/// Writes to the named `branch` (defaults to `main`). Mutations are atomic
/// per call and produce a new commit. Returns counts of nodes and edges
/// affected. **Destructive**: on success the branch is updated; rejected
/// mutations may still acquire locks briefly. Returns 409 when the prepared
/// write authority changes before effects.
///
/// Conditional callers use `POST /mutate/if-graph-commit`, which requires and
/// validates `Omnigraph-If-Graph-Commit`. This endpoint rejects that header.
///
/// Pairs with `POST /query` (read-only).
///
/// The GQ statements `branch create`, `branch delete`, and `branch merge`
/// (grammar: `BranchStmt` in `omnigraph-compiler`) are also served
/// here, with no `branch`, `name`, or `params`: each runs the handler body
/// of its `/branches` route (same Cedar action, admission check, and errors,
/// including the 409 of a conflicting merge) and answers a `ChangeOutput`
/// whose `outcome` names the effect. `branch list` and the branch control
/// reads (`branch diff`, `branch merge-base`, `branch merge … dry run`) are
/// rejected with 400; send them to `POST /query`.
pub(crate) async fn server_mutate(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    headers: axum::http::HeaderMap,
    ApiJson(request): ApiJson<ChangeRequest>,
) -> std::result::Result<Json<ChangeOutput>, ApiError> {
    reject_graph_commit_expected_head(&headers, "/mutate/if-graph-commit")?;
    let session = state.session(&handle, request.settings.as_ref())?;
    Ok(Json(
        run_mutate(
            state,
            handle,
            ingress,
            session,
            actor.as_ref().map(|Extension(actor)| actor),
            Door::Mutate,
            &request.query,
            request.name.as_deref(),
            request.params.as_ref(),
            request.branch,
            None,
        )
        .await?,
    ))
}

#[utoipa::path(
    post,
    path = "/mutate/if-graph-commit",
    tag = "mutations",
    operation_id = "mutate_if_graph_commit",
    request_body = ChangeRequest,
    params(
        ("Omnigraph-If-Graph-Commit" = String, Header, description = "Required raw graph-head commit id. The mutation runs only while the branch's effective head still equals it."),
    ),
    responses(
        (status = 200, description = "Conditional mutation results", body = ChangeOutput),
        (status = 400, description = "Missing, duplicate, malformed, or invalid request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Write-authority conflict", body = ErrorOutput),
        (status = 412, description = "Graph-commit precondition failed; the write had no effect", body = ErrorOutput),
        (status = 413, description = "Keyed write exceeds the per-commit entity or byte ceiling", body = ErrorOutput),
        (status = 424, description = "An allowed external Blob source could not be probed or read", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Apply a mutation only while the branch still has the required graph head.
///
/// This explicit conditional route requires `Omnigraph-If-Graph-Commit` and
/// validates its precondition before mutation effects. `/mutate` rejects the
/// precondition header; conditional callers must use this route.
pub(crate) async fn server_mutate_if_graph_commit(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    headers: axum::http::HeaderMap,
    ApiJson(request): ApiJson<ChangeRequest>,
) -> std::result::Result<Json<ChangeOutput>, ApiError> {
    let expected_head = require_graph_commit_expected_head(&headers)?;
    let session = state.session(&handle, request.settings.as_ref())?;
    Ok(Json(
        run_mutate(
            state,
            handle,
            ingress,
            session,
            actor.as_ref().map(|Extension(actor)| actor),
            Door::Mutate,
            &request.query,
            request.name.as_deref(),
            request.params.as_ref(),
            request.branch,
            Some(&expected_head),
        )
        .await?,
    ))
}

/// Path parameter for `POST /queries/{name}`.
#[derive(Deserialize)]
pub(crate) struct QueryNamePath {
    pub(crate) name: String,
}

pub(crate) fn parse_optional_invoke_body(
    body: Bytes,
) -> std::result::Result<InvokeStoredQueryRequest, ApiError> {
    if body.is_empty() {
        return Ok(InvokeStoredQueryRequest::default());
    }
    serde_json::from_slice::<Option<InvokeStoredQueryRequest>>(&body)
        .map(|request| request.unwrap_or_default())
        .map_err(|err| {
            ApiError::bad_request(format!("invalid stored-query invocation body: {err}"))
        })
}

#[utoipa::path(
    post,
    path = "/queries/{name}",
    tag = "queries",
    operation_id = "invoke_query",
    params(
        ("name" = String, Path, description = "Stored query name (the registry key)"),
    ),
    request_body = Option<InvokeStoredQueryRequest>,
    responses(
        (status = 200, description = "Read envelope (ReadOutput) or mutation envelope (ChangeOutput), serialized untagged", body = InvokeStoredQueryResponse),
        (status = 400, description = "Bad request (param type error; snapshot on a stored mutation)", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden (the inner `change` gate for a stored mutation)", body = ErrorOutput),
        (status = 404, description = "Unknown stored query, or `invoke_query` denied — indistinguishable to a caller without the grant", body = ErrorOutput),
        (status = 409, description = "Stored mutation write-authority conflict, or a full-text index requires explicit rebuilding (full_text_index_rebuild_required) or has no segment at the snapshot (full_text_index_required); neither is cleared by retrying", body = ErrorOutput),
        (status = 413, description = "Stored keyed mutation exceeds the per-commit entity or byte ceiling", body = ErrorOutput),
        (status = 424, description = "A stored mutation could not probe or read an allowed external Blob source", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 500, description = "Policy evaluation error (a denial is reported as 404, not 500)", body = ErrorOutput),
        (status = 503, description = "A stored mutation is blocked by a durable recovery intent", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Invoke a curated, server-side stored query by name.
///
/// The query source comes from the graph's `queries:` registry, not the
/// request body — callers send only runtime inputs (`params`, `branch`,
/// `snapshot`). Gated by the `invoke_query` Cedar action at the boundary;
/// a stored *mutation* additionally passes the engine's `change` gate
/// (double-gated). An actor **without** `invoke_query` cannot tell a denied
/// query from a missing one — both return the same 404, so the catalog
/// can't be probed without the grant. Once `invoke_query` is held, the
/// inner `read`/`change` gate may surface a 403 for an existing query the
/// actor can't run (the intended double-gate signal).
pub(crate) async fn server_invoke_query(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(QueryNamePath { name }): ApiPath<QueryNamePath>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<InvokeStoredQueryResponse>, ApiError> {
    reject_graph_commit_expected_head(&headers, &format!("/queries/{name}/if-graph-commit"))?;
    invoke_stored_query(state, handle, ingress, actor, name, body, None).await
}

#[utoipa::path(
    post,
    path = "/queries/{name}/if-graph-commit",
    tag = "queries",
    operation_id = "invoke_query_if_graph_commit",
    params(
        ("name" = String, Path, description = "Stored mutation name (the registry key)"),
        ("Omnigraph-If-Graph-Commit" = String, Header, description = "Required raw graph-head commit id. The stored mutation runs only while the branch's effective head still equals it."),
    ),
    request_body = Option<InvokeStoredQueryRequest>,
    responses(
        (status = 200, description = "Stored conditional mutation result", body = ChangeOutput),
        (status = 400, description = "Missing, duplicate, malformed, read-only, or invalid invocation", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden (the inner `change` gate)", body = ErrorOutput),
        (status = 404, description = "Unknown stored mutation, or `invoke_query` denied", body = ErrorOutput),
        (status = 409, description = "Stored mutation write-authority conflict", body = ErrorOutput),
        (status = 412, description = "Stored mutation graph-commit precondition failed; the write had no effect", body = ErrorOutput),
        (status = 413, description = "Stored keyed mutation exceeds the per-commit entity or byte ceiling", body = ErrorOutput),
        (status = 424, description = "A stored mutation could not probe or read an allowed external Blob source", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 500, description = "Policy evaluation error (a denial is reported as 404, not 500)", body = ErrorOutput),
        (status = 503, description = "A stored mutation is blocked by a durable recovery intent", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Invoke one stored mutation with a required graph-head precondition.
///
/// A distinct path makes support observable before any mutation runs; older
/// servers return 404 instead of ignoring an unknown conditional header.
pub(crate) async fn server_invoke_query_if_graph_commit(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(QueryNamePath { name }): ApiPath<QueryNamePath>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<InvokeStoredQueryResponse>, ApiError> {
    let expected_head = require_graph_commit_expected_head(&headers)?;
    invoke_stored_query(
        state,
        handle,
        ingress,
        actor,
        name,
        body,
        Some(expected_head),
    )
    .await
}

async fn invoke_stored_query(
    state: AppState,
    handle: GraphRequest,
    ingress: IngressLease,
    actor: Option<Extension<AuthenticatedActor>>,
    name: String,
    body: Bytes,
    expected_head: Option<String>,
) -> std::result::Result<Json<InvokeStoredQueryResponse>, ApiError> {
    let req = parse_optional_invoke_body(body)?;
    // A caller without `invoke_query` can't tell a denial from a missing
    // query: both 404 with this exact message, so the catalog can't be
    // probed without the grant. (A caller that holds invoke_query may still
    // see the inner gate's 403 for an existing query it can't run — intended.)
    const NOT_FOUND: &str = "stored query not found";
    let actor_ref = actor.as_ref().map(|Extension(actor)| actor);

    // Boundary gate (authentication already ran in `require_bearer_auth`).
    // A denial is hidden as 404 (deny == missing, so the catalog can't be
    // probed without the grant), but operational failures (401 missing bearer,
    // 500 policy-evaluation error) propagate with their true status via `?`
    // rather than being masked as a missing query.
    match authorize(
        actor_ref,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::InvokeQuery,
            // Graph-scoped: no branch dimension. The per-branch/snapshot
            // access is enforced by the inner read/change gate in the
            // runner, so the outer gate must not resolve a branch (doing so
            // was wrong for snapshot reads).
            branch: None,
            target_branch: None,
        },
    )? {
        Authz::Allowed => {}
        Authz::Denied(_) => return Err(ApiError::not_found(NOT_FOUND)),
    }

    // Resolve against the per-graph registry (same 404 on a miss).
    let stored = handle
        .queries
        .as_ref()
        .and_then(|registry| registry.lookup(&name))
        .ok_or_else(|| ApiError::not_found(NOT_FOUND))?;

    // Detach what we need before `handle` moves into the runner — the
    // registry borrow lives inside `handle`.
    let source = Arc::clone(&stored.source);
    let query_name = stored.name.clone();
    let is_mutation = stored.is_mutation();

    // RFC-011 D3: the CLI verb asserts the stored query's kind. `query <name>`
    // sends `expect_mutation: false`, `mutate <name>` sends `true`; a mismatch
    // is rejected here so the wrong verb errors instead of silently running.
    if let Some(expected) = req.expect_mutation {
        if expected != is_mutation {
            let (actual, verb) = if is_mutation {
                ("mutation", "mutate")
            } else {
                ("read", "query")
            };
            return Err(ApiError::bad_request(format!(
                "'{query_name}' is a {actual} — use omnigraph {verb} {query_name}"
            )));
        }
    }

    info!(
        graph_id = %handle.key.graph_id,
        actor = ?actor_ref.map(|a| a.actor_id.as_ref()),
        query = %query_name,
        kind = if is_mutation { "mutate" } else { "read" },
        "stored query invoked"
    );

    let session = state.session(&handle, None)?;
    if is_mutation {
        if req.snapshot.is_some() {
            return Err(ApiError::bad_request(
                "stored mutation cannot target a snapshot",
            ));
        }
        let output = run_mutate(
            state,
            handle,
            ingress,
            session,
            actor_ref,
            Door::Mutate,
            &source,
            Some(&query_name),
            req.params.as_ref(),
            req.branch,
            expected_head.as_deref(),
        )
        .await?;
        Ok(Json(InvokeStoredQueryResponse::Change(output)))
    } else {
        if expected_head.is_some() {
            return Err(ApiError::bad_request(
                "the graph-commit conditional route applies only to stored mutations",
            ));
        }
        let output = run_query(
            handle,
            session,
            actor_ref,
            Door::Query,
            &source,
            Some(&query_name),
            req.params.as_ref(),
            req.branch,
            req.snapshot,
        )
        .await?
        .into_read_output()?;
        Ok(Json(InvokeStoredQueryResponse::Read(output)))
    }
}

#[utoipa::path(
    get,
    path = "/queries",
    tag = "queries",
    operation_id = "list_queries",
    responses(
        (status = 200, description = "Stored-query catalog (every stored query, with typed params)", body = QueriesCatalogOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// List the graph's exposed stored queries as a typed tool catalog.
///
/// Returns every stored query in the `queries:` registry, each
/// with its MCP tool name, read/mutate flag, description/instruction, and
/// typed parameters — enough for a client to register them as tools without
/// fetching `.gq` source. Cluster-served graphs have no per-query expose flag,
/// so the catalog lists them all. Read-gated; the catalog is graph-wide (branch
/// independent — `read` is authorized against `main`). **Not** Cedar-filtered
/// per query yet, so it can list a query whose `invoke_query` the caller
/// lacks (a known gap until per-query authorization lands).
pub(crate) async fn server_list_queries(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> std::result::Result<Json<QueriesCatalogOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: Some("main".to_string()),
            target_branch: None,
        },
    )?;
    let queries = match handle.queries.as_ref() {
        Some(registry) => registry
            .iter()
            .filter(|q| q.expose)
            .map(api::query_catalog_entry)
            .collect(),
        None => Vec::new(),
    };
    Ok(Json(QueriesCatalogOutput { queries }))
}

#[utoipa::path(
    get,
    path = "/schema",
    tag = "schema",
    operation_id = "getSchema",
    responses(
        (status = 200, description = "Current schema source", body = SchemaOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Read the current schema source.
///
/// Returns the project's schema as a single string in `.pg` source form.
/// Useful for clients that want to introspect available types and properties
/// before constructing GQ queries. Read-only.
pub(crate) async fn server_schema_get(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> std::result::Result<Json<SchemaOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: None,
            target_branch: None,
        },
    )?;
    let (schema_source, system_columns) = {
        let db = &handle.engine;
        (db.schema_source().to_string(), db.catalog().system_columns)
    };
    Ok(Json(SchemaOutput {
        schema_source,
        system_columns: Some(system_columns.into()),
        catalogue_source: Some(omnigraph_compiler::catalogue_declarations()),
    }))
}

/// Authorize one load target without touching request data. Whether the load
/// forks its branch is the engine's to say: it lists the branches again when
/// it runs, and marks every failure after its fork.
async fn authorize_load_scope(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
    branch: &str,
    from: Option<&str>,
) -> std::result::Result<(), ApiError> {
    require_canonical_branches([Some(branch), from])?;
    let branch_exists = handle
        .engine
        .branch_list()
        .await
        .map_err(ApiError::from_omni)?
        .into_iter()
        .any(|name| name == branch);

    if !branch_exists {
        match from {
            // Fork-if-missing is opt-in by presence of `from`; without it a
            // typo'd branch name must surface as an error, not silently
            // create a fork and land the data there.
            None => {
                return Err(ApiError::not_found(format!(
                    "branch '{branch}' not found; pass `from` to create it"
                )));
            }
            Some(from) => authorize_request(
                actor,
                handle.policy.as_deref(),
                PolicyRequest {
                    action: PolicyAction::BranchCreate,
                    branch: Some(from.to_string()),
                    target_branch: Some(branch.to_string()),
                },
            )?,
        }
    }
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Change,
            branch: Some(branch.to_string()),
            target_branch: None,
        },
    )?;
    Ok(())
}

/// JSON `POST /load`:
/// branch-exists / fork-if-`from` check, Cedar authorization, admission, the
/// bulk `load_as`, and the `IngestOutput` mapping.
async fn run_json_load(
    state: AppState,
    handle: GraphRequest,
    ingress: IngressLease,
    actor: Option<&AuthenticatedActor>,
    request: IngestRequest,
) -> std::result::Result<IngestOutput, ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    let from = request.from;
    let mode = request.mode.unwrap_or(omnigraph::loader::LoadMode::Merge);
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor.map(|actor| actor.actor_id.as_ref());

    authorize_load_scope(&handle, actor, &branch, from.as_deref()).await?;
    let est_bytes = request.data.len() as u64;
    let admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;
    let session = state.session(&handle, None)?;
    let actor_id = actor_id.map(str::to_owned);
    owned_write(&state, admission, ingress, handle.clone(), async move {
        let receipt = session
            .load_as_with_receipt(
                &branch,
                from.as_deref(),
                &request.data,
                mode,
                actor_id.as_deref(),
            )
            .await
            .map_err(ApiError::from_omni)?;

        Ok(ingest_receipt_output(
            handle.uri.as_str(),
            &receipt,
            &session.catalog(),
            mode,
            actor_id,
        ))
    })
    .await
}

#[utoipa::path(
    post,
    path = "/load",
    tag = "mutations",
    operation_id = "load",
    request_body = IngestRequest,
    responses(
        (status = 200, description = "Load results", body = IngestOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Prepared load authority changed before effects", body = ErrorOutput),
        (status = 413, description = "Load input or external Blob admission exceeds a bounded per-operation entity or byte ceiling", body = ErrorOutput),
        (status = 424, description = "An allowed external Blob source could not be probed or read", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Compatibility-load NDJSON data through a JSON envelope.
///
/// `data` is NDJSON with one record per line. `mode` controls behavior on
/// existing entities: `merge` upserts by id (default), `append` strictly inserts
/// absent ids, and `overwrite` replaces type data. Branch creation is opt-in by
/// presence of `from`: with `from` set, a missing `branch` is created from
/// it; without `from`, `branch` must already exist — a missing branch is a
/// 404, never an implicit fork. **Destructive** when `mode` is `overwrite`
/// or when the load produces conflicting writes.
pub(crate) async fn server_load(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiJson(request): ApiJson<IngestRequest>,
) -> std::result::Result<Json<IngestOutput>, ApiError> {
    Ok(Json(
        run_json_load(
            state,
            handle,
            ingress,
            actor.as_ref().map(|Extension(actor)| actor),
            request,
        )
        .await?,
    ))
}

async fn collect_graph_batch_body(body: Body) -> std::result::Result<Bytes, ApiError> {
    let mut body = body.into_data_stream();
    let mut data = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|err| {
            ApiError::bad_request(format!("failed to read graph-batch request body: {err}"))
        })?;
        let actual = data.len().saturating_add(chunk.len());
        if actual > INGEST_REQUEST_BODY_LIMIT_BYTES {
            return Err(ApiError::resource_limit(
                format!(
                    "graph-batch request body exceeds {} bytes",
                    INGEST_REQUEST_BODY_LIMIT_BYTES
                ),
                api::ResourceLimitOutput {
                    resource: "graph_batch_request_bytes".to_string(),
                    limit: INGEST_REQUEST_BODY_LIMIT_BYTES as u64,
                    actual: actual as u64,
                },
            ));
        }
        data.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(data))
}

#[utoipa::path(
    post,
    path = "/load/ndjson",
    tag = "mutations",
    operation_id = "loadNdjson",
    params(GraphBatchLoadQuery),
    request_body(
        content = String,
        content_type = "application/x-ndjson",
        description = "Strict raw graph-level NDJSON. Each nonblank line is exactly one node envelope {\"type\":\"<Node>\",\"id\":\"<entity-id>\",\"data\":{...}} or edge envelope {\"edge\":\"<Edge>\",\"id\":\"<entity-id>\",\"from\":\"<src-id>\",\"to\":\"<dst-id>\",\"data\":{...}}. `data` defaults to {} and holds user properties; the optional top-level `id` follows ordinary ID semantics. Legacy-vintage graphs also accept `data.id` as identity when top-level `id` is absent and refuse both placements together. Duplicate, unknown, reserved physical, and noncanonical supplied entity-ID members are refused."
    ),
    responses(
        (status = 200, description = "One committed graph-batch result", body = GraphBatchLoadOutput),
        (status = 400, description = "Malformed query or graph batch", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Target branch missing without `from`", body = ErrorOutput),
        (status = 409, description = "Prepared load authority changed before effects", body = ErrorOutput),
        (status = 413, description = "Request, load, or external Blob admission exceeds a bounded ceiling", body = ErrorOutput),
        (status = 415, description = "Content-Type must be application/x-ndjson", body = ErrorOutput),
        (status = 424, description = "An allowed external Blob source could not be probed or read", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Load one strict, bounded graph-level NDJSON batch.
///
/// Bearer authentication runs in middleware. This handler completes both
/// branch authorization checks before polling the raw body. A successful
/// response describes logical schema declarations only and is returned after
/// the ordinary graph commit is visible.
pub(crate) async fn server_load_ndjson(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiQuery(query): ApiQuery<GraphBatchLoadQuery>,
    request: Request,
) -> std::result::Result<Json<GraphBatchLoadOutput>, ApiError> {
    let ingress = request
        .extensions()
        .get::<IngressLease>()
        .cloned()
        .ok_or_else(|| ApiError::internal("missing ingress reservation"))?;
    let actor = actor.as_ref().map(|Extension(actor)| actor);
    let branch = query.branch.unwrap_or_else(|| "main".to_string());
    let from = query.from;
    let mode = query.mode.unwrap_or(omnigraph::loader::LoadMode::Merge);

    authorize_load_scope(&handle, actor, &branch, from.as_deref()).await?;

    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    if !matches!(content_type, Some(value) if value.eq_ignore_ascii_case("application/x-ndjson")) {
        return Err(ApiError::unsupported_media_type(
            "graph-batch load requires Content-Type: application/x-ndjson",
        ));
    }

    if let Some(actual) = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|actual| *actual > INGEST_REQUEST_BODY_LIMIT_BYTES as u64)
    {
        return Err(ApiError::resource_limit(
            format!(
                "graph-batch request body exceeds {} bytes",
                INGEST_REQUEST_BODY_LIMIT_BYTES
            ),
            api::ResourceLimitOutput {
                resource: "graph_batch_request_bytes".to_string(),
                limit: INGEST_REQUEST_BODY_LIMIT_BYTES as u64,
                actual,
            },
        ));
    }

    let deadline = request
        .extensions()
        .get::<ingress::BodyDeadline>()
        .copied()
        .ok_or_else(|| ApiError::internal("missing request body deadline"))?;
    let data = tokio::time::timeout_at(deadline.0, collect_graph_batch_body(request.into_body()))
        .await
        .map_err(|_| ingress::body_timeout())??;
    ingress
        .shrink(data.len() as u64)
        .map_err(ApiError::from_workload_reject)?;
    let data = std::str::from_utf8(&data)
        .map_err(|_| ApiError::bad_request("graph-batch request body must be valid UTF-8"))?
        .to_owned();
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor.map(|actor| actor.actor_id.as_ref());
    let admission = state
        .workload
        .try_admit(&actor_arc, data.len() as u64)
        .map_err(ApiError::from_workload_reject)?;

    let session = state.session(&handle, None)?;
    let actor_id = actor_id.map(str::to_owned);
    owned_write(&state, admission, ingress, handle.clone(), async move {
        let receipt = session
            .load_graph_batch_as_with_receipt(
                &branch,
                from.as_deref(),
                &data,
                mode,
                actor_id.as_deref(),
            )
            .await
            .map_err(ApiError::from_omni)?;
        Ok(Json(graph_batch_load_receipt_output(
            &receipt,
            &session.catalog(),
            mode,
            actor_id,
        )))
    })
    .await
}

#[utoipa::path(
    get,
    path = "/branches",
    tag = "branches",
    operation_id = "listBranches",
    responses(
        (status = 200, description = "List of branches", body = BranchListOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// List all branches.
///
/// Returns branch names sorted by name in byte order. Read-only. The GQ statement
/// `branch list` on `POST /query` runs the same body.
pub(crate) async fn server_branch_list(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
) -> std::result::Result<Json<BranchListOutput>, ApiError> {
    let branches = branch_list_body(&handle, actor.as_ref().map(|Extension(actor)| actor)).await?;
    Ok(Json(BranchListOutput { branches }))
}

/// The scope-free `read` decision shared by `GET /branches`, `branch list`,
/// and `show`: the graph's Cedar `read` gate with no branch in the request.
fn authorize_scope_free_read(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
) -> std::result::Result<(), ApiError> {
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: None,
            target_branch: None,
        },
    )
}

/// Body shared by `GET /branches` and the `branch list` statement: one
/// scope-free `read` check, then the names in byte order.
async fn branch_list_body(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
) -> std::result::Result<Vec<String>, ApiError> {
    authorize_scope_free_read(handle, actor)?;
    let mut branches = handle
        .engine
        .branch_list()
        .await
        .map_err(ApiError::from_omni)?;
    branches.sort();
    Ok(branches)
}

#[utoipa::path(
    post,
    path = "/branches",
    tag = "branches",
    operation_id = "createBranch",
    request_body = BranchCreateRequest,
    responses(
        (status = 200, description = "Branch created", body = BranchCreateOutput),
        (status = 400, description = "Bad request, or both `from` and `from_commit`", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        // A commit the actor may not branch from is indistinguishable from
        // one that does not exist.
        (status = 404, description = "`from` names no branch, or `from_commit` names no commit the actor may branch from", body = ErrorOutput),
        (status = 409, description = "Branch already exists, or the commit `from_commit` names cannot start a branch; see branch_start_refusal", body = ErrorOutput),
        (status = 410, description = "The commit `from_commit` names is no longer retained; see branch_start_refusal", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Create a new branch.
///
/// Forks `name` off of `from` (defaults to `main`) at its head, or starts it
/// at the published graph commit `from_commit` with that commit's data and
/// history. The receipt names the branch forked and the commit the new
/// branch starts at. The new branch shares backing dataset data with its
/// parent until it is mutated. Returns 409 if `name` already exists. The GQ
/// statement `branch create` on `POST /mutate` runs the same body.
pub(crate) async fn server_branch_create(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiJson(request): ApiJson<BranchCreateRequest>,
) -> std::result::Result<Json<BranchCreateOutput>, ApiError> {
    let start = match (request.from, request.from_commit) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "request may specify from or from_commit, not both",
            ));
        }
        (_, Some(commit)) => ReadTarget::snapshot(SnapshotId::new(commit)),
        (from, None) => ReadTarget::branch(from.unwrap_or_else(|| "main".to_string())),
    };
    let created = branch_create_body(
        &state,
        &handle,
        actor.as_ref().map(|Extension(actor)| actor),
        ingress,
        start,
        &request.name,
    )
    .await?;
    Ok(Json(BranchCreateOutput {
        uri: handle.uri.clone(),
        from: created.from,
        name: request.name,
        actor_id: actor.map(|Extension(actor)| actor.actor_id.as_ref().to_string()),
        from_commit_id: created.from_commit_id,
    }))
}

/// Body shared by `POST /branches` and the `branch create` statement: the
/// `branch_create` check on (the start's branch, `name`), admission, the
/// engine call. A start at a commit is checked on the branch a control on
/// the commit addresses, and a denial answers the 404 of an unknown commit,
/// as the commit controls do, so the route discloses no commit the actor
/// may not branch from.
async fn branch_create_body(
    state: &AppState,
    handle: &GraphRequest,
    actor: Option<&AuthenticatedActor>,
    ingress: IngressLease,
    start: ReadTarget,
    name: &str,
) -> std::result::Result<BranchCreated, ApiError> {
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let source = match &start {
        ReadTarget::Branch(branch) => branch.clone(),
        ReadTarget::Snapshot(commit_id) => handle
            .engine
            .commit_target_branch(commit_id.as_str())
            .await
            .map_err(|error| change_route_commit_lookup_error(error, commit_id.as_str()))?,
    };
    let decision = authorize(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchCreate,
            branch: Some(source),
            target_branch: Some(name.to_string()),
        },
    )?;
    match (decision, &start) {
        (Authz::Allowed, _) => {}
        (Authz::Denied(message), ReadTarget::Branch(_)) => {
            return Err(ApiError::forbidden(message));
        }
        (Authz::Denied(_), ReadTarget::Snapshot(commit_id)) => {
            return Err(ApiError::not_found(format!(
                "commit '{commit_id}' not found"
            )));
        }
    }
    // Branch metadata only — small constant bytes estimate. The Lance
    // shallow-clone work is bounded by the parent's manifest size, not
    // the request body.
    let admission = state
        .workload
        .try_admit(&actor_arc, 256)
        .map_err(ApiError::from_workload_reject)?;
    let engine = Arc::clone(&handle.engine);
    let name = name.to_owned();
    let actor_id = actor.map(|actor| actor.actor_id.to_string());
    owned_write(state, admission, ingress, handle.clone(), async move {
        engine
            .branch_create_from_as(start, &name, actor_id.as_deref())
            .await
            .map_err(ApiError::from_omni)
    })
    .await
}

/// Path-param shape for [`server_branch_delete`]. Named-field
/// deserialization (rather than `Path<String>` or `Path<(String,)>`)
/// keeps the extractor stable across single-mode flat routes and
/// multi-mode nested routes: the `{branch}` capture is picked by
/// name and any other captures in scope (e.g. `{graph_id}` in
/// multi-mode) are ignored without breaking deserialization.
///
/// Closes the "handler path-extractor type is positional and breaks
/// when route nesting changes" class.
#[derive(Deserialize)]
pub(crate) struct BranchPath {
    pub(crate) branch: String,
}

#[utoipa::path(
    delete,
    path = "/branches/{branch}",
    tag = "branches",
    operation_id = "deleteBranch",
    params(
        ("branch" = String, Path, description = "Branch name to delete"),
    ),
    responses(
        (status = 200, description = "Branch deleted", body = BranchDeleteOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Branch not found", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Delete a branch.
///
/// **Irreversible.** Removes the branch pointer; commits remain reachable
/// only if referenced by another branch. Returns 404 if the branch does not
/// exist. The GQ statement `branch delete` on `POST /mutate` runs the same
/// body.
pub(crate) async fn server_branch_delete(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(BranchPath { branch }): ApiPath<BranchPath>,
) -> std::result::Result<Json<BranchDeleteOutput>, ApiError> {
    let actor_ref = actor.as_ref().map(|Extension(actor)| actor);
    branch_delete_body(&state, &handle, actor_ref, ingress, &branch).await?;
    Ok(Json(BranchDeleteOutput {
        uri: handle.uri.clone(),
        name: branch,
        actor_id: actor_ref.map(|actor| actor.actor_id.as_ref().to_string()),
    }))
}

/// Body shared by `DELETE /branches/{branch}` and the `branch delete`
/// statement: the `branch_delete` check on `name`, admission, the engine call.
async fn branch_delete_body(
    state: &AppState,
    handle: &GraphRequest,
    actor: Option<&AuthenticatedActor>,
    ingress: IngressLease,
    name: &str,
) -> std::result::Result<(), ApiError> {
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchDelete,
            branch: None,
            target_branch: Some(name.to_string()),
        },
    )?;
    // Metadata-only manifest tombstone — small constant estimate.
    let admission = state
        .workload
        .try_admit(&actor_arc, 256)
        .map_err(ApiError::from_workload_reject)?;
    let engine = Arc::clone(&handle.engine);
    let name = name.to_owned();
    let actor_id = actor.map(|actor| actor.actor_id.to_string());
    owned_write(state, admission, ingress, handle.clone(), async move {
        engine
            .branch_delete_as(&name, actor_id.as_deref())
            .await
            .map_err(ApiError::from_omni)
    })
    .await
}

#[utoipa::path(
    post,
    path = "/branches/merge",
    tag = "branches",
    operation_id = "mergeBranches",
    request_body = BranchMergeRequest,
    responses(
        (status = 200, description = "Branches merged", body = BranchMergeOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Merge conflict", body = ErrorOutput),
        (status = 413, description = "Merge entity, byte, or recovery-chain ceiling exceeded before effects", body = ErrorOutput),
        (status = 424, description = "A merge could not probe or read an allowed external Blob source", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Merge one branch into another.
///
/// Merges `source` into `target` (defaults to `main`). Outcome is one of
/// `already_up_to_date`, `fast_forward`, or `merged`. Returns 409 with the
/// list of conflicts if the merge cannot be completed; the target is left
/// unchanged in that case. **Destructive** to `target` on success. `commit`
/// carries this merge's own target publication, including for a fast-forward;
/// an already-up-to-date merge returns `commit: null`.
///
/// With `dry_run: true` the merge stops after classification and validation:
/// the response reports the outcome it would have now (`commit: null`,
/// `dry_run: true`), a conflict answers the same 409, and nothing is written.
/// A dry run is a read of both branches and needs no `branch_merge` grant.
///
/// With `delete_branch: true` the source branch is deleted after a successful
/// merge, under its own `branch_delete` policy check. The merge is durable by
/// then, so a deletion refusal or failure never fails the request; it is
/// reported via `branch_deleted: false` + `branch_delete_error_details`.
///
/// The GQ statement `branch merge` on `POST /mutate` runs the same body
/// (without the deletion composition) and answers the same 409 on conflict.
pub(crate) async fn server_branch_merge(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiJson(request): ApiJson<BranchMergeRequest>,
) -> std::result::Result<Json<BranchMergeOutput>, ApiError> {
    if request.as_commit && request.delete_branch {
        return Err(ApiError::bad_request(
            "a merge `as_commit` keeps its source until the commit is published or dropped; drop `delete_branch`",
        ));
    }
    if request.dry_run && (request.as_commit || request.delete_branch) {
        return Err(ApiError::bad_request(
            "a dry run writes nothing; drop `as_commit` and `delete_branch`",
        ));
    }
    let target = request.target.unwrap_or_else(|| "main".to_string());
    let actor_ref = actor.as_ref().map(|Extension(actor)| actor);
    let session = state.session(&handle, request.settings.as_ref())?;
    if request.dry_run {
        // A read of both branches: no merge admission, no write gate, no
        // `branch_merge` grant. Conflicts answer the same 409 a merge does.
        authorize_branch_reads(
            actor_ref,
            handle.policy.as_deref(),
            [&request.source, &target],
        )?;
        let result = session
            .branch_merge_dry_run(&request.source, &target)
            .await
            .map_err(ApiError::from_omni)?;
        return Ok(Json(BranchMergeOutput {
            source: request.source,
            target,
            outcome: result.outcome.into(),
            commit: None,
            actor_id: actor_ref.map(|actor| actor.actor_id.as_ref().to_string()),
            branch_deleted: None,
            branch_delete_error_details: None,
            dry_run: true,
        }));
    }
    let admission = admit_branch_merge(&state, &handle, actor_ref, &request.source, &target)?;
    let actor = actor.map(|Extension(actor)| actor);
    state
        .operations
        .submit((admission, ingress, handle.clone()), async move {
            let result = match session
                .branch_merge_as_with_options(
                    &request.source,
                    &target,
                    actor.as_ref().map(|actor| actor.actor_id.as_ref()),
                    request.as_commit,
                )
                .await
                .map_err(ApiError::from_omni)
            {
                Ok(result) => result,
                Err(error) => return OwnedResult::from(Err(error)),
            };
            let mut uncertain = false;
            let (branch_deleted, branch_delete_error_details) = if request.delete_branch {
                match delete_merged_source_branch(&handle, actor.as_ref(), &request.source).await {
                    Ok(()) => (Some(true), None),
                    Err(error) => {
                        uncertain = error.completion_uncertain();
                        (Some(false), Some(error.into_step_output()))
                    }
                }
            } else {
                (None, None)
            };
            OwnedResult {
                uncertain,
                result: Ok(Json(BranchMergeOutput {
                    source: request.source,
                    target,
                    outcome: result.outcome.into(),
                    commit: result.commit.as_ref().map(api::commit_output),
                    actor_id: actor
                        .as_ref()
                        .map(|actor| actor.actor_id.as_ref().to_string()),
                    branch_deleted,
                    branch_delete_error_details,
                    dry_run: false,
                })),
            }
        })?
        .result()
        .await
}

/// Body shared by `POST /branches/merge` and the `branch merge` statement:
/// the `branch_merge` check on (`source`, `target`), admission, the engine
/// call. A conflict surfaces as `ApiError::merge_conflict` (409).
#[allow(clippy::too_many_arguments)]
async fn branch_merge_body(
    state: &AppState,
    handle: &GraphRequest,
    session: &Session,
    actor: Option<&AuthenticatedActor>,
    ingress: IngressLease,
    source: &str,
    target: &str,
    as_commit: bool,
) -> std::result::Result<MergeResult, ApiError> {
    let admission = admit_branch_merge(state, handle, actor, source, target)?;
    let session = session.clone();
    let source = source.to_owned();
    let target = target.to_owned();
    let actor_id = actor.map(|actor| actor.actor_id.to_string());
    owned_write(state, admission, ingress, handle.clone(), async move {
        session
            .branch_merge_as_with_options(&source, &target, actor_id.as_deref(), as_commit)
            .await
            .map_err(ApiError::from_omni)
    })
    .await
}

fn admit_branch_merge(
    state: &AppState,
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
    source: &str,
    target: &str,
) -> std::result::Result<AdmissionGuard, ApiError> {
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchMerge,
            branch: Some(source.to_string()),
            target_branch: Some(target.to_string()),
        },
    )?;
    // Merge body is small JSON; the heavy work is in the engine but is
    // bounded per-(table, branch) by the writer queue. Small constant
    // estimate suffices for the actor in-flight count.
    state
        .workload
        .try_admit(&actor_arc, 256)
        .map_err(ApiError::from_workload_reject)
}

/// Delete the source branch of a just-landed merge, mirroring
/// `server_branch_delete`'s authorization (same action and target scope) but
/// converting every failure — policy denial, dependent-branch refusal,
/// operational error — into structured details instead of an error status.
/// The merge is already durable, so the request must not report failure for it.
async fn delete_merged_source_branch(
    handle: &GraphHandle,
    actor: Option<&AuthenticatedActor>,
    source: &str,
) -> std::result::Result<(), ApiError> {
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchDelete,
            branch: None,
            target_branch: Some(source.to_string()),
        },
    )
    .map_err(|mut error| {
        // This optional action has not called the engine yet. Preserve the
        // completed merge receipt without treating a policy evaluation error
        // as uncertainty about branch deletion.
        error.completion_uncertain = false;
        error
    })?;
    let actor_id = actor.map(|actor| actor.actor_id.as_ref());
    handle
        .engine
        .branch_delete_as(source, actor_id)
        .await
        .map_err(ApiError::from_omni)
}

#[utoipa::path(
    get,
    path = "/commits",
    tag = "commits",
    operation_id = "listCommits",
    params(CommitListQuery),
    responses(
        (status = 200, description = "List of commits", body = CommitListOutput),
        (status = 400, description = "Two listings selected, a page parameter with `unpublished` or `unattached`, a zero `limit`, or a rejected page token", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Branch not found", body = ErrorOutput),
        (status = 413, description = "Requested limit exceeds 1000 commits per page", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// List commits, most recent first.
///
/// `branch` selects which history to list: a named branch returns the history
/// reachable from that branch's head (the main commits inherited up to the
/// fork plus the branch-authored commits); omitting it returns `main`'s
/// history. There is no cross-branch listing. Ordering is part of the
/// contract: newest first along the branch's first-parent chain, which is
/// falling generation and falling (graph-manifest version, created-at, commit
/// id). Read-only.
///
/// `limit` pages the history: each page names the head its listing resolved
/// (`head_commit_id`) and continues with `next_page_token`, sent back as
/// `page_token` with the same `branch`; later pages answer for that head's
/// chain even after the branch moves. A page reads only the history it
/// returns. Without `limit` and `page_token` the whole history is one answer.
/// A caller whose `read` does not cover every branch continues only from a
/// commit of `branch`'s history: one the branch still holds or a readable
/// branch published costs nothing more; any other is looked for down the
/// branch's history, which costs what the whole listing costs, and a commit
/// the history does not reach is refused (400).
///
/// `unpublished=true` lists instead the unpublished commits whose recorded
/// target is `branch` (writes `as commit` awaiting `commit publish`), newest
/// first; each carries `published: false`. `unattached=true` lists instead
/// the live unattached snapshots whose recorded target is `branch` (writes
/// `as unattached branch`), newest first, each with `kind: "unattached"`;
/// the two filters together are refused (400), as is either with `limit` or
/// `page_token`.
pub(crate) async fn server_commit_list(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiQuery(query): ApiQuery<CommitListQuery>,
) -> std::result::Result<Json<CommitListOutput>, ApiError> {
    if query.unattached && query.unpublished {
        return Err(ApiError::bad_request(
            "`unattached=true` and `unpublished=true` select different listings; pass one",
        ));
    }
    let paged = query.limit.is_some() || query.page_token.is_some();
    if paged && (query.unattached || query.unpublished) {
        return Err(ApiError::bad_request(
            "`limit` and `page_token` page a branch's history, not `unpublished` or `unattached`",
        ));
    }
    if query.limit == Some(0) {
        return Err(ApiError::bad_request(
            "commit list page limit must be greater than zero",
        ));
    }
    // An omitted `branch` means main's history, so the policy gate must
    // see `main` — not `has_branch == false`, which a branch-scoped read
    // grant can never match.
    let branch = query.branch.unwrap_or_else(|| "main".to_string());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    let db = &handle.engine;
    if paged {
        // A page token is checksummed, not signed, so the commit it continues
        // from is the caller's claim. A caller allowed graph-wide `read` (the
        // gate of `GET /commits/{commit_id}`) may read any commit by id and
        // follow its parents, so its claim needs no proof; any other caller's
        // continuation is proven on the branch's history.
        let actor = actor.as_ref().map(|Extension(actor)| actor);
        let policy = handle.policy.as_deref();
        let any_commit = query.page_token.is_none()
            || matches!(
                authorize(
                    actor,
                    policy,
                    PolicyRequest {
                        action: PolicyAction::Read,
                        branch: None,
                        target_branch: None,
                    },
                )?,
                Authz::Allowed
            );
        let (branch, page_token) = (Some(branch.as_str()), query.page_token.as_deref());
        let page = if any_commit {
            db.list_commits_page(branch, page_token, query.limit).await
        } else {
            let mut failure = None;
            let page = db
                .list_commits_page_readable(branch, page_token, query.limit, &mut |branch| {
                    read_decision(actor, policy, &mut failure, branch)
                })
                .await;
            if let Some(error) = failure {
                return Err(error);
            }
            page
        }
        .map_err(ApiError::from_omni)?;
        return Ok(Json(CommitListOutput {
            commits: page.commits.iter().map(api::commit_output).collect(),
            head_commit_id: Some(page.head_commit_id),
            next_page_token: page.next_page_token,
        }));
    }
    let commits = if query.unattached {
        db.list_unattached_commits(Some(branch.as_str())).await
    } else if query.unpublished {
        db.list_unpublished_commits(Some(branch.as_str())).await
    } else {
        db.list_commits(Some(branch.as_str())).await
    }
    .map_err(ApiError::from_omni)?;
    Ok(Json(CommitListOutput {
        commits: commits.iter().map(api::commit_output).collect(),
        head_commit_id: None,
        next_page_token: None,
    }))
}

/// Path-param shape for [`server_commit_show`]. See [`BranchPath`]
/// for the design rationale — same pattern, different field name.
#[derive(Deserialize)]
pub(crate) struct CommitPath {
    pub(crate) commit_id: String,
}

#[utoipa::path(
    get,
    path = "/commits/{commit_id}",
    tag = "commits",
    operation_id = "getCommit",
    params(
        ("commit_id" = String, Path, description = "Commit identifier"),
    ),
    responses(
        (status = 200, description = "Commit details", body = api::CommitOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Commit not found", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]

/// Get a single commit.
///
/// Returns the commit's graph-manifest version, parent commit(s), and creation
/// metadata. Read-only.
pub(crate) async fn server_commit_show(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(CommitPath { commit_id }): ApiPath<CommitPath>,
) -> std::result::Result<Json<api::CommitOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: None,
            target_branch: None,
        },
    )?;
    let commit = {
        let db = &handle.engine;
        db.get_commit(&commit_id)
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(api::commit_output(&commit)))
}

#[utoipa::path(
    post,
    path = "/commits/{commit_id}/publish",
    tag = "commits",
    operation_id = "publishCommit",
    params(
        ("commit_id" = String, Path, description = "Unpublished commit identifier"),
    ),
    responses(
        (status = 200, description = "The commit as published on its recorded target branch; a published id is answered as it stands", body = api::CommitOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 404, description = "Commit not found, or its publication is not permitted", body = ErrorOutput),
        (status = 409, description = "The target's state moved under the proposal's authority (code `conflict`), or the commit is unattached and cannot publish (code `unattached_cannot_publish`, no durable effect)", body = ErrorOutput),
        (status = 412, description = "The target head moved past the commit's captured parent; the commit can only be dropped", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Publish an unpublished commit onto its recorded target branch.
///
/// The target is the branch the commit recorded when it was written `as
/// commit`, so the request carries no body. Runs under `commit_publish` on
/// that branch; a denial answers the 404 an unknown id answers. An
/// unattached snapshot (`as unattached branch`) never publishes: 409 with
/// code `unattached_cannot_publish`, after the same authorization. The GQ
/// statement `commit publish "<id>"` on `POST /mutate` runs the same body.
pub(crate) async fn server_commit_publish(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(CommitPath { commit_id }): ApiPath<CommitPath>,
) -> std::result::Result<Json<api::CommitOutput>, ApiError> {
    let actor_ref = actor.as_ref().map(|Extension(actor)| actor);
    let session = state.session(&handle, None)?;
    let CommitControlOutcome { commit, .. } = commit_control_body(
        &state,
        &handle,
        &session,
        actor_ref,
        ingress,
        omnigraph_compiler::query::ast::CommitWrite::Publish {
            graph_commit_id: commit_id,
        },
    )
    .await?;
    commit
        .map(Json)
        .ok_or_else(|| ApiError::internal("commit publish answered no commit"))
}

#[utoipa::path(
    delete,
    path = "/commits/{commit_id}",
    tag = "commits",
    operation_id = "dropCommit",
    params(
        ("commit_id" = String, Path, description = "Unpublished commit or unattached snapshot identifier"),
    ),
    responses(
        (status = 204, description = "Commit dropped: the requested id and every unattached descendant lost addressability, and a proposal's held merge inputs are released"),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 404, description = "Commit not found, or its drop is not permitted", body = ErrorOutput),
        (status = 409, description = "The commit is published", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Drop an unpublished commit or an unattached snapshot.
///
/// **Irreversible.** Runs under `commit_drop` on the commit's recorded target
/// branch; a denial answers the 404 an unknown id answers. A published commit
/// is never dropped (409). Dropping an unattached snapshot also drops every
/// unattached descendant, the root's whole chain included. The GQ statement
/// `commit drop "<id>"` on `POST /mutate` runs the same body.
pub(crate) async fn server_commit_drop(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(ingress): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(CommitPath { commit_id }): ApiPath<CommitPath>,
) -> std::result::Result<StatusCode, ApiError> {
    let actor_ref = actor.as_ref().map(|Extension(actor)| actor);
    let session = state.session(&handle, None)?;
    commit_control_body(
        &state,
        &handle,
        &session,
        actor_ref,
        ingress,
        omnigraph_compiler::query::ast::CommitWrite::Drop {
            graph_commit_id: commit_id,
        },
    )
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) fn read_target_from_request(
    branch: Option<String>,
    snapshot: Option<String>,
) -> ReadTarget {
    if let Some(snapshot) = snapshot {
        ReadTarget::snapshot(omnigraph::db::SnapshotId::new(snapshot))
    } else {
        ReadTarget::branch(branch.unwrap_or_else(|| "main".to_string()))
    }
}

pub(crate) fn select_named_query_decl(
    queries: Vec<QueryDecl>,
    requested_name: Option<&str>,
) -> Result<QueryDecl> {
    let query = if let Some(name) = requested_name {
        queries
            .into_iter()
            .find(|query| query.name == name)
            .ok_or_else(|| color_eyre::eyre::eyre!("query '{}' not found", name))?
    } else if queries.len() == 1 {
        queries.into_iter().next().unwrap()
    } else if queries.is_empty() {
        bail!(crate::api::query_file_refusals::NO_QUERY);
    } else {
        bail!("query file contains multiple queries; pass --name");
    };
    Ok(query)
}

pub(crate) fn query_params_from_json(
    query_params: &[omnigraph_compiler::query::ast::Param],
    params_json: Option<&Value>,
) -> Result<ParamMap> {
    json_params_to_param_map(params_json, query_params, JsonParamMode::Standard)
        .map_err(|err| color_eyre::eyre::eyre!(err.to_string()))
}

#[cfg(test)]
mod change_route_error_tests {
    use super::*;
    use crate::test_log_capture::Capture;

    /// Assert the captured log carries every `expected` spelling and none of
    /// the `leaked` ones.
    fn assert_log(capture: &Capture, expected: &[&str], leaked: &[&str]) {
        let logs = capture.output();
        for expected in expected {
            assert!(logs.contains(expected), "missing {expected}: {logs}");
        }
        for leaked in leaked {
            assert!(!logs.contains(leaked), "log leaked {leaked}: {logs}");
        }
    }

    #[test]
    fn change_route_error_hides_substrate_paths() {
        let capture = Capture::default();
        let _logs = tracing::subscriber::set_default(capture.subscriber("debug"));
        let leaky =
            "/srv/data/graph/nodes/0000000a-0000000b.lance: No such file or directory".to_string();
        let mapped = change_route_error(OmniError::Storage(omnigraph::error::StorageFailure::new(
            omnigraph::error::StorageFailureKind::Unknown,
            format!("storage: {leaky}"),
        )));
        assert_eq!(mapped.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            !mapped.message().contains(".lance") && !mapped.message().contains("/srv/data"),
            "change route leaked a substrate path: {}",
            mapped.message()
        );
        assert_log(
            &capture,
            &[
                r#"error_kind="change_route_internal""#,
                r#"error_variant="Storage""#,
                "storage_kind=Some(Unknown)",
            ],
            &[".lance", "/srv/data", "No such file"],
        );
    }

    #[test]
    fn change_route_error_hides_internal_manifest_table_keys() {
        let capture = Capture::default();
        let _logs = tracing::subscriber::set_default(capture.subscriber("debug"));
        let mapped = change_route_error(OmniError::manifest_internal(
            "invalid table key 'node:SecretType' at internal version 7",
        ));
        assert_eq!(mapped.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            !mapped.message().contains("node:SecretType"),
            "change route leaked an internal table key: {}",
            mapped.message()
        );
        assert_log(
            &capture,
            &[
                r#"error_variant="Manifest""#,
                "manifest_kind=Some(Internal)",
            ],
            &["node:SecretType", "internal version"],
        );
    }

    #[test]
    fn change_route_error_passes_only_allowlisted_graph_errors_through() {
        let capture = Capture::default();
        let _logs = tracing::subscriber::set_default(capture.subscriber("debug"));
        // Even Manifest::NotFound is too broad for the shared mapper. Only a
        // route that knows which public graph resource it looked up may turn
        // that category into a fixed 404.
        let mapped = change_route_error(OmniError::manifest_not_found(
            "missing /srv/private/table.lance for node:Secret",
        ));
        assert_eq!(mapped.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!mapped.message().contains("node:Secret"));
        let mapped = change_route_not_found(
            OmniError::manifest_not_found("missing /srv/private/table.lance"),
            "commit 'x' not found".to_string(),
        );
        assert_eq!(mapped.status(), StatusCode::NOT_FOUND);
        assert!(mapped.message().contains("not found"));
        assert!(!mapped.message().contains("/srv/private"));

        let mapped = change_route_error(OmniError::ChangeCursorRejected {
            reason: "token does not match this filter".to_string(),
        });
        assert_eq!(mapped.status(), StatusCode::BAD_REQUEST);
        assert!(mapped.message().contains("change cursor rejected"));

        let mapped = change_route_error(OmniError::BranchNotFound {
            branch: "feature".to_string(),
        });
        assert_eq!(mapped.status(), StatusCode::NOT_FOUND);
        assert_eq!(mapped.message(), "branch 'feature' not found");

        let mapped = change_route_commit_lookup_error(
            OmniError::BranchNotFound {
                branch: "secret-feature".to_string(),
            },
            "commit-x",
        );
        assert_eq!(mapped.status(), StatusCode::NOT_FOUND);
        assert_eq!(mapped.message(), "commit 'commit-x' not found");
        assert!(!mapped.message().contains("secret-feature"));

        // Manifest::BadRequest is intentionally NOT a pass-through category:
        // it is used throughout the engine and may acquire physical context.
        // The route validates its own public inputs before entering the engine.
        let mapped = change_route_error(OmniError::manifest(
            "bad table node:Secret at /srv/private/table.lance",
        ));
        assert_eq!(mapped.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!mapped.message().contains("node:Secret"));

        let mapped = change_route_error(OmniError::ResourceLimitExceeded {
            resource: "table node:Secret bytes".to_string(),
            limit: 1,
            actual: 2,
        });
        assert_eq!(mapped.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!mapped.message().contains("node:Secret"));
        // Neither the collapsed failures nor the contextual 404 log the text
        // the response withholds, even at debug level.
        assert_log(
            &capture,
            &[
                r#"public_message=commit 'x' not found"#,
                r#"error_variant="ResourceLimitExceeded""#,
            ],
            &["node:Secret", "/srv/private"],
        );
    }

    #[test]
    fn change_route_recovery_exposes_id_but_redacts_internal_reason() {
        let capture = Capture::default();
        let _logs = tracing::subscriber::set_default(capture.subscriber("debug"));
        let mapped = change_route_error(OmniError::RecoveryRequired {
            operation_id: "op-public".to_string(),
            reason: "sidecar /srv/private/recovery.json names node:Secret".to_string(),
        });
        assert_eq!(mapped.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(mapped.message().contains("recovery required"));
        assert!(!mapped.message().contains("/srv/private"));
        assert!(matches!(
            mapped.details.as_deref(),
            Some(crate::ApiErrorDetails::RecoveryRequired(details))
                if details.operation_id == "op-public"
        ));
        assert_log(
            &capture,
            &["change route requires recovery", "operation_id=op-public"],
            &["/srv/private", "node:Secret", "sidecar"],
        );
    }
}

#[cfg(test)]
mod blob_error_tests {
    use super::*;
    use std::fmt;

    use futures::stream::BoxStream;
    use object_store::path::Path as ObjectPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };

    /// Wraps every graph-catalog store a probed task opens so that each read
    /// fails as an object store would, with a physical URI in its message.
    /// Resolving a snapshot target reopens the graph catalog at the
    /// snapshot's version, so this is a storage failure inside target
    /// resolution.
    #[derive(Debug)]
    struct CatalogReadFault;

    impl lance::io::WrappingObjectStore for CatalogReadFault {
        fn wrap(&self, _store_prefix: &str, target: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
            Arc::new(CatalogReadFaultStore { target })
        }
    }

    #[derive(Debug)]
    struct CatalogReadFaultStore {
        target: Arc<dyn ObjectStore>,
    }

    impl fmt::Display for CatalogReadFaultStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "CatalogReadFaultStore({})", self.target)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CatalogReadFaultStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.target.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.target.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            _options: GetOptions,
        ) -> object_store::Result<GetResult> {
            Err(object_store::Error::PermissionDenied {
                path: format!("s3://private-bucket/{location}"),
                source: "GET denied".into(),
            })
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.target.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.target.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&ObjectPath>,
            offset: &ObjectPath,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.target.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.target.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.target.copy_opts(from, to, options).await
        }
    }

    /// A storage failure while the Blob route resolves a snapshot target this
    /// handle does not hold (the read goes to `__history`) for a policy-gated
    /// actor reaches the log with its class; the client sees the redacted 500.
    #[tokio::test]
    async fn blob_delivery_logs_the_class_of_a_target_resolution_storage_failure() {
        let capture = crate::test_log_capture::Capture::default();
        let _logs = tracing::subscriber::set_default(capture.subscriber("info"));
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        Omnigraph::init(
            uri,
            "node Document {\n    title: String @key\n    content: Blob?\n}\n",
        )
        .await
        .unwrap();
        let engine = Omnigraph::open(uri).await.unwrap();
        let snapshot = omnigraph::db::SnapshotId::new(
            "hb1.01ARZ3NDEKTSV4RRFFQ69G5FAV.0.01ARZ3NDEKTSV4RRFFQ69G5FAV",
        );
        let policy: PolicyConfig = yaml_serde::from_str(
            "version: 1\n\
             groups:\n  team: [act-alice]\n\
             rules:\n  - id: team-read\n    allow:\n      actors: { group: team }\n      actions: [read]\n      branch_scope: any\n",
        )
        .unwrap();
        let handle = GraphHandle {
            key: GraphKey::cluster(GraphId::try_from("graph").unwrap()),
            uri: uri.to_string(),
            engine: Arc::new(engine),
            policy: Some(Arc::new(PolicyCompiler::compile(&policy, "graph").unwrap())),
            queries: None,
        };
        let actor = AuthenticatedActor::cluster_static(Arc::from("act-alice"));
        let query = || api::BlobReadQuery {
            entity: api::BlobEntityKind::Node,
            r#type: "Document".to_string(),
            id: "missing".to_string(),
            property: "content".to_string(),
            branch: None,
            snapshot: Some(snapshot.as_str().to_string()),
        };

        let unarmed = read_blob_for_delivery(&handle, Some(&actor), query())
            .await
            .unwrap_err();
        assert_ne!(
            unarmed.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "unarmed, the request reads `__history`, finds no such commit and is refused"
        );

        let probes = omnigraph::instrumentation::QueryIoProbes {
            manifest_wrapper: Some(Arc::new(CatalogReadFault)),
            ..Default::default()
        };
        // Positive control: the engine error text names the bucket, so the
        // absence checks below test the redaction, not an already-clean error.
        let raw = omnigraph::instrumentation::with_query_io_probes(
            probes.clone(),
            handle.engine.resolved_branch_of(read_target_from_request(
                None,
                Some(snapshot.as_str().to_string()),
            )),
        )
        .await
        .unwrap_err();
        assert!(raw.to_string().contains("private-bucket"), "{raw}");
        let response = omnigraph::instrumentation::with_query_io_probes(
            probes,
            read_blob_for_delivery(&handle, Some(&actor), query()),
        )
        .await
        .unwrap_err()
        .into_response();
        assert_eq!(
            response.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "with the fault installed on this task, the read of `__history` fails in the object store"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let output: ErrorOutput = serde_json::from_slice(&body).unwrap();
        assert_eq!(output.error, "Blob delivery failed before response headers");
        assert!(!String::from_utf8_lossy(&body).contains("private-bucket"));

        let logs = capture.output();
        for expected in [
            r#"error_kind="blob_pre_header_internal""#,
            r#"stage="target" error_variant="Storage""#,
            "storage_kind=Some(",
        ] {
            assert!(logs.contains(expected), "missing {expected}: {logs}");
        }
        for absent in ["unclassified", "private-bucket", "denied"] {
            assert!(!logs.contains(absent), "log carries {absent}: {logs}");
        }
    }

    #[tokio::test]
    async fn pre_header_internal_errors_do_not_expose_physical_storage_or_identity() {
        let capture = crate::test_log_capture::Capture::default();
        let _logs = tracing::subscriber::set_default(capture.subscriber("info"));
        for (error, secret) in [
            (
                OmniError::Storage(omnigraph::error::StorageFailure::new(
                    omnigraph::error::StorageFailureKind::Unknown,
                    "storage: GET s3://private-bucket/tenant-a/table.lance?token=secret",
                )),
                "private-bucket",
            ),
            (
                OmniError::BlobIntegrity {
                    reason: "table_key node:Secret has stable table 42/incarnation 99".to_string(),
                },
                "node:Secret",
            ),
        ] {
            let response = map_blob_read_error(error).into_response();
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let output: ErrorOutput = serde_json::from_slice(&body).unwrap();
            assert_eq!(output.error, "Blob delivery failed before response headers");
            assert!(!String::from_utf8_lossy(&body).contains(secret));
        }

        let response = redact_blob_api_error(
            ApiError::internal("snapshot manifest at s3://private-bucket/graph/__manifest"),
            "target",
            None,
        )
        .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let output: ErrorOutput = serde_json::from_slice(&body).unwrap();
        assert_eq!(output.error, "Blob delivery failed before response headers");
        assert!(!String::from_utf8_lossy(&body).contains("private-bucket"));

        // The server log names each failure's class and stage, never its text.
        let logs = capture.output();
        for expected in [
            r#"error_kind="blob_pre_header_internal""#,
            r#"stage="cell""#,
            r#"error_variant="Storage""#,
            "storage_kind=Some(Unknown)",
            r#"error_variant="BlobIntegrity""#,
            r#"stage="target""#,
            r#"error_variant="unclassified""#,
        ] {
            assert!(logs.contains(expected), "missing {expected}: {logs}");
        }
        for leaked in [
            "private-bucket",
            "tenant-a",
            "token",
            "secret",
            "node:Secret",
            "incarnation",
            "__manifest",
        ] {
            assert!(!logs.contains(leaked), "log leaked {leaked}: {logs}");
        }
    }
}

// ─── Change surfaces ────────────────────────────────────────────────────────

/// Parsed change-surface query parameters. axum's `Query<T>` cannot collect
/// repeated keys into `Vec`s, so the change routes parse the raw query with a
/// STRICT allow-list: an unknown parameter is a 400, never silently ignored —
/// this is what keeps caller byte limits and physical vocabulary from ever
/// riding the new surfaces.
#[derive(Default)]
pub(crate) struct ParsedChangeParams {
    pub branch: Option<String>,
    /// The graph diff's read targets (`GET /diff`).
    pub from: Option<String>,
    pub to: Option<String>,
    /// The branch diff's and merge base's branches.
    pub source: Option<String>,
    pub target: Option<String>,
    pub cursor: Option<String>,
    pub start: Option<String>,
    pub page_token: Option<String>,
    pub limit: Option<usize>,
    pub kinds: Vec<api::EntityKindOutput>,
    pub types: Vec<String>,
    pub ops: Vec<api::ChangeOpOutput>,
    /// The `set=<name>=<value>` parameters, each checked against the settings
    /// definition and the `process` scope rule: validated here; consulted by
    /// nothing until the change feed's planner entry reads `engine` (the
    /// Session settings RFC's rollout step 2).
    pub settings: Vec<(SettingId, SettingValue)>,
}

/// Parse one `set=<name>=<value>` query parameter: a known `request`-scope
/// setting with a value its row accepts.
fn parse_set_parameter(raw: &str) -> std::result::Result<(SettingId, SettingValue), ApiError> {
    let bad_request =
        |error: omnigraph::settings::SessionSettingsError| ApiError::bad_request(error.to_string());
    let Some((name, value)) = raw.split_once('=') else {
        return Err(ApiError::bad_request(format!(
            "query parameter 'set' takes <name>=<value>, got '{raw}'"
        )));
    };
    let (id, value) = SettingId::parse_assignment(name, value).map_err(bad_request)?;
    id.refuse_from_request().map_err(bad_request)?;
    Ok((id, value))
}

pub(crate) const COMMIT_CHANGES_PARAMS: &[&str] =
    &["page_token", "limit", "kind", "type", "op", "set"];
pub(crate) const DIFF_PARAMS: &[&str] =
    &["from", "to", "page_token", "limit", "kind", "type", "op"];
pub(crate) const BRANCH_DIFF_PARAMS: &[&str] = &[
    "source",
    "target",
    "page_token",
    "limit",
    "kind",
    "type",
    "op",
];
pub(crate) const MERGE_BASE_PARAMS: &[&str] = &["source", "target"];
pub(crate) fn parse_change_query(
    raw: Option<&str>,
    allowed: &[&str],
) -> std::result::Result<ParsedChangeParams, ApiError> {
    let mut params = ParsedChangeParams::default();
    let Some(raw) = raw else { return Ok(params) };

    fn set_single(
        slot: &mut Option<String>,
        name: &str,
        value: String,
    ) -> std::result::Result<(), ApiError> {
        if slot.is_some() {
            return Err(ApiError::bad_request(format!(
                "query parameter '{name}' may appear at most once"
            )));
        }
        *slot = Some(value);
        Ok(())
    }

    for (name, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        let name = name.as_ref();
        if !allowed.contains(&name) {
            return Err(ApiError::bad_request(format!(
                "unknown query parameter '{name}'"
            )));
        }
        let value = value.into_owned();
        match name {
            "branch" => set_single(&mut params.branch, name, value)?,
            "from" => set_single(&mut params.from, name, value)?,
            "to" => set_single(&mut params.to, name, value)?,
            "source" => set_single(&mut params.source, name, value)?,
            "target" => set_single(&mut params.target, name, value)?,
            "cursor" => set_single(&mut params.cursor, name, value)?,
            "start" => set_single(&mut params.start, name, value)?,
            "page_token" => set_single(&mut params.page_token, name, value)?,
            "limit" => {
                let mut slot = None;
                set_single(&mut slot, name, value)?;
                let parsed =
                    slot.as_deref().unwrap().parse::<usize>().map_err(|_| {
                        ApiError::bad_request("limit must be a non-negative integer")
                    })?;
                if params.limit.replace(parsed).is_some() {
                    return Err(ApiError::bad_request(
                        "query parameter 'limit' may appear at most once",
                    ));
                }
            }
            "kind" => params
                .kinds
                .push(api::EntityKindOutput::parse(&value).ok_or_else(|| {
                    ApiError::bad_request(format!("unknown kind '{value}' (expected node | edge)"))
                })?),
            "type" => params.types.push(value),
            "op" => params
                .ops
                .push(api::ChangeOpOutput::parse(&value).ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "unknown op '{value}' (expected insert | update | delete)"
                    ))
                })?),
            "set" => params.settings.push(parse_set_parameter(&value)?),
            _ => unreachable!("allow-list covers every match arm"),
        }
    }
    Ok(params)
}

#[utoipa::path(
    get,
    path = "/commits/{commit_id}/changes",
    tag = "changes",
    operation_id = "getCommitChanges",
    params(
        ("commit_id" = String, Path, description = "Commit identifier"),
        api::CommitChangesQuery,
    ),
    responses(
        (status = 200, description = "Entity changes this commit made relative to its first parent, in frozen (kind, type, id, op) order with the cause stated once", body = api::CommitChangesOutput),
        (status = 400, description = "Invalid filter or limit, or a rejected page token", body = api::ChangeErrorOutput),
        (status = 401, description = "Unauthorized", body = api::ChangeErrorOutput),
        // No 403: a commit the actor cannot read is indistinguishable from an
        // unknown commit (404), so the diff is not a commit-existence oracle.
        (status = 404, description = "Commit not found, or the actor cannot read the commit's branch", body = api::ChangeErrorOutput),
        (status = 409, description = "Commit cannot be entity-diffed (parentless commit or schema boundary); see change_diff_refusal", body = api::ChangeErrorOutput),
        (status = 410, description = "Required retained history is no longer readable; see change_feed_gap and capture a new baseline", body = api::ChangeErrorOutput),
        (status = 413, description = "Requested limit exceeds the public change ceiling", body = api::ChangeErrorOutput),
        (status = 500, description = "Internal failure while reading changes", body = api::ChangeErrorOutput),
        (status = 503, description = "Recovery required before changes can be read", body = api::ChangeErrorOutput),
    ),
    security(("bearer_token" = [])),
)]

/// Entity changes one commit made relative to its first parent.
///
/// Read-only, in graph vocabulary with exact before/after images. Bounded:
/// a large commit continues via the opaque `page_token`. `set=` values are
/// validated and select nothing in this release.
pub(crate) async fn server_commit_changes(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiPath(CommitPath { commit_id }): ApiPath<CommitPath>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> std::result::Result<Json<api::CommitChangesOutput>, ApiError> {
    let params = parse_change_query(raw.as_deref(), COMMIT_CHANGES_PARAMS)?;
    validate_change_http_limit(params.limit)?;
    // Resolve the commit first: unlike commit-show, this response carries entity
    // images, so read authorization binds to the branch the commit landed on.
    let db = &handle.engine;
    let commit = db
        .get_commit(&commit_id)
        .await
        .map_err(|error| change_route_commit_lookup_error(error, &commit_id))?;
    let branch = commit
        .graph_branch
        .clone()
        .unwrap_or_else(|| "main".to_string());
    match authorize(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: Some(branch),
            target_branch: None,
        },
    )? {
        Authz::Allowed => {}
        // Do not distinguish a known-but-forbidden commit from an unknown one.
        // The commit was resolved across all branches BEFORE this check, so a
        // 403-vs-404 split would be a graph-wide commit-existence oracle (and,
        // with per-branch grants, would confirm the existence of commits on a
        // branch the actor cannot read). Collapse the denial to the exact 404
        // an unknown commit yields.
        Authz::Denied(_) => {
            return Err(ApiError::not_found(format!(
                "commit '{commit_id}' not found"
            )));
        }
    }
    let scope = api::change_scope(&params.kinds, &params.types, &params.ops);
    let page = db
        .commit_changes_page(
            &commit_id,
            &scope,
            params.page_token.as_deref(),
            params.limit,
            None,
        )
        .await
        .map_err(change_route_error)?;
    Ok(Json(api::commit_changes_output(&page)))
}

fn validate_change_http_limit(limit: Option<usize>) -> std::result::Result<(), ApiError> {
    if limit == Some(0) {
        return Err(ApiError::bad_request(
            "change page limit must be greater than zero",
        ));
    }
    Ok(())
}

/// The branch a change route names, `main` when it names none, as written: a
/// name that is not canonical is refused before authorization reads it.
fn requested_branch(branch: Option<&str>) -> std::result::Result<String, ApiError> {
    let branch = branch.unwrap_or("main");
    require_canonical_branches([Some(branch)])?;
    Ok(branch.to_string())
}

/// A side of a graph diff the caller must spell, passed on as written. A
/// branch name and a commit id are never trimmed, so a spelling neither could
/// be is refused here (the change routes' error allowlist would hide the
/// engine's refusal behind a 500).
fn required_diff_param(value: Option<String>, name: &str) -> std::result::Result<String, ApiError> {
    match value {
        Some(value) if !value.trim().is_empty() => {
            omnigraph_policy::ensure_canonical_branch_name(&value).map_err(|error| {
                ApiError::bad_request(format!("query parameter '{name}': {error}"))
            })?;
            Ok(value)
        }
        _ => Err(ApiError::bad_request(format!(
            "query parameter '{name}' is required"
        ))),
    }
}

#[utoipa::path(
    get,
    path = "/diff",
    tag = "changes",
    operation_id = "getDiff",
    params(api::DiffQuery),
    responses(
        (status = 200, description = "Net entity changes between the two resolved commits, in frozen (kind, type, id, op) order, without a cause", body = api::DiffOutput),
        (status = 400, description = "A missing side, an invalid filter or limit, a side naming both a branch and a commit, or a rejected page token", body = api::ChangeErrorOutput),
        (status = 401, description = "Unauthorized", body = api::ChangeErrorOutput),
        // No 403: a side the actor cannot read is indistinguishable from a
        // side that names nothing (404).
        (status = 404, description = "A side names no branch and no commit, or the actor cannot read the branch it resolves to", body = api::ChangeErrorOutput),
        (status = 409, description = "The two sides cross a schema boundary; see change_diff_refusal", body = api::ChangeErrorOutput),
        (status = 410, description = "Retained history for an endpoint is no longer readable; see change_feed_gap", body = api::ChangeErrorOutput),
        (status = 413, description = "Requested limit exceeds the public change ceiling", body = api::ChangeErrorOutput),
        (status = 500, description = "Internal failure while reading changes", body = api::ChangeErrorOutput),
        (status = 503, description = "Recovery required before changes can be read", body = api::ChangeErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Net entity changes between two read targets.
///
/// `from` and `to` each name a branch (its head now) or a graph commit
/// (published, an unpublished proposal, or an unattached snapshot); a string
/// naming both is refused. The first page resolves both sides to exact
/// commits and names them; a large result continues via the opaque
/// `page_token`, and every page answers for those same commits.
pub(crate) async fn server_diff(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> std::result::Result<Json<api::DiffOutput>, ApiError> {
    let params = parse_change_query(raw.as_deref(), DIFF_PARAMS)?;
    validate_change_http_limit(params.limit)?;
    let from = required_diff_param(params.from, "from")?;
    let to = required_diff_param(params.to, "to")?;
    let page = two_point_diff_page(
        &handle,
        actor.as_ref().map(|Extension(actor)| actor),
        from,
        to,
        api::change_scope(&params.kinds, &params.types, &params.ops),
        params.page_token,
        params.limit,
    )
    .await?;
    Ok(Json(api::diff_output(&page)))
}

/// One page of the net change between two read targets, for `GET /diff` and
/// the `branch diff <from> to <to>` statement. The engine authorizes each side
/// on the branch it resolves to (a branch name, or the authored branch, a
/// proposal's or snapshot's recorded target, of a commit) as soon as it
/// classifies that side, before it looks at the next side or opens a
/// snapshot. A side the actor cannot read is the 404 of a side that names
/// nothing, whichever side it is: the route is not an existence oracle for
/// branches or commits the actor cannot read.
pub(crate) async fn two_point_diff_page(
    handle: &GraphRequest,
    actor: Option<&AuthenticatedActor>,
    from: String,
    to: String,
    scope: omnigraph::changes::ChangeFeedScope,
    page_token: Option<String>,
    max_changes: Option<usize>,
) -> std::result::Result<omnigraph::changes::GraphDiffPage, ApiError> {
    readable_diff_page(
        handle,
        actor,
        omnigraph::changes::GraphDiffRequest {
            sides: omnigraph::changes::GraphDiffSides::Between { from, to },
            scope,
            page_token,
            max_changes,
            max_bytes: None,
        },
    )
    .await
}

/// One diff page read under `actor`'s `read` decisions, which the engine asks
/// for each branch a two-point side reads and for each commit a continuation
/// names that its side no longer resolves to. A policy failure (a missing
/// bearer, an evaluation error) is not a denial: it stops the read, and its
/// own error answers.
async fn readable_diff_page(
    handle: &GraphRequest,
    actor: Option<&AuthenticatedActor>,
    request: omnigraph::changes::GraphDiffRequest,
) -> std::result::Result<omnigraph::changes::GraphDiffPage, ApiError> {
    let policy = handle.policy.as_deref();
    let mut failure = None;
    let page = handle
        .engine
        .diff_page_readable(&request, &mut |branch| {
            read_decision(actor, policy, &mut failure, branch)
        })
        .await;
    if let Some(error) = failure {
        return Err(error);
    }
    page.map_err(change_route_error)
}

/// `actor`'s `read` decision on `branch`, for the engine's read checks. A
/// policy failure (a missing bearer, an evaluation error) is not a denial: it
/// is kept in `failure`, and the caller answers with it.
fn read_decision(
    actor: Option<&AuthenticatedActor>,
    policy: Option<&PolicyEngine>,
    failure: &mut Option<ApiError>,
    branch: &str,
) -> bool {
    let request = PolicyRequest {
        action: PolicyAction::Read,
        branch: Some(branch.to_string()),
        target_branch: None,
    };
    match authorize(actor, policy, request) {
        Ok(Authz::Allowed) => true,
        Ok(Authz::Denied(_)) => false,
        Err(error) => {
            failure.get_or_insert(error);
            false
        }
    }
}

/// Authorize `read` on each named branch of a branch diff or merge base.
pub(crate) fn authorize_branch_reads(
    actor: Option<&AuthenticatedActor>,
    policy: Option<&PolicyEngine>,
    branches: [&str; 2],
) -> std::result::Result<(), ApiError> {
    for branch in branches {
        authorize_request(
            actor,
            policy,
            PolicyRequest {
                action: PolicyAction::Read,
                branch: Some(branch.to_string()),
                target_branch: None,
            },
        )?;
    }
    Ok(())
}

#[utoipa::path(
    get,
    path = "/branches/diff",
    tag = "changes",
    operation_id = "getBranchDiff",
    params(api::BranchDiffQuery),
    responses(
        (status = 200, description = "What `source` changed since its merge base with `target`: the net entity changes from the base to the source head", body = api::DiffOutput),
        (status = 400, description = "A missing source, an invalid filter or limit, or a rejected page token", body = api::ChangeErrorOutput),
        (status = 401, description = "Unauthorized", body = api::ChangeErrorOutput),
        (status = 403, description = "The actor cannot read one of the branches", body = api::ChangeErrorOutput),
        (status = 404, description = "A branch is not found", body = api::ChangeErrorOutput),
        (status = 409, description = "The base and the source head cross a schema boundary; see change_diff_refusal", body = api::ChangeErrorOutput),
        (status = 410, description = "Retained history for the base is no longer readable; see change_feed_gap", body = api::ChangeErrorOutput),
        (status = 413, description = "Requested limit exceeds the public change ceiling", body = api::ChangeErrorOutput),
        (status = 500, description = "Internal failure while reading changes", body = api::ChangeErrorOutput),
        (status = 503, description = "Recovery required before changes can be read", body = api::ChangeErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// What a branch changed since its merge base with a target.
///
/// The merge base is the commit `POST /branches/merge` would classify
/// against; it and the source head are captured together. Changes the target
/// made after the base do not appear. Paged like `GET /diff`.
pub(crate) async fn server_branch_diff(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> std::result::Result<Json<api::DiffOutput>, ApiError> {
    let params = parse_change_query(raw.as_deref(), BRANCH_DIFF_PARAMS)?;
    validate_change_http_limit(params.limit)?;
    let source = required_diff_param(params.source, "source")?;
    let page = branch_diff_page(
        &handle,
        actor.as_ref().map(|Extension(actor)| actor),
        &source,
        params.target.as_deref(),
        api::change_scope(&params.kinds, &params.types, &params.ops),
        params.page_token,
        params.limit,
    )
    .await?;
    Ok(Json(api::diff_output(&page)))
}

/// One page of what `source` changed since its merge base with `target`
/// (`main` when absent), for `GET /branches/diff` and the `branch diff`
/// statement: `read` on both named branches before any lineage is read, and on
/// the branch of each commit a continuation names that the branches no longer
/// resolve to.
pub(crate) async fn branch_diff_page(
    handle: &GraphRequest,
    actor: Option<&AuthenticatedActor>,
    source: &str,
    target: Option<&str>,
    scope: omnigraph::changes::ChangeFeedScope,
    page_token: Option<String>,
    max_changes: Option<usize>,
) -> std::result::Result<omnigraph::changes::GraphDiffPage, ApiError> {
    let source = requested_branch(Some(source))?;
    let target = requested_branch(target)?;
    authorize_branch_reads(actor, handle.policy.as_deref(), [&source, &target])?;
    readable_diff_page(
        handle,
        actor,
        omnigraph::changes::GraphDiffRequest {
            sides: omnigraph::changes::GraphDiffSides::Branch { source, target },
            scope,
            page_token,
            max_changes,
            max_bytes: None,
        },
    )
    .await
}

#[utoipa::path(
    get,
    path = "/branches/merge-base",
    tag = "changes",
    operation_id = "getMergeBase",
    params(api::MergeBaseQuery),
    responses(
        (status = 200, description = "The commit a merge of `source` into `target` would classify against, with the two heads", body = api::MergeBaseOutput),
        (status = 400, description = "A missing or unknown parameter", body = api::ChangeErrorOutput),
        (status = 401, description = "Unauthorized", body = api::ChangeErrorOutput),
        (status = 403, description = "The actor cannot read one of the branches", body = api::ChangeErrorOutput),
        (status = 404, description = "A branch is not found", body = api::ChangeErrorOutput),
        (status = 500, description = "Internal failure while reading lineage", body = api::ChangeErrorOutput),
        (status = 503, description = "Recovery required before lineage can be read", body = api::ChangeErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// The merge base of two branches.
///
/// Reads lineage only, by the rule `POST /branches/merge` uses.
pub(crate) async fn server_merge_base(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> std::result::Result<Json<api::MergeBaseOutput>, ApiError> {
    let params = parse_change_query(raw.as_deref(), MERGE_BASE_PARAMS)?;
    let source = required_diff_param(params.source, "source")?;
    let (source, target, base) = merge_base_body(
        &handle,
        actor.as_ref().map(|Extension(actor)| actor),
        &source,
        params.target.as_deref(),
    )
    .await?;
    Ok(Json(api::merge_base_output(&source, &target, &base)))
}

/// The merge base of `source` into `target` (`main` when absent), for
/// `GET /branches/merge-base` and the `branch merge-base` statement: `read`
/// on both named branches first. Answers the normalized names with the base.
pub(crate) async fn merge_base_body(
    handle: &GraphRequest,
    actor: Option<&AuthenticatedActor>,
    source: &str,
    target: Option<&str>,
) -> std::result::Result<(String, String, omnigraph::changes::MergeBase), ApiError> {
    let source = requested_branch(Some(source))?;
    let target = requested_branch(target)?;
    authorize_branch_reads(actor, handle.policy.as_deref(), [&source, &target])?;
    let base = handle
        .engine
        .merge_base(&source, &target)
        .await
        .map_err(change_route_error)?;
    Ok((source, target, base))
}

/// Contextual 404 projection. `Manifest::NotFound` is a broad internal
/// category, so its original text is never reused; the handler supplies the
/// exact public resource spelling it attempted to resolve.
fn change_route_not_found(error: OmniError, public_message: String) -> ApiError {
    match error {
        OmniError::Manifest(manifest) if manifest.kind == ManifestErrorKind::NotFound => {
            tracing::debug!(%public_message, "change resource not found");
            ApiError::not_found(public_message)
        }
        other => change_route_error(other),
    }
}

/// Commit lookup runs before branch authorization because the persisted commit
/// selects the policy resource. A raced named-ref deletion can therefore fail
/// while the engine is searching branches. Collapse that typed branch miss to
/// the same fixed commit 404: the caller is not yet authorized to learn which
/// otherwise-unreadable branch was involved.
fn change_route_commit_lookup_error(error: OmniError, commit_id: &str) -> ApiError {
    match error {
        OmniError::BranchNotFound { branch } => {
            tracing::debug!(%branch, %commit_id, "commit lookup branch disappeared");
            ApiError::not_found(format!("commit '{commit_id}' not found"))
        }
        other => change_route_not_found(other, format!("commit '{commit_id}' not found")),
    }
}

/// Map an engine error on a change route to the graph-only wire contract.
///
/// This is intentionally an allowlist. Only variants whose types guarantee
/// graph-vocabulary fields cross the wire. Everything else — including broad
/// `Manifest::BadRequest` / conflict categories and any future `OmniError`
/// variant — is logged by its class and collapsed to a fixed 500 so adding an
/// engine error can never accidentally expose an internal storage identifier
/// or sidecar, in the response or in the log.
fn change_route_error(error: OmniError) -> ApiError {
    match error {
        OmniError::ResourceLimitExceeded {
            resource,
            limit,
            actual,
        } if matches!(
            resource.as_str(),
            "commit_changes_page_changes"
                | "commit_changes_page_bytes"
                | "change_feed_commits_per_poll"
                | "change_continuation_token_encoded_bytes"
                | "stream_export_slots"
        ) =>
        {
            ApiError::from_omni(OmniError::ResourceLimitExceeded {
                resource,
                limit,
                actual,
            })
        }
        safe @ (OmniError::ChangeCursorRejected { .. }
        | OmniError::BranchNotFound { .. }
        | OmniError::ReadTargetNotFound { .. }
        | OmniError::AmbiguousReadTarget { .. }
        | OmniError::ChangeFeedGap { .. }
        | OmniError::CommitHasNoParent { .. }
        | OmniError::ChangeSchemaBoundary { .. }) => ApiError::from_omni(safe),
        // The reason is engine text; the operation ID is what recovery needs.
        OmniError::RecoveryRequired { operation_id, .. } => {
            tracing::warn!(%operation_id, "change route requires recovery");
            ApiError::recovery_required(
                "recovery required before changes can be read".to_string(),
                operation_id,
            )
        }
        other => {
            let cause = RedactedCause::of(&other);
            tracing::error!(
                error_kind = "change_route_internal",
                error_variant = cause.variant,
                storage_kind = ?cause.storage_kind,
                manifest_kind = ?cause.manifest_kind,
                "change route internal error"
            );
            ApiError::internal("internal error while reading changes")
        }
    }
}

pub(crate) const CHANGE_FEED_PARAMS: &[&str] = &[
    "branch",
    "cursor",
    "start",
    "page_token",
    "limit",
    "kind",
    "type",
    "op",
    "set",
];

fn parse_change_feed_start(
    start: &str,
) -> std::result::Result<omnigraph::changes::ChangeFeedStart, ApiError> {
    match start {
        "now" => Ok(omnigraph::changes::ChangeFeedStart::Now),
        "beginning" => Ok(omnigraph::changes::ChangeFeedStart::Beginning),
        other => other
            .strip_prefix("after:")
            .filter(|commit_id| !commit_id.is_empty())
            .map(|commit_id| {
                omnigraph::changes::ChangeFeedStart::AfterCommit(commit_id.to_string())
            })
            .ok_or_else(|| {
                ApiError::bad_request("start must be now | beginning | after:<commit_id>")
            }),
    }
}

#[utoipa::path(
    get,
    path = "/changes",
    tag = "changes",
    operation_id = "pollChanges",
    params(api::ChangeFeedQuery),
    responses(
        (status = 200, description = "Change blocks in first-parent order. The durable cursor appears only on a terminal page, advanced only over complete commits; a mid-block page carries only next_page_token", body = api::ChangeFeedOutput),
        (status = 400, description = "Invalid start/filter combination, or a rejected cursor or page token", body = api::ChangeErrorOutput),
        (status = 401, description = "Unauthorized", body = api::ChangeErrorOutput),
        (status = 403, description = "Forbidden", body = api::ChangeErrorOutput),
        (status = 404, description = "Branch not found", body = api::ChangeErrorOutput),
        (status = 409, description = "The feed crossed an unprovable schema boundary; see change_diff_refusal", body = api::ChangeErrorOutput),
        (status = 410, description = "Feed gap: required history was reclaimed; reset via the baseline handshake", body = api::ChangeErrorOutput),
        (status = 413, description = "Requested limit exceeds the public change ceiling", body = api::ChangeErrorOutput),
        (status = 500, description = "Internal failure while reading changes", body = api::ChangeErrorOutput),
        (status = 503, description = "Recovery required before changes can be read", body = api::ChangeErrorOutput),
    ),
    security(("bearer_token" = [])),
)]

/// Poll the change feed of one branch.
///
/// At-least-once: retrying a cursor may replay the complete next commit, so
/// consumers apply blocks idempotently by `graph_commit_id` and persist the
/// terminal cursor together with its blocks. The server holds no consumer
/// state. `set=` values are validated and select nothing in this release.
pub(crate) async fn server_changes_feed(
    Extension(handle): Extension<GraphRequest>,
    actor: Option<Extension<AuthenticatedActor>>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> std::result::Result<Json<api::ChangeFeedOutput>, ApiError> {
    let params = parse_change_query(raw.as_deref(), CHANGE_FEED_PARAMS)?;
    validate_change_http_limit(params.limit)?;
    let branch = requested_branch(params.branch.as_deref())?;
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;

    let position = match (params.cursor, params.start, params.page_token) {
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) | (_, Some(_), Some(_)) => {
            return Err(ApiError::bad_request(
                "cursor, start, and page_token are mutually exclusive",
            ));
        }
        (Some(cursor), None, None) => omnigraph::changes::ChangeFeedPosition::Cursor(cursor),
        (None, Some(start), None) => {
            omnigraph::changes::ChangeFeedPosition::Start(parse_change_feed_start(&start)?)
        }
        (None, None, Some(token)) => omnigraph::changes::ChangeFeedPosition::PageToken(token),
        // A missing cursor is never an implicit beginning.
        (None, None, None) => {
            omnigraph::changes::ChangeFeedPosition::Start(omnigraph::changes::ChangeFeedStart::Now)
        }
    };

    let scope = api::change_scope(&params.kinds, &params.types, &params.ops);
    let page = {
        let db = &handle.engine;
        db.poll_change_feed(omnigraph::changes::ChangeFeedRequest {
            branch: Some(branch.clone()),
            position,
            scope,
            max_changes: params.limit,
            max_bytes: None,
            max_commits: None,
        })
        .await
        .map_err(change_route_error)?
    };
    Ok(Json(api::change_feed_output(&page)))
}

#[utoipa::path(
    post,
    path = "/changes/baseline",
    tag = "changes",
    operation_id = "captureChangeBaseline",
    request_body = api::ChangeBaselineRequest,
    responses(
        (status = 200, description = "NDJSON entity snapshot pinned at one captured commit. Every preceding record is one type-keyed entity record (the load/export NDJSON shape); the FINAL record is the ChangeBaselineRecord envelope — an interrupted stream has no terminal record and therefore no usable cursor. Install the snapshot durably before the cursor.", body = api::ChangeBaselineRecord, content_type = "application/x-ndjson"),
        (status = 400, description = "Invalid scope", body = api::ChangeErrorOutput),
        (status = 401, description = "Unauthorized", body = api::ChangeErrorOutput),
        (status = 403, description = "Forbidden", body = api::ChangeErrorOutput),
        (status = 404, description = "Branch not found", body = api::ChangeErrorOutput),
        (status = 413, description = "Baseline cut or transport capacity exhausted", body = api::ChangeErrorOutput),
        (status = 500, description = "Internal failure while capturing the baseline", body = api::ChangeErrorOutput),
        (status = 503, description = "Recovery required", body = api::ChangeErrorOutput),
    ),
    security(("bearer_token" = [])),
)]

/// Capture a change-feed baseline: one exact entity snapshot plus the cursor
/// that resumes the feed immediately after it.
///
/// A baseline is a full data export, so it requires the export action. The
/// snapshot honors the scope's kind and type dimensions; `op` binds only the
/// resume cursor's feed scope.
pub(crate) async fn server_changes_baseline(
    State(state): State<AppState>,
    Extension(handle): Extension<GraphRequest>,
    Extension(observer): Extension<operations::ReadObserver>,
    Extension(input): Extension<IngressLease>,
    actor: Option<Extension<AuthenticatedActor>>,
    ApiJson(request): ApiJson<api::ChangeBaselineRequest>,
) -> std::result::Result<Response, ApiError> {
    let branch = requested_branch(request.branch.as_deref())?;
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Export,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    // Reserve the bounded response transport before capturing the cut so a
    // saturated client population can never hold graph authority while it
    // waits for process memory (the served-export ordering).
    let queue_lease = state
        .export_transport
        .reserve()
        .await
        .map_err(ApiError::from_omni)?;
    let scope = api::change_scope(&request.kind, &request.r#type, &request.op);
    let (handshake, cut) = handle
        .engine
        .capture_served_change_baseline_cut(&branch, &scope)
        .await
        .map_err(change_route_error)?;
    let producer_queue_lease = Arc::clone(&queue_lease);
    let (tx, body_stream) = export_transport::channel(queue_lease, "change_baseline");
    tokio::spawn(
        async move {
            // Declared first so wrapped producer/input resources drop before
            // the final graph request owner on every exit path.
            let _producer_graph = handle;
            let _producer_observer = observer;
            let _producer_input = input;
            let _producer_queue_lease = producer_queue_lease;
            let closed_tx = tx.clone();
            let data_tx = tx.clone();
            let export = cut.write_chunks(move |chunk| {
                let data_tx = data_tx.clone();
                async move { data_tx.send_chunk(chunk).await }
            });
            tokio::pin!(export);
            tokio::select! {
                biased;
                _ = closed_tx.closed() => {
                    // Cancelling the pinned export future drops its move-only cut.
                }
                (cut, result) = &mut export => {
                    // The structural guarantee: the terminal handshake record is
                    // sent ONLY after every snapshot record succeeded. A failed or
                    // interrupted stream carries no usable cursor.
                    let failure = match result {
                        Ok(()) => tx
                            .send_json_line(&api::ChangeBaselineRecord {
                                baseline: api::change_baseline_output(&handshake),
                            })
                            .await
                            .err(),
                        Err(error) => Some(error),
                    };
                    tx.finish(cut, failure).await;
                }
            }
        }
        .in_current_span(),
    );
    let body = Body::from_stream(body_stream);
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, "application/x-ndjson; charset=utf-8")],
        body,
    )
        .into_response())
}
