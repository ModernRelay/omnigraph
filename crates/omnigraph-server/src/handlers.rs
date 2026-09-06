//! HTTP route handlers, the bearer-auth middleware, per-request
//! authorization, and the cluster-prefix OpenAPI rewrite (moved
//! verbatim from lib.rs in the modularization).

use super::*;
use futures::StreamExt;

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
        version: SERVER_VERSION.to_string(),
        internal_schema_version: SERVER_INTERNAL_SCHEMA_VERSION,
        source_version: SERVER_SOURCE_VERSION.map(str::to_string),
    })
}

/// Readiness witness (RFC 0049).
///
/// Unauthenticated, and therefore minimal: it reports whether this replica
/// is serving or draining, the applied `config_digest` it booted from, the
/// ledger revision and CAS it read, and how many graphs it serves and does
/// not serve. Graph ids are topology and stay behind `GET /graphs`. Answers
/// 503 once shutdown has begun; `/healthz` stays 200 while the process is
/// alive.
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "health",
    operation_id = "readiness",
    responses(
        (status = 200, description = "Serving", body = ReadinessOutput),
        (status = 503, description = "Draining", body = ReadinessOutput),
    ),
)]
pub(crate) async fn server_ready(
    State(state): State<AppState>,
) -> (StatusCode, Json<ReadinessOutput>) {
    let draining = state.draining.load(std::sync::atomic::Ordering::SeqCst);
    let served_graph_count = state.routing().registry.list().len();
    let quarantined_graph_count = state.quarantined_graphs().len();
    let output = ReadinessOutput {
        ready: !draining,
        status: if draining { "draining" } else { "serving" }.to_string(),
        booted_serving_digest: state.witness.booted_serving_digest.clone(),
        state_revision: state.witness.state_revision,
        state_cas: state.witness.state_cas.clone(),
        served_graph_count,
        quarantined_graph_count,
        shutdown_grace_seconds: state.shutdown_grace.as_secs(),
    };
    let status = if draining {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
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
        (status = 405, description = "Method not allowed (single-graph mode)", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// List every graph currently registered with this server.
///
/// Multi-graph mode only. In single mode, the route returns 405 — there's
/// no registry to enumerate. Cedar-gated by the server-level policy via
/// the `graph_list` action against `Omnigraph::Server::"root"`.
///
/// Order: alphabetical by `graph_id` (server-sorted so clients see
/// deterministic output across requests).
pub(crate) async fn server_graphs_list(
    State(state): State<AppState>,
    actor: Option<Extension<ResolvedActor>>,
) -> std::result::Result<Json<GraphListResponse>, ApiError> {
    let registry = &state.routing().registry;

    // Server-level Cedar gate. `state.server_policy` is loaded from the
    // cluster-scoped policy bundle at startup. When no server policy is
    // configured, `authorize_request_server` falls through to the MR-723
    // default-deny semantics (every non-Read action denied for an
    // authenticated actor). `GraphList` is not `Read`, so without a server
    // policy the request gets 403 — which is the right default (don't leak
    // the registry until the operator explicitly authorizes it).
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        state.server_policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::GraphList,
            branch: None,
            target_branch: None,
        },
    )?;

    let may_list = |id: &str| {
        actor
            .as_ref()
            .is_none_or(|actor| actor.0.permits_graph_listing(id))
    };
    let mut graphs: Vec<GraphInfo> = registry
        .list()
        .into_iter()
        .filter(|handle| may_list(handle.key.graph_id.as_str()))
        .map(|handle| GraphInfo {
            graph_id: handle.key.graph_id.as_str().to_string(),
            uri: handle.uri.clone(),
        })
        .collect();
    graphs.sort_by(|a, b| a.graph_id.cmp(&b.graph_id));
    Ok(Json(GraphListResponse {
        graphs,
        quarantined: state
            .quarantined_graphs()
            .into_iter()
            .filter(|id| may_list(id))
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
const ALWAYS_FLAT_PATHS: &[&str] = &["/healthz", "/readyz", "/graphs"];

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
    if !state.requires_bearer_auth() {
        return Ok(next.run(request).await);
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
    request.extensions_mut().insert(actor);

    Ok(next.run(request).await)
}

/// Routing middleware (RFC-011 cluster-only). Resolves the active graph
/// for the request and injects `Arc<GraphHandle>` as an extension so
/// handlers can extract it via `Extension<Arc<GraphHandle>>`.
///
/// Routes are always nested under `/graphs/{graph_id}/...`. The
/// middleware extracts `{graph_id}` from the URI path and looks it up in
/// the registry. Returns 404 if the graph is not registered.
///
/// The middleware fires AFTER `require_bearer_auth`, so the actor is
/// already in the request extensions (or auth was off entirely).
pub(crate) async fn resolve_graph_handle(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, ApiError> {
    let registry = &state.routing.registry;
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
    if let Some(actor) = request.extensions_mut().get_mut::<ResolvedActor>() {
        if !actor.select_graph(&graph_id) {
            return Err(ApiError::forbidden("credential does not permit this graph"));
        }
    }
    let key = GraphKey::cluster(graph_id.clone());
    let handle = match registry.get(&key) {
        RegistryLookup::Ready(handle) => handle,
        RegistryLookup::Gone => {
            return Err(ApiError::not_found(format!("graph '{graph_id}' not found")));
        }
    };

    // Per-request observability. `Span::current().record` would silently
    // no-op here because no upstream `#[tracing::instrument(...)]` macro
    // declares a `graph_id` field; emit an explicit event instead so the
    // routing decision actually lands in logs.
    info!(graph_id = %handle.key.graph_id, "graph routed");

    request.extensions_mut().insert(handle);
    Ok(next.run(request).await)
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
/// and reserving `Err` for operational failures (401 missing bearer, 500
/// policy-evaluation error). Two sources of the policy engine:
///   * Per-graph handler — passes `handle.policy.as_deref()` so the
///     graph's Cedar rules govern read/change/branch_*/schema_apply.
///   * Management handler — passes `state.server_policy.as_deref()` so
///     server-level Cedar rules govern `graph_list` (the only shipped
///     server-scoped action; runtime `graph_create` / `graph_delete`
///     are deferred until a managed cluster catalog lands).
///
/// The MR-731 invariant lives inside this function: actor identity is
/// supplied as a separate argument from the resolved bearer match. The
/// `PolicyRequest` struct itself does not carry identity (the field was
/// dropped from the type), so handlers cannot smuggle it through the
/// request. See `actor_id_resolves_from_bearer_token_ignoring_client_supplied_headers`
/// at `tests/server.rs`.
pub(crate) fn authorize(
    actor: Option<&ResolvedActor>,
    policy: Option<&PolicyEngine>,
    request: PolicyRequest,
) -> std::result::Result<Authz, ApiError> {
    if let Some(actor) = actor {
        if !actor.permits_action(request.action) {
            return Ok(Authz::Denied(
                "credential does not permit this action".to_string(),
            ));
        }
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
        //   (`graph_list`, future `graph_create`/`graph_delete`).
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
        // operator's only path to enabling it is configuring a
        // cluster-scoped policy bundle, applying the cluster, and
        // restarting the server.
        if request.action.resource_kind() == PolicyResourceKind::Server {
            return Ok(Authz::Denied(
                "server-scoped actions require an explicit cluster policy bundle \
                 applied with `omnigraph cluster apply` and served after restart — \
                 the management surface is closed by default in every runtime state, \
                 including --unauthenticated, so that server topology is never exposed \
                 without operator opt-in."
                    .to_string(),
            ));
        }
        if actor.is_some() && request.action != PolicyAction::Read {
            return Ok(Authz::Denied(
                "server runs in default-deny mode (bearer tokens configured but no \
                 applied policy bundle). Only `read` actions are permitted; configure \
                 a graph or cluster policy bundle in the cluster config, run \
                 `omnigraph cluster apply`, and restart the server to enable other actions."
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

/// Thin wrapper over [`authorize`] for the handlers that treat any denial as a
/// 403: a denial becomes `ApiError::forbidden`, and operational failures
/// (401 missing bearer, 500 policy-evaluation error) propagate unchanged. The
/// stored-query invoke handler does **not** use this — it consumes the
/// [`Authz`] decision directly to hide a denial as a 404 while letting an
/// operational failure keep its true status.
pub(crate) fn authorize_request(
    actor: Option<&ResolvedActor>,
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Query(query): Query<SnapshotQuery>,
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
    let (snapshot, internal_schema_version) = {
        let db = &handle.engine;
        let snapshot = db
            .snapshot_of(ReadTarget::branch(branch.as_str()))
            .await
            .map_err(ApiError::from_omni)?;
        let internal_schema_version = db
            .internal_schema_version_of(ReadTarget::branch(branch.as_str()))
            .await
            .map_err(ApiError::from_omni)?;
        (snapshot, internal_schema_version)
    };
    let output = snapshot_payload(&branch, &snapshot, internal_schema_version)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    Ok(Json(output))
}

/// Header values that flag a response as coming from a deprecated route
/// (RFC 9745 / RFC 8288) and point at the canonical successor.
pub(crate) fn deprecation_headers(successor_link: &'static str) -> [(HeaderName, HeaderValue); 2] {
    [
        (
            HeaderName::from_static("deprecation"),
            HeaderValue::from_static("true"),
        ),
        (
            HeaderName::from_static("link"),
            HeaderValue::from_static(successor_link),
        ),
    ]
}

#[utoipa::path(
    post,
    path = "/read",
    tag = "queries",
    operation_id = "read",
    request_body = ReadRequest,
    responses(
        (status = 200, description = "Legacy token-free query results (response includes `Deprecation: true` + `Link: <query>; rel=\"successor-version\"`)", body = LegacyReadOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Full-text index requires explicit rebuilding; full_text_index_rebuild_required is not cleared by retrying", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
#[deprecated(
    note = "use POST /query instead; /read is kept indefinitely with a byte-stable envelope"
)]
/// **Deprecated** — use [`POST /query`](#tag/queries/operation/query) instead.
///
/// Execute a GQ read query. The route is kept indefinitely with a byte-stable
/// envelope; cell spelling follows the JSON writer. New integrations
/// should target `POST /query`, which has clean field names (`query` /
/// `name`) and a 400-on-mutation guard. Responses from this route include
/// `Deprecation: true` and `Link: <query>; rel="successor-version"`
/// headers per RFC 9745 / RFC 8288 so SDKs and proxies can surface the
/// signal.
pub(crate) async fn server_read(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ReadRequest>,
) -> std::result::Result<([(HeaderName, HeaderValue); 2], Json<LegacyReadOutput>), ApiError> {
    let (selected_name, target, result, _graph_commit_id) = run_query(
        handle,
        actor.as_ref().map(|Extension(actor)| actor),
        &request.query_source,
        request.query_name.as_deref(),
        request.params.as_ref(),
        request.branch,
        request.snapshot,
        false, // /read predates the D2 rule; legacy callers may submit mutating queries here
    )
    .await?;
    Ok((
        deprecation_headers("<query>; rel=\"successor-version\""),
        Json(
            api::read_output(selected_name, &target, result, None)
                .map_err(render_error)?
                .into(),
        ),
    ))
}

#[utoipa::path(
    post,
    path = "/query",
    tag = "queries",
    operation_id = "query",
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query results", body = ReadOutput),
        (status = 400, description = "Bad request - also returned when the query body contains mutations; use POST /mutate (or its deprecated alias POST /change) for write queries", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Full-text index requires explicit rebuilding; full_text_index_rebuild_required is not cleared by retrying", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Execute an inline read query (friendlier-named alternative to `POST /read`).
///
/// Designed for ad-hoc exploration and AI-agent tool-use: short field
/// names (`query`, `name`) match the CLI `-e` flag and the GQ `query`
/// keyword. Mutations (`insert`/`update`/`delete`) are rejected with 400
/// -- use `POST /mutate` (or its deprecated alias `POST /change`) for
/// write queries. It shares `POST /read` target semantics (branch xor
/// snapshot) and the same Cedar action (Read), while its canonical response
/// additionally carries the pinned graph-commit token.
pub(crate) async fn server_query(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Json<ReadOutput>, ApiError> {
    let (selected_name, target, result, graph_commit_id) = run_query(
        handle,
        actor.as_ref().map(|Extension(actor)| actor),
        &request.query,
        request.name.as_deref(),
        request.params.as_ref(),
        request.branch,
        request.snapshot,
        true, // /query is read-only; reject mutations
    )
    .await?;
    Ok(Json(
        api::read_output(selected_name, &target, result, graph_commit_id).map_err(render_error)?,
    ))
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    headers: HeaderMap,
    query: std::result::Result<Query<BlobReadQuery>, QueryRejection>,
) -> std::result::Result<Response, ApiError> {
    let query = parse_blob_read_query(query)?;
    let read = read_blob_for_delivery(&handle, actor.as_ref().map(|Extension(actor)| actor), query)
        .await?;
    blob_transport::serve_blob_get(read, &headers)
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    headers: HeaderMap,
    query: std::result::Result<Query<BlobReadQuery>, QueryRejection>,
) -> std::result::Result<Response, ApiError> {
    let query = parse_blob_read_query(query)?;
    let read = read_blob_for_delivery(&handle, actor.as_ref().map(|Extension(actor)| actor), query)
        .await?;
    blob_transport::serve_blob_head(read, &headers)
}

fn parse_blob_read_query(
    query: std::result::Result<Query<BlobReadQuery>, QueryRejection>,
) -> std::result::Result<BlobReadQuery, ApiError> {
    query.map(|Query(query)| query).map_err(|rejection| {
        ApiError::bad_request(format!(
            "invalid Blob selector query parameters: {}",
            rejection.body_text()
        ))
    })
}

async fn read_blob_for_delivery(
    handle: &GraphHandle,
    actor: Option<&ResolvedActor>,
    query: BlobReadQuery,
) -> std::result::Result<omnigraph::BlobRead, ApiError> {
    let target = resolve_authorized_read_target(handle, actor, query.branch, query.snapshot)
        .await
        .map_err(redact_blob_api_error)?;
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
    redact_blob_api_error(ApiError::from_omni(error))
}

fn redact_blob_api_error(mapped: ApiError) -> ApiError {
    if mapped.status == StatusCode::INTERNAL_SERVER_ERROR {
        error!(
            error_kind = "blob_pre_header_internal",
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    request: std::result::Result<Json<ExportRequest>, JsonRejection>,
) -> std::result::Result<Response, ApiError> {
    let Json(request) = request
        .map_err(|rejection| ApiError::json_rejection("invalid export request", rejection))?;
    let branch = normalize_change_branch(request.branch.as_deref())?;
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
    let (tx, body_stream) = export_transport::channel(queue_lease);
    tokio::spawn(async move {
        // The producer half prevents disconnect from recycling queue bytes
        // until every pending send/scan future owned by this task is gone.
        let _producer_queue_lease = producer_queue_lease;
        let closed_tx = tx.clone();
        let data_tx = tx.clone();
        let export = cut.write_chunks(move |chunk| {
            let data_tx = data_tx.clone();
            async move {
                data_tx
                    .send(export_transport::ExportFrame::Data(Bytes::from(chunk)))
                    .await
                    .map_err(|_| {
                        OmniError::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "served export response closed",
                        ))
                    })
            }
        });
        tokio::pin!(export);
        tokio::select! {
            biased;
            _ = closed_tx.closed() => {
                // Cancelling the pinned export future drops its move-only cut.
            }
            (cut, result) = &mut export => {
                let error = result.err().map(|error| std::io::Error::other(error.to_string()));
                let _ = tx
                    .send(export_transport::ExportFrame::Terminal {
                        cut: Box::new(cut),
                        error,
                    })
                    .await;
            }
        }
    });
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

/// Shared implementation behind `POST /mutate` (canonical) and
/// `POST /change` (deprecated alias). Returns the bare `ChangeOutput`;
/// each route handler wraps it (the alias also attaches Deprecation
/// headers).
/// Shared backend for `/mutate` (canonical) and `/change` (deprecated alias).
///
/// Decoupled from `ChangeRequest` so MR-969's `/queries/{name}` stored-query
/// handler can call this directly with registry-supplied fields without
/// rebuilding the request body. Today's HTTP handlers unpack the request and
/// call here; the registry would do the same.
pub(crate) async fn run_mutate(
    state: AppState,
    handle: Arc<GraphHandle>,
    actor: Option<&ResolvedActor>,
    query: &str,
    name: Option<&str>,
    params_json: Option<&Value>,
    branch: String,
    expected_head: Option<&str>,
) -> std::result::Result<ChangeOutput, ApiError> {
    let actor_arc = actor
        .map(|a| Arc::clone(&a.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor.map(|a| a.actor_id.as_ref());
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Change,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    // Per-actor admission: bound concurrent in-flight mutations and
    // estimated bytes per actor. Cedar runs FIRST so denied requests
    // don't consume admission slots. Estimate uses the request body
    // size as a coarse proxy; engine memory pressure can run higher.
    let est_bytes =
        query.len() as u64 + params_json.map(|p| p.to_string().len() as u64).unwrap_or(0);
    let _admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;
    let (selected_name, query_params) =
        select_named_query(query, name).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let params = query_params_from_json(&query_params, params_json)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let receipt = {
        let db = &handle.engine;
        db.mutate_as_with_expected_head_receipt(
            &branch,
            query,
            &selected_name,
            &params,
            actor_id,
            expected_head,
        )
        .await
        .map_err(ApiError::from_omni)?
    };
    Ok(ChangeOutput {
        branch,
        query_name: selected_name,
        affected_nodes: receipt.result.affected_nodes,
        affected_edges: receipt.result.affected_edges,
        actor_id: actor_id.map(str::to_string),
        commit: receipt.commit.as_ref().map(api::commit_output),
    })
}

/// Shared backend for `/query` (canonical) and `/read` (deprecated alias).
///
/// Mirrors [`run_mutate`]'s decoupled shape so MR-969's stored-query handler
/// can call here with registry-supplied fields. Rejects inline source that
/// contains mutations (D2 rule); callers wanting writes go through
/// [`run_mutate`] instead.
///
/// Intentionally does **not** take [`AppState`] (unlike [`run_mutate`]):
/// reads are not admission-gated today, so there is no `state.workload`
/// consumer. The signature grows the parameter when Phase 1 (MR-976) adds
/// the request envelope's `expect: { max_rows_scanned: N }` budget, or
/// MR-969 extends per-actor admission to stored-read invocations.
pub(crate) async fn run_query(
    handle: Arc<GraphHandle>,
    actor: Option<&ResolvedActor>,
    query: &str,
    name: Option<&str>,
    params_json: Option<&Value>,
    branch: Option<String>,
    snapshot: Option<String>,
    reject_mutations: bool,
) -> std::result::Result<
    (
        String,
        ReadTarget,
        omnigraph_compiler::result::QueryResult,
        Option<String>,
    ),
    ApiError,
> {
    let target = resolve_authorized_read_target(&handle, actor, branch, snapshot).await?;
    let query_decl = select_named_query_decl(query, name)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    if reject_mutations && !query_decl.mutations.is_empty() {
        return Err(ApiError::bad_request(format!(
            "query '{}' contains mutations (insert/update/delete); use POST /mutate for write queries",
            query_decl.name
        )));
    }
    let selected_name = query_decl.name.clone();
    let params = query_params_from_json(&query_decl.params, params_json)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let (result, graph_commit_id) = {
        let db = &handle.engine;
        db.query_with_head(target.clone(), query, &selected_name, &params)
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok((selected_name, target, result, graph_commit_id))
}

/// Resolve one branch-or-snapshot read target and apply the graph's Cedar
/// `read` gate. Every HTTP carrier that accepts this target shape uses this
/// helper so snapshot-to-policy-branch resolution cannot drift by route.
pub(crate) async fn resolve_authorized_read_target(
    handle: &GraphHandle,
    actor: Option<&ResolvedActor>,
    branch: Option<String>,
    snapshot: Option<String>,
) -> std::result::Result<ReadTarget, ApiError> {
    if branch.is_some() && snapshot.is_some() {
        return Err(ApiError::bad_request(
            "request may specify branch or snapshot, not both",
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
            .map_err(ApiError::from_omni)?,
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
    )?;
    Ok(target)
}

#[utoipa::path(
    post,
    path = "/change",
    tag = "mutations",
    operation_id = "change",
    request_body = ChangeRequest,
    responses(
        (status = 200, description = "Mutation results (response includes `Deprecation: true` + `Link: <mutate>; rel=\"successor-version\"`)", body = ChangeOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Write-authority conflict", body = ErrorOutput),
        (status = 413, description = "Keyed write exceeds the per-commit entity or byte ceiling", body = ErrorOutput),
        (status = 424, description = "An allowed external Blob source could not be probed or read", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
#[deprecated(
    note = "use POST /mutate instead; /change retains its request and execution semantics"
)]
/// **Deprecated** — use [`POST /mutate`](#tag/mutations/operation/mutate) instead.
///
/// Apply a GQ mutation to a branch. The deprecated route retains its request
/// and execution semantics, while its response uses the current canonical
/// vocabulary. New integrations should target `POST /mutate`. Responses include
/// `Deprecation: true` and `Link: <mutate>; rel="successor-version"`
/// headers per RFC 9745 / RFC 8288 so SDKs and proxies can surface the
/// signal.
pub(crate) async fn server_change(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<([(HeaderName, HeaderValue); 2], Json<ChangeOutput>), ApiError> {
    reject_graph_commit_expected_head(&headers, "/mutate/if-graph-commit")?;
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    let output = run_mutate(
        state,
        handle,
        actor.as_ref().map(|Extension(actor)| actor),
        &request.query,
        request.name.as_deref(),
        request.params.as_ref(),
        branch,
        None,
    )
    .await?;
    Ok((
        deprecation_headers("<mutate>; rel=\"successor-version\""),
        Json(output),
    ))
}

#[utoipa::path(
    post,
    path = "/mutate",
    tag = "mutations",
    operation_id = "mutate",
    request_body = ChangeRequest,
    responses(
        (status = 200, description = "Mutation results", body = ChangeOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Write-authority conflict", body = ErrorOutput),
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
/// Conditional callers use `POST /mutate/if-graph-commit`. Keeping that
/// capability on a distinct path makes rolling upgrades fail closed: an older
/// server returns 404 instead of ignoring an unknown optional header and
/// mutating unconditionally.
///
/// Pairs with `POST /query` (read-only). The legacy `POST /change` route
/// has identical semantics and is kept as a deprecated alias.
pub(crate) async fn server_mutate(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<Json<ChangeOutput>, ApiError> {
    reject_graph_commit_expected_head(&headers, "/mutate/if-graph-commit")?;
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    Ok(Json(
        run_mutate(
            state,
            handle,
            actor.as_ref().map(|Extension(actor)| actor),
            &request.query,
            request.name.as_deref(),
            request.params.as_ref(),
            branch,
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
/// The dedicated path is the rolling-safe capability signal. Clients must not
/// send this header to `/mutate`: an older server could ignore an unknown
/// optional header after executing the write.
pub(crate) async fn server_mutate_if_graph_commit(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<Json<ChangeOutput>, ApiError> {
    let expected_head = require_graph_commit_expected_head(&headers)?;
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    Ok(Json(
        run_mutate(
            state,
            handle,
            actor.as_ref().map(|Extension(actor)| actor),
            &request.query,
            request.name.as_deref(),
            request.params.as_ref(),
            branch,
            Some(&expected_head),
        )
        .await?,
    ))
}

/// Path parameter for `POST /queries/{name}`.
#[derive(Deserialize)]
pub(crate) struct QueryNamePath {
    name: String,
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
        (status = 409, description = "Stored mutation write-authority conflict, or a full-text index requires explicit rebuilding; full_text_index_rebuild_required is not cleared by retrying", body = ErrorOutput),
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Path(QueryNamePath { name }): Path<QueryNamePath>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<InvokeStoredQueryResponse>, ApiError> {
    reject_graph_commit_expected_head(&headers, &format!("/queries/{name}/if-graph-commit"))?;
    invoke_stored_query(state, handle, actor, name, body, None).await
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Path(QueryNamePath { name }): Path<QueryNamePath>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> std::result::Result<Json<InvokeStoredQueryResponse>, ApiError> {
    let expected_head = require_graph_commit_expected_head(&headers)?;
    invoke_stored_query(state, handle, actor, name, body, Some(expected_head)).await
}

async fn invoke_stored_query(
    state: AppState,
    handle: Arc<GraphHandle>,
    actor: Option<Extension<ResolvedActor>>,
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
        graph = %handle.uri,
        actor = ?actor_ref.map(|a| a.actor_id.as_ref()),
        query = %query_name,
        kind = if is_mutation { "mutate" } else { "read" },
        "stored query invoked"
    );

    if is_mutation {
        if req.snapshot.is_some() {
            return Err(ApiError::bad_request(
                "stored mutation cannot target a snapshot",
            ));
        }
        let branch = req.branch.unwrap_or_else(|| "main".to_string());
        let output = run_mutate(
            state,
            handle,
            actor_ref,
            &source,
            Some(&query_name),
            req.params.as_ref(),
            branch,
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
        let (selected, target, result, graph_commit_id) = run_query(
            handle,
            actor_ref,
            &source,
            Some(&query_name),
            req.params.as_ref(),
            req.branch,
            req.snapshot,
            true,
        )
        .await?;
        Ok(Json(InvokeStoredQueryResponse::Read(
            api::read_output(selected, &target, result, graph_commit_id).map_err(render_error)?,
        )))
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
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
    let schema_source = {
        let db = &handle.engine;
        db.schema_source().to_string()
    };
    Ok(Json(SchemaOutput { schema_source }))
}

#[utoipa::path(
    post,
    path = "/schema/apply",
    tag = "mutations",
    operation_id = "applySchema",
    request_body = SchemaApplyRequest,
    responses(
        (status = 200, description = "Schema apply results", body = SchemaApplyOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Schema apply is disabled for cluster-backed serving; use `omnigraph cluster apply` and restart", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Apply a schema migration.
///
/// Cluster-backed servers reject this route with `409 Conflict`; operators
/// must apply schema changes through `omnigraph cluster apply` and restart.
///
/// Diffs `schema_source` against the current schema and applies the resulting
/// migration steps (add/drop type, add/drop property, etc.). **Destructive**:
/// some steps drop data. Returns the list of steps applied; if `applied` is
/// false the diff was unsupported and no changes were made.
pub(crate) async fn server_schema_apply(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<SchemaApplyRequest>,
) -> std::result::Result<Json<SchemaApplyOutput>, ApiError> {
    let actor_arc = actor
        .as_ref()
        .map(|Extension(actor)| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor
        .as_ref()
        .map(|Extension(actor)| actor.actor_id.as_ref());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::SchemaApply,
            branch: None,
            target_branch: Some("main".to_string()),
        },
    )?;
    // Disable HTTP schema apply on cluster-backed serving AFTER the Cedar gate,
    // so an unauthorized actor gets a 403 (not a 409 that would disclose the
    // server is cluster-backed): 401 → 403 → 409, never leak topology before
    // authorization. An authorized actor gets the actionable 409 signpost.
    if state.routing().config_path.is_some() {
        return Err(ApiError::conflict(
            "server-side schema apply is disabled for cluster-backed serving; \
             update the cluster config, run `omnigraph cluster apply`, and restart \
             the server.",
        ));
    }
    let est_bytes = request.schema_source.len() as u64;
    let _admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;
    let result = {
        let db = &handle.engine;
        let registry = handle.queries.as_deref();
        let label = handle.key.graph_id.as_str().to_string();
        // Engine-layer policy enforcement (MR-722): pass the resolved
        // actor through so apply_schema_as can call enforce() with the
        // authoritative identity. With a policy installed in AppState,
        // engine-side enforcement re-checks the same decision the
        // HTTP-layer authorize_request just made above. PR #3 collapses
        // the redundancy.
        db.apply_schema_as_with_catalog_check(
            &request.schema_source,
            omnigraph::db::SchemaApplyOptions {
                allow_data_loss: request.allow_data_loss,
            },
            actor_id,
            |catalog| {
                if let Some(registry) = registry {
                    validate_registry_against_catalog(registry, catalog, &label)?;
                }
                Ok(())
            },
        )
        .await
        .map_err(ApiError::from_omni)?
    };
    // Physical indexes are derived state. Schema apply records intent only;
    // explicit `ensure_indices` / `optimize` maintenance owns convergence on
    // every surface, including a long-lived server. Keeping the handler free
    // of detached physical writes also makes a successful response describe
    // the complete effect envelope of this request.
    Ok(Json(schema_apply_output(handle.uri.as_str(), result)))
}

/// Authorize one load target without touching request data.
async fn authorize_load_scope(
    handle: &GraphHandle,
    actor: Option<&ResolvedActor>,
    branch: &str,
    from: Option<&str>,
) -> std::result::Result<(), ApiError> {
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
    )
}

/// Shared body for JSON `POST /load` and `POST /ingest` (deprecated):
/// branch-exists / fork-if-`from` check, Cedar authorization, admission, the
/// bulk `load_as`, and the `IngestOutput` mapping.
async fn run_ingest(
    state: AppState,
    handle: Arc<GraphHandle>,
    actor: Option<&ResolvedActor>,
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
    let _admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;

    let receipt = {
        let db = &handle.engine;
        db.load_as_with_receipt(&branch, from.as_deref(), &request.data, mode, actor_id)
            .await
            .map_err(ApiError::from_omni)?
    };

    Ok(ingest_receipt_output(
        handle.uri.as_str(),
        &receipt,
        mode,
        actor_id.map(str::to_string),
    ))
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
///
/// The legacy `POST /ingest` route has identical semantics and is kept as a
/// deprecated alias.
pub(crate) async fn server_load(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<IngestRequest>,
) -> std::result::Result<Json<IngestOutput>, ApiError> {
    Ok(Json(
        run_ingest(
            state,
            handle,
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
        description = "Strict raw graph-level NDJSON. Each nonblank line is exactly one node envelope {\"type\":\"<Node>\",\"data\":{...}} or edge envelope {\"edge\":\"<Edge>\",\"from\":\"<src-id>\",\"to\":\"<dst-id>\",\"data\":{...}}. `data` defaults to {}; optional `data.id` follows ordinary ID semantics. Duplicate, unknown, reserved physical, and noncanonical supplied node-ID members are refused."
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Query(query): Query<GraphBatchLoadQuery>,
    request: Request,
) -> std::result::Result<Json<GraphBatchLoadOutput>, ApiError> {
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

    let data = collect_graph_batch_body(request.into_body()).await?;
    let data = std::str::from_utf8(&data)
        .map_err(|_| ApiError::bad_request("graph-batch request body must be valid UTF-8"))?;
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor.map(|actor| actor.actor_id.as_ref());
    let _admission = state
        .workload
        .try_admit(&actor_arc, data.len() as u64)
        .map_err(ApiError::from_workload_reject)?;

    let receipt = handle
        .engine
        .load_graph_batch_as_with_receipt(&branch, from.as_deref(), data, mode, actor_id)
        .await
        .map_err(ApiError::from_omni)?;
    Ok(Json(graph_batch_load_receipt_output(
        &receipt,
        mode,
        actor_id.map(str::to_string),
    )))
}

#[utoipa::path(
    post,
    path = "/ingest",
    tag = "mutations",
    operation_id = "ingest",
    request_body = IngestRequest,
    responses(
        (status = 200, description = "Load results (response includes `Deprecation: true` + `Link: <load>; rel=\"successor-version\"`)", body = IngestOutput),
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
#[deprecated(note = "use POST /load instead; /ingest retains its parser and branch defaults")]
/// **Deprecated** — use [`POST /load`](#tag/mutations/operation/load) instead.
///
/// Bulk-load NDJSON data into a branch. The deprecated route retains its
/// parser and branch defaults, but its response uses the current canonical
/// vocabulary. New integrations should target `POST /load`. Responses
/// include `Deprecation: true` and `Link: <load>; rel="successor-version"`
/// headers per RFC 9745 / RFC 8288 so SDKs and proxies can surface the signal.
pub(crate) async fn server_ingest(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<IngestRequest>,
) -> std::result::Result<([(HeaderName, HeaderValue); 2], Json<IngestOutput>), ApiError> {
    let output = run_ingest(
        state,
        handle,
        actor.as_ref().map(|Extension(actor)| actor),
        request,
    )
    .await?;
    Ok((
        deprecation_headers("<load>; rel=\"successor-version\""),
        Json(output),
    ))
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
/// Returns branch names sorted alphabetically. Read-only.
pub(crate) async fn server_branch_list(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
) -> std::result::Result<Json<BranchListOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: None,
            target_branch: None,
        },
    )?;
    let mut branches = {
        let db = &handle.engine;
        db.branch_list().await.map_err(ApiError::from_omni)?
    };
    branches.sort();
    Ok(Json(BranchListOutput { branches }))
}

#[utoipa::path(
    post,
    path = "/branches",
    tag = "branches",
    operation_id = "createBranch",
    request_body = BranchCreateRequest,
    responses(
        (status = 200, description = "Branch created", body = BranchCreateOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Branch already exists", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Create a new branch.
///
/// Forks `name` off of `from` (defaults to `main`). The new branch shares
/// backing dataset data with its parent until it is mutated. Returns 409 if `name`
/// already exists.
pub(crate) async fn server_branch_create(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<BranchCreateRequest>,
) -> std::result::Result<Json<BranchCreateOutput>, ApiError> {
    let from = request.from.unwrap_or_else(|| "main".to_string());
    let actor_arc = actor
        .as_ref()
        .map(|Extension(actor)| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchCreate,
            branch: Some(from.clone()),
            target_branch: Some(request.name.clone()),
        },
    )?;
    // Branch metadata only — small constant bytes estimate. The Lance
    // shallow-clone work is bounded by the parent's manifest size, not
    // the request body.
    let _admission = state
        .workload
        .try_admit(&actor_arc, 256)
        .map_err(ApiError::from_workload_reject)?;
    {
        let db = &handle.engine;
        db.branch_create_from_as(
            ReadTarget::branch(&from),
            &request.name,
            actor.as_ref().map(|Extension(a)| a.actor_id.as_ref()),
        )
        .await
        .map_err(ApiError::from_omni)?;
    }
    Ok(Json(BranchCreateOutput {
        uri: handle.uri.clone(),
        from,
        name: request.name,
        actor_id: actor.map(|Extension(actor)| actor.actor_id.as_ref().to_string()),
    }))
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
    branch: String,
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
/// exist.
pub(crate) async fn server_branch_delete(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Path(BranchPath { branch }): Path<BranchPath>,
) -> std::result::Result<Json<BranchDeleteOutput>, ApiError> {
    let actor_arc = actor
        .as_ref()
        .map(|Extension(actor)| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor
        .as_ref()
        .map(|Extension(actor)| actor.actor_id.as_ref());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchDelete,
            branch: None,
            target_branch: Some(branch.clone()),
        },
    )?;
    // Metadata-only manifest tombstone — small constant estimate.
    let _admission = state
        .workload
        .try_admit(&actor_arc, 256)
        .map_err(ApiError::from_workload_reject)?;
    {
        let db = &handle.engine;
        db.branch_delete_as(&branch, actor_id)
            .await
            .map_err(ApiError::from_omni)?;
    }
    Ok(Json(BranchDeleteOutput {
        uri: handle.uri.clone(),
        name: branch,
        actor_id: actor_id.map(str::to_string),
    }))
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
/// unchanged in that case. **Destructive** to `target` on success.
///
/// With `delete_branch: true` the source branch is deleted after a successful
/// merge, under its own `branch_delete` policy check. The merge is durable by
/// then, so a deletion refusal or failure never fails the request; it is
/// reported via `branch_deleted: false` + `branch_delete_error`.
pub(crate) async fn server_branch_merge(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<BranchMergeRequest>,
) -> std::result::Result<Json<BranchMergeOutput>, ApiError> {
    let target = request.target.unwrap_or_else(|| "main".to_string());
    let actor_arc = actor
        .as_ref()
        .map(|Extension(actor)| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor
        .as_ref()
        .map(|Extension(actor)| actor.actor_id.as_ref());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchMerge,
            branch: Some(request.source.clone()),
            target_branch: Some(target.clone()),
        },
    )?;
    // Merge body is small JSON; the heavy work is in the engine but is
    // bounded per-(table, branch) by the writer queue. Small constant
    // estimate suffices for the actor in-flight count.
    let _admission = state
        .workload
        .try_admit(&actor_arc, 256)
        .map_err(ApiError::from_workload_reject)?;
    let outcome = {
        let db = &handle.engine;
        db.branch_merge_as(&request.source, &target, actor_id)
            .await
            .map_err(ApiError::from_omni)?
    };
    let (branch_deleted, branch_delete_error) = if request.delete_branch {
        match delete_merged_source_branch(
            &handle,
            actor.as_ref().map(|Extension(a)| a),
            &request.source,
        )
        .await
        {
            Ok(()) => (Some(true), None),
            Err(message) => (Some(false), Some(message)),
        }
    } else {
        (None, None)
    };
    Ok(Json(BranchMergeOutput {
        source: request.source,
        target,
        outcome: outcome.into(),
        actor_id: actor_id.map(str::to_string),
        branch_deleted,
        branch_delete_error,
    }))
}

/// Delete the source branch of a just-landed merge, mirroring
/// `server_branch_delete`'s authorization (same action and target scope) but
/// converting every failure — policy denial, dependent-branch refusal,
/// operational error — into a message instead of an error status: the merge is
/// already durable, so the request must not report failure for it.
async fn delete_merged_source_branch(
    handle: &GraphHandle,
    actor: Option<&ResolvedActor>,
    source: &str,
) -> std::result::Result<(), String> {
    match authorize(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::BranchDelete,
            branch: None,
            target_branch: Some(source.to_string()),
        },
    ) {
        Ok(Authz::Allowed) => {}
        Ok(Authz::Denied(message)) => return Err(message),
        Err(err) => return Err(err.message.into()),
    }
    let actor_id = actor.map(|actor| actor.actor_id.as_ref());
    handle
        .engine
        .branch_delete_as(source, actor_id)
        .await
        .map_err(|err| err.to_string())
}

#[utoipa::path(
    get,
    path = "/commits",
    tag = "commits",
    operation_id = "listCommits",
    params(CommitListQuery),
    responses(
        (status = 200, description = "List of commits", body = CommitListOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// List commits, most recent first.
///
/// `branch` selects which history to list: a named branch returns the history
/// reachable from that branch's head (the main commits inherited up to the
/// fork plus the branch-authored commits); omitting it returns `main`'s
/// history. There is no cross-branch listing. Ordering is part of the
/// contract — newest first by (graph-manifest version, created-at, commit id) — and
/// a future `cursor`/`limit` pagination will be keyset-based on that same
/// order. Read-only.
pub(crate) async fn server_commit_list(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Query(query): Query<CommitListQuery>,
) -> std::result::Result<Json<CommitListOutput>, ApiError> {
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
    let commits = {
        let db = &handle.engine;
        db.list_commits(Some(branch.as_str()))
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(CommitListOutput {
        commits: commits.iter().map(api::commit_output).collect(),
    }))
}

/// Path-param shape for [`server_commit_show`]. See [`BranchPath`]
/// for the design rationale — same pattern, different field name.
#[derive(Deserialize)]
pub(crate) struct CommitPath {
    commit_id: String,
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Path(CommitPath { commit_id }): Path<CommitPath>,
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
    query_source: &str,
    requested_name: Option<&str>,
) -> Result<omnigraph_compiler::query::ast::QueryDecl> {
    let parsed = parse_query(query_source)?;
    let query = if let Some(name) = requested_name {
        parsed
            .queries
            .into_iter()
            .find(|query| query.name == name)
            .ok_or_else(|| color_eyre::eyre::eyre!("query '{}' not found", name))?
    } else if parsed.queries.len() == 1 {
        parsed.queries.into_iter().next().unwrap()
    } else {
        bail!("query file contains multiple queries; pass --name");
    };
    Ok(query)
}

pub(crate) fn select_named_query(
    query_source: &str,
    requested_name: Option<&str>,
) -> Result<(String, Vec<omnigraph_compiler::query::ast::Param>)> {
    let query = select_named_query_decl(query_source, requested_name)?;
    Ok((query.name, query.params))
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

    #[test]
    fn change_route_error_hides_substrate_paths() {
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
    }

    #[test]
    fn change_route_error_hides_internal_manifest_table_keys() {
        let mapped = change_route_error(OmniError::manifest_internal(
            "invalid table key 'node:SecretType' at internal version 7",
        ));
        assert_eq!(mapped.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            !mapped.message().contains("node:SecretType"),
            "change route leaked an internal table key: {}",
            mapped.message()
        );
    }

    #[test]
    fn change_route_error_passes_only_allowlisted_graph_errors_through() {
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
    }

    #[test]
    fn change_route_recovery_exposes_id_but_redacts_internal_reason() {
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
    }
}

#[cfg(test)]
mod blob_error_tests {
    use super::*;

    #[tokio::test]
    async fn pre_header_internal_errors_do_not_expose_physical_storage_or_identity() {
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

        let response = redact_blob_api_error(ApiError::internal(
            "snapshot manifest at s3://private-bucket/graph/__manifest",
        ))
        .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let output: ErrorOutput = serde_json::from_slice(&body).unwrap();
        assert_eq!(output.error, "Blob delivery failed before response headers");
        assert!(!String::from_utf8_lossy(&body).contains("private-bucket"));
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
    pub cursor: Option<String>,
    pub start: Option<String>,
    pub page_token: Option<String>,
    pub limit: Option<usize>,
    pub kinds: Vec<api::EntityKindOutput>,
    pub types: Vec<String>,
    pub ops: Vec<api::ChangeOpOutput>,
}

pub(crate) const COMMIT_CHANGES_PARAMS: &[&str] = &["page_token", "limit", "kind", "type", "op"];
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
/// a large commit continues via the opaque `page_token`.
pub(crate) async fn server_commit_changes(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Path(CommitPath { commit_id }): Path<CommitPath>,
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

/// Contextual 404 projection. `Manifest::NotFound` is a broad internal
/// category, so its original text is never reused; the handler supplies the
/// exact public resource spelling it attempted to resolve.
fn change_route_not_found(error: OmniError, public_message: String) -> ApiError {
    match error {
        OmniError::Manifest(manifest) if manifest.kind == ManifestErrorKind::NotFound => {
            tracing::debug!(internal_error = %manifest, %public_message, "change resource not found");
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
/// variant — is logged and collapsed to a fixed 500 so adding an engine error
/// can never accidentally expose an internal storage identifier or sidecar.
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
        | OmniError::ChangeFeedGap { .. }
        | OmniError::CommitHasNoParent { .. }
        | OmniError::ChangeSchemaBoundary { .. }) => ApiError::from_omni(safe),
        OmniError::RecoveryRequired {
            operation_id,
            reason,
        } => {
            tracing::warn!(%operation_id, %reason, "change route requires recovery");
            ApiError::recovery_required(
                "recovery required before changes can be read".to_string(),
                operation_id,
            )
        }
        other => {
            tracing::error!(error = %other, "change route internal error");
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

/// Normalize a caller-supplied change-surface branch BEFORE authorization so
/// Cedar and the engine classify the same identity. The engine trims late
/// (its own branch normalization), so authorizing the raw string would let a
/// padded spelling like " main " be classified as an unprotected named branch
/// and then resolve to protected main — a policy bypass. Empty-after-trim is
/// a malformed request rather than an implicit main.
fn normalize_change_branch(branch: Option<&str>) -> std::result::Result<String, ApiError> {
    let trimmed = branch.unwrap_or("main").trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request("branch name cannot be empty"));
    }
    Ok(trimmed.to_string())
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
/// state.
pub(crate) async fn server_changes_feed(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    axum::extract::RawQuery(raw): axum::extract::RawQuery,
) -> std::result::Result<Json<api::ChangeFeedOutput>, ApiError> {
    let params = parse_change_query(raw.as_deref(), CHANGE_FEED_PARAMS)?;
    validate_change_http_limit(params.limit)?;
    let branch = normalize_change_branch(params.branch.as_deref())?;
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
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<api::ChangeBaselineRequest>,
) -> std::result::Result<Response, ApiError> {
    let branch = normalize_change_branch(request.branch.as_deref())?;
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
    let terminal_record = {
        let mut line = serde_json::to_vec(&api::ChangeBaselineRecord {
            baseline: api::change_baseline_output(&handshake),
        })
        .map_err(|error| ApiError::internal(format!("encode baseline record: {error}")))?;
        line.push(b'\n');
        Bytes::from(line)
    };

    let producer_queue_lease = Arc::clone(&queue_lease);
    let (tx, body_stream) = export_transport::channel(queue_lease);
    tokio::spawn(async move {
        let _producer_queue_lease = producer_queue_lease;
        let closed_tx = tx.clone();
        let data_tx = tx.clone();
        let export = cut.write_chunks(move |chunk| {
            let data_tx = data_tx.clone();
            async move {
                data_tx
                    .send(export_transport::ExportFrame::Data(Bytes::from(chunk)))
                    .await
                    .map_err(|_| {
                        OmniError::Io(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            "served baseline response closed",
                        ))
                    })
            }
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
                let error = match result {
                    Ok(()) => {
                        match tx
                            .send(export_transport::ExportFrame::Data(terminal_record))
                            .await
                        {
                            Ok(()) => None,
                            Err(_) => Some(std::io::Error::new(
                                std::io::ErrorKind::BrokenPipe,
                                "served baseline response closed",
                            )),
                        }
                    }
                    Err(error) => Some(std::io::Error::other(error.to_string())),
                };
                let _ = tx
                    .send(export_transport::ExportFrame::Terminal {
                        cut: Box::new(cut),
                        error,
                    })
                    .await;
            }
        }
    });
    let body = Body::from_stream(body_stream);
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, "application/x-ndjson; charset=utf-8")],
        body,
    )
        .into_response())
}
