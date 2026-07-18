//! HTTP route handlers, the bearer-auth middleware, per-request
//! authorization, and the cluster-prefix OpenAPI rewrite (moved
//! verbatim from lib.rs in the modularization).

use super::*;

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
/// List every graph currently registered with this server (MR-668).
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

    let mut graphs: Vec<GraphInfo> = registry
        .list()
        .into_iter()
        .map(|handle| GraphInfo {
            graph_id: handle.key.graph_id.as_str().to_string(),
            uri: handle.uri.clone(),
        })
        .collect();
    graphs.sort_by(|a, b| a.graph_id.cmp(&b.graph_id));
    Ok(Json(GraphListResponse { graphs }))
}

pub(crate) async fn server_openapi(State(state): State<AppState>) -> Json<utoipa::openapi::OpenApi> {
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
const ALWAYS_FLAT_PATHS: &[&str] = &["/healthz", "/graphs"];

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

pub(crate) fn log_policy_decision(actor_id: &str, request: &PolicyRequest, decision: &PolicyDecision) {
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
        (status = 200, description = "Database snapshot", body = api::SnapshotOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Read the current snapshot of a branch.
///
/// Returns the manifest version plus per-table metadata (path, version, row
/// count) for every table on the branch. Defaults to `main` when `branch` is
/// omitted. Read-only.
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
    Ok(Json(snapshot_payload(
        &branch,
        &snapshot,
        internal_schema_version,
    )))
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
        (status = 200, description = "Query results (response includes `Deprecation: true` + `Link: <query>; rel=\"successor-version\"`)", body = ReadOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
#[deprecated(note = "use POST /query instead; /read is kept indefinitely for byte-stable back-compat")]
/// **Deprecated** — use [`POST /query`](#tag/queries/operation/query) instead.
///
/// Execute a GQ read query. Behavior is unchanged from prior releases; the
/// route is kept indefinitely for byte-stable back-compat. New integrations
/// should target `POST /query`, which has clean field names (`query` /
/// `name`) and a 400-on-mutation guard. Responses from this route include
/// `Deprecation: true` and `Link: <query>; rel="successor-version"`
/// headers per RFC 9745 / RFC 8288 so SDKs and proxies can surface the
/// signal.
pub(crate) async fn server_read(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ReadRequest>,
) -> std::result::Result<([(HeaderName, HeaderValue); 2], Json<ReadOutput>), ApiError> {
    let (selected_name, target, result) = run_query(
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
        Json(api::read_output(selected_name, &target, result)),
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
    ),
    security(("bearer_token" = [])),
)]
/// Execute an inline read query (friendlier-named alternative to `POST /read`).
///
/// Designed for ad-hoc exploration and AI-agent tool-use: short field
/// names (`query`, `name`) match the CLI `-e` flag and the GQ `query`
/// keyword. Mutations (`insert`/`update`/`delete`) are rejected with 400
/// -- use `POST /mutate` (or its deprecated alias `POST /change`) for
/// write queries. Otherwise behaves identically to `POST /read`: same
/// target semantics (branch xor snapshot), same Cedar action (Read),
/// same response shape.
pub(crate) async fn server_query(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<QueryRequest>,
) -> std::result::Result<Json<ReadOutput>, ApiError> {
    let (selected_name, target, result) = run_query(
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
    Ok(Json(api::read_output(selected_name, &target, result)))
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
    ),
    security(("bearer_token" = [])),
)]
/// Stream the contents of a branch as NDJSON.
///
/// Emits one JSON object per line (`application/x-ndjson`). Filter with
/// `type_names` (node/edge type names) and/or `table_keys`; both empty
/// streams the entire branch. Suitable for large exports — the response is
/// streamed, not buffered. Read-only.
pub(crate) async fn server_export(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ExportRequest>,
) -> std::result::Result<Response, ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Export,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    let engine = Arc::clone(&handle.engine);
    let type_names = request.type_names.clone();
    let table_keys = request.table_keys.clone();
    let (tx, rx) = mpsc::unbounded_channel::<std::result::Result<Bytes, io::Error>>();
    tokio::spawn(async move {
        let result = {
            let mut writer = ExportStreamWriter { sender: tx.clone() };
            engine
                .export_jsonl_to_writer(&branch, &type_names, &table_keys, &mut writer)
                .await
        };
        if let Err(err) = result {
            let _ = tx.send(Err(io::Error::other(err.to_string())));
        }
    });
    let body = Body::from_stream(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, "application/x-ndjson; charset=utf-8")],
        body,
    )
        .into_response())
}

/// Streaming chunk size for blob bodies. Memory per in-flight response is
/// bounded by this constant regardless of blob size.
const BLOB_STREAM_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
/// Bytes read for content-type sniffing (HEAD and GET alike, so their
/// headers agree).
const BLOB_SNIFF_BYTES: u64 = 512;

/// Outcome of parsing a `Range` header against a known total size.
enum BlobRangeOutcome {
    /// No header, a syntactically invalid header, or a multi-range request
    /// (unsupported by documented contract) — serve the full body.
    Full,
    /// One satisfiable range, half-open `[start, end)`.
    Partial { start: u64, end: u64 },
    /// A syntactically valid single range that cannot be satisfied — 416.
    Unsatisfiable,
}

/// Parse a single-range `bytes=` header per RFC 9110 §14. Multi-range
/// requests are deliberately ignored (full response) — single-range only is
/// the documented contract.
fn parse_blob_range(header: Option<&HeaderValue>, total: u64) -> BlobRangeOutcome {
    let Some(raw) = header.and_then(|value| value.to_str().ok()) else {
        return BlobRangeOutcome::Full;
    };
    let Some(spec) = raw.strip_prefix("bytes=") else {
        return BlobRangeOutcome::Full;
    };
    if spec.contains(',') {
        return BlobRangeOutcome::Full;
    }
    let spec = spec.trim();
    if let Some(suffix) = spec.strip_prefix('-') {
        // Suffix range: last N bytes.
        let Ok(n) = suffix.parse::<u64>() else {
            return BlobRangeOutcome::Full;
        };
        if n == 0 || total == 0 {
            return BlobRangeOutcome::Unsatisfiable;
        }
        return BlobRangeOutcome::Partial {
            start: total.saturating_sub(n),
            end: total,
        };
    }
    let (start_str, end_str) = match spec.split_once('-') {
        Some(parts) => parts,
        None => return BlobRangeOutcome::Full,
    };
    let Ok(start) = start_str.parse::<u64>() else {
        return BlobRangeOutcome::Full;
    };
    if start >= total {
        return BlobRangeOutcome::Unsatisfiable;
    }
    if end_str.is_empty() {
        return BlobRangeOutcome::Partial { start, end: total };
    }
    let Ok(end_inclusive) = end_str.parse::<u64>() else {
        return BlobRangeOutcome::Full;
    };
    if end_inclusive < start {
        return BlobRangeOutcome::Full;
    }
    BlobRangeOutcome::Partial {
        start,
        end: end_inclusive.saturating_add(1).min(total),
    }
}

/// Minimal magic-byte content-type sniffer for common media types; falls
/// back to `application/octet-stream`. Deliberately small and local — Lance
/// stores no MIME anywhere, so media type is a best-effort presentation
/// concern, never contract.
fn sniff_blob_content_type(head: &[u8]) -> &'static str {
    match head {
        [0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, ..] => "image/png",
        [0xFF, 0xD8, 0xFF, ..] => "image/jpeg",
        [b'G', b'I', b'F', b'8', ..] => "image/gif",
        [b'R', b'I', b'F', b'F', _, _, _, _, b'W', b'E', b'B', b'P', ..] => "image/webp",
        [b'%', b'P', b'D', b'F', ..] => "application/pdf",
        [0x50, 0x4B, 0x03, 0x04, ..] | [0x50, 0x4B, 0x05, 0x06, ..] => "application/zip",
        [0x1F, 0x8B, ..] => "application/gzip",
        [0x1A, 0x45, 0xDF, 0xA3, ..] => "video/webm",
        [_, _, _, _, b'f', b't', b'y', b'p', ..] => "video/mp4",
        _ => "application/octet-stream",
    }
}

#[utoipa::path(
    get,
    path = "/blob",
    tag = "data",
    operation_id = "getBlob",
    params(BlobQuery),
    responses(
        (status = 200, description = "Full blob content", content_type = "application/octet-stream"),
        (status = 206, description = "Requested byte range (single `Range: bytes=` range; multi-range is unsupported and answered with the full body)", content_type = "application/octet-stream"),
        (status = 302, description = "External blob reference; `Location` carries the stored absolute URI. The server never resolves or proxies the external location."),
        (status = 304, description = "Not modified (`If-None-Match` matched the ETag)"),
        (status = 400, description = "Property is not a Blob, or branch and snapshot were both given (they are mutually exclusive)", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 404, description = "Unknown type, unknown id, or null blob cell", body = ErrorOutput),
        (status = 416, description = "Requested range not satisfiable; `Content-Range` reports the total size"),
    ),
    security(("bearer_token" = [])),
)]
/// Stream one row's Blob property.
///
/// Resolves the cell at one pinned view of the target — `branch` xor
/// `snapshot`, defaulting to branch `main` (the same target semantics as
/// `POST /read`).
/// Internal blobs stream with `Content-Length`, `Accept-Ranges: bytes`, a
/// strong identity-derived `ETag`, and single-range `206` support; external
/// URI references answer `302 Found` without server-side resolution. `HEAD`
/// returns the same headers with no body (content-type sniffing reads at
/// most 512 bytes on both methods so the headers agree). Read-only; not
/// admission-gated.
pub(crate) async fn server_blob_get(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<BlobQuery>,
) -> std::result::Result<Response, ApiError> {
    if query.branch.is_some() && query.snapshot.is_some() {
        return Err(ApiError::bad_request(
            "request may specify branch or snapshot, not both",
        ));
    }
    let actor = actor.as_ref().map(|Extension(actor)| actor);
    let target = read_target_from_request(query.branch.clone(), query.snapshot.clone());
    let policy_branch = match &target {
        ReadTarget::Branch(branch) => Some(branch.clone()),
        ReadTarget::Snapshot(_) if handle.policy.is_some() && actor.is_some() => {
            let db = &handle.engine;
            db.resolved_branch_of(target.clone())
                .await
                .map(|branch| branch.or_else(|| Some("main".to_string())))
                .map_err(ApiError::from_omni)?
        }
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
    let blob = handle
        .engine
        .read_blob_at(target, &query.type_name, &query.id, &query.prop)
        .await
        .map_err(ApiError::from_omni)?;

    let reader = match blob.content {
        omnigraph::db::BlobContent::ExternalRef { uri } => {
            // GET and HEAD both redirect. The engine classified from the
            // descriptor alone, so an unreachable external location still
            // redirects — the client owns the fetch.
            let location = HeaderValue::from_str(&uri).map_err(|_| {
                ApiError::internal(format!(
                    "stored external blob URI is not a valid header value: {uri}"
                ))
            })?;
            return Ok((StatusCode::FOUND, [(LOCATION, location)]).into_response());
        }
        omnigraph::db::BlobContent::Streamed(reader) => reader,
    };

    let total = reader.size();
    let etag = blob_etag(&blob.version_tag, &query.prop);
    if headers
        .get(IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == etag || value.trim() == "*")
    {
        return Ok((
            StatusCode::NOT_MODIFIED,
            [(ETAG, HeaderValue::from_str(&etag).expect("hex etag"))],
        )
            .into_response());
    }

    // `If-Range`: honor the Range header only when the validator still
    // matches; otherwise serve the full current representation.
    let range_applicable = match headers.get(IF_RANGE).and_then(|value| value.to_str().ok()) {
        Some(validator) => validator.trim() == etag,
        None => true,
    };
    let (status, start, end) = if range_applicable {
        match parse_blob_range(headers.get(RANGE), total) {
            BlobRangeOutcome::Full => (StatusCode::OK, 0, total),
            BlobRangeOutcome::Partial { start, end } => (StatusCode::PARTIAL_CONTENT, start, end),
            BlobRangeOutcome::Unsatisfiable => {
                return Ok((
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    [(
                        CONTENT_RANGE,
                        HeaderValue::from_str(&format!("bytes */{total}")).expect("ascii"),
                    )],
                )
                    .into_response());
            }
        }
    } else {
        (StatusCode::OK, 0, total)
    };

    // One bounded prefix read for sniffing, shared by HEAD and GET.
    let sniff_len = total.min(BLOB_SNIFF_BYTES);
    let content_type = if sniff_len > 0 {
        let head = reader
            .read_range(0..sniff_len)
            .await
            .map_err(ApiError::from_omni)?;
        sniff_blob_content_type(&head)
    } else {
        "application/octet-stream"
    };

    let mut response_headers = HeaderMap::new();
    response_headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(ETAG, HeaderValue::from_str(&etag).expect("hex etag"));
    response_headers.insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&(end - start).to_string()).expect("ascii"),
    );
    if status == StatusCode::PARTIAL_CONTENT {
        response_headers.insert(
            CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end - 1, total))
                .expect("ascii"),
        );
    }

    if method == Method::HEAD {
        return Ok((status, response_headers).into_response());
    }

    let body = Body::from_stream(stream::try_unfold(
        (reader, start),
        move |(reader, position)| async move {
            if position >= end {
                return Ok(None);
            }
            let chunk_end = position.saturating_add(BLOB_STREAM_CHUNK_BYTES).min(end);
            let chunk = reader
                .read_range(position..chunk_end)
                .await
                .map_err(|err| io::Error::other(err.to_string()))?;
            Ok::<_, io::Error>(Some((chunk, (reader, chunk_end))))
        },
    ));
    Ok((status, response_headers, body).into_response())
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
    let est_bytes = query.len() as u64
        + params_json
            .map(|p| p.to_string().len() as u64)
            .unwrap_or(0);
    let _admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;
    let (selected_name, query_params) =
        select_named_query(query, name).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let params = query_params_from_json(&query_params, params_json)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = {
        let db = &handle.engine;
        db.mutate_as(&branch, query, &selected_name, &params, actor_id)
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(ChangeOutput {
        branch,
        query_name: selected_name,
        affected_nodes: result.affected_nodes,
        affected_edges: result.affected_edges,
        actor_id: actor_id.map(str::to_string),
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
) -> std::result::Result<(String, ReadTarget, omnigraph_compiler::result::QueryResult), ApiError> {
    if branch.is_some() && snapshot.is_some() {
        return Err(ApiError::bad_request(
            "request may specify branch or snapshot, not both",
        ));
    }

    let target = read_target_from_request(branch, snapshot);
    let policy_branch = match &target {
        ReadTarget::Branch(branch) => Some(branch.clone()),
        ReadTarget::Snapshot(_) if handle.policy.is_some() && actor.is_some() => {
            let db = &handle.engine;
            db.resolved_branch_of(target.clone())
                .await
                .map(|branch| branch.or_else(|| Some("main".to_string())))
                .map_err(ApiError::from_omni)?
        }
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
    let query_decl =
        select_named_query_decl(query, name).map_err(|err| ApiError::bad_request(err.to_string()))?;
    if reject_mutations && !query_decl.mutations.is_empty() {
        return Err(ApiError::bad_request(format!(
            "query '{}' contains mutations (insert/update/delete); use POST /mutate for write queries",
            query_decl.name
        )));
    }
    let selected_name = query_decl.name.clone();
    let params = query_params_from_json(&query_decl.params, params_json)
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = {
        let db = &handle.engine;
        db.query(target.clone(), query, &selected_name, &params)
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok((selected_name, target, result))
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
        (status = 413, description = "Keyed write exceeds the per-commit row or byte ceiling", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
#[deprecated(note = "use POST /mutate instead; /change is kept indefinitely for back-compat")]
/// **Deprecated** — use [`POST /mutate`](#tag/mutations/operation/mutate) instead.
///
/// Apply a GQ mutation to a branch. Behavior is unchanged; the route is
/// kept indefinitely for back-compat. New integrations should target
/// `POST /mutate`, which has identical semantics and a name that pairs
/// cleanly with `POST /query`. Responses from this route include
/// `Deprecation: true` and `Link: <mutate>; rel="successor-version"`
/// headers per RFC 9745 / RFC 8288 so SDKs and proxies can surface the
/// signal.
pub(crate) async fn server_change(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<([(HeaderName, HeaderValue); 2], Json<ChangeOutput>), ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    let output = run_mutate(
        state,
        handle,
        actor.as_ref().map(|Extension(actor)| actor),
        &request.query,
        request.name.as_deref(),
        request.params.as_ref(),
        branch,
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
        (status = 413, description = "Keyed write exceeds the per-commit row or byte ceiling", body = ErrorOutput),
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
/// Pairs with `POST /query` (read-only). The legacy `POST /change` route
/// has identical semantics and is kept as a deprecated alias.
pub(crate) async fn server_mutate(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<Json<ChangeOutput>, ApiError> {
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
    params(("name" = String, Path, description = "Stored query name (the registry key)")),
    request_body = Option<InvokeStoredQueryRequest>,
    responses(
        (status = 200, description = "Read envelope (ReadOutput) or mutation envelope (ChangeOutput), serialized untagged", body = InvokeStoredQueryResponse),
        (status = 400, description = "Bad request (param type error; snapshot on a stored mutation)", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden (the inner `change` gate for a stored mutation)", body = ErrorOutput),
        (status = 404, description = "Unknown stored query, or `invoke_query` denied — indistinguishable to a caller without the grant", body = ErrorOutput),
        (status = 409, description = "Stored mutation write-authority conflict", body = ErrorOutput),
        (status = 413, description = "Stored keyed mutation exceeds the per-commit row or byte ceiling", body = ErrorOutput),
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
    body: Bytes,
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
        )
        .await?;
        Ok(Json(InvokeStoredQueryResponse::Change(output)))
    } else {
        let (selected, target, result) = run_query(
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
        Ok(Json(InvokeStoredQueryResponse::Read(api::read_output(
            selected, &target, result,
        ))))
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
/// Useful for clients that want to introspect available types and tables
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
/// migration steps (add/drop type, add/drop column, etc.). **Destructive**:
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

/// Shared body for `POST /load` (canonical) and `POST /ingest` (deprecated):
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

    let branch_exists = {
        let db = &handle.engine;
        db.branch_list()
            .await
            .map_err(ApiError::from_omni)?
            .into_iter()
            .any(|name| name == branch)
    };

    if !branch_exists {
        match from.as_deref() {
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
                    target_branch: Some(branch.clone()),
                },
            )?,
        }
    }
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Change,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    let est_bytes = request.data.len() as u64;
    let _admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;

    let result = {
        let db = &handle.engine;
        db.load_as(&branch, from.as_deref(), &request.data, mode, actor_id)
            .await
            .map_err(ApiError::from_omni)?
    };

    Ok(ingest_output(
        handle.uri.as_str(),
        &result,
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
        (status = 413, description = "Keyed load exceeds the per-commit row or byte ceiling", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Bulk-load NDJSON data into a branch (canonical load endpoint).
///
/// `data` is NDJSON with one record per line. `mode` controls behavior on
/// existing rows: `merge` upserts by id (default), `append` strictly inserts
/// absent ids, and `overwrite` replaces table contents. Branch creation is opt-in by
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
        (status = 413, description = "Keyed load exceeds the per-commit row or byte ceiling", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 503, description = "An overlapping durable recovery intent must be resolved before retry", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
#[deprecated(note = "use POST /load instead; /ingest is kept indefinitely for back-compat")]
/// **Deprecated** — use [`POST /load`](#tag/mutations/operation/load) instead.
///
/// Bulk-load NDJSON data into a branch. Behavior is unchanged; the route is
/// kept indefinitely for back-compat. New integrations should target
/// `POST /load`, which has identical semantics. Responses from this route
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
/// table data with its parent until it is mutated. Returns 409 if `name`
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
        (status = 413, description = "Merge row, byte, or recovery-chain ceiling exceeded before effects", body = ErrorOutput),
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
        match delete_merged_source_branch(&handle, actor.as_ref().map(|Extension(a)| a), &request.source)
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
        Err(err) => return Err(err.message),
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
/// List commits.
///
/// Filter by `branch` to get the commits on a single branch (most recent
/// first); omit to list across all branches. Read-only.
pub(crate) async fn server_commit_list(
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Query(query): Query<CommitListQuery>,
) -> std::result::Result<Json<CommitListOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Read,
            branch: query.branch.clone(),
            target_branch: None,
        },
    )?;
    let commits = {
        let db = &handle.engine;
        db.list_commits(query.branch.as_deref())
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
/// Returns the commit's manifest version, parent commit(s), and creation
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

pub(crate) fn read_target_from_request(branch: Option<String>, snapshot: Option<String>) -> ReadTarget {
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
