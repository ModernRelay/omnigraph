pub use omnigraph_api_types as api;
pub mod auth;
pub use omnigraph_config as config;
pub mod graph_id;
pub mod identity;
pub mod policy;
pub use omnigraph_queries as queries;
pub mod registry;
pub mod workload;

pub use graph_id::GraphId;
pub use identity::{AuthSource, GraphKey, ResolvedActor, Scope, TenantId};
pub use registry::{GraphHandle, GraphRegistry, InsertError, RegistryLookup, RegistrySnapshot};

use crate::queries::{QueryRegistry, check, format_check_breakages};

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use api::{
    BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput, BranchListOutput,
    BranchMergeOutput, BranchMergeRequest, ChangeOutput, ChangeRequest, CommitListOutput,
    CommitListQuery, ErrorCode, ErrorOutput, ExportRequest, GraphInfo, GraphListResponse,
    HealthOutput, IngestOutput, IngestRequest, InvokeStoredQueryRequest, InvokeStoredQueryResponse,
    QueriesCatalogOutput, QueryRequest, ReadOutput, ReadRequest, SchemaApplyOutput,
    SchemaApplyRequest, SchemaOutput, SnapshotQuery, ingest_output, schema_apply_output,
    snapshot_payload,
};
pub use auth::{AWS_SECRET_ENV, EnvOrFileTokenSource, TokenSource, resolve_token_source};
use axum::body::{Body, Bytes};
use axum::extract::DefaultBodyLimit;
use axum::extract::{Extension, OriginalUri, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use color_eyre::eyre::{Result, WrapErr, bail};
pub use config::{
    AliasCommand, AliasConfig, CliDefaults, DEFAULT_CONFIG_FILE, OmnigraphConfig, PolicySettings,
    ProjectConfig, QueryDefaults, ReadOutputFormat, ServerDefaults, TableCellLayout, TargetConfig,
    graph_resource_id_for_selection, load_config,
};
use futures::stream;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::{ManifestConflictDetails, ManifestErrorKind, OmniError};
use omnigraph::storage::normalize_root_uri;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::json_params_to_param_map;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::{JsonParamMode, ParamMap};
pub use policy::{
    PolicyAction, PolicyCompiler, PolicyConfig, PolicyDecision, PolicyEngine, PolicyExpectation,
    PolicyRequest, PolicyResourceKind, PolicyTestConfig,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;
use utoipa::openapi::path::{Parameter, ParameterIn};
use utoipa::openapi::schema::{Object, Type};
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};

type BearerTokenHash = [u8; 32];

fn hash_bearer_token(token: &str) -> BearerTokenHash {
    let digest = Sha256::digest(token.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Omnigraph API",
        description = "HTTP API for the Omnigraph graph database",
    ),
    paths(
        server_health,
        server_graphs_list,
        server_snapshot,
        // deprecated; the #[deprecated] attribute on the handler
        // surfaces as `deprecated: true` on the OpenAPI operation.
        #[allow(deprecated)] server_read,
        server_query,
        server_export,
        #[allow(deprecated)] server_change,
        server_mutate,
        server_list_queries,
        server_invoke_query,
        server_schema_apply,
        server_schema_get,
        server_ingest,
        server_branch_list,
        server_branch_create,
        server_branch_delete,
        server_branch_merge,
        server_commit_list,
        server_commit_show,
    ),
    modifiers(&SecurityAddon),
)]
pub struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme(
                "bearer_token",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
    }
}

const DEFAULT_REQUEST_BODY_LIMIT_BYTES: usize = 1_048_576;
const INGEST_REQUEST_BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024;
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
const SERVER_SOURCE_VERSION: Option<&str> = option_env!("OMNIGRAPH_SOURCE_VERSION");

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Server topology + the graphs to open at startup. Single-mode
    /// invocations (`omnigraph-server <URI>` or `--target <name>`)
    /// produce `ServerConfigMode::Single`; multi-mode invocations
    /// (`--config omnigraph.yaml` with a non-empty `graphs:` map and
    /// no single-mode selector) produce `ServerConfigMode::Multi`.
    pub mode: ServerConfigMode,
    pub bind: String,
    /// Operator opt-in for fully-unauthenticated dev mode (MR-723).
    /// When neither bearer tokens nor a policy file are configured,
    /// `serve()` refuses to start unless this is true (set via
    /// `--unauthenticated` or `OMNIGRAPH_UNAUTHENTICATED=1`). The
    /// motivation is that "no tokens + no policy" looks like protection
    /// (no Cedar errors at boot) but is actually fully open — operators
    /// who set up auth and forgot the policy file would otherwise ship
    /// the illusion of protection.
    pub allow_unauthenticated: bool,
}

/// What `load_server_settings` produces after applying the four-rule
/// mode inference matrix (MR-668 decision 2).
#[derive(Debug, Clone)]
pub enum ServerConfigMode {
    /// Legacy invocation — one graph at the given URI. Either:
    ///   * `omnigraph-server <URI>` (CLI positional), or
    ///   * `omnigraph-server --target <name> --config omnigraph.yaml`, or
    ///   * `omnigraph-server --config omnigraph.yaml` with `server.graph`
    ///     set to a named target.
    Single {
        uri: String,
        /// Cedar graph resource id for the single graph. A named selection
        /// uses the graph name; an anonymous URI uses the normalized URI to
        /// preserve legacy single-graph policy identity.
        graph_id: String,
        /// Top-level `policy.file` (single-graph Cedar policy).
        policy_file: Option<PathBuf>,
        /// Top-level stored-query registry, loaded and identity-checked
        /// at settings-build time; type-checked against the schema when
        /// the engine opens.
        queries: QueryRegistry,
    },
    /// Multi-graph invocation — `--config omnigraph.yaml` with a
    /// non-empty `graphs:` map and no single-mode selector.
    Multi {
        /// Per-graph startup configs, sorted by graph id (BTreeMap
        /// iteration order). The parallel-open loop iterates this.
        graphs: Vec<GraphStartupConfig>,
        /// Path to the config file the server was started from. Kept on
        /// the mode so future runtime mutation (deferred — see release
        /// notes) can locate the source of truth without re-parsing CLI
        /// args.
        config_path: PathBuf,
        /// `server.policy.file` (server-level Cedar policy for the
        /// management endpoints). Wired into `GET /graphs` authorization.
        server_policy_file: Option<PathBuf>,
    },
}

/// One graph's startup-time configuration: id, opened URI, optional
/// per-graph policy file path. Constructed by `load_server_settings`
/// in multi mode; consumed by `serve`'s parallel open loop.
#[derive(Debug, Clone)]
pub struct GraphStartupConfig {
    pub graph_id: String,
    pub uri: String,
    pub policy_file: Option<PathBuf>,
    /// Per-graph stored-query registry, loaded and identity-checked at
    /// settings-build time; type-checked against the schema when this
    /// graph's engine opens.
    pub queries: QueryRegistry,
}

/// Runtime routing for the server. Single mode = legacy
/// `omnigraph-server <URI>` invocation, one graph, flat HTTP routes.
/// Multi mode = `--config omnigraph.yaml` with a non-empty `graphs:`
/// map, N graphs, cluster routes (`/graphs/{graph_id}/...`). Mode is
/// determined at startup by `load_server_settings`.
///
/// In single mode the handle lives here directly — there is no
/// registry, no sentinel key, no walk-and-assert. In multi mode the
/// registry carries N handles and the middleware dispatches on the
/// URL's `{graph_id}` segment.
///
/// Both modes share the same handler bodies — the routing middleware
/// (`resolve_graph_handle`) injects `Arc<GraphHandle>` as a request
/// extension so handlers never see the routing discriminator.
#[derive(Clone)]
pub enum GraphRouting {
    /// Single-graph deployment: one handle, flat routes (`/snapshot`,
    /// `/read`, …). The `handle.uri` field carries the URI the engine
    /// was opened from. Backward compatible with v0.6.0 deployments.
    Single { handle: Arc<GraphHandle> },
    /// Multi-graph deployment: many handles, cluster routes
    /// (`/graphs/{graph_id}/...`). `config_path` is the `omnigraph.yaml`
    /// the server reads at startup; preserved here so future runtime
    /// mutation (deferred) can find the source of truth without
    /// re-parsing CLI args. The server treats the file as
    /// operator-owned and never writes it.
    Multi {
        registry: Arc<GraphRegistry>,
        config_path: Option<PathBuf>,
    },
}

#[derive(Clone)]
pub struct AppState {
    /// Runtime routing — the single source of truth for where each
    /// request's graph lives. Single mode holds the handle directly;
    /// multi mode holds the registry + config path. Both arms are
    /// the same shape from a handler's perspective: middleware
    /// extracts an `Arc<GraphHandle>` and injects it as a request
    /// extension.
    routing: GraphRouting,
    /// Per-actor admission control. Process-wide (not per-graph) —
    /// see MR-668 decision Q6.
    workload: Arc<workload::WorkloadController>,
    bearer_tokens: Arc<[(BearerTokenHash, Arc<str>)]>,
    /// Server-level Cedar policy. Used by management endpoints (`POST
    /// /graphs`, `GET /graphs`) which act on the registry resource,
    /// not on a per-graph resource. Loaded from `server.policy.file`
    /// in `omnigraph.yaml`. `None` outside multi mode and when no
    /// server policy is configured. Per-graph policies live on each
    /// `GraphHandle.policy`.
    server_policy: Option<Arc<PolicyEngine>>,
}

struct ExportStreamWriter {
    sender: mpsc::UnboundedSender<std::result::Result<Bytes, io::Error>>,
}

impl Write for ExportStreamWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.sender
            .send(Ok(Bytes::copy_from_slice(buf)))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "export stream closed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: ErrorCode,
    message: String,
    merge_conflicts: Vec<api::MergeConflictOutput>,
    manifest_conflict: Option<api::ManifestConflictOutput>,
}

impl AppState {
    /// Canonical single-mode constructor. Every other `new_*` / `open_*`
    /// helper is a thin convenience wrapper around this one. Builds the
    /// engine + per-graph policy through `build_single_mode`, which
    /// applies `Omnigraph::with_policy` so HTTP-layer and engine-layer
    /// policy can never diverge — there is no "policy installed on HTTP
    /// but not on engine" representable state (closes the prior
    /// `with_policy_engine` footgun that reused the engine `Arc`
    /// without re-applying `with_policy`).
    pub fn new_single(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Vec<(String, String)>,
        policy_engine: Option<PolicyEngine>,
        workload: workload::WorkloadController,
    ) -> Self {
        let bearer_tokens = hash_bearer_tokens(bearer_tokens);
        let per_graph_policy = policy_engine.map(Arc::new);
        Self::build_single_mode(
            uri,
            db,
            bearer_tokens,
            per_graph_policy,
            Arc::new(workload),
            None,
        )
    }

    /// Like `new_single`, but attaches a pre-validated stored-query
    /// registry. Private — the production single-mode boot path
    /// (`open_single_with_queries`) is the only caller; every public
    /// `new_*` constructor builds with no stored queries.
    fn new_single_with_queries(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Vec<(String, String)>,
        policy_engine: Option<PolicyEngine>,
        workload: workload::WorkloadController,
        queries: Option<Arc<QueryRegistry>>,
    ) -> Self {
        let bearer_tokens = hash_bearer_tokens(bearer_tokens);
        let per_graph_policy = policy_engine.map(Arc::new);
        Self::build_single_mode(
            uri,
            db,
            bearer_tokens,
            per_graph_policy,
            Arc::new(workload),
            queries,
        )
    }

    pub fn new(uri: String, db: Omnigraph) -> Self {
        Self::new_single(
            uri,
            db,
            Vec::new(),
            None,
            workload::WorkloadController::from_env(),
        )
    }

    pub fn new_with_bearer_token(uri: String, db: Omnigraph, bearer_token: Option<String>) -> Self {
        let bearer_tokens = normalize_bearer_token(bearer_token)
            .into_iter()
            .map(|token| ("default".to_string(), token))
            .collect();
        Self::new_with_bearer_tokens(uri, db, bearer_tokens)
    }

    pub fn new_with_bearer_tokens(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Vec<(String, String)>,
    ) -> Self {
        Self::new_single(
            uri,
            db,
            bearer_tokens,
            None,
            workload::WorkloadController::from_env(),
        )
    }

    pub fn new_with_bearer_tokens_and_policy(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Vec<(String, String)>,
        policy_engine: Option<PolicyEngine>,
    ) -> Self {
        Self::new_single(
            uri,
            db,
            bearer_tokens,
            policy_engine,
            workload::WorkloadController::from_env(),
        )
    }

    /// Construct with a caller-provided [`workload::WorkloadController`].
    /// Tests and benches use this to override per-actor caps without
    /// mutating global env vars (unsafe in Rust 2024 once the async
    /// runtime is up — `setenv` isn't thread-safe). For tests that also
    /// need a custom `PolicyEngine`, use [`new_single`] directly.
    pub fn new_with_workload(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Vec<(String, String)>,
        workload: workload::WorkloadController,
    ) -> Self {
        Self::new_single(uri, db, bearer_tokens, None, workload)
    }

    pub async fn open(uri: impl Into<String>) -> Result<Self> {
        Self::open_with_bearer_token(uri, None).await
    }

    pub async fn open_with_bearer_token(
        uri: impl Into<String>,
        bearer_token: Option<String>,
    ) -> Result<Self> {
        let bearer_tokens = normalize_bearer_token(bearer_token)
            .into_iter()
            .map(|token| ("default".to_string(), token))
            .collect();
        Self::open_with_bearer_tokens(uri, bearer_tokens).await
    }

    pub async fn open_with_bearer_tokens(
        uri: impl Into<String>,
        bearer_tokens: Vec<(String, String)>,
    ) -> Result<Self> {
        let uri = normalize_root_uri(&uri.into()).wrap_err("normalize graph URI")?;
        let db = Omnigraph::open(&uri).await?;
        Ok(Self::new_with_bearer_tokens(uri, db, bearer_tokens))
    }

    pub async fn open_with_bearer_tokens_and_policy(
        uri: impl Into<String>,
        bearer_tokens: Vec<(String, String)>,
        policy_file: Option<&PathBuf>,
    ) -> Result<Self> {
        Self::open_single_with_queries(uri, bearer_tokens, policy_file, QueryRegistry::default())
            .await
    }

    /// Single-mode boot with a stored-query registry: open the engine,
    /// **type-check the registry against the live schema and refuse to
    /// start on a breakage** (same posture as bad policy YAML), log
    /// non-blocking warnings, then attach the registry to the handle.
    /// With an empty registry the check is a no-op and no registry is
    /// attached — that is the path `open_with_bearer_tokens_and_policy`
    /// (no stored queries) takes.
    pub async fn open_single_with_queries(
        uri: impl Into<String>,
        bearer_tokens: Vec<(String, String)>,
        policy_file: Option<&PathBuf>,
        queries: QueryRegistry,
    ) -> Result<Self> {
        Self::open_single_with_queries_for_graph_id(uri, bearer_tokens, policy_file, queries, None)
            .await
    }

    async fn open_single_with_queries_for_graph_id(
        uri: impl Into<String>,
        bearer_tokens: Vec<(String, String)>,
        policy_file: Option<&PathBuf>,
        queries: QueryRegistry,
        graph_id: Option<String>,
    ) -> Result<Self> {
        // The "policy requires tokens" invariant is enforced once by
        // `classify_server_runtime_state` in `serve()`, before either
        // single-mode or multi-mode construction is reached. By the
        // time we get here, the (policy, no-tokens) combination has
        // already been rejected — no second bail needed.
        let uri = normalize_root_uri(&uri.into()).wrap_err("normalize graph URI")?;
        let graph_id = graph_id.unwrap_or_else(|| uri.clone());
        let db = Omnigraph::open(&uri).await?;

        // Validate the registry against the live schema and resolve it to
        // an attachable handle (refuse boot on breakage).
        let registry = validate_and_attach(queries, &db.catalog(), &graph_id)?;

        let policy_engine = match policy_file {
            Some(path) => Some(PolicyEngine::load_graph(path, &graph_id)?),
            None => None,
        };
        Ok(Self::new_single_with_queries(
            uri,
            db,
            bearer_tokens,
            policy_engine,
            workload::WorkloadController::from_env(),
            registry,
        ))
    }

    /// Single-mode shared construction: wraps the bare engine + per-graph
    /// policy in a `GraphHandle` carried directly by `GraphRouting::Single`.
    /// Per-graph policy enforcement on the engine (MR-722) is re-applied
    /// via `Omnigraph::with_policy` so HTTP and engine layers can never
    /// diverge.
    fn build_single_mode(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Arc<[(BearerTokenHash, Arc<str>)]>,
        policy_engine: Option<Arc<PolicyEngine>>,
        workload: Arc<workload::WorkloadController>,
        queries: Option<Arc<QueryRegistry>>,
    ) -> Self {
        // Engine-layer policy gate (MR-722). With a per-graph policy
        // installed, every `_as` writer on `Omnigraph` calls into the
        // PolicyChecker. HTTP-layer `authorize_request` is the first
        // gate; engine-layer is the redundant-but-correct backstop.
        let db = if let Some(policy) = policy_engine.as_ref() {
            let checker = Arc::clone(policy) as Arc<dyn omnigraph_policy::PolicyChecker>;
            db.with_policy(checker)
        } else {
            db
        };
        // `GraphHandle.key` is required by the struct, but in single
        // mode it is never a registry key (there's no registry) and
        // never compared against user input (routes are flat, no
        // `{graph_id}` parameter). The label appears only in tracing
        // output from `resolve_graph_handle`. The literal below is a
        // log label, not a routing key — when the future cluster
        // catalog ships, single mode may carry the catalog-assigned
        // id here instead.
        let uri = normalize_root_uri(&uri).unwrap_or(uri);
        let key = GraphKey::cluster(
            GraphId::try_from("default").expect("'default' is a valid GraphId log label"),
        );
        let handle = Arc::new(GraphHandle {
            key,
            uri,
            engine: Arc::new(db),
            policy: policy_engine,
            queries,
        });
        Self {
            routing: GraphRouting::Single { handle },
            workload,
            bearer_tokens,
            server_policy: None,
        }
    }

    /// Multi-mode constructor — used by the startup loop. Operators
    /// reach this by invoking `omnigraph-server --config omnigraph.yaml`
    /// with a non-empty `graphs:` map.
    ///
    /// Caller supplies the already-opened `GraphHandle`s and (optionally)
    /// the path to the source config file. `server_policy` is loaded
    /// from `server.policy.file` if configured.
    pub fn new_multi(
        handles: Vec<Arc<GraphHandle>>,
        bearer_tokens: Vec<(String, String)>,
        server_policy: Option<PolicyEngine>,
        workload: workload::WorkloadController,
        config_path: Option<PathBuf>,
    ) -> std::result::Result<Self, InsertError> {
        let bearer_tokens = hash_bearer_tokens(bearer_tokens);
        let registry = Arc::new(GraphRegistry::from_handles(handles)?);
        Ok(Self {
            routing: GraphRouting::Multi {
                registry,
                config_path,
            },
            workload: Arc::new(workload),
            bearer_tokens,
            server_policy: server_policy.map(Arc::new),
        })
    }

    /// Runtime routing accessor. Handlers don't typically inspect this —
    /// they extract `Arc<GraphHandle>` via the routing middleware — but
    /// `build_app` matches on it to decide flat vs nested route
    /// mounting, and a handful of management endpoints (`GET /graphs`,
    /// the OpenAPI cluster rewrite) match on the discriminant.
    pub fn routing(&self) -> &GraphRouting {
        &self.routing
    }

    fn requires_bearer_auth(&self) -> bool {
        if !self.bearer_tokens.is_empty() {
            return true;
        }
        if self.server_policy.is_some() {
            return true;
        }
        // Any per-graph policy also requires auth — otherwise the
        // policy gate would receive unauthenticated requests. Reading
        // from `routing` is O(1) in both arms: single mode is a direct
        // `handle.policy.is_some()` check, multi mode reads the
        // cached `any_per_graph_policy` flag on the registry snapshot.
        match &self.routing {
            GraphRouting::Single { handle } => handle.policy.is_some(),
            GraphRouting::Multi { registry, .. } => registry.snapshot_ref().any_per_graph_policy,
        }
    }

    fn authenticate_bearer_token(&self, provided_token: &str) -> Option<ResolvedActor> {
        // Hash the incoming token and compare against every stored digest in
        // constant time. Iterate all entries unconditionally so total work —
        // and therefore response timing — doesn't depend on which slot matches.
        let provided_hash = hash_bearer_token(provided_token);
        let mut matched: Option<Arc<str>> = None;
        for (hash, actor) in self.bearer_tokens.iter() {
            if bool::from(hash.ct_eq(&provided_hash)) && matched.is_none() {
                matched = Some(Arc::clone(actor));
            }
        }
        matched.map(ResolvedActor::cluster_static)
    }
}

fn hash_bearer_tokens(bearer_tokens: Vec<(String, String)>) -> Arc<[(BearerTokenHash, Arc<str>)]> {
    let tokens: Vec<(BearerTokenHash, Arc<str>)> = bearer_tokens
        .into_iter()
        .map(|(actor, token)| (hash_bearer_token(&token), Arc::<str>::from(actor)))
        .collect();
    Arc::from(tokens)
}

impl ApiError {
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: ErrorCode::Unauthorized,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: ErrorCode::Forbidden,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: ErrorCode::BadRequest,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: ErrorCode::NotFound,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    /// HTTP 405 Method Not Allowed. Used when the route is mounted but
    /// the active server mode doesn't serve it (`GET /graphs` in
    /// single-graph mode returns this instead of 404 so clients can
    /// distinguish "wrong context" from "no such resource").
    pub fn method_not_allowed(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::METHOD_NOT_ALLOWED,
            code: ErrorCode::MethodNotAllowed,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: ErrorCode::Conflict,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: ErrorCode::Internal,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    /// HTTP 429 Too Many Requests — actor exceeded their per-actor
    /// admission cap (count or byte budget). Clients should respect the
    /// `Retry-After` header. Mapped from `RejectReason::InFlightCountExceeded`
    /// and `RejectReason::ByteBudgetExceeded`.
    pub fn too_many_requests(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: ErrorCode::TooManyRequests,
            message: message.into(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        }
    }

    /// Convert a `WorkloadController` rejection into the matching
    /// `ApiError` variant.
    pub fn from_workload_reject(reject: workload::RejectReason) -> Self {
        match reject {
            workload::RejectReason::InFlightCountExceeded { .. }
            | workload::RejectReason::ByteBudgetExceeded { .. } => {
                Self::too_many_requests(reject.to_string())
            }
        }
    }

    fn merge_conflict(conflicts: Vec<api::MergeConflictOutput>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: ErrorCode::Conflict,
            message: summarize_merge_conflicts(&conflicts),
            merge_conflicts: conflicts,
            manifest_conflict: None,
        }
    }

    fn manifest_version_conflict(message: String, details: api::ManifestConflictOutput) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: ErrorCode::Conflict,
            message,
            merge_conflicts: Vec::new(),
            manifest_conflict: Some(details),
        }
    }

    fn from_omni(err: OmniError) -> Self {
        match err {
            OmniError::Compiler(err) => Self::bad_request(err.to_string()),
            OmniError::DataFusion(message) => Self::bad_request(format!("query: {message}")),
            OmniError::Manifest(err) => match err.kind {
                ManifestErrorKind::BadRequest => Self::bad_request(err.message),
                ManifestErrorKind::NotFound => Self::not_found(err.message),
                ManifestErrorKind::Conflict => match err.details {
                    Some(ManifestConflictDetails::ExpectedVersionMismatch {
                        table_key,
                        expected,
                        actual,
                    }) => Self::manifest_version_conflict(
                        err.message,
                        api::ManifestConflictOutput {
                            table_key,
                            expected,
                            actual,
                        },
                    ),
                    _ => Self::conflict(err.message),
                },
                ManifestErrorKind::Internal => Self::internal(err.message),
            },
            OmniError::MergeConflicts(conflicts) => Self::merge_conflict(
                conflicts
                    .iter()
                    .map(api::MergeConflictOutput::from)
                    .collect(),
            ),
            OmniError::Lance(message) => Self::internal(format!("storage: {message}")),
            OmniError::Io(err) => Self::internal(format!("io: {err}")),
            // Engine-layer policy enforcement (MR-722). All denials and
            // evaluation failures surface here as 403. The HTTP-layer
            // `authorize_request` already distinguishes 401 (missing
            // bearer) from 403 (policy denial), so by the time the
            // engine gate fires, the bearer is valid — any failure from
            // the engine is a policy outcome, not an auth one.
            OmniError::Policy(message) => Self::forbidden(message),
            // `Omnigraph::init` against an existing graph URI in strict
            // mode. Not currently HTTP-reachable (POST /graphs was
            // pulled), but mapping is wired so the variant has a
            // single canonical translation when a future runtime
            // create endpoint lands.
            err @ OmniError::AlreadyInitialized { .. } => Self::conflict(err.to_string()),
        }
    }
}

fn summarize_merge_conflicts(conflicts: &[api::MergeConflictOutput]) -> String {
    if conflicts.is_empty() {
        return "merge conflicts".to_string();
    }

    let preview: Vec<String> = conflicts
        .iter()
        .take(3)
        .map(|conflict| match conflict.row_id.as_deref() {
            Some(row_id) => format!(
                "{}:{} ({})",
                conflict.table_key,
                row_id,
                conflict.kind.as_str()
            ),
            None => format!("{} ({})", conflict.table_key, conflict.kind.as_str()),
        })
        .collect();

    let suffix = if conflicts.len() > preview.len() {
        format!("; and {} more", conflicts.len() - preview.len())
    } else {
        String::new()
    };

    format!("merge conflicts: {}{}", preview.join("; "), suffix)
}

/// Constant `Retry-After` value (seconds) emitted on 429 responses.
const RETRY_AFTER_SECONDS: &str = "60";

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut headers = axum::http::HeaderMap::new();
        if matches!(self.code, ErrorCode::TooManyRequests) {
            headers.insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static(RETRY_AFTER_SECONDS),
            );
        }
        (
            self.status,
            headers,
            Json(ErrorOutput {
                error: self.message,
                code: Some(self.code),
                merge_conflicts: self.merge_conflicts,
                manifest_conflict: self.manifest_conflict,
            }),
        )
            .into_response()
    }
}

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Log each non-blocking advisory from a registry check report.
fn log_registry_warnings(label: &str, report: &queries::CheckReport) {
    for warning in &report.warnings {
        warn!(graph = label, query = %warning.query, "stored query: {}", warning.message);
    }
}

fn validate_registry_against_catalog(
    registry: &QueryRegistry,
    catalog: &Catalog,
    label: &str,
) -> omnigraph::error::Result<()> {
    let report = check(registry, catalog);
    if report.has_breakages() {
        return Err(OmniError::manifest(format_check_breakages(label, &report)));
    }
    log_registry_warnings(label, &report);
    Ok(())
}

/// Validate a loaded stored-query registry against the live schema and
/// resolve it to an attachable handle. Refuses boot on any breakage
/// (same posture as bad policy YAML), logs the non-blocking warnings,
/// and collapses an empty registry to `None` (nothing attached). This is
/// the single gate every open path funnels through, so no opener can
/// attach a registry that has not been schema-checked. `label` names the
/// graph in messages.
fn validate_and_attach(
    queries: QueryRegistry,
    catalog: &Catalog,
    label: &str,
) -> Result<Option<Arc<QueryRegistry>>> {
    validate_registry_against_catalog(&queries, catalog, label)
        .map_err(|err| color_eyre::eyre::eyre!(err.to_string()))?;
    Ok(if queries.is_empty() {
        None
    } else {
        Some(Arc::new(queries))
    })
}

/// Format every load error (parse / identity failure) into a multi-line
/// boot-abort message.
fn format_registry_load_errors(label: &str, errors: &[queries::LoadError]) -> String {
    let joined = errors
        .iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("\n  ");
    format!("graph '{label}': stored-query registry failed to load:\n  {joined}")
}

/// omnigraph-server serves embedded graphs only (RFC-002 §3). Reject a graph
/// whose resolved locator is remote (`server:` set, or a remote `http(s)://`
/// `uri:`) before any open — a server must not proxy another server. Reuses the
/// same `resolve_graph` classifier the CLI dispatches on, so the two can't drift.
fn ensure_embedded(
    config: &OmnigraphConfig,
    explicit_uri: Option<&str>,
    name: Option<&str>,
    label: &str,
) -> Result<()> {
    if config.resolve_graph(explicit_uri, name)?.is_remote() {
        bail!(
            "graph '{label}' is remote (it sets `server:` or a remote `http(s)://` URI), but \
             omnigraph-server serves embedded graphs only and does not proxy another server. \
             Serve an embedded `storage:` graph, or point a client at the remote server instead."
        );
    }
    Ok(())
}

pub fn load_server_settings(
    config_path: Option<&PathBuf>,
    cli_uri: Option<String>,
    cli_target: Option<String>,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
) -> Result<ServerConfig> {
    let config = load_config(config_path)?;
    let bind = cli_bind.unwrap_or_else(|| config.server_bind().to_string());
    // Either `--unauthenticated` or `OMNIGRAPH_UNAUTHENTICATED=1` flips
    // this. Treat any non-empty, non-"0"/"false" string as truthy —
    // standard 12-factor "any value is true" reading of the env var.
    let env_unauth = std::env::var("OMNIGRAPH_UNAUTHENTICATED")
        .ok()
        .map(|v| {
            let trimmed = v.trim();
            !trimmed.is_empty() && trimmed != "0" && !trimmed.eq_ignore_ascii_case("false")
        })
        .unwrap_or(false);
    let allow_unauthenticated = cli_allow_unauthenticated || env_unauth;

    // MR-668 decision 2 — four-rule mode inference matrix.
    //
    //   1. CLI `<URI>` positional        → Single (URI = the value)
    //   2. CLI `--target <name>`         → Single (URI = graphs.<name>.uri)
    //   3. `server.graph` in config      → Single (URI = graphs.<server.graph>.uri)
    //   4. `--config` + non-empty `graphs:` + no single-mode selector
    //                                    → Multi (every entry in `graphs:`)
    //   5. otherwise                     → error with migration hint
    //
    // Rules 1-3 are mutually compatible (CLI URI wins over `--target`
    // wins over `server.graph`), reusing the existing
    // `resolve_target_uri` precedence.
    let has_cli_uri = cli_uri.is_some();
    let has_cli_target = cli_target.is_some();
    let has_server_graph = config.server_graph_name().is_some();
    let has_graphs_map = !config.graphs.is_empty();
    let has_explicit_config = config_path.is_some();

    let mode = if has_cli_uri || has_cli_target || has_server_graph {
        // Rules 1, 2, or 3 → Single mode.
        // Config follows graph IDENTITY, not mode: a bare URI is anonymous
        // (top-level config); a graph chosen by name uses its per-graph
        // `graphs.<name>.{policy,queries}`. Resolved before the URI so the
        // embedded-only check and `resolve_target_uri` share one selection.
        let selected: Option<&str> = if has_cli_uri {
            None
        } else {
            cli_target.as_deref().or_else(|| config.server_graph_name())
        };
        // omnigraph-server serves embedded graphs only — refuse a remote target
        // (`server:`/remote `uri:`) before resolving or opening it.
        ensure_embedded(
            &config,
            cli_uri.as_deref(),
            selected,
            selected.or(cli_uri.as_deref()).unwrap_or("<graph>"),
        )?;
        let raw_uri = config.resolve_target_uri(
            cli_uri,
            cli_target.as_deref(),
            config.server_graph_name(),
        )?;
        let uri = normalize_root_uri(&raw_uri).wrap_err_with(|| {
            format!("normalize single-graph URI '{raw_uri}' from server settings")
        })?;
        // A named selection must not leave a populated top-level block
        // silently unused — refuse boot and point at the per-graph block. The
        // same rule the CLI selection gate enforces, shared via one helper so
        // the boot check and `omnigraph queries validate`/`list` can't drift.
        config.ensure_top_level_blocks_honored(selected)?;
        // Load + identity-check now (no engine needed); the schema
        // type-check happens when the engine opens.
        let policy_file = config.resolve_policy_file_for(selected);
        let queries = QueryRegistry::load(&config, config.query_entries_for(selected))
            .map_err(|errs| color_eyre::eyre::eyre!(format_registry_load_errors(&uri, &errs)))?;
        let graph_id = graph_resource_id_for_selection(selected, &uri);
        ServerConfigMode::Single {
            uri,
            graph_id,
            policy_file,
            queries,
        }
    } else if has_explicit_config && has_graphs_map {
        // Multi mode: every graph uses its per-graph block; top-level
        // policy/queries are never honored, so a populated one is an error.
        let unhonored = config.populated_top_level_blocks();
        if !unhonored.is_empty() {
            bail!(
                "multi-graph mode: top-level {} {} not honored — each graph uses its own \
                 `graphs.<graph_id>.…` block. Move per-graph rules there (and any \
                 `graph_list` policy to `server.policy.file`).",
                unhonored.join(" and "),
                if unhonored.len() == 1 { "is" } else { "are" },
            );
        }
        // Rule 4 → Multi mode. Build a startup config per graph.
        let mut graphs = Vec::with_capacity(config.graphs.len());
        for (name, target) in &config.graphs {
            // Validate the graph id can construct a `GraphId` newtype.
            // Doing this here (not at registry insert) so a malformed
            // omnigraph.yaml fails at startup with a clear error.
            GraphId::try_from(name.clone()).map_err(|err| {
                color_eyre::eyre::eyre!("invalid graph id '{name}' in omnigraph.yaml: {err}")
            })?;
            // omnigraph-server serves embedded graphs only — a remote entry
            // (`server:`/remote `uri:`) cannot be served, only proxied.
            ensure_embedded(&config, None, Some(name.as_str()), name)?;
            let raw_uri = config.resolve_uri_value(&target.uri);
            let uri = normalize_root_uri(&raw_uri).wrap_err_with(|| {
                format!("normalize URI '{raw_uri}' for graph '{name}' in omnigraph.yaml")
            })?;
            // Per-graph `queries:`, selected through the shared
            // `query_entries_for` so server and CLI resolve identically.
            // Load + identity-check now; the schema type-check happens
            // when this graph's engine opens.
            let queries =
                QueryRegistry::load(&config, config.query_entries_for(Some(name.as_str())))
                    .map_err(|errs| {
                        color_eyre::eyre::eyre!(format_registry_load_errors(name, &errs))
                    })?;
            graphs.push(GraphStartupConfig {
                graph_id: name.clone(),
                uri,
                policy_file: config.resolve_target_policy_file(name),
                queries,
            });
        }
        let config_path = config_path
            .cloned()
            .expect("has_explicit_config implies config_path is Some");
        let server_policy_file = config.resolve_server_policy_file();
        ServerConfigMode::Multi {
            graphs,
            config_path,
            server_policy_file,
        }
    } else {
        // Rule 5 → error with migration hint.
        bail!(
            "no graph to serve: pass a URI (`omnigraph-server <URI>`), select a target \
             (`--target <name> --config omnigraph.yaml`), set `server.graph: <name>` in \
             omnigraph.yaml, or for multi-graph mode add a `graphs:` map to the config \
             file referenced by `--config`."
        );
    };

    Ok(ServerConfig {
        mode,
        bind,
        allow_unauthenticated,
    })
}

/// Whether the loaded config will run the server in multi-graph mode.
/// Useful for the test that constructs `ServerConfig` directly.
pub fn server_config_is_multi(config: &ServerConfig) -> bool {
    matches!(config.mode, ServerConfigMode::Multi { .. })
}

/// MR-723 server runtime state, classified from the three-state matrix
/// of (bearer tokens configured) × (policy file configured) at startup.
///
/// * **Open** — neither tokens nor policy; requires explicit
///   `allow_unauthenticated`. Effectively a "trust the network" dev
///   mode. `serve()` refuses to start in this shape without the flag,
///   so the only way to reach this state at runtime is via deliberate
///   operator opt-in.
/// * **DefaultDeny** — tokens configured but no policy file. The
///   server requires a valid bearer token; once authenticated, every
///   action except `Read` is denied with 403. Closes the "tokens but
///   forgot the policy file" trap.
/// * **PolicyEnabled** — policy file configured and at least one
///   bearer token configured. Cedar evaluates every authenticated
///   request. Policy without tokens is rejected at startup —
///   such a server would 401 every request, which is bug-shaped
///   rather than feature-shaped (operators wanting "deny all
///   unauthenticated traffic" should configure tokens plus a
///   deny-all policy to get meaningful 403s with policy-decision
///   logging instead).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ServerRuntimeState {
    Open,
    DefaultDeny,
    PolicyEnabled,
}

/// Compute the [`ServerRuntimeState`] from the configured inputs.
/// Pulled out as a pure function so the matrix is unit-testable
/// without standing up the full server.
///
/// The classifier is the **single source of truth** for "should we
/// start?" — both `serve()`'s single-mode and multi-mode branches
/// call this before constructing their `AppState`. Adding a startup
/// invariant here means both modes enforce it automatically; the
/// alternative (per-constructor `bail!`) drifts the moment a third
/// mode is added.
pub fn classify_server_runtime_state(
    has_tokens: bool,
    has_policy: bool,
    allow_unauthenticated: bool,
) -> Result<ServerRuntimeState> {
    match (has_tokens, has_policy, allow_unauthenticated) {
        (false, false, false) => bail!(
            "server has no bearer tokens and no policy file configured. This is a fully \
             open server — pass `--unauthenticated` (or set OMNIGRAPH_UNAUTHENTICATED=1) \
             if you actually want that, otherwise configure bearer tokens (see \
             docs/user/server.md) and/or `policy.file` in omnigraph.yaml."
        ),
        (false, false, true) => Ok(ServerRuntimeState::Open),
        (true, false, _) => Ok(ServerRuntimeState::DefaultDeny),
        (false, true, _) => bail!(
            "policy file is configured but no bearer tokens — every request would 401 \
             because no token can ever match. Configure at least one bearer token (see \
             docs/user/server.md), or remove the policy file. To deny all unauthenticated \
             traffic deliberately, configure tokens plus a deny-all Cedar rule — that \
             produces meaningful 403s with policy-decision logging instead of silent 401s."
        ),
        (true, true, _) => Ok(ServerRuntimeState::PolicyEnabled),
    }
}

pub fn build_app(state: AppState) -> Router {
    // The per-graph protected routes, identical in single + multi mode.
    // Two middleware layers wrap them (outer first, inner last):
    //   1. `require_bearer_auth` — extracts the bearer token and injects
    //      `ResolvedActor` (or rejects 401).
    //   2. `resolve_graph_handle` — injects `Arc<GraphHandle>` based on
    //      the active mode (single: the only handle; multi: lookup by
    //      `{graph_id}` in the URI path).
    let per_graph_protected = Router::new()
        .route("/snapshot", get(server_snapshot))
        .route("/export", post(server_export))
        // /read and /change are kept indefinitely for back-compat;
        // their handlers carry #[deprecated] so the OpenAPI operation is
        // flagged and their responses include RFC 9745 Deprecation +
        // RFC 8288 Link headers. Suppress the call-site warning for the
        // route registration itself.
        .route(
            "/read",
            post({
                #[allow(deprecated)]
                server_read
            }),
        )
        .route("/query", post(server_query))
        .route(
            "/change",
            post({
                #[allow(deprecated)]
                server_change
            }),
        )
        .route("/mutate", post(server_mutate))
        .route("/queries", get(server_list_queries))
        .route("/queries/{name}", post(server_invoke_query))
        .route("/schema", get(server_schema_get))
        .route("/schema/apply", post(server_schema_apply))
        .route(
            "/ingest",
            post(server_ingest).layer(DefaultBodyLimit::max(INGEST_REQUEST_BODY_LIMIT_BYTES)),
        )
        .route(
            "/branches",
            get(server_branch_list).post(server_branch_create),
        )
        .route("/branches/{branch}", delete(server_branch_delete))
        .route("/branches/merge", post(server_branch_merge))
        .route("/commits", get(server_commit_list))
        .route("/commits/{commit_id}", get(server_commit_show))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            resolve_graph_handle,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_auth,
        ));

    // Management endpoints (`GET /graphs`) live alongside the per-graph
    // router. They go through bearer auth but NOT through
    // `resolve_graph_handle` — they operate on the registry directly.
    // The endpoint is mounted in both modes; in single mode the handler
    // returns 405 so clients see "resource exists, wrong context"
    // rather than 404 "no such resource."
    //
    // Runtime add/remove (`POST /graphs`, `DELETE /graphs/{id}`) is not
    // exposed in v0.6.0 — operators add graphs by editing
    // `omnigraph.yaml` and restarting.
    let management = Router::new()
        .route("/graphs", get(server_graphs_list))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_auth,
        ));

    // Mount the protected routes differently per mode:
    //   * Single → flat routes (legacy: `/snapshot`, `/read`, etc.)
    //   * Multi  → nested under `/graphs/{graph_id}/...`
    let protected: Router<AppState> = match state.routing() {
        GraphRouting::Single { .. } => per_graph_protected.merge(management),
        GraphRouting::Multi { .. } => Router::new()
            .nest("/graphs/{graph_id}", per_graph_protected)
            .merge(management),
    };

    Router::new()
        .route("/healthz", get(server_health))
        .route("/openapi.json", get(server_openapi))
        .merge(protected)
        .layer(DefaultBodyLimit::max(DEFAULT_REQUEST_BODY_LIMIT_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn serve(config: ServerConfig) -> Result<()> {
    let token_source = resolve_token_source().await?;
    info!(source = token_source.name(), "loaded bearer token source");
    let tokens = token_source.load().await?;

    // For runtime-state classification, "any policy configured" means
    // either the top-level/single-mode policy file OR a server-level
    // policy OR any per-graph policy file. Mirrors the
    // `requires_bearer_auth` semantics on AppState.
    let has_policy_configured = match &config.mode {
        ServerConfigMode::Single { policy_file, .. } => policy_file.is_some(),
        ServerConfigMode::Multi {
            graphs,
            server_policy_file,
            ..
        } => server_policy_file.is_some() || graphs.iter().any(|g| g.policy_file.is_some()),
    };
    let runtime_state = classify_server_runtime_state(
        !tokens.is_empty(),
        has_policy_configured,
        config.allow_unauthenticated,
    )?;
    match runtime_state {
        ServerRuntimeState::Open => warn!(
            "running with --unauthenticated: no bearer tokens, no policy file, all \
             requests permitted. This is for local dev only — do not expose to a \
             network you don't fully trust."
        ),
        ServerRuntimeState::DefaultDeny => warn!(
            "bearer tokens are configured but no policy file is set — running in \
             default-deny mode (only `read` actions are permitted for authenticated \
             actors). Configure `policy.file` in omnigraph.yaml to enable Cedar rules."
        ),
        ServerRuntimeState::PolicyEnabled => {}
    }

    let bind = config.bind.clone();
    let state = match config.mode {
        ServerConfigMode::Single {
            uri,
            graph_id,
            policy_file,
            queries,
        } => {
            let uri_for_log = uri.clone();
            info!(
                uri = %uri_for_log,
                graph_id = %graph_id,
                bind = %bind,
                mode = "single",
                "serving omnigraph"
            );
            AppState::open_single_with_queries_for_graph_id(
                uri,
                tokens,
                policy_file.as_ref(),
                queries,
                Some(graph_id),
            )
            .await?
        }
        ServerConfigMode::Multi {
            graphs,
            config_path,
            server_policy_file,
        } => {
            info!(
                bind = %bind,
                mode = "multi",
                graph_count = graphs.len(),
                config = %config_path.display(),
                "serving omnigraph"
            );
            open_multi_graph_state(graphs, tokens, server_policy_file.as_ref(), config_path).await?
        }
    };

    let listener = TcpListener::bind(&bind).await?;
    axum::serve(listener, build_app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Parallel open of every graph in the startup config, with bounded
/// concurrency (`buffer_unordered(4)`). Fail-fast — the first open error
/// aborts startup; other in-flight opens are dropped (their `Omnigraph`
/// instances close cleanly via Arc drop).
///
/// The bound 4 is a rule-of-thumb for I/O-bound work. At N ≤ 10 this
/// trades startup latency for a small amount of concurrent S3 / Lance
/// open pressure.
async fn open_multi_graph_state(
    graphs: Vec<GraphStartupConfig>,
    tokens: Vec<(String, String)>,
    server_policy_file: Option<&PathBuf>,
    config_path: PathBuf,
) -> Result<AppState> {
    use futures::{StreamExt, TryStreamExt};

    if graphs.is_empty() {
        bail!("multi-graph mode requires at least one graph in the `graphs:` map");
    }

    // Server-level policy (loaded once, applies to management endpoints).
    // The placeholder graph_id `"server"` is the sentinel the Cedar
    // resource-model refactor maps to the singleton
    // `Omnigraph::Server::"root"` entity at evaluation time.
    let server_policy = match server_policy_file {
        Some(path) => Some(PolicyEngine::load_server(path)?),
        None => None,
    };

    // `try_collect` propagates the first error eagerly, dropping every
    // in-flight open. `buffer_unordered + collect::<Vec<_>>` would drain
    // the stream before checking errors — incorrect for the docstring's
    // "fail-fast" claim and wasteful on S3-backed graphs.
    let handles: Vec<Arc<GraphHandle>> = futures::stream::iter(graphs.into_iter())
        .map(|cfg| async move { open_single_graph(cfg).await })
        .buffer_unordered(4)
        .try_collect()
        .await?;

    let workload = workload::WorkloadController::from_env();
    let state = AppState::new_multi(handles, tokens, server_policy, workload, Some(config_path))
        .map_err(|err| color_eyre::eyre::eyre!("multi-graph registry: {err}"))?;
    Ok(state)
}

/// Open one graph and wrap it in a `GraphHandle`. Used at startup by
/// `open_multi_graph_state`.
async fn open_single_graph(cfg: GraphStartupConfig) -> Result<Arc<GraphHandle>> {
    let graph_id = GraphId::try_from(cfg.graph_id.clone())
        .map_err(|err| color_eyre::eyre::eyre!("graph id '{}': {err}", cfg.graph_id))?;
    let uri = normalize_root_uri(&cfg.uri)
        .wrap_err_with(|| format!("normalize URI for graph '{}'", cfg.graph_id))?;

    let db = Omnigraph::open(&uri)
        .await
        .map_err(|err| color_eyre::eyre::eyre!("open graph '{}' at {}: {err}", graph_id, uri))?;

    // Validate this graph's stored queries against the live schema and
    // resolve them to an attachable handle (refuse boot on breakage).
    // Done before the policy match rebinds `db`; the catalog handle is an
    // owned `Arc`, so no borrow of `db` survives into the match.
    let queries = validate_and_attach(cfg.queries, &db.catalog(), graph_id.as_str())?;

    let (policy_arc, db) = match &cfg.policy_file {
        Some(path) => {
            let policy = PolicyEngine::load_graph(path, graph_id.as_str())?;
            let policy_arc: Arc<PolicyEngine> = Arc::new(policy);
            let checker = Arc::clone(&policy_arc) as Arc<dyn omnigraph_policy::PolicyChecker>;
            (Some(policy_arc), db.with_policy(checker))
        }
        None => (None, db),
    };

    Ok(Arc::new(GraphHandle {
        key: GraphKey::cluster(graph_id),
        uri,
        engine: Arc::new(db),
        policy: policy_arc,
        queries,
    }))
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        error!(error = %err, "failed to install ctrl-c handler");
        return;
    }
    info!("shutdown signal received");
}

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "health",
    operation_id = "health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthOutput),
    ),
)]
/// Liveness probe.
///
/// Returns server status and version. Unauthenticated; safe to call from any
/// caller. Use this to confirm the server is reachable before invoking other
/// endpoints.
async fn server_health() -> Json<HealthOutput> {
    Json(HealthOutput {
        status: "ok".to_string(),
        version: SERVER_VERSION.to_string(),
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
async fn server_graphs_list(
    State(state): State<AppState>,
    actor: Option<Extension<ResolvedActor>>,
) -> std::result::Result<Json<GraphListResponse>, ApiError> {
    // 405 in single mode — there's no registry to enumerate, and the
    // legacy URL surface didn't expose this endpoint.
    let registry = match state.routing() {
        GraphRouting::Single { .. } => {
            return Err(ApiError::method_not_allowed(
                "GET /graphs is only available in multi-graph mode",
            ));
        }
        GraphRouting::Multi { registry, .. } => registry,
    };

    // Server-level Cedar gate. `state.server_policy` is loaded from
    // `server.policy.file` in `omnigraph.yaml` at startup. When no
    // server policy is configured, `authorize_request_server` falls
    // through to the MR-723 default-deny semantics (every non-Read
    // action denied for an authenticated actor). `GraphList` is not
    // `Read`, so without a server policy the request gets 403 — which
    // is the right default (don't leak the registry until the operator
    // explicitly authorizes it).
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

async fn server_openapi(State(state): State<AppState>) -> Json<utoipa::openapi::OpenApi> {
    let mut doc = ApiDoc::openapi();
    if !state.requires_bearer_auth() {
        strip_security(&mut doc);
    }
    // MR-668: in multi mode, the protected routes live under
    // `/graphs/{graph_id}/...`. Rewrite the doc so the spec matches
    // the routes the router actually serves. Public paths (`/healthz`)
    // stay flat in both modes.
    if matches!(state.routing(), GraphRouting::Multi { .. }) {
        nest_paths_under_cluster_prefix(&mut doc);
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
fn nest_paths_under_cluster_prefix(doc: &mut utoipa::openapi::OpenApi) {
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

fn add_cluster_graph_id_parameter(item: &mut utoipa::openapi::PathItem) {
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

fn graph_id_path_parameter() -> Parameter {
    let mut parameter = Parameter::new("graph_id");
    parameter.parameter_in = ParameterIn::Path;
    parameter.description = Some("Graph id to route the request to.".to_string());
    parameter.schema = Some(Object::with_type(Type::String).into());
    parameter
}

/// Prefix every operation_id in this PathItem with `prefix`.
fn rename_operation_ids(item: &mut utoipa::openapi::PathItem, prefix: &str) {
    for op in path_item_operations_mut(item) {
        if let Some(id) = op.operation_id.as_deref() {
            op.operation_id = Some(format!("{prefix}{id}"));
        }
    }
}

fn path_item_operations_mut(
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

fn strip_security(doc: &mut utoipa::openapi::OpenApi) {
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

async fn require_bearer_auth(
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

/// Routing middleware (MR-668). Resolves the active graph for the
/// request and injects `Arc<GraphHandle>` as an extension so handlers can
/// extract it via `Extension<Arc<GraphHandle>>`.
///
/// **Single mode**: the routing field holds the single handle directly.
/// Routes are flat; every request resolves to that handle, regardless
/// of the URI path. No registry walk, no sentinel key, no
/// programmer-error guard.
///
/// **Multi mode**: routes are nested under `/graphs/{graph_id}/...`. The
/// middleware extracts `{graph_id}` from the URI path and looks it up in
/// the registry. Returns 404 if the graph is not registered.
///
/// The middleware fires AFTER `require_bearer_auth`, so the actor is
/// already in the request extensions (or auth was off entirely).
async fn resolve_graph_handle(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> std::result::Result<Response, ApiError> {
    let handle = match &state.routing {
        GraphRouting::Single { handle } => Arc::clone(handle),
        GraphRouting::Multi { registry, .. } => {
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
                    ApiError::bad_request(
                        "cluster route missing /graphs/{graph_id} prefix".to_string(),
                    )
                })?;
            let graph_id = GraphId::try_from(graph_id_str.to_string())
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
            let key = GraphKey::cluster(graph_id.clone());
            match registry.get(&key) {
                RegistryLookup::Ready(handle) => handle,
                RegistryLookup::Gone => {
                    return Err(ApiError::not_found(format!("graph '{graph_id}' not found")));
                }
            }
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

fn log_policy_decision(actor_id: &str, request: &PolicyRequest, decision: &PolicyDecision) {
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
enum Authz {
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
fn authorize(
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
        // operator's only path to enabling it is configuring an
        // explicit `server.policy.file` in omnigraph.yaml.
        if request.action.resource_kind() == PolicyResourceKind::Server {
            return Ok(Authz::Denied(
                "server-scoped actions require an explicit `server.policy.file` \
                 configured in omnigraph.yaml — the management surface is closed \
                 by default in every runtime state, including --unauthenticated, \
                 so that server topology is never exposed without operator opt-in."
                    .to_string(),
            ));
        }
        if actor.is_some() && request.action != PolicyAction::Read {
            return Ok(Authz::Denied(
                "server runs in default-deny mode (bearer tokens configured but no \
                 policy file). Only `read` actions are permitted; configure \
                 `policy.file` in omnigraph.yaml to enable other actions."
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
fn authorize_request(
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
async fn server_snapshot(
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
    let snapshot = {
        let db = &handle.engine;
        db.snapshot_of(ReadTarget::branch(branch.as_str()))
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(snapshot_payload(&branch, &snapshot)))
}

/// Header values that flag a response as coming from a deprecated route
/// (RFC 9745 / RFC 8288) and point at the canonical successor.
fn deprecation_headers(successor_link: &'static str) -> [(HeaderName, HeaderValue); 2] {
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
        (status = 200, description = "Query results (response includes `Deprecation: true` + `Link: </query>; rel=\"successor-version\"`)", body = ReadOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
#[deprecated(
    note = "use POST /query instead; /read is kept indefinitely for byte-stable back-compat"
)]
/// **Deprecated** — use [`POST /query`](#tag/queries/operation/query) instead.
///
/// Execute a GQ read query. Behavior is unchanged from prior releases; the
/// route is kept indefinitely for byte-stable back-compat. New integrations
/// should target `POST /query`, which has clean field names (`query` /
/// `name`) and a 400-on-mutation guard. Responses from this route include
/// `Deprecation: true` and `Link: </query>; rel="successor-version"`
/// headers per RFC 9745 / RFC 8288 so SDKs and proxies can surface the
/// signal.
async fn server_read(
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
        deprecation_headers("</query>; rel=\"successor-version\""),
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
async fn server_query(
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
async fn server_export(
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
async fn run_mutate(
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
async fn run_query(
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
        (status = 200, description = "Mutation results (response includes `Deprecation: true` + `Link: </mutate>; rel=\"successor-version\"`)", body = ChangeOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 409, description = "Merge conflict", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
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
/// `Deprecation: true` and `Link: </mutate>; rel="successor-version"`
/// headers per RFC 9745 / RFC 8288 so SDKs and proxies can surface the
/// signal.
async fn server_change(
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
        deprecation_headers("</mutate>; rel=\"successor-version\""),
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
        (status = 409, description = "Merge conflict", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Apply a GQ mutation to a branch (canonical mutation endpoint).
///
/// Writes to the named `branch` (defaults to `main`). Mutations are atomic
/// per call and produce a new commit. Returns counts of nodes and edges
/// affected. **Destructive**: on success the branch is updated; rejected
/// mutations may still acquire locks briefly. Returns 409 on merge conflict.
///
/// Pairs with `POST /query` (read-only). The legacy `POST /change` route
/// has identical semantics and is kept as a deprecated alias.
async fn server_mutate(
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
struct QueryNamePath {
    name: String,
}

fn parse_optional_invoke_body(
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
        (status = 409, description = "Merge conflict", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
        (status = 500, description = "Policy evaluation error (a denial is reported as 404, not 500)", body = ErrorOutput),
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
async fn server_invoke_query(
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
        (status = 200, description = "Stored-query catalog (the mcp.expose subset, with typed params)", body = QueriesCatalogOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// List the graph's exposed stored queries as a typed tool catalog.
///
/// Returns the `mcp.expose == true` subset of the `queries:` registry, each
/// with its MCP tool name, read/mutate flag, description/instruction, and
/// typed parameters — enough for a client to register them as tools without
/// fetching `.gq` source. Read-gated; the catalog is graph-wide (branch
/// independent — `read` is authorized against `main`). **Not** Cedar-filtered
/// per query yet, so it can list a query whose `invoke_query` the caller
/// lacks (a known gap until per-query authorization lands).
async fn server_list_queries(
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
async fn server_schema_get(
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
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Apply a schema migration.
///
/// Diffs `schema_source` against the current schema and applies the resulting
/// migration steps (add/drop type, add/drop column, etc.). **Destructive**:
/// some steps drop data. Returns the list of steps applied; if `applied` is
/// false the diff was unsupported and no changes were made.
async fn server_schema_apply(
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
    Ok(Json(schema_apply_output(handle.uri.as_str(), result)))
}

#[utoipa::path(
    post,
    path = "/ingest",
    tag = "mutations",
    operation_id = "ingest",
    request_body = IngestRequest,
    responses(
        (status = 200, description = "Ingest results", body = IngestOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Bulk-ingest NDJSON data into a branch.
///
/// `data` is NDJSON with one record per line. `mode` controls behavior on
/// existing rows: `merge` upserts by id (default), `append` blindly inserts,
/// `overwrite` replaces table contents. If `branch` does not exist it is
/// created from `from` (defaults to `main`). **Destructive** when `mode` is
/// `overwrite` or when ingest produces conflicting writes.
async fn server_ingest(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<IngestRequest>,
) -> std::result::Result<Json<IngestOutput>, ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    let from = request.from.unwrap_or_else(|| "main".to_string());
    let mode = request.mode.unwrap_or(omnigraph::loader::LoadMode::Merge);
    let actor_arc = actor
        .as_ref()
        .map(|Extension(actor)| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor
        .as_ref()
        .map(|Extension(actor)| actor.actor_id.as_ref());

    let branch_exists = {
        let db = &handle.engine;
        db.branch_list()
            .await
            .map_err(ApiError::from_omni)?
            .into_iter()
            .any(|name| name == branch)
    };

    if !branch_exists {
        authorize_request(
            actor.as_ref().map(|Extension(actor)| actor),
            handle.policy.as_deref(),
            PolicyRequest {
                action: PolicyAction::BranchCreate,
                branch: Some(from.clone()),
                target_branch: Some(branch.clone()),
            },
        )?;
    }
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
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
        db.ingest_as(&branch, Some(&from), &request.data, mode, actor_id)
            .await
            .map_err(ApiError::from_omni)?
    };

    Ok(Json(ingest_output(
        handle.uri.as_str(),
        &result,
        actor_id.map(str::to_string),
    )))
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
async fn server_branch_list(
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
    ),
    security(("bearer_token" = [])),
)]
/// Create a new branch.
///
/// Forks `name` off of `from` (defaults to `main`). The new branch shares
/// table data with its parent until it is mutated. Returns 409 if `name`
/// already exists.
async fn server_branch_create(
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
struct BranchPath {
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
    ),
    security(("bearer_token" = [])),
)]
/// Delete a branch.
///
/// **Irreversible.** Removes the branch pointer; commits remain reachable
/// only if referenced by another branch. Returns 404 if the branch does not
/// exist.
async fn server_branch_delete(
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
        (status = 429, description = "Per-actor admission cap exceeded; honor `Retry-After` header", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Merge one branch into another.
///
/// Merges `source` into `target` (defaults to `main`). Outcome is one of
/// `already_up_to_date`, `fast_forward`, or `merged`. Returns 409 with the
/// list of conflicts if the merge cannot be completed; the target is left
/// unchanged in that case. **Destructive** to `target` on success.
async fn server_branch_merge(
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
    Ok(Json(BranchMergeOutput {
        source: request.source,
        target,
        outcome: outcome.into(),
        actor_id: actor_id.map(str::to_string),
    }))
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
async fn server_commit_list(
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
struct CommitPath {
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
async fn server_commit_show(
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

fn read_target_from_request(branch: Option<String>, snapshot: Option<String>) -> ReadTarget {
    if let Some(snapshot) = snapshot {
        ReadTarget::snapshot(omnigraph::db::SnapshotId::new(snapshot))
    } else {
        ReadTarget::branch(branch.unwrap_or_else(|| "main".to_string()))
    }
}

fn select_named_query_decl(
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

fn select_named_query(
    query_source: &str,
    requested_name: Option<&str>,
) -> Result<(String, Vec<omnigraph_compiler::query::ast::Param>)> {
    let query = select_named_query_decl(query_source, requested_name)?;
    Ok((query.name, query.params))
}

fn query_params_from_json(
    query_params: &[omnigraph_compiler::query::ast::Param],
    params_json: Option<&Value>,
) -> Result<ParamMap> {
    json_params_to_param_map(params_json, query_params, JsonParamMode::Standard)
        .map_err(|err| color_eyre::eyre::eyre!(err.to_string()))
}

fn normalize_bearer_token(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_bearer_actor(value: String) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        bail!("bearer token actor names must not be blank");
    }
    Ok(value)
}

fn parse_bearer_tokens_json(value: &str) -> Result<Vec<(String, String)>> {
    let entries: HashMap<String, String> = serde_json::from_str(value)
        .wrap_err("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON must be a JSON object of actor->token")?;
    Ok(entries.into_iter().collect())
}

fn read_bearer_tokens_file(path: &str) -> Result<Vec<(String, String)>> {
    let contents = fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read bearer tokens file at {path}"))?;
    parse_bearer_tokens_json(&contents)
        .wrap_err_with(|| format!("failed to parse bearer tokens file at {path}"))
}

fn validate_bearer_tokens(entries: Vec<(String, String)>) -> Result<Vec<(String, String)>> {
    let mut seen_actors = HashSet::new();
    let mut seen_tokens = HashSet::new();
    let mut normalized = Vec::with_capacity(entries.len());

    for (actor, token) in entries {
        let actor = normalize_bearer_actor(actor)?;
        let Some(token) = normalize_bearer_token(Some(token)) else {
            bail!("bearer token for actor '{actor}' must not be blank");
        };
        if !seen_actors.insert(actor.clone()) {
            bail!("duplicate bearer token actor '{actor}'");
        }
        if !seen_tokens.insert(token.clone()) {
            bail!("duplicate bearer token value configured");
        }
        normalized.push((actor, token));
    }

    normalized.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(normalized)
}

fn server_bearer_tokens_from_env() -> Result<Vec<(String, String)>> {
    let mut entries = Vec::new();

    if let Some(token) = normalize_bearer_token(std::env::var("OMNIGRAPH_SERVER_BEARER_TOKEN").ok())
    {
        entries.push(("default".to_string(), token));
    }

    if let Some(path) =
        normalize_bearer_token(std::env::var("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE").ok())
    {
        entries.extend(read_bearer_tokens_file(&path)?);
    } else if let Some(json) =
        normalize_bearer_token(std::env::var("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON").ok())
    {
        entries.extend(parse_bearer_tokens_json(&json)?);
    }

    validate_bearer_tokens(entries)
}

#[cfg(test)]
mod tests {
    use super::{
        GraphStartupConfig, ServerConfig, ServerConfigMode, ServerRuntimeState,
        classify_server_runtime_state, hash_bearer_token, load_server_settings,
        normalize_bearer_token, parse_bearer_tokens_json, serve, server_bearer_tokens_from_env,
    };
    use serial_test::serial;
    use std::env;
    use std::fs;
    use tempfile::tempdir;

    /// `authorize` returns the allow/deny **decision** (`Authz`) and reserves
    /// `Err` for operational failures, so the invoke handler can hide a denial
    /// as 404 without also masking a 401/500. Pins each outcome.
    #[test]
    fn authorize_splits_decision_from_operational_error() {
        use super::{
            Authz, PolicyAction, PolicyCompiler, PolicyConfig, PolicyRequest, ResolvedActor,
            authorize,
        };
        use std::sync::Arc;

        fn req(action: PolicyAction) -> PolicyRequest {
            PolicyRequest {
                action,
                branch: None,
                target_branch: None,
            }
        }
        let actor = ResolvedActor::cluster_static(Arc::from("act-alice"));

        // --- No policy engine installed (open / default-deny modes) ---
        // A server-scoped action is denied in every no-policy state.
        assert!(matches!(
            authorize(Some(&actor), None, req(PolicyAction::GraphList)).unwrap(),
            Authz::Denied(_)
        ));
        // Authenticated actor + a non-read per-graph action → default-deny.
        assert!(matches!(
            authorize(Some(&actor), None, req(PolicyAction::Change)).unwrap(),
            Authz::Denied(_)
        ));
        // `read` is the one per-graph action permitted without a policy.
        assert!(matches!(
            authorize(Some(&actor), None, req(PolicyAction::Read)).unwrap(),
            Authz::Allowed
        ));
        // Open mode (no actor, no policy) → allowed.
        assert!(matches!(
            authorize(None, None, req(PolicyAction::Read)).unwrap(),
            Authz::Allowed
        ));

        // --- Policy engine installed ---
        let policy: PolicyConfig = serde_yaml::from_str(
            "version: 1\n\
             groups:\n  team: [act-alice]\n\
             rules:\n  - id: team-read\n    allow:\n      actors: { group: team }\n      actions: [read]\n      branch_scope: any\n",
        )
        .unwrap();
        let engine = PolicyCompiler::compile(&policy, "graph").unwrap();

        // A matched allow rule → Allowed.
        assert!(matches!(
            authorize(
                Some(&actor),
                Some(&engine),
                PolicyRequest {
                    action: PolicyAction::Read,
                    branch: Some("main".to_string()),
                    target_branch: None
                },
            )
            .unwrap(),
            Authz::Allowed
        ));
        // Known actor, no matching allow rule → Denied, carrying the decision message.
        match authorize(
            Some(&actor),
            Some(&engine),
            PolicyRequest {
                action: PolicyAction::Change,
                branch: Some("main".to_string()),
                target_branch: None,
            },
        )
        .unwrap()
        {
            Authz::Denied(message) => {
                assert!(!message.is_empty(), "a deny carries its decision message")
            }
            Authz::Allowed => panic!("change must be denied: only read is allowed"),
        }
        // Policy installed but no actor → operational failure (`Err`), NOT a
        // decision. This is the split that keeps a 401/500 from being masked
        // as the denial's response in the invoke handler.
        assert!(
            authorize(None, Some(&engine), req(PolicyAction::Read)).is_err(),
            "a missing actor with a policy installed is an operational error, not a deny"
        );
    }

    #[test]
    fn hash_bearer_token_produces_32_byte_output() {
        let hash = hash_bearer_token("any-token");
        assert_eq!(hash.len(), 32);
    }

    /// The single gate both open paths funnel through: it refuses a
    /// schema breakage (naming the graph label + query), attaches a clean
    /// registry, and collapses an empty one to `None`. Pure over its args
    /// (no engine), so it covers the multi-graph path's logic too — the
    /// only per-path difference is the `label`, asserted here.
    #[test]
    fn validate_and_attach_gates_on_schema_and_collapses_empty() {
        use crate::queries::{QueryRegistry, RegistrySpec};
        use omnigraph_compiler::catalog::build_catalog;
        use omnigraph_compiler::schema::parser::parse_schema;

        let schema = parse_schema("node User {\nname: String\n}\n").unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let spec = |name: &str, source: &str| RegistrySpec {
            name: name.to_string(),
            source: source.to_string(),
            expose: false,
            tool_name: None,
        };

        // Empty registry → nothing attached, no error.
        let empty = super::validate_and_attach(QueryRegistry::default(), &catalog, "g").unwrap();
        assert!(empty.is_none());

        // A query that type-checks → attached.
        let ok = QueryRegistry::from_specs(vec![spec(
            "find_user",
            "query find_user() { match { $u: User } return { $u.name } }",
        )])
        .unwrap();
        assert!(
            super::validate_and_attach(ok, &catalog, "g")
                .unwrap()
                .is_some()
        );

        // A query referencing a type the schema lacks → boot refusal that
        // names both the graph label and the offending query.
        let broken = QueryRegistry::from_specs(vec![spec(
            "ghost",
            "query ghost() { match { $w: Widget } return { $w.name } }",
        )])
        .unwrap();
        let err = super::validate_and_attach(broken, &catalog, "graph-x").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("graph-x"), "labels the graph: {msg}");
        assert!(msg.contains("ghost"), "names the query: {msg}");
        assert!(
            msg.contains("schema check"),
            "mentions the schema check: {msg}"
        );
    }

    #[test]
    fn hash_bearer_token_is_deterministic() {
        assert_eq!(
            hash_bearer_token("stable-input"),
            hash_bearer_token("stable-input"),
        );
    }

    #[test]
    fn hash_bearer_token_differs_for_different_inputs() {
        assert_ne!(hash_bearer_token("token-a"), hash_bearer_token("token-b"));
    }

    #[test]
    fn hash_bearer_token_matches_known_sha256_vector() {
        // SHA-256("abc"). If this ever fails, the hash function was swapped.
        let hash = hash_bearer_token("abc");
        let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn server_settings_load_from_yaml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
graphs:
  local:
    uri: /tmp/demo.omni
server:
  graph: local
  bind: 0.0.0.0:9090
"#,
        )
        .unwrap();

        let settings = load_server_settings(Some(&config), None, None, None, false).unwrap();
        match &settings.mode {
            ServerConfigMode::Single { uri, graph_id, .. } => {
                assert_eq!(uri, "/tmp/demo.omni");
                assert_eq!(graph_id, "local");
            }
            ServerConfigMode::Multi { .. } => panic!("expected Single mode, got Multi"),
        }
        assert_eq!(settings.bind, "0.0.0.0:9090");
    }

    #[test]
    fn server_settings_cli_flags_override_yaml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
graphs:
  local:
    uri: /tmp/demo.omni
server:
  graph: local
  bind: 127.0.0.1:8080
"#,
        )
        .unwrap();

        let settings = load_server_settings(
            Some(&config),
            Some("/tmp/override.omni".to_string()),
            None,
            Some("0.0.0.0:9999".to_string()),
            false,
        )
        .unwrap();
        match &settings.mode {
            ServerConfigMode::Single { uri, graph_id, .. } => {
                assert_eq!(uri, "/tmp/override.omni");
                assert_eq!(graph_id, "/tmp/override.omni");
            }
            ServerConfigMode::Multi { .. } => panic!("expected Single mode, got Multi"),
        }
        assert_eq!(settings.bind, "0.0.0.0:9999");
    }

    #[test]
    fn server_settings_can_resolve_named_target() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
graphs:
  local:
    uri: ./local.omni
  dev:
    uri: ./dev.omni
server:
  graph: local
  bind: 127.0.0.1:8080
"#,
        )
        .unwrap();

        // `--target dev` overrides `server.graph: local`, resolving the named
        // embedded graph. (A remote target is now rejected — see
        // `single_mode_rejects_named_remote_graph` in tests/server.rs.)
        let settings =
            load_server_settings(Some(&config), None, Some("dev".to_string()), None, false)
                .unwrap();
        match &settings.mode {
            ServerConfigMode::Single { uri, graph_id, .. } => {
                assert!(uri.ends_with("dev.omni"), "uri: {uri}");
                assert_eq!(graph_id, "dev");
            }
            ServerConfigMode::Multi { .. } => panic!("expected Single mode, got Multi"),
        }
    }

    #[test]
    fn server_settings_require_uri_from_cli_or_config() {
        let error = load_server_settings(None, None, None, None, false).unwrap_err();
        assert!(
            error.to_string().contains("no graph to serve"),
            "expected mode-inference error, got: {error}",
        );
    }

    #[test]
    fn classify_open_requires_explicit_unauthenticated_flag() {
        // State 1: no tokens, no policy, no flag → refuse to start.
        let error = classify_server_runtime_state(false, false, false).unwrap_err();
        let msg = error.to_string();
        assert!(
            msg.contains("--unauthenticated"),
            "expected refusal message mentioning --unauthenticated, got: {msg}"
        );

        // Same matrix cell but with the flag set → Open mode permitted.
        assert_eq!(
            classify_server_runtime_state(false, false, true).unwrap(),
            ServerRuntimeState::Open
        );
    }

    #[test]
    fn classify_tokens_without_policy_is_default_deny() {
        // State 2: tokens configured, no policy → DefaultDeny regardless
        // of the flag (the flag opts into the fully-open dev mode; it
        // doesn't downgrade default-deny back to open).
        assert_eq!(
            classify_server_runtime_state(true, false, false).unwrap(),
            ServerRuntimeState::DefaultDeny
        );
        assert_eq!(
            classify_server_runtime_state(true, false, true).unwrap(),
            ServerRuntimeState::DefaultDeny
        );
    }

    #[tokio::test]
    #[serial]
    async fn serve_refuses_to_start_with_policy_but_no_tokens_multi_mode() {
        // Bug 2 from the bot-review pass: multi-mode startup was missing
        // the "policy requires tokens" check that single-mode enforces.
        // After centralizing the check in `classify_server_runtime_state`,
        // both modes get the same enforcement. This test guards the
        // multi-mode propagation path.
        //
        // Sibling test below pins single mode. Together they pin that
        // the classifier is called from both branches of `serve()`.
        let _guard = EnvGuard::set(&[
            ("OMNIGRAPH_SERVER_BEARER_TOKEN", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET", None),
            ("OMNIGRAPH_UNAUTHENTICATED", None),
        ]);
        let temp = tempdir().unwrap();
        // The classifier reads `has_policy_configured` from the config
        // shape (does the Option contain a path?), not from file
        // existence, so we can hand it a path without writing a real
        // policy file — the bail fires before policy load.
        let policy_path = temp.path().join("server-policy.yaml");
        let config = ServerConfig {
            mode: ServerConfigMode::Multi {
                graphs: vec![GraphStartupConfig {
                    graph_id: "alpha".to_string(),
                    uri: temp
                        .path()
                        .join("alpha.omni")
                        .to_string_lossy()
                        .into_owned(),
                    policy_file: None,
                    queries: crate::queries::QueryRegistry::default(),
                }],
                config_path: temp.path().join("omnigraph.yaml"),
                server_policy_file: Some(policy_path),
            },
            bind: "127.0.0.1:0".to_string(),
            allow_unauthenticated: false,
        };
        let result = serve(config).await;
        let err = result
            .expect_err("serve should refuse to start in multi mode with policy but no tokens");
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("policy file is configured but no bearer tokens"),
            "expected policy-without-tokens rejection in multi mode, got: {msg}",
        );
    }

    #[tokio::test]
    #[serial]
    async fn serve_refuses_to_start_in_state_1_without_unauthenticated() {
        // MR-723 PR A: pin the integration boundary that the classifier
        // is actually called by `serve()` before any side-effecting
        // work (Lance dataset open, TcpListener::bind). The classifier
        // itself is unit-tested above; this test guards the propagation
        // path from `classify_server_runtime_state` through serve's
        // `?` so a future refactor that drops the call returns red.
        //
        // Marked `#[serial]` because we have to clear all bearer-token
        // env vars, and another test in this module setting any of them
        // concurrently would corrupt the read inside `resolve_token_source`.
        let _guard = EnvGuard::set(&[
            ("OMNIGRAPH_SERVER_BEARER_TOKEN", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET", None),
            ("OMNIGRAPH_UNAUTHENTICATED", None),
        ]);
        let temp = tempdir().unwrap();
        // Graph path doesn't need to exist — classifier fires before
        // `AppState::open_with_bearer_tokens_and_policy`.
        let config = ServerConfig {
            mode: ServerConfigMode::Single {
                uri: temp
                    .path()
                    .join("graph.omni")
                    .to_string_lossy()
                    .into_owned(),
                graph_id: "default".to_string(),
                policy_file: None,
                queries: crate::queries::QueryRegistry::default(),
            },
            bind: "127.0.0.1:0".to_string(),
            allow_unauthenticated: false,
        };
        let result = serve(config).await;
        let err =
            result.expect_err("serve should refuse to start in State 1 without --unauthenticated");
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("no bearer tokens") || msg.contains("policy file"),
            "expected refusal message naming the misconfiguration, got: {msg}",
        );
    }

    #[test]
    #[serial]
    fn unauthenticated_env_var_classification() {
        // MR-723 PR A: closes the gap where the env-var read path inside
        // `load_server_settings` was structurally implemented but not
        // exercised by any test. Three properties to pin, all in one
        // sequential test because `cargo test` runs the mod test suite
        // in parallel and `OMNIGRAPH_UNAUTHENTICATED` is process-global
        // — interleaving with another test that sets the same env var
        // (concurrent classifier tests, even the bearer-token suite
        // sharing `EnvGuard`) corrupts the read. Sequential within one
        // test fn is the simplest race-free shape.
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  local:
    uri: /tmp/demo-unauth.omni
server:
  graph: local
"#,
        )
        .unwrap();

        // Truthy values flip Open mode on, even with CLI flag off.
        for value in ["1", "true", "yes", "TRUE", "anything"] {
            let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", Some(value))]);
            let settings = load_server_settings(Some(&config_path), None, None, None, false)
                .expect("settings load should succeed");
            assert!(
                settings.allow_unauthenticated,
                "OMNIGRAPH_UNAUTHENTICATED={value:?} should enable Open mode",
            );
        }

        // Falsy values keep refusal behavior, even with CLI flag off.
        for value in ["0", "false", "FALSE", ""] {
            let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", Some(value))]);
            let settings = load_server_settings(Some(&config_path), None, None, None, false)
                .expect("settings load should succeed");
            assert!(
                !settings.allow_unauthenticated,
                "OMNIGRAPH_UNAUTHENTICATED={value:?} should NOT enable Open mode",
            );
        }

        // Unset env var: also false.
        let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", None)]);
        let settings = load_server_settings(Some(&config_path), None, None, None, false)
            .expect("settings load should succeed");
        assert!(
            !settings.allow_unauthenticated,
            "OMNIGRAPH_UNAUTHENTICATED unset should NOT enable Open mode",
        );
        drop(_guard);

        // CLI flag wins even when env is falsy — `serve()` honors the
        // OR of both inputs.
        let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", Some("0"))]);
        let settings = load_server_settings(Some(&config_path), None, None, None, true)
            .expect("settings load should succeed");
        assert!(
            settings.allow_unauthenticated,
            "--unauthenticated CLI flag should win even when env is falsy",
        );
    }

    #[test]
    fn classify_policy_enabled_requires_tokens() {
        // State 3: tokens + policy → PolicyEnabled, regardless of the
        // `allow_unauthenticated` flag (Cedar evaluates the bearer,
        // the flag is moot once tokens exist).
        assert_eq!(
            classify_server_runtime_state(true, true, false).unwrap(),
            ServerRuntimeState::PolicyEnabled
        );
        assert_eq!(
            classify_server_runtime_state(true, true, true).unwrap(),
            ServerRuntimeState::PolicyEnabled
        );
    }

    #[test]
    fn classify_policy_without_tokens_is_rejected() {
        // Closes the "policy installed but no tokens → silent 401 on
        // every request" footgun. The same shape that single-mode
        // `open_with_bearer_tokens_and_policy` used to bail on
        // privately is now rejected by the classifier so both single
        // and multi mode get the same enforcement from one source of
        // truth.
        for allow_unauthenticated in [false, true] {
            let err =
                classify_server_runtime_state(false, true, allow_unauthenticated).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("policy file is configured but no bearer tokens"),
                "expected policy-without-tokens rejection message; got: {msg}"
            );
            assert!(
                msg.contains("every request would 401"),
                "rejection message must name the failure mode; got: {msg}"
            );
        }
    }

    #[test]
    fn normalize_bearer_token_trims_and_filters_blank_values() {
        assert_eq!(normalize_bearer_token(None), None);
        assert_eq!(normalize_bearer_token(Some("   ".to_string())), None);
        assert_eq!(
            normalize_bearer_token(Some(" demo-token ".to_string())).as_deref(),
            Some("demo-token")
        );
    }

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
            let saved = vars
                .iter()
                .map(|(name, _)| (*name, env::var(name).ok()))
                .collect::<Vec<_>>();
            for (name, value) in vars {
                unsafe {
                    match value {
                        Some(value) => env::set_var(name, value),
                        None => env::remove_var(name),
                    }
                }
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.drain(..) {
                unsafe {
                    match value {
                        Some(value) => env::set_var(name, value),
                        None => env::remove_var(name),
                    }
                }
            }
        }
    }

    #[test]
    fn parse_bearer_tokens_json_reads_actor_token_map() {
        let tokens = parse_bearer_tokens_json(r#"{"alice":" token-a ","bob":"token-b"}"#).unwrap();
        assert_eq!(tokens.len(), 2);
        assert!(tokens.contains(&("alice".to_string(), " token-a ".to_string())));
        assert!(tokens.contains(&("bob".to_string(), "token-b".to_string())));
    }

    #[test]
    #[serial]
    fn server_bearer_tokens_from_env_reads_legacy_token_and_token_file() {
        let temp = tempdir().unwrap();
        let tokens_path = temp.path().join("tokens.json");
        fs::write(
            &tokens_path,
            r#"{"team-01":"token-one","team-02":"token-two"}"#,
        )
        .unwrap();

        let _guard = EnvGuard::set(&[
            ("OMNIGRAPH_SERVER_BEARER_TOKEN", Some(" legacy-token ")),
            (
                "OMNIGRAPH_SERVER_BEARER_TOKENS_FILE",
                Some(tokens_path.to_str().unwrap()),
            ),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", None),
        ]);

        let tokens = server_bearer_tokens_from_env().unwrap();
        assert_eq!(
            tokens,
            vec![
                ("default".to_string(), "legacy-token".to_string()),
                ("team-01".to_string(), "token-one".to_string()),
                ("team-02".to_string(), "token-two".to_string()),
            ]
        );
    }
}
