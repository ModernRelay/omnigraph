pub mod api;
mod handlers;
mod settings;
pub use settings::{load_server_settings, classify_server_runtime_state, server_config_is_multi, ServerRuntimeState};
use settings::*;
use handlers::*;
pub mod auth;
pub mod config;
pub mod graph_id;
pub mod identity;
pub mod policy;
pub mod queries;
pub mod registry;
pub mod workload;

pub use graph_id::GraphId;
pub use identity::{AuthSource, GraphKey, ResolvedActor, Scope, TenantId};
pub use registry::{GraphHandle, GraphRegistry, InsertError, RegistryLookup, RegistrySnapshot};

use crate::queries::{QueryRegistry, check, format_check_breakages};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use api::{
    BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput, BranchListOutput,
    BranchMergeOutput, BranchMergeRequest, ChangeOutput, ChangeRequest, CommitListOutput,
    CommitListQuery, ErrorCode, ErrorOutput, ExportRequest, GraphInfo, GraphListResponse,
    HealthOutput, IngestOutput, IngestRequest, InvokeStoredQueryRequest,
    InvokeStoredQueryResponse, QueriesCatalogOutput, QueryRequest, ReadOutput, ReadRequest,
    SchemaApplyOutput, SchemaApplyRequest, SchemaOutput, SnapshotQuery, ingest_output,
    schema_apply_output, snapshot_payload,
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
use color_eyre::eyre::{Result, WrapErr, bail, eyre};
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
        handlers::server_health,
        handlers::server_graphs_list,
        handlers::server_snapshot,
        // deprecated; the #[deprecated] attribute on the handler
        // surfaces as `deprecated: true` on the OpenAPI operation.
        #[allow(deprecated)] handlers::server_read,
        handlers::server_query,
        handlers::server_export,
        #[allow(deprecated)] handlers::server_change,
        handlers::server_mutate,
        handlers::server_list_queries,
        handlers::server_invoke_query,
        handlers::server_schema_apply,
        handlers::server_schema_get,
        handlers::server_load,
        // deprecated; the #[deprecated] attribute on the handler surfaces as
        // `deprecated: true` on the OpenAPI operation.
        #[allow(deprecated)] handlers::server_ingest,
        handlers::server_branch_list,
        handlers::server_branch_create,
        handlers::server_branch_delete,
        handlers::server_branch_merge,
        handlers::server_commit_list,
        handlers::server_commit_show,
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
        server_policy: Option<PolicySource>,
    },
}

/// Where a Cedar policy bundle comes from at startup. File-based for
/// omnigraph.yaml deployments; inline (digest-verified catalog content)
/// for cluster-mode boots, where the catalog may live on object storage
/// and the server must not re-read mutable state after the snapshot.
#[derive(Debug, Clone)]
pub enum PolicySource {
    File(PathBuf),
    Inline(String),
}

/// One graph's startup-time configuration: id, opened URI, optional
/// per-graph policy source. Constructed by `load_server_settings`
/// in multi mode; consumed by `serve`'s parallel open loop.
#[derive(Debug, Clone)]
pub struct GraphStartupConfig {
    pub graph_id: String,
    pub uri: String,
    pub policy: Option<PolicySource>,
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
        Self::build_single_mode(uri, db, bearer_tokens, per_graph_policy, Arc::new(workload), None)
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
        Self::open_single_with_queries(
            uri,
            bearer_tokens,
            policy_file,
            QueryRegistry::default(),
        )
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
        .route("/read", post({
            #[allow(deprecated)]
            server_read
        }))
        .route("/query", post(server_query))
        .route("/change", post({
            #[allow(deprecated)]
            server_change
        }))
        .route("/mutate", post(server_mutate))
        .route("/queries", get(server_list_queries))
        .route("/queries/{name}", post(server_invoke_query))
        .route("/schema", get(server_schema_get))
        .route("/schema/apply", post(server_schema_apply))
        .route(
            "/load",
            post(server_load).layer(DefaultBodyLimit::max(INGEST_REQUEST_BODY_LIMIT_BYTES)),
        )
        // /ingest is the deprecated alias of /load; its handler carries
        // #[deprecated] (OpenAPI operation flagged) and emits RFC 9745
        // Deprecation + RFC 8288 Link headers. Suppress the call-site warning.
        .route(
            "/ingest",
            post({
                #[allow(deprecated)]
                server_ingest
            })
            .layer(DefaultBodyLimit::max(INGEST_REQUEST_BODY_LIMIT_BYTES)),
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
            server_policy,
            ..
        } => server_policy.is_some() || graphs.iter().any(|g| g.policy.is_some()),
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
            server_policy,
        } => {
            info!(
                bind = %bind,
                mode = "multi",
                graph_count = graphs.len(),
                config = %config_path.display(),
                "serving omnigraph"
            );
            open_multi_graph_state(graphs, tokens, server_policy.as_ref(), config_path).await?
        }
    };

    let listener = TcpListener::bind(&bind).await?;
    axum::serve(listener, build_app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Load a graph-scoped policy bundle from either source kind.
fn load_graph_policy(source: &PolicySource, graph_id: &str) -> Result<PolicyEngine> {
    match source {
        PolicySource::File(path) => Ok(PolicyEngine::load_graph(path, graph_id)?),
        PolicySource::Inline(text) => Ok(PolicyEngine::load_graph_from_source(text, graph_id)?),
    }
}

/// Parallel open of every graph in the startup config, with bounded
/// concurrency (`buffer_unordered(4)`). Fail-fast — the first open error
/// aborts startup; other in-flight opens are dropped (their `Omnigraph`
/// instances close cleanly via Arc drop).
///
/// The bound 4 is a rule-of-thumb for I/O-bound work. At N ≤ 10 this
/// trades startup latency for a small amount of concurrent S3 / Lance
/// open pressure.
pub async fn open_multi_graph_state(
    graphs: Vec<GraphStartupConfig>,
    tokens: Vec<(String, String)>,
    server_policy_source: Option<&PolicySource>,
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
    let server_policy = match server_policy_source {
        Some(PolicySource::File(path)) => Some(PolicyEngine::load_server(path)?),
        Some(PolicySource::Inline(source)) => {
            Some(PolicyEngine::load_server_from_source(source)?)
        }
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

    let (policy_arc, db) = match &cfg.policy {
        Some(source) => {
            let policy = load_graph_policy(source, graph_id.as_str())?;
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


