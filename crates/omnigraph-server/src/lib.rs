pub mod api;
pub mod auth;
pub mod config;
pub mod graph_id;
pub mod identity;
pub mod policy;
pub mod registry;
pub mod workload;

pub use graph_id::GraphId;
pub use identity::{AuthSource, GraphKey, ResolvedActor, Scope, TenantId};
pub use registry::{GraphHandle, GraphRegistry, InsertError, RegistryLookup, RegistrySnapshot};

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
    HealthOutput, IngestOutput, IngestRequest, ReadOutput, ReadRequest, SchemaApplyOutput,
    SchemaApplyRequest, SchemaOutput, SnapshotQuery, ingest_output, schema_apply_output,
    snapshot_payload,
};
pub use auth::{AWS_SECRET_ENV, EnvOrFileTokenSource, TokenSource, resolve_token_source};
use axum::body::{Body, Bytes};
use axum::extract::DefaultBodyLimit;
use axum::extract::{Extension, OriginalUri, Path, Query, Request, State};
use axum::handler::Handler;
use axum::http::StatusCode;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use color_eyre::eyre::{Result, WrapErr, bail};
pub use config::{
    AliasCommand, AliasConfig, CliDefaults, DEFAULT_CONFIG_FILE, OmnigraphConfig, PolicySettings,
    ProjectConfig, QueryDefaults, ReadOutputFormat, ServerDefaults, TableCellLayout, TargetConfig,
    load_config,
};
use futures::stream;
use omnigraph::db::{Omnigraph, ReadTarget};
use omnigraph::error::{ManifestConflictDetails, ManifestErrorKind, OmniError};
use omnigraph_compiler::json_params_to_param_map;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::{JsonParamMode, ParamMap};
pub use policy::{
    PolicyAction, PolicyCompiler, PolicyConfig, PolicyDecision, PolicyEngine, PolicyExpectation,
    PolicyRequest, PolicyTestConfig,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use utoipa::OpenApi;
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
        server_read,
        server_export,
        server_change,
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
        /// Top-level `policy.file` (single-graph Cedar policy).
        policy_file: Option<PathBuf>,
    },
    /// Multi-graph invocation — `--config omnigraph.yaml` with a
    /// non-empty `graphs:` map and no single-mode selector.
    Multi {
        /// Per-graph startup configs, sorted by graph id (BTreeMap
        /// iteration order). PR 5's parallel-open loop iterates this.
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
}

/// Server runtime topology. Single mode = legacy `omnigraph-server <URI>`
/// invocation, one graph, flat HTTP routes. Multi mode = `--config
/// omnigraph.yaml` with a `graphs:` map, N graphs, cluster routes
/// (`/graphs/{graph_id}/...`). Mode is determined at startup by
/// `load_server_settings`.
///
/// Both modes share the same handler bodies — the routing middleware
/// (`resolve_graph_handle`) injects `Arc<GraphHandle>` as a request
/// extension so handlers never see the mode discriminator directly.
#[derive(Clone, Debug)]
pub enum ServerMode {
    /// Single-graph invocation. The `uri` is the only graph's URI.
    /// Backward compatible with v0.6.0 deployments.
    Single { uri: String },
    /// Multi-graph invocation (MR-668). `config_path` is the
    /// `omnigraph.yaml` the server reads at startup. The server treats
    /// the file as operator-owned and never writes it; runtime
    /// add/remove (deferred) is the only path that would touch it.
    Multi { config_path: Option<PathBuf> },
}

/// Sentinel `GraphId` for single-graph mode. The single graph is
/// registered under this key in the registry; the routing middleware
/// uses it to inject the handle on every flat-route request. The
/// value is operator-invisible — it does NOT need to be unique with
/// any user-facing graph id since single-mode never serves cluster
/// routes.
pub(crate) const SINGLE_GRAPH_KEY_ID: &str = "default";

#[derive(Clone)]
pub struct AppState {
    /// Topology + (single mode only) the single graph's URI for
    /// startup wiring. The registry below is the runtime source of truth.
    mode: ServerMode,
    /// PR 2 (MR-686) + PR 4a (MR-668): the engine and per-graph policy
    /// now live inside `GraphHandle`s in the registry. Reads via
    /// `ArcSwap` are lock-free; mutations (currently only `insert`)
    /// serialize through the registry's internal mutex.
    registry: Arc<GraphRegistry>,
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
    pub fn new(uri: String, db: Omnigraph) -> Self {
        Self::new_with_bearer_tokens(uri, db, Vec::new())
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
        Self::new_with_bearer_tokens_and_policy(uri, db, bearer_tokens, None)
    }

    pub fn new_with_bearer_tokens_and_policy(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Vec<(String, String)>,
        policy_engine: Option<PolicyEngine>,
    ) -> Self {
        let bearer_tokens = hash_bearer_tokens(bearer_tokens);
        let per_graph_policy: Option<Arc<PolicyEngine>> = policy_engine.map(Arc::new);
        let workload = Arc::new(workload::WorkloadController::from_env());
        Self::build_single_mode(uri, db, bearer_tokens, per_graph_policy, workload)
    }

    /// Construct with a caller-provided [`workload::WorkloadController`].
    /// Tests and benches use this to override per-actor caps without
    /// mutating global env vars (which is unsafe in Rust 2024 once the
    /// async runtime is up — `setenv` isn't thread-safe).
    pub fn new_with_workload(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Vec<(String, String)>,
        workload: workload::WorkloadController,
    ) -> Self {
        let bearer_tokens = hash_bearer_tokens(bearer_tokens);
        Self::build_single_mode(uri, db, bearer_tokens, None, Arc::new(workload))
    }

    /// Install a `PolicyEngine` post-construction (MR-723). Used by
    /// integration tests that need to thread custom workload limits
    /// alongside a permit-all policy — the existing `new_with_*` and
    /// `new_with_workload` constructors don't compose. Production
    /// callers should use `open_with_bearer_tokens_and_policy` which
    /// also installs the policy on the engine.
    ///
    /// PR 4a: rebuilds the single-mode handle with the policy attached
    /// on the HTTP-layer (`handle.policy`). The engine inside the handle
    /// is reused as-is — engine-layer policy enforcement is NOT
    /// reinstalled by this path (the old single-field `policy_engine`
    /// API had the same semantics — engine-layer enforcement was only
    /// applied by constructors that took `policy_engine: Option<...>`
    /// at build time). Tests that depend on engine-layer enforcement
    /// should use `open_with_bearer_tokens_and_policy` instead.
    pub fn with_policy_engine(self, engine: PolicyEngine) -> Self {
        let policy_arc: Arc<PolicyEngine> = Arc::new(engine);
        let existing = single_mode_handle(&self.registry)
            .expect("with_policy_engine called on a non-single-mode AppState");
        let new_handle = Arc::new(GraphHandle {
            key: existing.key.clone(),
            uri: existing.uri.clone(),
            engine: Arc::clone(&existing.engine),
            policy: Some(policy_arc),
        });
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![new_handle])
                .expect("rebuilt single-mode registry must accept one handle"),
        );
        Self {
            mode: self.mode,
            registry,
            workload: self.workload,
            bearer_tokens: self.bearer_tokens,
            server_policy: self.server_policy,
        }
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
        let uri = uri.into();
        let db = Omnigraph::open(&uri).await?;
        Ok(Self::new_with_bearer_tokens(uri, db, bearer_tokens))
    }

    pub async fn open_with_bearer_tokens_and_policy(
        uri: impl Into<String>,
        bearer_tokens: Vec<(String, String)>,
        policy_file: Option<&PathBuf>,
    ) -> Result<Self> {
        let uri = uri.into();
        let db = Omnigraph::open(&uri).await?;
        let policy_engine = match policy_file {
            Some(path) => Some(PolicyEngine::load(path, &uri)?),
            None => None,
        };
        if policy_engine.is_some() && bearer_tokens.is_empty() {
            bail!("policy requires at least one configured bearer token actor");
        }
        Ok(Self::new_with_bearer_tokens_and_policy(
            uri,
            db,
            bearer_tokens,
            policy_engine,
        ))
    }

    /// Single-mode shared construction: wraps the bare engine + per-graph
    /// policy in a `GraphHandle` registered under the sentinel
    /// `SINGLE_GRAPH_KEY_ID` key. Per-graph policy enforcement on the
    /// engine (MR-722) is re-applied via `Omnigraph::with_policy`.
    fn build_single_mode(
        uri: String,
        db: Omnigraph,
        bearer_tokens: Arc<[(BearerTokenHash, Arc<str>)]>,
        policy_engine: Option<Arc<PolicyEngine>>,
        workload: Arc<workload::WorkloadController>,
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
        let key = GraphKey::cluster(
            GraphId::try_from(SINGLE_GRAPH_KEY_ID)
                .expect("single-graph sentinel key must validate"),
        );
        let handle = Arc::new(GraphHandle {
            key,
            uri: uri.clone(),
            engine: Arc::new(db),
            policy: policy_engine,
        });
        let registry = Arc::new(
            GraphRegistry::from_handles(vec![handle])
                .expect("single-mode registry construction is infallible"),
        );
        Self {
            mode: ServerMode::Single { uri },
            registry,
            workload,
            bearer_tokens,
            server_policy: None,
        }
    }

    /// Multi-mode constructor — used by PR 5's startup loop. Operators
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
            mode: ServerMode::Multi { config_path },
            registry,
            workload: Arc::new(workload),
            bearer_tokens,
            server_policy: server_policy.map(Arc::new),
        })
    }

    /// Topology accessor. Handlers don't typically inspect this — they
    /// extract `Arc<GraphHandle>` via the routing middleware — but
    /// `build_app` does to decide flat vs nested route mounting.
    pub fn mode(&self) -> &ServerMode {
        &self.mode
    }

    /// The configured URI in single mode. Returns `None` in multi mode —
    /// each graph has its own URI on `handle.uri` instead.
    pub fn uri(&self) -> Option<&str> {
        match &self.mode {
            ServerMode::Single { uri } => Some(uri.as_str()),
            ServerMode::Multi { .. } => None,
        }
    }

    pub fn registry(&self) -> &Arc<GraphRegistry> {
        &self.registry
    }

    fn requires_bearer_auth(&self) -> bool {
        if !self.bearer_tokens.is_empty() {
            return true;
        }
        if self.server_policy.is_some() {
            return true;
        }
        // Any per-graph policy also requires auth — otherwise the
        // policy gate would receive unauthenticated requests.
        self.registry.list().iter().any(|h| h.policy.is_some())
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

fn hash_bearer_tokens(
    bearer_tokens: Vec<(String, String)>,
) -> Arc<[(BearerTokenHash, Arc<str>)]> {
    let tokens: Vec<(BearerTokenHash, Arc<str>)> = bearer_tokens
        .into_iter()
        .map(|(actor, token)| (hash_bearer_token(&token), Arc::<str>::from(actor)))
        .collect();
    Arc::from(tokens)
}

/// Look up the single-mode handle from the registry. Returns `None`
/// if the registry is empty or has multiple entries (the latter would
/// indicate a constructor bug).
fn single_mode_handle(registry: &GraphRegistry) -> Option<Arc<GraphHandle>> {
    let list = registry.list();
    if list.len() == 1 {
        list.into_iter().next()
    } else {
        None
    }
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
        let uri = config.resolve_target_uri(
            cli_uri,
            cli_target.as_deref(),
            config.server_graph_name(),
        )?;
        let policy_file = config.resolve_policy_file();
        ServerConfigMode::Single { uri, policy_file }
    } else if has_explicit_config && has_graphs_map {
        // Rule 4 → Multi mode. Build a startup config per graph.
        let mut graphs = Vec::with_capacity(config.graphs.len());
        for (name, target) in &config.graphs {
            // Validate the graph id can construct a `GraphId` newtype.
            // Doing this here (not at registry insert) so a malformed
            // omnigraph.yaml fails at startup with a clear error.
            GraphId::try_from(name.clone()).map_err(|err| {
                color_eyre::eyre::eyre!("invalid graph id '{name}' in omnigraph.yaml: {err}")
            })?;
            graphs.push(GraphStartupConfig {
                graph_id: name.clone(),
                uri: config.resolve_uri_value(&target.uri),
                policy_file: config.resolve_target_policy_file(name),
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
/// * **PolicyEnabled** — policy file configured. Cedar evaluates every
///   authenticated request. Tokens may also be configured (typical) or
///   not (unusual but valid — every request fails 401 without a
///   bearer, which is effectively "locked").
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ServerRuntimeState {
    Open,
    DefaultDeny,
    PolicyEnabled,
}

/// Compute the [`ServerRuntimeState`] from the configured inputs.
/// Pulled out as a pure function so the 3-state matrix is unit-testable
/// without standing up the full server.
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
        (_, true, _) => Ok(ServerRuntimeState::PolicyEnabled),
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
        .route("/read", post(server_read))
        .route("/change", post(server_change))
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
    // exposed in v0.7.0 — operators add graphs by editing
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
    let protected: Router<AppState> = match state.mode() {
        ServerMode::Single { .. } => per_graph_protected.merge(management),
        ServerMode::Multi { .. } => Router::new()
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
        ServerConfigMode::Single { uri, policy_file } => {
            let uri_for_log = uri.clone();
            info!(uri = %uri_for_log, bind = %bind, mode = "single", "serving omnigraph");
            AppState::open_with_bearer_tokens_and_policy(uri, tokens, policy_file.as_ref()).await?
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
            open_multi_graph_state(
                graphs,
                tokens,
                server_policy_file.as_ref(),
                config_path,
            )
            .await?
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
    use futures::StreamExt;

    if graphs.is_empty() {
        bail!("multi-graph mode requires at least one graph in the `graphs:` map");
    }

    // Server-level policy (loaded once, applies to management endpoints).
    // The placeholder graph_id `"server"` matches the PolicyEngine API
    // shape until the Cedar resource-model refactor (PR 6a) lands.
    let server_policy = match server_policy_file {
        Some(path) => Some(PolicyEngine::load(path, "server")?),
        None => None,
    };

    let handles: Vec<Arc<GraphHandle>> = futures::stream::iter(graphs.into_iter())
        .map(|cfg| async move {
            open_single_graph(cfg).await
        })
        .buffer_unordered(4)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    let workload = workload::WorkloadController::from_env();
    let state = AppState::new_multi(
        handles,
        tokens,
        server_policy,
        workload,
        Some(config_path),
    )
    .map_err(|err| color_eyre::eyre::eyre!("multi-graph registry: {err}"))?;
    Ok(state)
}

/// Open one graph and wrap it in a `GraphHandle`. Used at startup by
/// `open_multi_graph_state`.
async fn open_single_graph(cfg: GraphStartupConfig) -> Result<Arc<GraphHandle>> {
    let graph_id = GraphId::try_from(cfg.graph_id.clone())
        .map_err(|err| color_eyre::eyre::eyre!("graph id '{}': {err}", cfg.graph_id))?;

    let db = Omnigraph::open(&cfg.uri)
        .await
        .map_err(|err| color_eyre::eyre::eyre!("open graph '{}' at {}: {err}", graph_id, cfg.uri))?;

    let (policy_arc, db) = match &cfg.policy_file {
        Some(path) => {
            let policy = PolicyEngine::load(path, graph_id.as_str())?;
            let policy_arc: Arc<PolicyEngine> = Arc::new(policy);
            let checker = Arc::clone(&policy_arc) as Arc<dyn omnigraph_policy::PolicyChecker>;
            (Some(policy_arc), db.with_policy(checker))
        }
        None => (None, db),
    };

    Ok(Arc::new(GraphHandle {
        key: GraphKey::cluster(graph_id),
        uri: cfg.uri,
        engine: Arc::new(db),
        policy: policy_arc,
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
    if matches!(state.mode(), ServerMode::Single { .. }) {
        return Err(ApiError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            code: ErrorCode::BadRequest,
            message: "GET /graphs is only available in multi-graph mode".to_string(),
            merge_conflicts: Vec::new(),
            manifest_conflict: None,
        });
    }

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
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
            action: PolicyAction::GraphList,
            branch: None,
            target_branch: None,
        },
    )?;

    let mut graphs: Vec<GraphInfo> = state
        .registry()
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
    // MR-668 PR 4b: in multi mode, the protected routes live under
    // `/graphs/{graph_id}/...`. Rewrite the doc so the spec matches
    // the routes the router actually serves. Public paths (`/healthz`)
    // stay flat in both modes.
    if matches!(state.mode(), ServerMode::Multi { .. }) {
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
/// surfaces are merged. The `{graph_id}` URL placeholder is left
/// implicit in the path; consuming clients see it as a standard
/// OpenAPI path parameter.
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
        let new_path = format!("{CLUSTER_PATH_PREFIX}{path}");
        rewritten.insert(new_path, item);
    }
    doc.paths.paths = rewritten;
}

/// Prefix every operation_id in this PathItem with `prefix`.
fn rename_operation_ids(item: &mut utoipa::openapi::PathItem, prefix: &str) {
    for op in [
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
    {
        if let Some(id) = op.operation_id.as_deref() {
            op.operation_id = Some(format!("{prefix}{id}"));
        }
    }
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

/// Routing middleware (MR-668 PR 4a). Resolves the active graph for the
/// request and injects `Arc<GraphHandle>` as an extension so handlers can
/// extract it via `Extension<Arc<GraphHandle>>`.
///
/// **Single mode**: the registry has exactly one handle (keyed by the
/// `SINGLE_GRAPH_KEY_ID` sentinel). Routes are flat — every request
/// resolves to that single handle, regardless of the URI path.
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
    let handle = match &state.mode {
        ServerMode::Single { .. } => single_mode_handle(&state.registry).ok_or_else(|| {
            ApiError::internal(
                "single-mode registry is empty or has multiple handles \
                 (programmer error in AppState constructor)"
                    .to_string(),
            )
        })?,
        ServerMode::Multi { .. } => {
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
            match state.registry.get(&key) {
                RegistryLookup::Ready(handle) => handle,
                RegistryLookup::Gone => {
                    return Err(ApiError::not_found(format!(
                        "graph '{graph_id}' not found"
                    )));
                }
            }
        }
    };

    // Record the graph id on the current tracing span for per-request
    // observability. Operators correlating logs across requests reach for
    // this field; in single mode it's the sentinel `default`.
    tracing::Span::current().record("graph_id", handle.key.graph_id.as_str());

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

/// HTTP-layer Cedar policy gate. Two sources of the policy engine:
///   * Per-graph handler — passes `handle.policy.as_deref()` so the
///     graph's Cedar rules govern read/change/branch_*/schema_apply.
///   * Management handler — passes `state.server_policy.as_deref()` so
///     server-level Cedar rules govern `graph_list` (the only shipped
///     server-scoped action; runtime `graph_create` / `graph_delete`
///     are deferred until a managed cluster catalog lands).
///
/// The MR-731 invariant lives inside this function: actor identity is
/// overwritten from the resolved bearer match, never trusted from the
/// caller-built `PolicyRequest.actor_id`. See
/// `actor_id_resolves_from_bearer_token_ignoring_client_supplied_headers`
/// at `tests/server.rs:1114-1216`.
fn authorize_request(
    actor: Option<&ResolvedActor>,
    policy: Option<&PolicyEngine>,
    mut request: PolicyRequest,
) -> std::result::Result<(), ApiError> {
    let Some(engine) = policy else {
        // MR-723 default-deny path. We're here when no PolicyEngine is
        // installed. Two startup-validated shapes can reach this:
        //
        // * **Open mode** (`--unauthenticated`): no tokens, no policy.
        //   `require_bearer_auth` short-circuits before this is called,
        //   but defense in depth — if a future change makes the
        //   middleware call here for an unauthenticated request, we
        //   want every action to remain Ok rather than 403. The
        //   operator opted in.
        // * **DefaultDeny mode**: tokens configured but no policy. The
        //   request went through bearer auth, so `actor` is Some and
        //   identifies a known actor. Only `Read` is permitted; every
        //   other action returns 403. This closes the "configured auth
        //   but forgot the policy file" trap from MR-723.
        if actor.is_some() && request.action != PolicyAction::Read {
            return Err(ApiError::forbidden(
                "server runs in default-deny mode (bearer tokens configured but no \
                 policy file). Only `read` actions are permitted; configure \
                 `policy.file` in omnigraph.yaml to enable other actions.",
            ));
        }
        return Ok(());
    };
    let Some(actor) = actor else {
        return Err(ApiError::unauthorized("missing bearer token"));
    };
    // SECURITY INVARIANT (MR-731): actor identity comes from the matched
    // bearer token, never from a client-supplied request header, query
    // parameter, or body field. This line is the single chokepoint where
    // the authoritative actor (resolved from the bearer match by
    // `require_bearer_auth`) overwrites whatever the handler put in the
    // PolicyRequest. Removing or weakening it lets clients spoof identity —
    // exactly the Supabase RLS footgun ("trusting raw_user_meta_data is
    // asking the attacker if they're an admin"). The principle is codified
    // in `docs/dev/invariants.md` Hard Invariant 11 ("clients cannot set
    // actor identity directly") and pinned by the regression test
    // `actor_id_resolves_from_bearer_token_ignoring_client_supplied_headers`
    // in `crates/omnigraph-server/tests/server.rs`.
    //
    // Side effect: also prevents an empty-string default at any handler
    // call site from ever reaching the engine as a policy subject.
    request.actor_id = actor.actor_id.as_ref().to_string();
    let decision = engine
        .authorize(&request)
        .map_err(|err| ApiError::internal(format!("policy: {err}")))?;
    log_policy_decision(actor.actor_id.as_ref(), &request, &decision);
    if decision.allowed {
        Ok(())
    } else {
        Err(ApiError::forbidden(decision.message))
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
    State(_state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Query(query): Query<SnapshotQuery>,
) -> std::result::Result<Json<api::SnapshotOutput>, ApiError> {
    let branch = query.branch.unwrap_or_else(|| "main".to_string());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
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

#[utoipa::path(
    post,
    path = "/read",
    tag = "queries",
    operation_id = "read",
    request_body = ReadRequest,
    responses(
        (status = 200, description = "Query results", body = ReadOutput),
        (status = 400, description = "Bad request", body = ErrorOutput),
        (status = 401, description = "Unauthorized", body = ErrorOutput),
        (status = 403, description = "Forbidden", body = ErrorOutput),
    ),
    security(("bearer_token" = [])),
)]
/// Execute a GQ read query.
///
/// Runs the query in `query_source` against either a branch or a frozen
/// snapshot (mutually exclusive). When `query_source` defines multiple named
/// queries, pick one with `query_name`. `params` is a JSON object whose keys
/// match the parameters declared by the query. Returns rows as a JSON array
/// plus a `columns` list. Read-only.
async fn server_read(
    State(_state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ReadRequest>,
) -> std::result::Result<Json<ReadOutput>, ApiError> {
    if request.branch.is_some() && request.snapshot.is_some() {
        return Err(ApiError::bad_request(
            "read request may specify branch or snapshot, not both",
        ));
    }

    let target = read_target_from_request(request.branch, request.snapshot);
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
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
            action: PolicyAction::Read,
            branch: policy_branch,
            target_branch: None,
        },
    )?;
    let (selected_name, query_params) =
        select_named_query(&request.query_source, request.query_name.as_deref())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let params = query_params_from_json(&query_params, request.params.as_ref())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = {
        let db = &handle.engine;
        db.query(
            target.clone(),
            &request.query_source,
            &selected_name,
            &params,
        )
        .await
        .map_err(ApiError::from_omni)?
    };
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
    State(_state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ExportRequest>,
) -> std::result::Result<Response, ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
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

#[utoipa::path(
    post,
    path = "/change",
    tag = "mutations",
    operation_id = "change",
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
/// Apply a GQ mutation to a branch.
///
/// Writes to the named `branch` (defaults to `main`). Mutations are atomic
/// per call and produce a new commit. Returns counts of nodes and edges
/// affected. **Destructive**: on success the branch is updated; rejected
/// mutations may still acquire locks briefly. Returns 409 on merge conflict.
async fn server_change(
    State(state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<Json<ChangeOutput>, ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    let actor_arc = actor
        .as_ref()
        .map(|Extension(actor)| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor.as_ref().map(|Extension(actor)| actor.actor_id.as_ref());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor_id.map(str::to_string).unwrap_or_default(),
            action: PolicyAction::Change,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    // Per-actor admission: bound concurrent in-flight mutations and
    // estimated bytes per actor. Cedar runs FIRST so denied requests
    // don't consume admission slots. Estimate uses the request body
    // size as a coarse proxy; engine memory pressure can run higher.
    let est_bytes = request.query_source.len() as u64
        + request
            .params
            .as_ref()
            .map(|p| p.to_string().len() as u64)
            .unwrap_or(0);
    let _admission = state
        .workload
        .try_admit(&actor_arc, est_bytes)
        .map_err(ApiError::from_workload_reject)?;
    let (selected_name, query_params) =
        select_named_query(&request.query_source, request.query_name.as_deref())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let params = query_params_from_json(&query_params, request.params.as_ref())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = {
        let db = &handle.engine;
        db.mutate_as(
            &branch,
            &request.query_source,
            &selected_name,
            &params,
            actor_id,
        )
        .await
        .map_err(ApiError::from_omni)?
    };
    Ok(Json(ChangeOutput {
        branch,
        query_name: selected_name,
        affected_nodes: result.affected_nodes,
        affected_edges: result.affected_edges,
        actor_id: actor_id.map(str::to_string),
    }))
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
    State(_state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
) -> std::result::Result<Json<SchemaOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
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
    let actor_id = actor.as_ref().map(|Extension(actor)| actor.actor_id.as_ref());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor_id.map(str::to_string).unwrap_or_default(),
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
        // Engine-layer policy enforcement (MR-722): pass the resolved
        // actor through so apply_schema_as can call enforce() with the
        // authoritative identity. With a policy installed in AppState,
        // engine-side enforcement re-checks the same decision the
        // HTTP-layer authorize_request just made above. PR #3 collapses
        // the redundancy.
        db.apply_schema_as(
            &request.schema_source,
            omnigraph::db::SchemaApplyOptions {
                allow_data_loss: request.allow_data_loss,
            },
            actor_id,
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
    let actor_id = actor.as_ref().map(|Extension(actor)| actor.actor_id.as_ref());

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
                actor_id: actor_id.map(str::to_string).unwrap_or_default(),
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
            actor_id: actor_id.map(str::to_string).unwrap_or_default(),
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
    State(_state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
) -> std::result::Result<Json<BranchListOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
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
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
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
    Path(branch): Path<String>,
) -> std::result::Result<Json<BranchDeleteOutput>, ApiError> {
    let actor_arc = actor
        .as_ref()
        .map(|Extension(actor)| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let actor_id = actor.as_ref().map(|Extension(actor)| actor.actor_id.as_ref());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor_id.map(str::to_string).unwrap_or_default(),
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
    let actor_id = actor.as_ref().map(|Extension(actor)| actor.actor_id.as_ref());
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor_id.map(str::to_string).unwrap_or_default(),
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
    State(_state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Query(query): Query<CommitListQuery>,
) -> std::result::Result<Json<CommitListOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
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
    State(_state): State<AppState>,
    Extension(handle): Extension<Arc<GraphHandle>>,
    actor: Option<Extension<ResolvedActor>>,
    Path(commit_id): Path<String>,
) -> std::result::Result<Json<api::CommitOutput>, ApiError> {
    authorize_request(
        actor.as_ref().map(|Extension(actor)| actor),
        handle.policy.as_deref(),
        PolicyRequest {
            actor_id: actor
                .as_ref()
                .map(|Extension(actor)| actor.actor_id.as_ref().to_string())
                .unwrap_or_default(),
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

fn select_named_query(
    query_source: &str,
    requested_name: Option<&str>,
) -> Result<(String, Vec<omnigraph_compiler::query::ast::Param>)> {
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
        ServerConfig, ServerConfigMode, ServerRuntimeState, classify_server_runtime_state,
        hash_bearer_token, load_server_settings, normalize_bearer_token,
        parse_bearer_tokens_json, serve, server_bearer_tokens_from_env,
    };
    use serial_test::serial;
    use std::env;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn hash_bearer_token_produces_32_byte_output() {
        let hash = hash_bearer_token("any-token");
        assert_eq!(hash.len(), 32);
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
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/demo.omni"),
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
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "/tmp/override.omni"),
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
    uri: ./demo.omni
  dev:
    uri: http://127.0.0.1:8080
server:
  graph: local
  bind: 127.0.0.1:8080
"#,
        )
        .unwrap();

        let settings =
            load_server_settings(Some(&config), None, Some("dev".to_string()), None, false)
                .unwrap();
        match &settings.mode {
            ServerConfigMode::Single { uri, .. } => assert_eq!(uri, "http://127.0.0.1:8080"),
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
                policy_file: None,
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
    fn classify_policy_enabled_always_wins() {
        // State 3: any setup with a policy file → PolicyEnabled. The
        // flag doesn't matter and tokens-or-not doesn't matter (no
        // tokens + policy is unusual but valid — every request fails
        // 401 without a bearer, which is effectively "locked").
        assert_eq!(
            classify_server_runtime_state(true, true, false).unwrap(),
            ServerRuntimeState::PolicyEnabled
        );
        assert_eq!(
            classify_server_runtime_state(false, true, false).unwrap(),
            ServerRuntimeState::PolicyEnabled
        );
        assert_eq!(
            classify_server_runtime_state(true, true, true).unwrap(),
            ServerRuntimeState::PolicyEnabled
        );
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
