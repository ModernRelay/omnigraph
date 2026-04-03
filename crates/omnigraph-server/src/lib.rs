pub mod api;
pub mod config;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use api::{
    BranchCreateOutput, BranchCreateRequest, BranchListOutput, BranchMergeOutput,
    BranchMergeRequest, ChangeOutput, ChangeRequest, ErrorCode, ErrorOutput, HealthOutput,
    ReadOutput, ReadRequest, RunListOutput, SnapshotQuery, snapshot_payload,
};
use axum::extract::DefaultBodyLimit;
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use color_eyre::eyre::{Result, WrapErr, bail};
pub use config::{
    AliasCommand, AliasConfig, CliDefaults, DEFAULT_CONFIG_FILE, OmnigraphConfig, ProjectConfig,
    QueryDefaults, ReadOutputFormat, ServerDefaults, TableCellLayout, TargetConfig, load_config,
};
use omnigraph::db::{Omnigraph, ReadTarget, RunId};
use omnigraph::error::{ManifestErrorKind, OmniError};
use omnigraph_compiler::json_params_to_param_map;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::{JsonParamMode, ParamMap};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub uri: String,
    pub bind: String,
}

#[derive(Clone)]
pub struct AppState {
    uri: String,
    db: Arc<RwLock<Omnigraph>>,
    bearer_tokens: Arc<HashMap<Arc<str>, Arc<str>>>,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: ErrorCode,
    message: String,
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
        let bearer_tokens = bearer_tokens
            .into_iter()
            .map(|(actor, token)| (Arc::<str>::from(token), Arc::<str>::from(actor)))
            .collect();
        Self {
            uri,
            db: Arc::new(RwLock::new(db)),
            bearer_tokens: Arc::new(bearer_tokens),
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

    pub fn uri(&self) -> &str {
        &self.uri
    }

    fn requires_bearer_auth(&self) -> bool {
        !self.bearer_tokens.is_empty()
    }

    fn authenticate_bearer_token(&self, provided_token: &str) -> Option<Arc<str>> {
        self.bearer_tokens.get(provided_token).cloned()
    }
}

impl ApiError {
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: ErrorCode::Unauthorized,
            message: message.into(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: ErrorCode::BadRequest,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: ErrorCode::NotFound,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: ErrorCode::Conflict,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: ErrorCode::Internal,
            message: message.into(),
        }
    }

    fn from_omni(err: OmniError) -> Self {
        match err {
            OmniError::Compiler(err) => Self::bad_request(err.to_string()),
            OmniError::DataFusion(message) => Self::bad_request(format!("query: {message}")),
            OmniError::Manifest(err) => match err.kind {
                ManifestErrorKind::BadRequest => Self::bad_request(err.message),
                ManifestErrorKind::NotFound => Self::not_found(err.message),
                ManifestErrorKind::Conflict => Self::conflict(err.message),
                ManifestErrorKind::Internal => Self::internal(err.message),
            },
            OmniError::MergeConflicts(conflicts) => {
                Self::conflict(format!("merge conflicts: {:?}", conflicts))
            }
            OmniError::Lance(message) => Self::internal(format!("storage: {message}")),
            OmniError::Io(err) => Self::internal(format!("io: {err}")),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorOutput {
                error: self.message,
                code: Some(self.code),
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
) -> Result<ServerConfig> {
    let config = load_config(config_path)?;
    let uri =
        config.resolve_target_uri(cli_uri, cli_target.as_deref(), config.server_target_name())?;
    let bind = cli_bind.unwrap_or_else(|| config.server_bind().to_string());

    Ok(ServerConfig { uri, bind })
}

pub fn build_app(state: AppState) -> Router {
    let protected = Router::new()
        .route("/snapshot", get(server_snapshot))
        .route("/read", post(server_read))
        .route("/change", post(server_change))
        .route(
            "/branches",
            get(server_branch_list).post(server_branch_create),
        )
        .route("/branches/merge", post(server_branch_merge))
        .route("/runs", get(server_run_list))
        .route("/runs/{run_id}", get(server_run_show))
        .route("/runs/{run_id}/publish", post(server_run_publish))
        .route("/runs/{run_id}/abort", post(server_run_abort))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer_auth,
        ));

    Router::new()
        .route("/healthz", get(server_health))
        .merge(protected)
        .layer(DefaultBodyLimit::max(1_048_576))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub async fn serve(config: ServerConfig) -> Result<()> {
    let state =
        AppState::open_with_bearer_tokens(config.uri.clone(), server_bearer_tokens_from_env()?)
            .await?;
    let listener = TcpListener::bind(&config.bind).await?;
    info!(uri = %config.uri, bind = %config.bind, "serving omnigraph");
    axum::serve(listener, build_app(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(err) = tokio::signal::ctrl_c().await {
        error!(error = %err, "failed to install ctrl-c handler");
        return;
    }
    info!("shutdown signal received");
}

async fn server_health() -> Json<HealthOutput> {
    Json(HealthOutput {
        status: "ok".to_string(),
    })
}

async fn require_bearer_auth(
    State(state): State<AppState>,
    request: Request,
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

    if state.authenticate_bearer_token(provided_token).is_none() {
        return Err(ApiError::unauthorized("invalid bearer token"));
    }

    Ok(next.run(request).await)
}

async fn server_snapshot(
    State(state): State<AppState>,
    Query(query): Query<SnapshotQuery>,
) -> std::result::Result<Json<api::SnapshotOutput>, ApiError> {
    let branch = query.branch.unwrap_or_else(|| "main".to_string());
    let snapshot = {
        let db = Arc::clone(&state.db).read_owned().await;
        db.snapshot_of(ReadTarget::branch(branch.as_str()))
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(snapshot_payload(&branch, &snapshot)))
}

async fn server_read(
    State(state): State<AppState>,
    Json(request): Json<ReadRequest>,
) -> std::result::Result<Json<ReadOutput>, ApiError> {
    if request.branch.is_some() && request.snapshot.is_some() {
        return Err(ApiError::bad_request(
            "read request may specify branch or snapshot, not both",
        ));
    }

    let target = read_target_from_request(request.branch, request.snapshot);
    let (selected_name, query_params) =
        select_named_query(&request.query_source, request.query_name.as_deref())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let params = query_params_from_json(&query_params, request.params.as_ref())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = {
        let db = Arc::clone(&state.db).read_owned().await;
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

async fn server_change(
    State(state): State<AppState>,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<Json<ChangeOutput>, ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    let (selected_name, query_params) =
        select_named_query(&request.query_source, request.query_name.as_deref())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let params = query_params_from_json(&query_params, request.params.as_ref())
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    let result = {
        let mut db = Arc::clone(&state.db).write_owned().await;
        db.mutate(&branch, &request.query_source, &selected_name, &params)
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(ChangeOutput {
        branch,
        query_name: selected_name,
        affected_nodes: result.affected_nodes,
        affected_edges: result.affected_edges,
    }))
}

async fn server_branch_list(
    State(state): State<AppState>,
) -> std::result::Result<Json<BranchListOutput>, ApiError> {
    let mut branches = {
        let db = Arc::clone(&state.db).read_owned().await;
        db.branch_list().await.map_err(ApiError::from_omni)?
    };
    branches.sort();
    Ok(Json(BranchListOutput { branches }))
}

async fn server_branch_create(
    State(state): State<AppState>,
    Json(request): Json<BranchCreateRequest>,
) -> std::result::Result<Json<BranchCreateOutput>, ApiError> {
    let from = request.from.unwrap_or_else(|| "main".to_string());
    {
        let mut db = Arc::clone(&state.db).write_owned().await;
        db.branch_create_from(ReadTarget::branch(&from), &request.name)
            .await
            .map_err(ApiError::from_omni)?;
    }
    Ok(Json(BranchCreateOutput {
        uri: state.uri().to_string(),
        from,
        name: request.name,
    }))
}

async fn server_branch_merge(
    State(state): State<AppState>,
    Json(request): Json<BranchMergeRequest>,
) -> std::result::Result<Json<BranchMergeOutput>, ApiError> {
    let target = request.target.unwrap_or_else(|| "main".to_string());
    let outcome = {
        let mut db = Arc::clone(&state.db).write_owned().await;
        db.branch_merge(&request.source, &target)
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(BranchMergeOutput {
        source: request.source,
        target,
        outcome: outcome.into(),
    }))
}

async fn server_run_list(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunListOutput>, ApiError> {
    let runs = {
        let db = Arc::clone(&state.db).read_owned().await;
        db.list_runs().await.map_err(ApiError::from_omni)?
    };
    Ok(Json(RunListOutput {
        runs: runs.iter().map(api::run_output).collect(),
    }))
}

async fn server_run_show(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> std::result::Result<Json<api::RunOutput>, ApiError> {
    let run = {
        let db = Arc::clone(&state.db).read_owned().await;
        db.get_run(&RunId::new(run_id))
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(api::run_output(&run)))
}

async fn server_run_publish(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> std::result::Result<Json<api::RunOutput>, ApiError> {
    let run_id = RunId::new(run_id);
    let run = {
        let mut db = Arc::clone(&state.db).write_owned().await;
        db.publish_run(&run_id).await.map_err(ApiError::from_omni)?;
        db.get_run(&run_id).await.map_err(ApiError::from_omni)?
    };
    Ok(Json(api::run_output(&run)))
}

async fn server_run_abort(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> std::result::Result<Json<api::RunOutput>, ApiError> {
    let run = {
        let mut db = Arc::clone(&state.db).write_owned().await;
        db.abort_run(&RunId::new(run_id))
            .await
            .map_err(ApiError::from_omni)?
    };
    Ok(Json(api::run_output(&run)))
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
        load_server_settings, normalize_bearer_token, parse_bearer_tokens_json,
        server_bearer_tokens_from_env,
    };
    use std::env;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn server_settings_load_from_yaml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
targets:
  local:
    uri: /tmp/demo.omni
server:
  target: local
  bind: 0.0.0.0:9090
"#,
        )
        .unwrap();

        let settings = load_server_settings(Some(&config), None, None, None).unwrap();
        assert_eq!(settings.uri, "/tmp/demo.omni");
        assert_eq!(settings.bind, "0.0.0.0:9090");
    }

    #[test]
    fn server_settings_cli_flags_override_yaml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
targets:
  local:
    uri: /tmp/demo.omni
server:
  target: local
  bind: 127.0.0.1:8080
"#,
        )
        .unwrap();

        let settings = load_server_settings(
            Some(&config),
            Some("/tmp/override.omni".to_string()),
            None,
            Some("0.0.0.0:9999".to_string()),
        )
        .unwrap();
        assert_eq!(settings.uri, "/tmp/override.omni");
        assert_eq!(settings.bind, "0.0.0.0:9999");
    }

    #[test]
    fn server_settings_can_resolve_named_target() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
targets:
  local:
    uri: ./demo.omni
  dev:
    uri: http://127.0.0.1:8080
server:
  target: local
  bind: 127.0.0.1:8080
"#,
        )
        .unwrap();

        let settings =
            load_server_settings(Some(&config), None, Some("dev".to_string()), None).unwrap();
        assert_eq!(settings.uri, "http://127.0.0.1:8080");
    }

    #[test]
    fn server_settings_require_uri_from_cli_or_config() {
        let error = load_server_settings(None, None, None, None).unwrap_err();
        assert!(error.to_string().contains("URI must be provided"));
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
