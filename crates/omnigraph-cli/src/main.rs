use std::fs;
use std::path::PathBuf;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use clap::{Args, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, bail};
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget, RunId, RunRecord, SnapshotId};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::json_params_to_param_map;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::result::QueryResult;
use omnigraph_compiler::{JsonParamMode, ParamMap};
use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph graph database CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize a new repo from a schema
    Init {
        #[arg(long)]
        schema: PathBuf,
        /// Repo URI (local path or s3://)
        uri: String,
    },
    /// Load data into a repo
    Load {
        #[arg(long)]
        data: PathBuf,
        #[arg(long, default_value = "main")]
        branch: String,
        #[arg(long, default_value = "overwrite")]
        mode: CliLoadMode,
        #[arg(long)]
        json: bool,
        /// Repo URI
        uri: String,
    },
    /// Branch operations
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    /// Show repo snapshot
    Snapshot {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Run operations
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    /// Execute a read query against a branch or snapshot
    Read {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Execute a graph change query against a branch
    Change {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Run Omnigraph as an HTTP server
    Server {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        bind: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum BranchCommand {
    /// Create a new branch
    Create {
        /// Repo URI
        uri: String,
        #[arg(long, default_value = "main")]
        from: String,
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// List branches
    List {
        /// Repo URI
        uri: String,
        #[arg(long)]
        json: bool,
    },
    /// Merge a source branch into a target branch
    Merge {
        /// Repo URI
        uri: String,
        source: String,
        #[arg(long, default_value = "main")]
        target: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    /// List transactional runs
    List {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Show a transactional run
    Show {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Publish a transactional run
    Publish {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Abort a transactional run
    Abort {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        run_id: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Args, Clone)]
struct ParamsArgs {
    #[arg(long, conflicts_with = "params_file")]
    params: Option<String>,
    #[arg(long, conflicts_with = "params")]
    params_file: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct CliConfigFile {
    uri: Option<String>,
    bind: Option<String>,
    branch: Option<String>,
    snapshot: Option<String>,
}

#[derive(Debug, Clone)]
struct ServerSettings {
    uri: String,
    bind: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum CliLoadMode {
    Overwrite,
    Append,
    Merge,
}

impl From<CliLoadMode> for LoadMode {
    fn from(value: CliLoadMode) -> Self {
        match value {
            CliLoadMode::Overwrite => LoadMode::Overwrite,
            CliLoadMode::Append => LoadMode::Append,
            CliLoadMode::Merge => LoadMode::Merge,
        }
    }
}

impl CliLoadMode {
    fn as_str(self) -> &'static str {
        match self {
            CliLoadMode::Overwrite => "overwrite",
            CliLoadMode::Append => "append",
            CliLoadMode::Merge => "merge",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CliMergeOutcome {
    AlreadyUpToDate,
    FastForward,
    Merged,
}

impl From<MergeOutcome> for CliMergeOutcome {
    fn from(value: MergeOutcome) -> Self {
        match value {
            MergeOutcome::AlreadyUpToDate => CliMergeOutcome::AlreadyUpToDate,
            MergeOutcome::FastForward => CliMergeOutcome::FastForward,
            MergeOutcome::Merged => CliMergeOutcome::Merged,
        }
    }
}

impl CliMergeOutcome {
    fn as_str(self) -> &'static str {
        match self {
            CliMergeOutcome::AlreadyUpToDate => "already_up_to_date",
            CliMergeOutcome::FastForward => "fast_forward",
            CliMergeOutcome::Merged => "merged",
        }
    }
}

#[derive(Debug, Serialize)]
struct LoadOutput<'a> {
    uri: &'a str,
    branch: &'a str,
    mode: &'a str,
    nodes_loaded: usize,
    edges_loaded: usize,
}

#[derive(Debug, Serialize)]
struct BranchCreateOutput<'a> {
    uri: &'a str,
    from: &'a str,
    name: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
struct BranchListOutput {
    branches: Vec<String>,
}

#[derive(Debug, Serialize)]
struct BranchMergeOutput<'a> {
    source: &'a str,
    target: &'a str,
    outcome: CliMergeOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotTableOutput {
    table_key: String,
    table_path: String,
    table_version: u64,
    table_branch: Option<String>,
    row_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotOutput {
    branch: String,
    manifest_version: u64,
    tables: Vec<SnapshotTableOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunOutput {
    run_id: String,
    target_branch: String,
    run_branch: String,
    base_snapshot_id: String,
    base_manifest_version: u64,
    operation_hash: Option<String>,
    status: String,
    published_snapshot_id: Option<String>,
    created_at: i64,
    updated_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RunListOutput {
    runs: Vec<RunOutput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadTargetOutput {
    branch: Option<String>,
    snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReadOutput {
    query_name: String,
    target: ReadTargetOutput,
    row_count: usize,
    rows: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangeOutput {
    branch: String,
    query_name: String,
    affected_nodes: usize,
    affected_edges: usize,
}

#[derive(Debug, Clone)]
struct ServerState {
    uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReadRequest {
    query_source: String,
    query_name: Option<String>,
    params: Option<Value>,
    branch: Option<String>,
    snapshot: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChangeRequest {
    query_source: String,
    query_name: Option<String>,
    params: Option<Value>,
    branch: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SnapshotQuery {
    branch: Option<String>,
}

#[derive(Debug, Serialize)]
struct HealthOutput<'a> {
    status: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorOutput {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorOutput {
                error: self.message,
            }),
        )
            .into_response()
    }
}

fn ensure_local_repo_parent(uri: &str) -> Result<()> {
    if !uri.contains("://") {
        fs::create_dir_all(uri)?;
    }
    Ok(())
}

fn print_json<T: Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn load_cli_config(config_path: Option<&PathBuf>) -> Result<CliConfigFile> {
    if let Some(path) = config_path {
        let contents = fs::read_to_string(path)?;
        Ok(toml::from_str::<CliConfigFile>(&contents)?)
    } else {
        Ok(CliConfigFile {
            uri: None,
            bind: None,
            branch: None,
            snapshot: None,
        })
    }
}

fn is_remote_uri(uri: &str) -> bool {
    uri.starts_with("http://") || uri.starts_with("https://")
}

fn remote_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

async fn remote_json<T: DeserializeOwned>(
    method: Method,
    url: String,
    body: Option<Value>,
) -> Result<T> {
    let client = reqwest::Client::new();
    let request = client.request(method, url);
    let request = if let Some(body) = body {
        request.json(&body)
    } else {
        request
    };
    let response = request.send().await?;
    let status = response.status();
    let text = response.text().await?;
    if !status.is_success() {
        if let Ok(error) = serde_json::from_str::<ErrorOutput>(&text) {
            bail!(error.error);
        }
        bail!("server returned {}: {}", status, text);
    }
    Ok(serde_json::from_str(&text)?)
}

fn load_server_settings(
    config_path: Option<&PathBuf>,
    cli_uri: Option<String>,
    cli_bind: Option<String>,
) -> Result<ServerSettings> {
    let file_config = load_cli_config(config_path)?;

    let uri = cli_uri
        .or(file_config.uri)
        .ok_or_else(|| color_eyre::eyre::eyre!("server URI must be provided via <URI> or --config"))?;
    let bind = cli_bind
        .or(file_config.bind)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    Ok(ServerSettings { uri, bind })
}

fn resolve_uri(config_path: Option<&PathBuf>, cli_uri: Option<String>) -> Result<String> {
    let file_config = load_cli_config(config_path)?;
    cli_uri
        .or(file_config.uri)
        .ok_or_else(|| color_eyre::eyre::eyre!("URI must be provided via <URI> or --config"))
}

fn resolve_branch(
    config_path: Option<&PathBuf>,
    cli_branch: Option<String>,
    default_branch: &str,
) -> Result<String> {
    let file_config = load_cli_config(config_path)?;
    Ok(cli_branch
        .or(file_config.branch)
        .unwrap_or_else(|| default_branch.to_string()))
}

fn resolve_read_target(
    config_path: Option<&PathBuf>,
    cli_branch: Option<String>,
    cli_snapshot: Option<String>,
) -> Result<ReadTarget> {
    let file_config = load_cli_config(config_path)?;
    let branch = cli_branch.or(file_config.branch);
    let snapshot = cli_snapshot.or(file_config.snapshot);
    if branch.is_some() && snapshot.is_some() {
        bail!("read target may specify branch or snapshot, not both");
    }
    Ok(read_target_from_cli(branch, snapshot))
}

fn print_load_human(
    uri: &str,
    branch: &str,
    mode: CliLoadMode,
    nodes_loaded: usize,
    edges_loaded: usize,
) {
    println!(
        "loaded {} on branch {} with {}: {} node types, {} edge types",
        uri,
        branch,
        mode.as_str(),
        nodes_loaded,
        edges_loaded
    );
}

fn print_snapshot_human(branch: &str, manifest_version: u64, entries: &[SnapshotTableOutput]) {
    println!("branch: {}", branch);
    println!("manifest_version: {}", manifest_version);
    for entry in entries {
        println!(
            "{} v{} branch={} rows={}",
            entry.table_key,
            entry.table_version,
            entry.table_branch.as_deref().unwrap_or("main"),
            entry.row_count
        );
    }
}

fn print_read_human(output: &ReadOutput) -> Result<()> {
    println!(
        "{} rows from {}",
        output.row_count,
        output
            .target
            .snapshot
            .as_deref()
            .map(|id| format!("snapshot {}", id))
            .or_else(|| {
                output
                    .target
                    .branch
                    .as_deref()
                    .map(|branch| format!("branch {}", branch))
            })
            .unwrap_or_else(|| "target".to_string())
    );
    println!("{}", serde_json::to_string_pretty(&output.rows)?);
    Ok(())
}

fn print_change_human(output: &ChangeOutput) {
    println!(
        "changed {} via {}: {} nodes, {} edges",
        output.branch, output.query_name, output.affected_nodes, output.affected_edges
    );
}

fn run_output(run: &RunRecord) -> RunOutput {
    RunOutput {
        run_id: run.run_id.as_str().to_string(),
        target_branch: run.target_branch.clone(),
        run_branch: run.run_branch.clone(),
        base_snapshot_id: run.base_snapshot_id.as_str().to_string(),
        base_manifest_version: run.base_manifest_version,
        operation_hash: run.operation_hash.clone(),
        status: run.status.as_str().to_string(),
        published_snapshot_id: run.published_snapshot_id.clone(),
        created_at: run.created_at,
        updated_at: run.updated_at,
    }
}

fn print_run_list_human(runs: &[RunOutput]) {
    for run in runs {
        println!(
            "{} {} target={} branch={}",
            run.run_id, run.status, run.target_branch, run.run_branch
        );
    }
}

fn print_run_human(run: &RunOutput) {
    println!("run_id: {}", run.run_id);
    println!("status: {}", run.status);
    println!("target_branch: {}", run.target_branch);
    println!("run_branch: {}", run.run_branch);
    println!("base_snapshot_id: {}", run.base_snapshot_id);
    println!("base_manifest_version: {}", run.base_manifest_version);
    if let Some(operation_hash) = &run.operation_hash {
        println!("operation_hash: {}", operation_hash);
    }
    if let Some(snapshot_id) = &run.published_snapshot_id {
        println!("published_snapshot_id: {}", snapshot_id);
    }
    println!("created_at: {}", run.created_at);
    println!("updated_at: {}", run.updated_at);
}

fn read_target_from_cli(branch: Option<String>, snapshot: Option<String>) -> ReadTarget {
    if let Some(snapshot) = snapshot {
        ReadTarget::snapshot(SnapshotId::new(snapshot))
    } else {
        ReadTarget::branch(branch.unwrap_or_else(|| "main".to_string()))
    }
}

fn read_target_output(target: &ReadTarget) -> ReadTargetOutput {
    match target {
        ReadTarget::Branch(branch) => ReadTargetOutput {
            branch: Some(branch.clone()),
            snapshot: None,
        },
        ReadTarget::Snapshot(snapshot) => ReadTargetOutput {
            branch: None,
            snapshot: Some(snapshot.as_str().to_string()),
        },
    }
}

fn load_params_json(params: &ParamsArgs) -> Result<Option<Value>> {
    match (&params.params, &params.params_file) {
        (Some(inline), None) => Ok(Some(serde_json::from_str(inline)?)),
        (None, Some(path)) => Ok(Some(serde_json::from_str(&fs::read_to_string(path)?)?)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => bail!("only one of --params or --params-file may be provided"),
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

fn read_output(query_name: String, target: &ReadTarget, result: QueryResult) -> ReadOutput {
    ReadOutput {
        query_name,
        target: read_target_output(target),
        row_count: result.num_rows(),
        rows: result.to_rust_json(),
    }
}

async fn execute_read(
    uri: &str,
    query_source: &str,
    query_name: Option<&str>,
    target: ReadTarget,
    params_json: Option<&Value>,
) -> Result<ReadOutput> {
    let (selected_name, query_params) = select_named_query(query_source, query_name)?;
    let params = query_params_from_json(&query_params, params_json)?;
    let mut db = Omnigraph::open(uri).await?;
    let result = db
        .query(target.clone(), query_source, &selected_name, &params)
        .await?;
    Ok(read_output(selected_name, &target, result))
}

async fn execute_read_remote(
    uri: &str,
    query_source: &str,
    query_name: Option<&str>,
    target: ReadTarget,
    params_json: Option<&Value>,
) -> Result<ReadOutput> {
    let (branch, snapshot) = match &target {
        ReadTarget::Branch(branch) => (Some(branch.clone()), None),
        ReadTarget::Snapshot(snapshot) => (None, Some(snapshot.as_str().to_string())),
    };
    remote_json(
        Method::POST,
        remote_url(uri, "/read"),
        Some(serde_json::to_value(ReadRequest {
            query_source: query_source.to_string(),
            query_name: query_name.map(ToOwned::to_owned),
            params: params_json.cloned(),
            branch,
            snapshot,
        })?),
    )
    .await
}

async fn execute_change(
    uri: &str,
    query_source: &str,
    query_name: Option<&str>,
    branch: &str,
    params_json: Option<&Value>,
) -> Result<ChangeOutput> {
    let (selected_name, query_params) = select_named_query(query_source, query_name)?;
    let params = query_params_from_json(&query_params, params_json)?;
    let mut db = Omnigraph::open(uri).await?;
    let result = db
        .mutate(branch, query_source, &selected_name, &params)
        .await?;
    Ok(ChangeOutput {
        branch: branch.to_string(),
        query_name: selected_name,
        affected_nodes: result.affected_nodes,
        affected_edges: result.affected_edges,
    })
}

async fn execute_change_remote(
    uri: &str,
    query_source: &str,
    query_name: Option<&str>,
    branch: &str,
    params_json: Option<&Value>,
) -> Result<ChangeOutput> {
    remote_json(
        Method::POST,
        remote_url(uri, "/change"),
        Some(serde_json::to_value(ChangeRequest {
            query_source: query_source.to_string(),
            query_name: query_name.map(ToOwned::to_owned),
            params: params_json.cloned(),
            branch: Some(branch.to_string()),
        })?),
    )
    .await
}

fn snapshot_payload(branch: &str, snapshot: &omnigraph::db::Snapshot) -> SnapshotOutput {
    let mut entries: Vec<_> = snapshot.entries().cloned().collect();
    entries.sort_by(|a, b| a.table_key.cmp(&b.table_key));
    let tables = entries
        .iter()
        .map(|entry| SnapshotTableOutput {
            table_key: entry.table_key.clone(),
            table_path: entry.table_path.clone(),
            table_version: entry.table_version,
            table_branch: entry.table_branch.clone(),
            row_count: entry.row_count,
        })
        .collect::<Vec<_>>();
    SnapshotOutput {
        branch: branch.to_string(),
        manifest_version: snapshot.version(),
        tables,
    }
}

async fn server_health() -> Json<HealthOutput<'static>> {
    Json(HealthOutput { status: "ok" })
}

async fn run_request_task<F, Fut, T>(task: F) -> std::result::Result<T, ApiError>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = std::result::Result<T, ApiError>> + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| ApiError::internal(err.to_string()))?;
        runtime.block_on(task())
    })
    .await
    .map_err(|err| ApiError::internal(err.to_string()))?
}

#[axum::debug_handler]
async fn server_snapshot(
    State(state): State<ServerState>,
    Query(query): Query<SnapshotQuery>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let branch = query.branch.unwrap_or_else(|| "main".to_string());
    let uri = state.uri;
    let payload = run_request_task(move || async move {
        let db = Omnigraph::open(&uri)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let snapshot = db
            .snapshot_of(ReadTarget::branch(branch.as_str()))
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        Ok(snapshot_payload(&branch, &snapshot))
    })
    .await?;
    Ok(Json(
        serde_json::to_value(payload).map_err(|err| ApiError::internal(err.to_string()))?,
    ))
}

#[axum::debug_handler]
async fn server_read(
    State(state): State<ServerState>,
    Json(request): Json<ReadRequest>,
) -> std::result::Result<Json<ReadOutput>, ApiError> {
    if request.branch.is_some() && request.snapshot.is_some() {
        return Err(ApiError::bad_request(
            "read request may specify branch or snapshot, not both",
        ));
    }
    let uri = state.uri;
    let target = read_target_from_cli(request.branch, request.snapshot);
    let query_source = request.query_source;
    let query_name = request.query_name;
    let params_json = request.params;
    let output = run_request_task(move || async move {
        let (selected_name, query_params) =
            select_named_query(&query_source, query_name.as_deref())
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let params = query_params_from_json(&query_params, params_json.as_ref())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let mut db = Omnigraph::open(&uri)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let result = db
            .query(target.clone(), &query_source, &selected_name, &params)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        Ok(read_output(selected_name, &target, result))
    })
    .await?;
    Ok(Json(output))
}

#[axum::debug_handler]
async fn server_change(
    State(state): State<ServerState>,
    Json(request): Json<ChangeRequest>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let branch = request.branch.unwrap_or_else(|| "main".to_string());
    let uri = state.uri;
    let query_source = request.query_source;
    let query_name = request.query_name;
    let params_json = request.params;
    let output = run_request_task(move || async move {
        let (selected_name, query_params) =
            select_named_query(&query_source, query_name.as_deref())
                .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let params = query_params_from_json(&query_params, params_json.as_ref())
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let mut db = Omnigraph::open(&uri)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        let result = db
            .mutate(&branch, &query_source, &selected_name, &params)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        Ok(ChangeOutput {
            branch,
            query_name: selected_name,
            affected_nodes: result.affected_nodes,
            affected_edges: result.affected_edges,
        })
    })
    .await?;
    Ok(Json(
        serde_json::to_value(output).map_err(|err| ApiError::internal(err.to_string()))?,
    ))
}

#[axum::debug_handler]
async fn server_run_list(
    State(state): State<ServerState>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let uri = state.uri;
    let runs = run_request_task(move || async move {
        let db = Omnigraph::open(&uri)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        db.list_runs()
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))
    })
    .await?;
    Ok(Json(
        serde_json::to_value(RunListOutput {
            runs: runs.iter().map(run_output).collect(),
        })
        .map_err(|err| ApiError::internal(err.to_string()))?,
    ))
}

#[axum::debug_handler]
async fn server_run_show(
    State(state): State<ServerState>,
    Path(run_id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let uri = state.uri;
    let run = run_request_task(move || async move {
        let db = Omnigraph::open(&uri)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        db.get_run(&RunId::new(run_id))
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))
    })
    .await?;
    Ok(Json(
        serde_json::to_value(run_output(&run)).map_err(|err| ApiError::internal(err.to_string()))?,
    ))
}

#[axum::debug_handler]
async fn server_run_publish(
    State(state): State<ServerState>,
    Path(run_id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let uri = state.uri;
    let run = run_request_task(move || async move {
        let mut db = Omnigraph::open(&uri)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        db.publish_run(&RunId::new(run_id.clone()))
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        db.get_run(&RunId::new(run_id))
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))
    })
    .await?;
    Ok(Json(
        serde_json::to_value(run_output(&run)).map_err(|err| ApiError::internal(err.to_string()))?,
    ))
}

#[axum::debug_handler]
async fn server_run_abort(
    State(state): State<ServerState>,
    Path(run_id): Path<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let uri = state.uri;
    let run = run_request_task(move || async move {
        let mut db = Omnigraph::open(&uri)
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))?;
        db.abort_run(&RunId::new(run_id))
            .await
            .map_err(|err| ApiError::bad_request(err.to_string()))
    })
    .await?;
    Ok(Json(
        serde_json::to_value(run_output(&run)).map_err(|err| ApiError::internal(err.to_string()))?,
    ))
}

fn build_server_app(uri: String) -> Router {
    let state = ServerState { uri };
    Router::new()
        .route("/healthz", get(server_health))
        .route("/snapshot", get(server_snapshot))
        .route("/read", post(server_read))
        .route("/change", post(server_change))
        .route("/runs", get(server_run_list))
        .route("/runs/{run_id}", get(server_run_show))
        .route("/runs/{run_id}/publish", post(server_run_publish))
        .route("/runs/{run_id}/abort", post(server_run_abort))
        .with_state(state)
}

async fn run_server(bind: &str, uri: &str) -> Result<()> {
    let listener = TcpListener::bind(bind).await?;
    println!("serving {} on http://{}", uri, bind);
    axum::serve(listener, build_server_app(uri.to_string())).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    match cli.command {
        Command::Init { schema, uri } => {
            let schema_source = fs::read_to_string(&schema)?;
            ensure_local_repo_parent(&uri)?;
            Omnigraph::init(&uri, &schema_source).await?;
            println!("initialized {}", uri);
        }
        Command::Load {
            data,
            branch,
            mode,
            json,
            uri,
        } => {
            let mut db = Omnigraph::open(&uri).await?;
            let result = db
                .load_file(&branch, &data.to_string_lossy(), mode.into())
                .await?;
            let payload = LoadOutput {
                uri: &uri,
                branch: &branch,
                mode: mode.as_str(),
                nodes_loaded: result.nodes_loaded.len(),
                edges_loaded: result.edges_loaded.len(),
            };
            if json {
                print_json(&payload)?;
            } else {
                print_load_human(
                    &uri,
                    &branch,
                    mode,
                    payload.nodes_loaded,
                    payload.edges_loaded,
                );
            }
        }
        Command::Branch { command } => match command {
            BranchCommand::Create {
                uri,
                from,
                name,
                json,
            } => {
                let mut db = Omnigraph::open(&uri).await?;
                db.branch_create_from(ReadTarget::branch(&from), &name)
                    .await?;
                if json {
                    print_json(&BranchCreateOutput {
                        uri: &uri,
                        from: &from,
                        name: &name,
                    })?;
                } else {
                    println!("created branch {} from {}", name, from);
                }
            }
            BranchCommand::List { uri, json } => {
                let db = Omnigraph::open(&uri).await?;
                let mut branches = db.branch_list().await?;
                branches.sort();
                if json {
                    print_json(&BranchListOutput { branches })?;
                } else {
                    for branch in branches {
                        println!("{}", branch);
                    }
                }
            }
            BranchCommand::Merge {
                uri,
                source,
                target,
                json,
            } => {
                let mut db = Omnigraph::open(&uri).await?;
                let outcome: CliMergeOutcome = db.branch_merge(&source, &target).await?.into();
                if json {
                    print_json(&BranchMergeOutput {
                        source: &source,
                        target: &target,
                        outcome,
                    })?;
                } else {
                    println!("merged {} into {}: {}", source, target, outcome.as_str());
                }
            }
        },
        Command::Snapshot {
            uri,
            config,
            branch,
            json,
        } => {
            let uri = resolve_uri(config.as_ref(), uri)?;
            let branch = resolve_branch(config.as_ref(), branch, "main")?;
            let payload = if is_remote_uri(&uri) {
                remote_json::<SnapshotOutput>(
                    Method::GET,
                    format!(
                        "{}?branch={}",
                        remote_url(&uri, "/snapshot"),
                        branch
                    ),
                    None,
                )
                .await?
            } else {
                let db = Omnigraph::open(&uri).await?;
                let snapshot = db.snapshot_of(ReadTarget::branch(branch.as_str())).await?;
                snapshot_payload(&branch, &snapshot)
            };

            if json {
                print_json(&payload)?;
            } else {
                print_snapshot_human(&payload.branch, payload.manifest_version, &payload.tables);
            }
        }
        Command::Run { command } => match command {
            RunCommand::List { uri, config, json } => {
                let uri = resolve_uri(config.as_ref(), uri)?;
                let runs = if is_remote_uri(&uri) {
                    remote_json::<RunListOutput>(Method::GET, remote_url(&uri, "/runs"), None)
                        .await?
                        .runs
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    db.list_runs()
                        .await?
                        .iter()
                        .map(run_output)
                        .collect::<Vec<_>>()
                };
                if json {
                    print_json(&RunListOutput { runs })?;
                } else {
                    print_run_list_human(&runs);
                }
            }
            RunCommand::Show {
                uri,
                config,
                run_id,
                json,
            } => {
                let uri = resolve_uri(config.as_ref(), uri)?;
                let run = if is_remote_uri(&uri) {
                    remote_json::<RunOutput>(
                        Method::GET,
                        remote_url(&uri, &format!("/runs/{}", run_id)),
                        None,
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    run_output(&db.get_run(&RunId::new(run_id)).await?)
                };
                if json {
                    print_json(&run)?;
                } else {
                    print_run_human(&run);
                }
            }
            RunCommand::Publish {
                uri,
                config,
                run_id,
                json,
            } => {
                let uri = resolve_uri(config.as_ref(), uri)?;
                let run = if is_remote_uri(&uri) {
                    remote_json::<RunOutput>(
                        Method::POST,
                        remote_url(&uri, &format!("/runs/{}/publish", run_id)),
                        Some(serde_json::json!({})),
                    )
                    .await?
                } else {
                    let mut db = Omnigraph::open(&uri).await?;
                    db.publish_run(&RunId::new(run_id.clone())).await?;
                    run_output(&db.get_run(&RunId::new(run_id)).await?)
                };
                if json {
                    print_json(&run)?;
                } else {
                    print_run_human(&run);
                }
            }
            RunCommand::Abort {
                uri,
                config,
                run_id,
                json,
            } => {
                let uri = resolve_uri(config.as_ref(), uri)?;
                let run = if is_remote_uri(&uri) {
                    remote_json::<RunOutput>(
                        Method::POST,
                        remote_url(&uri, &format!("/runs/{}/abort", run_id)),
                        Some(serde_json::json!({})),
                    )
                    .await?
                } else {
                    let mut db = Omnigraph::open(&uri).await?;
                    run_output(&db.abort_run(&RunId::new(run_id)).await?)
                };
                if json {
                    print_json(&run)?;
                } else {
                    print_run_human(&run);
                }
            }
        },
        Command::Read {
            uri,
            config,
            query,
            name,
            params,
            branch,
            snapshot,
            json,
        } => {
            let uri = resolve_uri(config.as_ref(), uri)?;
            let query_source = fs::read_to_string(&query)?;
            let params_json = load_params_json(&params)?;
            let target = resolve_read_target(config.as_ref(), branch, snapshot)?;
            let output = if is_remote_uri(&uri) {
                execute_read_remote(
                    &uri,
                    &query_source,
                    name.as_deref(),
                    target,
                    params_json.as_ref(),
                )
                .await?
            } else {
                execute_read(
                    &uri,
                    &query_source,
                    name.as_deref(),
                    target,
                    params_json.as_ref(),
                )
                .await?
            };
            if json {
                print_json(&output)?;
            } else {
                print_read_human(&output)?;
            }
        }
        Command::Change {
            uri,
            config,
            query,
            name,
            params,
            branch,
            json,
        } => {
            let uri = resolve_uri(config.as_ref(), uri)?;
            let query_source = fs::read_to_string(&query)?;
            let params_json = load_params_json(&params)?;
            let branch = resolve_branch(config.as_ref(), branch, "main")?;
            let output = if is_remote_uri(&uri) {
                execute_change_remote(
                    &uri,
                    &query_source,
                    name.as_deref(),
                    &branch,
                    params_json.as_ref(),
                )
                .await?
            } else {
                execute_change(
                    &uri,
                    &query_source,
                    name.as_deref(),
                    &branch,
                    params_json.as_ref(),
                )
                .await?
            };
            if json {
                print_json(&output)?;
            } else {
                print_change_human(&output);
            }
        }
        Command::Server { uri, config, bind } => {
            let settings = load_server_settings(config.as_ref(), uri, bind)?;
            run_server(&settings.bind, &settings.uri).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::load_server_settings;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn server_settings_load_from_toml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("server.toml");
        fs::write(
            &config,
            r#"
uri = "/tmp/demo.omni"
bind = "0.0.0.0:9090"
"#,
        )
        .unwrap();

        let settings = load_server_settings(Some(&config), None, None).unwrap();
        assert_eq!(settings.uri, "/tmp/demo.omni");
        assert_eq!(settings.bind, "0.0.0.0:9090");
    }

    #[test]
    fn server_settings_cli_flags_override_toml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("server.toml");
        fs::write(
            &config,
            r#"
uri = "/tmp/demo.omni"
bind = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        let settings = load_server_settings(
            Some(&config),
            Some("/tmp/override.omni".to_string()),
            Some("0.0.0.0:9999".to_string()),
        )
        .unwrap();
        assert_eq!(settings.uri, "/tmp/override.omni");
        assert_eq!(settings.bind, "0.0.0.0:9999");
    }

    #[test]
    fn server_settings_require_uri_from_cli_or_config() {
        let error = load_server_settings(None, None, None).unwrap_err();
        assert!(error.to_string().contains("server URI must be provided"));
    }
}
