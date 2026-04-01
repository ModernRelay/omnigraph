use std::fs;
use std::path::Path;
use std::path::PathBuf;

use clap::{Arg, ArgAction, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, bail};
use omnigraph::db::{MergeOutcome, Omnigraph, ReadTarget, RunId, SnapshotId};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::json_params_to_param_map;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::{JsonParamMode, ParamMap};
use omnigraph_server::api::{
    ChangeOutput, ChangeRequest, ErrorOutput, ReadOutput, ReadRequest, RunListOutput, RunOutput,
    SnapshotOutput, SnapshotTableOutput, read_output, run_output, snapshot_payload,
};
use omnigraph_server::{AliasCommand, OmnigraphConfig, ReadOutputFormat, load_config};
use reqwest::Method;
use reqwest::header::AUTHORIZATION;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod read_format;

use read_format::{ReadRenderOptions, render_read};

const DEFAULT_BEARER_TOKEN_ENV: &str = "OMNIGRAPH_BEARER_TOKEN";

#[derive(Debug, Parser)]
#[command(name = "omnigraph")]
#[command(about = "Omnigraph graph database CLI")]
#[command(version = env!("CARGO_PKG_VERSION"), disable_version_flag = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the CLI version
    Version,
    /// Initialize a new repo from a schema
    Init {
        #[arg(long)]
        schema: PathBuf,
        /// Repo URI (local path or s3://)
        uri: String,
    },
    /// Load data into a repo
    Load {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long, default_value = "overwrite")]
        mode: CliLoadMode,
        #[arg(long)]
        json: bool,
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
        target: Option<String>,
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
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        alias: Option<String>,
        #[arg(long)]
        query: Option<PathBuf>,
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long, conflicts_with = "snapshot")]
        branch: Option<String>,
        #[arg(long, conflicts_with = "branch")]
        snapshot: Option<String>,
        #[arg(long, conflicts_with = "json")]
        format: Option<ReadOutputFormat>,
        #[arg(long, conflicts_with = "format")]
        json: bool,
        #[arg()]
        alias_args: Vec<String>,
    },
    /// Execute a graph change query against a branch
    Change {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        alias: Option<String>,
        #[arg(long)]
        query: Option<PathBuf>,
        #[arg(long)]
        name: Option<String>,
        #[command(flatten)]
        params: ParamsArgs,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg()]
        alias_args: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum BranchCommand {
    /// Create a new branch
    Create {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        from: Option<String>,
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// List branches
    List {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Merge a source branch into a target branch
    Merge {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        source: String,
        #[arg(long)]
        into: Option<String>,
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
        target: Option<String>,
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
        target: Option<String>,
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
        target: Option<String>,
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
        target: Option<String>,
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

fn is_remote_uri(uri: &str) -> bool {
    uri.starts_with("http://") || uri.starts_with("https://")
}

fn remote_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

fn normalize_bearer_token(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn bearer_token_from_env(var_name: &str) -> Option<String> {
    normalize_bearer_token(std::env::var(var_name).ok())
}

fn parse_env_assignment(line: &str) -> Option<(String, String)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    let line = line.strip_prefix("export ").unwrap_or(line).trim();
    let (name, value) = line.split_once('=')?;
    let name = name.trim();
    if name.is_empty() {
        return None;
    }

    let value = value.trim();
    let value = if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    };

    Some((name.to_string(), value.to_string()))
}

fn bearer_token_from_env_file(path: &Path, var_name: &str) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }

    for line in fs::read_to_string(path)?.lines() {
        let Some((name, value)) = parse_env_assignment(line) else {
            continue;
        };
        if name == var_name {
            return Ok(normalize_bearer_token(Some(value)));
        }
    }

    Ok(None)
}

fn load_env_file_into_process(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    for line in fs::read_to_string(path)?.lines() {
        let Some((name, value)) = parse_env_assignment(line) else {
            continue;
        };
        if std::env::var_os(&name).is_none() {
            unsafe {
                std::env::set_var(name, value);
            }
        }
    }

    Ok(())
}

fn load_cli_config(config_path: Option<&PathBuf>) -> Result<OmnigraphConfig> {
    let config = load_config(config_path)?;
    if let Some(path) = config.resolve_auth_env_file() {
        load_env_file_into_process(&path)?;
    }
    Ok(config)
}

fn resolve_remote_bearer_token(
    config: &OmnigraphConfig,
    explicit_uri: Option<&str>,
    explicit_target: Option<&str>,
) -> Result<Option<String>> {
    let scoped_env =
        config.target_bearer_token_env(explicit_uri, explicit_target, config.cli_target_name());
    let mut env_names = Vec::new();
    if let Some(name) = scoped_env {
        env_names.push(name.to_string());
    }
    if env_names
        .iter()
        .all(|name| name != DEFAULT_BEARER_TOKEN_ENV)
    {
        env_names.push(DEFAULT_BEARER_TOKEN_ENV.to_string());
    }

    let env_file = config.resolve_auth_env_file();
    for env_name in env_names {
        if let Some(token) = bearer_token_from_env(&env_name) {
            return Ok(Some(token));
        }
        if let Some(path) = env_file.as_ref() {
            if let Some(token) = bearer_token_from_env_file(path, &env_name)? {
                return Ok(Some(token));
            }
        }
    }

    Ok(None)
}

fn build_http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::new())
}

fn apply_bearer_token(
    request: reqwest::RequestBuilder,
    token: Option<&str>,
) -> reqwest::RequestBuilder {
    if let Some(token) = token {
        request.header(AUTHORIZATION, format!("Bearer {}", token))
    } else {
        request
    }
}

async fn remote_json<T: DeserializeOwned>(
    client: &reqwest::Client,
    method: Method,
    url: String,
    body: Option<Value>,
    bearer_token: Option<&str>,
) -> Result<T> {
    let request = apply_bearer_token(client.request(method, url), bearer_token);
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

fn resolve_uri(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
) -> Result<String> {
    config.resolve_target_uri(cli_uri, cli_target, config.cli_target_name())
}

fn resolve_local_uri(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
    operation: &str,
) -> Result<String> {
    let uri = resolve_uri(config, cli_uri, cli_target)?;
    if is_remote_uri(&uri) {
        bail!(
            "{} is only supported against local repo URIs in this milestone",
            operation
        );
    }
    Ok(uri)
}

fn resolve_branch(
    config: &OmnigraphConfig,
    cli_branch: Option<String>,
    alias_branch: Option<String>,
    default_branch: &str,
) -> String {
    cli_branch
        .or(alias_branch)
        .or_else(|| config.cli.branch.clone())
        .unwrap_or_else(|| default_branch.to_string())
}

fn resolve_read_target(
    config: &OmnigraphConfig,
    cli_branch: Option<String>,
    cli_snapshot: Option<String>,
    alias_branch: Option<String>,
) -> Result<ReadTarget> {
    if cli_branch.is_some() && cli_snapshot.is_some() {
        bail!("read target may specify branch or snapshot, not both");
    }
    Ok(read_target_from_cli(
        cli_branch
            .or(alias_branch)
            .or_else(|| config.cli.branch.clone()),
        cli_snapshot,
    ))
}

fn resolve_query_source(
    config: &OmnigraphConfig,
    explicit_query: Option<&PathBuf>,
    alias_query: Option<&str>,
) -> Result<String> {
    let query_path = explicit_query
        .map(PathBuf::from)
        .or_else(|| alias_query.map(PathBuf::from))
        .ok_or_else(|| {
            color_eyre::eyre::eyre!("exactly one of --query or --alias must be provided")
        })?;
    Ok(fs::read_to_string(config.resolve_query_path(&query_path)?)?)
}

fn parse_alias_value(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
}

fn merged_params_json(
    alias_name: Option<&str>,
    alias_arg_names: &[String],
    alias_arg_values: &[String],
    explicit: Option<Value>,
) -> Result<Option<Value>> {
    if alias_arg_values.len() > alias_arg_names.len() {
        let alias = alias_name.unwrap_or("<alias>");
        bail!(
            "alias '{}' expects at most {} args but got {}",
            alias,
            alias_arg_names.len(),
            alias_arg_values.len()
        );
    }

    let mut merged = serde_json::Map::new();
    for (arg_name, arg_value) in alias_arg_names.iter().zip(alias_arg_values.iter()) {
        merged.insert(arg_name.clone(), parse_alias_value(arg_value));
    }

    match explicit {
        Some(Value::Object(object)) => {
            for (key, value) in object {
                merged.insert(key, value);
            }
        }
        Some(_) => bail!("params JSON must be an object"),
        None => {}
    }

    if merged.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Value::Object(merged)))
    }
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

fn print_read_output(
    output: &ReadOutput,
    format: ReadOutputFormat,
    config: &OmnigraphConfig,
) -> Result<()> {
    println!(
        "{}",
        render_read(
            output,
            format,
            &ReadRenderOptions {
                max_column_width: config.table_max_column_width(),
                cell_layout: config.table_cell_layout(),
            },
        )?
    );
    Ok(())
}

fn print_change_human(output: &ChangeOutput) {
    println!(
        "changed {} via {}: {} nodes, {} edges",
        output.branch, output.query_name, output.affected_nodes, output.affected_edges
    );
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

fn resolve_read_format(
    config: &OmnigraphConfig,
    cli_format: Option<ReadOutputFormat>,
    json: bool,
    alias_format: Option<ReadOutputFormat>,
) -> ReadOutputFormat {
    if json {
        ReadOutputFormat::Json
    } else {
        cli_format
            .or(alias_format)
            .unwrap_or_else(|| config.cli_output_format())
    }
}

fn resolve_alias<'a>(
    config: &'a OmnigraphConfig,
    alias_name: Option<&'a str>,
    expected: AliasCommand,
) -> Result<Option<(&'a str, &'a omnigraph_server::AliasConfig)>> {
    let Some(alias_name) = alias_name else {
        return Ok(None);
    };
    let alias = config.alias(alias_name)?;
    if alias.command != expected {
        bail!(
            "alias '{}' is a {:?} alias, not a {:?} alias",
            alias_name,
            alias.command,
            expected
        );
    }
    Ok(Some((alias_name, alias)))
}

fn normalize_alias_args(
    uri: Option<String>,
    target: Option<&str>,
    default_target_present: bool,
    alias_name: Option<&str>,
    mut alias_args: Vec<String>,
) -> (Option<String>, Vec<String>) {
    let Some(candidate) = uri else {
        return (None, alias_args);
    };

    if alias_name.is_some()
        && (target.is_some() || default_target_present)
        && !is_remote_uri(&candidate)
        && !candidate.contains(std::path::MAIN_SEPARATOR)
        && !Path::new(&candidate).exists()
    {
        alias_args.insert(0, candidate);
        return (None, alias_args);
    }

    (Some(candidate), alias_args)
}

fn scaffold_config_if_missing(uri: &str) -> Result<()> {
    let path = inferred_config_path(uri)?;
    if path.exists() {
        return Ok(());
    }

    fs::write(
        path,
        format!(
            "\
project:
  name: Omnigraph Project

targets:
  local:
    uri: {}
    # bearer_token_env: OMNIGRAPH_BEARER_TOKEN

server:
  target: local
  bind: 127.0.0.1:8080

cli:
  target: local
  branch: main
  output_format: table
  table_max_column_width: 80
  table_cell_layout: truncate

query:
  roots:
    - queries
    - .

aliases:
  # owner:
  #   command: read
  #   query: context.gq
  #   name: decision_owner
  #   args: [slug]
  #   target: local
  #   branch: main
  #   format: kv
  #
  # attach_trace:
  #   command: change
  #   query: mutations.gq
  #   name: attach_trace
  #   args: [decision_slug, trace_slug]
  #   target: local
  #   branch: main

# auth:
#   env_file: ./.env.omni

policy: {{}}
",
            yaml_string(uri),
        ),
    )?;
    Ok(())
}

fn yaml_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn inferred_config_path(uri: &str) -> Result<PathBuf> {
    if uri.contains("://") {
        return Ok(omnigraph_server::config::default_config_path());
    }

    let path = Path::new(uri);
    let base = if path.is_absolute() {
        path.parent()
            .map(Path::to_path_buf)
            .unwrap_or(std::env::current_dir()?)
    } else {
        std::env::current_dir()?.join(path.parent().unwrap_or_else(|| Path::new(".")))
    };
    Ok(base.join(omnigraph_server::config::DEFAULT_CONFIG_FILE))
}

fn read_target_from_cli(branch: Option<String>, snapshot: Option<String>) -> ReadTarget {
    if let Some(snapshot) = snapshot {
        ReadTarget::snapshot(SnapshotId::new(snapshot))
    } else {
        ReadTarget::branch(branch.unwrap_or_else(|| "main".to_string()))
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

async fn execute_read(
    uri: &str,
    query_source: &str,
    query_name: Option<&str>,
    target: ReadTarget,
    params_json: Option<&Value>,
) -> Result<ReadOutput> {
    let (selected_name, query_params) = select_named_query(query_source, query_name)?;
    let params = query_params_from_json(&query_params, params_json)?;
    let db = Omnigraph::open(uri).await?;
    let result = db
        .query(target.clone(), query_source, &selected_name, &params)
        .await?;
    Ok(read_output(selected_name, &target, result))
}

async fn execute_read_remote(
    client: &reqwest::Client,
    uri: &str,
    query_source: &str,
    query_name: Option<&str>,
    target: ReadTarget,
    params_json: Option<&Value>,
    bearer_token: Option<&str>,
) -> Result<ReadOutput> {
    let (branch, snapshot) = match &target {
        ReadTarget::Branch(branch) => (Some(branch.clone()), None),
        ReadTarget::Snapshot(snapshot) => (None, Some(snapshot.as_str().to_string())),
    };
    remote_json(
        client,
        Method::POST,
        remote_url(uri, "/read"),
        Some(serde_json::to_value(ReadRequest {
            query_source: query_source.to_string(),
            query_name: query_name.map(ToOwned::to_owned),
            params: params_json.cloned(),
            branch,
            snapshot,
        })?),
        bearer_token,
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
    client: &reqwest::Client,
    uri: &str,
    query_source: &str,
    query_name: Option<&str>,
    branch: &str,
    params_json: Option<&Value>,
    bearer_token: Option<&str>,
) -> Result<ChangeOutput> {
    remote_json(
        client,
        Method::POST,
        remote_url(uri, "/change"),
        Some(serde_json::to_value(ChangeRequest {
            query_source: query_source.to_string(),
            query_name: query_name.map(ToOwned::to_owned),
            params: params_json.cloned(),
            branch: Some(branch.to_string()),
        })?),
        bearer_token,
    )
    .await
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = {
        let matches = Cli::command()
            .arg(
                Arg::new("version")
                    .short('v')
                    .long("version")
                    .action(ArgAction::Version)
                    .help("Print version"),
            )
            .get_matches();
        Cli::from_arg_matches(&matches)?
    };
    let http_client = build_http_client()?;
    match cli.command {
        Command::Version => {
            println!("omnigraph {}", env!("CARGO_PKG_VERSION"));
        }
        Command::Init { schema, uri } => {
            let schema_source = fs::read_to_string(&schema)?;
            ensure_local_repo_parent(&uri)?;
            Omnigraph::init(&uri, &schema_source).await?;
            scaffold_config_if_missing(&uri)?;
            println!("initialized {}", uri);
        }
        Command::Load {
            uri,
            target,
            config,
            data,
            branch,
            mode,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri = resolve_local_uri(&config, uri, target.as_deref(), "load")?;
            let branch = resolve_branch(&config, branch, None, "main");
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
                target,
                config,
                from,
                name,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri = resolve_local_uri(&config, uri, target.as_deref(), "branch create")?;
                let from = resolve_branch(&config, from, None, "main");
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
            BranchCommand::List {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri = resolve_local_uri(&config, uri, target.as_deref(), "branch list")?;
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
                target,
                config,
                source,
                into,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri = resolve_local_uri(&config, uri, target.as_deref(), "branch merge")?;
                let into = resolve_branch(&config, into, None, "main");
                let mut db = Omnigraph::open(&uri).await?;
                let outcome: CliMergeOutcome = db.branch_merge(&source, &into).await?.into();
                if json {
                    print_json(&BranchMergeOutput {
                        source: &source,
                        target: &into,
                        outcome,
                    })?;
                } else {
                    println!("merged {} into {}: {}", source, into, outcome.as_str());
                }
            }
        },
        Command::Snapshot {
            uri,
            target,
            config,
            branch,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let bearer_token =
                resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let branch = resolve_branch(&config, branch, None, "main");
            let payload = if is_remote_uri(&uri) {
                remote_json::<SnapshotOutput>(
                    &http_client,
                    Method::GET,
                    format!("{}?branch={}", remote_url(&uri, "/snapshot"), branch),
                    None,
                    bearer_token.as_deref(),
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
            RunCommand::List {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let runs = if is_remote_uri(&uri) {
                    remote_json::<RunListOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, "/runs"),
                        None,
                        bearer_token.as_deref(),
                    )
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
                target,
                config,
                run_id,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let run = if is_remote_uri(&uri) {
                    remote_json::<RunOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, &format!("/runs/{}", run_id)),
                        None,
                        bearer_token.as_deref(),
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
                target,
                config,
                run_id,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let run = if is_remote_uri(&uri) {
                    remote_json::<RunOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, &format!("/runs/{}/publish", run_id)),
                        Some(serde_json::json!({})),
                        bearer_token.as_deref(),
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
                target,
                config,
                run_id,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let run = if is_remote_uri(&uri) {
                    remote_json::<RunOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, &format!("/runs/{}/abort", run_id)),
                        Some(serde_json::json!({})),
                        bearer_token.as_deref(),
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
            target,
            config,
            alias,
            query,
            name,
            params,
            branch,
            snapshot,
            format,
            json,
            alias_args,
        } => {
            if alias.is_some() == query.is_some() {
                bail!("exactly one of --alias or --query must be provided");
            }

            let config = load_cli_config(config.as_ref())?;
            let alias = resolve_alias(&config, alias.as_deref(), AliasCommand::Read)?;
            let alias_name = alias.as_ref().map(|(name, _)| *name);
            let alias_config = alias.as_ref().map(|(_, alias)| *alias);
            let (uri, alias_args) = normalize_alias_args(
                uri,
                target.as_deref(),
                config.cli_target_name().is_some(),
                alias_name,
                alias_args,
            );
            let target_name = target
                .as_deref()
                .or_else(|| alias_config.and_then(|alias| alias.target.as_deref()));
            let bearer_token = resolve_remote_bearer_token(&config, uri.as_deref(), target_name)?;
            let uri = resolve_uri(&config, uri, target_name)?;
            let query_source = resolve_query_source(
                &config,
                query.as_ref(),
                alias_config.map(|a| a.query.as_str()),
            )?;
            let params_json = merged_params_json(
                alias_name,
                alias_config
                    .map(|alias| alias.args.as_slice())
                    .unwrap_or(&[]),
                &alias_args,
                load_params_json(&params)?,
            )?;
            let target = resolve_read_target(
                &config,
                branch,
                snapshot,
                alias_config.and_then(|alias| alias.branch.clone()),
            )?;
            let query_name = name.or_else(|| alias_config.and_then(|alias| alias.name.clone()));
            let output = if is_remote_uri(&uri) {
                execute_read_remote(
                    &http_client,
                    &uri,
                    &query_source,
                    query_name.as_deref(),
                    target,
                    params_json.as_ref(),
                    bearer_token.as_deref(),
                )
                .await?
            } else {
                execute_read(
                    &uri,
                    &query_source,
                    query_name.as_deref(),
                    target,
                    params_json.as_ref(),
                )
                .await?
            };
            let format = resolve_read_format(
                &config,
                format,
                json,
                alias_config.and_then(|alias| alias.format),
            );
            print_read_output(&output, format, &config)?;
        }
        Command::Change {
            uri,
            target,
            config,
            alias,
            query,
            name,
            params,
            branch,
            json,
            alias_args,
        } => {
            if alias.is_some() == query.is_some() {
                bail!("exactly one of --alias or --query must be provided");
            }

            let config = load_cli_config(config.as_ref())?;
            let alias = resolve_alias(&config, alias.as_deref(), AliasCommand::Change)?;
            let alias_name = alias.as_ref().map(|(name, _)| *name);
            let alias_config = alias.as_ref().map(|(_, alias)| *alias);
            let (uri, alias_args) = normalize_alias_args(
                uri,
                target.as_deref(),
                config.cli_target_name().is_some(),
                alias_name,
                alias_args,
            );
            let target_name = target
                .as_deref()
                .or_else(|| alias_config.and_then(|alias| alias.target.as_deref()));
            let bearer_token = resolve_remote_bearer_token(&config, uri.as_deref(), target_name)?;
            let uri = resolve_uri(&config, uri, target_name)?;
            let query_source = resolve_query_source(
                &config,
                query.as_ref(),
                alias_config.map(|a| a.query.as_str()),
            )?;
            let params_json = merged_params_json(
                alias_name,
                alias_config
                    .map(|alias| alias.args.as_slice())
                    .unwrap_or(&[]),
                &alias_args,
                load_params_json(&params)?,
            )?;
            let branch = resolve_branch(
                &config,
                branch,
                alias_config.and_then(|alias| alias.branch.clone()),
                "main",
            );
            let query_name = name.or_else(|| alias_config.and_then(|alias| alias.name.clone()));
            let output = if is_remote_uri(&uri) {
                execute_change_remote(
                    &http_client,
                    &uri,
                    &query_source,
                    query_name.as_deref(),
                    &branch,
                    params_json.as_ref(),
                    bearer_token.as_deref(),
                )
                .await?
            } else {
                execute_change(
                    &uri,
                    &query_source,
                    query_name.as_deref(),
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
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        DEFAULT_BEARER_TOKEN_ENV, apply_bearer_token, bearer_token_from_env_file, load_cli_config,
        load_env_file_into_process, normalize_bearer_token, parse_env_assignment,
        resolve_remote_bearer_token,
    };
    use omnigraph_server::load_config;
    use reqwest::header::AUTHORIZATION;
    use tempfile::tempdir;

    #[test]
    fn apply_bearer_token_adds_header_when_configured() {
        let client = reqwest::Client::new();
        let request = apply_bearer_token(client.get("http://example.com"), Some("demo-token"))
            .build()
            .unwrap();
        assert_eq!(
            request
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer demo-token")
        );
    }

    #[test]
    fn apply_bearer_token_leaves_request_unchanged_when_not_configured() {
        let client = reqwest::Client::new();
        let request = apply_bearer_token(client.get("http://example.com"), None)
            .build()
            .unwrap();
        assert!(request.headers().get(AUTHORIZATION).is_none());
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

    #[test]
    fn parse_env_assignment_supports_plain_and_exported_values() {
        assert_eq!(
            parse_env_assignment("DEMO_TOKEN=demo-token"),
            Some(("DEMO_TOKEN".to_string(), "demo-token".to_string()))
        );
        assert_eq!(
            parse_env_assignment("export DEMO_TOKEN=\"quoted-token\""),
            Some(("DEMO_TOKEN".to_string(), "quoted-token".to_string()))
        );
        assert_eq!(parse_env_assignment("# comment"), None);
        assert_eq!(parse_env_assignment("   "), None);
    }

    #[test]
    fn bearer_token_from_env_file_reads_named_value() {
        let temp = tempdir().unwrap();
        let env_file = temp.path().join(".env.omni");
        fs::write(
            &env_file,
            "FIRST=ignore\nexport DEMO_TOKEN=\" demo-token \"\n",
        )
        .unwrap();

        assert_eq!(
            bearer_token_from_env_file(&env_file, "DEMO_TOKEN")
                .unwrap()
                .as_deref(),
            Some("demo-token")
        );
        assert_eq!(
            bearer_token_from_env_file(&env_file, "MISSING").unwrap(),
            None
        );
    }

    #[test]
    fn load_env_file_into_process_sets_missing_values_without_overriding_existing_ones() {
        let temp = tempdir().unwrap();
        let env_file = temp.path().join(".env.omni");
        fs::write(
            &env_file,
            "AUTOLOAD_ONLY=from-file\nAUTOLOAD_PRESET=from-file\n",
        )
        .unwrap();

        let missing_key = "AUTOLOAD_ONLY";
        let preset_key = "AUTOLOAD_PRESET";
        let previous_missing = std::env::var_os(missing_key);
        let previous_preset = std::env::var_os(preset_key);

        unsafe {
            std::env::remove_var(missing_key);
            std::env::set_var(preset_key, "from-env");
        }

        load_env_file_into_process(&env_file).unwrap();

        assert_eq!(std::env::var(missing_key).unwrap(), "from-file");
        assert_eq!(std::env::var(preset_key).unwrap(), "from-env");

        unsafe {
            if let Some(value) = previous_missing {
                std::env::set_var(missing_key, value);
            } else {
                std::env::remove_var(missing_key);
            }

            if let Some(value) = previous_preset {
                std::env::set_var(preset_key, value);
            } else {
                std::env::remove_var(preset_key);
            }
        }
    }

    #[test]
    fn resolve_remote_bearer_token_uses_scoped_env_file_with_global_fallback() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            r#"
targets:
  demo:
    uri: https://example.com
    bearer_token_env: DEMO_TOKEN
auth:
  env_file: .env.omni
cli:
  target: demo
"#,
        )
        .unwrap();
        fs::write(
            temp.path().join(".env.omni"),
            "DEMO_TOKEN=scoped-token\nOMNIGRAPH_BEARER_TOKEN=global-token\n",
        )
        .unwrap();

        let previous = std::env::var_os(DEFAULT_BEARER_TOKEN_ENV);
        unsafe {
            std::env::remove_var(DEFAULT_BEARER_TOKEN_ENV);
        }

        let config_path = temp.path().join("omnigraph.yaml");
        let config = load_config(Some(&config_path)).unwrap();

        assert_eq!(
            resolve_remote_bearer_token(&config, None, Some("demo"))
                .unwrap()
                .as_deref(),
            Some("scoped-token")
        );
        assert_eq!(
            resolve_remote_bearer_token(&config, Some("https://override.example.com"), None)
                .unwrap()
                .as_deref(),
            Some("global-token")
        );

        unsafe {
            if let Some(value) = previous {
                std::env::set_var(DEFAULT_BEARER_TOKEN_ENV, value);
            } else {
                std::env::remove_var(DEFAULT_BEARER_TOKEN_ENV);
            }
        }
    }

    #[test]
    fn load_cli_config_autoloads_env_file_into_process() {
        let temp = tempdir().unwrap();
        fs::write(
            temp.path().join("omnigraph.yaml"),
            r#"
auth:
  env_file: .env.omni
targets:
  demo:
    uri: s3://bucket/prefix
"#,
        )
        .unwrap();
        fs::write(
            temp.path().join(".env.omni"),
            "AUTOLOAD_FROM_CONFIG=loaded\n",
        )
        .unwrap();

        let key = "AUTOLOAD_FROM_CONFIG";
        let previous = std::env::var_os(key);
        unsafe {
            std::env::remove_var(key);
        }

        let config_path = temp.path().join("omnigraph.yaml");
        let config = load_cli_config(Some(&config_path)).unwrap();

        assert_eq!(
            config.resolve_target_uri(None, Some("demo"), None).unwrap(),
            "s3://bucket/prefix"
        );
        assert_eq!(std::env::var(key).unwrap(), "loaded");

        unsafe {
            if let Some(value) = previous {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}
