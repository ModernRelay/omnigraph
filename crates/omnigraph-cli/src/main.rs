use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::path::PathBuf;

use clap::{Arg, ArgAction, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, bail};
use omnigraph::db::{Omnigraph, ReadTarget, RunId, SnapshotId};
use omnigraph::loader::LoadMode;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{
    JsonParamMode, ParamMap, QueryLintOutput, QueryLintQueryKind, QueryLintSchemaSource,
    QueryLintSeverity, QueryLintStatus, SchemaMigrationPlan, SchemaMigrationStep, build_catalog,
    json_params_to_param_map, lint_query_file,
};
use omnigraph_server::api::{
    BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput, BranchListOutput,
    BranchMergeOutput, BranchMergeRequest, ChangeOutput, ChangeRequest, CommitListOutput,
    CommitOutput, ErrorOutput, ExportRequest, IngestOutput, IngestRequest, ReadOutput, ReadRequest,
    RunListOutput, RunOutput, SchemaApplyOutput, SchemaApplyRequest, SchemaGetOutput, SnapshotOutput,
    SnapshotTableOutput, commit_output, ingest_output, read_output, run_output,
    schema_apply_output, snapshot_payload,
};
use omnigraph_server::{
    AliasCommand, OmnigraphConfig, PolicyAction, PolicyDecision, PolicyEngine, PolicyRequest,
    PolicyTestConfig, ReadOutputFormat, load_config,
};
use reqwest::Method;
use reqwest::header::AUTHORIZATION;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

mod embed;
mod read_format;

use embed::{EmbedArgs, EmbedOutput, execute_embed};
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
    /// Generate, clean, or refresh explicit seed embeddings
    Embed(EmbedArgs),
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
    /// Ingest data into a reviewable named branch
    Ingest {
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
        #[arg(long)]
        from: Option<String>,
        #[arg(long, default_value = "merge")]
        mode: CliLoadMode,
        #[arg(long)]
        json: bool,
    },
    /// Branch operations
    Branch {
        #[command(subcommand)]
        command: BranchCommand,
    },
    /// Schema planning operations
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Query validation and linting
    Query {
        #[command(subcommand)]
        command: QueryCommand,
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
    /// Export a full graph snapshot as JSONL
    Export {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long, hide = true)]
        jsonl: bool,
        #[arg(long = "type")]
        type_names: Vec<String>,
        #[arg(long = "table")]
        table_keys: Vec<String>,
    },
    /// Run operations
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    /// Commit history operations
    Commit {
        #[command(subcommand)]
        command: CommitCommand,
    },
    /// Execute a read query against a branch or snapshot
    Read {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(hide = true)]
        legacy_uri: Option<String>,
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
        #[arg(long)]
        uri: Option<String>,
        #[arg(hide = true)]
        legacy_uri: Option<String>,
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
    /// Policy administration and diagnostics
    Policy {
        #[command(subcommand)]
        command: PolicyCommand,
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
    /// Delete a branch
    Delete {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        name: String,
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
enum SchemaCommand {
    /// Plan a schema migration against the accepted persisted schema
    Plan {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Apply a supported schema migration
    Apply {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Get the current accepted schema source
    Get {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum QueryCommand {
    /// Validate queries and report higher-level drift warnings
    #[command(visible_alias = "check")]
    Lint {
        /// Repo URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        query: PathBuf,
        #[arg(long)]
        schema: Option<PathBuf>,
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

#[derive(Debug, Subcommand)]
enum CommitCommand {
    /// List graph commits
    List {
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
    /// Show a graph commit
    Show {
        /// Repo URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        commit_id: String,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
enum PolicyCommand {
    /// Validate policy YAML and compiled Cedar policy state
    Validate {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Run declarative policy tests from policy.tests.yaml
    Test {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Explain one policy decision locally
    Explain {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        actor: String,
        #[arg(long)]
        action: PolicyAction,
        #[arg(long)]
        branch: Option<String>,
        #[arg(long = "target-branch")]
        target_branch: Option<String>,
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

#[derive(Debug, Serialize)]
struct LoadOutput<'a> {
    uri: &'a str,
    branch: &'a str,
    mode: &'a str,
    nodes_loaded: usize,
    edges_loaded: usize,
    node_types_loaded: usize,
    edge_types_loaded: usize,
}

#[derive(Debug, Serialize)]
struct SchemaPlanOutput<'a> {
    uri: &'a str,
    supported: bool,
    step_count: usize,
    steps: &'a [SchemaMigrationStep],
}

fn print_schema_apply_human(output: &SchemaApplyOutput) {
    println!("schema apply for {}", output.uri);
    println!("supported: {}", if output.supported { "yes" } else { "no" });
    println!("applied: {}", if output.applied { "yes" } else { "no" });
    println!("manifest_version: {}", output.manifest_version);
    if output.steps.is_empty() {
        println!("no schema changes");
        return;
    }
    for step in &output.steps {
        println!("- {}", render_schema_plan_step(step));
    }
}

fn query_kind_label(kind: QueryLintQueryKind) -> &'static str {
    match kind {
        QueryLintQueryKind::Read => "read",
        QueryLintQueryKind::Mutation => "mutation",
    }
}

fn severity_label(severity: QueryLintSeverity) -> &'static str {
    match severity {
        QueryLintSeverity::Error => "ERROR",
        QueryLintSeverity::Warning => "WARN ",
        QueryLintSeverity::Info => "INFO ",
    }
}

fn print_query_lint_human(output: &QueryLintOutput) {
    for result in &output.results {
        match result.status {
            QueryLintStatus::Ok => {
                println!(
                    "OK    query `{}` ({})",
                    result.name,
                    query_kind_label(result.kind)
                );
            }
            QueryLintStatus::Error => {
                println!(
                    "ERROR query `{}`: {}",
                    result.name,
                    result.error.as_deref().unwrap_or("unknown error")
                );
            }
        }

        for warning in &result.warnings {
            println!("WARN  query `{}`: {}", result.name, warning);
        }
    }

    for finding in &output.findings {
        println!("{} {}", severity_label(finding.severity), finding.message);
    }

    println!(
        "INFO  Lint complete: {} queries processed ({} error(s), {} warning(s), {} info item(s))",
        output.queries_processed, output.errors, output.warnings, output.infos
    );
}

fn finish_query_lint(output: &QueryLintOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)?;
    } else {
        print_query_lint_human(output);
    }

    if output.status == QueryLintStatus::Error {
        io::stdout().flush()?;
        std::process::exit(1);
    }

    Ok(())
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

fn remote_branch_url(base: &str, branch: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(&format!("{}/", base.trim_end_matches('/')))?;
    url.path_segments_mut()
        .map_err(|_| color_eyre::eyre::eyre!("invalid remote base url"))?
        .extend(["branches", branch]);
    Ok(url.to_string())
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

fn resolve_policy_engine(config: &OmnigraphConfig) -> Result<PolicyEngine> {
    let policy_file = config
        .resolve_policy_file()
        .ok_or_else(|| color_eyre::eyre::eyre!("policy.file must be set in omnigraph.yaml"))?;
    PolicyEngine::load(&policy_file, &policy_repo_id(config))
}

fn resolve_policy_tests_path(config: &OmnigraphConfig) -> Result<PathBuf> {
    config.resolve_policy_tests_file().ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "policy.tests.yaml requires policy.file to be set in omnigraph.yaml"
        )
    })
}

fn policy_repo_id(config: &OmnigraphConfig) -> String {
    if let Some(name) = &config.project.name {
        return name.clone();
    }
    config
        .resolve_target_uri(None, None, config.server_graph_name())
        .or_else(|_| config.resolve_target_uri(None, None, config.cli_graph_name()))
        .unwrap_or_else(|_| "default".to_string())
}

fn resolve_remote_bearer_token(
    config: &OmnigraphConfig,
    explicit_uri: Option<&str>,
    explicit_target: Option<&str>,
) -> Result<Option<String>> {
    let scoped_env =
        config.graph_bearer_token_env(explicit_uri, explicit_target, config.cli_graph_name());
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
    config.resolve_target_uri(cli_uri, cli_target, config.cli_graph_name())
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

fn resolve_query_path(
    config: &OmnigraphConfig,
    explicit_query: Option<&PathBuf>,
    alias_query: Option<&str>,
) -> Result<PathBuf> {
    explicit_query
        .map(PathBuf::from)
        .or_else(|| alias_query.map(PathBuf::from))
        .ok_or_else(|| {
            color_eyre::eyre::eyre!("exactly one of --query or --alias must be provided")
        })
        .and_then(|query_path| config.resolve_query_path(&query_path))
}

fn resolve_query_source(
    config: &OmnigraphConfig,
    explicit_query: Option<&PathBuf>,
    alias_query: Option<&str>,
) -> Result<String> {
    Ok(fs::read_to_string(resolve_query_path(
        config,
        explicit_query,
        alias_query,
    )?)?)
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
    node_types_loaded: usize,
    edge_types_loaded: usize,
) {
    println!(
        "loaded {} on branch {} with {}: {} nodes across {} node types, {} edges across {} edge types",
        uri,
        branch,
        mode.as_str(),
        nodes_loaded,
        node_types_loaded,
        edges_loaded,
        edge_types_loaded
    );
}

fn print_ingest_human(output: &IngestOutput) {
    println!(
        "ingested {} into branch {} from {} with {} ({})",
        output.uri,
        output.branch,
        output.base_branch,
        output.mode.as_str(),
        if output.branch_created {
            "branch created"
        } else {
            "branch exists"
        }
    );
    for table in &output.tables {
        println!("{} rows_loaded={}", table.table_key, table.rows_loaded);
    }
    if let Some(actor_id) = &output.actor_id {
        println!("actor_id: {}", actor_id);
    }
}

fn print_schema_plan_human(uri: &str, plan: &SchemaMigrationPlan) {
    println!("schema plan for {}", uri);
    println!("supported: {}", if plan.supported { "yes" } else { "no" });
    if plan.steps.is_empty() {
        println!("no schema changes");
        return;
    }
    for step in &plan.steps {
        println!("- {}", render_schema_plan_step(step));
    }
}

fn render_schema_plan_step(step: &SchemaMigrationStep) -> String {
    match step {
        SchemaMigrationStep::AddType { type_kind, name } => {
            format!("add {} type '{}'", schema_type_kind_label(*type_kind), name)
        }
        SchemaMigrationStep::RenameType {
            type_kind,
            from,
            to,
        } => format!(
            "rename {} type '{}' -> '{}'",
            schema_type_kind_label(*type_kind),
            from,
            to
        ),
        SchemaMigrationStep::AddProperty {
            type_kind,
            type_name,
            property_name,
            property_type,
        } => format!(
            "add property '{}.{}' ({}) on {} '{}'",
            type_name,
            property_name,
            render_prop_type(property_type),
            schema_type_kind_label(*type_kind),
            type_name
        ),
        SchemaMigrationStep::RenameProperty {
            type_kind,
            type_name,
            from,
            to,
        } => format!(
            "rename property '{}.{}' -> '{}.{}' on {} '{}'",
            type_name,
            from,
            type_name,
            to,
            schema_type_kind_label(*type_kind),
            type_name
        ),
        SchemaMigrationStep::AddConstraint {
            type_kind,
            type_name,
            constraint,
        } => format!(
            "add constraint {} on {} '{}'",
            render_constraint(constraint),
            schema_type_kind_label(*type_kind),
            type_name
        ),
        SchemaMigrationStep::UpdateTypeMetadata {
            type_kind,
            name,
            annotations,
        } => format!(
            "update metadata on {} '{}' ({})",
            schema_type_kind_label(*type_kind),
            name,
            render_annotations(annotations)
        ),
        SchemaMigrationStep::UpdatePropertyMetadata {
            type_kind,
            type_name,
            property_name,
            annotations,
        } => format!(
            "update metadata on property '{}.{}' of {} '{}' ({})",
            type_name,
            property_name,
            schema_type_kind_label(*type_kind),
            type_name,
            render_annotations(annotations)
        ),
        SchemaMigrationStep::UnsupportedChange { entity, reason } => {
            format!("unsupported change on {}: {}", entity, reason)
        }
    }
}

fn schema_type_kind_label(kind: omnigraph_compiler::SchemaTypeKind) -> &'static str {
    match kind {
        omnigraph_compiler::SchemaTypeKind::Interface => "interface",
        omnigraph_compiler::SchemaTypeKind::Node => "node",
        omnigraph_compiler::SchemaTypeKind::Edge => "edge",
    }
}

fn render_prop_type(prop_type: &omnigraph_compiler::PropType) -> String {
    let base = if let Some(values) = &prop_type.enum_values {
        format!("Enum({})", values.join("|"))
    } else {
        prop_type.scalar.to_string()
    };
    let base = if prop_type.list {
        format!("[{}]", base)
    } else {
        base
    };
    if prop_type.nullable {
        format!("{}?", base)
    } else {
        base
    }
}

fn render_constraint(constraint: &omnigraph_compiler::schema::ast::Constraint) -> String {
    match constraint {
        omnigraph_compiler::schema::ast::Constraint::Key(columns) => {
            format!("@key({})", columns.join(", "))
        }
        omnigraph_compiler::schema::ast::Constraint::Unique(columns) => {
            format!("@unique({})", columns.join(", "))
        }
        omnigraph_compiler::schema::ast::Constraint::Index(columns) => {
            format!("@index({})", columns.join(", "))
        }
        omnigraph_compiler::schema::ast::Constraint::Range { property, min, max } => {
            format!("@range({}, {:?}, {:?})", property, min, max)
        }
        omnigraph_compiler::schema::ast::Constraint::Check { property, pattern } => {
            format!("@check({}, {:?})", property, pattern)
        }
    }
}

fn render_annotations(annotations: &[omnigraph_compiler::schema::ast::Annotation]) -> String {
    annotations
        .iter()
        .map(|annotation| match &annotation.value {
            Some(value) => format!("@{}({})", annotation.name, value),
            None => format!("@{}", annotation.name),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_embed_human(output: &EmbedOutput) {
    println!(
        "embedded {} rows (selected {}, cleaned {}) from {} -> {} [{} {}d]",
        output.embedded_rows,
        output.selected_rows,
        output.cleaned_rows,
        output.input,
        output.output,
        output.mode,
        output.dimension
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
    if let Some(actor_id) = &output.actor_id {
        println!("actor_id: {}", actor_id);
    }
}

fn print_run_list_human(runs: &[RunOutput]) {
    for run in runs {
        println!(
            "{} {} target={} branch={}{}",
            run.run_id,
            run.status,
            run.target_branch,
            run.run_branch,
            run.actor_id
                .as_deref()
                .map(|actor| format!(" actor={}", actor))
                .unwrap_or_default()
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
    if let Some(actor_id) = &run.actor_id {
        println!("actor_id: {}", actor_id);
    }
    if let Some(operation_hash) = &run.operation_hash {
        println!("operation_hash: {}", operation_hash);
    }
    if let Some(snapshot_id) = &run.published_snapshot_id {
        println!("published_snapshot_id: {}", snapshot_id);
    }
    println!("created_at: {}", run.created_at);
    println!("updated_at: {}", run.updated_at);
}

fn print_commit_list_human(commits: &[CommitOutput]) {
    for commit in commits {
        let branch = commit.manifest_branch.as_deref().unwrap_or("main");
        println!(
            "{} branch={} version={}{}",
            commit.graph_commit_id,
            branch,
            commit.manifest_version,
            commit
                .actor_id
                .as_deref()
                .map(|actor| format!(" actor={}", actor))
                .unwrap_or_default()
        );
    }
}

fn print_commit_human(commit: &CommitOutput) {
    println!("graph_commit_id: {}", commit.graph_commit_id);
    println!(
        "manifest_branch: {}",
        commit.manifest_branch.as_deref().unwrap_or("main")
    );
    println!("manifest_version: {}", commit.manifest_version);
    if let Some(parent_commit_id) = &commit.parent_commit_id {
        println!("parent_commit_id: {}", parent_commit_id);
    }
    if let Some(merged_parent_commit_id) = &commit.merged_parent_commit_id {
        println!("merged_parent_commit_id: {}", merged_parent_commit_id);
    }
    if let Some(actor_id) = &commit.actor_id {
        println!("actor_id: {}", actor_id);
    }
    println!("created_at: {}", commit.created_at);
}

fn print_policy_explain(decision: &PolicyDecision, request: &PolicyRequest) {
    println!(
        "decision: {}",
        if decision.allowed { "allow" } else { "deny" }
    );
    println!("actor: {}", request.actor_id);
    println!("action: {}", request.action);
    if let Some(branch) = &request.branch {
        println!("branch: {}", branch);
    }
    if let Some(target_branch) = &request.target_branch {
        println!("target_branch: {}", target_branch);
    }
    if let Some(rule_id) = &decision.matched_rule_id {
        println!("matched_rule: {}", rule_id);
    }
    println!("message: {}", decision.message);
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

fn normalize_legacy_alias_uri(
    uri: Option<String>,
    target_available: bool,
    alias_name: Option<&str>,
    mut alias_args: Vec<String>,
) -> (Option<String>, Vec<String>) {
    let Some(candidate) = uri else {
        return (None, alias_args);
    };

    if alias_name.is_some() && target_available {
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

graphs:
  local:
    uri: {}
    # bearer_token_env: OMNIGRAPH_BEARER_TOKEN

server:
  graph: local
  bind: 127.0.0.1:8080

cli:
  graph: local
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
  #   graph: local
  #   branch: main
  #   format: kv
  #
  # attach_trace:
  #   command: change
  #   query: mutations.gq
  #   name: attach_trace
  #   args: [decision_slug, trace_slug]
  #   graph: local
  #   branch: main

# auth:
#   env_file: ./.env.omni
#
# policy:
#   file: ./policy.yaml
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

async fn execute_query_lint(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
    schema_path: Option<&PathBuf>,
    query_path: &PathBuf,
) -> Result<QueryLintOutput> {
    let resolved_query_path = resolve_query_path(config, Some(query_path), None)?;
    let query_source = fs::read_to_string(&resolved_query_path)?;
    let query_path = resolved_query_path.to_string_lossy().into_owned();

    if let Some(schema_path) = schema_path {
        let schema_source = fs::read_to_string(schema_path)?;
        let schema =
            parse_schema(&schema_source).map_err(|err| color_eyre::eyre::eyre!(err.to_string()))?;
        let catalog =
            build_catalog(&schema).map_err(|err| color_eyre::eyre::eyre!(err.to_string()))?;
        return Ok(lint_query_file(
            &catalog,
            &query_source,
            query_path,
            QueryLintSchemaSource::file(schema_path.to_string_lossy().into_owned()),
        ));
    }

    let has_repo_target =
        cli_uri.is_some() || cli_target.is_some() || config.cli_graph_name().is_some();
    if !has_repo_target {
        bail!("query lint requires --schema <schema.pg> or a resolvable repo target");
    }

    let uri = resolve_local_uri(config, cli_uri, cli_target, "query lint")?;
    let db = Omnigraph::open(&uri).await?;
    Ok(lint_query_file(
        db.catalog(),
        &query_source,
        query_path,
        QueryLintSchemaSource::repo(uri),
    ))
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
        actor_id: None,
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

async fn execute_export_to_writer<W: Write>(
    uri: &str,
    branch: &str,
    type_names: &[String],
    table_keys: &[String],
    writer: &mut W,
) -> Result<()> {
    let db = Omnigraph::open(uri).await?;
    db.export_jsonl_to_writer(branch, type_names, table_keys, writer)
        .await?;
    writer.flush()?;
    Ok(())
}

async fn execute_export_remote_to_writer<W: Write>(
    client: &reqwest::Client,
    uri: &str,
    branch: &str,
    type_names: &[String],
    table_keys: &[String],
    bearer_token: Option<&str>,
    writer: &mut W,
) -> Result<()> {
    let request = apply_bearer_token(
        client.request(Method::POST, remote_url(uri, "/export")),
        bearer_token,
    )
    .json(&ExportRequest {
        branch: Some(branch.to_string()),
        type_names: type_names.to_vec(),
        table_keys: table_keys.to_vec(),
    });
    let mut response = request.send().await?;
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await?;
        if let Ok(error) = serde_json::from_str::<ErrorOutput>(&text) {
            bail!(error.error);
        }
        bail!("server returned {}: {}", status, text);
    }

    while let Some(chunk) = response.chunk().await? {
        writer.write_all(&chunk)?;
    }
    writer.flush()?;
    Ok(())
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
        Command::Embed(args) => {
            let output = execute_embed(&args).await?;
            if args.json {
                print_json(&output)?;
            } else {
                print_embed_human(&output);
            }
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
                nodes_loaded: result.nodes_loaded.values().sum(),
                edges_loaded: result.edges_loaded.values().sum(),
                node_types_loaded: result.nodes_loaded.len(),
                edge_types_loaded: result.edges_loaded.len(),
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
                    payload.node_types_loaded,
                    payload.edge_types_loaded,
                );
            }
        }
        Command::Ingest {
            uri,
            target,
            config,
            data,
            branch,
            from,
            mode,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let bearer_token =
                resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let branch = resolve_branch(&config, branch, None, "main");
            let from = resolve_branch(&config, from, None, "main");
            let payload = if is_remote_uri(&uri) {
                let data = fs::read_to_string(&data)?;
                remote_json::<IngestOutput>(
                    &http_client,
                    Method::POST,
                    remote_url(&uri, "/ingest"),
                    Some(serde_json::to_value(IngestRequest {
                        branch: Some(branch.clone()),
                        from: Some(from.clone()),
                        mode: Some(mode.into()),
                        data,
                    })?),
                    bearer_token.as_deref(),
                )
                .await?
            } else {
                let mut db = Omnigraph::open(&uri).await?;
                let result = db
                    .ingest_file(&branch, Some(&from), &data.to_string_lossy(), mode.into())
                    .await?;
                ingest_output(&uri, &result, None)
            };
            if json {
                print_json(&payload)?;
            } else {
                print_ingest_human(&payload);
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
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let from = resolve_branch(&config, from, None, "main");
                let payload = if is_remote_uri(&uri) {
                    remote_json::<BranchCreateOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, "/branches"),
                        Some(serde_json::to_value(BranchCreateRequest {
                            from: Some(from.clone()),
                            name: name.clone(),
                        })?),
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let mut db = Omnigraph::open(&uri).await?;
                    db.branch_create_from(ReadTarget::branch(&from), &name)
                        .await?;
                    BranchCreateOutput {
                        uri: uri.clone(),
                        from: from.clone(),
                        name: name.clone(),
                        actor_id: None,
                    }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    println!("created branch {} from {}", payload.name, payload.from);
                }
            }
            BranchCommand::List {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let payload = if is_remote_uri(&uri) {
                    remote_json::<BranchListOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, "/branches"),
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    let mut branches = db.branch_list().await?;
                    branches.sort();
                    BranchListOutput { branches }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    for branch in payload.branches {
                        println!("{}", branch);
                    }
                }
            }
            BranchCommand::Delete {
                uri,
                target,
                config,
                name,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let payload = if is_remote_uri(&uri) {
                    remote_json::<BranchDeleteOutput>(
                        &http_client,
                        Method::DELETE,
                        remote_branch_url(&uri, &name)?,
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let mut db = Omnigraph::open(&uri).await?;
                    db.branch_delete(&name).await?;
                    BranchDeleteOutput {
                        uri: uri.clone(),
                        name: name.clone(),
                        actor_id: None,
                    }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    println!("deleted branch {}", payload.name);
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
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let into = resolve_branch(&config, into, None, "main");
                let payload = if is_remote_uri(&uri) {
                    remote_json::<BranchMergeOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, "/branches/merge"),
                        Some(serde_json::to_value(BranchMergeRequest {
                            source: source.clone(),
                            target: Some(into.clone()),
                        })?),
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let mut db = Omnigraph::open(&uri).await?;
                    let outcome = db.branch_merge(&source, &into).await?;
                    BranchMergeOutput {
                        source: source.clone(),
                        target: into.clone(),
                        outcome: outcome.into(),
                        actor_id: None,
                    }
                };
                if json {
                    print_json(&payload)?;
                } else {
                    println!(
                        "merged {} into {}: {}",
                        payload.source,
                        payload.target,
                        payload.outcome.as_str()
                    );
                }
            }
        },
        Command::Commit { command } => match command {
            CommitCommand::List {
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
                let commits = if is_remote_uri(&uri) {
                    remote_json::<CommitListOutput>(
                        &http_client,
                        Method::GET,
                        if let Some(branch) = branch.as_deref() {
                            format!("{}?branch={}", remote_url(&uri, "/commits"), branch)
                        } else {
                            remote_url(&uri, "/commits")
                        },
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                    .commits
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    db.list_commits(branch.as_deref())
                        .await?
                        .iter()
                        .map(commit_output)
                        .collect::<Vec<_>>()
                };
                if json {
                    print_json(&CommitListOutput { commits })?;
                } else {
                    print_commit_list_human(&commits);
                }
            }
            CommitCommand::Show {
                uri,
                target,
                config,
                commit_id,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let commit = if is_remote_uri(&uri) {
                    remote_json::<CommitOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, &format!("/commits/{}", commit_id)),
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    commit_output(&db.get_commit(&commit_id).await?)
                };
                if json {
                    print_json(&commit)?;
                } else {
                    print_commit_human(&commit);
                }
            }
        },
        Command::Schema { command } => match command {
            SchemaCommand::Plan {
                uri,
                target,
                config,
                schema,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri = resolve_local_uri(&config, uri, target.as_deref(), "schema plan")?;
                let schema_source = fs::read_to_string(&schema)?;
                let db = Omnigraph::open(&uri).await?;
                let plan = db.plan_schema(&schema_source).await?;
                let output = SchemaPlanOutput {
                    uri: &uri,
                    supported: plan.supported,
                    step_count: plan.steps.len(),
                    steps: &plan.steps,
                };
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_plan_human(&uri, &plan);
                }
            }
            SchemaCommand::Apply {
                uri,
                target,
                config,
                schema,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let schema_source = fs::read_to_string(&schema)?;
                let output = if is_remote_uri(&uri) {
                    remote_json::<SchemaApplyOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, "/schema/apply"),
                        Some(serde_json::to_value(SchemaApplyRequest {
                            schema_source: schema_source.clone(),
                        })?),
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let mut db = Omnigraph::open(&uri).await?;
                    schema_apply_output(&uri, db.apply_schema(&schema_source).await?)
                };
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_apply_human(&output);
                }
            }
            SchemaCommand::Get {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                let output = if is_remote_uri(&uri) {
                    remote_json::<SchemaGetOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, "/schema"),
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    SchemaGetOutput {
                        source: db.schema_source().to_string(),
                    }
                };
                if json {
                    print_json(&output)?;
                } else {
                    print!("{}", output.source);
                }
            }
        },
        Command::Query { command } => match command {
            QueryCommand::Lint {
                uri,
                target,
                config,
                query,
                schema,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let output =
                    execute_query_lint(&config, uri, target.as_deref(), schema.as_ref(), &query)
                        .await?;
                finish_query_lint(&output, json)?;
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
        Command::Export {
            uri,
            target,
            config,
            branch,
            jsonl,
            type_names,
            table_keys,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let bearer_token =
                resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let branch = resolve_branch(&config, branch, None, "main");
            if jsonl {
                eprintln!("warning: --jsonl is deprecated; `omnigraph export` always emits JSONL");
            }

            let stdout = io::stdout();
            let mut stdout = stdout.lock();
            if is_remote_uri(&uri) {
                execute_export_remote_to_writer(
                    &http_client,
                    &uri,
                    &branch,
                    &type_names,
                    &table_keys,
                    bearer_token.as_deref(),
                    &mut stdout,
                )
                .await?;
            } else {
                execute_export_to_writer(&uri, &branch, &type_names, &table_keys, &mut stdout)
                    .await?;
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
            legacy_uri,
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
            let target_available = target.is_some()
                || alias_config
                    .and_then(|alias| alias.graph.as_deref())
                    .is_some()
                || config.cli_graph_name().is_some();
            let (legacy_uri, alias_args) =
                normalize_legacy_alias_uri(legacy_uri, target_available, alias_name, alias_args);
            let uri = uri.or(legacy_uri);
            let target_name = target
                .as_deref()
                .or_else(|| alias_config.and_then(|alias| alias.graph.as_deref()));
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
            legacy_uri,
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
            let target_available = target.is_some()
                || alias_config
                    .and_then(|alias| alias.graph.as_deref())
                    .is_some()
                || config.cli_graph_name().is_some();
            let (legacy_uri, alias_args) =
                normalize_legacy_alias_uri(legacy_uri, target_available, alias_name, alias_args);
            let uri = uri.or(legacy_uri);
            let target_name = target
                .as_deref()
                .or_else(|| alias_config.and_then(|alias| alias.graph.as_deref()));
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
        Command::Policy { command } => match command {
            PolicyCommand::Validate { config } => {
                let config = load_cli_config(config.as_ref())?;
                let engine = resolve_policy_engine(&config)?;
                let policy_file = config
                    .resolve_policy_file()
                    .expect("policy file should exist after resolve_policy_engine");
                println!(
                    "policy valid: {} [{} actors]",
                    policy_file.display(),
                    engine.known_actor_count()
                );
            }
            PolicyCommand::Test { config } => {
                let config = load_cli_config(config.as_ref())?;
                let engine = resolve_policy_engine(&config)?;
                let tests_path = resolve_policy_tests_path(&config)?;
                let tests = PolicyTestConfig::load(&tests_path)?;
                engine.run_tests(&tests)?;
                println!("policy tests passed: {} cases", tests.cases.len());
            }
            PolicyCommand::Explain {
                config,
                actor,
                action,
                branch,
                target_branch,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let engine = resolve_policy_engine(&config)?;
                let request = PolicyRequest {
                    actor_id: actor,
                    action,
                    branch,
                    target_branch,
                };
                let decision = engine.authorize(&request)?;
                print_policy_explain(&decision, &request);
            }
        },
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
graphs:
  demo:
    uri: https://example.com
    bearer_token_env: DEMO_TOKEN
auth:
  env_file: .env.omni
cli:
  graph: demo
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
graphs:
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
