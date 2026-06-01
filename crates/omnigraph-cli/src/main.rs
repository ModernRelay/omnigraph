use std::ffi::OsString;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Arg, ArgAction, Args, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, bail};
use omnigraph::db::{Omnigraph, ReadTarget, SnapshotId};
use omnigraph::loader::LoadMode;
use omnigraph::storage::normalize_root_uri;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::{
    JsonParamMode, ParamMap, QueryLintOutput, QueryLintQueryKind, QueryLintSchemaSource,
    QueryLintSeverity, QueryLintStatus, SchemaMigrationPlan, SchemaMigrationStep, build_catalog,
    json_params_to_param_map, lint_query_file,
};
use omnigraph_server::api::{
    BranchCreateOutput, BranchCreateRequest, BranchDeleteOutput, BranchListOutput,
    BranchMergeOutput, BranchMergeRequest, ChangeOutput, CommitListOutput, CommitOutput,
    ErrorOutput, ExportRequest, GraphListResponse, IngestOutput, IngestRequest, ReadOutput,
    ReadRequest, SchemaApplyOutput, SchemaApplyRequest, SchemaOutput, SnapshotOutput,
    SnapshotTableOutput, commit_output, ingest_output, read_output, schema_apply_output,
    snapshot_payload,
};
use omnigraph_server::queries::{QueryRegistry, check, format_check_breakages};
use omnigraph_server::{
    AliasCommand, OmnigraphConfig, PolicyAction, PolicyDecision, PolicyEngine, PolicyRequest,
    PolicyTestConfig, ReadOutputFormat, graph_resource_id_for_selection, load_config,
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
    /// Actor identity for direct-engine writes (MR-722). Overrides
    /// `cli.actor` from `omnigraph.yaml`. When the configured policy
    /// is in effect, Cedar evaluates this actor against the requested
    /// action and scope; with policy configured but neither this flag
    /// nor `cli.actor` set, the engine-layer footgun guard fires and
    /// the write is denied (no silent bypass). Has no effect on remote
    /// HTTP writes — those resolve their actor server-side from the
    /// bearer token.
    #[arg(long = "as", global = true, value_name = "ACTOR")]
    as_actor: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the CLI version
    Version,
    /// Generate, clean, or refresh explicit seed embeddings
    Embed(EmbedArgs),
    /// Initialize a new graph from a schema
    Init {
        #[arg(long)]
        schema: PathBuf,
        /// Graph URI (local path or s3://)
        uri: String,
        /// Overwrite existing schema artifacts at the URI. Without
        /// this flag, init refuses to touch a URI that already holds
        /// `_schema.pg`, `_schema.ir.json`, or `__schema_state.json`
        /// — closes the re-init footgun (MR-668 follow-up). With the
        /// flag, the operator opts in to destructive semantics.
        #[arg(long)]
        force: bool,
    },
    /// Load data into a graph
    Load {
        /// Graph URI
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
        /// Graph URI
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
    /// Validate queries against a schema (offline) or repo (repo-backed).
    ///
    /// Canonical name is `lint` (matches the `omnigraph_compiler::lint`
    /// module and the `OG-XXX-NNN` lint-code vocabulary). Replaces the
    /// deprecated `omnigraph query lint` / `omnigraph query check` /
    /// `omnigraph check` invocations — each is kept as an argv-level
    /// shim that prints a one-line stderr warning and rewrites to
    /// `omnigraph lint`. Aliases are deliberately *not* exposed via
    /// clap's `visible_alias` because that would advertise two
    /// equivalent canonical names, which agents emit interchangeably
    /// (see MR-981).
    Lint {
        /// Graph URI
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
    /// Operate on the server-side stored-query registry (`queries:`).
    Queries {
        #[command(subcommand)]
        command: QueriesCommand,
    },
    /// Show graph snapshot
    Snapshot {
        /// Graph URI
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
        /// Graph URI
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
    /// Commit history operations
    Commit {
        #[command(subcommand)]
        command: CommitCommand,
    },
    /// Execute a read query against a branch or snapshot.
    ///
    /// Canonical read endpoint. The previous name `omnigraph read` is
    /// kept as a visible alias and prints a one-line deprecation warning
    /// when used. Pairs with `omnigraph mutate` on the write side.
    #[command(visible_alias = "read")]
    Query {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(hide = true)]
        legacy_uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["query", "query_string"])]
        alias: Option<String>,
        #[arg(long, conflicts_with_all = ["alias", "query_string"])]
        query: Option<PathBuf>,
        /// Inline GQ source — alternative to `--query <path>` and `--alias <name>`.
        #[arg(short = 'e', long = "query-string", value_name = "GQ", conflicts_with_all = ["query", "alias"])]
        query_string: Option<String>,
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
    /// Execute a graph mutation query against a branch.
    ///
    /// Canonical mutation endpoint. The previous name `omnigraph change`
    /// is kept as a visible alias and prints a one-line deprecation
    /// warning when used. Pairs with `omnigraph query` on the read side.
    #[command(visible_alias = "change")]
    Mutate {
        /// Graph URI
        #[arg(long)]
        uri: Option<String>,
        #[arg(hide = true)]
        legacy_uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, conflicts_with_all = ["query", "query_string"])]
        alias: Option<String>,
        #[arg(long, conflicts_with_all = ["alias", "query_string"])]
        query: Option<PathBuf>,
        /// Inline GQ source — alternative to `--query <path>` and `--alias <name>`.
        #[arg(short = 'e', long = "query-string", value_name = "GQ", conflicts_with_all = ["query", "alias"])]
        query_string: Option<String>,
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
    /// Compact small Lance fragments in every table of the graph
    Optimize {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Remove old Lance versions from every table of the graph (destructive)
    Cleanup {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        /// Number of recent versions to keep per table. Either `--keep` or
        /// `--older-than` (or both) must be set.
        #[arg(long)]
        keep: Option<u32>,
        /// Only remove versions older than this duration. Accepts Go-style
        /// durations: `7d`, `24h`, `90m`. At least one of --keep / --older-than.
        #[arg(long)]
        older_than: Option<String>,
        /// Required to actually run; without it, prints what would be removed
        #[arg(long)]
        confirm: bool,
        #[arg(long)]
        json: bool,
    },
    /// Manage graphs on a multi-graph server (MR-668)
    Graphs {
        #[command(subcommand)]
        command: GraphsCommand,
    },
}

/// Operations on the graph registry of a multi-graph server (MR-668).
///
/// All operations target a remote multi-graph server URL (http:// or
/// https://). Local-URI invocations return a clear error. To add or
/// remove graphs, operators edit `omnigraph.yaml` directly and restart
/// the server — runtime mutation is not exposed in v0.6.0.
#[derive(Debug, Subcommand)]
enum GraphsCommand {
    /// List every graph registered with the multi-graph server.
    List {
        /// Remote server URL (e.g. `https://server.example.com`).
        #[arg(long)]
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
enum BranchCommand {
    /// Create a new branch
    Create {
        /// Graph URI
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
        /// Graph URI
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
        /// Graph URI
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
        /// Graph URI
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
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
        /// Show the plan as it would execute with `--allow-data-loss`.
        /// Promotes every `DropMode::Soft` step to `DropMode::Hard`
        /// so the plan output reflects the destructive intent.
        #[arg(long, default_value_t = false)]
        allow_data_loss: bool,
    },
    /// Apply a supported schema migration
    Apply {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        schema: PathBuf,
        #[arg(long)]
        json: bool,
        /// Allow destructive (data-loss) schema changes.
        ///
        /// Without this flag, drops are "soft": the column or table
        /// is removed from the current manifest version but prior
        /// versions are retained, so `snapshot_at_version(pre_drop)`
        /// can still read the dropped data until `omnigraph cleanup`
        /// runs. With this flag, drops are "hard": `cleanup_old_versions`
        /// runs on the affected datasets immediately after the apply,
        /// making the prior data unreachable.
        #[arg(long, default_value_t = false)]
        allow_data_loss: bool,
    },
    /// Show the current accepted schema source
    #[command(alias = "get")]
    Show {
        /// Graph URI
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

enum CommitCommand {
    /// List graph commits
    List {
        /// Graph URI
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
        /// Graph URI
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

#[derive(Debug, Subcommand)]
enum QueriesCommand {
    /// Type-check the stored-query registry against the live schema.
    ///
    /// Distinct from `omnigraph lint` (which lints one `.gq` file):
    /// this validates the whole `queries:` registry — opening the graph
    /// to read its schema and confirming every stored query still
    /// type-checks. Exits non-zero on any breakage.
    Validate {
        /// Graph URI
        uri: Option<String>,
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// List the registered stored queries (name, MCP exposure, params).
    List {
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        config: Option<PathBuf>,
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

fn ensure_local_graph_parent(uri: &str) -> Result<()> {
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

#[derive(Debug, Clone)]
struct ResolvedCliGraph {
    uri: String,
    selected: Option<String>,
    graph_id: String,
    policy_file: Option<PathBuf>,
    is_remote: bool,
}

impl ResolvedCliGraph {
    fn selected(&self) -> Option<&str> {
        self.selected.as_deref()
    }
}

struct ResolvedPolicyContext {
    policy_file: PathBuf,
    graph_id: String,
}

fn resolve_policy_context(config: &OmnigraphConfig) -> Result<ResolvedPolicyContext> {
    let selected = config.resolve_policy_tooling_graph_selection()?;
    let policy_file = config
        .resolve_policy_file_for(selected)
        .ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "policy.file or graphs.<name>.policy.file must be set in omnigraph.yaml"
            )
        })?;
    let graph_id = match selected {
        Some(name) => graph_resource_id_for_selection(Some(name), ""),
        None => graph_resource_id_for_selection(None, "default"),
    };
    Ok(ResolvedPolicyContext {
        policy_file,
        graph_id,
    })
}

fn resolve_policy_engine(context: &ResolvedPolicyContext) -> Result<PolicyEngine> {
    PolicyEngine::load_graph(&context.policy_file, &context.graph_id)
}

fn resolve_policy_engine_for_graph(graph: &ResolvedCliGraph) -> Result<PolicyEngine> {
    let policy_file = graph.policy_file.as_ref().ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "policy.file or graphs.<name>.policy.file must be set in omnigraph.yaml"
        )
    })?;
    PolicyEngine::load_graph(policy_file, &graph.graph_id)
}

/// Open a local graph and install the policy resolved for the same graph
/// identity that produced the URI. A named graph uses
/// `graphs.<name>.policy.file`; an explicit positional URI is anonymous and
/// uses the legacy top-level `policy.file`.
async fn open_local_db_with_policy(graph: &ResolvedCliGraph) -> Result<Omnigraph> {
    let db = Omnigraph::open(&graph.uri).await?;
    if graph.policy_file.is_some() {
        let engine = Arc::new(resolve_policy_engine_for_graph(graph)?);
        Ok(db.with_policy(engine as Arc<dyn omnigraph_policy::PolicyChecker>))
    } else {
        Ok(db)
    }
}

/// Resolve the CLI's effective actor identity for engine-layer policy
/// (MR-722). Precedence: `--as <ACTOR>` (top-level flag) overrides
/// `cli.actor` from `omnigraph.yaml`; both unset returns `None`. When
/// policy is configured and this returns `None`, the engine-layer
/// footgun guard intentionally denies — silent bypass via "I forgot the
/// actor" is what the guard prevents.
fn resolve_cli_actor<'a>(cli_as: Option<&'a str>, config: &'a OmnigraphConfig) -> Option<&'a str> {
    cli_as.or(config.cli.actor.as_deref())
}

fn resolve_policy_tests_path(context: &ResolvedPolicyContext) -> PathBuf {
    context.policy_file.with_file_name("policy.tests.yaml")
}

fn normalize_policy_graph_uri(uri: &str) -> Result<String> {
    if is_remote_uri(uri) {
        Ok(uri.trim_end_matches('/').to_string())
    } else {
        Ok(normalize_root_uri(uri)?)
    }
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

fn resolve_cli_graph(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
) -> Result<ResolvedCliGraph> {
    let selected = if cli_uri.is_some() {
        None
    } else {
        cli_target
            .map(str::to_string)
            .or_else(|| config.cli_graph_name().map(str::to_string))
    };
    config.resolve_graph_selection(selected.as_deref())?;
    let uri = resolve_uri(config, cli_uri, cli_target)?;
    let normalized_uri = normalize_policy_graph_uri(&uri)?;
    let graph_id = graph_resource_id_for_selection(selected.as_deref(), &normalized_uri);
    Ok(ResolvedCliGraph {
        graph_id,
        is_remote: is_remote_uri(&uri),
        policy_file: config.resolve_policy_file_for(selected.as_deref()),
        selected,
        uri,
    })
}

fn resolve_local_graph(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
    operation: &str,
) -> Result<ResolvedCliGraph> {
    let graph = resolve_cli_graph(config, cli_uri, cli_target)?;
    if graph.is_remote {
        bail!(
            "{} is only supported against local graph URIs in this milestone",
            operation
        );
    }
    Ok(graph)
}

/// Parse a Go-style compact duration: `7d`, `24h`, `30m`, `90s`, or a plain
/// integer as seconds. Used by the `cleanup --older-than` flag.
fn parse_duration_arg(s: &str) -> Result<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("duration is empty");
    }
    let (num_part, unit) = match s
        .char_indices()
        .rev()
        .find(|(_, c)| c.is_ascii_alphabetic())
    {
        Some((i, _)) => (
            &s[..i + 1 - s[i..].chars().next().unwrap().len_utf8()],
            &s[i..],
        ),
        None => (s, ""),
    };
    let n: u64 = num_part
        .parse()
        .map_err(|e| color_eyre::eyre::eyre!("invalid duration '{}': {}", s, e))?;
    let secs = match unit {
        "" | "s" => n,
        "m" => n * 60,
        "h" => n * 60 * 60,
        "d" => n * 60 * 60 * 24,
        "w" => n * 60 * 60 * 24 * 7,
        _ => bail!("unknown duration unit '{}'. Supported: s, m, h, d, w", unit),
    };
    Ok(std::time::Duration::from_secs(secs))
}

fn resolve_local_uri(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
    operation: &str,
) -> Result<String> {
    Ok(resolve_local_graph(config, cli_uri, cli_target, operation)?.uri)
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
            color_eyre::eyre::eyre!(
                "exactly one of --query, --query-string, or --alias must be provided"
            )
        })
        .and_then(|query_path| config.resolve_query_path(&query_path))
}

fn resolve_query_source(
    config: &OmnigraphConfig,
    explicit_query: Option<&PathBuf>,
    inline_query: Option<&str>,
    alias_query: Option<&str>,
) -> Result<String> {
    if let Some(inline) = inline_query {
        if inline.trim().is_empty() {
            bail!("--query-string must not be empty");
        }
        return Ok(inline.to_string());
    }
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
        SchemaMigrationStep::DropType {
            type_kind,
            name,
            mode,
        } => format!(
            "drop {} type '{}' ({} mode)",
            schema_type_kind_label(*type_kind),
            name,
            drop_mode_label(*mode),
        ),
        SchemaMigrationStep::DropProperty {
            type_kind,
            type_name,
            property_name,
            mode,
        } => format!(
            "drop property '{}.{}' of {} '{}' ({} mode)",
            type_name,
            property_name,
            schema_type_kind_label(*type_kind),
            type_name,
            drop_mode_label(*mode),
        ),
        SchemaMigrationStep::UnsupportedChange { entity, reason, .. } => {
            // When a schema-lint code is attached, render code + tier
            // so operators see at-a-glance the kind of risk (destructive
            // / validated / safe) — not just the rule identifier.
            // Reach the diagnostic via the `diagnostic()` helper so the
            // CLI doesn't need to know how the lookup works.
            match step.diagnostic() {
                Some(diag) => format!(
                    "unsupported change on {} [{}, {}]: {}",
                    entity,
                    diag.code,
                    schema_lint_tier_label(diag.tier),
                    reason,
                ),
                None => format!("unsupported change on {}: {}", entity, reason),
            }
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

fn schema_lint_tier_label(tier: omnigraph_compiler::SafetyTier) -> &'static str {
    match tier {
        omnigraph_compiler::SafetyTier::Safe => "safe",
        omnigraph_compiler::SafetyTier::Validated => "validated",
        omnigraph_compiler::SafetyTier::Destructive => "destructive",
    }
}

fn drop_mode_label(mode: omnigraph_compiler::DropMode) -> &'static str {
    match mode {
        omnigraph_compiler::DropMode::Soft => "soft",
        omnigraph_compiler::DropMode::Hard => "hard",
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

fn print_policy_explain(decision: &PolicyDecision, actor_id: &str, request: &PolicyRequest) {
    println!(
        "decision: {}",
        if decision.allowed { "allow" } else { "deny" }
    );
    println!("actor: {}", actor_id);
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

    let has_graph_target =
        cli_uri.is_some() || cli_target.is_some() || config.cli_graph_name().is_some();
    if !has_graph_target {
        bail!("query lint requires --schema <schema.pg> or a resolvable graph target");
    }

    let uri = resolve_local_uri(config, cli_uri, cli_target, "query lint")?;
    let db = Omnigraph::open(&uri).await?;
    Ok(lint_query_file(
        &db.catalog(),
        &query_source,
        query_path,
        QueryLintSchemaSource::graph(uri),
    ))
}

#[derive(serde::Serialize)]
struct QueriesIssue {
    query: String,
    message: String,
}

#[derive(serde::Serialize)]
struct QueriesValidateOutput {
    ok: bool,
    breakages: Vec<QueriesIssue>,
    warnings: Vec<QueriesIssue>,
}

#[derive(serde::Serialize)]
struct QueriesParam {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    nullable: bool,
}

#[derive(serde::Serialize)]
struct QueriesListItem {
    name: String,
    mcp_expose: bool,
    tool_name: Option<String>,
    mutation: bool,
    params: Vec<QueriesParam>,
}

#[derive(serde::Serialize)]
struct QueriesListOutput {
    queries: Vec<QueriesListItem>,
}

/// Resolve the selected graph to `(local URI, registry selection)` from one
/// precedence, so a command's schema and its stored-query registry can never
/// come from different graphs. A **positional URI is anonymous** (top-level
/// registry, ignoring the configured default graph); otherwise `--target`
/// or the configured `cli.graph` names the graph (its per-graph block).
/// Mirrors the server's single-mode identity rule.
fn resolve_selected_graph(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
    operation: &str,
) -> Result<(String, Option<String>)> {
    let graph = resolve_local_graph(config, cli_uri, cli_target, operation)?;
    Ok((graph.uri, graph.selected))
}

/// Load the stored-query registry for an already-resolved graph selection
/// (`None` = anonymous → top-level; `Some(name)` = that graph's block).
fn load_registry_or_report(
    config: &OmnigraphConfig,
    selected: Option<&str>,
) -> Result<QueryRegistry> {
    QueryRegistry::load(config, config.query_entries_for(selected)).map_err(|errors| {
        color_eyre::eyre::eyre!(
            "stored-query registry failed to load:\n  {}",
            errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n  ")
        )
    })
}

fn graph_query_registry_names(config: &OmnigraphConfig) -> Vec<&str> {
    config
        .graphs
        .iter()
        .filter_map(|(name, graph)| (!graph.queries.is_empty()).then_some(name.as_str()))
        .collect()
}

fn resolve_registry_selection_for_list(
    config: &OmnigraphConfig,
    target: Option<&str>,
) -> Result<Option<String>> {
    let selected = target
        .map(str::to_string)
        .or_else(|| config.cli_graph_name().map(str::to_string));
    if let Some(name) = selected.as_deref() {
        config.resolve_graph_selection(Some(name))?;
        return Ok(selected);
    }

    if !config.query_entries().is_empty() {
        return Ok(None);
    }

    let graph_names = graph_query_registry_names(config);
    if graph_names.is_empty() {
        return Ok(None);
    }

    bail!(
        "stored-query registries are configured for graph{} {} but no graph was selected. Pass `--target {}` or set `cli.graph`.",
        if graph_names.len() == 1 { "" } else { "s" },
        graph_names.join(", "),
        graph_names[0],
    )
}

fn validate_registry_for_catalog(
    registry: &QueryRegistry,
    catalog: &omnigraph_compiler::catalog::Catalog,
    label: &str,
) -> omnigraph::error::Result<()> {
    let report = check(registry, catalog);
    if report.has_breakages() {
        return Err(omnigraph::error::OmniError::manifest(
            format_check_breakages(label, &report),
        ));
    }
    Ok(())
}

async fn execute_queries_validate(
    uri: Option<String>,
    target: Option<String>,
    config_path: Option<&PathBuf>,
    json: bool,
) -> Result<()> {
    let config = load_cli_config(config_path)?;
    // One selection drives both the schema URI and the registry, so a
    // positional URI and a `--target` can't validate different graphs.
    let (uri, selected) =
        resolve_selected_graph(&config, uri, target.as_deref(), "queries validate")?;
    let registry = load_registry_or_report(&config, selected.as_deref())?;
    let db = Omnigraph::open(&uri).await?;
    let report = check(&registry, &db.catalog());

    let output = QueriesValidateOutput {
        ok: !report.has_breakages(),
        breakages: report
            .breakages
            .iter()
            .map(|b| QueriesIssue {
                query: b.query.clone(),
                message: b.message.clone(),
            })
            .collect(),
        warnings: report
            .warnings
            .iter()
            .map(|w| QueriesIssue {
                query: w.query.clone(),
                message: w.message.clone(),
            })
            .collect(),
    };

    if json {
        print_json(&output)?;
    } else {
        if output.breakages.is_empty() {
            println!(
                "OK  {} stored quer{} type-check against the schema",
                registry.len(),
                if registry.len() == 1 { "y" } else { "ies" }
            );
        }
        for issue in &output.breakages {
            println!("ERROR  query '{}': {}", issue.query, issue.message);
        }
        for issue in &output.warnings {
            println!("WARN   query '{}': {}", issue.query, issue.message);
        }
    }

    if report.has_breakages() {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

fn execute_queries_list(
    target: Option<String>,
    config_path: Option<&PathBuf>,
    json: bool,
) -> Result<()> {
    let config = load_cli_config(config_path)?;
    let selected = resolve_registry_selection_for_list(&config, target.as_deref())?;
    let registry = load_registry_or_report(&config, selected.as_deref())?;

    let output = QueriesListOutput {
        queries: registry
            .iter()
            .map(|q| QueriesListItem {
                name: q.name.clone(),
                mcp_expose: q.expose,
                tool_name: q.tool_name.clone(),
                mutation: q.is_mutation(),
                params: q
                    .decl
                    .params
                    .iter()
                    .map(|p| QueriesParam {
                        name: p.name.clone(),
                        type_name: p.type_name.clone(),
                        nullable: p.nullable,
                    })
                    .collect(),
            })
            .collect(),
    };

    if json {
        print_json(&output)?;
    } else if output.queries.is_empty() {
        println!("(no stored queries registered)");
    } else {
        for q in &output.queries {
            let kind = if q.mutation { "mutation" } else { "read" };
            let params = q
                .params
                .iter()
                .map(|p| {
                    format!(
                        "${}: {}{}",
                        p.name,
                        p.type_name,
                        if p.nullable { "?" } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let mcp = if q.mcp_expose {
                format!(" [mcp: {}]", q.tool_name.as_deref().unwrap_or(&q.name))
            } else {
                String::new()
            };
            println!("{kind}  {}({params}){mcp}", q.name);
        }
    }
    Ok(())
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
    graph: &ResolvedCliGraph,
    query_source: &str,
    query_name: Option<&str>,
    branch: &str,
    params_json: Option<&Value>,
    config: &OmnigraphConfig,
    cli_as_actor: Option<&str>,
) -> Result<ChangeOutput> {
    let (selected_name, query_params) = select_named_query(query_source, query_name)?;
    let params = query_params_from_json(&query_params, params_json)?;
    let db = open_local_db_with_policy(graph).await?;
    let actor = resolve_cli_actor(cli_as_actor, config);
    let result = db
        .mutate_as(branch, query_source, &selected_name, &params, actor)
        .await?;
    Ok(ChangeOutput {
        branch: branch.to_string(),
        query_name: selected_name,
        affected_nodes: result.affected_nodes,
        affected_edges: result.affected_edges,
        actor_id: actor.map(String::from),
    })
}

/// Build the JSON body for `POST /change` using the legacy wire shape.
///
/// `ChangeRequest`'s Rust field names are now `query` / `name` (the canonical
/// wire shape going forward), but old `omnigraph-server` builds still require
/// the legacy `query_source` / `query_name` keys on `/change`. Hand-rolling
/// the JSON with the legacy names keeps a newer CLI talking to an older
/// server intact -- the same byte-stability contract we apply to
/// `execute_read_remote` against `/read`.
fn legacy_change_request_body(
    query_source: &str,
    query_name: Option<&str>,
    branch: &str,
    params_json: Option<&Value>,
) -> Value {
    let mut body = serde_json::json!({
        "query_source": query_source,
        "branch": branch,
    });
    if let Some(name) = query_name {
        body["query_name"] = Value::String(name.to_string());
    }
    if let Some(params) = params_json {
        body["params"] = params.clone();
    }
    body
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
        Some(legacy_change_request_body(
            query_source,
            query_name,
            branch,
            params_json,
        )),
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

/// Rewrite deprecated CLI invocations into their canonical form.
///
/// The current rename pass moves four subcommands:
///   - `omnigraph read`        -> `omnigraph query`  (clap `visible_alias` handles parsing; we warn)
///   - `omnigraph change`      -> `omnigraph mutate` (clap `visible_alias` handles parsing; we warn)
///   - `omnigraph check`       -> `omnigraph lint`   (rewrite required; no visible_alias by design)
///   - `omnigraph query lint`  -> `omnigraph lint`   (rewrite required; `query` is now the read-runner)
///   - `omnigraph query check` -> `omnigraph lint`   (rewrite required)
///
/// `check` is *not* a clap visible_alias on `lint` even though they're
/// semantically equivalent. Visible aliases create two canonical names
/// that agents emit interchangeably depending on training-data drift
/// (see MR-981 §6 for the policy). The argv-shim + stderr warning
/// pattern preserves back-compat for human users while pointing every
/// caller at the single canonical name in `--help`.
///
/// Returns the (possibly rewritten) argv that clap should parse.
fn rewrite_deprecated_argv(args: Vec<OsString>) -> Vec<OsString> {
    if args.len() >= 3 {
        let sub = args[1].to_str();
        let sub2 = args[2].to_str();
        if sub == Some("query") && matches!(sub2, Some("lint") | Some("check")) {
            let suffix = sub2.unwrap();
            eprintln!(
                "warning: `omnigraph query {suffix}` is deprecated; use `omnigraph lint` instead"
            );
            // Drop the leading `query` token AND normalize `check` -> `lint`.
            // `check` is no longer a clap visible_alias (MR-981 §6), so the
            // rewritten argv must reach the canonical `lint` subcommand
            // directly. Result for `omnigraph query check --query foo.gq`:
            //   `omnigraph lint --query foo.gq`.
            let mut out = Vec::with_capacity(args.len() - 1);
            out.push(args[0].clone());
            out.push(OsString::from("lint"));
            out.extend(args[3..].iter().cloned());
            return out;
        }
    }
    if let Some(sub) = args.get(1).and_then(|s| s.to_str()) {
        match sub {
            "read" => eprintln!(
                "warning: `omnigraph read` is deprecated; use `omnigraph query` instead"
            ),
            "change" => eprintln!(
                "warning: `omnigraph change` is deprecated; use `omnigraph mutate` instead"
            ),
            "check" => {
                eprintln!(
                    "warning: `omnigraph check` is deprecated; use `omnigraph lint` instead"
                );
                // Rewrite the top-level subcommand to `lint`; pass through the rest.
                let mut out = Vec::with_capacity(args.len());
                out.push(args[0].clone());
                out.push(OsString::from("lint"));
                out.extend(args[2..].iter().cloned());
                return out;
            }
            _ => {}
        }
    }
    args
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = {
        let raw_args = rewrite_deprecated_argv(std::env::args_os().collect());
        let matches = Cli::command()
            .arg(
                Arg::new("version")
                    .short('v')
                    .long("version")
                    .action(ArgAction::Version)
                    .help("Print version"),
            )
            .get_matches_from(raw_args);
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
        Command::Init { schema, uri, force } => {
            let schema_source = fs::read_to_string(&schema)?;
            ensure_local_graph_parent(&uri)?;
            Omnigraph::init_with_options(
                &uri,
                &schema_source,
                omnigraph::db::InitOptions { force },
            )
            .await?;
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
            let graph = resolve_local_graph(&config, uri, target.as_deref(), "load")?;
            let uri = graph.uri.clone();
            let branch = resolve_branch(&config, branch, None, "main");
            let db = open_local_db_with_policy(&graph).await?;
            let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config);
            let result = db
                .load_file_as(&branch, &data.to_string_lossy(), mode.into(), actor)
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
            let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
            let uri = graph.uri.clone();
            let branch = resolve_branch(&config, branch, None, "main");
            let from = resolve_branch(&config, from, None, "main");
            let payload = if graph.is_remote {
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
                let db = open_local_db_with_policy(&graph).await?;
                let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config);
                let result = db
                    .ingest_file_as(
                        &branch,
                        Some(&from),
                        &data.to_string_lossy(),
                        mode.into(),
                        actor,
                    )
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
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let from = resolve_branch(&config, from, None, "main");
                let payload = if graph.is_remote {
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
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config);
                    db.branch_create_from_as(ReadTarget::branch(&from), &name, actor)
                        .await?;
                    BranchCreateOutput {
                        uri: uri.clone(),
                        from: from.clone(),
                        name: name.clone(),
                        actor_id: actor.map(String::from),
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
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let payload = if graph.is_remote {
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
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let payload = if graph.is_remote {
                    remote_json::<BranchDeleteOutput>(
                        &http_client,
                        Method::DELETE,
                        remote_branch_url(&uri, &name)?,
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config);
                    db.branch_delete_as(&name, actor).await?;
                    BranchDeleteOutput {
                        uri: uri.clone(),
                        name: name.clone(),
                        actor_id: actor.map(String::from),
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
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let into = resolve_branch(&config, into, None, "main");
                let payload = if graph.is_remote {
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
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config);
                    let outcome = db.branch_merge_as(&source, &into, actor).await?;
                    BranchMergeOutput {
                        source: source.clone(),
                        target: into.clone(),
                        outcome: outcome.into(),
                        actor_id: actor.map(String::from),
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
                allow_data_loss,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let uri = resolve_local_uri(&config, uri, target.as_deref(), "schema plan")?;
                let schema_source = fs::read_to_string(&schema)?;
                let db = Omnigraph::open(&uri).await?;
                let plan = db
                    .plan_schema_with_options(
                        &schema_source,
                        omnigraph::db::SchemaApplyOptions { allow_data_loss },
                    )
                    .await?;
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
                allow_data_loss,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let graph = resolve_cli_graph(&config, uri, target.as_deref())?;
                let uri = graph.uri.clone();
                let schema_source = fs::read_to_string(&schema)?;
                let output = if graph.is_remote {
                    // MR-694 PR B: SchemaApplyRequest gained an
                    // allow_data_loss field so Hard-mode drops are no
                    // longer CLI-only. The previous bail is gone; the
                    // field is forwarded into the JSON payload, and
                    // the server's `server_schema_apply` honors it.
                    remote_json::<SchemaApplyOutput>(
                        &http_client,
                        Method::POST,
                        remote_url(&uri, "/schema/apply"),
                        Some(serde_json::to_value(SchemaApplyRequest {
                            schema_source: schema_source.clone(),
                            allow_data_loss,
                        })?),
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = open_local_db_with_policy(&graph).await?;
                    let actor = resolve_cli_actor(cli.as_actor.as_deref(), &config);
                    let registry = load_registry_or_report(&config, graph.selected())?;
                    let registry = (!registry.is_empty()).then_some(registry);
                    let label = graph.selected().unwrap_or(&uri).to_string();
                    let result = db
                        .apply_schema_as_with_catalog_check(
                            &schema_source,
                            omnigraph::db::SchemaApplyOptions { allow_data_loss },
                            actor,
                            |catalog| {
                                if let Some(registry) = registry.as_ref() {
                                    validate_registry_for_catalog(registry, catalog, &label)?;
                                }
                                Ok(())
                            },
                        )
                        .await?;
                    schema_apply_output(&uri, result)
                };
                if json {
                    print_json(&output)?;
                } else {
                    print_schema_apply_human(&output);
                }
            }
            SchemaCommand::Show {
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
                    remote_json::<SchemaOutput>(
                        &http_client,
                        Method::GET,
                        remote_url(&uri, "/schema"),
                        None,
                        bearer_token.as_deref(),
                    )
                    .await?
                } else {
                    let db = Omnigraph::open(&uri).await?;
                    SchemaOutput {
                        schema_source: db.schema_source().to_string(),
                    }
                };
                if json {
                    print_json(&output)?;
                } else {
                    println!("{}", output.schema_source);
                }
            }
        },
        Command::Lint {
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
        Command::Queries { command } => match command {
            QueriesCommand::Validate {
                uri,
                target,
                config,
                json,
            } => {
                execute_queries_validate(uri, target, config.as_ref(), json).await?;
            }
            QueriesCommand::List {
                target,
                config,
                json,
            } => {
                execute_queries_list(target, config.as_ref(), json)?;
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
        Command::Query {
            uri,
            legacy_uri,
            target,
            config,
            alias,
            query,
            query_string,
            name,
            params,
            branch,
            snapshot,
            format,
            json,
            alias_args,
        } => {
            if alias.is_none() && query.is_none() && query_string.is_none() {
                bail!("exactly one of --query, --query-string, or --alias must be provided");
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
            let graph = resolve_cli_graph(&config, uri, target_name)?;
            let uri = graph.uri.clone();
            let query_source = resolve_query_source(
                &config,
                query.as_ref(),
                query_string.as_deref(),
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
            let output = if graph.is_remote {
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
        Command::Mutate {
            uri,
            legacy_uri,
            target,
            config,
            alias,
            query,
            query_string,
            name,
            params,
            branch,
            json,
            alias_args,
        } => {
            if alias.is_none() && query.is_none() && query_string.is_none() {
                bail!("exactly one of --query, --query-string, or --alias must be provided");
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
            let graph = resolve_cli_graph(&config, uri, target_name)?;
            let uri = graph.uri.clone();
            let query_source = resolve_query_source(
                &config,
                query.as_ref(),
                query_string.as_deref(),
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
            let output = if graph.is_remote {
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
                    &graph,
                    &query_source,
                    query_name.as_deref(),
                    &branch,
                    params_json.as_ref(),
                    &config,
                    cli.as_actor.as_deref(),
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
                let context = resolve_policy_context(&config)?;
                let engine = resolve_policy_engine(&context)?;
                println!(
                    "policy valid: {} [{} actors]",
                    context.policy_file.display(),
                    engine.known_actor_count()
                );
            }
            PolicyCommand::Test { config } => {
                let config = load_cli_config(config.as_ref())?;
                let context = resolve_policy_context(&config)?;
                let engine = resolve_policy_engine(&context)?;
                let tests_path = resolve_policy_tests_path(&context);
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
                let context = resolve_policy_context(&config)?;
                let engine = resolve_policy_engine(&context)?;
                let request = PolicyRequest {
                    action,
                    branch,
                    target_branch,
                };
                let decision = engine.authorize(&actor, &request)?;
                print_policy_explain(&decision, &actor, &request);
            }
        },
        Command::Optimize {
            uri,
            target,
            config,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;
            let db = Omnigraph::open(&uri).await?;
            let stats = db.optimize().await?;
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "tables": stats.iter().map(|s| serde_json::json!({
                        "table_key": s.table_key,
                        "fragments_removed": s.fragments_removed,
                        "fragments_added": s.fragments_added,
                        "committed": s.committed,
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                println!("optimize {} — {} tables", uri, stats.len());
                for s in &stats {
                    if s.committed {
                        println!(
                            "  {:<40} frags {} → {} ✓",
                            s.table_key,
                            s.fragments_removed + s.fragments_added - s.fragments_added,
                            s.fragments_added
                        );
                    } else {
                        println!("  {:<40} no-op", s.table_key);
                    }
                }
            }
        }
        Command::Cleanup {
            uri,
            target,
            config,
            keep,
            older_than,
            confirm,
            json,
        } => {
            let config = load_cli_config(config.as_ref())?;
            let uri = resolve_uri(&config, uri, target.as_deref())?;

            let older_than_dur = older_than.as_deref().map(parse_duration_arg).transpose()?;

            if keep.is_none() && older_than_dur.is_none() {
                bail!("cleanup requires at least one of --keep or --older-than");
            }

            let policy_desc = match (keep, older_than_dur) {
                (Some(k), Some(d)) => {
                    format!("keep {} versions, remove anything older than {:?}", k, d)
                }
                (Some(k), None) => format!("keep {} versions", k),
                (None, Some(d)) => format!("remove anything older than {:?}", d),
                _ => unreachable!(),
            };

            if !confirm {
                eprintln!(
                    "cleanup is destructive — rerun with --confirm. Policy for {}: {}",
                    uri, policy_desc
                );
                return Ok(());
            }

            let options = omnigraph::db::CleanupPolicyOptions {
                keep_versions: keep,
                older_than: older_than_dur,
            };

            let mut db = Omnigraph::open(&uri).await?;
            let stats = db.cleanup(options).await?;
            if json {
                let value = serde_json::json!({
                    "uri": uri,
                    "keep_versions": keep,
                    "older_than_secs": older_than_dur.map(|d| d.as_secs()),
                    "tables": stats.iter().map(|s| serde_json::json!({
                        "table_key": s.table_key,
                        "bytes_removed": s.bytes_removed,
                        "old_versions_removed": s.old_versions_removed,
                        "error": s.error,
                    })).collect::<Vec<_>>(),
                });
                print_json(&value)?;
            } else {
                let total_bytes: u64 = stats.iter().map(|s| s.bytes_removed).sum();
                let total_versions: u64 = stats.iter().map(|s| s.old_versions_removed).sum();
                let failed: Vec<&str> = stats
                    .iter()
                    .filter(|s| s.error.is_some())
                    .map(|s| s.table_key.as_str())
                    .collect();
                println!(
                    "cleanup {} ({}) — removed {} versions ({} bytes) across {} tables",
                    uri,
                    policy_desc,
                    total_versions,
                    total_bytes,
                    stats.len() - failed.len()
                );
                if !failed.is_empty() {
                    println!(
                        "  {} table(s) failed and will be retried on the next cleanup: {}",
                        failed.len(),
                        failed.join(", ")
                    );
                }
            }
        }
        Command::Graphs { command } => match command {
            GraphsCommand::List {
                uri,
                target,
                config,
                json,
            } => {
                let config = load_cli_config(config.as_ref())?;
                let bearer_token =
                    resolve_remote_bearer_token(&config, uri.as_deref(), target.as_deref())?;
                let uri = resolve_uri(&config, uri, target.as_deref())?;
                if !is_remote_uri(&uri) {
                    bail!(
                        "`omnigraph graphs list` requires a remote multi-graph server URL \
                         (http:// or https://). To enumerate local graphs, read `omnigraph.yaml` \
                         directly."
                    );
                }
                let payload = remote_json::<GraphListResponse>(
                    &http_client,
                    Method::GET,
                    remote_url(&uri, "/graphs"),
                    None,
                    bearer_token.as_deref(),
                )
                .await?;
                if json {
                    print_json(&payload)?;
                } else {
                    for entry in payload.graphs {
                        println!("{}\t{}", entry.graph_id, entry.uri);
                    }
                }
            }
        },
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        DEFAULT_BEARER_TOKEN_ENV, apply_bearer_token, bearer_token_from_env_file,
        legacy_change_request_body, load_cli_config, load_env_file_into_process,
        normalize_bearer_token, parse_env_assignment, resolve_policy_context,
        resolve_cli_graph, resolve_remote_bearer_token,
    };
    use omnigraph_server::load_config;
    use reqwest::header::AUTHORIZATION;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn legacy_change_request_body_uses_legacy_field_names() {
        // `execute_change_remote` hits `POST /change`, which old
        // `omnigraph-server` builds deserialize as `ChangeRequest` with
        // **required** `query_source` and optional `query_name` keys.
        // Newer servers accept both spellings via serde alias, but a
        // newer CLI must still emit the legacy keys on the wire so it
        // can talk to an old server during a rolling upgrade.
        let body = legacy_change_request_body(
            "query insert_person($n: String) { insert Person { name: $n } }",
            Some("insert_person"),
            "main",
            Some(&json!({ "n": "Alice" })),
        );
        assert_eq!(
            body["query_source"].as_str(),
            Some("query insert_person($n: String) { insert Person { name: $n } }"),
        );
        assert_eq!(body["query_name"].as_str(), Some("insert_person"));
        assert_eq!(body["branch"].as_str(), Some("main"));
        assert_eq!(body["params"]["n"].as_str(), Some("Alice"));
        // Crucially, the **new** field names must NOT appear -- old
        // servers would silently treat them as unknown fields and then
        // fail on missing required `query_source`.
        assert!(
            body.get("query").is_none(),
            "legacy /change body must not carry the renamed `query` key; got {body}"
        );
        assert!(
            body.get("name").is_none(),
            "legacy /change body must not carry the renamed `name` key; got {body}"
        );
    }

    #[test]
    fn legacy_change_request_body_omits_optional_fields_when_unset() {
        let body = legacy_change_request_body(
            "query find() { match { $p: Person } return { $p.name } }",
            None,
            "main",
            None,
        );
        assert_eq!(body["branch"].as_str(), Some("main"));
        assert!(body.get("query_name").is_none());
        assert!(body.get("params").is_none());
    }

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

    #[test]
    fn graph_identity_resolve_policy_context_named_cli_graph_uses_graph_key_not_project_name_or_uri() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/local-policy-graph.omni
    policy:
      file: ./policy.yaml
cli:
  graph: local
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let context = resolve_policy_context(&config).unwrap();
        assert_eq!(context.graph_id, "local");
    }

    #[test]
    fn graph_identity_resolve_policy_context_server_graph_uses_graph_key_when_cli_graph_absent() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/local-policy-graph.omni
    policy:
      file: ./server-policy.yaml
server:
  graph: local
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let context = resolve_policy_context(&config).unwrap();
        assert_eq!(context.graph_id, "local");
        assert!(context.policy_file.ends_with("server-policy.yaml"));
    }

    #[test]
    fn graph_identity_resolve_policy_context_anonymous_uses_top_level_default_identity() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/local-policy-graph.omni
policy:
  file: ./top-policy.yaml
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let context = resolve_policy_context(&config).unwrap();
        assert_eq!(context.graph_id, "default");
        assert!(context.policy_file.ends_with("top-policy.yaml"));
    }

    #[test]
    fn graph_identity_resolve_cli_graph_named_target_uses_graph_key_not_project_name_or_uri() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  prod:
    uri: s3://bucket/prod-graph/
    policy:
      file: ./prod-policy.yaml
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let graph = resolve_cli_graph(&config, None, Some("prod")).unwrap();
        assert_eq!(graph.selected(), Some("prod"));
        assert_eq!(graph.graph_id, "prod");
        assert_eq!(graph.uri, "s3://bucket/prod-graph/");
    }

    #[test]
    fn graph_identity_resolve_cli_graph_positional_uri_uses_anonymous_normalized_uri() {
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
project:
  name: misleading-project
graphs:
  local:
    uri: /tmp/configured-graph.omni
    policy:
      file: ./policy.yaml
cli:
  graph: local
"#,
        )
        .unwrap();

        let config = load_config(Some(&config_path)).unwrap();
        let local_graph_path = temp.path().join("explicit-graph.omni");
        let local_graph = resolve_cli_graph(
            &config,
            Some(format!("file://{}", local_graph_path.display())),
            None,
        )
        .unwrap();
        assert_eq!(local_graph.selected(), None);
        assert_eq!(
            local_graph.graph_id,
            local_graph_path.to_string_lossy().as_ref()
        );
        assert_eq!(local_graph.policy_file, None);

        let s3_graph = resolve_cli_graph(
            &config,
            Some("s3://bucket/anonymous-graph/".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(s3_graph.selected(), None);
        assert_eq!(s3_graph.graph_id, "s3://bucket/anonymous-graph");
        assert_eq!(s3_graph.policy_file, None);
    }
}
