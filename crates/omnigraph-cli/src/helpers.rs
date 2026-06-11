//! Resolution helpers: config/actor/graph/branch/query resolution,
//! remote HTTP, env/token handling, scaffolding (moved verbatim from
//! main.rs in the modularization).

use super::*;

pub(crate) fn ensure_local_graph_parent(uri: &str) -> Result<()> {
    if !uri.contains("://") {
        fs::create_dir_all(uri)?;
    }
    Ok(())
}

pub(crate) fn is_remote_uri(uri: &str) -> bool {
    uri.starts_with("http://") || uri.starts_with("https://")
}

pub(crate) fn remote_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

pub(crate) fn remote_branch_url(base: &str, branch: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(&format!("{}/", base.trim_end_matches('/')))?;
    url.path_segments_mut()
        .map_err(|_| color_eyre::eyre::eyre!("invalid remote base url"))?
        .extend(["branches", branch]);
    Ok(url.to_string())
}

pub(crate) fn normalize_bearer_token(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn bearer_token_from_env(var_name: &str) -> Option<String> {
    normalize_bearer_token(std::env::var(var_name).ok())
}

pub(crate) fn parse_env_assignment(line: &str) -> Option<(String, String)> {
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

pub(crate) fn bearer_token_from_env_file(path: &Path, var_name: &str) -> Result<Option<String>> {
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

pub(crate) fn load_env_file_into_process(path: &Path) -> Result<()> {
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

pub(crate) fn load_cli_config(config_path: Option<&PathBuf>) -> Result<OmnigraphConfig> {
    let config = load_config(config_path)?;
    if let Some(path) = config.resolve_auth_env_file() {
        load_env_file_into_process(&path)?;
    }
    Ok(config)
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedCliGraph {
    pub(crate) uri: String,
    pub(crate) selected: Option<String>,
    pub(crate) graph_id: String,
    pub(crate) policy_file: Option<PathBuf>,
    pub(crate) is_remote: bool,
}

impl ResolvedCliGraph {
    pub(crate) fn selected(&self) -> Option<&str> {
        self.selected.as_deref()
    }
}

pub(crate) struct ResolvedPolicyContext {
    pub(crate) policy_file: PathBuf,
    pub(crate) graph_id: String,
}

pub(crate) fn resolve_policy_context(config: &OmnigraphConfig) -> Result<ResolvedPolicyContext> {
    let selected = config.resolve_policy_tooling_graph_selection()?;
    let policy_file = config.resolve_policy_file_for(selected).ok_or_else(|| {
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

pub(crate) fn resolve_policy_engine(context: &ResolvedPolicyContext) -> Result<PolicyEngine> {
    PolicyEngine::load_graph(&context.policy_file, &context.graph_id)
}

pub(crate) fn resolve_policy_engine_for_graph(graph: &ResolvedCliGraph) -> Result<PolicyEngine> {
    let policy_file = graph.policy_file.as_ref().ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "policy.file or graphs.<name>.policy.file must be set in omnigraph.yaml"
        )
    })?;
    PolicyEngine::load_graph(policy_file, &graph.graph_id)
}

pub(crate) async fn open_local_db_with_policy(graph: &ResolvedCliGraph) -> Result<Omnigraph> {
    let db = Omnigraph::open(&graph.uri).await?;
    if graph.policy_file.is_some() {
        let engine = Arc::new(resolve_policy_engine_for_graph(graph)?);
        Ok(db.with_policy(engine as Arc<dyn omnigraph_policy::PolicyChecker>))
    } else {
        Ok(db)
    }
}

pub(crate) fn resolve_cluster_actor(cli_as: Option<&str>) -> Result<Option<String>> {
    if let Some(actor) = cli_as {
        return Ok(Some(actor.to_string()));
    }
    let config = load_config(None).wrap_err(
        "resolving the default actor from the per-operator omnigraph.yaml (pass --as <ACTOR> to skip this lookup)",
    )?;
    Ok(config.cli.actor.clone())
}

pub(crate) fn resolve_cli_actor<'a>(cli_as: Option<&'a str>, config: &'a OmnigraphConfig) -> Option<&'a str> {
    cli_as.or(config.cli.actor.as_deref())
}

pub(crate) fn resolve_policy_tests_path(context: &ResolvedPolicyContext) -> PathBuf {
    context.policy_file.with_file_name("policy.tests.yaml")
}

pub(crate) fn normalize_policy_graph_uri(uri: &str) -> Result<String> {
    if is_remote_uri(uri) {
        Ok(uri.trim_end_matches('/').to_string())
    } else {
        Ok(normalize_root_uri(uri)?)
    }
}

pub(crate) fn resolve_remote_bearer_token(
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

pub(crate) fn build_http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::new())
}

pub(crate) fn apply_bearer_token(
    request: reqwest::RequestBuilder,
    token: Option<&str>,
) -> reqwest::RequestBuilder {
    if let Some(token) = token {
        request.header(AUTHORIZATION, format!("Bearer {}", token))
    } else {
        request
    }
}

pub(crate) async fn remote_json<T: DeserializeOwned>(
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

pub(crate) fn resolve_uri(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
) -> Result<String> {
    config.resolve_target_uri(cli_uri, cli_target, config.cli_graph_name())
}

pub(crate) fn resolve_cli_graph(
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

pub(crate) fn resolve_local_graph(
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

pub(crate) fn parse_duration_arg(s: &str) -> Result<std::time::Duration> {
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

pub(crate) fn resolve_local_uri(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
    operation: &str,
) -> Result<String> {
    Ok(resolve_local_graph(config, cli_uri, cli_target, operation)?.uri)
}

pub(crate) fn resolve_branch(
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

pub(crate) fn resolve_read_target(
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

pub(crate) fn resolve_query_path(
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

pub(crate) fn resolve_query_source(
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

pub(crate) fn parse_alias_value(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
}

pub(crate) fn merged_params_json(
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

pub(crate) fn resolve_read_format(
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

pub(crate) fn resolve_alias<'a>(
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

pub(crate) fn normalize_legacy_alias_uri(
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

pub(crate) fn scaffold_config_if_missing(uri: &str) -> Result<()> {
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

pub(crate) fn inferred_config_path(uri: &str) -> Result<PathBuf> {
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

pub(crate) fn read_target_from_cli(branch: Option<String>, snapshot: Option<String>) -> ReadTarget {
    if let Some(snapshot) = snapshot {
        ReadTarget::snapshot(SnapshotId::new(snapshot))
    } else {
        ReadTarget::branch(branch.unwrap_or_else(|| "main".to_string()))
    }
}

pub(crate) fn load_params_json(params: &ParamsArgs) -> Result<Option<Value>> {
    match (&params.params, &params.params_file) {
        (Some(inline), None) => Ok(Some(serde_json::from_str(inline)?)),
        (None, Some(path)) => Ok(Some(serde_json::from_str(&fs::read_to_string(path)?)?)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => bail!("only one of --params or --params-file may be provided"),
    }
}

pub(crate) fn select_named_query(
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

pub(crate) fn query_params_from_json(
    query_params: &[omnigraph_compiler::query::ast::Param],
    params_json: Option<&Value>,
) -> Result<ParamMap> {
    json_params_to_param_map(params_json, query_params, JsonParamMode::Standard)
        .map_err(|err| color_eyre::eyre::eyre!(err.to_string()))
}

pub(crate) async fn execute_query_lint(
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

pub(crate) fn resolve_selected_graph(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cli_target: Option<&str>,
    operation: &str,
) -> Result<(String, Option<String>)> {
    let graph = resolve_local_graph(config, cli_uri, cli_target, operation)?;
    Ok((graph.uri, graph.selected))
}

pub(crate) fn load_registry_or_report(
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

pub(crate) fn graph_query_registry_names(config: &OmnigraphConfig) -> Vec<&str> {
    config
        .graphs
        .iter()
        .filter_map(|(name, graph)| (!graph.queries.is_empty()).then_some(name.as_str()))
        .collect()
}

pub(crate) fn resolve_registry_selection_for_list(
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

pub(crate) fn validate_registry_for_catalog(
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

pub(crate) async fn execute_queries_validate(
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

pub(crate) fn execute_queries_list(
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

pub(crate) async fn execute_read(
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

pub(crate) async fn execute_read_remote(
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

pub(crate) async fn execute_change(
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

pub(crate) fn legacy_change_request_body(
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

pub(crate) async fn execute_change_remote(
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

pub(crate) async fn execute_export_to_writer<W: Write>(
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

pub(crate) async fn execute_export_remote_to_writer<W: Write>(
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

pub(crate) fn rewrite_deprecated_argv(args: Vec<OsString>) -> Vec<OsString> {
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
            "read" => {
                eprintln!("warning: `omnigraph read` is deprecated; use `omnigraph query` instead")
            }
            "change" => eprintln!(
                "warning: `omnigraph change` is deprecated; use `omnigraph mutate` instead"
            ),
            "check" => {
                eprintln!("warning: `omnigraph check` is deprecated; use `omnigraph lint` instead");
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
