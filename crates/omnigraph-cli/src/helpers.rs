//! Resolution helpers: config/actor/graph/branch/query resolution,
//! remote HTTP, env/token handling, scaffolding (moved verbatim from
//! main.rs in the modularization).

use super::*;
use crate::operator;

pub(crate) fn ensure_local_graph_parent(uri: &str) -> Result<()> {
    if !uri.contains("://") {
        fs::create_dir_all(uri)?;
    }
    Ok(())
}

pub(crate) fn is_remote_uri(uri: &str) -> bool {
    uri.starts_with("http://") || uri.starts_with("https://")
}

/// THE one way the CLI composes a remote request URL. Every remote call
/// routes through here so URL assembly has a single mechanism instead of
/// per-callsite string interpolation.
///
/// - `base` is the resolved server root (single-graph) or `…/graphs/{id}`
///   (multi-graph).
/// - `segments` are appended as individual percent-encoded path segments, so
///   a dynamic component (branch name, commit id, query name) is always one
///   safe segment — e.g. a branch `etl/zendesk/run-1` becomes `%2F`-escaped.
/// - `query` pairs are percent-encoded values.
///
/// Trailing-slash normalization happens exactly once via `pop_if_empty`:
/// `Url::parse` normalizes a path-less base (`http://host`) to a single empty
/// trailing segment, and a `…/graphs/{id}/` base keeps its own. `extend`
/// appends *after* the last segment, so without dropping a trailing empty one
/// the join emits `…/graphs/{id}//branches/{name}` — the empty `//` segment
/// misses the route and 404s. Because callers pass structured segments rather
/// than a pre-joined string, neither a stray `//` nor an un-encoded dynamic
/// component is representable here.
pub(crate) fn remote_url(
    base: &str,
    segments: &[&str],
    query: &[(&str, &str)],
) -> Result<String> {
    let mut url = reqwest::Url::parse(base.trim_end_matches('/'))?;
    url.path_segments_mut()
        .map_err(|_| color_eyre::eyre::eyre!("invalid remote base url"))?
        .pop_if_empty()
        .extend(segments);
    if !query.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }
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

/// THE actor chain (RFC-007 §D3) — every command that needs an identity
/// resolves through this one function (one path per concern):
/// `--as` > legacy `cli.actor` in omnigraph.yaml (RFC-008 window) >
/// `operator.actor` in ~/.omnigraph/config.yaml > none.
pub(crate) fn resolve_actor(
    cli_as: Option<&str>,
    legacy_config_actor: Option<&str>,
) -> Result<Option<String>> {
    if let Some(actor) = cli_as {
        return Ok(Some(actor.to_string()));
    }
    if let Some(actor) = legacy_config_actor {
        return Ok(Some(actor.to_string()));
    }
    Ok(operator::load_operator_config()?
        .actor()
        .map(str::to_string))
}

pub(crate) fn resolve_cluster_actor(cli_as: Option<&str>) -> Result<Option<String>> {
    if let Some(actor) = cli_as {
        return Ok(Some(actor.to_string()));
    }
    let config = load_config(None).wrap_err(
        "resolving the default actor from omnigraph.yaml (pass --as <ACTOR> to skip this lookup)",
    )?;
    resolve_actor(None, config.cli.actor.as_deref())
}

pub(crate) fn resolve_cli_actor(
    cli_as: Option<&str>,
    config: &OmnigraphConfig,
) -> Result<Option<String>> {
    resolve_actor(cli_as, config.cli.actor.as_deref())
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
) -> Result<Option<String>> {
    // `--target` is gone; the legacy explicit-target name is always None.
    let explicit_target: Option<&str> = None;
    // The keyed hop (RFC-007 §D4, gh-host model): when the effective remote
    // URL belongs to an operator-defined server, that server's keyed chain
    // applies first — OMNIGRAPH_TOKEN_<NAME> env, then the 0600 credentials
    // file. Ok(None) falls through to the legacy chain unchanged, and the
    // keyed token is structurally scoped to its own server (§D5 rule 3):
    // a URL matching no operator server never sees it.
    if let Some(remote_url) = effective_remote_url(config, explicit_uri, explicit_target) {
        let operator_config = operator::load_operator_config()?;
        if let Some(server) = operator_config.find_server_for_url(&remote_url) {
            if let Some(token) = operator::resolve_keyed_token(server)? {
                return Ok(Some(token));
            }
        }
    }

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

/// `--server <name>` (RFC-007 PR 3): resolve an operator-defined server
/// name (+ optional `--graph` for multi-graph servers) to the effective
/// remote URI. The result feeds the ordinary `uri` slot, so graph
/// resolution and the keyed-token URL match work unchanged — the flag is
/// sugar for a URI the operator already owns. Unknown names fail loudly,
/// listing what IS defined.
pub(crate) fn resolve_server_flag(
    server: Option<&str>,
    graph: Option<&str>,
) -> Result<Option<String>> {
    let Some(server) = server else {
        return Ok(None);
    };
    // RFC-011 Decision 2: a value containing `://` is a literal base URL
    // (bypasses the operator-config registry); otherwise it is a config name.
    let base_url = if server.contains("://") {
        server.to_string()
    } else {
        let operator_config = operator::load_operator_config()?;
        let Some(entry) = operator_config.servers.get(server) else {
            let known = operator_config
                .servers
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            color_eyre::eyre::bail!(
                "unknown server '{server}' — servers defined in the operator config: [{known}] (add it under servers: in ~/.omnigraph/config.yaml)"
            );
        };
        entry.url.clone()
    };
    let base = base_url.trim_end_matches('/');
    Ok(Some(match graph {
        Some(graph) => format!("{base}/graphs/{graph}"),
        None => base.to_string(),
    }))
}

/// Execute an OPERATOR alias (RFC-007 PR 3): a pure binding invoking a
/// stored query by name on a named server — POST {base}/queries/{name}.
/// Param precedence: --params > positional args > the alias's fixed
/// params. The keyed token applies via the ordinary URL match.
pub(crate) async fn execute_operator_alias(
    client: &reqwest::Client,
    config: &OmnigraphConfig,
    alias_name: &str,
    alias: &crate::operator::OperatorAlias,
    alias_args: &[String],
    explicit_params: Option<Value>,
) -> Result<ReadOutput> {
    let uri = resolve_server_flag(Some(&alias.server), alias.graph.as_deref())?
        .expect("server name is present");
    let bearer_token = resolve_remote_bearer_token(config, Some(&uri))?;

    let mut params = serde_json::Map::new();
    for (key, value) in &alias.params {
        let Some(key) = key.as_str() else {
            bail!("alias '{alias_name}': params keys must be strings");
        };
        params.insert(key.to_string(), serde_json::to_value(value)?);
    }
    if alias_args.len() > alias.args.len() {
        bail!(
            "alias '{alias_name}' takes {} positional arg(s) ({}), got {}",
            alias.args.len(),
            alias.args.join(", "),
            alias_args.len()
        );
    }
    for (name, value) in alias.args.iter().zip(alias_args) {
        params.insert(name.clone(), parse_alias_value(value));
    }
    if let Some(Value::Object(explicit)) = explicit_params {
        for (key, value) in explicit {
            params.insert(key, value);
        }
    }

    let body = (!params.is_empty()).then(|| serde_json::json!({ "params": params }));
    remote_json(
        client,
        Method::POST,
        remote_url(&uri, &["queries", &alias.query], &[])?,
        body,
        bearer_token.as_deref(),
    )
    .await
}

/// Apply `--server`/`--graph` to a command's uri/target slots: exclusive
/// with both (loud error, not silent precedence), no-op when absent.
pub(crate) fn apply_server_flag(
    server: Option<&str>,
    graph: Option<&str>,
    uri: Option<String>,
) -> Result<Option<String>> {
    if server.is_none() {
        return Ok(uri);
    }
    if uri.is_some() {
        color_eyre::eyre::bail!(
            "--server is exclusive with a positional URI — pick one way to address the graph"
        );
    }
    resolve_server_flag(server, graph)
}

/// The remote base URL a token resolution is FOR — the same scoping
/// `graph_bearer_token_env` uses: an explicit http(s) `--uri` wins, else
/// the config-resolved target's uri (when remote). Local URIs → None.
fn effective_remote_url(
    config: &OmnigraphConfig,
    explicit_uri: Option<&str>,
    explicit_target: Option<&str>,
) -> Option<String> {
    if let Some(uri) = explicit_uri {
        return is_remote_uri(uri).then(|| uri.to_string());
    }
    let target = config.resolve_target_name(explicit_uri, explicit_target, config.cli_graph_name())?;
    let uri = &config.graphs.get(target)?.uri;
    is_remote_uri(uri).then(|| uri.clone())
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

pub(crate) fn resolve_uri(config: &OmnigraphConfig, cli_uri: Option<String>) -> Result<String> {
    // `--target` is gone; the second arg (the legacy explicit-target name) is
    // always None. A bare command still falls back to `cli.graph` (the third arg).
    config.resolve_target_uri(cli_uri, None, config.cli_graph_name())
}

pub(crate) fn resolve_cli_graph(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
) -> Result<ResolvedCliGraph> {
    let selected = if cli_uri.is_some() {
        None
    } else {
        config.cli_graph_name().map(str::to_string)
    };
    config.resolve_graph_selection(selected.as_deref())?;
    let uri = resolve_uri(config, cli_uri)?;
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
    operation: &str,
) -> Result<ResolvedCliGraph> {
    let graph = resolve_cli_graph(config, cli_uri)?;
    if graph.is_remote {
        bail!(
            "`{}` is a direct (storage-native) command and needs direct storage \
             access; the resolved target is a remote server ({}). Pass the \
             graph's file:// or s3:// URI.",
            operation,
            graph.uri
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
    operation: &str,
) -> Result<String> {
    Ok(resolve_local_graph(config, cli_uri, operation)?.uri)
}

/// Resolve a storage-plane verb's address to a direct storage URI (RFC-010
/// Slice 3). `--cluster <dir|uri> --cluster-graph <id>` resolves the graph's
/// storage URI from the **served cluster state** (the truth a `--cluster`
/// server serves); otherwise the ordinary positional-URI path.
/// clap enforces both-or-neither and exclusion with `uri`, so the mismatched
/// arm is defensive.
pub(crate) async fn resolve_storage_uri(
    config: &OmnigraphConfig,
    cli_uri: Option<String>,
    cluster: Option<&str>,
    cluster_graph: Option<&str>,
    operation: &str,
) -> Result<String> {
    match (cluster, cluster_graph) {
        (Some(cluster), Some(graph_id)) => resolve_cluster_graph_uri(cluster, graph_id).await,
        (None, None) => resolve_local_uri(config, cli_uri, operation),
        _ => bail!("--cluster and --cluster-graph must be given together"),
    }
}

/// Look up a graph's storage URI from a cluster's applied state ledger. Uses
/// the lightweight `resolve_graph_storage_uri` (NOT the full serving-snapshot
/// validation), so maintenance — especially `repair` — works even when an
/// unrelated catalog payload is corrupt or a recovery sweep is pending.
async fn resolve_cluster_graph_uri(cluster: &str, graph_id: &str) -> Result<String> {
    omnigraph_cluster::resolve_graph_storage_uri(cluster, graph_id)
        .await
        .map_err(|diagnostic| color_eyre::eyre::eyre!("{}", diagnostic.message))
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

/// The format cascade (RFC-007 §D3): `--json` > `--format` > alias format >
/// legacy `cli.output_format` (RFC-008 window) > operator `defaults.output`
/// > table.
pub(crate) fn resolve_read_format(
    config: &OmnigraphConfig,
    cli_format: Option<ReadOutputFormat>,
    json: bool,
    alias_format: Option<ReadOutputFormat>,
) -> ReadOutputFormat {
    if json {
        return ReadOutputFormat::Json;
    }
    cli_format
        .or(alias_format)
        .or(config.cli.output_format)
        .or_else(|| {
            operator::load_operator_config()
                .ok()
                .and_then(|operator| operator.output())
        })
        .unwrap_or_default()
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

    let has_graph_target = cli_uri.is_some() || config.cli_graph_name().is_some();
    if !has_graph_target {
        bail!("lint requires --schema <schema.pg> or a resolvable graph target");
    }

    let uri = resolve_local_uri(config, cli_uri, "lint")?;
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
    operation: &str,
) -> Result<(String, Option<String>)> {
    let graph = resolve_local_graph(config, cli_uri, operation)?;
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
) -> Result<Option<String>> {
    let selected = config.cli_graph_name().map(str::to_string);
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
        "stored-query registries are configured for graph{} {} but no graph was selected. Pass a positional URI or set `cli.graph`.",
        if graph_names.len() == 1 { "" } else { "s" },
        graph_names.join(", "),
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
    config_path: Option<&PathBuf>,
    json: bool,
) -> Result<()> {
    let config = load_cli_config(config_path)?;
    // One selection drives both the schema URI and the registry.
    let (uri, selected) = resolve_selected_graph(&config, uri, "queries validate")?;
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

pub(crate) fn execute_queries_list(config_path: Option<&PathBuf>, json: bool) -> Result<()> {
    let config = load_cli_config(config_path)?;
    let selected = resolve_registry_selection_for_list(&config)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    // RFC-011 Decision 2: `--server` accepts a literal URL (value with `://`),
    // bypassing the operator-config registry — so no config / OMNIGRAPH_HOME is
    // read on this path (hermetic).
    #[test]
    fn server_flag_accepts_a_literal_url() {
        assert_eq!(
            resolve_server_flag(Some("https://graph.example.com"), None).unwrap(),
            Some("https://graph.example.com".to_string())
        );
        // trailing slash trimmed; `--graph` appends the multi-graph path.
        assert_eq!(
            resolve_server_flag(Some("https://graph.example.com/"), Some("knowledge")).unwrap(),
            Some("https://graph.example.com/graphs/knowledge".to_string())
        );
    }

    // `branch delete` interpolates the branch into the URL path. The composed
    // path must be exactly `<base-path>/branches/<name>` with no empty `//`
    // segment — an empty segment misses the
    // `/graphs/{graph_id}/branches/{branch}` route and 404s.
    #[test]
    fn remote_url_multi_graph_base_has_no_double_slash() {
        let url = remote_url("http://host/graphs/p9-os", &["branches", "tmpbranch"], &[]).unwrap();
        assert_eq!(url, "http://host/graphs/p9-os/branches/tmpbranch");
        assert!(
            !url.contains("//branches"),
            "double slash before branches: {url}"
        );
    }

    #[test]
    fn remote_url_single_graph_base_has_no_double_slash() {
        let url = remote_url("http://host", &["branches", "tmpbranch"], &[]).unwrap();
        assert_eq!(url, "http://host/branches/tmpbranch");
    }

    #[test]
    fn remote_url_tolerates_trailing_slash_on_base() {
        let url = remote_url("http://host/graphs/p9-os/", &["branches", "tmpbranch"], &[]).unwrap();
        assert_eq!(url, "http://host/graphs/p9-os/branches/tmpbranch");
    }

    #[test]
    fn remote_url_encodes_slashes_in_path_segment() {
        let url = remote_url(
            "http://host/graphs/p9-os",
            &["branches", "etl/zendesk/run-1"],
            &[],
        )
        .unwrap();
        assert_eq!(
            url,
            "http://host/graphs/p9-os/branches/etl%2Fzendesk%2Frun-1"
        );
    }

    // Sibling cases the unified builder closes by construction: a dynamic
    // commit id in the path, and a branch name carried as a query value, are
    // both percent-encoded instead of interpolated raw.
    #[test]
    fn remote_url_encodes_dynamic_path_segment_for_commits() {
        let url = remote_url("http://host/graphs/p9-os", &["commits", "a/b c"], &[]).unwrap();
        assert_eq!(url, "http://host/graphs/p9-os/commits/a%2Fb%20c");
    }

    #[test]
    fn remote_url_encodes_query_values() {
        let url = remote_url(
            "http://host/graphs/p9-os",
            &["snapshot"],
            &[("branch", "feature&x=1")],
        )
        .unwrap();
        assert_eq!(
            url,
            "http://host/graphs/p9-os/snapshot?branch=feature%26x%3D1"
        );
    }
}
