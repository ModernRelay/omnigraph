//! Resolution helpers: config/actor/graph/branch/query resolution,
//! remote HTTP, env/token handling, scaffolding (moved verbatim from
//! main.rs in the modularization).

use std::io::IsTerminal;

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

/// Whether a resolved write target is *local* for the purposes of the RFC-011
/// Decision 9 destructive-confirm gate: a bare path or a `file://` URI. Anything
/// else carrying a scheme — `http(s)://` (served), `s3://` / `gs://` / … (object
/// store) — is non-local and a destructive write against it requires explicit
/// consent. Generalizes `is_remote_uri` (which only catches http(s)).
pub(crate) fn uri_is_local(uri: &str) -> bool {
    !uri.contains("://") || uri.starts_with("file://")
}

/// Echo the resolved write target + access path to stderr (RFC-011 Decision 9),
/// unless `--quiet`. One line, e.g. `omnigraph load → file://g.omni (direct,
/// local)`. stderr so `--json` consumers reading stdout are unaffected; the line
/// legitimately differs embedded-vs-served (that visibility is the point).
pub(crate) fn echo_write_target(quiet: bool, label: &str, uri: &str, served: bool) {
    if quiet {
        return;
    }
    let access = if served {
        "served"
    } else if uri_is_local(uri) {
        "direct, local"
    } else {
        "direct, remote"
    };
    eprintln!("omnigraph {label} → {uri} ({access})");
}

/// Gate a destructive write (`cleanup`, overwrite `load`, `branch delete`)
/// against a non-local scope (RFC-011 Decision 9). A local target needs no
/// confirmation; otherwise `--yes` consents, an interactive TTY is prompted, and
/// a non-TTY / `--json` run refuses rather than silently proceeding.
pub(crate) fn confirm_destructive(label: &str, uri: &str, yes: bool, json: bool) -> Result<()> {
    if uri_is_local(uri) || yes {
        return Ok(());
    }
    if json || !std::io::stdin().is_terminal() {
        bail!(
            "refusing destructive `{label}` against non-local target {uri} without confirmation; \
             pass --yes to confirm (an interactive TTY would be prompted instead)"
        );
    }
    eprint!(
        "About to run a destructive `{label}` against {uri} (not local). Type 'yes' to continue: "
    );
    io::stderr().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    match answer.trim().to_ascii_lowercase().as_str() {
        "yes" | "y" => Ok(()),
        _ => bail!("aborted: destructive `{label}` not confirmed"),
    }
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

#[derive(Debug, Clone)]
pub(crate) struct ResolvedCliGraph {
    pub(crate) uri: String,
    pub(crate) is_remote: bool,
}

/// Resolve the cluster for a control-plane tooling command (`policy`,
/// `queries`) from `--cluster`. A configured name (`clusters:` in operator
/// config) is rewritten to its root; a literal dir / `s3://`/`file://` root is
/// passed through. A `--profile`/`OMNIGRAPH_PROFILE` cluster binding also
/// resolves here when `--cluster` is absent. No omnigraph.yaml.
pub(crate) fn require_cluster_scope(
    cluster: Option<&str>,
    profile: Option<&str>,
    command: &str,
) -> Result<String> {
    let op = operator::load_operator_config()?;
    let resolve_name = |name: &str| {
        op.cluster_root(name)
            .map(str::to_string)
            .unwrap_or_else(|| name.to_string())
    };
    if let Some(cluster) = cluster {
        return Ok(resolve_name(cluster));
    }
    // A cluster profile (flag, else OMNIGRAPH_PROFILE) binds the cluster too.
    let profile_name = profile
        .map(str::to_string)
        .or_else(|| std::env::var(scope::PROFILE_ENV).ok().filter(|s| !s.is_empty()));
    if let Some(name) = profile_name {
        let profile = op.profile(&name).ok_or_else(|| {
            color_eyre::eyre::eyre!("unknown profile '{name}' (not defined under `profiles:`)")
        })?;
        if let crate::operator::ScopeBinding::Cluster(cluster) = profile.binding(&name)? {
            return Ok(resolve_name(&cluster));
        }
    }
    bail!(
        "`{command}` needs a cluster — pass --cluster <dir|uri> (or a name from `clusters:` \
         in ~/.omnigraph/config.yaml), or select a cluster profile"
    )
}

/// Read a cluster's serving snapshot for a control-plane tooling command,
/// flattening the readiness `Diagnostic` list into one loud error. The single
/// snapshot entry point for `policy`/`queries` so the not-servable message stays
/// identical across them.
async fn read_serving_snapshot_or_report(
    cluster: &str,
) -> Result<omnigraph_cluster::ServingSnapshot> {
    omnigraph_cluster::read_serving_snapshot(cluster)
        .await
        .map_err(|diagnostics| {
            color_eyre::eyre::eyre!(
                "cluster `{cluster}` is not servable:\n  {}",
                diagnostics
                    .iter()
                    .map(|d| d.message.clone())
                    .collect::<Vec<_>>()
                    .join("\n  ")
            )
        })
}

/// Resolve the Cedar policy bundle(s) for a `--cluster` policy-tooling command
/// (RFC-011). Sources the applied policies from the cluster's serving snapshot;
/// each `ServingPolicy` carries its `source` (digest-verified content) and the
/// scopes it `applies_to` (`cluster` | `graph.<id>`). The optional `graph`
/// selects a graph's bundle when several apply.
pub(crate) async fn read_cluster_policies(
    cluster: &str,
) -> Result<Vec<omnigraph_cluster::ServingPolicy>> {
    Ok(read_serving_snapshot_or_report(cluster).await?.policies)
}

/// Pick the single policy bundle that applies to the selection. With `--graph`,
/// the bundle bound to `graph.<id>` (or the cluster-wide one); without it, the
/// sole bundle if there's exactly one. Ambiguity or absence is a loud error.
pub(crate) fn select_cluster_policy<'p>(
    cluster: &str,
    policies: &'p [omnigraph_cluster::ServingPolicy],
    graph: Option<&str>,
) -> Result<&'p omnigraph_cluster::ServingPolicy> {
    if let Some(graph_id) = graph {
        let graph_ref = format!("graph.{graph_id}");
        let matching: Vec<&omnigraph_cluster::ServingPolicy> = policies
            .iter()
            .filter(|p| {
                p.applies_to
                    .iter()
                    .any(|s| s == &graph_ref || s == "cluster")
            })
            .collect();
        return match matching.as_slice() {
            [only] => Ok(only),
            [] => bail!(
                "cluster `{cluster}` has no policy bundle bound to graph `{graph_id}` \
                 (or to the cluster scope)"
            ),
            many => bail!(
                "graph `{graph_id}` in cluster `{cluster}` matches {} policy bundles ([{}]); \
                 the cluster model expects one bundle per graph scope",
                many.len(),
                many.iter().map(|p| p.name.as_str()).collect::<Vec<_>>().join(", ")
            ),
        };
    }
    match policies {
        [only] => Ok(only),
        [] => bail!("cluster `{cluster}` has no applied policy bundles"),
        many => bail!(
            "cluster `{cluster}` has {} policy bundles ([{}]); pass --graph <id> to select one",
            many.len(),
            many.iter().map(|p| p.name.as_str()).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// THE actor chain (RFC-011) — every command that needs an identity
/// resolves through this one function (one path per concern):
/// `--as` > `operator.actor` in ~/.omnigraph/config.yaml > none.
pub(crate) fn resolve_actor(cli_as: Option<&str>) -> Result<Option<String>> {
    if let Some(actor) = cli_as {
        return Ok(Some(actor.to_string()));
    }
    Ok(operator::load_operator_config()?
        .actor()
        .map(str::to_string))
}

pub(crate) fn resolve_cluster_actor(cli_as: Option<&str>) -> Result<Option<String>> {
    resolve_actor(cli_as)
}

pub(crate) fn resolve_cli_actor(cli_as: Option<&str>) -> Result<Option<String>> {
    resolve_actor(cli_as)
}

/// The bearer token for a remote request (RFC-011): the operator keyed chain
/// for the matching server (`OMNIGRAPH_TOKEN_<NAME>` env → 0600 credentials
/// file), then the default `OMNIGRAPH_BEARER_TOKEN` env. No omnigraph.yaml
/// chain.
pub(crate) fn resolve_remote_bearer_token(explicit_uri: Option<&str>) -> Result<Option<String>> {
    // The keyed hop (RFC-007 §D4, gh-host model): when the effective remote
    // URL belongs to an operator-defined server, that server's keyed chain
    // applies first — OMNIGRAPH_TOKEN_<NAME> env, then the 0600 credentials
    // file. The keyed token is structurally scoped to its own server: a URL
    // matching no operator server never sees it.
    if let Some(remote_url) = explicit_uri.filter(|uri| is_remote_uri(uri)) {
        let operator_config = operator::load_operator_config()?;
        if let Some(server) = operator_config.find_server_for_url(remote_url) {
            if let Some(token) = operator::resolve_keyed_token(server)? {
                return Ok(Some(token));
            }
        }
    }

    Ok(bearer_token_from_env(DEFAULT_BEARER_TOKEN_ENV))
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
    alias_name: &str,
    alias: &crate::operator::OperatorAlias,
    alias_args: &[String],
    explicit_params: Option<Value>,
) -> Result<ReadOutput> {
    let uri = resolve_server_flag(Some(&alias.server), alias.graph.as_deref())?
        .expect("server name is present");
    let bearer_token = resolve_remote_bearer_token(Some(&uri))?;

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

/// The graph URI a command addresses (RFC-011): the scope-resolved URI string
/// (positional URI / `--store` / `--profile` / `defaults.store`). No
/// omnigraph.yaml `cli.graph` fallback — an absent address is a loud error.
pub(crate) fn resolve_uri(cli_uri: Option<String>) -> Result<String> {
    cli_uri.ok_or_else(|| {
        color_eyre::eyre::eyre!(
            "no graph addressed — pass a positional URI, --store <uri>, --server <name>, \
             --profile <name>, or set a default scope in ~/.omnigraph/config.yaml"
        )
    })
}

pub(crate) fn resolve_cli_graph(cli_uri: Option<String>) -> Result<ResolvedCliGraph> {
    let uri = resolve_uri(cli_uri)?;
    Ok(ResolvedCliGraph {
        is_remote: is_remote_uri(&uri),
        uri,
    })
}

pub(crate) fn resolve_local_graph(
    cli_uri: Option<String>,
    operation: &str,
) -> Result<ResolvedCliGraph> {
    let graph = resolve_cli_graph(cli_uri)?;
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

pub(crate) fn resolve_local_uri(cli_uri: Option<String>, operation: &str) -> Result<String> {
    Ok(resolve_local_graph(cli_uri, operation)?.uri)
}

/// Resolve a direct (storage-native) verb's address to a storage URI through the
/// one RFC-011 scope path — the maintenance verbs (optimize/repair/cleanup) plus
/// `schema plan` and `lint`'s graph-target path. Every primitive funnels here: a
/// positional URI, `--store`, `--cluster <root> --graph <id>`, a `--profile`
/// cluster binding, or operator defaults — all resolved at the `Direct`
/// capability (so a server scope is rejected, a cluster scope is allowed when the
/// verb opts into cluster addressing), then mapped to a storage URI by
/// `resolve_storage_uri`.
pub(crate) async fn resolve_maintenance_uri(
    profile: Option<&str>,
    store: Option<&str>,
    cluster: Option<&str>,
    graph: Option<&str>,
    cli_uri: Option<String>,
    operation: &str,
) -> Result<String> {
    let scope = scope::resolve_scope(
        &operator::load_operator_config()?,
        planes::Capability::Direct,
        scope::ScopeFlags {
            profile,
            store,
            server: None,
            cluster,
            graph,
            uri: cli_uri,
        },
    )?;
    resolve_storage_uri(
        scope.uri,
        scope.cluster.as_deref(),
        scope.cluster_graph.as_deref(),
        operation,
    )
    .await
}

/// Map a resolved direct address to a storage URI: a cluster scope
/// (`--cluster <root> --graph <id>`, or a `--profile` cluster binding) resolves
/// the graph's storage URI from the **served cluster state**; otherwise the
/// ordinary positional-URI path. When a cluster scope carries no graph
/// selection (RFC-011 D7), enumerate the catalog: a sole graph is used
/// automatically, otherwise error and list the candidates so the operator can
/// pass `--graph <id>`.
pub(crate) async fn resolve_storage_uri(
    cli_uri: Option<String>,
    cluster: Option<&str>,
    cluster_graph: Option<&str>,
    operation: &str,
) -> Result<String> {
    match (cluster, cluster_graph) {
        (Some(cluster), Some(graph_id)) => resolve_cluster_graph_uri(cluster, graph_id).await,
        (Some(cluster), None) => {
            let graph_id = resolve_sole_cluster_graph(cluster).await?;
            resolve_cluster_graph_uri(cluster, &graph_id).await
        }
        (None, None) => resolve_local_uri(cli_uri, operation),
        (None, Some(_)) => {
            bail!("internal error: a graph was selected without a cluster scope")
        }
    }
}

/// Pick the graph for a cluster scope that has no `--graph`/`default_graph`
/// (RFC-011 D7): exactly one applied graph → use it; zero → error; more than
/// one → error and list the candidates. Never auto-picks among several.
async fn resolve_sole_cluster_graph(cluster: &str) -> Result<String> {
    let ids = omnigraph_cluster::cluster_graph_ids(cluster)
        .await
        .map_err(|diagnostic| color_eyre::eyre::eyre!("{}", diagnostic.message))?;
    match ids.as_slice() {
        [only] => Ok(only.clone()),
        [] => bail!("cluster `{cluster}` has no applied graphs; run `cluster apply` first"),
        many => bail!(
            "cluster `{cluster}` has {} graphs: [{}]; pass --graph <id> to select one",
            many.len(),
            many.join(", ")
        ),
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
    cli_branch: Option<String>,
    alias_branch: Option<String>,
    default_branch: &str,
) -> String {
    cli_branch
        .or(alias_branch)
        .unwrap_or_else(|| default_branch.to_string())
}

pub(crate) fn resolve_read_target(
    cli_branch: Option<String>,
    cli_snapshot: Option<String>,
    alias_branch: Option<String>,
) -> Result<ReadTarget> {
    if cli_branch.is_some() && cli_snapshot.is_some() {
        bail!("read target may specify branch or snapshot, not both");
    }
    Ok(read_target_from_cli(cli_branch.or(alias_branch), cli_snapshot))
}

pub(crate) fn resolve_query_path(
    explicit_query: Option<&PathBuf>,
    alias_query: Option<&str>,
) -> Result<PathBuf> {
    // The `.gq` path is resolved plainly (cwd-relative) — no omnigraph.yaml
    // `query.roots` search.
    explicit_query
        .map(PathBuf::from)
        .or_else(|| alias_query.map(PathBuf::from))
        .ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "exactly one of --query, --query-string, or --alias must be provided"
            )
        })
}

pub(crate) fn resolve_query_source(
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
        explicit_query,
        alias_query,
    )?)?)
}

pub(crate) fn parse_alias_value(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()))
}

/// The format cascade (RFC-011): `--json` > `--format` > alias format >
/// operator `defaults.output` > table.
pub(crate) fn resolve_read_format(
    cli_format: Option<ReadOutputFormat>,
    json: bool,
    alias_format: Option<ReadOutputFormat>,
) -> ReadOutputFormat {
    if json {
        return ReadOutputFormat::Json;
    }
    cli_format
        .or(alias_format)
        .or_else(|| {
            operator::load_operator_config()
                .ok()
                .and_then(|operator| operator.output())
        })
        .unwrap_or_default()
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
    cli_uri: Option<String>,
    schema_path: Option<&PathBuf>,
    query_path: &PathBuf,
) -> Result<QueryLintOutput> {
    let resolved_query_path = resolve_query_path(Some(query_path), None)?;
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

    if cli_uri.is_none() {
        bail!(
            "lint requires --schema <schema.pg> (offline) or a graph target \
             (--store <uri> / --cluster <dir> --graph <id>)"
        );
    }

    let uri = resolve_local_uri(cli_uri, "lint")?;
    let db = Omnigraph::open(&uri).await?;
    Ok(lint_query_file(
        &db.catalog(),
        &query_source,
        query_path,
        QueryLintSchemaSource::graph(uri),
    ))
}

/// Build a `QueryRegistry` from a cluster serving snapshot's stored queries,
/// optionally scoped to one graph. The `ServingQuery.source` is the
/// digest-verified `.gq` content, so no file I/O or omnigraph.yaml is involved.
fn registry_from_serving_queries(
    queries: &[omnigraph_cluster::ServingQuery],
    graph: Option<&str>,
) -> Result<QueryRegistry> {
    let specs: Vec<omnigraph_server::queries::RegistrySpec> = queries
        .iter()
        .filter(|q| graph.is_none_or(|g| q.graph_id == g))
        .map(|q| omnigraph_server::queries::RegistrySpec {
            name: q.name.clone(),
            source: q.source.clone(),
            expose: false,
            tool_name: None,
        })
        .collect();
    QueryRegistry::from_specs(specs).map_err(|errors| {
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


/// `queries validate --cluster <dir>` (RFC-011): type-check every stored query
/// in the cluster catalog against its graph's applied schema. Both the registry
/// and the schemas come from the cluster serving snapshot — no omnigraph.yaml.
/// With `--graph`, scope to a single graph.
pub(crate) async fn execute_queries_validate(
    cluster: &str,
    graph: Option<&str>,
    json: bool,
) -> Result<()> {
    let snapshot = read_serving_snapshot_or_report(cluster).await?;

    // Type-check per graph: each graph's stored queries against its own schema
    // (read from the graph's applied storage root). A `--graph` filter scopes to
    // exactly one graph; an unknown id is a loud error.
    let mut breakages = Vec::new();
    let mut warnings = Vec::new();
    let mut total = 0usize;
    let mut matched_any = false;
    for serving_graph in &snapshot.graphs {
        if graph.is_some_and(|g| g != serving_graph.graph_id) {
            continue;
        }
        matched_any = true;
        let registry = registry_from_serving_queries(&snapshot.queries, Some(&serving_graph.graph_id))?;
        let db = Omnigraph::open(&serving_graph.root.to_string_lossy()).await?;
        let report = check(&registry, &db.catalog());
        total += registry.len();
        for b in &report.breakages {
            breakages.push(QueriesIssue {
                query: b.query.clone(),
                message: b.message.clone(),
            });
        }
        for w in &report.warnings {
            warnings.push(QueriesIssue {
                query: w.query.clone(),
                message: w.message.clone(),
            });
        }
    }
    if let Some(graph_id) = graph {
        if !matched_any {
            bail!("graph `{graph_id}` is not applied in cluster `{cluster}`");
        }
    }

    let has_breakages = !breakages.is_empty();
    let output = QueriesValidateOutput {
        ok: !has_breakages,
        breakages,
        warnings,
    };

    if json {
        print_json(&output)?;
    } else {
        if output.breakages.is_empty() {
            println!(
                "OK  {} stored quer{} type-check against the schema",
                total,
                if total == 1 { "y" } else { "ies" }
            );
        }
        for issue in &output.breakages {
            println!("ERROR  query '{}': {}", issue.query, issue.message);
        }
        for issue in &output.warnings {
            println!("WARN   query '{}': {}", issue.query, issue.message);
        }
    }

    if has_breakages {
        io::stdout().flush()?;
        std::process::exit(1);
    }
    Ok(())
}

/// `queries list --cluster <dir>` (RFC-011): list the catalog's stored queries.
/// With `--graph`, scope to one graph.
pub(crate) async fn execute_queries_list(
    cluster: &str,
    graph: Option<&str>,
    json: bool,
) -> Result<()> {
    let snapshot = read_serving_snapshot_or_report(cluster).await?;
    let registry = registry_from_serving_queries(&snapshot.queries, graph)?;

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

    // RFC-011 Decision 9: locality classifier for the destructive-confirm gate.
    #[test]
    fn uri_is_local_truth_table() {
        // Local: bare path or file://.
        assert!(uri_is_local("graph.omni"));
        assert!(uri_is_local("/abs/path/graph.omni"));
        assert!(uri_is_local("file:///tmp/graph.omni"));
        // Non-local: served or object-store schemes.
        assert!(!uri_is_local("http://host/graphs/g"));
        assert!(!uri_is_local("https://host/graphs/g"));
        assert!(!uri_is_local("s3://bucket/graph.omni"));
        assert!(!uri_is_local("gs://bucket/graph.omni"));
    }

    // RFC-011 Decision 9: a non-local destructive write with `--json` (the CI
    // shape — also covers the no-TTY case, since tests run without a terminal)
    // refuses rather than proceeding; a local one and an explicit `--yes` pass.
    #[test]
    fn confirm_destructive_refuses_non_local_without_consent() {
        let err = confirm_destructive("cleanup", "s3://b/g.omni", false, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("--yes"), "{err}");
    }

    #[test]
    fn confirm_destructive_allows_local_and_explicit_yes() {
        // Local needs no confirmation, even with --json.
        assert!(confirm_destructive("cleanup", "file:///tmp/g.omni", false, true).is_ok());
        assert!(confirm_destructive("branch delete", "graph.omni", false, true).is_ok());
        // --yes consents to a non-local target.
        assert!(confirm_destructive("cleanup", "s3://b/g.omni", true, true).is_ok());
    }

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
