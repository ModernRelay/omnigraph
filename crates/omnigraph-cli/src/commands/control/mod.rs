//! Applied cluster queries, policy tooling, and graph registry access.

use std::io::Write;

use color_eyre::eyre::{Result, bail};
use omnigraph_server::queries::{QueryRegistry, check};
use omnigraph_server::{PolicyEngine, PolicyRequest, PolicyTestConfig};

use crate::app::Completion;
use crate::client::GraphClient;
use crate::helpers::graph_resource_id_for_selection;
use crate::operator;

mod args;
mod output;

pub(crate) use args::{GraphsCommand, PolicyCommand, QueriesCommand};
use output::{
    QueriesIssue, QueriesListItem, QueriesListOutput, QueriesParam, QueriesValidateOutput,
};

/// Only the existing global arguments consumed by these control tools.
#[derive(Clone, Copy)]
pub(crate) struct ControlScope<'a> {
    pub operator: &'a operator::InvocationConfig,
    pub cluster: Option<&'a str>,
    pub profile: Option<&'a str>,
    pub graph: Option<&'a str>,
    pub server: Option<&'a str>,
}

pub(crate) async fn run_queries(
    command: QueriesCommand,
    scope: ControlScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    // Root tools keep their explicit graph filter and ignore a target's
    // default graph, so their all-graph and ambiguity behaviour holds.
    let cluster = require_cluster_scope(
        scope.operator,
        scope.cluster,
        scope.profile,
        "queries",
        stderr,
    )?;
    let graph = scope.graph;
    match command {
        QueriesCommand::Validate { json } => {
            return execute_queries_validate(&cluster, graph, json, stdout).await;
        }
        QueriesCommand::List { json } => {
            execute_queries_list(&cluster, graph, json, stdout).await?;
        }
    }
    Ok(Completion::success())
}

pub(crate) async fn run_policy(
    command: PolicyCommand,
    scope: ControlScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    // Policy tooling sources the Cedar bundle(s) from the cluster's
    // applied policies (RFC-011): --cluster <dir>, + the global --graph
    // to pick a graph's bundle when several apply.
    // Root tools keep their explicit graph filter and ignore a target's
    // default graph, so their all-graph and ambiguity behaviour holds.
    let cluster = require_cluster_scope(
        scope.operator,
        scope.cluster,
        scope.profile,
        "policy",
        stderr,
    )?;
    let graph = scope.graph;
    let graph_id = match graph {
        Some(id) => graph_resource_id_for_selection(Some(id), ""),
        None => graph_resource_id_for_selection(None, "default"),
    };
    let policies = read_cluster_policies(&cluster).await?;
    match command {
        PolicyCommand::Validate {} => {
            let bundle = select_cluster_policy(&cluster, &policies, graph)?;
            let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
            writeln!(
                stdout,
                "policy valid: bundle '{}' [{} actors]",
                bundle.name,
                engine.known_actor_count()
            )?;
        }
        PolicyCommand::Test { tests } => {
            let bundle = select_cluster_policy(&cluster, &policies, graph)?;
            let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
            let tests = PolicyTestConfig::load(&tests)?;
            engine.run_tests(&tests)?;
            writeln!(stdout, "policy tests passed: {} cases", tests.cases.len())?;
        }
        PolicyCommand::Explain {
            actor,
            action,
            branch,
            target_branch,
        } => {
            let bundle = select_cluster_policy(&cluster, &policies, graph)?;
            let engine = PolicyEngine::load_graph_from_source(&bundle.source, &graph_id)?;
            let request = PolicyRequest {
                action,
                branch,
                target_branch,
            };
            let decision = engine.authorize(&actor, &request)?;
            output::write_policy_explain(&decision, &actor, &request, stdout)?;
        }
    }
    Ok(Completion::success())
}

pub(crate) async fn run_graphs(
    command: GraphsCommand,
    scope: ControlScope<'_>,
    managed_data: Option<GraphClient>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<Completion> {
    match command {
        GraphsCommand::List { json, discovery } => {
            let (client, discovery) = if let Some(client) = managed_data {
                (client, true)
            } else {
                // Explicit operator addressing retains the legacy catalog
                // unless discovery is explicitly requested. Token bytes do
                // not choose configuration or change static-token behavior.
                (
                    GraphClient::resolve_registry(
                        scope.operator,
                        scope.server,
                        scope.profile,
                        stderr,
                    )?,
                    discovery,
                )
            };
            if discovery {
                let payload = client.discover_graphs().await?;
                output::write_graphs_discovery(&payload, json, stdout)?;
                return Ok(Completion::success());
            }
            let payload = client.list_graphs().await?;
            output::write_graphs_list(&payload, json, stdout)?;
        }
    }
    Ok(Completion::success())
}

/// Resolve the cluster for a control-plane tooling command (`policy`,
/// `queries`) through the one scope resolver, in the order every command
/// resolves its address: `--target`, a complete address no other flag may add
/// to; `--cluster`, where a configured name (`clusters:` in operator config) is
/// rewritten to its root and a literal dir / `s3://`/`az://`/`file://` root is
/// passed through; a `--profile`/`OMNIGRAPH_PROFILE` binding; then
/// `defaults.target`. Only a cluster root serves; any other address refuses
/// by kind. No omnigraph.yaml.
fn require_cluster_scope(
    input: &operator::InvocationConfig,
    cluster: Option<&str>,
    profile: Option<&str>,
    command: &str,
    stderr: &mut dyn Write,
) -> Result<String> {
    let scope = crate::scope::resolve_scope(
        input.get(stderr)?,
        input.profile(),
        crate::planes::Capability::Control,
        crate::scope::ScopeFlags {
            target: input.target(),
            profile,
            store: None,
            server: None,
            cluster,
            graph: None,
            uri: None,
            reads_root: true,
        },
    )?;
    let Some(root) = scope.cluster else {
        bail!(
            "`{command}` needs a cluster — pass --cluster <dir|uri> (or a name from `clusters:` \
             in ~/.omnigraph/config.yaml), select a cluster profile, or name a cluster-root target"
        );
    };
    Ok(root)
}

/// Read a cluster's serving snapshot for a control-plane tooling command,
/// flattening the readiness `Diagnostic` list into one loud error. The single
/// snapshot entry point for `policy`/`queries` so the not-servable message stays
/// identical across them.
pub(crate) async fn read_serving_snapshot_or_report(
    cluster: &str,
) -> Result<omnigraph_cluster::ServingSnapshot> {
    if cluster.contains("://") {
        return read_storage_snapshot_or_report(cluster).await;
    }
    report_snapshot(
        cluster,
        omnigraph_cluster::read_serving_snapshot(cluster).await,
    )
}

/// Read the applied revision where a cluster's storage is, a root found by
/// its layout rather than named by configuration, so no `cluster.yaml`
/// beside it can point the read at another root.
pub(crate) async fn read_storage_snapshot_or_report(
    root: &str,
) -> Result<omnigraph_cluster::ServingSnapshot> {
    report_snapshot(
        root,
        omnigraph_cluster::read_serving_snapshot_from_storage(root).await,
    )
}

fn report_snapshot(
    cluster: &str,
    snapshot: std::result::Result<
        omnigraph_cluster::ServingSnapshot,
        Vec<omnigraph_cluster::Diagnostic>,
    >,
) -> Result<omnigraph_cluster::ServingSnapshot> {
    let diagnostic_cluster = omnigraph::storage::redacted_storage_uri(cluster);
    snapshot.map_err(|diagnostics| {
        color_eyre::eyre::eyre!(
            "cluster `{diagnostic_cluster}` is not servable:\n  {}",
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
async fn read_cluster_policies(cluster: &str) -> Result<Vec<omnigraph_cluster::ServingPolicy>> {
    Ok(read_serving_snapshot_or_report(cluster).await?.policies)
}

/// Pick the single policy bundle that applies to the selection. With `--graph`,
/// the bundle bound to `graph.<id>` (or the cluster-wide one); without it, the
/// sole bundle if there's exactly one. Ambiguity or absence is a loud error.
fn select_cluster_policy<'p>(
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
                many.iter()
                    .map(|p| p.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
    }
    match policies {
        [only] => Ok(only),
        [] => bail!("cluster `{cluster}` has no applied policy bundles"),
        many => bail!(
            "cluster `{cluster}` has {} policy bundles ([{}]); pass --graph <id> to select one",
            many.len(),
            many.iter()
                .map(|p| p.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Build a `QueryRegistry` from a cluster serving snapshot's stored queries,
/// optionally scoped to one graph. The `ServingQuery.source` is the
/// digest-verified `.gq` content, so no file I/O or omnigraph.yaml is involved.
fn registry_from_serving_queries(
    queries: &[omnigraph_cluster::ServingQuery],
    graph: Option<&str>,
) -> std::result::Result<QueryRegistry, Vec<omnigraph_server::queries::LoadError>> {
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
    QueryRegistry::from_specs(specs)
}

fn registry_load_issues(errors: Vec<omnigraph_server::queries::LoadError>) -> Vec<QueriesIssue> {
    errors
        .into_iter()
        .map(|error| QueriesIssue {
            query: error.query.unwrap_or_else(|| "<registry>".to_string()),
            message: error.message,
            diagnostic: error
                .diagnostic
                .as_deref()
                .map(omnigraph_api_types::DiagnosticOutput::from),
        })
        .collect()
}

/// `queries validate --cluster <dir>` (RFC-011): type-check every stored query
/// in the cluster catalog against its graph's applied schema. Both the registry
/// and the schemas come from the cluster serving snapshot — no omnigraph.yaml.
/// With `--graph`, scope to a single graph.
pub(crate) async fn execute_queries_validate(
    cluster: &str,
    graph: Option<&str>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<Completion> {
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
        let registry =
            match registry_from_serving_queries(&snapshot.queries, Some(&serving_graph.graph_id)) {
                Ok(registry) => registry,
                Err(errors) => {
                    breakages.extend(registry_load_issues(errors));
                    continue;
                }
            };
        let db = crate::admission::open_read_only(
            &serving_graph.root.to_string_lossy(),
            snapshot.state_cas.as_deref(),
        )
        .await?;
        let report = check(&registry, &db.catalog());
        total += registry.len();
        for b in &report.breakages {
            breakages.push(QueriesIssue {
                query: b.query.clone(),
                message: b.message.clone(),
                diagnostic: b
                    .diagnostic
                    .as_ref()
                    .map(omnigraph_api_types::DiagnosticOutput::from),
            });
        }
        for w in &report.warnings {
            warnings.push(QueriesIssue {
                query: w.query.clone(),
                message: w.message.clone(),
                diagnostic: None,
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

    output::write_queries_validate(&output, total, json, stdout)?;

    if has_breakages {
        stdout.flush()?;
        return Ok(Completion::exit(1));
    }
    Ok(Completion::success())
}

/// `queries list --cluster <dir>` (RFC-011): list the catalog's stored queries.
/// With `--graph`, scope to one graph.
pub(crate) async fn execute_queries_list(
    cluster: &str,
    graph: Option<&str>,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let snapshot = read_serving_snapshot_or_report(cluster).await?;
    let registry = registry_from_serving_queries(&snapshot.queries, graph).map_err(|errors| {
        color_eyre::eyre::eyre!(
            "stored-query registry failed to load:\n  {}",
            errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n  ")
        )
    })?;

    let output = QueriesListOutput {
        queries: registry
            .iter()
            .map(|q| QueriesListItem {
                name: q.name.clone(),
                mcp_expose: q.expose,
                tool_name: q.tool_name.clone(),
                mutation: q.is_mutation(),
                description: q.decl.description.clone(),
                instruction: q.decl.instruction.clone(),
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

    output::write_queries_list(&output, json, stdout)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_query_parse_failures_keep_diagnostics_in_validation_output() {
        let queries = vec![omnigraph_cluster::ServingQuery {
            graph_id: "g".to_string(),
            name: "broken".to_string(),
            source: "query broken { match { $p: Person } return { $p.name } }".to_string(),
        }];
        let failures = registry_from_serving_queries(&queries, Some("g")).unwrap_err();
        let output = QueriesValidateOutput {
            ok: false,
            breakages: registry_load_issues(failures),
            warnings: Vec::new(),
        };
        let json = serde_json::to_value(output).unwrap();
        let error = &json["breakages"][0];
        assert_eq!(error["query"], "broken");
        assert_eq!(error["diagnostic"]["code"], "Q002");
        assert_eq!(error["diagnostic"]["position"]["byte"], 12);
        assert_eq!(
            error["diagnostic"]["suggestion"]["edits"][0]["replacement"],
            "()"
        );
        assert!(
            registry_from_serving_queries(&queries, Some("other"))
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cluster_storage_errors_redact_uri_credentials() {
        let secret = "TOPSECRET-CLI-SAS";
        let cluster = format!("az://omnigraph/clusters/company-brain?sv=2026-01-01&sig={secret}");
        let error = read_serving_snapshot_or_report(&cluster)
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains(secret));
        assert!(error.contains("az://omnigraph/clusters/company-brain"));
        assert!(error.contains("query redacted"));
    }
}
