//! `access`: who the caller is at a target and what it may do there. A server
//! target decides through its access route; for direct storage the CLI
//! evaluates the graph's installed policy for the declared actor, and a graph
//! without one allows every action, as direct writes do.

use std::io::Write;

use color_eyre::Result;
use omnigraph::storage::CapturedStorageBinding;
use omnigraph_api_types::{AccessAction, AccessOutput, ErrorCode};
use serde::Serialize;

use super::args::AccessArgs;
use crate::helpers::CodedRefusal;
use crate::operator::InvocationConfig;
use crate::output::write_json;
use crate::targets::{ACCESS_PROTOCOL, ResolvedTarget, ServerAuth, TargetBinding};

#[derive(Serialize)]
struct AccessReport<'a> {
    protocol: u32,
    /// The target's name, or the address given inline.
    target: &'a str,
    /// `server` when a server decided, `direct` when the CLI did.
    source: &'static str,
    #[serde(flatten)]
    access: AccessOutput,
}

/// The id a standalone graph answers under: it is no cluster's member, so
/// no cluster names it.
const STANDALONE_GRAPH_ID: &str = "default";

pub(crate) async fn run_access(
    input: &InvocationConfig,
    target: &ResolvedTarget,
    graph: Option<&str>,
    actor: Option<&str>,
    args: AccessArgs,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    for branch in [args.branch.as_deref(), args.target_branch.as_deref()]
        .into_iter()
        .flatten()
    {
        omnigraph_policy::ensure_canonical_branch_name(branch)
            .map_err(|error| CodedRefusal::report(ErrorCode::BadRequest, error.to_string()))?;
    }
    let (source, access) = match &target.binding {
        TargetBinding::Server { endpoint, auth, .. } => (
            "server",
            server_access(input, target, endpoint, auth, graph, actor, &args).await?,
        ),
        TargetBinding::DirectRoot { .. } => {
            let graph = crate::scope::graph_selection(target, graph)?
                .expect("a root target selects its graph");
            let binding = crate::scope::capture_storage_binding(target)?;
            let snapshot = graph_snapshot(&binding, graph).await?;
            let actor = crate::helpers::resolve_actor(input, actor, stderr)?;
            ("direct", snapshot_access(&snapshot, graph, actor, &args)?)
        }
        TargetBinding::DirectGraph { .. } => {
            let actor = crate::helpers::resolve_actor(input, actor, stderr)?;
            ("direct", store_access(target, actor, &args).await?)
        }
        TargetBinding::DirectCatalogue | TargetBinding::Managed(_) => {
            return Err(crate::targets::refusal(
                "target_incompatible",
                "access is not available for this target kind and scope",
            ));
        }
    };
    let report = AccessReport {
        protocol: ACCESS_PROTOCOL,
        target: target.label(),
        source,
        access,
    };
    if args.json {
        return write_json(&report, stdout);
    }
    write_text(&report, stdout)
}

/// A server target's access route, with the target's token. `--as` rides as
/// the `Omnigraph-If-Actor` assertion, which the server refuses when the
/// token resolves to another actor.
async fn server_access(
    input: &InvocationConfig,
    target: &ResolvedTarget,
    endpoint: &str,
    auth: &ServerAuth,
    graph: Option<&str>,
    actor: Option<&str>,
    args: &AccessArgs,
) -> Result<AccessOutput> {
    let graph =
        crate::scope::graph_selection(target, graph)?.expect("a server target selects its graph");
    let base_url = crate::helpers::remote_url(endpoint, &["graphs", graph], &[])?;
    let token = crate::targets::resolve_server_token_for(input, auth, endpoint, &base_url)?;
    let http = crate::graph_http::GraphHttpClient::server_target(input, target)?
        .with_expected_actor(actor);
    let mut query = Vec::new();
    if let Some(branch) = args.branch.as_deref() {
        query.push(("branch", branch));
    }
    if let Some(target_branch) = args.target_branch.as_deref() {
        query.push(("target_branch", target_branch));
    }
    let url = crate::helpers::remote_url(&base_url, &["access"], &query)?;
    crate::helpers::remote_json(&http, reqwest::Method::GET, url, None, token.as_deref()).await
}

/// The serving snapshot of the cluster at `root`, read for `graph`. When
/// every applied graph is quarantined the reader serves nothing and refuses
/// the whole snapshot; if `graph` is one it quarantined, that is the graph's
/// unavailability, as a server answers it. Every other readiness failure
/// stays the cluster's.
async fn graph_snapshot(
    root: &CapturedStorageBinding,
    graph: &str,
) -> Result<omnigraph_cluster::ServingSnapshot> {
    omnigraph_cluster::read_serving_snapshot_with_binding(root)
        .await
        .map_err(|diagnostics| {
            quarantined_alone(&diagnostics, graph)
                .unwrap_or_else(|| crate::commands::control::not_servable(root, &diagnostics))
        })
}

/// The refusal for `graph` when the reader refused only because every
/// applied graph is quarantined and `graph` is among them.
fn quarantined_alone(
    diagnostics: &[omnigraph_cluster::Diagnostic],
    graph: &str,
) -> Option<color_eyre::Report> {
    let errors: Vec<_> = diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.severity == omnigraph_cluster::DiagnosticSeverity::Error)
        .collect();
    let only_quarantine = !errors.is_empty()
        && errors
            .iter()
            .all(|diagnostic| diagnostic.code == "cluster_no_healthy_graphs");
    let reasons = graph_reasons(diagnostics, graph);
    (only_quarantine && !reasons.is_empty()).then(|| quarantined(graph, &reasons))
}

/// The messages of the diagnostics the reader attributes to `graph`.
fn graph_reasons<'a>(
    diagnostics: &'a [omnigraph_cluster::Diagnostic],
    graph: &str,
) -> Vec<&'a str> {
    let path = format!("graph.{graph}");
    diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.path == path)
        .map(|diagnostic| diagnostic.message.as_str())
        .collect()
}

fn quarantined(graph: &str, reasons: &[&str]) -> color_eyre::Report {
    CodedRefusal::report(
        ErrorCode::GraphUnavailable,
        format!(
            "graph `{graph}` is not served: {}",
            if reasons.is_empty() {
                "the cluster quarantined it".to_string()
            } else {
                reasons.join("; ")
            }
        ),
    )
}

/// Check that the cluster serves `graph` before its policy is read. A
/// quarantined graph's bundle is withheld from the snapshot and an unknown
/// graph has none, so an absent bundle means "no policy" only for a served
/// graph; the others refuse as a server refuses them.
fn served_graph(snapshot: &omnigraph_cluster::ServingSnapshot, graph: &str) -> Result<()> {
    if snapshot
        .quarantined_graphs
        .iter()
        .any(|blocked| blocked.graph_id == graph)
    {
        return Err(quarantined(
            graph,
            &graph_reasons(&snapshot.diagnostics, graph),
        ));
    }
    if !snapshot
        .graphs
        .iter()
        .any(|served| served.graph_id == graph)
    {
        return Err(CodedRefusal::report(
            ErrorCode::NotFound,
            format!(
                "graph `{graph}` is not applied (applied graphs: [{}])",
                snapshot.applied_graphs.join(", ")
            ),
        ));
    }
    Ok(())
}

/// Once the cluster serves `graph`, its applied `graph.{id}` bundle decides
/// for `actor`, and a graph without one allows every action, with or without
/// an actor, since no policy is installed to refuse one.
fn snapshot_access(
    snapshot: &omnigraph_cluster::ServingSnapshot,
    graph: &str,
    actor: Option<String>,
    args: &AccessArgs,
) -> Result<AccessOutput> {
    served_graph(snapshot, graph)?;
    let binding = format!("graph.{graph}");
    let bundles: Vec<_> = snapshot
        .policies
        .iter()
        .filter(|policy| policy.applies_to.iter().any(|scope| scope == &binding))
        .collect();
    let engine = match bundles.as_slice() {
        [] => None,
        [bundle] => Some(omnigraph_policy::PolicyEngine::load_graph_from_source(
            &bundle.source,
            graph,
        )?),
        // A server refuses to start from such a ledger, so nothing serves
        // the graph until one bundle binds it.
        many => {
            return Err(CodedRefusal::report(
                ErrorCode::GraphUnavailable,
                format!(
                    "graph `{graph}` is not served: {} policy bundles bind it, and the cluster binds one per graph",
                    many.len()
                ),
            ));
        }
    };
    AccessOutput::evaluate(
        actor.clone(),
        graph.to_string(),
        args.branch.clone(),
        args.target_branch.clone(),
        |access, branch, target_branch| -> Result<bool> {
            let Some(engine) = &engine else {
                return Ok(true);
            };
            let Some(actor) = actor.as_deref() else {
                return Ok(false);
            };
            Ok(engine
                .authorize(
                    actor,
                    &omnigraph_policy::PolicyRequest {
                        action: access.into(),
                        branch: branch.map(str::to_string),
                        target_branch: target_branch.map(str::to_string),
                    },
                )?
                .allowed)
        },
    )
}

/// A direct graph target's access, by the policy installed where it lives. A
/// graph in a cluster's layout answers by the ledger applied at the root it
/// sits under, as a root target with `--graph` does. Any other store is a
/// standalone graph, which carries no policy, and the engine allows every
/// action on a handle without one; that answer holds only for a graph that
/// opens, so the store opens first.
async fn store_access(
    target: &ResolvedTarget,
    actor: Option<String>,
    args: &AccessArgs,
) -> Result<AccessOutput> {
    let binding = crate::scope::capture_storage_binding(target)?;
    let member = omnigraph_cluster::cluster_graph_for_graph_binding(&binding)
        .await
        .map_err(|diagnostic| {
            color_eyre::eyre::eyre!(
                "could not check cluster ownership for `{}`: {}",
                diagnostic.path,
                diagnostic.message
            )
        })?;
    if let Some(member) = member {
        let root = binding.for_uri(&member.root)?;
        let snapshot = graph_snapshot(&root, &member.graph_id).await?;
        return snapshot_access(&snapshot, &member.graph_id, actor, args);
    }
    require_standalone_graph(&binding).await?;
    AccessOutput::evaluate(
        actor,
        STANDALONE_GRAPH_ID.to_string(),
        args.branch.clone(),
        args.target_branch.clone(),
        |_, _, _| Ok::<_, color_eyre::Report>(true),
    )
}

/// Open a standalone graph read-only and let it go. A root where the engine
/// finds no graph refuses as a missing graph. Every failure of the open
/// itself stays the engine's own: a graph whose manifest names a missing
/// object is damaged, not absent.
async fn require_standalone_graph(binding: &CapturedStorageBinding) -> Result<()> {
    use omnigraph::db::Omnigraph;
    use omnigraph::storage::storage_for_binding;
    let uri = binding.uri();
    if !Omnigraph::exists_with_storage(uri, storage_for_binding(binding)?).await? {
        return Err(CodedRefusal::report(
            ErrorCode::NotFound,
            format!(
                "no graph at `{}`",
                omnigraph::storage::redacted_storage_uri(uri)
            ),
        ));
    }
    Omnigraph::open_read_only_with_storage(uri, storage_for_binding(binding)?).await?;
    Ok(())
}

fn write_text(report: &AccessReport<'_>, stdout: &mut dyn Write) -> Result<()> {
    let names = |actions: &[&AccessAction]| -> String {
        let names: Vec<String> = actions
            .iter()
            .map(|action| {
                serde_json::to_value(action)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_string))
                    .unwrap_or_default()
            })
            .collect();
        if names.is_empty() {
            "none".to_string()
        } else {
            names.join(", ")
        }
    };
    let access = &report.access;
    writeln!(
        stdout,
        "actor: {}",
        access.actor.as_deref().unwrap_or("(none)")
    )?;
    writeln!(stdout, "graph: {} ({})", access.graph_id, report.source)?;
    let graph_wide: Vec<_> = access.graph_wide.iter().collect();
    writeln!(stdout, "graph-wide: {}", names(&graph_wide))?;
    if let Some(branch) = &access.branch {
        let (transitions, on_branch): (Vec<_>, Vec<_>) = access
            .actions
            .iter()
            .partition(|action| action.is_transition());
        writeln!(stdout, "on {branch}: {}", names(&on_branch))?;
        if let Some(target_branch) = &access.target_branch {
            writeln!(
                stdout,
                "from {branch} into {target_branch}: {}",
                names(&transitions)
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_reads_policy_only_for_a_graph_the_cluster_serves() {
        let mut snapshot = omnigraph_cluster::ServingSnapshot {
            graphs: vec![omnigraph_cluster::ServingGraph {
                graph_id: "knowledge".into(),
                root: std::path::PathBuf::new(),
                embedding: None,
                external_blob_policy: Default::default(),
            }],
            queries: Vec::new(),
            policies: Vec::new(),
            diagnostics: vec![omnigraph_cluster::Diagnostic {
                code: "recovery_pending".into(),
                severity: omnigraph_cluster::DiagnosticSeverity::Error,
                path: "graph.archive".into(),
                message: "a recovery is pending".into(),
                detail: None,
            }],
            config_digest: None,
            state_revision: 1,
            state_cas: None,
            applied_graphs: vec!["archive".into(), "knowledge".into()],
            quarantined_graphs: vec![omnigraph_cluster::ServingBlockedGraph {
                graph_id: "archive".into(),
                root: std::path::PathBuf::new(),
            }],
        };
        let refusal = |error: color_eyre::Report| -> (ErrorCode, String) {
            let refusal = error.downcast_ref::<CodedRefusal>().unwrap();
            (refusal.code, refusal.message.clone())
        };
        served_graph(&snapshot, "knowledge").unwrap();
        for (graph, code, says) in [
            (
                "archive",
                ErrorCode::GraphUnavailable,
                "a recovery is pending",
            ),
            ("missing", ErrorCode::NotFound, "archive, knowledge"),
        ] {
            let (refused, message) = refusal(served_graph(&snapshot, graph).unwrap_err());
            assert_eq!(refused, code, "{graph}");
            assert!(message.contains(says), "{graph}: {message}");
        }
        // Two bundles bound to one graph are a ledger no server starts from,
        // so the graph is unavailable rather than its policy guessed.
        snapshot.policies = ["first", "second"]
            .into_iter()
            .map(|name| omnigraph_cluster::ServingPolicy {
                name: name.into(),
                source: String::new(),
                applies_to: vec!["graph.knowledge".into()],
            })
            .collect();
        let args = AccessArgs {
            branch: None,
            target_branch: None,
            json: true,
        };
        let (refused, message) =
            refusal(snapshot_access(&snapshot, "knowledge", None, &args).unwrap_err());
        assert_eq!(refused, ErrorCode::GraphUnavailable);
        assert!(message.contains("2 policy bundles"), "{message}");
    }
}
