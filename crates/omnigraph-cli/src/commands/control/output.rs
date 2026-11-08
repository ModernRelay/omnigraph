//! Fallible presentation for applied cluster tooling and graph registry results.

use std::io::Write;

use color_eyre::eyre::Result;
use omnigraph_api_types::{
    GraphDiscoveryOutput, GraphListOutput, ParamDescriptor, ParamKind, QueriesCatalogOutput,
};
use omnigraph_server::{PolicyDecision, PolicyRequest};

use crate::output::write_json;

#[derive(serde::Serialize)]
pub(crate) struct QueriesIssue {
    pub(crate) query: String,
    pub(crate) message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) diagnostic: Option<omnigraph_api_types::DiagnosticOutput>,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesValidateOutput {
    pub(crate) ok: bool,
    pub(crate) breakages: Vec<QueriesIssue>,
    pub(crate) warnings: Vec<QueriesIssue>,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesParam {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) type_name: String,
    pub(crate) nullable: bool,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesListItem {
    pub(crate) name: String,
    pub(crate) mcp_expose: bool,
    pub(crate) tool_name: Option<String>,
    pub(crate) mutation: bool,
    /// `@description` from the query declaration — what the query is for.
    /// Carried so the CLI catalog matches the HTTP `GET /queries` surface.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) description: Option<String>,
    /// `@instruction` from the query declaration — how/when to invoke it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) instruction: Option<String>,
    pub(crate) params: Vec<QueriesParam>,
}

#[derive(serde::Serialize)]
pub(crate) struct QueriesListOutput {
    pub(crate) queries: Vec<QueriesListItem>,
}

pub(super) fn write_policy_explain(
    decision: &PolicyDecision,
    actor_id: &str,
    request: &PolicyRequest,
    writer: &mut dyn Write,
) -> Result<()> {
    writeln!(
        writer,
        "decision: {}",
        if decision.allowed { "allow" } else { "deny" }
    )?;
    writeln!(writer, "actor: {}", actor_id)?;
    writeln!(writer, "action: {}", request.action)?;
    if let Some(branch) = &request.branch {
        writeln!(writer, "branch: {}", branch)?;
    }
    if let Some(target_branch) = &request.target_branch {
        writeln!(writer, "target_branch: {}", target_branch)?;
    }
    if let Some(rule_id) = &decision.matched_rule_id {
        writeln!(writer, "matched_rule: {}", rule_id)?;
    }
    writeln!(writer, "message: {}", decision.message)?;
    Ok(())
}

pub(super) fn write_queries_validate(
    output: &QueriesValidateOutput,
    total: usize,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        write_json(output, stdout)?;
    } else {
        if output.breakages.is_empty() {
            writeln!(
                stdout,
                "OK  {} stored quer{} type-check against the schema",
                total,
                if total == 1 { "y" } else { "ies" }
            )?;
        }
        for issue in &output.breakages {
            writeln!(stdout, "ERROR  query '{}': {}", issue.query, issue.message)?;
            if let Some(diagnostic) = &issue.diagnostic {
                for line in crate::output::diagnostic_detail_lines(diagnostic) {
                    writeln!(stdout, "{line}")?;
                }
            }
        }
        for issue in &output.warnings {
            writeln!(stdout, "WARN   query '{}': {}", issue.query, issue.message)?;
        }
    }

    Ok(())
}

pub(super) fn write_queries_list(
    output: &QueriesListOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        write_json(output, stdout)?;
    } else if output.queries.is_empty() {
        writeln!(stdout, "(no stored queries registered)")?;
    } else {
        for q in &output.queries {
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
                .collect::<Vec<_>>();
            let mcp = q
                .mcp_expose
                .then(|| q.tool_name.as_deref().unwrap_or(&q.name));
            write_query_entry(
                QueryEntry {
                    mutation: q.mutation,
                    name: &q.name,
                    params: &params,
                    mcp,
                    description: q.description.as_deref(),
                    instruction: q.instruction.as_deref(),
                },
                stdout,
            )?;
        }
    }
    Ok(())
}

/// `queries list` on a server target: the route's body under `--json`, else
/// the direct form's layout with each parameter typed as the body spells it.
pub(super) fn write_queries_catalog(
    catalog: &QueriesCatalogOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        write_json(catalog, stdout)?;
    } else if catalog.queries.is_empty() {
        writeln!(stdout, "(no stored queries registered)")?;
    } else {
        for q in &catalog.queries {
            let params = q
                .params
                .iter()
                .map(|p| format!("${}: {}", p.name, catalog_param_type(p)))
                .collect::<Vec<_>>();
            // Every listed query is a tool; its id is news only when renamed.
            let mcp = (q.tool_name != q.name).then_some(q.tool_name.as_str());
            write_query_entry(
                QueryEntry {
                    mutation: q.mutation,
                    name: &q.name,
                    params: &params,
                    mcp,
                    description: q.description.as_deref(),
                    instruction: q.instruction.as_deref(),
                },
                stdout,
            )?;
        }
    }
    Ok(())
}

/// A catalog parameter's type in the body's spelling: its kind, a list as
/// `[item]`, a vector with its dimension, and `?` when it may be omitted.
fn catalog_param_type(param: &ParamDescriptor) -> String {
    let spell = |kind: ParamKind| match serde_json::to_value(kind) {
        Ok(serde_json::Value::String(name)) => name,
        _ => format!("{kind:?}"),
    };
    let base = match (param.kind, param.item_kind, param.vector_dim) {
        (ParamKind::List, Some(item), _) => format!("[{}]", spell(item)),
        (ParamKind::Vector, _, Some(dim)) => format!("{}({dim})", spell(ParamKind::Vector)),
        (kind, _, _) => spell(kind),
    };
    format!("{base}{}", if param.nullable { "?" } else { "" })
}

/// One `queries list` entry, shared by the direct and served forms.
struct QueryEntry<'a> {
    mutation: bool,
    name: &'a str,
    params: &'a [String],
    mcp: Option<&'a str>,
    description: Option<&'a str>,
    instruction: Option<&'a str>,
}

fn write_query_entry(entry: QueryEntry<'_>, stdout: &mut dyn Write) -> Result<()> {
    let kind = if entry.mutation { "mutation" } else { "read" };
    let mcp = entry
        .mcp
        .map_or_else(String::new, |tool| format!(" [mcp: {tool}]"));
    writeln!(
        stdout,
        "{kind}  {}({}){mcp}",
        entry.name,
        entry.params.join(", ")
    )?;
    if let Some(description) = entry.description {
        write_query_annotation("description", description, stdout)?;
    }
    if let Some(instruction) = entry.instruction {
        write_query_annotation("instruction", instruction, stdout)?;
    }
    Ok(())
}

/// Print a stored-query annotation under its `queries list` entry. A
/// `@description`/`@instruction` value may be multiline (GQ string literals
/// admit newlines); continuation lines are indented to align under the first
/// so the catalog stays readable instead of breaking the left margin.
fn write_query_annotation(label: &str, value: &str, stdout: &mut dyn Write) -> Result<()> {
    let prefix = format!("    {label}: ");
    let continuation = " ".repeat(prefix.len());
    let mut lines = value.split('\n');
    match lines.next() {
        Some(first) => {
            writeln!(stdout, "{prefix}{first}")?;
            for line in lines {
                writeln!(stdout, "{continuation}{line}")?;
            }
        }
        None => writeln!(stdout, "{prefix}")?,
    }
    Ok(())
}

pub(super) fn write_graphs_discovery(
    payload: &GraphDiscoveryOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        write_json(payload, stdout)?;
    } else {
        for entry in &payload.graphs {
            if entry.display_name == entry.graph_id {
                writeln!(stdout, "{}", entry.graph_id)?;
            } else {
                writeln!(stdout, "{}\t{}", entry.graph_id, entry.display_name)?;
            }
        }
    }
    Ok(())
}

pub(super) fn write_graphs_list(
    payload: &GraphListOutput,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    if json {
        write_json(payload, stdout)?;
    } else {
        for entry in &payload.graphs {
            writeln!(stdout, "{}\t{}\t{}", entry.graph_id, entry.state, entry.uri)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_api_types::{
        GraphAvailability, GraphAvailabilityAction, GraphDiscoveryEntry, GraphInfo,
    };
    use omnigraph_server::PolicyAction;
    use serde_json::json;

    #[test]
    fn stored_query_list_preserves_annotations_and_json_fields() {
        let output = QueriesListOutput {
            queries: vec![QueriesListItem {
                name: "people".into(),
                mcp_expose: true,
                tool_name: None,
                mutation: false,
                description: Some("Find people\nby name".into()),
                instruction: Some(String::new()),
                params: vec![QueriesParam {
                    name: "name".into(),
                    type_name: "String".into(),
                    nullable: true,
                }],
            }],
        };
        let mut human = Vec::new();
        write_queries_list(&output, false, &mut human).unwrap();
        assert_eq!(
            String::from_utf8(human).unwrap(),
            concat!(
                "read  people($name: String?) [mcp: people]\n",
                "    description: Find people\n",
                "                 by name\n",
                "    instruction: \n",
            )
        );
        let mut encoded = Vec::new();
        write_queries_list(&output, true, &mut encoded).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
            json!({
                "queries": [{"name":"people", "mcp_expose":true, "tool_name":null,
                    "mutation":false, "description":"Find people\nby name", "instruction":"",
                    "params":[{"name":"name", "type":"String", "nullable":true}]}]
            })
        );
        assert_eq!(encoded.last(), Some(&b'\n'));
        for json in [false, true] {
            let mut full: &mut [u8] = &mut [];
            assert!(write_queries_list(&output, json, &mut full).is_err());
        }
        let mut empty = Vec::new();
        write_queries_list(&QueriesListOutput { queries: vec![] }, false, &mut empty).unwrap();
        assert_eq!(empty, b"(no stored queries registered)\n");
    }

    #[test]
    fn served_query_catalog_prints_the_route_body_and_the_direct_layout() {
        let param = |name: &str, kind, item_kind, vector_dim, nullable| ParamDescriptor {
            name: name.into(),
            kind,
            item_kind,
            vector_dim,
            nullable,
        };
        let catalog = QueriesCatalogOutput {
            queries: vec![
                omnigraph_api_types::QueryCatalogEntry {
                    name: "people".into(),
                    tool_name: "people".into(),
                    description: Some("Find people\nby name".into()),
                    instruction: None,
                    mutation: false,
                    params: vec![
                        param("name", ParamKind::String, None, None, true),
                        param(
                            "tags",
                            ParamKind::List,
                            Some(ParamKind::String),
                            None,
                            false,
                        ),
                        param("near", ParamKind::Vector, None, Some(3), false),
                        param("since", ParamKind::DateTime, None, None, false),
                    ],
                },
                omnigraph_api_types::QueryCatalogEntry {
                    name: "retag".into(),
                    tool_name: "retag_people".into(),
                    description: None,
                    instruction: Some("Use for bulk changes".into()),
                    mutation: true,
                    params: vec![param("count", ParamKind::BigInt, None, None, false)],
                },
            ],
        };
        let mut human = Vec::new();
        write_queries_catalog(&catalog, false, &mut human).unwrap();
        assert_eq!(
            String::from_utf8(human).unwrap(),
            concat!(
                "read  people($name: string?, $tags: [string], $near: vector(3), ",
                "$since: datetime)\n",
                "    description: Find people\n",
                "                 by name\n",
                "mutation  retag($count: bigint) [mcp: retag_people]\n",
                "    instruction: Use for bulk changes\n",
            )
        );
        let mut encoded = Vec::new();
        write_queries_catalog(&catalog, true, &mut encoded).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
            serde_json::to_value(&catalog).unwrap()
        );
        assert_eq!(encoded.last(), Some(&b'\n'));
        for json in [false, true] {
            let mut full: &mut [u8] = &mut [];
            assert!(write_queries_catalog(&catalog, json, &mut full).is_err());
        }
        let mut empty = Vec::new();
        write_queries_catalog(&QueriesCatalogOutput { queries: vec![] }, false, &mut empty)
            .unwrap();
        assert_eq!(empty, b"(no stored queries registered)\n");
    }

    #[test]
    fn stored_query_validation_preserves_success_errors_and_warnings() {
        let mut report = QueriesValidateOutput {
            ok: true,
            breakages: vec![],
            warnings: vec![],
        };
        for (total, expected) in [
            (1, "OK  1 stored query type-check against the schema\n"),
            (2, "OK  2 stored queries type-check against the schema\n"),
        ] {
            let mut human = Vec::new();
            write_queries_validate(&report, total, false, &mut human).unwrap();
            assert_eq!(human, expected.as_bytes());
        }
        report.ok = false;
        report.breakages.push(QueriesIssue {
            query: "broken".into(),
            message: "unknown node".into(),
            diagnostic: None,
        });
        report.warnings.push(QueriesIssue {
            query: "unused".into(),
            message: "unused parameter".into(),
            diagnostic: None,
        });
        let mut human = Vec::new();
        write_queries_validate(&report, 2, false, &mut human).unwrap();
        assert_eq!(
            human,
            b"ERROR  query 'broken': unknown node\nWARN   query 'unused': unused parameter\n"
        );
        let mut encoded = Vec::new();
        write_queries_validate(&report, 2, true, &mut encoded).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
            json!({
                "ok":false,
                "breakages":[{"query":"broken", "message":"unknown node"}],
                "warnings":[{"query":"unused", "message":"unused parameter"}]
            })
        );
        for json in [false, true] {
            let mut full: &mut [u8] = &mut [];
            assert!(write_queries_validate(&report, 2, json, &mut full).is_err());
        }
    }

    #[test]
    fn policy_explanation_preserves_optional_context_and_write_failures() {
        let mut request = PolicyRequest {
            action: PolicyAction::BranchMerge,
            branch: Some("feature".into()),
            target_branch: Some("main".into()),
        };
        let mut decision = PolicyDecision {
            allowed: true,
            matched_rule_id: Some("reviewers".into()),
            message: "permitted".into(),
        };
        let mut human = Vec::new();
        write_policy_explain(&decision, "alice", &request, &mut human).unwrap();
        assert_eq!(human, concat!(
            "decision: allow\nactor: alice\naction: branch_merge\n",
            "branch: feature\ntarget_branch: main\nmatched_rule: reviewers\nmessage: permitted\n",
        ).as_bytes());
        request.branch = None;
        request.target_branch = None;
        decision.allowed = false;
        decision.matched_rule_id = None;
        decision.message = "denied".into();
        human.clear();
        write_policy_explain(&decision, "alice", &request, &mut human).unwrap();
        assert_eq!(
            human,
            b"decision: deny\nactor: alice\naction: branch_merge\nmessage: denied\n"
        );
        let mut full: &mut [u8] = &mut [];
        assert!(write_policy_explain(&decision, "alice", &request, &mut full).is_err());
    }

    #[test]
    fn graph_inventory_keeps_discovery_and_operator_shapes() {
        let discovery = GraphDiscoveryOutput {
            graphs: vec![
                GraphDiscoveryEntry {
                    graph_id: "a".into(),
                    display_name: "a".into(),
                },
                GraphDiscoveryEntry {
                    graph_id: "b".into(),
                    display_name: "Bee".into(),
                },
            ],
        };
        let registry = GraphListOutput {
            graphs: vec![GraphInfo {
                graph_id: "a".into(),
                uri: "/graph.omni".into(),
                state: GraphAvailability::Ready,
                read_available: true,
                write_available: true,
                failure: None,
                action: GraphAvailabilityAction::None,
            }],
        };
        let mut human = Vec::new();
        write_graphs_discovery(&discovery, false, &mut human).unwrap();
        assert_eq!(human, b"a\nb\tBee\n");
        human.clear();
        write_graphs_list(&registry, false, &mut human).unwrap();
        assert_eq!(human, b"a\tready\t/graph.omni\n");
        let mut encoded = Vec::new();
        write_graphs_discovery(&discovery, true, &mut encoded).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
            json!({
                "graphs":[{"graph_id":"a","display_name":"a"},{"graph_id":"b","display_name":"Bee"}]
            })
        );
        encoded.clear();
        write_graphs_list(&registry, true, &mut encoded).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
            json!({
                "graphs":[{"graph_id":"a","uri":"/graph.omni","state":"ready",
                    "read_available":true,"write_available":true,"action":"none"}]
            })
        );
        for json in [false, true] {
            let mut full: &mut [u8] = &mut [];
            assert!(write_graphs_discovery(&discovery, json, &mut full).is_err());
            assert!(write_graphs_list(&registry, json, &mut full).is_err());
        }
    }
}
