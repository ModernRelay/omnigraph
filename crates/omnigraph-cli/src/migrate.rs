//! `omnigraph config migrate` (RFC-008 stage 2): split a legacy
//! `omnigraph.yaml` into its two destinations — the team half as a
//! ready-to-review `cluster.yaml` proposal, the personal half merged into
//! `~/.omnigraph/config.yaml` — and name what's obsolete. The command is
//! the completeness test of RFC-008's migration map: any key it cannot
//! place is a bug in the RFC.
//!
//! Touches nothing without `--write`. Referenced `.gq`/policy files are
//! never moved; manual steps are printed instead.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use color_eyre::Result;
use color_eyre::eyre::eyre;
use omnigraph_server::OmnigraphConfig;
use serde::Serialize;

use crate::operator;

#[derive(Debug, Serialize)]
pub(crate) struct MigrateReport {
    pub(crate) source: String,
    /// The ready-to-review cluster.yaml text (None when the legacy file
    /// declares nothing team-shaped).
    pub(crate) cluster_yaml: Option<String>,
    /// Operator keys to merge: dotted key -> YAML value text.
    pub(crate) operator_merge: BTreeMap<String, String>,
    /// Keys with no destination, and why.
    pub(crate) dropped: Vec<DroppedKey>,
    /// Steps the command will not do for you.
    pub(crate) manual_steps: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DroppedKey {
    pub(crate) key: String,
    pub(crate) reason: String,
}

/// Classify a parsed legacy config into the report. Pure — no I/O.
pub(crate) fn build_report(config: &OmnigraphConfig, source: &Path) -> MigrateReport {
    let mut dropped = Vec::new();
    let mut manual_steps = Vec::new();
    let mut operator_merge: BTreeMap<String, String> = BTreeMap::new();

    // ---- personal half ----
    if let Some(actor) = &config.cli.actor {
        operator_merge.insert("operator.actor".into(), actor.clone());
    }
    if let Some(format) = config.cli.output_format {
        operator_merge.insert(
            "defaults.output".into(),
            serde_yaml::to_string(&format).unwrap_or_default().trim().to_string(),
        );
    }
    if let Some(width) = config.cli.table_max_column_width {
        operator_merge.insert("defaults.table_max_column_width".into(), width.to_string());
    }
    if let Some(layout) = config.cli.table_cell_layout {
        operator_merge.insert(
            "defaults.table_cell_layout".into(),
            serde_yaml::to_string(&layout).unwrap_or_default().trim().to_string(),
        );
    }
    if config.cli.graph.is_some() {
        dropped.push(DroppedKey {
            key: "cli.graph".into(),
            reason: "no operator default-target yet — address graphs explicitly via --target/--server (RFC-002 locator territory)".into(),
        });
    }
    if config.cli.branch.is_some() {
        dropped.push(DroppedKey {
            key: "cli.branch".into(),
            reason: "pass --branch explicitly".into(),
        });
    }

    // Remote graphs with a token env become operator servers (the keyed
    // chain replaces invented env-var names).
    for (name, target) in &config.graphs {
        if target.uri.starts_with("http://") || target.uri.starts_with("https://") {
            operator_merge.insert(format!("servers.{name}.url"), target.uri.clone());
            if target.bearer_token_env.is_some() {
                manual_steps.push(format!(
                    "store the '{name}' token in the keyed chain: echo $TOKEN | omnigraph login {name} (replaces bearer_token_env)"
                ));
            }
        }
    }
    if config.auth.env_file.is_some() {
        manual_steps.push(
            "auth.env_file keeps working during the window; prefer `omnigraph login <server>` per server going forward".into(),
        );
    }

    // Legacy aliases split: content -> catalog stored query, binding ->
    // operator alias referencing the name.
    for (name, alias) in &config.aliases {
        let query_name = alias.name.clone().unwrap_or_else(|| name.clone());
        operator_merge.insert(
            format!("aliases.{name}"),
            format!(
                "{{ server: TODO-server-name, graph: {}, query: {query_name}, args: [{}] }}",
                alias.graph.as_deref().unwrap_or("TODO-graph-id"),
                alias.args.join(", ")
            ),
        );
        manual_steps.push(format!(
            "alias '{name}': move its query content ('{}') into the cluster checkout's queries/ so '{query_name}' becomes a catalog stored query",
            alias.query
        ));
    }

    // ---- team half ----
    let has_team_content = !config.graphs.is_empty()
        || !config.queries.is_empty()
        || config.policy.file.is_some()
        || config.server.policy.file.is_some();
    let cluster_yaml = has_team_content.then(|| {
        let mut out = String::from("version: 1\n");
        if let Some(name) = &config.project.name {
            out.push_str(&format!("metadata:\n  name: {name}\n"));
        }
        out.push_str("# storage: s3://bucket/prefix   # or omit: this folder is the root\n");
        if !config.graphs.is_empty() || !config.queries.is_empty() {
            out.push_str("graphs:\n");
        }
        // Single-graph top-level queries belong to a graph the legacy file
        // never named; propose one.
        if !config.queries.is_empty() && config.graphs.is_empty() {
            out.push_str("  default:   # TODO: pick the graph id\n    schema: # TODO: path to this graph's .pg schema\n    queries: queries/\n");
        }
        for (name, target) in &config.graphs {
            out.push_str(&format!("  {name}:\n"));
            out.push_str("    schema: # TODO: path to this graph's .pg schema\n");
            if !target.queries.is_empty() {
                out.push_str("    queries: queries/   # move the .gq files here\n");
            }
            out.push_str(&format!(
                "    # legacy root: {} — the cluster manages graph roots under its storage; run `omnigraph cluster import` after reviewing\n",
                target.uri
            ));
        }
        let mut policies: Vec<(String, String, String)> = Vec::new();
        if let Some(file) = &config.policy.file {
            policies.push(("default".into(), file.clone(), "graph.<id>  # TODO: bind".into()));
        }
        if let Some(file) = &config.server.policy.file {
            policies.push(("server".into(), file.clone(), "cluster".into()));
        }
        for (name, target) in &config.graphs {
            if let Some(file) = &target.policy.file {
                policies.push((name.clone(), file.clone(), format!("graph.{name}")));
            }
        }
        if !policies.is_empty() {
            out.push_str("policies:\n");
            for (name, file, binding) in policies {
                out.push_str(&format!(
                    "  {name}:\n    file: {file}\n    applies_to: [{binding}]\n"
                ));
            }
        }
        out
    });

    if !config.query.roots.is_empty() {
        dropped.push(DroppedKey {
            key: "query.roots".into(),
            reason: "obsolete — cluster query discovery (queries: <dir>) replaced it".into(),
        });
    }
    if config.server.bind.is_some() || config.server.graph.is_some() {
        dropped.push(DroppedKey {
            key: "server.bind / server.graph".into(),
            reason: "deployment runtime — pass --bind / target flags or env".into(),
        });
    }
    if config.project.name.is_some() && cluster_yaml.is_none() {
        dropped.push(DroppedKey {
            key: "project.name".into(),
            reason: "the cluster's metadata.name is the deployment label".into(),
        });
    }

    MigrateReport {
        source: source.display().to_string(),
        cluster_yaml,
        operator_merge,
        dropped,
        manual_steps,
    }
}

pub(crate) fn render_report(report: &MigrateReport) -> String {
    let mut out = format!("migration plan for {}\n", report.source);
    if let Some(cluster) = &report.cluster_yaml {
        out.push_str("\n== team half -> cluster.yaml (ready to review) ==\n");
        out.push_str(cluster);
    }
    if !report.operator_merge.is_empty() {
        out.push_str("\n== personal half -> ~/.omnigraph/config.yaml ==\n");
        for (key, value) in &report.operator_merge {
            out.push_str(&format!("  {key}: {value}\n"));
        }
    }
    if !report.dropped.is_empty() {
        out.push_str("\n== no destination ==\n");
        for dropped in &report.dropped {
            out.push_str(&format!("  {} — {}\n", dropped.key, dropped.reason));
        }
    }
    if !report.manual_steps.is_empty() {
        out.push_str("\n== manual steps ==\n");
        for step in &report.manual_steps {
            out.push_str(&format!("  - {step}\n"));
        }
    }
    out.push_str("\n(nothing written; pass --write to apply the operator merge and emit cluster.yaml)\n");
    out
}

/// `--write`: merge the personal half into the operator config (key-level,
/// existing entries always win; the prior file is backed up) and write the
/// team half to cluster.yaml in the legacy config's directory (or
/// cluster.yaml.proposed when one already exists).
pub(crate) fn apply_report(report: &MigrateReport, legacy_dir: &Path) -> Result<Vec<String>> {
    let mut written = Vec::new();

    if !report.operator_merge.is_empty() {
        let dir = operator::operator_dir()
            .ok_or_else(|| eyre!("no home directory resolvable for the operator config"))?;
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(operator::OPERATOR_CONFIG_FILE);
        let existing_text = std::fs::read_to_string(&path).unwrap_or_default();
        let mut mapping: serde_yaml::Mapping = if existing_text.trim().is_empty() {
            serde_yaml::Mapping::new()
        } else {
            serde_yaml::from_str(&existing_text)
                .map_err(|err| eyre!("operator config '{}' does not parse: {err}", path.display()))?
        };
        let mut merged_any = false;
        for (dotted, value_text) in &report.operator_merge {
            if merge_dotted_if_absent(&mut mapping, dotted, value_text)? {
                merged_any = true;
            }
        }
        if merged_any {
            if !existing_text.is_empty() {
                let backup = path.with_extension("yaml.bak");
                std::fs::write(&backup, &existing_text)?;
                written.push(format!("backed up prior operator config to {}", backup.display()));
            }
            let rendered = serde_yaml::to_string(&mapping)?;
            let tmp = path.with_extension(format!("yaml.tmp.{}", std::process::id()));
            std::fs::write(&tmp, &rendered)?;
            std::fs::rename(&tmp, &path)?;
            written.push(format!("merged personal keys into {}", path.display()));
        } else {
            written.push("operator config already carries every personal key (nothing merged)".into());
        }
    }

    if let Some(cluster) = &report.cluster_yaml {
        let target = legacy_dir.join("cluster.yaml");
        let target = if target.exists() {
            legacy_dir.join("cluster.yaml.proposed")
        } else {
            target
        };
        std::fs::write(&target, cluster)?;
        written.push(format!("wrote team-half proposal to {}", target.display()));
    }

    Ok(written)
}

/// Set `a.b.c` in the mapping only when absent; returns whether it wrote.
fn merge_dotted_if_absent(
    mapping: &mut serde_yaml::Mapping,
    dotted: &str,
    value_text: &str,
) -> Result<bool> {
    let value: serde_yaml::Value =
        serde_yaml::from_str(value_text).unwrap_or(serde_yaml::Value::String(value_text.into()));
    let parts: Vec<&str> = dotted.split('.').collect();
    let mut current = mapping;
    for part in &parts[..parts.len() - 1] {
        let key = serde_yaml::Value::String((*part).into());
        let entry = current
            .entry(key)
            .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
        current = entry
            .as_mapping_mut()
            .ok_or_else(|| eyre!("operator config key '{dotted}' collides with a non-mapping"))?;
    }
    let leaf = serde_yaml::Value::String(parts[parts.len() - 1].into());
    if current.contains_key(&leaf) {
        return Ok(false);
    }
    current.insert(leaf, value);
    Ok(true)
}

pub(crate) fn legacy_config_path(explicit: Option<&PathBuf>) -> PathBuf {
    explicit.cloned().unwrap_or_else(|| PathBuf::from("omnigraph.yaml"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_server::config::load_config;

    fn full_legacy_fixture(dir: &Path) -> PathBuf {
        let path = dir.join("omnigraph.yaml");
        std::fs::write(
            &path,
            r#"
project: { name: brain }
graphs:
  prod:
    uri: https://graph.example.com
    bearer_token_env: PROD_TOKEN
    policy: { file: ./prod.policy.yaml }
    queries:
      find: { file: ./find.gq }
  local:
    uri: /tmp/local.omni
server: { bind: "0.0.0.0:9999", policy: { file: ./server.policy.yaml } }
auth: { env_file: .env.omni }
cli:
  graph: prod
  branch: main
  actor: act-me
  output_format: json
  table_max_column_width: 40
query: { roots: ["."] }
aliases:
  triage: { command: query, query: ./triage.gq, name: weekly_triage, args: [since], graph: prod }
policy: { file: ./top.policy.yaml }
queries:
  top_q: { file: ./top.gq }
"#,
        )
        .unwrap();
        path
    }

    /// The RFC-008 completeness contract: every top-level key of the
    /// legacy schema must appear in the report somewhere (team half,
    /// operator merge, dropped, or manual steps).
    #[test]
    fn every_legacy_key_is_classified() {
        let dir = tempfile::tempdir().unwrap();
        let path = full_legacy_fixture(dir.path());
        let config = load_config(Some(&path)).unwrap();
        let report = build_report(&config, &path);
        let rendered = render_report(&report);

        let serialized =
            serde_yaml::to_value(OmnigraphConfig::default()).expect("default serializes");
        for key in serialized.as_mapping().unwrap().keys() {
            let key = key.as_str().unwrap();
            assert!(
                rendered.contains(key)
                    || report.operator_merge.keys().any(|k| k.contains(key))
                    || matches!(key, "graphs" | "queries" | "policy" | "project")
                        && report.cluster_yaml.is_some(),
                "legacy key '{key}' is unclassified — fix the RFC-008 map: {rendered}"
            );
        }

        // spot checks on each section
        assert_eq!(report.operator_merge["operator.actor"], "act-me");
        assert_eq!(report.operator_merge["defaults.output"], "json");
        assert_eq!(
            report.operator_merge["servers.prod.url"],
            "https://graph.example.com"
        );
        assert!(report.operator_merge["aliases.triage"].contains("query: weekly_triage"));
        let cluster = report.cluster_yaml.as_deref().unwrap();
        assert!(cluster.contains("version: 1"));
        assert!(cluster.contains("name: brain"));
        assert!(cluster.contains("  prod:"));
        assert!(cluster.contains("applies_to: [cluster]"));
        assert!(cluster.contains("applies_to: [graph.prod]"));
        assert!(report.dropped.iter().any(|d| d.key == "query.roots"));
        assert!(report.dropped.iter().any(|d| d.key.contains("server.bind")));
        assert!(
            report
                .manual_steps
                .iter()
                .any(|s| s.contains("omnigraph login prod"))
        );
    }

    #[test]
    fn merge_dotted_never_clobbers_existing() {
        let mut mapping: serde_yaml::Mapping =
            serde_yaml::from_str("operator:\n  actor: keep-me\n").unwrap();
        assert!(!merge_dotted_if_absent(&mut mapping, "operator.actor", "new").unwrap());
        assert!(merge_dotted_if_absent(&mut mapping, "defaults.output", "json").unwrap());
        let text = serde_yaml::to_string(&mapping).unwrap();
        assert!(text.contains("keep-me") && !text.contains("new"));
        assert!(text.contains("output: json"));
    }
}
