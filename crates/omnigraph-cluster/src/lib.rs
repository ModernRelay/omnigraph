use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use omnigraph_compiler::build_catalog;
use omnigraph_compiler::query::parser::parse_query;
use omnigraph_compiler::query::typecheck::typecheck_query_decl;
use omnigraph_compiler::schema::parser::parse_schema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CLUSTER_CONFIG_FILE: &str = "cluster.yaml";
pub const CLUSTER_STATE_FILE: &str = "__cluster/state.json";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Diagnostic {
    pub code: String,
    pub severity: DiagnosticSeverity,
    pub path: String,
    pub message: String,
}

impl Diagnostic {
    fn error(code: impl Into<String>, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            severity: DiagnosticSeverity::Error,
            path: path.into(),
            message: message.into(),
        }
    }

    fn warning(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            severity: DiagnosticSeverity::Warning,
            path: path.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResourceSummary {
    pub address: String,
    pub kind: String,
    pub digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dependency {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidateOutput {
    pub ok: bool,
    pub config_dir: String,
    pub config_file: String,
    pub resource_digests: BTreeMap<String, String>,
    pub resources: Vec<ResourceSummary>,
    pub dependencies: Vec<Dependency>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DesiredRevision {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StateObservations {
    pub state_path: String,
    pub state_found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_config_digest: Option<String>,
    pub resource_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanOperation {
    Create,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PlanChange {
    pub resource: String,
    pub operation: PlanOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct BlastRadius {
    pub resource: String,
    pub affected: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ApprovalRequirement {
    pub resource: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PlanOutput {
    pub ok: bool,
    pub config_dir: String,
    pub desired_revision: DesiredRevision,
    pub resource_digests: BTreeMap<String, String>,
    pub dependencies: Vec<Dependency>,
    pub state_observations: StateObservations,
    pub changes: Vec<PlanChange>,
    pub blast_radius: Vec<BlastRadius>,
    pub approvals_required: Vec<ApprovalRequirement>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone)]
struct DesiredCluster {
    config_dir: PathBuf,
    config_digest: String,
    resource_digests: BTreeMap<String, String>,
    resources: Vec<ResourceSummary>,
    dependencies: Vec<Dependency>,
}

#[derive(Debug)]
struct LoadOutcome {
    desired: Option<DesiredCluster>,
    diagnostics: Vec<Diagnostic>,
    config_dir: PathBuf,
    config_file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawClusterConfig {
    version: u32,
    #[serde(default)]
    metadata: Metadata,
    #[serde(default)]
    state: StateConfig,
    #[serde(default)]
    graphs: BTreeMap<String, GraphConfig>,
    #[serde(default)]
    policies: BTreeMap<String, PolicyConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    name: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateConfig {
    backend: Option<String>,
    lock: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphConfig {
    schema: PathBuf,
    #[serde(default)]
    queries: BTreeMap<String, QueryConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryConfig {
    file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyConfig {
    file: PathBuf,
    applies_to: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClusterState {
    version: u32,
    applied_revision: AppliedRevisionState,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppliedRevisionState {
    #[serde(default)]
    config_digest: Option<String>,
    #[serde(default)]
    resources: BTreeMap<String, StateResource>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StateResource {
    digest: String,
}

pub fn validate_config_dir(config_dir: impl AsRef<Path>) -> ValidateOutput {
    let outcome = load_desired(config_dir.as_ref());
    let (resource_digests, resources, dependencies) = match outcome.desired {
        Some(desired) => (
            desired.resource_digests,
            desired.resources,
            desired.dependencies,
        ),
        None => (BTreeMap::new(), Vec::new(), Vec::new()),
    };
    let ok = !has_errors(&outcome.diagnostics);

    ValidateOutput {
        ok,
        config_dir: display_path(&outcome.config_dir),
        config_file: display_path(&outcome.config_file),
        resource_digests,
        resources,
        dependencies,
        diagnostics: outcome.diagnostics,
    }
}

pub fn plan_config_dir(config_dir: impl AsRef<Path>) -> PlanOutput {
    let outcome = load_desired(config_dir.as_ref());
    let mut diagnostics = outcome.diagnostics;
    let state_path = outcome.config_dir.join(CLUSTER_STATE_FILE);
    let mut observations = StateObservations {
        state_path: display_path(&state_path),
        state_found: false,
        applied_config_digest: None,
        resource_count: 0,
    };

    let Some(desired) = outcome.desired else {
        return PlanOutput {
            ok: false,
            config_dir: display_path(&outcome.config_dir),
            desired_revision: DesiredRevision {
                config_digest: None,
            },
            resource_digests: BTreeMap::new(),
            dependencies: Vec::new(),
            state_observations: observations,
            changes: Vec::new(),
            blast_radius: Vec::new(),
            approvals_required: Vec::new(),
            diagnostics,
        };
    };

    let mut prior_resources = BTreeMap::new();
    if state_path.exists() {
        observations.state_found = true;
        match fs::read_to_string(&state_path) {
            Ok(text) => match serde_json::from_str::<ClusterState>(&text) {
                Ok(state) if state.version == 1 => {
                    observations.applied_config_digest = state.applied_revision.config_digest;
                    observations.resource_count = state.applied_revision.resources.len();
                    prior_resources = state
                        .applied_revision
                        .resources
                        .into_iter()
                        .map(|(address, resource)| (address, resource.digest))
                        .collect();
                }
                Ok(state) => diagnostics.push(Diagnostic::error(
                    "unsupported_state_version",
                    "state.version",
                    format!(
                        "unsupported cluster state version {}; this build supports version 1",
                        state.version
                    ),
                )),
                Err(err) => diagnostics.push(Diagnostic::error(
                    "invalid_state_json",
                    CLUSTER_STATE_FILE,
                    format!("could not parse state JSON: {err}"),
                )),
            },
            Err(err) => diagnostics.push(Diagnostic::error(
                "state_read_error",
                CLUSTER_STATE_FILE,
                format!("could not read state file: {err}"),
            )),
        }
    }

    let changes = if has_errors(&diagnostics) {
        Vec::new()
    } else {
        diff_resources(&prior_resources, &desired.resource_digests)
    };
    let blast_radius = compute_blast_radius(&changes, &desired.dependencies);
    let approvals_required = compute_approvals(&changes);
    let ok = !has_errors(&diagnostics);

    PlanOutput {
        ok,
        config_dir: display_path(&desired.config_dir),
        desired_revision: DesiredRevision {
            config_digest: Some(desired.config_digest),
        },
        resource_digests: desired.resource_digests,
        dependencies: desired.dependencies,
        state_observations: observations,
        changes,
        blast_radius,
        approvals_required,
        diagnostics,
    }
}

fn load_desired(config_dir: &Path) -> LoadOutcome {
    let config_dir = config_dir.to_path_buf();
    let config_file = config_dir.join(CLUSTER_CONFIG_FILE);
    let mut diagnostics = Vec::new();

    if !config_dir.is_dir() {
        diagnostics.push(Diagnostic::error(
            "config_dir_not_found",
            display_path(&config_dir),
            "`--config` must point at a directory containing cluster.yaml",
        ));
        return LoadOutcome {
            desired: None,
            diagnostics,
            config_dir,
            config_file,
        };
    }

    let text = match fs::read_to_string(&config_file) {
        Ok(text) => text,
        Err(err) => {
            diagnostics.push(Diagnostic::error(
                "cluster_config_read_error",
                CLUSTER_CONFIG_FILE,
                format!("could not read cluster.yaml: {err}"),
            ));
            return LoadOutcome {
                desired: None,
                diagnostics,
                config_dir,
                config_file,
            };
        }
    };

    diagnostics.extend(duplicate_key_diagnostics(&text));
    diagnostics.extend(future_field_diagnostics(&text));
    if has_errors(&diagnostics) {
        return LoadOutcome {
            desired: None,
            diagnostics,
            config_dir,
            config_file,
        };
    }

    let raw = match serde_yaml::from_str::<RawClusterConfig>(&text) {
        Ok(raw) => raw,
        Err(err) => {
            diagnostics.push(Diagnostic::error(
                "invalid_cluster_yaml",
                CLUSTER_CONFIG_FILE,
                format!("could not parse cluster.yaml: {err}"),
            ));
            return LoadOutcome {
                desired: None,
                diagnostics,
                config_dir,
                config_file,
            };
        }
    };

    if raw.version != 1 {
        diagnostics.push(Diagnostic::error(
            "unsupported_cluster_config_version",
            "version",
            format!(
                "unsupported cluster config version {}; this build supports version 1",
                raw.version
            ),
        ));
    }
    if let Some(name) = raw.metadata.name.as_deref() {
        if name.trim().is_empty() {
            diagnostics.push(Diagnostic::error(
                "empty_metadata_name",
                "metadata.name",
                "metadata.name must not be empty when provided",
            ));
        }
    }
    if let Some(backend) = raw.state.backend.as_deref() {
        if backend != "cluster" {
            diagnostics.push(Diagnostic::error(
                "unsupported_state_backend",
                "state.backend",
                "Stage 1 supports only omitted state.backend or `cluster`",
            ));
        }
    }
    let _lock_parsed_for_forward_compat = raw.state.lock;

    let mut resources = BTreeMap::new();
    let mut dependencies = BTreeSet::new();
    let mut graph_query_digests: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut graph_schema_digests: BTreeMap<String, String> = BTreeMap::new();

    for (graph_id, graph) in &raw.graphs {
        validate_id(
            "graph id",
            &format!("graphs.{graph_id}"),
            graph_id,
            &mut diagnostics,
        );
        let graph_address = graph_address(graph_id);
        let schema_address = schema_address(graph_id);
        dependencies.insert(Dependency {
            from: schema_address.clone(),
            to: graph_address.clone(),
        });

        let schema_path = resolve_config_path(&config_dir, &graph.schema);
        let schema_source = match fs::read_to_string(&schema_path) {
            Ok(source) => {
                let digest = sha256_hex(source.as_bytes());
                graph_schema_digests.insert(graph_id.clone(), digest.clone());
                resources.insert(
                    schema_address.clone(),
                    ResourceSummary {
                        address: schema_address.clone(),
                        kind: "schema".to_string(),
                        digest,
                        path: Some(display_path(&schema_path)),
                    },
                );
                Some(source)
            }
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_file_missing",
                    format!("graphs.{graph_id}.schema"),
                    format!(
                        "could not read schema file '{}': {err}",
                        schema_path.display()
                    ),
                ));
                None
            }
        };

        let catalog = schema_source.and_then(|source| match parse_schema(&source) {
            Ok(schema) => match build_catalog(&schema) {
                Ok(catalog) => Some(catalog),
                Err(err) => {
                    diagnostics.push(Diagnostic::error(
                        "schema_catalog_error",
                        format!("graphs.{graph_id}.schema"),
                        err.to_string(),
                    ));
                    None
                }
            },
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "schema_parse_error",
                    format!("graphs.{graph_id}.schema"),
                    err.to_string(),
                ));
                None
            }
        });

        for (query_name, query) in &graph.queries {
            validate_id(
                "query name",
                &format!("graphs.{graph_id}.queries.{query_name}"),
                query_name,
                &mut diagnostics,
            );
            let query_address = query_address(graph_id, query_name);
            dependencies.insert(Dependency {
                from: query_address.clone(),
                to: graph_address.clone(),
            });
            dependencies.insert(Dependency {
                from: query_address.clone(),
                to: schema_address.clone(),
            });

            let query_path = resolve_config_path(&config_dir, &query.file);
            match fs::read_to_string(&query_path) {
                Ok(source) => {
                    let digest = sha256_hex(source.as_bytes());
                    graph_query_digests
                        .entry(graph_id.clone())
                        .or_default()
                        .insert(query_name.clone(), digest.clone());
                    resources.insert(
                        query_address.clone(),
                        ResourceSummary {
                            address: query_address,
                            kind: "query".to_string(),
                            digest,
                            path: Some(display_path(&query_path)),
                        },
                    );
                    validate_query_source(
                        graph_id,
                        query_name,
                        &source,
                        catalog.as_ref(),
                        &mut diagnostics,
                    );
                }
                Err(err) => diagnostics.push(Diagnostic::error(
                    "query_file_missing",
                    format!("graphs.{graph_id}.queries.{query_name}.file"),
                    format!(
                        "could not read query file '{}': {err}",
                        query_path.display()
                    ),
                )),
            }
        }
    }

    for graph_id in raw.graphs.keys() {
        let digest = graph_digest(
            graph_id,
            graph_schema_digests.get(graph_id),
            graph_query_digests.get(graph_id),
        );
        resources.insert(
            graph_address(graph_id),
            ResourceSummary {
                address: graph_address(graph_id),
                kind: "graph".to_string(),
                digest,
                path: None,
            },
        );
    }

    for (policy_name, policy) in &raw.policies {
        validate_id(
            "policy name",
            &format!("policies.{policy_name}"),
            policy_name,
            &mut diagnostics,
        );
        if policy.applies_to.is_empty() {
            diagnostics.push(Diagnostic::error(
                "policy_missing_applies_to",
                format!("policies.{policy_name}.applies_to"),
                "policy.applies_to must name `cluster` or at least one graph",
            ));
        }

        let policy_address = policy_address(policy_name);
        for (idx, target) in policy.applies_to.iter().enumerate() {
            match normalize_policy_target(target) {
                PolicyTarget::Cluster => {}
                PolicyTarget::Graph(graph_id) => {
                    if raw.graphs.contains_key(&graph_id) {
                        dependencies.insert(Dependency {
                            from: policy_address.clone(),
                            to: graph_address(&graph_id),
                        });
                    } else {
                        diagnostics.push(Diagnostic::error(
                            "dangling_graph_reference",
                            format!("policies.{policy_name}.applies_to[{idx}]"),
                            format!(
                                "policy references graph `{graph_id}`, but no graph with that id is declared"
                            ),
                        ));
                    }
                }
                PolicyTarget::WrongKind(kind) => diagnostics.push(Diagnostic::error(
                    "wrong_kind_reference",
                    format!("policies.{policy_name}.applies_to[{idx}]"),
                    format!("policy applies_to expects graph refs or `cluster`, got `{kind}`"),
                )),
            }
        }

        let policy_path = resolve_config_path(&config_dir, &policy.file);
        match fs::read(&policy_path) {
            Ok(bytes) => {
                resources.insert(
                    policy_address.clone(),
                    ResourceSummary {
                        address: policy_address,
                        kind: "policy".to_string(),
                        digest: sha256_hex(&bytes),
                        path: Some(display_path(&policy_path)),
                    },
                );
            }
            Err(err) => diagnostics.push(Diagnostic::error(
                "policy_file_missing",
                format!("policies.{policy_name}.file"),
                format!(
                    "could not read policy file '{}': {err}",
                    policy_path.display()
                ),
            )),
        }
    }

    let mut resource_digests = BTreeMap::new();
    let mut resource_list = Vec::new();
    for (address, resource) in resources {
        resource_digests.insert(address, resource.digest.clone());
        resource_list.push(resource);
    }
    let dependencies: Vec<_> = dependencies.into_iter().collect();
    let config_digest = desired_config_digest(&text, &resource_digests);

    LoadOutcome {
        desired: Some(DesiredCluster {
            config_dir: config_dir.clone(),
            config_digest,
            resource_digests,
            resources: resource_list,
            dependencies,
        }),
        diagnostics,
        config_dir,
        config_file,
    }
}

fn validate_query_source(
    graph_id: &str,
    query_name: &str,
    source: &str,
    catalog: Option<&omnigraph_compiler::catalog::Catalog>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let path = format!("graphs.{graph_id}.queries.{query_name}");
    match parse_query(source) {
        Ok(query_file) => {
            let Some(query_decl) = query_file.queries.iter().find(|q| q.name == query_name) else {
                diagnostics.push(Diagnostic::error(
                    "query_key_mismatch",
                    path,
                    format!("no `query {query_name}` declaration found in the referenced .gq file"),
                ));
                return;
            };
            if let Some(catalog) = catalog {
                if let Err(err) = typecheck_query_decl(catalog, query_decl) {
                    diagnostics.push(Diagnostic::error(
                        "query_typecheck_error",
                        format!("graphs.{graph_id}.queries.{query_name}"),
                        err.to_string(),
                    ));
                }
            } else {
                diagnostics.push(Diagnostic::warning(
                    "query_typecheck_skipped",
                    format!("graphs.{graph_id}.queries.{query_name}"),
                    "query parsed, but type-check was skipped because the graph schema is invalid",
                ));
            }
        }
        Err(err) => diagnostics.push(Diagnostic::error(
            "query_parse_error",
            path,
            err.to_string(),
        )),
    }
}

fn diff_resources(
    prior: &BTreeMap<String, String>,
    desired: &BTreeMap<String, String>,
) -> Vec<PlanChange> {
    let mut changes = Vec::new();
    for (address, after) in desired {
        match prior.get(address) {
            None => changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Create,
                before_digest: None,
                after_digest: Some(after.clone()),
            }),
            Some(before) if before != after => changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Update,
                before_digest: Some(before.clone()),
                after_digest: Some(after.clone()),
            }),
            Some(_) => {}
        }
    }
    for (address, before) in prior {
        if !desired.contains_key(address) {
            changes.push(PlanChange {
                resource: address.clone(),
                operation: PlanOperation::Delete,
                before_digest: Some(before.clone()),
                after_digest: None,
            });
        }
    }
    changes.sort_by(|a, b| a.resource.cmp(&b.resource));
    changes
}

fn compute_blast_radius(changes: &[PlanChange], dependencies: &[Dependency]) -> Vec<BlastRadius> {
    changes
        .iter()
        .filter_map(|change| {
            let affected: Vec<_> = dependencies
                .iter()
                .filter_map(|dep| (dep.to == change.resource).then_some(dep.from.clone()))
                .collect();
            (!affected.is_empty()).then(|| BlastRadius {
                resource: change.resource.clone(),
                affected,
            })
        })
        .collect()
}

fn compute_approvals(changes: &[PlanChange]) -> Vec<ApprovalRequirement> {
    changes
        .iter()
        .filter_map(|change| {
            if change.operation == PlanOperation::Delete
                && (change.resource.starts_with("graph.") || change.resource.starts_with("schema."))
            {
                Some(ApprovalRequirement {
                    resource: change.resource.clone(),
                    reason: "delete may remove deployed graph or schema definition".to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn duplicate_key_diagnostics(text: &str) -> Vec<Diagnostic> {
    #[derive(Debug)]
    struct Frame {
        indent: isize,
        path: String,
        keys: BTreeSet<String>,
    }

    let mut diagnostics = Vec::new();
    let mut stack = vec![Frame {
        indent: -1,
        path: String::new(),
        keys: BTreeSet::new(),
    }];

    for (line_idx, line) in text.lines().enumerate() {
        let line_without_comment = strip_comment(line);
        if line_without_comment.trim().is_empty() {
            continue;
        }
        let indent = line_without_comment
            .chars()
            .take_while(|ch| *ch == ' ')
            .count() as isize;
        let trimmed = line_without_comment.trim_start();
        if trimmed.starts_with('-') {
            continue;
        }
        let Some((raw_key, raw_value)) = trimmed.split_once(':') else {
            continue;
        };
        let key = raw_key.trim();
        if key.is_empty() || key.starts_with('{') || key.starts_with('[') {
            continue;
        }

        while stack.last().is_some_and(|frame| indent <= frame.indent) {
            stack.pop();
        }
        let parent = stack.last_mut().expect("root frame is always present");
        let full_path = if parent.path.is_empty() {
            key.to_string()
        } else {
            format!("{}.{}", parent.path, key)
        };
        if !parent.keys.insert(key.to_string()) {
            diagnostics.push(Diagnostic::error(
                "duplicate_yaml_key",
                full_path.clone(),
                format!("duplicate YAML key `{key}` on line {}", line_idx + 1),
            ));
        }
        if raw_value.trim().is_empty() {
            stack.push(Frame {
                indent,
                path: full_path,
                keys: BTreeSet::new(),
            });
        }
    }

    diagnostics
}

fn future_field_diagnostics(text: &str) -> Vec<Diagnostic> {
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(text) else {
        return Vec::new();
    };
    let Some(mapping) = value.as_mapping() else {
        return Vec::new();
    };
    let future_fields = [
        "apply",
        "env_file",
        "providers",
        "pipelines",
        "embeddings",
        "ui",
        "aliases",
        "bindings",
    ];
    mapping
        .keys()
        .filter_map(|key| key.as_str())
        .filter(|key| future_fields.contains(key))
        .map(|key| {
            Diagnostic::error(
                "future_phase_field",
                key,
                format!("`{key}` is reserved for a later cluster-control phase"),
            )
        })
        .collect()
}

fn strip_comment(line: &str) -> String {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escaped = false;

    for (idx, ch) in line.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_double_quote => escaped = true,
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            '#' if !in_single_quote && !in_double_quote => return line[..idx].to_string(),
            _ => {}
        }
    }

    line.to_string()
}

fn validate_id(kind: &str, path: &str, value: &str, diagnostics: &mut Vec<Diagnostic>) {
    let mut chars = value.chars();
    let valid = chars
        .next()
        .is_some_and(|ch| ch.is_ascii_alphabetic() || ch == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    if !valid {
        diagnostics.push(Diagnostic::error(
            "invalid_resource_id",
            path,
            format!("{kind} `{value}` must start with a letter or `_` and contain only ASCII letters, digits, `_`, or `-`"),
        ));
    }
}

enum PolicyTarget {
    Cluster,
    Graph(String),
    WrongKind(String),
}

fn normalize_policy_target(value: &str) -> PolicyTarget {
    if value == "cluster" {
        PolicyTarget::Cluster
    } else if let Some(graph_id) = value.strip_prefix("graph.") {
        PolicyTarget::Graph(graph_id.to_string())
    } else if value.contains('.') {
        PolicyTarget::WrongKind(value.to_string())
    } else {
        PolicyTarget::Graph(value.to_string())
    }
}

fn graph_address(graph_id: &str) -> String {
    format!("graph.{graph_id}")
}

fn schema_address(graph_id: &str) -> String {
    format!("schema.{graph_id}")
}

fn query_address(graph_id: &str, query_name: &str) -> String {
    format!("query.{graph_id}.{query_name}")
}

fn policy_address(policy_name: &str) -> String {
    format!("policy.{policy_name}")
}

fn resolve_config_path(config_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    }
}

fn graph_digest(
    graph_id: &str,
    schema_digest: Option<&String>,
    query_digests: Option<&BTreeMap<String, String>>,
) -> String {
    let mut input = format!(
        "graph\0{graph_id}\0schema\0{}\0",
        schema_digest.map_or("", String::as_str)
    );
    if let Some(query_digests) = query_digests {
        for (name, digest) in query_digests {
            input.push_str("query\0");
            input.push_str(name);
            input.push('\0');
            input.push_str(digest);
            input.push('\0');
        }
    }
    sha256_hex(input.as_bytes())
}

fn desired_config_digest(
    config_source: &str,
    resource_digests: &BTreeMap<String, String>,
) -> String {
    let mut input = String::from("cluster-config\0");
    input.push_str(config_source);
    input.push('\0');
    for (address, digest) in resource_digests {
        input.push_str(address);
        input.push('\0');
        input.push_str(digest);
        input.push('\0');
    }
    sha256_hex(input.as_bytes())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn has_errors(diagnostics: &[Diagnostic]) -> bool {
    diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    const SCHEMA: &str = r#"
node Person {
  name: String @key
  age: I32?
}
"#;

    const QUERY: &str = r#"
query find_person($name: String) {
  match { $p: Person { name: $name } }
  return { $p.name, $p.age }
}
"#;

    fn fixture() -> tempfile::TempDir {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("people.pg"), SCHEMA).unwrap();
        fs::write(dir.path().join("people.gq"), QUERY).unwrap();
        fs::write(dir.path().join("base.policy.yaml"), "rules: []\n").unwrap();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
metadata:
  name: test
state:
  backend: cluster
  lock: true
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      find_person:
        file: ./people.gq
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [knowledge]
"#,
        )
        .unwrap();
        dir
    }

    #[test]
    fn valid_minimal_config() {
        let dir = fixture();
        let out = validate_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(out.resource_digests.contains_key("graph.knowledge"));
        assert!(out.resource_digests.contains_key("schema.knowledge"));
        assert!(
            out.dependencies
                .iter()
                .any(|dep| dep.from == "policy.base" && dep.to == "graph.knowledge")
        );
    }

    #[test]
    fn unknown_field_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\nwat: true\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(out.diagnostics[0].message.contains("unknown field"));
    }

    #[test]
    fn future_phase_field_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\npipelines: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "future_phase_field");
    }

    #[test]
    fn duplicate_yaml_key_rejection() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\ngraphs: {}\ngraphs: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "duplicate_yaml_key");
    }

    #[test]
    fn duplicate_yaml_key_rejection_keeps_quoted_hashes() {
        let diagnostics =
            duplicate_key_diagnostics("\"name#display\": one\n\"name#display\": two\n");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, "duplicate_yaml_key");
    }

    #[test]
    fn missing_schema_query_and_policy_files() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./missing.pg
    queries:
      find_person: { file: ./missing.gq }
policies:
  base:
    file: ./missing.policy.yaml
    applies_to: [knowledge]
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains("schema_file_missing"));
        assert!(codes.contains("query_file_missing"));
        assert!(codes.contains("policy_file_missing"));
    }

    #[test]
    fn wrong_kind_and_dangling_refs_fail() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
policies:
  base:
    file: ./base.policy.yaml
    applies_to: [query.knowledge.find_person, missing]
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        let codes: BTreeSet<_> = out.diagnostics.iter().map(|d| d.code.as_str()).collect();
        assert!(codes.contains("wrong_kind_reference"));
        assert!(codes.contains("dangling_graph_reference"));
    }

    #[test]
    fn query_key_mismatch_fails() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    queries:
      different: { file: ./people.gq }
"#,
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "query_key_mismatch");
    }

    #[test]
    fn query_typecheck_failure_fails() {
        let dir = fixture();
        fs::write(
            dir.path().join("people.gq"),
            "query find_person() { match { $d: DoesNotExist } return { $d.name } }\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert!(
            out.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "query_typecheck_error")
        );
    }

    #[test]
    fn missing_state_plans_creates() {
        let dir = fixture();
        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        assert!(!out.state_observations.state_found);
        assert!(
            out.changes
                .iter()
                .all(|c| c.operation == PlanOperation::Create)
        );
        assert!(out.changes.iter().any(|c| c.resource == "graph.knowledge"));
    }

    #[test]
    fn existing_state_plans_update_and_delete_deterministically() {
        let dir = fixture();
        let first = plan_config_dir(dir.path());
        let state_dir = dir.path().join("__cluster");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("state.json"),
            serde_json::to_string_pretty(&json!({
                "version": 1,
                "applied_revision": {
                    "config_digest": "old",
                    "resources": {
                        "graph.knowledge": { "digest": first.resource_digests["graph.knowledge"] },
                        "policy.old": { "digest": "abc" },
                        "schema.knowledge": { "digest": "old-schema" }
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let out = plan_config_dir(dir.path());
        assert!(out.ok, "{:?}", out.diagnostics);
        let rendered: Vec<_> = out
            .changes
            .iter()
            .map(|change| (change.resource.as_str(), &change.operation))
            .collect();
        assert_eq!(
            rendered,
            vec![
                ("policy.base", &PlanOperation::Create),
                ("policy.old", &PlanOperation::Delete),
                ("query.knowledge.find_person", &PlanOperation::Create),
                ("schema.knowledge", &PlanOperation::Update),
            ]
        );
    }

    #[test]
    fn external_state_backend_rejected() {
        let dir = fixture();
        fs::write(
            dir.path().join(CLUSTER_CONFIG_FILE),
            "version: 1\nstate:\n  backend: s3://bucket/state\ngraphs: {}\n",
        )
        .unwrap();
        let out = validate_config_dir(dir.path());
        assert!(!out.ok);
        assert_eq!(out.diagnostics[0].code, "unsupported_state_backend");
    }
}
