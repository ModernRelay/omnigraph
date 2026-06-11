//! Declared-configuration loading: cluster.yaml parsing, query
//! discovery, source digesting, validation (moved verbatim from lib.rs
//! in the modularization). Reads the operator's WORKING TREE — stored
//! state never lives here (see store.rs).

use super::*;

/// How a graph declares its stored queries. Terraform-style: the `.gq`
/// files ARE the declaration — point at them (or a directory) and every
/// `query <name>` they contain is discovered. The explicit name->file map
/// remains for fine-grained control.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum QueriesDecl {
    /// `queries: ./queries/` — a directory (top-level `*.gq`, sorted) or a
    /// single `.gq` file; every declaration inside is registered.
    Discover(PathBuf),
    /// `queries: [./queries/, ./extra.gq]` — several directories/files.
    DiscoverMany(Vec<PathBuf>),
    /// `queries: { name: { file: ... } }` — explicit registry.
    Explicit(BTreeMap<String, QueryConfig>),
}

impl Default for QueriesDecl {
    fn default() -> Self {
        QueriesDecl::Explicit(BTreeMap::new())
    }
}

/// Expand a graph's query declaration into the canonical name->file map.
/// Discovery reads and parses each `.gq`; unreadable or unparseable files
/// and duplicate query names are loud validation errors — a declaration the
/// tool cannot enumerate is broken, not partially usable.
pub(crate) fn resolve_query_decls(
    config_dir: &Path,
    graph_id: &str,
    decl: &QueriesDecl,
    diagnostics: &mut Vec<Diagnostic>,
) -> (BTreeMap<String, QueryConfig>, BTreeMap<PathBuf, String>) {
    let paths: Vec<PathBuf> = match decl {
        QueriesDecl::Explicit(map) => {
            return (
                map.iter()
                    .map(|(name, config)| {
                        (name.clone(), QueryConfig { file: config.file.clone() })
                    })
                    .collect(),
                BTreeMap::new(),
            );
        }
        QueriesDecl::Discover(path) => vec![path.clone()],
        QueriesDecl::DiscoverMany(paths) => paths.clone(),
    };

    let mut files: Vec<(PathBuf, PathBuf)> = Vec::new(); // (declared-relative, resolved)
    for declared in &paths {
        let resolved = resolve_config_path(config_dir, declared);
        if resolved.is_dir() {
            let mut entries: Vec<PathBuf> = match fs::read_dir(&resolved) {
                Ok(read) => read
                    .flatten()
                    .map(|entry| entry.path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "gq"))
                    .collect(),
                Err(err) => {
                    diagnostics.push(Diagnostic::error(
                        "query_dir_unreadable",
                        format!("graphs.{graph_id}.queries"),
                        format!("could not list query directory '{}': {err}", resolved.display()),
                    ));
                    continue;
                }
            };
            entries.sort();
            if entries.is_empty() {
                diagnostics.push(Diagnostic::warning(
                    "query_dir_empty",
                    format!("graphs.{graph_id}.queries"),
                    format!("query directory '{}' contains no .gq files", resolved.display()),
                ));
            }
            for path in entries {
                let relative = declared.join(path.file_name().expect("dir entries have names"));
                files.push((relative, path));
            }
        } else {
            files.push((declared.clone(), resolved));
        }
    }

    let mut registry: BTreeMap<String, QueryConfig> = BTreeMap::new();
    let mut origin: BTreeMap<String, PathBuf> = BTreeMap::new();
    // Content read once at discovery and handed to the caller — the per-query
    // digest/typecheck pass reuses it instead of re-reading (no N+1 reads, no
    // window for the file to change between enumeration and validation).
    let mut contents: BTreeMap<PathBuf, String> = BTreeMap::new();
    for (declared, resolved) in files {
        let source = match fs::read_to_string(&resolved) {
            Ok(source) => source,
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "query_file_missing",
                    format!("graphs.{graph_id}.queries"),
                    format!("could not read query file '{}': {err}", resolved.display()),
                ));
                continue;
            }
        };
        let parsed = match parse_query(&source) {
            Ok(parsed) => parsed,
            Err(err) => {
                diagnostics.push(Diagnostic::error(
                    "query_parse_error",
                    format!("graphs.{graph_id}.queries"),
                    format!("'{}' does not parse: {err}", resolved.display()),
                ));
                continue;
            }
        };
        for query_decl in &parsed.queries {
            let name = query_decl.name.clone();
            if let Some(previous) = origin.get(&name) {
                diagnostics.push(Diagnostic::error(
                    "duplicate_query_name",
                    format!("graphs.{graph_id}.queries.{name}"),
                    format!(
                        "query '{name}' is declared in both '{}' and '{}'",
                        previous.display(),
                        declared.display()
                    ),
                ));
                continue;
            }
            origin.insert(name.clone(), declared.clone());
            registry.insert(name, QueryConfig { file: declared.clone() });
        }
        contents.insert(declared, source);
    }
    (registry, contents)
}

pub(crate) fn parse_cluster_config(config_dir: &Path) -> ParsedConfig {
    let config_dir = config_dir.to_path_buf();
    let config_file = config_dir.join(CLUSTER_CONFIG_FILE);
    let mut diagnostics = Vec::new();

    if !config_dir.is_dir() {
        diagnostics.push(Diagnostic::error(
            "config_dir_not_found",
            display_path(&config_dir),
            "`--config` must point at a directory containing cluster.yaml",
        ));
        return ParsedConfig {
            raw: None,
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
            return ParsedConfig {
                raw: None,
                diagnostics,
                config_dir,
                config_file,
            };
        }
    };

    diagnostics.extend(duplicate_key_diagnostics(&text));
    diagnostics.extend(future_field_diagnostics(&text));
    if has_errors(&diagnostics) {
        return ParsedConfig {
            raw: None,
            diagnostics,
            config_dir,
            config_file,
        };
    }

    let raw = match serde_yaml::from_str::<RawClusterConfig>(&text) {
        Ok(raw) => Some(raw),
        Err(err) => {
            diagnostics.push(Diagnostic::error(
                "invalid_cluster_yaml",
                CLUSTER_CONFIG_FILE,
                format!("could not parse cluster.yaml: {err}"),
            ));
            None
        }
    };

    ParsedConfig {
        raw,
        diagnostics,
        config_dir,
        config_file,
    }
}

pub(crate) fn validate_cluster_header(
    raw: &RawClusterConfig,
    diagnostics: &mut Vec<Diagnostic>,
) -> ClusterSettings {
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
                "Stage 2C supports only omitted state.backend or `cluster`",
            ));
        }
    }

    ClusterSettings {
        state_lock: raw.state.lock.unwrap_or(true),
    }
}



pub(crate) fn state_resource_digests(state: &ClusterState) -> BTreeMap<String, String> {
    state
        .applied_revision
        .resources
        .iter()
        .map(|(address, resource)| (address.clone(), resource.digest.clone()))
        .collect()
}

pub(crate) fn initial_import_state(desired: &DesiredCluster) -> ClusterState {
    ClusterState {
        version: 1,
        state_revision: 0,
        applied_revision: AppliedRevisionState {
            config_digest: Some(desired.config_digest.clone()),
            resources: BTreeMap::new(),
        },
        resource_statuses: BTreeMap::new(),
        approval_records: BTreeMap::new(),
        recovery_records: BTreeMap::new(),
        observations: BTreeMap::new(),
    }
}


pub(crate) async fn observe_declared_graphs(desired: &DesiredCluster, state: &mut ClusterState) -> usize {
    let mut graph_error_count = 0;
    for graph in &desired.graphs {
        let graph_address = graph_address(&graph.id);
        let schema_address = schema_address(&graph.id);
        let graph_path = desired
            .config_dir
            .join(CLUSTER_GRAPHS_DIR)
            .join(format!("{}.omni", graph.id));
        let graph_uri = display_path(&graph_path);
        let observed_at = now_rfc3339();

        if !graph_path.exists() {
            state.applied_revision.resources.remove(&graph_address);
            state.applied_revision.resources.remove(&schema_address);
            state.observations.insert(
                graph_address.clone(),
                graph_observation_json(GraphObservationJson {
                    address: &graph_address,
                    graph_uri: &graph_uri,
                    observed_at: &observed_at,
                    exists: false,
                    manifest_version: None,
                    schema_digest: None,
                    desired_schema_digest: &graph.schema_digest,
                    schema_matches_desired: Some(false),
                    error: Some("derived graph root is missing"),
                }),
            );
            set_resource_status(
                state,
                &graph_address,
                ResourceLifecycleStatus::Drifted,
                "graph_missing",
                "derived graph root is missing",
            );
            set_resource_status(
                state,
                &schema_address,
                ResourceLifecycleStatus::Drifted,
                "graph_missing",
                "derived graph root is missing",
            );
            continue;
        }

        match observe_live_graph(&graph_uri).await {
            Ok(observation) => {
                let schema_matches = observation.schema_digest == graph.schema_digest;
                state.applied_revision.resources.insert(
                    schema_address.clone(),
                    StateResource {
                        digest: observation.schema_digest.clone(),
                        applies_to: None,
                    },
                );
                let query_digests = state_query_digests_for_graph(state, &graph.id);
                let graph_digest_value = graph_digest(
                    &graph.id,
                    Some(&observation.schema_digest),
                    Some(&query_digests),
                );
                state.applied_revision.resources.insert(
                    graph_address.clone(),
                    StateResource {
                        digest: graph_digest_value,
                        applies_to: None,
                    },
                );
                state.observations.insert(
                    graph_address.clone(),
                    graph_observation_json(GraphObservationJson {
                        address: &graph_address,
                        graph_uri: &graph_uri,
                        observed_at: &observed_at,
                        exists: true,
                        manifest_version: Some(observation.manifest_version),
                        schema_digest: Some(observation.schema_digest.as_str()),
                        desired_schema_digest: &graph.schema_digest,
                        schema_matches_desired: Some(schema_matches),
                        error: None,
                    }),
                );
                if schema_matches {
                    set_resource_status_applied(state, &graph_address);
                    set_resource_status_applied(state, &schema_address);
                } else {
                    set_resource_status(
                        state,
                        &graph_address,
                        ResourceLifecycleStatus::Drifted,
                        "schema_mismatch",
                        "live schema digest differs from desired schema digest",
                    );
                    set_resource_status(
                        state,
                        &schema_address,
                        ResourceLifecycleStatus::Drifted,
                        "schema_mismatch",
                        "live schema digest differs from desired schema digest",
                    );
                }
            }
            Err(error) => {
                graph_error_count += 1;
                state.observations.insert(
                    graph_address.clone(),
                    graph_observation_json(GraphObservationJson {
                        address: &graph_address,
                        graph_uri: &graph_uri,
                        observed_at: &observed_at,
                        exists: true,
                        manifest_version: None,
                        schema_digest: None,
                        desired_schema_digest: &graph.schema_digest,
                        schema_matches_desired: None,
                        error: Some(error.as_str()),
                    }),
                );
                set_resource_status(
                    state,
                    &graph_address,
                    ResourceLifecycleStatus::Error,
                    "graph_observation_error",
                    error.as_str(),
                );
                set_resource_status(
                    state,
                    &schema_address,
                    ResourceLifecycleStatus::Error,
                    "graph_observation_error",
                    error.as_str(),
                );
            }
        }
    }
    graph_error_count
}

/// RFC-004 §D7: the data-aware preview — the engine's migration plan for a
/// desired schema against the live graph, computed read-only (no lock).
pub(crate) async fn preview_schema_migration(
    graph_uri: &str,
    schema_path: &str,
) -> Result<SchemaMigrationPlan, String> {
    let source = fs::read_to_string(schema_path).map_err(|err| err.to_string())?;
    let db = Omnigraph::open_read_only(graph_uri)
        .await
        .map_err(|err| err.to_string())?;
    let preview = db
        .preview_schema_apply_with_options(&source, SchemaApplyOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    Ok(preview.plan)
}

struct LiveGraphObservation {
    manifest_version: u64,
    schema_digest: String,
}

pub(crate) async fn observe_live_graph(graph_uri: &str) -> Result<LiveGraphObservation, String> {
    let db = Omnigraph::open_read_only(graph_uri)
        .await
        .map_err(|err| err.to_string())?;
    let snapshot = db
        .snapshot_of(ReadTarget::branch("main"))
        .await
        .map_err(|err| err.to_string())?;
    let schema_source = db.schema_source();
    Ok(LiveGraphObservation {
        manifest_version: snapshot.version(),
        schema_digest: sha256_hex(schema_source.as_bytes()),
    })
}

struct GraphObservationJson<'a> {
    address: &'a str,
    graph_uri: &'a str,
    observed_at: &'a str,
    exists: bool,
    manifest_version: Option<u64>,
    schema_digest: Option<&'a str>,
    desired_schema_digest: &'a str,
    schema_matches_desired: Option<bool>,
    error: Option<&'a str>,
}

pub(crate) fn graph_observation_json(observation: GraphObservationJson<'_>) -> serde_json::Value {
    json!({
        "kind": "graph",
        "address": observation.address,
        "graph_uri": observation.graph_uri,
        "observed_at": observation.observed_at,
        "exists": observation.exists,
        "manifest_version": observation.manifest_version,
        "schema_digest": observation.schema_digest,
        "desired_schema_digest": observation.desired_schema_digest,
        "schema_matches_desired": observation.schema_matches_desired,
        "error": observation.error,
    })
}


pub(crate) fn load_desired(config_dir: &Path) -> LoadOutcome {
    let parsed = parse_cluster_config(config_dir);
    let config_dir = parsed.config_dir;
    let config_file = parsed.config_file;
    let mut diagnostics = parsed.diagnostics;
    let Some(raw) = parsed.raw else {
        return LoadOutcome {
            desired: None,
            diagnostics,
            config_dir,
            config_file,
        };
    };
    let settings = validate_cluster_header(&raw, &mut diagnostics);

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

        let (graph_queries, query_contents) =
            resolve_query_decls(&config_dir, graph_id, &graph.queries, &mut diagnostics);
        for (query_name, query) in &graph_queries {
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
            let source = match query_contents.get(&query.file) {
                Some(cached) => Ok(cached.clone()),
                None => fs::read_to_string(&query_path),
            };
            match source {
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

    let mut policy_bindings: BTreeMap<String, Vec<String>> = BTreeMap::new();
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
        let mut normalized_bindings: Vec<String> = Vec::new();
        for (idx, target) in policy.applies_to.iter().enumerate() {
            match normalize_policy_target(target) {
                PolicyTarget::Cluster => {
                    normalized_bindings.push("cluster".to_string());
                }
                PolicyTarget::Graph(graph_id) => {
                    normalized_bindings.push(graph_address(&graph_id));
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

        normalized_bindings.sort();
        normalized_bindings.dedup();
        policy_bindings.insert(policy_address.clone(), normalized_bindings);

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
    let graphs = raw
        .graphs
        .keys()
        .map(|graph_id| DesiredGraph {
            id: graph_id.clone(),
            schema_digest: graph_schema_digests
                .get(graph_id)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    let config_digest = desired_config_digest(&raw, &resource_digests);

    LoadOutcome {
        desired: Some(DesiredCluster {
            config_dir: config_dir.clone(),
            config_digest,
            state_lock: settings.state_lock,
            graphs,
            resource_digests,
            resources: resource_list,
            dependencies,
            policy_bindings,
        }),
        diagnostics,
        config_dir,
        config_file,
    }
}

pub(crate) fn validate_query_source(
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

pub(crate) fn future_field_diagnostics(text: &str) -> Vec<Diagnostic> {
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

pub(crate) fn validate_id(kind: &str, path: &str, value: &str, diagnostics: &mut Vec<Diagnostic>) {
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

pub(crate) fn normalize_policy_target(value: &str) -> PolicyTarget {
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

pub(crate) fn graph_address(graph_id: &str) -> String {
    format!("graph.{graph_id}")
}

pub(crate) fn schema_address(graph_id: &str) -> String {
    format!("schema.{graph_id}")
}

pub(crate) fn query_address(graph_id: &str, query_name: &str) -> String {
    format!("query.{graph_id}.{query_name}")
}

pub(crate) fn policy_address(policy_name: &str) -> String {
    format!("policy.{policy_name}")
}

pub(crate) fn resolve_config_path(config_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    }
}
