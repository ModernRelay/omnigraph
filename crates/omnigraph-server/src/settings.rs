//! Server settings: omnigraph.yaml/CLI/env resolution, mode inference
//! (single vs multi vs cluster), bearer-token sources, and runtime-state
//! classification (moved verbatim from lib.rs in the modularization).

use super::*;

/// Build serving settings from a cluster directory's applied revision
/// (RFC-005 §D2): graphs at derived roots, stored queries from verified
/// catalog blob content, policy bundles from blob paths with their applied
/// bindings. Always multi-graph routing. The unauthenticated/env handling
/// matches the omnigraph.yaml path.
pub(crate) async fn load_cluster_settings(
    cluster_dir: &PathBuf,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
) -> Result<ServerConfig> {
    let snapshot = omnigraph_cluster::read_serving_snapshot(cluster_dir).await.map_err(|diagnostics| {
        let details = diagnostics
            .iter()
            .map(|diagnostic| format!("[{}] {}: {}", diagnostic.code, diagnostic.path, diagnostic.message))
            .collect::<Vec<_>>()
            .join("\n  ");
        eyre!("the cluster at '{}' is not ready to serve:\n  {details}", cluster_dir.display())
    })?;

    // Bindings -> Cedar slots. The serving pipeline loads one bundle per
    // graph plus one server-level bundle; stacked bundles per scope are a
    // later slice — refuse loudly rather than silently merging policy.
    let mut server_policy_file: Option<PathBuf> = None;
    let mut graph_policy_files: BTreeMap<String, PathBuf> = BTreeMap::new();
    for policy in &snapshot.policies {
        for binding in &policy.applies_to {
            if binding == "cluster" {
                if server_policy_file.replace(policy.blob_path.clone()).is_some() {
                    bail!(
                        "multiple policy bundles bind the cluster scope; cluster-mode serving supports one bundle per scope — split or merge bundles (multi-bundle scopes are a later slice)"
                    );
                }
            } else if let Some(graph_id) = binding.strip_prefix("graph.") {
                if graph_policy_files
                    .insert(graph_id.to_string(), policy.blob_path.clone())
                    .is_some()
                {
                    bail!(
                        "multiple policy bundles bind graph '{graph_id}'; cluster-mode serving supports one bundle per scope — split or merge bundles (multi-bundle scopes are a later slice)"
                    );
                }
            } else {
                bail!("unrecognized policy binding '{binding}' in the applied revision");
            }
        }
    }

    let mut graphs = Vec::new();
    for graph in &snapshot.graphs {
        let specs: Vec<queries::RegistrySpec> = snapshot
            .queries
            .iter()
            .filter(|query| query.graph_id == graph.graph_id)
            .map(|query| queries::RegistrySpec {
                name: query.name.clone(),
                source: query.source.clone(),
                // The §D5 bridge: the cluster registry has no expose flag
                // (exposure becomes a policy decision in Phase 6) — cluster
                // mode lists every stored query.
                expose: true,
                tool_name: None,
            })
            .collect();
        let registry = QueryRegistry::from_specs(specs).map_err(|errors| {
            let details = errors
                .iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("\n  ");
            eyre!(
                "stored queries in the applied revision failed to parse:\n  {details}\nrun `cluster refresh` then `cluster apply`, and restart"
            )
        })?;
        graphs.push(GraphStartupConfig {
            graph_id: graph.graph_id.clone(),
            uri: graph.root.to_string_lossy().to_string(),
            policy_file: graph_policy_files.get(&graph.graph_id).cloned(),
            queries: registry,
        });
    }

    let env_unauth = std::env::var("OMNIGRAPH_UNAUTHENTICATED")
        .ok()
        .map(|v| {
            let trimmed = v.trim();
            !trimmed.is_empty() && trimmed != "0" && !trimmed.eq_ignore_ascii_case("false")
        })
        .unwrap_or(false);

    Ok(ServerConfig {
        mode: ServerConfigMode::Multi {
            graphs,
            config_path: cluster_dir.clone(),
            server_policy_file,
        },
        bind: cli_bind.unwrap_or_else(|| "127.0.0.1:8080".to_string()),
        allow_unauthenticated: cli_allow_unauthenticated || env_unauth,
    })
}

pub async fn load_server_settings(
    config_path: Option<&PathBuf>,
    cli_cluster: Option<&PathBuf>,
    cli_uri: Option<String>,
    cli_target: Option<String>,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
) -> Result<ServerConfig> {
    // Rule 0 (RFC-005): --cluster is an exclusive boot source. It is checked
    // before anything reads omnigraph.yaml — in cluster mode that file is
    // never opened, not even the implicit current-directory search.
    if let Some(cluster_dir) = cli_cluster {
        if cli_uri.is_some() || cli_target.is_some() || config_path.is_some() {
            bail!(
                "--cluster is an exclusive boot source; it cannot combine with a graph URI, --target, or --config (axiom 15: a deployment serves from one source)"
            );
        }
        return load_cluster_settings(cluster_dir, cli_bind, cli_allow_unauthenticated).await;
    }
    let config = load_config(config_path)?;
    let bind = cli_bind.unwrap_or_else(|| config.server_bind().to_string());
    // Either `--unauthenticated` or `OMNIGRAPH_UNAUTHENTICATED=1` flips
    // this. Treat any non-empty, non-"0"/"false" string as truthy —
    // standard 12-factor "any value is true" reading of the env var.
    let env_unauth = std::env::var("OMNIGRAPH_UNAUTHENTICATED")
        .ok()
        .map(|v| {
            let trimmed = v.trim();
            !trimmed.is_empty() && trimmed != "0" && !trimmed.eq_ignore_ascii_case("false")
        })
        .unwrap_or(false);
    let allow_unauthenticated = cli_allow_unauthenticated || env_unauth;

    // MR-668 decision 2 — four-rule mode inference matrix.
    //
    //   1. CLI `<URI>` positional        → Single (URI = the value)
    //   2. CLI `--target <name>`         → Single (URI = graphs.<name>.uri)
    //   3. `server.graph` in config      → Single (URI = graphs.<server.graph>.uri)
    //   4. `--config` + non-empty `graphs:` + no single-mode selector
    //                                    → Multi (every entry in `graphs:`)
    //   5. otherwise                     → error with migration hint
    //
    // Rules 1-3 are mutually compatible (CLI URI wins over `--target`
    // wins over `server.graph`), reusing the existing
    // `resolve_target_uri` precedence.
    let has_cli_uri = cli_uri.is_some();
    let has_cli_target = cli_target.is_some();
    let has_server_graph = config.server_graph_name().is_some();
    let has_graphs_map = !config.graphs.is_empty();
    let has_explicit_config = config_path.is_some();

    let mode = if has_cli_uri || has_cli_target || has_server_graph {
        // Rules 1, 2, or 3 → Single mode.
        let raw_uri = config.resolve_target_uri(
            cli_uri,
            cli_target.as_deref(),
            config.server_graph_name(),
        )?;
        let uri = normalize_root_uri(&raw_uri).wrap_err_with(|| {
            format!("normalize single-graph URI '{raw_uri}' from server settings")
        })?;
        // Config follows graph IDENTITY, not mode: a bare URI is anonymous
        // (top-level config); a graph chosen by name uses its per-graph
        // `graphs.<name>.{policy,queries}`. `resolve_target_uri` already
        // errored on an unknown name, so a `Some(name)` here is a known graph.
        let selected: Option<&str> = if has_cli_uri {
            None
        } else {
            cli_target.as_deref().or_else(|| config.server_graph_name())
        };
        // A named selection must not leave a populated top-level block
        // silently unused — refuse boot and point at the per-graph block. The
        // same rule the CLI selection gate enforces, shared via one helper so
        // the boot check and `omnigraph queries validate`/`list` can't drift.
        config.ensure_top_level_blocks_honored(selected)?;
        // Load + identity-check now (no engine needed); the schema
        // type-check happens when the engine opens.
        let policy_file = config.resolve_policy_file_for(selected);
        let queries = QueryRegistry::load(&config, config.query_entries_for(selected))
            .map_err(|errs| color_eyre::eyre::eyre!(format_registry_load_errors(&uri, &errs)))?;
        let graph_id = graph_resource_id_for_selection(selected, &uri);
        ServerConfigMode::Single {
            uri,
            graph_id,
            policy_file,
            queries,
        }
    } else if has_explicit_config && has_graphs_map {
        // Multi mode: every graph uses its per-graph block; top-level
        // policy/queries are never honored, so a populated one is an error.
        let unhonored = config.populated_top_level_blocks();
        if !unhonored.is_empty() {
            bail!(
                "multi-graph mode: top-level {} {} not honored — each graph uses its own \
                 `graphs.<graph_id>.…` block. Move per-graph rules there (and any \
                 `graph_list` policy to `server.policy.file`).",
                unhonored.join(" and "),
                if unhonored.len() == 1 { "is" } else { "are" },
            );
        }
        // Rule 4 → Multi mode. Build a startup config per graph.
        let mut graphs = Vec::with_capacity(config.graphs.len());
        for (name, target) in &config.graphs {
            // Validate the graph id can construct a `GraphId` newtype.
            // Doing this here (not at registry insert) so a malformed
            // omnigraph.yaml fails at startup with a clear error.
            GraphId::try_from(name.clone()).map_err(|err| {
                color_eyre::eyre::eyre!("invalid graph id '{name}' in omnigraph.yaml: {err}")
            })?;
            let raw_uri = config.resolve_uri_value(&target.uri);
            let uri = normalize_root_uri(&raw_uri).wrap_err_with(|| {
                format!("normalize URI '{raw_uri}' for graph '{name}' in omnigraph.yaml")
            })?;
            // Per-graph `queries:`, selected through the shared
            // `query_entries_for` so server and CLI resolve identically.
            // Load + identity-check now; the schema type-check happens
            // when this graph's engine opens.
            let queries = QueryRegistry::load(&config, config.query_entries_for(Some(name.as_str())))
                .map_err(|errs| color_eyre::eyre::eyre!(format_registry_load_errors(name, &errs)))?;
            graphs.push(GraphStartupConfig {
                graph_id: name.clone(),
                uri,
                policy_file: config.resolve_target_policy_file(name),
                queries,
            });
        }
        let config_path = config_path
            .cloned()
            .expect("has_explicit_config implies config_path is Some");
        let server_policy_file = config.resolve_server_policy_file();
        ServerConfigMode::Multi {
            graphs,
            config_path,
            server_policy_file,
        }
    } else {
        // Rule 5 → error with migration hint.
        bail!(
            "no graph to serve: pass a URI (`omnigraph-server <URI>`), select a target \
             (`--target <name> --config omnigraph.yaml`), set `server.graph: <name>` in \
             omnigraph.yaml, or for multi-graph mode add a `graphs:` map to the config \
             file referenced by `--config`."
        );
    };

    Ok(ServerConfig {
        mode,
        bind,
        allow_unauthenticated,
    })
}

/// Whether the loaded config will run the server in multi-graph mode.
/// Useful for the test that constructs `ServerConfig` directly.
pub fn server_config_is_multi(config: &ServerConfig) -> bool {
    matches!(config.mode, ServerConfigMode::Multi { .. })
}

/// MR-723 server runtime state, classified from the three-state matrix
/// of (bearer tokens configured) × (policy file configured) at startup.
///
/// * **Open** — neither tokens nor policy; requires explicit
///   `allow_unauthenticated`. Effectively a "trust the network" dev
///   mode. `serve()` refuses to start in this shape without the flag,
///   so the only way to reach this state at runtime is via deliberate
///   operator opt-in.
/// * **DefaultDeny** — tokens configured but no policy file. The
///   server requires a valid bearer token; once authenticated, every
///   action except `Read` is denied with 403. Closes the "tokens but
///   forgot the policy file" trap.
/// * **PolicyEnabled** — policy file configured and at least one
///   bearer token configured. Cedar evaluates every authenticated
///   request. Policy without tokens is rejected at startup —
///   such a server would 401 every request, which is bug-shaped
///   rather than feature-shaped (operators wanting "deny all
///   unauthenticated traffic" should configure tokens plus a
///   deny-all policy to get meaningful 403s with policy-decision
///   logging instead).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ServerRuntimeState {
    Open,
    DefaultDeny,
    PolicyEnabled,
}

/// Compute the [`ServerRuntimeState`] from the configured inputs.
/// Pulled out as a pure function so the matrix is unit-testable
/// without standing up the full server.
///
/// The classifier is the **single source of truth** for "should we
/// start?" — both `serve()`'s single-mode and multi-mode branches
/// call this before constructing their `AppState`. Adding a startup
/// invariant here means both modes enforce it automatically; the
/// alternative (per-constructor `bail!`) drifts the moment a third
/// mode is added.
pub fn classify_server_runtime_state(
    has_tokens: bool,
    has_policy: bool,
    allow_unauthenticated: bool,
) -> Result<ServerRuntimeState> {
    match (has_tokens, has_policy, allow_unauthenticated) {
        (false, false, false) => bail!(
            "server has no bearer tokens and no policy file configured. This is a fully \
             open server — pass `--unauthenticated` (or set OMNIGRAPH_UNAUTHENTICATED=1) \
             if you actually want that, otherwise configure bearer tokens (see \
             docs/user/server.md) and/or `policy.file` in omnigraph.yaml."
        ),
        (false, false, true) => Ok(ServerRuntimeState::Open),
        (true, false, _) => Ok(ServerRuntimeState::DefaultDeny),
        (false, true, _) => bail!(
            "policy file is configured but no bearer tokens — every request would 401 \
             because no token can ever match. Configure at least one bearer token (see \
             docs/user/server.md), or remove the policy file. To deny all unauthenticated \
             traffic deliberately, configure tokens plus a deny-all Cedar rule — that \
             produces meaningful 403s with policy-decision logging instead of silent 401s."
        ),
        (true, true, _) => Ok(ServerRuntimeState::PolicyEnabled),
    }
}

pub(crate) fn normalize_bearer_token(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn normalize_bearer_actor(value: String) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        bail!("bearer token actor names must not be blank");
    }
    Ok(value)
}

pub(crate) fn parse_bearer_tokens_json(value: &str) -> Result<Vec<(String, String)>> {
    let entries: HashMap<String, String> = serde_json::from_str(value)
        .wrap_err("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON must be a JSON object of actor->token")?;
    Ok(entries.into_iter().collect())
}

pub(crate) fn read_bearer_tokens_file(path: &str) -> Result<Vec<(String, String)>> {
    let contents = fs::read_to_string(path)
        .wrap_err_with(|| format!("failed to read bearer tokens file at {path}"))?;
    parse_bearer_tokens_json(&contents)
        .wrap_err_with(|| format!("failed to parse bearer tokens file at {path}"))
}

pub(crate) fn validate_bearer_tokens(entries: Vec<(String, String)>) -> Result<Vec<(String, String)>> {
    let mut seen_actors = HashSet::new();
    let mut seen_tokens = HashSet::new();
    let mut normalized = Vec::with_capacity(entries.len());

    for (actor, token) in entries {
        let actor = normalize_bearer_actor(actor)?;
        let Some(token) = normalize_bearer_token(Some(token)) else {
            bail!("bearer token for actor '{actor}' must not be blank");
        };
        if !seen_actors.insert(actor.clone()) {
            bail!("duplicate bearer token actor '{actor}'");
        }
        if !seen_tokens.insert(token.clone()) {
            bail!("duplicate bearer token value configured");
        }
        normalized.push((actor, token));
    }

    normalized.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(normalized)
}

pub(crate) fn server_bearer_tokens_from_env() -> Result<Vec<(String, String)>> {
    let mut entries = Vec::new();

    if let Some(token) = normalize_bearer_token(std::env::var("OMNIGRAPH_SERVER_BEARER_TOKEN").ok())
    {
        entries.push(("default".to_string(), token));
    }

    if let Some(path) =
        normalize_bearer_token(std::env::var("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE").ok())
    {
        entries.extend(read_bearer_tokens_file(&path)?);
    } else if let Some(json) =
        normalize_bearer_token(std::env::var("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON").ok())
    {
        entries.extend(parse_bearer_tokens_json(&json)?);
    }

    validate_bearer_tokens(entries)
}

#[cfg(test)]
mod tests {
    use super::{
        GraphStartupConfig, ServerConfig, ServerConfigMode, ServerRuntimeState,
        classify_server_runtime_state, hash_bearer_token, load_server_settings,
        normalize_bearer_token, parse_bearer_tokens_json, serve, server_bearer_tokens_from_env,
    };
    use serial_test::serial;
    use std::env;
    use std::fs;
    use tempfile::tempdir;

    /// `authorize` returns the allow/deny **decision** (`Authz`) and reserves
    /// `Err` for operational failures, so the invoke handler can hide a denial
    /// as 404 without also masking a 401/500. Pins each outcome.
    #[test]
    fn authorize_splits_decision_from_operational_error() {
        use super::{Authz, PolicyAction, PolicyCompiler, PolicyConfig, PolicyRequest, ResolvedActor, authorize};
        use std::sync::Arc;

        fn req(action: PolicyAction) -> PolicyRequest {
            PolicyRequest { action, branch: None, target_branch: None }
        }
        let actor = ResolvedActor::cluster_static(Arc::from("act-alice"));

        // --- No policy engine installed (open / default-deny modes) ---
        // A server-scoped action is denied in every no-policy state.
        assert!(matches!(
            authorize(Some(&actor), None, req(PolicyAction::GraphList)).unwrap(),
            Authz::Denied(_)
        ));
        // Authenticated actor + a non-read per-graph action → default-deny.
        assert!(matches!(
            authorize(Some(&actor), None, req(PolicyAction::Change)).unwrap(),
            Authz::Denied(_)
        ));
        // `read` is the one per-graph action permitted without a policy.
        assert!(matches!(
            authorize(Some(&actor), None, req(PolicyAction::Read)).unwrap(),
            Authz::Allowed
        ));
        // Open mode (no actor, no policy) → allowed.
        assert!(matches!(
            authorize(None, None, req(PolicyAction::Read)).unwrap(),
            Authz::Allowed
        ));

        // --- Policy engine installed ---
        let policy: PolicyConfig = serde_yaml::from_str(
            "version: 1\n\
             groups:\n  team: [act-alice]\n\
             rules:\n  - id: team-read\n    allow:\n      actors: { group: team }\n      actions: [read]\n      branch_scope: any\n",
        )
        .unwrap();
        let engine = PolicyCompiler::compile(&policy, "graph").unwrap();

        // A matched allow rule → Allowed.
        assert!(matches!(
            authorize(
                Some(&actor),
                Some(&engine),
                PolicyRequest { action: PolicyAction::Read, branch: Some("main".to_string()), target_branch: None },
            )
            .unwrap(),
            Authz::Allowed
        ));
        // Known actor, no matching allow rule → Denied, carrying the decision message.
        match authorize(
            Some(&actor),
            Some(&engine),
            PolicyRequest { action: PolicyAction::Change, branch: Some("main".to_string()), target_branch: None },
        )
        .unwrap()
        {
            Authz::Denied(message) => assert!(!message.is_empty(), "a deny carries its decision message"),
            Authz::Allowed => panic!("change must be denied: only read is allowed"),
        }
        // Policy installed but no actor → operational failure (`Err`), NOT a
        // decision. This is the split that keeps a 401/500 from being masked
        // as the denial's response in the invoke handler.
        assert!(
            authorize(None, Some(&engine), req(PolicyAction::Read)).is_err(),
            "a missing actor with a policy installed is an operational error, not a deny"
        );
    }

    #[test]
    fn hash_bearer_token_produces_32_byte_output() {
        let hash = hash_bearer_token("any-token");
        assert_eq!(hash.len(), 32);
    }

    /// The single gate both open paths funnel through: it refuses a
    /// schema breakage (naming the graph label + query), attaches a clean
    /// registry, and collapses an empty one to `None`. Pure over its args
    /// (no engine), so it covers the multi-graph path's logic too — the
    /// only per-path difference is the `label`, asserted here.
    #[test]
    fn validate_and_attach_gates_on_schema_and_collapses_empty() {
        use crate::queries::{QueryRegistry, RegistrySpec};
        use omnigraph_compiler::catalog::build_catalog;
        use omnigraph_compiler::schema::parser::parse_schema;

        let schema = parse_schema("node User {\nname: String\n}\n").unwrap();
        let catalog = build_catalog(&schema).unwrap();
        let spec = |name: &str, source: &str| RegistrySpec {
            name: name.to_string(),
            source: source.to_string(),
            expose: false,
            tool_name: None,
        };

        // Empty registry → nothing attached, no error.
        let empty =
            super::validate_and_attach(QueryRegistry::default(), &catalog, "g").unwrap();
        assert!(empty.is_none());

        // A query that type-checks → attached.
        let ok = QueryRegistry::from_specs(vec![spec(
            "find_user",
            "query find_user() { match { $u: User } return { $u.name } }",
        )])
        .unwrap();
        assert!(super::validate_and_attach(ok, &catalog, "g").unwrap().is_some());

        // A query referencing a type the schema lacks → boot refusal that
        // names both the graph label and the offending query.
        let broken = QueryRegistry::from_specs(vec![spec(
            "ghost",
            "query ghost() { match { $w: Widget } return { $w.name } }",
        )])
        .unwrap();
        let err = super::validate_and_attach(broken, &catalog, "graph-x").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("graph-x"), "labels the graph: {msg}");
        assert!(msg.contains("ghost"), "names the query: {msg}");
        assert!(msg.contains("schema check"), "mentions the schema check: {msg}");
    }

    #[test]
    fn hash_bearer_token_is_deterministic() {
        assert_eq!(
            hash_bearer_token("stable-input"),
            hash_bearer_token("stable-input"),
        );
    }

    #[test]
    fn hash_bearer_token_differs_for_different_inputs() {
        assert_ne!(hash_bearer_token("token-a"), hash_bearer_token("token-b"));
    }

    #[test]
    fn hash_bearer_token_matches_known_sha256_vector() {
        // SHA-256("abc"). If this ever fails, the hash function was swapped.
        let hash = hash_bearer_token("abc");
        let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn server_settings_load_from_yaml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
graphs:
  local:
    uri: /tmp/demo.omni
server:
  graph: local
  bind: 0.0.0.0:9090
"#,
        )
        .unwrap();

        let settings = load_server_settings(Some(&config), None, None, None, None, false).await.unwrap();
        match &settings.mode {
            ServerConfigMode::Single { uri, graph_id, .. } => {
                assert_eq!(uri, "/tmp/demo.omni");
                assert_eq!(graph_id, "local");
            }
            ServerConfigMode::Multi { .. } => panic!("expected Single mode, got Multi"),
        }
        assert_eq!(settings.bind, "0.0.0.0:9090");
    }

    #[tokio::test]
    async fn server_settings_cli_flags_override_yaml_config() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
graphs:
  local:
    uri: /tmp/demo.omni
server:
  graph: local
  bind: 127.0.0.1:8080
"#,
        )
        .unwrap();

        let settings = load_server_settings(
            Some(&config),
            None,
            Some("/tmp/override.omni".to_string()),
            None,
            Some("0.0.0.0:9999".to_string()),
            false,
        )
        .await
        .unwrap();
        match &settings.mode {
            ServerConfigMode::Single { uri, graph_id, .. } => {
                assert_eq!(uri, "/tmp/override.omni");
                assert_eq!(graph_id, "/tmp/override.omni");
            }
            ServerConfigMode::Multi { .. } => panic!("expected Single mode, got Multi"),
        }
        assert_eq!(settings.bind, "0.0.0.0:9999");
    }

    #[tokio::test]
    async fn server_settings_can_resolve_named_target() {
        let temp = tempdir().unwrap();
        let config = temp.path().join("omnigraph.yaml");
        fs::write(
            &config,
            r#"
graphs:
  local:
    uri: ./demo.omni
  dev:
    uri: http://127.0.0.1:8080
server:
  graph: local
  bind: 127.0.0.1:8080
"#,
        )
        .unwrap();

        let settings =
            load_server_settings(Some(&config), None, None, Some("dev".to_string()), None, false)
                .await
                .unwrap();
        match &settings.mode {
            ServerConfigMode::Single { uri, graph_id, .. } => {
                assert_eq!(uri, "http://127.0.0.1:8080");
                assert_eq!(graph_id, "dev");
            }
            ServerConfigMode::Multi { .. } => panic!("expected Single mode, got Multi"),
        }
    }

    #[tokio::test]
    async fn server_settings_require_uri_from_cli_or_config() {
        let error = load_server_settings(None, None, None, None, None, false).await.unwrap_err();
        assert!(
            error.to_string().contains("no graph to serve"),
            "expected mode-inference error, got: {error}",
        );
    }

    #[test]
    fn classify_open_requires_explicit_unauthenticated_flag() {
        // State 1: no tokens, no policy, no flag → refuse to start.
        let error = classify_server_runtime_state(false, false, false).unwrap_err();
        let msg = error.to_string();
        assert!(
            msg.contains("--unauthenticated"),
            "expected refusal message mentioning --unauthenticated, got: {msg}"
        );

        // Same matrix cell but with the flag set → Open mode permitted.
        assert_eq!(
            classify_server_runtime_state(false, false, true).unwrap(),
            ServerRuntimeState::Open
        );
    }

    #[test]
    fn classify_tokens_without_policy_is_default_deny() {
        // State 2: tokens configured, no policy → DefaultDeny regardless
        // of the flag (the flag opts into the fully-open dev mode; it
        // doesn't downgrade default-deny back to open).
        assert_eq!(
            classify_server_runtime_state(true, false, false).unwrap(),
            ServerRuntimeState::DefaultDeny
        );
        assert_eq!(
            classify_server_runtime_state(true, false, true).unwrap(),
            ServerRuntimeState::DefaultDeny
        );
    }

    #[tokio::test]
    #[serial]
    async fn serve_refuses_to_start_with_policy_but_no_tokens_multi_mode() {
        // Bug 2 from the bot-review pass: multi-mode startup was missing
        // the "policy requires tokens" check that single-mode enforces.
        // After centralizing the check in `classify_server_runtime_state`,
        // both modes get the same enforcement. This test guards the
        // multi-mode propagation path.
        //
        // Sibling test below pins single mode. Together they pin that
        // the classifier is called from both branches of `serve()`.
        let _guard = EnvGuard::set(&[
            ("OMNIGRAPH_SERVER_BEARER_TOKEN", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET", None),
            ("OMNIGRAPH_UNAUTHENTICATED", None),
        ]);
        let temp = tempdir().unwrap();
        // The classifier reads `has_policy_configured` from the config
        // shape (does the Option contain a path?), not from file
        // existence, so we can hand it a path without writing a real
        // policy file — the bail fires before policy load.
        let policy_path = temp.path().join("server-policy.yaml");
        let config = ServerConfig {
            mode: ServerConfigMode::Multi {
                graphs: vec![GraphStartupConfig {
                    graph_id: "alpha".to_string(),
                    uri: temp
                        .path()
                        .join("alpha.omni")
                        .to_string_lossy()
                        .into_owned(),
                    policy_file: None,
                    queries: crate::queries::QueryRegistry::default(),
                }],
                config_path: temp.path().join("omnigraph.yaml"),
                server_policy_file: Some(policy_path),
            },
            bind: "127.0.0.1:0".to_string(),
            allow_unauthenticated: false,
        };
        let result = serve(config).await;
        let err = result
            .expect_err("serve should refuse to start in multi mode with policy but no tokens");
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("policy file is configured but no bearer tokens"),
            "expected policy-without-tokens rejection in multi mode, got: {msg}",
        );
    }

    #[tokio::test]
    #[serial]
    async fn serve_refuses_to_start_in_state_1_without_unauthenticated() {
        // MR-723 PR A: pin the integration boundary that the classifier
        // is actually called by `serve()` before any side-effecting
        // work (Lance dataset open, TcpListener::bind). The classifier
        // itself is unit-tested above; this test guards the propagation
        // path from `classify_server_runtime_state` through serve's
        // `?` so a future refactor that drops the call returns red.
        //
        // Marked `#[serial]` because we have to clear all bearer-token
        // env vars, and another test in this module setting any of them
        // concurrently would corrupt the read inside `resolve_token_source`.
        let _guard = EnvGuard::set(&[
            ("OMNIGRAPH_SERVER_BEARER_TOKEN", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_FILE", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", None),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_AWS_SECRET", None),
            ("OMNIGRAPH_UNAUTHENTICATED", None),
        ]);
        let temp = tempdir().unwrap();
        // Graph path doesn't need to exist — classifier fires before
        // `AppState::open_with_bearer_tokens_and_policy`.
        let config = ServerConfig {
            mode: ServerConfigMode::Single {
                uri: temp
                    .path()
                    .join("graph.omni")
                    .to_string_lossy()
                    .into_owned(),
                graph_id: "default".to_string(),
                policy_file: None,
                queries: crate::queries::QueryRegistry::default(),
            },
            bind: "127.0.0.1:0".to_string(),
            allow_unauthenticated: false,
        };
        let result = serve(config).await;
        let err =
            result.expect_err("serve should refuse to start in State 1 without --unauthenticated");
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("no bearer tokens") || msg.contains("policy file"),
            "expected refusal message naming the misconfiguration, got: {msg}",
        );
    }

    #[tokio::test]
    #[serial]
    async fn unauthenticated_env_var_classification() {
        // MR-723 PR A: closes the gap where the env-var read path inside
        // `load_server_settings` was structurally implemented but not
        // exercised by any test. Three properties to pin, all in one
        // sequential test because `cargo test` runs the mod test suite
        // in parallel and `OMNIGRAPH_UNAUTHENTICATED` is process-global
        // — interleaving with another test that sets the same env var
        // (concurrent classifier tests, even the bearer-token suite
        // sharing `EnvGuard`) corrupts the read. Sequential within one
        // test fn is the simplest race-free shape.
        let temp = tempdir().unwrap();
        let config_path = temp.path().join("omnigraph.yaml");
        fs::write(
            &config_path,
            r#"
graphs:
  local:
    uri: /tmp/demo-unauth.omni
server:
  graph: local
"#,
        )
        .unwrap();

        // Truthy values flip Open mode on, even with CLI flag off.
        for value in ["1", "true", "yes", "TRUE", "anything"] {
            let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", Some(value))]);
            let settings = load_server_settings(Some(&config_path), None, None, None, None, false).await
                .expect("settings load should succeed");
            assert!(
                settings.allow_unauthenticated,
                "OMNIGRAPH_UNAUTHENTICATED={value:?} should enable Open mode",
            );
        }

        // Falsy values keep refusal behavior, even with CLI flag off.
        for value in ["0", "false", "FALSE", ""] {
            let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", Some(value))]);
            let settings = load_server_settings(Some(&config_path), None, None, None, None, false).await
                .expect("settings load should succeed");
            assert!(
                !settings.allow_unauthenticated,
                "OMNIGRAPH_UNAUTHENTICATED={value:?} should NOT enable Open mode",
            );
        }

        // Unset env var: also false.
        let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", None)]);
        let settings = load_server_settings(Some(&config_path), None, None, None, None, false).await
            .expect("settings load should succeed");
        assert!(
            !settings.allow_unauthenticated,
            "OMNIGRAPH_UNAUTHENTICATED unset should NOT enable Open mode",
        );
        drop(_guard);

        // CLI flag wins even when env is falsy — `serve()` honors the
        // OR of both inputs.
        let _guard = EnvGuard::set(&[("OMNIGRAPH_UNAUTHENTICATED", Some("0"))]);
        let settings = load_server_settings(Some(&config_path), None, None, None, None, true).await
            .expect("settings load should succeed");
        assert!(
            settings.allow_unauthenticated,
            "--unauthenticated CLI flag should win even when env is falsy",
        );
    }

    #[test]
    fn classify_policy_enabled_requires_tokens() {
        // State 3: tokens + policy → PolicyEnabled, regardless of the
        // `allow_unauthenticated` flag (Cedar evaluates the bearer,
        // the flag is moot once tokens exist).
        assert_eq!(
            classify_server_runtime_state(true, true, false).unwrap(),
            ServerRuntimeState::PolicyEnabled
        );
        assert_eq!(
            classify_server_runtime_state(true, true, true).unwrap(),
            ServerRuntimeState::PolicyEnabled
        );
    }

    #[test]
    fn classify_policy_without_tokens_is_rejected() {
        // Closes the "policy installed but no tokens → silent 401 on
        // every request" footgun. The same shape that single-mode
        // `open_with_bearer_tokens_and_policy` used to bail on
        // privately is now rejected by the classifier so both single
        // and multi mode get the same enforcement from one source of
        // truth.
        for allow_unauthenticated in [false, true] {
            let err =
                classify_server_runtime_state(false, true, allow_unauthenticated).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("policy file is configured but no bearer tokens"),
                "expected policy-without-tokens rejection message; got: {msg}"
            );
            assert!(
                msg.contains("every request would 401"),
                "rejection message must name the failure mode; got: {msg}"
            );
        }
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

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
            let saved = vars
                .iter()
                .map(|(name, _)| (*name, env::var(name).ok()))
                .collect::<Vec<_>>();
            for (name, value) in vars {
                unsafe {
                    match value {
                        Some(value) => env::set_var(name, value),
                        None => env::remove_var(name),
                    }
                }
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.drain(..) {
                unsafe {
                    match value {
                        Some(value) => env::set_var(name, value),
                        None => env::remove_var(name),
                    }
                }
            }
        }
    }

    #[test]
    fn parse_bearer_tokens_json_reads_actor_token_map() {
        let tokens = parse_bearer_tokens_json(r#"{"alice":" token-a ","bob":"token-b"}"#).unwrap();
        assert_eq!(tokens.len(), 2);
        assert!(tokens.contains(&("alice".to_string(), " token-a ".to_string())));
        assert!(tokens.contains(&("bob".to_string(), "token-b".to_string())));
    }

    #[test]
    #[serial]
    fn server_bearer_tokens_from_env_reads_legacy_token_and_token_file() {
        let temp = tempdir().unwrap();
        let tokens_path = temp.path().join("tokens.json");
        fs::write(
            &tokens_path,
            r#"{"team-01":"token-one","team-02":"token-two"}"#,
        )
        .unwrap();

        let _guard = EnvGuard::set(&[
            ("OMNIGRAPH_SERVER_BEARER_TOKEN", Some(" legacy-token ")),
            (
                "OMNIGRAPH_SERVER_BEARER_TOKENS_FILE",
                Some(tokens_path.to_str().unwrap()),
            ),
            ("OMNIGRAPH_SERVER_BEARER_TOKENS_JSON", None),
        ]);

        let tokens = server_bearer_tokens_from_env().unwrap();
        assert_eq!(
            tokens,
            vec![
                ("default".to_string(), "legacy-token".to_string()),
                ("team-01".to_string(), "token-one".to_string()),
                ("team-02".to_string(), "token-two".to_string()),
            ]
        );
    }
}
