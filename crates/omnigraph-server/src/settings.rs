//! Server settings: cluster/CLI/env resolution, bearer-token sources, and
//! runtime-state classification (moved verbatim from lib.rs in the
//! modularization).

use super::*;
use std::io::Read;
use std::path::Path;

/// Build serving settings from a cluster directory's applied revision
/// (RFC-005 §D2): graphs at derived roots, stored queries from verified
/// catalog blob content, policy bundles from blob paths with their applied
/// bindings. Always multi-graph routing.
pub(crate) async fn load_cluster_settings(
    cluster_dir: &Path,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
) -> Result<ServerConfig> {
    // `--cluster` accepts either a config directory (the ledger location is
    // resolved through cluster.yaml's `storage:` key) or a storage-root URI
    // directly (`s3://bucket/prefix` or `az://container/prefix`) — config-free serving: the ledger and
    // catalog on the bucket ARE the deployment artifact.
    // Any supported scheme-qualified argument (s3://, az://, file://) is a storage root; a
    // bare path is a config directory.
    let cluster_arg = cluster_dir.to_string_lossy();
    let admitted = omnigraph_cluster::admit_serving_snapshot(&cluster_arg)
        .await
        .map_err(|diagnostics| serving_snapshot_error(cluster_dir, &diagnostics))?;
    let (snapshot, _, admission) = admitted.into_parts();
    let mut config = match settings_from_snapshot(
        cluster_dir,
        cli_bind,
        cli_allow_unauthenticated,
        cli_require_all_graphs,
        snapshot,
    ) {
        Ok(config) => config,
        Err(error) => return Err(release_settings_refusal(error, admission).await),
    };
    config.cluster_admission = admission;
    Ok(config)
}

/// Only completed read-only settings validation reaches this release. No graph
/// open, native effect or child owner has started; cancellation still retains
/// admission because it does not execute this branch.
async fn release_settings_refusal(
    error: color_eyre::Report,
    admission: Option<omnigraph_cluster::ClusterAdmission>,
) -> color_eyre::Report {
    if let Some(admission) = admission {
        if let Err(release) = admission.release_after_settlement().await {
            return eyre!(
                "{error}; preflight admission release failed: [{}] {}",
                release.code,
                release.message
            );
        }
    }
    error
}

fn serving_snapshot_error(
    cluster_dir: &Path,
    diagnostics: &[omnigraph_cluster::Diagnostic],
) -> color_eyre::Report {
    let diagnostic_cluster =
        omnigraph::storage::redacted_storage_uri(&cluster_dir.to_string_lossy());
    let details = diagnostics
        .iter()
        .map(|diagnostic| {
            format!(
                "[{}] {}: {}",
                diagnostic.code, diagnostic.path, diagnostic.message
            )
        })
        .collect::<Vec<_>>()
        .join("\n  ");
    eyre!("the cluster at '{diagnostic_cluster}' is not ready to serve:\n  {details}")
}

pub(crate) fn settings_from_snapshot(
    cluster_dir: &Path,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
    snapshot: omnigraph_cluster::ServingSnapshot,
) -> Result<ServerConfig> {
    settings_from_snapshot_inner(
        cluster_dir,
        cli_bind,
        cli_allow_unauthenticated,
        cli_require_all_graphs,
        snapshot,
        false,
    )
}

/// Project a settled deployment's achieved inventory, including unavailable
/// graphs. Startup's all-graphs-ready environment setting does not veto an
/// already-settled management-policy handoff.
pub(crate) fn deployment_settings_from_snapshot(
    cluster_dir: &Path,
    snapshot: omnigraph_cluster::ServingSnapshot,
) -> Result<ServerConfig> {
    settings_from_snapshot_inner(cluster_dir, None, false, false, snapshot, true)
}

fn settings_from_snapshot_inner(
    cluster_dir: &Path,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
    snapshot: omnigraph_cluster::ServingSnapshot,
    deployment: bool,
) -> Result<ServerConfig> {
    for diagnostic in &snapshot.diagnostics {
        warn!(
            code = %diagnostic.code,
            path = %diagnostic.path,
            message = %diagnostic.message,
            "cluster startup diagnostic"
        );
    }
    let env_require_all_graphs = env_flag("OMNIGRAPH_REQUIRE_ALL_GRAPHS");
    let require_all_graphs = !deployment && (cli_require_all_graphs || env_require_all_graphs);
    // Boot provenance is independent of the complete runtime graph inventory.
    let witness = BootWitness {
        booted_serving_digest: snapshot.config_digest.clone(),
        state_revision: snapshot.state_revision,
        state_cas: snapshot.state_cas.clone(),
    };
    if require_all_graphs && !snapshot.diagnostics.is_empty() {
        let details = snapshot
            .diagnostics
            .iter()
            .map(|diagnostic| {
                format!(
                    "[{}] {}: {}",
                    diagnostic.code, diagnostic.path, diagnostic.message
                )
            })
            .collect::<Vec<_>>()
            .join("\n  ");
        bail!(
            "strict cluster boot requires every applied graph to be ready; startup diagnostics:\n  {details}"
        );
    }

    // Bindings -> Cedar slots. The serving pipeline loads one bundle per
    // graph plus one server-level bundle; stacked bundles per scope are a
    // later slice — refuse loudly rather than silently merging policy.
    let mut server_policy: Option<PolicySource> = None;
    let mut graph_policies: BTreeMap<String, PolicySource> = BTreeMap::new();
    for policy in &snapshot.policies {
        for binding in &policy.applies_to {
            if binding == "cluster" {
                if server_policy
                    .replace(PolicySource::Inline(policy.source.clone()))
                    .is_some()
                {
                    bail!(
                        "multiple policy bundles bind the cluster scope; cluster-mode serving supports one bundle per scope — split or merge bundles (multi-bundle scopes are a later slice)"
                    );
                }
            } else if let Some(graph_id) = binding.strip_prefix("graph.") {
                if graph_policies
                    .insert(
                        graph_id.to_string(),
                        PolicySource::Inline(policy.source.clone()),
                    )
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
    let mut skipped_graphs = Vec::new();
    for graph in &snapshot.quarantined_graphs {
        graphs.push(GraphStartupConfig {
            startup_failure: Some(StartupFailure::InvalidConfiguration),
            graph_id: graph.graph_id.clone(),
            uri: graph.root.to_string_lossy().into_owned(),
            // The cluster refused this binding before loading its policy.
            policy: None,
            embedding: None,
            external_blob_policy: omnigraph::ExternalBlobPolicy::Deny,
            queries: QueryRegistry::default(),
        });
    }
    for graph in &snapshot.graphs {
        let mut startup_failure = None;
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
        let registry = match QueryRegistry::from_specs(specs) {
            Ok(registry) => registry,
            Err(errors) => {
                let details = errors
                    .iter()
                    .map(|error| error.to_string())
                    .collect::<Vec<_>>()
                    .join("\n  ");
                warn!(
                    graph_id = %graph.graph_id,
                    errors = %details,
                    "graph quarantined because stored queries failed to parse"
                );
                skipped_graphs.push(format!(
                    "{}: stored queries failed to parse: {details}",
                    graph.graph_id
                ));
                startup_failure = Some(StartupFailure::InvalidStoredQueries);
                QueryRegistry::default()
            }
        };
        let embedding = match graph
            .embedding
            .as_ref()
            .map(|profile| {
                profile.resolve().map_err(|err| {
                    eyre!("embedding provider for graph '{}': {err}", graph.graph_id)
                })
            })
            .transpose()
        {
            Ok(embedding) => embedding,
            Err(err) => {
                warn!(
                    graph_id = %graph.graph_id,
                    error = %err,
                    "graph quarantined because embedding provider configuration failed"
                );
                skipped_graphs.push(format!("{}: {err}", graph.graph_id));
                startup_failure = Some(StartupFailure::InvalidConfiguration);
                None
            }
        };
        graphs.push(GraphStartupConfig {
            startup_failure,
            graph_id: graph.graph_id.clone(),
            uri: graph.root.to_string_lossy().to_string(),
            policy: graph_policies.get(&graph.graph_id).cloned(),
            embedding,
            external_blob_policy: graph.external_blob_policy.clone(),
            queries: registry,
        });
    }
    graphs.sort_by(|a, b| a.graph_id.cmp(&b.graph_id));
    if !deployment
        && graphs.iter().all(|graph| graph.startup_failure.is_some())
        && !snapshot.applied_graphs.is_empty()
    {
        let skipped = skipped_graphs.join(", ");
        bail!(
            "the cluster at '{}' has no healthy graphs to serve{}",
            cluster_dir.display(),
            if skipped.is_empty() {
                String::new()
            } else {
                format!(" (quarantined: {skipped})")
            }
        );
    }
    if require_all_graphs && !skipped_graphs.is_empty() {
        bail!(
            "strict cluster boot requires every graph to build startup settings (quarantined: {})",
            skipped_graphs.join(", ")
        );
    }

    let env_unauth = env_flag("OMNIGRAPH_UNAUTHENTICATED");

    Ok(ServerConfig {
        mode: ServerConfigMode::Multi {
            graphs,
            config_path: cluster_dir.to_path_buf(),
            server_policy,
        },
        bind: cli_bind.unwrap_or_else(|| "127.0.0.1:8080".to_string()),
        allow_unauthenticated: cli_allow_unauthenticated || env_unauth,
        require_all_graphs,
        witness,
        // The binary resolves the flag, then the environment, then the default
        // (`resolve_shutdown_grace`); settings carry the default.
        shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
        cluster_admission: None,
    })
}

/// RFC-011 cluster-only boot: the server serves exclusively from a
/// cluster's applied revision (`--cluster <dir | s3://… | az://…>`). The legacy
/// omnigraph.yaml / `--target` / positional-URI single-graph boot paths
/// were removed — a deployment serves from exactly one source.
pub async fn load_server_settings(
    cli_cluster: Option<&PathBuf>,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
) -> Result<ServerConfig> {
    let cluster_dir = required_cluster(cli_cluster)?;
    load_cluster_settings(
        cluster_dir,
        cli_bind,
        cli_allow_unauthenticated,
        cli_require_all_graphs,
    )
    .await
}

/// Load applied serving settings with explicitly enabled offline data-token
/// trust. Root metadata and the snapshot come from the same opened Core store;
/// trust is validated before any graph engine can be opened for recovery.
pub async fn load_server_settings_with_data_token_trust(
    cli_cluster: Option<&PathBuf>,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
    trust_path: &Path,
) -> Result<ManagedServerConfig> {
    load_server_settings_with_identity_trust(
        cli_cluster,
        cli_bind,
        cli_allow_unauthenticated,
        cli_require_all_graphs,
        Some(trust_path),
        None,
    )
    .await
}

/// Validate every enabled identity profile against the same opened serving
/// store before any engine can open. Direct/static configuration is unchanged.
pub async fn load_server_settings_with_identity_trust(
    cli_cluster: Option<&PathBuf>,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
    data_trust_path: Option<&Path>,
    oidc_trust_path: Option<&Path>,
) -> Result<ManagedServerConfig> {
    if data_trust_path.is_none() && oidc_trust_path.is_none() {
        bail!("at least one explicit identity trust profile is required");
    }
    let cluster_dir = required_cluster(cli_cluster)?;
    let cluster_arg = cluster_dir.to_string_lossy();
    let bound = omnigraph_cluster::admit_serving_snapshot(&cluster_arg)
        .await
        .map_err(|diagnostics| serving_snapshot_error(cluster_dir, &diagnostics))?;
    settings_from_admitted(
        cluster_dir,
        cli_bind,
        cli_allow_unauthenticated,
        cli_require_all_graphs,
        bound,
        (data_trust_path, oidc_trust_path),
        SettingsRefusal::ReleaseReadOnlyAdmission,
    )
    .await
}

/// Claim an exact fresh, empty S3 bootstrap receipt before constructing serving
/// settings. This is an administrator-supplied boot input, never HTTP authority.
/// Failure does not retry, fall back to ordinary admission, or remove the lock.
/// Optional identity profiles retain their ordinary canonical-root validation.
pub async fn load_server_settings_with_bootstrap_handoff(
    cli_cluster: Option<&PathBuf>,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
    receipt_path: &Path,
    data_trust_path: Option<&Path>,
    oidc_trust_path: Option<&Path>,
) -> Result<ManagedServerConfig> {
    let cluster_dir = required_cluster(cli_cluster)?;
    let receipt = read_bootstrap_handoff(receipt_path)?;
    let bound =
        omnigraph_cluster::claim_bootstrap_serving(&cluster_dir.to_string_lossy(), &receipt)
            .await
            .map_err(|diagnostics| serving_snapshot_error(cluster_dir, &diagnostics))?;
    settings_from_admitted(
        cluster_dir,
        cli_bind,
        cli_allow_unauthenticated,
        cli_require_all_graphs,
        bound,
        (data_trust_path, oidc_trust_path),
        SettingsRefusal::RetainClaimedAdmission,
    )
    .await
}

fn read_bootstrap_handoff(path: &Path) -> Result<omnigraph_cluster::BootstrapServingReceipt> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // A mistaken FIFO path must not block boot waiting for a writer.
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .wrap_err("failed to open bootstrap handoff receipt")?;
    if !file.metadata()?.is_file() {
        bail!("bootstrap handoff receipt must be a regular file");
    }
    let limit = omnigraph_cluster::MAX_BOOTSTRAP_SERVING_RECEIPT_BYTES;
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .wrap_err("failed to read bootstrap handoff receipt")?;
    if bytes.len() > limit {
        bail!("bootstrap handoff receipt exceeds {limit} bytes");
    }
    serde_json::from_slice(&bytes).wrap_err("invalid bootstrap handoff receipt JSON")
}

enum SettingsRefusal {
    ReleaseReadOnlyAdmission,
    RetainClaimedAdmission,
}

async fn settings_from_admitted(
    cluster_dir: &Path,
    cli_bind: Option<String>,
    cli_allow_unauthenticated: bool,
    cli_require_all_graphs: bool,
    bound: omnigraph_cluster::AdmittedServingSnapshot,
    (data_trust_path, oidc_trust_path): (Option<&Path>, Option<&Path>),
    refusal: SettingsRefusal,
) -> Result<ManagedServerConfig> {
    let canonical_root = bound.canonical_root().to_string();
    let (snapshot, _, admission) = bound.into_parts();
    let validated: Result<_> = (|| {
        let trust = data_trust_path
            .map(|path| data_tokens::DataTokenTrust::read(path, &canonical_root))
            .transpose()?;
        let oidc_trust = oidc_trust_path
            .map(|path| oidc_identity::OidcIdentityTrust::read(path, &canonical_root))
            .transpose()?;
        let config = settings_from_snapshot(
            cluster_dir,
            cli_bind,
            cli_allow_unauthenticated,
            cli_require_all_graphs,
            snapshot,
        )?;
        Ok((config, trust, oidc_trust))
    })();
    let (mut config, trust, oidc_trust) = match validated {
        Ok(validated) => validated,
        Err(error) => {
            return Err(match refusal {
                SettingsRefusal::ReleaseReadOnlyAdmission => {
                    release_settings_refusal(error, admission).await
                }
                // Even a completed settings refusal must retain a claimed lock:
                // delayed bootstrap attempts rely on this object never vanishing.
                SettingsRefusal::RetainClaimedAdmission => error,
            });
        }
    };
    config.cluster_admission = admission;
    Ok(ManagedServerConfig {
        config,
        canonical_root,
        trust,
        oidc_trust,
    })
}

fn required_cluster(cli_cluster: Option<&PathBuf>) -> Result<&PathBuf> {
    let Some(cluster_dir) = cli_cluster else {
        bail!(
            "omnigraph-server boots from a cluster: pass --cluster <dir|s3://…|az://…> \
             (the cluster's applied revision is the deployment artifact). The legacy \
             single-graph boot (positional <URI>, --target, --config omnigraph.yaml) \
             has been removed."
        );
    };
    Ok(cluster_dir)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            let trimmed = v.trim();
            !trimmed.is_empty() && trimmed != "0" && !trimmed.eq_ignore_ascii_case("false")
        })
        .unwrap_or(false)
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
             docs/user/operations/server.md). Declare required graph and cluster \
             policy bundles when bootstrapping; this deployment class keeps existing \
             policy bindings fixed."
        ),
        (false, false, true) => Ok(ServerRuntimeState::Open),
        (true, false, _) => Ok(ServerRuntimeState::DefaultDeny),
        (false, true, _) => bail!(
            "policy file is configured but no bearer tokens — every request would 401 \
             because no token can ever match. Configure at least one bearer token (see \
             docs/user/operations/server.md), or remove the policy file. To deny all unauthenticated \
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

pub(crate) fn validate_bearer_tokens(
    entries: Vec<(String, String)>,
) -> Result<Vec<(String, String)>> {
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
        BTreeMap, DEFAULT_SHUTDOWN_GRACE, Path, PathBuf, open_multi_graph_state,
        settings_from_snapshot,
    };
    use super::{
        GraphId, GraphKey, GraphStartupConfig, RegistryLookup, ServerConfig, ServerConfigMode,
        ServerRuntimeState, StartupFailure, classify_server_runtime_state, hash_bearer_token,
        normalize_bearer_token, parse_bearer_tokens_json, serve, server_bearer_tokens_from_env,
    };
    use serial_test::serial;
    use std::env;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tempfile::tempdir;

    #[test]
    fn bootstrap_handoff_reader_is_strict_and_bounded() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("receipt.json");
        let value = serde_json::json!({
            "version": 1, "canonical_root": "s3://example/cluster",
            "bootstrap_lock_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "bootstrap_lock_version": "\"etag\"",
            "ledger_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW", "state_revision": 3,
            "state_cas": format!("sha256:{}", "a".repeat(64)),
            "deployment_id": "01ARZ3NDEKTSV4RRFFQ69G5FAW:1:01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "input_digest": "b".repeat(64), "config_digest": "c".repeat(64),
            "result_revision": 1
        });
        let mut exact = serde_json::to_vec(&value).unwrap();
        exact.resize(omnigraph_cluster::MAX_BOOTSTRAP_SERVING_RECEIPT_BYTES, b' ');
        fs::write(&path, &exact).unwrap();
        assert_eq!(
            super::read_bootstrap_handoff(&path).unwrap().canonical_root,
            "s3://example/cluster"
        );
        exact.push(b' ');
        fs::write(&path, exact).unwrap();
        assert!(
            super::read_bootstrap_handoff(&path)
                .unwrap_err()
                .to_string()
                .contains("exceeds 16384 bytes")
        );
        for invalid in [
            "{".to_string(),
            "{}".to_string(),
            format!("{value} {{}}"),
            value.to_string().replacen('{', "{\"version\":1,", 1),
            value
                .to_string()
                .replacen('{', "{\"unrecognized\":true,", 1),
        ] {
            fs::write(&path, invalid).unwrap();
            assert!(
                super::read_bootstrap_handoff(&path)
                    .unwrap_err()
                    .to_string()
                    .contains("invalid bootstrap handoff receipt JSON")
            );
        }
        assert!(super::read_bootstrap_handoff(temp.path()).is_err());
    }

    #[tokio::test]
    async fn malformed_bootstrap_handoff_never_falls_back_to_ordinary_boot() {
        let temp = tempdir().unwrap();
        let cluster = temp.path().join("must-not-be-created");
        let receipt = temp.path().join("receipt.json");
        fs::write(&receipt, b"{}").unwrap();
        let error = super::load_server_settings_with_bootstrap_handoff(
            Some(&cluster),
            None,
            true,
            false,
            &receipt,
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid bootstrap handoff receipt JSON"),
            "{error}"
        );
        assert!(!cluster.exists());
    }

    /// Uses the same admitted startup and HTTP deployment path as `serve_config`.
    /// S3 qualification is independently gated from the local settings suite.
    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn s3_bootstrap_handoff_serves_empty_then_deploys_first_graph() {
        use crate::api::{HTTP_API_CONTRACT, HTTP_API_CONTRACT_HEADER};
        use axum::body::{Body, to_bytes};
        use axum::http::{Request, StatusCode};
        use serde_json::{Value, json};
        use tower::ServiceExt;

        let Ok(bucket) = env::var("OMNIGRAPH_S3_TEST_BUCKET") else {
            eprintln!("skipping S3 bootstrap serving test: OMNIGRAPH_S3_TEST_BUCKET is not set");
            return;
        };
        async fn response(
            app: &axum::Router,
            method: &str,
            path: &str,
            body: Value,
        ) -> (StatusCode, Value) {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .header(HTTP_API_CONTRACT_HEADER, HTTP_API_CONTRACT)
                        .header("authorization", "Bearer bootstrap-secret")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            let code = response.status();
            let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
            (code, serde_json::from_slice(&bytes).unwrap())
        }
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = format!(
            "s3://{bucket}/server-bootstrap/{}-{unique}",
            std::process::id()
        );
        let temp = tempdir().unwrap();
        let source = format!(
            "version: 1\nstorage: {root}\npolicies:\n  management:\n    file: ./management.yaml\n    applies_to: [cluster]\n"
        );
        fs::write(temp.path().join("management.yaml"), "version: 1\ngroups:\n  operators: [operator]\nrules:\n  - id: manage\n    allow:\n      actors: {group: operators}\n      actions: [config_manage]\n  - id: discover\n    allow:\n      actors: {group: operators}\n      actions: [graph_list]\n").unwrap();
        fs::write(temp.path().join("cluster.yaml"), &source).unwrap();
        let caller = omnigraph_cluster::DeploymentCaller::storage_owner(None);
        let receipt = omnigraph_cluster::bootstrap_serving(temp.path(), &caller)
            .await
            .unwrap();
        let receipt_path = temp.path().join("receipt.json");
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        let root_path = PathBuf::from(&root);
        let storage = omnigraph::storage::storage_for_uri(&root).unwrap();
        let lock_uri = format!("{root}/__cluster/lock.json");
        let bootstrap_lock = storage.read_text(&lock_uri).await.unwrap();
        // A receipt may not choose a different root than the administrator did.
        let wrong = PathBuf::from(format!("{root}-wrong"));
        assert!(
            super::load_server_settings_with_bootstrap_handoff(
                Some(&wrong),
                None,
                false,
                false,
                &receipt_path,
                None,
                None,
            )
            .await
            .is_err()
        );
        assert_eq!(storage.read_text(&lock_uri).await.unwrap(), bootstrap_lock);
        for name in ["lock.json", "state.json"] {
            assert!(
                storage
                    .read_text_if_exists(&format!("{root}-wrong/__cluster/{name}"))
                    .await
                    .unwrap()
                    .is_none()
            );
        }

        let settings = super::load_server_settings_with_bootstrap_handoff(
            Some(&root_path),
            None,
            false,
            false,
            &receipt_path,
            None,
            None,
        )
        .await
        .unwrap();
        let config = settings.config;
        let owner = config.cluster_admission.as_ref().unwrap();
        assert_ne!(owner.lock_id(), receipt.bootstrap_lock_id);
        assert_eq!(owner.canonical_root(), receipt.canonical_root);
        assert_eq!(config.witness.state_revision, receipt.state_revision);
        assert_eq!(
            config.witness.state_cas.as_deref(),
            Some(receipt.state_cas.as_str())
        );
        let serving_lock = storage.read_text(&lock_uri).await.unwrap();
        assert!(
            super::load_server_settings_with_bootstrap_handoff(
                Some(&root_path),
                None,
                false,
                false,
                &receipt_path,
                None,
                None,
            )
            .await
            .is_err(),
            "a receipt must be single-claim"
        );
        assert_eq!(storage.read_text(&lock_uri).await.unwrap(), serving_lock);

        let ServerConfigMode::Multi {
            graphs,
            config_path,
            server_policy,
        } = config.mode;
        assert!(graphs.is_empty());
        let state = super::open_multi_graph_state_admitted(
            graphs,
            vec![("operator".into(), "bootstrap-secret".into())],
            server_policy.as_ref(),
            config_path,
            false,
            config.cluster_admission,
        )
        .await
        .unwrap()
        .with_boot_witness(
            config.witness,
            Arc::new(AtomicBool::new(false)),
            DEFAULT_SHUTDOWN_GRACE,
        );
        super::deployment::initialize_boot_activation(&state);
        let app = super::build_app(state);
        let (code, ready) = response(&app, "GET", "/readyz", Value::Null).await;
        assert_eq!(code, StatusCode::OK, "{ready}");
        assert_eq!(ready["ready_graph_count"], 0);
        let (code, inventory) = response(&app, "GET", "/graphs", Value::Null).await;
        assert_eq!(code, StatusCode::OK, "{inventory}");
        assert!(inventory["graphs"].as_array().unwrap().is_empty());
        let (code, status) = response(&app, "GET", "/cluster/deployments", Value::Null).await;
        assert_eq!(code, StatusCode::OK, "{status}");
        let status: omnigraph_cluster::DeploymentStatus =
            serde_json::from_value(status["status"].clone()).unwrap();
        let id = status.next_deployment_id();
        fs::write(
            temp.path().join("people.pg"),
            "node Person { name: String @key }\n",
        )
        .unwrap();
        fs::write(
            temp.path().join("cluster.yaml"),
            format!("{source}graphs:\n  people:\n    schema: ./people.pg\n"),
        )
        .unwrap();
        let candidate = omnigraph_cluster::capture_deployment(temp.path()).unwrap();
        let (code, accepted) = response(
            &app,
            "POST",
            "/cluster/deployments",
            json!({"deployment_id":id,"deployment":candidate}),
        )
        .await;
        assert!(
            matches!(code, StatusCode::OK | StatusCode::ACCEPTED),
            "{code}: {accepted}"
        );
        let completed = tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let (code, result) = response(
                    &app,
                    "GET",
                    &format!("/cluster/deployments/{id}"),
                    Value::Null,
                )
                .await;
                assert_eq!(code, StatusCode::OK, "{result}");
                if result["in_progress"] == false {
                    break result;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("first graph deployment must complete");
        assert_eq!(completed["active"], true, "{completed}");
        assert_eq!(
            completed["deployment"]["result"]["converged"], true,
            "{completed}"
        );
        let (code, inventory) = response(&app, "GET", "/graphs", Value::Null).await;
        assert_eq!(code, StatusCode::OK, "{inventory}");
        assert_eq!(inventory["graphs"][0]["graph_id"], "people");
        let (code, snapshot) = response(&app, "GET", "/graphs/people/snapshot", Value::Null).await;
        assert_eq!(code, StatusCode::OK, "{snapshot}");
        assert!(snapshot["datasets"].is_array(), "{snapshot}");
        assert_eq!(storage.read_text(&lock_uri).await.unwrap(), serving_lock);
        drop(app);
        assert_eq!(storage.read_text(&lock_uri).await.unwrap(), serving_lock);

        // Invalid identity trust after a successful claim must retain the fresh
        // owner too, even though startup never constructs a router/listener.
        let refused_root = format!("{root}-trust-refused");
        fs::write(
            temp.path().join("cluster.yaml"),
            source.replace(&root, &refused_root),
        )
        .unwrap();
        let receipt = omnigraph_cluster::bootstrap_serving(temp.path(), &caller)
            .await
            .unwrap();
        fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        let trust = temp.path().join("bad-trust.json");
        fs::write(&trust, b"{}").unwrap();
        let refused_path = PathBuf::from(&refused_root);
        assert!(
            super::load_server_settings_with_bootstrap_handoff(
                Some(&refused_path),
                None,
                false,
                false,
                &receipt_path,
                Some(&trust),
                None,
            )
            .await
            .is_err()
        );
        let refused_uri = format!("{refused_root}/__cluster/lock.json");
        let retained = storage.read_text(&refused_uri).await.unwrap();
        let retained_json: Value = serde_json::from_str(&retained).unwrap();
        assert_ne!(retained_json["lock_id"], receipt.bootstrap_lock_id);
        assert!(
            super::load_server_settings_with_bootstrap_handoff(
                Some(&refused_path),
                None,
                false,
                false,
                &receipt_path,
                None,
                None,
            )
            .await
            .is_err()
        );
        assert_eq!(storage.read_text(&refused_uri).await.unwrap(), retained);
    }

    /// `authorize` returns the allow/deny **decision** (`Authz`) and reserves
    /// `Err` for operational failures, so the invoke handler can hide a denial
    /// as 404 without also masking a 401/500. Pins each outcome.
    #[test]
    fn authorize_splits_decision_from_operational_error() {
        use super::{
            AuthenticatedActor, Authz, PolicyAction, PolicyCompiler, PolicyConfig, PolicyRequest,
            authorize,
        };
        use std::sync::Arc;

        fn req(action: PolicyAction) -> PolicyRequest {
            PolicyRequest {
                action,
                branch: None,
                target_branch: None,
            }
        }
        let actor = AuthenticatedActor::cluster_static(Arc::from("act-alice"));

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
                PolicyRequest {
                    action: PolicyAction::Read,
                    branch: Some("main".to_string()),
                    target_branch: None
                },
            )
            .unwrap(),
            Authz::Allowed
        ));
        // Known actor, no matching allow rule → Denied, carrying the decision message.
        match authorize(
            Some(&actor),
            Some(&engine),
            PolicyRequest {
                action: PolicyAction::Change,
                branch: Some("main".to_string()),
                target_branch: None,
            },
        )
        .unwrap()
        {
            Authz::Denied(message) => {
                assert!(!message.is_empty(), "a deny carries its decision message")
            }
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
        let empty = super::validate_and_attach(QueryRegistry::default(), &catalog, "g").unwrap();
        assert!(empty.is_none());

        // A query that type-checks → attached.
        let ok = QueryRegistry::from_specs(vec![spec(
            "find_user",
            "query find_user() { match { $u: User } return { $u.name } }",
        )])
        .unwrap();
        assert!(
            super::validate_and_attach(ok, &catalog, "g")
                .unwrap()
                .is_some()
        );

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
        assert!(
            msg.contains("schema check"),
            "mentions the schema check: {msg}"
        );
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
    async fn server_settings_require_cluster_boot_source() {
        // RFC-011 cluster-only: with no --cluster the server refuses to
        // start and names the cluster-required remedy.
        let error = super::load_server_settings(None, None, false, false)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("boots from a cluster"),
            "expected cluster-required error, got: {error}",
        );
    }

    #[tokio::test]
    async fn server_settings_redact_cluster_uri_credentials() {
        let secret = "TOPSECRET-SERVER-SAS";
        let cluster = std::path::PathBuf::from(format!(
            "az://omnigraph/clusters/company-brain?sv=2026-01-01&sig={secret}"
        ));
        let error = super::load_server_settings(Some(&cluster), None, false, false)
            .await
            .unwrap_err()
            .to_string();
        assert!(!error.contains(secret));
        assert!(error.contains("az://omnigraph/clusters/company-brain"));
        assert!(error.contains("query redacted"));
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
                    startup_failure: None,
                    graph_id: "alpha".to_string(),
                    uri: temp
                        .path()
                        .join("alpha.omni")
                        .to_string_lossy()
                        .into_owned(),
                    policy: None,
                    embedding: None,
                    external_blob_policy: omnigraph::ExternalBlobPolicy::Deny,
                    queries: crate::queries::QueryRegistry::default(),
                }],
                config_path: temp.path().join("omnigraph.yaml"),
                server_policy: Some(crate::PolicySource::File(policy_path)),
            },
            bind: "127.0.0.1:0".to_string(),
            allow_unauthenticated: false,
            require_all_graphs: false,
            witness: crate::BootWitness::default(),
            shutdown_grace: crate::DEFAULT_SHUTDOWN_GRACE,
            cluster_admission: None,
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
        // any engine open.
        let config = ServerConfig {
            mode: ServerConfigMode::Multi {
                graphs: vec![GraphStartupConfig {
                    startup_failure: None,
                    graph_id: "default".to_string(),
                    uri: temp
                        .path()
                        .join("graph.omni")
                        .to_string_lossy()
                        .into_owned(),
                    policy: None,
                    embedding: None,
                    external_blob_policy: omnigraph::ExternalBlobPolicy::Deny,
                    queries: crate::queries::QueryRegistry::default(),
                }],
                config_path: temp.path().join("cluster"),
                server_policy: None,
            },
            bind: "127.0.0.1:0".to_string(),
            allow_unauthenticated: false,
            require_all_graphs: false,
            witness: crate::BootWitness::default(),
            shutdown_grace: crate::DEFAULT_SHUTDOWN_GRACE,
            cluster_admission: None,
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

    /// Every file under `root` with its bytes, so a boot attempt can prove it
    /// moved neither the ledger nor any graph's storage.
    fn tree_bytes(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut files = BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    let bytes = std::fs::read(&path).unwrap();
                    files.insert(path, bytes);
                }
            }
        }
        files
    }

    /// A graph whose applied server-safe external Blob base overlaps the
    /// cluster storage root is quarantined at boot: an ordinary boot serves
    /// the healthy sibling and reports the quarantine, a strict boot refuses,
    /// and neither moves the ledger or any graph. Server-safe bases are
    /// `s3://` only, so the cluster is applied in a local directory (where
    /// the base is disjoint and apply accepts it) and the production snapshot
    /// reader then reads it with the storage root spelled as the overlapping
    /// `s3://` prefix. Graph roots derived from that spelling name the same
    /// bytes, so both graph URIs return to their local roots before startup
    /// admission probes cluster membership; only the served sibling is opened.
    #[tokio::test]
    async fn boot_quarantines_overlapping_external_blob_base_and_strict_boot_refuses() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("people.pg"),
            "\nnode Person {\n  name: String @key\n}\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("cluster.yaml"),
            r#"
version: 1
graphs:
  knowledge:
    schema: ./people.pg
    external_blobs:
      allow:
        - base: s3://assets/cluster/graphs/
          scope: server_safe
  archive:
    schema: ./people.pg
"#,
        )
        .unwrap();
        let caller = omnigraph_cluster::DeploymentCaller::storage_owner(None);
        let apply = omnigraph_cluster::apply_deployment(dir.path(), None, &caller, |_, _, _| {})
            .await
            .unwrap();
        assert!(
            matches!(apply, omnigraph_cluster::DeploymentLookup::Complete { ref result } if result.converged),
            "{apply:?}"
        );
        let status =
            omnigraph_cluster::deployment_status(dir.path().to_str().unwrap(), None, &caller)
                .await
                .unwrap();
        if let Some(lock_id) = status.lock_id {
            omnigraph_cluster::force_unlock_storage_root(dir.path().to_str().unwrap(), &lock_id)
                .await
                .unwrap();
        }
        let before = tree_bytes(dir.path());

        let snapshot = omnigraph_cluster::read_serving_snapshot_with_display_root(
            dir.path(),
            "s3://assets/cluster",
        )
        .await
        .unwrap();
        assert_eq!(snapshot.quarantined_graphs.len(), 1);
        assert_eq!(snapshot.quarantined_graphs[0].graph_id, "knowledge");
        assert_eq!(
            snapshot.quarantined_graphs[0].root,
            PathBuf::from("s3://assets/cluster/graphs/knowledge.omni"),
        );
        assert!(snapshot.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "external_blob_base_overlaps_storage_root"
                && diagnostic.path == "graph.knowledge"
        }));

        // Strict boot refuses on the quarantine diagnostic before building
        // any graph's settings.
        let refused =
            settings_from_snapshot(dir.path(), None, true, true, snapshot.clone()).unwrap_err();
        let refused = refused.to_string();
        assert!(
            refused.contains("strict cluster boot")
                && refused.contains("external_blob_base_overlaps_storage_root")
                && refused.contains("graph.knowledge"),
            "{refused}"
        );

        // Ordinary boot serves the sibling and reports the quarantine.
        let config = settings_from_snapshot(dir.path(), None, true, false, snapshot).unwrap();
        assert!(!config.require_all_graphs);
        let ServerConfigMode::Multi {
            mut graphs,
            config_path,
            server_policy,
        } = config.mode;
        assert_eq!(
            graphs
                .iter()
                .map(|graph| graph.graph_id.as_str())
                .collect::<Vec<_>>(),
            vec!["archive", "knowledge"]
        );
        assert_eq!(
            graphs[1].startup_failure,
            Some(StartupFailure::InvalidConfiguration)
        );
        assert_eq!(graphs[0].uri, "s3://assets/cluster/graphs/archive.omni");
        for graph in &mut graphs {
            graph.uri = dir
                .path()
                .join(format!("graphs/{}.omni", graph.graph_id))
                .to_string_lossy()
                .to_string();
        }
        let state = open_multi_graph_state(
            graphs,
            Vec::new(),
            server_policy.as_ref(),
            config_path,
            false,
        )
        .await
        .unwrap()
        .with_boot_witness(
            config.witness,
            Arc::new(AtomicBool::new(false)),
            DEFAULT_SHUTDOWN_GRACE,
        );
        assert_eq!(
            state
                .routing
                .registry
                .list()
                .iter()
                .map(|handle| handle.key.graph_id.as_str().to_string())
                .collect::<Vec<_>>(),
            vec!["archive".to_string()]
        );
        assert_eq!(state.routing.registry.len(), 2);
        match state
            .routing
            .registry
            .get(&GraphKey::cluster(GraphId::try_from("knowledge").unwrap()))
        {
            RegistryLookup::Blocked(graph) => {
                assert_eq!(graph.failure, StartupFailure::InvalidConfiguration);
                assert!(graph.policy.is_none());
            }
            _ => panic!("snapshot refusal must remain in the runtime inventory"),
        }
        let admission = state.cluster_admission.clone().unwrap();
        drop(state);
        // This fixture has issued no requests; completed local opens have no
        // surviving native writer. Hand off its exact owner before another boot.
        admission.release_after_settlement().await.unwrap();

        // Settings failures must retain the same complete inventory too.
        // Point the rejected graph at a missing root: these failures must
        // survive without attempting an engine open at that root.
        let snapshot = omnigraph_cluster::read_serving_snapshot(dir.path())
            .await
            .unwrap();
        let rejected_root = dir.path().join("graphs/knowledge.omni");
        let saved_root = dir.path().join("saved-knowledge");
        std::fs::rename(&rejected_root, &saved_root).unwrap();
        for failure in [
            StartupFailure::InvalidStoredQueries,
            StartupFailure::InvalidConfiguration,
        ] {
            let mut snapshot = snapshot.clone();
            let rejected = snapshot
                .graphs
                .iter_mut()
                .find(|graph| graph.graph_id == "knowledge")
                .unwrap();
            rejected.root = rejected_root.clone();
            if failure == StartupFailure::InvalidStoredQueries {
                snapshot.queries.push(omnigraph_cluster::ServingQuery {
                    graph_id: "knowledge".to_string(),
                    name: "broken".to_string(),
                    source: "invalid query".to_string(),
                });
            } else {
                rejected.embedding = Some(omnigraph_cluster::EmbeddingProviderConfig {
                    kind: Some("openai".to_string()),
                    base_url: None,
                    model: None,
                    api_key: None,
                });
            }
            let config = settings_from_snapshot(dir.path(), None, true, false, snapshot).unwrap();
            let ServerConfigMode::Multi {
                graphs,
                config_path,
                server_policy,
            } = config.mode;
            assert_eq!(graphs.len(), 2);
            let state = open_multi_graph_state(
                graphs,
                Vec::new(),
                server_policy.as_ref(),
                config_path,
                false,
            )
            .await
            .unwrap();
            assert_eq!(state.routing.registry.len(), 2);
            assert_eq!(state.routing.registry.list().len(), 1);
            match state
                .routing
                .registry
                .get(&GraphKey::cluster(GraphId::try_from("knowledge").unwrap()))
            {
                RegistryLookup::Blocked(graph) => assert_eq!(graph.failure, failure),
                _ => panic!("settings refusal must remain in the runtime inventory"),
            }
            assert!(!rejected_root.exists());
            let admission = state.cluster_admission.clone().unwrap();
            drop(state);
            admission.release_after_settlement().await.unwrap();
        }

        std::fs::rename(&saved_root, &rejected_root).unwrap();
        assert_eq!(tree_bytes(dir.path()), before);
    }
}
