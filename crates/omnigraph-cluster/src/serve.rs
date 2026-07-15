//! Phase-5 serving snapshot: the read-only loader a `--cluster` server
//! boots from (moved verbatim from lib.rs in the modularization).

use super::*;

/// One graph in a serving snapshot: its id and on-disk root.
#[derive(Debug, Clone)]
pub struct ServingGraph {
    pub graph_id: String,
    pub root: PathBuf,
    pub embedding: Option<EmbeddingProviderConfig>,
}

/// One stored query: its graph binding, registry name, and verified source.
#[derive(Debug, Clone)]
pub struct ServingQuery {
    pub graph_id: String,
    pub name: String,
    pub source: String,
}

/// One policy bundle: its verified catalog blob path and applied bindings
/// (normalized typed refs: `cluster` | `graph.<id>`).
#[derive(Debug, Clone)]
pub struct ServingPolicy {
    pub name: String,
    /// The policy bundle CONTENT, digest-verified against the applied
    /// revision at read time. Content, not a path: the catalog may live on
    /// object storage, and the server must not re-read mutable state.
    pub source: String,
    pub applies_to: Vec<String>,
}

/// Everything a server needs to boot from the cluster catalog (RFC-005 §D2).
#[derive(Debug, Clone)]
pub struct ServingSnapshot {
    pub graphs: Vec<ServingGraph>,
    pub queries: Vec<ServingQuery>,
    pub policies: Vec<ServingPolicy>,
    pub diagnostics: Vec<Diagnostic>,
}

/// Read the applied revision as a serving snapshot — the read-only loader for
/// the Phase-5 server boot. Cluster-global readiness failures are still
/// all-or-nothing, but graph-attributed pending recovery sidecars quarantine
/// only that graph so healthy graphs can continue serving. This loader never
/// runs a recovery sweep.
/// Takes no lock: the state file is replaced atomically, so this reads a
/// consistent point-in-time ledger.
pub async fn read_serving_snapshot(
    config_dir: impl AsRef<Path>,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let config_dir = config_dir.as_ref().to_path_buf();
    // The declared storage: root decides where the ledger/catalog/graphs
    // live; config parse errors surface through the normal validation path.
    let parsed = parse_cluster_config(&config_dir);
    let storage_root = parsed.raw.as_ref().and_then(|raw| {
        raw.storage
            .as_deref()
            .map(str::trim)
            .filter(|root| !root.is_empty())
            .map(|root| root.trim_end_matches('/').to_string())
    });
    let backend = match storage_root.as_deref() {
        Some(root) => match ClusterStore::for_storage_root(root) {
            Ok(backend) => backend,
            Err(diagnostic) => return Err(vec![diagnostic]),
        },
        None => ClusterStore::for_config_dir(&config_dir),
    };
    read_snapshot_with_store(backend).await
}

/// Read the applied revision directly from a storage root URI — config-free
/// serving: a `--cluster s3://bucket/prefix` server needs no local files at
/// all, only the bucket and credentials. The ledger and catalog ARE the
/// deployment artifact.
pub async fn read_serving_snapshot_from_storage(
    storage_root: &str,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let backend =
        ClusterStore::for_storage_root(storage_root).map_err(|diagnostic| vec![diagnostic])?;
    read_snapshot_with_store(backend).await
}

/// Cluster root for a graph **storage URI** of the cluster layout
/// (`<root>/graphs/<id>.omni`), if `<root>` is actually a cluster (holds
/// `__cluster/state.json`); otherwise `None`. Used by the CLI to refuse
/// `init` into a cluster-managed location — graphs there are created by
/// `cluster apply`, not `init`.
///
/// Cheap by construction: a URI that does not match the `<root>/graphs/<id>.omni`
/// shape returns `None` without any I/O, so ordinary `init` targets
/// (`./kb.omni`, `s3://bucket/kb.omni`) never probe storage. Works for
/// `file://` and `s3://` via the storage adapter.
pub async fn cluster_root_for_graph_uri(graph_uri: &str) -> Option<String> {
    let root = cluster_root_of_graph_layout(graph_uri)?;
    let store = ClusterStore::for_storage_root(&root).ok()?;
    store
        .has_state()
        .await
        .then(|| store.display_root().to_string())
}

/// Resolve a graph's **storage URI** (`<root>/graphs/<id>.omni`) from a cluster's
/// applied state ledger — the lightweight path for storage-plane maintenance
/// (`optimize`/`repair`/`cleanup`).
///
/// Unlike [`read_serving_snapshot`], this deliberately does NOT validate catalog
/// payloads or recovery readiness: maintenance only needs the derivable graph
/// root, and must not be blocked by an unrelated corrupt policy/query blob or a
/// pending recovery sweep — a degraded cluster is exactly when an operator
/// reaches for `repair`. It reads the state ledger, confirms the graph is in the
/// applied revision, and returns `graph_root(id)`.
///
/// `cluster` is a config directory or a storage-root URI (`s3://…`, config-free),
/// mirroring the server's `--cluster` dispatch.
pub async fn resolve_graph_storage_uri(
    cluster: &str,
    graph_id: &str,
) -> Result<String, Diagnostic> {
    let backend = open_cluster_backend(cluster)?;
    let mut observations = backend.observations();
    let snapshot = backend.read_state(&mut observations).await?;
    let state = snapshot
        .state
        .ok_or_else(|| missing_state_diagnostic(cluster))?;
    let address = format!("graph.{graph_id}");
    if !state.applied_revision.resources.contains_key(&address) {
        let applied = applied_graph_ids(&state);
        return Err(Diagnostic::error(
            "graph_not_applied",
            address,
            format!(
                "graph `{graph_id}` is not applied in cluster `{cluster}` (applied graphs: [{}]); \
                 declare it in cluster.yaml and run `cluster apply`, or check the id",
                applied.join(", ")
            ),
        ));
    }
    Ok(backend.graph_root(graph_id))
}

/// List the graph ids applied in a cluster's served state (sorted). Reads the
/// ledger only — no catalog validation — like `resolve_graph_storage_uri`, so
/// it works on a degraded cluster. Used to enumerate candidates when no
/// `--graph` is selected (RFC-011 Decision 7).
pub async fn cluster_graph_ids(cluster: &str) -> Result<Vec<String>, Diagnostic> {
    let backend = open_cluster_backend(cluster)?;
    let mut observations = backend.observations();
    let snapshot = backend.read_state(&mut observations).await?;
    let state = snapshot
        .state
        .ok_or_else(|| missing_state_diagnostic(cluster))?;
    Ok(applied_graph_ids(&state))
}

fn open_cluster_backend(cluster: &str) -> Result<ClusterStore, Diagnostic> {
    if cluster.contains("://") {
        ClusterStore::for_storage_root(cluster)
    } else {
        Ok(ClusterStore::for_config_dir(Path::new(cluster)))
    }
}

fn missing_state_diagnostic(cluster: &str) -> Diagnostic {
    Diagnostic::error(
        "cluster_state_missing",
        CLUSTER_STATE_FILE,
        format!("cluster `{cluster}` has no applied state; run `cluster apply` first"),
    )
}

fn applied_graph_ids(state: &crate::types::ClusterState) -> Vec<String> {
    let mut ids: Vec<String> = state
        .applied_revision
        .resources
        .keys()
        .filter_map(|a| a.strip_prefix("graph."))
        .map(str::to_string)
        .collect();
    ids.sort();
    ids
}

/// Split `<root>/graphs/<id>.omni` → `<root>`, gating on the exact cluster
/// graph-layout shape (a single `<id>` segment, no nested path). `None` for
/// anything else — no I/O is done for non-cluster-shaped URIs.
fn cluster_root_of_graph_layout(graph_uri: &str) -> Option<String> {
    let trimmed = graph_uri.trim_end_matches('/');
    let rest = trimmed.strip_suffix(".omni")?;
    let (root, id) = rest.rsplit_once("/graphs/")?;
    if root.is_empty() || id.is_empty() || id.contains('/') {
        return None;
    }
    Some(root.to_string())
}

async fn read_snapshot_with_store(
    backend: ClusterStore,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    let mut startup_diagnostics: Vec<Diagnostic> = Vec::new();
    let mut quarantined_graphs: BTreeSet<String> = BTreeSet::new();

    // Do not sweep at serve time. Valid graph-attributed sidecars quarantine
    // that graph; malformed/unattributable sidecars remain cluster-fatal
    // because serving cannot prove their blast radius.
    let sidecar_diag_start = diagnostics.len();
    let sidecars = backend.list_recovery_sidecars(&mut diagnostics).await;
    // Every diagnostic `list_recovery_sidecars` appends is a genuine
    // read/parse/version failure (emitted as a warning by `store::list_json_dir`)
    // whose blast radius serving cannot prove — promote each to a cluster-fatal
    // error. This depends on that listing only ever emitting failure diagnostics;
    // if it grows a benign/informational one, promote by code instead.
    for diagnostic in diagnostics.iter_mut().skip(sidecar_diag_start) {
        diagnostic.severity = DiagnosticSeverity::Error;
    }
    for (path, sidecar) in sidecars {
        if sidecar.graph_id.trim().is_empty() {
            diagnostics.push(Diagnostic::error(
                "cluster_recovery_unattributed",
                path,
                "recovery sidecar has no graph id; run a state-mutating cluster command to sweep it before serving",
            ));
            continue;
        }
        quarantined_graphs.insert(sidecar.graph_id.clone());
        startup_diagnostics.push(Diagnostic::warning(
            "cluster_recovery_pending",
            graph_address(&sidecar.graph_id),
            format!(
                "graph `{}` is quarantined because interrupted operation `{}` awaits recovery; run any state-mutating cluster command (e.g. `cluster apply`) to sweep",
                sidecar.graph_id, sidecar.operation_id
            ),
        ));
    }
    if has_errors(&diagnostics) {
        return Err(diagnostics);
    }

    let mut observations = backend.observations();
    let state = match backend.read_state(&mut observations).await {
        Ok(snapshot) => match snapshot.state {
            Some(state) => Some(state),
            None => {
                diagnostics.push(Diagnostic::error(
                    "cluster_state_missing",
                    CLUSTER_STATE_FILE,
                    "no cluster state ledger; run `cluster import` and `cluster apply` first",
                ));
                None
            }
        },
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            None
        }
    };
    let Some(state) = state else {
        diagnostics.extend(startup_diagnostics);
        return Err(diagnostics);
    };

    let required_embedding_providers: BTreeSet<String> = state
        .applied_revision
        .resources
        .iter()
        .filter_map(|(address, entry)| match resource_kind(address) {
            ResourceKind::Graph(graph_id) if !quarantined_graphs.contains(&graph_id) => {
                entry.embedding_provider.clone()
            }
            _ => None,
        })
        .collect();
    let mut embedding_profiles: BTreeMap<String, EmbeddingProviderConfig> = BTreeMap::new();
    for (address, entry) in &state.applied_revision.resources {
        if !matches!(resource_kind(address), ResourceKind::EmbeddingProvider(_)) {
            continue;
        }
        if !required_embedding_providers.contains(address) {
            continue;
        }
        let Some(profile) = entry.embedding_profile.clone() else {
            diagnostics.push(Diagnostic::error(
                "embedding_provider_profile_missing",
                address.clone(),
                "no applied embedding provider profile recorded; re-run `cluster apply` to backfill",
            ));
            continue;
        };
        let actual_digest = embedding_provider_digest(&profile);
        if actual_digest != entry.digest {
            diagnostics.push(Diagnostic::error(
                "embedding_provider_digest_mismatch",
                address.clone(),
                format!(
                    "applied embedding provider profile does not match its recorded digest (actual sha256:{actual_digest}); run `cluster refresh` then `cluster apply`, and restart"
                ),
            ));
            continue;
        }
        embedding_profiles.insert(address.clone(), profile);
    }

    let mut graphs = Vec::new();
    let mut queries = Vec::new();
    let mut policies = Vec::new();
    let mut saw_applied_graph = false;
    for (address, entry) in &state.applied_revision.resources {
        match resource_kind(address) {
            ResourceKind::Graph(graph_id) => {
                saw_applied_graph = true;
                if quarantined_graphs.contains(&graph_id) {
                    continue;
                }
                let embedding = match entry.embedding_provider.as_deref() {
                    Some(provider_address) => match resource_kind(provider_address) {
                        ResourceKind::EmbeddingProvider(_) => {
                            match embedding_profiles.get(provider_address) {
                                Some(profile) => Some(profile.clone()),
                                None => {
                                    diagnostics.push(Diagnostic::error(
                                        "embedding_provider_missing",
                                        address.clone(),
                                        format!(
                                            "graph references `{provider_address}`, but no applied embedding provider profile is available; re-run `cluster apply`"
                                        ),
                                    ));
                                    None
                                }
                            }
                        }
                        _ => {
                            diagnostics.push(Diagnostic::error(
                                "wrong_kind_reference",
                                address.clone(),
                                format!(
                                    "graph embedding_provider expects `provider.embedding.<name>`, got `{provider_address}`"
                                ),
                            ));
                            None
                        }
                    },
                    None => None,
                };
                graphs.push(ServingGraph {
                    root: PathBuf::from(backend.graph_root(&graph_id)),
                    graph_id,
                    embedding,
                });
            }
            ResourceKind::Schema(_) => {}
            kind @ ResourceKind::Query { .. } => {
                let ResourceKind::Query { graph, name } = &kind else {
                    unreachable!()
                };
                if quarantined_graphs.contains(graph) {
                    continue;
                }
                match backend
                    .read_verified_payload(&kind, &entry.digest, address)
                    .await
                {
                    Ok(source) => queries.push(ServingQuery {
                        graph_id: graph.clone(),
                        name: name.clone(),
                        source,
                    }),
                    Err(diagnostic) => diagnostics.push(diagnostic),
                }
            }
            kind @ ResourceKind::Policy(_) => {
                let ResourceKind::Policy(name) = &kind else {
                    unreachable!()
                };
                let Some(applies_to) = entry.applies_to.clone() else {
                    diagnostics.push(Diagnostic::error(
                        "policy_bindings_missing",
                        address.clone(),
                        "no applied applies_to bindings recorded (ledger predates binding metadata); re-run `cluster apply` to backfill",
                    ));
                    continue;
                };
                let applies_to: Vec<String> = applies_to
                    .into_iter()
                    .filter(|binding| {
                        binding
                            .strip_prefix("graph.")
                            .is_none_or(|graph| !quarantined_graphs.contains(graph))
                    })
                    .collect();
                if applies_to.is_empty() {
                    continue;
                }
                match backend
                    .read_verified_payload(&kind, &entry.digest, address)
                    .await
                {
                    Ok(source) => policies.push(ServingPolicy {
                        name: name.clone(),
                        source,
                        applies_to,
                    }),
                    Err(diagnostic) => diagnostics.push(diagnostic),
                }
            }
            ResourceKind::EmbeddingProvider(_) => {}
            ResourceKind::Unknown => {}
        }
    }

    if graphs.is_empty() {
        if saw_applied_graph && !quarantined_graphs.is_empty() {
            diagnostics.push(Diagnostic::error(
                "cluster_no_healthy_graphs",
                CLUSTER_RECOVERIES_DIR,
                "all applied graphs are quarantined by pending recovery sidecars; run any state-mutating cluster command (e.g. `cluster apply`) to sweep, then retry",
            ));
        } else {
            diagnostics.push(Diagnostic::error(
                "cluster_empty",
                CLUSTER_STATE_FILE,
                "the applied revision records no graphs; apply a cluster with at least one graph before serving from it",
            ));
        }
    }
    if has_errors(&diagnostics) {
        diagnostics.extend(startup_diagnostics);
        return Err(diagnostics);
    }
    Ok(ServingSnapshot {
        graphs,
        queries,
        policies,
        diagnostics: startup_diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_layout_gating_does_no_io_for_non_cluster_shapes() {
        // Only `<root>/graphs/<id>.omni` matches; everything else is None.
        assert_eq!(
            cluster_root_of_graph_layout("/data/cluster/graphs/kb.omni").as_deref(),
            Some("/data/cluster")
        );
        assert_eq!(
            cluster_root_of_graph_layout("s3://bucket/prefix/graphs/kb.omni").as_deref(),
            Some("s3://bucket/prefix")
        );
        assert_eq!(cluster_root_of_graph_layout("./kb.omni"), None);
        assert_eq!(cluster_root_of_graph_layout("s3://bucket/kb.omni"), None);
        // nested id under graphs/ is not the cluster layout
        assert_eq!(cluster_root_of_graph_layout("/c/graphs/a/b.omni"), None);
        // not a .omni graph
        assert_eq!(cluster_root_of_graph_layout("/c/graphs/kb"), None);
    }

    #[tokio::test]
    async fn cluster_root_detected_only_when_state_ledger_present() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir_all(root.join("graphs")).unwrap();
        let graph_uri = format!("{}/graphs/kb.omni", root.to_string_lossy());

        // No __cluster/state.json yet → not a cluster.
        assert_eq!(cluster_root_for_graph_uri(&graph_uri).await, None);

        // Lay down the state ledger → now it's a cluster-managed location.
        std::fs::create_dir_all(root.join("__cluster")).unwrap();
        std::fs::write(root.join(CLUSTER_STATE_FILE), "{}").unwrap();
        let detected = cluster_root_for_graph_uri(&graph_uri).await;
        assert!(detected.is_some(), "expected cluster root to be detected");

        // A non-cluster-shaped target never probes and is always None.
        assert_eq!(
            cluster_root_for_graph_uri(&format!("{}/plain.omni", root.to_string_lossy())).await,
            None
        );
    }
}
