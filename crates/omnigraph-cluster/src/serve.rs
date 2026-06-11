//! Phase-5 serving snapshot: the read-only loader a `--cluster` server
//! boots from (moved verbatim from lib.rs in the modularization).

use super::*;

/// One graph in a serving snapshot: its id and on-disk root.
#[derive(Debug, Clone)]
pub struct ServingGraph {
    pub graph_id: String,
    pub root: PathBuf,
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
}

/// Read the applied revision as a serving snapshot — the read-only loader for
/// the Phase-5 server boot. All-or-nothing per RFC-005 §D4: every readiness
/// failure is collected and the whole snapshot refused; no partial serving.
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

async fn read_snapshot_with_store(
    backend: ClusterStore,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let mut diagnostics: Vec<Diagnostic> = Vec::new();

    // A ledger a sweep is about to rewrite must not start serving.
    let sidecars = backend.list_recovery_sidecars(&mut diagnostics).await;
    if !sidecars.is_empty() {
        diagnostics.push(Diagnostic::error(
            "cluster_recovery_pending",
            CLUSTER_RECOVERIES_DIR,
            format!(
                "{} interrupted operation(s) await recovery; run any state-mutating cluster command (e.g. `cluster apply`) to sweep, then retry",
                sidecars.len()
            ),
        ));
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
        return Err(diagnostics);
    };

    let mut graphs = Vec::new();
    let mut queries = Vec::new();
    let mut policies = Vec::new();
    for (address, entry) in &state.applied_revision.resources {
        match resource_kind(address) {
            ResourceKind::Graph(graph_id) => {
                graphs.push(ServingGraph {
                    root: PathBuf::from(backend.graph_root(&graph_id)),
                    graph_id,
                });
            }
            ResourceKind::Schema(_) => {}
            kind @ ResourceKind::Query { .. } => {
                let ResourceKind::Query { graph, name } = &kind else {
                    unreachable!()
                };
                match backend.read_verified_payload(&kind, &entry.digest, address).await {
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
                match backend.read_verified_payload(&kind, &entry.digest, address).await {
                    Ok(source) => policies.push(ServingPolicy {
                        name: name.clone(),
                        source,
                        applies_to,
                    }),
                    Err(diagnostic) => diagnostics.push(diagnostic),
                }
            }
            ResourceKind::Unknown => {}
        }
    }

    if graphs.is_empty() {
        diagnostics.push(Diagnostic::error(
            "cluster_empty",
            CLUSTER_STATE_FILE,
            "the applied revision records no graphs; apply a cluster with at least one graph before serving from it",
        ));
    }
    if has_errors(&diagnostics) {
        return Err(diagnostics);
    }
    Ok(ServingSnapshot {
        graphs,
        queries,
        policies,
    })
}

