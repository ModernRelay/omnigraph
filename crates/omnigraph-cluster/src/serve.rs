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
    pub blob_path: PathBuf,
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
    let backend = ClusterStore::for_config_dir(&config_dir);
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
                    root: config_dir
                        .join(CLUSTER_GRAPHS_DIR)
                        .join(format!("{graph_id}.omni")),
                    graph_id,
                });
            }
            ResourceKind::Schema(_) => {}
            kind @ ResourceKind::Query { .. } => {
                let ResourceKind::Query { graph, name } = &kind else {
                    unreachable!()
                };
                match read_verified_payload(&config_dir, &kind, &entry.digest, address) {
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
                match read_verified_payload(&config_dir, &kind, &entry.digest, address) {
                    Ok(_) => policies.push(ServingPolicy {
                        name: name.clone(),
                        blob_path: payload_path(&config_dir, &kind, &entry.digest)
                            .expect("policy kind always has a payload path"),
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

/// Read a catalog blob and verify it against the recorded digest.
pub(crate) fn read_verified_payload(
    config_dir: &Path,
    kind: &ResourceKind,
    digest: &str,
    address: &str,
) -> Result<String, Diagnostic> {
    let path = payload_path(config_dir, kind, digest)
        .expect("query/policy kinds always have a payload path");
    let bytes = fs::read(&path).map_err(|err| {
        Diagnostic::error(
            "catalog_payload_missing",
            address,
            format!(
                "catalog blob '{}' unreadable ({err}); run `cluster refresh` then `cluster apply`, and restart",
                display_path(&path)
            ),
        )
    })?;
    if sha256_hex(&bytes) != digest {
        return Err(Diagnostic::error(
            "catalog_payload_digest_mismatch",
            address,
            format!(
                "catalog blob '{}' does not match its recorded digest; run `cluster refresh` then `cluster apply`, and restart",
                display_path(&path)
            ),
        ));
    }
    String::from_utf8(bytes).map_err(|err| {
        Diagnostic::error(
            "catalog_payload_invalid",
            address,
            format!("catalog blob is not valid UTF-8: {err}"),
        )
    })
}
