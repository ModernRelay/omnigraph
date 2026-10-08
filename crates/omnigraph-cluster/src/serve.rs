//! Phase-5 serving snapshot: the read-only loader a `--cluster` server
//! boots from (moved verbatim from lib.rs in the modularization).

use super::*;
use crate::config::storage_root_conflict_code;

/// One graph in a serving snapshot: its id and on-disk root.
#[derive(Debug, Clone)]
pub struct ServingGraph {
    pub graph_id: String,
    pub root: PathBuf,
    pub embedding: Option<EmbeddingProviderConfig>,
    /// Full normalized applied policy. The server projects this exactly once,
    /// immediately before installing it on the engine handle.
    pub external_blob_policy: omnigraph::ExternalBlobPolicy,
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

/// An applied graph refused by snapshot safety checks. Preserve its exact
/// configured storage root without loading rejected policies or graph data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServingBlockedGraph {
    pub graph_id: String,
    pub root: PathBuf,
}

/// Everything a server needs to boot from the cluster catalog (RFC-005 §D2).
#[derive(Debug, Clone)]
pub struct ServingSnapshot {
    pub graphs: Vec<ServingGraph>,
    pub queries: Vec<ServingQuery>,
    pub policies: Vec<ServingPolicy>,
    pub diagnostics: Vec<Diagnostic>,
    /// The applied revision's `config_digest`: what a server booted from
    /// reports as its `booted_serving_digest` (RFC 0049).
    pub config_digest: Option<String>,
    /// The ledger revision and CAS this snapshot was read from.
    pub state_revision: u64,
    pub state_cas: Option<String>,
    /// Every graph the applied revision names, sorted.
    pub applied_graphs: Vec<String>,
    /// Applied graphs refused by legacy recovery-evidence or external Blob
    /// safety checks, sorted. Evidence for an unregistered graph never adds
    /// that graph to the applied inventory.
    pub quarantined_graphs: Vec<ServingBlockedGraph>,
}

/// A serving snapshot paired with the canonical root of the same opened store.
///
/// The binding is read-only metadata for managed boot trust, not a writer fence
/// or a guarantee that the ledger has not changed since this snapshot was read.
/// Only the root-bound readers can construct this pair.
#[derive(Debug, Clone)]
pub struct RootBoundServingSnapshot {
    snapshot: ServingSnapshot,
    canonical_root: String,
}

impl RootBoundServingSnapshot {
    pub fn snapshot(&self) -> &ServingSnapshot {
        &self.snapshot
    }

    pub fn canonical_root(&self) -> &str {
        &self.canonical_root
    }

    /// Discard the root binding and return the legacy snapshot projection.
    pub fn into_snapshot(self) -> ServingSnapshot {
        self.snapshot
    }
}

/// Serving input captured while holding the v2 cluster's lifetime admission.
/// Legacy ledgers must be explicitly converted before ordinary serving.
#[derive(Debug, Clone)]
pub struct AdmittedServingSnapshot {
    snapshot: ServingSnapshot,
    canonical_root: String,
    admission: Option<crate::admission::ClusterAdmission>,
}

impl AdmittedServingSnapshot {
    pub(crate) fn from_bootstrap(
        snapshot: ServingSnapshot,
        canonical_root: String,
        admission: crate::admission::ClusterAdmission,
    ) -> Self {
        Self {
            snapshot,
            canonical_root,
            admission: Some(admission),
        }
    }

    pub fn canonical_root(&self) -> &str {
        &self.canonical_root
    }

    pub fn into_parts(
        self,
    ) -> (
        ServingSnapshot,
        String,
        Option<crate::admission::ClusterAdmission>,
    ) {
        (self.snapshot, self.canonical_root, self.admission)
    }
}

/// Acquire v2 admission before capturing the applied serving input. The caller
/// must retain the admission through graph opening and the server's lifetime.
/// A bare path resolves `cluster.yaml`'s storage root; a URI is config-free.
pub async fn admit_serving_snapshot(
    cluster: &str,
) -> Result<AdmittedServingSnapshot, Vec<Diagnostic>> {
    let store = if cluster.contains("://") {
        ClusterStore::for_storage_root(cluster).map_err(|diagnostic| vec![diagnostic])?
    } else {
        store_for_serving_snapshot(Path::new(cluster))?
    };
    let store = store
        .with_io_scope(omnigraph_storage::StorageIoScope::new())
        .map_err(|diagnostic| vec![diagnostic])?;
    let admission = crate::admission::acquire_with_store(
        &store,
        crate::admission::ClusterAdmissionPurpose::Serve,
    )
    .await
    .map_err(|diagnostic| vec![diagnostic])?;
    // Snapshot capture only reads verified control payloads; no graph engine
    // or write-capable owner exists yet. Completed refusal can release safely.
    let captured = async {
        let snapshot = read_snapshot_with_store(&store).await?;
        let canonical_root = store
            .canonical_root()
            .map_err(|diagnostic| vec![diagnostic])?;
        Ok::<_, Vec<Diagnostic>>((snapshot, canonical_root))
    }
    .await;
    let (snapshot, canonical_root) = match captured {
        Ok(captured) => captured,
        Err(mut diagnostics) => {
            if let Some(owner) = admission {
                let error = diagnostics.remove(0);
                diagnostics.insert(0, owner.release_refused_preflight(error).await);
            }
            return Err(diagnostics);
        }
    };
    Ok(AdmittedServingSnapshot {
        snapshot,
        canonical_root,
        admission,
    })
}

/// Acquire lifetime admission for a server config directory or storage URI.
/// This resolves the root only; serving input must be captured under the guard.
pub async fn acquire_serving_admission(
    cluster: &str,
) -> Result<Option<crate::admission::ClusterAdmission>, Vec<Diagnostic>> {
    let store = if cluster.contains("://") {
        ClusterStore::for_storage_root(cluster).map_err(|diagnostic| vec![diagnostic])?
    } else {
        store_for_serving_snapshot(Path::new(cluster))?
    };
    let store = store
        .with_io_scope(omnigraph_storage::StorageIoScope::new())
        .map_err(|diagnostic| vec![diagnostic])?;
    crate::admission::acquire_with_store(&store, crate::admission::ClusterAdmissionPurpose::Serve)
        .await
        .map_err(|diagnostic| vec![diagnostic])
}

/// Read the applied revision as a serving snapshot — the read-only loader for
/// server boot. Cluster-global readiness failures are all-or-nothing;
/// graph-attributed legacy evidence or unsafe Blob bindings quarantine their
/// graphs. This loader never runs recovery or converts a legacy ledger.
/// Takes no lock: the state file is replaced atomically, so this reads a
/// consistent point-in-time ledger.
pub async fn read_serving_snapshot(
    config_dir: impl AsRef<Path>,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let backend = store_for_serving_snapshot(config_dir.as_ref())?;
    read_snapshot_with_store(&backend).await
}

fn store_for_serving_snapshot(config_dir: &Path) -> Result<ClusterStore, Vec<Diagnostic>> {
    // The declared storage: root decides where the ledger/catalog/graphs
    // live; config parse errors surface through the normal validation path.
    let parsed = parse_cluster_config(config_dir);
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
        None => ClusterStore::for_config_dir(config_dir),
    };
    Ok(backend)
}

/// Read the applied revision directly from a storage root URI — config-free
/// serving: a `--cluster s3://bucket/prefix` or
/// `--cluster az://container/prefix` server needs no local files at all, only
/// the object-store location and credentials. The ledger and catalog ARE the
/// deployment artifact.
pub async fn read_serving_snapshot_from_storage(
    storage_root: &str,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let backend =
        ClusterStore::for_storage_root(storage_root).map_err(|diagnostic| vec![diagnostic])?;
    read_snapshot_with_store(&backend).await
}

/// Project only deployment-affected graphs and their verified runtime payloads.
/// The ledger and cluster management policy remain authoritative in full;
/// unrelated graph payload availability cannot veto an independent cutover.
pub async fn read_deployment_serving_snapshot(
    storage_root: &str,
    affected: &[String],
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let backend =
        ClusterStore::for_storage_root(storage_root).map_err(|diagnostic| vec![diagnostic])?;
    read_snapshot_impl(&backend, false, None, Some(affected), None).await
}

/// Test support: read a local cluster's serving snapshot through the
/// production reader while its storage root reads as `display_root` (see
/// `ClusterStore::with_display_root`). Graph roots derive from it too.
#[cfg(any(test, feature = "test-util"))]
pub async fn read_serving_snapshot_with_display_root(
    config_dir: impl AsRef<Path>,
    display_root: &str,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let backend = ClusterStore::for_config_dir(config_dir.as_ref()).with_display_root(display_root);
    read_snapshot_with_store(&backend).await
}

/// Read an applied snapshot and its canonical store root for managed boot trust.
/// Ordinary snapshot reads do not perform this extra canonicalization step.
pub async fn read_root_bound_serving_snapshot(
    config_dir: impl AsRef<Path>,
) -> Result<RootBoundServingSnapshot, Vec<Diagnostic>> {
    let backend = store_for_serving_snapshot(config_dir.as_ref())?;
    read_root_bound_snapshot_with_store(&backend).await
}

/// Read a root-bound applied snapshot directly from its storage URI.
pub async fn read_root_bound_serving_snapshot_from_storage(
    storage_root: &str,
) -> Result<RootBoundServingSnapshot, Vec<Diagnostic>> {
    let backend =
        ClusterStore::for_storage_root(storage_root).map_err(|diagnostic| vec![diagnostic])?;
    read_root_bound_snapshot_with_store(&backend).await
}

async fn read_root_bound_snapshot_with_store(
    backend: &ClusterStore,
) -> Result<RootBoundServingSnapshot, Vec<Diagnostic>> {
    let snapshot = read_snapshot_with_store(backend).await?;
    let canonical_root = backend
        .canonical_root()
        .map_err(|diagnostic| vec![diagnostic])?;
    Ok(RootBoundServingSnapshot {
        snapshot,
        canonical_root,
    })
}

/// Cluster root for a graph **storage URI** of the cluster layout
/// (`<root>/graphs/<id>.omni`), if `<root>` is actually a cluster (holds
/// `__cluster/state.json`); otherwise `None`. Used by the CLI to refuse
/// `init` into a cluster-managed location — graphs there are created by
/// `cluster apply`, not `init`.
///
/// Local aliases are resolved before testing the layout. Non-cluster-shaped
/// remote URIs never probe storage. Works for `file://`, `s3://`, and `az://`.
pub async fn cluster_root_for_graph_uri(graph_uri: &str) -> Result<Option<String>, Diagnostic> {
    // Resolve aliases before deciding that a graph is standalone. A symlink to
    // a managed graph can have any filename. Also inspect the lexical layout
    // so a managed graph symlink escaping the root is refused by v2 admission.
    let canonical = crate::admission::canonical_graph_uri(graph_uri)?;
    let lexical = omnigraph_storage::normalize_root_uri(graph_uri).map_err(|error| {
        Diagnostic::error("cluster_graph_uri_error", "graph", error.to_string())
    })?;
    let mut roots = BTreeSet::new();
    for uri in [&canonical, &lexical] {
        let Some(root) = cluster_root_of_graph_layout(uri) else {
            continue;
        };
        if !roots.insert(root.clone()) {
            continue;
        }
        let store = ClusterStore::for_storage_root(&root)?;
        let has_state = store.has_state().await.map_err(|error| {
            Diagnostic::error(
                "cluster_state_probe_error",
                omnigraph_storage::redacted_storage_uri(&root),
                format!("could not inspect cluster state: {error}"),
            )
        })?;
        if has_state {
            return Ok(Some(store.display_root().to_string()));
        }
    }
    Ok(None)
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
/// `cluster` is a config directory or a storage-root URI (`s3://…` or
/// `az://…`, config-free), mirroring the server's `--cluster` dispatch.
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
    if state.version != 2 {
        return Err(Diagnostic::error(
            "ledger_upgrade_required",
            CLUSTER_STATE_FILE,
            "convert the stopped cluster ledger to v2 before using its graphs",
        ));
    }
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
    if state.version != 2 {
        return Err(Diagnostic::error(
            "ledger_upgrade_required",
            CLUSTER_STATE_FILE,
            "convert the stopped cluster ledger to v2 before using its graphs",
        ));
    }
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

pub(crate) async fn read_snapshot_with_store(
    backend: &ClusterStore,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    read_snapshot_impl(backend, false, None, None, None).await
}

/// Decode and validate legacy applied facts only for stopped-writer conversion.
/// This is never authority to serve a v1 ledger or execute a v1 operation.
pub(crate) async fn read_snapshot_for_ledger_upgrade(
    backend: &ClusterStore,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    read_snapshot_impl(backend, true, None, None, None).await
}

/// Use the ordinary serving projector with already validated frozen candidate
/// resources. No candidate state or payload is written to storage.
pub(crate) async fn preview_snapshot_with_store(
    backend: &ClusterStore,
    candidate: &crate::CapturedDeployment,
    affected: &[String],
    state: ClusterState,
    state_cas: String,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    read_snapshot_impl(
        backend,
        false,
        Some(candidate),
        Some(affected),
        Some((state, state_cas)),
    )
    .await
}

async fn serving_payload(
    backend: &ClusterStore,
    candidate: Option<&crate::CapturedDeployment>,
    kind: &ResourceKind,
    digest: &str,
    address: &str,
) -> Result<String, Diagnostic> {
    if let Some(candidate) = candidate {
        return candidate.sources.get(digest).cloned().ok_or_else(|| {
            Diagnostic::error(
                "deployment_input_invalid",
                address,
                "validated candidate payload is missing",
            )
        });
    }
    backend.read_verified_payload(kind, digest, address).await
}

async fn read_snapshot_impl(
    backend: &ClusterStore,
    legacy_conversion: bool,
    candidate: Option<&crate::CapturedDeployment>,
    affected: Option<&[String]>,
    captured: Option<(ClusterState, String)>,
) -> Result<ServingSnapshot, Vec<Diagnostic>> {
    let selected = |graph: &str| affected.is_none_or(|graphs| graphs.iter().any(|id| id == graph));
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
                "legacy recovery evidence has no graph id; resolve it with the build that wrote it before ledger conversion",
            ));
            continue;
        }
        quarantined_graphs.insert(sidecar.graph_id.clone());
        startup_diagnostics.push(Diagnostic::warning(
            "cluster_recovery_pending",
            graph_address(&sidecar.graph_id),
            format!(
                "graph `{}` is quarantined because legacy interrupted operation `{}` awaits recovery; resolve it with the build that wrote the evidence",
                sidecar.graph_id, sidecar.operation_id
            ),
        ));
    }
    if has_errors(&diagnostics) {
        return Err(diagnostics);
    }

    let mut observations = backend.observations();
    let state = if let Some((state, cas)) = captured {
        observations.state_cas = Some(cas);
        Some(state)
    } else {
        match backend.read_state(&mut observations).await {
            Ok(snapshot) => match snapshot.state {
                Some(state) => Some(state),
                None => {
                    diagnostics.push(Diagnostic::error(
                        "cluster_state_missing",
                        CLUSTER_STATE_FILE,
                        "no cluster state ledger; run `cluster apply` first",
                    ));
                    None
                }
            },
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                None
            }
        }
    };
    let Some(mut state) = state else {
        diagnostics.extend(startup_diagnostics);
        return Err(diagnostics);
    };
    if state.version != 2 && !legacy_conversion {
        return Err(vec![Diagnostic::error(
            "ledger_upgrade_required",
            CLUSTER_STATE_FILE,
            "convert the stopped cluster ledger to v2 before serving",
        )]);
    }
    if let Some(candidate) = candidate {
        // Read-only preflight projects the candidate through the same checks;
        // revision/CAS still identify the captured achieved base, not approval.
        state.applied_revision.resources = candidate.resources.clone();
        state.applied_revision.config_digest = Some(candidate.config_digest.clone());
    }
    let boot_config_digest = state.applied_revision.config_digest.clone();
    let boot_state_revision = state.state_revision;
    let boot_state_cas = observations.state_cas.clone();
    let boot_applied_graphs: Vec<_> = applied_graph_ids(&state)
        .into_iter()
        .filter(|id| selected(id))
        .collect();
    let recovery_pending = boot_applied_graphs
        .iter()
        .any(|graph_id| quarantined_graphs.contains(graph_id));
    for (graph_id, conflict) in
        overlapping_served_external_blob_policies(&state, backend.display_root())
    {
        if !selected(&graph_id) {
            continue;
        }
        quarantined_graphs.insert(graph_id.clone());
        let remedy = match conflict {
            omnigraph::StorageRootConflict::UncomparableRoot { .. } => {
                "serving cannot prove the base lies outside the cluster storage root; remove the graph's server-safe bases with `cluster apply`, or serve from a storage root without empty, dot or percent-encoded path components, and restart"
            }
            _ => {
                "move the base to a prefix outside the cluster storage root, run `cluster apply`, and restart"
            }
        };
        startup_diagnostics.push(Diagnostic::warning(
            storage_root_conflict_code(&conflict),
            graph_address(&graph_id),
            format!(
                "graph `{graph_id}` is quarantined because its applied external Blob policy is unsafe to serve: {conflict}; {remedy}"
            ),
        ));
    }

    let required_embedding_providers: BTreeSet<String> = state
        .applied_revision
        .resources
        .iter()
        .filter_map(|(address, entry)| match resource_kind(address) {
            ResourceKind::Graph(graph_id)
                if selected(&graph_id) && !quarantined_graphs.contains(&graph_id) =>
            {
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
                    "applied embedding provider profile does not match its recorded digest (actual sha256:{actual_digest}); restore the verified provider metadata from a trusted ledger before serving"
                ),
            ));
            continue;
        }
        embedding_profiles.insert(address.to_owned(), profile);
    }

    let mut graphs = Vec::new();
    let mut queries = Vec::new();
    let mut policies = Vec::new();
    let mut saw_applied_graph = false;
    for (address, entry) in &state.applied_revision.resources {
        match resource_kind(address) {
            ResourceKind::Graph(graph_id) => {
                if !selected(&graph_id) {
                    continue;
                }
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
                let external_blob_policy = entry.external_blob_policy.clone().unwrap_or_default();
                let expected_digest =
                    expected_state_graph_resource_digest(&state, &graph_id, entry);
                if expected_digest != entry.digest {
                    diagnostics.push(Diagnostic::error(
                        "external_blob_policy_digest_mismatch",
                        address.clone(),
                        "the applied graph resource metadata is not bound by its composite digest; restore the cluster state ledger from a trusted copy before retrying",
                    ));
                    continue;
                }
                let graph_root = backend.graph_root(&graph_id);
                graphs.push(ServingGraph {
                    root: PathBuf::from(graph_root),
                    graph_id,
                    embedding,
                    external_blob_policy,
                });
            }
            ResourceKind::Schema(_) => {}
            kind @ ResourceKind::Query { .. } => {
                let ResourceKind::Query { graph, name } = &kind else {
                    unreachable!()
                };
                if !selected(graph) || quarantined_graphs.contains(graph) {
                    continue;
                }
                match serving_payload(backend, candidate, &kind, &entry.digest, address).await {
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
                        binding.strip_prefix("graph.").is_none_or(|graph| {
                            selected(graph) && !quarantined_graphs.contains(graph)
                        })
                    })
                    .collect();
                if applies_to.is_empty() {
                    continue;
                }
                match serving_payload(backend, candidate, &kind, &entry.digest, address).await {
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

    if graphs.is_empty() && affected.is_none() {
        if saw_applied_graph {
            diagnostics.push(Diagnostic::error(
                "cluster_no_healthy_graphs",
                if recovery_pending {
                    CLUSTER_RECOVERIES_DIR
                } else {
                    CLUSTER_STATE_FILE
                },
                "all applied graphs are quarantined by startup safety checks; resolve the graph-specific diagnostics, then retry",
            ));
        } else if boot_state_revision == 0
            || boot_state_cas.is_none()
            || !boot_config_digest.as_deref().is_some_and(|digest| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        {
            diagnostics.push(Diagnostic::error(
                "cluster_empty",
                CLUSTER_STATE_FILE,
                "an empty cluster requires an applied configuration digest, a positive state revision and an observed ledger CAS; run `cluster apply` before serving",
            ));
        } else if let Err(diagnostic) = backend.canonical_root() {
            // Empty serving still needs an actual storage root. Unlike a
            // nonempty revision, it will not open a graph to check one later.
            diagnostics.push(diagnostic);
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
        config_digest: boot_config_digest,
        state_revision: boot_state_revision,
        state_cas: boot_state_cas,
        quarantined_graphs: quarantined_graphs
            .into_iter()
            .filter(|graph_id| boot_applied_graphs.contains(graph_id))
            .map(|graph_id| ServingBlockedGraph {
                root: PathBuf::from(backend.graph_root(&graph_id)),
                graph_id,
            })
            .collect(),
        applied_graphs: boot_applied_graphs,
    })
}

/// Applied graphs whose server-safe external Blob bases overlap, or cannot be
/// compared with, the cluster storage root, with the conflict. The ledger is
/// checked as read, not trusted to have passed `cluster validate`. A policy
/// that does not project or validate is left to the server's install to
/// refuse, and an entry whose composite digest does not bind its policy to the
/// boot-fatal `external_blob_policy_digest_mismatch` check.
pub(crate) fn overlapping_served_external_blob_policies(
    state: &ClusterState,
    storage_root: &str,
) -> Vec<(String, omnigraph::StorageRootConflict)> {
    state
        .applied_revision
        .resources
        .iter()
        .filter_map(|(address, entry)| {
            let ResourceKind::Graph(graph_id) = resource_kind(address) else {
                return None;
            };
            if expected_state_graph_resource_digest(state, &graph_id, entry) != entry.digest {
                return None;
            }
            let policy = entry
                .external_blob_policy
                .clone()
                .unwrap_or_default()
                .server_safe_only()
                .ok()?;
            match policy.ensure_disjoint_from_storage_root(storage_root) {
                Ok(()) | Err(omnigraph::StorageRootConflict::InvalidPolicy(_)) => None,
                Err(conflict) => Some((graph_id, conflict)),
            }
        })
        .collect()
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
        assert_eq!(
            cluster_root_of_graph_layout("az://container/prefix/graphs/kb.omni").as_deref(),
            Some("az://container/prefix")
        );
        assert_eq!(cluster_root_of_graph_layout("./kb.omni"), None);
        assert_eq!(cluster_root_of_graph_layout("s3://bucket/kb.omni"), None);
        assert_eq!(cluster_root_of_graph_layout("az://container/kb.omni"), None);
        // nested id under graphs/ is not the cluster layout
        assert_eq!(cluster_root_of_graph_layout("/c/graphs/a/b.omni"), None);
        // not a .omni graph
        assert_eq!(cluster_root_of_graph_layout("/c/graphs/kb"), None);
    }

    #[tokio::test]
    async fn config_free_serving_rejects_unknown_storage_schemes() {
        let diagnostics = read_serving_snapshot_from_storage(
            "https://account.blob.core.windows.net/container/cluster",
        )
        .await
        .expect_err("an Azure HTTPS alias must not fall through to local storage");
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "storage_root_invalid"),
            "{diagnostics:?}"
        );
    }

    #[tokio::test]
    async fn cluster_root_detected_only_when_state_ledger_present() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        std::fs::create_dir_all(root.join("graphs")).unwrap();
        let graph_uri = format!("{}/graphs/kb.omni", root.to_string_lossy());

        // No __cluster/state.json yet → not a cluster.
        assert_eq!(cluster_root_for_graph_uri(&graph_uri).await.unwrap(), None);

        // Lay down the state ledger → now it's a cluster-managed location.
        std::fs::create_dir_all(root.join("__cluster")).unwrap();
        std::fs::write(root.join(CLUSTER_STATE_FILE), "{}").unwrap();
        let detected = cluster_root_for_graph_uri(&graph_uri).await.unwrap();
        assert!(detected.is_some(), "expected cluster root to be detected");

        // A non-cluster-shaped target never probes and is always None.
        assert_eq!(
            cluster_root_for_graph_uri(&format!("{}/plain.omni", root.to_string_lossy()))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn cluster_root_probe_rejects_unsupported_shaped_uri() {
        let error = cluster_root_for_graph_uri(
            "https://account.blob.core.windows.net/container/graphs/kb.omni",
        )
        .await
        .expect_err("an unsupported storage scheme must not mean not-a-cluster");
        assert_eq!(error.code, "storage_root_invalid");
    }

    #[tokio::test]
    async fn cluster_root_probe_keeps_local_enotdir_loud() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::write(temp.path().join("__cluster"), "not a directory").unwrap();
        let graph_uri = format!("{}/graphs/kb.omni", temp.path().to_string_lossy());

        let error = cluster_root_for_graph_uri(&graph_uri)
            .await
            .expect_err("an inconclusive state-ledger probe must not mean not-a-cluster");
        assert_eq!(error.code, "cluster_state_probe_error");
    }
}
