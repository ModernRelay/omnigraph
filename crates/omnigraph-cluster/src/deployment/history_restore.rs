//! Read-only online verification and settlement of a sealed first-graph restore.
use super::*;

pub(super) fn restore_directives(
    bundle: &DeploymentBundle,
) -> Result<BTreeMap<String, String>, Diagnostic> {
    let config: RawClusterConfig = serde_json::from_str(&bundle.config_semantics)
        .map_err(|error| refusal("deployment_input_invalid", error.to_string()))?;
    Ok(config
        .graphs
        .into_iter()
        .filter_map(|(name, graph)| graph.restore_from.map(|digest| (name, digest)))
        .collect())
}

pub(super) fn initial_restore_requests(
    state: &ClusterState,
    bundle: &DeploymentBundle,
    caller: &DeploymentCaller,
) -> Result<BTreeMap<String, String>, Diagnostic> {
    let mut requests = restore_directives(bundle)?;
    // Like a genesis declaration, this directive has no effect after publication.
    requests.retain(|graph, _| {
        !state
            .applied_revision
            .resources
            .contains_key(&graph_address(graph))
    });
    if requests.is_empty() {
        return Ok(requests);
    }
    if !matches!(caller, DeploymentCaller::AuthenticatedIdentity(identity) if !identity.has_bootstrap_authority())
    {
        return Err(refusal(
            "policy_denied",
            "restoring a graph requires an authenticated configuration manager under the applied cluster policy",
        ));
    }
    restore::fresh_restore_ledger(state)?;
    if bundle.graph_ids().len() != 1 {
        return Err(refusal(
            "restore_graph_count",
            "a full-history restore must declare exactly one graph",
        ));
    }
    Ok(requests)
}

pub(super) async fn verify_staged_restore_heads(
    store: &ClusterStore,
    bundle: &DeploymentBundle,
    graph: &str,
    digest: &str,
    ledger: &str,
    sequence: u64,
) -> Result<(VerifiedGraphRestore, u64), Diagnostic> {
    let receipt = store.read_sealed_graph_restore(digest).await?;
    receipt.validate_binding(bundle.canonical_root(), ledger, sequence, graph)?;
    let root = store.canonical_managed_graph_root(graph)?;
    Omnigraph::check_restore_birth_claim(&root, store.graph_storage(&root)?, digest)
        .await
        .map_err(|error| refusal("restore_birth_claim_mismatch", error.to_string()))?;
    Omnigraph::ensure_no_pending_recovery_with_storage(&root, store.graph_storage(&root)?)
        .await
        .map_err(|error| refusal("graph_recovery_required", error.to_string()))?;
    let db = Omnigraph::open_read_only_with_storage(&root, store.graph_storage(&root)?)
        .await
        .map_err(|error| refusal("graph_unavailable", error.to_string()))?;
    let contract = db.schema_contract_digest();
    // The storage owner seals only after independently verifying the complete
    // staged history and excluding further staging writes. The online path
    // binds that immutable proof to all current heads within the request
    // deadline; it must never scan old table images on a serving request.
    let branches = db
        .branch_list_detailed(&mut |name| receipt.history.branch_heads.contains_key(name))
        .await
        .map_err(|error| refusal("restore_history_invalid", error.to_string()))?;
    let observed_heads: BTreeMap<_, _> = branches
        .details
        .iter()
        .map(|branch| (branch.name.clone(), branch.head.graph_commit_id.clone()))
        .collect();
    if contract != receipt.history.schema
        || contract.source_hash != bundle.resources[&schema_address(graph)].digest
        || branches.branches.len() != receipt.history.branch_heads.len()
        || branches.details.len() != branches.branches.len()
        || observed_heads != receipt.history.branch_heads
    {
        return Err(refusal(
            "restore_history_mismatch",
            "the complete set of staged branch heads or schema differs from the sealed history and declared schema",
        ));
    }
    let main = branches
        .details
        .iter()
        .find(|branch| branch.name == "main")
        .ok_or_else(|| {
            refusal(
                "restore_history_mismatch",
                "the sealed main head is missing",
            )
        })?;
    Ok((
        VerifiedGraphRestore {
            receipt_sha256: digest.to_owned(),
            contract,
        },
        main.head.graph_manifest_version,
    ))
}

pub(super) async fn settle_history_restore(
    store: &ClusterStore,
    bundle: &DeploymentBundle,
    graph: &str,
    expected: &VerifiedGraphRestore,
    expected_version: u64,
    id: &str,
) -> Result<GraphDeploymentResult, Diagnostic> {
    let id = DeploymentId::parse(id)?;
    let (verified, version) = verify_staged_restore_heads(
        store,
        bundle,
        graph,
        &expected.receipt_sha256,
        &id.ledger,
        id.sequence,
    )
    .await?;
    if verified != *expected || version != expected_version {
        return Err(refusal(
            "deployment_outcome_unknown",
            "the staged graph changed after deployment acceptance",
        ));
    }
    Ok(GraphDeploymentResult::Restored {
        receipt_sha256: verified.receipt_sha256,
        contract: verified.contract,
        graph_manifest_version: version,
    })
}
