use super::*;
use omnigraph::db::RestoreHistoryWitness;
use omnigraph_storage::CapturedStorageBinding;

/// Shared upper bound on either exact descriptor object and the local input.
pub const MAX_RESTORE_DESCRIPTOR_BYTES: usize = 1024 * 1024;

/// Inspect a graph payload with only read access to this particular copy.
/// Storage rights for a different source must be excluded by the caller.
pub async fn verify_restore_history(
    binding: &CapturedStorageBinding,
    graph: &str,
) -> Result<RestoreHistoryWitness, Diagnostic> {
    let mut diagnostics = Vec::new();
    config::validate_id("graph", "graph", graph, &mut diagnostics);
    if let Some(error) = diagnostics.into_iter().next() {
        return Err(error);
    }
    if graph.starts_with(omnigraph_compiler::SYSTEM_GRAPH_ID_PREFIX) {
        return Err(deployment::refusal(
            "invalid_resource_id",
            "system graphs cannot be restored",
        ));
    }
    let store = ClusterStore::for_storage_binding(binding)?;
    let uri = store.graph_root(graph);
    let storage = store.graph_storage(&uri)?;
    let db = Omnigraph::open_read_only_with_storage(&uri, storage)
        .await
        .map_err(|err| deployment::refusal("restore_graph_unreadable", err.to_string()))?;
    db.verify_restored_history()
        .await
        .map_err(|err| deployment::refusal("restore_history_invalid", err.to_string()))
}

/// Storage-owner preflight and reservation only. Publication requires the
/// separately sealed payload and an authorized native configuration deployment.
pub async fn prepare_graph_restore(
    binding: &CapturedStorageBinding,
    descriptor_json: &str,
) -> Result<String, Diagnostic> {
    let receipt = PreparedGraphRestore::parse_exact(descriptor_json)?;
    let store = ClusterStore::for_storage_binding(binding)?;
    let before = store.read_state(&mut store.observations()).await?;
    let state = before
        .state
        .ok_or_else(|| deployment::refusal("state_missing", "bootstrap the destination first"))?;
    let ledger = restore::fresh_restore_ledger(&state)?;
    receipt.validate_binding(&store.canonical_root()?, ledger, 2, &receipt.graph)?;
    let digest = sha256_hex(descriptor_json.as_bytes());
    let graph_uri = store.graph_root(&receipt.graph);
    let adapter = store.graph_storage(&graph_uri)?;
    if store
        .has_restore_reservation(&digest, descriptor_json)
        .await?
    {
        Omnigraph::check_restore_birth_claim(&graph_uri, adapter, &digest).await
    } else {
        Omnigraph::reserve_restore_birth(&graph_uri, adapter, &digest).await
    }
    .map_err(|err| deployment::refusal("restore_birth_claim_failed", err.to_string()))?;

    let after = store.read_state(&mut store.observations()).await?;
    if before.state_cas.is_none() || before.state_cas != after.state_cas {
        return Err(deployment::refusal(
            "restore_destination_changed",
            "destination ledger changed during reservation",
        ));
    }
    store
        .write_content_addressed(
            &format!("__cluster/restore-reservations/{digest}.json"),
            &digest,
            descriptor_json,
            MAX_RESTORE_DESCRIPTOR_BYTES,
        )
        .await
        .map_err(|err| deployment::refusal("restore_reservation_write", err))?;
    Ok(digest)
}
