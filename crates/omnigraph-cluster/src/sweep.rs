//! The recovery sweep: RFC-004's roll-forward-only sidecar
//! classification (moved verbatim from lib.rs in the modularization).

use super::*;

/// Recovery sweep (RFC-004 §D3): runs at the start of every state-mutating
/// cluster command, under the state lock, before the command's own work.
/// Roll-forward-only — the engine's own sidecars make each graph-level
/// operation atomic within the graph, so the cluster never rolls a graph
/// back; it converges the ledger to observable reality or refuses loudly.
/// Mutations ride the calling command's CAS-checked state write; completed
/// sidecars are deleted only after that write lands.
pub(crate) async fn sweep_recovery_sidecars(
    backend: &ClusterStore,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
) -> SweepOutcome {
    let mut outcome = SweepOutcome::default();
    for (path, sidecar) in backend.list_recovery_sidecars(diagnostics).await {
        match sidecar.kind {
            RecoverySidecarKind::GraphCreate => {
                sweep_graph_create_sidecar(
                    backend,
                    path,
                    sidecar,
                    state,
                    diagnostics,
                    &mut outcome,
                )
                .await;
            }
            RecoverySidecarKind::SchemaApply => {
                sweep_schema_apply_sidecar(path, sidecar, state, diagnostics, &mut outcome).await;
            }
            RecoverySidecarKind::GraphDelete => {
                sweep_graph_delete_sidecar(
                    backend,
                    path,
                    sidecar,
                    state,
                    diagnostics,
                    &mut outcome,
                )
                .await;
            }
        }
    }
    outcome
}

pub(crate) async fn sweep_graph_create_sidecar(
    backend: &ClusterStore,
    path: String,
    sidecar: RecoverySidecar,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
    outcome: &mut SweepOutcome,
) {
    let graph_address = graph_address(&sidecar.graph_id);
    let schema_addr = schema_address(&sidecar.graph_id);

    // Row 1: nothing moved — the init never landed. The sidecar is pure
    // intent; retire it (deferred to the command's post-CAS cleanup, like
    // every other completed sidecar — a failed CAS simply re-sweeps it) and
    // let the command's own plan re-propose the create.
    if !backend.graph_root_exists(&sidecar.graph_uri).await {
        outcome.completed_sidecars.push(path);
        return;
    }

    match Omnigraph::open_read_only(&sidecar.graph_uri).await {
        Ok(db) => {
            let live_digest = sha256_hex(db.schema_source().as_bytes());
            let recorded = state
                .applied_revision
                .resources
                .get(&schema_addr)
                .map(|resource| resource.digest.clone());
            if recorded.as_deref() == Some(live_digest.as_str()) {
                // Row 2: crash fell between the state CAS and sidecar delete.
                outcome.completed_sidecars.push(path);
            } else if live_digest == sidecar.desired_schema_digest {
                // Row 4: the create completed on the graph; roll the cluster
                // state forward to observable reality.
                state.applied_revision.resources.insert(
                    schema_addr.clone(),
                    StateResource {
                        digest: live_digest.clone(),
                        applies_to: None,
                        embedding_provider: None,
                        embedding_profile: None,
                    },
                );
                let query_digests = state_query_digests_for_graph(state, &sidecar.graph_id);
                let embedding_provider = state_graph_embedding_provider(state, &sidecar.graph_id);
                let embedding_provider_digest =
                    state_embedding_provider_digest(state, embedding_provider.as_deref());
                let composite = graph_digest(
                    &sidecar.graph_id,
                    Some(&live_digest),
                    Some(&query_digests),
                    embedding_provider.as_deref(),
                    embedding_provider_digest.as_ref(),
                );
                state.applied_revision.resources.insert(
                    graph_address.clone(),
                    StateResource {
                        digest: composite,
                        applies_to: None,
                        embedding_provider,
                        embedding_profile: None,
                    },
                );
                set_resource_status_applied(state, &graph_address);
                set_resource_status_applied(state, &schema_addr);
                state.recovery_records.insert(
                    sidecar.operation_id.clone(),
                    json!({
                        "kind": "graph_create",
                        "graph_id": sidecar.graph_id,
                        "outcome": "rolled_forward",
                        "recovered_at": now_rfc3339(),
                        "actor": sidecar.actor,
                    }),
                );
                diagnostics.push(Diagnostic::warning(
                    "cluster_recovery_rolled_forward",
                    graph_address.clone(),
                    "an interrupted graph create had completed on the graph; cluster state was rolled forward to match",
                ));
                outcome.completed_sidecars.push(path);
            } else {
                // Row 6: the graph moved to something the sidecar did not
                // intend. Refuse to guess; require refresh + operator re-plan.
                set_resource_status(
                    state,
                    &graph_address,
                    ResourceLifecycleStatus::Drifted,
                    "actual_applied_state_pending",
                    "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
                );
                set_resource_status(
                    state,
                    &schema_addr,
                    ResourceLifecycleStatus::Drifted,
                    "actual_applied_state_pending",
                    "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
                );
                diagnostics.push(Diagnostic::warning(
                    "cluster_recovery_pending",
                    graph_address.clone(),
                    "an interrupted graph create left unexpected graph state; graph-moving work is blocked until repaired",
                ));
                outcome.pending_graphs.insert(sidecar.graph_id.clone());
            }
        }
        Err(err) => {
            // Row 5: partial root (the engine's documented init gap). Never
            // auto-delete — reconciler deletes are the same data-loss class
            // as human deletes; the operator removes the root explicitly.
            set_resource_status(
                state,
                &graph_address,
                ResourceLifecycleStatus::Error,
                "graph_create_incomplete",
                "graph root exists but cannot be opened; remove the graph root and re-run `cluster apply`",
            );
            set_resource_status(
                state,
                &schema_addr,
                ResourceLifecycleStatus::Error,
                "graph_create_incomplete",
                "graph root exists but cannot be opened; remove the graph root and re-run `cluster apply`",
            );
            diagnostics.push(Diagnostic::error(
                "graph_create_incomplete",
                graph_address.clone(),
                format!(
                    "graph root '{}' exists but cannot be opened ({err}); remove the graph root and re-run `cluster apply`",
                    sidecar.graph_uri
                ),
            ));
            outcome.pending_graphs.insert(sidecar.graph_id.clone());
        }
    }
}

pub(crate) async fn sweep_schema_apply_sidecar(
    path: String,
    sidecar: RecoverySidecar,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
    outcome: &mut SweepOutcome,
) {
    let graph_address = graph_address(&sidecar.graph_id);
    let schema_addr = schema_address(&sidecar.graph_id);

    // Digest-based classification: robust to unrelated manifest movement;
    // the sidecar's version pins stay forensic.
    let live_digest = match Omnigraph::open_read_only(&sidecar.graph_uri).await {
        Ok(db) => sha256_hex(db.schema_source().as_bytes()),
        Err(err) => {
            // Cannot verify the interrupted operation — refuse to guess.
            diagnostics.push(Diagnostic::warning(
                "cluster_recovery_pending",
                graph_address.clone(),
                format!(
                    "an interrupted schema apply cannot be verified (graph '{}' did not open: {err}); graph-moving work is blocked until repaired",
                    sidecar.graph_uri
                ),
            ));
            outcome.pending_graphs.insert(sidecar.graph_id.clone());
            return;
        }
    };

    let recorded = state
        .applied_revision
        .resources
        .get(&schema_addr)
        .map(|resource| resource.digest.clone());
    if recorded.as_deref() == Some(live_digest.as_str()) {
        // Ledger consistent with the live graph (the apply never landed, or
        // landed and was recorded): the sidecar is stale intent — retire it.
        outcome.completed_sidecars.push(path);
    } else if live_digest == sidecar.desired_schema_digest {
        // RFC-004 §D3 row 3: the schema apply completed on the graph; roll
        // the cluster state forward to observable reality.
        state.applied_revision.resources.insert(
            schema_addr.clone(),
            StateResource {
                digest: live_digest.clone(),
                applies_to: None,
                embedding_provider: None,
                embedding_profile: None,
            },
        );
        let query_digests = state_query_digests_for_graph(state, &sidecar.graph_id);
        let embedding_provider = state_graph_embedding_provider(state, &sidecar.graph_id);
        let embedding_provider_digest =
            state_embedding_provider_digest(state, embedding_provider.as_deref());
        let composite = graph_digest(
            &sidecar.graph_id,
            Some(&live_digest),
            Some(&query_digests),
            embedding_provider.as_deref(),
            embedding_provider_digest.as_ref(),
        );
        state.applied_revision.resources.insert(
            graph_address.clone(),
            StateResource {
                digest: composite,
                applies_to: None,
                embedding_provider,
                embedding_profile: None,
            },
        );
        set_resource_status_applied(state, &graph_address);
        set_resource_status_applied(state, &schema_addr);
        state.recovery_records.insert(
            sidecar.operation_id.clone(),
            json!({
                "kind": "schema_apply",
                "graph_id": sidecar.graph_id,
                "outcome": "rolled_forward",
                "recovered_at": now_rfc3339(),
                "actor": sidecar.actor,
            }),
        );
        diagnostics.push(Diagnostic::warning(
            "cluster_recovery_rolled_forward",
            graph_address.clone(),
            "an interrupted schema apply had completed on the graph; cluster state was rolled forward to match",
        ));
        outcome.completed_sidecars.push(path);
    } else {
        // Row 6: live schema is neither the recorded nor the desired digest.
        set_resource_status(
            state,
            &graph_address,
            ResourceLifecycleStatus::Drifted,
            "actual_applied_state_pending",
            "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
        );
        set_resource_status(
            state,
            &schema_addr,
            ResourceLifecycleStatus::Drifted,
            "actual_applied_state_pending",
            "graph state does not match the interrupted operation; run `cluster refresh` and re-plan",
        );
        diagnostics.push(Diagnostic::warning(
            "cluster_recovery_pending",
            graph_address.clone(),
            "an interrupted schema apply left unexpected graph state; graph-moving work is blocked until repaired",
        ));
        outcome.pending_graphs.insert(sidecar.graph_id.clone());
    }
}

pub(crate) async fn sweep_graph_delete_sidecar(
    backend: &ClusterStore,
    path: String,
    sidecar: RecoverySidecar,
    state: &mut ClusterState,
    diagnostics: &mut Vec<Diagnostic>,
    outcome: &mut SweepOutcome,
) {
    let graph_address = graph_address(&sidecar.graph_id);

    if backend.graph_root_exists(&sidecar.graph_uri).await {
        // Row 8: the delete never completed. Prefix removal is idempotent and
        // works on partial roots, so the repair is simply the re-proposed,
        // still-approved delete on a later run — retire the stale intent.
        diagnostics.push(Diagnostic::warning(
            "graph_delete_incomplete",
            graph_address,
            "a previous graph delete did not complete; it will be re-proposed by plan and can be retried under its approval",
        ));
        outcome.completed_sidecars.push(path);
        return;
    }

    if !state
        .applied_revision
        .resources
        .contains_key(&graph_address)
    {
        // Row 7: already tombstoned (or never recorded); crash fell between
        // the state CAS and sidecar delete.
        outcome.completed_sidecars.push(path);
        return;
    }

    // Row 7b: the root is gone, the ledger is stale — roll forward the
    // tombstone, consume the approval the sidecar carries, audit.
    tombstone_graph_subtree(
        state,
        &sidecar.graph_id,
        sidecar.approval_id.as_deref(),
        sidecar.actor.as_deref(),
    );
    state.recovery_records.insert(
        sidecar.operation_id.clone(),
        json!({
            "kind": "graph_delete",
            "graph_id": sidecar.graph_id,
            "outcome": "rolled_forward",
            "recovered_at": now_rfc3339(),
            "actor": sidecar.actor,
        }),
    );
    if let Some(approval_id) = &sidecar.approval_id {
        record_approval_consumed(state, approval_id, &sidecar.operation_id);
        outcome.consumed_approvals.push(approval_id.clone());
    }
    diagnostics.push(Diagnostic::warning(
        "cluster_recovery_rolled_forward",
        graph_address,
        "an interrupted graph delete had completed on disk; cluster state was rolled forward to match",
    ));
    outcome.completed_sidecars.push(path);
}

/// Remove a graph's subtree (graph, schema, queries) from the ledger and
/// leave a tombstone observation. Idempotent.
pub(crate) fn tombstone_graph_subtree(
    state: &mut ClusterState,
    graph_id: &str,
    approval_id: Option<&str>,
    actor: Option<&str>,
) {
    let graph_addr = graph_address(graph_id);
    let schema_addr = schema_address(graph_id);
    let query_prefix = format!("query.{graph_id}.");
    state.applied_revision.resources.remove(&graph_addr);
    state.applied_revision.resources.remove(&schema_addr);
    state
        .applied_revision
        .resources
        .retain(|address, _| !address.starts_with(&query_prefix));
    state.resource_statuses.remove(&graph_addr);
    state.resource_statuses.remove(&schema_addr);
    state
        .resource_statuses
        .retain(|address, _| !address.starts_with(&query_prefix));
    state.observations.insert(
        graph_addr,
        json!({
            "kind": "tombstone",
            "deleted_at": now_rfc3339(),
            "approval_id": approval_id,
            "actor": actor,
        }),
    );
}

/// Record approval consumption in the state ledger. The artifact FILE is
/// rewritten with consumed_at only after the state write lands, so a failed
/// CAS leaves the approval valid for the retry.
pub(crate) fn record_approval_consumed(
    state: &mut ClusterState,
    approval_id: &str,
    operation_id: &str,
) {
    state.approval_records.insert(
        approval_id.to_string(),
        json!({
            "consumed_at": now_rfc3339(),
            "consumed_by_operation": operation_id,
        }),
    );
}

/// Mark approval artifact files consumed on disk (post-CAS).
pub(crate) async fn mark_approvals_consumed(backend: &ClusterStore, approval_ids: &[String]) {
    if approval_ids.is_empty() {
        return;
    }
    let mut sink = Vec::new();
    for (_, mut artifact) in backend.list_approval_artifacts(&mut sink).await {
        if approval_ids.contains(&artifact.approval_id) && artifact.consumed_at.is_none() {
            artifact.consumed_at = Some(now_rfc3339());
            let _ = backend.write_approval_artifact(&artifact).await;
        }
    }
}

/// Read-only commands report pending sidecars without acting on them.
pub(crate) async fn warn_pending_recovery_sidecars(
    backend: &ClusterStore,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for location in backend.list_recovery_sidecar_locations(diagnostics).await {
        diagnostics.push(Diagnostic::warning(
            "cluster_recovery_pending",
            location,
            "a recovery sidecar from an interrupted apply is pending; the next state-mutating command will classify it",
        ));
    }
}
