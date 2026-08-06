//! Offline cluster control plane for irreversible stream-authority retirement.
//!
//! Both halves hold the real cluster state lock and a stopped-process guard.
//! Planning is graph-effect-free; confirmation consumes the exact digest from
//! planning and delegates all receipt-first idempotency and graph publication
//! to the engine.

use omnigraph::db::Omnigraph;
use omnigraph_control_authority::{
    AuthorityOperationClass, OfflineAuthorityRequest, STREAM_AUTHORITY_RETIREMENT_LOCK_OPERATION,
    StateLockGuard, validate_offline_guard,
};

use super::*;

struct PreparedRetirementCommand {
    config_dir: String,
    graph_id: String,
    graph_uri: String,
    actor: String,
    desired: DesiredCluster,
    backend: ClusterStore,
    state: ClusterState,
    state_cas: String,
    observations: StateObservations,
    diagnostics: Vec<Diagnostic>,
    lock_guard: StateLockGuard,
}

struct FailedRetirementCommand {
    config_dir: String,
    graph_id: String,
    graph_uri: Option<String>,
    actor: Option<String>,
    observations: StateObservations,
    diagnostics: Vec<Diagnostic>,
}

pub async fn plan_stream_authority_retirement_config_dir(
    config_dir: impl AsRef<Path>,
    graph_id: impl AsRef<str>,
    options: StreamAuthorityRetirementOptions,
) -> StreamAuthorityRetirementPlanOutput {
    let graph_id = graph_id.as_ref().to_string();
    let prepared = match prepare_retirement_command(config_dir.as_ref(), &graph_id, &options).await
    {
        Ok(prepared) => prepared,
        Err(failed) => {
            return StreamAuthorityRetirementPlanOutput {
                ok: false,
                config_dir: failed.config_dir,
                graph_id: failed.graph_id,
                graph_uri: failed.graph_uri,
                actor: failed.actor,
                state_observations: failed.observations,
                plan: None,
                diagnostics: failed.diagnostics,
            };
        }
    };

    let PreparedRetirementCommand {
        config_dir,
        graph_id,
        graph_uri,
        actor,
        desired,
        backend,
        state,
        state_cas,
        observations,
        mut diagnostics,
        lock_guard,
    } = prepared;
    let result = async {
        let db =
            open_authorized_retirement_graph(&graph_id, &graph_uri, &desired, &backend, &state)
                .await?;
        let status = db.stream_status().await?;
        let operation_id =
            retirement_plan_authority_id(&graph_id, &state_cas, status.profile_revision);
        let authority = checked_retirement_authority(
            &db,
            &lock_guard,
            &graph_id,
            &graph_uri,
            &actor,
            &state,
            &state_cas,
            status.profile_revision,
            &operation_id,
        )
        .await?;
        db.plan_stream_authority_retirement(authority).await
    }
    .await;

    let plan = match result {
        Ok(plan) => Some(plan),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_authority_retirement_plan_failed",
                format!("graph.{graph_id}"),
                error.to_string(),
            ));
            None
        }
    };
    StreamAuthorityRetirementPlanOutput {
        ok: plan.is_some() && !has_errors(&diagnostics),
        config_dir,
        graph_id,
        graph_uri: Some(graph_uri),
        actor: Some(actor),
        state_observations: observations,
        plan,
        diagnostics,
    }
}

pub async fn confirm_stream_authority_retirement_config_dir(
    config_dir: impl AsRef<Path>,
    graph_id: impl AsRef<str>,
    retirement_id: impl AsRef<str>,
    expected_plan_digest: impl AsRef<str>,
    options: StreamAuthorityRetirementOptions,
) -> StreamAuthorityRetirementConfirmOutput {
    let graph_id = graph_id.as_ref().to_string();
    let prepared = match prepare_retirement_command(config_dir.as_ref(), &graph_id, &options).await
    {
        Ok(prepared) => prepared,
        Err(failed) => {
            return StreamAuthorityRetirementConfirmOutput {
                ok: false,
                config_dir: failed.config_dir,
                graph_id: failed.graph_id,
                graph_uri: failed.graph_uri,
                actor: failed.actor,
                state_observations: failed.observations,
                result: None,
                diagnostics: failed.diagnostics,
            };
        }
    };

    let retirement_id = retirement_id.as_ref();
    let expected_plan_digest = expected_plan_digest.as_ref();
    let PreparedRetirementCommand {
        config_dir,
        graph_id,
        graph_uri,
        actor,
        desired,
        backend,
        mut state,
        state_cas,
        mut observations,
        mut diagnostics,
        lock_guard,
    } = prepared;
    let result = async {
        let db =
            open_authorized_retirement_graph(&graph_id, &graph_uri, &desired, &backend, &state)
                .await?;
        let status = db.stream_status().await?;
        let authority = checked_retirement_authority(
            &db,
            &lock_guard,
            &graph_id,
            &graph_uri,
            &actor,
            &state,
            &state_cas,
            status.profile_revision,
            retirement_id,
        )
        .await?;
        db.confirm_stream_authority_retirement(authority, retirement_id, expected_plan_digest)
            .await
    }
    .await;

    let result = match result {
        Ok(result) => {
            if let Err(diagnostic) = converge_retired_streaming_resource(
                &backend,
                &mut state,
                &state_cas,
                &mut observations,
                &graph_id,
                result.profile_revision,
            )
            .await
            {
                diagnostics.push(diagnostic);
            }
            Some(result)
        }
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_authority_retirement_confirm_failed",
                format!("graph.{graph_id}"),
                error.to_string(),
            ));
            None
        }
    };
    StreamAuthorityRetirementConfirmOutput {
        ok: result.is_some() && !has_errors(&diagnostics),
        config_dir,
        graph_id,
        graph_uri: Some(graph_uri),
        actor: Some(actor),
        state_observations: observations,
        result,
        diagnostics,
    }
}

/// Converge a managed streaming declaration to the exact terminal profile
/// published by retirement while the caller still holds the retirement state
/// lock. An unmanaged graph deliberately has no streaming resource to update;
/// its graph-resource binding remains the serving authority.
async fn converge_retired_streaming_resource(
    backend: &ClusterStore,
    state: &mut ClusterState,
    expected_state_cas: &str,
    observations: &mut StateObservations,
    graph_id: &str,
    profile_revision: u64,
) -> Result<(), Diagnostic> {
    let streaming_address = format!("streaming.{graph_id}");
    let Some(streaming) = state.applied_revision.resources.get_mut(&streaming_address) else {
        return Ok(());
    };
    if streaming.streaming_enabled == Some(false)
        && streaming.profile_mode.as_deref() == Some("RETIRED")
        && streaming.profile_revision == Some(profile_revision)
        && streaming.declaration_revision.is_some()
    {
        return Ok(());
    }

    // The declaration digest and declaration revision remain the cluster
    // identity of this managed resource. Retirement changes only the live
    // manifest profile projection recorded beside them. A legacy/malformed
    // row may lack the deterministically derived declaration revision; repair
    // that local projection now so serving cannot quarantine a row refresh
    // otherwise recognizes as converged.
    if streaming.declaration_revision.is_none() {
        streaming.declaration_revision = Some(crate::config::streaming_declaration_revision(
            graph_id,
            &streaming.digest,
        ));
    }
    streaming.streaming_enabled = Some(false);
    streaming.profile_mode = Some("RETIRED".to_string());
    streaming.profile_revision = Some(profile_revision);
    state.state_revision = state.state_revision.saturating_add(1);
    crate::failpoints::maybe_fail(
        crate::failpoints::names::CLUSTER_STREAM_RETIREMENT_BEFORE_STATE_WRITE,
    )?;
    backend
        .write_state(state, Some(expected_state_cas), observations)
        .await
}

async fn prepare_retirement_command(
    config_dir: &Path,
    graph_id: &str,
    options: &StreamAuthorityRetirementOptions,
) -> Result<PreparedRetirementCommand, FailedRetirementCommand> {
    let outcome = load_desired(config_dir);
    let mut diagnostics = outcome.diagnostics;
    let config_dir_display = display_path(&outcome.config_dir);
    let storage_root = outcome
        .desired
        .as_ref()
        .and_then(|desired| desired.storage_root.clone());
    let backend = match store_for(&outcome.config_dir, storage_root.as_deref()) {
        Ok(backend) => backend,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            ClusterStore::for_config_dir(&outcome.config_dir)
        }
    };
    let mut observations = backend.observations();
    let actor = options
        .actor
        .as_deref()
        .map(str::trim)
        .filter(|actor| !actor.is_empty())
        .map(str::to_string);

    if actor.is_none() {
        diagnostics.push(Diagnostic::error(
            "stream_authority_retirement_actor_required",
            "actor",
            "stream-authority retirement requires an authenticated actor",
        ));
    }
    if !options.confirm_stream_offline {
        diagnostics.push(Diagnostic::error(
            "streaming_offline_confirmation_required",
            "confirm_stream_offline",
            "stream-authority retirement requires --confirm-stream-offline after every writer-capable process for the graph has stopped",
        ));
    }

    let Some(desired) = outcome.desired else {
        return Err(FailedRetirementCommand {
            config_dir: config_dir_display,
            graph_id: graph_id.to_string(),
            graph_uri: None,
            actor,
            observations,
            diagnostics,
        });
    };
    let graph_uri = backend.graph_root(graph_id);
    if !desired.graphs.iter().any(|graph| graph.id == graph_id) {
        diagnostics.push(Diagnostic::error(
            "stream_authority_retirement_graph_not_declared",
            format!("graphs.{graph_id}"),
            "retirement requires a graph declared by this cluster config",
        ));
    }
    if !desired.state_lock {
        diagnostics.push(Diagnostic::error(
            "streaming_requires_state_lock",
            "state.lock",
            "stream-authority retirement requires state.lock: true and the held cluster state lock",
        ));
    }
    if has_errors(&diagnostics) {
        return Err(FailedRetirementCommand {
            config_dir: config_dir_display,
            graph_id: graph_id.to_string(),
            graph_uri: Some(graph_uri),
            actor,
            observations,
            diagnostics,
        });
    }

    let lock_guard = match backend
        .acquire_lock(
            STREAM_AUTHORITY_RETIREMENT_LOCK_OPERATION,
            &mut observations,
        )
        .await
    {
        Ok(guard) => guard,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return Err(FailedRetirementCommand {
                config_dir: config_dir_display,
                graph_id: graph_id.to_string(),
                graph_uri: Some(graph_uri),
                actor,
                observations,
                diagnostics,
            });
        }
    };
    let snapshot = match backend.read_state(&mut observations).await {
        Ok(snapshot) => snapshot,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return Err(FailedRetirementCommand {
                config_dir: config_dir_display,
                graph_id: graph_id.to_string(),
                graph_uri: Some(graph_uri),
                actor,
                observations,
                diagnostics,
            });
        }
    };
    let Some(state) = snapshot.state else {
        diagnostics.push(Diagnostic::error(
            "state_missing",
            CLUSTER_STATE_FILE,
            "retirement requires an existing state.json; run `cluster import` first",
        ));
        return Err(FailedRetirementCommand {
            config_dir: config_dir_display,
            graph_id: graph_id.to_string(),
            graph_uri: Some(graph_uri),
            actor,
            observations,
            diagnostics,
        });
    };
    let state_cas = snapshot
        .state_cas
        .expect("a present cluster state always has a content CAS");
    if !state
        .applied_revision
        .resources
        .contains_key(&graph_address(graph_id))
    {
        diagnostics.push(Diagnostic::error(
            "stream_authority_retirement_graph_not_applied",
            format!("graph.{graph_id}"),
            "retirement requires the selected graph to exist in applied cluster state",
        ));
    }

    // An irreversible root-wide cut cannot be planned around an unknown or
    // interrupted cluster graph mutation. Unlike ordinary read-only `plan`,
    // warnings are insufficient here because an unreadable sidecar could own
    // the exact graph mapping or schema authority being frozen.
    let diagnostic_count_before_recovery_scan = diagnostics.len();
    let pending_recovery = backend.list_recovery_sidecars(&mut diagnostics).await;
    if diagnostics.len() != diagnostic_count_before_recovery_scan {
        diagnostics.push(Diagnostic::error(
            "stream_authority_retirement_recovery_unverifiable",
            CLUSTER_RECOVERIES_DIR,
            "could not prove cluster recovery clear before irreversible retirement",
        ));
    }
    for (path, sidecar) in pending_recovery {
        if sidecar.graph_id == graph_id {
            diagnostics.push(Diagnostic::error(
                "stream_authority_retirement_recovery_pending",
                path,
                "an interrupted cluster graph operation must be settled before retirement",
            ));
        }
    }
    if has_errors(&diagnostics) {
        return Err(FailedRetirementCommand {
            config_dir: config_dir_display,
            graph_id: graph_id.to_string(),
            graph_uri: Some(graph_uri),
            actor,
            observations,
            diagnostics,
        });
    }

    Ok(PreparedRetirementCommand {
        config_dir: config_dir_display,
        graph_id: graph_id.to_string(),
        graph_uri,
        actor: actor.expect("actor preflight succeeded"),
        desired,
        backend,
        state,
        state_cas,
        observations,
        diagnostics,
        lock_guard,
    })
}

async fn open_authorized_retirement_graph(
    graph_id: &str,
    graph_uri: &str,
    desired: &DesiredCluster,
    backend: &ClusterStore,
    state: &ClusterState,
) -> omnigraph::error::Result<Omnigraph> {
    let policy = stream_profile_policy_checker(graph_id, state, desired, backend)
        .await
        .map_err(omnigraph::error::OmniError::Policy)?;
    let db = Omnigraph::open(graph_uri).await?;
    Ok(match policy {
        Some(policy) => db.with_policy(policy),
        None => db,
    })
}

#[allow(clippy::too_many_arguments)]
async fn checked_retirement_authority<'lock>(
    db: &Omnigraph,
    lock_guard: &'lock StateLockGuard,
    graph_id: &str,
    graph_uri: &str,
    actor: &str,
    state: &ClusterState,
    state_cas: &str,
    expected_profile_revision: u64,
    operation_id: &str,
) -> omnigraph::error::Result<omnigraph::db::CheckedClusterRetirementAuthority<'lock>> {
    let (declaration_revision, declaration_digest) = retirement_control_binding(state, graph_id)?;
    let guard = validate_offline_guard(
        lock_guard,
        OfflineAuthorityRequest {
            graph_id,
            graph_store_uri: graph_uri,
            expected_state_cas: state_cas,
            state_revision: state.state_revision,
            declaration_revision: &declaration_revision,
            declaration_digest: &declaration_digest,
            expected_profile_revision,
            operation_id,
            operation: AuthorityOperationClass::StreamAuthorityRetirement,
            actor,
            confirm_stream_offline: true,
        },
    )
    .await
    .map_err(
        |error| omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: error.to_string(),
        },
    )?;
    db.check_cluster_retirement_authority(guard).await
}

fn retirement_control_binding(
    state: &ClusterState,
    graph_id: &str,
) -> omnigraph::error::Result<(String, String)> {
    if let Some(streaming) = state
        .applied_revision
        .resources
        .get(&format!("streaming.{graph_id}"))
    {
        if let Some(revision) = streaming.declaration_revision.as_deref() {
            return Ok((revision.to_string(), streaming.digest.clone()));
        }
    }
    let graph = state
        .applied_revision
        .resources
        .get(&graph_address(graph_id))
        .ok_or_else(|| omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: format!("applied graph resource 'graph.{graph_id}' disappeared"),
        })?;
    Ok((
        "stream-authority-retirement-unmanaged-v1".to_string(),
        graph.digest.clone(),
    ))
}

fn retirement_plan_authority_id(graph_id: &str, state_cas: &str, profile_revision: u64) -> String {
    format!(
        "retirement-plan:{}",
        sha256_hex(
            format!(
                "omnigraph.cluster.stream-authority-retirement-plan-authority.v1\0{graph_id}\0{state_cas}\0{profile_revision}"
            )
            .as_bytes()
        )
    )
}
