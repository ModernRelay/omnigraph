//! Offline cluster control plane for strict stream-block inspection and data
//! correction.
//!
//! Both operations retain the real cluster apply lock and the stopped-process
//! guard. Inspection is graph-effect-free; correction delegates its exact
//! block/revision fencing, receipt-first idempotency, recovery, and graph
//! publication to the engine.

use omnigraph::db::{Omnigraph, StreamDataCorrectionRequest};
use omnigraph_control_authority::{
    AuthorityOperationClass, OfflineAuthorityRequest, StateLockGuard, ValidatedOfflineGuard,
    validate_offline_guard,
};

use super::*;

struct PreparedBlockCommand {
    config_dir: String,
    graph_id: String,
    graph_uri: String,
    actor: String,
    desired: DesiredCluster,
    backend: ClusterStore,
    state: ClusterState,
    state_cas: String,
    expected_profile_revision: u64,
    observations: StateObservations,
    diagnostics: Vec<Diagnostic>,
    lock_guard: StateLockGuard,
}

struct FailedBlockCommand {
    config_dir: String,
    graph_id: String,
    graph_uri: Option<String>,
    actor: Option<String>,
    observations: StateObservations,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Clone, Copy)]
enum OfflineStreamControlKind {
    Block,
    DeadLetter,
}

impl OfflineStreamControlKind {
    fn subject(self) -> &'static str {
        match self {
            Self::Block => "stream-block control",
            Self::DeadLetter => "stream dead-letter control",
        }
    }

    fn code(self, suffix: &str) -> String {
        let prefix = match self {
            Self::Block => "stream_block",
            Self::DeadLetter => "stream_dead_letter",
        };
        format!("{prefix}_{suffix}")
    }
}

pub async fn show_stream_data_block_config_dir(
    config_dir: impl AsRef<Path>,
    graph_id: impl AsRef<str>,
    table_key: impl AsRef<str>,
    block_token: impl AsRef<str>,
    cursor: Option<&str>,
    options: StreamBlockControlOptions,
) -> StreamBlockShowOutput {
    let graph_id = graph_id.as_ref().to_string();
    let prepared = match prepare_block_command(config_dir.as_ref(), &graph_id, &options).await {
        Ok(prepared) => prepared,
        Err(failed) => {
            return StreamBlockShowOutput {
                ok: false,
                config_dir: failed.config_dir,
                graph_id: failed.graph_id,
                graph_uri: failed.graph_uri,
                actor: failed.actor,
                state_observations: failed.observations,
                page: None,
                diagnostics: failed.diagnostics,
            };
        }
    };

    let table_key = table_key.as_ref();
    let block_token = block_token.as_ref();
    let PreparedBlockCommand {
        config_dir,
        graph_id,
        graph_uri,
        actor,
        desired,
        backend,
        state,
        state_cas,
        expected_profile_revision,
        observations,
        mut diagnostics,
        lock_guard,
    } = prepared;
    let result = async {
        let guard = validated_block_offline_guard(
            &lock_guard,
            &graph_id,
            &graph_uri,
            &actor,
            &state,
            &state_cas,
            expected_profile_revision,
            block_token,
        )
        .await?;
        let db =
            open_authorized_block_graph(&graph_id, &graph_uri, &desired, &backend, &state).await?;
        let authority = db.check_cluster_block_authority(guard).await?;
        db.show_stream_data_block(authority, table_key, block_token, cursor)
            .await
    }
    .await;

    let page = match result {
        Ok(page) => Some(page),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_block_show_failed",
                format!("graph.{graph_id}"),
                error.to_string(),
            ));
            None
        }
    };
    StreamBlockShowOutput {
        ok: page.is_some() && !has_errors(&diagnostics),
        config_dir,
        graph_id,
        graph_uri: Some(graph_uri),
        actor: Some(actor),
        state_observations: observations,
        page,
        diagnostics,
    }
}

pub async fn correct_stream_data_block_config_dir(
    config_dir: impl AsRef<Path>,
    graph_id: impl AsRef<str>,
    table_key: impl AsRef<str>,
    request: StreamDataCorrectionRequest,
    options: StreamBlockControlOptions,
) -> StreamBlockCorrectOutput {
    let graph_id = graph_id.as_ref().to_string();
    let prepared = match prepare_block_command(config_dir.as_ref(), &graph_id, &options).await {
        Ok(prepared) => prepared,
        Err(failed) => {
            return StreamBlockCorrectOutput {
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

    let table_key = table_key.as_ref();
    let correction_id = request.correction_id.clone();
    let PreparedBlockCommand {
        config_dir,
        graph_id,
        graph_uri,
        actor,
        desired,
        backend,
        state,
        state_cas,
        expected_profile_revision,
        observations,
        mut diagnostics,
        lock_guard,
    } = prepared;
    let result = async {
        let guard = validated_block_offline_guard(
            &lock_guard,
            &graph_id,
            &graph_uri,
            &actor,
            &state,
            &state_cas,
            expected_profile_revision,
            &correction_id,
        )
        .await?;
        let db =
            open_authorized_block_graph(&graph_id, &graph_uri, &desired, &backend, &state).await?;
        let authority = db.check_cluster_block_authority(guard).await?;
        db.correct_stream_data_block(authority, table_key, request)
            .await
    }
    .await;

    let result = match result {
        Ok(result) => Some(result),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_block_correct_failed",
                format!("graph.{graph_id}"),
                error.to_string(),
            ));
            None
        }
    };
    StreamBlockCorrectOutput {
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

pub async fn list_stream_dead_letters_config_dir(
    config_dir: impl AsRef<Path>,
    graph_id: impl AsRef<str>,
    cursor: Option<&str>,
    options: StreamDeadLetterControlOptions,
) -> StreamDeadLetterListOutput {
    let graph_id = graph_id.as_ref().to_string();
    let prepared = match prepare_offline_control_command(
        config_dir.as_ref(),
        &graph_id,
        options.actor.as_deref(),
        options.confirm_stream_offline,
        OfflineStreamControlKind::DeadLetter,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(failed) => {
            return StreamDeadLetterListOutput {
                ok: false,
                config_dir: failed.config_dir,
                graph_id: failed.graph_id,
                graph_uri: failed.graph_uri,
                actor: failed.actor,
                state_observations: failed.observations,
                page: None,
                diagnostics: failed.diagnostics,
            };
        }
    };
    let PreparedBlockCommand {
        config_dir,
        graph_id,
        graph_uri,
        actor,
        desired,
        backend,
        state,
        state_cas,
        expected_profile_revision,
        observations,
        mut diagnostics,
        lock_guard,
    } = prepared;
    let result = async {
        let db =
            open_authorized_block_graph(&graph_id, &graph_uri, &desired, &backend, &state).await?;
        let guard = validated_dead_letter_offline_guard(
            &lock_guard,
            &graph_id,
            &graph_uri,
            &actor,
            &state,
            &state_cas,
            expected_profile_revision,
            "dead-letter-list",
        )
        .await?;
        let authority = db.check_cluster_dead_letter_authority(guard).await?;
        db.list_stream_dead_letters(authority, cursor).await
    }
    .await;
    let page = match result {
        Ok(page) => Some(page),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_dead_letter_list_failed",
                format!("graph.{graph_id}"),
                error.to_string(),
            ));
            None
        }
    };
    StreamDeadLetterListOutput {
        ok: page.is_some() && !has_errors(&diagnostics),
        config_dir,
        graph_id,
        graph_uri: Some(graph_uri),
        actor: Some(actor),
        state_observations: observations,
        page,
        diagnostics,
    }
}

pub async fn export_stream_dead_letters_config_dir(
    config_dir: impl AsRef<Path>,
    graph_id: impl AsRef<str>,
    cursor: Option<&str>,
    options: StreamDeadLetterControlOptions,
) -> StreamDeadLetterExportOutput {
    let graph_id = graph_id.as_ref().to_string();
    let prepared = match prepare_offline_control_command(
        config_dir.as_ref(),
        &graph_id,
        options.actor.as_deref(),
        options.confirm_stream_offline,
        OfflineStreamControlKind::DeadLetter,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(failed) => {
            return StreamDeadLetterExportOutput {
                ok: false,
                config_dir: failed.config_dir,
                graph_id: failed.graph_id,
                graph_uri: failed.graph_uri,
                actor: failed.actor,
                state_observations: failed.observations,
                page: None,
                diagnostics: failed.diagnostics,
            };
        }
    };
    let PreparedBlockCommand {
        config_dir,
        graph_id,
        graph_uri,
        actor,
        desired,
        backend,
        state,
        state_cas,
        expected_profile_revision,
        observations,
        mut diagnostics,
        lock_guard,
    } = prepared;
    let result = async {
        let db =
            open_authorized_block_graph(&graph_id, &graph_uri, &desired, &backend, &state).await?;
        let guard = validated_dead_letter_offline_guard(
            &lock_guard,
            &graph_id,
            &graph_uri,
            &actor,
            &state,
            &state_cas,
            expected_profile_revision,
            "dead-letter-export",
        )
        .await?;
        let authority = db.check_cluster_dead_letter_authority(guard).await?;
        db.export_stream_dead_letter_payloads(authority, cursor)
            .await
    }
    .await;
    let page = match result {
        Ok(page) => Some(page),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_dead_letter_export_failed",
                format!("graph.{graph_id}"),
                error.to_string(),
            ));
            None
        }
    };
    StreamDeadLetterExportOutput {
        ok: page.is_some() && !has_errors(&diagnostics),
        config_dir,
        graph_id,
        graph_uri: Some(graph_uri),
        actor: Some(actor),
        state_observations: observations,
        page,
        diagnostics,
    }
}

async fn prepare_block_command(
    config_dir: &Path,
    graph_id: &str,
    options: &StreamBlockControlOptions,
) -> Result<PreparedBlockCommand, FailedBlockCommand> {
    prepare_offline_control_command(
        config_dir,
        graph_id,
        options.actor.as_deref(),
        options.confirm_stream_offline,
        OfflineStreamControlKind::Block,
    )
    .await
}

async fn prepare_offline_control_command(
    config_dir: &Path,
    graph_id: &str,
    actor: Option<&str>,
    confirm_stream_offline: bool,
    kind: OfflineStreamControlKind,
) -> Result<PreparedBlockCommand, FailedBlockCommand> {
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
    let actor = actor
        .map(str::trim)
        .filter(|actor| !actor.is_empty())
        .map(str::to_string);

    if actor.is_none() {
        diagnostics.push(Diagnostic::error(
            kind.code("actor_required"),
            "actor",
            format!("{} requires an authenticated actor", kind.subject()),
        ));
    }
    if !confirm_stream_offline {
        diagnostics.push(Diagnostic::error(
            "streaming_offline_confirmation_required",
            "confirm_stream_offline",
            format!("{} requires --confirm-stream-offline after every writer-capable process for the graph has stopped", kind.subject()),
        ));
    }

    let Some(desired) = outcome.desired else {
        return Err(FailedBlockCommand {
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
            kind.code("graph_not_declared"),
            format!("graphs.{graph_id}"),
            format!(
                "{} requires a graph declared by this cluster config",
                kind.subject()
            ),
        ));
    }
    if !desired.state_lock {
        diagnostics.push(Diagnostic::error(
            "streaming_requires_state_lock",
            "state.lock",
            format!(
                "{} requires state.lock: true and the held cluster state lock",
                kind.subject()
            ),
        ));
    }
    if has_errors(&diagnostics) {
        return Err(FailedBlockCommand {
            config_dir: config_dir_display,
            graph_id: graph_id.to_string(),
            graph_uri: Some(graph_uri),
            actor,
            observations,
            diagnostics,
        });
    }

    let lock_guard = match backend.acquire_lock("apply", &mut observations).await {
        Ok(guard) => guard,
        Err(diagnostic) => {
            diagnostics.push(diagnostic);
            return Err(FailedBlockCommand {
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
            return Err(FailedBlockCommand {
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
            format!(
                "{} requires an existing state.json; run `cluster import` first",
                kind.subject()
            ),
        ));
        return Err(FailedBlockCommand {
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
            kind.code("graph_not_applied"),
            format!("graph.{graph_id}"),
            format!(
                "{} requires the selected graph in applied cluster state",
                kind.subject()
            ),
        ));
    }
    let streaming_address = crate::config::streaming_address(graph_id);
    let expected_profile_revision = match state.applied_revision.resources.get(&streaming_address) {
        Some(streaming)
            if streaming.declaration_revision.is_some() && streaming.profile_revision.is_some() =>
        {
            streaming.profile_revision
        }
        _ => {
            diagnostics.push(Diagnostic::error(
                kind.code("profile_not_applied"),
                streaming_address,
                format!("{} requires applied streaming declaration and exact profile-revision authority; run cluster refresh/apply first", kind.subject()),
            ));
            None
        }
    };

    let diagnostic_count_before_recovery_scan = diagnostics.len();
    let pending_recovery = backend.list_recovery_sidecars(&mut diagnostics).await;
    if diagnostics.len() != diagnostic_count_before_recovery_scan {
        diagnostics.push(Diagnostic::error(
            kind.code("recovery_unverifiable"),
            CLUSTER_RECOVERIES_DIR,
            format!(
                "could not prove cluster recovery clear before {}",
                kind.subject()
            ),
        ));
    }
    for (path, sidecar) in pending_recovery {
        if sidecar.graph_id == graph_id {
            diagnostics.push(Diagnostic::error(
                kind.code("recovery_pending"),
                path,
                format!(
                    "an interrupted cluster graph operation must be settled before {}",
                    kind.subject()
                ),
            ));
        }
    }
    if has_errors(&diagnostics) {
        return Err(FailedBlockCommand {
            config_dir: config_dir_display,
            graph_id: graph_id.to_string(),
            graph_uri: Some(graph_uri),
            actor,
            observations,
            diagnostics,
        });
    }

    Ok(PreparedBlockCommand {
        config_dir: config_dir_display,
        graph_id: graph_id.to_string(),
        graph_uri,
        actor: actor.expect("actor preflight succeeded"),
        desired,
        backend,
        state,
        state_cas,
        expected_profile_revision: expected_profile_revision
            .expect("applied profile-revision preflight succeeded"),
        observations,
        diagnostics,
        lock_guard,
    })
}

async fn open_authorized_block_graph(
    graph_id: &str,
    graph_uri: &str,
    desired: &DesiredCluster,
    backend: &ClusterStore,
    state: &ClusterState,
) -> omnigraph::error::Result<Omnigraph> {
    let policy = stream_profile_policy_checker(graph_id, state, desired, backend)
        .await
        .map_err(omnigraph::error::OmniError::Policy)?;
    // Construction is effect-free for both inspection and correction. The
    // correction path may heal and publish only after the lower offline guard,
    // engine capability check, and request-shape validation have all passed.
    let db = Omnigraph::open_read_only(graph_uri).await?;
    Ok(match policy {
        Some(policy) => db.with_policy(policy),
        None => db,
    })
}

#[allow(clippy::too_many_arguments)]
async fn validated_block_offline_guard<'lock>(
    lock_guard: &'lock StateLockGuard,
    graph_id: &str,
    graph_uri: &str,
    actor: &str,
    state: &ClusterState,
    state_cas: &str,
    expected_profile_revision: u64,
    operation_id: &str,
) -> omnigraph::error::Result<ValidatedOfflineGuard<'lock>> {
    let streaming = state
        .applied_revision
        .resources
        .get(&crate::config::streaming_address(graph_id))
        .ok_or_else(|| omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: format!("applied streaming resource 'streaming.{graph_id}' disappeared"),
        })?;
    let declaration_revision = streaming.declaration_revision.as_deref().ok_or_else(|| {
        omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: format!(
                "applied streaming resource 'streaming.{graph_id}' has no declaration revision"
            ),
        }
    })?;
    validate_offline_guard(
        lock_guard,
        OfflineAuthorityRequest {
            graph_id,
            graph_store_uri: graph_uri,
            expected_state_cas: state_cas,
            state_revision: state.state_revision,
            declaration_revision,
            declaration_digest: &streaming.digest,
            expected_profile_revision,
            operation_id,
            operation: AuthorityOperationClass::StreamBlockControl,
            actor,
            confirm_stream_offline: true,
        },
    )
    .await
    .map_err(
        |error| omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: error.to_string(),
        },
    )
}

#[allow(clippy::too_many_arguments)]
async fn validated_dead_letter_offline_guard<'lock>(
    lock_guard: &'lock StateLockGuard,
    graph_id: &str,
    graph_uri: &str,
    actor: &str,
    state: &ClusterState,
    state_cas: &str,
    expected_profile_revision: u64,
    operation_id: &str,
) -> omnigraph::error::Result<ValidatedOfflineGuard<'lock>> {
    let streaming = state
        .applied_revision
        .resources
        .get(&crate::config::streaming_address(graph_id))
        .ok_or_else(|| omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: format!("applied streaming resource 'streaming.{graph_id}' disappeared"),
        })?;
    let declaration_revision = streaming.declaration_revision.as_deref().ok_or_else(|| {
        omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: format!(
                "applied streaming resource 'streaming.{graph_id}' has no declaration revision"
            ),
        }
    })?;
    validate_offline_guard(
        lock_guard,
        OfflineAuthorityRequest {
            graph_id,
            graph_store_uri: graph_uri,
            expected_state_cas: state_cas,
            state_revision: state.state_revision,
            declaration_revision,
            declaration_digest: &streaming.digest,
            expected_profile_revision,
            operation_id,
            operation: AuthorityOperationClass::StreamDeadLetterControl,
            actor,
            confirm_stream_offline: true,
        },
    )
    .await
    .map_err(
        |error| omnigraph::error::OmniError::StreamingAuthorityMismatch {
            reason: error.to_string(),
        },
    )
}
