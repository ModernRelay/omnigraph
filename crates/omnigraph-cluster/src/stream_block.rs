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

fn redacted_stream_control_error(
    kind: OfflineStreamControlKind,
    error: &omnigraph::error::OmniError,
) -> &'static str {
    use omnigraph::error::{ManifestErrorKind, OmniError};

    match error {
        OmniError::Policy(_) => "graph stream control was denied by policy",
        OmniError::StreamingAuthorityMismatch { .. }
        | OmniError::StreamingRequiresClusterRuntime { .. } => {
            "offline graph streaming authority is unavailable or changed; stop every live runtime, refresh cluster state, and retry"
        }
        OmniError::ResourceLimitExceeded { .. }
        | OmniError::Manifest(omnigraph::error::ManifestError {
            kind: ManifestErrorKind::BadRequest,
            ..
        }) => "graph stream control request is invalid or exceeds a safety limit",
        OmniError::StreamLifecycleChanged { .. }
        | OmniError::StreamLifecycleIdempotencyConflict { .. }
        | OmniError::Manifest(omnigraph::error::ManifestError {
            kind: ManifestErrorKind::Conflict,
            ..
        }) => "graph stream authority changed; reread graph stream status and retry",
        OmniError::RecoveryRequired { .. } => {
            "graph recovery must settle before stream control can continue"
        }
        _ => match kind {
            OfflineStreamControlKind::Block => {
                "graph stream-block control failed; reread graph stream status and retry"
            }
            OfflineStreamControlKind::DeadLetter => {
                "graph stream dead-letter control failed; reread graph stream status and retry"
            }
        },
    }
}

fn project_logical_declaration(
    table_key: &str,
) -> omnigraph::error::Result<StreamLogicalDeclarationOutput> {
    let (kind, type_name) = if let Some(type_name) = table_key.strip_prefix("node:") {
        ("node", type_name)
    } else if let Some(type_name) = table_key.strip_prefix("edge:") {
        ("edge", type_name)
    } else {
        return Err(omnigraph::error::OmniError::manifest_internal(
            "graph stream projection found an invalid logical declaration key",
        ));
    };
    if type_name.is_empty() {
        return Err(omnigraph::error::OmniError::manifest_internal(
            "graph stream projection found an empty logical declaration name",
        ));
    }
    Ok(StreamLogicalDeclarationOutput {
        kind: kind.to_string(),
        type_name: type_name.to_string(),
    })
}

fn project_stream_block_page(
    page: omnigraph::db::StreamDataBlockPage,
) -> omnigraph::error::Result<StreamBlockPageOutput> {
    let declaration = project_logical_declaration(&page.table_key)?;
    if page
        .entries
        .iter()
        .any(|entry| entry.table_key != page.table_key)
    {
        return Err(omnigraph::error::OmniError::manifest_internal(
            "graph stream block projection crossed logical declarations",
        ));
    }
    Ok(StreamBlockPageOutput {
        declaration,
        block_token: page.block_token,
        lifecycle_revision: page.lifecycle_revision,
        correction_view_digest: page.correction_view_digest,
        entries: page
            .entries
            .into_iter()
            .map(|entry| StreamBlockEntryOutput {
                ordinal: entry.ordinal,
                logical_key: entry.logical_key,
                current_blocked_winner_stream_token: entry.current_blocked_winner_stream_token,
                violation_code: entry.violation_code,
                field_path_or_group: entry.field_path_or_group,
                violation_instance_id: entry.violation_instance_id,
                allowed_actions: entry.allowed_actions,
            })
            .collect(),
        next_cursor: page.next_cursor,
    })
}

fn project_stream_dead_letter_entry(
    entry: omnigraph::db::StreamDeadLetterEntry,
) -> omnigraph::error::Result<StreamDeadLetterEntryOutput> {
    Ok(StreamDeadLetterEntryOutput {
        declaration: project_logical_declaration(&entry.table_key)?,
        logical_id: entry.logical_id,
        occurrence_token: entry.occurrence_token,
        predecessor_token: entry.predecessor_token,
        write_id: entry.write_id,
        contributor_id: entry.contributor_id,
        payload_digest: entry.payload_digest,
        reason_code: entry.reason_code,
        candidate_ordinal: entry.candidate_ordinal,
    })
}

fn project_stream_dead_letter_page(
    page: omnigraph::db::StreamDeadLetterPage,
) -> omnigraph::error::Result<StreamDeadLetterPageOutput> {
    Ok(StreamDeadLetterPageOutput {
        source_manifest_version: page.source_manifest_version,
        source_profile_revision: page.source_profile_revision,
        entries: page
            .entries
            .into_iter()
            .map(project_stream_dead_letter_entry)
            .collect::<omnigraph::error::Result<_>>()?,
        next_cursor: page.next_cursor,
    })
}

fn project_stream_dead_letter_payload_page(
    page: omnigraph::db::StreamDeadLetterPayloadPage,
) -> omnigraph::error::Result<StreamDeadLetterPayloadPageOutput> {
    Ok(StreamDeadLetterPayloadPageOutput {
        source_manifest_version: page.source_manifest_version,
        source_profile_revision: page.source_profile_revision,
        entries: page
            .entries
            .into_iter()
            .map(|entry| {
                Ok(StreamDeadLetterPayloadEntryOutput {
                    authority: project_stream_dead_letter_entry(entry.authority)?,
                    payload: entry.payload,
                })
            })
            .collect::<omnigraph::error::Result<_>>()?,
        next_cursor: page.next_cursor,
    })
}

pub async fn show_stream_data_block_config_dir(
    config_dir: impl AsRef<Path>,
    graph_id: impl AsRef<str>,
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
        let page = db
            .show_graph_stream_data_block(authority, block_token, cursor)
            .await?;
        project_stream_block_page(page)
    }
    .await;

    let page = match result {
        Ok(page) => Some(page),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_block_show_failed",
                format!("graph.{graph_id}"),
                redacted_stream_control_error(OfflineStreamControlKind::Block, &error),
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
        db.correct_graph_stream_data_block(authority, request).await
    }
    .await;

    let result = match result {
        Ok(result) => Some(result),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_block_correct_failed",
                format!("graph.{graph_id}"),
                redacted_stream_control_error(OfflineStreamControlKind::Block, &error),
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
        let page = db.list_stream_dead_letters(authority, cursor).await?;
        project_stream_dead_letter_page(page)
    }
    .await;
    let page = match result {
        Ok(page) => Some(page),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_dead_letter_list_failed",
                format!("graph.{graph_id}"),
                redacted_stream_control_error(OfflineStreamControlKind::DeadLetter, &error),
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
        let page = db
            .export_stream_dead_letter_payloads(authority, cursor)
            .await?;
        project_stream_dead_letter_payload_page(page)
    }
    .await;
    let page = match result {
        Ok(page) => Some(page),
        Err(error) => {
            diagnostics.push(Diagnostic::error(
                "stream_dead_letter_export_failed",
                format!("graph.{graph_id}"),
                redacted_stream_control_error(OfflineStreamControlKind::DeadLetter, &error),
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

#[cfg(test)]
mod projection_tests {
    use super::*;

    fn raw_dead_letter_entry() -> omnigraph::db::StreamDeadLetterEntry {
        omnigraph::db::StreamDeadLetterEntry {
            stable_table_id: 41,
            table_incarnation_id: 42,
            table_key: "node:SecretDeclaration".to_string(),
            logical_id: "logical-1".to_string(),
            stream_incarnation_id: "secret-stream-incarnation".to_string(),
            occurrence_token: "occurrence-1".to_string(),
            predecessor_token: Some("predecessor-1".to_string()),
            write_id: "write-1".to_string(),
            contributor_id: "contributor-1".to_string(),
            payload_digest: "sha256:payload".to_string(),
            reason_code: "UNIQUE_VIOLATION".to_string(),
            fold_operation_id: "secret-fold-operation".to_string(),
            object_location: "s3://private-bucket/secret-object".to_string(),
            object_digest: "sha256:secret-object".to_string(),
            object_encoded_length: 99,
            object_candidate_count: 3,
            candidate_ordinal: 2,
        }
    }

    #[test]
    fn block_projection_exposes_only_graph_level_correction_authority() {
        let projected = project_stream_block_page(omnigraph::db::StreamDataBlockPage {
            block_token: "opaque-block-token".to_string(),
            stable_table_id: 41,
            table_incarnation_id: 42,
            table_key: "node:SecretDeclaration".to_string(),
            lifecycle_revision: 7,
            correction_view_digest: "sha256:view".to_string(),
            entries: vec![omnigraph::db::StreamDataBlockEntry {
                ordinal: 0,
                table_key: "node:SecretDeclaration".to_string(),
                logical_key: "logical-1".to_string(),
                current_blocked_winner_stream_token: "winner-1".to_string(),
                violation_code: "UNIQUE_VIOLATION".to_string(),
                field_path_or_group: vec!["email".to_string()],
                violation_instance_id: "violation-1".to_string(),
                allowed_actions: vec!["WITHDRAW".to_string()],
            }],
            next_cursor: Some("opaque-cursor".to_string()),
        })
        .unwrap();

        let json = serde_json::to_string(&projected).unwrap();
        assert!(json.contains("opaque-block-token"));
        assert!(json.contains("logical-1"));
        assert!(json.contains(r#""kind":"node""#));
        assert!(json.contains(r#""type":"SecretDeclaration""#));
        for forbidden in [
            "stable_table_id",
            "table_incarnation_id",
            "table_key",
        ] {
            assert!(
                !json.contains(forbidden),
                "projection leaked {forbidden}: {json}"
            );
        }
    }

    #[test]
    fn dead_letter_projections_hide_dataset_and_object_coordinates() {
        let projected = project_stream_dead_letter_page(omnigraph::db::StreamDeadLetterPage {
            source_manifest_version: 10,
            source_profile_revision: 11,
            token_table_version: 12,
            token_transaction_uuid: "secret-token-transaction".to_string(),
            entries: vec![raw_dead_letter_entry()],
            next_cursor: Some("opaque-cursor".to_string()),
        })
        .unwrap();
        let json = serde_json::to_string(&projected).unwrap();
        assert!(json.contains("logical-1"));
        assert!(json.contains("source_manifest_version"));
        assert!(json.contains(r#""kind":"node""#));
        assert!(json.contains(r#""type":"SecretDeclaration""#));
        for forbidden in [
            "stable_table_id",
            "table_incarnation_id",
            "table_key",
            "stream_incarnation_id",
            "fold_operation_id",
            "object_location",
            "object_digest",
            "object_encoded_length",
            "object_candidate_count",
            "token_table_version",
            "token_transaction_uuid",
            "private-bucket",
            "secret-stream-incarnation",
            "secret-fold-operation",
            "secret-token-transaction",
        ] {
            assert!(
                !json.contains(forbidden),
                "projection leaked {forbidden}: {json}"
            );
        }

        let payload =
            serde_json::value::RawValue::from_string(r#"{"nested":{"legal":true}}"#.to_string())
                .unwrap();
        let payload_projection =
            project_stream_dead_letter_payload_page(omnigraph::db::StreamDeadLetterPayloadPage {
                source_manifest_version: 10,
                source_profile_revision: 11,
                token_table_version: 12,
                token_transaction_uuid: "secret-token-transaction".to_string(),
                entries: vec![omnigraph::db::StreamDeadLetterPayloadEntry {
                    authority: raw_dead_letter_entry(),
                    payload,
                }],
                next_cursor: None,
            })
            .unwrap();
        let payload_json = serde_json::to_string(&payload_projection).unwrap();
        assert!(payload_json.contains(r#""nested":{"legal":true}"#));
        assert!(!payload_json.contains("private-bucket"));
        assert!(!payload_json.contains("token_transaction_uuid"));
    }

    #[test]
    fn diagnostics_do_not_render_physical_stream_identity() {
        let error = omnigraph::error::OmniError::StreamLifecycleChanged {
            stable_table_id: 41,
            table_incarnation_id: 42,
            expected_revision: 7,
            current_revision: 8,
        };
        let message = redacted_stream_control_error(OfflineStreamControlKind::Block, &error);
        assert_eq!(
            message,
            "graph stream authority changed; reread graph stream status and retry"
        );
        assert!(!message.contains("0000000000000029"));
        assert!(!message.contains("000000000000002a"));
    }
}
