//! Branch-statement dispatch behind `run_query` and `run_mutate`: the door a
//! source arrived through, the refusals a statement meets at the wrong door
//! or with a request envelope, and the control-write runner that answers as
//! a mutation body does.

use super::*;
use crate::api::branch_statement_refusals as refusals;
use omnigraph::loader::CsvLoadRequest;
use omnigraph_compiler::query::ast::{
    BranchCreateFrom, BranchDiffSides, BranchRead, BranchStmt, BranchWrite, CommitWrite,
    EXPLAIN_STATEMENT_NAME, LoadStmt, SettingStmt, show_statement_name,
};
use omnigraph_compiler::settings::{SettingId, SettingRow};

/// The HTTP entry a GQ source arrived through. A statement is served only at
/// its own door: `Query` serves `branch list`, the branch control reads
/// (`branch diff`, `branch merge-base`, `branch merge … dry run`) and
/// `explain`; `Mutate` serves `branch create`, `branch delete`, and
/// `branch merge`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Door {
    Query,
    Mutate,
}

/// What [`run_query`] answered: a declared query's rows, the branch names
/// of a `branch list` statement sorted in byte order, the answer of a branch
/// control read, or the setting rows of a `show` statement in definition
/// order.
pub(crate) enum ReadDispatch {
    Rows {
        query_name: String,
        target: ReadTarget,
        result: omnigraph_compiler::result::QueryResult,
        graph_commit_id: Option<String>,
    },
    BranchList(Vec<String>),
    BranchRead(ReadOutput),
    Show(Vec<SettingRow>),
}

impl ReadDispatch {
    /// Render any answer as the `ReadOutput` every read route serves.
    pub(crate) fn into_read_output(self) -> std::result::Result<ReadOutput, ApiError> {
        match self {
            ReadDispatch::Rows {
                query_name,
                target,
                result,
                graph_commit_id,
            } => {
                api::read_output(query_name, &target, result, graph_commit_id).map_err(render_error)
            }
            ReadDispatch::BranchList(branches) => {
                api::branch_list_read_output(&branches).map_err(render_error)
            }
            ReadDispatch::BranchRead(output) => Ok(output),
            ReadDispatch::Show(rows) => api::show_read_output(&rows).map_err(render_error),
        }
    }
}

/// The `process` scope rule at a remote door: a `set` or `reset` of a
/// `process` setting in a request's text is refused before anything runs.
pub(super) fn refuse_process_settings(
    settings: &[SettingStmt],
) -> std::result::Result<(), ApiError> {
    for id in settings.iter().filter_map(SettingStmt::id) {
        id.refuse_from_request()
            .map_err(|error| ApiError::bad_request(error.to_string()))?;
    }
    Ok(())
}

/// A source no door can pick a statement from: settings-only, or holding
/// nothing at all.
pub(super) fn refuse_empty_file(file: &QueryFile) -> std::result::Result<(), ApiError> {
    match file.empty_kind() {
        Some(EmptyFile::SettingsOnly) => Err(ApiError::bad_request(
            crate::api::query_file_refusals::ONLY_SETTINGS,
        )),
        Some(EmptyFile::NoStatement) => Err(ApiError::bad_request(
            crate::api::query_file_refusals::NO_QUERY,
        )),
        None => Ok(()),
    }
}

/// The request's session with the source's `set` and `reset` lines applied,
/// for the statements the engine does not parse itself (`branch merge`,
/// `show`); the request's own session is unchanged.
pub(super) fn session_with_prefix(
    session: &Session,
    settings: &[SettingStmt],
) -> std::result::Result<Session, ApiError> {
    session
        .with_prefix(settings)
        .map_err(|error| ApiError::bad_request(error.to_string()))
}

/// Parse the source into its kind: `query` declarations or one branch
/// statement. Runs before target resolution, authorization, and admission at
/// every door, so the door's Cedar action is known before it is checked.
pub(super) fn classify(query: &str) -> std::result::Result<QueryFile, ApiError> {
    parse_query(query).map_err(|err| ApiError::from_compiler(&err))
}

pub(super) fn control_write_at_read_door(write: &BranchWrite) -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::CONTROL_WRITE_AT_READ_DOOR,
        write.statement_name(),
    ))
}

/// `commit publish` and `commit drop` are control writes: the refusal a
/// branch write meets at the read door, under the statement's own name.
pub(super) fn commit_write_at_read_door(write: &CommitWrite) -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::CONTROL_WRITE_AT_READ_DOOR,
        write.statement_name(),
    ))
}

/// `load csv` is a write: the refusal a branch write meets at the read
/// door, under the statement's own name.
pub(super) fn load_at_read_door(stmt: &LoadStmt) -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::CONTROL_WRITE_AT_READ_DOOR,
        stmt.statement_name(),
    ))
}

/// Run one `load csv` statement as a mutation body: the request `branch`
/// (defaulting to `main` only here) receives the load after the `Change`
/// check on it and admission; the answer counts the loaded nodes and edges
/// and carries the load's own publication.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_load_statement(
    state: &AppState,
    handle: &GraphRequest,
    session: &Session,
    actor: Option<&AuthenticatedActor>,
    ingress: IngressLease,
    stmt: LoadStmt,
    branch: Option<String>,
    source_len: usize,
) -> std::result::Result<ChangeOutput, ApiError> {
    let branch = branch.unwrap_or_else(|| "main".to_string());
    authorize_request(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action: PolicyAction::Change,
            branch: Some(branch.clone()),
            target_branch: None,
        },
    )?;
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let retained_input_bytes =
        (source_len as u64).saturating_add(session.settings().load_source_max_bytes());
    let admission = state
        .workload
        .try_admit(&actor_arc, retained_input_bytes)
        .map_err(ApiError::from_workload_reject)?;
    let actor_id = actor.map(|actor| actor.actor_id.as_ref().to_string());
    let query_name = stmt.statement_name().to_string();
    let request = CsvLoadRequest::from_statement(&stmt);
    let session = session.clone();
    owned_write(state, admission, ingress, handle.clone(), async move {
        let receipt = session
            .load_csv_as_with_receipt(&branch, &request, actor_id.as_deref())
            .await
            .map_err(ApiError::from_omni)?;
        Ok(ChangeOutput {
            branch: receipt.result.branch,
            query_name,
            affected_nodes: receipt.result.nodes_loaded.values().sum(),
            affected_edges: receipt.result.edges_loaded.values().sum(),
            actor_id,
            commit: Some(api::commit_output(&receipt.commit)),
            outcome: None,
        })
    })
    .await
}

pub(super) fn read_at_write_door(statement_name: &str) -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::READ_AT_WRITE_DOOR,
        statement_name,
    ))
}

/// `show` is a read: the same refusal `branch list` meets at the write door.
pub(super) fn show_at_write_door(id: Option<SettingId>) -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::READ_AT_WRITE_DOOR,
        &show_statement_name(id),
    ))
}

/// `Query` serves only branch reads; `Mutate` serves only control writes.
pub(super) fn refuse_wrong_door(
    door: Door,
    stmt: &BranchStmt,
) -> std::result::Result<(), ApiError> {
    match (door, stmt) {
        (Door::Query, BranchStmt::Write(write)) => Err(control_write_at_read_door(write)),
        (Door::Mutate, BranchStmt::List | BranchStmt::Read(_)) => {
            Err(read_at_write_door(stmt.statement_name()))
        }
        (Door::Query, BranchStmt::List | BranchStmt::Read(_))
        | (Door::Mutate, BranchStmt::Write(_)) => Ok(()),
    }
}

/// Answer a branch control read with the body of the route it shares: the
/// same authorization, engine call and error mapping as `GET /diff`,
/// `GET /branches/diff`, `GET /branches/merge-base` and the dry run of
/// `POST /branches/merge`.
pub(super) async fn branch_read_body(
    handle: &GraphRequest,
    session: &Session,
    actor: Option<&AuthenticatedActor>,
    read: BranchRead,
) -> std::result::Result<ReadOutput, ApiError> {
    match read {
        BranchRead::Diff { sides, page } => {
            let scope = omnigraph::changes::ChangeFeedScope::default();
            let diff = match sides {
                BranchDiffSides::Branch { source, into } => {
                    branch_diff_page(
                        handle,
                        actor,
                        &source,
                        into.as_deref(),
                        scope,
                        page.after_page,
                        page.limit,
                    )
                    .await?
                }
                BranchDiffSides::Between { from, to } => {
                    two_point_diff_page(handle, actor, from, to, scope, page.after_page, page.limit)
                        .await?
                }
            };
            api::branch_diff_read_output(&diff).map_err(render_error)
        }
        BranchRead::MergeBase { source, into } => {
            let (source, target, base) =
                merge_base_body(handle, actor, &source, into.as_deref()).await?;
            api::branch_merge_base_read_output(&source, &target, &base).map_err(render_error)
        }
        BranchRead::MergeDryRun { source, into } => {
            let target = into.unwrap_or_else(|| "main".to_string());
            // A read of both branches: no merge admission, no write gate, no
            // `branch_merge` grant. Conflicts answer the 409 a merge does.
            authorize_branch_reads(actor, handle.policy.as_deref(), [&source, &target])?;
            let result = session
                .branch_merge_dry_run(&source, &target)
                .await
                .map_err(ApiError::from_omni)?;
            api::branch_merge_dry_run_read_output(&source, &target, result.outcome.into())
                .map_err(render_error)
        }
    }
}

/// An `explain` statement is served at `Query` alone: a read refused at
/// `Mutate` as `branch list` is.
pub(super) fn refuse_explain(door: Door) -> std::result::Result<(), ApiError> {
    match door {
        Door::Query => Ok(()),
        Door::Mutate => Err(explain_at_write_door()),
    }
}

/// `explain` is a read: the same refusal `branch list` meets at the write door.
pub(super) fn explain_at_write_door() -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::READ_AT_WRITE_DOOR,
        EXPLAIN_STATEMENT_NAME,
    ))
}

/// A branch statement names every branch it acts on, so a request target, a
/// query name or parameters, or an expected head beside one is refused: two
/// sources for one fact are never reconciled.
pub(super) fn refuse_statement_envelope(
    has_target: bool,
    has_name_or_params: bool,
    has_expected_head: bool,
) -> std::result::Result<(), ApiError> {
    if has_target {
        return Err(ApiError::bad_request(refusals::REQUEST_TARGET));
    }
    if has_name_or_params {
        return Err(ApiError::bad_request(refusals::NAME_OR_PARAMS));
    }
    if has_expected_head {
        return Err(ApiError::bad_request(refusals::COMMIT_PRECONDITION));
    }
    Ok(())
}

/// Run one control write through its `/branches` route body and answer the
/// `ChangeOutput` of a mutation body: `branch` received the effect, both
/// counts are `0`, `commit` is this merge's own publication, or null for a no-op.
pub(super) async fn run_branch_statement(
    state: &AppState,
    handle: &GraphRequest,
    session: &Session,
    actor: Option<&AuthenticatedActor>,
    ingress: IngressLease,
    write: BranchWrite,
) -> std::result::Result<ChangeOutput, ApiError> {
    let query_name = write.statement_name().to_string();
    let actor_id = actor.map(|actor| actor.actor_id.as_ref().to_string());
    let (branch, commit, outcome) = match write {
        BranchWrite::Create { name, from } => {
            let start = match from {
                Some(BranchCreateFrom::Commit(commit)) => {
                    ReadTarget::snapshot(SnapshotId::new(commit))
                }
                Some(BranchCreateFrom::Branch(branch)) => ReadTarget::branch(branch),
                None => ReadTarget::branch("main"),
            };
            let created = branch_create_body(state, handle, actor, ingress, start, &name).await?;
            (
                name.clone(),
                None,
                api::BranchOutcomeOutput::Created {
                    from: created.from,
                    name,
                    from_commit_id: created.from_commit_id,
                },
            )
        }
        BranchWrite::Delete { name } => {
            branch_delete_body(state, handle, actor, ingress, &name).await?;
            (
                name.clone(),
                None,
                api::BranchOutcomeOutput::Deleted { name },
            )
        }
        BranchWrite::Merge {
            source,
            into,
            as_commit,
        } => {
            let target = into.unwrap_or_else(|| "main".to_string());
            let result = branch_merge_body(
                state, handle, session, actor, ingress, &source, &target, as_commit,
            )
            .await?;
            let merge = result.outcome.into();
            let commit = result.commit.as_ref().map(api::commit_output);
            (
                target.clone(),
                commit,
                api::BranchOutcomeOutput::Merged {
                    source,
                    target,
                    merge,
                },
            )
        }
    };
    Ok(ChangeOutput {
        branch,
        query_name,
        affected_nodes: 0,
        affected_edges: 0,
        actor_id,
        commit,
        outcome: Some(outcome),
    })
}

/// Run `commit publish` or `commit drop` as a mutation body: `branch` is the
/// proposal's recorded target, counts are `0`, `commit` is the published commit
/// or null after a drop; a policy denial answers the 404 an unknown id yields.
pub(super) async fn run_commit_statement(
    state: &AppState,
    handle: &GraphRequest,
    session: &Session,
    actor: Option<&AuthenticatedActor>,
    ingress: IngressLease,
    write: CommitWrite,
) -> std::result::Result<ChangeOutput, ApiError> {
    let query_name = write.statement_name().to_string();
    let actor_id = actor.map(|actor| actor.actor_id.as_ref().to_string());
    let CommitControlOutcome {
        target,
        commit,
        outcome,
    } = commit_control_body(state, handle, session, actor, ingress, write).await?;
    Ok(ChangeOutput {
        branch: target,
        query_name,
        affected_nodes: 0,
        affected_edges: 0,
        actor_id,
        commit,
        outcome: Some(outcome),
    })
}

/// What a commit control write answered: the proposal's recorded target, the
/// published commit (null after a drop), the outcome kind.
pub(super) struct CommitControlOutcome {
    pub(super) target: String,
    pub(super) commit: Option<api::CommitOutput>,
    pub(super) outcome: api::BranchOutcomeOutput,
}

/// Body shared by `POST /commits/{id}/publish`, `DELETE /commits/{id}` and
/// the `commit publish` / `commit drop` statements: the target from the
/// commit itself, the `commit_publish` / `commit_drop` check on it (a denial
/// answers the 404 an unknown id yields), admission, the engine call.
pub(super) async fn commit_control_body(
    state: &AppState,
    handle: &GraphRequest,
    session: &Session,
    actor: Option<&AuthenticatedActor>,
    ingress: IngressLease,
    write: CommitWrite,
) -> std::result::Result<CommitControlOutcome, ApiError> {
    let commit_id = write.graph_commit_id().to_string();
    let target = session
        .commit_target_branch(&commit_id)
        .await
        .map_err(|error| change_route_commit_lookup_error(error, &commit_id))?;
    let action = match &write {
        CommitWrite::Publish { .. } => PolicyAction::CommitPublish,
        CommitWrite::Drop { .. } => PolicyAction::CommitDrop,
    };
    match authorize(
        actor,
        handle.policy.as_deref(),
        PolicyRequest {
            action,
            branch: None,
            target_branch: Some(target.clone()),
        },
    )? {
        Authz::Allowed => {}
        Authz::Denied(_) => {
            return Err(ApiError::not_found(format!(
                "commit '{commit_id}' not found"
            )));
        }
    }
    let actor_arc = actor
        .map(|actor| Arc::clone(&actor.actor_id))
        .unwrap_or_else(|| Arc::<str>::from("anonymous"));
    let admission = state
        .workload
        .try_admit(&actor_arc, 256)
        .map_err(ApiError::from_workload_reject)?;
    let caller = actor.map(|actor| actor.actor_id.as_ref().to_string());
    let session = session.clone();
    let (commit, outcome) = owned_write(state, admission, ingress, handle.clone(), async move {
        match write {
            CommitWrite::Publish { graph_commit_id } => {
                let published = session
                    .publish_commit_as(&graph_commit_id, caller.as_deref())
                    .await
                    .map_err(ApiError::from_omni)?;
                Ok((
                    Some(api::commit_output(&published)),
                    api::BranchOutcomeOutput::Published,
                ))
            }
            CommitWrite::Drop { graph_commit_id } => {
                session
                    .drop_commit_as(&graph_commit_id, caller.as_deref())
                    .await
                    .map_err(ApiError::from_omni)?;
                Ok((None, api::BranchOutcomeOutput::Dropped { graph_commit_id }))
            }
        }
    })
    .await?;
    Ok(CommitControlOutcome {
        target,
        commit,
        outcome,
    })
}
