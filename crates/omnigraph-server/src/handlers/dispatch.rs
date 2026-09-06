//! Branch-statement dispatch behind `run_query` and `run_mutate`: the door a
//! source arrived through, the refusals a statement meets at the wrong door
//! or with a request envelope, and the control-write runner that answers as
//! a mutation body does.

use super::*;
use crate::api::branch_statement_refusals as refusals;
use omnigraph_compiler::query::ast::{BranchStmt, BranchWrite};

/// The HTTP entry a GQ source arrived through. A branch statement is served
/// only at its own door: `Query` serves `branch list`, `Mutate` serves
/// `branch create`, `branch delete`, and `branch merge`, and the deprecated
/// `Read` and `Change` serve none. `Read` is also the one door that runs a
/// mutation body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Door {
    Query,
    Read,
    Mutate,
    Change,
}

/// What [`run_query`] answered: a declared query's rows, or the branch names
/// of a `branch list` statement sorted in byte order.
pub(crate) enum ReadDispatch {
    Rows {
        query_name: String,
        target: ReadTarget,
        result: omnigraph_compiler::result::QueryResult,
        graph_commit_id: Option<String>,
    },
    BranchList(Vec<String>),
}

impl ReadDispatch {
    /// Render either answer as the `ReadOutput` every read route serves.
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
        }
    }
}

/// Parse the source into its kind: `query` declarations or one branch
/// statement. Runs before target resolution, authorization, and admission at
/// every door, so the door's Cedar action is known before it is checked.
pub(super) fn classify(query: &str) -> std::result::Result<QueryFile, ApiError> {
    parse_query(query).map_err(|err| ApiError::bad_request(err.to_string()))
}

pub(super) fn control_write_at_read_door(write: &BranchWrite) -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::CONTROL_WRITE_AT_READ_DOOR,
        write.statement_name(),
    ))
}

pub(super) fn read_at_write_door() -> ApiError {
    ApiError::bad_request(refusals::with_statement(
        refusals::READ_AT_WRITE_DOOR,
        BranchStmt::List.statement_name(),
    ))
}

/// A branch statement is refused at every door that does not serve it: the two
/// deprecated routes serve none, `Query` serves only the read, `Mutate` only
/// the control writes.
pub(super) fn refuse_wrong_door(
    door: Door,
    stmt: &BranchStmt,
) -> std::result::Result<(), ApiError> {
    match (door, stmt) {
        (Door::Read | Door::Change, _) => Err(ApiError::bad_request(refusals::DEPRECATED_ROUTE)),
        (Door::Query, BranchStmt::Write(write)) => Err(control_write_at_read_door(write)),
        (Door::Mutate, BranchStmt::List) => Err(read_at_write_door()),
        (Door::Query, BranchStmt::List) | (Door::Mutate, BranchStmt::Write(_)) => Ok(()),
    }
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
/// counts are `0`, `commit` is the target's head after a publishing merge.
pub(super) async fn run_branch_statement(
    state: &AppState,
    handle: &GraphHandle,
    actor: Option<&ResolvedActor>,
    write: BranchWrite,
) -> std::result::Result<ChangeOutput, ApiError> {
    let query_name = write.statement_name().to_string();
    let actor_id = actor.map(|actor| actor.actor_id.as_ref().to_string());
    let (branch, commit, outcome) = match write {
        BranchWrite::Create { name, from } => {
            let from = from.unwrap_or_else(|| "main".to_string());
            branch_create_body(state, handle, actor, &from, &name).await?;
            (
                name.clone(),
                None,
                api::BranchOutcomeOutput::Created { from, name },
            )
        }
        BranchWrite::Delete { name } => {
            branch_delete_body(state, handle, actor, &name).await?;
            (
                name.clone(),
                None,
                api::BranchOutcomeOutput::Deleted { name },
            )
        }
        BranchWrite::Merge { source, into } => {
            let target = into.unwrap_or_else(|| "main".to_string());
            let merge: api::BranchMergeOutcome =
                branch_merge_body(state, handle, actor, &source, &target)
                    .await?
                    .into();
            let commit = match merge {
                api::BranchMergeOutcome::AlreadyUpToDate => None,
                api::BranchMergeOutcome::FastForward | api::BranchMergeOutcome::Merged => handle
                    .engine
                    .list_commits(Some(&target))
                    .await
                    .ok()
                    .and_then(|commits| commits.first().map(api::commit_output)),
            };
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
