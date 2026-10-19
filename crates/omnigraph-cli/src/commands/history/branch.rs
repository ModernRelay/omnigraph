//! Branch reads and controls, preserving the client's policy and merge outcomes.
use super::prepare::{ChangeFilter, Pagination};
use super::{BranchCommand, HistoryScope, run_diff};
use crate::client::{self, GraphClient};
use crate::helpers::{confirm_destructive, resolve_branch};
use crate::output::{write_json, write_merge_base_human};
use color_eyre::eyre::Result;
use std::io::Write;

pub(crate) async fn run_branch(
    command: BranchCommand,
    scope: HistoryScope<'_>,
    input: &mut crate::input::Input<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    match command {
        BranchCommand::Create { from, name, json } => {
            let client = GraphClient::from_target(
                scope.operator,
                scope.target,
                scope.graph,
                scope.actor,
                stderr,
            )
            .await?;
            let from = resolve_branch(from, None, "main");
            let payload = client.branch_create_from(&from, &name, stderr).await?;
            if json {
                write_json(&payload, stdout)?;
            } else {
                writeln!(
                    stdout,
                    "created branch {} from {}",
                    payload.name, payload.from
                )?;
            }
        }
        BranchCommand::List { json } => {
            let client =
                GraphClient::from_target(scope.operator, scope.target, scope.graph, None, stderr)
                    .await?;
            let payload = client.branch_list().await?;
            if json {
                write_json(&payload, stdout)?;
            } else {
                for branch in payload.branches {
                    writeln!(stdout, "{}", branch)?;
                }
            }
        }
        BranchCommand::Delete { name, json } => {
            let client = GraphClient::from_target(
                scope.operator,
                scope.target,
                scope.graph,
                scope.actor,
                stderr,
            )
            .await?;
            confirm_destructive(
                "branch delete",
                client.uri(),
                scope.yes,
                json,
                input,
                stderr,
            )?;
            let payload = client.branch_delete(&name, stderr).await?;
            if json {
                write_json(&payload, stdout)?;
            } else {
                writeln!(stdout, "deleted branch {}", payload.name)?;
            }
        }
        BranchCommand::Merge {
            source,
            into,
            delete_branch,
            as_commit,
            dry_run,
            settings,
            json,
        } => {
            let settings = client::parse_set_flags(&settings)?;
            let client = GraphClient::from_target(
                scope.operator,
                scope.target,
                scope.graph,
                scope.actor,
                stderr,
            )
            .await?;
            let into = resolve_branch(into, None, "main");
            let payload = client
                .branch_merge(
                    &source,
                    &into,
                    delete_branch,
                    as_commit,
                    dry_run,
                    &settings,
                    stderr,
                )
                .await?;
            // Keep the successful merge receipt on stdout; optional
            // deletion has its own structured result and human warning.
            if let Some(error) = &payload.branch_delete_error_details {
                writeln!(
                    stderr,
                    "warning: merged, but could not delete branch '{}': {}",
                    payload.source, error.error
                )?;
            }
            if json {
                write_json(&payload, stdout)?;
            } else if payload.dry_run {
                writeln!(
                    stdout,
                    "dry run: merging {} into {} would be: {} (nothing was written)",
                    payload.source,
                    payload.target,
                    payload.outcome.as_str()
                )?;
            } else {
                match payload.commit.as_ref().filter(|commit| !commit.published) {
                    Some(commit) => writeln!(
                        stdout,
                        "merged {} into {} as commit {}: {}",
                        payload.source,
                        payload.target,
                        commit.graph_commit_id,
                        payload.outcome.as_str()
                    )?,
                    None => writeln!(
                        stdout,
                        "merged {} into {}: {}",
                        payload.source,
                        payload.target,
                        payload.outcome.as_str()
                    )?,
                }
                if payload.branch_deleted == Some(true) {
                    writeln!(stdout, "deleted branch {}", payload.source)?;
                }
            }
        }
        BranchCommand::Diff {
            source,
            into,
            limit,
            page_token,
            kinds,
            types,
            ops,
            json,
        } => {
            let client =
                GraphClient::from_target(scope.operator, scope.target, scope.graph, None, stderr)
                    .await?;
            let into = resolve_branch(into, None, "main");
            let sides = client::DiffSidesArg::Branch {
                source: &source,
                target: &into,
            };
            run_diff(
                &client,
                sides,
                Pagination::from_args(page_token, limit),
                ChangeFilter::from_args(kinds, types, ops),
                json,
                stdout,
            )
            .await?;
        }
        BranchCommand::MergeBase { source, into, json } => {
            let client =
                GraphClient::from_target(scope.operator, scope.target, scope.graph, None, stderr)
                    .await?;
            let into = resolve_branch(into, None, "main");
            let output = client.merge_base(&source, &into).await?;
            if json {
                write_json(&output, stdout)?;
            } else {
                write_merge_base_human(&output, stdout)?;
            }
        }
    }
    Ok(())
}
