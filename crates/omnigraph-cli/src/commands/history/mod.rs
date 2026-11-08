//! Graph history, branches, snapshots and bounded change traversal.
use crate::client::{self, GraphClient};
use crate::helpers::resolve_branch;
use crate::output::{
    DiffHumanStream, DiffJsonStream, write_diff_human, write_json, write_snapshot_human,
};
use color_eyre::eyre::Result;
use std::io::{self, Write};

mod args;
mod branch;
pub(crate) mod changes;
mod commit;
mod prepare;

pub(crate) use args::{BranchCommand, ChangesCommand, CommitCommand, DiffArgs, SnapshotArgs};
pub(crate) use branch::run_branch;
pub(crate) use changes::run_changes;
pub(crate) use commit::run_commit;
use prepare::{ChangeFilter, Pagination};

/// Only the existing global arguments this command family consumes.
#[derive(Clone, Copy)]
pub(crate) struct HistoryScope<'a> {
    pub(crate) operator: &'a crate::operator::InvocationConfig,
    pub target: &'a crate::targets::ResolvedTarget,
    pub graph: Option<&'a str>,
    pub actor: Option<&'a str>,
    pub yes: bool,
}

pub(crate) async fn run_snapshot(
    args: SnapshotArgs,
    scope: HistoryScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let SnapshotArgs { branch, json } = args;
    let client =
        GraphClient::from_target(scope.operator, scope.target, scope.graph, None, stderr).await?;
    let branch = resolve_branch(branch, None, "main");
    let payload = client.snapshot(&branch).await?;
    if json {
        write_json(&payload, stdout)?;
    } else {
        write_snapshot_human(
            &payload.graph_branch,
            payload.graph_manifest_version,
            payload.internal_schema_version,
            &payload.datasets,
            stdout,
        )?;
    }
    Ok(())
}

pub(crate) async fn run_diff_command(
    args: DiffArgs,
    scope: HistoryScope<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    let DiffArgs {
        from,
        to,
        limit,
        page_token,
        kinds,
        types,
        ops,
        json,
    } = args;
    let client =
        GraphClient::from_target(scope.operator, scope.target, scope.graph, None, stderr).await?;
    let sides = client::DiffSidesArg::Between {
        from: &from,
        to: &to,
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
    Ok(())
}

/// Print a graph diff: one page when `page_token` is given (the raw escape
/// hatch), otherwise every page in turn, each emitted before the next is
/// fetched.
async fn run_diff(
    client: &GraphClient,
    sides: client::DiffSidesArg<'_>,
    pagination: Pagination,
    filter: ChangeFilter,
    json: bool,
    stdout: &mut dyn Write,
) -> Result<()> {
    let filter = filter.as_args();
    if let Pagination::One { token, limit } = &pagination {
        let page = client
            .diff_page(sides, Some(token), *limit, &filter)
            .await?;
        if json {
            write_json(&page, stdout)?;
        } else {
            write_diff_human(&page, stdout)?;
        }
        return Ok(());
    }
    let mut next_page_token: Option<String> = None;
    if json {
        let mut output = DiffJsonStream::new(io::BufWriter::new(&mut *stdout));
        loop {
            let page = client
                .diff_page(
                    sides,
                    next_page_token.as_deref(),
                    pagination.limit(),
                    &filter,
                )
                .await?;
            next_page_token = page.next_page_token.clone();
            output.write_page(&page)?;
            if next_page_token.is_none() {
                break;
            }
        }
        let _ = output.finish()?;
    } else {
        let mut output = DiffHumanStream::new(&mut *stdout);
        loop {
            let page = client
                .diff_page(
                    sides,
                    next_page_token.as_deref(),
                    pagination.limit(),
                    &filter,
                )
                .await?;
            next_page_token = page.next_page_token.clone();
            output.write_page(&page)?;
            if next_page_token.is_none() {
                break;
            }
        }
    }
    Ok(())
}
