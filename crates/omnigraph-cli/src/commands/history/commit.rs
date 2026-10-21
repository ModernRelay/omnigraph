//! Commit reads and proposal publication/drop controls.
use super::prepare::{ChangeFilter, CommitListSelection, Pagination};
use super::{CommitCommand, HistoryScope};
use crate::client::{self, GraphClient};
use crate::helpers::confirm_destructive;
use crate::output::{
    CommitChangesHumanStream, CommitChangesJsonStream, write_commit_changes_human,
    write_commit_human, write_commit_list_human, write_json,
};
use color_eyre::eyre::Result;
use std::io::{self, Write};

pub(crate) async fn run_commit(
    command: CommitCommand,
    scope: HistoryScope<'_>,
    managed_client: Option<GraphClient>,
    input: &mut crate::input::Input<'_>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    match command {
        CommitCommand::List {
            branch,
            unpublished,
            unattached,
            limit,
            page_token,
            json,
        } => {
            let client = match managed_client {
                Some(client) => client,
                None => {
                    GraphClient::from_target(
                        scope.operator,
                        scope.target,
                        scope.graph,
                        None,
                        stderr,
                    )
                    .await?
                }
            };
            let payload = match CommitListSelection::from_args(unpublished, unattached) {
                CommitListSelection::Unattached => {
                    client.list_unattached_commits(branch.as_deref()).await?
                }
                CommitListSelection::Unpublished => {
                    client.list_unpublished_commits(branch.as_deref()).await?
                }
                CommitListSelection::Published if limit.is_some() || page_token.is_some() => {
                    client
                        .list_commits_page(branch.as_deref(), page_token.as_deref(), limit)
                        .await?
                }
                CommitListSelection::Published => client.list_commits(branch.as_deref()).await?,
            };
            if json {
                write_json(&payload, stdout)?;
            } else {
                write_commit_list_human(&payload, stdout)?;
            }
        }
        CommitCommand::Publish { commit_id, json } => {
            let client = GraphClient::from_target(
                scope.operator,
                scope.target,
                scope.graph,
                scope.actor,
                stderr,
            )
            .await?;
            let commit = client.commit_publish(&commit_id, stderr).await?;
            if json {
                write_json(&commit, stdout)?;
            } else {
                writeln!(
                    stdout,
                    "published commit {} on {}",
                    commit.graph_commit_id,
                    commit.graph_branch.as_deref().unwrap_or("main")
                )?;
            }
        }
        CommitCommand::Drop { commit_id, json } => {
            let client = GraphClient::from_target(
                scope.operator,
                scope.target,
                scope.graph,
                scope.actor,
                stderr,
            )
            .await?;
            confirm_destructive("commit drop", client.uri(), scope.yes, json, input, stderr)?;
            client.commit_drop(&commit_id, stderr).await?;
            if json {
                write_json(
                    &serde_json::json!({
                        "uri": client.uri(),
                        "graph_commit_id": commit_id,
                    }),
                    stdout,
                )?;
            } else {
                writeln!(stdout, "dropped commit {commit_id}")?;
            }
        }
        CommitCommand::Show { commit_id, json } => {
            let client = match managed_client {
                Some(client) => client,
                None => {
                    GraphClient::from_target(
                        scope.operator,
                        scope.target,
                        scope.graph,
                        None,
                        stderr,
                    )
                    .await?
                }
            };
            let commit = client.get_commit(&commit_id).await?;
            if json {
                write_json(&commit, stdout)?;
            } else {
                write_commit_human(&commit, stdout)?;
            }
        }
        CommitCommand::Changes {
            commit_id,
            limit,
            page_token,
            kinds,
            types,
            ops,
            settings,
            json,
        } => {
            let settings = client::parse_set_flags(&settings)?;
            let client =
                GraphClient::from_target(scope.operator, scope.target, scope.graph, None, stderr)
                    .await?;
            let filter = ChangeFilter::from_args(kinds, types, ops);
            let filter = filter.as_args();
            let pagination = Pagination::from_args(page_token, limit);
            if let Pagination::One { token, limit } = &pagination {
                // An explicit token is the raw one-page escape hatch.
                let page = client
                    .commit_changes_page(&commit_id, Some(token), *limit, &filter, &settings)
                    .await?;
                if json {
                    write_json(&page, stdout)?;
                } else {
                    write_commit_changes_human(&page, stdout)?;
                }
            } else if json {
                let mut output = CommitChangesJsonStream::new(io::BufWriter::new(&mut *stdout));
                let mut next_page_token: Option<String> = None;
                loop {
                    let page = client
                        .commit_changes_page(
                            &commit_id,
                            next_page_token.as_deref(),
                            pagination.limit(),
                            &filter,
                            &settings,
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
                let mut output = CommitChangesHumanStream::new(&mut *stdout);
                let mut next_page_token: Option<String> = None;
                loop {
                    let page = client
                        .commit_changes_page(
                            &commit_id,
                            next_page_token.as_deref(),
                            pagination.limit(),
                            &filter,
                            &settings,
                        )
                        .await?;
                    next_page_token = page.next_page_token.clone();
                    output.write_page(&page)?;
                    if next_page_token.is_none() {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}
