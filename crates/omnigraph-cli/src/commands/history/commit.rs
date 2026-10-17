//! Commit reads and proposal publication/drop controls.
use super::prepare::{ChangeFilter, CommitListSelection, Pagination};
use super::{CommitCommand, HistoryScope};
use crate::client::{self, GraphClient};
use crate::helpers::{confirm_destructive_with_writer, write_target};
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
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> Result<()> {
    match command {
        CommitCommand::List {
            uri,
            branch,
            unpublished,
            unattached,
            json,
        } => {
            let client = match managed_client {
                Some(client) => client,
                None => {
                    GraphClient::resolve(
                        scope.capability,
                        scope.server,
                        scope.graph,
                        uri,
                        scope.profile,
                        scope.store,
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
                CommitListSelection::Published => client.list_commits(branch.as_deref()).await?,
            };
            if json {
                write_json(&payload, stdout)?;
            } else {
                write_commit_list_human(&payload.commits, stdout)?;
            }
        }
        CommitCommand::Publish {
            uri,
            commit_id,
            json,
        } => {
            let client = GraphClient::resolve_with_policy(
                scope.capability,
                scope.server,
                scope.graph,
                uri,
                scope.actor,
                scope.profile,
                scope.store,
            )
            .await?;
            write_target(
                scope.quiet,
                "commit publish",
                client.uri(),
                client.is_remote(),
                stderr,
            )?;
            let commit = client.commit_publish(&commit_id).await?;
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
        CommitCommand::Drop {
            uri,
            commit_id,
            json,
        } => {
            let client = GraphClient::resolve_with_policy(
                scope.capability,
                scope.server,
                scope.graph,
                uri,
                scope.actor,
                scope.profile,
                scope.store,
            )
            .await?;
            confirm_destructive_with_writer("commit drop", client.uri(), scope.yes, json, stderr)?;
            write_target(
                scope.quiet,
                "commit drop",
                client.uri(),
                client.is_remote(),
                stderr,
            )?;
            client.commit_drop(&commit_id).await?;
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
        CommitCommand::Show {
            uri,
            commit_id,
            json,
        } => {
            let client = match managed_client {
                Some(client) => client,
                None => {
                    GraphClient::resolve(
                        scope.capability,
                        scope.server,
                        scope.graph,
                        uri,
                        scope.profile,
                        scope.store,
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
            uri,
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
            let client = GraphClient::resolve(
                scope.capability,
                scope.server,
                scope.graph,
                uri,
                scope.profile,
                scope.store,
            )
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
