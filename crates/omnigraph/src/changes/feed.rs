//! The durable first-parent change feed.
//!
//! A poll walks one captured branch incarnation's first-parent chain from the
//! caller's position toward the captured head (the cut), emitting one
//! [`GraphChangeBlock`] per commit through the shared per-commit enumerator.
//! The durable cursor advances only over complete commits: a response that
//! ends inside a block carries a page token and no cursor, so a caller can
//! never checkpoint a partial block. The server persists no consumer state —
//! every position lives in the caller-owned opaque cursor.

use std::collections::HashMap;

use super::enumerate::{
    self, CommitEnumeration, ContinuationKey, PageBudget, enumerate_commit_changes,
};
use super::model::{
    CHANGE_FEED_DEFAULT_COMMITS_PER_POLL, CHANGE_FEED_MAX_COMMITS_PER_POLL,
    COMMIT_CHANGES_DEFAULT_BYTES, COMMIT_CHANGES_DEFAULT_ROWS, ChangeCause, ChangeFeedContinuation,
    ChangeFeedPage, ChangeFeedPosition, ChangeFeedRequest, ChangeFeedStart, GraphChangeBlock,
    GraphEntityChange,
};
use super::token::{
    self, FEED_PURPOSE, FeedCursorV1, FeedPageTokenV1, KIND_FEED_CURSOR, KIND_FEED_PAGE,
    cursor_rejected,
};
use crate::db::commit_graph::GraphCommit;
use crate::db::manifest::{ManifestCoordinator, Snapshot};
use crate::error::{OmniError, Result};
use crate::table_store::TableStore;

/// One coherent capture of a branch for feed derivation: head, incarnation
/// witness, genesis, and the full lineage projection.
pub(crate) struct ChangeFeedCut {
    pub(crate) branch: Option<String>,
    pub(crate) head: String,
    pub(crate) witness: String,
    pub(crate) genesis: String,
    pub(crate) commits: HashMap<String, GraphCommit>,
}

impl ChangeFeedCut {
    /// The first-parent chain strictly after `after`, from `from` (inclusive)
    /// down, returned oldest-first. `None` when `after` is not an ancestor of
    /// (or equal to) `from` on this chain.
    fn chain_after(&self, from: &str, after: &str) -> Option<Vec<String>> {
        let mut chain = Vec::new();
        let mut cursor = from.to_string();
        loop {
            if cursor == after {
                chain.reverse();
                return Some(chain);
            }
            let commit = self.commits.get(&cursor)?;
            chain.push(cursor.clone());
            cursor = commit.parent_commit_id.clone()?;
        }
    }

    fn is_on_chain(&self, commit_id: &str) -> bool {
        self.chain_after(&self.head, commit_id).is_some()
    }
}

struct FeedScope {
    graph_identity: String,
    genesis: String,
    branch: Option<String>,
    witness: String,
    filter_digest: String,
}

impl FeedScope {
    fn mint_cursor(&self, after_commit_id: &str) -> Result<String> {
        token::encode_token(&FeedCursorV1 {
            version: token::TOKEN_VERSION,
            kind: KIND_FEED_CURSOR.to_string(),
            graph_identity: self.graph_identity.clone(),
            genesis_commit_id: self.genesis.clone(),
            purpose: FEED_PURPOSE.to_string(),
            branch: self.branch.clone(),
            branch_witness: self.witness.clone(),
            filter_digest: self.filter_digest.clone(),
            after_commit_id: after_commit_id.to_string(),
        })
    }

    fn mint_page_token(
        &self,
        cut_commit_id: &str,
        after_commit_id: &str,
        current_commit_id: &str,
        key: &ContinuationKey,
    ) -> Result<String> {
        token::encode_token(&FeedPageTokenV1 {
            version: token::TOKEN_VERSION,
            kind: KIND_FEED_PAGE.to_string(),
            graph_identity: self.graph_identity.clone(),
            genesis_commit_id: self.genesis.clone(),
            purpose: FEED_PURPOSE.to_string(),
            branch: self.branch.clone(),
            branch_witness: self.witness.clone(),
            filter_digest: self.filter_digest.clone(),
            cut_commit_id: cut_commit_id.to_string(),
            after_commit_id: after_commit_id.to_string(),
            current_commit_id: current_commit_id.to_string(),
            stable_table_id: key.identity.stable_table_id,
            table_incarnation_id: key.identity.table_incarnation_id,
            id: key.id.clone(),
            operation_rank: key.operation_rank,
            change_index: key.change_index,
        })
    }

    /// Validate the shared scope fields every feed continuation binds.
    fn validate(
        &self,
        graph_identity: &str,
        genesis: &str,
        branch: &Option<String>,
        witness: &str,
        filter_digest: &str,
    ) -> Result<()> {
        if graph_identity != self.graph_identity {
            return Err(cursor_rejected(
                "change feed continuation does not belong to this graph",
            ));
        }
        if genesis != self.genesis {
            return Err(cursor_rejected(
                "change feed continuation was minted for a different graph history",
            ));
        }
        if branch != &self.branch {
            return Err(cursor_rejected(
                "change feed continuation was minted for a different branch",
            ));
        }
        if witness != self.witness {
            return Err(cursor_rejected(
                "change feed continuation was minted for a different branch incarnation",
            ));
        }
        if filter_digest != self.filter_digest {
            return Err(cursor_rejected(
                "change feed continuation was minted for a different filter scope",
            ));
        }
        Ok(())
    }
}

/// Mint a durable cursor positioned after `after_commit_id` in this cut and
/// scope — the baseline handshake's `AfterCommit(H)` equivalent.
pub(crate) fn mint_cursor_after(
    graph_identity: &str,
    cut: &ChangeFeedCut,
    scope: &super::model::ChangeFeedScope,
    after_commit_id: &str,
) -> Result<String> {
    FeedScope {
        graph_identity: token::hashed_identity(graph_identity),
        genesis: cut.genesis.clone(),
        branch: cut.branch.clone(),
        witness: cut.witness.clone(),
        filter_digest: token::filter_digest(scope),
    }
    .mint_cursor(after_commit_id)
}

/// Where this poll starts, after position validation.
struct ResolvedPosition {
    /// Head of this poll's cut (commits after it stay outside the page).
    cut_commit_id: String,
    /// Last complete commit already consumed.
    after_commit_id: String,
    /// In-commit resume for an interrupted poll.
    resume: Option<(String, ContinuationKey)>,
    /// The original durable cursor, echoed into a typed gap when the very
    /// first commit of the poll is unreadable.
    durable_cursor: Option<String>,
}

/// One poll of the change feed against one captured cut.
pub(crate) async fn poll(
    root_uri: &str,
    store: &TableStore,
    graph_identity: &str,
    cut: &ChangeFeedCut,
    request: &ChangeFeedRequest,
) -> Result<ChangeFeedPage> {
    let max_changes = request.max_changes.unwrap_or(COMMIT_CHANGES_DEFAULT_ROWS);
    let max_bytes = request.max_bytes.unwrap_or(COMMIT_CHANGES_DEFAULT_BYTES);
    enumerate::validate_change_page_limits(max_changes, max_bytes)?;
    let max_commits = request
        .max_commits
        .unwrap_or(CHANGE_FEED_DEFAULT_COMMITS_PER_POLL);
    if max_commits == 0 {
        return Err(OmniError::manifest(
            "change feed commit ceiling must be greater than zero",
        ));
    }
    if max_commits > CHANGE_FEED_MAX_COMMITS_PER_POLL {
        return Err(OmniError::resource_limit(
            "change_feed_commits_per_poll",
            CHANGE_FEED_MAX_COMMITS_PER_POLL as u64,
            max_commits as u64,
        ));
    }

    let scope = FeedScope {
        graph_identity: token::hashed_identity(graph_identity),
        genesis: cut.genesis.clone(),
        branch: cut.branch.clone(),
        witness: cut.witness.clone(),
        filter_digest: token::filter_digest(&request.scope),
    };

    let position = resolve_position(&scope, cut, &request.position)?;
    let Some(chain) = cut.chain_after(&position.cut_commit_id, &position.after_commit_id) else {
        return Err(cursor_rejected(
            "change feed continuation names a commit outside this branch's first-parent chain",
        ));
    };
    if chain.is_empty() {
        return Ok(ChangeFeedPage {
            blocks: Vec::new(),
            continuation: ChangeFeedContinuation::AtBlockBoundary {
                cursor: scope.mint_cursor(&position.after_commit_id)?,
                caught_up: position.cut_commit_id == cut.head,
            },
        });
    }
    if let Some((current_commit_id, _)) = &position.resume {
        if chain.first() != Some(current_commit_id) {
            return Err(cursor_rejected(
                "change feed page token no longer names the next commit of its cut",
            ));
        }
    }

    let mut budget = PageBudget::new(max_changes, max_bytes);
    let mut blocks: Vec<GraphChangeBlock> = Vec::new();
    let mut after_completed = position.after_commit_id.clone();
    // The parent snapshot of chain[0] is the position commit's snapshot; each
    // child snapshot is then reused as the next edge's parent, so a poll costs
    // one manifest snapshot resolution per commit examined plus one.
    let mut parent_snapshot = commit_snapshot(root_uri, cut, &after_completed).await?;

    for (index, commit_id) in chain.iter().enumerate() {
        let commit = cut
            .commits
            .get(commit_id)
            .expect("chain commits come from the projection");
        let boundary_stop =
            |blocks: Vec<GraphChangeBlock>, after: &str| -> Result<ChangeFeedPage> {
                Ok(ChangeFeedPage {
                    blocks,
                    continuation: ChangeFeedContinuation::AtBlockBoundary {
                        cursor: scope.mint_cursor(after)?,
                        caught_up: false,
                    },
                })
            };
        if index == max_commits || budget.remaining_rows == 0 {
            return boundary_stop(blocks, &after_completed);
        }

        let child_snapshot = match commit_snapshot(root_uri, cut, commit_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return first_or_boundary(
                    error,
                    commit_id,
                    blocks,
                    &position,
                    &scope,
                    &after_completed,
                );
            }
        };
        let resume = position
            .resume
            .as_ref()
            .filter(|(current, _)| index == 0 && current == commit_id)
            .map(|(_, key)| key.clone());
        let mut changes: Vec<GraphEntityChange> = Vec::new();
        let outcome = enumerate_commit_changes(
            store,
            &parent_snapshot,
            &child_snapshot,
            graph_identity,
            commit_id,
            &request.scope,
            resume.as_ref(),
            &mut budget,
            &mut changes,
        )
        .await;
        let cause = ChangeCause::from(commit);
        match outcome {
            Ok(CommitEnumeration::Complete) => {
                blocks.push(GraphChangeBlock { cause, changes });
                after_completed = commit_id.clone();
                parent_snapshot = child_snapshot;
            }
            Ok(CommitEnumeration::Truncated(key)) => {
                let page_token = scope.mint_page_token(
                    &position.cut_commit_id,
                    &after_completed,
                    commit_id,
                    &key,
                )?;
                blocks.push(GraphChangeBlock { cause, changes });
                return Ok(ChangeFeedPage {
                    blocks,
                    continuation: ChangeFeedContinuation::MidBlock { page_token },
                });
            }
            Ok(CommitEnumeration::Exhausted { required_bytes }) => {
                if blocks.is_empty() && position.resume.is_none() {
                    // A fresh poll could not admit even one change: the single
                    // change exceeds the byte ceiling itself.
                    return Err(OmniError::resource_limit(
                        "commit_changes_page_bytes",
                        max_bytes,
                        required_bytes,
                    ));
                }
                return boundary_stop(blocks, &after_completed);
            }
            Err(error) => {
                return first_or_boundary(
                    error,
                    commit_id,
                    blocks,
                    &position,
                    &scope,
                    &after_completed,
                );
            }
        }
    }

    Ok(ChangeFeedPage {
        blocks,
        continuation: ChangeFeedContinuation::AtBlockBoundary {
            cursor: scope.mint_cursor(&after_completed)?,
            caught_up: position.cut_commit_id == cut.head,
        },
    })
}

/// A commit that cannot be derived (reclaimed history or a schema boundary)
/// surfaces as its typed error only when it is the FIRST commit of the poll;
/// otherwise the page ends atomically at the previous block boundary and the
/// next poll surfaces the typed error deterministically.
fn first_or_boundary(
    error: OmniError,
    commit_id: &str,
    blocks: Vec<GraphChangeBlock>,
    position: &ResolvedPosition,
    scope: &FeedScope,
    after_completed: &str,
) -> Result<ChangeFeedPage> {
    let typed = match error {
        OmniError::HistoricalVersionReclaimed { .. } => OmniError::ChangeFeedGap {
            cursor: position.durable_cursor.clone(),
            first_unreadable_commit_id: commit_id.to_string(),
        },
        error => error,
    };
    if blocks.is_empty() {
        return Err(typed);
    }
    match typed {
        OmniError::ChangeFeedGap { .. } | OmniError::ChangeSchemaBoundary { .. } => {
            Ok(ChangeFeedPage {
                blocks,
                continuation: ChangeFeedContinuation::AtBlockBoundary {
                    cursor: scope.mint_cursor(after_completed)?,
                    caught_up: false,
                },
            })
        }
        // Anything else (I/O, internal) fails the whole page: a partial page
        // is never published on an unclassified error.
        other => Err(other),
    }
}

async fn commit_snapshot(root_uri: &str, cut: &ChangeFeedCut, commit_id: &str) -> Result<Snapshot> {
    let commit = cut.commits.get(commit_id).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "lineage projection is missing commit '{commit_id}'"
        ))
    })?;
    ManifestCoordinator::snapshot_at(
        root_uri,
        commit.manifest_branch.as_deref(),
        commit.manifest_version,
    )
    .await
}

fn resolve_position(
    scope: &FeedScope,
    cut: &ChangeFeedCut,
    position: &ChangeFeedPosition,
) -> Result<ResolvedPosition> {
    match position {
        ChangeFeedPosition::Start(start) => {
            let after = match start {
                ChangeFeedStart::Now => cut.head.clone(),
                ChangeFeedStart::Beginning => cut.genesis.clone(),
                ChangeFeedStart::AfterCommit(commit_id) => {
                    if !cut.is_on_chain(commit_id) {
                        return Err(cursor_rejected(
                            "start commit is not on this branch's first-parent chain",
                        ));
                    }
                    commit_id.clone()
                }
            };
            Ok(ResolvedPosition {
                cut_commit_id: cut.head.clone(),
                after_commit_id: after,
                resume: None,
                durable_cursor: None,
            })
        }
        ChangeFeedPosition::Cursor(encoded) => {
            let cursor = token::decode_feed_cursor(encoded)?;
            scope.validate(
                &cursor.graph_identity,
                &cursor.genesis_commit_id,
                &cursor.branch,
                &cursor.branch_witness,
                &cursor.filter_digest,
            )?;
            if !cut.is_on_chain(&cursor.after_commit_id) {
                return Err(cursor_rejected(
                    "change feed cursor names a commit outside this branch's first-parent chain",
                ));
            }
            Ok(ResolvedPosition {
                cut_commit_id: cut.head.clone(),
                after_commit_id: cursor.after_commit_id,
                resume: None,
                durable_cursor: Some(encoded.clone()),
            })
        }
        ChangeFeedPosition::PageToken(encoded) => {
            let page = token::decode_feed_page_token(encoded)?;
            scope.validate(
                &page.graph_identity,
                &page.genesis_commit_id,
                &page.branch,
                &page.branch_witness,
                &page.filter_digest,
            )?;
            if !cut.is_on_chain(&page.cut_commit_id) {
                return Err(cursor_rejected(
                    "change feed page token names a cut outside this branch's first-parent chain",
                ));
            }
            let key = ContinuationKey {
                identity: page.identity(),
                id: page.id.clone(),
                operation_rank: page.operation_rank,
                change_index: page.change_index,
            };
            Ok(ResolvedPosition {
                // The token's captured cut, NOT the fresh head: commits that
                // arrived after the poll began stay outside its pages.
                cut_commit_id: page.cut_commit_id.clone(),
                after_commit_id: page.after_commit_id.clone(),
                resume: Some((page.current_commit_id.clone(), key)),
                durable_cursor: None,
            })
        }
    }
}
