//! Settling a publication whose answer was lost, from the settlement branch's
//! history.
//!
//! A run that publishes under an exact graph-head expectation can only land
//! as the commit whose parent is the head every attempt read. Once the
//! branch's graph head moves, the input head's child is that commit or proves
//! the run did not commit: every `__manifest` version an attempt could have
//! taken is occupied, and a later attempt reads the moved head and refuses.
//! The walk reads the branch's first-parent chain one page at a time, so one
//! attempt costs a bounded number of reads however far the branch has moved;
//! the caller keeps the page's unvisited parent as its cursor.

use super::*;

/// What the settlement branch's history says about a publication with a given
/// intent nonce.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PublicationLookup {
    /// The branch's graph head is still the input head. A configuration-only
    /// `__manifest` version does not move it, and an attempt at the version
    /// after one can still be in flight.
    HeadUnchanged,
    /// The input head's child answers the nonce: the run committed as it.
    Committed { commit_id: String },
    /// The input head's child does not answer the nonce.
    NotCommitted,
    /// The page ended before the input head's child; `next` is the first
    /// commit it did not visit, where the next page starts, and `visited`
    /// the commits this page covered.
    Walking { next: String, visited: u64 },
    /// The walk could not reach the input head's child, so nothing is proven
    /// either way; `reason` says where it stopped.
    Unproven { reason: String },
}

impl Omnigraph {
    /// Read one page of `branch`'s first-parent chain (`limit` commits, newest
    /// first) from `cursor`, or from the head of a fresh read of the branch's
    /// `__manifest` when `cursor` is `None`, toward the child of
    /// `input_commit`, and judge that child against `nonce`. Every commit on a
    /// branch is written on that branch, so each step's
    /// `graph_manifest_version` is below the one before it, and the walk
    /// stops at the first link not above `input_manifest_version`. The head
    /// row, the commit buffer and the history cache cost no request; a
    /// released commit is read with the rest of its `__history` extent.
    pub async fn resolve_publication(
        &self,
        branch: Option<&str>,
        nonce: &str,
        input_commit: Option<&str>,
        input_manifest_version: u64,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<PublicationLookup> {
        let coordinator = self.open_coordinator_for_branch(branch).await?;
        let (head, page) = match coordinator.first_parent_page(cursor, limit).await {
            Ok(read) => read,
            // A missing link in the chain proves nothing; a later page may.
            Err(error) => {
                return Ok(PublicationLookup::Unproven {
                    reason: format!("reading the branch's first-parent chain: {error}"),
                });
            }
        };
        if cursor.is_none() && Some(head.graph_commit_id.as_str()) == input_commit {
            return Ok(PublicationLookup::HeadUnchanged);
        }
        let Some(page) = page else {
            return Ok(PublicationLookup::Unproven {
                reason: format!(
                    "no held commit or `__history` extent holds {}",
                    cursor.unwrap_or(&head.graph_commit_id)
                ),
            });
        };
        let mut visited = 0_u64;
        for commit in page.commits {
            visited += 1;
            if commit.parent_commit_id.as_deref() == input_commit {
                return Ok(
                    match omnigraph_core::graph_commit_id::commit_id_answers(
                        &commit.graph_commit_id,
                        nonce,
                    ) {
                        Ok(true) => PublicationLookup::Committed {
                            commit_id: commit.graph_commit_id,
                        },
                        Ok(false) => PublicationLookup::NotCommitted,
                        Err(error) => PublicationLookup::Unproven {
                            reason: format!(
                                "the input head's child {} has no readable id: {error}",
                                commit.graph_commit_id
                            ),
                        },
                    },
                );
            }
            if commit.graph_manifest_version <= input_manifest_version {
                return Ok(PublicationLookup::Unproven {
                    reason: format!(
                        "the walk passed the input head's version at {}",
                        commit.graph_commit_id
                    ),
                });
            }
        }
        Ok(match page.next {
            Some(next) => PublicationLookup::Walking { next, visited },
            None => PublicationLookup::Unproven {
                reason: "the walk reached the first commit without meeting the input head"
                    .to_string(),
            },
        })
    }
}
