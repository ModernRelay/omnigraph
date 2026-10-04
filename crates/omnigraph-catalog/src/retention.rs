//! Durable input lifetimes for a merge that may outlive its source HEAD.
//!
//! A native tag roots one exact graph manifest. Its immutable name carries
//! the target publication witness, so a crashed attempt remains protected
//! until the target moves or retires. No table version or graph head changes.

use std::collections::{BTreeMap, HashMap};

use lance::Dataset;
use lance::dataset::refs::{BranchContents, Ref, TagContents};
use sha2::{Digest, Sha256};

use super::{CapturedManifestProbe, ManifestCoordinator, Snapshot};
use crate::branch_names::{MERGE_INPUT_PREFIX, encode_head, is_merge_input_tag};
pub use crate::branch_names::{MergeInputOwner, merge_input_owner};
use crate::commit_graph::{CommitGraph, GraphCommit, HistoryCache};
use crate::error::{OmniError, Result};
use crate::staging::StagingWitness;

pub fn incarnation_digest(incarnation: &str) -> String {
    format!("{:x}", Sha256::digest(incarnation.as_bytes()))
}

/// Exact native manifest coordinates and their reduced graph snapshot.
pub struct PinnedGraphManifest {
    pub dataset: Dataset,
    pub snapshot: Snapshot,
}

impl PinnedGraphManifest {
    /// The commit graph of the head this manifest version holds, over the
    /// settled commits `history` has read.
    pub async fn commit_graph(
        &self,
        root_uri: &str,
        history: &HistoryCache,
    ) -> Result<CommitGraph> {
        let rows = crate::state::read_manifest_rows_projected(&self.dataset).await?;
        Ok(CommitGraph::from_head(
            root_uri,
            self.dataset.session(),
            rows.head,
            rows.buffer.commits(),
            history.clone(),
        ))
    }

    async fn from_dataset(root_uri: &str, dataset: Dataset) -> Result<Self> {
        let native = dataset.manifest().branch.clone();
        let mut snapshot = ManifestCoordinator::snapshot_from_state(
            root_uri,
            super::read_manifest_state(&dataset).await?,
        );
        snapshot.graph_branch = native
            .as_deref()
            .map(crate::branch_names::logical_branch_name)
            .map(str::to_string);
        snapshot.native_branch = native;
        Ok(Self { dataset, snapshot })
    }
}

impl CapturedManifestProbe {
    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }
}

/// A cancellation can leave these tags behind. Cleanup uses the immutable
/// owner witness, never age, to release that abandoned protection.
pub struct MergeInputGuard {
    dataset: Dataset,
    owner: MergeInputOwner,
    acknowledged_names: Vec<String>,
}

impl MergeInputGuard {
    pub fn new(dataset: &Dataset, witness: &StagingWitness) -> Self {
        Self {
            dataset: dataset.clone(),
            owner: MergeInputOwner {
                incarnation_digest: incarnation_digest(witness.branch_incarnation()),
                graph_head: witness.graph_head().map(str::to_string),
            },
            acknowledged_names: Vec::new(),
        }
    }

    pub async fn pin(&mut self, dataset: &Dataset) -> Result<()> {
        let name = format!(
            "{MERGE_INPUT_PREFIX}{}_{}_{}",
            self.owner.incarnation_digest,
            encode_head(self.owner.graph_head.as_deref()),
            crate::dst_ids::new_ulid(),
        );
        self.dataset
            .tags()
            .create(
                &name,
                Ref::Version(
                    dataset.manifest().branch.clone(),
                    Some(dataset.version().version),
                ),
            )
            .await
            .map_err(OmniError::storage)?;
        self.acknowledged_names.push(name);
        Ok(())
    }

    pub async fn release(&self) -> Result<()> {
        release_merge_input_tags(&self.dataset, &self.acknowledged_names).await
    }
}

/// Delete only nonce-owned, immutable engine tags. No existence HEAD is
/// necessary: absence is the idempotent completed release outcome.
pub async fn release_merge_input_tags(dataset: &Dataset, names: &[String]) -> Result<()> {
    let root = dataset
        .branch_location()
        .find_main()
        .map_err(OmniError::storage)?;
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    for name in names {
        if !is_merge_input_tag(name) {
            return Err(OmniError::manifest_internal(
                "refusing to release an unowned native tag",
            ));
        }
        if let Err(error) = store
            .delete(&lance::dataset::refs::tag_path(&root.path, name))
            .await
            && !error.is_not_found()
        {
            return Err(OmniError::storage(error));
        }
    }
    Ok(())
}

/// One immutable inventory boundary around all graph snapshots a collector
/// combines. User tags may be updated, so both names and contents must match.
pub struct ManifestTagInventory {
    dataset: Dataset,
    pub tags: BTreeMap<String, TagContents>,
}

impl ManifestTagInventory {
    pub async fn capture(dataset: &Dataset) -> Result<Self> {
        let tags = dataset.tags().list().await.map_err(OmniError::storage)?;
        for name in tags.keys() {
            merge_input_owner(name)?;
        }
        Ok(Self {
            dataset: dataset.clone(),
            tags: tags.into_iter().collect(),
        })
    }

    pub async fn snapshot(&self, tag: &TagContents, root_uri: &str) -> Result<PinnedGraphManifest> {
        let dataset = self
            .dataset
            .checkout_version(Ref::Version(tag.branch.clone(), Some(tag.version)))
            .await
            .map_err(OmniError::storage)?;
        PinnedGraphManifest::from_dataset(root_uri, dataset).await
    }

    pub async fn validate(&self) -> Result<()> {
        let current = self
            .dataset
            .tags()
            .list()
            .await
            .map_err(OmniError::storage)?;
        let matches = current.len() == self.tags.len()
            && self.tags.iter().all(|(name, old)| {
                current.get(name).is_some_and(|new| {
                    old.branch == new.branch
                        && old.version == new.version
                        && old.manifest_size == new.manifest_size
                        && old.created_at == new.created_at
                        && old.updated_at == new.updated_at
                        && old.metadata == new.metadata
                })
            });
        if !matches {
            return Err(OmniError::manifest_conflict(
                "collector native tag inventory changed during capture; retry cleanup",
            ));
        }
        Ok(())
    }
}

/// Include both sides of retirement's archive-before-unlink crash boundary.
pub async fn retired_manifest_branches(
    dataset: &Dataset,
) -> Result<HashMap<String, BranchContents>> {
    let mut retired = crate::branch_control::list_archived_manifest_branches(dataset).await?;
    retired.extend(crate::branch_control::list_retired_manifest_branch_contents(dataset).await?);
    Ok(retired)
}

impl ManifestCoordinator {
    /// Resolve a logical graph commit by immutable physical coordinates. A
    /// recreated logical name may reuse its version number, but never its
    /// graph commit id; retired histories remain eligible lookup candidates.
    pub async fn pinned_graph_commit(
        root_uri: &str,
        commit: &GraphCommit,
    ) -> Result<PinnedGraphManifest> {
        let main = super::open_manifest_dataset_native_with_session(
            root_uri,
            None,
            &crate::lance_access::control_session(),
        )
        .await?;
        let mut candidates = Vec::new();
        match commit.graph_branch.as_deref() {
            None | Some("main") => candidates.push(None),
            Some(logical) => {
                let mut physical = crate::branch_control::list_all_branch_contents(&main).await?;
                physical.extend(retired_manifest_branches(&main).await?);
                candidates.extend(
                    physical
                        .into_iter()
                        .filter(|(native, contents)| {
                            crate::branch_names::logical_branch_name(native) == logical
                                && contents.parent_version < commit.graph_manifest_version
                        })
                        .map(|(native, _)| Some(native)),
                );
                candidates.sort();
            }
        }
        for native in candidates {
            let dataset = match main
                .checkout_version(Ref::Version(native, Some(commit.graph_manifest_version)))
                .await
            {
                Ok(dataset) => dataset,
                Err(lance::Error::DatasetNotFound { .. }) => continue,
                Err(error) if error.is_not_found() => continue,
                Err(error) => return Err(OmniError::storage(error)),
            };
            let head = crate::state::read_manifest_rows_projected(&dataset)
                .await?
                .head;
            if crate::commit_graph::graph_commit_from_manifest_row(head) != *commit {
                continue;
            }
            return PinnedGraphManifest::from_dataset(root_uri, dataset).await;
        }
        Err(OmniError::manifest_not_found(format!(
            "merge base '{}' has no matching retained native manifest at version {}",
            commit.graph_commit_id, commit.graph_manifest_version,
        )))
    }

    /// The commit graph of every retired branch incarnation by native ref,
    /// over the settled commits `history` has read.
    pub async fn retired_commit_graphs(
        root_uri: &str,
        history: &HistoryCache,
    ) -> Result<Vec<(String, CommitGraph)>> {
        let main = super::open_manifest_dataset_native_with_session(
            root_uri,
            None,
            &crate::lance_access::control_session(),
        )
        .await?;
        let mut names = retired_manifest_branches(&main)
            .await?
            .into_keys()
            .collect::<Vec<_>>();
        names.sort();
        let mut graphs = Vec::new();
        for native in names.into_iter().rev() {
            let dataset = main
                .checkout_version(Ref::Version(Some(native.clone()), None))
                .await
                .map_err(OmniError::storage)?;
            let rows = crate::state::read_manifest_rows_projected(&dataset).await?;
            let graph = CommitGraph::from_head(
                root_uri,
                dataset.session(),
                rows.head,
                rows.buffer.commits(),
                history.clone(),
            );
            graphs.push((native, graph));
        }
        Ok(graphs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_core::graph_commit_id::HISTORY_BLOCK_SLOTS;

    #[test]
    fn merge_input_tags_require_canonical_authority() {
        let digest = "ab".repeat(32);
        let head = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let nonce = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
        let valid = format!(
            "{MERGE_INPUT_PREFIX}{digest}_{}_{nonce}",
            encode_head(Some(head))
        );
        assert_eq!(
            merge_input_owner(&valid)
                .unwrap()
                .unwrap()
                .graph_head
                .as_deref(),
            Some(head)
        );
        let block_head = format!("hb1.{head}.15.{nonce}");
        let block_tag = format!(
            "{MERGE_INPUT_PREFIX}{digest}_{}_{nonce}",
            encode_head(Some(&block_head))
        );
        assert_eq!(
            merge_input_owner(&block_tag)
                .unwrap()
                .unwrap()
                .graph_head
                .as_deref(),
            Some(block_head.as_str())
        );
        for bad in [
            format!(
                "{MERGE_INPUT_PREFIX}{digest}_{}_{nonce}",
                encode_head(Some(&format!("hb1.{head}.{HISTORY_BLOCK_SLOTS}.{nonce}")))
            ),
            valid.replacen(&digest, &digest.to_uppercase(), 1),
            valid.replace(nonce, &nonce.to_lowercase()),
            format!(
                "{MERGE_INPUT_PREFIX}{digest}_{}_{nonce}",
                encode_head(Some("not-a-commit"))
            ),
            format!(
                "{MERGE_INPUT_PREFIX}{digest}_{}_{nonce}",
                encode_head(Some(&head.to_lowercase()))
            ),
            valid.replace("s3031", "s3A31"),
        ] {
            assert!(
                merge_input_owner(&bad).is_err(),
                "accepted malformed ownership: {bad}"
            );
            assert!(!is_merge_input_tag(&bad));
        }
    }
}
