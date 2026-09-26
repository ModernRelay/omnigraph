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
use crate::db::commit_graph::{CommitGraph, GraphCommit};
use crate::error::{OmniError, Result};
use crate::table_store::StagingWitness;

const MERGE_INPUT_PREFIX: &str = "__omnigraph_merge_input_v1_";

/// Ownership encoded before the tag's single create-if-absent publication.
/// The digest bounds name length even for deeply nested branch identifiers;
/// the actual graph head remains available for the collector's ancestry test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergeInputOwner {
    pub(crate) incarnation_digest: String,
    pub(crate) graph_head: Option<String>,
}

pub(crate) fn incarnation_digest(incarnation: &str) -> String {
    format!("{:x}", Sha256::digest(incarnation.as_bytes()))
}

fn encode_head(head: Option<&str>) -> String {
    match head {
        None => "n".to_string(),
        Some(head) => {
            let mut encoded = String::from("s");
            for byte in head.as_bytes() {
                use std::fmt::Write;
                write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
            }
            encoded
        }
    }
}

fn decode_head(encoded: &str) -> Option<Option<String>> {
    if encoded == "n" {
        return Some(None);
    }
    let encoded = encoded.strip_prefix('s')?;
    if encoded.is_empty() || encoded.len() % 2 != 0 || !encoded.is_ascii() {
        return None;
    }
    let bytes = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).ok())
        .collect::<Option<Vec<_>>>()?;
    let head = String::from_utf8(bytes).ok()?;
    let id = head.parse::<ulid::Ulid>().ok()?;
    if id.to_string() != head || encode_head(Some(&head)).strip_prefix('s') != Some(encoded) {
        return None;
    }
    Some(Some(head))
}

pub(crate) fn merge_input_owner(name: &str) -> Result<Option<MergeInputOwner>> {
    let Some(encoded) = name.strip_prefix(MERGE_INPUT_PREFIX) else {
        return Ok(None);
    };
    let invalid = || OmniError::manifest_conflict("malformed merge input retention tag");
    let mut parts = encoded.split('_');
    let digest = parts.next().ok_or_else(invalid)?;
    let head = parts.next().and_then(decode_head).ok_or_else(invalid)?;
    let nonce = parts.next().ok_or_else(invalid)?;
    if parts.next().is_some()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || nonce
            .parse::<ulid::Ulid>()
            .ok()
            .is_none_or(|id| id.to_string() != nonce)
    {
        return Err(invalid());
    }
    Ok(Some(MergeInputOwner {
        incarnation_digest: digest.to_string(),
        graph_head: head,
    }))
}

/// Only valid engine-owned tags exempt logical retirement; arbitrary native
/// tags retain Lance's existing refusal semantics.
pub(crate) fn is_merge_input_tag(name: &str) -> bool {
    matches!(merge_input_owner(name), Ok(Some(_)))
}

/// Exact native manifest coordinates and their reduced graph snapshot.
pub(crate) struct PinnedGraphManifest {
    pub(crate) dataset: Dataset,
    pub(crate) snapshot: Snapshot,
}

impl PinnedGraphManifest {
    pub(crate) async fn commit_graph(&self, root_uri: &str) -> Result<CommitGraph> {
        let (rows, _) = super::read_graph_lineage(&self.dataset).await?;
        Ok(CommitGraph::from_manifest_rows(
            root_uri,
            self.snapshot.graph_branch.as_deref(),
            rows,
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
    pub(crate) fn dataset(&self) -> &Dataset {
        &self.dataset
    }
}

/// A cancellation can leave these tags behind. Cleanup uses the immutable
/// owner witness, never age, to release that abandoned protection.
pub(crate) struct MergeInputGuard {
    dataset: Dataset,
    owner: MergeInputOwner,
    acknowledged_names: Vec<String>,
}

impl MergeInputGuard {
    pub(crate) fn new(dataset: &Dataset, witness: &StagingWitness) -> Self {
        Self {
            dataset: dataset.clone(),
            owner: MergeInputOwner {
                incarnation_digest: incarnation_digest(witness.branch_incarnation()),
                graph_head: witness.graph_head().map(str::to_string),
            },
            acknowledged_names: Vec::new(),
        }
    }

    pub(crate) async fn pin(&mut self, dataset: &Dataset) -> Result<()> {
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

    pub(crate) async fn release(&self) -> Result<()> {
        release_merge_input_tags(&self.dataset, &self.acknowledged_names).await
    }
}

/// Delete only nonce-owned, immutable engine tags. No existence HEAD is
/// necessary: absence is the idempotent completed release outcome.
pub(crate) async fn release_merge_input_tags(dataset: &Dataset, names: &[String]) -> Result<()> {
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
pub(crate) struct ManifestTagInventory {
    dataset: Dataset,
    pub(crate) tags: BTreeMap<String, TagContents>,
}

impl ManifestTagInventory {
    pub(crate) async fn capture(dataset: &Dataset) -> Result<Self> {
        let tags = dataset.tags().list().await.map_err(OmniError::storage)?;
        for name in tags.keys() {
            merge_input_owner(name)?;
        }
        Ok(Self {
            dataset: dataset.clone(),
            tags: tags.into_iter().collect(),
        })
    }

    pub(crate) async fn snapshot(
        &self,
        tag: &TagContents,
        root_uri: &str,
    ) -> Result<PinnedGraphManifest> {
        let dataset = self
            .dataset
            .checkout_version(Ref::Version(tag.branch.clone(), Some(tag.version)))
            .await
            .map_err(OmniError::storage)?;
        PinnedGraphManifest::from_dataset(root_uri, dataset).await
    }

    pub(crate) async fn validate(&self) -> Result<()> {
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
pub(crate) async fn retired_manifest_branches(
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
    pub(crate) async fn pinned_graph_commit(
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
                Err(error) if error.is_not_found() => continue,
                Err(error) => return Err(OmniError::storage(error)),
            };
            let (rows, heads) = super::read_graph_lineage(&dataset).await?;
            let graph =
                CommitGraph::from_manifest_rows(root_uri, commit.graph_branch.as_deref(), rows);
            let head = heads
                .get(commit.graph_branch.as_deref().unwrap_or("main"))
                .cloned()
                .or(graph.head_commit_id().await?);
            if head.as_deref() != Some(commit.graph_commit_id.as_str())
                || graph.get_commit(&commit.graph_commit_id).as_ref() != Some(commit)
            {
                continue;
            }
            return PinnedGraphManifest::from_dataset(root_uri, dataset).await;
        }
        Err(OmniError::manifest_not_found(format!(
            "merge base '{}' has no matching retained native manifest at version {}",
            commit.graph_commit_id, commit.graph_manifest_version,
        )))
    }

    pub(crate) async fn retired_commit_graphs(
        root_uri: &str,
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
            let (rows, _) = super::read_graph_lineage(&dataset).await?;
            let graph = CommitGraph::from_manifest_rows(
                root_uri,
                Some(crate::branch_names::logical_branch_name(&native)),
                rows,
            );
            graphs.push((native, graph));
        }
        Ok(graphs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        for bad in [
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
