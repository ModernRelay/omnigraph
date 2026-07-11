//! Crash classification for Lance's native branch controls.
//!
//! Lance branch creation is deliberately two-phase: it first commits a shallow
//! clone under `tree/{branch}` and then writes authoritative `BranchContents`.
//! Deletion removes `BranchContents` before reclaiming that tree.  These helpers
//! keep OmniGraph on Lance's public APIs while making both operations bounded
//! and idempotent under the documented single-writer-process branch-control
//! boundary. Live graph names are path-prefix-disjoint because Lance cannot
//! recursively reclaim an ancestor tree while a path-child branch remains.

use std::collections::HashMap;

use lance::Dataset;
use lance::dataset::refs::{BranchContents, BranchIdentifier, check_valid_branch};

use crate::error::{OmniError, Result};

/// Result of a recoverable native create attempt.
pub(crate) enum BranchCreateOutcome {
    /// The requested branch is durably authoritative and its dataset opens.
    Created(Dataset),
    /// The target ref existed before this invocation. Callers classify its
    /// ownership at their own logical layer; this helper never deletes it.
    /// A ref that appears after target absence was captured is instead a typed
    /// authority-change error and cannot enter an orphan-reclaim path.
    RefAlreadyExists,
}

fn lance_error(error: lance::Error) -> OmniError {
    OmniError::Lance(error.to_string())
}

/// Lance's local backend reports `NotFound` when the target tree is already
/// absent, while object stores commonly treat the empty-prefix delete as a
/// success. Normalize both to the idempotent contract every OmniGraph branch
/// reconciler needs.
pub(crate) async fn force_delete_branch_idempotent(
    dataset: &mut Dataset,
    branch: &str,
) -> Result<()> {
    let branches = list_branch_contents(dataset).await?;
    if let Some(child) = path_descendant(&branches, branch) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot reclaim branch '{branch}' while live branch '{child}' shares its physical \
             Lance path; delete the child branch first"
        )));
    }
    force_delete_branch_tree_unchecked(dataset, branch).await
}

async fn force_delete_branch_tree_unchecked(dataset: &mut Dataset, branch: &str) -> Result<()> {
    match dataset.force_delete_branch(branch).await {
        Ok(()) | Err(lance::Error::RefNotFound { .. }) | Err(lance::Error::NotFound { .. }) => {
            Ok(())
        }
        Err(error) => Err(lance_error(error)),
    }
}

async fn list_branch_contents(dataset: &Dataset) -> Result<HashMap<String, BranchContents>> {
    dataset.list_branches().await.map_err(lance_error)
}

async fn branch_contents(dataset: &Dataset, branch: &str) -> Result<Option<BranchContents>> {
    Ok(list_branch_contents(dataset).await?.get(branch).cloned())
}

pub(crate) fn path_descendant<'a>(
    branches: &'a HashMap<String, BranchContents>,
    branch: &str,
) -> Option<&'a str> {
    let prefix = format!("{branch}/");
    branches
        .keys()
        .map(String::as_str)
        .filter(|candidate| candidate.starts_with(&prefix))
        .min()
}

fn path_collision<'a>(
    branches: &'a HashMap<String, BranchContents>,
    branch: &str,
) -> Option<&'a str> {
    let branch_prefix = format!("{branch}/");
    branches
        .keys()
        .map(String::as_str)
        .filter(|candidate| {
            *candidate != branch
                && *candidate != "main"
                && (candidate.starts_with(&branch_prefix)
                    || branch.starts_with(&format!("{candidate}/")))
        })
        .min()
}

fn encode_identifier(identifier: &BranchIdentifier) -> Result<String> {
    serde_json::to_string(identifier).map_err(|error| {
        OmniError::manifest_internal(format!("failed to encode Lance branch identifier: {error}"))
    })
}

async fn authority_appeared_after_absence(dataset: &Dataset, branch: &str) -> Result<OmniError> {
    match branch_contents(dataset, branch).await? {
        Some(contents) => Ok(OmniError::manifest_read_set_changed(
            format!("branch_identifier:{branch}"),
            None,
            Some(encode_identifier(&contents.identifier)?),
        )),
        None => Ok(OmniError::manifest_conflict(format!(
            "branch authority for '{branch}' changed during absent-only reclaim; refusing \
             destructive cleanup"
        ))),
    }
}

/// Reclaim only when the authoritative ref is freshly absent. Returns `false`
/// if a ref exists and therefore nothing was touched. A live path-child is a
/// conflict because Lance deliberately keeps the ancestor directory in that
/// state; the graph namespace prevents new instances of that overlap.
pub(crate) async fn reclaim_ref_absent_tree(dataset: &mut Dataset, branch: &str) -> Result<bool> {
    let branches = list_branch_contents(dataset).await?;
    if branches.contains_key(branch) {
        return Ok(false);
    }
    if let Some(child) = path_descendant(&branches, branch) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot reclaim absent branch '{branch}' while live branch '{child}' shares its \
             physical Lance path; delete the child branch first"
        )));
    }
    // This is the final authority read before the destructive call. The
    // supported single-writer-process gate boundary prevents a local create
    // from interleaving after it; an exact ref observed here is never passed to
    // Lance's broad force-delete primitive.
    force_delete_branch_tree_unchecked(dataset, branch).await?;
    Ok(true)
}

/// A completed create receives an identifier equal to the parent's complete
/// identifier mapping plus one newly-generated `(parent_version, uuid)` entry.
/// The UUID itself cannot be pre-minted through Lance's public API, so this is
/// the strongest completion proof available inside the supported
/// single-writer-process boundary.
fn matches_create_expectation(
    contents: &BranchContents,
    parent_branch: Option<&str>,
    parent_version: u64,
    parent_identifier: &BranchIdentifier,
) -> bool {
    let expected_parent = parent_branch.filter(|branch| *branch != "main");
    let mapping = &contents.identifier.version_mapping;
    let parent_mapping = &parent_identifier.version_mapping;
    contents.parent_branch.as_deref() == expected_parent
        && contents.parent_version == parent_version
        && mapping.len() == parent_mapping.len() + 1
        && mapping.starts_with(parent_mapping)
        && mapping
            .last()
            .is_some_and(|(version, uuid)| *version == parent_version && !uuid.is_empty())
}

/// Create a branch with a bounded recovery classifier.
///
/// The caller must already hold OmniGraph's schema → source/target branch →
/// table gate envelope. An absent `BranchContents` ref makes any same-name tree
/// derived garbage, so it is reclaimed before the first attempt. An ambiguous
/// native error is then classified from fresh authority: matching contents are
/// accepted as a lost acknowledgement, mismatching contents are never deleted,
/// and a ref-less clone is reclaimed before one bounded retry.
pub(crate) async fn create_branch_recoverably(
    source: &mut Dataset,
    branch: &str,
    source_version: u64,
) -> Result<BranchCreateOutcome> {
    // Lance validates inside phase 2 today. Validate before phase 1 so an
    // invalid name cannot leave a clone-only zombie.
    check_valid_branch(branch).map_err(lance_error)?;
    if source.version().version != source_version {
        return Err(OmniError::manifest_conflict(format!(
            "branch source moved before native create: expected version {}, current {}",
            source_version,
            source.version().version
        )));
    }

    let parent_branch = source.manifest().branch.clone();
    let parent_identifier = source.branch_identifier().await.map_err(lance_error)?;

    let initial_branches = list_branch_contents(source).await?;
    if initial_branches.contains_key(branch) {
        return Ok(BranchCreateOutcome::RefAlreadyExists);
    }
    if let Some(conflicting) = path_collision(&initial_branches, branch) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot create branch '{branch}' while live branch '{conflicting}' shares its \
             physical Lance path; live graph branch names may not be ancestors or descendants"
        )));
    }
    if !reclaim_ref_absent_tree(source, branch).await? {
        return Err(authority_appeared_after_absence(source, branch).await?);
    }

    for attempt in 0..2 {
        let native_error = match source
            .create_branch(branch, source_version, None)
            .await
            .map_err(lance_error)
        {
            Ok(created) => match crate::failpoints::maybe_fail(
                crate::failpoints::names::BRANCH_CREATE_POST_NATIVE,
            ) {
                Ok(()) => return Ok(BranchCreateOutcome::Created(created)),
                Err(error) => error,
            },
            Err(error) => error,
        };

        let observed = branch_contents(source, branch).await.map_err(|classifier_error| {
            OmniError::manifest_internal(format!(
                "native create of branch '{branch}' returned an ambiguous error ({native_error}); \
                 reading BranchContents to classify it also failed ({classifier_error})"
            ))
        })?;
        if let Some(contents) = observed {
            if !matches_create_expectation(
                &contents,
                parent_branch.as_deref(),
                source_version,
                &parent_identifier,
            ) {
                return Err(OmniError::manifest_read_set_changed(
                    format!("branch_identifier:{branch}"),
                    None,
                    Some(encode_identifier(&contents.identifier)?),
                ));
            }
            let created = source.checkout_branch(branch).await.map_err(|error| {
                OmniError::manifest_internal(format!(
                    "branch '{}' has matching authoritative metadata after an ambiguous create, \
                     but its branch dataset cannot be opened: {}; original native error: {}",
                    branch, error, native_error
                ))
            })?;
            return Ok(BranchCreateOutcome::Created(created));
        }

        match reclaim_ref_absent_tree(source, branch).await {
            Ok(true) => {}
            Ok(false) => {
                return Err(authority_appeared_after_absence(source, branch).await?);
            }
            Err(cleanup_error) => {
                return Err(OmniError::manifest_internal(format!(
                    "native create of branch '{}' failed before authoritative metadata was visible \
                     ({native_error}); clone-only cleanup also failed ({cleanup_error})",
                    branch
                )));
            }
        }
        if attempt == 1 {
            return Err(native_error);
        }
    }

    unreachable!("bounded native branch-create loop returns from every attempt")
}

/// Delete a branch and classify Lance's authority-removal → tree-cleanup gap.
///
/// A missing ref after an error is logical success. Tree reclamation is derived
/// state and is retried best-effort; a recreated identifier is never deleted.
pub(crate) async fn delete_branch_recoverably(
    dataset: &mut Dataset,
    branch: &str,
    expected_identifier: &BranchIdentifier,
) -> Result<()> {
    let initial = list_branch_contents(dataset).await?;
    let Some(initial_contents) = initial.get(branch) else {
        match reclaim_ref_absent_tree(dataset, branch).await {
            Ok(true) => {}
            Ok(false) => {
                return Err(authority_appeared_after_absence(dataset, branch).await?);
            }
            Err(cleanup_error) => {
                tracing::warn!(
                    target: "omnigraph::branch_control",
                    branch,
                    error = %cleanup_error,
                    "branch authority is already deleted; derived tree reclaim remains pending",
                );
            }
        }
        return Ok(());
    };
    if initial_contents.identifier != *expected_identifier {
        return Err(OmniError::manifest_read_set_changed(
            format!("branch_identifier:{branch}"),
            Some(encode_identifier(expected_identifier)?),
            Some(encode_identifier(&initial_contents.identifier)?),
        ));
    }
    if let Some(child) = path_descendant(&initial, branch) {
        return Err(OmniError::manifest_conflict(format!(
            "cannot delete branch '{branch}' while live branch '{child}' shares its physical \
             Lance path; delete the child branch first"
        )));
    }

    let native_result = match dataset.delete_branch(branch).await.map_err(lance_error) {
        Ok(()) => {
            crate::failpoints::maybe_fail(crate::failpoints::names::BRANCH_DELETE_POST_NATIVE)
        }
        Err(error) => Err(error),
    };
    let native_error = match native_result {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };

    let observed = branch_contents(dataset, branch)
        .await
        .map_err(|classifier_error| {
            OmniError::manifest_internal(format!(
                "native delete of branch '{branch}' returned an ambiguous error ({native_error}); \
             reading BranchContents to classify it also failed ({classifier_error})"
            ))
        })?;
    match observed {
        None => {
            match reclaim_ref_absent_tree(dataset, branch).await {
                Ok(true) => {}
                Ok(false) => {
                    return Err(authority_appeared_after_absence(dataset, branch).await?);
                }
                Err(cleanup_error) => {
                    tracing::warn!(
                        target: "omnigraph::branch_control",
                        branch,
                        error = %cleanup_error,
                        "branch authority is deleted; derived tree reclaim remains pending",
                    );
                }
            }
            Ok(())
        }
        Some(contents) if contents.identifier == *expected_identifier => Err(native_error),
        Some(contents) => Err(OmniError::manifest_read_set_changed(
            format!("branch_identifier:{branch}"),
            Some(encode_identifier(expected_identifier)?),
            Some(encode_identifier(&contents.identifier)?),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{WriteMode, WriteParams};

    async fn test_dataset(dir: &tempfile::TempDir) -> Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        // forbidden-api-allow: test-only raw Lance fixture for native branch-control truth cells
        Dataset::write(
            reader,
            dir.path().to_str().unwrap(),
            Some(WriteParams {
                mode: WriteMode::Create,
                auto_cleanup: None,
                skip_auto_cleanup: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    #[test]
    fn create_match_requires_exact_parent_incarnation_prefix() {
        let parent = BranchIdentifier {
            version_mapping: vec![(2, "parent-a".to_string())],
        };
        let matching = BranchContents {
            parent_branch: Some("source".to_string()),
            identifier: BranchIdentifier {
                version_mapping: vec![(2, "parent-a".to_string()), (7, "child".to_string())],
            },
            parent_version: 7,
            create_at: 0,
            manifest_size: 0,
            metadata: Default::default(),
        };
        assert!(matches_create_expectation(
            &matching,
            Some("source"),
            7,
            &parent
        ));

        let foreign_parent = BranchIdentifier {
            version_mapping: vec![(2, "parent-b".to_string())],
        };
        assert!(!matches_create_expectation(
            &matching,
            Some("source"),
            7,
            &foreign_parent
        ));
    }

    #[tokio::test]
    async fn delete_accepts_absent_authority_and_reclaims_remaining_tree() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let identifier = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        std::fs::remove_file(
            dir.path()
                .join("_refs")
                .join("branches")
                .join("feature.json"),
        )
        .unwrap();

        delete_branch_recoverably(&mut dataset, "feature", &identifier)
            .await
            .unwrap();
        assert!(!dir.path().join("tree").join("feature").exists());
    }

    #[tokio::test]
    async fn delete_preserves_native_error_while_same_identifier_remains() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        let mut feature = dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let identifier = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        let feature_version = feature.version().version;
        feature
            .create_branch("dependent", feature_version, None)
            .await
            .unwrap();

        let error = delete_branch_recoverably(&mut dataset, "feature", &identifier)
            .await
            .expect_err("a lineage-dependent ref must preserve Lance's delete refusal");
        assert!(error.to_string().contains("referenc"));
        assert_eq!(
            dataset
                .list_branches()
                .await
                .unwrap()
                .get("feature")
                .unwrap()
                .identifier,
            identifier
        );
    }

    #[tokio::test]
    async fn delete_rejects_recreated_identifier_without_removing_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let original = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        dataset.delete_branch("feature").await.unwrap();
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let recreated = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        assert_ne!(original, recreated);

        let error = delete_branch_recoverably(&mut dataset, "feature", &original)
            .await
            .expect_err("a recreated target must be fenced by its identifier");
        let OmniError::Manifest(error) = error else {
            panic!("expected typed manifest conflict");
        };
        match error.details {
            Some(crate::error::ManifestConflictDetails::ReadSetChanged {
                member,
                expected: Some(expected),
                actual: Some(actual),
            }) => {
                assert_eq!(member, "branch_identifier:feature");
                assert_eq!(expected, serde_json::to_string(&original).unwrap());
                assert_eq!(actual, serde_json::to_string(&recreated).unwrap());
            }
            other => panic!("expected identifier ReadSetChanged, got {other:?}"),
        }
        assert_eq!(
            dataset
                .list_branches()
                .await
                .unwrap()
                .get("feature")
                .unwrap()
                .identifier,
            recreated,
            "classifier must never delete the recreated authority"
        );
    }

    #[tokio::test]
    async fn lower_create_chokepoint_rejects_prefix_collision_before_clone() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();

        let error = match create_branch_recoverably(&mut dataset, "feature/child", version).await {
            Ok(_) => panic!("the lower branch-control surface must enforce prefix disjointness"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("feature"));
        assert!(error.to_string().contains("physical Lance path"));
        assert!(
            !dataset
                .list_branches()
                .await
                .unwrap()
                .contains_key("feature/child"),
            "admission refusal must precede authoritative ref creation"
        );
        assert!(
            !dir.path()
                .join("tree")
                .join("feature")
                .join("child")
                .exists(),
            "admission refusal must precede Lance's shallow-clone phase"
        );
    }

    #[tokio::test]
    async fn lower_create_preserves_preexisting_same_name_ref() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let before = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();

        let outcome = create_branch_recoverably(&mut dataset, "feature", version)
            .await
            .unwrap();
        assert!(matches!(outcome, BranchCreateOutcome::RefAlreadyExists));
        let after = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();
        assert_eq!(
            before, after,
            "classification must not replace the live ref"
        );
    }

    #[tokio::test]
    async fn absent_only_reclaim_preserves_authority_that_appeared_after_prior_absence() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        assert!(
            !dataset
                .list_branches()
                .await
                .unwrap()
                .contains_key("feature"),
            "precondition: an earlier classifier observed target absence"
        );
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();
        let before = dataset
            .list_branches()
            .await
            .unwrap()
            .get("feature")
            .unwrap()
            .identifier
            .clone();

        assert!(
            !reclaim_ref_absent_tree(&mut dataset, "feature")
                .await
                .unwrap(),
            "the final authority check must refuse destructive cleanup"
        );
        assert_eq!(
            dataset
                .list_branches()
                .await
                .unwrap()
                .get("feature")
                .unwrap()
                .identifier,
            before
        );
    }

    #[tokio::test]
    async fn force_reclaim_reports_lexically_first_live_physical_path_child() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = test_dataset(&dir).await;
        let version = dataset.version().version;
        dataset
            .create_branch("feature/zeta", version, None)
            .await
            .unwrap();
        dataset
            .create_branch("feature/alpha", version, None)
            .await
            .unwrap();
        dataset
            .create_branch("feature", version, None)
            .await
            .unwrap();

        let error = force_delete_branch_idempotent(&mut dataset, "feature")
            .await
            .expect_err("ancestor reclaim must not report success while a path-child is live");
        assert!(error.to_string().contains("feature/alpha"));
        assert!(
            dataset
                .list_branches()
                .await
                .unwrap()
                .contains_key("feature"),
            "preflight refusal must preserve ancestor authority"
        );
    }
}
