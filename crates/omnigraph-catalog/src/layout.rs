use std::sync::Arc;

use lance::Dataset;
use lance::dataset::refs::BranchIdentifier;
#[cfg(any(test, feature = "test-util"))]
use lance_namespace::Error as LanceNamespaceError;

use crate::error::{OmniError, Result};
use crate::storage::{StorageKind, join_uri, storage_kind_for_uri};

use super::TableIdentity;

const MANIFEST_DIR: &str = "__manifest";
const HISTORY_DIR: &str = "__history";
const BRANCH_IDENTIFIER_CAPTURE_ATTEMPTS: usize = 8;

pub(crate) fn branch_ref_error(error: lance::Error, branch: &str) -> OmniError {
    match error {
        // Only Lance's typed ref miss proves logical branch absence. A generic
        // object miss can be the branch's manifest or another required object
        // and must remain an internal/storage failure rather than a public 404.
        lance::Error::RefNotFound { .. } => OmniError::BranchNotFound {
            branch: branch.to_string(),
        },
        other => OmniError::storage(other),
    }
}

pub fn manifest_uri(root: &str) -> String {
    format!("{}/{}", root.trim_end_matches('/'), MANIFEST_DIR)
}

/// The `__history` dataset of the graph at `root`, a sibling of `__manifest`
/// that every graph branch shares.
pub fn history_uri(root: &str) -> String {
    format!("{}/{}", root.trim_end_matches('/'), HISTORY_DIR)
}

/// Resolve a logical graph branch to its live native manifest ref.
///
/// The manifest dataset's ref list is the branch registry: a logical branch is
/// live iff exactly one native ref splits back to it (see `branch_names`).
/// Only that branch's own refs are read, so another branch's concurrent
/// retirement cannot fail this lookup. Absence is the typed public miss; more
/// than one incarnation fails loudly.
pub(crate) async fn resolve_native_manifest_branch(
    dataset: &Dataset,
    logical: &str,
) -> Result<String> {
    crate::branch_control::resolve_live_native_branch(dataset, logical)
        .await?
        .ok_or_else(|| OmniError::BranchNotFound {
            branch: logical.to_string(),
        })
}

#[cfg(any(test, feature = "test-util"))]
pub async fn open_manifest_dataset(root_uri: &str, branch: Option<&str>) -> Result<Dataset> {
    let control_session = crate::lance_access::control_session();
    open_manifest_dataset_with_session(root_uri, branch, &control_session).await
}

pub async fn open_manifest_dataset_with_session(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let dataset = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::manifest_wrapper(),
    )
    .await?;
    match branch {
        Some(branch) if branch != "main" => {
            let native = resolve_native_manifest_branch(&dataset, branch).await?;
            dataset
                .checkout_branch(&native)
                .await
                .map_err(|error| branch_ref_error(error, branch))
        }
        _ => Ok(dataset),
    }
}

/// Open one manifest branch by its NATIVE ref name, skipping resolution.
/// For callers that already hold a fresh listing (branch-delete's per-branch
/// dependency probe) and must not pay another per-branch listing.
pub async fn open_manifest_dataset_native_with_session(
    root_uri: &str,
    native: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let dataset = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::manifest_wrapper(),
    )
    .await?;
    match native {
        Some(native) if native != "main" => {
            dataset.checkout_branch(native).await.map_err(|error| {
                branch_ref_error(error, crate::branch_names::logical_branch_name(native))
            })
        }
        _ => Ok(dataset),
    }
}

/// Open one manifest branch together with the exact Lance branch lifetime
/// that selected it and the native ref name the logical branch resolved to
/// (`None` for main).
///
/// `checkout_branch` and `BranchContents` are separate reads. Surrounding the
/// checkout with identifier reads prevents a concurrent delete/recreate from
/// pairing an old checked-out dataset with the replacement branch's identity.
/// A later recreation is harmless: the returned identifier remains the
/// witness for this pinned dataset and the coordinator's freshness probe will
/// observe the new identity.
pub(crate) async fn open_manifest_branch_with_identifier(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, BranchIdentifier, Option<String>)> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let dataset = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::manifest_wrapper(),
    )
    .await?;
    let Some(branch) = branch.filter(|branch| *branch != "main") else {
        return Ok((dataset, BranchIdentifier::main(), None));
    };
    let native = resolve_native_manifest_branch(&dataset, branch).await?;

    for _ in 0..BRANCH_IDENTIFIER_CAPTURE_ATTEMPTS {
        let before = crate::branch_control::get_branch_identifier(&dataset, &native)
            .await
            .map_err(|error| branch_ref_error(error, branch))?;
        let branch_dataset = dataset
            .checkout_branch(&native)
            .await
            .map_err(|error| branch_ref_error(error, branch))?;
        let after = crate::branch_control::get_branch_identifier(&dataset, &native)
            .await
            .map_err(|error| branch_ref_error(error, branch))?;
        if before == after {
            return Ok((branch_dataset, before, Some(native)));
        }
        tokio::task::yield_now().await;
    }

    Err(OmniError::manifest_conflict(format!(
        "manifest branch '{branch}' changed repeatedly during coherent open; retry"
    )))
}

pub(crate) fn table_object_id(identity: TableIdentity) -> String {
    format!(
        "table:{:016x}:{:016x}",
        identity.stable_table_id, identity.table_incarnation_id
    )
}

/// Row key of a `replaced_table` row: the identity and the `__manifest`
/// version whose publish replaced the row.
pub(crate) fn replaced_table_object_id(identity: TableIdentity, replaced_at: u64) -> String {
    format!("{}@{replaced_at}", table_object_id(identity))
}

pub(crate) fn table_uri_for_path(
    root_uri: &str,
    table_path: &str,
    branch: Option<&str>,
) -> Result<String> {
    let mut dataset_location = join_uri(root_uri, table_path);
    if let Some(branch) = branch.filter(|branch| *branch != "main") {
        dataset_location = join_uri(&dataset_location, "tree");
        for segment in branch.split('/') {
            dataset_location = join_uri(&dataset_location, segment);
        }
    }
    match storage_kind_for_uri(root_uri)? {
        StorageKind::Local => Ok(url::Url::from_file_path(&dataset_location)
            .map(|uri| uri.to_string())
            .unwrap_or(dataset_location)),
        StorageKind::S3 | StorageKind::Azure => Ok(dataset_location),
    }
}

#[cfg(any(test, feature = "test-util"))]
pub(crate) fn namespace_internal_error(message: impl Into<String>) -> LanceNamespaceError {
    LanceNamespaceError::namespace_source(Box::new(std::io::Error::other(message.into())))
}
