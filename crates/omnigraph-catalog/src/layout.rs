use std::sync::Arc;

use lance::Dataset;
use lance::dataset::refs::BranchIdentifier;
#[cfg(any(test, feature = "test-util"))]
use lance_namespace::Error as LanceNamespaceError;

use crate::error::{OmniError, Result};
use crate::instrumentation::VersionResolution;
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

/// Whether a failed checkout of a tree's latest version found no version
/// under its `_versions/`: Lance answers that listing with `NotFound`, and a
/// version it listed whose manifest object is missing with `DatasetNotFound`.
pub(crate) fn tree_never_written(error: &lance::Error) -> bool {
    matches!(error, lance::Error::NotFound { .. })
}

/// Whether a failed checkout leaves a delete no head to end with a tombstone:
/// a tree never written, or a listed version whose manifest object is gone.
pub(crate) fn tree_has_no_head(error: &lance::Error) -> bool {
    tree_never_written(error) || matches!(error, lance::Error::DatasetNotFound { .. })
}

/// A failed read of the tree a live row names (RFC "Branch existence in the
/// catalog" §Readers): no version under its `_versions/` is the not-ready
/// verdict, a row whose create wrote no tree; the rest is the read's own.
pub(crate) fn branch_tree_error(error: lance::Error, branch: &str) -> OmniError {
    match tree_never_written(&error) {
        true => OmniError::BranchNotReady {
            branch: branch.to_string(),
        },
        false => OmniError::storage(error),
    }
}

/// Whether `main`'s stamp keeps the branch set in its rows, so a tree is
/// opened through them and never through a ref.
pub fn has_branch_rows(main: &Dataset) -> bool {
    crate::migrations::guard_stamp(main).is_ok_and(crate::migrations::supports_branch_rows)
}

/// The error of a branch checkout from `main` at `resolution`: the tree's
/// for its latest version at or above the branch-row stamp, the ref's below
/// it; a known version that is missing is the storage miss it is.
pub(crate) fn branch_open_error(
    main: &Dataset,
    resolution: VersionResolution,
    error: lance::Error,
    branch: &str,
) -> OmniError {
    match (has_branch_rows(main), resolution) {
        (true, VersionResolution::Latest) => branch_tree_error(error, branch),
        (true, VersionResolution::At(_)) => OmniError::storage(error),
        (false, _) => branch_ref_error(error, branch),
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
    if has_branch_rows(dataset) {
        return crate::BranchRows::decode(dataset.manifest())?
            .by_logical(logical)
            .map(|row| row.native.clone())
            .ok_or_else(|| OmniError::BranchNotFound {
                branch: logical.to_string(),
            });
    }
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
    open_manifest_branch(root_uri, branch, VersionResolution::Latest, control_session).await
}

/// [`open_manifest_dataset_with_session`] keeping the main the branch was
/// resolved through, whose rows answer the branch's identifier at or above
/// the branch-row stamp; `None` when main itself was opened.
pub(crate) async fn open_manifest_dataset_through_main(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, Option<Dataset>)> {
    let Some(branch) = branch.filter(|branch| *branch != "main") else {
        let main = open_main_manifest(root_uri, VersionResolution::Latest, control_session).await?;
        return Ok((main, None));
    };
    let main = open_main_manifest(root_uri, VersionResolution::Latest, control_session).await?;
    let native = resolve_native_manifest_branch(&main, branch).await?;
    let dataset = checkout_manifest_branch(&main, &native, VersionResolution::Latest)
        .await
        .map_err(|error| branch_open_error(&main, VersionResolution::Latest, error, branch))?;
    Ok((dataset, Some(main)))
}

/// Open one manifest branch at a `version` the caller already knows,
/// resolving only that version. Main opens at the version directly; a named
/// branch is resolved to its native ref from main as
/// [`open_manifest_dataset_with_session`] does, then checked out at the
/// version, never at its latest first. Lance's checkout still refuses a
/// manifest that belongs to another branch.
pub async fn open_manifest_dataset_at_with_session(
    root_uri: &str,
    branch: Option<&str>,
    version: u64,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    open_manifest_branch(
        root_uri,
        branch,
        VersionResolution::At(version),
        control_session,
    )
    .await
}

/// Open one manifest branch by its NATIVE ref name, skipping resolution.
/// For callers that already hold a fresh listing (branch-delete's per-branch
/// dependency probe) and must not pay another per-branch listing.
pub async fn open_manifest_dataset_native_with_session(
    root_uri: &str,
    native: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    open_manifest_native(root_uri, native, VersionResolution::Latest, control_session).await
}

/// [`open_manifest_dataset_at_with_session`] for a caller that already holds
/// the branch's NATIVE ref name, as [`open_manifest_dataset_native_with_session`].
pub async fn open_manifest_dataset_native_at_with_session(
    root_uri: &str,
    native: Option<&str>,
    version: u64,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    open_manifest_native(
        root_uri,
        native,
        VersionResolution::At(version),
        control_session,
    )
    .await
}

/// The one body of the logical-name opens: main at `resolution`, or the named
/// branch's native ref, resolved from main's latest, checked out at
/// `resolution`.
async fn open_manifest_branch(
    root_uri: &str,
    branch: Option<&str>,
    resolution: VersionResolution,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    let Some(branch) = branch.filter(|branch| *branch != "main") else {
        return open_main_manifest(root_uri, resolution, control_session).await;
    };
    let main = open_main_manifest(root_uri, VersionResolution::Latest, control_session).await?;
    let native = resolve_native_manifest_branch(&main, branch).await?;
    checkout_manifest_branch(&main, &native, resolution)
        .await
        .map_err(|error| branch_open_error(&main, resolution, error, branch))
}

/// The one body of the native-name opens: [`open_manifest_branch`] without
/// resolving the native ref.
async fn open_manifest_native(
    root_uri: &str,
    native: Option<&str>,
    resolution: VersionResolution,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    let Some(native) = native.filter(|native| *native != "main") else {
        return open_main_manifest(root_uri, resolution, control_session).await;
    };
    let main = open_main_manifest(root_uri, VersionResolution::Latest, control_session).await?;
    checkout_manifest_branch(&main, native, resolution)
        .await
        .map_err(|error| {
            branch_open_error(
                &main,
                resolution,
                error,
                crate::branch_names::logical_branch_name(native),
            )
        })
}

/// Main's `__manifest` at `resolution`. A version the caller already holds
/// opens through [`crate::instrumentation::open_dataset_at_version`], so a
/// miss stays the storage error it is rather than reclaimed table history.
async fn open_main_manifest(
    root_uri: &str,
    resolution: VersionResolution,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let wrapper = crate::instrumentation::manifest_wrapper();
    match resolution {
        VersionResolution::Latest => {
            crate::instrumentation::open_dataset(&uri, resolution, Some(control_session), wrapper)
                .await
        }
        VersionResolution::At(version) => {
            crate::instrumentation::open_dataset_at_version(
                &uri,
                version,
                Some(control_session),
                wrapper,
            )
            .await
        }
    }
}

/// A native branch of the opened `main`, at its latest or at a known version.
async fn checkout_manifest_branch(
    main: &Dataset,
    native: &str,
    resolution: VersionResolution,
) -> lance::Result<Dataset> {
    match resolution {
        VersionResolution::Latest => main.checkout_branch(native).await,
        VersionResolution::At(version) => main.checkout_version((native, version)).await,
    }
}

/// One manifest branch opened at its latest version with the lifetime that
/// selected it: the identifier, the native name the logical branch resolved
/// to (`None` for main), and the main the branch resolved through (`None` for
/// main), whose rows answer the branch's existence without a request at or
/// above [`crate::migrations::BRANCH_ROWS_STAMP`].
pub(crate) struct OpenedBranch {
    pub(crate) dataset: Dataset,
    pub(crate) branch_identifier: BranchIdentifier,
    pub(crate) native_branch: Option<String>,
    pub(crate) main: Option<Dataset>,
}

impl OpenedBranch {
    /// Main itself, opened at `dataset`.
    pub(crate) fn main(dataset: Dataset) -> Self {
        Self {
            dataset,
            branch_identifier: BranchIdentifier::main(),
            native_branch: None,
            main: None,
        }
    }
}

/// RFC "Branch existence in the catalog" §Readers: a chain whose head carries
/// a tombstone refuses a reader's open and refresh with `branch_deleted`; the
/// delete's own open and the collector's inventory read the row and go on.
pub(crate) fn refuse_tombstoned(opened: &OpenedBranch, branch: Option<&str>) -> Result<()> {
    let Some(branch) = branch.filter(|branch| *branch != "main") else {
        return Ok(());
    };
    if crate::BranchRows::decode(opened.dataset.manifest())?
        .tombstone()
        .is_some()
    {
        return Err(OmniError::BranchDeleted {
            branch: branch.to_string(),
        });
    }
    Ok(())
}

/// Open one manifest branch together with the exact Lance branch lifetime
/// that selected it and the native name the logical branch resolved to.
///
/// At or above the branch-row stamp the lifetime is the row of the main
/// opened here, and the tree is checked out by that row's native name. Below
/// it, `checkout_branch` and `BranchContents` are separate reads: surrounding
/// the checkout with identifier reads prevents a concurrent delete/recreate
/// from pairing an old checked-out dataset with the replacement branch's
/// identity. A later recreation is harmless: the returned identifier remains
/// the witness for this pinned dataset and the coordinator's freshness probe
/// will observe the new identity.
pub(crate) async fn open_manifest_branch_with_identifier(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<OpenedBranch> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let dataset = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::manifest_wrapper(),
    )
    .await?;
    let Some(branch) = branch.filter(|branch| *branch != "main") else {
        return Ok(OpenedBranch::main(dataset));
    };
    if crate::migrations::supports_branch_rows(crate::migrations::guard_stamp(&dataset)?) {
        let row = crate::BranchRows::decode(dataset.manifest())?
            .by_logical(branch)
            .cloned()
            .ok_or_else(|| OmniError::BranchNotFound {
                branch: branch.to_string(),
            })?;
        let branch_dataset = dataset
            .checkout_branch(&row.native)
            .await
            .map_err(|error| branch_tree_error(error, branch))?;
        return Ok(OpenedBranch {
            dataset: branch_dataset,
            branch_identifier: row.identifier,
            native_branch: Some(row.native),
            main: Some(dataset),
        });
    }
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
            return Ok(OpenedBranch {
                dataset: branch_dataset,
                branch_identifier: before,
                native_branch: Some(native),
                main: Some(dataset),
            });
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

/// Row key of a `history_block` row: the block and the run of slots it describes.
pub fn history_block_object_id(block: ulid::Ulid, first_slot: u16, last_slot: u16) -> String {
    format!("history_block:{block}:{first_slot}-{last_slot}")
}

/// Row key of a `history_merge` row: the block and the slot of the merge commit.
pub fn history_merge_object_id(block: ulid::Ulid, slot: u16) -> String {
    format!("history_merge:{block}:{slot}")
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
