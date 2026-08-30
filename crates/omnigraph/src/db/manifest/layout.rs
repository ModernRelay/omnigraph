use std::sync::Arc;

use lance::Dataset;
use lance::dataset::refs::BranchIdentifier;
use lance_namespace::Error as LanceNamespaceError;

use crate::error::{OmniError, Result};
use crate::storage::{StorageKind, join_uri, storage_kind_for_uri};

use super::TableIdentity;

const MANIFEST_DIR: &str = "__manifest";
const BRANCH_IDENTIFIER_CAPTURE_ATTEMPTS: usize = 8;

pub(super) fn branch_ref_error(error: lance::Error, branch: &str) -> OmniError {
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

pub(crate) fn manifest_uri(root: &str) -> String {
    format!("{}/{}", root.trim_end_matches('/'), MANIFEST_DIR)
}

#[cfg(test)]
pub(super) async fn open_manifest_dataset(root_uri: &str, branch: Option<&str>) -> Result<Dataset> {
    let control_session = crate::lance_access::control_session();
    open_manifest_dataset_with_session(root_uri, branch, &control_session).await
}

pub(super) async fn open_manifest_dataset_with_session(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    Ok(
        open_manifest_dataset_resolved_with_session(root_uri, branch, control_session)
            .await?
            .0,
    )
}

/// The native ref one logical branch name resolves to for this open, plus its
/// registry row when one exists (legacy pre-registry branches have none).
pub(super) struct ResolvedBranchCheckout {
    pub(super) native_ref: String,
    pub(super) registration: Option<super::state::BranchRegistration>,
}

/// Resolve a logical branch name to the native ref of its CURRENT life
/// (issue #562), reading the branch registry from the given ROOT-manifest
/// dataset. `main` never resolves (callers filter it), internal system refs
/// pass through untouched (their name is the token), a name with no registry
/// row falls back to itself (a legacy pre-registry branch), and a dead row is
/// branch absence — the logical name does not exist between a delete and the
/// next rebirth.
pub(super) async fn resolve_native_branch_checkout(
    root_dataset: &Dataset,
    logical: &str,
) -> Result<ResolvedBranchCheckout> {
    if crate::db::is_internal_system_branch(logical) {
        return Ok(ResolvedBranchCheckout {
            native_ref: logical.to_string(),
            registration: None,
        });
    }
    // A name already carrying an incarnation token is a NATIVE ref: the
    // public namespace reserves the separator, so only engine-internal
    // callers hold one, and they are addressing an exact life — no registry
    // scan needed (this keeps per-branch probes at one open + state read).
    if crate::db::branch_identity::split_native_branch_ref(logical)
        .1
        .is_some()
    {
        return Ok(ResolvedBranchCheckout {
            native_ref: logical.to_string(),
            registration: None,
        });
    }
    let mut registry = super::state::read_branch_registry(root_dataset).await?;
    match registry.remove(logical) {
        Some(registration) if registration.live => Ok(ResolvedBranchCheckout {
            native_ref: registration.native_ref.clone(),
            registration: Some(registration),
        }),
        Some(_dead) => Err(OmniError::BranchNotFound {
            branch: logical.to_string(),
        }),
        None => Ok(ResolvedBranchCheckout {
            native_ref: logical.to_string(),
            registration: None,
        }),
    }
}

/// Forward recovery for the branch-create crash window (issue #562): the
/// registry commit landed (the commit point) but the native ref was never
/// created. Re-verifies the registration against a FRESH root manifest —
/// a concurrent delete marks the row dead before touching the ref, so a
/// still-live row proves the ref is missing because of the crash window, not
/// a delete in flight — then completes the ref at the pinned source version.
/// (A lost race after the re-read at worst recreates a ref whose row just
/// went dead: an unreferenced ref that is invisible to listing and
/// resolution. Table-level fork garbage is the orphan reconciler's job;
/// a manifest-level ref like this one lingers until manual cleanup.)
async fn complete_registered_branch_ref(
    root_uri: &str,
    control_session: &Arc<lance::session::Session>,
    logical: &str,
    expected_native: &str,
) -> Result<()> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let fresh = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::manifest_wrapper(),
    )
    .await?;
    let registry = super::state::read_branch_registry(&fresh).await?;
    let Some(registration) = registry.get(logical) else {
        return Err(OmniError::BranchNotFound {
            branch: logical.to_string(),
        });
    };
    if !registration.live || registration.native_ref != expected_native {
        return Err(OmniError::BranchNotFound {
            branch: logical.to_string(),
        });
    }
    let mut source_dataset = match registration.source_ref.as_deref() {
        Some(source_ref) => fresh
            .checkout_branch(source_ref)
            .await
            .map_err(|error| branch_ref_error(error, source_ref))?,
        None => fresh,
    };
    crate::branch_control::complete_branch_ref_at_version(
        &mut source_dataset,
        &registration.native_ref,
        registration.source_version,
    )
    .await
    .map(|_| ())
}

/// Open one manifest branch by LOGICAL name, resolving it to its current
/// life's native ref. Returns the native ref actually checked out (`None`
/// for main) so lifecycle callers can address Lance directly.
pub(super) async fn open_manifest_dataset_resolved_with_session(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, Option<String>)> {
    // Boxed AND type-erased wholesale: the resolution seam (registry scan +
    // forward-recovery completion) rides inside already-deep coordinator
    // futures — the engine's known stack-depth hazard — and the erasure also
    // keeps downstream auto-trait proofs (axum handler bounds) from recursing
    // through the whole Lance future tree.
    let fut: std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send + '_>> = Box::pin(
        open_manifest_dataset_resolved_inner(root_uri, branch, control_session),
    );
    fut.await
}

async fn open_manifest_dataset_resolved_inner(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, Option<String>)> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let dataset = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::manifest_wrapper(),
    )
    .await?;
    let Some(branch) = branch.filter(|branch| *branch != "main") else {
        return Ok((dataset, None));
    };
    let resolved = resolve_native_branch_checkout(&dataset, branch).await?;
    match dataset.checkout_branch(&resolved.native_ref).await {
        Ok(checked_out) => Ok((checked_out, Some(resolved.native_ref))),
        Err(lance::Error::RefNotFound { .. }) if resolved.registration.is_some() => {
            complete_registered_branch_ref(root_uri, control_session, branch, &resolved.native_ref)
                .await?;
            let checked_out = dataset
                .checkout_branch(&resolved.native_ref)
                .await
                .map_err(|error| branch_ref_error(error, branch))?;
            Ok((checked_out, Some(resolved.native_ref)))
        }
        // Typed absence, named by the LOGICAL branch: reached for a
        // pass-through native ref whose life is refless (the create crash
        // window) or already reclaimed — errors speak logical (issue #562).
        Err(error) => Err(branch_ref_error(
            error,
            crate::db::branch_identity::split_native_branch_ref(branch).0,
        )),
    }
}

/// Open one manifest branch together with the exact Lance branch lifetime
/// that selected it.
///
/// `checkout_branch` and `BranchContents` are separate reads. Surrounding the
/// checkout with identifier reads prevents a concurrent delete/recreate from
/// pairing an old checked-out dataset with the replacement branch's identity.
/// A later recreation is harmless: the returned identifier remains the
/// witness for this pinned dataset and the coordinator's freshness probe will
/// observe the new identity.
pub(super) async fn open_manifest_dataset_with_identifier_with_session(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<(Dataset, BranchIdentifier, Option<String>)> {
    // Boxed and type-erased for the same reasons as the resolved open above.
    let fut: std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send + '_>> = Box::pin(
        open_manifest_dataset_with_identifier_inner(root_uri, branch, control_session),
    );
    fut.await
}

async fn open_manifest_dataset_with_identifier_inner(
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
    // Resolve the logical name to its current life's native ref (issue #562);
    // the identifier capture below then witnesses exactly that life. A
    // concurrent rebirth changes the NAME, so a stale resolution surfaces as
    // ref absence, never as silently pairing with the replacement life.
    let resolved = resolve_native_branch_checkout(&dataset, branch).await?;
    let native = resolved.native_ref.as_str();
    // Errors speak LOGICAL (issue #562): a pass-through caller supplies the
    // native spelling, so split before naming; logical callers split to
    // themselves.
    let error_name = crate::db::branch_identity::split_native_branch_ref(branch).0;

    let mut completed_missing_ref = false;
    for _ in 0..BRANCH_IDENTIFIER_CAPTURE_ATTEMPTS {
        let before = match dataset.branches().get_identifier(Some(native)).await {
            Ok(identifier) => identifier,
            Err(lance::Error::RefNotFound { .. })
                if resolved.registration.is_some() && !completed_missing_ref =>
            {
                // Forward recovery: registry committed, native ref unmade.
                complete_registered_branch_ref(root_uri, control_session, branch, native).await?;
                completed_missing_ref = true;
                continue;
            }
            Err(error) => return Err(branch_ref_error(error, error_name)),
        };
        let branch_dataset = dataset
            .checkout_branch(native)
            .await
            .map_err(|error| branch_ref_error(error, error_name))?;
        let after = dataset
            .branches()
            .get_identifier(Some(native))
            .await
            .map_err(|error| branch_ref_error(error, error_name))?;
        if before == after {
            return Ok((branch_dataset, before, Some(resolved.native_ref)));
        }
        tokio::task::yield_now().await;
    }

    Err(OmniError::manifest_conflict(format!(
        "manifest branch '{error_name}' changed repeatedly during coherent open; retry"
    )))
}

fn format_table_version(version: u64) -> String {
    format!("{version:020}")
}

pub(super) fn table_object_id(identity: TableIdentity) -> String {
    format!(
        "table:{:016x}:{:016x}",
        identity.stable_table_id, identity.table_incarnation_id
    )
}

pub(super) fn version_object_id(identity: TableIdentity, version: u64) -> String {
    format!(
        "table_version:{:016x}:{:016x}:{}",
        identity.stable_table_id,
        identity.table_incarnation_id,
        format_table_version(version)
    )
}

pub(super) fn tombstone_object_id(identity: TableIdentity, version: u64) -> String {
    format!(
        "table_tombstone:{:016x}:{:016x}:{}",
        identity.stable_table_id,
        identity.table_incarnation_id,
        format_table_version(version)
    )
}

pub(super) fn table_id_to_key(request_id: Option<&Vec<String>>) -> lance_namespace::Result<String> {
    match request_id {
        Some(request_id) if request_id.len() == 1 && !request_id[0].is_empty() => {
            Ok(request_id[0].clone())
        }
        Some(request_id) => Err(LanceNamespaceError::invalid_input(format!(
            "expected single table id component, got {:?}",
            request_id
        ))),
        None => Err(LanceNamespaceError::invalid_input("table id is required")),
    }
}

pub(super) fn table_uri_for_path(
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

#[cfg(test)]
pub(super) fn namespace_internal_error(message: impl Into<String>) -> LanceNamespaceError {
    LanceNamespaceError::namespace_source(Box::new(std::io::Error::other(message.into())))
}
