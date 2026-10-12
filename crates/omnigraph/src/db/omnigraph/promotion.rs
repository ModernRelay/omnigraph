//! Table pins and their detached chains (RFC 0067, detached-only tables).
//! Writers stage on the pin (`open_pinned_for_write`) and never promote it;
//! `table_location` and `open_at` resolve a pin's versions, and
//! `promote_pin_at_head` judges one for `repair` without replaying.

use crate::db::manifest::DatasetEntry;
use crate::db::omnigraph::Omnigraph;
use crate::error::{OmniError, Result};
use crate::instrumentation::{VersionResolution, open_dataset, table_wrapper};
use crate::storage_layer::SnapshotHandle;

/// What a pin's linear target holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Promotion {
    /// The target already carried the pin's transaction.
    AlreadyPromoted,
    /// The pin cannot be promoted; the reason names why. Reads stay correct
    /// through the staged version.
    Blocked(String),
}

/// Where a table's versions live: the dataset root for main, the native
/// branch tree otherwise.
pub(crate) fn table_location(full_path: &str, table_branch: Option<&str>) -> String {
    match table_branch.filter(|branch| *branch != "main") {
        Some(branch) => {
            let mut location = crate::storage::join_uri(full_path, "tree");
            for segment in branch.split('/') {
                location = crate::storage::join_uri(&location, segment);
            }
            location
        }
        None => full_path.to_string(),
    }
}

/// Open one version of a table, or `None` when that version is reclaimed.
pub(crate) async fn open_at(
    db: &Omnigraph,
    location: &str,
    version: u64,
) -> Result<Option<SnapshotHandle>> {
    let session = db.read_caches().session.clone();
    match open_dataset(
        location,
        VersionResolution::At(version),
        Some(&session),
        table_wrapper(),
    )
    .await
    {
        Ok(dataset) => Ok(Some(SnapshotHandle::new(dataset))),
        Err(OmniError::HistoricalVersionReclaimed { .. }) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Whether the target carries the pin's transaction, a foreign one, or is
/// absent.
async fn target_state(
    db: &Omnigraph,
    location: &str,
    target: u64,
    uuid: &str,
) -> Result<Option<Promotion>> {
    let Some(existing) = open_at(db, location, target).await? else {
        return Ok(None);
    };
    Ok(Some(match db.storage().transaction_identity(&existing) {
        Ok(link) if link.uuid == uuid => Promotion::AlreadyPromoted,
        _ => Promotion::Blocked(format!(
            "linear version {target} exists with a foreign or unreadable transaction"
        )),
    }))
}

/// Open the pinned base a writer stages on: a held handle costs no request,
/// otherwise one pinned open. The linear HEAD is never consulted; a foreign
/// commit above the pin is `repair`'s `foreign_drift`, not the writer's.
pub(crate) async fn open_pinned_for_write(
    db: &Omnigraph,
    full_path: &str,
    entry: &DatasetEntry,
) -> Result<SnapshotHandle> {
    resolve_pinned_for_write(db, full_path, entry)
        .await
        .map(SnapshotHandle::new)
}

async fn resolve_pinned_for_write(
    db: &Omnigraph,
    full_path: &str,
    entry: &DatasetEntry,
) -> Result<lance::Dataset> {
    let caches = db.read_caches();
    let target = entry.published_dataset_version;
    let table_branch = entry.native_dataset_branch.as_deref();
    let e_tag = entry.version_metadata.e_tag();
    let location = table_location(full_path, table_branch);
    let dataset = match held_twin(db, entry).await {
        Some(held) => held,
        None => {
            caches
                .handles
                .get_or_open(
                    &entry.dataset_path,
                    table_branch,
                    target,
                    e_tag,
                    entry.version_metadata.staged_version(),
                    entry.version_metadata.transaction_uuid(),
                    entry.version_metadata.last_linear_version(),
                    &location,
                    Some(&caches.session),
                )
                .await?
        }
    };
    Ok(dataset)
}

/// The pin's version when this process holds it in the read-handle cache;
/// it costs no request.
async fn held_twin(db: &Omnigraph, entry: &DatasetEntry) -> Option<lance::Dataset> {
    let target = entry.published_dataset_version;
    db.read_caches()
        .handles
        .get(
            &entry.dataset_path,
            entry.native_dataset_branch.as_deref(),
            target,
            entry.version_metadata.staged_version(),
            entry.version_metadata.e_tag(),
        )
        .await
        .filter(|held| {
            held.version().version == entry.version_metadata.staged_version().unwrap_or(target)
        })
}

/// What a writer that plans on a table's linear HEAD finds at the pin.
pub(crate) enum PinAtHead {
    /// HEAD carries the pin, or the entry names no detached version.
    Carried,
    /// The pin cannot be promoted; the reason names why.
    Blocked(String),
}

/// Judge the pin from the HEAD `repair` opened: a HEAD at the pin's version
/// is checked by its recorded transaction, a HEAD beyond it asks the target
/// once, and a HEAD behind it is `Blocked`, since nothing promotes a pin.
pub(crate) async fn promote_pin_at_head(
    db: &Omnigraph,
    table_key: &str,
    full_path: &str,
    entry: &DatasetEntry,
    head: &SnapshotHandle,
) -> Result<PinAtHead> {
    let (Some(_), Some(uuid)) = (
        entry.version_metadata.staged_version(),
        entry.version_metadata.transaction_uuid(),
    ) else {
        return Ok(PinAtHead::Carried);
    };
    let target = entry.published_dataset_version;
    let table_branch = entry.native_dataset_branch.as_deref();
    if head.version() == target {
        return Ok(match db.storage().transaction_identity(head) {
            Ok(link) if link.uuid == uuid => PinAtHead::Carried,
            _ => PinAtHead::Blocked(format!(
                "linear version {target} exists with a foreign or unreadable transaction"
            )),
        });
    }
    if head.version() > target {
        // Promoted and advanced since, or drift the writer's own baseline
        // check reports; only a foreign transaction at the target blocks.
        let location = table_location(full_path, table_branch);
        return Ok(match target_state(db, &location, target, uuid).await? {
            Some(Promotion::Blocked(reason)) => PinAtHead::Blocked(reason),
            _ => PinAtHead::Carried,
        });
    }
    Ok(PinAtHead::Blocked(format!(
        "{table_key}: promotion to version {target} refused by the no-promotion inventory feature"
    )))
}
