//! Promotion of table pins onto their linear history (RFC 0067).
//!
//! A pin published as `(target, staged, uuid)` names a detached Lance version
//! whose twin has not yet been written at `target`. Promotion replays the
//! recorded transaction at `target - 1` through the sealed storage boundary.
//! It is derived, idempotent reconciler work: any process may run it, two
//! promoters racing on one pin leave exactly one twin, and a pin that cannot
//! be promoted because a foreign commit occupies its target stays readable
//! through its staged version and degrades only reclamation.
//!
//! A chain of detached commits links itself: every detached commit records
//! the version it was staged on, so a pin whose linear base is absent is the
//! tip of a chain found by following read-version links, never by reading
//! manifest history.

use crate::db::omnigraph::Omnigraph;
use crate::error::{OmniError, Result};
use crate::instrumentation::{VersionResolution, open_dataset, table_wrapper};
use crate::storage_layer::{PromotionOutcome, SnapshotHandle};

/// Outcome of promoting one pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Promotion {
    /// The twin landed at the target.
    Promoted(u64),
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

/// Where Lance keeps a detached version's manifest: `d{version}.manifest`
/// under `_versions`, the masked version spelled in full.
pub(crate) fn detached_manifest_path(location: &str, staged: u64) -> String {
    format!("{location}/_versions/d{staged}.manifest")
}

/// Open one version of a table, or `None` when that version is reclaimed.
async fn open_at(db: &Omnigraph, location: &str, version: u64) -> Result<Option<SnapshotHandle>> {
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

/// Follow a detached tip's read-version links back to the linear base it was
/// staged from. Returns the base, or `None` when the walk met a predecessor
/// whose detached manifest is already reaped, and the chain tip first as
/// `(version, uuid)`.
pub(crate) async fn walk_chain(
    db: &Omnigraph,
    location: &str,
    tip: u64,
) -> Result<(Option<u64>, Vec<(u64, String)>)> {
    let mut chain = Vec::new();
    let mut version = tip;
    loop {
        let Some(handle) = open_at(db, location, version).await? else {
            if chain.is_empty() {
                return Err(OmniError::HistoricalVersionReclaimed {
                    published_dataset_version: version,
                });
            }
            return Ok((None, chain));
        };
        let link = db.storage().transaction_identity(&handle)?;
        let base_is_detached = link.base_is_detached();
        let read_version = link.read_version;
        chain.push((version, link.uuid));
        if !base_is_detached {
            return Ok((Some(read_version), chain));
        }
        version = read_version;
    }
}

/// Promote the pin `(target, staged, uuid)` of one table, and every detached
/// commit behind it, oldest first. A promoter that did not stage the pin
/// checks the target first: the same pin is registered on every branch that
/// inherits it, and a promoted pin's detached manifest may already be
/// reaped. Bounded by the chain length.
pub(crate) async fn promote_pin(
    db: &Omnigraph,
    table_key: &str,
    full_path: &str,
    table_branch: Option<&str>,
    target: u64,
    staged: u64,
    uuid: &str,
) -> Result<Promotion> {
    let location = table_location(full_path, table_branch);
    if let Some(known) = target_state(db, &location, target, uuid).await? {
        return Ok(known);
    }
    let (base, chain) = walk_chain(db, &location, staged).await?;
    if chain[0].1 != uuid {
        return Ok(Promotion::Blocked(
            "staged version carries no matching transaction".to_string(),
        ));
    }
    let base = match base {
        Some(base) if base + chain.len() as u64 == target => base,
        Some(base) => {
            return Ok(Promotion::Blocked(format!(
                "{table_key}: a chain of {} detached commits on linear base {base} does not reach target {target}",
                chain.len()
            )));
        }
        None => {
            let base = target.saturating_sub(chain.len() as u64);
            if open_at(db, &location, base).await?.is_none() {
                return Ok(Promotion::Blocked(format!(
                    "{table_key}: chain predecessors were reclaimed and linear base {base} is absent"
                )));
            }
            base
        }
    };
    for (index, (staged, uuid)) in chain.iter().rev().enumerate() {
        let step_target = base + 1 + index as u64;
        match promote_step(db, &location, step_target, *staged, uuid).await? {
            Promotion::Promoted(_) | Promotion::AlreadyPromoted => {}
            blocked => return Ok(blocked),
        }
    }
    Ok(Promotion::Promoted(target))
}

/// One link of a chain: replay `staged` at `target`, rechecking the target
/// when Lance refuses the replay because a racing promoter landed first.
async fn promote_step(
    db: &Omnigraph,
    location: &str,
    target: u64,
    staged: u64,
    uuid: &str,
) -> Result<Promotion> {
    if let Some(known) = target_state(db, location, target, uuid).await? {
        return Ok(known);
    }
    let Some(base) = open_at(db, location, target - 1).await? else {
        return Ok(Promotion::Blocked(format!(
            "linear base {} is absent",
            target - 1
        )));
    };
    let Some(staged_handle) = open_at(db, location, staged).await? else {
        return Ok(Promotion::Blocked(format!(
            "staged version {staged} is reclaimed"
        )));
    };
    match db
        .storage()
        .promote_detached(base, &staged_handle, target, uuid)
        .await?
    {
        PromotionOutcome::Landed(twin) => {
            debug_assert_eq!(twin.version(), target);
            Ok(Promotion::Promoted(target))
        }
        PromotionOutcome::Refused => Ok(target_state(db, location, target, uuid)
            .await?
            .unwrap_or_else(|| {
                Promotion::Blocked("replay refused and the target is absent".to_string())
            })),
        PromotionOutcome::Unsafe(reason) => Ok(Promotion::Blocked(reason)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::manifest::DatasetEntry;
    use crate::instrumentation::open_pinned_dataset;
    use crate::loader::LoadMode;
    use crate::table_store::{StagedTransactionIdentity, TableStore};

    const SCHEMA: &str = "node Person { name: String @key }\n";
    const PERSON: &str = "node:Person";

    /// A graph whose Person table holds `a`..`h` after one linear load.
    async fn graph_with_people() -> (tempfile::TempDir, Omnigraph) {
        let dir = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        let rows: Vec<String> = ('a'..='h')
            .map(|name| format!(r#"{{"type":"Person","data":{{"name":"{name}"}}}}"#))
            .collect();
        db.load_as("main", None, &rows.join("\n"), LoadMode::Merge, None)
            .await
            .unwrap();
        (dir, db)
    }

    async fn person_entry(db: &Omnigraph) -> DatasetEntry {
        db.snapshot_for_branch(None)
            .await
            .unwrap()
            .dataset(PERSON)
            .unwrap()
            .clone()
    }

    /// The table's full path and the location its versions live under.
    fn location_of(db: &Omnigraph, entry: &DatasetEntry) -> (String, String) {
        let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
        let location = table_location(&full_path, entry.native_dataset_branch.as_deref());
        (full_path, location)
    }

    /// Stage one delete on `base` and commit it as a detached version.
    async fn detached_delete(
        db: &Omnigraph,
        base: SnapshotHandle,
        name: &str,
    ) -> (SnapshotHandle, StagedTransactionIdentity) {
        let staged = db
            .storage()
            .stage_delete(&base, &format!("name = '{name}'"))
            .await
            .unwrap()
            .expect("the row exists");
        db.storage()
            .commit_staged_detached(base, staged)
            .await
            .unwrap()
    }

    async fn uuid_at(db: &Omnigraph, location: &str, version: u64) -> Option<String> {
        open_at(db, location, version)
            .await
            .unwrap()
            .map(|handle| db.storage().transaction_identity(&handle).unwrap().uuid)
    }

    async fn rows_at(db: &Omnigraph, location: &str, version: u64) -> usize {
        let handle = open_at(db, location, version).await.unwrap().unwrap();
        db.storage().count_rows(&handle, None).await.unwrap()
    }

    async fn load_one(db: &Omnigraph, name: &str) {
        let row = format!(r#"{{"type":"Person","data":{{"name":"{name}"}}}}"#);
        db.load_as("main", None, &row, LoadMode::Merge, None)
            .await
            .unwrap();
    }

    /// GQT cannot observe a detached Lance version: it never appears in a
    /// manifest. A detached commit is private, links to its base, resolves
    /// through its pin, and promotion lands its twin exactly once.
    #[tokio::test]
    async fn detached_commit_stays_private_until_its_pin_is_promoted() {
        let (_dir, db) = graph_with_people().await;
        let entry = person_entry(&db).await;
        let (full_path, location) = location_of(&db, &entry);
        let branch = entry.native_dataset_branch.as_deref();
        let base = entry.published_dataset_version;
        let head = db.storage().open_snapshot_at_entry(&entry).await.unwrap();

        let (detached, identity) = detached_delete(&db, head, "b").await;
        assert!(TableStore::is_detached_version(detached.version()));
        assert_eq!(identity.read_version, base);
        assert!(
            open_at(&db, &location, base + 1).await.unwrap().is_none(),
            "a detached commit must not move the linear history"
        );
        let link = db.storage().transaction_identity(&detached).unwrap();
        assert_eq!(link.uuid, identity.uuid);
        assert_eq!(link.read_version, base);
        assert!(!link.base_is_detached());
        assert_eq!(db.storage().count_rows(&detached, None).await.unwrap(), 7);

        // While the twin is absent the pin resolves to the staged version.
        let pending = open_pinned_dataset(
            &location,
            base + 1,
            Some(detached.version()),
            Some(&identity.uuid),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(pending.version().version, detached.version());

        let promoted = promote_pin(
            &db,
            PERSON,
            &full_path,
            branch,
            base + 1,
            detached.version(),
            &identity.uuid,
        )
        .await
        .unwrap();
        assert_eq!(promoted, Promotion::Promoted(base + 1));
        assert_eq!(
            uuid_at(&db, &location, base + 1).await.as_deref(),
            Some(identity.uuid.as_str())
        );
        assert_eq!(rows_at(&db, &location, base + 1).await, 7);

        // Promotion is idempotent and the pin now resolves to the twin.
        let again = promote_pin(
            &db,
            PERSON,
            &full_path,
            branch,
            base + 1,
            detached.version(),
            &identity.uuid,
        )
        .await
        .unwrap();
        assert_eq!(again, Promotion::AlreadyPromoted);
        let resolved = open_pinned_dataset(
            &location,
            base + 1,
            Some(detached.version()),
            Some(&identity.uuid),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(resolved.version().version, base + 1);

        // Replaying over the landed twin is refused, never duplicated.
        let stale_base = open_at(&db, &location, base).await.unwrap().unwrap();
        let refused = db
            .storage()
            .promote_detached(stale_base, &detached, base + 1, &identity.uuid)
            .await
            .unwrap();
        assert!(matches!(refused, PromotionOutcome::Refused), "{refused:?}");
        assert!(open_at(&db, &location, base + 2).await.unwrap().is_none());
    }

    /// A pin staged on a detached version is the tip of a chain. Promotion
    /// lands the chain oldest first, and a reaped predecessor does not block
    /// a tip whose linear base already exists.
    #[tokio::test]
    async fn chain_promotes_oldest_first_and_tolerates_reaped_predecessors() {
        let (_dir, db) = graph_with_people().await;
        let entry = person_entry(&db).await;
        let (full_path, location) = location_of(&db, &entry);
        let branch = entry.native_dataset_branch.as_deref();
        let base = entry.published_dataset_version;
        let head = db.storage().open_snapshot_at_entry(&entry).await.unwrap();

        let (first, first_id) = detached_delete(&db, head, "a").await;
        let first_again = open_at(&db, &location, first.version())
            .await
            .unwrap()
            .unwrap();
        let (second, second_id) = detached_delete(&db, first_again, "b").await;
        let link = db.storage().transaction_identity(&second).unwrap();
        assert!(link.base_is_detached());
        assert_eq!(link.read_version, first.version());

        let outcome = promote_pin(
            &db,
            PERSON,
            &full_path,
            branch,
            base + 2,
            second.version(),
            &second_id.uuid,
        )
        .await
        .unwrap();
        assert_eq!(outcome, Promotion::Promoted(base + 2));
        assert_eq!(uuid_at(&db, &location, base + 1).await, Some(first_id.uuid));
        assert_eq!(
            uuid_at(&db, &location, base + 2).await,
            Some(second_id.uuid)
        );
        assert_eq!(rows_at(&db, &location, base + 2).await, 6);

        // Promote a chain's first link, reap its detached manifest, then
        // promote the tip through the reaped predecessor.
        let head = open_at(&db, &location, base + 2).await.unwrap().unwrap();
        let (third, third_id) = detached_delete(&db, head, "c").await;
        let third_again = open_at(&db, &location, third.version())
            .await
            .unwrap()
            .unwrap();
        let (fourth, fourth_id) = detached_delete(&db, third_again, "d").await;
        let outcome = promote_pin(
            &db,
            PERSON,
            &full_path,
            branch,
            base + 3,
            third.version(),
            &third_id.uuid,
        )
        .await
        .unwrap();
        assert_eq!(outcome, Promotion::Promoted(base + 3));
        db.storage_adapter()
            .delete(&detached_manifest_path(&location, third.version()))
            .await
            .unwrap();
        assert!(
            open_at(&db, &location, third.version())
                .await
                .unwrap()
                .is_none()
        );
        let outcome = promote_pin(
            &db,
            PERSON,
            &full_path,
            branch,
            base + 4,
            fourth.version(),
            &fourth_id.uuid,
        )
        .await
        .unwrap();
        assert_eq!(outcome, Promotion::Promoted(base + 4));
        assert_eq!(
            uuid_at(&db, &location, base + 4).await,
            Some(fourth_id.uuid)
        );
        assert_eq!(rows_at(&db, &location, base + 4).await, 4);
    }

    /// A foreign linear commit at the target blocks promotion while the pin
    /// keeps reading its own staged effect; a target pruned behind the
    /// linear head is reclaimed, never silently served from the staged
    /// version.
    #[tokio::test]
    async fn foreign_target_blocks_and_resolution_follows_the_linear_head() {
        let (_dir, db) = graph_with_people().await;
        let entry = person_entry(&db).await;
        let (full_path, location) = location_of(&db, &entry);
        let branch = entry.native_dataset_branch.as_deref();
        let base = entry.published_dataset_version;
        let head = db.storage().open_snapshot_at_entry(&entry).await.unwrap();
        let (detached, identity) = detached_delete(&db, head, "a").await;

        load_one(&db, "z").await;
        assert_eq!(person_entry(&db).await.published_dataset_version, base + 1);
        let foreign = uuid_at(&db, &location, base + 1).await.unwrap();
        assert_ne!(foreign, identity.uuid);

        let outcome = promote_pin(
            &db,
            PERSON,
            &full_path,
            branch,
            base + 1,
            detached.version(),
            &identity.uuid,
        )
        .await
        .unwrap();
        assert!(
            matches!(&outcome, Promotion::Blocked(reason) if reason.contains("foreign")),
            "{outcome:?}"
        );
        assert_eq!(uuid_at(&db, &location, base + 1).await, Some(foreign));
        assert!(open_at(&db, &location, base + 2).await.unwrap().is_none());
        let resolved = open_pinned_dataset(
            &location,
            base + 1,
            Some(detached.version()),
            Some(&identity.uuid),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(resolved.version().version, detached.version());

        // Once the head has passed a pruned target, the pin is reclaimed.
        load_one(&db, "y").await;
        let linear = format!(
            "{location}/_versions/{:020}.manifest",
            u64::MAX - (base + 1)
        );
        db.storage_adapter().delete(&linear).await.unwrap();
        let error = open_pinned_dataset(
            &location,
            base + 1,
            Some(detached.version()),
            Some(&identity.uuid),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, OmniError::HistoricalVersionReclaimed { .. }),
            "{error}"
        );
    }
}
