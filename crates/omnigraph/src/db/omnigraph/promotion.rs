//! Table pins and their detached chains (RFC 0067, detached-only tables).
//! Writers stage on the pin (`open_pinned_for_write`) and never promote it;
//! `table_location`, `open_at` and `walk_chain` resolve a pin's versions,
//! and `blocked_pin_reason` judges one for `repair` without replaying.
//! The one replay left is the v11 upgrade route's (`replay_pin`), run once
//! per pending v10 pin.

use crate::db::manifest::DatasetEntry;
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

/// A genuine detached chain is at most one branch merge's worth of links
/// (`MAX_BRANCH_MERGE_DATA_TRANSACTIONS` = 1024); this bound sits far above
/// that so a longer walk means a corrupt manifest whose read-version links
/// cycle. The walk then fails loud instead of looping forever (invariant 11:
/// bounded I/O even on corrupt input).
const MAX_PROMOTION_CHAIN_LINKS: usize = 1 << 16;

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
        if chain.len() >= MAX_PROMOTION_CHAIN_LINKS {
            return Err(OmniError::manifest_internal(format!(
                "detached chain from version {tip} exceeds {MAX_PROMOTION_CHAIN_LINKS} links; \
                 the manifest's read-version links are corrupt or cyclic"
            )));
        }
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

/// The linear base a walked chain replays onto, or the reason the chain's
/// shape blocks the pin. Shared by promotion and `repair`'s read-only report.
async fn replay_base(
    db: &Omnigraph,
    table_key: &str,
    location: &str,
    target: u64,
    uuid: &str,
    base: Option<u64>,
    chain: &[(u64, String)],
) -> Result<std::result::Result<u64, String>> {
    if chain[0].1 != uuid {
        return Ok(Err(
            "staged version carries no matching transaction".to_string()
        ));
    }
    Ok(match base {
        Some(base) if base + chain.len() as u64 == target => Ok(base),
        Some(base) => Err(format!(
            "{table_key}: a chain of {} detached commits on linear base {base} does not reach target {target}",
            chain.len()
        )),
        None => {
            let base = target.saturating_sub(chain.len() as u64);
            if open_at(db, location, base).await?.is_none() {
                Err(format!(
                    "{table_key}: chain predecessors were reclaimed and linear base {base} is absent"
                ))
            } else {
                Ok(base)
            }
        }
    })
}

/// The pin replay the v11 upgrade step runs once per pending v10 pin, the
/// last promotion the engine performs (RFC "Detached-only tables", Format
/// and migration).
pub(crate) async fn replay_pin(
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
    let base = match replay_base(db, table_key, &location, target, uuid, base, &chain).await? {
        Ok(base) => base,
        Err(reason) => return Ok(Promotion::Blocked(reason)),
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

/// Detached copies whose linear twins an immutable published pin and its chain
/// prove, or whose targets were pruned behind the linear head. A missing link
/// proves nothing about any predecessor; a UUID mismatch never authorizes deletion.
pub(crate) async fn promoted_chain_versions(
    db: &Omnigraph,
    entry: &DatasetEntry,
) -> Result<Vec<u64>> {
    let (Some(staged), Some(uuid)) = (
        entry.version_metadata.staged_version(),
        entry.version_metadata.transaction_uuid(),
    ) else {
        return Ok(Vec::new());
    };
    let full_path = format!("{}/{}", db.root_uri(), entry.dataset_path);
    let location = table_location(&full_path, entry.native_dataset_branch.as_deref());
    let head = db
        .storage()
        .open_dataset_head(&full_path, entry.native_dataset_branch.as_deref())
        .await?
        .version();
    if !twin_settled(db, &location, entry.published_dataset_version, uuid, head).await? {
        return Ok(Vec::new());
    }
    let chain = match walk_chain(db, &location, staged).await {
        Ok((_, chain)) => chain,
        Err(OmniError::HistoricalVersionReclaimed { .. }) => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    if chain[0].1 != uuid {
        return Ok(Vec::new());
    }
    let mut redundant = Vec::new();
    for (index, (version, uuid)) in chain.into_iter().enumerate() {
        let Some(target) = entry.published_dataset_version.checked_sub(index as u64) else {
            break;
        };
        if twin_settled(db, &location, target, &uuid, head).await? {
            redundant.push(version);
        }
    }
    Ok(redundant)
}

/// The foreign commit or chain shape blocking a pin or a link of its chain, judged
/// without replaying anything; `None` while the pin is linear, pending or promoted.
pub(crate) async fn blocked_pin_reason(
    db: &Omnigraph,
    entry: &DatasetEntry,
) -> Result<Option<String>> {
    let (Some(staged), Some(uuid)) = (
        entry.version_metadata.staged_version(),
        entry.version_metadata.transaction_uuid(),
    ) else {
        return Ok(None);
    };
    let full_path = format!("{}/{}", db.root_uri(), entry.dataset_path);
    let location = table_location(&full_path, entry.native_dataset_branch.as_deref());
    match target_state(db, &location, entry.published_dataset_version, uuid).await? {
        Some(Promotion::Blocked(reason)) => return Ok(Some(reason)),
        Some(_) => return Ok(None),
        None => {}
    }
    let (base, chain) = match walk_chain(db, &location, staged).await {
        Ok(walked) => walked,
        Err(OmniError::HistoricalVersionReclaimed { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    let target = entry.published_dataset_version;
    if let Err(reason) =
        replay_base(db, &entry.type_key, &location, target, uuid, base, &chain).await?
    {
        return Ok(Some(reason));
    }
    for (index, (_, uuid)) in chain.iter().enumerate().skip(1) {
        let Some(target) = entry.published_dataset_version.checked_sub(index as u64) else {
            break;
        };
        if let Some(Promotion::Blocked(reason)) = target_state(db, &location, target, uuid).await? {
            return Ok(Some(reason));
        }
    }
    Ok(None)
}

/// Whether a copy's twin is proved at `target`, or was pruned behind `head`.
/// `head` is read before the target, so an absent target at or below it was
/// committed and then removed; resolution never serves the copy from there.
async fn twin_settled(
    db: &Omnigraph,
    location: &str,
    target: u64,
    uuid: &str,
    head: u64,
) -> Result<bool> {
    Ok(match target_state(db, location, target, uuid).await? {
        Some(Promotion::AlreadyPromoted) => true,
        Some(_) => false,
        None => head >= target,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::manifest::DatasetEntry;
    use crate::loader::LoadMode;
    use crate::table_store::StagedTransactionIdentity;

    const SCHEMA: &str = "node Person { name: String @key }\n";
    const PERSON: &str = "node:Person";

    /// A graph whose Person table holds `a`..`h` after one linear load.
    async fn graph_with_people() -> (tempfile::TempDir, crate::Session) {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Session::from_defaults(
            std::sync::Arc::new(
                Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
                    .await
                    .unwrap(),
            ),
            crate::settings::SessionSettings::default(),
        );
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
            .stage_delete(
                &base,
                datafusion::prelude::ident("name").eq(datafusion::prelude::lit(name)),
            )
            .await
            .unwrap()
            .expect("the row exists");
        let witness = crate::table_store::StagingWitness::new(
            &lance::dataset::refs::BranchIdentifier::main(),
            None,
        )
        .unwrap();
        db.storage()
            .commit_staged_detached(base, staged, &witness)
            .await
            .unwrap()
    }

    /// Replay an entry's pin the way the v11 upgrade route does.
    async fn replay_entry(db: &Omnigraph, full_path: &str, entry: &DatasetEntry) -> Promotion {
        replay_pin(
            db,
            &entry.type_key,
            full_path,
            entry.native_dataset_branch.as_deref(),
            entry.published_dataset_version,
            entry
                .version_metadata
                .staged_version()
                .expect("a staged pin"),
            entry
                .version_metadata
                .transaction_uuid()
                .expect("a pin uuid"),
        )
        .await
        .unwrap()
    }

    async fn load_one(db: &crate::Session, name: &str) {
        let row = format!(r#"{{"type":"Person","data":{{"name":"{name}"}}}}"#);
        db.load_as("main", None, &row, LoadMode::Merge, None)
            .await
            .unwrap();
    }

    /// The v11 route's replay: a pin replayed at its target proves its chain,
    /// a twin pruned behind the linear head leaves its copies reclaimable, and
    /// a foreign transaction at the target never authorizes deletion.
    #[tokio::test]
    async fn pruned_twin_is_reclaimable_and_a_foreign_target_is_not() {
        let (_dir, db) = graph_with_people().await;
        let entry = person_entry(&db).await;
        let (full_path, location) = location_of(&db, &entry);
        let base = entry.published_dataset_version;
        let head = db.storage().open_snapshot_at_entry(&entry).await.unwrap();
        let (detached, identity) = detached_delete(&db, head, "a").await;

        load_one(&db, "z").await;
        let pin = person_entry(&db).await;
        assert_eq!(pin.published_dataset_version, base + 1);
        let staged = pin
            .version_metadata
            .staged_version()
            .expect("a load publishes a staged pin");
        let chain: Vec<u64> = walk_chain(&db, &location, staged)
            .await
            .unwrap()
            .1
            .into_iter()
            .map(|(version, _)| version)
            .collect();
        assert_eq!(chain[0], staged);
        assert_eq!(
            promoted_chain_versions(&db, &pin).await.unwrap(),
            Vec::<u64>::new(),
            "a pin no replay promoted proves nothing"
        );
        assert_eq!(
            replay_entry(&db, &full_path, &pin).await,
            Promotion::Promoted(base + 1)
        );
        let mut foreign = pin.clone();
        foreign.version_metadata = foreign
            .version_metadata
            .clone()
            .with_staged(detached.version(), identity.uuid.clone());
        assert_eq!(
            promoted_chain_versions(&db, &foreign).await.unwrap(),
            Vec::<u64>::new(),
            "a foreign transaction at the target proves nothing"
        );
        assert_eq!(promoted_chain_versions(&db, &pin).await.unwrap(), chain);
        let mut pending = pin.clone();
        pending.published_dataset_version = base + 2;
        assert_eq!(
            promoted_chain_versions(&db, &pending).await.unwrap(),
            Vec::<u64>::new(),
            "an absent target above the linear head is a pending pin, not a pruned twin"
        );

        load_one(&db, "y").await;
        let next = person_entry(&db).await;
        assert_eq!(
            replay_entry(&db, &full_path, &next).await,
            Promotion::Promoted(base + 2)
        );
        let linear = format!(
            "{location}/_versions/{:020}.manifest",
            u64::MAX - (base + 1)
        );
        db.storage_adapter().delete(&linear).await.unwrap();
        assert!(open_at(&db, &location, base + 1).await.unwrap().is_none());
        assert_eq!(
            promoted_chain_versions(&db, &pin).await.unwrap(),
            chain,
            "a twin pruned behind the linear head leaves its copy reclaimable"
        );
    }
}
