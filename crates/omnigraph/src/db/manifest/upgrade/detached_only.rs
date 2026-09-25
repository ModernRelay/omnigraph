//! The engine half of the step `detached-only-v10-to-v11` (RFC "Detached-only
//! tables", Format and migration). Before the route fences, the v10 graph is
//! opened through an engine handle admitted at its stamp: every pin on every
//! live branch is judged, a blocked one refuses the step, every pending pin
//! is replayed once (the last promotion the engine runs), the copies that
//! replay proves are reaped, and `omnigraph.last_linear_version` is recorded
//! on every current registration row, one publication per live branch. Every
//! write is idempotent, so an interrupted run reruns it whole; the metadata
//! restamp that follows is the fenced, receipted half.

use std::collections::HashSet;

use crate::db::Omnigraph;
use crate::db::manifest::{
    DatasetEntry, ExpectedTableVersions, NativeRefPin, TableVersionExpectation,
};
use crate::db::omnigraph::promotion::{self, Promotion};
use crate::error::{OmniError, Result};

/// The actor every recording publication names.
const ACTOR: &str = "omnigraph:upgrade:detached-only";

/// The stamp this step converts from.
const SOURCE_STAMP: u32 = 10;

/// What the engine half did, or the pins that refused it.
#[derive(Debug, Default)]
pub(super) struct DetachedOnlyWork {
    /// One line per blocked pin; non-empty means nothing was written.
    pub(super) blocked: Vec<String>,
    pub(super) promoted: u64,
    pub(super) reaped: u64,
    pub(super) recorded: u64,
    pub(super) published_branches: u64,
}

/// Judge every pin of every live branch without writing.
pub(super) async fn preflight(root: &str) -> Result<Vec<String>> {
    let _admission = super::super::migrations::admit_conversion_source(root, SOURCE_STAMP);
    let db = Omnigraph::open_read_only(root).await?;
    blocked_pins(&db).await
}

/// Promote, reap and record on every live branch; refuse before any write
/// when a pin is blocked.
pub(super) async fn execute(root: &str) -> Result<DetachedOnlyWork> {
    let _admission = super::super::migrations::admit_conversion_source(root, SOURCE_STAMP);
    let db = Omnigraph::open(root).await?;
    let mut work = DetachedOnlyWork {
        blocked: blocked_pins(&db).await?,
        ..DetachedOnlyWork::default()
    };
    if !work.blocked.is_empty() {
        return Ok(work);
    }
    let branches = live_branches(&db).await?;
    let mut settled = HashSet::new();
    for branch in &branches {
        let snapshot = db.fresh_snapshot_for_branch(branch.as_deref()).await?;
        for entry in snapshot.datasets() {
            let (Some(staged), Some(uuid)) = (
                entry.version_metadata.staged_version(),
                entry.version_metadata.transaction_uuid(),
            ) else {
                continue;
            };
            if !settled.insert(pin_key(entry)) {
                continue;
            }
            let full_path = format!("{}/{}", db.uri(), entry.dataset_path);
            match promotion::replay_pin(
                &db,
                &entry.type_key,
                &full_path,
                entry.native_dataset_branch.as_deref(),
                entry.published_dataset_version,
                staged,
                uuid,
            )
            .await?
            {
                Promotion::Promoted(_) => work.promoted += 1,
                Promotion::AlreadyPromoted => {}
                Promotion::Blocked(reason) => {
                    return Err(OmniError::manifest(format!(
                        "{}: pin {} could not be promoted during the v11 conversion: {reason}",
                        entry.type_key, entry.published_dataset_version
                    )));
                }
            }
            work.reaped += reap_proven_copies(&db, entry).await?;
        }
    }
    for branch in &branches {
        let snapshot = db.fresh_snapshot_for_branch(branch.as_deref()).await?;
        let mut updates = Vec::new();
        let mut expected = ExpectedTableVersions::new();
        for entry in snapshot.datasets() {
            if entry.version_metadata.last_linear_version() == Some(entry.published_dataset_version)
            {
                continue;
            }
            updates.push(crate::db::DatasetUpdate {
                identity: entry.identity,
                type_key: entry.type_key.clone(),
                published_dataset_version: entry.published_dataset_version,
                native_dataset_branch: entry.native_dataset_branch.clone(),
                entity_count: entry.entity_count,
                version_metadata: entry
                    .version_metadata
                    .clone()
                    .with_last_linear_version(Some(entry.published_dataset_version)),
            });
            expected.insert(
                entry.identity,
                TableVersionExpectation {
                    table_key: entry.type_key.clone(),
                    table_version: entry.published_dataset_version,
                    native_ref: NativeRefPin::Exact(entry.native_dataset_branch.clone()),
                },
            );
        }
        if updates.is_empty() {
            continue;
        }
        let _branch_guard = db.write_queue().acquire_branch(branch.as_deref()).await;
        let mut coordinator = db.open_coordinator_for_branch(branch.as_deref()).await?;
        coordinator
            .commit_updates_with_actor_with_expected(&updates, &expected, Some(ACTOR))
            .await?;
        work.recorded += updates.len() as u64;
        work.published_branches += 1;
    }
    Ok(work)
}

async fn live_branches(db: &Omnigraph) -> Result<Vec<Option<String>>> {
    Ok(crate::db::omnigraph::optimize::cleanup_graph_branches(db)
        .await?
        .into_iter()
        .filter(|branch| {
            branch
                .as_deref()
                .is_none_or(|branch| !crate::db::is_internal_system_branch(branch))
        })
        .collect())
}

/// One pin as several branches can share it: table, lineage and target.
fn pin_key(entry: &DatasetEntry) -> (crate::db::manifest::TableIdentity, Option<String>, u64) {
    (
        entry.identity,
        entry.native_dataset_branch.clone(),
        entry.published_dataset_version,
    )
}

async fn blocked_pins(db: &Omnigraph) -> Result<Vec<String>> {
    let mut judged = HashSet::new();
    let mut blocked = Vec::new();
    for branch in live_branches(db).await? {
        let snapshot = db.fresh_snapshot_for_branch(branch.as_deref()).await?;
        for entry in snapshot.datasets() {
            if !judged.insert(pin_key(entry)) {
                continue;
            }
            if let Some(reason) = promotion::blocked_pin_reason(db, entry).await? {
                blocked.push(format!(
                    "{} on branch '{}': {reason}",
                    entry.type_key,
                    branch.as_deref().unwrap_or("main")
                ));
            }
        }
    }
    Ok(blocked)
}

/// Delete the detached copies whose linear twins the promoted pin proves,
/// through the table's own Lance object store, oldest link first: the
/// registered tip goes last, so an interrupted reap is rediscovered from it.
async fn reap_proven_copies(db: &Omnigraph, entry: &DatasetEntry) -> Result<u64> {
    let versions = promotion::promoted_chain_versions(db, entry).await?;
    if versions.is_empty() {
        return Ok(0);
    }
    let full_path = format!("{}/{}", db.uri(), entry.dataset_path);
    let handle = db
        .storage()
        .open_dataset_head(&full_path, entry.native_dataset_branch.as_deref())
        .await?;
    let dataset = handle.dataset();
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let mut reaped = 0;
    for (index, version) in versions.into_iter().rev().enumerate() {
        if index > 0 {
            crate::seams::fail(&super::UPGRADE_DETACHED_ONLY_BETWEEN_REAPS)?;
        }
        let detached = dataset.versions_dir().join(format!("d{version}.manifest"));
        if store.exists(&detached).await.map_err(OmniError::storage)? {
            store.delete(&detached).await.map_err(OmniError::storage)?;
            reaped += 1;
        }
    }
    Ok(reaped)
}
