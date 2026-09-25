//! The tracing collector of the detached-only tables RFC (§Garbage
//! collection). `plan_collection` computes what a run deletes and deletes
//! nothing; `sweep_table` performs one location's plan. `cleanup` runs both
//! under its gates on the detached-only path and copies the counts onto its
//! rows; `Omnigraph::cleanup_plan` runs the plan alone for tests and the DST
//! oracle.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use futures::StreamExt;
use lance::index::DatasetIndexExt;

use crate::db::commit_graph::{CommitGraph, GraphCommit, MergeBaseResolver};
use crate::db::manifest::retention::{ManifestTagInventory, incarnation_digest, merge_input_owner};
use crate::db::manifest::{CollectorBranch, DatasetEntry, ManifestCoordinator, Snapshot};
use crate::db::omnigraph::Omnigraph;
use crate::error::{OmniError, Result};
use crate::seams::{decide_seam, fail};
use crate::storage_layer::SnapshotHandle;
use crate::table_store::{DeletedIdsRecord, StagingWitness, deleted_ids_record};

use super::optimize::CleanupPolicyOptions;
use super::promotion::{open_at, table_location};

decide_seam! {
    /// Inside the collector's read of one live branch's `__manifest`, after
    /// its version history and before its registration rows; both come from
    /// one open, so a publication landing here is invisible to both.
    pub static CLEANUP_COLLECTOR_MID_BRANCH_READ = ("cleanup.collector_mid_branch_read", Unreachable, [Fail]);
}

decide_seam! {
    /// After the collector took every live branch's `__manifest` snapshot and
    /// before it lists the first table; a publication landing here is judged
    /// by the snapshot the run already holds.
    pub static CLEANUP_COLLECTOR_POST_SNAPSHOT = ("cleanup.collector_post_snapshot", Unreachable, [Fail]);
}

/// Days an object no listed manifest references must be older than before
/// the run deletes it: Lance's `UNVERIFIED_THRESHOLD_DAYS`, the window in
/// which a writer's files still await the manifest that would name them.
pub const UNVERIFIED_THRESHOLD_DAYS: i64 = 7;

decide_seam! {
    /// Before the first manifest of a table's sweep is deleted (recorded, on
    /// the report-only path).
    pub static CLEANUP_SWEEP_PRE_MANIFEST_DELETE = ("cleanup.sweep_pre_manifest_delete", Unreachable, [Fail]);
}

decide_seam! {
    /// Between two manifests of one table's sweep.
    pub static CLEANUP_SWEEP_BETWEEN_MANIFESTS = ("cleanup.sweep_between_manifests", Unreachable, [Fail]);
}

decide_seam! {
    /// Before the files a table's sweep frees are deleted (recorded, on the
    /// report-only path).
    pub static CLEANUP_SWEEP_PRE_FILE_DELETE = ("cleanup.sweep_pre_file_delete", Unreachable, [Fail]);
}

/// Requests the run made, for the two ceilings in `maintenance.rs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectorCost {
    /// `__manifest` reads: one per live branch at HEAD plus one per retained
    /// version below HEAD.
    pub manifest_snapshots: u64,
    /// HEAD reads that prove the captured branch snapshots coexisted.
    pub manifest_rechecks: u64,
    /// Listings: per table location its `_versions/` once, then its objects
    /// once for the orphan rule.
    pub listings: u64,
    /// Table manifests opened: each chain link once for its predecessor, then
    /// each root, frozen linear version, unpublished manifest and sweep
    /// candidate once for its paths.
    pub table_opens: u64,
    /// Index sections read: one per marked or examined manifest.
    pub index_reads: u64,
    /// Transaction files read for a spilled deleted-ids record: one per
    /// marked or examined manifest that names a transaction.
    pub transaction_reads: u64,
}

/// The `__manifest` versions of one live branch the run's `--keep` and
/// `--older-than` retain, and the ones the prune would drop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedManifestVersions {
    pub branch: Option<String>,
    pub retained: Vec<u64>,
    pub would_prune: Vec<u64>,
}

/// The staging rule's verdict on a manifest no `__manifest` version named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagingVerdict {
    /// Its recorded authority is the branch's current authority: in flight.
    Live,
    /// Its publication can never land; the reason names which case.
    Dead(String),
    /// The snapshot cannot decide it; retained.
    Undecidable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnpublishedManifest {
    pub version: u64,
    pub bytes: u64,
    pub verdict: StagingVerdict,
}

/// One table location's trace: what a deleting run would do there.
#[derive(Debug, Clone, Default)]
pub struct TableCollectionPlan {
    pub table_key: String,
    pub full_path: String,
    pub location: String,
    /// Detached manifests the `_versions/` listing shows.
    pub present_manifests: BTreeSet<u64>,
    /// Their sizes from the same listing.
    pub manifest_bytes: BTreeMap<u64, u64>,
    /// Linear versions the listing shows.
    pub linear_present: BTreeSet<u64>,
    /// The newest `omnigraph.last_linear_version` any registration records:
    /// a permanent root; linear versions above it are foreign.
    pub last_linear_version: Option<u64>,
    /// Detached versions a `__manifest` version ever named, present in the
    /// listing, with the chain links behind them.
    pub published_set: BTreeSet<u64>,
    /// Members of the published set a retained `__manifest` version pins.
    pub roots: BTreeSet<u64>,
    /// Linear versions a retained `__manifest` version pins, and the linear
    /// twins of staged pins whose proven copy the RFC 0067 reaper reclaimed.
    pub linear_roots: BTreeSet<u64>,
    /// Paths under the location every root references, relative to it.
    pub marked_paths: BTreeSet<String>,
    /// Published, unrooted manifests still present, a chain's links before its
    /// tip: the run deletes them.
    pub sweep: Vec<u64>,
    /// Linear versions below the last linear version that a registration
    /// named and no retained `__manifest` version pins: the run deletes them.
    pub linear_sweep: Vec<u64>,
    /// Paths only the swept manifests reference: the run deletes them.
    pub sweep_paths: BTreeSet<String>,
    pub unpublished: Vec<UnpublishedManifest>,
    /// Paths only the dead stagings reference: the run deletes them with
    /// their manifests.
    pub dead_paths: BTreeSet<String>,
    /// Paths under the location any listed manifest references, roots and
    /// garbage alike: what the orphan rule tests against.
    pub referenced_paths: BTreeSet<String>,
    /// Objects no listed manifest references, older than
    /// [`UNVERIFIED_THRESHOLD_DAYS`]: the run deletes them.
    pub orphan_paths: BTreeSet<String>,
    pub orphan_bytes: u64,
    /// Physical object-store prefixes borrowed by retained native snapshots.
    pub(crate) borrowed_origins: BTreeSet<String>,
    pub(crate) object_base: String,
    orphan_sizes: BTreeMap<String, u64>,
    /// Linear versions above the last linear version (above the newest pin
    /// any registration names while none is recorded).
    pub foreign_versions: Vec<u64>,
    /// A root the listing does not show, or a trace that did not finish.
    pub errors: Vec<String>,
}

/// The four collector fields of one `cleanup` row.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectorRowSummary {
    pub manifests_removed: u64,
    pub unpublished_manifests: u64,
    pub unpublished_bytes: u64,
    pub foreign_versions: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct CollectorReport {
    pub branches: Vec<RetainedManifestVersions>,
    pub tables: Vec<TableCollectionPlan>,
    pub cost: CollectorCost,
    /// Nonce-owned tags whose target can no longer publish, released only by
    /// destructive cleanup after planning. Inspection never mutates storage.
    pub(crate) expired_merge_input_tags: Vec<String>,
    /// Owners visible in the same graph cut as every physical root.
    pub(crate) live_branch_incarnations: HashSet<String>,
    /// Retired graph trees still supplying an exact root or imported lineage.
    pub(crate) protected_manifest_branches: HashSet<String>,
    /// Native trees outside graph registrations may still have tags or an age floor.
    /// Their file dependencies do not themselves keep an unrooted tree alive.
    pub(crate) auxiliary_file_origins: BTreeSet<String>,
}

impl CollectorReport {
    /// The row fields of one table path, summed over its locations (the main
    /// dataset and its forks); `foreign_versions` from the main location. A
    /// table with a location whose trace did not finish gets zeros: a partial
    /// plan is not a count.
    pub fn row_summary(&self, full_path: &str) -> CollectorRowSummary {
        let mut summary = CollectorRowSummary::default();
        let plans = self
            .tables
            .iter()
            .filter(|plan| plan.full_path == full_path)
            .collect::<Vec<_>>();
        if plans.iter().any(|plan| !plan.errors.is_empty()) {
            return summary;
        }
        for plan in plans {
            summary.manifests_removed += plan.would_remove().len() as u64;
            summary.unpublished_manifests += plan.unpublished.len() as u64;
            summary.unpublished_bytes += plan.unpublished.iter().map(|m| m.bytes).sum::<u64>();
            if plan.location == plan.full_path {
                summary.foreign_versions = plan.foreign_versions.clone();
            }
        }
        summary
    }

    pub fn table(&self, location: &str) -> Option<&TableCollectionPlan> {
        self.tables.iter().find(|plan| plan.location == location)
    }
}

impl TableCollectionPlan {
    /// The published manifests a deleting run removes at this location: the
    /// detached sweep, then the linear one.
    pub fn would_remove(&self) -> Vec<u64> {
        self.sweep
            .iter()
            .chain(self.linear_sweep.iter())
            .copied()
            .collect()
    }

    /// The unpublished manifests the staging rule judged dead.
    pub fn dead_stagings(&self) -> Vec<u64> {
        self.unpublished
            .iter()
            .filter(|manifest| matches!(manifest.verdict, StagingVerdict::Dead(_)))
            .map(|manifest| manifest.version)
            .collect()
    }
}

/// What one location's deleting pass removed so far; partial on an error.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweepStats {
    /// Manifests deleted, published and dead alike.
    pub manifests_removed: u64,
    pub bytes_removed: u64,
    /// Dead stagings among the manifests deleted.
    pub unpublished_removed: u64,
}

/// Perform one location's plan into `stats`: the freed files, then the
/// published manifests oldest first, then the dead stagings' files and
/// manifests, then the orphans. A plan that did not finish is refused; an
/// error leaves `stats` at what was deleted before it.
pub(crate) async fn sweep_table(
    db: &Omnigraph,
    plan: &TableCollectionPlan,
    stats: &mut SweepStats,
) -> Result<()> {
    if !plan.errors.is_empty() {
        return Err(OmniError::manifest_conflict(format!(
            "{}: the collector's trace did not finish, nothing is deleted: {}",
            plan.location,
            plan.errors.join("; ")
        )));
    }
    let Some(root) = plan.roots.iter().chain(plan.linear_roots.iter()).next() else {
        return Ok(());
    };
    let Some(handle) = open_at(db, &plan.location, *root).await? else {
        return Err(OmniError::manifest_conflict(format!(
            "{}: root version {root} is not openable, nothing is deleted",
            plan.location
        )));
    };
    let dataset = handle.dataset();
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let naming = dataset.manifest_location().naming_scheme;
    let base = location_base(dataset);
    fail(&CLEANUP_SWEEP_PRE_FILE_DELETE)?;
    stats.bytes_removed += delete_paths(&store, &base, &plan.sweep_paths).await?;
    fail(&CLEANUP_SWEEP_PRE_MANIFEST_DELETE)?;
    for (index, version) in plan.would_remove().iter().enumerate() {
        if index > 0 {
            fail(&CLEANUP_SWEEP_BETWEEN_MANIFESTS)?;
        }
        let manifest = naming.manifest_path(&base, *version);
        let known = plan.manifest_bytes.get(version).copied();
        stats.bytes_removed += delete_object(&store, &manifest, known).await?;
        stats.manifests_removed += 1;
    }
    let dead = plan.dead_stagings();
    if !dead.is_empty() {
        stats.bytes_removed += delete_paths(&store, &base, &plan.dead_paths).await?;
    }
    for version in dead {
        let manifest = naming.manifest_path(&base, version);
        let known = plan.manifest_bytes.get(&version).copied();
        stats.bytes_removed += delete_object(&store, &manifest, known).await?;
        stats.manifests_removed += 1;
        stats.unpublished_removed += 1;
    }
    stats.bytes_removed += delete_paths(&store, &base, &plan.orphan_paths).await?;
    Ok(())
}

/// The location's base path in its object store: the parent of `_versions/`.
fn location_base(dataset: &lance::Dataset) -> object_store::path::Path {
    let versions_dir = dataset.versions_dir();
    let mut parts: Vec<_> = versions_dir.parts().collect();
    parts.pop();
    parts.into_iter().collect()
}

/// Delete the location-relative `paths` (never an inherited `base:` file),
/// an `_indices/<uuid>` entry as a directory; returns the bytes freed,
/// with a file an earlier pass already removed counting nothing.
async fn delete_paths(
    store: &lance::io::ObjectStore,
    base: &object_store::path::Path,
    paths: &BTreeSet<String>,
) -> Result<u64> {
    let mut bytes = 0;
    for path in paths {
        if path.starts_with("base:") {
            continue;
        }
        let full: object_store::path::Path = base
            .parts()
            .chain(object_store::path::Path::from(path.as_str()).parts())
            .collect();
        if path.starts_with("_indices/") {
            store
                .remove_dir_all(full)
                .await
                .map_err(OmniError::storage)?;
            continue;
        }
        bytes += delete_object(store, &full, None).await?;
    }
    Ok(bytes)
}

/// Delete one object and return its size (`known` when the listing gave it);
/// an absent object was removed by an earlier pass and counts nothing.
async fn delete_object(
    store: &lance::io::ObjectStore,
    path: &object_store::path::Path,
    known: Option<u64>,
) -> Result<u64> {
    let size = match known {
        Some(size) => size,
        None => match store.size(path).await {
            Ok(size) => size,
            Err(lance::Error::NotFound { .. }) => return Ok(0),
            Err(error) => return Err(OmniError::storage(error)),
        },
    };
    match store.delete(path).await {
        Ok(()) => Ok(size),
        Err(lance::Error::NotFound { .. }) => Ok(0),
        Err(error) => Err(OmniError::storage(error)),
    }
}

/// Exact retained objects captured before cleanup, including inherited files
/// and every member of legacy index directories. This snapshot holds no GC authority.
#[derive(Clone, Default)]
pub struct CollectorPathSnapshot {
    objects: Vec<RetainedObject>,
    missing_at_capture: Vec<(String, String)>,
}

#[derive(Clone)]
struct RetainedObject {
    location: String,
    path: object_store::path::Path,
    store: std::sync::Arc<lance::io::ObjectStore>,
}

impl CollectorPathSnapshot {
    /// Probe the exact captured object inventory without refreshing its roots
    /// or re-listing legacy index directories after deletion.
    pub async fn missing_paths(&self) -> Result<Vec<(String, String)>> {
        let mut missing: BTreeSet<_> = self.missing_at_capture.iter().cloned().collect();
        for object in &self.objects {
            if !object
                .store
                .exists(&object.path)
                .await
                .map_err(OmniError::storage)?
            {
                missing.insert((object.location.clone(), object.path.to_string()));
            }
        }
        Ok(missing.into_iter().collect())
    }

    fn insert(
        &mut self,
        location: &str,
        store: std::sync::Arc<lance::io::ObjectStore>,
        path: object_store::path::Path,
    ) {
        self.objects.push(RetainedObject {
            location: location.to_string(),
            path,
            store,
        });
    }
}

fn probe_join(base: &object_store::path::Path, relative: &str) -> object_store::path::Path {
    base.parts()
        .chain(object_store::path::Path::from(relative).parts())
        .collect()
}

/// Resolve one root's own base-id namespace using Lance's directory rules.
fn probe_directory(
    dataset: &lance::Dataset,
    base_id: Option<u32>,
    directory: &str,
) -> Result<object_store::path::Path> {
    let Some(base_id) = base_id else {
        let base = location_base(dataset);
        return Ok(if directory.is_empty() {
            base
        } else {
            base.clone().join(directory)
        });
    };
    let base = dataset.manifest().base_paths.get(&base_id).ok_or_else(|| {
        OmniError::manifest(format!("retained root has unknown base path {base_id}"))
    })?;
    if directory.is_empty() && !base.is_dataset_root {
        return Err(OmniError::manifest(format!(
            "deletion base {base_id} is not a dataset root"
        )));
    }
    let path = base
        .extract_path(dataset.session().store_registry())
        .map_err(OmniError::storage)?;
    Ok(if base.is_dataset_root && !directory.is_empty() {
        path.join(directory)
    } else {
        path
    })
}

async fn capture_index_paths(
    snapshot: &mut CollectorPathSnapshot,
    dataset: &lance::Dataset,
    location: &str,
    index: &lance_table::format::IndexMetadata,
) -> Result<()> {
    let store = dataset
        .object_store(index.base_id)
        .await
        .map_err(OmniError::storage)?;
    let directory =
        probe_directory(dataset, index.base_id, "_indices")?.join(index.uuid.to_string());
    if let Some(files) = &index.files {
        if files.is_empty() {
            snapshot
                .missing_at_capture
                .push((location.to_string(), directory.to_string()));
        }
        for file in files {
            snapshot.insert(location, store.clone(), probe_join(&directory, &file.path));
        }
    } else {
        let mut entries = store.read_dir_all(&directory, None);
        let mut found = false;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(OmniError::storage)?;
            found = true;
            snapshot.insert(location, store.clone(), entry.location);
        }
        if !found {
            snapshot
                .missing_at_capture
                .push((location.to_string(), directory.to_string()));
        }
    }
    Ok(())
}

async fn capture_dataset_paths(
    snapshot: &mut CollectorPathSnapshot,
    dataset: &lance::Dataset,
    location: &str,
) -> Result<()> {
    let local = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let base = location_base(dataset);
    snapshot.insert(
        location,
        local.clone(),
        dataset.manifest_location().path.clone(),
    );
    for fragment in dataset.manifest().fragments.iter() {
        for file in fragment.referenced_lance_files() {
            let store = dataset
                .object_store(file.base_id)
                .await
                .map_err(OmniError::storage)?;
            let path = probe_directory(dataset, file.base_id, "data")?.join(file.path.as_str());
            snapshot.insert(location, store, path);
        }
        if let Some(deletion) = &fragment.deletion_file {
            let store = dataset
                .object_store(deletion.base_id)
                .await
                .map_err(OmniError::storage)?;
            let base = probe_directory(dataset, deletion.base_id, "")?;
            let path = lance_table::io::deletion::deletion_file_path(&base, fragment.id, deletion);
            snapshot.insert(location, store, path);
        }
    }
    if let Some(transaction) = &dataset.manifest().transaction_file {
        snapshot.insert(
            location,
            local.clone(),
            base.clone()
                .join("_transactions")
                .join(transaction.as_str()),
        );
        if let Some(DeletedIdsRecord::Spilled(relative)) = dataset
            .read_transaction()
            .await
            .map_err(OmniError::storage)?
            .as_ref()
            .and_then(deleted_ids_record)
        {
            snapshot.insert(location, local, probe_join(&base, &relative));
        }
    }
    for index in dataset
        .load_indices()
        .await
        .map_err(OmniError::storage)?
        .iter()
    {
        capture_index_paths(snapshot, dataset, location, index).await?;
    }
    Ok(())
}

/// Capture every retained root separately: base ids are scoped to a manifest.
pub(crate) async fn capture_marked_paths(
    db: &Omnigraph,
    report: &CollectorReport,
) -> Result<CollectorPathSnapshot> {
    let mut snapshot = CollectorPathSnapshot::default();
    for plan in report.tables.iter().filter(|plan| plan.errors.is_empty()) {
        for root in plan.roots.iter().chain(plan.linear_roots.iter()) {
            let Some(handle) = open_at(db, &plan.location, *root).await? else {
                snapshot
                    .missing_at_capture
                    .push((plan.location.clone(), format!("root {root}")));
                continue;
            };
            capture_dataset_paths(&mut snapshot, handle.dataset(), &plan.location).await?;
        }
    }
    Ok(snapshot)
}

/// Probe current retained objects; preserve a path snapshot across cleanup to
/// detect missing siblings in legacy indices without metadata file lists.
pub(crate) async fn missing_marked_paths(
    db: &Omnigraph,
    report: &CollectorReport,
) -> Result<Vec<(String, String)>> {
    capture_marked_paths(db, report)
        .await?
        .missing_paths()
        .await
}

struct BranchView {
    branch: Option<String>,
    identifier: String,
    manifest_version: u64,
    head: Option<String>,
    commit_graph: Option<CommitGraph>,
    /// The branch's first-parent lineage strictly below the snapshot's head,
    /// walked as far as a staging's judgement needed it.
    lineage: HashSet<String>,
    lineage_cursor: Option<String>,
    lineage_started: bool,
}

impl BranchView {
    /// Whether `commit` sits on the branch's first-parent lineage strictly
    /// below the snapshot's head. The walk starts at that head, so a commit
    /// published after the snapshot is never found.
    async fn lineage_holds(&mut self, db: &Omnigraph, commit: &str) -> Result<bool> {
        if self.lineage.contains(commit) {
            return Ok(true);
        }
        if !self.lineage_started {
            self.lineage_started = true;
            let Some(head) = self.head.clone() else {
                return Ok(false);
            };
            if self.commit_graph.is_none() {
                self.commit_graph = Some(match &self.branch {
                    Some(branch) => CommitGraph::open_at_branch(db.root_uri(), branch).await?,
                    None => CommitGraph::open(db.root_uri()).await?,
                });
            }
            self.lineage_cursor = self
                .commit_graph
                .as_ref()
                .and_then(|graph| graph.get_commit(&head))
                .and_then(|commit| commit.parent_commit_id);
        }
        let graph = self.commit_graph.as_ref();
        while let Some(id) = self.lineage_cursor.take() {
            self.lineage_cursor = graph
                .and_then(|graph| graph.get_commit(&id))
                .and_then(|commit| commit.parent_commit_id)
                .filter(|parent| !self.lineage.contains(parent));
            self.lineage.insert(id.clone());
            if id == commit {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// The form a staging witness records a branch incarnation in.
fn incarnation_key(identifier: &lance::dataset::refs::BranchIdentifier) -> Result<String> {
    serde_json::to_string(identifier).map_err(|error| {
        OmniError::manifest_internal(format!("branch identifier is not serializable: {error}"))
    })
}

/// The `__manifest` versions `keep` and `cutoff` retain (the per-table rule of
/// `cleanup_old_versions` per branch), HEAD always among them and, with
/// `keep_base` (a graph branch, deleting), its oldest: the fork-point merge base.
fn retained_versions(
    versions: &[lance::dataset::Version],
    keep: Option<u32>,
    cutoff: Option<chrono::DateTime<chrono::Utc>>,
    keep_base: bool,
) -> (Vec<u64>, Vec<u64>) {
    let mut sorted: Vec<&lance::dataset::Version> = versions.iter().collect();
    sorted.sort_by_key(|version| version.version);
    let Some(head) = sorted.last().map(|version| version.version) else {
        return (Vec::new(), Vec::new());
    };
    let base = sorted.first().map(|version| version.version);
    let floor = keep.map(|keep| {
        let retain = (keep as usize).max(1);
        sorted[sorted.len().saturating_sub(retain)].version
    });
    let mut retained = Vec::new();
    let mut would_prune = Vec::new();
    for version in sorted {
        let kept = version.version == head
            || (keep_base && Some(version.version) == base)
            || floor.is_some_and(|floor| version.version >= floor)
            || cutoff.is_some_and(|cutoff| version.timestamp >= cutoff);
        if kept {
            retained.push(version.version);
        } else {
            would_prune.push(version.version);
        }
    }
    (retained, would_prune)
}

/// Per table location, the staged pins as `(staged version, linear twin)`
/// and the linear pins, over every retained `__manifest` version.
type PinRoots = HashMap<String, (BTreeSet<(u64, u64)>, BTreeSet<u64>)>;

/// Per table location, its `(type key, full path, native table branch)`.
type TableLocations = BTreeMap<String, (String, String, Option<String>)>;

/// Record the pins of one retained `__manifest` version: a staged pin as
/// `(staged version, linear twin)`, a linear pin as its version.
fn record_roots(
    db: &Omnigraph,
    snapshot: &Snapshot,
    roots: &mut PinRoots,
    tables: &mut TableLocations,
) {
    for entry in snapshot.datasets() {
        let full_path = format!("{}/{}", db.root_uri(), entry.dataset_path);
        let location = table_location(&full_path, entry.native_dataset_branch.as_deref());
        tables.entry(location.clone()).or_insert_with(|| {
            (
                entry.type_key.clone(),
                full_path.clone(),
                entry.native_dataset_branch.clone(),
            )
        });
        let slot = roots.entry(location).or_default();
        match entry.version_metadata.staged_version() {
            Some(staged) => {
                slot.0.insert((staged, entry.published_dataset_version));
            }
            None => {
                slot.1.insert(entry.published_dataset_version);
            }
        }
    }
}

/// The same lazy import frontier as merge, evaluated entirely from captured
/// live lineages. Retired histories supply records no current branch carries.
async fn retained_merge_bases(
    db: &Omnigraph,
    branches: &[(Option<String>, CommitGraph, Option<String>)],
    live_count: usize,
) -> Result<(Vec<GraphCommit>, HashSet<String>)> {
    let mut bases = BTreeMap::new();
    let mut imported_owners = HashSet::new();
    let mut retired = None;
    for left in 0..branches.len() {
        for right in left + 1..branches.len() {
            let (Some(source_head), Some(target_head)) =
                (branches[left].2.as_deref(), branches[right].2.as_deref())
            else {
                continue;
            };
            if source_head == target_head {
                continue;
            }
            let source = branches[left].1.snapshot();
            let target = branches[right].1.snapshot();
            let mut resolver = MergeBaseResolver::new(&source, &target, source_head, target_head);
            for (name, graph, _) in branches[..live_count].iter().rev() {
                if !resolver.search().needs_import() {
                    break;
                }
                if name != &branches[left].0 && name != &branches[right].0 {
                    resolver.import(graph.load_commits().await?)?;
                }
            }
            if resolver.search().needs_import() {
                if retired.is_none() {
                    retired =
                        Some(ManifestCoordinator::retired_commit_graphs(db.root_uri()).await?);
                }
                for (native, graph) in retired.as_ref().expect("retired histories were loaded") {
                    if !resolver.search().needs_import() {
                        break;
                    }
                    resolver.import_retired(graph.load_commits().await?, native)?;
                }
            }
            let base = resolver.search().base.ok_or_else(|| OmniError::manifest_conflict(
                "collector cannot establish the current branches' merge base; nothing is deleted",
            ))?;
            imported_owners.extend(resolver.retained_import_owners());
            if base.graph_commit_id != source_head && base.graph_commit_id != target_head {
                bases.insert(base.graph_commit_id.clone(), base);
            }
        }
    }
    Ok((bases.into_values().collect(), imported_owners))
}

/// Exact native table tags share one capture across all traced locations.
/// Main tags retain their versions too; revalidation precedes every delete.
struct TableTagInventory {
    dataset: lance::Dataset,
    tags: HashMap<String, lance::dataset::refs::TagContents>,
}

impl TableTagInventory {
    async fn capture(dataset: &lance::Dataset) -> Result<Self> {
        Ok(Self {
            dataset: dataset.clone(),
            tags: dataset.tags().list().await.map_err(OmniError::storage)?,
        })
    }

    async fn validate(&self) -> Result<()> {
        let current = self
            .dataset
            .tags()
            .list()
            .await
            .map_err(OmniError::storage)?;
        let unchanged = current.len() == self.tags.len()
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
        if !unchanged {
            return Err(OmniError::manifest_conflict(
                "collector native table tag inventory changed during capture; retry cleanup",
            ));
        }
        Ok(())
    }
}

/// Plan one run over `branches` (every live graph branch), deleting nothing.
pub(crate) async fn plan_collection(
    db: &Omnigraph,
    options: &CleanupPolicyOptions,
    branches: &[Option<String>],
) -> Result<CollectorReport> {
    let now = crate::dst_clock::now_utc();
    let cutoff = options.older_than.map(|older_than| now - older_than);
    let mut report = CollectorReport::default();
    let mut views = Vec::new();
    let mut captured_lineages = Vec::new();
    let mut rows: HashMap<String, Vec<DatasetEntry>> = HashMap::new();
    let mut roots = PinRoots::new();
    let mut tables = TableLocations::new();
    let mut table_tags = HashMap::new();
    let main = ManifestCoordinator::collector_branch_under_control_gates(
        db.root_uri(),
        None,
        &db.control_session(),
    )
    .await?;
    let tags = ManifestTagInventory::capture(main.dataset()).await?;
    let registry = main.clone();
    let mut main = Some(main);
    for branch in branches {
        if branch
            .as_deref()
            .is_some_and(crate::db::is_internal_system_branch)
        {
            continue;
        }
        let opened = match branch {
            None => main.take().ok_or_else(|| {
                OmniError::manifest_internal("collector captured main more than once")
            })?,
            Some(_) => {
                ManifestCoordinator::collector_branch_under_control_gates(
                    db.root_uri(),
                    branch.as_deref(),
                    &db.control_session(),
                )
                .await?
            }
        };
        report.cost.manifest_snapshots += 1;
        report
            .protected_manifest_branches
            .extend(opened.dataset().manifest().branch.iter().cloned());
        if let Some(native) = opened.dataset().manifest().branch.as_deref()
            && let Some(incarnation) = crate::branch_names::split_native_branch_name(native).1
        {
            report
                .live_branch_incarnations
                .insert(incarnation.to_string());
        }
        let head_version = opened.head_version();
        let versions = opened.versions().await?;
        fail(&CLEANUP_COLLECTOR_MID_BRANCH_READ)?;
        let (retained, would_prune) =
            retained_versions(&versions, options.keep_versions, cutoff, branch.is_some());
        for entry in opened.rows().await? {
            let full_path = format!("{}/{}", db.root_uri(), entry.dataset_path);
            let location = table_location(&full_path, entry.native_dataset_branch.as_deref());
            tables.entry(location.clone()).or_insert_with(|| {
                (
                    entry.type_key.clone(),
                    full_path.clone(),
                    entry.native_dataset_branch.clone(),
                )
            });
            rows.entry(location).or_default().push(entry);
        }
        let snapshot = opened.snapshot().await?;
        let graph = opened.commit_graph().await?;
        let effective_head = snapshot
            .graph_head(branch.as_deref())
            .map(str::to_string)
            .or(graph.head_commit_id().await?);
        captured_lineages.push((branch.clone(), graph.capture(), effective_head));
        for version in &retained {
            if *version == head_version {
                record_roots(db, &snapshot, &mut roots, &mut tables);
                continue;
            }
            let pinned =
                ManifestCoordinator::snapshot_at(db.root_uri(), branch.as_deref(), *version)
                    .await?;
            report.cost.manifest_snapshots += 1;
            record_roots(db, &pinned, &mut roots, &mut tables);
        }
        views.push(BranchView {
            branch: branch.clone(),
            identifier: incarnation_key(opened.identifier())?,
            manifest_version: head_version,
            head: snapshot.graph_head(branch.as_deref()).map(str::to_string),
            commit_graph: Some(graph),
            lineage: HashSet::new(),
            lineage_cursor: None,
            lineage_started: false,
        });
        report.branches.push(RetainedManifestVersions {
            branch: branch.clone(),
            retained,
            would_prune,
        });
    }
    let live_count = captured_lineages.len();
    for (name, tag) in &tags.tags {
        let dead = if let Some(owner) = merge_input_owner(name)? {
            if let Some(view) = views
                .iter_mut()
                .find(|view| incarnation_digest(&view.identifier) == owner.incarnation_digest)
            {
                if view.head == owner.graph_head {
                    false
                } else if let Some(head) = owner.graph_head.as_deref() {
                    view.lineage_holds(db, head).await?
                } else {
                    true
                }
            } else {
                true
            }
        } else {
            false
        };
        if dead {
            report.expired_merge_input_tags.push(name.clone());
            continue;
        }
        let pinned = tags.snapshot(tag, db.root_uri()).await?;
        report
            .protected_manifest_branches
            .extend(tag.branch.iter().cloned());
        report.cost.manifest_snapshots += 1;
        record_roots(db, &pinned.snapshot, &mut roots, &mut tables);
        let graph = pinned.commit_graph(db.root_uri()).await?;
        let branch = tag
            .branch
            .as_deref()
            .map(crate::branch_names::logical_branch_name)
            .map(str::to_string);
        let head = pinned
            .snapshot
            .graph_head(branch.as_deref())
            .map(str::to_string)
            .or(graph.head_commit_id().await?);
        if !captured_lineages
            .iter()
            .any(|(_, _, previous)| previous == &head)
        {
            captured_lineages.push((branch, graph, head));
        }
    }
    let (bases, imported_owners) = retained_merge_bases(db, &captured_lineages, live_count).await?;
    report.protected_manifest_branches.extend(imported_owners);
    for base in bases {
        let pinned = ManifestCoordinator::pinned_graph_commit(db.root_uri(), &base).await?;
        report
            .protected_manifest_branches
            .extend(pinned.dataset.manifest().branch.iter().cloned());
        report.cost.manifest_snapshots += 1;
        record_roots(db, &pinned.snapshot, &mut roots, &mut tables);
    }
    fail(&CLEANUP_COLLECTOR_POST_SNAPSHOT)?;

    for (location, (table_key, full_path, table_branch)) in tables {
        let mut plan = TableCollectionPlan {
            table_key,
            full_path,
            location: location.clone(),
            ..Default::default()
        };
        let table_rows = rows.remove(&location).unwrap_or_default();
        let (detached_roots, linear_roots) = roots.remove(&location).unwrap_or_default();
        if let Err(error) = trace_table(
            db,
            &mut plan,
            table_branch.as_deref(),
            &table_rows,
            &detached_roots,
            &linear_roots,
            &mut views,
            &registry,
            &mut table_tags,
            &mut report.cost,
        )
        .await
        {
            plan.errors.push(error.to_string());
        }
        report.tables.push(plan);
    }
    let (auxiliary, auxiliary_cost) = auxiliary_native_origins(db, &report, cutoff).await?;
    report.auxiliary_file_origins = auxiliary;
    report.cost.listings += auxiliary_cost.listings;
    report.cost.table_opens += auxiliary_cost.table_opens;
    report.cost.index_reads += auxiliary_cost.index_reads;
    protect_borrowed_files(&mut report);
    let current_branches = super::optimize::cleanup_graph_branches(db).await?;
    if current_branches.as_slice() != branches {
        return Err(OmniError::manifest_conflict(
            "collector branch inventory changed during capture; retry cleanup",
        ));
    }
    for view in &views {
        let current = ManifestCoordinator::collector_branch_under_control_gates(
            db.root_uri(),
            view.branch.as_deref(),
            &db.control_session(),
        )
        .await?;
        report.cost.manifest_rechecks += 1;
        if current.head_version() != view.manifest_version
            || incarnation_key(current.identifier())? != view.identifier
        {
            return Err(OmniError::manifest_conflict(format!(
                "collector branch '{}' changed during capture; retry cleanup",
                view.branch.as_deref().unwrap_or("main")
            )));
        }
    }
    tags.validate().await?;
    for inventory in table_tags.values() {
        inventory.validate().await?;
        report.cost.listings += 1;
    }
    Ok(report)
}

fn parse_detached_name(name: &str) -> Option<u64> {
    name.strip_prefix('d')?
        .strip_suffix(".manifest")?
        .parse::<u64>()
        .ok()
}

#[allow(clippy::too_many_arguments)]
async fn trace_table(
    db: &Omnigraph,
    plan: &mut TableCollectionPlan,
    table_branch: Option<&str>,
    rows: &[DatasetEntry],
    detached_roots: &BTreeSet<(u64, u64)>,
    linear_roots: &BTreeSet<u64>,
    views: &mut [BranchView],
    registry: &CollectorBranch,
    table_tags: &mut HashMap<String, TableTagInventory>,
    cost: &mut CollectorCost,
) -> Result<()> {
    let location = plan.location.clone();
    let handle = db
        .storage()
        .open_dataset_head(&plan.full_path, table_branch)
        .await?;
    if let std::collections::hash_map::Entry::Vacant(entry) =
        table_tags.entry(plan.full_path.clone())
    {
        entry.insert(TableTagInventory::capture(handle.dataset()).await?);
        cost.listings += 1;
    }
    plan.object_base = location_base(handle.dataset()).to_string();
    let (present, linear_present) = list_manifests(handle.dataset(), cost).await?;
    let post_inventory_live = registry
        .live_identifiers()
        .await?
        .iter()
        .map(incarnation_key)
        .collect::<Result<BTreeSet<_>>>()?;
    plan.present_manifests = present.keys().copied().collect();
    plan.manifest_bytes = present.clone();
    plan.linear_present = linear_present.clone();
    plan.last_linear_version = rows
        .iter()
        .filter_map(|entry| entry.version_metadata.last_linear_version())
        .max();
    let (published, chain_order) = discover_published(db, &location, rows, &present, cost).await?;
    plan.published_set = published.clone();
    resolve_roots(
        plan,
        &present,
        &linear_present,
        detached_roots,
        linear_roots,
    );
    for tag in table_tags[&plan.full_path].tags.values() {
        if tag.branch != handle.dataset().manifest().branch {
            continue;
        }
        if present.contains_key(&tag.version) {
            plan.roots.insert(tag.version);
        } else if linear_present.contains(&tag.version) {
            plan.linear_roots.insert(tag.version);
        } else {
            plan.errors.push(format!(
                "native table tag version {} is absent from the listing",
                tag.version
            ));
        }
    }
    if let Some(last) = plan.last_linear_version {
        if linear_present.contains(&last) {
            plan.linear_roots.insert(last);
        } else {
            plan.errors.push(format!(
                "the last linear version {last} is absent from the listing"
            ));
        }
    }
    for root in plan.roots.iter().chain(plan.linear_roots.iter()) {
        let Some(handle) = open_at(db, &location, *root).await? else {
            plan.errors
                .push(format!("root version {root} listed but not openable"));
            continue;
        };
        cost.table_opens += 1;
        let paths = referenced_paths(&handle, cost).await?;
        plan.referenced_paths.extend(paths.iter().cloned());
        plan.marked_paths.extend(paths);
        plan.borrowed_origins
            .extend(borrowed_origins(handle.dataset(), cost).await?);
    }
    let outside = judge_outside(
        db,
        plan,
        &present,
        &published,
        rows,
        views,
        &post_inventory_live,
        cost,
    )
    .await?;
    plan_sweep(db, plan, &chain_order, &outside, cost).await?;
    plan.foreign_versions = foreign_versions(rows, &linear_present, plan.last_linear_version);
    plan_orphans(handle.dataset(), plan, cost).await?;
    Ok(())
}

/// Inventory Blob sidecars of swept data files and unreferenced objects
/// older than [`UNVERIFIED_THRESHOLD_DAYS`], after tracing every manifest.
async fn plan_orphans(
    dataset: &lance::Dataset,
    plan: &mut TableCollectionPlan,
    cost: &mut CollectorCost,
) -> Result<()> {
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let base = location_base(dataset);
    let cutoff = crate::dst_clock::now_utc() - chrono::Duration::days(UNVERIFIED_THRESHOLD_DAYS);
    let mut files = store.read_dir_all(&base, None);
    cost.listings += 1;
    let mut index_dirs: BTreeMap<String, (bool, u64)> = BTreeMap::new();
    while let Some(file) = files.next().await {
        let file = file.map_err(OmniError::storage)?;
        let Some(relative) = relative_to(&base, &file.location) else {
            continue;
        };
        let Some(key) = orphan_key(&relative) else {
            continue;
        };
        if relative.ends_with(".blob") {
            if plan.sweep_paths.contains(&key) {
                plan.sweep_paths.insert(relative.clone());
            } else if plan.dead_paths.contains(&key) {
                plan.dead_paths.insert(relative.clone());
            }
        }
        if plan.referenced_paths.contains(&key) {
            continue;
        }
        let old = file.last_modified < cutoff;
        if key.starts_with("_indices/") {
            let slot = index_dirs.entry(key).or_insert((true, 0));
            slot.0 &= old;
            slot.1 += file.size;
        } else if old {
            plan.orphan_sizes.insert(relative.clone(), file.size);
            plan.orphan_paths.insert(relative);
            plan.orphan_bytes += file.size;
        }
    }
    for (dir, (all_old, bytes)) in index_dirs {
        if all_old {
            plan.orphan_sizes.insert(dir.clone(), bytes);
            plan.orphan_paths.insert(dir);
            plan.orphan_bytes += bytes;
        }
    }
    Ok(())
}

/// The location-relative form of an object its listing returned.
fn relative_to(
    base: &object_store::path::Path,
    location: &object_store::path::Path,
) -> Option<String> {
    let base = base.as_ref();
    let location = location.as_ref();
    if base.is_empty() {
        return Some(location.to_string());
    }
    location
        .strip_prefix(base)
        .and_then(|rest| rest.strip_prefix('/'))
        .map(str::to_string)
}

/// Resolve an object's reachability key: its parent for Blob sidecars,
/// UUID directory for indexes, otherwise itself. Unknown layouts are retained.
fn orphan_key(relative: &str) -> Option<String> {
    if relative.starts_with("_versions/.tmp") {
        return Some(relative.to_string());
    }
    let parts: Vec<&str> = relative.split('/').collect();
    match parts.as_slice() {
        ["data", file] if file.ends_with(".lance") => Some(relative.to_string()),
        ["data", stem, blob] if blob.ends_with(".blob") => Some(format!("data/{stem}.lance")),
        ["_deletions", _] | ["_transactions", _] => Some(relative.to_string()),
        ["_omnigraph", "deleted_ids", _] => Some(relative.to_string()),
        ["_indices", uuid, _, ..] => Some(format!("_indices/{uuid}")),
        _ => None,
    }
}

/// The manifests a table location's `_versions/` listing shows: detached
/// versions with their sizes, and linear versions.
async fn list_manifests(
    dataset: &lance::Dataset,
    cost: &mut CollectorCost,
) -> Result<(BTreeMap<u64, u64>, BTreeSet<u64>)> {
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let naming = dataset.manifest_location().naming_scheme;
    let mut files = store.read_dir_all(&dataset.versions_dir(), None);
    let mut present = BTreeMap::new();
    let mut linear_present = BTreeSet::new();
    while let Some(file) = files.next().await {
        let file = file.map_err(OmniError::storage)?;
        let Some(name) = file.location.filename() else {
            continue;
        };
        if !name.ends_with(".manifest") {
            continue;
        }
        if let Some(version) = parse_detached_name(name) {
            present.insert(version, file.size);
        } else if let Some(version) = naming.parse_version(name) {
            linear_present.insert(version);
        }
    }
    cost.listings += 1;
    Ok((present, linear_present))
}

/// The published set (every staged version a registration row ever named that
/// the listing shows, plus the chain links behind it; a walk ends at a covered
/// or absent version) and the same set in sweep order, links before tips.
async fn discover_published(
    db: &Omnigraph,
    location: &str,
    rows: &[DatasetEntry],
    present: &BTreeMap<u64, u64>,
    cost: &mut CollectorCost,
) -> Result<(BTreeSet<u64>, Vec<u64>)> {
    let mut published = BTreeSet::new();
    let mut order = Vec::new();
    for member in rows
        .iter()
        .filter_map(|entry| entry.version_metadata.staged_version())
    {
        let mut walk = Vec::new();
        let mut version = member;
        while present.contains_key(&version) && published.insert(version) {
            let Some(handle) = open_at(db, location, version).await? else {
                published.remove(&version);
                break;
            };
            walk.push(version);
            cost.table_opens += 1;
            let link = db.storage().transaction_identity(&handle)?;
            if !link.base_is_detached() {
                break;
            }
            version = link.read_version;
        }
        order.extend(walk.into_iter().rev());
    }
    Ok((published, order))
}

/// Roots against the listing: a staged pin by its copy, or by its linear
/// twin once the RFC 0067 reaper reclaimed the copy; a linear pin by its
/// version. A pin the listing shows nowhere is an error of the table.
fn resolve_roots(
    plan: &mut TableCollectionPlan,
    present: &BTreeMap<u64, u64>,
    linear_present: &BTreeSet<u64>,
    detached_roots: &BTreeSet<(u64, u64)>,
    linear_roots: &BTreeSet<u64>,
) {
    for (root, twin) in detached_roots {
        if present.contains_key(root) {
            plan.roots.insert(*root);
        } else if linear_present.contains(twin) {
            plan.linear_roots.insert(*twin);
        } else {
            plan.errors.push(format!(
                "detached version {root}, pinned by a retained `__manifest` version, is absent \
                 from the listing and so is its linear twin {twin}"
            ));
        }
    }
    for root in linear_roots {
        if linear_present.contains(root) {
            plan.linear_roots.insert(*root);
        } else {
            plan.errors.push(format!(
                "linear version {root}, pinned by a retained `__manifest` version, is absent from the listing"
            ));
        }
    }
}

/// The paths of the manifests outside the published set that are kept: the
/// linear versions that are neither roots nor swept, and each unpublished
/// staging by version.
struct Outside {
    linear: BTreeSet<String>,
    unpublished: BTreeMap<u64, BTreeSet<String>>,
}

impl Outside {
    /// The paths the stagings other than `except` reference.
    fn staging_paths(&self, except: &[u64]) -> BTreeSet<String> {
        self.unpublished
            .iter()
            .filter(|(version, _)| !except.contains(version))
            .flat_map(|(_, paths)| paths.iter().cloned())
            .collect()
    }
}

/// Whether a linear version the retained `__manifest` versions do not pin is
/// swept: below the recorded last linear version and named by a registration
/// (a v10 pin's linear twin, or a pin from before detached commits).
fn linear_swept(plan: &TableCollectionPlan, rows: &[DatasetEntry], version: u64) -> bool {
    plan.last_linear_version.is_some_and(|last| version < last)
        && rows
            .iter()
            .any(|entry| entry.published_dataset_version == version)
}

/// The kept manifests outside the published set, the frozen linear versions
/// and the unpublished staging, with each staging's verdict; the linear
/// versions the run sweeps are recorded on the plan instead.
#[allow(clippy::too_many_arguments)]
async fn judge_outside(
    db: &Omnigraph,
    plan: &mut TableCollectionPlan,
    present: &BTreeMap<u64, u64>,
    published: &BTreeSet<u64>,
    rows: &[DatasetEntry],
    views: &mut [BranchView],
    post_inventory_live: &BTreeSet<String>,
    cost: &mut CollectorCost,
) -> Result<Outside> {
    let location = plan.location.clone();
    let mut outside = Outside {
        linear: BTreeSet::new(),
        unpublished: BTreeMap::new(),
    };
    for version in &plan.linear_present {
        if plan.linear_roots.contains(version) {
            continue;
        }
        if linear_swept(plan, rows, *version) {
            plan.linear_sweep.push(*version);
            continue;
        }
        let Some(handle) = open_at(db, &location, *version).await? else {
            continue;
        };
        cost.table_opens += 1;
        let paths = referenced_paths(&handle, cost).await?;
        plan.referenced_paths.extend(paths.iter().cloned());
        outside.linear.extend(paths);
        plan.borrowed_origins
            .extend(borrowed_origins(handle.dataset(), cost).await?);
    }
    for (version, bytes) in present {
        if published.contains(version) || plan.roots.contains(version) {
            continue;
        }
        let Some(handle) = open_at(db, &location, *version).await? else {
            continue;
        };
        cost.table_opens += 1;
        let paths = referenced_paths(&handle, cost).await?;
        plan.referenced_paths.extend(paths.iter().cloned());
        outside.unpublished.insert(*version, paths);
        let transaction = handle
            .dataset()
            .read_transaction()
            .await
            .map_err(OmniError::storage)?;
        let verdict = match transaction
            .as_ref()
            .and_then(StagingWitness::from_transaction)
        {
            None => StagingVerdict::Undecidable(
                "no staging witness recorded; a writer from before the witness staged it"
                    .to_string(),
            ),
            Some(witness) => judge_staging(db, &witness, views, post_inventory_live).await?,
        };
        if !matches!(verdict, StagingVerdict::Dead(_)) {
            plan.borrowed_origins
                .extend(borrowed_origins(handle.dataset(), cost).await?);
        }
        plan.unpublished.push(UnpublishedManifest {
            version: *version,
            bytes: *bytes,
            verdict,
        });
    }
    Ok(outside)
}

/// A native clone reads files at these origins without retaining their old
/// manifest versions. Keep each borrowed origin's files until no retained
/// snapshot references it; unrelated locations remain independently collectable.
async fn borrowed_origins(
    dataset: &lance::Dataset,
    cost: &mut CollectorCost,
) -> Result<BTreeSet<String>> {
    let manifest = dataset.manifest();
    let mut ids = BTreeSet::new();
    for fragment in manifest.fragments.iter() {
        ids.extend(
            fragment
                .referenced_lance_files()
                .filter_map(|file| file.base_id),
        );
        ids.extend(
            fragment
                .deletion_file
                .iter()
                .filter_map(|file| file.base_id),
        );
    }
    let indices = dataset.load_indices().await.map_err(OmniError::storage)?;
    cost.index_reads += 1;
    ids.extend(indices.iter().filter_map(|index| index.base_id));
    ids.into_iter()
        .map(|id| {
            let base = manifest.base_paths.get(&id).ok_or_else(|| {
                OmniError::manifest_conflict(format!("retained snapshot lacks borrowed base {id}"))
            })?;
            Ok(base
                .extract_path(dataset.session().store_registry())
                .map_err(OmniError::storage)?
                .to_string())
        })
        .collect()
}

/// Protect borrowed origins of native trees absent from graph registrations.
/// File protection expires after tree removal and never roots the tree itself.
async fn auxiliary_native_origins(
    db: &Omnigraph,
    report: &CollectorReport,
    cutoff: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(BTreeSet<String>, CollectorCost)> {
    let mut origins = BTreeSet::new();
    let mut cost = CollectorCost::default();
    if report.tables.iter().any(|table| !table.errors.is_empty()) {
        return Ok((origins, cost));
    }
    let full_paths: BTreeSet<_> = report.tables.iter().map(|table| &table.full_path).collect();
    for full_path in full_paths {
        let handle = db.storage().open_dataset_head(full_path, None).await?;
        let dataset = handle.dataset();
        let retained =
            super::optimize::auxiliary_native_tree_roots(dataset, full_path, report, cutoff)
                .await?;
        if retained.is_empty() {
            continue;
        }
        for native in &retained {
            origins.insert(
                dataset
                    .branch_location()
                    .find_branch(Some(native))
                    .map_err(OmniError::storage)?
                    .path
                    .to_string(),
            );
        }
        let base = location_base(dataset);
        let store = dataset
            .object_store(None)
            .await
            .map_err(OmniError::storage)?;
        let tree = base.clone().join("tree");
        let mut files = store.read_dir_all(&tree, None);
        cost.listings += 1;
        while let Some(file) = files.next().await {
            let file = file.map_err(OmniError::storage)?;
            let Some(relative) = relative_to(&tree, &file.location) else {
                continue;
            };
            let Some((native, filename)) = relative.rsplit_once("/_versions/") else {
                continue;
            };
            if !retained.iter().any(|root| {
                native == root
                    || native
                        .strip_prefix(root.as_str())
                        .is_some_and(|suffix| suffix.starts_with('/'))
            }) {
                continue;
            }
            let version = parse_detached_name(filename).or_else(|| {
                dataset
                    .manifest_location()
                    .naming_scheme
                    .parse_version(filename)
            });
            let Some(version) = version else {
                continue;
            };
            let location = format!("{full_path}/tree/{native}");
            let Some(native) = open_at(db, &location, version).await? else {
                return Err(OmniError::manifest_conflict(
                    "native table history changed during borrowed-file capture; retry cleanup",
                ));
            };
            cost.table_opens += 1;
            origins.extend(borrowed_origins(native.dataset(), &mut cost).await?);
        }
    }
    Ok((origins, cost))
}

/// Object paths are normalized by Lance's registry. Equal paths in different
/// backing stores conservatively retain extra files; they cannot permit a delete.
pub(crate) fn physical_prefixes_overlap(left: &str, right: &str) -> bool {
    fn within(path: &str, prefix: &str) -> bool {
        prefix.is_empty()
            || path == prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    }
    within(left, right) || within(right, left)
}

/// Per-location tracing cannot see another native snapshot's borrowed files.
/// Apply that shared retention only after every table's retained inputs are known.
fn protect_borrowed_files(report: &mut CollectorReport) {
    if report.tables.iter().any(|plan| !plan.errors.is_empty()) {
        for plan in &mut report.tables {
            plan.sweep_paths.clear();
            plan.dead_paths.clear();
            plan.orphan_paths.clear();
            plan.orphan_bytes = 0;
        }
        return;
    }
    let origins: BTreeSet<_> = report
        .tables
        .iter()
        .flat_map(|plan| plan.borrowed_origins.iter().cloned())
        .chain(report.auxiliary_file_origins.iter().cloned())
        .collect();
    for plan in &mut report.tables {
        let base = object_store::path::Path::from(plan.object_base.as_str());
        let unborrowed = |relative: &String| {
            let path: object_store::path::Path = base
                .parts()
                .chain(object_store::path::Path::from(relative.as_str()).parts())
                .collect();
            !origins
                .iter()
                .any(|origin| physical_prefixes_overlap(path.as_ref(), origin))
        };
        plan.sweep_paths.retain(unborrowed);
        plan.dead_paths.retain(unborrowed);
        plan.orphan_paths.retain(unborrowed);
        plan.orphan_bytes = plan
            .orphan_paths
            .iter()
            .filter_map(|path| plan.orphan_sizes.get(path))
            .sum();
    }
}

/// The sweep: published, unrooted, still present, a chain's links before its
/// tip, then the swept linear versions; the paths only the swept manifests
/// reference, and the paths only the dead stagings reference.
async fn plan_sweep(
    db: &Omnigraph,
    plan: &mut TableCollectionPlan,
    chain_order: &[u64],
    outside: &Outside,
    cost: &mut CollectorCost,
) -> Result<()> {
    let location = plan.location.clone();
    let linear_candidates = std::mem::take(&mut plan.linear_sweep);
    let candidates: Vec<u64> = chain_order
        .iter()
        .copied()
        .filter(|version| !plan.roots.contains(version))
        .chain(linear_candidates.iter().copied())
        .collect();
    let mut swept_paths = BTreeSet::new();
    for version in candidates.iter() {
        let Some(handle) = open_at(db, &location, *version).await? else {
            continue;
        };
        cost.table_opens += 1;
        let paths = referenced_paths(&handle, cost).await?;
        plan.referenced_paths.extend(paths.iter().cloned());
        swept_paths.extend(paths);
        if linear_candidates.contains(version) {
            plan.linear_sweep.push(*version);
        } else {
            plan.sweep.push(*version);
        }
    }
    let staging_paths = outside.staging_paths(&[]);
    plan.sweep_paths = swept_paths
        .into_iter()
        .filter(|path| {
            !plan.marked_paths.contains(path)
                && !outside.linear.contains(path)
                && !staging_paths.contains(path)
        })
        .collect();
    let dead = plan.dead_stagings();
    let kept_staging_paths = outside.staging_paths(&dead);
    plan.dead_paths = dead
        .iter()
        .filter_map(|version| outside.unpublished.get(version))
        .flat_map(|paths| paths.iter().cloned())
        .filter(|path| {
            !plan.marked_paths.contains(path)
                && !outside.linear.contains(path)
                && !kept_staging_paths.contains(path)
        })
        .collect();
    Ok(())
}

/// Linear versions above the last linear version, or above the newest pin any
/// registration names while none is recorded.
fn foreign_versions(
    rows: &[DatasetEntry],
    linear_present: &BTreeSet<u64>,
    last_linear_version: Option<u64>,
) -> Vec<u64> {
    let floor = last_linear_version.unwrap_or_else(|| {
        rows.iter()
            .map(|entry| entry.published_dataset_version)
            .max()
            .unwrap_or(0)
    });
    linear_present
        .iter()
        .copied()
        .filter(|version| *version > floor)
        .collect()
}

/// Every path under the location a manifest references, relative to it.
async fn referenced_paths(
    handle: &SnapshotHandle,
    cost: &mut CollectorCost,
) -> Result<BTreeSet<String>> {
    let dataset = handle.dataset();
    let manifest = dataset.manifest();
    let mut paths = BTreeSet::new();
    for fragment in manifest.fragments.iter() {
        for file in fragment.referenced_lance_files() {
            paths.insert(match file.base_id {
                Some(base) => format!("base:{base}/data/{}", file.path),
                None => format!("data/{}", file.path),
            });
        }
        if let Some(deletion) = &fragment.deletion_file {
            let path = lance_table::io::deletion::deletion_file_path(
                &object_store::path::Path::default(),
                fragment.id,
                deletion,
            )
            .to_string();
            paths.insert(match deletion.base_id {
                Some(base) => format!("base:{base}/{path}"),
                None => path,
            });
        }
    }
    if let Some(transaction_file) = &manifest.transaction_file {
        paths.insert(format!("_transactions/{transaction_file}"));
        cost.transaction_reads += 1;
        if let Some(DeletedIdsRecord::Spilled(relative)) = dataset
            .read_transaction()
            .await
            .map_err(OmniError::storage)?
            .as_ref()
            .and_then(deleted_ids_record)
        {
            paths.insert(relative);
        }
    }
    let indices = dataset.load_indices().await.map_err(OmniError::storage)?;
    cost.index_reads += 1;
    for index in indices.iter() {
        paths.insert(match index.base_id {
            Some(base) => format!("base:{base}/_indices/{}", index.uuid),
            None => format!("_indices/{}", index.uuid),
        });
    }
    Ok(paths)
}

/// Keep captured head decisions; judge absent owners only against a complete
/// authority inventory captured after the staging list. Native IDs never recur.
async fn judge_staging(
    db: &Omnigraph,
    witness: &StagingWitness,
    views: &mut [BranchView],
    post_inventory_live: &BTreeSet<String>,
) -> Result<StagingVerdict> {
    let Some(view) = views
        .iter_mut()
        .find(|view| view.identifier == witness.branch_incarnation())
    else {
        return Ok(
            if !post_inventory_live.contains(witness.branch_incarnation()) {
                StagingVerdict::Dead(
                    "the recorded branch incarnation is absent after the frozen staging inventory"
                        .to_string(),
                )
            } else {
                StagingVerdict::Undecidable(
                    "the recorded branch incarnation is absent from the run's coherent snapshot"
                        .to_string(),
                )
            },
        );
    };
    if view.head.as_deref() == witness.graph_head() {
        return Ok(StagingVerdict::Live);
    }
    let Some(recorded) = witness.graph_head() else {
        return Ok(StagingVerdict::Dead(format!(
            "the branch's head moved from none to {}",
            view.head.as_deref().unwrap_or("none")
        )));
    };
    Ok(if view.lineage_holds(db, recorded).await? {
        StagingVerdict::Dead(format!(
            "the branch's head moved past {recorded} to {}",
            view.head.as_deref().unwrap_or("none")
        ))
    } else {
        StagingVerdict::Undecidable(format!(
            "recorded head {recorded} is not below the snapshot's head on the branch"
        ))
    })
}

#[cfg(test)]
mod path_probe_tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::transaction::{Operation, Transaction};
    use lance::dataset::{CommitBuilder, WriteParams};
    use lance_index::{IndexType, scalar::ScalarIndexParams};
    use object_store::ObjectStoreExt;
    use std::sync::Arc;

    async fn indexed_dataset(uri: &str, legacy: bool, with_deletion: bool) -> lance::Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        // forbidden-api-allow: test-only raw indexed snapshot fixture for exact physical-path probes
        let mut dataset = lance::Dataset::write(reader, uri, Some(WriteParams::default()))
            .await
            .unwrap();
        if with_deletion {
            dataset.delete("value = 2").await.unwrap();
        }
        let index = dataset
            // forbidden-api-allow: test-only native BTree fixture with multiple persisted sibling files
            .create_index_builder(&["value"], IndexType::BTree, &ScalarIndexParams::default())
            .execute_uncommitted()
            .await
            .unwrap();
        let transaction = Transaction::new(
            dataset.version().version,
            Operation::CreateIndex {
                new_indices: vec![index],
                removed_indices: vec![],
            },
            None,
        );
        // forbidden-api-allow: test-only native index publication for the retained-path probe
        let dataset = CommitBuilder::new(Arc::new(dataset))
            .execute(transaction)
            .await
            .unwrap();
        if !legacy {
            return dataset;
        }
        use lance_io::traits::{WriteExt, Writer};
        let store = dataset.object_store(None).await.unwrap();
        let mut manifest = dataset.manifest().clone();
        let mut indices: Vec<_> = dataset
            .load_indices()
            .await
            .unwrap()
            .iter()
            .cloned()
            .collect();
        for index in &mut indices {
            index.files = None;
        }
        let transaction = dataset.read_transaction().await.unwrap();
        let transaction = transaction.as_ref().map(Into::into);
        let mut writer = store
            .create(&dataset.manifest_location().path)
            .await
            .unwrap();
        let position = lance_table::io::manifest::write_manifest(
            writer.as_mut(),
            &mut manifest,
            Some(indices),
            transaction,
        )
        .await
        .unwrap();
        writer
            .write_magics(
                position,
                lance_table::format::MAJOR_VERSION,
                lance_table::format::MINOR_VERSION,
                lance_table::format::MAGIC,
            )
            .await
            .unwrap();
        Writer::shutdown(writer.as_mut()).await.unwrap();
        // forbidden-api-allow: test-only cold native open after rewriting legacy index metadata
        lance::dataset::builder::DatasetBuilder::from_uri(uri)
            .with_session(Arc::new(lance::session::Session::default()))
            .load()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn retained_path_snapshot_detects_one_missing_index_sibling() {
        let dir = tempfile::tempdir().unwrap();
        for legacy in [false, true] {
            let uri = dir.path().join(format!("index-{legacy}.lance"));
            let dataset = indexed_dataset(uri.to_str().unwrap(), legacy, false).await;
            let index = dataset.load_indices().await.unwrap()[0].clone();
            assert_eq!(index.files.is_none(), legacy);
            let store = dataset.object_store(None).await.unwrap();
            let directory = location_base(&dataset)
                .join("_indices")
                .join(index.uuid.to_string());
            let mut members = Vec::new();
            let mut entries = store.read_dir_all(&directory, None);
            while let Some(entry) = entries.next().await {
                members.push(entry.unwrap().location);
            }
            members.sort();
            assert!(
                members.len() >= 2,
                "real BTree fixture must have sibling files"
            );
            let mut captured = CollectorPathSnapshot::default();
            capture_dataset_paths(&mut captured, &dataset, dataset.uri())
                .await
                .unwrap();
            assert!(captured.missing_paths().await.unwrap().is_empty());
            let removed = members.last().unwrap();
            store.inner.delete(removed).await.unwrap();
            assert!(
                store
                    .read_dir_all(&directory, None)
                    .next()
                    .await
                    .transpose()
                    .unwrap()
                    .is_some(),
                "the old any-file predicate would still pass"
            );
            let missing = captured.missing_paths().await.unwrap();
            assert!(
                missing.iter().any(|(_, path)| path == removed.as_ref()),
                "{missing:?}"
            );
            if legacy {
                let mut after = CollectorPathSnapshot::default();
                capture_dataset_paths(&mut after, &dataset, dataset.uri())
                    .await
                    .unwrap();
                assert!(
                    after.missing_paths().await.unwrap().is_empty(),
                    "legacy member loss requires preserving the pre-cleanup inventory"
                );
            }
        }
    }

    #[tokio::test]
    async fn retained_path_snapshot_resolves_each_roots_inherited_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut sources = Vec::new();
        let mut branches = Vec::new();
        for name in ["first", "second"] {
            let uri = dir.path().join(format!("{name}.lance"));
            let mut source = indexed_dataset(uri.to_str().unwrap(), false, true).await;
            let version = source.version().version;
            branches.push(source.create_branch("child", version, None).await.unwrap());
            sources.push(source);
        }
        let first_id = branches[0].manifest().fragments[0].files[0].base_id;
        let second_id = branches[1].manifest().fragments[0].files[0].base_id;
        assert!(first_id.is_some());
        assert_eq!(
            first_id, second_id,
            "same base id has different meanings in separate roots"
        );
        let mut captured = CollectorPathSnapshot::default();
        for branch in &branches {
            capture_dataset_paths(&mut captured, branch, branch.uri())
                .await
                .unwrap();
        }
        assert!(captured.missing_paths().await.unwrap().is_empty());
        for source in &sources {
            let base = location_base(source);
            let fragment = &source.manifest().fragments[0];
            let data = probe_join(&base.clone().join("data"), &fragment.files[0].path);
            let deletion = lance_table::io::deletion::deletion_file_path(
                &base,
                fragment.id,
                fragment.deletion_file.as_ref().expect("fixture deletion"),
            );
            let index = source.load_indices().await.unwrap()[0].clone();
            let indexed = probe_join(
                &base.clone().join("_indices").join(index.uuid.to_string()),
                &index.files.as_ref().expect("modern index inventory")[0].path,
            );
            let store = source.object_store(None).await.unwrap();
            for path in [data, deletion, indexed] {
                let bytes = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
                store.inner.delete(&path).await.unwrap();
                let missing = captured.missing_paths().await.unwrap();
                assert!(
                    missing.iter().any(|(_, absent)| absent == path.as_ref()),
                    "{missing:?}"
                );
                store.inner.put(&path, bytes.into()).await.unwrap();
            }
        }
        assert!(captured.missing_paths().await.unwrap().is_empty());
    }
}

#[cfg(test)]
mod borrowed_origin_tests {
    use super::*;
    use crate::Session;
    use crate::db::ReadTarget;
    use crate::db::manifest::{DatasetUpdate, ManifestChange, TableVersionMetadata};
    use crate::loader::LoadMode;
    use crate::settings::SessionSettings;
    use arrow_array::{Array, Int32Array, StringArray};
    use std::sync::Arc;

    async fn load_person(db: &Session, name: &str, age: i32) {
        db.load_jsonl(
            &format!(r#"{{"type":"Person","data":{{"name":"{name}","age":{age}}}}}"#),
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    }

    async fn load_company(db: &Session, name: &str) {
        db.load_jsonl(
            &format!(r#"{{"type":"Company","data":{{"name":"{name}"}}}}"#),
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    }

    /// An upgraded native fork can borrow files from a later main image than
    /// its graph fork point. Reclaim main's history while keeping its files.
    #[tokio::test]
    async fn cleanup_keeps_legacy_native_origins_and_reclaims_unrelated_payloads() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let schema = "node Person {\n    name: String @key\n    age: I32 @index\n}\nnode Company {\n    name: String @key\n}";
        let db = Session::from_defaults(
            Arc::new(Omnigraph::init(uri, schema).await.unwrap()),
            SessionSettings::default(),
        );
        load_person(&db, "initial", 0).await;
        load_company(&db, "initial-company").await;
        db.branch_create("legacy").await.unwrap();
        load_person(&db, "borrowed", 41).await;
        db.ensure_indices().await.unwrap();
        let source_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let entry = source_snapshot.dataset("node:Person").unwrap().clone();
        let mut source = source_snapshot
            .open_lance_dataset("node:Person")
            .await
            .unwrap();
        let source_version = source.version().version;
        let source_base = location_base(&source);
        let source_data: Vec<_> = source
            .manifest()
            .fragments
            .iter()
            .flat_map(|fragment| {
                fragment
                    .files
                    .iter()
                    .map(|file| source_base.clone().join("data").join(file.path.as_str()))
            })
            .collect();
        let source_indices: Vec<_> = source
            .load_indices()
            .await
            .unwrap()
            .iter()
            .flat_map(|index| {
                let directory = source_base
                    .clone()
                    .join("_indices")
                    .join(index.uuid.to_string());
                index
                    .files
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(move |file| probe_join(&directory, &file.path))
            })
            .collect();
        assert!(!source_data.is_empty() && !source_indices.is_empty());
        // forbidden-api-allow: test-only linear legacy fork source; its temporary linear manifest is removed before cleanup
        source.restore().await.unwrap();
        let restored_version = source.version().version;
        let restored_path = source.manifest_location().path.clone();
        let mut fork = crate::storage_layer::lance_clone::create_branch(
            &mut source,
            "legacy-table",
            restored_version,
        )
        .await
        .unwrap();
        source
            .object_store(None)
            .await
            .unwrap()
            .delete(&restored_path)
            .await
            .unwrap();
        let mut coordinator = ManifestCoordinator::open_at_branch(uri, "legacy")
            .await
            .unwrap();
        let old = coordinator
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .clone();
        while fork.version().version <= old.published_dataset_version {
            fork.delete("false").await.unwrap();
        }
        let native_version = fork.version().version;
        let metadata = TableVersionMetadata::from_dataset(uri, &entry.dataset_path, &fork)
            .unwrap()
            .with_last_linear_version(Some(native_version));
        let lineage = db
            .new_lineage_intent_for_branch(Some("legacy"), None)
            .await
            .unwrap();
        coordinator
            .commit_changes_with_lineage(
                &[ManifestChange::Update(DatasetUpdate {
                    identity: entry.identity,
                    type_key: entry.type_key.clone(),
                    published_dataset_version: native_version,
                    native_dataset_branch: Some("legacy-table".into()),
                    entity_count: 1,
                    version_metadata: metadata,
                })],
                &Default::default(),
                Some(&lineage),
            )
            .await
            .unwrap();
        drop((coordinator, fork, source_snapshot));
        load_company(&db, "unrelated-old").await;
        let company_snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let company = company_snapshot
            .open_lance_dataset("node:Company")
            .await
            .unwrap();
        let company_base = location_base(&company);
        let company_old: Vec<_> = company
            .manifest()
            .fragments
            .iter()
            .flat_map(|fragment| {
                fragment
                    .files
                    .iter()
                    .map(|file| company_base.clone().join("data").join(file.path.as_str()))
            })
            .collect();
        assert!(!company_old.is_empty());
        load_company(&db, "unrelated-current").await;
        load_person(&db, "main-current", 99).await;
        db.ensure_indices().await.unwrap();
        let options = super::super::optimize::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        };
        let report = db.cleanup_plan(options.clone()).await.unwrap();
        let source_plan = report
            .tables
            .iter()
            .find(|plan| plan.table_key == "node:Person" && plan.location == plan.full_path)
            .unwrap();
        assert!(
            source_plan.would_remove().contains(&source_version),
            "borrowed source manifest is reclaimable"
        );
        let rows = db.cleanup(options).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        let source_store = source.object_store(None).await.unwrap();
        for path in source_data.iter().chain(&source_indices) {
            assert!(
                source_store.exists(path).await.unwrap(),
                "borrowed object lost: {path}"
            );
        }
        let company_store = company.object_store(None).await.unwrap();
        for path in &company_old {
            assert!(
                !company_store.exists(path).await.unwrap(),
                "unrelated garbage survived: {path}"
            );
        }
        drop((db, source, company, company_snapshot));
        let reopened = Omnigraph::open(uri).await.unwrap();
        let snapshot = reopened
            .snapshot_of(ReadTarget::branch("legacy"))
            .await
            .unwrap();
        assert_eq!(
            snapshot
                .dataset("node:Person")
                .unwrap()
                .native_dataset_branch
                .as_deref(),
            Some("legacy-table")
        );
        let inherited = snapshot.open_lance_dataset("node:Person").await.unwrap();
        let mut scan = inherited.scan();
        scan.project(&["name", "age"]).unwrap();
        scan.filter("age = 41").unwrap();
        assert!(
            scan.explain_plan(true)
                .await
                .unwrap()
                .contains("ScalarIndexQuery")
        );
        let rows = scan.try_into_batch().await.unwrap();
        assert_eq!(rows.num_rows(), 1);
        assert_eq!(
            rows.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "borrowed"
        );
        assert_eq!(
            rows.column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            41
        );
    }
    #[tokio::test]
    async fn cleanup_keeps_files_borrowed_by_unregistered_tagged_native_fork() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(uri, "node Person { name: String @key age: I32 }")
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        load_person(&db, "tagged", 41).await;
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let mut source = snapshot.open_lance_dataset("node:Person").await.unwrap();
        let version = source.version().version;
        let fork =
            crate::storage_layer::lance_clone::create_branch(&mut source, "native-only", version)
                .await
                .unwrap();
        let native_version = fork.version().version;
        source
            .tags()
            .create("native-reader", ("native-only", native_version))
            .await
            .unwrap();
        crate::storage_layer::lance_clone::create_branch(&mut source, "unrelated-native", version)
            .await
            .unwrap();
        let table_uri = source.uri().to_string();
        drop((source, fork, snapshot));
        load_person(&db, "current", 99).await;
        let options = super::super::optimize::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        };
        let plan = db.cleanup_plan(options.clone()).await.unwrap();
        assert!(
            plan.tables
                .iter()
                .any(|table| table.would_remove().contains(&version)),
            "old source pin must be reclaimable without the native tag's borrowed files"
        );
        let rows = db.cleanup(options).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        drop(db);
        // forbidden-api-allow: test-only cold native read of a tagged fork absent from graph registrations
        let native = lance::dataset::builder::DatasetBuilder::from_uri(&table_uri)
            .with_session(Arc::new(lance::session::Session::default()))
            .load()
            .await
            .unwrap();
        assert!(
            !native
                .list_branches()
                .await
                .unwrap()
                .contains_key("unrelated-native"),
            "borrowing main's files must not retain unrelated native trees beneath main"
        );
        let native = native
            .checkout_version(("native-only", native_version))
            .await
            .unwrap();
        let mut scan = native.scan();
        scan.project(&["name", "age"]).unwrap();
        let rows = scan.try_into_batch().await.unwrap();
        assert_eq!(rows.num_rows(), 1);
        assert_eq!(
            rows.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "tagged"
        );
        assert_eq!(
            rows.column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            41
        );
    }
}

#[cfg(test)]
mod overlay_retention_tests {
    use super::*;
    use crate::Session;
    use crate::db::ReadTarget;
    use crate::db::manifest::{DatasetUpdate, ManifestChange, TableVersionMetadata};
    use crate::loader::LoadMode;
    use crate::settings::SessionSettings;
    use arrow_array::{Array, Int32Array};
    use lance::dataset::transaction::{Operation, Transaction};
    use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
    use std::sync::Arc;

    async fn load_person(db: &Session, age: i32) {
        db.load_jsonl(
            &format!(r#"{{"type":"Person","data":{{"name":"person","age":{age}}}}}"#),
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    }

    async fn overlay_snapshot(foreign: bool) {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(uri, "node Person { name: String @key age: I32 }")
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        load_person(&db, 41).await;
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let entry = snapshot.dataset("node:Person").unwrap().clone();
        let source = snapshot.open_lance_dataset("node:Person").await.unwrap();
        let table_uri = format!("{uri}/{}", entry.dataset_path);
        let mut fragments = source.manifest().fragments.as_ref().clone();
        assert_eq!(fragments.len(), 1);
        let mut overlay_file = fragments[0].files[0].clone();
        assert!(overlay_file.base_id.is_none());
        let data_dir = probe_directory(&source, None, "data").unwrap();
        let original_path = data_dir.clone().join(overlay_file.path.as_str());
        overlay_file.path = "retained-overlay.lance".to_string();
        let overlay_path = data_dir.join(overlay_file.path.as_str());
        let local_path =
            |path: &object_store::path::Path| std::path::Path::new("/").join(path.as_ref());
        std::fs::copy(local_path(&original_path), local_path(&overlay_path)).unwrap();
        std::fs::File::open(local_path(&overlay_path))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
            .unwrap();
        if foreign {
            overlay_file.base_id = Some(7);
        }
        fragments[0].overlays.push(DataOverlayFile {
            data_file: overlay_file,
            coverage: OverlayCoverage::Shared(Arc::new([0_u32].into_iter().collect())),
            committed_version: source.version().version,
        });
        let mut base = if foreign {
            // forbidden-api-allow: test-only foreign linear overlay source
            lance::Dataset::open(&table_uri).await.unwrap()
        } else {
            source.clone()
        };
        if foreign {
            let transaction = Transaction::new(
                base.version().version,
                Operation::UpdateBases {
                    new_bases: vec![lance_table::format::BasePath::new(
                        7,
                        table_uri.clone(),
                        None,
                        true,
                    )],
                },
                None,
            );
            // forbidden-api-allow: test-only overlay base metadata fixture
            base = lance::dataset::CommitBuilder::new(Arc::new(base))
                .with_skip_auto_cleanup(true)
                .execute(transaction)
                .await
                .unwrap();
        }
        let transaction = Transaction::new(
            base.version().version,
            Operation::Overwrite {
                fragments,
                schema: source.schema().clone(),
                config_upsert_values: None,
                initial_bases: None,
            },
            None,
        );
        let transaction_uuid = transaction.uuid.clone();
        // forbidden-api-allow: test-only foreign linear or published detached overlay fixture
        let overlay = lance::dataset::CommitBuilder::new(Arc::new(base))
            .with_detached(!foreign)
            .with_skip_auto_cleanup(true)
            .execute(transaction)
            .await
            .unwrap();
        let version = overlay.version().version;
        if !foreign {
            let mut coordinator = ManifestCoordinator::open(uri).await.unwrap();
            let lineage = db.new_lineage_intent_for_branch(None, None).await.unwrap();
            coordinator
                .commit_changes_with_lineage(
                    &[ManifestChange::Update(DatasetUpdate {
                        identity: entry.identity,
                        type_key: entry.type_key.clone(),
                        published_dataset_version: entry.published_dataset_version + 1,
                        native_dataset_branch: None,
                        entity_count: 1,
                        version_metadata: TableVersionMetadata::from_dataset(
                            uri,
                            &entry.dataset_path,
                            &overlay,
                        )
                        .unwrap()
                        .with_last_linear_version(entry.version_metadata.last_linear_version())
                        .with_staged(version, transaction_uuid),
                    })],
                    &Default::default(),
                    Some(&lineage),
                )
                .await
                .unwrap();
        }
        let options = CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        };
        let report = db.cleanup_plan(options.clone()).await.unwrap();
        let plan = report
            .tables
            .iter()
            .find(|plan| plan.location == table_uri)
            .unwrap();
        assert!(plan.errors.is_empty(), "{:?}", plan.errors);
        assert!(if foreign {
            plan.foreign_versions.contains(&version)
        } else {
            plan.roots.contains(&version)
        });
        if foreign {
            assert!(
                plan.borrowed_origins
                    .contains(location_base(&source).as_ref()),
                "an overlay-only base id must protect its physical origin"
            );
        }
        assert!(
            !plan
                .orphan_paths
                .iter()
                .any(|path| path.ends_with(overlay_path.filename().unwrap())),
            "a live overlay is not an aged orphan: {plan:?}"
        );
        let mut expected = CollectorPathSnapshot::default();
        capture_dataset_paths(&mut expected, &overlay, &table_uri)
            .await
            .unwrap();
        assert!(
            expected
                .objects
                .iter()
                .any(|object| object.path == overlay_path),
            "the independent pre-cleanup probe must include overlays"
        );
        let rows = db.cleanup(options.clone()).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        let store = overlay.object_store(None).await.unwrap();
        assert!(store.exists(&overlay_path).await.unwrap());
        assert!(expected.missing_paths().await.unwrap().is_empty());
        // forbidden-api-allow: test-only cold read proves cleanup retained the actual overlay file
        let cold = lance::dataset::builder::DatasetBuilder::from_uri(&table_uri)
            .with_session(Arc::new(lance::session::Session::default()))
            .with_version(version)
            .load()
            .await
            .unwrap();
        let mut scan = cold.scan();
        scan.project(&["age"]).unwrap();
        let batch = scan.try_into_batch().await.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            41
        );
        let bytes = std::fs::read(local_path(&overlay_path)).unwrap();
        store.delete(&overlay_path).await.unwrap();
        assert!(
            expected
                .missing_paths()
                .await
                .unwrap()
                .iter()
                .any(|(_, path)| path == overlay_path.as_ref()),
            "deleting only the overlay must trip the saved inventory"
        );
        std::fs::write(local_path(&overlay_path), bytes).unwrap();
        std::fs::File::open(local_path(&overlay_path))
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::UNIX_EPOCH))
            .unwrap();
        if foreign {
            store
                .delete(&overlay.manifest_location().path)
                .await
                .unwrap();
        } else {
            load_person(&db, 99).await;
        }
        let rows = db.cleanup(options).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        assert!(
            !store.exists(&overlay_path).await.unwrap(),
            "overlay must be reclaimed once no kept snapshot references it"
        );
    }

    #[tokio::test]
    async fn cleanup_keeps_retained_overlay_and_probe_detects_its_loss() {
        overlay_snapshot(false).await;
    }

    #[tokio::test]
    async fn cleanup_keeps_foreign_overlay_until_foreign_manifest_is_removed() {
        overlay_snapshot(true).await;
    }
}

#[cfg(test)]
mod native_table_tag_tests {
    use super::*;
    use crate::Session;
    use crate::db::ReadTarget;
    use crate::db::manifest::{DatasetUpdate, ManifestChange, TableVersionMetadata};
    use crate::loader::LoadMode;
    use crate::settings::SessionSettings;
    use arrow_array::{Array, StringArray};
    use std::sync::Arc;

    async fn load_person(db: &Session, name: &str) {
        db.load_jsonl(
            &format!(r#"{{"type":"Person","data":{{"name":"{name}"}}}}"#),
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
    }

    async fn publish_native(db: &Session, dataset: &lance::Dataset, count: u64) {
        let mut coordinator = ManifestCoordinator::open_at_branch(db.root_uri(), "legacy")
            .await
            .unwrap();
        let entry = coordinator
            .snapshot()
            .dataset("node:Person")
            .unwrap()
            .clone();
        let lineage = db
            .new_lineage_intent_for_branch(Some("legacy"), None)
            .await
            .unwrap();
        coordinator
            .commit_changes_with_lineage(
                &[ManifestChange::Update(DatasetUpdate {
                    identity: entry.identity,
                    type_key: entry.type_key.clone(),
                    published_dataset_version: dataset.version().version,
                    native_dataset_branch: dataset.manifest().branch.clone(),
                    entity_count: count,
                    version_metadata: TableVersionMetadata::from_dataset(
                        db.root_uri(),
                        &entry.dataset_path,
                        dataset,
                    )
                    .unwrap()
                    .with_last_linear_version(Some(dataset.version().version)),
                })],
                &Default::default(),
                Some(&lineage),
            )
            .await
            .unwrap();
    }

    async fn tagged_old_version_survives(native: bool) {
        let dir = tempfile::tempdir().unwrap();
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(
                    dir.path().to_str().unwrap(),
                    "node Person { name: String @key }",
                )
                .await
                .unwrap(),
            ),
            SessionSettings::default(),
        );
        load_person(&db, "tagged-old").await;
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let mut source = snapshot.open_lance_dataset("node:Person").await.unwrap();
        let table_uri = format!(
            "{}/{}",
            db.root_uri(),
            snapshot.dataset("node:Person").unwrap().dataset_path
        );
        let mut tagged = if native {
            db.branch_create("legacy").await.unwrap();
            // forbidden-api-allow: test-only legacy linear fork origin; remove the temporary linear manifest before cleanup
            source.restore().await.unwrap();
            let source_version = source.version().version;
            let restored_path = source.manifest_location().path.clone();
            assert!(!lance_table::format::is_detached_version(source_version));
            let mut fork = crate::storage_layer::lance_clone::create_branch(
                &mut source,
                "registered-native",
                source_version,
            )
            .await
            .unwrap();
            source
                .object_store(None)
                .await
                .unwrap()
                .delete(&restored_path)
                .await
                .unwrap();
            assert!(!lance_table::format::is_detached_version(
                fork.version().version
            ));
            let counter = snapshot
                .dataset("node:Person")
                .unwrap()
                .published_dataset_version;
            while fork.version().version <= counter {
                fork.delete("false").await.unwrap();
            }
            publish_native(&db, &fork, 1).await;
            fork
        } else {
            source.clone()
        };
        let version = tagged.version().version;
        let native_branch = tagged.manifest().branch.clone();
        let manifest_path = tagged.manifest_location().path.clone();
        let store = tagged.object_store(None).await.unwrap();
        tagged
            .tags()
            .create(
                "keep-old",
                lance::dataset::refs::Ref::Version(native_branch.clone(), Some(version)),
            )
            .await
            .unwrap();
        let tag = tagged.tags().get("keep-old").await.unwrap();
        assert_eq!(tag.branch, native_branch);
        assert_eq!(tag.version, version);
        if native {
            tagged.delete("name = 'tagged-old'").await.unwrap();
            assert!(tagged.version().version > version);
            assert!(!lance_table::format::is_detached_version(
                tagged.version().version
            ));
            publish_native(&db, &tagged, 0).await;
        } else {
            load_person(&db, "current").await;
        }
        let options = CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        };
        let report = db.cleanup_plan(options.clone()).await.unwrap();
        let location = table_location(&table_uri, native_branch.as_deref());
        let plan = report
            .tables
            .iter()
            .find(|plan| plan.location == location)
            .unwrap();
        assert!(plan.errors.is_empty(), "{:?}", plan.errors);
        assert!(
            !plan.would_remove().contains(&version),
            "an exact native table tag must exclude its version from cleanup: {plan:?}",
        );
        let rows = db.cleanup(options.clone()).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        assert!(store.exists(&manifest_path).await.unwrap());
        // forbidden-api-allow: test-only cold native tag read after the graph collector ran
        let cold = lance::dataset::builder::DatasetBuilder::from_uri(&table_uri)
            .with_session(Arc::new(lance::session::Session::default()))
            .load()
            .await
            .unwrap()
            .checkout_version(lance::dataset::refs::Ref::Version(
                native_branch,
                Some(version),
            ))
            .await
            .unwrap();
        let mut scan = cold.scan();
        scan.project(&["name"]).unwrap();
        let rows = scan.try_into_batch().await.unwrap();
        assert_eq!(rows.num_rows(), 1);
        assert_eq!(
            rows.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "tagged-old"
        );
        tagged.tags().delete("keep-old").await.unwrap();
        let report = db.cleanup_plan(options.clone()).await.unwrap();
        assert!(
            report
                .tables
                .iter()
                .any(|plan| plan.location == location && plan.would_remove().contains(&version)),
            "removing the tag must make the old version reclaimable"
        );
        let rows = db.cleanup(options).await.unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        assert!(
            !store.exists(&manifest_path).await.unwrap(),
            "untagged old manifest survived"
        );
    }

    #[tokio::test]
    async fn cleanup_retains_exact_main_table_tag_until_removed() {
        tagged_old_version_survives(false).await;
    }

    #[tokio::test]
    async fn cleanup_retains_exact_registered_native_table_tag_until_removed() {
        tagged_old_version_survives(true).await;
    }

    #[tokio::test]
    async fn table_tag_capture_rejects_retarget_metadata_removal_and_creation() {
        let dir = tempfile::tempdir().unwrap();
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(
                    dir.path().to_str().unwrap(),
                    "node Person { name: String @key }",
                )
                .await
                .unwrap(),
            ),
            SessionSettings::default(),
        );
        load_person(&db, "first").await;
        let first = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .open_lance_dataset("node:Person")
            .await
            .unwrap();
        load_person(&db, "second").await;
        let second = db
            .snapshot_of(ReadTarget::branch("main"))
            .await
            .unwrap()
            .open_lance_dataset("node:Person")
            .await
            .unwrap();
        first
            .tags()
            .create("keep", first.version().version)
            .await
            .unwrap();
        let inventory = TableTagInventory::capture(&first).await.unwrap();
        inventory.validate().await.unwrap();
        first
            .tags()
            .update("keep", second.version().version)
            .await
            .unwrap();
        assert!(
            inventory
                .validate()
                .await
                .unwrap_err()
                .to_string()
                .contains("table tag inventory changed")
        );
        let inventory = TableTagInventory::capture(&first).await.unwrap();
        first
            .tags()
            .replace_metadata("keep", HashMap::from([("owner".into(), "changed".into())]))
            .await
            .unwrap();
        assert!(inventory.validate().await.is_err());
        let inventory = TableTagInventory::capture(&first).await.unwrap();
        first.tags().delete("keep").await.unwrap();
        assert!(inventory.validate().await.is_err());
        let inventory = TableTagInventory::capture(&first).await.unwrap();
        first
            .tags()
            .create("later", first.version().version)
            .await
            .unwrap();
        assert!(inventory.validate().await.is_err());
    }
}
