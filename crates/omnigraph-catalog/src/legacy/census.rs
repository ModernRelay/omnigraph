//! The census of a source root: every graph commit a `__manifest` ref of the
//! root wrote, as the complete history record a stamp-14 reader serves for it,
//! with the writer that owns it. The plan cuts the census into the objects of
//! `__history/legacy/`, and the conversion batch is the stamp-14 rows of one
//! live head. Nothing here depends on the source stamp: every row is read
//! through a [`LegacyManifestSource`].

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lance::Dataset;
use lance::dataset::refs::Ref;
use sha2::{Digest, Sha256};
use ulid::Ulid;

use super::{HeadScan, LegacyCommit, LegacyManifestSource, SourceRole, VersionSchema, head_among};
use crate::branch_names::logical_branch_name;
use crate::error::{OmniError, Result};
use crate::history::{
    self, HistoryRecord, LegacyChain, LegacyLayout, LegacyObjects, LegacyWriterKind,
    MAX_RECORD_BYTES, RecordSource,
};
use crate::migrations::{INTERNAL_MANIFEST_SCHEMA_VERSION, INTERNAL_SCHEMA_VERSION_KEY};
use crate::state::{
    CommitBuffer, GraphLineageRow, ManifestRows, SchemaContractRow, TableRow, TableState,
    commit_bytes,
};
use crate::{MAIN_BRANCH_HEAD_KEY, TableRegistration};

/// The cells of the `object_type` column one census may read. Every source
/// version rewrites every row, so the sum over the versions of a lineage
/// grows with the square of its commit count.
pub const MAX_CENSUS_CELLS: u64 = 1 << 33;
/// The inline bytes of the table snapshots one census may retain: the record
/// of every graph commit holds one [`TableRow`] per table of its version.
pub const MAX_CENSUS_SNAPSHOT_BYTES: u64 = 1 << 30;
/// Version reads in flight at once.
const READS_IN_FLIGHT: usize = 16;
/// Ids or refs one finding names before it counts the rest.
const NAMED_PER_FINDING: usize = 20;

/// Why a source root cannot be converted, or a resumed plan cannot be finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FindingCode {
    UnsupportedSource,
    LineageIncomplete,
    LineageCorrupt,
    UncommittedChange,
    RecordOverBound,
    CensusOverBound,
    DirectoryOverBound,
    HeadRecordMismatch,
    PlanChanged,
}

impl FindingCode {
    /// The code an upgrade report carries for the finding.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedSource => "unsupported_source",
            Self::LineageIncomplete => "legacy_lineage_incomplete",
            Self::LineageCorrupt => "legacy_lineage_corrupt",
            Self::UncommittedChange => "legacy_uncommitted_change",
            Self::RecordOverBound => "legacy_record_over_bound",
            Self::CensusOverBound => "legacy_census_over_bound",
            Self::DirectoryOverBound => "legacy_directory_over_bound",
            Self::HeadRecordMismatch => "legacy_head_record_mismatch",
            Self::PlanChanged => "legacy_plan_changed",
        }
    }
}

/// One finding about the source root: its code and what it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyFinding {
    pub code: FindingCode,
    pub message: String,
}

/// A finding about the source root, or the failure of a read of it.
#[derive(Debug)]
pub enum CensusError {
    Finding(LegacyFinding),
    Read(OmniError),
}

impl From<OmniError> for CensusError {
    fn from(error: OmniError) -> Self {
        Self::Read(error)
    }
}

impl std::fmt::Display for CensusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Finding(finding) => {
                write!(formatter, "{}: {}", finding.code.as_str(), finding.message)
            }
            Self::Read(error) => error.fmt(formatter),
        }
    }
}

type CensusResult<T> = std::result::Result<T, CensusError>;

fn finding(code: FindingCode, message: impl Into<String>) -> CensusError {
    CensusError::Finding(LegacyFinding {
        code,
        message: message.into(),
    })
}

fn unsupported(error: OmniError) -> CensusError {
    finding(FindingCode::UnsupportedSource, error.to_string())
}

/// One `__manifest` ref pinned for the census: its Lance native name (`None`
/// for main), its head version, and the ref and version it forked at (`None`
/// and 0 for main).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CensusRef {
    pub native: Option<String>,
    pub version: u64,
    pub parent: Option<String>,
    pub parent_version: u64,
}

/// The refs one census reads: the live ones, main among them, which the
/// upgrade converts, and the retired ones, whose own commits it keeps. The
/// attempt is written into the directory, so a resumed census passes the
/// attempt of the intent and plans the same bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CensusInput {
    pub attempt: Ulid,
    pub live: Vec<CensusRef>,
    pub retired: Vec<CensusRef>,
}

/// The head a live ref is converted to: the record of its effective head,
/// which a fresh fork shares with its source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CensusHead {
    pub native: Option<String>,
    pub version: u64,
    pub record: HistoryRecord,
}

/// What one census read and found, for the report of the upgrade.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CensusCounts {
    pub live_refs: u64,
    pub retired_refs: u64,
    pub orphan_writers: u64,
    pub legacy_commits: u64,
    pub bookkeeping_versions: u64,
    pub pre_genesis_versions: u64,
    pub absent_parents: u64,
    pub schema_contents: u64,
    pub census_reads: u64,
    pub census_cells: u64,
    pub legacy_bytes: u64,
}

/// Every commit of the source root as its history record, by writer: main's
/// chain first, then the live refs, the retired refs and the orphaned writers,
/// each group in name order and each chain oldest first. `contracts` holds
/// each distinct schema content a record names, in the order of their archive
/// names, and `heads` one entry per live ref, main first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCensus {
    pub attempt: Ulid,
    pub source_stamp: u32,
    pub chains: Vec<LegacyChain>,
    pub absent_parents: BTreeSet<Ulid>,
    pub contracts: Vec<SchemaContractRow>,
    pub heads: Vec<CensusHead>,
    pub counts: CensusCounts,
}

/// What the intent of an upgrade binds of the legacy objects: the layout they
/// are cut under, their counts and the SHA-256 of the directory, which lists
/// the digest of every other object.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyPlan {
    pub layout: LegacyLayout,
    pub commits: u64,
    pub data_files: u32,
    pub id_shards: u32,
    pub writer_shards: u32,
    pub directory_sha256: String,
}

/// Read the source root at the pinned refs of `input` and derive the history
/// record of every commit. Nothing is written. A source the upgrade cannot
/// convert is a [`CensusError::Finding`].
pub async fn census(
    root_uri: &str,
    input: &CensusInput,
    source: &dyn LegacyManifestSource,
) -> CensusResult<LegacyCensus> {
    census_within(
        root_uri,
        input,
        source,
        MAX_CENSUS_CELLS,
        MAX_CENSUS_SNAPSHOT_BYTES,
    )
    .await
}

/// [`census`] under a caller's bounds in place of [`MAX_CENSUS_CELLS`] and
/// [`MAX_CENSUS_SNAPSHOT_BYTES`].
#[doc(hidden)]
pub async fn census_within(
    root_uri: &str,
    input: &CensusInput,
    source: &dyn LegacyManifestSource,
    max_cells: u64,
    max_snapshot_bytes: u64,
) -> CensusResult<LegacyCensus> {
    let session = crate::lance_access::control_session();
    let main =
        crate::layout::open_manifest_dataset_native_with_session(root_uri, None, &session).await?;
    let mut counts = CensusCounts {
        live_refs: input.live.len() as u64,
        retired_refs: input.retired.len() as u64,
        ..CensusCounts::default()
    };
    let mut source_stamp = 0;
    let mut lineages = Vec::new();
    for (kind, pinned) in ordered_refs(input)? {
        let head = checkout(&main, pinned.native.as_deref(), pinned.version).await?;
        let role = match kind {
            LegacyWriterKind::Retired => SourceRole::RetiredHead,
            _ => SourceRole::LiveHead,
        };
        let stamp = source.admits(&head, role).map_err(unsupported)?;
        if kind == LegacyWriterKind::Main {
            source_stamp = stamp;
        }
        let scan = source.scan_head(&head).await?;
        counts.census_reads += 1;
        lineages.push(Lineage::new(kind, pinned, head, scan)?);
    }
    let graph = CommitGraph::of(&lineages)?;
    let mut snapshots = Snapshots {
        rows: 0,
        registrations: 0,
        max_bytes: max_snapshot_bytes,
    };
    snapshots.admit(&lineages, &graph)?;

    for lineage in &mut lineages {
        lineage.list_versions().await?;
        counts.census_cells = counts.census_cells.saturating_add(lineage.cells());
    }
    if counts.census_cells > max_cells {
        return Err(finding(
            FindingCode::CensusOverBound,
            format!(
                "reading every retained version would scan {} cells of the object_type column, \
                 above the bound of {max_cells}; no version was read",
                counts.census_cells
            ),
        ));
    }

    let mut contracts = Contracts::default();
    let main_contract = lineages[0].scan.contract.clone();
    let mut deferred = Vec::new();
    for (index, lineage) in lineages.iter_mut().enumerate() {
        lineage.fallback = match lineage.scan.contract.as_ref().or(main_contract.as_ref()) {
            Some(contract) => Some(contracts.intern(contract)?),
            None => None,
        };
        let mut reading = Reading {
            index,
            main: &main,
            source,
            graph: &graph,
            contracts: &mut contracts,
            counts: &mut counts,
            deferred: &mut deferred,
            snapshots: &mut snapshots,
        };
        lineage.read_versions(&mut reading).await?;
    }

    let mut chains = Vec::new();
    for lineage in &mut lineages {
        let records = std::mem::take(&mut lineage.records);
        if records.is_empty() {
            continue;
        }
        chains.push(LegacyChain {
            kind: lineage.kind,
            native: lineage.pinned.native.clone(),
            parent: lineage.pinned.parent.clone(),
            parent_version: lineage.pinned.parent_version,
            head_version: lineage.pinned.version,
            records,
        });
    }
    if chains
        .first()
        .is_none_or(|chain| chain.kind != LegacyWriterKind::Main)
    {
        return Err(finding(
            FindingCode::LineageIncomplete,
            "main holds no graph commit",
        ));
    }
    for (logical, orphans) in &graph.orphans {
        let mut orphans: Vec<&Orphan> = orphans.values().collect();
        orphans.sort_by(|a, b| order_of(&a.commit).cmp(&order_of(&b.commit)));
        let mut records = Vec::with_capacity(orphans.len());
        for orphan in orphans {
            let adopter = &lineages[orphan.adopter];
            let seen = adopter.first.as_ref().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "{} was read without the schema of any version",
                    named(adopter.pinned.native.as_deref())
                ))
            })?;
            let version = orphan.commit.graph_manifest_version;
            let contract = contracts.named(
                seen.contract.or(adopter.fallback),
                adopter.pinned.native.as_deref(),
                version,
            )?;
            snapshots.retain(seen.tables.len())?;
            let record = HistoryRecord {
                commit: lineage_row(&orphan.commit, Some(logical), &graph, contract),
                tables: tables_at(&adopter.scan, version, &seen.tables),
            };
            check_record_bounds(&record, crate::HISTORY_RELEASE_BYTES)?;
            records.push(record);
        }
        let head_version = records
            .last()
            .map_or(0, |record| record.commit.graph_manifest_version);
        chains.push(LegacyChain {
            kind: LegacyWriterKind::Orphaned,
            native: Some(logical.clone()),
            parent: None,
            parent_version: 0,
            head_version,
            records,
        });
        counts.orphan_writers += 1;
    }

    let by_id: HashMap<&str, &HistoryRecord> = chains
        .iter()
        .flat_map(|chain| &chain.records)
        .map(|record| (record.commit.graph_commit_id.as_str(), record))
        .collect();
    for check in &deferred {
        let nearest = by_id.get(check.nearest.as_str()).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "inherited graph commit '{}' has no record",
                check.nearest
            ))
        })?;
        if !same_membership(&nearest.tables, &check.tables) {
            return Err(uncommitted(
                lineages[check.lineage].pinned.native.as_deref(),
                check.version,
                MEMBERSHIP,
                Some(&nearest.commit),
            ));
        }
    }
    let mut heads = Vec::new();
    for lineage in &lineages {
        if lineage.kind == LegacyWriterKind::Retired {
            continue;
        }
        heads.push(lineage.head_record(&by_id, &mut contracts)?);
    }

    for record in chains.iter().flat_map(|chain| &chain.records) {
        counts.legacy_commits += 1;
        counts.legacy_bytes = counts
            .legacy_bytes
            .saturating_add(record.record_bytes() as u64);
    }
    counts.absent_parents = graph.absent.len() as u64;
    counts.schema_contents = contracts.rows.len() as u64;
    contracts.rows.sort_by(|a, b| a.1.cmp(&b.1));
    Ok(LegacyCensus {
        attempt: input.attempt,
        source_stamp,
        chains,
        absent_parents: graph.absent,
        contracts: contracts.rows.into_iter().map(|(row, _)| row).collect(),
        heads,
        counts,
    })
}

/// The refs of `input` in census order: main, the live refs by name, the
/// retired refs by name.
fn ordered_refs(input: &CensusInput) -> Result<Vec<(LegacyWriterKind, &CensusRef)>> {
    let invalid = |message: &str| OmniError::manifest_internal(format!("census input: {message}"));
    let mut mains = input.live.iter().filter(|pinned| pinned.native.is_none());
    let main = mains
        .next()
        .filter(|main| main.parent.is_none() && main.parent_version == 0)
        .ok_or_else(|| invalid("the live refs hold no main, or main names a parent"))?;
    if mains.next().is_some() || input.retired.iter().any(|pinned| pinned.native.is_none()) {
        return Err(invalid("main is listed twice or as a retired ref"));
    }
    fn by_name(refs: &[CensusRef], kind: LegacyWriterKind) -> Vec<(LegacyWriterKind, &CensusRef)> {
        let mut refs: Vec<_> = refs
            .iter()
            .filter(|pinned| pinned.native.is_some())
            .map(|pinned| (kind, pinned))
            .collect();
        refs.sort_by(|a, b| a.1.native.cmp(&b.1.native));
        refs
    }
    let mut ordered = vec![(LegacyWriterKind::Main, main)];
    ordered.extend(by_name(&input.live, LegacyWriterKind::Live));
    ordered.extend(by_name(&input.retired, LegacyWriterKind::Retired));
    let mut names = BTreeSet::new();
    if !ordered
        .iter()
        .all(|(_, pinned)| names.insert(&pinned.native))
    {
        return Err(invalid("a native ref is listed twice"));
    }
    Ok(ordered)
}

async fn checkout(main: &Dataset, native: Option<&str>, version: u64) -> Result<Dataset> {
    main.checkout_version(Ref::Version(native.map(str::to_string), Some(version)))
        .await
        .map_err(OmniError::storage)
}

pub(super) fn named(native: Option<&str>) -> String {
    native.map_or_else(|| "main".to_string(), |native| format!("ref '{native}'"))
}

fn canonical_id(id: &str) -> CensusResult<Ulid> {
    Ulid::from_string(id)
        .ok()
        .filter(|parsed| parsed.to_string() == id)
        .ok_or_else(|| {
            finding(
                FindingCode::UnsupportedSource,
                format!("graph commit ID '{id}' is not a canonical ULID"),
            )
        })
}

fn corrupt(message: String) -> CensusError {
    finding(FindingCode::LineageCorrupt, message)
}

/// The order a source publish chose its parent by.
fn order_of(commit: &LegacyCommit) -> (u64, i64, &str) {
    (
        commit.graph_manifest_version,
        commit.created_at,
        commit.graph_commit_id.as_str(),
    )
}

/// The membership, aliases and paths of one version, and its interned contract.
struct SchemaAt {
    tables: Vec<TableRegistration>,
    contract: Option<usize>,
}

/// The distinct schema contents the census met, each with its archive name.
#[derive(Default)]
struct Contracts {
    rows: Vec<(SchemaContractRow, String)>,
}

impl Contracts {
    fn intern(&mut self, contract: &SchemaContractRow) -> Result<usize> {
        if let Some(index) = self.rows.iter().position(|(row, _)| row == contract) {
            return Ok(index);
        }
        let digest = history::schema_content_hash(contract)?;
        self.rows.push((contract.clone(), digest));
        Ok(self.rows.len() - 1)
    }

    /// The contract a commit of `native` at `version` is recorded under.
    fn named(
        &self,
        index: Option<usize>,
        native: Option<&str>,
        version: u64,
    ) -> CensusResult<&(SchemaContractRow, String)> {
        index.map(|index| &self.rows[index]).ok_or_else(|| {
            finding(
                FindingCode::UnsupportedSource,
                format!(
                    "version {version} of {} holds no schema_contract row, and neither does its \
                     head nor the head of main",
                    named(native)
                ),
            )
        })
    }
}

/// An inherited commit whose writer no pinned ref is: the copy first met and
/// the ref that inherits it.
struct Orphan {
    commit: LegacyCommit,
    adopter: usize,
}

/// The commits of every lineage as one graph: the generation of each, the
/// orphans by logical branch name, and the merged parents no ref holds.
struct CommitGraph {
    generations: HashMap<String, u64>,
    orphans: BTreeMap<String, BTreeMap<String, Orphan>>,
    absent: BTreeSet<Ulid>,
}

impl CommitGraph {
    fn of(lineages: &[Lineage<'_>]) -> CensusResult<Self> {
        let mut commits: HashMap<&str, &LegacyCommit> = HashMap::new();
        for lineage in lineages {
            for commit in lineage.own_commits() {
                if commits
                    .insert(commit.graph_commit_id.as_str(), commit)
                    .is_some()
                {
                    return Err(corrupt(format!(
                        "graph commit '{}' is an own commit of two refs",
                        commit.graph_commit_id
                    )));
                }
            }
        }
        let mut orphans: BTreeMap<String, BTreeMap<String, Orphan>> = BTreeMap::new();
        for (adopter, lineage) in lineages.iter().enumerate() {
            for commit in lineage.inherited_commits() {
                if commits.contains_key(commit.graph_commit_id.as_str()) {
                    continue;
                }
                let Some(logical) = &commit.graph_branch else {
                    return Err(corrupt(format!(
                        "{} inherits main's graph commit '{}', which main does not hold",
                        named(lineage.pinned.native.as_deref()),
                        commit.graph_commit_id
                    )));
                };
                canonical_id(&commit.graph_commit_id)?;
                match orphans
                    .entry(logical.clone())
                    .or_default()
                    .entry(commit.graph_commit_id.clone())
                {
                    Entry::Vacant(entry) => {
                        entry.insert(Orphan {
                            commit: commit.clone(),
                            adopter,
                        });
                    }
                    Entry::Occupied(entry) if entry.get().commit != *commit => {
                        return Err(corrupt(format!(
                            "two refs inherit different copies of graph commit '{}'",
                            commit.graph_commit_id
                        )));
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        commits.extend(
            orphans
                .values()
                .flat_map(BTreeMap::values)
                .map(|orphan| (orphan.commit.graph_commit_id.as_str(), &orphan.commit)),
        );

        let mut gaps = BTreeSet::new();
        let mut absent = BTreeSet::new();
        for commit in commits.values() {
            if let Some(parent) = commit.parent_commit_id.as_deref() {
                if !commits.contains_key(parent) {
                    gaps.insert((commit.graph_commit_id.as_str(), parent));
                }
            }
            if let Some(merged) = commit.merged_parent_commit_id.as_deref() {
                if !commits.contains_key(merged) {
                    absent.insert(canonical_id(merged)?);
                }
            }
        }
        if !gaps.is_empty() {
            let listed: Vec<String> = gaps
                .iter()
                .take(NAMED_PER_FINDING)
                .map(|(child, parent)| format!("'{parent}' (first parent of '{child}')"))
                .collect();
            return Err(finding(
                FindingCode::LineageIncomplete,
                format!(
                    "{} first parents are held by no ref: {}",
                    gaps.len(),
                    listed.join(", ")
                ),
            ));
        }
        let generations = generations(&commits)?;
        Ok(Self {
            generations,
            orphans,
            absent,
        })
    }
}

/// The generation of every commit: 0 without a present parent, else the
/// greatest generation among its present parents plus one.
fn generations(commits: &HashMap<&str, &LegacyCommit>) -> CensusResult<HashMap<String, u64>> {
    enum Walk {
        Open,
        Done(u64),
    }
    let cycle = |id: &str| corrupt(format!("graph commit '{id}' is its own ancestor"));
    let parents = |commit: &LegacyCommit| {
        [&commit.parent_commit_id, &commit.merged_parent_commit_id]
            .into_iter()
            .flatten()
            .filter_map(|parent| commits.get_key_value(parent.as_str()))
            .map(|(id, _)| *id)
            .collect::<Vec<&str>>()
    };
    let mut walked: HashMap<&str, Walk> = HashMap::with_capacity(commits.len());
    let mut starts: Vec<&str> = commits.keys().copied().collect();
    starts.sort_unstable();
    for start in starts {
        let mut stack = vec![(start, false)];
        while let Some((id, expanded)) = stack.pop() {
            let commit = commits[id];
            match (walked.get(id), expanded) {
                (Some(Walk::Done(_)), _) => {}
                (Some(Walk::Open), false) => return Err(cycle(id)),
                (None, true) => {
                    return Err(OmniError::manifest_internal(format!(
                        "graph commit '{id}' was closed before it was opened"
                    ))
                    .into());
                }
                (None, false) => {
                    walked.insert(id, Walk::Open);
                    stack.push((id, true));
                    for parent in parents(commit) {
                        match walked.get(parent) {
                            Some(Walk::Done(_)) => {}
                            Some(Walk::Open) => return Err(cycle(parent)),
                            None => stack.push((parent, false)),
                        }
                    }
                }
                (Some(Walk::Open), true) => {
                    let mut generation = 0u64;
                    for parent in parents(commit) {
                        let Some(Walk::Done(of_parent)) = walked.get(parent) else {
                            return Err(cycle(parent));
                        };
                        generation = generation.max(of_parent.checked_add(1).ok_or_else(|| {
                            corrupt(format!("the generation of graph commit '{id}' overflows"))
                        })?);
                    }
                    walked.insert(id, Walk::Done(generation));
                }
            }
        }
    }
    Ok(walked
        .into_iter()
        .filter_map(|(id, walk)| match walk {
            Walk::Done(generation) => Some((id.to_string(), generation)),
            Walk::Open => None,
        })
        .collect())
}

fn lineage_row(
    commit: &LegacyCommit,
    native: Option<&str>,
    graph: &CommitGraph,
    (contract, digest): &(SchemaContractRow, String),
) -> GraphLineageRow {
    GraphLineageRow {
        graph_commit_id: commit.graph_commit_id.clone(),
        schema_contract: Some(contract.head.clone()),
        schema_content_hash: Some(digest.clone()),
        graph_branch: commit.graph_branch.clone(),
        native_branch: native.map(str::to_string),
        graph_manifest_version: commit.graph_manifest_version,
        generation: graph.generations[&commit.graph_commit_id],
        parent_commit_id: commit.parent_commit_id.clone(),
        merged_parent_commit_id: commit.merged_parent_commit_id.clone(),
        actor_id: commit.actor_id.clone(),
        created_at: commit.created_at,
    }
}

/// The `table` rows of `version` on the lineage `scan` is the head of: each
/// registration with its greatest pin clock at or below `version`, dropped
/// when a tombstone clock at or below `version` is not older than that pin.
fn tables_at(scan: &HeadScan, version: u64, registrations: &[TableRegistration]) -> Vec<TableRow> {
    registrations
        .iter()
        .map(|registration| {
            let clocks = (registration.identity, 0)..=(registration.identity, version);
            let pin = scan.pins.range(clocks.clone()).next_back();
            let tombstone = scan.tombstones.range(clocks).next_back();
            let pinned_at = pin.map(|((_, clock), _)| *clock);
            let state = match (pin, tombstone) {
                (_, Some(((_, dropped_at), sealed_version)))
                    if pinned_at.is_none_or(|pinned_at| pinned_at <= *dropped_at) =>
                {
                    TableState::Dropped {
                        dropped_at: *dropped_at,
                        sealed_version: *sealed_version,
                    }
                }
                (Some((_, pin)), _) => TableState::Pinned(pin.clone()),
                (None, _) => TableState::Registered,
            };
            TableRow {
                registration: registration.clone(),
                state,
            }
        })
        .collect()
}

fn same_membership(tables: &[TableRow], registrations: &[TableRegistration]) -> bool {
    tables
        .iter()
        .map(|table| &table.registration)
        .eq(registrations)
}

fn check_record_bounds(record: &HistoryRecord, release_bytes: usize) -> CensusResult<()> {
    let fields = commit_bytes(&record.commit);
    let bytes = record.record_bytes();
    if fields > release_bytes || bytes > MAX_RECORD_BYTES {
        return Err(finding(
            FindingCode::RecordOverBound,
            format!(
                "graph commit '{}' records {fields} bytes of commit fields and {bytes} bytes with \
                 its table rows; one legacy record holds at most {release_bytes} bytes of commit \
                 fields and {MAX_RECORD_BYTES} bytes in all",
                record.commit.graph_commit_id
            ),
        ));
    }
    Ok(())
}

/// The table rows the census retains in records and heads, and the table
/// registrations of every [`SchemaAt`] and [`Deferred`] it keeps beside them,
/// against the bound on their inline bytes.
struct Snapshots {
    rows: u64,
    registrations: u64,
    max_bytes: u64,
}

impl Snapshots {
    fn over(&self, rows: u64, registrations: u64, retained: &str) -> CensusResult<()> {
        let bytes = rows
            .saturating_mul(size_of::<TableRow>() as u64)
            .saturating_add(registrations.saturating_mul(size_of::<TableRegistration>() as u64));
        if bytes <= self.max_bytes {
            return Ok(());
        }
        let held = match registrations {
            0 => format!("{rows} table rows"),
            _ => format!("{rows} table rows and {registrations} table registrations"),
        };
        Err(finding(
            FindingCode::CensusOverBound,
            format!(
                "{retained} {held}, {bytes} bytes of table snapshots, above the bound of {}; \
                 rebuild the graph with the build that wrote it",
                self.max_bytes
            ),
        ))
    }

    /// Refuse before any version is read when every own commit and head of a
    /// ref, and every orphan of its adopter, keeping the tables of the ref's
    /// head is over the bound: a head holds every registration of its lineage.
    fn admit(&self, lineages: &[Lineage<'_>], graph: &CommitGraph) -> CensusResult<()> {
        let mut holders: Vec<u64> = lineages
            .iter()
            .map(|lineage| lineage.own.len() as u64 + 1)
            .collect();
        for orphan in graph.orphans.values().flat_map(BTreeMap::values) {
            holders[orphan.adopter] += 1;
        }
        let rows = lineages
            .iter()
            .zip(&holders)
            .fold(0u64, |rows, (lineage, holders)| {
                rows.saturating_add(holders.saturating_mul(lineage.head_table_rows()))
            });
        let refs = lineages.len() as u64;
        let commits = holders.iter().sum::<u64>() - refs;
        self.over(
            rows,
            0,
            &format!(
                "no version was read: the records of {commits} graph commits and the heads of \
                 {refs} refs would retain"
            ),
        )
    }

    fn retain(&mut self, tables: usize) -> CensusResult<()> {
        self.rows = self.rows.saturating_add(tables as u64);
        self.retained()
    }

    /// Charge the registrations a [`SchemaAt`] or a [`Deferred`] keeps to the
    /// end of the census.
    fn retain_registrations(&mut self, tables: usize) -> CensusResult<()> {
        self.registrations = self.registrations.saturating_add(tables as u64);
        self.retained()
    }

    fn retained(&self) -> CensusResult<()> {
        self.over(
            self.rows,
            self.registrations,
            "the records and heads read so far retain",
        )
    }
}

const MEMBERSHIP: &str = "the table membership or an alias";
const PIN: &str = "a table pin or drop";

fn uncommitted(
    native: Option<&str>,
    version: u64,
    field: &str,
    nearest: Option<&GraphLineageRow>,
) -> CensusError {
    let since = match nearest {
        Some(commit) => format!(
            "graph commit '{}' at version {}",
            commit.graph_commit_id, commit.graph_manifest_version
        ),
        None => "the creation of the root, before its genesis commit".to_string(),
    };
    finding(
        FindingCode::UncommittedChange,
        format!(
            "version {version} of {} holds no graph commit, yet {field} changed since {since}",
            named(native)
        ),
    )
}

/// A version that holds no commit and lies before the first own commit of
/// its ref: its membership is compared with its inherited nearest commit once
/// every record exists.
struct Deferred {
    lineage: usize,
    version: u64,
    tables: Vec<TableRegistration>,
    nearest: String,
}

/// What reading the versions of one lineage shares with the others.
struct Reading<'a> {
    index: usize,
    main: &'a Dataset,
    source: &'a dyn LegacyManifestSource,
    graph: &'a CommitGraph,
    contracts: &'a mut Contracts,
    counts: &'a mut CensusCounts,
    deferred: &'a mut Vec<Deferred>,
    snapshots: &'a mut Snapshots,
}

/// One pinned ref: its head scan, its own commits by version, the clocks of
/// its pins and drops, its own versions, and what reading them derives.
/// `awaiting` indexes the records of versions that hold no contract row.
struct Lineage<'a> {
    kind: LegacyWriterKind,
    pinned: &'a CensusRef,
    head: Dataset,
    scan: HeadScan,
    own: Vec<usize>,
    clocks: Vec<u64>,
    versions: Vec<u64>,
    fallback: Option<usize>,
    records: Vec<HistoryRecord>,
    awaiting: Vec<usize>,
    first: Option<SchemaAt>,
    head_tables: Option<Vec<TableRow>>,
}

impl<'a> Lineage<'a> {
    fn new(
        kind: LegacyWriterKind,
        pinned: &'a CensusRef,
        head: Dataset,
        scan: HeadScan,
    ) -> CensusResult<Self> {
        let native = pinned.native.as_deref();
        if pinned.version < pinned.parent_version {
            return Err(OmniError::manifest_internal(format!(
                "census input: {} is pinned at version {}, below its fork point {}",
                named(native),
                pinned.version,
                pinned.parent_version
            ))
            .into());
        }
        let logical = native.map(logical_branch_name);
        let mut own = Vec::new();
        for (index, commit) in scan.commits.iter().enumerate() {
            let version = commit.graph_manifest_version;
            if version <= pinned.parent_version {
                continue;
            }
            if commit.graph_branch.as_deref() != logical || version > pinned.version {
                return Err(corrupt(format!(
                    "graph commit '{}' names branch {:?} and version {version}; {} wrote every \
                     commit above its fork point {} as branch {logical:?}, up to its head {}",
                    commit.graph_commit_id,
                    commit.graph_branch,
                    named(native),
                    pinned.parent_version,
                    pinned.version
                )));
            }
            canonical_id(&commit.graph_commit_id)?;
            own.push(index);
        }
        own.sort_by_key(|index| scan.commits[*index].graph_manifest_version);
        if let Some(pair) = own.windows(2).find(|pair| {
            scan.commits[pair[0]].graph_manifest_version
                == scan.commits[pair[1]].graph_manifest_version
        }) {
            return Err(corrupt(format!(
                "version {} of {} holds two graph commits, '{}' and '{}'",
                scan.commits[pair[0]].graph_manifest_version,
                named(native),
                scan.commits[pair[0]].graph_commit_id,
                scan.commits[pair[1]].graph_commit_id
            )));
        }
        let mut clocks: Vec<u64> = scan
            .pins
            .keys()
            .chain(scan.tombstones.keys())
            .map(|(_, clock)| *clock)
            .collect();
        clocks.sort_unstable();
        Ok(Self {
            kind,
            pinned,
            head,
            scan,
            own,
            clocks,
            versions: Vec::new(),
            fallback: None,
            records: Vec::new(),
            awaiting: Vec::new(),
            first: None,
            head_tables: None,
        })
    }

    fn own_commits(&self) -> impl Iterator<Item = &LegacyCommit> {
        self.own.iter().map(|index| &self.scan.commits[*index])
    }

    /// The `table` rows of the head: every scanned row that is no commit,
    /// pin, drop, head pointer or contract.
    fn head_table_rows(&self) -> u64 {
        let scan = &self.scan;
        let other = scan.commits.len()
            + scan.pins.len()
            + scan.tombstones.len()
            + scan.heads.len()
            + usize::from(scan.contract.is_some());
        scan.rows.saturating_sub(other as u64)
    }

    fn inherited_commits(&self) -> impl Iterator<Item = &LegacyCommit> {
        self.scan
            .commits
            .iter()
            .filter(|commit| commit.graph_manifest_version <= self.pinned.parent_version)
    }

    /// List the versions above the fork point up to the pinned head, and
    /// refuse a commit or a head whose version is no longer retained.
    async fn list_versions(&mut self) -> CensusResult<()> {
        let (parent, head) = (self.pinned.parent_version, self.pinned.version);
        let mut versions: Vec<u64> = self
            .head
            .versions()
            .await
            .map_err(OmniError::storage)?
            .into_iter()
            .map(|version| version.version)
            .filter(|version| *version > parent && *version <= head)
            .collect();
        versions.sort_unstable();
        versions.dedup();
        let lost: Vec<String> = self
            .own_commits()
            .map(|commit| commit.graph_manifest_version)
            .chain((head > parent).then_some(head))
            .filter(|version| versions.binary_search(version).is_err())
            .take(NAMED_PER_FINDING)
            .map(|version| version.to_string())
            .collect();
        if !lost.is_empty() {
            return Err(finding(
                FindingCode::LineageIncomplete,
                format!(
                    "{} no longer retains versions {} that hold its graph commits or its head",
                    named(self.pinned.native.as_deref()),
                    lost.join(", ")
                ),
            ));
        }
        self.versions = versions;
        Ok(())
    }

    /// The rows the version reads of this lineage scan: at each own version,
    /// the rows of the head that carry no clock and those dated at or below it.
    fn cells(&self) -> u64 {
        let scan = &self.scan;
        let mut dated: Vec<u64> = scan
            .commits
            .iter()
            .map(|commit| commit.graph_manifest_version)
            .chain(self.clocks.iter().copied())
            .collect();
        dated.sort_unstable();
        let undated = scan.rows.saturating_sub(dated.len() as u64);
        self.versions.iter().fold(0u64, |cells, version| {
            cells
                .saturating_add(undated)
                .saturating_add(dated.partition_point(|clock| clock <= version) as u64)
        })
    }

    fn changed_within(&self, after: u64, upto: u64) -> bool {
        self.clocks.partition_point(|clock| *clock <= after)
            < self.clocks.partition_point(|clock| *clock <= upto)
    }

    async fn read_versions(&mut self, reading: &mut Reading<'_>) -> CensusResult<()> {
        let pinned = self.pinned;
        let native = pinned.native.as_deref();
        let versions = std::mem::take(&mut self.versions);
        let mut previous: Option<VersionSchema> = None;
        let mut next_own = 0;
        for chunk in versions.chunks(READS_IN_FLIGHT) {
            let (main, source, before) = (reading.main, reading.source, previous.as_ref());
            let schemas = futures::future::try_join_all(chunk.iter().map(|version| async move {
                let dataset = checkout(main, native, *version).await?;
                source
                    .admits(&dataset, SourceRole::Version)
                    .map_err(unsupported)?;
                Ok::<_, CensusError>(source.version_schema(&dataset, before).await?)
            }))
            .await?;
            reading.counts.census_reads += chunk.len() as u64;
            for (version, schema) in chunk.iter().zip(&schemas) {
                self.fold_version(*version, schema, &mut next_own, reading)?;
            }
            previous = schemas.into_iter().next_back();
        }
        if next_own != self.own.len() {
            return Err(OmniError::manifest_internal(format!(
                "{} holds graph commits at versions its version reads did not reach",
                named(native)
            ))
            .into());
        }
        if versions.is_empty() {
            let schema = reading.source.version_schema(&self.head, None).await?;
            reading.counts.census_reads += 1;
            reading.snapshots.retain(schema.tables.len())?;
            reading
                .snapshots
                .retain_registrations(schema.tables.len())?;
            self.head_tables = Some(tables_at(&self.scan, self.pinned.version, &schema.tables));
            self.first = Some(SchemaAt {
                contract: match &schema.contract {
                    Some(contract) => Some(reading.contracts.intern(contract)?),
                    None => None,
                },
                tables: schema.tables,
            });
        }
        self.versions = versions;
        Ok(())
    }

    fn fold_version(
        &mut self,
        version: u64,
        schema: &VersionSchema,
        next_own: &mut usize,
        reading: &mut Reading<'_>,
    ) -> CensusResult<()> {
        let pinned = self.pinned;
        let native = pinned.native.as_deref();
        let contract = match &schema.contract {
            Some(contract) => Some(reading.contracts.intern(contract)?),
            None => None,
        };
        if self.first.is_none() {
            reading
                .snapshots
                .retain_registrations(schema.tables.len())?;
            self.first = Some(SchemaAt {
                tables: schema.tables.clone(),
                contract,
            });
        }
        if let Some(index) = contract {
            self.record_awaiting_under(&reading.contracts.rows[index])?;
        }
        let tables = tables_at(&self.scan, version, &schema.tables);
        if version == self.pinned.version {
            reading.snapshots.retain(tables.len())?;
            self.head_tables = Some(tables.clone());
        }
        let commit = self
            .own
            .get(*next_own)
            .map(|index| &self.scan.commits[*index])
            .filter(|commit| commit.graph_manifest_version == version);
        if let Some(commit) = commit {
            *next_own += 1;
            reading.snapshots.retain(tables.len())?;
            let contract = reading
                .contracts
                .named(contract.or(self.fallback), native, version)?;
            let record = HistoryRecord {
                commit: lineage_row(commit, native, reading.graph, contract),
                tables,
            };
            check_record_bounds(&record, crate::HISTORY_RELEASE_BYTES)?;
            if schema.contract.is_none() {
                self.awaiting.push(self.records.len());
            }
            self.records.push(record);
            return Ok(());
        }

        reading.counts.bookkeeping_versions += 1;
        if let Some(nearest) = self.records.last() {
            let field = if self.changed_within(nearest.commit.graph_manifest_version, version) {
                Some(PIN)
            } else if !same_membership(&nearest.tables, &schema.tables) {
                Some(MEMBERSHIP)
            } else {
                None
            };
            return match field {
                Some(field) => Err(uncommitted(native, version, field, Some(&nearest.commit))),
                None => Ok(()),
            };
        }
        match head_among(self.inherited_commits()) {
            Some(nearest) => {
                if self.changed_within(nearest.graph_manifest_version, version) {
                    return Err(finding(
                        FindingCode::UncommittedChange,
                        format!(
                            "version {version} of {} holds no graph commit, yet {PIN} changed \
                             since inherited graph commit '{}' at version {}",
                            named(native),
                            nearest.graph_commit_id,
                            nearest.graph_manifest_version
                        ),
                    ));
                }
                reading
                    .snapshots
                    .retain_registrations(schema.tables.len())?;
                reading.deferred.push(Deferred {
                    lineage: reading.index,
                    version,
                    tables: schema.tables.clone(),
                    nearest: nearest.graph_commit_id.clone(),
                });
            }
            None => {
                reading.counts.pre_genesis_versions += 1;
                let field = if self.changed_within(0, version) {
                    Some(PIN)
                } else if !schema.tables.is_empty() {
                    Some(MEMBERSHIP)
                } else {
                    None
                };
                if let Some(field) = field {
                    return Err(uncommitted(native, version, field, None));
                }
            }
        }
        Ok(())
    }

    /// Record the commits of versions that hold no contract row under the
    /// first contract a later version of this ref holds: the one the route
    /// that brought the ref to the content stamp wrote, without a commit.
    fn record_awaiting_under(
        &mut self,
        (contract, digest): &(SchemaContractRow, String),
    ) -> CensusResult<()> {
        for index in std::mem::take(&mut self.awaiting) {
            let record = &mut self.records[index];
            record.commit.schema_contract = Some(contract.head.clone());
            record.commit.schema_content_hash = Some(digest.clone());
            check_record_bounds(record, crate::HISTORY_RELEASE_BYTES)?;
        }
        Ok(())
    }

    /// The head this live ref is converted to, proven equal to the legacy
    /// record of the same commit: its tables at the head version, the contract
    /// its head row carries, and the graph_head pointer of a head it wrote.
    fn head_record(
        &self,
        by_id: &HashMap<&str, &HistoryRecord>,
        contracts: &mut Contracts,
    ) -> CensusResult<CensusHead> {
        let native = self.pinned.native.as_deref();
        let head = head_among(self.scan.commits.iter()).ok_or_else(|| {
            finding(
                FindingCode::LineageIncomplete,
                format!("{} holds no graph commit", named(native)),
            )
        })?;
        let key = native.map_or(MAIN_BRANCH_HEAD_KEY, logical_branch_name);
        if let Some(pointed) = self.scan.heads.get(key) {
            let own = self
                .own_commits()
                .any(|commit| commit.graph_commit_id == *pointed);
            if own && *pointed != head.graph_commit_id {
                return Err(corrupt(format!(
                    "the graph_head row of {} names its own commit '{pointed}', its lineage ends \
                     at '{}'",
                    named(native),
                    head.graph_commit_id
                )));
            }
        }
        let record = by_id.get(head.graph_commit_id.as_str()).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "head graph commit '{}' of {} has no record",
                head.graph_commit_id,
                named(native)
            ))
        })?;
        let pointed = self.scan.heads.get(key);
        if record.commit.native_branch.as_deref() == native
            && pointed != Some(&head.graph_commit_id)
        {
            return Err(corrupt(match pointed {
                Some(pointed) => format!(
                    "the graph_head row of {} names '{pointed}', its lineage ends at its own \
                     commit '{}'",
                    named(native),
                    head.graph_commit_id
                ),
                None => format!(
                    "{} holds no graph_head row for '{key}', its lineage ends at its own commit \
                     '{}'",
                    named(native),
                    head.graph_commit_id
                ),
            }));
        }
        let mismatch = |field: String| {
            finding(
                FindingCode::HeadRecordMismatch,
                format!(
                    "{} at its head version {} differs from the record of its head graph commit \
                     '{}' in {field}",
                    named(native),
                    self.pinned.version,
                    head.graph_commit_id
                ),
            )
        };
        let tables = self.head_tables.as_ref().ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "the head version of {} was not read",
                named(native)
            ))
        })?;
        if let Some(index) = (0..tables.len().max(record.tables.len()))
            .find(|index| tables.get(*index) != record.tables.get(*index))
        {
            let table = tables.get(index).or(record.tables.get(index));
            return Err(mismatch(format!(
                "the table row of '{}'",
                table.map_or("", |table| table.registration.table_key.as_str())
            )));
        }
        if let Some(current) = &self.scan.contract {
            let index = contracts.intern(current)?;
            if record.commit.schema_content_hash.as_ref() != Some(&contracts.rows[index].1) {
                return Err(mismatch("the schema contract".to_string()));
            }
        }
        Ok(CensusHead {
            native: self.pinned.native.clone(),
            version: self.pinned.version,
            record: (*record).clone(),
        })
    }
}

fn objects(census: &LegacyCensus, layout: &LegacyLayout) -> CensusResult<LegacyObjects> {
    let release_bytes = usize::try_from(layout.release_bytes).unwrap_or(usize::MAX);
    for record in census.chains.iter().flat_map(|chain| &chain.records) {
        check_record_bounds(record, release_bytes)?;
    }
    LegacyObjects::plan(
        layout,
        census.source_stamp,
        census.attempt,
        &census.chains,
        &census.absent_parents,
    )
    .map_err(|error| match error {
        OmniError::ResourceLimitExceeded {
            resource,
            limit,
            actual,
        } => finding(
            FindingCode::DirectoryOverBound,
            format!("the legacy set needs {actual} {resource}, above the bound of {limit}"),
        ),
        error => CensusError::Read(error),
    })
}

/// Cut `census` into the legacy objects under `layout`, in memory, and bind
/// them by the digest of the directory. The plan is a pure function of its
/// two arguments.
pub fn plan(census: &LegacyCensus, layout: &LegacyLayout) -> CensusResult<LegacyPlan> {
    plan_of(&objects(census, layout)?, layout)
}

fn plan_of(objects: &LegacyObjects, layout: &LegacyLayout) -> CensusResult<LegacyPlan> {
    let directory = objects.directory();
    let count = |length: usize, what: &str| {
        u32::try_from(length).map_err(|_| {
            OmniError::manifest_internal(format!("{length} legacy {what} overflow u32"))
        })
    };
    Ok(LegacyPlan {
        layout: *layout,
        commits: directory.commits,
        data_files: count(objects.files().len(), "data files")?,
        id_shards: count(objects.id_shards().len(), "id shards")?,
        writer_shards: count(objects.writer_shards().len(), "writer shards")?,
        directory_sha256: format!("{:x}", Sha256::digest(directory.encode()?)),
    })
}

/// Archive the schema contents of `census` and create its legacy objects, the
/// directory last. The objects are cut again under the layout of `plan` and
/// must be the ones it binds; an object that exists must hold the same bytes.
pub async fn materialize(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    census: &LegacyCensus,
    plan: &LegacyPlan,
) -> CensusResult<()> {
    let objects = objects(census, &plan.layout)?;
    let planned = plan_of(&objects, &plan.layout)?;
    if planned != *plan {
        return Err(finding(
            FindingCode::PlanChanged,
            format!(
                "the census plans directory {} over {} commits in {} data files; the upgrade \
                 that fenced the root bound directory {} over {} commits in {} data files",
                planned.directory_sha256,
                planned.commits,
                planned.data_files,
                plan.directory_sha256,
                plan.commits,
                plan.data_files
            ),
        ));
    }
    for contract in &census.contracts {
        history::archive_schema(root_uri, session, contract).await?;
    }
    history::write_legacy(root_uri, session, &objects).await?;
    Ok(())
}

/// The stamp-14 rows a live ref is converted to: the `table` rows of `head`,
/// its commit as the head row, an empty buffer, and the schema contract the
/// commit names, read from its archive. `metadata` is the schema metadata of
/// the converted version and carries stamp 14.
pub async fn conversion_batch(
    root_uri: &str,
    session: &Arc<lance::session::Session>,
    head: &HistoryRecord,
    metadata: HashMap<String, String>,
) -> Result<(SchemaRef, RecordBatch)> {
    let served = INTERNAL_MANIFEST_SCHEMA_VERSION.to_string();
    if metadata.get(INTERNAL_SCHEMA_VERSION_KEY) != Some(&served) {
        return Err(OmniError::manifest_internal(format!(
            "conversion rows are written under stamp {served}, the metadata carries {:?}",
            metadata.get(INTERNAL_SCHEMA_VERSION_KEY)
        )));
    }
    let commit = &head.commit;
    let (Some(identity), Some(digest)) = (&commit.schema_contract, &commit.schema_content_hash)
    else {
        return Err(OmniError::manifest_internal(format!(
            "legacy head graph commit '{}' names no archived schema content",
            commit.graph_commit_id
        )));
    };
    let contract = history::read_schema(root_uri, session, digest, identity).await?;
    let mut tables = head.tables.clone();
    tables.sort_by_key(|table| table.registration.identity);
    let rows = ManifestRows {
        tables,
        head: commit.clone(),
        buffer: CommitBuffer::default(),
        schema_contract_head: Some(contract.head.clone()),
        schema_contract: Some(contract),
    };
    let schema = crate::record::manifest_storage_schema(metadata);
    let batch = crate::record::compact_to_storage(&rows.to_batch()?, &schema)?;
    Ok((schema, batch))
}
