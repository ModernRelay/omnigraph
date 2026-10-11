use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray};
use futures::TryStreamExt;
use lance::Dataset;
use lance_table::format::Fragment;

use crate::error::{OmniError, Result};
use crate::history::{self, Appended, HistoryRecord};
use crate::record::expand_from_storage;
/// The row schema's public path; it is defined in the private `record` module.
pub use crate::record::manifest_schema;
use crate::row::{
    CommitColumns, CommitColumnsBuilder, RawTable, RegistryColumns, RegistryColumnsBuilder,
    ReplacedClock, ReplacedColumns, ReplacedColumnsBuilder, TableColumns, TableColumnsBuilder,
    metadata_measure,
};

use super::layout::{
    history_block_object_id, history_merge_object_id, replaced_table_object_id, table_object_id,
};
use super::metadata::TableVersionMetadata;
use super::migrations::{guard_row_layout, read_stamp};
use super::{
    HistoryReleaseBytes, MAIN_BRANCH_HEAD_KEY, OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_HISTORY_BLOCK,
    OBJECT_TYPE_HISTORY_MERGE, OBJECT_TYPE_REPLACED_TABLE, OBJECT_TYPE_SETTLED_COMMIT,
    OBJECT_TYPE_TABLE, TAIL_MAX_COMMITS, TableIdentity, TableRegistration,
};
use omnigraph_core::graph_commit_id::parse_history_block_id;
use ulid::Ulid;

#[derive(Debug, Clone)]
pub struct DatasetEntry {
    pub identity: TableIdentity,
    pub type_key: String,
    pub dataset_path: String,
    pub published_dataset_version: u64,
    pub native_dataset_branch: Option<String>,
    pub entity_count: u64,
    pub version_metadata: TableVersionMetadata,
    /// The `__manifest` version whose publish wrote this registration: the
    /// clock the projection orders registrations by (RFC 0062).
    pub manifest_version: u64,
}

impl DatasetEntry {
    /// Field-for-field equal registration, the Lance manifest metadata included.
    pub fn same_registration(&self, other: &DatasetEntry) -> bool {
        self.identity == other.identity
            && self.type_key == other.type_key
            && self.dataset_path == other.dataset_path
            && self.published_dataset_version == other.published_dataset_version
            && self.native_dataset_branch == other.native_dataset_branch
            && self.entity_count == other.entity_count
            && self.version_metadata == other.version_metadata
            && self.manifest_version == other.manifest_version
    }
}

#[derive(Debug, Clone)]
pub struct ManifestState {
    pub version: u64,
    pub entries: Vec<DatasetEntry>,
    /// The exact head of the branch read, keyed by its logical name (`main` for
    /// main), from this SAME manifest version: the head commit's id when the
    /// branch incarnation read wrote that commit, empty on a fork that has not
    /// published.
    pub graph_heads: HashMap<String, String>,
    pub schema_contract: Option<SchemaContractHead>,
}

/// The small fields of the `schema_contract` row: the JSON payload of its
/// `metadata` column and the part of the contract every manifest-state read
/// folds. The texts stay in the content columns, projected for serving admission
/// and by [`read_schema_contract_row`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SchemaContractHead {
    pub schema_ir_hash: String,
    pub schema_identity_version: u32,
    pub schema_identity_domain: String,
}

/// The whole `schema_contract` row: the `.pg` source and the IR JSON,
/// byte-exact, beside the folded head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaContractRow {
    pub source: String,
    pub ir: String,
    pub head: SchemaContractHead,
}

impl SchemaContractRow {
    /// A contract for a catalog-only fixture: the IR text and hash of the
    /// catalog's bound IR, an empty source, identity version 1.
    #[cfg(any(test, feature = "test-util"))]
    pub fn for_test_catalog(catalog: &omnigraph_compiler::catalog::Catalog) -> Result<Self> {
        let schema_ir = catalog.bound_schema_ir().ok_or_else(|| {
            OmniError::manifest_internal(
                "a test schema contract requires an identity-bound catalog".to_string(),
            )
        })?;
        Ok(Self {
            source: String::new(),
            ir: omnigraph_compiler::schema_ir_pretty_json(schema_ir)
                .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
            head: SchemaContractHead {
                schema_ir_hash: omnigraph_compiler::schema_ir_hash(schema_ir)
                    .map_err(|error| OmniError::manifest_internal(error.to_string()))?,
                schema_identity_version: 1,
                schema_identity_domain: schema_ir.schema_identity_domain.as_str().to_string(),
            },
        })
    }
}

/// The record of one graph commit: the `graph_commit` row of a `__manifest`
/// version while the commit is a branch's head, a `settled_commit` row while
/// the branch buffers it, a `__history` row once the branch has appended it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphLineageRow {
    pub graph_commit_id: String,
    /// Accepted schema identity of this commit, also retained in history.
    pub schema_contract: Option<SchemaContractHead>,
    /// The name of this commit's schema content under `__history/schemas/`
    /// ([`history::schema_content_hash`]); present exactly when
    /// `schema_contract` is.
    pub schema_content_hash: Option<String>,
    pub graph_branch: Option<String>,
    /// The Lance native ref of the branch incarnation that wrote the commit,
    /// `None` on main.
    pub native_branch: Option<String>,
    pub graph_manifest_version: u64,
    /// The greatest generation among the commit's parents plus one; the
    /// genesis commit is generation 0.
    #[serde(default)]
    pub generation: u64,
    pub parent_commit_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub actor_id: Option<String>,
    pub created_at: i64,
}

/// The pin of a live table: the table version a graph commit published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePin {
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
    pub metadata: TableVersionMetadata,
    /// The `__manifest` version whose publish wrote the pin.
    pub manifest_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableState {
    /// Registered, with no pin.
    Registered,
    Pinned(TablePin),
    /// Dropped by the publish of `__manifest` version `dropped_at`, which
    /// sealed the table at `sealed_version`. A dropped identity never returns.
    Dropped {
        dropped_at: u64,
        sealed_version: u64,
    },
}

/// One `table` row: the registration of a table identity and its state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRow {
    pub registration: TableRegistration,
    pub state: TableState,
}

/// What a `replaced_table` row restores in the versions before its publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplacedRow {
    /// The `table` row as the publish read it.
    Table(Box<TableRow>),
    /// No row: the publish registered the identity.
    Unregistered(TableRegistration),
}

/// One `replaced_table` row: what the publish of `__manifest` version
/// `replaced_at` read for a table identity it registered, pinned, renamed or
/// dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacedTable {
    pub replaced_at: u64,
    pub before: ReplacedRow,
}

impl ReplacedTable {
    pub fn identity(&self) -> TableIdentity {
        match &self.before {
            ReplacedRow::Table(table) => table.registration.identity,
            ReplacedRow::Unregistered(registration) => registration.identity,
        }
    }
}

const COMMIT_ROW_BYTES: usize = 128;
pub(crate) const TABLE_ROW_BYTES: usize = 256;

/// The size of one commit record as `__history` and the head row store it,
/// in the measure of [`CommitBuffer::buffered_bytes`].
pub(crate) fn commit_bytes(commit: &GraphLineageRow) -> usize {
    let contract = commit.schema_contract.as_ref();
    [
        Some(commit.graph_commit_id.as_str()),
        commit.graph_branch.as_deref(),
        commit.native_branch.as_deref(),
        commit.parent_commit_id.as_deref(),
        commit.merged_parent_commit_id.as_deref(),
        commit.actor_id.as_deref(),
        contract.map(|head| head.schema_ir_hash.as_str()),
        contract.map(|head| head.schema_identity_domain.as_str()),
        commit.schema_content_hash.as_deref(),
    ]
    .into_iter()
    .flatten()
    .map(str::len)
    .fold(COMMIT_ROW_BYTES, usize::saturating_add)
}

/// The size of one `table` row in the same measure.
pub(crate) fn table_bytes(table: &TableRow) -> usize {
    let registration = &table.registration;
    let pin = match &table.state {
        TableState::Pinned(pin) => {
            pin.table_branch.as_ref().map_or(0, String::len)
                + metadata_measure(&pin.metadata, None).unwrap_or(0)
        }
        _ => 0,
    };
    TABLE_ROW_BYTES + registration.table_key.len() + registration.table_path.len() + pin
}

/// The size of one `settled_commit` row as stored
/// ([`crate::row::CommitColumnsBuilder::push_settled`]): the parent only when
/// it is not `previous`, the schema contract only when it differs from `child`'s.
fn settled_bytes(
    commit: &GraphLineageRow,
    previous: Option<&GraphLineageRow>,
    child: &GraphLineageRow,
) -> usize {
    let own_parent = previous.is_none_or(|previous| {
        commit.parent_commit_id.as_deref() != Some(previous.graph_commit_id.as_str())
    });
    let own_contract = commit.schema_contract != child.schema_contract
        || commit.schema_content_hash != child.schema_content_hash;
    let contract = commit.schema_contract.as_ref().filter(|_| own_contract);
    [
        Some(commit.graph_commit_id.as_str()),
        commit.graph_branch.as_deref(),
        commit.native_branch.as_deref(),
        commit.parent_commit_id.as_deref().filter(|_| own_parent),
        commit.merged_parent_commit_id.as_deref(),
        commit.actor_id.as_deref(),
        contract.map(|head| head.schema_ir_hash.as_str()),
        contract.map(|head| head.schema_identity_domain.as_str()),
        commit
            .schema_content_hash
            .as_deref()
            .filter(|_| own_contract),
    ]
    .into_iter()
    .flatten()
    .map(str::len)
    .fold(COMMIT_ROW_BYTES, usize::saturating_add)
}

/// One `replaced_table` row as stored over `current`, the identity's `table` row of
/// the same version ([`crate::row::TableColumnsBuilder::push_replaced`]): the key only
/// when `current` has another, a pin's metadata as its delta against `current`'s pin.
fn replaced_bytes(replaced: &ReplacedTable, current: Option<&TableRow>) -> usize {
    let (registration, pin) = match &replaced.before {
        ReplacedRow::Table(table) => (
            &table.registration,
            match &table.state {
                TableState::Pinned(pin) => Some(pin),
                _ => None,
            },
        ),
        ReplacedRow::Unregistered(registration) => (registration, None),
    };
    let key = match current {
        Some(current) if current.registration.table_key == registration.table_key => 0,
        _ => registration.table_key.len(),
    };
    let pin = pin.map_or(0, |before| {
        let base = match current.map(|current| &current.state) {
            Some(TableState::Pinned(base)) => Some(&base.metadata),
            _ => None,
        };
        let metadata = metadata_measure(&before.metadata, base).unwrap_or(0);
        before.table_branch.as_ref().map_or(0, String::len) + metadata
    });
    TABLE_ROW_BYTES.saturating_add(key).saturating_add(pin)
}

#[cfg(test)]
thread_local! {
    /// The `table` rows [`CommitBuffer::tables_at`] has cloned on this thread.
    pub(crate) static TABLE_ROWS_REBUILT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// The buffer of one `__manifest` version: the most recent first-parent
/// ancestors of its head that the branch has not appended to `__history`,
/// oldest first, and the `replaced_table` rows that restore the `table` rows
/// each of them was written with.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitBuffer {
    pub(crate) commits: Vec<GraphLineageRow>,
    pub(crate) replaced: Vec<ReplacedTable>,
}

impl CommitBuffer {
    /// The buffered commits, oldest first.
    pub fn commits(&self) -> &[GraphLineageRow] {
        &self.commits
    }

    pub fn replaced(&self) -> &[ReplacedTable] {
        &self.replaced
    }

    pub fn get(&self, graph_commit_id: &str) -> Option<&GraphLineageRow> {
        self.commits
            .iter()
            .find(|commit| commit.graph_commit_id == graph_commit_id)
    }

    /// Whether the next publish on top of `head` appends the buffered commits
    /// to `__history`. A head with an `hb1` id carries the decision in its
    /// slot: slot 0 opened a new block, so the buffer holds a complete one.
    /// Any other head is measured over `current`, the `table` rows of the
    /// version that holds this buffer, and the commit ceiling bounds both.
    #[cfg(any(test, feature = "test-util"))]
    pub fn is_full<'a>(
        &self,
        head: &GraphLineageRow,
        current: impl IntoIterator<Item = &'a TableRow>,
    ) -> Result<bool> {
        self.is_full_under(head, current, HistoryReleaseBytes::PRODUCTION)
    }

    /// [`Self::is_full`] under `budget`, the byte budget of the publish that
    /// asks. A head with an `hb1` id answers from its slot whatever the
    /// budget, so sessions with different budgets agree; a malformed one is refused.
    pub(crate) fn is_full_under<'a>(
        &self,
        head: &GraphLineageRow,
        current: impl IntoIterator<Item = &'a TableRow>,
        budget: HistoryReleaseBytes,
    ) -> Result<bool> {
        if self.commits.is_empty() {
            return Ok(false);
        }
        if self.commits.len() >= TAIL_MAX_COMMITS {
            return Ok(true);
        }
        Ok(match parse_history_block_id(&head.graph_commit_id)? {
            Some(id) => id.slot == 0,
            None => self.buffered_bytes(head, current) >= budget.0,
        })
    }

    /// Whether the commit published on top of `head` is the last one the
    /// buffer takes before its release: the byte budget or the commit ceiling
    /// is reached once `head` joins the buffered commits. `current` is as in
    /// [`Self::buffered_bytes`].
    #[cfg(any(test, feature = "test-util"))]
    pub fn closes_with<'a>(
        &self,
        head: &GraphLineageRow,
        current: impl IntoIterator<Item = &'a TableRow>,
    ) -> bool {
        self.closes_with_under(head, current, HistoryReleaseBytes::PRODUCTION)
    }

    /// [`Self::closes_with`] under `budget`, as [`Self::is_full_under`].
    pub(crate) fn closes_with_under<'a>(
        &self,
        head: &GraphLineageRow,
        current: impl IntoIterator<Item = &'a TableRow>,
        budget: HistoryReleaseBytes,
    ) -> bool {
        self.buffered_bytes(head, current) >= budget.0 || self.commits.len() + 1 >= TAIL_MAX_COMMITS
    }

    /// The measure the byte budget is checked against: the buffered commits
    /// as stored, the `replaced_table` rows as stored over `current`, the
    /// `table` rows of the version that holds this buffer, and `head` whole,
    /// as string lengths plus a fixed allowance per row. A pure function of
    /// one `__manifest` version.
    pub fn buffered_bytes<'a>(
        &self,
        head: &GraphLineageRow,
        current: impl IntoIterator<Item = &'a TableRow>,
    ) -> usize {
        let settled = self.commits.iter().enumerate().map(|(index, commit)| {
            let previous = index.checked_sub(1).map(|previous| &self.commits[previous]);
            settled_bytes(
                commit,
                previous,
                self.commits.get(index + 1).unwrap_or(head),
            )
        });
        let replaced = (!self.replaced.is_empty()).then(|| {
            let current: HashMap<TableIdentity, &TableRow> = current
                .into_iter()
                .map(|table| (table.registration.identity, table))
                .collect();
            self.replaced
                .iter()
                .map(|replaced| {
                    replaced_bytes(replaced, current.get(&replaced.identity()).copied())
                })
                .fold(0, usize::saturating_add)
        });
        settled
            .chain(std::iter::once(commit_bytes(head)))
            .chain(replaced)
            .fold(0, usize::saturating_add)
    }

    /// The `table` rows of `__manifest` version `version`, from the `table`
    /// rows `current` of the version that holds this buffer: per identity the
    /// row the first publish after `version` read, else the current row. Exact
    /// for the version of the head and of every buffered commit.
    pub fn tables_at(&self, version: u64, current: &[TableRow]) -> Vec<TableRow> {
        let tables: Vec<TableRow> = self.rows_at(version, current).cloned().collect();
        #[cfg(test)]
        TABLE_ROWS_REBUILT.set(TABLE_ROWS_REBUILT.get() + tables.len());
        tables
    }

    /// The rows of [`Self::tables_at`], borrowed from `current` and the buffer.
    fn rows_at<'a>(
        &'a self,
        version: u64,
        current: &'a [TableRow],
    ) -> impl Iterator<Item = &'a TableRow> {
        let mut first_after = HashMap::<TableIdentity, &ReplacedTable>::new();
        for replaced in self.replaced.iter().filter(|row| row.replaced_at > version) {
            first_after
                .entry(replaced.identity())
                .and_modify(|first| {
                    if replaced.replaced_at < first.replaced_at {
                        *first = replaced;
                    }
                })
                .or_insert(replaced);
        }
        current.iter().filter_map(move |table| {
            match first_after.get(&table.registration.identity).copied() {
                None => Some(table),
                Some(ReplacedTable {
                    before: ReplacedRow::Table(before),
                    ..
                }) => Some(before.as_ref()),
                Some(ReplacedTable {
                    before: ReplacedRow::Unregistered(_),
                    ..
                }) => None,
            }
        })
    }

    /// The record of `commit`, a buffered commit or the head, with the `table`
    /// rows of the version that wrote it.
    pub fn record_of(&self, commit: &GraphLineageRow, current: &[TableRow]) -> HistoryRecord {
        HistoryRecord {
            commit: commit.clone(),
            tables: self.tables_at(commit.graph_manifest_version, current),
        }
    }

    /// The record of every buffered commit, oldest first.
    pub fn records(&self, current: &[TableRow]) -> Vec<HistoryRecord> {
        self.commits
            .iter()
            .map(|commit| self.record_of(commit, current))
            .collect()
    }

    /// The buffer of the version a publish writes over the version holding
    /// this one. `replaced_head` is the head that publish replaces, `changed`
    /// the rows it read and changed, `head` the head it writes, `current` the
    /// `table` rows it read, `budget` the byte budget of that publish. A full
    /// buffer gives way to `replaced_head` alone, and only when `appended`
    /// holds every commit that leaves.
    pub(crate) fn after_publish<'a>(
        &self,
        replaced_head: Option<&GraphLineageRow>,
        changed: Vec<ReplacedTable>,
        head: &GraphLineageRow,
        appended: Option<&Appended>,
        current: impl IntoIterator<Item = &'a TableRow>,
        budget: HistoryReleaseBytes,
    ) -> Result<Self> {
        let releases = match replaced_head {
            Some(head) => self.is_full_under(head, current, budget)?,
            None => false,
        };
        let mut commits = if releases {
            if let Some(kept) = self
                .commits
                .iter()
                .find(|commit| !appended.is_some_and(|appended| appended.holds(commit)))
            {
                return Err(OmniError::manifest_internal(format!(
                    "graph commit '{}' would leave the buffer of `__manifest` and no Append of \
                     this attempt wrote it to `__history`",
                    kept.graph_commit_id
                )));
            }
            Vec::new()
        } else {
            self.commits.clone()
        };
        commits.extend(replaced_head.cloned());
        let oldest = commits.first().unwrap_or(head).graph_manifest_version;
        let replaced = self
            .replaced
            .iter()
            .cloned()
            .chain(changed)
            .filter(|row| row.replaced_at > oldest)
            .collect();
        Ok(Self { commits, replaced })
    }
}

/// The records of every commit one `__manifest` version holds: the buffered
/// commits, oldest first, and the head. What a merge appends for its source
/// branch and a delete for the branch it retires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchRecords {
    pub buffer: CommitBuffer,
    pub head: HistoryRecord,
    /// The merge base the merging branch resolved, when it is known: the target
    /// descends from it, so it and the commits before it are not appended again.
    pub merge_base: Option<String>,
    /// The captured source's registry rows absent from the captured target,
    /// imported by the target publication for later ancestry walks.
    pub registry: LineageRegistry,
}

impl BranchRecords {
    /// The buffered commits, oldest first, then the head.
    pub fn commits(&self) -> impl Iterator<Item = &GraphLineageRow> {
        self.buffer
            .commits
            .iter()
            .chain(std::iter::once(&self.head.commit))
    }

    /// The record of each of [`Self::commits`], unbuilt.
    pub(crate) fn held(&self) -> impl Iterator<Item = HeldRecord<'_>> {
        self.commits().map(|commit| HeldRecord {
            commit,
            buffer: &self.buffer,
            current: &self.head.tables,
        })
    }

    /// The buffered records, oldest first, then the head, each built as it is taken.
    pub fn oldest_first(&self) -> impl Iterator<Item = HistoryRecord> {
        self.held().map(|held| history::RecordSource::record(&held))
    }

    pub fn get(&self, graph_commit_id: &str) -> Option<HistoryRecord> {
        self.held()
            .find(|held| held.commit.graph_commit_id == graph_commit_id)
            .map(|held| history::RecordSource::record(&held))
    }
}

impl From<HistoryRecord> for BranchRecords {
    /// The records of a version that buffers nothing.
    fn from(head: HistoryRecord) -> Self {
        Self {
            buffer: CommitBuffer::default(),
            head,
            merge_base: None,
            registry: LineageRegistry::default(),
        }
    }
}

/// The record of `commit`, a buffered commit or the head of the version whose
/// `table` rows are `current`, built when its extent is encoded.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeldRecord<'a> {
    pub(crate) commit: &'a GraphLineageRow,
    pub(crate) buffer: &'a CommitBuffer,
    pub(crate) current: &'a [TableRow],
}

impl history::RecordSource for HeldRecord<'_> {
    fn commit_row(&self) -> &GraphLineageRow {
        self.commit
    }

    fn record_bytes(&self) -> usize {
        self.buffer
            .rows_at(self.commit.graph_manifest_version, self.current)
            .map(table_bytes)
            .fold(commit_bytes(self.commit), usize::saturating_add)
    }

    fn record(&self) -> HistoryRecord {
        self.buffer.record_of(self.commit, self.current)
    }
}

/// One released first-parent run of a history block, as a `history_block`
/// row records it: slots `first_slot..=last_slot` of `block`, each slot's
/// first parent the slot before it, `first_slot`'s first parent
/// `parent_commit_id` (none for a genesis run), and the generation of
/// `first_slot`; a later slot's generation is its distance from `first_slot`
/// plus the jumps of the [`MergeEdge`]s between them.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockRun {
    pub block: Ulid,
    pub first_slot: u16,
    pub last_slot: u16,
    pub first_generation: u64,
    pub parent_commit_id: Option<String>,
}

/// One merge commit inside a released run, as a `history_merge` row records
/// it: the one edge that leaves the run's chain, and the generation the merge
/// gave its slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MergeEdge {
    pub block: Ulid,
    pub slot: u16,
    pub generation: u64,
    pub merged_parent_commit_id: String,
}

/// The block registry one `__manifest` version holds: the runs and the merge
/// edges of released history its branch wrote or merged, sorted, one copy of
/// each. Derived from `__history` and never its authority: a slot no run
/// describes is read from its block.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LineageRegistry {
    pub runs: Vec<BlockRun>,
    pub merges: Vec<MergeEdge>,
}

impl LineageRegistry {
    pub fn is_empty(&self) -> bool {
        self.runs.is_empty() && self.merges.is_empty()
    }

    pub fn len(&self) -> usize {
        self.runs.len() + self.merges.len()
    }

    /// Add `other`'s rows, keeping one copy of each.
    pub fn merge(&mut self, other: &LineageRegistry) {
        self.runs.extend(other.runs.iter().cloned());
        self.merges.extend(other.merges.iter().cloned());
        self.normalize();
    }

    /// Keep rows absent from the captured target, including distinct overlapping runs.
    pub fn retain_missing(&mut self, known: &Self) {
        let runs: HashSet<_> = known.runs.iter().collect();
        let merges: HashSet<_> = known.merges.iter().collect();
        self.runs.retain(|run| !runs.contains(run));
        self.merges.retain(|edge| !merges.contains(edge));
    }

    /// Every row, taken out of the hot fragment for a cold fragment of its
    /// own, once the hot fragment holds more than
    /// [`crate::commit::REGISTRY_HOT_ROWS`] of them; `None` leaves the rows
    /// where they are.
    pub(crate) fn cold_push(&mut self) -> Option<LineageRegistry> {
        (self.len() > crate::commit::REGISTRY_HOT_ROWS).then(|| std::mem::take(self))
    }

    /// The logical [`manifest_schema`] batch of these rows alone: what a cold
    /// fragment holds.
    pub(crate) fn to_batch(&self) -> Result<RecordBatch> {
        let mut batch = RowBatch::default();
        batch.push_registry(self);
        batch.finish(None)
    }

    fn normalize(&mut self) {
        self.runs.sort();
        self.runs.dedup();
        self.merges.sort();
        self.merges.dedup();
    }

    /// The rows of `commits`, a first-parent run oldest first as a publish
    /// releases it: one run per block the `hb1` ids name, split where the
    /// block or the slot sequence changes, and one edge per merged parent.
    /// Refuses a run whose slots are not the chain their ids claim: a slot's
    /// first parent must be the slot before it and its generation one above
    /// it, or more at a merge.
    pub fn of_released<'a>(commits: impl IntoIterator<Item = &'a GraphLineageRow>) -> Result<Self> {
        let mut registry = Self::default();
        let mut previous: Option<(
            &GraphLineageRow,
            omnigraph_core::graph_commit_id::HistoryBlockId,
        )> = None;
        for commit in commits {
            let Some(id) = parse_history_block_id(&commit.graph_commit_id)? else {
                previous = None;
                continue;
            };
            let continues = previous.as_ref().is_some_and(|(last, last_id)| {
                last_id.block == id.block
                    && last_id.slot + 1 == id.slot
                    && commit.parent_commit_id.as_deref() == Some(last.graph_commit_id.as_str())
            });
            if continues {
                let (last, _) = previous.expect("a continued run has a last commit");
                let floor = last.generation.saturating_add(1);
                let chain = if commit.merged_parent_commit_id.is_some() {
                    commit.generation >= floor
                } else {
                    commit.generation == floor
                };
                if !chain {
                    return Err(OmniError::manifest_internal(format!(
                        "graph commit '{}' at generation {} follows '{}' at generation {} in a \
                         released run; a run's generations rise by one, more at a merge",
                        commit.graph_commit_id,
                        commit.generation,
                        last.graph_commit_id,
                        last.generation
                    )));
                }
                let run = registry.runs.last_mut().expect("a continued run is open");
                run.last_slot = id.slot;
            } else {
                registry.runs.push(BlockRun {
                    block: id.block,
                    first_slot: id.slot,
                    last_slot: id.slot,
                    first_generation: commit.generation,
                    parent_commit_id: commit.parent_commit_id.clone(),
                });
            }
            if let Some(merged) = &commit.merged_parent_commit_id {
                registry.merges.push(MergeEdge {
                    block: id.block,
                    slot: id.slot,
                    generation: commit.generation,
                    merged_parent_commit_id: merged.clone(),
                });
            }
            previous = Some((commit, id));
        }
        registry.normalize();
        Ok(registry)
    }
}

/// Every row of one `__manifest` version: one row per table identity, the
/// head commit's record, the buffer, and the block registry rows the version
/// keeps in its hot fragment.
#[derive(Debug, Clone)]
pub(crate) struct ManifestRows {
    pub(crate) tables: Vec<TableRow>,
    pub(crate) head: GraphLineageRow,
    pub(crate) buffer: CommitBuffer,
    pub(crate) schema_contract_head: Option<SchemaContractHead>,
    pub(crate) schema_contract: Option<SchemaContractRow>,
    pub(crate) registry: LineageRegistry,
}

/// The columns of a [`manifest_schema`] batch under construction, one entry
/// per row in every builder.
#[derive(Default)]
struct RowBatch {
    object_ids: Vec<String>,
    object_types: Vec<&'static str>,
    tables: TableColumnsBuilder,
    commits: CommitColumnsBuilder,
    replaced: ReplacedColumnsBuilder,
    registry: RegistryColumnsBuilder,
}

impl RowBatch {
    fn push_registry(&mut self, registry: &LineageRegistry) {
        for run in &registry.runs {
            self.object_ids.push(history_block_object_id(
                run.block,
                run.first_slot,
                run.last_slot,
            ));
            self.object_types.push(OBJECT_TYPE_HISTORY_BLOCK);
            self.tables.push_null();
            self.commits.push_lineage_edges(
                run.first_generation,
                run.parent_commit_id.as_deref(),
                None,
            );
            self.replaced.push_null();
            self.registry.push_run(run);
        }
        for edge in &registry.merges {
            self.object_ids
                .push(history_merge_object_id(edge.block, edge.slot));
            self.object_types.push(OBJECT_TYPE_HISTORY_MERGE);
            self.tables.push_null();
            self.commits.push_lineage_edges(
                edge.generation,
                None,
                Some(&edge.merged_parent_commit_id),
            );
            self.replaced.push_null();
            self.registry.push_merge(edge);
        }
    }

    /// The batch, with `contract`'s texts on its last row, the contract row.
    fn finish(self, contract: Option<&SchemaContractRow>) -> Result<RecordBatch> {
        let row_count = self.object_types.len();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(self.object_ids)),
            Arc::new(StringArray::from(self.object_types)),
        ];
        columns.extend(self.tables.finish());
        columns.extend(self.commits.finish());
        columns.extend(self.replaced.finish());
        columns.extend(self.registry.finish());
        let mut sources = vec![None; row_count];
        let mut irs = vec![None; row_count];
        if let Some(contract) = contract {
            sources[row_count - 1] = Some(contract.source.as_str());
            irs[row_count - 1] = Some(contract.ir.as_str());
        }
        columns.push(Arc::new(LargeStringArray::from(sources)));
        columns.push(Arc::new(LargeStringArray::from(irs)));
        RecordBatch::try_new(manifest_schema(), columns).map_err(OmniError::arrow_internal)
    }
}

impl ManifestRows {
    /// The logical [`manifest_schema`] batch of these rows.
    pub(crate) fn to_batch(&self) -> Result<RecordBatch> {
        if self.schema_contract_head.as_ref() != self.schema_contract.as_ref().map(|row| &row.head)
        {
            return Err(OmniError::manifest_internal(
                "cannot serialize a projected or inconsistent schema contract",
            ));
        }
        let mut batch = RowBatch::default();
        for table in &self.tables {
            batch
                .object_ids
                .push(table_object_id(table.registration.identity));
            batch.object_types.push(OBJECT_TYPE_TABLE);
            batch.tables.push(table)?;
            batch.commits.push_null();
            batch.replaced.push_null();
            batch.registry.push_null();
        }
        batch.object_ids.push(self.head.graph_commit_id.clone());
        batch.object_types.push(OBJECT_TYPE_GRAPH_COMMIT);
        batch.tables.push_null();
        batch.commits.push(&self.head);
        batch.replaced.push_null();
        batch.registry.push_null();
        let buffered = &self.buffer.commits;
        for (index, commit) in buffered.iter().enumerate() {
            let child = buffered.get(index + 1).unwrap_or(&self.head);
            batch.object_ids.push(commit.graph_commit_id.clone());
            batch.object_types.push(OBJECT_TYPE_SETTLED_COMMIT);
            batch.tables.push_null();
            batch.commits.push_settled(
                commit,
                index.checked_sub(1).map(|_| &buffered[index - 1]),
                child,
            );
            batch.replaced.push_null();
            batch.registry.push_null();
        }
        batch.push_registry(&self.registry);
        let current: HashMap<TableIdentity, &TableRow> = self
            .tables
            .iter()
            .map(|table| (table.registration.identity, table))
            .collect();
        for row in &self.buffer.replaced {
            batch
                .object_ids
                .push(replaced_table_object_id(row.identity(), row.replaced_at));
            batch.object_types.push(OBJECT_TYPE_REPLACED_TABLE);
            let current = current.get(&row.identity()).copied();
            let registered = match &row.before {
                ReplacedRow::Table(table) => {
                    batch.tables.push_replaced(table, current)?;
                    false
                }
                ReplacedRow::Unregistered(registration) => {
                    batch.tables.push_replaced(
                        &TableRow {
                            registration: registration.clone(),
                            state: TableState::Registered,
                        },
                        current,
                    )?;
                    true
                }
            };
            batch.commits.push_null();
            batch.replaced.push(ReplacedClock {
                replaced_at: row.replaced_at,
                registered,
            });
            batch.registry.push_null();
        }

        if let Some(contract) = &self.schema_contract {
            batch
                .object_ids
                .push(crate::SCHEMA_CONTRACT_OBJECT_ID.to_string());
            batch.object_types.push(crate::OBJECT_TYPE_SCHEMA_CONTRACT);
            batch.tables.push_contract(&contract.head)?;
            batch.commits.push_null();
            batch.replaced.push_null();
            batch.registry.push_null();
        }
        batch.finish(self.schema_contract.as_ref())
    }

    /// The records of the buffered commits, oldest first, and of the head.
    pub(crate) fn records(&self) -> BranchRecords {
        BranchRecords {
            buffer: self.buffer.clone(),
            head: HistoryRecord {
                commit: self.head.clone(),
                tables: self.tables.clone(),
            },
            merge_base: None,
            registry: self.registry.clone(),
        }
    }

    /// The record of the head or of the buffered commit `graph_commit_id`.
    pub(crate) fn record(&self, graph_commit_id: &str) -> Option<HistoryRecord> {
        let held = std::iter::once(&self.head).chain(self.buffer.commits());
        held.into_iter()
            .find(|commit| commit.graph_commit_id == graph_commit_id)
            .map(|commit| self.buffer.record_of(commit, &self.tables))
    }

    /// The visible state of these rows, read at `version` of the branch whose
    /// native ref is `native_branch`: the pinned tables by alias, and the exact
    /// head.
    pub(crate) fn state(&self, version: u64, native_branch: Option<&str>) -> Result<ManifestState> {
        let mut state = manifest_state(&self.tables, &self.head, version, native_branch)?;
        state.schema_contract = self.schema_contract_head.clone();
        Ok(state)
    }
}

/// The visible state of the `table` rows and the head commit's record of one
/// `__manifest` version, as [`ManifestRows::state`] reads it.
pub(crate) fn manifest_state(
    tables: &[TableRow],
    head: &GraphLineageRow,
    version: u64,
    native_branch: Option<&str>,
) -> Result<ManifestState> {
    let mut entries: Vec<DatasetEntry> = tables
        .iter()
        .filter_map(|table| match &table.state {
            TableState::Pinned(pin) => Some(dataset_entry(&table.registration, pin)),
            TableState::Registered | TableState::Dropped { .. } => None,
        })
        .collect();
    let mut aliases = HashMap::<&str, TableIdentity>::new();
    for entry in &entries {
        if let Some(existing) = aliases.insert(&entry.type_key, entry.identity) {
            return Err(OmniError::manifest_internal(format!(
                "manifest has two live table identities ({existing} and {}) bound to alias '{}'",
                entry.identity, entry.type_key
            )));
        }
    }
    entries.sort_by(|a, b| a.type_key.cmp(&b.type_key));
    Ok(ManifestState {
        schema_contract: head.schema_contract.clone(),
        version,
        entries,
        graph_heads: exact_head(head, native_branch)
            .map(|head| (head_key(head), head.graph_commit_id.clone()))
            .into_iter()
            .collect(),
    })
}

fn dataset_entry(registration: &TableRegistration, pin: &TablePin) -> DatasetEntry {
    DatasetEntry {
        identity: registration.identity,
        type_key: registration.table_key.clone(),
        dataset_path: registration.table_path.clone(),
        published_dataset_version: pin.table_version,
        native_dataset_branch: pin.table_branch.clone(),
        entity_count: pin.row_count,
        version_metadata: pin.metadata.clone(),
        manifest_version: pin.manifest_version,
    }
}

/// `head` when the branch incarnation `native_branch` wrote it. A fork that has
/// not published holds the head its source wrote, which is its effective head
/// and not its exact one.
pub(crate) fn exact_head<'a>(
    head: &'a GraphLineageRow,
    native_branch: Option<&str>,
) -> Option<&'a GraphLineageRow> {
    (head.native_branch.as_deref() == native_branch).then_some(head)
}

fn head_key(head: &GraphLineageRow) -> String {
    head.graph_branch
        .as_deref()
        .unwrap_or(MAIN_BRANCH_HEAD_KEY)
        .to_string()
}

/// Full rows for a publisher: source and IR must be present and valid.
pub(crate) async fn read_manifest_rows(dataset: &Dataset) -> Result<ManifestRows> {
    let (rows, content_error) = scan_manifest_rows(dataset, true).await?;
    if let Some(error) = content_error {
        return Err(error);
    }
    Ok(rows)
}

/// Ordinary state, history and retention reads need the accepted identity, not its texts.
pub(crate) async fn read_manifest_rows_projected(dataset: &Dataset) -> Result<ManifestRows> {
    let (rows, _) = scan_manifest_rows(dataset, false).await?;
    Ok(rows)
}

/// Cold admission reads all columns once, using main's bounded scan-local IO policy.
/// Content errors stay deferred until the caller has refreshed this exact capture.
pub(crate) async fn read_manifest_rows_with_contract(
    dataset: &Dataset,
) -> Result<(ManifestRows, Result<SchemaContractRow>)> {
    let content_shape = require_schema_content(dataset);
    let scan_dataset = if content_shape.is_ok()
        && !dataset
            .schema()
            .metadata
            .contains_key(crate::migrations::UPGRADE_PENDING_KEY)
    {
        crate::instrumentation::manifest_scan_dataset(dataset).await?
    } else {
        dataset.clone()
    };
    let (rows, error) = scan_manifest_rows(&scan_dataset, content_shape.is_ok()).await?;
    let contract = content_shape.and(error.map_or(Ok(()), Err)).and_then(|()| {
        finish_schema_contract_row(
            rows.schema_contract.clone(),
            rows.schema_contract_head.as_ref(),
            dataset.version().version,
        )
    });
    Ok((rows, contract))
}

async fn scan_manifest_rows(
    dataset: &Dataset,
    with_content: bool,
) -> Result<(ManifestRows, Option<OmniError>)> {
    guard_row_layout(dataset)?;
    scan_rows_unguarded(dataset, with_content).await
}

/// The fragment of a `__manifest` version that holds its head row, buffer,
/// `table` rows, schema contract and hot registry rows: the one with the
/// highest id, which every publish lists last among the fragments it writes
/// (`commit.rs`). A version of one fragment is all hot.
pub(crate) fn hot_fragment(dataset: &Dataset) -> Result<Fragment> {
    dataset
        .fragments()
        .iter()
        .max_by_key(|fragment| fragment.id)
        .cloned()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "`__manifest` version {} holds no fragment",
                dataset.version().version
            ))
        })
}

/// The fragments of a `__manifest` version beside [`hot_fragment`]: each holds
/// cold registry rows and nothing else, and a publish names it again without
/// rewriting it.
pub(crate) fn cold_fragments(dataset: &Dataset) -> Vec<Fragment> {
    let hot = dataset.fragments().iter().map(|fragment| fragment.id).max();
    dataset
        .fragments()
        .iter()
        .filter(|fragment| Some(fragment.id) != hot)
        .cloned()
        .collect()
}

/// A scan of `dataset` over its hot fragment alone when it has cold ones.
fn hot_scanner(dataset: &Dataset) -> Result<lance::dataset::scanner::Scanner> {
    let mut scanner = dataset.scan();
    if dataset.fragments().len() > 1 {
        scanner.with_fragments(vec![hot_fragment(dataset)?]);
    }
    Ok(scanner)
}

/// [`scan_manifest_rows`] without the stamp and pending-key guard, which the
/// caller has answered for. Reads the hot fragment only.
async fn scan_rows_unguarded(
    dataset: &Dataset,
    with_content: bool,
) -> Result<(ManifestRows, Option<OmniError>)> {
    crate::instrumentation::record_manifest_scan();
    let mut scanner = hot_scanner(dataset)?;
    scanner
        .project(&crate::record::packed_projection(dataset, with_content))
        .map_err(OmniError::storage)?;
    let mut batches = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;
    let mut rows = RowsDecoder::default();
    while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
        rows.push(&batch, Some(dataset.version().version), with_content)?;
    }
    let error = rows.content_error.take();
    Ok((rows.finish()?, error))
}

/// The registry rows of `fragments`, cold fragments of `dataset`, which hold
/// no other row. One scan, so one request per fragment; a merge keeps the
/// result per handle.
pub(crate) async fn read_cold_registry(
    dataset: &Dataset,
    fragments: &[Fragment],
) -> Result<LineageRegistry> {
    if fragments.is_empty() {
        return Ok(LineageRegistry::default());
    }
    crate::instrumentation::record_manifest_scan();
    let mut scanner = dataset.scan();
    scanner.with_fragments(fragments.to_vec());
    scanner
        .project(&crate::record::packed_projection(dataset, false))
        .map_err(OmniError::storage)?;
    let mut batches = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;
    let mut rows = RowsDecoder::default();
    while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
        rows.push(&batch, Some(dataset.version().version), false)?;
    }
    rows.into_registry()
}

/// Strict decoding of the acknowledged batch retained by the publisher cache.
pub(crate) fn rows_of_batch(stored: &RecordBatch) -> Result<ManifestRows> {
    let mut rows = RowsDecoder::default();
    rows.push(stored, None, true)?;
    if let Some(error) = rows.content_error.take() {
        return Err(error);
    }
    rows.finish()
}

#[derive(Default)]
struct RowsDecoder {
    schema_contract_head: Option<SchemaContractHead>,
    schema_contract: Option<SchemaContractRow>,
    content_error: Option<OmniError>,
    tables: BTreeMap<TableIdentity, TableRow>,
    head: Option<GraphLineageRow>,
    buffered: Vec<(GraphLineageRow, bool)>,
    replaced: BTreeMap<(u64, TableIdentity), (ReplacedClock, RawTable)>,
    registry: LineageRegistry,
    version: Option<u64>,
}

/// Refuse a commit row read at `version` that names a later `__manifest` version.
fn check_commit_clock(commit: &GraphLineageRow, version: Option<u64>) -> Result<()> {
    if version.is_some_and(|version| commit.graph_manifest_version > version) {
        return Err(OmniError::manifest_internal(format!(
            "graph commit {} carries manifest version {} above the scanned dataset version",
            commit.graph_commit_id, commit.graph_manifest_version
        )));
    }
    Ok(())
}

impl RowsDecoder {
    /// Decode a stored batch read at `version`, which no clock of a row may exceed.
    fn push(
        &mut self,
        stored: &RecordBatch,
        version: Option<u64>,
        with_content: bool,
    ) -> Result<()> {
        self.version = version;
        let batch = expand_from_storage(stored)?;
        let lookup = |name: &str| batch.column_by_name(name);
        let object_ids = string_column(&batch, "object_id")?;
        let object_types = string_column(&batch, "object_type")?;
        let table_columns = TableColumns::new(&lookup)?;
        let commit_columns = CommitColumns::new(&lookup)?;
        let replaced_columns = ReplacedColumns::new(&lookup)?;
        let registry_columns = RegistryColumns::new(&lookup)?;
        if with_content && self.content_error.is_none() {
            if let Err(error) = fold_schema_contract_batch(&batch, &mut self.schema_contract) {
                self.content_error = Some(error);
            }
        }
        for row in 0..batch.num_rows() {
            let object_type = object_types.value(row);
            if object_ids.value(row) == crate::SCHEMA_CONTRACT_OBJECT_ID
                && object_type != crate::OBJECT_TYPE_SCHEMA_CONTRACT
            {
                return Err(OmniError::manifest_internal(format!(
                    "manifest row '{}' has object_type '{object_type}'",
                    crate::SCHEMA_CONTRACT_OBJECT_ID
                )));
            }
            let foreign = match object_type {
                OBJECT_TYPE_TABLE => !commit_columns.is_null(row) || !replaced_columns.is_null(row),
                OBJECT_TYPE_GRAPH_COMMIT | OBJECT_TYPE_SETTLED_COMMIT => {
                    !table_columns.is_null(row) || !replaced_columns.is_null(row)
                }
                OBJECT_TYPE_REPLACED_TABLE => !commit_columns.is_null(row),
                OBJECT_TYPE_HISTORY_BLOCK | OBJECT_TYPE_HISTORY_MERGE => {
                    !table_columns.is_null(row)
                        || !replaced_columns.is_null(row)
                        || !commit_columns.holds_only_lineage_edges(row)
                }
                _ => false,
            };
            let registry_row = matches!(
                object_type,
                OBJECT_TYPE_HISTORY_BLOCK | OBJECT_TYPE_HISTORY_MERGE
            );
            if foreign || (!registry_row && !registry_columns.is_null(row)) {
                return Err(foreign_fields(object_type, row));
            }
            match object_type {
                OBJECT_TYPE_HISTORY_BLOCK => {
                    let (block, first_slot, last_slot) = registry_columns.decode(row)?;
                    let (generation, parent_commit_id, merged) =
                        commit_columns.lineage_edges(row)?;
                    let expected = history_block_object_id(block, first_slot, last_slot);
                    if object_ids.value(row) != expected
                        || first_slot > last_slot
                        || merged.is_some()
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest history_block row at index {row} has object_id '{}', \
                             expected '{expected}' with no merged parent",
                            object_ids.value(row)
                        )));
                    }
                    self.registry.runs.push(BlockRun {
                        block,
                        first_slot,
                        last_slot,
                        first_generation: generation,
                        parent_commit_id,
                    });
                }
                OBJECT_TYPE_HISTORY_MERGE => {
                    let (block, slot, last_slot) = registry_columns.decode(row)?;
                    let (generation, parent_commit_id, merged) =
                        commit_columns.lineage_edges(row)?;
                    let expected = history_merge_object_id(block, slot);
                    let (Some(merged_parent_commit_id), None) = (merged, parent_commit_id) else {
                        return Err(foreign_fields(object_type, row));
                    };
                    if object_ids.value(row) != expected || slot != last_slot {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest history_merge row at index {row} has object_id '{}', \
                             expected '{expected}'",
                            object_ids.value(row)
                        )));
                    }
                    self.registry.merges.push(MergeEdge {
                        block,
                        slot,
                        generation,
                        merged_parent_commit_id,
                    });
                }
                crate::OBJECT_TYPE_SCHEMA_CONTRACT => {
                    if !commit_columns.is_null(row) || !replaced_columns.is_null(row) {
                        return Err(foreign_fields(object_type, row));
                    }
                    let head = decode_schema_contract_row(
                        object_ids,
                        string_column(&batch, "metadata")?,
                        row,
                    )?;
                    if self.schema_contract_head.replace(head).is_some() {
                        return Err(OmniError::manifest_internal(
                            "manifest has two schema_contract rows",
                        ));
                    }
                }
                OBJECT_TYPE_TABLE => {
                    let table = table_columns.decode(row, version)?;
                    let identity = table.registration.identity;
                    let expected = table_object_id(identity);
                    if object_ids.value(row) != expected {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table row at index {row} has object_id '{}', expected '{expected}'",
                            object_ids.value(row)
                        )));
                    }
                    if self.tables.insert(identity, table).is_some() {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest has two table rows for identity {identity}"
                        )));
                    }
                }
                OBJECT_TYPE_GRAPH_COMMIT => {
                    let (commit, inherits) = commit_columns.decode(row, object_ids.value(row))?;
                    if inherits {
                        return Err(OmniError::manifest_internal(format!(
                            "head commit '{}' inherits a schema contract and has no newer commit",
                            commit.graph_commit_id
                        )));
                    }
                    check_commit_clock(&commit, version)?;
                    if let Some(first) = self.head.replace(commit) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest holds more than one graph_commit row ({} among them); a \
                             branch holds one head commit",
                            first.graph_commit_id
                        )));
                    }
                }
                OBJECT_TYPE_SETTLED_COMMIT => {
                    let (commit, inherits) = commit_columns.decode(row, object_ids.value(row))?;
                    check_commit_clock(&commit, version)?;
                    self.buffered.push((commit, inherits));
                }
                OBJECT_TYPE_REPLACED_TABLE => {
                    let clock = replaced_columns.decode(row)?;
                    let table = table_columns.raw(row)?;
                    let expected = replaced_table_object_id(table.identity, clock.replaced_at);
                    if object_ids.value(row) != expected {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest replaced_table row at index {row} has object_id '{}', \
                             expected '{expected}'",
                            object_ids.value(row)
                        )));
                    }
                    if version.is_some_and(|version| clock.replaced_at > version) {
                        return Err(OmniError::manifest_internal(format!(
                            "replaced_table row '{expected}' is replaced above the scanned \
                             dataset version"
                        )));
                    }
                    if self
                        .replaced
                        .insert((clock.replaced_at, table.identity), (clock, table))
                        .is_some()
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest has two replaced_table rows '{expected}'"
                        )));
                    }
                }
                other => {
                    return Err(OmniError::manifest_internal(format!(
                        "manifest row at index {row} has object type '{other}', which this \
                         storage format does not hold"
                    )));
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<ManifestRows> {
        let head = self.head.ok_or_else(|| {
            OmniError::manifest_internal(
                "manifest holds no graph_commit row; every version carries its head commit"
                    .to_string(),
            )
        })?;
        let mut buffered = self.buffered;
        buffered.sort_by_key(|(commit, _)| commit.graph_manifest_version);
        if buffered.len() > TAIL_MAX_COMMITS {
            return Err(OmniError::manifest_internal(format!(
                "manifest buffers {} commits, above the bound of {TAIL_MAX_COMMITS}",
                buffered.len()
            )));
        }
        let mut commits: Vec<GraphLineageRow> = Vec::with_capacity(buffered.len());
        for (mut commit, inherits) in buffered.into_iter().rev() {
            if inherits {
                let child = commits.last().unwrap_or(&head);
                commit.schema_contract = child.schema_contract.clone();
                commit.schema_content_hash = child.schema_content_hash.clone();
            }
            commits.push(commit);
        }
        commits.reverse();
        for index in 1..commits.len() {
            if commits[index].parent_commit_id.is_none() {
                commits[index].parent_commit_id = Some(commits[index - 1].graph_commit_id.clone());
            }
        }
        let children = commits.iter().skip(1).chain(std::iter::once(&head));
        if let Some((parent, child)) = commits.iter().zip(children).find(|(parent, child)| {
            child.parent_commit_id.as_deref() != Some(parent.graph_commit_id.as_str())
        }) {
            return Err(OmniError::manifest_internal(format!(
                "manifest buffers graph commit '{}' and the next commit it holds, '{}', is not \
                 its first-parent child",
                parent.graph_commit_id, child.graph_commit_id
            )));
        }
        let mut replaced = Vec::with_capacity(self.replaced.len());
        for ((replaced_at, identity), (clock, raw)) in self.replaced {
            let Some(current) = self.tables.get(&identity) else {
                return Err(OmniError::manifest_internal(format!(
                    "manifest has a replaced_table row for identity {identity} and no table row \
                     for it"
                )));
            };
            let table = raw.into_replaced_row(Some(current), self.version)?;
            let before = match (clock.registered, table.state) {
                (false, state) => ReplacedRow::Table(Box::new(TableRow {
                    registration: table.registration,
                    state,
                })),
                (true, TableState::Registered) => ReplacedRow::Unregistered(table.registration),
                (true, _) => {
                    return Err(OmniError::manifest_internal(format!(
                        "replaced_table row '{}' marks a registration and carries a pin or a \
                         drop",
                        replaced_table_object_id(identity, replaced_at)
                    )));
                }
            };
            replaced.push(ReplacedTable {
                replaced_at,
                before,
            });
        }
        let mut registry = self.registry;
        registry.normalize();
        Ok(ManifestRows {
            schema_contract_head: self.schema_contract_head,
            schema_contract: self.schema_contract,
            tables: self.tables.into_values().collect(),
            head,
            buffer: CommitBuffer { commits, replaced },
            registry,
        })
    }

    /// The rows of cold fragments: registry rows and nothing else.
    fn into_registry(self) -> Result<LineageRegistry> {
        if self.head.is_some()
            || !self.tables.is_empty()
            || !self.buffered.is_empty()
            || !self.replaced.is_empty()
            || self.schema_contract_head.is_some()
            || self.schema_contract.is_some()
        {
            return Err(OmniError::manifest_internal(
                "a cold fragment of `__manifest` holds rows beside the block registry",
            ));
        }
        let mut registry = self.registry;
        registry.normalize();
        Ok(registry)
    }
}

fn foreign_fields(object_type: &str, row: usize) -> OmniError {
    OmniError::manifest_internal(format!(
        "manifest {object_type} row at index {row} carries fields of another row type"
    ))
}

pub async fn read_manifest_state(dataset: &Dataset) -> Result<ManifestState> {
    read_manifest_rows_projected(dataset).await?.state(
        dataset.version().version,
        dataset.manifest().branch.as_deref(),
    )
}

/// The visible state and the schema contract of a version the offline storage
/// upgrade wrote, from one scan. Unlike every other reader it reads through
/// the pending key main's conversion carries, so the upgrade can compare and
/// validate what it converted before it activates the root. The stamp must
/// still be one this build serves, and the version must hold the contract.
#[doc(hidden)]
pub async fn read_converted_state(dataset: &Dataset) -> Result<(ManifestState, SchemaContractRow)> {
    let version = dataset.version().version;
    let stamp = read_stamp(dataset).ok_or_else(|| {
        OmniError::manifest(format!(
            "__manifest version {version} carries no internal-schema stamp that is a version \
             number; a converted version is stamped"
        ))
    })?;
    crate::migrations::refuse_if_stamp_unsupported(stamp)?;
    require_schema_content(dataset)?;
    let (rows, content_error) = scan_rows_unguarded(dataset, true).await?;
    if let Some(error) = content_error {
        return Err(error);
    }
    let state = rows.state(version, dataset.manifest().branch.as_deref())?;
    let contract = finish_schema_contract_row(
        rows.schema_contract,
        rows.schema_contract_head.as_ref(),
        version,
    )?;
    Ok((state, contract))
}

/// The visible table state of `dataset`'s version and the rows it was read
/// from, the head commit's record among them, from one scan. Reads
/// `__manifest` only.
pub(crate) async fn read_manifest_state_and_rows(
    dataset: &Dataset,
) -> Result<(ManifestState, ManifestRows)> {
    let rows = read_manifest_rows_projected(dataset).await?;
    let state = rows.state(
        dataset.version().version,
        dataset.manifest().branch.as_deref(),
    )?;
    Ok((state, rows))
}

/// Every pin the branch `dataset` is checked out on has held: the pins of its
/// `__manifest` version, of the commits that version buffers, and the pins
/// recorded with each older commit of its lineage, which `__history` holds.
pub(crate) async fn read_manifest_entries(
    root_uri: &str,
    dataset: &Dataset,
) -> Result<Vec<DatasetEntry>> {
    let rows = read_manifest_rows_projected(dataset).await?;
    let oldest = rows.buffer.commits.first().unwrap_or(&rows.head);
    let mut parent = oldest.parent_commit_id.clone();
    let mut records = match parent {
        Some(_) => history::read_records(root_uri, &dataset.session()).await?,
        None => HashMap::new(),
    };
    let mut pins = BTreeMap::<(TableIdentity, u64), TablePin>::new();
    for buffered in &rows.buffer.commits {
        for table in rows
            .buffer
            .rows_at(buffered.graph_manifest_version, &rows.tables)
        {
            if let TableState::Pinned(pin) = &table.state {
                pins.insert(
                    (table.registration.identity, pin.manifest_version),
                    pin.clone(),
                );
            }
        }
    }
    while let Some(id) = parent {
        let record = records.remove(&id).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "graph commit '{id}' is the parent of a commit on this branch and `__history` \
                 does not hold it"
            ))
        })?;
        parent = record.commit.parent_commit_id;
        for table in record.tables {
            if let TableState::Pinned(pin) = table.state {
                pins.insert((table.registration.identity, pin.manifest_version), pin);
            }
        }
    }
    let mut registrations = HashMap::with_capacity(rows.tables.len());
    for table in rows.tables {
        if let TableState::Pinned(pin) = table.state {
            pins.insert((table.registration.identity, pin.manifest_version), pin);
        }
        registrations.insert(table.registration.identity, table.registration);
    }
    pins.into_iter()
        .map(|((identity, _), pin)| {
            let registration = registrations.get(&identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "manifest missing table row for identity {identity}"
                ))
            })?;
            Ok(dataset_entry(registration, &pin))
        })
        .collect()
}

pub(crate) async fn read_manifest_table_registrations(
    dataset: &Dataset,
) -> Result<Vec<TableRegistration>> {
    Ok(read_manifest_rows_projected(dataset)
        .await?
        .tables
        .into_iter()
        .map(|table| table.registration)
        .collect())
}

pub(crate) fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest column '{name}' is not Utf8"))
        })
}

/// Decode one `schema_contract` row's head out of its `metadata` column.
/// Shared by the state scan and the content read, so the two cannot drift.
fn decode_schema_contract_row(
    object_ids: &StringArray,
    metadata: &StringArray,
    row: usize,
) -> Result<SchemaContractHead> {
    require_object_id(
        object_ids,
        row,
        crate::SCHEMA_CONTRACT_OBJECT_ID,
        crate::OBJECT_TYPE_SCHEMA_CONTRACT,
    )?;
    if metadata.is_null(row) {
        return Err(OmniError::manifest_internal(
            "manifest schema_contract row missing metadata".to_string(),
        ));
    }
    serde_json::from_str(metadata.value(row)).map_err(|e| {
        OmniError::manifest_internal(format!("failed to decode schema_contract metadata: {e}"))
    })
}

/// Read the complete contract at the pinned version and verify its folded head.
/// AllEarly prevents Lance 11's heuristic from splitting the packed record
/// into incompatible child projections during the filtered scan.
pub(crate) async fn read_schema_contract_row(
    dataset: &Dataset,
    expected: Option<&SchemaContractHead>,
) -> Result<SchemaContractRow> {
    require_schema_content(dataset)?;
    crate::instrumentation::record_manifest_scan();
    let mut scanner = hot_scanner(dataset)?;
    scanner
        .project(&crate::record::packed_projection(dataset, true))
        .map_err(OmniError::storage)?;
    scanner.filter_expr(
        datafusion::prelude::col("object_id")
            .eq(datafusion::prelude::lit(crate::SCHEMA_CONTRACT_OBJECT_ID)),
    );
    scanner.materialization_style(lance::dataset::scanner::MaterializationStyle::AllEarly);
    let mut batches = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;
    let mut found: Option<SchemaContractRow> = None;
    while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
        let batch = expand_from_storage(&batch)?;
        fold_schema_contract_batch(&batch, &mut found)?;
    }
    finish_schema_contract_row(found, expected, dataset.version().version)
}

pub(crate) fn schema_contract_from_batch(
    dataset: &Dataset,
    batch: &RecordBatch,
    expected: Option<&SchemaContractHead>,
) -> Result<SchemaContractRow> {
    require_schema_content(dataset)?;
    let batch = expand_from_storage(batch)?;
    let mut found = None;
    fold_schema_contract_batch(&batch, &mut found)?;
    finish_schema_contract_row(found, expected, dataset.version().version)
}

fn require_schema_content(dataset: &Dataset) -> Result<()> {
    let stamp = read_stamp(dataset);
    if !stamp.is_some_and(crate::migrations::is_served_stamp)
        || crate::record::SCHEMA_CONTENT_COLUMNS
            .iter()
            .any(|name| dataset.schema().field(name).is_none())
    {
        return Err(OmniError::manifest_internal(format!(
            "__manifest version {} carries no schema_contract row: it was written at internal \
             schema v{}, before the contract lived in the manifest",
            dataset.version().version,
            stamp.map_or("?".to_string(), |stamp| stamp.to_string())
        )));
    }
    for name in crate::record::SCHEMA_CONTENT_COLUMNS {
        let field = dataset
            .schema()
            .field(name)
            .expect("content field checked above");
        if field.data_type() != arrow_schema::DataType::LargeUtf8 {
            return Err(OmniError::manifest_internal(format!(
                "manifest column '{name}' is {}, expected LargeUtf8",
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn fold_schema_contract_batch(
    batch: &RecordBatch,
    found: &mut Option<SchemaContractRow>,
) -> Result<()> {
    let object_ids = string_column(batch, "object_id")?;
    let object_types = string_column(batch, "object_type")?;
    let metadata = string_column(batch, "metadata")?;
    let sources = large_string_column(batch, crate::record::SCHEMA_CONTENT_COLUMNS[0])?;
    let irs = large_string_column(batch, crate::record::SCHEMA_CONTENT_COLUMNS[1])?;
    for row in 0..batch.num_rows() {
        if object_ids.value(row) != crate::SCHEMA_CONTRACT_OBJECT_ID {
            continue;
        }
        if object_types.value(row) != crate::OBJECT_TYPE_SCHEMA_CONTRACT {
            return Err(OmniError::manifest_internal(format!(
                "manifest row '{}' has object_type '{}'",
                crate::SCHEMA_CONTRACT_OBJECT_ID,
                object_types.value(row)
            )));
        }
        let head = decode_schema_contract_row(object_ids, metadata, row)?;
        if sources.is_null(row) || irs.is_null(row) {
            return Err(OmniError::manifest_internal(
                "manifest schema_contract row is missing its source or IR text".to_string(),
            ));
        }
        let contract = SchemaContractRow {
            source: sources.value(row).to_string(),
            ir: irs.value(row).to_string(),
            head,
        };
        if found.replace(contract).is_some() {
            return Err(OmniError::manifest_internal(
                "manifest has two schema_contract rows".to_string(),
            ));
        }
    }
    Ok(())
}

fn finish_schema_contract_row(
    found: Option<SchemaContractRow>,
    expected: Option<&SchemaContractHead>,
    version: u64,
) -> Result<SchemaContractRow> {
    let contract = found.ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "__manifest version {} has no schema_contract row",
            version
        ))
    })?;
    if let Some(expected) = expected
        && *expected != contract.head
    {
        return Err(OmniError::manifest_internal(format!(
            "manifest schema_contract row disagrees with the folded state: the row carries IR \
             hash {} (identity v{} of domain {}), the state {} (identity v{} of domain {})",
            contract.head.schema_ir_hash,
            contract.head.schema_identity_version,
            contract.head.schema_identity_domain,
            expected.schema_ir_hash,
            expected.schema_identity_version,
            expected.schema_identity_domain,
        )));
    }
    Ok(contract)
}

fn large_string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a LargeStringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest column '{name}' is not LargeUtf8"))
        })
}

fn require_object_id(
    object_ids: &StringArray,
    row: usize,
    expected: &str,
    object_type: &str,
) -> Result<()> {
    let actual = object_ids.value(row);
    if actual != expected {
        return Err(OmniError::manifest_internal(format!(
            "manifest {object_type} row at index {row} has object_id '{actual}', expected '{expected}'"
        )));
    }
    Ok(())
}
