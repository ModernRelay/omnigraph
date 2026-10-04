//! The reader of the `__manifest` stamps the offline storage upgrade converts
//! from, and the test fixture writer of those stamps. The reader is handed a
//! dataset checked out at one version and never writes. Everything a source
//! stamp decides (which versions it admits, its row shapes, its key grammar)
//! sits behind [`LegacyManifestSource`], so a reader of another source stamp
//! replaces [`Stamp13Source`] alone. The census, the plan and the conversion
//! rows derived from what the reader returns are source-independent.

mod census;
mod layout;
mod record;

pub use census::{
    CensusCounts, CensusError, CensusHead, CensusInput, CensusRef, FindingCode, LegacyCensus,
    LegacyFinding, LegacyPlan, MAX_CENSUS_CELLS, MAX_CENSUS_SNAPSHOT_BYTES, census, census_within,
    conversion_batch, materialize, plan,
};

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::RangeInclusive;
use std::sync::Arc;

use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, UInt64Array};
use datafusion::prelude::{col, lit};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::refs::BranchContents;
use lance::session::Session;

use crate::error::{OmniError, Result};
use crate::history::{
    self, ExtentCache, HistoryRecord, LegacyReader, LegacyWriter, LegacyWriterKind,
};
use crate::metadata::TableVersionMetadata;
use crate::migrations::{
    INTERNAL_MANIFEST_SCHEMA_VERSION, INTERNAL_SCHEMA_VERSION_KEY, UPGRADE_PENDING_KEY,
};
use crate::record::{RECORD_COLUMN, SCHEMA_CONTENT_COLUMNS};
use crate::state::{
    GraphLineageRow, SchemaContractHead, SchemaContractRow, TablePin, string_column,
};
use crate::{
    OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_SCHEMA_CONTRACT, OBJECT_TYPE_TABLE,
    SCHEMA_CONTRACT_OBJECT_ID, TableIdentity, TableRegistration,
};
use census::named;
use layout::{GRAPH_HEAD_OBJECT_ID_PREFIX, manifest_version_from_object_id};
use record::{
    SCHEMA_CONTENT_STAMP, StoredShape, expand_from_storage, flat_projection, stored_shape,
};

/// A registration of a table version, one row per publish that pinned the table.
pub const OBJECT_TYPE_TABLE_VERSION: &str = "table_version";
/// The seal of a dropped table.
pub const OBJECT_TYPE_TABLE_TOMBSTONE: &str = "table_tombstone";
/// The head pointer of one branch, keyed `graph_head:<logical branch>`.
pub const OBJECT_TYPE_GRAPH_HEAD: &str = "graph_head";

/// The stamp this route converts from: a live ref's head carries it.
const SOURCE_STAMP: u32 = SCHEMA_CONTENT_STAMP;
/// The stamps a retired ref's head may carry: retirement metadata arrived at 8.
const RETIRED_HEAD_STAMPS: RangeInclusive<u32> = 8..=SOURCE_STAMP;
/// The stamps a retained version may carry: the oldest a supported route left behind is 6.
const VERSION_STAMPS: RangeInclusive<u32> = 6..=SOURCE_STAMP;
/// What an unstamped bootstrap version admits as: the stamp an absent key means (`guard_stamp`).
const ABSENT_STAMP: u32 = 1;

/// What the upgrade reads a `__manifest` version as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceRole {
    /// The head of a live ref, which the upgrade converts in place.
    LiveHead,
    /// The head of a retired ref, whose own commits the upgrade keeps.
    RetiredHead,
    /// One retained version of any ref, read for its membership and contract.
    /// The fence and the conversion an earlier, completed route left on main
    /// are such versions, though they carry its pending key.
    Version,
}

impl SourceRole {
    fn admits(self, stamp: Option<u32>, version: u64) -> bool {
        match (self, stamp) {
            (Self::LiveHead, Some(stamp)) => stamp == SOURCE_STAMP,
            (Self::RetiredHead, Some(stamp)) => RETIRED_HEAD_STAMPS.contains(&stamp),
            (Self::Version, Some(stamp)) => VERSION_STAMPS.contains(&stamp),
            (Self::Version, None) => matches!(version, 1 | 2),
            (Self::LiveHead | Self::RetiredHead, None) => false,
        }
    }

    fn admitted(self) -> &'static str {
        match self {
            Self::LiveHead => "the head of a live branch must be stamped v13",
            Self::RetiredHead => "the head of a retired branch must be stamped v8 to v13",
            Self::Version => {
                "a retained version must be stamped v6 to v13, or be the unstamped version 1 \
                 or 2 an older init wrote"
            }
        }
    }
}

/// One `graph_commit` row of stamps 4 to 13: the id is the row's `object_id`,
/// the branch its `table_branch`, the version its `table_version`, the rest its
/// `metadata` JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyCommit {
    pub graph_commit_id: String,
    /// The logical name of the branch that wrote the commit, `None` on main.
    pub graph_branch: Option<String>,
    pub graph_manifest_version: u64,
    pub parent_commit_id: Option<String>,
    pub merged_parent_commit_id: Option<String>,
    pub actor_id: Option<String>,
    pub created_at: i64,
}

/// Everything one scan of a ref's head holds: every commit and head row,
/// every registration and tombstone keyed by `(identity, clock)`, the contract
/// row (`None` below stamp 13) and the count of rows scanned. A stamp-13
/// overwrite rewrites every row and never removes a registration, so a head
/// holds every pin and drop written on its lineage before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadScan {
    pub commits: Vec<LegacyCommit>,
    /// Head commit ids keyed by logical branch name (`main` for main).
    pub heads: HashMap<String, String>,
    pub pins: BTreeMap<(TableIdentity, u64), TablePin>,
    /// The Lance version each drop sealed its table at.
    pub tombstones: BTreeMap<(TableIdentity, u64), u64>,
    pub contract: Option<SchemaContractRow>,
    pub rows: u64,
}

/// The table membership, aliases and paths of one version, in identity order,
/// and its contract (`None` below stamp 13 or when the version holds no
/// `schema_contract` row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSchema {
    pub tables: Vec<TableRegistration>,
    pub contract: Option<SchemaContractRow>,
}

/// The boundary between the stamp a source root was written at and everything
/// the upgrade derives from it.
#[async_trait::async_trait]
pub trait LegacyManifestSource: Send + Sync {
    /// The stamp the rows of `dataset` are stored as when it may be read as
    /// `role` (1 for an unstamped bootstrap version), else the refusal naming
    /// its version. A pending conversion admits only as [`SourceRole::Version`].
    fn admits(&self, dataset: &Dataset, role: SourceRole) -> Result<u32>;

    /// One full scan of a ref's head, admitted as a retired head (a live
    /// head's stamp is one of those).
    async fn scan_head(&self, dataset: &Dataset) -> Result<HeadScan>;

    /// One scan of the `table` and `schema_contract` rows of a retained
    /// version. The IR text is read only when the contract head or the source
    /// differs from `previous`'s; an equal pair names the same IR.
    async fn version_schema(
        &self,
        dataset: &Dataset,
        previous: Option<&VersionSchema>,
    ) -> Result<VersionSchema>;
}

/// The source of the 13 → 14 route: packed 8-field rows, the content columns
/// at 13, flat rows below 12, and the stamp-7 key grammar.
#[derive(Debug, Clone, Copy, Default)]
pub struct Stamp13Source;

#[async_trait::async_trait]
impl LegacyManifestSource for Stamp13Source {
    fn admits(&self, dataset: &Dataset, role: SourceRole) -> Result<u32> {
        let version = dataset.version().version;
        let metadata = &dataset.schema().metadata;
        let pending = metadata.get(UPGRADE_PENDING_KEY);
        if pending.is_some() && role != SourceRole::Version {
            return Err(OmniError::manifest(format!(
                "__manifest version {version} carries a pending storage conversion \
                 ({UPGRADE_PENDING_KEY}); it is not a v13 source"
            )));
        }
        let stamp = match metadata.get(INTERNAL_SCHEMA_VERSION_KEY) {
            None => None,
            Some(value) => Some(value.parse::<u32>().map_err(|_| {
                OmniError::manifest(format!(
                    "__manifest version {version} carries the internal-schema stamp '{value}', \
                     which is not a version number"
                ))
            })?),
        };
        if let Some(intent) = pending {
            return earlier_route_stamp(dataset, stamp, intent);
        }
        if !role.admits(stamp, version) {
            let found = stamp.map_or_else(|| "unstamped".to_string(), |stamp| format!("v{stamp}"));
            return Err(OmniError::manifest(format!(
                "__manifest version {version} is {found}: {}",
                role.admitted()
            )));
        }
        let stamp = stamp.unwrap_or(ABSENT_STAMP);
        require_stored_shape(dataset, stamp)?;
        Ok(stamp)
    }

    async fn scan_head(&self, dataset: &Dataset) -> Result<HeadScan> {
        let stamp = self.admits(dataset, SourceRole::RetiredHead)?;
        let shape = stored_shape(Some(stamp));
        let projection = match shape {
            StoredShape::Packed => crate::record::packed_projection(dataset, true),
            StoredShape::Flat => flat_projection(),
        };
        crate::instrumentation::record_manifest_scan();
        let mut scanner = dataset.scan();
        scanner.project(&projection).map_err(OmniError::storage)?;
        let mut batches = scanner
            .try_into_stream()
            .await
            .map_err(OmniError::storage)?;
        let mut fold = HeadFold::new(dataset.version().version);
        while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
            fold.fold(&logical(shape, batch)?)?;
        }
        Ok(fold.finish()?.0)
    }

    async fn version_schema(
        &self,
        dataset: &Dataset,
        previous: Option<&VersionSchema>,
    ) -> Result<VersionSchema> {
        let stamp = self.admits(dataset, SourceRole::Version)?;
        let shape = stored_shape(Some(stamp));
        let mut projection: Vec<String> = match shape {
            StoredShape::Packed => vec!["object_id", "object_type", RECORD_COLUMN],
            StoredShape::Flat => vec![
                "object_id",
                "object_type",
                "location",
                "metadata",
                "table_key",
                "stable_table_id",
                "table_incarnation_id",
            ],
        }
        .into_iter()
        .map(str::to_string)
        .collect();
        if stamp == SCHEMA_CONTENT_STAMP {
            projection.push(SCHEMA_CONTENT_COLUMNS[0].to_string());
        }
        crate::instrumentation::record_manifest_scan();
        let mut scanner = dataset.scan();
        scanner.project(&projection).map_err(OmniError::storage)?;
        scanner.filter_expr(col("object_type").in_list(
            vec![lit(OBJECT_TYPE_TABLE), lit(OBJECT_TYPE_SCHEMA_CONTRACT)],
            false,
        ));
        scanner.materialization_style(lance::dataset::scanner::MaterializationStyle::AllEarly);
        let mut batches = scanner
            .try_into_stream()
            .await
            .map_err(OmniError::storage)?;
        let mut tables = BTreeMap::new();
        let mut found: Option<(SchemaContractHead, String)> = None;
        while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
            let batch = logical(shape, batch)?;
            let table_columns = TableColumns::of(&batch)?;
            let object_ids = table_columns.object_ids;
            let object_types = string_column(&batch, "object_type")?;
            let metadata = metadata_column(&batch)?;
            let sources = batch
                .column_by_name(SCHEMA_CONTENT_COLUMNS[0])
                .map(|_| large_string_column(&batch, SCHEMA_CONTENT_COLUMNS[0]))
                .transpose()?;
            for row in 0..batch.num_rows() {
                require_contract_type(object_ids, object_types, row)?;
                match object_types.value(row) {
                    OBJECT_TYPE_TABLE => insert_registration(
                        &mut tables,
                        table_columns.registration(row, dataset.version().version)?,
                    )?,
                    OBJECT_TYPE_SCHEMA_CONTRACT => {
                        let head = decode_schema_contract_head(object_ids, metadata, row)?;
                        let source = sources
                            .filter(|sources| !sources.is_null(row))
                            .ok_or_else(|| {
                                OmniError::manifest_internal(format!(
                                    "__manifest version {} has a schema_contract row without its \
                                     source text",
                                    dataset.version().version
                                ))
                            })?
                            .value(row)
                            .to_string();
                        if found.replace((head, source)).is_some() {
                            return Err(OmniError::manifest_internal(
                                "manifest has two schema_contract rows".to_string(),
                            ));
                        }
                    }
                    other => {
                        return Err(OmniError::manifest_internal(format!(
                            "the table and contract scan returned a '{other}' row"
                        )));
                    }
                }
            }
        }
        let contract = match found {
            None => None,
            Some((head, source)) => {
                let ir = match previous
                    .and_then(|previous| previous.contract.as_ref())
                    .filter(|previous| previous.head == head && previous.source == source)
                {
                    Some(previous) => previous.ir.clone(),
                    None => read_schema_ir(dataset).await?,
                };
                Some(SchemaContractRow { source, ir, head })
            }
        };
        Ok(VersionSchema {
            tables: tables.into_values().collect(),
            contract,
        })
    }
}

/// The current head of a lineage: the commit with the greatest
/// `(graph_manifest_version, created_at, graph_commit_id)`, the rule a stamp-13
/// publish chose its parent by. `None` only for a lineage with no commit.
pub fn head_lineage_row(commits: &[LegacyCommit]) -> Option<&LegacyCommit> {
    head_among(commits.iter())
}

/// [`head_lineage_row`] over any subset of a lineage's commits.
fn head_among<'a>(commits: impl Iterator<Item = &'a LegacyCommit>) -> Option<&'a LegacyCommit> {
    commits.max_by(|a, b| {
        a.graph_manifest_version
            .cmp(&b.graph_manifest_version)
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.graph_commit_id.cmp(&b.graph_commit_id))
    })
}

/// The versions of main above its legacy head that carry the pending key of
/// the upgrade that wrote the legacy history: its fence and its conversion.
const FENCED_VERSIONS: u64 = 2;

/// What the legacy history holds for one version of a ref.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LegacyAt {
    /// The version holds this commit.
    Exact(HistoryRecord),
    /// The version holds no commit; this is the greatest commit at or below it.
    Nearest(HistoryRecord),
    /// A version below the genesis commit of main, which holds no table.
    PreGenesis,
}

impl LegacyAt {
    /// The record a read of `version` of the ref `native` is served from.
    pub(crate) fn served(self, native: Option<&str>, version: u64) -> Result<HistoryRecord> {
        match self {
            Self::Exact(record) | Self::Nearest(record) => Ok(record),
            Self::PreGenesis => Err(pre_genesis(native, version)),
        }
    }
}

/// The refusal of a read of a version below the genesis commit.
pub(crate) fn pre_genesis(native: Option<&str>, version: u64) -> OmniError {
    OmniError::manifest_not_found(format!(
        "version {version} of {} precedes the genesis commit",
        named(native)
    ))
}

/// What the head version of a retired ref yields for its commit graph.
pub(crate) enum RetiredHead {
    /// Rows this build reads, or a root with no legacy history: read the version.
    Rows,
    /// The head commit the writer shards record for the ref.
    Commit(Box<GraphLineageRow>),
    /// A legacy ref with no commit of its own: its lineage is its parent's.
    Commitless,
}

/// `Some` when `dataset`'s version is read from the legacy history of a root
/// that has one: it is unstamped, stamped below this build's stamp, or main's
/// conversion under the pending key. The value says whether it carries the key.
fn legacy_version(dataset: &Dataset) -> Option<bool> {
    let metadata = &dataset.schema().metadata;
    let fenced = metadata.contains_key(UPGRADE_PENDING_KEY);
    let legacy = match metadata
        .get(INTERNAL_SCHEMA_VERSION_KEY)
        .map(|stamp| stamp.parse::<u32>())
    {
        None => true,
        Some(Ok(stamp)) => {
            stamp < INTERNAL_MANIFEST_SCHEMA_VERSION
                || (stamp == INTERNAL_MANIFEST_SCHEMA_VERSION && fenced)
        }
        Some(Err(_)) => false,
    };
    legacy.then_some(fenced)
}

/// The legacy record `dataset`'s version is served from; `None` when the
/// version holds rows this build reads or the root has no legacy history, so
/// the caller reads the version and the stamp guard answers for it.
pub(crate) async fn at_version(
    root_uri: &str,
    session: &Arc<Session>,
    cache: &ExtentCache,
    dataset: &Dataset,
) -> Result<Option<LegacyAt>> {
    let Some(fenced) = legacy_version(dataset) else {
        return Ok(None);
    };
    if history::legacy_directory(root_uri, session, Some(cache))
        .await?
        .is_none()
    {
        return Ok(None);
    }
    let native = dataset.manifest().branch.as_deref();
    let version = dataset.version().version;
    record_at(root_uri, session, Some(cache), native, version, fenced)
        .await
        .map(Some)
}

/// The commit the legacy history holds at `version` of the ref `native`, or
/// the greatest one at or below it, walking to the ref it forked from. Main's
/// fence and conversion versions are admitted only when `fenced`.
pub(crate) async fn record_at(
    root_uri: &str,
    session: &Arc<Session>,
    cache: Option<&ExtentCache>,
    native: Option<&str>,
    version: u64,
    fenced: bool,
) -> Result<LegacyAt> {
    let Some(mut reader) = LegacyReader::open(root_uri, session, cache).await? else {
        return Err(OmniError::manifest_not_found(format!(
            "version {version} of {} is stored in an earlier format and the root holds no \
             legacy history under __history/legacy/",
            named(native)
        )));
    };
    let mut refs: Option<HashMap<String, BranchContents>> = None;
    let mut walked = BTreeSet::new();
    let (mut at, mut upto) = (native.map(str::to_string), version);
    loop {
        let requested = walked.is_empty();
        if !walked.insert(at.clone()) {
            return Err(OmniError::manifest_internal(format!(
                "the refs {} forked from lead back to {}",
                named(native),
                named(at.as_deref())
            )));
        }
        let found = |record: HistoryRecord| match requested
            && record.commit.graph_manifest_version == version
        {
            true => LegacyAt::Exact(record),
            false => LegacyAt::Nearest(record),
        };
        let writers = reader.writers(at.as_deref()).await?;
        if let Some(writer) = writers
            .iter()
            .find(|writer| writer.kind != LegacyWriterKind::Orphaned)
        {
            let admitted = upto <= writer.head_version
                || (writer.kind == LegacyWriterKind::Main
                    && requested
                    && fenced
                    && upto - writer.head_version <= FENCED_VERSIONS);
            if !admitted {
                return Err(OmniError::manifest_not_found(format!(
                    "version {upto} of {} is above version {}, the last one the legacy history \
                     records for it, and is not a fence or a conversion of the storage upgrade",
                    named(at.as_deref()),
                    writer.head_version
                )));
            }
            if upto > writer.parent_version
                && let Some(record) = own_at(&mut reader, writer, upto).await?
            {
                return Ok(found(record));
            }
            if writer.kind == LegacyWriterKind::Main {
                return Ok(LegacyAt::PreGenesis);
            }
            upto = upto.min(writer.parent_version);
            at = writer.parent.clone();
            continue;
        }
        let Some(name) = at.clone() else {
            return Err(OmniError::manifest_internal(
                "the legacy history lists no writer for main",
            ));
        };
        if refs.is_none() {
            refs = Some(manifest_refs(root_uri, session).await?);
        }
        if let Some(contents) = refs.as_ref().and_then(|refs| refs.get(&name)) {
            upto = upto.min(contents.parent_version);
            at = contents.parent_branch.clone();
            continue;
        }
        let Some(orphan) = writers.first() else {
            return Err(OmniError::manifest_not_found(format!(
                "'{name}' is neither a ref of __manifest nor a writer of the legacy history; \
                 version {upto} of it cannot be read"
            )));
        };
        return match own_at(&mut reader, orphan, upto).await? {
            Some(record) => Ok(found(record)),
            None => Err(OmniError::manifest_not_found(format!(
                "version {upto} of the deleted branch '{name}' is below the first graph commit \
                 the legacy history kept of it, and the ref it forked from is unknown"
            ))),
        };
    }
}

/// The record the live ref `native`, pinned at `version`, is converted to,
/// read from the legacy history of a root whose census is no longer run: its
/// greatest own commit, else that of the ref it forked from.
#[doc(hidden)]
pub async fn converted_head(
    root_uri: &str,
    session: &Arc<Session>,
    native: Option<&str>,
    version: u64,
) -> Result<HistoryRecord> {
    record_at(root_uri, session, None, native, version, false)
        .await?
        .served(native, version)
}

/// The head a retired ref's commit graph starts at, `dataset` being the head
/// version of that ref.
pub(crate) async fn retired_head(
    root_uri: &str,
    session: &Arc<Session>,
    cache: &ExtentCache,
    dataset: &Dataset,
) -> Result<RetiredHead> {
    if legacy_version(dataset).is_none() {
        return Ok(RetiredHead::Rows);
    }
    let Some(mut reader) = LegacyReader::open(root_uri, session, Some(cache)).await? else {
        return Ok(RetiredHead::Rows);
    };
    let native = dataset.manifest().branch.as_deref();
    let writers = reader.writers(native).await?;
    let Some(writer) = writers
        .iter()
        .find(|writer| writer.kind != LegacyWriterKind::Orphaned)
    else {
        return Ok(RetiredHead::Commitless);
    };
    let id = writer.head.to_string();
    let head = history::read_commit_in(root_uri, session, cache, &id)
        .await?
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "the legacy history lists '{id}' as the head of {} and holds no such graph commit",
                named(native)
            ))
        })?;
    Ok(RetiredHead::Commit(Box::new(head)))
}

/// The greatest own commit of `writer` at or below `version`.
async fn own_at(
    reader: &mut LegacyReader<'_>,
    writer: &LegacyWriter,
    version: u64,
) -> Result<Option<HistoryRecord>> {
    let directory = Arc::clone(reader.directory());
    let (first, count) = (writer.first_file as usize, writer.files as usize);
    let files = directory.files.get(first..first + count).ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "a legacy writer names data files {first} to {} and the directory lists {}",
            first + count,
            directory.files.len()
        ))
    })?;
    let Some(index) = files
        .partition_point(|file| file.first_version <= version)
        .checked_sub(1)
    else {
        return Ok(None);
    };
    let records = reader.file(writer.first_file + index as u32).await?;
    Ok(records
        .iter()
        .rev()
        .find(|record| record.commit.graph_manifest_version <= version)
        .cloned())
}

/// Every ref of `__manifest`, live or retired, by native name.
async fn manifest_refs(
    root_uri: &str,
    session: &Arc<Session>,
) -> Result<HashMap<String, BranchContents>> {
    let main =
        crate::layout::open_manifest_dataset_native_with_session(root_uri, None, session).await?;
    let mut refs = crate::branch_control::list_all_branch_contents(&main).await?;
    refs.extend(crate::retention::retired_manifest_branches(&main).await?);
    Ok(refs)
}

/// The upgrade routes a stamp-13 root may have completed, as the
/// `(protocol, source_format, target_format)` their intents carry.
const EARLIER_ROUTES: [(u32, u32, u32); 7] = [
    (1, 6, 7),
    (2, 7, 8),
    (3, 8, 10),
    (3, 9, 10),
    (4, 10, 11),
    (5, 11, 13),
    (5, 12, 13),
];

/// What a retained version's pending intent names of its route.
#[derive(serde::Deserialize)]
struct EarlierRoute {
    protocol: u32,
    source_format: u32,
    target_format: u32,
}

/// The stamp the rows of a retained main version under an earlier route's
/// pending key are stored as: the target's for its conversion, the source's
/// for its fence, which changed the metadata over the rows below it.
fn earlier_route_stamp(dataset: &Dataset, stamp: Option<u32>, intent: &str) -> Result<u32> {
    let version = dataset.version().version;
    let refused = |why: String| {
        OmniError::manifest(format!(
            "__manifest version {version} carries a pending storage conversion \
             ({UPGRADE_PENDING_KEY}) that is not the fence or the conversion of a completed \
             earlier upgrade: {why}"
        ))
    };
    let route: EarlierRoute = serde_json::from_str(intent)
        .map_err(|error| refused(format!("its intent is unreadable: {error}")))?;
    let EarlierRoute {
        protocol,
        source_format,
        target_format,
    } = route;
    if !EARLIER_ROUTES.contains(&(protocol, source_format, target_format)) {
        return Err(refused(format!(
            "its intent names protocol {protocol} from v{source_format} to v{target_format}, \
             which no released upgrade ran"
        )));
    }
    if stamp != Some(target_format) {
        return Err(refused(format!(
            "its intent targets v{target_format} and the version is stamped {stamp:?}"
        )));
    }
    if let Some(native) = &dataset.manifest().branch {
        return Err(refused(format!(
            "it is a version of ref '{native}', and an upgrade fences main alone"
        )));
    }
    if require_stored_shape(dataset, target_format).is_ok() {
        return Ok(target_format);
    }
    require_stored_shape(dataset, source_format)
        .map(|()| source_format)
        .map_err(|_| {
            refused(format!(
                "its rows are stored neither as v{target_format} nor as v{source_format} stores \
                 them"
            ))
        })
}

fn require_stored_shape(dataset: &Dataset, stamp: u32) -> Result<()> {
    let schema = dataset.schema();
    let record = schema.field(RECORD_COLUMN).map(|field| field.data_type());
    let packed = match &record {
        Some(data_type) => record::is_record_type(data_type)?,
        None => false,
    };
    let flat = record.is_none()
        && ["stable_table_id", "table_incarnation_id"]
            .iter()
            .all(|name| schema.field(name).is_some());
    let content = SCHEMA_CONTENT_COLUMNS.iter().all(|name| {
        schema
            .field(name)
            .is_some_and(|field| field.data_type() == arrow_schema::DataType::LargeUtf8)
    });
    let no_content = SCHEMA_CONTENT_COLUMNS
        .iter()
        .all(|name| schema.field(name).is_none());
    let (matches, expected) = match (stored_shape(Some(stamp)), stamp == SCHEMA_CONTENT_STAMP) {
        (StoredShape::Packed, true) => (
            packed && content,
            "the packed 8-field record and both schema content columns",
        ),
        (StoredShape::Packed, false) => (
            packed && no_content,
            "the packed 8-field record and no schema content column",
        ),
        (StoredShape::Flat, _) => (flat && no_content, "the flat columns"),
    };
    if !matches {
        return Err(OmniError::manifest(format!(
            "__manifest version {} is stamped v{stamp} but its rows are not stored as a v{stamp} \
             version stores them: expected {expected}",
            dataset.version().version
        )));
    }
    Ok(())
}

/// The logical rows of a stored batch of either shape.
fn logical(shape: StoredShape, batch: RecordBatch) -> Result<RecordBatch> {
    match shape {
        StoredShape::Packed => expand_from_storage(&batch),
        StoredShape::Flat => Ok(batch),
    }
}

/// The IR text of the `schema_contract` row, read alone.
async fn read_schema_ir(dataset: &Dataset) -> Result<String> {
    crate::instrumentation::record_manifest_scan();
    let mut scanner = dataset.scan();
    scanner
        .project(&["object_id", SCHEMA_CONTENT_COLUMNS[1]])
        .map_err(OmniError::storage)?;
    scanner.filter_expr(col("object_id").eq(lit(SCHEMA_CONTRACT_OBJECT_ID)));
    let mut batches = scanner
        .try_into_stream()
        .await
        .map_err(OmniError::storage)?;
    let mut found = None;
    while let Some(batch) = batches.try_next().await.map_err(OmniError::storage)? {
        let irs = large_string_column(&batch, SCHEMA_CONTENT_COLUMNS[1])?;
        for row in 0..batch.num_rows() {
            if irs.is_null(row) {
                return Err(OmniError::manifest_internal(
                    "manifest schema_contract row is missing its IR text".to_string(),
                ));
            }
            if found.replace(irs.value(row).to_string()).is_some() {
                return Err(OmniError::manifest_internal(
                    "manifest has two schema_contract rows".to_string(),
                ));
            }
        }
    }
    found.ok_or_else(|| {
        OmniError::manifest_internal(format!(
            "__manifest version {} lost its schema_contract row between two reads",
            dataset.version().version
        ))
    })
}

/// Immutable commit fields stored as the `graph_commit` row's `metadata` JSON.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct GraphCommitMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merged_parent_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_id: Option<String>,
    created_at: i64,
}

/// The `graph_head` row's `metadata` JSON.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct GraphHeadMetadata {
    head_commit_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_commit_id: Option<String>,
}

/// The reduction of a head's rows, shared by [`Stamp13Source::scan_head`] and
/// the fixture writer so both decode a row one way.
struct HeadFold {
    version: u64,
    registrations: BTreeMap<TableIdentity, TableRegistration>,
    pins: BTreeMap<(TableIdentity, u64), TablePin>,
    tombstones: BTreeMap<(TableIdentity, u64), (u64, String)>,
    commits: Vec<LegacyCommit>,
    heads: HashMap<String, String>,
    contract: Option<SchemaContractRow>,
    rows: u64,
}

impl HeadFold {
    fn new(version: u64) -> Self {
        Self {
            version,
            registrations: BTreeMap::new(),
            pins: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            commits: Vec::new(),
            heads: HashMap::new(),
            contract: None,
            rows: 0,
        }
    }

    fn fold(&mut self, batch: &RecordBatch) -> Result<()> {
        let table_columns = TableColumns::of(batch)?;
        let object_ids = table_columns.object_ids;
        let object_types = string_column(batch, "object_type")?;
        let metadata = metadata_column(batch)?;
        let table_keys = table_columns.table_keys;
        let stable_table_ids = table_columns.stable_table_ids;
        let table_incarnation_ids = table_columns.table_incarnation_ids;
        let versions = u64_column(batch, "table_version")?;
        let branches = string_column(batch, "table_branch")?;
        let row_counts = u64_column(batch, "row_count")?;
        let content = match (
            batch.column_by_name(SCHEMA_CONTENT_COLUMNS[0]),
            batch.column_by_name(SCHEMA_CONTENT_COLUMNS[1]),
        ) {
            (Some(_), Some(_)) => Some((
                large_string_column(batch, SCHEMA_CONTENT_COLUMNS[0])?,
                large_string_column(batch, SCHEMA_CONTENT_COLUMNS[1])?,
            )),
            _ => None,
        };
        self.rows = self
            .rows
            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                OmniError::manifest_internal("manifest row count overflows u64".to_string())
            })?)
            .ok_or_else(|| {
                OmniError::manifest_internal("manifest row count overflows u64".to_string())
            })?;
        for row in 0..batch.num_rows() {
            require_contract_type(object_ids, object_types, row)?;
            let object_type = object_types.value(row);
            match object_type {
                OBJECT_TYPE_TABLE => {
                    insert_registration(
                        &mut self.registrations,
                        table_columns.registration(row, self.version)?,
                    )?;
                }
                OBJECT_TYPE_TABLE_VERSION | OBJECT_TYPE_TABLE_TOMBSTONE => {
                    let identity = required_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        object_type,
                    )?;
                    let clock = manifest_version_from_object_id(
                        object_ids.value(row),
                        identity,
                        object_type,
                    )?;
                    if clock > self.version {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest row for {} carries manifest version {clock} above the \
                             scanned dataset version {}",
                            table_keys.value(row),
                            self.version
                        )));
                    }
                    if self.pins.contains_key(&(identity, clock))
                        || self.tombstones.contains_key(&(identity, clock))
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest has two rows for identity {identity} at manifest version \
                             {clock}"
                        )));
                    }
                    let table_version = required_u64(versions, row, "table_version")?;
                    if object_type == OBJECT_TYPE_TABLE_TOMBSTONE {
                        self.tombstones.insert(
                            (identity, clock),
                            (table_version, table_keys.value(row).to_string()),
                        );
                        continue;
                    }
                    if metadata.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table_version row missing metadata for {}",
                            table_keys.value(row)
                        )));
                    }
                    self.pins.insert(
                        (identity, clock),
                        TablePin {
                            table_version,
                            table_branch: (!branches.is_null(row))
                                .then(|| branches.value(row).to_string()),
                            row_count: required_u64(row_counts, row, "row_count")?,
                            metadata: TableVersionMetadata::from_json_str(metadata.value(row))?,
                            manifest_version: clock,
                        },
                    );
                }
                OBJECT_TYPE_GRAPH_COMMIT => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        object_type,
                    )?;
                    self.commits.push(decode_graph_commit_row(
                        object_ids, metadata, versions, branches, row,
                    )?);
                }
                OBJECT_TYPE_GRAPH_HEAD => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        object_type,
                    )?;
                    let (branch, head) = decode_graph_head_row(object_ids, metadata, row)?;
                    if self.heads.insert(branch, head).is_some() {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest has two '{}' rows",
                            object_ids.value(row)
                        )));
                    }
                }
                OBJECT_TYPE_SCHEMA_CONTRACT => {
                    require_null_table_identity(
                        stable_table_ids,
                        table_incarnation_ids,
                        row,
                        object_type,
                    )?;
                    let head = decode_schema_contract_head(object_ids, metadata, row)?;
                    let (sources, irs) = content
                        .filter(|(sources, irs)| !sources.is_null(row) && !irs.is_null(row))
                        .ok_or_else(|| {
                            OmniError::manifest_internal(
                                "manifest schema_contract row is missing its source or IR text"
                                    .to_string(),
                            )
                        })?;
                    let contract = SchemaContractRow {
                        source: sources.value(row).to_string(),
                        ir: irs.value(row).to_string(),
                        head,
                    };
                    if self.contract.replace(contract).is_some() {
                        return Err(OmniError::manifest_internal(
                            "manifest has two schema_contract rows".to_string(),
                        ));
                    }
                }
                other => {
                    return Err(OmniError::manifest_internal(format!(
                        "manifest row '{}' has object_type '{other}', which no v13 row carries",
                        object_ids.value(row)
                    )));
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<(HeadScan, BTreeMap<TableIdentity, TableRegistration>)> {
        for ((identity, _), (_, alias)) in &self.tombstones {
            let registration = self.registrations.get(identity).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "manifest tombstone for identity {identity} has no table registration"
                ))
            })?;
            if registration.table_key != *alias {
                return Err(OmniError::manifest_internal(format!(
                    "manifest tombstone identity {identity} has diagnostic alias '{alias}', \
                     current binding is '{}'",
                    registration.table_key
                )));
            }
        }
        Ok((
            HeadScan {
                commits: self.commits,
                heads: self.heads,
                pins: self.pins,
                tombstones: self
                    .tombstones
                    .into_iter()
                    .map(|(key, (sealed, _))| (key, sealed))
                    .collect(),
                contract: self.contract,
                rows: self.rows,
            },
            self.registrations,
        ))
    }
}

fn insert_registration(
    registrations: &mut BTreeMap<TableIdentity, TableRegistration>,
    registration: TableRegistration,
) -> Result<()> {
    let identity = registration.identity;
    if let Some(existing) = registrations.insert(identity, registration.clone()) {
        if existing != registration {
            return Err(OmniError::manifest_internal(format!(
                "manifest has conflicting table rows for identity {identity}"
            )));
        }
    }
    Ok(())
}

/// The columns a `table` row is decoded from, looked up once per batch.
struct TableColumns<'a> {
    object_ids: &'a StringArray,
    locations: &'a StringArray,
    table_keys: &'a StringArray,
    stable_table_ids: &'a UInt64Array,
    table_incarnation_ids: &'a UInt64Array,
}

impl<'a> TableColumns<'a> {
    fn of(batch: &'a RecordBatch) -> Result<Self> {
        Ok(Self {
            object_ids: string_column(batch, "object_id")?,
            locations: string_column(batch, "location")?,
            table_keys: string_column(batch, "table_key")?,
            stable_table_ids: u64_column(batch, "stable_table_id")?,
            table_incarnation_ids: u64_column(batch, "table_incarnation_id")?,
        })
    }

    /// The registration of the `table` row at `row`, its key and canonical path checked.
    fn registration(&self, row: usize, version: u64) -> Result<TableRegistration> {
        let identity = required_table_identity(
            self.stable_table_ids,
            self.table_incarnation_ids,
            row,
            OBJECT_TYPE_TABLE,
        )?;
        require_object_id(
            self.object_ids,
            row,
            &crate::layout::table_object_id(identity),
            OBJECT_TYPE_TABLE,
        )?;
        let table_key = self.table_keys.value(row).to_string();
        if self.locations.is_null(row) {
            return Err(OmniError::manifest_internal(format!(
                "manifest table row of version {version} is missing the location of {table_key}"
            )));
        }
        let table_path = self.locations.value(row).to_string();
        let canonical_path = crate::table_path_for_identity(&table_key, identity)?;
        if table_path != canonical_path {
            return Err(OmniError::manifest_internal(format!(
                "manifest table row for identity {identity} has path '{table_path}', expected \
                 '{canonical_path}'"
            )));
        }
        Ok(TableRegistration {
            identity,
            table_key,
            table_path,
        })
    }
}

fn require_contract_type(
    object_ids: &StringArray,
    object_types: &StringArray,
    row: usize,
) -> Result<()> {
    if object_ids.value(row) == SCHEMA_CONTRACT_OBJECT_ID
        && object_types.value(row) != OBJECT_TYPE_SCHEMA_CONTRACT
    {
        return Err(OmniError::manifest_internal(format!(
            "manifest row '{SCHEMA_CONTRACT_OBJECT_ID}' has object_type '{}'",
            object_types.value(row)
        )));
    }
    Ok(())
}

fn decode_schema_contract_head(
    object_ids: &StringArray,
    metadata: &StringArray,
    row: usize,
) -> Result<SchemaContractHead> {
    require_object_id(
        object_ids,
        row,
        SCHEMA_CONTRACT_OBJECT_ID,
        OBJECT_TYPE_SCHEMA_CONTRACT,
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

fn decode_graph_commit_row(
    object_ids: &StringArray,
    metadata: &StringArray,
    versions: &UInt64Array,
    branches: &StringArray,
    row: usize,
) -> Result<LegacyCommit> {
    if metadata.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest graph_commit row missing metadata for {}",
            object_ids.value(row)
        )));
    }
    let commit: GraphCommitMetadata = serde_json::from_str(metadata.value(row)).map_err(|e| {
        OmniError::manifest_internal(format!("failed to decode graph_commit metadata: {e}"))
    })?;
    Ok(LegacyCommit {
        graph_commit_id: object_ids.value(row).to_string(),
        graph_branch: (!branches.is_null(row)).then(|| branches.value(row).to_string()),
        graph_manifest_version: required_u64(versions, row, "table_version")?,
        parent_commit_id: commit.parent_commit_id,
        merged_parent_commit_id: commit.merged_parent_commit_id,
        actor_id: commit.actor_id,
        created_at: commit.created_at,
    })
}

/// The logical branch name and head commit id of a `graph_head` row.
fn decode_graph_head_row(
    object_ids: &StringArray,
    metadata: &StringArray,
    row: usize,
) -> Result<(String, String)> {
    if metadata.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest graph_head row missing metadata for {}",
            object_ids.value(row)
        )));
    }
    let head: GraphHeadMetadata = serde_json::from_str(metadata.value(row)).map_err(|e| {
        OmniError::manifest_internal(format!("failed to decode graph_head metadata: {e}"))
    })?;
    let branch = object_ids
        .value(row)
        .strip_prefix(GRAPH_HEAD_OBJECT_ID_PREFIX)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "invalid graph_head object id {}",
                object_ids.value(row)
            ))
        })?;
    Ok((branch.to_string(), head.head_commit_id))
}

fn metadata_column(batch: &RecordBatch) -> Result<&StringArray> {
    string_column(batch, "metadata")
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

fn u64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest batch missing '{name}' column"))
        })?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("manifest column '{name}' is not UInt64"))
        })
}

fn required_u64(column: &UInt64Array, row: usize, name: &str) -> Result<u64> {
    if column.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest column '{name}' is null at row {row}"
        )));
    }
    Ok(column.value(row))
}

fn required_table_identity(
    stable_table_ids: &UInt64Array,
    table_incarnation_ids: &UInt64Array,
    row: usize,
    object_type: &str,
) -> Result<TableIdentity> {
    let stable_table_id = required_u64(stable_table_ids, row, "stable_table_id")?;
    let table_incarnation_id = required_u64(table_incarnation_ids, row, "table_incarnation_id")?;
    TableIdentity::new(stable_table_id, table_incarnation_id).map_err(|error| {
        OmniError::manifest_internal(format!(
            "manifest {object_type} row at index {row} has invalid table identity: {error}"
        ))
    })
}

fn require_null_table_identity(
    stable_table_ids: &UInt64Array,
    table_incarnation_ids: &UInt64Array,
    row: usize,
    object_type: &str,
) -> Result<()> {
    if !stable_table_ids.is_null(row) || !table_incarnation_ids.is_null(row) {
        return Err(OmniError::manifest_internal(format!(
            "manifest {object_type} row at index {row} must not carry table identity"
        )));
    }
    Ok(())
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

/// The fixture writer of stamp-13 `__manifest` histories (and the older
/// stamps a retained version may carry) the upgrade is tested against.
#[cfg(any(test, feature = "test-util"))]
pub mod write {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow_array::{BooleanArray, LargeStringArray, RecordBatch, StringArray, UInt64Array};
    use datafusion::arrow::compute::filter_record_batch;
    use futures::TryStreamExt;
    use lance::Dataset;

    use super::layout::{graph_head_object_id, tombstone_object_id, version_object_id};
    use super::record::{
        SCHEMA_CONTENT_STAMP, StoredShape, compact_to_storage, expand_from_storage,
        flat_manifest_schema, flat_projection, flat_to_storage, manifest_schema,
        manifest_storage_schema, stored_shape,
    };
    use super::{
        GraphCommitMetadata, GraphHeadMetadata, HeadFold, OBJECT_TYPE_GRAPH_HEAD,
        OBJECT_TYPE_TABLE_TOMBSTONE, OBJECT_TYPE_TABLE_VERSION, head_lineage_row,
    };
    use crate::error::{OmniError, Result};
    use crate::metadata::TableVersionMetadata;
    use crate::migrations::{INTERNAL_SCHEMA_VERSION_KEY, UPGRADE_PENDING_KEY, read_stamp};
    use crate::state::{SchemaContractRow, string_column};
    use crate::{
        OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_SCHEMA_CONTRACT, OBJECT_TYPE_TABLE,
        SCHEMA_CONTRACT_OBJECT_ID, TableIdentity, TableRegistration,
    };

    /// A pin one scripted publish writes, keyed by that publish's version.
    #[derive(Debug, Clone)]
    pub struct LegacyPin {
        pub identity: TableIdentity,
        pub table_version: u64,
        pub table_branch: Option<String>,
        pub row_count: u64,
        pub metadata: TableVersionMetadata,
    }

    /// The commit one scripted publish records. Its parent is the head of the
    /// rows the publish rewrites and its branch the ref's logical name, as a
    /// stamp-13 publish chose them.
    #[derive(Debug, Clone)]
    pub struct LegacyCommitIntent {
        pub graph_commit_id: String,
        pub merged_parent_commit_id: Option<String>,
        pub actor_id: Option<String>,
        pub created_at: i64,
    }

    /// One scripted stamp-13 publish: the `table` rows it writes (a
    /// registration or a rename), the contract it replaces, its pins and
    /// drops (each an identity and the Lance version it seals the table at),
    /// and its commit. A publish without a commit writes a version that holds
    /// none, as a conversion or maintenance version does.
    #[derive(Debug, Clone, Default)]
    pub struct LegacyPublish {
        pub tables: Vec<TableRegistration>,
        pub contract: Option<SchemaContractRow>,
        pub pins: Vec<LegacyPin>,
        pub drops: Vec<(TableIdentity, u64)>,
        pub commit: Option<LegacyCommitIntent>,
    }

    /// A `__manifest` history a stamp-13 engine would have written. Every
    /// publish overwrites the ref's whole row set, the rows it keeps first and
    /// the rows it writes after, replacing only the ids it writes again
    /// (`graph_head:*`, `schema_contract` and re-emitted `table:*`). A fork is
    /// a Lance ref of `__manifest` at its source's head; a retirement is the
    /// production `retire_branch_recoverably`.
    pub struct Stamp13History {
        root: String,
        heads: HashMap<Option<String>, Dataset>,
    }

    impl Stamp13History {
        /// Create the history at `root` with `genesis` as main's version 1.
        pub async fn create(root: &str, genesis: LegacyPublish) -> Result<Self> {
            let rows = pending_rows(&genesis, &[], &HashMap::new(), None, 1)?;
            let batch = compact_to_storage(&rows, &stamp_13_schema(HashMap::new())?)?;
            let dataset =
                crate::commit::create_for_test(&crate::layout::manifest_uri(root), batch).await?;
            Ok(Self {
                root: root.to_string(),
                heads: HashMap::from([(None, dataset)]),
            })
        }

        pub fn root(&self) -> &str {
            &self.root
        }

        /// The head of the ref `native` (`None` for main) as last written.
        pub fn head(&self, native: Option<&str>) -> Result<&Dataset> {
            self.heads.get(&native.map(str::to_string)).ok_or_else(|| {
                OmniError::manifest_not_found(format!("the fixture holds no live ref {native:?}"))
            })
        }

        /// Write `publish` on `native` as its next version, stamped 13, and
        /// return that version.
        pub async fn publish(
            &mut self,
            native: Option<&str>,
            publish: LegacyPublish,
        ) -> Result<u64> {
            let dataset = self.head(native)?.clone();
            let version = dataset.version().version + 1;
            let live = logical_rows(&dataset).await?;
            let mut fold = HeadFold::new(dataset.version().version);
            for batch in &live {
                fold.fold(batch)?;
            }
            let (scan, registrations) = fold.finish()?;
            let aliases = registrations
                .into_values()
                .map(|registration| (registration.identity, registration.table_key))
                .collect();
            let logical_branch = native.map(crate::branch_names::logical_branch_name);
            let pending = pending_rows(&publish, &scan.commits, &aliases, logical_branch, version)?;
            let replaced: HashSet<String> = string_column(&pending, "object_id")?
                .iter()
                .flatten()
                .map(str::to_string)
                .collect();
            let mut metadata = dataset.schema().metadata.clone();
            metadata.insert(
                INTERNAL_SCHEMA_VERSION_KEY.to_string(),
                SCHEMA_CONTENT_STAMP.to_string(),
            );
            let schema = stamp_13_schema(metadata)?;
            let mut batches = Vec::with_capacity(live.len() + 1);
            for batch in &live {
                let keep = BooleanArray::from_iter(
                    string_column(batch, "object_id")?
                        .iter()
                        .map(|id| Some(!id.is_some_and(|id| replaced.contains(id)))),
                );
                let kept = filter_record_batch(batch, &keep).map_err(OmniError::arrow_internal)?;
                if kept.num_rows() > 0 {
                    batches.push(compact_to_storage(&kept, &schema)?);
                }
            }
            batches.push(compact_to_storage(&pending, &schema)?);
            self.overwrite_head(native, dataset, batches).await
        }

        /// Fork the ref `native` from the head of `source` and return the
        /// version it forks at.
        pub async fn fork(&mut self, source: Option<&str>, native: &str) -> Result<u64> {
            let mut dataset = self.head(source)?.clone();
            let version = dataset.version().version;
            match crate::branch_control::create_branch_recoverably(&mut dataset, native, version)
                .await?
            {
                crate::branch_control::BranchCreateOutcome::Created => {}
                crate::branch_control::BranchCreateOutcome::RefAlreadyExists => {
                    return Err(OmniError::manifest_conflict(format!(
                        "the fixture already holds a ref '{native}'"
                    )));
                }
            }
            let forked = dataset
                .checkout_branch(native)
                .await
                .map_err(OmniError::storage)?;
            self.heads.insert(Some(native.to_string()), forked);
            Ok(version)
        }

        /// Retire the live ref `native` as a branch delete does.
        pub async fn retire(&mut self, native: &str) -> Result<()> {
            let main = self.head(None)?.clone();
            let identifier = crate::branch_control::list_live_manifest_branch_contents(&main)
                .await?
                .remove(native)
                .ok_or_else(|| {
                    OmniError::manifest_not_found(format!("the fixture holds no ref '{native}'"))
                })?
                .identifier;
            crate::branch_control::retire_branch_recoverably(&main, native, &identifier).await?;
            self.heads.remove(&Some(native.to_string()));
            Ok(())
        }

        /// Rewrite the rows of `native`'s head as the next version stamped
        /// `stamp` (unstamped when `None`, as an older init's bootstrap left
        /// it), stored in that stamp's shape and less its `schema_contract`
        /// row, which no version below 13 carried. The keys keep the stamp-7
        /// grammar. Returns the new version.
        pub async fn restamp_for_test(
            &mut self,
            native: Option<&str>,
            stamp: Option<u32>,
        ) -> Result<u64> {
            if let Some(stamp) = stamp.filter(|stamp| *stamp >= SCHEMA_CONTENT_STAMP) {
                return Err(OmniError::manifest_internal(format!(
                    "a restamp writes a stamp below {SCHEMA_CONTENT_STAMP}, not {stamp}; \
                     publish writes {SCHEMA_CONTENT_STAMP}"
                )));
            }
            let dataset = self.head(native)?.clone();
            let mut metadata = dataset.schema().metadata.clone();
            match stamp {
                Some(stamp) => {
                    metadata.insert(INTERNAL_SCHEMA_VERSION_KEY.to_string(), stamp.to_string());
                }
                None => {
                    metadata.remove(INTERNAL_SCHEMA_VERSION_KEY);
                }
            }
            let schema = match stored_shape(stamp) {
                StoredShape::Flat => Arc::new(
                    flat_manifest_schema()
                        .as_ref()
                        .clone()
                        .with_metadata(metadata),
                ),
                StoredShape::Packed => manifest_storage_schema(metadata, false)?,
            };
            let mut batches = Vec::new();
            for batch in logical_rows(&dataset).await? {
                let keep = BooleanArray::from_iter(
                    string_column(&batch, "object_type")?
                        .iter()
                        .map(|object_type| Some(object_type != Some(OBJECT_TYPE_SCHEMA_CONTRACT))),
                );
                let kept = filter_record_batch(&batch, &keep).map_err(OmniError::arrow_internal)?;
                batches.push(match stored_shape(stamp) {
                    StoredShape::Flat => flat_to_storage(&kept, &schema)?,
                    StoredShape::Packed => compact_to_storage(&kept, &schema)?,
                });
            }
            self.overwrite_head(native, dataset, batches).await
        }

        /// Fence main as an upgrade to `target` did: its next version keeps
        /// the rows of its head under stamp `target` and `intent` as the
        /// pending key. A publish on the fenced head keeps the key, as main's
        /// conversion did. Returns the new version.
        pub async fn fence_for_test(&mut self, intent: &str, target: u32) -> Result<u64> {
            let mut dataset = self.head(None)?.clone();
            dataset
                .update_schema_metadata([
                    (INTERNAL_SCHEMA_VERSION_KEY.to_string(), target.to_string()),
                    (UPGRADE_PENDING_KEY.to_string(), intent.to_string()),
                ])
                .await
                .map_err(OmniError::storage)?;
            let version = dataset.version().version;
            self.heads.insert(None, dataset);
            Ok(version)
        }

        /// Activate main as a completed upgrade did: its next version drops
        /// the pending key and keeps its rows. Returns the new version.
        pub async fn activate_for_test(&mut self) -> Result<u64> {
            let mut dataset = self.head(None)?.clone();
            let remaining: Vec<(String, String)> = dataset
                .schema()
                .metadata
                .iter()
                .filter(|(key, _)| key.as_str() != UPGRADE_PENDING_KEY)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            dataset
                .update_schema_metadata(remaining)
                .replace()
                .await
                .map_err(OmniError::storage)?;
            let version = dataset.version().version;
            self.heads.insert(None, dataset);
            Ok(version)
        }

        async fn overwrite_head(
            &mut self,
            native: Option<&str>,
            dataset: Dataset,
            batches: Vec<RecordBatch>,
        ) -> Result<u64> {
            let dataset = crate::commit::commit_overwrite(dataset, batches).await?;
            let version = dataset.version().version;
            self.heads.insert(native.map(str::to_string), dataset);
            Ok(version)
        }
    }

    fn stamp_13_schema(mut metadata: HashMap<String, String>) -> Result<arrow_schema::SchemaRef> {
        metadata.insert(
            INTERNAL_SCHEMA_VERSION_KEY.to_string(),
            SCHEMA_CONTENT_STAMP.to_string(),
        );
        manifest_storage_schema(metadata, true)
    }

    /// Every row of `dataset` in the logical schema, whatever its stored shape.
    async fn logical_rows(dataset: &Dataset) -> Result<Vec<RecordBatch>> {
        let shape = stored_shape(read_stamp(dataset));
        let projection = match shape {
            StoredShape::Packed => crate::record::packed_projection(dataset, true),
            StoredShape::Flat => flat_projection(),
        };
        let mut scanner = dataset.scan();
        scanner.project(&projection).map_err(OmniError::storage)?;
        let mut stream = scanner
            .try_into_stream()
            .await
            .map_err(OmniError::storage)?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
            batches.push(match shape {
                StoredShape::Packed => expand_from_storage(&batch)?,
                StoredShape::Flat => {
                    let rows = batch.num_rows();
                    let mut columns = batch.columns().to_vec();
                    columns.extend([
                        Arc::new(LargeStringArray::new_null(rows)) as arrow_array::ArrayRef,
                        Arc::new(LargeStringArray::new_null(rows)),
                    ]);
                    RecordBatch::try_new(manifest_schema(), columns)
                        .map_err(OmniError::arrow_internal)?
                }
            });
        }
        Ok(batches)
    }

    /// The rows `publish` writes at `version`, in the order a stamp-13 publish
    /// wrote them: `table` rows, then the contract, pins and drops, then the
    /// commit and its head.
    fn pending_rows(
        publish: &LegacyPublish,
        commits: &[super::LegacyCommit],
        aliases: &HashMap<TableIdentity, String>,
        logical_branch: Option<&str>,
        version: u64,
    ) -> Result<RecordBatch> {
        let mut aliases = aliases.clone();
        let mut rows = Rows::default();
        for table in &publish.tables {
            aliases.insert(table.identity, table.table_key.clone());
            rows.push(Row {
                object_id: crate::layout::table_object_id(table.identity),
                object_type: OBJECT_TYPE_TABLE,
                location: Some(table.table_path.clone()),
                table_key: table.table_key.clone(),
                identity: Some(table.identity),
                ..Row::default()
            });
        }
        let alias = |identity: TableIdentity| {
            aliases.get(&identity).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "the fixture writes a row for unregistered identity {identity}"
                ))
            })
        };
        if let Some(contract) = &publish.contract {
            rows.push(Row {
                object_id: SCHEMA_CONTRACT_OBJECT_ID.to_string(),
                object_type: OBJECT_TYPE_SCHEMA_CONTRACT,
                metadata: Some(json(&contract.head)?),
                content: Some((contract.source.clone(), contract.ir.clone())),
                ..Row::default()
            });
        }
        for pin in &publish.pins {
            rows.push(Row {
                object_id: version_object_id(pin.identity, version),
                object_type: OBJECT_TYPE_TABLE_VERSION,
                metadata: Some(pin.metadata.to_json_string()?),
                table_key: alias(pin.identity)?,
                identity: Some(pin.identity),
                table_version: Some(pin.table_version),
                table_branch: pin.table_branch.clone(),
                row_count: Some(pin.row_count),
                ..Row::default()
            });
        }
        for (identity, sealed_version) in &publish.drops {
            rows.push(Row {
                object_id: tombstone_object_id(*identity, version),
                object_type: OBJECT_TYPE_TABLE_TOMBSTONE,
                table_key: alias(*identity)?,
                identity: Some(*identity),
                table_version: Some(*sealed_version),
                ..Row::default()
            });
        }
        if let Some(commit) = &publish.commit {
            let parent_commit_id =
                head_lineage_row(commits).map(|head| head.graph_commit_id.clone());
            rows.push(Row {
                object_id: commit.graph_commit_id.clone(),
                object_type: OBJECT_TYPE_GRAPH_COMMIT,
                metadata: Some(json(&GraphCommitMetadata {
                    parent_commit_id: parent_commit_id.clone(),
                    merged_parent_commit_id: commit.merged_parent_commit_id.clone(),
                    actor_id: commit.actor_id.clone(),
                    created_at: commit.created_at,
                })?),
                table_version: Some(version),
                table_branch: logical_branch.map(str::to_string),
                ..Row::default()
            });
            rows.push(Row {
                object_id: graph_head_object_id(logical_branch),
                object_type: OBJECT_TYPE_GRAPH_HEAD,
                metadata: Some(json(&GraphHeadMetadata {
                    head_commit_id: commit.graph_commit_id.clone(),
                    parent_commit_id,
                })?),
                ..Row::default()
            });
        }
        rows.batch()
    }

    fn json(value: &impl serde::Serialize) -> Result<String> {
        serde_json::to_string(value).map_err(|error| {
            OmniError::manifest_internal(format!("failed to encode fixture metadata: {error}"))
        })
    }

    #[derive(Default)]
    struct Row {
        object_id: String,
        object_type: &'static str,
        location: Option<String>,
        metadata: Option<String>,
        table_key: String,
        identity: Option<TableIdentity>,
        table_version: Option<u64>,
        table_branch: Option<String>,
        row_count: Option<u64>,
        content: Option<(String, String)>,
    }

    #[derive(Default)]
    struct Rows(Vec<Row>);

    impl Rows {
        fn push(&mut self, row: Row) {
            self.0.push(row);
        }

        fn batch(self) -> Result<RecordBatch> {
            let rows = self.0;
            let strings = |field: fn(&Row) -> Option<String>| -> arrow_array::ArrayRef {
                Arc::new(StringArray::from(
                    rows.iter().map(field).collect::<Vec<_>>(),
                ))
            };
            let numbers = |field: fn(&Row) -> Option<u64>| -> arrow_array::ArrayRef {
                Arc::new(UInt64Array::from(
                    rows.iter().map(field).collect::<Vec<_>>(),
                ))
            };
            let content = |field: fn(&(String, String)) -> &String| -> arrow_array::ArrayRef {
                Arc::new(LargeStringArray::from(
                    rows.iter()
                        .map(|row| row.content.as_ref().map(field).cloned())
                        .collect::<Vec<_>>(),
                ))
            };
            RecordBatch::try_new(
                manifest_schema(),
                vec![
                    strings(|row| Some(row.object_id.clone())),
                    strings(|row| Some(row.object_type.to_string())),
                    strings(|row| row.location.clone()),
                    strings(|row| row.metadata.clone()),
                    strings(|row| Some(row.table_key.clone())),
                    numbers(|row| row.identity.map(|identity| identity.stable_table_id)),
                    numbers(|row| row.identity.map(|identity| identity.table_incarnation_id)),
                    numbers(|row| row.table_version),
                    strings(|row| row.table_branch.clone()),
                    numbers(|row| row.row_count),
                    content(|(source, _)| source),
                    content(|(_, ir)| ir),
                ],
            )
            .map_err(OmniError::arrow_internal)
        }
    }
}
