//! Graph-level batch publish over the namespace `__manifest` table.
//!
//! Lance now owns most of the table/version control plane for Omnigraph:
//! table storage, table-local versioning, namespace lookup, and native table
//! history. This module exists for the remaining graph-specific gap:
//! Omnigraph needs one atomic publish point across multiple tables and the
//! current Rust namespace surface does not expose a branch-aware
//! `BatchCreateTableVersions` path for `DirectoryNamespace`.
//!
//! Until Lance exposes that operation directly, this publisher owns only:
//! - validating batch publish invariants against the current `__manifest` state
//! - appending the commits a full buffer holds, and the records of a merged
//!   branch, to `__history`
//! - atomically replacing the `table` rows, the head commit's record and the
//!   buffer in `__manifest`
//! - returning the refreshed manifest dataset that defines the visible graph
//!
//! This module should disappear once Lance Rust can do branch-aware batch table
//! version publication against a managed namespace manifest.

use arrow_array::RecordBatch;
use lance_core::deepsize::DeepSizeOf;
use lance_table::format::Manifest;
use lance_table::io::commit::ManifestLocation;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lance::Dataset;
use lance::Error as LanceError;
use lance_namespace::NamespaceError;
#[cfg(any(test, feature = "test-util"))]
use lance_namespace::models::CreateTableVersionRequest;
use omnigraph_core::graph_commit_id::{
    HISTORY_BLOCK_SLOTS, HistoryBlockId, canonical_ulid, parse_history_block_id,
};

use crate::error::{OmniError, Result};

#[cfg(any(test, feature = "test-util"))]
use super::DatasetUpdate;
use super::commit;
use super::history;
use super::layout::open_manifest_dataset_with_session;
use super::metadata::parse_namespace_version_request;
use super::migrations::{INTERNAL_MANIFEST_SCHEMA_VERSION, guard_stamp, read_stamp};
use super::state::{
    BranchRecords, CommitBuffer, GraphLineageRow, HeldRecord, ManifestRows, ManifestState,
    ReplacedRow, ReplacedTable, TablePin, TableRow, TableState, exact_head, manifest_state,
    read_manifest_rows,
};
use super::{
    ExpectedTableVersions, HistoryReleaseBytes, MAIN_BRANCH_HEAD_KEY, ManifestChange, NativeRefPin,
    TableIdentity, TableRename, TableTombstone,
};
use crate::seams::{contention, decide_seam, fail};

/// Retries around the version CAS of `commit::overwrite`, whose own Lance
/// retries are 0 (a rebase would be transparent merge, wrong for OCC); each
/// attempt re-runs `load_publish_state` and the expected-version pre-check.
const PUBLISHER_RETRY_BUDGET: u32 = 5;

/// The graph-lineage commit to record atomically with a manifest publish
/// (RFC-013 Phase 7). One logical intent per publish: its nonce is minted once
/// by the caller. The physical commit ID is allocated against each CAS attempt's
/// captured head and returned by publication. [`PublishPrecondition::Any`] publishes re-resolve the parent
/// per attempt. An exact-head publish instead rejects a retry once that authority
/// changed, so a prepared write can never be silently re-parented.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LineageIntent {
    /// Stable intent nonce. A canonical ULID is wrapped in the attempted block
    /// address; explicit opaque test identities remain singleton records.
    pub graph_commit_id: String,
    /// The branch this commit lands on (`None` = main).
    pub branch: Option<String>,
    /// Authoring actor, or `None` for unauthored / system writes.
    pub actor_id: Option<String>,
    /// The records of the merged-in source branch, `Some` only for a
    /// branch-merge commit: the commits its `__manifest` buffers and its head.
    /// The publish appends them to `__history` before its CAS, so a reader of
    /// the target finds the merge commit's second parent and its ancestors in
    /// `__history` whatever the source branch does next. A serialized intent
    /// names the merged head only, and one that names any does not read back.
    #[serde(
        rename = "merged_parent_commit_id",
        serialize_with = "serialize_merged_head",
        deserialize_with = "deserialize_no_merged_head"
    )]
    pub merged_parent: Option<BranchRecords>,
    /// Commit timestamp (microseconds since the UNIX epoch).
    pub created_at: i64,
    /// The byte budget of the target's buffer for this publish:
    /// [`crate::HISTORY_RELEASE_BYTES`] unless the session lowered it with
    /// `history_release_bytes`. Every attempt of one publish reads the same
    /// value, so the slot the commit id records, the records appended and the
    /// buffer emptied agree. Not serialized: a prepared token reads the
    /// production budget back.
    #[serde(skip, default)]
    pub history_release_bytes: HistoryReleaseBytes,
}

fn serialize_merged_head<S: serde::Serializer>(
    merged_parent: &Option<BranchRecords>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serde::Serialize::serialize(
        &merged_parent
            .as_ref()
            .map(|records| records.head.commit.graph_commit_id.as_str()),
        serializer,
    )
}

fn deserialize_no_merged_head<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<BranchRecords>, D::Error> {
    match <Option<String> as serde::Deserialize>::deserialize(deserializer)? {
        None => Ok(None),
        Some(_) => Err(serde::de::Error::custom(
            "a serialized lineage intent cannot carry the records of a merged branch",
        )),
    }
}

/// The exact graph-head authority a prepared write observed. An absent head is
/// first-class: a freshly-created named branch holds the head its source wrote
/// and has no head of its own until its first graph commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GraphHeadExpectation {
    /// `None` means main; `Some("main")` is normalized to `None` by [`new`].
    pub branch: Option<String>,
    /// Lance-native stable branch identity. This detects delete/recreate ABA;
    /// manifest versions/eTags are deliberately not branch identity.
    pub branch_identifier: lance::dataset::refs::BranchIdentifier,
    /// The id of the branch's exact head commit, or `None` when absent.
    pub head_commit_id: Option<String>,
}

impl GraphHeadExpectation {
    pub fn new(
        branch: Option<&str>,
        branch_identifier: lance::dataset::refs::BranchIdentifier,
        head_commit_id: Option<String>,
    ) -> Self {
        Self {
            branch: branch
                .filter(|branch| *branch != "main")
                .map(ToOwned::to_owned),
            branch_identifier,
            head_commit_id,
        }
    }

    /// The name a changed head carries in a `ReadSetChanged` conflict.
    fn read_set_member(&self) -> String {
        format!(
            "graph_head:{}",
            self.branch.as_deref().unwrap_or(MAIN_BRANCH_HEAD_KEY)
        )
    }
}

/// Authority checked by the manifest publisher on every CAS attempt.
///
/// `Any` preserves the legacy dispatcher semantics: version-CAS contention may
/// retry and re-parent a lineage intent. `ExactGraphHead` is the RFC-022
/// foundation for prepared writes: after contention, any head movement becomes
/// `ReadSetChanged` rather than a transparent re-parent. Its native branch-id
/// check detects delete/recreate ABA on every attempt; it is not a distributed
/// ref-control fence (Lance branch create/delete still lacks conditional CAS),
/// so branch control remains within the documented single-writer-process bound.
/// `ExactGraphVersion` additionally fixes the numeric base on every retry,
/// including metadata-only contention that preserves graph HEAD.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PublishPrecondition {
    Any,
    ExactGraphHead(GraphHeadExpectation),
    /// A prepared schema publication or its settlement fence may consume only
    /// `version + 1`. Even head-preserving metadata contention must not rebase it.
    ExactGraphVersion {
        authority: GraphHeadExpectation,
        version: u64,
    },
}

/// The result of a manifest publish that may have folded in a graph commit.
#[derive(Debug)]
pub struct PublishOutcome {
    /// The advanced `__manifest` dataset (its version is the published version).
    pub dataset: Dataset,
    /// The parent the publisher resolved for the recorded commit, if a
    /// [`LineageIntent`] was supplied. `None` when no lineage was recorded.
    pub parent_commit_id: Option<String>,
    /// The new visible per-table state, reduced in memory from the rows the
    /// publish wrote by the reduction a read of `dataset` applies, so the
    /// caller skips that read.
    pub known_state: ManifestState,
    /// The head commit's record in `dataset`: the commit this publish recorded
    /// when a [`LineageIntent`] was supplied, else the head the publish read.
    pub head: GraphLineageRow,
    /// The `table` rows of `dataset`'s version.
    pub tables: Vec<TableRow>,
    /// The buffer of `dataset`'s version.
    pub buffer: CommitBuffer,
    pub schema_contract: Option<crate::SchemaContractRow>,
}

#[async_trait]
pub trait ManifestBatchPublisher: Send + Sync {
    fn cached_rows(&self, _dataset: &Dataset) -> Option<RecordBatch> {
        None
    }

    /// Publish without a graph-head precondition. Every production writer
    /// calls `publish_with_precondition`; only the publisher's own tests use
    /// this shorthand.
    #[cfg(any(test, feature = "test-util"))]
    async fn publish(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
    ) -> Result<PublishOutcome> {
        self.publish_with_precondition(
            changes,
            expected_table_versions,
            lineage,
            &PublishPrecondition::Any,
        )
        .await
    }

    async fn publish_with_precondition(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
        precondition: &PublishPrecondition,
    ) -> Result<PublishOutcome>;
}

pub struct GraphNamespacePublisher {
    root_uri: String,
    branch: Option<String>,
    control_session: Arc<lance::session::Session>,
    published_rows: Mutex<Option<PublishedRows>>,
}

pub(crate) const PUBLISHED_ROWS_CACHE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
struct PublishedRows {
    manifest: Manifest,
    location: ManifestLocation,
    batch: RecordBatch,
    acknowledged: Option<Dataset>,
}

impl PublishedRows {
    async fn new(dataset: &Dataset, batch: RecordBatch) -> Option<Self> {
        let manifest = dataset.manifest();
        let location = dataset.manifest_location();
        let bytes = batch
            .get_array_memory_size()
            .saturating_add(manifest.deep_size_of())
            .saturating_add(location.path.as_ref().len())
            .saturating_add(location.e_tag.as_ref().map_or(0, String::capacity))
            .saturating_add(std::mem::size_of::<Self>());
        if bytes > PUBLISHED_ROWS_CACHE_BYTES
            || read_stamp(dataset) != Some(INTERNAL_MANIFEST_SCHEMA_VERSION)
            || manifest
                .schema
                .fields_pre_order()
                .any(|field| field.dictionary.is_some())
        {
            return None;
        }
        let acknowledged = crate::instrumentation::retain_control_dataset(
            dataset,
            PUBLISHED_ROWS_CACHE_BYTES - bytes,
        )
        .await;
        Some(Self {
            manifest: manifest.clone(),
            location: location.clone(),
            batch,
            acknowledged,
        })
    }

    fn matches(&self, dataset: &Dataset) -> bool {
        manifest_image_matches(&self.manifest, &self.location, dataset)
    }
}

pub(crate) fn manifest_image_matches(
    manifest: &Manifest,
    location: &ManifestLocation,
    dataset: &Dataset,
) -> bool {
    let other = dataset.manifest_location();
    location.path == other.path
        && location.version == other.version
        && location.naming_scheme == other.naming_scheme
        && !matches!((&location.e_tag, &other.e_tag), (Some(a), Some(b)) if a != b)
        && !matches!((location.size, other.size), (Some(a), Some(b)) if a != b)
        && manifest == dataset.manifest()
        // Lance Schema equality compares fields but omits schema metadata.
        && manifest.schema.metadata == dataset.schema().metadata
}

/// Everything one CAS attempt needs, from one `__manifest` scan: the open
/// dataset, its `table` rows and its head commit's record.
struct LoadedPublishState {
    dataset: Dataset,
    rows: ManifestRows,
}

type TableRows = BTreeMap<TableIdentity, TableRow>;

/// The rows one CAS attempt read and its overwrite replaces.
pub(crate) struct HeldRows<'a> {
    pub(crate) tables: &'a [TableRow],
    pub(crate) head: &'a GraphLineageRow,
    pub(crate) buffer: &'a CommitBuffer,
}

decide_seam! {
    /// The publisher's `load_publish_state` read, inside the CAS retry loop.
    /// Contention here proves the outer retry re-runs the load.
    pub static PUBLISH_LOAD_STATE = ("publish.load_state", AnyWrite, [Contention]);
}

decide_seam! {
    /// Before the `__manifest` overwrite is issued: nothing has landed, and
    /// the failure carries no conflict details, so the publish loop's ambiguity
    /// arm must prove the attempted version absent instead of reporting doubt.
    pub static PUBLISH_PRE_MERGE = ("publish.pre_merge", AnyWrite, [Fail]);
}

decide_seam! {
    /// After the `__manifest` overwrite committed durably and before the
    /// publisher acknowledges it: the graph is already published and visible,
    /// only the caller's acknowledgement is at risk (the lost-ack window). A
    /// failure here models a dropped acknowledgement of a durable commit; the
    /// publisher's ambiguity read-back must recognize it as success (RFC 0067).
    pub static PUBLISH_POST_MERGE_PRE_ACK = ("publish.post_merge_pre_ack", AnyWrite, [Fail]);
}

decide_seam! {
    /// Readback may be unavailable after a lost acknowledgement. This cannot
    /// turn an indeterminate commit into permission to replay the mutation.
    pub static PUBLISH_READ_BACK = ("publish.read_back", AnyWrite, [Fail]);
}

impl GraphNamespacePublisher {
    #[cfg(any(test, feature = "test-util"))]
    pub fn new(root_uri: &str, branch: Option<&str>) -> Self {
        Self::new_with_session(root_uri, branch, crate::lance_access::control_session())
    }

    pub(crate) fn new_with_session(
        root_uri: &str,
        branch: Option<&str>,
        control_session: Arc<lance::session::Session>,
    ) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            branch: branch
                .filter(|branch| *branch != "main")
                .map(ToOwned::to_owned),
            control_session,
            published_rows: Mutex::new(None),
        }
    }

    async fn dataset(&self) -> Result<Dataset> {
        if self.branch.is_none() {
            let held = self
                .published_rows
                .lock()
                .expect("published rows cache lock poisoned")
                .as_ref()
                .and_then(|rows| rows.acknowledged.clone());
            if let Some(held) = held
                && let Some(current) = crate::instrumentation::current_control_dataset(
                    &held,
                    crate::instrumentation::manifest_wrapper(),
                )
                .await?
            {
                return Ok(current);
            }
        }
        open_manifest_dataset_with_session(
            &self.root_uri,
            self.branch.as_deref(),
            &self.control_session,
        )
        .await
    }

    async fn load_publish_state(&self) -> Result<LoadedPublishState> {
        // Test seam: inject a retryable contention here to exercise the outer
        // retry loop's re-run-on-retryable-load-error path (no-op without the
        // `failpoints` feature). The migration surfaces the same typed error.
        contention(&PUBLISH_LOAD_STATE)?;
        let dataset = self.dataset().await?;
        guard_stamp(&dataset)?;
        let rows = match self.cached_rows(&dataset) {
            Some(batch) => super::state::rows_of_batch(&batch)?,
            None => read_manifest_rows(&dataset).await?,
        };
        Ok(LoadedPublishState { dataset, rows })
    }

    /// The `table` rows after `changes`, and whether `changes` wrote any row.
    /// Registrations and renames apply first, so an update or a drop in the
    /// same batch resolves through the binding the batch leaves.
    fn apply_changes(
        changes: &[ManifestChange],
        tables: &TableRows,
        new_manifest_version: u64,
    ) -> Result<(TableRows, bool)> {
        let mut claimed_identities = HashSet::<TableIdentity>::new();
        let mut binding_changes = HashSet::<TableIdentity>::new();
        let mut tables = tables.clone();
        let mut wrote = false;

        for change in changes {
            match change {
                ManifestChange::RegisterTable(registration) => {
                    registration.identity.validate()?;
                    let canonical_path = super::table_path_for_identity(
                        &registration.table_key,
                        registration.identity,
                    )?;
                    if canonical_path != registration.table_path {
                        return Err(OmniError::storage_namespace(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table {} identity {} must use canonical path {}, got {}",
                                    registration.table_key,
                                    registration.identity,
                                    canonical_path,
                                    registration.table_path,
                                ),
                            },
                        ));
                    }
                    if let Some(existing) = tables.get(&registration.identity) {
                        let existing = &existing.registration;
                        if existing == registration {
                            continue;
                        }
                        return Err(OmniError::storage_namespace(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table identity {} is already registered as {} at {}",
                                    registration.identity, existing.table_key, existing.table_path,
                                ),
                            },
                        ));
                    }
                    if !binding_changes.insert(registration.identity) {
                        return Err(OmniError::manifest(format!(
                            "manifest batch changes table binding {} more than once",
                            registration.identity
                        )));
                    }
                    tables.insert(
                        registration.identity,
                        TableRow {
                            registration: registration.clone(),
                            state: TableState::Registered,
                        },
                    );
                    wrote = true;
                }
                ManifestChange::RenameTable(TableRename {
                    identity,
                    expected_table_key,
                    table_key,
                    table_path,
                }) => {
                    identity.validate()?;
                    if !binding_changes.insert(*identity) {
                        return Err(OmniError::manifest(format!(
                            "manifest batch changes table binding {identity} more than once"
                        )));
                    }
                    let existing = tables.get_mut(identity).ok_or_else(|| {
                        OmniError::storage_namespace(NamespaceError::TableNotFound {
                            message: format!("table identity {identity} not found"),
                        })
                    })?;
                    if !matches!(existing.state, TableState::Pinned(_)) {
                        return Err(OmniError::storage_namespace(
                            NamespaceError::TableNotFound {
                                message: format!(
                                    "live table identity {identity} not found for rename"
                                ),
                            },
                        ));
                    }
                    let registration = &mut existing.registration;
                    if registration.table_key != *expected_table_key
                        || registration.table_path != *table_path
                    {
                        return Err(OmniError::manifest_read_set_changed(
                            format!("dataset_binding:{identity}"),
                            Some(format!("{expected_table_key}@{table_path}")),
                            Some(format!(
                                "{}@{}",
                                registration.table_key, registration.table_path
                            )),
                        ));
                    }
                    let canonical_path = super::table_path_for_identity(table_key, *identity)?;
                    if canonical_path != *table_path {
                        return Err(OmniError::manifest(format!(
                            "rename of table identity {identity} must preserve physical path \
                             {table_path}; alias '{table_key}' implies {canonical_path}"
                        )));
                    }
                    if table_key == expected_table_key {
                        continue;
                    }
                    registration.table_key = table_key.clone();
                    wrote = true;
                }
                ManifestChange::Update(_)
                | ManifestChange::Tombstone(_)
                | ManifestChange::SchemaContract(_) => {}
            }
        }

        for change in changes {
            match change {
                ManifestChange::RegisterTable(_)
                | ManifestChange::RenameTable(_)
                | ManifestChange::SchemaContract(_) => {}
                ManifestChange::Update(update) => {
                    update.identity.validate()?;
                    let request = update.to_create_table_version_request();
                    let (table_key, table_version, row_count, table_branch, metadata) =
                        parse_namespace_version_request(&request).map_err(OmniError::storage)?;
                    let table = Self::claim(
                        &mut tables,
                        &mut claimed_identities,
                        update.identity,
                        &table_key,
                    )?;
                    if matches!(table.state, TableState::Dropped { .. }) {
                        return Err(OmniError::manifest(format!(
                            "table identity {} ({}) is tombstoned; a dropped table is never \
                             re-registered (re-adding a type mints a new incarnation)",
                            update.identity, table_key
                        )));
                    }
                    table.state = TableState::Pinned(TablePin {
                        table_version,
                        table_branch,
                        row_count,
                        metadata,
                        manifest_version: new_manifest_version,
                    });
                    wrote = true;
                }
                ManifestChange::Tombstone(TableTombstone {
                    identity,
                    table_key,
                    tombstone_version,
                }) => {
                    identity.validate()?;
                    let table =
                        Self::claim(&mut tables, &mut claimed_identities, *identity, table_key)?;
                    if matches!(table.state, TableState::Dropped { .. }) {
                        return Err(OmniError::manifest(format!(
                            "table identity {identity} ({table_key}) is already tombstoned"
                        )));
                    }
                    table.state = TableState::Dropped {
                        dropped_at: new_manifest_version,
                        sealed_version: *tombstone_version,
                    };
                    wrote = true;
                }
            }
        }

        Ok((tables, wrote))
    }

    /// The row of `identity` for the one update or drop a publish may carry
    /// for it, bound to the alias the change names.
    fn claim<'a>(
        tables: &'a mut TableRows,
        claimed_identities: &mut HashSet<TableIdentity>,
        identity: TableIdentity,
        table_key: &str,
    ) -> Result<&'a mut TableRow> {
        let table = tables.get_mut(&identity).ok_or_else(|| {
            OmniError::storage_namespace(NamespaceError::TableNotFound {
                message: format!("table identity {identity} not found"),
            })
        })?;
        if table.registration.table_key != table_key {
            return Err(OmniError::storage_namespace(
                NamespaceError::ConcurrentModification {
                    message: format!(
                        "table identity {identity} is bound to {}, not {table_key}",
                        table.registration.table_key
                    ),
                },
            ));
        }
        if !claimed_identities.insert(identity) {
            return Err(OmniError::manifest(format!(
                "table identity {identity} ({table_key}) is claimed twice in one publish request"
            )));
        }
        Ok(table)
    }

    /// The commit `intent` records on top of `parent`, the head this attempt
    /// read, at the `__manifest` version this attempt writes.
    fn next_head(
        intent: &LineageIntent,
        parent: &GraphLineageRow,
        buffer: &CommitBuffer,
        tables: &TableRows,
        native_branch: Option<String>,
        new_manifest_version: u64,
    ) -> Result<GraphLineageRow> {
        let generation = intent
            .merged_parent
            .as_ref()
            .map_or(parent.generation, |merged| {
                merged.head.commit.generation.max(parent.generation)
            })
            .checked_add(1)
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "generation of graph commit {} overflows",
                    intent.graph_commit_id
                ))
            })?;
        Ok(GraphLineageRow {
            graph_commit_id: Self::attempt_commit_id(
                intent,
                parent,
                buffer,
                tables,
                native_branch.as_deref(),
            )?,
            schema_contract: parent.schema_contract.clone(),
            schema_content_hash: parent.schema_content_hash.clone(),
            graph_branch: intent.branch.clone(),
            native_branch,
            graph_manifest_version: new_manifest_version,
            generation,
            parent_commit_id: Some(parent.graph_commit_id.clone()),
            merged_parent_commit_id: intent
                .merged_parent
                .as_ref()
                .map(|merged| merged.head.commit.graph_commit_id.clone()),
            actor_id: intent.actor_id.clone(),
            created_at: intent.created_at,
        })
    }

    /// The block id of this attempt's commit. Slot 0 is the release decision: the
    /// commit that reaches the byte budget or the slot ceiling opens a new block, and
    /// the publish after it reads that off the id and archives the buffered block whole.
    fn attempt_commit_id(
        intent: &LineageIntent,
        parent: &GraphLineageRow,
        buffer: &CommitBuffer,
        tables: &TableRows,
        native_branch: Option<&str>,
    ) -> Result<String> {
        if intent.graph_commit_id == "hb1" || intent.graph_commit_id.starts_with("hb1.") {
            return Err(OmniError::manifest_internal(
                "a publication intent must contain a nonce, not a history block commit id",
            ));
        }
        let Some(nonce) = canonical_ulid(&intent.graph_commit_id) else {
            return Ok(intent.graph_commit_id.clone());
        };
        let slot = if buffer.is_full_under(parent, tables.values(), intent.history_release_bytes)? {
            1
        } else if buffer.closes_with_under(parent, tables.values(), intent.history_release_bytes) {
            0
        } else {
            buffer.commits().len() + 1
        };
        let slot = u16::try_from(slot)
            .ok()
            .filter(|slot| *slot < HISTORY_BLOCK_SLOTS)
            .ok_or_else(|| OmniError::manifest_internal("history block slot overflow"))?;
        let block = parse_history_block_id(&parent.graph_commit_id)?
            .filter(|id| parent.native_branch.as_deref() == native_branch && id.slot + 1 == slot)
            .map_or(nonce, |id| id.block);
        Ok(HistoryBlockId::new(block, slot, nonce)?.to_string())
    }

    /// What the publish of version `replaced_at` read for each identity whose
    /// row it writes changed: the row of `read`, or its absence.
    fn replaced_tables(read: &TableRows, next: &TableRows, replaced_at: u64) -> Vec<ReplacedTable> {
        next.iter()
            .filter_map(|(identity, table)| {
                let before = match read.get(identity) {
                    None => ReplacedRow::Unregistered(table.registration.clone()),
                    Some(before) if before != table => ReplacedRow::Table(Box::new(before.clone())),
                    Some(_) => return None,
                };
                Some(ReplacedTable {
                    replaced_at,
                    before,
                })
            })
            .collect()
    }

    /// The records this publish appends to `__history`, oldest first: the full buffer,
    /// then the merged records after the merge base and the last one a held merge
    /// commit names, less the commits `read`, the branch `native_branch`, wrote or holds.
    fn records_to_append<'a>(
        read: &HeldRows<'a>,
        native_branch: Option<&str>,
        intent: &'a LineageIntent,
    ) -> Result<(Vec<HeldRecord<'a>>, history::ClosedBlocks)> {
        let mut closed = history::ClosedBlocks::default();
        let released: Vec<HeldRecord<'a>> =
            if read
                .buffer
                .is_full_under(read.head, read.tables, intent.history_release_bytes)?
            {
                let held = read.buffer.commits().iter().chain([read.head]);
                history::closed_blocks(held, &mut closed)?;
                let (buffer, current) = (read.buffer, read.tables);
                let held = |commit| HeldRecord {
                    commit,
                    buffer,
                    current,
                };
                buffer.commits().iter().map(held).collect()
            } else {
                Vec::new()
            };
        let Some(merged) = &intent.merged_parent else {
            return Ok((released, closed));
        };
        history::closed_blocks(merged.commits(), &mut closed)?;
        let kept_by_target = |commit: &GraphLineageRow| {
            commit.native_branch.as_deref() == native_branch
                || commit.graph_commit_id == read.head.graph_commit_id
                || read.buffer.get(&commit.graph_commit_id).is_some()
        };
        let merged_by_held_commits: HashSet<&str> = read
            .buffer
            .commits()
            .iter()
            .chain([read.head])
            .filter_map(|held| held.merged_parent_commit_id.as_deref())
            .collect();
        let source_run: Vec<HeldRecord<'a>> = merged.held().collect();
        let archived_through = source_run.iter().rposition(|source| {
            let id = source.commit.graph_commit_id.as_str();
            merged_by_held_commits.contains(id) || merged.merge_base.as_deref() == Some(id)
        });
        let merged = source_run
            .into_iter()
            .skip(archived_through.map_or(0, |at| at + 1))
            .filter(|merged| !kept_by_target(merged.commit));
        Ok((released.into_iter().chain(merged).collect(), closed))
    }

    /// The records [`Self::records_to_append`] chooses, for a test of what choosing them builds.
    #[cfg(test)]
    pub(crate) fn chosen_records_to_append<'a>(
        read: &HeldRows<'a>,
        native_branch: Option<&str>,
        intent: &'a LineageIntent,
    ) -> Result<Vec<HeldRecord<'a>>> {
        Ok(Self::records_to_append(read, native_branch, intent)?.0)
    }

    /// Check each expectation against the table's row: the version first
    /// (`PublishedDatasetVersionMismatch`, `actual = 0` when the table is
    /// absent), then an `Exact` pin's ref (`ReadSetChanged`).
    fn check_expected_table_versions(
        tables: &TableRows,
        expected: &ExpectedTableVersions,
    ) -> Result<()> {
        let mut __dst_ex: Vec<_> = expected.iter().collect();
        __dst_ex.sort_by(|a, b| a.0.cmp(b.0));
        for (identity, expectation) in __dst_ex {
            identity.validate()?;
            let table = tables.get(identity);
            if let Some(TableRow { registration, .. }) = table
                && registration.table_key != expectation.table_key
            {
                return Err(OmniError::manifest_read_set_changed(
                    format!("dataset_binding:{identity}"),
                    Some(expectation.table_key.clone()),
                    Some(registration.table_key.clone()),
                ));
            }
            let (actual, pinned_ref) = match table.map(|table| &table.state) {
                Some(TableState::Pinned(pin)) => (pin.table_version, Some(&pin.table_branch)),
                Some(TableState::Dropped { sealed_version, .. }) => (*sealed_version, None),
                Some(TableState::Registered) | None => (0, None),
            };
            if actual != expectation.table_version {
                return Err(OmniError::published_dataset_version_mismatch(
                    expectation.table_key.clone(),
                    expectation.table_version,
                    actual,
                ));
            }
            if let NativeRefPin::Exact(pinned) = &expectation.native_ref
                && let Some(winner_ref) = pinned_ref
                && winner_ref != pinned
            {
                return Err(OmniError::manifest_read_set_changed(
                    format!("native_ref:{}", expectation.table_key),
                    Some(format!("{pinned:?}")),
                    Some(format!("{winner_ref:?}")),
                ));
            }
        }
        Ok(())
    }

    /// Check authority against this attempt's own scan (graph head) and ref
    /// (native branch id) before the next rows are built, so a CAS loser with an
    /// exact expectation cannot silently re-parent on its next attempt.
    async fn check_publish_precondition(
        &self,
        dataset: &Dataset,
        head: &GraphLineageRow,
        precondition: &PublishPrecondition,
    ) -> Result<()> {
        let expected = match precondition {
            PublishPrecondition::Any => return Ok(()),
            PublishPrecondition::ExactGraphHead(expected) => expected,
            PublishPrecondition::ExactGraphVersion { authority, version } => {
                if dataset.version().version != *version {
                    return Err(OmniError::manifest_read_set_changed(
                        "prepared_schema_manifest_version",
                        Some(version.to_string()),
                        Some(dataset.version().version.to_string()),
                    ));
                }
                authority
            }
        };

        let expected_branch = expected
            .branch
            .as_deref()
            .filter(|branch| *branch != "main");
        if expected_branch != self.branch.as_deref() {
            return Err(OmniError::manifest_internal(format!(
                "publish graph-head precondition targets branch '{}' but publisher is bound to '{}'",
                expected_branch.unwrap_or("main"),
                self.branch.as_deref().unwrap_or("main"),
            )));
        }

        let branch_identity_member =
            format!("branch_identifier:{}", expected_branch.unwrap_or("main"));
        let expected_branch_identifier = serde_json::to_string(&expected.branch_identifier)
            .map_err(|e| {
                OmniError::manifest_internal(format!(
                    "failed to encode expected Lance branch identifier: {e}"
                ))
            })?;
        let observed_identifier = match dataset.manifest().branch.as_deref() {
            Some(native) => {
                crate::branch_control::get_live_manifest_branch_contents(dataset, native)
                    .await
                    .map(|contents| contents.identifier)
            }
            None => Ok(lance::dataset::refs::BranchIdentifier::main()),
        };
        let actual_branch_identifier = match observed_identifier {
            Ok(identifier) => identifier,
            Err(OmniError::BranchNotFound { .. }) => {
                return Err(OmniError::manifest_read_set_changed(
                    branch_identity_member,
                    Some(expected_branch_identifier),
                    None,
                ));
            }
            Err(err) => return Err(err),
        };
        if actual_branch_identifier != expected.branch_identifier {
            let actual = serde_json::to_string(&actual_branch_identifier).map_err(|e| {
                OmniError::manifest_internal(format!(
                    "failed to encode current Lance branch identifier: {e}"
                ))
            })?;
            return Err(OmniError::manifest_read_set_changed(
                branch_identity_member,
                Some(expected_branch_identifier),
                Some(actual),
            ));
        }

        let actual = exact_head(head, dataset.manifest().branch.as_deref())
            .map(|head| head.graph_commit_id.clone());
        if actual != expected.head_commit_id {
            return Err(OmniError::manifest_read_set_changed(
                expected.read_set_member(),
                expected.head_commit_id.clone(),
                actual,
            ));
        }
        Ok(())
    }

    async fn merge_rows(&self, dataset: Dataset, rows: &ManifestRows) -> Result<Dataset> {
        fail(&PUBLISH_PRE_MERGE)?;
        let (new_dataset, batch) = commit::overwrite(dataset, rows).await?;
        // The commit is durable and the graph is published; a failure here
        // models the acknowledgement being lost after that (RFC 0067). It is
        // an opaque error with no conflict details, so the publish loop's
        // ambiguity arm reads the manifest back rather than reporting failure.
        fail(&PUBLISH_POST_MERGE_PRE_ACK)?;
        let cached = PublishedRows::new(&new_dataset, batch).await;
        *self
            .published_rows
            .lock()
            .expect("published rows cache lock poisoned") = cached;
        Ok(new_dataset)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) async fn publish_requests(
        &self,
        requests: &[CreateTableVersionRequest],
    ) -> Result<Dataset> {
        let tables = self.load_publish_state().await?.rows.tables;
        let changes = requests
            .iter()
            .map(|request| {
                let (table_key, table_version, row_count, table_branch, version_metadata) =
                    parse_namespace_version_request(request).map_err(OmniError::storage)?;
                let identity = tables
                    .iter()
                    .map(|table| &table.registration)
                    .find(|registration| registration.table_key == table_key)
                    .map(|registration| registration.identity)
                    .ok_or_else(|| {
                        OmniError::manifest(format!(
                            "test namespace request references unknown table alias {table_key}"
                        ))
                    })?;
                Ok(ManifestChange::Update(DatasetUpdate {
                    identity,
                    type_key: table_key,
                    published_dataset_version: table_version,
                    native_dataset_branch: table_branch,
                    entity_count: row_count,
                    version_metadata,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self
            .publish(&changes, &ExpectedTableVersions::new(), None)
            .await?
            .dataset)
    }
}

/// The conflict vocabulary of the `__manifest` version CAS (`commit::overwrite`,
/// a zero-retry Lance commit): the attempted version already exists, so this
/// attempt landed nothing. Lance reports that as `CommitConflict`,
/// `RetryableCommitConflict` or `TooMuchWriteContention`; each becomes the typed
/// `RowLevelCasContention` the publish loop re-prepares on, so callers match
/// without parsing strings. Everything else is opaque storage.
pub fn map_lance_publish_error(err: LanceError) -> OmniError {
    if matches!(
        err,
        LanceError::CommitConflict { .. }
            | LanceError::RetryableCommitConflict { .. }
            | LanceError::TooMuchWriteContention { .. }
    ) {
        return OmniError::manifest_row_level_cas_contention(format!(
            "manifest publish lost the version CAS: {err}"
        ));
    }
    OmniError::storage(err)
}

/// Construct the typed in-doubt outcome for an ambiguous manifest publish: an
/// opaque error whose read-back could not itself complete, so whether the
/// commit is durable is genuinely unknown. Distinct from an opaque storage
/// error so a caller does not blindly retry a possibly-durable write; kind
/// Internal, so the server surfaces it as a server-side unknown rather than a
/// definitive conflict (RFC 0067).
fn publish_outcome_in_doubt(
    original: &OmniError,
    graph_commit_id: &str,
    cause: impl std::fmt::Display,
) -> OmniError {
    OmniError::manifest_publish_in_doubt(format!(
        "manifest publish outcome is in doubt: graph commit {graph_commit_id} may be durable but \
         it could not be confirmed ({cause}); reopen the graph and look that commit up before \
         retrying a non-idempotent write. original error: {original}"
    ))
}

#[async_trait]
impl ManifestBatchPublisher for GraphNamespacePublisher {
    fn cached_rows(&self, dataset: &Dataset) -> Option<RecordBatch> {
        self.published_rows
            .lock()
            .expect("published rows cache lock poisoned")
            .as_ref()
            .filter(|rows| rows.matches(dataset))
            .map(|rows| rows.batch.clone())
    }

    async fn publish_with_precondition(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
        precondition: &PublishPrecondition,
    ) -> Result<PublishOutcome> {
        for attempt in 0..=PUBLISHER_RETRY_BUDGET {
            // Route a retryable `load_publish_state` error through the SAME retry
            // path as a retryable `merge_rows` conflict below, so typed contention
            // composes with the publisher retry instead of aborting the publish.
            let loaded = match self.load_publish_state().await {
                Ok(loaded) => loaded,
                Err(err)
                    if attempt < PUBLISHER_RETRY_BUDGET && is_retryable_publish_conflict(&err) =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            };
            let LoadedPublishState {
                dataset,
                rows:
                    ManifestRows {
                        tables,
                        head,
                        buffer,
                        schema_contract,
                        ..
                    },
            } = loaded;
            let native_branch = dataset.manifest().branch.clone();

            self.check_publish_precondition(&dataset, &head, precondition)
                .await?;

            let tables: TableRows = tables
                .into_iter()
                .map(|table| (table.registration.identity, table))
                .collect();
            // Pre-check on every attempt against freshly loaded state so a
            // concurrent commit that broke the caller's expectation is
            // surfaced as `PublishedDatasetVersionMismatch` rather than retried.
            Self::check_expected_table_versions(&tables, expected_table_versions)?;

            let new_manifest_version = dataset.version().version + 1;
            let (next_tables, mut wrote) =
                Self::apply_changes(changes, &tables, new_manifest_version)?;
            let mut next_contract = schema_contract;
            let mut seen_contract = false;
            for change in changes {
                if let ManifestChange::SchemaContract(contract) = change {
                    if seen_contract {
                        return Err(OmniError::manifest(
                            "schema_contract row cannot be replaced twice in one publish"
                                .to_string(),
                        ));
                    }
                    seen_contract = true;
                    next_contract = Some(contract.clone());
                    wrote = true;
                }
            }

            let Some(mut next_head) = lineage
                .map(|intent| {
                    Self::next_head(
                        intent,
                        &head,
                        &buffer,
                        &tables,
                        native_branch.clone(),
                        new_manifest_version,
                    )
                })
                .transpose()?
                .or_else(|| wrote.then(|| head.clone()))
            else {
                let rows = ManifestRows {
                    schema_contract_head: next_contract.as_ref().map(|row| row.head.clone()),
                    tables: tables.into_values().collect(),
                    head,
                    buffer,
                    schema_contract: next_contract,
                };
                let known_state =
                    rows.state(dataset.version().version, native_branch.as_deref())?;
                return Ok(PublishOutcome {
                    dataset,
                    parent_commit_id: None,
                    known_state,
                    head: rows.head,
                    tables: rows.tables,
                    buffer: rows.buffer,
                    schema_contract: rows.schema_contract,
                });
            };
            if lineage.is_none() && !cfg!(any(test, feature = "test-util")) {
                return Err(OmniError::manifest_internal(
                    "a publish that changes `__manifest` rows records a graph commit, so that \
                     the `table` rows of every version are the state as of its head commit; \
                     this publish carries changes and no lineage intent"
                        .to_string(),
                ));
            }
            let changed = Self::replaced_tables(&tables, &next_tables, new_manifest_version);
            let tables: Vec<TableRow> = tables.into_values().collect();
            let next_tables: Vec<TableRow> = next_tables.into_values().collect();
            if lineage.is_some() {
                next_head.schema_contract = next_contract.as_ref().map(|row| row.head.clone());
                let content_unchanged = !seen_contract
                    && head.schema_content_hash.is_some()
                    && head.schema_contract == next_head.schema_contract;
                if !content_unchanged {
                    next_head.schema_content_hash = next_contract
                        .as_ref()
                        .map(history::schema_content_hash)
                        .transpose()?;
                }
                history::check_head_record(&next_head, &next_tables)?;
                if let Some(contract) = &next_contract
                    && next_head.schema_content_hash != head.schema_content_hash
                {
                    history::archive_schema(&self.root_uri, &self.control_session, contract)
                        .await?;
                }
            }
            let parent_commit_id = lineage.map(|_| head.graph_commit_id.clone());
            let mut known_state = manifest_state(
                &next_tables,
                &next_head,
                new_manifest_version,
                native_branch.as_deref(),
            )?;
            known_state.schema_contract = next_contract.as_ref().map(|row| row.head.clone());
            let appended = match lineage {
                Some(intent) => {
                    let read = HeldRows {
                        tables: &tables,
                        head: &head,
                        buffer: &buffer,
                    };
                    let (records, closed) =
                        Self::records_to_append(&read, native_branch.as_deref(), intent)?;
                    match records.is_empty() {
                        true => None,
                        false => Some(
                            history::settle_closed(
                                &self.root_uri,
                                &self.control_session,
                                &records,
                                &closed,
                            )
                            .await?,
                        ),
                    }
                }
                None => None,
            };
            let next = ManifestRows {
                schema_contract_head: next_contract.as_ref().map(|row| row.head.clone()),
                buffer: buffer.after_publish(
                    lineage.map(|_| &head),
                    changed,
                    &next_head,
                    appended.as_ref(),
                    &tables,
                    lineage.map_or(HistoryReleaseBytes::PRODUCTION, |intent| {
                        intent.history_release_bytes
                    }),
                )?,
                tables: next_tables,
                head: next_head,
                schema_contract: next_contract,
            };

            match self.merge_rows(dataset.clone(), &next).await {
                Ok(new_dataset) => {
                    if new_dataset.version().version != new_manifest_version {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest commit at version {} is durable but its rows carry manifest \
                             version {}; the coordinator's view is stale, reopen the graph",
                            new_dataset.version().version,
                            new_manifest_version
                        )));
                    }
                    known_state.version = new_dataset.version().version;
                    return Ok(PublishOutcome {
                        dataset: new_dataset,
                        parent_commit_id,
                        known_state,
                        head: next.head,
                        tables: next.tables,
                        buffer: next.buffer,
                        schema_contract: next.schema_contract,
                    });
                }
                Err(err) => {
                    if attempt < PUBLISHER_RETRY_BUDGET && is_retryable_publish_conflict(&err) {
                        continue;
                    }
                    // Ambiguity resolution (RFC 0067): a typed conflict (details
                    // present) definitively did not land — surface it. An opaque
                    // error may instead mean the `__manifest` overwrite landed
                    // durably and only its acknowledgement was lost (a dropped S3
                    // 200), in which case the write is already graph-visible.
                    let ambiguous = !matches!(&err, OmniError::Manifest(m) if m.details.is_some());
                    if ambiguous && lineage.is_some() {
                        if let Err(readback_err) = fail(&PUBLISH_READ_BACK) {
                            return Err(publish_outcome_in_doubt(
                                &err,
                                &next.head.graph_commit_id,
                                readback_err,
                            ));
                        }
                        match dataset.checkout_version(new_manifest_version).await {
                            Ok(reloaded) => match read_manifest_rows(&reloaded).await {
                                Ok(rows) => {
                                    // Read the attempted immutable version on the captured
                                    // native branch, even when another writer has moved HEAD.
                                    let landed = rows.head == next.head;
                                    if landed {
                                        // Durable; only the ack was lost. Return
                                        // the exact success outcome this attempt
                                        // would have produced.
                                        known_state.version = reloaded.version().version;
                                        return Ok(PublishOutcome {
                                            dataset: reloaded,
                                            parent_commit_id,
                                            known_state,
                                            head: next.head,
                                            tables: next.tables,
                                            buffer: next.buffer,
                                            schema_contract: next.schema_contract,
                                        });
                                    }
                                    if rows.head.graph_commit_id == next.head.graph_commit_id {
                                        return Err(publish_outcome_in_doubt(
                                            &err,
                                            &next.head.graph_commit_id,
                                            "attempted commit identity has inconsistent lineage",
                                        ));
                                    }
                                    // This exact version belongs to a different commit.
                                    // A moved latest HEAD alone would prove nothing.
                                    return Err(err);
                                }
                                Err(scan_err) => {
                                    return Err(publish_outcome_in_doubt(
                                        &err,
                                        &next.head.graph_commit_id,
                                        scan_err,
                                    ));
                                }
                            },
                            Err(readback_err) => {
                                let version_absent = matches!(
                                    readback_err,
                                    LanceError::VersionNotFound { .. }
                                        | LanceError::DatasetNotFound { .. }
                                );
                                let failed_before_any_store_request =
                                    err.storage_failure().is_none();
                                if version_absent
                                    && failed_before_any_store_request
                                    && matches!(
                                        dataset.latest_version_id().await,
                                        Ok(latest) if latest < new_manifest_version
                                    )
                                {
                                    return Err(err);
                                }
                                return Err(publish_outcome_in_doubt(
                                    &err,
                                    &next.head.graph_commit_id,
                                    readback_err,
                                ));
                            }
                        }
                    }
                    return Err(err);
                }
            }
        }

        Err(OmniError::manifest_conflict(format!(
            "manifest publish exhausted {} retries against concurrent writers",
            PUBLISHER_RETRY_BUDGET
        )))
    }
}

/// A retryable conflict here means: the `__manifest` version CAS rejected our
/// commit because another writer landed the version we attempted (mapped by
/// `map_lance_publish_error` to
/// `ManifestConflictDetails::RowLevelCasContention`). This is transparent
/// contention; if the caller's `expected_table_versions` still holds against
/// the new manifest state, we re-attempt. Other conflict variants (notably
/// `PublishedDatasetVersionMismatch`) propagate so the caller learns immediately.
pub fn is_retryable_publish_conflict(err: &OmniError) -> bool {
    matches!(
        err,
        OmniError::Manifest(m)
            if matches!(
                m.details,
                Some(crate::error::ManifestConflictDetails::RowLevelCasContention)
            )
    )
}
