use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array, new_null_array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;

use crate::error::{OmniError, Result};

use super::layout::version_object_id;
use super::metadata::TableVersionMetadata;
use super::{
    MAIN_BRANCH_HEAD_KEY, OBJECT_TYPE_GRAPH_COMMIT, OBJECT_TYPE_GRAPH_HEAD, OBJECT_TYPE_TABLE,
    OBJECT_TYPE_TABLE_TOMBSTONE, OBJECT_TYPE_TABLE_VERSION,
};

#[derive(Debug, Clone)]
pub struct SubTableEntry {
    pub table_key: String,
    pub table_path: String,
    pub table_version: u64,
    pub table_branch: Option<String>,
    pub row_count: u64,
    pub(crate) version_metadata: TableVersionMetadata,
}

#[derive(Debug, Clone)]
pub(super) struct ManifestState {
    pub(super) version: u64,
    pub(super) entries: Vec<SubTableEntry>,
}

#[derive(Debug, Clone)]
struct TableTombstoneEntry {
    table_key: String,
    tombstone_version: u64,
}

/// A graph-lineage commit projected out of the `__manifest` `graph_commit`
/// rows (RFC-013 step 4). Field-for-field identical to `commit_graph::GraphCommit`
/// so the commit-graph cache can be sourced from the manifest projection without
/// touching any reader above that boundary. Kept as a separate struct here to
/// keep `state.rs` free of the `commit_graph` module dependency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GraphLineageRow {
    pub(crate) graph_commit_id: String,
    pub(crate) manifest_branch: Option<String>,
    pub(crate) manifest_version: u64,
    pub(crate) parent_commit_id: Option<String>,
    pub(crate) merged_parent_commit_id: Option<String>,
    pub(crate) actor_id: Option<String>,
    pub(crate) created_at: i64,
}

/// JSON payload of a `graph_commit` row's `metadata` column. The immutable
/// commit fields that have no dedicated manifest column live here; the mutable
/// ones (`graph_commit_id`, `manifest_branch`, `manifest_version`) reuse
/// `object_id` / `table_branch` / `table_version`.
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

/// JSON payload of a `graph_head` row's `metadata` column.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct GraphHeadMetadata {
    head_commit_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_commit_id: Option<String>,
}

/// The `object_id` for a branch's mutable head pointer row. Main encodes as
/// `graph_head:main`; named branches as `graph_head:<branch>`.
pub(crate) fn graph_head_object_id(branch: Option<&str>) -> String {
    format!("graph_head:{}", branch.unwrap_or(MAIN_BRANCH_HEAD_KEY))
}

#[derive(Debug, Clone)]
struct ManifestScan {
    table_locations: HashMap<String, String>,
    version_entries: Vec<SubTableEntry>,
    tombstones: Vec<TableTombstoneEntry>,
}

pub(super) fn manifest_schema() -> SchemaRef {
    // `object_id` is the merge-insert join key in the publisher; marking it as
    // Lance's unenforced primary key engages row-level CAS at commit time, so
    // two concurrent writers that try to land the same `object_id` row are
    // detected by Lance via bloom-filter intersection (see
    // `.context/merge-insert-cas-granularity.md`). Without this metadata,
    // Lance's conflict resolver would silently rebase both writers' new
    // fragments and admit duplicate rows.
    let object_id_metadata: HashMap<String, String> =
        [("lance-schema:unenforced-primary-key", "true")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
    Arc::new(Schema::new(vec![
        Field::new("object_id", DataType::Utf8, false).with_metadata(object_id_metadata),
        Field::new("object_type", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, true),
        Field::new("metadata", DataType::Utf8, true),
        Field::new(
            "base_objects",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("table_key", DataType::Utf8, false),
        Field::new("table_version", DataType::UInt64, true),
        Field::new("table_branch", DataType::Utf8, true),
        Field::new("row_count", DataType::UInt64, true),
    ]))
}

pub(super) async fn read_manifest_state(dataset: &Dataset) -> Result<ManifestState> {
    let version = dataset.version().version;
    let scan = read_manifest_scan(dataset).await?;
    let mut latest_versions = HashMap::<String, SubTableEntry>::new();

    for entry in scan.version_entries {
        match latest_versions.get(&entry.table_key) {
            Some(existing) if existing.table_version >= entry.table_version => {}
            _ => {
                latest_versions.insert(entry.table_key.clone(), entry);
            }
        }
    }

    let mut tombstones = HashMap::<String, u64>::new();
    for tombstone in scan.tombstones {
        match tombstones.get(&tombstone.table_key) {
            Some(existing) if *existing >= tombstone.tombstone_version => {}
            _ => {
                tombstones.insert(tombstone.table_key, tombstone.tombstone_version);
            }
        }
    }

    let mut entries: Vec<SubTableEntry> = latest_versions
        .into_values()
        .filter(|entry| {
            tombstones
                .get(&entry.table_key)
                .map(|tombstone_version| *tombstone_version < entry.table_version)
                .unwrap_or(true)
        })
        .collect();
    entries.sort_by(|a, b| a.table_key.cmp(&b.table_key));

    Ok(ManifestState { version, entries })
}

pub(super) async fn read_manifest_entries(dataset: &Dataset) -> Result<Vec<SubTableEntry>> {
    Ok(read_manifest_scan(dataset).await?.version_entries)
}

pub(super) async fn read_registered_table_locations(
    dataset: &Dataset,
) -> Result<HashMap<String, String>> {
    Ok(read_manifest_scan(dataset).await?.table_locations)
}

pub(super) async fn read_tombstone_versions(
    dataset: &Dataset,
) -> Result<HashMap<(String, u64), ()>> {
    Ok(read_manifest_scan(dataset)
        .await?
        .tombstones
        .into_iter()
        .map(|tombstone| ((tombstone.table_key, tombstone.tombstone_version), ()))
        .collect())
}

async fn read_manifest_scan(dataset: &Dataset) -> Result<ManifestScan> {
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut table_locations = HashMap::new();
    let mut version_entries = Vec::new();
    let mut tombstones = Vec::new();

    for batch in &batches {
        let object_types = string_column(batch, "object_type")?;
        let locations = string_column(batch, "location")?;
        let metadata = string_column(batch, "metadata")?;
        let table_keys = string_column(batch, "table_key")?;
        let versions = u64_column(batch, "table_version")?;
        let branches = string_column(batch, "table_branch")?;
        let row_counts = u64_column(batch, "row_count")?;

        for row in 0..batch.num_rows() {
            let table_key = table_keys.value(row).to_string();
            match object_types.value(row) {
                OBJECT_TYPE_TABLE => {
                    if locations.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table row missing location for {}",
                            table_key
                        )));
                    }
                    table_locations.insert(table_key, locations.value(row).to_string());
                }
                OBJECT_TYPE_TABLE_VERSION => {
                    let table_version = required_u64(versions, row, "table_version")?;
                    let row_count = required_u64(row_counts, row, "row_count")?;
                    if metadata.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest table_version row missing metadata for {}",
                            table_key
                        )));
                    }
                    let table_branch = if branches.is_null(row) {
                        None
                    } else {
                        Some(branches.value(row).to_string())
                    };
                    version_entries.push(SubTableEntry {
                        table_key: table_key.clone(),
                        table_path: String::new(),
                        table_version,
                        table_branch,
                        row_count,
                        version_metadata: TableVersionMetadata::from_json_str(metadata.value(row))?,
                    });
                }
                OBJECT_TYPE_TABLE_TOMBSTONE => {
                    let tombstone_version = required_u64(versions, row, "table_version")?;
                    tombstones.push(TableTombstoneEntry {
                        table_key,
                        tombstone_version,
                    });
                }
                // Graph-lineage rows (`graph_commit` / `graph_head`, RFC-013
                // step 4) are skipped on the table-state hot path: this scan
                // feeds table snapshots and runs on every read/refresh, so it
                // must not pay O(commits) JSON decoding for lineage it never
                // uses. `read_graph_lineage` has its own scan that decodes only
                // those two object types. (Same forward-compat skip as any
                // unknown future object type.)
                _ => {}
            }
        }
    }

    let mut entries = version_entries
        .into_iter()
        .map(|mut entry| {
            entry.table_path = table_locations
                .get(&entry.table_key)
                .cloned()
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "manifest missing table row for {}",
                        entry.table_key
                    ))
                })?;
            Ok(entry)
        })
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by(|a, b| {
        a.table_key
            .cmp(&b.table_key)
            .then(a.table_version.cmp(&b.table_version))
    });

    Ok(ManifestScan {
        table_locations,
        version_entries: entries,
        tombstones,
    })
}

/// Project the graph-lineage rows (`graph_commit` + `graph_head`) out of
/// `__manifest` (RFC-013 step 4). Returns every commit and the per-branch head
/// map (keyed by branch name, `"main"` for main). `__manifest` is the single
/// source of graph lineage: the commit-graph cache is sourced from here, and the
/// publisher resolves a new commit's parent from here inside its CAS loop.
///
/// Dedicated scan (separate from `read_manifest_scan`): it decodes ONLY the two
/// lineage object types and builds no table snapshot, so the table-state hot
/// path never pays for lineage JSON and this path never pays for table-entry
/// assembly.
pub(crate) async fn read_graph_lineage(
    dataset: &Dataset,
) -> Result<(Vec<GraphLineageRow>, HashMap<String, String>)> {
    let batches: Vec<RecordBatch> = dataset
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;

    let mut graph_commits = Vec::new();
    let mut graph_heads = HashMap::new();

    for batch in &batches {
        let object_ids = string_column(batch, "object_id")?;
        let object_types = string_column(batch, "object_type")?;
        let metadata = string_column(batch, "metadata")?;
        let versions = u64_column(batch, "table_version")?;
        let branches = string_column(batch, "table_branch")?;

        for row in 0..batch.num_rows() {
            match object_types.value(row) {
                OBJECT_TYPE_GRAPH_COMMIT => {
                    if metadata.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest graph_commit row missing metadata for {}",
                            object_ids.value(row)
                        )));
                    }
                    let commit_meta: GraphCommitMetadata =
                        serde_json::from_str(metadata.value(row)).map_err(|e| {
                            OmniError::manifest_internal(format!(
                                "failed to decode graph_commit metadata: {e}"
                            ))
                        })?;
                    graph_commits.push(GraphLineageRow {
                        graph_commit_id: object_ids.value(row).to_string(),
                        manifest_branch: if branches.is_null(row) {
                            None
                        } else {
                            Some(branches.value(row).to_string())
                        },
                        manifest_version: required_u64(versions, row, "table_version")?,
                        parent_commit_id: commit_meta.parent_commit_id,
                        merged_parent_commit_id: commit_meta.merged_parent_commit_id,
                        actor_id: commit_meta.actor_id,
                        created_at: commit_meta.created_at,
                    });
                }
                OBJECT_TYPE_GRAPH_HEAD => {
                    if metadata.is_null(row) {
                        return Err(OmniError::manifest_internal(format!(
                            "manifest graph_head row missing metadata for {}",
                            object_ids.value(row)
                        )));
                    }
                    let head_meta: GraphHeadMetadata = serde_json::from_str(metadata.value(row))
                        .map_err(|e| {
                            OmniError::manifest_internal(format!(
                                "failed to decode graph_head metadata: {e}"
                            ))
                        })?;
                    // `object_id` is `graph_head:<branch>`; the branch key after
                    // the prefix is the projection's map key (`main` for main).
                    let branch_key = object_ids
                        .value(row)
                        .strip_prefix("graph_head:")
                        .unwrap_or_default()
                        .to_string();
                    graph_heads.insert(branch_key, head_meta.head_commit_id);
                }
                _ => {}
            }
        }
    }

    Ok((graph_commits, graph_heads))
}

/// The current head of a branch's lineage: the [`GraphLineageRow`] with the
/// greatest `(manifest_version, created_at, graph_commit_id)`. This is the same
/// ordering the commit-graph cache uses to pick its head (`should_replace_head`)
/// — kept in one place so the publisher's per-attempt parent resolution and the
/// cache agree by construction. `None` only for a graph with no commits yet
/// (a parentless genesis).
pub(crate) fn head_lineage_row(rows: &[GraphLineageRow]) -> Option<&GraphLineageRow> {
    rows.iter().max_by(|a, b| {
        a.manifest_version
            .cmp(&b.manifest_version)
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.graph_commit_id.cmp(&b.graph_commit_id))
    })
}

/// One `__manifest` row materializing a piece of a graph commit's lineage. The
/// publisher maps these onto its `PendingVersionRow`s (folding lineage into the
/// table-version publish batch), and the genesis init path pushes them straight
/// into the init batch.
pub(crate) struct GraphLineageRowPart {
    pub(crate) object_id: String,
    pub(crate) object_type: &'static str,
    pub(crate) metadata: String,
    pub(crate) table_version: Option<u64>,
    pub(crate) table_branch: Option<String>,
}

/// Encode one graph commit into its two `__manifest` rows: the immutable
/// `graph_commit` row plus the mutable `graph_head:<branch>` pointer (a
/// merge-insert on `object_id` updates the head in place). `branch` is `None`
/// for main. The immutable commit fields with no dedicated column live in the
/// `graph_commit` row's `metadata` JSON; the mutable head pointer payload lives
/// in the `graph_head` row's `metadata`.
pub(crate) fn graph_lineage_row_parts(
    commit: &GraphLineageRow,
    branch: Option<&str>,
) -> Result<[GraphLineageRowPart; 2]> {
    let commit_metadata = serde_json::to_string(&GraphCommitMetadata {
        parent_commit_id: commit.parent_commit_id.clone(),
        merged_parent_commit_id: commit.merged_parent_commit_id.clone(),
        actor_id: commit.actor_id.clone(),
        created_at: commit.created_at,
    })
    .map_err(|e| {
        OmniError::manifest_internal(format!("failed to encode graph_commit metadata: {e}"))
    })?;
    let head_metadata = serde_json::to_string(&GraphHeadMetadata {
        head_commit_id: commit.graph_commit_id.clone(),
        parent_commit_id: commit.parent_commit_id.clone(),
    })
    .map_err(|e| {
        OmniError::manifest_internal(format!("failed to encode graph_head metadata: {e}"))
    })?;

    Ok([
        // Only the immutable commit row carries the manifest version + branch.
        GraphLineageRowPart {
            object_id: commit.graph_commit_id.clone(),
            object_type: OBJECT_TYPE_GRAPH_COMMIT,
            metadata: commit_metadata,
            table_version: Some(commit.manifest_version),
            table_branch: commit.manifest_branch.clone(),
        },
        // The head row reuses `metadata` for its pointer payload.
        GraphLineageRowPart {
            object_id: graph_head_object_id(branch),
            object_type: OBJECT_TYPE_GRAPH_HEAD,
            metadata: head_metadata,
            table_version: None,
            table_branch: None,
        },
    ])
}

pub(super) fn entries_to_batch(
    entries: &[SubTableEntry],
    version_metadata: &HashMap<String, String>,
    genesis_lineage: &[GraphLineageRowPart],
) -> Result<RecordBatch> {
    let cap = entries.len() * 2 + genesis_lineage.len();
    let mut object_ids = Vec::with_capacity(cap);
    let mut object_types = Vec::with_capacity(cap);
    let mut locations = Vec::with_capacity(cap);
    let mut metadata = Vec::with_capacity(cap);
    let mut table_keys = Vec::with_capacity(cap);
    let mut table_versions = Vec::with_capacity(cap);
    let mut table_branches = Vec::with_capacity(cap);
    let mut row_counts = Vec::with_capacity(cap);

    for entry in entries {
        object_ids.push(entry.table_key.clone());
        object_types.push(OBJECT_TYPE_TABLE.to_string());
        locations.push(Some(entry.table_path.clone()));
        metadata.push(None);
        table_keys.push(entry.table_key.clone());
        table_versions.push(None);
        table_branches.push(None);
        row_counts.push(None);

        object_ids.push(version_object_id(&entry.table_key, entry.table_version));
        object_types.push(OBJECT_TYPE_TABLE_VERSION.to_string());
        locations.push(None);
        metadata.push(Some(
            version_metadata
                .get(&entry.table_key)
                .cloned()
                .ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "missing initial version metadata for {}",
                        entry.table_key
                    ))
                })?,
        ));
        table_keys.push(entry.table_key.clone());
        table_versions.push(Some(entry.table_version));
        table_branches.push(entry.table_branch.clone());
        row_counts.push(Some(entry.row_count));
    }

    // Genesis graph-lineage rows ride the init write so a fresh graph carries
    // its `graph_commit` + `graph_head` in `__manifest` from version one (no
    // separate lineage fragment, no second commit). `table_key` is non-nullable
    // but lineage rows have no table identity, so the empty string stands in
    // (never matched by a real key).
    for part in genesis_lineage {
        object_ids.push(part.object_id.clone());
        object_types.push(part.object_type.to_string());
        locations.push(None);
        metadata.push(Some(part.metadata.clone()));
        table_keys.push(String::new());
        table_versions.push(part.table_version);
        table_branches.push(part.table_branch.clone());
        row_counts.push(None);
    }

    manifest_rows_batch(
        object_ids,
        object_types,
        locations,
        metadata,
        table_keys,
        table_versions,
        table_branches,
        row_counts,
    )
}

pub(super) fn manifest_rows_batch(
    object_ids: Vec<String>,
    object_types: Vec<String>,
    locations: Vec<Option<String>>,
    metadata: Vec<Option<String>>,
    table_keys: Vec<String>,
    table_versions: Vec<Option<u64>>,
    table_branches: Vec<Option<String>>,
    row_counts: Vec<Option<u64>>,
) -> Result<RecordBatch> {
    let len = object_ids.len();
    RecordBatch::try_new(
        manifest_schema(),
        vec![
            Arc::new(StringArray::from(object_ids)),
            Arc::new(StringArray::from(object_types)),
            Arc::new(StringArray::from(locations)),
            Arc::new(StringArray::from(metadata)),
            new_null_array(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                len,
            ),
            Arc::new(StringArray::from(table_keys)),
            Arc::new(UInt64Array::from(table_versions)),
            Arc::new(StringArray::from(table_branches)),
            Arc::new(UInt64Array::from(row_counts)),
        ],
    )
    .map_err(|e| OmniError::Lance(e.to_string()))
}

pub(super) fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
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
