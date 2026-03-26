use std::collections::HashSet;

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_cast::display::array_value_to_string;
use lance::dataset::scanner::ColumnOrdering;

use crate::db::SubTableEntry;
use crate::db::manifest::Snapshot;
use crate::error::Result;
use crate::table_store::TableStore;

// ─── Types ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    pub src: String,
    pub dst: String,
}

#[derive(Debug, Clone)]
pub struct EntityChange {
    pub table_key: String,
    pub kind: EntityKind,
    pub type_name: String,
    pub id: String,
    pub op: ChangeOp,
    pub manifest_version: u64,
    pub endpoints: Option<Endpoints>,
}

#[derive(Debug, Clone, Default)]
pub struct ChangeFilter {
    pub kinds: Option<Vec<EntityKind>>,
    pub type_names: Option<Vec<String>>,
    pub ops: Option<Vec<ChangeOp>>,
}

#[derive(Debug, Clone, Default)]
pub struct ChangeStats {
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
    pub types_affected: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub from_version: u64,
    pub to_version: u64,
    pub branch: Option<String>,
    pub changes: Vec<EntityChange>,
    pub stats: ChangeStats,
}

// ─── Filter helpers ─────────────────────────────────────────────────────────

fn parse_table_key(table_key: &str) -> (EntityKind, &str) {
    if let Some(name) = table_key.strip_prefix("node:") {
        (EntityKind::Node, name)
    } else if let Some(name) = table_key.strip_prefix("edge:") {
        (EntityKind::Edge, name)
    } else {
        (EntityKind::Node, table_key)
    }
}

impl ChangeFilter {
    fn matches_table(&self, table_key: &str) -> bool {
        let (kind, type_name) = parse_table_key(table_key);
        if let Some(ref kinds) = self.kinds {
            if !kinds.contains(&kind) {
                return false;
            }
        }
        if let Some(ref names) = self.type_names {
            if !names.iter().any(|n| n == type_name) {
                return false;
            }
        }
        true
    }

    fn wants_op(&self, op: ChangeOp) -> bool {
        match &self.ops {
            Some(ops) => ops.contains(&op),
            None => true,
        }
    }
}

// ─── Core diff ──────────────────────────────────────────────────────────────

/// Net-current diff between two snapshots.
///
/// Uses a three-level algorithm:
/// 1. Manifest diff — skip unchanged sub-tables
/// 2. Lineage check — same branch → version-column diff; different → ID-based diff
/// 3. Row-level diff
pub async fn diff_snapshots(
    root_uri: &str,
    from: &Snapshot,
    to: &Snapshot,
    filter: &ChangeFilter,
    branch: Option<String>,
) -> Result<ChangeSet> {
    let table_store = TableStore::new(root_uri);
    let mut all_keys: HashSet<String> = HashSet::new();
    for entry in from.entries() {
        all_keys.insert(entry.table_key.clone());
    }
    for entry in to.entries() {
        all_keys.insert(entry.table_key.clone());
    }

    let mut changes = Vec::new();

    for table_key in &all_keys {
        if !filter.matches_table(table_key) {
            continue;
        }

        let from_entry = from.entry(table_key);
        let to_entry = to.entry(table_key);

        // Skip if both snapshots have identical state for this table
        if same_state(from_entry, to_entry) {
            continue;
        }

        let (kind, type_name) = parse_table_key(table_key);
        let is_edge = kind == EntityKind::Edge;

        let table_changes = if from_entry.is_none() {
            // Table added — all rows are inserts
            diff_table_added(&table_store, to, table_key, is_edge, filter).await?
        } else if to_entry.is_none() {
            // Table removed — all rows are deletes
            diff_table_removed(&table_store, from, table_key, is_edge, filter).await?
        } else if same_lineage(from_entry, to_entry) {
            // Fast path: version-column diff
            diff_table_same_lineage(
                &table_store,
                from_entry.unwrap(),
                to_entry.unwrap(),
                is_edge,
                filter,
            )
            .await?
        } else {
            // Cross-branch path: streaming ID-based diff
            diff_table_cross_branch(&table_store, from, to, table_key, is_edge, filter).await?
        };

        for mut c in table_changes {
            c.table_key = table_key.clone();
            c.kind = kind;
            c.type_name = type_name.to_string();
            if c.manifest_version == 0 {
                c.manifest_version = to.version();
            }
            changes.push(c);
        }
    }

    let stats = compute_stats(&changes);
    Ok(ChangeSet {
        from_version: from.version(),
        to_version: to.version(),
        branch,
        changes,
        stats,
    })
}

fn same_state(a: Option<&SubTableEntry>, b: Option<&SubTableEntry>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a.table_version == b.table_version && a.table_branch == b.table_branch
        }
        _ => false,
    }
}

fn same_lineage(from: Option<&SubTableEntry>, to: Option<&SubTableEntry>) -> bool {
    match (from, to) {
        (Some(f), Some(t)) => f.table_branch == t.table_branch,
        _ => false,
    }
}

fn compute_stats(changes: &[EntityChange]) -> ChangeStats {
    let mut stats = ChangeStats::default();
    let mut types = HashSet::new();
    for c in changes {
        match c.op {
            ChangeOp::Insert => stats.inserts += 1,
            ChangeOp::Update => stats.updates += 1,
            ChangeOp::Delete => stats.deletes += 1,
        }
        types.insert(c.type_name.clone());
    }
    stats.types_affected = types.into_iter().collect();
    stats.types_affected.sort();
    stats
}

// ─── Fast path: version-column diff ─────────────────────────────────────────

async fn diff_table_same_lineage(
    table_store: &TableStore,
    from_entry: &SubTableEntry,
    to_entry: &SubTableEntry,
    is_edge: bool,
    filter: &ChangeFilter,
) -> Result<Vec<EntityChange>> {
    let vf = from_entry.table_version;
    let vt = to_entry.table_version;
    let to_ds = table_store.open_at_entry(to_entry).await?;

    let cols: Vec<&str> = if is_edge {
        vec!["id", "src", "dst", "_row_last_updated_at_version"]
    } else {
        vec!["id", "_row_last_updated_at_version"]
    };

    let wants_inserts = filter.wants_op(ChangeOp::Insert);
    let wants_updates = filter.wants_op(ChangeOp::Update);
    let wants_deletes = filter.wants_op(ChangeOp::Delete);

    let mut changes = Vec::new();

    // Inserts + Updates: use _row_last_updated_at_version to find all rows
    // touched since Vf, then classify by checking whether the ID existed at Vf.
    //
    // Why not _row_created_at_version for inserts: Lance's merge_insert stamps
    // new rows with _row_created_at_version = dataset_creation_version (v1),
    // not the merge_insert commit version. This makes _row_created_at_version
    // unreliable for detecting inserts from merge_insert writes. Using
    // _row_last_updated_at_version catches all touched rows regardless of
    // write mode, and ID-set membership distinguishes inserts from updates.
    if wants_inserts || wants_updates {
        let filter_sql = format!(
            "_row_last_updated_at_version > {} AND _row_last_updated_at_version <= {}",
            vf, vt
        );
        let changed_rows = scan_with_filter(table_store, &to_ds, &cols, &filter_sql).await?;

        if !changed_rows.is_empty() {
            // Build the set of IDs that existed at the from version
            let from_ds = table_store.open_at_entry(from_entry).await?;
            let from_ids: HashSet<String> = scan_id_set(table_store, &from_ds, &["id"])
                .await?
                .into_iter()
                .map(|r| r.id)
                .collect();

            for row in changed_rows {
                if from_ids.contains(&row.id) {
                    if wants_updates {
                        changes.push(entity_change_from_row(&row, ChangeOp::Update, is_edge));
                    }
                } else if wants_inserts {
                    changes.push(entity_change_from_row(&row, ChangeOp::Insert, is_edge));
                }
            }
        }
    }

    // Deletes: ID set-difference
    if wants_deletes {
        let from_ds = table_store.open_at_entry(from_entry).await?;
        let deleted = deleted_ids_by_set_diff(table_store, &from_ds, &to_ds, is_edge).await?;
        changes.extend(deleted);
    }

    Ok(changes)
}

// ─── Cross-branch path: streaming ID-based diff ────────────────────────────

async fn diff_table_cross_branch(
    table_store: &TableStore,
    from_snap: &Snapshot,
    to_snap: &Snapshot,
    table_key: &str,
    is_edge: bool,
    filter: &ChangeFilter,
) -> Result<Vec<EntityChange>> {
    let from_ds = table_store
        .open_snapshot_table(from_snap, table_key)
        .await?;
    let to_ds = table_store.open_snapshot_table(to_snap, table_key).await?;

    let from_rows = scan_all_rows_ordered(table_store, &from_ds, is_edge).await?;
    let to_rows = scan_all_rows_ordered(table_store, &to_ds, is_edge).await?;

    let mut changes = Vec::new();
    let mut fi = 0;
    let mut ti = 0;

    while fi < from_rows.len() || ti < to_rows.len() {
        let from_id = from_rows.get(fi).map(|r| r.id.as_str());
        let to_id = to_rows.get(ti).map(|r| r.id.as_str());

        match (from_id, to_id) {
            (Some(fid), Some(tid)) if fid < tid => {
                // ID only in from → Delete
                if filter.wants_op(ChangeOp::Delete) {
                    changes.push(entity_change_from_row(
                        &from_rows[fi],
                        ChangeOp::Delete,
                        is_edge,
                    ));
                }
                fi += 1;
            }
            (Some(fid), Some(tid)) if fid > tid => {
                // ID only in to → Insert
                if filter.wants_op(ChangeOp::Insert) {
                    changes.push(entity_change_from_row(
                        &to_rows[ti],
                        ChangeOp::Insert,
                        is_edge,
                    ));
                }
                ti += 1;
            }
            (Some(_), Some(_)) => {
                // Same ID — check signature
                if from_rows[fi].signature != to_rows[ti].signature
                    && filter.wants_op(ChangeOp::Update)
                {
                    changes.push(entity_change_from_row(
                        &to_rows[ti],
                        ChangeOp::Update,
                        is_edge,
                    ));
                }
                fi += 1;
                ti += 1;
            }
            (Some(_), None) => {
                if filter.wants_op(ChangeOp::Delete) {
                    changes.push(entity_change_from_row(
                        &from_rows[fi],
                        ChangeOp::Delete,
                        is_edge,
                    ));
                }
                fi += 1;
            }
            (None, Some(_)) => {
                if filter.wants_op(ChangeOp::Insert) {
                    changes.push(entity_change_from_row(
                        &to_rows[ti],
                        ChangeOp::Insert,
                        is_edge,
                    ));
                }
                ti += 1;
            }
            (None, None) => break,
        }
    }

    Ok(changes)
}

// ─── Table added/removed ────────────────────────────────────────────────────

async fn diff_table_added(
    table_store: &TableStore,
    to_snap: &Snapshot,
    table_key: &str,
    is_edge: bool,
    filter: &ChangeFilter,
) -> Result<Vec<EntityChange>> {
    if !filter.wants_op(ChangeOp::Insert) {
        return Ok(Vec::new());
    }
    let ds = table_store.open_snapshot_table(to_snap, table_key).await?;
    let rows = scan_all_rows_ordered(table_store, &ds, is_edge).await?;
    Ok(rows
        .into_iter()
        .map(|r| entity_change_from_row(&r, ChangeOp::Insert, is_edge))
        .collect())
}

async fn diff_table_removed(
    table_store: &TableStore,
    from_snap: &Snapshot,
    table_key: &str,
    is_edge: bool,
    filter: &ChangeFilter,
) -> Result<Vec<EntityChange>> {
    if !filter.wants_op(ChangeOp::Delete) {
        return Ok(Vec::new());
    }
    let ds = table_store
        .open_snapshot_table(from_snap, table_key)
        .await?;
    let rows = scan_all_rows_ordered(table_store, &ds, is_edge).await?;
    Ok(rows
        .into_iter()
        .map(|r| entity_change_from_row(&r, ChangeOp::Delete, is_edge))
        .collect())
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Scan with a SQL filter, projecting specific columns.
async fn scan_with_filter(
    table_store: &TableStore,
    ds: &lance::Dataset,
    cols: &[&str],
    filter_sql: &str,
) -> Result<Vec<ScannedRow>> {
    let batches = table_store
        .scan(ds, Some(cols), Some(filter_sql), None)
        .await?;
    Ok(extract_rows(&batches))
}

/// Scan all rows ordered by id, projecting id (+ src/dst for edges) + all columns for signature.
async fn scan_all_rows_ordered(
    table_store: &TableStore,
    ds: &lance::Dataset,
    is_edge: bool,
) -> Result<Vec<ScannedRow>> {
    let batches = table_store
        .scan(
            ds,
            None,
            None,
            Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
        )
        .await?;
    Ok(extract_rows_with_signature(&batches, is_edge))
}

/// Compute deleted IDs: scan id at from and to, set-difference.
async fn deleted_ids_by_set_diff(
    table_store: &TableStore,
    from_ds: &lance::Dataset,
    to_ds: &lance::Dataset,
    is_edge: bool,
) -> Result<Vec<EntityChange>> {
    let cols: Vec<&str> = if is_edge {
        vec!["id", "src", "dst"]
    } else {
        vec!["id"]
    };

    let from_rows = scan_id_set(table_store, from_ds, &cols).await?;
    let to_ids: HashSet<String> = scan_id_set(table_store, to_ds, &["id"])
        .await?
        .into_iter()
        .map(|r| r.id)
        .collect();

    Ok(from_rows
        .into_iter()
        .filter(|r| !to_ids.contains(&r.id))
        .map(|r| entity_change_from_row(&r, ChangeOp::Delete, is_edge))
        .collect())
}

async fn scan_id_set(
    table_store: &TableStore,
    ds: &lance::Dataset,
    cols: &[&str],
) -> Result<Vec<ScannedRow>> {
    let batches = table_store.scan(ds, Some(cols), None, None).await?;
    Ok(extract_rows(&batches))
}

// ─── Row extraction ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ScannedRow {
    id: String,
    src: Option<String>,
    dst: Option<String>,
    signature: String,
    change_version: Option<u64>,
}

fn extract_rows(batches: &[RecordBatch]) -> Vec<ScannedRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let Some(ids) = ids else { continue };
        let srcs = batch
            .column_by_name("src")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let dsts = batch
            .column_by_name("dst")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        for i in 0..ids.len() {
            rows.push(ScannedRow {
                id: ids.value(i).to_string(),
                src: srcs.map(|a| a.value(i).to_string()),
                dst: dsts.map(|a| a.value(i).to_string()),
                signature: String::new(),
                change_version: batch
                    .column_by_name("_row_last_updated_at_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                    .map(|versions| versions.value(i)),
            });
        }
    }
    rows
}

fn extract_rows_with_signature(batches: &[RecordBatch], is_edge: bool) -> Vec<ScannedRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let Some(ids) = ids else { continue };
        let srcs = if is_edge {
            batch
                .column_by_name("src")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        } else {
            None
        };
        let dsts = if is_edge {
            batch
                .column_by_name("dst")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        } else {
            None
        };
        for i in 0..ids.len() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for col in batch.columns() {
                if let Ok(v) = array_value_to_string(col.as_ref(), i) {
                    values.push(v);
                }
            }
            rows.push(ScannedRow {
                id: ids.value(i).to_string(),
                src: srcs.map(|a| a.value(i).to_string()),
                dst: dsts.map(|a| a.value(i).to_string()),
                signature: values.join("\x1f"),
                change_version: batch
                    .column_by_name("_row_last_updated_at_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                    .map(|versions| versions.value(i)),
            });
        }
    }
    rows
}

fn entity_change_from_row(row: &ScannedRow, op: ChangeOp, is_edge: bool) -> EntityChange {
    EntityChange {
        table_key: String::new(),
        kind: if is_edge {
            EntityKind::Edge
        } else {
            EntityKind::Node
        },
        type_name: String::new(),
        id: row.id.clone(),
        op,
        manifest_version: row.change_version.unwrap_or(0),
        endpoints: if is_edge {
            Some(Endpoints {
                src: row.src.clone().unwrap_or_default(),
                dst: row.dst.clone().unwrap_or_default(),
            })
        } else {
            None
        },
    }
}
