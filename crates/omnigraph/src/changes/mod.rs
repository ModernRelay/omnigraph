use std::collections::{HashMap, HashSet};

use arrow_array::{
    Array, BinaryArray, FixedSizeListArray, Float32Array, LargeBinaryArray, RecordBatch,
    StringArray, UInt64Array,
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::DataType;
use lance::dataset::scanner::ColumnOrdering;
use sha2::{Digest, Sha256};

use crate::db::SubTableEntry;
use crate::db::manifest::{Snapshot, TableIdentity};
use crate::error::Result;
use crate::storage_layer::{SnapshotHandle, TableStorage};
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

/// Keyset position in a change list, ordered by `(table_key, id)`.
///
/// The change list is totally ordered on that pair, so a cursor is an exact
/// resume point rather than an offset: it cannot skip or repeat rows if the
/// underlying snapshots are unchanged (and both snapshots are immutable, so
/// they are).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeCursor {
    pub table_key: String,
    pub id: String,
}

#[derive(Debug, Clone, Default)]
pub struct ChangeFilter {
    pub kinds: Option<Vec<EntityKind>>,
    pub type_names: Option<Vec<String>>,
    pub ops: Option<Vec<ChangeOp>>,
    /// Maximum changes to return. `None` is unbounded and is only appropriate
    /// for embedded callers that can absorb the whole change set; every
    /// exposed surface (HTTP, CLI) sets this.
    ///
    /// The bound stops table processing once satisfied, so a small limit does
    /// not walk the remaining tables. It does not yet bound work *within* a
    /// single table — that needs per-table pushdown (RFC-029 §7).
    pub limit: Option<usize>,
    /// Resume strictly after this position. Pair with `limit` to page.
    pub after: Option<ChangeCursor>,
}

#[derive(Debug, Clone, Default)]
pub struct ChangeStats {
    pub inserts: usize,
    pub updates: usize,
    pub deletes: usize,
    pub types_affected: Vec<String>,
}

/// Whole-diff totals with the versions they were computed over.
///
/// The cheap tier's return type — deliberately has no `changes` field, so a
/// summary caller cannot accidentally depend on a row list being present.
#[derive(Debug, Clone)]
pub struct ChangeSummary {
    pub from_version: u64,
    pub to_version: u64,
    pub stats: ChangeStats,
}

#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub from_version: u64,
    pub to_version: u64,
    pub branch: Option<String>,
    /// Totally ordered by `(table_key, id)`.
    pub changes: Vec<EntityChange>,
    /// Statistics for the **returned page**. When `ChangeFilter::limit` is set
    /// these are not whole-diff totals — use [`diff_summary_snapshots`] for
    /// those. This split is deliberate: whole-diff totals cannot be produced
    /// without walking the whole diff, which is exactly what a bounded page
    /// exists to avoid.
    pub stats: ChangeStats,
    /// Set when the page was truncated by `limit`; feed back as
    /// `ChangeFilter::after` to continue. `None` means the change list is
    /// exhausted.
    pub next_cursor: Option<ChangeCursor>,
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

/// Where the walk sends each classified change.
///
/// The two tiers share one walk so they cannot drift in what they count, but
/// only `Rows` retains the change list. `Totals` folds each change into
/// counters and drops it, which is what lets the summary tier answer
/// whole-diff questions without an O(changes) allocation.
enum ChangeSink {
    Rows(Vec<EntityChange>),
    Totals {
        inserts: usize,
        updates: usize,
        deletes: usize,
        types: HashSet<String>,
    },
}

impl ChangeSink {
    fn totals() -> Self {
        Self::Totals {
            inserts: 0,
            updates: 0,
            deletes: 0,
            types: HashSet::new(),
        }
    }

    fn accept(&mut self, change: EntityChange) {
        match self {
            Self::Rows(rows) => rows.push(change),
            Self::Totals {
                inserts,
                updates,
                deletes,
                types,
            } => {
                match change.op {
                    ChangeOp::Insert => *inserts += 1,
                    ChangeOp::Update => *updates += 1,
                    ChangeOp::Delete => *deletes += 1,
                }
                if !types.contains(&change.type_name) {
                    types.insert(change.type_name);
                }
            }
        }
    }

    /// Number of changes accepted so far — the quantity `limit` bounds.
    fn len(&self) -> usize {
        match self {
            Self::Rows(rows) => rows.len(),
            Self::Totals {
                inserts,
                updates,
                deletes,
                ..
            } => inserts + updates + deletes,
        }
    }

    fn into_stats(self) -> ChangeStats {
        match self {
            Self::Rows(rows) => compute_stats(&rows),
            Self::Totals {
                inserts,
                updates,
                deletes,
                types,
            } => {
                let mut types_affected: Vec<String> = types.into_iter().collect();
                types_affected.sort();
                ChangeStats {
                    inserts,
                    updates,
                    deletes,
                    types_affected,
                }
            }
        }
    }
}

/// Net-current diff between two snapshots.
///
/// Uses a three-level algorithm:
/// 1. Manifest diff — skip unchanged sub-tables
/// 2. Lineage check — same branch → version-column diff; different → ID-based diff
/// 3. Row-level diff
pub(crate) async fn diff_snapshots(
    table_store: &TableStore,
    from: &Snapshot,
    to: &Snapshot,
    filter: &ChangeFilter,
    branch: Option<String>,
) -> Result<ChangeSet> {
    let mut sink = ChangeSink::Rows(Vec::new());
    let next_cursor = walk_changes(table_store, from, to, filter, &mut sink).await?;
    let ChangeSink::Rows(changes) = sink else {
        unreachable!("this walk was given the Rows sink")
    };
    let stats = compute_stats(&changes);
    Ok(ChangeSet {
        from_version: from.version(),
        to_version: to.version(),
        branch,
        changes,
        stats,
        next_cursor,
    })
}

/// Walk the change list, sending each classified change to `sink`.
///
/// Returns the resume cursor when `filter.limit` truncated the walk.
async fn walk_changes(
    table_store: &TableStore,
    from: &Snapshot,
    to: &Snapshot,
    filter: &ChangeFilter,
    sink: &mut ChangeSink,
) -> Result<Option<ChangeCursor>> {
    let from_by_identity = from
        .entries()
        .map(|entry| (entry.identity, entry))
        .collect::<HashMap<_, _>>();
    let to_by_identity = to
        .entries()
        .map(|entry| (entry.identity, entry))
        .collect::<HashMap<_, _>>();
    // Resolve each identity to the table_key the change list is ordered by,
    // then walk in that order. This previously iterated a `HashSet`, leaving
    // table order unspecified — both a deny-list violation ("hash-map
    // iteration order in result ordering") and incompatible with the keyset
    // cursor, which needs a total order to resume against.
    let mut ordered: Vec<(&str, TableIdentity)> = from_by_identity
        .keys()
        .chain(to_by_identity.keys())
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .map(|identity| {
            // Prefer the destination alias for a rename; a removed table has
            // only its source alias. Logical pairing never depends on either
            // name — this is presentation and ordering only.
            let table_key = to_by_identity
                .get(&identity)
                .or_else(|| from_by_identity.get(&identity))
                .expect("identity came from one snapshot")
                .table_key
                .as_str();
            (table_key, identity)
        })
        .collect();
    // Identity breaks ties so a duplicated alias cannot reintroduce
    // nondeterminism.
    ordered.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(&b.1)));

    // Tracks the last accepted position so a truncated walk can report where
    // to resume; the sink itself does not necessarily retain rows.
    let mut last_position: Option<ChangeCursor> = None;
    // Set when a change was found that the page had no room for. That is
    // strictly stronger than "the page is full": a page filled exactly to the
    // limit with nothing after it must report no cursor.
    let mut overflowed = false;

    for (_, identity) in ordered {
        if overflowed {
            break;
        }
        let from_entry = from_by_identity.get(&identity).copied();
        let to_entry = to_by_identity.get(&identity).copied();
        // Prefer the destination alias for a rename; a removed table has only
        // its source alias. Logical pairing never depends on either name.
        let table_key = &to_entry
            .or(from_entry)
            .expect("identity came from one snapshot")
            .table_key;
        if !filter.matches_table(table_key) {
            continue;
        }

        // Skip if both snapshots have identical state for this table
        if same_state(from_entry, to_entry) {
            continue;
        }

        // Cursor: whole tables before the resume point are skipped without
        // being opened at all.
        if let Some(after) = &filter.after {
            if table_key.as_str() < after.table_key.as_str() {
                continue;
            }
        }

        let (kind, type_name) = parse_table_key(table_key);
        let is_edge = kind == EntityKind::Edge;

        let table_changes = if from_entry.is_none() {
            // Table added — all rows are inserts
            diff_table_added(table_store, to_entry.unwrap(), is_edge, filter).await?
        } else if to_entry.is_none() {
            // Table removed — all rows are deletes
            diff_table_removed(table_store, from_entry.unwrap(), is_edge, filter).await?
        } else if same_lineage(from_entry, to_entry) {
            // Fast path: version-column diff
            diff_table_same_lineage(
                table_store,
                from_entry.unwrap(),
                to_entry.unwrap(),
                is_edge,
                filter,
            )
            .await?
        } else {
            // Cross-branch path: streaming ID-based diff
            diff_table_cross_branch(
                table_store,
                from_entry.unwrap(),
                to_entry.unwrap(),
                is_edge,
                filter,
            )
            .await?
        };

        let mut table_changes = table_changes;
        // Per-table id order. The cross-branch merge and the added/removed
        // scans already emit in id order, but the same-lineage path appends
        // deletes after inserts/updates, so it does not. Sorting here is what
        // makes `(table_key, id)` a total order over the whole change list.
        table_changes.sort_by(|a, b| a.id.cmp(&b.id));

        for mut c in table_changes {
            // Resume strictly after the cursor within its own table.
            if let Some(after) = &filter.after {
                if table_key.as_str() == after.table_key.as_str() && c.id <= after.id {
                    continue;
                }
            }
            // A change exists but the page is full: stop without accepting it,
            // so the sink never holds more than `limit`.
            if filter.limit.is_some_and(|limit| sink.len() >= limit) {
                overflowed = true;
                break;
            }
            c.table_key = table_key.clone();
            c.kind = kind;
            c.type_name = type_name.to_string();
            if c.manifest_version == 0 {
                c.manifest_version = to.version();
            }
            last_position = Some(ChangeCursor {
                table_key: c.table_key.clone(),
                id: c.id.clone(),
            });
            sink.accept(c);
        }
    }

    Ok(if overflowed { last_position } else { None })
}

/// Whole-diff totals without materializing the change list.
///
/// The scan cost is the same as [`diff_snapshots`] — the rows still have to be
/// classified — but the caller never builds `Vec<EntityChange>`, which is three
/// `String` clones (`table_key`, `type_name`, `id`) plus an optional
/// `Endpoints` per change. On a large branch that is the difference between a
/// summary that fits in memory and one that does not.
pub(crate) async fn diff_summary_snapshots(
    table_store: &TableStore,
    from: &Snapshot,
    to: &Snapshot,
    filter: &ChangeFilter,
) -> Result<ChangeSummary> {
    // Totals are whole-diff by definition, so page bounds must not apply.
    let unbounded = ChangeFilter {
        kinds: filter.kinds.clone(),
        type_names: filter.type_names.clone(),
        ops: filter.ops.clone(),
        limit: None,
        after: None,
    };
    let mut sink = ChangeSink::totals();
    walk_changes(table_store, from, to, &unbounded, &mut sink).await?;
    Ok(ChangeSummary {
        from_version: from.version(),
        to_version: to.version(),
        stats: sink.into_stats(),
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
    let storage: &dyn TableStorage = table_store;
    let to_ds = storage.open_snapshot_at_entry(to_entry).await?;

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
    // We key on _row_last_updated_at_version because one scan over it catches
    // every row touched in the window — inserts and updates alike — regardless
    // of write mode, and ID-set membership at Vf then distinguishes inserts from
    // updates. (lance#6774 made merge_insert stamp new rows' _row_created_at_version
    // with the commit version, so created_at became reliable too; last_updated
    // stays the right key since it also covers updates.)
    if wants_inserts || wants_updates {
        let filter_sql = format!(
            "_row_last_updated_at_version > {} AND _row_last_updated_at_version <= {}",
            vf, vt
        );
        let changed_rows = scan_with_filter(storage, &to_ds, &cols, &filter_sql).await?;

        if !changed_rows.is_empty() {
            // Build the set of IDs that existed at the from version
            let from_ds = storage.open_snapshot_at_entry(from_entry).await?;
            let from_ids: HashSet<String> = scan_id_set(storage, &from_ds, &["id"])
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
        let from_ds = storage.open_snapshot_at_entry(from_entry).await?;
        let deleted = deleted_ids_by_set_diff(storage, &from_ds, &to_ds, is_edge).await?;
        changes.extend(deleted);
    }

    Ok(changes)
}

// ─── Cross-branch path: streaming ID-based diff ────────────────────────────

async fn diff_table_cross_branch(
    table_store: &TableStore,
    from_entry: &SubTableEntry,
    to_entry: &SubTableEntry,
    is_edge: bool,
    filter: &ChangeFilter,
) -> Result<Vec<EntityChange>> {
    let storage: &dyn TableStorage = table_store;
    let from_ds = storage.open_snapshot_at_entry(from_entry).await?;
    let to_ds = storage.open_snapshot_at_entry(to_entry).await?;

    let from_rows = scan_all_rows_ordered(storage, &from_ds, is_edge).await?;
    let to_rows = scan_all_rows_ordered(storage, &to_ds, is_edge).await?;

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
    to_entry: &SubTableEntry,
    is_edge: bool,
    filter: &ChangeFilter,
) -> Result<Vec<EntityChange>> {
    if !filter.wants_op(ChangeOp::Insert) {
        return Ok(Vec::new());
    }
    let storage: &dyn TableStorage = table_store;
    let ds = storage.open_snapshot_at_entry(to_entry).await?;
    // Every row of an added table is unconditionally an insert, so no
    // signature is needed. This previously scanned every column and built a
    // full signature that was then discarded.
    let rows = scan_identity_rows_ordered(storage, &ds, is_edge).await?;
    Ok(rows
        .into_iter()
        .map(|r| entity_change_from_row(&r, ChangeOp::Insert, is_edge))
        .collect())
}

async fn diff_table_removed(
    table_store: &TableStore,
    from_entry: &SubTableEntry,
    is_edge: bool,
    filter: &ChangeFilter,
) -> Result<Vec<EntityChange>> {
    if !filter.wants_op(ChangeOp::Delete) {
        return Ok(Vec::new());
    }
    let storage: &dyn TableStorage = table_store;
    let ds = storage.open_snapshot_at_entry(from_entry).await?;
    // Same as the added case: every row is unconditionally a delete.
    let rows = scan_identity_rows_ordered(storage, &ds, is_edge).await?;
    Ok(rows
        .into_iter()
        .map(|r| entity_change_from_row(&r, ChangeOp::Delete, is_edge))
        .collect())
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Scan with a SQL filter, projecting specific columns.
async fn scan_with_filter(
    storage: &dyn TableStorage,
    ds: &SnapshotHandle,
    cols: &[&str],
    filter_sql: &str,
) -> Result<Vec<ScannedRow>> {
    let batches = storage.scan(ds, Some(cols), Some(filter_sql), None).await?;
    Ok(extract_rows(&batches))
}

/// Scan identity columns only, ordered by id.
///
/// For callers that classify every row the same way (a wholly added or wholly
/// removed table), so no value comparison is required.
async fn scan_identity_rows_ordered(
    storage: &dyn TableStorage,
    ds: &SnapshotHandle,
    is_edge: bool,
) -> Result<Vec<ScannedRow>> {
    let cols: Vec<&str> = if is_edge {
        vec!["id", "src", "dst"]
    } else {
        vec!["id"]
    };
    let batches = storage
        .scan(
            ds,
            Some(&cols),
            None,
            Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
        )
        .await?;
    Ok(extract_rows(&batches))
}

/// Scan all rows ordered by id, projecting id (+ src/dst for edges) + all columns for signature.
async fn scan_all_rows_ordered(
    storage: &dyn TableStorage,
    ds: &SnapshotHandle,
    is_edge: bool,
) -> Result<Vec<ScannedRow>> {
    let batches = storage
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
    storage: &dyn TableStorage,
    from_ds: &SnapshotHandle,
    to_ds: &SnapshotHandle,
    is_edge: bool,
) -> Result<Vec<EntityChange>> {
    let cols: Vec<&str> = if is_edge {
        vec!["id", "src", "dst"]
    } else {
        vec!["id"]
    };

    let from_rows = scan_id_set(storage, from_ds, &cols).await?;
    let to_ids: HashSet<String> = scan_id_set(storage, to_ds, &["id"])
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
    storage: &dyn TableStorage,
    ds: &SnapshotHandle,
    cols: &[&str],
) -> Result<Vec<ScannedRow>> {
    let batches = storage.scan(ds, Some(cols), None, None).await?;
    Ok(extract_rows(&batches))
}

// ─── Row extraction ─────────────────────────────────────────────────────────

/// Fixed-width digest of a row's user-visible column values.
///
/// This used to be the stringified columns joined together and retained per
/// row, so a table with a `Vector(1536)` or a `Blob` column paid the full
/// rendered width of every cell for every row of *both* snapshots. Comparison
/// only ever asked "are these two rows equal", so a digest answers the same
/// question in 16 bytes.
type RowSignature = [u8; 16];

const EMPTY_SIGNATURE: RowSignature = [0u8; 16];

#[derive(Debug, Clone)]
struct ScannedRow {
    id: String,
    src: Option<String>,
    dst: Option<String>,
    signature: RowSignature,
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
                // Callers of this path never compare signatures.
                signature: EMPTY_SIGNATURE,
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
            let mut hasher = Sha256::new();
            for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
                if field.name().starts_with("_row_") {
                    continue;
                }
                // Field-separated so ("ab","c") and ("a","bc") differ.
                hasher.update([0x1f]);
                match col.data_type() {
                    // Wide binary payloads: hash the raw bytes rather than
                    // paying `array_value_to_string` to render them. This is
                    // the allocation the digest exists to avoid.
                    DataType::FixedSizeList(_, _) | DataType::LargeBinary | DataType::Binary => {
                        hash_raw_cell(&mut hasher, col.as_ref(), i);
                    }
                    // Everything else keeps the exact rendering the previous
                    // string signature compared, so classification is
                    // unchanged.
                    _ => {
                        if let Ok(v) = array_value_to_string(col.as_ref(), i) {
                            hasher.update(v.as_bytes());
                        }
                    }
                }
            }
            let digest = hasher.finalize();
            let mut signature = EMPTY_SIGNATURE;
            signature.copy_from_slice(&digest[..16]);

            rows.push(ScannedRow {
                id: ids.value(i).to_string(),
                src: srcs.map(|a| a.value(i).to_string()),
                dst: dsts.map(|a| a.value(i).to_string()),
                signature,
                change_version: batch
                    .column_by_name("_row_last_updated_at_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                    .map(|versions| versions.value(i)),
            });
        }
    }
    rows
}

/// Hash a wide binary cell without rendering it to a string.
///
/// Deliberately goes through the typed accessors rather than the underlying
/// buffers: `FixedSizeListArray::value` returns a *slice* of the shared child
/// array, so hashing its raw buffers would ignore the slice offset and make
/// distinct rows collide. Iterating the typed values respects offset and
/// length.
fn hash_raw_cell(hasher: &mut Sha256, col: &dyn Array, i: usize) {
    if col.is_null(i) {
        hasher.update([0x00]);
        return;
    }
    match col.data_type() {
        DataType::LargeBinary => {
            if let Some(a) = col.as_any().downcast_ref::<LargeBinaryArray>() {
                hasher.update(a.value(i));
                return;
            }
        }
        DataType::Binary => {
            if let Some(a) = col.as_any().downcast_ref::<BinaryArray>() {
                hasher.update(a.value(i));
                return;
            }
        }
        DataType::FixedSizeList(_, _) => {
            if let Some(a) = col.as_any().downcast_ref::<FixedSizeListArray>() {
                let child = a.value(i);
                if let Some(floats) = child.as_any().downcast_ref::<Float32Array>() {
                    for value in floats.iter() {
                        match value {
                            Some(v) => hasher.update(v.to_le_bytes()),
                            None => hasher.update([0xff]),
                        }
                    }
                    return;
                }
            }
        }
        _ => {}
    }
    // Unrecognized shape: fall back to the rendering the previous signature
    // used, so an unexpected type is still compared rather than silently
    // treated as unchanged.
    if let Ok(v) = array_value_to_string(col, i) {
        hasher.update(v.as_bytes());
    }
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
