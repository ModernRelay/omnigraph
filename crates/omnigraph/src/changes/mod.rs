pub(crate) mod enumerate;
pub(crate) mod feed;
pub mod model;
pub(crate) mod row_compare;
pub(crate) mod token;

pub use model::{
    CHANGE_FEED_DEFAULT_COMMITS_PER_POLL, CHANGE_FEED_MAX_COMMITS_PER_POLL,
    COMMIT_CHANGES_DEFAULT_BYTES, COMMIT_CHANGES_DEFAULT_ROWS, COMMIT_CHANGES_MAX_BYTES,
    COMMIT_CHANGES_MAX_ROWS, ChangeBaseline, ChangeCause, ChangeEntityKind, ChangeFeedContinuation,
    ChangeFeedPage, ChangeFeedPosition, ChangeFeedRequest, ChangeFeedScope, ChangeFeedStart,
    ChangeOpKind, CommitChangesPage, EntityEndpoints, EntityImage, GraphChangeBlock,
    GraphEntityChange, GraphTypeRef,
};

use std::collections::{BTreeMap, HashSet};

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};

use self::row_compare::{OrderedRows, RawRow, rows_equal, user_schema_fingerprint};
use crate::db::SubTableEntry;
use crate::db::manifest::{Snapshot, TableIdentity};
use crate::error::{OmniError, Result};
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

/// One immutable table lifetime whose physical state differs between two
/// graph snapshots.
///
/// Identity, not alias, pairs the endpoints. A rename therefore stays one
/// interval (and is elided when its physical state did not move), while a
/// drop/re-add under the same public name remains two distinct lifetimes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TableChangeInterval<'a> {
    pub(crate) identity: TableIdentity,
    pub(crate) from: Option<&'a SubTableEntry>,
    pub(crate) to: Option<&'a SubTableEntry>,
}

impl<'a> TableChangeInterval<'a> {
    fn table_key(&self) -> &'a str {
        &self
            .to
            .or(self.from)
            .expect("a changed interval has at least one endpoint")
            .table_key
    }
}

/// Derive changed table lifetimes in stable graph-visible order: destination
/// alias (or source alias for a removal), then immutable identity.
///
/// This is the graph-commit CDC pruning layer: later row enumeration only
/// needs to inspect these exact endpoint pairs. It persists no parallel change
/// log and does not infer identity from an alias, path, or Lance version.
pub(crate) fn changed_table_intervals<'a>(
    from: &'a Snapshot,
    to: &'a Snapshot,
) -> Vec<TableChangeInterval<'a>> {
    let mut by_identity =
        BTreeMap::<TableIdentity, (Option<&'a SubTableEntry>, Option<&'a SubTableEntry>)>::new();
    for entry in from.entries() {
        by_identity.entry(entry.identity).or_default().0 = Some(entry);
    }
    for entry in to.entries() {
        by_identity.entry(entry.identity).or_default().1 = Some(entry);
    }

    let mut intervals = by_identity
        .into_iter()
        .filter_map(|(identity, (from, to))| {
            (!same_state(from, to)).then_some(TableChangeInterval { identity, from, to })
        })
        .collect::<Vec<_>>();
    intervals.sort_by(|left, right| {
        left.table_key()
            .cmp(right.table_key())
            .then_with(|| left.identity.cmp(&right.identity))
    });
    intervals
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
    // Graph-vocabulary identity of the `to` side, used only to name a
    // cross-branch schema boundary. `None`/empty when the target is a raw
    // snapshot with no resolved commit.
    to_commit_id: Option<String>,
) -> Result<ChangeSet> {
    let mut changes = Vec::new();

    for interval in changed_table_intervals(from, to) {
        let from_entry = interval.from;
        let to_entry = interval.to;
        // Prefer the destination alias for a rename; a removed table has only
        // its source alias. Logical pairing never depends on either name.
        let table_key = &to_entry
            .or(from_entry)
            .expect("identity came from one snapshot")
            .table_key;
        debug_assert!(
            from_entry
                .into_iter()
                .chain(to_entry)
                .all(|entry| entry.identity == interval.identity),
            "table interval endpoints must retain their immutable identity"
        );
        if !filter.matches_table(table_key) {
            continue;
        }

        let (kind, type_name) = parse_table_key(table_key);
        let is_edge = kind == EntityKind::Edge;

        let table_changes = match (from_entry, to_entry) {
            // Table added — all rows are inserts
            (None, Some(to)) => diff_table_added(table_store, to, is_edge, filter).await?,
            // Table removed — all rows are deletes
            (Some(from), None) => diff_table_removed(table_store, from, is_edge, filter).await?,
            // Fast path: version-column diff
            (Some(from), Some(to)) if same_lineage(from_entry, to_entry) => {
                diff_table_same_lineage(table_store, from, to, is_edge, filter).await?
            }
            // Cross-branch path: streaming ID-based diff
            (Some(from), Some(to)) => {
                diff_table_cross_branch(
                    table_store,
                    from,
                    to,
                    is_edge,
                    filter,
                    type_name,
                    to_commit_id.as_deref().unwrap_or_default(),
                )
                .await?
            }
            // Unreachable: `same_state` above already skipped absent-on-both-sides tables.
            (None, None) => continue,
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
    type_name: &str,
    to_commit_id: &str,
) -> Result<Vec<EntityChange>> {
    // Stream both snapshots id-ordered and merge them, using the SAME typed
    // row-equality that the per-commit enumerator uses (`row_compare`). The
    // former display-string signature rendered null and `""` identically and
    // skipped every `_row_`-prefixed column, so a `""`↔null flip or a legal
    // `_row_notes` change was silently dropped; typed Arrow comparison keeps
    // them distinct and only skips the five reserved virtual columns.
    let from_dataset = table_store.open_at_entry(from_entry).await?;
    let to_dataset = table_store.open_at_entry(to_entry).await?;

    // Schema-boundary gate, symmetric with the per-commit enumerator. The typed
    // row equality below walks the left row's fields, so it is only sound when
    // both sides share one user schema. Today no two branch lifetimes of one
    // table can diverge in user schema (schema apply requires a graph with only
    // main, so a branch cannot be evolved separately), which is why this gate
    // does not fire on any supported operation. It is load-bearing for future
    // branch-scoped schema evolution: it turns a divergent-schema pair into a
    // loud typed refusal instead of a silently dropped update (extra column on
    // the right) or a `manifest_internal` error (extra column on the left).
    if user_schema_fingerprint(&from_dataset) != user_schema_fingerprint(&to_dataset) {
        return Err(OmniError::ChangeSchemaBoundary {
            graph_commit_id: to_commit_id.to_string(),
            type_name: type_name.to_string(),
        });
    }

    let mut from = OrderedRows::open(from_dataset, None).await?;
    let mut to = OrderedRows::open(to_dataset, None).await?;

    let mut changes = Vec::new();
    loop {
        let from_id = from.peek().await?.map(|row| row.id.clone());
        let to_id = to.peek().await?.map(|row| row.id.clone());
        match (from_id, to_id) {
            (None, None) => break,
            // ID only in from → Delete
            (Some(_), None) => {
                let row = from.pop().await?.expect("peeked row present");
                if filter.wants_op(ChangeOp::Delete) {
                    changes.push(entity_change_from_raw(&row, ChangeOp::Delete, is_edge));
                }
            }
            // ID only in to → Insert
            (None, Some(_)) => {
                let row = to.pop().await?.expect("peeked row present");
                if filter.wants_op(ChangeOp::Insert) {
                    changes.push(entity_change_from_raw(&row, ChangeOp::Insert, is_edge));
                }
            }
            (Some(fid), Some(tid)) if fid < tid => {
                let row = from.pop().await?.expect("peeked row present");
                if filter.wants_op(ChangeOp::Delete) {
                    changes.push(entity_change_from_raw(&row, ChangeOp::Delete, is_edge));
                }
            }
            (Some(fid), Some(tid)) if fid > tid => {
                let row = to.pop().await?.expect("peeked row present");
                if filter.wants_op(ChangeOp::Insert) {
                    changes.push(entity_change_from_raw(&row, ChangeOp::Insert, is_edge));
                }
            }
            // Same ID — typed structural comparison, Blob-descriptor aware. The
            // managed-Blob identity is qualified by the immutable data-file UUID,
            // which is globally unique, so cross-branch equality is sound without
            // a scope hint.
            (Some(_), Some(_)) => {
                let left = from.pop().await?.expect("peeked row present");
                let right = to.pop().await?.expect("peeked row present");
                if filter.wants_op(ChangeOp::Update)
                    && !rows_equal(from.dataset(), &left, to.dataset(), &right).await?
                {
                    changes.push(entity_change_from_raw(&right, ChangeOp::Update, is_edge));
                }
            }
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
    drain_all_rows(table_store, to_entry, ChangeOp::Insert, is_edge).await
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
    drain_all_rows(table_store, from_entry, ChangeOp::Delete, is_edge).await
}

/// Enumerate every row of one table snapshot as the given op. Used for an
/// added table (all inserts) or a removed one (all deletes); the streamed
/// `OrderedRows` shares the enumerator's scan shape so there is one row reader.
async fn drain_all_rows(
    table_store: &TableStore,
    entry: &SubTableEntry,
    op: ChangeOp,
    is_edge: bool,
) -> Result<Vec<EntityChange>> {
    let dataset = table_store.open_at_entry(entry).await?;
    let mut rows = OrderedRows::open(dataset, None).await?;
    let mut changes = Vec::new();
    while let Some(row) = rows.pop().await? {
        changes.push(entity_change_from_raw(&row, op, is_edge));
    }
    Ok(changes)
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

#[derive(Debug, Clone)]
struct ScannedRow {
    id: String,
    src: Option<String>,
    dst: Option<String>,
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
                change_version: batch
                    .column_by_name("_row_last_updated_at_version")
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
                    .map(|versions| versions.value(i)),
            });
        }
    }
    rows
}

/// Build a change from a scanned key row (same-lineage inserts/updates and the
/// deleted-set path, which classify by id membership and never compare images).
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

/// Build a change from a typed comparison row (the cross-branch path and the
/// added/removed enumerations). Endpoints and the change version are read
/// directly from the one-row slice; a missing version leaves 0, which
/// `diff_snapshots` fills with the destination snapshot version.
fn entity_change_from_raw(raw: &RawRow, op: ChangeOp, is_edge: bool) -> EntityChange {
    let string_col = |name: &str| -> Option<String> {
        raw.slice
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .filter(|array| array.is_valid(0))
            .map(|array| array.value(0).to_string())
    };
    let change_version = raw
        .slice
        .column_by_name("_row_last_updated_at_version")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .filter(|array| array.is_valid(0))
        .map(|array| array.value(0));
    EntityChange {
        table_key: String::new(),
        kind: if is_edge {
            EntityKind::Edge
        } else {
            EntityKind::Node
        },
        type_name: String::new(),
        id: raw.id.clone(),
        op,
        manifest_version: change_version.unwrap_or(0),
        endpoints: if is_edge {
            Some(Endpoints {
                src: string_col("src").unwrap_or_default(),
                dst: string_col("dst").unwrap_or_default(),
            })
        } else {
            None
        },
    }
}
