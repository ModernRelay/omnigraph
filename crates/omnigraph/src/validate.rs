//! Integrity validation of a write's delta against the committed image.
//!
//! The constraints (value and enum, uniqueness, edge referential integrity,
//! cardinality) are declared once in the schema; the planner derives the
//! checks of every write (`omnigraph_compiler::ir::checks_for`) and decides
//! how each committed lookup reads ([`plan::plan_write`], the `load csv`
//! plan), and the one judge in [`load`] executes them: the `load csv`
//! operator over its partitions, the staged routes (a JSONL load, a
//! mutation, a blob cell, a branch merge) over their batches in memory.
//!
//! This module holds what the judge reads: [`CommittedState`], the committed
//! view behind the lookups, with one open per table shared between them,
//! and the `@card` count every route shares ([`evaluate_cardinality`]).
//! Δ-scoping: row-local checks read the delta alone, the cross-row checks
//! read the delta against the committed table under the plan's access.

mod kernels;
mod key_set;
pub(crate) mod load;
pub(crate) mod plan;

use std::collections::{HashMap, HashSet};

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::prelude::{Expr, col, ident, lit};
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::EdgeType;

use crate::db::{Omnigraph, Snapshot};
use crate::error::{MergeConflict, MergeConflictKind, OmniError, Result};
use crate::table_store::{ID_SCAN_BATCH_BYTES, ID_SCAN_BATCH_ROWS, TableStore};
use kernels::{CardPlan, HoldersByKey, card_affected, card_violations, match_unique_holders};
use key_set::{IntKeySet, IntKeySetLayout, canonical_i64, key_bytes};

/// A single integrity violation, surface-neutral: a load or a mutation
/// refuses with its message ([`Violation::into_omni_error`]), a branch merge
/// lists it with its kind ([`Violation::into_merge_conflict`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Violation {
    pub table_key: String,
    pub row_id: Option<String>,
    pub kind: MergeConflictKind,
    pub message: String,
}

impl Violation {
    pub(crate) fn into_merge_conflict(self) -> MergeConflict {
        MergeConflict {
            type_key: self.table_key,
            entity_id: self.row_id,
            kind: self.kind,
            message: self.message,
        }
    }

    /// Map to the write-path surface error. The message already matches the
    /// write path's text (`validators.rs` asserts via `.contains`).
    pub(crate) fn into_omni_error(self) -> OmniError {
        OmniError::manifest(self.message)
    }
}

/// Per-table change a branch merge computes for its target: the rows added
/// or changed (as batches, projected to what the checks read) plus the ids
/// removed. The merge's delta container, planned table by table.
#[derive(Debug, Default)]
pub(crate) struct TableChange {
    /// Rows new in the result (absent from base).
    pub added: Vec<RecordBatch>,
    /// Rows present before but with changed values.
    pub changed: Vec<RecordBatch>,
    /// Ids removed in the result.
    pub deleted_ids: Vec<String>,
}

impl TableChange {
    /// Batches carrying field values a row-local constraint must check
    /// (`added ∪ changed`). Unchanged rows were validated at write time.
    pub fn value_batches(&self) -> impl Iterator<Item = &RecordBatch> {
        self.added.iter().chain(self.changed.iter())
    }
}

/// Per-table changes keyed by `table_key` (`node:Type` / `edge:Type`).
pub(crate) type ChangeSet = HashMap<String, TableChange>;

/// Wrap a leaf-check error as a value-constraint [`Violation`]; the message
/// is the leaf's own text, which every route's refusal carries verbatim.
fn value_violation(table_key: &str, err: OmniError) -> Violation {
    Violation {
        table_key: table_key.to_string(),
        row_id: None,
        kind: MergeConflictKind::ValueConstraintViolation,
        message: err.to_string(),
    }
}

/// How a constraint reads the committed rows it checks the delta against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AccessPath {
    /// Filtered scans carrying the delta's keys, served by the scalar index.
    Probe,
    /// One projected scan of the committed table, matched in memory.
    Scan,
}

/// Keys per batched committed probe: uniqueness holders and `@card` edges.
/// Bounds the pushed-down filter size, since a merge delta is not row-capped
/// and a load holds many keyed batches of `KEYED_WRITE_MAX_ROWS` rows.
const PROBE_CHUNK_KEYS: usize = 8_192;

/// Index-backed view of committed target state for the merge delta's lookups.
/// Every method reads the (indexed) target table via a structured `filter_expr`
/// so Lance serves it from the BTREE (index-search → take) rather than a full
/// scan — with the documented uncovered-fragment-tail caveat (a stale index
/// degrades to a tail scan; correctness is unaffected).
pub(crate) struct CommittedState<'a> {
    /// The committed view for existence / uniqueness / cardinality lookups: the
    /// merge target snapshot, the write path's pinned base, or the loader's
    /// pinned pre-load base. `None` means EMPTY — an `Overwrite` load, where the
    /// batch is the whole new image and no prior committed row survives.
    committed: Option<&'a Snapshot>,
    /// Tables whose committed view is EMPTY because this op replaces them: the
    /// tables an `Overwrite` load touches. `Overwrite` is PER-TABLE (a table
    /// absent from the load batch is retained), so this is the set of touched
    /// tables, not a global flag — an edges-only overwrite still sees committed
    /// nodes for RI. Empty on the merge / mutation / append / merge-load paths.
    overwritten: HashSet<String>,
    /// Write path only: open edge tables from a fresh graph-branch manifest
    /// snapshot for `@card` (the #298 stale-handle fix). This is the live
    /// committed graph view, not a raw Lance HEAD that may be unpublished or
    /// belong to an inherited source ref. `None` on merge/load.
    live: Option<(&'a Omnigraph, Option<&'a str>)>,
    /// The committed datasets this validation has opened, one open per
    /// table: the access choice and the lookups share the handle.
    opened: std::sync::Mutex<HashMap<String, Dataset>>,
}

impl<'a> CommittedState<'a> {
    async fn open(&self, table_key: &str) -> Result<Option<Dataset>> {
        if self.overwritten.contains(table_key) {
            return Ok(None);
        }
        let Some(committed) = self.committed else {
            return Ok(None);
        };
        if committed.dataset(table_key).is_none() {
            return Ok(None);
        }
        if let Some(ds) = self.opened().get(table_key) {
            return Ok(Some(ds.clone()));
        }
        let ds = committed.open_lance_dataset(table_key).await?;
        self.opened().insert(table_key.to_string(), ds.clone());
        Ok(Some(ds))
    }

    fn opened(&self) -> std::sync::MutexGuard<'_, HashMap<String, Dataset>> {
        self.opened
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Open an edge table for cardinality counting: the current manifest-visible
    /// graph-branch snapshot on the write path (so a concurrent published edge
    /// is counted — #298), the pinned committed snapshot otherwise. Resolving
    /// through the fresh graph snapshot is load-bearing for first-touch named
    /// branches: their table still inherits another Lance ref until this write
    /// forks it, so opening the target ref directly would be invalid.
    async fn open_cardinality(&self, table_key: &str) -> Result<Option<Dataset>> {
        if self.overwritten.contains(table_key) {
            return Ok(None);
        }
        let Some(committed) = self.committed else {
            return Ok(None);
        };
        let Some(_entry) = committed.dataset(table_key) else {
            return Ok(None);
        };
        match self.live {
            Some((db, branch)) => {
                let live = db.fresh_snapshot_for_branch_unchecked(branch).await?;
                match live.dataset(table_key) {
                    Some(_) => Ok(Some(live.open_lance_dataset(table_key).await?)),
                    None => Ok(None),
                }
            }
            None => Ok(Some(committed.open_lance_dataset(table_key).await?)),
        }
    }

    /// Which of `ids` exist as committed rows in `table_key` (by `id`): one
    /// `id IN (...)` filtered scan on `Probe`, one bounded scan of `id` matched
    /// in memory on `Scan`.
    async fn existing_ids(
        &self,
        table_key: &str,
        ids: &[String],
        system_columns: SystemColumns,
        access: AccessPath,
    ) -> Result<HashSet<String>> {
        let Some(ds) = self.open(table_key).await? else {
            return Ok(HashSet::new());
        };
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let mut present = HashSet::new();
        match access {
            AccessPath::Probe => {
                let expr = col(system_columns.id)
                    .in_list(ids.iter().map(|k| lit(k.clone())).collect(), false);
                let batches = scan_filtered(&ds, &[system_columns.id], expr).await?;
                for batch in &batches {
                    let column = string_col(batch, system_columns.id)?;
                    for i in 0..column.len() {
                        if !column.is_null(i) {
                            present.insert(column.value(i).to_string());
                        }
                    }
                }
            }
            AccessPath::Scan => {
                let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
                let mut stream = scan_bounded(&ds, &[system_columns.id]).await?;
                while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
                    let column = string_col(&batch, system_columns.id)?;
                    for i in 0..column.len() {
                        if !column.is_null(i) && wanted.contains(column.value(i)) {
                            present.insert(column.value(i).to_string());
                        }
                    }
                }
            }
        }
        Ok(present)
    }

    /// The committed `id`s of `table_key` among `wanted` that spell canonical
    /// integers, as one probeable key set: `Probe` filters the read by `wanted`,
    /// `Scan` reads the id column once and keeps only the wanted ids.
    async fn existing_int_keys(
        &self,
        table_key: &str,
        wanted: &[String],
        system_columns: SystemColumns,
        access: AccessPath,
        admit: &mut load::ByteAdmission<'_>,
    ) -> Result<(IntKeySet, Box<dyn Send>)> {
        let ds = self.open(table_key).await?;
        let bound = match &ds {
            Some(ds) => {
                let rows = ds.count_rows(None).await.map_err(OmniError::storage)?;
                rows.min(wanted.len())
            }
            None => 0,
        };
        let _input_memory = admit(key_bytes(bound).ok_or_else(|| integer_capacity_error(bound))?)?;
        let mut keys = Vec::with_capacity(bound);
        if let Some(ds) = ds.filter(|_| bound > 0) {
            match access {
                AccessPath::Probe => {
                    let expr = col(system_columns.id)
                        .in_list(wanted.iter().map(|k| lit(k.clone())).collect(), false);
                    for batch in &scan_filtered(&ds, &[system_columns.id], expr).await? {
                        push_int_ids(batch, system_columns.id, &mut keys, bound, &|_| true)?;
                    }
                }
                AccessPath::Scan => {
                    let wanted: HashSet<i64> =
                        wanted.iter().filter_map(|key| canonical_i64(key)).collect();
                    scan_int_ids(&ds, system_columns.id, &mut keys, bound, &|key| {
                        wanted.contains(&key)
                    })
                    .await?
                }
            }
        }
        let layout =
            IntKeySetLayout::new(keys.len()).ok_or_else(|| integer_capacity_error(keys.len()))?;
        let retained = admit(layout.retained_bytes)?;
        let _scratch = admit(layout.scratch_bytes)?;
        Ok((IntKeySet::build(&keys, layout), retained))
    }

    /// Committed holders of the `columns` tuples in `table_key`, holder lists in
    /// scan order: `Probe` filters per [`PROBE_CHUNK_KEYS`] chunk, `Scan`
    /// reads the id and group columns once; both match exact tuples in Rust.
    async fn unique_holders(
        &self,
        table_key: &str,
        columns: &[String],
        keys: &[(Vec<String>, Vec<ScalarValue>)],
        system_columns: SystemColumns,
        access: AccessPath,
    ) -> Result<HoldersByKey> {
        let mut holders = HoldersByKey::new();
        if keys.is_empty() || columns.is_empty() {
            return Ok(holders);
        }
        let Some(ds) = self.open(table_key).await? else {
            return Ok(holders);
        };
        let projection: Vec<&str> = std::iter::once(system_columns.id)
            .chain(columns.iter().map(String::as_str))
            .collect();
        match access {
            AccessPath::Probe => {
                for chunk in keys.chunks(PROBE_CHUNK_KEYS) {
                    let wanted: HashSet<&Vec<String>> =
                        chunk.iter().map(|(canonical, _)| canonical).collect();
                    let batches =
                        scan_filtered(&ds, &projection, unique_probe_filter(columns, chunk))
                            .await?;
                    for batch in &batches {
                        match_unique_holders(
                            batch,
                            table_key,
                            columns,
                            system_columns,
                            &wanted,
                            &mut holders,
                        )?;
                    }
                }
            }
            AccessPath::Scan => {
                let wanted: HashSet<&Vec<String>> =
                    keys.iter().map(|(canonical, _)| canonical).collect();
                let mut stream = scan_bounded(&ds, &projection).await?;
                while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
                    match_unique_holders(
                        &batch,
                        table_key,
                        columns,
                        system_columns,
                        &wanted,
                        &mut holders,
                    )?;
                }
            }
        }
        Ok(holders)
    }

    /// Committed edges `(id, src)` in `edge_table` whose id is one of `ids`,
    /// probed in filtered scans of at most [`PROBE_CHUNK_KEYS`] ids each. An id
    /// names at most one edge, so the result is bounded by `ids`.
    async fn committed_edges_by_id(
        &self,
        edge_table: &str,
        ids: &[String],
        system_columns: SystemColumns,
    ) -> Result<Vec<(String, String)>> {
        // No keys, no open: on the write path an open refreshes the branch
        // snapshot.
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let Some(ds) = self.open_cardinality(edge_table).await? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for chunk in ids.chunks(PROBE_CHUNK_KEYS) {
            let expr = col(system_columns.id)
                .in_list(chunk.iter().map(|id| lit(id.clone())).collect(), false);
            let batches =
                scan_filtered(&ds, &[system_columns.id, system_columns.src], expr).await?;
            for batch in &batches {
                let ids = string_col(batch, system_columns.id)?;
                let srcs = string_col(batch, system_columns.src)?;
                for i in 0..batch.num_rows() {
                    out.push((ids.value(i).to_string(), srcs.value(i).to_string()));
                }
            }
        }
        Ok(out)
    }

    /// Committed edges of `edge_table` per source among `srcs`, an edge whose
    /// id `excluded` names (removed or re-placed by the change) left out.
    /// The sources are probed in sorted filtered scans of at most
    /// [`PROBE_CHUNK_KEYS`] each and the edges stream past batch by batch, so
    /// memory holds one count per source, never the edges themselves.
    async fn committed_source_counts(
        &self,
        edge_table: &str,
        srcs: &HashSet<String>,
        system_columns: SystemColumns,
        excluded: impl Fn(&str) -> bool,
    ) -> Result<HashMap<String, u32>> {
        let mut counts = HashMap::new();
        if srcs.is_empty() {
            return Ok(counts);
        }
        let Some(ds) = self.open_cardinality(edge_table).await? else {
            return Ok(counts);
        };
        let mut sorted: Vec<&String> = srcs.iter().collect();
        sorted.sort();
        for chunk in sorted.chunks(PROBE_CHUNK_KEYS) {
            let expr = col(system_columns.src)
                .in_list(chunk.iter().map(|src| lit((*src).clone())).collect(), false);
            let projection = [system_columns.id, system_columns.src];
            let mut stream = TableStore::scan_stream_with(
                &ds,
                Some(&projection),
                None,
                None,
                false,
                move |scanner| {
                    scanner.filter_expr(expr);
                    Ok(())
                },
            )
            .await?;
            while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
                let ids = string_col(&batch, system_columns.id)?;
                let batch_srcs = string_col(&batch, system_columns.src)?;
                for i in 0..batch.num_rows() {
                    if excluded(ids.value(i)) {
                        continue;
                    }
                    let count = counts
                        .entry(batch_srcs.value(i).to_string())
                        .or_insert(0u32);
                    *count = count.checked_add(1).ok_or_else(|| {
                        OmniError::manifest_internal("@card edge count overflows u32")
                    })?;
                }
            }
        }
        Ok(counts)
    }

    /// The smallest source in `edge_table`'s image before this write that
    /// `reject` names, read by one bounded scan of the src column: the
    /// replaced image of an overwritten table, which its committed view hides.
    async fn first_replaced_source(
        &self,
        edge_table: &str,
        system_columns: SystemColumns,
        reject: impl Fn(&str) -> bool,
    ) -> Result<Option<String>> {
        let Some(committed) = self.committed else {
            return Ok(None);
        };
        if committed.dataset(edge_table).is_none() {
            return Ok(None);
        }
        let ds = committed.open_lance_dataset(edge_table).await?;
        let mut stream = scan_bounded(&ds, &[system_columns.src]).await?;
        let mut first: Option<String> = None;
        while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
            let srcs = string_col(&batch, system_columns.src)?;
            for row in 0..srcs.len() {
                if srcs.is_null(row) {
                    continue;
                }
                let src = srcs.value(row);
                if reject(src) && first.as_deref().is_none_or(|smallest| src < smallest) {
                    first = Some(src.to_string());
                }
            }
        }
        Ok(first)
    }

    /// Committed edges `(id, src, dst)` of `edge_table` whose src is in
    /// `src_nodes` or dst in `dst_nodes`, the edges a node removal would strand;
    /// `limit_one` reads the first alone when no delta or deletion can exempt it.
    async fn edges_referencing(
        &self,
        edge_table: &str,
        src_nodes: &[String],
        dst_nodes: &[String],
        system_columns: SystemColumns,
        limit_one: bool,
        admit: &mut (dyn FnMut(&RecordBatch) -> Result<()> + Send),
    ) -> Result<Vec<(String, String, String)>> {
        if src_nodes.is_empty() && dst_nodes.is_empty() {
            return Ok(Vec::new());
        }
        let Some(ds) = self.open(edge_table).await? else {
            return Ok(Vec::new());
        };
        let mut expr: Option<Expr> = None;
        if !src_nodes.is_empty() {
            expr = Some(
                col(system_columns.src)
                    .in_list(src_nodes.iter().map(|k| lit(k.clone())).collect(), false),
            );
        }
        if !dst_nodes.is_empty() {
            let dst = col(system_columns.dst)
                .in_list(dst_nodes.iter().map(|k| lit(k.clone())).collect(), false);
            expr = Some(match expr {
                Some(acc) => acc.or(dst),
                None => dst,
            });
        }
        let mut stream = TableStore::scan_stream_with(
            &ds,
            Some(&[system_columns.id, system_columns.src, system_columns.dst]),
            None,
            None,
            false,
            move |scanner| {
                scanner.filter_expr(expr.expect("at least one endpoint has removed ids"));
                if limit_one {
                    scanner.limit(1)?;
                    scanner.batch_size(1);
                    scanner.batch_size_bytes(ID_SCAN_BATCH_BYTES);
                    scanner.batch_readahead(1);
                }
                Ok(())
            },
        )
        .await?;
        let mut out = Vec::new();
        while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
            if batch.num_rows() == 0 {
                continue;
            }
            admit(&batch)?;
            let ids = string_col(&batch, system_columns.id)?;
            let srcs = string_col(&batch, system_columns.src)?;
            let dsts = string_col(&batch, system_columns.dst)?;
            for i in 0..batch.num_rows() {
                out.push((
                    ids.value(i).to_string(),
                    srcs.value(i).to_string(),
                    dsts.value(i).to_string(),
                ));
            }
            if limit_one {
                break;
            }
        }
        Ok(out)
    }
}

/// Scan `ds` projecting `projection`, filtered by a structured `expr` applied via
/// `Scanner::filter_expr` so Lance can route it through the scalar index. The one
/// place the index-backed scan boilerplate lives.
async fn scan_filtered(ds: &Dataset, projection: &[&str], expr: Expr) -> Result<Vec<RecordBatch>> {
    TableStore::scan_stream_with(ds, Some(projection), None, None, false, move |scanner| {
        scanner.filter_expr(expr);
        Ok(())
    })
    .await?
    .try_collect()
    .await
    .map_err(OmniError::storage)
}

/// Stream `ds` projecting `projection` in bounded batches, unfiltered. The
/// `Scan` access path and the overwrite id sweep read whole tables this way.
async fn scan_bounded(
    ds: &Dataset,
    projection: &[&str],
) -> Result<lance::dataset::scanner::DatasetRecordBatchStream> {
    TableStore::scan_stream_bounded(
        ds,
        Some(projection),
        None,
        None,
        false,
        ID_SCAN_BATCH_ROWS,
        ID_SCAN_BATCH_BYTES,
    )
    .await
}

/// The `Probe` filter for one chunk of keys: an AND of per-column IN-lists of
/// typed literals, one BTREE `IsIn` per indexed column; for a composite group a
/// superset that may hold another chunk's tuple, so Rust matches the chunk's own.
fn unique_probe_filter(columns: &[String], chunk: &[(Vec<String>, Vec<ScalarValue>)]) -> Expr {
    let mut expr: Option<Expr> = None;
    for (i, column) in columns.iter().enumerate() {
        let mut seen: HashSet<&str> = HashSet::new();
        let values: Vec<Expr> = chunk
            .iter()
            .filter(|(canonical, _)| seen.insert(canonical[i].as_str()))
            .map(|(_, typed)| lit(typed[i].clone()))
            .collect();
        let in_list = ident(column.as_str()).in_list(values, false);
        expr = Some(match expr {
            Some(acc) => acc.and(in_list),
            None => in_list,
        });
    }
    expr.expect("invariant: a unique group names at least one column")
}

/// Ids a replacement of `table_key` removes: committed ids in `base` that are
/// not in `batches`, the table's whole new image. The judge's retained-edge
/// checks and cardinality recounts read them (a retained edge to a removed
/// node, a src an overwrite empties). Reads the raw base, not the
/// replacement-emptied [`CommittedState`] view; empty if the table is new in
/// `base`.
pub(crate) async fn overwrite_removed_ids(
    base: &Snapshot,
    table_key: &str,
    batches: &[RecordBatch],
    system_columns: SystemColumns,
    mut admit: impl FnMut(&str) -> Result<()>,
) -> Result<Vec<String>> {
    if base.dataset(table_key).is_none() {
        return Ok(Vec::new());
    }
    let mut new_ids: HashSet<&str> = HashSet::new();
    for batch in batches {
        let column = string_col(batch, system_columns.id)?;
        for i in 0..column.len() {
            if !column.is_null(i) {
                new_ids.insert(column.value(i));
            }
        }
    }
    let ds = base.open_lance_dataset(table_key).await?;
    let mut removed = Vec::new();
    let mut stream = scan_bounded(&ds, &[system_columns.id]).await?;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        let column = string_col(&batch, system_columns.id)?;
        for i in 0..column.len() {
            if !column.is_null(i) && !new_ids.contains(column.value(i)) {
                admit(column.value(i))?;
                removed.push(column.value(i).to_string());
            }
        }
    }
    Ok(removed)
}

/// A key count whose vectors cannot be addressed: the allocation ceiling as
/// the limit, the keys' own bytes as the demand.
fn integer_capacity_error(keys: usize) -> OmniError {
    OmniError::resource_limit(
        "query_memory_bytes",
        isize::MAX as u64,
        (keys as u64).saturating_mul(size_of::<i64>() as u64),
    )
}

/// Every committed id of `ds` that spells a canonical integer and passes
/// `keep`, at most `bound`.
async fn scan_int_ids(
    ds: &Dataset,
    id: &str,
    keys: &mut Vec<i64>,
    bound: usize,
    keep: &(dyn Fn(i64) -> bool + Sync),
) -> Result<()> {
    let mut stream = scan_bounded(ds, &[id]).await?;
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        push_int_ids(&batch, id, keys, bound, keep)?;
    }
    Ok(())
}

/// Every non-null canonical-integer value of `batch`'s `name` column that
/// passes `keep`, appended to `keys`; a value that is not a canonical integer
/// is no id of an integer-keyed table and is skipped.
fn push_int_ids(
    batch: &RecordBatch,
    name: &str,
    keys: &mut Vec<i64>,
    bound: usize,
    keep: &(dyn Fn(i64) -> bool + Sync),
) -> Result<()> {
    let column = string_col(batch, name)?;
    for row in 0..column.len() {
        if !column.is_null(row) {
            if let Some(key) = canonical_i64(column.value(row)).filter(|key| keep(*key)) {
                if keys.len() == bound {
                    return Err(OmniError::manifest_internal(
                        "integer endpoint scan exceeded its admitted row bound",
                    ));
                }
                keys.push(key);
            }
        }
    }
    Ok(())
}

fn string_col<'b>(batch: &'b RecordBatch, name: &str) -> Result<&'b StringArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OmniError::manifest(format!("batch missing column '{name}'")))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest(format!("column '{name}' is not Utf8")))
}

/// `@card` for an edge type, scoped to the srcs the change affects: the one
/// rule every write route counts by, the evaluator's and the `load csv`
/// judge's alike. `plan` is the edge delta coalesced by edge id (last-wins, as
/// commit does); the merged edge count per src = (committed edges with that
/// src, minus those deleted or re-placed by the delta) + (coalesced delta edges
/// with that src). The affected set includes the new src of each delta edge
/// AND the old committed src of each changed/deleted edge id, so moving an edge
/// off a src recounts the vacated src. Under a non-zero minimum it also
/// includes every source node the write inserts or updates (`written`), so a
/// node written without its edges is counted, and an overwritten edge table
/// takes [`evaluate_replaced_cardinality`]. A src in `deleted_src_nodes` is
/// skipped.
#[allow(clippy::too_many_arguments)]
async fn evaluate_cardinality<F>(
    edge_table: &str,
    edge_type: &EdgeType,
    plan: &CardPlan,
    written: &HashSet<String>,
    deleted_src_nodes: &HashSet<String>,
    committed: &CommittedState<'_>,
    system_columns: SystemColumns,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let card = &edge_type.cardinality;
    // Default unbounded cardinality can never be violated — skip the lookups.
    if card.is_default() {
        return Ok(());
    }
    if card.min > 0 && committed.overwritten.contains(edge_table) {
        return evaluate_replaced_cardinality(
            edge_table,
            edge_type,
            plan,
            written,
            deleted_src_nodes,
            committed,
            system_columns,
            sink,
        )
        .await;
    }

    // Committed srcs of the edges this delta touches. `removed_edges` are the
    // deleted ids (direct deletes; a node-delete cascade already lands those ids
    // in `deleted_ids`). `moved_from` are the changed ids' OLD committed srcs: an
    // upsert that moves an edge's src vacates its old src, which must be
    // recounted or a drop below @card min is missed.
    let removed_edges = committed
        .committed_edges_by_id(edge_table, &plan.removed_ids, system_columns)
        .await?;
    let moved_from = committed
        .committed_edges_by_id(edge_table, &plan.changed_ids, system_columns)
        .await?;
    let affected = card_affected(
        plan,
        &removed_edges,
        &moved_from,
        written,
        deleted_src_nodes,
    );
    if affected.is_empty() {
        return Ok(());
    }

    let removed: HashSet<&str> = plan.removed_ids.iter().map(String::as_str).collect();
    let committed_counts = committed
        .committed_source_counts(edge_table, &affected, system_columns, |id| {
            removed.contains(id) || plan.delta_by_id.contains_key(id)
        })
        .await?;
    card_violations(
        edge_table,
        edge_type,
        plan,
        &affected,
        &committed_counts,
        sink,
    )
}

/// `@card` under a non-zero minimum for an edge table the write replaces (an
/// `overwrite` load). Every source of the replaced image or the delta then
/// holds exactly its coalesced delta edges, so a source the overwrite empties
/// is below the minimum although no delta row names it. The replaced image's
/// sources stream past once in bounded batches and only the smallest source
/// the delta leaves without an edge is kept, so memory stays with the delta.
/// The first violation in source order is always among those reported, which
/// is the one a load refuses on. A src that is itself a deleted node is
/// skipped.
#[allow(clippy::too_many_arguments)]
async fn evaluate_replaced_cardinality<F>(
    edge_table: &str,
    edge_type: &EdgeType,
    plan: &CardPlan,
    written: &HashSet<String>,
    deleted_src_nodes: &HashSet<String>,
    committed: &CommittedState<'_>,
    system_columns: SystemColumns,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let delta_sources: HashSet<&str> = plan.delta_by_id.values().map(String::as_str).collect();
    let emptied = committed
        .first_replaced_source(edge_table, system_columns, |src| {
            !delta_sources.contains(src) && !deleted_src_nodes.contains(src)
        })
        .await?;
    let mut affected = card_affected(plan, &[], &[], written, deleted_src_nodes);
    affected.extend(emptied);
    card_violations(
        edge_table,
        edge_type,
        plan,
        &affected,
        &HashMap::new(),
        sink,
    )
}
