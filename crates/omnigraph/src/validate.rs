//! Integrity validation of a write's delta against the committed image.
//!
//! The constraints (value and enum, uniqueness, edge referential integrity,
//! cardinality) are declared once in the schema; the planner derives the
//! checks of every write (`omnigraph_compiler::ir::checks_for`) and decides
//! how each committed lookup reads ([`plan::plan_write`], the `load csv`
//! plan), and the one judge in [`load`] executes them: the `load csv`
//! operator over its partitions, the staged routes (a JSONL load, a
//! mutation, a blob cell) over their batches in memory, and a branch merge
//! over its delta streamed from storage ([`load::Sweep`]).
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

use arrow_array::{Array, RecordBatch};
use datafusion::prelude::{Expr, col, ident, lit};
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::EdgeType;

use crate::db::{Omnigraph, Snapshot};
use crate::error::{MergeConflict, MergeConflictKind, OmniError, Result};
use crate::table_store::key_reads::{PROBE_CHUNK_KEYS, string_col, visit_key_matches};
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

/// The judge's committed view. Lookups may probe a scalar index, scan
/// projected columns, or combine the two across a pinned dataset's fragments.
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
    live: Option<LiveCardinality<'a>>,
    /// The committed datasets this validation has opened, one open per
    /// table: the access choice and the lookups share the handle.
    opened: std::sync::Mutex<HashMap<String, Dataset>>,
}

/// Where a write counts `@card`: the live branch of `db`, and the graph head
/// the caller's precondition names, when the write carries one (#1424).
#[derive(Clone, Copy)]
pub(crate) struct LiveCardinality<'a> {
    pub(crate) db: &'a Omnigraph,
    pub(crate) branch: Option<&'a str>,
    pub(crate) caller_head: Option<&'a str>,
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
    ///
    /// The live read is the one read of a write that reaches past the head
    /// it pinned. A write carrying a caller precondition promises that its
    /// outcome is decided at the caller's head, so a live head that differs
    /// is the caller's precondition failing, before any effect (#1424).
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
            Some(LiveCardinality {
                db,
                branch,
                caller_head,
            }) => {
                let (live, live_head) = db.fresh_snapshot_for_branch_unchecked(branch).await?;
                if let Some(expected) = caller_head
                    && live_head != expected
                {
                    return Err(OmniError::precondition_failed(
                        branch.unwrap_or("main"),
                        expected,
                        Some(live_head),
                    ));
                }
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

    /// Edges a node removal would strand. Both endpoints can match different
    /// probe chunks, so only the first occurrence of each edge id is retained.
    async fn edges_referencing(
        &self,
        edge_table: &str,
        (src_nodes, dst_nodes): (&[&str], &[&str]),
        system_columns: SystemColumns,
        limit_one: bool,
        scratch: &mut load::ByteAdmission<'_>,
        admit: &mut (dyn FnMut(&RecordBatch) -> Result<()> + Send),
    ) -> Result<Vec<(String, String, String)>> {
        if src_nodes.is_empty() && dst_nodes.is_empty() {
            return Ok(Vec::new());
        }
        let Some(ds) = self.open(edge_table).await? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut found = HashSet::new();
        visit_key_matches(
            &ds,
            &[system_columns.id, system_columns.src, system_columns.dst],
            (system_columns.src, src_nodes),
            Some((system_columns.dst, dst_nodes)),
            limit_one,
            scratch,
            |batch| {
                admit(batch)?;
                let ids = string_col(batch, system_columns.id)?;
                let srcs = string_col(batch, system_columns.src)?;
                let dsts = string_col(batch, system_columns.dst)?;
                for i in 0..batch.num_rows() {
                    if limit_one || found.insert(ids.value(i).to_owned()) {
                        out.push((
                            ids.value(i).to_owned(),
                            srcs.value(i).to_owned(),
                            dsts.value(i).to_owned(),
                        ));
                    }
                }
                Ok(())
            },
        )
        .await?;
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

/// Add the committed source of each removed or replaced edge. The source set
/// coalesces multiple old edges of one source as the cardinality check does.
async fn committed_edge_sources(
    ds: Option<&Dataset>,
    plan: &CardPlan,
    system_columns: SystemColumns,
    admit: &mut load::ByteAdmission<'_>,
) -> Result<HashSet<String>> {
    let mut sources = HashSet::new();
    let Some(ds) = ds else { return Ok(sources) };
    let keys = plan
        .delta_by_id
        .len()
        .saturating_add(plan.removed_ids.len());
    if keys == 0 {
        return Ok(sources);
    }
    let _keys = admit(keys.saturating_mul(size_of::<&String>()))?;
    let mut ids: Vec<&String> = plan.delta_by_id.keys().chain(&plan.removed_ids).collect();
    ids.sort_unstable();
    ids.dedup();
    visit_key_matches(
        ds,
        &[system_columns.src],
        (system_columns.id, &ids),
        None,
        false,
        admit,
        |batch| {
            let rows = string_col(batch, system_columns.src)?;
            for row in 0..rows.len() {
                if !sources.contains(rows.value(row)) {
                    sources.insert(rows.value(row).to_owned());
                }
            }
            Ok(())
        },
    )
    .await?;
    Ok(sources)
}

/// Count committed edges for the affected sources, excluding edges this change
/// removes or replaces. Only one count per source survives a scan batch.
async fn committed_source_counts(
    ds: Option<&Dataset>,
    srcs: &HashSet<String>,
    system_columns: SystemColumns,
    excluded: impl Fn(&str) -> bool,
    admit: &mut load::ByteAdmission<'_>,
) -> Result<HashMap<String, u32>> {
    let mut counts = HashMap::<String, u32>::new();
    let Some(ds) = ds.filter(|_| !srcs.is_empty()) else {
        return Ok(counts);
    };
    let _keys = admit(srcs.len().saturating_mul(size_of::<&String>()))?;
    let mut sorted: Vec<&String> = srcs.iter().collect();
    sorted.sort_unstable();
    visit_key_matches(
        ds,
        &[system_columns.id, system_columns.src],
        (system_columns.src, &sorted),
        None,
        false,
        admit,
        |batch| {
            let ids = string_col(batch, system_columns.id)?;
            let batch_srcs = string_col(batch, system_columns.src)?;
            for row in 0..batch.num_rows() {
                if excluded(ids.value(row)) {
                    continue;
                }
                let src = batch_srcs.value(row);
                if let Some(count) = counts.get_mut(src) {
                    *count = count.checked_add(1).ok_or_else(|| {
                        OmniError::manifest_internal("@card edge count overflows u32")
                    })?;
                } else {
                    counts.insert(src.to_owned(), 1);
                }
            }
            Ok(())
        },
    )
    .await?;
    Ok(counts)
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
    admit: &mut load::ByteAdmission<'_>,
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

    if plan.delta_by_id.is_empty()
        && plan.removed_ids.is_empty()
        && written.is_subset(deleted_src_nodes)
    {
        return Ok(());
    }
    // A mutation needs the current branch image (#298), and every lookup in
    // this count must refer to the same image. Open it once for both columns.
    let ds = committed.open_cardinality(edge_table).await?;
    let old_sources = committed_edge_sources(ds.as_ref(), plan, system_columns, admit).await?;
    let affected = card_affected(plan, old_sources, written, deleted_src_nodes);
    let removed: HashSet<&str> = plan.removed_ids.iter().map(String::as_str).collect();
    let committed_counts = committed_source_counts(
        ds.as_ref(),
        &affected,
        system_columns,
        |id| removed.contains(id) || plan.delta_by_id.contains_key(id),
        admit,
    )
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
    let mut affected = card_affected(plan, HashSet::new(), written, deleted_src_nodes);
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

#[cfg(test)]
mod endpoint_probe_tests {
    use super::PROBE_CHUNK_KEYS;
    use crate::table_store::key_reads::{KeyReads, MAX_LITERAL_BYTES};

    /// The plan over a real edge table follows its live BTREE coverage: with
    /// no index every fragment is matched in memory, after `ensure_indices`
    /// every fragment is probed, and after one more append the indexed
    /// fragments are probed and only the new one is matched. A removal
    /// with a single filter inside both the key-count and literal-byte
    /// ceilings can probe every fragment.
    #[tokio::test]
    async fn removal_reads_follow_live_coverage_and_literal_bytes_issue_1442_issue_1445() {
        use crate::db::{Omnigraph, ReadTarget};
        use crate::loader::LoadMode;

        let dir = tempfile::tempdir().unwrap();
        let db = crate::Session::from_defaults(
            std::sync::Arc::new(
                Omnigraph::init(
                    dir.path().to_str().unwrap(),
                    "node Doc { slug: String @key }\nedge Link: Doc -> Doc\n",
                )
                .await
                .unwrap(),
            ),
            omnigraph_compiler::settings::SessionSettings::default(),
        );
        db.load(
            "main",
            r#"{"type":"Doc","data":{"slug":"keep"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
        let append = || async {
            db.load(
                "main",
                r#"{"edge":"Link","from":"keep","to":"keep"}
{"edge":"Link","from":"keep","to":"keep"}"#,
                LoadMode::Append,
            )
            .await
            .unwrap();
        };
        // Each append puts both matching rows in one fragment.
        append().await;
        append().await;

        let many: Vec<String> = (0..=PROBE_CHUNK_KEYS).map(|i| format!("d{i:07}")).collect();
        let one = vec!["d0000000".to_string()];
        // (fragments probed, `None` for every fragment; fragments matched)
        let split = |ids: &[String]| {
            let ids = ids.to_vec();
            let db = &db;
            async move {
                let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
                let ds = snapshot.open_lance_dataset("edge:Link").await.unwrap();
                let reads = KeyReads::plan(&ds, ("__src", &ids), Some(("__dst", &ids)))
                    .await
                    .unwrap();
                (
                    reads.probe_fragments().map(|fragments| fragments.len()),
                    reads.matched().map_or(0, |(fragments, _)| fragments.len()),
                )
            }
        };

        assert_eq!(
            split(&many).await,
            (Some(0), 2),
            "no index: every fragment matched"
        );
        assert_eq!(
            split(&one).await,
            (None, 0),
            "one id: its filter on every fragment"
        );
        // One key on two endpoints is tiny by count but its copies on the
        // two uncovered fragments exceed the probe's literal-byte ceiling.
        let wide = vec!["x".repeat(MAX_LITERAL_BYTES / 8)];
        let snapshot = db.snapshot_of(ReadTarget::branch("main")).await.unwrap();
        let committed = super::CommittedState {
            committed: Some(&snapshot),
            overwritten: Default::default(),
            live: None,
            opened: Default::default(),
        };
        let mut targets: Vec<_> = many.iter().map(String::as_str).collect();
        targets.push("keep");
        let mut admitted_rows = 0;
        let retained = committed
            .edges_referencing(
                "edge:Link",
                (&targets, &[]),
                db.catalog().system_columns,
                true,
                &mut |_| Ok(Box::new(())),
                &mut |batch| {
                    admitted_rows += batch.num_rows();
                    Ok(())
                },
            )
            .await
            .unwrap();
        assert_eq!(
            (split(&wide).await, retained.len(), admitted_rows),
            ((Some(0), 2), 1, 1),
            "wide keys scan the tail and a first-orphan check retains only its first matching row"
        );
        db.ensure_indices_as(None).await.unwrap();
        assert_eq!(
            split(&many).await,
            (None, 0),
            "full coverage: every fragment probed"
        );
        // Full coverage avoids the per-fragment copies, but even a
        // fully indexed table scans when one literal cannot fit by itself.
        assert_eq!(split(&wide).await, (None, 0));
        assert_eq!(split(&["x".repeat(MAX_LITERAL_BYTES)]).await, (Some(0), 2));
        append().await;
        assert_eq!(
            split(&many).await,
            (Some(2), 1),
            "the indexed fragments are probed, the appended one matched"
        );
        assert_eq!(
            split(&one).await,
            (None, 0),
            "one id: its filter on every fragment"
        );
    }
}
