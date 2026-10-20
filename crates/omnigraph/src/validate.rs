//! Unified, catalog-derived integrity validation.
//!
//! Validation invariants (value/enum, uniqueness, edge referential integrity,
//! cardinality) are declared once in the schema and should be *evaluated* once,
//! not re-implemented per write surface. Historically the merge path
//! (`exec/merge.rs`) carried its own copy of these checks, parallel to the write
//! path (`loader`/`exec/mutation.rs`); the two drifted (merge validated
//! `@range`/`@check` but not enum membership), which is the class of bug this
//! module closes.
//!
//! The evaluator does NOT reimplement the leaf checks — it orchestrates the
//! existing ones (`loader::validate_value_constraints`,
//! `loader::validate_enum_constraints`, ...) over a per-table [`ChangeSet`], so
//! the surfaces that adopt it cannot diverge. All three write surfaces —
//! branch-merge (`exec/merge.rs`), mutation (`exec/mutation.rs`), and bulk load
//! (`loader`) — now route their integrity checks through this one evaluator, so
//! the drift class above is unrepresentable.
//!
//! Δ-scoping: checks run over the *changed* rows (the merge/write delta), not the
//! whole table. Row-local checks (value/enum) need only the changed rows because
//! unchanged rows were validated when written. Cross-row/cross-table checks
//! (uniqueness, RI, cardinality) evaluate the delta against an index-backed view
//! of committed state (the merge target snapshot, or the write's pinned base /
//! live HEAD depending on the surface).

mod kernels;
mod key_set;
pub(crate) mod load;

use std::collections::{HashMap, HashSet};

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::prelude::{Expr, col, ident, lit};
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::{Catalog, EdgeType};

use crate::db::{Omnigraph, Snapshot};
use crate::error::{MergeConflict, MergeConflictKind, OmniError, Result};
use crate::loader::{validate_enum_constraints, validate_value_constraints};
use crate::table_store::{ID_SCAN_BATCH_BYTES, ID_SCAN_BATCH_ROWS, TableStore};
use kernels::{
    HoldersByKey, card_affected, card_plan, card_violations, deleted_endpoints, edge_ri_delta,
    match_unique_holders, orphans_in_delta, orphans_retained, partition_endpoints,
    unique_coalesced_dups, unique_committed_collisions, unique_image, unique_probe_keys,
};
use key_set::{IntKeySet, IntKeySetLayout, canonical_i64, key_bytes};

/// A single integrity violation, surface-neutral. Maps to the merge path's
/// [`MergeConflict`] today via [`Violation::into_merge_conflict`]; a write-path
/// `into_omni_error` mapping is added when the write path migrates.
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

/// Per-table change produced by a write or a merge: the rows that were added or
/// changed (as batches, so row-local checks scan only them) plus the ids
/// removed. The unit of Δ-scoping.
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

/// Row-local value validation — `@range`/`@check` (nodes) and enum membership
/// (nodes **and** edges) — Δ-scoped to the changed rows. Reuses the loader
/// leaves so the merge and write paths share one implementation; including the
/// enum check here is what closes the merge-vs-write drift. Leaf validators
/// still produce at most one error per invocation, while the sink avoids
/// retaining their aggregate across every changed table and batch.
fn evaluate_value_constraints_with_sink<F>(
    changeset: &ChangeSet,
    catalog: &Catalog,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let mut __dst_cs: Vec<_> = changeset.iter().collect();
    __dst_cs.sort_by(|a, b| a.0.cmp(b.0));
    for (table_key, change) in __dst_cs {
        if let Some(type_name) = table_key.strip_prefix("node:") {
            let Some(node_type) = catalog.node_types.get(type_name) else {
                continue;
            };
            for batch in change.value_batches() {
                if let Err(err) = validate_value_constraints(batch, node_type) {
                    sink(value_violation(table_key, err))?;
                }
                if let Err(err) = validate_enum_constraints(batch, &node_type.properties, type_name)
                {
                    sink(value_violation(table_key, err))?;
                }
            }
        } else if let Some(type_name) = table_key.strip_prefix("edge:") {
            let Some(edge_type) = catalog.edge_types.get(type_name) else {
                continue;
            };
            // Edges carry no @range/@check (NodeType-only), but their properties
            // can be enum-typed — the check the merge path was missing.
            for batch in change.value_batches() {
                if let Err(err) = validate_enum_constraints(batch, &edge_type.properties, type_name)
                {
                    sink(value_violation(table_key, err))?;
                }
            }
        }
    }
    Ok(())
}

/// Wrap a leaf-check error as a value-constraint [`Violation`]. The message is
/// the leaf's own text (`err.to_string()`), matching what the merge path
/// previously surfaced — error text is a contract.
fn value_violation(table_key: &str, err: OmniError) -> Violation {
    Violation {
        table_key: table_key.to_string(),
        row_id: None,
        kind: MergeConflictKind::ValueConstraintViolation,
        message: err.to_string(),
    }
}

// ── Cross-row / cross-table checks (uniqueness, edge-RI, cardinality) ─────────
//
// These evaluate the merge delta against committed state. Because adopt is only
// chosen when `same_manifest_state(base, target)` (target unchanged since fork),
// the merged content of EVERY table is `target ± delta`. So committed lookups go
// to the indexed TARGET table and the in-memory delta is applied on top — never
// the source table or the unindexed staged temp (which carries no index).

/// A declared integrity constraint, derived from the catalog. Mirrors the
/// `PropType::index_kind` chokepoint: adding a kind is one variant + one arm in
/// [`evaluate`], run on every surface that adopts the evaluator.
#[derive(Debug, Clone)]
pub(crate) enum Constraint {
    /// Row-local value/enum validation across the whole change-set (one entry
    /// covers every table; handled by [`evaluate_value_constraints_with_sink`]).
    Value,
    Unique {
        table_key: String,
        columns: Vec<String>,
        /// True for the `@key` group: it is id-backed, so a committed holder of a
        /// key value is always the same row (an upsert), never a cross-version
        /// duplicate. Intra-delta dedup suffices; the committed lookup is skipped.
        is_key: bool,
    },
    EdgeRi {
        table_key: String,
        from_type: String,
        to_type: String,
    },
    Cardinality {
        table_key: String,
    },
}

/// A constraint's identity: the fields an access-path choice is keyed on.
/// `Value` has none; it consults no committed state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ConstraintKey {
    Unique {
        table_key: String,
        columns: Vec<String>,
    },
    EdgeRi {
        table_key: String,
    },
    Cardinality {
        table_key: String,
    },
}

/// How a constraint reads the committed rows it checks the delta against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AccessPath {
    /// Filtered scans carrying the delta's keys, served by the scalar index.
    Probe,
    /// One projected scan of the committed table, matched in memory.
    #[allow(dead_code)]
    Scan,
}

impl Constraint {
    pub(crate) fn key(&self) -> Option<ConstraintKey> {
        match self {
            Constraint::Value => None,
            Constraint::Unique {
                table_key, columns, ..
            } => Some(ConstraintKey::Unique {
                table_key: table_key.clone(),
                columns: columns.clone(),
            }),
            Constraint::EdgeRi { table_key, .. } => Some(ConstraintKey::EdgeRi {
                table_key: table_key.clone(),
            }),
            Constraint::Cardinality { table_key } => Some(ConstraintKey::Cardinality {
                table_key: table_key.clone(),
            }),
        }
    }

    /// The access paths this constraint's committed lookup can take. Empty
    /// when it has no committed lookup: `Value`, and a `@key` group, whose
    /// committed holder of a key value is the same row.
    #[allow(dead_code)]
    pub(crate) fn access_paths(&self) -> &'static [AccessPath] {
        match self {
            Constraint::Value | Constraint::Unique { is_key: true, .. } => &[],
            Constraint::Unique { is_key: false, .. } | Constraint::EdgeRi { .. } => {
                &[AccessPath::Probe, AccessPath::Scan]
            }
            Constraint::Cardinality { .. } => &[AccessPath::Probe],
        }
    }
}

/// The access path chosen per constraint; every constraint without a choice
/// probes.
#[derive(Debug, Clone, Default)]
pub(crate) struct AccessPaths {
    chosen: HashMap<ConstraintKey, AccessPath>,
}

impl AccessPaths {
    pub(crate) fn probe_everywhere() -> Self {
        Self::default()
    }

    /// Record `path` for `constraint`.
    ///
    /// # Errors
    /// `path` is not among [`Constraint::access_paths`].
    #[allow(dead_code)]
    pub(crate) fn choose(&mut self, constraint: &Constraint, path: AccessPath) -> Result<()> {
        let Some(key) = constraint
            .key()
            .filter(|_| constraint.access_paths().contains(&path))
        else {
            return Err(OmniError::manifest_internal(format!(
                "access path {path:?} is not available for {constraint:?}"
            )));
        };
        self.chosen.insert(key, path);
        Ok(())
    }

    pub(crate) fn path_for(&self, constraint: &Constraint) -> AccessPath {
        constraint
            .key()
            .and_then(|key| self.chosen.get(&key).copied())
            .unwrap_or(AccessPath::Probe)
    }
}

/// Derive the runtime constraint set from the catalog (the schema's declared
/// invariants). One `Value` plus one entry per `@unique` group, edge-RI, and
/// `@card` edge.
pub(crate) fn constraints_for(catalog: &Catalog) -> Vec<Constraint> {
    let mut out = vec![Constraint::Value];
    let mut __dst_vn: Vec<_> = catalog.node_types.iter().collect();
    __dst_vn.sort_by(|a, b| a.0.cmp(b.0));
    for (name, node_type) in __dst_vn {
        let table_key = format!("node:{name}");
        // `@key` is id-backed: cross-version duplication is impossible (the key
        // IS the identity), so it needs only intra-delta dedup — `is_key: true`
        // tells the evaluator to skip the committed lookup.
        if let Some(key) = &node_type.key {
            out.push(Constraint::Unique {
                table_key: table_key.clone(),
                columns: key.clone(),
                is_key: true,
            });
        }
        // `@unique` (non-key) groups CAN collide cross-version → committed lookup.
        for columns in &node_type.unique_constraints {
            if Some(columns) == node_type.key.as_ref() {
                continue; // same column tuple as @key — already covered above.
            }
            out.push(Constraint::Unique {
                table_key: table_key.clone(),
                columns: columns.clone(),
                is_key: false,
            });
        }
    }
    let mut __dst_ve: Vec<_> = catalog.edge_types.iter().collect();
    __dst_ve.sort_by(|a, b| a.0.cmp(b.0));
    for (name, edge_type) in __dst_ve {
        let table_key = format!("edge:{name}");
        // `@key` is id-backed: cross-version duplication is impossible (the key
        // IS the identity), so it needs only intra-delta dedup — `is_key: true`
        // tells the evaluator to skip the committed lookup. Sound only because
        // a key exists from type creation on: schema_plan refuses every key
        // change on an existing type, so committed rows always carry derived
        // ids.
        if let Some(key) = &edge_type.key {
            out.push(Constraint::Unique {
                table_key: table_key.clone(),
                columns: key.clone(),
                is_key: true,
            });
        }
        // `@unique` (non-key) groups CAN collide cross-version → committed lookup.
        // Subsumption compares column SETS: uniqueness is order-free, and the
        // catalog stores the key endpoint-first while shape normalization
        // stores `@unique` groups lexically, so tuple equality can never hold.
        let sorted_key = edge_type.key.as_ref().map(|key| {
            let mut key = key.clone();
            key.sort();
            key
        });
        for columns in &edge_type.unique_constraints {
            let mut sorted_columns = columns.clone();
            sorted_columns.sort();
            if Some(&sorted_columns) == sorted_key.as_ref() {
                continue; // same column set as @key — already covered above.
            }
            out.push(Constraint::Unique {
                table_key: table_key.clone(),
                columns: columns.clone(),
                is_key: false,
            });
        }
        out.push(Constraint::EdgeRi {
            table_key: table_key.clone(),
            from_type: edge_type.from_type.clone(),
            to_type: edge_type.to_type.clone(),
        });
        out.push(Constraint::Cardinality { table_key });
    }
    out
}

/// Keys per batched committed-uniqueness scan. Bounds the pushed-down filter
/// size on the merge path (whose deltas are not row-capped); Mutation/Load
/// deltas are already capped at this many rows, so they probe in one chunk.
const UNIQUE_PROBE_CHUNK_KEYS: usize = 8_192;

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
}

impl<'a> CommittedState<'a> {
    /// Merge path: read the merge target snapshot for every lookup.
    pub(crate) fn merge(target: &'a Snapshot) -> Self {
        Self {
            committed: Some(target),
            overwritten: HashSet::new(),
            live: None,
        }
    }

    /// Write path: existence/uniqueness read `committed` (the write's pinned
    /// base); cardinality reads the live committed branch snapshot via `db`
    /// (#298).
    pub(crate) fn write(
        committed: &'a Snapshot,
        db: &'a Omnigraph,
        branch: Option<&'a str>,
    ) -> Self {
        Self {
            committed: Some(committed),
            overwritten: HashSet::new(),
            live: Some((db, branch)),
        }
    }

    /// Bulk-load path: validate against the pinned pre-load `base` (never live
    /// HEAD — the loader pins its base, unlike the mutation `@card` #298 case).
    /// `Overwrite` replaces only the touched tables (PER-TABLE), so the committed
    /// view of each table in `changeset` is EMPTY — the batch is that table's
    /// whole new image — while tables absent from the batch keep `base` (an
    /// edges-only overwrite still resolves RI against committed nodes).
    /// `Append`/`Merge` keep `base` for every table.
    pub(crate) fn load(
        base: &'a Snapshot,
        mode: crate::loader::LoadMode,
        changeset: &ChangeSet,
    ) -> Self {
        let overwritten = match mode {
            crate::loader::LoadMode::Overwrite => changeset.keys().cloned().collect(),
            crate::loader::LoadMode::Append | crate::loader::LoadMode::Merge => HashSet::new(),
        };
        Self {
            committed: Some(base),
            overwritten,
            live: None,
        }
    }

    async fn open(&self, table_key: &str) -> Result<Option<Dataset>> {
        if self.overwritten.contains(table_key) {
            return Ok(None);
        }
        let Some(committed) = self.committed else {
            return Ok(None);
        };
        match committed.dataset(table_key) {
            Some(_) => Ok(Some(committed.open_lance_dataset(table_key).await?)),
            None => Ok(None),
        }
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
                // `CommittedState::write` is constructed only after WriteTxn
                // schema validation, so use the unchecked manifest refresh to
                // avoid another full contract read while retaining live branch
                // authority. `Snapshot::open_lance_dataset` follows the entry's
                // actual native dataset branch and pinned version (including inheritance).
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

    /// The committed `id`s of `table_key` that spell canonical integers, as one
    /// probeable key set: `Probe` filters by `wanted`, `Scan` reads every id.
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
                match access {
                    AccessPath::Probe => rows.min(wanted.len()),
                    AccessPath::Scan => rows,
                }
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
                        push_int_ids(batch, system_columns.id, &mut keys, bound)?;
                    }
                }
                AccessPath::Scan => {
                    let mut stream = scan_bounded(&ds, &[system_columns.id]).await?;
                    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
                        push_int_ids(&batch, system_columns.id, &mut keys, bound)?;
                    }
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
    /// scan order: `Probe` filters per [`UNIQUE_PROBE_CHUNK_KEYS`] chunk, `Scan`
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
                for chunk in keys.chunks(UNIQUE_PROBE_CHUNK_KEYS) {
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

    /// Committed edges `(id, src)` in `edge_table` matching `keys` on `key_col`
    /// (id or src). Index-backed.
    async fn committed_edges(
        &self,
        edge_table: &str,
        key_col: &str,
        keys: &[String],
        system_columns: SystemColumns,
    ) -> Result<Vec<(String, String)>> {
        let Some(ds) = self.open_cardinality(edge_table).await? else {
            return Ok(Vec::new());
        };
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let expr = col(key_col).in_list(keys.iter().map(|k| lit(k.clone())).collect(), false);
        let batches = scan_filtered(&ds, &[system_columns.id, system_columns.src], expr).await?;
        let mut out = Vec::new();
        for batch in &batches {
            let ids = string_col(batch, system_columns.id)?;
            let srcs = string_col(batch, system_columns.src)?;
            for i in 0..batch.num_rows() {
                out.push((ids.value(i).to_string(), srcs.value(i).to_string()));
            }
        }
        Ok(out)
    }

    /// The first committed edge a single-table node overwrite would strand.
    async fn first_edge_referencing(
        &self,
        edge_table: &str,
        src_nodes: &[String],
        dst_nodes: &[String],
        system_columns: SystemColumns,
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
                scanner.limit(1)?;
                scanner.batch_size(1);
                scanner.batch_size_bytes(ID_SCAN_BATCH_BYTES);
                scanner.batch_readahead(1);
                Ok(())
            },
        )
        .await?;
        while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
            if batch.num_rows() == 0 {
                continue;
            }
            admit(&batch)?;
            let ids = string_col(&batch, system_columns.id)?;
            let srcs = string_col(&batch, system_columns.src)?;
            let dsts = string_col(&batch, system_columns.dst)?;
            return Ok(vec![(
                ids.value(0).to_string(),
                srcs.value(0).to_string(),
                dsts.value(0).to_string(),
            )]);
        }
        Ok(Vec::new())
    }

    /// Committed edges `(id, src, dst)` in `edge_table` whose src is in
    /// `src_nodes` OR dst is in `dst_nodes` — the edges a node deletion would
    /// strand. Index-backed (BTREE on src/dst).
    async fn edges_referencing(
        &self,
        edge_table: &str,
        src_nodes: &[String],
        dst_nodes: &[String],
        system_columns: SystemColumns,
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
        let batches = scan_filtered(
            &ds,
            &[system_columns.id, system_columns.src, system_columns.dst],
            expr.unwrap(),
        )
        .await?;
        let mut out = Vec::new();
        for batch in &batches {
            let ids = string_col(batch, system_columns.id)?;
            let srcs = string_col(batch, system_columns.src)?;
            let dsts = string_col(batch, system_columns.dst)?;
            for i in 0..batch.num_rows() {
                out.push((
                    ids.value(i).to_string(),
                    srcs.value(i).to_string(),
                    dsts.value(i).to_string(),
                ));
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

/// Ids an `Overwrite` of `table_key` removes: committed ids in `base` that are
/// NOT in `change`'s replacement image (`added ∪ changed`). The loader folds
/// these into the change-set's `deleted_ids` so edge-RI (path-b) and cardinality
/// recompute against a node/edge the overwrite drops — e.g. a retained edge to a
/// removed node, or a src an overwrite empties. Reads the RAW base (NOT the
/// overwrite-emptied [`CommittedState`] view). Empty if the table is new in `base`.
pub(crate) async fn overwrite_removed_ids(
    base: &Snapshot,
    table_key: &str,
    change: &TableChange,
    system_columns: SystemColumns,
    mut admit: impl FnMut(&str) -> Result<()>,
) -> Result<Vec<String>> {
    if base.dataset(table_key).is_none() {
        return Ok(Vec::new());
    }
    let mut new_ids: HashSet<&str> = HashSet::new();
    for batch in change.value_batches() {
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

/// Every non-null canonical-integer value of `batch`'s `name` column, appended
/// to `keys`; a value that is not a canonical integer is no id of an
/// integer-keyed table and is skipped.
fn push_int_ids(batch: &RecordBatch, name: &str, keys: &mut Vec<i64>, bound: usize) -> Result<()> {
    let column = string_col(batch, name)?;
    for row in 0..column.len() {
        if !column.is_null(row) {
            if let Some(key) = canonical_i64(column.value(row)) {
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

/// Non-null `id`s across a table's added∪changed delta rows.
fn delta_id_set(change: &TableChange, system_columns: SystemColumns) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for batch in change.value_batches() {
        let column = string_col(batch, system_columns.id)?;
        for i in 0..column.len() {
            if !column.is_null(i) {
                ids.insert(column.value(i).to_string());
            }
        }
    }
    Ok(ids)
}

/// Write-path tail: derive the catalog constraints, run [`evaluate`] over the
/// change-set against `committed`, and return the first violation as an
/// `OmniError`. Shared by the mutation and loader paths (the merge path maps to
/// `MergeConflict`s instead, so it calls [`evaluate`] directly).
pub(crate) async fn validate_changeset(
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    catalog: &Catalog,
) -> Result<()> {
    if changeset.is_empty() {
        return Ok(());
    }
    validate_changeset_with_access(
        changeset,
        committed,
        catalog,
        &AccessPaths::probe_everywhere(),
    )
    .await
}

/// [`validate_changeset`] with the committed lookups taking the access path
/// `access` records per constraint (an accepted load plan's choices).
pub(crate) async fn validate_changeset_with_access(
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    catalog: &Catalog,
    access: &AccessPaths,
) -> Result<()> {
    if changeset.is_empty() {
        return Ok(());
    }
    let constraints = constraints_for(catalog);
    let violations =
        evaluate_with_access(&constraints, changeset, committed, catalog, access).await?;
    match violations.into_iter().next() {
        Some(violation) => Err(violation.into_omni_error()),
        None => Ok(()),
    }
}

/// Run the declared constraints over the merge delta against committed state,
/// probing committed state everywhere. Δ-scoped: only tables present in
/// `changeset` do any work.
pub(crate) async fn evaluate(
    constraints: &[Constraint],
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    catalog: &Catalog,
) -> Result<Vec<Violation>> {
    evaluate_with_access(
        constraints,
        changeset,
        committed,
        catalog,
        &AccessPaths::probe_everywhere(),
    )
    .await
}

/// [`evaluate`] with the committed lookups taking the access path `access`
/// records per constraint.
pub(crate) async fn evaluate_with_access(
    constraints: &[Constraint],
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    catalog: &Catalog,
    access: &AccessPaths,
) -> Result<Vec<Violation>> {
    let mut violations = Vec::new();
    evaluate_with_access_and_sink(
        constraints,
        changeset,
        committed,
        catalog,
        access,
        |violation| {
            violations.push(violation);
            Ok(())
        },
    )
    .await?;
    Ok(violations)
}

/// Run the declared constraints while emitting each violation as soon as it is
/// produced, probing committed state everywhere. Constraint order,
/// per-constraint ordering, and messages are identical to [`evaluate`]; unlike
/// that compatibility wrapper, this entry point does not retain a graph-wide
/// violation vector.
///
/// A fallible sink may observe a successful prefix before either it or a later
/// validator returns an error. The returned count includes only violations the
/// sink accepted.
#[allow(dead_code)]
pub(crate) async fn evaluate_with_sink<F>(
    constraints: &[Constraint],
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    catalog: &Catalog,
    sink: F,
) -> Result<usize>
where
    F: FnMut(Violation) -> Result<()>,
{
    evaluate_with_access_and_sink(
        constraints,
        changeset,
        committed,
        catalog,
        &AccessPaths::probe_everywhere(),
        sink,
    )
    .await
}

/// [`evaluate_with_sink`] with the committed lookups taking the access path
/// `access` records per constraint.
pub(crate) async fn evaluate_with_access_and_sink<F>(
    constraints: &[Constraint],
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    catalog: &Catalog,
    access: &AccessPaths,
    mut sink: F,
) -> Result<usize>
where
    F: FnMut(Violation) -> Result<()>,
{
    let mut violation_count = 0usize;
    let system_columns = catalog.system_columns;
    {
        let mut emit = |violation| {
            let next_count = violation_count
                .checked_add(1)
                .ok_or_else(|| OmniError::manifest("integrity violation count overflow"))?;
            sink(violation)?;
            violation_count = next_count;
            Ok(())
        };

        for constraint in constraints {
            match constraint {
                Constraint::Value => {
                    evaluate_value_constraints_with_sink(changeset, catalog, &mut emit)?;
                }
                Constraint::Unique {
                    table_key,
                    columns,
                    is_key,
                } => {
                    if let Some(change) = changeset.get(table_key) {
                        evaluate_unique(
                            table_key,
                            columns,
                            *is_key,
                            change,
                            committed,
                            system_columns,
                            access.path_for(constraint),
                            &mut emit,
                        )
                        .await?;
                    }
                }
                Constraint::EdgeRi {
                    table_key,
                    from_type,
                    to_type,
                } => {
                    // Run when the edge itself has a delta OR when a referenced node
                    // type has deletions (path-b can strand a committed target edge
                    // even if this edge table has no delta of its own).
                    let node_deleted = |node_type: &str| {
                        changeset
                            .get(&format!("node:{node_type}"))
                            .map(|change| !change.deleted_ids.is_empty())
                            .unwrap_or(false)
                    };
                    let change = changeset.get(table_key);
                    if change.is_some() || node_deleted(from_type) || node_deleted(to_type) {
                        evaluate_edge_ri(
                            table_key,
                            from_type,
                            to_type,
                            change,
                            changeset,
                            committed,
                            system_columns,
                            access.path_for(constraint),
                            &mut emit,
                        )
                        .await?;
                    }
                }
                Constraint::Cardinality { table_key } => {
                    if let Some(change) = changeset.get(table_key) {
                        let Some(type_name) = table_key.strip_prefix("edge:") else {
                            continue;
                        };
                        if let Some(edge_type) = catalog.edge_types.get(type_name) {
                            evaluate_cardinality(
                                table_key,
                                edge_type,
                                change,
                                changeset,
                                committed,
                                system_columns,
                                &mut emit,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }
    Ok(violation_count)
}

/// Uniqueness for one group on `table_key` over the delta's final coalesced
/// image: within-batch repeats ([`unique_image`]; a hit ends the group), then
/// cross-id repeats, then for a non-`@key` group the committed holders via `access`.
#[allow(clippy::too_many_arguments)]
async fn evaluate_unique<F>(
    table_key: &str,
    columns: &[String],
    is_key: bool,
    change: &TableChange,
    committed: &CommittedState<'_>,
    system_columns: SystemColumns,
    access: AccessPath,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let delta_ids = delta_id_set(change, system_columns)?;
    let deleted: HashSet<&String> = change.deleted_ids.iter().collect();

    let image = unique_image(table_key, columns, change, system_columns, sink)?;
    if image.within_batch_violation {
        return Ok(());
    }
    unique_coalesced_dups(table_key, columns, &image.entries, sink)?;
    if !is_key {
        let probe = unique_probe_keys(&image.entries);
        let holders_by_key = committed
            .unique_holders(table_key, columns, &probe, system_columns, access)
            .await?;
        unique_committed_collisions(
            table_key,
            columns,
            &image.entries,
            &holders_by_key,
            &delta_ids,
            &deleted,
            sink,
        )?;
    }
    Ok(())
}

/// Edge referential integrity: path-a resolves each delta edge's endpoints in
/// the merged node universe (the committed part via `access`); path-b probes the
/// committed edges referencing a node this change deletes and reports survivors.
#[allow(clippy::too_many_arguments)]
async fn evaluate_edge_ri<F>(
    edge_table: &str,
    from_type: &str,
    to_type: &str,
    change: Option<&TableChange>,
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    system_columns: SystemColumns,
    access: AccessPath,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let from_table = format!("node:{from_type}");
    let to_table = format!("node:{to_type}");
    let mut delta_edge_ids: HashSet<String> = HashSet::new();

    if let Some(change) = change {
        let edges = edge_ri_delta(change, system_columns)?;
        delta_edge_ids.extend(edges.iter().map(|(id, _, _)| id.clone()));
        if !edges.is_empty() {
            let srcs: Vec<String> = edges.iter().map(|(_, src, _)| src.clone()).collect();
            let dsts: Vec<String> = edges.iter().map(|(_, _, dst)| dst.clone()).collect();
            let from_exist = merged_node_existence(
                &from_table,
                &srcs,
                changeset,
                committed,
                system_columns,
                access,
            )
            .await?;
            let to_exist = merged_node_existence(
                &to_table,
                &dsts,
                changeset,
                committed,
                system_columns,
                access,
            )
            .await?;
            orphans_in_delta(
                edge_table,
                from_type,
                to_type,
                &edges,
                &from_exist,
                &to_exist,
                system_columns,
                sink,
            )?;
        }
    }

    let (deleted_from, deleted_to) = deleted_endpoints(changeset, &from_table, &to_table);
    if !deleted_from.is_empty() || !deleted_to.is_empty() {
        let removed: HashSet<&String> = change
            .map(|change| change.deleted_ids.iter().collect())
            .unwrap_or_default();
        let retained = committed
            .edges_referencing(edge_table, &deleted_from, &deleted_to, system_columns)
            .await?;
        orphans_retained(
            edge_table,
            from_type,
            to_type,
            &retained,
            &delta_edge_ids,
            &removed,
            &deleted_from,
            &deleted_to,
            system_columns,
            sink,
        )?;
    }

    Ok(())
}

/// Which of `ids` exist in the merged node table `node_table` = `target ± delta`:
/// present if added/changed in the delta, absent if deleted, else a committed
/// lookup through `access`.
async fn merged_node_existence(
    node_table: &str,
    ids: &[String],
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    system_columns: SystemColumns,
    access: AccessPath,
) -> Result<HashSet<String>> {
    let (added_changed, deleted) = match changeset.get(node_table) {
        Some(change) => (
            delta_id_set(change, system_columns)?,
            change.deleted_ids.iter().cloned().collect::<HashSet<_>>(),
        ),
        None => (HashSet::new(), HashSet::new()),
    };
    let (mut exist, to_probe) = partition_endpoints(ids, &added_changed, &deleted);
    exist.extend(
        committed
            .existing_ids(node_table, &to_probe, system_columns, access)
            .await?,
    );
    Ok(exist)
}

/// `@card` for an edge type, scoped to the srcs the delta affects. The delta is
/// coalesced by edge id (last-wins, as commit does); the merged edge set per src
/// = (committed edges with that src, minus those deleted or re-placed by the
/// delta) ∪ (coalesced delta edges with that src). The affected set includes the
/// new src of each delta edge AND the old committed src of each changed/deleted
/// edge id, so moving an edge off a src recounts the vacated src. A src that is
/// itself a deleted node is skipped.
async fn evaluate_cardinality<F>(
    edge_table: &str,
    edge_type: &EdgeType,
    change: &TableChange,
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    system_columns: SystemColumns,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let card = &edge_type.cardinality;
    // Default unbounded cardinality can never be violated — skip the lookups.
    if card.min == 0 && card.max.is_none() {
        return Ok(());
    }
    let plan = card_plan(change, system_columns)?;

    // Committed srcs of the edges this delta touches. `removed_edges` are the
    // deleted ids (direct deletes; a node-delete cascade already lands those ids
    // in `deleted_ids`). `moved_from` are the changed ids' OLD committed srcs: an
    // upsert that moves an edge's src vacates its old src, which must be
    // recounted or a drop below @card min is missed.
    let removed_edges = committed
        .committed_edges(
            edge_table,
            system_columns.id,
            &plan.removed_ids,
            system_columns,
        )
        .await?;
    let moved_from = committed
        .committed_edges(
            edge_table,
            system_columns.id,
            &plan.changed_ids,
            system_columns,
        )
        .await?;

    let deleted_src_nodes: HashSet<String> = changeset
        .get(&format!("node:{}", edge_type.from_type))
        .map(|change| change.deleted_ids.iter().cloned().collect())
        .unwrap_or_default();
    let affected = card_affected(&plan, &removed_edges, &moved_from, &deleted_src_nodes);
    if affected.is_empty() {
        return Ok(());
    }

    let affected_vec: Vec<String> = affected.iter().cloned().collect();
    let committed_for_affected = committed
        .committed_edges(
            edge_table,
            system_columns.src,
            &affected_vec,
            system_columns,
        )
        .await?;
    card_violations(
        edge_table,
        edge_type,
        &plan,
        &affected,
        &committed_for_affected,
        sink,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::schema::parser::parse_schema;
    use omnigraph_compiler::settings::SessionSettings;

    use crate::Session;
    use crate::loader::LoadMode;

    const DOC_SCHEMA: &str =
        "node Doc {\n  slug: String @key\n  status: enum(draft, published)\n}\n";

    fn catalog(src: &str) -> Catalog {
        build_catalog(&parse_schema(src).unwrap()).unwrap()
    }

    /// A change-set touching only `Doc.status` with the given values.
    fn status_change(values: &[&str]) -> ChangeSet {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__id", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
        ]));
        let ids = (0..values.len())
            .map(|index| format!("doc-{index}"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)) as _,
                Arc::new(StringArray::from(values.to_vec())) as _,
            ],
        )
        .unwrap();
        let mut change = TableChange::default();
        change.changed.push(batch);
        let mut cs = ChangeSet::new();
        cs.insert("node:Doc".to_string(), change);
        cs
    }

    fn duplicate_slug_change() -> ChangeSet {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__id", DataType::Utf8, false),
            Field::new("slug", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["doc-0", "doc-1", "doc-2", "doc-3"])) as _,
                Arc::new(StringArray::from(vec!["alpha", "alpha", "beta", "beta"])) as _,
                Arc::new(StringArray::from(vec![
                    "bogus",
                    "draft",
                    "draft",
                    "published",
                ])) as _,
            ],
        )
        .unwrap();
        let mut change = TableChange::default();
        change.changed.push(batch);
        let mut changeset = ChangeSet::new();
        changeset.insert("node:Doc".to_string(), change);
        changeset
    }

    fn collect_value_constraints(changeset: &ChangeSet, catalog: &Catalog) -> Vec<Violation> {
        let mut violations = Vec::new();
        evaluate_value_constraints_with_sink(changeset, catalog, &mut |violation| {
            violations.push(violation);
            Ok(())
        })
        .expect("the test violation collector is infallible");
        violations
    }

    /// The merge path previously validated `@range`/`@check` but NOT enum
    /// membership, so a delta carrying an out-of-enum value slipped through (W1).
    /// The unified evaluator runs the enum check the write path always ran.
    #[test]
    fn evaluator_flags_out_of_enum_value_in_delta() {
        let v = collect_value_constraints(&status_change(&["bogus"]), &catalog(DOC_SCHEMA));
        assert_eq!(v.len(), 1, "expected one enum violation, got {v:?}");
        assert_eq!(v[0].kind, MergeConflictKind::ValueConstraintViolation);
        assert!(
            v[0].message.contains("bogus"),
            "message was: {}",
            v[0].message
        );
    }

    #[test]
    fn evaluator_accepts_valid_delta() {
        assert!(
            collect_value_constraints(&status_change(&["draft"]), &catalog(DOC_SCHEMA)).is_empty()
        );
    }

    #[tokio::test]
    async fn sink_evaluator_matches_collecting_order_and_messages() {
        let catalog = catalog(DOC_SCHEMA);
        let constraints = vec![
            Constraint::Value,
            Constraint::Unique {
                table_key: "node:Doc".to_string(),
                columns: vec!["slug".to_string()],
                is_key: true,
            },
        ];
        let changeset = duplicate_slug_change();
        let committed = CommittedState {
            committed: None,
            overwritten: HashSet::new(),
            live: None,
        };

        let collected = evaluate(&constraints, &changeset, &committed, &catalog)
            .await
            .unwrap();
        let mut streamed = Vec::new();
        let count = evaluate_with_sink(
            &constraints,
            &changeset,
            &committed,
            &catalog,
            |violation| {
                streamed.push(violation);
                Ok(())
            },
        )
        .await
        .unwrap();

        let probed = evaluate_with_access(
            &constraints,
            &changeset,
            &committed,
            &catalog,
            &AccessPaths::probe_everywhere(),
        )
        .await
        .unwrap();

        assert_eq!(count, collected.len());
        assert_eq!(collected.len(), 3, "one value then two unique violations");
        assert_eq!(
            collected
                .iter()
                .map(|violation| (&violation.kind, violation.row_id.as_deref()))
                .collect::<Vec<_>>(),
            vec![
                (&MergeConflictKind::ValueConstraintViolation, None),
                (&MergeConflictKind::UniqueViolation, Some("doc-1")),
                (&MergeConflictKind::UniqueViolation, Some("doc-3")),
            ]
        );
        assert_eq!(streamed, collected);
        assert_eq!(probed, collected);
    }

    const GRAPH_SCHEMA: &str = "node Person {\n  name: String @key\n  email: String @unique\n}\nedge Knows: Person -> Person\n";

    const COMMITTED_PEOPLE: &str = r#"{"type": "Person", "data": {"name": "Alice", "email": "alice@x"}}
{"type": "Person", "data": {"name": "Bob", "email": "bob@x"}}
{"type": "Person", "data": {"name": "Carol", "email": "carol@x"}}
{"type": "Person", "data": {"name": "Dave", "email": "dave@x"}}
"#;

    /// A graph at `dir` whose committed `Person` table holds
    /// [`COMMITTED_PEOPLE`]; a `@key` node's id is its name.
    async fn committed_people(dir: &tempfile::TempDir) -> Session {
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(dir.path().to_str().unwrap(), GRAPH_SCHEMA)
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        db.load_jsonl(COMMITTED_PEOPLE, LoadMode::Overwrite)
            .await
            .unwrap();
        db
    }

    /// A `node:Person` batch of `(name, email)` rows, id = name.
    fn person_batch(sc: SystemColumns, rows: &[(&str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(sc.id, DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]));
        let names: Vec<&str> = rows.iter().map(|(name, _)| *name).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names.clone())) as _,
                Arc::new(StringArray::from(names)) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, email)| *email).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    /// An `edge:Knows` batch of `(id, src, dst)` rows.
    fn knows_batch(sc: SystemColumns, rows: &[(&str, &str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(sc.id, DataType::Utf8, false),
            Field::new(sc.src, DataType::Utf8, false),
            Field::new(sc.dst, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, src, _)| *src).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, dst)| *dst).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    /// Every constraint that offers `Scan` takes it.
    fn scan_everywhere(constraints: &[Constraint]) -> AccessPaths {
        let mut access = AccessPaths::probe_everywhere();
        for constraint in constraints {
            if constraint.access_paths().contains(&AccessPath::Scan) {
                access.choose(constraint, AccessPath::Scan).unwrap();
            }
        }
        access
    }

    #[tokio::test]
    async fn scan_and_probe_access_paths_report_the_same_violations() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_people(&dir).await;
        let catalog = db.catalog();
        let sc = catalog.system_columns;
        let snapshot = db.snapshot().await;
        let committed = CommittedState::merge(&snapshot);
        let constraints = constraints_for(&catalog);

        let mut people = TableChange::default();
        people.added.push(person_batch(
            sc,
            &[
                ("Zed", "alice@x"),
                ("Bob", "bob@x"),
                ("Yan", "carol@x"),
                ("Xia", "dave@x"),
                ("Wu", "wu@x"),
            ],
        ));
        people.deleted_ids.push("Dave".to_string());
        let mut knows = TableChange::default();
        knows.added.push(knows_batch(
            sc,
            &[
                ("e1", "Alice", "Bob"),
                ("e2", "Alice", "Nobody"),
                ("e3", "Ghost", "Bob"),
                ("e4", "Zed", "Alice"),
                ("e5", "Dave", "Alice"),
            ],
        ));
        let mut changeset = ChangeSet::new();
        changeset.insert("node:Person".to_string(), people);
        changeset.insert("edge:Knows".to_string(), knows);

        let probed = evaluate(&constraints, &changeset, &committed, &catalog)
            .await
            .unwrap();
        let scanned = evaluate_with_access(
            &constraints,
            &changeset,
            &committed,
            &catalog,
            &scan_everywhere(&constraints),
        )
        .await
        .unwrap();

        assert_eq!(scanned, probed);
        let (src, dst) = (sc.src, sc.dst);
        let e2 = format!("{dst} 'Nobody' not found in Person");
        let e3 = format!("{src} 'Ghost' not found in Person");
        let e5 = format!("{src} 'Dave' not found in Person");
        assert_eq!(
            probed
                .iter()
                .map(|v| (v.kind, v.row_id.as_deref(), v.message.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (
                    MergeConflictKind::UniqueViolation,
                    Some("Yan"),
                    "@unique violation on Person.email: value 'carol@x' held by 'Carol' and 'Yan'",
                ),
                (
                    MergeConflictKind::UniqueViolation,
                    Some("Zed"),
                    "@unique violation on Person.email: value 'alice@x' held by 'Alice' and 'Zed'",
                ),
                (MergeConflictKind::OrphanEdge, Some("e2"), e2.as_str()),
                (MergeConflictKind::OrphanEdge, Some("e3"), e3.as_str()),
                (MergeConflictKind::OrphanEdge, Some("e5"), e5.as_str()),
            ]
        );
    }

    #[test]
    fn access_paths_refuse_a_path_the_constraint_does_not_offer() {
        let catalog = catalog(GRAPH_SCHEMA);
        let constraints = constraints_for(&catalog);
        let mut access = AccessPaths::probe_everywhere();
        for constraint in &constraints {
            let offered = constraint.access_paths();
            for path in [AccessPath::Probe, AccessPath::Scan] {
                assert_eq!(
                    access.choose(constraint, path).is_ok(),
                    offered.contains(&path),
                    "{constraint:?} {path:?}"
                );
            }
            assert_eq!(
                access.path_for(constraint),
                if offered.contains(&AccessPath::Scan) {
                    AccessPath::Scan
                } else {
                    AccessPath::Probe
                }
            );
        }
        assert_eq!(
            constraints
                .iter()
                .map(|constraint| constraint.access_paths().len())
                .collect::<Vec<_>>(),
            vec![0, 0, 2, 2, 1],
            "Value, Person.name @key, Person.email, Knows RI, Knows @card"
        );
    }

    #[tokio::test]
    async fn sink_evaluator_propagates_sink_failure_without_buffering_later_violations() {
        let catalog = catalog(DOC_SCHEMA);
        let constraints = vec![Constraint::Unique {
            table_key: "node:Doc".to_string(),
            columns: vec!["slug".to_string()],
            is_key: true,
        }];
        let changeset = duplicate_slug_change();
        let committed = CommittedState {
            committed: None,
            overwritten: HashSet::new(),
            live: None,
        };
        let mut observed = Vec::new();

        let error = evaluate_with_sink(
            &constraints,
            &changeset,
            &committed,
            &catalog,
            |violation| {
                observed.push(violation.row_id);
                Err(OmniError::manifest("test sink stopped"))
            },
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("test sink stopped"));
        assert_eq!(observed, [Some("doc-1".to_string())]);
    }

    #[test]
    fn evaluator_ignores_empty_changeset() {
        assert!(collect_value_constraints(&ChangeSet::new(), &catalog(DOC_SCHEMA)).is_empty());
    }
}
