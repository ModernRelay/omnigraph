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

use std::collections::{HashMap, HashSet};

use arrow_array::{Array, RecordBatch, StringArray};
use datafusion::prelude::{Expr, col, lit};
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lance::Dataset;
use omnigraph_compiler::catalog::{Catalog, EdgeType};

use crate::db::{Omnigraph, Snapshot};
use crate::error::{MergeConflict, MergeConflictKind, OmniError, Result};
use crate::loader::{
    composite_unique_key, format_tuple, validate_enum_constraints, validate_value_constraints,
};
use crate::table_store::TableStore;

/// A single integrity violation, surface-neutral. Maps to the merge path's
/// [`MergeConflict`] today via [`Violation::into_merge_conflict`]; a write-path
/// `into_omni_error` mapping is added when the write path migrates.
#[derive(Debug, Clone)]
pub(crate) struct Violation {
    pub table_key: String,
    pub row_id: Option<String>,
    pub kind: MergeConflictKind,
    pub message: String,
}

impl Violation {
    pub(crate) fn into_merge_conflict(self) -> MergeConflict {
        MergeConflict {
            table_key: self.table_key,
            row_id: self.row_id,
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
/// enum check here is what closes the merge-vs-write drift.
pub(crate) fn evaluate_value_constraints(changeset: &ChangeSet, catalog: &Catalog) -> Vec<Violation> {
    let mut violations = Vec::new();
    for (table_key, change) in changeset {
        if let Some(type_name) = table_key.strip_prefix("node:") {
            let Some(node_type) = catalog.node_types.get(type_name) else {
                continue;
            };
            for batch in change.value_batches() {
                if let Err(err) = validate_value_constraints(batch, node_type) {
                    violations.push(value_violation(table_key, err));
                }
                if let Err(err) = validate_enum_constraints(batch, &node_type.properties, type_name) {
                    violations.push(value_violation(table_key, err));
                }
            }
        } else if let Some(type_name) = table_key.strip_prefix("edge:") {
            let Some(edge_type) = catalog.edge_types.get(type_name) else {
                continue;
            };
            // Edges carry no @range/@check (NodeType-only), but their properties
            // can be enum-typed — the check the merge path was missing.
            for batch in change.value_batches() {
                if let Err(err) = validate_enum_constraints(batch, &edge_type.properties, type_name) {
                    violations.push(value_violation(table_key, err));
                }
            }
        }
    }
    violations
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
/// `node_prop_index_kind` chokepoint: adding a kind is one variant + one arm in
/// [`evaluate`], run on every surface that adopts the evaluator.
#[derive(Debug, Clone)]
pub(crate) enum Constraint {
    /// Row-local value/enum validation across the whole change-set (one entry
    /// covers every table; handled by [`evaluate_value_constraints`]).
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

/// Derive the runtime constraint set from the catalog (the schema's declared
/// invariants). One `Value` plus one entry per `@unique` group, edge-RI, and
/// `@card` edge.
pub(crate) fn constraints_for(catalog: &Catalog) -> Vec<Constraint> {
    let mut out = vec![Constraint::Value];
    for (name, node_type) in &catalog.node_types {
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
    for (name, edge_type) in &catalog.edge_types {
        let table_key = format!("edge:{name}");
        // Edges have no `@key`; every `@unique` group needs the committed lookup.
        for columns in &edge_type.unique_constraints {
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
    pub(crate) fn write(committed: &'a Snapshot, db: &'a Omnigraph, branch: Option<&'a str>) -> Self {
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
        match committed.entry(table_key) {
            Some(_) => Ok(Some(committed.open_dataset(table_key).await?)),
            None => Ok(None),
        }
    }

    /// Open an edge table for cardinality counting: the current manifest-visible
    /// graph-branch snapshot on the write path (so a concurrent published edge
    /// is counted — #298), the pinned committed snapshot otherwise. Resolving
    /// through the fresh graph snapshot is load-bearing for first-touch named
    /// branches: their table still inherits another Lance ref until this write's
    /// sidecar is armed, so opening the target ref directly would be invalid.
    async fn open_cardinality(&self, table_key: &str) -> Result<Option<Dataset>> {
        if self.overwritten.contains(table_key) {
            return Ok(None);
        }
        let Some(committed) = self.committed else {
            return Ok(None);
        };
        let Some(_entry) = committed.entry(table_key) else {
            return Ok(None);
        };
        match self.live {
            Some((db, branch)) => {
                // `CommittedState::write` is constructed only after WriteTxn
                // schema validation, so use the unchecked manifest refresh to
                // avoid another full contract read while retaining live branch
                // authority. Snapshot::open follows the entry's actual
                // `table_branch` and pinned version (including inheritance).
                let live = db.fresh_snapshot_for_branch_unchecked(branch).await?;
                match live.entry(table_key) {
                    Some(_) => Ok(Some(live.open_dataset(table_key).await?)),
                    None => Ok(None),
                }
            }
            None => Ok(Some(committed.open_dataset(table_key).await?)),
        }
    }

    /// Which of `ids` exist as committed rows in `table_key` (by `id`).
    async fn existing_ids(&self, table_key: &str, ids: &[String]) -> Result<HashSet<String>> {
        let Some(ds) = self.open(table_key).await? else {
            return Ok(HashSet::new());
        };
        if ids.is_empty() {
            return Ok(HashSet::new());
        }
        let expr = col("id").in_list(ids.iter().map(|k| lit(k.clone())).collect(), false);
        let batches = scan_filtered(&ds, &["id"], expr).await?;
        let mut present = HashSet::new();
        for batch in &batches {
            let column = string_col(batch, "id")?;
            for i in 0..column.len() {
                if !column.is_null(i) {
                    present.insert(column.value(i).to_string());
                }
            }
        }
        Ok(present)
    }

    /// Ids of committed rows in `table_key` whose `columns` tuple equals `key`.
    /// Used to detect a cross-version unique collision (the one constraint the
    /// write path does not enforce, so it is load-bearing at merge).
    async fn unique_holders(
        &self,
        table_key: &str,
        columns: &[String],
        key_values: &[ScalarValue],
    ) -> Result<Vec<String>> {
        let Some(ds) = self.open(table_key).await? else {
            return Ok(Vec::new());
        };
        // AND of per-column equality so each indexed column is served by its
        // BTREE (a non-indexed `@unique` column falls back to a scan). The
        // literal is TYPED (built from the row's Arrow column), so the
        // pushed-down filter compares like-typed. A stringified key would push a
        // Utf8 literal against a typed column — a coercion error on Date/Bool
        // (breaking every write) or a silent miss on Float.
        let mut expr: Option<Expr> = None;
        for (column, value) in columns.iter().zip(key_values.iter()) {
            let eq = col(column.as_str()).eq(lit(value.clone()));
            expr = Some(match expr {
                Some(acc) => acc.and(eq),
                None => eq,
            });
        }
        let Some(expr) = expr else {
            return Ok(Vec::new());
        };
        let batches = scan_filtered(&ds, &["id"], expr).await?;
        let mut ids = Vec::new();
        for batch in &batches {
            let column = string_col(batch, "id")?;
            for i in 0..column.len() {
                if !column.is_null(i) {
                    ids.push(column.value(i).to_string());
                }
            }
        }
        Ok(ids)
    }

    /// Committed edges `(id, src)` in `edge_table` matching `keys` on `key_col`
    /// (`"id"` or `"src"`). Index-backed.
    async fn committed_edges(
        &self,
        edge_table: &str,
        key_col: &str,
        keys: &[String],
    ) -> Result<Vec<(String, String)>> {
        let Some(ds) = self.open_cardinality(edge_table).await? else {
            return Ok(Vec::new());
        };
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let expr = col(key_col).in_list(keys.iter().map(|k| lit(k.clone())).collect(), false);
        let batches = scan_filtered(&ds, &["id", "src"], expr).await?;
        let mut out = Vec::new();
        for batch in &batches {
            let ids = string_col(batch, "id")?;
            let srcs = string_col(batch, "src")?;
            for i in 0..batch.num_rows() {
                out.push((ids.value(i).to_string(), srcs.value(i).to_string()));
            }
        }
        Ok(out)
    }

    /// Committed edges `(id, src, dst)` in `edge_table` whose src is in
    /// `src_nodes` OR dst is in `dst_nodes` — the edges a node deletion would
    /// strand. Index-backed (BTREE on src/dst).
    async fn edges_referencing(
        &self,
        edge_table: &str,
        src_nodes: &[String],
        dst_nodes: &[String],
    ) -> Result<Vec<(String, String, String)>> {
        if src_nodes.is_empty() && dst_nodes.is_empty() {
            return Ok(Vec::new());
        }
        let Some(ds) = self.open(edge_table).await? else {
            return Ok(Vec::new());
        };
        let mut expr: Option<Expr> = None;
        if !src_nodes.is_empty() {
            expr =
                Some(col("src").in_list(src_nodes.iter().map(|k| lit(k.clone())).collect(), false));
        }
        if !dst_nodes.is_empty() {
            let dst = col("dst").in_list(dst_nodes.iter().map(|k| lit(k.clone())).collect(), false);
            expr = Some(match expr {
                Some(acc) => acc.or(dst),
                None => dst,
            });
        }
        let batches = scan_filtered(&ds, &["id", "src", "dst"], expr.unwrap()).await?;
        let mut out = Vec::new();
        for batch in &batches {
            let ids = string_col(batch, "id")?;
            let srcs = string_col(batch, "src")?;
            let dsts = string_col(batch, "dst")?;
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
    .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Scan `projection` from every row (no filter). Used to enumerate a table's
/// committed ids when computing what an `Overwrite` removes.
async fn scan_all(ds: &Dataset, projection: &[&str]) -> Result<Vec<RecordBatch>> {
    TableStore::scan_stream_with(ds, Some(projection), None, None, false, |_| Ok(()))
        .await?
        .try_collect()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
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
) -> Result<Vec<String>> {
    if base.entry(table_key).is_none() {
        return Ok(Vec::new());
    }
    let mut new_ids: HashSet<String> = HashSet::new();
    for batch in change.value_batches() {
        let column = string_col(batch, "id")?;
        for i in 0..column.len() {
            if !column.is_null(i) {
                new_ids.insert(column.value(i).to_string());
            }
        }
    }
    let ds = base.open_dataset(table_key).await?;
    let mut removed = Vec::new();
    for batch in &scan_all(&ds, &["id"]).await? {
        let column = string_col(batch, "id")?;
        for i in 0..column.len() {
            if !column.is_null(i) && !new_ids.contains(column.value(i)) {
                removed.push(column.value(i).to_string());
            }
        }
    }
    Ok(removed)
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
fn delta_id_set(change: &TableChange) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for batch in change.value_batches() {
        let column = string_col(batch, "id")?;
        for i in 0..column.len() {
            if !column.is_null(i) {
                ids.insert(column.value(i).to_string());
            }
        }
    }
    Ok(ids)
}

/// `(edge_id, src)` for a table's added∪changed delta edge rows.
fn delta_edge_src(change: &TableChange) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for batch in change.value_batches() {
        let ids = string_col(batch, "id")?;
        let srcs = string_col(batch, "src")?;
        for i in 0..batch.num_rows() {
            out.push((ids.value(i).to_string(), srcs.value(i).to_string()));
        }
    }
    Ok(out)
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
    let constraints = constraints_for(catalog);
    let violations = evaluate(&constraints, changeset, committed, catalog).await?;
    match violations.into_iter().next() {
        Some(violation) => Err(violation.into_omni_error()),
        None => Ok(()),
    }
}

/// Run the declared constraints over the merge delta against committed state.
/// Δ-scoped: only tables present in `changeset` do any work.
pub(crate) async fn evaluate(
    constraints: &[Constraint],
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
    catalog: &Catalog,
) -> Result<Vec<Violation>> {
    let mut violations = Vec::new();
    for constraint in constraints {
        match constraint {
            Constraint::Value => {
                violations.extend(evaluate_value_constraints(changeset, catalog));
            }
            Constraint::Unique {
                table_key,
                columns,
                is_key,
            } => {
                if let Some(change) = changeset.get(table_key) {
                    violations.extend(
                        evaluate_unique(table_key, columns, *is_key, change, committed).await?,
                    );
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
                    violations.extend(
                        evaluate_edge_ri(table_key, from_type, to_type, change, changeset, committed)
                            .await?,
                    );
                }
            }
            Constraint::Cardinality { table_key } => {
                if let Some(change) = changeset.get(table_key) {
                    let Some(type_name) = table_key.strip_prefix("edge:") else {
                        continue;
                    };
                    if let Some(edge_type) = catalog.edge_types.get(type_name) {
                        violations.extend(
                            evaluate_cardinality(table_key, edge_type, change, changeset, committed)
                                .await?,
                        );
                    }
                }
            }
        }
    }
    Ok(violations)
}

/// Uniqueness for one `@unique`/`@key` group on `table_key`, evaluated against
/// the delta's FINAL coalesced image (last-wins per id) — the same image commit
/// persists. Three checks:
/// 1. **within ONE batch** — two distinct input records sharing a key (a bulk
///    load listing the same `@key`/`@unique` value twice). Always a violation,
///    even with the same id (a load has no ordering), and coalescing would hide
///    it — so it is checked per-batch first.
/// 2. **across the coalesced image** — two DISTINCT ids holding the same final
///    key. Coalescing by id means a read-your-writes update that changes a row's
///    key (`temp -> final`) releases the old key, so a later row may reuse it.
/// 3. **committed cross-version** (non-`@key`) — a final key colliding with a
///    SURVIVING committed row (not itself in the delta, not deleted). `@key` is
///    id-backed, so a committed holder of a key value is the same row (an
///    upsert) — self-excluded — so the probe is skipped.
async fn evaluate_unique(
    table_key: &str,
    columns: &[String],
    is_key: bool,
    change: &TableChange,
    committed: &CommittedState<'_>,
) -> Result<Vec<Violation>> {
    let mut violations = Vec::new();
    let delta_ids = delta_id_set(change)?;
    let deleted: HashSet<&String> = change.deleted_ids.iter().collect();

    // Pass 1: per-batch within-batch dup detection AND coalesce the delta by id
    // (last-wins) into each id's final (key, typed values). A row whose key
    // became null removes the id (it no longer holds a unique key).
    let mut final_by_id: HashMap<String, (Vec<String>, Vec<ScalarValue>)> = HashMap::new();
    for batch in change.value_batches() {
        let group_columns = columns
            .iter()
            .map(|name| {
                batch.column_by_name(name).cloned().ok_or_else(|| {
                    OmniError::manifest(format!("table {table_key} missing unique column '{name}'"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let ids = string_col(batch, "id")?;
        let mut seen_in_batch: HashMap<Vec<String>, String> = HashMap::new();
        for row in 0..batch.num_rows() {
            let id = ids.value(row).to_string();
            let Some(key) = composite_unique_key(&group_columns, row)? else {
                final_by_id.remove(&id);
                continue;
            };
            if let Some(prior) = seen_in_batch.insert(key.clone(), id.clone()) {
                violations.push(unique_violation(table_key, columns, &key, &id, &prior));
            }
            // Typed literals from the row's Arrow columns for the committed probe
            // (a stringified key would compare a typed column to Utf8). `key` is
            // `Some`, so every column is non-null and `try_from_array` is concrete.
            let values = group_columns
                .iter()
                .map(|arr| ScalarValue::try_from_array(arr, row))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| OmniError::manifest(e.to_string()))?;
            final_by_id.insert(id, (key, values));
        }
    }
    // A within-batch duplicate is an unambiguous bulk-input error; report it
    // without the coalesced cross-row pass (which would re-report the same pair).
    if !violations.is_empty() {
        return Ok(violations);
    }

    // Deterministic order — no HashMap iteration in violation ordering.
    let mut entries: Vec<(String, (Vec<String>, Vec<ScalarValue>))> =
        final_by_id.into_iter().collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Pass 2: two DISTINCT ids holding the same final key.
    let mut holder_by_key: HashMap<&Vec<String>, &String> = HashMap::new();
    for (id, (key, _)) in &entries {
        if let Some(other) = holder_by_key.insert(key, id) {
            if other != id {
                violations.push(unique_violation(table_key, columns, key, id, other));
            }
        }
    }

    // Pass 3: committed cross-version (non-`@key` only).
    if !is_key {
        for (id, (key, values)) in &entries {
            for holder in committed.unique_holders(table_key, columns, values).await? {
                if !delta_ids.contains(&holder) && !deleted.contains(&holder) {
                    violations.push(unique_violation(table_key, columns, key, id, &holder));
                    break;
                }
            }
        }
    }
    Ok(violations)
}

/// Edge referential integrity for added∪changed edges: each endpoint must exist
/// in the MERGED node universe (`target ± delta`). The path-b case — a deleted
/// node stranding a pre-existing committed edge — is unreachable here: `mutate`
/// cascades a node delete to its edges and `load` validates RI, so a surviving
/// edge can never reference a node the same merge deleted (it would either be
/// cascade-removed or surface as a structural `DeleteVsUpdate`). So checking the
/// edge delta is sufficient and equivalent to the old full scan on all reachable
/// inputs.
async fn evaluate_edge_ri(
    edge_table: &str,
    from_type: &str,
    to_type: &str,
    change: Option<&TableChange>,
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
) -> Result<Vec<Violation>> {
    let from_table = format!("node:{from_type}");
    let to_table = format!("node:{to_type}");
    let mut violations = Vec::new();
    // Delta edge ids — excluded from path-b (path-a already covers them).
    let mut delta_edge_ids: HashSet<String> = HashSet::new();

    // Path-a: each added/changed edge's endpoints must exist in the merged node
    // universe (`target ± delta`).
    if let Some(change) = change {
        let mut edges = Vec::new();
        for batch in change.value_batches() {
            let ids = string_col(batch, "id")?;
            let srcs = string_col(batch, "src")?;
            let dsts = string_col(batch, "dst")?;
            for i in 0..batch.num_rows() {
                let id = ids.value(i).to_string();
                delta_edge_ids.insert(id.clone());
                edges.push((id, srcs.value(i).to_string(), dsts.value(i).to_string()));
            }
        }
        if !edges.is_empty() {
            let srcs: Vec<String> = edges.iter().map(|(_, src, _)| src.clone()).collect();
            let dsts: Vec<String> = edges.iter().map(|(_, _, dst)| dst.clone()).collect();
            let from_exist = merged_node_existence(&from_table, &srcs, changeset, committed).await?;
            let to_exist = merged_node_existence(&to_table, &dsts, changeset, committed).await?;
            for (id, src, dst) in &edges {
                if !from_exist.contains(src) {
                    violations.push(orphan_violation(edge_table, id, "src", src, from_type));
                }
                if !to_exist.contains(dst) {
                    violations.push(orphan_violation(edge_table, id, "dst", dst, to_type));
                }
            }
        }
    }

    // Path-b: a node deleted by this merge can strand a committed (target) edge
    // the merge keeps — reachable when the edge lives on the target side and the
    // node deletion on the source side, so the edge is neither cascade-removed
    // nor in this table's delta. Probe committed target edges referencing the
    // deleted nodes; any that survive (not in the delta, not removed) are orphans.
    let deleted_from: Vec<String> = changeset
        .get(&from_table)
        .map(|change| change.deleted_ids.clone())
        .unwrap_or_default();
    let deleted_to: Vec<String> = changeset
        .get(&to_table)
        .map(|change| change.deleted_ids.clone())
        .unwrap_or_default();
    if !deleted_from.is_empty() || !deleted_to.is_empty() {
        let removed: HashSet<&String> = change
            .map(|change| change.deleted_ids.iter().collect())
            .unwrap_or_default();
        let from_set: HashSet<&String> = deleted_from.iter().collect();
        let to_set: HashSet<&String> = deleted_to.iter().collect();
        for (id, src, dst) in committed
            .edges_referencing(edge_table, &deleted_from, &deleted_to)
            .await?
        {
            if delta_edge_ids.contains(&id) || removed.contains(&id) {
                continue;
            }
            if from_set.contains(&src) {
                violations.push(orphan_violation(edge_table, &id, "src", &src, from_type));
            }
            if to_set.contains(&dst) {
                violations.push(orphan_violation(edge_table, &id, "dst", &dst, to_type));
            }
        }
    }

    Ok(violations)
}

/// Which of `ids` exist in the merged node table `node_table` = `target ± delta`:
/// present if added/changed in the delta, absent if deleted, else an index probe
/// of the committed target.
async fn merged_node_existence(
    node_table: &str,
    ids: &[String],
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
) -> Result<HashSet<String>> {
    let (added_changed, deleted) = match changeset.get(node_table) {
        Some(change) => (
            delta_id_set(change)?,
            change.deleted_ids.iter().cloned().collect::<HashSet<_>>(),
        ),
        None => (HashSet::new(), HashSet::new()),
    };
    let mut exist = HashSet::new();
    let mut to_probe = Vec::new();
    for id in ids {
        if added_changed.contains(id) {
            exist.insert(id.clone());
        } else if !deleted.contains(id) {
            to_probe.push(id.clone());
        }
    }
    for id in committed.existing_ids(node_table, &to_probe).await? {
        exist.insert(id);
    }
    Ok(exist)
}

/// `@card` for an edge type, scoped to the srcs the delta affects. The delta is
/// coalesced by edge id (last-wins, as commit does); the merged edge set per src
/// = (committed edges with that src, minus those deleted or re-placed by the
/// delta) ∪ (coalesced delta edges with that src). The affected set includes the
/// new src of each delta edge AND the old committed src of each changed/deleted
/// edge id, so moving an edge off a src recounts the vacated src. A src that is
/// itself a deleted node is skipped.
async fn evaluate_cardinality(
    edge_table: &str,
    edge_type: &EdgeType,
    change: &TableChange,
    changeset: &ChangeSet,
    committed: &CommittedState<'_>,
) -> Result<Vec<Violation>> {
    let card = &edge_type.cardinality;
    // Default unbounded cardinality can never be violated — skip the lookups.
    if card.min == 0 && card.max.is_none() {
        return Ok(Vec::new());
    }
    let delta_edges = delta_edge_src(change)?;
    let removed_ids: Vec<String> = change.deleted_ids.clone();
    let removed_id_set: HashSet<&String> = removed_ids.iter().collect();

    // Coalesce the delta by edge id, last-wins — matching commit's
    // `dedupe_merge_batches_by_id`. A Merge load can list the same edge id twice
    // with different srcs; commit keeps the last, so counting raw delta rows
    // would place one id under multiple srcs and over-count.
    let mut delta_by_id: HashMap<String, String> = HashMap::new();
    for (id, src) in &delta_edges {
        delta_by_id.insert(id.clone(), src.clone());
    }
    let changed_ids: Vec<String> = delta_by_id.keys().cloned().collect();
    let delta_id_set: HashSet<&String> = changed_ids.iter().collect();

    // Committed srcs of the edges this delta touches. `removed_edges` are the
    // deleted ids (direct deletes; a node-delete cascade already lands those ids
    // in `deleted_ids`). `moved_from` are the changed ids' OLD committed srcs: an
    // upsert that moves an edge's src vacates its old src, which must be
    // recounted or a drop below @card min is missed.
    let removed_edges = committed.committed_edges(edge_table, "id", &removed_ids).await?;
    let moved_from = committed.committed_edges(edge_table, "id", &changed_ids).await?;

    let deleted_src_nodes: HashSet<String> = changeset
        .get(&format!("node:{}", edge_type.from_type))
        .map(|change| change.deleted_ids.iter().cloned().collect())
        .unwrap_or_default();

    let mut affected: HashSet<String> = HashSet::new();
    for src in delta_by_id.values() {
        affected.insert(src.clone());
    }
    for (_, src) in removed_edges.iter().chain(moved_from.iter()) {
        affected.insert(src.clone());
    }
    affected.retain(|src| !deleted_src_nodes.contains(src));
    if affected.is_empty() {
        return Ok(Vec::new());
    }

    let affected_vec: Vec<String> = affected.iter().cloned().collect();
    let committed_for_affected = committed
        .committed_edges(edge_table, "src", &affected_vec)
        .await?;

    // Merged edge-id set per src. A committed edge is dropped from its src when
    // the delta deletes it (`removed_id_set`) OR re-places it (`delta_id_set` — a
    // changed edge is recounted at its new src below, so its old src must not
    // keep counting it). Then add the coalesced delta edges at their last-wins
    // src. Counting by id keeps the validated set equal to what commit persists.
    let mut per_src: HashMap<String, HashSet<String>> = HashMap::new();
    for (id, src) in &committed_for_affected {
        if removed_id_set.contains(id) || delta_id_set.contains(id) {
            continue;
        }
        per_src.entry(src.clone()).or_default().insert(id.clone());
    }
    for (id, src) in &delta_by_id {
        per_src.entry(src.clone()).or_default().insert(id.clone());
    }

    let mut violations = Vec::new();
    for src in &affected {
        let count = per_src.get(src).map(|ids| ids.len() as u32).unwrap_or(0);
        if let Some(max) = card.max {
            if count > max {
                violations.push(cardinality_violation(
                    edge_table,
                    &edge_type.name,
                    src,
                    count,
                    "max",
                    max,
                ));
            }
        }
        if count < card.min {
            violations.push(cardinality_violation(
                edge_table,
                &edge_type.name,
                src,
                count,
                "min",
                card.min,
            ));
        }
    }
    Ok(violations)
}

/// Canonical `@unique` violation message, matching the write path's format
/// (`validators.rs` asserts the `"@unique violation on {Type}.{cols}"` prefix via
/// `.contains`). `type_name` is the bare type (`User`), not the `node:`/`edge:`
/// table key; `columns`/`key` render via `format_tuple` (single → `email`,
/// composite → `(a, b)`).
fn unique_violation(
    table_key: &str,
    columns: &[String],
    key: &[String],
    id: &str,
    other: &str,
) -> Violation {
    let type_name = table_key
        .strip_prefix("node:")
        .or_else(|| table_key.strip_prefix("edge:"))
        .unwrap_or(table_key);
    Violation {
        table_key: table_key.to_string(),
        row_id: Some(id.to_string()),
        kind: MergeConflictKind::UniqueViolation,
        message: format!(
            "@unique violation on {type_name}.{}: value '{}' held by '{other}' and '{id}'",
            format_tuple(columns),
            format_tuple(key)
        ),
    }
}

/// Canonical orphan-edge message, matching the write path's `"{src|dst} '{id}'
/// not found in {Type}"` format.
fn orphan_violation(
    edge_table: &str,
    edge_id: &str,
    label: &str,
    endpoint: &str,
    node_type: &str,
) -> Violation {
    Violation {
        table_key: edge_table.to_string(),
        row_id: Some(edge_id.to_string()),
        kind: MergeConflictKind::OrphanEdge,
        message: format!("{label} '{endpoint}' not found in {node_type}"),
    }
}

fn cardinality_violation(
    edge_table: &str,
    edge_name: &str,
    src: &str,
    count: u32,
    bound: &str,
    limit: u32,
) -> Violation {
    Violation {
        table_key: edge_table.to_string(),
        row_id: None,
        kind: MergeConflictKind::CardinalityViolation,
        message: format!(
            "@card violation on edge {edge_name}: source '{src}' has {count} edges ({bound} {limit})"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::schema::parser::parse_schema;

    const DOC_SCHEMA: &str = "node Doc {\n  slug: String @key\n  status: enum(draft, published)\n}\n";

    fn catalog(src: &str) -> Catalog {
        build_catalog(&parse_schema(src).unwrap()).unwrap()
    }

    /// A change-set touching only `Doc.status` with the given values.
    fn status_change(values: &[&str]) -> ChangeSet {
        let schema = Arc::new(Schema::new(vec![Field::new("status", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec())) as _])
                .unwrap();
        let mut change = TableChange::default();
        change.changed.push(batch);
        let mut cs = ChangeSet::new();
        cs.insert("node:Doc".to_string(), change);
        cs
    }

    /// The merge path previously validated `@range`/`@check` but NOT enum
    /// membership, so a delta carrying an out-of-enum value slipped through (W1).
    /// The unified evaluator runs the enum check the write path always ran.
    #[test]
    fn evaluator_flags_out_of_enum_value_in_delta() {
        let v = evaluate_value_constraints(&status_change(&["bogus"]), &catalog(DOC_SCHEMA));
        assert_eq!(v.len(), 1, "expected one enum violation, got {v:?}");
        assert_eq!(v[0].kind, MergeConflictKind::ValueConstraintViolation);
        assert!(v[0].message.contains("bogus"), "message was: {}", v[0].message);
    }

    #[test]
    fn evaluator_accepts_valid_delta() {
        assert!(
            evaluate_value_constraints(&status_change(&["draft"]), &catalog(DOC_SCHEMA)).is_empty()
        );
    }

    /// Δ-scoping: an empty change-set does no work and raises nothing —
    /// validation cost follows the delta, not the table size.
    #[test]
    fn evaluator_ignores_empty_changeset() {
        assert!(evaluate_value_constraints(&ChangeSet::new(), &catalog(DOC_SCHEMA)).is_empty());
    }
}
