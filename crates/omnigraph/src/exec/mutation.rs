use super::*;

use super::query::literal_to_sql;

// ─── Mutation helpers ────────────────────────────────────────────────────────

/// Resolve an IRExpr to a concrete Literal value at runtime.
fn resolve_expr_value(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    match expr {
        IRExpr::Literal(lit) => Ok(lit.clone()),
        IRExpr::Param(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| OmniError::manifest(format!("parameter '{}' not provided", name))),
        other => Err(OmniError::manifest(format!(
            "unsupported expression in mutation: {:?}",
            other
        ))),
    }
}

/// Create a single-element or N-element array from a Literal, matching the target DataType.
fn literal_to_typed_array(
    lit: &Literal,
    data_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    Ok(match (lit, data_type) {
        (Literal::Null, _) => arrow_array::new_null_array(data_type, num_rows),
        (Literal::String(s), DataType::Utf8) => {
            Arc::new(StringArray::from(vec![s.as_str(); num_rows])) as ArrayRef
        }
        (Literal::Integer(n), DataType::Int32) => {
            Arc::new(Int32Array::from(vec![*n as i32; num_rows]))
        }
        (Literal::Integer(n), DataType::Int64) => Arc::new(Int64Array::from(vec![*n; num_rows])),
        (Literal::Integer(n), DataType::UInt32) => {
            Arc::new(UInt32Array::from(vec![*n as u32; num_rows]))
        }
        (Literal::Integer(n), DataType::UInt64) => {
            Arc::new(UInt64Array::from(vec![*n as u64; num_rows]))
        }
        (Literal::Float(f), DataType::Float32) => {
            Arc::new(Float32Array::from(vec![*f as f32; num_rows]))
        }
        (Literal::Float(f), DataType::Float64) => Arc::new(Float64Array::from(vec![*f; num_rows])),
        (Literal::Bool(b), DataType::Boolean) => Arc::new(BooleanArray::from(vec![*b; num_rows])),
        (Literal::Date(s), DataType::Date32) => {
            let days = crate::loader::parse_date32_literal(s)?;
            Arc::new(Date32Array::from(vec![days; num_rows]))
        }
        (Literal::DateTime(s), DataType::Date64) => Arc::new(Date64Array::from(vec![
            crate::loader::parse_date64_literal(s)?;
            num_rows
        ])),
        (Literal::List(items), DataType::List(field)) => {
            typed_list_literal_to_array(items, field.data_type(), num_rows)?
        }
        (Literal::List(items), DataType::FixedSizeList(field, dim))
            if field.data_type() == &DataType::Float32 =>
        {
            if items.len() != *dim as usize {
                return Err(OmniError::manifest(format!(
                    "vector property expects {} dimensions, got {}",
                    dim,
                    items.len()
                )));
            }
            let mut builder = FixedSizeListBuilder::with_capacity(
                Float32Builder::with_capacity(num_rows * (*dim as usize)),
                *dim,
                num_rows,
            )
            .with_field(field.clone());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f32),
                        Literal::Float(value) => builder.values().append_value(*value as f32),
                        _ => {
                            return Err(OmniError::manifest(
                                "vector elements must be numeric".to_string(),
                            ));
                        }
                    }
                }
                builder.append(true);
            }
            Arc::new(builder.finish())
        }
        _ => {
            return Err(OmniError::manifest(format!(
                "cannot convert {:?} to {:?}",
                lit, data_type
            )));
        }
    })
}

fn typed_list_literal_to_array(
    items: &[Literal],
    item_type: &DataType,
    num_rows: usize,
) -> Result<ArrayRef> {
    match item_type {
        DataType::Utf8 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::String(value) => builder.values().append_value(value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = ListBuilder::new(BooleanBuilder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Bool(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = ListBuilder::new(Int32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => {
                            let value = i32::try_from(*value).map_err(|_| {
                                OmniError::manifest(format!(
                                    "list value {} exceeds Int32 range",
                                    value
                                ))
                            })?;
                            builder.values().append_value(value);
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = ListBuilder::new(Int64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt32 => {
            let mut builder = ListBuilder::new(UInt32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => {
                            let value = u32::try_from(*value).map_err(|_| {
                                OmniError::manifest(format!(
                                    "list value {} exceeds UInt32 range",
                                    value
                                ))
                            })?;
                            builder.values().append_value(value);
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            let mut builder = ListBuilder::new(UInt64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => {
                            let value = u64::try_from(*value).map_err(|_| {
                                OmniError::manifest(format!(
                                    "list value {} exceeds UInt64 range",
                                    value
                                ))
                            })?;
                            builder.values().append_value(value);
                        }
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = ListBuilder::new(Float32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f32),
                        Literal::Float(value) => builder.values().append_value(*value as f32),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = ListBuilder::new(Float64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Integer(value) => builder.values().append_value(*value as f64),
                        Literal::Float(value) => builder.values().append_value(*value),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = ListBuilder::new(Date32Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::Date(value) => builder
                            .values()
                            .append_value(crate::loader::parse_date32_literal(value)?),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date64 => {
            let mut builder = ListBuilder::new(Date64Builder::new());
            for _ in 0..num_rows {
                for item in items {
                    match item {
                        Literal::DateTime(value) => builder
                            .values()
                            .append_value(crate::loader::parse_date64_literal(value)?),
                        _ => builder.values().append_null(),
                    }
                }
                builder.append(true);
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(OmniError::manifest(format!(
            "cannot convert list literal to {:?}",
            other
        ))),
    }
}

/// Build a single-element blob array from a URI or base64 value string.
fn build_blob_array_from_value(value: &str) -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(1);
    crate::loader::append_blob_value(&mut builder, value)?;
    builder
        .finish()
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Build a null blob array with one element.
fn build_null_blob_array() -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(1);
    builder
        .push_null()
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    builder
        .finish()
        .map_err(|e| OmniError::Lance(e.to_string()))
}

/// Build a single-row RecordBatch from resolved assignments.
fn build_insert_batch(
    schema: &SchemaRef,
    id: &str,
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        if field.name() == "id" {
            columns.push(Arc::new(StringArray::from(vec![id])));
        } else if blob_properties.contains(field.name()) {
            if let Some(Literal::String(uri)) = assignments.get(field.name()) {
                columns.push(build_blob_array_from_value(uri)?);
            } else if field.is_nullable() {
                columns.push(build_null_blob_array()?);
            } else {
                return Err(OmniError::manifest(format!(
                    "missing required blob property '{}'",
                    field.name()
                )));
            }
        } else if field.name() == "src" {
            let lit = assignments.get("from").ok_or_else(|| {
                OmniError::manifest("missing required edge endpoint 'from'".to_string())
            })?;
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if field.name() == "dst" {
            let lit = assignments.get("to").ok_or_else(|| {
                OmniError::manifest("missing required edge endpoint 'to'".to_string())
            })?;
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if let Some(lit) = assignments.get(field.name()) {
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if field.is_nullable() {
            columns.push(arrow_array::new_null_array(field.data_type(), 1));
        } else {
            return Err(OmniError::manifest(format!(
                "missing required property '{}'",
                field.name()
            )));
        }
    }

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| OmniError::Lance(e.to_string()))
}

/// Convert an IRMutationPredicate to a Lance SQL filter string.
fn predicate_to_sql(
    predicate: &IRMutationPredicate,
    params: &ParamMap,
    is_edge: bool,
) -> Result<String> {
    let column = if is_edge {
        match predicate.property.as_str() {
            "from" => "src".to_string(),
            "to" => "dst".to_string(),
            other => other.to_string(),
        }
    } else {
        predicate.property.clone()
    };

    let value = resolve_expr_value(&predicate.value, params)?;
    let value_sql = literal_to_sql(&value);

    let op = match predicate.op {
        CompOp::Eq => "=",
        CompOp::Ne => "!=",
        CompOp::Gt => ">",
        CompOp::Lt => "<",
        CompOp::Ge => ">=",
        CompOp::Le => "<=",
        CompOp::Contains => {
            return Err(OmniError::manifest(
                "contains predicate not supported in mutations".to_string(),
            ));
        }
    };

    // #283: emit the column UNQUOTED. Lance's `Scanner::filter(&str)` (the
    // committed-scan consumer) preserves an unquoted identifier's case but
    // treats a double-quoted `"col"` as a string literal, so quoting here
    // would silently match zero committed rows. The pending-batch MemTable
    // query is instead made case-preserving by disabling DataFusion identifier
    // normalization on its `SessionContext` (see `scan_pending_batches`).
    Ok(format!("{} {} {}", column, op, value_sql))
}

/// Replace specific columns in a RecordBatch with new literal values.
///
/// Field-level update: the column plan for a partial-schema update.
struct PartialUpdatePlan {
    /// Accumulated-batch schema: (id + assigned + completion), in catalog
    /// order — the validation change-set's view.
    output_schema: SchemaRef,
    /// Completion columns that ride ONLY the validation change-set and are
    /// projected OUT of the staged merge source (staging them would patch
    /// unassigned columns and prune their indexes). The staged set is derived
    /// at stage time as (batch schema − validation_only), so it stays
    /// consistent with `apply_assignments`' own omissions (e.g. a Blob
    /// assigned a non-String value is omitted from the batch and therefore
    /// never listed for staging).
    validation_only: Vec<String>,
    /// Scan schema: (id + completion-minus-assigned), in catalog order —
    /// the only columns whose OLD values the update needs.
    scan_schema: SchemaRef,
    /// `scan_schema`'s column names (the scan projection).
    scan_cols: Vec<String>,
}

/// Compute the partial-update column plan: the staged source carries the merge
/// key (`id`) and the assigned columns; the validation change-set additionally
/// carries the constraint-completion columns so the end-of-query evaluator can
/// validate whole tuples (an update assigning only `room` of
/// `@unique(room, hour)` must still detect a (room, hour) collision).
///
/// Completion is the FIXED-POINT CLOSURE over every `@unique` group AND the
/// `@key` group (the evaluator registers `@key` as a unique constraint too):
/// overlapping groups chain — assigning `room` pulls in `hour` via
/// `(room, hour)`, which pulls in `day` via `(hour, day)` — and the evaluator
/// hard-errors on a partially present group, so any group intersecting the
/// needed set must be completed in full. Blob columns are excluded from
/// completion/scan end-to-end (they cannot be projected through the scanner;
/// the schema layer also rejects `@unique` on Blob/Vector).
///
/// Columns outside the plan are left physically untouched by the matched-only
/// merge, and every index over them keeps its coverage (Lance prunes only
/// `fields_modified`).
fn partial_update_plan(
    node_type: &omnigraph_compiler::catalog::NodeType,
    full_schema: &SchemaRef,
    resolved: &HashMap<String, Literal>,
) -> PartialUpdatePlan {
    // A blob assigned a non-String value is a no-op on every path (the
    // whole-row path copies the committed value through; historically the
    // column was omitted) — exclude it from the plan so the partial batch
    // neither requires a materializing scan nor patches the blob.
    let assigned: HashSet<&str> = resolved
        .iter()
        .filter(|(name, value)| {
            !(node_type.blob_properties.contains(name.as_str())
                && !matches!(value, Literal::String(_)))
        })
        .map(|(name, _)| name.as_str())
        .collect();

    let mut groups: Vec<&[String]> = node_type
        .unique_constraints
        .iter()
        .map(|g| g.as_slice())
        .collect();
    if let Some(key) = &node_type.key {
        groups.push(key.as_slice());
    }
    let mut needed: HashSet<&str> = assigned.clone();
    loop {
        let mut grew = false;
        for group in &groups {
            if group.iter().any(|c| needed.contains(c.as_str()))
                && !group.iter().all(|c| needed.contains(c.as_str()))
            {
                for c in group.iter() {
                    needed.insert(c.as_str());
                }
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }

    let mut output_fields = Vec::new();
    let mut scan_fields = Vec::new();
    let mut validation_only = Vec::new();
    for field in full_schema.fields() {
        let name = field.name().as_str();
        let is_assigned = assigned.contains(name);
        let is_completion = !is_assigned && name != "id" && needed.contains(name);
        let is_blob = node_type.blob_properties.contains(name);
        if is_completion && is_blob {
            // Defense in depth for pre-guard schemas: a Blob can neither be
            // scanned by projection nor compared by the evaluator's key
            // encoding; the parser now rejects @unique on Blob outright.
            continue;
        }
        if name == "id" || is_assigned || is_completion {
            output_fields.push(field.clone());
        }
        if is_completion {
            validation_only.push(name.to_string());
            scan_fields.push(field.clone());
        } else if name == "id" {
            scan_fields.insert(0, field.clone());
        }
    }
    let scan_cols = scan_fields
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    PartialUpdatePlan {
        output_schema: Arc::new(Schema::new(output_fields)),
        validation_only,
        scan_schema: Arc::new(Schema::new(scan_fields)),
        scan_cols,
    }
}

/// Blob-bearing updates always arrive with the full logical schema. Committed
/// blob payloads were materialized by the caller and rebuilt as logical
/// `Struct<data,uri>` arrays; pending batches already have that shape. An
/// unassigned blob is copied through, while an assigned string URI is rebuilt
/// with the same blob writer used by inserts. Consequently every update batch
/// has the catalog schema and can safely share one pending merge stream with
/// inserts and earlier updates.
fn apply_assignments(
    full_schema: &SchemaRef,
    batch: &RecordBatch,
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(full_schema.fields().len());
    for field in full_schema.fields().iter() {
        if blob_properties.contains(field.name()) {
            if let Some(Literal::String(uri)) = assignments.get(field.name()) {
                // Assigned: build a single blob column from the URI.
                let mut builder = BlobArrayBuilder::new(batch.num_rows());
                for _ in 0..batch.num_rows() {
                    crate::loader::append_blob_value(&mut builder, uri)?;
                }
                columns.push(
                    builder
                        .finish()
                        .map_err(|e| OmniError::Lance(e.to_string()))?,
                );
            } else {
                // Unassigned: the materializing scan must have normalized the
                // committed value (or pending value) to the logical blob
                // schema, so copying it preserves both bytes and full-schema
                // merge compatibility.
                let col = batch.column_by_name(field.name()).ok_or_else(|| {
                    OmniError::Lance(format!(
                        "blob column '{}' not found in full-schema mutation scan",
                        field.name()
                    ))
                })?;
                columns.push(col.clone());
            }
        } else if let Some(lit) = assignments.get(field.name()) {
            columns.push(literal_to_typed_array(
                lit,
                field.data_type(),
                batch.num_rows(),
            )?);
        } else {
            let col = batch.column_by_name(field.name()).ok_or_else(|| {
                OmniError::Lance(format!(
                    "column '{}' not found in scan result",
                    field.name()
                ))
            })?;
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(full_schema.clone(), columns).map_err(|e| OmniError::Lance(e.to_string()))
}

// ─── Mutation execution ──────────────────────────────────────────────────────

use super::staging::{MutationStaging, PendingMode};

/// Open a sub-table dataset for read or staged write within the current
/// mutation query, capturing pre-write metadata in `staging` on first touch.
/// The captured table version is the physical staging baseline. The publisher's
/// logical CAS fence is the enclosing `WriteTxn` authority: native branch
/// identity, exact optional graph head, and accepted schema identity.
///
/// On first touch, resolves the table from the transaction's pinned base.
/// Strict read-modify-write operations open that exact version and retain the
/// early HEAD-vs-pin drift guard; Insert/Merge may skip the physical open and
/// carry only a reclaimable stage plan. Neither path substitutes for the final
/// branch-wide token check under the effect gates.
///
/// On subsequent touches *within the same query*, Lance HEAD has not moved
/// since first touch — inserts, updates AND deletes all stage their work and
/// defer every HEAD advance to the end-of-query commit, so no op inline-commits
/// between touches. A fresh `open_for_mutation_on_branch` therefore still
/// matches the manifest pinned version; we go through it again and `ensure_path`
/// is a no-op (idempotent on the captured `expected_version`). This holds for a
/// delete cascade or multiple delete statements hitting the same table: each
/// touch records another predicate (`record_delete`), and `stage_all` combines
/// them into one staged delete — there is no post-inline-commit reopen to
/// special-case anymore.
impl Omnigraph {}

async fn open_table_for_mutation(
    db: &Omnigraph,
    staging: &mut MutationStaging,
    branch: Option<&str>,
    table_key: &str,
    op_kind: crate::db::MutationOpKind,
    txn: Option<&crate::db::WriteTxn>,
) -> Result<(Option<SnapshotHandle>, String, Option<String>)> {
    // `open_for_mutation_on_branch` returns the expected version even when it
    // skips the open (collapse #1, the non-strict insert/merge path): the version
    // is the pinned base's, identical to the opened handle's `.version()`. Use it
    // directly for `ensure_path` so the no-open path retains its exact physical
    // staging baseline; the branch-wide WriteTxn is the publisher CAS fence.
    let opened = db
        .open_for_mutation_on_branch(branch, table_key, op_kind, txn)
        .await?;
    // Pin the open-skip contract (collapse #1): a missing handle is legal ONLY on
    // the non-strict `txn` path. A future change that returns `None` elsewhere
    // (e.g. a new strict arm) trips this in debug builds rather than silently
    // handing a `None` to a `require_handle` consumer.
    debug_assert!(
        opened.handle.is_some() || (txn.is_some() && !op_kind.strict_pre_stage_version_check()),
        "open_for_mutation_on_branch returned no handle outside the non-strict txn open-skip path",
    );
    staging.ensure_path(
        table_key,
        opened.full_path.clone(),
        opened.table_branch.clone(),
        opened.deferred_fork.clone(),
        opened.expected_version,
        op_kind,
    );
    Ok((opened.handle, opened.full_path, opened.table_branch))
}

/// Build the committed-snapshot filter used to COUNT a delete statement's
/// `affected_*`, excluding rows a prior delete statement on the same table
/// already scheduled for removal in this query.
///
/// Deletes stage — they no longer inline-commit — so every statement in a
/// delete-only query scans the same unchanged committed snapshot. Counting each
/// predicate independently would double-count overlapping statements (the old
/// inline path did not, because each delete committed before the next ran). The
/// combined staged delete actually removes the UNION `p₁ ∪ p₂ ∪ …`; excluding
/// the prior predicates here makes each statement contribute `|pₙ \ (p₁ ∪ …)|`,
/// whose sum is exactly that distinct count. `base` (the original predicate) is
/// still what gets recorded — only the count uses this exclusion.
///
/// LOAD-BEARING on D₂: this exclusion assumes the committed snapshot is
/// invariant across the query's statements, which holds only because D₂
/// (`enforce_no_mixed_destructive_constructive`) forbids mixing inserts/updates
/// with deletes — so a delete-touched table never has pending writes that would
/// shift what a later statement sees. If D₂ is ever relaxed, this dedup must be
/// revisited (a later delete would then need to see prior in-query writes).
///
/// The exclusion uses `IS NOT TRUE`, not `NOT`, because of SQL three-valued
/// logic: a prior predicate referencing a column that is NULL for some row
/// (e.g. `age > 30` on a row with NULL `age`) evaluates to UNKNOWN, and
/// `NOT UNKNOWN` is still UNKNOWN — which a `WHERE` treats as not-matched, so
/// the row would be wrongly dropped from this statement's scan even though the
/// prior delete never matched it (dropping it from `deleted_ids` skips its
/// cascade, or — if it is the only match — leaves the node undeleted). Only
/// rows a prior predicate matched as definitely TRUE should be excluded:
/// `(prior) IS NOT TRUE` keeps both FALSE and UNKNOWN rows.
fn dedup_delete_filter(base: &str, prior: &[String]) -> String {
    if prior.is_empty() {
        base.to_string()
    } else {
        let excluded = prior
            .iter()
            .map(|p| format!("({p})"))
            .collect::<Vec<_>>()
            .join(" OR ");
        format!("({base}) AND (({excluded}) IS NOT TRUE)")
    }
}

/// D₂ parse-time check: a single mutation query is either insert/update-only
/// or delete-only. Mixed → reject before any I/O.
///
/// This is a deliberate semantic boundary, not temporary scaffolding. Inserts
/// and updates accumulate as pending in-memory batches and deletes accumulate
/// as predicates; both stage and commit at end-of-query. Keeping a single query
/// to one kind means read-your-writes stays unambiguous (a read never has to
/// reconcile pending inserts against same-query delete predicates) and each
/// touched table commits at most one version per query. Compose mixed
/// operations by issuing separate atomic mutations (writes, then deletes), or a
/// branch + merge when one atomic commit is required. Allowing mixing would
/// instead demand an in-query delete view, pending pruning, and per-table
/// two-commit ordering in the hot mutation path — complexity this boundary
/// deliberately avoids.
fn enforce_no_mixed_destructive_constructive(
    ir: &omnigraph_compiler::ir::MutationIR,
) -> Result<()> {
    let mut has_constructive = false;
    let mut has_delete = false;
    for op in &ir.ops {
        match op {
            MutationOpIR::Insert { .. } | MutationOpIR::Update { .. } => {
                has_constructive = true;
            }
            MutationOpIR::Delete { .. } => {
                has_delete = true;
            }
        }
    }
    if has_constructive && has_delete {
        return Err(OmniError::manifest(format!(
            "mutation '{}' on the same query mixes inserts/updates and deletes; \
             split into separate mutations: (1) inserts and updates, then (2) deletes. \
             A query is deliberately constructive or destructive, not both, so its \
             read-your-writes stays unambiguous; run the two on a branch and merge \
             if you need them in one atomic commit.",
            ir.name
        )));
    }
    Ok(())
}

impl Omnigraph {
    pub async fn mutate(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        self.mutate_as(branch, query_source, query_name, params, None)
            .await
    }

    pub async fn mutate_as(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
    ) -> Result<MutationResult> {
        // Engine-layer policy gate (MR-722 fan-out / PR #3). Scope is
        // `Branch(branch)` to match the HTTP-layer convention at
        // `server_change` (branch=Some(branch), target_branch=None). When no
        // PolicyChecker is installed this is a no-op; with policy installed
        // and actor=None this fails hard (forget-the-actor footgun guard).
        self.enforce(
            omnigraph_policy::PolicyAction::Change,
            &omnigraph_policy::ResourceScope::Branch(branch.to_string()),
            actor_id,
        )?;
        self.mutate_with_current_actor(branch, query_source, query_name, params, actor_id)
            .await
    }

    /// End-of-query validation for a constructive mutation: build the change-set
    /// from the accumulated staging and run the unified evaluator (value/enum,
    /// uniqueness incl. cross-version, edge-RI, cardinality) against committed
    /// state. Read-your-writes is inherent — every same-query insert is already
    /// in the change-set. Destructive queries (D2) stage no constructive batches,
    /// so the change-set is empty and this is a no-op (deletes cascade).
    async fn validate_staged_mutation(
        &self,
        staging: &MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<()> {
        // RI/uniqueness read the write's already-validated pinned base (`txn.base`),
        // NOT a fresh `snapshot_for_branch` — per-table resolution must not add a
        // schema-contract validation beyond capture + the pre-effect gate.
        // Cardinality reads a fresh manifest-visible branch snapshot (the #298
        // stale-handle fix) via `CommittedState::write`; it never follows an
        // unpublished raw Lance HEAD or a not-yet-created first-touch ref.
        let committed =
            crate::validate::CommittedState::write(&txn.base, self, txn.branch.as_deref());
        // `to_changeset` carries both constructive batches and the ids the delete
        // ops captured from their own scans (`deleted_ids`), so the evaluator
        // recounts the srcs a delete empties (`@card`) and sees removed rows for
        // RI — the faithful change-set the merge path also builds.
        crate::validate::validate_changeset(&staging.to_changeset(), &committed, &txn.catalog).await
    }

    async fn mutate_with_current_actor(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
    ) -> Result<MutationResult> {
        const MAX_PRE_EFFECT_REPREPARES: usize = 32;

        // Resolve request-scoped values such as now() once so a safe
        // pre-effect retry does not change the logical input.
        let resolved_params = enrich_mutation_params(params)?;
        for attempt in 0..=MAX_PRE_EFFECT_REPREPARES {
            let mut retryable = false;
            match self
                .mutate_one_attempt(
                    branch,
                    query_source,
                    query_name,
                    &resolved_params,
                    actor_id,
                    &mut retryable,
                )
                .await
            {
                Err(err)
                    if retryable
                        && err.is_read_set_changed()
                        && attempt < MAX_PRE_EFFECT_REPREPARES =>
                {
                    tracing::debug!(
                        attempt = attempt + 1,
                        branch,
                        "prepared mutation authority changed before effects; repreparing"
                    );
                    self.refresh().await?;
                }
                result => return result,
            }
        }
        unreachable!("bounded mutation retry loop always returns")
    }

    async fn mutate_one_attempt(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
        retryable: &mut bool,
    ) -> Result<MutationResult> {
        let requested = Self::normalize_branch_name(branch)?;
        // Reject internal `__run__*` / system-prefixed branches at the
        // public write boundary. Direct-publish paths assert this
        // explicitly so a caller can't write to legacy or system
        // staging branches by passing the prefix verbatim.
        if let Some(name) = requested.as_deref() {
            crate::db::ensure_public_branch_ref(name, "mutate")?;
        }
        // Stage A: converge any roll-forward-eligible sidecars, then close the
        // barrier on every unresolved intent for this graph branch. This MUST
        // run before `open_write_txn`: healing may advance the manifest, and a
        // deferred Armed intent remains ownership even when no table HEAD moved.
        self.heal_pending_recovery_sidecars_for_write(&[requested.as_deref()])
            .await?;
        // Capture one branch-wide write authority after the recovery barrier:
        // native branch identity, exact optional graph head, accepted schema
        // identity/catalog, and the base table snapshot. Execution, validation,
        // staging, and publication all use this immutable attempt. `commit_all`
        // revalidates the complete token under the root-shared schema → branch →
        // sorted-table gates before it arms recovery or advances Lance HEAD.
        let txn = self.open_write_txn(requested.as_deref()).await?;
        let resolved_params = params.clone();

        // Per-query staging accumulator. Inserts and updates push batches into
        // `pending`; deletes push predicates into `delete_predicates`. At the
        // boundary, `stage_all` prepares one exact transaction per touched table
        // and `commit_all` records those identities in a durable schema-v3
        // recovery intent before independently advancing the table HEADs. The
        // publisher then makes the complete result graph-visible in one manifest
        // CAS. Branch is threaded explicitly — no coordinator swap.
        let mut staging = MutationStaging::default();

        // Lower + validate up front so the touched-table set is known before
        // execution. A lowering/validation error returns exactly as it did
        // when this happened inside execute_named_mutation.
        let ir = self.lower_named_mutation(&txn.catalog, query_source, query_name)?;
        // Only an insert-only mutation is safe to replay automatically after a
        // pre-effect authority mismatch. Update/Delete keep strict caller-visible
        // `ReadSetChanged`; replaying their stale read-modify-write plan would be
        // a semantic rebase rather than a fresh execution contract.
        *retryable = ir
            .ops
            .iter()
            .all(|op| matches!(op, MutationOpIR::Insert { .. }));

        let exec_result = self
            .execute_named_mutation(
                &ir,
                &resolved_params,
                requested.as_deref(),
                &mut staging,
                &txn,
            )
            .await;

        match exec_result {
            Err(e) => Err(e),
            Ok(total) if staging.is_empty() => Ok(total),
            Ok(total) => {
                self.validate_staged_mutation(&staging, &txn).await?;
                let staged = staging.stage_all(self, requested.as_deref()).await?;
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::MUTATION_POST_STAGE_PRE_EFFECT_GATE,
                )?;
                let lineage_intent = self
                    .new_lineage_intent_for_branch(requested.as_deref(), actor_id)
                    .await?;
                // `_queue_guards` holds the root-shared schema gate, branch
                // effect gate, and sorted table gates acquired by `commit_all`.
                // They remain held through manifest publication, covering the
                // complete same-process sidecar/effect lifetime. They are a
                // local serialization aid; the exact publisher precondition and
                // durable v3 recovery plan remain the correctness authorities.
                let super::staging::CommittedMutation {
                    updates,
                    expected_versions,
                    sidecar_handle,
                    guards: _queue_guards,
                } = staged
                    .commit_all(
                        self,
                        requested.as_deref(),
                        crate::db::manifest::SidecarKind::Mutation,
                        actor_id,
                        &txn,
                        &lineage_intent,
                    )
                    .await?;
                // Failpoint for the confirmed-effects → publisher boundary:
                // table HEADs have advanced but graph visibility has not. The
                // v3 sidecar already contains exact transaction identities,
                // immutable manifest delta, and fixed lineage/rollback outcomes.
                // Any failure from here is `RecoveryRequired`; synchronous heal
                // or a read-write open converges the recorded outcome. See
                // `tests/failpoints.rs::recovery_rolls_forward_after_finalize_publisher_failure`.
                crate::failpoints::maybe_fail(
                    crate::failpoints::names::MUTATION_POST_FINALIZE_PRE_PUBLISHER,
                )?;
                let publish_result = self
                    .commit_updates_on_branch_with_expected(
                        requested.as_deref(),
                        &updates,
                        &expected_versions,
                        actor_id,
                        &txn,
                        lineage_intent,
                    )
                    .await;
                if let Err(err) = publish_result {
                    // A sidecar exists iff at least one table effect was
                    // committed. Lineage-only / zero-row mutations have no
                    // physical residual to recover, so preserve their original
                    // publish error (notably ReadSetChanged) and let the normal
                    // retry/409 path handle it.
                    return match sidecar_handle.as_ref() {
                        Some(handle) => Err(OmniError::recovery_required(
                            handle.operation_id.clone(),
                            err.to_string(),
                        )),
                        None => Err(err),
                    };
                }
                if let Some(handle) = sidecar_handle {
                    // Best-effort cleanup: the manifest publish already
                    // succeeded, so the user's mutation is durable. A failed
                    // delete leaves a fixed, idempotent v3 outcome for the next
                    // synchronous heal or read-write open to audit and remove.
                    // Failing the user here would report an error for a write
                    // that already landed.
                    if let Err(err) =
                        crate::db::manifest::delete_sidecar(&handle, self.storage_adapter()).await
                    {
                        tracing::warn!(
                            error = %err,
                            operation_id = handle.operation_id.as_str(),
                            "recovery sidecar cleanup failed; the next open's recovery sweep will resolve it"
                        );
                    }
                }
                Ok(total)
            }
        }
    }

    /// Lower + validate a named mutation query into its IR.
    ///
    /// Hoisted out of [`Self::execute_named_mutation`] so the caller can
    /// inspect the IR before execution — specifically to compute the
    /// touched-table set (see [`Self::touched_table_keys`]) for up-front
    /// write-queue acquisition. Performs the same find → typecheck → lower
    /// → D₂ checks that execution previously did inline, so error behavior
    /// is unchanged.
    fn lower_named_mutation(
        &self,
        catalog: &omnigraph_compiler::catalog::Catalog,
        query_source: &str,
        query_name: &str,
    ) -> Result<omnigraph_compiler::ir::MutationIR> {
        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;

        let checked = typecheck_query_decl(catalog, &query_decl)?;
        match checked {
            CheckedQuery::Mutation(_) => {}
            CheckedQuery::Read(_) => {
                return Err(OmniError::manifest(
                    "mutation execution called on a read query; use query instead".to_string(),
                ));
            }
        }

        let ir = lower_mutation_query(&query_decl)?;
        // D₂: reject mixed insert/update + delete before any I/O.
        enforce_no_mixed_destructive_constructive(&ir)?;
        Ok(ir)
    }

    async fn execute_named_mutation(
        &self,
        ir: &omnigraph_compiler::ir::MutationIR,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let mut total = MutationResult::default();
        for op in &ir.ops {
            let result = match op {
                MutationOpIR::Insert {
                    type_name,
                    assignments,
                } => {
                    self.execute_insert(type_name, assignments, params, branch, staging, txn)
                        .await?
                }
                MutationOpIR::Update {
                    type_name,
                    assignments,
                    predicate,
                } => {
                    // Field-level-update eligibility: partial staging only when this
                    // update is the table's ONLY op in the query (the count
                    // includes this op itself, so == 1 means "no other op
                    // touches the type"). Every other combination falls back
                    // to whole-row staging: partial and full batches cannot
                    // share one uniform-schema merge source (a present
                    // column's null cell means "set NULL", so widening a
                    // partial batch would null-overwrite), a second same-table
                    // op's read-your-writes scan must see full rows, and one
                    // table commits at most one version per query
                    // (invariant 4). Op lists are tiny; the inline count
                    // replaces a per-query census map.
                    let partial_ok = ir
                        .ops
                        .iter()
                        .filter(|op| {
                            let t = match op {
                                MutationOpIR::Insert { type_name, .. }
                                | MutationOpIR::Update { type_name, .. }
                                | MutationOpIR::Delete { type_name, .. } => type_name,
                            };
                            t == type_name
                        })
                        .count()
                        == 1;
                    self.execute_update(
                        type_name,
                        assignments,
                        predicate,
                        params,
                        branch,
                        staging,
                        txn,
                        partial_ok,
                    )
                    .await?
                }
                MutationOpIR::Delete {
                    type_name,
                    predicate,
                } => {
                    self.execute_delete(type_name, predicate, params, branch, staging, txn)
                        .await?
                }
            };
            total.affected_nodes += result.affected_nodes;
            total.affected_edges += result.affected_edges;
        }
        Ok(total)
    }

    async fn execute_insert(
        &self,
        type_name: &str,
        assignments: &[IRAssignment],
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }

        let catalog = &txn.catalog;
        let is_node = catalog.node_types.contains_key(type_name);
        let is_edge = catalog.edge_types.contains_key(type_name);

        if is_node {
            let node_type = &catalog.node_types[type_name];
            let schema = node_type.arrow_schema.clone();
            let blob_props = node_type.blob_properties.clone();
            let id = if let Some(key_prop) = node_type.key_property() {
                match resolved.get(key_prop) {
                    Some(Literal::String(s)) => s.clone(),
                    Some(other) => literal_to_sql(other).trim_matches('\'').to_string(),
                    None => {
                        return Err(OmniError::manifest(format!(
                            "insert missing @key property '{}'",
                            key_prop
                        )));
                    }
                }
            } else {
                ulid::Ulid::new().to_string()
            };

            let batch = build_insert_batch(&schema, &id, &resolved, &blob_props)?;
            // Validation (value/enum/unique) runs end-of-query via the evaluator.
            let has_key = node_type.key_property().is_some();
            let table_key = format!("node:{}", type_name);
            // Capture pre-write metadata on first touch (no Lance write).
            let insert_kind = if has_key {
                crate::db::MutationOpKind::Merge
            } else {
                crate::db::MutationOpKind::Insert
            };
            // Node inserts are non-strict (Insert/Merge), so with a `WriteTxn`
            // this opens NOTHING (collapse #1) — the handle is discarded anyway;
            // only `ensure_path`'s captured version (read inside
            // `open_table_for_mutation`) is used downstream.
            let (_ds, _full_path, _table_branch) =
                open_table_for_mutation(self, staging, branch, &table_key, insert_kind, Some(txn))
                    .await?;
            // Accumulate. @key inserts go into the Merge stream (so a
            // later update on the same id coalesces correctly); no-key
            // inserts go into the Append stream.
            let mode = if has_key {
                PendingMode::Merge
            } else {
                PendingMode::Append
            };
            staging.append_batch(&table_key, schema, mode, batch)?;

            Ok(MutationResult {
                affected_nodes: 1,
                affected_edges: 0,
            })
        } else if is_edge {
            let edge_type = &catalog.edge_types[type_name];
            let schema = edge_type.arrow_schema.clone();
            let blob_props = edge_type.blob_properties.clone();
            let id = ulid::Ulid::new().to_string();

            let batch = build_insert_batch(&schema, &id, &resolved, &blob_props)?;
            // Validation (edge-RI, enum, unique, @card against the live
            // manifest-visible branch snapshot) runs
            // end-of-query via the evaluator.
            let table_key = format!("edge:{}", type_name);
            // Capture pre-write metadata on first touch (ensure_path). Edge
            // inserts are non-strict, so with a `WriteTxn` this opens NOTHING
            // (collapse #1) and the handle is discarded — validation, including
            // `@card` against the live committed branch snapshot, runs
            // end-of-query via the evaluator.
            let (_handle, _full_path, _table_branch) = open_table_for_mutation(
                self,
                staging,
                branch,
                &table_key,
                crate::db::MutationOpKind::Insert,
                Some(txn),
            )
            .await?;
            // Accumulate the new edge row. Edge IDs are ULID-generated so
            // Append mode is correct (no key-based dedup needed).
            staging.append_batch(&table_key, schema, PendingMode::Append, batch)?;

            self.invalidate_graph_index().await;

            Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 1,
            })
        } else {
            Err(OmniError::manifest(format!("unknown type '{}'", type_name)))
        }
    }

    async fn execute_update(
        &self,
        type_name: &str,
        assignments: &[IRAssignment],
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
        partial_ok: bool,
    ) -> Result<MutationResult> {
        let catalog = &txn.catalog;
        // Defense in depth: ensure this is a node type
        if !catalog.node_types.contains_key(type_name) {
            return Err(OmniError::manifest(format!(
                "update is only supported for node types, not '{}'",
                type_name
            )));
        }

        // Reject updates to @key properties — identity is immutable. Checks
        // EVERY key column (composite keys included), not just the first.
        if let Some(key_cols) = &catalog.node_types[type_name].key {
            if let Some(a) = assignments.iter().find(|a| key_cols.contains(&a.property)) {
                return Err(OmniError::manifest(format!(
                    "cannot update @key property '{}' — delete and re-insert instead",
                    a.property
                )));
            }
        }

        let pred_sql = predicate_to_sql(predicate, params, false)?;
        let schema = catalog.node_types[type_name].arrow_schema.clone();
        let blob_props = catalog.node_types[type_name].blob_properties.clone();

        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }

        // Field-level update: when this update is the table's only op in the query,
        // stage a PARTIAL source — (id + assigned + constraint-completion)
        // columns — instead of whole rows. `partial_plan` is `None` on the
        // whole-row fallback path. Takes RESOLVED assignments so no-op blob
        // assignments (non-String values) are excluded from the plan.
        //
        // Blob-bearing tables always take the whole-row path: Lance's
        // partial-source column patch trips the blob writer on a blob-bearing
        // fragment even when no blob column is in the source ("Blob struct
        // missing `data` field", lance blob.rs) — pinned by
        // `end_to_end::update_with_blob_mid_schema_does_not_panic`. Revisit
        // when Lance's blob merge accepts partial sources.
        let partial_ok = partial_ok && blob_props.is_empty();
        let partial_plan = if partial_ok {
            Some(partial_update_plan(
                &catalog.node_types[type_name],
                &schema,
                &resolved,
            ))
        } else {
            None
        };

        let table_key = format!("node:{}", type_name);
        let (handle, _full_path, _table_branch) = open_table_for_mutation(
            self,
            staging,
            branch,
            &table_key,
            crate::db::MutationOpKind::Update,
            Some(txn),
        )
        .await?;
        // Update is a STRICT op, so collapse #1 never skips its open — the
        // handle is always `Some` (and it's needed for the committed scan below).
        let ds = handle.expect("strict Update op always opens its dataset");

        // Scan committed via Lance + apply the same predicate to pending
        // batches via DataFusion `MemTable` (read-your-writes for prior ops in
        // this query). The pending side may include rows from earlier
        // `insert` / `update` ops on the same table.
        let pending_batches = staging.pending_batches(&table_key);
        let pending_schema = staging.pending_schema(&table_key);
        // Use merge semantics on the union: a committed row whose `id`
        // also appears in pending has been logically updated by an
        // earlier op in this query and is shadowed from the scan,
        // otherwise the predicate runs against stale committed values
        // and a chained `update where <pred>` can match a row whose
        // pending value no longer satisfies <pred>.
        // A blob-v2 scan normally yields physical descriptor structs, which
        // cannot be fed back to the full-schema merge writer. Select matched
        // committed row ids without projecting blobs, then take and rebuild
        // only those payloads into the logical blob schema before unioning
        // pending rows. This keeps correctness independent of whether an id
        // index happens to steer Lance onto its legacy partial-column plan.
        let batches = if let Some(plan) = partial_plan.as_ref() {
            // Field-level update: scan only (id + completion) — assigned
            // columns' old values are never needed (assignments are literal
            // values) and the WHERE predicate evaluates by pushdown without
            // being projected. Blobs are never in this projection (the plan
            // excludes them), so the blob-materialization arm below is
            // unnecessary here; pending is empty by the eligibility rule.
            let scan_cols: Vec<&str> = plan.scan_cols.iter().map(|c| c.as_str()).collect();
            self.storage()
                .scan_with_pending(
                    &ds,
                    pending_batches,
                    pending_schema,
                    Some(scan_cols.as_slice()),
                    Some(&pred_sql),
                    Some("id"),
                )
                .await?
        } else if blob_props.is_empty() {
            self.storage()
                .scan_with_pending(
                    &ds,
                    pending_batches,
                    pending_schema,
                    None,
                    Some(&pred_sql),
                    Some("id"),
                )
                .await?
        } else {
            self.storage()
                .scan_with_pending_materialized_blobs(
                    &ds,
                    pending_batches,
                    pending_schema,
                    Some(&pred_sql),
                    Some("id"),
                )
                .await?
        };

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        // Concat the matched batches (committed + pending) into one. The
        // helper binds both sides to the catalog's full logical schema (partial
        // path: to the plan's SCAN schema — id + completion). Any divergence
        // here is an internal scan/staging contract violation.
        let (concat_schema, output_schema) = match partial_plan.as_ref() {
            Some(plan) => (plan.scan_schema.clone(), plan.output_schema.clone()),
            None => (schema.clone(), schema.clone()),
        };
        let matched = concat_match_batches_to_schema(&concat_schema, batches)?;

        let affected_count = matched.num_rows();

        let updated = apply_assignments(&output_schema, &matched, &resolved, &blob_props)?;
        // Validation (value/enum/unique) runs end-of-query via the evaluator.

        // Accumulate the updated batch. Whole-row: the Merge-mode pending
        // stream (may coalesce with prior same-table ops; finalize dedupes by
        // id, last wins). Partial: a dedicated partial-update entry
        // that stages as a matched-only merge.
        let updated_schema = updated.schema();
        if let Some(plan) = partial_plan.as_ref() {
            staging.append_partial_update_batch(
                &table_key,
                updated_schema,
                updated,
                plan.validation_only.clone(),
            )?;
        } else {
            staging.append_batch(&table_key, updated_schema, PendingMode::Merge, updated)?;
        }

        Ok(MutationResult {
            affected_nodes: affected_count,
            affected_edges: 0,
        })
    }

    async fn execute_delete(
        &self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let is_node = txn.catalog.node_types.contains_key(type_name);
        if is_node {
            self.execute_delete_node(type_name, predicate, params, branch, staging, txn)
                .await
        } else {
            self.execute_delete_edge(type_name, predicate, params, branch, staging, txn)
                .await
        }
    }

    async fn execute_delete_node(
        &self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, false)?;

        let table_key = format!("node:{}", type_name);
        let (handle, _full_path, _table_branch) = open_table_for_mutation(
            self,
            staging,
            branch,
            &table_key,
            crate::db::MutationOpKind::Delete,
            Some(txn),
        )
        .await?;
        // Delete is a STRICT op, so collapse #1 never skips its open.
        let ds = handle.expect("strict Delete op always opens its dataset");

        // Scan matching IDs for cascade. Per D₂ this never overlaps with
        // staged inserts (mixed insert/delete in one query is rejected at
        // parse time), so we scan committed only. Exclude IDs a prior delete
        // statement on this table already scheduled (deletes stage, so the
        // committed snapshot is unchanged across statements): without this,
        // overlapping predicates would double-count `affected_nodes` AND
        // re-cascade already-deleted nodes' edges. The combined staged delete
        // still removes the union, so we record the original `pred_sql` below.
        let scan_filter =
            dedup_delete_filter(&pred_sql, staging.recorded_delete_predicates(&table_key));
        let batches = self
            .storage()
            .scan(&ds, Some(&["id"]), Some(&scan_filter), None)
            .await?;

        let deleted_ids: Vec<String> = ids_from_batches(&batches);

        if deleted_ids.is_empty() {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        let affected_nodes = deleted_ids.len();

        // Record the node delete as a staged predicate. D₂ keeps inserts and
        // deletes from coexisting in one query, so this table carries no
        // pending write batches; `stage_all` turns the predicate into one
        // `stage_delete` (a deletion-vector transaction) that advances Lance
        // HEAD only at the unified end-of-query commit — no inline residual.
        // `open_table_for_mutation` above already captured the table's
        // path/version/op-kind via `ensure_path`.
        crate::failpoints::maybe_fail(
            crate::failpoints::names::MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE,
        )?;
        staging.record_deleted_ids(&table_key, &deleted_ids);
        staging.record_delete(&table_key, pred_sql.clone());

        let mut affected_edges = 0usize;
        let escaped: Vec<String> = deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let id_list = escaped.join(", ");

        let edge_info: Vec<(String, String, String)> = txn
            .catalog
            .edge_types
            .iter()
            .map(|(name, et)| (name.clone(), et.from_type.clone(), et.to_type.clone()))
            .collect();

        for (edge_name, from_type, to_type) in &edge_info {
            let mut cascade_filters = Vec::new();
            if from_type == type_name {
                cascade_filters.push(format!("src IN ({})", id_list));
            }
            if to_type == type_name {
                cascade_filters.push(format!("dst IN ({})", id_list));
            }
            if cascade_filters.is_empty() {
                continue;
            }

            let edge_table_key = format!("edge:{}", edge_name);
            let cascade_filter = cascade_filters.join(" OR ");
            let (edge_handle, _edge_full_path, _edge_table_branch) = open_table_for_mutation(
                self,
                staging,
                branch,
                &edge_table_key,
                crate::db::MutationOpKind::Delete,
                Some(txn),
            )
            .await?;
            // Delete is a STRICT op, so collapse #1 never skips its open.
            let edge_ds = edge_handle.expect("strict Delete op always opens its dataset");

            // `affected_edges` was the post-inline-commit `deleted_rows`; with
            // staged deletes the rows aren't removed until end-of-query, so
            // count the matching committed edges now. Exact under D₂ (no staged
            // inserts can add matches mid-query), and bounded by the cascade
            // working set. Exclude edges a prior delete statement (a prior
            // cascade, or an explicit edge delete) on this table already
            // scheduled, so an edge incident to two deleted nodes — or matched
            // by both a cascade and an explicit `delete <Edge>` — is counted
            // once. Record the ORIGINAL cascade filter (the combined staged
            // delete removes the union); skip only when nothing NEW matches.
            let count_filter = dedup_delete_filter(
                &cascade_filter,
                staging.recorded_delete_predicates(&edge_table_key),
            );
            // Scan (not count) the cascade-removed edge ids so validation
            // recounts the OTHER endpoint's @card after the cascade; `len()` is
            // the affected count.
            let matched_ids = ids_from_batches(
                &self
                    .storage()
                    .scan(&edge_ds, Some(&["id"]), Some(&count_filter), None)
                    .await?,
            );
            let matched = matched_ids.len();
            affected_edges += matched;

            if matched > 0 {
                staging.record_deleted_ids(&edge_table_key, &matched_ids);
                staging.record_delete(&edge_table_key, cascade_filter);
            }
        }

        if affected_edges > 0 {
            self.invalidate_graph_index().await;
        }

        Ok(MutationResult {
            affected_nodes,
            affected_edges,
        })
    }

    async fn execute_delete_edge(
        &self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, true)?;

        let table_key = format!("edge:{}", type_name);
        let (handle, _full_path, _table_branch) = open_table_for_mutation(
            self,
            staging,
            branch,
            &table_key,
            crate::db::MutationOpKind::Delete,
            Some(txn),
        )
        .await?;
        // Delete is a STRICT op, so collapse #1 never skips its open.
        let ds = handle.expect("strict Delete op always opens its dataset");

        // Count matching committed edges now (the staged delete won't remove
        // them until end-of-query). Exact under D₂; exclude edges a prior delete
        // statement on this table (an earlier cascade or edge delete) already
        // scheduled, so overlapping statements don't double-count. Record the
        // ORIGINAL predicate below (the combined staged delete removes the
        // union); only record when something NEW matches.
        let count_filter =
            dedup_delete_filter(&pred_sql, staging.recorded_delete_predicates(&table_key));
        // Scan the matched edge ids (not just count): the ids feed validation so
        // a delete emptying a src below @card min is rejected; `len()` is the
        // affected count. One scan replaces the former count-here + resolve-at-
        // validation re-scan.
        let deleted_ids = ids_from_batches(
            &self
                .storage()
                .scan(&ds, Some(&["id"]), Some(&count_filter), None)
                .await?,
        );
        let affected = deleted_ids.len();

        if affected > 0 {
            staging.record_deleted_ids(&table_key, &deleted_ids);
            staging.record_delete(&table_key, pred_sql.clone());
            self.invalidate_graph_index().await;
        }

        Ok(MutationResult {
            affected_nodes: 0,
            affected_edges: affected,
        })
    }
}

/// Extract the `id` column (projection index 0) from scanned batches. Used by
/// the delete paths to capture the rows they remove, so validation recounts a
/// src a delete empties without re-resolving the predicate.
fn ids_from_batches(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..ids.len())
                .map(|i| ids.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Concat the matched batches from `scan_with_pending` into a single batch.
/// `scan_with_pending` returns committed-side and pending-side batches in
/// order; both should share a schema if pending was produced through
/// `apply_assignments` with full-schema scan input. If schemas drift,
/// surface the internal contract failure at the mutation boundary.
fn concat_match_batches_to_schema(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    if batches.len() == 1 {
        let batch = batches.into_iter().next().unwrap();
        return RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
            .map_err(|e| OmniError::Lance(e.to_string()));
    }
    arrow_select::concat::concat_batches(schema, &batches).map_err(|e| {
        OmniError::Lance(format!(
            "mutation scan returned batches that violate the full logical schema \
             across the committed/pending boundary ({})",
            e
        ))
    })
}

fn enrich_mutation_params(params: &ParamMap) -> Result<ParamMap> {
    let mut resolved = params.clone();
    if !resolved.contains_key(NOW_PARAM_NAME) {
        let now = OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|e| OmniError::manifest(format!("failed to format now(): {}", e)))?;
        resolved.insert(NOW_PARAM_NAME.to_string(), Literal::DateTime(now));
    }
    Ok(resolved)
}

#[cfg(test)]
mod predicate_sql_tests {
    use super::*;

    // #283: a camelCase column in a mutation predicate must be emitted
    // UNQUOTED and case-preserved. The committed-scan consumer, Lance's
    // `Scanner::filter(&str)`, preserves an unquoted identifier's case but
    // treats a double-quoted `"col"` as a string literal (which silently
    // matches zero rows), so the predicate string must not quote the column.
    // The pending MemTable path stays case-preserving by disabling DataFusion
    // identifier normalization on its context, not by quoting here.
    #[test]
    fn predicate_to_sql_preserves_camelcase_column_unquoted() {
        let predicate = IRMutationPredicate {
            property: "repoName".to_string(),
            op: CompOp::Eq,
            value: IRExpr::Literal(Literal::String("acme".into())),
        };
        let sql = predicate_to_sql(&predicate, &ParamMap::new(), false).unwrap();
        assert_eq!(
            sql, "repoName = 'acme'",
            "column must be unquoted and case-preserved, got {sql}"
        );
    }
}
