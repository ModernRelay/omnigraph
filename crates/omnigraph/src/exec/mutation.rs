use super::*;

use crate::db::manifest::HistoryReleaseBytes;
use crate::engine::{
    evaluate_constant, fill_declared_params, id_in_list_expr, ir_expr_to_df_expr, validate_params,
};
use crate::instrumentation::record_mutation_table_open;
use crate::loader::append_blob_value;
use crate::seams::{decide_seam, fail};
use crate::session::Session;
use crate::storage_layer::{DeletedIdBudget, PendingScanBudget, SnapshotHandle, WriteBudget};
use datafusion::prelude::Expr;
use futures::TryStreamExt;

// ─── Mutation helpers ────────────────────────────────────────────────────────

/// The assignments evaluated per attempt over the invocation's fixed parameter
/// map (`now()` bound before the retry loop, so every attempt gets one value); a
/// null result on a non-nullable property, `Blob` included, is refused before any batch.
fn resolve_assignments(
    type_name: &str,
    schema: &Schema,
    assignments: &[IRAssignment],
    params: &ParamMap,
) -> Result<HashMap<String, Literal>> {
    let mut resolved = HashMap::with_capacity(assignments.len());
    for assignment in assignments {
        let value = evaluate_constant(&assignment.value, params)?;
        let non_nullable = schema
            .field_with_name(&assignment.property)
            .is_ok_and(|field| !field.is_nullable());
        if non_nullable && matches!(value, Literal::Null) {
            return Err(OmniError::manifest(format!(
                "cannot assign null to non-nullable property '{}' of {type_name}",
                assignment.property
            )));
        }
        resolved.insert(assignment.property.clone(), value);
    }
    Ok(resolved)
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
            let value = i32::try_from(*n).map_err(|_| {
                OmniError::manifest(format!("integer value {n} exceeds Int32 range"))
            })?;
            Arc::new(Int32Array::from(vec![value; num_rows]))
        }
        (Literal::Integer(n), DataType::Int64) => Arc::new(Int64Array::from(vec![*n; num_rows])),
        (Literal::Integer(n), DataType::UInt32) => {
            let value = u32::try_from(*n).map_err(|_| {
                OmniError::manifest(format!("integer value {n} exceeds UInt32 range"))
            })?;
            Arc::new(UInt32Array::from(vec![value; num_rows]))
        }
        (Literal::Integer(n), DataType::UInt64) => {
            let value = u64::try_from(*n).map_err(|_| {
                OmniError::manifest(format!("integer value {n} exceeds UInt64 range"))
            })?;
            Arc::new(UInt64Array::from(vec![value; num_rows]))
        }
        (Literal::Float(f), DataType::Float32) => {
            Arc::new(Float32Array::from(vec![
                checked_f32(*f, "float value")?;
                num_rows
            ]))
        }
        (Literal::Float(f), DataType::Float64) => {
            Arc::new(Float64Array::from(vec![
                checked_f64(*f, "float value")?;
                num_rows
            ]))
        }
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
                        Literal::Integer(value) => builder
                            .values()
                            .append_value(checked_f32(*value as f64, "vector element")?),
                        Literal::Float(value) => builder
                            .values()
                            .append_value(checked_f32(*value, "vector element")?),
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

fn checked_f32(value: f64, context: &str) -> Result<f32> {
    checked_f64(value, context)?;
    // Use the result of IEEE round-to-nearest as the range authority. This
    // accepts decimal round-trips at the finite boundary while still rejecting
    // every value whose Float32 result is infinite.
    let narrowed = value as f32;
    if !narrowed.is_finite() {
        return Err(OmniError::manifest(format!(
            "{context} {value} exceeds Float32 range"
        )));
    }
    Ok(narrowed)
}

fn checked_f64(value: f64, context: &str) -> Result<f64> {
    if !value.is_finite() {
        return Err(OmniError::manifest(format!(
            "{context} {value} must be finite"
        )));
    }
    Ok(value)
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
                        Literal::Integer(value) => builder
                            .values()
                            .append_value(checked_f32(*value as f64, "list value")?),
                        Literal::Float(value) => builder
                            .values()
                            .append_value(checked_f32(*value, "list value")?),
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
                        Literal::Float(value) => builder
                            .values()
                            .append_value(checked_f64(*value, "list value")?),
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
fn build_blob_array_from_value(value: &str, write_budget: WriteBudget) -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(1);
    append_blob_value(&mut builder, value, write_budget)?;
    builder.finish().map_err(OmniError::lance_internal)
}

/// Build a null blob array with `num_rows` elements.
fn build_null_blob_array(num_rows: usize) -> Result<ArrayRef> {
    let mut builder = BlobArrayBuilder::new(num_rows);
    for _ in 0..num_rows {
        builder.push_null().map_err(OmniError::lance_internal)?;
    }
    builder.finish().map_err(OmniError::lance_internal)
}

fn preflight_blob_assignments(
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
    rows: usize,
    write_budget: WriteBudget,
) -> Result<()> {
    let mut bytes = 0_u64;
    for property in blob_properties {
        if let Some(Literal::String(value)) = assignments.get(property)
            && let Some(encoded) = value.strip_prefix("base64:")
        {
            bytes = bytes
                .checked_add(crate::loader::decoded_blob_bytes(encoded)?)
                .ok_or_else(|| {
                    OmniError::manifest_internal("Blob assignment byte count overflow")
                })?;
        }
    }
    let bytes =
        bytes
            .checked_mul(u64::try_from(rows).map_err(|_| {
                OmniError::manifest_internal("Blob assignment row count exceeds u64")
            })?)
            .ok_or_else(|| OmniError::manifest_internal("Blob assignment byte count overflow"))?;
    write_budget.check("decoded blob input bytes per operation", bytes)
}

/// Build a single-row RecordBatch from resolved assignments.
fn build_insert_batch(
    schema: &SchemaRef,
    id: &str,
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
    system_columns: SystemColumns,
    write_budget: WriteBudget,
) -> Result<RecordBatch> {
    preflight_blob_assignments(assignments, blob_properties, 1, write_budget)?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        if field.name() == system_columns.id {
            columns.push(Arc::new(StringArray::from(vec![id])));
        } else if blob_properties.contains(field.name()) {
            if let Some(Literal::String(uri)) = assignments.get(field.name()) {
                columns.push(build_blob_array_from_value(uri, write_budget)?);
            } else if field.is_nullable() {
                columns.push(build_null_blob_array(1)?);
            } else {
                return Err(OmniError::manifest(format!(
                    "missing required blob property '{}'",
                    field.name()
                )));
            }
        } else if field.name() == system_columns.src {
            let lit = assignments.get("from").ok_or_else(|| {
                OmniError::manifest("missing required edge endpoint 'from'".to_string())
            })?;
            columns.push(literal_to_typed_array(lit, field.data_type(), 1)?);
        } else if field.name() == system_columns.dst {
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

    RecordBatch::try_new(schema.clone(), columns).map_err(OmniError::arrow_internal)
}

/// The mutation `where` as the typed DataFusion expression Lance evaluates,
/// through the lowering a read scan uses; the compiler already put every
/// property leaf on its physical column, and `schema` types the literals.
fn mutation_predicate_expr(predicate: &IRExpr, params: &ParamMap, schema: &Schema) -> Result<Expr> {
    if let Some(name) = first_unbound_param(predicate, params) {
        return Err(OmniError::manifest(format!(
            "parameter '{name}' not provided"
        )));
    }
    ir_expr_to_df_expr(predicate, params, Some(schema)).ok_or_else(|| {
        OmniError::manifest(format!(
            "unsupported expression in mutation predicate: {predicate}"
        ))
    })
}

/// The first parameter of a mutation predicate, in written order, that
/// `params` does not bind: the lowering answers `None` for it, indistinguishable
/// from an inexpressible shape, so the name is checked before lowering.
fn first_unbound_param<'a>(expr: &'a IRExpr, params: &ParamMap) -> Option<&'a str> {
    match expr {
        IRExpr::Param(name, _) => (!params.contains_key(name)).then_some(name.as_str()),
        IRExpr::Binary { left, right, .. } => {
            first_unbound_param(left, params).or_else(|| first_unbound_param(right, params))
        }
        IRExpr::Not(inner, _) | IRExpr::Cast { expr: inner, .. } => {
            first_unbound_param(inner, params)
        }
        IRExpr::IsNull { expr, .. } => first_unbound_param(expr, params),
        IRExpr::PropAccess { .. }
        | IRExpr::Nearest { .. }
        | IRExpr::Search { .. }
        | IRExpr::Fuzzy { .. }
        | IRExpr::MatchText { .. }
        | IRExpr::Bm25 { .. }
        | IRExpr::Rrf { .. }
        | IRExpr::Variable(_, _)
        | IRExpr::Literal(_, _)
        | IRExpr::Aggregate { .. }
        | IRExpr::AliasRef(_, _) => None,
    }
}

/// Rebuild a matched batch, which lacks the Blobs it assigns, on `full_schema`
/// with the assigned values, so every update batch shares one pending merge
/// stream with inserts and earlier updates.
fn apply_assignments(
    full_schema: &SchemaRef,
    batch: &RecordBatch,
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
    write_budget: WriteBudget,
) -> Result<RecordBatch> {
    preflight_blob_assignments(assignments, blob_properties, batch.num_rows(), write_budget)?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(full_schema.fields().len());
    for field in full_schema.fields().iter() {
        if blob_properties.contains(field.name()) {
            let column = match assignments.get(field.name()) {
                Some(Literal::String(uri)) => {
                    let mut builder = BlobArrayBuilder::new(batch.num_rows());
                    for _ in 0..batch.num_rows() {
                        append_blob_value(&mut builder, uri, write_budget)?;
                    }
                    builder.finish().map_err(OmniError::lance_internal)?
                }
                Some(Literal::Null) => build_null_blob_array(batch.num_rows())?,
                Some(other) => {
                    return Err(OmniError::manifest_internal(format!(
                        "Blob property '{}' assigned non-string constant {other:?}",
                        field.name()
                    )));
                }
                // Unassigned: the materializing scan must have normalized the
                // committed value (or pending value) to the logical blob
                // schema, so copying it preserves both bytes and full-schema
                // merge compatibility.
                None => batch
                    .column_by_name(field.name())
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "blob column '{}' not found in full-schema mutation scan",
                            field.name()
                        ))
                    })?
                    .clone(),
            };
            columns.push(column);
        } else if let Some(lit) = assignments.get(field.name()) {
            columns.push(literal_to_typed_array(
                lit,
                field.data_type(),
                batch.num_rows(),
            )?);
        } else {
            let col = batch.column_by_name(field.name()).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "column '{}' not found in scan result",
                    field.name()
                ))
            })?;
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(full_schema.clone(), columns).map_err(OmniError::arrow_internal)
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
async fn open_table_for_mutation(
    db: &Omnigraph,
    staging: &mut MutationStaging,
    branch: Option<&str>,
    table_key: &str,
    op_kind: crate::db::MutationOpKind,
    txn: Option<&crate::db::WriteTxn>,
) -> Result<(Option<SnapshotHandle>, String, Option<String>)> {
    record_mutation_table_open();
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
        opened.identity,
        opened.full_path.clone(),
        opened.table_branch.clone(),
        opened.pinned_native_ref.clone(),
        opened.entry.clone(),
        opened.expected_version,
        op_kind,
    )?;
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
fn dedup_delete_filter(base: &Expr, prior: &[Expr]) -> Expr {
    match prior.iter().cloned().reduce(Expr::or) {
        None => base.clone(),
        Some(excluded) => base.clone().and(excluded.is_not_true()),
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

decide_seam! {
    pub static MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE = ("mutation.delete_node_pre_primary_delete", Mutation, [Fail]);
}

decide_seam! {
    pub static MUTATION_POST_FINALIZE_PRE_PUBLISHER = ("mutation.post_finalize_pre_publisher", Mutation, [Fail]);
}

decide_seam! {
    /// Deterministic OCC rendezvous after a mutation has validated and staged
    /// its complete attempt, but before the RFC-022 branch effect gate is
    /// acquired and the write authority token is revalidated. Tests park the
    /// first writer here, commit a conflicting second writer, then prove the
    /// first attempt is discarded and validation is rerun from a fresh token.
    pub static MUTATION_POST_STAGE_PRE_EFFECT_GATE = ("mutation.post_stage_pre_effect_gate", Mutation, [Fail]);
}

decide_seam! {
    /// After a conditional mutation has executed to a zero-effect result, but
    /// before it acquires the branch gate and revalidates the caller's graph
    /// head. This pins the linearization point for successful no-op CAS calls.
    pub static MUTATION_POST_NO_EFFECT_PRE_GATE = ("mutation.post_no_effect_pre_gate", Mutation, [Fail]);
}

impl Session {
    pub async fn mutate(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        Ok(self
            .mutate_as_with_expected_head_receipt(
                branch,
                query_source,
                query_name,
                params,
                None,
                None,
            )
            .await?
            .result)
    }

    pub async fn mutate_with_receipt(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<crate::MutationReceipt> {
        self.mutate_as_with_expected_head_receipt(
            branch,
            query_source,
            query_name,
            params,
            None,
            None,
        )
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
        Ok(self
            .mutate_as_with_expected_head_receipt(
                branch,
                query_source,
                query_name,
                params,
                actor_id,
                None,
            )
            .await?
            .result)
    }

    pub async fn mutate_as_with_receipt(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
    ) -> Result<crate::MutationReceipt> {
        self.mutate_as_with_expected_head_receipt(
            branch,
            query_source,
            query_name,
            params,
            actor_id,
            None,
        )
        .await
    }

    /// [`Self::mutate_as`] with a caller-supplied compare-and-swap
    /// precondition on the branch head (the HTTP
    /// `Omnigraph-If-Graph-Commit` surface).
    ///
    /// When `expected_head` is `Some`, the mutation runs only if the branch's
    /// effective head commit id still equals it — i.e. nothing has committed
    /// to the branch since the caller read that id. The comparison uses the
    /// same pinned view the write executes against and is re-evaluated under
    /// the process-wide branch gate immediately before effects (or before a
    /// no-op is acknowledged). Within OmniGraph's supported
    /// single-writer-process topology, success therefore proves that no other
    /// write interleaved after the caller's read.
    ///
    /// # Errors
    ///
    /// Returns [`OmniError::PreconditionFailed`] — before any effect, never
    /// internally retried — when the local authoritative check observes a
    /// mismatch. This is not a distributed lease: an unsupported foreign
    /// writer that races after that check is rejected by the exact publisher,
    /// which leaves the graph unchanged. All other error behavior matches
    /// [`Self::mutate_as`].
    pub async fn mutate_as_with_expected_head(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
        expected_head: Option<&str>,
    ) -> Result<MutationResult> {
        Ok(self
            .mutate_as_with_expected_head_receipt(
                branch,
                query_source,
                query_name,
                params,
                actor_id,
                expected_head,
            )
            .await?
            .result)
    }

    /// Receipt-returning form of [`Self::mutate_as_with_expected_head`].
    ///
    /// The optional commit is the exact [`crate::db::GraphCommit`] produced by the
    /// manifest publication. A successful no-op has no commit.
    pub async fn mutate_as_with_expected_head_receipt(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
        expected_head: Option<&str>,
    ) -> Result<crate::MutationReceipt> {
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
        let settings = self.effective(query_source)?;
        self.mutate_with_current_actor(
            branch,
            query_source,
            query_name,
            params,
            actor_id,
            expected_head,
            settings.stage_write_concurrency(),
            HistoryReleaseBytes(settings.history_release_bytes()),
            WriteBudget::from_settings(&settings),
        )
        .await
    }
}

impl Omnigraph {
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
        expected_head: Option<&str>,
        stage_write_concurrency: usize,
        history_release_bytes: HistoryReleaseBytes,
        write_budget: WriteBudget,
    ) -> Result<crate::MutationReceipt> {
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
                    expected_head,
                    stage_write_concurrency,
                    history_release_bytes,
                    attempt == 0,
                    &mut retryable,
                    write_budget,
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
                    crate::instrumentation::record_mutation_reprepare();
                    self.refresh_coordinator_only().await?;
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
        expected_head: Option<&str>,
        stage_write_concurrency: usize,
        history_release_bytes: HistoryReleaseBytes,
        first_attempt: bool,
        retryable: &mut bool,
        write_budget: WriteBudget,
    ) -> Result<crate::MutationReceipt> {
        let requested = Self::normalize_branch_name(branch)?;
        // Capture one branch-wide write authority: native branch identity,
        // exact optional graph head, accepted schema identity/catalog, and the
        // base table snapshot. Execution, validation, staging, and publication
        // all use this immutable attempt. `commit_all` revalidates the complete
        // token under the root-shared schema → branch → sorted-table gates
        // before its first detached commit.
        let mut txn = self
            .open_write_txn(requested.as_deref())
            .await
            .map_err(|error| {
                if first_attempt {
                    error.before_effect()
                } else {
                    error.without_pre_effect_evidence()
                }
            })?;
        // Caller CAS gate against the pinned view this attempt executes with —
        // a separate head lookup would reopen the race. Re-checked per
        // reprepare; not `ReadSetChanged`, so the retry loop never replays it.
        if let Some(expected) = expected_head {
            let actual = txn.effective_graph_head.as_deref();
            if actual != Some(expected) {
                return Err(OmniError::precondition_failed(
                    requested.as_deref().unwrap_or("main"),
                    expected,
                    actual.map(str::to_string),
                ));
            }
            txn.caller_expected_graph_head = Some(expected.to_string());
        }
        let mut resolved_params = params.clone();

        // Per-query staging accumulator. Inserts and updates push batches into
        // `pending`; deletes push predicates into `delete_predicates`. At the
        // boundary, `stage_all` prepares one exact transaction per touched table
        // and `commit_all` commits each as a detached version of its pinned
        // base (RFC 0067). The publisher then makes the complete result
        // graph-visible in one manifest CAS. Branch is threaded explicitly — no
        // coordinator swap.
        let mut staging = MutationStaging::new(write_budget);

        // Lower + validate up front so the touched-dataset set is known before
        // execution. A lowering/validation error returns exactly as it did
        // when this happened inside execute_named_mutation.
        let ir = self.lower_named_mutation(&txn.catalog, query_source, query_name)?;
        fill_declared_params(&mut resolved_params, &ir.params)?;
        validate_params(&resolved_params, &ir.params)?;
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
            Ok(total) if staging.is_empty() => {
                if txn.caller_expected_graph_head.is_some() {
                    fail(&MUTATION_POST_NO_EFFECT_PRE_GATE)?;
                    // A no-op has no table transaction, so it never reaches
                    // `commit_all`. It still needs a linearization point for
                    // the caller's CAS promise: under the same schema -> branch
                    // ordering as effectful writes (shared permit — this pass
                    // only reads the accepted view), revalidate the complete
                    // authority and map a moved caller head to terminal 412.
                    let _schema_permit = self.write_queue().acquire_schema_shared().await;
                    let _branch_guard = self
                        .write_queue()
                        .acquire_branch(requested.as_deref())
                        .await;
                    self.revalidate_write_txn(&txn).await?;
                }
                Ok(crate::MutationReceipt {
                    result: total,
                    commit: None,
                })
            }
            Ok(total) => {
                self.validate_staged_mutation(&staging, &txn).await?;
                let staged = staging
                    .stage_all_with_concurrency(self, requested.as_deref(), stage_write_concurrency)
                    .await?;
                fail(&MUTATION_POST_STAGE_PRE_EFFECT_GATE)?;
                let lineage_intent = self
                    .new_lineage_intent_for_branch(
                        requested.as_deref(),
                        actor_id,
                        history_release_bytes,
                    )
                    .await?;
                // `_held_gates` holds the shared schema permit, branch
                // effect gate, and sorted table gates acquired by `commit_all`.
                // They remain held through manifest publication, covering the
                // complete same-process effect lifetime. They are a local
                // serialization aid; the exact publisher precondition remains
                // the correctness authority.
                let super::staging::CommittedMutation {
                    updates,
                    expected_versions,
                    gates: _held_gates,
                } = staged.commit_all(self, requested.as_deref(), &txn).await?;
                // Failpoint for the detached-effects → publisher boundary:
                // every table effect is committed detached but nothing is
                // graph-visible. A failure here leaves the graph unchanged and
                // the detached versions as reclaimable garbage. See
                // `tests/failpoints.rs::finalize_publisher_residual_does_not_drift_untouched_tables`.
                fail(&MUTATION_POST_FINALIZE_PRE_PUBLISHER)?;
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
                // RFC 0067: every effect is a detached commit of its pinned base,
                // so a publish failure leaves the graph unchanged; the error
                // is returned as is (a moved head is `ReadSetChanged`).
                let commit = publish_result?;
                Ok(crate::MutationReceipt {
                    result: total,
                    commit: Some(commit),
                })
            }
        }
    }

    /// Lower + validate a named mutation query into its IR.
    ///
    /// Hoisted out of [`Self::execute_named_mutation`] so the caller can
    /// inspect the IR before execution — specifically to compute the
    /// touched-dataset set (see [`Self::touched_table_keys`]) for up-front
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
            .map_err(query_lookup_error)?;

        let checked = typecheck_query_decl(catalog, &query_decl)?;
        let mutation_ctx = match checked {
            CheckedQuery::Mutation(ctx) => ctx,
            CheckedQuery::Read(_) => {
                return Err(OmniError::manifest(
                    "mutation execution called on a read query; use query instead".to_string(),
                ));
            }
        };

        let ir = lower_mutation_query(catalog, &query_decl, &mutation_ctx)?;
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
                    target,
                    assignments,
                } => {
                    self.execute_insert(target, assignments, params, branch, staging, txn)
                        .await?
                }
                MutationOpIR::Update {
                    target,
                    assignments,
                    predicate,
                } => {
                    self.execute_update(
                        target,
                        assignments,
                        predicate,
                        params,
                        branch,
                        staging,
                        txn,
                    )
                    .await?
                }
                MutationOpIR::Delete { target, predicate } => {
                    self.execute_delete(target, predicate, params, branch, staging, txn)
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
        target: &MutationTarget,
        assignments: &[IRAssignment],
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let catalog = &txn.catalog;
        match target {
            MutationTarget::Node { type_name } => {
                let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "checked mutation node type '{type_name}' is missing from catalog"
                    ))
                })?;
                let schema = node_type.arrow_schema.clone();
                let resolved = resolve_assignments(type_name, &schema, assignments, params)?;
                let blob_props = node_type.blob_properties.clone();
                let id = if let Some(key_properties) = node_type.key.as_ref() {
                    let mut typed_keys = Vec::with_capacity(key_properties.len());
                    for key_prop in key_properties {
                        let key_literal = resolved.get(key_prop).ok_or_else(|| {
                            OmniError::manifest(format!(
                                "insert missing @key property '{}'",
                                key_prop
                            ))
                        })?;
                        let key_field = schema.field_with_name(key_prop).map_err(|_| {
                            OmniError::manifest_internal(format!(
                                "@key property '{}' is missing from node {} Arrow schema",
                                key_prop, node_type.name
                            ))
                        })?;
                        typed_keys.push(literal_to_typed_array(
                            key_literal,
                            key_field.data_type(),
                            1,
                        )?);
                    }
                    crate::loader::canonical_key_id(&typed_keys, 0)?.ok_or_else(|| {
                        let key_description = match key_properties.as_slice() {
                            [key] => format!("@key property '{key}'"),
                            _ => format!("@key properties ({})", key_properties.join(", ")),
                        };
                        OmniError::manifest(format!("insert {key_description} cannot contain null"))
                    })?
                } else {
                    crate::dst_ids::new_ulid().to_string()
                };

                let batch = build_insert_batch(
                    &schema,
                    &id,
                    &resolved,
                    &blob_props,
                    catalog.system_columns,
                    staging.write_budget(),
                )?;
                let has_key = node_type.key.is_some();
                let table_key = format!("node:{}", type_name);
                let insert_kind = if has_key {
                    crate::db::MutationOpKind::Merge
                } else {
                    crate::db::MutationOpKind::Insert
                };
                let (_ds, _full_path, _table_branch) = open_table_for_mutation(
                    self,
                    staging,
                    branch,
                    &table_key,
                    insert_kind,
                    Some(txn),
                )
                .await?;
                let mode = if has_key {
                    PendingMode::Upsert
                } else {
                    PendingMode::StrictInsert
                };
                staging.append_batch(&table_key, schema, mode, batch)?;

                Ok(MutationResult {
                    affected_nodes: 1,
                    affected_edges: 0,
                })
            }
            MutationTarget::Edge { type_name } => {
                let edge_type = catalog.edge_types.get(type_name).ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "checked mutation edge type '{type_name}' is missing from catalog"
                    ))
                })?;
                let schema = edge_type.arrow_schema.clone();
                let resolved = resolve_assignments(type_name, &schema, assignments, params)?;
                let blob_props = edge_type.blob_properties.clone();
                let id = if let Some(key_columns) = edge_type.key.as_ref() {
                    let system_columns = catalog.system_columns;
                    let mut typed_keys = Vec::with_capacity(key_columns.len());
                    for key_col in key_columns {
                        let (assignment, is_endpoint) = match key_col.as_str() {
                            column if column == system_columns.src => ("from", true),
                            column if column == system_columns.dst => ("to", true),
                            other => (other, false),
                        };
                        let key_literal = resolved.get(assignment).ok_or_else(|| {
                            if is_endpoint {
                                OmniError::manifest(format!(
                                    "missing required edge endpoint '{}'",
                                    assignment
                                ))
                            } else {
                                OmniError::manifest(format!(
                                    "insert missing @key property '{}'",
                                    assignment
                                ))
                            }
                        })?;
                        let key_field = schema.field_with_name(key_col).map_err(|_| {
                            OmniError::manifest_internal(format!(
                                "@key property '{}' is missing from edge {} Arrow schema",
                                key_col, edge_type.name
                            ))
                        })?;
                        typed_keys.push(literal_to_typed_array(
                            key_literal,
                            key_field.data_type(),
                            1,
                        )?);
                    }
                    crate::loader::canonical_key_id(&typed_keys, 0)?.ok_or_else(|| {
                        OmniError::manifest(format!(
                            "insert @key properties ({}) cannot contain null",
                            key_columns.join(", ")
                        ))
                    })?
                } else {
                    crate::dst_ids::new_ulid().to_string()
                };

                let batch = build_insert_batch(
                    &schema,
                    &id,
                    &resolved,
                    &blob_props,
                    catalog.system_columns,
                    staging.write_budget(),
                )?;
                let has_key = edge_type.key.is_some();
                let table_key = format!("edge:{}", type_name);
                let insert_kind = if has_key {
                    crate::db::MutationOpKind::Merge
                } else {
                    crate::db::MutationOpKind::Insert
                };
                let (_handle, _full_path, _table_branch) = open_table_for_mutation(
                    self,
                    staging,
                    branch,
                    &table_key,
                    insert_kind,
                    Some(txn),
                )
                .await?;
                let mode = if has_key {
                    PendingMode::Upsert
                } else {
                    PendingMode::StrictInsert
                };
                staging.append_batch(&table_key, schema, mode, batch)?;

                self.invalidate_graph_index().await;

                Ok(MutationResult {
                    affected_nodes: 0,
                    affected_edges: 1,
                })
            }
        }
    }

    async fn execute_update(
        &self,
        target: &MutationTarget,
        assignments: &[IRAssignment],
        predicate: &IRExpr,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let catalog = &txn.catalog;
        let type_name = match target {
            MutationTarget::Node { type_name } => type_name,
            MutationTarget::Edge { type_name } => {
                return Err(OmniError::manifest(format!(
                    "update is only supported for node types, not '{}'",
                    type_name
                )));
            }
        };
        let node_type = catalog.node_types.get(type_name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "checked mutation node type '{type_name}' is missing from catalog"
            ))
        })?;

        // Reject updates to every @key component — physical identity is the
        // canonical typed tuple, so changing even a non-leading component
        // without changing `id` would make the row unreachable by its key.
        if let Some(key_properties) = node_type.key.as_ref() {
            if let Some(key_prop) = key_properties
                .iter()
                .find(|key_prop| assignments.iter().any(|a| a.property == key_prop.as_str()))
            {
                return Err(OmniError::manifest(format!(
                    "cannot update @key property '{}' — delete and re-insert instead",
                    key_prop
                )));
            }
        }

        let schema = node_type.arrow_schema.clone();
        let pred_expr = mutation_predicate_expr(predicate, params, &schema)?;
        // Resolved before the table is opened, so a null on a non-nullable
        // property is refused even when the predicate matches no row.
        let resolved = resolve_assignments(type_name, &schema, assignments, params)?;
        let blob_props = node_type.blob_properties.clone();
        preflight_blob_assignments(&resolved, &blob_props, 1, staging.write_budget())?;
        // Catalog order is kept: `concat_match_batches_to_schema` binds by position.
        let assigned_blobs = schema
            .fields()
            .iter()
            .filter(|field| {
                blob_props.contains(field.name()) && resolved.contains_key(field.name())
            })
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        let scan_schema: SchemaRef = if assigned_blobs.is_empty() {
            schema.clone()
        } else {
            let indices = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| !assigned_blobs.contains(&field.name().as_str()))
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            Arc::new(
                schema
                    .project(&indices)
                    .map_err(OmniError::arrow_internal)?,
            )
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
        let scan_budget = PendingScanBudget::new(
            &table_key,
            staging.pending_resource_usage(&table_key)?,
            staging.write_budget(),
        );
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
        let batches = if blob_props.is_empty() {
            self.storage()
                .scan_with_pending(
                    &ds,
                    pending_batches,
                    pending_schema,
                    None,
                    Some(pred_expr),
                    Some(catalog.system_columns.id),
                    scan_budget,
                )
                .await?
        } else {
            self.storage()
                .scan_with_pending_materialized_blobs(
                    &ds,
                    pending_batches,
                    pending_schema,
                    Some(pred_expr),
                    Some(catalog.system_columns.id),
                    &assigned_blobs,
                    scan_budget,
                )
                .await?
        };

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        let matched = concat_match_batches_to_schema(&scan_schema, batches)?;

        let affected_count = matched.num_rows();

        let updated = apply_assignments(
            &schema,
            &matched,
            &resolved,
            &blob_props,
            staging.write_budget(),
        )?;
        // Validation (value/enum/unique) runs end-of-query via the evaluator.

        // Accumulate the updated batch into the Merge-mode pending stream.
        // The accumulator may now contain entries with the same id as a
        // prior insert or update on this table; `MutationStaging::finalize`
        // dedupes by id (last-occurrence wins) before issuing the single
        // `stage_merge_insert` call at end-of-query.
        let updated_schema = updated.schema();
        staging.append_batch(&table_key, updated_schema, PendingMode::Upsert, updated)?;

        Ok(MutationResult {
            affected_nodes: affected_count,
            affected_edges: 0,
        })
    }

    async fn execute_delete(
        &self,
        target: &MutationTarget,
        predicate: &IRExpr,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        match target {
            MutationTarget::Node { type_name } => {
                self.execute_delete_node(type_name, predicate, params, branch, staging, txn)
                    .await
            }
            MutationTarget::Edge { type_name } => {
                self.execute_delete_edge(type_name, predicate, params, branch, staging, txn)
                    .await
            }
        }
    }

    async fn execute_delete_node(
        &self,
        type_name: &str,
        predicate: &IRExpr,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let node_type = txn.catalog.node_types.get(type_name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "checked mutation node type '{type_name}' is missing from catalog"
            ))
        })?;
        let pred_expr = mutation_predicate_expr(predicate, params, &node_type.arrow_schema)?;

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

        let scan_filter =
            dedup_delete_filter(&pred_expr, staging.recorded_delete_predicates(&table_key));
        let deleted_ids = scan_deleted_ids(
            self,
            &ds,
            txn.catalog.system_columns.id,
            scan_filter,
            &mut staging.deleted_id_budget,
        )
        .await?;

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
        fail(&MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE)?;
        staging.record_delete(&table_key, pred_expr.clone());

        let mut affected_edges = 0usize;

        let edge_info: Vec<(String, String, String)> = txn
            .catalog
            .edge_types
            .iter()
            .map(|(name, et)| (name.clone(), et.from_type.clone(), et.to_type.clone()))
            .collect();

        for (edge_name, from_type, to_type) in &edge_info {
            let mut cascade_filters = Vec::new();
            if from_type == type_name {
                cascade_filters.push(id_in_list_expr(
                    &deleted_ids,
                    txn.catalog.system_columns.src,
                ));
            }
            if to_type == type_name {
                cascade_filters.push(id_in_list_expr(
                    &deleted_ids,
                    txn.catalog.system_columns.dst,
                ));
            }
            let Some(cascade_filter) = cascade_filters.into_iter().reduce(Expr::or) else {
                continue;
            };

            let edge_table_key = format!("edge:{}", edge_name);
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
            let matched_ids = scan_deleted_ids(
                self,
                &edge_ds,
                txn.catalog.system_columns.id,
                count_filter,
                &mut staging.deleted_id_budget,
            )
            .await?;
            let matched = matched_ids.len();
            affected_edges += matched;

            if matched > 0 {
                staging.record_deleted_ids(&edge_table_key, matched_ids);
                staging.record_delete(&edge_table_key, cascade_filter);
            }
        }

        staging.record_deleted_ids(&table_key, deleted_ids);

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
        predicate: &IRExpr,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: &crate::db::WriteTxn,
    ) -> Result<MutationResult> {
        let edge_type = txn.catalog.edge_types.get(type_name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "checked mutation edge type '{type_name}' is missing from catalog"
            ))
        })?;
        let pred_expr = mutation_predicate_expr(predicate, params, &edge_type.arrow_schema)?;

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
            dedup_delete_filter(&pred_expr, staging.recorded_delete_predicates(&table_key));
        // Scan the matched edge ids (not just count): the ids feed validation so
        // a delete emptying a src below @card min is rejected; `len()` is the
        // affected count. One scan replaces the former count-here + resolve-at-
        // validation re-scan.
        let deleted_ids = scan_deleted_ids(
            self,
            &ds,
            txn.catalog.system_columns.id,
            count_filter,
            &mut staging.deleted_id_budget,
        )
        .await?;
        let affected = deleted_ids.len();

        if affected > 0 {
            staging.record_deleted_ids(&table_key, deleted_ids);
            staging.record_delete(&table_key, pred_expr.clone());
            self.invalidate_graph_index().await;
        }

        Ok(MutationResult {
            affected_nodes: 0,
            affected_edges: affected,
        })
    }
}

/// Walk the exact typed predicate a batch at a time, admitting each id against
/// `budget` before copying it.
async fn scan_deleted_ids(
    db: &Omnigraph,
    snapshot: &SnapshotHandle,
    id_column: &str,
    filter: Expr,
    budget: &mut DeletedIdBudget,
) -> Result<Vec<String>> {
    let mut stream = db
        .storage()
        .scan_filtered(snapshot, Some(&[id_column]), filter)
        .await?;
    let mut removed = Vec::new();
    while let Some(batch) = stream.try_next().await.map_err(OmniError::storage)? {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OmniError::manifest_internal("delete id scan did not return Utf8"))?;
        for i in 0..ids.len() {
            let id = ids.value(i);
            budget.retain(id)?;
            removed.push(id.to_owned());
        }
    }
    Ok(removed)
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
            .map_err(OmniError::arrow_internal);
    }
    arrow_select::concat::concat_batches(schema, &batches).map_err(|e| {
        OmniError::manifest_internal(format!(
            "mutation scan returned batches that violate the full logical schema \
             across the committed/pending boundary ({})",
            e
        ))
    })
}

fn enrich_mutation_params(params: &ParamMap) -> Result<ParamMap> {
    let mut resolved = params.clone();
    if resolved.contains_key(NOW_PARAM_NAME) {
        return Err(OmniError::manifest(format!(
            "param '{NOW_PARAM_NAME}': reserved for now() and cannot be bound"
        )));
    }
    let now = OffsetDateTime::from(crate::dst_clock::system_time_now())
        .truncate_to_millisecond()
        .format(&Rfc3339)
        .map_err(|e| OmniError::manifest(format!("failed to format now(): {}", e)))?;
    resolved.insert(NOW_PARAM_NAME.to_string(), Literal::DateTime(now));
    Ok(resolved)
}

#[cfg(test)]
mod literal_narrowing_tests {
    use super::*;

    #[test]
    fn scalar_narrowing_accepts_boundaries_and_rejects_wraparound() {
        assert!(
            literal_to_typed_array(&Literal::Integer(i32::MAX as i64), &DataType::Int32, 1).is_ok()
        );
        assert!(
            literal_to_typed_array(&Literal::Integer(i32::MAX as i64 + 1), &DataType::Int32, 1,)
                .is_err()
        );
        assert!(
            literal_to_typed_array(&Literal::Integer(u32::MAX as i64), &DataType::UInt32, 1)
                .is_ok()
        );
        assert!(
            literal_to_typed_array(&Literal::Integer(u32::MAX as i64 + 1), &DataType::UInt32, 1,)
                .is_err()
        );
        assert!(literal_to_typed_array(&Literal::Integer(-1), &DataType::UInt32, 1).is_err());
        assert!(literal_to_typed_array(&Literal::Integer(-1), &DataType::UInt64, 1).is_err());
    }

    #[test]
    fn float32_narrowing_accepts_boundary_and_rejects_nonfinite_results() {
        assert!(
            literal_to_typed_array(&Literal::Float(f32::MAX as f64), &DataType::Float32, 1,)
                .is_ok()
        );
        assert!(
            literal_to_typed_array(
                &Literal::Float(f32::MAX as f64 * (1.0 + f64::EPSILON)),
                &DataType::Float32,
                1,
            )
            .is_ok()
        );
        assert!(
            literal_to_typed_array(
                &Literal::Float(f32::MAX as f64 * (1.0 + f32::EPSILON as f64)),
                &DataType::Float32,
                1,
            )
            .is_err()
        );
        assert!(literal_to_typed_array(&Literal::Float(f64::MAX), &DataType::Float32, 1).is_err());
        assert!(
            literal_to_typed_array(&Literal::Float(f64::INFINITY), &DataType::Float32, 1).is_err()
        );
        assert!(
            literal_to_typed_array(&Literal::Float(f64::NEG_INFINITY), &DataType::Float64, 1,)
                .is_err()
        );
        assert!(literal_to_typed_array(&Literal::Float(f64::NAN), &DataType::Float64, 1).is_err());
    }
}

#[cfg(test)]
mod target_tests {
    use super::*;
    use crate::loader::LoadMode;
    use omnigraph_compiler::query::typecheck::MutationTypeContext;
    use omnigraph_compiler::settings::SessionSettings;

    #[tokio::test]
    async fn retained_edge_target_selects_edge_staging() {
        let dir = tempfile::tempdir().unwrap();
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(
                    dir.path().to_str().unwrap(),
                    r#"
node Endpoint { name: String @key }
node Shared { label: String }
edge Shared: Endpoint -> Endpoint { label: String }
"#,
                )
                .await
                .unwrap(),
            ),
            SessionSettings::default(),
        );
        db.load_jsonl(
            r#"{"type":"Endpoint","data":{"name":"a"}}
{"type":"Endpoint","data":{"name":"b"}}
{"type":"Shared","id":"node-existing","data":{"label":"existing"}}
{"edge":"Shared","id":"edge-existing","from":"a","to":"b","data":{"label":"existing"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();

        let txn = db.open_write_txn(None).await.unwrap();
        assert!(txn.catalog.node_types.contains_key("Shared"));
        assert!(txn.catalog.edge_types.contains_key("Shared"));
        let target = MutationTarget::Edge {
            type_name: "Shared".into(),
        };
        let ctx = MutationTypeContext {
            targets: vec![target.clone()],
        };
        for (source, is_insert) in [
            (
                r#"query q() { insert Shared { from: "a", to: "b", label: "new" } }"#,
                true,
            ),
            (
                r#"query q() { delete Shared where label = "existing" }"#,
                false,
            ),
        ] {
            let decl = omnigraph_compiler::find_named_query(source, "q").unwrap();
            let ir = lower_mutation_query(&txn.catalog, &decl, &ctx).unwrap();
            let retained = match &ir.ops[0] {
                MutationOpIR::Insert { target, .. } | MutationOpIR::Delete { target, .. } => target,
                other => panic!("unexpected operation: {other:?}"),
            };
            assert_eq!(retained, &target);
            let mut staging = MutationStaging::default();
            let result = db
                .execute_named_mutation(&ir, &ParamMap::new(), None, &mut staging, &txn)
                .await
                .unwrap();
            assert_eq!((result.affected_nodes, result.affected_edges), (0, 1));
            assert_eq!(staging.paths.len(), 1);
            assert!(staging.paths.contains_key("edge:Shared"));
            if is_insert {
                assert_eq!(
                    staging
                        .pending_batches("edge:Shared")
                        .iter()
                        .map(RecordBatch::num_rows)
                        .sum::<usize>(),
                    1
                );
                assert!(staging.pending_batches("node:Shared").is_empty());
                assert!(staging.deleted_ids.is_empty());
            } else {
                assert!(staging.pending.is_empty());
                assert_eq!(staging.deleted_ids.len(), 1);
                assert_eq!(staging.deleted_ids["edge:Shared"], ["edge-existing"]);
            }
        }

        let decl = omnigraph_compiler::find_named_query(
            r#"query q() { update Shared set { label: "changed" } where label = "existing" }"#,
            "q",
        )
        .unwrap();
        let ir = lower_mutation_query(&txn.catalog, &decl, &ctx).unwrap();
        let MutationOpIR::Update {
            target: retained, ..
        } = &ir.ops[0]
        else {
            panic!("expected update");
        };
        assert_eq!(retained, &target);
        let mut staging = MutationStaging::default();
        let error = db
            .execute_named_mutation(&ir, &ParamMap::new(), None, &mut staging, &txn)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("update is only supported for node types"),
            "{error}"
        );
        assert!(staging.is_empty());
        assert!(staging.paths.is_empty());
        assert!(staging.deleted_ids.is_empty());
    }
}
