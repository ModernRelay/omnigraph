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
/// Blob columns may or may not be present in `batch` depending on the
/// caller's scan projection:
/// - If `batch` does NOT contain a blob column AND it has no assignment,
///   the column is OMITTED from the output. `merge_insert` leaves it
///   untouched.
/// - If `batch` DOES contain a blob column AND it has no assignment, the
///   column is COPIED to the output. This enables coalescing of
///   different-shape updates into a single full-schema merge batch (the
///   per-table accumulator in `MutationStaging` requires consistent
///   schemas across pending batches for `concat_batches`). The
///   round-tripping cost is acceptable for typical agent-driven
///   mutations; tables with large blobs and unassigned-blob updates may
///   want to be split into separate queries.
/// - If a blob column has a string-URI assignment, build the blob array
///   inline.
/// RFC-022: the column plan for a partial-schema update.
struct PartialUpdatePlan {
    /// Staged-batch schema: (id + assigned + completion), in catalog order.
    output_schema: SchemaRef,
    /// Scan schema: (id + completion-minus-assigned), in catalog order —
    /// the only columns whose OLD values the update needs.
    scan_schema: SchemaRef,
    /// `scan_schema`'s column names (the scan projection).
    scan_cols: Vec<String>,
}

/// Compute the partial-update column plan: the staged source carries the merge
/// key (`id`), the assigned columns (values from literals), and the
/// constraint-completion columns — every member of a `@unique` group that
/// intersects the assigned set, so the end-of-query evaluator can validate the
/// whole tuple (an update assigning only `room` of `@unique(room, hour)` must
/// still detect a (room, hour) collision). Columns outside the plan are left
/// physically untouched by the matched-only merge, and every index over them
/// keeps its coverage (Lance prunes only `fields_modified`).
fn partial_update_plan(
    node_type: &omnigraph_compiler::catalog::NodeType,
    full_schema: &SchemaRef,
    assignments: &[IRAssignment],
) -> PartialUpdatePlan {
    let assigned: HashSet<&str> = assignments.iter().map(|a| a.property.as_str()).collect();
    let mut completion: HashSet<&str> = HashSet::new();
    for group in &node_type.unique_constraints {
        if group.iter().any(|col| assigned.contains(col.as_str())) {
            for col in group {
                completion.insert(col.as_str());
            }
        }
    }

    let mut output_fields = Vec::new();
    let mut scan_fields = Vec::new();
    for field in full_schema.fields() {
        let name = field.name().as_str();
        let is_assigned = assigned.contains(name);
        let in_completion = completion.contains(name);
        if name == "id" || is_assigned || in_completion {
            output_fields.push(field.clone());
        }
        if name == "id" || (in_completion && !is_assigned) {
            scan_fields.push(field.clone());
        }
    }
    let scan_cols = scan_fields
        .iter()
        .map(|f| f.name().clone())
        .collect::<Vec<_>>();
    PartialUpdatePlan {
        output_schema: Arc::new(Schema::new(output_fields)),
        scan_schema: Arc::new(Schema::new(scan_fields)),
        scan_cols,
    }
}

fn apply_assignments(
    full_schema: &SchemaRef,
    batch: &RecordBatch,
    assignments: &HashMap<String, Literal>,
    blob_properties: &HashSet<String>,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(full_schema.fields().len());
    let mut out_fields: Vec<Field> = Vec::with_capacity(full_schema.fields().len());

    for field in full_schema.fields().iter() {
        if blob_properties.contains(field.name()) {
            if let Some(Literal::String(uri)) = assignments.get(field.name()) {
                // Assigned: build a single blob column from the URI.
                let mut builder = BlobArrayBuilder::new(batch.num_rows());
                for _ in 0..batch.num_rows() {
                    crate::loader::append_blob_value(&mut builder, uri)?;
                }
                let blob_field = lance::blob::blob_field(field.name(), true);
                out_fields.push(blob_field);
                columns.push(
                    builder
                        .finish()
                        .map_err(|e| OmniError::Lance(e.to_string()))?,
                );
            } else if let Some(col) = batch.column_by_name(field.name()) {
                // Unassigned but scan included it: copy through (writes
                // back the same blob, no observable change but uniform
                // schema for the accumulator).
                let blob_field = lance::blob::blob_field(field.name(), field.is_nullable());
                out_fields.push(blob_field);
                columns.push(col.clone());
            }
            // else: scan did not include this blob column and no
            // assignment — omit. Caller's accumulator must accept the
            // narrower schema (legacy single-merge_insert path).
        } else if let Some(lit) = assignments.get(field.name()) {
            out_fields.push(field.as_ref().clone());
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
            out_fields.push(field.as_ref().clone());
            columns.push(col.clone());
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(out_fields)), columns)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

// ─── Mutation execution ──────────────────────────────────────────────────────

use super::staging::{MutationStaging, PendingMode};

/// Open a sub-table dataset for read or staged write within the current
/// mutation query, capturing pre-write metadata in `staging` on first touch.
/// The captured version is the publisher's CAS fence at end-of-query
/// (per-table OCC).
///
/// On first touch, opens the dataset at HEAD on the requested branch
/// via `open_for_mutation_on_branch`, which compares Lance HEAD against
/// the manifest's pinned version — that fence is the engine's
/// publisher-style OCC catching cross-writer drift before we make any
/// changes. For delete-only queries, this strict open is also the uncovered
/// drift guard.
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
impl Omnigraph {
}

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
    // directly for `ensure_path` so the no-open path still captures the publisher
    // CAS fence.
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
        // NOT a fresh `snapshot_for_branch` — which would re-run the schema-contract
        // validation the WriteTxn already did once (RFC-013 step 3b capture-once).
        // Cardinality reads LIVE HEAD per edge table (the #298 stale-handle fix) via
        // the live opener in `CommittedState::write`.
        let committed =
            crate::validate::CommittedState::write(&txn.base, self, txn.branch.as_deref());
        // `to_changeset` carries both constructive batches and the ids the delete
        // ops captured from their own scans (`deleted_ids`), so the evaluator
        // recounts the srcs a delete empties (`@card`) and sees removed rows for
        // RI — the faithful change-set the merge path also builds.
        crate::validate::validate_changeset(&staging.to_changeset(), &committed, &self.catalog())
            .await
    }

    async fn mutate_with_current_actor(
        &self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
    ) -> Result<MutationResult> {
        // Converge any pending recovery sidecar (a previously failed
        // writer's Phase B → Phase C residual) before executing: the
        // inline delete path advances Lance HEAD during execution and
        // the staged path's commit-time drift guard refuses
        // sidecar-covered drift, so a long-lived handle must heal here
        // — not at restart. One `list_dir` when no sidecars exist (the
        // steady state). MUST run before `open_write_txn` below — the heal
        // may advance the manifest, so the pinned base must be captured after.
        self.heal_pending_recovery_sidecars().await?;
        let requested = Self::normalize_branch_name(branch)?;
        // Reject internal `__run__*` / system-prefixed branches at the
        // public write boundary. Direct-publish paths assert this
        // explicitly so a caller can't write to legacy or system
        // staging branches by passing the prefix verbatim.
        if let Some(name) = requested.as_deref() {
            crate::db::ensure_public_branch_ref(name, "mutate")?;
        }
        // Capture-once write transaction (RFC-013 step 3b). `open_write_txn`
        // validates the schema contract ONCE (it resolves the branch target,
        // whose first line is `ensure_schema_state_valid`) and pins the base
        // snapshot for this write. Threaded as `Some(&txn)` through execution,
        // staging commit, and the manifest publish so the per-table opens and
        // the commit-time OCC re-read reuse the pinned base instead of
        // re-validating the contract at every resolve point. Captured AFTER the
        // recovery heal (which may advance the manifest) and AFTER `requested`
        // is known so it pins the post-heal snapshot for the correct branch.
        let txn = self.open_write_txn(requested.as_deref()).await?;
        let resolved_params = enrich_mutation_params(params)?;

        // Per-query staging accumulator. Inserts and updates push batches
        // into `pending`; deletes push predicates into `delete_predicates`. At
        // end-of-query, `finalize` issues one `stage_*` + `commit_staged` per
        // touched table (inserts/updates/deletes alike), then the publisher
        // commits the manifest atomically across all touched tables. Branch is
        // threaded explicitly — no coordinator swap.
        let mut staging = MutationStaging::default();

        // Lower + validate up front so the touched-table set is known before
        // execution. A lowering/validation error returns exactly as it did
        // when this happened inside execute_named_mutation.
        let ir = self.lower_named_mutation(query_source, query_name)?;

        // Up-front fork-queue acquisition (see the loader for the full
        // rationale): if this mutation will fork any touched table onto a
        // non-main branch, acquire the per-(table, branch) write queues for
        // every touched table before the first fork and hold them through the
        // publish, so the orphan-fork reclaim can't race a concurrent
        // in-process fork. The touched set is derived from the lowered IR.
        let fork_queue_guards: Option<(
            Vec<(String, Option<String>)>,
            Vec<tokio::sync::OwnedMutexGuard<()>>,
        )> = if let Some(active) = requested.as_deref() {
            let snapshot = self.snapshot_for_branch(Some(active)).await?;
            let touched: Vec<(String, Option<String>)> = self
                .touched_table_keys(&ir)
                .into_iter()
                .map(|k| (k, Some(active.to_string())))
                .collect();
            let needs_fork = touched.iter().any(|(table_key, _)| {
                snapshot
                    .entry(table_key)
                    .map(|e| e.table_branch.as_deref() != Some(active))
                    .unwrap_or(false)
            });
            if needs_fork {
                let guards = self.write_queue().acquire_many(&touched).await;
                Some((touched, guards))
            } else {
                None
            }
        } else {
            None
        };

        let exec_result = self
            .execute_named_mutation(
                &ir,
                &resolved_params,
                requested.as_deref(),
                &mut staging,
                Some(&txn),
            )
            .await;

        match exec_result {
            Err(e) => Err(e),
            Ok(total) if staging.is_empty() => Ok(total),
            Ok(total) => {
                self.validate_staged_mutation(&staging, &txn).await?;
                let staged = staging.stage_all(self, requested.as_deref()).await?;
                // `_queue_guards` holds per-(table_key, branch) write
                // queues acquired inside `commit_all`. Held across the
                // manifest publish below so no concurrent writer can
                // interleave between our commit_staged and our publish
                // (which would correctly fail our CAS but leave Lance
                // HEAD advanced — the residual class MR-870 recovers).
                let super::staging::CommittedMutation {
                    updates,
                    expected_versions,
                    sidecar_handle,
                    guards: _queue_guards,
                    committed_handles,
                } = staged
                    .commit_all(
                        self,
                        requested.as_deref(),
                        crate::db::manifest::SidecarKind::Mutation,
                        actor_id,
                        fork_queue_guards,
                        Some(&txn),
                    )
                    .await?;
                // Failpoint that wedges the documented finalize→publisher
                // residual: per-table `commit_staged` calls already
                // advanced Lance HEAD on every touched table; a failure
                // injected here mirrors the production-rare case where
                // the publisher's CAS pre-check rejects (or the manifest
                // write throws) after staged commits succeeded. The
                // sidecar written inside `staging.finalize()` persists
                // across this failure so the next `Omnigraph::open`'s
                // recovery sweep can roll forward — see
                // `tests/failpoints.rs::recovery_rolls_forward_after_finalize_publisher_failure`.
                crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_POST_FINALIZE_PRE_PUBLISHER)?;
                self.commit_updates_on_branch_with_expected(
                    requested.as_deref(),
                    &updates,
                    &expected_versions,
                    actor_id,
                    Some(&txn),
                    committed_handles,
                )
                .await?;
                // Phase C succeeded — sidecar can be deleted. If this
                // delete fails, the next open's sweep classifies every
                // table as NoMovement (manifest pin == Lance HEAD ==
                // post_commit_pin) and the sidecar is treated as a
                // stale artifact (cleaned up via the Phase 2 logic).
                if let Some(handle) = sidecar_handle {
                    // Best-effort cleanup: the manifest publish already
                    // succeeded, so the user's mutation is durable. A
                    // failed delete leaves the sidecar on disk; the
                    // next open's recovery sweep classifies every table
                    // as `NoMovement` (manifest pin == Lance HEAD ==
                    // post_commit_pin) and tidies up. Failing the user
                    // here would return an error for a write that
                    // already landed.
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
        query_source: &str,
        query_name: &str,
    ) -> Result<omnigraph_compiler::ir::MutationIR> {
        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;

        let checked = typecheck_query_decl(&self.catalog(), &query_decl)?;
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

    /// The COMPLETE set of `(node|edge):{type}` table keys a mutation IR can
    /// touch at execution time, keyed as `MutationStaging`/`commit_all` key
    /// them. Must be a superset of everything execution forks/commits, since
    /// it drives the up-front fork-queue acquisition and `commit_all`'s
    /// held-guard coverage check — a miss means an unserialized fork/commit.
    ///
    /// The set is a pure function of (IR ops + catalog). For each op it mirrors
    /// the execute path's node-vs-edge dispatch (`node_types` first, then
    /// `edge_types`). A `delete <Node>` additionally **cascades** to every edge
    /// type whose endpoint is that node (see `execute_delete_node`), forking
    /// those edge tables during execution — so they are included here, derived
    /// the same way the executor derives them (`from_type`/`to_type` match).
    /// Unknown types are skipped (the execute path surfaces the error).
    /// Sorted + deduped for one-shot `acquire_many`.
    fn touched_table_keys(&self, ir: &omnigraph_compiler::ir::MutationIR) -> Vec<String> {
        use omnigraph_compiler::ir::MutationOpIR;
        let catalog = self.catalog();
        let mut keys: Vec<String> = Vec::new();
        for op in &ir.ops {
            let type_name = match op {
                MutationOpIR::Insert { type_name, .. }
                | MutationOpIR::Update { type_name, .. }
                | MutationOpIR::Delete { type_name, .. } => type_name,
            };
            if catalog.node_types.contains_key(type_name) {
                keys.push(format!("node:{type_name}"));
                // A node delete cascades to every edge touching this node type,
                // forking those edge tables. Include them so the up-front
                // acquisition covers the cascade (mirrors execute_delete_node).
                if matches!(op, MutationOpIR::Delete { .. }) {
                    for (edge_name, edge_type) in &catalog.edge_types {
                        if edge_type.from_type == *type_name || edge_type.to_type == *type_name {
                            keys.push(format!("edge:{edge_name}"));
                        }
                    }
                }
            } else if catalog.edge_types.contains_key(type_name) {
                keys.push(format!("edge:{type_name}"));
            }
        }
        keys.sort();
        keys.dedup();
        keys
    }

    async fn execute_named_mutation(
        &self,
        ir: &omnigraph_compiler::ir::MutationIR,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
        txn: Option<&crate::db::WriteTxn>,
    ) -> Result<MutationResult> {
        // RFC-022 eligibility: a node table whose ONLY op in this query is a
        // single `update` stages a partial-schema matched-only merge (key +
        // assigned + constraint-completion columns). Every other combination
        // falls back to whole-row staging: partial and full batches cannot
        // share one uniform-schema merge source (a present column's null cell
        // means "set NULL", so widening a partial batch would null-overwrite),
        // a second same-table op's read-your-writes scan must see full rows,
        // and one table commits at most one version per query (invariant 4).
        let mut update_op_census: std::collections::HashMap<&str, (usize, usize)> =
            std::collections::HashMap::new();
        for op in &ir.ops {
            let (type_name, is_update) = match op {
                MutationOpIR::Update { type_name, .. } => (type_name.as_str(), true),
                MutationOpIR::Insert { type_name, .. }
                | MutationOpIR::Delete { type_name, .. } => (type_name.as_str(), false),
            };
            let entry = update_op_census.entry(type_name).or_insert((0, 0));
            entry.0 += 1;
            if is_update {
                entry.1 += 1;
            }
        }

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
                    let partial_ok = update_op_census
                        .get(type_name.as_str())
                        .is_some_and(|&(ops, updates)| ops == 1 && updates == 1);
                    self.execute_update(
                        type_name, assignments, predicate, params, branch, staging, txn,
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
        txn: Option<&crate::db::WriteTxn>,
    ) -> Result<MutationResult> {
        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }

        let is_node = self.catalog().node_types.contains_key(type_name);
        let is_edge = self.catalog().edge_types.contains_key(type_name);

        if is_node {
            let node_type = &self.catalog().node_types[type_name];
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
                open_table_for_mutation(self, staging, branch, &table_key, insert_kind, txn).await?;
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
            let edge_type = &self.catalog().edge_types[type_name];
            let schema = edge_type.arrow_schema.clone();
            let blob_props = edge_type.blob_properties.clone();
            let id = ulid::Ulid::new().to_string();

            let batch = build_insert_batch(&schema, &id, &resolved, &blob_props)?;
            // Validation (edge-RI, enum, unique, @card against LIVE HEAD) runs
            // end-of-query via the evaluator.
            let table_key = format!("edge:{}", type_name);
            // Capture pre-write metadata on first touch (ensure_path). Edge
            // inserts are non-strict, so with a `WriteTxn` this opens NOTHING
            // (collapse #1) and the handle is discarded — validation, including
            // `@card` against LIVE HEAD, runs end-of-query via the evaluator.
            let (_handle, _full_path, _table_branch) = open_table_for_mutation(
                self,
                staging,
                branch,
                &table_key,
                crate::db::MutationOpKind::Insert,
                txn,
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
        txn: Option<&crate::db::WriteTxn>,
        partial_ok: bool,
    ) -> Result<MutationResult> {
        // Defense in depth: ensure this is a node type
        if !self.catalog().node_types.contains_key(type_name) {
            return Err(OmniError::manifest(format!(
                "update is only supported for node types, not '{}'",
                type_name
            )));
        }

        // Reject updates to @key properties — identity is immutable
        if let Some(key_prop) = self.catalog().node_types[type_name].key_property() {
            if assignments.iter().any(|a| a.property == key_prop) {
                return Err(OmniError::manifest(format!(
                    "cannot update @key property '{}' — delete and re-insert instead",
                    key_prop
                )));
            }
        }

        let pred_sql = predicate_to_sql(predicate, params, false)?;
        let schema = self.catalog().node_types[type_name].arrow_schema.clone();
        let blob_props = self.catalog().node_types[type_name].blob_properties.clone();

        // RFC-022: when this update is the table's only op in the query,
        // stage a PARTIAL source — (id + assigned + constraint-completion)
        // columns — instead of whole rows. `partial_plan` is `None` on the
        // whole-row fallback path.
        let partial_plan = if partial_ok {
            Some(partial_update_plan(
                &self.catalog().node_types[type_name],
                &schema,
                assignments,
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
            txn,
        )
        .await?;
        // Update is a STRICT op, so collapse #1 never skips its open — the
        // handle is always `Some` (and it's needed for the committed scan below).
        let ds = handle.expect("strict Update op always opens its dataset");

        // Scan committed via Lance + apply the same predicate to pending
        // batches via DataFusion `MemTable` (read-your-writes for prior
        // ops in this query). The pending side may include rows from
        // earlier `insert` / `update` ops on the same table.
        //
        // For blob tables we project away the blob columns: Lance's
        // scanner doesn't accept the standard projection path on blob
        // descriptors and would panic with a `Field::project` assertion.
        // The downstream `apply_assignments` synthesizes blob columns
        // from explicit assignments and omits unassigned blobs (Lance's
        // merge_insert leaves them untouched). Tables without blob
        // columns scan the full schema unprojected.
        let non_blob_cols: Vec<&str> = schema
            .fields()
            .iter()
            .filter(|f| !blob_props.contains(f.name()))
            .map(|f| f.name().as_str())
            .collect();
        // Partial path: scan only (id + completion-minus-assigned) — assigned
        // columns' old values are never needed (`.gq` assignments are literal
        // values), and the WHERE predicate evaluates by pushdown without being
        // projected. Whole-row path: unchanged (full schema minus blobs).
        let partial_scan_cols: Vec<&str> = partial_plan
            .as_ref()
            .map(|plan| plan.scan_cols.iter().map(|c| c.as_str()).collect())
            .unwrap_or_default();
        let projection: Option<&[&str]> = if partial_plan.is_some() {
            Some(partial_scan_cols.as_slice())
        } else {
            (!blob_props.is_empty()).then_some(non_blob_cols.as_slice())
        };
        let pending_batches = staging.pending_batches(&table_key);
        let pending_schema = staging.pending_schema(&table_key);
        // Use merge semantics on the union: a committed row whose `id`
        // also appears in pending has been logically updated by an
        // earlier op in this query and is shadowed from the scan,
        // otherwise the predicate runs against stale committed values
        // and a chained `update where <pred>` can match a row whose
        // pending value no longer satisfies <pred>.
        let batches = self
            .storage()
            .scan_with_pending(
                &ds,
                pending_batches,
                pending_schema,
                projection,
                Some(&pred_sql),
                Some("id"),
            )
            .await?;

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        // Concat the matched batches (committed + pending) into one. The
        // helper trusts that both sides share a schema — Lance returns
        // dataset-schema-ordered columns and DataFusion returns
        // MemTable-schema-ordered columns; both should match the catalog's
        // arrow_schema when the projection is consistent. If they
        // diverge (typically a blob-table mid-schema-shift), the helper
        // surfaces a clear error directing the caller to split the
        // mutation.
        // Partial path: normalize/concat against the SCAN schema (id +
        // completion), then apply assignments over the OUTPUT schema (id +
        // assigned + completion) — assigned columns come from literals, so
        // they need no scanned values. Whole-row path: unchanged.
        let (concat_schema, output_schema) = match partial_plan.as_ref() {
            Some(plan) => (plan.scan_schema.clone(), plan.output_schema.clone()),
            None => (schema.clone(), schema.clone()),
        };
        let matched = concat_match_batches_to_schema(&concat_schema, &blob_props, batches)?;

        let affected_count = matched.num_rows();

        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }
        let updated = apply_assignments(&output_schema, &matched, &resolved, &blob_props)?;
        // Validation (value/enum/unique) runs end-of-query via the evaluator.

        // Accumulate the updated batch. Whole-row: the Merge-mode pending
        // stream (may coalesce with prior same-table ops; finalize dedupes by
        // id, last wins). Partial (RFC-022): a dedicated partial-update entry
        // that stages as a matched-only merge.
        let updated_schema = updated.schema();
        if partial_plan.is_some() {
            staging.append_partial_update_batch(&table_key, updated_schema, updated)?;
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
        txn: Option<&crate::db::WriteTxn>,
    ) -> Result<MutationResult> {
        let is_node = self.catalog().node_types.contains_key(type_name);
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
        txn: Option<&crate::db::WriteTxn>,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, false)?;

        let table_key = format!("node:{}", type_name);
        let (handle, _full_path, _table_branch) = open_table_for_mutation(
            self,
            staging,
            branch,
            &table_key,
            crate::db::MutationOpKind::Delete,
            txn,
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
        crate::failpoints::maybe_fail(crate::failpoints::names::MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE)?;
        staging.record_deleted_ids(&table_key, &deleted_ids);
        staging.record_delete(&table_key, pred_sql.clone());

        let mut affected_edges = 0usize;
        let escaped: Vec<String> = deleted_ids
            .iter()
            .map(|id| format!("'{}'", id.replace('\'', "''")))
            .collect();
        let id_list = escaped.join(", ");

        let edge_info: Vec<(String, String, String)> = self
            .catalog()
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
                txn,
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
            let count_filter =
                dedup_delete_filter(&cascade_filter, staging.recorded_delete_predicates(&edge_table_key));
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
        txn: Option<&crate::db::WriteTxn>,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, true)?;

        let table_key = format!("edge:{}", type_name);
        let (handle, _full_path, _table_branch) = open_table_for_mutation(
            self,
            staging,
            branch,
            &table_key,
            crate::db::MutationOpKind::Delete,
            txn,
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
/// surface a clear error so the user can split the query.
fn concat_match_batches_to_schema(
    _schema: &SchemaRef,
    _blob_properties: &HashSet<String>,
    batches: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    let common = batches[0].schema();
    arrow_select::concat::concat_batches(&common, &batches).map_err(|e| {
        OmniError::Lance(format!(
            "scan_with_pending returned batches with mismatched schemas \
             across the committed/pending boundary; this typically indicates \
             a blob-column shape mismatch between the committed table and a \
             prior in-query insert/update. Split blob-touching mutations \
             into separate queries. ({})",
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
