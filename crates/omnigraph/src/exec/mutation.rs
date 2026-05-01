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

async fn validate_edge_insert_endpoints(
    db: &Omnigraph,
    staging: &MutationStaging,
    branch: Option<&str>,
    edge_name: &str,
    assignments: &HashMap<String, Literal>,
) -> Result<()> {
    let edge_type = db
        .catalog()
        .edge_types
        .get(edge_name)
        .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", edge_name)))?;
    let from = match assignments.get("from") {
        Some(Literal::String(value)) => value.as_str(),
        Some(other) => {
            return Err(OmniError::manifest(format!(
                "edge {} from endpoint must be a string id, got {}",
                edge_name,
                literal_to_sql(other)
            )));
        }
        None => {
            return Err(OmniError::manifest(format!(
                "edge {} missing 'from' endpoint",
                edge_name
            )));
        }
    };
    let to = match assignments.get("to") {
        Some(Literal::String(value)) => value.as_str(),
        Some(other) => {
            return Err(OmniError::manifest(format!(
                "edge {} to endpoint must be a string id, got {}",
                edge_name,
                literal_to_sql(other)
            )));
        }
        None => {
            return Err(OmniError::manifest(format!(
                "edge {} missing 'to' endpoint",
                edge_name
            )));
        }
    };

    ensure_node_id_exists(db, staging, branch, &edge_type.from_type, from, "src").await?;
    ensure_node_id_exists(db, staging, branch, &edge_type.to_type, to, "dst").await?;
    Ok(())
}

/// Quick scan of pending batches for an `id` value match. Used by the
/// mutation path's edge endpoint validation to satisfy read-your-writes
/// for same-query inserts before they're committed to Lance.
fn pending_batches_contain_id(batches: &[RecordBatch], id: &str) -> bool {
    for batch in batches {
        let Some(col) = batch.column_by_name("id") else {
            continue;
        };
        let Some(arr) = col.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for i in 0..arr.len() {
            if arr.is_valid(i) && arr.value(i) == id {
                return true;
            }
        }
    }
    false
}

async fn ensure_node_id_exists(
    db: &Omnigraph,
    staging: &MutationStaging,
    branch: Option<&str>,
    node_type: &str,
    id: &str,
    label: &str,
) -> Result<()> {
    let table_key = format!("node:{}", node_type);

    // Prefer the in-query pending accumulator so a same-query insert of
    // the referenced node is visible to this validation. Fall back to
    // the pre-mutation manifest snapshot when nothing pending matches.
    let pending = staging.pending_batches(&table_key);
    if pending_batches_contain_id(pending, id) {
        return Ok(());
    }

    let filter = format!("id = '{}'", id.replace('\'', "''"));
    let snapshot = db.snapshot_for_branch(branch).await?;
    let ds = snapshot.open(&table_key).await?;
    let exists = ds
        .count_rows(Some(filter))
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        > 0;

    if exists {
        Ok(())
    } else {
        Err(OmniError::manifest(format!(
            "{} '{}' not found in {}",
            label, id, node_type
        )))
    }
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

/// Open a sub-table dataset for read or inline-commit-write within the
/// current mutation query, capturing pre-write metadata in `staging` on
/// first touch. The captured version is the publisher's CAS fence at
/// end-of-query (per-table OCC).
///
/// The dataset is always opened at HEAD on the requested branch. Under
/// the staged-write rewire (MR-794 step 2+), no per-op commit advances
/// HEAD, so subsequent opens within the same query observe the same
/// version. The exception is the inline-commit delete path
/// (`execute_delete_*`), which still advances HEAD per-op — but D₂ at
/// parse time prevents inserts/updates and deletes from coexisting in
/// one query, so this can't conflict with a pending-batch read.
async fn open_table_for_mutation(
    db: &Omnigraph,
    staging: &mut MutationStaging,
    branch: Option<&str>,
    table_key: &str,
) -> Result<(Dataset, String, Option<String>)> {
    let (ds, full_path, table_branch) =
        db.open_for_mutation_on_branch(branch, table_key).await?;
    let expected_version = ds.version().version;
    staging.ensure_path(
        table_key,
        full_path.clone(),
        table_branch.clone(),
        expected_version,
    );
    Ok((ds, full_path, table_branch))
}

/// D₂ parse-time check: a single mutation query is either insert/update-only
/// or delete-only. Mixed → reject before any I/O.
///
/// Reason: under the staged-write rewire (MR-794 step 2+), inserts and
/// updates accumulate in memory and commit at end-of-query, while deletes
/// still inline-commit (Lance lacks a public two-phase delete in 4.0.0).
/// Mixing creates ordering hazards (same-row insert→delete becomes a no-op
/// because the staged insert isn't visible to delete; cascading deletes
/// of just-inserted edges break referential integrity by silent design).
/// Until Lance exposes `DeleteJob::execute_uncommitted`, the parse-time
/// rejection keeps both paths atomic and correct.
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
             This restriction lifts when Lance exposes a two-phase delete API \
             (tracked: MR-793 / Lance-upstream).",
            ir.name
        )));
    }
    Ok(())
}

impl Omnigraph {
    pub async fn mutate(
        &mut self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        self.mutate_as(branch, query_source, query_name, params, None)
            .await
    }

    pub async fn mutate_as(
        &mut self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        actor_id: Option<&str>,
    ) -> Result<MutationResult> {
        let previous_actor = self.audit_actor_id.clone();
        self.audit_actor_id = actor_id.map(str::to_string);
        let result = self
            .mutate_with_current_actor(branch, query_source, query_name, params)
            .await;
        self.audit_actor_id = previous_actor;
        result
    }

    async fn mutate_with_current_actor(
        &mut self,
        branch: &str,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        self.ensure_schema_state_valid().await?;
        let requested = Self::normalize_branch_name(branch)?;
        // Reject internal `__run__*` / system-prefixed branches at the public
        // write boundary. The pre-MR-771 path got this guard transitively via
        // `begin_run`'s `ensure_public_branch_ref` call; the direct-publish
        // path needs to assert it explicitly so a caller can't write to
        // legacy or system staging branches by passing the prefix verbatim.
        if let Some(name) = requested.as_deref() {
            crate::db::ensure_public_branch_ref(name, "mutate")?;
        }
        let resolved_params = enrich_mutation_params(params)?;

        // Per-query staging accumulator. Inserts and updates push batches
        // into `pending`; deletes still inline-commit and record into
        // `inline_committed`. At end-of-query, `finalize` issues one
        // `stage_*` + `commit_staged` per pending table, then the
        // publisher commits the manifest atomically across all touched
        // tables. Branch is threaded explicitly — no coordinator swap.
        let mut staging = MutationStaging::default();

        let exec_result = self
            .execute_named_mutation(
                query_source,
                query_name,
                &resolved_params,
                requested.as_deref(),
                &mut staging,
            )
            .await;

        match exec_result {
            Err(e) => Err(e),
            Ok(total) if staging.is_empty() => Ok(total),
            Ok(total) => {
                let (updates, expected_versions) = staging
                    .finalize(self, requested.as_deref())
                    .await?;
                self.commit_updates_on_branch_with_expected(
                    requested.as_deref(),
                    &updates,
                    &expected_versions,
                )
                .await?;
                Ok(total)
            }
        }
    }

    async fn execute_named_mutation(
        &mut self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
    ) -> Result<MutationResult> {
        let query_decl = omnigraph_compiler::find_named_query(query_source, query_name)
            .map_err(|e| OmniError::manifest(e.to_string()))?;

        let checked = typecheck_query_decl(self.catalog(), &query_decl)?;
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

        let mut total = MutationResult::default();
        for op in &ir.ops {
            let result = match op {
                MutationOpIR::Insert {
                    type_name,
                    assignments,
                } => {
                    self.execute_insert(type_name, assignments, params, branch, staging)
                        .await?
                }
                MutationOpIR::Update {
                    type_name,
                    assignments,
                    predicate,
                } => {
                    self.execute_update(
                        type_name,
                        assignments,
                        predicate,
                        params,
                        branch,
                        staging,
                    )
                    .await?
                }
                MutationOpIR::Delete {
                    type_name,
                    predicate,
                } => {
                    self.execute_delete(type_name, predicate, params, branch, staging)
                        .await?
                }
            };
            total.affected_nodes += result.affected_nodes;
            total.affected_edges += result.affected_edges;
        }
        Ok(total)
    }

    async fn execute_insert(
        &mut self,
        type_name: &str,
        assignments: &[IRAssignment],
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
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
            crate::loader::validate_value_constraints(&batch, node_type)?;
            crate::loader::validate_enum_constraints(&batch, &node_type.properties, type_name)?;
            let unique_props = crate::loader::unique_property_names_for_node(node_type);
            if !unique_props.is_empty() {
                crate::loader::enforce_unique_constraints_intra_batch(
                    &batch,
                    type_name,
                    &unique_props,
                )?;
            }
            let has_key = node_type.key_property().is_some();
            let table_key = format!("node:{}", type_name);
            // Capture pre-write metadata on first touch (no Lance write).
            let (_ds, _full_path, _table_branch) =
                open_table_for_mutation(self, staging, branch, &table_key).await?;
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
            validate_edge_insert_endpoints(self, staging, branch, type_name, &resolved).await?;
            crate::loader::validate_enum_constraints(&batch, &edge_type.properties, type_name)?;
            let unique_props = crate::loader::unique_property_names_for_edge(edge_type);
            if !unique_props.is_empty() {
                crate::loader::enforce_unique_constraints_intra_batch(
                    &batch,
                    type_name,
                    &unique_props,
                )?;
            }
            let table_key = format!("edge:{}", type_name);
            // Capture pre-write metadata on first touch (no Lance write).
            let (ds, _full_path, _table_branch) =
                open_table_for_mutation(self, staging, branch, &table_key).await?;
            // Accumulate the new edge row. Edge IDs are ULID-generated so
            // Append mode is correct (no key-based dedup needed).
            staging.append_batch(&table_key, schema, PendingMode::Append, batch.clone())?;

            // Edge cardinality validation: scan committed edges via Lance
            // + iterate pending edges in-memory for the `src` column,
            // group-by-src. The pending side already includes the row
            // we just appended (above).
            validate_edge_cardinality_with_pending(
                self,
                &ds,
                staging,
                &table_key,
                edge_type,
            )
            .await?;

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
        &mut self,
        type_name: &str,
        assignments: &[IRAssignment],
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
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

        let table_key = format!("node:{}", type_name);
        let (ds, _full_path, _table_branch) =
            open_table_for_mutation(self, staging, branch, &table_key).await?;

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
        let projection: Option<&[&str]> =
            (!blob_props.is_empty()).then_some(non_blob_cols.as_slice());
        let pending_batches = staging.pending_batches(&table_key);
        let pending_schema = staging.pending_schema(&table_key);
        let batches = self
            .table_store()
            .scan_with_pending(
                &ds,
                pending_batches,
                pending_schema,
                projection,
                Some(&pred_sql),
            )
            .await?;

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        // Concat the matched batches into one. The pending side may have
        // a slightly different schema (e.g. Lance's `_rowid` column on
        // the committed side, missing on the pending side). Normalize by
        // dropping any `_rowid` / `_rowaddr` columns and reordering to
        // the table's canonical schema.
        let matched = concat_match_batches_to_schema(&schema, &blob_props, batches)?;

        let affected_count = matched.num_rows();

        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }
        let updated = apply_assignments(&schema, &matched, &resolved, &blob_props)?;
        let node_type = &self.catalog().node_types[type_name];
        crate::loader::validate_value_constraints(&updated, node_type)?;
        crate::loader::validate_enum_constraints(&updated, &node_type.properties, type_name)?;
        let unique_props = crate::loader::unique_property_names_for_node(node_type);
        if !unique_props.is_empty() {
            crate::loader::enforce_unique_constraints_intra_batch(
                &updated,
                type_name,
                &unique_props,
            )?;
        }

        // Accumulate the updated batch into the Merge-mode pending stream.
        // The accumulator may now contain entries with the same id as a
        // prior insert or update on this table; `MutationStaging::finalize`
        // dedupes by id (last-occurrence wins) before issuing the single
        // `stage_merge_insert` call at end-of-query.
        let updated_schema = updated.schema();
        staging.append_batch(&table_key, updated_schema, PendingMode::Merge, updated)?;

        Ok(MutationResult {
            affected_nodes: affected_count,
            affected_edges: 0,
        })
    }

    async fn execute_delete(
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
    ) -> Result<MutationResult> {
        let is_node = self.catalog().node_types.contains_key(type_name);
        if is_node {
            self.execute_delete_node(type_name, predicate, params, branch, staging)
                .await
        } else {
            self.execute_delete_edge(type_name, predicate, params, branch, staging)
                .await
        }
    }

    async fn execute_delete_node(
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, false)?;

        let table_key = format!("node:{}", type_name);
        let (ds, full_path, table_branch) =
            open_table_for_mutation(self, staging, branch, &table_key).await?;
        let initial_version = ds.version().version;

        // Scan matching IDs for cascade. Per D₂ this never overlaps with
        // staged inserts (mixed insert/delete in one query is rejected at
        // parse time), so we scan committed only.
        let batches = self
            .table_store()
            .scan(&ds, Some(&["id"]), Some(&pred_sql), None)
            .await?;

        let deleted_ids: Vec<String> = batches
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
            .collect();

        if deleted_ids.is_empty() {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        let affected_nodes = deleted_ids.len();

        // Delete nodes — still inline-commit (Lance's `Dataset::delete` is
        // not exposed as a two-phase op in 4.0.0). D₂ keeps inserts and
        // deletes from coexisting in one query, so this advance of Lance
        // HEAD is the only HEAD movement during the query and the
        // publisher's CAS captures it intact.
        let mut ds = self
            .reopen_for_mutation(
                &table_key,
                &full_path,
                table_branch.as_deref(),
                initial_version,
            )
            .await?;
        let delete_state = self
            .table_store()
            .delete_where(&full_path, &mut ds, &pred_sql)
            .await?;

        staging.record_inline(crate::db::SubTableUpdate {
            table_key: table_key.clone(),
            table_version: delete_state.version,
            table_branch: table_branch.clone(),
            row_count: delete_state.row_count,
            version_metadata: delete_state.version_metadata,
        });

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
            let (mut edge_ds, edge_full_path, edge_table_branch) =
                open_table_for_mutation(self, staging, branch, &edge_table_key).await?;

            let edge_delete = self
                .table_store()
                .delete_where(&edge_full_path, &mut edge_ds, &cascade_filter)
                .await?;

            affected_edges += edge_delete.deleted_rows;

            if edge_delete.deleted_rows > 0 {
                staging.record_inline(crate::db::SubTableUpdate {
                    table_key: edge_table_key,
                    table_version: edge_delete.version,
                    table_branch: edge_table_branch,
                    row_count: edge_delete.row_count,
                    version_metadata: edge_delete.version_metadata,
                });
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
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        branch: Option<&str>,
        staging: &mut MutationStaging,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, true)?;

        let table_key = format!("edge:{}", type_name);
        let (mut ds, full_path, table_branch) =
            open_table_for_mutation(self, staging, branch, &table_key).await?;

        let delete_state = self
            .table_store()
            .delete_where(&full_path, &mut ds, &pred_sql)
            .await?;
        let affected = delete_state.deleted_rows;

        if affected > 0 {
            staging.record_inline(crate::db::SubTableUpdate {
                table_key,
                table_version: delete_state.version,
                table_branch,
                row_count: delete_state.row_count,
                version_metadata: delete_state.version_metadata,
            });
            self.invalidate_graph_index().await;
        }

        Ok(MutationResult {
            affected_nodes: 0,
            affected_edges: affected,
        })
    }
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

/// Validate that adding `pending` edges plus the committed edges does not
/// exceed the per-source cardinality bound on `edge_type`. Reads the `src`
/// column from both committed (Lance) and pending (in-memory) and counts
/// per src.
async fn validate_edge_cardinality_with_pending(
    db: &Omnigraph,
    committed_ds: &Dataset,
    staging: &MutationStaging,
    table_key: &str,
    edge_type: &omnigraph_compiler::catalog::EdgeType,
) -> Result<()> {
    use std::collections::HashMap as StdHashMap;

    if edge_type.cardinality.is_default() {
        return Ok(());
    }

    let mut counts: StdHashMap<String, u32> = StdHashMap::new();

    // Committed side: scan `src` column (cheap, no filter).
    let committed = db
        .table_store()
        .scan(committed_ds, Some(&["src"]), None, None)
        .await?;
    for batch in &committed {
        let srcs = batch
            .column_by_name("src")
            .ok_or_else(|| OmniError::Lance("missing 'src' column on edge table".into()))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OmniError::Lance("'src' column is not Utf8".into()))?;
        for i in 0..srcs.len() {
            if srcs.is_valid(i) {
                *counts.entry(srcs.value(i).to_string()).or_insert(0) += 1;
            }
        }
    }

    // Pending side: walk in-memory pending batches for `src`.
    for batch in staging.pending_batches(table_key) {
        let Some(col) = batch.column_by_name("src") else {
            continue;
        };
        let Some(srcs) = col.as_any().downcast_ref::<StringArray>() else {
            continue;
        };
        for i in 0..srcs.len() {
            if srcs.is_valid(i) {
                *counts.entry(srcs.value(i).to_string()).or_insert(0) += 1;
            }
        }
    }

    let card = &edge_type.cardinality;
    for (src, count) in &counts {
        if let Some(max) = card.max {
            if *count > max {
                return Err(OmniError::manifest(format!(
                    "@card violation on edge {}: source '{}' has {} edges (max {})",
                    edge_type.name, src, count, max
                )));
            }
        }
        // Note: per-source minimum cardinality cannot be checked
        // mid-query (a bound of `2..` requires both edges to be inserted
        // before validation). The publisher path could re-validate at
        // commit time; for now, defer to the loader's end-of-query check.
        let _ = card.min;
    }

    Ok(())
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
