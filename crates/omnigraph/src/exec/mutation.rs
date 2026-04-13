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

    ensure_node_id_exists(db, &edge_type.from_type, from, "src").await?;
    ensure_node_id_exists(db, &edge_type.to_type, to, "dst").await?;
    Ok(())
}

async fn ensure_node_id_exists(
    db: &Omnigraph,
    node_type: &str,
    id: &str,
    label: &str,
) -> Result<()> {
    let snapshot = db.snapshot();
    let table_key = format!("node:{}", node_type);
    let ds = snapshot.open(&table_key).await?;
    let filter = format!("id = '{}'", id.replace('\'', "''"));
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
/// Blob columns are excluded from the scan result, so assigned blob values are
/// synthesized from the full table schema and included inline in the update
/// batch. Unassigned blob columns are omitted so merge_insert leaves them
/// untouched.
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
            // Blob columns aren't in the scan result. If this blob has an
            // assignment, build the blob array inline so the single
            // merge_insert covers both scalar and blob updates. Unassigned
            // blob columns are omitted — merge_insert only touches columns
            // present in the batch.
            if let Some(Literal::String(uri)) = assignments.get(field.name()) {
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
            }
            // else: no assignment for this blob column — skip it
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
        let resolved_params = enrich_mutation_params(params)?;
        let operation = format!(
            "mutation:{}:branch={}",
            query_name,
            requested.as_deref().unwrap_or("main")
        );

        if requested.as_deref().is_some_and(is_internal_run_branch) {
            return self
                .execute_named_mutation_on_branch(
                    requested.as_deref(),
                    query_source,
                    query_name,
                    &resolved_params,
                )
                .await;
        }

        let target_branch = requested.clone().unwrap_or_else(|| "main".to_string());
        let target_head_before = self.latest_branch_snapshot_id(&target_branch).await?;
        let run = self
            .begin_run(&target_branch, Some(operation.as_str()))
            .await?;

        let staged_result = match self
            .execute_named_mutation_on_branch(
                Some(run.run_branch.as_str()),
                query_source,
                query_name,
                &resolved_params,
            )
            .await
        {
            Ok(result) => result,
            Err(err) => {
                let _ = self.fail_run(&run.run_id).await;
                return Err(err);
            }
        };

        let target_head_now = self.latest_branch_snapshot_id(&target_branch).await?;
        if target_head_now.as_str() != target_head_before.as_str() {
            let _ = self.fail_run(&run.run_id).await;
            return Err(OmniError::manifest_conflict(format!(
                "target branch '{}' advanced during transactional mutation; retry",
                target_branch
            )));
        }

        if let Err(err) = self.publish_run(&run.run_id).await {
            let _ = self.fail_run(&run.run_id).await;
            return Err(err);
        }

        Ok(staged_result)
    }

    async fn execute_named_mutation_on_branch(
        &mut self,
        branch: Option<&str>,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let requested = match branch {
            Some(branch) => Self::normalize_branch_name(branch)?,
            None => None,
        };
        let current = self.active_branch().map(str::to_string);
        if requested == current {
            return self
                .execute_named_mutation(query_source, query_name, params)
                .await;
        }

        let previous = self
            .swap_coordinator_for_branch(requested.as_deref())
            .await?;
        let result = self
            .execute_named_mutation(query_source, query_name, params)
            .await;
        self.restore_coordinator(previous);
        result
    }

    async fn execute_named_mutation(
        &mut self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
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

        let mut total = MutationResult::default();
        for op in &ir.ops {
            let result = match op {
                MutationOpIR::Insert {
                    type_name,
                    assignments,
                } => self.execute_insert(type_name, assignments, params).await?,
                MutationOpIR::Update {
                    type_name,
                    assignments,
                    predicate,
                } => {
                    self.execute_update(type_name, assignments, predicate, params)
                        .await?
                }
                MutationOpIR::Delete {
                    type_name,
                    predicate,
                } => self.execute_delete(type_name, predicate, params).await?,
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
            let has_key = node_type.key_property().is_some();
            let (state, table_branch) = if has_key {
                self.upsert_batch(type_name, true, schema, batch).await?
            } else {
                self.append_batch(type_name, true, schema, batch).await?
            };

            let table_key = format!("node:{}", type_name);
            self.commit_updates(&[crate::db::SubTableUpdate {
                table_key,
                table_version: state.version,
                table_branch,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            }])
            .await?;

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
            validate_edge_insert_endpoints(self, type_name, &resolved).await?;
            let (state, table_branch) = self.append_batch(type_name, false, schema, batch).await?;

            let table_key = format!("edge:{}", type_name);
            self.commit_updates(&[crate::db::SubTableUpdate {
                table_key,
                table_version: state.version,
                table_branch,
                row_count: state.row_count,
                version_metadata: state.version_metadata,
            }])
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

    /// Append a batch to a sub-table, returning (new_version, row_count).
    async fn append_batch(
        &self,
        type_name: &str,
        is_node: bool,
        _schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(crate::table_store::TableState, Option<String>)> {
        let table_key = if is_node {
            format!("node:{}", type_name)
        } else {
            format!("edge:{}", type_name)
        };
        let (mut ds, full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let state = self
            .table_store()
            .append_batch(&full_path, &mut ds, batch)
            .await?;
        Ok((state, table_branch))
    }

    /// Upsert a batch into a sub-table using merge_insert keyed by "id".
    /// Used for @key node types to enforce uniqueness.
    async fn upsert_batch(
        &self,
        type_name: &str,
        is_node: bool,
        _schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(crate::table_store::TableState, Option<String>)> {
        let table_key = if is_node {
            format!("node:{}", type_name)
        } else {
            format!("edge:{}", type_name)
        };
        let (ds, full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let state = self
            .table_store()
            .merge_insert_batch(
                &full_path,
                ds,
                batch,
                vec!["id".to_string()],
                lance::dataset::WhenMatched::UpdateAll,
                lance::dataset::WhenNotMatched::InsertAll,
            )
            .await?;
        Ok((state, table_branch))
    }

    async fn execute_update(
        &mut self,
        type_name: &str,
        assignments: &[IRAssignment],
        predicate: &IRMutationPredicate,
        params: &ParamMap,
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
        let (ds, full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let initial_version = ds.version().version;

        let non_blob_cols: Vec<&str> = schema
            .fields()
            .iter()
            .filter(|f| !blob_props.contains(f.name()))
            .map(|f| f.name().as_str())
            .collect();
        let batches = self
            .table_store()
            .scan(
                &ds,
                (!blob_props.is_empty()).then_some(non_blob_cols.as_slice()),
                Some(&pred_sql),
                None,
            )
            .await?;

        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 0,
            });
        }

        let matched = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            let s = batches[0].schema();
            arrow_select::concat::concat_batches(&s, &batches)
                .map_err(|e| OmniError::Lance(e.to_string()))?
        };

        let affected_count = matched.num_rows();

        let mut resolved: HashMap<String, Literal> = HashMap::new();
        for a in assignments {
            resolved.insert(a.property.clone(), resolve_expr_value(&a.value, params)?);
        }
        let updated = apply_assignments(&schema, &matched, &resolved, &blob_props)?;
        crate::loader::validate_value_constraints(&updated, &self.catalog().node_types[type_name])?;

        // Re-open for merge_insert (scan consumed the dataset;
        // version guard was already applied by open_for_mutation above)
        let ds = self
            .reopen_for_mutation(
                &table_key,
                &full_path,
                table_branch.as_deref(),
                initial_version,
            )
            .await?;
        let update_state = self
            .table_store()
            .merge_insert_batch(
                &full_path,
                ds,
                updated,
                vec!["id".to_string()],
                lance::dataset::WhenMatched::UpdateAll,
                lance::dataset::WhenNotMatched::DoNothing,
            )
            .await?;

        self.commit_updates(&[crate::db::SubTableUpdate {
            table_key,
            table_version: update_state.version,
            table_branch,
            row_count: update_state.row_count,
            version_metadata: update_state.version_metadata,
        }])
        .await?;

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
    ) -> Result<MutationResult> {
        let is_node = self.catalog().node_types.contains_key(type_name);
        if is_node {
            self.execute_delete_node(type_name, predicate, params).await
        } else {
            self.execute_delete_edge(type_name, predicate, params).await
        }
    }

    async fn execute_delete_node(
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, false)?;

        let table_key = format!("node:{}", type_name);
        let (ds, full_path, table_branch) = self.open_for_mutation(&table_key).await?;
        let initial_version = ds.version().version;

        // Scan matching IDs for cascade
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

        // Delete nodes (re-open needed because the scan consumed the dataset;
        // version guard was already applied by open_for_mutation above)
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

        let mut updates = vec![crate::db::SubTableUpdate {
            table_key,
            table_version: delete_state.version,
            table_branch: table_branch.clone(),
            row_count: delete_state.row_count,
            version_metadata: delete_state.version_metadata,
        }];

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
                self.open_for_mutation(&edge_table_key).await?;

            let edge_delete = self
                .table_store()
                .delete_where(&edge_full_path, &mut edge_ds, &cascade_filter)
                .await?;

            affected_edges += edge_delete.deleted_rows;

            if edge_delete.deleted_rows > 0 {
                updates.push(crate::db::SubTableUpdate {
                    table_key: edge_table_key,
                    table_version: edge_delete.version,
                    table_branch: edge_table_branch,
                    row_count: edge_delete.row_count,
                    version_metadata: edge_delete.version_metadata,
                });
            }
        }

        self.commit_updates(&updates).await?;

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
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, true)?;

        let table_key = format!("edge:{}", type_name);
        let (mut ds, full_path, table_branch) = self.open_for_mutation(&table_key).await?;

        let delete_state = self
            .table_store()
            .delete_where(&full_path, &mut ds, &pred_sql)
            .await?;
        let affected = delete_state.deleted_rows;

        if affected > 0 {
            self.commit_updates(&[crate::db::SubTableUpdate {
                table_key,
                table_version: delete_state.version,
                table_branch,
                row_count: delete_state.row_count,
                version_metadata: delete_state.version_metadata,
            }])
            .await?;

            self.invalidate_graph_index().await;
        }

        Ok(MutationResult {
            affected_nodes: 0,
            affected_edges: affected,
        })
    }
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
