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

    ensure_node_id_exists(db, staging, &edge_type.from_type, from, "src").await?;
    ensure_node_id_exists(db, staging, &edge_type.to_type, to, "dst").await?;
    Ok(())
}

async fn ensure_node_id_exists(
    db: &Omnigraph,
    staging: &MutationStaging,
    node_type: &str,
    id: &str,
    label: &str,
) -> Result<()> {
    let table_key = format!("node:{}", node_type);
    let filter = format!("id = '{}'", id.replace('\'', "''"));

    // Prefer the in-query staged dataset so a same-query insert of the
    // referenced node is visible to this validation. Fall back to the
    // pre-mutation manifest snapshot when no prior op touched this table.
    let exists = if let Some(staged) = staging.latest.get(&table_key) {
        let ds = db
            .reopen_for_mutation(
                &table_key,
                &staged.full_path,
                staged.table_branch.as_deref(),
                staged.table_version,
            )
            .await?;
        ds.count_rows(Some(filter))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            > 0
    } else {
        let snapshot = db.snapshot();
        let ds = snapshot.open(&table_key).await?;
        ds.count_rows(Some(filter))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            > 0
    };

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

/// Per-query staging state for direct-to-target mutations. Replaces the
/// `__run__` staging branch with an in-memory accumulator.
///
/// Each unique table touched by the mutation is captured at first-open time:
/// - `expected_versions[table_key]` records the manifest version we observed
///   pre-write — the publisher's CAS fence at end-of-query.
/// - `latest[table_key]` records the most recent post-write state on that
///   table — used to thread between ops within the query so subsequent ops
///   see prior writes (read-your-writes).
///
/// **Known limitation (mid-query partial failure).** If op-N succeeds at the
/// Lance level (a fragment is committed, advancing the table's Lance HEAD)
/// and op-N+1 then fails before the publisher commits, the table is left
/// with `Lance HEAD > manifest_version`. The next `mutate_as` against the
/// same table will surface `ExpectedVersionMismatch` (Lance HEAD ahead of
/// the manifest snapshot). Lance's `restore()` is *not* a rewind — it
/// creates a new commit, monotonically advancing the version. A proper fix
/// requires per-table Lance-internal branches (write to a transient branch,
/// fast-forward main on success, drop branch on failure); tracked as a
/// follow-up to MR-771. In practice this path is narrow: most validation
/// runs before any Lance write, so single-statement mutations are
/// unaffected. See `docs/runs.md`.
#[derive(Default)]
struct MutationStaging {
    expected_versions: HashMap<String, u64>,
    latest: HashMap<String, StagedTable>,
}

struct StagedTable {
    table_key: String,
    table_branch: Option<String>,
    table_version: u64,
    row_count: u64,
    full_path: String,
    version_metadata: crate::db::manifest::TableVersionMetadata,
}

trait IntoStagedRecord {
    fn version(&self) -> u64;
    fn row_count(&self) -> u64;
    fn version_metadata(&self) -> &crate::db::manifest::TableVersionMetadata;
}

impl IntoStagedRecord for crate::table_store::TableState {
    fn version(&self) -> u64 {
        self.version
    }
    fn row_count(&self) -> u64 {
        self.row_count
    }
    fn version_metadata(&self) -> &crate::db::manifest::TableVersionMetadata {
        &self.version_metadata
    }
}

impl IntoStagedRecord for crate::table_store::DeleteState {
    fn version(&self) -> u64 {
        self.version
    }
    fn row_count(&self) -> u64 {
        self.row_count
    }
    fn version_metadata(&self) -> &crate::db::manifest::TableVersionMetadata {
        &self.version_metadata
    }
}

impl MutationStaging {
    fn is_empty(&self) -> bool {
        self.latest.is_empty()
    }

    fn record<S: IntoStagedRecord>(
        &mut self,
        table_key: String,
        full_path: String,
        table_branch: Option<String>,
        state: &S,
    ) {
        self.latest.insert(
            table_key.clone(),
            StagedTable {
                table_key,
                table_branch,
                table_version: state.version(),
                row_count: state.row_count(),
                full_path,
                version_metadata: state.version_metadata().clone(),
            },
        );
    }

    fn into_updates(self) -> (Vec<crate::db::SubTableUpdate>, HashMap<String, u64>) {
        let updates = self
            .latest
            .into_values()
            .map(|st| crate::db::SubTableUpdate {
                table_key: st.table_key,
                table_version: st.table_version,
                table_branch: st.table_branch,
                row_count: st.row_count,
                version_metadata: st.version_metadata,
            })
            .collect();
        (updates, self.expected_versions)
    }
}

/// RAII helper that restores `Omnigraph::coordinator` on drop. Used by
/// `mutate_with_current_actor` so a panic between the coordinator swap and
/// the explicit restore (e.g. an assertion deep inside Lance) does not
/// leave the handle pinned to the requested branch indefinitely. The
/// captured coordinator is `take`n on drop and assigned via the
/// (synchronous) `restore_coordinator` accessor.
///
/// Holds a bare `*mut Omnigraph` (no lifetime parameter) deliberately:
/// borrowing the engine through this guard would lock out the rest of
/// `mutate_with_current_actor` from calling `&mut self` methods on the
/// engine while the guard is alive. The unsafe is bounded by the
/// constructor contract — the caller must not let the guard outlive the
/// `&mut self` it was built from. In practice this is enforced by the
/// guard being assigned to a stack-local `_guard` binding inside one
/// function and never moved out.
struct CoordinatorRestoreGuard {
    db: *mut Omnigraph,
    previous: Option<crate::db::GraphCoordinator>,
}

// SAFETY: the pointer addresses an `Omnigraph`, which is `Send`. The guard
// is short-lived and the only operation it performs is the sync
// `restore_coordinator` field assignment in `Drop`. No reference is shared
// across threads — the future holding the guard moves between threads
// (e.g. when an Axum handler is awaited on a worker), and the swap-back is
// always invoked at most once on whichever thread runs `Drop`.
unsafe impl Send for CoordinatorRestoreGuard {}

impl CoordinatorRestoreGuard {
    /// SAFETY: `db` must outlive the returned guard, and the caller must
    /// not move the guard outside the borrow scope of `db`.
    fn new(db: &mut Omnigraph, previous: crate::db::GraphCoordinator) -> Self {
        Self {
            db: db as *mut Omnigraph,
            previous: Some(previous),
        }
    }
}

impl Drop for CoordinatorRestoreGuard {
    fn drop(&mut self) {
        if let Some(prev) = self.previous.take() {
            // SAFETY: per the `new` contract, `db` is still valid here.
            // `restore_coordinator` is a sync field assignment and does not
            // re-enter the runtime.
            unsafe {
                (*self.db).restore_coordinator(prev);
            }
        }
    }
}

/// Open a sub-table dataset for write in the current mutation query. On the
/// first touch of a table, captures the pre-write manifest version into
/// `staging.expected_versions` so the publisher can enforce OCC. On
/// subsequent touches, re-opens the dataset at the locally-staged version
/// (the version we wrote in a prior op of the same query) — bypassing the
/// manifest because nothing has been committed yet.
async fn open_for_mutation_in_query(
    db: &Omnigraph,
    staging: &mut MutationStaging,
    table_key: &str,
) -> Result<(Dataset, String, Option<String>)> {
    if let Some(staged) = staging.latest.get(table_key) {
        let ds = db
            .reopen_for_mutation(
                table_key,
                &staged.full_path,
                staged.table_branch.as_deref(),
                staged.table_version,
            )
            .await?;
        return Ok((ds, staged.full_path.clone(), staged.table_branch.clone()));
    }
    let (ds, full_path, table_branch) = db.open_for_mutation(table_key).await?;
    staging
        .expected_versions
        .entry(table_key.to_string())
        .or_insert(ds.version().version);
    Ok((ds, full_path, table_branch))
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

        // Direct-to-target write path. Per-query staging captures pre-write
        // manifest versions (publisher CAS fence) and threads dataset state
        // across ops to maintain read-your-writes within a multi-statement
        // query without per-op manifest commits.
        let mut staging = MutationStaging::default();

        let current = self.active_branch().map(str::to_string);
        let needs_swap = requested.as_deref() != current.as_deref();

        // RAII guard for coordinator state. If we swapped to the requested
        // branch, the original coordinator is captured here and unconditionally
        // restored on drop — including on panic from `execute_named_mutation`
        // or the publisher. Without this, a Lance-internal panic between swap
        // and restore would leave the handle pinned to the wrong branch for
        // its remaining lifetime.
        let _guard = if needs_swap {
            let previous = self.swap_coordinator_for_branch(requested.as_deref()).await?;
            Some(CoordinatorRestoreGuard::new(self, previous))
        } else {
            None
        };

        let exec_result = self
            .execute_named_mutation(query_source, query_name, &resolved_params, &mut staging)
            .await;

        let publish_result = match exec_result {
            Err(e) => Err(e),
            Ok(total) => {
                if staging.is_empty() {
                    Ok(total)
                } else {
                    let (updates, expected_versions) = staging.into_updates();
                    match self
                        .commit_updates_on_branch_with_expected(
                            requested.as_deref(),
                            &updates,
                            &expected_versions,
                        )
                        .await
                    {
                        Ok(_) => Ok(total),
                        Err(e) => Err(e),
                    }
                }
            }
        };

        // `_guard` drops here and restores the coordinator (also restores on
        // any panic that unwound through this function above).
        publish_result
    }

    async fn execute_named_mutation(
        &mut self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
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

        let mut total = MutationResult::default();
        for op in &ir.ops {
            let result = match op {
                MutationOpIR::Insert {
                    type_name,
                    assignments,
                } => {
                    self.execute_insert(type_name, assignments, params, staging)
                        .await?
                }
                MutationOpIR::Update {
                    type_name,
                    assignments,
                    predicate,
                } => {
                    self.execute_update(type_name, assignments, predicate, params, staging)
                        .await?
                }
                MutationOpIR::Delete {
                    type_name,
                    predicate,
                } => {
                    self.execute_delete(type_name, predicate, params, staging)
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
            let (state, full_path, table_branch) = if has_key {
                self.upsert_batch_staged(&table_key, staging, schema, batch).await?
            } else {
                self.append_batch_staged(&table_key, staging, schema, batch).await?
            };

            staging.record(table_key, full_path, table_branch, &state);

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
            validate_edge_insert_endpoints(self, staging, type_name, &resolved).await?;
            crate::loader::validate_enum_constraints(&batch, &edge_type.properties, type_name)?;
            let unique_props = crate::loader::unique_property_names_for_edge(edge_type);
            if !unique_props.is_empty() {
                crate::loader::enforce_unique_constraints_intra_batch(
                    &batch,
                    type_name,
                    &unique_props,
                )?;
            }
            let active_branch = self.active_branch().map(str::to_string);
            let table_key = format!("edge:{}", type_name);
            let (state, full_path, table_branch) = self
                .append_batch_staged(&table_key, staging, schema, batch)
                .await?;

            crate::loader::validate_edge_cardinality(
                self,
                active_branch.as_deref(),
                type_name,
                state.version,
                table_branch.as_deref(),
            )
            .await?;

            staging.record(table_key, full_path, table_branch, &state);

            self.invalidate_graph_index().await;

            Ok(MutationResult {
                affected_nodes: 0,
                affected_edges: 1,
            })
        } else {
            Err(OmniError::manifest(format!("unknown type '{}'", type_name)))
        }
    }

    /// Append a batch to a sub-table within an in-flight mutation, routing
    /// through `MutationStaging` so subsequent ops in the same query see the
    /// write before publish. Returns the new state, the full path, and the
    /// table branch.
    async fn append_batch_staged(
        &self,
        table_key: &str,
        staging: &mut MutationStaging,
        _schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(crate::table_store::TableState, String, Option<String>)> {
        let (mut ds, full_path, table_branch) =
            open_for_mutation_in_query(self, staging, table_key).await?;
        let state = self
            .table_store()
            .append_batch(&full_path, &mut ds, batch)
            .await?;
        Ok((state, full_path, table_branch))
    }

    /// Upsert a batch into a sub-table using merge_insert keyed by "id",
    /// routing through `MutationStaging`.
    async fn upsert_batch_staged(
        &self,
        table_key: &str,
        staging: &mut MutationStaging,
        _schema: SchemaRef,
        batch: RecordBatch,
    ) -> Result<(crate::table_store::TableState, String, Option<String>)> {
        let (ds, full_path, table_branch) =
            open_for_mutation_in_query(self, staging, table_key).await?;
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
        Ok((state, full_path, table_branch))
    }

    async fn execute_update(
        &mut self,
        type_name: &str,
        assignments: &[IRAssignment],
        predicate: &IRMutationPredicate,
        params: &ParamMap,
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
        let (ds, full_path, table_branch) =
            open_for_mutation_in_query(self, staging, &table_key).await?;
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

        staging.record(table_key, full_path, table_branch, &update_state);

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
        staging: &mut MutationStaging,
    ) -> Result<MutationResult> {
        let is_node = self.catalog().node_types.contains_key(type_name);
        if is_node {
            self.execute_delete_node(type_name, predicate, params, staging)
                .await
        } else {
            self.execute_delete_edge(type_name, predicate, params, staging)
                .await
        }
    }

    async fn execute_delete_node(
        &mut self,
        type_name: &str,
        predicate: &IRMutationPredicate,
        params: &ParamMap,
        staging: &mut MutationStaging,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, false)?;

        let table_key = format!("node:{}", type_name);
        let (ds, full_path, table_branch) =
            open_for_mutation_in_query(self, staging, &table_key).await?;
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

        staging.record(
            table_key.clone(),
            full_path.clone(),
            table_branch.clone(),
            &delete_state,
        );

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
                open_for_mutation_in_query(self, staging, &edge_table_key).await?;

            let edge_delete = self
                .table_store()
                .delete_where(&edge_full_path, &mut edge_ds, &cascade_filter)
                .await?;

            affected_edges += edge_delete.deleted_rows;

            if edge_delete.deleted_rows > 0 {
                staging.record(
                    edge_table_key,
                    edge_full_path,
                    edge_table_branch,
                    &edge_delete,
                );
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
        staging: &mut MutationStaging,
    ) -> Result<MutationResult> {
        let pred_sql = predicate_to_sql(predicate, params, true)?;

        let table_key = format!("edge:{}", type_name);
        let (mut ds, full_path, table_branch) =
            open_for_mutation_in_query(self, staging, &table_key).await?;

        let delete_state = self
            .table_store()
            .delete_where(&full_path, &mut ds, &pred_sql)
            .await?;
        let affected = delete_state.deleted_rows;

        if affected > 0 {
            staging.record(table_key, full_path, table_branch, &delete_state);
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
