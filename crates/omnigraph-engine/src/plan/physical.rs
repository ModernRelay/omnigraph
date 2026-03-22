use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use ahash::AHashSet;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, FixedSizeListArray, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result as DFResult;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use crate::error::{NanoError, Result};
use crate::ir::{IRAssignment, IRExpr, IRMutationPredicate, MutationIR, MutationOpIR, ParamMap};
use crate::plan::bindings::{binding_property_array, flat_binding_col, split_flat_binding_col};
use crate::query::ast::{CompOp, Literal};
use crate::store::csr::CsrIndex;
use crate::store::database::persist::{read_sparse_node_batch, resolve_sparse_node_id_by_name};
use crate::store::database::{Database, DatabaseWriteGuard, DeleteOp, DeletePredicate};
use crate::store::metadata::DatabaseMetadata;
use crate::store::runtime::{DatabaseRuntime, EdgeIndexPair, NodeLookup};
use crate::types::Direction;

/// Physical execution plan that expands from source nodes along an edge type,
/// producing new destination flat binding columns.
#[derive(Debug)]
pub(crate) struct ExpandExec {
    input: Arc<dyn ExecutionPlan>,
    src_var: String,
    dst_var: String,
    edge_type: String,
    direction: Direction,
    dst_type: String,
    min_hops: u32,
    max_hops: Option<u32>,
    output_schema: SchemaRef,
    runtime: Arc<DatabaseRuntime>,
    properties: PlanProperties,
}

#[derive(Debug, Clone)]
pub(crate) struct ExpandSpec {
    pub src_var: String,
    pub dst_var: String,
    pub edge_type: String,
    pub direction: Direction,
    pub dst_type: String,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
}

impl ExpandExec {
    pub(crate) fn new(
        input: Arc<dyn ExecutionPlan>,
        spec: ExpandSpec,
        runtime: Arc<DatabaseRuntime>,
    ) -> Self {
        let input_schema = input.schema();
        let ExpandSpec {
            src_var,
            dst_var,
            edge_type,
            direction,
            dst_type,
            min_hops,
            max_hops,
        } = spec;

        // Build output schema: input fields + new flat columns for dst
        let dst_node_type = &runtime.catalog().node_types[&dst_type];
        let dst_fields: Vec<Field> = dst_node_type
            .arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let field = f.as_ref();
                Field::new(
                    &flat_binding_col(&dst_var, field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();

        let mut output_fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        output_fields.extend(dst_fields);
        let output_schema = Arc::new(Schema::new(output_fields));

        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion_physical_plan::execution_plan::EmissionType::Incremental,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            input,
            src_var,
            dst_var,
            edge_type,
            direction,
            dst_type,
            min_hops,
            max_hops,
            output_schema,
            runtime,
            properties,
        }
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let bound = match self.max_hops {
            Some(max) if self.min_hops == 1 && max == 1 => "".to_string(),
            Some(max) => format!("{{{},{}}}", self.min_hops, max),
            None => format!("{{{},}}", self.min_hops),
        };
        write!(
            f,
            "ExpandExec: ${} --[{}{}]--> ${}",
            self.src_var, self.edge_type, bound, self.dst_var
        )
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ExpandExec::new(
            children[0].clone(),
            ExpandSpec {
                src_var: self.src_var.clone(),
                dst_var: self.dst_var.clone(),
                edge_type: self.edge_type.clone(),
                direction: self.direction,
                dst_type: self.dst_type.clone(),
                min_hops: self.min_hops,
                max_hops: self.max_hops,
            },
            self.runtime.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.clone();
        let schema = self.output_schema.clone();
        let edge_type = self.edge_type.clone();
        let direction = self.direction;
        let src_var = self.src_var.clone();
        let dst_var = self.dst_var.clone();
        let dst_type = self.dst_type.clone();
        let min_hops = self.min_hops;
        let max_hops = self.max_hops;
        let runtime = self.runtime.clone();

        let stream = futures::stream::once(async move {
            use datafusion_physical_plan::common::collect;
            let batches = collect(input.execute(partition, context)?).await?;

            // Create an ExpandExec with the real input plan for correct output_schema
            let expand = ExpandExec {
                input: Arc::new(datafusion_physical_plan::empty::EmptyExec::new(Arc::new(
                    Schema::new(Vec::<Field>::new()),
                ))),
                src_var,
                dst_var,
                edge_type,
                direction,
                dst_type,
                min_hops,
                max_hops,
                output_schema: schema.clone(),
                runtime,
                properties: PlanProperties::new(
                    EquivalenceProperties::new(schema.clone()),
                    datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
                    datafusion_physical_plan::execution_plan::EmissionType::Incremental,
                    datafusion_physical_plan::execution_plan::Boundedness::Bounded,
                ),
            };
            let edge_indices = expand.resolve_edge_indices().await?;
            let dst_lookup = expand.resolve_destination_lookup().await?;

            let mut result_columns: Vec<Vec<ArrayRef>> = Vec::new();
            let mut total_rows = 0usize;

            for batch in &batches {
                let expanded =
                    expand.expand_batch(batch, edge_indices.as_ref(), dst_lookup.as_deref())?;
                if expanded.num_rows() > 0 {
                    if result_columns.is_empty() {
                        result_columns.resize(expanded.num_columns(), Vec::new());
                    }
                    total_rows += expanded.num_rows();
                    for (i, col) in expanded.columns().iter().enumerate() {
                        result_columns[i].push(col.clone());
                    }
                }
            }

            if total_rows == 0 {
                return Ok(RecordBatch::new_empty(schema));
            }

            let mut final_cols = Vec::new();
            for col_arrays in &result_columns {
                let refs: Vec<&dyn arrow_array::Array> =
                    col_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated = arrow_select::concat::concat(&refs)?;
                final_cols.push(concatenated);
            }

            RecordBatch::try_new(schema, final_cols)
                .map_err(|e| datafusion_common::DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(
            datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                self.output_schema.clone(),
                stream,
            ),
        ))
    }
}

impl ExpandExec {
    async fn resolve_edge_indices(&self) -> DFResult<Arc<EdgeIndexPair>> {
        if let (Some(dataset_path), Some(dataset_version)) = (
            self.runtime.edge_dataset_path(&self.edge_type),
            self.runtime.edge_dataset_version(&self.edge_type),
        ) {
            return self
                .runtime
                .edge_index_cache()
                .get_or_build(
                    &self.edge_type,
                    dataset_path,
                    dataset_version,
                    self.runtime.next_node_id(),
                )
                .await
                .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()));
        }
        Err(datafusion_common::DataFusionError::Execution(format!(
            "edge type {} has no dataset locator",
            self.edge_type
        )))
    }

    async fn resolve_destination_lookup(&self) -> DFResult<Option<Arc<NodeLookup>>> {
        self.runtime
            .load_node_lookup(&self.dst_type)
            .await
            .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))
    }

    fn output_binding_fields(output_schema: &SchemaRef, variable: &str) -> Vec<Field> {
        output_schema
            .fields()
            .iter()
            .filter(|field| {
                split_flat_binding_col(field.name())
                    .map(|(candidate, _)| candidate == variable)
                    .unwrap_or(false)
            })
            .map(|field| field.as_ref().clone())
            .collect()
    }

    fn align_column_to_field(col: &ArrayRef, field: &Field) -> DFResult<ArrayRef> {
        if col.data_type() == field.data_type() {
            return Ok(col.clone());
        }

        if let (
            DataType::FixedSizeList(actual, actual_dim),
            DataType::FixedSizeList(expected, expected_dim),
        ) = (col.data_type(), field.data_type())
            && actual_dim == expected_dim
            && actual.data_type() == expected.data_type()
        {
            let list = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "failed to downcast FixedSizeList column while aligning expand output"
                            .to_string(),
                    )
                })?;
            let rebuilt = FixedSizeListArray::try_new(
                expected.clone(),
                *expected_dim,
                list.values().clone(),
                list.nulls().cloned(),
            )
            .map_err(|e| {
                datafusion_common::DataFusionError::Execution(format!(
                    "failed to align FixedSizeList column '{}': {}",
                    field.name(),
                    e
                ))
            })?;
            return Ok(Arc::new(rebuilt) as ArrayRef);
        }

        arrow_cast::cast(col, field.data_type()).map_err(|e| {
            datafusion_common::DataFusionError::Execution(format!(
                "failed to cast expanded column '{}' from {:?} to {:?}: {}",
                field.name(),
                col.data_type(),
                field.data_type(),
                e
            ))
        })
    }

    fn expand_batch(
        &self,
        input: &RecordBatch,
        edge_indices: &EdgeIndexPair,
        dst_lookup: Option<&NodeLookup>,
    ) -> DFResult<RecordBatch> {
        let csr = match self.direction {
            Direction::Out => edge_indices.csr.as_ref(),
            Direction::In => edge_indices.csc.as_ref(),
        };
        let src_id_col = binding_property_array(input, &self.src_var, "id")
            .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?;
        let id_array = src_id_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Execution("id field is not UInt64".to_string())
            })?;

        // Get destination node data
        let dst_lookup = match dst_lookup {
            Some(lookup) => lookup,
            None => {
                return Ok(RecordBatch::new_empty(self.output_schema.clone()));
            }
        };
        let dst_batch = &dst_lookup.batch;

        // For each input row, perform bounded/unbounded expansion.
        let mut output_row_indices: Vec<(usize, u64)> = Vec::new(); // (input_row, dst_node_id)
        for row in 0..input.num_rows() {
            let src_id = id_array.value(row);
            let expanded_ids =
                collect_traversal_neighbors(csr, src_id, self.min_hops, self.max_hops);
            for dst_id in expanded_ids {
                output_row_indices.push((row, dst_id));
            }
        }

        if output_row_indices.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // Resolve destination rows and fail fast if edge references a missing destination.
        let mut valid_indices: Vec<(usize, usize)> = Vec::with_capacity(output_row_indices.len());
        for (src_idx, dst_id) in &output_row_indices {
            let dst_row = dst_lookup.id_to_row.get(dst_id).copied().ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(format!(
                    "edge {} references missing destination node id {}",
                    self.edge_type, dst_id
                ))
            })?;
            valid_indices.push((*src_idx, dst_row));
        }

        if valid_indices.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // Build output columns
        let mut output_columns: Vec<ArrayRef> = Vec::new();

        // Replicate input columns based on expansion
        let input_row_indices: Vec<usize> = valid_indices.iter().map(|(r, _)| *r).collect();
        for col_idx in 0..input.num_columns() {
            let col = input.column(col_idx);
            let taken = take_rows(col, &input_row_indices)?;
            output_columns.push(taken);
        }

        let dst_schema = dst_batch.schema();
        let dst_output_fields = Self::output_binding_fields(&self.output_schema, &self.dst_var);

        let dst_row_indices: Vec<usize> = valid_indices.iter().map(|(_, dr)| *dr).collect();
        for field in &dst_output_fields {
            let property = split_flat_binding_col(field.name())
                .map(|(_, property)| property)
                .unwrap_or_else(|| field.name());
            let field_idx = dst_schema
                .index_of(field.name())
                .map_err(|e| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "destination column '{}' missing in type '{}': {}",
                        property, self.dst_type, e
                    ))
                })
                .or_else(|_| {
                    dst_schema.index_of(property).map_err(|e| {
                        datafusion_common::DataFusionError::Execution(format!(
                            "destination column '{}' missing in type '{}': {}",
                            property, self.dst_type, e
                        ))
                    })
                })?;
            let dst_col = dst_batch.column(field_idx);
            let taken = take_rows(dst_col, &dst_row_indices)?;
            let aligned = Self::align_column_to_field(&taken, field)?;
            output_columns.push(aligned);
        }

        RecordBatch::try_new(self.output_schema.clone(), output_columns)
            .map_err(|e| datafusion_common::DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Take specific rows from an array by index.
fn take_rows(array: &ArrayRef, indices: &[usize]) -> DFResult<ArrayRef> {
    let idx_array = UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
    let taken = arrow_select::take::take(array.as_ref(), &idx_array, None)?;
    Ok(taken)
}

fn collect_traversal_neighbors(
    csr: &CsrIndex,
    src_id: u64,
    min_hops: u32,
    max_hops: Option<u32>,
) -> Vec<u64> {
    match max_hops {
        Some(max_hops) => collect_bounded_neighbors(csr, src_id, min_hops, max_hops),
        None => collect_unbounded_neighbors(csr, src_id, min_hops),
    }
}

fn collect_bounded_neighbors(
    csr: &CsrIndex,
    src_id: u64,
    min_hops: u32,
    max_hops: u32,
) -> Vec<u64> {
    if max_hops < min_hops {
        return Vec::new();
    }

    let mut frontier = vec![src_id];
    let mut emitted = AHashSet::new();
    let mut out = Vec::new();

    for depth in 1..=max_hops {
        if frontier.is_empty() {
            break;
        }

        let mut next = Vec::new();
        let mut next_seen = AHashSet::new();
        for node in &frontier {
            for &neighbor in csr.neighbors(*node) {
                if next_seen.insert(neighbor) {
                    next.push(neighbor);
                }
            }
        }

        if next.is_empty() {
            break;
        }

        if depth >= min_hops {
            for node in &next {
                if emitted.insert(*node) {
                    out.push(*node);
                }
            }
        }
        frontier = next;
    }

    out
}

fn collect_unbounded_neighbors(csr: &CsrIndex, src_id: u64, min_hops: u32) -> Vec<u64> {
    let mut frontier = vec![src_id];
    let mut seen = AHashSet::new();
    let mut out = Vec::new();
    let mut depth = 0u32;

    while !frontier.is_empty() {
        depth = depth.saturating_add(1);

        let mut next = Vec::new();
        for node in &frontier {
            for &neighbor in csr.neighbors(*node) {
                if seen.insert(neighbor) {
                    next.push(neighbor);
                }
            }
        }

        if next.is_empty() {
            break;
        }

        if depth >= min_hops {
            out.extend(next.iter().copied());
        }
        frontier = next;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::ExpandExec;
    use arrow_array::ArrayRef;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn align_column_to_field_rebuilds_fixed_size_list_child_nullability() {
        let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        emb_builder.values().append_value(1.0);
        emb_builder.values().append_value(2.0);
        emb_builder.append(true);
        let actual = Arc::new(emb_builder.finish()) as ArrayRef;

        let expected = Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
            false,
        );

        let aligned = ExpandExec::align_column_to_field(&actual, &expected)
            .expect("align vector child nullability");
        assert_eq!(aligned.data_type(), expected.data_type());
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MutationExecResult {
    pub affected_nodes: usize,
    pub affected_edges: usize,
}

pub(crate) async fn execute_mutation(
    ir: &MutationIR,
    db: &Database,
    params: &ParamMap,
    writer: &mut DatabaseWriteGuard<'_>,
) -> Result<MutationExecResult> {
    match &ir.op {
        MutationOpIR::Insert {
            type_name,
            assignments,
        } => execute_insert_mutation(db, type_name, assignments, params, writer).await,
        MutationOpIR::Update {
            type_name,
            assignments,
            predicate,
        } => execute_update_mutation(db, type_name, assignments, predicate, params, writer).await,
        MutationOpIR::Delete {
            type_name,
            predicate,
        } => execute_delete_mutation(db, type_name, predicate, params, writer).await,
    }
}

async fn execute_insert_mutation(
    db: &Database,
    type_name: &str,
    assignments: &[IRAssignment],
    params: &ParamMap,
    writer: &mut DatabaseWriteGuard<'_>,
) -> Result<MutationExecResult> {
    let is_node_type = db.catalog.node_types.contains_key(type_name);
    let is_edge_type = db.catalog.edge_types.contains_key(type_name);
    if is_edge_type {
        return execute_insert_edge_mutation(db, type_name, assignments, params, writer).await;
    }
    if !is_node_type {
        return Err(NanoError::Execution(format!(
            "unknown mutation target type `{}`",
            type_name
        )));
    }

    let mut data = serde_json::Map::new();
    for assignment in assignments {
        let lit = resolve_mutation_literal(&assignment.value, params)?;
        data.insert(assignment.property.clone(), literal_to_json(&lit)?);
    }
    let line = serde_json::json!({
        "type": type_name,
        "data": data
    })
    .to_string();
    db.apply_append_mutation_locked(&line, "mutation:insert_node", writer)
        .await?;
    Ok(MutationExecResult {
        affected_nodes: 1,
        affected_edges: 0,
    })
}

async fn execute_insert_edge_mutation(
    db: &Database,
    type_name: &str,
    assignments: &[IRAssignment],
    params: &ParamMap,
    writer: &mut DatabaseWriteGuard<'_>,
) -> Result<MutationExecResult> {
    let mut from_name: Option<String> = None;
    let mut to_name: Option<String> = None;
    let mut data = serde_json::Map::new();

    for assignment in assignments {
        let lit = resolve_mutation_literal(&assignment.value, params)?;
        match assignment.property.as_str() {
            "from" => {
                from_name = Some(literal_to_endpoint_name(&lit, "from")?);
            }
            "to" => {
                to_name = Some(literal_to_endpoint_name(&lit, "to")?);
            }
            _ => {
                data.insert(assignment.property.clone(), literal_to_json(&lit)?);
            }
        }
    }

    let from = from_name.ok_or_else(|| {
        NanoError::Execution(format!(
            "edge insert for `{}` requires endpoint property `from`",
            type_name
        ))
    })?;
    let to = to_name.ok_or_else(|| {
        NanoError::Execution(format!(
            "edge insert for `{}` requires endpoint property `to`",
            type_name
        ))
    })?;

    let line = serde_json::json!({
        "edge": type_name,
        "from": from,
        "to": to,
        "data": data
    })
    .to_string();

    db.apply_append_mutation_locked(&line, "mutation:insert_edge", writer)
        .await?;
    Ok(MutationExecResult {
        affected_nodes: 0,
        affected_edges: 1,
    })
}

async fn execute_update_mutation(
    db: &Database,
    type_name: &str,
    assignments: &[IRAssignment],
    predicate: &IRMutationPredicate,
    params: &ParamMap,
    writer: &mut DatabaseWriteGuard<'_>,
) -> Result<MutationExecResult> {
    let key_prop = find_key_property(db, type_name).ok_or_else(|| {
        NanoError::Storage(format!(
            "update mutation requires @key on node type `{}` for identity-safe updates",
            type_name
        ))
    })?;
    if assignments.iter().any(|a| a.property == key_prop) {
        return Err(NanoError::Storage(format!(
            "update mutation cannot assign @key property `{}`",
            key_prop
        )));
    }

    let metadata = DatabaseMetadata::open(db.path())?;
    let target_batch = match read_sparse_node_batch(&metadata, type_name).await? {
        Some(batch) => batch,
        None => return Ok(MutationExecResult::default()),
    };
    let delete_pred = build_delete_predicate(predicate, params)?;
    let match_mask =
        crate::store::database::build_delete_mask_for_mutation(&target_batch, &delete_pred)?;

    let matched_rows: Vec<usize> = (0..target_batch.num_rows())
        .filter(|&row| !match_mask.is_null(row) && match_mask.value(row))
        .collect();
    if matched_rows.is_empty() {
        return Ok(MutationExecResult::default());
    }

    let mut assignment_values = HashMap::new();
    for assignment in assignments {
        let lit = resolve_mutation_literal(&assignment.value, params)?;
        assignment_values.insert(assignment.property.clone(), literal_to_json(&lit)?);
    }

    let schema = target_batch.schema();
    let prop_columns: Vec<(usize, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if idx == 0 {
                None
            } else {
                Some((idx, field.name().clone()))
            }
        })
        .collect();

    let mut payload_lines = Vec::with_capacity(matched_rows.len());
    for row in matched_rows.iter().copied() {
        let mut data_obj = serde_json::Map::new();
        for (idx, prop_name) in &prop_columns {
            let val = array_value_to_json(target_batch.column(*idx), row);
            data_obj.insert(prop_name.clone(), val);
        }
        for (prop, value) in &assignment_values {
            data_obj.insert(prop.clone(), value.clone());
        }

        payload_lines.push(
            serde_json::json!({
                "type": type_name,
                "data": data_obj
            })
            .to_string(),
        );
    }

    let payload = payload_lines.join("\n");
    db.apply_merge_mutation_locked(&payload, "mutation:update_node", writer)
        .await?;
    Ok(MutationExecResult {
        affected_nodes: matched_rows.len(),
        affected_edges: 0,
    })
}

async fn execute_delete_mutation(
    db: &Database,
    type_name: &str,
    predicate: &IRMutationPredicate,
    params: &ParamMap,
    writer: &mut DatabaseWriteGuard<'_>,
) -> Result<MutationExecResult> {
    if db.catalog.node_types.contains_key(type_name) {
        let delete_pred = build_delete_predicate(predicate, params)?;
        let result = db
            .delete_nodes_locked(type_name, &delete_pred, writer)
            .await?;
        return Ok(MutationExecResult {
            affected_nodes: result.deleted_nodes,
            affected_edges: result.deleted_edges,
        });
    }

    if db.catalog.edge_types.contains_key(type_name) {
        return execute_delete_edge_mutation(db, type_name, predicate, params, writer).await;
    }

    Err(NanoError::Execution(format!(
        "unknown mutation target type `{}`",
        type_name
    )))
}

async fn execute_delete_edge_mutation(
    db: &Database,
    type_name: &str,
    predicate: &IRMutationPredicate,
    params: &ParamMap,
    writer: &mut DatabaseWriteGuard<'_>,
) -> Result<MutationExecResult> {
    let edge_type = db
        .catalog
        .edge_types
        .get(type_name)
        .ok_or_else(|| NanoError::Execution(format!("unknown edge type `{}`", type_name)))?;
    let src_type = edge_type.from_type.clone();
    let dst_type = edge_type.to_type.clone();

    let delete_pred = build_delete_predicate(predicate, params)?;
    let mapped_pred = match predicate.property.as_str() {
        "from" => {
            let endpoint = resolve_mutation_literal(&predicate.value, params)?;
            let endpoint_name = literal_to_endpoint_name(&endpoint, "from")?;
            let metadata = DatabaseMetadata::open(db.path())?;
            let src_id =
                resolve_sparse_node_id_by_name(&metadata, &src_type, &endpoint_name).await?;
            DeletePredicate {
                property: "src".to_string(),
                op: delete_pred.op,
                value: src_id.to_string(),
            }
        }
        "to" => {
            let endpoint = resolve_mutation_literal(&predicate.value, params)?;
            let endpoint_name = literal_to_endpoint_name(&endpoint, "to")?;
            let metadata = DatabaseMetadata::open(db.path())?;
            let dst_id =
                resolve_sparse_node_id_by_name(&metadata, &dst_type, &endpoint_name).await?;
            DeletePredicate {
                property: "dst".to_string(),
                op: delete_pred.op,
                value: dst_id.to_string(),
            }
        }
        _ => delete_pred,
    };

    let result = db
        .delete_edges_locked(type_name, &mapped_pred, writer)
        .await?;
    Ok(MutationExecResult {
        affected_nodes: 0,
        affected_edges: result.deleted_edges,
    })
}

fn build_delete_predicate(
    predicate: &IRMutationPredicate,
    params: &ParamMap,
) -> Result<DeletePredicate> {
    let lit = resolve_mutation_literal(&predicate.value, params)?;
    Ok(DeletePredicate {
        property: predicate.property.clone(),
        op: comp_op_to_delete_op(predicate.op)?,
        value: literal_to_predicate_string(&lit)?,
    })
}

fn resolve_mutation_literal(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    match expr {
        IRExpr::Literal(l) => Ok(l.clone()),
        IRExpr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            NanoError::Execution(format!("missing required mutation parameter `${}`", name))
        }),
        other => Err(NanoError::Execution(format!(
            "mutation expression must be literal or parameter, got {:?}",
            other
        ))),
    }
}

fn comp_op_to_delete_op(op: CompOp) -> Result<DeleteOp> {
    match op {
        CompOp::Eq => Ok(DeleteOp::Eq),
        CompOp::Ne => Ok(DeleteOp::Ne),
        CompOp::Gt => Ok(DeleteOp::Gt),
        CompOp::Lt => Ok(DeleteOp::Lt),
        CompOp::Ge => Ok(DeleteOp::Ge),
        CompOp::Le => Ok(DeleteOp::Le),
        CompOp::Contains => Err(NanoError::Execution(
            "membership predicates are not supported for delete operations".to_string(),
        )),
    }
}

fn literal_to_json(lit: &Literal) -> Result<serde_json::Value> {
    match lit {
        Literal::String(s) => Ok(serde_json::Value::String(s.clone())),
        Literal::Integer(i) => Ok(serde_json::Value::Number((*i).into())),
        Literal::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| NanoError::Execution(format!("invalid float literal {}", f))),
        Literal::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Literal::Date(s) => Ok(serde_json::Value::String(s.clone())),
        Literal::DateTime(s) => Ok(serde_json::Value::String(s.clone())),
        Literal::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(literal_to_json(item)?);
            }
            Ok(serde_json::Value::Array(out))
        }
    }
}

fn literal_to_predicate_string(lit: &Literal) -> Result<String> {
    match lit {
        Literal::String(s) => Ok(s.clone()),
        Literal::Integer(i) => Ok(i.to_string()),
        Literal::Float(f) => {
            if !f.is_finite() {
                return Err(NanoError::Execution(format!("invalid float literal {}", f)));
            }
            Ok(f.to_string())
        }
        Literal::Bool(b) => Ok(b.to_string()),
        Literal::Date(s) => Ok(s.clone()),
        Literal::DateTime(s) => Ok(s.clone()),
        Literal::List(_) => Err(NanoError::Execution(
            "list literal is not supported in mutation predicates".to_string(),
        )),
    }
}

fn literal_to_endpoint_name(lit: &Literal, endpoint: &str) -> Result<String> {
    match lit {
        Literal::String(s) => Ok(s.clone()),
        _ => Err(NanoError::Execution(format!(
            "edge endpoint `{}` must be a String literal or String parameter",
            endpoint
        ))),
    }
}

fn find_key_property(db: &Database, type_name: &str) -> Option<String> {
    db.schema_ir
        .node_types()
        .find(|n| n.name == type_name)
        .and_then(|n| n.properties.iter().find(|p| p.key).map(|p| p.name.clone()))
}

fn array_value_to_json(array: &ArrayRef, row: usize) -> serde_json::Value {
    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(row).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as u64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|a| {
                serde_json::Number::from_f64(a.value(row) as f64).map(serde_json::Value::Number)
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(row)).map(serde_json::Value::Number))
            .unwrap_or(serde_json::Value::Null),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| {
                let days = a.value(row);
                arrow_array::temporal_conversions::date32_to_datetime(days)
                    .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d").to_string()))
                    .unwrap_or_else(|| serde_json::Value::Number((days as i64).into()))
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| {
                let ms = a.value(row);
                arrow_array::temporal_conversions::date64_to_datetime(ms)
                    .map(|dt| {
                        serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
                    })
                    .unwrap_or_else(|| serde_json::Value::Number(ms.into()))
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|a| {
                let values = a.value(row);
                serde_json::Value::Array(
                    (0..values.len())
                        .map(|idx| array_value_to_json(&values, idx))
                        .collect(),
                )
            })
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}
