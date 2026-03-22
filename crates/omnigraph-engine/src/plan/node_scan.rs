use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, FixedSizeListArray, RecordBatch};
use arrow_schema::{DataType, Field, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use lance::Dataset;
use tracing::debug;

use crate::plan::bindings::split_flat_binding_col;
use crate::plan::literal_utils;
use crate::query::ast::{CompOp, Literal};
use crate::store::lance_io::logical_node_field_to_lance;
use crate::store::runtime::DatabaseRuntime;

#[derive(Debug, Clone)]
pub(crate) struct NodeScanPredicate {
    pub(crate) property: String,
    pub(crate) op: CompOp,
    pub(crate) literal: Literal,
    pub(crate) index_eligible: bool,
}

/// Physical execution plan that scans all nodes of a type, outputting flat binding columns.
#[derive(Debug)]
pub(crate) struct NodeScanExec {
    type_name: String,
    variable_name: String,
    output_schema: SchemaRef,
    pushdown_filters: Vec<NodeScanPredicate>,
    limit: Option<usize>,
    runtime: Arc<DatabaseRuntime>,
    properties: PlanProperties,
}

impl NodeScanExec {
    pub(crate) fn new(
        type_name: String,
        variable_name: String,
        output_schema: SchemaRef,
        pushdown_filters: Vec<NodeScanPredicate>,
        limit: Option<usize>,
        runtime: Arc<DatabaseRuntime>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion_physical_plan::execution_plan::EmissionType::Incremental,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            type_name,
            variable_name,
            output_schema,
            pushdown_filters,
            limit,
            runtime,
            properties,
        }
    }

    fn literal_to_array(literal: &Literal, num_rows: usize) -> Result<ArrayRef> {
        literal_utils::literal_to_array(literal, num_rows).map_err(DataFusionError::Execution)
    }

    fn compare_arrays(left: &ArrayRef, right: &ArrayRef, op: CompOp) -> Result<BooleanArray> {
        use arrow_ord::cmp;

        if op == CompOp::Contains {
            return literal_utils::compare_list_membership(left, right)
                .map_err(datafusion_common::DataFusionError::Execution);
        }

        match op {
            CompOp::Eq => cmp::eq(left, right),
            CompOp::Ne => cmp::neq(left, right),
            CompOp::Gt => cmp::gt(left, right),
            CompOp::Lt => cmp::lt(left, right),
            CompOp::Ge => cmp::gt_eq(left, right),
            CompOp::Le => cmp::lt_eq(left, right),
            CompOp::Contains => unreachable!("handled above"),
        }
        .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))
    }

    fn apply_pushdown_filters_with_predicates(
        pushdown_filters: &[NodeScanPredicate],
        input: &RecordBatch,
    ) -> Result<RecordBatch> {
        if pushdown_filters.is_empty() {
            return Ok(input.clone());
        }

        let mut current = input.clone();
        for predicate in pushdown_filters {
            let left = current
                .column_by_name(&predicate.property)
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(format!(
                        "column {} not found during node scan pushdown",
                        predicate.property
                    ))
                })?
                .clone();

            let right = Self::literal_to_array(&predicate.literal, current.num_rows())?;
            let right = if predicate.op != CompOp::Contains && left.data_type() != right.data_type()
            {
                arrow_cast::cast(&right, left.data_type())
                    .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?
            } else {
                right
            };

            let mask = Self::compare_arrays(&left, &right, predicate.op)?;
            current = arrow_select::filter::filter_record_batch(&current, &mask)
                .map_err(|e| datafusion_common::DataFusionError::Execution(e.to_string()))?;

            if current.num_rows() == 0 {
                break;
            }
        }

        Ok(current)
    }

    fn output_fields(output_schema: &SchemaRef) -> Vec<Field> {
        output_schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect()
    }

    fn align_column_to_field(col: &ArrayRef, field: &Field) -> Result<ArrayRef> {
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
                    DataFusionError::Execution(
                        "failed to downcast FixedSizeList column while aligning schema".to_string(),
                    )
                })?;
            let rebuilt = FixedSizeListArray::try_new(
                expected.clone(),
                *expected_dim,
                list.values().clone(),
                list.nulls().cloned(),
            )
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "failed to align FixedSizeList column '{}': {}",
                    field.name(),
                    e
                ))
            })?;
            return Ok(Arc::new(rebuilt) as ArrayRef);
        }

        arrow_cast::cast(col, field.data_type()).map_err(|e| {
            DataFusionError::Execution(format!(
                "failed to cast column '{}' from {:?} to {:?}: {}",
                field.name(),
                col.data_type(),
                field.data_type(),
                e
            ))
        })
    }

    fn materialize_batch_for_schema(
        output_schema: &SchemaRef,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let output_fields = Self::output_fields(output_schema);
        let mut output_columns = Vec::with_capacity(output_fields.len());
        for field in &output_fields {
            let property = split_flat_binding_col(field.name())
                .map(|(_, property)| property)
                .unwrap_or_else(|| field.name());
            let col = batch
                .column_by_name(property)
                .or_else(|| batch.column_by_name(logical_node_field_to_lance(property)))
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "column {} not found while materializing node scan output",
                        property
                    ))
                })?;
            output_columns.push(Self::align_column_to_field(col, field)?);
        }

        RecordBatch::try_new(output_schema.clone(), output_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn projected_column_names(&self) -> Result<Vec<String>> {
        Ok(Self::output_fields(&self.output_schema)
            .into_iter()
            .map(|f| {
                split_flat_binding_col(f.name())
                    .map(|(_, property)| logical_node_field_to_lance(property).to_string())
                    .unwrap_or_else(|| logical_node_field_to_lance(f.name()).to_string())
            })
            .collect())
    }

    fn literal_to_lance_sql(literal: &Literal) -> Option<String> {
        literal_utils::literal_to_lance_sql(literal)
    }

    fn lance_filter_sql(&self) -> std::result::Result<Option<String>, String> {
        if self.pushdown_filters.is_empty() {
            return Ok(None);
        }

        let mut clauses = Vec::with_capacity(self.pushdown_filters.len());
        for pred in &self.pushdown_filters {
            let op = match pred.op {
                CompOp::Eq => "=",
                CompOp::Ne => "!=",
                CompOp::Gt => ">",
                CompOp::Lt => "<",
                CompOp::Ge => ">=",
                CompOp::Le => "<=",
                CompOp::Contains => {
                    return Err(format!(
                        "membership predicate for property {} is not supported in Lance SQL pushdown",
                        pred.property
                    ));
                }
            };
            let lit = Self::literal_to_lance_sql(&pred.literal).ok_or_else(|| {
                format!(
                    "unsupported literal in Lance filter pushdown for property {}",
                    pred.property
                )
            })?;
            clauses.push(format!(
                "{} {} {}",
                logical_node_field_to_lance(&pred.property),
                op,
                lit
            ));
        }

        Ok(Some(clauses.join(" AND ")))
    }

    fn maybe_lance_dataset_path(&self) -> Option<PathBuf> {
        self.runtime
            .node_dataset_path(&self.type_name)
            .map(|p| p.to_path_buf())
            .filter(|p| p.exists())
    }

    fn has_index_eligible_pushdown(&self) -> bool {
        self.pushdown_filters.iter().any(|p| p.index_eligible)
    }
}

impl DisplayAs for NodeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NodeScanExec: ${}: {} (filters={}, index_eligible={})",
            self.variable_name,
            self.type_name,
            self.pushdown_filters.len(),
            self.has_index_eligible_pushdown()
        )
    }
}

impl ExecutionPlan for NodeScanExec {
    fn name(&self) -> &str {
        "NodeScanExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if let Some(dataset_path) = self.maybe_lance_dataset_path() {
            let output_schema = self.output_schema.clone();
            let projected_columns = self.projected_column_names()?;
            let pushdown_filters = self.pushdown_filters.clone();
            let (filter_sql, apply_filters_after_scan) = match self.lance_filter_sql() {
                Ok(sql) => (sql, false),
                Err(err) => {
                    debug!(
                        node_type = %self.type_name,
                        reason = %err,
                        "using Lance scan with in-process filtering: cannot convert predicate to Lance SQL"
                    );
                    (None, true)
                }
            };
            let filter_sql_for_debug = filter_sql.clone();
            let limit = self.limit;
            let pushdown_limit = if apply_filters_after_scan {
                None
            } else {
                limit.and_then(|v| i64::try_from(v).ok())
            };
            let dataset_version = self.runtime.node_dataset_version(&self.type_name);
            let stream = futures::stream::once(async move {
                let uri = dataset_path.to_string_lossy().to_string();
                let dataset = Dataset::open(&uri).await.map_err(|e| {
                    DataFusionError::Execution(format!("lance dataset open error: {}", e))
                })?;
                let dataset = if let Some(version) = dataset_version {
                    dataset.checkout_version(version).await.map_err(|e| {
                        DataFusionError::Execution(format!(
                            "lance dataset checkout version {} error: {}",
                            version, e
                        ))
                    })?
                } else {
                    dataset
                };
                let mut scanner = dataset.scan();
                scanner.project(&projected_columns).map_err(|e| {
                    DataFusionError::Execution(format!("lance projection pushdown error: {}", e))
                })?;
                if let Some(expr) = filter_sql.as_deref() {
                    scanner.filter(expr).map_err(|e| {
                        DataFusionError::Execution(format!("lance predicate pushdown error: {}", e))
                    })?;
                }
                if let Some(lim) = pushdown_limit {
                    scanner.limit(Some(lim), None).map_err(|e| {
                        DataFusionError::Execution(format!("lance limit pushdown error: {}", e))
                    })?;
                }

                let mut scan_stream = scanner.try_into_stream().await.map_err(|e| {
                    DataFusionError::Execution(format!("lance scanner stream error: {}", e))
                })?;
                let mut batches = Vec::new();
                while let Some(batch) = scan_stream.next().await {
                    batches.push(batch.map_err(|e| {
                        DataFusionError::Execution(format!("lance stream batch error: {}", e))
                    })?);
                }

                if batches.is_empty() {
                    return Ok(RecordBatch::new_empty(output_schema.clone()));
                }

                let merged = if batches.len() == 1 {
                    batches.remove(0)
                } else {
                    let projected_schema = batches[0].schema();
                    arrow_select::concat::concat_batches(&projected_schema, &batches).map_err(
                        |e| {
                            DataFusionError::Execution(format!(
                                "concat Lance scan batches error: {}",
                                e
                            ))
                        },
                    )?
                };

                let filtered = if apply_filters_after_scan {
                    NodeScanExec::apply_pushdown_filters_with_predicates(
                        &pushdown_filters,
                        &merged,
                    )?
                } else {
                    merged
                };

                if filtered.num_rows() == 0 {
                    return Ok(RecordBatch::new_empty(output_schema.clone()));
                }

                let filtered = if let Some(limit) = limit {
                    let rows_to_emit = filtered.num_rows().min(limit);
                    if rows_to_emit < filtered.num_rows() {
                        filtered.slice(0, rows_to_emit)
                    } else {
                        filtered
                    }
                } else {
                    filtered
                };

                NodeScanExec::materialize_batch_for_schema(&output_schema, &filtered)
            });

            debug!(
                node_type = %self.type_name,
                filter_sql = ?filter_sql_for_debug,
                limit = ?self.limit,
                dataset_version = ?dataset_version,
                index_eligible = self.has_index_eligible_pushdown(),
                apply_filters_after_scan,
                "using Lance-native node scan pushdown path"
            );

            return Ok(Box::pin(
                datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(
                    self.output_schema.clone(),
                    stream,
                ),
            ));
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::new_empty(self.output_schema.clone())],
            self.output_schema.clone(),
            None,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::NodeScanExec;
    use crate::plan::bindings::flat_binding_col;
    use crate::query::ast::Literal;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn literal_to_lance_sql_formats_temporal_literals_with_typed_casts() {
        let date_sql = NodeScanExec::literal_to_lance_sql(&Literal::Date("2026-02-14".to_string()));
        assert_eq!(date_sql.as_deref(), Some("CAST('2026-02-14' AS DATE)"));

        let dt_sql = NodeScanExec::literal_to_lance_sql(&Literal::DateTime(
            "2026-02-14T10:11:12Z".to_string(),
        ));
        assert_eq!(
            dt_sql.as_deref(),
            Some("CAST('2026-02-14 10:11:12.000' AS TIMESTAMP(3))")
        );
    }

    #[test]
    fn literal_to_lance_sql_returns_none_for_invalid_temporal_literals() {
        let bad_date = NodeScanExec::literal_to_lance_sql(&Literal::Date("not-a-date".to_string()));
        assert!(bad_date.is_none());

        let bad_dt =
            NodeScanExec::literal_to_lance_sql(&Literal::DateTime("not-a-datetime".to_string()));
        assert!(bad_dt.is_none());
    }

    #[test]
    fn materialize_batch_normalizes_fixed_size_list_child_nullability() {
        let mut emb_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        emb_builder.values().append_value(1.0);
        emb_builder.values().append_value(0.0);
        emb_builder.append(true);
        emb_builder.values().append_value(0.0);
        emb_builder.values().append_value(1.0);
        emb_builder.append(true);
        let embedding: ArrayRef = Arc::new(emb_builder.finish());

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            false,
        )]));
        let input_batch = RecordBatch::try_new(input_schema, vec![embedding]).unwrap();

        let output_schema = Arc::new(Schema::new(vec![Field::new(
            &flat_binding_col("n", "embedding"),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2),
            false,
        )]));

        let wrapped = NodeScanExec::materialize_batch_for_schema(&output_schema, &input_batch)
            .expect("expected schema alignment to succeed");
        assert_eq!(wrapped.num_rows(), 2);
        assert_eq!(
            wrapped.schema().field(0).data_type(),
            &DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 2)
        );
    }
}
