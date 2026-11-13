//! `UnionExec`: `from union(a, b)` as a pipeline breaker over its two arms,
//! a bag. Each arm is its own pipeline binding the union's reference
//! columns; every row of both leaves under the columns of those bindings
//! (`<binding>.<column>`), in the left arm's order, matched by name in the
//! right arm. The arms' other columns, the names an arm binds without
//! returning them, stay behind.

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::shape::{Breaker, work};
use super::{drain, external};
use crate::error::{OmniError, Result};

pub(crate) struct UnionExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    bindings: Vec<String>,
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl UnionExec {
    pub(crate) fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        bindings: Vec<String>,
    ) -> Result<Self> {
        let schema = union_schema(&left.schema(), &right.schema(), &bindings)?;
        Ok(Self {
            left,
            right,
            bindings,
            shape: Breaker::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// The columns of the union's bindings in the left arm, each nullable where
/// either arm's is; the right arm must carry each with the same type.
fn union_schema(left: &Schema, right: &Schema, bindings: &[String]) -> Result<SchemaRef> {
    let mut fields = Vec::new();
    for field in left.fields() {
        let Some((binding, _)) = field.name().split_once('.') else {
            continue;
        };
        if !bindings.iter().any(|name| name == binding) {
            continue;
        }
        let other = right.field_with_name(field.name()).map_err(|_| {
            OmniError::manifest_internal(format!(
                "the right arm of a union carries no column '{}'",
                field.name()
            ))
        })?;
        if other.data_type() != field.data_type() {
            return Err(OmniError::manifest_internal(format!(
                "the arms of a union carry '{}' as {:?} and {:?}",
                field.name(),
                field.data_type(),
                other.data_type()
            )));
        }
        fields.push(
            Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable() || other.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
        );
    }
    Ok(Arc::new(Schema::new(fields)))
}

impl fmt::Debug for UnionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnionExec")
            .field("bindings", &self.bindings)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for UnionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnionExec: {}", self.bindings.join(", "))
    }
}

impl ExecutionPlan for UnionExec {
    fn name(&self) -> &str {
        "UnionExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 2, "UnionExec has a left and a right arm");
        let right = children.pop().expect("right arm");
        let left = children.pop().expect("left arm");
        Ok(Arc::new(
            Self::try_new(left, right, self.bindings.clone()).map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "UnionExec has one partition");
        let schema: SchemaRef = self.schema();
        let left = self.left.execute(0, Arc::clone(&ctx))?;
        let right = Arc::clone(&self.right);
        let right_ctx = Arc::clone(&ctx);
        let memory = work(ctx, "UnionExec", &self.metrics)?;
        Ok(self
            .shape
            .stream(memory, &self.metrics, move |reservation| async move {
                let mut batches = drain(left, &reservation).await?;
                batches.extend(drain(right.execute(0, right_ctx)?, &reservation).await?);
                let parts = batches
                    .iter()
                    .filter(|batch| batch.num_rows() > 0)
                    .map(|batch| arm_rows(batch, &schema))
                    .collect::<Result<Vec<_>>>()
                    .map_err(external)?;
                if parts.is_empty() {
                    return Ok(RecordBatch::new_empty(schema));
                }
                reservation.concat(&schema, &parts)
            }))
    }
}

/// An arm's batch under the union's schema: its columns by name.
fn arm_rows(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "a union arm's batch carries no column '{}'",
                    field.name()
                ))
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)
}
