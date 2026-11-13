//! `OptionalJoinExec`: `optional { p }` as a pipeline breaker over the outer
//! wide batch. Its inner tree runs once as a lowered plan whose leaf,
//! `OuterReferenceExec`, reads the outer batch tagged with each row's index
//! from the slot this operator fills before executing it, as a correlated
//! block's does. Every inner row is a match of the outer row its tag names
//! and leaves under the operator's schema without the tag; an outer row no
//! inner row names leaves once, every column the inner tree adds null on it
//! (the core's `Union(Correlate(R, p), Project(Filter(R, ¬ exists p),
//! nulls))`).

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, new_null_array};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::anti_join::{FilledSlot, OuterSlot, tag_batch, tagged_schema};
use super::shape::{Breaker, work};
use super::{drain, drain_one, external};
use crate::error::{OmniError, Result};

pub(crate) struct OptionalJoinExec {
    outer: Arc<dyn ExecutionPlan>,
    inner: Arc<dyn ExecutionPlan>,
    outer_var: String,
    tag_column: String,
    slot: Arc<OuterSlot>,
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl OptionalJoinExec {
    pub(crate) fn try_new(
        outer: Arc<dyn ExecutionPlan>,
        inner: Arc<dyn ExecutionPlan>,
        outer_var: String,
        tag_column: String,
        slot: Arc<OuterSlot>,
    ) -> Result<Self> {
        let schema = joined_schema(&outer.schema(), &inner.schema(), &tag_column)?;
        Ok(Self {
            outer,
            inner,
            outer_var,
            tag_column,
            slot,
            shape: Breaker::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// The inner tree's columns without the tag: the outer columns it carries
/// through as they are, and every column it adds nullable, since an outer
/// row with no match holds null there. The inner tree carries every outer
/// column (its leaf is the whole outer batch).
fn joined_schema(outer: &Schema, inner: &Schema, tag_column: &str) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(inner.fields().len());
    for field in inner.fields() {
        if field.name() == tag_column {
            continue;
        }
        fields.push(match outer.field_with_name(field.name()) {
            Ok(outer_field) => outer_field.clone(),
            Err(_) => Field::new(field.name(), field.data_type().clone(), true)
                .with_metadata(field.metadata().clone()),
        });
    }
    if let Some(missing) = outer
        .fields()
        .iter()
        .find(|field| !fields.iter().any(|kept| kept.name() == field.name()))
    {
        return Err(OmniError::manifest_internal(format!(
            "the inner tree of an optional drops the outer column '{}'",
            missing.name()
        )));
    }
    Ok(Arc::new(Schema::new(fields)))
}

impl fmt::Debug for OptionalJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OptionalJoinExec")
            .field("outer_var", &self.outer_var)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for OptionalJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OptionalJoinExec: ${}", self.outer_var)
    }
}

impl ExecutionPlan for OptionalJoinExec {
    fn name(&self) -> &str {
        "OptionalJoinExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.outer, &self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(
            children.len(),
            2,
            "OptionalJoinExec has an outer and an inner child"
        );
        let inner = children.pop().expect("inner child");
        let outer = children.pop().expect("outer child");
        Ok(Arc::new(
            Self::try_new(
                outer,
                inner,
                self.outer_var.clone(),
                self.tag_column.clone(),
                Arc::clone(&self.slot),
            )
            .map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "OptionalJoinExec has one partition");
        let schema: SchemaRef = self.schema();
        let outer = self.outer.execute(0, Arc::clone(&ctx))?;
        let outer_schema = self.outer.schema();
        let inner = Arc::clone(&self.inner);
        let inner_ctx = Arc::clone(&ctx);
        let tag_column = self.tag_column.clone();
        let tagged = tagged_schema(&outer_schema, &tag_column);
        let slot = Arc::clone(&self.slot);
        let memory = work(ctx, "OptionalJoinExec", &self.metrics)?;
        Ok(self
            .shape
            .stream(memory, &self.metrics, move |reservation| async move {
                let wide = drain_one(outer, &outer_schema, &reservation).await?;
                reservation
                    .blocking(move |reservation| async move {
                        let num_rows = wide.num_rows();
                        if num_rows == 0 {
                            return Ok(RecordBatch::new_empty(schema));
                        }
                        let tagged_batch = tag_batch(&wide, tagged).map_err(external)?;
                        reservation.hold(&tagged_batch)?;
                        slot.fill(tagged_batch);
                        let filled = FilledSlot(&slot);
                        let result =
                            async { drain(inner.execute(0, inner_ctx)?, &reservation).await }.await;
                        drop(filled);
                        let inner_batches = result?;
                        reservation.entries::<bool>(num_rows)?;
                        let mut matched = vec![false; num_rows];
                        let mut parts = Vec::with_capacity(inner_batches.len() + 1);
                        for batch in &inner_batches {
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            let tags = batch
                                .column_by_name(&tag_column)
                                .and_then(|tags| tags.as_any().downcast_ref::<UInt32Array>())
                                .ok_or_else(|| {
                                    external(OmniError::manifest_internal(format!(
                                        "an optional's inner batch carries no '{tag_column}' tag"
                                    )))
                                })?;
                            for tag in tags.iter().flatten() {
                                if let Some(seen) = matched.get_mut(tag as usize) {
                                    *seen = true;
                                }
                            }
                            parts.push(matches(batch, &schema).map_err(external)?);
                        }
                        let unmatched = UInt32Array::from_iter_values(
                            (0..num_rows as u32).filter(|row| !matched[*row as usize]),
                        );
                        if !unmatched.is_empty() {
                            let kept = reservation.take(&wide, &unmatched)?;
                            parts.push(padded(&kept, &schema).map_err(external)?);
                        }
                        if parts.is_empty() {
                            return Ok(RecordBatch::new_empty(schema));
                        }
                        reservation.concat(&schema, &parts)
                    })
                    .await
            }))
    }
}

/// An inner batch under the operator's schema: its columns by name, the
/// tag left out.
fn matches(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "an optional's inner batch carries no column '{}'",
                    field.name()
                ))
            })
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)
}

/// The outer rows with no match under the operator's schema: each outer
/// column as it is, each column the inner tree adds null.
fn padded(outer: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let rows = outer.num_rows();
    let columns = schema
        .fields()
        .iter()
        .map(|field| match outer.column_by_name(field.name()) {
            Some(column) => Arc::clone(column),
            None => new_null_array(field.data_type(), rows),
        })
        .collect::<Vec<ArrayRef>>();
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)
}
