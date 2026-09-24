//! `AntiJoinMaskExec`: a correlated block (`not { … }`, `count { … } > 2`,
//! `sum($m.size) { … } > 100`) as a pipeline breaker over the outer wide
//! batch. The inner tree runs once as a lowered plan whose leaf,
//! `OuterReferenceExec`, reads the tagged outer batch from a slot this
//! operator fills before executing it; `SubqueryAggregate` then decides per
//! outer row. The shapes `Lowering::bulk_row_count` selects are answered by
//! the bulk CSR degree check instead (`bulk_anti_join_mask`).

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::ir::{ParamMap, SubqueryPredicate};
use omnigraph_compiler::types::Direction;

use super::subquery_aggregate::{RowCountPredicate, SubqueryAggregate, absorb_inner_batches};
use super::{GraphEnv, breaker_properties, breaker_stream, drain, drain_one, external, polled};
use crate::engine::graph::bulk_anti_join_mask;
use crate::error::{OmniError, Result};

/// The outer batch handed from `AntiJoinMaskExec` to the `OuterReferenceExec`
/// leaf of its inner plan, tagged with the outer row index.
#[derive(Debug, Default)]
pub(crate) struct OuterSlot(Mutex<Option<RecordBatch>>);

impl OuterSlot {
    fn fill(&self, batch: RecordBatch) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(batch);
    }

    fn take(&self) -> Option<RecordBatch> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

struct FilledSlot<'a>(&'a OuterSlot);

impl Drop for FilledSlot<'_> {
    fn drop(&mut self) {
        self.0.take();
    }
}

/// The leaf of an anti-join's inner plan: the tagged outer rows.
#[derive(Debug)]
pub(crate) struct OuterReferenceExec {
    outer_var: String,
    slot: Arc<OuterSlot>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl OuterReferenceExec {
    pub(crate) fn new(outer_var: String, slot: Arc<OuterSlot>, schema: SchemaRef) -> Self {
        Self {
            outer_var,
            slot,
            properties: breaker_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for OuterReferenceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OuterReferenceExec: ${}", self.outer_var)
    }
}

impl ExecutionPlan for OuterReferenceExec {
    fn name(&self) -> &str {
        "OuterReferenceExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty(), "OuterReferenceExec has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "OuterReferenceExec has one partition");
        let schema: SchemaRef = self.schema();
        let slot = Arc::clone(&self.slot);
        let stream = breaker_stream(
            "OuterReferenceExec",
            schema,
            &ctx,
            &self.metrics,
            move |_reservation| async move {
                slot.take().ok_or_else(|| {
                    external(OmniError::manifest_internal(
                        "OuterReference executed without its outer batch",
                    ))
                })
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}

pub(crate) struct AntiJoinMaskExec {
    outer: Arc<dyn ExecutionPlan>,
    inner: Arc<dyn ExecutionPlan>,
    outer_var: String,
    predicate: SubqueryPredicate,
    params: Arc<ParamMap>,
    tag_column: String,
    slot: Arc<OuterSlot>,
    /// The edge the bulk CSR check answers (`Lowering::bulk_row_count`).
    bulk: Option<(String, Direction)>,
    env: Arc<GraphEnv>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl AntiJoinMaskExec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        outer: Arc<dyn ExecutionPlan>,
        inner: Arc<dyn ExecutionPlan>,
        outer_var: String,
        predicate: SubqueryPredicate,
        params: Arc<ParamMap>,
        tag_column: String,
        slot: Arc<OuterSlot>,
        bulk: Option<(String, Direction)>,
        env: Arc<GraphEnv>,
    ) -> Self {
        let schema = outer.schema();
        Self {
            outer,
            inner,
            outer_var,
            predicate,
            params,
            tag_column,
            slot,
            bulk,
            env,
            properties: breaker_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

/// The synthetic index each outer row carries through an inner tree, under
/// a name the batch does not already hold: a nested negation tags a second
/// time, and `column_by_name` answers the first match.
pub(crate) fn fresh_tag_column(schema: &Schema) -> String {
    let mut n = 0usize;
    loop {
        let candidate = format!("__antijoin_outer_row_{n}");
        if schema.column_with_name(&candidate).is_none() {
            break candidate;
        }
        n += 1;
    }
}

/// The outer schema with the tag column, what `OuterReferenceExec` declares.
pub(crate) fn tagged_schema(outer: &Schema, tag_column: &str) -> SchemaRef {
    let mut fields: Vec<Field> = outer.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(tag_column, DataType::UInt32, false));
    Arc::new(Schema::new(fields))
}

fn tag_batch(wide: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let num_rows = wide.num_rows();
    let mut columns: Vec<ArrayRef> = wide.columns().to_vec();
    columns.push(Arc::new(UInt32Array::from_iter_values(0..num_rows as u32)));
    RecordBatch::try_new(schema, columns).map_err(OmniError::arrow_internal)
}

impl fmt::Debug for AntiJoinMaskExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AntiJoinMaskExec")
            .field("outer_var", &self.outer_var)
            .field("predicate", &self.predicate.to_string())
            .field("bulk", &self.bulk)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for AntiJoinMaskExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AntiJoinMaskExec: ${} {}",
            self.outer_var, self.predicate
        )?;
        if let Some((edge_type, direction)) = &self.bulk {
            write!(f, ", bulk={edge_type} {direction:?}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for AntiJoinMaskExec {
    fn name(&self) -> &str {
        "AntiJoinMaskExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
            "AntiJoinMaskExec has an outer and an inner child"
        );
        let inner = children.pop().expect("inner child");
        let outer = children.pop().expect("outer child");
        Ok(Arc::new(Self::new(
            outer,
            inner,
            self.outer_var.clone(),
            self.predicate.clone(),
            Arc::clone(&self.params),
            self.tag_column.clone(),
            Arc::clone(&self.slot),
            self.bulk.clone(),
            Arc::clone(&self.env),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "AntiJoinMaskExec has one partition");
        let schema: SchemaRef = self.schema();
        let outer = self.outer.execute(0, Arc::clone(&ctx))?;
        let inner = Arc::clone(&self.inner);
        let inner_ctx = Arc::clone(&ctx);
        let outer_var = self.outer_var.clone();
        let predicate = self.predicate.clone();
        let params = Arc::clone(&self.params);
        let tag_column = self.tag_column.clone();
        let tagged = tagged_schema(&schema, &tag_column);
        let slot = Arc::clone(&self.slot);
        let bulk = self.bulk.clone();
        let env = Arc::clone(&self.env);
        let declared = Arc::clone(&schema);
        let stream = breaker_stream(
            "AntiJoinMaskExec",
            schema,
            &ctx,
            &self.metrics,
            move |reservation| async move {
                let wide = drain_one(outer, &declared, &reservation).await?;
                reservation
                    .blocking(move |reservation| async move {
                        reservation.entries::<u32>(wide.num_rows())?;
                        let num_rows = wide.num_rows();
                        if num_rows == 0 {
                            return Ok(wide);
                        }
                        if let Some((edge_type, direction)) = &bulk
                            && let Some(row_count) =
                                RowCountPredicate::resolve(&predicate, &params).map_err(external)?
                        {
                            let gi = env.graph_index.get().await.map_err(external)?;
                            if let Some(mask) = bulk_anti_join_mask(
                                &wide,
                                edge_type,
                                *direction,
                                gi,
                                &env.catalog,
                                &outer_var,
                                &row_count,
                                &reservation,
                            )
                            .map_err(external)?
                            {
                                return reservation.filter(&wide, &mask);
                            }
                        }
                        let tagged_batch = tag_batch(&wide, tagged).map_err(external)?;
                        reservation.hold(&tagged_batch)?;
                        slot.fill(tagged_batch);
                        let filled = FilledSlot(&slot);
                        let result =
                            async { drain(inner.execute(0, inner_ctx)?, &reservation).await }.await;
                        drop(filled);
                        let inner_batches = result?;
                        reservation.entries::<u64>(num_rows)?;
                        if SubqueryAggregate::tracks_values(predicate.func) {
                            reservation
                                .entries::<super::subquery_aggregate::ValueAccumulator>(num_rows)?;
                        }
                        let mut aggregate = SubqueryAggregate::new(&predicate, &params, num_rows)
                            .map_err(external)?;
                        absorb_inner_batches(
                            &mut aggregate,
                            &inner_batches,
                            &tag_column,
                            predicate.arg.as_ref(),
                            &|batch, arg| crate::engine::expr::evaluate_expr(batch, arg, &params),
                        )
                        .map_err(external)?;
                        let keep = aggregate.keep_mask().map_err(external)?;
                        reservation.filter(&wide, &keep)
                    })
                    .await
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}
