//! `AntiJoinMaskExec`: a correlated block (`not { … }`, `count { … } > 2`,
//! `sum($m.size) { … } > 100`) as a pipeline breaker over the outer wide
//! batch. Under the `Correlated` strategy the inner tree runs once as a
//! lowered plan whose leaf, `OuterReferenceExec`, reads the tagged outer batch
//! from a slot this operator fills before executing it; `SubqueryAggregate`
//! then decides per outer row. The shapes `Lowering::bulk_row_count` selects
//! are answered by the bulk CSR degree check instead (`bulk_anti_join_mask`).
//! Under `KeySet` the inner tree reads no outer rows and ends on the
//! correlation binding: its rows fold into one aggregate per outer key as
//! they stream, and each outer row takes its key's verdict (`key_set_mask`).

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{ParamMap, SubqueryPredicate};
use omnigraph_compiler::types::Direction;
use omnigraph_planner::{AggregateSpec, AntiJoinStrategy};

use super::memory::WorkMemory;
use super::shape::{Breaker, work};
use super::subquery_aggregate::{
    RowCountPredicate, SubqueryAggregate, ValueAccumulator, absorb_inner_batches,
};
use super::{drain, drain_one, external};
use crate::engine::graph::{GraphIndexHandle, bulk_anti_join_mask};
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
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl OuterReferenceExec {
    pub(crate) fn new(outer_var: String, slot: Arc<OuterSlot>, schema: SchemaRef) -> Self {
        Self {
            outer_var,
            slot,
            shape: Breaker::new(schema),
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
        self.shape.properties()
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
        let slot = Arc::clone(&self.slot);
        let memory = work(ctx, "OuterReferenceExec", &self.metrics)?;
        Ok(self
            .shape
            .stream(memory, &self.metrics, move |_reservation| async move {
                slot.take().ok_or_else(|| {
                    external(OmniError::manifest_internal(
                        "OuterReference executed without its outer batch",
                    ))
                })
            }))
    }
}

pub(crate) struct AntiJoinMaskExec {
    outer: Arc<dyn ExecutionPlan>,
    inner: Arc<dyn ExecutionPlan>,
    outer_var: String,
    predicate: SubqueryPredicate,
    aggregate: Option<AggregateSpec>,
    params: Arc<ParamMap>,
    tag_column: String,
    slot: Arc<OuterSlot>,
    strategy: AntiJoinStrategy,
    /// The edge the bulk CSR check answers (`Lowering::bulk_row_count`).
    bulk: Option<(String, Direction)>,
    catalog: Arc<Catalog>,
    graph_index: Arc<GraphIndexHandle>,
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl AntiJoinMaskExec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        outer: Arc<dyn ExecutionPlan>,
        inner: Arc<dyn ExecutionPlan>,
        outer_var: String,
        predicate: SubqueryPredicate,
        aggregate: Option<AggregateSpec>,
        params: Arc<ParamMap>,
        tag_column: String,
        slot: Arc<OuterSlot>,
        strategy: AntiJoinStrategy,
        bulk: Option<(String, Direction)>,
        catalog: Arc<Catalog>,
        graph_index: Arc<GraphIndexHandle>,
    ) -> Self {
        let schema = outer.schema();
        Self {
            outer,
            inner,
            outer_var,
            predicate,
            aggregate,
            params,
            tag_column,
            slot,
            strategy,
            bulk,
            catalog,
            graph_index,
            shape: Breaker::new(schema),
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
            .field("strategy", &self.strategy)
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
        if self.strategy == AntiJoinStrategy::KeySet {
            write!(f, ", key_set")?;
        }
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
            "AntiJoinMaskExec has an outer and an inner child"
        );
        let inner = children.pop().expect("inner child");
        let outer = children.pop().expect("outer child");
        Ok(Arc::new(Self::new(
            outer,
            inner,
            self.outer_var.clone(),
            self.predicate.clone(),
            self.aggregate,
            Arc::clone(&self.params),
            self.tag_column.clone(),
            Arc::clone(&self.slot),
            self.strategy,
            self.bulk.clone(),
            Arc::clone(&self.catalog),
            Arc::clone(&self.graph_index),
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
        let aggregate_spec = self.aggregate;
        let params = Arc::clone(&self.params);
        let tag_column = self.tag_column.clone();
        let tagged = tagged_schema(&schema, &tag_column);
        let slot = Arc::clone(&self.slot);
        let strategy = self.strategy;
        let bulk = self.bulk.clone();
        let catalog = Arc::clone(&self.catalog);
        let graph_index = Arc::clone(&self.graph_index);
        let declared = Arc::clone(&schema);
        let memory = work(ctx, "AntiJoinMaskExec", &self.metrics)?;
        Ok(self
            .shape
            .stream(memory, &self.metrics, move |reservation| async move {
                let wide = drain_one(outer, &declared, &reservation).await?;
                reservation
                    .blocking(move |reservation| async move {
                        reservation.entries::<u32>(wide.num_rows())?;
                        let num_rows = wide.num_rows();
                        if num_rows == 0 {
                            return Ok(wide);
                        }
                        if strategy == AntiJoinStrategy::KeySet {
                            let key_column = format!("{outer_var}.{}", catalog.system_columns.id);
                            let keep = key_set_mask(
                                &wide,
                                inner.execute(0, inner_ctx)?,
                                &key_column,
                                &predicate,
                                aggregate_spec,
                                &params,
                                &reservation,
                            )
                            .await?;
                            return reservation.filter(&wide, &keep);
                        }
                        if let Some((edge_type, direction)) = &bulk
                            && let Some(row_count) =
                                RowCountPredicate::resolve(&predicate, &params).map_err(external)?
                        {
                            let gi = graph_index.get().await.map_err(external)?;
                            if let Some(mask) = bulk_anti_join_mask(
                                &wide,
                                edge_type,
                                *direction,
                                gi,
                                &catalog,
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
                        if SubqueryAggregate::tracks_values(aggregate_spec) {
                            reservation
                                .entries::<super::subquery_aggregate::ValueAccumulator>(num_rows)?;
                        }
                        let mut aggregate =
                            SubqueryAggregate::new(&predicate, aggregate_spec, &params, num_rows)
                                .map_err(external)?;
                        absorb_inner_batches(
                            &mut aggregate,
                            &inner_batches,
                            &tag_column,
                            predicate.left.arg(),
                            &|batch, arg| crate::engine::expr::evaluate_expr(batch, arg, &params),
                        )
                        .map_err(external)?;
                        let keep = aggregate.keep_mask().map_err(external)?;
                        reservation.filter(&wide, &keep)
                    })
                    .await
            }))
    }
}

/// The keep mask of a `KeySet` block over the outer `wide` batch: each outer
/// key gets one aggregate, the inner rows fold into their key's as they
/// stream (an inner key no outer row holds goes to a discarded group), and
/// every outer row takes its key's verdict. A null outer key takes the
/// verdict of no matches, as its correlated expansion matches nothing.
async fn key_set_mask(
    wide: &RecordBatch,
    mut inner: SendableRecordBatchStream,
    key_column: &str,
    predicate: &SubqueryPredicate,
    aggregate_spec: Option<AggregateSpec>,
    params: &ParamMap,
    reservation: &WorkMemory,
) -> DfResult<BooleanArray> {
    let outer_keys = key_array(wide, key_column).map_err(external)?;
    let num_rows = wide.num_rows();
    reservation.entries::<(&str, u32)>(num_rows)?;
    reservation.entries::<u32>(num_rows)?;
    let mut groups: HashMap<&str, u32> = HashMap::new();
    let mut row_groups = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        row_groups.push(outer_keys.is_valid(row).then(|| {
            let next = groups.len() as u32;
            *groups.entry(outer_keys.value(row)).or_insert(next)
        }));
    }
    let unmatched = groups.len() as u32;
    let discarded = unmatched + 1;
    let group_count = groups.len() + 2;
    reservation.entries::<u64>(group_count)?;
    if SubqueryAggregate::tracks_values(aggregate_spec) {
        reservation.entries::<ValueAccumulator>(group_count)?;
    }
    let mut aggregate =
        SubqueryAggregate::new(predicate, aggregate_spec, params, group_count).map_err(external)?;
    while let Some(batch) = inner.next().await {
        let batch = batch?;
        reservation.metric("input_rows", batch.num_rows());
        if batch.num_rows() == 0 {
            continue;
        }
        let inner_keys = key_array(&batch, key_column).map_err(external)?;
        let tags = UInt32Array::from_iter_values((0..batch.num_rows()).map(|row| {
            inner_keys
                .is_valid(row)
                .then(|| groups.get(inner_keys.value(row)).copied())
                .flatten()
                .unwrap_or(discarded)
        }));
        let values = predicate
            .left
            .arg()
            .map(|arg| crate::engine::expr::evaluate_expr(&batch, arg, params))
            .transpose()
            .map_err(external)?;
        aggregate.absorb(&tags, values.as_ref()).map_err(external)?;
    }
    let verdicts = aggregate.keep_mask().map_err(external)?;
    Ok(row_groups
        .into_iter()
        .map(|group| Some(verdicts.value(group.unwrap_or(unmatched) as usize)))
        .collect())
}

/// The correlation binding's identity column of a key-set batch.
fn key_array<'a>(batch: &'a RecordBatch, key_column: &str) -> Result<&'a StringArray> {
    batch
        .column_by_name(key_column)
        .ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "key-set anti-join batch carries no '{key_column}' column"
            ))
        })?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            OmniError::manifest_internal(format!("'{key_column}' key column is not Utf8"))
        })
}
