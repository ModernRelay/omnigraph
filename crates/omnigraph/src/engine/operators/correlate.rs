//! `CorrelateExec`: `from` an inline read with its own aggregate,
//! `distinct`, `order` or `limit`, the core's `Correlate` over the read's
//! `Group`, `Order` and `Cut` (docs/rfcs/2026-11-07-gq-3.md, sections 2 and
//! 3). The read's inner tree runs once: correlated, over the outer batch
//! tagged with each row's index from the slot this operator fills before
//! executing it (as an `optional`'s does), so each inner row names the
//! outer row it belongs to; uncorrelated, from nothing. The read is then
//! evaluated per outer row (once over all rows when uncorrelated): its
//! group (`AggregateExec` over the inner rows, grouped by the tag and the
//! keys, built at lowering and fed from its own slot), its value columns on
//! every row of the read, its order (the keys, then the tie rule) and its
//! cut, each partitioned by the tag, so an outer `where` or `limit` never
//! reaches the read's own cut. Each outer row leaves once per row of its
//! read: the outer columns, then the read's reference columns (a group key
//! carries its binding's columns from the group's first inner row) and its
//! value columns, `values.<column>`. A read grouped by no key is one row
//! for each outer row, over no inner row too: `count` 0 and every other
//! aggregate null.

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::ir::{IRExpr, IROrdering, ParamMap};

use super::anti_join::{FilledSlot, OuterSlot, tag_batch, tagged_schema};
use super::memory::WorkMemory;
use super::shape::{Breaker, work};
use super::{drain, drain_one, external};
use crate::engine::expr::evaluate_expr;
use crate::error::{OmniError, Result};

/// The read's group: the `AggregateExec` that groups the inner rows, fed
/// from `slot`, and how its output maps onto the read's rows.
pub(crate) struct CorrelateGroup {
    /// The leaf the plan reads: the inner rows with `row_column`.
    pub(crate) slot: Arc<OuterSlot>,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// The row index column appended to the inner rows.
    pub(crate) row_column: String,
    /// The plan's `min(row_column)` output: each group's first inner row.
    pub(crate) first_column: String,
    /// The bindings a reference key carries, from each group's first row.
    pub(crate) carried: Vec<String>,
    /// The plan's value outputs, each a column of the read's `values`:
    /// the plan's output name and the column it becomes.
    pub(crate) outputs: Vec<(String, String)>,
    /// Grouped by no key: one row per outer row (once uncorrelated) over
    /// no inner row too, its `count` columns 0 and every other null.
    pub(crate) keyless: Option<Vec<String>>,
}

/// What the read computes over its rows and what leaves.
pub(crate) struct CorrelateRead {
    pub(crate) group: Option<CorrelateGroup>,
    /// Each value column: its output name (`values.<column>`), its
    /// expression over the read's rows and its type.
    pub(crate) values: Vec<(String, IRExpr, DataType)>,
    pub(crate) order_by: Vec<IROrdering>,
    /// The tie rule's columns after the keys, by name.
    pub(crate) tiebreak: Vec<String>,
    pub(crate) limit: Option<usize>,
    /// The reference columns that leave: each binding's columns.
    pub(crate) references: Vec<String>,
    pub(crate) params: Arc<ParamMap>,
}

/// The correlated outer rows: the slot the inner tree's `OuterReference`
/// reads and the tag column naming each outer row.
pub(crate) struct CorrelateOuter {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) outer_var: String,
    pub(crate) tag_column: String,
    pub(crate) slot: Arc<OuterSlot>,
}

pub(crate) struct CorrelateExec {
    outer: Option<CorrelateOuter>,
    inner: Arc<dyn ExecutionPlan>,
    read: Arc<CorrelateRead>,
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl CorrelateExec {
    pub(crate) fn try_new(
        outer: Option<CorrelateOuter>,
        inner: Arc<dyn ExecutionPlan>,
        read: Arc<CorrelateRead>,
    ) -> Result<Self> {
        let schema = output_schema(
            outer.as_ref().map(|outer| outer.plan.schema()),
            &inner.schema(),
            &read,
        )?;
        Ok(Self {
            outer,
            inner,
            read,
            shape: Breaker::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// The outer columns (correlated), then each reference's columns as the
/// inner tree carries them, then each value column, nullable.
fn output_schema(
    outer: Option<SchemaRef>,
    inner: &Schema,
    read: &CorrelateRead,
) -> Result<SchemaRef> {
    let mut fields: Vec<Field> = outer
        .iter()
        .flat_map(|schema| schema.fields().iter().map(|f| f.as_ref().clone()))
        .collect();
    for binding in &read.references {
        let prefix = format!("{binding}.");
        let carried: Vec<Field> = inner
            .fields()
            .iter()
            .filter(|field| field.name().starts_with(&prefix))
            .map(|field| field.as_ref().clone())
            .collect();
        if carried.is_empty() {
            return Err(OmniError::manifest_internal(format!(
                "the inner tree of a correlated read carries no column of `${binding}`"
            )));
        }
        fields.extend(carried);
    }
    for (name, _, data_type) in &read.values {
        fields.push(Field::new(name, data_type.clone(), true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

impl fmt::Debug for CorrelateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CorrelateExec")
            .field(
                "outer_var",
                &self.outer.as_ref().map(|outer| &outer.outer_var),
            )
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CorrelateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.outer {
            Some(outer) => write!(f, "CorrelateExec: ${}", outer.outer_var),
            None => write!(f, "CorrelateExec"),
        }
    }
}

impl ExecutionPlan for CorrelateExec {
    fn name(&self) -> &str {
        "CorrelateExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.outer {
            Some(outer) => vec![&outer.plan, &self.inner],
            None => vec![&self.inner],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let expected = if self.outer.is_some() { 2 } else { 1 };
        assert_eq!(
            children.len(),
            expected,
            "CorrelateExec has its inner child and, correlated, an outer one"
        );
        let inner = children.pop().expect("inner child");
        let outer = self.outer.as_ref().map(|outer| CorrelateOuter {
            plan: children.pop().expect("outer child"),
            outer_var: outer.outer_var.clone(),
            tag_column: outer.tag_column.clone(),
            slot: Arc::clone(&outer.slot),
        });
        Ok(Arc::new(
            Self::try_new(outer, inner, Arc::clone(&self.read)).map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CorrelateExec has one partition");
        let schema: SchemaRef = self.schema();
        let outer = self.outer.as_ref().map(|outer| {
            let outer_schema = outer.plan.schema();
            (
                Arc::clone(&outer.plan),
                tagged_schema(&outer_schema, &outer.tag_column),
                outer_schema,
                outer.tag_column.clone(),
                Arc::clone(&outer.slot),
            )
        });
        let inner = Arc::clone(&self.inner);
        let inner_schema = inner.schema();
        let read = Arc::clone(&self.read);
        let memory = work(Arc::clone(&ctx), "CorrelateExec", &self.metrics)?;
        Ok(self
            .shape
            .stream(memory, &self.metrics, move |reservation| async move {
                let Some((outer_plan, tagged, outer_schema, tag_column, slot)) = outer else {
                    let rows = drain_one(
                        inner.execute(0, Arc::clone(&ctx))?,
                        &inner_schema,
                        &reservation,
                    )
                    .await?;
                    let result = evaluate(&rows, None, None, &read, &ctx, &reservation).await?;
                    return assemble(&result, None, None, &schema).map_err(external);
                };
                let wide = drain_one(
                    outer_plan.execute(0, Arc::clone(&ctx))?,
                    &outer_schema,
                    &reservation,
                )
                .await?;
                if wide.num_rows() == 0 {
                    return Ok(RecordBatch::new_empty(schema));
                }
                let tagged_batch = tag_batch(&wide, tagged).map_err(external)?;
                reservation.hold(&tagged_batch)?;
                slot.fill(tagged_batch);
                let filled = FilledSlot(&slot);
                let rows = async {
                    let batches = drain(inner.execute(0, Arc::clone(&ctx))?, &reservation).await?;
                    reservation.concat(&inner_schema, &batches)
                }
                .await;
                drop(filled);
                let rows = rows?;
                let result = evaluate(
                    &rows,
                    Some(&tag_column),
                    Some(wide.num_rows()),
                    &read,
                    &ctx,
                    &reservation,
                )
                .await?;
                assemble(&result, Some(&wide), Some(&tag_column), &schema).map_err(external)
            }))
    }
}

/// The read's rows from the inner rows `rows`, partitioned by `tag` when
/// correlated over `outer_rows` rows: grouped, its value columns appended,
/// ordered and cut.
async fn evaluate(
    rows: &RecordBatch,
    tag: Option<&str>,
    outer_rows: Option<usize>,
    read: &CorrelateRead,
    ctx: &Arc<TaskContext>,
    reservation: &Arc<WorkMemory>,
) -> DfResult<RecordBatch> {
    let rows = match &read.group {
        Some(group) => grouped(rows, tag, outer_rows, group, ctx, reservation).await?,
        None => rows.clone(),
    };
    let mut fields: Vec<Field> = rows
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns: Vec<ArrayRef> = rows.columns().to_vec();
    for (name, expr, data_type) in &read.values {
        let values = evaluate_expr(&rows, expr, &read.params).map_err(external)?;
        fields.push(Field::new(name, data_type.clone(), true));
        columns.push(values);
    }
    let rows = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    reservation.hold(&rows)?;
    if read.order_by.is_empty() && read.limit.is_none() {
        return Ok(rows);
    }
    let kept = ordered_cut(&rows, tag, read).map_err(external)?;
    reservation.take(&rows, &kept)
}

/// The inner rows grouped: the group plan over them, each group's carried
/// reference columns taken from its first row; a keyless group adds its
/// empty-input row for each outer row (once uncorrelated) it has none for.
async fn grouped(
    rows: &RecordBatch,
    tag: Option<&str>,
    outer_rows: Option<usize>,
    group: &CorrelateGroup,
    ctx: &Arc<TaskContext>,
    reservation: &Arc<WorkMemory>,
) -> DfResult<RecordBatch> {
    let mut indexed: Vec<ArrayRef> = rows.columns().to_vec();
    indexed.push(Arc::new(UInt64Array::from_iter_values(
        0..rows.num_rows() as u64,
    )));
    let mut fields: Vec<Field> = rows
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(&group.row_column, DataType::UInt64, false));
    let indexed = RecordBatch::try_new(Arc::new(Schema::new(fields)), indexed)?;
    reservation.hold(&indexed)?;
    group.slot.fill(indexed);
    let filled = FilledSlot(&group.slot);
    let output = async {
        let plan_schema = group.plan.schema();
        let batches = drain(group.plan.execute(0, Arc::clone(ctx))?, reservation).await?;
        reservation.concat(&plan_schema, &batches)
    }
    .await;
    drop(filled);
    let output = output?;
    let first = output
        .column_by_name(&group.first_column)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| {
            external(OmniError::manifest_internal(
                "a correlated read's group carries no first-row index",
            ))
        })?;
    let first = UInt32Array::from_iter(first.iter().map(|row| row.map(|row| row as u32)));
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    if let Some(tag) = tag {
        let tags = output.column_by_name(tag).ok_or_else(|| {
            external(OmniError::manifest_internal(
                "a correlated read's group carries no outer tag",
            ))
        })?;
        fields.push(Field::new(tag, DataType::UInt32, false));
        columns.push(Arc::clone(tags));
    }
    let schema = rows.schema();
    for binding in &group.carried {
        let prefix = format!("{binding}.");
        for (index, field) in schema.fields().iter().enumerate() {
            if field.name().starts_with(&prefix) {
                fields.push(field.as_ref().clone().with_nullable(true));
                columns.push(arrow_select::take::take(
                    rows.column(index).as_ref(),
                    &first,
                    None,
                )?);
            }
        }
    }
    for (output_name, column) in &group.outputs {
        let values = output.column_by_name(output_name).ok_or_else(|| {
            external(OmniError::manifest_internal(format!(
                "a correlated read's group carries no column '{output_name}'"
            )))
        })?;
        fields.push(Field::new(column, values.data_type().clone(), true));
        columns.push(Arc::clone(values));
    }
    let schema = Arc::new(Schema::new(fields));
    let mut result = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    if let Some(counts) = &group.keyless {
        let missing: Vec<u32> = match (tag, outer_rows) {
            (Some(_), Some(outer_rows)) => {
                let present: HashSet<u32> = result
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .map(|tags| tags.iter().flatten().collect())
                    .unwrap_or_default();
                (0..outer_rows as u32)
                    .filter(|row| !present.contains(row))
                    .collect()
            }
            _ if result.num_rows() == 0 => vec![0],
            _ => Vec::new(),
        };
        if !missing.is_empty() {
            let padding = schema
                .fields()
                .iter()
                .map(|field| {
                    if tag == Some(field.name().as_str()) {
                        Ok(Arc::new(UInt32Array::from(missing.clone())) as ArrayRef)
                    } else if counts.contains(field.name()) {
                        Ok(Arc::new(Int64Array::from(vec![0i64; missing.len()])) as ArrayRef)
                    } else {
                        Ok(arrow_array::new_null_array(
                            field.data_type(),
                            missing.len(),
                        ))
                    }
                })
                .collect::<DfResult<Vec<ArrayRef>>>()?;
            let padding = RecordBatch::try_new(Arc::clone(&schema), padding)?;
            result = reservation.concat(&schema, &[result, padding])?;
        }
    }
    reservation.hold(&result)?;
    Ok(result)
}

/// The indices of the rows the read keeps, in its order: sorted by the tag
/// (correlated), the `order` keys and the tie rule, then the first `limit`
/// rows of each tag.
fn ordered_cut(rows: &RecordBatch, tag: Option<&str>, read: &CorrelateRead) -> Result<UInt32Array> {
    let mut keys = Vec::new();
    if let Some(tag) = tag {
        keys.push(SortColumn {
            values: column(rows, tag)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });
    }
    for key in &read.order_by {
        keys.push(SortColumn {
            values: evaluate_expr(rows, &key.expr, &read.params)?,
            options: Some(SortOptions {
                descending: key.descending,
                nulls_first: !key.descending,
            }),
        });
    }
    for name in &read.tiebreak {
        keys.push(SortColumn {
            values: column(rows, name)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });
    }
    let sorted = if keys.is_empty() {
        UInt32Array::from_iter_values(0..rows.num_rows() as u32)
    } else {
        lexsort_to_indices(&keys, None).map_err(OmniError::arrow_internal)?
    };
    let Some(limit) = read.limit else {
        return Ok(sorted);
    };
    let tags = match tag {
        Some(tag) => Some(column(rows, tag)?),
        None => None,
    };
    let tags = tags
        .as_ref()
        .and_then(|tags| tags.as_any().downcast_ref::<UInt32Array>());
    let mut kept = Vec::new();
    let mut current: Option<u32> = None;
    let mut taken = 0usize;
    for row in sorted.values().iter().copied() {
        let partition = tags.map_or(0, |tags| tags.value(row as usize));
        if current != Some(partition) {
            current = Some(partition);
            taken = 0;
        }
        if taken < limit {
            kept.push(row);
            taken += 1;
        }
    }
    Ok(UInt32Array::from(kept))
}

fn column(rows: &RecordBatch, name: &str) -> Result<ArrayRef> {
    rows.column_by_name(name).map(Arc::clone).ok_or_else(|| {
        OmniError::manifest_internal(format!("a correlated read's rows carry no column '{name}'"))
    })
}

/// The output rows: each read row's outer row (correlated) by its tag, then
/// the read's reference columns and value columns.
fn assemble(
    result: &RecordBatch,
    wide: Option<&RecordBatch>,
    tag: Option<&str>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    if let (Some(wide), Some(tag)) = (wide, tag) {
        let tags = column(result, tag)?;
        let tags = tags
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| OmniError::manifest_internal("an outer tag is not UInt32"))?;
        for outer in wide.columns() {
            columns.push(
                arrow_select::take::take(outer.as_ref(), tags, None)
                    .map_err(OmniError::arrow_internal)?,
            );
        }
    }
    let skip = columns.len();
    for field in schema.fields().iter().skip(skip) {
        columns.push(column(result, field.name())?);
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)
}
