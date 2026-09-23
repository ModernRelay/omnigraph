//! `SortExec`: a `Sort` node's total order. With a `fetch`, a streaming
//! top-k: every input batch is merged into the retained best `fetch` rows and
//! released, so the pool's steady state is one batch and `fetch` rows, with
//! the merge's transient copies on top. Without one, the whole input is held
//! under the query pool (no spill) and sorted once. The columns the
//! projection below carried for the sort alone (the `HIDDEN` prefix) leave
//! here.

use std::fmt;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::memory::WorkMemory;
use super::{breaker_properties, breaker_stream, drain, empty_stream, external, polled};
use crate::engine::lower::HIDDEN;
use crate::error::{OmniError, Result};

/// One key of the order: a column of the input, its direction and where
/// nulls go.
#[derive(Debug, Clone)]
pub(crate) struct SortKey {
    pub(crate) column: String,
    pub(crate) descending: bool,
    pub(crate) nulls_first: bool,
}

pub(crate) struct SortExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<SortKey>,
    fetch: Option<usize>,
    /// The input columns the output keeps, in input order.
    visible: Vec<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl SortExec {
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        keys: Vec<SortKey>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        for key in &keys {
            if input_schema.column_with_name(&key.column).is_none() {
                return Err(OmniError::manifest_internal(format!(
                    "the sort key column '{}' is not in the sort input",
                    key.column
                )));
            }
        }
        let visible: Vec<usize> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| !field.name().starts_with(HIDDEN))
            .map(|(index, _)| index)
            .collect();
        let schema = Arc::new(Schema::new(
            visible
                .iter()
                .map(|index| input_schema.field(*index).clone())
                .collect::<Vec<_>>(),
        ));
        Ok(Self {
            input,
            keys,
            fetch,
            visible,
            properties: breaker_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl fmt::Debug for SortExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SortExec")
            .field("keys", &self.keys)
            .field("fetch", &self.fetch)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SortExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<String> = self
            .keys
            .iter()
            .map(|key| {
                format!(
                    "{} {} NULLS {}",
                    key.column,
                    if key.descending { "DESC" } else { "ASC" },
                    if key.nulls_first { "FIRST" } else { "LAST" }
                )
            })
            .collect();
        write!(f, "SortExec: [{}]", keys.join(", "))?;
        if let Some(fetch) = self.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for SortExec {
    fn name(&self) -> &str {
        "SortExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "SortExec has one child");
        Ok(Arc::new(
            Self::try_new(
                children.pop().expect("one child"),
                self.keys.clone(),
                self.fetch,
            )
            .map_err(external)?,
        ))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "SortExec has one partition");
        let schema: SchemaRef = self.schema();
        if self.fetch == Some(0) {
            return Ok(polled(&self.metrics, empty_stream(schema)));
        }
        let input_schema = self.input.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let keys = Arc::new(self.keys.clone());
        let fetch = self.fetch;
        let visible = self.visible.clone();
        let stream = breaker_stream(
            "SortExec",
            schema,
            &ctx,
            &self.metrics,
            move |reservation| async move {
                let sorted = match fetch {
                    Some(fetch) => top_k(input, &input_schema, &keys, fetch, &reservation).await?,
                    None => {
                        let batches = drain(input, &reservation).await?;
                        let batch = reservation.concat(&input_schema, &batches)?;
                        reservation
                            .blocking(move |reservation| async move {
                                sorted(&batch, &keys, None, &reservation)
                            })
                            .await?
                    }
                };
                Ok(sorted.project(&visible)?)
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}

/// `batch` in the order of `keys`, the first `fetch` rows when bounded,
/// admitted and held under `memory`.
fn sorted(
    batch: &RecordBatch,
    keys: &[SortKey],
    fetch: Option<usize>,
    memory: &WorkMemory,
) -> DfResult<RecordBatch> {
    let columns = keys
        .iter()
        .map(|key| {
            let values = batch.column_by_name(&key.column).ok_or_else(|| {
                external(OmniError::manifest_internal(format!(
                    "the sort key column '{}' left the sort input",
                    key.column
                )))
            })?;
            Ok(SortColumn {
                values: Arc::clone(values),
                options: Some(SortOptions {
                    descending: key.descending,
                    nulls_first: key.nulls_first,
                }),
            })
        })
        .collect::<DfResult<Vec<_>>>()?;
    memory.entries::<u32>(batch.num_rows())?;
    let indices = if columns.is_empty() {
        let rows = fetch.map_or(batch.num_rows(), |fetch| fetch.min(batch.num_rows()));
        let rows = u32::try_from(rows).map_err(|_| {
            external(OmniError::manifest_internal(
                "sort input exceeds row index range",
            ))
        })?;
        UInt32Array::from_iter_values(0..rows)
    } else {
        lexsort_to_indices(&columns, fetch)?
    };
    memory.permute(batch, &indices)
}

/// The best `fetch` rows of `input` under `keys`: each batch is merged into
/// the rows kept so far and released, the kept rows held under their own
/// lease until the next merge replaces them.
async fn top_k(
    mut input: SendableRecordBatchStream,
    schema: &SchemaRef,
    keys: &Arc<Vec<SortKey>>,
    fetch: usize,
    reservation: &Arc<WorkMemory>,
) -> DfResult<RecordBatch> {
    let mut kept: Option<(RecordBatch, WorkMemory)> = None;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        reservation.metric("input_rows", batch.num_rows());
        if batch.num_rows() == 0 {
            continue;
        }
        let previous = kept.take();
        let keys = Arc::clone(keys);
        let schema = Arc::clone(schema);
        kept = Some(
            reservation
                .blocking(move |reservation| async move {
                    let step = reservation.child("sort step")?;
                    step.hold(&batch)?;
                    let merged = match &previous {
                        None => batch,
                        Some((kept, _)) => step.concat(&schema, &[kept.clone(), batch])?,
                    };
                    let next = sorted(&merged, &keys, Some(fetch), &step)?;
                    let retained = reservation.child("sort retained")?;
                    retained.output(&next)?;
                    drop(previous);
                    Ok((next, retained))
                })
                .await?,
        );
    }
    Ok(match kept {
        Some((batch, retained)) => {
            reservation.hold(&batch)?;
            drop(retained);
            batch
        }
        None => RecordBatch::new_empty(Arc::clone(schema)),
    })
}
