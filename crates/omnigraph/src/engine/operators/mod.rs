//! The read engine's operators, one per read `PhysicalNode` kind, with
//! shared query memory accounting. Each pipeline breaker drains the input it
//! needs, admits retained batches and work storage to the query's pool, and
//! emits results in batch-size slices. Every operator counts the polls of
//! the streams it hands out (`polled`) and, where its node declares a switch,
//! sets the `switch` gauge to the side it took; the execution report reads
//! both off `metrics()`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use self::memory::WorkMemory;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{PlanProperties, RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};

mod anti_join;
mod cross_join;
mod expand;
mod expand_stream;
mod filter;
mod hash_join;
mod limit;
pub(in crate::engine) mod memory;
mod metadata_count;
mod producer;
mod projection;
mod rank_fuse;
mod scan;
mod single_hop;
mod sort;
mod subquery_aggregate;

pub(super) use anti_join::{
    AntiJoinMaskExec, OuterReferenceExec, OuterSlot, fresh_tag_column, tagged_schema,
};
pub(super) use cross_join::CrossJoinExec;
pub(super) use expand::{ExpandExec, ExpandStep, GraphEnv};
pub(super) use filter::FilterExec;
pub(super) use hash_join::{HashJoinExec, LookupSpec};
pub(super) use limit::LimitExec;
pub(super) use metadata_count::MetadataCountExec;
pub(super) use projection::ProjectionExec;
pub(super) use rank_fuse::{ArmOrder, RankFuseExec};
pub(super) use scan::{ScanExec, ScanSource};
pub(super) use sort::{SortExec, SortKey};
pub(crate) use subquery_aggregate::{RowCountPredicate, SubqueryAggregate, absorb_inner_batches};

/// The `polls` counter: one per `poll_next` of any stream the operator handed
/// out, so a zero says no consumer ever asked the operator for a batch.
const POLLS: &str = "polls";

/// The `drained` counter: one per stream the operator handed out whose
/// consumer pulled it to its end, so a row count is complete only where this
/// is set.
const DRAINED: &str = "drained";

/// The `switch` gauge: the slot of the [`Switch`] side an operator with a
/// declared switch took, 0 until it took one.
const SWITCH: &str = "switch";

/// One side of a switch the plan declares on a node: the access path a
/// `HashJoin` ran through, the mode an `Expand` ended on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Switch {
    HashJoin,
    IdLookup,
    Csr,
    IndexedScan,
}

impl Switch {
    const ALL: [Self; 4] = [Self::HashJoin, Self::IdLookup, Self::Csr, Self::IndexedScan];

    fn from_slot(slot: usize) -> Option<Self> {
        Self::ALL.get(slot.checked_sub(1)?).copied()
    }

    fn slot(self) -> usize {
        Self::ALL
            .iter()
            .position(|switch| *switch == self)
            .map_or(0, |index| index + 1)
    }

    /// The gauge an operator with a declared switch sets; a later `record`
    /// overwrites, so the value is the side the operator ended on.
    pub(super) fn gauge(metrics: &ExecutionPlanMetricsSet) -> Gauge {
        MetricBuilder::new(metrics).gauge(SWITCH, 0)
    }

    pub(super) fn record(self, gauge: &Gauge) {
        gauge.set(self.slot());
    }

    /// The side the operator behind `metrics` took, `None` on an operator
    /// without a declared switch or one that took no side yet.
    pub(in crate::engine) fn read(metrics: &MetricsSet) -> Option<Self> {
        metrics.iter().find_map(|metric| match metric.value() {
            MetricValue::Gauge { name, gauge } if name == SWITCH => Self::from_slot(gauge.value()),
            _ => None,
        })
    }
}

/// Whether any stream of the operator behind `metrics` was polled: its
/// `polls` count; a DataFusion operator, which counts no polls, is read
/// through the rows it produced or the end it recorded.
pub(in crate::engine) fn was_polled(metrics: &MetricsSet) -> bool {
    let mut counts_polls = false;
    let mut polled = false;
    let mut produced = false;
    for metric in metrics.iter() {
        match metric.value() {
            MetricValue::Count { name, count } if name == POLLS => {
                counts_polls = true;
                polled = count.value() > 0;
            }
            MetricValue::OutputRows(count) if count.value() > 0 => produced = true,
            MetricValue::EndTimestamp(end) if end.value().is_some() => produced = true,
            _ => {}
        }
    }
    if counts_polls { polled } else { produced }
}

/// Whether the operator behind `metrics` was drained: its consumer pulled a
/// stream to its end. Only an omnigraph operator counts that; a DataFusion
/// operator records its end timestamp on drop as well, which observes
/// nothing, so it reads `None`.
pub(in crate::engine) fn was_drained(metrics: &MetricsSet) -> Option<bool> {
    let mut counts_polls = false;
    let mut drained = false;
    for metric in metrics.iter() {
        match metric.value() {
            MetricValue::Count { name, .. } if name == POLLS => counts_polls = true,
            MetricValue::Count { name, count } if name == DRAINED => drained = count.value() > 0,
            _ => {}
        }
    }
    counts_polls.then_some(drained)
}

/// `stream` with every poll counted on `metrics`, and its end when reached.
pub(super) fn polled(
    metrics: &ExecutionPlanMetricsSet,
    stream: SendableRecordBatchStream,
) -> SendableRecordBatchStream {
    Box::pin(Polled {
        stream,
        polls: MetricBuilder::new(metrics).counter(POLLS, 0),
        drained: MetricBuilder::new(metrics).counter(DRAINED, 0),
    })
}

struct Polled {
    stream: SendableRecordBatchStream,
    polls: Count,
    drained: Count,
}

impl Stream for Polled {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.add(1);
        let polled = self.stream.as_mut().poll_next(cx);
        if matches!(polled, Poll::Ready(None)) {
            self.drained.add(1);
        }
        polled
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl RecordBatchStream for Polled {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

/// A stream that ends at once: the operator ran and produced nothing, its
/// input never executed.
pub(super) fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::empty(),
    ))
}

/// The properties of a pipeline breaker: one partition, output only
/// once the input is complete, bounded.
pub(super) fn breaker_properties(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

/// The properties of an operator that emits as its input arrives: one
/// partition, incremental, bounded.
pub(super) fn streaming_properties(schema: SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Incremental,
        Boundedness::Bounded,
    ))
}

pub(super) fn external(error: OmniError) -> DataFusionError {
    error.into_datafusion_external()
}

/// The child's batches, each charged to `reservation` before it is held.
pub(super) async fn drain(
    mut stream: SendableRecordBatchStream,
    reservation: &WorkMemory,
) -> DfResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        reservation.metric("input_rows", batch.num_rows());
        reservation.hold(&batch)?;
        reservation.entries::<RecordBatch>(1)?;
        batches.push(batch);
    }
    Ok(batches)
}

/// The child's batches as one batch of the child's schema.
pub(super) async fn drain_one(
    stream: SendableRecordBatchStream,
    schema: &SchemaRef,
    reservation: &WorkMemory,
) -> DfResult<RecordBatch> {
    let input_memory = reservation.child("graph input concatenation")?;
    let batches = drain(stream, &input_memory).await?;
    let batch = input_memory.concat(schema, &batches)?;
    reservation.hold(&batch)?;
    Ok(batch)
}

/// A produced batch under the schema the operator declared: columns matched
/// by name, types exact, nothing missing or extra. A zero-row batch carries
/// no data, so it is re-declared without a check (v1 shapes an empty
/// hydration and an empty scan differently).
pub(super) fn conform(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    if batch.num_columns() != schema.fields().len() {
        return Err(OmniError::manifest_internal(format!(
            "operator produced {} columns where its schema declares {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            let column = batch.column_by_name(field.name()).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "operator produced no column '{}' its schema declares",
                    field.name()
                ))
            })?;
            if column.data_type() != field.data_type() {
                return Err(OmniError::manifest_internal(format!(
                    "operator produced column '{}' as {:?} where its schema declares {:?}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )));
            }
            Ok(Arc::clone(column))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)
}

/// A produced batch re-declared by position: the RRF fusion concatenates
/// slices of both legs positionally, as v1's `build_fused_batch` does.
pub(super) fn conform_positional(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    RecordBatch::try_new(Arc::clone(schema), batch.columns().to_vec())
        .map_err(OmniError::arrow_internal)
}

/// `left` beside `right`, refused on a shared column name the way
/// `hconcat_batches` refuses it (#605).
pub(super) fn joined_schema(left: &Schema, right: &Schema) -> Result<SchemaRef> {
    let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
    for field in right.fields() {
        if left.column_with_name(field.name()).is_some() {
            return Err(OmniError::manifest_internal(format!(
                "duplicate column '{}' when joining batches",
                field.name()
            )));
        }
        fields.push(field.as_ref().clone());
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// The stream of a breaker: `body` builds the whole output under the
/// reservation, which then lives with the stream while the output leaves in
/// `batch_size` slices; dropping the stream drops the body, the held batches
/// and the reservation together.
pub(super) fn breaker_stream<F>(
    name: &str,
    schema: SchemaRef,
    ctx: &Arc<datafusion::execution::TaskContext>,
    metrics: &ExecutionPlanMetricsSet,
    body: impl FnOnce(Arc<WorkMemory>) -> F,
) -> SendableRecordBatchStream
where
    F: Future<Output = DfResult<RecordBatch>> + Send + 'static,
{
    let reservation = match WorkMemory::new(Arc::clone(ctx), name) {
        Ok(mut memory) => {
            memory.set_metrics(metrics.clone());
            memory.metric("input_rows", 0);
            Arc::new(memory)
        }
        Err(error) => {
            return Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::once(async move { Err(error) }),
            ));
        }
    };
    let batch_size = ctx.session_config().batch_size();
    let emitted = Arc::clone(&reservation);
    let output_schema = Arc::clone(&schema);
    let future = body(reservation);
    let baseline = BaselineMetrics::new(metrics, 0);
    let stream = futures::stream::once(async move {
        let mut future = Box::pin(future);
        let batch = futures::future::poll_fn(|cx| {
            let _timer = baseline.elapsed_compute().timer();
            future.as_mut().poll(cx)
        })
        .await?;
        baseline.record_output(batch.num_rows());
        baseline.done();
        emitted.output(&batch)?;
        let held = Arc::clone(&emitted);
        Ok::<_, DataFusionError>(
            futures::stream::iter((0..batch.num_rows()).step_by(batch_size.max(1)).map(
                move |offset| Ok(batch.slice(offset, batch_size.min(batch.num_rows() - offset))),
            ))
            .map(move |batch| {
                let _held = &held;
                batch
            }),
        )
    })
    .try_flatten();
    Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
}
