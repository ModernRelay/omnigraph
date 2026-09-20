//! Scan and graph `ExecutionPlan`s with shared query memory accounting.
//! Each pipeline breaker drains the input it needs, admits retained batches
//! and work storage to the query's pool, and emits results in batch-size
//! slices.

use std::future::Future;
use std::sync::Arc;

use self::memory::WorkMemory;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{PlanProperties, SendableRecordBatchStream};
use futures::{StreamExt, TryStreamExt};

use crate::error::{OmniError, Result};

mod anti_join;
mod charge;
mod expand;
mod expand_stream;
mod hash_fallback;
pub(in crate::engine) mod memory;
mod metadata_count;
mod producer;
mod rank_fuse;
mod scan;
mod single_hop;

pub(super) use anti_join::{
    AntiJoinMaskExec, OuterReferenceExec, OuterSlot, fresh_tag_column, tagged_schema,
};
pub(super) use charge::ChargeExec;
pub(super) use expand::{ExpandExec, ExpandStep, GraphEnv};
pub(super) use hash_fallback::{HashFallbackExec, HashProbeExec};
pub(super) use metadata_count::MetadataCountExec;
pub(super) use rank_fuse::RankFuseExec;
pub(super) use scan::{ScanExec, ScanSource};

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
