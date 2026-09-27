//! The pipelined path of a marked plain table read: bounded Lance batches, filter
//! or none, each sieved by the `RuntimeFilter` the parent left, prefixed,
//! conformed and sent as it is read, so the scan never holds the table.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;

use super::runtime_filter::{RuntimeFilter, count_unsieved, lance_batch_rows, scan_counters};
use super::{ScanExec, ScanSource};
use crate::engine::operators::memory::WorkMemory;
use crate::engine::operators::producer::{BatchSender, producer_stream};
use crate::engine::operators::{conform, external, polled};
use crate::engine::scan::{NodeRead, add_null_blob_columns, prefix_batch};
use crate::engine::search::SearchMode;

/// The pool owner of one streamed batch's hold, from its sieve to its send.
const SCAN_BATCH: &str = "v2 scan batch";

impl ScanExec {
    /// Whether the scan sends each sieved Lance batch as it is read: a marked
    /// table read under no search mode (a ranked scan is never marked).
    pub(super) fn pipelines(&self) -> bool {
        self.runtime_filter.is_some()
            && matches!(
                &self.source,
                ScanSource::Table { mode, .. } if mode.bm25.is_none() && mode.nearest.is_none()
            )
    }

    /// The marked table read as a pipeline over `stream_batches`, under the
    /// filter the parent left in the slot (`None` when it left none).
    pub(super) fn execute_pipelined(
        &self,
        mode: SearchMode,
        filter: Option<RuntimeFilter>,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let schema: SchemaRef = self.schema();
        let declared = Arc::clone(&schema);
        let type_name = self.type_name.clone();
        let binding = self.binding.clone();
        let filters = self.filters.clone();
        let projection = self.projection.clone();
        let params = Arc::clone(&self.params);
        let snapshot = self.snapshot.clone();
        let catalog = Arc::clone(&self.catalog);
        let mut work = WorkMemory::new(ctx, "ScanExec")?;
        work.set_metrics(self.metrics.clone());
        work.metric("input_rows", 0);
        scan_counters(&work, filter.is_some());
        let stream = producer_stream(
            schema,
            Arc::new(work),
            Some(&self.metrics),
            move |memory, sender| async move {
                let read = NodeRead::resolve(
                    &type_name,
                    &filters,
                    &params,
                    &snapshot,
                    &catalog,
                    &mode,
                    projection.as_ref(),
                    &memory,
                )
                .await
                .map_err(external)?;
                stream_batches(read, filter.as_ref(), &binding, &declared, &memory, &sender).await
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}

/// The batches of `read` under `declared`, each sieved by `filter` under its
/// own `SCAN_BATCH` charge (a mixed selection's copy admitted before it is
/// built) and sent; an emptied batch is skipped, and the Lance batch let go.
async fn stream_batches(
    read: NodeRead<'_>,
    filter: Option<&RuntimeFilter>,
    binding: &str,
    declared: &SchemaRef,
    memory: &WorkMemory,
    sender: &BatchSender,
) -> DfResult<()> {
    if read.proven_empty {
        return Ok(());
    }
    let bytes = memory.batch_bytes();
    let plan = read
        .plan(Some((lance_batch_rows(bytes), bytes)), |_| Ok(()))
        .await
        .map_err(external)?;
    let (_plan, mut stream) = memory.stream(plan)?;
    let in_flight = memory.child("runtime filter input")?;
    let mut inert = false;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        in_flight.hold(&batch)?;
        let work = Arc::new(memory.child(SCAN_BATCH)?);
        let kept = match filter {
            Some(filter) => filter.keep(&batch, &work, SCAN_BATCH, &mut inert)?,
            None => {
                count_unsieved(&work, &batch);
                batch.clone()
            }
        };
        if kept.num_rows() > 0 {
            work.hold(&kept)?;
            let kept = if read.columns.has_blobs {
                work.grow(
                    read.node_type
                        .blob_properties
                        .len()
                        .saturating_mul(kept.num_rows().saturating_mul(8).saturating_add(128)),
                )?;
                let kept = add_null_blob_columns(&kept, read.node_type).map_err(external)?;
                work.hold(&kept)?;
                kept
            } else {
                kept
            };
            let prefixed = prefix_batch(&kept, binding).map_err(external)?;
            let batch = conform(prefixed, declared).map_err(external)?;
            sender.send(batch, work).await?;
        }
        in_flight.release_work();
    }
    Ok(())
}
