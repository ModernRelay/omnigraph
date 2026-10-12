//! The pipelined path of every unranked table read: bounded Lance batches,
//! each sieved by the `RuntimeFilter` a marking parent left (a contains join),
//! prefixed, conformed, gathered up to the session's batch rows or the byte
//! target and sent, so the scan never holds the table, a consumer that stops
//! early (a `limit`) stops the read, and a table of many small fragments still
//! reaches its consumer in full-size batches.

use std::sync::Arc;

use arrow_array::RecordBatch;
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
/// The pool owner of the Lance batch being sieved.
const SCAN_INPUT: &str = "v2 scan input";

/// Whether a table read under `mode` streams: it ranks nothing.
pub(super) fn streams(mode: &SearchMode) -> bool {
    mode.bm25.is_none() && mode.nearest.is_none()
}

impl ScanExec {
    /// Whether the scan sends each Lance batch as it is read: every table read
    /// under no search mode. A ranked scan stays a breaker: its overfetch
    /// ladder reruns it whole.
    pub(super) fn pipelines(&self) -> bool {
        matches!(&self.source, ScanSource::Table { mode, .. } if streams(mode))
    }

    /// The table read as a pipeline over `stream_batches`, under the filter a
    /// marking parent left in the slot (`None` when it left none or the scan
    /// is unmarked); only a marked scan counts what its filter read.
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
        let marked = self.runtime_filter.is_some();
        let gather_rows = self.gather_rows;
        if marked {
            scan_counters(&work, filter.is_some());
        }
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
                stream_batches(
                    read,
                    filter.as_ref(),
                    marked,
                    gather_rows,
                    &binding,
                    &declared,
                    &memory,
                    &sender,
                )
                .await
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}

/// The batches of `read` under `declared`, each sieved by `filter` under its
/// own `SCAN_BATCH` charge (a mixed selection's copy admitted before it is
/// built); an emptied batch is skipped, and the Lance batch let go. Kept rows
/// gather under one charge until they reach the session's batch rows (or the
/// `gather_rows` of a `Limit` over the scan) or the byte target and leave as
/// one batch: Lance yields a batch per fragment, and a per-batch consumer (a
/// traversal's index lookup) would otherwise pay once per fragment. `marked` scans record the runtime-filter counters,
/// unfiltered reads too.
async fn stream_batches(
    read: NodeRead<'_>,
    filter: Option<&RuntimeFilter>,
    marked: bool,
    gather_rows: Option<usize>,
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
    let in_flight = memory.child(SCAN_INPUT)?;
    let mut inert = false;
    let mut gathered = Gathered::new(memory, gather_rows)?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        in_flight.hold(&batch)?;
        let work = Arc::new(memory.child(SCAN_BATCH)?);
        let kept = match filter {
            Some(filter) => filter.keep(&batch, &work, SCAN_BATCH, &mut inert)?,
            None => {
                if marked {
                    count_unsieved(&work, &batch);
                }
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
            if gathered.push(batch)? {
                gathered.send(declared, memory, sender).await?;
            }
        }
        in_flight.release_work();
    }
    gathered.send(declared, memory, sender).await
}

/// Kept batches waiting to leave as one, held under their own `SCAN_BATCH`
/// charge until the send hands it to the consumer.
struct Gathered {
    work: Arc<WorkMemory>,
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    /// The session's batch rows, or the rows of a `Limit` over the scan.
    row_target: usize,
    byte_target: usize,
}

impl Gathered {
    fn new(memory: &WorkMemory, gather_rows: Option<usize>) -> DfResult<Self> {
        let rows = memory.batch_rows();
        Ok(Self {
            work: Arc::new(memory.child(SCAN_BATCH)?),
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            row_target: gather_rows.map_or(rows, |limit| limit.min(rows)),
            byte_target: memory.batch_bytes(),
        })
    }

    /// Hold `batch` and add it; `true` once the gathered rows reach the row
    /// target or their bytes the byte target.
    fn push(&mut self, batch: RecordBatch) -> DfResult<bool> {
        self.work.hold(&batch)?;
        self.rows += batch.num_rows();
        self.bytes = self.bytes.saturating_add(batch.get_array_memory_size());
        self.batches.push(batch);
        Ok(self.rows >= self.row_target || self.bytes >= self.byte_target)
    }

    /// Send what is gathered as one batch (concatenated under the charge,
    /// which then holds only the result) and start again; nothing when empty.
    async fn send(
        &mut self,
        declared: &SchemaRef,
        memory: &WorkMemory,
        sender: &BatchSender,
    ) -> DfResult<()> {
        if self.batches.is_empty() {
            return Ok(());
        }
        let next = Self {
            work: Arc::new(memory.child(SCAN_BATCH)?),
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            row_target: self.row_target,
            byte_target: self.byte_target,
        };
        let gathered = std::mem::replace(self, next);
        let batch = match <[RecordBatch; 1]>::try_from(gathered.batches) {
            Ok([batch]) => batch,
            Err(batches) => gathered.work.concat(declared, &batches)?,
        };
        gathered.work.output(&batch)?;
        sender.send(batch, gathered.work).await
    }
}
