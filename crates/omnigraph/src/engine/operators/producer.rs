//! A bounded producer channel with query-owned leases for queued Arrow batches.

use std::future::Future;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use futures::StreamExt;
use tokio::sync::mpsc;

use super::memory::WorkMemory;

/// A single producer sends each batch and its reservation in the same order.
pub(super) struct BatchSender {
    batches: mpsc::Sender<Result<RecordBatch>>,
    leases: mpsc::Sender<Arc<WorkMemory>>,
}

impl BatchSender {
    pub(super) async fn send(&self, batch: RecordBatch, memory: Arc<WorkMemory>) -> Result<()> {
        let queued = Arc::new(memory.child("queued producer batch")?);
        queued.output(&batch)?;
        self.leases.send(queued).await.map_err(|_| cancelled())?;
        self.batches.send(Ok(batch)).await.map_err(|_| cancelled())
    }

    /// Compact batches to the byte target; an indivisible row uses the query budget.
    pub(super) async fn send_bounded(
        &self,
        batch: RecordBatch,
        memory: Arc<WorkMemory>,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let input = Arc::new(memory.child("producer batch input")?);
        input.hold(&batch)?;
        let target = memory.batch_bytes();
        if batch.get_array_memory_size() <= target {
            return self.send(batch, memory).await;
        }
        let mut offset = 0;
        while offset < batch.num_rows() {
            let mut rows =
                (batch.num_rows() - offset).min(memory.ctx.session_config().batch_size());
            while rows > 1 && memory.slice_bytes(&batch, offset, rows)? > target {
                rows = rows.div_ceil(2);
            }
            loop {
                memory.check()?;
                let work = Arc::new(memory.child("expand output chunk")?);
                work.entries::<u32>(rows)?;
                let indices = (offset..offset + rows)
                    .map(u32::try_from)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|_| {
                        DataFusionError::Execution("expand batch exceeds row index range".into())
                    })?;
                let output = work.take(&batch, &UInt32Array::from(indices))?;
                if output.get_array_memory_size() <= target || rows == 1 {
                    self.send(output, work).await?;
                    offset += rows;
                    break;
                }
                rows = rows.div_ceil(2);
            }
        }
        Ok(())
    }
}

fn cancelled() -> DataFusionError {
    DataFusionError::Execution("query batch receiver closed".into())
}

/// Spawn one producer. The receiving stream owns cancellation and queued leases;
/// a consumed batch stays charged until the next poll transfers ownership onward.
pub(super) fn producer_stream<F>(
    schema: SchemaRef,
    memory: Arc<WorkMemory>,
    metrics: Option<&ExecutionPlanMetricsSet>,
    body: impl FnOnce(Arc<WorkMemory>, BatchSender) -> F + Send + 'static,
) -> SendableRecordBatchStream
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    let mut builder = RecordBatchReceiverStreamBuilder::new(Arc::clone(&schema), 2);
    let (leases, receiver) = mpsc::channel(2);
    let sender = BatchSender {
        batches: builder.tx(),
        leases,
    };
    let io = crate::instrumentation::capture_query_io_probes();
    let probes = crate::instrumentation::current_query_memory_probes();
    builder.spawn(async move {
        let work = async move {
            let work = memory.blocking(move |memory| body(memory, sender));
            match io {
                Some(probes) => crate::instrumentation::with_query_io_probes(probes, work).await,
                None => work.await,
            }
        };
        match probes {
            Some(probes) => crate::instrumentation::with_query_memory_probes(probes, work).await,
            None => work.await,
        }
    });
    let baseline = metrics.map(|metrics| BaselineMetrics::new(metrics, 0));
    let counts = metrics.map(|metrics| {
        datafusion::physical_plan::metrics::MetricBuilder::new(metrics).counter("output_batches", 0)
    });
    let stream = futures::stream::try_unfold(
        (builder.build(), receiver, None::<Arc<WorkMemory>>),
        move |(mut batches, mut leases, held)| {
            let baseline = baseline.clone();
            let counts = counts.clone();
            async move {
                drop(held);
                match batches.next().await {
                    Some(Ok(batch)) => {
                        let lease = leases.recv().await.ok_or_else(|| {
                            DataFusionError::Internal("producer batch has no memory lease".into())
                        })?;
                        if let Some(baseline) = &baseline {
                            baseline.record_output(batch.num_rows());
                        }
                        if let Some(counts) = &counts {
                            counts.add(1);
                        }
                        Ok(Some((batch, (batches, leases, Some(lease)))))
                    }
                    Some(Err(error)) => Err(error),
                    None => {
                        if let Some(baseline) = &baseline {
                            baseline.done();
                        }
                        Ok(None)
                    }
                }
            }
        },
    );
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::engine::context::QueryContext;
    use crate::instrumentation::{
        QueryMemoryProbes, with_query_memory_limit, with_query_memory_probes,
    };

    const ROWS: usize = 8_192;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    fn batch(value: i64) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(vec![value; ROWS]))],
        )
        .unwrap()
    }

    async fn released(probes: &QueryMemoryProbes) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while probes.reserved_bytes() != 0 || probes.active_blocking_work() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("producer tasks and queued reservations must stop with their stream");
    }

    /// Observes a producer blocked on a full queue and its reserved bytes; a case sees final rows only.
    #[tokio::test]
    async fn bounded_queue_preserves_each_batch_lease_with_a_shared_parent() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(
            probes.clone(),
            with_query_memory_limit(1024 * 1024, async {
                let context = QueryContext::new().unwrap();
                let memory =
                    Arc::new(WorkMemory::new(context.task_ctx(), "producer test").unwrap());
                let completed = Arc::new(AtomicUsize::new(0));
                let observed = Arc::clone(&completed);
                let (queued, ready) = tokio::sync::oneshot::channel();
                let mut stream =
                    producer_stream(schema(), memory, None, move |memory, sender| async move {
                        let mut queued = Some(queued);
                        for value in 0..4 {
                            sender.send(batch(value), Arc::clone(&memory)).await?;
                            observed.fetch_add(1, Ordering::SeqCst);
                            if value == 1 {
                                queued.take().unwrap().send(()).unwrap();
                            }
                        }
                        Ok(())
                    });
                tokio::time::timeout(Duration::from_secs(3), ready)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    completed.load(Ordering::SeqCst),
                    2,
                    "the third send must await capacity"
                );
                assert!(
                    probes.reserved_bytes() >= 2 * ROWS * size_of::<i64>(),
                    "two queued batches must retain separate buffer charges, even with one parent"
                );
                for expected in 0..4 {
                    let received = tokio::time::timeout(Duration::from_secs(3), stream.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert_eq!(received, batch(expected));
                }
                assert!(
                    tokio::time::timeout(Duration::from_secs(3), stream.next())
                        .await
                        .unwrap()
                        .is_none()
                );
                assert_eq!(completed.load(Ordering::SeqCst), 4);
                drop(stream);
                drop(context);
                released(&probes).await;
                assert!(
                    probes.blocking_started() > 0,
                    "producer work must remain observable"
                );
            }),
        )
        .await;
    }

    /// Reads reserved bytes while the consumer has not polled; a case cannot pause a consumer.
    #[tokio::test]
    async fn compaction_keeps_its_input_charged_while_the_queue_is_full() {
        let probes = QueryMemoryProbes::default();
        with_query_memory_probes(
            probes.clone(),
            with_query_memory_limit(4 * 1024 * 1024, async {
                let context = QueryContext::new().unwrap();
                let memory =
                    Arc::new(WorkMemory::new(context.task_ctx(), "compact producer").unwrap());
                let target = memory.batch_bytes();
                let total_rows = ROWS * 16;
                let mut stream =
                    producer_stream(schema(), memory, None, move |memory, sender| async move {
                        let input = RecordBatch::try_new(
                            schema(),
                            vec![Arc::new(Int64Array::from(vec![7; total_rows]))],
                        )?;
                        sender.send_bounded(input, memory).await
                    });
                tokio::time::timeout(Duration::from_secs(3), async {
                    while probes.reserved_bytes() < (total_rows + 2 * ROWS) * size_of::<i64>() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("the producer must retain its original input and queued compact batches");
                let mut rows = 0;
                while let Some(batch) = stream.next().await {
                    let batch = batch.unwrap();
                    assert!(batch.get_array_memory_size() <= target);
                    rows += batch.num_rows();
                }
                assert_eq!(rows, total_rows);
                drop(stream);
                drop(context);
                released(&probes).await;
            }),
        )
        .await;
    }

    /// Drops the stream while the producer is paused at a checkpoint; a case cannot drop a stream.
    #[tokio::test(flavor = "current_thread")]
    async fn dropping_stream_cancels_charged_producer_work() {
        let probes = QueryMemoryProbes::default();
        let pause = probes.pause_blocking_work();
        with_query_memory_probes(probes.clone(), async {
            let context = QueryContext::new().unwrap();
            let memory =
                Arc::new(WorkMemory::new(context.task_ctx(), "producer cancellation").unwrap());
            let stream =
                producer_stream(schema(), memory, None, move |memory, _sender| async move {
                    memory.grow(4_096)?;
                    memory.checkpoint()?;
                    std::future::pending::<()>().await;
                    Ok(())
                });
            tokio::time::timeout(Duration::from_secs(3), async {
                while !pause.entered() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("producer must reach its charged checkpoint");
            assert!(pause.is_paused());
            assert!(probes.active_blocking_work() > 0);
            assert!(probes.reserved_bytes() >= 4_096);
            drop(stream);
            drop(context);
            pause.release();
            released(&probes).await;
        })
        .await;
    }

    /// Injects a producer error and a panic; no query reaches either from a case.
    #[tokio::test]
    async fn producer_failures_reach_the_consumer_and_release_work() {
        for panic in [false, true] {
            let probes = QueryMemoryProbes::default();
            with_query_memory_probes(probes.clone(), async {
                let context = QueryContext::new().unwrap();
                let memory =
                    Arc::new(WorkMemory::new(context.task_ctx(), "producer failure").unwrap());
                let mut stream =
                    producer_stream(schema(), memory, None, move |memory, _sender| async move {
                        memory.grow(4_096)?;
                        assert!(!panic, "producer panic sentinel");
                        Err(DataFusionError::Execution(
                            "producer error sentinel".to_string(),
                        ))
                    });
                let error = tokio::time::timeout(Duration::from_secs(3), stream.next())
                    .await
                    .unwrap()
                    .expect("producer failure must reach the stream")
                    .unwrap_err();
                let expected = if panic {
                    "producer panic sentinel"
                } else {
                    "producer error sentinel"
                };
                assert!(error.to_string().contains(expected), "{error}");
                drop(stream);
                drop(context);
                released(&probes).await;
            })
            .await;
        }
    }
}
