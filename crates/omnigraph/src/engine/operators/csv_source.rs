//! A load's leaf: the captured CSV object parsed into the target table's
//! typed delta, in source order, by one reader.

use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::catalog::Catalog;

use super::memory::WorkMemory;
use super::producer::producer_stream;
use super::{breaker_properties, external, polled};
use crate::loader::csv::{CsvLoadRequest, prepare_csv_batches};
use crate::table_store::CapturedSource;

/// The `csv_source_rows` metric: the records the source held, counted once
/// the parse finished.
pub(crate) const CSV_SOURCE_ROWS: &str = "csv_source_rows";

pub(crate) struct CsvSourceExec {
    parse: Arc<Parse>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

/// What one execution parses, shared with its producer.
struct Parse {
    request: CsvLoadRequest,
    capture: Arc<CapturedSource>,
    catalog: Arc<Catalog>,
}

impl CsvSourceExec {
    /// `schema` is the target type's Arrow schema, the shape every normalized
    /// batch carries.
    pub(crate) fn new(
        request: CsvLoadRequest,
        capture: Arc<CapturedSource>,
        catalog: Arc<Catalog>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            parse: Arc::new(Parse {
                request,
                capture,
                catalog,
            }),
            properties: breaker_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for CsvSourceExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvSourceExec")
            .field("source", &self.parse.request.source)
            .field("table", &self.parse.request.target.table_key())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CsvSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CsvSourceExec: {} -> {}",
            self.parse.request.source,
            self.parse.request.target.table_key()
        )
    }
}

impl ExecutionPlan for CsvSourceExec {
    fn name(&self) -> &str {
        "CsvSourceExec"
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
        assert!(children.is_empty(), "CsvSourceExec has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CsvSourceExec has one partition");
        let schema = self.schema();
        let mut work = WorkMemory::new(ctx, "CsvSourceExec")?;
        work.set_metrics(self.metrics.clone());
        work.metric(CSV_SOURCE_ROWS, 0);
        let parse = Arc::clone(&self.parse);
        let stream = producer_stream(
            schema,
            Arc::new(work),
            Some(&self.metrics),
            move |memory, sender| async move {
                let decoding = memory.child("CSV decoding")?;
                decoding.checkpoint()?;
                let prepared = prepare_csv_batches(
                    &parse.catalog,
                    &parse.request,
                    &parse.capture,
                    &mut |bytes| {
                        decoding
                            .grow(usize::try_from(bytes).unwrap_or(usize::MAX))
                            .map_err(|error| decoding.error(error))
                    },
                )
                .map_err(external)?;
                for batch in &prepared.batches {
                    memory.hold(batch)?;
                }
                drop(decoding);
                memory.metric(CSV_SOURCE_ROWS, prepared.rows);
                for batch in prepared.batches {
                    sender.send(batch, Arc::clone(&memory)).await?;
                }
                Ok(())
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use arrow_array::RecordBatch;
    use futures::StreamExt;
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::query::ast::LoadTarget;
    use omnigraph_compiler::schema::parser::parse_schema;

    use super::super::fixtures;
    use super::*;
    use crate::loader::LoadMode;
    use crate::loader::csv::{CSV_READ_BATCH_ROWS, target_arrow_schema};

    fn request() -> CsvLoadRequest {
        CsvLoadRequest {
            source: "file:///input.csv".to_string(),
            target: LoadTarget::Node("Person".to_string()),
            mode: LoadMode::Overwrite,
            delimiter: b',',
            header: true,
            columns: None,
        }
    }

    fn capture(source: &str) -> Arc<CapturedSource> {
        Arc::new(CapturedSource {
            bytes: Arc::from(source.as_bytes()),
            admission: None,
            size: source.len() as u64,
            e_tag: None,
            version: None,
        })
    }

    fn catalog() -> Arc<Catalog> {
        Arc::new(
            build_catalog(
                &parse_schema("node Person {\n  key: I64 @key\n  score: F64\n}\n").unwrap(),
            )
            .unwrap(),
        )
    }

    async fn run(exec: &CsvSourceExec) -> Vec<DfResult<RecordBatch>> {
        let (_pool, ctx) = fixtures::context(64 << 20, CSV_READ_BATCH_ROWS);
        exec.execute(0, ctx).unwrap().collect().await
    }

    /// The operator's stream against the loader's own parse of the same
    /// capture, across a reader-batch boundary; a case sees only the rows
    /// a load leaves, never the batches an operator hands on.
    #[tokio::test]
    async fn streams_the_prepared_batches_in_order_and_counts_the_rows() {
        let mut source = String::from("key,score\n");
        for row in 0..=CSV_READ_BATCH_ROWS {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let catalog = catalog();
        let capture = capture(&source);
        let expected =
            prepare_csv_batches(&catalog, &request(), &capture, &mut |_| Ok(())).unwrap();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = CsvSourceExec::new(request(), capture, catalog, schema);
        let batches: Vec<RecordBatch> = run(&exec)
            .await
            .into_iter()
            .map(|batch| batch.unwrap())
            .collect();
        assert_eq!(batches, expected.batches);
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            [CSV_READ_BATCH_ROWS, 1]
        );
        let rows = exec
            .metrics()
            .and_then(|metrics| metrics.sum_by_name(CSV_SOURCE_ROWS))
            .map(|metric| metric.as_usize());
        assert_eq!(rows, Some(expected.rows));
    }

    /// A refusal the parse raises reaches the consumer as the loader's own
    /// error text, through the producer.
    #[tokio::test]
    async fn parse_refusals_reach_the_stream_unchanged() {
        let catalog = catalog();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = CsvSourceExec::new(request(), capture("key,score\n1,\n"), catalog, schema);
        let outputs = run(&exec).await;
        let [Err(error)] = &outputs[..] else {
            panic!("one error and no batch, found {outputs:?}");
        };
        assert!(
            error.to_string().contains(
                "load csv into node:Person: record 1 has an empty field for non-nullable property 'score'"
            ),
            "{error}"
        );
    }
    /// Drops the load caller while its blocking CSV producer owns captured bytes;
    /// GQT cannot pause a worker or observe the lifetime of a reservation.
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_csv_producer_keeps_the_source_admission_until_it_settles() {
        use crate::engine::LoadMemory;
        use crate::instrumentation::{QueryMemoryProbes, with_query_memory_probes};
        use futures::FutureExt as _;
        use std::time::Duration;

        let probes = QueryMemoryProbes::default();
        let pause = probes.pause_blocking_work();
        with_query_memory_probes(probes.clone(), async {
            let memory = LoadMemory::new(64 << 20).unwrap();
            let settlement = memory.context().owned_workers();
            let source = b"key,score\n1,1.5\n";
            let charge = Arc::new(memory.charge("load source").unwrap());
            charge.grow(source.len() as u64).unwrap();
            let weak_charge = Arc::downgrade(&charge);
            let captured = Arc::new(CapturedSource {
                bytes: Arc::from(source.as_slice()),
                admission: Some(charge.clone()),
                size: source.len() as u64,
                e_tag: None,
                version: None,
            });
            let catalog = catalog();
            let schema = target_arrow_schema(&catalog, &request().target).unwrap();
            let exec = CsvSourceExec::new(request(), captured, catalog, schema);
            let stream = exec.execute(0, memory.context().task_ctx()).unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while !pause.entered() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("CSV producer must reach its charged checkpoint");
            assert!(pause.is_paused());
            assert!(probes.active_blocking_work() > 0);

            drop(exec);
            drop(stream);
            drop(charge);
            drop(memory);
            assert!(weak_charge.upgrade().is_some());
            assert!(probes.reserved_bytes() >= source.len());
            assert!(settlement.wait().now_or_never().is_none());

            pause.release();
            tokio::time::timeout(Duration::from_secs(3), settlement.wait())
                .await
                .expect("cancelled CSV producer must settle after release");
            assert!(weak_charge.upgrade().is_none());
            assert_eq!(probes.active_blocking_work(), 0);
            assert_eq!(probes.reserved_bytes(), 0);
        })
        .await;
    }
}
