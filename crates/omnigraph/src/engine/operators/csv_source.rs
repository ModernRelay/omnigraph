//! A load's leaf: the captured CSV object parsed into the target table's
//! typed delta by up to `partitions` readers, each claiming the next slice
//! the resolve pass cut, the slices reordered by the breaker above.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::catalog::Catalog;

use super::external;
use super::shape::{Source, work};
use crate::error::OmniError;
use crate::loader::LoadSliceExecuted;
use crate::loader::csv::{CsvLoadRequest, ResolvedCsv};
use crate::table_store::CapturedSource;

/// The `csv_source_rows` metric: the records the source held, counted once
/// the parse finished.
pub(crate) const CSV_SOURCE_ROWS: &str = "csv_source_rows";

/// The schema metadata key every produced batch carries its slice index
/// under, so the breaker restores source order whatever partition parsed it.
pub(crate) const CSV_SLICE_METADATA: &str = "omnigraph.load.slice";

pub(crate) struct CsvSourceExec {
    parse: Arc<Parse>,
    shape: Source,
    partitions: usize,
    metrics: ExecutionPlanMetricsSet,
}

/// What every partition of one execution parses from, shared with its
/// producers: the statement, the capture, the resolve pass once it ran, the
/// slice claim counter and what each slice measured.
struct Parse {
    request: CsvLoadRequest,
    capture: Arc<CapturedSource>,
    catalog: Arc<Catalog>,
    slice_bytes: usize,
    resolved: Mutex<Option<Arc<ResolvedCsv>>>,
    next_slice: AtomicUsize,
    stop: AtomicBool,
    started: OnceLock<Instant>,
    slices: Mutex<Vec<LoadSliceExecuted>>,
    failed: Mutex<Vec<(usize, usize)>>,
}

impl Parse {
    fn resolved(&self) -> crate::error::Result<Arc<ResolvedCsv>> {
        let mut resolved = self
            .resolved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(resolved) = resolved.as_ref() {
            return Ok(Arc::clone(resolved));
        }
        let fresh = Arc::new(ResolvedCsv::resolve(
            &self.catalog,
            &self.request,
            &self.capture.bytes,
            self.slice_bytes,
        )?);
        *resolved = Some(Arc::clone(&fresh));
        Ok(fresh)
    }
}

impl CsvSourceExec {
    /// `schema` is the target type's Arrow schema, the shape every normalized
    /// batch carries; `partitions` the output streams, the plan's
    /// `max_partitions`; `slice_bytes` the size a slice is cut at.
    pub(crate) fn new(
        request: CsvLoadRequest,
        capture: Arc<CapturedSource>,
        catalog: Arc<Catalog>,
        schema: SchemaRef,
        partitions: usize,
        slice_bytes: usize,
    ) -> Self {
        let partitions = partitions.max(1);
        Self {
            parse: Arc::new(Parse {
                request,
                capture,
                catalog,
                slice_bytes: slice_bytes.max(1),
                resolved: Mutex::new(None),
                next_slice: AtomicUsize::new(0),
                stop: AtomicBool::new(false),
                started: OnceLock::new(),
                slices: Mutex::new(Vec::new()),
                failed: Mutex::new(Vec::new()),
            }),
            shape: Source::with_partitions(schema, partitions),
            partitions,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub(crate) fn partitions(&self) -> usize {
        self.partitions
    }

    /// What each slice measured, in slice order, once the plan ran.
    pub(crate) fn executed_slices(&self) -> Vec<LoadSliceExecuted> {
        let mut slices = self
            .parse
            .slices
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        slices.sort_by_key(|slice| slice.slice);
        slices
    }

    /// The `(slice, partition)` pairs whose parse was refused, so the
    /// breaker answers the lowest slice's refusal whatever partition met it.
    pub(crate) fn failed_slices(&self) -> Vec<(usize, usize)> {
        self.parse
            .failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl fmt::Debug for CsvSourceExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CsvSourceExec")
            .field("source", &self.parse.request.source)
            .field("table", &self.parse.request.target.table_key())
            .field("partitions", &self.partitions)
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
        self.shape.properties()
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
        assert!(
            partition < self.partitions,
            "CsvSourceExec has {} partitions, partition {partition} executed",
            self.partitions
        );
        let memory = work(ctx, "CsvSourceExec", &self.metrics)?;
        memory.metric(CSV_SOURCE_ROWS, 0);
        let parse = Arc::clone(&self.parse);
        Ok(self.shape.partition_stream(
            partition,
            memory,
            &self.metrics,
            move |memory, sender| async move {
                let started = *parse.started.get_or_init(Instant::now);
                let resolved = parse.resolved().map_err(external)?;
                loop {
                    if parse.stop.load(Ordering::Acquire) {
                        break;
                    }
                    let slice = parse.next_slice.fetch_add(1, Ordering::AcqRel);
                    if slice >= resolved.slices().len() {
                        break;
                    }
                    let start_ns = elapsed_ns(started);
                    let decoding = memory.child("CSV decoding")?;
                    decoding.checkpoint()?;
                    let computing = Instant::now();
                    let parsed = resolved.parse_slice(
                        &parse.catalog,
                        &parse.request,
                        &parse.capture.bytes,
                        slice,
                        &mut |bytes| {
                            decoding
                                .grow(usize::try_from(bytes).unwrap_or(usize::MAX))
                                .map_err(|error| decoding.error(error))
                        },
                    );
                    let batches = match parsed {
                        Ok(batches) => batches,
                        Err(error) => {
                            parse.stop.store(true, Ordering::Release);
                            parse
                                .failed
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push((slice, partition));
                            return Err(external(error));
                        }
                    };
                    let elapsed_compute_ns = elapsed_ns(computing);
                    for batch in &batches {
                        memory.hold(batch)?;
                    }
                    drop(decoding);
                    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    memory.metric(CSV_SOURCE_ROWS, rows);
                    for batch in batches {
                        sender
                            .send(tag_slice(batch, slice)?, Arc::clone(&memory))
                            .await?;
                    }
                    parse
                        .slices
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(LoadSliceExecuted {
                            slice: u32::try_from(slice).unwrap_or(u32::MAX),
                            partition: u32::try_from(partition).unwrap_or(u32::MAX),
                            rows: rows as u64,
                            elapsed_compute_ns,
                            start_ns,
                            end_ns: elapsed_ns(started),
                        });
                }
                Ok(())
            },
        ))
    }
}

fn elapsed_ns(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// `batch` with its slice index in the schema metadata, the columns shared.
fn tag_slice(batch: RecordBatch, slice: usize) -> DfResult<RecordBatch> {
    let mut metadata: HashMap<String, String> = batch.schema().metadata().clone();
    metadata.insert(CSV_SLICE_METADATA.to_string(), slice.to_string());
    let schema = Arc::new(batch.schema().as_ref().clone().with_metadata(metadata));
    RecordBatch::try_new(schema, batch.columns().to_vec())
        .map_err(|error| external(OmniError::arrow_internal(error)))
}

/// The slice index a produced batch carries, 0 for a batch with none.
pub(crate) fn slice_of(batch: &RecordBatch) -> usize {
    batch
        .schema()
        .metadata()
        .get(CSV_SLICE_METADATA)
        .and_then(|slice| slice.parse().ok())
        .unwrap_or(0)
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
    use crate::loader::csv::{CSV_READ_BATCH_ROWS, prepare_csv_batches, target_arrow_schema};

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

    fn keys(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values()
            .to_vec()
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
        let exec = CsvSourceExec::new(request(), capture, catalog, schema, 1, usize::MAX);
        let batches: Vec<RecordBatch> = run(&exec)
            .await
            .into_iter()
            .map(|batch| batch.unwrap())
            .collect();
        assert_eq!(
            batches.iter().map(keys).collect::<Vec<_>>(),
            expected.batches.iter().map(keys).collect::<Vec<_>>()
        );
        assert!(batches.iter().all(|batch| slice_of(batch) == 0));
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
        assert_eq!(exec.executed_slices().len(), 1);
    }

    /// Three partitions over a capture cut into small slices: every slice is
    /// claimed once, its batches carry its index, and all partitions' rows
    /// sorted by slice are the loader's; a case never sees the claim.
    #[tokio::test]
    async fn partitions_claim_every_slice_once_and_tag_their_batches() {
        let mut source = String::from("key,score\n");
        for row in 0..1000 {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let catalog = catalog();
        let capture = capture(&source);
        let expected =
            prepare_csv_batches(&catalog, &request(), &capture, &mut |_| Ok(())).unwrap();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = CsvSourceExec::new(request(), capture, catalog, schema, 3, 512);
        let (_pool, ctx) = fixtures::context(64 << 20, CSV_READ_BATCH_ROWS);
        let streams: Vec<_> = (0..3)
            .map(|partition| exec.execute(partition, Arc::clone(&ctx)).unwrap())
            .collect();
        let outputs = futures::future::join_all(
            streams
                .into_iter()
                .map(|stream| stream.collect::<Vec<DfResult<RecordBatch>>>()),
        )
        .await;
        let mut tagged: Vec<(usize, RecordBatch)> = outputs
            .into_iter()
            .flatten()
            .map(|batch| batch.unwrap())
            .map(|batch| (slice_of(&batch), batch))
            .collect();
        tagged.sort_by_key(|(slice, _)| *slice);
        let keys_in_order: Vec<i64> = tagged.iter().flat_map(|(_, batch)| keys(batch)).collect();
        let expected_keys: Vec<i64> = expected.batches.iter().flat_map(keys).collect();
        assert_eq!(keys_in_order, expected_keys);

        let slices = exec.executed_slices();
        assert!(slices.len() > 3, "{} slices", slices.len());
        assert_eq!(
            slices.iter().map(|slice| slice.slice).collect::<Vec<_>>(),
            (0..slices.len() as u32).collect::<Vec<_>>()
        );
        assert!(slices.iter().all(|slice| slice.partition < 3));
        assert_eq!(
            slices.iter().map(|slice| slice.rows).sum::<u64>(),
            expected.rows as u64
        );
        assert!(
            slices
                .iter()
                .all(|slice| slice.start_ns <= slice.end_ns && slice.rows > 0)
        );
        assert!(exec.failed_slices().is_empty());
    }

    /// A refusal the parse raises reaches the consumer as the loader's own
    /// error text, through the producer.
    #[tokio::test]
    async fn parse_refusals_reach_the_stream_unchanged() {
        let catalog = catalog();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = CsvSourceExec::new(
            request(),
            capture("key,score\n1,\n"),
            catalog,
            schema,
            1,
            usize::MAX,
        );
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
        assert_eq!(exec.failed_slices(), vec![(0, 0)]);
    }

    /// A refused slice stops the claim loop: the partitions in flight finish
    /// their slice and no later slice is parsed, so the lowest refused slice
    /// is always among the parsed ones.
    #[tokio::test]
    async fn a_refused_slice_stops_the_claims() {
        let mut source = String::from("key,score\n");
        for row in 0..200 {
            if row == 20 {
                writeln!(&mut source, "{row},").unwrap();
            } else {
                writeln!(&mut source, "{row},1.5").unwrap();
            }
        }
        let catalog = catalog();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = CsvSourceExec::new(request(), capture(&source), catalog, schema, 1, 64);
        let outputs = run(&exec).await;
        let error = outputs
            .iter()
            .find_map(|output| output.as_ref().err())
            .expect("the bad record refuses the load");
        assert!(
            error.to_string().contains("record 21 has an empty field"),
            "{error}"
        );
        let failed = exec.failed_slices();
        assert_eq!(failed.len(), 1);
        let parsed = exec.executed_slices();
        assert!(
            parsed
                .iter()
                .all(|slice| (slice.slice as usize) < failed[0].0)
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
            let exec = CsvSourceExec::new(request(), captured, catalog, schema, 1, usize::MAX);
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
