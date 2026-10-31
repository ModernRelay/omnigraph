//! A load's leaf: the probed CSV object parsed into the target table's typed
//! delta by `partitions` readers, each reading its own byte range of the
//! source by pieces and cutting it on record ends, the slices reordered by
//! the breaker above. Nothing reads the source before the partitions run.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
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
use super::load_budget::ShareClass;
use super::memory::{WorkMemory, batch_bytes};
use super::shape::Source;
use crate::error::OmniError;
use crate::loader::LoadSliceExecuted;
use crate::loader::csv::{
    CSV_HEAD_MIN_BYTES, CsvBytes, CsvLoadRequest, NormalizeFailure, NormalizeStep, RangeScanner,
    ResolvedCsv, SliceRefusal,
};

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

/// What a batch's validation facts cost, charged on `facts` by the producer
/// in its sweeper's stead; the `usize` is the partition's rows probed so far.
pub(crate) type FactsAdmission =
    Box<dyn Fn(&WorkMemory, &RecordBatch, &mut usize) -> DfResult<()> + Send + Sync>;

/// What every partition of one execution shares: the statement, the probed
/// source, the header resolved once, each partition's range bounds and what
/// each slice measured.
struct Parse {
    request: CsvLoadRequest,
    bytes: Arc<CsvBytes>,
    catalog: Arc<Catalog>,
    slice_bytes: usize,
    partitions: usize,
    resolved: tokio::sync::OnceCell<Arc<ResolvedCsv>>,
    stop: AtomicBool,
    refused: Arc<AtomicBool>,
    started: OnceLock<Instant>,
    /// The rows of partition 0's first slice, set before that slice's
    /// batches leave the partition: the row estimate's sample, the seed's.
    first_slice_rows: OnceLock<u64>,
    /// The consumer's facts admission, installed before any partition runs,
    /// and the charges made ahead by slice key and batch ordinal, for the
    /// sweeper to take and keep to the judgment.
    admit_facts: OnceLock<FactsAdmission>,
    facts_admissions: Mutex<HashMap<(usize, usize), WorkMemory>>,
    slices: Mutex<Vec<LoadSliceExecuted>>,
    failed: Mutex<Vec<FailedSlice>>,
    bounds: Mutex<Vec<Option<(u64, u64)>>>,
    rows: AtomicU64,
    finished: AtomicUsize,
}

/// A slice a partition refused and, for a normalization refusal, where it
/// ranks; the breaker names the best-ranked one's refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FailedSlice {
    pub(crate) slice: usize,
    pub(crate) partition: usize,
    pub(crate) normalize: Option<NormalizeStep>,
}

impl FailedSlice {
    /// The order the breaker names refusals in: a parse refusal first, the
    /// lowest slice's, then a normalization refusal by its step and record;
    /// the order the whole-capture path meets them in.
    pub(crate) fn rank(&self) -> (bool, usize, usize) {
        match self.normalize {
            None => (false, self.slice, 0),
            Some(at) => (true, at.step, at.record),
        }
    }
}

/// The order key of a slice across partitions: the partition's ranges are
/// in source order and so are its slices, so the key sorts by source
/// position.
fn slice_key(partition: usize, index: usize) -> usize {
    (partition << 32) | (index & 0xFFFF_FFFF)
}

impl Parse {
    fn record_failure(&self, failed: FailedSlice) {
        self.failed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(failed);
    }

    /// Stop every partition on `error`; with more than one partition the
    /// loader reruns the plan single-partitioned, whose refusal numbers its
    /// records from the source.
    fn fail(&self, error: OmniError) -> datafusion::common::DataFusionError {
        self.stop.store(true, Ordering::Release);
        if self.partitions > 1 {
            self.refused.store(true, Ordering::Release);
        }
        external(error)
    }

    /// The source bytes one partition holds, the head range the header is
    /// read from; admitted by `execute` before any partition is polled, so a
    /// refusal lands on the statement's task, not in a racing worker.
    fn source_admission(&self) -> usize {
        let head_end = (self.slice_bytes as u64)
            .max(CSV_HEAD_MIN_BYTES)
            .min(self.bytes.size());
        usize::try_from(head_end).unwrap_or(usize::MAX)
    }

    /// The header resolved from the source's first bytes, read once, within
    /// the partition's source admission.
    async fn resolved(&self) -> crate::error::Result<Arc<ResolvedCsv>> {
        self.resolved
            .get_or_try_init(|| async {
                let size = self.bytes.size();
                let head_end = self.source_admission() as u64;
                let head = self.bytes.read(&self.request, 0..head_end).await?;
                Ok(Arc::new(ResolvedCsv::resolve(
                    &self.catalog,
                    &self.request,
                    &head,
                    size,
                )?))
            })
            .await
            .cloned()
    }

    /// Partition `partition`'s share of the records: equal byte widths from
    /// `records_start`, the last partition to the source's end.
    fn range(&self, partition: usize, records_start: u64) -> (u64, u64) {
        let size = self.bytes.size();
        let width = size
            .saturating_sub(records_start)
            .div_ceil(self.partitions as u64)
            .max(1);
        let start = records_start
            .saturating_add(width.saturating_mul(partition as u64))
            .min(size);
        let end = if partition + 1 == self.partitions {
            size
        } else {
            records_start
                .saturating_add(width.saturating_mul(partition as u64 + 1))
                .min(size)
        };
        (start, end)
    }

    /// The first pair of neighbouring partitions whose ranges do not meet at
    /// one record end: `(partition, its end, the next one's start)`.
    fn boundary_mismatch(&self) -> Option<(usize, u64, u64)> {
        let bounds = self
            .bounds
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        bounds.windows(2).enumerate().find_map(|(partition, pair)| {
            let (_, end) = pair[0]?;
            let (start, _) = pair[1]?;
            (end != start).then_some((partition, end, start))
        })
    }
}

impl CsvSourceExec {
    /// `schema` is the target type's Arrow schema, the shape every normalized
    /// batch carries; `partitions` the output streams, the plan's
    /// `max_partitions`; `slice_bytes` the size a slice is cut at and a
    /// piece of the source is read at; `refused` is set when a run of more
    /// than one partition refuses, so the loader reruns it single-partitioned.
    pub(crate) fn new(
        request: CsvLoadRequest,
        bytes: Arc<CsvBytes>,
        catalog: Arc<Catalog>,
        schema: SchemaRef,
        partitions: usize,
        slice_bytes: usize,
        refused: Arc<AtomicBool>,
    ) -> Self {
        let partitions = partitions.max(1);
        Self {
            parse: Arc::new(Parse {
                request,
                bytes,
                catalog,
                slice_bytes: slice_bytes.max(1),
                partitions,
                resolved: tokio::sync::OnceCell::new(),
                stop: AtomicBool::new(false),
                refused,
                started: OnceLock::new(),
                first_slice_rows: OnceLock::new(),
                admit_facts: OnceLock::new(),
                facts_admissions: Mutex::new(HashMap::new()),
                slices: Mutex::new(Vec::new()),
                failed: Mutex::new(Vec::new()),
                bounds: Mutex::new(vec![None; partitions]),
                rows: AtomicU64::new(0),
                finished: AtomicUsize::new(0),
            }),
            shape: Source::with_partitions(schema, partitions),
            partitions,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub(crate) fn partitions(&self) -> usize {
        self.partitions
    }

    /// The source bytes one partition holds before it can cut a slice: its
    /// admission and one slice of pieces; the floor its load share must
    /// reach at admission.
    pub(crate) fn admission_floor(&self) -> usize {
        let slice = (self.parse.slice_bytes as u64).min(self.parse.bytes.size());
        self.parse
            .source_admission()
            .saturating_add(usize::try_from(slice).unwrap_or(usize::MAX))
    }

    /// Mark a run of more than one partition refused by its sweep, so the
    /// loader reruns it single-partitioned as it does for a parse refusal.
    pub(crate) fn mark_refused(&self) {
        if self.partitions > 1 {
            self.parse.refused.store(true, Ordering::Release);
        }
    }

    /// Install what a batch's validation facts cost, charged by the producer
    /// before the batch leaves its partition; the first install holds.
    pub(crate) fn admit_facts_ahead(&self, admit: FactsAdmission) {
        let _ = self.parse.admit_facts.set(admit);
    }

    /// The facts charge the producer made ahead for the batch at `ordinal`
    /// of `slice`, the sweeper's to keep to the judgment.
    pub(crate) fn take_facts_admission(&self, slice: usize, ordinal: usize) -> Option<WorkMemory> {
        self.parse
            .facts_admissions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(slice, ordinal))
    }

    /// Whether a run of more than one partition refused, marking the rerun.
    #[cfg(test)]
    pub(crate) fn refused(&self) -> bool {
        self.parse.refused.load(Ordering::Acquire)
    }

    /// What each slice measured, in source order and numbered so, once the
    /// plan ran.
    pub(crate) fn executed_slices(&self) -> Vec<LoadSliceExecuted> {
        let mut slices = self
            .parse
            .slices
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        slices.sort_by_key(|slice| (slice.partition, slice.slice));
        for (index, slice) in slices.iter_mut().enumerate() {
            slice.slice = u32::try_from(index).unwrap_or(u32::MAX);
        }
        slices
    }

    /// The source's rows, estimated from the first slice any partition
    /// parsed: its rows per slice byte over the records' bytes; `None`
    /// until a slice parsed. The sample is set before the slice's batches
    /// leave the partition, so a consumer holding a batch reads `Some`. A
    /// short last slice under-estimates, which only a source smaller than
    /// its partitions' slices can meet.
    pub(crate) fn rows_estimate(&self) -> Option<u64> {
        let records_start = self.parse.resolved.get()?.records_start();
        let rows = *self.parse.first_slice_rows.get()?;
        let records_bytes = self.parse.bytes.size().saturating_sub(records_start);
        Some(rows.saturating_mul(records_bytes) / self.parse.slice_bytes as u64)
    }

    /// Every refused slice with the partition that met it, one per failed
    /// partition (its best-ranked), so the breaker answers the best-ranked
    /// refusal whatever partition met it.
    pub(crate) fn failed_slices(&self) -> Vec<FailedSlice> {
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
        let mut memory = WorkMemory::new(ctx, "CsvSourceExec")?;
        memory.set_metrics(self.metrics.clone());
        let memory = Arc::new(memory.in_share(ShareClass::Partition(partition)));
        memory.metric(CSV_SOURCE_ROWS, 0);
        let parse = Arc::clone(&self.parse);
        let source_bytes = memory.child("CSV source bytes")?;
        let admitted = parse.source_admission();
        source_bytes
            .grow(admitted)
            .map_err(|error| parse.fail(source_bytes.error(error)))?;
        Ok(self.shape.partition_stream(
            partition,
            memory,
            &self.metrics,
            move |memory, sender| async move {
                let started = *parse.started.get_or_init(Instant::now);
                if parse.stop.load(Ordering::Acquire) {
                    return Ok(());
                }
                let resolved = match parse.resolved().await {
                    Ok(resolved) => resolved,
                    Err(error) => return Err(parse.fail(error)),
                };
                let (range_start, range_end) = parse.range(partition, resolved.records_start());
                let mut scanner = RangeScanner::new(
                    Arc::clone(&parse.bytes),
                    parse.request.delimiter,
                    range_start,
                    range_end,
                    parse.slice_bytes,
                    partition > 0,
                );
                let mut failed: Option<(usize, NormalizeFailure)> = None;
                let mut first_record = 1_usize;
                let mut index = 0_usize;
                let mut partition_rows = 0_u64;
                let mut probed_rows = 0_usize;
                loop {
                    if parse.stop.load(Ordering::Acquire) {
                        break;
                    }
                    let start_ns = elapsed_ns(started);
                    let mut admit_piece = |held: usize| {
                        source_bytes
                            .ensure(held)
                            .map_err(|error| source_bytes.error(error))
                    };
                    let slice = match scanner.next_slice(&parse.request, &mut admit_piece).await {
                        Ok(Some(slice)) => slice,
                        Ok(None) => break,
                        Err(error) => return Err(parse.fail(error)),
                    };
                    let key = slice_key(partition, index);
                    index += 1;
                    let decoding = memory.child("CSV decoding")?;
                    decoding.checkpoint()?;
                    let computing = Instant::now();
                    let parsed = resolved.parse_slice(
                        &parse.catalog,
                        &parse.request,
                        &slice.body,
                        slice.rows,
                        first_record,
                        &mut |bytes| {
                            decoding
                                .grow(usize::try_from(bytes).unwrap_or(usize::MAX))
                                .map_err(|error| decoding.error(error))
                        },
                    );
                    first_record += slice.rows;
                    let read_ns = slice.read_ns;
                    drop(slice);
                    source_bytes.shrink_to(scanner.buffered().max(admitted));
                    let batches = match parsed {
                        Ok(batches) => batches,
                        Err(SliceRefusal::Normalize(failure)) => {
                            if failed.as_ref().is_none_or(|(_, held)| failure.at < held.at) {
                                failed = Some((key, failure));
                            }
                            continue;
                        }
                        Err(SliceRefusal::Parse(error)) => {
                            parse.record_failure(FailedSlice {
                                slice: key,
                                partition,
                                normalize: None,
                            });
                            return Err(parse.fail(*error));
                        }
                    };
                    if failed.is_some() {
                        continue;
                    }
                    let elapsed_compute_ns = elapsed_ns(computing);
                    for batch in &batches {
                        memory
                            .hold(batch)
                            .map_err(|error| parse.fail(memory.error(error)))?;
                    }
                    drop(decoding);
                    for (ordinal, batch) in batches.iter().enumerate() {
                        let sink_copy = batch_bytes(batch);
                        memory
                            .grow(sink_copy)
                            .map_err(|error| parse.fail(memory.error(error)))?;
                        if let Some(admit) = parse.admit_facts.get() {
                            let facts = memory.child("load validation facts")?;
                            admit(&facts, batch, &mut probed_rows)
                                .map_err(|error| parse.fail(facts.error(error)))?;
                            parse
                                .facts_admissions
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .insert((key, ordinal), facts);
                        }
                    }
                    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    memory.metric(CSV_SOURCE_ROWS, rows);
                    partition_rows = partition_rows.saturating_add(rows as u64);
                    if partition == 0 {
                        let _ = parse.first_slice_rows.set(rows as u64);
                    }
                    for batch in batches {
                        sender
                            .send(tag_slice(batch, key)?, Arc::clone(&memory))
                            .await?;
                    }
                    parse
                        .slices
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(LoadSliceExecuted {
                            slice: u32::try_from(index - 1).unwrap_or(u32::MAX),
                            partition: u32::try_from(partition).unwrap_or(u32::MAX),
                            rows: rows as u64,
                            elapsed_compute_ns,
                            start_ns,
                            end_ns: elapsed_ns(started),
                            read_ns,
                        });
                }
                parse
                    .bounds
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)[partition] =
                    Some((scanner.start(), scanner.end().unwrap_or(scanner.start())));
                parse.rows.fetch_add(partition_rows, Ordering::AcqRel);
                if let Some((key, failure)) = failed {
                    parse.record_failure(FailedSlice {
                        slice: key,
                        partition,
                        normalize: Some(failure.at),
                    });
                    return Err(parse.fail(*failure.error));
                }
                let last = parse.finished.fetch_add(1, Ordering::AcqRel) + 1 == parse.partitions;
                if last && !parse.stop.load(Ordering::Acquire) {
                    if let Some((before, end, start)) = parse.boundary_mismatch() {
                        return Err(parse.fail(OmniError::manifest(format!(
                            "load csv into {}: partition {before} ended at byte {end} and partition {} began at byte {start}; a range boundary fell inside a quoted field",
                            resolved.table_key(),
                            before + 1
                        ))));
                    }
                    if parse.rows.load(Ordering::Acquire) == 0 {
                        return Err(parse.fail(OmniError::manifest(format!(
                            "load csv into {}: the source holds no records",
                            resolved.table_key()
                        ))));
                    }
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
    use crate::table_store::CapturedSource;

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

    fn bytes(source: &str) -> Arc<CsvBytes> {
        Arc::new(CsvBytes::Memory(Arc::from(source.as_bytes())))
    }

    fn capture(source: &str) -> CapturedSource {
        CapturedSource {
            bytes: Arc::from(source.as_bytes()),
            admission: None,
            size: source.len() as u64,
            e_tag: None,
            version: None,
        }
    }

    fn exec(source: &str, partitions: usize, slice_bytes: usize) -> CsvSourceExec {
        let catalog = catalog();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        CsvSourceExec::new(
            request(),
            bytes(source),
            catalog,
            schema,
            partitions,
            slice_bytes,
            Arc::default(),
        )
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
        let expected =
            prepare_csv_batches(&catalog(), &request(), &capture(&source), &mut |_| Ok(()))
                .unwrap();
        let exec = exec(&source, 1, usize::MAX);
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

    /// A consumer holding a partition's first batch reads the row estimate:
    /// its sample is set before the batches are sent, an order between two
    /// threads that GQT cannot observe (a DST replay saw it both ways).
    #[tokio::test(flavor = "current_thread")]
    async fn the_row_estimate_precedes_the_first_batch() {
        let exec = exec("key,score\n1,1.5\n2,2.5\n", 1, usize::MAX);
        let (_pool, ctx) = fixtures::context(64 << 20, CSV_READ_BATCH_ROWS);
        let mut stream = exec.execute(0, ctx).unwrap();
        let first = stream
            .next()
            .await
            .expect("the source holds records")
            .unwrap();
        assert_eq!(first.num_rows(), 2);
        assert!(exec.rows_estimate().is_some());
    }

    /// Every partition's output, each collected to its end.
    async fn run_all(exec: &CsvSourceExec) -> Vec<Vec<DfResult<RecordBatch>>> {
        let (_pool, ctx) = fixtures::context(64 << 20, CSV_READ_BATCH_ROWS);
        let streams: Vec<_> = (0..exec.partitions())
            .map(|partition| exec.execute(partition, Arc::clone(&ctx)).unwrap())
            .collect();
        futures::future::join_all(
            streams
                .into_iter()
                .map(|stream| stream.collect::<Vec<DfResult<RecordBatch>>>()),
        )
        .await
    }

    /// Three partitions reading their ranges in 512-byte pieces meet at record
    /// ends, number their slices from the source and, sorted by slice key,
    /// yield the loader's rows; a case cannot cut a source below 8 MiB.
    #[tokio::test]
    async fn partitions_read_their_ranges_and_meet_at_record_ends() {
        let mut source = String::from("key,score\n");
        for row in 0..1000 {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let expected =
            prepare_csv_batches(&catalog(), &request(), &capture(&source), &mut |_| Ok(()))
                .unwrap();
        let exec = exec(&source, 3, 512);
        let outputs = run_all(&exec).await;
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
        assert!((0..3).all(|partition| slices.iter().any(|slice| slice.partition == partition)));
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
        assert!(exec.parse.boundary_mismatch().is_none());
        let bounds = exec.parse.bounds.lock().unwrap().clone();
        assert_eq!(bounds[0].unwrap().0, 10);
        assert_eq!(bounds[2].unwrap().1, source.len() as u64);
    }

    /// Range starts inside quoted line breaks make a partition begin
    /// mid-record or skip one: the run refuses (a parse error or the boundary
    /// disagreement) and marks itself for the single-partition rerun.
    #[tokio::test]
    async fn a_range_start_inside_a_quoted_field_is_refused_for_the_rerun() {
        let catalog = Arc::new(
            build_catalog(
                &parse_schema("node Person {\n  key: I64 @key\n  name: String\n}\n").unwrap(),
            )
            .unwrap(),
        );
        let mut source = String::from("key,name\n");
        for row in 0..40 {
            writeln!(&mut source, "{row},\"a\nb\"").unwrap();
        }
        let request = CsvLoadRequest {
            target: LoadTarget::Node("Person".to_string()),
            ..self::request()
        };
        let schema = target_arrow_schema(&catalog, &request.target).unwrap();
        let refused = Arc::new(AtomicBool::new(false));
        let exec = CsvSourceExec::new(
            request,
            bytes(&source),
            catalog,
            schema,
            4,
            16,
            Arc::clone(&refused),
        );
        let outputs = run_all(&exec).await;
        let errors: Vec<String> = outputs
            .iter()
            .flatten()
            .filter_map(|output| output.as_ref().err())
            .map(ToString::to_string)
            .collect();
        assert!(!errors.is_empty(), "{outputs:?}");
        assert!(refused.load(Ordering::Acquire));
    }

    /// A single partition numbers every refusal from the source, so the
    /// loader's rerun reports the record a case pins.
    #[tokio::test]
    async fn one_partition_numbers_its_refusal_from_the_source() {
        let mut source = String::from("key,score\n");
        for row in 0..200 {
            if row == 150 {
                writeln!(&mut source, "{row},").unwrap();
            } else {
                writeln!(&mut source, "{row},1.5").unwrap();
            }
        }
        let refused = Arc::new(AtomicBool::new(false));
        let catalog = catalog();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = CsvSourceExec::new(
            request(),
            bytes(&source),
            catalog,
            schema,
            1,
            64,
            Arc::clone(&refused),
        );
        let outputs = run(&exec).await;
        let error = outputs
            .iter()
            .find_map(|output| output.as_ref().err())
            .expect("the bad record refuses the load");
        assert!(
            error.to_string().contains("record 151 has an empty field"),
            "{error}"
        );
        assert!(!refused.load(Ordering::Acquire));
    }

    /// A header alone, or a source of no records, is refused by the last
    /// partition to finish, in the loader's words.
    #[tokio::test]
    async fn a_source_without_records_is_refused() {
        for partitions in [1, 3] {
            let exec = exec("key,score\n", partitions, 8);
            let outputs = run_all(&exec).await;
            let errors: Vec<String> = outputs
                .iter()
                .flatten()
                .filter_map(|output| output.as_ref().err())
                .map(ToString::to_string)
                .collect();
            assert_eq!(errors.len(), 1, "{errors:?}");
            assert!(
                errors[0].contains("load csv into node:Person: the source holds no records"),
                "{errors:?}"
            );
        }
    }

    /// A refusal the parse raises reaches the consumer as the loader's own
    /// error text, through the producer.
    #[tokio::test]
    async fn parse_refusals_reach_the_stream_unchanged() {
        let exec = exec("key,score\n1,\n", 1, usize::MAX);
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
        assert_eq!(
            exec.failed_slices(),
            vec![FailedSlice {
                slice: 0,
                partition: 0,
                normalize: Some(NormalizeStep { step: 5, record: 1 }),
            }]
        );
    }

    /// A normalization refusal holds nothing after it: the partition keeps
    /// normalizing the later slices for an earlier-ranked refusal, but
    /// records and sends none of them.
    #[tokio::test]
    async fn a_refused_slice_records_nothing_after_it() {
        let mut source = String::from("key,score\n");
        for row in 0..200 {
            if row == 20 {
                writeln!(&mut source, "{row},").unwrap();
            } else {
                writeln!(&mut source, "{row},1.5").unwrap();
            }
        }
        let exec = exec(&source, 1, 64);
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
                .all(|slice| (slice.slice as usize) < failed[0].slice)
        );
        assert_eq!(outputs.len(), parsed.len() + 1, "{outputs:?}");
    }

    /// `execute` charges every partition's source admission before any poll,
    /// refusing there and marking the single-partition rerun; GQT sees one
    /// `actual` per replay but cannot observe a reservation before a poll.
    #[tokio::test]
    async fn partitions_admit_their_source_before_the_first_poll() {
        let mut source = String::from("key,score\n");
        for row in 0..3000 {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let admission = source.len().min(CSV_HEAD_MIN_BYTES as usize);
        let exec = exec(&source, 3, 512);
        let (pool, ctx) = fixtures::context(3 * admission - 1, CSV_READ_BATCH_ROWS);
        let _first = exec.execute(0, Arc::clone(&ctx)).unwrap();
        let _second = exec.execute(1, Arc::clone(&ctx)).unwrap();
        assert!(pool.reserved() >= 2 * admission, "{}", pool.reserved());
        let refused = exec
            .execute(2, ctx)
            .err()
            .expect("the third admission exceeds the pool");
        assert!(
            refused.to_string().contains("query_memory_bytes"),
            "{refused}"
        );
        assert!(exec.refused());
    }

    /// Drops the load caller while its blocking CSV producer is paused at a
    /// checkpoint: the worker settles only once released and its reservations
    /// go with it; GQT cannot pause a worker or observe a reservation.
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_csv_producer_settles_and_releases_its_admission() {
        use crate::engine::LoadMemory;
        use crate::instrumentation::{QueryMemoryProbes, with_query_memory_probes};
        use futures::FutureExt as _;
        use std::time::Duration;

        let probes = QueryMemoryProbes::default();
        let pause = probes.pause_blocking_work();
        with_query_memory_probes(probes.clone(), async {
            let memory = LoadMemory::new(64 << 20).unwrap();
            let settlement = memory.context().owned_workers();
            let source = "key,score\n1,1.5\n";
            let exec = exec(source, 1, usize::MAX);
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
            drop(memory);
            assert!(settlement.wait().now_or_never().is_none());

            pause.release();
            tokio::time::timeout(Duration::from_secs(3), settlement.wait())
                .await
                .expect("cancelled CSV producer must settle after release");
            assert_eq!(probes.active_blocking_work(), 0);
            assert_eq!(probes.reserved_bytes(), 0);
        })
        .await;
    }
}
