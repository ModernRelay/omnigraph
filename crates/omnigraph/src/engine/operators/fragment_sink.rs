//! A load's write: the validated delta staged as Lance fragments of the
//! target under the open transaction, never committed here, written while
//! the delta drains: each source partition's rows fill one chunk at a time
//! and a full chunk goes to a Lance writer as the next rows arrive. DataFusion's
//! `DataSinkExec` hosts the sink and emits the one `count` row; the driver
//! takes the staged transaction from the slot and commits it after the plan.
//! A refusal reaching the sink deletes every fragment already written.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::Result as DfResult;
use datafusion::datasource::sink::DataSink;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;

use super::csv_source::slice_of;
use super::external;
use super::load_budget::ShareClass;
use super::memory::WorkMemory;
use crate::db::MutationOpKind;
use crate::engine::LoadWrite;
use crate::error::OmniError;
use crate::exec::staging::MutationStaging;
use crate::loader::STAGED_WRITE_INPUT_BYTES_RESOURCE;
use crate::table_store::{OverwriteChunk, overwrite_chunk_rows, write_batch_bytes};

/// The `staged_rows` metric: the rows the sink staged.
pub(crate) const STAGED_ROWS: &str = "staged_rows";

pub(crate) struct LanceFragmentSink {
    table: String,
    schema: SchemaRef,
    write: LoadWrite,
    /// The plan's bound on one staged-write input, step 2's rule.
    max_input_bytes: u64,
    metrics: ExecutionPlanMetricsSet,
}

impl LanceFragmentSink {
    /// `schema` is the input's: the target's typed delta, system columns
    /// included, which `DataSinkExec` enforces non-null columns against.
    pub(crate) fn new(
        table: String,
        schema: SchemaRef,
        write: LoadWrite,
        max_input_bytes: u64,
    ) -> Self {
        Self {
            table,
            schema,
            write,
            max_input_bytes,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for LanceFragmentSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceFragmentSink")
            .field("table", &self.table)
            .field("max_input_bytes", &self.max_input_bytes)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LanceFragmentSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanceFragmentSink: {} overwrite", self.table)
    }
}

#[async_trait]
impl DataSink for LanceFragmentSink {
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut work =
            WorkMemory::new(Arc::clone(context), "LanceFragmentSink")?.in_share(ShareClass::Sink);
        work.set_metrics(self.metrics.clone());
        work.metric(STAGED_ROWS, 0);
        MetricBuilder::new(&self.metrics)
            .start_timestamp(0)
            .record();
        let baseline = BaselineMetrics::new(&self.metrics, 0);
        let db = &self.write.db;
        let txn = &self.write.txn;
        let branch = txn.branch.as_deref();
        let mut staging = MutationStaging::new(self.write.write_budget)
            .with_overwrite_writers(self.write.writers);
        let opened = db
            .open_for_mutation_on_branch(
                branch,
                &self.table,
                MutationOpKind::SchemaRewrite,
                Some(txn),
            )
            .await
            .map_err(external)?;
        staging
            .ensure_path(
                &self.table,
                opened.identity,
                opened.full_path,
                opened.table_branch,
                opened.pinned_native_ref,
                opened.entry,
                opened.expected_version,
                MutationOpKind::SchemaRewrite,
            )
            .map_err(external)?;
        let input_failure: Arc<Mutex<Option<DataFusionError>>> = Arc::new(Mutex::new(None));
        let chunker = Chunker::new(
            Arc::clone(&self.schema),
            overwrite_chunk_rows(),
            self.max_input_bytes,
            work.child("overwrite chunks")?,
        );
        let chunks = futures::stream::try_unfold(
            (data, chunker, Vec::<OverwriteChunk>::new(), false),
            {
                let input_failure = Arc::clone(&input_failure);
                let baseline = baseline.clone();
                move |(mut data, mut chunker, mut ready, mut ended)| {
                    let input_failure = Arc::clone(&input_failure);
                    let baseline = baseline.clone();
                    async move {
                        loop {
                            if let Some(chunk) = ready.pop() {
                                return Ok(Some((chunk, (data, chunker, ready, ended))));
                            }
                            if ended {
                                return Ok(None);
                            }
                            match data.next().await {
                                Some(Ok(batch)) => {
                                    let _timer = baseline.elapsed_compute().timer();
                                    let mut cut = chunker.push(batch)?;
                                    cut.reverse();
                                    ready = cut;
                                }
                                Some(Err(error)) => {
                                    *input_failure
                                        .lock()
                                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                        Some(error);
                                    return Err(OmniError::manifest_internal(
                                        "load csv: the validated delta refused before the write finished",
                                    ));
                                }
                                None => {
                                    ended = true;
                                    let mut cut = chunker.flush();
                                    cut.reverse();
                                    ready = cut;
                                }
                            }
                        }
                    }
                }
            },
        );
        let rows = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let chunks = chunks.inspect({
            let rows = Arc::clone(&rows);
            move |chunk| {
                if let Ok(chunk) = chunk {
                    let in_chunk: usize = chunk.batches.iter().map(RecordBatch::num_rows).sum();
                    rows.fetch_add(in_chunk, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
        let mut staging = Box::pin(staging.stage_overwrite_stream(
            db,
            &self.table,
            chunks,
            Arc::clone(&self.schema),
        ));
        let staged = futures::future::poll_fn(|cx| {
            let _timer = baseline.elapsed_compute().timer();
            staging.as_mut().poll(cx)
        })
        .await;
        drop(staging);
        let staged = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let refused = input_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                return Err(refused.unwrap_or_else(|| external(error)));
            }
        };
        let rows = rows.load(std::sync::atomic::Ordering::Relaxed);
        *self
            .write
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(staged);
        work.metric(STAGED_ROWS, rows);
        baseline.record_output(rows);
        baseline.done();
        u64::try_from(rows).map_err(|_| {
            external(OmniError::manifest(
                "staged row count exceeds u64".to_string(),
            ))
        })
    }
}

/// One source partition's rows between two chunk cuts: the batches so far,
/// their count, the chunk's index within the partition, and the memory
/// they are charged to until their writer returns.
struct OpenChunk {
    batches: Vec<RecordBatch>,
    rows: usize,
    index: u64,
    memory: WorkMemory,
}

/// Cuts the sink's input into [`OverwriteChunk`]s of exactly `chunk_rows`
/// rows per source partition (the slice tag names it; the last chunk shorter),
/// each batch bound-checked and rewrapped under the declared schema.
struct Chunker {
    schema: SchemaRef,
    chunk_rows: usize,
    max_input_bytes: u64,
    work: WorkMemory,
    open: BTreeMap<u32, OpenChunk>,
}

impl Chunker {
    fn new(schema: SchemaRef, chunk_rows: usize, max_input_bytes: u64, work: WorkMemory) -> Self {
        Self {
            schema,
            chunk_rows: chunk_rows.max(1),
            max_input_bytes,
            work,
            open: BTreeMap::new(),
        }
    }

    fn push(&mut self, batch: RecordBatch) -> crate::error::Result<Vec<OverwriteChunk>> {
        let bytes = write_batch_bytes(&batch)?.rows;
        if bytes > self.max_input_bytes {
            return Err(OmniError::resource_limit(
                STAGED_WRITE_INPUT_BYTES_RESOURCE,
                self.max_input_bytes,
                bytes,
            ));
        }
        let partition = u32::try_from(slice_of(&batch) >> 32).unwrap_or(u32::MAX);
        let mut batch = RecordBatch::try_new(Arc::clone(&self.schema), batch.columns().to_vec())
            .map_err(OmniError::arrow_internal)?;
        let mut cut = Vec::new();
        if batch.num_rows() == 0 {
            return Ok(cut);
        }
        let mut open = match self.open.remove(&partition) {
            Some(open) => open,
            None => OpenChunk {
                batches: Vec::new(),
                rows: 0,
                index: 0,
                memory: self
                    .work
                    .child("overwrite chunk")
                    .map_err(OmniError::datafusion)?,
            },
        };
        while open.rows + batch.num_rows() >= self.chunk_rows {
            let fill = self.chunk_rows - open.rows;
            let head = batch.slice(0, fill);
            open.memory.hold(&head).map_err(OmniError::datafusion)?;
            open.batches.push(head);
            let next = OpenChunk {
                batches: Vec::new(),
                rows: 0,
                index: open.index + 1,
                memory: self
                    .work
                    .child("overwrite chunk")
                    .map_err(OmniError::datafusion)?,
            };
            let full = std::mem::replace(&mut open, next);
            cut.push(OverwriteChunk {
                order: (partition, full.index),
                batches: full.batches,
                lease: Some(Box::new(full.memory)),
            });
            batch = batch.slice(fill, batch.num_rows() - fill);
        }
        if batch.num_rows() > 0 {
            open.memory.hold(&batch).map_err(OmniError::datafusion)?;
            open.rows += batch.num_rows();
            open.batches.push(batch);
        }
        self.open.insert(partition, open);
        Ok(cut)
    }

    fn flush(&mut self) -> Vec<OverwriteChunk> {
        std::mem::take(&mut self.open)
            .into_iter()
            .filter(|(_, open)| open.rows > 0)
            .map(|(partition, open)| OverwriteChunk {
                order: (partition, open.index),
                batches: open.batches,
                lease: Some(Box::new(open.memory)),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use arrow_array::{Array, RecordBatch, UInt64Array};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::sink::DataSinkExec;
    use datafusion::physical_plan::ExecutionPlan;
    use omnigraph_compiler::query::ast::LoadTarget;
    use omnigraph_compiler::settings::SessionSettings;

    use super::super::fixtures;
    use super::*;
    use crate::Session;
    use crate::db::Omnigraph;
    use crate::loader::LoadMode;
    use crate::loader::csv::{CsvLoadRequest, prepare_csv_batches};
    use crate::storage_layer::WriteBudget;
    use crate::table_store::CapturedSource;

    const GRAPH_SCHEMA: &str = "node Person {\n  name: String @key\n  email: String\n}\n";

    const COMMITTED: &str = r#"{"type": "Person", "data": {"name": "Alice", "email": "alice@x"}}
{"type": "Person", "data": {"name": "Bob", "email": "bob@x"}}
"#;

    async fn committed_graph(dir: &tempfile::TempDir) -> (Arc<Omnigraph>, Session) {
        let db = Arc::new(
            Omnigraph::init(dir.path().to_str().unwrap(), GRAPH_SCHEMA)
                .await
                .unwrap(),
        );
        let session = Session::from_defaults(Arc::clone(&db), SessionSettings::default());
        session
            .load_jsonl(COMMITTED, LoadMode::Overwrite)
            .await
            .unwrap();
        (db, session)
    }

    /// The target's typed delta from a CSV, the shape the plan's source emits.
    fn person_batches(session: &Session, csv: &str) -> Vec<RecordBatch> {
        let request = CsvLoadRequest {
            source: "file:///people.csv".to_string(),
            target: LoadTarget::Node("Person".to_string()),
            mode: LoadMode::Overwrite,
            delimiter: b',',
            header: true,
            columns: None,
        };
        let capture = CapturedSource {
            bytes: Arc::from(csv.as_bytes()),
            admission: None,
            size: csv.len() as u64,
            e_tag: None,
            version: None,
        };
        prepare_csv_batches(&session.catalog(), &request, &capture, &mut |_| Ok(()))
            .unwrap()
            .batches
    }

    async fn run_sink(
        db: &Arc<Omnigraph>,
        batches: Vec<RecordBatch>,
        max_input_bytes: u64,
    ) -> (
        Vec<DfResult<RecordBatch>>,
        Arc<Mutex<Option<crate::exec::staging::StagedMutation>>>,
    ) {
        let schema = batches[0].schema();
        let input = MemorySourceConfig::try_new_exec(&[batches], schema.clone(), None).unwrap();
        let txn = Arc::new(db.open_write_txn(None).await.unwrap());
        let staged = Arc::new(Mutex::new(None));
        let write = LoadWrite {
            db: Arc::clone(db),
            txn,
            write_budget: WriteBudget::default(),
            writers: 2,
            staged: Arc::clone(&staged),
        };
        let sink =
            LanceFragmentSink::new("node:Person".to_string(), schema, write, max_input_bytes);
        let exec = DataSinkExec::new(input, Arc::new(sink), None);
        let (_pool, ctx) = fixtures::context(64 << 20, 8192);
        let outputs = exec.execute(0, ctx).unwrap().collect().await;
        (outputs, staged)
    }

    async fn person_version(session: &Session) -> u64 {
        session
            .snapshot()
            .await
            .dataset("node:Person")
            .expect("Person is committed")
            .published_dataset_version
    }

    /// The sink stages one overwrite of the target and reports the rows,
    /// while the branch's table version stays where the transaction pinned
    /// it: nothing commits inside the sink.
    #[tokio::test]
    async fn stages_the_delta_without_committing() {
        let dir = tempfile::tempdir().unwrap();
        let (db, session) = committed_graph(&dir).await;
        let before = person_version(&session).await;
        let batches = person_batches(
            &session,
            "name,email\nCarol,carol@x\nDave,dave@x\nErin,erin@x\n",
        );
        let (outputs, staged) = run_sink(&db, batches, u64::MAX).await;
        let [Ok(count)] = &outputs[..] else {
            panic!("one count batch, found {outputs:?}");
        };
        let counts = count
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("DataSinkExec counts in u64");
        assert_eq!(counts.values(), &[3]);
        let staged_tables = staged
            .lock()
            .unwrap()
            .as_ref()
            .map(|staged| staged.staged_table_keys().join(","));
        assert_eq!(staged_tables.as_deref(), Some("node:Person"));
        assert_eq!(person_version(&session).await, before);
    }

    /// The sink over `input`, chunks cut every `chunk_rows` rows.
    async fn write_all_chunked(
        db: &Arc<Omnigraph>,
        input: SendableRecordBatchStream,
        chunk_rows: usize,
    ) -> (
        DfResult<u64>,
        Arc<Mutex<Option<crate::exec::staging::StagedMutation>>>,
    ) {
        let schema = input.schema();
        let txn = Arc::new(db.open_write_txn(None).await.unwrap());
        let staged = Arc::new(Mutex::new(None));
        let write = LoadWrite {
            db: Arc::clone(db),
            txn,
            write_budget: WriteBudget::default(),
            writers: 2,
            staged: Arc::clone(&staged),
        };
        let sink = LanceFragmentSink::new("node:Person".to_string(), schema, write, u64::MAX);
        let (_pool, ctx) = fixtures::context(64 << 20, 8192);
        let written = crate::instrumentation::with_overwrite_chunk_rows(
            chunk_rows,
            sink.write_all(input, &ctx),
        )
        .await;
        (written, staged)
    }

    /// Lance's data files under every `data/` directory below `dir`, the
    /// committed fragments and whatever a writer left.
    fn lance_data_files(dir: &std::path::Path) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    lance_data_files(&path)
                } else {
                    let under_data = path
                        .parent()
                        .and_then(std::path::Path::file_name)
                        .is_some_and(|name| name == "data");
                    usize::from(under_data && path.extension().is_some_and(|ext| ext == "lance"))
                }
            })
            .sum()
    }

    /// A chunk the keyed-write fence refuses does not end the reading: the
    /// sink follows its input to the verdict the validate operator parks and
    /// returns that, the fence's own text only when the input ends clean.
    #[tokio::test]
    async fn a_fence_refusal_yields_to_the_inputs_own_verdict() {
        let dir = tempfile::tempdir().unwrap();
        let (db, session) = committed_graph(&dir).await;
        let before_version = person_version(&session).await;
        let before_files = lance_data_files(dir.path());
        let csv = "name,email\nCarol,carol@x\nCarol,carol2@x\nDave,dave@x\nErin,erin@x\n";
        let batches = person_batches(&session, csv);
        let schema = batches[0].schema();
        let judged = futures::stream::iter(batches.into_iter().map(Ok)).chain(
            futures::stream::once(async {
                Err(external(OmniError::manifest(
                    "@unique violation on Person.name: value 'Carol'".to_string(),
                )))
            }),
        );
        let judged: SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                judged,
            ),
        );
        let (written, staged) = write_all_chunked(&db, judged, 2).await;
        let error = written.unwrap_err().to_string();
        assert!(
            error.contains("@unique violation on Person.name: value 'Carol'"),
            "{error}"
        );
        assert!(!error.contains("already has this id"), "{error}");
        assert!(staged.lock().unwrap().is_none());
        assert_eq!(lance_data_files(dir.path()), before_files);
        assert_eq!(person_version(&session).await, before_version);

        let batches = person_batches(&session, csv);
        let unjudged: SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(batches.into_iter().map(Ok)),
            ),
        );
        let (written, staged) = write_all_chunked(&db, unjudged, 2).await;
        let error = written.unwrap_err().to_string();
        assert!(error.contains("already has this id"), "{error}");
        assert!(staged.lock().unwrap().is_none());
        assert_eq!(lance_data_files(dir.path()), before_files);
        assert_eq!(person_version(&session).await, before_version);
    }

    /// A refusal after chunks were written leaves `data/` as the commit left
    /// it, nothing staged, the input's own error returned; the same rows clean
    /// stage one file per chunk, so the refused run had chunks to delete.
    #[tokio::test]
    async fn a_refused_input_leaves_no_fragment_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        let (db, session) = committed_graph(&dir).await;
        let before_version = person_version(&session).await;
        let before_files = lance_data_files(dir.path());
        let csv =
            "name,email\nCarol,carol@x\nDave,dave@x\nErin,erin@x\nFrank,frank@x\nGrace,grace@x\n";
        let batches = person_batches(&session, csv);
        let schema = batches[0].schema();
        let refused = futures::stream::iter(batches.into_iter().map(Ok)).chain(
            futures::stream::once(async {
                Err(external(OmniError::manifest(
                    "record 6 has an empty field".to_string(),
                )))
            }),
        );
        let refused: SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                refused,
            ),
        );
        let (written, staged) = write_all_chunked(&db, refused, 2).await;
        let error = written.unwrap_err();
        assert!(
            error.to_string().contains("record 6 has an empty field"),
            "{error}"
        );
        assert!(staged.lock().unwrap().is_none());
        assert_eq!(
            lance_data_files(dir.path()),
            before_files,
            "the refused run's chunk files were deleted"
        );
        assert_eq!(person_version(&session).await, before_version);

        let batches = person_batches(&session, csv);
        let clean: SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(batches.into_iter().map(Ok)),
            ),
        );
        let (written, staged) = write_all_chunked(&db, clean, 2).await;
        assert_eq!(written.unwrap(), 5);
        assert!(staged.lock().unwrap().is_some());
        assert_eq!(
            lance_data_files(dir.path()),
            before_files + 3,
            "five rows in chunks of two stage three files, uncommitted"
        );
        assert_eq!(person_version(&session).await, before_version);
    }

    /// One input over the plan's bound refuses with the loader's resource
    /// text before anything is staged.
    #[tokio::test]
    async fn refuses_an_input_over_the_write_bound_before_staging() {
        let dir = tempfile::tempdir().unwrap();
        let (db, session) = committed_graph(&dir).await;
        let batches = person_batches(&session, "name,email\nCarol,carol@x\n");
        let (outputs, staged) = run_sink(&db, batches, 1).await;
        let [Err(error)] = &outputs[..] else {
            panic!("one error and no count, found {outputs:?}");
        };
        assert!(
            error
                .to_string()
                .contains(STAGED_WRITE_INPUT_BYTES_RESOURCE),
            "{error}"
        );
        assert!(staged.lock().unwrap().is_none());
    }
}
