//! A load's write: the validated delta staged as Lance fragments of the
//! target under the open transaction, never committed here. DataFusion's
//! `DataSinkExec` hosts the sink and emits the one `count` row; the driver
//! takes the staged transaction from the slot and commits it after the plan.

use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::common::Result as DfResult;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;

use super::external;
use super::memory::WorkMemory;
use crate::db::MutationOpKind;
use crate::engine::LoadWrite;
use crate::error::OmniError;
use crate::exec::staging::{MutationStaging, PendingMode};
use crate::loader::STAGED_WRITE_INPUT_BYTES_RESOURCE;
use crate::table_store::write_batch_bytes;

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
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        let mut work = WorkMemory::new(Arc::clone(context), "LanceFragmentSink")?;
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
        let mut rows: usize = 0;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            let _timer = baseline.elapsed_compute().timer();
            let bytes = write_batch_bytes(&batch).map_err(external)?.rows;
            if bytes > self.max_input_bytes {
                return Err(external(OmniError::resource_limit(
                    STAGED_WRITE_INPUT_BYTES_RESOURCE,
                    self.max_input_bytes,
                    bytes,
                )));
            }
            work.hold(&batch)?;
            rows += batch.num_rows();
            staging
                .append_batch(&self.table, batch.schema(), PendingMode::Overwrite, batch)
                .map_err(external)?;
        }
        let mut staging = Box::pin(staging.stage_all_with_concurrency(db, branch, 1));
        let staged = futures::future::poll_fn(|cx| {
            let _timer = baseline.elapsed_compute().timer();
            staging.as_mut().poll(cx)
        })
        .await
        .map_err(external)?;
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
