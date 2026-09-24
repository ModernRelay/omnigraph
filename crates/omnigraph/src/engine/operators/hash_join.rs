//! `HashJoinExec`: a traversal's destination table read once (the build, a
//! table `ScanExec`) and hashed on its id, probed by the traversal's rows in
//! their order. The first probe batch is read before the build, so an empty
//! traversal (a source the gate proved empty) never reads the destination.
//! The switch the `HashJoin` node declares is a branch here: a memory refusal
//! while the build is drained releases the build and reaches the destination
//! rows through the per-slice id lookup over the whole probe instead, when
//! the node declares that fallback. An empty build stops the probe after its
//! first batch.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_planner::AccessPath;

use super::memory::WorkMemory;
use super::producer::producer_stream;
use super::scan::lookup_candidates;
use super::{ScanExec, Switch, external, polled, streaming_properties};
use crate::db::Snapshot;
use crate::engine::scan::hconcat_batches;
use crate::engine::search::NeededColumns;
use crate::error::{OmniError, Result};

/// The refusal name the pool reports when an output batch does not fit.
const OUTPUT: &str = "hash join output";

/// What reaching the destination rows takes on either side of the switch:
/// the destination type, the binding whose ids the probe carries, the pushed
/// filters and the projection.
#[derive(Clone)]
pub(crate) struct LookupSpec {
    pub(crate) type_name: String,
    pub(crate) binding: String,
    pub(crate) filters: Vec<IRExpr>,
    pub(crate) projection: Option<NeededColumns>,
    pub(crate) params: Arc<ParamMap>,
    pub(crate) snapshot: Snapshot,
    pub(crate) catalog: Arc<Catalog>,
}

impl LookupSpec {
    /// `<binding>.<id>`, the column both sides carry.
    pub(crate) fn id_column(&self) -> String {
        format!("{}.{}", self.binding, self.catalog.system_columns.id)
    }
}

pub(crate) struct HashJoinExec {
    probe: Arc<dyn ExecutionPlan>,
    build: Arc<dyn ExecutionPlan>,
    fallback: Option<AccessPath>,
    lookup: Arc<LookupSpec>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl HashJoinExec {
    pub(crate) fn try_new(
        probe: Arc<dyn ExecutionPlan>,
        build: Arc<dyn ExecutionPlan>,
        fallback: Option<AccessPath>,
        lookup: LookupSpec,
    ) -> Result<Self> {
        if fallback == Some(AccessPath::HashJoin) {
            return Err(OmniError::manifest_internal(
                "a hash join declares id_lookup or no fallback",
            ));
        }
        let schema = ScanExec::input_schema(
            &probe.schema(),
            &lookup.type_name,
            &lookup.binding,
            lookup.projection.as_ref(),
            &lookup.catalog,
        )?;
        let id_column = lookup.id_column();
        if build.schema().column_with_name(&id_column).is_none() {
            return Err(OmniError::manifest_internal(format!(
                "destination scan projects no '{id_column}'"
            )));
        }
        Ok(Self {
            probe,
            build,
            fallback,
            lookup: Arc::new(lookup),
            properties: streaming_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl fmt::Debug for HashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HashJoinExec")
            .field("on", &self.lookup.id_column())
            .field("fallback", &self.fallback)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for HashJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HashJoinExec: on={}", self.lookup.id_column())?;
        match self.fallback {
            Some(AccessPath::IdLookup) => write!(f, ", fallback=id_lookup"),
            Some(AccessPath::HashJoin) | None => write!(f, ", fallback=none"),
        }
    }
}

/// The drained build side: its rows and the row of every id, held under
/// `memory` for as long as the probe runs.
struct Built {
    batch: RecordBatch,
    row_by_id: HashMap<String, u32>,
    _memory: WorkMemory,
}

async fn build_side(
    stream: SendableRecordBatchStream,
    schema: &SchemaRef,
    id_column: &str,
    memory: &WorkMemory,
) -> DfResult<Built> {
    let work = memory.child("hash join build")?;
    let input = work.child("hash join build input")?;
    let batches = input.collect(stream).await?;
    let batch = work.concat(schema, &batches)?;
    drop(batches);
    drop(input);
    let ids = batch
        .column_by_name(id_column)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            external(OmniError::manifest_internal(format!(
                "destination scan must return Utf8 '{id_column}'"
            )))
        })?;
    work.entries::<(String, u32)>(ids.len())?;
    work.string(ids.iter().flatten().map(str::len).sum())?;
    let mut row_by_id = HashMap::with_capacity(ids.len());
    for (row, id) in ids.iter().enumerate() {
        if let Some(id) = id {
            let row = u32::try_from(row).map_err(|_| {
                external(OmniError::manifest_internal(
                    "destination scan exceeds row index range",
                ))
            })?;
            row_by_id.entry(id.to_string()).or_insert(row);
        }
    }
    Ok(Built {
        batch,
        row_by_id,
        _memory: work,
    })
}

/// `output`'s columns in the declared order, by name: the build scan also
/// carries the columns its pushed filters read, which the schema omits.
fn declared_columns(output: &RecordBatch, declared: &SchemaRef) -> Result<RecordBatch> {
    let columns = declared
        .fields()
        .iter()
        .map(|field| {
            output.column_by_name(field.name()).cloned().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "hash join output has no column '{}' the dependent scan declares",
                    field.name()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(declared), columns).map_err(OmniError::arrow_internal)
}

impl ExecutionPlan for HashJoinExec {
    fn name(&self) -> &str {
        "HashJoinExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.probe, &self.build]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 2, "HashJoinExec has a probe and a build");
        let build = children.pop().expect("build child");
        let probe = children.pop().expect("probe child");
        Ok(Arc::new(
            Self::try_new(probe, build, self.fallback, LookupSpec::clone(&self.lookup))
                .map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "HashJoinExec has one partition");
        let schema: SchemaRef = self.schema();
        let declared = Arc::clone(&schema);
        let switch = Switch::gauge(&self.metrics);
        let fallbacks = MetricBuilder::new(&self.metrics).counter("hash_build_fallbacks", 0);
        let mut work = WorkMemory::new(Arc::clone(&ctx), "HashJoinExec")?;
        work.set_metrics(self.metrics.clone());
        work.metric("input_rows", 0);
        let memory = Arc::new(work);
        let build_schema = self.build.schema();
        let build = Arc::clone(&self.build);
        let probe = Arc::clone(&self.probe);
        let fallback = self.fallback;
        let lookup = Arc::clone(&self.lookup);
        let stream = producer_stream(
            schema,
            memory,
            Some(&self.metrics),
            move |memory, sender| async move {
                let id_column = lookup.id_column();
                let probe_schema = probe.schema();
                let mut probe = probe.execute(0, Arc::clone(&memory.ctx))?;
                let head = memory.child("hash join probe head")?;
                let first = loop {
                    match probe.next().await.transpose()? {
                        Some(batch) if batch.num_rows() == 0 => continue,
                        Some(batch) => {
                            head.hold(&batch)?;
                            break batch;
                        }
                        None => return Ok(()),
                    }
                };
                let mut probe: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                    probe_schema,
                    futures::stream::iter([Ok(first)]).chain(probe),
                ));
                let build = build.execute(0, Arc::clone(&memory.ctx))?;
                let built = match build_side(build, &build_schema, &id_column, &memory).await {
                    Ok(built) => built,
                    Err(error)
                        if fallback.is_some() && OmniError::is_query_memory_failure(&error) =>
                    {
                        fallbacks.add(1);
                        Switch::IdLookup.record(&switch);
                        return lookup_candidates(probe, &lookup, &declared, &memory, &sender)
                            .await;
                    }
                    Err(error) => return Err(error),
                };
                drop(head);
                Switch::HashJoin.record(&switch);
                if built.batch.num_rows() == 0 {
                    return Ok(());
                }
                while let Some(batch) = probe.next().await {
                    let batch = batch?;
                    memory.metric("input_rows", batch.num_rows());
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let id_index = batch.schema().index_of(&id_column).map_err(|error| {
                        external(OmniError::manifest_internal(error.to_string()))
                    })?;
                    let ids = batch
                        .column(id_index)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            external(OmniError::manifest_internal(
                                "traversal output IDs must be Utf8",
                            ))
                        })?;
                    let work = Arc::new(memory.child(OUTPUT)?);
                    work.entries::<u32>(ids.len().saturating_mul(2))?;
                    let mut probe_rows = Vec::with_capacity(ids.len());
                    let mut build_rows = Vec::with_capacity(ids.len());
                    for (row, id) in ids.iter().enumerate() {
                        if let Some(build_row) = id.and_then(|id| built.row_by_id.get(id)) {
                            probe_rows.push(u32::try_from(row).map_err(|_| {
                                external(OmniError::manifest_internal(
                                    "traversal batch exceeds row index range",
                                ))
                            })?);
                            build_rows.push(*build_row);
                        }
                    }
                    if probe_rows.is_empty() {
                        continue;
                    }
                    let probe_columns: Vec<usize> = (0..batch.num_columns())
                        .filter(|index| *index != id_index)
                        .collect();
                    let probe_side = batch.project(&probe_columns)?;
                    let probe_side =
                        work.take_once(&probe_side, &UInt32Array::from(probe_rows), OUTPUT)?;
                    let build_side =
                        work.take_once(&built.batch, &UInt32Array::from(build_rows), OUTPUT)?;
                    let output = hconcat_batches(&probe_side, &build_side).map_err(external)?;
                    let output = declared_columns(&output, &declared).map_err(external)?;
                    work.hold(&output)?;
                    sender.send_bounded(output, work).await?;
                }
                Ok(())
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}
