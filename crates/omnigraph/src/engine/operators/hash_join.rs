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
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRExpr, ParamMap};
use omnigraph_planner::AccessPath;

use super::memory::WorkMemory;
use super::scan::Lookup;
use super::shape::{BatchSender, CollectedSide, Flow, Streamed, Streaming, work};
use super::{ScanExec, Switch, external};
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
    shape: Streaming,
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
            shape: Streaming::new(schema),
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
    build: &Arc<dyn ExecutionPlan>,
    id_column: &str,
    memory: &WorkMemory,
) -> DfResult<Built> {
    let side = CollectedSide::collect(build, memory, "hash join build").await?;
    let work = side.held;
    let batch = side.batch;
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
        self.shape.properties()
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
        let memory = work(Arc::clone(&ctx), "HashJoinExec", &self.metrics)?;
        memory.metric("input_rows", 0);
        let probe = Arc::clone(&self.probe);
        let body = Probe {
            build: Arc::clone(&self.build),
            lookup: Arc::clone(&self.lookup),
            declared: self.schema(),
            fallback: self.fallback,
            switch: Switch::gauge(&self.metrics),
            fallbacks: MetricBuilder::new(&self.metrics).counter("hash_build_fallbacks", 0),
            side: Side::Pending,
        };
        Ok(self
            .shape
            .produce(memory, &self.metrics, move || probe.execute(0, ctx), body))
    }
}

/// The per-batch body of the join over the probe: the build is drained at
/// the first non-empty probe batch, into the hash or, on a memory refusal
/// the node's fallback covers, into the per-slice id lookup.
struct Probe {
    build: Arc<dyn ExecutionPlan>,
    lookup: Arc<LookupSpec>,
    declared: SchemaRef,
    fallback: Option<AccessPath>,
    switch: Gauge,
    fallbacks: Count,
    side: Side,
}

/// How the probe reaches the destination rows, once the build was tried.
enum Side {
    Pending,
    Hashed(Box<Built>),
    Lookup(Lookup),
    /// The build held no row: nothing joins, the probe stops.
    Empty,
}

impl Probe {
    /// The build tried at the first non-empty probe batch, which `head`
    /// holds meanwhile.
    async fn drain_build(&mut self, first: &RecordBatch, memory: &Arc<WorkMemory>) -> DfResult<()> {
        let head = memory.child("hash join probe head")?;
        head.hold(first)?;
        let id_column = self.lookup.id_column();
        self.side = match build_side(&self.build, &id_column, memory).await {
            Ok(built) => {
                Switch::HashJoin.record(&self.switch);
                if built.batch.num_rows() == 0 {
                    Side::Empty
                } else {
                    Side::Hashed(Box::new(built))
                }
            }
            Err(error) if self.fallback.is_some() && OmniError::is_query_memory_failure(&error) => {
                self.fallbacks.add(1);
                Switch::IdLookup.record(&self.switch);
                Side::Lookup(Lookup {
                    spec: Arc::clone(&self.lookup),
                    declared: Arc::clone(&self.declared),
                })
            }
            Err(error) => return Err(error),
        };
        drop(head);
        Ok(())
    }

    async fn hashed(
        &self,
        built: &Built,
        batch: RecordBatch,
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
    ) -> DfResult<()> {
        let id_column = self.lookup.id_column();
        let id_index = batch
            .schema()
            .index_of(&id_column)
            .map_err(|error| external(OmniError::manifest_internal(error.to_string())))?;
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
            return Ok(());
        }
        let probe_columns: Vec<usize> = (0..batch.num_columns())
            .filter(|index| *index != id_index)
            .collect();
        let probe_side = batch.project(&probe_columns)?;
        let probe_side = work.take_once(&probe_side, &UInt32Array::from(probe_rows), OUTPUT)?;
        let build_side = work.take_once(&built.batch, &UInt32Array::from(build_rows), OUTPUT)?;
        let output = hconcat_batches(&probe_side, &build_side).map_err(external)?;
        let output = declared_columns(&output, &self.declared).map_err(external)?;
        work.hold(&output)?;
        sender.send_bounded(output, work).await
    }
}

impl Streamed for Probe {
    fn batch<'a>(
        &'a mut self,
        batch: RecordBatch,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<Flow>> {
        Box::pin(async move {
            memory.metric("input_rows", batch.num_rows());
            if batch.num_rows() == 0 {
                return Ok(Flow::Continue);
            }
            if let Side::Pending = self.side {
                self.drain_build(&batch, memory).await?;
            }
            match &self.side {
                Side::Hashed(built) => {
                    self.hashed(built, batch, memory, sender).await?;
                    Ok(Flow::Continue)
                }
                Side::Lookup(lookup) => {
                    lookup.resolve(batch, memory, sender).await?;
                    Ok(Flow::Continue)
                }
                Side::Empty => Ok(Flow::Stop),
                Side::Pending => Err(DataFusionError::Internal(
                    "hash join probed before its build was tried".into(),
                )),
            }
        })
    }
}
