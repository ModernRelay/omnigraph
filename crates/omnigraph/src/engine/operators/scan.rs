//! Executes a binding's scan under its `SearchMode`, prefixes its columns,
//! and records its `ScanReport` for the search retry ladders. Lance plans
//! execute under the query's shared `TaskContext`.

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::ir::{IRFilter, ParamMap};

use super::{breaker_properties, breaker_stream, conform, external};
use crate::db::Snapshot;
use crate::engine::scan::{execute_node_scan, prefix_batch, scan_output_schema};
use crate::engine::search::{NeededColumns, ScanReport, SearchMode};
use crate::error::Result;

mod input;

pub(crate) enum ScanSource {
    Table {
        mode: Box<SearchMode>,
        report: Arc<Mutex<ScanReport>>,
    },
    Dependent {
        input: Arc<dyn ExecutionPlan>,
    },
}

pub(crate) struct ScanExec {
    source: ScanSource,
    type_name: String,
    binding: String,
    filters: Vec<IRFilter>,
    projection: Option<NeededColumns>,
    params: Arc<ParamMap>,
    snapshot: Snapshot,
    catalog: Arc<Catalog>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ScanExec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        source: ScanSource,
        type_name: String,
        binding: String,
        filters: Vec<IRFilter>,
        projection: Option<NeededColumns>,
        params: Arc<ParamMap>,
        snapshot: Snapshot,
        catalog: Arc<Catalog>,
    ) -> Result<Self> {
        let properties = match &source {
            ScanSource::Dependent { input } => Self::input_properties(Self::input_schema(
                &input.schema(),
                &type_name,
                &binding,
                projection.as_ref(),
                &catalog,
            )?),
            ScanSource::Table { mode, .. } => {
                let schema = scan_output_schema(
                    &type_name,
                    &binding,
                    &filters,
                    &params,
                    &catalog,
                    mode,
                    projection.as_ref(),
                )?;
                breaker_properties(schema)
            }
        };
        Ok(Self {
            source,
            type_name,
            binding,
            filters,
            projection,
            params,
            snapshot,
            catalog,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl fmt::Debug for ScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanExec")
            .field("type_name", &self.type_name)
            .field("binding", &self.binding)
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScanExec: ${}: {}", self.binding, self.type_name)?;
        if !self.filters.is_empty() {
            let filters: Vec<String> = self.filters.iter().map(ToString::to_string).collect();
            write!(f, ", filters=[{}]", filters.join(", "))?;
        }
        if let Some(NeededColumns(columns)) = &self.projection {
            let mut columns: Vec<&str> = columns.iter().map(String::as_str).collect();
            columns.sort_unstable();
            write!(f, ", projection=[{}]", columns.join(", "))?;
        }
        if matches!(self.source, ScanSource::Dependent { .. }) {
            write!(f, ", id_restriction=input")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for ScanExec {
    fn name(&self) -> &str {
        "ScanExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.source {
            ScanSource::Table { .. } => Vec::new(),
            ScanSource::Dependent { input } => vec![input],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let input = match &self.source {
            ScanSource::Table { .. } => {
                assert!(children.is_empty(), "ScanExec input arity");
                return Ok(self);
            }
            ScanSource::Dependent { .. } => {
                assert_eq!(children.len(), 1, "ScanExec input arity");
                children.pop().ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal("ScanExec needs one input".into())
                })?
            }
        };
        Ok(Arc::new(
            Self::try_new(
                ScanSource::Dependent { input },
                self.type_name.clone(),
                self.binding.clone(),
                self.filters.clone(),
                self.projection.clone(),
                Arc::clone(&self.params),
                self.snapshot.clone(),
                Arc::clone(&self.catalog),
            )
            .map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "ScanExec has one partition");
        let (mode, report) = match &self.source {
            ScanSource::Dependent { input } => {
                return self.execute_input(input.execute(0, Arc::clone(&ctx))?, ctx);
            }
            ScanSource::Table { mode, report } => (mode.as_ref().clone(), Arc::clone(report)),
        };
        let schema: SchemaRef = self.schema();
        let type_name = self.type_name.clone();
        let binding = self.binding.clone();
        let filters = self.filters.clone();
        let projection = self.projection.clone();
        let params = Arc::clone(&self.params);
        let snapshot = self.snapshot.clone();
        let catalog = Arc::clone(&self.catalog);
        let declared = Arc::clone(&schema);
        Ok(breaker_stream(
            "ScanExec",
            schema,
            &ctx,
            &self.metrics,
            move |reservation| async move {
                let mut scan_report = ScanReport::default();
                let batch = execute_node_scan(
                    &type_name,
                    &binding,
                    &filters,
                    &params,
                    &snapshot,
                    &catalog,
                    &mode,
                    &mut scan_report,
                    projection.as_ref(),
                    &reservation,
                )
                .await
                .map_err(external)?;
                if mode
                    .bm25
                    .as_ref()
                    .is_some_and(|(variable, ..)| variable == &binding)
                {
                    reservation.metric("bm25_scan_rows", batch.num_rows());
                }
                if let Some(nearest) = scan_report.nearest_scan {
                    reservation.metric("nearest_rows", nearest.rows);
                    reservation.metric("nearest_k", nearest.k);
                    reservation.metric(
                        "nearest_maximum_nprobes",
                        nearest.maximum_nprobes.unwrap_or(0),
                    );
                    reservation.metric("nearest_exhausted", usize::from(nearest.exhausted));
                    reservation.metric("nearest_dataset_rows", nearest.dataset_rows as usize);
                }
                if scan_report.nearest_scan.is_some() {
                    *report
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = scan_report;
                }
                let prefixed = prefix_batch(&batch, &binding).map_err(external)?;
                conform(prefixed, &declared).map_err(external)
            },
        ))
    }
}
