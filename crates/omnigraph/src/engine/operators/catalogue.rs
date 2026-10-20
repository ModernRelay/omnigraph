//! The leaf of a catalogue scan: the engine catalogue's rows of one `@`
//! type, projected to the columns the plan reads and prefixed with the
//! binding, from memory. No storage handle, no storage fallback: the rows
//! are the binary's own.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::catalog::Catalog;

use super::shape::{Breaker, work};
use crate::engine::catalogue;
use crate::engine::scan::{ScanColumns, SearchColumns, prefix_batch};
use crate::engine::search::NeededColumns;
use crate::error::{OmniError, Result};

pub(crate) struct CatalogueExec {
    type_name: String,
    binding: String,
    /// The projected, prefixed rows, built at lowering so the operator's
    /// schema is the batch's.
    batch: RecordBatch,
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl CatalogueExec {
    /// The rows of the catalogue type `type_name` for `binding`, keeping
    /// the key and every column in `projection` (every column when `None`),
    /// as `ScanExec` would shape the same read of a stored type.
    pub(crate) fn try_new(
        type_name: &str,
        binding: &str,
        projection: Option<&NeededColumns>,
        catalog: &Catalog,
    ) -> Result<Self> {
        let table = catalogue::table(type_name).ok_or_else(|| {
            OmniError::manifest_internal(format!("`{type_name}` is not a catalogue type"))
        })?;
        let node_type = catalog.query_node_type(type_name).ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "catalogue type `{type_name}` is not in the query catalog"
            ))
        })?;
        let full = table.batch(node_type)?;
        let columns = ScanColumns::new(node_type, SearchColumns::default(), projection);
        let kept = columns.empty_batch(node_type).schema();
        let projected = full
            .project(
                &kept
                    .fields()
                    .iter()
                    .map(|field| full.schema().index_of(field.name()))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(OmniError::arrow_internal)?,
            )
            .map_err(OmniError::arrow_internal)?;
        let batch = prefix_batch(&projected, binding)?;
        Ok(Self {
            type_name: type_name.to_string(),
            binding: binding.to_string(),
            shape: Breaker::new(batch.schema()),
            batch,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl fmt::Debug for CatalogueExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogueExec")
            .field("type_name", &self.type_name)
            .field("binding", &self.binding)
            .field("rows", &self.batch.num_rows())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CatalogueExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CatalogueExec: ${}: {} ({} rows)",
            self.binding,
            self.type_name,
            self.batch.num_rows()
        )
    }
}

impl ExecutionPlan for CatalogueExec {
    fn name(&self) -> &str {
        "CatalogueExec"
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
        assert!(children.is_empty(), "CatalogueExec has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CatalogueExec has one partition");
        let batch = self.batch.clone();
        let memory = work(ctx, "CatalogueExec", &self.metrics)?;
        Ok(self
            .shape
            .stream(memory, &self.metrics, move |memory| async move {
                memory.metric("catalogue_rows", batch.num_rows());
                memory.hold(&batch)?;
                Ok(batch)
            }))
    }
}
