//! Exact live-row counts from a query's pinned Lance snapshot.

use std::fmt;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::{breaker_properties, breaker_stream, external, polled};
use crate::db::Snapshot;
use crate::error::OmniError;

pub(crate) struct MetadataCountExec {
    type_key: String,
    snapshot: Snapshot,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl MetadataCountExec {
    pub(crate) fn new(type_key: String, snapshot: Snapshot, schema: SchemaRef) -> Self {
        Self {
            type_key,
            snapshot,
            properties: breaker_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for MetadataCountExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataCountExec")
            .field("type_key", &self.type_key)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for MetadataCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetadataCountExec: {}", self.type_key)
    }
}

impl ExecutionPlan for MetadataCountExec {
    fn name(&self) -> &str {
        "MetadataCountExec"
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
        assert!(children.is_empty(), "MetadataCountExec has no children");
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "MetadataCountExec has one partition");
        let snapshot = self.snapshot.clone();
        let type_key = self.type_key.clone();
        let schema = self.schema();
        let stream = breaker_stream(
            "MetadataCountExec",
            Arc::clone(&schema),
            &ctx,
            &self.metrics,
            move |memory| async move {
                let dataset = snapshot
                    .open_lance_dataset(&type_key)
                    .await
                    .map_err(external)?;
                let rows = dataset
                    .count_rows(None)
                    .await
                    .map_err(|error| external(OmniError::storage_context("count_rows", error)))?;
                memory.metric("metadata_count_rows", rows);
                let count = i64::try_from(rows).map_err(|_| {
                    external(OmniError::manifest("row count exceeds I64".to_string()))
                })?;
                let value: ArrayRef = Arc::new(Int64Array::from(vec![count]));
                let columns = schema.fields().iter().map(|_| Arc::clone(&value)).collect();
                RecordBatch::try_new(schema, columns).map_err(Into::into)
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}
